/*
 * Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution.
 *
 *   This program is free software; you can redistribute it and/or modify
 *   it under the terms of the GNU General Public License as published by
 *   the Free Software Foundation; either version 2 of the License, or
 *   (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
 *
 */

/*
 * object_domain.c: type, domain and value operations.
 * This module primarily defines support for domain structures.
 */

#ident "$Id$"


#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <float.h>
#include <math.h>
#include <errno.h>

#include "memory_alloc.h"

#include "mprec.h"
#include "object_representation.h"
#include "db.h"

#include "object_primitive.h"
#include "object_domain.h"

#include "work_space.h"
#if !defined (SERVER_MODE)
#include "object_accessor.h"
#else /* SERVER_MODE */
#include "object_accessor.h"
#endif /* !SERVER_MODE */
#include "set_object.h"

#include "string_opfunc.h"
#include "cnv.h"
#include "cnverr.h"

#if !defined (SERVER_MODE)
#include "schema_manager.h"
#include "locator_cl.h"
#endif /* !SERVER_MODE */

#if defined (SERVER_MODE)
#include "connection_error.h"
#include "language_support.h"
#include "xserver_interface.h"
#endif /* SERVER_MODE */

#include "server_interface.h"
#include "chartype.h"


/* this must be the last header file included!!! */
#include "dbval.h"

#if !defined (SERVER_MODE)
#define pthread_mutex_init(a, b)
#define pthread_mutex_destroy(a)
#define pthread_mutex_lock(b) 0
#define pthread_mutex_unlock(a)
#endif /* !SERVER_MODE */

#define ARE_COMPARABLE(typ1, typ2)				\
    ((typ1 == typ2)						\
     && !(TP_IS_VARBIT_TYPE ((typ1)) || TP_IS_VARBIT_TYPE ((typ2))))

#define DBL_MAX_DIGITS    ((int)ceil(DBL_MAX_EXP * log10((double) FLT_RADIX)))

typedef enum tp_coersion_mode
{
  TP_EXPLICIT_COERCION = 0,
  TP_IMPLICIT_COERCION
} TP_COERCION_MODE;

/*
 * These are arranged to get relative types for symmetrical
 * coercion selection. The absolute position is not critical.
 * If two types are mutually coercible, the more general
 * should appear later. Eg. Float should appear after integer.
 */

static const DB_TYPE db_type_rank[] = {
  DB_TYPE_NULL,
  DB_TYPE_INTEGER,
  DB_TYPE_BIGINT,
  DB_TYPE_NUMERIC,
  DB_TYPE_DOUBLE,
  DB_TYPE_SEQUENCE,
  DB_TYPE_TIME,
  DB_TYPE_DATE,
  DB_TYPE_DATETIME,
  DB_TYPE_OID,
  DB_TYPE_OBJECT,
  DB_TYPE_VARCHAR,
  DB_TYPE_VARBIT,
  DB_TYPE_VARIABLE,
  DB_TYPE_SUB,
  (DB_TYPE) (DB_TYPE_LAST + 1)
};

extern unsigned int db_on_server;

/*
 * Shorthand to initialize a bunch of fields without duplication.
 * Initializes the fields from precision to the end.
 */
#define DOMAIN_INIT                                    \
  0,            /* precision */                        \
  0,            /* scale */                            \
  NULL,         /* class */                            \
  NULL,         /* set domain */                       \
  NULL_OID_INITIALIZER, /* class OID */                \
  1,            /* built_in_index (set in tp_init) */  \
  0,            /* collation id */		       \
  0,            /* self_ref */                         \
  1,            /* is_cached */                        \
  0,            /* is_parameterized */                 \
  0				/* is_visited */

/* Same as above, but leaves off the prec and scale, and sets the codeset */
#define DOMAIN_INIT2(coll)                    \
  NULL,         /* class */                            \
  NULL,         /* set domain */                       \
  NULL_OID_INITIALIZER, /* class OID */                \
  1,            /* built_in_index (set in tp_init) */  \
  (coll),	/* collation id */		       \
  0,            /* self_ref */                         \
  1,            /* is_cached */                        \
  1,            /* is_parameterized */                 \
  0				/* is_visited */

/*
 * Same as DOMAIN_INIT but it sets the is_parameterized flag.
 * Used for things that don't have a precision but which are parameterized in
 * other ways.
 */
#define DOMAIN_INIT3                                   \
  0,            /* precision */                        \
  0,            /* scale */                            \
  NULL,         /* class */                            \
  NULL,         /* set domain */                       \
  NULL_OID_INITIALIZER, /* class OID */                \
  1,            /* built_in_index (set in tp_init) */  \
  0,            /* collation id */		       \
  0,            /* self_ref */                         \
  1,            /* is_cached */                        \
  1,            /* is_parameterized */                 \
  0				/* is_visited */

/* Same as DOMAIN_INIT but set the prec and scale. */
#define DOMAIN_INIT4(prec, scale)                      \
  (prec),       /* precision */                        \
  (scale),      /* scale */                            \
  NULL,         /* class */                            \
  NULL,         /* set domain */                       \
  NULL_OID_INITIALIZER, /* class OID */                \
  1,            /* built_in_index (set in tp_init) */  \
  0,            /* collation id */                     \
  0,            /* self_ref */                         \
  1,            /* is_cached */                        \
  0,            /* is_parameterized */                 \
  0				/* is_visited */

TP_DOMAIN tp_Null_domain = { NULL, NULL, &tp_Null, DOMAIN_INIT };
TP_DOMAIN tp_Integer_domain = { NULL, NULL, &tp_Integer,
  DOMAIN_INIT4 (DB_INTEGER_PRECISION, 0)
};
TP_DOMAIN tp_Bigint_domain = { NULL, NULL, &tp_Bigint,
  DOMAIN_INIT4 (DB_BIGINT_PRECISION, 0)
};
TP_DOMAIN tp_Double_domain = { NULL, NULL, &tp_Double,
  DOMAIN_INIT4 (DB_DOUBLE_DECIMAL_PRECISION, 0)
};

TP_DOMAIN tp_String_domain = { NULL, NULL, &tp_String,
  DB_MAX_VARCHAR_PRECISION, 0,
  DOMAIN_INIT2 (LANG_COLL_UTF8_EN_CI)
};
TP_DOMAIN tp_Object_domain = { NULL, NULL, &tp_Object, DOMAIN_INIT3 };

TP_DOMAIN tp_Sequence_domain = {
  NULL, NULL, &tp_Sequence, DOMAIN_INIT3
};

TP_DOMAIN tp_Time_domain = { NULL, NULL, &tp_Time,
  DOMAIN_INIT4 (DB_TIME_PRECISION, 0)
};
TP_DOMAIN tp_Date_domain = { NULL, NULL, &tp_Date,
  DOMAIN_INIT4 (DB_DATE_PRECISION, 0)
};
TP_DOMAIN tp_Datetime_domain = { NULL, NULL, &tp_Datetime,
  DOMAIN_INIT4 (DB_DATETIME_PRECISION, DB_DATETIME_DECIMAL_SCALE)
};

TP_DOMAIN tp_Variable_domain = {
  NULL, NULL, &tp_Variable, DOMAIN_INIT3
};

TP_DOMAIN tp_Substructure_domain = {
  NULL, NULL, &tp_Substructure, DOMAIN_INIT3
};
TP_DOMAIN tp_Oid_domain = { NULL, NULL, &tp_Oid, DOMAIN_INIT3 };

TP_DOMAIN tp_Numeric_domain = { NULL, NULL, &tp_Numeric,
  DB_DEFAULT_NUMERIC_PRECISION, DB_DEFAULT_NUMERIC_SCALE,
  DOMAIN_INIT2 (0)
};

TP_DOMAIN tp_VarBit_domain = { NULL, NULL, &tp_VarBit,
  DB_MAX_VARBIT_PRECISION, 0,
  DOMAIN_INIT2 (0)
};

static bool tp_Domain_is_initialized = false;

/* These must be in DB_TYPE order */
static TP_DOMAIN *tp_Domains[] = {
  &tp_Null_domain,
  &tp_Integer_domain,
  &tp_Null_domain,
  &tp_Double_domain,
  &tp_String_domain,
  &tp_Object_domain,
  &tp_Null_domain,
  &tp_Null_domain,
  &tp_Sequence_domain,
  &tp_Null_domain,
  &tp_Time_domain,
  &tp_Null_domain,
  &tp_Date_domain,
  &tp_Null_domain,
  &tp_Variable_domain,
  &tp_Substructure_domain,
  &tp_Null_domain,
  &tp_Null_domain,
  &tp_Null_domain,
  &tp_Null_domain,
  &tp_Oid_domain,		/* does this make sense? shouldn't we share tp_Object_domain */
  &tp_Null_domain,		/* current position of DB_TYPE_DB_VALUE */
  &tp_Numeric_domain,
  &tp_Null_domain,
  &tp_VarBit_domain,
  &tp_Null_domain,
  &tp_Null_domain,
  &tp_Null_domain,
  &tp_Null_domain,		/*result set */
  &tp_Null_domain,
  &tp_Null_domain,
  &tp_Bigint_domain,
  &tp_Datetime_domain,

  /* beginning of some "padding" built-in domains that can be used as
   * expansion space when new primitive data types are added.
   */
  &tp_Null_domain,
  &tp_Null_domain,
  &tp_Null_domain,
  &tp_Null_domain,
  &tp_Null_domain,
  &tp_Null_domain,
  &tp_Null_domain,
  &tp_Null_domain,
  &tp_Null_domain,
  &tp_Null_domain,
  &tp_Null_domain,
  &tp_Null_domain,
  &tp_Null_domain,
  &tp_Null_domain,
  &tp_Null_domain,
  &tp_Null_domain,

  /* beginning of the built-in, complex domains */


  /* end of built-in domain marker */
  NULL
};

#if defined (SERVER_MODE)
/* lock for domain list cache */
static pthread_mutex_t tp_domain_cache_lock = PTHREAD_MUTEX_INITIALIZER;
#endif /* SERVER_MODE */

static void domain_init (TP_DOMAIN * domain, DB_TYPE typeid_);
static int tp_domain_size_internal (const TP_DOMAIN * domain);
static void tp_value_slam_domain (DB_VALUE * value, const DB_DOMAIN * domain);
static TP_DOMAIN *tp_is_domain_cached (TP_DOMAIN * dlist,
				       TP_DOMAIN * transient, TP_MATCH exact,
				       TP_DOMAIN ** ins_pos);
#if !defined (SERVER_MODE)
static void tp_swizzle_oid (TP_DOMAIN * domain);
#endif /* SERVER_MODE */
#if defined(ENABLE_UNUSED_FUNCTION)
static const TP_DOMAIN *tp_domain_find_compatible (const TP_DOMAIN * src,
						   const TP_DOMAIN * dest);
static int tp_null_terminate (const DB_VALUE * src, char **strp, int str_len,
			      bool * do_alloc);
#endif
static int tp_atotime (const DB_VALUE * src, DB_TIME * temp);
static int tp_atodate (const DB_VALUE * src, DB_DATE * temp);
static int tp_atoudatetime (const DB_VALUE * src, DB_DATETIME * temp);
static int tp_atonumeric (const DB_VALUE * src, DB_VALUE * temp);
static int tp_atof (const DB_VALUE * src, double *num_value,
		    DB_DATA_STATUS * data_stat);
static int tp_atobi (const DB_VALUE * src, DB_BIGINT * num_value,
		     DB_DATA_STATUS * data_stat);
#if defined(ENABLE_UNUSED_FUNCTION)
static char *tp_itoa (int value, char *string, int radix);
#endif
static char *tp_ltoa (DB_BIGINT value, char *string, int radix);
static void format_floating_point (char *new_string, char *rve, int ndigits,
				   int decpt, int sign);
static void tp_dtoa (DB_VALUE const *src, DB_VALUE * result);
#if defined(ENABLE_UNUSED_FUNCTION)
static int bfmt_print (int bfmt, const DB_VALUE * the_db_bit, char *string,
		       int max_size);
#endif
static TP_DOMAIN_STATUS
tp_value_coerce_internal (const DB_VALUE * src, DB_VALUE * dest,
			  const TP_DOMAIN * desired_domain,
			  TP_COERCION_MODE coercion_mode,
			  bool preserve_domain);
#if 0				/* DO NOT DELETE ME */
static int oidcmp (OID * oid1, OID * oid2);
#endif
static int tp_domain_match_internal (const TP_DOMAIN * dom1,
				     const TP_DOMAIN * dom2, TP_MATCH exact,
				     bool match_order);
#if defined(RYE_DEBUG)
static void fprint_domain (FILE * fp, TP_DOMAIN * domain);
#endif
static TP_DOMAIN *tp_domain_get_list (DB_TYPE type);

/*
 * tp_init - Global initialization for this module.
 *    return: none
 */
void
tp_init (void)
{
  TP_DOMAIN *d;
  int i;

  if (tp_Domain_is_initialized == true)
    {
      return;
    }
  /*
   * Make sure the next pointer on all the built-in domains is clear.
   * Also make sure the built-in domain numbers are assigned consistently.
   * Assign the builtin indexes starting from 1 so we can use zero to mean
   * that the domain isn't built-in.
   */
  for (i = 0; tp_Domains[i] != NULL; i++)
    {
      d = tp_Domains[i];
      d->next_list = NULL;
      d->class_mop = NULL;
      d->self_ref = 0;
      d->setdomain = NULL;
      d->class_oid.volid = d->class_oid.pageid = d->class_oid.slotid = -1;
      d->is_cached = 1;
      d->built_in_index = i + 1;

      /* ! need to be adding this to the corresponding list */
    }

  tp_Domain_is_initialized = true;
}

/*
 * tp_apply_sys_charset - applies system charset to string domains
 *    return: none
 */
void
tp_apply_sys_charset (void)
{
  /* update string domains with current codeset */
  tp_String_domain.collation_id = LANG_COERCIBLE_COLL;
}

/*
 * tp_final - Global shutdown for this module.
 *    return: none
 * Note:
 *    Frees all the cached domains.
 */
void
tp_final (void)
{
  TP_DOMAIN *dlist, *d, *next, *prev;
  int i;

  if (tp_Domain_is_initialized == false)
    {
      return;
    }

  /*
   * Make sure the next pointer on all the built-in domains is clear.
   * Also make sure the built-in domain numbers are assigned consistently.
   */
  for (i = 0; tp_Domains[i] != NULL; i++)
    {
      dlist = tp_Domains[i];
      /*
       * The first element in the domain array is always a built-in, there
       * can potentially be other built-ins in the list mixed in with
       * allocated domains.
       */
      for (d = dlist->next_list, prev = dlist, next = NULL; d != NULL;
	   d = next)
	{
	  next = d->next_list;
	  if (d->built_in_index)
	    {
	      prev = d;
	    }
	  else
	    {
	      prev->next_list = next;

	      /*
	       * Make sure to turn off the cache bit or else tp_domain_free
	       * will ignore the request.
	       */
	      d->is_cached = 0;
	      tp_domain_free (d);
	    }
	}
    }

  tp_Domain_is_initialized = false;
}

/*
 * tp_get_fixed_precision - return the fixed precision of the given type.
 *    return: the fixed precision for the fixed types, otherwise -1.
 *    domain_type(in): The type of the domain
 */
int
tp_get_fixed_precision (DB_TYPE domain_type)
{
  int precision;

  switch (domain_type)
    {
    case DB_TYPE_INTEGER:
      precision = DB_INTEGER_PRECISION;
      break;
    case DB_TYPE_BIGINT:
      precision = DB_BIGINT_PRECISION;
      break;
    case DB_TYPE_DOUBLE:
      precision = DB_DOUBLE_DECIMAL_PRECISION;
      break;
    case DB_TYPE_DATE:
      precision = DB_DATE_PRECISION;
      break;
    case DB_TYPE_TIME:
      precision = DB_TIME_PRECISION;
      break;
    case DB_TYPE_DATETIME:
      precision = DB_DATETIME_PRECISION;
      break;
    default:
      precision = DB_DEFAULT_PRECISION;
      break;
    }

  return precision;
}

/*
 * tp_domain_free - free a hierarchical domain structure.
 *    return: none
 *    dom(out): domain to free
 * Note:
 *    This routine can be called for a transient or cached domain.  If
 *    the domain has been cached, the request is ignored.
 *    Note that you can only call this on the root of a domain hierarchy,
 *    you are not allowed to grab pointers into the middle of a hierarchical
 *    domain and free that.
 */
void
tp_domain_free (TP_DOMAIN * dom)
{
  TP_DOMAIN *d, *next;
#if !defined (NDEBUG)
  TP_DOMAIN *start = NULL;
#endif

  if (dom != NULL && !dom->is_cached)
    {
#if 1				/* TODO - nested set fix me */
//      assert (dom->next == NULL);
#endif

      /* NULL things that might be problems for garbage collection */
      dom->class_mop = NULL;

      /*
       * sub-domains are always completely owned by their root domain,
       * they cannot be cached anywhere else.
       */
      for (d = dom->setdomain, next = NULL; d != NULL; d = next)
	{
#if 1				/* TODO - nested set fix me */
	  if (d == d->next || d == dom
#if !defined (NDEBUG)
	      || d == start
#endif
	    )
	    {
#if !defined (NDEBUG)
	      assert (false);
#endif
	      if (er_errid () == NO_ERROR)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR,
			  1, "");
		}

	      break;
	    }
#endif

#if !defined (NDEBUG)
	  /* to trace nested set domain */
	  if (TP_IS_SET_TYPE (TP_DOMAIN_TYPE (dom)))
	    {
	      if (start == NULL)
		{
		  start = d;
		}
	    }
#endif
	  next = d->next;
	  tp_domain_free (d);
	}

      free_and_init (dom);
    }
}

/*
 * domain_init - initializes a domain structure to contain reasonable "default"
 * values.
 *    return: none
 *    domain(out): domain structure to initialize
 *    typeid(in): basic type of the domain
 * Note:
 *    Used by tp_domain_new and also in some other places
 *    where we need to quickly synthesize some transient domain structures.
 */
static void
domain_init (TP_DOMAIN * domain, DB_TYPE typeid_)
{
  TP_DOMAIN *built_in_dom;

  assert (typeid_ <= DB_TYPE_LAST);

  domain->next = NULL;
  domain->next_list = NULL;
  domain->type = PR_TYPE_FROM_ID (typeid_);
  domain->precision = 0;
  domain->scale = 0;
  domain->class_mop = NULL;
  domain->self_ref = 0;
  domain->setdomain = NULL;
  OID_SET_NULL (&domain->class_oid);

  if (TP_TYPE_HAS_COLLATION (typeid_))
    {
      domain->collation_id = LANG_COERCIBLE_COLL;
    }
  else
    {
      domain->collation_id = 0;
    }
  domain->is_cached = 0;
  domain->built_in_index = 0;

  /* use the built-in domain template to see if we're parameterized or not */
  built_in_dom = tp_domain_resolve_default (typeid_);

  domain->is_parameterized = built_in_dom->is_parameterized;

}

/*
 * tp_domain_new - returns a new initialized transient domain.
 *    return: new transient domain
 *    type(in): type id
 * Note:
 *    It is intended for use in places where domains are being created
 *    incrementally for eventual passing to tp_domain_cache.
 *    Only the type id is passed here since that is the only common
 *    piece of information shared by all domains.
 *    The contents of the domain can be filled in by the caller assuming
 *    they obey the rules.
 */
TP_DOMAIN *
tp_domain_new (DB_TYPE type)
{
  TP_DOMAIN *new_;

  new_ = (TP_DOMAIN *) malloc (sizeof (TP_DOMAIN));
  if (new_ == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, sizeof (TP_DOMAIN));

      return NULL;
    }

  domain_init (new_, type);

  assert (new_->next_list == NULL);
  assert (new_->is_cached == 0);
  assert (new_->next == NULL);

  return new_;
}


/*
 * tp_domain_construct - create a transient domain object with type, class,
 * precision, scale and setdomain.
 *    return:
 *    domain_type(in): The basic type of the domain
 *    class_obj(in): The class of the domain (for DB_TYPE_OBJECT)
 *    precision(in): The precision of the domain
 *    scale(in): The class of the domain
 *    setdomain(in): The setdomain of the domain
 * Note:
 *    Used in a few places, callers must be aware that there may be more
 *    initializations to do since not all of the domain parameters are
 *    arguments to this function.
 *
 *    The setdomain must also be a transient domain list.
 */
TP_DOMAIN *
tp_domain_construct (DB_TYPE domain_type, DB_OBJECT * class_obj,
		     int precision, int scale, TP_DOMAIN * setdomain)
{
  TP_DOMAIN *new_;
  int fixed_precision;

  switch (domain_type)
    {
    case DB_TYPE_VARCHAR:
      if (precision == DB_DEFAULT_PRECISION)
	{
	  precision = DB_MAX_VARCHAR_PRECISION;
	}
      break;
    case DB_TYPE_VARBIT:
      if (precision == DB_DEFAULT_PRECISION)
	{
	  precision = DB_MAX_VARBIT_PRECISION;
	}
      break;
    case DB_TYPE_NUMERIC:
      if (precision == DB_DEFAULT_PRECISION)
	{
	  precision = DB_DEFAULT_NUMERIC_PRECISION;
	}
      break;
    default:
      break;
    }

  new_ = tp_domain_new (domain_type);
  if (new_)
    {
      fixed_precision = tp_get_fixed_precision (domain_type);
      if (fixed_precision != DB_DEFAULT_PRECISION)
	{
#if !defined (NDEBUG)
	  if (precision != fixed_precision)
	    {
	      assert (false);
	    }
#endif
	  precision = fixed_precision;
	}

      new_->precision = precision;
      new_->scale = scale;
      new_->setdomain = setdomain;

#if !defined (NDEBUG)
      if (TP_IS_SET_TYPE (domain_type))
	{
	  TP_DOMAIN *d;
	  for (d = new_->setdomain; d != NULL; d = d->next)
	    {
	      assert (d->next_list == NULL);
	      assert (d->is_cached == 0);
	    }
	}
#endif /* NDEBUG */

      if (class_obj == (DB_OBJECT *) TP_DOMAIN_SELF_REF)
	{
	  new_->class_mop = NULL;
	  new_->self_ref = 1;
	}
      else
	{
	  new_->class_mop = class_obj;
	  new_->self_ref = 0;
	  /*
	   * For compatibility on the server side, class objects must have
	   * the oid in the domain match the oid in the class object.
	   */
	  if (class_obj)
	    {
	      COPY_OID (&(new_->class_oid), &(class_obj->ws_oid));
	    }
	}

      /*
       * have to leave the class OID uninitialized because we don't know how
       * to get an OID out of a DB_OBJECT on the server.
       * That shouldn't matter since the server side unpackers will use
       * tp_domain_new and set the domain fields directly.
       */
      assert (new_->next_list == NULL);
      assert (new_->is_cached == 0);
      assert (new_->next == NULL);
    }

  return new_;
}

/*
 * tp_domain_copy - copy a hierarcical domain structure
 *    return: new domain
 *    dom(in): domain to copy
 * Note:
 *    If the domain was cached, we simply return a handle to the cached
 *    domain, otherwise we make a full structure copy.
 *    This should only be used in a few places in the schema manager which
 *    maintains separate copies of all the attribute domains during
 *    flattening.  Could be converted to used cached domains perhaps.
 *    But the "self referencing" domain is the problem.
 *
 *    New functionality:  We make
 *    a NEW copy of the parameter domain whether it is cached or not.   This
 *    is used for updating fields of a cached domain.  We don't want to
 *    update a domain that has already been cached because multiple structures
 *    may be pointing to it.
 */
TP_DOMAIN *
tp_domain_copy (const TP_DOMAIN * domain)
{
  TP_DOMAIN *new_domain, *first, *last;
  const TP_DOMAIN *d;

  first = NULL;
  if (domain != NULL)
    {
      last = NULL;

      for (d = domain; d != NULL; d = d->next)
	{
	  new_domain = tp_domain_new (TP_DOMAIN_TYPE (d));
	  if (new_domain == NULL)
	    {
	      tp_domain_free (first);
	      return NULL;
	    }
	  else
	    {
	      /* copy over the domain parameters */
	      new_domain->class_mop = d->class_mop;
	      new_domain->class_oid = d->class_oid;
	      new_domain->precision = d->precision;
	      new_domain->scale = d->scale;
	      new_domain->collation_id = d->collation_id;
	      new_domain->self_ref = d->self_ref;
	      new_domain->is_parameterized = d->is_parameterized;

	      if (d->setdomain != NULL)
		{
		  new_domain->setdomain = tp_domain_copy (d->setdomain);
		  if (new_domain->setdomain == NULL)
		    {
		      tp_domain_free (new_domain);
		      tp_domain_free (first);
		      return NULL;
		    }
		}

	      if (first == NULL)
		{
		  first = new_domain;
		}
	      else
		{
		  last->next = new_domain;
		}
	      last = new_domain;
	    }
	}

      assert (first->is_cached == 0);
    }

  return first;
}


/*
 * tp_domain_size_internal - count the number of domains in a domain list
 *    return: number of domains in a domain list
 *    domain(in): a domain list
 */
static int
tp_domain_size_internal (const TP_DOMAIN * domain)
{
  int size = 0;

  while (domain)
    {
      ++size;
      domain = domain->next;
    }

  return size;
}

/*
 * tp_domain_size - count the number of domains in a domain list
 *    return: number of domains in a domain list
 *    domain(in): domain
 */
int
tp_domain_size (const TP_DOMAIN * domain)
{
  return tp_domain_size_internal (domain);
}

/*
 * tp_value_slam_domain - alter the domain of an existing DB_VALUE
 *    return: nothing
 *    value(out): value whose domain is to be altered
 *    domain(in): domain descriptor
 * Note:
 * used usually in a context like tp_value_coerce where we know that we
 * have a perfectly good fixed-length string that we want tocast as a varchar.
 *
 * This is a dangerous function and should not be exported to users.  use
 * only if you know exactly what you're doing!!!!
 */
static void
tp_value_slam_domain (DB_VALUE * value, const DB_DOMAIN * domain)
{
  switch (TP_DOMAIN_TYPE (domain))
    {
    case DB_TYPE_VARCHAR:
      db_string_put_cs_and_collation (value, TP_DOMAIN_COLLATION (domain));
    case DB_TYPE_VARBIT:
      value->domain.char_info.type = TP_DOMAIN_TYPE (domain);
      value->domain.char_info.length = domain->precision;
      break;

    case DB_TYPE_NUMERIC:
      value->domain.numeric_info.type = TP_DOMAIN_TYPE (domain);
      value->domain.numeric_info.precision = domain->precision;
      value->domain.numeric_info.scale = domain->scale;
      break;

    default:
      value->domain.general_info.type = TP_DOMAIN_TYPE (domain);
      break;
    }
}

/*
 * tp_domain_match - examins two domains to see if they are logically same
 *    return: non-zero if the domains are the same
 *    dom1(in): first domain
 *    dom2(in): second domain
 *    exact(in): how tolerant we are of mismatches
 */
int
tp_domain_match (const TP_DOMAIN * dom1, const TP_DOMAIN * dom2,
		 TP_MATCH exact)
{
  return tp_domain_match_internal (dom1, dom2, exact, true);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * tp_domain_match_ignore_order - examins two domains to see if they are logically same
 *    return: non-zero if the domains are the same
 *    dom1(in): first domain
 *    dom2(in): second domain
 *    exact(in): how tolerant we are of mismatches
 */
int
tp_domain_match_ignore_order (const TP_DOMAIN * dom1, const TP_DOMAIN * dom2,
			      TP_MATCH exact)
{
  return tp_domain_match_internal (dom1, dom2, exact, false);
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * tp_domain_match_internal - examins two domains to see if they are logically same
 *    return: non-zero if the domains are the same
 *    dom1(in): first domain
 *    dom2(in): second domain
 *    exact(in): how tolerant we are of mismatches
 *    match_order(in): check for asc/desc
 */
static int
tp_domain_match_internal (const TP_DOMAIN * dom1, const TP_DOMAIN * dom2,
			  TP_MATCH exact, UNUSED_ARG bool match_order)
{
  int match = 0;

  if (dom1 == NULL || dom2 == NULL)
    {
      return 0;
    }

  /* in the case where their both cached */
  if (dom1 == dom2)
    {
      return 1;
    }

  if (TP_DOMAIN_TYPE (dom1) != TP_DOMAIN_TYPE (dom2))
    {
      return 0;
    }

  /*
   * At this point, either dom1 and dom2 have exactly the same type
   */

  /* could use the new is_parameterized flag to avoid the switch ? */

  switch (TP_DOMAIN_TYPE (dom1))
    {

    case DB_TYPE_NULL:
    case DB_TYPE_INTEGER:
    case DB_TYPE_BIGINT:
    case DB_TYPE_DOUBLE:
    case DB_TYPE_TIME:
    case DB_TYPE_DATETIME:
    case DB_TYPE_DATE:
      /*
       * these domains have no parameters, they match if the types are the
       * same.
       */
      match = 1;
      break;

    case DB_TYPE_OBJECT:
    case DB_TYPE_SUB:

      /*
       * if "exact" is zero, we should be checking the subclass hierarchy of
       * dom1 to see id dom2 is in it !
       */

      /* Always prefer comparison of MOPs */
      if (dom1->class_mop != NULL && dom2->class_mop != NULL)
	{
	  match = (dom1->class_mop == dom2->class_mop);
	}
      else if (dom1->class_mop == NULL && dom2->class_mop == NULL)
	{
	  match = OID_EQ (&dom1->class_oid, &dom2->class_oid);
	}
      else
	{
	  /*
	   * We have a mixture of OID & MOPS, it probably isn't necessary to
	   * be this general but try to avoid assuming the class OIDs have
	   * been set when there is a MOP present.
	   */
	  if (dom1->class_mop == NULL)
	    {
	      match = OID_EQ (&dom1->class_oid, WS_OID (dom2->class_mop));
	    }
	  else
	    {
	      match = OID_EQ (WS_OID (dom1->class_mop), &dom2->class_oid);
	    }
	}

      if (match == 0 && exact == TP_SET_MATCH
	  && dom1->class_mop == NULL && OID_ISNULL (&dom1->class_oid))
	{
	  match = 1;
	}
      break;

    case DB_TYPE_VARIABLE:
    case DB_TYPE_SEQUENCE:
#if 1
      /* >>>>> NEED MORE CONSIDERATION <<<<<
       * do not check order
       * must be rollback with tp_domain_add()
       */
      if (dom1->setdomain == dom2->setdomain)
	{
	  match = 1;
	}
      else
	{
	  int dsize;

	  /* don't bother comparing the lists unless the sizes are the same */
	  dsize = tp_domain_size (dom1->setdomain);
	  if (dsize == tp_domain_size (dom2->setdomain))
	    {
	      /* handle the simple single domain case quickly */
	      if (dsize == 1)
		{
		  match = tp_domain_match (dom1->setdomain, dom2->setdomain,
					   exact);
		}
	      else
		{
		  TP_DOMAIN *d1, *d2;

		  match = 1;
		  for (d1 = dom1->setdomain, d2 = dom2->setdomain;
		       d1 != NULL && d2 != NULL; d1 = d1->next, d2 = d2->next)
		    {
#if 1				/* remove nested set */
		      assert (!TP_IS_SET_TYPE (TP_DOMAIN_TYPE (d1)));
		      assert (!TP_IS_SET_TYPE (TP_DOMAIN_TYPE (d2)));
#endif
		      if (!tp_domain_match (d1, d2, exact))
			{
			  match = 0;
			  break;	/* immediately exit for loop */
			}
		    }
		}
	    }
	}
#else /* 0 */
      if (dom1->setdomain == dom2->setdomain)
	{
	  match = 1;
	}
      else
	{
	  int dsize;

	  /* don't bother comparing the lists unless the sizes are the same */
	  dsize = tp_domain_size (dom1->setdomain);
	  if (dsize == tp_domain_size (dom2->setdomain))
	    {

	      /* handle the simple single domain case quickly */
	      if (dsize == 1)
		{
		  match = tp_domain_match (dom1->setdomain, dom2->setdomain,
					   exact);
		}
	      else
		{
		  TP_DOMAIN *d1, *d2;

		  /* clear the visited flag in the second subdomain list */
		  for (d2 = dom2->setdomain; d2 != NULL; d2 = d2->next)
		    {
		      d2->is_visited = 0;
		    }

		  match = 1;
		  for (d1 = dom1->setdomain;
		       d1 != NULL && match; d1 = d1->next)
		    {
		      for (d2 = dom2->setdomain; d2 != NULL; d2 = d2->next)
			{
			  if (!d2->is_visited
			      && tp_domain_match (d1, d2, exact))
			    {
			      break;
			    }
			}
		      /* did we find the domain in the other list ? */
		      if (d2 != NULL)
			{
			  d2->is_visited = 1;
			}
		      else
			{
			  match = 0;
			}
		    }
		}
	    }
	}
#endif /* 1 */
      break;

    case DB_TYPE_VARCHAR:
      if (dom1->collation_id != dom2->collation_id)
	{
	  match = 0;
	  break;
	}
      /* fall through */
    case DB_TYPE_VARBIT:
      if (exact == TP_EXACT_MATCH || exact == TP_SET_MATCH)
	{
	  match = dom1->precision == dom2->precision;
	}
      else if (exact == TP_STR_MATCH)
	{
	  /*
	   * Allow the match if the precisions would allow us to reuse the
	   * string without modification.
	   */
	  match = (dom1->precision >= dom2->precision);
	}
      else
	{
	  /*
	   * Allow matches regardless of precision, let the actual length of the
	   * value determine if it can be assigned.  This is important for
	   * literal strings as their precision will be the maximum but they
	   * can still be assigned to domains with a smaller precision
	   * provided the actual value is within the destination domain
	   * tolerance.
	   */
	  assert (false);

	  match = 1;
	}
      break;

    case DB_TYPE_NUMERIC:
      /*
       * note that we never allow inexact matches here because the
       * mr_setmem_numeric function is not currently able to perform the
       * deferred coercion.
       */
      match = ((dom1->precision == dom2->precision)
	       && (dom1->scale == dom2->scale));
      break;

    case DB_TYPE_OID:
      /*
       * These are internal domains, they shouldn't be seen, in case they are,
       * just let them match without parameters.
       */
      match = 1;
      break;

    case DB_TYPE_RESULTSET:
    case DB_TYPE_TABLE:
      break;
      /* don't have a default so we make sure to add clauses for all types */
    }

  return match;
}

/*
 * tp_domain_get_list - get the head of domain list
 *    return: the head of the list
 *    type(in): type of value
 */
static TP_DOMAIN *
tp_domain_get_list (DB_TYPE type)
{
  if (type >= sizeof (tp_Domains) / sizeof (tp_Domains[0]))
    {
      assert_release (false);
      return NULL;
    }

#if 1				/* TODO - */
  assert ((type == DB_TYPE_NULL
	   || type == DB_TYPE_INTEGER
	   || type == DB_TYPE_DOUBLE
	   || type == DB_TYPE_VARCHAR
	   || type == DB_TYPE_OBJECT
	   || type == DB_TYPE_SEQUENCE
	   || type == DB_TYPE_TIME
	   || type == DB_TYPE_DATE
	   || type == DB_TYPE_VARIABLE
	   || type == DB_TYPE_OID
	   || type == DB_TYPE_NUMERIC
	   || type == DB_TYPE_VARBIT
	   || type == DB_TYPE_BIGINT
	   || type == DB_TYPE_DATETIME)
	  || (tp_Domains[type] == &tp_Null_domain));
#endif

  return tp_Domains[type];
}

/*
 * tp_is_domain_cached - find matching domain from domain list
 *    return: matched domain
 *    dlist(in): domain list
 *    transient(in): transient domain
 *    exact(in): matching level
 *    ins_pos(out): domain found
 * Note:
 * DB_TYPE_VARCHAR, DB_TYPE_VARBIT : precision's desc order
 *                           others: precision's asc order
 */
static TP_DOMAIN *
tp_is_domain_cached (TP_DOMAIN * dlist, TP_DOMAIN * transient, TP_MATCH exact,
		     TP_DOMAIN ** ins_pos)
{
  TP_DOMAIN *domain = dlist;
  int match = 0;

  /* in the case where their both cached */
  if (domain == transient)
    {
      return domain;
    }

  if (TP_DOMAIN_TYPE (domain) != TP_DOMAIN_TYPE (transient))
    {
      return NULL;
    }

  *ins_pos = domain;

  /*
   * At this point, either domain and transient have exactly the same type
   */

  while (domain != NULL)
    {
      match = tp_domain_match_internal (domain, transient, exact, true);
      if (match)
	{
	  return domain;	/* found */
	}

      switch (TP_DOMAIN_TYPE (domain))
	{
	case DB_TYPE_NULL:
	case DB_TYPE_INTEGER:
	case DB_TYPE_BIGINT:
	case DB_TYPE_DOUBLE:
	case DB_TYPE_TIME:
	case DB_TYPE_DATETIME:
	case DB_TYPE_DATE:
	  assert (false);
	  domain = NULL;	/* give up */
	  break;

	case DB_TYPE_OBJECT:
	case DB_TYPE_SUB:
	case DB_TYPE_VARIABLE:
	case DB_TYPE_SEQUENCE:
	  *ins_pos = domain;
	  domain = domain->next_list;
	  break;

	case DB_TYPE_VARCHAR:
	case DB_TYPE_VARBIT:
	  /* check for descending order */
	  if (domain->precision < transient->precision)
	    {
	      domain = NULL;	/* not found */
	    }
	  else
	    {
	      *ins_pos = domain;
	      domain = domain->next_list;
	    }
	  break;

	case DB_TYPE_NUMERIC:
	  assert (domain->precision != transient->precision
		  || domain->scale != transient->scale);
	  /*
	   * The first domain is a default domain for numeric type,
	   * actually NUMERIC(15,0)
	   */
          assert (DB_DOUBLE_DECIMAL_PRECISION == 15);
          assert (DB_DEFAULT_NUMERIC_SCALE == 0);
	  if (domain->precision == DB_DOUBLE_DECIMAL_PRECISION
	      || domain->scale == DB_DEFAULT_NUMERIC_SCALE)
	    {
	      *ins_pos = domain;
	      domain = domain->next_list;
	    }
	  else
	    {
              /*
	       * The other domains for numeric values are sorted
	       * by descending order of precision and scale.
	       */
	      if ((domain->precision < transient->precision)
		  || ((domain->precision == transient->precision)
		      && (domain->scale < transient->scale)))
		{
		  domain = NULL;	/* not found */
		}
	      else
		{
		  *ins_pos = domain;
		  domain = domain->next_list;
		}
	    }
	  break;

	case DB_TYPE_OID:
	case DB_TYPE_RESULTSET:
	case DB_TYPE_TABLE:
	  assert (false);
	  domain = NULL;	/* give up */
	  break;

	  /* don't have a default so we make sure to add clauses for all types */
	}
    }				/* while */

  /* not found */

  return NULL;
}

#if !defined (SERVER_MODE)
/*
 * tp_swizzle_oid - swizzle oid of a domain class recursively
 *    return: void
 *    domain(in): domain to swizzle
 * Note:
 *   If the code caching the domain was written for the server, we will
 *   only have the OID of the class here if this is an object domain.  If
 *   the domain table is being shared by the client and server (e.g. in
 *   standalone mode), it is important that we "swizzle" the OID into
 *   a corresponding workspace MOP during the cache.  This ensures that we
 *   never get an object domain entered into the client's domain table that
 *   doesn't have a real DB_OBJECT pointer for the domain class.  There is
 *   a lot of code that expects this to be the case.
 */
static void
tp_swizzle_oid (TP_DOMAIN * domain)
{
  TP_DOMAIN *d;
  DB_TYPE type;

  type = TP_DOMAIN_TYPE (domain);

  if ((type == DB_TYPE_OBJECT || type == DB_TYPE_OID)
      && domain->class_mop == NULL && !OID_ISNULL (&domain->class_oid))
    {
      /* swizzle the pointer if we're on the client */
      domain->class_mop = ws_mop (&domain->class_oid, NULL);
    }
  else if (TP_IS_SET_TYPE (type))
    {
      for (d = domain->setdomain; d != NULL; d = d->next)
	{
	  tp_swizzle_oid (d);
	}
    }
}
#endif /* !SERVER_MODE */

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * tp_domain_find_noparam - get domain for give type
 *    return: domain
 *    type(in): domain type
 */
TP_DOMAIN *
tp_domain_find_noparam (DB_TYPE type)
{
  TP_DOMAIN *dom;

  /* tp_domain_find_with_no_param */
  /* type :
     DB_TYPE_NULL         DB_TYPE_INTEGER
     DB_TYPE_DOUBLE       DB_TYPE_TIME
     DB_TYPE_DATE         DB_TYPE_DATETIME
     DB_TYPE_BIGINT
   */

  dom = tp_domain_get_list (type);

  return dom;
}
#endif

/*
 * tp_domain_find_numeric - find domain for given type, precision and scale
 *    return: domain that matches
 *    type(in): DB_TYPE
 *    precision(in): precision
 *    scale(in): scale
 */
TP_DOMAIN *
tp_domain_find_numeric (DB_TYPE type, int precision, int scale)
{
  TP_DOMAIN *dom;

  /* tp_domain_find_with_precision_scale */
  /* type : DB_TYPE_NUMERIC */
  assert (type == DB_TYPE_NUMERIC);

  /*
   * The first domain is a default domain for numeric type,
   * actually NUMERIC(15,0). We try to match it first.
   */
  dom = tp_domain_get_list (type);
  if (precision == dom->precision && scale == dom->scale)
    {
      return dom;
    }

  /* search the list for a domain that matches */
  for (dom = dom->next_list; dom != NULL; dom = dom->next_list)
    {
      if ((precision > dom->precision)
	  || ((precision == dom->precision) && (scale > dom->scale)))
	{
	  return NULL;		/* not exist */
	}

      /* we MUST perform exact matches here */
      if (dom->precision == precision && dom->scale == scale)
	{
	  break;		/* found */
	}
    }

  return dom;
}

/*
 * tp_domain_find_charbit - find domain for given codeset and precision
 *    return: domain that matches
 *    type(in): DB_TYPE
 *    collation_id(in): collation id
 *    precision(in): precision
 */
TP_DOMAIN *
tp_domain_find_charbit (DB_TYPE type, int collation_id, int precision)
{
  TP_DOMAIN *dom = NULL;

  /* tp_domain_find_with_codeset_precision */
  /*
   * type :
   * DB_TYPE_VARCHAR DB_TYPE_VARBIT
   */
  assert (type == DB_TYPE_VARCHAR || type == DB_TYPE_VARBIT);

  if (type == DB_TYPE_VARCHAR || type == DB_TYPE_VARBIT)
    {
      /* search the list for a domain that matches */
      for (dom = tp_domain_get_list (type); dom != NULL; dom = dom->next_list)
	{
	  /* Variable character/bit is sorted in descending order of precision. */
	  if (precision > dom->precision)
	    {
	      return NULL;	/* not exist */
	    }

	  /* we MUST perform exact matches here */
	  if (dom->precision == precision)
	    {
	      if (type == DB_TYPE_VARBIT)
		{
		  break;	/* found */
		}
	      else if (dom->collation_id == collation_id)
		{
		  /* codeset should be the same if collations are equal */
		  break;
		}
	    }
	}
    }

  return dom;
}

#if 0
/*
 * tp_domain_find_object - find domain for given class OID and class
 *    return: domain that matches
 *    type(in): DB_TYPE
 *    class_oid(in): class oid
 *    class_mop(in): class structure
 */
TP_DOMAIN *
tp_domain_find_object (DB_TYPE type, OID * class_oid,
		       struct db_object * class_mop)
{
  TP_DOMAIN *dom;

  /* tp_domain_find_with_classinfo */

  /* search the list for a domain that matches */
  for (dom = tp_domain_get_list (type); dom != NULL; dom = dom->next_list)
    {
      /* we MUST perform exact matches here */

      /* Always prefer comparison of MOPs */
      if (dom->class_mop != NULL && class_mop != NULL)
	{
	  if (dom->class_mop == class_mop)
	    {
	      break;		/* found */
	    }
	}
      else if (dom->class_mop == NULL && class_mop == NULL)
	{
	  if (OID_EQ (&dom->class_oid, class_oid))
	    {
	      break;		/* found */
	    }
	}
      else
	{
	  /*
	   * We have a mixture of OID & MOPS, it probably isn't necessary to be
	   * this general but try to avoid assuming the class OIDs have been set
	   * when there is a MOP present.
	   */
	  if (dom->class_mop == NULL)
	    {
	      if (OID_EQ (&dom->class_oid, WS_OID (class_mop)))
		{
		  break;	/* found */
		}
	    }
	  else
	    {
	      if (OID_EQ (WS_OID (dom->class_mop), class_oid))
		{
		  break;	/* found */
		}
	    }
	}
    }

  return dom;
}
#endif

/*
 * tp_domain_find_set - find domain that matches for given set domain
 *    return: domain that matches
 *    type(in): DB_TYPE
 *    setdomain(in): set domain
 */
TP_DOMAIN *
tp_domain_find_set (DB_TYPE type, TP_DOMAIN * setdomain)
{
  TP_DOMAIN *dom;
  int dsize;
  int src_dsize;

  if (type != DB_TYPE_SEQUENCE)
    {
      assert (false);
      return NULL;
    }

  src_dsize = tp_domain_size (setdomain);

  /* search the list for a domain that matches */
  for (dom = tp_domain_get_list (type); dom != NULL; dom = dom->next_list)
    {
      /* we MUST perform exact matches here */
      if (dom->setdomain == setdomain)
	{
	  break;
	}

      /* don't bother comparing the lists unless the sizes are the same */
      dsize = tp_domain_size (dom->setdomain);
      if (dsize == src_dsize)
	{
	  /* handle the simple single domain case quickly */
	  if (dsize == 1)
	    {
	      if (tp_domain_match (dom->setdomain, setdomain, TP_EXACT_MATCH))
		{
		  break;
		}
	    }
	  else
	    {
	      TP_DOMAIN *d1, *d2;
	      int match, i;

	      if (dsize == src_dsize)
		{
		  match = 1;
		  d1 = dom->setdomain;
		  d2 = setdomain;

		  for (i = 0; i < dsize; i++)
		    {
		      match = tp_domain_match (d1, d2, TP_EXACT_MATCH);
		      if (match == 0)
			{
			  break;
			}
		      d1 = d1->next;
		      d2 = d2->next;
		    }
		  if (match == 1)
		    {
		      break;
		    }
		}
	    }
	}
    }

  return dom;
}

/*
 * tp_domain_cache - caches a transient domain
 *    return: cached domain
 *    transient(in/out): transient domain
 * Note:
 *    If the domain has already been cached, it is located and returned.
 *    Otherwise, a new domain is cached and returned.
 *    In either case, the transient domain may be freed so you should never
 *    depend on it being valid after this function returns.
 *
 *    Note that if a new domain is added to the list, it is always appended
 *    to the end.  It is vital that the deafult "built-in" domain be
 *    at the head of the domain lists in tp_Domains.
 */
TP_DOMAIN *
tp_domain_cache (TP_DOMAIN * transient)
{
  TP_DOMAIN *domain, *dlist;
  TP_DOMAIN *ins_pos = NULL;
#if defined (SERVER_MODE)
  UNUSED_VAR int rv;
#endif /* SERVER_MODE */

  /* guard against a bad transient domain */
  if (transient == NULL || transient->type == NULL)
    {
      return NULL;
    }

  assert (transient->next == NULL);

  /* return this domain if its already cached */
  if (transient->is_cached)
    {
      return transient;
    }

#if !defined (SERVER_MODE)
  /* see comments for tp_swizzle_oid */
  tp_swizzle_oid (transient);
#endif /* !SERVER_MODE */

  /*
   * first search stage: NO LOCK
   */
  /* locate the root of the cache list for domains of this type */
  dlist = tp_domain_get_list (TP_DOMAIN_TYPE (transient));

  /* search the list for a domain that matches */
  if (dlist != NULL)
    {
      ins_pos = NULL;
      domain = tp_is_domain_cached (dlist, transient, TP_EXACT_MATCH,
				    &ins_pos);
      if (domain != NULL)
	{
	  /*
	   * We found one in the cache, free the supplied domain and return
	   * the cached one
	   */
	  tp_domain_free (transient);
	  return domain;
	}
    }

  /*
   * second search stage: LOCK
   */
#if defined (SERVER_MODE)
  rv = pthread_mutex_lock (&tp_domain_cache_lock);	/* LOCK */

  /* locate the root of the cache list for domains of this type */
  dlist = tp_domain_get_list (TP_DOMAIN_TYPE (transient));

  /* search the list for a domain that matches */
  if (dlist != NULL)
    {
      ins_pos = NULL;
      domain = tp_is_domain_cached (dlist, transient, TP_EXACT_MATCH,
				    &ins_pos);
      if (domain != NULL)
	{
	  /*
	   * We found one in the cache, free the supplied domain and return
	   * the cached one
	   */
	  tp_domain_free (transient);
	  pthread_mutex_unlock (&tp_domain_cache_lock);
	  return domain;
	}
    }
#endif /* SERVER_MODE */

  /*
   * We couldn't find one, install the transient domain that was passed in.
   * Since by far the most common domain match is going to be the built-in
   * domain at the head of the list, append new domains to the end of the
   * list as they are encountered.
   */
  transient->is_cached = 1;

  if (dlist)
    {
      if (ins_pos)
	{
	  TP_DOMAIN *tmp;

	  tmp = ins_pos->next_list;
	  ins_pos->next_list = transient;
	  transient->next_list = tmp;
	}
    }
  else
    {
      dlist = transient;
    }

  domain = transient;

#if !defined (NDEBUG)
  if (domain != NULL)
    {
      assert (domain->next == NULL);
    }
#endif

#if defined (SERVER_MODE)
  pthread_mutex_unlock (&tp_domain_cache_lock);
#endif /* SERVER_MODE */

  return domain;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * tp_domain_resolve - Find a domain object that matches the type, class,
 * precision, scale and setdomain.
 *    return: domain found
 *    domain_type(in): The basic type of the domain
 *    class_obj(in): The class of the domain (for DB_TYPE_OBJECT)
 *    precision(in): The precision of the domain
 *    scale(in): The class of the domain
 *    setdomain(in): The setdomain of the domain
 *    collation(in): The collation of domain
 * Note:
 *    Current implementation just creates a new one then returns it.
 */
TP_DOMAIN *
tp_domain_resolve (DB_TYPE domain_type, DB_OBJECT * class_obj,
		   int precision, int scale, TP_DOMAIN * setdomain,
		   int collation)
{
  TP_DOMAIN *d;

  d = tp_domain_new (domain_type);
  if (d != NULL)
    {
      d->precision = precision;
      d->scale = scale;
      d->class_mop = class_obj;
      d->setdomain = setdomain;
      if (TP_TYPE_HAS_COLLATION (domain_type))
	{
	  LANG_COLLATION *lc;
	  d->collation_id = collation;

	  lc = lang_get_collation (collation);
	}

      d = tp_domain_cache (d);
    }

  return d;
}
#endif

/*
 * tp_domain_resolve_default - returns the built-in "default" domain for a
 * given primitive type
 *    return: cached domain
 *    type(in): type id
 * Note:
 *    This is used only in special cases where we need to get quickly to
 *    a built-in domain without worrying about domain parameters.
 *    Note that this relies on the fact that the built-in domain is at
 *    the head of our domain lists.
 */
TP_DOMAIN *
tp_domain_resolve_default (DB_TYPE type)
{
  return tp_domain_get_list (type);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * tp_domain_resolve_default_w_coll -
 *
 *    return: cached domain
 *    type(in): type id
 *    coll_id(in): collation
 * Note:
 *  It returns a special domain having the desired collation and collation
 *  mode flag. Use this in context of type inference for argument coercion
 */
TP_DOMAIN *
tp_domain_resolve_default_w_coll (DB_TYPE type, int coll_id)
{
  TP_DOMAIN *default_dom;
  TP_DOMAIN *resolved_dom;

  default_dom = tp_domain_resolve_default (type);

  if (TP_TYPE_HAS_COLLATION (type))
    {
      resolved_dom = tp_domain_copy (default_dom);
      resolved_dom->collation_id = coll_id;
      resolved_dom = tp_domain_cache (resolved_dom);
    }
  else
    {
      resolved_dom = default_dom;
    }

  return resolved_dom;
}
#endif

/*
 * tp_domain_resolve_value - Find a domain object that describes the type info
 * in the DB_VALUE.
 *    return: domain found
 *    val(in): A DB_VALUE for which we need to obtain a domain
 */
TP_DOMAIN *
tp_domain_resolve_value (const DB_VALUE * val)
{
  TP_DOMAIN *domain;
  DB_TYPE value_type;
  int precision;
  int scale;

  domain = NULL;
  value_type = DB_VALUE_DOMAIN_TYPE (val);

  if (TP_IS_SET_TYPE (value_type))
    {
      DB_SET *set;
      /*
       * For sets, just return the domain attached to the set since it
       * will already have been cached.
       */
      set = db_get_set (val);
      if (set != NULL)
	{
	  domain = set_get_domain (set);

	  /* handle case of incomplete set domain: build full domain */
	  if (domain->setdomain == NULL
	      || tp_domain_check (domain, val,
				  TP_EXACT_MATCH) != DOMAIN_COMPATIBLE)
	    {
	      if (domain->is_cached)
		{
		  domain = tp_domain_new (value_type);
		}

	      if (domain != NULL)
		{
		  int err_status;

		  err_status =
		    setobj_build_domain_from_col (set->set,
						  &(domain->setdomain));
		  if (err_status != NO_ERROR && !domain->is_cached)
		    {
		      tp_domain_free (domain);
		      domain = NULL;
		    }
		  else
		    {
		      /* cache this new domain */
		      domain = tp_domain_cache (domain);
		    }
		}
	    }
	}
      else
	{
	  /* we need to synthesize a wildcard set domain for this value */
	  domain = tp_domain_resolve_default (value_type);
	}
    }
  else
    {
      switch (value_type)
	{
	case DB_TYPE_NULL:
	case DB_TYPE_INTEGER:
	case DB_TYPE_BIGINT:
	case DB_TYPE_DOUBLE:
	case DB_TYPE_TIME:
	case DB_TYPE_DATE:
	case DB_TYPE_DATETIME:
	  /* domains without parameters, return the built-in domain */
	  domain = tp_domain_resolve_default (value_type);
	  break;

	case DB_TYPE_OBJECT:
	case DB_TYPE_OID:
	  domain = &tp_Object_domain;
	  break;

	case DB_TYPE_VARCHAR:
	case DB_TYPE_VARBIT:
	  precision = DB_VALUE_PRECISION (val);

#if !defined(NDEBUG)
	  if (!DB_IS_NULL (val) && precision < DB_GET_STRING_LENGTH (val))
	    {
	      assert (false);
	      return NULL;
	    }
#endif

	  /*
	   * Convert references to the "floating" precisions to actual
	   * precisions.  This may not be necessary or desireable?
	   * Zero seems to pop up occasionally in DB_VALUE precisions, until
	   * this is fixed, treat it as the floater for the variable width
	   * types.
	   */
	  if (value_type == DB_TYPE_VARCHAR)
	    {
	      if (precision == 0 || precision == TP_FLOATING_PRECISION_VALUE
		  || precision > DB_MAX_VARCHAR_PRECISION)
		{
		  precision = DB_MAX_VARCHAR_PRECISION;
		}
	    }
	  else
	    {
	      assert (value_type == DB_TYPE_VARBIT);
	      if (precision == 0 || precision == TP_FLOATING_PRECISION_VALUE
		  || precision > DB_MAX_VARBIT_PRECISION)
		{
		  precision = DB_MAX_VARBIT_PRECISION;
		}
	    }

	  domain = tp_domain_find_charbit (value_type,
					   db_get_string_collation (val),
					   precision);
	  if (domain == NULL)
	    {
	      /* must find one with a matching precision */
	      domain = tp_domain_new (value_type);
	      if (domain == NULL)
		{
		  return NULL;
		}

	      domain->collation_id = db_get_string_collation (val);
	      domain->precision = precision;

	      domain = tp_domain_cache (domain);
	    }
	  break;

	case DB_TYPE_NUMERIC:
	  precision = DB_VALUE_PRECISION (val);
	  scale = DB_VALUE_SCALE (val);

	  /*
	   * Hack, precision seems to be commonly -1 DB_VALUES, turn this into
	   * the default "maximum" precision.
	   * This may not be necessary any more.
	   */
	  if (precision == -1)
	    {
	      precision = DB_DEFAULT_NUMERIC_PRECISION;
	    }

	  if (scale == -1)
	    {
	      scale = DB_DEFAULT_NUMERIC_SCALE;
	    }

	  domain = tp_domain_find_numeric (value_type, precision, scale);
	  if (domain == NULL)
	    {
	      /* must find one with a matching precision and scale */
	      domain = tp_domain_new (value_type);
	      if (domain == NULL)
		{
		  return NULL;
		}

	      domain->precision = precision;
	      domain->scale = scale;

	      domain = tp_domain_cache (domain);
	    }
	  break;

	case DB_TYPE_SUB:
	case DB_TYPE_VARIABLE:
	  /*
	   * These are internal domains, they shouldn't be seen, in case they
	   * are, match to a built-in
	   */
	  domain = tp_domain_resolve_default (value_type);
	  break;

	  /*
	   * things handled in logic outside the switch, shuts up compiler
	   * warnings
	   */
	case DB_TYPE_SEQUENCE:
	  break;
	case DB_TYPE_RESULTSET:
	case DB_TYPE_TABLE:
	  break;
	}
    }

#if !defined (NDEBUG)
  if (domain != NULL)
    {
      assert (domain->is_cached);
    }
#endif

  return domain;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * tp_create_domain_resolve_value - adjust domain of a DB_VALUE with respect to
 * the primitive value of the value
 *    return: domain
 *    val(in): DB_VALUE
 *    domain(in): domain
 * Note: val->domain changes
 */
TP_DOMAIN *
tp_create_domain_resolve_value (DB_VALUE * val, TP_DOMAIN * domain)
{
  DB_TYPE value_type;

  value_type = DB_VALUE_DOMAIN_TYPE (val);

  switch (value_type)
    {
    case DB_TYPE_INTEGER:
    case DB_TYPE_BIGINT:
    case DB_TYPE_DOUBLE:
    case DB_TYPE_TIME:
    case DB_TYPE_DATE:
    case DB_TYPE_DATETIME:
    case DB_TYPE_OBJECT:
    case DB_TYPE_OID:
      break;

    case DB_TYPE_VARCHAR:
    case DB_TYPE_VARBIT:
      if (DB_VALUE_PRECISION (val) == TP_FLOATING_PRECISION_VALUE)
	{
	  /* Check for floating precision. */
	  val->domain.char_info.length = domain->precision;
	}
      else
	{
	  if (domain->precision == TP_FLOATING_PRECISION_VALUE)
	    {
	      ;			/* nop */
	    }
	  else
	    {
	      if (DB_VALUE_PRECISION (val) > domain->precision)
		{
		  val->domain.char_info.length = domain->precision;
		}
	    }
	}
      break;

    case DB_TYPE_NUMERIC:
      break;

    case DB_TYPE_NULL:		/* for idxkey elements */
      break;

    default:
      return NULL;
    }

  /* if(domain) return tp_domain_cache(domain); */
  return domain;
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * tp_domain_add - Adds a domain structure to a domain list if it doesn't
 * already exist.
 *    return: error code
 *    dlist(in/out): domain list
 *    domain(in): domain structure
 * Note:
 *    This routine should only be used to construct a transient domain.
 *    Note that there are no error messages if a duplicate isn't added.
 */
int
tp_domain_add (TP_DOMAIN ** dlist, TP_DOMAIN * domain)
{
  int error = NO_ERROR;
  TP_DOMAIN *d, *found, *last;
  UNUSED_VAR DB_TYPE type_id;

  last = NULL;
  type_id = TP_DOMAIN_TYPE (domain);

  for (d = *dlist, found = NULL; d != NULL && found == NULL; d = d->next)
    {
#if 1
      /* >>>>> NEED MORE CONSIDERATION <<<<<
       * do not check duplication
       * must be rollback with tp_domain_match()
       */
#else /* 0 */
      if (TP_DOMAIN_TYPE (d) == type_id)
	{
	  switch (type_id)
	    {
	    case DB_TYPE_INTEGER:
	    case DB_TYPE_BIGINT:
	    case DB_TYPE_DOUBLE:
	    case DB_TYPE_TIME:
	    case DB_TYPE_DATE:
	    case DB_TYPE_DATETIME:
	    case DB_TYPE_SUB:
	    case DB_TYPE_OID:
	    case DB_TYPE_NULL:
	    case DB_TYPE_VARIABLE:
	    case DB_TYPE_SEQUENCE:
	    case DB_TYPE_VARCHAR:
	    case DB_TYPE_VARBIT:
	      found = d;
	      break;

	    case DB_TYPE_NUMERIC:
	      if ((d->precision == domain->precision)
		  && (d->scale == domain->scale))
		{
		  found = d;
		}
	      break;

	    case DB_TYPE_OBJECT:
	      if (d->class_mop == domain->class_mop)
		{
		  found = d;
		}
	      break;

	    default:
	      break;
	    }
	}
#endif /* 1 */

      last = d;
    }

  if (found == NULL)
    {
      if (last == NULL)
	{
	  *dlist = domain;
	}
      else
	{
	  last->next = domain;
	}
    }
  else
    {
      /* the domain already existed, free the supplied domain */
      tp_domain_free (domain);
    }

  return error;
}

/*
 * tp_domain_attach - concatenate two domains
 *    return: concatenated domain
 *    dlist(out): domain 1
 *    domain(in): domain 2
 */
int
tp_domain_attach (TP_DOMAIN ** dlist, TP_DOMAIN * domain)
{
  int error = NO_ERROR;
  TP_DOMAIN *d;

  d = *dlist;

  if (*dlist == NULL)
    {
      *dlist = domain;
    }
  else
    {
      while (d->next)
	{
	  d = d->next;
	}

      d->next = domain;
    }

  return error;
}

#if defined(ENABLE_UNUSED_FUNCTION)
#if !defined (SERVER_MODE)
/*
 * tp_domain_drop - Removes a domain from a list if it was found.
 *    return: non-zero if domain was dropped
 *    dlist(in/out): domain list
 *    domain(in/out):  domain class
 * Note:
 *    This routine should only be used to modify a transient domain.
 */
int
tp_domain_drop (TP_DOMAIN ** dlist, TP_DOMAIN * domain)
{
  TP_DOMAIN *d, *found, *prev;
  int dropped = 0;
  DB_TYPE type_id;

  type_id = TP_DOMAIN_TYPE (domain);
  for (d = *dlist, prev = NULL, found = NULL; d != NULL && found == NULL;
       d = d->next)
    {
      if (TP_DOMAIN_TYPE (d) == type_id)
	{
	  switch (type_id)
	    {
	    case DB_TYPE_INTEGER:
	    case DB_TYPE_BIGINT:
	    case DB_TYPE_DOUBLE:
	    case DB_TYPE_TIME:
	    case DB_TYPE_DATE:
	    case DB_TYPE_DATETIME:
	    case DB_TYPE_SUB:
	    case DB_TYPE_OID:
	    case DB_TYPE_NULL:
	    case DB_TYPE_VARIABLE:
	    case DB_TYPE_SEQUENCE:
	    case DB_TYPE_VARCHAR:
	    case DB_TYPE_VARBIT:
	      found = d;
	      break;

	    case DB_TYPE_NUMERIC:
	      if (d->precision == domain->precision
		  && d->scale == domain->scale)
		{
		  found = d;
		}
	      break;

	    case DB_TYPE_OBJECT:
	      if (d->class_mop == domain->class_mop)
		{
		  found = d;
		}
	      break;

	    default:
	      break;
	    }
	}

      if (found == NULL)
	{
	  prev = d;
	}
    }

  if (found != NULL)
    {
      if (prev == NULL)
	{
	  *dlist = found->next;
	}
      else
	{
	  prev->next = found->next;
	}

      found->next = NULL;
      tp_domain_free (found);

      dropped = 1;
    }

  return dropped;
}
#endif /* !SERVER_MODE */
#endif

/*
 * tp_domain_filter_list - filter out any domain references to classes that
 * have been deleted or are otherwise invalid from domain list
 *    return: non-zero if changes were made
 *    dlist():  domain list
 * Note:
 *    The semantic for deleted classes is that the domain reverts
 *    to the root "object" domain, thereby allowing all object references.
 *    This could become more sophisticated but not without a lot of extra
 *    bookkeeping in the database.   If a domain is downgraded to "object",
 *    be sure to remove it from the list entirely if there is already an
 *    "object" domain present.
 */
int
tp_domain_filter_list (TP_DOMAIN * dlist)
{
  TP_DOMAIN *d, *next;
  UNUSED_VAR TP_DOMAIN *prev;
  int changes;
  UNUSED_VAR int has_object;

  has_object = changes = 0;

  for (d = dlist, prev = NULL, next = NULL; d != NULL; d = next)
    {
      next = d->next;

      /* domain is still valid, see if its "object" */
      if (d->type == tp_Type_object && d->class_mop == NULL)
	{
	  has_object = 1;
	}
      else if (pr_is_set_type (TP_DOMAIN_TYPE (d)) && d->setdomain != NULL)
	{
	  /* recurse on set domain list */
	  changes = tp_domain_filter_list (d->setdomain);
	}
      prev = d;
    }

  return changes;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * tp_domain_find_compatible - two domains are compatible for the purposes of
 * assignment of values.
 *    return: non-zero if domains are compatible
 *    src(in): domain we're wanting to assign
 *    dest(in): domain we're trying to go into
 * Note:
 *    Domains are compatible if they are equal.
 *    Further, domain 1 is compatible with domain 2 if domain 2 is more
 *    general.
 *
 *    This will not properly detect of the domains are compatible due
 *    to a proper subclass superclass relationship between object domains.
 *    It will only check to see if the class matches exactly.
 *
 *    This is the function used to test to see if a particular set domain
 *    is "within" another set domain during assignment of set values to
 *    attributes.  src in this case will be the domain of the set we were
 *    given and dest will be the domain of the attribute.
 *    All of the sub-domains in src must also be found in dest.
 *
 *    This is somewhat different than tp_domain_match because the comparison
 *    of set domains is more of an "is it a subset" operation rather than
 *    an "is it equal to" operation.
 */
static const TP_DOMAIN *
tp_domain_find_compatible (const TP_DOMAIN * src, const TP_DOMAIN * dest)
{
  const TP_DOMAIN *d, *found;

  found = NULL;

  /*
   * If we have a hierarchical domain, perform a lenient "superset" comparison
   * rather than an exact match.
   */
  if (TP_IS_SET_TYPE (TP_DOMAIN_TYPE (src))
      || TP_DOMAIN_TYPE (src) == DB_TYPE_VARIABLE)
    {
      for (d = dest; d != NULL && found == NULL; d = d->next)
	{
	  if (TP_DOMAIN_TYPE (src) == TP_DOMAIN_TYPE (d)
	      && tp_domain_compatible (src->setdomain, dest->setdomain))
	    {
	      found = d;
	    }
	}
    }
  else
    {

      for (d = dest; d != NULL && found == NULL; d = d->next)
	{
	  if (tp_domain_match ((TP_DOMAIN *) src, (TP_DOMAIN *) d,
			       TP_EXACT_MATCH))
	    {
	      /* exact match flag is on */
	      found = d;
	    }
	}

    }

  return found;
}

/*
 * tp_domain_compatible - check compatibility of src domain w.r.t dest
 *    return: 1 if compatible, 0 otherwise
 *    src(in): src domain
 *    dest(in): dest domain
 */
int
tp_domain_compatible (const TP_DOMAIN * src, const TP_DOMAIN * dest)
{
  const TP_DOMAIN *d;
  int equal = 0;

  if (src != NULL && dest != NULL)
    {
      equal = 1;
      if (src != dest)
	{
	  /*
	   * for every domain in src, make sure we have a compatible one in
	   * dest
	   */
	  for (d = src; equal && d != NULL; d = d->next)
	    {
	      if (tp_domain_find_compatible (d, dest) == NULL)
		{
		  equal = 0;
		}
	    }
	}
    }

  return equal;
}
#endif

/*
 * tp_domain_select - select a domain from a list of possible domains that is
 * the exact match (or closest, depending on the value of exact_match) to the
 * supplied value.
 *    return: domain
 *    domain_list(in): list of possible domains
 *    value(in): value of interest
 *    exact_match(in): controls tolerance permitted during match
 * Note:
 *    This operation is used for basic domain compatibility checking
 *    as well as value coercion.
 *    If an appropriate domain could not be found, NULL is returned.
 *
 *    This is known not to work correctly for nested set domains.  In order
 *    for the best domain to be selected, we must recursively check the
 *    complete set domains here.
 *
 *    The exact_match flag determines if we allow "tolerance" matches when
 *    checking domains for attributes.  See commentary in tp_domain_match
 *    for more information.
 */
TP_DOMAIN *
tp_domain_select (const TP_DOMAIN * domain_list,
		  const DB_VALUE * value, TP_MATCH exact_match)
{
  TP_DOMAIN *best, *d;
  DB_TYPE vtype;
  UNUSED_VAR DB_VALUE temp;

  best = NULL;

  /*
   * NULL values are allowed in any domain, a NULL domain means that any value
   * is allowed, return the first thing on the list.
   */
  if (value == NULL || domain_list == NULL ||
      (vtype = DB_VALUE_TYPE (value)) == DB_TYPE_NULL)
    {
      return (TP_DOMAIN *) domain_list;
    }

  if (vtype == DB_TYPE_OID)
    {
      if (db_on_server)
	{
	  /*
	   * On the server, just make sure that we have any object domain in
	   * the list.
	   */
	  for (d = (TP_DOMAIN *) domain_list; d != NULL && best == NULL;
	       d = d->next)
	    {
	      if (TP_DOMAIN_TYPE (d) == DB_TYPE_OBJECT)
		{
		  best = d;
		}
	    }
	}
#if !defined (SERVER_MODE)
      else
	{
	  /*
	   * On the client, swizzle to an object and fall in to the next
	   * clause
	   */
	  OID *oid;
	  DB_OBJECT *mop;

	  oid = (OID *) db_get_oid (value);
	  if (oid)
	    {
	      if (OID_ISNULL (oid))
		{
		  /* this is the same as the NULL case above */
		  return (TP_DOMAIN *) domain_list;
		}
	      else
		{
		  mop = ws_mop (oid, NULL);
		  db_make_object (&temp, mop);
		  /*
		   * we don't have to worry about clearing this since its an
		   * object
		   */
		  value = (const DB_VALUE *) &temp;
		  vtype = DB_TYPE_OBJECT;
		}
	    }
	}
#endif /* !SERVER_MODE */
    }

  /*
   * Handling of object domains is more complex than just comparing the
   * types and parameters.  We have to see if the instance's class is
   * somewhere in the subclass hierarchy of the domain class.
   * This can't be done on the server yet though presumably we could
   * implement something like this using OID chasing.
   */

  if (vtype == DB_TYPE_OBJECT)
    {
      if (db_on_server)
	{
	  /*
	   * we really shouldn't get here but if we do, handle it like the
	   * OID case above, just return the first object domain that we find.
	   */
	  for (d = (TP_DOMAIN *) domain_list; d != NULL && best == NULL;
	       d = d->next)
	    {
	      if (TP_DOMAIN_TYPE (d) == DB_TYPE_OBJECT)
		{
		  best = d;
		}
	    }
	  return best;
	}
#if !defined (SERVER_MODE)
      else
	{
	  /*
	   * On the client, check to see if the instance is within the subclass
	   * hierarchy of the object domains.  If there are more than one
	   * acceptable domains, we just pick the first one.
	   */
	  DB_OBJECT *obj = db_get_object (value);

	  for (d = (TP_DOMAIN *) domain_list; d != NULL && best == NULL;
	       d = d->next)
	    {
	      if (TP_DOMAIN_TYPE (d) == DB_TYPE_OBJECT
		  && sm_check_object_domain (d, obj))
		{
		  best = d;
		}
	    }
	}
#endif /* !SERVER_MODE */
    }
  else if (TP_IS_SET_TYPE (vtype))
    {
      /*
       * Now that we cache set domains, there might be a faster way to do
       * this !
       */
      DB_SET *set;

      set = db_get_set (value);
      for (d = (TP_DOMAIN *) domain_list; d != NULL && best == NULL;
	   d = d->next)
	{
	  if (TP_DOMAIN_TYPE (d) == vtype)
	    {
	      if (set_check_domain (set, d) == DOMAIN_COMPATIBLE)
		{
		  best = d;
		}
	    }
	}
    }
  else
    {
      /*
       * synthesize a domain for the value and look for a match.
       * Could we be doing this for the set values too ?
       * Hack, since this will be used only for comparison purposes,
       * don't go through the overhead of caching the domain every time,
       * especially for numeric types.  This will be a lot simpler if we
       * store the domain
       * pointer directly in the DB_VALUE.
       */
      TP_DOMAIN *val_domain;

      val_domain = tp_domain_resolve_value ((DB_VALUE *) value);

      for (d = (TP_DOMAIN *) domain_list; d != NULL && best == NULL;
	   d = d->next)
	{
	  /* hack, try allowing "tolerance" matches of the domain ! */
	  if (tp_domain_match (d, val_domain, exact_match))
	    {
	      best = d;
	    }
	}
    }

  return best;
}

/*
 * tp_domain_check - does basic validation of a value against a domain.
 *    return: domain status
 *    domain(in): destination domain
 *    value(in): value to look at
 *    exact_match(in): controls the tolerance permitted for the match
 * Note:
 *    It does NOT do coercion.  If the intention is to perform coercion,
 *    them tp_domain_select should be used.
 *    Exact match is used to request a deferred coercion of values that
 *    are within "tolerance" of the destination domain.  This is currently
 *    only specified for assignment of attribute values and will be
 *    recognized only by those types whose "setmem" and "writeval" functions
 *    are able to perform delayed coercion.  Examples are the CHAR types
 *    which will do truncation or blank padding as the values are being
 *    assigned.  See commentary in tp_domain_match for more information.
 */
TP_DOMAIN_STATUS
tp_domain_check (const TP_DOMAIN * domain,
		 const DB_VALUE * value, TP_MATCH exact_match)
{
  TP_DOMAIN_STATUS status;
  TP_DOMAIN *d;

  assert (exact_match == TP_EXACT_MATCH);

  if (domain == NULL)
    {
      status = DOMAIN_COMPATIBLE;
    }
  else
    {
#if 1				/* TODO - trace */
      assert (domain->next == NULL);
#endif
      d = tp_domain_select (domain, value, exact_match);
      if (d == NULL)
	{
	  status = DOMAIN_INCOMPATIBLE;
	}
      else
	{
	  status = DOMAIN_COMPATIBLE;
	}
    }

  return status;
}

/*
 * COERCION
 */


/*
 * tp_can_steal_string - check if the string currently held in "val" can be
 * safely reused
 *    WITHOUT copying.
 *    return: error code
 *    val(in): source (and destination) value
 *    desired_domain(in): desired domain for coerced value
 * Note:
 *    Basically, this holds if
 *       1. the dest precision is "floating", or
 *       2. the dest type is varying and the length of the string is less
 *          than or equal to the dest precision, or
 *       3. the dest type is fixed and the length of the string is exactly
 *          equal to the dest precision.
 *    Since the desired domain is often a varying char, this wins often.
 */
int
tp_can_steal_string (const DB_VALUE * val, const DB_DOMAIN * desired_domain)
{
  DB_TYPE original_type, desired_type;
  int original_length, desired_precision;

  original_type = DB_VALUE_DOMAIN_TYPE (val);
  if (!TP_IS_CHAR_BIT_TYPE (original_type))
    {
      return 0;
    }

  original_length = DB_GET_STRING_LENGTH (val);
  desired_type = TP_DOMAIN_TYPE (desired_domain);
  desired_precision = desired_domain->precision;

  if (TP_IS_CHAR_TYPE (original_type)
      && TP_IS_CHAR_TYPE (TP_DOMAIN_TYPE (desired_domain)))
    {
      if (DB_GET_STRING_COLLATION (val)
	  != TP_DOMAIN_COLLATION (desired_domain)
	  && !LANG_IS_COERCIBLE_COLL (DB_GET_STRING_COLLATION (val)))
	{
	  return 0;
	}
    }

  if (desired_precision == TP_FLOATING_PRECISION_VALUE)
    {
      desired_precision = original_length;
    }

  switch (desired_type)
    {
    case DB_TYPE_VARCHAR:
      return (desired_precision >= original_length
	      && original_type == DB_TYPE_VARCHAR);
    case DB_TYPE_VARBIT:
      return (desired_precision >= original_length
	      && original_type == DB_TYPE_VARBIT);
    default:
      return 0;
    }
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * tp_null_terminate - NULL terminate the given DB_VALUE string.
 *    return: NO_ERROR or error code
 *    src(in): string to null terminate
 *    strp(out): pointer for output
 *    str_len(in): length of 'str'
 *    do_alloc(out): set true if allocation occurred
 * Note:
 *    Don't call this unless src is a string db_value.
 */
static int
tp_null_terminate (const DB_VALUE * src, char **strp, int str_len,
		   bool * do_alloc)
{
  char *str;
  int str_size;

  *do_alloc = false;		/* init */

  str = DB_GET_STRING (src);
  if (str == NULL)
    {
      return ER_FAILED;
    }

  str_size = DB_GET_STRING_SIZE (src);

  if (str[str_size] == '\0')
    {
      /* already NULL terminated */
      *strp = str;

      return NO_ERROR;
    }

  if (str_size >= str_len)
    {
      *strp = (char *) malloc (str_size + 1);
      if (*strp == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, str_size + 1);
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      *do_alloc = true;		/* mark as alloced */
    }

  memcpy (*strp, str, str_size);
  (*strp)[str_size] = '\0';

  return NO_ERROR;
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * tp_atotime - coerce a string to a time
 *    return: NO_ERROR or error code
 *    src(in): string DB_VALUE
 *    temp(out): time container
 * Note:
 *    Accepts strings that are not null terminated. Don't call this unless
 *    src is a string db_value.
 */
static int
tp_atotime (const DB_VALUE * src, DB_TIME * temp)
{
  int milisec;
  char *strp;
  int str_len;
  int status = NO_ERROR;

  assert (DB_VALUE_TYPE (src) == DB_TYPE_VARCHAR);

  strp = DB_GET_STRING (src);
  if (strp == NULL)
    {
      assert (false);		/* is impossible */
      return ER_FAILED;
    }

  str_len = DB_GET_STRING_SIZE (src);

  if (db_date_parse_time (strp, str_len, temp, &milisec) != NO_ERROR)
    {
      status = ER_FAILED;
    }

  return status;
}

/*
 * tp_atodate - coerce a string to a date
 *    return: NO_ERROR or error code
 *    src(in): string DB_VALUE
 *    temp(out): date container
 * Note:
 *    Accepts strings that are not null terminated. Don't call this unless
 *    src is a string db_value.
 */
static int
tp_atodate (const DB_VALUE * src, DB_DATE * temp)
{
  char *strp;
  int str_len;
  int status = NO_ERROR;

  assert (DB_VALUE_TYPE (src) == DB_TYPE_VARCHAR);

  strp = DB_GET_STRING (src);
  if (strp == NULL)
    {
      assert (false);		/* is impossible */
      return ER_FAILED;
    }

  str_len = DB_GET_STRING_SIZE (src);

  if (db_date_parse_date (strp, str_len, temp) != NO_ERROR)
    {
      status = ER_FAILED;
    }

  return status;
}

/*
 * tp_atoudatetime - coerce a string to a datetime.
 *    return: NO_ERROR or error code
 *    src(in): string DB_VALUE
 *    temp(out): datetime container
 * Note:
 *    Accepts strings that are not null terminated. Don't call this unless
 *    src is a string db_value.
 */
static int
tp_atoudatetime (const DB_VALUE * src, DB_DATETIME * temp)
{
  char *strp;
  int str_len;
  int status = NO_ERROR;

  assert (DB_VALUE_TYPE (src) == DB_TYPE_VARCHAR);

  strp = DB_GET_STRING (src);
  if (strp == NULL)
    {
      assert (false);		/* is impossible */
      return ER_FAILED;
    }

  str_len = DB_GET_STRING_SIZE (src);

  if (db_date_parse_datetime (strp, str_len, temp) != NO_ERROR)
    {
      status = ER_FAILED;
    }

  return status;
}

/*
 * tp_atonumeric - Coerce a string to a numeric.
 *    return: NO_ERROR or error code
 *    src(in): string DB_VALUE
 *    temp(out): numeirc container
 * Note:
 *    Accepts strings that are not null terminated. Don't call this unless
 *    src is a string db_value.
 */
static int
tp_atonumeric (const DB_VALUE * src, DB_VALUE * temp)
{
  char *strp;
  int status = NO_ERROR;
  int str_len;

  strp = DB_PULL_STRING (src);
  if (strp == NULL)
    {
      return ER_FAILED;
    }

  str_len = DB_GET_STRING_SIZE (src);

  if (numeric_coerce_string_to_num (strp, str_len, temp) != NO_ERROR)
    {
      status = ER_FAILED;
    }

  return status;
}

/*
 * tp_atof - Coerce a string to a double.
 *    return: NO_ERROR or error code.
 *    src(in): string DB_VALUE
 *    num_value(out): float container
 *    data_stat(out): if overflow is detected, this is set to
 *		      DATA_STATUS_TRUNCATED. If there exists some characters
 *		      that are not numeric codes or spaces then it is set to
 *		      DATA_STATUS_NOT_CONSUMED.
 * Note:
 *    Accepts strings that are not null terminated. Don't call this unless
 *    src is a string db_value.
 */
static int
tp_atof (const DB_VALUE * src, double *num_value, DB_DATA_STATUS * data_stat)
{
  char str[NUM_BUF_SIZE];
  char *strp = str;
  bool do_alloc = false;
  double d;
  char *p, *end;
  int status = NO_ERROR;
  unsigned int size;

  assert (DB_VALUE_TYPE (src) == DB_TYPE_VARCHAR);

  *data_stat = DATA_STATUS_OK;

  p = DB_GET_STRING (src);
  if (p == NULL)
    {
      assert (false);		/* is impossible */
      *data_stat = DATA_STATUS_NOT_CONSUMED;
      return ER_FAILED;
    }

  size = DB_GET_STRING_SIZE (src);
  if (size > 0)
    {
      end = p + size - 1;
    }
  else
    {
      end = p;
    }

  if (*end)
    {
      while (p <= end && char_isspace (*p))
	{
	  p++;
	}

      while (p < end && char_isspace (*end))
	{
	  end--;
	}

      size = end - p + 1;

      if (size > sizeof (str) - 1)
	{
	  strp = (char *) malloc (size + 1);
	  if (strp == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, size + 1);
	      return ER_OUT_OF_VIRTUAL_MEMORY;
	    }
	  do_alloc = true;
	}
      if (size)
	{
	  memcpy (strp, p, size);
	}
      strp[size] = '\0';
    }
  else
    {
      strp = p;
    }

  /* don't use atof() which cannot detect the error. */
  errno = 0;
  d = string_to_double (strp, &p);

  if (errno == ERANGE)
    {
      if (d != 0)
	{
	  /* overflow */
	  *data_stat = DATA_STATUS_TRUNCATED;
	}
      /* d == 0 is underflow, we don't have an error for this */
    }

  /* ignore trailing spaces */
  p = (char *) intl_skip_spaces (p, NULL);
  if (*p)			/* all input does not consumed */
    {
      *data_stat = DATA_STATUS_NOT_CONSUMED;
    }
  *num_value = d;

  if (do_alloc)
    {
      free_and_init (strp);
    }

  return status;
}

/*
 * tp_atobi - Coerce a string to a bigint.
 *    return: NO_ERROR or error code
 *    src(in): string DB_VALUE
 *    num_value(out): bigint container
 *    data_stat(out): if overflow is detected, this is set to
 *		      DATA_STATUS_TRUNCATED
 * Note:
 *    Accepts strings that are not null terminated. Don't call this unless
 *    src is a string db_value.
 *    If string contains decimal part, performs rounding.
 *
 */
static int
tp_atobi (const DB_VALUE * src, DB_BIGINT * num_value,
	  DB_DATA_STATUS * data_stat)
{
  char str[64];
  char *strp = DB_GET_STRING (src);
  char *stre;
  size_t n_digits;
  DB_BIGINT bigint;
  char *p;
  int status = NO_ERROR;
  bool round = false, is_negative, truncated = false;

  if (strp == NULL)
    {
      return ER_FAILED;
    }

  stre = strp + DB_GET_STRING_SIZE (src);

  /* skip leading spaces */
  while (strp != stre && char_isspace (*strp))
    {
      strp++;
    }

  /* read sign if any */
  if (strp != stre && (*strp == '-' || *strp == '+'))
    {
      is_negative = (*strp == '-');
      strp++;
    }
  else
    {
      is_negative = false;
    }

  /* skip leading zeros */
  while (strp != stre && *strp == '0')
    {
      strp++;
    }

  /* count number of significant digits */
  p = strp;

  while (p != stre && char_isdigit (*p))
    {
      p++;
    }

  n_digits = p - strp;
  if (n_digits > sizeof (str) / sizeof (str[0]) - 2)
    {
      /* more than 62 significant digits in the input number
         (63 chars with sign) */
      truncated = true;
    }

  /* skip decimal point and the digits after, keep the round flag */
  if (p != stre && *p == '.')
    {
      p++;

      if (p != stre)
	{
	  if (char_isdigit (*p))
	    {
	      if (*p >= '5')
		{
		  round = true;
		}

	      /* skip all digits after decimal point */
	      do
		{
		  p++;
		}
	      while (p != stre && char_isdigit (*p));
	    }
	}
    }

  /* skip trailing whitespace characters */
  p = (char *) intl_skip_spaces (p, stre);

  if (p != stre)
    {
      /* trailing characters in string */
      return ER_FAILED;
    }

  if (truncated)
    {
      *data_stat = DATA_STATUS_TRUNCATED;
      if (is_negative)
	{
	  bigint = DB_BIGINT_MIN;
	}
      else
	{
	  bigint = DB_BIGINT_MAX;
	}
    }
  else
    {
      /* Copy the number to str, excluding leading spaces and '0's and
         trailing spaces. Anything other than leading and trailing spaces
         already resulted in an error. */
      if (is_negative)
	{
	  str[0] = '-';
	  strncpy (str + 1, strp, n_digits);
	  str[n_digits + 1] = '\0';
	  strp = str;
	}
      else
	{
	  strp = strncpy (str, strp, n_digits);
	  str[n_digits] = '\0';
	}

      errno = 0;
      if (str_to_int64 (&bigint, &p, strp, 10) == 0)
	{
	  *data_stat = DATA_STATUS_OK;
	}
      else
	{
	  *data_stat = DATA_STATUS_NOT_CONSUMED;
	  if (errno == ERANGE)
	    {
	      *data_stat = DATA_STATUS_TRUNCATED;
	    }
	}

      /* round number if a '5' or greater digit was found after the decimal point */
      if (round)
	{
	  if (is_negative)
	    {
	      if (bigint > DB_BIGINT_MIN)
		{
		  bigint--;
		}
	      else
		{
		  *data_stat = DATA_STATUS_TRUNCATED;
		}
	    }
	  else
	    {
	      if (bigint < DB_BIGINT_MAX)
		{
		  bigint++;
		}
	      else
		{
		  *data_stat = DATA_STATUS_TRUNCATED;
		}
	    }
	}
    }

  *num_value = bigint;

  return status;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * tp_itoa - int to string representation for given radix
 *    return: string pointer (given or malloc'd)
 *    value(in): int value
 *    string(in/out): dest buffer or NULL
 *    radix(in): int value between 2 and 36
 */
static char *
tp_itoa (int value, char *string, int radix)
{
  char tmp[33];
  char *tp = tmp;
  int i;
  unsigned v;
  int sign;
  char *sp;

  if (radix > 36 || radix <= 1)
    {
      return 0;
    }

  sign = (radix == 10 && value < 0);

  if (sign)
    {
      v = -value;
    }
  else
    {
      v = (unsigned) value;
    }

  while (v || tp == tmp)
    {
      i = v % radix;
      v = v / radix;
      if (i < 10)
	{
	  *tp++ = i + '0';
	}
      else
	{
	  *tp++ = i + 'a' - 10;
	}
    }

  if (string == NULL)
    {
      string = (char *) malloc ((tp - tmp) + sign + 1);
      if (string == NULL)
	{
	  return string;
	}
    }
  sp = string;

  if (sign)
    {
      *sp++ = '-';
    }
  while (tp > tmp)
    {
      *sp++ = *--tp;
    }
  *sp = '\0';
  return string;
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * tp_ltoa - bigint to string representation for given radix
 *    return: string pointer (given or malloc'd)
 *    value(in): bigint value
 *    string(in/out): dest buffer or NULL
 *    radix(in): int value between 2 and 36
 */
static char *
tp_ltoa (DB_BIGINT value, char *string, int radix)
{
  char tmp[33];
  char *tp = tmp;
  int i;
  UINT64 v;
  int sign;
  char *sp;

  if (radix > 36 || radix <= 1)
    {
      return 0;
    }

  sign = (radix == 10 && value < 0);

  if (sign)
    {
      v = -value;
    }
  else
    {
      v = (UINT64) value;
    }

  while (v || tp == tmp)
    {
      i = v % radix;
      v = v / radix;
      if (i < 10)
	{
	  *tp++ = i + '0';
	}
      else
	{
	  *tp++ = i + 'a' - 10;
	}
    }

  if (string == NULL)
    {
      string = (char *) malloc ((tp - tmp) + sign + 1);
      if (string == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, (tp - tmp) + sign + 1);
	  return NULL;
	}
    }
  sp = string;

  if (sign)
    {
      *sp++ = '-';
    }
  while (tp > tmp)
    {
      *sp++ = *--tp;
    }
  *sp = '\0';

  return string;
}

/*
 * format_floating_point() - formats a digits sequence and an integer exponent
 *			     for a floating-point number (from dtoa) to the
 *			     printable representation
 *
 *  return:
 *  new_string(out):	the sequence of decimal digits for the floating-point
 *			number mantisa, to be reformated into a character
 *			sequence for a printable floating point number.
 *			the buffer is assumed to be large enought for the
 *			printable sequence
 *  rve(out):		end of sequence of digits
 *  ndigits(in):	floating number precision to be used for printing
 *  decpt(in):		decimal point position in the digits sequence
 *			(similar to the exponent)
 *  sign(in):		sign of the floating-point number
 */
static void
format_floating_point (char *new_string, char *rve, int ndigits, int decpt,
		       int sign)
{
  assert (new_string && rve);

  if (decpt != 9999)
    {
      if (ndigits >= decpt && decpt > -4)	/* as in the C 2005
						 * standard for printf conversion
						 * specification */
	{
	  /* print as a fractional number */
	  if (decpt > rve - new_string)
	    {
	      /* append with zeros until the decimal point is encountered */
	      while (new_string + decpt > rve)
		{
		  *rve++ = '0';
		}
	      *rve = '\0';
	      /* no decimal point needed */
	    }
	  else if (decpt <= 0)
	    {
	      /* prepend zeroes until the decimal point is encountered */
	      /* if decpt is -3, insert 3 zeroes between the decimal point
	       * and first non-zero digit */
	      size_t n_left_pad = +2 - decpt;

	      char *p = new_string + n_left_pad;

	      rve += n_left_pad;
	      do
		{
		  *rve = *(rve - n_left_pad);
		  rve--;
		}
	      while (rve != p);

	      *rve = *(rve - n_left_pad);

	      p--;
	      while (p != new_string + 1)
		{
		  *p-- = '0';
		}
	      *p-- = '.';
	      *p = '0';
	    }
	  else if (decpt != rve - new_string)
	    {
	      /* insert decimal point within the digits sequence at
	       * position indicated by decpt */
	      rve++;

	      while (rve != new_string + decpt)
		{
		  *rve = *(rve - 1);
		  rve--;
		}
	      *rve = '.';
	    }
	}
      else
	{
	  /* print as mantisa followed by exponent */
	  if (rve > new_string + 1)
	    {
	      /* insert the decimal point before the second digit, if any */
	      char *p = rve;

	      while (p != new_string)
		{
		  *p = *(p - 1);
		  p--;
		}

	      p[1] = '.';
	      rve++;
	    }
	  *rve++ = 'e';

	  decpt--;		/* convert from 0.432e12 to 4.32e11 */
	  if (decpt < 0)
	    {
	      *rve++ = '-';
	      decpt = -decpt;
	    }
	  else
	    {
	      *rve++ = '+';
	    }

	  if (decpt > 99)
	    {
	      *rve++ = '0' + decpt / 100;
	      *rve++ = '0' + decpt % 100 / 10;
	      *rve++ = '0' + decpt % 10;
	    }
	  else if (decpt > 9)
	    {
	      *rve++ = '0' + decpt / 10;
	      *rve++ = '0' + decpt % 10;
	    }
	  else
	    *rve++ = '0' + decpt;

	  *rve = '\0';
	}
    }

  /* prepend '-' sign if number is negative */
  if (sign)
    {
      char ch = *new_string;

      rve = new_string + 1;

      while (*rve)
	{
	  /* swap(ch, *rve); */
	  ch ^= *rve;
	  *rve = ch ^ *rve;
	  ch ^= *rve++;
	}

      /* swap(ch, *rve); */
      ch ^= *rve;
      *rve = ch ^ *rve;
      ch ^= *rve++;

      rve[0] = '\0';
      *new_string = '-';
    }
}

/*
 *  tp_dtoa():	    converts a double DB_VALUE to a string DB_VALUE.
 *		    Only as many digits as can be computed exactly are
 *		    written in the resulting string.
 *
 * return:
 * src(in):	    double DB_VALUE to be converted to string
 * result(in/out):  string DB_VALUE of the desired [VAR][N]CHAR domain
 *		    type and null value, to receive the converted float
 */
void
tp_dtoa (DB_VALUE const *src, DB_VALUE * result)
{
  /* dtoa() appears to ignore the requested number of digits... */
  const int ndigits = TP_DOUBLE_MANTISA_DECIMAL_PRECISION;
  char *str_double, *rve;
  int decpt, sign;

  assert (DB_VALUE_TYPE (src) == DB_TYPE_DOUBLE);
  assert (DB_VALUE_TYPE (result) == DB_TYPE_NULL);

  rve = str_double = (char *) malloc (TP_DOUBLE_AS_CHAR_LENGTH + 1);
  if (str_double == NULL)
    {
      DB_MAKE_NULL (result);
      return;
    }

  _dtoa (DB_GET_DOUBLE (src), 0, ndigits, &decpt, &sign, &rve, str_double, 0);
  /* rounding should also be performed here */
  str_double[ndigits] = '\0';	/* _dtoa() disregards ndigits */

  format_floating_point (str_double, str_double + strlen (str_double),
			 ndigits, decpt, sign);

  switch (DB_VALUE_DOMAIN_TYPE (result))
    {
    case DB_TYPE_VARCHAR:
      DB_MAKE_VARCHAR (result, DB_VALUE_PRECISION (result), str_double,
		       strlen (str_double), DB_GET_STRING_COLLATION (result));
      result->need_clear = true;
      break;

    default:
      free_and_init (str_double);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_TP_CANT_COERCE, 2,
	      pr_type_name ((DB_VALUE_DOMAIN_TYPE (src))),
	      pr_type_name ((DB_VALUE_DOMAIN_TYPE (result))));
      DB_MAKE_NULL (result);
      break;
    }
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * bfmt_print - Change the given string to a representation of the given
 * bit string value in the given format.
 *    return: NO_ERROR or -1 if max_size is too small
 *    bfmt(in): 0: for binary representation or 1: for hex representation
 *    the_db_bit(in): DB_VALUE
 *    string(out): output buffer
 *    max_size(in): size of output buffer
 */
static int
bfmt_print (int bfmt, const DB_VALUE * the_db_bit, char *string, int max_size)
{
  int length = 0;
  int string_index = 0;
  int byte_index;
  int bit_index;
  char *bstring;
  int error = NO_ERROR;
  static const char digits[16] = {
    '0', '1', '2', '3', '4', '5', '6', '7',
    '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
  };

  /* Get the buffer and the length from the_db_bit */
  bstring = DB_GET_VARBIT (the_db_bit, &length);

  switch (bfmt)
    {
    case 0:			/* BIT_STRING_BINARY */
      if (length + 1 > max_size)
	{
	  error = -1;
	}
      else
	{
	  for (byte_index = 0; byte_index < BITS_TO_BYTES (length);
	       byte_index++)
	    {
	      for (bit_index = 7;
		   bit_index >= 0 && string_index < length; bit_index--)
		{
		  *string =
		    digits[((bstring[byte_index] >> bit_index) & 0x1)];
		  string++;
		  string_index++;
		}
	    }
	  *string = '\0';
	}
      break;

    case 1:			/* BIT_STRING_HEX */
      if (BITS_TO_HEX (length) + 1 > max_size)
	{
	  error = -1;
	}
      else
	{
	  for (byte_index = 0; byte_index < BITS_TO_BYTES (length);
	       byte_index++)
	    {
	      *string = digits[((bstring[byte_index] >> BITS_IN_HEX) & 0x0f)];
	      string++;
	      string_index++;
	      if (string_index < BITS_TO_HEX (length))
		{
		  *string = digits[((bstring[byte_index] & 0x0f))];
		  string++;
		  string_index++;
		}
	    }
	  *string = '\0';
	}
      break;

    default:
      break;
    }

  return error;
}
#endif

#define ROUND(x)		  ((x) > 0 ? ((x) + .5) : ((x) - .5))
#define SECONDS_IN_A_DAY	  (long)(86400)	/* 24L * 60L * 60L */

#define TP_IMPLICIT_COERCION_NOT_ALLOWED(src_type, dest_type)		\
   ((src_type != dest_type)						\
    && (TP_IS_VARBIT_TYPE(src_type) || TP_IS_VARBIT_TYPE(dest_type)))

/*
 * tp_value_string_to_double - Coerce a string to a double.
 *    return: NO_ERROR, ER_OUT_OF_VIRTUAL_MEMORY or ER_FAILED.
 *    src(in): string DB_VALUE
 *    result(in/out): float container
 * Note:
 *    Accepts strings that are not null terminated.
 */
int
tp_value_string_to_double (const DB_VALUE * value, DB_VALUE * result)
{
  DB_DATA_STATUS data_stat;
  double dbl;
  int ret;
  DB_TYPE type = DB_VALUE_TYPE (value);

  if (type != DB_TYPE_VARCHAR)
    {
      DB_MAKE_DOUBLE (result, 0);
      return ER_FAILED;
    }

  ret = tp_atof (value, &dbl, &data_stat);
  if (ret != NO_ERROR || data_stat != DATA_STATUS_OK)
    {
      DB_MAKE_DOUBLE (result, 0);
      return ER_FAILED;
    }

  DB_MAKE_DOUBLE (result, dbl);

  return ret;
}

static void
make_desired_string_db_value (DB_TYPE desired_type,
			      const TP_DOMAIN * desired_domain,
			      const char *new_string, DB_VALUE * target,
			      TP_DOMAIN_STATUS * status,
			      DB_DATA_STATUS * data_stat)
{
  DB_VALUE temp;

  *status = DOMAIN_COMPATIBLE;
  switch (desired_type)
    {
    case DB_TYPE_VARCHAR:
      db_make_varchar (&temp, desired_domain->precision,
		       (char *) new_string, strlen (new_string),
		       TP_DOMAIN_COLLATION (desired_domain));
      break;
    default:			/* Can't get here.  This just quiets the compiler */
      break;
    }

  temp.need_clear = true;
  if (db_char_string_coerce (&temp, target, data_stat) != NO_ERROR)
    {
      *status = DOMAIN_INCOMPATIBLE;
    }
  else
    {
      *status = DOMAIN_COMPATIBLE;
    }
  pr_clear_value (&temp);
}

/*
 * tp_value_coerce - Coerce a value into one of another domain Implicitly.
 *    return: TP_DOMAIN_STATUS
 *    src(in): source value
 *    dest(out): destination value
 *    desired_domain(in): destination domain
 */
TP_DOMAIN_STATUS
tp_value_coerce (const DB_VALUE * src, DB_VALUE * dest,
		 const TP_DOMAIN * desired_domain)
{
  TP_DOMAIN_STATUS status;

  status =
    tp_value_coerce_internal (src, dest, desired_domain, TP_IMPLICIT_COERCION,
			      false);

  return status;
}

/*
 * tp_value_coerce_strict () - convert a value to desired domain without loss
 *			       of precision
 * return : error code or NO_ERROR
 * src (in)   : source value
 * dest (out) : destination value
 * desired_domain (in) : destination domain
 */
int
tp_value_coerce_strict (const DB_VALUE * src, DB_VALUE * dest,
			const TP_DOMAIN * desired_domain)
{
  DB_TYPE desired_type, original_type;
  int err = NO_ERROR;
  DB_VALUE temp_dest, *target;

  if (src != dest)
    {
      (void) pr_clear_value (dest);
    }

  /* A NULL src is allowed but destination remains NULL, not desired_domain */
  if (src == NULL || (original_type = DB_VALUE_TYPE (src)) == DB_TYPE_NULL)
    {
      db_make_null (dest);
      return err;
    }

  if (desired_domain == NULL)
    {
      db_make_null (dest);
      return ER_FAILED;
    }

  desired_type = TP_DOMAIN_TYPE (desired_domain);

  if (!TP_IS_NUMERIC_TYPE (desired_type)
      && !TP_IS_DATE_OR_TIME_TYPE (desired_type))
    {
      db_make_null (dest);
      return ER_FAILED;
    }

  if (desired_type == original_type)
    {
      /*
       * If there is an easy to check exact match on a non-parameterized
       * domain, just do a simple clone of the value.
       */
      if (!desired_domain->is_parameterized)
	{
	  if (src != dest)
	    {
	      assert (dest->need_clear == false);
	      pr_clone_value ((DB_VALUE *) src, dest);
	      return NO_ERROR;
	    }
	}
    }

  /*
   * If src == dest, coerce into a temporary variable and
   * handle the conversion before returning.
   */
  if (src == dest)
    {
      target = &temp_dest;
    }
  else
    {
      target = dest;
    }

  /*
   * Initialize the destination domain, important for the
   * nm_ coercion functions which take domain information inside the
   * destination db value.
   */
  db_value_domain_init (target, desired_type,
			desired_domain->precision, desired_domain->scale);

  switch (desired_type)
    {
    case DB_TYPE_INTEGER:
      switch (original_type)
	{
	case DB_TYPE_BIGINT:
	  if (OR_CHECK_INT_OVERFLOW (DB_GET_BIGINT (src)))
	    {
	      err = ER_FAILED;
	      break;
	    }
	  db_make_int (target, (int) DB_GET_BIGINT (src));
	  break;
	case DB_TYPE_DOUBLE:
	  {
	    double i = 0;
	    const double val = DB_GET_DOUBLE (src);
	    if (OR_CHECK_INT_OVERFLOW (val))
	      {
		err = ER_FAILED;
		break;
	      }
	    if (modf (val, &i) != 0)
	      {
		err = ER_FAILED;
		break;
	      }
	    db_make_int (target, (int) i);
	    break;
	  }
	case DB_TYPE_NUMERIC:
	  err =
	    numeric_db_value_coerce_from_num_strict ((DB_VALUE *) src,
						     target);
	  break;
	case DB_TYPE_VARCHAR:
	  {
	    double num_value = 0.0, i = 0.0;
	    DB_DATA_STATUS data_stat = DATA_STATUS_OK;
	    if (tp_atof (src, &num_value, &data_stat) != NO_ERROR)
	      {
		err = ER_FAILED;
		break;
	      }
	    if (data_stat != DATA_STATUS_OK
		|| OR_CHECK_INT_OVERFLOW (num_value))
	      {
		err = ER_FAILED;
		break;
	      }
	    if (modf (num_value, &i) != 0)
	      {
		err = ER_FAILED;
		break;
	      }
	    db_make_int (target, (int) i);
	    break;
	  }
	default:
	  err = ER_FAILED;
	  break;
	}
      break;

    case DB_TYPE_BIGINT:
      switch (original_type)
	{
	case DB_TYPE_INTEGER:
	  db_make_bigint (target, DB_GET_INTEGER (src));
	  break;
	case DB_TYPE_DOUBLE:
	  {
	    double i = 0;
	    const double val = DB_GET_DOUBLE (src);
	    if (OR_CHECK_BIGINT_OVERFLOW (val))
	      {
		err = ER_FAILED;
		break;
	      }
	    if (modf (val, &i) != 0)
	      {
		err = ER_FAILED;
		break;
	      }
	    db_make_bigint (target, (DB_BIGINT) i);
	    break;
	  }
	case DB_TYPE_NUMERIC:
	  err =
	    numeric_db_value_coerce_from_num_strict ((DB_VALUE *) src,
						     target);
	  break;
	case DB_TYPE_VARCHAR:
	  {
	    double num_value = 0.0, i = 0.0;
	    DB_DATA_STATUS data_stat = DATA_STATUS_OK;
	    if (tp_atof (src, &num_value, &data_stat) != NO_ERROR)
	      {
		err = ER_FAILED;
		break;
	      }
	    if (data_stat != DATA_STATUS_OK
		|| OR_CHECK_BIGINT_OVERFLOW (num_value))
	      {
		err = ER_FAILED;
		break;
	      }
	    if (modf (num_value, &i) != 0)
	      {
		err = ER_FAILED;
		break;
	      }
	    db_make_bigint (target, (DB_BIGINT) i);
	    break;
	  }
	default:
	  err = ER_FAILED;
	  break;
	}
      break;

    case DB_TYPE_DOUBLE:
      switch (original_type)
	{
	case DB_TYPE_INTEGER:
	  db_make_double (target, (double) DB_GET_INTEGER (src));
	  break;
	case DB_TYPE_BIGINT:
	  db_make_double (target, (double) DB_GET_BIGINT (src));
	  break;
	case DB_TYPE_NUMERIC:
	  err =
	    numeric_db_value_coerce_from_num_strict ((DB_VALUE *) src,
						     target);
	  break;
	case DB_TYPE_VARCHAR:
	  {
	    DB_DATA_STATUS data_stat = DATA_STATUS_OK;
	    double num_value = 0.0;
	    if (tp_atof (src, &num_value, &data_stat) != NO_ERROR)
	      {
		err = ER_FAILED;
		break;
	      }
	    if (data_stat != DATA_STATUS_OK)
	      {
		err = ER_FAILED;
		break;
	      }
	    db_make_double (target, num_value);
	    break;
	  }
	default:
	  err = ER_FAILED;
	  break;
	}
      break;

    case DB_TYPE_NUMERIC:
      switch (original_type)
	{
	case DB_TYPE_VARCHAR:
	  {
	    DB_VALUE temp;
	    unsigned char num[DB_NUMERIC_BUF_SIZE];	/* copy of a DB_C_NUMERIC */

	    if (tp_atonumeric (src, &temp) != NO_ERROR)
	      {
		err = ER_FAILED;
	      }
	    else
	      {
		err = numeric_coerce_num_to_num (db_locate_numeric (&temp),
						 DB_VALUE_PRECISION (&temp),
						 DB_VALUE_SCALE (&temp),
						 desired_domain->precision,
						 desired_domain->scale, num);
		if (err == NO_ERROR)
		  {
		    err =
		      DB_MAKE_NUMERIC (target, num, desired_domain->precision,
				       desired_domain->scale);
		  }
	      }
	    break;
	  }
	case DB_TYPE_INTEGER:
	case DB_TYPE_BIGINT:
	case DB_TYPE_NUMERIC:
	  {
	    DB_DATA_STATUS data_stat = DATA_STATUS_OK;
	    err = numeric_db_value_coerce_to_num ((DB_VALUE *) src, target,
						  &data_stat);
	    if (data_stat != DATA_STATUS_OK)
	      {
		err = ER_FAILED;
		break;
	      }
	    break;
	  }
	default:
	  err = ER_FAILED;
	  break;
	}
      break;

    case DB_TYPE_TIME:
      switch (original_type)
	{
	case DB_TYPE_VARCHAR:
	  {
	    DB_TIME time = 0;
	    if (tp_atotime (src, &time) != NO_ERROR)
	      {
		err = ER_FAILED;
		break;
	      }
	    db_value_put_encoded_time (target, &time);
	    break;
	  }
	default:
	  err = ER_FAILED;
	  break;
	}
      break;

    case DB_TYPE_DATE:
      switch (original_type)
	{
	case DB_TYPE_VARCHAR:
	  {
	    DB_DATE date = 0;
	    if (tp_atodate (src, &date) != NO_ERROR)
	      {
		err = ER_FAILED;
		break;
	      }
	    db_value_put_encoded_date (target, &date);
	    break;
	  }
	case DB_TYPE_DATETIME:
	  {
	    DB_DATETIME *src_dt = NULL;
	    src_dt = DB_GET_DATETIME (src);
	    if (src_dt->time != 0)
	      {
		/* only "downcast" if time is 0 */
		err = ER_FAILED;
		break;
	      }
	    db_value_put_encoded_date (target, (DB_DATE *) & src_dt->date);
	    break;
	  }
	default:
	  err = ER_FAILED;
	  break;
	}
      break;

    case DB_TYPE_DATETIME:
      switch (original_type)
	{
	case DB_TYPE_DATE:
	  {
	    DB_DATETIME datetime = { 0, 0 };
	    datetime.date = *DB_GET_DATE (src);
	    datetime.time = 0;
	    db_make_datetime (target, &datetime);
	    break;
	  }
	case DB_TYPE_VARCHAR:
	  {
	    DB_DATETIME datetime = { 0, 0 };
	    if (tp_atoudatetime (src, &datetime) != NO_ERROR)
	      {
		err = ER_FAILED;
		break;
	      }
	    db_make_datetime (target, &datetime);
	    break;
	  }
	default:
	  err = ER_FAILED;
	  break;
	}
      break;

    default:
      err = ER_FAILED;
      break;
    }

  if (err == ER_FAILED)
    {
      /* the above code might have set an error message but we don't want
       * to propagate it in this context
       */
      er_clear ();
    }

#if !defined (NDEBUG)
  if (err == NO_ERROR)
    {
      assert (desired_type == TP_DOMAIN_TYPE (desired_domain));
      if (desired_domain->is_parameterized && !DB_IS_NULL (target))
	{
	  assert (desired_type == DB_VALUE_TYPE (target));
/*
          assert (desired_domain->precision == DB_VALUE_PRECISION (target));
          assert (desired_domain->scale == DB_VALUE_SCALE (target));
*/
	}
    }
#endif

  return err;
}

/*
 * tp_value_coerce_internal - Coerce a value into one of another domain.
 *    return: error code
 *    src(in): source value
 *    dest(out): destination value
 *    desired_domain(in): destination domain
 *    coercion_mode(in): flag for the coercion mode
 *    preserve_domain(in): flag to preserve dest's domain
 */
static TP_DOMAIN_STATUS
tp_value_coerce_internal (const DB_VALUE * src, DB_VALUE * dest,
			  const TP_DOMAIN * desired_domain,
			  const TP_COERCION_MODE coercion_mode,
			  bool preserve_domain)
{
  DB_TYPE desired_type = DB_TYPE_NULL, original_type = DB_TYPE_NULL;
  int err;
  TP_DOMAIN_STATUS status;
  DB_DATETIME v_datetime;
  DB_TIME v_time;
  DB_DATE v_date;
  DB_DATA_STATUS data_stat;
  DB_VALUE temp, *target;
  int hour, minute, second, millisecond;
  int year, month, day;

  err = NO_ERROR;
  status = DOMAIN_COMPATIBLE;

  if (src != dest)
    {
      (void) pr_clear_value (dest);
    }

  if (desired_domain == NULL)
    {
      db_make_null (dest);
      status = DOMAIN_INCOMPATIBLE;
      goto exit_on_error;
    }

  desired_type = TP_DOMAIN_TYPE (desired_domain);

  /* A NULL src is allowed but destination remains NULL, not desired_domain */
  if (src == NULL || (original_type = DB_VALUE_TYPE (src)) == DB_TYPE_NULL)
    {
      if (preserve_domain)
	{
	  db_value_domain_init (dest, desired_type, desired_domain->precision,
				desired_domain->scale);
	  db_value_put_null (dest);
	}
      else
	{
	  db_make_null (dest);
	}
      goto done;
    }

  if (desired_type == original_type)
    {
      /*
       * If there is an easy to check exact match on a non-parameterized
       * domain, just do a simple clone of the value.
       */
      if (!desired_domain->is_parameterized)
	{
	  if (src != dest)
	    {
	      assert (dest->need_clear == false);
	      pr_clone_value ((DB_VALUE *) src, dest);
	    }
	  goto done;
	}
      else
	{			/* is parameterized domain */
	  switch (desired_type)
	    {
	    case DB_TYPE_NUMERIC:
	      if (desired_domain->precision == DB_VALUE_PRECISION (src)
		  && desired_domain->scale == DB_VALUE_SCALE (src))
		{
		  if (src != dest)
		    {
		      assert (dest->need_clear == false);
		      pr_clone_value ((DB_VALUE *) src, dest);
		    }
		  goto done;
		}
	      break;

	    case DB_TYPE_OID:
	      if (src != dest)
		{
		  assert (dest->need_clear == false);
		  pr_clone_value ((DB_VALUE *) src, dest);
		}
	      goto done;
	      break;

	    default:
	      /* pr_is_string_type(desired_type) - NEED MORE CONSIDERATION */
	      break;
	    }
	}
    }

  /*
   * If the coercion_mode is TP_IMPLICIT_COERCION, check to see if the original
   * type can be implicitly coerced to the desired_type.
   *
   * (Note: This macro only picks up only coercions that are not allowed
   *        implicitly but are allowed explicitly.)
   */
  if (coercion_mode == TP_IMPLICIT_COERCION)
    {
      if (TP_IMPLICIT_COERCION_NOT_ALLOWED (original_type, desired_type))
	{
	  if (preserve_domain)
	    {
	      db_value_domain_init (dest, desired_type,
				    desired_domain->precision,
				    desired_domain->scale);
	      db_value_put_null (dest);
	    }
	  else
	    {
	      db_make_null (dest);
	    }
	  status = DOMAIN_INCOMPATIBLE;
	  goto exit_on_error;
	}
    }

  /*
   * If src == dest, coerce into a temporary variable and
   * handle the conversion before returning.
   */
  if (src == dest)
    {
      target = &temp;
    }
  else
    {
      target = dest;
    }

  /*
   * Initialize the destination domain, important for the
   * nm_ coercion functions thich take domain information inside the
   * destination db value.
   */
  db_value_domain_init (target, desired_type,
			desired_domain->precision, desired_domain->scale);

  if (target != NULL && TP_IS_CHAR_TYPE (DB_VALUE_DOMAIN_TYPE (target)))
    {
      db_string_put_cs_and_collation (target,
				      TP_DOMAIN_COLLATION (desired_domain));
    }

  switch (desired_type)
    {
    case DB_TYPE_INTEGER:
      switch (original_type)
	{
	case DB_TYPE_BIGINT:
	  if (OR_CHECK_INT_OVERFLOW (DB_GET_BIGINT (src)))
	    {
	      status = DOMAIN_OVERFLOW;
	    }
	  else
	    {
	      db_make_int (target, (int) DB_GET_BIGINT (src));
	    }
	  break;
	case DB_TYPE_DOUBLE:
	  if (OR_CHECK_INT_OVERFLOW (DB_GET_DOUBLE (src)))
	    {
	      status = DOMAIN_OVERFLOW;
	    }
	  else
	    {
	      db_make_int (target, (int) ROUND (DB_GET_DOUBLE (src)));
	    }
	  break;
	case DB_TYPE_NUMERIC:
	  status = (TP_DOMAIN_STATUS)
	    numeric_db_value_coerce_from_num ((DB_VALUE *) src, target,
					      &data_stat);
	  if (status != NO_ERROR)
	    {
	      status = DOMAIN_OVERFLOW;
	    }
	  break;
	case DB_TYPE_VARCHAR:
	  {
	    double num_value = 0.0;

	    if (tp_atof (src, &num_value, &data_stat) != NO_ERROR
		|| data_stat == DATA_STATUS_NOT_CONSUMED)
	      {
		if (er_errid () != NO_ERROR)	/* i.e, malloc failure */
		  {
		    status = DOMAIN_ERROR;
		    goto exit_on_error;
		  }
		status = DOMAIN_INCOMPATIBLE;	/* conversion error */
		break;
	      }

	    if (data_stat == DATA_STATUS_TRUNCATED
		|| OR_CHECK_INT_OVERFLOW (num_value))
	      {
		status = DOMAIN_OVERFLOW;
	      }
	    else
	      {
		db_make_int (target, (int) ROUND (num_value));
	      }
	    break;
	  }
	default:
	  status = DOMAIN_INCOMPATIBLE;
	  break;
	}
      break;

    case DB_TYPE_BIGINT:
      switch (original_type)
	{
	case DB_TYPE_INTEGER:
	  db_make_bigint (target, DB_GET_INTEGER (src));
	  break;
	case DB_TYPE_DOUBLE:
	  {
	    double tmp_double;
	    DB_BIGINT tmp_bi;

	    if (OR_CHECK_BIGINT_OVERFLOW (DB_GET_DOUBLE (src)))
	      {
		status = DOMAIN_OVERFLOW;
	      }
	    else
	      {
		tmp_double = DB_GET_DOUBLE (src);
		tmp_bi = (DB_BIGINT) ROUND (tmp_double);

		if (OR_CHECK_ASSIGN_OVERFLOW (tmp_bi, tmp_double))
		  {
		    status = DOMAIN_OVERFLOW;
		  }
		else
		  {
		    db_make_bigint (target, tmp_bi);
		  }
	      }
	  }
	  break;
	case DB_TYPE_NUMERIC:
	  status = (TP_DOMAIN_STATUS)
	    numeric_db_value_coerce_from_num ((DB_VALUE *) src, target,
					      &data_stat);
	  if (status != NO_ERROR)
	    {
	      status = DOMAIN_OVERFLOW;
	    }
	  break;
	case DB_TYPE_VARCHAR:
	  {
	    DB_BIGINT num_value = 0;

	    if (tp_atobi (src, &num_value, &data_stat) != NO_ERROR)
	      {
		if (er_errid () != NO_ERROR)	/* i.e, malloc failure */
		  {
		    status = DOMAIN_ERROR;
		    goto exit_on_error;
		  }
		status = DOMAIN_INCOMPATIBLE;	/* conversion error */
		break;
	      }

	    if (data_stat == DATA_STATUS_TRUNCATED)
	      {
		status = DOMAIN_OVERFLOW;
		break;
	      }
	    else if (data_stat != DATA_STATUS_OK)
	      {
		status = DOMAIN_INCOMPATIBLE;	/* conversion error */
		break;
	      }

	    db_make_bigint (target, num_value);
	    break;
	  }
	default:
	  status = DOMAIN_INCOMPATIBLE;
	  break;
	}
      break;

    case DB_TYPE_DOUBLE:
      switch (original_type)
	{
	case DB_TYPE_INTEGER:
	  db_make_double (target, (double) DB_GET_INTEGER (src));
	  break;
	case DB_TYPE_BIGINT:
	  db_make_double (target, (double) DB_GET_BIGINT (src));
	  break;
	case DB_TYPE_NUMERIC:
	  status = (TP_DOMAIN_STATUS)
	    numeric_db_value_coerce_from_num ((DB_VALUE *) src, target,
					      &data_stat);
	  if (status != NO_ERROR)
	    {
	      status = DOMAIN_OVERFLOW;
	    }
	  break;
	case DB_TYPE_VARCHAR:
	  {
	    double num_value = 0.0;

	    if (tp_atof (src, &num_value, &data_stat) != NO_ERROR
		|| data_stat == DATA_STATUS_NOT_CONSUMED)
	      {
		if (er_errid () != NO_ERROR)	/* i.e, malloc failure */
		  {
		    status = DOMAIN_ERROR;
		    goto exit_on_error;
		  }
		status = DOMAIN_INCOMPATIBLE;	/* conversion error */
		break;
	      }

	    if (data_stat == DATA_STATUS_TRUNCATED)
	      {
		status = DOMAIN_OVERFLOW;
	      }
	    else
	      {
		db_make_double (target, num_value);
	      }

	    break;
	  }
	default:
	  status = DOMAIN_INCOMPATIBLE;
	  break;
	}
      break;

    case DB_TYPE_NUMERIC:
      /*
       * Numeric-to-numeric coercion will be handled in the nm_ module.
       * The desired precision & scale is communicated through the destination
       * value.
       */
      switch (original_type)
	{
	case DB_TYPE_VARCHAR:
	  {
	    DB_VALUE temp;
	    unsigned char num[DB_NUMERIC_BUF_SIZE];	/* copy of a DB_C_NUMERIC */

	    if (tp_atonumeric (src, &temp) != NO_ERROR)
	      {
		if (er_errid () != NO_ERROR)
		  {
		    status = DOMAIN_ERROR;
		    goto exit_on_error;
		  }
		status = DOMAIN_INCOMPATIBLE;
	      }
	    else
	      {
		if (numeric_coerce_num_to_num (db_locate_numeric (&temp),
					       DB_VALUE_PRECISION (&temp),
					       DB_VALUE_SCALE (&temp),
					       desired_domain->precision,
					       desired_domain->scale,
					       num) != NO_ERROR)
		  {
		    status = DOMAIN_ERROR;
		    goto exit_on_error;
		  }

		if (DB_MAKE_NUMERIC (target, num, desired_domain->precision,
				     desired_domain->scale) != NO_ERROR)
		  {
		    status = DOMAIN_ERROR;
		    goto exit_on_error;
		  }
	      }
	    break;
	  }
	default:
	  {
	    int error_code = numeric_db_value_coerce_to_num ((DB_VALUE *) src,
							     target,
							     &data_stat);
	    if (error_code == ER_NUM_OVERFLOW
		|| data_stat == DATA_STATUS_TRUNCATED)
	      {
		status = DOMAIN_OVERFLOW;
	      }
	    else if (error_code != NO_ERROR)
	      {
		status = DOMAIN_INCOMPATIBLE;
	      }
	    else
	      {
		status = DOMAIN_COMPATIBLE;
	      }
	  }
	  break;
	}
      break;

    case DB_TYPE_DATETIME:
      switch (original_type)
	{
	case DB_TYPE_VARCHAR:
	  if (tp_atoudatetime (src, &v_datetime) != NO_ERROR)
	    {
	      status = DOMAIN_ERROR;
	      goto exit_on_error;
	    }
	  else
	    {
	      db_make_datetime (target, &v_datetime);
	    }
	  break;

	case DB_TYPE_DATE:
	  v_datetime.date = *DB_GET_DATE (src);
	  v_datetime.time = 0;
	  db_make_datetime (target, &v_datetime);
	  break;

	default:
	  status = DOMAIN_INCOMPATIBLE;
	  break;
	}
      break;

    case DB_TYPE_DATE:
      switch (original_type)
	{
	case DB_TYPE_VARCHAR:
	  if (tp_atodate (src, &v_date) == NO_ERROR)
	    {
	      db_date_decode (&v_date, &month, &day, &year);
	    }
	  else
	    {
	      status = DOMAIN_ERROR;
	      goto exit_on_error;
	    }

	  if (db_make_date (target, month, day, year) != NO_ERROR)
	    {
	      status = DOMAIN_ERROR;
	      goto exit_on_error;
	    }
	  break;

	case DB_TYPE_DATETIME:
	  db_datetime_decode ((DB_DATETIME *) DB_GET_DATETIME (src), &month,
			      &day, &year, &hour, &minute, &second,
			      &millisecond);
	  db_make_date (target, month, day, year);
	  break;

	default:
	  status = DOMAIN_INCOMPATIBLE;
	  break;
	}
      break;

    case DB_TYPE_TIME:
      switch (original_type)
	{
	case DB_TYPE_DATETIME:
	  db_datetime_decode ((DB_DATETIME *) DB_GET_DATETIME (src), &month,
			      &day, &year, &hour, &minute, &second,
			      &millisecond);
	  db_make_time (target, hour, minute, second);
	  break;
	case DB_TYPE_VARCHAR:
	  if (tp_atotime (src, &v_time) == NO_ERROR)
	    {
	      db_time_decode (&v_time, &hour, &minute, &second);
	    }
	  else
	    {
	      status = DOMAIN_ERROR;
	      goto exit_on_error;
	    }

	  if (db_make_time (target, hour, minute, second) != NO_ERROR)
	    {
	      status = DOMAIN_ERROR;
	      goto exit_on_error;
	    }
	  break;
	default:
	  status = DOMAIN_INCOMPATIBLE;
	  break;
	}
      break;

#if 0				/* TODO - trace */
#if !defined (SERVER_MODE)
    case DB_TYPE_OBJECT:
#if 1				/* TODO - */
      status = DOMAIN_INCOMPATIBLE;	/* not permit */
#else
      {
	DB_OBJECT *v_obj = NULL;
	/* Make sure the domains are compatible.  Coerce view objects to
	   real objects.
	 */
	switch (original_type)
	  {
	  case DB_TYPE_OBJECT:
	    if (!sm_coerce_object_domain ((TP_DOMAIN *) desired_domain,
					  DB_GET_OBJECT (src), &v_obj))
	      {
		status = DOMAIN_INCOMPATIBLE;
	      }
	    break;
	  case DB_TYPE_OID:
	    {
	      OID *oid;

	      oid = DB_GET_OID (src);
	      if (oid != NULL && !OID_ISNULL (oid))
		{
		  v_obj = ws_mop (oid, NULL);
		}
	    }
	    break;

	  default:
	    status = DOMAIN_INCOMPATIBLE;
	  }

	{
	  /* check we got an object in a proper class */
	  if (v_obj && desired_domain->class_mop)
	    {
	      DB_OBJECT *obj_class;

	      obj_class = sm_get_class (v_obj);
	      if (obj_class == desired_domain->class_mop)
		{
		  /* everything is fine */
		}
	      else if (db_is_vclass (desired_domain->class_mop))
		{
		  /*
		   * This should still be an error, and the above
		   * code should have constructed a virtual mop.
		   * I'm not sure the rest of the code is consistent
		   * in this regard.
		   */
		}
	      else
		{
		  status = DOMAIN_INCOMPATIBLE;
		}
	    }
	  db_make_object (target, v_obj);
	}
      }
#endif
      break;
#endif /* !SERVER_MODE */

    case DB_TYPE_SEQUENCE:
#if 1				/* TODO - */
      status = DOMAIN_INCOMPATIBLE;	/* not permit */
#else
      if (!TP_IS_SET_TYPE (original_type))
	{
	  status = DOMAIN_INCOMPATIBLE;
	}
      else
	{
	  SETREF *setref;

	  setref = db_get_set (src);
	  if (setref)
	    {
	      TP_DOMAIN *set_domain;

	      set_domain = setobj_domain (setref->set);
	      if (src == dest
		  && tp_domain_compatible (set_domain, desired_domain))
		{
		  /*
		   * We know that this is a "coerce-in-place" operation, and
		   * we know that no coercion is necessary, so do nothing: we
		   * can use the exact same set without any conversion.
		   * Setting "src" to NULL prevents the wrapup code from
		   * clearing the set; that's important since we haven't made
		   * a copy.
		   */
		  setobj_put_domain (setref->set,
				     (TP_DOMAIN *) desired_domain);
		  src = NULL;
		}
	      else
		{
		  if (tp_domain_compatible (set_domain, desired_domain))
		    {
		      /*
		       * Well, we can't use the exact same set, but we don't
		       * have to do the whole hairy coerce thing either: we
		       * can just make a copy and then take the more general
		       * domain.  setobj_put_domain() guards against null
		       * pointers, there's no need to check first.
		       */
		      setref = set_copy (setref);
		      if (setref)
			{
			  setobj_put_domain (setref->set,
					     (TP_DOMAIN *) desired_domain);
			}
		    }
		  else
		    {
		      /*
		       * Well, now we have to use the whole hairy coercion
		       * thing.  Too bad...
		       *
		       * This case will crop up when someone tries to cast a
		       * "set of int" as a "set of float", for example.
		       */
		      setref = set_coerce (setref, (TP_DOMAIN *)
					   desired_domain,
					   (coercion_mode ==
					    TP_IMPLICIT_COERCION));
		    }

		  if (setref == NULL)
		    {
		      err = er_errid ();
		    }
		  else
		    {
		      err = db_make_sequence (target, setref);
		    }
		}
	      if (!setref || err < 0)
		{
		  status = DOMAIN_INCOMPATIBLE;
		}
	    }
	}
#endif
      break;
#endif

    case DB_TYPE_VARBIT:
      switch (original_type)
	{
	case DB_TYPE_VARBIT:
	  if (src == dest && tp_can_steal_string (src, desired_domain))
	    {
	      tp_value_slam_domain (dest, desired_domain);
	      /*
	       * Set "src" to NULL to prevent the wrapup code from undoing
	       * our work; since we haven't actually made a copy, we don't
	       * want to clear the original.
	       */
	      src = NULL;
	    }
	  else if (db_bit_string_coerce (src, target, &data_stat) != NO_ERROR)
	    {
	      status = DOMAIN_INCOMPATIBLE;
	    }
	  else if (data_stat == DATA_STATUS_TRUNCATED
		   && coercion_mode == TP_IMPLICIT_COERCION)
	    {
	      status = DOMAIN_OVERFLOW;
	      db_value_clear (target);
	    }
	  else
	    {
	      status = DOMAIN_COMPATIBLE;
	    }
	  break;

	default:
	  status = DOMAIN_INCOMPATIBLE;
	  break;
	}
      break;

    case DB_TYPE_VARCHAR:
      switch (original_type)
	{
	case DB_TYPE_VARCHAR:
	  if (src == dest && tp_can_steal_string (src, desired_domain))
	    {
	      tp_value_slam_domain (dest, desired_domain);
	      /*
	       * Set "src" to NULL to prevent the wrapup code from undoing
	       * our work; since we haven't actually made a copy, we don't
	       * want to clear the original.
	       */
	      src = NULL;
	    }
	  else if (db_char_string_coerce (src, target, &data_stat) !=
		   NO_ERROR)
	    {
	      status = DOMAIN_INCOMPATIBLE;
	    }
	  else if (data_stat == DATA_STATUS_TRUNCATED &&
		   coercion_mode == TP_IMPLICIT_COERCION)
	    {
	      status = DOMAIN_OVERFLOW;
	      db_value_clear (target);
	    }
	  else
	    {
	      status = DOMAIN_COMPATIBLE;

	      if (target != NULL
		  && TP_IS_CHAR_TYPE (DB_VALUE_DOMAIN_TYPE (target)))
		{
		  if (db_string_put_cs_and_collation (target,
						      TP_DOMAIN_COLLATION
						      (desired_domain)) !=
		      NO_ERROR)
		    {
		      status = DOMAIN_INCOMPATIBLE;
		    }
		}

	    }
	  break;

	case DB_TYPE_BIGINT:
	case DB_TYPE_INTEGER:
	  {
	    int max_size = TP_BIGINT_PRECISION + 2 + 1;
	    char *new_string;
	    DB_BIGINT num;

	    new_string = (char *) malloc (max_size);
	    if (new_string == NULL)
	      {
		status = DOMAIN_ERROR;
		goto exit_on_error;
	      }

	    if (original_type == DB_TYPE_BIGINT)
	      {
		num = DB_GET_BIGINT (src);
	      }
	    else
	      {
		assert (original_type == DB_TYPE_INTEGER);
		num = (DB_BIGINT) DB_GET_INTEGER (src);
	      }

	    if (tp_ltoa (num, new_string, 10))
	      {
		if (desired_domain->precision != TP_FLOATING_PRECISION_VALUE
		    && desired_domain->precision < (int) strlen (new_string))
		  {
		    status = DOMAIN_OVERFLOW;
		    free_and_init (new_string);
		  }
		else
		  {
		    make_desired_string_db_value (desired_type,
						  desired_domain, new_string,
						  target, &status,
						  &data_stat);
		  }
	      }
	    else
	      {
		status = DOMAIN_ERROR;
		free_and_init (new_string);
	      }
	  }
	  break;

	case DB_TYPE_DOUBLE:
	  {
	    tp_dtoa (src, target);

	    if (DB_IS_NULL (target))
	      {
		if (er_errid () == ER_OUT_OF_VIRTUAL_MEMORY)
		  {
		    /* no way to report "out of memory" from
		     * tp_value_coerce_internal() ?? */
		    status = DOMAIN_ERROR;
		  }
		else
		  {
		    status = DOMAIN_INCOMPATIBLE;
		  }
	      }
	    else
	      if (desired_domain->precision != TP_FLOATING_PRECISION_VALUE
		  && desired_domain->precision <
		  DB_GET_STRING_LENGTH (target))
	      {
		status = DOMAIN_OVERFLOW;
		(void) pr_clear_value (target);
	      }
	  }
	  break;

	case DB_TYPE_NUMERIC:
	  {
	    int max_size = 38 + 2 + 1;
	    char *new_string, *ptr;

	    new_string = (char *) malloc (max_size);
	    if (new_string == NULL)
	      {
		status = DOMAIN_ERROR;
		goto exit_on_error;
	      }

	    ptr = numeric_db_value_print ((DB_VALUE *) src);
	    strcpy (new_string, ptr);

	    if (desired_domain->precision != TP_FLOATING_PRECISION_VALUE
		&& desired_domain->precision < (int) strlen (new_string))
	      {
		status = DOMAIN_OVERFLOW;
		free_and_init (new_string);
	      }
	    else
	      {
		make_desired_string_db_value (desired_type, desired_domain,
					      new_string, target, &status,
					      &data_stat);
	      }
	  }
	  break;

	case DB_TYPE_DATE:
	case DB_TYPE_TIME:
	case DB_TYPE_DATETIME:
	  {
	    int max_size = DATETIME_BUF_SIZE;
	    char *new_string;

	    new_string = (char *) malloc (max_size);
	    if (new_string == NULL)
	      {
		status = DOMAIN_ERROR;
		goto exit_on_error;
	      }

	    if (original_type == DB_TYPE_DATE)
	      {
		db_date_to_string (new_string, max_size,
				   (DB_DATE *) DB_GET_DATE (src));
	      }
	    else if (original_type == DB_TYPE_TIME)
	      {
		db_time_to_string (new_string, max_size,
				   (DB_TIME *) DB_GET_TIME (src));
	      }
	    else
	      {
		db_datetime_to_string (new_string, max_size,
				       (DB_DATETIME *) DB_GET_DATETIME (src));
	      }

	    if (desired_domain->precision != TP_FLOATING_PRECISION_VALUE
		&& desired_domain->precision < (int) strlen (new_string))
	      {
		status = DOMAIN_OVERFLOW;
		free_and_init (new_string);
	      }
	    else
	      {
		make_desired_string_db_value (desired_type, desired_domain,
					      new_string, target, &status,
					      &data_stat);
	      }
	  }
	  break;

	default:
	  status = DOMAIN_INCOMPATIBLE;
	  break;
	}
      break;

    default:
      status = DOMAIN_INCOMPATIBLE;
      break;
    }

  if (err < 0)
    {
      status = DOMAIN_ERROR;
    }

  if (status != DOMAIN_COMPATIBLE)
    {
      if (src != dest)
	{
	  /* make sure this doesn't have any partial results */
	  if (preserve_domain)
	    {
	      db_value_domain_init (dest, desired_type,
				    desired_domain->precision,
				    desired_domain->scale);
	      db_value_put_null (dest);
	    }
	  else
	    {
	      db_make_null (dest);
	    }
	}
    }
  else if (src == dest)
    {
      /* coercsion successful, transfer the value if src == dest */
      db_value_clear (dest);
      *dest = temp;
    }

  if (status != DOMAIN_COMPATIBLE)
    {
      goto exit_on_error;
    }
#if !defined (NDEBUG)
  else
    {
      assert (desired_type == TP_DOMAIN_TYPE (desired_domain));
      if (desired_domain->is_parameterized && !DB_IS_NULL (dest))
	{
	  assert (desired_type == DB_VALUE_TYPE (dest));
	}
    }
#endif

done:
  assert (status == DOMAIN_COMPATIBLE);

  return status;

exit_on_error:
  assert (status != DOMAIN_COMPATIBLE);

  if (er_errid () == NO_ERROR)
    {
      switch (status)
	{
	case DOMAIN_INCOMPATIBLE:
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
		  pr_type_name (original_type), pr_type_name (desired_type));
	  break;

	case DOMAIN_OVERFLOW:
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IT_DATA_OVERFLOW, 1,
		  pr_type_name (desired_type));
	  break;

	default:
	  break;
	}
    }

  return status;
}

/*
 * tp_value_cast - Coerce a value into one of another domain Explicitly.
 *    return: TP_DOMAIN_STATUS
 *    src(in): src DB_VALUE
 *    dest(out): dest DB_VALUE
 *    desired_domain(in):
 *    implicit_coercion(in): flag for the coercion is implicit
 * Note:
 *    This function does select domain from desired_domain
 */
TP_DOMAIN_STATUS
tp_value_cast (const DB_VALUE * src, DB_VALUE * dest,
	       const TP_DOMAIN * desired_domain)
{
  TP_DOMAIN_STATUS status;

  assert (src != dest);

  /* defense code */
  if (TP_DOMAIN_TYPE (desired_domain) == DB_TYPE_VARIABLE)
    {
//      assert (false);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
	      pr_type_name (DB_VALUE_TYPE (src)),
	      pr_type_name (DB_TYPE_VARIABLE));

      return DOMAIN_INCOMPATIBLE;
    }

  status =
    tp_value_coerce_internal (src, dest, desired_domain, TP_EXPLICIT_COERCION,
			      false);

  return status;
}

/*
 * VALUE COMPARISON
 */

#if 0				/* DO NOT DELETE ME */
/*
 * oidcmp - Compares two OIDs and returns a DB_ style status code.
 *    return: DB_ comparison status code
 *    oid1(in): first oid
 *    oid2(in): second oid
 * Note:
 *    The underlying oid_compare should be using these so we can avoid
 *    an extra level of indirection.
 */
static int
oidcmp (OID * oid1, OID * oid2)
{
  int status;

  status = oid_compare (oid1, oid2);
  if (status < 0)
    {
      status = DB_LT;
    }
  else if (status > 0)
    {
      status = DB_GT;
    }
  else
    {
      status = DB_EQ;
    }

  return status;
}
#endif

/*
 * tp_more_general_type - compares two type with respect to generality
 *    return: 0 if same type,
 *           <0 if type1 less general then type2,
 *           >0 otherwise
 *    type1(in): first type
 *    type2(in): second type
 */
int
tp_more_general_type (const DB_TYPE type1, const DB_TYPE type2)
{
  static int rank[DB_TYPE_LAST + 1];
  static int rank_init = 0;
  int i;

#if 0				/* TODO - */
  assert ((TP_IS_DATE_TYPE (type1) && TP_IS_DATE_TYPE (type2))
	  || (TP_IS_SET_TYPE (type1) && TP_IS_SET_TYPE (type2))
	  || (TP_IS_NUMERIC_TYPE (type1) && TP_IS_NUMERIC_TYPE (type2)));
#endif

  if (type1 == type2)
    {
      return 0;
    }
  if ((unsigned) type1 > DB_TYPE_LAST)
    {
#if defined (RYE_DEBUG)
      printf ("tp_more_general_type: DB type 1 out of range: %d\n", type1);
#endif /* RYE_DEBUG */
      return 0;
    }
  if ((unsigned) type2 > DB_TYPE_LAST)
    {
#if defined (RYE_DEBUG)
      printf ("tp_more_general_type: DB type 2 out of range: %d\n", type2);
#endif /* RYE_DEBUG */
      return 0;
    }
  if (!rank_init)
    {
      /* set up rank so we can do fast table lookup */
      for (i = 0; i <= DB_TYPE_LAST; i++)
	{
	  rank[i] = 0;
	}
      for (i = 0; db_type_rank[i] < (DB_TYPE_LAST + 1); i++)
	{
	  rank[db_type_rank[i]] = i;
	}
      rank_init = 1;
    }

  return rank[type1] - rank[type2];
}

/*
 * tp_value_compare - compares two values, log error iff needed
 *    return: zero if equal, <0 if less, >0 if greater
 *    value1(in): first value
 *    value2(in): second value
 *    do_coercion(in): coercion flag
 *    total_order(in): total order flag
 *    can_compare(out): set if values are comparable
 * Note:
 *    There is some implicit conversion going on here, not sure if this
 *    is a good idea because it gives the impression that these have
 *    compatible domains.
 *
 *    If the total_order flag is set, it will return one of DB_LT, DB_GT, or
 *    DB_EQ, it will not return DB_UNK.  For the purposes of the total
 *    ordering, two NULL values are DB_EQ and if only one value is NULL, that
 *    value is less than the non-null value.
 *
 *    If "can_compare" is not null, in the event of incomparable values an
 *    error will be logged and the boolean that is pointed by "can_compare"
 *    will be set to false.
 */
DB_VALUE_COMPARE_RESULT
tp_value_compare (const DB_VALUE * value1, const DB_VALUE * value2,
		  int do_coercion, int total_order, bool * can_compare)
{
  DB_VALUE_COMPARE_RESULT cmp_res;
  DB_VALUE temp1, temp2;
  int coercion;
  DB_VALUE *v1, *v2;
  DB_TYPE vtype1, vtype2;
#if 0				/* DO NOT DELETE ME */
  DB_OBJECT *mop;
  DB_IDENTIFIER *oid1, *oid2;
#endif

  cmp_res = DB_UNK;
  coercion = 0;

  if (can_compare != NULL)
    {
      *can_compare = true;
    }

  if (DB_IS_NULL (value1))
    {
      if (DB_IS_NULL (value2))
	{
	  return (total_order ? DB_EQ : DB_UNK);
	}
      else
	{
	  return (total_order ? DB_LT : DB_UNK);
	}
    }
  else if (DB_IS_NULL (value2))
    {
      return (total_order ? DB_GT : DB_UNK);
    }

  assert (cmp_res == DB_UNK);

  v1 = (DB_VALUE *) value1;
  v2 = (DB_VALUE *) value2;

  vtype1 = DB_VALUE_DOMAIN_TYPE (v1);
  vtype2 = DB_VALUE_DOMAIN_TYPE (v2);

  /*
   * Hack, DB_TYPE_OID & DB_TYPE_OBJECT are logically the same domain
   * although their physical representations are different.
   * If we see a pair of those, handle it up front before we
   * fall in and try to perform coercion.  Avoid "coercion" between
   * OIDs and OBJECTs because we usually try to keep OIDs unswizzled
   * as long as possible.
   */
  if (vtype1 != vtype2)
    {
#if 1				/* TODO -trace */
      if (vtype1 == DB_TYPE_OBJECT || vtype2 == DB_TYPE_OBJECT)
	{
	  return DB_UNK;
	}
#else /* DO NOT DELETE ME */
      if (vtype1 == DB_TYPE_OBJECT)
	{
	  if (vtype2 == DB_TYPE_OID)
	    {
	      mop = db_get_object (v1);
	      oid1 = mop ? WS_OID (mop) : NULL;
	      oid2 = db_get_oid (v2);
	      if (oid1 && oid2)
		{
		  return oidcmp (oid1, oid2);
		}
	      else
		{
		  return DB_UNK;
		}
	    }
	}
      else if (vtype2 == DB_TYPE_OBJECT)
	{
	  if (vtype1 == DB_TYPE_OID)
	    {
	      oid1 = db_get_oid (v1);
	      mop = db_get_object (v2);
	      oid2 = mop ? WS_OID (mop) : NULL;

	      if (oid1 && oid2)
		{
		  return oidcmp (oid1, oid2);
		}
	      else
		{
		  return DB_UNK;
		}
	    }
	}
#endif

      assert (cmp_res == DB_UNK);

      /*
       * If value types aren't exact, try coercion.
       * May need to be using the domain returned by
       * tp_domain_resolve_value here ?
       */
      if (do_coercion && !ARE_COMPARABLE (vtype1, vtype2))
	{
	  TP_DOMAIN_STATUS status;

	  status = DOMAIN_INCOMPATIBLE;

	  DB_MAKE_NULL (&temp1);
	  DB_MAKE_NULL (&temp2);
	  coercion = 1;

	  assert (vtype1 != vtype2);

	  if (TP_IS_CHAR_TYPE (vtype1) && TP_IS_NUMERIC_TYPE (vtype2))
	    {
	      /* coerce v1 to double */
	      status = tp_value_coerce (v1, &temp1,
					tp_domain_resolve_default
					(DB_TYPE_DOUBLE));
	      if (status == DOMAIN_COMPATIBLE)
		{
		  v1 = &temp1;
		  vtype1 = DB_VALUE_TYPE (v1);

		  if (vtype2 != DB_TYPE_DOUBLE)
		    {
		      status = tp_value_coerce (v2, &temp2,
						tp_domain_resolve_default
						(DB_TYPE_DOUBLE));

		      if (status == DOMAIN_COMPATIBLE)
			{
			  v2 = &temp2;
			  vtype2 = DB_VALUE_TYPE (v2);
			}
		    }
		}
	    }
	  else if (TP_IS_NUMERIC_TYPE (vtype1) && TP_IS_CHAR_TYPE (vtype2))
	    {
	      /* coerce v2 to double */
	      status = tp_value_coerce (v2, &temp2,
					tp_domain_resolve_default
					(DB_TYPE_DOUBLE));
	      if (status == DOMAIN_COMPATIBLE)
		{
		  v2 = &temp2;
		  vtype2 = DB_VALUE_TYPE (v2);

		  if (vtype1 != DB_TYPE_DOUBLE)
		    {
		      status = tp_value_coerce (v1, &temp1,
						tp_domain_resolve_default
						(DB_TYPE_DOUBLE));

		      if (status == DOMAIN_COMPATIBLE)
			{
			  v1 = &temp1;
			  vtype1 = DB_VALUE_TYPE (v1);
			}
		    }
		}
	    }
	  else if (TP_IS_CHAR_TYPE (vtype1)
		   && TP_IS_DATE_OR_TIME_TYPE (vtype2))
	    {
	      /* vtype2 is the date or time type, coerce value 1 */
	      TP_DOMAIN *d2 = tp_domain_resolve_default (vtype2);
	      status = tp_value_coerce (v1, &temp1, d2);
	      if (status == DOMAIN_COMPATIBLE)
		{
		  v1 = &temp1;
		  vtype1 = DB_VALUE_TYPE (v1);
		}
	    }
	  else if (TP_IS_DATE_OR_TIME_TYPE (vtype1)
		   && TP_IS_CHAR_TYPE (vtype2))
	    {
	      /* vtype1 is the date or time type, coerce value 2 */
	      TP_DOMAIN *d1 = tp_domain_resolve_default (vtype1);
	      status = tp_value_coerce (v2, &temp2, d1);
	      if (status == DOMAIN_COMPATIBLE)
		{
		  v2 = &temp2;
		  vtype2 = DB_VALUE_TYPE (v2);
		}
	    }
	  else if (tp_more_general_type (vtype1, vtype2) > 0)
	    {
	      /* vtype1 is more general, coerce value 2 */
	      TP_DOMAIN *d1 = tp_domain_resolve_default (vtype1);

	      if (TP_TYPE_HAS_COLLATION (vtype2) && TP_IS_CHAR_TYPE (vtype1))
		{
		  /* create a new domain with type of v1 */
		  d1 = tp_domain_copy (d1);
		  assert (TP_IS_CHAR_TYPE (vtype2));

		  /* keep the codeset and collation from original
		   * value v2 */
		  d1->collation_id = DB_GET_STRING_COLLATION (v2);

		  d1 = tp_domain_cache (d1);
		}

	      status = tp_value_coerce (v2, &temp2, d1);
	      if (status != DOMAIN_COMPATIBLE)
		{
		  /*
		   * This is arguably an error condition
		   * but Not Equal is as close as we can come
		   * to reporting it.
		   */
		}
	      else
		{
		  v2 = &temp2;
		  vtype2 = DB_VALUE_TYPE (v2);
		}
	    }
	  else
	    {
	      /* coerce value1 to value2's type */
	      TP_DOMAIN *d2 = tp_domain_resolve_default (vtype2);

	      if (TP_TYPE_HAS_COLLATION (vtype1) && TP_IS_CHAR_TYPE (vtype2))
		{
		  /* create a new domain with type of v2 */
		  d2 = tp_domain_copy (d2);
		  assert (TP_IS_CHAR_TYPE (vtype1));

		  /* keep the codeset and collation from original
		   * value v1 */
		  d2->collation_id = DB_GET_STRING_COLLATION (v1);

		  d2 = tp_domain_cache (d2);
		}

	      status = tp_value_coerce (v1, &temp1, d2);
	      if (status != DOMAIN_COMPATIBLE)
		{
		  /*
		   * This is arguably an error condition
		   * but Not Equal is as close as we can come
		   * to reporting it.
		   */
		}
	      else
		{
		  v1 = &temp1;
		  vtype1 = DB_VALUE_TYPE (v1);
		}
	    }
	}
    }

  assert (cmp_res == DB_UNK);

  if (!ARE_COMPARABLE (vtype1, vtype2))
    {
      /* set incompatibility flag */
      if (can_compare != NULL)
	{
	  *can_compare = false;
	}

      /* Default status for mismatched types.
       * Not correct but will be consistent.
       */
      if (tp_more_general_type (vtype1, vtype2) > 0)
	{
	  cmp_res = DB_GT;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_TP_CANT_COERCE, 2, pr_type_name (vtype1),
		  pr_type_name (vtype2));
	}
      else
	{
	  cmp_res = DB_LT;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_TP_CANT_COERCE, 2, pr_type_name (vtype2),
		  pr_type_name (vtype1));
	}
    }
  else
    {
      int common_coll;
      PR_TYPE *pr_type;

      common_coll = -1;
      pr_type = PR_TYPE_FROM_ID (vtype1);
      assert (pr_type != NULL);

      if (pr_type)
	{
	  if (!TP_IS_CHAR_TYPE (vtype1))
	    {
	      common_coll = 0;
	    }
	  else
	    {
	      LANG_RT_COMMON_COLL (DB_GET_STRING_COLLATION (v1),
				   DB_GET_STRING_COLLATION (v2), common_coll);
	    }

	  if (common_coll == -1)
	    {
	      if (can_compare != NULL)
		{
		  *can_compare = false;
		}
	      cmp_res = DB_UNK;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_QSTR_INCOMPATIBLE_COLLATIONS, 0);
	    }
	  else
	    {
	      cmp_res = (*(pr_type->cmpval)) (v1, v2, do_coercion,
					      total_order, common_coll);
	    }
	}
      else
	{
	  /* safe guard */
	  assert (false);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_MR_NULL_DOMAIN, 0);
	  cmp_res = DB_UNK;
	}
    }

  if (coercion)
    {
      pr_clear_value (&temp1);
      pr_clear_value (&temp2);
    }

  return cmp_res;
}

/*
 * tp_value_equal - compares the contents of two DB_VALUE structures and
 * determines if they are equal
 *    return: non-zero if the values are equal
 *    value1(in): first value
 *    value2(in): second value
 *    do_coercion(): coercion flag
 * Note:
 *    determines if they are equal.  This is a boolean comparison, you
 *    cannot use this for sorting.
 *
 *    This used to be fully implemented, since this got a lot more complicated
 *    with the introduction of parameterized types, and it is doubtful that
 *    it saved much in performance anyway, it has been re-implemented to simply
 *    call tp_value_compare.  The old function is commented out below in case
 *    this causes problems.  After awhile, it can be removed.
 *
 */
int
tp_value_equal (const DB_VALUE * value1, const DB_VALUE * value2,
		int do_coercion)
{
  DB_VALUE_COMPARE_RESULT c = DB_UNK;

  c = tp_value_compare (value1, value2, do_coercion, 0, NULL);
  assert (c != DB_UNK);

  return (c == DB_EQ);
}

/*
 * DOMAIN INFO FUNCTIONS
 */


/*
 * tp_domain_disk_size - Caluclate the disk size necessary to store a value
 * for a particular domain.
 *    return: disk size in bytes. -1 if this is a variable width domain or
 *            floating precision in fixed domain.
 *    domain(in): domain to consider
 * Note:
 *    This is here because it takes a domain handle.
 *    Since this is going to get called a lot, we might want to just add
 *    this to the TP_DOMAIN structure and calculate it internally when
 *    it is cached.
 */
int
tp_domain_disk_size (TP_DOMAIN * domain)
{
  int size;

  if (domain->type->variable_p)
    {
      assert (false);
      return -1;
    }

  assert (domain->precision != TP_FLOATING_PRECISION_VALUE);

  /*
   * Use the "lengthmem" function here with a NULL pointer.  The size will
   * not be dependent on the actual value.
   * The decision of whether or not to use the lengthmem function probably
   * should be based on the value of "disksize" ?
   */
  if (domain->type->data_lengthmem != NULL)
    {
      size = (*(domain->type->data_lengthmem)) (NULL, domain, 1);
    }
  else
    {
      size = domain->type->disksize;
    }

  assert (size > 0 || (size == 0 && TP_DOMAIN_TYPE (domain) == DB_TYPE_NULL));

  return size;
}


/*
 * tp_domain_memory_size - Calculates the "instance memory" size required
 * to hold a value for a particular domain.
 *    return: bytes size
 *    domain(in): domain to consider
 */
int
tp_domain_memory_size (TP_DOMAIN * domain)
{
  int size;

  /*
   * Use the "lengthmem" function here with a NULL pointer and a "disk"
   * flag of zero.
   * This will cause it to return the instance memory size.
   */
  if (domain->type->data_lengthmem != NULL)
    {
      size = (*(domain->type->data_lengthmem)) (NULL, domain, 0);
    }
  else
    {
      size = domain->type->size;
    }

  return size;
}

/*
 * tp_init_value_domain - initializes the domain information in a DB_VALUE to
 * correspond to the information from a TP_DOMAIN structure.
 *    return: none
 *    domain(out): domain information
 *    value(in): value to initialize
 * Note:
 *    Used primarily by the value unpacking functions.
 *    It uses the "initval" type function.  This needs to be changed
 *    to take a full domain rather than just precision/scale but the
 *    currently behavior will work for now.
 *
 *    Think about the need for "initval" all it really does is call
 *    db_value_domain_init() with the supplied arguments.
 */
void
tp_init_value_domain (TP_DOMAIN * domain, DB_VALUE * value)
{
  if (domain == NULL)
    {
      /* shouldn't happen ? */
      db_value_domain_init (value, DB_TYPE_NULL, DB_DEFAULT_PRECISION,
			    DB_DEFAULT_SCALE);
    }
  else
    {
      (*(domain->type->initval)) (value, domain->precision, domain->scale);
    }
}


/*
 * tp_check_value_size - check a particular variable sized value (e.g.
 * varchar, char, bit) against a destination domain.
 *    return: domain status (ok or overflow)
 *    domain(in): target domain
 *    value(in): value to be assigned
 * Note:
 *    It is assumed that basic domain compatibility has already been
 *    performed and that the supplied domain will match with what is
 *    in the value.
 *    This is used primarily for character data that is allowed to fit
 *    within a domain if the byte size is within tolerance.
 */
TP_DOMAIN_STATUS
tp_check_value_size (TP_DOMAIN * domain, DB_VALUE * value)
{
  TP_DOMAIN_STATUS status;
  int src_precision, src_length;
  DB_TYPE dbtype;
  char *src;

  status = DOMAIN_COMPATIBLE;

  /* if target domain is "floating", its always ok */
  if (domain->precision != TP_FLOATING_PRECISION_VALUE)
    {

      dbtype = TP_DOMAIN_TYPE (domain);
      switch (dbtype)
	{
	case DB_TYPE_VARCHAR:
	case DB_TYPE_VARBIT:
	  /*
	   * The compatibility of the value is always determined by the
	   * actual length of the value, not the destination precision.
	   */
	  src = DB_GET_STRING (value);
	  if (src != NULL)
	    {
	      if (dbtype == DB_TYPE_VARCHAR)
		{
		  src_length = db_get_string_length (value);
		  assert (src_length >= 0);
		}
	      else
		{
		  src_length = db_get_string_size (value);
		}

	      /*
	       * Work backwards from the source length into a minimum precision.
	       * This feels like it should be a nice packed utility
	       * function somewhere.
	       */
	      src_precision = src_length;

	      if (src_precision > domain->precision)
		{
		  status = DOMAIN_OVERFLOW;
		}
	    }
	  break;

	default:
	  /*
	   * None of the other types require this form of value dependent domain
	   * precision checking.
	   */
	  break;
	}
    }

  return status;
}

#if defined(RYE_DEBUG)
/*
 * fprint_domain - print information of a domain
 *    return: void
 *    fp(out): FILE pointer
 *    domain(in): domain to print
 */
static void
fprint_domain (FILE * fp, TP_DOMAIN * domain)
{
  TP_DOMAIN *d;

  for (d = domain; d != NULL; d = d->next)
    {

      switch (TP_DOMAIN_TYPE (d))
	{

	case DB_TYPE_OBJECT:
	case DB_TYPE_OID:
	case DB_TYPE_SUB:
	  if (TP_DOMAIN_TYPE (d) == DB_TYPE_SUB)
	    {
	      fprintf (fp, "sub(");
	    }
#if !defined (SERVER_MODE)
	  if (d->class_mop != NULL)
	    {
	      fprintf (fp, "%s", sm_class_name (d->class_mop));
	    }
	  else if (OID_ISNULL (&d->class_oid))
	    {
	      fprintf (fp, "object");
	    }
	  else
#endif /* !SERVER_MODE */
	    {
	      fprintf (fp, "object(%d,%d,%d)",
		       d->class_oid.volid, d->class_oid.pageid,
		       d->class_oid.slotid);
	    }
	  if (TP_DOMAIN_TYPE (d) == DB_TYPE_SUB)
	    {
	      fprintf (fp, ")");
	    }
	  break;

	case DB_TYPE_VARIABLE:
	  fprintf (fp, "union(");
	  fprint_domain (fp, d->setdomain);
	  fprintf (fp, ")");
	  break;

	case DB_TYPE_SEQUENCE:
	  fprintf (fp, "sequence(");
	  fprint_domain (fp, d->setdomain);
	  fprintf (fp, ")");
	  break;

	case DB_TYPE_VARBIT:
	  fprintf (fp, "%s(%d)", d->type->name, d->precision);
	  break;

	case DB_TYPE_VARCHAR:
	  fprintf (fp, "%s(%d) collate %s", d->type->name, d->precision,
		   lang_get_collation_name (d->collation_id));
	  break;

	case DB_TYPE_NUMERIC:
	  fprintf (fp, "%s(%d,%d)", d->type->name, d->precision, d->scale);
	  break;

	default:
	  fprintf (fp, "%s", d->type->name);
	  break;
	}

      if (d->next != NULL)
	{
	  fprintf (fp, ",");
	}
    }
}

/*
 * tp_dump_domain - fprint_domain to stdout
 *    return: void
 *    domain(in): domain to print
 */
void
tp_dump_domain (TP_DOMAIN * domain)
{
  fprint_domain (stdout, domain);
  fprintf (stdout, "\n");
}

/*
 * tp_domain_print - fprint_domain to stdout
 *    return: void
 *    domain(in): domain to print
 */
void
tp_domain_print (TP_DOMAIN * domain)
{
  fprint_domain (stdout, domain);
}

/*
 * tp_domain_fprint - fprint_domain to stdout
 *    return: void
 *    fp(out): FILE pointer
 *    domain(in): domain to print
 */
void
tp_domain_fprint (FILE * fp, TP_DOMAIN * domain)
{
  fprint_domain (fp, domain);
}
#endif

/*
 * tp_valid_indextype - check for valid index type
 *    return: 1 if type is a valid index type, 0 otherwise.
 *    type(in): a database type constant
 */
int
tp_valid_indextype (DB_TYPE type)
{
  switch (type)
    {
    case DB_TYPE_INTEGER:
    case DB_TYPE_DOUBLE:
    case DB_TYPE_VARCHAR:
#if 0
      /* server doesn't treat DB_TYPE_OBJECT, so that convert it to
         DB_TYPE_OID */
    case DB_TYPE_OBJECT:
#endif
    case DB_TYPE_TIME:
    case DB_TYPE_DATETIME:
    case DB_TYPE_DATE:
    case DB_TYPE_BIGINT:
    case DB_TYPE_OID:
    case DB_TYPE_NUMERIC:
      return 1;
    default:
      return 0;
    }
}


/*
 * tp_value_str_cast_to_number () - checks if the original value
 *	  is of type string, and cast it to a DOUBLE type domain.
 *   return: error code.
 *   src(in): source DB_VALUE
 *   dest(out): destination DB_VALUE
 *   val_type(in/out): db type of value; modified if the cast is performed
 *
 *  Note : this is a helper function used by arithmetic functions to accept
 *	   string arguments.
 */
int
tp_value_str_cast_to_number (DB_VALUE * src, DB_VALUE * dest,
			     DB_TYPE * val_type)
{
  TP_DOMAIN *cast_dom = NULL;
  TP_DOMAIN_STATUS dom_status;
  int er_status = NO_ERROR;

  assert (src != NULL);
  assert (dest != NULL);
  assert (val_type != NULL);
  assert (TP_IS_CHAR_TYPE (*val_type));
  assert (src != dest);

  DB_MAKE_NULL (dest);

  /* cast string to DOUBLE */
  cast_dom = tp_domain_resolve_default (DB_TYPE_DOUBLE);
  if (cast_dom == NULL)
    {
      return ER_FAILED;
    }

  dom_status = tp_value_coerce (src, dest, cast_dom);
  if (dom_status != DOMAIN_COMPATIBLE)
    {
      er_status = tp_domain_status_er_set (dom_status, ARG_FILE_LINE,
					   src, cast_dom);

      pr_clear_value (dest);
      return er_status;
    }

  *val_type = DB_VALUE_DOMAIN_TYPE (dest);

  return NO_ERROR;
}

/*
 * tp_infer_common_domain () -
 *   return:
 *
 *   arg1(in):
 *   arg2(in):
 *   force_arg1(in):
 *
 *  Note :
 */
TP_DOMAIN *
tp_infer_common_domain (TP_DOMAIN * arg1, TP_DOMAIN * arg2, bool force_arg1)
{
  TP_DOMAIN *target_domain = NULL;
  DB_TYPE arg1_type, arg2_type, common_type;
  int common_coll = -1;
  bool need_to_domain_update = false;

  assert (arg1 != NULL);
  assert (arg2 != NULL);

  arg1_type = TP_DOMAIN_TYPE (arg1);
  arg2_type = TP_DOMAIN_TYPE (arg2);

  /* get collation */
  if (TP_IS_CHAR_TYPE (arg1_type) && TP_IS_CHAR_TYPE (arg2_type))
    {
      /* check collation */
      LANG_RT_COMMON_COLL (TP_DOMAIN_COLLATION (arg1),
			   TP_DOMAIN_COLLATION (arg2), common_coll);
      if (common_coll == -1)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QSTR_INCOMPATIBLE_COLLATIONS, 0);

	  return NULL;
	}
    }

  if (arg1_type == arg2_type || force_arg1)
    {
      common_type = arg1_type;
      target_domain = tp_domain_copy (arg1);
      need_to_domain_update = true;
    }
  else if (arg1_type == DB_TYPE_NULL)
    {
      common_type = arg2_type;
      target_domain = tp_domain_copy (arg2);
    }
  else if (arg2_type == DB_TYPE_NULL)
    {
      common_type = arg1_type;
      target_domain = tp_domain_copy (arg1);
    }
  else if ((TP_IS_DATE_TYPE (arg1_type) && TP_IS_DATE_TYPE (arg2_type))
	   || (TP_IS_SET_TYPE (arg1_type) && TP_IS_SET_TYPE (arg2_type))
	   || (TP_IS_NUMERIC_TYPE (arg1_type)
	       && TP_IS_NUMERIC_TYPE (arg2_type)))
    {
      if (tp_more_general_type (arg1_type, arg2_type) > 0)
	{
	  common_type = arg1_type;
	  target_domain = tp_domain_copy (arg1);
	}
      else
	{
	  common_type = arg2_type;
	  target_domain = tp_domain_copy (arg2);
	}
      need_to_domain_update = true;
    }
  else if (TP_IS_CHAR_TYPE (arg1_type)
	   && (TP_IS_NUMERIC_TYPE (arg2_type) || TP_IS_DATE_TYPE (arg2_type)))
    {
      common_type = arg2_type;
      target_domain = tp_domain_copy (arg2);
      need_to_domain_update = true;
    }
  else if ((TP_IS_NUMERIC_TYPE (arg1_type) || TP_IS_DATE_TYPE (arg1_type))
	   && TP_IS_CHAR_TYPE (arg2_type))
    {
      common_type = arg1_type;
      target_domain = tp_domain_copy (arg1);
      need_to_domain_update = true;
    }
  else
    {
      common_type = DB_TYPE_VARCHAR;
      target_domain = tp_domain_resolve_default (common_type);
      target_domain = tp_domain_copy (target_domain);
    }

  if (need_to_domain_update)
    {
      int arg1_prec, arg2_prec, arg1_scale, arg2_scale;

      arg1_prec = arg1->precision;
      arg1_scale = arg1->scale;

      arg2_prec = arg2->precision;
      arg2_scale = arg2->scale;

      if (arg1_prec == TP_FLOATING_PRECISION_VALUE
	  || arg2_prec == TP_FLOATING_PRECISION_VALUE)
	{
	  target_domain->precision = TP_FLOATING_PRECISION_VALUE;
	  target_domain->scale = 0;
	}
      else if (common_type == DB_TYPE_NUMERIC)
	{
	  int integral_digits1, integral_digits2;

	  integral_digits1 = arg1_prec - arg1_scale;
	  integral_digits2 = arg2_prec - arg2_scale;
	  target_domain->scale = MAX (arg1_scale, arg2_scale);
	  target_domain->precision =
	    (target_domain->scale + MAX (integral_digits1, integral_digits2));
	  target_domain->precision =
	    MIN (target_domain->precision, DB_MAX_NUMERIC_PRECISION);
	}
      else
	{
	  target_domain->precision = MAX (arg1_prec, arg2_prec);
	  target_domain->scale = 0;
	}
    }

  /* set collation */
  if (TP_IS_CHAR_TYPE (arg1_type) && TP_IS_CHAR_TYPE (arg2_type))
    {
      assert (common_coll != -1);

      target_domain->collation_id = common_coll;
    }

  assert (target_domain->next_list == NULL);
  assert (target_domain->is_cached == 0);
  assert (target_domain->next == NULL);

  target_domain = tp_domain_cache (target_domain);

#if !defined (NDEBUG)
  if (target_domain != NULL)
    {
      assert (target_domain->is_cached);
      assert (target_domain->next == NULL);
    }
#endif

  return target_domain;
}

/*
 * tp_domain_status_er_set () -
 *   return:
 *
 *  Note :
 */
int
tp_domain_status_er_set (TP_DOMAIN_STATUS status, const char *file_name,
			 const int line_no, const DB_VALUE * src,
			 const TP_DOMAIN * domain)
{
  int error = NO_ERROR;

  assert (src != NULL);
  assert (domain != NULL);

  /* prefer to change error code; hide internal errors from users view
   */
  if (status == DOMAIN_ERROR)
    {
      error = er_errid ();

      if (error == ER_IT_DATA_OVERFLOW)
	{
	  status = DOMAIN_OVERFLOW;
	}
      else
	{
	  status = DOMAIN_INCOMPATIBLE;
	}
    }

  assert (status != DOMAIN_ERROR);

  switch (status)
    {
    case DOMAIN_INCOMPATIBLE:
      error = ER_TP_CANT_COERCE;
      er_set (ER_ERROR_SEVERITY, file_name, line_no, error, 2,
	      pr_type_name (DB_VALUE_DOMAIN_TYPE (src)),
	      pr_type_name (TP_DOMAIN_TYPE (domain)));
      break;

    case DOMAIN_OVERFLOW:
      error = ER_IT_DATA_OVERFLOW;
      er_set (ER_ERROR_SEVERITY, file_name, line_no, error, 1,
	      pr_type_name (TP_DOMAIN_TYPE (domain)));
      break;

    case DOMAIN_ERROR:
      assert (false);		/* is impossible */
      break;

    default:
      assert (false);		/* is impossible */
      break;
    }

  return error;
}
