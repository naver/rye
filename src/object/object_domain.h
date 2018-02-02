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
 * object_domain.h: data type definitions
 */

#ifndef _OBJECT_DOMAIN_H_
#define _OBJECT_DOMAIN_H_

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include "error_manager.h"
#include "object_representation.h"

/*
 * TP_DOMAIN_SELF_REF is used as an argument
 * to tp_domain_construct so that the self_ref flag can be set
 */
#define TP_DOMAIN_SELF_REF -1

typedef struct tp_domain
{
  struct tp_domain *next;	/* next in the same domain list */
  struct tp_domain *next_list;	/* next domain list */
  struct pr_type *type;

  int precision;
  int scale;

  struct db_object *class_mop;	/* swizzled class oid if on client */
  struct tp_domain *setdomain;	/* hierarchical domain for sets */

  OID class_oid;		/* Class OID if type is tp_Object */

  /* built-in reference number */
  int built_in_index;

  int collation_id;		/* collation identifier */

  unsigned self_ref:1;		/* object self reference */
  /*
   * merge this with self_ref when we get a chance to rebuild the whole
   * system
   */
  unsigned is_cached:1;		/* set when the domain has been cached */

  /* non-zero if this type can be parameterized */
  unsigned is_parameterized:1;

  /* run-time flag used during domain comparison */
  unsigned is_visited:1;
} TP_DOMAIN;


/*
 * We probably should make this 0 rather than -1 so that we can more easily
 * represent precisions with unsigned integers.  Zero is not a valid
 * precision.
 */
#define TP_FLOATING_PRECISION_VALUE -1

/*
 * TP_ALLOC_CONTEXT
 *    This structure is used in places where the storage allocation of
 *    certain structures must be controlled.
 *
 */

typedef struct tp_alloc_context
{
  void *(*alloc_func) (int size, ...);	/* optional second argument */
  void *alloc_args;
  void (*free_func) (void *mem, ...);	/* optional second argument */
  void *free_args;
} TP_ALLOC_CONTEXT;

#define TP_ALLOC(con, size) \
  (*(con)->alloc_func)(size, (con)->alloc_args)

#define TP_FREE(con, mem) \
  (*(con)->free_func)(mem, (con)->free_args)


/*
 * BUILT IN DOMAINS
 */

extern TP_DOMAIN tp_Null_domain;
extern TP_DOMAIN tp_Integer_domain;
extern TP_DOMAIN tp_Bigint_domain;
extern TP_DOMAIN tp_Double_domain;
extern TP_DOMAIN tp_String_domain;
extern TP_DOMAIN tp_Object_domain;
extern TP_DOMAIN tp_Sequence_domain;
extern TP_DOMAIN tp_Time_domain;
extern TP_DOMAIN tp_Date_domain;
extern TP_DOMAIN tp_Variable_domain;
extern TP_DOMAIN tp_Substructure_domain;
extern TP_DOMAIN tp_Oid_domain;
extern TP_DOMAIN tp_Numeric_domain;
extern TP_DOMAIN tp_Char_domain;
extern TP_DOMAIN tp_VarBit_domain;

/*
 * TP_DOMAIN_STATUS
 *    This is used to defined a set of possible return codes
 *    for the domain comparison and checking functions.
 *    These don't set errors but rather rely on the higher level
 *    modules to set a more appropriate error.
 */

typedef enum tp_domain_status
{
  DOMAIN_COMPATIBLE = 0,	/* success */
  DOMAIN_INCOMPATIBLE,		/* can't be coerced */
  DOMAIN_OVERFLOW,		/* value out of range */
  DOMAIN_ERROR			/* an error has been set */
} TP_DOMAIN_STATUS;

/*
 * TP_MATCH
 *    This is used to describe the amount of "tolerance" to be exhibited by
 *    domain matching functions when looking for compatible domains.
 */
typedef enum tp_match
{
  TP_EXACT_MATCH,		/* Domain must match exactly.  */
  TP_STR_MATCH,			/* "String" domains match subject to length
				 * constraints and fixed/varying types, i.e., a
				 * varying domain with precision n "matches" a
				 * fixed domain with precision m if n >= m.
				 * Only used in very special circumstances
				 * where we're trying to avoid copying strings.
				 */

  TP_SET_MATCH
} TP_MATCH;

/*
 * TP_IS_SET_TYPE
 *    Macros for detecting the set types, saves a function call.
 */

#define TP_IS_SET_TYPE(typenum) \
  ((((typenum) == DB_TYPE_SEQUENCE)) ? true : false)

/*
 * TP_IS_VARBIT_TYPE
 *    Tests to see if the type id is one of the binary string types.
 */

#define TP_IS_VARBIT_TYPE(typeid) \
  (((typeid) == DB_TYPE_VARBIT))

/*
 * TP_IS_CHAR_TYPE
 *    Tests to see if a type is any one of the character types.
 */

#define TP_IS_CHAR_TYPE(typeid) \
  (((typeid) == DB_TYPE_VARCHAR))

/*
 * TP_IS_CHAR_BIT_TYPE
 *    Tests to see if a type is one of the character or bit types.
 */

#define TP_IS_CHAR_BIT_TYPE(typeid) (TP_IS_CHAR_TYPE(typeid) \
                                     || TP_IS_VARBIT_TYPE(typeid))

#define TP_IS_NUMERIC_TYPE(typeid) \
  (((typeid) == DB_TYPE_INTEGER) \
   || ((typeid) == DB_TYPE_DOUBLE)  \
   || ((typeid) == DB_TYPE_NUMERIC) || ((typeid) == DB_TYPE_BIGINT))

#define TP_IS_DOUBLE_ALIGN_TYPE(typeid) \
  ((typeid) == DB_TYPE_DOUBLE || (typeid) == DB_TYPE_BIGINT)

#define TP_IS_DATE_TYPE(typeid) \
  (((typeid) == DB_TYPE_DATE) || ((typeid) == DB_TYPE_DATETIME))

#define TP_IS_DATE_OR_TIME_TYPE(typeid) \
  (((typeid) == DB_TYPE_TIME) || TP_IS_DATE_TYPE(typeid))

#define TP_IS_FLOATING_NUMBER_TYPE(typeid) \
  (((typeid) == DB_TYPE_DOUBLE) || ((typeid) == DB_TYPE_NUMERIC))

/*
 * Precision for non-parameterized predefined types
 *
 * SQL-3 says time and timestamp types should be parameterized.
 * Should Cubrid implement the standard then the corresponding time/timestamp
 * macros found here would be removed and placed in dbtype.h similar to
 * the numeric macros (DB_MAX_NUMERIC_PRECISION).
 */

#define TP_DOUBLE_MANTISA_BINARY_PRECISION  53
#define TP_DOUBLE_EXPONENT_BINARY_PRECISION 11
#define TP_DOUBLE_BINARY_PRECISION	    TP_DOUBLE_MANTISA_BINARY_PRECISION

#define TP_DOUBLE_MANTISA_DECIMAL_PRECISION   16	/* 15.955 */
#define TP_DOUBLE_EXPONENT_DECIMAL_PRECISION  3	/*  3.311 */
#define TP_DOUBLE_DECIMAL_PRECISION	    TP_DOUBLE_MANTISA_DECIMAL_PRECISION

/* add 4 for exponent and mantisa sign, decimal point and the exponent
 * introducer 'e' in a floating-point literal */
#define TP_DOUBLE_AS_CHAR_LENGTH \
    (4 + TP_DOUBLE_MANTISA_DECIMAL_PRECISION + \
    TP_DOUBLE_EXPONENT_DECIMAL_PRECISION)

#define TP_BIGINT_PRECISION	      19
#define TP_BIGINT_SCALE		      0
#define TP_BIGINT_AS_CHAR_LENGTH      20

#define TP_INTEGER_PRECISION	      10
#define TP_INTEGER_SCALE	      0
#define TP_INTEGER_AS_CHAR_LENGTH     11

#define TP_TIME_PRECISION	      6
#define TP_TIME_AS_CHAR_LENGTH	      11

#define TP_DATE_PRECISION	      8
#define TP_DATE_AS_CHAR_LENGTH	      10

#define TP_DATETIME_PRECISION	      17
#define TP_DATETIME_AS_CHAR_LENGTH    26

#define TP_DOMAIN_TYPE(dom) \
   ((dom) ? (dom)->type->id : DB_TYPE_NULL)

#define TP_TYPE_HAS_COLLATION(typeid) (TP_IS_CHAR_TYPE(typeid))

#define TP_DOMAIN_COLLATION(dom) \
    ((dom) ? (dom)->collation_id : LANG_SYS_COLLATION)

/*
 * FUNCTIONS
 */

/* called during workspace initialization */

extern void tp_area_init (void);

/* Domain support functions */

extern void tp_init (void);
extern void tp_apply_sys_charset (void);
extern void tp_final (void);
#if defined (ENABLE_UNUSED_FUNCTION)
extern TP_DOMAIN *tp_domain_resolve (DB_TYPE domain_type,
				     DB_OBJECT * class_obj,
				     int precision,
				     int scale, TP_DOMAIN * setdomain,
				     int collation);
#endif
extern TP_DOMAIN *tp_domain_resolve_default (DB_TYPE type);
#if defined (ENABLE_UNUSED_FUNCTION)
extern TP_DOMAIN *tp_domain_resolve_default_w_coll (DB_TYPE type,
						    int coll_id);
#endif

extern void tp_domain_free (TP_DOMAIN * dom);
extern TP_DOMAIN *tp_domain_new (DB_TYPE type);
extern TP_DOMAIN *tp_domain_copy (const TP_DOMAIN * dom);
extern TP_DOMAIN *tp_domain_construct (DB_TYPE domain_type,
				       DB_OBJECT * class_obj,
				       int precision,
				       int scale, TP_DOMAIN * setdomain);

extern void tp_init_value_domain (TP_DOMAIN * domain, DB_VALUE * value);

extern TP_DOMAIN *tp_domain_cache (TP_DOMAIN * domain);

extern int tp_domain_add (TP_DOMAIN ** dlist, TP_DOMAIN * domain);

#if defined(ENABLE_UNUSED_FUNCTION)
extern int tp_domain_drop (TP_DOMAIN ** dlist, TP_DOMAIN * domain);
#endif

extern int tp_domain_filter_list (TP_DOMAIN * dlist);

extern int tp_domain_size (const TP_DOMAIN * domain);

extern int tp_domain_match (const TP_DOMAIN * dom1, const TP_DOMAIN * dom2,
			    TP_MATCH exact);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int tp_domain_match_ignore_order (const TP_DOMAIN * dom1,
					 const TP_DOMAIN * dom2,
					 TP_MATCH exact);
extern int tp_domain_compatible (const TP_DOMAIN * dom1,
				 const TP_DOMAIN * dom2);
#endif /* ENABLE_UNUSED_FUNCTION */

extern TP_DOMAIN *tp_domain_select (const TP_DOMAIN * domain_list,
				    const DB_VALUE * value,
				    TP_MATCH exact_match);

extern TP_DOMAIN_STATUS tp_domain_check (const TP_DOMAIN * domain,
					 const DB_VALUE * value,
					 TP_MATCH exact_match);
#if defined(ENABLE_UNUSED_FUNCTION)
TP_DOMAIN *tp_domain_find_noparam (DB_TYPE type);
#endif
TP_DOMAIN *tp_domain_find_numeric (DB_TYPE type, int precision, int scale);
TP_DOMAIN *tp_domain_find_charbit (DB_TYPE type, int collation_id,
				   int precision);
#if 0
TP_DOMAIN *tp_domain_find_object (DB_TYPE type, OID * class_oid,
				  struct db_object *class_);
#endif
TP_DOMAIN *tp_domain_find_set (DB_TYPE type, TP_DOMAIN * setdomain);
TP_DOMAIN *tp_domain_resolve_value (const DB_VALUE * val);
#if defined(ENABLE_UNUSED_FUNCTION)
TP_DOMAIN *tp_create_domain_resolve_value (DB_VALUE * val,
					   TP_DOMAIN * domain);
#endif /* ENABLE_UNUSED_FUNCTION */
int tp_can_steal_string (const DB_VALUE * val,
			 const DB_DOMAIN * desired_domain);

int tp_get_fixed_precision (DB_TYPE domain_type);

/* value functions */

extern TP_DOMAIN_STATUS tp_value_coerce (const DB_VALUE * src,
					 DB_VALUE * dest,
					 const TP_DOMAIN * desired_domain);
extern int tp_value_coerce_strict (const DB_VALUE * src, DB_VALUE * dest,
				   const TP_DOMAIN * desired_domain);

extern TP_DOMAIN_STATUS tp_value_cast (const DB_VALUE * src, DB_VALUE * dest,
				       const TP_DOMAIN * desired_domain);

extern int tp_value_equal (const DB_VALUE * value1,
			   const DB_VALUE * value2, int allow_coercion);

extern int tp_more_general_type (const DB_TYPE type1, const DB_TYPE type2);

extern DB_VALUE_COMPARE_RESULT tp_value_compare (const DB_VALUE * value1,
						 const DB_VALUE * value2,
						 int allow_coercion,
						 int total_order,
						 bool * can_compare);

/* misc info */

extern int tp_domain_disk_size (TP_DOMAIN * domain);
extern int tp_domain_memory_size (TP_DOMAIN * domain);
extern TP_DOMAIN_STATUS tp_check_value_size (TP_DOMAIN * domain,
					     DB_VALUE * value);

extern int tp_valid_indextype (DB_TYPE type);
#if defined(RYE_DEBUG)
extern void tp_dump_domain (TP_DOMAIN * domain);
extern void tp_domain_print (TP_DOMAIN * domain);
extern void tp_domain_fprint (FILE * fp, TP_DOMAIN * domain);
#endif
extern int tp_domain_attach (TP_DOMAIN ** dlist, TP_DOMAIN * domain);

extern int tp_value_str_cast_to_number (DB_VALUE * src,
					DB_VALUE * dest, DB_TYPE * val_type);
extern TP_DOMAIN *tp_infer_common_domain (TP_DOMAIN * arg1, TP_DOMAIN * arg2,
					  bool force_arg1);
extern int tp_value_string_to_double (const DB_VALUE * value,
				      DB_VALUE * result);
extern int tp_domain_status_er_set (TP_DOMAIN_STATUS status,
				    const char *file_name, const int line_no,
				    const DB_VALUE * src,
				    const TP_DOMAIN * domain);
#endif /* _OBJECT_DOMAIN_H_ */
