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
 * transform_cl.c: Functions related to the storage of instances and classes.
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <string.h>

#include "porting.h"
#include "memory_alloc.h"
#include "error_manager.h"
#include "work_space.h"
#include "oid.h"
#include "object_representation.h"
#include "object_domain.h"
#include "object_primitive.h"
#include "class_object.h"
#include "set_object.h"
#include "transform.h"
#include "transform_cl.h"
#include "schema_manager.h"
#include "locator_cl.h"
#include "object_accessor.h"
#include "locator.h"
#include "server_interface.h"
#include "execute_statement.h"

/* this must be the last header file included!!! */
#include "dbval.h"

/*
 * Set functions
 *    These are shorthand macros for functions that are required as
 *    arguments to the variable set functions below.
 *
 */
typedef int (*LSIZER) (void *);
typedef void (*LWRITER) (void *, void *);
typedef DB_LIST *(*LREADER) (void *);
typedef void (*VREADER) (void *, void *);

/*
 * Forward declaration of local functions
 */
static int put_varinfo (OR_BUF * buf, char *obj, SM_CLASS * class_,
			int offset_size);
static int object_size (SM_CLASS * class_, MOBJ obj, int *offset_size_ptr);
static int put_attributes (OR_BUF * buf, char *obj, SM_CLASS * class_);
static char *get_current (OR_BUF * buf, SM_CLASS * class_, MOBJ * obj_ptr,
			  int bound_bit_flag, int offset_size);
#if defined (ENABLE_UNUSED_FUNCTION)
static SM_ATTRIBUTE *find_current_attribute (SM_CLASS * class_, int id);
static void clear_new_unbound (char *obj, SM_CLASS * class_,
			       SM_REPRESENTATION * oldrep);
static char *get_old (OR_BUF * buf, SM_CLASS * class_, MOBJ * obj_ptr,
		      int repid, int bound_bit_flag, int offset_size);
#endif
static char *unpack_allocator (int size);
static OR_VARINFO *read_var_table (OR_BUF * buf, int nvars);
static OR_VARINFO *read_var_table_internal (OR_BUF * buf, int nvars,
					    int offset_size);
static void free_var_table (OR_VARINFO * vars);
static int string_disk_size (const char *string);
static char *get_string (OR_BUF * buf, int length);
static void put_string (OR_BUF * buf, const char *string);
static int substructure_set_size (DB_LIST * list, LSIZER function);
static void put_substructure_set (OR_BUF * buf, DB_LIST * list,
				  LWRITER writer, OID * class_, int repid);
static DB_LIST *get_substructure_set (OR_BUF * buf, int *num_list,
				      LREADER reader, int expected);
static void install_substructure_set (OR_BUF * buf, DB_LIST * list,
				      VREADER reader, int expected);
static int property_list_size (DB_SEQ * properties);
static void put_property_list (OR_BUF * buf, DB_SEQ * properties);
static DB_SEQ *get_property_list (OR_BUF * buf, int expected_size);
static int domain_size (TP_DOMAIN * domain);
static void domain_to_disk (OR_BUF * buf, TP_DOMAIN * domain);
static TP_DOMAIN *disk_to_domain2 (OR_BUF * buf);
static TP_DOMAIN *disk_to_domain (OR_BUF * buf);

/* query spec */
static int query_spec_to_disk (OR_BUF * buf, SM_QUERY_SPEC * statement);
static int query_spec_size (SM_QUERY_SPEC * statement);
static SM_QUERY_SPEC *disk_to_query_spec (OR_BUF * buf);

/* disk constraint */
static int constraint_size (SM_DISK_CONSTRAINT * disk_cons);
static int disk_constraint_attribute_size (SM_DISK_CONSTRAINT_ATTRIBUTE *
					   disk_cons_att);
static SM_DISK_CONSTRAINT *disk_to_constraint (OR_BUF * buf);
static SM_DISK_CONSTRAINT_ATTRIBUTE *disk_to_constraint_attribute (OR_BUF *
								   buf);
static int constraint_attribute_to_disk (OR_BUF * buf,
					 SM_DISK_CONSTRAINT_ATTRIBUTE *
					 constraint_attribute);
static int disk_constraint_to_disk (OR_BUF * buf,
				    SM_DISK_CONSTRAINT * disk_cons);

/* attribute */
static int attribute_to_disk (OR_BUF * buf, SM_ATTRIBUTE * att);
static int attribute_size (SM_ATTRIBUTE * att);
static void disk_to_attribute (OR_BUF * buf, SM_ATTRIBUTE * att);

/* representation */
static int repattribute_to_disk (OR_BUF * buf, SM_REPR_ATTRIBUTE * rat);
static int repattribute_size (SM_REPR_ATTRIBUTE * rat);
static SM_REPR_ATTRIBUTE *disk_to_repattribute (OR_BUF * buf);
static int representation_size (SM_REPRESENTATION * rep);
static int representation_to_disk (OR_BUF * buf, SM_REPRESENTATION * rep);
static SM_REPRESENTATION *disk_to_representation (OR_BUF * buf);


static int check_class_structure (SM_CLASS * class_);
static int put_class_varinfo (OR_BUF * buf, SM_CLASS * class_);
static void put_class_attributes (OR_BUF * buf, SM_CLASS * class_);
static void class_to_disk (OR_BUF * buf, SM_CLASS * class_);
static SM_CLASS *disk_to_class (OR_BUF * buf, SM_CLASS ** class_ptr);

static void root_to_disk (OR_BUF * buf, ROOT_CLASS * root);
static int root_size (MOBJ rootobj);
static int tf_class_size (MOBJ classobj);
static ROOT_CLASS *disk_to_root (OR_BUF * buf);

static int tf_attribute_default_expr_to_property (SM_ATTRIBUTE * attr_list);

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * tf_find_temporary_oids - walks over the memory representation of an
 * instance and finds all the temporary OIDs within it and adds them to the
 * oidset.
 *    return: NO_ERROR if successful, error code otherwise
 *    oidset(out): oidset to populate
 *    classobj(in): object to examine
 */
int
tf_find_temporary_oids (LC_OIDSET * oidset, MOBJ classobj, MOBJ obj)
{
  int error = NO_ERROR;
  SM_CLASS *class_;
  SM_ATTRIBUTE *att;
  DB_TYPE type;
  SETOBJ *col;

  /*
   * Do this only for instance objects.  This means that class objects
   * with temporary oids in, say, class variables won't get flushed at
   * this time, but that's probably ok.  They'll get flushed as part of
   * the ordinary class object packing business, and since they ought to
   * be few in number, that shouldn't have much of an impact.
   */
  if ((classobj != (MOBJ) (&sm_Root_class)) && (obj != NULL))
    {

      class_ = (SM_CLASS *) classobj;

      for (att = class_->attributes; att != NULL && error == NO_ERROR;
	   att = (SM_ATTRIBUTE *) att->header.next)
	{
	  assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

	  type = TP_DOMAIN_TYPE (att->sma_domain);
	  if (type == DB_TYPE_OBJECT)
	    {
	      WS_MEMOID *mem;
	      OID *oid;
	      mem = (WS_MEMOID *) (obj + att->offset);
	      oid = &mem->oid;

	      if (OID_ISTEMP (oid) && mem->pointer != NULL
		  && !mem->pointer->deleted)
		{

		  /* Make sure the ws_memoid mop is temporary. */
		  if (OID_ISTEMP (WS_OID (mem->pointer)))
		    {

		      /* its an undeleted temporary object, promote */
		      if (locator_add_oidset_object
			  (oidset, mem->pointer) == NULL)
			{
			  error = er_errid ();
			}
		    }
		}
	    }
	  else if (TP_IS_SET_TYPE (type))
	    {
	      col = *(SETOBJ **) (obj + att->offset);

	      error = setobj_find_temporary_oids (col, oidset);
	    }
	}
    }

  return error;
}
#endif /* ENABLE_UNUSED_FUNCTION */


/*
 * put_varinfo - Write the variable attribute offset table for an instance.
 *    return:  NO_ERROR
 *    buf(in/out): translation buffer
 *    obj(in): instance memory
 *    class(in): class of instance
 */
static int
put_varinfo (OR_BUF * buf, char *obj, SM_CLASS * class_, int offset_size)
{
  SM_ATTRIBUTE *att;
  char *mem;
  int a, offset, len;
  int rc = NO_ERROR;

  if (class_->variable_count)
    {

      /* calculate offset to first variable value */
      offset = OR_HEADER_SIZE +
	OR_VAR_TABLE_SIZE_INTERNAL (class_->variable_count, offset_size) +
	class_->fixed_size + OR_BOUND_BIT_BYTES (class_->fixed_count);

      for (a = class_->fixed_count; a < class_->att_count; a++)
	{
	  att = &class_->attributes[a];
	  mem = obj + att->offset;

	  assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

	  if (att->sma_domain->type->data_lengthmem != NULL)
	    {
	      len =
		(*(att->sma_domain->type->data_lengthmem)) (mem,
							    att->sma_domain,
							    1);
	    }
	  else
	    {
	      len = att->sma_domain->type->disksize;
	    }

	  or_put_offset_internal (buf, offset, offset_size);
	  offset += len;
	}

      or_put_offset_internal (buf, offset, offset_size);
      buf->ptr = PTR_ALIGN (buf->ptr, INT_ALIGNMENT);
    }
  return rc;
}


/*
 * object_size - Calculates the amount of disk storage required for an instance.
 *    return: bytes of disk storage required
 *    class(in): class structure
 *    obj(in): instance to examine
 */
static int
object_size (SM_CLASS * class_, MOBJ obj, int *offset_size_ptr)
{
  SM_ATTRIBUTE *att;
  char *mem;
  int a, size;

  *offset_size_ptr = OR_BYTE_SIZE;

re_check:
  size = OR_HEADER_SIZE +
    class_->fixed_size + OR_BOUND_BIT_BYTES (class_->fixed_count);

  if (class_->variable_count)
    {
      size +=
	OR_VAR_TABLE_SIZE_INTERNAL (class_->variable_count, *offset_size_ptr);
      for (a = class_->fixed_count; a < class_->att_count; a++)
	{
	  att = &class_->attributes[a];
	  mem = obj + att->offset;

	  assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

	  if (att->sma_domain->type->data_lengthmem != NULL)
	    {
	      size +=
		(*(att->sma_domain->type->data_lengthmem)) (mem,
							    att->sma_domain,
							    1);
	    }
	  else
	    {
	      size += att->sma_domain->type->disksize;
	    }
	}
    }

  if (*offset_size_ptr == OR_BYTE_SIZE && size > OR_MAX_BYTE)
    {
      *offset_size_ptr = OR_SHORT_SIZE;	/* 2byte */
      goto re_check;
    }
  if (*offset_size_ptr == OR_SHORT_SIZE && size > OR_MAX_SHORT)
    {
      *offset_size_ptr = BIG_VAR_OFFSET_SIZE;	/* 4byte */
      goto re_check;
    }
  return (size);
}


/*
 * put_attributes - Write the fixed and variable attribute values.
 *    return: on overflow, or_overflow will call longjmp and jump to the
 *    outer caller
 *    buf(in/out): translation buffer
 *    obj(in): instance memory
 *    class(in): class structure
 * Note:
 *    The object  header and offset table will already have been written.
 *    Assumes that the schema manager is smart enough to keep the
 *    attributes in their appropriate disk ordering.
 */
static int
put_attributes (OR_BUF * buf, char *obj, SM_CLASS * class_)
{
  SM_ATTRIBUTE *att;
  char *start;
  int pad;

  /*
   * write fixed attribute values, if unbound, leave zero or garbage
   * it doesn't really matter.
   */
  start = buf->ptr;
  for (att = class_->attributes; att != NULL && !att->type->variable_p;
       att = att->next)
    {
      assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

      PRIM_WRITE (att->type, att->sma_domain, buf, obj + att->offset);
    }

  /* bring the end of the fixed width block up to proper alignment */
  pad = (int) (buf->ptr - start);
  if (pad < class_->fixed_size)
    {
      or_pad (buf, class_->fixed_size - pad);
    }
  else if (pad > class_->fixed_size)
    {				/* mismatched fixed block calculations */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_SM_CORRUPTED, 0);
      or_abort (buf);
    }

  /* write the bound bits */
  if (class_->fixed_count)
    {
      or_put_data (buf, obj + OBJ_HEADER_BOUND_BITS_OFFSET,
		   OR_BOUND_BIT_BYTES (class_->fixed_count));
    }
  /* write the variable attributes */

  for (; att != NULL; att = att->next)
    {
      assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

      PRIM_WRITE (att->type, att->sma_domain, buf, obj + att->offset);
    }
  return NO_ERROR;
}


/*
 * tf_mem_to_disk - Translate an instance from its memory format into the
 *                  disk format.
 *    return: zero on success, non-zero on error
 *    classmop(in): class of this instance
 *    classobj(in): class structure
 *    obj(in/out): instance memory to translate
 *    record(out): destination disk record
 * Note:
 *    If there was an overflow error on the given record, we determine
 *    the required size and return this size as a negative number, otherwise
 *    a zero is returned to indicate successful translation.
 *    Note that volatile is used here to prevent the compiler from
 *    optimizing variables into registers that aren't preserved by
 *    setjmp/longjmp.
 */
TF_STATUS
tf_mem_to_disk (MOP classmop, MOBJ classobj,
		MOBJ volatile obj, RECDES * record)
{
  OR_BUF orep, *buf;
  SM_CLASS *volatile class_;	/* prevent register optimization */
  unsigned int repid_bits;
  TF_STATUS status;
  volatile int expected_size;
  volatile int offset_size;
  int tmp_offset_size;
  OID *oidp;

  buf = &orep;
  or_init (buf, record->data, record->area_size);
  buf->error_abort = 1;

  class_ = (SM_CLASS *) classobj;

  expected_size = object_size (class_, obj, &tmp_offset_size);
  offset_size = tmp_offset_size;

  oidp = WS_OID (classmop);

  switch (_setjmp (buf->env))
    {
    case 0:
      status = TF_SUCCESS;

      if (OID_ISTEMP (oidp))
	{
	  /*
	   * since this isn't a mem_oid, can't rely on write_oid to do this,
	   * don't bother making this part of the deferred fixup stuff yet.
	   */
	  if (locator_assign_permanent_oid (classmop) == NULL)
	    {
	      or_abort (buf);
	    }

	  assert (oidp->groupid == NULL_GROUPID);
	}

      /* header */

      repid_bits = class_->repid;
      if (class_->fixed_count)
	{
	  repid_bits |= OR_BOUND_BIT_FLAG;
	}
      /* offset size */
      OR_SET_VAR_OFFSET_SIZE (repid_bits, offset_size);

      or_put_int (buf, repid_bits);

      /* keep out shard table to enter
       */
      if (classobj_find_shard_key_column (class_) == NULL)
	{
	  or_put_int (buf, GLOBAL_GROUPID);
	}
      else
	{
	  assert (false);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_TF_INVALID_METACLASS, 0);
	  or_abort (buf);
	}

      /* variable info block */
      put_varinfo (buf, obj, class_, offset_size);

      /* attributes, fixed followed by variable */
      put_attributes (buf, obj, class_);

      record->length = CAST_BUFLEN (buf->ptr - buf->buffer);

      break;

      /* if the longjmp status was anything other than ER_TF_BUFFER_OVERFLOW,
         it represents an error condition and er_set will have been called */
    case ER_TF_BUFFER_OVERFLOW:
      status = TF_OUT_OF_SPACE;
      record->length = -expected_size;
      break;

    default:
      status = TF_ERROR;
      break;
    }

  buf->error_abort = 0;
  return (status);
}


/*
 * get_current - Loads an instance from disk using the latest class
 * representation.
 *    return: new instance memory
 *    buf(in/out): translation buffer
 *    class(): class of this instance
 *    obj_ptr(out): loaded object (same as return value)
 *    bound_bit_flag(in): initial status of bound bits
 */
static char *
get_current (OR_BUF * buf, SM_CLASS * class_, MOBJ * obj_ptr,
	     int bound_bit_flag, int offset_size)
{
  SM_ATTRIBUTE *att;
  char *obj, *mem, *start;
  int *vars, rc = NO_ERROR;
  int i, j, offset, offset2, pad;

  obj = NULL;
  /* need nicer way to store these */
  vars = NULL;
  if (class_->variable_count)
    {
      vars = (int *) db_ws_alloc (sizeof (int) * class_->variable_count);
      if (vars == NULL)
	{
	  or_abort (buf);
	}
      else
	{
	  offset = or_get_offset_internal (buf, &rc, offset_size);
	  for (i = 0; i < class_->variable_count; i++)
	    {
	      offset2 = or_get_offset_internal (buf, &rc, offset_size);
	      vars[i] = offset2 - offset;
	      offset = offset2;
	    }
	  buf->ptr = PTR_ALIGN (buf->ptr, INT_ALIGNMENT);
	}
    }

  /*
   * if there are no bound bits on disk, allocate the instance block
   * with all the bits turned on, we have to assume that the values
   * are non-null
   */
  obj = *obj_ptr = obj_alloc (class_, (bound_bit_flag) ? 0 : 1);

  if (obj == NULL)
    {
      if (vars != NULL)
	{
	  db_ws_free (vars);
	  vars = NULL;
	}
      or_abort (buf);
    }
  else
    {
      /* fixed */
      start = buf->ptr;
      for (i = 0; i < class_->fixed_count; i++)
	{
	  att = &(class_->attributes[i]);
	  assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

	  mem = obj + att->offset;
	  PRIM_READ (att->type, att->sma_domain, buf, mem, -1);
	}

      /* round up to a to the end of the fixed block */
      pad = (int) (buf->ptr - start);
      if (pad < class_->fixed_size)
	{
	  or_advance (buf, class_->fixed_size - pad);
	}
      /* bound bits, if not present, we have to assume everything is bound */
      if (bound_bit_flag && class_->fixed_count)
	{
	  or_get_data (buf, obj + OBJ_HEADER_BOUND_BITS_OFFSET,
		       OR_BOUND_BIT_BYTES (class_->fixed_count));
	}

      /* variable */
      if (vars != NULL)
	{
	  for (i = class_->fixed_count, j = 0;
	       i < class_->att_count && j < class_->variable_count; i++, j++)
	    {
	      att = &(class_->attributes[i]);
	      assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

	      mem = obj + att->offset;
	      PRIM_READ (att->type, att->sma_domain, buf, mem, vars[j]);
	    }
	}
    }

  if (vars != NULL)
    {
      db_ws_free (vars);
    }

  return (obj);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * find_current_attribute - Given an instance attribute id, find the
 * corresponding attribute in the latest representation.
 *    return: pointer to current attribute. NULL if not found
 *    class(in): class structure
 *    id(in): attribute id
 */
static SM_ATTRIBUTE *
find_current_attribute (SM_CLASS * class_, int id)
{
  SM_ATTRIBUTE *found, *att;

  found = NULL;
  for (att = class_->attributes; att != NULL && found == NULL;
       att = att->next)
    {
      if (att->id == id)
	{
	  found = att;
	}
    }
  return (found);
}

/*
 * clear_new_unbound - This initializes the space created for instance
 * attributes that had no saved values on disk.
 *    return: void
 *    obj(in/out): instance memory
 *    class(in): class of this instance
 *    oldrep(in): old representation (of instance on disk)
 * Note:
 *    This happens when an object is converted to a new representation
 *    that had one or more attributes added.
 *    It might be more efficient to do this for all fields when the instance
 *    memory is first allocated ?
 *    The original_value is used if it has a value.
 */
static void
clear_new_unbound (char *obj, SM_CLASS * class_, SM_REPRESENTATION * oldrep)
{
  SM_ATTRIBUTE *att;
  SM_REPR_ATTRIBUTE *rat, *found;
  char *mem;

  for (att = class_->attributes; att != NULL; att = att->next)
    {
      assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

      found = NULL;
      for (rat = oldrep->attributes; rat != NULL && found == NULL;
	   rat = rat->next)
	{
	  if (rat->attid == att->id)
	    {
	      found = rat;
	    }
	}
      if (found == NULL)
	{
	  mem = obj + att->offset;
	  /* initialize in case there isn't an initial value */
	  PRIM_INITMEM (att->type, mem, att->sma_domain);
	  if (!DB_IS_NULL (&att->default_value.original_value))
	    {
	      /* assign the initial value, should check for non-existance ? */
	      PRIM_SETMEM (att->type, att->sma_domain, mem,
			   &att->default_value.original_value);
	      if (!att->type->variable_p)
		{
		  OBJ_SET_BOUND_BIT (obj, att->storage_order);
		}
	    }
	}
    }
}

/*
 * get_old - This creates an instance from an obsolete disk representation.
 *    return: new instance memory
 *    buf(in/out): translation buffer
 *    class(in): class of this instance
 *    obj_ptr(out): return value
 *    repid(in): old representation id
 *    bound_bit_flag(in): initial status of bound bits
 * Note:
 *    Any new attributes that had no values on disk are given a starting
 *    value from the "original_value" field in the class.
 *    Values for deleted attributes are discarded.
 */
static char *
get_old (OR_BUF * buf, SM_CLASS * class_, MOBJ * obj_ptr,
	 int repid, int bound_bit_flag, int offset_size)
{
  SM_REPRESENTATION *oldrep;
  SM_REPR_ATTRIBUTE *rat;
  SM_ATTRIBUTE **attmap;
  PR_TYPE *type;
  char *obj, *bits, *start;
  int *vars = NULL, rc = NO_ERROR;
  int i, total, offset, offset2, bytes, att_index, padded_size, fixed_size;

  obj = NULL;
  oldrep = classobj_find_representation (class_, repid);

  if (oldrep == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TF_INVALID_REPRESENTATION,
	      1, class_->header.name);
    }
  else
    {
      /*
       * if there are no bound bits on disk, allocate the instance block
       * with all the bits turned on, we have to assume that the values
       * are non-null
       */
      obj = *obj_ptr = obj_alloc (class_, (bound_bit_flag) ? 0 : 1);

      if (obj == NULL)
	{
	  or_abort (buf);
	}
      else
	{
	  /*
	   * read the variable offset table, can't we just leave this on
	   * disk ?
	   */
	  vars = NULL;
	  if (oldrep->variable_count)
	    {
	      vars =
		(int *) db_ws_alloc (sizeof (int) * oldrep->variable_count);
	      if (vars == NULL)
		{
		  or_abort (buf);
		  return NULL;
		}
	      else
		{
		  offset = or_get_offset_internal (buf, &rc, offset_size);
		  for (i = 0; i < oldrep->variable_count; i++)
		    {
		      offset2 =
			or_get_offset_internal (buf, &rc, offset_size);
		      vars[i] = offset2 - offset;
		      offset = offset2;
		    }
		  buf->ptr = PTR_ALIGN (buf->ptr, INT_ALIGNMENT);
		}
	    }

	  /* calculate an attribute map */
	  total = oldrep->fixed_count + oldrep->variable_count;
	  attmap = NULL;
	  if (total > 0)
	    {
	      attmap =
		(SM_ATTRIBUTE **) db_ws_alloc (sizeof (SM_ATTRIBUTE *) *
					       total);
	      if (attmap == NULL)
		{
		  if (vars != NULL)
		    {
		      db_ws_free (vars);
		    }
		  or_abort (buf);
		  return NULL;
		}
	      else
		{
		  for (rat = oldrep->attributes, i = 0; rat != NULL;
		       rat = rat->next, i++)
		    {
		      attmap[i] = find_current_attribute (class_, rat->attid);
		    }
		}
	    }

	  /* read the fixed attributes */
	  rat = oldrep->attributes;
	  start = buf->ptr;
	  for (i = 0;
	       i < oldrep->fixed_count && rat != NULL && attmap != NULL;
	       i++, rat = rat->next)
	    {
	      type = PR_TYPE_FROM_ID (rat->typeid_);
	      if (type == NULL)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_TF_INVALID_REPRESENTATION, 1,
			  class_->header.name);

		  db_ws_free (attmap);
		  if (vars != NULL)
		    {
		      db_ws_free (vars);
		    }

		  or_abort (buf);
		  return NULL;
		}

	      if (attmap[i] == NULL)
		{
		  PRIM_READ (type, rat->domain, buf, NULL, -1);
		}
	      else
		{
		  PRIM_READ (type, rat->domain, buf,
			     obj + attmap[i]->offset, -1);
		}
	    }

	  fixed_size = (int) (buf->ptr - start);
	  padded_size = DB_ATT_ALIGN (fixed_size);
	  or_advance (buf, (padded_size - fixed_size));


	  /*
	   * sigh, we now have to process the bound bits in much the same way
	   * as the attributes above, it would be nice if these could be done
	   * in parallel but we don't have the fixed size of the old
	   * representation so we can't easily sneak the buffer pointer
	   * forward, work on this someday
	   */

	  if (bound_bit_flag && oldrep->fixed_count)
	    {
	      bits = buf->ptr;
	      bytes = OR_BOUND_BIT_BYTES (oldrep->fixed_count);
	      if ((buf->ptr + bytes) > buf->endptr)
		{
		  or_overflow (buf);
		}

	      rat = oldrep->attributes;
	      for (i = 0; i < oldrep->fixed_count && rat != NULL
		   && attmap != NULL; i++, rat = rat->next)
		{
		  if (attmap[i] != NULL)
		    {
		      if (OR_GET_BOUND_BIT (bits, i))
			{
			  OBJ_SET_BOUND_BIT (obj, attmap[i]->storage_order);
			}
		    }
		}
	      or_advance (buf, bytes);
	    }

	  /* variable */
	  if (vars != NULL)
	    {
	      for (i = 0; i < oldrep->variable_count && rat != NULL
		   && attmap != NULL; i++, rat = rat->next)
		{
		  type = PR_TYPE_FROM_ID (rat->typeid_);
		  if (type == NULL)
		    {
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_TF_INVALID_REPRESENTATION, 1,
			      class_->header.name);

		      db_ws_free (attmap);
		      db_ws_free (vars);

		      or_abort (buf);
		      return NULL;
		    }

		  att_index = oldrep->fixed_count + i;
		  if (attmap[att_index] == NULL)
		    {
		      PRIM_READ (type, rat->domain, buf, NULL, vars[i]);
		    }
		  else
		    {
		      PRIM_READ (type, rat->domain, buf,
				 obj + attmap[att_index]->offset, vars[i]);
		    }
		}
	    }

	  /* should be optimizing this operation ! */
	  clear_new_unbound (obj, class_, oldrep);

	  if (attmap != NULL)
	    {
	      db_ws_free (attmap);
	    }
	  if (vars != NULL)
	    {
	      db_ws_free (vars);
	    }
	}
    }

  return (obj);
}
#endif

/*
 * tf_disk_to_mem - Interface function for transforming the disk
 * representation of an instance into the memory representation.
 *    return: MOBJ
 *    classobj(in): class structure
 *    record(in): disk record
 *    convertp(in): set to non-zero if the object had to be converted from
 *                  an obsolete disk representation.
 * Note:
 *    It is imperitive that garbage collection be disabled during this
 *    operation.
 *    This is because the structure being built may contain pointers
 *    to MOPs but it has not itself been attached to its own MOP yet so it
 *    doesn't serve as a gc root.   Make sure the caller
 *    calls ws_cache() immediately after we return so the new object
 *    gets attached as soon as possible.  We must also have already
 *    allocated the MOP in the caller so that we don't get one of the
 *    periodic gc's when we attempt to allocate a new MOP.
 *    This dependency should be isolated in a ws_ function called by
 *    the locator.
 */
MOBJ
tf_disk_to_mem (MOBJ classobj, RECDES * record, int *convertp)
{
  OR_BUF orep, *buf;
  SM_CLASS *class_ = NULL;
  char *obj = NULL;
  unsigned int repid_bits;
  int repid, convert, bound_bit_flag;
  int grpid;
  int rc = NO_ERROR;
  int offset_size;

  buf = &orep;
  or_init (buf, record->data, record->length);
  buf->error_abort = 1;

  switch (_setjmp (buf->env))
    {
    case 0:
      class_ = (SM_CLASS *) classobj;
      obj = NULL;
      convert = 0;
      /* offset size */
      offset_size = OR_GET_OFFSET_SIZE (buf->ptr);

      repid_bits = or_get_int (buf, &rc);

      grpid = or_get_int (buf, &rc);

      /* mask out the repid & bound bit flag  & offset size flag */
      repid = repid_bits & ~OR_BOUND_BIT_FLAG & ~OR_OFFSET_SIZE_FLAG;
      bound_bit_flag = repid_bits & OR_BOUND_BIT_FLAG;

#if 1				/* TODO - delete me someday */
      assert (repid == class_->repid);
      if (repid != class_->repid)
	{
	  obj = NULL;		/* something wrong */
	  break;
	}

      obj = get_current (buf, class_, &obj, bound_bit_flag, offset_size);
#else
      if (repid == class_->repid)
	{
	  obj = get_current (buf, class_, &obj, bound_bit_flag, offset_size);
	}
      else
	{
	  obj =
	    get_old (buf, class_, &obj, repid, bound_bit_flag, offset_size);
	  convert = 1;
	}
#endif

      *convertp = convert;
      break;

    default:
      /* make sure to clear the object that was being created,
         an appropriate error will have been set */
      if (obj != NULL)
	{
	  obj_free_memory (class_, obj);
	  obj = NULL;
	}
      break;
    }

  buf->error_abort = 0;
  return (obj);
}

/*
 * unpack_allocator - memory allocator
 *    return: memory allocated
 *    size(in): size to allocate
 * Note:
 *    Used in cases where an allocatino function needs to be passed
 *    to one of the transformation routines.
 */
static char *
unpack_allocator (int size)
{
  return ((char *) malloc (size));
}


/*
 * read_var_table -  extracts the variable offset table from the disk
 * representation and build a more convenient memory representation.
 *    return: array of OR_VARINFO structures
 *    buf(in/out): translation buffr
 *    nvars(in): expected number of variables
 */
static OR_VARINFO *
read_var_table (OR_BUF * buf, int nvars)
{
  return (read_var_table_internal (buf, nvars, BIG_VAR_OFFSET_SIZE));
}

/*
 * read_var_table_internal -  extracts the variable offset table from the disk
 * representation and build a more convenient memory representation.
 *    return: array of OR_VARINFO structures
 *    buf(in/out): translation buffr
 *    nvars(in): expected number of variables
 */
static OR_VARINFO *
read_var_table_internal (OR_BUF * buf, int nvars, int offset_size)
{
  return (or_get_var_table_internal
	  (buf, nvars, unpack_allocator, offset_size));
}


/*
 * free_var_table - frees a table allocated by read_var_table
 *    return: void
 *    vars(out): variable table
 */
static void
free_var_table (OR_VARINFO * vars)
{
  if (vars != NULL)
    {
      free_and_init (vars);
    }
}


/*
 * string_disk_size - calculate the disk size of a NULL terminated ASCII
 * string that is supposed to be stored as a VARNCHAR attribute in one of
 * the various class objects.
 *    return: byte size for packed "varchar" string
 *    string(in):
 */
static int
string_disk_size (const char *string)
{
  DB_VALUE value;
  int str_length;

  str_length = 0;
  if (string != NULL)
    {
      str_length = strlen (string);
    }

  db_make_varchar (&value, DB_MAX_VARCHAR_PRECISION, (char *) string,
		   str_length, LANG_SYS_COLLATION);
  return (*(tp_String.data_lengthval)) (&value, 1);
}


/*
 * get_string - read a string out of the OR_BUF and return it as a NULL
 * terminated ASCII string
 *    return: string from value or NULL
 *    buf(in/out): translation buffer
 *    length(in): length of string or -1 if read variable length
 * Note:
 *    Shorthand macro to read a string out of the OR_BUF and return it
 *    as a NULL terminated ASCII string which everything stored as part
 *    of the class definition is.
 *
 *    A jmp_buf has previously been established.
 */
static char *
get_string (OR_BUF * buf, int length)
{
  DB_VALUE value;
  char *string = NULL;
  DB_DOMAIN my_domain;

  /*
   * Make sure this starts off initialized so "readval" won't try to free
   * any existing contents.
   */
  db_make_null (&value);

  /*
   * The domain here is always a server side VARCHAR.  Set a temporary
   * domain to reflect this.
   */
  my_domain.precision = DB_MAX_VARCHAR_PRECISION;
  my_domain.collation_id = LANG_SYS_COLLATION;

  (*(tp_String.data_readval)) (buf, &value, &my_domain, length, true);

  string = db_get_string (&value);

  return string;
}


/*
 * put_string - Put a NULL terminated ASCII string into the disk buffer in the
 * usual way.
 *    return: void
 *    buf(in/out): translation buffer
 *    string(in): string to store
 * Note:
 *    See get_string & string_disk_size for the related functions.
 */
static void
put_string (OR_BUF * buf, const char *string)
{
  DB_VALUE value;
  int str_length;

  str_length = 0;
  if (string != NULL)
    {
      str_length = strlen (string);
    }

  db_make_varchar (&value, DB_MAX_VARCHAR_PRECISION, (char *) string,
		   str_length, LANG_SYS_COLLATION);
  (*(tp_String.data_writeval)) (buf, &value);
}

/*
 * SET SUPPORT FUNCTIONS
 *
 * These make it easier to read and write object and substructure sets
 * which are used often in a class's disk representation.
 *
 * SUBSTRUCTURE SET RULES
 *
 * The set header containing type and count is always present.
 * The variable offset table is present only if there are elements.
 * The substructure tag is present only if there are elements.
 *
 */

/*
 * substructure_set_size - Calculates the disk storage for a set created from
 * a list of memory elements.
 *    return: byte of disk storage required
 *    list(in): list of elements
 *    function(in): function to calculate element sizes
 * Note:
 *    The supplied function is used to find the size of the list elements.
 *    Even if the list is empty, there will always be at least a set header
 *    written to disk.
 */
static int
substructure_set_size (DB_LIST * list, LSIZER function)
{
  DB_LIST *l;
  int size, count;

  /* store NULL for empty list */
  if (list == NULL)
    {
      return 0;
    }

  /* header + domain length word + domain */
  size = OR_SET_HEADER_SIZE + OR_INT_SIZE + OR_SUB_DOMAIN_SIZE;

  count = 0;
  for (l = list; l != NULL; l = l->next, count++)
    {
      size += (*function) (l);
    }

  if (count)
    {
      /*
       * we have elements to store, in that case we need to add the
       * common substructure header at the front and an offset table
       */
      size += OR_VAR_TABLE_SIZE (count);
      size += OR_SUB_HEADER_SIZE;
    }

  return (size);
}


/*
 * put_substructure_set - Write the disk representation of a substructure
 * set from a linked list of structures.
 *    return:  void
 *    buf(in/out): translation buffer
 *    list(in): substructure list
 *    writer(in): translation function
 *    class(in): OID of meta class for this substructure
 *    repid(in): repid for the meta class
 * Note:
 *    The supplied functions calculate the size and write the elements.
 *    The OID/repid for the metaclass will be one of the meta classes
 *    defined in the catalog.
 */
static void
put_substructure_set (OR_BUF * buf, DB_LIST * list,
		      LWRITER writer, OID * class_, int repid)
{
  DB_LIST *l;
  char *start;
  int count;
  char *offset_ptr;

  /* store NULL for empty list */
  if (list == NULL)
    {
      return;
    }

  count = 0;
  for (l = list; l != NULL; l = l->next, count++);

  /* with domain, no bound bits, with offsets, no tags, common sub header */
  start = buf->ptr;
  or_put_set_header (buf, DB_TYPE_SEQUENCE, count, 1, 0, 1, 0, 1);

  if (count == 0)
    {
      /* we must at least store the domain even though there will be no elements */
      or_put_int (buf, OR_SUB_DOMAIN_SIZE);
      or_put_sub_domain (buf);
    }
  else
    {
      /* begin an offset table */
      offset_ptr = buf->ptr;
      or_advance (buf, OR_VAR_TABLE_SIZE (count));

      /* write the domain */
      or_put_int (buf, OR_SUB_DOMAIN_SIZE);
      or_put_sub_domain (buf);

      /* write the common substructure header */
      or_put_oid (buf, class_);
      or_put_int (buf, repid);
      or_put_int (buf, 0);	/* flags */

      /* write each substructure */
      for (l = list; l != NULL; l = l->next)
	{
	  /* determine the offset to the this element and put it in the table */
	  OR_PUT_OFFSET (offset_ptr, (buf->ptr - start));
	  offset_ptr += BIG_VAR_OFFSET_SIZE;
	  /* write the substructure */
	  (*writer) (buf, l);
	}
      /* write the final offset */
      OR_PUT_OFFSET (offset_ptr, (buf->ptr - start));
    }
}


/*
 * get_substructure_set - extracts a substructure set on disk and create a
 * list of memory structures.
 *    return: list of structures
 *    buf(in/out): translation buffer
 *    reader(in): function to read the elements
 *    expected(in): expected size
 * Note:
 *    It is important that the elements be APPENDED here.
 */
static DB_LIST *
get_substructure_set (OR_BUF * buf, int *num_list, LREADER reader,
		      int expected)
{
  DB_LIST *list, *obj;
  int count, i;

  if (num_list != NULL)
    {
      *num_list = 0;
    }
  /* handle NULL case, variable width will be zero */
  if (expected == 0)
    {
      return NULL;
    }

  list = NULL;
  count = or_skip_set_header (buf);
  for (i = 0; i < count; i++)
    {
      obj = (*reader) (buf);
      if (obj != NULL)
	{
	  WS_LIST_APPEND (&list, obj);
	}
      else
	{
	  or_abort (buf);
	}
    }

  if (num_list != NULL)
    {
      *num_list = count;
    }

  return (list);
}


/*
 * install_substructure_set - loads a substructure set from disk into a list
 * of memory structures.
 *    return: void
 *    buf(in/out): translation buffer
 *    list(in): threaded array of structures
 *    reader(in): function to read elements
 *    expected(in): expected size;
 * Note:
 *    The difference is this function does not allocate storage for the
 *    elements, these have been previously allocated and are simply filled in
 *    by the reader.
 */
static void
install_substructure_set (OR_BUF * buf, DB_LIST * list, VREADER reader,
			  int expected)
{
  DB_LIST *p;
  int count, i;

  if (expected)
    {
      count = or_skip_set_header (buf);
      for (i = 0, p = list; i < count && p != NULL; i++, p = p->next)
	{
	  (*reader) (buf, p);
	}
    }
}


/*
 * property_list_size - Calculate the disk storage requirements for a
 * property list.
 *    return: byte size of property list
 *    properties(in): property list
 */
static int
property_list_size (DB_SEQ * properties)
{
  DB_VALUE value;
  int size, max;

  size = 0;
  if (properties != NULL)
    {
      /* collapse empty property sequences to NULL values on disk */
      max = set_size (properties);
      if (max)
	{
	  db_make_sequence (&value, properties);
	  size = (*(tp_Sequence.data_lengthval)) (&value, 1);
	}
    }
  return size;
}

/*
 * put_property_list - Write the disk representation of a property list.
 *    return: void
 *    buf(in/out): translation buffer
 *    properties(in): property list
 */
static void
put_property_list (OR_BUF * buf, DB_SEQ * properties)
{
  DB_VALUE value;
  int max;

  if (properties == NULL)
    {
      return;
    }
  /* collapse empty property sequences to NULL values on disk */
  max = set_size (properties);
  if (max > 0)
    {
      db_make_sequence (&value, properties);
      (*(tp_Sequence.data_writeval)) (buf, &value);
    }
}


/*
 * get_property_list - This reads a property list from disk.
 *    return: a new property list
 *    buf(in/out): translation buffer
 *    expected_size(in): number of bytes on disk
 * Note:
 *    This reads a property list from disk.
 *    If either the expected size is zero, NULL is returned.
 *    If a sequence was stored on disk but it had no elements, NULL is
 *    returned and the empty sequence is freed.  This is to handle the
 *    case where old style class objects had an empty substructure set
 *    stored at this position.  Since these will be converted to property
 *    lists, we can just ignore them.
 *
 *    A jmp_buf has previously been established.
 */
static DB_SEQ *
get_property_list (OR_BUF * buf, int expected_size)
{
  DB_VALUE value;
  DB_SEQ *properties;
  int max;

  properties = NULL;
  if (expected_size)
    {
      (*(tp_Sequence.data_readval)) (buf, &value, NULL, expected_size, true);
      properties = db_get_set (&value);
      if (properties == NULL)
	or_abort (buf);		/* trouble allocating a handle */
      else
	{
	  max = set_size (properties);
	  if (max <= 0)
	    {
	      /*
	       * there is an empty sequence here, get rid of it so we don't
	       * have to carry it around
	       */
	      set_free (properties);
	      properties = NULL;
	    }
	}
    }
  return (properties);
}


/*
 * domain_size - Calculates the number of bytes required to store a domain
 * list on disk.
 *    return: disk size of domain
 *    domain(in): domain list
 */
static int
domain_size (TP_DOMAIN * domain)
{
  int size;

  size =
    tf_Metaclass_domain.fixed_size +
    OR_VAR_TABLE_SIZE (tf_Metaclass_domain.n_variable);

  size +=
    substructure_set_size ((DB_LIST *) domain->setdomain,
			   (LSIZER) domain_size);

  return (size);
}


/*
 * domain_to_disk - Translates a domain list to its disk representation.
 *    return: void
 *    buf(in/out): translation buffer
 *    domain(in): domain list
 * Note:
 * Translates a domain list to its disk representation.
 */
static void
domain_to_disk (OR_BUF * buf, TP_DOMAIN * domain)
{
  char *start;
  int offset;

  /* VARIABLE OFFSET TABLE */
  start = buf->ptr;
  offset =
    tf_Metaclass_domain.fixed_size +
    OR_VAR_TABLE_SIZE (tf_Metaclass_domain.n_variable);
  or_put_offset (buf, offset);
  offset +=
    substructure_set_size ((DB_LIST *) domain->setdomain,
			   (LSIZER) domain_size);
  or_put_offset (buf, offset);

  buf->ptr = PTR_ALIGN (buf->ptr, INT_ALIGNMENT);

  /* ATTRIBUTES */
  or_put_int (buf, (int) TP_DOMAIN_TYPE (domain));
  or_put_int (buf, domain->precision);
  or_put_int (buf, domain->scale);
  or_put_int (buf, INTL_CODESET_UTF8);
  or_put_int (buf, domain->collation_id);
  pr_write_mop (buf, domain->class_mop);

  put_substructure_set (buf, (DB_LIST *) domain->setdomain,
			(LWRITER) domain_to_disk,
			&tf_Metaclass_domain.classoid,
			tf_Metaclass_domain.repid);

  if (start + offset != buf->ptr)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TF_OUT_OF_SYNC, 0);
    }
}


/*
 * disk_to_domain2 - Create the memory representation for a domain list from
 * the disk representation.
 *    return: domain structure
 *    buf(in/out): translation buffer
 * Note:
 *    This builds a transient domain, it is called by disk_to_domain which
 *    just turns around and caches it.
 *
 *    A jmp_buf has previously been established.
 */
static TP_DOMAIN *
disk_to_domain2 (OR_BUF * buf)
{
  OR_VARINFO *vars;
  TP_DOMAIN *domain;
  DB_TYPE typeid_;
  OID oid;
  int rc = NO_ERROR;

  vars = read_var_table (buf, tf_Metaclass_domain.n_variable);
  if (vars == NULL)
    {
      or_abort (buf);
      return NULL;
    }

  typeid_ = (DB_TYPE) or_get_int (buf, &rc);

  domain = tp_domain_new (typeid_);
  if (domain == NULL)
    {
      free_var_table (vars);
      or_abort (buf);
      return NULL;
    }

  domain->precision = or_get_int (buf, &rc);
  domain->scale = or_get_int (buf, &rc);
  (void) or_get_int (buf, &rc);	/* charset */
  domain->collation_id = or_get_int (buf, &rc);

  /*
   * Read the domain class OID without promoting it to a MOP.
   * Could use readval, and extract the OID out of the already swizzled
   * MOP too.
   */
  (*(tp_Oid.data_readmem)) (buf, &oid, NULL, -1);
  domain->class_oid = oid;

  /* swizzle the pointer, we know we're on the client here */
  if (!OID_ISNULL (&domain->class_oid))
    {
      domain->class_mop = ws_mop (&domain->class_oid, sm_Root_class_mop);
      if (domain->class_mop == NULL)
	{
	  or_abort (buf);
	}
    }

  domain->setdomain =
    (TP_DOMAIN *) get_substructure_set (buf, NULL,
					(LREADER) disk_to_domain2,
					vars[ORC_DOMAIN_SETDOMAIN_INDEX].
					length);

  assert (domain->next_list == NULL);
  assert (domain->is_cached == 0);

#if !defined(NDEBUG)
  {
    TP_DOMAIN *d;

    for (d = domain->setdomain; d != NULL; d = d->next)
      {
	assert (d->next_list == NULL);
	assert (d->is_cached == 0);
      }
  }
#endif

  free_var_table (vars);

  return (domain);
}


/*
 * disk_to_domain - Create the memory representation for a domain list from
 * the disk representation.
 *    return: domain structure
 *    buf(in/out): translation buffer
 * Note:
 *    Calls disk_to_domain2 which builds the transient domain which we
 *    then cache when we're done.
 *    We need to separate the two operations because disk_to_domain2 is
 *    called recursively for nested domains and we don't want to
 *    cache each of those.
 */
static TP_DOMAIN *
disk_to_domain (OR_BUF * buf)
{
  TP_DOMAIN *domain;

  domain = disk_to_domain2 (buf);
  if (domain != NULL)
    {
      assert (TP_DOMAIN_TYPE (domain) != DB_TYPE_VARIABLE);

      domain = tp_domain_cache (domain);
    }

  return domain;
}

/*
 * query_spec_to_disk - Write the disk representation of a virtual class
 * query_spec statement.
 *    return: NO_ERROR or error code
 *    buf(in/out): translation buffer
 *    statement(in): query_spec statement
 */
static int
query_spec_to_disk (OR_BUF * buf, SM_QUERY_SPEC * statement)
{
  char *start;
  int offset;

  start = buf->ptr;
  /* VARIABLE OFFSET TABLE */
  offset = tf_Metaclass_query_spec.fixed_size +
    OR_VAR_TABLE_SIZE (tf_Metaclass_query_spec.n_variable);

  or_put_offset (buf, offset);
  offset += string_disk_size (statement->specification);

  or_put_offset (buf, offset);
  buf->ptr = PTR_ALIGN (buf->ptr, INT_ALIGNMENT);

  /* ATTRIBUTES */
  put_string (buf, statement->specification);

  if (start + offset != buf->ptr)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TF_OUT_OF_SYNC, 0);
    }

  return NO_ERROR;
}


/*
 * query_spec_size - Calculates the disk size of a query_spec statement.
 *    return: disk size of query_spec statement
 *    statement(in): query_spec statement
 */
static int
query_spec_size (SM_QUERY_SPEC * statement)
{
  int size;

  size = tf_Metaclass_query_spec.fixed_size +
    OR_VAR_TABLE_SIZE (tf_Metaclass_query_spec.n_variable);
  size += string_disk_size (statement->specification);

  return (size);
}


/*
 * disk_to_query_spec - Reads the disk representation of a query_spec
 * statement and creates the memory representation.
 *    return: new query_spec structure
 *    buf(in/out): translation buffer
 */
static SM_QUERY_SPEC *
disk_to_query_spec (OR_BUF * buf)
{
  SM_QUERY_SPEC *statement;
  OR_VARINFO *vars;

  statement = NULL;
  vars = read_var_table (buf, tf_Metaclass_query_spec.n_variable);
  if (vars == NULL)
    {
      or_abort (buf);
    }
  else
    {
      statement = classobj_make_query_spec (NULL);
      if (statement == NULL)
	{
	  or_abort (buf);
	}
      else
	{
	  statement->specification =
	    get_string (buf, vars[ORC_QUERY_SPEC_SPEC_INDEX].length);
	}

      free_var_table (vars);
    }
  return (statement);
}

/*
 * disk_constraint_attribute_size - Calculate the disk size for a disk constraint
 *   attribute.
 *    return: byte size of the disk constraint attribute
 *    disk_cons_att(in): disk constraint attribute
 */
static int
disk_constraint_attribute_size (UNUSED_ARG SM_DISK_CONSTRAINT_ATTRIBUTE *
				disk_cons_att)
{
  int size;

  size = tf_Metaclass_constraint_attribute.fixed_size +
    OR_VAR_TABLE_SIZE (tf_Metaclass_constraint_attribute.n_variable);

  /* reserved size */
  size += string_disk_size (NULL);

  return (size);
}

/*
 * constraint_size - Calculate the disk size for a disk constraint.
 *    return: byte size of the constraint
 *    constraint(in): constraint
 */
static int
constraint_size (SM_DISK_CONSTRAINT * disk_cons)
{
  char buf[128];
  int size;

  assert (disk_cons->disk_info_of_atts != NULL);

  size = tf_Metaclass_constraint.fixed_size +
    OR_VAR_TABLE_SIZE (tf_Metaclass_constraint.n_variable);

  size += string_disk_size (disk_cons->name);

  btid_to_string (buf, sizeof (buf), &disk_cons->index_btid);
  size += string_disk_size (buf);

  size += substructure_set_size ((DB_LIST *) disk_cons->disk_info_of_atts,
				 (LSIZER) disk_constraint_attribute_size);

  return (size);
}

/*
 * constraint_attribute_to_disk - Write the disk constraint attriburte of an
 * SM_CLASS_CONSTRAINT_ATTRIBUTE.
 *    return: NO_ERROR
 *    buf(in/out): translation buffer
 *    constraint(in): constraint
 * Note:
 *    On overflow, or_overflow will call longjmp and jump to the outer caller
 */
static int
constraint_attribute_to_disk (OR_BUF * buf,
			      SM_DISK_CONSTRAINT_ATTRIBUTE *
			      constraint_attribute)
{
  char *start;
  int offset;

  start = buf->ptr;

  /* reserved */
  offset = tf_Metaclass_constraint_attribute.fixed_size +
    OR_VAR_TABLE_SIZE (tf_Metaclass_constraint_attribute.n_variable);
  or_put_offset (buf, offset);

  /* end of object */
  offset += string_disk_size (NULL);
  or_put_offset (buf, offset);

  or_get_align (buf, INT_ALIGNMENT);

  assert (constraint_attribute->att_id >= 0);
  or_put_int (buf, constraint_attribute->att_id);
  or_put_int (buf, constraint_attribute->asc_desc);

  /* reserved */
  put_string (buf, NULL);

  if (start + offset != buf->ptr)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TF_OUT_OF_SYNC, 0);
    }

  return NO_ERROR;
}

/*
 * disk_constraint_to_disk - Write the disk constraint of an
 * SM_DISK_CONSTRAINT.
 *    return: NO_ERROR
 *    buf(in/out): translation buffer
 *    constraint(in): constraint
 * Note:
 *    On overflow, or_overflow will call longjmp and jump to the outer caller
 */
static int
disk_constraint_to_disk (OR_BUF * buf, SM_DISK_CONSTRAINT * disk_cons)
{
  char buf_btid[128];
  char *start;
  int offset;

  start = buf->ptr;

  assert (disk_cons->disk_info_of_atts != NULL);

  /* name */
  offset = tf_Metaclass_constraint.fixed_size +
    OR_VAR_TABLE_SIZE (tf_Metaclass_constraint.n_variable);
  or_put_offset (buf, offset);

  /* btid */
  offset += string_disk_size (disk_cons->name);
  or_put_offset (buf, offset);

  /* constraint attribute */
  btid_to_string (buf_btid, sizeof (buf_btid), &disk_cons->index_btid);
  offset += string_disk_size (buf_btid);
  or_put_offset (buf, offset);

  /* end of object */
  offset +=
    substructure_set_size ((DB_LIST *) disk_cons->disk_info_of_atts,
			   (LSIZER) disk_constraint_attribute_size);
  or_put_offset (buf, offset);

  or_get_align (buf, INT_ALIGNMENT);

  or_put_int (buf, disk_cons->type);
  or_put_int (buf, disk_cons->index_status);

  /* variable values */

  put_string (buf, disk_cons->name);
  put_string (buf, buf_btid);

  put_substructure_set (buf, (DB_LIST *) disk_cons->disk_info_of_atts,
			(LWRITER) constraint_attribute_to_disk,
			&tf_Metaclass_constraint_attribute.classoid,
			tf_Metaclass_constraint_attribute.repid);

  if (start + offset != buf->ptr)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TF_OUT_OF_SYNC, 0);
    }

  return NO_ERROR;
}

/*
 * disk_to_constraint - Read the disk representation for an
 *              SM_CLASS_CONSTRAINT structure and build the memory structure.
 *    return: new constraint structure
 *    buf(in/out): translation buffer
 */
static SM_DISK_CONSTRAINT *
disk_to_constraint (OR_BUF * buf)
{
  SM_DISK_CONSTRAINT *disk_cons;
  OR_VARINFO *vars;
  char *btid_string;
  int rc;

  disk_cons = NULL;
  vars = read_var_table (buf, tf_Metaclass_constraint.n_variable);
  if (vars == NULL)
    {
      goto exit_on_error;
    }

  disk_cons = classobj_make_disk_constraint ();
  if (disk_cons == NULL)
    {
      goto exit_on_error;
    }

  disk_cons->type = or_get_int (buf, &rc);
  if (rc != NO_ERROR)
    {
      goto exit_on_error;
    }
  disk_cons->index_status = or_get_int (buf, &rc);
  if (rc != NO_ERROR)
    {
      goto exit_on_error;
    }

  /* variable attribute 1 : constraint name */
  disk_cons->name = get_string (buf,
				vars[ORC_CLASS_CONSTRAINTS_NAME_INDEX].
				length);

  /* variable attribute 1 : btid */
  btid_string = get_string (buf,
			    vars[ORC_CLASS_CONSTRAINTS_BTID_INDEX].length);
  classobj_decompose_property_btid (btid_string, &disk_cons->index_btid);
  free_and_init (btid_string);

  /* variable attribute 3 : constranint attribute */
  disk_cons->disk_info_of_atts =
    (SM_DISK_CONSTRAINT_ATTRIBUTE *) get_substructure_set (buf,
							   &disk_cons->
							   num_atts,
							   (LREADER)
							   disk_to_constraint_attribute,
							   vars
							   [ORC_CLASS_CONSTRAINTS_ATTS_INDEX].
							   length);

  free_var_table (vars);

  return disk_cons;

exit_on_error:
  or_abort (buf);
  if (disk_cons != NULL)
    {
      classobj_free_disk_constraint (disk_cons);
    }
  return NULL;
}

/*
 * disk_to_constraint - Read the disk representation for an
 *              SM_CLASS_CONSTRAINT structure and build the memory structure.
 *    return: new constraint structure
 *    buf(in/out): translation buffer
 */
static SM_DISK_CONSTRAINT_ATTRIBUTE *
disk_to_constraint_attribute (OR_BUF * buf)
{
  SM_DISK_CONSTRAINT_ATTRIBUTE *disk_cons_att;
  OR_VARINFO *vars;
  int rc;
  char *tmp;

  disk_cons_att = NULL;		/* init */

  vars = read_var_table (buf, tf_Metaclass_constraint_attribute.n_variable);
  if (vars == NULL)
    {
      goto exit_on_error;
    }

  disk_cons_att = classobj_make_disk_constraint_attribute ();
  if (disk_cons_att == NULL)
    {
      goto exit_on_error;
    }

  disk_cons_att->att_id = or_get_int (buf, &rc);
  if (rc != NO_ERROR)
    {
      goto exit_on_error;
    }
  assert (disk_cons_att->att_id >= 0);

  disk_cons_att->asc_desc = or_get_int (buf, &rc);
  if (rc != NO_ERROR)
    {
      goto exit_on_error;
    }

  tmp =
    get_string (buf, vars[ORC_CLASS_CONSTRAINTS_ATTS_RESERVED_INDEX].length);
  free_and_init (tmp);

  free_var_table (vars);

  return disk_cons_att;

exit_on_error:
  if (disk_cons_att != NULL)
    {
      classobj_free_disk_constraint_attribute (disk_cons_att);
      disk_cons_att = NULL;
    }
  or_abort (buf);
  return NULL;
}

/*
 * attribute_to_disk - Write the disk representation of an attribute.
 *    return: on overflow, or_overflow will call longjmp and                        jump to the outer caller
 *    buf(in/out): translation buffer
 *    att(in): attribute
 * Note:
 *    On overflow, or_overflow will call longjmp and jump to the outer caller
 */
static int
attribute_to_disk (OR_BUF * buf, SM_ATTRIBUTE * att)
{
  char *start;
  int offset;

  assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

  start = buf->ptr;
  /* VARIABLE OFFSET TABLE */
  /* name */
  offset = tf_Metaclass_attribute.fixed_size +
    OR_VAR_TABLE_SIZE (tf_Metaclass_attribute.n_variable);
  or_put_offset (buf, offset);

  offset += string_disk_size (att->name);

  /* initial value variable */
  or_put_offset (buf, offset);
  /* could avoid domain tag here ? */
  offset += or_packed_value_size (&att->default_value.value, 1, 1);

  /* original value */
  or_put_offset (buf, offset);
  /* could avoid domain tag here ? */
  offset += or_packed_value_size (&att->default_value.original_value, 1, 1);

  /* domain list */
  or_put_offset (buf, offset);
  offset +=
    substructure_set_size ((DB_LIST *) att->sma_domain, (LSIZER) domain_size);

  /* property list */
  or_put_offset (buf, offset);
  offset += property_list_size (att->properties);

  /* end of object */
  or_put_offset (buf, offset);
  buf->ptr = PTR_ALIGN (buf->ptr, INT_ALIGNMENT);

  /* ATTRIBUTES */
  or_put_int (buf, att->id);
  or_put_int (buf, (int) att->type->id);
  or_put_int (buf, 0);		/* memory offsets are now calculated after loading */
  or_put_int (buf, att->order);
  pr_write_mop (buf, att->class_mop);
  or_put_int (buf, att->flags);

  /* index BTID */
  /*
   * The index member of the attribute structure has been removed.  Indexes
   * are now stored on the class property list.  We still need to store
   * something out to disk since the disk representation has not changed.
   * For now, store a NULL BTID on disk.  - JB
   */
  or_put_int (buf, NULL_FILEID);
  or_put_int (buf, NULL_PAGEID);
  or_put_int (buf, 0);

  /* name */
  put_string (buf, att->name);

  /* value/original value,
   * make sure the flags match the calls to or_packed_value_size above !
   */
  or_put_value (buf, &att->default_value.value, 1, 1);
  or_put_value (buf, &att->default_value.original_value, 1, 1);

  /* domain */
  put_substructure_set (buf, (DB_LIST *) att->sma_domain,
			(LWRITER) domain_to_disk,
			&tf_Metaclass_domain.classoid,
			tf_Metaclass_domain.repid);

  /* formerly att_extension_to_disk(buf, att) */
  put_property_list (buf, att->properties);

  if (start + offset != buf->ptr)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TF_OUT_OF_SYNC, 0);
    }

  return NO_ERROR;
}


/*
 * attribute_size - Calculates the disk size of an attribute.
 *    return: disk size of attribute
 *    att(in): attribute
 */
static int
attribute_size (SM_ATTRIBUTE * att)
{
  int size;

  assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

  size = tf_Metaclass_attribute.fixed_size +
    OR_VAR_TABLE_SIZE (tf_Metaclass_attribute.n_variable);

  size += string_disk_size (att->name);
  size += or_packed_value_size (&att->default_value.value, 1, 1);
  size += or_packed_value_size (&att->default_value.original_value, 1, 1);
  size +=
    substructure_set_size ((DB_LIST *) att->sma_domain, (LSIZER) domain_size);

  /* size += att_extension_size(att); */
  size += property_list_size (att->properties);

  return size;
}

/*
 * disk_to_attribute - Reads the disk representation of an attribute and
 * fills in the supplied attribute structure.
 *    return: void
 *    buf(in/out): translation buffer
 *    att(out): attribute structure
 * Note:
 *    A jmp_buf has previously been established.
 */
static void
disk_to_attribute (OR_BUF * buf, SM_ATTRIBUTE * att)
{
  OR_VARINFO *vars;
  int fileid;
  DB_VALUE value;
  int rc = NO_ERROR;
  DB_TYPE dbval_type;

  vars = read_var_table (buf, tf_Metaclass_attribute.n_variable);
  if (vars == NULL)
    {
      or_abort (buf);

      return;
    }

  /*
   * must be sure to initialize these, the function classobj_make_attribute
   * should be split into creation & initialization functions so we can
   * have a single function that initializes the various fields.  As it
   * stands now, any change made to the attribute structure has to be
   * initialized in classobj_make_attribute and here as well
   */
  att->constraints = NULL;
  att->properties = NULL;

  att->id = or_get_int (buf, &rc);
  dbval_type = (DB_TYPE) or_get_int (buf, &rc);
  att->type = PR_TYPE_FROM_ID (dbval_type);
  att->offset = or_get_int (buf, &rc);
  att->offset = 0;		/* calculated later */
  att->order = or_get_int (buf, &rc);

  (*(tp_Object.data_readval)) (buf, &value, NULL, -1, true);
  att->class_mop = db_get_object (&value);
  /* prevents clear on next readval call */
  db_value_put_null (&value);

  att->flags = or_get_int (buf, &rc);

  fileid = or_get_int (buf, &rc);

  /* index BTID */
  /*
   * Read the NULL BTID from disk.  There is no place to put this so
   * ignore it.  - JB
   */
  (void) or_get_int (buf, &rc);
  (void) or_get_int (buf, &rc);

  /* variable attribute 0 : name */
  att->name = get_string (buf, vars[ORC_ATT_NAME_INDEX].length);

  /* variable attribute 1 : value */
  or_get_value (buf, &att->default_value.value, NULL,
		vars[ORC_ATT_CURRENT_VALUE_INDEX].length, true);

  /* variable attribute 2 : original value */
  or_get_value (buf, &att->default_value.original_value, NULL,
		vars[ORC_ATT_ORIGINAL_VALUE_INDEX].length, true);

  /* variable attribute 3 : domain */
  att->sma_domain =
    (TP_DOMAIN *) get_substructure_set (buf, NULL,
					(LREADER) disk_to_domain,
					vars[ORC_ATT_DOMAIN_INDEX].length);
  assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

  /* variable attribute 4: property list */
  /* formerly disk_to_att_extension(buf, att, vars[4].length); */
  att->properties =
    get_property_list (buf, vars[ORC_ATT_PROPERTIES_INDEX].length);

  att->default_value.default_expr = DB_DEFAULT_NONE;
  if (att->properties)
    {
      if (classobj_get_prop (att->properties, "default_expr", &value) > 0)
	{
	  att->default_value.default_expr = DB_GET_INTEGER (&value);
	  db_value_clear (&value);
	}
    }

  free_var_table (vars);
}


/*
 * repattribute_to_disk - Writes the disk representation of a representation
 * attribute.
 *    return: NO_ERROR or error code
 *    buf(in): translation buffer
 *    rat(in): attribute
 * Note:
 *    On overflow, or_overflow will call longjmp and jump to the outer caller
 */
static int
repattribute_to_disk (OR_BUF * buf, SM_REPR_ATTRIBUTE * rat)
{
  char *start;
  int offset;

  start = buf->ptr;
  /* VARIABLE OFFSET TABLE */
  offset = tf_Metaclass_repattribute.fixed_size +
    OR_VAR_TABLE_SIZE (tf_Metaclass_repattribute.n_variable);

  /* domain list */
  or_put_offset (buf, offset);
  offset +=
    substructure_set_size ((DB_LIST *) rat->domain, (LSIZER) domain_size);

  /* end of object */
  or_put_offset (buf, offset);
  buf->ptr = PTR_ALIGN (buf->ptr, INT_ALIGNMENT);
  /* fixed width attributes */
  or_put_int (buf, rat->attid);
  or_put_int (buf, (int) rat->typeid_);

  /* domain */
  put_substructure_set (buf, (DB_LIST *) rat->domain,
			(LWRITER) domain_to_disk,
			&tf_Metaclass_domain.classoid,
			tf_Metaclass_domain.repid);

  if (start + offset != buf->ptr)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TF_OUT_OF_SYNC, 0);
    }

  return NO_ERROR;
}


/*
 * repattribute_size - Calculates the disk size for the REPATTRIBUTE.
 *    return: disk size of repattribute
 *    rat(in): memory attribute
 */
static int
repattribute_size (SM_REPR_ATTRIBUTE * rat)
{
  int size;

  size = tf_Metaclass_repattribute.fixed_size +
    OR_VAR_TABLE_SIZE (tf_Metaclass_repattribute.n_variable);

  size +=
    substructure_set_size ((DB_LIST *) rat->domain, (LSIZER) domain_size);

  return (size);
}


/*
 * disk_to_repattribute - Reads the disk representation of a representation
 * attribute.
 *    return: new repattribute
 *    buf(in/out): translation buffer
 */
static SM_REPR_ATTRIBUTE *
disk_to_repattribute (OR_BUF * buf)
{
  SM_REPR_ATTRIBUTE *rat;
  OR_VARINFO *vars;
  int id, tid;
  int rc = NO_ERROR;

  vars = read_var_table (buf, tf_Metaclass_repattribute.n_variable);
  if (vars == NULL)
    {
      or_abort (buf);
      return NULL;
    }

  id = or_get_int (buf, &rc);
  tid = or_get_int (buf, &rc);
  rat = classobj_make_repattribute (id, (DB_TYPE) tid, NULL);
  if (rat == NULL)
    {
      free_var_table (vars);
      or_abort (buf);
      return NULL;
    }

  rat->domain =
    (TP_DOMAIN *) get_substructure_set (buf, NULL, (LREADER) disk_to_domain,
					vars[ORC_REPATT_DOMAIN_INDEX].length);

  free_var_table (vars);

  return rat;
}


/*
 * representation_size - Calculate the disk size for a representation.
 *    return: byte size of the representation
 *    rep(in): representation
 */
static int
representation_size (SM_REPRESENTATION * rep)
{
  int size;

  size = tf_Metaclass_representation.fixed_size +
    OR_VAR_TABLE_SIZE (tf_Metaclass_representation.n_variable);

  size += substructure_set_size ((DB_LIST *) rep->attributes,
				 (LSIZER) repattribute_size);

  size += property_list_size (NULL);

  return (size);
}


/*
 * representation_to_disk - Write the disk representation of an
 * SM_REPRESENTATION.
 *    return: NO_ERROR
 *    buf(in/out): translation buffer
 *    rep(in): representation
 * Note:
 *    On overflow, or_overflow will call longjmp and jump to the outer caller
 */
static int
representation_to_disk (OR_BUF * buf, SM_REPRESENTATION * rep)
{
  char *start;
  int offset;

  start = buf->ptr;
  offset = tf_Metaclass_representation.fixed_size +
    OR_VAR_TABLE_SIZE (tf_Metaclass_representation.n_variable);

  or_put_offset (buf, offset);
  offset +=
    substructure_set_size ((DB_LIST *) rep->attributes,
			   (LSIZER) repattribute_size);

  or_put_offset (buf, offset);
  offset += property_list_size (NULL);
  /* end of object */
  or_put_offset (buf, offset);

  buf->ptr = PTR_ALIGN (buf->ptr, INT_ALIGNMENT);

  or_put_int (buf, rep->id);
  or_put_int (buf, rep->fixed_count);
  or_put_int (buf, rep->variable_count);

  /* no longer have the fixed_size field, leave it for future expansion */
  or_put_int (buf, 0);

  put_substructure_set (buf, (DB_LIST *) rep->attributes,
			(LWRITER) repattribute_to_disk,
			&tf_Metaclass_repattribute.classoid,
			tf_Metaclass_repattribute.repid);

  put_property_list (buf, NULL);

  if (start + offset != buf->ptr)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TF_OUT_OF_SYNC, 0);
    }

  return NO_ERROR;
}


/*
 * disk_to_representation - Read the disk representation for an
 * SM_REPRESENTATION structure and build the memory structure.
 *    return: new representation structure
 *    buf(in/out): translation buffer
 */
static SM_REPRESENTATION *
disk_to_representation (OR_BUF * buf)
{
  SM_REPRESENTATION *rep;
  OR_VARINFO *vars;
  int rc = NO_ERROR;

  vars = read_var_table (buf, tf_Metaclass_representation.n_variable);
  if (vars == NULL)
    {
      or_abort (buf);
      return NULL;
    }

  rep = classobj_make_representation ();

  if (rep == NULL)
    {
      free_var_table (vars);
      or_abort (buf);
      return NULL;
    }
  else
    {
      rep->id = or_get_int (buf, &rc);
      rep->fixed_count = or_get_int (buf, &rc);
      rep->variable_count = or_get_int (buf, &rc);

      /* we no longer use this field, formerly fixed_size */
      (void) or_get_int (buf, &rc);

      /* variable 0 : attributes */
      rep->attributes = (SM_REPR_ATTRIBUTE *)
	get_substructure_set (buf, NULL, (LREADER) disk_to_repattribute,
			      vars[ORC_REP_ATTRIBUTES_INDEX].length);
    }

  free_var_table (vars);
  return (rep);
}


/*
 * check_class_structure - maps over the class prior to storage to make sure
 * that everything looks ok.
 *    return: error code
 *    class(in): class strucutre
 * Note:
 *    The point is to get errors detected early on so that the lower level
 *    translation functions don't have to worry about them.
 *    It is MANDATORY that this be called before any size or other walk of
 *    the class
 */
static int
check_class_structure (UNUSED_ARG SM_CLASS * class_)
{
  int ok = 1;

  /* Currently, there is nothing to decache.  I expect have to populate
     the class property list here someting in the near future so I'll
     leave the function in place.  - JB */

/*  decache_attribute_properties(class); */
  return (ok);
}


/*
 * put_class_varinfo - Writes the variable offset table for a class object.
 *    return: ending offset
 *    buf(in/out): translation buffer
 *    class(in): class structure
 * Note:
 *    This is the only meta object that includes OR_HEADER_SIZE
 *    as part of the offset calculations.  This is because the other
 *    substructures are all stored in sets inside the class object.
 *    Returns the offset within the buffer after the offset table
 *    (offset to first fixed attribute).
 */
static int
put_class_varinfo (OR_BUF * buf, SM_CLASS * class_)
{
  int offset;
  DB_VALUE owner_name;
  char *str_name = NULL;

  DB_MAKE_NULL (&owner_name);

  /* name */
  offset = OR_HEADER_SIZE + tf_Metaclass_class.fixed_size +
    OR_VAR_TABLE_SIZE (tf_Metaclass_class.n_variable);
  or_put_offset (buf, offset);

  /* owner_name */
  offset += string_disk_size (class_->header.name);
  or_put_offset (buf, offset);

  /* loader_commands */
  if (class_->owner != NULL)
    {
      obj_get (class_->owner, "name", &owner_name);
      str_name = DB_GET_STRING (&owner_name);
    }
  offset += string_disk_size (str_name);
  or_put_offset (buf, offset);

  /* representation */
  offset += string_disk_size (class_->loader_commands);
  or_put_offset (buf, offset);

  /* attributes */
  offset += substructure_set_size ((DB_LIST *) class_->representations,
				   (LSIZER) representation_size);
  or_put_offset (buf, offset);

  /* query spec */
  offset += substructure_set_size ((DB_LIST *) class_->attributes,
				   (LSIZER) attribute_size);
  or_put_offset (buf, offset);

  /* constraints */
  offset +=
    substructure_set_size ((DB_LIST *) class_->query_spec,
			   (LSIZER) query_spec_size);
  or_put_offset (buf, offset);

  /* end of object */
  offset += substructure_set_size ((DB_LIST *) class_->disk_constraints,
				   (LSIZER) constraint_size);

  or_put_offset (buf, offset);

  buf->ptr = PTR_ALIGN (buf->ptr, INT_ALIGNMENT);

  pr_clear_value (&owner_name);

  return (offset);
}

/*
 * put_class_attributes - Writes the fixed and variable attributes of a class.
 *    return: void
 *    buf(in/out): translation buffer
 *    class(in): class structure
 */
static void
put_class_attributes (OR_BUF * buf, SM_CLASS * class_)
{
  DB_VALUE owner_name;
  char *str_name = NULL;

  DB_MAKE_NULL (&owner_name);

  /* attribute id counters */

  /* doesn't exist yet */
  or_put_int (buf, class_->att_ids);
  or_put_int (buf, 0);		/* unused, formerly rep_ids counter */

  or_put_int (buf, class_->header.heap.vfid.fileid);
  or_put_int (buf, class_->header.heap.vfid.volid);
  or_put_int (buf, class_->header.heap.hpgid);

  or_put_int (buf, class_->repid);
  or_put_int (buf, class_->fixed_count);
  or_put_int (buf, class_->variable_count);
  or_put_int (buf, class_->fixed_size);

  assert (class_->att_count
	  == (class_->fixed_count + class_->variable_count));
  or_put_int (buf, class_->att_count);
  /* object size is now calculated after loading */
  or_put_int (buf, 0);
  or_put_int (buf, class_->flags);
  or_put_int (buf, (int) class_->class_type);

  /* owner object */
  pr_write_mop (buf, class_->owner);
  or_put_int (buf, (int) LANG_SYS_COLLATION);	/* unused, collation_id */

  /* 0: NAME */
  put_string (buf, class_->header.name);

  /* 1: owner name */
  if (class_->owner != NULL)
    {
      obj_get (class_->owner, "name", &owner_name);
      str_name = DB_GET_STRING (&owner_name);
    }
  put_string (buf, str_name);

  /* 2: loader_commands */
  put_string (buf, class_->loader_commands);

  /* 3: representations */
  put_substructure_set (buf, (DB_LIST *) class_->representations,
			(LWRITER) representation_to_disk,
			&tf_Metaclass_representation.classoid,
			tf_Metaclass_representation.repid);

  /* 4: attributes */
  put_substructure_set (buf, (DB_LIST *) class_->attributes,
			(LWRITER) attribute_to_disk,
			&tf_Metaclass_attribute.classoid,
			tf_Metaclass_attribute.repid);

  /* 5: query_spec */
  put_substructure_set (buf, (DB_LIST *) class_->query_spec,
			(LWRITER) query_spec_to_disk,
			&tf_Metaclass_query_spec.classoid,
			tf_Metaclass_query_spec.repid);

  /* 6: constraint */
  put_substructure_set (buf, (DB_LIST *) class_->disk_constraints,
			(LWRITER) disk_constraint_to_disk,
			&tf_Metaclass_representation.classoid,
			tf_Metaclass_representation.repid);

  pr_clear_value (&owner_name);
}


/*
 * class_to_disk - Write the disk representation of a class.
 *    return: void
 *    buf(in/out): translation buffer
 *    class(in): class structure
 * Note:
 *    The writing of the offset table and the attributes was split into
 *    two pieces because it was getting too long.
 *    Caller must have a setup a jmpbuf (called setjmp) to handle any
 *    errors
 */
static void
class_to_disk (OR_BUF * buf, SM_CLASS * class_)
{
  char *start;
  int offset;

  /* kludge, we may have to do some last minute adj of the class structures
     before saving.  In particular, some of the attribute fields need to
     be placed in the attribute property list because there are no corresponding
     fields in the disk representation.  This may result in storage allocation
     which because we don't have modern computers may fail.  This
     function does all of the various checking up front so we don't have
     to detect it later in the substructure conversion routines */
  if (!check_class_structure (class_))
    {
      or_abort (buf);
    }
  else
    {
      start = buf->ptr - OR_HEADER_SIZE;	/* header already written */
      offset = put_class_varinfo (buf, class_);
      put_class_attributes (buf, class_);

      if (start + offset != buf->ptr)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TF_OUT_OF_SYNC, 0);
	  or_abort (buf);
	}
    }
}


/*
 * tf_class_size - Calculates the disk size of a class.
 *    return: disk size of class
 *    classobj(in): pointer to class structure
 */
static int
tf_class_size (MOBJ classobj)
{
  SM_CLASS *class_;
  int size;
  DB_VALUE owner_name;
  char *str_name = NULL;

  DB_MAKE_NULL (&owner_name);

  class_ = (SM_CLASS *) classobj;

  /* make sure properties are decached */
  if (!check_class_structure (class_))
    {
      return (-1);
    }

  size = OR_HEADER_SIZE;	/* ? */
  size += tf_Metaclass_class.fixed_size +
    OR_VAR_TABLE_SIZE (tf_Metaclass_class.n_variable);

  size += string_disk_size (class_->header.name);

  if (class_->owner != NULL)
    {
      obj_get (class_->owner, "name", &owner_name);
      str_name = DB_GET_STRING (&owner_name);
    }
  size += string_disk_size (str_name);

  size += string_disk_size (class_->loader_commands);

  size += substructure_set_size ((DB_LIST *) class_->representations,
				 (LSIZER) representation_size);

  size += substructure_set_size ((DB_LIST *) class_->attributes,
				 (LSIZER) attribute_size);

  size +=
    substructure_set_size ((DB_LIST *) class_->query_spec,
			   (LSIZER) query_spec_size);

  size += substructure_set_size ((DB_LIST *) class_->disk_constraints,
				 (LSIZER) constraint_size);

  pr_clear_value (&owner_name);

  return (size);
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * tf_dump_class_size - Debugging function to display disk size information
 * for a class.
 *    return:  void
 *    classobj(in): pointer to class structure
 */
void
tf_dump_class_size (MOBJ classobj)
{
  SM_CLASS *class_;
  int size, s;

  class_ = (SM_CLASS *) classobj;

  /* make sure properties are decached */
  if (!check_class_structure (class_))
    {
      return;
    }

  size = OR_HEADER_SIZE;	/* ? */
  size += tf_Metaclass_class.fixed_size +
    OR_VAR_TABLE_SIZE (tf_Metaclass_class.n_variable);
  fprintf (stdout, "Fixed size %d\n", size);

  s = string_disk_size (class_->header.name);
  fprintf (stdout, "Header name %d\n", s);
  size += s;

  s = string_disk_size (class_->loader_commands);
  fprintf (stdout, "Loader commands %d\n", s);
  size += s;

  s = substructure_set_size ((DB_LIST *) class_->representations,
			     (LSIZER) representation_size);
  fprintf (stdout, "Representations %d\n", s);
  size += s;

  s = substructure_set_size ((DB_LIST *) class_->attributes,
			     (LSIZER) attribute_size);
  fprintf (stdout, "Attributes %d\n", s);
  size += s;

  s =
    substructure_set_size ((DB_LIST *) class_->query_spec,
			   (LSIZER) query_spec_size);
  fprintf (stdout, "Query_Spec statements %d\n", s);
  size += s;

  s = property_list_size (class_->properties);
  fprintf (stdout, "Properties %d\n", s);
  size += s;

  fprintf (stdout, "TOTAL: %d\n", size);
}
#endif /* ENABLE_UNUSED_FUNCTION */


/*
 * disk_to_class - Reads the disk representation of a class and creates the
 * memory structure.
 *    return: class structure
 *    buf(in/out): translation buffer
 *    class_ptr(out): return pointer
 * Note:
      A jmp_buf has previously been established.
 */
static SM_CLASS *
disk_to_class (OR_BUF * buf, SM_CLASS ** class_ptr)
{
  SM_CLASS *class_;
  OR_VARINFO *vars;
  DB_VALUE value;
  int rc = NO_ERROR;

  class_ = NULL;
  vars = read_var_table (buf, tf_Metaclass_class.n_variable);
  if (vars == NULL)
    {
      or_abort (buf);
      return NULL;
    }

  class_ = *class_ptr = classobj_make_class (NULL);
  if (class_ == NULL)
    {
      free_var_table (vars);
      or_abort (buf);
      return NULL;
    }

  class_->att_ids = or_get_int (buf, &rc);
  (void) or_get_int (buf, &rc);	/* unused, rep_ids */

  class_->header.heap.vfid.fileid = or_get_int (buf, &rc);
  class_->header.heap.vfid.volid = or_get_int (buf, &rc);
  class_->header.heap.hpgid = or_get_int (buf, &rc);

  class_->repid = or_get_int (buf, &rc);
  class_->fixed_count = or_get_int (buf, &rc);
  class_->variable_count = or_get_int (buf, &rc);
  class_->fixed_size = or_get_int (buf, &rc);
  class_->att_count = or_get_int (buf, &rc);
  class_->object_size = or_get_int (buf, &rc);
  class_->object_size = 0;	/* calculated later */
  class_->flags = or_get_int (buf, &rc);
  class_->class_type = (SM_CLASS_TYPE) or_get_int (buf, &rc);

  /* owner object */
  (*(tp_Object.data_readval)) (buf, &value, NULL, -1, true);
  class_->owner = db_get_object (&value);
  (void) or_get_int (buf, &rc);	/* unused, collation_id */

  /* variable 0 */
  assert (vars[ORC_NAME_INDEX].offset
	  == CAST_BUFLEN (buf->ptr - buf->buffer));
  class_->header.name = get_string (buf, vars[ORC_NAME_INDEX].length);

  /* variable 1 */
  /* skip reading owner name string value.
   * This stored value is used in catalog record insertion.
   */
  assert (vars[ORC_OWNER_NAME_INDEX].offset
	  == CAST_BUFLEN (buf->ptr - buf->buffer));
  or_advance (buf, vars[ORC_OWNER_NAME_INDEX].length);

  /* variable 2 */
  assert (vars[ORC_LOADER_COMMANDS_INDEX].offset
	  == CAST_BUFLEN (buf->ptr - buf->buffer));
  class_->loader_commands =
    get_string (buf, vars[ORC_LOADER_COMMANDS_INDEX].length);

  /* variable 3 */
  assert (vars[ORC_REPRESENTATIONS_INDEX].offset
	  == CAST_BUFLEN (buf->ptr - buf->buffer));
  class_->representations = (SM_REPRESENTATION *)
    get_substructure_set (buf, NULL, (LREADER) disk_to_representation,
			  vars[ORC_REPRESENTATIONS_INDEX].length);

  /* variable 4 */
  class_->attributes = (SM_ATTRIBUTE *)
    classobj_alloc_threaded_array (sizeof (SM_ATTRIBUTE), class_->att_count);
  if (class_->att_count && class_->attributes == NULL)
    {
      db_ws_free (class_);
      free_var_table (vars);
      or_abort (buf);
      return NULL;
    }
  assert (vars[ORC_ATTRIBUTES_INDEX].offset
	  == CAST_BUFLEN (buf->ptr - buf->buffer));
  install_substructure_set (buf, (DB_LIST *) class_->attributes,
			    (VREADER) disk_to_attribute,
			    vars[ORC_ATTRIBUTES_INDEX].length);

  /* variable 5 */
  assert (vars[ORC_QUERY_SPEC_INDEX].offset
	  == CAST_BUFLEN (buf->ptr - buf->buffer));
  class_->query_spec = (SM_QUERY_SPEC *)
    get_substructure_set (buf, NULL, (LREADER) disk_to_query_spec,
			  vars[ORC_QUERY_SPEC_INDEX].length);

  /* variable 6 */
  assert (vars[ORC_CONSTRAINTS_INDEX].offset
	  == CAST_BUFLEN (buf->ptr - buf->buffer));
  class_->disk_constraints =
    (SM_DISK_CONSTRAINT *) get_substructure_set (buf, NULL,
						 (LREADER)
						 disk_to_constraint,
						 vars[ORC_CONSTRAINTS_INDEX].
						 length);

  /* build the ordered instance instance list */
  classobj_fixup_loaded_class (class_);

  free_var_table (vars);

  return class_;
}

/*
 * root_to_disk - Write the disk representation of the root class.
 *    return: void
 *    buf(in/out): translation buffer
 *    root(in): root class object
 * Note:
 *    Caller must have a setup a jmpbuf (called setjmp) to handle any
 *    errors
 */
static void
root_to_disk (OR_BUF * buf, ROOT_CLASS * root)
{
  char *start;
  int offset;

  start = buf->ptr - OR_HEADER_SIZE;	/* header already written */
  /* variable table */
  offset = OR_HEADER_SIZE + tf_Metaclass_root.fixed_size +
    OR_VAR_TABLE_SIZE (tf_Metaclass_root.n_variable);

  /* name */
  or_put_offset (buf, offset);
  offset += string_disk_size (root->header.name);

  /* end of object */
  or_put_offset (buf, offset);
  buf->ptr = PTR_ALIGN (buf->ptr, INT_ALIGNMENT);

  /* heap file id - see assumptions in comment above */
  or_put_int (buf, (int) root->header.heap.vfid.fileid);
  or_put_int (buf, (int) root->header.heap.vfid.volid);
  or_put_int (buf, (int) root->header.heap.hpgid);

  put_string (buf, root->header.name);

  if (start + offset != buf->ptr)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TF_OUT_OF_SYNC, 0);
    }
}


/*
 * root_size - Calculates the disk size of the root class.
 *    return: disk size of root class
 *    rootobj(in): root class object
 */
static int
root_size (MOBJ rootobj)
{
  ROOT_CLASS *root;
  int size;

  root = (ROOT_CLASS *) rootobj;

  size = OR_HEADER_SIZE;	/* ? */
  size += tf_Metaclass_root.fixed_size +
    OR_VAR_TABLE_SIZE (tf_Metaclass_root.n_variable);

  /* name */
  size += string_disk_size (root->header.name);

  return (size);
}


/*
 * disk_to_root - Reads the disk representation of the root class and builds
 * the memory rootclass.
 *    return: root class object
 *    buf(in/out): translation buffer
 * Note:
 *    We know there is only one static structure for the root class so we use
 *    it rather than allocating a structure.
 */
static ROOT_CLASS *
disk_to_root (OR_BUF * buf)
{
  OR_VARINFO *vars;
  int rc = NO_ERROR;

  vars = read_var_table (buf, tf_Metaclass_root.n_variable);
  if (vars == NULL)
    {
      or_abort (buf);
    }
  else
    {
      sm_Root_class.header.heap.vfid.fileid = (FILEID) or_get_int (buf, &rc);
      sm_Root_class.header.heap.vfid.volid = (VOLID) or_get_int (buf, &rc);
      sm_Root_class.header.heap.hpgid = (PAGEID) or_get_int (buf, &rc);

      /* name - could make sure its the same as sm_Root_class_name */
      or_advance (buf, vars[0].length);

      free_var_table (vars);
    }
  return (&sm_Root_class);
}


/*
 * tf_disk_to_class - transforming the disk representation of a class.
 *    return: class structure
 *    oid(in):
 *    record(in): disk record
 * Note:
 *    It is imperitive that garbage collection be disabled during this
 *    operation.
 *    This is because the structure being built may contain pointers
 *    to MOPs but it has not itself been attached to its own MOP yet so it
 *    doesn't serve as a gc root.   Make sure the caller
 *    calls ws_cache() immediately after we return so the new object
 *    gets attached as soon as possible.  We must also have already
 *    allocated the MOP in the caller so that we don't get one of the
 *    periodic gc's when we attempt to allocate a new MOP.
 *    This dependency should be isolated in a ws_ function called by
 *    the locator.
 */
MOBJ
tf_disk_to_class (OID * oid, RECDES * record)
{
  OR_BUF orep, *buf;
  unsigned int repid;
  int grpid;
  /* declare volatile to prevent reg optimization */
  MOBJ volatile class_;
  int rc = NO_ERROR;

  assert (oid != NULL && !OID_ISNULL (oid));

  /* should we assume this ? */
  if (!tf_Metaclass_class.n_variable)
    {
      tf_compile_meta_classes ();
    }

  class_ = NULL;

  buf = &orep;
  or_init (buf, record->data, record->length);
  buf->error_abort = 1;

  switch (_setjmp (buf->env))
    {
    case 0:
      /* offset size */
      assert (OR_GET_OFFSET_SIZE (buf->ptr) == BIG_VAR_OFFSET_SIZE);

      repid = or_get_int (buf, &rc);
      repid = repid & ~OR_OFFSET_SIZE_FLAG;

      grpid = or_get_int (buf, &rc);

      /* set grpid to (((SM_CLASS *)class_)->header.obj_header.xxx)
       * if necessary.
       */

      if (oid_is_root (oid))
	{
	  class_ = (MOBJ) disk_to_root (buf);
	}
      else
	{
	  class_ = (MOBJ) disk_to_class (buf, (SM_CLASS **) & class_);
	}
      break;

    default:
      /*
       * make sure to clear the class that was being created,
       * an appropriate error will have been set
       */
      if (class_ != NULL)
	{
	  classobj_free_class ((SM_CLASS *) class_);
	  class_ = NULL;
	}
      break;
    }

  sm_bump_global_schema_version ();

  buf->error_abort = 0;
  return (class_);
}


/*
 * tf_class_to_disk - creates the disk representation of a class.
 *    return: zero for success, non-zero if errors
 *    classobj(in): pointer to class structure
 *    record(out): disk record
 * Note:
 *    If the record was not large enough for the class, resulting in
 *    the or_overflow error, this function returns the required size of the
 *    record as a negative number.
 */
TF_STATUS
tf_class_to_disk (MOBJ classobj, RECDES * record)
{
  OR_BUF orep, *buf;
  /* prevent reg optimization which hoses longmp */
  SM_CLASS *volatile class_;
  volatile int expected_size;
  SM_CLASS_HEADER *header;
  TF_STATUS status;
  int rc = 0;
  unsigned int repid;

  /* should we assume this ? */
  if (!tf_Metaclass_class.n_variable)
    {
      tf_compile_meta_classes ();
    }

  /*
   * don't worry about deferred fixup for classes, we don't usually have
   * many temporary OIDs in classes.
   */
  buf = &orep;
  or_init (buf, record->data, record->area_size);
  buf->error_abort = 1;

  class_ = (SM_CLASS *) classobj;

  header = (SM_CLASS_HEADER *) classobj;
  if (header->type != Meta_root)
    {
      /* put all default_expr values in attribute properties */
      rc = tf_attribute_default_expr_to_property (class_->attributes);
    }

  /*
   * test - this isn't necessary but we've been having a class size related
   * bug that I want to try to catch - take this out when we're sure
   */
  if (class_ == (SM_CLASS *) (&sm_Root_class))
    {
      expected_size = root_size (classobj);
    }
  else
    {
      expected_size = tf_class_size (classobj);
    }

  /* if anything failed this far, no need to save stack context, return code
     will be handled in the switch below */
  if (rc == NO_ERROR)
    {
      rc = _setjmp (buf->env);
    }

  switch (rc)
    {
    case 0:
      status = TF_SUCCESS;

      /* representation id, offset size */
      repid = 0;
      OR_SET_VAR_OFFSET_SIZE (repid, BIG_VAR_OFFSET_SIZE);	/* 4byte */
      or_put_int (buf, repid);

      /* get groupid from (class_->header.obj_header.xxx ) if necessary. */
      or_put_int (buf, GLOBAL_GROUPID);

      if (header->type == Meta_root)
	{
	  root_to_disk (buf, (ROOT_CLASS *) class_);
	}
      else
	{
	  class_to_disk (buf, (SM_CLASS *) class_);
	}

      record->length = CAST_BUFLEN (buf->ptr - buf->buffer);

      /* sanity test, this sets an error only so we can see it if it happens */
      if (record->length != expected_size)
	{
	  assert (false);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TF_SIZE_MISMATCH, 2,
		  expected_size, record->length);
	  or_abort (buf);
	}

      /* fprintf(stdout, "Saved class in %d bytes\n", record->length); */
      break;

      /*
       * if the longjmp status was anything other than ER_TF_BUFFER_OVERFLOW,
       * it represents an error condition and er_set will have been called
       */

    case ER_TF_BUFFER_OVERFLOW:
      status = TF_OUT_OF_SPACE;
      record->length = -expected_size;
      break;

    default:
      status = TF_ERROR;
      break;

    }

  buf->error_abort = 0;
  return (status);
}


/*
 * tf_object_size - Determines the number of byte required to store an object
 * on disk.
 *    return: byte size of object on disk
 *    classobj(in): class of instance
 *    obj(in): instance to examine
 * Note:
 *    This will work for any object; classes, instances, or the rootclass.
 */
int
tf_object_size (MOBJ classobj, MOBJ obj)
{
  int size = 0;

  if (classobj != (MOBJ) (&sm_Root_class))
    {
      int dummy;
      size = object_size ((SM_CLASS *) classobj, obj, &dummy);
    }
  else if (obj == (MOBJ) (&sm_Root_class))
    {
      size = root_size (obj);
    }
  else
    {
      size = tf_class_size (obj);
    }

  return size;
}

/*
 * tf_attribute_default_expr_to_property - transfer default_expr flag to a
 *                                         disk stored property
 *  returns: error code or NO_ERROR
 *  attr_list(in): attribute list to process
 */
static int
tf_attribute_default_expr_to_property (SM_ATTRIBUTE * attr_list)
{
  SM_ATTRIBUTE *attr = NULL;

  if (attr_list == NULL)
    {
      /* nothing to do */
      return NO_ERROR;
    }

  for (attr = attr_list; attr; attr = attr->next)
    {
      if (attr->default_value.default_expr != DB_DEFAULT_NONE)
	{
	  /* attr has expression as default value */
	  DB_VALUE default_expr;

	  if (attr->properties == NULL)
	    {
	      /* allocate new property sequence */
	      attr->properties = classobj_make_prop ();

	      if (attr->properties == NULL)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_OUT_OF_VIRTUAL_MEMORY, sizeof (DB_SEQ));
		  return er_errid ();
		}
	    }

	  /* add default_expr property to sequence */
	  db_make_int (&default_expr, attr->default_value.default_expr);
	  classobj_put_prop (attr->properties, "default_expr", &default_expr);
	  pr_clear_value (&default_expr);
	}
      else if (attr->properties != NULL)
	{
	  /* make sure property is unset for existing attributes */
	  classobj_drop_prop (attr->properties, "default_expr");
	}
    }

  /* all ok */
  return NO_ERROR;
}
