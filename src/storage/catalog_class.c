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
 * catalog_class.c - catalog class
 */

#ident "$Id$"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "storage_common.h"
#include "error_manager.h"
#include "system_catalog.h"
#include "heap_file.h"
#include "btree.h"
#include "oid.h"
#include "object_representation.h"
#include "object_representation_sr.h"
#include "transform.h"
#include "set_object.h"
#include "locator_sr.h"
#include "memory_hash.h"
#include "system_parameter.h"
#include "class_object.h"
#include "critical_section.h"
#include "xserver_interface.h"
#include "memory_alloc.h"
#include "language_support.h"
#include "numeric_opfunc.h"
#include "string_opfunc.h"
#include "dbtype.h"
#include "db_date.h"

/* this must be the last header file included!!! */
#include "dbval.h"

#define IS_SUBSET(value)        (value).sub.count >= 0

#define EXCHANGE_OR_VALUE(a,b) \
  do { \
    OR_VALUE t; \
    t = a; \
    a = b; \
    b = t; \
  } while (0)


#define CATCLS_OID_TABLE_SIZE   1024

typedef struct or_value OR_VALUE;
typedef struct catcls_entry CATCLS_ENTRY;
typedef int (*CREADER) (THREAD_ENTRY * thread_p, OR_BUF * buf,
			OR_VALUE * value_p, DB_VALUE * pkey_vals);

struct or_value
{
  union or_id
  {
    OID classoid;
    ATTR_ID attrid;
  } id;
  DB_VALUE value;
  struct or_sub
  {
    struct or_value *value;
    int count;
  } sub;
};

struct catcls_entry
{
  OID class_oid;
  OID oid;
  CATCLS_ENTRY *next;
};

/* TODO: add to ct_class.h */
bool catcls_Enable = false;

static BTID catcls_Btid;
static CATCLS_ENTRY *catcls_Free_entry_list = NULL;
static MHT_TABLE *catcls_Class_oid_to_oid_hash_table = NULL;

/* TODO: move to ct_class.h */
extern int catcls_compile_catalog_classes (THREAD_ENTRY * thread_p);
extern int catcls_insert_catalog_classes (THREAD_ENTRY * thread_p,
					  RECDES * record);
extern int catcls_delete_catalog_classes (THREAD_ENTRY * thread_p,
					  const char *name, OID * class_oid);
extern int catcls_update_catalog_classes (THREAD_ENTRY * thread_p,
					  const char *name, RECDES * record);
extern int catcls_finalize_class_oid_to_oid_hash_table (THREAD_ENTRY *
							thread_p);
extern int catcls_remove_entry (THREAD_ENTRY * thread_p, OID * class_oid);
extern int catcls_get_db_collation (THREAD_ENTRY * thread_p,
				    LANG_COLL_COMPAT ** db_collations,
				    int *coll_cnt);
extern int catcls_get_applier_info (THREAD_ENTRY * thread_p,
				    INT64 * max_delay);
extern int catcls_get_analyzer_info (THREAD_ENTRY * thread_p,
				     INT64 * current_pageid,
				     INT64 * required_pageid);
extern int catcls_get_writer_info (THREAD_ENTRY * thread_p,
				   INT64 * last_flushed_pageid,
				   INT64 * eof_pageid);

static int catcls_initialize_class_oid_to_oid_hash_table (THREAD_ENTRY *
							  thread_p,
							  int num_entry);
static int catcls_get_or_value_from_class (THREAD_ENTRY * thread_p,
					   OR_BUF * buf_p,
					   OR_VALUE * value_p);
static int catcls_get_or_value_from_attribute (THREAD_ENTRY * thread_p,
					       OR_BUF * buf_p,
					       OR_VALUE * value_p,
					       DB_VALUE * pkey_vals);
static int catcls_get_or_value_from_attrid (THREAD_ENTRY * thread_p,
					    OR_BUF * buf_p,
					    OR_VALUE * value_p,
					    DB_VALUE * pkey_vals);
static int catcls_get_or_value_from_domain (THREAD_ENTRY * thread_p,
					    OR_BUF * buf_p,
					    OR_VALUE * value_p,
					    DB_VALUE * pkey_vals);
static int catcls_get_or_value_from_query_spec (THREAD_ENTRY * thread_p,
						OR_BUF * buf_p,
						OR_VALUE * value_p,
						DB_VALUE * pkey_vals);

static int catcls_get_subset (THREAD_ENTRY * thread_p, OR_BUF * buf_p,
			      int expected_size, OR_VALUE * value_p,
			      CREADER reader, DB_VALUE * pkey_val);
static int catcls_get_constraints (THREAD_ENTRY * thread_p,
				   OR_BUF * buf_p, OR_VALUE * value_p,
				   DB_VALUE * pkey_vals);
static int catcls_get_constraints_atts (THREAD_ENTRY * thread_p,
					OR_BUF * buf_p, OR_VALUE * value_p,
					DB_VALUE * pkey_vals);
static int catcls_reorder_attributes_by_repr (THREAD_ENTRY * thread_p,
					      OR_VALUE * value_p);
static int catcls_expand_or_value_by_repr (OR_VALUE * value_p,
					   OID * class_oid, DISK_REPR * rep);
static void catcls_expand_or_value_by_subset (THREAD_ENTRY * thread_p,
					      OR_VALUE * value_p);

static int catcls_get_or_value_from_buffer (THREAD_ENTRY * thread_p,
					    OR_BUF * buf_p,
					    OR_VALUE * value_p,
					    DISK_REPR * rep);
static int catcls_put_or_value_into_buffer (OR_VALUE * value_p,
					    OR_BUF * buf_p, OID * class_oid,
					    DISK_REPR * rep);

static OR_VALUE *catcls_get_or_value_from_class_record (THREAD_ENTRY *
							thread_p,
							RECDES * record);
static OR_VALUE *catcls_get_or_value_from_record (THREAD_ENTRY * thread_p,
						  RECDES * record,
						  OID * class_oid);
static int catcls_put_or_value_into_record (THREAD_ENTRY * thread_p,
					    OR_VALUE * value_p,
					    RECDES * record, OID * class_oid);

static int catcls_insert_subset (THREAD_ENTRY * thread_p, OR_VALUE * value_p,
				 OID * root_oid);
static int catcls_delete_subset (THREAD_ENTRY * thread_p, OR_VALUE * value_p);
static int catcls_update_subset (THREAD_ENTRY * thread_p, OR_VALUE * value_p,
				 OR_VALUE * old_value, int *uflag);

static int catcls_insert_instance (THREAD_ENTRY * thread_p,
				   OR_VALUE * value_p, OID * oid,
				   OID * root_oid, OID * class_oid,
				   HFID * hfid, HEAP_SCANCACHE * scan);
static int catcls_delete_instance (THREAD_ENTRY * thread_p, OID * oid,
				   OID * class_oid, HFID * hfid,
				   HEAP_SCANCACHE * scan);
static int catcls_update_instance (THREAD_ENTRY * thread_p,
				   OR_VALUE * value_p, OID * oid,
				   OID * class_oid, HFID * hfid,
				   HEAP_SCANCACHE * scan);
static CATCLS_ENTRY *catcls_allocate_entry (THREAD_ENTRY * thread_p);
static int catcls_free_entry (const void *key, void *data, void *args);
static OID *catcls_find_oid (THREAD_ENTRY * thread_p, OID * class_oid);
static int catcls_put_entry (THREAD_ENTRY * thread_p, CATCLS_ENTRY * entry);
static char *catcls_unpack_allocator (int size);
static OR_VALUE *catcls_allocate_or_value (int size);
static void catcls_free_sub_value (OR_VALUE * values, int count);
static void catcls_free_or_value (OR_VALUE * value);
static int catcls_expand_or_value_by_def (OR_VALUE * value_p, CT_CLASS * def);
static int catcls_guess_record_length (OR_VALUE * value_p);
static int catcls_find_class_oid_by_class_name (THREAD_ENTRY * thread_p,
						const char *name,
						OID * class_oid);
static int catcls_find_btid_of_class_name (THREAD_ENTRY * thread_p,
					   BTID * btid);
static int catcls_find_oid_by_class_name (THREAD_ENTRY * thread_p,
					  const char *name, OID * oid);
static int catcls_convert_class_oid_to_oid (THREAD_ENTRY * thread_p,
					    DB_VALUE * oid_val);
static int catcls_convert_attr_id_to_name (THREAD_ENTRY * thread_p,
					   OR_BUF * orbuf_p,
					   OR_VALUE * value_p);

/*
 * catcls_allocate_entry () -
 *   return:
 *   thread_p(in):
 */
static CATCLS_ENTRY *
catcls_allocate_entry (UNUSED_ARG THREAD_ENTRY * thread_p)
{
  CATCLS_ENTRY *entry_p;

  assert (csect_check_own (thread_p, CSECT_CT_OID_TABLE) == 1);

  if (catcls_Free_entry_list != NULL)
    {
      entry_p = catcls_Free_entry_list;
      catcls_Free_entry_list = catcls_Free_entry_list->next;
    }
  else
    {
      entry_p = (CATCLS_ENTRY *) malloc (sizeof (CATCLS_ENTRY));
      if (entry_p == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, sizeof (CATCLS_ENTRY));
	  return NULL;
	}
    }

  entry_p->next = NULL;
  return entry_p;
}

/*
 * catcls_free_entry () -
 *   return:
 *   key(in):
 *   data(in):
 *   args(in):
 */
static int
catcls_free_entry (UNUSED_ARG const void *key, void *data,
		   UNUSED_ARG void *args)
{
//  THREAD_ENTRY *thread_p = thread_get_thread_entry_info ();
  CATCLS_ENTRY *entry_p;

//  assert (csect_check_own (thread_p, CSECT_CT_OID_TABLE) == 1);
  assert (csect_check_own
	  (thread_get_thread_entry_info (), CSECT_CT_OID_TABLE) == 1);

  entry_p = (CATCLS_ENTRY *) data;
  entry_p->next = catcls_Free_entry_list;
  catcls_Free_entry_list = entry_p;

  return NO_ERROR;
}

/*
 * catcls_initialize_class_oid_to_oid_hash_table () -
 *   return:
 *   thread_p(in):
 *   num_entry(in):
 */
static int
catcls_initialize_class_oid_to_oid_hash_table (UNUSED_ARG THREAD_ENTRY *
					       thread_p, int num_entry)
{
  if (csect_enter (thread_p, CSECT_CT_OID_TABLE, INF_WAIT) != NO_ERROR)
    {
      return ER_FAILED;
    }

  catcls_Class_oid_to_oid_hash_table =
    mht_create ("Class OID to OID", num_entry, oid_hash, oid_compare_equals);

  if (catcls_Class_oid_to_oid_hash_table == NULL)
    {
      csect_exit (CSECT_CT_OID_TABLE);
      return ER_FAILED;
    }

  csect_exit (CSECT_CT_OID_TABLE);

  return NO_ERROR;
}

/*
 * catcls_finalize_class_oid_to_oid_hash_table () -
 *   return:
 *   thread_p(in):
 */
int
catcls_finalize_class_oid_to_oid_hash_table (UNUSED_ARG THREAD_ENTRY *
					     thread_p)
{
  CATCLS_ENTRY *entry_p, *next_p;

  if (csect_enter (thread_p, CSECT_CT_OID_TABLE, INF_WAIT) != NO_ERROR)
    {
      return ER_FAILED;
    }

  if (catcls_Class_oid_to_oid_hash_table)
    {
      mht_map (catcls_Class_oid_to_oid_hash_table, catcls_free_entry, NULL);
      mht_destroy (catcls_Class_oid_to_oid_hash_table);
    }

  for (entry_p = catcls_Free_entry_list; entry_p; entry_p = next_p)
    {
      next_p = entry_p->next;
      free_and_init (entry_p);
    }
  catcls_Free_entry_list = NULL;
  catcls_Class_oid_to_oid_hash_table = NULL;

  csect_exit (CSECT_CT_OID_TABLE);

  return NO_ERROR;
}

/*
 * catcls_find_oid () -
 *   return:
 *   thread_p(in):
 *   class_oid(in):
 */
static OID *
catcls_find_oid (UNUSED_ARG THREAD_ENTRY * thread_p, OID * class_oid_p)
{
  CATCLS_ENTRY *entry_p;

#if defined (SERVER_MODE)
  assert (csect_check_own (thread_p, CSECT_CT_OID_TABLE) == 2);
#endif

  if (catcls_Class_oid_to_oid_hash_table)
    {
      entry_p =
	(CATCLS_ENTRY *) mht_get (catcls_Class_oid_to_oid_hash_table,
				  (void *) class_oid_p);
      if (entry_p != NULL)
	{
	  return &entry_p->oid;
	}
      else
	{
	  return NULL;
	}
    }

  return NULL;
}

/*
 * catcls_put_entry () -
 *   return:
 *   thread_p(in):
 *   entry(in):
 */
static int
catcls_put_entry (UNUSED_ARG THREAD_ENTRY * thread_p, CATCLS_ENTRY * entry_p)
{
  assert (csect_check_own (thread_p, CSECT_CT_OID_TABLE) == 1);

  if (catcls_Class_oid_to_oid_hash_table)
    {
      if (mht_put
	  (catcls_Class_oid_to_oid_hash_table, &entry_p->class_oid,
	   entry_p) == NULL)
	{
	  return ER_FAILED;
	}
    }

  return NO_ERROR;
}

/*
 * catcls_remove_entry () -
 *   return:
 *   thread_p(in):
 *   class_oid(in):
 */
int
catcls_remove_entry (UNUSED_ARG THREAD_ENTRY * thread_p, OID * class_oid_p)
{
  assert (csect_check_own (thread_p, CSECT_CT_OID_TABLE) == 1);

  if (catcls_Class_oid_to_oid_hash_table)
    {
      mht_rem (catcls_Class_oid_to_oid_hash_table, class_oid_p,
	       catcls_free_entry, NULL);
    }

  return NO_ERROR;
}

/*
 * catcls_unpack_allocator () -
 *   return:
 *   size(in):
 */
static char *
catcls_unpack_allocator (int size)
{
  return ((char *) malloc (size));
}

/*
 * catcls_allocate_or_value () -
 *   return:
 *   size(in):
 */
static OR_VALUE *
catcls_allocate_or_value (int size)
{
  OR_VALUE *value_p;
  int msize, i;

  msize = size * sizeof (OR_VALUE);
  value_p = (OR_VALUE *) malloc (msize);
  if (value_p == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      msize);
    }
  else
    {
      for (i = 0; i < size; i++)
	{
	  db_value_put_null (&value_p[i].value);
	  value_p[i].sub.value = NULL;
	  value_p[i].sub.count = -1;
	}
    }

  return (value_p);
}

/*
 * cr_free_sub_value () -
 *   return:
 *   values(in):
 *   count(in):
 */
static void
catcls_free_sub_value (OR_VALUE * values, int count)
{
  int i;

  if (values != NULL)
    {
      for (i = 0; i < count; i++)
	{
	  pr_clear_value (&values[i].value);
	  catcls_free_sub_value (values[i].sub.value, values[i].sub.count);
	}
      free_and_init (values);
    }
}

/*
 * catcls_free_or_value () -
 *   return:
 *   value(in):
 */
static void
catcls_free_or_value (OR_VALUE * value_p)
{
  if (value_p != NULL)
    {
      pr_clear_value (&value_p->value);
      catcls_free_sub_value (value_p->sub.value, value_p->sub.count);
      free_and_init (value_p);
    }
}

/*
 * catcls_expand_or_value_by_def () -
 *   return:
 *   value(in):
 *   def(in):
 */
static int
catcls_expand_or_value_by_def (OR_VALUE * value_p, CT_CLASS * def_p)
{
  OR_VALUE *attrs_p;
  int n_attrs;
  CT_ATTR *att_def_p;
  int i;
  int error;

  if (value_p != NULL)
    {
      /* index_of */
      COPY_OID (&value_p->id.classoid, &def_p->classoid);

      n_attrs = def_p->n_atts;
      attrs_p = catcls_allocate_or_value (n_attrs);
      if (attrs_p == NULL)
	{
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      value_p->sub.value = attrs_p;
      value_p->sub.count = n_attrs;

      att_def_p = def_p->atts;
      for (i = 0; i < n_attrs; i++)
	{
	  attrs_p[i].id.attrid = att_def_p[i].id;
	  error = db_value_domain_init (&attrs_p[i].value, att_def_p[i].type,
					DB_DEFAULT_PRECISION,
					DB_DEFAULT_SCALE);
	  if (error != NO_ERROR)
	    {
	      return error;
	    }
	}
    }

  return NO_ERROR;
}

/*
 * catcls_guess_record_length () -
 *   return:
 *   value(in):
 */
static int
catcls_guess_record_length (OR_VALUE * value_p)
{
  int length;
  DB_TYPE data_type;
  PR_TYPE *map_p;
  OR_VALUE *attrs_p;
  int n_attrs, i;

  attrs_p = value_p->sub.value;
  n_attrs = value_p->sub.count;

  length = OR_HEADER_SIZE + OR_VAR_TABLE_SIZE (n_attrs)
    + OR_BOUND_BIT_BYTES (n_attrs);

  for (i = 0; i < n_attrs; i++)
    {
      data_type = DB_VALUE_DOMAIN_TYPE (&attrs_p[i].value);
      map_p = tp_Type_id_map[data_type];

      if (map_p->data_lengthval != NULL)
	{
	  length += (*(map_p->data_lengthval)) (&attrs_p[i].value, 1);
	}
      else if (map_p->disksize)
	{
	  length += map_p->disksize;
	}
      /* else : is null-type */
    }

  return (length);
}

/*
 * catcls_find_class_oid_by_class_name () -
 *   return:
 *   name(in):
 *   class_oid(in):
 */
static int
catcls_find_class_oid_by_class_name (THREAD_ENTRY * thread_p,
				     const char *name_p, OID * class_oid_p)
{
  LC_FIND_CLASSNAME status;

  status = xlocator_find_class_oid (thread_p, name_p, class_oid_p, NULL_LOCK);

  if (status == LC_CLASSNAME_ERROR)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
	      ER_LC_UNKNOWN_CLASSNAME, 1, name_p);
      return ER_FAILED;
    }
  else if (status == LC_CLASSNAME_DELETED)
    {
      /* not found the class */
      OID_SET_NULL (class_oid_p);
    }

  return NO_ERROR;
}

/*
 * catcls_find_btid_of_class_name () -
 *   return:
 *   btid(in):
 */
static int
catcls_find_btid_of_class_name (THREAD_ENTRY * thread_p, BTID * btid_p)
{
  CT_ATTR *atts;
  int n_atts;
  int i;
  DISK_REPR *repr_p = NULL;
  DISK_ATTR *att_repr_p;
  REPR_ID repr_id;
  OID *index_class_p;
  ATTR_ID index_key;
  int error = NO_ERROR;

#define CATCLS_INDEX_NAME "i__db_table_table_name"

  index_class_p = &ct_Class.classoid;	/* init */
  index_key = -1;		/* init */

  atts = ct_Class.atts;
  n_atts = ct_Class.n_atts;

  for (i = 0; i < n_atts; i++)
    {
      if (strcmp (atts[i].name, "table_name") == 0)
	{
	  index_key = atts[i].id;
	  break;
	}
    }

  if (index_key == -1)
    {
      assert (false);
      goto exit_on_error;	/* not found */
    }

  error =
    catalog_get_last_representation_id (thread_p, index_class_p, &repr_id);
  if (error != NO_ERROR)
    {
      goto exit_on_error;
    }

  repr_p = catalog_get_representation (thread_p, index_class_p, repr_id);
  if (repr_p == NULL)
    {
      assert (false);
      error = er_errid ();
      if (error == NO_ERROR)
	{
	  error = ER_FAILED;
	}
      goto exit_on_error;
    }

  for (att_repr_p = repr_p->variable; att_repr_p->id != index_key;
       att_repr_p++)
    {
      ;
    }

  if (att_repr_p->bt_stats == NULL)
    {
      assert (false);
      error = ER_SM_NO_INDEX;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, CATCLS_INDEX_NAME);
      goto exit_on_error;

    }
  else
    {
      BTID_COPY (btid_p, &(att_repr_p->bt_stats->btid));
    }

  if (error != NO_ERROR)
    {
      goto exit_on_error;
    }

exit_on_end:

  if (repr_p)
    {
      catalog_free_representation (repr_p);
    }

  return error;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "invalid error code");
    }

  goto exit_on_end;
}

/*
 * catcls_find_oid_by_class_name () - Get an instance oid in the ct_Class using the
 *                               index for classname
 *   return:
 *   name(in):
 *   oid(in):
 */
static int
catcls_find_oid_by_class_name (THREAD_ENTRY * thread_p, const char *name_p,
			       OID * oid_p)
{
  DB_IDXKEY class_name_key;
  int error = NO_ERROR;

  DB_IDXKEY_MAKE_NULL (&class_name_key);

  error = db_make_varchar (&(class_name_key.vals[0]),
			   DB_MAX_IDENTIFIER_LENGTH,
			   (char *) name_p, strlen (name_p),
			   LANG_SYS_COLLATION);
  if (error != NO_ERROR)
    {
      return error;
    }

  class_name_key.size = 1;

  error = xbtree_find_unique (thread_p, &ct_Class.classoid, &catcls_Btid,
			      &class_name_key, oid_p);
  if (error == BTREE_ERROR_OCCURRED)
    {
      error = er_errid ();
      return ((error == NO_ERROR) ? ER_FAILED : error);
    }
  else if (error == BTREE_KEY_NOTFOUND)
    {
      OID_SET_NULL (oid_p);
    }

  return NO_ERROR;
}

/*
 * catcls_convert_class_oid_to_oid () -
 *   return:
 *   oid_val(in):
 */
static int
catcls_convert_class_oid_to_oid (THREAD_ENTRY * thread_p,
				 DB_VALUE * oid_val_p)
{
  char *name_p = NULL;
  OID oid_buf;
  OID *class_oid_p, *oid_p;
  CATCLS_ENTRY *entry_p;

  if (DB_IS_NULL (oid_val_p))
    {
      return NO_ERROR;
    }

  class_oid_p = DB_PULL_OID (oid_val_p);

  if (csect_enter_as_reader (thread_p, CSECT_CT_OID_TABLE, INF_WAIT) !=
      NO_ERROR)
    {
      return ER_FAILED;
    }

  oid_p = catcls_find_oid (thread_p, class_oid_p);

  csect_exit (CSECT_CT_OID_TABLE);

  if (oid_p == NULL)
    {
      oid_p = &oid_buf;
      name_p = heap_get_class_name (thread_p, class_oid_p);
      if (name_p == NULL)
	{
	  return NO_ERROR;
	}

      if (catcls_find_oid_by_class_name (thread_p, name_p, oid_p) != NO_ERROR)
	{
	  free_and_init (name_p);
	  return er_errid ();
	}

      if (!OID_ISNULL (oid_p))
	{
	  if (csect_enter (thread_p, CSECT_CT_OID_TABLE, INF_WAIT) !=
	      NO_ERROR)
	    {
	      return ER_FAILED;
	    }

	  entry_p = catcls_allocate_entry (thread_p);
	  if (entry_p != NULL)
	    {
	      COPY_OID (&entry_p->class_oid, class_oid_p);
	      COPY_OID (&entry_p->oid, oid_p);
	      catcls_put_entry (thread_p, entry_p);
	    }

	  csect_exit (CSECT_CT_OID_TABLE);
	}
    }

  db_push_oid (oid_val_p, oid_p);

  if (name_p)
    {
      free_and_init (name_p);
    }

  return NO_ERROR;
}

/*
 * catcls_convert_attr_id_to_name () -
 *   return:
 *   obuf(in):
 *   value(in):
 */
static int
catcls_convert_attr_id_to_name (THREAD_ENTRY * thread_p, OR_BUF * orbuf_p,
				OR_VALUE * value_p)
{
  OR_BUF *buf_p, orep;
  OR_VALUE *indexes, *keys;
  OR_VALUE *index_atts, *key_atts;
  OR_VALUE *id_val_p = NULL, *id_atts;
  OR_VALUE *ids;
  OR_VARINFO *vars = NULL;
  int id;
  int size;
  int i, j, k;
  int error = NO_ERROR;

  buf_p = &orep;
  or_init (buf_p, orbuf_p->buffer, (int) (orbuf_p->endptr - orbuf_p->buffer));

  or_advance (buf_p, OR_HEADER_SIZE);

  size = tf_Metaclass_class.n_variable;
  vars = or_get_var_table (buf_p, size, catcls_unpack_allocator);
  if (vars == NULL)
    {
      int msize = size * sizeof (OR_VARINFO);
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, msize);

      return error;
    }

  /* jump to the 'attributes' and extract its id/name.
   * there are no indexes for shared or class attributes,
   * so we need only id/name for 'attributes'.
   */
  or_seek (buf_p, vars[ORC_ATTRIBUTES_INDEX].offset);

  id_val_p = catcls_allocate_or_value (1);
  if (id_val_p == NULL)
    {
      free_and_init (vars);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  error = catcls_get_subset (thread_p, buf_p,
			     vars[ORC_ATTRIBUTES_INDEX].length,
			     id_val_p, catcls_get_or_value_from_attrid, NULL);
  if (error != NO_ERROR)
    {
      free_and_init (vars);
      free_and_init (id_val_p);
      return error;
    }

  /* replace id with name for each key attribute */
  for (indexes = value_p->sub.value, i = 0; i < value_p->sub.count; i++)
    {
      index_atts = indexes[i].sub.value;

      for (keys = (index_atts[5]).sub.value,
	   j = 0; j < (index_atts[5]).sub.count; j++)
	{
	  key_atts = keys[j].sub.value;

	  if (!DB_IS_NULL (&key_atts[3].value))
	    {
	      id = DB_GET_INT (&key_atts[3].value);
	      pr_clear_value (&key_atts[3].value);

	      for (ids = id_val_p->sub.value, k = 0; k < id_val_p->sub.count;
		   k++)
		{
		  id_atts = ids[k].sub.value;
		  if (!DB_IS_NULL (&id_atts[0].value)
		      && id == DB_GET_INT (&id_atts[0].value))
		    {
		      pr_clone_value (&id_atts[1].value, &key_atts[3].value);
		    }
		}
	    }
	}
    }

  catcls_free_or_value (id_val_p);
  free_and_init (vars);

  return NO_ERROR;
}

/*
 * catcls_get_or_value_from_class () -
 *   return:
 *   buf(in):
 *   value(in):
 */
static int
catcls_get_or_value_from_class (THREAD_ENTRY * thread_p, OR_BUF * buf_p,
				OR_VALUE * value_p)
{
  OR_VALUE *attrs;
  DB_VALUE *attr_val_p;
  DB_VALUE key_value;
  OR_VARINFO *vars = NULL;
  int size;
  OR_VALUE *resolution_p = NULL;
  OID class_oid;
  int error = NO_ERROR;

  DB_MAKE_NULL (&key_value);

  error = catcls_expand_or_value_by_def (value_p, &ct_Class);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  attrs = value_p->sub.value;

  /** variable offset **/
  size = tf_Metaclass_class.n_variable;
  vars = or_get_var_table (buf_p, size, catcls_unpack_allocator);
  if (vars == NULL)
    {
      int msize = size * sizeof (OR_VARINFO);

      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, msize);
      GOTO_EXIT_ON_ERROR;
    }

  /* fixed */

  or_advance (buf_p, ORC_ATT_COUNT_OFFSET);

  /* attribute_count */
  (*(tp_Integer.data_readval)) (buf_p, &attrs[1].value,
				tp_domain_resolve_default (DB_TYPE_INTEGER),
				-1, true);

  /* object_size */
  or_advance (buf_p, OR_INT_SIZE);

  /* flags */
  (*(tp_Integer.data_readval)) (buf_p, &attrs[2].value,
				tp_domain_resolve_default (DB_TYPE_INTEGER),
				-1, true);

  /* class_type */
  (*(tp_Integer.data_readval)) (buf_p, &attrs[3].value,
				tp_domain_resolve_default (DB_TYPE_INTEGER),
				-1, true);

  /* owner */
  (*(tp_Object.data_readval)) (buf_p, &attrs[4].value,
			       tp_domain_resolve_default (DB_TYPE_OBJECT), -1,
			       true);

  /* collation_id */
  (*(tp_Integer.data_readval)) (buf_p, &attrs[5].value,
				tp_domain_resolve_default (DB_TYPE_INTEGER),
				-1, true);

  /* variable */

  /* name */
  attr_val_p = &attrs[6].value;
  (*(tp_String.data_readval)) (buf_p, attr_val_p,
			       tp_domain_resolve_default (DB_TYPE_VARCHAR),
			       vars[ORC_NAME_INDEX].length, true);
  db_string_truncate (attr_val_p, DB_MAX_IDENTIFIER_LENGTH);

  /* (class_of) */
  error = catcls_find_class_oid_by_class_name (thread_p,
					       DB_GET_STRING (&attrs[6].
							      value),
					       &class_oid);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  db_push_oid (&attrs[0].value, &class_oid);

  /* owner_name */
  attr_val_p = &attrs[7].value;
  (*(tp_String.data_readval)) (buf_p, attr_val_p,
			       tp_domain_resolve_default (DB_TYPE_VARCHAR),
			       vars[ORC_OWNER_NAME_INDEX].length, true);
  db_string_truncate (attr_val_p, DB_MAX_IDENTIFIER_LENGTH);

  /* loader_commands */
  or_advance (buf_p, vars[ORC_LOADER_COMMANDS_INDEX].length);

  /* representations */
  or_advance (buf_p, vars[ORC_REPRESENTATIONS_INDEX].length);

  /* pass keys to child */
  db_value_clone (&attrs[6].value, &key_value);

  /* attributes */
  error = catcls_get_subset (thread_p, buf_p,
			     vars[ORC_ATTRIBUTES_INDEX].length,
			     &attrs[8], catcls_get_or_value_from_attribute,
			     &key_value);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* query_spec */
  error = catcls_get_subset (thread_p, buf_p,
			     vars[ORC_QUERY_SPEC_INDEX].length,
			     &attrs[9], catcls_get_or_value_from_query_spec,
			     &key_value);
  if (error != NO_ERROR)
    {

      GOTO_EXIT_ON_ERROR;
    }

  /* constraints */
  error = catcls_get_subset (thread_p, buf_p,
			     vars[ORC_CONSTRAINTS_INDEX].length,
			     &attrs[10], catcls_get_constraints, &key_value);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = catcls_convert_attr_id_to_name (thread_p, buf_p, &attrs[10]);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  db_value_clear (&key_value);

  if (vars)
    {
      free_and_init (vars);
    }

  assert (error == NO_ERROR);
  return NO_ERROR;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "invalid error code");
    }

  if (vars)
    {
      free_and_init (vars);
    }

  if (resolution_p)
    {
      catcls_free_or_value (resolution_p);
    }

  db_value_clear (&key_value);

  return error;
}

/*
 * catcls_get_or_value_from_attribute () -
 *   return:
 *   buf(in):
 *   value(in):
 */
static int
catcls_get_or_value_from_attribute (THREAD_ENTRY * thread_p, OR_BUF * buf_p,
				    OR_VALUE * value_p, DB_VALUE * pkey_vals)
{
  OR_VALUE *attrs;
  DB_VALUE *attr_val_p;
  DB_VALUE default_expr, val;
  DB_VALUE tmp_val;
  DB_SEQ *att_props;
  OR_VARINFO *vars = NULL;
  DB_VALUE key_values[2];
  int size;
  int error = NO_ERROR;

  assert (value_p != NULL);

  DB_MAKE_NULL (&(key_values[0]));
  DB_MAKE_NULL (&(key_values[1]));

  error = catcls_expand_or_value_by_def (value_p, &ct_Attribute);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* The order of attrs[] is same with that of ct_attribute_atts[]. */
  attrs = value_p->sub.value;

  /** variable offset **/
  size = tf_Metaclass_attribute.n_variable;
  vars = or_get_var_table (buf_p, size, catcls_unpack_allocator);
  if (vars == NULL)
    {
      int msize = size * sizeof (OR_VARINFO);

      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, msize);
      GOTO_EXIT_ON_ERROR;
    }

  /* id */
  or_advance (buf_p, OR_INT_SIZE);

  /* parent class key value */
  if (pkey_vals != NULL)
    {
      db_value_clone (pkey_vals, &attrs[1].value);
    }

  /* type -> data_type */
  (*(tp_Integer.data_readval)) (buf_p, &attrs[3].value,
				tp_domain_resolve_default (DB_TYPE_INTEGER),
				-1, true);

  /* offset */
  or_advance (buf_p, OR_INT_SIZE);

  /* order -> def_order */
  (*(tp_Integer.data_readval)) (buf_p, &attrs[4].value,
				tp_domain_resolve_default (DB_TYPE_INTEGER),
				-1, true);

  /* class */
  or_advance (buf_p, OR_OID_SIZE);

  /* flags -> is_nullable */
  DB_MAKE_NULL (&tmp_val);
  (*(tp_Integer.data_readval)) (buf_p, &tmp_val,
				tp_domain_resolve_default (DB_TYPE_INTEGER),
				-1, true);

  /* for 'is_nullable', reverse NON_NULL flag */
  attr_val_p = &attrs[5].value;
  db_make_int (attr_val_p,
	       (DB_GET_INT (&tmp_val) & SM_ATTFLAG_NON_NULL) ? 0 : 1);

  attr_val_p = &attrs[6].value;
  db_make_int (attr_val_p,
	       (DB_GET_INT (&tmp_val) & SM_ATTFLAG_SHARD_KEY) ? 1 : 0);

  db_value_clear (&tmp_val);

  /* index_fileid */
  or_advance (buf_p, OR_INT_SIZE);

  /* index_root_pageid */
  or_advance (buf_p, OR_INT_SIZE);

  /* index_volid_key */
  or_advance (buf_p, OR_INT_SIZE);

  /** variable **/

  /* name -> attr_name */
  attr_val_p = &attrs[2].value;
  (*(tp_String.data_readval)) (buf_p, attr_val_p,
			       tp_domain_resolve_default (DB_TYPE_VARCHAR),
			       vars[ORC_ATT_NAME_INDEX].length, true);
  db_string_truncate (attr_val_p, DB_MAX_IDENTIFIER_LENGTH);

  /* value -> default_value */
  attr_val_p = &attrs[7].value;
  or_get_value (buf_p, attr_val_p, NULL,
		vars[ORC_ATT_CURRENT_VALUE_INDEX].length, true);

  /* original_value - advance only */
  or_advance (buf_p, vars[ORC_ATT_ORIGINAL_VALUE_INDEX].length);

  /* pass key to child */
  db_value_clone (&attrs[1].value, &key_values[0]);
  db_value_clone (&attrs[2].value, &key_values[1]);

  /* domain -> domains */
  error =
    catcls_get_subset (thread_p, buf_p, vars[ORC_ATT_DOMAIN_INDEX].length,
		       &attrs[8], catcls_get_or_value_from_domain,
		       key_values);
  if (error != NO_ERROR)
    {
      db_value_clear (&key_values[0]);
      db_value_clear (&key_values[1]);

      GOTO_EXIT_ON_ERROR;
    }

  db_value_clear (&key_values[0]);
  db_value_clear (&key_values[1]);

  /* properties */
  or_get_value (buf_p, &val, tp_domain_resolve_default (DB_TYPE_SEQUENCE),
		vars[ORC_ATT_PROPERTIES_INDEX].length, true);
  att_props = DB_GET_SEQUENCE (&val);
  attr_val_p = &attrs[7].value;
  if (att_props != NULL &&
      classobj_get_prop (att_props, "default_expr", &default_expr) > 0)
    {
      char *str_val;

      str_val = (char *) malloc (strlen ("UNIX_TIMESTAMP") + 1);

      if (str_val == NULL)
	{
	  pr_clear_value (&default_expr);
	  pr_clear_value (&val);

	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
		  1, strlen ("UNIX_TIMESTAMP") + 1);
	  GOTO_EXIT_ON_ERROR;
	}

      switch (DB_GET_INT (&default_expr))
	{
	case DB_DEFAULT_SYSDATE:
	  strcpy (str_val, "SYS_DATE");
	  break;

	case DB_DEFAULT_SYSDATETIME:
	  strcpy (str_val, "SYS_DATETIME");
	  break;

	case DB_DEFAULT_UNIX_TIMESTAMP:
	  strcpy (str_val, "UNIX_TIMESTAMP");
	  break;

	case DB_DEFAULT_USER:
	  strcpy (str_val, "USER");
	  break;

	case DB_DEFAULT_CURR_USER:
	  strcpy (str_val, "CURRENT_USER");
	  break;

	default:
	  assert (false);

	  pr_clear_value (&default_expr);
	  pr_clear_value (&val);

	  free_and_init (str_val);

	  error = ER_GENERIC_ERROR;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");

	  GOTO_EXIT_ON_ERROR;
	  break;
	}

      pr_clear_value (attr_val_p);	/*clean old default value */
      DB_MAKE_STRING (attr_val_p, str_val);
    }
  else
    {
      valcnv_convert_value_to_string (attr_val_p);
    }
  pr_clear_value (&val);
  attr_val_p->need_clear = true;
  db_string_truncate (attr_val_p, DB_MAX_IDENTIFIER_LENGTH);

  if (vars)
    {
      free_and_init (vars);
    }

  assert (error == NO_ERROR);
  return NO_ERROR;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "invalid error code");
    }

  if (vars)
    {
      free_and_init (vars);
    }

  return error;
}

/*
 * catcls_get_or_value_from_attrid () -
 *   return:
 *   buf(in):
 *   value(in):
 */
static int
catcls_get_or_value_from_attrid (UNUSED_ARG THREAD_ENTRY * thread_p,
				 OR_BUF * buf, OR_VALUE * value,
				 UNUSED_ARG DB_VALUE * pkey_vals)
{
  OR_VALUE *attrs;
  DB_VALUE *attr_val;
  OR_VARINFO *vars = NULL;
  int size;
  char *start_ptr;
  int error = NO_ERROR;

  error = catcls_expand_or_value_by_def (value, &ct_Attrid);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  attrs = value->sub.value;

  /* variable offset */
  start_ptr = buf->ptr;

  size = tf_Metaclass_attribute.n_variable;
  vars = or_get_var_table (buf, size, catcls_unpack_allocator);
  if (vars == NULL)
    {
      int msize = size * sizeof (OR_VARINFO);

      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, msize);
      GOTO_EXIT_ON_ERROR;
    }

  /* id */
  (*(tp_Integer.data_readval)) (buf, &attrs[0].value,
				tp_domain_resolve_default (DB_TYPE_INTEGER),
				-1, true);

  or_advance (buf,
	      (int) (start_ptr - buf->ptr) + vars[ORC_ATT_NAME_INDEX].offset);

  /* name */
  attr_val = &attrs[1].value;
  (*(tp_String.data_readval)) (buf, attr_val,
			       tp_domain_resolve_default (DB_TYPE_VARCHAR),
			       vars[ORC_ATT_NAME_INDEX].length, true);
  db_string_truncate (attr_val, DB_MAX_IDENTIFIER_LENGTH);

  /* go to the end */
  or_advance (buf, (int) (start_ptr - buf->ptr)
	      + (vars[(size - 1)].offset + vars[(size - 1)].length));

  if (vars)
    {
      free_and_init (vars);
    }

  assert (error == NO_ERROR);
  return NO_ERROR;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "invalid error code");
    }

  if (vars)
    {
      free_and_init (vars);
    }

  return error;
}

/*
 * catcls_get_or_value_from_domain () -
 *   return:
 *   buf(in):
 *   value(in):
 */
static int
catcls_get_or_value_from_domain (THREAD_ENTRY * thread_p, OR_BUF * buf_p,
				 OR_VALUE * value_p, DB_VALUE * pkey_vals)
{
  OR_VALUE *attrs;
  DB_VALUE *attr_val_p;
  OR_VARINFO *vars = NULL;
  int size;
  int error = NO_ERROR;
  char *domain_class_name = NULL;
  OID *class_oid_p;

  error = catcls_expand_or_value_by_def (value_p, &ct_Domain);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  attrs = value_p->sub.value;

  /** variable offset **/
  size = tf_Metaclass_domain.n_variable;
  vars = or_get_var_table (buf_p, size, catcls_unpack_allocator);
  if (vars == NULL)
    {
      int msize = size * sizeof (OR_VARINFO);

      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, msize);
      GOTO_EXIT_ON_ERROR;
    }

  if (pkey_vals != NULL)
    {
      /* parent class key value */
      db_value_clone (&pkey_vals[0], &attrs[1].value);
      db_value_clone (&pkey_vals[1], &attrs[2].value);
    }

  /* type */
  (*(tp_Integer.data_readval)) (buf_p, &attrs[3].value,
				tp_domain_resolve_default (DB_TYPE_INTEGER),
				-1, true);

  /* precision */
  (*(tp_Integer.data_readval)) (buf_p, &attrs[4].value,
				tp_domain_resolve_default (DB_TYPE_INTEGER),
				-1, true);

  /* scale */
  (*(tp_Integer.data_readval)) (buf_p, &attrs[5].value,
				tp_domain_resolve_default (DB_TYPE_INTEGER),
				-1, true);

  /* codeset */
  (*(tp_Integer.data_readval)) (buf_p, &attrs[6].value,
				tp_domain_resolve_default (DB_TYPE_INTEGER),
				-1, true);

  /* collation id */
  (*(tp_Integer.data_readval)) (buf_p, &attrs[7].value,
				tp_domain_resolve_default (DB_TYPE_INTEGER),
				-1, true);

  /* class */
  attr_val_p = &attrs[8].value;
  (*(tp_Object.data_readval)) (buf_p, attr_val_p,
			       tp_domain_resolve_default (DB_TYPE_OBJECT), -1,
			       true);

  if (!DB_IS_NULL (attr_val_p))
    {
      class_oid_p = DB_PULL_OID (attr_val_p);
      domain_class_name = heap_get_class_name (thread_p, class_oid_p);

      if (domain_class_name != NULL)
	{
	  DB_MAKE_STRING (&attrs[9].value, domain_class_name);
	  attrs[9].value.need_clear = true;

	  db_string_truncate (&attrs[9].value, DB_MAX_IDENTIFIER_LENGTH);
	}

      error = catcls_convert_class_oid_to_oid (thread_p, attr_val_p);

      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      if (DB_IS_NULL (attr_val_p))
	{
	  /* if self reference for example, "class x (a x)"
	     set an invalid data type, and fill its value later */
	  error = db_value_domain_init (attr_val_p, DB_TYPE_VARIABLE,
					DB_DEFAULT_PRECISION,
					DB_DEFAULT_SCALE);
	  if (error != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}
    }

  /* set_domain */
  error =
    catcls_get_subset (thread_p, buf_p,
		       vars[ORC_DOMAIN_SETDOMAIN_INDEX].length, &attrs[10],
		       catcls_get_or_value_from_domain, pkey_vals);

  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (vars)
    {
      free_and_init (vars);
    }

  assert (error == NO_ERROR);
  return NO_ERROR;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "invalid error code");
    }

  if (domain_class_name)
    {
      free_and_init (domain_class_name);
    }
  if (vars)
    {
      free_and_init (vars);
    }

  return error;
}

/*
 * catcls_get_or_value_from_query_spec () -
 *   return:
 *   buf(in):
 *   value(in):
 */
static int
catcls_get_or_value_from_query_spec (UNUSED_ARG THREAD_ENTRY * thread_p,
				     OR_BUF * buf_p, OR_VALUE * value_p,
				     DB_VALUE * pkey_vals)
{
  OR_VALUE *attrs;
  DB_VALUE *attr_val_p;
  OR_VARINFO *vars = NULL;
  int size;
  int error = NO_ERROR;

  error = catcls_expand_or_value_by_def (value_p, &ct_Queryspec);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  attrs = value_p->sub.value;

  /** variable offset **/
  size = tf_Metaclass_query_spec.n_variable;
  vars = or_get_var_table (buf_p, size, catcls_unpack_allocator);
  if (vars == NULL)
    {
      int msize = size * sizeof (OR_VARINFO);

      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, msize);
      GOTO_EXIT_ON_ERROR;
    }

  /* parent class key */
  if (pkey_vals != NULL)
    {
      db_value_clone (pkey_vals, &attrs[1].value);
    }

  /* specification */
  attr_val_p = &attrs[2].value;
  (*(tp_String.data_readval)) (buf_p, attr_val_p,
			       tp_domain_resolve_default (DB_TYPE_VARCHAR),
			       vars[ORC_QUERY_SPEC_SPEC_INDEX].length, true);
  db_string_truncate (attr_val_p, DB_MAX_SPEC_LENGTH);

  if (vars)
    {
      free_and_init (vars);
    }

  assert (error == NO_ERROR);
  return NO_ERROR;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "invalid error code");
    }

  if (vars)
    {
      free_and_init (vars);
    }

  return error;
}

/*
 * catcls_get_subset () -
 *   return:
 *   buf(in):
 *   expected_size(in):
 *   value(in):
 *   reader(in):
 */
static int
catcls_get_subset (THREAD_ENTRY * thread_p, OR_BUF * buf_p,
		   int expected_size, OR_VALUE * value_p, CREADER reader,
		   DB_VALUE * pkey_val)
{
  OR_VALUE *subset_p;
  int count, i;
  int error = NO_ERROR;

  if (expected_size == 0)
    {
      value_p->sub.count = 0;
      return NO_ERROR;
    }

  count = or_skip_set_header (buf_p);
  subset_p = catcls_allocate_or_value (count);
  if (subset_p == NULL)
    {
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  value_p->sub.value = subset_p;
  value_p->sub.count = count;

  for (i = 0; i < count; i++)
    {
      error = (*reader) (thread_p, buf_p, &subset_p[i], pkey_val);
      if (error != NO_ERROR)
	{
	  return error;
	}
    }

  return NO_ERROR;
}

/*
 * catcls_get_constraints () -
 *   return:
 *   buf(in):
 *   expected_size(in):
 *   value(in):
 */
static int
catcls_get_constraints (THREAD_ENTRY * thread_p,
			OR_BUF * buf_p, OR_VALUE * value_p,
			DB_VALUE * pkey_vals)
{
  OR_VALUE *attrs, *cons_atts;
  DB_VALUE *attr_val_p;
  OR_VARINFO *vars = NULL;
  DB_VALUE tmp_val;
  DB_VALUE key_values[2];
  int size;
  int error = NO_ERROR;
  int i;

  assert (value_p != NULL);

  DB_MAKE_NULL (&(key_values[0]));
  DB_MAKE_NULL (&(key_values[1]));

  error = catcls_expand_or_value_by_def (value_p, &ct_Index);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  attrs = value_p->sub.value;

  /** variable offset **/
  size = tf_Metaclass_constraint.n_variable;
  vars = or_get_var_table (buf_p, size, catcls_unpack_allocator);
  if (vars == NULL)
    {
      int msize = size * sizeof (OR_VARINFO);

      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, msize);
      GOTO_EXIT_ON_ERROR;
    }

  /* parent class name */
  if (pkey_vals != NULL)
    {
      db_value_clone (pkey_vals, &attrs[1].value);
    }

  /* sm_constraint_type */
  DB_MAKE_NULL (&tmp_val);
  (*(tp_Integer.data_readval)) (buf_p, &tmp_val,
				tp_domain_resolve_default (DB_TYPE_INTEGER),
				-1, true);

  attr_val_p = &attrs[3].value;
  db_make_int (attr_val_p,
	       SM_IS_CONSTRAINT_UNIQUE_FAMILY (DB_GET_INT (&tmp_val))
	       ? 1 : 0);

  attr_val_p = &attrs[6].value;
  db_make_int (attr_val_p,
	       DB_GET_INT (&tmp_val) == SM_CONSTRAINT_PRIMARY_KEY ? 1 : 0);

  /* constraints status */
  attr_val_p = &attrs[7].value;
  (*(tp_Integer.data_readval)) (buf_p, attr_val_p,
				tp_domain_resolve_default (DB_TYPE_INTEGER),
				-1, true);

  /* constraint name */
  attr_val_p = &attrs[2].value;
  (*(tp_String.data_readval)) (buf_p, attr_val_p,
			       tp_domain_resolve_default (DB_TYPE_VARCHAR),
			       vars[ORC_CLASS_CONSTRAINTS_NAME_INDEX].length,
			       true);
  db_string_truncate (attr_val_p, DB_MAX_IDENTIFIER_LENGTH);

  /* btid */
  attr_val_p = &tmp_val;
  (*(tp_String.data_readval)) (buf_p, attr_val_p,
			       tp_domain_resolve_default (DB_TYPE_VARCHAR),
			       vars[ORC_CLASS_CONSTRAINTS_BTID_INDEX].length,
			       true);
  pr_clear_value (&tmp_val);

  /* pass key to child */
  db_value_clone (&attrs[1].value, &key_values[0]);
  db_value_clone (&attrs[2].value, &key_values[1]);

  error = catcls_get_subset (thread_p, buf_p,
			     vars[ORC_CLASS_CONSTRAINTS_ATTS_INDEX].length,
			     &attrs[5], catcls_get_constraints_atts,
			     key_values);

  db_value_clear (&key_values[0]);
  db_value_clear (&key_values[1]);

  /* key_order */
  cons_atts = attrs[5].sub.value;
  for (i = 0; i < attrs[5].sub.count; i++)
    {
      db_make_int (&cons_atts[i].sub.value[4].value, i);
    }

  /* number of attributes */
  db_make_int (&attrs[4].value, attrs[5].sub.count);

  free_and_init (vars);

  assert (error == NO_ERROR);
  return NO_ERROR;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "invalid error code");
    }

  return error;
}

/*
 * catcls_get_constraints_atts () -
 *   return:
 *   buf(in):
 *   expected_size(in):
 *   value(in):
 */
static int
catcls_get_constraints_atts (UNUSED_ARG THREAD_ENTRY * thread_p,
			     OR_BUF * buf_p, OR_VALUE * value_p,
			     DB_VALUE * pkey_vals)
{
  OR_VALUE *attrs;
  DB_VALUE *attr_val_p;
  OR_VARINFO *vars = NULL;
  DB_VALUE tmp_val;
  int size;
  int error = NO_ERROR;

  assert (value_p != NULL);

  error = catcls_expand_or_value_by_def (value_p, &ct_Indexkey);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  attrs = value_p->sub.value;

  /** variable offset **/
  size = tf_Metaclass_constraint_attribute.n_variable;
  vars = or_get_var_table (buf_p, size, catcls_unpack_allocator);
  if (vars == NULL)
    {
      int msize = size * sizeof (OR_VARINFO);

      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, msize);
      GOTO_EXIT_ON_ERROR;
    }

  /* parent class key (class_name) */
  if (pkey_vals != NULL)
    {
      db_value_clone (&pkey_vals[0], &attrs[1].value);
      db_value_clone (&pkey_vals[1], &attrs[2].value);
    }

  /* att_id */
  attr_val_p = &attrs[3].value;
  (*(tp_Integer.data_readval)) (buf_p, attr_val_p,
				tp_domain_resolve_default (DB_TYPE_INTEGER),
				-1, true);

  /* asc_desc */
  attr_val_p = &attrs[5].value;
  (*(tp_Integer.data_readval)) (buf_p, attr_val_p,
				tp_domain_resolve_default (DB_TYPE_INTEGER),
				-1, true);

  /* reserved */
  attr_val_p = &tmp_val;
  (*(tp_String.data_readval)) (buf_p, attr_val_p,
			       tp_domain_resolve_default (DB_TYPE_VARCHAR),
			       vars
			       [ORC_CLASS_CONSTRAINTS_ATTS_RESERVED_INDEX].
			       length, true);

  pr_clear_value (&tmp_val);
  free_and_init (vars);

  assert (error == NO_ERROR);
  return NO_ERROR;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "invalid error code");
    }

  return error;
}

/*
 * catcls_reorder_attributes_by_repr () -
 *   return:
 *   value(in):
 */
static int
catcls_reorder_attributes_by_repr (THREAD_ENTRY * thread_p,
				   OR_VALUE * value_p)
{
  OR_VALUE *attrs, *var_attrs;
  int n_attrs;
  DISK_ATTR *fixed_p, *variable_p;
  int n_fixed, n_variable;
  DISK_REPR *repr_p = NULL;
  REPR_ID repr_id;
  OID *class_oid_p;
  int i, j;
  int error = NO_ERROR;

  class_oid_p = &value_p->id.classoid;
  error =
    catalog_get_last_representation_id (thread_p, class_oid_p, &repr_id);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  else
    {
      repr_p = catalog_get_representation (thread_p, class_oid_p, repr_id);
      if (repr_p == NULL)
	{
	  error = er_errid ();
	  GOTO_EXIT_ON_ERROR;
	}
    }

  fixed_p = repr_p->fixed;
  n_fixed = repr_p->n_fixed;
  variable_p = repr_p->variable;
  n_variable = repr_p->n_variable;

  attrs = value_p->sub.value;
  n_attrs = n_fixed + n_variable;

  for (i = 0; i < n_fixed; i++)
    {
      for (j = i; j < n_attrs; j++)
	{
	  if (fixed_p[i].id == attrs[j].id.attrid)
	    {
	      if (i != j)
		{		/* need to exchange */
		  EXCHANGE_OR_VALUE (attrs[i], attrs[j]);
		}
	      break;
	    }
	}
    }

  var_attrs = &attrs[n_fixed];
  for (i = 0; i < n_variable; i++)
    {
      for (j = i; j < n_variable; j++)
	{
	  if (variable_p[i].id == var_attrs[j].id.attrid)
	    {
	      if (i != j)
		{		/* need to exchange */
		  EXCHANGE_OR_VALUE (var_attrs[i], var_attrs[j]);
		}
	      break;
	    }
	}
    }
#if !defined(NDEBUG)
  {
    for (i = 0; i < n_attrs; i++)
      {
	if (db_value_is_null (&attrs[i].value))
	  {
	    continue;
	  }

	if (i < n_fixed)
	  {
	    assert (!pr_is_variable_type (DB_VALUE_TYPE (&attrs[i].value)));
	  }
	else
	  {
	    assert (pr_is_variable_type (DB_VALUE_TYPE (&attrs[i].value)));
	  }
      }
  }
#endif
  catalog_free_representation (repr_p);

  assert (error == NO_ERROR);
  return NO_ERROR;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "invalid error code");
    }

  if (repr_p)
    {
      catalog_free_representation (repr_p);
    }

  return error;
}

/*
 * catcls_expand_or_value_by_repr () -
 *   return:
 *   value(in):
 *   class_oid(in):
 *   rep(in):
 */
static int
catcls_expand_or_value_by_repr (OR_VALUE * value_p, OID * class_oid_p,
				DISK_REPR * repr_p)
{
  OR_VALUE *attrs, *var_attrs;
  int n_attrs;
  DISK_ATTR *fixed_p, *variable_p;
  int n_fixed, n_variable;
  int i;
  int error = NO_ERROR;

  fixed_p = repr_p->fixed;
  n_fixed = repr_p->n_fixed;
  variable_p = repr_p->variable;
  n_variable = repr_p->n_variable;

  n_attrs = n_fixed + n_variable;
  attrs = catcls_allocate_or_value (n_attrs);
  if (attrs == NULL)
    {
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  value_p->sub.value = attrs;
  value_p->sub.count = n_attrs;

  COPY_OID (&value_p->id.classoid, class_oid_p);

  for (i = 0; i < n_fixed; i++)
    {
      attrs[i].id.attrid = fixed_p[i].id;
      error = db_value_domain_init (&attrs[i].value, fixed_p[i].type,
				    DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
      if (error != NO_ERROR)
	{
	  return error;
	}
    }

  var_attrs = &attrs[n_fixed];
  for (i = 0; i < n_variable; i++)
    {
      var_attrs[i].id.attrid = variable_p[i].id;
      error = db_value_domain_init (&var_attrs[i].value, variable_p[i].type,
				    DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
      if (error != NO_ERROR)
	{
	  return error;
	}
    }

  return NO_ERROR;
}

/*
 * catcls_expand_or_value_by_subset () -
 *   return:
 *   value(in):
 */
static void
catcls_expand_or_value_by_subset (THREAD_ENTRY * thread_p, OR_VALUE * value_p)
{
  DB_SET *set_p;
  int size, i;
  DB_VALUE element;
  OID *oid_p, class_oid;
  OR_VALUE *subset_p;

  if (pr_is_set_type (DB_VALUE_TYPE (&value_p->value)))
    {
      set_p = DB_PULL_SET (&value_p->value);
      size = set_size (set_p);
      if (size > 0)
	{
	  set_get_element (set_p, 0, &element);
	  if (DB_VALUE_TYPE (&element) == DB_TYPE_OID)
	    {
	      oid_p = DB_PULL_OID (&element);
	      (void) heap_get_class_oid (thread_p, &class_oid, oid_p);

	      if (!OID_EQ (&class_oid, &ct_Class.classoid))
		{
		  subset_p = catcls_allocate_or_value (size);
		  if (subset_p != NULL)
		    {
		      value_p->sub.value = subset_p;
		      value_p->sub.count = size;

		      for (i = 0; i < size; i++)
			{
			  COPY_OID (&((subset_p[i]).id.classoid), &class_oid);
			}
		    }
		}
	    }
	}
    }
}

/*
 * catcls_put_or_value_into_buffer () -
 *   return:
 *   value(in):
 *   buf(in):
 *   class_oid(in):
 *   rep(in):
 */
static int
catcls_put_or_value_into_buffer (OR_VALUE * value_p,
				 OR_BUF * buf_p, UNUSED_ARG OID * class_oid_p,
				 DISK_REPR * repr_p)
{
  OR_VALUE *attrs, *var_attrs;
//  int n_attrs;
  DISK_ATTR *fixed_p, *variable_p;
  int n_fixed, n_variable;
  DB_TYPE data_type;
  unsigned int repr_id_bits;
  char *bound_bits = NULL;
  int bound_size;
  char *offset_p, *start_p;
  int i, pad, offset;
  int error = NO_ERROR;

  fixed_p = repr_p->fixed;
  n_fixed = repr_p->n_fixed;
  variable_p = repr_p->variable;
  n_variable = repr_p->n_variable;

  attrs = value_p->sub.value;
//  n_attrs = n_fixed + n_variable;

  bound_size = OR_BOUND_BIT_BYTES (n_fixed);
  bound_bits = (char *) malloc (bound_size);
  if (bound_bits == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, bound_size);
      GOTO_EXIT_ON_ERROR;
    }
  memset (bound_bits, 0, bound_size);

  /* header */

  repr_id_bits = repr_p->id;
  if (n_fixed)
    {
      repr_id_bits |= OR_BOUND_BIT_FLAG;
    }

  OR_SET_VAR_OFFSET_SIZE (repr_id_bits, BIG_VAR_OFFSET_SIZE);	/* 4byte */
  or_put_int (buf_p, repr_id_bits);

  /* catalog's shard groupid */
  or_put_int (buf_p, GLOBAL_GROUPID);

  /* offset table */
  offset_p = buf_p->ptr;
  or_advance (buf_p, OR_VAR_TABLE_SIZE (n_variable));

  /* fixed */
  start_p = buf_p->ptr;
  for (i = 0; i < n_fixed; i++)
    {
      data_type = fixed_p[i].type;

      if (DB_IS_NULL (&attrs[i].value))
	{
	  or_advance (buf_p, tp_Type_id_map[data_type]->disksize);
	  OR_CLEAR_BOUND_BIT (bound_bits, i);
	}
      else
	{
	  (*(tp_Type_id_map[data_type]->data_writeval)) (buf_p,
							 &attrs[i].value);
	  OR_ENABLE_BOUND_BIT (bound_bits, i);
	}
    }

  pad = (int) (buf_p->ptr - start_p);
  if (pad < repr_p->fixed_length)
    {
      or_pad (buf_p, repr_p->fixed_length - pad);
    }
  else if (pad > repr_p->fixed_length)
    {
      error = ER_SM_CORRUPTED;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      GOTO_EXIT_ON_ERROR;
    }

  /* bound bits */
  if (n_fixed)
    {
      or_put_data (buf_p, bound_bits, bound_size);
    }

  /* variable */
  var_attrs = &attrs[n_fixed];
  for (i = 0; i < n_variable; i++)
    {
      offset = CAST_BUFLEN (buf_p->ptr - buf_p->buffer);

      data_type = variable_p[i].type;
      (*(tp_Type_id_map[data_type]->data_writeval)) (buf_p,
						     &var_attrs[i].value);

      OR_PUT_OFFSET (offset_p, offset);
      offset_p += BIG_VAR_OFFSET_SIZE;
    }

  /* put last offset */
  offset = CAST_BUFLEN (buf_p->ptr - buf_p->buffer);
  OR_PUT_OFFSET (offset_p, offset);

  if (bound_bits)
    {
      free_and_init (bound_bits);
    }

  assert (error == NO_ERROR);
  return NO_ERROR;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "invalid error code");
    }

  if (bound_bits)
    {
      free_and_init (bound_bits);
    }

  return error;
}

/*
 * catcls_get_or_value_from_buffer () -
 *   return:
 *   buf(in):
 *   value(in):
 *   rep(in):
 */
static int
catcls_get_or_value_from_buffer (THREAD_ENTRY * thread_p, OR_BUF * buf_p,
				 OR_VALUE * value_p, DISK_REPR * repr_p)
{
  OR_VALUE *attrs, *var_attrs;
//  int n_attrs;
  DISK_ATTR *fixed_p, *variable_p;
  int n_fixed, n_variable;
  DB_TYPE data_type;
  OR_VARINFO *vars = NULL;
  unsigned int repr_id_bits;
  char *bound_bits = NULL;
  int bound_bits_flag = false;
  char *start_p;
  int i, pad, size, rc;
  int error = NO_ERROR;

  fixed_p = repr_p->fixed;
  n_fixed = repr_p->n_fixed;
  variable_p = repr_p->variable;
  n_variable = repr_p->n_variable;

  attrs = value_p->sub.value;
//  n_attrs = n_fixed + n_variable;

  /* header */
  assert (OR_GET_OFFSET_SIZE (buf_p->ptr) == BIG_VAR_OFFSET_SIZE);

  repr_id_bits = or_get_int (buf_p, &rc);	/* repid */
  bound_bits_flag = repr_id_bits & OR_BOUND_BIT_FLAG;

  or_advance (buf_p, OR_INT_SIZE);	/* group id */

  if (bound_bits_flag)
    {
      size = OR_BOUND_BIT_BYTES (n_fixed);
      bound_bits = (char *) malloc (size);
      if (bound_bits == NULL)
	{
	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, size);
	  GOTO_EXIT_ON_ERROR;
	}
      memset (bound_bits, 0, size);
    }

  /* offset table */
  vars = or_get_var_table (buf_p, n_variable, catcls_unpack_allocator);
  if (vars == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
	      n_variable * sizeof (OR_VARINFO));
      GOTO_EXIT_ON_ERROR;
    }

  /* fixed */
  start_p = buf_p->ptr;

  /* read bound bits before accessing fixed attributes */
  buf_p->ptr += repr_p->fixed_length;
  if (bound_bits_flag)
    {
      or_get_data (buf_p, bound_bits, OR_BOUND_BIT_BYTES (n_fixed));
    }

  buf_p->ptr = start_p;
  for (i = 0; i < n_fixed; i++)
    {
      data_type = fixed_p[i].type;

      if (bound_bits_flag && OR_GET_BOUND_BIT (bound_bits, i))
	{
	  (*(tp_Type_id_map[data_type]->data_readval)) (buf_p,
							&attrs[i].value,
							tp_domain_resolve_default
							(data_type), -1,
							true);
	}
      else
	{
	  db_value_put_null (&attrs[i].value);
	  or_advance (buf_p, tp_Type_id_map[data_type]->disksize);
	}
    }

  pad = (int) (buf_p->ptr - start_p);
  if (pad < repr_p->fixed_length)
    {
      or_advance (buf_p, repr_p->fixed_length - pad);
    }
  else if (pad > repr_p->fixed_length)
    {
      error = ER_SM_CORRUPTED;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      GOTO_EXIT_ON_ERROR;
    }

  /* bound bits */
  if (bound_bits_flag)
    {
      or_advance (buf_p, OR_BOUND_BIT_BYTES (n_fixed));
    }

  /* variable */
  var_attrs = &attrs[n_fixed];
  for (i = 0; i < n_variable; i++)
    {
      data_type = variable_p[i].type;
      (*(tp_Type_id_map[data_type]->data_readval)) (buf_p,
						    &var_attrs[i].value,
						    tp_domain_resolve_default
						    (data_type),
						    vars[i].length, true);
      catcls_expand_or_value_by_subset (thread_p, &var_attrs[i]);
    }

  if (vars)
    {
      free_and_init (vars);
    }

  if (bound_bits)
    {
      free_and_init (bound_bits);
    }

  assert (error == NO_ERROR);
  return NO_ERROR;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "invalid error code");
    }

  if (vars)
    {
      free_and_init (vars);
    }

  if (bound_bits)
    {
      free_and_init (bound_bits);
    }

  return error;
}

/*
 * catcls_put_or_value_into_record () -
 *   return:
 *   value(in):
 *   record(in):
 *   class_oid(in):
 */
static int
catcls_put_or_value_into_record (THREAD_ENTRY * thread_p,
				 OR_VALUE * value_p, RECDES * record_p,
				 OID * class_oid_p)
{
  OR_BUF *buf_p, repr_buffer;
  DISK_REPR *repr_p = NULL;
  REPR_ID repr_id;
  int error = NO_ERROR;

  error = catalog_get_last_representation_id (thread_p, class_oid_p,
					      &repr_id);
  if (error != NO_ERROR)
    {
      return error;
    }
  else
    {
      repr_p = catalog_get_representation (thread_p, class_oid_p, repr_id);
      if (repr_p == NULL)
	{
	  error = er_errid ();
	  return error;
	}
    }

  buf_p = &repr_buffer;
  or_init (buf_p, record_p->data, record_p->length);

  error = catcls_put_or_value_into_buffer (value_p, buf_p,
					   class_oid_p, repr_p);
  if (error != NO_ERROR)
    {
      catalog_free_representation (repr_p);
      return error;
    }

  record_p->length = CAST_BUFLEN (buf_p->ptr - buf_p->buffer);
  catalog_free_representation (repr_p);

  return NO_ERROR;
}

/*
 * catcls_get_or_value_from_class_record () -
 *   return:
 *   record(in):
 */
static OR_VALUE *
catcls_get_or_value_from_class_record (THREAD_ENTRY *
				       thread_p, RECDES * record_p)
{
  OR_VALUE *value_p = NULL;
  OR_BUF *buf_p, repr_buffer;

  value_p = catcls_allocate_or_value (1);
  if (value_p == NULL)
    {
      return NULL;
    }

  assert (OR_GET_OFFSET_SIZE (record_p->data) == BIG_VAR_OFFSET_SIZE);

  buf_p = &repr_buffer;
  or_init (buf_p, record_p->data, record_p->length);

  or_advance (buf_p, OR_HEADER_SIZE);
  if (catcls_get_or_value_from_class (thread_p, buf_p, value_p) != NO_ERROR)
    {
      catcls_free_or_value (value_p);
      return NULL;
    }

  return value_p;
}

/*
 * catcls_get_or_value_from_record () -
 *   return:
 *   record(in):
 *   class_oid(in):
 */
static OR_VALUE *
catcls_get_or_value_from_record (THREAD_ENTRY * thread_p,
				 RECDES * record_p, OID * class_oid_p)
{
  OR_VALUE *value_p = NULL;
  OR_BUF *buf_p, repr_buffer;
  REPR_ID repr_id;
  DISK_REPR *repr_p = NULL;
  int error;

  error =
    catalog_get_last_representation_id (thread_p, class_oid_p, &repr_id);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  repr_p = catalog_get_representation (thread_p, class_oid_p, repr_id);
  if (repr_p == NULL)
    {
      error = er_errid ();
      GOTO_EXIT_ON_ERROR;
    }

  value_p = catcls_allocate_or_value (1);
  if (value_p == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (catcls_expand_or_value_by_repr (value_p, class_oid_p, repr_p) !=
      NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  buf_p = &repr_buffer;
  or_init (buf_p, record_p->data, record_p->length);

  if (catcls_get_or_value_from_buffer (thread_p, buf_p, value_p, repr_p) !=
      NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  catalog_free_representation (repr_p);

  assert (error == NO_ERROR);
  return value_p;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "invalid error code");
    }

  if (value_p)
    {
      catcls_free_or_value (value_p);
    }

  if (repr_p)
    {
      catalog_free_representation (repr_p);
    }

  return NULL;
}

/*
 * catcls_insert_subset () -
 *   return:
 *   value(in):
 *   root_oid(in):
 */
static int
catcls_insert_subset (THREAD_ENTRY * thread_p, OR_VALUE * value_p,
		      OID * root_oid_p)
{
  OR_VALUE *subset_p;
  int n_subset;
  OID *class_oid_p, oid;
  CLS_INFO *cls_info_p = NULL;
  HFID *hfid_p;
  DB_SET *oid_set_p = NULL;
  DB_VALUE oid_val;
  int i;
  HEAP_SCANCACHE scan;
  bool is_scan_inited = false;
  int error = NO_ERROR;

  subset_p = value_p->sub.value;
  n_subset = value_p->sub.count;

  if (n_subset == 0)
    {
      return NO_ERROR;
    }

  oid_set_p = set_create_sequence (n_subset);
  if (oid_set_p == NULL)
    {
      error = er_errid ();
      GOTO_EXIT_ON_ERROR;
    }

  class_oid_p = &subset_p[0].id.classoid;
  cls_info_p = catalog_get_class_info (thread_p, class_oid_p);
  if (cls_info_p == NULL)
    {
      error = er_errid ();
      GOTO_EXIT_ON_ERROR;
    }

  hfid_p = &cls_info_p->hfid;
  error =
    heap_scancache_start_modify (thread_p, &scan, hfid_p, 0, class_oid_p);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  is_scan_inited = true;

  for (i = 0; i < n_subset; i++)
    {
      error = catcls_insert_instance (thread_p, &subset_p[i], &oid,
				      root_oid_p, class_oid_p, hfid_p, &scan);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      db_push_oid (&oid_val, &oid);
      error = set_put_element (oid_set_p, i, &oid_val);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  db_make_sequence (&value_p->value, oid_set_p);

  heap_scancache_end_modify (thread_p, &scan);
  catalog_free_class_info (cls_info_p);

  assert (error == NO_ERROR);
  return NO_ERROR;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "invalid error code");
    }

  if (oid_set_p)
    {
      set_free (oid_set_p);
    }

  if (is_scan_inited)
    {
      heap_scancache_end_modify (thread_p, &scan);
    }

  if (cls_info_p)
    {
      catalog_free_class_info (cls_info_p);
    }

  return error;
}

/*
 * catcls_delete_subset () -
 *   return:
 *   value(in):
 */
static int
catcls_delete_subset (THREAD_ENTRY * thread_p, OR_VALUE * value_p)
{
  OR_VALUE *subset_p;
  int n_subset;
  OID *class_oid_p, *oid_p;
  CLS_INFO *cls_info_p = NULL;
  HFID *hfid_p;
  DB_SET *oid_set_p;
  DB_VALUE oid_val;
  int i;
  HEAP_SCANCACHE scan;
  bool is_scan_inited = false;
  int error = NO_ERROR;

  subset_p = value_p->sub.value;
  n_subset = value_p->sub.count;

  if (n_subset == 0)
    {
      return NO_ERROR;
    }

  oid_set_p = DB_GET_SET (&value_p->value);
  class_oid_p = &subset_p[0].id.classoid;

  cls_info_p = catalog_get_class_info (thread_p, class_oid_p);
  if (cls_info_p == NULL)
    {
      error = er_errid ();
      GOTO_EXIT_ON_ERROR;
    }

  hfid_p = &cls_info_p->hfid;
  error =
    heap_scancache_start_modify (thread_p, &scan, hfid_p, 0, class_oid_p);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  is_scan_inited = true;

  for (i = 0; i < n_subset; i++)
    {
      error = set_get_element (oid_set_p, i, &oid_val);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      oid_p = DB_GET_OID (&oid_val);
      error =
	catcls_delete_instance (thread_p, oid_p, class_oid_p, hfid_p, &scan);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  heap_scancache_end_modify (thread_p, &scan);
  catalog_free_class_info (cls_info_p);

  assert (error == NO_ERROR);
  return NO_ERROR;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "invalid error code");
    }

  if (is_scan_inited)
    {
      heap_scancache_end_modify (thread_p, &scan);
    }

  if (cls_info_p)
    {
      catalog_free_class_info (cls_info_p);
    }

  return error;
}

/*
 * catcls_update_subset () -
 *   return:
 *   value(in):
 *   old_value(in):
 *   uflag(in):
 */
static int
catcls_update_subset (THREAD_ENTRY * thread_p, OR_VALUE * value_p,
		      OR_VALUE * old_value_p, int *update_flag_p)
{
  OR_VALUE *subset_p, *old_subset_p;
  int n_subset, n_old_subset;
  int n_min_subset;
  OID *class_oid_p;
  CLS_INFO *cls_info_p = NULL;
  HFID *hfid_p;
  OID *oid_p, tmp_oid;
  DB_SET *old_oid_set_p = NULL;
  DB_VALUE oid_val;
  int i;
  HEAP_SCANCACHE scan;
  bool is_scan_inited = false;
  int error = NO_ERROR;

  if ((value_p->sub.count > 0) &&
      ((old_value_p->sub.count < 0) || DB_IS_NULL (&old_value_p->value)))
    {
      old_oid_set_p = set_create_sequence (0);
      db_make_sequence (&old_value_p->value, old_oid_set_p);
      old_value_p->sub.count = 0;
    }

  subset_p = value_p->sub.value;
  n_subset = value_p->sub.count;

  old_subset_p = old_value_p->sub.value;
  n_old_subset = old_value_p->sub.count;

  n_min_subset = (n_subset > n_old_subset) ? n_old_subset : n_subset;

  if (subset_p != NULL)
    {
      class_oid_p = &subset_p[0].id.classoid;
    }
  else if (old_subset_p != NULL)
    {
      class_oid_p = &old_subset_p[0].id.classoid;
    }
  else
    {
      return NO_ERROR;
    }

  old_oid_set_p = DB_PULL_SET (&old_value_p->value);
  cls_info_p = catalog_get_class_info (thread_p, class_oid_p);
  if (cls_info_p == NULL)
    {
      error = er_errid ();
      GOTO_EXIT_ON_ERROR;
    }

  hfid_p = &cls_info_p->hfid;
  error =
    heap_scancache_start_modify (thread_p, &scan, hfid_p, 0, class_oid_p);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  is_scan_inited = true;

  /* update components */
  for (i = 0; i < n_min_subset; i++)
    {
      error = set_get_element (old_oid_set_p, i, &oid_val);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      if (DB_VALUE_TYPE (&oid_val) != DB_TYPE_OID)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      oid_p = DB_PULL_OID (&oid_val);
      error = catcls_update_instance (thread_p, &subset_p[i], oid_p,
				      class_oid_p, hfid_p, &scan);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  /* drop components */
  if (n_old_subset > n_subset)
    {
      for (i = n_old_subset - 1; i >= n_min_subset; i--)
	{
	  error = set_get_element (old_oid_set_p, i, &oid_val);
	  if (error != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

	  if (DB_VALUE_TYPE (&oid_val) != DB_TYPE_OID)
	    {
	      error = ER_GENERIC_ERROR;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
		      1, "Invalid type");
	      GOTO_EXIT_ON_ERROR;
	    }

	  oid_p = DB_PULL_OID (&oid_val);
	  error = catcls_delete_instance (thread_p, oid_p, class_oid_p,
					  hfid_p, &scan);
	  if (error != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

	  error = set_drop_seq_element (old_oid_set_p, i);
	  if (error != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}

      if (set_size (old_oid_set_p) == 0)
	{
	  pr_clear_value (&old_value_p->value);
	}

      *update_flag_p = true;
    }
  /* add components */
  else if (n_old_subset < n_subset)
    {
      OID root_oid = NULL_OID_INITIALIZER;

      for (i = n_min_subset, oid_p = &tmp_oid; i < n_subset; i++)
	{
	  error = catcls_insert_instance (thread_p, &subset_p[i], oid_p,
					  &root_oid, class_oid_p, hfid_p,
					  &scan);
	  if (error != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

	  db_push_oid (&oid_val, oid_p);
	  error = set_add_element (old_oid_set_p, &oid_val);
	  if (error != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}
      *update_flag_p = true;
    }

  heap_scancache_end_modify (thread_p, &scan);
  catalog_free_class_info (cls_info_p);

  assert (error == NO_ERROR);
  return NO_ERROR;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "invalid error code");
    }

  if (is_scan_inited)
    {
      heap_scancache_end_modify (thread_p, &scan);
    }

  if (cls_info_p)
    {
      catalog_free_class_info (cls_info_p);
    }

  return error;
}

/*
 * catcls_insert_instance () -
 *   return:
 *   value(in):
 *   oid(in):
 *   root_oid(in):
 *   class_oid(in):
 *   hfid(in):
 *   scan(in):
 */
static int
catcls_insert_instance (THREAD_ENTRY * thread_p, OR_VALUE * value_p,
			OID * oid_p, OID * root_oid_p, OID * class_oid_p,
			HFID * hfid_p, HEAP_SCANCACHE * scan_p)
{
  RECDES record = RECDES_INITIALIZER;
  OR_VALUE *attrs;
  OR_VALUE *subset_p, *attr_p;
  bool old;
  int i, j, k;
  int error = NO_ERROR;

  record.data = NULL;

  error = heap_assign_address_with_class_oid (thread_p, hfid_p, class_oid_p,
					      oid_p, 0);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (OID_ISNULL (root_oid_p))
    {
      COPY_OID (root_oid_p, oid_p);
    }

  for (attrs = value_p->sub.value, i = 0; i < value_p->sub.count; i++)
    {
      if (IS_SUBSET (attrs[i]))
	{
	  /* set backward oid */
	  for (subset_p = attrs[i].sub.value,
	       j = 0; j < attrs[i].sub.count; j++)
	    {
	      /* assume that the attribute values of xxx are ordered by
	         { class_of, xxx_name, xxx_type, from_xxx_name, ... } */

	      attr_p = subset_p[j].sub.value;
	      db_push_oid (&attr_p[0].value, oid_p);

	      if (OID_EQ (class_oid_p, &ct_Class.classoid))
		{
		  /* if root node, eliminate self references */
		  for (k = 1; k < subset_p[j].sub.count; k++)
		    {
		      if (DB_VALUE_TYPE (&attr_p[k].value) == DB_TYPE_OID)
			{
			  if (OID_EQ (oid_p, DB_PULL_OID (&attr_p[k].value)))
			    {
			      db_value_put_null (&attr_p[k].value);
			    }
			}
		    }
		}
	    }

	  error = catcls_insert_subset (thread_p, &attrs[i], root_oid_p);
	  if (error != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}
      else if (DB_VALUE_DOMAIN_TYPE (&attrs[i].value) == DB_TYPE_VARIABLE)
	{
	  /* set a self referenced oid */
	  db_push_oid (&attrs[i].value, root_oid_p);
	}
    }

  error = catcls_reorder_attributes_by_repr (thread_p, value_p);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  record.length = catcls_guess_record_length (value_p);
  record.area_size = record.length;
  record.type = REC_HOME;
  record.data = (char *) malloc (record.length);

  if (record.data == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, record.length);
      GOTO_EXIT_ON_ERROR;
    }

  error = catcls_put_or_value_into_record (thread_p, value_p, &record,
					   class_oid_p);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* set groupid
   */
  assert (oid_p->groupid == NULL_GROUPID);
  oid_p->groupid = or_grp_id (&record);
  assert (oid_p->groupid == GLOBAL_GROUPID);

  /* for replication */
  error = locator_add_or_remove_index (thread_p, &record, oid_p, class_oid_p,
				       true, false, false, hfid_p);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (heap_update (thread_p, hfid_p, class_oid_p, oid_p, &record,
		   &old, scan_p) == NULL)
    {
      error = er_errid ();
      GOTO_EXIT_ON_ERROR;
    }

  free_and_init (record.data);

  assert (error == NO_ERROR);
  return NO_ERROR;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "invalid error code");
    }

  if (record.data)
    {
      free_and_init (record.data);
    }

  return error;
}

/*
 * catcls_delete_instance () -
 *   return:
 *   oid(in):
 *   class_oid(in):
 *   hfid(in):
 *   scan(in):
 */
static int
catcls_delete_instance (THREAD_ENTRY * thread_p, OID * oid_p,
			OID * class_oid_p, HFID * hfid_p,
			HEAP_SCANCACHE * scan_p)
{
  RECDES record = RECDES_INITIALIZER;
  OR_VALUE *value_p = NULL;
  OR_VALUE *attrs;
  int i;
  int error = NO_ERROR;

  record.data = NULL;

  if (heap_get (thread_p, oid_p, &record, scan_p, COPY) != S_SUCCESS)
    {
      error = er_errid ();
      GOTO_EXIT_ON_ERROR;
    }

  value_p = catcls_get_or_value_from_record (thread_p, &record, class_oid_p);
  if (value_p == NULL)
    {
      error = er_errid ();
      GOTO_EXIT_ON_ERROR;
    }

  for (attrs = value_p->sub.value, i = 0; i < value_p->sub.count; i++)
    {
      if (IS_SUBSET (attrs[i]))
	{
	  error = catcls_delete_subset (thread_p, &attrs[i]);
	  if (error != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}
    }

  /* for replication */
  error = locator_add_or_remove_index (thread_p, &record, oid_p, class_oid_p,
				       false, false, false, hfid_p);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (heap_delete (thread_p, hfid_p, oid_p, scan_p, NULL) == NULL)
    {
      error = er_errid ();
      GOTO_EXIT_ON_ERROR;
    }

  catcls_free_or_value (value_p);

  assert (error == NO_ERROR);
  return NO_ERROR;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "invalid error code");
    }

  if (value_p)
    {
      catcls_free_or_value (value_p);
    }

  return error;
}

/*
 * catcls_update_instance () -
 *   return:
 *   value(in):
 *   oid(in):
 *   class_oid(in):
 *   hfid(in):
 *   scan(in):
 */
static int
catcls_update_instance (THREAD_ENTRY * thread_p, OR_VALUE * value_p,
			OID * oid_p, OID * class_oid_p, HFID * hfid_p,
			HEAP_SCANCACHE * scan_p)
{
  int rc = DB_UNK;
  RECDES record = RECDES_INITIALIZER, old_record = RECDES_INITIALIZER;
  OR_VALUE *old_value_p = NULL;
  OR_VALUE *attrs, *old_attrs;
  OR_VALUE *subset_p, *attr_p;
  int uflag = false;
  bool old;
  int i, j, k;
  int error = NO_ERROR;

  record.data = NULL;
  old_record.data = NULL;

  if (heap_get (thread_p, oid_p, &old_record, scan_p, COPY) != S_SUCCESS)
    {
      error = er_errid ();
      GOTO_EXIT_ON_ERROR;
    }

  old_value_p = catcls_get_or_value_from_record (thread_p, &old_record,
						 class_oid_p);
  if (old_value_p == NULL)
    {
      error = er_errid ();
      GOTO_EXIT_ON_ERROR;
    }

  error = catcls_reorder_attributes_by_repr (thread_p, value_p);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* update old_value */
  for (attrs = value_p->sub.value, old_attrs = old_value_p->sub.value,
       i = 0; i < value_p->sub.count; i++)
    {
      if (IS_SUBSET (attrs[i]))
	{
	  /* set backward oid */
	  for (subset_p = attrs[i].sub.value,
	       j = 0; j < attrs[i].sub.count; j++)
	    {
	      /* assume that the attribute values of xxx are ordered by
	         { class_of, xxx_name, xxx_type, from_xxx_name, ... } */
	      attr_p = subset_p[j].sub.value;
	      db_push_oid (&attr_p[0].value, oid_p);

	      if (OID_EQ (class_oid_p, &ct_Class.classoid))
		{
		  /* if root node, eliminate self references */
		  for (k = 1; k < subset_p[j].sub.count; k++)
		    {
		      if (DB_VALUE_TYPE (&attr_p[k].value) == DB_TYPE_OID)
			{
			  if (OID_EQ (oid_p, DB_PULL_OID (&attr_p[k].value)))
			    {
			      db_value_put_null (&attr_p[k].value);
			    }
			}
		    }
		}
	    }

	  error = catcls_update_subset (thread_p, &attrs[i], &old_attrs[i],
					&uflag);
	  if (error != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}
      else
	{
	  rc = tp_value_compare (&old_attrs[i].value, &attrs[i].value,
				 1, 1, NULL);
	  assert (rc != DB_UNK);
	  if (rc != DB_EQ)
	    {
	      assert (DB_IS_NULL (&old_attrs[i].value)
		      || DB_IS_NULL (&attrs[i].value)
		      || (db_value_type (&old_attrs[i].value)
			  == db_value_type (&attrs[i].value)));

	      pr_clear_value (&old_attrs[i].value);
	      pr_clone_value (&attrs[i].value, &old_attrs[i].value);
	      uflag = true;
	    }
	}
    }

  if (uflag == true)
    {
      record.length = catcls_guess_record_length (old_value_p);
      record.area_size = record.length;
      record.type = REC_HOME;
      record.data = (char *) malloc (record.length);

      if (record.data == NULL)
	{
	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, record.length);
	  GOTO_EXIT_ON_ERROR;
	}

      error = catcls_put_or_value_into_record (thread_p, old_value_p,
					       &record, class_oid_p);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      /* give up setting updated attr info */
      error = locator_update_index (thread_p, &record, &old_record, NULL, 0,
				    oid_p, class_oid_p, false, false);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      if (heap_update (thread_p, hfid_p, class_oid_p, oid_p, &record, &old,
		       scan_p) == NULL)
	{
	  error = er_errid ();
	  GOTO_EXIT_ON_ERROR;
	}

      free_and_init (record.data);
    }

  catcls_free_or_value (old_value_p);

  assert (error == NO_ERROR);
  return NO_ERROR;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "invalid error code");
    }

  if (record.data)
    {
      free_and_init (record.data);
    }

  if (old_value_p)
    {
      catcls_free_or_value (old_value_p);
    }

  return error;
}

/*
 * catcls_insert_catalog_classes () -
 *   return:
 *   record(in):
 */
int
catcls_insert_catalog_classes (THREAD_ENTRY * thread_p, RECDES * record_p)
{
  OR_VALUE *value_p = NULL;
  OID oid, *class_oid_p;
  OID root_oid = NULL_OID_INITIALIZER;
  CLS_INFO *cls_info_p = NULL;
  HFID *hfid_p;
  HEAP_SCANCACHE scan;
  bool is_scan_inited = false;
  int error = NO_ERROR;

  value_p = catcls_get_or_value_from_class_record (thread_p, record_p);
  if (value_p == NULL)
    {
      error = er_errid ();
      GOTO_EXIT_ON_ERROR;
    }

  class_oid_p = &ct_Class.classoid;
  cls_info_p = catalog_get_class_info (thread_p, class_oid_p);
  if (cls_info_p == NULL)
    {
      error = er_errid ();
      GOTO_EXIT_ON_ERROR;
    }

  hfid_p = &cls_info_p->hfid;
  error =
    heap_scancache_start_modify (thread_p, &scan, hfid_p, 0, class_oid_p);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  is_scan_inited = true;

  error =
    catcls_insert_instance (thread_p, value_p, &oid, &root_oid, class_oid_p,
			    hfid_p, &scan);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  heap_scancache_end_modify (thread_p, &scan);
  catalog_free_class_info (cls_info_p);
  catcls_free_or_value (value_p);

  assert (error == NO_ERROR);
  return NO_ERROR;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "invalid error code");
    }

  if (is_scan_inited)
    {
      heap_scancache_end_modify (thread_p, &scan);
    }

  if (cls_info_p)
    {
      catalog_free_class_info (cls_info_p);
    }

  if (value_p)
    {
      catcls_free_or_value (value_p);
    }

  return ER_FAILED;
}

/*
 * catcls_delete_catalog_classes () -
 *   return:
 *   name(in):
 *   class_oid(in):
 */
int
catcls_delete_catalog_classes (THREAD_ENTRY * thread_p, const char *name_p,
			       OID * class_oid_p)
{
  OID oid, *ct_class_oid_p;
  CLS_INFO *cls_info_p = NULL;
  HFID *hfid_p;
  HEAP_SCANCACHE scan;
  bool is_scan_inited = false;
  int error = NO_ERROR;

  error = catcls_find_oid_by_class_name (thread_p, name_p, &oid);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  ct_class_oid_p = &ct_Class.classoid;
  cls_info_p = catalog_get_class_info (thread_p, ct_class_oid_p);
  if (cls_info_p == NULL)
    {
      error = er_errid ();
      GOTO_EXIT_ON_ERROR;
    }

  hfid_p = &cls_info_p->hfid;
  error = heap_scancache_start_modify (thread_p, &scan, hfid_p, 0,
				       ct_class_oid_p);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  is_scan_inited = true;

  error = catcls_delete_instance (thread_p, &oid, ct_class_oid_p,
				  hfid_p, &scan);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = csect_enter (thread_p, CSECT_CT_OID_TABLE, INF_WAIT);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = catcls_remove_entry (thread_p, class_oid_p);
  if (error != NO_ERROR)
    {
      csect_exit (CSECT_CT_OID_TABLE);
      GOTO_EXIT_ON_ERROR;
    }

  csect_exit (CSECT_CT_OID_TABLE);

  heap_scancache_end_modify (thread_p, &scan);
  catalog_free_class_info (cls_info_p);

  assert (error == NO_ERROR);
  return NO_ERROR;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "Invalid error code");
    }

  if (is_scan_inited)
    {
      heap_scancache_end_modify (thread_p, &scan);
    }

  if (cls_info_p)
    {
      catalog_free_class_info (cls_info_p);
    }

  return error;
}

/*
 * catcls_update_catalog_classes () -
 *   return:
 *   name(in):
 *   record(in):
 */
int
catcls_update_catalog_classes (THREAD_ENTRY * thread_p, const char *name_p,
			       RECDES * record_p)
{
  OR_VALUE *value_p = NULL;
  OID oid, *class_oid_p;
  CLS_INFO *cls_info_p = NULL;
  HFID *hfid_p;
  HEAP_SCANCACHE scan;
  bool is_scan_inited = false;
  int error = NO_ERROR;

  error = catcls_find_oid_by_class_name (thread_p, name_p, &oid);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (OID_ISNULL (&oid))
    {
      return (catcls_insert_catalog_classes (thread_p, record_p));
    }

  value_p = catcls_get_or_value_from_class_record (thread_p, record_p);
  if (value_p == NULL)
    {
      error = er_errid ();
      GOTO_EXIT_ON_ERROR;
    }

  class_oid_p = &ct_Class.classoid;
  cls_info_p = catalog_get_class_info (thread_p, class_oid_p);
  if (cls_info_p == NULL)
    {
      error = er_errid ();
      GOTO_EXIT_ON_ERROR;
    }

  hfid_p = &cls_info_p->hfid;
  error =
    heap_scancache_start_modify (thread_p, &scan, hfid_p, 0, class_oid_p);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  is_scan_inited = true;

  error =
    catcls_update_instance (thread_p, value_p, &oid, class_oid_p, hfid_p,
			    &scan);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  heap_scancache_end_modify (thread_p, &scan);
  catalog_free_class_info (cls_info_p);
  catcls_free_or_value (value_p);

  assert (error == NO_ERROR);
  return NO_ERROR;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "invalid error code");
    }

  if (is_scan_inited)
    {
      heap_scancache_end_modify (thread_p, &scan);
    }

  if (cls_info_p)
    {
      catalog_free_class_info (cls_info_p);
    }

  if (value_p)
    {
      catcls_free_or_value (value_p);
    }

  return ER_FAILED;
}

/*
 * catcls_compile_catalog_classes () -
 *   return:
 *   void(in):
 */
int
catcls_compile_catalog_classes (THREAD_ENTRY * thread_p)
{
  RECDES class_record = RECDES_INITIALIZER;
  OID *class_oid_p, tmp_oid;
  const char *class_name_p;
  const char *attr_name_p;
  CT_ATTR *atts;
  int n_atts;
  int c, a, i;
  HEAP_SCANCACHE scan;

  /* check if an old version database */
  if (catcls_find_class_oid_by_class_name (thread_p, CT_TABLE_NAME, &tmp_oid)
      != NO_ERROR)
    {
      return ER_FAILED;
    }
  else if (OID_ISNULL (&tmp_oid))
    {
      /* no catalog classes */
      return NO_ERROR;
    }

  /* fill classoid and attribute ids for each meta catalog classes */
  for (c = 0; ct_Classes[c] != NULL; c++)
    {
      class_name_p = ct_Classes[c]->name;
      class_oid_p = &ct_Classes[c]->classoid;

      if (catcls_find_class_oid_by_class_name (thread_p, class_name_p,
					       class_oid_p) != NO_ERROR)
	{
	  return ER_FAILED;
	}

      atts = ct_Classes[c]->atts;
      n_atts = ct_Classes[c]->n_atts;

      if (heap_scancache_quick_start (&scan) != NO_ERROR)
	{
	  return ER_FAILED;
	}
      if (heap_get (thread_p, class_oid_p, &class_record, &scan, PEEK) !=
	  S_SUCCESS)
	{
	  (void) heap_scancache_end (thread_p, &scan);
	  return ER_FAILED;
	}

      for (i = 0; i < n_atts; i++)
	{
	  attr_name_p = or_get_attrname (&class_record, i);
	  if (attr_name_p == NULL)
	    {
	      (void) heap_scancache_end (thread_p, &scan);
	      return ER_FAILED;
	    }

	  for (a = 0; a < n_atts; a++)
	    {
	      if (strcmp (atts[a].name, attr_name_p) == 0)
		{
		  atts[a].id = i;
		  break;
		}
	    }
	}
      if (heap_scancache_end (thread_p, &scan) != NO_ERROR)
	{
	  return ER_FAILED;
	}
    }

  catcls_Enable = true;

  if (catcls_find_btid_of_class_name (thread_p, &catcls_Btid) != NO_ERROR)
    {
      assert (false);
      return ER_FAILED;
    }

  if (catcls_initialize_class_oid_to_oid_hash_table (thread_p,
						     CATCLS_OID_TABLE_SIZE) !=
      NO_ERROR)
    {
      assert (false);
      return ER_FAILED;
    }

  return NO_ERROR;
}


/*
 * catcls_get_db_collation () - get infomation on all collation in DB
 *				stored in the "_db_collation" system table
 *
 *   return: NO_ERROR, or error code
 *   thread_p(in)  : thread context
 *   db_collations(out): array of collation info
 *   coll_cnt(out): number of collations found in DB
 *
 *  Note : This function is called during server initialization, for this
 *	   reason, no locks are required on the class.
 */
int
catcls_get_db_collation (THREAD_ENTRY * thread_p,
			 LANG_COLL_COMPAT ** db_collations, int *coll_cnt)
{
  OID class_oid;
  OID inst_oid;
  HFID hfid;
  HEAP_CACHE_ATTRINFO attr_info;
  HEAP_SCANCACHE scan_cache;
  RECDES recdes = RECDES_INITIALIZER;
  const char *class_name = "db_collation";
  int i;
  int error = NO_ERROR;
  int att_id_cnt = 0;
  int max_coll_cnt;
  int coll_id_att_id = -1, coll_name_att_id = -1, charset_id_att_id = -1,
    checksum_att_id = -1;
  int alloc_size;
  bool attr_info_inited = false;
  bool scan_cache_inited = false;

  assert (db_collations != NULL);
  assert (coll_cnt != NULL);
  if (db_collations == NULL || coll_cnt == NULL)
    {
      return ER_FAILED;
    }

  OID_SET_NULL (&class_oid);
  OID_SET_NULL (&inst_oid);

  error = catcls_find_class_oid_by_class_name (thread_p, class_name,
					       &class_oid);
  if (error != NO_ERROR)
    {
      goto exit;
    }

  if (OID_ISNULL (&class_oid))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LC_UNKNOWN_CLASSNAME,
	      1, class_name);
      error = ER_LC_UNKNOWN_CLASSNAME;
      goto exit;
    }

  error = heap_attrinfo_start (thread_p, &class_oid, -1, NULL, &attr_info);
  if (error != NO_ERROR)
    {
      goto exit;
    }
  attr_info_inited = true;

  (void) heap_scancache_quick_start (&scan_cache);
  scan_cache_inited = true;

  if (heap_get (thread_p, &class_oid, &recdes, &scan_cache, PEEK) !=
      S_SUCCESS)
    {
      error = ER_FAILED;
      goto exit;
    }

  for (i = 0; i < attr_info.num_values; i++)
    {
      const char *rec_attr_name_p = or_get_attrname (&recdes, i);
      if (rec_attr_name_p == NULL)
	{
	  error = ER_FAILED;
	  goto exit;
	}

      if (strcmp (CT_DBCOLL_COLL_ID_COLUMN, rec_attr_name_p) == 0)
	{
	  coll_id_att_id = i;
	  att_id_cnt++;
	}
      else if (strcmp (CT_DBCOLL_COLL_NAME_COLUMN, rec_attr_name_p) == 0)
	{
	  coll_name_att_id = i;
	  att_id_cnt++;
	}
      else if (strcmp (CT_DBCOLL_CHARSET_ID_COLUMN, rec_attr_name_p) == 0)
	{
	  charset_id_att_id = i;
	  att_id_cnt++;
	}
      else if (strcmp (CT_DBCOLL_CHECKSUM_COLUMN, rec_attr_name_p) == 0)
	{
	  checksum_att_id = i;
	  att_id_cnt++;
	}
      if (att_id_cnt >= 4)
	{
	  break;
	}
    }

  if (att_id_cnt != 4)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
      error = ER_FAILED;
      goto exit;
    }

  (void) heap_scancache_end (thread_p, &scan_cache);
  scan_cache_inited = false;

  /* read values of all records in heap */
  error = heap_get_hfid_from_class_oid (thread_p, &class_oid, &hfid);
  if (error != NO_ERROR || HFID_IS_NULL (&hfid))
    {
      error = ER_FAILED;
      goto exit;
    }

  error = heap_scancache_start (thread_p, &scan_cache, &hfid, NULL, true);
  if (error != NO_ERROR)
    {
      goto exit;
    }
  scan_cache_inited = true;

  max_coll_cnt = LANG_MAX_COLLATIONS;
  alloc_size = max_coll_cnt * sizeof (LANG_COLL_COMPAT);
  *db_collations = (LANG_COLL_COMPAT *) malloc (alloc_size);
  if (*db_collations == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      goto exit;
    }

  *coll_cnt = 0;
  while (heap_next (thread_p, &hfid, NULL, &inst_oid, &recdes,
		    &scan_cache, PEEK) == S_SUCCESS)
    {
      HEAP_ATTRVALUE *heap_value = NULL;
      LANG_COLL_COMPAT *curr_coll;

      if (heap_attrinfo_read_dbvalues (thread_p, &inst_oid, &recdes,
				       &attr_info) != NO_ERROR)
	{
	  error = ER_FAILED;
	  goto exit;
	}

      if (*coll_cnt >= max_coll_cnt)
	{
	  max_coll_cnt = max_coll_cnt * 2;
	  alloc_size = max_coll_cnt * sizeof (LANG_COLL_COMPAT);
	  *db_collations =
	    (LANG_COLL_COMPAT *) realloc (*db_collations, alloc_size);
	  if (*db_collations == NULL)
	    {
	      error = ER_OUT_OF_VIRTUAL_MEMORY;
	      goto exit;
	    }
	}

      curr_coll = &((*db_collations)[(*coll_cnt)++]);
      memset (curr_coll, 0, sizeof (LANG_COLL_COMPAT));

      for (i = 0, heap_value = attr_info.values;
	   i < attr_info.num_values; i++, heap_value++)
	{
	  if (heap_value->attrid == coll_id_att_id)
	    {
	      assert (DB_VALUE_DOMAIN_TYPE (&(heap_value->dbvalue))
		      == DB_TYPE_INTEGER);

	      curr_coll->coll_id = DB_GET_INTEGER (&heap_value->dbvalue);
	    }
	  else if (heap_value->attrid == coll_name_att_id)
	    {
	      char *lang_str = NULL;
	      int lang_str_len;

	      assert (DB_VALUE_DOMAIN_TYPE (&(heap_value->dbvalue))
		      == DB_TYPE_VARCHAR);

	      lang_str = DB_GET_STRING (&heap_value->dbvalue);
	      lang_str_len = (lang_str != NULL) ? strlen (lang_str) : 0;
	      lang_str_len =
		MIN (lang_str_len, (int) sizeof (curr_coll->coll_name) - 1);

	      strncpy (curr_coll->coll_name, lang_str, lang_str_len);
	      curr_coll->coll_name[sizeof (curr_coll->coll_name) - 1] = '\0';
	    }
	  else if (heap_value->attrid == charset_id_att_id)
	    {
	      assert (DB_VALUE_DOMAIN_TYPE (&(heap_value->dbvalue))
		      == DB_TYPE_INTEGER);

	      curr_coll->codeset =
		(INTL_CODESET) DB_GET_INTEGER (&heap_value->dbvalue);
	    }
	  else if (heap_value->attrid == checksum_att_id)
	    {
	      char *checksum_str = NULL;
	      int str_len;

	      assert (DB_VALUE_DOMAIN_TYPE (&(heap_value->dbvalue))
		      == DB_TYPE_VARCHAR);

	      checksum_str = DB_GET_STRING (&heap_value->dbvalue);
	      str_len = (checksum_str != NULL) ? strlen (checksum_str) : 0;

	      assert (str_len == 32);

	      strncpy (curr_coll->checksum, checksum_str, str_len);
	      curr_coll->checksum[str_len] = '\0';
	    }
	}
    }

exit:
  if (scan_cache_inited == true)
    {
      (void) heap_scancache_end (thread_p, &scan_cache);
      scan_cache_inited = false;
    }

  if (attr_info_inited == true)
    {
      heap_attrinfo_end (thread_p, &attr_info);
      attr_info_inited = false;
    }

  return error;
}

/*
 * catcls_get_applier_info () - get max log_record_time
 *                                            in db_ha_apply_info
 *
 *   return: NO_ERROR, or error code
 *
 *   thread_p(in)  : thread context
 *   max_delay(out):
 *
 */
int
catcls_get_applier_info (THREAD_ENTRY * thread_p, INT64 * max_delay)
{
  static OID class_oid = NULL_OID_INITIALIZER;
  static int repl_delay_att_id = -1;
  static HFID hfid = NULL_HFID_INITIALIZER;

  OID inst_oid;
  HEAP_CACHE_ATTRINFO attr_info;
  HEAP_SCANCACHE scan_cache;
  RECDES recdes = RECDES_INITIALIZER;
  INT64 tmp_repl_delay = 0;
  int error = NO_ERROR;
  int i;
  bool attr_info_inited = false;
  bool scan_cache_inited = false;
  int num_record = 0;

  assert (max_delay != NULL);
  *max_delay = 0;

  OID_SET_NULL (&inst_oid);

  if (OID_ISNULL (&class_oid))
    {
      error = catcls_find_class_oid_by_class_name (thread_p,
						   CT_LOG_APPLIER_NAME,
						   &class_oid);
      if (error != NO_ERROR)
	{
	  goto exit;
	}

      if (OID_ISNULL (&class_oid))
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LC_UNKNOWN_CLASSNAME,
		  1, CT_LOG_APPLIER_NAME);
	  error = ER_LC_UNKNOWN_CLASSNAME;
	  goto exit;
	}
    }

  error = heap_attrinfo_start (thread_p, &class_oid, -1, NULL, &attr_info);
  if (error != NO_ERROR)
    {
      goto exit;
    }
  attr_info_inited = true;

  if (repl_delay_att_id == -1)
    {
      heap_scancache_quick_start (&scan_cache);
      scan_cache_inited = true;

      if (heap_get (thread_p, &class_oid, &recdes, &scan_cache, PEEK) !=
	  S_SUCCESS)
	{
	  error = ER_FAILED;
	  goto exit;
	}

      for (i = 0; i < attr_info.num_values; i++)
	{
	  const char *rec_attr_name_p = or_get_attrname (&recdes, i);
	  if (rec_attr_name_p == NULL)
	    {
	      error = ER_FAILED;
	      goto exit;
	    }

	  if (strcmp ("repl_delay", rec_attr_name_p) == 0)
	    {
	      repl_delay_att_id = i;
	      break;
	    }
	}

      if (repl_delay_att_id == -1)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
	  error = ER_FAILED;
	  goto exit;
	}

      heap_scancache_end (thread_p, &scan_cache);
      scan_cache_inited = false;
    }

  if (HFID_IS_NULL (&hfid))
    {
      error = heap_get_hfid_from_class_oid (thread_p, &class_oid, &hfid);
      if (error != NO_ERROR || HFID_IS_NULL (&hfid))
	{
	  error = ER_FAILED;
	  goto exit;
	}
    }

  error = heap_scancache_start (thread_p, &scan_cache, &hfid, NULL, true);
  if (error != NO_ERROR)
    {
      goto exit;
    }
  scan_cache_inited = true;

  while (heap_next (thread_p, &hfid, NULL, &inst_oid, &recdes,
		    &scan_cache, PEEK) == S_SUCCESS)
    {
      HEAP_ATTRVALUE *heap_value = NULL;

      if (heap_attrinfo_read_dbvalues (thread_p, &inst_oid,
				       &recdes, &attr_info) != NO_ERROR)
	{
	  error = ER_FAILED;
	  goto exit;
	}

      tmp_repl_delay = 0;
      for (i = 0, heap_value = attr_info.values; i < attr_info.num_values;
	   i++, heap_value++)
	{
	  if (heap_value->attrid == repl_delay_att_id)
	    {
	      tmp_repl_delay = DB_GET_BIGINT (&heap_value->dbvalue);

	      break;
	    }
	}

      if (tmp_repl_delay > *max_delay)
	{
	  *max_delay = tmp_repl_delay;
	}

      num_record++;
    }

exit:
  if (scan_cache_inited == true)
    {
      (void) heap_scancache_end (thread_p, &scan_cache);
      scan_cache_inited = false;
    }

  if (attr_info_inited == true)
    {
      heap_attrinfo_end (thread_p, &attr_info);
      attr_info_inited = false;
    }

  if (error == NO_ERROR && num_record == 0)
    {
      error = ER_FAILED;
    }

  return error;
}

/*
 * catcls_get_analyzer_info () -
 *
 *   return: NO_ERROR, or error code
 *
 *   thread_p(in)  : thread context
 *   current_pageid(out):
 *   required_pageid(out):
 *
 */
int
catcls_get_analyzer_info (THREAD_ENTRY * thread_p,
			  INT64 * current_pageid, INT64 * required_pageid)
{
  static OID class_oid = NULL_OID_INITIALIZER;
  static int current_lsa_att_id = NULL_ATTRID;
  static int required_lsa_att_id = NULL_ATTRID;
  static HFID hfid = NULL_HFID_INITIALIZER;

  OID inst_oid;
  HEAP_CACHE_ATTRINFO attr_info;
  HEAP_SCANCACHE scan_cache;
  RECDES recdes = RECDES_INITIALIZER;
  INT64 tmp_value = 0;
  int error = NO_ERROR;
  int i;
  bool attr_info_inited = false;
  bool scan_cache_inited = false;
  int num_record = 0;

  assert (current_pageid != NULL && required_pageid != NULL);
  *current_pageid = *required_pageid = NULL_PAGEID;

  OID_SET_NULL (&inst_oid);

  if (OID_ISNULL (&class_oid))
    {
      error = catcls_find_class_oid_by_class_name (thread_p,
						   CT_LOG_ANALYZER_NAME,
						   &class_oid);
      if (error != NO_ERROR)
	{
	  goto exit;
	}

      if (OID_ISNULL (&class_oid))
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LC_UNKNOWN_CLASSNAME,
		  1, CT_LOG_APPLIER_NAME);
	  error = ER_LC_UNKNOWN_CLASSNAME;
	  goto exit;
	}
    }

  error = heap_attrinfo_start (thread_p, &class_oid, -1, NULL, &attr_info);
  if (error != NO_ERROR)
    {
      goto exit;
    }
  attr_info_inited = true;

  if (current_lsa_att_id == NULL_ATTRID || required_lsa_att_id == NULL_ATTRID)
    {
      heap_scancache_quick_start (&scan_cache);
      scan_cache_inited = true;

      if (heap_get (thread_p, &class_oid, &recdes, &scan_cache, PEEK) !=
	  S_SUCCESS)
	{
	  error = ER_FAILED;
	  goto exit;
	}

      for (i = 0; i < attr_info.num_values; i++)
	{
	  const char *rec_attr_name_p = or_get_attrname (&recdes, i);
	  if (rec_attr_name_p == NULL)
	    {
	      error = ER_FAILED;
	      goto exit;
	    }

	  if (strcmp ("current_lsa", rec_attr_name_p) == 0)
	    {
	      current_lsa_att_id = i;
	    }
	  if (strcmp ("required_lsa", rec_attr_name_p) == 0)
	    {
	      required_lsa_att_id = i;
	    }

	  if (current_lsa_att_id != NULL_ATTRID
	      && required_lsa_att_id != NULL_ATTRID)
	    {
	      break;
	    }
	}

      if (current_lsa_att_id == NULL_ATTRID
	  || required_lsa_att_id == NULL_ATTRID)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
	  error = ER_FAILED;
	  goto exit;
	}

      heap_scancache_end (thread_p, &scan_cache);
      scan_cache_inited = false;
    }

  if (HFID_IS_NULL (&hfid))
    {
      error = heap_get_hfid_from_class_oid (thread_p, &class_oid, &hfid);
      if (error != NO_ERROR || HFID_IS_NULL (&hfid))
	{
	  error = ER_FAILED;
	  goto exit;
	}
    }

  error = heap_scancache_start (thread_p, &scan_cache, &hfid, NULL, true);
  if (error != NO_ERROR)
    {
      goto exit;
    }
  scan_cache_inited = true;

  while (heap_next (thread_p, &hfid, NULL, &inst_oid, &recdes,
		    &scan_cache, PEEK) == S_SUCCESS)
    {
      HEAP_ATTRVALUE *heap_value = NULL;

      if (heap_attrinfo_read_dbvalues (thread_p, &inst_oid,
				       &recdes, &attr_info) != NO_ERROR)
	{
	  error = ER_FAILED;
	  goto exit;
	}

      for (i = 0, heap_value = attr_info.values; i < attr_info.num_values;
	   i++, heap_value++)
	{
	  if (heap_value->attrid == current_lsa_att_id)
	    {
	      tmp_value = DB_GET_BIGINT (&heap_value->dbvalue);
	      *current_pageid = tmp_value >> 15;
	    }
	  if (heap_value->attrid == required_lsa_att_id)
	    {
	      tmp_value = DB_GET_BIGINT (&heap_value->dbvalue);
	      *required_pageid = tmp_value >> 15;
	    }
	}
      num_record++;
    }

exit:
  if (scan_cache_inited == true)
    {
      (void) heap_scancache_end (thread_p, &scan_cache);
      scan_cache_inited = false;
    }

  if (attr_info_inited == true)
    {
      heap_attrinfo_end (thread_p, &attr_info);
      attr_info_inited = false;
    }

  if (error == NO_ERROR && num_record == 0)
    {
      error = ER_FAILED;
    }

  return error;
}

/*
 * catcls_get_writer_info () -
 *
 *   return: NO_ERROR, or error code
 *
 *   thread_p(in)  : thread context
 *   last_flushed_pageid(out):
 *   eof_pageid(out):
 *
 */
int
catcls_get_writer_info (THREAD_ENTRY * thread_p,
			INT64 * last_flushed_pageid, INT64 * eof_pageid)
{
  static OID class_oid = NULL_OID_INITIALIZER;
  static int last_flushed_lsa_att_id = NULL_ATTRID;
  static int eof_lsa_att_id = NULL_ATTRID;
  static HFID hfid = NULL_HFID_INITIALIZER;

  OID inst_oid;
  HEAP_CACHE_ATTRINFO attr_info;
  HEAP_SCANCACHE scan_cache;
  RECDES recdes = RECDES_INITIALIZER;
  INT64 tmp_value = 0;
  int error = NO_ERROR;
  int i;
  bool attr_info_inited = false;
  bool scan_cache_inited = false;
  int num_record = 0;

  assert (last_flushed_pageid != NULL && eof_pageid != NULL);
  *last_flushed_pageid = *eof_pageid = NULL_PAGEID;

  OID_SET_NULL (&inst_oid);

  if (OID_ISNULL (&class_oid))
    {
      error = catcls_find_class_oid_by_class_name (thread_p,
						   CT_LOG_WRITER_NAME,
						   &class_oid);
      if (error != NO_ERROR)
	{
	  goto exit;
	}

      if (OID_ISNULL (&class_oid))
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LC_UNKNOWN_CLASSNAME,
		  1, CT_LOG_APPLIER_NAME);
	  error = ER_LC_UNKNOWN_CLASSNAME;
	  goto exit;
	}
    }

  error = heap_attrinfo_start (thread_p, &class_oid, -1, NULL, &attr_info);
  if (error != NO_ERROR)
    {
      goto exit;
    }
  attr_info_inited = true;

  if (last_flushed_lsa_att_id == NULL_ATTRID || eof_lsa_att_id == NULL_ATTRID)
    {
      heap_scancache_quick_start (&scan_cache);
      scan_cache_inited = true;

      if (heap_get (thread_p, &class_oid, &recdes, &scan_cache, PEEK) !=
	  S_SUCCESS)
	{
	  error = ER_FAILED;
	  goto exit;
	}

      for (i = 0; i < attr_info.num_values; i++)
	{
	  const char *rec_attr_name_p = or_get_attrname (&recdes, i);
	  if (rec_attr_name_p == NULL)
	    {
	      error = ER_FAILED;
	      goto exit;
	    }

	  if (strcmp ("last_received_pageid", rec_attr_name_p) == 0)
	    {
	      last_flushed_lsa_att_id = i;
	    }
	  if (strcmp ("eof_lsa", rec_attr_name_p) == 0)
	    {
	      eof_lsa_att_id = i;
	    }

	  if (last_flushed_lsa_att_id != NULL_ATTRID
	      && eof_lsa_att_id != NULL_ATTRID)
	    {
	      break;
	    }
	}

      if (last_flushed_lsa_att_id == NULL_ATTRID
	  || eof_lsa_att_id == NULL_ATTRID)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
	  error = ER_FAILED;
	  goto exit;
	}

      heap_scancache_end (thread_p, &scan_cache);
      scan_cache_inited = false;
    }

  if (HFID_IS_NULL (&hfid))
    {
      error = heap_get_hfid_from_class_oid (thread_p, &class_oid, &hfid);
      if (error != NO_ERROR || HFID_IS_NULL (&hfid))
	{
	  error = ER_FAILED;
	  goto exit;
	}
    }

  error = heap_scancache_start (thread_p, &scan_cache, &hfid, NULL, true);
  if (error != NO_ERROR)
    {
      goto exit;
    }
  scan_cache_inited = true;

  while (heap_next (thread_p, &hfid, NULL, &inst_oid, &recdes,
		    &scan_cache, PEEK) == S_SUCCESS)
    {
      HEAP_ATTRVALUE *heap_value = NULL;

      if (heap_attrinfo_read_dbvalues (thread_p, &inst_oid,
				       &recdes, &attr_info) != NO_ERROR)
	{
	  error = ER_FAILED;
	  goto exit;
	}

      for (i = 0, heap_value = attr_info.values; i < attr_info.num_values;
	   i++, heap_value++)
	{
	  if (heap_value->attrid == last_flushed_lsa_att_id)
	    {
	      *last_flushed_pageid = DB_GET_BIGINT (&heap_value->dbvalue);
	    }
	  if (heap_value->attrid == eof_lsa_att_id)
	    {
	      tmp_value = DB_GET_BIGINT (&heap_value->dbvalue);
	      *eof_pageid = tmp_value >> 15;
	    }
	}
      num_record++;
    }

exit:
  if (scan_cache_inited == true)
    {
      (void) heap_scancache_end (thread_p, &scan_cache);
      scan_cache_inited = false;
    }

  if (attr_info_inited == true)
    {
      heap_attrinfo_end (thread_p, &attr_info);
      attr_info_inited = false;
    }

  if (error == NO_ERROR && num_record == 0)
    {
      error = ER_FAILED;
    }

  return error;
}
