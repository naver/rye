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
 * object_representation_sr.c - Class representation parsing for the server only
 * This is used for updating the catalog manager when class objects are
 * flushed to the server.
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <string.h>

#include "error_manager.h"
#include "oid.h"
#include "storage_common.h"
#include "object_representation.h"
#include "object_representation_sr.h"
#include "system_catalog.h"
#include "object_domain.h"
#include "set_object.h"
#include "btree_load.h"
#include "page_buffer.h"
#include "heap_file.h"
#include "class_object.h"
#include "statistics_sr.h"

/* this must be the last header file included!!! */
#include "dbval.h"

#define DATA_INIT(data, type) memset(data, 0, sizeof(DB_DATA))
#define OR_ARRAY_EXTENT 10

/*
 * VARIABLE OFFSET TABLE ACCESSORS
 * The variable offset table is present in the headers of objects and sets.
 */

#define OR_VAR_TABLE_ELEMENT_OFFSET(table, index) 			\
            OR_VAR_TABLE_ELEMENT_OFFSET_INTERNAL(table, index, 		\
                                                 BIG_VAR_OFFSET_SIZE)

#define OR_VAR_TABLE_ELEMENT_LENGTH(table, index) 			\
            OR_VAR_TABLE_ELEMENT_LENGTH_INTERNAL(table, index, 		\
                                                 BIG_VAR_OFFSET_SIZE)

/* ATTRIBUTE LOCATION */

#define OR_FIXED_ATTRIBUTES_OFFSET(nvars) \
   (OR_FIXED_ATTRIBUTES_OFFSET_INTERNAL(nvars, BIG_VAR_OFFSET_SIZE))

#define OR_FIXED_ATTRIBUTES_OFFSET_INTERNAL(nvars, offset_size) \
   (OR_HEADER_SIZE + OR_VAR_TABLE_SIZE_INTERNAL (nvars, offset_size))


static TP_DOMAIN *or_get_domain_internal (char *ptr);
static TP_DOMAIN *or_get_att_domain_and_cache (char *ptr);
static void or_get_att_index (char *ptr, BTID * btid);
static int or_get_default_value (OR_ATTRIBUTE * attr, char *ptr, int length);
static int or_get_current_default_value (OR_ATTRIBUTE * attr, char *ptr,
					 int length);
#if defined (ENABLE_UNUSED_FUNCTION)
static int or_cl_get_prop_nocopy (DB_SEQ * properties, const char *name,
				  DB_VALUE * pvalue);
#endif
static int or_init_index (OR_INDEX * or_index);
static int or_init_classrep (OR_CLASSREP * or_classrep);
static int or_init_attribute (OR_ATTRIBUTE * or_att);
static OR_CLASSREP *or_get_current_representation (RECDES * record);
static OR_CLASSREP *or_get_old_representation (RECDES * record, int repid);
static int or_get_representation_attributes (OR_ATTRIBUTE ** out_attributes,
					     int *fixed_length,
					     char *disk_rep);
static int or_get_constraints (OR_INDEX ** or_index, int *n_constraints,
			       RECDES * record, OR_ATTRIBUTE * attributes,
			       int n_attributes);
static int or_get_attributes (OR_ATTRIBUTE ** or_attribute,
			      int *fixed_length, RECDES * record);


/*
 * orc_class_repid () - Extracts the current representation id from a record
 *                      containing the disk representation of a class
 *   return: repid of the class object
 *   record(in): disk record
 */
int
orc_class_repid (RECDES * record)
{
  char *ptr;
  int id;

  ptr = (char *) record->data +
    OR_FIXED_ATTRIBUTES_OFFSET (ORC_CLASS_VAR_ATT_COUNT) + ORC_REPID_OFFSET;

  id = OR_GET_INT (ptr);

  return (id);
}

/*
 * orc_class_hfid_from_record () - Extracts just the HFID from the disk
 *                                 representation of a class
 *   return: void
 *   record(in): packed disk record containing class
 *   hfid(in): pointer to HFID structure to be filled in
 *
 * Note: It is used by the catalog manager to update the class information
 *       structure when the HFID is assigned.  Since HFID's are assigned only
 *       when instances are created, a class may be entered into the catalog
 *       before the HFID is known.
 */
void
orc_class_hfid_from_record (RECDES * record, HFID * hfid)
{
  char *ptr;

  ptr = record->data + OR_FIXED_ATTRIBUTES_OFFSET (ORC_CLASS_VAR_ATT_COUNT);
  hfid->vfid.fileid = OR_GET_INT (ptr + ORC_HFID_FILEID_OFFSET);
  hfid->vfid.volid = OR_GET_INT (ptr + ORC_HFID_VOLID_OFFSET);
  hfid->hpgid = OR_GET_INT (ptr + ORC_HFID_PAGEID_OFFSET);
}

/*
 * orc_diskrep_from_record () - Calculate the corresponding DISK_REPR structure
 *                              for the catalog
 *   return: disk representation structure
 *   record(in): disk record
 */
DISK_REPR *
orc_diskrep_from_record (UNUSED_ARG THREAD_ENTRY * thread_p, RECDES * record)
{
  DISK_ATTR *att, *att_fixed, *att_variable;
  OR_ATTRIBUTE *or_att;
  int i, j, k, n_attributes, n_btstats;
  BTREE_STATS *bt_statsp;

  DISK_REPR *rep = NULL;
  OR_CLASSREP *or_rep = NULL;

  int indx_id;
  OR_INDEX *indexp = NULL;

  or_rep = or_get_classrep (record, NULL_REPRID);
  if (or_rep == NULL)
    {
      goto exit_on_error;
    }

  rep = (DISK_REPR *) malloc (sizeof (DISK_REPR));
  if (rep == NULL)
    {
      goto exit_on_error;
    }

  rep->id = or_rep->id;
  rep->n_fixed = 0;
  rep->n_variable = 0;
  rep->fixed_length = or_rep->fixed_length;
#if 0				/* reserved for future use */
  rep->repr_reserved_1 = 0;
#endif
  rep->fixed = NULL;
  rep->variable = NULL;

  /* Calculate the number of fixed and variable length attributes */
  n_attributes = or_rep->n_attributes;
  or_att = or_rep->attributes;
  for (i = 0; i < n_attributes; i++, or_att++)
    {
      if (or_att->is_fixed)
	{
	  (rep->n_fixed)++;
	}
      else
	{
	  (rep->n_variable)++;
	}
    }

  if (rep->n_fixed)
    {
      rep->fixed = (DISK_ATTR *) malloc (sizeof (DISK_ATTR) * rep->n_fixed);
      if (rep->fixed == NULL)
	{
	  goto exit_on_error;
	}
    }

  if (rep->n_variable)
    {
      rep->variable =
	(DISK_ATTR *) malloc (sizeof (DISK_ATTR) * rep->n_variable);
      if (rep->variable == NULL)
	{
	  goto exit_on_error;
	}
    }

  /* Copy the attribute information */
  att_fixed = rep->fixed;
  att_variable = rep->variable;
  or_att = or_rep->attributes;

  for (i = 0; i < n_attributes; i++, or_att++)
    {
      if (or_att->is_fixed)
	{
	  att = att_fixed;
	  att_fixed++;
	}
      else
	{
	  att = att_variable;
	  att_variable++;
	}

      if (att == NULL)
	{
	  goto exit_on_error;
	}

      att->type = or_att->type;
      att->id = or_att->id;
      att->location = or_att->location;
      att->position = or_att->position;
      att->val_length = or_att->default_value.val_length;
      att->value = or_att->default_value.value;
      att->default_expr = or_att->default_value.default_expr;
      or_att->default_value.value = NULL;
      att->classoid = or_att->classoid;

      DATA_INIT (&att->unused_min_value, att->type);
      DATA_INIT (&att->unused_max_value, att->type);

      /* initialize B+tree statistics information */

      n_btstats = att->n_btstats = or_att->n_btids;
      if (n_btstats > 0)
	{
	  att->bt_stats =
	    (BTREE_STATS *) malloc (sizeof (BTREE_STATS) * n_btstats);
	  if (att->bt_stats == NULL)
	    {
	      goto exit_on_error;
	    }

	  for (j = 0, bt_statsp = att->bt_stats; j < n_btstats;
	       j++, bt_statsp++)
	    {
	      bt_statsp->btid = or_att->btids[j];

	      bt_statsp->leafs = 0;
	      bt_statsp->pages = 0;
	      bt_statsp->height = 0;
	      bt_statsp->keys = 0;

	      bt_statsp->pkeys_size = 0;
	      for (k = 0; k < BTREE_STATS_PKEYS_NUM; k++)
		{
		  bt_statsp->pkeys[k] = 0;
		}

	      bt_statsp->tot_free_space = 0;

	      bt_statsp->num_table_vpids = 0;
	      bt_statsp->num_user_pages_mrkdelete = 0;
	      bt_statsp->num_allocsets = 0;

	      /* get the index ID which corresponds to the BTID */
	      indx_id =
		heap_classrepr_find_index_id (or_rep, &(bt_statsp->btid));
	      if (indx_id < 0)
		{
		  /* currently, does not know BTID
		   */
		  continue;
		}

	      indexp = &(or_rep->indexes[indx_id]);

#if 0				/* reserved for future use */
	      for (k = 0; k < BTREE_STATS_RESERVED_NUM; k++)
		{
		  bt_statsp->reserved[k] = 0;
		}
#endif

	      bt_statsp->pkeys_size = indexp->n_atts;
	      assert_release (bt_statsp->pkeys_size > 0);
	      assert_release (bt_statsp->pkeys_size <= BTREE_STATS_PKEYS_NUM);

	      for (k = 0; k < bt_statsp->pkeys_size; k++)
		{
		  bt_statsp->pkeys[k] = 0;
		}

#if 0
	      DB_MAKE_NULL (&(bt_statsp->unused_min_value));
	      DB_MAKE_NULL (&(bt_statsp->unused_max_value));
#endif
	    }			/* for (j = 0, ...) */
	}
      else
	{
	  att->bt_stats = NULL;
	}
    }

  or_free_classrep (or_rep);

  return (rep);

exit_on_error:
  if (rep != NULL)
    {
      orc_free_diskrep (rep);
    }

  if (or_rep != NULL)
    {
      or_free_classrep (or_rep);
    }

  return NULL;
}

/*
 * orc_free_diskrep () - Frees a DISK_REPR structure that was built with
 *                       orc_diskrep_from_record
 *   return: void
 *   rep(in): representation structure
 */
void
orc_free_diskrep (DISK_REPR * rep)
{
  int i;

  if (rep != NULL)
    {
      if (rep->fixed != NULL)
	{
	  for (i = 0; i < rep->n_fixed; i++)
	    {
	      if (rep->fixed[i].value != NULL)
		{
		  free_and_init (rep->fixed[i].value);
		}

	      if (rep->fixed[i].bt_stats != NULL)
		{
		  free_and_init (rep->fixed[i].bt_stats);
		  rep->fixed[i].bt_stats = NULL;
		}
	    }

	  free_and_init (rep->fixed);
	}

      if (rep->variable != NULL)
	{
	  for (i = 0; i < rep->n_variable; i++)
	    {
	      if (rep->variable[i].value != NULL)
		{
		  free_and_init (rep->variable[i].value);
		}

	      if (rep->variable[i].bt_stats != NULL)
		{
		  free_and_init (rep->variable[i].bt_stats);
		  rep->variable[i].bt_stats = NULL;
		}
	    }

	  free_and_init (rep->variable);
	}

      free_and_init (rep);
    }
}

/*
 * orc_class_info_from_record () - Extract the information necessary to build
 *                                 a CLS_INFO
 *    structure for the catalog
 *   return: class info structure
 *   record(in): disk record with class
 */
CLS_INFO *
orc_class_info_from_record (RECDES * record)
{
  CLS_INFO *info;

  info = (CLS_INFO *) malloc (sizeof (CLS_INFO));
  if (info == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      sizeof (CLS_INFO));
      return NULL;
    }

  info->tot_pages = 0;
  info->tot_objects = 0;
  info->time_stamp = stats_get_time_stamp ();	/* current system time */

  orc_class_hfid_from_record (record, &info->hfid);

  return (info);
}

/*
 * orc_free_class_info () - Frees a CLS_INFO structure that was allocated by
 *                          orc_class_info_from_record
 *   return: void
 *   info(in): class info structure
 */
void
orc_free_class_info (CLS_INFO * info)
{
  free_and_init (info);
}

/*
 * or_class_repid () - extracts the current representation id from a record
 *                     containing the disk representation of a class
 *   return: disk record
 *   record(in): repid of the class object
 */
int
or_class_repid (RECDES * record)
{
  char *ptr;
  int id;

  assert (OR_GET_OFFSET_SIZE (record->data) == BIG_VAR_OFFSET_SIZE);

  ptr = (char *) record->data +
    OR_FIXED_ATTRIBUTES_OFFSET (ORC_CLASS_VAR_ATT_COUNT) + ORC_REPID_OFFSET;

  id = OR_GET_INT (ptr);

  return (id);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * or_class_hfid () - Extracts just the HFID from the disk representation of
 *                    a class
 *   return: void
 *   record(in): packed disk record containing class
 *   hfid(in): pointer to HFID structure to be filled in
 *
 * Note: It is used by the catalog manager to update the class information
 *       structure when the HFID is assigned.  Since HFID's are assigned only
 *       when instances are created, a class may be entered into the catalog
 *       before the HFID is known.
 */
void
or_class_hfid (RECDES * record, HFID * hfid)
{
  char *ptr;

  assert (OR_GET_OFFSET_SIZE (record->data) == BIG_VAR_OFFSET_SIZE);

  ptr = record->data + OR_FIXED_ATTRIBUTES_OFFSET (ORC_CLASS_VAR_ATT_COUNT);
  hfid->vfid.fileid = OR_GET_INT (ptr + ORC_HFID_FILEID_OFFSET);
  hfid->vfid.volid = OR_GET_INT (ptr + ORC_HFID_VOLID_OFFSET);
  hfid->hpgid = OR_GET_INT (ptr + ORC_HFID_PAGEID_OFFSET);
}

/*
 * or_class_statistics () - extracts the OID of the statistics instance for
 *                          this class from the disk representation of a class
 *   return: void
 *   record(in): packed disk record containing class
 *   oid(in): pointer to OID structure to be filled in
 */
void
or_class_statistics (RECDES * record, OID * oid)
{
  assert (OR_GET_OFFSET_SIZE (record->data) == BIG_VAR_OFFSET_SIZE);

  /* this doesn't exist yet, return NULL */
  OID_SET_NULL (oid);
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * or_get_domain_internal () -
 *   return: transient domain
 *   ptr(in): pointer to the beginning of a domain in a class
 */
static TP_DOMAIN *
or_get_domain_internal (char *ptr)
{
  TP_DOMAIN *domain, *last, *new_;
  int n_domains, offset, i;
  char *dstart, *fixed;
  DB_TYPE typeid_;

  domain = last = NULL;

  /* ptr has the beginning of a substructure set of domains */
  n_domains = OR_SET_ELEMENT_COUNT (ptr);
  for (i = 0; i < n_domains; i++)
    {
      /* find the start of the domain in the set */
      dstart = ptr + OR_SET_ELEMENT_OFFSET (ptr, i);

      /* dstart points to the offset table for this substructure,
         get the position of the first fixed attribute. */
      fixed = dstart + OR_VAR_TABLE_SIZE (ORC_DOMAIN_VAR_ATT_COUNT);

      typeid_ = (DB_TYPE) OR_GET_INT (fixed + ORC_DOMAIN_TYPE_OFFSET);

      new_ = tp_domain_new (typeid_);
      if (new_ == NULL)
	{
	  /* error handling */
	  while (domain != NULL)
	    {
	      TP_DOMAIN *next = domain->next;

	      assert (domain->next_list == NULL);
	      assert (domain->is_cached == 0);

	      tp_domain_free (domain);
	      domain = next;
	    }
	  return NULL;
	}

      if (last == NULL)
	{
	  domain = new_;
	}
      else
	{
	  last->next = new_;
	}
      last = new_;

      new_->precision = OR_GET_INT (fixed + ORC_DOMAIN_PRECISION_OFFSET);
      new_->scale = OR_GET_INT (fixed + ORC_DOMAIN_SCALE_OFFSET);
      new_->collation_id = OR_GET_INT (fixed +
				       ORC_DOMAIN_COLLATION_ID_OFFSET);

      OR_GET_OID (fixed + ORC_DOMAIN_CLASS_OFFSET, &new_->class_oid);
      /* can't swizzle the pointer on the server */
      new_->class_mop = NULL;

      if (OR_VAR_TABLE_ELEMENT_LENGTH (dstart, ORC_DOMAIN_SETDOMAIN_INDEX) ==
	  0)
	{
	  new_->setdomain = NULL;
	}
      else
	{
	  offset =
	    OR_VAR_TABLE_ELEMENT_OFFSET (dstart, ORC_DOMAIN_SETDOMAIN_INDEX);
	  new_->setdomain = or_get_domain_internal (dstart + offset);
	}

      assert (new_->next_list == NULL);
      assert (new_->is_cached == 0);
      assert (new_->next == NULL);
    }

  return domain;
}

/*
 * or_get_att_domain_and_cache () -
 *   return:
 *   ptr(in):
 */
static TP_DOMAIN *
or_get_att_domain_and_cache (char *ptr)
{
  TP_DOMAIN *domain;

  domain = or_get_domain_internal (ptr);
  if (domain != NULL)
    {
      assert (domain->is_cached == 0);
      assert (domain->next == NULL);

      domain = tp_domain_cache (domain);
    }

  return domain;
}

/*
 * or_get_att_index () - Extracts a BTID from the disk representation of an
 *                       attribute
 *   return: void
 *   ptr(in): buffer pointer
 *   btid(out): btree identifier
 */
static void
or_get_att_index (char *ptr, BTID * btid)
{
  unsigned int uval;

  btid->vfid.fileid = (FILEID) OR_GET_INT (ptr);
  ptr += OR_INT_SIZE;
  btid->root_pageid = (PAGEID) OR_GET_INT (ptr);
  ptr += OR_INT_SIZE;
  uval = (unsigned int) OR_GET_INT (ptr);
  btid->vfid.volid = (VOLID) (uval & 0xFFFF);
}

/*
 * or_get_default_value () - Copies the default value of an attribute from disk
 *   return: NO_ERROR or error code
 *   attr(in): disk attribute structure
 *   ptr(in): pointer to beginning of value
 *   length(in): length of value on disk
 *
 * Note: The data manipulation for this is a bit odd, owing to the "rich"
 *       and varied history of default value manipulation in the catalog.
 *       The callers expect to be given a value buffer in disk representation
 *       format, which as it turns out they will immediately turn around and
 *       use the "readval" function on to get it into a DB_VALUE.  This prevents
 *       us from actually returning the value in a DB_VALUE here because then
 *       the callers would have to deal with two different value formats,
 *       diskrep for non-default values and DB_VALUE rep for default values.
 *       This might not be hard to do and should be considered at some point.
 *
 *       As it stands, we have to perform some of the same operations as
 *       or_get_value here and return a buffer containing a copy of the disk
 *       representation of the value only (not the domain).
 */
static int
or_get_default_value (OR_ATTRIBUTE * attr, char *ptr, int length)
{
  int is_null;
  TP_DOMAIN *domain;
  char *vptr;
  int error = NO_ERROR;

  if (length == 0)
    {
      return NO_ERROR;
    }

  /* skip over the domain tag, check for tagged NULL */
  domain = NULL;
  vptr = or_unpack_domain (ptr, &domain, &is_null);
  if (domain == NULL)
    {
      error = er_errid ();

      GOTO_EXIT_ON_ERROR;
    }

  /* reduce the expected size by the amount consumed with the domain tag */
  length -= (int) (vptr - ptr);
  if (length < 0)
    {
      assert (length >= 0);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");

      GOTO_EXIT_ON_ERROR;
    }

  if (is_null == false && length != 0)
    {
      attr->default_value.val_length = length;
      attr->default_value.value = malloc (length);
      if (attr->default_value.value == NULL)
	{
	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, length);

	  GOTO_EXIT_ON_ERROR;
	}

      memcpy (attr->default_value.value, vptr, length);
    }

  assert (error == NO_ERROR);
  return error;

exit_on_error:

  if (error == NO_ERROR)
    {
      assert (error != NO_ERROR);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
    }

  if (attr->default_value.value != NULL)
    {
      free_and_init (attr->default_value.value);
    }

  return error;
}

/*
 * or_get_current_default_value () - Copies the current default value of an
 *				    attribute from disk
 *   return: NO_ERROR or error code
 *   attr(in): disk attribute structure
 *   ptr(in): pointer to beginning of value
 *   length(in): length of value on disk
 */
static int
or_get_current_default_value (OR_ATTRIBUTE * attr, char *ptr, int length)
{
  int is_null;
  TP_DOMAIN *domain;
  char *vptr;
  int error = NO_ERROR;

  if (length == 0)
    {
      return NO_ERROR;
    }

  /* skip over the domain tag, check for tagged NULL */
  domain = NULL;
  vptr = or_unpack_domain (ptr, &domain, &is_null);
  if (domain == NULL)
    {
      error = er_errid ();

      GOTO_EXIT_ON_ERROR;
    }

  /* reduce the expected size by the amount consumed with the domain tag */
  length -= (int) (vptr - ptr);
  if (length < 0)
    {
      assert (length >= 0);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");

      GOTO_EXIT_ON_ERROR;
    }

  if (is_null == false && length != 0)
    {
      attr->current_default_value.val_length = length;
      attr->current_default_value.value = malloc (length);
      if (attr->current_default_value.value == NULL)
	{
	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, length);

	  GOTO_EXIT_ON_ERROR;
	}

      memcpy (attr->current_default_value.value, vptr, length);
    }

  assert (error == NO_ERROR);
  return error;

exit_on_error:

  if (error == NO_ERROR)
    {
      assert (error != NO_ERROR);
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
    }

  if (attr->current_default_value.value == NULL)
    {
      free_and_init (attr->current_default_value.value);
    }

  return error;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * or_cl_get_prop_nocopy () - Modified version of classobj_get_prop that tries to
 *                            avoid copying of the values
 *   return: non-zero if the property was found
 *   properties(in):  property sequence
 *   name(in): name of property to find
 *   pvalue(in): property value
 *
 * Note: This was written for object_representation_sr.c but could be used in other cases if
 *       you're careful.
 *       Uses the hacked set_get_element_nocopy function above, this is
 *       probably what we should be doing anyway, it would make property list
 *       operations faster.
 */
static int
or_cl_get_prop_nocopy (DB_SEQ * properties, const char *name,
		       DB_VALUE * pvalue)
{
  int error;
  int found, max, i;
  DB_VALUE value;
  char *prop_name;

  error = NO_ERROR;
  found = 0;

  if (properties != NULL && name != NULL && pvalue != NULL)
    {
      max = set_size (properties);
      for (i = 0; i < max && !found && error == NO_ERROR; i += 2)
	{
	  error = set_get_element_nocopy (properties, i, &value);
	  if (error == NO_ERROR)
	    {
	      if (DB_VALUE_TYPE (&value) != DB_TYPE_VARCHAR
		  || DB_GET_STRING (&value) == NULL)
		{
		  error = ER_SM_INVALID_PROPERTY;
		}
	      else
		{
		  prop_name = DB_PULL_STRING (&value);
		  if (strcmp (name, prop_name) == 0)
		    {
		      if ((i + 1) >= max)
			{
			  error = ER_SM_INVALID_PROPERTY;
			}
		      else
			{
			  error =
			    set_get_element_nocopy (properties, i + 1,
						    pvalue);
			  if (error == NO_ERROR)
			    found = i + 1;
			}
		    }
		}
	    }
	}
    }

  if (error)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
    }

  return (found);
}
#endif

/* or_init_index() -
 *
 * return: NO_ERROR
 *
 * or_index(out):
 */
static int
or_init_index (OR_INDEX * or_index)
{
  or_index->next = NULL;
  or_index->atts = NULL;
  or_index->asc_desc = NULL;
  or_index->btname = NULL;
  or_index->type = BTREE_INDEX;
  or_index->n_atts = 0;
  or_index->index_status = INDEX_STATUS_INIT;
  BTID_SET_NULL (&or_index->btid);

  return NO_ERROR;
}

/*
 * or_init_classrep() -
 *
 *   return: NO_ERROR
 *
 *   or_classrep(out):
 */
static int
or_init_classrep (OR_CLASSREP * or_classrep)
{
  or_classrep->next = NULL;

  or_classrep->attributes = NULL;
  or_classrep->indexes = NULL;

  or_classrep->id = NULL_REPRID;

  or_classrep->fixed_length = 0;
  or_classrep->n_attributes = 0;
  or_classrep->n_variable = 0;
  or_classrep->n_indexes = 0;

  return NO_ERROR;
}

/* or_init_attribute() -
 *
 * return: NO_ERROR
 *
 * or_index(out):
 */
static int
or_init_attribute (OR_ATTRIBUTE * or_att)
{
  int i;

  or_att->next = NULL;

  or_att->id = NULL_ATTRID;
  or_att->type = DB_TYPE_UNKNOWN;
  or_att->def_order = 0;
  or_att->location = 0;
  or_att->position = 0;
  OID_SET_NULL (&or_att->classoid);

  memset (&or_att->default_value, 0, sizeof (OR_DEFAULT_VALUE));
  memset (&or_att->current_default_value, 0, sizeof (OR_DEFAULT_VALUE));
  or_att->btids = NULL;
  or_att->domain = NULL;
  or_att->n_btids = 0;
  BTID_SET_NULL (&or_att->index);

  or_att->max_btids = 0;
  for (i = 0; i < OR_ATT_BTID_PREALLOC; i++)
    {
      BTID_SET_NULL (&or_att->btid_pack[i]);
    }

  or_att->is_fixed = 0;
  or_att->is_notnull = 0;
  or_att->is_shard_key = 0;

  return NO_ERROR;
}

/*
 * or_get_current_representation () - build an OR_CLASSREP structure for the
 *                                    most recent representation
 *   return: disk representation structure
 *   record(in): disk record
 *
 * Note: This is similar to the old function orc_diskrep_from_record, but is
 *       a little simpler now that we don't need to maintain a separate
 *       list for the fixed and variable length attributes.
 *
 *       The logic is different from the logic in get_old_representation
 *       because the structures used to hold the most recent representation
 *       are different than the simplified structures used to hold the old
 *       representations.
 */
static OR_CLASSREP *
or_get_current_representation (RECDES * record)
{
  OR_CLASSREP *rep;
  char *start, *ptr;
  int n_fixed, n_variable;
  int fixed_length;
  int error = NO_ERROR;

  rep = (OR_CLASSREP *) malloc (sizeof (OR_CLASSREP));
  if (rep == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
	      sizeof (OR_CLASSREP));

      GOTO_EXIT_ON_ERROR;
    }

  or_init_classrep (rep);

  start = record->data;
  assert (OR_GET_OFFSET_SIZE (start) == BIG_VAR_OFFSET_SIZE);

  ptr = start + OR_FIXED_ATTRIBUTES_OFFSET (ORC_CLASS_VAR_ATT_COUNT);

  rep->id = OR_GET_INT (ptr + ORC_REPID_OFFSET);
  rep->fixed_length = OR_GET_INT (ptr + ORC_FIXED_LENGTH_OFFSET);
  n_fixed = OR_GET_INT (ptr + ORC_FIXED_COUNT_OFFSET);
  n_variable = OR_GET_INT (ptr + ORC_VARIABLE_COUNT_OFFSET);
  rep->n_attributes = n_fixed + n_variable;
  rep->n_variable = n_variable;

  error = or_get_attributes (&rep->attributes, &fixed_length, record);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  assert (rep->fixed_length == fixed_length);

  /* Read the B-tree IDs from the class constraint list */
  error = or_get_constraints (&rep->indexes, &rep->n_indexes, record,
			      rep->attributes, rep->n_attributes);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  assert ((rep->n_indexes > 0 && rep->indexes != NULL)
	  || (rep->n_indexes == 0 && rep->indexes == NULL));

  return rep;

exit_on_error:

  if (error == NO_ERROR)
    {
      assert (error != NO_ERROR);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
    }

  if (rep != NULL)
    {
      or_free_classrep (rep);
      rep = NULL;
    }

  return NULL;
}

/*
 * or_get_attributes () -
 *
 *   return: number of attributes or error code
 *
 *   or_attributes(out):
 *   out_n_attributes(out):
 *   out_n_variable(out):
 *   record(in):
 */
static int
or_get_attributes (OR_ATTRIBUTE ** or_attribute, int *fixed_length,
		   RECDES * record)
{
  OR_ATTRIBUTE *attributes = NULL, *att = NULL;

  OID oid;
  char *start, *ptr, *attset, *diskatt, *valptr, *dptr, *valptr1, *valptr2;
  int i, offset, vallen, vallen1, vallen2;
  int n_attributes = 0, n_variable = 0, n_fixed = 0;
  OR_BUF buf;
  DB_VALUE val, def_expr;
  DB_SEQ *att_props = NULL;
  int error = NO_ERROR;


  /* init out parameters */
  *or_attribute = NULL;
  *fixed_length = 0;


  start = record->data;

  assert (OR_GET_OFFSET_SIZE (start) == BIG_VAR_OFFSET_SIZE);

  ptr = start + OR_FIXED_ATTRIBUTES_OFFSET (ORC_CLASS_VAR_ATT_COUNT);


  n_fixed = OR_GET_INT (ptr + ORC_FIXED_COUNT_OFFSET);
  n_variable = OR_GET_INT (ptr + ORC_VARIABLE_COUNT_OFFSET);
  n_attributes = n_fixed + n_variable;

  if (n_attributes == 0)
    {
      return NO_ERROR;
    }

  attributes = (OR_ATTRIBUTE *) malloc (sizeof (OR_ATTRIBUTE) * n_attributes);
  if (attributes == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
	      sizeof (OR_ATTRIBUTE) * n_attributes);

      GOTO_EXIT_ON_ERROR;
    }
  for (i = 0; i < n_attributes; i++)
    {
      or_init_attribute (&attributes[i]);
    }

  /* find the beginning of the "set_of(attribute)" attribute inside the class */
  attset = start + OR_VAR_OFFSET (start, ORC_ATTRIBUTES_INDEX);

  /* calculate the offset to the first fixed width attribute in instances
     of this class. */
  offset = 0;

  for (i = 0, att = attributes; i < n_attributes; i++, att++)
    {
      /* diskatt will now be pointing at the offset table for this attribute.
         this is logically the "start" of this nested object. */
      diskatt = attset + OR_SET_ELEMENT_OFFSET (attset, i);

      /* find out where the original default value is kept */
      valptr = (diskatt +
		OR_VAR_TABLE_ELEMENT_OFFSET (diskatt,
					     ORC_ATT_ORIGINAL_VALUE_INDEX));

      vallen = OR_VAR_TABLE_ELEMENT_LENGTH (diskatt,
					    ORC_ATT_ORIGINAL_VALUE_INDEX);

      valptr2 = (diskatt +
		 OR_VAR_TABLE_ELEMENT_OFFSET (diskatt,
					      ORC_ATT_CURRENT_VALUE_INDEX));

      vallen2 = OR_VAR_TABLE_ELEMENT_LENGTH (diskatt,
					     ORC_ATT_CURRENT_VALUE_INDEX);

      valptr1 = (diskatt +
		 OR_VAR_TABLE_ELEMENT_OFFSET (diskatt,
					      ORC_ATT_PROPERTIES_INDEX));

      vallen1 = OR_VAR_TABLE_ELEMENT_LENGTH (diskatt,
					     ORC_ATT_PROPERTIES_INDEX);

      or_init (&buf, valptr1, vallen1);

      /* set ptr to the beginning of the fixed attributes */
      ptr = diskatt + OR_VAR_TABLE_SIZE (ORC_ATT_VAR_ATT_COUNT);

      if (OR_GET_INT (ptr + ORC_ATT_FLAG_OFFSET) & SM_ATTFLAG_NON_NULL)
	{
	  att->is_notnull = 1;
	}
      else
	{
	  att->is_notnull = 0;
	}

      if (OR_GET_INT (ptr + ORC_ATT_FLAG_OFFSET) & SM_ATTFLAG_SHARD_KEY)
	{
	  att->is_shard_key = 1;
	}
      else
	{
	  att->is_shard_key = 0;
	}

      att->type = (DB_TYPE) OR_GET_INT (ptr + ORC_ATT_TYPE_OFFSET);
      att->id = OR_GET_INT (ptr + ORC_ATT_ID_OFFSET);
      att->def_order = OR_GET_INT (ptr + ORC_ATT_DEF_ORDER_OFFSET);
      att->position = i;
      att->default_value.val_length = 0;
      att->default_value.value = NULL;
      att->current_default_value.val_length = 0;
      att->current_default_value.value = NULL;
      OR_GET_OID (ptr + ORC_ATT_CLASS_OFFSET, &oid);
      att->classoid = oid;

      /* get the btree index id if an index has been assigned */
      or_get_att_index (ptr + ORC_ATT_INDEX_OFFSET, &att->index);

      /* We won't know if there are any B-tree ID's for unique constraints
         until we read the class property list later on */
      att->n_btids = 0;
      att->btids = NULL;

      /* Extract the full domain for this attribute, think about caching here
         it will add some time that may not be necessary. */
      if (OR_VAR_TABLE_ELEMENT_LENGTH (diskatt, ORC_ATT_DOMAIN_INDEX) == 0)
	{
	  /* shouldn't happen, fake one up from the type ! */
	  att->domain = tp_domain_resolve_default (att->type);
	}
      else
	{
	  dptr =
	    diskatt + OR_VAR_TABLE_ELEMENT_OFFSET (diskatt,
						   ORC_ATT_DOMAIN_INDEX);
	  att->domain = or_get_att_domain_and_cache (dptr);
	}

      if (i < n_fixed)
	{
	  att->is_fixed = 1;
	  att->location = offset;
	  offset += tp_domain_disk_size (att->domain);
	}
      else
	{
	  att->is_fixed = 0;
	  att->location = i - n_fixed;
	}

      /* get the current default value */
      if (vallen2)
	{
	  error = or_get_current_default_value (att, valptr2, vallen2);
	  if (error != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}

      /* get the default value, this could be using a new DB_VALUE ? */
      if (vallen)
	{
	  error = or_get_default_value (att, valptr, vallen);
	  if (error != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}

      att->default_value.default_expr = DB_DEFAULT_NONE;
      att->current_default_value.default_expr = DB_DEFAULT_NONE;
      if (vallen1 > 0)
	{
	  db_make_null (&val);
	  db_make_null (&def_expr);
	  or_get_value (&buf, &val,
			tp_domain_resolve_default (DB_TYPE_SEQUENCE),
			vallen1, true);
	  att_props = DB_GET_SEQUENCE (&val);
	  if (att_props != NULL &&
	      classobj_get_prop (att_props, "default_expr", &def_expr) > 0)
	    {
	      att->default_value.default_expr = DB_GET_INTEGER (&def_expr);
	      att->current_default_value.default_expr = DB_GET_INTEGER
		(&def_expr);
	    }

	  pr_clear_value (&def_expr);
	  pr_clear_value (&val);
	}
    }

  *or_attribute = attributes;
  *fixed_length = DB_ATT_ALIGN (offset);

  assert (n_attributes > 0);
  assert (error == NO_ERROR);
  return error;

exit_on_error:

  if (error == NO_ERROR)
    {
      assert (error != NO_ERROR);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
    }

  if (attributes != NULL)
    {
      assert (n_attributes > 0);

      or_free_attributes (attributes, n_attributes);
      attributes = NULL;
      n_attributes = 0;
    }

  return error;
}

/*
 * or_get_constraints () -
 *
 *   return: number of constraints or error code
 *
 *   or_index(out):
 *   record(in):
 *   attributes(in):
 *   n_attributes(in):
 */
static int
or_get_constraints (OR_INDEX ** or_index, int *n_constraints, RECDES * record,
		    OR_ATTRIBUTE * attributes, int n_attributes)
{
  OR_ATTRIBUTE *att, *found_att = NULL;
  OR_INDEX *indexes = NULL;
  OR_BUF buf;
  char *start, *conset, *diskcon, *disk_con_att, *ptr;
  char *valptr, *valptr1, *valptr2;
  int vallen, vallen1, vallen2;
  char *btid_string;
  int num_cons, n_atts;
  int att_id;
  int i, j, k;
  int size;
  int error = NO_ERROR;
  SM_CONSTRAINT_TYPE cons_type;

  /* init out param. */
  *or_index = NULL;
  *n_constraints = 0;

  start = record->data;

  if (OR_VAR_IS_NULL (start, ORC_CONSTRAINTS_INDEX))
    {
      /* not found constraints */
      return NO_ERROR;
    }

  conset = (start + OR_VAR_OFFSET (start, ORC_CONSTRAINTS_INDEX));
  num_cons = OR_SET_ELEMENT_COUNT (conset);
  if (num_cons <= 0)
    {
      assert (num_cons > 0);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");

      GOTO_EXIT_ON_ERROR;
    }

  indexes = (OR_INDEX *) malloc (sizeof (OR_INDEX) * num_cons);
  if (indexes == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      error, 1, sizeof (OR_INDEX) * num_cons);

      GOTO_EXIT_ON_ERROR;
    }
  for (i = 0; i < num_cons; i++)
    {
      or_init_index (&indexes[i]);
    }

  conset = start + OR_VAR_OFFSET (start, ORC_CONSTRAINTS_INDEX);

  for (i = 0; i < num_cons; i++)
    {
      diskcon = conset + OR_SET_ELEMENT_OFFSET (conset, i);

      valptr = (diskcon + OR_VAR_TABLE_ELEMENT_OFFSET (diskcon,
						       ORC_CLASS_CONSTRAINTS_NAME_INDEX));

      vallen = OR_VAR_TABLE_ELEMENT_LENGTH (diskcon,
					    ORC_CLASS_CONSTRAINTS_NAME_INDEX);

      valptr1 = (diskcon + OR_VAR_TABLE_ELEMENT_OFFSET (diskcon,
							ORC_CLASS_CONSTRAINTS_BTID_INDEX));

      vallen1 = OR_VAR_TABLE_ELEMENT_LENGTH (diskcon,
					     ORC_CLASS_CONSTRAINTS_BTID_INDEX);

      valptr2 = (diskcon + OR_VAR_TABLE_ELEMENT_OFFSET (diskcon,
							ORC_CLASS_CONSTRAINTS_ATTS_INDEX));

      vallen2 = OR_VAR_TABLE_ELEMENT_LENGTH (diskcon,
					     ORC_CLASS_CONSTRAINTS_ATTS_INDEX);

      /* set ptr to the beginning of the fixed attributes */
      ptr = diskcon + OR_VAR_TABLE_SIZE (ORC_CLASS_CONSTRAINTS_VAR_ATT_COUNT);

      cons_type =
	(SM_CONSTRAINT_TYPE) OR_GET_INT (ptr +
					 ORC_CLASS_CONSTRAINTS_TYPE_OFFSET);
      switch (cons_type)
	{
	case SM_CONSTRAINT_UNIQUE:
	  indexes[i].type = BTREE_UNIQUE;
	  break;
	case SM_CONSTRAINT_INDEX:
	  indexes[i].type = BTREE_INDEX;
	  break;
	case SM_CONSTRAINT_NOT_NULL:
	  assert (cons_type != SM_CONSTRAINT_NOT_NULL);
	  indexes[i].type = BTREE_INDEX;
	  break;
	case SM_CONSTRAINT_PRIMARY_KEY:
	  indexes[i].type = BTREE_PRIMARY_KEY;
	  break;
	default:
	  assert (false);
	  indexes[i].type = BTREE_INDEX;
	  break;
	}

      indexes[i].index_status = OR_GET_INT (ptr +
					    ORC_CLASS_CONSTRAINTS_STATUS_OFFSET);

      or_init (&buf, valptr, vallen);
      indexes[i].btname = or_packed_get_varchar (&buf, &vallen);

      or_init (&buf, valptr1, vallen1);
      btid_string = or_packed_get_varchar (&buf, &vallen1);
      classobj_decompose_property_btid (btid_string, &indexes[i].btid);
      free_and_init (btid_string);


      n_atts = OR_SET_ELEMENT_COUNT (valptr2);
      indexes[i].atts =
	(OR_ATTRIBUTE **) malloc (sizeof (OR_ATTRIBUTE *) * n_atts);
      if (indexes[i].atts == NULL)
	{
	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  error, 1, sizeof (OR_ATTRIBUTE *) * n_atts);
	  goto exit_on_error;
	}
      memset (indexes[i].atts, 0, sizeof (OR_ATTRIBUTE *) * n_atts);

      indexes[i].asc_desc = (int *) malloc (sizeof (int) * n_atts);
      if (indexes[i].asc_desc == NULL)
	{
	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  error, 1, sizeof (int) * n_atts);

	  GOTO_EXIT_ON_ERROR;
	}

      indexes[i].n_atts = 0;
      for (j = 0; j < n_atts; j++)
	{
	  disk_con_att = valptr2 + OR_SET_ELEMENT_OFFSET (valptr2, j);

	  disk_con_att = disk_con_att +
	    OR_VAR_TABLE_SIZE (ORC_CLASS_CONSTRAINTS_ATTS_VAR_ATT_COUNT);

	  indexes[i].asc_desc[j] = OR_GET_INT (disk_con_att +
					       ORC_CLASS_CONSTRAINTS_ATT_ASCDESC_OFFSET);
	  att_id = OR_GET_INT (disk_con_att +
			       ORC_CLASS_CONSTRAINTS_ATT_ATTID_OFFSET);
	  found_att = NULL;
	  for (k = 0, att = attributes; k < n_attributes; k++, att++)
	    {
	      if (att->id == att_id)
		{
		  found_att = att;
		  indexes[i].atts[indexes[i].n_atts] = found_att;
		  indexes[i].n_atts++;
		  break;
		}
	    }

	  /* found_att == NULL if drop column */
	  if (j == 0 && found_att != NULL)
	    {
	      /* first attribute */
	      assert (indexes[i].atts[j] == found_att);
	      if (found_att->btids == NULL)
		{
		  /* we've never had one before, use the local pack */
		  found_att->btids = found_att->btid_pack;
		  found_att->max_btids = OR_ATT_BTID_PREALLOC;
		}
	      else
		{
		  /* we've already got one, continue to use the local pack until that
		     runs out and then start mallocing. */
		  if (found_att->n_btids >= found_att->max_btids)
		    {
		      if (found_att->btids == found_att->btid_pack)
			{
			  /* allocate a bigger array and copy over our local pack */
			  size = found_att->n_btids + OR_ATT_BTID_PREALLOC;
			  found_att->btids =
			    (BTID *) malloc (sizeof (BTID) * size);
			  if (found_att->btids != NULL)
			    {
			      memcpy (found_att->btids,
				      found_att->btid_pack,
				      (sizeof (BTID) * found_att->n_btids));
			    }
			  found_att->max_btids = size;
			}
		      else
			{
			  /* we already have an externally allocated array, make it
			     bigger */
			  size = found_att->n_btids + OR_ATT_BTID_PREALLOC;
			  found_att->btids =
			    (BTID *) realloc (found_att->btids,
					      size * sizeof (BTID));
			  found_att->max_btids = size;
			}
		    }
		}

	      if (found_att->btids)
		{
		  found_att->btids[found_att->n_btids] = indexes[i].btid;
		  found_att->n_btids += 1;
		}
	    }
	}
    }

  *or_index = indexes;
  indexes = NULL;
  *n_constraints = num_cons;

  assert (num_cons > 0);
  return error;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (error != NO_ERROR);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
    }

  if (indexes != NULL)
    {
      assert (num_cons > 0);
      or_free_constraints (indexes, num_cons);
      indexes = NULL;
      num_cons = 0;
    }

  return error;;
}

/*
 * or_get_old_representation () - Extracts the description of an old
 *                                representation from the disk image of a
 *                                class
 *   return:
 *   record(in): record with class diskrep
 *   repid(in): representation id to extract
 *
 * Note: It is similar to get_current_representation
 *       except that it must get its information out of the compressed
 *       SM_REPRESENTATION & SM_REPR_ATTRIBUTE structures which are used for
 *       storing the old representations.  The current representation is stored
 *       in top-level SM_ATTRIBUTE structures which are much larger.
 *
 *       If repid is -1 here, it returns the current representation.
 *       It returns NULL on error.  This can happen during memory allocation
 *       failure but is more likely to happen if the repid given was not
 *       found within the class.
 */
static OR_CLASSREP *
or_get_old_representation (RECDES * record, int repid)
{
  OR_CLASSREP *rep = NULL;
  char *repset, *disk_rep;
  int rep_count, i, n_fixed, n_variable, id;
  char *fixed = NULL;
  int error = NO_ERROR;

  if (repid == NULL_REPRID)
    {
      return or_get_current_representation (record);
    }

#if !defined(NDEBUG)
  {
    int current_repid;

    fixed =
      record->data + OR_FIXED_ATTRIBUTES_OFFSET (ORC_CLASS_VAR_ATT_COUNT);
    current_repid = OR_GET_INT (fixed + ORC_REPID_OFFSET);

    assert (repid != current_repid);

    fixed = NULL;
  }
#endif


  /* find the beginning of the "set_of(representation)" attribute inside
   * the class.
   * If this attribute is NULL, we're missing the representations, its an
   * error.
   */
  if (OR_VAR_IS_NULL (record->data, ORC_REPRESENTATIONS_INDEX))
    {
      return NULL;
    }

  assert (OR_GET_OFFSET_SIZE (record->data) == BIG_VAR_OFFSET_SIZE);

  repset = (record->data +
	    OR_VAR_OFFSET (record->data, ORC_REPRESENTATIONS_INDEX));

  /* repset now points to the beginning of a complex set representation,
     find out how many elements are in the set. */
  rep_count = OR_SET_ELEMENT_COUNT (repset);

  /* locate the beginning of the representation in this set whose id matches
     the given repid. */
  disk_rep = NULL;
  for (i = 0; i < rep_count; i++)
    {
      /* set disk_rep to the beginning of the i'th set element */
      disk_rep = repset + OR_SET_ELEMENT_OFFSET (repset, i);

      /* move ptr up to the beginning of the fixed width attributes in this
         object */
      fixed = disk_rep + OR_VAR_TABLE_SIZE (ORC_REP_VAR_ATT_COUNT);

      /* extract the id of this representation */
      id = OR_GET_INT (fixed + ORC_REP_ID_OFFSET);

      if (id == repid)
	{
	  break;
	}
      else
	{
	  disk_rep = NULL;
	}
    }

  if (disk_rep == NULL)
    {
      error = ER_CT_UNKNOWN_REPRID;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, repid);
      return NULL;
    }

  /* allocate a new memory structure for this representation */
  rep = (OR_CLASSREP *) malloc (sizeof (OR_CLASSREP));
  if (rep == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
	      sizeof (OR_CLASSREP));

      return NULL;
    }

  or_init_classrep (rep);

  /* at this point, disk_rep points to the beginning of the representation
     object and "fixed" points at the first fixed width attribute. */

  n_fixed = OR_GET_INT (fixed + ORC_REP_FIXED_COUNT_OFFSET);
  n_variable = OR_GET_INT (fixed + ORC_REP_VARIABLE_COUNT_OFFSET);

  rep->id = repid;
  rep->n_attributes = n_fixed + n_variable;
  rep->n_variable = n_variable;

  if (rep->n_attributes == 0)
    {
      /* its an empty representation, return it */
      return rep;
    }

  error = or_get_representation_attributes (&rep->attributes,
					    &rep->fixed_length, disk_rep);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  return rep;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (error != NO_ERROR);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
    }

  if (rep != NULL)
    {
      or_free_classrep (rep);
      rep = NULL;
    }

  return NULL;
}

/*
 * or_get_representation_attributes () -
 *   return:
 *   record(in): record with class diskrep
 *   repid(in): representation id to extract
 */
static int
or_get_representation_attributes (OR_ATTRIBUTE ** out_attributes,
				  int *fixed_length, char *disk_rep)
{
  OR_ATTRIBUTE *att, *attributes;
  char *attset, *repatt, *dptr;
  int i, offset;
  int n_attributes, n_fixed, n_variable;
  char *fixed = NULL;
  int error = NO_ERROR;

  /* init out param */
  *out_attributes = NULL;
  *fixed_length = 0;

  /* init local */
  attributes = NULL;
  n_attributes = 0;
  n_variable = 0;
  n_fixed = 0;


  assert (disk_rep != NULL);

  fixed = disk_rep + OR_VAR_TABLE_SIZE (ORC_REP_VAR_ATT_COUNT);


  /* at this point, disk_rep points to the beginning of the representation
     object and "fixed" points at the first fixed width attribute. */
  n_fixed = OR_GET_INT (fixed + ORC_REP_FIXED_COUNT_OFFSET);
  n_variable = OR_GET_INT (fixed + ORC_REP_VARIABLE_COUNT_OFFSET);

  n_attributes = n_fixed + n_variable;

  if (n_attributes == 0)
    {
      return NO_ERROR;
    }

  attributes = (OR_ATTRIBUTE *) malloc (sizeof (OR_ATTRIBUTE) * n_attributes);
  if (attributes == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
	      sizeof (OR_ATTRIBUTE) * n_attributes);

      GOTO_EXIT_ON_ERROR;
    }
  for (i = 0; i < n_attributes; i++)
    {
      or_init_attribute (&attributes[i]);
    }

  /* Calculate the beginning of the set_of(rep_attribute) in the representation
   * object. Assume that the start of the disk_rep points directly at the the
   * substructure's variable offset table (which it does) and use
   * OR_VAR_TABLE_ELEMENT_OFFSET.
   */
  attset =
    disk_rep + OR_VAR_TABLE_ELEMENT_OFFSET (disk_rep,
					    ORC_REP_ATTRIBUTES_INDEX);

  /* Calculate the offset to the first fixed width attribute in instances
   * of this class.  Save the start of this region so we can calculate the
   * total fixed witdh size.
   */
  offset = 0;

  /* build up the attribute descriptions */
  for (i = 0, att = attributes; i < n_attributes; i++, att++)
    {
      /* set repatt to the beginning of the rep_attribute object in the set */
      repatt = attset + OR_SET_ELEMENT_OFFSET (attset, i);

      /* set fixed to the beginning of the fixed width attributes for this
         object */
      fixed = repatt + OR_VAR_TABLE_SIZE (ORC_REPATT_VAR_ATT_COUNT);

      att->id = OR_GET_INT (fixed + ORC_REPATT_ID_OFFSET);
      att->type = (DB_TYPE) OR_GET_INT (fixed + ORC_REPATT_TYPE_OFFSET);
      att->position = i;
      att->default_value.val_length = 0;
      att->default_value.value = NULL;
      att->default_value.default_expr = DB_DEFAULT_NONE;
      att->current_default_value.val_length = 0;
      att->current_default_value.value = NULL;
      att->current_default_value.default_expr = DB_DEFAULT_NONE;

      /* We won't know if there are any B-tree ID's for unique constraints
         until we read the class property list later on */
      att->n_btids = 0;
      att->btids = NULL;

      /* not currently available, will this be a problem ? */
      OID_SET_NULL (&(att->classoid));
      BTID_SET_NULL (&(att->index));

      /* Extract the full domain for this attribute, think about caching here
         it will add some time that may not be necessary. */
      if (OR_VAR_TABLE_ELEMENT_LENGTH (repatt, ORC_REPATT_DOMAIN_INDEX) == 0)
	{
	  assert (false);
	  /* shouldn't happen, fake one up from the type ! */
	  att->domain = tp_domain_resolve_default (att->type);
	}
      else
	{
	  dptr =
	    repatt + OR_VAR_TABLE_ELEMENT_OFFSET (repatt,
						  ORC_REPATT_DOMAIN_INDEX);
	  att->domain = or_get_att_domain_and_cache (dptr);
	}

      if (i < n_fixed)
	{
	  att->is_fixed = 1;
	  att->location = offset;
	  offset += tp_domain_disk_size (att->domain);
	}
      else
	{
	  att->is_fixed = 0;
	  att->location = i - n_fixed;
	}
    }

  /* Offset at this point contains the total fixed size of the
   * representation plus the starting offset, remove the starting offset
   * to get the length of just the fixed width attributes.
   */

  *out_attributes = attributes;
  /* must align up to a word boundar ! */
  *fixed_length = DB_ATT_ALIGN (offset);

  assert (error == NO_ERROR);
  return error;

exit_on_error:

  if (error == NO_ERROR)
    {
      assert (error != NO_ERROR);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
    }

  if (attributes != NULL)
    {
      free_and_init (attributes);
    }

  return error;
}

/*
 * or_get_all_representation () - Extracts the description of all
 *                                representation from the disk image of a
 *                                class.
 *   return:
 *   record(in): record with class diskrep
 *   count(out): the number of representation to be returned
 */
OR_CLASSREP **
or_get_all_representation (RECDES * record, int *count)
{
  OR_CLASSREP **rep_arr = NULL;
  char *repset = NULL;
  int repid;
  int old_rep_count = 0, i;

  if (count == NULL)
    {
      assert (false);
      goto error;
    }

  *count = 0;

  assert (OR_GET_OFFSET_SIZE (record->data) == BIG_VAR_OFFSET_SIZE);

  if (!OR_VAR_IS_NULL (record->data, ORC_REPRESENTATIONS_INDEX))
    {
      repset = (record->data +
		OR_VAR_OFFSET (record->data, ORC_REPRESENTATIONS_INDEX));
      old_rep_count = OR_SET_ELEMENT_COUNT (repset);
    }

  /* add one for current representation */
  rep_arr =
    (OR_CLASSREP **) malloc (sizeof (OR_CLASSREP *) * (old_rep_count + 1));
  if (rep_arr == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      (sizeof (OR_CLASSREP *) * (old_rep_count + 1)));
      return NULL;
    }

  memset (rep_arr, 0x0, sizeof (OR_CLASSREP *) * (old_rep_count + 1));

  /* current representation */
  rep_arr[0] = or_get_current_representation (record);
  if (rep_arr[0] == NULL)
    {
      goto error;
    }

  for (repid = rep_arr[0]->id - 1, i = 0; repid >= 0 && i < old_rep_count;
       repid--)
    {
      rep_arr[i + 1] = or_get_old_representation (record, repid);
      if (rep_arr[i + 1] != NULL)
	{
	  i++;
	}
    }
  assert (old_rep_count == i);

  *count = old_rep_count + 1;

  return rep_arr;

error:
  if (rep_arr != NULL)
    {
      for (i = 0; i < old_rep_count + 1; i++)
	{
	  or_free_classrep (rep_arr[i]);
	}
      free_and_init (rep_arr);
    }

  return NULL;
}

/*
 * or_get_classrep () - builds an in-memory OR_CLASSREP that describes the
 *                      class
 *   return: OR_CLASSREP structure
 *   record(in): disk record
 *   repid(in): representation of interest (-1) for current
 *
 * Note: This structure is in turn used to navigate over the instances of this
 *       class stored in the heap.
 *       It calls either get_current_representation or get_old_representation
 *       to do the work.
 */
OR_CLASSREP *
or_get_classrep (RECDES * record, int repid)
{
  OR_CLASSREP *rep;
  char *fixed;
  int current;

  assert (OR_GET_OFFSET_SIZE (record->data) == BIG_VAR_OFFSET_SIZE);

  if (repid == NULL_REPRID)
    {
      rep = or_get_current_representation (record);
    }
  else
    {
      /* find out what the most recent representation is */
      fixed =
	record->data + OR_FIXED_ATTRIBUTES_OFFSET (ORC_CLASS_VAR_ATT_COUNT);
      current = OR_GET_INT (fixed + ORC_REPID_OFFSET);

      if (current == repid)
	{
	  rep = or_get_current_representation (record);
	}
      else
	{
	  rep = or_get_old_representation (record, repid);
	}
    }

  return rep;
}

/*
 * or_free_classrep () - Frees an OR_CLASSREP structure returned by
 *                       or_get_classrep
 *   return: void
 *   rep(in): representation structure
 */
void
or_free_classrep (OR_CLASSREP * rep)
{
  if (rep != NULL)
    {
      if (rep->attributes != NULL)
	{
	  or_free_attributes (rep->attributes, rep->n_attributes);
	  rep->n_attributes = 0;
	  rep->attributes = NULL;
	}

      if (rep->indexes != NULL)
	{
	  or_free_constraints (rep->indexes, rep->n_indexes);
	  rep->n_indexes = 0;
	  rep->indexes = NULL;
	}

      free_and_init (rep);
    }
}

/*
 * or_free_attributes () - Frees an OR_ATTRIBUTE structure returned by
 *                       or_get_attributes
 *   return: void
 *   attributes(in/out): attribute structure
 *   n_attributes(in): number of attributes
 */
void
or_free_attributes (OR_ATTRIBUTE * attributes, int n_attributes)
{
  OR_ATTRIBUTE *att;
  int i;


  for (i = 0, att = attributes; i < n_attributes; i++, att++)
    {
      if (att->default_value.value != NULL)
	{
	  free_and_init (att->default_value.value);
	}

      if (att->current_default_value.value != NULL)
	{
	  free_and_init (att->current_default_value.value);
	}

      if (att->btids != NULL && att->btids != att->btid_pack)
	{
	  free_and_init (att->btids);
	}
    }

  free_and_init (attributes);
}

/*
 * or_free_constraints() - Frees an OR_INDEX structure returned by
 *                       or_get_constraints
 *   return: void
 *   indexes(in/out): index structure
 *   n_indexes(in): number of index
 */
void
or_free_constraints (OR_INDEX * indexes, int n_indexes)
{
  OR_INDEX *index;
  int i;


  for (i = 0, index = indexes; i < n_indexes; i++, index++)
    {
      if (index->atts != NULL)
	{
	  free_and_init (index->atts);
	}

      if (index->btname != NULL)
	{
	  free_and_init (index->btname);
	}

      if (index->asc_desc != NULL)
	{
	  free_and_init (index->asc_desc);
	}
    }

  free_and_init (indexes);
}

/*
 * or_get_attrname () - Find the name of the given attribute
 *   return: attr_name
 *   record(in): disk record
 *   attrid(in): desired attribute
 *
 * Note: The name returned is an actual pointer to the record structure
 *       if the record is changed, the pointer may be trashed.
 *
 *       The name retruned is the name of the actual representation.
 *       If the given attribute identifier does not exist for current
 *       representation, NULL is retruned.
 */
const char *
or_get_attrname (RECDES * record, int attrid)
{
  int n_fixed, n_variable;
  int n_attrs;
  int i, id;
  bool found;
  char *attr_name, *start, *ptr, *attset, *diskatt = NULL;

  start = record->data;

  assert (OR_GET_OFFSET_SIZE (record->data) == BIG_VAR_OFFSET_SIZE);

  ptr = start + OR_FIXED_ATTRIBUTES_OFFSET (ORC_CLASS_VAR_ATT_COUNT);
  attr_name = NULL;

  n_fixed = OR_GET_INT (ptr + ORC_FIXED_COUNT_OFFSET);
  n_variable = OR_GET_INT (ptr + ORC_VARIABLE_COUNT_OFFSET);

  found = false;

  /*
   * INSTANCE ATTRIBUTES
   *
   * find the start of the "set_of(attribute)" fix/variable attribute
   * list inside the class
   */
  attset = start + OR_VAR_OFFSET (start, ORC_ATTRIBUTES_INDEX);
  n_attrs = n_fixed + n_variable;

  for (i = 0, found = false; i < n_attrs && found == false; i++)
    {
      /*
       * diskatt will now be pointing at the offset table for this attribute.
       * this is logically the "start" of this nested object.
       *
       * set ptr to the beginning of the fixed attributes
       */
      diskatt = attset + OR_SET_ELEMENT_OFFSET (attset, i);
      ptr = diskatt + OR_VAR_TABLE_SIZE (ORC_ATT_VAR_ATT_COUNT);
      id = OR_GET_INT (ptr + ORC_ATT_ID_OFFSET);
      if (id == attrid)
	{
	  found = true;
	}
    }

  /*
   * diskatt now points to the attribute that we are interested in.
   Get the attribute name. */
  if (found == true)
    {
      unsigned char len;

      attr_name = (diskatt +
		   OR_VAR_TABLE_ELEMENT_OFFSET (diskatt, ORC_ATT_NAME_INDEX));

      /*
       * kludge kludge kludge
       * This is now an encoded "varchar" string, we need to skip over the
       * length before returning it.  Note that this also depends on the
       * stored string being NULL terminated.
       */
      len = *((unsigned char *) attr_name);
      if (len < 0xFFU)
	{
	  attr_name += 1;
	}
      else
	{
	  attr_name = attr_name + 1 + OR_INT_SIZE;
	}
    }

  return attr_name;
}
