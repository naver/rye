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
 * class_object.c - Class Constructors
 */

#ident "$Id$"

#include "config.h"

#include <stdlib.h>
#include <string.h>

#include "language_support.h"
#include "work_space.h"
#include "object_representation.h"
#include "object_primitive.h"
#include "class_object.h"
#include "boot_cl.h"
#include "locator_cl.h"
#include "authenticate.h"
#include "set_object.h"
#include "object_accessor.h"
#include "parser.h"
#include "schema_manager.h"
#include "dbi.h"

#include "dbval.h"		/* this must be the last header file included */

/* Macro to generate the UNIQUE property string from the components */
#define SM_SPRINTF_UNIQUE_PROPERTY_VALUE(buffer, volid, fileid, pageid) \
  sprintf(buffer, "%d|%d|%d", (int)volid, (int)fileid, (int)pageid)

const int SM_MAX_STRING_LENGTH = 1073741823;	/* 0x3fffffff */

#if 0
static SM_CONSTRAINT_TYPE Constraint_types[] = {
  SM_CONSTRAINT_PRIMARY_KEY,
  SM_CONSTRAINT_UNIQUE,
  SM_CONSTRAINT_INDEX
};

static const char *Constraint_properties[] = {
  SM_PROPERTY_PRIMARY_KEY,
  SM_PROPERTY_UNIQUE,
  SM_PROPERTY_INDEX
};
#endif

#define NUM_CONSTRAINT_TYPES            \
  ((int)(sizeof(Constraint_types)/sizeof(Constraint_types[0])))
#define NUM_CONSTRAINT_PROPERTIES       \
  ((int)(sizeof(Constraint_properties)/sizeof(Constraint_properties[0])))

#if defined (RYE_DEBUG)
static void classobj_print_props (DB_SEQ * properties);
#endif
static SM_CONSTRAINT *classobj_make_constraint (const char *name,
						SM_CONSTRAINT_TYPE type,
						int status, BTID * id);
static void classobj_free_constraint (SM_CONSTRAINT * constraint);
static int classobj_constraint_size (SM_CONSTRAINT * constraint);
static int classobj_cache_not_null_constraints (const char *class_name,
						SM_ATTRIBUTE * attributes,
						SM_CLASS_CONSTRAINT **
						con_ptr);
static int classobj_domain_size (TP_DOMAIN * domain);
static void classobj_filter_attribute_props (DB_SEQ * props);
static int classobj_init_attribute (SM_ATTRIBUTE * src, SM_ATTRIBUTE * dest,
				    int copy);
static void classobj_clear_attribute_value (DB_VALUE * value);
static void classobj_clear_attribute (SM_ATTRIBUTE * att);
static int classobj_attribute_size (SM_ATTRIBUTE * att);
static void classobj_free_repattribute (SM_REPR_ATTRIBUTE * rat);
static int classobj_repattribute_size (void);
static int classobj_representation_size (SM_REPRESENTATION * rep);
static int classobj_query_spec_size (SM_QUERY_SPEC * query_spec);
static void classobj_insert_ordered_attribute (SM_ATTRIBUTE ** attlist,
					       SM_ATTRIBUTE * att);
static SM_REPRESENTATION *classobj_capture_representation (SM_CLASS * class_);
#if defined (ENABLE_UNUSED_FUNCTION)
static void classobj_sort_attlist (SM_ATTRIBUTE ** source);
#endif
static int classobj_copy_attribute_like (DB_CTMPL * ctemplate,
					 SM_ATTRIBUTE * attribute,
					 const char *const like_class_name);
static int classobj_copy_constraint_like (DB_CTMPL * ctemplate,
					  SM_CLASS_CONSTRAINT * constraint,
					  const char *const like_class_name);


/* THREADED ARRAYS */
/*
 * These are used for the representation of the flattened attribute
 * lists.  The structures are maintained contiguously in an array for
 * quick indexing but in addition have a link field at the top so they can be
 * traversed as lists.  This is particularly helpful during class definition
 * and makes it simpler for the class transformer to walk over the structures.
 */

/*
 * classobj_alloc_threaded_array() - Allocates a threaded array and initializes
 * 			       the thread pointers.
 *   return: threaded array
 *   size(in): element size
 *   count(in): number of elements
 */

DB_LIST *
classobj_alloc_threaded_array (int size, int count)
{
  DB_LIST *array, *l;
  char *ptr;
  int i;

  array = NULL;
  if (count)
    {
      array = (DB_LIST *) db_ws_alloc (size * count);
      if (array == NULL)
	{
	  return NULL;
	}

      ptr = (char *) array;
      for (i = 0; i < (count - 1); i++)
	{
	  l = (DB_LIST *) ptr;
	  l->next = (DB_LIST *) (ptr + size);
	  ptr += size;
	}
      l = (DB_LIST *) ptr;
      l->next = NULL;
    }
  return (array);
}

/*
 * classobj_free_threaded_array() - Frees a threaded array and calls a function on
 *    each element to free any storage referenced by the elements.
 *   return: none
 *   array(in): threaded array
 *   clear(in): function to free storage contained in elements
 */

void
classobj_free_threaded_array (DB_LIST * array, LFREEER clear)
{
  DB_LIST *l;

  if (clear != NULL)
    {
      for (l = array; l != NULL; l = l->next)
	{
	  if (clear != NULL)
	    {
	      (*clear) (l);
	    }
	}
    }
  db_ws_free (array);
}

/* PROPERTY LISTS */
/*
 * These are used to maintain random values on class, and attribute
 * definitions.  The values are used infrequently so there is a motiviation
 * for not reserving space for them unconditionally in the main
 * structure body.  They are implemented as sequences of name/value pairs.
 */

/*
 * classobj_make_prop() - Creates an empty property list for a class, attribute
 *   return: an initialized property list
 */

DB_SEQ *
classobj_make_prop ()
{
  return (set_create_sequence (0));
}

/*
 * classobj_free_prop() - Frees a property list for a class, attribute.
 *   return: none
 *   properties(in): a property list
 */

void
classobj_free_prop (DB_SEQ * properties)
{
  if (properties != NULL)
    {
      set_free (properties);
    }
}

/*
 * classobj_put_prop() - This is used to add a new property to a property list.
 *    First the list is searched for a property with the given name.  If the
 *    property already exists, the property value will be replaced with the
 *    new value and a non-zero is returned from the function.
 *    If the property does not exist, it will be added to the end of the
 *    list.
 *   return: index of the replaced property, 0 if not found
 *   properties(in): property list
 *   name(in): property name
 *   pvalue(in): new property value
 */

int
classobj_put_prop (DB_SEQ * properties, const char *name, DB_VALUE * pvalue)
{
  int error;
  int found, max, i;
  DB_VALUE value;
  char *val_str;

  error = NO_ERROR;
  found = 0;


  if (properties == NULL || name == NULL || pvalue == NULL)
    {
      goto error;
    }

  max = set_size (properties);
  for (i = 0; i < max && !found && error == NO_ERROR; i += 2)
    {
      error = set_get_element (properties, i, &value);
      if (error != NO_ERROR)
	{
	  continue;
	}

      if (DB_VALUE_TYPE (&value) != DB_TYPE_VARCHAR ||
	  (val_str = DB_GET_STRING (&value)) == NULL)
	{
	  error = ER_SM_INVALID_PROPERTY;
	}
      else
	{
	  if (strcmp (name, val_str) == 0)
	    {
	      if ((i + 1) >= max)
		{
		  error = ER_SM_INVALID_PROPERTY;
		}
	      else
		{
		  found = i + 1;
		}
	    }
	}
      pr_clear_value (&value);
    }

  if (error == NO_ERROR)
    {
      if (found)
	{
	  set_put_element (properties, found, pvalue);
	}
      else
	{
	  /* start with the property value to avoid growing the array twice */
	  set_put_element (properties, max + 1, pvalue);
	  db_make_string (&value, name);
	  set_put_element (properties, max, &value);
	}
    }

error:
  if (error)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
    }

  return (found);
}

/*
 * classobj_drop_prop() - This removes a property from a property list.
 *    If the property was found, the name and value entries in the sequence
 *    are removed and a non-zero value is returned.  If the property
 *    was not found, the sequence is unchanged and a zero is returned.
 *   return: non-zero if the property was dropped
 *   properties(in): property list
 *   name(in): property name
 */

int
classobj_drop_prop (DB_SEQ * properties, const char *name)
{
  int error;
  int dropped, max, i;
  DB_VALUE value;
  char *val_str;

  error = NO_ERROR;
  dropped = 0;

  if (properties == NULL || name == NULL)
    {
      goto error;
    }

  max = set_size (properties);
  for (i = 0; i < max && !dropped && error == NO_ERROR; i += 2)
    {
      error = set_get_element (properties, i, &value);
      if (error != NO_ERROR)
	{
	  continue;
	}

      if (DB_VALUE_TYPE (&value) != DB_TYPE_VARCHAR ||
	  (val_str = DB_GET_STRING (&value)) == NULL)
	{
	  error = ER_SM_INVALID_PROPERTY;
	}
      else
	{
	  if (strcmp (name, val_str) == 0)
	    {
	      if ((i + 1) >= max)
		{
		  error = ER_SM_INVALID_PROPERTY;
		}
	      else
		{
		  dropped = 1;
		  /* drop the two elements at the found position */
		  set_drop_seq_element (properties, i);
		  set_drop_seq_element (properties, i);
		}
	    }
	}
      pr_clear_value (&value);

    }

error:
  if (error)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
    }

  return (dropped);
}

#if defined (RYE_DEBUG)
/*
 * classobj_print_props() - Debug function to dump a property list to standard out.
 *   return: none
 *   properties(in): property list
 */

static void
classobj_print_props (DB_SEQ * properties)
{
  set_print (properties);
  fprintf (stdout, "\n");
#if 0
  DB_VALUE value;
  int max, i;

  if (properties == NULL)
    {
      return;
    }
  max = set_size (properties);
  if (max)
    {
      for (i = 0; i < max; i++)
	{
	  if (set_get_element (properties, i, &value) != NO_ERROR)
	    fprintf (stdout, "*** error *** ");
	  else
	    {
	      help_fprint_value (stdout, &value);
	      pr_clear_value (&value);
	      fprintf (stdout, " ");
	    }
	}
      fprintf (stdout, "\n");
    }

#endif /* 0 */
}
#endif

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * classobj_map_constraint_to_property() - Return the SM_PROPERTY type corresponding to
 *    the SM_CONSTRAINT_TYPE. This is necessary since we don't store
 *    SM_CONSTRAINT_TYPE's in the property lists.
 *    A NULL is returned if there is not a corresponding SM_PROPERTY type
 *   return: SM_PROPERTY type
 *   constraint(in): constraint type
 */

const char *
classobj_map_constraint_to_property (SM_CONSTRAINT_TYPE constraint)
{
  const char *property_type = NULL;

  switch (constraint)
    {
    case SM_CONSTRAINT_INDEX:
      property_type = SM_PROPERTY_INDEX;
      break;
    case SM_CONSTRAINT_UNIQUE:
      property_type = SM_PROPERTY_UNIQUE;
      break;
    case SM_CONSTRAINT_NOT_NULL:
      property_type = SM_PROPERTY_NOT_NULL;
      break;
    case SM_CONSTRAINT_PRIMARY_KEY:
      property_type = SM_PROPERTY_PRIMARY_KEY;
      break;
    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_SM_INVALID_CONSTRAINT, 0);
      break;
    }

  return property_type;
}
#endif

/*
 * classobj_copy_props() - Copies a property list.  The filter class is optional and
 *    will cause those properties whose origin is the given class to be copied
 *    and others to be filtered out.  This is useful when we want to filter out
 *    inherited properties.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   properties(in): property list to copy
 *   new_properties(out): output property list where the properties are copied to
 */

int
classobj_copy_props (DB_SEQ * properties, DB_SEQ ** new_properties)
{
  int error = NO_ERROR;

  if (properties == NULL)
    {
      return error;
    }

  *new_properties = set_copy (properties);
  if (*new_properties == NULL)
    {
      return er_errid ();
    }

  return error;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * classobj_find_prop_constraint() - This function is used to find and return
 *    a constraint from a class property list.
 *   return: non-zero if property was found
 *   properties(in): Class property list
 *   prop_name(in): Class property name
 *   cnstr_name(in): Class constraint name
 *   cnstr_val(out): Returned class constraint value
 */

int
classobj_find_prop_constraint (DB_SEQ * properties, const char *prop_name,
			       const char *cnstr_name, DB_VALUE * cnstr_val)
{
  DB_VALUE prop_val;
  DB_SEQ *prop_seq;
  int found = 0;

  db_make_null (&prop_val);
  if (classobj_get_prop (properties, prop_name, &prop_val) > 0)
    {
      prop_seq = DB_GET_SEQ (&prop_val);
      found = classobj_get_prop (prop_seq, cnstr_name, cnstr_val);
    }

  pr_clear_value (&prop_val);
  return found;
}

/*
 * classobj_btid_from_property_value() - Little helper function to get a btid out of
 *    a DB_VALUE that was obtained from a property list.
 *    Note that it is still up to the caller to clear this value when they're
 *    done with it.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   value(in): value containing btid property
 *   btid(out): btid we extracted
 */
int
classobj_btid_from_property_value (DB_VALUE * value, BTID * btid)
{
  char *btid_string;
  int error;

  if (DB_VALUE_TYPE (value) != DB_TYPE_VARCHAR)
    {
      goto exit_on_error;
    }

  btid_string = DB_GET_STRING (value);
  if (btid_string == NULL)
    {
      goto exit_on_error;
    }

  error = classobj_decompose_property_btid (btid_string, btid);
  if (error != NO_ERROR)
    {
      goto exit_on_error;
    }

  return NO_ERROR;

exit_on_error:
  BTID_SET_NULL (btid);

  /* should have a more appropriate error for this */
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_SM_INVALID_PROPERTY, 0);
  return ER_SM_INVALID_PROPERTY;
}

/*
 * classobj_oid_from_property_value()
 *   return: NO_ERROR on success, non-zero for ERROR
 *   value(in):
 *   oid(out):
 */

int
classobj_oid_from_property_value (DB_VALUE * value, OID * oid)
{
  char *oid_string;
  int error;

  if (DB_VALUE_TYPE (value) != DB_TYPE_VARCHAR)
    {
      goto exit_on_error;
    }

  oid_string = DB_GET_STRING (value);
  if (oid_string == NULL)
    {
      goto exit_on_error;
    }
  error = classobj_decompose_property_oid (oid_string, oid);
  if (error != NO_ERROR)
    {
      goto exit_on_error;
    }

  return NO_ERROR;

exit_on_error:
  OID_SET_NULL (oid);

  /* should have a more appropriate error for this */
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_SM_INVALID_PROPERTY, 0);
  return ER_SM_INVALID_PROPERTY;
}
#endif

/*
 * classobj_get_cached_index_family() - This is used to find a index constraint of the given
 *    type, and return the ID.  If the constraint list does not contain
 *    a constraint of the requested type, a 0 is returned. Non-zero is returned
 *    if the constraint is found.  The value pointed to by id is only
 *    valid if the function returns non-zero.
 *   return: non-zero if id was found
 *   constraints(in): constraint list
 *   type(in): constraint type
 *   id(out): pointer to ID
 */
int
classobj_get_cached_index_family (SM_CONSTRAINT * constraints,
				  SM_CONSTRAINT_TYPE type, BTID * id)
{
  SM_CONSTRAINT *cnstr;
  int ok = 0;

  assert (SM_IS_CONSTRAINT_INDEX_FAMILY (type));

  for (cnstr = constraints; cnstr != NULL; cnstr = cnstr->next)
    {
      if (cnstr->type != type && cnstr->status == INDEX_STATUS_IN_PROGRESS)
	{
	  continue;
	}

      if (id != NULL)
	{
	  *id = cnstr->index;
	}
      ok = 1;
      break;

    }

  return ok;
}

/*
 * classobj_has_unique_constraint ()
 *   return: true if an unique constraint is contained in the constraint list,
 *           otherwise false.
 *   constraints(in): constraint list
 */
bool
classobj_has_unique_constraint (SM_CONSTRAINT * constraints)
{
  SM_CONSTRAINT *c;

  for (c = constraints; c != NULL; c = c->next)
    {
      if (SM_IS_CONSTRAINT_UNIQUE_FAMILY (c->type))
	{
	  return true;
	}
    }

  return false;
}

/* SM_CONSTRAINT */
/*
 * classobj_make_constraint() - Creates a new constraint node.
 *   return: new constraint structure
 *   name(in): Constraint name
 *   type(in): Constraint type
 *   type(in): Constraint status
 *   id(in): Unique BTID
 */

static SM_CONSTRAINT *
classobj_make_constraint (const char *name, SM_CONSTRAINT_TYPE type,
			  int status, BTID * id)
{
  SM_CONSTRAINT *constraint;

  constraint = (SM_CONSTRAINT *) db_ws_alloc (sizeof (SM_CONSTRAINT));

  if (constraint == NULL)
    {
      return NULL;
    }

  constraint->next = NULL;
  constraint->name = ws_copy_string (name);
  constraint->type = type;
  constraint->status = status;
  constraint->index = *id;

  if (name && !constraint->name)
    {
      classobj_free_constraint (constraint);
      return NULL;
    }

  return constraint;
}

/*
 * classobj_free_constraint() - Frees an constraint structure and all memory
 *    associated with a constraint node.
 *   return: none
 *   constraint(in): Pointer to constraint node
 */

static void
classobj_free_constraint (SM_CONSTRAINT * constraint)
{
  SM_CONSTRAINT *c, *next;

  for (c = constraint, next = NULL; c != NULL; c = next)
    {
      next = c->next;
      if (c->name != NULL)
	{
	  db_ws_free (c->name);
	  c->name = NULL;
	}

      db_ws_free_and_init (c);
    }
}

/*
 * classobj_constraint_size() - Calculates the total number of bytes occupied by
 *                     the constraint list.
 *   return: size of constraint list
 *   constraint(in): Pointer to constraint list
 */

static int
classobj_constraint_size (UNUSED_ARG SM_CONSTRAINT * constraint)
{
  return sizeof (SM_CONSTRAINT);
}

/*
 * classobj_cache_constraints() - Cache the constraint properties from the property
 *    list into the attribute's constraint structure for faster retrieval.
 *   return: true if constraint properties were cached
 *   class(in): Pointer to attribute structure
 */
bool
classobj_cache_constraints (SM_CLASS * class_)
{
  SM_ATTRIBUTE *att;
  SM_DISK_CONSTRAINT *disk_cons;
  SM_DISK_CONSTRAINT_ATTRIBUTE *disk_att;
  SM_CONSTRAINT *att_cons;
  bool ok = true;

  /*
   *  Clear the attribute caches
   */
  for (att = class_->attributes; att != NULL; att = att->next)
    {
      if (att->constraints)
	{
	  classobj_free_constraint (att->constraints);
	  att->constraints = NULL;
	}
    }

  /*
   *  Extract the constraint property and process
   */
  if (class_->disk_constraints == NULL)
    {
      return ok;
    }

  for (disk_cons = class_->disk_constraints; disk_cons != NULL;
       disk_cons = disk_cons->next)
    {
      for (disk_att = disk_cons->disk_info_of_atts; disk_att != NULL;
	   disk_att = disk_att->next)
	{
	  if (disk_att->att_id >= 0)
	    {
	      att = classobj_find_attribute_id (class_->attributes,
						disk_att->att_id);
	    }
	  else
	    {
	      att = classobj_find_attribute (class_->attributes,
					     disk_att->name);
	    }

	  /* att == NULL if drop column */
	  if (att != NULL)
	    {
	      /*
	       *  Add a new constraint node to the cache list
	       */
	      att_cons = classobj_make_constraint (disk_cons->name,
						   disk_cons->type,
						   disk_cons->index_status,
						   &disk_cons->index_btid);
	      if (att_cons == NULL)
		{
		  return false;
		}

	      if (att->constraints == NULL)
		{
		  att->constraints = att_cons;
		}
	      else
		{
		  att_cons->next = att->constraints;
		  att->constraints = att_cons;
		}
	    }
	}
    }

  return ok;
}

/* SM_CLASS_CONSTRAINT */


/*
 * classobj_make_class_constraint() - Allocate and initialize a new constraint
 *    structure. The supplied name is ours to keep, don't make a copy.
 *   return: new constraint structure
 */

SM_CLASS_CONSTRAINT *
classobj_make_class_constraint ()
{
  SM_CLASS_CONSTRAINT *new_;

  /* make a new constraint list entry */
  new_ = (SM_CLASS_CONSTRAINT *) db_ws_alloc (sizeof (SM_CLASS_CONSTRAINT));
  if (new_ == NULL)
    {
      return NULL;
    }

  new_->next = NULL;
  new_->name = NULL;
  new_->type = SM_CONSTRAINT_UNIQUE;
  new_->attributes = NULL;
  new_->asc_desc = NULL;
  BTID_SET_NULL (&new_->index_btid);
  new_->index_status = INDEX_STATUS_INIT;
  new_->num_atts = 0;

  return new_;
}

/*
 * classobj_make_disk_constraint_attribute() - Allocate and initialize
 *              a new disk constraint attribute structure.
 *
 *   return: new disk constraint attribute structure
 */

SM_DISK_CONSTRAINT_ATTRIBUTE *
classobj_make_disk_constraint_attribute ()
{
  SM_DISK_CONSTRAINT_ATTRIBUTE *new_;

  /* make a new disk constraint entry */
  new_ =
    (SM_DISK_CONSTRAINT_ATTRIBUTE *)
    db_ws_alloc (sizeof (SM_DISK_CONSTRAINT_ATTRIBUTE));
  if (new_ == NULL)
    {
      return NULL;
    }

  new_->next = NULL;
  new_->name = NULL;
  new_->att_id = 0;
  new_->asc_desc = 0;

  return new_;
}

/*
 * classobj_free_disk_constraint_attribute() - Free disk constraint attribute structures
 *   return: none
 *   constraint attributes(in): disk constraint attribute list
 */
void
classobj_free_disk_constraint_attribute (SM_DISK_CONSTRAINT_ATTRIBUTE *
					 cons_att)
{
  if (cons_att->name != NULL)
    {
      db_ws_free_and_init (cons_att->name);
    }

  db_ws_free_and_init (cons_att);
}

/*
 * classobj_free_class_constraint() - Free class constraint structures
 *   return: none
 *   cons(in): constraint
 */

void
classobj_free_class_constraint (SM_CLASS_CONSTRAINT * cons)
{
  if (cons->name != NULL)
    {
      ws_free_string (cons->name);
      cons->name = NULL;
    }
  if (cons->attributes != NULL)
    {
      db_ws_free_and_init (cons->attributes);
    }
  if (cons->asc_desc != NULL)
    {
      db_ws_free_and_init (cons->asc_desc);
    }
  db_ws_free_and_init (cons);
}

/*
 * classobj_free_disk_constraint() - Free of disk constraint structures
 *    allocated by classobj_make_disk_constraint.
 *   return: none
 *   disk_cons(in): disk constraint
 */
void
classobj_free_disk_constraint (SM_DISK_CONSTRAINT * disk_cons)
{
  if (disk_cons->disk_info_of_atts != NULL)
    {
      WS_LIST_FREE (disk_cons->disk_info_of_atts,
		    classobj_free_disk_constraint_attribute);
      disk_cons->disk_info_of_atts = NULL;
    }
  if (disk_cons->name != NULL)
    {
      ws_free_string (disk_cons->name);
      disk_cons->name = NULL;
    }

  db_ws_free_and_init (disk_cons);
}

/*
 * classobj_make_class_constraints ()
 *   return: NO_ERROR on success, non-zero for ERROR
 *   class_props(in): class property list
 *   attributes(in):
 *   con_ptr(out):
 */

int
classobj_make_class_constraints (SM_CLASS_CONSTRAINT ** new_list,
				 SM_ATTRIBUTE * attributes,
				 SM_DISK_CONSTRAINT * disk_cons_list)
{

  SM_CLASS_CONSTRAINT *new_cons, *first, *last;
  SM_DISK_CONSTRAINT *disk_cons;
  SM_DISK_CONSTRAINT_ATTRIBUTE *cons_att;
  SM_ATTRIBUTE *att;
  int error = NO_ERROR;
  int i;

  *new_list = NULL;
  first = last = NULL;

  for (disk_cons = disk_cons_list; disk_cons != NULL;
       disk_cons = disk_cons->next)
    {
      new_cons = classobj_make_class_constraint ();
      if (new_cons == NULL)
	{
	  error = er_errid ();

	  GOTO_EXIT_ON_ERROR;
	}

      /* append to list */
      if (first == NULL)
	{
	  first = new_cons;
	}
      else
	{
	  last->next = (SM_CLASS_CONSTRAINT *) new_cons;
	}
      last = new_cons;

      new_cons->name = ws_copy_string (disk_cons->name);
      BTID_COPY (&new_cons->index_btid, &disk_cons->index_btid);
      new_cons->index_status = disk_cons->index_status;
      new_cons->type = disk_cons->type;
      new_cons->num_atts = disk_cons->num_atts;

      new_cons->attributes =
	(SM_ATTRIBUTE **) malloc (sizeof (SM_ATTRIBUTE *) *
				  (new_cons->num_atts + 1));
      if (new_cons->attributes == NULL)
	{
	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  error, 1,
		  sizeof (SM_ATTRIBUTE *) * (new_cons->num_atts + 1));

	  GOTO_EXIT_ON_ERROR;
	}

      new_cons->asc_desc = (int *) malloc (sizeof (int) * new_cons->num_atts);
      if (new_cons->asc_desc == NULL)
	{
	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  error, 1, sizeof (int) * new_cons->num_atts);

	  GOTO_EXIT_ON_ERROR;
	}

      att = NULL;
      for (i = 0, cons_att = disk_cons->disk_info_of_atts;
	   cons_att != NULL; cons_att = cons_att->next, i++)
	{
	  if (cons_att->att_id >= 0)
	    {
	      att = classobj_find_attribute_id (attributes, cons_att->att_id);
	    }
	  else
	    {
	      att = classobj_find_attribute (attributes, cons_att->name);
	    }

	  /* att == NULL if drop column. */
	  new_cons->attributes[i] = att;

	  if (att == NULL)
	    {
	      break;
	    }
	  new_cons->asc_desc[i] = cons_att->asc_desc;
	}
      if (att == NULL)
	{
	  for (i = 0; i < new_cons->num_atts; i++)
	    {
	      new_cons->attributes[i] = NULL;
	    }
	}

      assert (new_cons->num_atts == i);
      new_cons->attributes[i] = NULL;
    }
  *new_list = first;

  return NO_ERROR;

exit_on_error:

  if (first != NULL)
    {
      WS_LIST_FREE (first, classobj_free_class_constraint);
      first = NULL;
    }

  if (error == NO_ERROR)
    {
      assert (error != NO_ERROR);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
    }

  return error;
}

/*
 * classobj_decache_class_constraints() - Removes any cached constraint information
 *                                  from the class.
 *   return: none
 *   class(in): class to ponder
 */

void
classobj_decache_class_constraints (SM_CLASS * class_)
{
  if (class_->constraints != NULL)
    {
      WS_LIST_FREE (class_->constraints, classobj_free_class_constraint);
      class_->constraints = NULL;
    }
}


/*
 * classobj_cache_not_null_constraints() - Cache the NOT NULL constraints from
 *    the attribute list into the CLASS constraint cache.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   class_name(in): Class Name
 *   attributes(in): Pointer to an attribute list.  NOT NULL constraints can
 *               be applied to normal, class attributes.
 *   con_ptr(in/out): Pointer to the class constraint cache.
 */

static int
classobj_cache_not_null_constraints (const char *class_name,
				     SM_ATTRIBUTE * attributes,
				     SM_CLASS_CONSTRAINT ** con_ptr)
{
  SM_ATTRIBUTE *att = NULL;
  SM_CLASS_CONSTRAINT *new_ = NULL;
  SM_CLASS_CONSTRAINT *constraints = NULL;
  SM_CLASS_CONSTRAINT *last = NULL;
  const char *att_names[2];
  char *ws_name = NULL;
  char *constraint_name = NULL;

  /* Set constraints to point to the first node of the constraint cache and
     last to point to the last node. */

  assert (con_ptr != NULL);

  constraints = last = *con_ptr;

  if (last != NULL)
    {
      while (last->next != NULL)
	{
	  last = last->next;
	}
    }

  for (att = attributes; att != NULL; att = att->next)
    {
      if (att->flags & SM_ATTFLAG_NON_NULL)
	{

	  /* Construct a default name for the constraint node.  The constraint
	     name is normally allocated from the heap but we want it stored
	     in the workspace so we'll construct it as usual and then copy
	     it into the workspace before calling classobj_make_class_constraint().
	     After the name is copied into the workspace it can be deallocated
	     from the heap.  The name will be deallocated from the workspace
	     when the constraint node is destroyed.  */
	  att_names[0] = att->name;
	  att_names[1] = NULL;
	  constraint_name = sm_produce_constraint_name (class_name,
							DB_CONSTRAINT_NOT_NULL,
							att_names, NULL,
							NULL);
	  if (constraint_name == NULL)
	    {
	      goto memory_error;
	    }

	  ws_name = ws_copy_string (constraint_name);
	  if (ws_name == NULL)
	    {
	      goto memory_error;
	    }



	  /* Allocate a new class constraint node */
	  new_ = classobj_make_class_constraint ();
	  if (new_ == NULL)
	    {
	      goto memory_error;
	    }
	  new_->name = ws_name;
	  new_->type = SM_CONSTRAINT_NOT_NULL;

	  /* The constraint node now has a pointer to the workspace name so
	     we'll disassociate our local pointer with the string. */
	  ws_name = NULL;

	  /* Add the new constraint node to the list */
	  if (constraints == NULL)
	    {
	      constraints = new_;
	    }
	  else
	    {
	      last->next = new_;
	    }

	  last = new_;

	  /* Allocate an array for the attribute involved in the constraint.
	     The array will always contain one attribute pointer and a
	     terminating NULL pointer. */
	  new_->attributes =
	    (SM_ATTRIBUTE **) db_ws_alloc (sizeof (SM_ATTRIBUTE *) * 2);
	  if (new_->attributes == NULL)
	    {
	      goto memory_error;
	    }

	  new_->attributes[0] = att;
	  new_->attributes[1] = NULL;
	  new_->num_atts = 1;

	  free_and_init (constraint_name);
	}
    }

  *con_ptr = constraints;

  return NO_ERROR;


  /* ERROR PROCESSING */

memory_error:
  WS_LIST_FREE (constraints, classobj_free_class_constraint);
  constraints = NULL;
  if (constraint_name)
    {
      free_and_init (constraint_name);
    }
  if (ws_name)
    {
      db_ws_free (ws_name);
    }

  return er_errid ();

}

/*
 * classobj_cache_class_constraints() - Converts the constraint information stored on
 *    the class's property list into the cache SM_CLASS_CONSTRAINT list in
 *    the class structure. This is way more convenient to deal with than
 *    walking through the property list. Note that modifications to the class
 *    do NOT become persistent.
 *    Need to merge this with the earlier code for SM_CONSTRAINT maintenance
 *    above.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   class(in): class to ponder
 */
int
classobj_cache_class_constraints (SM_CLASS * class_)
{
  int error = NO_ERROR;

  classobj_decache_class_constraints (class_);

  /* Cache the Indexes and Unique constraints found in the property list */
  error = classobj_make_class_constraints (&class_->constraints,
					   class_->attributes,
					   class_->disk_constraints);

  /* The NOT NULL constraints are not in the property lists but are instead
     contained in the SM_ATTRIBUTE structures as flags.  Search through
     the attributes and cache the NOT NULL constraints found. */
  if (error == NO_ERROR)
    {
      error = classobj_cache_not_null_constraints (class_->header.name,
						   class_->attributes,
						   &(class_->constraints));
    }

  return error;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * classobj_find_class_constraint() - Searches a list of class constraint structures
 *    for one with a certain name.  Couldn't we be using nlist for this?
 *   return: constraint
 *   constraints(in): constraint list
 *   type(in):
 *   name(in): name to look for
 */

SM_CLASS_CONSTRAINT *
classobj_find_class_constraint (SM_CLASS_CONSTRAINT * constraints,
				SM_CONSTRAINT_TYPE type, const char *name)
{
  SM_CLASS_CONSTRAINT *con;

  for (con = constraints; con != NULL; con = con->next)
    {
      if ((con->type == type)
	  && (intl_identifier_casecmp (con->name, name) == 0))
	{
	  break;
	}
    }
  return con;
}
#endif

/*
 * classobj_find_cons_index()
 *   return: constraint
 *   cons_list(in): constraint list
 *   name(in): name to look for
 */

SM_CLASS_CONSTRAINT *
classobj_find_constraint_by_name (SM_CLASS_CONSTRAINT * cons_list,
				  const char *name)
{
  SM_CLASS_CONSTRAINT *cons;

  for (cons = cons_list; cons; cons = cons->next)
    {
      if ((SM_IS_CONSTRAINT_INDEX_FAMILY (cons->type))
	  && SM_COMPARE_NAMES (cons->name, name) == 0)
	{
	  break;
	}
    }

  return cons;
}

/*
 * classobj_find_class_index()
 *   return: constraint
 *   class(in):
 *   name(in): name to look for
 */

SM_CLASS_CONSTRAINT *
classobj_find_class_index (SM_CLASS * class_, const char *name)
{
  return classobj_find_constraint_by_name (class_->constraints, name);
}

/*
 * classobj_find_cons_primary_key()
 *   return: constraint
 *   cons_list(in):
 */

SM_CLASS_CONSTRAINT *
classobj_find_cons_primary_key (SM_CLASS_CONSTRAINT * cons_list)
{
  SM_CLASS_CONSTRAINT *cons = NULL;

  for (cons = cons_list; cons; cons = cons->next)
    {
      if (cons->type == SM_CONSTRAINT_PRIMARY_KEY)
	break;
    }

  return cons;
}

/*
 * classobj_find_class_primary_key()
 *   return: constraint
 *   class(in):
 */

SM_CLASS_CONSTRAINT *
classobj_find_class_primary_key (SM_CLASS * class_)
{
  return classobj_find_cons_primary_key (class_->constraints);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * cl_remove_class_constraint() - Drop the constraint node from the class
 *                                constraint cache.
 *   return: none
 *   constraints(in): Pointer to class constraint list
 *   node(in): Pointer to a node in the constraint list
 */

void
classobj_remove_class_constraint_node (SM_CLASS_CONSTRAINT ** constraints,
				       SM_CLASS_CONSTRAINT * node)
{
  SM_CLASS_CONSTRAINT *con = NULL, *next = NULL, *prev = NULL;

  for (con = *constraints; con != NULL; con = next)
    {
      next = con->next;
      if (con != node)
	{
	  prev = con;
	}
      else
	{
	  if (prev == NULL)
	    {
	      *constraints = con->next;
	    }
	  else
	    {
	      prev->next = con->next;
	    }

	  con->next = NULL;
	}
    }
}

/*
 * classobj_class_has_indexes() - Searches the class constraints
 *   return: true if the class contains indexes(INDEX or UNIQUE constraints)
 *   class(in): Class
 */

bool
classobj_class_has_indexes (SM_CLASS * class_)
{
  SM_CLASS_CONSTRAINT *con;
  bool has_index = false;

  has_index = false;
  for (con = class_->constraints; (con != NULL && !has_index);
       con = con->next)
    {
      if (SM_IS_CONSTRAINT_INDEX_FAMILY (con->type))
	{
	  has_index = true;
	}
    }

  return has_index;
}
#endif

/* SM_DOMAIN */

/*
 * classobj_domain_size() - Caclassobj_domain_sizee number of bytes of memory required for
 *    a domain list.
 *   return: byte size of domain list
 *   domain(in): domain list
 */

static int
classobj_domain_size (TP_DOMAIN * domain)
{
  int size;

  size = sizeof (TP_DOMAIN);
  size +=
    ws_list_total ((DB_LIST *) domain->setdomain,
		   (LTOTALER) classobj_domain_size);

  return (size);
}

/* SM_ATTRIBUTE */
/*
 * classobj_make_attribute() - Construct a new attribute structure.
 *   return: attribute structure
 *   name(in): attribute name
 *   type(in): primitive type
 */

SM_ATTRIBUTE *
classobj_make_attribute (const char *name, PR_TYPE * type)
{
  SM_ATTRIBUTE *att;

  att = (SM_ATTRIBUTE *) db_ws_alloc (sizeof (SM_ATTRIBUTE));
  if (att == NULL)
    {
      return NULL;
    }
  att->next = NULL;
  att->name = NULL;
  att->id = -1;
  /* try to start phasing out att->type and instead use att->sma_domain
   * everywhere - jsl
   */
  att->type = type;
  att->sma_domain = NULL;
  att->class_mop = NULL;
  att->offset = 0;
  att->flags = 0;
  att->order = 0;
  att->storage_order = 0;
  att->default_value.default_expr = DB_DEFAULT_NONE;
  /* initial values are unbound */
  db_make_null (&att->default_value.original_value);
  db_make_null (&att->default_value.value);

  att->constraints = NULL;
  att->order_link = NULL;
  att->properties = NULL;

  if (name != NULL)
    {
      att->name = ws_copy_string (name);
      if (att->name == NULL)
	{
	  db_ws_free (att);
	  return NULL;
	}
    }

  return att;
}

/*
 * classobj_filter_attribute_props() - This examines the property list for copied
 *    attribute and removes properties that aren't supposed to be copied as
 *    attributes definitions are flattened.  We could possibly make this
 *    part of classobj_copy_props above.
 *    UNIQUE properties are inheritable but INDEX properties are not.
 *   return: none
 *   properties(in): property list to filter
 */

static void
classobj_filter_attribute_props (DB_SEQ * props)
{
  /* these properties aren't inherited, they must be defined locally */

  classobj_drop_prop (props, SM_PROPERTY_INDEX);
}

/*
 * classobj_init_attribute() - Initializes an attribute using the contents of
 *    another attribute. This is used when an attribute list is flattened
 *    during class definition and the attribute lists are converted into
 *    a threaded array of attributes.
 *    NOTE: External allocations like name & domain may be either copied
 *    or simply have their pointers transfered depending on the value
 *    of the copy flag.
 *    NOTE: Be careful not to touch the "next" field here since it may
 *    have been already initialized as part of a threaded array.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   src(in): source attribute
 *   dest(out): destination attribute
 *   copy(in): copy flag (non-zero to copy)
 */

static int
classobj_init_attribute (SM_ATTRIBUTE * src, SM_ATTRIBUTE * dest, int copy)
{
  int error = NO_ERROR;

  dest->name = NULL;
  dest->id = src->id;		/* correct ? */
  dest->type = src->type;
  dest->class_mop = src->class_mop;
  dest->offset = src->offset;
  dest->flags = src->flags;
  dest->order = src->order;
  dest->storage_order = src->storage_order;
  dest->order_link = NULL;	/* can never be copied */
  dest->constraints = NULL;
  dest->sma_domain = NULL;
  dest->properties = NULL;
  DB_MAKE_NULL (&(dest->default_value.original_value));
  DB_MAKE_NULL (&(dest->default_value.value));
  dest->default_value.default_expr = src->default_value.default_expr;

  if (copy)
    {
      if (src->name != NULL)
	{
	  dest->name = ws_copy_string (src->name);
	  if (dest->name == NULL)
	    {
	      goto memory_error;
	    }
	}
      if (src->sma_domain != NULL)
	{
	  assert (TP_DOMAIN_TYPE (src->sma_domain) != DB_TYPE_VARIABLE);
	  dest->sma_domain = tp_domain_copy (src->sma_domain);
	  if (dest->sma_domain == NULL)
	    {
	      goto memory_error;
	    }

	  dest->sma_domain = tp_domain_cache (dest->sma_domain);
	}
      if (src->properties != NULL)
	{
	  error = classobj_copy_props (src->properties, &(dest->properties));
	  if (error != NO_ERROR)
	    {
	      goto memory_error;
	    }
	}

      /* remove the properties that can't be inherited */
      classobj_filter_attribute_props (dest->properties);

      if (src->constraints != NULL)
	{
	  /*
	   *  We used to just copy the unique BTID from the source to the
	   *  destination.  We might want to copy the src cache to dest, or
	   *  maybe regenerate the cache for dest since the information is
	   *  already in its property list.  - JB
	   */
	}

      /* make a copy of the default value */
      if (pr_clone_value
	  (&src->default_value.value, &dest->default_value.value))
	{
	  goto memory_error;
	}

      if (pr_clone_value (&src->default_value.original_value,
			  &dest->default_value.original_value))
	{
	  goto memory_error;
	}
    }
  else
    {
      dest->name = src->name;
      dest->constraints = src->constraints;
      dest->properties = src->properties;
      src->name = NULL;
      src->constraints = NULL;
      src->properties = NULL;

      /* Note that we don't clear the source domain here since it must
       * be cached at this point.  We keep the src->sma_domain around until the
       * attribute is freed in case it is needed for something related
       * to the default values, etc.
       */
      assert (TP_DOMAIN_TYPE (src->sma_domain) != DB_TYPE_VARIABLE);
      dest->sma_domain = src->sma_domain;

      /*
       * do structure copies on the values and make sure the sources
       * get cleared
       */
      dest->default_value.value = src->default_value.value;
      dest->default_value.original_value = src->default_value.original_value;

      db_value_put_null (&src->default_value.value);
      db_value_put_null (&src->default_value.original_value);
    }

  return NO_ERROR;

memory_error:
  /* Could try to free the partially allocated things.  If we get
     here then we're out of virtual memory, a few leaks aren't
     going to matter much. */

  if (dest->sma_domain != NULL)
    {
      assert (TP_DOMAIN_TYPE (dest->sma_domain) != DB_TYPE_VARIABLE);
      tp_domain_free (dest->sma_domain);
    }

  return er_errid ();
}

/*
 * classobj_copy_attribute() - Copies an attribute.
 *    The alias if provided will override the attribute name.
 *   return: new attribute
 *   src(in): source attribute
 *   alias(in): alias name (can be NULL)
 */

SM_ATTRIBUTE *
classobj_copy_attribute (SM_ATTRIBUTE * src, const char *alias)
{
  SM_ATTRIBUTE *att;

  att = (SM_ATTRIBUTE *) db_ws_alloc (sizeof (SM_ATTRIBUTE));
  if (att == NULL)
    {
      return NULL;
    }
  att->next = NULL;

  /* make a unique copy */
  if (classobj_init_attribute (src, att, 1))
    {
      db_ws_free (att);
      return NULL;
    }

  if (alias != NULL)
    {
      ws_free_string (att->name);
      att->name = ws_copy_string (alias);
      if (att->name == NULL)
	{
	  db_ws_free (att);
	  return NULL;
	}
    }

  return (att);
}

/*
 * classobj_copy_att_ordered_list() - Copies an attribute list.  This does NOT return a
 *    threaded array.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   attlist(in): attribute list
 *   copy_ptr(out): new attribute list
 */

int
classobj_copy_att_ordered_list (SM_ATTRIBUTE * attlist,
				SM_ATTRIBUTE ** copy_ptr)
{
  SM_ATTRIBUTE *att, *new_, *first, *last, *next;
  int error = NO_ERROR;

  first = last = NULL;

  for (att = attlist, next = NULL; att != NULL; att = next)
    {
      next = att->order_link;

      new_ = classobj_copy_attribute (att, NULL);
      if (new_ == NULL)
	{
	  goto memory_error;
	}

      if (first == NULL)
	{
	  first = new_;
	}
      else
	{
	  last->next = new_;
	}
      last = new_;
    }

  *copy_ptr = first;
  return NO_ERROR;

memory_error:
  /* Could try to free the previously copied attribute list. We're
     out of virtual memory at this point.  A few leaks aren't going
     to matter. */
  if (first != NULL)
    {
      WS_LIST_FREE (first, classobj_free_attribute);
      first = NULL;
    }

  error = er_errid ();
  if (error == NO_ERROR)
    {
      assert (false);
      error = ER_FAILED;
    }

  return error;
}

/*
 * classobj_copy_constraint_attribute() - Copies an constraint attribute.
 *
 *   return: new constraint attribute
 *
 *   src(in): source constraint attribute
 */

SM_DISK_CONSTRAINT_ATTRIBUTE *
classobj_copy_disk_constraint_attribute (SM_DISK_CONSTRAINT_ATTRIBUTE * src)
{
  SM_DISK_CONSTRAINT_ATTRIBUTE *dest;
  int error = NO_ERROR;

  dest = classobj_make_disk_constraint_attribute ();
  if (dest == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      sizeof (SM_DISK_CONSTRAINT_ATTRIBUTE));

      goto exit_on_error;
    }
  dest->next = NULL;

  dest->name = ws_copy_string (src->name);
  dest->att_id = src->att_id;
  dest->asc_desc = src->asc_desc;

  return dest;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (error != NO_ERROR);
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
    }

  if (dest != NULL)
    {
      db_ws_free (dest);
      dest = NULL;
    }

  return NULL;
}

/*
 * classobj_put_disk_constraint()
 *
 *   return: NO_ERROR or error code
 *
 *   disk_constraints(in/out):
 *   src_cons(in):
 */
int
classobj_put_disk_constraint (SM_DISK_CONSTRAINT ** disk_cons_list,
			      SM_CLASS_CONSTRAINT * src_cons)
{
  SM_DISK_CONSTRAINT *cons_list, *prev_cons, *found_cons, *new_disk_cons;
  SM_DISK_CONSTRAINT_ATTRIBUTE *att, *first, *last;
  int error = NO_ERROR;
  int i;

  /* init local */
  new_disk_cons = NULL;
  first = last = NULL;
  prev_cons = found_cons = NULL;

  new_disk_cons = classobj_make_disk_constraint ();
  if (new_disk_cons == NULL)
    {
      error = er_errid ();
      GOTO_EXIT_ON_ERROR;
    }

  new_disk_cons->name = ws_copy_string (src_cons->name);
  if (new_disk_cons->name == NULL)
    {
      error = er_errid ();
      GOTO_EXIT_ON_ERROR;
    }

  BTID_COPY (&new_disk_cons->index_btid, &src_cons->index_btid);
  new_disk_cons->index_status = src_cons->index_status;
  new_disk_cons->type = src_cons->type;
  new_disk_cons->num_atts = src_cons->num_atts;

  for (i = 0; src_cons->attributes[i] != NULL; i++)
    {
      att = classobj_make_disk_constraint_attribute ();
      if (att == NULL)
	{
	  error = er_errid ();
	  GOTO_EXIT_ON_ERROR;
	}

      att->asc_desc = src_cons->asc_desc[i];

      if (src_cons->attributes[i]->name != NULL)
	{
	  att->name = ws_copy_string (src_cons->attributes[i]->name);
	}
      assert (src_cons->attributes[i]->id >= 0);
      att->att_id = src_cons->attributes[i]->id;


      if (first == NULL)
	{
	  first = att;
	  last = att;
	}
      else
	{
	  assert (last != NULL);
	  last->next = att;
	  last = att;
	}
    }
  assert (i == src_cons->num_atts);

  new_disk_cons->disk_info_of_atts = first;

  prev_cons = NULL;
  for (cons_list = *disk_cons_list; cons_list != NULL;
       cons_list = cons_list->next)
    {
      if (strcmp (cons_list->name, src_cons->name) == 0)
	{
	  found_cons = cons_list;
	  break;
	}
      prev_cons = cons_list;
    }

  if (found_cons != NULL)
    {
      /* replace disk constraint */

      /* remove found_cons */
      new_disk_cons->next = found_cons->next;
      found_cons->next = NULL;

      if (prev_cons == NULL)
	{
	  /* found first */
	  assert (*disk_cons_list == found_cons);

	  *disk_cons_list = new_disk_cons;
	}
      else
	{
	  assert (prev_cons->next == found_cons);

	  prev_cons->next = new_disk_cons;
	}

      classobj_free_disk_constraint (found_cons);
      found_cons = NULL;
    }
  else
    {
      /* append disk constraint */
      if (*disk_cons_list == NULL)
	{
	  *disk_cons_list = new_disk_cons;
	}
      else
	{
	  /* prev_cons is last of list */
	  assert (prev_cons != NULL && prev_cons->next == NULL);

	  /* append last */
	  prev_cons->next = new_disk_cons;
	}
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

  if (new_disk_cons != NULL)
    {
      if (new_disk_cons->disk_info_of_atts == first)
	{
	  first = NULL;		/* keep out double free */
	}

      classobj_free_disk_constraint (new_disk_cons);
      new_disk_cons = NULL;
    }

  if (first != NULL)
    {
      WS_LIST_FREE (first, db_ws_free);
      first = NULL;
    }

  return error;
}

/*
 * classobj_copy_disk_constraint() - Copies an disk constraint.
 *
 *   return: new disk constraint
 *
 *   src(in): source disk constraint
 */
SM_DISK_CONSTRAINT *
classobj_copy_disk_constraint (SM_DISK_CONSTRAINT * src)
{
  SM_DISK_CONSTRAINT *dest;
  int error = NO_ERROR;

  dest = classobj_make_disk_constraint ();
  if (dest == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      sizeof (SM_CLASS_CONSTRAINT));

      goto exit_on_error;
    }

  dest->name = ws_copy_string (src->name);
  if (dest->name == NULL)
    {
      error = er_errid ();

      goto exit_on_error;
    }

  BTID_COPY (&dest->index_btid, &src->index_btid);
  dest->index_status = src->index_status;
  dest->type = src->type;
  dest->num_atts = src->num_atts;

  if (src->disk_info_of_atts != NULL)
    {
      assert (src->num_atts > 0);

      dest->disk_info_of_atts =
	(SM_DISK_CONSTRAINT_ATTRIBUTE *) WS_LIST_COPY (src->disk_info_of_atts,
						       classobj_copy_disk_constraint_attribute,
						       db_ws_free);
      if (dest->disk_info_of_atts == NULL)
	{
	  error = er_errid ();

	  goto exit_on_error;
	}
    }

  assert (dest != NULL);
  return dest;

exit_on_error:

  if (error == NO_ERROR)
    {
      assert (error != NO_ERROR);
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
    }

  if (dest != NULL)
    {
      classobj_free_disk_constraint (dest);
      dest = NULL;
    }

  return NULL;
}

/*
 * classobj_clear_attribute_value() - This gets rid of storage for a DB_VALUE attached
 *    to a class. This is a kludge primarily for the handling of set handles.
 *    In normal db_value_clear() semantics, we would simply end up calling
 *    set_free() on the set reference.
 *    set_free() checks the ownership if the reference and will not free
 *    the underlying set object if it is owned.  Also, it won't free the
 *    reference or set if the reference count is >1.
 *    Here its a bit different since the class completely in charge of
 *    how the storage for this set is managed.
 *    We go around the usual db_value_clear() rules to make sure the
 *    set gets freed.
 *    As always, the handling of set memory needs to be cleaned up.
 *   return: none
 *   value(in/out): value to clear
 */

static void
classobj_clear_attribute_value (DB_VALUE * value)
{
  SETREF *ref;

  if (!DB_IS_NULL (value) && TP_IS_SET_TYPE (DB_VALUE_TYPE (value)))
    {
      /* get directly to the set */
      ref = DB_GET_SET (value);
      if (ref != NULL)
	{
	  /* always free the underlying set object */
	  if (ref->set != NULL)
	    {
	      setobj_free (ref->set);
	      ref->set = NULL;
	    }

	  /* now free the reference, if the counter goes to zero its freed
	     otherwise, it gets left dangling but at least we've free the
	     set storage at this point */
	  set_free (ref);
	}
    }
  else
    {
      /* clear it the usual way */
      pr_clear_value (value);
    }
}

/*
 * classobj_clear_attribute() - Deallocate storage associated with an attribute.
 *    Note that this doesn't deallocate the attribute structure itself since
 *    this may be part of a threaded array.
 *   return: none
 *   att(in/out): attribute
 */

static void
classobj_clear_attribute (SM_ATTRIBUTE * att)
{
  if (att == NULL)
    {
      return;
    }
  if (att->name != NULL)
    {
      ws_free_string (att->name);
      att->name = NULL;
    }
  if (att->constraints != NULL)
    {
      classobj_free_constraint (att->constraints);
      att->constraints = NULL;
    }
  if (att->properties != NULL)
    {
      classobj_free_prop (att->properties);
      att->properties = NULL;
    }
  classobj_clear_attribute_value (&att->default_value.value);
  classobj_clear_attribute_value (&att->default_value.original_value);

  /* Do this last in case we needed it for default value maintenance or something.
   * This probably isn't necessary, the domain should have been cached at
   * this point ?
   */
  if (att->sma_domain != NULL)
    {
      assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);
      tp_domain_free (att->sma_domain);
      att->sma_domain = NULL;
    }
}

/*
 * classobj_free_attribute() - Frees an attribute structure and all memory
 *    associated with the attribute.
 *   return: none
 *   att(in): attribute
 */

void
classobj_free_attribute (SM_ATTRIBUTE * att)
{
  if (att != NULL)
    {
      classobj_clear_attribute (att);
      db_ws_free (att);
    }
}

/*
 * classobj_attribute_size() - Calculates the number of bytes required for the
 *    memory representation of an attribute.
 *   return: byte size of attribute
 *   att(in): attribute
 */

static int
classobj_attribute_size (SM_ATTRIBUTE * att)
{
  int size;

  assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

  size = sizeof (SM_ATTRIBUTE);
  /* this can be NULL only for attributes used in an old representation */
  if (att->name != NULL)
    {
      size += strlen (att->name) + 1;
    }
  size +=
    ws_list_total ((DB_LIST *) att->sma_domain,
		   (LTOTALER) classobj_domain_size);
  size += pr_value_mem_size (&att->default_value.value);
  size += pr_value_mem_size (&att->default_value.original_value);

  if (att->constraints != NULL)
    {
      size +=
	ws_list_total ((DB_LIST *) att->constraints,
		       (LTOTALER) classobj_constraint_size);
    }

  /* need to add in property set */

  return (size);
}

/* SM_REPR_ATTRIBUTE */
/*
 * classobj_make_repattribute() - Creates a new representation attribute structure.
 *   return: new repattribute structure
 *   attid(in): attribute id
 *   typeid(in): type id
 *   domain(in):
 */

SM_REPR_ATTRIBUTE *
classobj_make_repattribute (int attid, DB_TYPE type_id, TP_DOMAIN * domain)
{
  SM_REPR_ATTRIBUTE *rat;

  rat = (SM_REPR_ATTRIBUTE *) db_ws_alloc (sizeof (SM_REPR_ATTRIBUTE));
  if (rat == NULL)
    {
      return NULL;
    }

  rat->next = NULL;
  rat->attid = attid;
  rat->typeid_ = type_id;
  /* think about consolidating the typeid & domain fields */
  rat->domain = domain;

  return (rat);
}

/*
 * classobj_free_repattribute() - Frees storage for a representation attribute.
 *   return: none
 *   rat(in): representation attribute
 */

static void
classobj_free_repattribute (SM_REPR_ATTRIBUTE * rat)
{
  if (rat != NULL)
    {
      db_ws_free (rat);
    }
}

/*
 * classobj_repattribute_size() - memory size of a representation attribute.
 *   return: byte size of attribute
 */

static int
classobj_repattribute_size (void)
{
  int size = sizeof (SM_REPR_ATTRIBUTE);

  return (size);
}

/* SM_REPRESENTATION */
/*
 * classobj_make_representation() - Create a new representation structure.
 *   return: new representation
 */

SM_REPRESENTATION *
classobj_make_representation ()
{
  SM_REPRESENTATION *rep;

  rep = (SM_REPRESENTATION *) db_ws_alloc (sizeof (SM_REPRESENTATION));

  if (rep == NULL)
    {
      return NULL;
    }
  rep->next = NULL;
  rep->id = -1;
  rep->fixed_count = 0;
  rep->variable_count = 0;
  rep->attributes = NULL;

  return (rep);
}

/*
 * classobj_free_representation() - Free a representation structure and any
 *                            associated memory.
 *   return: none
 *   rep(in): representation
 */

void
classobj_free_representation (SM_REPRESENTATION * rep)
{
  if (rep != NULL)
    {
      WS_LIST_FREE (rep->attributes, classobj_free_repattribute);
      db_ws_free (rep);
    }
}

/*
 * classobj_representation_size() - memory storage used by a representation.
 *   return: byte size of representation
 *   rep(in): representation strcuture
 */

static int
classobj_representation_size (SM_REPRESENTATION * rep)
{
  SM_REPR_ATTRIBUTE *rat;
  int size;

  size = sizeof (SM_REPRESENTATION);
  for (rat = rep->attributes; rat != NULL; rat = rat->next)
    {
      size += classobj_repattribute_size ();
    }

  return (size);
}

/* SM_QUERY_SPEC */
/*
 * classobj_make_query_spec() - Allocate and initialize a query_spec structure.
 *   return: new query_spec structure
 *   specification(in): query_spec string
 */

SM_QUERY_SPEC *
classobj_make_query_spec (const char *specification)
{
  SM_QUERY_SPEC *query_spec;

  query_spec = (SM_QUERY_SPEC *) db_ws_alloc (sizeof (SM_QUERY_SPEC));

  if (query_spec == NULL)
    {
      return NULL;
    }

  query_spec->next = NULL;
  query_spec->specification = NULL;

  if (specification != NULL)
    {
      query_spec->specification = ws_copy_string (specification);
      if (query_spec->specification == NULL)
	{
	  db_ws_free (query_spec);
	  query_spec = NULL;
	}
    }

  return (query_spec);
}

/*
 * classobj_copy_query_spec() - Copy SM_QUERY_SPEC structures.
 *   return: new
 *   query_spec(in): source
 */

SM_QUERY_SPEC *
classobj_copy_query_spec (SM_QUERY_SPEC * query_spec)
{
  SM_QUERY_SPEC *new_;

  new_ = classobj_make_query_spec (query_spec->specification);
  if (new_ == NULL)
    {
      goto memory_error;
    }

  return new_;

memory_error:
  return NULL;
}

/*
 * classobj_free_query_spec() - Frees storage for a query_spec specification and
 *                        any associated memory.
 *   return: none
 *   query_spec(in): query_spec structure to free
 */

void
classobj_free_query_spec (SM_QUERY_SPEC * query_spec)
{
  if (query_spec != NULL)
    {
      if (query_spec->specification != NULL)
	{
	  ws_free_string (query_spec->specification);
	}
      db_ws_free (query_spec);
    }
}

/*
 * classobj_query_spec_size() - Calculates the amount of storage used by
 *                     a query_spec structure.
 *   return: byte size of query_spec
 *   query_spec(in): query_spec structure
 */

static int
classobj_query_spec_size (SM_QUERY_SPEC * query_spec)
{
  int size;

  size = sizeof (SM_QUERY_SPEC);
  size += strlen (query_spec->specification) + 1;

  return (size);
}

/* SM_TEMPLATE */
/*
 * classobj_free_template() - Frees a class template and any associated memory.
 *   return: none
 *   template(in): class editing template
 */

void
classobj_free_template (SM_TEMPLATE * template_ptr)
{
  if (template_ptr == NULL)
    {
      return;
    }

  WS_LIST_FREE (template_ptr->attributes, classobj_free_attribute);
  template_ptr->attributes = NULL;

  WS_LIST_FREE (template_ptr->instance_attributes, classobj_free_attribute);
  template_ptr->instance_attributes = NULL;

  WS_LIST_FREE (template_ptr->query_spec, classobj_free_query_spec);
  template_ptr->query_spec = NULL;

  ws_free_string (template_ptr->loader_commands);
  template_ptr->loader_commands = NULL;

  ws_free_string (template_ptr->name);
  template_ptr->name = NULL;

  WS_LIST_FREE (template_ptr->disk_constraints,
		classobj_free_disk_constraint);
  template_ptr->disk_constraints = NULL;

  free_and_init (template_ptr);
}

/*
 * classobj_make_template() - Allocates and initializes a class editing template.
 *    The class MOP and structure are optional, it supplied the template
 *    will be initialized with the contents of the class.  If not supplied
 *    the template will be empty.
 *   return: new template
 *   name(in): class name
 *   op(in): class MOP
 *   class(in): class structure
 */

SM_TEMPLATE *
classobj_make_template (const char *name, MOP op, SM_CLASS * class_)
{
  SM_TEMPLATE *template_ptr;
  int error = NO_ERROR;

  template_ptr = (SM_TEMPLATE *) malloc (sizeof (SM_TEMPLATE));
  if (template_ptr == NULL)
    {
      return NULL;
    }

  template_ptr->class_type = SM_CLASS_CT;
  template_ptr->op = op;
  template_ptr->current = class_;
  template_ptr->tran_index = tm_Tran_index;
  template_ptr->name = NULL;
  template_ptr->attributes = NULL;
  template_ptr->loader_commands = NULL;
  template_ptr->query_spec = NULL;
  template_ptr->instance_attributes = NULL;
  template_ptr->disk_constraints = NULL;

  if (name != NULL)
    {
      template_ptr->name = ws_copy_string (name);
      if (template_ptr->name == NULL)
	{
	  goto memory_error;
	}
    }

  if (class_ != NULL)
    {
      template_ptr->class_type = class_->class_type;

      if (classobj_copy_att_ordered_list (class_->ordered_attributes,
					  &template_ptr->attributes))
	{
	  goto memory_error;
	}

      if (class_->loader_commands != NULL)
	{
	  template_ptr->loader_commands =
	    ws_copy_string (class_->loader_commands);
	  if (template_ptr->loader_commands == NULL)
	    {
	      goto memory_error;
	    }
	}
      if (class_->query_spec)
	{
	  template_ptr->query_spec = (SM_QUERY_SPEC *)
	    WS_LIST_COPY (class_->query_spec, classobj_copy_query_spec,
			  classobj_free_query_spec);
	  if (template_ptr->query_spec == NULL)
	    {
	      goto memory_error;
	    }
	}
      if (class_->constraints != NULL)
	{
	  template_ptr->disk_constraints = (SM_DISK_CONSTRAINT *)
	    WS_LIST_COPY (class_->disk_constraints,
			  classobj_copy_disk_constraint,
			  classobj_free_disk_constraint);
	  if (error != NO_ERROR)
	    {
	      goto memory_error;
	    }
	}

    }

  return (template_ptr);

memory_error:
  if (template_ptr != NULL)
    {
      classobj_free_template (template_ptr);
    }

  return NULL;
}

/*
 * classobj_make_template_like() - Allocates and initializes a class template
 *                                 based on an existing class.
 *    The existing class attributes and constraints are duplicated so that the
 *    new template can be used for the "CREATE LIKE" statement.
 *    Indexes cannot be duplicated by this function because class templates
 *    don't allow index creation. The indexes will be duplicated after the class
 *    is created.
 *   return: the new template
 *   name(in): the name of the new class
 *   class(in): class structure to duplicate
 */

SM_TEMPLATE *
classobj_make_template_like (const char *name, SM_CLASS * class_)
{
  SM_TEMPLATE *template_ptr;
  const char *existing_name = NULL;
  SM_ATTRIBUTE *a;
  SM_CLASS_CONSTRAINT *c;

  assert (name != NULL);
  assert (class_ != NULL);
  assert (class_->class_type == SM_CLASS_CT);
  assert (class_->query_spec == NULL);

  existing_name = class_->header.name;

  template_ptr = smt_def_class (name);
  if (template_ptr == NULL)
    {
      return NULL;
    }

  if (class_->attributes != NULL)
    {
      for (a = class_->ordered_attributes; a != NULL; a = a->order_link)
	{
	  if (classobj_copy_attribute_like (template_ptr, a,
					    existing_name) != NO_ERROR)
	    {
	      goto error_exit;
	    }
	}
    }

  if (class_->constraints != NULL)
    {
      for (c = class_->constraints; c; c = c->next)
	{
	  if (SM_IS_CONSTRAINT_INDEX_FAMILY (c->type))
	    {
	      if (classobj_copy_constraint_like (template_ptr, c,
						 existing_name) != NO_ERROR)
		{
		  goto error_exit;
		}
	    }
	  else
	    {
	      /* NOT NULL have already been copied by classobj_copy_attribute_like.
	         INDEX will be duplicated after the class is created. */
	      assert (c->type == SM_CONSTRAINT_NOT_NULL);
	    }
	}
    }

  return template_ptr;

error_exit:
  if (template_ptr != NULL)
    {
      classobj_free_template (template_ptr);
    }

  return NULL;
}

/*
 * classobj_copy_attribute_like() - Copies an attribute from an existing class
 *                                  to a new class template.
 *    Potential NOT NULL constraints on the attribute are copied also.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   ctemplate(in): the template to copy to
 *   attribute(in): the attribute to be duplicated
 *   like_class_name(in): the name of the class that is duplicated
 */

static int
classobj_copy_attribute_like (DB_CTMPL * ctemplate, SM_ATTRIBUTE * attribute,
			      UNUSED_ARG const char *const like_class_name)
{
  int error = NO_ERROR;
  const char *names[2];
  bool is_shard_key = false;

  assert (TP_DOMAIN_TYPE (attribute->sma_domain) != DB_TYPE_VARIABLE);

  assert (like_class_name != NULL);

  if (attribute->flags & SM_ATTFLAG_SHARD_KEY)
    {
      is_shard_key = true;
    }

  error = smt_add_attribute_w_dflt (ctemplate, attribute->name, NULL,
				    attribute->sma_domain,
				    &attribute->default_value.value,
				    attribute->default_value.default_expr,
				    is_shard_key);
  if (error != NO_ERROR)
    {
      return error;
    }

  if (attribute->flags & SM_ATTFLAG_NON_NULL)
    {
      names[0] = attribute->name;
      names[1] = NULL;
      error =
	dbt_add_constraint (ctemplate, DB_CONSTRAINT_NOT_NULL, NULL, names);
      if (error != NO_ERROR)
	{
	  return error;
	}
    }

  return error;
}

/*
 * classobj_point_at_att_names() - Allocates a NULL-terminated array of pointers
 *                                 to the names of the attributes referenced in
 *                                 a constraint.
 *   return: the array on success, NULL on error
 *   constraint(in): the constraint
 *   count_ref(out): if supplied, the referenced integer will be modified to
 *                   contain the number of attributes
 */

const char **
classobj_point_at_att_names (SM_CLASS_CONSTRAINT * constraint, int *count_ref)
{
  const char **att_names = NULL;
  SM_ATTRIBUTE **attribute_p = NULL;
  int count;
  int i;

  for (attribute_p = constraint->attributes, count = 0;
       *attribute_p; ++attribute_p)
    {
      ++count;
    }
  att_names = (const char **) malloc ((count + 1) * sizeof (const char *));
  if (att_names == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      (count + 1) * sizeof (const char *));
      return NULL;
    }
  for (attribute_p = constraint->attributes, i = 0;
       *attribute_p != NULL; ++attribute_p, ++i)
    {
      att_names[i] = (*attribute_p)->name;
    }
  att_names[i] = NULL;

  if (count_ref != NULL)
    {
      *count_ref = count;
    }
  return att_names;
}

/*
 * classobj_copy_constraint_like() - Copies a constraint from an existing
 *                                   class to a new class template.
 *    Constraint names are copied as they are, even if they are the defaults
 *    given to unnamed constraints. The default names will be a bit misleading
 *    since they will have the duplicated class name in their contents. MySQL
 *    also copies the default name for indexes.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   ctemplate(in): the template to copy to
 *   constraint(in): the constraint to be duplicated
 *   like_class_name(in): the name of the class that is duplicated
 */

static int
classobj_copy_constraint_like (DB_CTMPL * ctemplate,
			       SM_CLASS_CONSTRAINT * constraint,
			       UNUSED_ARG const char *const like_class_name)
{
  int error = NO_ERROR;
  DB_CONSTRAINT_TYPE constraint_type = db_constraint_type (constraint);
  const char **att_names = NULL;
  const char **ref_attrs = NULL;
  int count = 0;
  const char *new_cons_name = NULL;

  assert (like_class_name != NULL);

  assert (SM_IS_CONSTRAINT_INDEX_FAMILY (constraint_type));

  new_cons_name = constraint->name;
  if (new_cons_name == NULL)
    {
      assert (false);

      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");

      return ER_GENERIC_ERROR;
    }
  att_names = classobj_point_at_att_names (constraint, &count);
  if (att_names == NULL)
    {
      return er_errid ();
    }

  error = smt_add_constraint (ctemplate, constraint_type,
			      new_cons_name, att_names, constraint->asc_desc);

  free_and_init (att_names);

  return error;

#if 0
error_exit:
#endif

  if (att_names != NULL)
    {
      free_and_init (att_names);
    }

  if (ref_attrs != NULL)
    {
      free_and_init (ref_attrs);
    }

  return error;
}

/* SM_CLASS */
/*
 * classobj_make_class() - Creates a new class structure.
 *   return: new class structure
 *   name(in): class name
 */

SM_CLASS *
classobj_make_class (const char *name)
{
  SM_CLASS *class_;

  class_ = (SM_CLASS *) db_ws_alloc (sizeof (SM_CLASS));
  if (class_ == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (SM_CLASS));

      return NULL;
    }

  class_->class_type = SM_CLASS_CT;
  class_->header.type = Meta_class;
  class_->header.name = NULL;
  /* shouldn't know how to initialize these, either need external init function */
  HFID_SET_NULL (&class_->header.heap);
  class_->header.heap.vfid.volid = boot_User_volid;

  class_->repid = 0;		/* initial rep is zero */
  class_->representations = NULL;

  class_->object_size = 0;
  class_->att_count = 0;
  class_->attributes = NULL;
  class_->ordered_attributes = NULL;

  class_->query_spec = NULL;
  class_->loader_commands = NULL;

  class_->fixed_count = 0;
  class_->variable_count = 0;
  class_->fixed_size = 0;

  class_->post_load_cleanup = 0;

  class_->att_ids = 0;

  class_->new_ = NULL;
  class_->stats = NULL;
  class_->owner = NULL;
  class_->auth_cache = NULL;
  class_->flags = 0;

  class_->disk_constraints = NULL;
  class_->virtual_cache_schema_id = 0;
  class_->virtual_query_cache = NULL;
  class_->constraints = NULL;

  if (name != NULL)
    {
      class_->header.name = ws_copy_string (name);
      if (class_->header.name == NULL)
	{
	  db_ws_free (class_);
	  class_ = NULL;
	}
    }

  return (class_);
}

/*
 * classobj_free_class() - Frees a class and any associated memory.
 *   return: none
 *   class(in): class structure
 */

void
classobj_free_class (SM_CLASS * class_)
{
  if (class_ == NULL)
    {
      return;
    }

  ws_free_string (class_->header.name);
  ws_free_string (class_->loader_commands);

  WS_LIST_FREE (class_->representations, classobj_free_representation);
  WS_LIST_FREE (class_->query_spec, classobj_free_query_spec);

  classobj_free_threaded_array ((DB_LIST *) class_->attributes,
				(LFREEER) classobj_clear_attribute);

  /* this shouldn't happen here ? - make sure we can't GC this away
   * in the middle of an edit.
   */
#if 0
  if (class_->new_ != NULL)
    {
      classobj_free_template (class_->new_);
    }
#endif /* 0 */

  if (class_->stats != NULL)
    {
      stats_free_statistics (class_->stats);
      class_->stats = NULL;
    }

  if (class_->disk_constraints != NULL)
    {
      WS_LIST_FREE (class_->disk_constraints, classobj_free_disk_constraint);
      class_->disk_constraints = NULL;
    }

  if (class_->virtual_query_cache)
    {
      mq_free_virtual_query_cache (class_->virtual_query_cache);
    }

  if (class_->auth_cache != NULL)
    {
      au_free_authorization_cache (class_->auth_cache);
    }

  if (class_->constraints != NULL)
    {
      WS_LIST_FREE (class_->constraints, classobj_free_class_constraint);
      class_->constraints = NULL;
    }

  db_ws_free (class_);
}

/*
 * classobj_class_size() - Calculates the amount of memory used by a class structure.
 *   return: byte size of class
 *   class(in): class structure
 */

int
classobj_class_size (SM_CLASS * class_)
{
  SM_ATTRIBUTE *att;
  int size;

  size = sizeof (SM_CLASS);
  size += strlen (class_->header.name) + 1;
  size +=
    ws_list_total ((DB_LIST *) class_->representations,
		   (LTOTALER) classobj_representation_size);

  size +=
    ws_list_total ((DB_LIST *) class_->query_spec,
		   (LTOTALER) classobj_query_spec_size);

  if (class_->loader_commands != NULL)
    {
      size += strlen (class_->loader_commands) + 1;
    }

  for (att = class_->attributes; att != NULL; att = att->next)
    {
      size += classobj_attribute_size (att);
    }

  /* should have constraint cache here */

  return (size);
}

/*
 * classobj_insert_ordered_attribute() - Inserts an attribute in a list ordered by
 *    the "order" field in the attribute.
 *    Work function for classobj_fixup_loaded_class.
 *   return: none
 *   attlist(in/out): pointer to attribute list root
 *   att(in): attribute to insert
 */

static void
classobj_insert_ordered_attribute (SM_ATTRIBUTE ** attlist,
				   SM_ATTRIBUTE * att)
{
  SM_ATTRIBUTE *a, *prev;

  prev = NULL;
  for (a = *attlist; a != NULL && a->order < att->order; a = a->order_link)
    {
      prev = a;
    }

  att->order_link = a;
  if (prev == NULL)
    {
      *attlist = att;
    }
  else
    {
      prev->order_link = att;
    }
}

/*
 * classobj_fixup_loaded_class() - Orders the instance attributes of
 *    a class in a single list according to the order in which the attributes
 *    were defined. This list is not stored with the disk representation of
 *    a class, it is created in memory when the class is loaded.
 *    The actual attribute lists are kept separate in storage order.
 *    The transformer can call this for a newly loaded class or the
 *    schema manager can call this after a class has been edited to
 *    create the ordered list prior to returning control to the user.
 *    This now also goes through and assigns storage_order because this
 *    isn't currently stored as part of the disk representation.
 *   return: none
 *   class(in/out): class to ordrer
 */

void
classobj_fixup_loaded_class (SM_CLASS * class_)
{
  SM_ATTRIBUTE *att;
  int offset = 0;
  int i, fixed_count;

  class_->ordered_attributes = NULL;

  /* Calculate the number of fixed width attributes,
   * Isn't this already set in the fixed_count field ?
   */
  fixed_count = 0;
  for (att = class_->attributes; att != NULL; att = att->next)
    {
      assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

      if (!att->sma_domain->type->variable_p)
	{
	  fixed_count++;
	}
    }

  /* if we have at least one fixed width attribute, then we'll also need
   * a bound bit array.
   */
  if (fixed_count)
    {
      offset += OBJ_BOUND_BIT_BYTES (fixed_count);
    }

  /* Make sure the first attribute is brought up to a longword alignment.
   */
  offset = DB_ATT_ALIGN (offset);

  /* set storage order index and calculate memory offsets */
  for (i = 0, att = class_->attributes; att != NULL; i++, att = att->next)
    {
      assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

      att->storage_order = i;

      /* when we get to the end of the fixed attributes, bring the alignment
       * up to a word boundary.
       */
      if (i == fixed_count)
	{
	  offset = DB_ATT_ALIGN (offset);
	}

      att->offset = offset;
      offset += tp_domain_memory_size (att->sma_domain);
      classobj_insert_ordered_attribute (&class_->ordered_attributes, att);
    }

  offset = DB_ATT_ALIGN (offset);
  class_->object_size = offset;

  for (att = class_->ordered_attributes, i = 0; att != NULL;
       att = att->order_link, i++)
    {
      att->order = i;
    }

  /* Cache constraints into both the class constraint list & the attribute
   * constraint lists.
   */
  (void) classobj_cache_class_constraints (class_);
  (void) classobj_cache_constraints (class_);
}

/*
 * classobj_capture_representation() - Builds a representation structure for
 *   the current state of a class.
 *   return: new representation structure
 *   class(in): class structure
 */

static SM_REPRESENTATION *
classobj_capture_representation (SM_CLASS * class_)
{
  SM_REPRESENTATION *rep;
  SM_REPR_ATTRIBUTE *rat, *last;
  SM_ATTRIBUTE *att;

  rep = classobj_make_representation ();
  if (rep == NULL)
    {
      return NULL;
    }
  rep->id = class_->repid;
  rep->fixed_count = class_->fixed_count;
  rep->variable_count = class_->variable_count;
  rep->next = class_->representations;
  rep->attributes = NULL;

  last = NULL;
  for (att = class_->attributes; att != NULL; att = att->next)
    {
      assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

      rat =
	classobj_make_repattribute (att->id, TP_DOMAIN_TYPE (att->sma_domain),
				    att->sma_domain);
      if (rat == NULL)
	{
	  goto memory_error;
	}
      if (last == NULL)
	{
	  rep->attributes = rat;
	}
      else
	{
	  last->next = rat;
	}
      last = rat;
    }

  return (rep);

memory_error:
  if (rep != NULL)
    {
      classobj_free_representation (rep);
    }
  return NULL;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * classobj_sort_attlist () - Work function for classobj_install_template
 *    Destructively modifies a list so that it is ordered according
 *    to the "order" field.
 *    Rather than have two versions of this for attributes,
 *    can we make this part of the component header ?
 *   return: none
 *   source(in/out): list to sort
 */

static void
classobj_sort_attlist (SM_ATTRIBUTE ** source)
{
  SM_ATTRIBUTE *sorted, *next, *prev, *ins, *att;

  sorted = NULL;
  for (att = *source, next = NULL; att != NULL; att = next)
    {
      next = att->next;

      prev = NULL;
      for (ins = sorted; ins != NULL && ins->order < att->order;
	   ins = ins->next)
	{
	  prev = ins;
	}

      att->next = ins;
      if (prev == NULL)
	{
	  sorted = att;
	}
      else
	{
	  prev->next = att;
	}
    }
  *source = sorted;
}
#endif

/*
 * classobj_install_template() - This is called after a template has been flattened
 *    and validated to install the new definitions in the class.  If the newrep
 *    argument is non zero, a representation will be saved from the current
 *    class contents before installing the template.
 *    NOTE: It is extremely important that as fields in the class structure
 *    are being replaced, that the field be set to NULL.
 *    This is particularly important for the attribute lists.
 *    The reason is that garbage collection can happen during the template
 *    installation and the attribute lists that are freed must not be
 *    scanned by the gc class scanner.
 *    It is critical that errors be handled here without damaging the
 *    class structure.  Perform all allocations before the class is touched
 *    so we can make sure that if we return an error, the class is untouched.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   class(in/out): class structure
 *   flat(in/out): flattened template
 *   saverep(in): flag indicating new representation
 */

int
classobj_install_template (SM_CLASS * class_, SM_TEMPLATE * flat, int saverep)
{
  SM_ATTRIBUTE *att, *atts;
  SM_REPRESENTATION *oldrep;
  int fixed_size, fixed_count, variable_count;
  int att_count;
  int i;

  /* shapshot the representation if necessary */
  oldrep = NULL;
  if (saverep)
    {
      oldrep = classobj_capture_representation (class_);
      if (oldrep == NULL)
	{
	  goto memory_error;
	}

      /* save the old representation */
      oldrep->next = class_->representations;
      class_->representations = oldrep;
      class_->repid = class_->repid + 1;
    }

  atts = NULL;
  fixed_count = 0;
  variable_count = 0;
  fixed_size = 0;

  /* install attribute lists */
  classobj_free_threaded_array ((DB_LIST *) class_->attributes,
				(LFREEER) classobj_clear_attribute);
  class_->attributes = NULL;	/* init */

  att_count = ws_list_length ((DB_LIST *) flat->instance_attributes);
  if (att_count)
    {
      atts = (SM_ATTRIBUTE *)
	classobj_alloc_threaded_array (sizeof (SM_ATTRIBUTE), att_count);
      if (atts == NULL)
	{
	  goto memory_error;
	}

      class_->attributes = atts;

      /* in order to properly calculate the memory offset, we must make an initial
         pass and count the number of fixed width attributes */
      for (att = flat->instance_attributes; att != NULL; att = att->next)
	{
	  assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

	  if (!att->sma_domain->type->variable_p)
	    {
	      fixed_count++;
	    }
	  else
	    {
	      variable_count++;
	    }
	}

      /* calculate the disk size of the fixed width attribute block */
      for (att = flat->instance_attributes, i = 0; att != NULL;
	   att = att->next, i++)
	{
	  assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

	  if (classobj_init_attribute (att, &atts[i], 0))
	    {
	      goto memory_error;
	    }
	  /* disk information */
	  if (!att->sma_domain->type->variable_p)
	    {
	      fixed_size += tp_domain_disk_size (att->sma_domain);
	    }
	}
      /* bring the size of the fixed block up to a word boundary */
      fixed_size = DB_ATT_ALIGN (fixed_size);
    }

  /* NO ERRORS ARE ALLOWED AFTER THIS POINT !
     Modify the class structure to contain the new information.
   */

  class_->class_type = flat->class_type;
  class_->att_count = att_count;
  class_->fixed_count = fixed_count;
  class_->variable_count = variable_count;
  class_->fixed_size = fixed_size;

  /* build the definition order list from the instance attribute list */
  classobj_fixup_loaded_class (class_);

  /* install loader commands */
  class_->loader_commands = NULL;
  flat->loader_commands = NULL;

  /* install the query spec */
  WS_LIST_FREE (class_->query_spec, classobj_free_query_spec);
  class_->query_spec = flat->query_spec;
  flat->query_spec = NULL;

  /* install the property list */
  WS_LIST_FREE (class_->disk_constraints, classobj_free_disk_constraint);
  class_->disk_constraints = NULL;

  class_->disk_constraints = flat->disk_constraints;
  flat->disk_constraints = NULL;

  /* Cache constraints into both the class constraint list & the attribute
   * constraint lists.
   */
  if (classobj_cache_class_constraints (class_))
    {
      goto memory_error;
    }
  if (!classobj_cache_constraints (class_))
    {
      goto memory_error;
    }

  return NO_ERROR;

memory_error:
  /* This is serious, the caller probably should be prepared to
     abort the current transaction.  The class state has
     been preserved but a nested schema update may now be
     in an inconsistent state.
   */
  return er_errid ();
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * classobj_find_representation() - This searches a class for a representation
 *    structure with a particular id.  Called by the object transformer when
 *    obsolete objects are encountered.
 *   return: representation
 *   class(in): class structure
 *   id(in): representation id
 */

SM_REPRESENTATION *
classobj_find_representation (SM_CLASS * class_, int id)
{
  SM_REPRESENTATION *rep, *found;

  for (rep = class_->representations, found = NULL;
       rep != NULL && found == NULL; rep = rep->next)
    {
      if (rep->id == id)
	{
	  found = rep;
	}
    }

  return (found);
}
#endif

/* SM_DISK_CONSTRAINT */

SM_DISK_CONSTRAINT *
classobj_make_disk_constraint (void)
{
  SM_DISK_CONSTRAINT *new_;

  /* make a new constraint list entry */
  new_ = (SM_DISK_CONSTRAINT *) db_ws_alloc (sizeof (SM_DISK_CONSTRAINT));
  if (new_ == NULL)
    {
      return NULL;
    }

  new_->next = NULL;
  new_->name = NULL;
  new_->type = SM_CONSTRAINT_UNIQUE;
  BTID_SET_NULL (&new_->index_btid);
  new_->index_status = INDEX_STATUS_INIT;
  new_->disk_info_of_atts = NULL;

  return new_;
}

/*
 * classobj_find_disk_constraint() - Searches a list of class constraint structures
 *    for one with a certain name.  Couldn't we be using nlist for this?
 *   return: constraint
 *   constraints(in): constraint list
 *   name(in): name to look for
 */

SM_DISK_CONSTRAINT *
classobj_find_disk_constraint (SM_DISK_CONSTRAINT * disk_cons_list,
			       const char *name)
{
  SM_DISK_CONSTRAINT *cons;

  for (cons = disk_cons_list; cons != NULL; cons = cons->next)
    {
      if (intl_identifier_casecmp (cons->name, name) == 0)
	{
	  break;
	}
    }
  return cons;
}

#if defined (RYE_DEBUG)
/* DEBUGGING */
/*
 * classobj_print() - This debug function is used for printing out things that aren't
 *    displayed by the help_ level utility functions.
 *   return: none
 *   class(in): class structure
 */

void
classobj_print (SM_CLASS * class_)
{
  SM_ATTRIBUTE *att;

  if (class_ == NULL)
    {
      return;
    }

  fprintf (stdout, "Class : %s\n", class_->header.name);

  if (class_->properties != NULL)
    {
      fprintf (stdout, "  Properties : ");
      classobj_print_props (class_->properties);
    }

  if (class_->ordered_attributes != NULL)
    {
      fprintf (stdout, "Attributes\n");
      for (att = class_->ordered_attributes;
	   att != NULL; att = att->order_link)
	{
	  assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

	  fprintf (stdout, "  Name=%-25s, id=%3d", att->header.name, att->id);
	  if (att->sma_domain != NULL && att->sma_domain->type != NULL)
	    {
	      fprintf (stdout, ", pr_type=%-10s",
		       att->sma_domain->type->name);
	    }
	  fprintf (stdout, "\n");
	  fprintf (stdout,
		   "    mem_offset=%3d, order=%3d, storage_order=%3d\n",
		   att->offset, att->order, att->storage_order);

	  if (att->properties != NULL)
	    {
	      fprintf (stdout, "    Properties : ");
	      classobj_print_props (att->properties);
	    }
	}
    }
}
#endif

/* MISC UTILITIES */
/*
 * classobj_find_attribute() - Finds a named attribute within a class structure.
 *   return: attribute descriptor
 *   class(in): class structure
 *   name(in): attribute name
 */

SM_ATTRIBUTE *
classobj_find_attribute (SM_ATTRIBUTE * attlist, const char *name)
{
  SM_ATTRIBUTE *att;

  for (att = attlist; att != NULL; att = att->next)
    {
      if (intl_identifier_casecmp (att->name, name) == 0)
	{
	  assert (att->sma_domain != NULL);

	  return att;
	}
    }

  return NULL;
}

/*
 * classobj_find_attribute_id() - Finds an attribute within a class structure by id.
 *   return: attribute descriptor
 *   class(in): class structure
 *   id(in): attribute id
 */

SM_ATTRIBUTE *
classobj_find_attribute_id (SM_ATTRIBUTE * attlist, int id)
{
  SM_ATTRIBUTE *att;

  for (att = attlist; att != NULL; att = att->next)
    {
      if (att->id == id)
	{
	  return att;
	}
    }

  return NULL;
}

/* DESCRIPTORS */

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * classobj_make_desclist() - Builds a descriptor list element and initializes
 *                      all the fields.
 *   return: descriptor list element
 *   classobj(in): class MOP
 *   class(in): class structure
 *   att(in): attribute
 *   write_access(in): non-zero if we already have write access
 */

SM_DESCRIPTOR_LIST *
classobj_make_desclist (MOP classobj, SM_CLASS * class_,
			SM_ATTRIBUTE * att, int write_access)
{
  SM_DESCRIPTOR_LIST *dl;

  dl = (SM_DESCRIPTOR_LIST *) malloc (sizeof (SM_DESCRIPTOR_LIST));
  if (dl == NULL)
    {
      return NULL;
    }

  dl->next = NULL;
  dl->classobj = classobj;
  dl->class_ = class_;
  dl->att = att;
  dl->write_access = write_access;

  return dl;
}

/*
 * classobj_free_desclist() - Frees a descriptor list
 *   return: none
 *   dl(in): descriptor list element
 */

void
classobj_free_desclist (SM_DESCRIPTOR_LIST * dl)
{
  SM_DESCRIPTOR_LIST *next;

  for (next = NULL; dl != NULL; dl = next)
    {
      next = dl->next;

      /* make sure to NULL potential GC roots */
      dl->classobj = NULL;
      free_and_init (dl);
    }
}

/*
 * classobj_free_descriptor() - Frees a descriptor including all the map list entries
 *   return: none
 *   desc(in): descriptor
 */

void
classobj_free_descriptor (SM_DESCRIPTOR * desc)
{
  if (desc == NULL)
    {
      return;
    }
  classobj_free_desclist (desc->map);

  if (desc->name != NULL)
    {
      free_and_init (desc->name);
    }

  if (desc->valid != NULL)
    {
      ml_ext_free (desc->valid->validated_classes);
      free_and_init (desc->valid);
    }

  free_and_init (desc);

}

/*
 * classobj_make_descriptor() - Builds a descriptor structure including an initial
 *    class map entry and initializes it with the supplied information.
 *   return: descriptor structure
 *   class_mop(in): class MOP
 *   classobj(in): class structure
 *   att(in): attribute
 *   write_access(in): non-zero if we already have write access on the class
 */

SM_DESCRIPTOR *
classobj_make_descriptor (MOP class_mop, SM_CLASS * classobj,
			  SM_ATTRIBUTE * att, int write_access)
{
  SM_DESCRIPTOR *desc;
  SM_VALIDATION *valid;

  desc = (SM_DESCRIPTOR *) malloc (sizeof (SM_DESCRIPTOR));
  if (desc == NULL)
    {
      return NULL;
    }

  desc->next = NULL;
  desc->map = NULL;
  desc->class_mop = class_mop;

  if (att != NULL)
    {
      /* save the component name so we can rebuild the map cache
         after schema/transaction changes */
      desc->name = (char *) malloc (strlen (att->name) + 1);
      if (desc->name == NULL)
	{
	  free_and_init (desc);
	  return NULL;
	}
      strcpy (desc->name, att->name);
    }

  /* create the initial map entry if we have the information */
  if (class_mop != NULL)
    {
      desc->map =
	classobj_make_desclist (class_mop, classobj, att, write_access);
      if (desc->map == NULL)
	{
	  classobj_free_descriptor (desc);
	  desc = NULL;
	}
    }

  /* go ahead and make a validation cache all the time */
  valid = (SM_VALIDATION *) malloc (sizeof (SM_VALIDATION));
  if (valid == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      sizeof (SM_VALIDATION));
      classobj_free_descriptor (desc);
      desc = NULL;
    }
  else
    {
      if (desc == NULL)
	{
	  free_and_init (valid);
	  return desc;
	}
      else
	{
	  desc->valid = valid;
	}

      valid->last_class = NULL;
      valid->validated_classes = NULL;
      valid->last_setdomain = NULL;
      /* don't use DB_TYPE_NULL as the "uninitialized" value here
       * as it can prevent NULL constraint checking from happening
       * correctly.
       * Should have a magic constant somewhere that could be used for
       * this purpose.
       */
      valid->last_type = DB_TYPE_LAST;
      valid->last_precision = 0;
      valid->last_scale = 0;
    }

  return desc;
}
#endif

/*
 * classobj_check_index_exist() - Check index is duplicated.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   constraint(in): the constraints list
 *   constraint_name(in): Constraint name.
 */
int
classobj_check_index_exist (SM_CLASS_CONSTRAINT * constraints,
			    const char *class_name,
			    const char *constraint_name)
{
  int error = NO_ERROR;
  SM_CLASS_CONSTRAINT *existing_con;

  if (constraints == NULL)
    {
      return NO_ERROR;
    }

  /* check index name uniqueness */
  existing_con = classobj_find_constraint_by_name (constraints,
						   constraint_name);
  if (existing_con && existing_con->index_status != INDEX_STATUS_IN_PROGRESS)
    {
      ERROR2 (error, ER_SM_INDEX_EXISTS, class_name, existing_con->name);
      return error;
    }

  return NO_ERROR;
}

/*
 * classobj_find_shard_key_column()
 *   return: constraint
 *   class(in):
 */
SM_ATTRIBUTE *
classobj_find_shard_key_column (SM_CLASS * class_)
{
  SM_ATTRIBUTE *att;

  for (att = class_->attributes; att != NULL; att = att->next)
    {
      if (att->flags & SM_ATTFLAG_SHARD_KEY)
	{
	  return att;
	}
    }

  return NULL;
}
