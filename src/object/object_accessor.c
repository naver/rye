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
 * object_accessor.c - Object accessor module.
 *
 *    This contains code for attribute access, instance creation
 *    and deletion, and misc utilitities related to instances.
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>

#include "chartype.h"
#include "error_manager.h"
#include "system_parameter.h"
#include "server_interface.h"
#include "dbtype.h"
#include "work_space.h"
#include "object_domain.h"
#include "object_primitive.h"
#include "set_object.h"
#include "class_object.h"
#include "schema_manager.h"
#include "object_accessor.h"
#include "authenticate.h"
#include "db.h"
#include "locator_cl.h"
#include "parser.h"
#include "transaction_cl.h"
#include "view_transform.h"
#include "network_interface_cl.h"

/* Include this last; it redefines some macros! */
#include "dbval.h"

/*
 * OBJ_MAX_ARGS
 *
 * Note :
 *    This is the maximum number of arguments currently supported
 *    This should be unlimited when we have full support for overflow
 *    argument lists.
 *
 */

#define OBJ_MAX_ARGS 32
#define MAX_DOMAIN_NAME 128

typedef enum
{
  TEMPOID_FLUSH_FAIL = -1,
  TEMPOID_FLUSH_OK = 0,
  TEMPOID_FLUSH_NOT_SUPPORT = 1
} TEMPOID_FLUSH_RESULT;

static int find_attribute (SM_CLASS ** classp, SM_ATTRIBUTE ** attp, MOP op,
			   const char *name, int for_write);
static int assign_null_value (MOP op, SM_ATTRIBUTE * att, char *mem);
static int assign_set_value (MOP op, SM_ATTRIBUTE * att, char *mem,
			     SETREF * setref);

static int obj_set_att (MOP op, SM_CLASS * class_, SM_ATTRIBUTE * att,
			DB_VALUE * value);

static int get_object_value (MOP op, SM_ATTRIBUTE * att, char *mem,
			     DB_VALUE * source, DB_VALUE * dest);
static int get_set_value (MOP op, SM_ATTRIBUTE * att, char *mem,
			  DB_VALUE * source, DB_VALUE * dest);

static MOP find_unique (MOP classop, SM_ATTRIBUTE * att,
			DB_IDXKEY * key, LOCK lock);
#if defined (ENABLE_UNUSED_FUNCTION)
static int flush_temporary_OID (MOP classop, DB_VALUE * key);

static DB_VALUE *obj_make_key_value (DB_VALUE * key,
				     const DB_VALUE * values[], int size);
#endif

/* ATTRIBUTE LOCATION */

/*
 * find_attribute - This is the primary attriubte lookup function
 *                  for object operations.
 *    return: error code
 *    classp(out): class pointer (returned)
 *    attp(out): pointer to attribute descriptor (returned())
 *    op(in): class or object pointer
 *    name(in): attribute name
 *    for_write(in): flag set if intention is for update/alter
 *
 * Note:
 *    It will fetch the class with the proper mode and find the named
 *    attribute.
 *    Compare this with the new function sm_get_att_desc() and
 *    try to merge where possible.
 */
static int
find_attribute (SM_CLASS ** classp, SM_ATTRIBUTE ** attp, MOP op,
		const char *name, UNUSED_ARG int for_write)
{
  int error = NO_ERROR;
  SM_CLASS *class_;
  SM_ATTRIBUTE *att;
  bool error_saved = false;

  class_ = NULL;
  att = NULL;

  if (er_errid () != NO_ERROR)
    {
      er_stack_push ();
      error_saved = true;
    }

  assert (locator_is_class (op) == false);

  error = er_errid ();
  if (error_saved)
    {
      if (error == NO_ERROR)
	{
	  er_stack_pop ();
	}
      else
	{
	  /* Current error(occurred in locator_is_class) is returned,
	   * and the previous error is cleared from the stack */
	  er_stack_clear ();
	}
    }

  if (error != NO_ERROR)
    {
      return error;
    }

  error = au_fetch_class (op, &class_, S_LOCK, AU_SELECT);
  if (error == NO_ERROR)
    {
      att = classobj_find_attribute (class_->attributes, name);
    }

  if (error == NO_ERROR && att == NULL)
    {
      ERROR1 (error, ER_OBJ_INVALID_ATTRIBUTE, name);
    }

  *classp = class_;
  *attp = att;
  return error;
}

#if !defined(SERVER_MODE)
/*
 * obj_locate_attribute -
 *    return: error code
 *    op(in): class or object pointer
 *    attid(in): id
 *    for_write(in):flag set if intention is for update/alter
 *    memp(out): pointer to instance memory block (returned)
 *    attp(out): pointer to attribute descriptor (returned)
 *
 * Note:
 *    This is an attribute lookup routine used when the attribute id
 *    is known.  Since id ranges are unique across all attribute types,
 *    this can be used for normal, class attributes.
 *    This is made public so that it can be used by the set module to
 *    locate set valued attributes for a set reference MOP.
 *    Similar to find_attribute() except that it also fetches the instance
 *    and returns the memory offset, consider merging the two.
 */

int
obj_locate_attribute (MOP op, int attid, int for_write,
		      char **memp, SM_ATTRIBUTE ** attp)
{
  int error = NO_ERROR;
  SM_CLASS *class_;
  SM_ATTRIBUTE *att, *found;
  MOBJ obj;
  char *memory;

  found = NULL;
  memory = NULL;

  assert (locator_is_class (op) == false);

  error = au_fetch_class (op, &class_, S_LOCK, AU_SELECT);
  if (error == NO_ERROR)
    {
      if (for_write)
	{
	  error = au_fetch_instance (op, &obj, X_LOCK, AU_UPDATE);
	}
      else
	{
	  error = au_fetch_instance (op, &obj, S_LOCK, AU_SELECT);
	}

      if (error == NO_ERROR)
	{
	  if (for_write)
	    {
	      /* must call this when updating instances */
	      ws_class_has_object_dependencies (op->class_mop);
	    }

	  found = NULL;
	  for (att = class_->attributes; att != NULL && found == NULL;
	       att = att->next)
	    {
	      if (att->id == attid)
		{
		  found = att;
		}
	    }

	  if (found != NULL)
	    {
	      memory = (char *) (((char *) obj) + found->offset);
	    }
	}
    }

  if (error == NO_ERROR && found == NULL)
    {
      ERROR1 (error, ER_OBJ_INVALID_ATTRIBUTE, "???");
    }

  if (attp != NULL)
    {
      *attp = found;
    }
  *memp = memory;

  return error;
}
#endif

/* VALUE ASSIGNMENT */

/*
 * assign_null_value - Work function for assign_value.
 *    return: error
 *    op(in): class or instance pointer
 *    att(in):attribute descriptor
 *    mem(in):pointer to instance memory (only if instance attribute)
 *
 * Note:
 *    This is used to set the value of an attribute to NULL.
 */
static int
assign_null_value (MOP op, SM_ATTRIBUTE * att, char *mem)
{
  /*
   * the mr_ functions are responsible for initializing/freeing the
   * value if NULL is passed in
   */

  assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

  if (mem == NULL)
    {
      pr_clear_value (&att->default_value.value);
    }
  else
    {
      if (PRIM_SETMEM (att->sma_domain->type, att->sma_domain, mem, NULL))
	{
	  return er_errid ();
	}
      else
	{
	  if (!att->sma_domain->type->variable_p)
	    {
	      OBJ_CLEAR_BOUND_BIT (op->object, att->storage_order);
	    }
	}
    }

  return NO_ERROR;
}

/*
 * assign_set_value - Work function for assign_value
 *    return: error
 *    op(in): class or instance pointer
 *    att(in): attribute descriptor
 *    mem(in): pointer to instance memory (for instance attributes only)
 *    setref(in): set pointer to assign
 *
 * Note:
 *    This is used to assign a set value to an attribute.  Sets have extra
 *    overhead in assignment to maintain the ownership information in the
 *    set descriptor.  Unlike strings, sets are not freed when they are
 *    replaced in an assignment.  They will be subject to gargabe collection.
 *
 *    Make sure the set is checked for compliance with the attribute domain
 *    if the set currently has no domain specification.
 */

static int
assign_set_value (MOP op, SM_ATTRIBUTE * att, char *mem, SETREF * setref)
{
  int error = NO_ERROR;
  MOP owner;
  SETREF *new_set, *current_set;
  DB_VALUE val;

  assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

  /* change ownership of the set, copy if necessary */
  if (setref == NULL)
    {
      new_set = NULL;
    }
  else
    {
      owner = op;
      if (mem == NULL && !locator_is_class (op))
	{
	  owner = op->class_mop;
	}

      new_set = set_change_owner (setref, owner, att->id, att->sma_domain);
      if (new_set == NULL)
	{
	  error = er_errid ();
	}
    }

  if (error == NO_ERROR)
    {
      /* assign the value */
      if (mem != NULL)
	{
	  DB_MAKE_SEQUENCE (&val, new_set);

	  error =
	    PRIM_SETMEM (att->sma_domain->type, att->sma_domain, mem, &val);
	  db_value_put_null (&val);

	  if (error == NO_ERROR)
	    {
	      if (new_set != NULL && new_set != setref)
		{
		  set_free (new_set);
		}
	    }
	}
      else
	{
	  /*
	   * remove ownership information in the current set,
	   * need to be able to free this !!!
	   */
	  current_set = DB_GET_SET (&att->default_value.value);
	  if (current_set != NULL)
	    {
	      error = set_disconnect (current_set);
	    }

	  if (error == NO_ERROR)
	    {

	      /* set the new value */
	      if (new_set != NULL)
		{
		  DB_MAKE_SEQUENCE (&att->default_value.value, new_set);
		}
	      else
		{
		  DB_MAKE_NULL (&att->default_value.value);
		}

	      if (new_set != NULL)
		{
		  new_set->ref_count++;
		}
	    }
	}
    }

  return error;
}

/*
 * obj_assign_value - This is a generic value assignment function.
 *    return: error code
 *    op(in): class or instance pointer
 *    att(in): attribute descriptor
 *    mem(in): instance memory pointer (instance attribute only)
 *    value(in): value to assign
 *
 * Note:
 *    It will check the type of the value and call one of the specialized
 *    assignment functions as necessary.
 *    This is called by obj_set and by the template assignment function.
 */

int
obj_assign_value (MOP op, SM_ATTRIBUTE * att, char *mem, DB_VALUE * value)
{
  int error = NO_ERROR;
  MOP mop;

  if (op == NULL || att == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
	      0);
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

  if (DB_IS_NULL (value))
    {
      error = assign_null_value (op, att, mem);
    }
  else
    {
      if (TP_IS_SET_TYPE (TP_DOMAIN_TYPE (att->sma_domain)))
	{
	  error = assign_set_value (op, att, mem, DB_GET_SET (value));
	}
      else
	{
	  if (att->sma_domain->type == tp_Type_object
	      && (mop = DB_GET_OBJECT (value)) && WS_MOP_IS_NULL (mop))
	    {
	      error = assign_null_value (op, att, mem);
	    }
	  else
	    {
	      /* uncomplicated assignment, use the primitive type macros */
	      if (mem != NULL)
		{
		  error =
		    PRIM_SETMEM (att->sma_domain->type, att->sma_domain, mem,
				 value);
		  if (!error && !att->sma_domain->type->variable_p)
		    {
		      OBJ_SET_BOUND_BIT (op->object, att->storage_order);
		    }
		}
	      else
		{
		  pr_clear_value (&att->default_value.value);
		  pr_clone_value (value, &att->default_value.value);
		}
	    }
	}
    }

  return error;
}

/*
 *       		  DIRECT ATTRIBUTE ASSIGNMENT
 */

/*
 * obj_set_att -
 *    return: error code
 *    op(in): object
 *    class(in): class structure
 *    att(in): attribute structure
 *    value(in): value to assign
 *
 * Note:
 *    This is the common assignment function shared by obj_set() and
 *    obj_desc_set().  At this point we have direct pointers to the
 *    class & attribute structures and we can assume that the appropriate
 *    locks have been obtained.
 */
static int
obj_set_att (MOP op, UNUSED_ARG SM_CLASS * class_, SM_ATTRIBUTE * att,
	     DB_VALUE * value)
{
  int error = NO_ERROR;
  char *mem;
  int opin, cpin;
  DB_VALUE *actual;
  DB_VALUE base_value;
  OBJ_TEMPLATE *temp;
  MOBJ obj;

  DB_MAKE_NULL (&base_value);

  /* Check for the presence of unique constraints, use
   * templates in those cases.
   */
  if (classobj_has_unique_constraint (att->constraints))
    {
      temp = obt_edit_object (op);
      if (temp == NULL)
	{
	  error = er_errid ();
	}
      else
	{
	  error = obt_assign (temp, att, value);
	  if (error == NO_ERROR)
	    {
	      error = obt_update (temp, NULL);
	    }
	  else
	    {
	      obt_quit (temp);
	    }
	}
    }
  else
    {
      /*
       * simple, single valued update,
       * avoid template overhead
       */

      /* assume class locks are good, get memory offset */
      mem = NULL;
      if (au_fetch_instance (op, &obj, X_LOCK, AU_UPDATE))
	{
	  return er_errid ();
	}

      /* must call this when updating instances */
      ws_class_has_object_dependencies (op->class_mop);
      mem = (char *) (((char *) obj) + att->offset);

      /*
       * now that we have a memory pointer into the object, must pin it
       * to prevent workspace flush from destroying it
       */
      ws_pin_instance_and_class (op, &opin, &cpin);

      if (error == NO_ERROR)
	{
	  actual = obt_check_assignment (att, value, 0);
	  if (actual == NULL)
	    {
	      error = er_errid ();
	    }
	  else
	    {
	      error = obj_assign_value (op, att, mem, actual);
	      if (actual != value)
		{
		  pr_free_ext_value (actual);
		}
	    }
	}

      ws_restore_pin (op, opin, cpin);
    }

  return error;
}

/*
 * obj_set - This is the external function for assigning the value of an attribute.
 *    return: error code
 *    op(in): class or instance pointer
 *    name(in): attribute
 *    value(in):value to assign
 *
 * Note:
 *    It will locate the attribute, perform type validation, and make
 *    the assignment if everything looks ok.  If the op
 *    argument is an instance object, it will assign a value to either
 *    a normal attribute.
 */
int
obj_set (MOP op, const char *name, DB_VALUE * value)
{
  int error = NO_ERROR;
  SM_ATTRIBUTE *att;
  SM_CLASS *class_;

  if ((op == NULL) || (name == NULL)
      || ((value != NULL) && (DB_VALUE_TYPE (value) > DB_TYPE_LAST)))
    {
      ERROR0 (error, ER_OBJ_INVALID_ARGUMENTS);
    }
  else
    {
      error = find_attribute (&class_, &att, op, name, 1);
      if (error == NO_ERROR)
	{
	  error = obj_set_att (op, class_, att, value);
	}
    }

  return (error);
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * obj_desc_set - This is similar to obj_set() excpet that the attribute is
 *                identified with a descriptor rather than a name.
 *    return: error code
 *    op(in): object
 *    desc(in): attribute descriptor
 *    value(in): value to assign
 *
 * Note:
 *    Once the actual class & attribute structures are located, it calls
 *    obj_set_att() to do the work.
 */

int
obj_desc_set (MOP op, SM_DESCRIPTOR * desc, DB_VALUE * value)
{
  int error = NO_ERROR;
  SM_ATTRIBUTE *att;
  SM_CLASS *class_;

  if ((op == NULL) || (desc == NULL)
      || ((value != NULL) && (DB_VALUE_TYPE (value) > DB_TYPE_LAST)))
    {
      ERROR0 (error, ER_OBJ_INVALID_ARGUMENTS);
    }
  else
    {
      /* map the descriptor into an actual pair of class/attribute structures */
      error = sm_get_descriptor_component (op, desc, 1, &class_, &att);
      if (error != NO_ERROR)
	{
	  return error;
	}

      error = obj_set_att (op, class_, att, value, desc->valid);
    }

  return error;
}
#endif

/*
 *       			VALUE ACCESSORS
 */

/*
 * get_object_value - Work function for obj_get_value.
 *    return: int
 *    op(in): class or instance pointer
 *    att(in): attribute descriptor
 *    mem(in): instance memory pointer (only if instance attribute)
 *    source(out): source value container
 *    dest(out): destination value container
 *
 * Note:
 *    This is the primitive accessor for "object" valued attributes.
 *    The main addition here over other attribute types is that it
 *    will check for deleted object references and convert these to
 *    NULL for return.
 */

static int
get_object_value (MOP op, SM_ATTRIBUTE * att, char *mem,
		  DB_VALUE * source, DB_VALUE * dest)
{
  MOP current;
  DB_VALUE curval;
  int rc = NO_ERROR;

  assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

  /* use class value if alternate source isn't provided */
  if (mem == NULL && source == NULL)
    {
      source = &att->default_value.value;
    }

  current = NULL;
  if (mem != NULL)
    {
      DB_MAKE_OBJECT (&curval, NULL);
      if (PRIM_GETMEM (att->sma_domain->type, att->sma_domain, mem, &curval))
	{
	  return er_errid ();
	}
      current = DB_GET_OBJECT (&curval);
    }
  else if (TP_DOMAIN_TYPE (att->sma_domain) == DB_VALUE_TYPE (source))
    {
      current = DB_GET_OBJECT (source);
    }

  if (current != NULL && WS_ISMARK_DELETED (current))
    {
      /* convert deleted MOPs to NULL values */
      DB_MAKE_NULL (dest);

      /*
       * set the attribute value so we don't hit this condition again,
       * note that this doesn't dirty the object
       */

      if (mem != NULL)
	{
	  if (PRIM_SETMEM (att->sma_domain->type, att->sma_domain, mem, NULL))
	    {
	      return er_errid ();
	    }
	  OBJ_CLEAR_BOUND_BIT (op->object, att->storage_order);
	}
      else
	{
	  DB_MAKE_NULL (source);
	}
    }
  else
    {
      if (current != NULL)
	{
	  DB_MAKE_OBJECT (dest, current);
	}
      else
	{
	  DB_MAKE_NULL (dest);
	}
    }

  return rc;
}

/*
 * get_set_value - Work function for obj_get_value.
 *    return: int
 *    op(in): class or instance pointer
 *    att(in): attirubte descriptor
 *    mem(in): instance memory pointer (only for instance attribute)
 *    source(out): source value container
 *    dest(out): destination value container
 *
 * Note:
 *    This is the primitive accessor for set valued attributes.
 *    This will make sure the set structure is stamped with the MOP of the
 *    owning object and the attribute id of the attribute that points to
 *    it.  This is so we can get back to this attribute if someone tries
 *    to do destructive operations to the set descriptor.
 */

static int
get_set_value (MOP op, SM_ATTRIBUTE * att, char *mem,
	       DB_VALUE * source, DB_VALUE * dest)
{
  SETREF *set;
  DB_VALUE setval;
  MOP owner;

  if (op == NULL || att == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
	      0);
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

  /* use class value if alternate source isn't provided */
  if (mem == NULL && source == NULL)
    {
      source = &att->default_value.value;
    }

  /* get owner and current value */
  set = NULL;
  owner = op;
  if (mem != NULL)
    {
      db_value_domain_init (&setval, TP_DOMAIN_TYPE (att->sma_domain),
			    DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
      if (PRIM_GETMEM (att->sma_domain->type, att->sma_domain, mem, &setval))
	{
	  return er_errid ();
	}
      set = DB_GET_SET (&setval);
      db_value_put_null (&setval);
    }
  else
    {
      /* note, we may have a temporary OP here ! */
      if (!locator_is_class (op))
	{
	  owner = op->class_mop;	/* owner is class */
	}
      if (TP_DOMAIN_TYPE (att->sma_domain) == DB_VALUE_TYPE (source))
	{
	  set = DB_GET_SET (source);
	  /* KLUDGE: shouldn't be doing this at this level */
	  if (set != NULL)
	    {
	      set->ref_count++;
	    }
	}
    }

  /*
   * make sure set has proper ownership tags, this shouldn't happen
   * in normal circumstances
   */
  if (set != NULL && set->owner != owner)
    {
      if (set_connect (set, owner, att->id, att->sma_domain))
	{
	  return er_errid ();
	}
    }

  /* convert NULL sets to DB_TYPE_NULL */
  if (set == NULL)
    {
      DB_MAKE_NULL (dest);
    }
  else
    {
      DB_MAKE_SEQUENCE (dest, set);
    }

  return NO_ERROR;
}

/*
 * obj_get_value -
 *    return: int
 *    op(in): class or instance pointer
 *    att(in): attribute descriptor
 *    mem(in): instance memory pointer (only for instance attribute)
 *    source(out): alternate source value (optional)
 *    dest(out): destionation value container
 *
 * Note:
 *    This is the basic generic function for accessing an attribute
 *    value.  It will call one of the specialized accessor functions above
 *    as necessary.
 */

int
obj_get_value (MOP op, SM_ATTRIBUTE * att, void *mem,
	       DB_VALUE * source, DB_VALUE * dest)
{
  int error = NO_ERROR;

  if (op == NULL || att == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
	      0);
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  assert (att->sma_domain != NULL);
  assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

  /* use class value if alternate source isn't provided */
  if (mem == NULL && source == NULL)
    {
      source = &att->default_value.value;
    }

  /* first check the bound bits */
  if (!att->sma_domain->type->variable_p && mem != NULL
      && OBJ_GET_BOUND_BIT (op->object, att->storage_order) == 0)
    {
      DB_MAKE_NULL (dest);
    }
  else
    {
      if (TP_IS_SET_TYPE (TP_DOMAIN_TYPE (att->sma_domain)))
	{
#if 1				/* TODO - trace */
	  assert (false);
#endif
	  error = get_set_value (op, att, (char *) mem, source, dest);
	}
      else if (att->sma_domain->type == tp_Type_object)
	{
#if 1				/* TODO - trace */
	  assert (false);
#endif
	  error = get_object_value (op, att, (char *) mem, source, dest);
	}
      else
	{
	  if (mem != NULL)
	    {
	      error =
		PRIM_GETMEM (att->sma_domain->type, att->sma_domain, mem,
			     dest);
	      if (!error)
		{
		  OBJ_FORCE_SIMPLE_NULL_TO_UNBOUND (dest);
		}
	    }
	  else
	    {
	      error = pr_clone_value (source, dest);
	      if (!error)
		{
		  OBJ_FORCE_SIMPLE_NULL_TO_UNBOUND (dest);
		}
	    }
	}
    }

  return error;
}

/*
 * obj_get_att - This is a common attribute retriveal function shared by
 *               obj_get.
 *    return: error code
 *    op(in): object
 *    class(in): class structure
 *    att(in): attribute structure
 *    value(out): value container(output)
 *
 * Note:
 *    It operates assuming that we now
 *    have direct pointers to the class & attribute structures and that
 *    the appropriate locks have been obtained.
 *    It handles the difference between temporary, virtual, and normal
 *    instnace MOPs.
 */

int
obj_get_att (MOP op, UNUSED_ARG SM_CLASS * class_, SM_ATTRIBUTE * att,
	     DB_VALUE * value)
{
  int error = NO_ERROR;
  char *mem;
  int opin, cpin;
  MOBJ obj;

  /* fetch the instance if necessary */
  mem = NULL;
  /* fetch the instance and caluclate memory offset */
  if (au_fetch_instance_force (op, &obj, S_LOCK) != NO_ERROR)
    {
      return er_errid ();
    }
  mem = (char *) (((char *) obj) + att->offset);

  ws_pin_instance_and_class (op, &opin, &cpin);
  error = obj_get_value (op, att, mem, NULL, value);
  ws_restore_pin (op, opin, cpin);

  return error;
}

/*
 * obj_get - This is the external function for accessing attribute values.
 *    return: error code
 *    op(in): class or instance pointer
 *    name(in): attribute name
 *    value(out): destination value container
 *
 * Note:
 *    If the named attribute is found, the value is returned through the
 *    supplied value container.
 */

int
obj_get (MOP op, const char *name, DB_VALUE * value)
{
  int error = NO_ERROR;
  SM_ATTRIBUTE *att;
  SM_CLASS *class_;

  if ((op == NULL) || (name == NULL) || (value == NULL))
    {
      ERROR0 (error, ER_OBJ_INVALID_ARGUMENTS);
    }
  else
    {
      error = find_attribute (&class_, &att, op, name, 0);
      if (error == NO_ERROR)
	{
	  error = obj_get_att (op, class_, att, value);
	}
    }

  return error;
}


/*
 *
 *       	   OBJECT ACCESS WITH PSEUDO PATH EXPRESSION
 *
 *
 */

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * obj_get_path -
 *    return: error
 *    object(in): class or instance
 *    attpath(in): a simple attribute name or path expression
 *    value(out): value container to hold the returned value
 *
 */

int
obj_get_path (DB_OBJECT * object, const char *attpath, DB_VALUE * value)
{
  int error;
  char buf[512];
  char *token, *end;
  char delimiter, nextdelim;
  DB_VALUE temp_value;
  int index;

  error = NO_ERROR;
  (void) strcpy (&buf[0], attpath);
  delimiter = '.';		/* start with implicit dot */
  DB_MAKE_OBJECT (&temp_value, object);
  for (token = &buf[0]; char_isspace (*token) && *token != '\0'; token++);
  end = token;

  while (delimiter != '\0' && error == NO_ERROR)
    {
      nextdelim = '\0';
      if (delimiter == '.')
	{
	  if (DB_VALUE_TYPE (&temp_value) != DB_TYPE_OBJECT)
	    {
	      ERROR0 (error, ER_OBJ_INVALID_OBJECT_IN_PATH);
	    }
	  else
	    {
	      for (end = token; !char_isspace (*end) && *end != '\0';)
		{
		  if ((*end != '.') && (*end != '['))
		    {
		      end++;
		    }
		  else
		    {
		      nextdelim = *end;
		      *end = '\0';
		    }
		}

	      if (token == end)
		{
		  ERROR0 (error, ER_OBJ_INVALID_PATH_EXPRESSION);
		}
	      else
		{
		  error = obj_get (DB_GET_OBJECT (&temp_value), token,
				   &temp_value);
		}
	    }
	}
      else if (delimiter == '[')
	{
	  DB_TYPE temp_type;

	  temp_type = DB_VALUE_TYPE (&temp_value);
	  if (!TP_IS_SET_TYPE (temp_type))
	    {
	      ERROR0 (error, ER_OBJ_INVALID_SET_IN_PATH);
	    }
	  else
	    {
	      for (end = token; char_isdigit (*end) && *end != '\0'; end++)
		;

	      nextdelim = *end;
	      *end = '\0';
	      if (end == token)
		{
		  ERROR0 (error, ER_OBJ_INVALID_INDEX_IN_PATH);
		}
	      else
		{
		  index = atoi (token);
		  if (temp_type == DB_TYPE_SEQUENCE)
		    {
		      error = db_seq_get (DB_GET_SET (&temp_value), index,
					  &temp_value);
		    }
		  else
		    {
		      error = db_set_get (DB_GET_SET (&temp_value), index,
					  &temp_value);
		    }

		  if (error == NO_ERROR)
		    {
		      for (++end; nextdelim != ']' && nextdelim != '\0';
			   nextdelim = *end++)
			;
		      if (nextdelim != '\0')
			{
			  nextdelim = *end;
			}
		    }
		}
	    }
	}
      else
	{
	  ERROR0 (error, ER_OBJ_INVALID_PATH_EXPRESSION);
	}

      /* next iteration */
      delimiter = nextdelim;
      token = end + 1;
    }

  if (error == NO_ERROR)
    {
      *value = temp_value;
    }

  return error;
}
#endif

/*
 *
 *       		  OBJECT CREATION AND DELETION
 *
 *
 */

/*
 * obj_alloc -  Allocate and initialize storage for an instance block.
 *    return: instance block
 *    class(in): class structure
 *    bound_bit_status(in): nitial state for bound bits
 *
 * Note:
 *    The bit_status argument has the initial state for the bound bits.
 *    If it is zero, all bits are off, if it is non-zero, all bits are
 *    on.
 */
char *
obj_alloc (SM_CLASS * class_, int bound_bit_status)
{
  SM_ATTRIBUTE *att;
  char *obj, *mem;
  unsigned int *bits;
  int nwords, i;

  obj = (char *) db_ws_alloc (class_->object_size);

  if (obj != NULL)
    {
      /* init the bound bit vector */
      if (class_->fixed_count)
	{
	  bits = (unsigned int *) (obj + OBJ_HEADER_BOUND_BITS_OFFSET);
	  nwords = OR_BOUND_BIT_WORDS (class_->fixed_count);
	  for (i = 0; i < nwords; i++)
	    {
	      if (bound_bit_status)
		{
		  bits[i] = 0xFFFFFFFF;
		}
	      else
		{
		  bits[i] = 0;
		}
	    }
	}

      /* clear the object */
      for (att = class_->attributes; att != NULL; att = att->next)
	{
	  assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

	  mem = obj + att->offset;
	  PRIM_INITMEM (att->sma_domain->type, mem, att->sma_domain);
	}
    }

  return obj;
}

/*
 * obj_create - Creates a new instance of a class.
 *    return: new object
 *    classop(in):  class or instance pointer
 *
 * Note:
 *    Formerly, this allocated the instance and assigned the default
 *    values directly into the object.  Now that
 *    virtual classes complicate things, we use an insert template
 *    for all creations.
 */

MOP
obj_create (MOP classop)
{
  OBJ_TEMPLATE *obj_template;
  MOP new_mop;

  new_mop = NULL;

  obj_template = obt_def_object (classop);
  if (obj_template != NULL)
    {
      /* remember to disable the NON NULL integrity constraint checking */
      if (obt_update_internal (obj_template, &new_mop, 0) != NO_ERROR)
	{
	  obt_quit (obj_template);
	}
    }
  return (new_mop);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * obj_copy -
 *    return: new object
 *    op(in): object to copy
 *
 * Note:
 *    Utility function to do a simple object copy.  This only does a single
 *    level copy.
 *    Formerly, this did a rather low level optimized copy of the object.
 *    Now with virtual objects etc., we simply create an insert
 *    template with the current values of the object.
 *    This isn't particularly efficient but not very many people use
 *    object copy anyway.
 *
 */

MOP
obj_copy (MOP op)
{
  OBJ_TEMPLATE *obj_template;
  MOP new_mop;
  SM_CLASS *class_;
  SM_ATTRIBUTE *att;
  MOBJ src;
  DB_VALUE value;

  new_mop = NULL;
  DB_MAKE_NULL (&value);

  /* op must be an object */
  if (op == NULL || locator_is_class (op))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }
  else
    {
      if (au_fetch_class (op, &class_, S_LOCK, AU_INSERT) != NO_ERROR)
	{
	  return NULL;
	}

      /* do this so that we make really sure that op->class is set up */
      if (au_fetch_instance (op, &src, S_LOCK, AU_SELECT) != NO_ERROR)
	{
	  return NULL;
	}

      obj_template = obt_def_object (op->class_mop);
      if (obj_template != NULL)
	{
	  for (att = class_->attributes; att != NULL; att = att->next)
	    {
	      if (obj_get_att (op, class_, att, &value) != NO_ERROR)
		{
		  goto error;
		}

	      if (obt_assign (obj_template, att, &value, NULL) != NO_ERROR)
		{
		  goto error;
		}

	      (void) pr_clear_value (&value);
	    }

	  /* leaves new NULL if error */
	  if (obt_update_internal (obj_template, &new_mop, 0) != NO_ERROR)
	    {
	      obt_quit (obj_template);
	    }
	}
    }

  return new_mop;

error:
  obt_quit (obj_template);

  return NULL;
}
#endif

/*
 * obj_free_memory - This frees all of the storage allocated for an object.
 *    return: none
 *    class(in):
 *    obj(in): object pointer
 *
 * Note:
 * It will be called indirectly by the workspace manager when an
 * object is decached.
 */

void
obj_free_memory (SM_CLASS * class_, MOBJ obj)
{
  SM_ATTRIBUTE *att;
  char *mem;

  for (att = class_->attributes; att != NULL; att = att->next)
    {
      assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

      mem = ((char *) obj) + att->offset;
      PRIM_FREEMEM (att->sma_domain->type, mem);
    }

  db_ws_free (obj);
}

/*
 * obj_delete - This is the external function for deleting an object.
 *    return: error code
 *    op(in): instance pointer
 *
 * Note:
 *    You cannot delete classes with this function, only instances. This will
 *    decache the instance and mark the MOP as deleted but will not free the
 *    MOP since there may be references to it in the application. The mop will
 *    be garbage collected later.
 */
int
obj_delete (MOP op)
{
  int error = NO_ERROR;
  DB_OBJECT *base_op = NULL;
  int pin = 0;
  int pin2 = 0;
  bool unpin_on_error = false;

  /* op must be an object */
  if (op == NULL || locator_is_class (op))
    {
      ERROR0 (error, ER_OBJ_INVALID_ARGUMENTS);
      goto error_exit;
    }

  error = au_fetch_class (op, NULL, S_LOCK, AU_DELETE);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  error = au_fetch_instance (op, NULL, X_LOCK, AU_DELETE);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  base_op = op;

  /* TODO - We need to keep it pinned for the duration of trigger processing. */
  pin = ws_pin (op, 1);
  if (base_op != NULL && base_op != op)
    {
      pin2 = ws_pin (base_op, 1);
    }
  unpin_on_error = true;

  /*
   * Unpin this now since the remaining operations will mark the instance as
   * deleted and it doesn't make much sense to have pinned & deleted objects.
   */
  (void) ws_pin (op, pin);
  if (base_op != NULL && base_op != op)
    {
      (void) ws_pin (base_op, pin2);
    }
  unpin_on_error = false;

  /*
   * We don't need to decache the object as it will be decached when the mop
   * is GC'd in the usual way.
   */

  locator_remove_instance (op);

  return error;

error_exit:
  if (unpin_on_error)
    {
      /* TODO - trigger failure, remember to unpin */
      (void) ws_pin (op, pin);
      if (base_op != NULL && base_op != op)
	{
	  (void) ws_pin (base_op, pin2);
	}
      unpin_on_error = false;
    }
  return error;
}

/*
 *       		     MISC OBJECT UTILITIES
 *
 */

/*
 * find_unique - Internal function called by the various flavors of functions
 *               that look for unique attribute values.
 *    return: object pointer
 *    classop(in):
 *    att(in): attrubute descriptor
 *    key(in): value to look for
 *    lock(in): access type
 *
 * Note:
 *    This will try to find an object that has a particular value in either
 *    a unique btree or a regular query btree.
 *
 *    If the attribute is not associated with any index or other optimized
 *    lookup structure, we will fabricate a select statement that attempts
 *    to locate the object.
 *
 *    The end result is that this function should try hard to locate the
 *    object in the most efficient way possible, avoiding restrictive
 *    class locks where possible.
 *
 *    If NULL is returned an error will be set.  The error set if the
 *    object could not be found will be ER_OBJ_OBJECT_NOT_FOUND
 */
static MOP
find_unique (MOP classop, SM_ATTRIBUTE * att, DB_IDXKEY * key, LOCK lock)
{
  MOP found = NULL;
  OID *class_oid;
  OID unique_oid;
  BTID btid;
  int r;

  assert (classop != NULL);
  assert (att != NULL);
  assert (key != NULL);
  assert (key->size == 1);
  assert (DB_VALUE_DOMAIN_TYPE (&(key->vals[0])) == DB_TYPE_VARCHAR);

  class_oid = ws_oid (classop);

#if 1				/* TODO - remove me someday */
  /* make sure all dirtied objects have been flushed */
  if (sm_flush_objects (classop) != NO_ERROR)
    {
      return NULL;
    }
#endif

  /*
   * Check to see if we have any sort of index we can search, if not,
   * then return an error indicating that the indexes do not exist rather than
   * the "object not found" error.
   */

  BTID_SET_NULL (&btid);

  /* look for a unique index on this attribute */
  r = classobj_get_cached_index_family (att->constraints,
					SM_CONSTRAINT_UNIQUE, &btid);
  if (r == 0)
    {
      /* look for a primary key on this attribute */
      r = classobj_get_cached_index_family (att->constraints,
					    SM_CONSTRAINT_PRIMARY_KEY, &btid);
      if (r == 0)
	{
	  /* couldn't find one, check for a index */
	  r = classobj_get_cached_index_family (att->constraints,
						SM_CONSTRAINT_INDEX, &btid);
	  if (r == 0)
	    {
	      /* couldn't find anything to search in */
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OBJ_INDEX_NOT_FOUND, 0);
	      return NULL;
	    }
	}
    }

  /* now search the index */
  if (btree_find_unique (class_oid, &btid, key, &unique_oid) ==
      BTREE_KEY_FOUND)
    {
      found = ws_mop (&unique_oid, classop);
    }

  /*
   * If we got an object, obtain an "S" lock before returning it, this
   * avoid problems peeking at objects that were created
   * by another transaction but which have not yet been committed.
   * We may suspend here.
   * Note that we're not getting an S lock on the class so we're still
   * not technically correct in terms of the usual index scan locking
   * model, but that's actually a desireable feature in this case.
   */
  if (found != NULL)
    {
      if (au_fetch_instance_force (found, NULL, lock) != NO_ERROR)
	{
	  return NULL;
	}
    }

#if 0
notfound:
#endif
  /*
   * since this is a common case, set this as a warning so we don't clutter
   * up the error log.
   */
  if (found == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_OBJECT_NOT_FOUND, 0);
    }

  return found;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * flush_temporary_OID -
 *    return:      0 : is not temporary OID or flushed
 *                 1 : don't support
 *                 -1 : error occurs
 *
 *    classop(in):
 *    key(in): OID value to look for
 *
 * Note:
 *    If the OID is temporary, we have something of a dilemma.  It can't
 *    possibly be in the index since it's never been flushed to the server.
 *    That makes the index lookup useless, however, we could still have an
 *    object in the workspace with an attribute pointing to this object.
 *    The only reliable way to use the index is to first flush the class.
 *    This is what a SELECT statement would do anyway so its no big deal.
 *    If after flushing, the referenced object is still temporary, then it
 *    can't possibly be referenced by this class.
 */
static int
flush_temporary_OID (MOP classop, DB_VALUE * key)
{
  MOP mop;

  mop = DB_GET_OBJECT (key);
  if (mop == NULL)
    {
      /* if this is a virtual object, we don't support that */
      return TEMPOID_FLUSH_NOT_SUPPORT;
    }
  else if (OID_ISTEMP (WS_OID (mop)))
    {
      /* flush this class and see if the value remains temporary */
      if (sm_flush_objects (classop) != NO_ERROR)
	{
	  return TEMPOID_FLUSH_FAIL;
	}
      if (OID_ISTEMP (WS_OID (mop)))
	{
	  return TEMPOID_FLUSH_NOT_SUPPORT;
	}
    }
  return TEMPOID_FLUSH_OK;
}
#endif

/*
 * obj_find_unique - This is used to find the object which has a particular
 *                   unique value.
 *    return: object which has the value if any
 *    op(in): class object
 *    attname(in): attribute name
 *    key(in): value to look for
 *    lock(in): access type
 *
 * Note:
 *      Calls find_unique to do the work after locating the proper internal
 *      attribute structure.
 *
 */
MOP
obj_find_unique (MOP op, const char *attname, DB_IDXKEY * key, LOCK lock)
{
  SM_CLASS *class_;
  SM_ATTRIBUTE *att;
  MOP obj;

  assert (op != NULL);
  assert (attname != NULL);
  assert (key != NULL);
  assert (key->size == 1);
  assert (DB_VALUE_DOMAIN_TYPE (&(key->vals[0])) == DB_TYPE_VARCHAR);

  obj = NULL;

  if (op == NULL || attname == NULL || key == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }
  else if (au_fetch_class (op, &class_, S_LOCK, AU_SELECT) == NO_ERROR)
    {
      att = classobj_find_attribute (class_->attributes, attname);
      if (att == NULL)
	{
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		  ER_OBJ_INVALID_ATTRIBUTE, 1, attname);
	}
      else
	{
	  obj = find_unique (op, att, key, lock);
	}
    }

  return obj;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * obj_make_key_value
 *   return : object with the key value
 *
 *   key(in):
 *   values(in):
 *   size(in):
 *
 */
static DB_VALUE *
obj_make_key_value (DB_VALUE * key, const DB_VALUE * values[], int size)
{
  int i, nullcnt;
  DB_SEQ *mc_seq = NULL;

  if (size == 1)
    {
      if (DB_IS_NULL (values[0]))
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OBJ_INVALID_ARGUMENTS, 0);
	  return NULL;
	}
      *key = *values[0];
    }
  else
    {
      mc_seq = set_create_sequence (size);
      if (mc_seq == NULL)
	{
	  return NULL;
	}

      for (i = 0, nullcnt = 0; i < size; i++)
	{
	  if (values[i] == NULL
	      || set_put_element (mc_seq, i,
				  (DB_VALUE *) values[i]) != NO_ERROR)
	    {
	      set_free (mc_seq);
	      return NULL;
	    }

	  if (DB_IS_NULL (values[i]))
	    {
	      nullcnt++;
	    }
	}

      if (nullcnt >= size)
	{
	  set_free (mc_seq);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OBJ_INVALID_ARGUMENTS, 0);
	  return NULL;
	}

      DB_MAKE_SEQUENCE (key, mc_seq);
    }

  return key;
}

/*
 * obj_repl_add_object : create a replication object and add it to link
 *                              for bulk flushing
 *    return:
 *    class_name(in):
 *    key (in): primary key value
 *    type (in): item type (INSERT, UPDATE, or DELETE)
 *    recdes(in): record to be inserted
 */
int
obj_repl_add_object (const char *class_name, DB_IDXKEY * key, int type,
		     RECDES * recdes)
{
  int error = NO_ERROR;
  int operation = 0;

  if (key == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  switch (type)
    {
    case RVREPL_DATA_UPDATE:
      operation = LC_FLUSH_UPDATE;
      break;
    case RVREPL_DATA_INSERT:
      operation = LC_FLUSH_INSERT;
      break;
    case RVREPL_DATA_DELETE:
      operation = LC_FLUSH_DELETE;
      break;
    default:
      assert (false);
    }

  if (DB_IDXKEY_IS_NULL (key))
    {
      assert (false);
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_OBJECT_NOT_FOUND, 0);
      return ER_OBJ_OBJECT_NOT_FOUND;
    }

  error = ws_add_to_repl_obj_list (class_name, key, recdes, operation);

  return error;
}

/*
 * obj_isinstance - Tests to see if an object is an instance object.
 *    return: true if object is an instance
 *    obj(in): object
 *
 */

bool
obj_isinstance (MOP obj)
{
  bool is_instance = false;

  if (obj != NULL)
    {
      if (!locator_is_class (obj))
	{
	  /*
	   * before declaring this an instance, we have to make sure it
	   * isn't deleted
	   */
	  if (!WS_ISMARK_DELETED (obj))
	    {
	      is_instance = true;
	    }
	}
    }

  return (is_instance);
}
#endif

/*
 * obj_lock - Simplified interface for obtaining the basic read/write locks
 *    on an object.
 *    return: error code
 *    op(in): object to lock
 *    for_write(in): non-zero to get a write lock
 *
 */

int
obj_lock (MOP op, int for_write)
{
  int error = NO_ERROR;

  if (locator_is_class (op))
    {
      if (for_write)
	{
	  error = au_fetch_class (op, NULL, X_LOCK, AU_ALTER);
	}
      else
	{
	  error = au_fetch_class (op, NULL, S_LOCK, AU_SELECT);
	}
    }
  else
    {
      if (for_write)
	{
	  error = au_fetch_instance (op, NULL, X_LOCK, AU_UPDATE);
	}
      else
	{
	  error = au_fetch_instance (op, NULL, S_LOCK, AU_SELECT);
	}
    }

  return (error);
}
