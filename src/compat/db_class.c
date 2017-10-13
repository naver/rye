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
 * db_class.c - API functions for schema definition.
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <ctype.h>

#include "system_parameter.h"
#include "storage_common.h"
#include "db.h"
#include "class_object.h"
#include "object_print.h"
#include "server_interface.h"
#include "boot_cl.h"
#include "locator_cl.h"
#include "schema_manager.h"
#include "schema_template.h"
#include "object_accessor.h"
#include "set_object.h"
#include "parser.h"
#include "execute_schema.h"

#if defined (ENABLE_UNUSED_FUNCTION)
/* Error signaling macros */
static int drop_internal (MOP class_, const char *name);
#endif

/*
 * CLASS DEFINITION
 */

/*
 * db_create_class() - This function creates a new class.
 *    Returns NULL on error with error status left in global error.
 *    The most common reason for returning NULL was that a class with
 *    the given name could not be found.
 * return  : new class object pointer.
 * name(in): the name of a class
 */
DB_OBJECT *
db_create_class (const char *name)
{
  SM_TEMPLATE *def;
  MOP class_;

  CHECK_CONNECT_NULL ();
  CHECK_1ARG_NULL (name);
  CHECK_MODIFICATION_NULL ();

  class_ = NULL;
  def = smt_def_class (name);
  if (def != NULL)
    {
      if (smt_finish_class (def, &class_) != NO_ERROR)
	{
	  smt_quit (def);
	}
    }
  return (class_);
}

/*
 * db_drop_class() - This function is used to completely remove a class and
 *    all of its instances from the database.  Obviously this should be used
 *    with care. Returns non-zero error status if the operation could not be
 *    performed. The most common reason for error is that the current user was
 *    not authorized to delete the specified class.
 * return   : error code
 * class(in): class object
 */
int
db_drop_class (MOP class_)
{
  int retval;

  CHECK_CONNECT_ERROR ();
  CHECK_1ARG_ERROR (class_);
  CHECK_MODIFICATION_ERROR ();

  retval = sm_delete_class_mop (class_);

  return retval;
}

/*
 * db_rename_class() - This function changes the name of a class in the
 *    database. Returns non-zero error if the operation could not be
 *    performed. The most common reason for rename failure is that another
 *    class with the desired name existed.  The current user may also not
 *    have ALTER class authorization.
 * return : error code
 * classop(in/out): class object
 * new_name(in)   : the new name
 */
int
db_rename_class (MOP classop, const char *new_name)
{
  int retval;

  CHECK_CONNECT_ERROR ();
  CHECK_2ARGS_ERROR (classop, new_name);
  CHECK_MODIFICATION_ERROR ();

  retval = sm_rename_class (classop, new_name);

  return retval;
}

/*
 * ATTRIBUTES
 */

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_add_attribute_internal() - This is a generic work function for adding
 *    attributes of the various types.  It saves redundant error checking code
 *    in each of the type specific attribute routines.
 * return : error code
 * class(in/out) : class object
 * name(in)      : attribute name
 * domain(in)    : domain string
 * default_value(in): default_value
 */
int
db_add_attribute_internal (MOP class_, const char *name, const char *domain,
			   DB_VALUE * default_value)
{
  int error = NO_ERROR;
  SM_TEMPLATE *def;
  MOP newmop;

  CHECK_CONNECT_ERROR ();
  CHECK_MODIFICATION_ERROR ();
  CHECK_3ARGS_RETURN_EXPR (class_, name, domain, ER_OBJ_INVALID_ARGUMENTS);

  def = smt_edit_class_mop_with_lock (class_, X_LOCK);
  if (def == NULL)
    {
      error = er_errid ();
    }
  else
    {
      error = smt_add_attribute_any (def, name, domain, (DB_DOMAIN *) 0,
				     false, NULL, false);
      if (error)
	{
	  smt_quit (def);
	}
      else
	{
	  if (default_value != NULL)
	    {
	      error =
		smt_set_attribute_default (def, name, default_value,
					   DB_DEFAULT_NONE);
	    }
	  if (error)
	    {
	      smt_quit (def);
	    }
	  else
	    {
	      error = smt_finish_class (def, &newmop);
	      if (error)
		{
		  smt_quit (def);
		}
	    }
	}
    }
  return error;
}

/*
 * db_add_attribute() - This function adds a normal attribute to a class
 *                      definition.
 * return : error code
 * obj(in/out): class or instance (usually a class)
 * name(in)   : attribute name
 * domain(in) : domain specifier
 * default_value(in): optional default value
 */
int
db_add_attribute (MOP obj, const char *name, const char *domain,
		  DB_VALUE * default_value)
{
  int retval = 0;

  retval = db_add_attribute_internal (obj, name, domain, default_value);

  return retval;
}

/*
 * drop_attribute_internal() - This is internal work function for removing
 *    definitions from a class. Can be used to remove any type of attribute.
 *    The db_ function layer currently forces the callers to
 *    recognize the difference between normal/class attributes
 *    when calling the drop routines.  The interpreter
 *    doesn't have this restriction since the smt_ layer offers an interface
 *    similar to this one. Consider offering the same thing at the db_ layer.
 * return : error code
 * class(in)    : class or instance
 * name(in)     : attribute
 */
static int
drop_internal (MOP class_, const char *name)
{
  int error = NO_ERROR;
  SM_TEMPLATE *def;
  MOP newmop;

  CHECK_CONNECT_ERROR ();
  CHECK_MODIFICATION_ERROR ();
  CHECK_2ARGS_RETURN_EXPR (class_, name, ER_OBJ_INVALID_ARGUMENTS);

  def = smt_edit_class_mop_with_lock (class_, X_LOCK);
  if (def == NULL)
    {
      error = er_errid ();
    }
  else
    {
      error = smt_delete_any (def, name);
      if (error)
	{
	  smt_quit (def);
	}
      else
	{
	  error = smt_finish_class (def, &newmop);
	  if (error)
	    {
	      smt_quit (def);
	    }
	}
    }

  return error;
}

/*
 * db_drop_attribute_internal() - This function removes instance
 *    attributes from a class. The attribute is consequently dropped from any
 *    subclasses, as well.
 * return : error code
 * class(in): class or instance
 * name(in) : attribute name
 */
int
db_drop_attribute_internal (MOP class_, const char *name)
{
  int error = NO_ERROR;

  error = drop_internal (class_, name);

  return error;
}

/*
 * db_change_default() - This function changes the default value definition of
 *    an attribute. Default values are normally established when the attribute
 *    is first defined using the db_add_attribute() function. This function
 *    can be used to change the default value after the attribute has been
 *    defined.
 * return : error code
 * class(in): class or instance pointer
 * name(in) : attribute name
 * value(in): value container with value
 */
int
db_change_default (MOP class_, const char *name, DB_VALUE * value)
{
  int error = NO_ERROR;
  SM_TEMPLATE *def;
  MOP newmop;

  CHECK_CONNECT_ERROR ();
  CHECK_MODIFICATION_ERROR ();
  CHECK_3ARGS_RETURN_EXPR (class_, name, value, ER_OBJ_INVALID_ARGUMENTS);

  def = smt_edit_class_mop_with_lock (class_, X_LOCK);
  if (def == NULL)
    {
      error = er_errid ();
    }
  else
    {
      error = smt_set_attribute_default (def, name, value, DB_DEFAULT_NONE);
      if (error)
	{
	  smt_quit (def);
	}
      else
	{
	  error = smt_finish_class (def, &newmop);
	  if (error)
	    {
	      smt_quit (def);
	    }
	}
    }
  return (error);
}

/*
 * db_rename() - See the db_rename_internal function.
 * return : error code
 * class(in)   : class to alter
 * name(in)    : component name
 * class_namespace(in): class namespace flag
 * newname(in) : new component name
 */
int
db_rename (MOP class_, const char *name,
	   int class_namespace, const char *newname)
{
  int error = NO_ERROR;

  CHECK_CONNECT_ERROR ();
  CHECK_MODIFICATION_ERROR ();

  assert (class_namespace == false);

  error = db_rename_internal (class_, name, newname);

  return error;
}

/*
 * db_rename_internal() - This will rename any of the various class components:
 *    attributes, class attributes.
 * return : error code
 * class(in)   : class to alter
 * name(in)    : component name
 * newname(in) : new component name
 */
int
db_rename_internal (MOP class_, const char *name, const char *newname)
{
  int error = NO_ERROR;
  SM_TEMPLATE *def;
  MOP newmop;

  CHECK_CONNECT_ERROR ();
  CHECK_MODIFICATION_ERROR ();

  def = smt_edit_class_mop_with_lock (class_, X_LOCK);
  if (def == NULL)
    {
      error = er_errid ();
    }
  else
    {
      error = smt_rename_any (def, name, newname);
      if (error)
	{
	  smt_quit (def);
	}
      else
	{
	  error = smt_finish_class (def, &newmop);
	  if (error)
	    {
	      smt_quit (def);
	    }
	}
    }

  return (error);
}

/*
 * INTEGRITY CONSTRAINTS
 */

/*
 * db_constrain_non_null() - This function sets the state of an attribute's
 *                           NON_NULL constraint.
 * return : error code
 * class(in): class or instance object
 * name(in) : attribute name
 * on_or_off(in): non-zero if constraint is to be enabled
 *
 */
int
db_constrain_non_null (MOP class_, const char *name, int on_or_off)
{
  const char *att_names[2];
  int retval;

  att_names[0] = name;
  att_names[1] = NULL;
  if (on_or_off)
    {
#if !defined(NDEBUG)
      {
	bool has_nulls = false;

	assert (do_check_rows_for_null (class_, name, &has_nulls) ==
		NO_ERROR);
	assert (has_nulls == false);
      }
#endif

      retval = db_add_constraint (class_, DB_CONSTRAINT_NOT_NULL,
				  NULL, att_names);
    }
  else
    {
      retval = db_drop_constraint (class_, DB_CONSTRAINT_NOT_NULL,
				   NULL, att_names);
    }

  return retval;
}

/*
 *  INDEX CONTROL
 */

/*
 * db_add_index() - This will add an index to an attribute if one doesn't
 *                  already exist.
 * return : error code
 * classmop(in): class (or instance) pointer
 * attname(in) : attribute name
 *
 * note : This may be an expensive operation if there are a lot of previously
 *    created instances in the database since the index attributes for all of
 *    those instances must be added to the b-tree after it is created.
 */
int
db_add_index (MOP classmop, const char *attname)
{
  const char *att_names[2];
  int retval;

  CHECK_CONNECT_ERROR ();
  CHECK_2ARGS_ERROR (classmop, attname);
  CHECK_MODIFICATION_ERROR ();

  att_names[0] = attname;
  att_names[1] = NULL;
  retval = db_add_constraint (classmop, DB_CONSTRAINT_INDEX, NULL, att_names);

  return (retval);
}

/*
 * db_drop_index() - This function drops an index for an attribute if one was
 *    defined. Multi-attribute indexes can be dropped with the
 *    db_drop_constraint() function.
 * return : error code
 * classmop(in): class (or instance) pointer
 * attname(in) : attribute name
 */
int
db_drop_index (MOP classmop, const char *attname)
{
  const char *att_names[2];
  int retval;

  CHECK_CONNECT_ERROR ();
  CHECK_2ARGS_ERROR (classmop, attname);
  CHECK_MODIFICATION_ERROR ();

  att_names[0] = attname;
  att_names[1] = NULL;
  retval = db_drop_constraint (classmop, DB_CONSTRAINT_INDEX,
			       NULL, att_names);

  return (retval);
}
#endif

/*
 * db_add_constraint() - This function is used to add constraints to a class.
 *    The types of constraints are defined by DB_CONSTRAINT_TYPE and currently
 *    include UNIQUE, NOT NULL, INDEX, PRIMARY_KEY.
 * return : error code
 * classmop(in): class (or instance) pointer
 * constraint_type(in): type of constraint to add(refer to DB_CONSTRAINT_TYPE).
 * constraint_name(in): constraint name.
 * att_names(in): Names of attributes to be constrained
 */
int
db_add_constraint (MOP classmop,
		   DB_CONSTRAINT_TYPE constraint_type,
		   const char *constraint_name, const char **att_names)
{
  int retval;
  char *name = NULL;

  CHECK_CONNECT_ERROR ();
  CHECK_2ARGS_ERROR (classmop, att_names);
  CHECK_MODIFICATION_ERROR ();

  name = sm_produce_constraint_name_mop (classmop, constraint_type,
					 att_names, NULL, constraint_name);
  if (name == NULL)
    {
      retval = er_errid ();
    }
  else
    {
      retval = sm_add_constraint (classmop, constraint_type, name,
				  att_names, NULL);
      free_and_init (name);
    }

  return (retval);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_drop_constraint() - This function is used remove constraint from a class.
 *    Please refer to the header information for db_add_constraint() for basic
 *    information on classes and constraints.
 * return : error code
 * classmop: class (or instance) pointer
 * constraint_type: type of constraint to drop
 * constraint_name: constraint name
 * att_names: names of attributes to be constrained
 *
 * note :
 *    If the name is known, the constraint can be dropped by name, in which
 *    case the <att_names> parameter should be NULL.
 *    If the name is not known, the constraint can be specified by the
 *    combination of class pointer and attribute names.
 *    The order of the attribute names must match the order given when the
 *    constraint was added. In this case, the <constraint_name> should be NULL.
 */
int
db_drop_constraint (MOP classmop,
		    DB_CONSTRAINT_TYPE constraint_type,
		    const char *constraint_name, const char **att_names)
{
  int retval;
  char *name = NULL;

  CHECK_CONNECT_ERROR ();
  CHECK_1ARG_ERROR (classmop);
  CHECK_MODIFICATION_ERROR ();

  name = sm_produce_constraint_name_mop (classmop, constraint_type,
					 att_names, NULL, constraint_name);

  if (name == NULL)
    {
      retval = er_errid ();
    }
  else
    {
      retval =
	sm_drop_constraint (classmop, constraint_type, name, att_names);
      free_and_init (name);
    }

  return (retval);
}
#endif
