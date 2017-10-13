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
 * db_temp.c - API functions for schema definition templates.
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
#include "execute_statement.h"
#include "execute_schema.h"
#include "network_interface_cl.h"

#define ERROR_SET(error, code) \
  do {                     \
    error = code;          \
    er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, code, 0); \
  } while (0)

#define ATTR_RENAME_SAVEPOINT          "aTTrNAMeSAVE"

/*
 * SCHEMA TEMPLATES
 */

/*
 * dbt_create_class() - This function creates a class template for a new class.
 *    A class with the given name cannot already exist.
 * return : class template
 * name(in): new class name
 *
 * note : When the template is no longer needed, it should be applied using
 *    dbt_finish_class() or destroyed using dbt_abort_class().
 */
DB_CTMPL *
dbt_create_class (const char *name)
{
  DB_CTMPL *def = NULL;

  CHECK_CONNECT_NULL ();
  CHECK_1ARG_NULL (name);
  CHECK_MODIFICATION_NULL ();

  def = smt_def_class (name);

  return (def);
}

/*
 * dbt_create_vclass() - This function creates a class template for a new
 *    virtual class. A class with the specified name cannot already exist.
 * return : schema template
 * name(in): the name of a virtual class
 *
 * note : The template can be applied using dbt_finish_class() or destroyed
 *   using dbt_abort_class().
 */
DB_CTMPL *
dbt_create_vclass (const char *name)
{
  DB_CTMPL *def = NULL;

  CHECK_CONNECT_NULL ();
  CHECK_1ARG_NULL (name);
  CHECK_MODIFICATION_NULL ();

  def = smt_def_typed_class (name, SM_VCLASS_CT);

  return (def);
}

/*
 * dbt_edit_class() - This function creates a class template for an existing
 *    class. The template is initialized with the current definition of the
 *    class, and it is edited with the other class template functions.
 * return : class template
 * classobj(in): class object pointer
 *
 * note : When finished, the class template can be applied with
 *    dbt_finish_class() or destroyed with dbt_abort_class().
 */
DB_CTMPL *
dbt_edit_class (MOP classobj)
{
  DB_CTMPL *def = NULL;

  CHECK_CONNECT_NULL ();
  CHECK_1ARG_NULL (classobj);
  CHECK_MODIFICATION_NULL ();

  def = smt_edit_class_mop_with_lock (classobj, X_LOCK);

  return (def);
}

/*
 * dbt_copy_class() - This function creates a class template based on an
 *                    existing class.
 * return : class template
 * new_name(in): name of the class to be created
 * existing_name(in): name of the class to be duplicated
 * class_(out): the current definition of the duplicated class is returned
 *              in order to be used for subsequent operations (such as
 *              duplicating indexes).
 *
 * Note : When finished, the class template can be applied with
 *        dbt_finish_class() or destroyed with dbt_abort_class().
 */
DB_CTMPL *
dbt_copy_class (const char *new_name, const char *existing_name,
		SM_CLASS ** class_)
{
  DB_CTMPL *def = NULL;

  CHECK_CONNECT_NULL ();
  CHECK_2ARGS_NULL (new_name, existing_name);
  CHECK_MODIFICATION_NULL ();

  def = smt_copy_class (new_name, existing_name, class_);

  return (def);
}


/*
 * dbt_finish_class() - This function applies a class template. If the template
 *    is applied without error, a pointer to the class object is returned.
 *    If the template is for a new class, a new object pointer is returned.
 *    If the template is for an existing class, the same pointer that was
 *    passed to dbt_edit_class() is returned.
 * return : class pointer
 * def: class template
 *
 * note : If there are no errors, the template is freed and cannot be reused.
 *    If an error is detected, NULL is returned, the global error code is set,
 *    and the template is not freed. The template can either be corrected and
 *    reapplied, or destroyed.
 */
DB_OBJECT *
dbt_finish_class (DB_CTMPL * def)
{
  MOP classmop = NULL;

  CHECK_CONNECT_NULL ();
  CHECK_1ARG_NULL (def);
  CHECK_MODIFICATION_NULL ();

  if (smt_finish_class (def, &classmop) != NO_ERROR)
    {
      classmop = NULL;		/* probably not necessary but be safe */
    }

  return (classmop);
}

/*
 * dbt_abort_class() - This function destroys a class template and frees all
 *    memory allocated for the template.
 * return : none
 * def(in): class template
 */
void
dbt_abort_class (DB_CTMPL * def)
{
  if (def != NULL)
    {
      smt_quit (def);
    }
}

/*
 * SCHEMA TEMPLATE OPERATIONS
 * The descriptions of these functions is the same as that for the
 * non-template versions.
 */

/*
 * dbt_constrain_non_null() -
 * return:
 * def(in) :
 * name(in) :
 * on_or_off(in) :
 *
 * note : Please consider using the newer functions dbt_add_constraint()
 *    and dbt_drop_constraint().
 */
int
dbt_constrain_non_null (DB_CTMPL * def, const char *name, int on_or_off)
{
  const char *names[2];
  int error = NO_ERROR;

  CHECK_CONNECT_ERROR ();
  CHECK_2ARGS_ERROR (def, name);
  CHECK_MODIFICATION_ERROR ();

  names[0] = name;
  names[1] = NULL;
  if (on_or_off)
    {
      error = dbt_add_constraint (def, DB_CONSTRAINT_NOT_NULL, NULL, names);
    }
  else
    {
      error = dbt_drop_constraint (def, DB_CONSTRAINT_NOT_NULL, NULL, names);
    }

  return error;
}

/*
 * dbt_add_constraint() - This function adds a constraint to one or more
 *    attributes if one does not already exist. This function is similar
 *    to the db_add_constraint() function, except that it operates on a
 *    schema template. Since INDEX'es are not manipulated via templates,
 *    this function should only be called for DB_CONSTRAINT_UNIQUE,
 *    DB_CONSTRAINT_NOT_NULL, and DB_CONSTRAINT_PRIMARY_KEY constraint types.
 * return : error code
 * def(in): class template
 * constraint_type(in): constraint type.
 * constraint_name(in): optional name for constraint.
 * attnames(in): NULL terminated array of attribute names.
 */
int
dbt_add_constraint (DB_CTMPL * def,
		    DB_CONSTRAINT_TYPE constraint_type,
		    const char *constraint_name, const char **attnames)
{
  int error = NO_ERROR;
  char *name = NULL;

  CHECK_CONNECT_ERROR ();
  CHECK_2ARGS_ERROR (def, attnames);
  CHECK_MODIFICATION_ERROR ();

  if (!DB_IS_CONSTRAINT_UNIQUE_FAMILY (constraint_type) &&
      constraint_type != DB_CONSTRAINT_NOT_NULL)
    {
      ERROR_SET (error, ER_SM_INVALID_CONSTRAINT);
    }

  if (error == NO_ERROR)
    {
      name = sm_produce_constraint_name_tmpl (def, constraint_type,
					      attnames, NULL,
					      constraint_name);
      if (name == NULL)
	{
	  error = er_errid ();
	}
      else
	{
	  error = smt_add_constraint (def, constraint_type, name,
				      attnames, NULL);
	  free_and_init (name);
	}
    }

  return (error);
}

/*
 * dbt_drop_constraint() - This is a version of db_drop_constraint() which is
 *    designed to operate on templates.  Since INDEX'es are not manipulated via
 *    templates, this function should only be called for DB_CONSTRAINT_UNIQUE,
 *    DB_CONSTRAINT_NOT_NULL, and DB_CONSTRAINT_PRIMARY_KEY constraint types.
 * return : error code
 * def(in): Class template
 * constraint_type(in): Constraint type.
 * constraint_name(in): Constraint name. NOT NULL constraints are not named
 *                   so this parameter should be NULL in that case.
 * attnames(in): NULL terminated array of attribute names.
 */
int
dbt_drop_constraint (DB_CTMPL * def,
		     DB_CONSTRAINT_TYPE constraint_type,
		     const char *constraint_name, const char **attnames)
{
  int error = NO_ERROR;
  char *name = NULL;
  SM_ATTRIBUTE_FLAG attflag = SM_ATTFLAG_UNIQUE;

  CHECK_CONNECT_ERROR ();
  CHECK_1ARG_ERROR (def);
  CHECK_MODIFICATION_ERROR ();

  if (!DB_IS_CONSTRAINT_FAMILY (constraint_type))
    {
      ERROR_SET (error, ER_SM_INVALID_CONSTRAINT);
    }

  attflag = SM_MAP_CONSTRAINT_TO_ATTFLAG (constraint_type);

  if (error == NO_ERROR)
    {
      name = sm_produce_constraint_name_tmpl (def, constraint_type,
					      attnames, NULL,
					      constraint_name);

      if (name == NULL)
	{
	  error = er_errid ();
	}
      else
	{
	  /* TODO We might want to check that the dropped constraint really had
	     the type indicated by the constraint_type parameter. */
	  error = smt_drop_constraint (def, attnames, name, attflag);
	  free_and_init (name);
	}
    }

  return error;
}

/*
 * dbt_change_primary_key ()
 *    return:
 *
 *    def(in/out):
 *    index_name(in):
 */
int
dbt_change_primary_key (DB_CTMPL * def, const char *index_name)
{
  int error = NO_ERROR;
  SM_CLASS_CONSTRAINT *unique_con = NULL, *pk_con = NULL;
  SM_ATTRIBUTE *att = NULL, *tmpl_attp = NULL;
  SM_DISK_CONSTRAINT *disk_con = NULL;
  int i;

  CHECK_CONNECT_ERROR ();
  CHECK_2ARGS_ERROR (def, index_name);
  CHECK_MODIFICATION_ERROR ();

  unique_con = classobj_find_class_index (def->current, index_name);
  if (unique_con == NULL)
    {
      error = er_errid ();
      if (error == NO_ERROR)
	{
	  error = ER_GENERIC_ERROR;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
		  1, "Not found unique constraint.");
	}

      GOTO_EXIT_ON_ERROR;
    }

  /*
   * step 1: check source constraint
   */
  if (unique_con->type != DB_CONSTRAINT_UNIQUE)
    {
      error = ER_SM_NO_UNIQUE_CONSTRAINT;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, index_name);
      GOTO_EXIT_ON_ERROR;
    }

  for (i = 0; i < unique_con->num_atts; i++)
    {
      att = unique_con->attributes[i];
      if ((att->flags & SM_ATTFLAG_NON_NULL) == 0)
	{
	  error = ER_SM_NO_NOT_NULL_CONSTRAINT;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, att->name);
	  GOTO_EXIT_ON_ERROR;
	}
    }


  /*
   * step 2: change primary constraint
   */
  pk_con = classobj_find_class_primary_key (def->current);
  if (pk_con == NULL)
    {
      error = er_errid ();
      if (error == NO_ERROR)
	{
	  error = ER_GENERIC_ERROR;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
		  1, "Not found primary key constraint.");
	}

      GOTO_EXIT_ON_ERROR;
    }

  disk_con = classobj_find_disk_constraint (def->disk_constraints,
					    pk_con->name);
  if (disk_con == NULL)
    {
      assert (false);
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "Not found disk constraint.");

      GOTO_EXIT_ON_ERROR;
    }

  assert (disk_con->type == SM_CONSTRAINT_PRIMARY_KEY);
  disk_con->type = SM_CONSTRAINT_UNIQUE;

  for (i = 0; i < pk_con->num_atts; i++)
    {
      att = pk_con->attributes[i];
      error = smt_find_attribute (def, att->name, &tmpl_attp);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      if (tmpl_attp->flags & SM_ATTFLAG_PRIMARY_KEY)
	{
	  tmpl_attp->flags &= ~SM_ATTFLAG_PRIMARY_KEY;
	}
    }

  /*
   * step 3: change source constraint to primary key
   */
  disk_con = classobj_find_disk_constraint (def->disk_constraints,
					    unique_con->name);
  if (disk_con == NULL)
    {
      assert (false);
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "Not found disk constraint.");
      GOTO_EXIT_ON_ERROR;
    }
  assert (disk_con->type == SM_CONSTRAINT_UNIQUE);

  disk_con->type = SM_CONSTRAINT_PRIMARY_KEY;

  for (i = 0; i < unique_con->num_atts; i++)
    {
      att = unique_con->attributes[i];

      error = smt_find_attribute (def, att->name, &tmpl_attp);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      if ((tmpl_attp->flags & SM_ATTFLAG_PRIMARY_KEY) == 0)
	{
	  tmpl_attp->flags |= SM_ATTFLAG_PRIMARY_KEY;
	}
    }

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

  return error;
}

/*
 * dbt_drop_attribute() -
 * return:
 * def(in) :
 * name(in) :
 */
int
dbt_drop_attribute (DB_CTMPL * def, const char *name)
{
  int error = NO_ERROR;

  CHECK_CONNECT_ERROR ();
  CHECK_2ARGS_ERROR (def, name);
  CHECK_MODIFICATION_ERROR ();

  error = smt_delete_any (def, name);

  return error;
}

/*
 * dbt_rename() -
 * return:
 * def(in) :
 * name(in) :
 * class_namespace(in) :
 * newname(in) :
 */
int
dbt_rename (DB_CTMPL * def,
	    const char *name, UNUSED_ARG int class_namespace,
	    const char *newname)
{
  int error = NO_ERROR;

  CHECK_CONNECT_ERROR ();
  CHECK_3ARGS_ERROR (def, name, newname);
  CHECK_MODIFICATION_ERROR ();

  assert (class_namespace == false);

  error = smt_rename_any (def, name, newname);

  return error;
}

/*
 * dbt_add_query_spec() -
 * return:
 * def(in) :
 * query(in) :
 */
int
dbt_add_query_spec (DB_CTMPL * def, const char *query)
{
  int error = NO_ERROR;

  CHECK_CONNECT_ERROR ();
  CHECK_2ARGS_ERROR (def, query);
  CHECK_MODIFICATION_ERROR ();

  error = smt_add_query_spec (def, query);

  return (error);
}
