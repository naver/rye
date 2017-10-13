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
 * db_virt.c - API functions related to virtual class.
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
#include "view_transform.h"

#define ERROR_SET(error, code) \
  do {                     \
    error = code;          \
    er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, code, 0); \
  } while (0)


/*
 * VCLASS CREATION
 */

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_create_vclass() - This function creates and returns a new virtual class
 *    with the given name. Initially, the virtual class is created with no
 *    definition. that is, it has no attributes, or query
 *    specifications.
 *    If the name specified has already been used by an existing class in the
 *    database, NULL is returned. In this case, the system sets the global
 *    error status to indicate the exact nature of the error.
 * return : new virtual class object
 * name(in): the name of a virtual class
 */
DB_OBJECT *
db_create_vclass (const char *name)
{
  int error = NO_ERROR;
  SM_TEMPLATE *def;
  DB_OBJECT *virtual_class;
  PR_TYPE *type;

  CHECK_CONNECT_NULL ();
  CHECK_MODIFICATION_NULL ();

  virtual_class = NULL;
  if (name != NULL)
    {
      type = pr_find_type (name);
      if (type != NULL || pt_is_reserved_word (name))
	{
	  error = ER_SM_CLASS_WITH_PRIM_NAME;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_SM_CLASS_WITH_PRIM_NAME, 1, name);
	}
      else
	{
	  def = smt_def_typed_class (name, SM_VCLASS_CT);
	  if (def == NULL)
	    {
	      error = er_errid ();
	    }
	  else
	    {
	      error = smt_finish_class (def, &virtual_class);
	      if (error)
		{
		  smt_quit (def);
		}
	    }
	}
    }

  return (virtual_class);
}

/*
 * db_add_query_spec() - This function adds a query to a virtual class.
 * return : error code
 * class(in): vrtual class
 * query(in): query string
 */
int
db_add_query_spec (MOP vclass, const char *query)
{
  int error = NO_ERROR;
  SM_TEMPLATE *def;
  MOP newmop;

  CHECK_CONNECT_ERROR ();
  CHECK_MODIFICATION_ERROR ();

  if ((vclass == NULL) || (query == NULL))
    {
      ERROR_SET (error, ER_OBJ_INVALID_ARGUMENTS);
    }
  else
    {
      def = smt_edit_class_mop_with_lock (vclass, X_LOCK);
      if (def == NULL)
	{
	  error = er_errid ();
	}
      else
	{
	  error = smt_add_query_spec (def, query);
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

  return (error);
}
#endif

/*
 * db_get_query_specs() - This function returns a list of query_spec
 *    descriptors for a virtual class.
 * return : list of query specifications
 * obj(in): class or instance
 */
DB_QUERY_SPEC *
db_get_query_specs (DB_OBJECT * obj)
{
  SM_QUERY_SPEC *query_spec;
  SM_CLASS *class_;

  CHECK_CONNECT_NULL ();

  query_spec = NULL;

  if (au_fetch_class (obj, &class_, S_LOCK, AU_SELECT) == NO_ERROR)
    {
      query_spec = class_->query_spec;
    }

  return ((DB_QUERY_SPEC *) query_spec);
}

/*
 * db_query_spec_next() - This function returns the next query_spec descriptor
 *    in the list or NULL if you're at the end of the list.
 * return : query_spec descriptor
 * query_spec(in): query_spec descriptor
 */
DB_QUERY_SPEC *
db_query_spec_next (DB_QUERY_SPEC * query_spec)
{
  DB_QUERY_SPEC *next = NULL;

  if (query_spec != NULL)
    {
      next = query_spec->next;
    }

  return (next);
}

/*
 * db_query_spec_string() - This function returns the string defining the
 *   virtual query
 * return : query specification string
 * query_spec(in): query_spec descriptor
 */
const char *
db_query_spec_string (DB_QUERY_SPEC * query_spec)
{
  const char *spec = NULL;

  if (query_spec != NULL)
    {
      spec = query_spec->specification;
    }

  return (spec);
}

/*
 * db_is_vclass() - This function returns a non-zero value if and only if the
 *    object is a virtual class.
 * return : non-zero if the object is a virtual class
 * op(in): class pointer
 */
int
db_is_vclass (DB_OBJECT * op)
{
  SM_CLASS *class_ = NULL;

  CHECK_CONNECT_ZERO ();

  if (op == NULL)
    {
      return 0;
    }
  if (!locator_is_class (op))
    {
      return 0;
    }
  if (au_fetch_class_force (op, &class_, S_LOCK) != NO_ERROR)
    {
      return 0;
    }
  if (sm_get_class_type (class_) != SM_VCLASS_CT)
    {
      return 0;
    }
  return 1;
}
