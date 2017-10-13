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
 * db_obj.c - API functions for accessing instances.
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
#include "execute_schema.h"
#include "execute_statement.h"
#include "parser.h"
#include "view_transform.h"
#include "network_interface_cl.h"
#include "transform.h"

#include "dbval.h"		/* this must be the last header file included!!! */

/*
 * OBJECT CREATION/DELETION
 */

/*
 * db_create_internal() - This function creates a new instance of a class.
 * return : new object
 * obj(in) : class object
 */
DB_OBJECT *
db_create_internal (DB_OBJECT * obj)
{
  DB_OBJECT *retval;

  CHECK_CONNECT_NULL ();
  CHECK_1ARG_NULL (obj);
  CHECK_MODIFICATION_NULL ();

  retval = obj_create (obj);

  return (retval);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_drop() - This function deletes an instance from the database. All of the
 *    attribute values contained in the dropped instance are lost.
 * return : error code
 * obj(in): instance
 */
int
db_drop (DB_OBJECT * obj)
{
  int retval;

  CHECK_CONNECT_ERROR ();
  CHECK_1ARG_ERROR (obj);
  CHECK_MODIFICATION_ERROR ();

  retval = (obj_delete (obj));

  return (retval);
}
#endif

/*
 * ATTRIBUTE ACCESS
 */

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_get() -This is the basic function for retrieving the value of an
 *    attribute.  This is typically called with just an attribute name.
 *    If the supplied object is an instance, this will look for and return
 *    the values of attributes.  If the supplied object
 *    is a class, this will only look for class attributes.
 * return : error code
 * object(in): class or instance
 * attpath(in): a simple attribute name or path expression
 * value(out) : value container to hold the returned value
 *
 * note : Since this is a copy the value must be freed using db_value_clear
 *   or db_value_free when it is no longer required. And This function will
 *   parse a simplified form of path expression to accepting an attribute name.
 *   it is intended only as a convenience feature for users of the functional
 *   interface. Basically the path expression allows value references to follow
 *   hierarchies of objects and sets.
 *   Example path expressions are as follows:
 *
 *    foo.bar           foo is an object that has a bar attribute
 *    foo.bar.baz       three level indirection through object attributes
 *    foo[0]            foo is a set, first element is returned
 *    foo[0].bar        foo is a set of objects, bar attribute of first
 *                      element is returned
 */
int
db_get (DB_OBJECT * object, const char *attpath, DB_VALUE * value)
{
  int retval;

  CHECK_CONNECT_ERROR ();
  CHECK_2ARGS_ERROR (object, attpath);

  /* handles NULL */
  retval = (obj_get_path (object, attpath, value));

  return (retval);
}
#endif

#if 0				/* unused */
/*
 * db_put() - This function changes the value of an instance or class
 *    attribute. Please refer to the db_put_internal function.
 * return : error code
 * obj(in): instance or class
 * name(in): attribute name
 * vlaue(in): new value
 */
int
db_put (DB_OBJECT * obj, const char *name, DB_VALUE * value)
{
  int error = NO_ERROR;

  CHECK_CONNECT_ERROR ();
  CHECK_2ARGS_ERROR (obj, name);
  CHECK_MODIFICATION_ERROR ();

  error = db_put_internal (obj, name, value);

  return error;
}
#endif

/*
 * db_put_internal() - This function changes the value of an instance or class
 *    attribute. If the
 *    object pointer references an instance object, the attribute name
 *    must be the name of an attribute.
 * return : error code
 * obj(in): instance or class
 * name(in): attribute name
 * vlaue(in): new value
 */
int
db_put_internal (DB_OBJECT * obj, const char *name, DB_VALUE * value)
{
  int retval;

  CHECK_CONNECT_ERROR ();
  CHECK_2ARGS_ERROR (obj, name);
  CHECK_MODIFICATION_ERROR ();

  retval = (obj_set (obj, name, value));

  return (retval);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 *  OBJECT TEMPLATES
 */

/*
 * dbt_edit_object() - This function creates an object template for an existing
 *    object.  The template is initially empty. The template is populated with
 *    the dbt_put_internal() function.
 *    When finished the template can be applied with the
 *    dbt_finish_object function or destroyed with the dbt_abort_object
 *    function.
 * return  : object template
 * object(in): object pointer
 *
 * note : The purpose of the template when using the dbt_edit_object() function
 *    is to be able to make several changes (to several attributes) to an
 *    object through one update. The template is treated as one update.
 *    Therefore, if one of the changes in the template fails, the entire update
 *    fails (none of the changes in the template are applied). Thus, populated
 *    object templates ensure that an object is updated with multiple attribute
 *    values in a single atomic operation. If your attempt to apply the
 *    template fails for any reason, the underlying object is not modified.
 */
DB_OTMPL *
dbt_edit_object (MOP object)
{
  DB_OTMPL *def = NULL;

  CHECK_CONNECT_NULL ();
  CHECK_1ARG_NULL (object);
  CHECK_MODIFICATION_NULL ();

  def = obt_edit_object (object);

  return (def);
}

/*
 * dbt_finish_object() - This function applies an object template.  If the
 *    template can be applied without error, a pointer to the object is
 *    returned and the template is freed.  If this is a template for a new
 *    object, a new object pointer is created and returned.  If this is a
 *    template for an old object the returned pointer is the same as that
 *    passed to dbt_edit_object. If an error is detected, this function
 *    returns NULL, the global error code is set, and the template is not
 *    freed.  In this case, the template can either be corrected and
 *    re-applied or it can be destroyed with dbt_abort_object.
 * return : object pointer
 * def(in): object template
 */
DB_OBJECT *
dbt_finish_object (DB_OTMPL * def)
{
  MOP object = NULL;

  CHECK_CONNECT_NULL ();
  CHECK_1ARG_NULL (def);
  CHECK_MODIFICATION_NULL ();

  if (obt_update (def, &object) != NO_ERROR)
    {
      object = NULL;		/* probably not necessary but be safe */
    }

  return (object);
}

/*
 * dbt_abort_object() -
 * return : none
 * def(in): object template
 *
 * description:
 *    This function destroys an object template. All memory allocated for the
 *    template are released. It is only necessary to call this function if a
 *    template was built but could not be applied without error.
 *    If dbt_finish_object succeeds, the template will be freed and there is
 *    no need to call this function.
 */
void
dbt_abort_object (DB_OTMPL * def)
{
  /* always allow this to be freed, will this be a problem if the
     transaction has been aborted or the connection is down ? */
  if (def != NULL)
    {
      obt_quit (def);
    }
}

/*
 * dbt_put_internal() - This function makes an assignment to an attribute in an
 *    object template. It is similar to db_put and can return the same error
 *    conditions. There is an additional data type accepted by this function.
 *    This can be used to build a
 *    hiararchy of object templates, necessary for the processing of a
 *    nested INSERT statement.  There can be cycles in the hierarchy as
 *    an object is allowed to reference any other object including itself.
 *    When a hierarical object template is built it MUST be applied by
 *    giving the top level template to dbt_finish_object.  You cannot
 *    directly apply an object template that has been nested inside
 *    another template.
 * return : error code
 * def(in/out): object template
 * name(in): attribute name
 * value(in): new value
 */
int
dbt_put_internal (DB_OTMPL * def, const char *name, DB_VALUE * value)
{
  int error = NO_ERROR;

  CHECK_CONNECT_ERROR ();
  CHECK_2ARGS_ERROR (def, name);
  CHECK_MODIFICATION_ERROR ();

  error = obt_set (def, name, value);

  return (error);
}

/*
 *  DESCRIPTOR FUNCTIONS
 *
 *  These provide a faster interface for attribute access
 *  during repetetive operations.
 *
 */

/*
 * db_get_attribute_descriptor() - This builds an attribute descriptor for the
 *    named attribute of the given object.  The attribute descriptor can then
 *    be used by other descriptor functions rather than continuing to reference
 *    the attribute by name.  This speeds up repetitive operations on the same
 *    attribute since the system does not have to keep searching the attribute
 *    name list each the attribute is accessed. The same descriptor can be used
 *    with any class that has an attribute with the given name.
 * return : error code
 * obj(in): instance or class
 * attname(in)    : attribute name
 * for_update(in) : non-zero if the intention is to update
 * descriptor(out): returned attribute descriptor
 *
 * note : The descriptor must be freed with db_free_attribute_descriptor.
 *    If you intend to use the descriptor with the db_putd() or dbt_putd()
 *    functions, set the "for_update" flag to a non-zero value so that
 *    the appropriate locks and authorization checks can be made immediately.
 *    An error is returned if the object does not have an attribute
 *    with the given name.  If an error is returned, an attribute descriptor
 *    is NOT returned.
 */
int
db_get_attribute_descriptor (DB_OBJECT * obj,
			     const char *attname,
			     int for_update, DB_ATTDESC ** descriptor)
{
  int retval;

  CHECK_CONNECT_ERROR ();
  CHECK_3ARGS_ERROR (obj, attname, descriptor);

  retval = sm_get_attribute_descriptor (obj, attname, for_update, descriptor);

  return retval;
}

/*
 * db_free_attribute_descriptor() - This function frees an attribute descriptor
 *    previously returned by db_get_attribute_descriptor.
 * return : error code
 * descriptor(in): attribute descriptor
 */
void
db_free_attribute_descriptor (DB_ATTDESC * descriptor)
{
  (void) sm_free_descriptor ((SM_DESCRIPTOR *) descriptor);
}

/*
 * dbt_dput_internal() - This is the same as dbt_put_internal() except the attribute is
 *    identified through a descriptor rather than by name.
 * returns/side-effects: error code
 * def(in)  : template
 * attribute(in): attribute descriptor
 * value(in): container with value to assign
 */
int
dbt_dput_internal (DB_OTMPL * def, DB_ATTDESC * attribute, DB_VALUE * value)
{
  int retval;

  CHECK_CONNECT_ERROR ();
  CHECK_MODIFICATION_ERROR ();

  retval = obt_desc_set (def, attribute, value);

  return (retval);
}
#endif
