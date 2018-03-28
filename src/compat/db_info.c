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
 * db_info.c - API functions for accessing database information
 *             and browsing classes.
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
#include "authenticate.h"
#include "network_interface_cl.h"

/*
 *  CLASS LOCATION
 */

/*
 *  OBJECT PREDICATES
 */

/*
 * db_is_class() - This function is used to test if a particular object
 *    pointer (MOP) actually is a reference to a class object.
 * return : non-zero if object is a class
 * obj(in): a pointer to a class or instance
 *
 * note : If it can be detected that the MOP has been deleted, this will
 *    return zero as well.  This means that you can't simply use this to
 *    see if a MOP is an instance or not.
 */
int
db_is_class (MOP obj)
{
  SM_CLASS *class_ = NULL;

  CHECK_CONNECT_ZERO ();

  if (obj == NULL)
    {
      return 0;
    }
  if (!locator_is_class (obj))
    {
      return 0;
    }
  if (au_fetch_class_force (obj, &class_, S_LOCK) != NO_ERROR)
    {
      return 0;
    }

  assert (class_ != NULL);
  if (sm_get_class_type (class_) != SM_CLASS_CT)
    {
      return 0;
    }

  return 1;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_is_any_class() - This function is used to test if a particular object
 *    pointer (MOP) actually is a reference to a {class|vclass|view}
 *    object.
 * return : non-zero if object is a {class|vclass|view}
 * obj(in): a pointer to a class or instance
 * note : If it can be detected that the MOP has been deleted, this will return
 *    zero as well.  Note that this means that you can't simply use this to see
 *    if a MOP is an instance or not.
 */
int
db_is_any_class (MOP obj)
{
  SM_CLASS *class_ = NULL;

  CHECK_CONNECT_ZERO ();

  if (obj == NULL)
    {
      return 0;
    }
  if (!locator_is_class (obj))
    {
      return 0;
    }
  if (au_fetch_class_force (obj, &class_, S_LOCK) != NO_ERROR)
    {
      return 0;
    }

  assert (class_ != NULL);

  return 1;
}
#endif

/*
 * db_is_system_table() - This function is a convenience function to determine
 *    if a class is one of the system defined classes or a user defined class.
 * return: true if op is system class
 * op(in): class pointer
 */
int
db_is_system_table (MOP op)
{
  int retval;

  CHECK_CONNECT_ZERO ();
  CHECK_1ARG_ZERO (op);

  retval = sm_is_system_table (op) ? true : false;

  return retval;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_is_deleted() - This function is used to determine whether or not the
 *    database object associated with an object handle has been deleted.
 * return : status code
 *      0 = The object is not deleted.
 *     >0 = The object is deleted.
 *     <0 = An error was detected, the state of the object is unknown and
 *           an error code is available through db_error_code().
 * obj(in): object to examine
 *
 * note : It performs this test in a more optimal way by
 *    avoiding authorization checking, and testing the workspace state before
 *    calling the more expensive fetch & lock function. For speed, we're not
 *    checking for connections unless we have to attempt to fetch the object.
 *
 */
int
db_is_deleted (DB_OBJECT * obj)
{
  int error;

  CHECK_1ARG_ERROR (obj);

  if (obj->deleted)
    {
      return 1;
    }

  /* If we have obtained any lock except X_LOCK, that means it is real.
   * However deleted bit is off and to hold X_LOCK does not guarantee it exists,
   * Therefore we need to check it if we hold X_LOCK on the object.
   */
  if (NULL_LOCK < obj->ws_lock && obj->ws_lock < X_LOCK)
    {
      return 0;
    }

  /* couldn't figure it out from the MOP hints, we'll have to try fetching it,
   * note that we're acquiring a read lock here, this may be bad if
   * we really intend to update the object.
   */
  error = obj_lock (obj, 0);

  if (!error)
    {
      return 0;
    }

  /* if this is the deleted object error, then its deleted, the test
   * for the MOP deleted flag should be unnecessary but be safe.
   */
  if (error == ER_HEAP_UNKNOWN_OBJECT || obj->deleted)
    {
      return 1;
    }

  return error;
}
#endif

/*
 *  CLASS INFORMATION
 */

/*
 * db_get_type_name() - This function maps a type identifier constant to a
 *    printable string containing the type name.
 * return : string containing type name (or NULL if error)
 * type_id(in): an integer type identifier (DB_TYPE enumeration)
 */
const char *
db_get_type_name (DB_TYPE type_id)
{
  const char *name = NULL;

  name = pr_type_name (type_id);
  if (name == NULL)
    {
      name = "unknown primitive type identifier";
    }

  return (name);
}

/*
 *  ATTRIBUTE ACCESSORS
 */

/*
 * db_get_attribute() - This function returns a structure that describes the
 *    definition of an attribute.
 * return : an attribute descriptor
 * obj(in): class or instance
 * name(in): attribute name
 *
 * note : returned structure can be examined by using the db_attribute_
 *        functions.
 */
DB_ATTRIBUTE *
db_get_attribute (DB_OBJECT * obj, const char *name)
{
  SM_CLASS *class_;
  SM_ATTRIBUTE *att;

  CHECK_CONNECT_NULL ();

  if (obj == NULL || name == NULL)
    {
      return NULL;
    }

  att = NULL;
  if (au_fetch_class (obj, &class_, S_LOCK, AU_SELECT) == NO_ERROR)
    {
      att = classobj_find_attribute (class_->attributes, name);
      if (att == NULL)
        {
          er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ATTRIBUTE, 1, name);
        }
    }

  return ((DB_ATTRIBUTE *) att);
}

/*
 * db_get_attribute_by_name() - This function returns a structure that
 *    describes the definition of an attribute.
 * return : an attribute descriptor
 * class_name(in): class name
 * name(in): attribute name
 *
 * note : returned structure can be examined by using the db_attribute_
 *        functions.
 */
DB_ATTRIBUTE *
db_get_attribute_by_name (const char *class_name, const char *atrribute_name)
{
  DB_OBJECT *db_obj = NULL;

  if (class_name == NULL || atrribute_name == NULL)
    {
      return NULL;
    }

  db_obj = sm_find_class (class_name);
  if (db_obj == NULL)
    {
      return NULL;
    }

  return db_get_attribute (db_obj, atrribute_name);
}

/*
 * db_get_attributes() - This function Returns descriptors for all of the
 *    attributes of a class. The attribute descriptors are maintained in
 *    a linked list so you can iterate through them using the db_attribute_next
 *    function.
 * return : attribute descriptor (in a list)
 * obj(in): class or instance
 */
DB_ATTRIBUTE *
db_get_attributes (DB_OBJECT * obj)
{
  SM_CLASS *class_;
  SM_ATTRIBUTE *atts;

  CHECK_CONNECT_NULL ();
  CHECK_1ARG_NULL (obj);

  atts = NULL;
  if (au_fetch_class (obj, &class_, S_LOCK, AU_SELECT) == NO_ERROR)
    {
      /* formerly returned the attribute list */
      /* atts = class_->attributes; */
      atts = class_->ordered_attributes;
      if (atts == NULL)
        {
          er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_NO_COMPONENTS, 0);
        }
    }

  return ((DB_ATTRIBUTE *) atts);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_attribute_type() - This function returns the basic type constant
 *    for an attribute.
 * return : type identifier constant
 * attribute(in): attribute descriptor
 */
DB_TYPE
db_attribute_type (DB_ATTRIBUTE * attribute)
{
  DB_TYPE type = DB_TYPE_NULL;

  if (attribute != NULL)
    {
      type = attribute->type->id;
    }

  return (type);
}
#endif

/*
 * db_attribute_next() - This function is used to iterate through a list of
 *    attribute descriptors such as that returned from the db_get_attributes()
 *    function.
 * return : attribute descriptor (or NULL if end of list)
 * attribute(in): attribute descriptor
 */
DB_ATTRIBUTE *
db_attribute_next (DB_ATTRIBUTE * attribute)
{
  DB_ATTRIBUTE *next = NULL;

  if (attribute != NULL)
    {
      next = attribute->order_link;
    }

  return next;
}

/*
 * db_attribute_name() - This function gets the name string for an attribute.
 * return : C string containing name
 * attribute(in): attribute descriptor
 */
const char *
db_attribute_name (DB_ATTRIBUTE * attribute)
{
  const char *name = NULL;

  if (attribute != NULL)
    {
      name = attribute->name;
    }

  return (name);
}

/*
 * db_attribute_order() - This function returns the position of an attribute
 *    within the attribute list for the class. This list is ordered according
 *    to the original class definition.
 * return : internal attribute identifier
 * attribute(in): attribute descriptor
 */
int
db_attribute_order (DB_ATTRIBUTE * attribute)
{
  int order = -1;

  if (attribute != NULL)
    {
      order = attribute->order;
    }

  return (order);
}

/*
 * db_attribute_domain() - This function returns the complete domain descriptor
 *    for an attribute.
 * return : domain descriptor
 * attribute(in): attribute descriptor
 *
 * note : The domain information is examined using the db_domain_ functions.
 */
DB_DOMAIN *
db_attribute_domain (DB_ATTRIBUTE * attribute)
{
  DB_DOMAIN *domain = NULL;

  if (attribute != NULL)
    {
      domain = attribute->sma_domain;
      assert (TP_DOMAIN_TYPE (domain) != DB_TYPE_VARIABLE);

      /* always filter the domain before returning to the higher levels */
      sm_filter_domain (domain);
    }

  return (domain);
}

/*
 * db_attribute_class() - This function returns a pointer to the class that is
 *    the source of this attribute in the class hierarchy. This can be used to
 *    see if an attribute was inherited from another class or if it was defined
 *    against the current class.
 * return : a pointer to a class
 * attribute(in): an attribute descriptor
 */
DB_OBJECT *
db_attribute_class (DB_ATTRIBUTE * attribute)
{
  DB_OBJECT *class_mop = NULL;

  if (attribute != NULL)
    {
      class_mop = attribute->class_mop;
    }

  return (class_mop);
}

/*
 * db_attribute_default() - This function returns the default value of
 *    an attribute.
 * return : a value container
 * attribute(in): attribute descriptor
 */
DB_VALUE *
db_attribute_default (DB_ATTRIBUTE * attribute)
{
  DB_VALUE *value = NULL;

  if (attribute != NULL)
    {
      value = &attribute->default_value.value;
    }

  return (value);
}

/*
 * db_attribute_is_unique() - This function tests the status of the UNIQUE
 *    integrity constraint for an attribute.
 * return : non-zero if unique is defined
 * attribute(in): attribute descriptor
 */
int
db_attribute_is_unique (DB_ATTRIBUTE * attribute)
{
  int status = 0;

  if (attribute != NULL)
    {
      SM_CONSTRAINT *con;

      for (con = attribute->constraints; con != NULL && !status; con = con->next)
        {
          if (con->type == SM_CONSTRAINT_UNIQUE || con->type == SM_CONSTRAINT_PRIMARY_KEY)
            {
              status = 1;
            }
        }
    }

  return (status);
}

/*
 * db_attribute_is_primary_key() - This function tests the status of the
 *    PRIMARY KEY integrity constraint for an attribute.
 * return : non-zero if primary key is defined
 * attribute(in): attribute descriptor
 */
int
db_attribute_is_primary_key (DB_ATTRIBUTE * attribute)
{
  int status = 0;

  if (attribute != NULL)
    {
      SM_CONSTRAINT *con;

      for (con = attribute->constraints; con != NULL && !status; con = con->next)
        {
          if (con->type == SM_CONSTRAINT_PRIMARY_KEY)
            {
              status = 1;
            }
        }
    }

  return (status);
}

/*
 * db_attribute_is_non_null() - This function tests the status of the NON_NULL
 *    integrity constraint for an attribute.
 * return : non-zero if non_null is defined.
 * attribute(in): attribute descriptor
 */
int
db_attribute_is_non_null (DB_ATTRIBUTE * attribute)
{
  int status = 0;

  if (attribute != NULL)
    {
      status = attribute->flags & SM_ATTFLAG_NON_NULL;
    }

  return (status);
}

/*
 * db_attribute_is_indexed() - This function tests to see if an attribute is
 *    indexed. Similar to db_is_indexed but works directly off the attribute
 *    descriptor structure.
 * return: non-zero if attribute is indexed
 * attributre(in): attribute descriptor
 */
int
db_attribute_is_indexed (DB_ATTRIBUTE * attribute)
{
  int status = 0;

  if (attribute != NULL)
    {
      SM_CONSTRAINT *con;

      for (con = attribute->constraints; con != NULL && !status; con = con->next)
        {
          if (SM_IS_CONSTRAINT_INDEX_FAMILY (con->type))
            {
              status = 1;
            }
        }
    }

  return (status);
}

/*
 *  CONSTRAINT ACCESSORS
 */

/*
 * db_get_constraints() - This function returns descriptors for all of the
 *    constraints of a class. The constraint descriptors are maintained in
 *    a linked list so that you can iterate through them using the
 *    db_constraint_next() function.
 * return : constraint descriptor list (or NULL if the object does not
 *          contain any constraints)
 * obj(in): class or instance
 */
DB_CONSTRAINT *
db_get_constraints (DB_OBJECT * obj)
{
  SM_CLASS *class_;
  SM_CLASS_CONSTRAINT *constraints;

  CHECK_CONNECT_NULL ();
  CHECK_1ARG_NULL (obj);

  constraints = NULL;
  if (au_fetch_class (obj, &class_, S_LOCK, AU_SELECT) == NO_ERROR)
    {
      constraints = class_->constraints;
      if (constraints == NULL)
        {
          er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_NO_COMPONENTS, 0);
        }
    }

  return ((DB_CONSTRAINT *) constraints);
}


/*
 * db_constraint_next() - This function is used to iterate through a list of
 *    constraint descriptors returned by the db_get_constraint() function.
 * return:constraint descriptor (or NULL if end of list)
 * constraint(in): constraint descriptor
 */
DB_CONSTRAINT *
db_constraint_next (DB_CONSTRAINT * constraint)
{
  DB_CONSTRAINT *next = NULL;

  if (constraint != NULL)
    {
      next = constraint->next;
    }

  return (next);
}

/*
 * db_constraint_type()- This function is used to return the type of constraint
 * return : internal constraint identifier
 * constraint(in): constraint descriptor
 */
DB_CONSTRAINT_TYPE
db_constraint_type (const DB_CONSTRAINT * constraint)
{
  DB_CONSTRAINT_TYPE type = DB_CONSTRAINT_INDEX;

  if (constraint != NULL)
    {
      if (constraint->type == SM_CONSTRAINT_UNIQUE)
        {
          type = DB_CONSTRAINT_UNIQUE;
        }
      else if (constraint->type == SM_CONSTRAINT_INDEX)
        {
          type = DB_CONSTRAINT_INDEX;
        }
      else if (constraint->type == SM_CONSTRAINT_NOT_NULL)
        {
          type = DB_CONSTRAINT_NOT_NULL;
        }
      else if (constraint->type == SM_CONSTRAINT_PRIMARY_KEY)
        {
          type = DB_CONSTRAINT_PRIMARY_KEY;
        }
      else
        {
          assert (false);
        }
    }

  return (type);
}

/*
 * db_constraint_name() - This function returns the name string
 *    for a constraint.
 * return : C string containing name
 * constraint(in): constraint descriptor
 */
const char *
db_constraint_name (DB_CONSTRAINT * constraint)
{
  const char *name = NULL;

  if (constraint != NULL)
    {
      name = constraint->name;
    }

  return (name);
}


/*
 * db_constraint_attributes() - This function returns an array of attributes
 *    that belong to the constraint. Each element of the NULL-terminated array
 *    is a pointer to an DB_ATTRIBUTE structure.
 * return : NULL terminated array of attribute structure pointers
 * constraint: constraint descriptor
 */
DB_ATTRIBUTE **
db_constraint_attributes (DB_CONSTRAINT * constraint)
{
  SM_ATTRIBUTE **atts = NULL;

  if (constraint != NULL)
    {
      atts = constraint->attributes;
    }

  return ((DB_ATTRIBUTE **) atts);
}

/*
 * db_constraint_asc_desc() - This function returns an array of asc/desc info
 * return : non-NULL terminated integer array
 * constraint: constraint descriptor
 */
const int *
db_constraint_asc_desc (DB_CONSTRAINT * constraint)
{
  const int *asc_desc = NULL;

  if (constraint != NULL)
    {
      asc_desc = constraint->asc_desc;
    }

  return asc_desc;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_constraint_index() - This function returns the BTID of index constraint.
 * return : C string containing name
 * constraint(in): constraint descriptor
 */
BTID *
db_constraint_index (DB_CONSTRAINT * constraint, BTID * index)
{
  if (constraint != NULL)
    {
      BTID_COPY (index, &(constraint->index_btid));
    }

  return index;
}
#endif

/*
 * db_get_class_num_objs_and_pages() -
 * return : error code
 * classmop(in): class object
 * approximation(in):
 * nobjs(out): number of objects in this classmop
 * npages(out): number of pages in this classmop
 */
int
db_get_class_num_objs_and_pages (DB_OBJECT * classmop, int approximation, int64_t * nobjs, int *npages)
{
  HFID *hfid;
  int error;

  if (classmop == NULL)
    {
      return ER_FAILED;
    }

  hfid = sm_get_heap (classmop);
  if (hfid == NULL)
    {
      return ER_FAILED;
    }

  error = heap_get_class_num_objects_pages (hfid, approximation, nobjs, npages);

  return error;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_get_class_privilege() -
 * return : error code
 * classmop(in):
 * auth(out):
 */
int
db_get_class_privilege (DB_OBJECT * mop, unsigned int *auth)
{
  if (mop == NULL)
    {
      return ER_FAILED;
    }
  return au_get_class_privilege (mop, auth);
}
#endif
