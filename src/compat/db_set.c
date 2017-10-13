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
 * db_set.c - API functions for manipulating sets & sequences.
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <ctype.h>
#include <string.h>

#include "system_parameter.h"
#include "storage_common.h"
#include "db.h"
#include "class_object.h"
#include "server_interface.h"
#include "set_object.h"

#if !defined(SERVER_MODE)
#include "object_print.h"
#include "boot_cl.h"
#include "locator_cl.h"
#include "schema_manager.h"
#include "schema_template.h"
#include "object_accessor.h"
#endif

#include "parser.h"

#include "dbval.h"		/* this must be the last header file included!!! */

#define ERROR_SET(error, code) \
  do {                     \
    error =  code;         \
    er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, code, 0); \
  } while (0)

#define ERROR_SET1(error, code, arg1) \
  do {                            \
    error = code;                 \
    er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, code, 1, arg1); \
  } while (0)

/*
 *  SET CREATION FUNCTIONS
 */

/*
 * db_seq_create() - This function creates an empty sequence. The class and
 *    name arguments can be set to NULL. If values are supplied, a check will
 *    be made to make sure that the attribute was defined with the sequence
 *    domain.
 * return : a set (sequence) descriptor
 * classop(in): class or instance
 * name(in): attribute name
 * size(in): initial size
 */
DB_SET *
db_seq_create (MOP classop, const char *name, int size)
{
  DB_SET *set;
#if !defined(SERVER_MODE)
  int error = NO_ERROR;
#endif

  CHECK_CONNECT_NULL ();

  set = NULL;
  if (classop == NULL || name == NULL)
    {
      set = set_create_sequence (size);
    }
  else
    {
#if !defined(SERVER_MODE)
      SM_CLASS *class_;
      SM_ATTRIBUTE *att;

      if (au_fetch_class (classop, &class_, S_LOCK, AU_SELECT) == NO_ERROR)
	{
	  att = classobj_find_attribute (class_->attributes, name);
	  if (att == NULL)
	    {
	      ERROR_SET1 (error, ER_OBJ_INVALID_ATTRIBUTE, name);
	    }
	  else
	    {
	      if (att->type->id == DB_TYPE_SEQUENCE)
		{
		  set = set_create_sequence (size);
		}
	      else
		{
		  ERROR_SET1 (error, ER_OBJ_DOMAIN_CONFLICT, name);
		}
	    }
	}
#endif
    }

  return (set);
}

/*
 *  SET/MULTI-SET FUNCTIONS
 */

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_set_add() - This function adds an element to a set or multiset. If the
 *    set is a basic set and the value already exists in the set, a zero is
 *    returned, indicating that no error occurred. If the set is a multiset,
 *    the value will be added even if it already exists. If the set has been
 *    assigned as the value of an attribute, the domain of the value is first
 *    checked against the attribute domain. If they do not match, a zero is
 *    returned, indicating that no error occurred.
 * return : error code
 * set(in): set descriptor
 * value(in): value to add
 *
 * note : you may not make any assumptions about the position of the value
 *    within the set; it will be added wherever the system determines is most
 *    convenient. If you need to have sets with ordered elements, you must use
 *    sequences. Sets and multi-sets cannot contain NULL elements, if the value
 *    has a basic type of DB_TYPE_NULL, an error is returned.
 */
int
db_set_add (DB_SET * set, DB_VALUE * value)
{
  int error = NO_ERROR;

  CHECK_CONNECT_ERROR ();
  CHECK_1ARG_ERROR (set);

  /*  Check if modifications are disabled only if the set is owned */
  if (set->owner != NULL)
    {
      CHECK_MODIFICATION_ERROR ();
    }

  if ((value != NULL) && (DB_VALUE_TYPE (value) > DB_TYPE_LAST))
    {
      ERROR_SET (error, ER_OBJ_INVALID_ARGUMENTS);
    }
  else
    {
      error = set_add_element (set, value);
    }

  return (error);
}
#endif

/*
 * db_set_get() - This function gets the value of an element of a set or
 *    multiset. This is the only set or multiset function that accepts an
 *    index. The first element of the set is accessed with an index of 0
 *    (zero). The index is used to sequentially retrieve elements and assumes
 *    that the set will not be modified for the duration of the set iteration
 *    loop. You cannot assume that the elements of the set remain in any
 *    particular order after a db_set_add statement.
 * return : error code
 * set(in): set descriptor
 * index(in) : element index
 * value(out): value container to be filled in with element value
 *
 * note : The supplied value container is filled in with a copy of the set
 *    element contents and must be freed with db_value_clear or db_value_free
 *    when the value is no longer required.
 */
int
db_set_get (DB_SET * set, int index, DB_VALUE * value)
{
  int retval;

  CHECK_CONNECT_ERROR ();
  CHECK_2ARGS_ERROR (set, value);

  retval = (set_get_element (set, index, value));

  return (retval);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_set_drop() - This function removes the first matching element from a set
 *    or multiset. If no element matches the supplied value, a zero is
 *    returned, indicating no error occurred. If more than one element matches,
 *    only the first one is removed.
 * return : error code
 * set(in): set descriptor
 * value(in): value to drop from the set
 */
int
db_set_drop (DB_SET * set, DB_VALUE * value)
{
  int retval;

  CHECK_CONNECT_ERROR ();
  CHECK_1ARG_ERROR (set);

  /*  Check if modifications are disabled only if the set is owned */
  if (set->owner != NULL)
    {
      CHECK_MODIFICATION_ERROR ();
    }

  retval = (set_drop_element (set, value, false));

  return (retval);
}
#endif

/*
 * db_set_size() - This function returns the total number of elements in a set,
 *    including elements that may have a NULL value. This function should
 *    always be used prior to using program loops to iterate over the set
 *    elements.
 * return : total number of elements in the set
 * set(in): set descriptor
 */
int
db_set_size (DB_SET * set)
{
  int retval;

  CHECK_CONNECT_MINUSONE ();
  CHECK_1ARG_MINUSONE (set);

  /* allow all types */
  retval = (set_size (set));

  return (retval);
}

#if defined (RYE_DEBUG)
/*
 * db_set_print() - This is a debugging function that prints a simple
 *    description of a set. This should be used for information purposes only.
 * return : error code
 * set(in): set descriptor
 */
int
db_set_print (DB_SET * set)
{
  CHECK_CONNECT_ERROR ();
  CHECK_1ARG_ERROR (set);

  /* allow all types */
  set_print (set);

  return (NO_ERROR);
}
#endif

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_set_type() - This function returns the type identifier for a set. This
 *    can be used in places where it is not known if a set descriptor is for
 *    a set, multi-set or sequence.
 * return : set type identifier
 * set(in): set descriptor
 */
DB_TYPE
db_set_type (DB_SET * set)
{
  DB_TYPE type = DB_TYPE_NULL;

  if (set != NULL)
    {
      type = set_get_type (set);
    }

  return (type);
}
#endif

/*
 * SEQUENCE FUNCTIONS
 */

/*
 * db_seq_get() - This function retrieves the value of a sequence element.
 *    The first element of the sequence is accessed with an index of 0 (zero).
 * return : error code
 * set(in): sequence identifier
 * index(in): element index
 * value(out): value to be filled in with element contents
 *
 * note : The value will be copied from the sequence, so it must be released
 *    using either function db_value_clear() or db_value_free() when it is
 *    no longer needed.
 */
int
db_seq_get (DB_SET * set, int index, DB_VALUE * value)
{
  int retval;

  CHECK_CONNECT_ERROR ();
  CHECK_2ARGS_ERROR (set, value);

  /* should make sure this is a sequence, probably introduce another
     set_ level function to do this rather than checking the type here */
  retval = (set_get_element (set, index, value));

  return (retval);
}

/*
 * db_seq_put() - This function puts a value in a sequence at a fixed position.
 *    The value will always remain in the specified position so you can use
 *    db_seq_get() with the same index to retrieve it at a later time.
 *    This will overwrite the current value at that position if one exists. The
 *    value can be of type DB_TYPE_NULL, in which case the current element will
 *    be cleared and set to NULL.
 * return : error code
 * set(in): sequence descriptor
 * index(in): element index
 * value(in): value to put in the sequence
 *
 * note : The domain of the value must be compatible with the domain of the set
 *   (if the set has been assigned to an attribute). If the set does not have
 *   an element with the specified index, it will automatically grow to be as
 *   large as the given index. The empty elements (if any) between the former
 *   length of the set and the new index will be set to DB_TYPE_NULL.
 */
int
db_seq_put (DB_SET * set, int index, DB_VALUE * value)
{
  int error = NO_ERROR;

  CHECK_CONNECT_ERROR ();
  CHECK_1ARG_ERROR (set);

  /*  Check if modifications are disabled only if the set is owned */
  if (set->owner != NULL)
    {
      CHECK_MODIFICATION_ERROR ();
    }

  if ((value != NULL) && (DB_VALUE_TYPE (value) > DB_TYPE_LAST))
    {
      ERROR_SET (error, ER_OBJ_INVALID_ARGUMENTS);
    }
  else
    {
      error = set_put_element (set, index, value);
    }

  return (error);
}

/*
 * db_seq_size() - This function returns the total number of slots allocated
 *    for a sequence.
 * return : total number of elements in the sequence
 * set(in): sequence descriptor
 */
int
db_seq_size (DB_SET * set)
{
  int retval;

  CHECK_CONNECT_MINUSONE ();
  CHECK_1ARG_MINUSONE (set);

  retval = (set_size (set));

  return (retval);
}

/*
 *  GENERIC COLLECTION FUNCTIONS
 */

/*
 * The new DB_COLLECTION style of collection maintenance is preferred over
 * the old SEQUENCE distinction.  Its easier to code against
 * and makes switching between collection types easier.
 *
 */

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_col_create() - This is the primary function for constructing a new
 *    collection. Type should be DB_TYPE_SEQUENCE.
 *    Size should always be specified if known before
 *    hand as it will significantly improve performance when incrementally
 *    building collections. Domain is optional. If NULL, it is assumed that
 *    this is a "wildcard" collection and can contain elements of any domain.
 *    The elements already existing in the collection should be within the
 *    supplied domain.If a domain is supplied, the type argument is ignored.
 * return : collection (NULL if error)
 * type(in): one of the DB_TYPE_ collection types
 * size(in): initial size to preallocate (zero if unknown)
 * domain(in): fully specified domain (optional)
 */
DB_COLLECTION *
db_col_create (DB_TYPE type, int size, DB_DOMAIN * domain)
{
  DB_COLLECTION *col;

  CHECK_CONNECT_NULL ();

  if (domain != NULL)
    {
      col = set_create_with_domain (domain, size);
    }
  else
    {
      col = set_create (type, size);
    }

  return col;
}

/*
 * db_col_size() - This function is used to obtain the number of elements found
 *    within the collection.
 * return : number of elements in the collection
 * col(in): collection
 */
int
db_col_size (DB_COLLECTION * col)
{
  int size;

  CHECK_CONNECT_MINUSONE ();
  CHECK_1ARG_MINUSONE (col);

  size = set_size (col);

  return size;
}

/*
 * db_col_get() - This function is the primary function for retrieving values
 *    out of a collection. It can be used for collections of all types.
 * return : error status
 * col(in): collection
 * element_index(in): index of element to access
 * value(in): container in which to store value
 *
 * note : The collection is guaranteed to
 *    retain its current order as long as no modifications are made to the
 *    collection. This makes it possible to iterate over the elements of a
 *    collection using an index. Iterations over a collection are normally
 *    performed by first obtaining the size of the collection with the
 *    db_col_size() function and then entering a loop whose index begins at 0
 *    and ends at the value of db_col_size() minus one.
 */
int
db_col_get (DB_COLLECTION * col, int element_index, DB_VALUE * value)
{
  int error;

  CHECK_CONNECT_ERROR ();
  CHECK_2ARGS_ERROR (col, value);

  error = set_get_element (col, element_index, value);

  return error;
}

/*
 * db_col_type() - This function returns the base type for this collection.
 * return : DB_TYPE_SEQUENCE.
 * col(in): collection
 */
DB_TYPE
db_col_type (DB_COLLECTION * col)
{
  DB_TYPE type = DB_TYPE_NULL;

  CHECK_CONNECT_ERROR ();
  CHECK_1ARG_ERROR_WITH_TYPE (col, DB_TYPE);

  type = set_get_type (col);

  return type;
}
#endif
