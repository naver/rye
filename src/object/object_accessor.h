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
 * object_accessor.h - Definitions for the object manager
 *
 */

#ifndef _OBJECT_ACCESSOR_H_
#define _OBJECT_ACCESSOR_H_

#ident "$Id$"

#include <stdarg.h>
#include "object_representation.h"
#include "class_object.h"
#include "object_template.h"
#include "authenticate.h"

#define OBJ_FORCE_SIMPLE_NULL_TO_UNBOUND(dbvalue) \
  if ((DB_VALUE_TYPE(dbvalue) == DB_TYPE_VARCHAR) && \
      (DB_GET_STRING(dbvalue) == NULL)) \
  DB_MAKE_NULL(dbvalue);


#if 0                           /* remove chn. not used, heyman */

/*
 *
 *       		      OBJECT HEADER FIELDS
 *
 *
 */

/*
 * These are the header fields that are present in every object.
 * Note that the first field in the header must be able to be overlayed
 * by the WS_OBJECT_HEADER structure.  This is so the workspace manager
 * can get to the CHN without knowing anything about the details of the
 * object structure.
 */
#endif

#define OBJ_HEADER_BOUND_BITS_OFFSET 0

/*
 * Try to define these in terms of the OR bound bit macros if possible.
 * When the time comes where we have to support heterogeneous networks,
 * this may no longer be an option.  We're lucky now in that the
 * disk representation for these is exactly the same as the memory
 * representation.
 *
 */

#define OBJ_BOUND_BIT_WORDS OR_BOUND_BIT_WORDS
#define OBJ_BOUND_BIT_BYTES OR_BOUND_BIT_BYTES

#define OBJ_GET_BOUND_BITS(obj) \
  ((char *)(obj) + OBJ_HEADER_BOUND_BITS_OFFSET)

#define OBJ_GET_BOUND_BIT(obj, element) \
  OR_GET_BOUND_BIT(OBJ_GET_BOUND_BITS(obj), element)

#define OBJ_SET_BOUND_BIT(obj, element) \
  OR_ENABLE_BOUND_BIT(OBJ_GET_BOUND_BITS(obj), element)

#define OBJ_CLEAR_BOUND_BIT(obj, element) \
  OR_CLEAR_BOUND_BIT(OBJ_GET_BOUND_BITS(obj), element)

extern char *obj_Method_error_msg;

/*
 *
 *       		    OBJECT ACCESS FUNCTIONS
 *
 *
 */

/* Creation and deletion */
extern MOP obj_create (MOP classop);
#if defined (ENABLE_UNUSED_FUNCTION)
extern MOP obj_copy (MOP op);
#endif
extern int obj_delete (MOP op);

/* Attribute access functions */

extern int obj_get (MOP op, const char *name, DB_VALUE * value);

extern int obj_get_att (MOP op, SM_CLASS * class_, SM_ATTRIBUTE * att, DB_VALUE * value);

#if defined (ENABLE_UNUSED_FUNCTION)
extern int obj_get_path (DB_OBJECT * object, const char *attpath, DB_VALUE * value);
#endif
extern int obj_set (MOP op, const char *name, DB_VALUE * value);

extern int obj_assign_value (MOP op, SM_ATTRIBUTE * att, char *mem, DB_VALUE * value);

#if defined(ENABLE_UNUSED_FUNCTION)
/* Attribute descriptor interface */
extern int obj_desc_set (MOP op, SM_DESCRIPTOR * desc, DB_VALUE * value);

extern int obj_send_stack (MOP obj, const char *name, DB_VALUE * returnval, ...);
extern int obj_desc_send_stack (MOP obj, SM_DESCRIPTOR * desc, DB_VALUE * returnval, ...);
/* backward compatibility, should use obj_send_list() */
extern int obj_send (MOP obj, const char *name, DB_VALUE * returnval, DB_VALUE_LIST * arglist);
extern int obj_isclass (MOP obj);

/* Tests */
extern bool obj_isinstance (MOP obj);
#endif

/* Misc operations */
extern int obj_lock (MOP op, int for_write);
extern MOP obj_find_unique (MOP op, const char *attname, DB_IDXKEY * key, LOCK lock);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int obj_repl_add_object (const char *class_name, DB_IDXKEY * key, int type, RECDES * recdes);
#endif

/* Internal support for specific modules */

/* called by Workspace */
extern void obj_free_memory (SM_CLASS * class_, MOBJ obj);

/* attribute locator for set handler */
#if !defined(SERVER_MODE)
extern int obj_locate_attribute (MOP op, int attid, int for_write, char **memp, SM_ATTRIBUTE ** attp);
#endif
extern char *obj_alloc (SM_CLASS * class_, int bound_bit_status);


extern int obj_get_value (MOP op, SM_ATTRIBUTE * att, void *mem, DB_VALUE * source, DB_VALUE * dest);
#if defined(ENABLE_UNUSED_FUNCTION)
extern int obj_find_unique_id (MOP op, const char *att_name, BTID * id_array, int id_array_size, int *total_ids);
#endif

#endif /* _OBJECT_ACCESSOR_H_ */
