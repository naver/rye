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
 * object_template.h - Definitions for the object manager
 */

#ifndef _OBJECT_TEMPLATE_H_
#define _OBJECT_TEMPLATE_H_

#ident "$Id$"

#include <stdarg.h>

#include "object_representation.h"
#include "class_object.h"

/*
 * OBJ_TEMPASSIGN
 *
 * Note :
 *    Substructure of OBJ_TEMPLATE.  Used to store information about
 *    a pending attribute assignment.
 */
typedef struct obj_tempassign
{

  struct obj_template *obj;     /* if new object assignment */
  DB_VALUE *variable;           /* if non-object assignment */

  /*
   * cache of attribute definition, must be verified as part
   * of the outer template validation
   */
  SM_ATTRIBUTE *att;

  unsigned is_default:1;

} OBJ_TEMPASSIGN;

/*
 * OBJ_TEMPLATE
 *
 * Note :
 *    Used to define a set of attribute assignments that are to be
 *    considered as a single operation.  Either all of the assignments
 *    will be applied or none.
 */
typedef struct obj_template
{
  /* edited object, NULL if insert template */

  MOP object;

  /* class cache, always set, matches the "object" field  */
  MOP classobj;
  SM_CLASS *class_;

  /* class cache validation info */
  int tran_id;                  /* transaction id at the time the template was created */
  unsigned int schema_id;       /* schema counter at the time the template was created */

  /* template assignment vector */
  OBJ_TEMPASSIGN **assignments;

  /* Number of assignments allocated for the vector */
  int nassigns;

  /* Used to detect cycles in the template hierarchy */
  unsigned int traversal;

  /* for detection of cycles in template hierarchy */
  unsigned traversed:1;

  /*
   * Set if we ever make an assignment for an attribute that has the
   * UNIQUE constraint.  Speeds up a common test.
   */
  unsigned uniques_were_modified:1;

} OBJ_TEMPLATE, *OBT;

/* OBJECT TEMPLATE FUNCTIONS */

extern OBJ_TEMPLATE *obt_def_object (MOP class_);
extern OBJ_TEMPLATE *obt_edit_object (MOP object);
extern int obt_quit (OBJ_TEMPLATE * template_ptr);

#if defined(ENABLE_UNUSED_FUNCTION)
extern int obt_set (OBJ_TEMPLATE * template_ptr, const char *attname, DB_VALUE * value);
#endif
extern void obt_set_label (OBJ_TEMPLATE * template_ptr, DB_VALUE * label);
extern int obt_update (OBJ_TEMPLATE * template_ptr, MOP * newobj);
extern int obt_assign (OBJ_TEMPLATE * template_ptr, SM_ATTRIBUTE * att, DB_VALUE * value);

/* UTILITY FUNCTIONS */

extern DB_VALUE *obt_check_assignment (SM_ATTRIBUTE * att, DB_VALUE * proposed_value, unsigned force_check_not_null);
extern int obt_update_internal (OBJ_TEMPLATE * template_ptr, MOP * newobj, int check_non_null);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int obt_find_attribute (OBJ_TEMPLATE * template_ptr, const char *name, SM_ATTRIBUTE ** attp);
extern int obt_desc_set (OBJ_TEMPLATE * template_ptr, SM_DESCRIPTOR * desc, DB_VALUE * value);
#endif
extern int obt_check_missing_assignments (OBJ_TEMPLATE * template_ptr);
#if defined(ENABLE_UNUSED_FUNCTION)
extern int obt_populate_known_arguments (OBJ_TEMPLATE * template_ptr);
extern void obt_retain_after_finish (OBJ_TEMPLATE * template_ptr);
#endif
#endif /* _OBJECT_TEMPLATE_H_ */
