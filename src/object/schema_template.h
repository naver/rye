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
 * schema_template.h - Definitions for the schema template interface
 */

#ifndef _SCHEMA_TEMPLATE_H_
#define _SCHEMA_TEMPLATE_H_

#ident "$Id$"

#include "work_space.h"
#include "class_object.h"

/* Template creation */
extern SM_TEMPLATE *smt_def_class (const char *name);
extern SM_TEMPLATE *smt_edit_class_mop_with_lock (MOP class_, LOCK lock);
#if defined(ENABLE_UNUSED_FUNCTION)
extern SM_TEMPLATE *smt_edit_class (const char *name);
#endif /* ENABLE_UNUSED_FUNCTION */
extern SM_TEMPLATE *smt_copy_class_mop (const char *name, MOP op,
					SM_CLASS ** class_);
extern SM_TEMPLATE *smt_copy_class (const char *new_name,
				    const char *existing_name,
				    SM_CLASS ** class_);
extern int smt_quit (SM_TEMPLATE * template_);

/* Virtual class support */
extern SM_TEMPLATE *smt_def_typed_class (const char *name, SM_CLASS_TYPE ct);
extern SM_CLASS_TYPE smt_get_class_type (SM_TEMPLATE * template_);

/* Attribute definition */
extern int smt_add_attribute_w_dflt (DB_CTMPL * def,
				     const char *name,
				     const char *domain_string,
				     DB_DOMAIN * domain,
				     DB_VALUE * default_value,
				     DB_DEFAULT_EXPR_TYPE default_expr,
				     bool is_shard_key);

extern int smt_add_attribute_w_dflt_w_order (DB_CTMPL * def,
					     const char *name,
					     const char *domain_string,
					     DB_DOMAIN * domain,
					     DB_VALUE * default_value,
					     const bool add_first,
					     const char *add_after_attribute,
					     DB_DEFAULT_EXPR_TYPE
					     default_expr, bool is_shard_key);

extern int smt_add_attribute_any (SM_TEMPLATE * template_,
				  const char *name,
				  const char *domain_string,
				  DB_DOMAIN * domain,
				  const bool add_first,
				  const char *add_after_attribute,
				  bool is_shard_key);

extern int smt_add_attribute (SM_TEMPLATE * template_,
			      const char *name,
			      const char *domain_string, DB_DOMAIN * domain);

#if defined(ENABLE_UNUSED_FUNCTION)
extern int smt_delete_set_attribute_domain (SM_TEMPLATE * template_,
					    const char *name,
					    const char *domain_string,
					    DB_DOMAIN * domain);
#endif

extern int smt_set_attribute_default (SM_TEMPLATE * template_,
				      const char *name,
				      DB_VALUE * value,
				      DB_DEFAULT_EXPR_TYPE default_expr);

extern int smt_add_constraint (SM_TEMPLATE * template_,
			       DB_CONSTRAINT_TYPE constraint_type,
			       const char *constraint_name,
			       const char **att_names, const int *asc_desc);

extern int smt_drop_constraint (SM_TEMPLATE * template_,
				const char **att_names,
				const char *constraint_name,
				SM_ATTRIBUTE_FLAG constraint);

extern int smt_add_index (SM_TEMPLATE * template_, const char *name,
			  int on_or_off);

extern int smt_find_attribute (SM_TEMPLATE * template_, const char *name,
			       SM_ATTRIBUTE ** attp);

/* Rename functions */
extern int smt_rename_any (SM_TEMPLATE * template_, const char *name,
			   const char *new_name);

/* Deletion functions */
extern int smt_delete_any (SM_TEMPLATE * template_, const char *name);
#if defined(ENABLE_UNUSED_FUNCTION)
extern int smt_delete (SM_TEMPLATE * template_, const char *name);
extern int smt_class_delete (SM_TEMPLATE * template_, const char *name);
#endif

/* Query_spec functions */
extern int smt_add_query_spec (SM_TEMPLATE * template_,
			       const char *specification);
extern int smt_change_attribute_pos (SM_TEMPLATE * template_,
				     const char *name,
				     const bool change_first,
				     const char *change_after_attribute);
extern int smt_change_attribute_domain (SM_TEMPLATE * template_,
					const char *name,
					TP_DOMAIN * new_domain);

#if defined(ENABLE_UNUSED_FUNCTION)
extern void smt_downcase_all_class_info (void);
#endif

#endif /* _SCHEMA_TEMPLATE_H_ */
