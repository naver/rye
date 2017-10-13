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
 * schema_manager.h - External definitions for the schema manager
 */

#ifndef _SCHEMA_MANAGER_H_
#define _SCHEMA_MANAGER_H_

#ident "$Id$"

#include "language_support.h"	/* for international string functions */
#include "storage_common.h"	/* for HFID */
#include "object_domain.h"	/* for TP_DOMAIN */
#include "work_space.h"		/* for MOP */
#include "class_object.h"	/* for SM_CLASS */
#include "schema_template.h"	/* template interface */
#include "dbdef.h"

/*
 * This is NOT the "object" class but rather functions more like
 * the meta-class of class objects.
 * This formerly stored the list of classes that had no super classes,
 * in that way it was kind of like the root "object" of the class
 * hierarchy.  Unfortunately, maintaining this list caused contention
 * problems on the the root object so it was removed.  The list
 * of base classes is now generated manually by examining all classes.
 */
typedef struct root_class ROOT_CLASS;

struct root_class
{
  SM_CLASS_HEADER header;
};

/*
 * Structure used when truncating a class and changing an attribute.
 * During these operations, indexes are dropped and recreated.
 * The information needed to recreate the constraints (indexes) are saved in
 * this structure.
 */
typedef struct sm_constraint_info SM_CONSTRAINT_INFO;

struct sm_constraint_info
{
  struct sm_constraint_info *next;
  char *name;
  char **att_names;
  int *asc_desc;
  char *ref_cls_name;
  char **ref_attrs;
  DB_CONSTRAINT_TYPE constraint_type;
};

extern ROOT_CLASS sm_Root_class;

extern const char TEXT_CONSTRAINT_PREFIX[];

extern MOP sm_Root_class_mop;
extern HFID *sm_Root_class_hfid;
extern const char *sm_Root_class_name;

extern int smt_finish_class (SM_TEMPLATE * template_, MOP * classmop);
extern int sm_delete_class_mop (MOP op);
#if defined(ENABLE_UNUSED_FUNCTION)
extern int sm_delete_class (const char *name);

extern int sm_get_index (MOP classop, const char *attname, BTID * index);
#endif
extern char *sm_produce_constraint_name (const char *class_name,
					 DB_CONSTRAINT_TYPE constraint_type,
					 const char **att_names,
					 const int *asc_desc,
					 const char *given_name);
extern char *sm_produce_constraint_name_mop (MOP classop,
					     DB_CONSTRAINT_TYPE
					     constraint_type,
					     const char **att_names,
					     const int *asc_desc,
					     const char *given_name);
extern char *sm_produce_constraint_name_tmpl (SM_TEMPLATE * tmpl,
					      DB_CONSTRAINT_TYPE
					      constraint_type,
					      const char **att_names,
					      const int *asc_desc,
					      const char *given_name);
extern int sm_add_constraint (MOP classop,
			      DB_CONSTRAINT_TYPE constraint_type,
			      const char *constraint_name,
			      const char **att_names, const int *asc_desc);
extern int sm_drop_constraint (MOP classop,
			       DB_CONSTRAINT_TYPE constraint_type,
			       const char *constraint_name,
			       const char **att_names);

/* Misc schema operations */
extern int sm_rename_class (MOP op, const char *new_name);
extern void sm_mark_system_classes (void);
extern int sm_update_all_catalog_statistics (bool update_stats,
					     bool with_fullscan);
extern int sm_update_catalog_statistics (const char *class_name,
					 bool update_stats,
					 bool with_fullscan);
extern int sm_force_write_all_classes (void);
#ifdef SA_MODE
extern void sm_mark_system_class_for_catalog (void);
#endif /* SA_MODE */
extern int sm_mark_system_class (MOP classop, int on_or_off);
extern int sm_is_system_table (MOP op);
extern bool sm_is_shard_table (MOP op);
#if defined (ENABLE_UNUSED_FUNCTION)
extern const char *sm_shard_key_col_name (MOP op);
#endif
extern int sm_set_class_flag (MOP classop, SM_CLASS_FLAG flag, int onoff);
extern int sm_get_class_flag (MOP op, SM_CLASS_FLAG flag);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int sm_destroy_representations (MOP op);
#endif

extern int sm_save_constraint_info (SM_CONSTRAINT_INFO ** save_info,
				    const SM_CLASS_CONSTRAINT * const c);
extern void sm_free_constraint_info (SM_CONSTRAINT_INFO ** save_info);


/* Utility functions */
extern int sm_check_name (const char *name);

/* Class location functions */
extern MOP sm_get_class (MOP obj);
extern SM_CLASS_TYPE sm_get_class_type (SM_CLASS * class_);

extern DB_OBJLIST *sm_fetch_all_classes (LOCK lock);
extern DB_OBJLIST *sm_fetch_all_objects (DB_OBJECT * op, LOCK lock);

/* Domain maintenance */
extern int sm_filter_domain (TP_DOMAIN * domain);
#if !defined (SERVER_MODE)
extern int sm_check_object_domain (TP_DOMAIN * domain, MOP object);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int sm_check_class_domain (TP_DOMAIN * domain, MOP class_);
extern int sm_coerce_object_domain (TP_DOMAIN * domain, MOP object,
				    MOP * dest_object);
#endif
#endif

/* Extra cached state */
extern int sm_clean_class (MOP classmop, SM_CLASS * class_);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int sm_touch_class (MOP classmop);
#endif

/* Statistics functions */
extern SM_CLASS *sm_get_class_with_statistics (MOP classop);
#if defined (ENABLE_UNUSED_FUNCTION)
extern CLASS_STATS *sm_get_statistics_force (MOP classop);
#endif
extern int sm_update_statistics (MOP classop, bool update_stats,
				 bool with_fullscan);
extern int sm_update_all_statistics (bool update_stats, bool with_fullscan);

/* Misc information functions */
extern const char *sm_class_name (MOP op);
extern int sm_object_size_quick (SM_CLASS * class_, MOBJ obj);
extern SM_CLASS_CONSTRAINT *sm_class_constraints (MOP classop);

/* Locator support functions */
extern const char *sm_classobj_name (MOBJ classobj);
extern HFID *sm_heap (MOBJ clobj);
extern HFID *sm_get_heap (MOP classmop);
#if defined (ENABLE_UNUSED_FUNCTION)
extern bool sm_has_indexes (MOBJ class_);
#endif

/* Interpreter support functions */
extern void sm_downcase_name (const char *name, char *buf, int maxlen);
extern MOP sm_find_class (const char *name);

extern const char *sm_get_att_name (MOP classop, int id);
extern int sm_att_id (MOP classop, const char *name);
#if defined (ENABLE_UNUSED_FUNCTION)
extern DB_TYPE sm_att_type_id (MOP classop, const char *name);

extern MOP sm_att_class (MOP classop, const char *name);
#endif
extern int sm_att_info (MOP classop, const char *name, int *idp,
			TP_DOMAIN ** domainp);
extern int sm_att_constrained (MOP classop, const char *name,
			       SM_ATTRIBUTE_FLAG cons);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int sm_att_default_value (MOP classop, const char *name,
				 DB_VALUE * value,
				 DB_DEFAULT_EXPR_TYPE * default_expr);
#endif

extern BTID *sm_find_index (MOP classop, const char **att_names,
			    int num_atts,
			    bool unique_index_only, BTID * btid);

/* Query processor support functions */
extern unsigned int sm_local_schema_version (void);
extern void sm_bump_local_schema_version (void);
extern unsigned int sm_global_schema_version (void);
extern void sm_bump_global_schema_version (void);
extern struct parser_context *sm_virtual_queries (void *parent_parser,
						  DB_OBJECT * class_object);

#if 1				/* TODO - remove me someday */
extern int sm_flush_objects (MOP obj);
#endif
extern int sm_flush_and_decache_objects (MOP obj, int decache);

/* Workspace & Garbage collection functions */
extern int sm_issystem (SM_CLASS * class_);
extern int sm_isshard_table (SM_CLASS * class_);

/* Attribute & Method descriptors */
#if defined (ENABLE_UNUSED_FUNCTION)
extern int sm_get_attribute_descriptor (DB_OBJECT * op, const char *name,
					int for_update,
					SM_DESCRIPTOR ** desc);

extern void sm_free_descriptor (SM_DESCRIPTOR * desc);

extern int sm_get_descriptor_component (MOP op, SM_DESCRIPTOR * desc,
					int for_update,
					SM_CLASS ** class_ptr,
					SM_ATTRIBUTE ** att_ptr);
#endif

/* Module control */
extern void sm_final (void);
#if defined(ENABLE_UNUSED_FUNCTION)
extern void sm_transaction_boundary (void);
#endif

extern void sm_create_root (OID * rootclass_oid, HFID * rootclass_hfid);
extern void sm_init (OID * rootclass_oid, HFID * rootclass_hfid);
extern int sm_att_unique_constrained (MOP classop, const char *name);

/* currently this is a private function to be called only by AU_SET_USER */
extern int sc_set_current_schema (MOP user);
/* Obtain (pointer to) current schema name.                            */

#if defined (ENABLE_UNUSED_FUNCTION)
extern int sm_has_non_null_attribute (SM_ATTRIBUTE ** attrs);
#endif

#endif /* _SCHEMA_MANAGER_H_ */
