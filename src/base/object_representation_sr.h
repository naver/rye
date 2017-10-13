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
 * object_representation_sr.h - Definitions for the server side functions
 *          that extract class information from disk representations
 */

#ifndef _OBJECT_REPRESENTATION_SR_H_
#define _OBJECT_REPRESENTATION_SR_H_

#ident "$Id$"

#include "storage_common.h"
#include "system_catalog.h"

#define OR_ATT_BTID_PREALLOC 8

typedef struct or_default_value OR_DEFAULT_VALUE;
struct or_default_value
{
  /* could this be converted to a server side DB_VALUE ? */
  void *value;			/* default value */
  int val_length;		/* default value length */
  DB_DEFAULT_EXPR_TYPE default_expr;	/* identifier for the pseudo-column
					 * default expression */
};

/*
 * OR_ATTRIBUTE
 *
 *    Server side memory representation of an attribute definition.
 *    Built from the disk representation of a class.
 *    Part of the OR_CLASSREP structure hierarchy.
 */

typedef struct or_attribute OR_ATTRIBUTE;
struct or_attribute
{
  OR_ATTRIBUTE *next;

  int id;			/* unique id */
  DB_TYPE type;			/* basic type */
  int def_order;		/* order of attributes in class */
  int location;			/* fixed offset or variable table index */
  int position;			/* storage position (list index) */
  OID classoid;			/* source class object id */

  OR_DEFAULT_VALUE default_value;	/* default value */
  OR_DEFAULT_VALUE current_default_value;	/* default value */
  BTID *btids;			/* B-tree ID's for indexes and constraints */
#if 1				/* TODO - */
  TP_DOMAIN *domain;		/* full domain of this attribute */
#endif
  int n_btids;			/* Number of ID's in the btids array */
  BTID index;			/* btree id if indexed */

  /* local array of btid's to use  if possible */
  int max_btids;		/* Size of the btids array */
  BTID btid_pack[OR_ATT_BTID_PREALLOC];

  unsigned is_fixed:1;		/* non-zero if this is a fixed width attribute */
  unsigned is_notnull:1;	/* non-zero if has not null constraint */
  unsigned is_shard_key:1;	/* non-zero if is the shard key attribute */
};

typedef enum
{
  BTREE_UNIQUE,
  BTREE_INDEX,
  BTREE_PRIMARY_KEY
} BTREE_TYPE;

typedef struct or_index OR_INDEX;
struct or_index
{
  OR_INDEX *next;
  OR_ATTRIBUTE **atts;		/* Array of associated attributes */
  int *asc_desc;		/* array of ascending / descending */
  char *btname;			/* index( or constraint) name */
  BTREE_TYPE type;		/* btree type */
  int n_atts;			/* Number of associated attributes */
  int index_status;
  BTID btid;			/* btree ID */
};

#define INDEX_IS_PRIMARY_KEY(idx)	\
  ((idx)->type == BTREE_PRIMARY_KEY)
#define INDEX_IS_UNIQUE(idx)		\
  (INDEX_IS_PRIMARY_KEY (idx) || (idx)->type == BTREE_UNIQUE)
#define INDEX_IS_IN_PROGRESS(idx)	\
 ((idx)->index_status == INDEX_STATUS_IN_PROGRESS)

typedef struct or_classrep OR_CLASSREP;
struct or_classrep
{
  OR_CLASSREP *next;

  OR_ATTRIBUTE *attributes;	/* list of attributes */
  OR_INDEX *indexes;		/* list of BTIDs for this class */

  REPR_ID id;			/* representation id */

  int fixed_length;		/* total size of the fixed width attributes */
  int n_attributes;		/* size of attribute array */
  int n_variable;		/* number of variable width attributes */
  int n_indexes;		/* number of indexes */
};

typedef struct or_class OR_CLASS;
struct or_class
{
  OR_CLASSREP *representations;
  OID statistics;		/* object containing statistics */
};

extern int or_class_repid (RECDES * record);
#if defined (ENABLE_UNUSED_FUNCTION)
extern void or_class_hfid (RECDES * record, HFID * hfid);
extern void or_class_statistics (RECDES * record, OID * oid);
#endif
extern OR_CLASSREP *or_get_classrep (RECDES * record, int repid);
extern void or_free_classrep (OR_CLASSREP * rep);
extern void or_free_constraints (OR_INDEX * indexes, int n_indexes);
extern void or_free_attributes (OR_ATTRIBUTE * attributes, int n_attributes);
extern const char *or_get_attrname (RECDES * record, int attrid);
extern OR_CLASS *or_get_class (RECDES * record);
extern void or_free_class (OR_CLASS * class_);

/* OLD STYLE INTERFACE */
extern int orc_class_repid (RECDES * record);
extern void orc_class_hfid_from_record (RECDES * record, HFID * hfid);
extern DISK_REPR *orc_diskrep_from_record (THREAD_ENTRY * thread_p,
					   RECDES * record);
extern void orc_free_diskrep (DISK_REPR * rep);
extern CLS_INFO *orc_class_info_from_record (RECDES * record);
extern void orc_free_class_info (CLS_INFO * info);
extern OR_CLASSREP **or_get_all_representation (RECDES * record, int *count);
#endif /* _OBJECT_REPRESENTATION_SR_H_ */
