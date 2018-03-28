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
 * system_catalog.h : Catalog manager interface header file
 */

#ifndef _SYSTEM_CATALOG_H_
#define _SYSTEM_CATALOG_H_

#ident "$Id$"

#include "config.h"

#include "error_manager.h"
#include "log_manager.h"
#include "memory_alloc.h"
#include "statistics.h"
#include "disk_manager.h"
#include "object_representation.h"
#include "storage_common.h"

#define NULL_REPRID       -1    /* Null Representation Identifier */
#define NULL_ATTRID       -1    /* Null Attribute Identifier */

typedef struct ctid CTID;
struct ctid
{
  VFID vfid;                    /* catalog volume identifier */
  EHID xhid;                    /* extendible hash index identifier */
  PAGEID hpgid;                 /* catalog header page identifier */
};                              /* catalog identifier */

typedef int REPR_ID;            /* representation identifier */
typedef int ATTR_ID;            /* attribute identifier */

/*
 * disk_representation
 *
 * This is the primary communication structure between the schema manager
 * and the catalog manager.   It contains information on the order and
 * location of attributes for a particular class of object.
 */

typedef struct disk_representation DISK_REPR;
struct disk_representation
{
  REPR_ID id;                   /* representation identifier  */
  int n_fixed;                  /* number of fixed attributes */
  struct disk_attribute *fixed; /* fixed attribute structures */
  int fixed_length;             /* total length of fixed attributes */
  int n_variable;               /* number of variable attributes */
  struct disk_attribute *variable;      /* variable attribute structures */
#if 1                           /* reserved for future use */
  int repr_reserved_1;
#endif
};                              /* object disk representation */




typedef struct disk_attribute DISK_ATTR;
struct disk_attribute
{
  ATTR_ID id;                   /* attribute identifier */
  int location;                 /* location in disk representation
                                 * exact offset if fixed attr.
                                 * index to offset table if var attr.*/
  DB_TYPE type;                 /* datatype */
  int val_length;               /* default value length >= 0 */
  void *value;                  /* default value */
  DB_DEFAULT_EXPR_TYPE default_expr;    /* default expression identifier */
  int position;                 /* storage position (fixed attributes only) */
  OID classoid;                 /* source class object id */
  DB_DATA unused_min_value;     /* unused - minimum existing value */
  DB_DATA unused_max_value;     /* unused - maximum existing value */
  int n_btstats;                /* number of B+tree statistics information */
  BTREE_STATS *bt_stats;        /* pointer to array of BTREE_STATS;
                                 * BTREE_STATS[n_btstats] */
};                              /* disk attribute structure */

typedef struct cls_info CLS_INFO;
struct cls_info
{
  HFID hfid;                    /* heap file identifier for the class */
  int tot_pages;                /* total number of pages in the heap file */
  INT64 tot_objects;            /* total number of objects for the class */
  unsigned int time_stamp;      /* timestamp of last update */
};                              /* class specific information */

extern CTID catalog_Id;         /* global catalog identifier */

extern void catalog_free_disk_attribute (DISK_ATTR * atr);
extern void catalog_free_representation (DISK_REPR * dr);
extern void catalog_free_class_info (CLS_INFO * Cls_Info);
extern void catalog_initialize (CTID * catid);
extern void catalog_finalize (void);

/* these two routines should be called only once and by the root */
extern CTID *catalog_create (THREAD_ENTRY * thread_p, CTID * catid, DKNPAGES exp_ncatpg, DKNPAGES exp_nindpg);
extern int catalog_destroy (void);

#if defined (ENABLE_UNUSED_FUNCTION)
extern int catalog_reclaim_space (THREAD_ENTRY * thread_p);
#endif
extern int catalog_add_representation (THREAD_ENTRY * thread_p, OID * class_id, REPR_ID repr_id, DISK_REPR * Disk_Repr);
extern int catalog_add_class_info (THREAD_ENTRY * thread_p, OID * class_id, CLS_INFO * Cls_Info);
extern CLS_INFO *catalog_update_class_info (THREAD_ENTRY * thread_p,
                                            OID * class_id, CLS_INFO * cls_info, bool skip_logging);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int catalog_drop_old_representations (THREAD_ENTRY * thread_p, OID * class_id);
#endif
extern DISK_REPR *catalog_get_representation (THREAD_ENTRY * thread_p, OID * class_id, REPR_ID repr_id);
extern CLS_INFO *catalog_get_class_info (THREAD_ENTRY * thread_p, OID * class_id);
extern int catalog_get_representation_directory (THREAD_ENTRY * thread_p,
                                                 OID * class_id, REPR_ID ** reprid_set, int *repr_cnt);
extern int catalog_get_last_representation_id (THREAD_ENTRY * thread_p, OID * cls_oid, REPR_ID * repr_id);
extern int catalog_insert (THREAD_ENTRY * thread_p, RECDES * record, OID * classoid);
extern int catalog_update (THREAD_ENTRY * thread_p, RECDES * record, OID * classoid);
extern int catalog_delete (THREAD_ENTRY * thread_p, OID * classoid);

#if defined (ENABLE_UNUSED_FUNCTION)
/* Checkdb consistency check routines */
extern DISK_ISVALID catalog_check_consistency (THREAD_ENTRY * thread_p);
#endif

/* Dump routines */
extern void catalog_dump (THREAD_ENTRY * thread_p, FILE * fp, int dump_flg);

/* Recovery routines */
extern int catalog_rv_new_page_redo (THREAD_ENTRY * thread_p, LOG_RCV * recv);
extern int catalog_rv_insert_redo (THREAD_ENTRY * thread_p, LOG_RCV * recv);
extern int catalog_rv_insert_undo (THREAD_ENTRY * thread_p, LOG_RCV * recv);
extern int catalog_rv_delete_redo (THREAD_ENTRY * thread_p, LOG_RCV * recv);
extern int catalog_rv_delete_undo (THREAD_ENTRY * thread_p, LOG_RCV * recv);
extern int catalog_rv_update (THREAD_ENTRY * thread_p, LOG_RCV * recv);
extern int catalog_rv_ovf_page_logical_insert_undo (THREAD_ENTRY * thread_p, LOG_RCV * recv);
extern int catalog_get_index_info_by_name (THREAD_ENTRY * thread_p,
                                           const char *class_name,
                                           const char *index_name, const int key_pos, int *cardinality);
#endif /* _SYSTEM_CATALOG_H_ */
