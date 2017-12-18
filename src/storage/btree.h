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
 * btree.h: B+tree index manager module(interface)
 */

#ifndef _BTREE_H_
#define _BTREE_H_

#ident "$Id$"

#include "config.h"

#include "storage_common.h"
#include "error_manager.h"
#include "oid.h"
#include "statistics.h"
#include "disk_manager.h"
#include "object_domain.h"
#include "query_evaluator.h"
#include "lock_manager.h"
#include "recovery.h"
#include "page_buffer.h"

/* We never want to store keys larger than 900 bytes.
 * key size should be smaller than (DB_PAGESIZE / 4)
 */
#define BTREE_MAX_KEYLEN (900)

#define BTREE_SPLIT_DEFAULT_PIVOT 0.5f

#define BTREE_SPLIT_LOWER_BOUND 0.20f
#define BTREE_SPLIT_UPPER_BOUND (1.0f - BTREE_SPLIT_LOWER_BOUND)

#define BTREE_SPLIT_MIN_PIVOT 0.05f
#define BTREE_SPLIT_MAX_PIVOT (1.0f - BTREE_SPLIT_MIN_PIVOT)

#define BTREE_VALID_RANGE(range)                                                       \
  ((range) == EQ_NA || (range) == GT_LT || (range) == GT_LE || (range) == GE_LT          \
                   || (range) == GE_LE || (range) == GE_INF || (range) == GT_INF        \
                   || (range) == INF_LE || (range) == INF_LT || (range) == INF_INF)

/*
 * Type definitions related to b+tree structure and operations
 */

typedef struct btree_node_header BTREE_NODE_HEADER;
struct btree_node_header
{				/*  Node header information  */
  BTREE_NODE_SPLIT_INFO split_info;	/* split point info. of the node */
  VPID next_vpid;		/* Leaf Page Next Node Pointer         */
  VPID prev_vpid;		/* Leaf Page Previous Node Pointer     */

#if 1				/* TODO - delete me someday */
  short key_cnt;		/* Key count for the node              */
#endif
  short node_level;		/* node_level > 1 ? BTREE_NON_LEAF_NODE : BTREE_LEAF_NODE */
};

typedef struct non_leaf_rec NON_LEAF_REC;
struct non_leaf_rec
{				/*  Fixed part of a non_leaf record  */
  VPID pnt;			/* The Child Page Pointer  */
};

typedef enum
{
  BTREE_KEY_ERROR = 0,
  BTREE_KEY_OUT_OF_RANGE = 1,
  BTREE_KEY_NOT_SATISFIED = 2,
  BTREE_KEY_SATISFIED = 3
} BTREE_CHECK_KEY;

enum
{ BTREE_COERCE_KEY_WITH_MIN_VALUE = 1, BTREE_COERCE_KEY_WITH_MAX_VALUE = 2 };

/* BTID_INT structure from btree_load.h */
typedef struct btid_int BTID_INT;
struct btid_int
{				/* Internal btree block */
  BTID *sys_btid;

  OID cls_oid;			/* class object identifier */
  OR_CLASSREP *classrepr;	/* desired class last representation */
  int classrepr_cache_idx;	/* index of representation cache */
  int indx_id;			/* index ID */

#if !defined(NDEBUG)
  /* check page range (left_fence, right_fence] */
  DB_IDXKEY left_fence;
  DB_IDXKEY right_fence;
#endif
};

#if !defined(NDEBUG)
#define BTREE_INIT_BTID_INT(bi)                \
  do {                                          \
    (bi)->sys_btid = NULL;                      \
    OID_SET_NULL (&((bi)->cls_oid));            \
    (bi)->classrepr = NULL;                     \
    (bi)->classrepr_cache_idx = -1;             \
    (bi)->indx_id = -1;                         \
    DB_IDXKEY_MAKE_NULL (&((bi)->left_fence));	\
    DB_IDXKEY_MAKE_NULL (&((bi)->right_fence));	\
  } while (0)
#else
#define BTREE_INIT_BTID_INT(bi)                \
  do {                                          \
    (bi)->sys_btid = NULL;			\
    OID_SET_NULL (&((bi)->cls_oid));		\
    (bi)->classrepr = NULL;			\
    (bi)->classrepr_cache_idx = -1;		\
    (bi)->indx_id = -1;				\
  } while (0)
#endif

typedef struct key_val_range KEY_VAL_RANGE;
struct key_val_range
{
  RANGE range;
  DB_IDXKEY lower_key;
  DB_IDXKEY upper_key;
  int num_index_term;		/* #terms associated with index key range */
};

/* Btree range search scan structure */
typedef struct btree_scan BTREE_SCAN;	/* BTS */
struct btree_scan
{
  BTID_INT btid_int;

  VPID P_vpid;			/* vpid of previous leaf page */
  VPID C_vpid;			/* vpid of current leaf page */

  PAGE_PTR P_page;		/* page ptr to previous leaf page */
  PAGE_PTR C_page;		/* page ptr to current leaf page */

  INT16 slot_id;		/* current slot identifier */

  DB_IDXKEY cur_key;		/* current key value */
  bool clear_cur_key;		/* clear flag for current key value */

  KEY_VAL_RANGE *key_val;	/* key range information */
  FILTER_INFO *key_filter;	/* key filter information */

  int use_desc_index;		/* use descending index */

  /* for query trace */
  int read_keys;
  int qualified_keys;

  bool is_first_search;

  /* for resume next search */
  LOG_LSA cur_leaf_lsa;		/* page LSA of current leaf page */
};

#define BTREE_INIT_SCAN(bts)			\
  do {						\
    (bts)->P_vpid.pageid = NULL_PAGEID;		\
    (bts)->C_vpid.pageid = NULL_PAGEID;		\
    (bts)->P_page = NULL;			\
    (bts)->C_page = NULL;			\
    (bts)->slot_id = -1;			\
    (bts)->is_first_search = true;              \
  } while (0)

#define BTREE_END_OF_SCAN(bts) \
   ((bts)->C_vpid.pageid == NULL_PAGEID)

#define BTREE_START_OF_SCAN(bts) BTREE_END_OF_SCAN(bts)

typedef struct btree_capacity BTREE_CAPACITY;
struct btree_capacity
{
  int dis_key_cnt;		/* Distinct key count (in leaf pages) */
  int tot_val_cnt;		/* Total number of values stored in tree */
  int avg_val_per_key;		/* Average number of values (OIDs) per key */
  int leaf_pg_cnt;		/* Leaf page count */
  int nleaf_pg_cnt;		/* NonLeaf page count */
  int tot_pg_cnt;		/* Total page count */
  int height;			/* Height of the tree */
  float sum_rec_len;		/* Sum of all record lengths */
  float sum_key_len;		/* Sum of all distinct key lengths */
  int avg_key_len;		/* Average key length */
  int avg_rec_len;		/* Average page record length */
  float tot_free_space;		/* Total free space in index */
  float tot_space;		/* Total space occupied by index */
  float tot_used_space;		/* Total used space in index */
  int avg_pg_key_cnt;		/* Average page key count (in leaf pages) */
  float avg_pg_free_sp;		/* Average page free space */
};

/*
 * Recovery structures
 */
typedef struct pageid_struct PAGEID_STRUCT;
struct pageid_struct
{				/* Recovery pageid structure */
  VFID vfid;			/* Volume id in which page resides */
  VPID vpid;			/* Virtual page identifier */
};

typedef struct recset_header RECSET_HEADER;
struct recset_header
{				/* Recovery set of recdes structure */
  INT16 rec_cnt;		/* number of RECDESs stored */
  INT16 first_slotid;		/* first slot id */
};

extern void btree_scan_clear_key (BTREE_SCAN * btree_scan);

extern int btree_get_stats (THREAD_ENTRY * thread_p,
			    OID * class_oid, BTREE_STATS * stat_info,
			    bool with_fullscan);

extern int btree_get_pkey_btid (THREAD_ENTRY * thread_p, OID * cls_oid,
				BTID * pkey_btid);
#if defined (ENABLE_UNUSED_FUNCTION)
extern DISK_ISVALID btree_check_by_class_oid (THREAD_ENTRY * thread_p,
					      OID * cls_oid);
extern DISK_ISVALID btree_check_all (THREAD_ENTRY * thread_p);
#endif

extern PAGE_PTR btree_get_next_page (THREAD_ENTRY * thread_p,
				     BTID_INT * btid, PAGE_PTR page_p);

extern DB_IDXKEY *btree_delete (THREAD_ENTRY * thread_p,
				BTID_INT * btid, DB_IDXKEY * key);
extern DB_IDXKEY *btree_insert (THREAD_ENTRY * thread_p,
				BTID_INT * btid, DB_IDXKEY * key);
extern int btree_update (THREAD_ENTRY * thread_p,
			 BTID_INT * btid,
			 DB_IDXKEY * old_key, DB_IDXKEY * new_key);
extern int btree_find_min_or_max_key (THREAD_ENTRY * thread_p,
				      OID * class_oid, BTID * btid,
				      const VPID * top_vpid,
				      DB_IDXKEY * idxkey, int flag_minkey);

extern bool btree_key_is_null (const DB_IDXKEY * key);
#if defined (ENABLE_UNUSED_FUNCTION)
extern bool btree_key_has_null (const DB_IDXKEY * key);
#endif

/* Dump routines */
extern int btree_dump_capacity_all (THREAD_ENTRY * thread_p, FILE * fp);
extern int btree_dump_tree (THREAD_ENTRY * thread_p, FILE * fp,
			    BTID_INT * btid, int dump_level, bool dump_key);
extern void btree_dump_page (THREAD_ENTRY * thread_p, FILE * fp,
			     BTID_INT * btid, PAGE_PTR page_ptr,
			     VPID * pg_vpid, int n, bool dump_key);

/* Recovery routines */
extern int btree_rv_nodehdr_redo_insert (THREAD_ENTRY * thread_p,
					 LOG_RCV * recv);
extern int btree_rv_nodehdr_undo_insert (THREAD_ENTRY * thread_p,
					 LOG_RCV * recv);
extern void btree_rv_nodehdr_dump (FILE * fp, int length, void *data);
extern int btree_rv_noderec_undoredo_update (THREAD_ENTRY * thread_p,
					     LOG_RCV * recv);
extern int btree_rv_noderec_redo_insert (THREAD_ENTRY * thread_p,
					 LOG_RCV * recv);
extern int btree_rv_noderec_undo_insert (THREAD_ENTRY * thread_p,
					 LOG_RCV * recv);
extern void btree_rv_noderec_dump_slot_id (FILE * fp, int length, void *data);
extern int btree_rv_pagerec_insert (THREAD_ENTRY * thread_p, LOG_RCV * recv);
extern int btree_rv_pagerec_delete (THREAD_ENTRY * thread_p, LOG_RCV * recv);
extern int btree_rv_newroot_redo_init (THREAD_ENTRY * thread_p,
				       LOG_RCV * recv);
extern int btree_rv_newpage_redo_init (THREAD_ENTRY * thread_p,
				       LOG_RCV * recv);
extern int btree_rv_newpage_undo_alloc (THREAD_ENTRY * thread_p,
					LOG_RCV * recv);
extern void btree_rv_newpage_dump_undo_alloc (FILE * fp, int length,
					      void *data);
extern int btree_rv_keyval_undo_insert (THREAD_ENTRY * thread_p,
					LOG_RCV * recv);
extern int btree_rv_keyval_undo_delete (THREAD_ENTRY * thread_p,
					LOG_RCV * recv);
extern void btree_rv_keyval_dump (FILE * fp, int length, void *data);
extern int btree_rv_undoredo_copy_page (THREAD_ENTRY * thread_p,
					LOG_RCV * recv);
extern int btree_rv_leafrec_redo_delete (THREAD_ENTRY * thread_p,
					 LOG_RCV * recv);
extern int btree_rv_leafrec_redo_insert_key (THREAD_ENTRY * thread_p,
					     LOG_RCV * recv);
extern int btree_rv_nop (THREAD_ENTRY * thread_p, LOG_RCV * recv);

extern int btree_attrinfo_read_dbvalues (THREAD_ENTRY * thread_p,
					 const DB_IDXKEY * curr_key,
					 OR_CLASSREP * classrepr, int indx_id,
					 HEAP_CACHE_ATTRINFO * attr_info);
extern int btree_coerce_idxkey (DB_IDXKEY * key,
				OR_INDEX * indexp, int num_term,
				int key_minmax);
extern void btree_get_indexname_on_table (THREAD_ENTRY * thread_p,
					  const BTID_INT * btid, char *buffer,
					  const int buffer_len);

extern bool btree_clear_key_value (bool * clear_flag, DB_IDXKEY * key);

extern int btree_get_key_length (const DB_IDXKEY * key);
extern int btree_get_oid_from_key (THREAD_ENTRY * thread_p, BTID_INT * btid,
				   const DB_IDXKEY * key, OID * oid);
extern int btree_read_node_header (BTID_INT * btid, PAGE_PTR pg_ptr,
				   BTREE_NODE_HEADER * header);
extern void btree_read_fixed_portion_of_non_leaf_record (RECDES * rec,
							 NON_LEAF_REC *
							 nlf_rec);
extern int btree_read_record (THREAD_ENTRY * thread_p, BTID_INT * btid,
			      RECDES * Rec, DB_IDXKEY * key,
			      void *rec_header,
			      int node_type, bool * clear_key, int copy);
extern int btree_write_record (THREAD_ENTRY * thread_p, BTID_INT * btid,
			       void *node_rec, const DB_IDXKEY * key,
			       int node_type, RECDES * rec);

extern int btree_find_lower_bound_leaf (THREAD_ENTRY * thread_p,
					const VPID * top_vpid,
					BTREE_SCAN * BTS,
					BTREE_STATS * stat_info);
extern int btree_find_next_record (THREAD_ENTRY * thread_p, BTREE_SCAN * bts,
				   int direction, BTREE_STATS * stat_info);

extern int btree_init_node_header (THREAD_ENTRY * thread_p,
				   BTREE_NODE_HEADER * node_header);
extern int btree_insert_node_header (THREAD_ENTRY * thread_p, PAGE_PTR pg_ptr,
				     BTREE_NODE_HEADER * header);
extern int btree_write_node_header (BTID_INT * btid, PAGE_PTR pg_ptr,
				    BTREE_NODE_HEADER * header);

extern PAGE_PTR btree_get_new_page (THREAD_ENTRY * thread_p, BTID_INT * btid,
				    VPID * vpid, VPID * near_vpid);

extern PAGE_PTR btree_pgbuf_fix (THREAD_ENTRY * thread_p, const VFID * vfid,
				 const VPID * vpid,
				 int requestmode,
				 PGBUF_LATCH_CONDITION condition,
				 const PAGE_TYPE ptype);

extern int btree_search_nonleaf_page (THREAD_ENTRY * thread_p,
				      BTID_INT * btid, PAGE_PTR page_ptr,
				      const DB_IDXKEY * key,
				      INT16 * slot_id, VPID * child_vpid);
extern bool btree_search_leaf_page (THREAD_ENTRY * thread_p, BTID_INT * btid,
				    PAGE_PTR page_ptr,
				    const DB_IDXKEY * key,
				    INT16 * slot_id,
				    int *max_diff_column_index);

extern int btree_set_vpid_previous_vpid (THREAD_ENTRY * thread_p,
					 BTID_INT * btid, PAGE_PTR page_p,
					 VPID * prev);
extern int btree_split_next_pivot (BTREE_NODE_SPLIT_INFO * split_info,
				   float new_value, int max_index);

extern void btree_write_fixed_portion_of_non_leaf_record (RECDES * rec,
							  NON_LEAF_REC *
							  nlf_rec);

extern int btree_rv_save_keyval (BTID_INT * btid, const DB_IDXKEY * key,
				 char *data, int *length);
extern int btree_rv_util_save_page_records (THREAD_ENTRY * thread_p,
					    BTID_INT * btid,
					    PAGE_PTR page_ptr,
					    INT16 first_slotid, int rec_cnt,
					    INT16 ins_slotid, char *data,
					    const int data_len, int *length);

#if !defined(NDEBUG)
extern int btree_fence_check_key (THREAD_ENTRY * thread_p,
				  BTID_INT * btid,
				  const DB_IDXKEY * left_key,
				  const DB_IDXKEY * right_key,
				  const bool with_eq);
#endif

#endif /* _BTREE_H_ */
