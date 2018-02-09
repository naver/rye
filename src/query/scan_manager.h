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


#ifndef _SCAN_MANAGER_H_
#define _SCAN_MANAGER_H_

#ident "$Id$"

/*
 * Scan (Server Side)
 */
#include <time.h>
#if defined(SERVER_MODE)
#include "jansson.h"
#endif

#include "btree.h"		/* TODO: for BTREE_SCAN */

#include "oid.h"		/* for OID */
#include "storage_common.h"	/* for PAGEID */
#include "heap_file.h"		/* for HEAP_SCANCACHE */
#include "db.h"
#include "dbtype.h"
#include "query_opfunc.h"

/*
 *       	TYPEDEFS RELATED TO THE SCAN DATA STRUCTURES
 */

#define IDX_COV_DEFAULT_TUPLES 200

typedef enum
{
  S_HEAP_SCAN = 1,
  S_INDX_SCAN,
  S_LIST_SCAN
} SCAN_TYPE;

typedef struct heap_scan_id HEAP_SCAN_ID;
struct heap_scan_id
{
  OID curr_oid;			/* current object identifier */
  OID cls_oid;			/* class object identifier */
  HFID hfid;			/* heap file identifier */
  HEAP_SCANCACHE scan_cache;	/* heap file scan_cache */
  SCAN_PRED scan_pred;		/* scan predicates(filters) */
  SCAN_ATTRS pred_attrs;	/* attr info from predicates */
  REGU_VARIABLE_LIST rest_regu_list;	/* regulator variable list */
  SCAN_ATTRS rest_attrs;	/* attr info from other than preds */
  bool caches_inited;		/* are the caches initialized?? */
  bool scancache_inited;
};				/* Regular Heap File Scan Identifier */

typedef struct indx_cov INDX_COV;
struct indx_cov
{
  QFILE_LIST_ID *list_id;	/* list file identifier */
  QFILE_TUPLE_VALUE_TYPE_LIST *type_list;	/* type list */
  QFILE_TUPLE_RECORD *tplrec;	/* tuple record */
  QFILE_LIST_SCAN_ID *lsid;	/* list file scan identifier */
  VAL_DESCR *val_descr;		/* val descriptor */
  OUTPTR_LIST *output_val_list;	/* output val list */
  REGU_VARIABLE_LIST regu_val_list;	/* regulator variable list */
  QUERY_ID query_id;		/* query id */
  int max_tuples;		/* maximum tuples stored in list_id */
};

/* multiple range optimization used on range search index scan:
 * - uses memory instead of lists to store range search results
 * - drops range search faster when key condition is not fullfilled */
typedef struct range_opt_item RANGE_OPT_ITEM;
struct range_opt_item
{
  DB_IDXKEY index_value;	/* key as it is read from B+tree */
  OID inst_oid;			/* instance OID corresponding to index key */
};

typedef struct multi_range_opt MULTI_RANGE_OPT;
struct multi_range_opt
{
  bool use;			/* true/false */
  int cnt;			/* current number of entries */
  int size;			/* expected number of entries */
  int no_attrs;			/* number of order by attributes */
  bool *is_desc_order;		/* sorting in descending order */
  int *sort_att_idx;		/* index of IDXKEY attribute on which the
				   sort is performed */
  QFILE_TUPLE_RECORD tplrec;	/* tuple record to dump MIDXKEYs into */
  TP_DOMAIN **sort_col_dom;	/* sorting column domain */
  RANGE_OPT_ITEM **top_n_items;	/* array with top n items */
  RANGE_OPT_ITEM **buffer;	/* temporary buffer used to copy elements from
				 * top_n_items
				 */
};

typedef struct indx_scan_id INDX_SCAN_ID;
struct indx_scan_id
{
  INDX_INFO *indx_info;		/* index information */
  BTREE_SCAN bt_scan;		/* index scan info. structure */
  int one_range;		/* a single range? */
  int curr_keyno;		/* current key number */
  int curr_oidno;		/* current oid number */
  OID *curr_oidp;		/* current oid pointer */
  OID_LIST oid_list;		/* list of object identifiers */
  HFID hfid;			/* heap file identifier */
  HEAP_SCANCACHE scan_cache;	/* heap file scan_cache */
  SCAN_PRED key_pred;		/* key predicates(filters) */
  SCAN_ATTRS key_attrs;		/* attr info from key filter */
  SCAN_PRED scan_pred;		/* scan predicates(filters) */
  SCAN_ATTRS pred_attrs;	/* attr info from predicates */
  REGU_VARIABLE_LIST rest_regu_list;	/* regulator variable list */
  REGU_VARIABLE_LIST pk_next_regu_list;	/* host_var regulator variable list */
  SCAN_ATTRS rest_attrs;	/* attr info from other than preds */
  KEY_VAL_RANGE *key_vals;	/* for eliminating duplicate ranges */
  int key_cnt;			/* number of valid ranges */
  bool need_count_only;		/* get count only, no OIDs are copied */
  bool caches_inited;		/* are the caches initialized?? */
  bool scancache_inited;
  DB_BIGINT key_limit_lower;	/* lower key limit */
  DB_BIGINT key_limit_upper;	/* upper key limit */
  INDX_COV indx_cov;		/* index covering information */
  MULTI_RANGE_OPT multi_range_opt;	/* optimization for multiple range
					 * search*/
};

typedef struct llist_scan_id LLIST_SCAN_ID;
struct llist_scan_id
{
  QFILE_LIST_ID *list_id;	/* Points to XASL tree */
  QFILE_LIST_SCAN_ID lsid;	/* List File Scan Identifier */
  SCAN_PRED scan_pred;		/* scan predicates(filters) */
  REGU_VARIABLE_LIST rest_regu_list;	/* regulator variable list */
  QFILE_TUPLE_RECORD *tplrecp;	/* tuple record pointer; output param */
};

typedef struct set_scan_id SET_SCAN_ID;
struct set_scan_id
{
  REGU_VARIABLE *set_ptr;	/* Points to XASL tree */
  REGU_VARIABLE_LIST operand;	/* operand points current element */
  DB_VALUE set;			/* set we will scan */
  int set_card;			/* cardinality of the set */
  int cur_index;		/* current element index */
  SCAN_PRED scan_pred;		/* scan predicates(filters) */
};

/* Note: Scan position is currently supported only for list file scans. */
typedef struct scan_pos SCAN_POS;
struct scan_pos
{
  SCAN_STATUS status;		/* Scan status                    */
  SCAN_POSITION position;	/* Scan position                  */
  QFILE_TUPLE_POSITION ls_tplpos;	/* List file index scan position  */
};				/* Scan position structure */

typedef struct scan_stats SCAN_STATS;
struct scan_stats
{
  struct timeval elapsed_scan;
  UINT64 num_fetches;
  UINT64 num_ioreads;

  /* for heap & list scan */
  int read_rows;		/* # of rows read */
  int qualified_rows;		/* # of rows qualified by data filter */

  /* for btree scan */
  int read_keys;		/* # of keys read */
  int qualified_keys;		/* # of keys qualified by key filter */
  int key_qualified_rows;	/* # of rows qualified by key filter */
  int data_qualified_rows;	/* # of rows qualified by data filter */
  struct timeval elapsed_lookup;
  bool covered_index;
  bool multi_range_opt;
};

typedef struct scan_id_struct SCAN_ID;
struct scan_id_struct
{
  SCAN_TYPE type;		/* Scan Type */
  SCAN_STATUS status;		/* Scan Status */
  SCAN_POSITION position;	/* Scan Position */
  SCAN_OPERATION_TYPE scan_op_type;	/* SELECT, DELETE, UPDATE */

  int fixed;			/* if true, pages containing scan
				   items in a group keep fixed */
  int qualified_block;		/* scan block has qualified items,
				   initially set to true */
  QPROC_FETCH_TYPE fetch_type;	/* scan fetch mode */
  int single_fetched;		/* if true, first qualified scan
				   item already fetched. */
  int null_fetched;		/* if true, null-padding scan item
				   already fetched. used in outer join */
  VAL_LIST *val_list;		/* value list */
  VAL_DESCR *vd;		/* value descriptor */
  union
  {
    LLIST_SCAN_ID llsid;	/* List File Scan Identifier */
    HEAP_SCAN_ID hsid;		/* Regular Heap File Scan Identifier */
    INDX_SCAN_ID isid;		/* Indexed Heap File Scan Identifier */
  } s;

  SCAN_STATS stats;
  bool scan_immediately_stop;
};				/* Scan Identifier */

#define SCAN_IS_INDEX_COVERED(iscan_id_p)   ((iscan_id_p)->indx_cov.list_id != NULL)

extern int scan_open_heap_scan (THREAD_ENTRY * thread_p, SCAN_ID * scan_id,
				/* fields of SCAN_ID */
				int readonly_scan,
				int fixed,
				QPROC_FETCH_TYPE fetch_type,
				VAL_LIST * val_list, VAL_DESCR * vd,
				/* fields of HEAP_SCAN_ID */
				OID * cls_oid,
				HFID * hfid,
				REGU_VARIABLE_LIST regu_list_pred,
				PRED_EXPR * pr,
				REGU_VARIABLE_LIST regu_list_rest,
				int num_attrs_pred,
				ATTR_ID * attrids_pred,
				HEAP_CACHE_ATTRINFO * cache_pred,
				int num_attrs_rest,
				ATTR_ID * attrids_rest,
				HEAP_CACHE_ATTRINFO * cache_rest);
extern int scan_open_index_scan (THREAD_ENTRY * thread_p, SCAN_ID * scan_id,
				 /* fields of SCAN_ID */
				 int readonly_scan,
				 int fixed,
				 QPROC_FETCH_TYPE fetch_type,
				 VAL_LIST * val_list, VAL_DESCR * vd,
				 /* fields of INDX_SCAN_ID */
				 INDX_INFO * indx_info,
				 OID * cls_oid,
				 HFID * hfid,
				 REGU_VARIABLE_LIST regu_list_key,
				 PRED_EXPR * pr_key,
				 REGU_VARIABLE_LIST regu_list_pred,
				 PRED_EXPR * pr,
				 REGU_VARIABLE_LIST regu_list_rest,
				 REGU_VARIABLE_LIST regu_list_pk_next,
				 OUTPTR_LIST * output_val_list,
				 REGU_VARIABLE_LIST regu_val_list,
				 int num_attrs_key,
				 ATTR_ID * attrids_key,
				 HEAP_CACHE_ATTRINFO * cache_key,
				 int num_attrs_pred,
				 ATTR_ID * attrids_pred,
				 HEAP_CACHE_ATTRINFO * cache_pred,
				 int num_attrs_rest,
				 ATTR_ID * attrids_rest,
				 HEAP_CACHE_ATTRINFO * cache_rest,
				 QUERY_ID query_id);
extern int scan_open_list_scan (THREAD_ENTRY * thread_p, SCAN_ID * scan_id,
				/* fields of SCAN_ID */
				QPROC_FETCH_TYPE fetch_type,
				VAL_LIST * val_list, VAL_DESCR * vd,
				/* fields of LLIST_SCAN_ID */
				QFILE_LIST_ID * list_id,
				REGU_VARIABLE_LIST regu_list_pred,
				PRED_EXPR * pr,
				REGU_VARIABLE_LIST regu_list_rest);
extern int scan_start_scan (THREAD_ENTRY * thread_p, SCAN_ID * s_id);
extern SCAN_CODE scan_reset_scan_block (THREAD_ENTRY * thread_p,
					SCAN_ID * s_id);
extern SCAN_CODE scan_next_scan_block (THREAD_ENTRY * thread_p,
				       SCAN_ID * s_id);
extern void scan_end_scan (THREAD_ENTRY * thread_p, SCAN_ID * s_id);
extern void scan_close_scan (THREAD_ENTRY * thread_p, SCAN_ID * s_id);
extern SCAN_CODE scan_next_scan (THREAD_ENTRY * thread_p, SCAN_ID * s_id);
extern void scan_init_index_scan (INDX_SCAN_ID * isidp, OID * oid_buf,
				  int oid_buf_size);
extern void scan_initialize (void);
extern void scan_finalize (void);

#if defined(SERVER_MODE)
extern void scan_print_stats_json (SCAN_ID * scan_id, json_t * stats);
extern void scan_print_stats_text (FILE * fp, SCAN_ID * scan_id);
#endif /* SERVER_MODE */

#if 1				/* TODO - */
extern int btree_keyval_search (THREAD_ENTRY * thread_p, BTID * btid,
				KEY_VAL_RANGE * key_val_range,
				INDX_SCAN_ID * isidp);
extern int btree_range_search (THREAD_ENTRY * thread_p, BTID * btid,
			       KEY_VAL_RANGE * key_val_range,
			       FILTER_INFO * filter, INDX_SCAN_ID * isidp);
#endif

#endif /* _SCAN_MANAGER_H_ */
