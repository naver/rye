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
 * XASL (eXtented Access Specification Language) interpreter internal
 * definitions.
 * For a brief description of ASL principles see "Access Path Selection in a
 * Relational Database Management System" by P. Griffiths Selinger et al
 */

#ifndef _QUERY_EXECUTOR_H_
#define _QUERY_EXECUTOR_H_

#ident "$Id$"

#include <time.h>
#if defined(SERVER_MODE)
#include "jansson.h"
#include "memory_hash.h"
#endif

#include "storage_common.h"
#include "oid.h"
#include "lock_manager.h"
#include "scan_manager.h"
#include "thread.h"
#include "external_sort.h"

#include "repl.h"

/* this must be non-0 and probably should be word aligned */
#define XASL_STREAM_HEADER 8

/*
 * Macros for easier handling of the ACCESS_SPEC_TYPE members.
 */

#define ACCESS_SPEC_CLS_SPEC(ptr) \
        ((ptr)->s.cls_node)

#define ACCESS_SPEC_CLS_REGU_LIST(ptr) \
        ((ptr)->s.cls_node.cls_regu_list)

#define ACCESS_SPEC_HFID(ptr) \
        ((ptr)->s.cls_node.hfid)

#define ACCESS_SPEC_CLS_OID(ptr) \
        ((ptr)->s.cls_node.cls_oid)

#define ACCESS_SPEC_LIST_SPEC(ptr) \
        ((ptr)->s.list_node)

#define ACCESS_SPEC_RLIST_SPEC(ptr) \
        ((ptr)->s.reguval_list_node)

#define ACCESS_SPEC_LIST_REGU_LIST(ptr) \
        ((ptr)->s.list_node.list_regu_list)

#define ACCESS_SPEC_XASL_NODE(ptr) \
        ((ptr)->s.list_node.xasl_node)

#define ACCESS_SPEC_LIST_ID(ptr) \
        (ACCESS_SPEC_XASL_NODE(ptr)->list_id)

#define ACCESS_SPEC_RLIST_VALPTR_LIST(ptr) \
        ((ptr)->s.reguval_list_node.valptr_list)

/*
 * Macros for xasl structure
 */

#define XASL_ORDBYNUM_FLAG_SCAN_CONTINUE    0x01
#define XASL_ORDBYNUM_FLAG_SCAN_CHECK       0x02
#define XASL_ORDBYNUM_FLAG_SCAN_STOP        0x04

#define XASL_INSTNUM_FLAG_SCAN_CONTINUE     0x01
#define XASL_INSTNUM_FLAG_SCAN_CHECK        0x02
#define XASL_INSTNUM_FLAG_SCAN_STOP	    0x04
#define XASL_INSTNUM_FLAG_SCAN_LAST_STOP    0x08
#define XASL_INSTNUM_FLAG_EVAL_DEFER	    0x10

/*
 * Macros for buildlist block
 */

#define XASL_G_GRBYNUM_FLAG_SCAN_CONTINUE   0x01
#define XASL_G_GRBYNUM_FLAG_SCAN_CHECK      0x02
#define XASL_G_GRBYNUM_FLAG_SCAN_STOP       0x04
#define XASL_G_GRBYNUM_FLAG_LIMIT_LT	    0x08
#define XASL_G_GRBYNUM_FLAG_LIMIT_GT_LT	    0x10

/* XASL cache related macros */

#define XASL_STREAM_HEADER_PTR(stream) \
    ((char *) (stream))
#define GET_XASL_STREAM_HEADER_SIZE(stream) \
    (*((int *) XASL_STREAM_HEADER_PTR(stream)))
#define SET_XASL_STREAM_HEADER_SIZE(stream, size) \
    (*((int *) XASL_STREAM_HEADER_PTR(stream)) = (size))
#define GET_XASL_STREAM_HEADER_DATA(stream) \
    ((char *) XASL_STREAM_HEADER_PTR(stream) + sizeof(int))
#define SET_XASL_STREAM_HEADER_DATA(stream, data, size) \
    SET_XASL_STREAM_HEADER_SIZE(stream, size); \
    (void) memcpy((void *) GET_XASL_STREAM_HEADER_DATA(stream), \
                  (void *) (data), (size_t) (size))

#define XASL_STREAM_BODY_PTR(stream) \
    (GET_XASL_STREAM_HEADER_DATA(stream) + GET_XASL_STREAM_HEADER_SIZE(stream))
#define GET_XASL_STREAM_BODY_SIZE(stream) \
    (*((int *) XASL_STREAM_BODY_PTR(stream)))
#define SET_XASL_STREAM_BODY_SIZE(stream, size) \
    (*((int *) XASL_STREAM_BODY_PTR(stream)) = (size))
#define GET_XASL_STREAM_BODY_DATA(stream) \
    ((char *) XASL_STREAM_BODY_PTR(stream) + sizeof(int))
#define SET_XASL_STREAM_BODY_DATA(stream, data, size) \
    (void) memcpy((void *) GET_XASL_STREAM_BODY_DATA(stream), \
                  (void *) (data), (size_t) (size))

#define GET_XASL_HEADER_CREATOR_OID(header) \
    ((OID *) header)
#define SET_XASL_HEADER_CREATOR_OID(header, oid) \
    (*((OID *) header) = *(oid))
#define GET_XASL_HEADER_N_OID_LIST(header) \
    (*((int *) ((char *) (header) + sizeof(OID))))
#define SET_XASL_HEADER_N_OID_LIST(header, n) \
    (*((int *) ((char *) (header) + sizeof(OID))) = (n))
#define GET_XASL_HEADER_CLASS_OID_LIST(header) \
    ((OID *) ((char *) (header) + sizeof(OID) + sizeof(int)))
#define SET_XASL_HEADER_CLASS_OID_LIST(header, list, n) \
    (void) memcpy((void *) GET_XASL_HEADER_CLASS_OID_LIST(header), \
                  (void *) (list), (size_t) sizeof(OID) * (n))
#define GET_XASL_HEADER_REPR_ID_LIST(header) \
    ((int *) ((char *) (header) + sizeof(OID) + sizeof(int) + \
                            GET_XASL_HEADER_N_OID_LIST(header) * sizeof(OID)))
#define SET_XASL_HEADER_REPR_ID_LIST(header, list, n) \
    (void) memcpy((void *) GET_XASL_HEADER_REPR_ID_LIST(header), \
                  (void *) (list), (size_t) sizeof(int) * (n))
#define GET_XASL_HEADER_DBVAL_CNT(header) \
    (*((int *) ((char *) (header) + sizeof(OID) + sizeof(int) + \
                            GET_XASL_HEADER_N_OID_LIST(header) * sizeof(OID) + \
                            GET_XASL_HEADER_N_OID_LIST(header) * sizeof(int))))
#define SET_XASL_HEADER_DBVAL_CNT(header, cnt) \
    (*((int *) ((char *) (header) + sizeof(OID) + sizeof(int) + \
                            GET_XASL_HEADER_N_OID_LIST(header) * sizeof(OID) + \
                            GET_XASL_HEADER_N_OID_LIST(header) * sizeof(int))) = (cnt))

/*
 * Start procedure information
 */

#define QEXEC_NULL_COMMAND_ID   -1	/* Invalid command identifier */

/*
 * Access specification information
 */

typedef enum
{
  TARGET_CLASS = 1,
  TARGET_LIST
} TARGET_TYPE;

typedef enum
{
  ACCESS_SPEC_FLAG_NONE = 0,
  ACCESS_SPEC_FLAG_FOR_UPDATE = 0x01	/* used with FOR UPDATE clause.
					 * The spec that will be locked. */
} ACCESS_SPEC_FLAG;

typedef struct cls_spec_node CLS_SPEC_TYPE;
struct cls_spec_node
{
  REGU_VARIABLE_LIST cls_regu_list_key;	/* regu list for the key filter */
  REGU_VARIABLE_LIST cls_regu_list_pred;	/* regu list for the predicate */
  REGU_VARIABLE_LIST cls_regu_list_rest;	/* regu list for rest of attrs */
  REGU_VARIABLE_LIST cls_regu_list_pk_next;	/* regu list for primary key next */
  OUTPTR_LIST *cls_output_val_list;	/*regu list writer for val list */
  REGU_VARIABLE_LIST cls_regu_val_list;	/*regu list reader for val list */
  HFID hfid;			/* heap file identifier */
  OID cls_oid;			/* class object identifier */
  ATTR_ID *attrids_key;		/* array of attr ids from the key filter */
  HEAP_CACHE_ATTRINFO *cache_key;	/* cache for the key filter attrs */
  int num_attrs_key;		/* number of atts from the key filter */
  int num_attrs_pred;		/* number of atts from the predicate */
  ATTR_ID *attrids_pred;	/* array of attr ids from the pred */
  HEAP_CACHE_ATTRINFO *cache_pred;	/* cache for the pred attrs */
  ATTR_ID *attrids_rest;	/* array of attr ids other than pred */
  HEAP_CACHE_ATTRINFO *cache_rest;	/* cache for the non-pred attrs */
  int num_attrs_rest;		/* number of atts other than pred */
};

typedef struct list_spec_node LIST_SPEC_TYPE;
struct list_spec_node
{
  REGU_VARIABLE_LIST list_regu_list_pred;	/* regu list for the predicate */
  REGU_VARIABLE_LIST list_regu_list_rest;	/* regu list for rest of attrs */
  XASL_NODE *xasl_node;		/* the XASL node that contains the
				 * list file identifier
				 */
};

typedef struct reguval_list_spec_node REGUVAL_LIST_SPEC_TYPE;
struct reguval_list_spec_node
{
  VALPTR_LIST *valptr_list;	/* point to xasl.outptr_list */
};

typedef union hybrid_node HYBRID_NODE;
union hybrid_node
{
  CLS_SPEC_TYPE cls_node;	/* class specification */
  LIST_SPEC_TYPE list_node;	/* list specification */
  REGUVAL_LIST_SPEC_TYPE reguval_list_node;	/* reguval_list specification */
};				/* class/list access specification */

typedef struct access_spec_node ACCESS_SPEC_TYPE;
struct access_spec_node
{
  TARGET_TYPE type;		/* target class or list */
  ACCESS_METHOD access;		/* access method */
  INDX_INFO *indexptr;		/* index info if index accessing */
  INDX_ID indx_id;
  PRED_EXPR *where_key;		/* key filter expression */
  PRED_EXPR *where_pred;	/* predicate expression */
  HYBRID_NODE s;		/* class/list access specification */
  SCAN_ID s_id;			/* scan identifier */
  int fixed_scan;		/* scan pages are kept fixed? */
  int qualified_block;		/* qualified scan block */
  QPROC_FETCH_TYPE fetch_type;	/* open scan in inner or outer fetch mode */
  ACCESS_SPEC_TYPE *next;	/* next access specification */
  ACCESS_SPEC_FLAG flags;	/* flags from ACCESS_SPEC_FLAG enum */
};


/*
 * Xasl body node information
 */

/* UNION_PROC, DIFFERENCE_PROC, INTERSECTION_PROC */
typedef struct union_proc_node UNION_PROC_NODE;
struct union_proc_node
{
  XASL_NODE *left;		/* first subquery */
  XASL_NODE *right;		/* second subquery */
};

typedef struct buildlist_proc_node BUILDLIST_PROC_NODE;
struct buildlist_proc_node
{
  DB_VALUE **output_columns;	/* array of pointers to the
				 * value list that hold the
				 * values of temporary list
				 * file columns --
				 * used only in XASL generator
				 */
  XASL_NODE *eptr_list;		/* having subquery list */
  SORT_LIST *groupby_list;	/* sorting fields */
  SORT_LIST *after_groupby_list;	/* sorting fields */
  OUTPTR_LIST *g_outptr_list;	/* group_by output ptr list */
  REGU_VARIABLE_LIST g_regu_list;	/* group_by regu. list */
  VAL_LIST *g_val_list;		/* group_by value list */
  PRED_EXPR *g_having_pred;	/* having  predicate */
  PRED_EXPR *g_grbynum_pred;	/* groupby_num() predicate */
  DB_VALUE *g_grbynum_val;	/* groupby_num() value result */
  AGGREGATE_TYPE *g_agg_list;	/* aggregate function list */
  int g_grbynum_flag;		/* stop or continue grouping? */
  int g_with_rollup;		/* WITH ROLLUP clause for GROUP BY */
  EHID upd_del_ehid;		/* temporary extendible hash
				   for UPDATE/DELETE generated SELECT statement */
};


typedef struct buildvalue_proc_node BUILDVALUE_PROC_NODE;
struct buildvalue_proc_node
{
  PRED_EXPR *having_pred;	/* having  predicate */
  DB_VALUE *grbynum_val;	/* groupby_num() value result */
  AGGREGATE_TYPE *agg_list;	/* aggregate function list */
  ARITH_TYPE *outarith_list;	/* outside arithmetic list */
};

/* assignment details structure for server update execution */
typedef struct update_assignment UPDATE_ASSIGNMENT;
struct update_assignment
{
  int att_idx;			/* index in the class attributes array */
  REGU_VARIABLE *regu_var;	/* regu variable for rhs in assignment */
};

/*update/delete class info structure */
typedef struct upddel_class_info UPDDEL_CLASS_INFO;
struct upddel_class_info
{
  OID class_oid;		/* OID of the class                 */
  HFID class_hfid;		/* Heap file ID of the class        */
  int no_attrs;			/* total number of attrs involved       */
  int *att_id;			/* ID's of attributes (array)           */
  int has_uniques;		/* whether there are unique constraints */
};

typedef struct update_proc_node UPDATE_PROC_NODE;
struct update_proc_node
{
  UPDDEL_CLASS_INFO *class_info;	/* target class info */
  PRED_EXPR *cons_pred;		/* constraint predicate                 */
  int no_assigns;		/* total no. of assignments */
  UPDATE_ASSIGNMENT *assigns;	/* assignments array */
  int no_orderby_keys;		/* no of keys for ORDER_BY */
};

/*on duplicate key update info structure */
typedef struct odku_info ODKU_INFO;
struct odku_info
{
  PRED_EXPR *cons_pred;		/* constraint predicate */
  int no_assigns;		/* number of assignments */
  UPDATE_ASSIGNMENT *assignments;	/* assignments */
  HEAP_CACHE_ATTRINFO *attr_info;	/* attr info */
  int *attr_ids;		/* ID's of attributes (array) */
};

typedef struct insert_proc_node INSERT_PROC_NODE;
struct insert_proc_node
{
  OID class_oid;		/* OID of the class involved            */
  HFID class_hfid;		/* Heap file ID of the class            */
  int no_vals;			/* total number of attrs involved       */
  int no_default_expr;		/* total number of attrs which require
				 * a default value to be inserted       */
  int *att_id;			/* ID's of attributes (array)           */
  DB_VALUE **vals;		/* values (array)                       */
  PRED_EXPR *cons_pred;		/* constraint predicate                 */
  ODKU_INFO *odku;		/* ON DUPLICATE KEY UPDATE assignments  */
  int has_uniques;		/* whether there are unique constraints */
  int do_replace;		/* duplicate tuples should be replaced */
  int force_page_allocation;	/* force new page allocation */
  int no_val_lists;		/* number of value lists in values clause */
  VALPTR_LIST **valptr_lists;	/* OUTPTR lists for each list of values */
};

typedef struct delete_proc_node DELETE_PROC_NODE;
struct delete_proc_node
{
  UPDDEL_CLASS_INFO *class_info;	/* target class info */
};

typedef enum
{
  UNION_PROC,
  DIFFERENCE_PROC,
  INTERSECTION_PROC,
  BUILDLIST_PROC,
  BUILDVALUE_PROC,
  SCAN_PROC,
  UPDATE_PROC,
  DELETE_PROC,
  INSERT_PROC
} PROC_TYPE;

typedef enum
{
  XASL_CLEARED,
  XASL_SUCCESS,
  XASL_FAILURE,
  XASL_INITIALIZED
} XASL_STATUS;

typedef struct topn_tuple TOPN_TUPLE;
struct topn_tuple
{
  DB_VALUE *values;		/* tuple values */
  int values_size;		/* total size in bytes occupied by the
				 * objects stored in the values array
				 */
};

typedef enum
{
  TOPN_SUCCESS,
  TOPN_OVERFLOW,
  TOPN_FAILURE
} TOPN_STATUS;

/* top-n sorting object */
typedef struct topn_tuples TOPN_TUPLES;
struct topn_tuples
{
  SORT_LIST *sort_items;	/* sort items position in tuple and sort
				 * order */
  BINARY_HEAP *heap;		/* heap used to hold top-n tuples */
  TOPN_TUPLE *tuples;		/* actual tuples stored in memory */
  int values_count;		/* number of values in a tuple */
  UINT64 total_size;		/* size in bytes of stored tuples */
  UINT64 max_size;		/* maximum size which tuples may occupy */
};

typedef struct orderby_stat ORDERBY_STATS;
struct orderby_stat
{
  struct timeval orderby_time;
  bool orderby_filesort;
  bool orderby_topnsort;
  UINT64 orderby_pages;
  UINT64 orderby_ioreads;
};

typedef struct groupby_stat GROUPBY_STATS;
struct groupby_stat
{
  struct timeval groupby_time;
  UINT64 groupby_pages;
  UINT64 groupby_ioreads;
  int rows;
  bool run_groupby;
  bool groupby_sort;
};

typedef struct xasl_stat XASL_STATS;
struct xasl_stat
{
  struct timeval elapsed_time;
  UINT64 fetches;
  UINT64 ioreads;
};

struct xasl_node
{
  XASL_NODE_HEADER header;	/* XASL header */
  XASL_NODE *next;		/* next XASL block */
  PROC_TYPE type;		/* XASL type */
  int flag;			/* flags */
  QFILE_LIST_ID *list_id;	/* list file identifier */
  SORT_LIST *after_iscan_list;	/* sorting fields */
  SORT_LIST *orderby_list;	/* sorting fields */
  PRED_EXPR *ordbynum_pred;	/* orderby_num() predicate */
  DB_VALUE *ordbynum_val;	/* orderby_num() value result */
  REGU_VARIABLE *orderby_limit;	/* the limit to use in top K sorting. Computed
				 * from [ordby_num < X] clauses */
  int ordbynum_flag;		/* stop or continue ordering? */
  TOPN_TUPLES *topn_items;	/* top-n tuples for orderby limit */
  XASL_STATUS status;		/* current status */

  VAL_LIST *single_tuple;	/* single tuple result */

  int is_single_tuple;		/* single tuple subquery? */

  QUERY_OPTIONS option;		/* UNIQUE option */
  OUTPTR_LIST *outptr_list;	/* output pointer list */
  ACCESS_SPEC_TYPE *spec_list;	/* access spec. list */
  VAL_LIST *val_list;		/* output-value list */
  XASL_NODE *aptr_list;		/* first uncorrelated subquery */
  XASL_NODE *dptr_list;		/* corr. subquery list */
  PRED_EXPR *after_join_pred;	/* after-join predicate */
  PRED_EXPR *if_pred;		/* if predicate */
  PRED_EXPR *instnum_pred;	/* inst_num() predicate */
  DB_VALUE *instnum_val;	/* inst_num() value result */
  DB_VALUE *save_instnum_val;	/* inst_num() value kept after being substi-
				 * tuted for ordbynum_val; */
  REGU_VARIABLE *limit_row_count;	/* the record count from a limit clause */
  XASL_NODE *scan_ptr;		/* SCAN_PROC pointer */

  ACCESS_SPEC_TYPE *curr_spec;	/* current spec. node */
  int instnum_flag;		/* stop or continue scan? */
  int next_scan_on;		/* next scan is initiated ? */
  int next_scan_block_on;	/* next scan block is initiated ? */

  int query_in_progress;	/* flag which tells if the query is
				 * currently executing.  Used by
				 * qmgr_clear_trans_wakeup() to determine how
				 * much of the xasl tree to clean up.
				 */

  int upd_del_class_cnt;	/* number of classes affected by update or
				 * delete (used only in case of UPDATE or
				 * DELETE in the generated SELECT statement)
				 */

  union
  {
    UNION_PROC_NODE union_;	/* UNION_PROC,
				 * DIFFERENCE_PROC,
				 * INTERSECTION_PROC
				 */
    BUILDLIST_PROC_NODE buildlist;	/* BUILDLIST_PROC */
    BUILDVALUE_PROC_NODE buildvalue;	/* BUILDVALUE_PROC */
    UPDATE_PROC_NODE update;	/* UPDATE_PROC */
    INSERT_PROC_NODE insert;	/* INSERT_PROC */
    DELETE_PROC_NODE delete_;	/* DELETE_PROC */
  } proc;

  double cardinality;		/* estimated cardinality of result */

  ORDERBY_STATS orderby_stats;
  GROUPBY_STATS groupby_stats;
  XASL_STATS xasl_stats;

  /* XASL cache related information */
  OID creator_oid;		/* OID of the user who created this XASL */
  int projected_size;		/* # of bytes per result tuple */
  int n_oid_list;		/* size of the referenced OID list */
  OID *class_oid_list;		/* list of class OIDs referenced in the XASL */
  int *tcard_list;		/* list of #pages of the class OIDs */
  int dbval_cnt;		/* number of host variables in this XASL */
  HL_HEAPID private_heap_id;
};

#define XASL_LINK_TO_REGU_VARIABLE 1	/* is linked to regu variable ? */
#define XASL_SKIP_ORDERBY_LIST     2	/* skip sorting for orderby_list ? */
#define XASL_ZERO_CORR_LEVEL       4	/* is zero-level uncorrelated subquery ? */
#define XASL_TOP_MOST_XASL         8	/* this is a top most XASL */
#if 0
#define XASL_TO_BE_CACHED         16	/* reserved */	/* the result will be cached */
#endif
#define XASL_TO_CATALOG_TABLE   32	/* is DML to system catalog table */
#define	XASL_TO_SHARD_TABLE	  64	/* is DML to shard table */
#if 0
#define XASL_FLAG_RESERVED_02    128	/* reserved */
#define XASL_FLAG_RESERVED_03	 256	/* reserved */
#define XASL_FLAG_RESERVED_04	 512	/* reserved */
#define	XASL_OBJFETCH_IGNORE_CLASSOID 1024	/* fetch proc should ignore class oid */
#endif
#define XASL_USES_MRO	      2048	/* query uses multi range optimization */
#if 0
#define XASL_FLAG_RESERVED_05	     4096	/* reserved */
#endif
#define XASL_NO_FIXED_SCAN    8192	/* disable fixed scan for this proc */

#define XASL_IS_FLAGED(x, f)        ((x)->flag & (int) (f))
#define XASL_SET_FLAG(x, f)         (x)->flag |= (int) (f)
#define XASL_CLEAR_FLAG(x, f)       (x)->flag &= (int) ~(f)

#define EXECUTE_REGU_VARIABLE_XASL(thread_p, r, v)                            \
do {                                                                          \
    XASL_NODE *_x = REGU_VARIABLE_XASL(r);                                    \
                                                                              \
    /* check for xasl node                                               */   \
    if (_x) {                                                                 \
        if (XASL_IS_FLAGED(_x, XASL_LINK_TO_REGU_VARIABLE)) {                 \
            /* clear correlated subquery list files                      */   \
            if ((_x)->status == XASL_CLEARED				      \
		|| (_x)->status == XASL_INITIALIZED) {                        \
                /* execute xasl query                                    */   \
                qexec_execute_mainblock((thread_p), _x, (v)->xasl_state);     \
            } /* else: already evaluated. success or failure */               \
        } else {                                                              \
            /* currently, not-supported unknown case                     */   \
            (_x)->status = XASL_FAILURE; /* return error              */      \
        }                                                                     \
    }                                                                         \
} while (0)

#define CHECK_REGU_VARIABLE_XASL_STATUS(r)                                    \
    (REGU_VARIABLE_XASL(r) ? (REGU_VARIABLE_XASL(r))->status : XASL_SUCCESS)

/*
 * Moved to a public place to allow for streaming queries to setup
 * the list file up front.
 */
typedef struct xasl_state XASL_STATE;
struct xasl_state
{
  VAL_DESCR vd;			/* Value Descriptor */
  QUERY_ID query_id;		/* Query associated with XASL */
  int qp_xasl_line;		/* Error line */
  int shard_groupid;		/* shard groupid for INSERT */
};				/* XASL Tree State Information */


/*
 * xasl head node information
 */

typedef struct xasl_qstr_ht_key XASL_QSTR_HT_KEY;
struct xasl_qstr_ht_key
{
  const char *query_string;
  OID creator_oid;		/* OID of the user who created this XASL */
};

/* XASL cache entry type definition */
typedef struct xasl_cache_ent XASL_CACHE_ENTRY;
struct xasl_cache_ent
{
  EXECUTION_INFO sql_info;	/* cache entry hash key, user input string & plan */

  XASL_ID xasl_id;		/* XASL file identifier */
  int xasl_header_flag;		/* XASL header info */
#if defined(SERVER_MODE)
  char *tran_fix_count_array;	/* fix count of each transaction;
				 * size is MAX_NTRANS */
  int num_fixed_tran;		/* number of transactions
				 * fixed this entry */
#endif
  const OID *class_oid_list;	/* list of class OIDs referenced in the XASL */
  const int *tcard_list;	/* list of #pages of the class OIDs */
  struct timeval time_created;	/* when this entry created */
  struct timeval time_last_used;	/* when this entry used lastly */
  int n_oid_list;		/* size of the class OID list */
  int ref_count;		/* how many times this entry used */
  int dbval_cnt;		/* number of DB_VALUE parameters of the XASL */
  struct xasl_cache_clo *clo_list;	/* list of cache clones for this XASL */
  bool deletion_marker;		/* this entry will be deleted if marker set */
  XASL_QSTR_HT_KEY qstr_ht_key;	/* The key of query string hash table */
  HENTRY_PTR qstr_ht_entry_ptr;	/* Hash entry of the query string hash table
				 * that holds this xasl cache entry.
				 * This pointer is used to update
				 * query string hash table's lru list.
				 */
};

/* XASL cache clone type definition */
typedef struct xasl_cache_clo XASL_CACHE_CLONE;
struct xasl_cache_clo
{
  XASL_CACHE_CLONE *next;
  XASL_CACHE_CLONE *LRU_prev;
  XASL_CACHE_CLONE *LRU_next;
  XASL_CACHE_ENTRY *ent_ptr;	/* cache entry pointer */
  XASL_NODE *xasl;		/* XASL tree root pointer */
};

typedef struct cirp_ct_index_stats CIRP_CT_INDEX_STATS;
struct cirp_ct_index_stats
{
  char *table_name;
  char *index_name;
  int pages;
  int leafs;
  int height;
  INT64 keys;
  INT64 leaf_space_free;
  double leaf_pct_free;
  int num_table_vpids;		/* Number of total pages for file table */
  int num_user_pages_mrkdelete;	/* Num marked deleted pages */
  int num_allocsets;		/* Number of volume arrays */
};

extern QFILE_LIST_ID *qexec_execute_query (THREAD_ENTRY * thread_p,
					   XASL_NODE * xasl, int dbval_cnt,
					   DB_VALUE * dbval_ptr,
					   QUERY_ID query_id,
					   int shard_groupid,
					   DB_VALUE * shard_key,
					   bool req_from_migrator);
extern int qexec_execute_mainblock (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
				    XASL_STATE * xasl_state);
extern int qexec_start_mainblock_iterations (THREAD_ENTRY * thread_p,
					     XASL_NODE * xasl,
					     XASL_STATE * xasl_state);
extern int qexec_clear_xasl (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
			     bool final);

extern QFILE_LIST_ID *qexec_get_xasl_list_id (XASL_NODE * xasl);
#if defined(RYE_DEBUG)
extern void get_xasl_dumper_linked_in ();
#endif

/* XASL cache entry manipulation functions */
extern int qexec_initialize_xasl_cache (THREAD_ENTRY * thread_p);
extern int qexec_finalize_xasl_cache (THREAD_ENTRY * thread_p);
extern int qexec_dump_xasl_cache_internal (THREAD_ENTRY * thread_p, FILE * fp,
					   int mask);
#if defined(RYE_DEBUG)
extern int qexec_dump_xasl_cache (THREAD_ENTRY * thread_p, const char *fname,
				  int mask);
#endif
extern XASL_CACHE_ENTRY *qexec_lookup_xasl_cache_ent (THREAD_ENTRY * thread_p,
						      const char *qstr,
						      const OID * user_oid);
extern XASL_CACHE_ENTRY *qexec_update_xasl_cache_ent (THREAD_ENTRY * thread_p,
						      COMPILE_CONTEXT *
						      context,
						      XASL_STREAM * stream,
						      const OID * oid,
						      int n_oids,
						      const OID * class_oids,
						      const int *repr_ids,
						      int dbval_cnt);

extern int qexec_remove_my_tran_id_in_xasl_entry (THREAD_ENTRY * thread_p,
						  XASL_CACHE_ENTRY * ent,
						  bool unfix_all);

extern XASL_CACHE_ENTRY *qexec_check_xasl_cache_ent_by_xasl (THREAD_ENTRY *
							     thread_p,
							     const XASL_ID *
							     xasl_id,
							     int dbval_cnt,
							     XASL_CACHE_CLONE
							     ** clop,
							     bool save_clop);
extern int qexec_free_xasl_cache_clo (XASL_CACHE_CLONE * clo);
extern int xasl_id_hash_cmpeq (const void *key1, const void *key2);
extern int qexec_remove_xasl_cache_ent_by_class (THREAD_ENTRY * thread_p,
						 const OID * class_oid,
						 int force_remove);
extern int qexec_remove_xasl_cache_ent_by_qstr (THREAD_ENTRY * thread_p,
						const char *qstr,
						const OID * user_oid);
extern int qexec_remove_xasl_cache_ent_by_xasl (THREAD_ENTRY * thread_p,
						const XASL_ID * xasl_id);
extern int qexec_remove_all_xasl_cache_ent_by_xasl (THREAD_ENTRY * thread_p);
extern bool qdump_print_xasl (XASL_NODE * xasl);
#if defined(RYE_DEBUG)
extern bool qdump_check_xasl_tree (XASL_NODE * xasl);
#endif /* RYE_DEBUG */
extern int xts_map_xasl_to_stream (const XASL_NODE * xasl,
				   XASL_STREAM * stream);

#if defined (ENABLE_UNUSED_FUNCTION)
extern void xts_final (void);
#endif

extern int stx_map_stream_to_xasl (THREAD_ENTRY * thread_p,
				   XASL_NODE ** xasl_tree, char *xasl_stream,
				   int xasl_stream_size);
extern int stx_map_stream_to_xasl_node_header (THREAD_ENTRY * thread_p,
					       XASL_NODE_HEADER *
					       xasl_header_p,
					       char *xasl_stream);
#if defined (SERVER_MODE)
extern void stx_free_xasl_unpack_info (void *unpack_info_ptr);
#endif /* SERVER_MODE */

#if defined (ENABLE_UNUSED_FUNCTION)
extern void stx_free_additional_buff (THREAD_ENTRY * thread_p,
				      void *unpack_info_ptr);
#endif

extern int qexec_get_tuple_column_value (QFILE_TUPLE tpl,
					 int index,
					 TP_DOMAIN * index_domain,
					 DB_VALUE * valp);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int qexec_set_tuple_column_value (QFILE_TUPLE tpl,
					 int index,
					 DB_VALUE * valp, TP_DOMAIN * domain);
#endif /* ENABLE_UNUSED_FUNCTION */
extern int qexec_insert_tuple_into_list (THREAD_ENTRY * thread_p,
					 QFILE_LIST_ID * list_id,
					 OUTPTR_LIST * outptr_list,
					 VAL_DESCR * vd,
					 QFILE_TUPLE_RECORD * tplrec);
extern int qexec_update_index_stats (THREAD_ENTRY * thread_p,
				     CIRP_CT_INDEX_STATS * index_stats);
extern int qexec_upsert_applier_info (THREAD_ENTRY * thread_p, DB_IDXKEY * pk,
				      CIRP_CT_LOG_APPLIER * applier);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int qexec_update_applier_info (THREAD_ENTRY * thread_p,
				      DB_IDXKEY * pk,
				      CIRP_CT_LOG_APPLIER * applier_info);
extern int qexec_update_analyzer_info (THREAD_ENTRY * thread_p,
				       DB_IDXKEY * pk,
				       CIRP_CT_LOG_ANALYZER * analyzer);
#endif
extern int qexec_upsert_analyzer_info (THREAD_ENTRY * thread_p,
				       DB_IDXKEY * pk,
				       CIRP_CT_LOG_ANALYZER * analyzer);

#if defined (SERVER_MODE)
extern void qdump_print_stats_json (XASL_NODE * xasl_p, json_t * parent);
extern void qdump_print_stats_text (FILE * fp, XASL_NODE * xasl_p,
				    int indent);
#endif /* SERVER_MODE */

extern const char *qdump_data_type_string (DB_TYPE type);
extern const char *qdump_function_type_string (FUNC_TYPE ftype);
#endif /* _QUERY_EXECUTOR_H_ */
