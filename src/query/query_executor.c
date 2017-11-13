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
 * query_executor.c - Query evaluator module
 */

#ident "$Id$"


#include "config.h"

#include <stdio.h>
#include <string.h>
#include <math.h>
#include <search.h>
#include <sys/timeb.h>
#if defined(SERVER_MODE)
#include "jansson.h"
#endif

#include "porting.h"
#include "error_manager.h"
#include "memory_alloc.h"
#include "oid.h"
#include "slotted_page.h"
#include "heap_file.h"
#include "page_buffer.h"
#include "extendible_hash.h"
#include "locator_sr.h"
#include "btree.h"
#include "repl_log.h"
#include "xserver_interface.h"
#include "regex38a.h"
#include "statistics_sr.h"

#if defined(SERVER_MODE)
#include "connection_error.h"
#include "thread.h"
#endif /* SERVER_MODE */

#include "query_manager.h"
#include "fetch.h"
#include "arithmetic.h"
#include "transaction_sr.h"
#include "system_parameter.h"
#include "memory_hash.h"
#include "parser.h"
#include "set_object.h"
#include "session.h"
#include "btree_load.h"
#include "transform.h"
#if defined(RYE_DEBUG)
#include "environment_variable.h"
#endif /* RYE_DEBUG */

#define QEXEC_GOTO_EXIT_ON_ERROR \
  do \
    { \
      qexec_failure_line (__LINE__, xasl_state); \
      goto exit_on_error; \
    } \
  while (0)

#define QEXEC_CLEAR_AGG_LIST_VALUE(agg_list) \
  do \
    { \
      AGGREGATE_TYPE *agg_ptr; \
      for (agg_ptr = (agg_list); agg_ptr; agg_ptr = agg_ptr->next) \
	{ \
	  if (agg_ptr->function == PT_GROUPBY_NUM) \
	    continue; \
	  pr_clear_value (agg_ptr->accumulator.value); \
	} \
    } \
  while (0)

#define QEXEC_EMPTY_ACCESS_SPEC_SCAN(specp) \
  ((specp)->type == TARGET_CLASS \
    && ((ACCESS_SPEC_HFID((specp)).vfid.fileid == NULL_FILEID \
         || ACCESS_SPEC_HFID((specp)).vfid.volid == NULL_VOLID)))

#define QEXEC_IS_MULTI_TABLE_UPDATE_DELETE(xasl)		\
    (assert (xasl->upd_del_class_cnt <= 1),			\
     (xasl->upd_del_class_cnt == 1 && xasl->scan_ptr != NULL))

#if 0
/* Note: the following macro is used just for replacement of a repetitive
 * text in order to improve the readability.
 */

#if !defined(SERVER_MODE)
#define pthread_mutex_init(a, b)
#define pthread_mutex_destroy(a)
#define pthread_mutex_lock(a)	0
#define pthread_mutex_unlock(a)
#endif
#endif

#define QEXEC_INITIALIZE_XASL_CACHE_CLO(c, e) \
  do \
    { \
      (c)->next = NULL; \
      (c)->LRU_prev = (c)->LRU_next = NULL; \
      (c)->ent_ptr = (e);	/* save entry pointer */ \
      (c)->xasl = NULL; \
    } \
  while (0)

#define MAKE_XASL_QSTR_HT_KEY(K, SQL, O)\
  do {\
    assert ((SQL) != NULL && (O) != NULL);\
    (K).query_string = (SQL);\
    COPY_OID((&(K).creator_oid), (O));\
  } while (0)

/* XASL scan block function */
typedef SCAN_CODE (*XSAL_SCAN_FUNC) (THREAD_ENTRY * thread_p, XASL_NODE *,
				     XASL_STATE *, QFILE_TUPLE_RECORD *,
				     void *);

/* pointer to XASL scan function */
typedef XSAL_SCAN_FUNC *XASL_SCAN_FNC_PTR;

typedef enum groupby_dimension_flag GROUPBY_DIMENSION_FLAG;
enum groupby_dimension_flag
{
  GROUPBY_DIM_FLAG_NONE = 0,
  GROUPBY_DIM_FLAG_GROUP_BY = 1,
  GROUPBY_DIM_FLAG_ROLLUP = 2,
  GROUPBY_DIM_FLAG_CUBE = 4,
  GROUPBY_DIM_FLAG_SET = 8
};

typedef struct groupby_dimension GROUPBY_DIMENSION;
struct groupby_dimension
{
  GROUPBY_DIMENSION_FLAG d_flag;	/* dimension info */
  AGGREGATE_TYPE *d_agg_list;	/* aggregation colunms list */
};

typedef struct groupby_state GROUPBY_STATE;
struct groupby_state
{
  int state;

  SORTKEY_INFO key_info;
  QFILE_LIST_SCAN_ID *input_scan;
#if 0				/* SortCache */
  VPID fixed_vpid;		/* current fixed page info of  */
  PAGE_PTR fixed_page;		/* input list file             */
#endif
  QFILE_LIST_ID *output_file;

  PRED_EXPR *having_pred;
  PRED_EXPR *grbynum_pred;
  DB_VALUE *grbynum_val;
  int grbynum_flag;
  XASL_NODE *eptr_list;
  AGGREGATE_TYPE *g_output_agg_list;
  REGU_VARIABLE_LIST g_regu_list;
  VAL_LIST *g_val_list;
  OUTPTR_LIST *g_outptr_list;
  XASL_NODE *xasl;
  XASL_STATE *xasl_state;

  RECDES current_key;
  RECDES gby_rec;
  QFILE_TUPLE_RECORD input_tpl;
  QFILE_TUPLE_RECORD *output_tplrec;
  int input_recs;

  int with_rollup;
  GROUPBY_DIMENSION *g_dim;	/* dimensions for Data Cube */
  int g_dim_levels;		/* dimensions size */

  SORT_CMP_FUNC *cmp_fn;
};

/*
 * Information required for processing the ORDBY_NUM() function. See
 * qexec_eval_ordbynum_pred ().
 */
typedef struct ordbynum_info ORDBYNUM_INFO;
struct ordbynum_info
{
  XASL_STATE *xasl_state;
  PRED_EXPR *ordbynum_pred;
  DB_VALUE *ordbynum_val;
  int ordbynum_flag;
  int ordbynum_pos_cnt;
  int *ordbynum_pos;
  int reserved[2];
};

/* XASL cache related things */

/* counters */
typedef struct xasl_cache_counter XASL_CACHE_COUNTER;
struct xasl_cache_counter
{
  unsigned int lookup;		/* counter of cache lookup */
  unsigned int hit;		/* counter of cache hit */
  unsigned int miss;		/* counter of cache miss */
  unsigned int full;		/* counter of cache full */
};

#if defined (SERVER_MODE)
typedef struct xasl_cache_ent_mark_deleted_list XASL_CACHE_MARK_DELETED_LIST;
struct xasl_cache_ent_mark_deleted_list
{
  XASL_CACHE_ENTRY *entry;
  bool removed_from_hash;
  XASL_CACHE_MARK_DELETED_LIST *next;
};
#endif

/* cache entries info */
typedef struct xasl_cache_ent_info XASL_CACHE_ENT_INFO;
struct xasl_cache_ent_info
{
  int max_entries;		/* max number of cache entries */
  int num;			/* number of cache entries in use */
  XASL_CACHE_COUNTER counter;	/* counter of cache entry */
  MHT_TABLE *qstr_ht;		/* memory hash table for XASL stream cache
				   referencing by query string */
  MHT_TABLE *xid_ht;		/* memory hash table for XASL stream cache
				   referencing by xasl file id (XASL_ID) */
  MHT_TABLE *oid_ht;		/* memory hash table for XASL stream cache
				   referencing by class/serial oid */
#if defined (SERVER_MODE)
  XASL_CACHE_MARK_DELETED_LIST *mark_deleted_list;
  XASL_CACHE_MARK_DELETED_LIST *mark_deleted_list_tail;
#endif
};

/* cache clones info */
typedef struct xasl_cache_clo_info XASL_CACHE_CLO_INFO;
struct xasl_cache_clo_info
{
  int max_clones;		/* max number of cache clones */
  int num;			/* number of cache clones in use */
  XASL_CACHE_COUNTER counter;	/* counter of cache clone */
  XASL_CACHE_CLONE *head;	/* LRU head of cache clones in use */
  XASL_CACHE_CLONE *tail;	/* LRU tail of cache clones in use */
  XASL_CACHE_CLONE *free_list;	/* cache clones in free */
  int n_alloc;			/* number of alloc_arr */
  XASL_CACHE_CLONE **alloc_arr;	/* alloced cache clones */
};

/* XASL cache entry pooling */
#define FIXED_SIZE_OF_POOLED_XASL_CACHE_ENTRY   4096
#define ADDITION_FOR_POOLED_XASL_CACHE_ENTRY    offsetof(POOLED_XASL_CACHE_ENTRY, s.entry)	/* s.next field */
#define POOLED_XASL_CACHE_ENTRY_FROM_XASL_CACHE_ENTRY(p) \
        ((POOLED_XASL_CACHE_ENTRY *) ((char*) p - ADDITION_FOR_POOLED_XASL_CACHE_ENTRY))

typedef union pooled_xasl_cache_entry POOLED_XASL_CACHE_ENTRY;
union pooled_xasl_cache_entry
{
  struct
  {
    int next;			/* next entry in the free list */
    XASL_CACHE_ENTRY entry;	/* XASL cache entry data */
  } s;
  char dummy[FIXED_SIZE_OF_POOLED_XASL_CACHE_ENTRY];
  /* 4K size including XASL cache entry itself
   *   and reserved spaces for
   *   xasl_cache_ent.sql_hash_text,
   *   xasl_cache_ent.sql_plan_text,
   *   xasl_cache_ent.class_oid_list, and
   *   xasl_cache_ent.tcard_list */
};

typedef struct xasl_cache_entry_pool XASL_CACHE_ENTRY_POOL;
struct xasl_cache_entry_pool
{
  POOLED_XASL_CACHE_ENTRY *pool;	/* array of POOLED_XASL_CACHE_ENTRY */
  int n_entries;		/* number of entries in the pool */
  int free_list;		/* the head(first entry) of the free list */
};

/* used for internal update/delete execution */
typedef struct upddel_class_info_internal UPDDEL_CLASS_INFO_INTERNAL;
struct upddel_class_info_internal
{
  OID *class_oid;		/* oid of current class */
  HFID *class_hfid;		/* hfid of current class */
  bool scan_cache_inited;	/* true if scan_cache member has valid data */
  HEAP_SCANCACHE scan_cache;	/* scan cache */

  HEAP_CACHE_ATTRINFO attr_info;	/* attribute cache info */
  bool is_attr_info_inited;	/* true if attr_info has valid data */
};

typedef struct ct_table_gid_skey CT_GID_SKEY;
struct ct_table_gid_skey
{
  int gid;
  DB_VALUE *skey;
};

typedef enum ct_modify_mode
{
  CT_MODIFY_MODE_INSERT,
  CT_MODIFY_MODE_UPDATE,
  CT_MODIFY_MODE_UPSERT
} CT_MODIFY_MODE;

typedef int (*cast_ct_table_to_idxkey) (DB_IDXKEY * val, void *ct_table);

typedef struct ct_table_cache_info CT_CACHE_INFO;
struct ct_table_cache_info
{
  OID class_oid;
  HFID hfid;
  BTID btid;
  cast_ct_table_to_idxkey cast_func;
  ATTR_ID *atts;
};


#define CT_CACHE_INFO_INITIALIZER \
  { NULL_OID_INITIALIZER, NULL_HFID_INITIALIZER, NULL_BTID_INITIALIZER, NULL, NULL}

static const int RESERVED_SIZE_FOR_XASL_CACHE_ENTRY =
  (FIXED_SIZE_OF_POOLED_XASL_CACHE_ENTRY -
   ADDITION_FOR_POOLED_XASL_CACHE_ENTRY);

/* XASL cache related things */

/* XASL entry cache and related information */
static XASL_CACHE_ENT_INFO xasl_ent_cache = {
  0,				/*max_entries */
  0,				/*num */
  {0,				/*lookup */
   0,				/*hit */
   0,				/*miss */
   0 /*full */ },		/*counter */
  NULL,				/*qstr_ht */
  NULL,				/*xid_ht */
  NULL				/*oid_ht */
#if defined (SERVER_MODE)
    ,
  NULL,				/* mark deleted list */
  NULL				/* mark deleted list tail pointer */
#endif
};

/* XASL clone cache and related information */
static XASL_CACHE_CLO_INFO xasl_clo_cache = {
  0,				/*max_clones */
  0,				/*num */
  {0,				/*lookup */
   0,				/*hit */
   0,				/*miss */
   0 /*full */ },		/*counter */
  NULL,				/*head */
  NULL,				/*tail */
  NULL,				/*free_list */
  0,				/*n_alloc */
  NULL				/*alloc_arr */
};

/* XASL cache entry pool */
static XASL_CACHE_ENTRY_POOL xasl_cache_entry_pool = { NULL, 0, -1 };

#if defined(SERVER_MODE)
pthread_mutex_t xasl_Cache_lock = PTHREAD_MUTEX_INITIALIZER;

#define XASL_CACHE_LOCK() pthread_mutex_lock(&xasl_Cache_lock)
#define XASL_CACHE_UNLOCK() pthread_mutex_unlock(&xasl_Cache_lock)
#else
#define XASL_CACHE_LOCK()
#define XASL_CACHE_UNLOCK()
#endif

/*
 *  XASL_CACHE_ENTRY memory structure :=
 *      [|ent structure itself|TRANID array(tran_id_array)
 *       |OID array(class_oid_ilst)|int array(tcard_list)
 *	 |char array(sql_hash_text)|char array(sql_plan_text)|char array(sql_user_text)]
 *  ; malloc all in one memory block
*/

#if defined(SERVER_MODE)

#define XASL_CACHE_ENTRY_ALLOC_SIZE(qlen, noid) \
        (sizeof(XASL_CACHE_ENTRY)       /* space for structure */ \
         + sizeof(char) * MAX_NTRANS	/* space for tran_fix_count_array */ \
         + sizeof(OID) * (noid)         /* space for class_oid_list */ \
         + sizeof(int) * (noid)    /* space for tcard_list */ \
         + (qlen))		/* space for sql_hash_text, sql_plan_text, sql_user_text */
#define XASL_CACHE_ENTRY_TRAN_FIX_COUNT_ARRAY(ent) \
        (int *) ((char *) ent + sizeof(XASL_CACHE_ENTRY))
#define XASL_CACHE_ENTRY_CLASS_OID_LIST(ent) \
        (OID *) ((char *) ent + sizeof(XASL_CACHE_ENTRY) + \
                 sizeof(char) * MAX_NTRANS)
#define XASL_CACHE_ENTRY_TCARD_LIST(ent) \
        (int *) ((char *) ent + sizeof(XASL_CACHE_ENTRY) + \
                      sizeof(char) * MAX_NTRANS + \
                      sizeof(OID) * ent->n_oid_list)
#define XASL_CACHE_ENTRY_SQL_HASH_TEXT(ent) \
        (char *) ((char *) ent + sizeof(XASL_CACHE_ENTRY) + \
                  sizeof(char) * MAX_NTRANS + \
                  sizeof(OID) * ent->n_oid_list + \
                  sizeof(int) * ent->n_oid_list)
#define XASL_CACHE_ENTRY_SQL_PLAN_TEXT(ent) \
        (char *) ((char *) ent + sizeof(XASL_CACHE_ENTRY) + \
                  sizeof(char) * MAX_NTRANS + \
                  sizeof(OID) * ent->n_oid_list + \
                  sizeof(int) * ent->n_oid_list + \
                  strlen (ent->sql_info.sql_hash_text) + 1)

#define XASL_CACHE_ENTRY_SQL_USER_TEXT(ent) \
        (char *) ((char *) ent + sizeof(XASL_CACHE_ENTRY) + \
                  sizeof(char) * MAX_NTRANS + \
                  sizeof(OID) * ent->n_oid_list + \
                  sizeof(int) * ent->n_oid_list + \
                  strlen (ent->sql_info.sql_hash_text) + 1 + \
                  (ent->sql_info.sql_plan_text ? \
		  (strlen (ent->sql_info.sql_plan_text) + 1) : 0))

#else /* SA_MODE */

#define XASL_CACHE_ENTRY_ALLOC_SIZE(qlen, noid) \
        (sizeof(XASL_CACHE_ENTRY)       /* space for structure */ \
         + sizeof(OID) * (noid)         /* space for class_oid_list */ \
         + sizeof(int) * (noid)    /* space for tcard_list */ \
         + (qlen))		/* space for sql_hash_text, sql_plan_text, sql_user_text */
#define XASL_CACHE_ENTRY_CLASS_OID_LIST(ent) \
        (OID *) ((char *) ent + sizeof(XASL_CACHE_ENTRY))
#define XASL_CACHE_ENTRY_TCARD_LIST(ent) \
        (int *) ((char *) ent + sizeof(XASL_CACHE_ENTRY) + \
                      sizeof(OID) * ent->n_oid_list)
#define XASL_CACHE_ENTRY_SQL_HASH_TEXT(ent) \
        (char *) ((char *) ent + sizeof(XASL_CACHE_ENTRY) + \
                  sizeof(OID) * ent->n_oid_list + \
                  sizeof(int) * ent->n_oid_list)
#define XASL_CACHE_ENTRY_SQL_PLAN_TEXT(ent) \
        (char *) ((char *) ent + sizeof(XASL_CACHE_ENTRY) + \
                  sizeof(OID) * ent->n_oid_list + \
                  sizeof(int) * ent->n_oid_list + \
                  strlen(ent->sql_info.sql_hash_text) + 1)
#define XASL_CACHE_ENTRY_SQL_USER_TEXT(ent) \
        (char *) ((char *) ent + sizeof(XASL_CACHE_ENTRY) + \
                  sizeof(OID) * ent->n_oid_list + \
                  sizeof(int) * ent->n_oid_list + \
                  strlen(ent->sql_info.sql_hash_text) + 1 + \
                  (ent->sql_info.sql_plan_text ? \
		  (strlen(ent->sql_info.sql_plan_text) + 1) : 0))
#endif /* SERVER_MODE */


/***************************************************************************
 * for qexec_modify_catalog_table
 ***************************************************************************/
pthread_mutex_t ct_Cache_lock = PTHREAD_MUTEX_INITIALIZER;

CT_CACHE_INFO Writer_info = CT_CACHE_INFO_INITIALIZER;
CT_CACHE_INFO Analyzer_info = CT_CACHE_INFO_INITIALIZER;
CT_CACHE_INFO Applier_info = CT_CACHE_INFO_INITIALIZER;
CT_CACHE_INFO Shard_gid_skey_info = CT_CACHE_INFO_INITIALIZER;
CT_CACHE_INFO Index_stats_info = CT_CACHE_INFO_INITIALIZER;


static DB_LOGICAL qexec_eval_instnum_pred (THREAD_ENTRY * thread_p,
					   XASL_NODE * xasl,
					   XASL_STATE * xasl_state);
static QPROC_TPLDESCR_STATUS qexec_generate_tuple_descriptor (THREAD_ENTRY *
							      thread_p,
							      QFILE_LIST_ID *
							      list_id,
							      VALPTR_LIST *
							      outptr_list,
							      VAL_DESCR * vd);
static int qexec_upddel_add_unique_oid_to_ehid (THREAD_ENTRY * thread_p,
						XASL_NODE * xasl,
						XASL_STATE * xasl_state);
static int qexec_end_one_iteration (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
				    XASL_STATE * xasl_state,
				    QFILE_TUPLE_RECORD * tplrec);
static void qexec_failure_line (int line, XASL_STATE * xasl_state);
#if defined (ENABLE_UNUSED_FUNCTION)
static void qexec_reset_regu_variable (REGU_VARIABLE * var);
static void qexec_reset_regu_variable_list (REGU_VARIABLE_LIST list);
static void qexec_reset_pred_expr (PRED_EXPR * pred);
#endif
static int qexec_clear_xasl_head (THREAD_ENTRY * thread_p, XASL_NODE * xasl);
static int qexec_clear_arith_list (XASL_NODE * xasl_p, ARITH_TYPE * list,
				   int final);
static int qexec_clear_regu_var (XASL_NODE * xasl_p, REGU_VARIABLE * regu_var,
				 int final);
static int qexec_clear_regu_list (XASL_NODE * xasl_p, REGU_VARIABLE_LIST list,
				  int final);
static void qexec_clear_db_val_list (QPROC_DB_VALUE_LIST list);
static int qexec_clear_pred (XASL_NODE * xasl_p, PRED_EXPR * pr, int final);
static int qexec_clear_access_spec_list (XASL_NODE * xasl_p,
					 THREAD_ENTRY * thread_p,
					 ACCESS_SPEC_TYPE * list, int final);
static int qexec_clear_agg_list (XASL_NODE * xasl_p, AGGREGATE_TYPE * list,
				 int final);
static void qexec_clear_head_lists (THREAD_ENTRY * thread_p,
				    XASL_NODE * xasl_list);
static void qexec_clear_scan_all_lists (THREAD_ENTRY * thread_p,
					XASL_NODE * xasl_list);
static void qexec_clear_all_lists (THREAD_ENTRY * thread_p,
				   XASL_NODE * xasl_list);
static DB_LOGICAL qexec_eval_ordbynum_pred (THREAD_ENTRY * thread_p,
					    ORDBYNUM_INFO * ordby_info);
static int qexec_ordby_put_next (THREAD_ENTRY * thread_p,
				 const RECDES * recdes, void *arg);
static int qexec_orderby_distinct (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
				   QUERY_OPTIONS option,
				   XASL_STATE * xasl_state);
static int qexec_orderby_distinct_by_sorting (THREAD_ENTRY * thread_p,
					      XASL_NODE * xasl,
					      QUERY_OPTIONS option,
					      XASL_STATE * xasl_state);
static DB_LOGICAL qexec_eval_grbynum_pred (THREAD_ENTRY * thread_p,
					   GROUPBY_STATE * gbstate);
static GROUPBY_STATE *qexec_initialize_groupby_state (GROUPBY_STATE * gbstate,
						      SORT_LIST *
						      groupby_list,
						      PRED_EXPR * having_pred,
						      PRED_EXPR *
						      grbynum_pred,
						      DB_VALUE * grbynum_val,
						      int grbynum_flag,
						      XASL_NODE * eptr_list,
						      AGGREGATE_TYPE *
						      g_agg_list,
						      REGU_VARIABLE_LIST
						      g_regu_list,
						      VAL_LIST * g_val_list,
						      OUTPTR_LIST *
						      g_outptr_list,
						      int with_rollup,
						      XASL_NODE * xasl,
						      XASL_STATE * xasl_state,
						      QFILE_TUPLE_VALUE_TYPE_LIST
						      * type_list,
						      QFILE_TUPLE_RECORD *
						      tplrec);
static void qexec_clear_groupby_state (THREAD_ENTRY * thread_p,
				       GROUPBY_STATE * gbstate);
static int qexec_gby_init_group_dim (GROUPBY_STATE * gbstate);
static void qexec_gby_clear_group_dim (THREAD_ENTRY * thread_p,
				       GROUPBY_STATE * gbstate);
static void qexec_gby_agg_tuple (THREAD_ENTRY * thread_p,
				 GROUPBY_STATE * gbstate, QFILE_TUPLE tpl,
				 int peek);
static void qexec_gby_start_group_dim (THREAD_ENTRY * thread_p,
				       GROUPBY_STATE * gbstate,
				       const RECDES * recdes);
static void qexec_gby_start_group (THREAD_ENTRY * thread_p,
				   GROUPBY_STATE * gbstate,
				   const RECDES * recdes, int N);
static void qexec_gby_finalize_group_val_list (THREAD_ENTRY * thread_p,
					       GROUPBY_STATE * gbstate,
					       int N);
static int qexec_gby_finalize_group_dim (THREAD_ENTRY * thread_p,
					 GROUPBY_STATE * gbstate,
					 const RECDES * recdes);
static void qexec_gby_finalize_group (THREAD_ENTRY * thread_p,
				      GROUPBY_STATE * gbstate, int N,
				      bool keep_list_file);
static SORT_STATUS qexec_gby_get_next (THREAD_ENTRY * thread_p,
				       RECDES * recdes, void *arg);
static int qexec_gby_put_next (THREAD_ENTRY * thread_p, const RECDES * recdes,
			       void *arg);
static int qexec_groupby (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
			  XASL_STATE * xasl_state,
			  QFILE_TUPLE_RECORD * tplrec);
static int qexec_groupby_index (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
				XASL_STATE * xasl_state,
				QFILE_TUPLE_RECORD * tplrec);
static int qexec_open_scan (THREAD_ENTRY * thread_p,
			    ACCESS_SPEC_TYPE * curr_spec, VAL_LIST * val_list,
			    VAL_DESCR * vd, int readonly_scan, int fixed,
			    SCAN_ID * s_id, QUERY_ID query_id,
			    bool scan_immediately_stop);
static void qexec_close_scan (THREAD_ENTRY * thread_p,
			      ACCESS_SPEC_TYPE * curr_spec);
static void qexec_end_scan (THREAD_ENTRY * thread_p,
			    ACCESS_SPEC_TYPE * curr_spec);
static SCAN_CODE qexec_next_scan_block (THREAD_ENTRY * thread_p,
					XASL_NODE * xasl);
static SCAN_CODE qexec_next_scan_block_iterations (THREAD_ENTRY * thread_p,
						   XASL_NODE * xasl);
static SCAN_CODE qexec_execute_scan (THREAD_ENTRY * thread_p,
				     XASL_NODE * xasl,
				     XASL_STATE * xasl_state,
				     QFILE_TUPLE_RECORD * ignore,
				     XASL_SCAN_FNC_PTR next_scan_fnc);
static SCAN_CODE qexec_intprt_fnc (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
				   XASL_STATE * xasl_state,
				   QFILE_TUPLE_RECORD * tplrec,
				   XASL_SCAN_FNC_PTR next_scan_fnc);
static int qexec_setup_list_id (THREAD_ENTRY * thread_p, XASL_NODE * xasl);
static int qexec_init_upddel_ehash_files (THREAD_ENTRY * thread_p,
					  XASL_NODE * buildlist);
static void qexec_destroy_upddel_ehash_files (THREAD_ENTRY * thread_p,
					      XASL_NODE * buildlist);
static int qexec_execute_update (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
				 XASL_STATE * xasl_state);
static int qexec_execute_delete (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
				 XASL_STATE * xasl_state);
static int qexec_execute_insert (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
				 XASL_STATE * xasl_state, bool skip_aptr);
static int qexec_end_buildvalueblock_iterations (THREAD_ENTRY * thread_p,
						 XASL_NODE * xasl,
						 XASL_STATE * xasl_state,
						 QFILE_TUPLE_RECORD * tplrec);
static int qexec_end_mainblock_iterations (THREAD_ENTRY * thread_p,
					   XASL_NODE * xasl,
					   XASL_STATE * xasl_state,
					   QFILE_TUPLE_RECORD * tplrec);
static void qexec_clear_mainblock_iterations (THREAD_ENTRY * thread_p,
					      XASL_NODE * xasl);

static int qexec_check_modification (THREAD_ENTRY * thread_p,
				     XASL_NODE * xasl,
				     XASL_STATE * xasl_state);
static int qexec_execute_mainblock_internal (THREAD_ENTRY * thread_p,
					     XASL_NODE * xasl,
					     XASL_STATE * xasl_state);

static unsigned int xasl_id_hash (const void *key, unsigned int htsize);
static int qexec_print_xasl_cache_ent (FILE * fp, const void *key,
				       void *data, void *args);
static XASL_CACHE_ENTRY *qexec_alloc_xasl_cache_ent (int req_size);
static XASL_CACHE_CLONE *qexec_expand_xasl_cache_clo_arr (int n_exp);
static XASL_CACHE_CLONE *qexec_alloc_xasl_cache_clo (XASL_CACHE_ENTRY * ent);
static int qexec_append_LRU_xasl_cache_clo (XASL_CACHE_CLONE * clo);
static int qexec_delete_LRU_xasl_cache_clo (XASL_CACHE_CLONE * clo);
static int qexec_free_xasl_cache_ent (THREAD_ENTRY * thread_p, void *data,
				      void *args);
static int qexec_delete_xasl_cache_ent (THREAD_ENTRY * thread_p, void *data,
					void *args);
#if defined (ENABLE_UNUSED_FUNCTION)
static REGU_VARIABLE *replace_null_arith (REGU_VARIABLE * regu_var,
					  DB_VALUE * set_dbval);
static REGU_VARIABLE *replace_null_dbval (REGU_VARIABLE * regu_var,
					  DB_VALUE * set_dbval);
#endif
static int qexec_remove_duplicates_for_replace (THREAD_ENTRY * thread_p,
						HEAP_SCANCACHE * scan_cache,
						OID * oid,
						HEAP_CACHE_ATTRINFO *
						attr_info,
						HEAP_CACHE_ATTRINFO *
						index_attr_info,
						const HEAP_IDX_ELEMENTS_INFO *
						idx_info, int *removed_count);
static int qexec_oid_of_duplicate_key_update (THREAD_ENTRY * thread_p,
					      HEAP_SCANCACHE * scan_cache,
					      OID * oid,
					      HEAP_CACHE_ATTRINFO * attr_info,
					      HEAP_CACHE_ATTRINFO *
					      index_attr_info,
					      const HEAP_IDX_ELEMENTS_INFO *
					      idx_info, OID * unique_oid);
static int qexec_execute_duplicate_key_update (THREAD_ENTRY * thread_p,
					       ODKU_INFO * odku, HFID * hfid,
					       OID * oid, VAL_DESCR * vd,
					       HEAP_SCANCACHE * scan_cache,
					       HEAP_CACHE_ATTRINFO *
					       attr_info,
					       HEAP_CACHE_ATTRINFO *
					       index_attr_info,
					       HEAP_IDX_ELEMENTS_INFO *
					       idx_info, int *force_count);

#if defined (ENABLE_UNUSED_FUNCTION)
static int *tranid_lsearch (const int *key, int *base, int *nmemb);
static int *tranid_lfind (const int *key, const int *base, int *nmemb);
#endif

static int query_multi_range_opt_check_set_sort_col (THREAD_ENTRY * thread_p,
						     XASL_NODE * xasl);
static ACCESS_SPEC_TYPE *query_multi_range_opt_check_specs (THREAD_ENTRY *
							    thread_p,
							    XASL_NODE * xasl);
static int qexec_init_instnum_val (XASL_NODE * xasl,
				   THREAD_ENTRY * thread_p,
				   XASL_STATE * xasl_state);
static int qexec_lock_table_update_delete (THREAD_ENTRY * thread_p,
					   XASL_NODE * aptr_list,
					   UPDDEL_CLASS_INFO * upd_cls,
					   bool is_shard_table,
					   bool is_catalog_table);
static int qexec_lock_table_select_for_update (THREAD_ENTRY * thread_p,
					       XASL_NODE * scan_list,
					       int *num_shard_table,
					       bool * for_update);
static int qexec_init_internal_class (THREAD_ENTRY * thread_p,
				      UPDDEL_CLASS_INFO_INTERNAL * class);
static void qexec_clear_internal_class (THREAD_ENTRY * thread_p,
					UPDDEL_CLASS_INFO_INTERNAL * class);
static int qexec_upddel_setup_current_class (THREAD_ENTRY * thread_p,
					     UPDDEL_CLASS_INFO * class_,
					     UPDDEL_CLASS_INFO_INTERNAL *
					     class_info);
static int qexec_evaluate_aggregates_optimize (THREAD_ENTRY * thread_p,
					       AGGREGATE_TYPE * agg_list,
					       ACCESS_SPEC_TYPE * spec,
					       bool * is_scan_needed);

static int qexec_setup_topn_proc (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
				  VAL_DESCR * vd);
static BH_CMP_RESULT qexec_topn_compare (const BH_ELEM left,
					 const BH_ELEM right, BH_CMP_ARG arg);
static BH_CMP_RESULT qexec_topn_cmpval (DB_VALUE * left, DB_VALUE * right,
					SORT_LIST * sort_spec);
static TOPN_STATUS qexec_add_tuple_to_topn (THREAD_ENTRY * thread_p,
					    TOPN_TUPLES * sort_stop,
					    QFILE_TUPLE_DESCRIPTOR *
					    tpldescr);
static int qexec_topn_tuples_to_list_id (THREAD_ENTRY * thread_p,
					 XASL_NODE * xasl,
					 XASL_STATE * xasl_state,
					 bool is_final);
static void qexec_clear_topn_tuple (THREAD_ENTRY * thread_p,
				    TOPN_TUPLE * tuple, int count);
static int qexec_get_orderbynum_upper_bound (THREAD_ENTRY * tread_p,
					     PRED_EXPR * pred, VAL_DESCR * vd,
					     DB_VALUE * ubound);

#if defined(SERVER_MODE)
static void qexec_set_xasl_trace_to_session (THREAD_ENTRY * thread_p,
					     XASL_NODE * xasl);
static int qexec_remove_mark_deleted_xasl_entries (THREAD_ENTRY * thread_p,
						   int remove_count);
static int qexec_free_mark_deleted_xasl_entry (THREAD_ENTRY * thread_p,
					       XASL_CACHE_ENTRY * ent,
					       bool removed_from_hash);
static int qexec_mark_delete_xasl_entry (THREAD_ENTRY * thread_p,
					 XASL_CACHE_ENTRY * ent,
					 bool removed_from_hash);
#endif /* SERVER_MODE */

static unsigned int qexec_xasl_qstr_ht_hash (const void *key,
					     unsigned int ht_size);
static int qexec_xasl_qstr_ht_keys_are_equal (const void *key1,
					      const void *key2);
static int qexec_lock_table_and_shard_key (THREAD_ENTRY * thread_p,
					   XASL_NODE * xasl,
					   int shard_groupid,
					   DB_VALUE * shard_key,
					   bool req_from_migrator);
static int qexec_insert_gid_skey_info (THREAD_ENTRY * thread_p,
				       int shard_groupid,
				       DB_VALUE * shard_key);


static int qexec_modify_catalog_table (THREAD_ENTRY * thread_p,
				       DB_IDXKEY * pk, void *ct_data,
				       CATCLS_TABLE * table,
				       CT_MODIFY_MODE modify_mode);
/* for qexec_modify_catalog_table () */
static CT_CACHE_INFO *qexec_get_info_of_catalog_table (THREAD_ENTRY *
						       thread_p,
						       CATCLS_TABLE * table);
static int qexec_get_attrids_from_catalog_table (THREAD_ENTRY * thread_p,
						 int *att_ids,
						 OID * class_oid,
						 CATCLS_TABLE * ct_table);
static int qexec_insert_with_values (THREAD_ENTRY * thread_p, OID * class_oid,
				     HFID * hfid, ATTR_ID * attr_ids,
				     int force_page_allocation,
				     DB_IDXKEY * values);
static int qexec_update_with_values (THREAD_ENTRY * thread_p, OID * class_oid,
				     HFID * hfid, ATTR_ID * attr_ids,
				     OID * oid, DB_IDXKEY * values);
static int qexec_cast_ct_index_stats_to_idxkey (DB_IDXKEY * val,
						void *ct_table);
static int qexec_cast_ct_gid_skey_to_idxkey (DB_IDXKEY * val, void *ct_table);
static int qexec_cast_ct_applier_to_idxkey (DB_IDXKEY * val, void *ct_table);
static int qexec_cast_ct_analyzer_to_idxkey (DB_IDXKEY * val, void *ct_table);
static int qexec_cast_ct_writer_to_idxkey (DB_IDXKEY * val, void *ct_table);

/*
 * Utility routines
 */

/*
 * qexec_eval_instnum_pred () -
 *   return:
 *   xasl(in)   :
 *   xasl_state(in)     :
 */
static DB_LOGICAL
qexec_eval_instnum_pred (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
			 XASL_STATE * xasl_state)
{
  DB_LOGICAL ev_res;

  /* instant numbering; increase the value of inst_num() by 1 */
  if (xasl->instnum_val)
    {
      xasl->instnum_val->data.bigint++;
    }
  if (xasl->save_instnum_val)
    {
      xasl->save_instnum_val->data.bigint++;
    }

  if (xasl->instnum_pred)
    {
      PRED_EXPR *pr = xasl->instnum_pred;

      /* this case is for:
       *  select * from table limit 3,  or
       *  select * from table where rownum <= 3
       *  and we can change operator <= to < and reevaluate last
       *  condition. (to stop scan at this time)
       */
      if (pr->type == T_EVAL_TERM &&
	  pr->pe.eval_term.et_type == T_COMP_EVAL_TERM &&
	  (pr->pe.eval_term.et.et_comp.comp_lhs->type == TYPE_CONSTANT &&
	   pr->pe.eval_term.et.et_comp.comp_rhs->type == TYPE_POS_VALUE) &&
	  xasl->instnum_pred->pe.eval_term.et.et_comp.comp_rel_op == R_LE)
	{
	  xasl->instnum_pred->pe.eval_term.et.et_comp.comp_rel_op = R_LT;
	  /* evaluate predicate */
	  ev_res = eval_pred (thread_p, xasl->instnum_pred,
			      &xasl_state->vd, NULL);

	  xasl->instnum_pred->pe.eval_term.et.et_comp.comp_rel_op = R_LE;

	  if (ev_res != V_TRUE)
	    {
	      ev_res = eval_pred (thread_p, xasl->instnum_pred,
				  &xasl_state->vd, NULL);

	      if (ev_res == V_TRUE)
		{
		  xasl->instnum_flag |= XASL_INSTNUM_FLAG_SCAN_LAST_STOP;
		}
	    }
	}
      else
	{
	  /* evaluate predicate */
	  ev_res = eval_pred (thread_p, xasl->instnum_pred,
			      &xasl_state->vd, NULL);
	}

      switch (ev_res)
	{
	case V_FALSE:
	  /* evaluation is false; if check flag was set, stop scan */
	  if (xasl->instnum_flag & XASL_INSTNUM_FLAG_SCAN_CHECK)
	    {
	      xasl->instnum_flag |= XASL_INSTNUM_FLAG_SCAN_STOP;
	    }
	  break;
	case V_TRUE:
	  /* evaluation is true; if not continue scan mode, set scan check flag */
	  if (!(xasl->instnum_flag & XASL_INSTNUM_FLAG_SCAN_CONTINUE)
	      && !(xasl->instnum_flag & XASL_INSTNUM_FLAG_SCAN_CHECK))
	    {
	      xasl->instnum_flag |= XASL_INSTNUM_FLAG_SCAN_CHECK;
	    }
	  break;
	case V_ERROR:
	  break;
	default:		/* V_UNKNOWN */
	  break;
	}
    }
  else
    {
      /* no predicate; always true */
      ev_res = V_TRUE;
    }

  return ev_res;
}

/*
 * qexec_generate_tuple_descriptor () -
 *   return: status
 *   thread_p(in)   :
 *   list_id(in/out)     :
 *   outptr_list(in) :
 *   vd(in) :
 *
 */
static QPROC_TPLDESCR_STATUS
qexec_generate_tuple_descriptor (THREAD_ENTRY * thread_p,
				 QFILE_LIST_ID * list_id,
				 VALPTR_LIST * outptr_list, VAL_DESCR * vd)
{
  QPROC_TPLDESCR_STATUS status;
  size_t size;

  status = QPROC_TPLDESCR_FAILURE;	/* init */

  /* make f_valp array */
  if (list_id->tpl_descr.f_valp == NULL && list_id->type_list.type_cnt > 0)
    {
      size = list_id->type_list.type_cnt * DB_SIZEOF (DB_VALUE *);

      list_id->tpl_descr.f_valp = (DB_VALUE **) malloc (size);
      if (list_id->tpl_descr.f_valp == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OUT_OF_VIRTUAL_MEMORY, 1, size);
	  goto exit_on_error;
	}
    }

  /* build tuple descriptor */
  status =
    qdata_generate_tuple_desc_for_valptr_list (thread_p, list_id,
					       outptr_list,
					       vd, &(list_id->tpl_descr));
  if (status == QPROC_TPLDESCR_FAILURE)
    {
      goto exit_on_error;
    }

  return status;

exit_on_error:

  return QPROC_TPLDESCR_FAILURE;
}

/*
 * qexec_upddel_add_unique_oid_to_ehid () -
 *   return: error code (<0) or the number of removed OIDs (>=0).
 *   thread_p(in) :
 *   xasl(in) : The XASL node of the generated SELECT statement for UPDATE or
 *		DELETE. It must be a BUILDLIST_PROC and have the temporary hash
 *		files already created (upd_del_ehid).
 *   xasl_state(in) :
 *
 *  Note: This function is used only for the SELECT queries generated for UPDATE
 *	  or DELETE statements. It sets each instance OID from the outptr_list
 *	  to null if the OID already exists in the hash file associated with the
 *	  source table of the OID. (It eliminates duplicate OIDs in order to not
 *	  UPDATE/DELETE them more than once). The function returns the number of
 *	  removed OIDs so that the caller can remove the entire row from
 *	  processing (SELECT list) if all OIDs were removed. Otherwise only the
 *	  null instance OIDs will be skipped from UPDATE/DELETE processing.
 */
static int
qexec_upddel_add_unique_oid_to_ehid (THREAD_ENTRY * thread_p,
				     XASL_NODE * xasl,
				     XASL_STATE * xasl_state)
{
  REGU_VARIABLE_LIST reg_var_list = NULL;
  DB_VALUE *dbval = NULL, *orig_dbval = NULL;
  DB_TYPE typ;
  int ret = NO_ERROR, rem_cnt = 0;
  EHID *ehid = NULL;
  OID oid;
  EH_SEARCH eh_search;

  assert (xasl->upd_del_class_cnt == 1);

  if (xasl == NULL || xasl->type != BUILDLIST_PROC
      || EHID_IS_NULL (&(xasl->proc.buildlist.upd_del_ehid)))
    {
      return NO_ERROR;
    }

  reg_var_list = xasl->outptr_list->valptrp;
  if (reg_var_list != NULL)
    {
      ret = fetch_peek_dbval (thread_p, &reg_var_list->value, &xasl_state->vd,
			      NULL, NULL, &dbval);
      if (ret != NO_ERROR)
	{
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}

      if (!DB_IS_NULL (dbval))
	{
	  orig_dbval = dbval;

	  typ = DB_VALUE_DOMAIN_TYPE (dbval);
	  if (typ != DB_TYPE_OID)
	    {
	      db_value_clear (dbval);
	      QEXEC_GOTO_EXIT_ON_ERROR;
	    }

	  /* Get the hash file and check if the OID exists in the file */
	  ehid = &(xasl->proc.buildlist.upd_del_ehid);
	  eh_search = ehash_search (thread_p, ehid, DB_GET_OID (dbval), &oid);
	  switch (eh_search)
	    {
	    case EH_KEY_NOTFOUND:
	      /* The OID was not processed so insert it in the hash file */
	      if (ehash_insert (thread_p, ehid, DB_GET_OID (dbval),
				DB_GET_OID (dbval)) == NULL)
		{
		  QEXEC_GOTO_EXIT_ON_ERROR;
		}
	      break;

	    case EH_KEY_FOUND:
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_QPROC_MULTI_UPD_DEL, 0);
	      /* fall through */
	    case EH_ERROR_OCCURRED:
	    default:
	      assert (eh_search == EH_KEY_FOUND);
	      QEXEC_GOTO_EXIT_ON_ERROR;
	    }
	}
      else
	{
	  rem_cnt++;
	}
    }

  assert (rem_cnt == 0 || rem_cnt == 1);

  return rem_cnt;

exit_on_error:

  if (ret == NO_ERROR)
    {
      ret = er_errid ();
      if (ret == NO_ERROR)
	{
	  ret = ER_FAILED;
	}
    }

  assert (ret != NO_ERROR);

  return ret;
}

/*
 * qexec_end_one_iteration () -
 *   return: NO_ERROR or ER_code
 *   xasl(in)   :
 *   xasl_state(in)     :
 *   tplrec(in) :
 *
 * Note: Processing to be accomplished when a candidate row has been qualified.
 */
static int
qexec_end_one_iteration (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
			 XASL_STATE * xasl_state, QFILE_TUPLE_RECORD * tplrec)
{
  QPROC_TPLDESCR_STATUS tpldescr_status;
  TOPN_STATUS topn_stauts = TOPN_SUCCESS;
  int ret = NO_ERROR;
  bool output_tuple = true;

  if (QEXEC_IS_MULTI_TABLE_UPDATE_DELETE (xasl))
    {
      /* Remove OIDs already processed */
      ret = qexec_upddel_add_unique_oid_to_ehid (thread_p, xasl, xasl_state);
      if (ret < 0)
	{
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}

      if (ret == xasl->upd_del_class_cnt)
	{
	  assert (ret == 1);
	  return NO_ERROR;
	}
    }

  if (xasl->type == BUILDLIST_PROC)
    {
      tpldescr_status = qexec_generate_tuple_descriptor (thread_p,
							 xasl->list_id,
							 xasl->outptr_list,
							 &xasl_state->vd);
      if (tpldescr_status == QPROC_TPLDESCR_FAILURE)
	{
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}

      /* process tuple */
      switch (tpldescr_status)
	{
	case QPROC_TPLDESCR_SUCCESS:
	  if (xasl->topn_items != NULL)
	    {
	      topn_stauts = qexec_add_tuple_to_topn (thread_p,
						     xasl->topn_items,
						     &xasl->list_id->
						     tpl_descr);
	      if (topn_stauts == TOPN_SUCCESS)
		{
		  /* successfully added tuple */
		  break;
		}
	      else if (topn_stauts == TOPN_FAILURE)
		{
		  /* error while adding tuple */
		  QEXEC_GOTO_EXIT_ON_ERROR;
		}

	      assert (topn_stauts == TOPN_OVERFLOW);

	      /* The new tuple overflows the topn size. Dump current results
	       * to list_id and continue with normal execution. The current
	       * tuple (from tpl_descr) was not added to the list yet, it will
	       * be added below.
	       */
	      if (qfile_generate_tuple_into_list (thread_p, xasl->list_id,
						  T_NORMAL) != NO_ERROR)
		{
		  QEXEC_GOTO_EXIT_ON_ERROR;
		}
	      if (qexec_topn_tuples_to_list_id (thread_p, xasl, xasl_state,
						false) != NO_ERROR)
		{
		  QEXEC_GOTO_EXIT_ON_ERROR;
		}
	      output_tuple = false;
	      assert (xasl->topn_items == NULL);
	    }

	  if (output_tuple)
	    {
	      /* generate tuple into list file page */
	      if (qfile_generate_tuple_into_list (thread_p, xasl->list_id,
						  T_NORMAL) != NO_ERROR)
		{
		  QEXEC_GOTO_EXIT_ON_ERROR;
		}
	    }
	  break;

	case QPROC_TPLDESCR_RETRY_SET_TYPE:
	case QPROC_TPLDESCR_RETRY_BIG_REC:
	  /* BIG QFILE_TUPLE or a SET-field is included */
	  if (tplrec->tpl == NULL)
	    {
	      /* allocate tuple descriptor */
	      tplrec->size = DB_PAGESIZE;
	      tplrec->tpl = (QFILE_TUPLE) malloc (DB_PAGESIZE);
	      if (tplrec->tpl == NULL)
		{
		  QEXEC_GOTO_EXIT_ON_ERROR;
		}
	    }

	  if (qdata_copy_valptr_list_to_tuple (thread_p, xasl->list_id,
					       xasl->outptr_list,
					       &xasl_state->vd,
					       tplrec) != NO_ERROR)
	    {
	      QEXEC_GOTO_EXIT_ON_ERROR;
	    }

	  if (qfile_add_tuple_to_list (thread_p, xasl->list_id,
				       tplrec->tpl) != NO_ERROR)
	    {
	      QEXEC_GOTO_EXIT_ON_ERROR;
	    }
	  break;

	default:
	  break;
	}

      if (xasl->topn_items != NULL
	  && tpldescr_status != QPROC_TPLDESCR_SUCCESS)
	{
	  /* abandon top-n processing */
	  if (qexec_topn_tuples_to_list_id (thread_p, xasl, xasl_state, false)
	      != NO_ERROR)
	    {
	      QEXEC_GOTO_EXIT_ON_ERROR;
	    }
	  assert (xasl->topn_items == NULL);
	}
    }
  else if (xasl->type == BUILDVALUE_PROC)
    {
      if (qdata_evaluate_aggregate_list
	  (thread_p, xasl->proc.buildvalue.agg_list,
	   &xasl_state->vd) != NO_ERROR)
	{
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}
    }

  return ret;

exit_on_error:

  return (ret == NO_ERROR
	  && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;
}

/*
 * Clean_up processing routines
 */

/*
 * qexec_failure_line () -
 *   return: int
 *   line(in)   :
 *   xasl_state(in)     :
 */
static void
qexec_failure_line (int line, XASL_STATE * xasl_state)
{
  if (!xasl_state->qp_xasl_line)
    {
      xasl_state->qp_xasl_line = line;
    }
}

/*
 * qexec_clear_xasl_head () -
 *   return: int
 *   xasl(in)   : XASL Tree procedure block
 *
 * Note: Clear XASL head node by destroying the resultant list file,
 * if any, and also resultant single values, if any. Return the
 * number of total pages deallocated.
 */
static int
qexec_clear_xasl_head (THREAD_ENTRY * thread_p, XASL_NODE * xasl)
{
  int pg_cnt = 0;
  VAL_LIST *single_tuple;
  QPROC_DB_VALUE_LIST value_list;
  int i;

  if (xasl->list_id)
    {				/* destroy list file */
      (void) qfile_close_list (thread_p, xasl->list_id);
      qfile_destroy_list (thread_p, xasl->list_id);
    }

  single_tuple = xasl->single_tuple;
  if (single_tuple)
    {
      /* clear result value */
      for (value_list = single_tuple->valp, i = 0;
	   i < single_tuple->val_cnt; value_list = value_list->next, i++)
	{
	  pr_clear_value (value_list->val);
	}
    }

  xasl->status = XASL_CLEARED;

  return pg_cnt;
}

/*
 * qexec_clear_arith_list () - clear the db_values in the db_val list
 *   return:
 *   xasl_p(in) :
 *   list(in)   :
 *   final(in)  :
 */
static int
qexec_clear_arith_list (XASL_NODE * xasl_p, ARITH_TYPE * list, int loose_end)
{
  ARITH_TYPE *p = list;
  int pg_cnt;

  pg_cnt = 0;
  if (p != NULL)
    {
      pg_cnt += qexec_clear_regu_var (xasl_p, p->leftptr, loose_end);
      pg_cnt += qexec_clear_regu_var (xasl_p, p->rightptr, loose_end);
      pg_cnt += qexec_clear_regu_var (xasl_p, p->thirdptr, loose_end);
      pg_cnt += qexec_clear_pred (xasl_p, p->pred, loose_end);

      if (loose_end == false)
	{
	  pr_clear_value (p->value);
	  if (p->rand_seed != NULL)
	    {
	      free_and_init (p->rand_seed);
	    }
	}
    }

  return pg_cnt;
}

/*
 * qexec_clear_regu_var () - clear the db_values in the regu_variable
 *   return:
 *   xasl_p(in) :
 *   regu_var(in) :      :
 *   final(in)  :
 */
static int
qexec_clear_regu_var (XASL_NODE * xasl_p, REGU_VARIABLE * regu_var,
		      int loose_end)
{
  int pg_cnt;

  pg_cnt = 0;
  if (!regu_var)
    {
      return pg_cnt;
    }

#if !defined(NDEBUG)
  if (loose_end == false)
    {
      if (REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_ALL_CONST))
	{
	  assert (!REGU_VARIABLE_IS_FLAGED (regu_var,
					    REGU_VARIABLE_FETCH_NOT_CONST));
	}
      if (REGU_VARIABLE_IS_FLAGED (regu_var, REGU_VARIABLE_FETCH_NOT_CONST))
	{
	  assert (!REGU_VARIABLE_IS_FLAGED (regu_var,
					    REGU_VARIABLE_FETCH_ALL_CONST));
	}
    }
#endif

  /* clear run-time setting info */
  REGU_VARIABLE_CLEAR_FLAG (regu_var, REGU_VARIABLE_FETCH_ALL_CONST);
  REGU_VARIABLE_CLEAR_FLAG (regu_var, REGU_VARIABLE_FETCH_NOT_CONST);

  switch (regu_var->type)
    {
    case TYPE_ATTR_ID:		/* fetch object attribute value */
      regu_var->value.attr_descr.cache_dbvalp = NULL;
      break;
    case TYPE_CONSTANT:
      if (loose_end == false)
	{
	  pg_cnt += pr_clear_value (regu_var->value.dbvalptr);
	}
      /* Fall through */
    case TYPE_LIST_ID:
      if (regu_var->xasl != NULL && regu_var->xasl->status != XASL_CLEARED)
	{
	  pg_cnt += qexec_clear_xasl (NULL, regu_var->xasl, loose_end);
	}
      break;
    case TYPE_INARITH:
    case TYPE_OUTARITH:
      pg_cnt += qexec_clear_arith_list (xasl_p, regu_var->value.arithptr,
					loose_end);
      break;
    case TYPE_FUNC:
      if (loose_end == false)
	{
	  pr_clear_value (regu_var->value.funcp->value);
	}
      pg_cnt += qexec_clear_regu_list (xasl_p, regu_var->value.funcp->operand,
				       loose_end);
      break;
    case TYPE_DBVAL:
      if (loose_end == true)
	{
	  pr_clear_value (&regu_var->value.dbval);
	}
      break;
    default:
      break;
    }

  return pg_cnt;
}


/*
 * qexec_clear_regu_list () - clear the db_values in the regu list
 *   return:
 *   xasl_p(in) :
 *   list(in)   :
 *   final(in)  :
 */
static int
qexec_clear_regu_list (XASL_NODE * xasl_p, REGU_VARIABLE_LIST list,
		       int loose_end)
{
  REGU_VARIABLE_LIST p;
  int pg_cnt;

  pg_cnt = 0;
  for (p = list; p; p = p->next)
    {
      pg_cnt += qexec_clear_regu_var (xasl_p, &p->value, loose_end);
    }

  return pg_cnt;
}

/*
 * qexec_clear_db_val_list () - clear the db_values in the db_val list
 *   return:
 *   list(in)   :
 */
static void
qexec_clear_db_val_list (QPROC_DB_VALUE_LIST list)
{
  QPROC_DB_VALUE_LIST p;

  for (p = list; p; p = p->next)
    {
      pr_clear_value (p->val);
    }

}

/*
 * qexec_clear_pred () - clear the db_values in a predicate
 *   return:
 *   xasl_p(in) :
 *   pr(in)     :
 *   final(in)  :
 */
static int
qexec_clear_pred (XASL_NODE * xasl_p, PRED_EXPR * pr, int loose_end)
{
  int pg_cnt;
  PRED_EXPR *expr;

  pg_cnt = 0;
  if (!pr)
    {
      return pg_cnt;
    }

  switch (pr->type)
    {
    case T_PRED:
      pg_cnt += qexec_clear_pred (xasl_p, pr->pe.pred.lhs, loose_end);
      for (expr = pr->pe.pred.rhs;
	   expr && expr->type == T_PRED; expr = expr->pe.pred.rhs)
	{
	  pg_cnt += qexec_clear_pred (xasl_p, expr->pe.pred.lhs, loose_end);
	}
      pg_cnt += qexec_clear_pred (xasl_p, expr, loose_end);
      break;
    case T_EVAL_TERM:
      switch (pr->pe.eval_term.et_type)
	{
	case T_COMP_EVAL_TERM:
	  {
	    COMP_EVAL_TERM *et_comp = &pr->pe.eval_term.et.et_comp;

	    pg_cnt += qexec_clear_regu_var (xasl_p, et_comp->comp_lhs,
					    loose_end);
	    pg_cnt += qexec_clear_regu_var (xasl_p, et_comp->comp_rhs,
					    loose_end);
	  }
	  break;
	case T_ALSM_EVAL_TERM:
	  {
	    ALSM_EVAL_TERM *et_alsm = &pr->pe.eval_term.et.et_alsm;

	    pg_cnt += qexec_clear_regu_var (xasl_p, et_alsm->elem, loose_end);
	    pg_cnt += qexec_clear_regu_var (xasl_p, et_alsm->elemset,
					    loose_end);
	  }
	  break;
	case T_LIKE_EVAL_TERM:
	  {
	    LIKE_EVAL_TERM *et_like = &pr->pe.eval_term.et.et_like;

	    pg_cnt += qexec_clear_regu_var (xasl_p, et_like->src, loose_end);
	    pg_cnt += qexec_clear_regu_var (xasl_p, et_like->pattern,
					    loose_end);
	    pg_cnt += qexec_clear_regu_var (xasl_p, et_like->esc_char,
					    loose_end);
	  }
	  break;
	case T_RLIKE_EVAL_TERM:
	  {
	    RLIKE_EVAL_TERM *et_rlike = &pr->pe.eval_term.et.et_rlike;

	    pg_cnt += qexec_clear_regu_var (xasl_p, et_rlike->src, loose_end);
	    pg_cnt += qexec_clear_regu_var (xasl_p, et_rlike->pattern,
					    loose_end);
	    pg_cnt += qexec_clear_regu_var (xasl_p, et_rlike->case_sensitive,
					    loose_end);

	    if (loose_end == false)
	      {
		/* free memory of compiled regex object */
		if (et_rlike->compiled_regex != NULL)
		  {
		    cub_regfree (et_rlike->compiled_regex);
		    free_and_init (et_rlike->compiled_regex);
		  }

		/* free memory of regex compiled pattern */
		if (et_rlike->compiled_pattern != NULL)
		  {
		    free_and_init (et_rlike->compiled_pattern);
		  }
	      }

	  }
	  break;
	}
      break;
    case T_NOT_TERM:
      pg_cnt += qexec_clear_pred (xasl_p, pr->pe.not_term, loose_end);
      break;
    }

  return pg_cnt;
}

/*
 * qexec_clear_access_spec_list () - clear the db_values in the access spec list
 *   return:
 *   xasl_p(in) :
 *   list(in)   :
 *   final(in)  :
 */
static int
qexec_clear_access_spec_list (XASL_NODE * xasl_p, THREAD_ENTRY * thread_p,
			      ACCESS_SPEC_TYPE * list, int loose_end)
{
  ACCESS_SPEC_TYPE *p;
  HEAP_SCAN_ID *hsidp;
  INDX_SCAN_ID *isidp;
  int pg_cnt;

  /* I'm not sure this access structure could be anymore complicated
   * (surely some of these dbvalues are redundant)
   */

  pg_cnt = 0;
  for (p = list; p; p = p->next)
    {
      pg_cnt += qexec_clear_pred (xasl_p, p->where_key, loose_end);
      pg_cnt += qexec_clear_pred (xasl_p, p->where_pred, loose_end);

      switch (p->s_id.type)
	{
	case S_HEAP_SCAN:
	  pg_cnt +=
	    qexec_clear_regu_list (xasl_p, p->s_id.s.hsid.scan_pred.regu_list,
				   loose_end);
	  pg_cnt +=
	    qexec_clear_regu_list (xasl_p, p->s_id.s.hsid.rest_regu_list,
				   loose_end);
	  hsidp = &p->s_id.s.hsid;
	  if (loose_end == false && hsidp->caches_inited)
	    {
	      heap_attrinfo_end (thread_p, hsidp->pred_attrs.attr_cache);
	      heap_attrinfo_end (thread_p, hsidp->rest_attrs.attr_cache);
	      hsidp->caches_inited = false;
	    }
	  break;

	case S_INDX_SCAN:
	  pg_cnt +=
	    qexec_clear_regu_list (xasl_p, p->s_id.s.isid.key_pred.regu_list,
				   loose_end);
	  pg_cnt +=
	    qexec_clear_regu_list (xasl_p, p->s_id.s.isid.scan_pred.regu_list,
				   loose_end);
	  pg_cnt +=
	    qexec_clear_regu_list (xasl_p, p->s_id.s.isid.rest_regu_list,
				   loose_end);

	  pg_cnt +=
	    qexec_clear_regu_list (xasl_p, p->s_id.s.isid.pk_next_regu_list,
				   loose_end);

	  if (p->s_id.s.isid.indx_cov.regu_val_list != NULL)
	    {
	      pg_cnt +=
		qexec_clear_regu_list (xasl_p,
				       p->s_id.s.isid.indx_cov.regu_val_list,
				       loose_end);
	    }

	  if (p->s_id.s.isid.indx_cov.output_val_list != NULL)
	    {
	      pg_cnt +=
		qexec_clear_regu_list (xasl_p,
				       p->s_id.s.isid.
				       indx_cov.output_val_list->valptrp,
				       loose_end);
	    }

	  isidp = &p->s_id.s.isid;
	  if (loose_end == false && isidp->caches_inited)
	    {
	      if (isidp->key_pred.regu_list)
		{
		  heap_attrinfo_end (thread_p, isidp->key_attrs.attr_cache);
		}
	      heap_attrinfo_end (thread_p, isidp->pred_attrs.attr_cache);
	      heap_attrinfo_end (thread_p, isidp->rest_attrs.attr_cache);
	      isidp->caches_inited = false;
	    }
	  break;

	case S_LIST_SCAN:
	  pg_cnt +=
	    qexec_clear_regu_list (xasl_p,
				   p->s_id.s.llsid.scan_pred.regu_list,
				   loose_end);
	  pg_cnt +=
	    qexec_clear_regu_list (xasl_p, p->s_id.s.llsid.rest_regu_list,
				   loose_end);
	  break;

	default:
#if 0				/* TODO - enable me someday */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE,
		  0);
	  assert (false);
#endif
	  break;
	}

      if (loose_end == false && p->s_id.val_list)
	{
	  qexec_clear_db_val_list (p->s_id.val_list->valp);
	}

      switch (p->type)
	{
	case TARGET_CLASS:
	  pg_cnt +=
	    qexec_clear_regu_list (xasl_p, p->s.cls_node.cls_regu_list_key,
				   loose_end);
	  pg_cnt +=
	    qexec_clear_regu_list (xasl_p, p->s.cls_node.cls_regu_list_pred,
				   loose_end);
	  pg_cnt +=
	    qexec_clear_regu_list (xasl_p, p->s.cls_node.cls_regu_list_rest,
				   loose_end);

	  pg_cnt +=
	    qexec_clear_regu_list (xasl_p,
				   p->s.cls_node.cls_regu_list_pk_next,
				   loose_end);


	  if (p->access == INDEX)
	    {
	      INDX_INFO *indx_info;

	      indx_info = p->indexptr;
	      if (indx_info)
		{
		  int i, N;

		  N = indx_info->key_info.key_cnt;
		  for (i = 0; i < N; i++)
		    {
		      pg_cnt +=
			qexec_clear_regu_var (xasl_p,
					      indx_info->
					      key_info.key_ranges[i].key1,
					      loose_end);
		      pg_cnt +=
			qexec_clear_regu_var (xasl_p,
					      indx_info->
					      key_info.key_ranges[i].key2,
					      loose_end);
		    }
		  pg_cnt +=
		    qexec_clear_regu_var (xasl_p,
					  indx_info->key_info.key_limit_l,
					  loose_end);
		  pg_cnt +=
		    qexec_clear_regu_var (xasl_p,
					  indx_info->key_info.key_limit_u,
					  loose_end);
		}
	    }
	  break;

	case TARGET_LIST:
	  pg_cnt +=
	    qexec_clear_regu_list (xasl_p, p->s.list_node.list_regu_list_pred,
				   loose_end);
	  pg_cnt +=
	    qexec_clear_regu_list (xasl_p, p->s.list_node.list_regu_list_rest,
				   loose_end);
	  break;

	default:
	  assert (false);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE,
		  0);
	  break;
	}
    }

  return pg_cnt;
}

/*
 * qexec_clear_agg_list () - clear the db_values in the agg list
 *   return:
 *   xasl_p(in) :
 *   list(in)   :
 *   final(in)  :
 */
static int
qexec_clear_agg_list (XASL_NODE * xasl_p, AGGREGATE_TYPE * list,
		      int loose_end)
{
  AGGREGATE_TYPE *p;
  int pg_cnt;

  pg_cnt = 0;
  for (p = list; p; p = p->next)
    {
      pr_clear_value (p->accumulator.value);
      pr_clear_value (p->accumulator.value2);
      pg_cnt +=
	qexec_clear_regu_var (xasl_p, &p->group_concat_sep, loose_end);
      pg_cnt += qexec_clear_regu_var (xasl_p, &p->operand, loose_end);
    }

  return pg_cnt;
}

/*
 * qexec_clear_xasl () -
 *   return: int
 *   xasl(in)   : XASL Tree procedure block
 *   final(in)  : true iff DB_VALUES, etc should be whacked
 *                (i.e., if this XASL tree will ***NEVER*** be used again)
 *
 * Note: Destroy all the list files (temporary or result list files)
 * created during interpretation of XASL Tree procedure block
 * and return the number of total pages deallocated.
 */
int
qexec_clear_xasl (THREAD_ENTRY * thread_p, XASL_NODE * xasl, bool loose_end)
{
  int pg_cnt;
  int query_save_state;

  pg_cnt = 0;
  if (xasl == NULL)
    {
      return pg_cnt;
    }

  /*
   ** We set this because in some M paths (e.g. when a driver crashes)
   ** the function qexec_clear_xasl() can be called recursively. By setting
   ** the query_in_progress flag, we prevent qmgr_clear_trans_wakeup() from
   ** clearing the xasl structure; thus preventing a core at the
   ** primary calling level.
   */
  query_save_state = xasl->query_in_progress;

  xasl->query_in_progress = true;

  if (loose_end == false)
    {
      /* clear the head node */
      pg_cnt += qexec_clear_xasl_head (thread_p, xasl);
    }

  /* clear the body node */
  if (xasl->aptr_list)
    {
      pg_cnt += qexec_clear_xasl (thread_p, xasl->aptr_list, loose_end);
    }
  if (xasl->dptr_list)
    {
      pg_cnt += qexec_clear_xasl (thread_p, xasl->dptr_list, loose_end);
    }
  if (xasl->scan_ptr)
    {
      pg_cnt += qexec_clear_xasl (thread_p, xasl->scan_ptr, loose_end);
    }

  /* clear the db_values in the tree */
  if (xasl->outptr_list)
    {
      pg_cnt +=
	qexec_clear_regu_list (xasl, xasl->outptr_list->valptrp, loose_end);
    }
  pg_cnt +=
    qexec_clear_access_spec_list (xasl, thread_p, xasl->spec_list, loose_end);

  if (loose_end == false)
    {
      if (xasl->val_list)
	{
	  qexec_clear_db_val_list (xasl->val_list->valp);
	}
    }

  pg_cnt += qexec_clear_pred (xasl, xasl->after_join_pred, loose_end);
  pg_cnt += qexec_clear_pred (xasl, xasl->if_pred, loose_end);
  if (loose_end == false && xasl->instnum_val)
    {
      pr_clear_value (xasl->instnum_val);
    }
  pg_cnt += qexec_clear_pred (xasl, xasl->instnum_pred, loose_end);
  if (loose_end == false && xasl->ordbynum_val)
    {
      pr_clear_value (xasl->ordbynum_val);
    }
  pg_cnt += qexec_clear_pred (xasl, xasl->ordbynum_pred, loose_end);

  if (xasl->orderby_limit)
    {
      pg_cnt += qexec_clear_regu_var (xasl, xasl->orderby_limit, loose_end);
    }

  if (xasl->limit_row_count)
    {
      pg_cnt += qexec_clear_regu_var (xasl, xasl->limit_row_count, loose_end);
    }

  if (loose_end == false && xasl->topn_items != NULL)
    {
      int i;
      BINARY_HEAP *heap;

      heap = xasl->topn_items->heap;
      for (i = 0; i < heap->element_count; i++)
	{
	  qexec_clear_topn_tuple (thread_p, heap->members[i],
				  xasl->topn_items->values_count);
	}

      if (heap != NULL)
	{
	  bh_destroy (thread_p, heap);
	}

      if (xasl->topn_items->tuples != NULL)
	{
	  free_and_init (xasl->topn_items->tuples);
	}

      free_and_init (xasl->topn_items);
    }

  switch (xasl->type)
    {

    case BUILDLIST_PROC:
      {
	BUILDLIST_PROC_NODE *buildlist = &xasl->proc.buildlist;

	if (buildlist->eptr_list)
	  {
	    pg_cnt +=
	      qexec_clear_xasl (thread_p, buildlist->eptr_list, loose_end);
	  }

	if (loose_end == false)
	  {
	    if (xasl->curr_spec)
	      {
		scan_end_scan (thread_p, &xasl->curr_spec->s_id);
		scan_close_scan (thread_p, &xasl->curr_spec->s_id);
	      }
	    if (!EHID_IS_NULL (&(buildlist->upd_del_ehid)))
	      {
		qexec_destroy_upddel_ehash_files (thread_p, xasl);
	      }
	  }
	if (buildlist->g_outptr_list)
	  {
	    pg_cnt +=
	      qexec_clear_regu_list (xasl,
				     buildlist->g_outptr_list->valptrp,
				     loose_end);
	  }
	pg_cnt +=
	  qexec_clear_regu_list (xasl, buildlist->g_regu_list, loose_end);
	if (loose_end == false && buildlist->g_val_list)
	  {
	    qexec_clear_db_val_list (buildlist->g_val_list->valp);
	  }
	pg_cnt +=
	  qexec_clear_agg_list (xasl, buildlist->g_agg_list, loose_end);
	pg_cnt +=
	  qexec_clear_pred (xasl, buildlist->g_having_pred, loose_end);
	pg_cnt +=
	  qexec_clear_pred (xasl, buildlist->g_grbynum_pred, loose_end);
	if (loose_end == false && buildlist->g_grbynum_val)
	  {
	    pr_clear_value (buildlist->g_grbynum_val);
	  }
      }
      break;

    case BUILDVALUE_PROC:
      {
	BUILDVALUE_PROC_NODE *buildvalue = &xasl->proc.buildvalue;

	if (loose_end == false)
	  {
	    if (xasl->curr_spec)
	      {
		scan_end_scan (thread_p, &xasl->curr_spec->s_id);
		scan_close_scan (thread_p, &xasl->curr_spec->s_id);
	      }
	  }

	pg_cnt +=
	  qexec_clear_agg_list (xasl, buildvalue->agg_list, loose_end);
	pg_cnt +=
	  qexec_clear_arith_list (xasl, buildvalue->outarith_list, loose_end);
	pg_cnt += qexec_clear_pred (xasl, buildvalue->having_pred, loose_end);
	if (loose_end == false && buildvalue->grbynum_val)
	  {
	    pr_clear_value (buildvalue->grbynum_val);
	  }
      }
      break;

    case SCAN_PROC:
      if (loose_end == false && xasl->curr_spec)
	{
	  scan_end_scan (thread_p, &xasl->curr_spec->s_id);
	  scan_close_scan (thread_p, &xasl->curr_spec->s_id);
	}
      break;

    case INSERT_PROC:
      if (xasl->proc.insert.odku != NULL)
	{
	  int i;
	  UPDATE_ASSIGNMENT *assignment = NULL;
	  for (i = 0; i < xasl->proc.insert.odku->no_assigns; i++)
	    {
	      assignment = &xasl->proc.insert.odku->assignments[i];
	      if (assignment->regu_var != NULL)
		{
		  pg_cnt +=
		    qexec_clear_regu_var (xasl, assignment->regu_var,
					  loose_end);
		}
	    }
	}

      if (xasl->proc.insert.valptr_lists != NULL
	  && xasl->proc.insert.no_val_lists > 0)
	{
	  int i;
	  VALPTR_LIST *valptr_list = NULL;
	  REGU_VARIABLE_LIST regu_list = NULL;

	  for (i = 0; i < xasl->proc.insert.no_val_lists; i++)
	    {
	      valptr_list = xasl->proc.insert.valptr_lists[i];
	      for (regu_list = valptr_list->valptrp; regu_list != NULL;
		   regu_list = regu_list->next)
		{
		  pg_cnt +=
		    qexec_clear_regu_var (xasl, &regu_list->value, loose_end);
		}
	    }
	}
      break;

    default:
      break;
    }				/* switch */

  /* Note: Here reset the current pointer to access specification nodes.
   *       This is needed beause this XASL tree may be used again if
   *       this thread is suspended and restarted.
   */
  xasl->curr_spec = NULL;

  /* clear the next xasl node */

  if (xasl->next)
    {
      pg_cnt += qexec_clear_xasl (thread_p, xasl->next, loose_end);
    }

  xasl->query_in_progress = query_save_state;

  return pg_cnt;
}

/*
 * qexec_clear_head_lists () -
 *   return:
 *   xasl_list(in)      : List of XASL procedure blocks
 *
 * Note: Traverse through the given list of XASL procedure blocks and
 * clean/destroy results generated by interpretation of these
 * blocks such as list files generated.
 */
static void
qexec_clear_head_lists (THREAD_ENTRY * thread_p, XASL_NODE * xasl_list)
{
  XASL_NODE *xasl;

  for (xasl = xasl_list; xasl != NULL; xasl = xasl->next)
    {
      if (XASL_IS_FLAGED (xasl, XASL_ZERO_CORR_LEVEL))
	{
	  /* skip out zero correlation-level uncorrelated subquery */
	  continue;
	}
      /* clear XASL head node */
      (void) qexec_clear_xasl_head (thread_p, xasl);
    }
}

/*
 * qexec_clear_scan_all_lists () -
 *   return:
 *   xasl_list(in)      :
 */
static void
qexec_clear_scan_all_lists (THREAD_ENTRY * thread_p, XASL_NODE * xasl_list)
{
  XASL_NODE *xasl;

  for (xasl = xasl_list; xasl != NULL; xasl = xasl->scan_ptr)
    {
      if (xasl->dptr_list)
	{
	  qexec_clear_head_lists (thread_p, xasl->dptr_list);
	}
    }
}

/*
 * qexec_clear_all_lists () -
 *   return:
 *   xasl_list(in)      :
 */
static void
qexec_clear_all_lists (THREAD_ENTRY * thread_p, XASL_NODE * xasl_list)
{
  XASL_NODE *xasl;

  for (xasl = xasl_list; xasl != NULL; xasl = xasl->next)
    {
      /* Note: Dptr lists are only procedure blocks (other than aptr_list)
       * which can produce a LIST FILE. Therefore, we are trying to clear
       * all the dptr_list result LIST FILES in the XASL tree per iteration.
       */
      if (xasl->dptr_list)
	{
	  qexec_clear_head_lists (thread_p, xasl->dptr_list);
	}

      if (xasl->scan_ptr)
	{
	  qexec_clear_scan_all_lists (thread_p, xasl->scan_ptr);
	}
    }
}

/*
 * qexec_get_xasl_list_id () -
 *   return: QFILE_LIST_ID *, or NULL
 *   xasl(in)   : XASL Tree procedure block
 *
 * Note: Extract the list file identifier from the head node of the
 * specified XASL tree procedure block. This represents the
 * result of the interpretation of the block.
 */
QFILE_LIST_ID *
qexec_get_xasl_list_id (XASL_NODE * xasl)
{
  QFILE_LIST_ID *list_id = (QFILE_LIST_ID *) NULL;
  VAL_LIST *single_tuple;
  QPROC_DB_VALUE_LIST value_list;
  int i;

  if (xasl->list_id)
    {
      /* allocate region for list file identifier */
      list_id = (QFILE_LIST_ID *) malloc (sizeof (QFILE_LIST_ID));
      if (list_id == NULL)
	{
	  return (QFILE_LIST_ID *) NULL;
	}

      QFILE_CLEAR_LIST_ID (list_id);
      if (qfile_copy_list_id (list_id, xasl->list_id, true) != NO_ERROR)
	{
	  QFILE_FREE_AND_INIT_LIST_ID (list_id);
	  return (QFILE_LIST_ID *) NULL;
	}
      qfile_clear_list_id (xasl->list_id);
    }

  single_tuple = xasl->single_tuple;
  if (single_tuple)
    {
      /* clear result value */
      for (value_list = single_tuple->valp, i = 0;
	   i < single_tuple->val_cnt; value_list = value_list->next, i++)
	{
	  pr_clear_value (value_list->val);
	}
    }

  return list_id;
}

/*
 * qexec_eval_ordbynum_pred () -
 *   return:
 *   ordby_info(in)     :
 */
static DB_LOGICAL
qexec_eval_ordbynum_pred (THREAD_ENTRY * thread_p, ORDBYNUM_INFO * ordby_info)
{
  DB_LOGICAL ev_res;

  if (ordby_info->ordbynum_val)
    {
      /* Increment the value of orderby_num() used for "order by" numbering */
      ordby_info->ordbynum_val->data.bigint++;
    }

  if (ordby_info->ordbynum_pred)
    {
      /*
       * Evaluate the predicate.
       * Rye does not currently support such predicates in WHERE condition
       * lists but might support them in future versions (see the usage of
       * MSGCAT_SEMANTIC_ORDERBYNUM_SELECT_LIST_ERR). Currently such
       * predicates must only be used with the "order by [...] for [...]"
       * syntax.
       * Sample query:
       *   select * from participant
       *   order by silver for orderby_num() between 1 and 10;
       * Invalid query (at present):
       *   select * from participant where orderby_num() between 1 and 10
       *   order by silver;
       */
      ev_res =
	eval_pred (thread_p, ordby_info->ordbynum_pred,
		   &ordby_info->xasl_state->vd, NULL);
      switch (ev_res)
	{
	case V_FALSE:
	  if (ordby_info->ordbynum_flag & XASL_ORDBYNUM_FLAG_SCAN_CHECK)
	    {
	      /* If in the "scan check" mode then signal that the scan should
	       * stop, as there will be no more tuples to return.
	       */
	      ordby_info->ordbynum_flag |= XASL_ORDBYNUM_FLAG_SCAN_STOP;
	    }
	  break;
	case V_TRUE:
	  /* The predicate evaluated as true. It is possible that we are in
	   * the "continue scan" mode, indicated by
	   * XASL_ORDBYNUM_FLAG_SCAN_CONTINUE. This mode means we should
	   * continue evaluating the predicate for all the other tuples
	   * because the predicate is complex and we cannot predict its vale.
	   * If the predicate is very simple we can predict that it will be
	   * true for a single range of tuples, like the range in the
	   * following example:
	   * Tuple1 Tuple2 Tuple3 Tuple4 Tuple5 Tuple6 Tuple7 Tuple8 Tuple9
	   * False  False  False  True   True   True   True   False  False
	   * When we find the first true predicate we set the "scan check"
	   * mode.
	   */
	  if (!(ordby_info->ordbynum_flag & XASL_ORDBYNUM_FLAG_SCAN_CONTINUE)
	      && !(ordby_info->ordbynum_flag & XASL_ORDBYNUM_FLAG_SCAN_CHECK))
	    {
	      ordby_info->ordbynum_flag |= XASL_ORDBYNUM_FLAG_SCAN_CHECK;
	    }
	  break;
	case V_ERROR:
	  break;
	case V_UNKNOWN:
	default:
	  break;
	}
    }
  else
    {
      /* No predicate was given so no filtering is required. */
      ev_res = V_TRUE;
    }

  return ev_res;
}

/*
 * qexec_ordby_put_next () -
 *   return:
 *   recdes(in) :
 *   arg(in)    :
 */
static int
qexec_ordby_put_next (THREAD_ENTRY * thread_p, const RECDES * recdes,
		      void *arg)
{
  SORT_INFO *info;
  SORT_REC *key;
  char *data, *tvalhp;
  int tval_size;
  ORDBYNUM_INFO *ordby_info;
  PAGE_PTR page;
  VPID ovfl_vpid;
  DB_LOGICAL ev_res;
  int error;
  int i;
  VPID vpid;
  QFILE_LIST_ID *list_idp;
  QFILE_TUPLE_RECORD tplrec;

  error = NO_ERROR;

  info = (SORT_INFO *) arg;
  ordby_info = (ORDBYNUM_INFO *) info->extra_arg;

  /* Traverse next link */
  for (key = (SORT_REC *) recdes->data; key && error == NO_ERROR;
       key = key->next)
    {
      ev_res = V_TRUE;
      if (ordby_info != NULL && ordby_info->ordbynum_val)
	{
	  /* evaluate orderby_num predicates */
	  ev_res = qexec_eval_ordbynum_pred (thread_p, ordby_info);
	  if (ev_res == V_ERROR)
	    {
	      return er_errid ();
	    }

	  if (ordby_info->ordbynum_flag & XASL_ORDBYNUM_FLAG_SCAN_STOP)
	    {
	      /* reset ordbynum_val for next use */
	      DB_MAKE_BIGINT (ordby_info->ordbynum_val, 0);
	      /* setting SORT_PUT_STOP will make 'sr_in_sort()' stop processing;
	         the caller, 'qexec_gby_put_next()', returns 'gbstate->state' */
	      return SORT_PUT_STOP;
	    }
	}

      if (ordby_info != NULL && ev_res == V_TRUE)
	{
	  if (info->key_info.use_original)
	    {			/* P_sort_key */
	      /* We need to consult the original file for the bonafide tuple.
	         The SORT_REC only kept the keys that we needed so that we
	         wouldn't have to drag them around while we were sorting. */

	      list_idp = &(info->s_id->s_id->list_id);
	      vpid.pageid = key->s.original.pageid;
	      vpid.volid = key->s.original.volid;

#if 0				/* SortCache */
	      /* check if page is already fixed */
	      if (VPID_EQ (&(info->fixed_vpid), &vpid))
		{
		  /* use cached page pointer */
		  page = info->fixed_page;
		}
	      else
		{
		  /* free currently fixed page */
		  if (info->fixed_page != NULL)
		    {
		      qmgr_free_old_page_and_init (info->fixed_page,
						   list_idp->tfile_vfid);
		    }

		  /* fix page and cache fixed vpid */
		  page = qmgr_get_old_page (&vpid, list_idp->tfile_vfid);
		  if (page == NULL)
		    {
		      return er_errid ();
		    }

		  /* cache page pointer */
		  info->fixed_vpid = vpid;
		  info->fixed_page = page;
		}		/* else */
#else
	      page =
		qmgr_get_old_page (thread_p, &vpid, list_idp->tfile_vfid);
	      if (page == NULL)
		{
		  return er_errid ();
		}
#endif

	      QFILE_GET_OVERFLOW_VPID (&ovfl_vpid, page);

	      if (ovfl_vpid.pageid == NULL_PAGEID ||
		  ovfl_vpid.pageid == NULL_PAGEID_ASYNC)
		{
		  /* This is the normal case of a non-overflow tuple. We can
		     use the page image directly, since we know that the
		     tuple resides entirely on that page. */
		  data = page + key->s.original.offset;
		  /* update orderby_num() in the tuple */
		  for (i = 0; ordby_info && i < ordby_info->ordbynum_pos_cnt;
		       i++)
		    {
		      QFILE_GET_TUPLE_VALUE_HEADER_POSITION (data,
							     ordby_info->
							     ordbynum_pos[i],
							     tvalhp);
		      (void) qdata_copy_db_value_to_tuple_value (ordby_info->
								 ordbynum_val,
								 tvalhp,
								 &tval_size);
		    }
		  error =
		    qfile_add_tuple_to_list (thread_p, info->output_file,
					     data);
		}
	      else
		{
		  /* Rats; this tuple requires overflow pages. We need to
		     copy all of the pages from the input file to the output
		     file. */
		  if (ordby_info && ordby_info->ordbynum_pos_cnt > 0)
		    {
		      /* I think this way is very inefficient. */
		      tplrec.size = 0;
		      tplrec.tpl = NULL;
		      qfile_get_tuple (thread_p, page,
				       page + key->s.original.offset, &tplrec,
				       list_idp);
		      data = tplrec.tpl;
		      /* update orderby_num() in the tuple */
		      for (i = 0;
			   ordby_info && i < ordby_info->ordbynum_pos_cnt;
			   i++)
			{
			  QFILE_GET_TUPLE_VALUE_HEADER_POSITION (data,
								 ordby_info->
								 ordbynum_pos
								 [i], tvalhp);
			  (void)
			    qdata_copy_db_value_to_tuple_value (ordby_info->
								ordbynum_val,
								tvalhp,
								&tval_size);
			}
		      error =
			qfile_add_tuple_to_list (thread_p, info->output_file,
						 data);
		      free_and_init (tplrec.tpl);
		    }
		  else
		    {
		      error =
			qfile_add_overflow_tuple_to_list (thread_p,
							  info->output_file,
							  page, list_idp);
		    }
		}
#if 1				/* SortCache */
	      qmgr_free_old_page_and_init (thread_p, page,
					   list_idp->tfile_vfid);
#endif
	    }
	  else
	    {			/* A_sort_key */
	      /* We didn't record the original vpid, and we should just
	         reconstruct the original record from this sort key (rather
	         than pressure the page buffer pool by reading in the original
	         page to get the original tuple) */

	      if (qfile_generate_sort_tuple (&info->key_info, key,
					     &info->output_recdes) == NULL)
		{
		  error = ER_FAILED;
		}
	      else
		{
		  data = info->output_recdes.data;
		  /* update orderby_num() in the tuple */
		  for (i = 0; ordby_info && i < ordby_info->ordbynum_pos_cnt;
		       i++)
		    {
		      QFILE_GET_TUPLE_VALUE_HEADER_POSITION (data,
							     ordby_info->
							     ordbynum_pos[i],
							     tvalhp);
		      (void) qdata_copy_db_value_to_tuple_value (ordby_info->
								 ordbynum_val,
								 tvalhp,
								 &tval_size);
		    }
		  error =
		    qfile_add_tuple_to_list (thread_p, info->output_file,
					     data);
		}
	    }

	}			/* if (ev_res == V_TRUE) */

    }				/* for (key = (SORT_REC *) recdes->data; ...) */

  return (error == NO_ERROR) ? NO_ERROR : er_errid ();
}


/*
 * qexec_fill_sort_limit () - gets the ordbynum max and saves it to the XASL
 *   return: NO_ERROR or error code on failure
 *   thread_p(in)  :
 *   xasl(in)      :
 *   xasl_state(in):
 *   limit_ptr(in) : pointer to an integer which will store the max
 *
 *   Note: The "LIMIT 10" from a query gets translated into a pred expr
 *         like ordby_num < ?. At xasl generation we save the "?" as a
 *         regu-var, defining the maximum. The regu var is most likely
 *         to contain host variables, so we can only interpret it at
 *         runtime. This is the function's purpose: get an integer from
 *         the XASL to represent the upper bound for the sorted results.
 */
static int
qexec_fill_sort_limit (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
		       XASL_STATE * xasl_state, int *limit_ptr)
{
  DB_VALUE *dbvalp = NULL;
  TP_DOMAIN *domainp = tp_domain_resolve_default (DB_TYPE_INTEGER);
  DB_TYPE orig_type;

  if (limit_ptr == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_PARAMETER,
	      0);
      return ER_FAILED;
    }

  *limit_ptr = NO_SORT_LIMIT;

  /* If this option is disabled, keep the limit negative (NO_SORT_LIMIT). */
  if (!prm_get_bool_value (PRM_ID_USE_ORDERBY_SORT_LIMIT) || !xasl
      || !xasl->orderby_limit)
    {
      return NO_ERROR;
    }

  if (fetch_peek_dbval (thread_p, xasl->orderby_limit, &xasl_state->vd,
			NULL, NULL, &dbvalp) != NO_ERROR)
    {
      return ER_FAILED;
    }

  orig_type = DB_VALUE_DOMAIN_TYPE (dbvalp);

  if (orig_type != DB_TYPE_INTEGER)
    {
      TP_DOMAIN_STATUS status = tp_value_coerce (dbvalp, dbvalp, domainp);
      if (status == DOMAIN_OVERFLOW)
	{
	  /* The limit is too bog to fit an integer. However, since this limit
	   * is used to keep the sort run flushes small (for instance only
	   * keep the first 10 elements of each run if ORDER BY LIMIT 10 is
	   * specified), there is no conceivable way this limit would be
	   * useful if it is larger than 2.147 billion: such a large run
	   * is infeasible anyway. So if it does not fit into an integer,
	   * discard it.
	   */
	  return NO_ERROR;
	}

      if (status != DOMAIN_COMPATIBLE)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
		  pr_type_name (orig_type),
		  pr_type_name (TP_DOMAIN_TYPE (domainp)));
	  return ER_FAILED;
	}

      assert (DB_VALUE_DOMAIN_TYPE (dbvalp) == DB_TYPE_INTEGER);
    }

  *limit_ptr = DB_GET_INTEGER (dbvalp);
  if (*limit_ptr < 0)
    {
      /* If the limit is below 0, set it to 0 and still return success. */
      *limit_ptr = 0;
    }

  return NO_ERROR;
}

/*
 * qexec_orderby_distinct () -
 *   return: NO_ERROR, or ER_code
 *   xasl(in)   :
 *   option(in) : Distinct/All indication flag
 *   xasl_state(in)     : Ptr to the XASL_STATE for this tree
 *
 */
static int
qexec_orderby_distinct (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
			QUERY_OPTIONS option, XASL_STATE * xasl_state)
{
  int error = NO_ERROR;
  struct timeval start, end;
  UINT64 old_sort_pages = 0, old_sort_ioreads = 0;

  if (thread_is_on_trace (thread_p))
    {
      gettimeofday (&start, NULL);
      if (xasl->orderby_stats.orderby_filesort)
	{
	  old_sort_pages =
	    mnt_get_stats (thread_p, MNT_STATS_SORT_DATA_PAGES);
	  old_sort_ioreads =
	    mnt_get_stats (thread_p, MNT_STATS_SORT_IO_PAGES);
	}
    }

  if (xasl->topn_items != NULL)
    {
      /* already sorted, just dump tuples to list */
      error = qexec_topn_tuples_to_list_id (thread_p, xasl, xasl_state, true);
    }
  else
    {
      error = qexec_orderby_distinct_by_sorting (thread_p, xasl, option,
						 xasl_state);
    }

  if (thread_is_on_trace (thread_p))
    {
      gettimeofday (&end, NULL);
      ADD_TIMEVAL (xasl->orderby_stats.orderby_time, start, end);

      if (xasl->orderby_stats.orderby_filesort)
	{
	  xasl->orderby_stats.orderby_pages =
	    mnt_get_stats (thread_p,
			   MNT_STATS_SORT_DATA_PAGES) - old_sort_pages;
	  xasl->orderby_stats.orderby_ioreads =
	    mnt_get_stats (thread_p,
			   MNT_STATS_SORT_IO_PAGES) - old_sort_ioreads;
	}
    }

  return error;
}

/*
 * qexec_orderby_distinct_by_sorting () -
 *   return: NO_ERROR, or ER_code
 *   xasl(in)   :
 *   option(in) : Distinct/All indication flag
 *   xasl_state(in)     : Ptr to the XASL_STATE for this tree
 *
 * Note: Depending on the indicated set of sorting items and the value
 * of the distinct/all option, the given list file is sorted on
 * several columns and/or duplications are eliminated. If only
 * duplication elimination is specified and all the columns of the
 * list file contains orderable types (non-sets), first the list
 * file is sorted on all columns and then duplications are
 * eliminated on the fly, thus causing and ordered-distinct list file output.
 */
static int
qexec_orderby_distinct_by_sorting (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
				   QUERY_OPTIONS option,
				   XASL_STATE * xasl_state)
{
  QFILE_LIST_ID *list_id = xasl->list_id;
  SORT_LIST *order_list = xasl->orderby_list;
  PRED_EXPR *ordbynum_pred = xasl->ordbynum_pred;
  DB_VALUE *ordbynum_val = xasl->ordbynum_val;
  int ordbynum_flag = xasl->ordbynum_flag;
  OUTPTR_LIST *outptr_list;
  SORT_LIST *orderby_ptr, *order_ptr, *orderby_list;
  SORT_LIST *order_ptr2, temp_ord;
  bool orderby_alloc = false;
  int k, n, i, ls_flag;
  ORDBYNUM_INFO ordby_info;
  REGU_VARIABLE_LIST regu_list;
  SORT_PUT_FUNC *put_fn;
  int limit;
  int error = NO_ERROR;

  xasl->orderby_stats.orderby_filesort = true;

  if (xasl->type == BUILDLIST_PROC)
    {
      /* choose appropriate list */
      if (xasl->proc.buildlist.groupby_list != NULL)
	{
	  outptr_list = xasl->proc.buildlist.g_outptr_list;
	}
      else
	{
	  outptr_list = xasl->outptr_list;
	}
    }
  else
    {
      outptr_list = xasl->outptr_list;
    }

  if (order_list == NULL && option != Q_DISTINCT)
    {
      return NO_ERROR;
    }

  memset (&ordby_info, 0, sizeof (ORDBYNUM_INFO));

  /* sort the result list file */
  /* form the linked list of sort type items */
  if (option != Q_DISTINCT)
    {
      orderby_list = order_list;
      orderby_alloc = false;
    }
  else
    {
      /* allocate space for  sort list */
      orderby_list = qfile_allocate_sort_list (list_id->type_list.type_cnt);
      if (orderby_list == NULL)
	{
	  error = ER_FAILED;
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}

      /* form an order_by list including all list file positions */
      orderby_alloc = true;
      for (k = 0, order_ptr = orderby_list; k < list_id->type_list.type_cnt;
	   k++, order_ptr = order_ptr->next)
	{
	  /* sort with descending order if we have the use_desc hint and
	   * no order by
	   */
	  if (order_list == NULL && xasl->spec_list
	      && xasl->spec_list->indexptr
	      && xasl->spec_list->indexptr->use_desc_index)
	    {
	      order_ptr->s_order = S_DESC;
	      order_ptr->s_nulls = S_NULLS_LAST;
	    }
	  else
	    {
	      order_ptr->s_order = S_ASC;
	      order_ptr->s_nulls = S_NULLS_FIRST;
	    }
	  order_ptr->pos_descr.pos_no = k;
	}			/* for */

      /* put the original order_by specifications, if any,
       * to the beginning of the order_by list.
       */
      for (orderby_ptr = order_list, order_ptr = orderby_list;
	   orderby_ptr != NULL;
	   orderby_ptr = orderby_ptr->next, order_ptr = order_ptr->next)
	{
	  /* save original content */
	  temp_ord.s_order = order_ptr->s_order;
	  temp_ord.s_nulls = order_ptr->s_nulls;
	  temp_ord.pos_descr = order_ptr->pos_descr;

	  /* put original order_by node */
	  order_ptr->s_order = orderby_ptr->s_order;
	  order_ptr->s_nulls = orderby_ptr->s_nulls;
	  order_ptr->pos_descr = orderby_ptr->pos_descr;

	  /* put temporary node into old order_by node position */
	  for (order_ptr2 = order_ptr->next; order_ptr2 != NULL;
	       order_ptr2 = order_ptr2->next)
	    {
	      if (orderby_ptr->pos_descr.pos_no ==
		  order_ptr2->pos_descr.pos_no)
		{
		  order_ptr2->s_order = temp_ord.s_order;
		  order_ptr2->s_nulls = temp_ord.s_nulls;
		  order_ptr2->pos_descr = temp_ord.pos_descr;
		  break;	/* immediately exit inner loop */
		}
	    }
	}

    }				/* if-else */

  /* sort the list file */
  ordby_info.ordbynum_pos_cnt = 0;
  ordby_info.ordbynum_pos = ordby_info.reserved;
  if (outptr_list)
    {
      for (n = 0, regu_list = outptr_list->valptrp; regu_list;
	   regu_list = regu_list->next)
	{
	  if (regu_list->value.type == TYPE_ORDERBY_NUM)
	    {
	      n++;
	    }
	}
      ordby_info.ordbynum_pos_cnt = n;
      if (n > 2)
	{
	  ordby_info.ordbynum_pos = (int *) malloc (sizeof (int) * n);
	  if (ordby_info.ordbynum_pos == NULL)
	    {
	      error = ER_FAILED;
	      QEXEC_GOTO_EXIT_ON_ERROR;
	    }
	}

      for (n = 0, i = 0, regu_list = outptr_list->valptrp; regu_list;
	   regu_list = regu_list->next, i++)
	{
	  if (regu_list->value.type == TYPE_ORDERBY_NUM)
	    {
	      ordby_info.ordbynum_pos[n++] = i;
	    }
	}
    }

  ordby_info.xasl_state = xasl_state;
  ordby_info.ordbynum_pred = ordbynum_pred;
  ordby_info.ordbynum_val = ordbynum_val;
  ordby_info.ordbynum_flag = ordbynum_flag;
  put_fn = (ordbynum_val) ? &qexec_ordby_put_next : NULL;

  if (ordbynum_val == NULL && orderby_list
      && qfile_is_sort_list_covered (list_id->sort_list, orderby_list) == true
      && option != Q_DISTINCT)
    {
      /* no need to sort here */
    }
  else
    {
      ls_flag = ((option == Q_DISTINCT) ? QFILE_FLAG_DISTINCT
		 : QFILE_FLAG_ALL);
      /* If this is the top most XASL, then the list file to be open will be
         the last result file.
         (Note that 'order by' is the last processing.) */
      if (XASL_IS_FLAGED (xasl, XASL_TOP_MOST_XASL)
	  && XASL_IS_FLAGED (xasl, XASL_TO_BE_CACHED))
	{
	  QFILE_SET_FLAG (ls_flag, QFILE_FLAG_RESULT_FILE);
	}

      limit = NO_SORT_LIMIT;
      if (qexec_fill_sort_limit (thread_p, xasl, xasl_state, &limit)
	  != NO_ERROR)
	{
	  error = ER_FAILED;
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}

      list_id = qfile_sort_list_with_func (thread_p, list_id, orderby_list,
					   option, ls_flag, NULL, put_fn,
					   NULL, &ordby_info, limit, true);
      if (list_id == NULL)
	{
	  error = ER_FAILED;
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}
    }

exit_on_error:
  if (ordby_info.ordbynum_pos
      && ordby_info.ordbynum_pos != ordby_info.reserved)
    {
      free_and_init (ordby_info.ordbynum_pos);
    }

  /* free temporarily allocated areas */
  if (orderby_alloc == true)
    {
      qfile_free_sort_list (orderby_list);
    }

  return error;
}

/*
 * qexec_eval_grbynum_pred () -
 *   return:
 *   gbstate(in)        :
 */
static DB_LOGICAL
qexec_eval_grbynum_pred (THREAD_ENTRY * thread_p, GROUPBY_STATE * gbstate)
{
  DB_LOGICAL ev_res;

  /* groupby numbering; increase the value of groupby_num() by 1 */
  if (gbstate->grbynum_val)
    {
      gbstate->grbynum_val->data.bigint++;
    }

  if (gbstate->grbynum_pred)
    {
      /* evaluate predicate */
      ev_res = eval_pred (thread_p, gbstate->grbynum_pred,
			  &gbstate->xasl_state->vd, NULL);
      switch (ev_res)
	{
	case V_FALSE:
	  /* evaluation is false; if check flag was set, stop scan */
	  if (gbstate->grbynum_flag & XASL_G_GRBYNUM_FLAG_SCAN_CHECK)
	    {
	      gbstate->grbynum_flag |= XASL_G_GRBYNUM_FLAG_SCAN_STOP;
	    }
	  break;

	case V_TRUE:
	  /* evaluation is true; if not continue scan mode, set scan check flag */
	  if (!(gbstate->grbynum_flag & XASL_G_GRBYNUM_FLAG_SCAN_CONTINUE)
	      && !(gbstate->grbynum_flag & XASL_G_GRBYNUM_FLAG_SCAN_CHECK))
	    {
	      gbstate->grbynum_flag |= XASL_G_GRBYNUM_FLAG_SCAN_CHECK;
	    }
	  break;

	case V_ERROR:
	  break;

	case V_UNKNOWN:
	default:
	  break;
	}
    }
  else
    {
      /* no predicate; always true */
      ev_res = V_TRUE;
    }

  return ev_res;
}

/*
 * qexec_initialize_groupby_state () -
 *   return:
 *   gbstate(in)        :
 *   groupby_list(in)   : Group_by sorting list specification
 *   having_pred(in)    : Having predicate expression
 *   grbynum_pred(in)   :
 *   grbynum_val(in)    :
 *   grbynum_flag(in)   :
 *   eptr_list(in)      : Having subquery list
 *   g_agg_list(in)     : Group_by aggregation list
 *   g_regu_list(in)    : Regulator Variable List
 *   g_val_list(in)     : Value List
 *   g_outptr_list(in)  : Output pointer list
 *   g_with_rollup(in)	: Has WITH ROLLUP clause
 *   xasl_state(in)     : XASL tree state information
 *   type_list(in)      :
 *   tplrec(out) 	: Tuple record descriptor to store result tuples
 */
static GROUPBY_STATE *
qexec_initialize_groupby_state (GROUPBY_STATE * gbstate,
				SORT_LIST * groupby_list,
				PRED_EXPR * having_pred,
				PRED_EXPR * grbynum_pred,
				DB_VALUE * grbynum_val, int grbynum_flag,
				XASL_NODE * eptr_list,
				AGGREGATE_TYPE * g_agg_list,
				REGU_VARIABLE_LIST g_regu_list,
				VAL_LIST * g_val_list,
				OUTPTR_LIST * g_outptr_list,
				int with_rollup,
				XASL_NODE * xasl,
				XASL_STATE * xasl_state,
				QFILE_TUPLE_VALUE_TYPE_LIST * type_list,
				QFILE_TUPLE_RECORD * tplrec)
{
  assert (groupby_list != NULL);

  gbstate->state = NO_ERROR;

  gbstate->input_scan = NULL;
#if 0				/* SortCache */
  VPID_SET_NULL (&(gbstate->fixed_vpid));
  gbstate->fixed_page = NULL;
#endif
  gbstate->output_file = NULL;

  gbstate->having_pred = having_pred;
  gbstate->grbynum_pred = grbynum_pred;
  gbstate->grbynum_val = grbynum_val;
  gbstate->grbynum_flag = grbynum_flag;
  gbstate->eptr_list = eptr_list;
  gbstate->g_output_agg_list = g_agg_list;
  gbstate->g_regu_list = g_regu_list;
  gbstate->g_val_list = g_val_list;
  gbstate->g_outptr_list = g_outptr_list;
  gbstate->xasl = xasl;
  gbstate->xasl_state = xasl_state;

  gbstate->current_key.area_size = 0;
  gbstate->current_key.length = 0;
  gbstate->current_key.type = 0;	/* Unused */
  gbstate->current_key.data = NULL;
  gbstate->gby_rec.area_size = 0;
  gbstate->gby_rec.length = 0;
  gbstate->gby_rec.type = 0;	/* Unused */
  gbstate->gby_rec.data = NULL;
  gbstate->output_tplrec = NULL;
  gbstate->input_tpl.size = 0;
  gbstate->input_tpl.tpl = 0;
  gbstate->input_recs = 0;

  if (qfile_initialize_sort_key_info (&gbstate->key_info, groupby_list,
				      type_list) == NULL)
    {
      return NULL;
    }

  gbstate->current_key.data = (char *) malloc (DB_PAGESIZE);
  if (gbstate->current_key.data == NULL)
    {
      return NULL;
    }
  gbstate->current_key.area_size = DB_PAGESIZE;

  gbstate->output_tplrec = tplrec;

  gbstate->with_rollup = with_rollup;

  /* initialize aggregate lists */
  if (qexec_gby_init_group_dim (gbstate) != NO_ERROR)
    {
      return NULL;
    }

  return gbstate;
}

/*
 * qexec_clear_groupby_state () -
 *   return:
 *   gbstate(in)        :
 */
static void
qexec_clear_groupby_state (THREAD_ENTRY * thread_p, GROUPBY_STATE * gbstate)
{
  int i;
#if 0				/* SortCache */
  QFILE_LIST_ID *list_idp;
#endif

  for (i = 0; i < gbstate->g_dim_levels; i++)
    {
      QEXEC_CLEAR_AGG_LIST_VALUE (gbstate->g_dim[i].d_agg_list);
    }
  if (gbstate->eptr_list)
    {
      qexec_clear_head_lists (thread_p, gbstate->eptr_list);
    }
  if (gbstate->current_key.data)
    {
      free_and_init (gbstate->current_key.data);
      gbstate->current_key.area_size = 0;
    }
  if (gbstate->gby_rec.data)
    {
      free_and_init (gbstate->gby_rec.data);
      gbstate->gby_rec.area_size = 0;
    }
  gbstate->output_tplrec = NULL;
  /*
   * Don't cleanup gbstate->input_tpl; the memory it points to was
   * managed by the listfile manager (via input_scan), and it's not
   * ours to free.
   */
#if 0				/* SortCache */
  list_idp = &(gbstate->input_scan->list_id);
  /* free currently fixed page */
  if (gbstate->fixed_page != NULL)
    {
      qmgr_free_old_page_and_init (gbstate->fixed_page, list_idp->tfile_vfid);
    }
#endif

  qfile_clear_sort_key_info (&gbstate->key_info);
  if (gbstate->input_scan)
    {
      qfile_close_scan (thread_p, gbstate->input_scan);
      gbstate->input_scan = NULL;
    }
  if (gbstate->output_file)
    {
      qfile_close_list (thread_p, gbstate->output_file);
      QFILE_FREE_AND_INIT_LIST_ID (gbstate->output_file);
    }

  /* destroy aggregates lists */
  qexec_gby_clear_group_dim (thread_p, gbstate);
}

/*
 * qexec_gby_agg_tuple () -
 *   return:
 *   gbstate(in)        :
 *   tpl(in)    :
 *   peek(in)   :
 */
static void
qexec_gby_agg_tuple (THREAD_ENTRY * thread_p, GROUPBY_STATE * gbstate,
		     QFILE_TUPLE tpl, int peek)
{
  XASL_STATE *xasl_state = gbstate->xasl_state;
  QFILE_TUPLE_VALUE_TYPE_LIST *tpl_type_list = NULL;
  int i;

  if (gbstate->state != NO_ERROR)
    {
      return;
    }

  tpl_type_list = &(gbstate->input_scan->list_id.type_list);
  assert (tpl_type_list != NULL);

  /*
   * Read the incoming tuple into DB_VALUEs and do the necessary
   * aggregation...
   */
  if (fetch_val_list (thread_p, gbstate->g_regu_list,
		      &gbstate->xasl_state->vd, NULL,
		      tpl, tpl_type_list, peek) != NO_ERROR)
    {
      QEXEC_GOTO_EXIT_ON_ERROR;
    }

  /* evaluate aggregates lists */
  for (i = 0; i < gbstate->g_dim_levels; i++)
    {
      assert (gbstate->g_dim[i].d_flag != GROUPBY_DIM_FLAG_NONE);

      if (qdata_evaluate_aggregate_list (thread_p,
					 gbstate->g_dim[i].d_agg_list,
					 &gbstate->xasl_state->vd) !=
	  NO_ERROR)
	{
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}
    }

wrapup:
  return;

exit_on_error:
  gbstate->state = er_errid ();
  goto wrapup;
}

/*
 * qexec_gby_get_next () -
 *   return:
 *   recdes(in) :
 *   arg(in)    :
 */
static SORT_STATUS
qexec_gby_get_next (THREAD_ENTRY * thread_p, RECDES * recdes, void *arg)
{
  GROUPBY_STATE *gbstate;

  gbstate = (GROUPBY_STATE *) arg;

  return qfile_make_sort_key (thread_p, &gbstate->key_info,
			      recdes, gbstate->input_scan,
			      &gbstate->input_tpl);
}

/*
 * qexec_gby_put_next () -
 *   return:
 *   recdes(in) :
 *   arg(in)    :
 */
static int
qexec_gby_put_next (THREAD_ENTRY * thread_p, const RECDES * recdes, void *arg)
{
  GROUPBY_STATE *info;
  SORT_REC *key;
  char *data;
  PAGE_PTR page;
  VPID vpid;
  int peek, rollup_level;
  QFILE_LIST_ID *list_idp;

  QFILE_TUPLE_RECORD dummy;
  int status;

  info = (GROUPBY_STATE *) arg;
  list_idp = &(info->input_scan->list_id);

  data = NULL;
  page = NULL;

  /* Traverse next link */
  for (key = (SORT_REC *) recdes->data; key; key = key->next)
    {
      if (info->state != NO_ERROR)
	{
	  goto exit_on_error;
	}

      peek = COPY;		/* default */
      if (info->key_info.use_original)
	{			/* P_sort_key */
	  /*
	   * Retrieve the original tuple.  This will be the case if the
	   * original tuple had more fields than we were sorting on.
	   */
	  vpid.pageid = key->s.original.pageid;
	  vpid.volid = key->s.original.volid;

#if 0				/* SortCache */
	  /* check if page is already fixed */
	  if (VPID_EQ (&(info->fixed_vpid), &vpid))
	    {
	      /* use cached page pointer */
	      page = info->fixed_page;
	    }
	  else
	    {
	      /* free currently fixed page */
	      if (info->fixed_page != NULL)
		{
		  qmgr_free_old_page_and_init (info->fixed_page,
					       list_idp->tfile_vfid);
		}

	      /* fix page and cache fixed vpid */
	      page = qmgr_get_old_page (&vpid, list_idp->tfile_vfid);
	      if (page == NULL)
		{
		  goto exit_on_error;
		}

	      /* save page pointer */
	      info->fixed_vpid = vpid;
	      info->fixed_page = page;
	    }			/* else */
#else
	  page = qmgr_get_old_page (thread_p, &vpid, list_idp->tfile_vfid);
	  if (page == NULL)
	    {
	      goto exit_on_error;
	    }
#endif

	  QFILE_GET_OVERFLOW_VPID (&vpid, page);
	  data = page + key->s.original.offset;
	  if (vpid.pageid != NULL_PAGEID)
	    {
	      /*
	       * This sucks; why do we need two different structures to
	       * accomplish exactly the same goal?
	       */
	      dummy.size = info->gby_rec.area_size;
	      dummy.tpl = info->gby_rec.data;
	      status =
		qfile_get_tuple (thread_p, page, data, &dummy, list_idp);

	      if (dummy.tpl != info->gby_rec.data)
		{
		  /*
		   * DON'T FREE THE BUFFER!  qfile_get_tuple() already did
		   * that, and what you have here in gby_rec is a dangling
		   * pointer.
		   */
		  info->gby_rec.area_size = dummy.size;
		  info->gby_rec.data = dummy.tpl;
		}
	      if (status != NO_ERROR)
		{
		  goto exit_on_error;
		}

	      data = info->gby_rec.data;
	    }
	  else
	    {
	      peek = PEEK;	/* avoid unnecessary COPY */
	    }
	}
      else
	{			/* A_sort_key */
	  /*
	   * We didn't record the original vpid, and we should just
	   * reconstruct the original record from this sort key (rather
	   * than pressure the page buffer pool by reading in the original
	   * page to get the original tuple).
	   */
	  if (qfile_generate_sort_tuple (&info->key_info,
					 key, &info->gby_rec) == NULL)
	    {
	      goto exit_on_error;
	    }
	  data = info->gby_rec.data;
	}

      if (info->input_recs == 0)
	{
	  /*
	   * First record we've seen; put it out and set up the group
	   * comparison key(s).
	   */
	  qexec_gby_start_group_dim (thread_p, info, recdes);
	}
      else if ((*info->cmp_fn) (&info->current_key.data, &key,
				&info->key_info) == 0)
	{
	  /*
	   * Still in the same group; accumulate the tuple and proceed,
	   * leaving the group key the same.
	   */
	}
      else
	{
	  /*
	   * We got a new group; finalize the group we were accumulating,
	   * and start a new group using the current key as the group key.
	   */
	  rollup_level =
	    qexec_gby_finalize_group_dim (thread_p, info, recdes);
	  if (info->state == SORT_PUT_STOP)
	    {
	      goto wrapup;
	    }
	}

      /* aggregate tuple */
      qexec_gby_agg_tuple (thread_p, info, data, peek);

      info->input_recs++;

#if 1				/* SortCache */
      if (page)
	{
	  qmgr_free_old_page_and_init (thread_p, page, list_idp->tfile_vfid);
	}
#endif

    }				/* for (key = (SORT_REC *) recdes->data; ...) */

wrapup:
#if 1				/* SortCache */
  if (page)
    {
      qmgr_free_old_page_and_init (thread_p, page, list_idp->tfile_vfid);
    }
#endif

  return info->state;

exit_on_error:
  info->state = er_errid ();
  goto wrapup;
}

/*
 * qexec_groupby () -
 *   return: NO_ERROR, or ER_code
 *   xasl(in)   :
 *   xasl_state(in)     : XASL tree state information
 *   tplrec(out) : Tuple record descriptor to store result tuples
 *
 * Note: Apply the group_by clause to the given list file to group it
 * using the specified group_by parameters.
 */
static int
qexec_groupby (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
	       XASL_STATE * xasl_state, QFILE_TUPLE_RECORD * tplrec)
{
  QFILE_LIST_ID *list_id = xasl->list_id;
  BUILDLIST_PROC_NODE *buildlist = &xasl->proc.buildlist;
  GROUPBY_STATE gbstate;
  QFILE_LIST_SCAN_ID input_scan_id;
  int ls_flag = 0;
  struct timeval start, end;
  UINT64 old_sort_pages = 0, old_sort_ioreads = 0;

  if (buildlist->groupby_list == NULL)
    {
      return NO_ERROR;
    }

  if (thread_is_on_trace (thread_p))
    {
      gettimeofday (&start, NULL);
      xasl->groupby_stats.run_groupby = true;
      xasl->groupby_stats.rows = 0;
    }

  /* initialize groupby_num() value */
  if (buildlist->g_grbynum_val && DB_IS_NULL (buildlist->g_grbynum_val))
    {
      DB_MAKE_BIGINT (buildlist->g_grbynum_val, 0);
    }

  /* clear group by limit flags when skip group by is not used */
  if (buildlist->g_grbynum_flag & XASL_G_GRBYNUM_FLAG_LIMIT_LT)
    {
      buildlist->g_grbynum_flag &= ~XASL_G_GRBYNUM_FLAG_LIMIT_LT;
    }
  if (buildlist->g_grbynum_flag & XASL_G_GRBYNUM_FLAG_LIMIT_GT_LT)
    {
      buildlist->g_grbynum_flag &= ~XASL_G_GRBYNUM_FLAG_LIMIT_GT_LT;
    }

  if (qexec_initialize_groupby_state (&gbstate, buildlist->groupby_list,
				      buildlist->g_having_pred,
				      buildlist->g_grbynum_pred,
				      buildlist->g_grbynum_val,
				      buildlist->g_grbynum_flag,
				      buildlist->eptr_list,
				      buildlist->g_agg_list,
				      buildlist->g_regu_list,
				      buildlist->g_val_list,
				      buildlist->g_outptr_list,
				      buildlist->g_with_rollup,
				      xasl,
				      xasl_state,
				      &list_id->type_list, tplrec) == NULL)
    {
      QEXEC_GOTO_EXIT_ON_ERROR;
    }

  /*
   * Create a new listfile to receive the results.
   */
  {
    QFILE_TUPLE_VALUE_TYPE_LIST output_type_list;
    QFILE_LIST_ID *output_list_id;

    if (qdata_get_valptr_type_list (thread_p,
				    buildlist->g_outptr_list,
				    &output_type_list) != NO_ERROR)
      {
	QEXEC_GOTO_EXIT_ON_ERROR;
      }
    /* If it does not have 'order by'(xasl->orderby_list),
       then the list file to be open at here will be the last one.
       Otherwise, the last list file will be open at
       qexec_orderby_distinct().
       (Note that only one that can have 'group by' is BUILDLIST_PROC type.)
       And, the top most XASL is the other condition for the list file
       to be the last result file. */

    QFILE_SET_FLAG (ls_flag, QFILE_FLAG_ALL);
    if (XASL_IS_FLAGED (xasl, XASL_TOP_MOST_XASL)
	&& XASL_IS_FLAGED (xasl, XASL_TO_BE_CACHED)
	&& (xasl->orderby_list == NULL
	    || XASL_IS_FLAGED (xasl, XASL_SKIP_ORDERBY_LIST))
	&& xasl->option != Q_DISTINCT)
      {
	QFILE_SET_FLAG (ls_flag, QFILE_FLAG_RESULT_FILE);
      }

    output_list_id = qfile_open_list (thread_p, &output_type_list,
				      buildlist->after_groupby_list,
				      xasl_state->query_id, ls_flag);
    if (output_list_id == NULL)
      {
	if (output_type_list.domp)
	  {
	    free_and_init (output_type_list.domp);
	  }

	QEXEC_GOTO_EXIT_ON_ERROR;
      }

    if (output_type_list.domp)
      {
	free_and_init (output_type_list.domp);
      }

    gbstate.output_file = output_list_id;
  }

  /* check for quick finalization scenarios */
  if (list_id->tuple_cnt == 0)
    {
      qfile_destroy_list (thread_p, list_id);
      qfile_close_list (thread_p, gbstate.output_file);
      qfile_copy_list_id (list_id, gbstate.output_file, true);
      qexec_clear_groupby_state (thread_p, &gbstate);

      return NO_ERROR;
    }

  if (thread_is_on_trace (thread_p))
    {
      xasl->groupby_stats.groupby_sort = true;
      old_sort_pages = mnt_get_stats (thread_p, MNT_STATS_SORT_DATA_PAGES);
      old_sort_ioreads = mnt_get_stats (thread_p, MNT_STATS_SORT_IO_PAGES);
    }

  /*
   * Open a scan on the unsorted input file
   */
  if (qfile_open_list_scan (list_id, &input_scan_id) != NO_ERROR)
    {
      QEXEC_GOTO_EXIT_ON_ERROR;
    }
  gbstate.input_scan = &input_scan_id;

  /*
   * Now load up the sort module and set it off...
   */
  gbstate.key_info.use_original =
    (gbstate.key_info.nkeys != list_id->type_list.type_cnt);
  gbstate.cmp_fn = (gbstate.key_info.use_original == 1
		    ? &qfile_compare_partial_sort_record
		    : &qfile_compare_all_sort_record);

  if (sort_listfile (thread_p, NULL_VOLID,
		     qfile_get_estimated_pages_for_sorting (list_id,
							    &gbstate.
							    key_info),
		     &qexec_gby_get_next, &gbstate, &qexec_gby_put_next,
		     &gbstate, gbstate.cmp_fn, &gbstate.key_info, SORT_DUP,
		     NO_SORT_LIMIT) != NO_ERROR)
    {
      QEXEC_GOTO_EXIT_ON_ERROR;
    }

  /*
   * There may be one unfinished group in the output, since the sort_listfile
   * interface doesn't include a finalization function.  If so, finish
   * off that group.
   */
  if (gbstate.input_recs != 0)
    {
      qexec_gby_finalize_group_dim (thread_p, &gbstate, NULL);
    }

  /* close output file */
  qfile_close_list (thread_p, gbstate.output_file);
#if 0				/* SortCache */
  /* free currently fixed page */
  if (gbstate.fixed_page != NULL)
    {
      QFILE_LIST_ID *list_idp;

      list_idp = &(gbstate.input_scan->list_id);
      qmgr_free_old_page_and_init (gbstate.fixed_page, list_idp->tfile_vfid);
    }
#endif
  qfile_destroy_list (thread_p, list_id);
  qfile_copy_list_id (list_id, gbstate.output_file, true);
  /* qexec_clear_groupby_state() will free gbstate.output_file */

wrapup:
  {
    int result;

    /* SORT_PUT_STOP set by 'qexec_gby_finalize_group_dim ()' isn't error */
    result = (gbstate.state == NO_ERROR || gbstate.state == SORT_PUT_STOP)
      ? NO_ERROR : ER_FAILED;

    /* cleanup */
    qexec_clear_groupby_state (thread_p, &gbstate);

    if (thread_is_on_trace (thread_p))
      {
	gettimeofday (&end, NULL);
	ADD_TIMEVAL (xasl->groupby_stats.groupby_time, start, end);
	if (xasl->groupby_stats.groupby_sort == true)
	  {
	    xasl->groupby_stats.groupby_pages =
	      mnt_get_stats (thread_p,
			     MNT_STATS_SORT_DATA_PAGES) - old_sort_pages;
	    xasl->groupby_stats.groupby_ioreads =
	      mnt_get_stats (thread_p,
			     MNT_STATS_SORT_IO_PAGES) - old_sort_ioreads;
	  }
      }

    return result;
  }

exit_on_error:

  gbstate.state = er_errid ();
  if (gbstate.state == NO_ERROR || gbstate.state == SORT_PUT_STOP)
    {
      gbstate.state = ER_FAILED;
    }

  goto wrapup;
}

/*
 * Interpreter routines
 */

/*
 * qexec_open_scan () -
 *   return: NO_ERROR, or ER_code
 *   curr_spec(in)      : Access Specification Node
 *   val_list(in)       : Value list pointer
 *   vd(in)     : Value descriptor
 *   readonly_scan(in)  :
 *   fixed(in)  : Fixed scan flag
 *   s_id(out)   : Set to the scan identifier
 *
 * Note: This routine is used to open a scan on an access specification
 * node. A scan identifier is created with the given parameters.
 */
static int
qexec_open_scan (THREAD_ENTRY * thread_p, ACCESS_SPEC_TYPE * curr_spec,
		 VAL_LIST * val_list, VAL_DESCR * vd, int readonly_scan,
		 int fixed, SCAN_ID * s_id,
		 QUERY_ID query_id, bool scan_immediately_stop)
{
  SCAN_TYPE scan_type;
  INDX_INFO *indx_info;

  switch (curr_spec->type)
    {
    case TARGET_CLASS:
      if (curr_spec->access == SEQUENTIAL)
	{
	  /* open a sequential heap file scan */
	  scan_type = S_HEAP_SCAN;
	  indx_info = NULL;
	}
      else if (curr_spec->access == INDEX)
	{
	  /* open an indexed heap file scan */
	  scan_type = S_INDX_SCAN;
	  indx_info = curr_spec->indexptr;
	}
      else
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE,
		  0);
	  return ER_FAILED;
	}			/* if */
      if (scan_type == S_HEAP_SCAN)
	{
	  if (scan_open_heap_scan (thread_p, s_id,
				   readonly_scan,
				   fixed,
				   curr_spec->fetch_type,
				   val_list,
				   vd,
				   &ACCESS_SPEC_CLS_OID (curr_spec),
				   &ACCESS_SPEC_HFID (curr_spec),
				   curr_spec->s.cls_node.cls_regu_list_pred,
				   curr_spec->where_pred,
				   curr_spec->s.cls_node.cls_regu_list_rest,
				   curr_spec->s.cls_node.num_attrs_pred,
				   curr_spec->s.cls_node.attrids_pred,
				   curr_spec->s.cls_node.cache_pred,
				   curr_spec->s.cls_node.num_attrs_rest,
				   curr_spec->s.cls_node.attrids_rest,
				   curr_spec->s.cls_node.cache_rest) !=
	      NO_ERROR)
	    {
	      goto exit_on_error;
	    }
	}
      else
	{
	  if (scan_open_index_scan (thread_p, s_id,
				    readonly_scan,
				    fixed,
				    curr_spec->fetch_type,
				    val_list,
				    vd,
				    indx_info,
				    &ACCESS_SPEC_CLS_OID (curr_spec),
				    &ACCESS_SPEC_HFID (curr_spec),
				    curr_spec->s.cls_node.cls_regu_list_key,
				    curr_spec->where_key,
				    curr_spec->s.cls_node.cls_regu_list_pred,
				    curr_spec->where_pred,
				    curr_spec->s.cls_node.cls_regu_list_rest,
				    curr_spec->s.cls_node.
				    cls_regu_list_pk_next,
				    curr_spec->s.cls_node.cls_output_val_list,
				    curr_spec->s.cls_node.cls_regu_val_list,
				    curr_spec->s.cls_node.num_attrs_key,
				    curr_spec->s.cls_node.attrids_key,
				    curr_spec->s.cls_node.cache_key,
				    curr_spec->s.cls_node.num_attrs_pred,
				    curr_spec->s.cls_node.attrids_pred,
				    curr_spec->s.cls_node.cache_pred,
				    curr_spec->s.cls_node.num_attrs_rest,
				    curr_spec->s.cls_node.attrids_rest,
				    curr_spec->s.cls_node.cache_rest,
				    query_id) != NO_ERROR)
	    {
	      goto exit_on_error;
	    }
	  /* monitor */
	  mnt_stats_counter (thread_p, MNT_STATS_QUERY_ISCANS, 1);
	}
      break;

    case TARGET_LIST:
      /* open a list file scan */
      if (scan_open_list_scan (thread_p, s_id,
			       curr_spec->fetch_type,
			       val_list,
			       vd,
			       ACCESS_SPEC_LIST_ID (curr_spec),
			       curr_spec->s.list_node.list_regu_list_pred,
			       curr_spec->where_pred,
			       curr_spec->s.list_node.list_regu_list_rest) !=
	  NO_ERROR)
	{
	  goto exit_on_error;
	}
      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
      goto exit_on_error;
    }				/* switch */

  s_id->scan_immediately_stop = scan_immediately_stop;

  return NO_ERROR;

exit_on_error:

  return ER_FAILED;
}

/*
 * qexec_close_scan () -
 *   return:
 *   curr_spec(in)      : Access Specification Node
 *
 * Note: This routine is used to close the access specification node scan.
 */
static void
qexec_close_scan (THREAD_ENTRY * thread_p, ACCESS_SPEC_TYPE * curr_spec)
{
  if (curr_spec)
    {
      /* monitoring */
      switch (curr_spec->type)
	{
	case TARGET_CLASS:
	  if (curr_spec->access == SEQUENTIAL)
	    {
	      mnt_stats_counter (thread_p, MNT_STATS_QUERY_SSCANS, 1);
	    }
	  else if (curr_spec->access == INDEX)
	    {
	      mnt_stats_counter (thread_p, MNT_STATS_QUERY_ISCANS, 1);
	    }
	  break;
	case TARGET_LIST:
	  mnt_stats_counter (thread_p, MNT_STATS_QUERY_LSCANS, 1);
	  break;
	default:
	  assert (false);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE,
		  0);
	  break;
	}

      scan_close_scan (thread_p, &curr_spec->s_id);
    }
}

/*
 * qexec_end_scan () -
 *   return:
 *   curr_spec(in)      : Access Specification Node
 *
 * Note: This routine is used to end the access specification node scan.
 *
 * Note: This routine is called for ERROR CASE scan end operation.
 */
static void
qexec_end_scan (THREAD_ENTRY * thread_p, ACCESS_SPEC_TYPE * curr_spec)
{
  if (curr_spec)
    {
      scan_end_scan (thread_p, &curr_spec->s_id);
    }
}


/*
 * qexec_next_scan_block () -
 *   return: SCAN_CODE (S_SUCCESS, S_END, S_ERROR)
 *   xasl(in)   : XASL Tree pointer
 *
 * Note: This function is used to move the current access specification
 * node scan identifier for the given XASL block to the next
 * scan block. If there are no more scan blocks for the current
 * access specfication node, it moves to the next access
 * specification, if any, and starts the new scan block for that
 * node. If there are no more access specification nodes left,
 * it returns S_END.
 */
static SCAN_CODE
qexec_next_scan_block (THREAD_ENTRY * thread_p, XASL_NODE * xasl)
{
  SCAN_CODE sb_scan;

  if (xasl->curr_spec == NULL)
    {
      /* initialize scan id */
      xasl->curr_spec = xasl->spec_list;

      /* check for and skip the case of empty heap file cases */
      while (xasl->curr_spec != NULL
	     && QEXEC_EMPTY_ACCESS_SPEC_SCAN (xasl->curr_spec))
	{
	  xasl->curr_spec = xasl->curr_spec->next;
	}

      if (xasl->curr_spec == NULL)
	{
	  return S_END;
	}

      assert (xasl->curr_spec != NULL);

      if (scan_start_scan (thread_p, &xasl->curr_spec->s_id) != NO_ERROR)
	{
	  return S_ERROR;
	}
    }

  do
    {
      sb_scan = scan_next_scan_block (thread_p, &xasl->curr_spec->s_id);
      if (sb_scan == S_SUCCESS)
	{
	  return S_SUCCESS;
	}
      else if (sb_scan == S_END)
	{
	  /* close old scan */
	  scan_end_scan (thread_p, &xasl->curr_spec->s_id);

	  /* move to the following access specifications left */
	  xasl->curr_spec = xasl->curr_spec->next;

	  /* check for and skip the case of empty heap files */
	  while (xasl->curr_spec != NULL
		 && QEXEC_EMPTY_ACCESS_SPEC_SCAN (xasl->curr_spec))
	    {
	      xasl->curr_spec = xasl->curr_spec->next;
	    }

	  if (xasl->curr_spec == NULL)
	    {
	      return S_END;
	    }

	  /* initialize scan */
	  if (scan_start_scan (thread_p, &xasl->curr_spec->s_id) != NO_ERROR)
	    {
	      return S_ERROR;
	    }
	}
      else
	{
	  return S_ERROR;
	}
    }
  while (1);

}

/*
 * qexec_next_scan_block_iterations () -
 *   return: SCAN_CODE (S_SUCCESS, S_END, S_ERROR)
 *   xasl(in)   : XASL Tree pointer
 *
 */
static SCAN_CODE
qexec_next_scan_block_iterations (THREAD_ENTRY * thread_p, XASL_NODE * xasl)
{
  SCAN_CODE sb_next;
  SCAN_CODE xs_scan;
  SCAN_CODE xs_scan2;
  XASL_NODE *last_xptr;
  XASL_NODE *prev_xptr;
  XASL_NODE *xptr2, *xptr3;

  /* first find the last scan block to be moved */
  for (last_xptr = xasl; last_xptr->scan_ptr; last_xptr = last_xptr->scan_ptr)
    {
      if (!last_xptr->next_scan_block_on
	  || (last_xptr->curr_spec
	      && last_xptr->curr_spec->s_id.status == S_STARTED
	      && !last_xptr->curr_spec->s_id.qualified_block))
	{
	  break;
	}
    }

  /* move the last scan block and reset further scans */

  /* if there are no qualified items in the current scan block,
   * this scan block will make no contribution with other possible
   * scan block combinations from following classes. Thus, directly
   * move to the next scan block in this class.
   */
  if (last_xptr->curr_spec
      && last_xptr->curr_spec->s_id.status == S_STARTED
      && !last_xptr->curr_spec->s_id.qualified_block)
    {
      if ((xs_scan = qexec_next_scan_block (thread_p, last_xptr)) == S_END)
	{
	  /* close following scan procedures if they are still active */
	  for (xptr2 = last_xptr; xptr2; xptr2 = xptr2->scan_ptr)
	    {
	      if (xptr2->scan_ptr && xptr2->next_scan_block_on)
		{
		  if (xptr2->scan_ptr->curr_spec)
		    {
		      scan_end_scan (thread_p,
				     &xptr2->scan_ptr->curr_spec->s_id);
		    }
		  xptr2->scan_ptr->curr_spec = NULL;
		  xptr2->next_scan_block_on = false;
		}
	    }
	}
      else if (xs_scan == S_ERROR)
	{
	  return S_ERROR;
	}
    }
  else if ((xs_scan = qexec_next_scan_block (thread_p, last_xptr)) ==
	   S_SUCCESS)
    {				/* reset all the futher scans */
      for (xptr2 = last_xptr; xptr2; xptr2 = xptr2->scan_ptr)
	{
	  if (xptr2->scan_ptr)
	    {
	      sb_next = qexec_next_scan_block (thread_p, xptr2->scan_ptr);
	      if (sb_next == S_SUCCESS)
		{
		  xptr2->next_scan_block_on = true;
		}
	      else if (sb_next == S_END)
		{
		  /* close all preceding scan procedures and return */
		  for (xptr3 = xasl; xptr3 && xptr3 != xptr2->scan_ptr;
		       xptr3 = xptr3->scan_ptr)
		    {
		      if (xptr3->curr_spec)
			{
			  scan_end_scan (thread_p, &xptr3->curr_spec->s_id);
			}
		      xptr3->curr_spec = NULL;
		      xptr3->next_scan_block_on = false;
		    }
		  return S_END;
		}
	      else
		{
		  return S_ERROR;
		}
	    }
	}
    }
  else if (xs_scan == S_ERROR)
    {
      return S_ERROR;
    }

  /* now move backwards, resetting all the previous scans */
  while (last_xptr != xasl)
    {

      /* find the previous to last xptr */
      for (prev_xptr = xasl; prev_xptr->scan_ptr != last_xptr;
	   prev_xptr = prev_xptr->scan_ptr)
	;

      /* set previous scan according to the last scan status */
      if (last_xptr->curr_spec == NULL)
	{			/* last scan ended */
	  prev_xptr->next_scan_block_on = false;

	  /* move the scan block of the previous scan */
	  xs_scan2 = qexec_next_scan_block (thread_p, prev_xptr);
	  if (xs_scan2 == S_SUCCESS)
	    {
	      /* move all the further scan blocks */
	      for (xptr2 = prev_xptr; xptr2; xptr2 = xptr2->scan_ptr)
		{
		  if (xptr2->scan_ptr)
		    {
		      sb_next = qexec_next_scan_block (thread_p,
						       xptr2->scan_ptr);
		      if (sb_next == S_SUCCESS)
			{
			  xptr2->next_scan_block_on = true;
			}
		      else if (sb_next == S_END)
			{
			  /* close all preceding scan procedures and return */
			  for (xptr3 = xasl;
			       xptr3 && xptr3 != xptr2->scan_ptr;
			       xptr3 = xptr3->scan_ptr)
			    {
			      if (xptr3->curr_spec)
				{
				  scan_end_scan (thread_p,
						 &xptr3->curr_spec->s_id);
				}
			      xptr3->curr_spec = NULL;
			      xptr3->next_scan_block_on = false;
			    }
			  return S_END;
			}
		      else
			{
			  return S_ERROR;
			}
		    }
		}
	    }
	  else if (xs_scan2 == S_ERROR)
	    {
	      return S_ERROR;
	    }

	}
      else			/* last scan successfully moved */
	{
	  if (scan_reset_scan_block (thread_p, &prev_xptr->curr_spec->s_id) ==
	      S_ERROR)
	    {
	      return S_ERROR;
	    }
	}

      /* make previous scan the last scan ptr */
      last_xptr = prev_xptr;
    }				/* while */

  /* return the status of the first XASL block */
  return (xasl->curr_spec) ? S_SUCCESS : S_END;
}

/*
 * qexec_execute_scan () -
 *   return: SCAN_CODE (S_SUCCESS, S_END, S_ERROR)
 *   xasl(in)   : XASL Tree block
 *   xasl_state(in)     : XASL tree state information
 *   next_scan_fnc(in)  : Function to interpret following scan block
 *
 * Note: This routine executes one iteration on a scan operation on
 * the given XASL tree block. If end_of_scan is reached, it
 * return S_END. If an error occurs, it returns
 * S_ERROR. Each scan procedure block may have its own scan
 * procedures forming a path of scan procedure blocks. Thus, for
 * each scan procedure block interpretation, if there are already
 * active scan procedures started from that block, first their
 * execution is requested. Only if all of the following scan
 * procedures come to an end returning S_END, then the
 * current scan procedure scan item is advanced. When this scan
 * procedure too come to an end, it returns S_END to the
 * caller, indicating that there are no more scan items in the
 * path of the scan procedure blocks.
 *
 * Note: This function is the general scan block interpretation function.
 */
static SCAN_CODE
qexec_execute_scan (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
		    XASL_STATE * xasl_state, QFILE_TUPLE_RECORD * ignore,
		    XASL_SCAN_FNC_PTR next_scan_fnc)
{
  XASL_NODE *xptr;
  SCAN_CODE sc_scan;
  SCAN_CODE xs_scan;
  DB_LOGICAL ev_res;
  int qualified;

  /* check if further scan procedure are still active */
  if (xasl->scan_ptr && xasl->next_scan_on)
    {
      xs_scan = (*next_scan_fnc) (thread_p, xasl->scan_ptr, xasl_state,
				  ignore, next_scan_fnc + 1);
      if (xs_scan != S_END)
	{
	  return xs_scan;
	}

      xasl->next_scan_on = false;
    }

  do
    {
      sc_scan = scan_next_scan (thread_p, &xasl->curr_spec->s_id);
      if (sc_scan != S_SUCCESS)
	{
	  return sc_scan;
	}

      /* set scan item as qualified */
      qualified = true;

      if (qualified)
	{
	  /* evaluate dptr list */
	  for (xptr = xasl->dptr_list; xptr != NULL; xptr = xptr->next)
	    {
	      /* clear correlated subquery list files */
	      qexec_clear_head_lists (thread_p, xptr);
	      if (XASL_IS_FLAGED (xptr, XASL_LINK_TO_REGU_VARIABLE))
		{
		  /* skip if linked to regu var */
		  continue;
		}
	      if (qexec_execute_mainblock (thread_p, xptr, xasl_state) !=
		  NO_ERROR)
		{
		  return S_ERROR;
		}
	    }
	}			/* if (qualified) */

      if (qualified)
	{
	  /* evaluate after join predicate */
	  ev_res = V_UNKNOWN;
	  if (xasl->after_join_pred != NULL)
	    {
	      ev_res = eval_pred (thread_p, xasl->after_join_pred,
				  &xasl_state->vd, NULL);
	      if (ev_res == V_ERROR)
		{
		  return S_ERROR;
		}
	    }
	  qualified = (xasl->after_join_pred == NULL || ev_res == V_TRUE);
	}			/* if (qualified) */

      if (qualified)
	{
	  /* evaluate if predicate */
	  ev_res = V_UNKNOWN;
	  if (xasl->if_pred != NULL)
	    {
	      ev_res =
		eval_pred (thread_p, xasl->if_pred, &xasl_state->vd, NULL);
	      if (ev_res == V_ERROR)
		{
		  return S_ERROR;
		}
	    }
	  qualified = (xasl->if_pred == NULL || ev_res == V_TRUE);
	}			/* if (qualified) */

      if (qualified)
	{
	  if (!xasl->scan_ptr)
	    {
	      /* no scan procedure block */
	      return S_SUCCESS;
	    }
	  else
	    {
	      /* current scan block has at least one qualified item */
	      xasl->curr_spec->s_id.qualified_block = true;

	      /* start following scan procedure */
	      xasl->scan_ptr->next_scan_on = false;
	      if (scan_reset_scan_block
		  (thread_p, &xasl->scan_ptr->curr_spec->s_id) == S_ERROR)
		{
		  return S_ERROR;
		}

	      xasl->next_scan_on = true;

	      /* execute following scan procedure */
	      xs_scan =
		(*next_scan_fnc) (thread_p, xasl->scan_ptr, xasl_state,
				  ignore, next_scan_fnc + 1);
	      if (xs_scan == S_END)
		{
		  xasl->next_scan_on = false;
		}
	      else
		{
		  return xs_scan;
		}
	    }
	}			/* if (qualified) */

    }
  while (1);

}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * qexec_reset_regu_variable_list () - reset value cache for a list of regu
 *				       variables
 * return : void
 * list (in) : regu variable list
 */
static void
qexec_reset_regu_variable_list (REGU_VARIABLE_LIST list)
{
  REGU_VARIABLE_LIST var = list;

  while (var != NULL)
    {
      qexec_reset_regu_variable (&var->value);
      var = var->next;
    }
}

/*
 * qexec_reset_pred_expr () - reset value cache for a pred expr
 * return : void
 * pred (in) : pred expr
 */
static void
qexec_reset_pred_expr (PRED_EXPR * pred)
{
  if (pred == NULL)
    {
      return;
    }
  switch (pred->type)
    {
    case T_PRED:
      qexec_reset_pred_expr (pred->pe.pred.lhs);
      qexec_reset_pred_expr (pred->pe.pred.rhs);
      break;
    case T_EVAL_TERM:
      switch (pred->pe.eval_term.et_type)
	{
	case T_COMP_EVAL_TERM:
	  {
	    COMP_EVAL_TERM *et_comp = &pred->pe.eval_term.et.et_comp;

	    qexec_reset_regu_variable (et_comp->comp_lhs);
	    qexec_reset_regu_variable (et_comp->comp_rhs);
	  }
	  break;
	case T_ALSM_EVAL_TERM:
	  {
	    ALSM_EVAL_TERM *et_alsm = &pred->pe.eval_term.et.et_alsm;

	    qexec_reset_regu_variable (et_alsm->elem);
	    qexec_reset_regu_variable (et_alsm->elemset);
	  }
	  break;
	case T_LIKE_EVAL_TERM:
	  {
	    LIKE_EVAL_TERM *et_like = &pred->pe.eval_term.et.et_like;

	    qexec_reset_regu_variable (et_like->src);
	    qexec_reset_regu_variable (et_like->pattern);
	    qexec_reset_regu_variable (et_like->esc_char);
	  }
	  break;
	case T_RLIKE_EVAL_TERM:
	  {
	    RLIKE_EVAL_TERM *et_rlike = &pred->pe.eval_term.et.et_rlike;
	    qexec_reset_regu_variable (et_rlike->case_sensitive);
	    qexec_reset_regu_variable (et_rlike->pattern);
	    qexec_reset_regu_variable (et_rlike->src);
	  }
	}
      break;
    case T_NOT_TERM:
      qexec_reset_pred_expr (pred->pe.not_term);
      break;
    }
}

/*
 * qexec_reset_regu_variable () - reset the cache for a regu variable
 * return : void
 * var (in) : regu variable
 */
static void
qexec_reset_regu_variable (REGU_VARIABLE * var)
{
  if (var == NULL)
    {
      return;
    }

  switch (var->type)
    {
    case TYPE_ATTR_ID:
      var->value.attr_descr.cache_dbvalp = NULL;
      break;
    case TYPE_INARITH:
    case TYPE_OUTARITH:
      qexec_reset_regu_variable (var->value.arithptr->leftptr);
      qexec_reset_regu_variable (var->value.arithptr->rightptr);
      qexec_reset_regu_variable (var->value.arithptr->thirdptr);
      /* use arithptr */
      break;
    case TYPE_FUNC:
      /* use funcp */
      qexec_reset_regu_variable_list (var->value.funcp->operand);
      break;
    default:
      break;
    }
}
#endif

/*
 * qexec_intprt_fnc () -
 *   return: scan code
 *   xasl(in)   : XASL Tree pointer
 *   xasl_state(in)     : XASL Tree state information
 *   tplrec(out) : Tuple record descriptor to store result tuples
 *   next_scan_fnc(in)  : Function to interpret following XASL scan block
 *
 * Note: This function is the main function used to interpret an XASL
 * tree block. That is, it assumes a general format XASL block
 * with all possible representations.
 */
static SCAN_CODE
qexec_intprt_fnc (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
		  XASL_STATE * xasl_state, QFILE_TUPLE_RECORD * tplrec,
		  XASL_SCAN_FNC_PTR next_scan_fnc)
{
  XASL_NODE *xptr;
  SCAN_CODE xs_scan;
  SCAN_CODE xb_scan;
  SCAN_CODE ls_scan;
  DB_LOGICAL ev_res;
  int qualified;
  AGGREGATE_TYPE *agg_ptr;
  bool count_star_with_iscan_opt = false;

  if (xasl->type == BUILDVALUE_PROC)
    {
      BUILDVALUE_PROC_NODE *buildvalue = &xasl->proc.buildvalue;
      if (buildvalue->agg_list != NULL)
	{
	  int error = NO_ERROR;
	  bool is_scan_needed = false;

	  error = qexec_evaluate_aggregates_optimize (thread_p,
						      buildvalue->agg_list,
						      xasl->spec_list,
						      &is_scan_needed);
	  if (error != NO_ERROR)
	    {
	      is_scan_needed = true;
	    }

	  if (prm_get_bool_value
	      (PRM_ID_OPTIMIZER_ENABLE_AGGREGATE_OPTIMIZATION) == false)
	    {
	      is_scan_needed = true;
	    }

	  if (!is_scan_needed)
	    {
	      return S_SUCCESS;
	    }

	  agg_ptr = buildvalue->agg_list;
	  if (!xasl->scan_ptr	/* no scan procedure */
	      && !xasl->if_pred	/* no if predicates */
	      && !xasl->instnum_pred	/* no instnum predicate */
	      && agg_ptr->next == NULL	/* no other aggregate functions */
	      && agg_ptr->function == PT_COUNT_STAR)
	    {
	      /* only one count(*) function */
	      ACCESS_SPEC_TYPE *specp = xasl->spec_list;
	      if (specp->next == NULL
		  && specp->access == INDEX
		  && specp->s.cls_node.cls_regu_list_pred == NULL
		  && specp->where_pred == NULL)
		{
		  /* count(*) query will scan an index
		   * but does not have a data-filter
		   */
		  specp->s_id.s.isid.need_count_only = true;
		  count_star_with_iscan_opt = true;
		}
	    }
	}
    }
  else if (xasl->type == BUILDLIST_PROC)
    {
      /* If it is BUILDLIST, do not optimize aggregation */
      if (xasl->proc.buildlist.g_agg_list != NULL)
	{
	  for (agg_ptr = xasl->proc.buildlist.g_agg_list; agg_ptr;
	       agg_ptr = agg_ptr->next)
	    {
	      agg_ptr->flag_agg_optimize = false;
	    }
	}
    }

  while ((xb_scan = qexec_next_scan_block_iterations (thread_p,
						      xasl)) == S_SUCCESS)
    {
      while ((ls_scan = scan_next_scan (thread_p,
					&xasl->curr_spec->s_id)) == S_SUCCESS)
	{
	  if (count_star_with_iscan_opt)
	    {
	      xasl->proc.buildvalue.agg_list->accumulator.curr_cnt +=
		(&xasl->curr_spec->s_id)->s.isid.oid_list.oid_cnt;
	      /* may have more scan ranges */
	      continue;
	    }

	  /* set scan item as qualified */
	  qualified = true;

	  if (qualified)
	    {
	      /* evaluate dptr list */
	      for (xptr = xasl->dptr_list; xptr != NULL; xptr = xptr->next)
		{
		  /* clear correlated subquery list files */
		  qexec_clear_head_lists (thread_p, xptr);
		  if (XASL_IS_FLAGED (xptr, XASL_LINK_TO_REGU_VARIABLE))
		    {
		      /* skip if linked to regu var */
		      continue;
		    }
		  if (qexec_execute_mainblock (thread_p, xptr, xasl_state) !=
		      NO_ERROR)
		    {
		      return S_ERROR;
		    }
		}

	      /* evaluate after join predicate */
	      ev_res = V_UNKNOWN;
	      if (xasl->after_join_pred != NULL)
		{
		  ev_res = eval_pred (thread_p, xasl->after_join_pred,
				      &xasl_state->vd, NULL);
		  if (ev_res == V_ERROR)
		    {
		      return S_ERROR;
		    }
		}
	      qualified = (xasl->after_join_pred == NULL || ev_res == V_TRUE);

	      if (qualified)
		{
		  /* evaluate if predicate */
		  ev_res = V_UNKNOWN;
		  if (xasl->if_pred != NULL)
		    {
		      ev_res = eval_pred (thread_p, xasl->if_pred,
					  &xasl_state->vd, NULL);
		      if (ev_res == V_ERROR)
			{
			  return S_ERROR;
			}
		    }
		  qualified = (xasl->if_pred == NULL || ev_res == V_TRUE);
		}

	      if (qualified)
		{
		  if (!xasl->scan_ptr)
		    {		/* no scan procedure block */

		      /* evaluate inst_num predicate */
		      if (xasl->instnum_val)
			{
			  ev_res = qexec_eval_instnum_pred (thread_p,
							    xasl, xasl_state);
			  if (ev_res == V_ERROR)
			    {
			      return S_ERROR;
			    }

			  if ((xasl->instnum_flag
			       & XASL_INSTNUM_FLAG_SCAN_LAST_STOP))
			    {
			      if (qexec_end_one_iteration
				  (thread_p, xasl, xasl_state,
				   tplrec) != NO_ERROR)
				{
				  return S_ERROR;
				}

			      return S_SUCCESS;
			    }

			  if ((xasl->instnum_flag
			       & XASL_INSTNUM_FLAG_SCAN_STOP))
			    {
			      return S_SUCCESS;
			    }
			}
		      qualified = (xasl->instnum_pred == NULL
				   || ev_res == V_TRUE);
		      if (qualified
			  && (qexec_end_one_iteration (thread_p, xasl,
						       xasl_state,
						       tplrec) != NO_ERROR))
			{
			  return S_ERROR;
			}

		    }
		  else
		    {		/* handle the scan procedure */
		      /* current scan block has at least one qualified item */
		      xasl->curr_spec->s_id.qualified_block = true;

		      /* handle the scan procedure */
		      xasl->scan_ptr->next_scan_on = false;
		      if (scan_reset_scan_block
			  (thread_p,
			   &xasl->scan_ptr->curr_spec->s_id) == S_ERROR)
			{
			  return S_ERROR;
			}

		      xasl->next_scan_on = true;


		      while ((xs_scan = (*next_scan_fnc) (thread_p,
							  xasl->scan_ptr,
							  xasl_state,
							  tplrec,
							  next_scan_fnc +
							  1)) == S_SUCCESS)
			{

			  /* evaluate inst_num predicate */
			  if (xasl->instnum_val)
			    {
			      ev_res =
				qexec_eval_instnum_pred (thread_p,
							 xasl, xasl_state);
			      if (ev_res == V_ERROR)
				{
				  return S_ERROR;
				}

			      if ((xasl->instnum_flag
				   & XASL_INSTNUM_FLAG_SCAN_LAST_STOP))
				{
				  if (qexec_end_one_iteration
				      (thread_p, xasl, xasl_state,
				       tplrec) != NO_ERROR)
				    {
				      return S_ERROR;
				    }

				  return S_SUCCESS;
				}

			      if (xasl->instnum_flag
				  & XASL_INSTNUM_FLAG_SCAN_STOP)
				{
				  return S_SUCCESS;
				}
			    }
			  qualified = (xasl->instnum_pred == NULL
				       || ev_res == V_TRUE);

			  if (qualified
			      /* one iteration successfully completed */
			      && qexec_end_one_iteration (thread_p,
							  xasl,
							  xasl_state,
							  tplrec) != NO_ERROR)
			    {
			      return S_ERROR;
			    }

			}

		      if (xs_scan != S_END)	/* an error happened */
			{
			  return S_ERROR;
			}
		    }
		}
	    }

	  qexec_clear_all_lists (thread_p, xasl);
	}

      if (ls_scan != S_END)	/* an error happened */
	{
	  return S_ERROR;
	}
    }

  if (xb_scan != S_END)		/* an error happened */
    {
      return S_ERROR;
    }

  return S_SUCCESS;
}

/*
 * qexec_setup_list_id () -
 *   return: NO_ERROR, or ER_code
 *   xasl(in)   : XASL Tree block
 *
 * Note: This routine is used by update/delete/insert to set up a
 * type_list. Copying a list_id structure fails unless it has a type list.
 */
static int
qexec_setup_list_id (UNUSED_ARG THREAD_ENTRY * thread_p, XASL_NODE * xasl)
{
  QFILE_LIST_ID *list_id;

  list_id = xasl->list_id;
  list_id->tuple_cnt = 0;
  list_id->page_cnt = 0;

  /* For streaming queries, set last_pgptr->next_vpid to NULL */
  if (list_id->last_pgptr != NULL)
    {
      QFILE_PUT_NEXT_VPID_NULL (list_id->last_pgptr);
    }

  list_id->last_pgptr = NULL;	/* don't want qfile_close_list() to free this
				 * bogus listid
				 */
  list_id->type_list.type_cnt = 1;
  list_id->type_list.domp =
    (TP_DOMAIN **) malloc (list_id->type_list.type_cnt *
			   sizeof (TP_DOMAIN *));
  if (list_id->type_list.domp == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      list_id->type_list.type_cnt * sizeof (TP_DOMAIN *));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }
  /* set up to return object domains in case we want to return
   * the updated/inserted/deleted oid's
   */
  list_id->type_list.domp[0] = &tp_Object_domain;

  return NO_ERROR;
}

/*
 * qexec_init_upddel_ehash_files () - Initializes the hash files used for
 *				       duplicate OIDs elimination.
 *   return: NO_ERROR, or ER_code
 *   thread_p(in):
 *   buildlist(in): BUILDLIST_PROC XASL
 *
 * Note: The function is used only for SELECT statement generated for
 *	 UPDATE/DELETE. The case of SINGLE-UPDATE/SINGLE-DELETE is skipped.
 */
static int
qexec_init_upddel_ehash_files (THREAD_ENTRY * thread_p, XASL_NODE * buildlist)
{
  EHID *ehid;

  if (buildlist == NULL || buildlist->type != BUILDLIST_PROC)
    {
      return NO_ERROR;
    }

  assert (buildlist->upd_del_class_cnt == 1);

  ehid = &(buildlist->proc.buildlist.upd_del_ehid);
  assert (EHID_IS_NULL (ehid));

  EHID_SET_NULL (ehid);
  ehid->vfid.volid = LOG_DBFIRST_VOLID;
  if (xehash_create (thread_p, ehid, DB_TYPE_OBJECT, -1, NULL, 0, true) ==
      NULL)
    {
      return ER_FAILED;
    }

  assert (!EHID_IS_NULL (&(buildlist->proc.buildlist.upd_del_ehid)));

  return NO_ERROR;
}

/*
 * qexec_destroy_upddel_ehash_files () - Destroys the hash files used for
 *					 duplicate rows elimination in
 *					 UPDATE/DELETE.
 *   return: void
 *   thread_p(in):
 *   buildlist(in): BUILDLIST_PROC XASL
 *
 * Note: The function is used only for SELECT statement generated for
 *	 UPDATE/DELETE.
 */
static void
qexec_destroy_upddel_ehash_files (THREAD_ENTRY * thread_p,
				  XASL_NODE * buildlist)
{
  EHID *ehid;

  assert (buildlist->upd_del_class_cnt == 1);

  ehid = &(buildlist->proc.buildlist.upd_del_ehid);
  if (!EHID_IS_NULL (ehid))
    {
      xehash_destroy (thread_p, ehid);
      EHID_SET_NULL (ehid);
    }

  assert (EHID_IS_NULL (&(buildlist->proc.buildlist.upd_del_ehid)));
}

/*
 * qexec_execute_update () -
 *   return: NO_ERROR, or ER_code
 *   xasl(in): XASL Tree block
 *   xasl_state(in):
 */
static int
qexec_execute_update (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
		      XASL_STATE * xasl_state)
{
  UPDATE_PROC_NODE *update = &xasl->proc.update;
  UPDDEL_CLASS_INFO *upd_cls = NULL;
  UPDATE_ASSIGNMENT *assign = NULL;
  SCAN_CODE xb_scan;
  SCAN_CODE ls_scan;
  XASL_NODE *aptr;
  DB_VALUE *valp;
  QPROC_DB_VALUE_LIST vallist;
  int assign_idx = 0;
  int rc;
  int attr_id;
  OID *oid = NULL;
  UPDDEL_CLASS_INFO_INTERNAL internal_class;
  ACCESS_SPEC_TYPE *specp = NULL;
  SCAN_ID *s_id;
  LOG_LSA lsa;
  int savepoint_used = 0;
  int satisfies_constraints;
  int force_count;
  int tuple_cnt, error = NO_ERROR;
  bool scan_open = false;

  if (qexec_init_internal_class (thread_p, &internal_class) != NO_ERROR)
    {
      assert (false);
      QEXEC_GOTO_EXIT_ON_ERROR;
    }

  aptr = xasl->aptr_list;

  if (qexec_execute_mainblock (thread_p, aptr, xasl_state) != NO_ERROR)
    {
      QEXEC_GOTO_EXIT_ON_ERROR;
    }

  /* This guarantees that the result list file will have a type list.
     Copying a list_id structure fails unless it has a type list. */
  if (qexec_setup_list_id (thread_p, xasl) != NO_ERROR)
    {
      QEXEC_GOTO_EXIT_ON_ERROR;
    }

  /* need to start a topop to ensure statement atomicity.
     One update statement might update several disk images.
     For example, one row update might update zero or more index keys,
     one heap record, and other things.
     So, the update statement must be performed atomically.
   */
  if (xtran_server_start_topop (thread_p, &lsa) != NO_ERROR)
    {
      QEXEC_GOTO_EXIT_ON_ERROR;
    }
  savepoint_used = 1;

  specp = xasl->spec_list;
  /* readonly_scan = true */
  if (qexec_open_scan (thread_p, specp, xasl->val_list, &xasl_state->vd, true,
		       specp->fixed_scan,
		       &specp->s_id, xasl_state->query_id, false) != NO_ERROR)
    {
      if (savepoint_used)
	{
	  xtran_server_end_topop (thread_p, LOG_RESULT_TOPOP_ABORT, &lsa);
	}

      QEXEC_GOTO_EXIT_ON_ERROR;
    }

  scan_open = true;

  tuple_cnt = 1;
  while ((xb_scan =
	  qexec_next_scan_block_iterations (thread_p, xasl)) == S_SUCCESS)
    {
      s_id = &xasl->curr_spec->s_id;
      while ((ls_scan = scan_next_scan (thread_p, s_id)) == S_SUCCESS)
	{
	  tuple_cnt++;

	  /* evaluate constraint predicate */
	  satisfies_constraints = V_UNKNOWN;
	  if (update->cons_pred != NULL)
	    {
	      satisfies_constraints = eval_pred (thread_p, update->cons_pred,
						 &xasl_state->vd, NULL);
	      if (satisfies_constraints == V_ERROR)
		{
		  QEXEC_GOTO_EXIT_ON_ERROR;
		}
	    }

	  if (update->cons_pred != NULL && satisfies_constraints != V_TRUE)
	    {
	      /* currently there are only NOT NULL constraints */
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_NULL_CONSTRAINT_VIOLATION, 0);
	      QEXEC_GOTO_EXIT_ON_ERROR;
	    }

	  /* calc. OID, HFID, attributes cache info and
	   * statistical information only if class has changed */
	  vallist = s_id->val_list->valp;

	  upd_cls = update->class_info;

	  /* instance OID */
	  valp = vallist->val;
	  if (valp == NULL)
	    {
	      QEXEC_GOTO_EXIT_ON_ERROR;
	    }
	  if (DB_IS_NULL (valp))
	    {
	      assert (false);
	      QEXEC_GOTO_EXIT_ON_ERROR;
	    }
	  oid = DB_GET_OID (valp);
	  assert (!OID_ISNULL (oid));

	  vallist = vallist->next;

	  if (internal_class.class_oid == NULL)
	    {
	      /* Load internal_class object with information for class_oid */
	      error = qexec_upddel_setup_current_class (thread_p, upd_cls,
							&internal_class);
	      if (error != NO_ERROR)
		{
		  QEXEC_GOTO_EXIT_ON_ERROR;
		}

	      /* start attribute cache information for new subclass */
	      if (heap_attrinfo_start (thread_p, &(upd_cls->class_oid),
				       -1, NULL,
				       &internal_class.attr_info) != NO_ERROR)
		{
		  QEXEC_GOTO_EXIT_ON_ERROR;
		}

	      internal_class.is_attr_info_inited = true;
	    }

	  if (heap_attrinfo_clear_dbvalues (&internal_class.attr_info)
	      != NO_ERROR)
	    {
	      QEXEC_GOTO_EXIT_ON_ERROR;
	    }

	  /* perform assignments */
	  for (assign_idx = 0;
	       assign_idx < update->no_assigns && vallist != NULL;
	       assign_idx++, vallist = vallist->next)
	    {
	      HEAP_CACHE_ATTRINFO *attr_info;

	      assign = &update->assigns[assign_idx];

	      attr_info = &internal_class.attr_info;
	      upd_cls = update->class_info;

	      assert (!OID_ISNULL (oid));

	      attr_id = upd_cls->att_id[assign->att_idx];

	      rc = heap_attrinfo_set (oid, attr_id, vallist->val, attr_info);
	      if (rc != NO_ERROR)
		{
		  QEXEC_GOTO_EXIT_ON_ERROR;
		}
	    }

	  /* Flush new values for the class */
	  upd_cls = update->class_info;
	  force_count = 0;

	  assert (!OID_ISNULL (oid));

	  error = locator_attribute_info_force (thread_p,
						internal_class.class_hfid,
						oid,
						&internal_class.attr_info,
						upd_cls->att_id,
						upd_cls->no_attrs,
						LC_FLUSH_UPDATE,
						&internal_class.scan_cache,
						&force_count);
	  if (error != NO_ERROR && error != ER_HEAP_UNKNOWN_OBJECT)
	    {
	      QEXEC_GOTO_EXIT_ON_ERROR;
	    }

	  /* either NO_ERROR or unknown object */
	  force_count = 1;
	  error = NO_ERROR;

	  /* Instances are not put into the result list file,
	   * but are counted.
	   */
	  if (force_count)
	    {
	      xasl->list_id->tuple_cnt++;
	    }
	}			/* while (...) */

      if (ls_scan != S_END)
	{
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}
    }				/* while (...) */

  if (xb_scan != S_END)
    {
      QEXEC_GOTO_EXIT_ON_ERROR;
    }

  qexec_close_scan (thread_p, specp);

  qexec_clear_internal_class (thread_p, &internal_class);

  if (savepoint_used)
    {
      if (xtran_server_end_topop
	  (thread_p, LOG_RESULT_TOPOP_ATTACH_TO_OUTER, &lsa) != TRAN_ACTIVE)
	{
	  qexec_failure_line (__LINE__, xasl_state);
	  return ER_FAILED;
	}
    }

  return NO_ERROR;

exit_on_error:

  if (scan_open)
    {
      qexec_end_scan (thread_p, specp);
      qexec_close_scan (thread_p, specp);
    }

  if (savepoint_used)
    {
      xtran_server_end_topop (thread_p, LOG_RESULT_TOPOP_ABORT, &lsa);
    }

  qexec_clear_internal_class (thread_p, &internal_class);

  return ER_FAILED;
}

/*
 * qexec_execute_delete () -
 *   return: NO_ERROR or ER_code
 *   xasl(in)   : XASL Tree block
 *   xasl_state(in)     :
 */
static int
qexec_execute_delete (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
		      XASL_STATE * xasl_state)
{
#define MIN_NUM_ROWS_FOR_MULTI_DELETE   20

  DELETE_PROC_NODE *delete_ = &xasl->proc.delete_;
  UPDDEL_CLASS_INFO *del_cls = NULL;
  SCAN_CODE xb_scan = S_END;
  SCAN_CODE ls_scan = S_END;
  XASL_NODE *aptr = NULL;
  DB_VALUE *valp = NULL;
  OID *oid = NULL;
  ACCESS_SPEC_TYPE *specp = NULL;
  SCAN_ID *s_id = NULL;
  LOG_LSA lsa;
  int savepoint_used = 0;
  int force_count = 0;
  int error = NO_ERROR;
  QPROC_DB_VALUE_LIST val_list = NULL;
  bool scan_open = false;
  UPDDEL_CLASS_INFO_INTERNAL internal_class;

  if (qexec_init_internal_class (thread_p, &internal_class) != NO_ERROR)
    {
      assert (false);
      QEXEC_GOTO_EXIT_ON_ERROR;
    }

  aptr = xasl->aptr_list;

  if (qexec_execute_mainblock (thread_p, aptr, xasl_state) != NO_ERROR)
    {
      QEXEC_GOTO_EXIT_ON_ERROR;
    }

  /* This guarantees that the result list file will have a type list.
     Copying a list_id structure fails unless it has a type list. */
  if ((qexec_setup_list_id (thread_p, xasl) != NO_ERROR)
      /* it can be > 2
         || (aptr->list_id->type_list.type_cnt != 2) */ )
    {
      QEXEC_GOTO_EXIT_ON_ERROR;
    }

  /* need to start a topop to ensure statement atomicity.
   * One delete statement might update several disk images.
   * For example, one row delete might update
   * zero or more index keys, one heap record,
   * catalog info of object count, and other things.
   * So, the delete statement must be performed atomically.
   */
  if (xtran_server_start_topop (thread_p, &lsa) != NO_ERROR)
    {
      QEXEC_GOTO_EXIT_ON_ERROR;
    }

  savepoint_used = 1;

  specp = xasl->spec_list;
  /* readonly_scan = true */
  if (qexec_open_scan (thread_p, specp, xasl->val_list,
		       &xasl_state->vd, true, specp->fixed_scan,
		       &specp->s_id, xasl_state->query_id, false) != NO_ERROR)
    {
      QEXEC_GOTO_EXIT_ON_ERROR;
    }

  scan_open = true;

  while ((xb_scan =
	  qexec_next_scan_block_iterations (thread_p, xasl)) == S_SUCCESS)
    {
      s_id = &xasl->curr_spec->s_id;

      while ((ls_scan = scan_next_scan (thread_p, s_id)) == S_SUCCESS)
	{
	  val_list = s_id->val_list->valp;

	  del_cls = delete_->class_info;

	  valp = val_list->val;
	  if (valp == NULL)
	    {
	      QEXEC_GOTO_EXIT_ON_ERROR;
	    }
	  if (DB_IS_NULL (valp))
	    {
	      assert (false);
	      QEXEC_GOTO_EXIT_ON_ERROR;
	    }
	  oid = DB_GET_OID (valp);
	  assert (!OID_ISNULL (oid));

	  val_list = val_list->next;

	  if (internal_class.class_oid == NULL)
	    {
	      /* find class HFID */
	      error = qexec_upddel_setup_current_class (thread_p, del_cls,
							&internal_class);
	      if (error != NO_ERROR)
		{
		  QEXEC_GOTO_EXIT_ON_ERROR;
		}
	    }

	  force_count = 0;

	  if (locator_attribute_info_force (thread_p,
					    internal_class.class_hfid, oid,
					    NULL, NULL, 0, LC_FLUSH_DELETE,
					    &internal_class.scan_cache,
					    &force_count) != NO_ERROR)
	    {
	      QEXEC_GOTO_EXIT_ON_ERROR;
	    }

	  if (force_count)
	    {
	      xasl->list_id->tuple_cnt++;
	    }

	}			/* while (...) */

      if (ls_scan != S_END)
	{
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}

    }				/* while (...) */

  if (xb_scan != S_END)
    {
      QEXEC_GOTO_EXIT_ON_ERROR;
    }

  qexec_close_scan (thread_p, specp);

  qexec_clear_internal_class (thread_p, &internal_class);

  if (savepoint_used)
    {
      if (xtran_server_end_topop
	  (thread_p, LOG_RESULT_TOPOP_ATTACH_TO_OUTER, &lsa) != TRAN_ACTIVE)
	{
	  qexec_failure_line (__LINE__, xasl_state);
	  return ER_FAILED;
	}
    }

  return NO_ERROR;

exit_on_error:
  if (scan_open)
    {
      qexec_end_scan (thread_p, specp);
      qexec_close_scan (thread_p, specp);
    }

  qexec_clear_internal_class (thread_p, &internal_class);

  if (savepoint_used)
    {
      xtran_server_end_topop (thread_p, LOG_RESULT_TOPOP_ABORT, &lsa);
    }

  return ER_FAILED;
}


/*
 * qexec_remove_duplicates_for_replace () - Removes the objects that would
 *       generate unique index violations when inserting the given attr_info
 *       (This is used for executing REPLACE statements)
 *   return: NO_ERROR or ER_code
 *   scan_cache(in):
 *   oid(in): for groupid
 *   attr_info(in/out): The attribute information that will be inserted
 *   index_attr_info(in/out):
 *   idx_info(in):
 *   removed_count (in/out):
 */
static int
qexec_remove_duplicates_for_replace (THREAD_ENTRY * thread_p,
				     HEAP_SCANCACHE * scan_cache, OID * oid,
				     HEAP_CACHE_ATTRINFO * attr_info,
				     HEAP_CACHE_ATTRINFO * index_attr_info,
				     const HEAP_IDX_ELEMENTS_INFO * idx_info,
				     int *removed_count)
{
  LC_COPYAREA *copyarea = NULL;
  RECDES new_recdes = RECDES_INITIALIZER;
  int i = 0;
  int error = NO_ERROR;
  DB_IDXKEY key;
  int force_count = 0;
  OR_INDEX *index = NULL;
  OID unique_oid;
  OID class_oid;
  BTID btid;
  HFID class_hfid, pruned_hfid;
  HEAP_SCANCACHE *local_scan_cache = NULL;
  BTREE_SEARCH r;

  assert (oid != NULL);
  assert (oid->groupid != NULL_GROUPID);

  *removed_count = 0;

  DB_IDXKEY_MAKE_NULL (&key);

  if (heap_attrinfo_clear_dbvalues (index_attr_info) != NO_ERROR)
    {
      goto error_exit;
    }

  copyarea =
    locator_allocate_copy_area_by_attr_info (thread_p, attr_info,
					     NULL, &new_recdes,
					     oid->groupid, -1);
  if (copyarea == NULL)
    {
      goto error_exit;
    }

  if (idx_info->has_single_col)
    {
      error = heap_attrinfo_read_dbvalues (thread_p, &oid_Null_oid,
					   &new_recdes, index_attr_info);
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}
    }
  assert_release (index_attr_info->last_classrepr != NULL);

  HFID_COPY (&class_hfid, &scan_cache->hfid);
  COPY_OID (&class_oid, &attr_info->class_oid);

  local_scan_cache = scan_cache;

  for (i = 0; i < idx_info->num_btids; ++i)
    {
      index = &(index_attr_info->last_classrepr->indexes[i]);
      if (!INDEX_IS_UNIQUE (index) || INDEX_IS_IN_PROGRESS (index))
	{
	  continue;
	}

      HFID_COPY (&pruned_hfid, &class_hfid);
      BTID_COPY (&btid, &index->btid);

      assert (DB_IDXKEY_IS_NULL (&key));
      error = heap_attrvalue_get_key (thread_p, i, index_attr_info,
				      &oid_Null_oid, &new_recdes,
				      &btid, &key);
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}

      OID_SET_NULL (&unique_oid);
      r = xbtree_find_unique (thread_p, &class_oid, &btid, &key, &unique_oid);

      if (r == BTREE_KEY_FOUND)
	{
	  force_count = 0;
	  error = locator_attribute_info_force (thread_p, &pruned_hfid,
						&unique_oid, NULL, NULL, 0,
						LC_FLUSH_DELETE,
						local_scan_cache,
						&force_count);
	  if (error != NO_ERROR)
	    {
	      goto error_exit;
	    }
	  if (force_count != 0)
	    {
	      assert (force_count == 1);
	      *removed_count += force_count;
	      force_count = 0;
	    }
	}
      else if (r == BTREE_ERROR_OCCURRED)
	{
	  goto error_exit;
	}
      else
	{
	  /* BTREE_KEY_NOTFOUND */
	  ;			/* just go to the next one */
	}

      db_idxkey_clear (&key);
    }

  if (copyarea != NULL)
    {
      locator_free_copy_area (copyarea);
      copyarea = NULL;
      new_recdes.data = NULL;
      new_recdes.area_size = 0;
    }

  return NO_ERROR;

error_exit:
  db_idxkey_clear (&key);

  if (copyarea != NULL)
    {
      locator_free_copy_area (copyarea);
      copyarea = NULL;
      new_recdes.data = NULL;
      new_recdes.area_size = 0;
    }
  return ER_FAILED;
}

/*
 * qexec_oid_of_duplicate_key_update () - Finds an OID of an object that would
 *       generate unique index violations when inserting the given attr_info
 *       (This is used for executing INSERT ON DUPLICATE KEY UPDATE
 *        statements)
 *   return: NO_ERROR or ER_code
 *   thread_p(in):
 *   scan_cache(in):
 *   oid(in): for grupid
 *   attr_info(in/out): The attribute information that will be inserted
 *   index_attr_info(in/out):
 *   idx_info(in):
 *   unique_oid_p(out): the OID of one object to be updated or a NULL OID if
 *                      there are no potential unique index violations
 * Note: A single OID is returned even if there are several objects that would
 *       generate unique index violations (this can only happen if there are
 *       several unique indexes).
 */
static int
qexec_oid_of_duplicate_key_update (THREAD_ENTRY * thread_p,
				   HEAP_SCANCACHE * scan_cache, OID * oid,
				   HEAP_CACHE_ATTRINFO * attr_info,
				   HEAP_CACHE_ATTRINFO * index_attr_info,
				   const HEAP_IDX_ELEMENTS_INFO * idx_info,
				   OID * unique_oid_p)
{
  LC_COPYAREA *copyarea = NULL;
  RECDES recdes = RECDES_INITIALIZER;
  int i = 0;
  int error_code = NO_ERROR;
  DB_IDXKEY key;
  bool found_duplicate = false;
  BTID btid;
  OR_INDEX *index;
  OID unique_oid;
  OID class_oid;
  HFID class_hfid;
  BTREE_SEARCH r;

  DB_IDXKEY_MAKE_NULL (&key);
  OID_SET_NULL (unique_oid_p);
  OID_SET_NULL (&unique_oid);

  assert (oid != NULL);
  assert (oid->groupid != NULL_GROUPID);

  if (heap_attrinfo_clear_dbvalues (index_attr_info) != NO_ERROR)
    {
      goto error_exit;
    }

  copyarea =
    locator_allocate_copy_area_by_attr_info (thread_p, attr_info,
					     NULL, &recdes, oid->groupid, -1);
  if (copyarea == NULL)
    {
      goto error_exit;
    }

  if (idx_info->has_single_col)
    {
      error_code = heap_attrinfo_read_dbvalues (thread_p, &oid_Null_oid,
						&recdes, index_attr_info);
      if (error_code != NO_ERROR)
	{
	  goto error_exit;
	}
    }
  assert (index_attr_info->last_classrepr != NULL);

  HFID_COPY (&class_hfid, &scan_cache->hfid);
  COPY_OID (&class_oid, &attr_info->class_oid);

  for (i = 0; i < idx_info->num_btids && !found_duplicate; ++i)
    {
      index = &(index_attr_info->last_classrepr->indexes[i]);
      if (!INDEX_IS_UNIQUE (index) || INDEX_IS_IN_PROGRESS (index))
	{
	  continue;
	}

      assert (DB_IDXKEY_IS_NULL (&key));
      error_code = heap_attrvalue_get_key (thread_p, i, index_attr_info,
					   &oid_Null_oid, &recdes,
					   &btid, &key);
      if (error_code != NO_ERROR)
	{
	  goto error_exit;
	}

      OID_SET_NULL (&unique_oid);
      r = xbtree_find_unique (thread_p, &class_oid, &btid, &key, &unique_oid);

      if (r == BTREE_KEY_FOUND)
	{
	  found_duplicate = true;
	  COPY_OID (unique_oid_p, &unique_oid);
	}
      else if (r == BTREE_ERROR_OCCURRED)
	{
	  goto error_exit;
	}
      else
	{
	  /* BTREE_KEY_NOTFOUND */
	  ;			/* just go to the next one */
	}

      db_idxkey_clear (&key);
    }

  if (copyarea != NULL)
    {
      locator_free_copy_area (copyarea);
      copyarea = NULL;
      recdes.data = NULL;
      recdes.area_size = 0;
    }

  return NO_ERROR;

error_exit:

  db_idxkey_clear (&key);

  if (copyarea != NULL)
    {
      locator_free_copy_area (copyarea);
      copyarea = NULL;
      recdes.data = NULL;
      recdes.area_size = 0;
    }

  return ER_FAILED;
}

/*
 * qexec_execute_duplicate_key_update () - Executes an update on a given OID
 *       (required by INSERT ON DUPLICATE KEY UPDATE processing)
 *   return: NO_ERROR or ER_code
 *   thread_p(in) :
 *   odku(in) : on duplicate key update clause info
 *   hfid(in) : class HFID
 *   oid(in) : for groupid
 *   vd(in) : values descriptor
 *   scan_cache(in): scan cache
 *   attr_info(in): attribute cache info
 *   index_attr_info(in): attribute info cache for indexes
 *   idx_info(in): index info
 *   force_count(out): the number of objects that have been updated; it should
 *                     always be 1 on success and 0 on error
 */
static int
qexec_execute_duplicate_key_update (THREAD_ENTRY * thread_p, ODKU_INFO * odku,
				    HFID * hfid, OID * oid, VAL_DESCR * vd,
				    HEAP_SCANCACHE * scan_cache,
				    HEAP_CACHE_ATTRINFO * attr_info,
				    HEAP_CACHE_ATTRINFO * index_attr_info,
				    HEAP_IDX_ELEMENTS_INFO * idx_info,
				    int *force_count)
{
  int satisfies_constraints;
  int assign_idx;
  UPDATE_ASSIGNMENT *assign;
  RECDES rec_descriptor = RECDES_INITIALIZER;
  SCAN_CODE scan_code;
  DB_VALUE *val = NULL;
  int error = NO_ERROR;
  bool need_clear = 0;
  OID unique_oid;
  HEAP_SCANCACHE *local_scan_cache = NULL;

  assert (oid != NULL);
  assert (oid->groupid != NULL_GROUPID);

  OID_SET_NULL (&unique_oid);

  local_scan_cache = scan_cache;

  error = qexec_oid_of_duplicate_key_update (thread_p,
					     scan_cache, oid, attr_info,
					     index_attr_info, idx_info,
					     &unique_oid);
  if (error != NO_ERROR)
    {
      goto exit_on_error;
    }

  if (OID_ISNULL (&unique_oid))
    {
      *force_count = 0;
      return NO_ERROR;
    }

  /* get attribute values */
  scan_code = heap_get (thread_p, &unique_oid, &rec_descriptor,
			local_scan_cache, PEEK);
  if (scan_code != S_SUCCESS)
    {
      goto exit_on_error;
    }

  error = heap_attrinfo_read_dbvalues (thread_p, &unique_oid, &rec_descriptor,
				       odku->attr_info);
  if (error != NO_ERROR)
    {
      goto exit_on_error;
    }

  need_clear = true;

  /* evaluate constraint predicate */
  satisfies_constraints = V_UNKNOWN;
  if (odku->cons_pred != NULL)
    {
      satisfies_constraints = eval_pred (thread_p, odku->cons_pred, vd, NULL);
      if (satisfies_constraints == V_ERROR)
	{
	  goto exit_on_error;
	}

      if (satisfies_constraints != V_TRUE)
	{
	  /* currently there are only NOT NULL constraints */
	  error = ER_NULL_CONSTRAINT_VIOLATION;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	  goto exit_on_error;
	}
    }

  /* set values for object */
  heap_attrinfo_clear_dbvalues (attr_info);
  for (assign_idx = 0; assign_idx < odku->no_assigns && error == NO_ERROR;
       assign_idx++)
    {
      assign = &odku->assignments[assign_idx];
#if 1				/* TODO - auto_param */
      if (assign->regu_var == NULL)
	{
	  assert (false);
	  goto exit_on_error;
	}
#endif

      assert_release (assign->regu_var != NULL);
      error = fetch_peek_dbval (thread_p, assign->regu_var, vd,
				NULL, NULL, &val);
      if (error != NO_ERROR)
	{
	  goto exit_on_error;
	}
      error = heap_attrinfo_set (&unique_oid, odku->attr_ids[assign_idx],
				 val, attr_info);
    }

  if (error != NO_ERROR)
    {
      goto exit_on_error;
    }

  error = locator_attribute_info_force (thread_p, hfid, &unique_oid,
					attr_info, odku->attr_ids,
					odku->no_assigns, LC_FLUSH_UPDATE,
					local_scan_cache, force_count);
  if (error != NO_ERROR)
    {
      goto exit_on_error;
    }

  assert (error == NO_ERROR);

  heap_attrinfo_clear_dbvalues (attr_info);
  heap_attrinfo_clear_dbvalues (odku->attr_info);

  return error;

exit_on_error:
  if (need_clear)
    {
      heap_attrinfo_clear_dbvalues (odku->attr_info);
    }

  return (error == NO_ERROR
	  && (error = er_errid ()) == NO_ERROR) ? ER_FAILED : error;
}

/*
 * qexec_execute_insert () -
 *   return: NO_ERROR or ER_code
 *   xasl(in)   : XASL Tree block
 *   xasl_state(in)     :
 */
static int
qexec_execute_insert (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
		      XASL_STATE * xasl_state, bool skip_aptr)
{
  INSERT_PROC_NODE *insert = &xasl->proc.insert;
  SCAN_CODE xb_scan;
  SCAN_CODE ls_scan;
  XASL_NODE *aptr = NULL;
  DB_VALUE *valp = NULL;
  QPROC_DB_VALUE_LIST vallist;
  int i, k;
  int val_no;
  int rc;
  OID oid;
  OID class_oid;
  HFID class_hfid;
  ACCESS_SPEC_TYPE *specp = NULL;
  SCAN_ID *s_id = NULL;
  HEAP_CACHE_ATTRINFO attr_info;
  HEAP_CACHE_ATTRINFO index_attr_info;
  HEAP_IDX_ELEMENTS_INFO idx_info;
  bool attr_info_inited = false;
  volatile bool index_attr_info_inited = false;
  volatile bool odku_attr_info_inited = false;
  LOG_LSA lsa;
  int savepoint_used = 0;
  int satisfies_constraints;
  HEAP_SCANCACHE scan_cache;
  bool scan_cache_inited = false;
  int force_count = 0;
  int no_default_expr = 0;
  volatile int n_indexes = 0;
  int error = 0;
  ODKU_INFO *odku_assignments = insert->odku;

  assert (xasl_state != NULL);

#if 1				/* TODO - NEED MORE CONSIDERATION */
  /* set shard groupid
   */
  OID_SET_NULL (&oid);
  oid.groupid = xasl_state->shard_groupid;
  assert (oid.groupid != NULL_GROUPID);
#endif

  aptr = xasl->aptr_list;
  val_no = insert->no_vals;

  if (!skip_aptr)
    {
      if (aptr
	  && qexec_execute_mainblock (thread_p, aptr, xasl_state) != NO_ERROR)
	{
	  qexec_failure_line (__LINE__, xasl_state);
	  return ER_FAILED;
	}
    }

  /* This guarantees that the result list file will have a type list.
     Copying a list_id structure fails unless it has a type list. */
  if (qexec_setup_list_id (thread_p, xasl) != NO_ERROR)
    {
      qexec_failure_line (__LINE__, xasl_state);
      return ER_FAILED;
    }

  /* need to start a topop to ensure statement atomicity.
     One insert statement might update several disk images.
     For example, one row insert might update
     one heap record, zero or more index keys,
     catalog info of object count, and other things.
     So, the insert statement must be performed atomically.
   */
  if (xtran_server_start_topop (thread_p, &lsa) != NO_ERROR)
    {
      qexec_failure_line (__LINE__, xasl_state);
      return ER_FAILED;
    }
  savepoint_used = 1;

  COPY_OID (&class_oid, &insert->class_oid);
  HFID_COPY (&class_hfid, &insert->class_hfid);

  if (insert->has_uniques && (insert->do_replace || odku_assignments != NULL))
    {
      if (heap_attrinfo_start_with_index (thread_p, &class_oid, NULL,
					  &index_attr_info, &idx_info) < 0)
	{
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}
      index_attr_info_inited = true;
      if (odku_assignments != NULL)
	{
	  error = heap_attrinfo_start (thread_p, &insert->class_oid, -1, NULL,
				       odku_assignments->attr_info);
	  if (error != NO_ERROR)
	    {
	      QEXEC_GOTO_EXIT_ON_ERROR;
	    }
	  odku_attr_info_inited = true;
	}
    }

  if (heap_attrinfo_start (thread_p, &class_oid, -1, NULL, &attr_info) !=
      NO_ERROR)
    {
      QEXEC_GOTO_EXIT_ON_ERROR;
    }
  attr_info_inited = true;
  n_indexes = attr_info.last_classrepr->n_indexes;

  /* first values should be the results of default expressions */
  no_default_expr = insert->no_default_expr;
  if (no_default_expr < 0)
    {
      no_default_expr = 0;
    }

  for (k = 0; k < no_default_expr; k++)
    {
      OR_ATTRIBUTE *attr;
      DB_VALUE *new_val;
      DB_VALUE unix_ts;
      int error = NO_ERROR;
      TP_DOMAIN_STATUS status = DOMAIN_COMPATIBLE;

      DB_MAKE_NULL (&unix_ts);

      attr = heap_locate_last_attrepr (insert->att_id[k], &attr_info);
      if (attr == NULL)
	{
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}
      new_val = (DB_VALUE *) db_private_alloc (thread_p, sizeof (DB_VALUE));
      if (new_val == NULL)
	{
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}
      DB_MAKE_NULL (new_val);
      insert->vals[k] = new_val;
      switch (attr->current_default_value.default_expr)
	{
	case DB_DEFAULT_SYSDATE:
	  DB_MAKE_DATE (insert->vals[k], 1, 1, 1);
	  insert->vals[k]->data.date = xasl_state->vd.sys_datetime.date;
	  break;
	case DB_DEFAULT_SYSDATETIME:
	  DB_MAKE_DATETIME (insert->vals[k], &xasl_state->vd.sys_datetime);
	  break;
	case DB_DEFAULT_UNIX_TIMESTAMP:
	  DB_MAKE_DATETIME (insert->vals[k], &xasl_state->vd.sys_datetime);

	  (void) pr_clear_value (&unix_ts);

	  error = db_unix_timestamp (insert->vals[k], &unix_ts);
	  if (error == NO_ERROR)
	    {
	      /* check for unix_timestamp success */
	      if (!DB_IS_NULL (&unix_ts))
		{
		  (void) pr_clear_value (insert->vals[k]);
		  pr_clone_value (&unix_ts, insert->vals[k]);
		}
	    }

	  (void) pr_clear_value (&unix_ts);
	  break;
	case DB_DEFAULT_USER:
	  {
	    LOG_TDES *tdes = NULL;
	    char *temp = NULL;

	    tdes = logtb_get_current_tdes (thread_p);
	    if (tdes)
	      {
		int len =
		  strlen (tdes->client.db_user) +
		  strlen (tdes->client.host_name) + 2;
		temp = (char *) malloc (len);
		if (!temp)
		  {
		    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			    ER_OUT_OF_VIRTUAL_MEMORY, 1, len);
		  }
		else
		  {
		    strcpy (temp, tdes->client.db_user);
		    strcat (temp, "@");
		    strcat (temp, tdes->client.host_name);
		  }
	      }
	    DB_MAKE_STRING (insert->vals[k], temp);
	    insert->vals[k]->need_clear = true;
	  }
	  break;
	case DB_DEFAULT_CURR_USER:
	  {
	    LOG_TDES *tdes = NULL;
	    char *temp = NULL;

	    tdes = logtb_get_current_tdes (thread_p);
	    if (tdes != NULL)
	      {
		temp = tdes->client.db_user;
	      }
	    DB_MAKE_STRING (insert->vals[k], temp);
	  }
	  break;
	case DB_DEFAULT_NONE:
	  if (attr->current_default_value.val_length <= 0)
	    {
	      /* leave default value as NULL */
	      break;
	    }
	  else
	    {
	      OR_BUF buf;
	      PR_TYPE *pr_type = PR_TYPE_FROM_ID (attr->type);
	      bool copy = (pr_is_set_type (attr->type)) ? true : false;
	      if (pr_type != NULL)
		{
		  or_init (&buf, attr->current_default_value.value,
			   attr->current_default_value.val_length);
		  buf.error_abort = 1;
		  switch (_setjmp (buf.env))
		    {
		    case 0:
		      error =
			(*(pr_type->data_readval)) (&buf, insert->vals[k],
						    attr->domain,
						    attr->
						    current_default_value.
						    val_length, copy);
		      if (error != NO_ERROR)
			{
			  QEXEC_GOTO_EXIT_ON_ERROR;
			}
		      break;
		    default:
		      error = ER_FAILED;
		      QEXEC_GOTO_EXIT_ON_ERROR;
		      break;
		    }
		}
	    }
	  break;
	default:
	  assert (0);
	  error = ER_FAILED;
	  QEXEC_GOTO_EXIT_ON_ERROR;
	  break;
	}

      if (error != NO_ERROR)
	{
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}

      if (attr->current_default_value.default_expr == DB_DEFAULT_NONE)
	{
	  /* skip the value cast */
	  continue;
	}

      status =
	tp_value_coerce (insert->vals[k], insert->vals[k], attr->domain);
      if (status != DOMAIN_COMPATIBLE)
	{
	  (void) tp_domain_status_er_set (status, ARG_FILE_LINE,
					  insert->vals[k], attr->domain);
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}
    }

  specp = xasl->spec_list;
  if (specp)
    {
      /* we are inserting multiple values ...
       * ie. insert into foo select ... */

      if (locator_start_force_scan_cache (thread_p, &scan_cache,
					  &insert->class_hfid, 0,
					  &class_oid) != NO_ERROR)
	{
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}
      scan_cache_inited = true;

      /* readonly_scan = true */
      if (qexec_open_scan (thread_p, specp, xasl->val_list,
			   &xasl_state->vd, true, specp->fixed_scan,
			   &specp->s_id, xasl_state->query_id,
			   false) != NO_ERROR)
	{
	  if (savepoint_used)
	    {
	      xtran_server_end_topop (thread_p, LOG_RESULT_TOPOP_ABORT, &lsa);
	    }
	  qexec_failure_line (__LINE__, xasl_state);
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}

      while ((xb_scan = qexec_next_scan_block_iterations (thread_p,
							  xasl)) == S_SUCCESS)
	{
	  s_id = &xasl->curr_spec->s_id;
	  while ((ls_scan = scan_next_scan (thread_p, s_id)) == S_SUCCESS)
	    {
	      for (k = no_default_expr, vallist = s_id->val_list->valp;
		   k < val_no; k++, vallist = vallist->next)
		{
		  if (vallist == NULL || vallist->val == NULL)
		    {
		      assert (0);
		      QEXEC_GOTO_EXIT_ON_ERROR;
		    }

		  insert->vals[k] = vallist->val;
		}

	      /* evaluate constraint predicate */
	      satisfies_constraints = V_UNKNOWN;
	      if (insert->cons_pred != NULL)
		{
		  satisfies_constraints = eval_pred (thread_p,
						     insert->cons_pred,
						     &xasl_state->vd, NULL);
		  if (satisfies_constraints == V_ERROR)
		    {
		      QEXEC_GOTO_EXIT_ON_ERROR;
		    }
		}

	      if (insert->cons_pred != NULL
		  && satisfies_constraints != V_TRUE)
		{
		  /* currently there are only NOT NULL constraints */
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_NULL_CONSTRAINT_VIOLATION, 0);
		  QEXEC_GOTO_EXIT_ON_ERROR;
		}

	      if (heap_attrinfo_clear_dbvalues (&attr_info) != NO_ERROR)
		{
		  QEXEC_GOTO_EXIT_ON_ERROR;
		}
	      for (k = 0; k < val_no; ++k)
		{
		  if (DB_IS_NULL (insert->vals[k]))
		    {
		      OR_ATTRIBUTE *attr =
			heap_locate_last_attrepr (insert->att_id[k],
						  &attr_info);
		      if (attr == NULL)
			{
			  QEXEC_GOTO_EXIT_ON_ERROR;
			}
		    }

		  rc = heap_attrinfo_set (NULL, insert->att_id[k],
					  insert->vals[k], &attr_info);
		  if (rc != NO_ERROR)
		    {
		      QEXEC_GOTO_EXIT_ON_ERROR;
		    }
		}

	      if (insert->do_replace && insert->has_uniques)
		{
		  int removed_count = 0;

		  assert (index_attr_info_inited == true);
		  error = qexec_remove_duplicates_for_replace (thread_p,
							       &scan_cache,
							       &oid,
							       &attr_info,
							       &index_attr_info,
							       &idx_info,
							       &removed_count);
		  if (error != NO_ERROR)
		    {
		      QEXEC_GOTO_EXIT_ON_ERROR;
		    }
		  xasl->list_id->tuple_cnt += removed_count;
		}

	      if (odku_assignments && insert->has_uniques)
		{
		  force_count = 0;
		  error =
		    qexec_execute_duplicate_key_update (thread_p,
							insert->odku,
							&insert->class_hfid,
							&oid, &xasl_state->vd,
							&scan_cache,
							&attr_info,
							&index_attr_info,
							&idx_info,
							&force_count);
		  if (error != NO_ERROR)
		    {
		      QEXEC_GOTO_EXIT_ON_ERROR;
		    }
		  if (force_count != 0)
		    {
		      assert (force_count == 1);
		      xasl->list_id->tuple_cnt += force_count * 2;
		      continue;
		    }
		}

	      force_count = 0;
	      if (locator_attribute_info_force (thread_p, &insert->class_hfid,
						&oid, &attr_info,
						NULL, 0, LC_FLUSH_INSERT,
						&scan_cache, &force_count) !=
		  NO_ERROR)
		{
		  QEXEC_GOTO_EXIT_ON_ERROR;
		}
	      /* restore class oid and hfid that might have changed in the
	       * call above */
#if 1				/* TODO - trace */
	      assert (HFID_EQ (&insert->class_hfid, &class_hfid));
	      assert (OID_EQ (&(attr_info.class_oid), &class_oid));
#endif
	      HFID_COPY (&insert->class_hfid, &class_hfid);
	      COPY_OID (&(attr_info.class_oid), &class_oid);
	      /* Instances are not put into the result list file,
	       * but are counted. */
	      if (force_count)
		{
		  assert (force_count == 1);

		  xasl->list_id->tuple_cnt += force_count;
		}
	    }

	  if (ls_scan != S_END)
	    {
	      QEXEC_GOTO_EXIT_ON_ERROR;
	    }
	}

      if (xb_scan != S_END)
	{
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}
      qexec_close_scan (thread_p, specp);
    }
  else
    {
      /* we are inserting a single row
       * ie. insert into foo values(...) */
      REGU_VARIABLE_LIST regu_list = NULL;

      if (locator_start_force_scan_cache (thread_p, &scan_cache,
					  &insert->class_hfid,
					  insert->force_page_allocation,
					  &class_oid) != NO_ERROR)
	{
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}
      scan_cache_inited = true;

      if (XASL_IS_FLAGED (xasl, XASL_LINK_TO_REGU_VARIABLE))
	{
	  /* do not allow references to reusable oids in sub-inserts.
	   * this is a safety check and should have been detected at semantic
	   * level
	   */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_REFERENCE_TO_NON_REFERABLE_NOT_ALLOWED, 0);
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}

      for (i = 0; i < insert->no_val_lists; i++)
	{
	  for (regu_list = insert->valptr_lists[i]->valptrp, vallist =
	       xasl->val_list->valp, k = no_default_expr; k < val_no;
	       k++, regu_list = regu_list->next, vallist = vallist->next)
	    {
	      if (fetch_peek_dbval
		  (thread_p, &regu_list->value, &xasl_state->vd,
		   NULL, NULL, &valp) != NO_ERROR)
		{
		  QEXEC_GOTO_EXIT_ON_ERROR;
		}
	      if (!qdata_copy_db_value (vallist->val, valp))
		{
		  QEXEC_GOTO_EXIT_ON_ERROR;
		}
	      insert->vals[k] = valp;
	    }

	  /* evaluate constraint predicate */
	  satisfies_constraints = V_UNKNOWN;
	  if (insert->cons_pred != NULL)
	    {
	      satisfies_constraints = eval_pred (thread_p, insert->cons_pred,
						 &xasl_state->vd, NULL);
	      if (satisfies_constraints == V_ERROR)
		{
		  QEXEC_GOTO_EXIT_ON_ERROR;
		}
	    }

	  if (insert->cons_pred != NULL && satisfies_constraints != V_TRUE)
	    {
	      /* currently there are only NOT NULL constraints */
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_NULL_CONSTRAINT_VIOLATION, 0);
	      QEXEC_GOTO_EXIT_ON_ERROR;
	    }

	  if (heap_attrinfo_clear_dbvalues (&attr_info) != NO_ERROR)
	    {
	      QEXEC_GOTO_EXIT_ON_ERROR;
	    }

	  for (k = 0; k < val_no; ++k)
	    {
	      if (DB_IS_NULL (insert->vals[k]))
		{
		  OR_ATTRIBUTE *attr =
		    heap_locate_last_attrepr (insert->att_id[k], &attr_info);
		  if (attr == NULL)
		    {
		      QEXEC_GOTO_EXIT_ON_ERROR;
		    }
		}

	      rc = heap_attrinfo_set (NULL, insert->att_id[k],
				      insert->vals[k], &attr_info);
	      if (rc != NO_ERROR)
		{
		  QEXEC_GOTO_EXIT_ON_ERROR;
		}
	    }

	  if (insert->do_replace && insert->has_uniques)
	    {
	      int removed_count = 0;
	      assert (index_attr_info_inited == true);
	      error =
		qexec_remove_duplicates_for_replace (thread_p, &scan_cache,
						     &oid, &attr_info,
						     &index_attr_info,
						     &idx_info,
						     &removed_count);
	      if (error != NO_ERROR)
		{
		  QEXEC_GOTO_EXIT_ON_ERROR;
		}
	      xasl->list_id->tuple_cnt += removed_count;
	    }

	  force_count = 0;
	  if (odku_assignments && insert->has_uniques)
	    {
	      error = qexec_execute_duplicate_key_update (thread_p,
							  insert->odku,
							  &insert->class_hfid,
							  &oid,
							  &xasl_state->vd,
							  &scan_cache,
							  &attr_info,
							  &index_attr_info,
							  &idx_info,
							  &force_count);
	      if (error != NO_ERROR)
		{
		  QEXEC_GOTO_EXIT_ON_ERROR;
		}

	      if (force_count)
		{
		  assert (force_count == 1);
		  xasl->list_id->tuple_cnt += force_count * 2;
		}
	    }

	  if (force_count == 0)
	    {
	      if (locator_attribute_info_force (thread_p, &insert->class_hfid,
						&oid, &attr_info,
						NULL, 0, LC_FLUSH_INSERT,
						&scan_cache, &force_count) !=
		  NO_ERROR)
		{
		  QEXEC_GOTO_EXIT_ON_ERROR;
		}

	      /* Instances are not put into the result list file, but are counted. */
	      if (force_count)
		{
		  assert (force_count == 1);

		  xasl->list_id->tuple_cnt += force_count;
		}
	    }

	  if (XASL_IS_FLAGED (xasl, XASL_LINK_TO_REGU_VARIABLE))
	    {
#if 1
	      /* this must be a sub-insert
	       */
	      assert (false);	/* not permit */
	      QEXEC_GOTO_EXIT_ON_ERROR;
#else
	      /* this must be a sub-insert, and the inserted OID must be
	       * saved to obj_oid in insert_proc
	       */
	      assert (force_count == 1);
	      if (force_count != 1)
		{
		  QEXEC_GOTO_EXIT_ON_ERROR;
		}
	      db_make_oid (insert->obj_oid, &oid);
	      /* Clear the list id */
	      qfile_clear_list_id (xasl->list_id);
#endif
	    }
	}
    }

  if (index_attr_info_inited)
    {
      (void) heap_attrinfo_end (thread_p, &index_attr_info);
    }
  if (attr_info_inited)
    {
      (void) heap_attrinfo_end (thread_p, &attr_info);
    }
  if (scan_cache_inited)
    {
      (void) locator_end_force_scan_cache (thread_p, &scan_cache);
    }
  if (savepoint_used)
    {
      if (xtran_server_end_topop (thread_p, LOG_RESULT_TOPOP_ATTACH_TO_OUTER,
				  &lsa) != TRAN_ACTIVE)
	{
	  qexec_failure_line (__LINE__, xasl_state);
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}
    }

  for (k = 0; k < no_default_expr; k++)
    {
      pr_clear_value (insert->vals[k]);
      db_private_free_and_init (thread_p, insert->vals[k]);
    }

  if (odku_assignments && insert->has_uniques)
    {
      heap_attrinfo_end (thread_p, odku_assignments->attr_info);
    }

  return NO_ERROR;

exit_on_error:
  for (k = 0; k < no_default_expr; k++)
    {
      pr_clear_value (insert->vals[k]);
      db_private_free_and_init (thread_p, insert->vals[k]);
    }
  qexec_end_scan (thread_p, specp);
  qexec_close_scan (thread_p, specp);
  if (index_attr_info_inited)
    {
      (void) heap_attrinfo_end (thread_p, &index_attr_info);
    }
  if (attr_info_inited)
    {
      (void) heap_attrinfo_end (thread_p, &attr_info);
    }
  if (odku_attr_info_inited)
    {
      (void) heap_attrinfo_end (thread_p, odku_assignments->attr_info);
    }
  if (scan_cache_inited)
    {
      (void) locator_end_force_scan_cache (thread_p, &scan_cache);
    }
  if (savepoint_used)
    {
      xtran_server_end_topop (thread_p, LOG_RESULT_TOPOP_ABORT, &lsa);
    }
  if (XASL_IS_FLAGED (xasl, XASL_LINK_TO_REGU_VARIABLE)
      && xasl->list_id != NULL)
    {
      qfile_clear_list_id (xasl->list_id);
    }

  return ER_FAILED;
}

/*
 * qexec_init_instnum_val () -
 *   return: NO_ERROR, or ER_code
 *   xasl(in)   : XASL Tree pointer
 *   thread_p   : Thread entry pointer
 *   xasl_state(in)     : XASL tree state information
 *
 * Note: This routine initializes the instnum value used in execution
 *       to evaluate rownum() predicates.
 *       Usually the value is set to 0, so that the first row number will be 1,
 *       but for single table index scans that have a keylimit with a lower
 *       value, we initialize instnum_val with keylimit's low value.
 *       Otherwise, keylimit would skip some rows and instnum will start
 *       counting from 1, which is wrong.
 */
static int
qexec_init_instnum_val (XASL_NODE * xasl, THREAD_ENTRY * thread_p,
			XASL_STATE * xasl_state)
{
  TP_DOMAIN *domainp = tp_domain_resolve_default (DB_TYPE_BIGINT);
  DB_TYPE orig_type;
  REGU_VARIABLE *key_limit_l;
  DB_VALUE *dbvalp;

  assert (xasl && xasl->instnum_val);
  DB_MAKE_BIGINT (xasl->instnum_val, 0);

  if (xasl->save_instnum_val)
    {
      DB_MAKE_BIGINT (xasl->save_instnum_val, 0);
    }

  /* Single table, index scan, with keylimit that has lower value */
  if (xasl->scan_ptr == NULL &&
      xasl->spec_list != NULL && xasl->spec_list->next == NULL &&
      xasl->spec_list->access == INDEX &&
      xasl->spec_list->indexptr &&
      xasl->spec_list->indexptr->key_info.key_limit_l)
    {
      key_limit_l = xasl->spec_list->indexptr->key_info.key_limit_l;
      if (fetch_peek_dbval (thread_p, key_limit_l, &xasl_state->vd,
			    NULL, NULL, &dbvalp) != NO_ERROR)
	{
	  goto exit_on_error;
	}

      orig_type = DB_VALUE_DOMAIN_TYPE (dbvalp);
      if (orig_type != DB_TYPE_BIGINT)
	{
	  if (DB_IS_NULL (dbvalp)
	      || tp_value_coerce (dbvalp, dbvalp,
				  domainp) != DOMAIN_COMPATIBLE)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
		      pr_type_name (orig_type),
		      pr_type_name (TP_DOMAIN_TYPE (domainp)));
	      goto exit_on_error;
	    }

	  assert (DB_VALUE_DOMAIN_TYPE (dbvalp) == DB_TYPE_BIGINT);
	}

      (void) pr_clear_value (xasl->instnum_val);
      if (pr_clone_value (dbvalp, xasl->instnum_val) != NO_ERROR)
	{
	  goto exit_on_error;
	}

      if (xasl->save_instnum_val)
	{
	  (void) pr_clear_value (xasl->save_instnum_val);
	  if (pr_clone_value (dbvalp, xasl->save_instnum_val) != NO_ERROR)
	    {
	      goto exit_on_error;
	    }
	}
    }

  xasl->instnum_flag &=
    (0xff - (XASL_INSTNUM_FLAG_SCAN_CHECK + XASL_INSTNUM_FLAG_SCAN_STOP));

  return NO_ERROR;

exit_on_error:
  return ER_FAILED;
}

/*
 * qexec_start_mainblock_iterations () -
 *   return: NO_ERROR, or ER_code
 *   xasl(in)   : XASL Tree pointer
 *   xasl_state(in)     : XASL tree state information
 *
 * Note: This routines performs the start-up operations for a main
 * procedure block iteration. The main procedure block nodes can
 * be of type BUILDLIST_PROC, BUILDVALUE, UNION_PROC,
 * DIFFERENCE_PROC and INTERSECTION_PROC.
 */
int
qexec_start_mainblock_iterations (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
				  XASL_STATE * xasl_state)
{
  QFILE_TUPLE_VALUE_TYPE_LIST type_list;
  QFILE_LIST_ID *t_list_id = NULL;
  int ls_flag = 0;

  switch (xasl->type)
    {
    case BUILDLIST_PROC:	/* start BUILDLIST_PROC iterations */
      {
	BUILDLIST_PROC_NODE *buildlist = &xasl->proc.buildlist;

	/* Initialize extendible hash file for SELECT statement generated for
	   multi UPDATE/DELETE */
	assert (EHID_IS_NULL (&(buildlist->upd_del_ehid)));
	if (QEXEC_IS_MULTI_TABLE_UPDATE_DELETE (xasl))
	  {
	    if (qexec_init_upddel_ehash_files (thread_p, xasl) != NO_ERROR)
	      {
		QEXEC_GOTO_EXIT_ON_ERROR;
	      }
	  }
	else
	  {
	    EHID_SET_NULL (&(buildlist->upd_del_ehid));
	  }

	/* initialize groupby_num() value for BUILDLIST_PROC */
	if (buildlist->g_grbynum_val)
	  {
	    DB_MAKE_BIGINT (buildlist->g_grbynum_val, 0);
	  }

	if (xasl->list_id->type_list.type_cnt == 0)
	  {
	    if (qdata_get_valptr_type_list (thread_p, xasl->outptr_list,
					    &type_list) != NO_ERROR)
	      {
		if (type_list.domp)
		  {
		    free_and_init (type_list.domp);
		  }
		QEXEC_GOTO_EXIT_ON_ERROR;
	      }


	    QFILE_SET_FLAG (ls_flag, QFILE_FLAG_ALL);
	    if (XASL_IS_FLAGED (xasl, XASL_TOP_MOST_XASL)
		&& XASL_IS_FLAGED (xasl, XASL_TO_BE_CACHED)
		&& buildlist->groupby_list == NULL
		&& (xasl->orderby_list == NULL
		    || XASL_IS_FLAGED (xasl, XASL_SKIP_ORDERBY_LIST))
		&& xasl->option != Q_DISTINCT)
	      {
		QFILE_SET_FLAG (ls_flag, QFILE_FLAG_RESULT_FILE);
	      }

	    t_list_id = qfile_open_list (thread_p, &type_list,
					 xasl->after_iscan_list,
					 xasl_state->query_id, ls_flag);
	    if (t_list_id == NULL)
	      {
		if (type_list.domp)
		  {
		    free_and_init (type_list.domp);
		  }
		if (t_list_id)
		  {
		    QFILE_FREE_AND_INIT_LIST_ID (t_list_id);
		  }
		QEXEC_GOTO_EXIT_ON_ERROR;
	      }

	    if (type_list.domp)
	      {
		free_and_init (type_list.domp);
	      }

	    if (qfile_copy_list_id (xasl->list_id, t_list_id, true) !=
		NO_ERROR)
	      {
		QFILE_FREE_AND_INIT_LIST_ID (t_list_id);
		QEXEC_GOTO_EXIT_ON_ERROR;
	      }			/* if */

	    QFILE_FREE_AND_INIT_LIST_ID (t_list_id);

	    if (xasl->orderby_list != NULL)
	      {
		if (qexec_setup_topn_proc (thread_p, xasl, &xasl_state->vd)
		    != NO_ERROR)
		  {
		    QEXEC_GOTO_EXIT_ON_ERROR;
		  }
	      }
	  }
	break;
      }

    case BUILDVALUE_PROC:	/* start BUILDVALUE_PROC iterations */
      {
	BUILDVALUE_PROC_NODE *buildvalue = &xasl->proc.buildvalue;

	/* set groupby_num() value as 1 for BUILDVALUE_PROC */
	if (buildvalue->grbynum_val)
	  {
	    DB_MAKE_BIGINT (buildvalue->grbynum_val, 1);
	  }

	/* initialize aggregation list */
	if (qdata_initialize_aggregate_list (thread_p,
					     buildvalue->agg_list,
					     xasl_state->query_id) !=
	    NO_ERROR)
	  {
	    QEXEC_GOTO_EXIT_ON_ERROR;
	  }
	break;
      }

    case UNION_PROC:
    case DIFFERENCE_PROC:
    case INTERSECTION_PROC:	/* start SET block iterations */
      {
	break;
      }

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
      QEXEC_GOTO_EXIT_ON_ERROR;
    }				/* switch */

  /* initialize inst_num() value, instnum_flag */
  if (xasl->instnum_val &&
      qexec_init_instnum_val (xasl, thread_p, xasl_state) != NO_ERROR)
    {
      goto exit_on_error;
    }

  /* initialize orderby_num() value */
  if (xasl->ordbynum_val)
    {
      DB_MAKE_BIGINT (xasl->ordbynum_val, 0);
    }

  return NO_ERROR;

exit_on_error:

  if (xasl->type == BUILDLIST_PROC)
    {
      if (!EHID_IS_NULL (&(xasl->proc.buildlist.upd_del_ehid)))
	{
	  qexec_destroy_upddel_ehash_files (thread_p, xasl);
	}
    }
  return ER_FAILED;
}

/*
 * qexec_end_buildvalueblock_iterations () -
 *   return: NO_ERROR or ER_code
 *   xasl(in)   : XASL Tree pointer
 *   xasl_state(in)     : XASL tree state information
 *   tplrec(out) : Tuple record descriptor to store result tuples
 *
 * Note: This routines performs the finish-up operations for BUILDVALUE
 * block iteration.
 */
static int
qexec_end_buildvalueblock_iterations (THREAD_ENTRY * thread_p,
				      XASL_NODE * xasl,
				      XASL_STATE * xasl_state,
				      QFILE_TUPLE_RECORD * tplrec)
{
  QFILE_LIST_ID *t_list_id = NULL;
  int status = NO_ERROR;
  int ls_flag = 0;
  QPROC_TPLDESCR_STATUS tpldescr_status;
  DB_LOGICAL ev_res = V_UNKNOWN;
  QFILE_LIST_ID *output = NULL;
  QFILE_TUPLE_VALUE_TYPE_LIST type_list;
  BUILDVALUE_PROC_NODE *buildvalue = &xasl->proc.buildvalue;

  /* make final pass on aggregate list nodes */
  if (buildvalue->agg_list
      && qdata_finalize_aggregate_list (thread_p,
					buildvalue->agg_list,
					&xasl_state->vd, false) != NO_ERROR)
    {
      QEXEC_GOTO_EXIT_ON_ERROR;
    }

  /* evaluate having predicate */
  if (buildvalue->having_pred != NULL)
    {
      ev_res = eval_pred (thread_p, buildvalue->having_pred,
			  &xasl_state->vd, NULL);
      if (ev_res == V_ERROR)
	{
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}

      if (ev_res != V_TRUE
	  && qdata_set_valptr_list_unbound (thread_p, xasl->outptr_list,
					    &xasl_state->vd) != NO_ERROR)
	{
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}
    }

  /* a list of one tuple with a single value needs to be produced */
  if (qdata_get_valptr_type_list (thread_p, xasl->outptr_list, &type_list) !=
      NO_ERROR)
    {
      QEXEC_GOTO_EXIT_ON_ERROR;
    }

  /* If BUILDVALUE_PROC does not have 'order by'(xasl->orderby_list),
     then the list file to be open at here will be the last one.
     Otherwise, the last list file will be open at
     qexec_orderby_distinct().
     (Note that only one that can have 'group by' is BUILDLIST_PROC type.)
     And, the top most XASL is the other condition for the list file
     to be the last result file. */
  QFILE_SET_FLAG (ls_flag, QFILE_FLAG_ALL);
  if (XASL_IS_FLAGED (xasl, XASL_TOP_MOST_XASL) &&
      XASL_IS_FLAGED (xasl, XASL_TO_BE_CACHED) &&
      (xasl->orderby_list == NULL ||
       XASL_IS_FLAGED (xasl, XASL_SKIP_ORDERBY_LIST)) &&
      xasl->option != Q_DISTINCT)
    {
      QFILE_SET_FLAG (ls_flag, QFILE_FLAG_RESULT_FILE);
    }
  t_list_id =
    qfile_open_list (thread_p, &type_list, NULL, xasl_state->query_id,
		     ls_flag);
  if (t_list_id == NULL)
    {
      if (type_list.domp)
	{
	  free_and_init (type_list.domp);
	}
      QEXEC_GOTO_EXIT_ON_ERROR;
    }

      /***** WHAT IN THE WORLD IS THIS? *****/
  if (type_list.domp)
    {
      free_and_init (type_list.domp);
    }

  if (qfile_copy_list_id (xasl->list_id, t_list_id, true) != NO_ERROR)
    {
      QEXEC_GOTO_EXIT_ON_ERROR;
    }

  output = xasl->list_id;
  QFILE_FREE_AND_INIT_LIST_ID (t_list_id);

  if (buildvalue->having_pred == NULL || ev_res == V_TRUE)
    {
      tpldescr_status = qexec_generate_tuple_descriptor (thread_p,
							 xasl->list_id,
							 xasl->outptr_list,
							 &xasl_state->vd);
      if (tpldescr_status == QPROC_TPLDESCR_FAILURE)
	{
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}

      switch (tpldescr_status)
	{
	case QPROC_TPLDESCR_SUCCESS:
	  /* build tuple into the list file page */
	  if (qfile_generate_tuple_into_list
	      (thread_p, xasl->list_id, T_NORMAL) != NO_ERROR)
	    {
	      QEXEC_GOTO_EXIT_ON_ERROR;
	    }
	  break;

	case QPROC_TPLDESCR_RETRY_SET_TYPE:
	case QPROC_TPLDESCR_RETRY_BIG_REC:
	  if (tplrec->tpl == NULL)
	    {
	      /* allocate tuple descriptor */
	      tplrec->size = DB_PAGESIZE;
	      tplrec->tpl = (QFILE_TUPLE) malloc (DB_PAGESIZE);
	      if (tplrec->tpl == NULL)
		{
		  QEXEC_GOTO_EXIT_ON_ERROR;
		}
	    }

	  if (qdata_copy_valptr_list_to_tuple (thread_p, xasl->list_id,
					       xasl->outptr_list,
					       &xasl_state->vd,
					       tplrec) != NO_ERROR)
	    {
	      QEXEC_GOTO_EXIT_ON_ERROR;
	    }

	  if (qfile_add_tuple_to_list (thread_p, xasl->list_id, tplrec->tpl)
	      != NO_ERROR)
	    {
	      QEXEC_GOTO_EXIT_ON_ERROR;
	    }
	  break;

	default:
	  break;
	}
    }

end:

  QEXEC_CLEAR_AGG_LIST_VALUE (buildvalue->agg_list);
  if (t_list_id)
    {
      QFILE_FREE_AND_INIT_LIST_ID (t_list_id);
    }
  if (output)
    {
      qfile_close_list (thread_p, output);
    }

  return status;

exit_on_error:

  status = ER_FAILED;
  goto end;
}

/*
 * qexec_end_mainblock_iterations () -
 *   return: NO_ERROR, or ER_code
 *   xasl(in)   : XASL Tree pointer
 *   xasl_state(in)     : XASL tree state information
 *   tplrec(out) : Tuple record descriptor to store result tuples
 *
 * Note: This routines performs the finish-up operations for a main
 * procedure block iteration. The main procedure block nodes can
 * be of type BUILDLIST_PROC, BUILDVALUE, UNION_PROC,
 * DIFFERENCE_PROC and INTERSECTION_PROC.
 */
static int
qexec_end_mainblock_iterations (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
				XASL_STATE * xasl_state,
				QFILE_TUPLE_RECORD * tplrec)
{
  QFILE_LIST_ID *t_list_id = NULL;
  int status = NO_ERROR;
  bool distinct_needed;
  int ls_flag = 0;

  distinct_needed = (xasl->option == Q_DISTINCT) ? true : false;

  switch (xasl->type)
    {

    case BUILDLIST_PROC:	/* end BUILDLIST_PROC iterations */
      /* Destroy the extendible hash files for SELECT statement generated for
         UPDATE/DELETE */
      if (!EHID_IS_NULL (&(xasl->proc.buildlist.upd_del_ehid)))
	{
	  qexec_destroy_upddel_ehash_files (thread_p, xasl);
	}

      /* close the list file */
      qfile_close_list (thread_p, xasl->list_id);
      break;

    case BUILDVALUE_PROC:	/* end BUILDVALUE_PROC iterations */
      status =
	qexec_end_buildvalueblock_iterations (thread_p, xasl, xasl_state,
					      tplrec);
      break;

    case UNION_PROC:
    case DIFFERENCE_PROC:
    case INTERSECTION_PROC:
      if (xasl->type == UNION_PROC)
	{
	  QFILE_SET_FLAG (ls_flag, QFILE_FLAG_UNION);
	}
      else if (xasl->type == DIFFERENCE_PROC)
	{
	  QFILE_SET_FLAG (ls_flag, QFILE_FLAG_DIFFERENCE);
	}
      else
	{
	  QFILE_SET_FLAG (ls_flag, QFILE_FLAG_INTERSECT);
	}

      if (distinct_needed)
	{
	  QFILE_SET_FLAG (ls_flag, QFILE_FLAG_DISTINCT);
	}
      else
	{
	  QFILE_SET_FLAG (ls_flag, QFILE_FLAG_ALL);
	}

      /* For UNION_PROC, DIFFERENCE_PROC, and INTERSECTION_PROC,
         if they do not have 'order by'(xasl->orderby_list),
         then the list file to be open at here will be the last one.
         Otherwise, the last list file will be open at qexec_groupby()
         or qexec_orderby_distinct().
         (Note that only one that can have 'group by' is BUILDLIST_PROC type.)
         And, the top most XASL is the other condition for the list file
         to be the last result file. */

      if (XASL_IS_FLAGED (xasl, XASL_TOP_MOST_XASL)
	  && XASL_IS_FLAGED (xasl, XASL_TO_BE_CACHED)
	  && (xasl->orderby_list == NULL
	      || XASL_IS_FLAGED (xasl, XASL_SKIP_ORDERBY_LIST)))
	{
	  QFILE_SET_FLAG (ls_flag, QFILE_FLAG_RESULT_FILE);
	}

      t_list_id = qfile_combine_two_list (thread_p,
					  xasl->proc.union_.left->list_id,
					  xasl->proc.union_.right->list_id,
					  ls_flag);
      distinct_needed = false;
      if (!t_list_id)
	{
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}
      if (qfile_copy_list_id (xasl->list_id, t_list_id, true) != NO_ERROR)
	{
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}
      QFILE_FREE_AND_INIT_LIST_ID (t_list_id);
      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
      QEXEC_GOTO_EXIT_ON_ERROR;
    }				/* switch */

  /* DISTINCT processing (i.e, duplicates elimination) is performed at
   * qexec_orderby_distinct() after GROUP BY processing
   */

success:
  if (t_list_id)
    {
      QFILE_FREE_AND_INIT_LIST_ID (t_list_id);
    }
  return status;

exit_on_error:
  status = ER_FAILED;
  goto success;

}

/*
 * qexec_clear_mainblock_iterations () -
 *   return:
 *   xasl(in)   : XASL Tree pointer
 */
static void
qexec_clear_mainblock_iterations (THREAD_ENTRY * thread_p, XASL_NODE * xasl)
{
  AGGREGATE_TYPE *agg_p;

  switch (xasl->type)
    {
    case BUILDLIST_PROC:
      /* Destroy the extendible hash files for SELECT statement generated for
         UPDATE/DELETE */
      if (!EHID_IS_NULL (&(xasl->proc.buildlist.upd_del_ehid)))
	{
	  qexec_destroy_upddel_ehash_files (thread_p, xasl);
	}
      qfile_close_list (thread_p, xasl->list_id);
      break;

    case BUILDVALUE_PROC:
      for (agg_p = xasl->proc.buildvalue.agg_list; agg_p != NULL;
	   agg_p = agg_p->next)
	{
	  qfile_close_list (thread_p, agg_p->list_id);
	  qfile_destroy_list (thread_p, agg_p->list_id);
	}
      break;

    case UNION_PROC:
    case DIFFERENCE_PROC:
    case INTERSECTION_PROC:
    case SCAN_PROC:
    case UPDATE_PROC:
    case DELETE_PROC:
    case INSERT_PROC:
      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
      break;
    }

  return;
}

/*
 * qexec_execute_mainblock () -
 *   return: NO_ERROR, or ER_code
 *   xasl(in)   : XASL Tree pointer
 *   xasl_state(in)     : XASL state information
 *
 */
int
qexec_execute_mainblock (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
			 XASL_STATE * xasl_state)
{
  int error = NO_ERROR;
  bool on_trace;
  struct timeval start, end;
  UINT64 old_fetches = 0, old_ioreads = 0;

  if (thread_get_recursion_depth (thread_p)
      > prm_get_integer_value (PRM_ID_MAX_RECURSION_SQL_DEPTH))
    {
      error = ER_MAX_RECURSION_SQL_DEPTH;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
	      prm_get_integer_value (PRM_ID_MAX_RECURSION_SQL_DEPTH));
      return error;
    }
  thread_inc_recursion_depth (thread_p);

  on_trace = thread_is_on_trace (thread_p);
  if (on_trace)
    {
      gettimeofday (&start, NULL);
      old_fetches = mnt_get_stats (thread_p, MNT_STATS_DATA_PAGE_FETCHES);
      old_ioreads = mnt_get_stats (thread_p, MNT_STATS_DATA_PAGE_IOREADS);
    }

  error = qexec_execute_mainblock_internal (thread_p, xasl, xasl_state);

  if (on_trace)
    {
      gettimeofday (&end, NULL);
      ADD_TIMEVAL (xasl->xasl_stats.elapsed_time, start, end);
      xasl->xasl_stats.fetches +=
	mnt_get_stats (thread_p, MNT_STATS_DATA_PAGE_FETCHES) - old_fetches;
      xasl->xasl_stats.ioreads +=
	mnt_get_stats (thread_p, MNT_STATS_DATA_PAGE_IOREADS) - old_ioreads;
    }

  thread_dec_recursion_depth (thread_p);

  return error;
}

/*
 * qexec_check_modification () -
 *   return: NO_ERROR, or ER_code
 *   xasl(in)   : XASL Tree pointer
 *   xasl_state(in)     : XASL state information
 *
 */
static int
qexec_check_modification (THREAD_ENTRY * thread_p,
			  UNUSED_ARG XASL_NODE * xasl,
			  XASL_STATE * xasl_state)
{
  int error = NO_ERROR;
  int tran_index;
  QMGR_QUERY_ENTRY *query_p = NULL;
  XASL_CACHE_ENTRY *ent = NULL;

  CHECK_MODIFICATION_NO_RETURN (thread_p, error);
  if (error != NO_ERROR)
    {
      assert (error == ER_DB_NO_MODIFICATIONS);

      tran_index = logtb_get_current_tran_index (thread_p);

      query_p =
	qmgr_get_query_entry (thread_p, xasl_state->query_id, tran_index);
      assert (query_p != NULL);
      assert (query_p->xasl_ent != NULL);

      ent = query_p->xasl_ent;
      assert (ent->deletion_marker == false);

      er_log_debug (ARG_FILE_LINE, "sql_user_text = %s\n",
		    ent->sql_info.sql_user_text);
    }

  return error;
}

/*
 * qexec_execute_mainblock_internal () -
 *   return: NO_ERROR, or ER_code
 *   xasl(in)   : XASL Tree pointer
 *   xasl_state(in)     : XASL state information
 *
 */
static int
qexec_execute_mainblock_internal (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
				  XASL_STATE * xasl_state)
{
  XASL_NODE *xptr, *xptr2;
  QFILE_TUPLE_RECORD tplrec = { NULL, 0 };
  SCAN_CODE qp_scan;
  int level;
  ACCESS_SPEC_TYPE *specp;
  XASL_SCAN_FNC_PTR func_vector = (XASL_SCAN_FNC_PTR) NULL;
  int readonly_scan = true, readonly_scan2, multi_readonly_scan = false;
  XASL_NODE *fixed_scan_xasl = NULL;
  bool has_index_scan = false;
  int error;
  DB_LOGICAL limit_zero;
  bool scan_immediately_stop = false;

  /*
   * Pre_processing
   */

  if (xasl->limit_row_count)
    {
      limit_zero = eval_limit_count_is_0 (thread_p, xasl->limit_row_count,
					  &xasl_state->vd);
      if (limit_zero == V_ERROR)
	{
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}

      if (limit_zero == V_TRUE)
	{
	  if (XASL_IS_FLAGED (xasl, XASL_TOP_MOST_XASL))
	    {
	      er_log_debug (ARG_FILE_LINE,
			    "This statement has no record by 'limit 0' clause.\n");
	      return NO_ERROR;
	    }
	  else
	    {
	      scan_immediately_stop = true;
	    }
	}
    }

  switch (xasl->type)
    {
    case UPDATE_PROC:
      error = qexec_check_modification (thread_p, xasl, xasl_state);
      if (error != NO_ERROR)
	{
	  return error;
	}

      error = qexec_execute_update (thread_p, xasl, xasl_state);
      if (error != NO_ERROR)
	{
	  return error;
	}

      /* monitor */
      mnt_stats_counter (thread_p, MNT_STATS_QUERY_UPDATES, 1);
      break;

    case DELETE_PROC:
      error = qexec_check_modification (thread_p, xasl, xasl_state);
      if (error != NO_ERROR)
	{
	  return error;
	}

      error = qexec_execute_delete (thread_p, xasl, xasl_state);
      if (error != NO_ERROR)
	{
	  return error;
	}

      /* monitor */
      mnt_stats_counter (thread_p, MNT_STATS_QUERY_DELETES, 1);
      break;

    case INSERT_PROC:
      error = qexec_check_modification (thread_p, xasl, xasl_state);
      if (error != NO_ERROR)
	{
	  return error;
	}

      error = qexec_execute_insert (thread_p, xasl, xasl_state, false);
      if (error != NO_ERROR)
	{
	  return error;
	}

      /* monitor */
      mnt_stats_counter (thread_p, MNT_STATS_QUERY_INSERTS, 1);
      break;

    default:

      multi_readonly_scan = QEXEC_IS_MULTI_TABLE_UPDATE_DELETE (xasl);
      if (xasl->upd_del_class_cnt > 0 || multi_readonly_scan)
	{
	  readonly_scan = false;
	}

      /* evaluate all the aptr lists in all scans */
      for (xptr = xasl; xptr; xptr = xptr->scan_ptr)
	{

	  for (xptr2 = xptr->aptr_list; xptr2; xptr2 = xptr2->next)
	    {
	      if (XASL_IS_FLAGED (xptr2, XASL_LINK_TO_REGU_VARIABLE))
		{
		  /* skip if linked to regu var */
		  continue;
		}

	      if (xptr2->status == XASL_CLEARED
		  || xptr2->status == XASL_INITIALIZED)
		{
		  if (qexec_execute_mainblock (thread_p, xptr2, xasl_state) !=
		      NO_ERROR)
		    {
		      if (tplrec.tpl)
			{
			  free_and_init (tplrec.tpl);
			}
		      qexec_failure_line (__LINE__, xasl_state);
		      return ER_FAILED;
		    }
		}
	      else
		{		/* already executed. success or failure */
		  if (xptr2->status != XASL_SUCCESS)
		    {
		      if (tplrec.tpl)
			{
			  free_and_init (tplrec.tpl);
			}
		      qexec_failure_line (__LINE__, xasl_state);
		      return ER_FAILED;
		    }
		}
	    }
	}

      /* start main block iterations */
      if (qexec_start_mainblock_iterations (thread_p, xasl, xasl_state) !=
	  NO_ERROR)
	{
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}

      /*
       * Processing
       */
      /* Block out main part of query processing for performance profiling of
       * JDBC driver and CAS side. Main purpose of this modification is
       * to pretend that the server's scan time is very fast so that it affect
       * only little portion of whole turnaround time in the point of view
       * of the JDBC driver.
       */

      /* iterative processing is done only for XASL blocks that has
       * access specification list blocks.
       */
      if (xasl->spec_list)
	{
	  /* Decide which scan will use fixed flags and which won't.
	   * There are several cases here:
	   * 1. Do not use fixed scans if locks on objects are required.
	   * 2. Disable all fixed scans if any index scan is used (this is
	   *    legacy and should be reconsidered).
	   * 3. Disable fixed scan for outer scans. Fixed cannot be allowed
	   *    while new scans start which also need to fix pages. This may
	   *    lead to page deadlocks.
	   *
	   * NOTE: Only the innermost scans are allowed fixed scans.
	   */

	  assert (xasl->upd_del_class_cnt <= 1);

	  if (xasl->upd_del_class_cnt > 0)
	    {
	      ;			/* Fall through */
	    }
	  else
	    {
	      for (xptr = xasl; xptr; xptr = xptr->scan_ptr)
		{
		  specp = xptr->spec_list;
		  for (; specp; specp = specp->next)
		    {
		      if (specp->type == TARGET_CLASS)
			{
			  /* Update fixed scan XASL */
			  fixed_scan_xasl = xptr;
			  if (IS_ANY_INDEX_ACCESS (specp->access))
			    {
			      has_index_scan = true;
			      break;
			    }
			}
		    }
		  if (has_index_scan)
		    {
		      /* Stop search */
		      break;
		    }
		}
	    }

	  if (has_index_scan)
	    {
	      /* Index found, no fixed is allowed */
	      fixed_scan_xasl = NULL;
	    }
	  if (XASL_IS_FLAGED (xasl, XASL_NO_FIXED_SCAN))
	    {
	      /* no fixed scan if it was decided so during compilation */
	      fixed_scan_xasl = NULL;
	    }
	  if (xasl->dptr_list != NULL)
	    {
	      /* correlated subquery found, no fixed is allowed */
	      fixed_scan_xasl = NULL;
	    }
	  if (xasl->type == BUILDLIST_PROC
	      && xasl->proc.buildlist.eptr_list != NULL)
	    {
	      /* subquery in HAVING clause, can't have fixed scan */
	      fixed_scan_xasl = NULL;
	    }
	  for (xptr = xasl->aptr_list; xptr != NULL; xptr = xptr->next)
	    {
	      if (XASL_IS_FLAGED (xptr, XASL_LINK_TO_REGU_VARIABLE))
		{
		  /* uncorrelated query that is not pre-executed, but evaluated
		     in a reguvar; no fixed scan in this case */
		  fixed_scan_xasl = NULL;
		}
	    }

	  /* open all the scans that are involved within the query,
	   * for SCAN blocks
	   */
	  for (xptr = xasl, level = 0; xptr; xptr = xptr->scan_ptr, level++)
	    {
	      /* consider all the access specification nodes */
	      for (specp = xptr->spec_list; specp; specp = specp->next)
		{
		  specp->fixed_scan = (xptr == fixed_scan_xasl);

		  if (multi_readonly_scan)
		    {
		      assert (xasl->upd_del_class_cnt == 1);

		      if (xasl->upd_del_class_cnt > 0)
			{
			  readonly_scan2 = false;
			}
		      else
			{
#if 1
			  assert (false);	/* is impossible */
#endif
			  readonly_scan2 = true;
			}
		    }
		  else
		    {
		      readonly_scan2 = readonly_scan;
		    }

		  if (qexec_open_scan (thread_p, specp,
				       xptr->val_list,
				       &xasl_state->vd,
				       readonly_scan2,
				       specp->fixed_scan,
				       &specp->s_id,
				       xasl_state->query_id,
				       scan_immediately_stop) != NO_ERROR)
		    {
		      qexec_clear_mainblock_iterations (thread_p, xasl);
		      QEXEC_GOTO_EXIT_ON_ERROR;
		    }
		}
	    }

	  /* allocate xasl scan function vector */
	  func_vector =
	    (XASL_SCAN_FNC_PTR) malloc (level * sizeof (XSAL_SCAN_FUNC));
	  if (func_vector == NULL)
	    {
	      qexec_clear_mainblock_iterations (thread_p, xasl);
	      QEXEC_GOTO_EXIT_ON_ERROR;
	    }

	  for (xptr = xasl, level = 0; xptr; xptr = xptr->scan_ptr, level++)
	    {
	      /* set all the following scan blocks to off */
	      xasl->next_scan_block_on = false;

	      /* set the associated function with the scan */

	      /* Having more than one interpreter function was a bad
	       * idea, so I've removed the specialized ones. dkh.
	       */
	      if (level == 0)
		{
		  func_vector[level] = (XSAL_SCAN_FUNC) qexec_intprt_fnc;
		}
	      else
		{
		  func_vector[level] = (XSAL_SCAN_FUNC) qexec_execute_scan;
		  /* monitor */
		  mnt_stats_counter (thread_p, MNT_STATS_QUERY_NLJOINS, 1);
		}
	    }

	  if (query_multi_range_opt_check_set_sort_col (thread_p, xasl) !=
	      NO_ERROR)
	    {
	      qexec_clear_mainblock_iterations (thread_p, xasl);
	      QEXEC_GOTO_EXIT_ON_ERROR;
	    }

	  /* call the first xasl interpreter function */
	  qp_scan = (*func_vector[0]) (thread_p, xasl, xasl_state, &tplrec,
				       &func_vector[1]);

	  /* free the function vector */
	  free_and_init (func_vector);

	  /* close all the scans that are involved within the query */
	  for (xptr = xasl, level = 0; xptr; xptr = xptr->scan_ptr, level++)
	    {
	      for (specp = xptr->spec_list; specp; specp = specp->next)
		{
		  qexec_end_scan (thread_p, specp);
		  qexec_close_scan (thread_p, specp);
		}
	      if (xptr->curr_spec != NULL)
		{
		  xptr->curr_spec = NULL;
		}
	    }

	  if (qp_scan != S_SUCCESS)	/* error case */
	    {
	      qexec_clear_mainblock_iterations (thread_p, xasl);
	      QEXEC_GOTO_EXIT_ON_ERROR;
	    }
	}

      /* end main block iterations */
      if (qexec_end_mainblock_iterations (thread_p, xasl, xasl_state, &tplrec)
	  != NO_ERROR)
	{
	  qexec_clear_mainblock_iterations (thread_p, xasl);
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}

      /*
       * Post_processing
       */

      /*
       * DISTINCT processing caused by statement set operators(UNION,
       * DIFFERENCE, INTERSECTION) has already taken place now.
       * But, in the other cases, DISTINCT are not processed yet.
       * qexec_orderby_distinct() will handle it.
       */

      /* GROUP BY processing */

      /* if groupby skip, we compute group by from the already sorted list */
      if (xasl->spec_list && xasl->spec_list->indexptr &&
	  xasl->spec_list->indexptr->groupby_skip)
	{
	  if (qexec_groupby_index (thread_p, xasl, xasl_state, &tplrec)
	      != NO_ERROR)
	    {
	      QEXEC_GOTO_EXIT_ON_ERROR;
	    }
	}
      else if (xasl->type == BUILDLIST_PROC	/* it is SELECT query */
	       && xasl->proc.buildlist.groupby_list)	/* it has GROUP BY clause */
	{
	  if (qexec_groupby (thread_p, xasl, xasl_state, &tplrec) != NO_ERROR)
	    {
	      QEXEC_GOTO_EXIT_ON_ERROR;
	    }
	}

      /* ORDER BY and DISTINCT processing */
      if (xasl->type == UNION_PROC
	  || xasl->type == DIFFERENCE_PROC || xasl->type == INTERSECTION_PROC)
	{
	  /* DISTINCT was already processed in these cases. Consider only
	     ORDER BY */
	  if (xasl->orderby_list	/* it has ORDER BY clause */
	      && (xasl->list_id->tuple_cnt > 1	/* the result has more than one tuple */
		  || xasl->ordbynum_val != NULL))	/* ORDERBY_NUM() is used */
	    {
	      /* It has ORDER BY clause and the result has more than one
	         tuple. We cannot skip the processing some cases such as
	         'orderby_num() < 1', for example. */
	      if (qexec_orderby_distinct (thread_p, xasl, Q_ALL, xasl_state)
		  != NO_ERROR)
		{
		  QEXEC_GOTO_EXIT_ON_ERROR;
		}
	    }
	}
      else
	{
	  /* DISTINCT & ORDER BY
	     check orderby_list flag for skipping order by */
	  if ((xasl->orderby_list	/* it has ORDER BY clause */
	       && (!XASL_IS_FLAGED (xasl, XASL_SKIP_ORDERBY_LIST)	/* cannot skip */
		   || XASL_IS_FLAGED (xasl, XASL_USES_MRO))	/* MRO must go on */
	       && (xasl->list_id->tuple_cnt > 1	/* the result has more than one tuple */
		   || xasl->ordbynum_val != NULL	/* ORDERBY_NUM() is used */
		   || xasl->topn_items != NULL))	/* used internal sort */
	      || (xasl->option == Q_DISTINCT))	/* DISTINCT must be go on */
	    {
	      if (qexec_orderby_distinct (thread_p, xasl, xasl->option,
					  xasl_state) != NO_ERROR)
		{
		  QEXEC_GOTO_EXIT_ON_ERROR;
		}
	    }
	}

      /* monitor */
      mnt_stats_counter (thread_p, MNT_STATS_QUERY_SELECTS, 1);
      break;
    }

  if (xasl->is_single_tuple)
    {
      if (xasl->list_id->tuple_cnt > 1)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_INVALID_QRY_SINGLE_TUPLE, 0);
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}

      if (xasl->single_tuple
	  && (qdata_get_single_tuple_from_list_id (thread_p, xasl->list_id,
						   xasl->single_tuple) !=
	      NO_ERROR))
	{
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}
    }

  /*
   * Cleanup and Exit processing
   */

  /* clear only non-zero correlation-level uncorrelated subquery list files */
  for (xptr = xasl; xptr; xptr = xptr->scan_ptr)
    {
      if (xptr->aptr_list)
	{
	  qexec_clear_head_lists (thread_p, xptr->aptr_list);
	}
    }
  if (tplrec.tpl)
    {
      free_and_init (tplrec.tpl);
    }

  xasl->status = XASL_SUCCESS;

  return NO_ERROR;

  /*
   * Error processing
   */
exit_on_error:
#if defined(SERVER_MODE)
  /* query execution error must be set up before qfile_close_list(). */
  if (er_errid () < 0)
    {
      qmgr_set_query_error (thread_p, xasl_state->query_id);
    }
#endif
  qfile_close_list (thread_p, xasl->list_id);
  if (func_vector)
    {
      free_and_init (func_vector);
    }

  /* close all the scans that are involved within the query */
  for (xptr = xasl, level = 0; xptr; xptr = xptr->scan_ptr, level++)
    {
      for (specp = xptr->spec_list; specp; specp = specp->next)
	{
	  qexec_end_scan (thread_p, specp);
	  qexec_close_scan (thread_p, specp);
	}
      if (xptr->curr_spec != NULL)
	{
	  xptr->curr_spec = NULL;
	}
    }

  if (tplrec.tpl)
    {
      free_and_init (tplrec.tpl);
    }

  xasl->status = XASL_FAILURE;

  qexec_failure_line (__LINE__, xasl_state);
  return ER_FAILED;
}

/*
 * qexec_update_index_stats () -
 *    return: error code
 */
int
qexec_update_index_stats (THREAD_ENTRY * thread_p,
			  CIRP_CT_INDEX_STATS * index_stats)
{
  DB_IDXKEY key;
  int error = NO_ERROR;

  DB_IDXKEY_MAKE_NULL (&key);

  /* make pkey idxkey */
  key.size = 2;
  db_make_string (&key.vals[0], index_stats->table_name);
  db_make_string (&key.vals[1], index_stats->index_name);

  error = qexec_modify_catalog_table (thread_p, &key, index_stats,
				      &table_IndexStats,
				      CT_MODIFY_MODE_UPSERT);

  db_idxkey_clear (&key);

  return error;
}

/*
 * qexec_insert_gid_skey_info () -
 *    return: error code
 *
 *    shard_groupid(in):
 *    shard_key(in):
 */
static int
qexec_insert_gid_skey_info (THREAD_ENTRY * thread_p,
			    int shard_groupid, DB_VALUE * shard_key)
{
  DB_IDXKEY key;
  CT_GID_SKEY ct_gid_skey;
  int error = NO_ERROR;

  assert (shard_key != NULL);

  DB_IDXKEY_MAKE_NULL (&key);

  /* make pkey idxkey */
  key.size = 2;
  db_make_int (&key.vals[0], ABS (shard_groupid));
  db_value_clone (shard_key, &key.vals[1]);

  /* make data */
  ct_gid_skey.gid = ABS (shard_groupid);
  ct_gid_skey.skey = shard_key;

  error = qexec_modify_catalog_table (thread_p, &key, &ct_gid_skey,
				      &table_ShardGidSkey,
				      CT_MODIFY_MODE_INSERT);

  db_idxkey_clear (&key);

  return error;
}

/*
 * qexec_upsert_writer_info () -
 *    return: error code
 *
 *    pk(in):
 *    class_oid(in):
 *    hfid(in):
 *    analyzer(in):
 */
int
qexec_upsert_writer_info (THREAD_ENTRY * thread_p, DB_IDXKEY * pk,
			  CIRP_CT_LOG_WRITER * writer)
{
  int error = NO_ERROR;

  error = qexec_modify_catalog_table (thread_p, pk, writer,
				      &table_LogWriter,
				      CT_MODIFY_MODE_UPSERT);

  return error;
}

/*
 * qexec_update_writer_info () -
 *    return: error code
 *
 *    pk(in):
 *    class_oid(in):
 *    hfid(in):
 *    applier(in):
 */
int
qexec_update_writer_info (THREAD_ENTRY * thread_p, DB_IDXKEY * pk,
			  CIRP_CT_LOG_WRITER * writer)
{
  int error = NO_ERROR;

  error = qexec_modify_catalog_table (thread_p, pk, writer,
				      &table_LogWriter,
				      CT_MODIFY_MODE_UPDATE);

  return error;
}

/*
 * qexec_upsert_applier_info () -
 *    return: error code
 *
 *    pk(in):
 *    class_oid(in):
 *    hfid(in):
 *    analyzer(in):
 */
int
qexec_upsert_applier_info (THREAD_ENTRY * thread_p, DB_IDXKEY * pk,
			   CIRP_CT_LOG_APPLIER * applier)
{
  int error = NO_ERROR;

  error = qexec_modify_catalog_table (thread_p, pk, applier,
				      &table_LogApplier,
				      CT_MODIFY_MODE_UPSERT);

  return error;
}

/*
 * qexec_update_applier_info () -
 *    return: error code
 *
 *    pk(in):
 *    class_oid(in):
 *    hfid(in):
 *    applier(in):
 */
int
qexec_update_applier_info (THREAD_ENTRY * thread_p, DB_IDXKEY * pk,
			   CIRP_CT_LOG_APPLIER * applier)
{
  int error = NO_ERROR;

  error = qexec_modify_catalog_table (thread_p, pk, applier,
				      &table_LogApplier,
				      CT_MODIFY_MODE_UPDATE);

  return error;
}

/*
 * qexec_upsert_analyzer_info () -
 *    return: error code
 *
 *    pk(in):
 *    class_oid(in):
 *    hfid(in):
 *    analyzer(in):
 */
int
qexec_upsert_analyzer_info (THREAD_ENTRY * thread_p, DB_IDXKEY * pk,
			    CIRP_CT_LOG_ANALYZER * analyzer)
{
  int error = NO_ERROR;

  error = qexec_modify_catalog_table (thread_p, pk, analyzer,
				      &table_LogAnalyzer,
				      CT_MODIFY_MODE_UPSERT);

  return error;
}

/*
 * qexec_update_analyzer_info () -
 *    return: error code
 *
 *    pk(in):
 *    class_oid(in):
 *    hfid(in):
 *    analyzer(in):
 */
int
qexec_update_analyzer_info (THREAD_ENTRY * thread_p, DB_IDXKEY * pk,
			    CIRP_CT_LOG_ANALYZER * analyzer)
{
  int error = NO_ERROR;

  error = qexec_modify_catalog_table (thread_p, pk, analyzer,
				      &table_LogAnalyzer,
				      CT_MODIFY_MODE_UPDATE);

  return error;
}

/*
 * qexec_modify_catalog_table () -
 *    return: error code
 */
static int
qexec_modify_catalog_table (THREAD_ENTRY * thread_p, DB_IDXKEY * pk,
			    void *ct_data, CATCLS_TABLE * table,
			    CT_MODIFY_MODE modify_mode)
{
#define OP_INSERT 1
#define OP_UPDATE 2

  OID oid;
  BTREE_SEARCH found_status;
  int error = NO_ERROR;
  int op_mode = 0;
  int force_page_allocation = 0;

  DB_IDXKEY values;
  CT_CACHE_INFO *ct_cache = NULL;

  DB_IDXKEY_MAKE_NULL (&values);

  ct_cache = qexec_get_info_of_catalog_table (thread_p, table);
  if (ct_cache == NULL)
    {
      error = er_errid ();
      GOTO_EXIT_ON_ERROR;
    }

#if !defined(NDEBUG)
  {
    int i;

    for (i = 0; i < table->num_columns; i++)
      {
	assert (ct_cache->atts[i] != NULL_ATTRID);
      }
  }
#endif

  error = ct_cache->cast_func (&values, ct_data);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  found_status = xbtree_find_unique (thread_p, &ct_cache->class_oid,
				     &ct_cache->btid, pk, &oid);
  switch (found_status)
    {
    case BTREE_KEY_NOTFOUND:
      switch (modify_mode)
	{
	case CT_MODIFY_MODE_INSERT:
	case CT_MODIFY_MODE_UPSERT:
	  op_mode = OP_INSERT;
	  break;
	case CT_MODIFY_MODE_UPDATE:
	  error = ER_BTREE_UNKNOWN_KEY;
	default:
	  GOTO_EXIT_ON_ERROR;
	}
      break;
    case BTREE_KEY_FOUND:
      switch (modify_mode)
	{
	case CT_MODIFY_MODE_UPDATE:
	case CT_MODIFY_MODE_UPSERT:
	  op_mode = OP_UPDATE;
	  break;
	case CT_MODIFY_MODE_INSERT:
	  error = ER_BTREE_UNIQUE_FAILED;
	default:
	  GOTO_EXIT_ON_ERROR;
	}
      break;
    case BTREE_ERROR_OCCURRED:
      error = er_errid ();
    default:
      GOTO_EXIT_ON_ERROR;
    }

  if (op_mode == OP_INSERT)
    {
      force_page_allocation = 0;
      if (strcasecmp (table->name, CT_LOG_APPLIER_NAME) == 0)
	{
	  force_page_allocation = 1;
	}
      error = qexec_insert_with_values (thread_p, &ct_cache->class_oid,
					&ct_cache->hfid, ct_cache->atts,
					force_page_allocation, &values);
    }
  else
    {
      assert (op_mode == OP_UPDATE);
      error = qexec_update_with_values (thread_p, &ct_cache->class_oid,
					&ct_cache->hfid, ct_cache->atts,
					&oid, &values);
    }
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  db_idxkey_clear (&values);

  assert (error == NO_ERROR);
  return NO_ERROR;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "unknown error");
    }

  db_idxkey_clear (&values);

  return error;
}

/*
 * qexec_get_info_of_catalog_table ()-
 *   return: ct_cache_info
 *
 *   table(in):
 */
static CT_CACHE_INFO *
qexec_get_info_of_catalog_table (THREAD_ENTRY * thread_p,
				 CATCLS_TABLE * table)
{
  CT_CACHE_INFO *ct_cache_info = NULL;
  CT_CACHE_INFO tmp_cache = CT_CACHE_INFO_INITIALIZER;
  LC_FIND_CLASSNAME status;
  int error = NO_ERROR;
  bool has_mutex = false;

  if (strncasecmp (table->name, CT_LOG_WRITER_NAME,
		   strlen (CT_LOG_WRITER_NAME)) == 0)
    {
      ct_cache_info = &Writer_info;
      tmp_cache.cast_func = qexec_cast_ct_writer_to_idxkey;
    }
  else if (strncasecmp (table->name, CT_LOG_ANALYZER_NAME,
			strlen (CT_LOG_ANALYZER_NAME)) == 0)
    {
      ct_cache_info = &Analyzer_info;
      tmp_cache.cast_func = qexec_cast_ct_analyzer_to_idxkey;
    }
  else if (strncasecmp (table->name, CT_LOG_APPLIER_NAME,
			strlen (CT_LOG_APPLIER_NAME)) == 0)
    {
      ct_cache_info = &Applier_info;
      tmp_cache.cast_func = qexec_cast_ct_applier_to_idxkey;
    }
  else if (strncasecmp (table->name, CT_SHARD_GID_SKEY_INFO_NAME,
			strlen (CT_SHARD_GID_SKEY_INFO_NAME)) == 0)
    {
      ct_cache_info = &Shard_gid_skey_info;
      tmp_cache.cast_func = qexec_cast_ct_gid_skey_to_idxkey;
    }
  else if (strncasecmp (table->name, CT_INDEX_STATS_NAME,
			strlen (CT_INDEX_STATS_NAME)) == 0)
    {
      ct_cache_info = &Index_stats_info;
      tmp_cache.cast_func = qexec_cast_ct_index_stats_to_idxkey;
    }
  else
    {
      assert (false);
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
      GOTO_EXIT_ON_ERROR;
    }

  if (ct_cache_info->atts != NULL)
    {
      assert (!OID_ISNULL (&ct_cache_info->class_oid));
      assert (!HFID_IS_NULL (&ct_cache_info->hfid));
      assert (!BTID_IS_NULL (&ct_cache_info->btid));
      assert (ct_cache_info->cast_func != NULL);
      assert (ct_cache_info->atts != NULL);

      return ct_cache_info;
    }

  error = pthread_mutex_lock (&ct_Cache_lock);
  if (error != NO_ERROR)
    {
      error = ER_CSS_PTHREAD_MUTEX_LOCK;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

      GOTO_EXIT_ON_ERROR;
    }
  has_mutex = true;

  if (ct_cache_info->atts != NULL)
    {
      pthread_mutex_unlock (&ct_Cache_lock);
      has_mutex = false;

      assert (!OID_ISNULL (&ct_cache_info->class_oid));
      assert (!HFID_IS_NULL (&ct_cache_info->hfid));
      assert (!BTID_IS_NULL (&ct_cache_info->btid));
      assert (ct_cache_info->cast_func != NULL);
      assert (ct_cache_info->atts != NULL);

      return ct_cache_info;
    }

  status = xlocator_find_class_oid (thread_p, table->name,
				    &tmp_cache.class_oid, S_LOCK);
  if (status == LC_CLASSNAME_ERROR || status == LC_CLASSNAME_DELETED)
    {
      error = ER_LC_UNKNOWN_CLASSNAME;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
	      CT_LOG_ANALYZER_NAME);

      GOTO_EXIT_ON_ERROR;
    }

  error = heap_get_hfid_from_class_oid (thread_p, &tmp_cache.class_oid,
					&tmp_cache.hfid);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = btree_get_pkey_btid (thread_p, &tmp_cache.class_oid,
			       &tmp_cache.btid);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  tmp_cache.atts = (ATTR_ID *) malloc (table->num_columns * sizeof (int));
  if (tmp_cache.atts == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, table->num_columns * sizeof (int));
      GOTO_EXIT_ON_ERROR;
    }

  error = qexec_get_attrids_from_catalog_table (thread_p, tmp_cache.atts,
						&tmp_cache.class_oid, table);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  *ct_cache_info = tmp_cache;

  pthread_mutex_unlock (&ct_Cache_lock);

  assert (!OID_ISNULL (&ct_cache_info->class_oid));
  assert (!HFID_IS_NULL (&ct_cache_info->hfid));
  assert (!BTID_IS_NULL (&ct_cache_info->btid));
  assert (ct_cache_info->cast_func != NULL);
  assert (ct_cache_info->atts != NULL);

  return ct_cache_info;

exit_on_error:
  assert (error != NO_ERROR);

  if (tmp_cache.atts != NULL)
    {
      free_and_init (tmp_cache.atts);
    }

  if (has_mutex == true)
    {
      pthread_mutex_unlock (&ct_Cache_lock);
      has_mutex = false;
    }

  return NULL;
}

/*
 * qexec_cast_ct_index_stats_to_idxkey
 *   return: error code
 *
 *   val(out):
 *   index_stats(in):
 */
static int
qexec_cast_ct_index_stats_to_idxkey (DB_IDXKEY * val, void *ct_table)
{
  CIRP_CT_INDEX_STATS *index_stats;
  int i;

  index_stats = (CIRP_CT_INDEX_STATS *) ct_table;

  /* init parameter */
  DB_IDXKEY_MAKE_NULL (val);

  /* table_IndexStats column order */
  i = 0;
  db_make_string (&val->vals[i++], index_stats->table_name);
  db_make_string (&val->vals[i++], index_stats->index_name);
  db_make_int (&val->vals[i++], index_stats->pages);
  db_make_int (&val->vals[i++], index_stats->leafs);
  db_make_int (&val->vals[i++], index_stats->height);
  db_make_bigint (&val->vals[i++], index_stats->keys);
  db_make_bigint (&val->vals[i++], index_stats->leaf_space_free);
  db_make_double (&val->vals[i++], index_stats->leaf_pct_free);
  db_make_int (&val->vals[i++], index_stats->num_table_vpids);
  db_make_int (&val->vals[i++], index_stats->num_user_pages_mrkdelete);
  db_make_int (&val->vals[i++], index_stats->num_allocsets);

  val->size = i;

  return NO_ERROR;
}

/*
 * qexec_cast_ct_gid_skey_to_idxkey
 *   return: error code
 *
 *   val(out):
 *   ct_table(in):
 */
static int
qexec_cast_ct_gid_skey_to_idxkey (DB_IDXKEY * val, void *ct_table)
{
  CT_GID_SKEY *gid_skey;
  int i;

  gid_skey = (CT_GID_SKEY *) ct_table;

  /* init parameter */
  DB_IDXKEY_MAKE_NULL (val);

  /* table_ShardGidSkey column order */
  i = 0;
  db_make_int (&val->vals[i++], gid_skey->gid);
  db_value_clone (gid_skey->skey, &val->vals[i++]);

  val->size = i;

  return NO_ERROR;
}

/*
 * qexec_cast_ct_applier_to_idxkey
 *   return: error code
 *
 *   val(out):
 *   ct_table(in):
 */
static int
qexec_cast_ct_applier_to_idxkey (DB_IDXKEY * val, void *ct_table)
{
  CIRP_CT_LOG_APPLIER *log_applier;
  INT64 bi;
  int i;

  log_applier = (CIRP_CT_LOG_APPLIER *) ct_table;

  /* init parameter */
  DB_IDXKEY_MAKE_NULL (val);

  /* table_LogApplier column order */
  i = 0;
  db_make_string (&val->vals[i++], log_applier->host_ip);
  db_make_int (&val->vals[i++], log_applier->id);

  bi = lsa_to_int64 (log_applier->committed_lsa);
  db_make_bigint (&val->vals[i++], bi);

  db_make_bigint (&val->vals[i++], log_applier->master_last_commit_time);
  db_make_bigint (&val->vals[i++], log_applier->repl_delay);
  db_make_bigint (&val->vals[i++], log_applier->insert_count);
  db_make_bigint (&val->vals[i++], log_applier->update_count);
  db_make_bigint (&val->vals[i++], log_applier->delete_count);
  db_make_bigint (&val->vals[i++], log_applier->schema_count);
  db_make_bigint (&val->vals[i++], log_applier->commit_count);
  db_make_bigint (&val->vals[i++], log_applier->fail_count);

  val->size = i;

  return NO_ERROR;
}

/*
 * qexec_cast_ct_analyzer_to_idxkey
 *   return: error code
 *
 *   val(out):
 *   ct_table(in):
 */
static int
qexec_cast_ct_analyzer_to_idxkey (DB_IDXKEY * val, void *ct_table)
{
  CIRP_CT_LOG_ANALYZER *log_analyzer;
  INT64 bi;
  int i;

  log_analyzer = (CIRP_CT_LOG_ANALYZER *) ct_table;

  /* init parameter */
  DB_IDXKEY_MAKE_NULL (val);

  /* table_LogApplier column order */
  i = 0;
  db_make_string (&val->vals[i++], log_analyzer->host_ip);

  bi = lsa_to_int64 (log_analyzer->current_lsa);
  db_make_bigint (&val->vals[i++], bi);
  bi = lsa_to_int64 (log_analyzer->required_lsa);
  db_make_bigint (&val->vals[i++], bi);

  db_make_bigint (&val->vals[i++], log_analyzer->start_time);
  db_make_bigint (&val->vals[i++], log_analyzer->source_applied_time);
  db_make_bigint (&val->vals[i++], log_analyzer->creation_time);

  db_make_bigint (&val->vals[i++], log_analyzer->queue_full);

  val->size = i;

  return NO_ERROR;
}

/*
 * qexec_cast_ct_writer_to_idxkey
 *   return: error code
 *
 *   val(out):
 *   ct_table(in):
 */
static int
qexec_cast_ct_writer_to_idxkey (DB_IDXKEY * val, void *ct_table)
{
  CIRP_CT_LOG_WRITER *writer;
  INT64 bi;
  int i;

  writer = (CIRP_CT_LOG_WRITER *) ct_table;

  /* init parameter */
  DB_IDXKEY_MAKE_NULL (val);

  /* table_LogWriter column order */
  i = 0;
  db_make_string (&val->vals[i++], writer->host_ip);

  db_make_bigint (&val->vals[i++], writer->last_flushed_pageid);

  db_make_bigint (&val->vals[i++], writer->last_received_time);

  bi = lsa_to_int64 (writer->eof_lsa);
  db_make_bigint (&val->vals[i++], bi);

  val->size = i;

  return NO_ERROR;
}

/*
 * qexec_insert_with_values () -
 *    return: error code or NO_ERROR
 *
 *    class_oid(in):
 *    hfid(in):
 *    attr_ids(in):
 *    force_page_allocation(in):
 *    values(in):
 */
static int
qexec_insert_with_values (THREAD_ENTRY * thread_p, OID * class_oid,
			  HFID * hfid, ATTR_ID * attr_ids,
			  int force_page_allocation, DB_IDXKEY * values)
{
  HEAP_SCANCACHE scan_cache;
  int i;
  HEAP_CACHE_ATTRINFO attr_info;
  int force_count;
  bool attr_info_inited = false;
  bool scan_cache_inited = false;
  int error = NO_ERROR;
  OID oid;


  error = locator_start_force_scan_cache (thread_p, &scan_cache,
					  hfid, force_page_allocation,
					  class_oid);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  scan_cache_inited = true;

  error = heap_attrinfo_start (thread_p, class_oid, -1, NULL, &attr_info);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  attr_info_inited = true;

  for (i = 0; i < values->size; i++)
    {
      error = heap_attrinfo_set (NULL, attr_ids[i], &values->vals[i],
				 &attr_info);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  OID_SET_NULL (&oid);
  oid.groupid = GLOBAL_GROUPID;
  error = locator_attribute_info_force (thread_p, hfid,
					&oid, &attr_info,
					NULL, 0, LC_FLUSH_INSERT,
					&scan_cache, &force_count);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (attr_info_inited)
    {
      (void) heap_attrinfo_end (thread_p, &attr_info);
    }
  if (scan_cache_inited)
    {
      (void) locator_end_force_scan_cache (thread_p, &scan_cache);
    }


  assert (error == NO_ERROR);

  return NO_ERROR;

exit_on_error:
  assert (error != NO_ERROR);

  if (attr_info_inited)
    {
      (void) heap_attrinfo_end (thread_p, &attr_info);
    }
  if (scan_cache_inited)
    {
      (void) locator_end_force_scan_cache (thread_p, &scan_cache);
    }


  return error;
}

/*
 * qexec_update_with_values () -
 *    return: error code or NO_ERROR
 *
 *    class_oid(in):
 *    hfid(in):
 *    attr_ids(in):
 *    oid(in):
 *    values(in):
 */
static int
qexec_update_with_values (THREAD_ENTRY * thread_p, OID * class_oid,
			  HFID * hfid, ATTR_ID * attr_ids, OID * oid,
			  DB_IDXKEY * values)
{
  HEAP_SCANCACHE scan_cache;
  int i;
  HEAP_CACHE_ATTRINFO attr_info;
  int force_count;
  bool attr_info_inited = false;
  bool scan_cache_inited = false;
  int error = NO_ERROR;


  error = locator_start_force_scan_cache (thread_p, &scan_cache,
					  hfid, 0, class_oid);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  scan_cache_inited = true;

  error = heap_attrinfo_start (thread_p, class_oid, -1, NULL, &attr_info);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  attr_info_inited = true;

  for (i = 0; i < values->size; i++)
    {
      error = heap_attrinfo_set (NULL, attr_ids[i], &values->vals[i],
				 &attr_info);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  assert (oid->groupid == GLOBAL_GROUPID);
  error = locator_attribute_info_force (thread_p, hfid,
					oid, &attr_info,
					NULL, 0, LC_FLUSH_UPDATE,
					&scan_cache, &force_count);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (attr_info_inited)
    {
      (void) heap_attrinfo_end (thread_p, &attr_info);
    }
  if (scan_cache_inited)
    {
      (void) locator_end_force_scan_cache (thread_p, &scan_cache);
    }

  assert (error == NO_ERROR);

  return NO_ERROR;

exit_on_error:
  assert (error != NO_ERROR);

  if (attr_info_inited)
    {
      (void) heap_attrinfo_end (thread_p, &attr_info);
    }
  if (scan_cache_inited)
    {
      (void) locator_end_force_scan_cache (thread_p, &scan_cache);
    }

  return error;
}

/*
 * qexec_get_attrids_from_catalog_table () -
 *    return: error code or NO_ERROR
 *
 *    att_ids(out):
 *    ct_table(in):
 */
static int
qexec_get_attrids_from_catalog_table (THREAD_ENTRY * thread_p,
				      int *att_ids, OID * class_oid,
				      CATCLS_TABLE * ct_table)
{
  RECDES class_record = RECDES_INITIALIZER;
  const char *attr_name_p = NULL;
  HEAP_SCANCACHE scan_cache;
  int i, j;
  HEAP_CACHE_ATTRINFO attr_info;
  bool attr_info_inited = false;
  bool scan_cache_inited = false;
  int error = NO_ERROR;

  /* init parameter */
  for (i = 0; i < ct_table->num_columns; i++)
    {
      att_ids[i] = NULL_ATTRID;
    }

  error = heap_scancache_quick_start (&scan_cache);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  scan_cache_inited = true;

  error = heap_attrinfo_start (thread_p, class_oid, -1, NULL, &attr_info);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  attr_info_inited = true;

  if (heap_get (thread_p, class_oid, &class_record,
		&scan_cache, PEEK) != S_SUCCESS)
    {
      error = er_errid ();
      GOTO_EXIT_ON_ERROR;
    }

  for (i = 0; i < attr_info.last_classrepr->n_attributes; i++)
    {
      attr_name_p = or_get_attrname (&class_record, i);
      if (attr_name_p == NULL)
	{
	  error = er_errid ();

	  GOTO_EXIT_ON_ERROR;
	}

      for (j = 0; j < ct_table->num_columns; j++)
	{
	  if (strcmp (attr_name_p, ct_table->columns[j].name) == 0)
	    {
	      att_ids[j] = i;
	      break;
	    }
	}
    }

  if (attr_info_inited)
    {
      (void) heap_attrinfo_end (thread_p, &attr_info);
    }
  if (scan_cache_inited)
    {
      (void) heap_scancache_end (thread_p, &scan_cache);
    }

  assert (error == NO_ERROR);
  return NO_ERROR;

exit_on_error:
  assert (error != NO_ERROR);
  if (error == NO_ERROR)
    {
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "unknown error");
    }

  if (attr_info_inited)
    {
      (void) heap_attrinfo_end (thread_p, &attr_info);
    }
  if (scan_cache_inited)
    {
      (void) heap_scancache_end (thread_p, &scan_cache);
    }

  return error;
}

/*
 * qexec_lock_table_and_shard_key () - lock tables and shard key before
 *                                     execute query
 *   return: error code
 *   thread_p(in):
 *   xasl(in):
 *   shard_groupid(in):
 *   shard_key(in):
 */
static int
qexec_lock_table_and_shard_key (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
				int shard_groupid, DB_VALUE * shard_key,
				bool req_from_migrator)
{
  int error = NO_ERROR;
  bool for_update = false;
  bool is_shard_table = false, is_catalog_table = false;
  int num_shard_table = 0;
  DB_VALUE lock_val;
  OID *class_oid = NULL;

  assert (xasl != NULL);
  assert (shard_groupid != NULL_GROUPID);

  if (XASL_IS_FLAGED (xasl, XASL_TO_SHARD_TABLE))
    {
      /* is DML */
      assert (!XASL_IS_FLAGED (xasl, XASL_TO_CATALOG_TABLE));
      assert (shard_groupid != GLOBAL_GROUPID);
      assert (shard_key != NULL);
      assert (!DB_IS_NULL (shard_key));

      is_shard_table = true;
    }

  if (XASL_IS_FLAGED (xasl, XASL_TO_CATALOG_TABLE))
    {
      assert (!XASL_IS_FLAGED (xasl, XASL_TO_SHARD_TABLE));
      assert (shard_groupid == GLOBAL_GROUPID);
      assert (shard_key == NULL);

      is_catalog_table = true;
    }

  switch (xasl->type)
    {
    case BUILDLIST_PROC:
    case BUILDVALUE_PROC:
      assert (is_shard_table == false);
      assert (is_catalog_table == false);

      if (shard_groupid != GLOBAL_GROUPID)
	{
	  /* one shard SELECT */
	  assert (shard_key != NULL);
	  assert (!DB_IS_NULL (shard_key));

	  is_shard_table = true;
	}

      error = qexec_lock_table_select_for_update (thread_p, xasl,
						  &num_shard_table,
						  &for_update);
      if (error == NO_ERROR)
	{
	  if (is_shard_table)
	    {
	      /* one shard SELECT */
	      assert (num_shard_table > 0);
	    }
	  else
	    {
	      assert (shard_groupid == GLOBAL_GROUPID);
	      assert (shard_key == NULL);

	      if (for_update == false)
		{
		  /* no need to lock shard key */
		  return NO_ERROR;
		}
	    }
	}
      break;

    case INSERT_PROC:
      if (shard_groupid == GLOBAL_GROUPID && logtb_is_blocked_global_dml ()
	  && req_from_migrator != true)
	{
	  error = ER_SHARD_CANT_GLOBAL_DML_UNDER_MIGRATION;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	  return error;
	}

      DB_MAKE_OID (&lock_val, &xasl->proc.insert.class_oid);
      if (lock_object (thread_p, &lock_val, S_LOCK, LK_UNCOND_LOCK) !=
	  LK_GRANTED)
	{
	  error = er_errid ();
	  if (error == NO_ERROR)
	    {
	      error = ER_FAILED;
	    }

	  return error;
	}

      assert (heap_classrepr_is_shard_table (thread_p, DB_GET_OID (&lock_val))
	      == is_shard_table);
      for_update = true;
      break;

    case UPDATE_PROC:
      if (shard_groupid == GLOBAL_GROUPID && logtb_is_blocked_global_dml ()
	  && req_from_migrator != true)
	{
	  error = ER_SHARD_CANT_GLOBAL_DML_UNDER_MIGRATION;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	  return error;
	}

      error = qexec_lock_table_update_delete (thread_p, xasl->aptr_list,
					      xasl->proc.update.class_info,
					      is_shard_table,
					      is_catalog_table);
      for_update = true;
      break;

    case DELETE_PROC:
      if (shard_groupid == GLOBAL_GROUPID && logtb_is_blocked_global_dml ()
	  && req_from_migrator != true)
	{
	  error = ER_SHARD_CANT_GLOBAL_DML_UNDER_MIGRATION;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	  return error;
	}

      error = qexec_lock_table_update_delete (thread_p, xasl->aptr_list,
					      xasl->proc.delete_.class_info,
					      is_shard_table,
					      is_catalog_table);
      for_update = true;
      break;

    default:
      /* no need to lock shard key */
      return NO_ERROR;
    }

  /* at here, xasl is for DML
   */

  if (error != NO_ERROR)
    {
      return error;
    }

  /* defense code; check iff valid xasl
   */
  if (is_shard_table && is_catalog_table)
    {
      assert (false);
      error = ER_GENERIC_ERROR;	/* TODO - */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
	      "qexec_lock_table_and_shard_key(): Invalid shard_key");
      return error;
    }

  /* defense code; check iff valid DML
   */
  if ((shard_groupid != GLOBAL_GROUPID && is_shard_table)
      || (shard_groupid == GLOBAL_GROUPID && !is_shard_table))
    {
      ;				/* OK */
    }
  else
    {
      /* is DML for global table joined with shard table
       * with shard key lock
       */
      assert (false);
      error = ER_SHARD_INVALID_GROUPID;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, shard_groupid);
      return error;
    }

  class_oid = NULL;
  if (for_update == true && xasl->n_oid_list == 1)
    {
      class_oid = &xasl->class_oid_list[0];
    }
  error = lock_shard_key_lock (thread_p, shard_groupid, shard_key,
			       class_oid, is_shard_table,
			       is_catalog_table, for_update);
  if (error != NO_ERROR)
    {
      return error;
    }

  /* register new shard key into the catalog table
   */
  if (is_shard_table == true && xasl->type == INSERT_PROC)
    {
      assert (shard_groupid != GLOBAL_GROUPID);
      assert (shard_key != NULL);
      assert (for_update == true);

      error = qexec_insert_gid_skey_info (thread_p, shard_groupid, shard_key);
      if (error == ER_BTREE_UNIQUE_FAILED)
	{
	  /* skip unique error */
	  error = NO_ERROR;
	}
    }

  return error;
}

static void
qexec_init_xasl_state (THREAD_ENTRY * thread_p, XASL_STATE * xasl_state,
		       int dbval_cnt, DB_VALUE * dbval_ptr,
		       QUERY_ID query_id, int shard_groupid)
{
  struct timeb tloc;
  struct tm *c_time_struct, tm_val;
  struct drand48_data *rand_buf_p;

  assert (shard_groupid != NULL_GROUPID);

  /* form the value descriptor to represent positional values */
  xasl_state->vd.dbval_cnt = dbval_cnt;
  xasl_state->vd.dbval_ptr = dbval_ptr;
  ftime (&tloc);
  c_time_struct = localtime_r (&tloc.time, &tm_val);

  if (c_time_struct != NULL)
    {
      db_datetime_encode (&xasl_state->vd.sys_datetime,
			  c_time_struct->tm_mon + 1, c_time_struct->tm_mday,
			  c_time_struct->tm_year + 1900,
			  c_time_struct->tm_hour, c_time_struct->tm_min,
			  c_time_struct->tm_sec, tloc.millitm);
    }

  rand_buf_p = qmgr_get_rand_buf (thread_p);
  lrand48_r (rand_buf_p, &xasl_state->vd.lrand);
  drand48_r (rand_buf_p, &xasl_state->vd.drand);
  xasl_state->vd.xasl_state = xasl_state;

  /* save the query_id into the XASL state struct */
  xasl_state->query_id = query_id;

  /* initialize error line */
  xasl_state->qp_xasl_line = 0;

  /* save shard groupid for INSERT */
  xasl_state->shard_groupid = shard_groupid;

  return;
}

/*
 * qexec_execute_query () -
 *   return: Query result list file identifier, or NULL
 *   xasl(in)   : XASL Tree pointer
 *   dbval_cnt(in)      : Number of positional values (0 or more)
 *   dbval_ptr(in)      : List of positional values (optional)
 *   query_id(in)       : Query Associated with the XASL tree
 *   shard_groupid(in)        :
 *   shard_key(in)      :
 *
 *
 * Note: This routine executes the query represented by the given XASL
 * tree. The XASL tree may be associated with a set of positional
 * values (coming from esql programs positional values) which
 * may be used during query execution. The query result file
 * identifier is returned. if an error occurs during execution,
 * NULL is returned.
 */

QFILE_LIST_ID *
qexec_execute_query (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
		     int dbval_cnt, DB_VALUE * dbval_ptr,
		     QUERY_ID query_id, int shard_groupid,
		     DB_VALUE * shard_key, bool req_from_migrator)
{
  int stat;
  QFILE_LIST_ID *list_id = NULL;
  XASL_STATE xasl_state;
  char err_buf[512];
#if defined(SERVER_MODE)
  bool old_check_groupid;
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
  if (LOG_IS_HA_CLIENT (logtb_find_current_client_type (thread_p))
      || shard_groupid < GLOBAL_GROUPID)
    {
      /* client is HA applier or shard group migrator or GC
       * do not filter groupid
       */
      old_check_groupid = thread_set_check_groupid (thread_p, false);
    }
#endif /* SERVER_MODE */

  /* this routine should not be called if an outstanding error condition
   * already exists.
   */
  er_clear ();
  qexec_init_xasl_state (thread_p, &xasl_state, dbval_cnt, dbval_ptr,
			 query_id, shard_groupid);

  /* execute the query
   *
   * set the query in progress flag so that qmgr_clear_trans_wakeup() will
   * not remove our XASL tree out from under us in the event the
   * transaction is unilaterally aborted during query execution.
   */

  xasl->query_in_progress = true;
  stat = qexec_lock_table_and_shard_key (thread_p, xasl, shard_groupid,
					 shard_key, req_from_migrator);
  if (stat == NO_ERROR)
    {
      int tran_index;
      QMGR_QUERY_ENTRY *query_p = NULL;

      tran_index = logtb_get_current_tran_index (thread_p);

      query_p = qmgr_get_query_entry (thread_p, query_id, tran_index);
      if (query_p == NULL || query_p->xasl_ent == NULL)
	{
	  assert (false);
	  stat = ER_QPROC_UNKNOWN_QUERYID;
	}
      else
	{
	  XASL_CACHE_ENTRY *ent = NULL;

	  ent = query_p->xasl_ent;
	  if (ent->deletion_marker)
	    {
	      /* It was marked to be deleted. */
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_QPROC_INVALID_XASLNODE, 0);
	      stat = ER_QPROC_INVALID_XASLNODE;
	    }
	  else
	    {
	      stat = qexec_execute_mainblock (thread_p, xasl, &xasl_state);
	    }
	}
    }
  xasl->query_in_progress = false;

#if defined(SERVER_MODE)
  if (thread_is_on_trace (thread_p))
    {
      qexec_set_xasl_trace_to_session (thread_p, xasl);
    }
#endif /* SERVER_MODE */

  if (stat != NO_ERROR)
    {
      switch (er_errid ())
	{
	case NO_ERROR:
	  {
	    snprintf (err_buf, 511, "Query execution failure #%d.",
		      xasl_state.qp_xasl_line);
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		    ER_PT_EXECUTE, 2, err_buf, "");
	    break;
	  }
	case ER_INTERRUPTED:
	  /*
	   * Most of the cleanup that's about to happen will get
	   * screwed up if the interrupt is still in effect (e.g.,
	   * someone will do a pb_fetch, which will quit early, and
	   * so they'll bail without actually finishing their
	   * cleanup), so disable it.
	   */
	  xlogtb_set_interrupt (thread_p, false);
	  break;
	}

      qmgr_set_query_error (thread_p, query_id);	/* propagate error */

      if (xasl->list_id)
	{
	  qfile_close_list (thread_p, xasl->list_id);
	}

      list_id = qexec_get_xasl_list_id (xasl);

      (void) qexec_clear_xasl (thread_p, xasl, false);

#if defined(SERVER_MODE)
      (void) thread_set_check_groupid (thread_p, old_check_groupid);
#endif /* SERVER_MODE */

      /* caller will detect the error condition and free the listid */
      return list_id;
    }
  else
    {
      er_clear ();
    }

  list_id = qexec_get_xasl_list_id (xasl);

  /* set last_pgptr->next_vpid to NULL */
  if (list_id && list_id->last_pgptr != NULL)
    {
      QFILE_PUT_NEXT_VPID_NULL (list_id->last_pgptr);
    }

#if defined(SERVER_MODE)
  if (thread_need_clear_trace (thread_p))
    {
      (void) session_clear_trace_stats (thread_p);
    }
#endif

  /* clear XASL tree */
  (void) qexec_clear_xasl (thread_p, xasl, false);

#if defined(SERVER_MODE)
  (void) thread_set_check_groupid (thread_p, old_check_groupid);
#endif /* SERVER_MODE */

  return list_id;
}

/*
 * Generation of a pseudo value from the XASL_ID.
 * It is used for hashing purposes.
 */
#define XASL_ID_PSEUDO_KEY(xasl_id) \
  ((((xasl_id)->first_vpid.pageid) | ((xasl_id)->first_vpid.volid) << 24) ^ \
   (((xasl_id)->temp_vfid.fileid) | ((xasl_id)->temp_vfid.volid) >> 8))

/*
 * xasl_id_hash () - Hash an XASL_ID (XASL file identifier)
 *   return:
 *   key(in)    :
 *   htsize(in) :
 */
static unsigned int
xasl_id_hash (const void *key, unsigned int htsize)
{
  unsigned int hash;
  const XASL_ID *xasl_id = (const XASL_ID *) key;

  hash = XASL_ID_PSEUDO_KEY (xasl_id);
  return (hash % htsize);
}

/*
 * xasl_id_hash_cmpeq () - Compare two XASL_IDs for hash purpose
 *   return:
 *   key1(in)   :
 *   key2(in)   :
 */
int
xasl_id_hash_cmpeq (const void *key1, const void *key2)
{
  const XASL_ID *xasl_id1 = (const XASL_ID *) key1;
  const XASL_ID *xasl_id2 = (const XASL_ID *) key2;

  return XASL_ID_EQ (xasl_id1, xasl_id2);
}

/*
 * qexec_initialize_xasl_cache () - Initialize XASL cache
 *   return: NO_ERROR, or ER_code
 */
int
qexec_initialize_xasl_cache (THREAD_ENTRY * thread_p)
{
  int i;
  POOLED_XASL_CACHE_ENTRY *pent;

  assert (prm_get_integer_value (PRM_ID_XASL_MAX_PLAN_CACHE_ENTRIES) >= 1000);

  XASL_CACHE_LOCK ();

  /* init cache entry info */
  xasl_ent_cache.max_entries =
    prm_get_integer_value (PRM_ID_XASL_MAX_PLAN_CACHE_ENTRIES);
  xasl_ent_cache.num = 0;
  xasl_ent_cache.counter.lookup = 0;
  xasl_ent_cache.counter.hit = 0;
  xasl_ent_cache.counter.miss = 0;
  xasl_ent_cache.counter.full = 0;

  /* memory hash table for XASL stream cache referencing by query string */
  if (xasl_ent_cache.qstr_ht)
    {
      /* if the hash table already exist, clear it out */
      (void) mht_map_no_key (thread_p, xasl_ent_cache.qstr_ht,
			     qexec_free_xasl_cache_ent, NULL);
      (void) mht_clear (xasl_ent_cache.qstr_ht, NULL, NULL);
    }
  else
    {
      /* create */
      xasl_ent_cache.qstr_ht = mht_create ("XASL stream cache (query string)",
					   xasl_ent_cache.max_entries,
					   qexec_xasl_qstr_ht_hash,
					   qexec_xasl_qstr_ht_keys_are_equal);
    }
  mnt_stats_gauge (thread_p, MNT_STATS_PLAN_CACHE_QUERY_STRING_HASH_ENTRIES,
		   0);

  xasl_ent_cache.qstr_ht->build_lru_list = true;

  /* memory hash table for XASL stream cache referencing by xasl file id */
  if (xasl_ent_cache.xid_ht)
    {
      /* if the hash table already exist, clear it out */
      /*(void) mht_map_no_key(xasl_ent_cache.xid_ht, NULL, NULL); */
      (void) mht_clear (xasl_ent_cache.xid_ht, NULL, NULL);
    }
  else
    {
      /* create */
      xasl_ent_cache.xid_ht = mht_create ("XASL stream cache (xasl file id)",
					  xasl_ent_cache.max_entries,
					  xasl_id_hash, xasl_id_hash_cmpeq);
    }
  mnt_stats_gauge (thread_p, MNT_STATS_PLAN_CACHE_XASL_ID_HASH_ENTRIES, 0);

  /* memory hash table for XASL stream cache referencing by class/serial oid */
  if (xasl_ent_cache.oid_ht)
    {
      /* if the hash table already exist, clear it out */
      /*(void) mht_map_no_key(xasl_ent_cache.oid_ht, NULL, NULL); */
      (void) mht_clear (xasl_ent_cache.oid_ht, NULL, NULL);
    }
  else
    {
      /* create */
      xasl_ent_cache.oid_ht = mht_create ("XASL stream cache (class oid)",
					  xasl_ent_cache.max_entries,
					  oid_hash, oid_compare_equals);
    }
  mnt_stats_gauge (thread_p, MNT_STATS_PLAN_CACHE_CLASS_OID_HASH_ENTRIES, 0);

  /* init cache clone info */
  xasl_clo_cache.max_clones =
    prm_get_integer_value (PRM_ID_XASL_MAX_PLAN_CACHE_CLONES);
  xasl_clo_cache.num = 0;
  xasl_clo_cache.counter.lookup = 0;
  xasl_clo_cache.counter.hit = 0;
  xasl_clo_cache.counter.miss = 0;
  xasl_clo_cache.counter.full = 0;
  xasl_clo_cache.head = NULL;
  xasl_clo_cache.tail = NULL;
  xasl_clo_cache.free_list = NULL;

  /* if cache clones already exist, free it */
  for (i = 0; i < xasl_clo_cache.n_alloc; i++)
    {
      free_and_init (xasl_clo_cache.alloc_arr[i]);
    }
  free_and_init (xasl_clo_cache.alloc_arr);
  xasl_clo_cache.n_alloc = 0;

  /* now, alloc clones array */
  if (xasl_clo_cache.max_clones > 0)
    {
      xasl_clo_cache.free_list = qexec_expand_xasl_cache_clo_arr (1);
      if (!xasl_clo_cache.free_list)
	{
	  xasl_clo_cache.max_clones = 0;
	}
    }

  /* XASL cache entry pool */
  if (xasl_cache_entry_pool.pool)
    {
      free_and_init (xasl_cache_entry_pool.pool);
    }

  xasl_cache_entry_pool.n_entries =
    prm_get_integer_value (PRM_ID_XASL_MAX_PLAN_CACHE_ENTRIES) + 10;
  xasl_cache_entry_pool.pool =
    (POOLED_XASL_CACHE_ENTRY *) calloc (xasl_cache_entry_pool.n_entries,
					sizeof (POOLED_XASL_CACHE_ENTRY));

  if (xasl_cache_entry_pool.pool != NULL)
    {
      xasl_cache_entry_pool.free_list = 0;
      for (pent = xasl_cache_entry_pool.pool, i = 0;
	   pent && i < xasl_cache_entry_pool.n_entries - 1; pent++, i++)
	{
	  pent->s.next = i + 1;
	}

      if (pent != NULL)
	{
	  pent->s.next = -1;
	}
    }

  XASL_CACHE_UNLOCK ();

  return ((xasl_ent_cache.qstr_ht && xasl_ent_cache.xid_ht
	   && xasl_ent_cache.oid_ht
	   && xasl_cache_entry_pool.pool) ? NO_ERROR : ER_FAILED);
}

/*
 * qexec_finalize_xasl_cache () - Final XASL cache
 *   return: NO_ERROR, or ER_code
 */
int
qexec_finalize_xasl_cache (THREAD_ENTRY * thread_p)
{
  int ret = NO_ERROR;
  int i;

  if (xasl_ent_cache.max_entries <= 0)
    {
      return NO_ERROR;
    }

  XASL_CACHE_LOCK ();

  /* memory hash table for XASL stream cache referencing by query string */
  if (xasl_ent_cache.qstr_ht)
    {
      (void) mht_map_no_key (thread_p, xasl_ent_cache.qstr_ht,
			     qexec_free_xasl_cache_ent, NULL);
      mht_destroy (xasl_ent_cache.qstr_ht);
      xasl_ent_cache.qstr_ht = NULL;
    }
  mnt_stats_gauge (thread_p, MNT_STATS_PLAN_CACHE_QUERY_STRING_HASH_ENTRIES,
		   0);

  /* memory hash table for XASL stream cache referencing by xasl file id */
  if (xasl_ent_cache.xid_ht)
    {
      mht_destroy (xasl_ent_cache.xid_ht);
      xasl_ent_cache.xid_ht = NULL;
    }
  mnt_stats_gauge (thread_p, MNT_STATS_PLAN_CACHE_XASL_ID_HASH_ENTRIES, 0);

  /* memory hash table for XASL stream cache referencing by class/serial oid */
  if (xasl_ent_cache.oid_ht)
    {
      mht_destroy (xasl_ent_cache.oid_ht);
      xasl_ent_cache.oid_ht = NULL;
    }
  mnt_stats_gauge (thread_p, MNT_STATS_PLAN_CACHE_CLASS_OID_HASH_ENTRIES, 0);

  /* free all cache clone and XASL tree */
  if (xasl_clo_cache.head)
    {
      XASL_CACHE_CLONE *clo;

      while ((clo = xasl_clo_cache.head) != NULL)
	{
	  clo->next = NULL;	/* cut-off */
	  /* delete from LRU list */
	  (void) qexec_delete_LRU_xasl_cache_clo (clo);

	  /* add clone to free_list */
	  ret = qexec_free_xasl_cache_clo (clo);
	}			/* while */
    }

  for (i = 0; i < xasl_clo_cache.n_alloc; i++)
    {
      free_and_init (xasl_clo_cache.alloc_arr[i]);
    }
  free_and_init (xasl_clo_cache.alloc_arr);
  xasl_clo_cache.n_alloc = 0;

  /* XASL cache entry pool */
  if (xasl_cache_entry_pool.pool)
    {
      free_and_init (xasl_cache_entry_pool.pool);
    }

  XASL_CACHE_UNLOCK ();

  return NO_ERROR;
}

/*
 * qexec_print_xasl_cache_ent () - Print the entry
 *                              Will be used by mht_dump() function
 *   return:
 *   fp(in)     :
 *   key(in)    :
 *   data(in)   :
 *   args(in)   :
 */

static int
qexec_print_xasl_cache_ent (FILE * fp, UNUSED_ARG const void *key,
			    void *data, UNUSED_ARG void *args)
{
  XASL_CACHE_ENTRY *ent = (XASL_CACHE_ENTRY *) data;
  XASL_CACHE_CLONE *clo;
  int i;
#if defined(SERVER_MODE)
  int num_tran;
  int num_fixed_tran;
#endif
  const OID *o;
  char str[20];
  time_t tmp_time;
  struct tm *c_time_struct, tm_val;
  char *sql_id;

  if (ent == NULL)
    {
      return false;
    }
  if (fp == NULL)
    {
      fp = stdout;
    }

  fprintf (fp, "XASL_CACHE_ENTRY (%p) {\n", data);
  fprintf (fp, "     sql_user_text = %s\n", ent->sql_info.sql_user_text);

  if (qmgr_get_sql_id (NULL, &sql_id, ent->sql_info.sql_hash_text,
		       strlen (ent->sql_info.sql_hash_text)) != NO_ERROR)
    {
      sql_id = NULL;
    }

  fprintf (fp, "     sql_hash_text = /* SQL_ID: %s */ %s\n",
	   sql_id ? sql_id : "(null)", ent->sql_info.sql_hash_text);

  if (sql_id != NULL)
    {
      free (sql_id);
    }

  if (prm_get_bool_value (PRM_ID_SQL_TRACE_EXECUTION_PLAN) == true)
    {
      fprintf (fp, "     sql_plan_text = %s\n", ent->sql_info.sql_plan_text);
    }

  fprintf (fp,
	   "           xasl_id = { first_vpid = { %d %d } temp_vfid = { %d %d } }\n",
	   ent->xasl_id.first_vpid.pageid, ent->xasl_id.first_vpid.volid,
	   ent->xasl_id.temp_vfid.fileid, ent->xasl_id.temp_vfid.volid);
#if defined(SERVER_MODE)
  fprintf (fp, "  tran_index_array = [");

  num_fixed_tran = ent->num_fixed_tran;
  for (i = 0, num_tran = 0; i < MAX_NTRANS && num_tran < num_fixed_tran; i++)
    {
      if (ent->tran_fix_count_array[i] > 0)
	{
	  fprintf (fp, " %d", i);
	  num_tran++;
	}
    }
  fprintf (fp, " ]\n");
  fprintf (fp, "    num_fixed_tran = %d\n", num_tran);
#endif
  fprintf (fp, "       creator_oid = { %d %d %d }\n",
	   ent->qstr_ht_key.creator_oid.pageid,
	   ent->qstr_ht_key.creator_oid.slotid,
	   ent->qstr_ht_key.creator_oid.volid);
  fprintf (fp, "        n_oid_list = %d\n", ent->n_oid_list);
  fprintf (fp, "    class_oid_list = [");
  for (i = 0, o = ent->class_oid_list; i < ent->n_oid_list; i++, o++)
    {
      fprintf (fp, " { %d %d %d }", ent->class_oid_list[i].pageid,
	       ent->class_oid_list[i].slotid, ent->class_oid_list[i].volid);
    }
  fprintf (fp, " ]\n");
  fprintf (fp, "        tcard_list = [");
  if (ent->tcard_list)
    {
      for (i = 0; i < ent->n_oid_list; i++)
	{
	  fprintf (fp, " %d", ent->tcard_list[i]);
	}
    }
  fprintf (fp, " ]\n");

  tmp_time = ent->time_created.tv_sec;
  c_time_struct = localtime_r (&tmp_time, &tm_val);
  if (c_time_struct == NULL)
    {
      fprintf (fp, "      time_created.tv_sec is invalid (%ld)\n",
	       ent->time_created.tv_sec);
    }
  else
    {
      (void) strftime (str, sizeof (str), "%x %X", c_time_struct);
      fprintf (fp, "      time_created = %s.%d\n", str,
	       (int) ent->time_created.tv_usec);
    }

  tmp_time = ent->time_last_used.tv_sec;
  c_time_struct = localtime_r (&tmp_time, &tm_val);
  if (c_time_struct == NULL)
    {
      fprintf (fp, "    time_last_used.tv_sec is invalid (%ld)\n",
	       ent->time_last_used.tv_sec);
    }
  else
    {
      (void) strftime (str, sizeof (str), "%x %X", c_time_struct);
      fprintf (fp, "    time_last_used = %s.%d\n", str,
	       (int) ent->time_last_used.tv_usec);
    }

  fprintf (fp, "         ref_count = %d\n", ent->ref_count);
  fprintf (fp, "   deletion_marker = %s\n",
	   (ent->deletion_marker) ? "true" : "false");
  fprintf (fp, "         dbval_cnt = %d\n", ent->dbval_cnt);
  fprintf (fp, "          clo_list = [");
  for (clo = ent->clo_list; clo; clo = clo->next)
    {
      fprintf (fp, " %p", (void *) clo);
    }
  fprintf (fp, " ]\n");
  fprintf (fp, "}\n");

  return true;
}

/*
 * qexec_dump_xasl_cache_internal () -
 *   return: NO_ERROR, or ER_code
 *   fp(in)     :
 *   mask(in)   :
 */
int
qexec_dump_xasl_cache_internal (UNUSED_ARG THREAD_ENTRY * thread_p,
				FILE * fp, int mask)
{
  if (!xasl_ent_cache.qstr_ht || !xasl_ent_cache.xid_ht
      || !xasl_ent_cache.oid_ht)
    {
      return ER_FAILED;
    }
  if (xasl_ent_cache.max_entries <= 0)
    {
      return ER_FAILED;
    }

  if (!fp)
    {
      fp = stdout;
    }

  XASL_CACHE_LOCK ();

  fprintf (fp, "\n");
  fprintf (fp,
	   "CACHE        MAX        NUM     LOOKUP        HIT       MISS       FULL\n");
  fprintf (fp, "entry %10d %10d %10d %10d %10d %10d\n",
	   xasl_ent_cache.max_entries, xasl_ent_cache.num,
	   xasl_ent_cache.counter.lookup, xasl_ent_cache.counter.hit,
	   xasl_ent_cache.counter.miss, xasl_ent_cache.counter.full);
  fprintf (fp, "clone %10d %10d %10d %10d %10d %10d\n",
	   xasl_clo_cache.max_clones, xasl_clo_cache.num,
	   xasl_clo_cache.counter.lookup, xasl_clo_cache.counter.hit,
	   xasl_clo_cache.counter.miss, xasl_clo_cache.counter.full);
  fprintf (fp, "\n");

  {
    int i, j, k;
    XASL_CACHE_CLONE *clo;

    for (i = 0, clo = xasl_clo_cache.head; clo; clo = clo->LRU_next)
      {
	i++;
      }
    for (j = 0, clo = xasl_clo_cache.tail; clo; clo = clo->LRU_prev)
      {
	j++;
      }
    for (k = 0, clo = xasl_clo_cache.free_list; clo; clo = clo->next)
      {
	k++;
      }
    fprintf (fp, "CACHE  HEAD_LIST  TAIL_LIST  FREE_LIST    N_ALLOC\n");
    fprintf (fp, "clone %10d %10d %10d %10d\n", i, j, k,
	     xasl_clo_cache.n_alloc);
    fprintf (fp, "\n");
  }

  if (mask & 1)
    {
      (void) mht_dump (fp, xasl_ent_cache.qstr_ht, true,
		       qexec_print_xasl_cache_ent, NULL);
    }
  if (mask & 2)
    {
      (void) mht_dump (fp, xasl_ent_cache.xid_ht, true,
		       qexec_print_xasl_cache_ent, NULL);
    }
  if (mask & 4)
    {
      (void) mht_dump (fp, xasl_ent_cache.oid_ht, true,
		       qexec_print_xasl_cache_ent, NULL);
    }

  XASL_CACHE_UNLOCK ();

  return NO_ERROR;
}

#if defined (RYE_DEBUG)
/*
 * qexec_dump_xasl_cache () -
 *   return: NO_ERROR, or ER_code
 *   fname(in)  :
 *   mask(in)   :
 */
int
qexec_dump_xasl_cache (THREAD_ENTRY * thread_p, const char *fname, int mask)
{
  int rc;
  FILE *fp;

  fp = (fname) ? fopen (fname, "a") : stdout;
  if (!fp)
    {
      fp = stdout;
    }
  rc = qexec_dump_xasl_cache_internal (thread_p, fp, mask);
  if (fp != stdout)
    {
      fclose (fp);
    }

  return rc;
}
#endif

/*
 * qexec_alloc_xasl_cache_ent () - Allocate the entry or get one from the pool
 *   return:
 *   req_size(in)       :
 */
static XASL_CACHE_ENTRY *
qexec_alloc_xasl_cache_ent (int req_size)
{
  /* this function should be called within CSECT_QP_XASL_CACHE */
  POOLED_XASL_CACHE_ENTRY *pent = NULL;

  if (req_size > RESERVED_SIZE_FOR_XASL_CACHE_ENTRY ||
      xasl_cache_entry_pool.free_list == -1)
    {
      /* malloc from the heap if required memory size is bigger than reserved,
         or the pool is exhausted */
      pent =
	(POOLED_XASL_CACHE_ENTRY *) malloc (req_size +
					    ADDITION_FOR_POOLED_XASL_CACHE_ENTRY);
      if (pent != NULL)
	{
	  /* mark as to be freed rather than returning back to the pool */
	  pent->s.next = -2;
	}
      else
	{
	  er_log_debug (ARG_FILE_LINE,
			"qexec_alloc_xasl_cache_ent: allocation failed\n");
	}
    }
  else
    {
      /* get one from the pool */
      pent = &xasl_cache_entry_pool.pool[xasl_cache_entry_pool.free_list];
      xasl_cache_entry_pool.free_list = pent->s.next;
      pent->s.next = -1;
    }
  /* initialize */
  if (pent)
    {
      (void) memset ((void *) &pent->s.entry, 0, req_size);
    }

  return (pent ? &pent->s.entry : NULL);
}

/*
 * qexec_expand_xasl_cache_clo_arr () - Expand alloced clone array
 *   return:
 *   n_exp(in)  :
 */
static XASL_CACHE_CLONE *
qexec_expand_xasl_cache_clo_arr (int n_exp)
{
  XASL_CACHE_CLONE **alloc_arr = NULL, *clo = NULL;
  int i, j, s, n, size;

  size = xasl_clo_cache.max_clones;
  if (size <= 0)
    {
      return xasl_clo_cache.free_list;	/* do nothing */
    }

  n = xasl_clo_cache.n_alloc + n_exp;	/* total number */

  if (xasl_clo_cache.n_alloc == 0)
    {
      s = 0;			/* start */
      alloc_arr = (XASL_CACHE_CLONE **)
	calloc (n, sizeof (XASL_CACHE_CLONE *));

      assert (xasl_clo_cache.alloc_arr == NULL);
    }
  else
    {
      s = xasl_clo_cache.n_alloc;	/* start */
      alloc_arr = (XASL_CACHE_CLONE **)
	realloc (xasl_clo_cache.alloc_arr, sizeof (XASL_CACHE_CLONE *) * n);

      assert (xasl_clo_cache.alloc_arr != NULL);

      if (alloc_arr != NULL)
	{
	  memset (alloc_arr + xasl_clo_cache.n_alloc, 0x00,
		  sizeof (XASL_CACHE_CLONE *) * n_exp);

	  xasl_clo_cache.alloc_arr = alloc_arr;
	}
    }

  if (alloc_arr == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, sizeof (XASL_CACHE_CLONE *) * n);
      return NULL;
    }

  /* alloc blocks */
  for (i = s; i < n; i++)
    {
      alloc_arr[i] = (XASL_CACHE_CLONE *)
	calloc (size, sizeof (XASL_CACHE_CLONE));
      if (alloc_arr[i] == NULL)
	{
	  int k;

	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, sizeof (XASL_CACHE_CLONE) * size);

	  /* free alloced memory */
	  for (k = s; k < i; k++)
	    {
	      free_and_init (alloc_arr[k]);
	    }
	  if (s == 0)
	    {			/* is alloced( not realloced) */
	      free_and_init (alloc_arr);
	    }

	  return NULL;
	}
    }

  /* init link */
  for (i = s; i < n; i++)
    {
      for (j = 0; j < size; j++)
	{
	  clo = &alloc_arr[i][j];

	  /* initialize */
	  QEXEC_INITIALIZE_XASL_CACHE_CLO (clo, NULL);
	  clo->next = &alloc_arr[i][j + 1];
	}
      if (i + 1 < n)
	{
	  clo->next = &alloc_arr[i + 1][0];	/* link to next block */
	}
    }

  if (clo != NULL)
    {
      clo->next = NULL;		/* last link */
    }

  xasl_clo_cache.n_alloc = n;
  xasl_clo_cache.alloc_arr = alloc_arr;

  return &xasl_clo_cache.alloc_arr[s][0];
}

/*
 * qexec_alloc_xasl_cache_clo () - Pop the clone from the free_list, or alloc it
 *   return:
 *   ent(in)    :
 */
static XASL_CACHE_CLONE *
qexec_alloc_xasl_cache_clo (XASL_CACHE_ENTRY * ent)
{
  XASL_CACHE_CLONE *clo;

  if (xasl_clo_cache.free_list == NULL && xasl_clo_cache.max_clones > 0)
    {
      /* need more clones; expand alloced clones */
      xasl_clo_cache.free_list = qexec_expand_xasl_cache_clo_arr (1);
    }

  clo = xasl_clo_cache.free_list;
  if (clo)
    {
      /* delete from free_list */
      xasl_clo_cache.free_list = clo->next;

      /* initialize */
      QEXEC_INITIALIZE_XASL_CACHE_CLO (clo, ent);
    }

  return clo;
}

/*
 * qexec_free_xasl_cache_clo () - Push the clone to free_list and free XASL tree
 *   return: NO_ERROR, or ER_code
 *   cache_clone_p(in)    :
 */
int
qexec_free_xasl_cache_clo (XASL_CACHE_CLONE * clo)
{
  if (!clo)
    {
      return ER_FAILED;
    }

  (void) qexec_clear_xasl (NULL, clo->xasl, true);
#if defined (SERVER_MODE)
  db_destroy_private_heap (clo->xasl->private_heap_id);
#endif

  /* initialize */
  QEXEC_INITIALIZE_XASL_CACHE_CLO (clo, NULL);

  /* add to free_list */
  clo->next = xasl_clo_cache.free_list;
  xasl_clo_cache.free_list = clo;

  return NO_ERROR;
}

/*
 * qexec_append_LRU_xasl_cache_clo () - Append the clone to LRU list tail
 *   return: NO_ERROR, or ER_code
 *   cache_clone_p(in)    :
 */
static int
qexec_append_LRU_xasl_cache_clo (XASL_CACHE_CLONE * clo)
{
  int ret = NO_ERROR;

  /* check the number of XASL cache clones */
  if (xasl_clo_cache.num >= xasl_clo_cache.max_clones)
    {
      XASL_CACHE_ENTRY *ent;
      XASL_CACHE_CLONE *del, *pre, *cur;

      xasl_clo_cache.counter.full++;	/* counter */

      del = xasl_clo_cache.head;	/* get LRU head as victim */
      ent = del->ent_ptr;	/* get entry pointer */

      pre = NULL;
      for (cur = ent->clo_list; cur; cur = cur->next)
	{
	  if (cur == del)
	    {			/* found victim */
	      break;
	    }
	  pre = cur;
	}

      if (!cur)
	{			/* unknown error */
	  er_log_debug (ARG_FILE_LINE,
			"qexec_append_LRU_xasl_cache_clo: not found victim for qstr %s xasl_id { first_vpid { %d %d } temp_vfid { %d %d } }\n",
			ent->sql_info.sql_hash_text,
			ent->xasl_id.first_vpid.pageid,
			ent->xasl_id.first_vpid.volid,
			ent->xasl_id.temp_vfid.fileid,
			ent->xasl_id.temp_vfid.volid);
	  er_log_debug (ARG_FILE_LINE, "\tdel = %p, clo_list = [", del);
	  for (cur = ent->clo_list; cur; cur = cur->next)
	    {
	      er_log_debug (ARG_FILE_LINE, " %p", clo);
	    }
	  er_log_debug (ARG_FILE_LINE, " ]\n");
	  return ER_FAILED;
	}

      /* delete from entry's clone list */
      if (pre == NULL)
	{			/* the first */
	  ent->clo_list = del->next;
	}
      else
	{
	  pre->next = del->next;
	}
      del->next = NULL;		/* cut-off */

      /* delete from LRU list */
      (void) qexec_delete_LRU_xasl_cache_clo (del);

      /* add clone to free_list */
      ret = qexec_free_xasl_cache_clo (del);
    }

  clo->LRU_prev = clo->LRU_next = NULL;	/* init */

  /* append to LRU list */
  if (xasl_clo_cache.head == NULL)
    {				/* the first */
      xasl_clo_cache.head = xasl_clo_cache.tail = clo;
    }
  else
    {
      clo->LRU_prev = xasl_clo_cache.tail;
      xasl_clo_cache.tail->LRU_next = clo;

      xasl_clo_cache.tail = clo;	/* move tail */
    }

  xasl_clo_cache.num++;

  return NO_ERROR;
}

/*
 * qexec_delete_LRU_xasl_cache_clo () - Delete the clone from LRU list
 *   return: NO_ERROR, or ER_code
 *   cache_clone_p(in)    :
 */
static int
qexec_delete_LRU_xasl_cache_clo (XASL_CACHE_CLONE * clo)
{
  if (xasl_clo_cache.head == NULL)
    {				/* is empty LRU list */
      return ER_FAILED;
    }

  /* delete from LRU list */
  if (xasl_clo_cache.head == clo)
    {				/* the first */
      xasl_clo_cache.head = clo->LRU_next;	/* move head */
    }
  else
    {
      clo->LRU_prev->LRU_next = clo->LRU_next;
    }

  if (xasl_clo_cache.tail == clo)
    {				/* the last */
      xasl_clo_cache.tail = clo->LRU_prev;	/* move tail */
    }
  else
    {
      clo->LRU_next->LRU_prev = clo->LRU_prev;
    }
  clo->LRU_prev = clo->LRU_next = NULL;	/* cut-off */

  xasl_clo_cache.num--;

  return NO_ERROR;
}

/*
 * qexec_free_xasl_cache_ent () - Remove the entry from the hash and free it
 *                             Can be used by mht_map_no_key() function
 *   return:
 *   data(in)   :
 *   args(in)   :
 */
static int
qexec_free_xasl_cache_ent (THREAD_ENTRY * thread_p, void *data,
			   UNUSED_ARG void *args)
{
  /* this function should be called within CSECT_QP_XASL_CACHE */
  int ret = NO_ERROR;
  POOLED_XASL_CACHE_ENTRY *pent;
  XASL_CACHE_ENTRY *ent = (XASL_CACHE_ENTRY *) data;

  if (!ent)
    {
      return ER_FAILED;
    }

  /* add clones to free_list */
  if (ent->clo_list)
    {
      XASL_CACHE_CLONE *clo, *next;

      for (clo = ent->clo_list; clo; clo = next)
	{
	  next = clo->next;	/* save next link */
	  clo->next = NULL;	/* cut-off */
	  if (xasl_clo_cache.max_clones > 0)
	    {			/* enable cache clone */
	      /* delete from LRU list */
	      (void) qexec_delete_LRU_xasl_cache_clo (clo);
	    }
	  /* add clone to free_list */
	  ret = qexec_free_xasl_cache_clo (clo);
	}			/* for (cache_clone_p = ent->clo_list; ...) */
    }

  /* if this entry is from the pool return it, else free it */
  pent = POOLED_XASL_CACHE_ENTRY_FROM_XASL_CACHE_ENTRY (ent);
  if (pent->s.next == -2)
    {
      free_and_init (pent);
    }
  else
    {
      /* return it back to the pool */
      (void) memset (&pent->s.entry, 0, sizeof (XASL_CACHE_ENTRY));
      pent->s.next = xasl_cache_entry_pool.free_list;
      xasl_cache_entry_pool.free_list =
	CAST_BUFLEN (pent - xasl_cache_entry_pool.pool);
    }

  mnt_stats_counter (thread_p, MNT_STATS_PLAN_CACHE_DELETE, 1);

  return NO_ERROR;
}				/* qexec_free_xasl_cache_ent() */

/*
 * qexec_lookup_xasl_cache_ent () - Lookup the XASL cache with the query string
 *   return:
 *   qstr(in)   :
 *   user_oid(in)       :
 *
 * NOTE : In SERVER_MODE. If a entry is found in the query string hash table,
 *        this function increases the fix count in the tran_fix_count_array
 *        of the xasl cache entry.
 *        This count must be decreased with qexec_remove_my_tran_id_in_xasl_entry
 *        before current request is finished.
 *
 *        A fixed xasl cache entry cannot be deleted
 *        from the xasl_id hash table when victimized.
 */
XASL_CACHE_ENTRY *
qexec_lookup_xasl_cache_ent (THREAD_ENTRY * thread_p,
			     const char *qstr, const OID * user_oid)
{
  XASL_CACHE_ENTRY *ent;
  XASL_QSTR_HT_KEY key;
#if defined(SERVER_MODE)
  int tran_index;
#endif

  if (xasl_ent_cache.max_entries <= 0 || qstr == NULL)
    {
      return NULL;
    }

  XASL_CACHE_LOCK ();

  MAKE_XASL_QSTR_HT_KEY (key, qstr, user_oid);

  /* look up the hash table with the key */
  ent = (XASL_CACHE_ENTRY *) mht_get (xasl_ent_cache.qstr_ht, &key);
  xasl_ent_cache.counter.lookup++;	/* counter */
  mnt_stats_counter (thread_p, MNT_STATS_PLAN_CACHE_LOOKUP, 1);

  if (ent)
    {
      /* check if it is marked to be deleted */
      if (ent->deletion_marker)
	{
	  /* mark deleted entry can't be found from qstr_ht */
	  assert (false);
	  ent = NULL;
	  goto end;
	}

      /* check age - timeout */
      if (ent && prm_get_integer_value (PRM_ID_XASL_PLAN_CACHE_TIMEOUT) >= 0
	  && (difftime (time (NULL),
			ent->time_created.tv_sec) >
	      prm_get_integer_value (PRM_ID_XASL_PLAN_CACHE_TIMEOUT)))
	{
	  /* delete the entry which is timed out */
	  (void) qexec_delete_xasl_cache_ent (thread_p, ent, NULL);
	  ent = NULL;
	}

      /* finally, we found an useful cache entry to reuse */
      if (ent)
	{
	  /* record my transaction id into the entry
	   * and adjust timestamp and reference counter
	   */
#if defined(SERVER_MODE)
	  tran_index = logtb_get_current_tran_index (thread_p);
	  if (ent->tran_fix_count_array[tran_index] == 0)
	    {
	      ATOMIC_INC_32 (&ent->num_fixed_tran, 1);
	    }
	  ent->tran_fix_count_array[tran_index]++;

	  assert (ent->tran_fix_count_array[tran_index] > 0);
#endif
	  (void) gettimeofday (&ent->time_last_used, NULL);
	  ent->ref_count++;
	}
    }

  if (ent)
    {
      xasl_ent_cache.counter.hit++;	/* counter */
      mnt_stats_counter (thread_p, MNT_STATS_PLAN_CACHE_HIT, 1);
    }
  else
    {
      xasl_ent_cache.counter.miss++;	/* counter */
      mnt_stats_counter (thread_p, MNT_STATS_PLAN_CACHE_MISS, 1);
    }

end:
  XASL_CACHE_UNLOCK ();

  return ent;
}

/*
 * qexec_update_xasl_cache_ent () -
 *   return:
 *   context(in)       : sql_hash_text is used as hash key
 *   stream(in)        : xasl stream, size & xasl_id info
 *   oid(in)    : creator oid
 *   n_oids(in) : # of class_oids
 *   class_oids(in)     : class_oids which have relation with xasl
 *   tcards(in)       : #pages of class_oids
 *   dbval_cnt(in)      :
 *
 * Note: Update XASL cache entry if exist or create new one
 * As a side effect, the given 'xasl_id' can be change if the entry which has
 * the same query is found in the cache
 */
XASL_CACHE_ENTRY *
qexec_update_xasl_cache_ent (THREAD_ENTRY * thread_p,
			     COMPILE_CONTEXT * context,
			     XASL_STREAM * stream,
			     const OID * oid, int n_oids,
			     const OID * class_oids,
			     const int *tcards, int dbval_cnt)
{
  XASL_CACHE_ENTRY *ent, *victim_ent;
  const OID *o;
  int sql_hash_text_len, sql_plan_text_len, sql_user_text_len, i;
  int current_count, max_victim_count;
#if defined(SERVER_MODE)
  bool all_entries_are_fixed = false;
#endif /* SERVER_MODE */
  XASL_NODE_HEADER xasl_header;
  HENTRY_PTR h_entry;
  XASL_QSTR_HT_KEY key;

  if (xasl_ent_cache.max_entries <= 0)
    {
      return NULL;
    }

  XASL_CACHE_LOCK ();

  MAKE_XASL_QSTR_HT_KEY (key, context->sql_hash_text, oid);

  /* check again whether the entry is in the cache */
  ent = (XASL_CACHE_ENTRY *) mht_get (xasl_ent_cache.qstr_ht, &key);
  if (ent != NULL)
    {
      if (ent->deletion_marker)
	{
	  /* mark deleted entry can't be found from qstr_ht */
	  assert (0);
	  ent = NULL;
	  goto end;
	}

      /* the other competing thread which is running the same query
         already updated this entry after that this and the thread had failed
         to find the query in the cache;
         change the given XASL_ID to force to use the cached entry */
      XASL_ID_COPY (stream->xasl_id, &(ent->xasl_id));

      (void) gettimeofday (&ent->time_last_used, NULL);
      ent->ref_count++;

      goto end;
    }

  if ((XASL_CACHE_ENTRY *) mht_get (xasl_ent_cache.xid_ht, stream->xasl_id) !=
      NULL)
    {
      XASL_ID *xasl_id = stream->xasl_id;
      er_log_debug (ARG_FILE_LINE,
		    "qexec_update_xasl_cache_ent: duplicated xasl_id "
		    "{ first_vpid { %d %d } temp_vfid { %d %d } }\n",
		    xasl_id->first_vpid.pageid, xasl_id->first_vpid.volid,
		    xasl_id->temp_vfid.fileid, xasl_id->temp_vfid.volid);
      ent = NULL;
      goto end;
    }

  /* check the number of XASL cache entries; compare with qstr hash entries */
  if ((int) mht_count (xasl_ent_cache.qstr_ht) >= xasl_ent_cache.max_entries)
    {
      /* Cache full! We need to remove some entries from the cache.
       * We will refer to the LRU list of query string hash table
       * to find out victims.
       */
      xasl_ent_cache.counter.full++;	/* counter */
      mnt_stats_counter (thread_p, MNT_STATS_PLAN_CACHE_FULL, 1);

      /* Number of entries to victimize. It will be one in normal case. */
      max_victim_count = (int) mht_count (xasl_ent_cache.qstr_ht)
	- xasl_ent_cache.max_entries + 1;

      h_entry = xasl_ent_cache.qstr_ht->lru_head;
      assert (h_entry != NULL);

      current_count = 0;
      while (current_count < max_victim_count && h_entry)
	{
	  victim_ent = (XASL_CACHE_ENTRY *) h_entry->data;
	  h_entry = h_entry->lru_next;
#if defined (SERVER_MODE)
	  if (victim_ent->num_fixed_tran > 0)
	    {
	      /* This entry is being used. skip this entry */
	      continue;
	    }
#endif
	  (void) qexec_delete_xasl_cache_ent (thread_p, (void *) victim_ent,
					      NULL);
	  current_count++;
	}

      if (current_count == 0)
	{
#if defined (SERVER_MODE)
	  /* we can't remove any entry */
	  all_entries_are_fixed = true;
#else
	  assert (0);
#endif
	}
    }				/* if */

#if defined (SERVER_MODE)
  if (xasl_ent_cache.mark_deleted_list)
    {
      /* We will remove two entries(if there are) in the mark deleted list
       * before add new entry.
       */
      (void) qexec_remove_mark_deleted_xasl_entries (thread_p, 2);
    }
#endif

  /* make new XASL_CACHE_ENTRY */
  sql_hash_text_len = strlen (context->sql_hash_text) + 1;

  sql_plan_text_len = 0;
  if (context->sql_plan_text)
    {
      sql_plan_text_len = strlen (context->sql_plan_text) + 1;
    }

  sql_user_text_len = 0;
  if (context->sql_user_text)
    {
      sql_user_text_len = strlen (context->sql_user_text) + 1;
    }

  /* get new entry from the XASL_CACHE_ENTRY_POOL */
  ent = qexec_alloc_xasl_cache_ent (XASL_CACHE_ENTRY_ALLOC_SIZE
				    (sql_hash_text_len + sql_plan_text_len +
				     sql_user_text_len, n_oids));
  if (ent == NULL)
    {
      goto end;
    }
  /* initialize the entry */
#if defined(SERVER_MODE)
  ATOMIC_TAS_32 (&ent->num_fixed_tran, 0);

  ent->tran_fix_count_array =
    (char *) memset (XASL_CACHE_ENTRY_TRAN_FIX_COUNT_ARRAY (ent),
		     0, MAX_NTRANS * sizeof (char));
#endif
  ent->n_oid_list = n_oids;

  if (class_oids != NULL)
    {
      ent->class_oid_list = memcpy (XASL_CACHE_ENTRY_CLASS_OID_LIST (ent),
				    class_oids, n_oids * sizeof (OID));
    }

  if (tcards != NULL)
    {
      ent->tcard_list = memcpy (XASL_CACHE_ENTRY_TCARD_LIST (ent),
				tcards, n_oids * sizeof (int));
    }

  ent->sql_info.sql_hash_text =
    memcpy (XASL_CACHE_ENTRY_SQL_HASH_TEXT (ent),
	    context->sql_hash_text, sql_hash_text_len);

  if (context->sql_plan_text)
    {
      ent->sql_info.sql_plan_text =
	memcpy (XASL_CACHE_ENTRY_SQL_PLAN_TEXT (ent),
		context->sql_plan_text, sql_plan_text_len);
    }

  if (context->sql_user_text)
    {
      ent->sql_info.sql_user_text =
	memcpy (XASL_CACHE_ENTRY_SQL_USER_TEXT (ent),
		context->sql_user_text, sql_user_text_len);
    }

  XASL_ID_COPY (&ent->xasl_id, stream->xasl_id);
  qfile_load_xasl_node_header (thread_p, stream->xasl_id, &xasl_header);
  ent->xasl_header_flag = xasl_header.xasl_flag;

  (void) gettimeofday (&ent->time_created, NULL);
  (void) gettimeofday (&ent->time_last_used, NULL);
  ent->ref_count = 0;
  ent->deletion_marker = false;
  ent->dbval_cnt = dbval_cnt;
  ent->clo_list = NULL;

  MAKE_XASL_QSTR_HT_KEY (ent->qstr_ht_key, ent->sql_info.sql_hash_text, oid);

  /* insert (or update) the entry into the query string hash table */
  if (mht_put_new (xasl_ent_cache.qstr_ht, &ent->qstr_ht_key, ent) == NULL)
    {
      er_log_debug (ARG_FILE_LINE,
		    "qexec_update_xasl_cache_ent: mht_put failed for sql_hash_text %s\n",
		    ent->sql_info.sql_hash_text);
      (void) qexec_delete_xasl_cache_ent (thread_p, ent, NULL);
      ent = NULL;
      goto end;
    }

  if (xasl_ent_cache.qstr_ht->build_lru_list)
    {
      if (xasl_ent_cache.qstr_ht->lru_tail
	  && xasl_ent_cache.qstr_ht->lru_tail->data == ent)
	{
	  ent->qstr_ht_entry_ptr = xasl_ent_cache.qstr_ht->lru_tail;
	}
      else
	{
	  assert (0);
	}
    }

  mnt_stats_gauge (thread_p, MNT_STATS_PLAN_CACHE_QUERY_STRING_HASH_ENTRIES,
		   mht_count (xasl_ent_cache.qstr_ht));

  /* insert (or update) the entry into the xasl file id hash table */
  if (mht_put_new (xasl_ent_cache.xid_ht, &ent->xasl_id, ent) == NULL)
    {
      er_log_debug (ARG_FILE_LINE,
		    "qexec_update_xasl_cache_ent: mht_put failed for xasl_id { first_vpid { %d %d } temp_vfid { %d %d } }\n",
		    ent->xasl_id.first_vpid.pageid,
		    ent->xasl_id.first_vpid.volid,
		    ent->xasl_id.temp_vfid.fileid,
		    ent->xasl_id.temp_vfid.volid);
      (void) qexec_delete_xasl_cache_ent (thread_p, ent, NULL);
      ent = NULL;
      goto end;
    }
  mnt_stats_gauge (thread_p, MNT_STATS_PLAN_CACHE_XASL_ID_HASH_ENTRIES,
		   mht_count (xasl_ent_cache.xid_ht));

  /* insert the entry into the class oid hash table
     Note that mht_put2() allows mutiple data with the same key */
  for (i = 0, o = ent->class_oid_list; i < n_oids; i++, o++)
    {
      if (mht_put2_new (xasl_ent_cache.oid_ht, o, ent) == NULL)
	{
	  er_log_debug (ARG_FILE_LINE,
			"qexec_update_xasl_cache_ent: mht_put2 failed for class_oid { %d %d %d }\n",
			o->pageid, o->slotid, o->volid);
	  (void) qexec_delete_xasl_cache_ent (thread_p, ent, NULL);
	  ent = NULL;
	  goto end;
	}
    }				/* for (i = 0, ...) */
  mnt_stats_gauge (thread_p, MNT_STATS_PLAN_CACHE_CLASS_OID_HASH_ENTRIES,
		   mht_count (xasl_ent_cache.oid_ht));

  xasl_ent_cache.num++;
  mnt_stats_counter (thread_p, MNT_STATS_PLAN_CACHE_ADD, 1);

#if defined (SERVER_MODE)
  if (all_entries_are_fixed)
    {
      int current = (int) mht_count (xasl_ent_cache.qstr_ht);

      assert_release (current > xasl_ent_cache.max_entries);
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
	      ER_ALL_PLAN_CACHE_ENTRIES_ARE_FIXED, 2, current,
	      xasl_ent_cache.max_entries);
    }
#endif

end:
  XASL_CACHE_UNLOCK ();

  return ent;
}

/*
 * qexec_remove_my_tran_id_in_xasl_entry () -
 *   return: NO_ERROR, or ER_code
 *   ent(in)    :
 *   unfix_all(in) :
 */
int
qexec_remove_my_tran_id_in_xasl_entry (UNUSED_ARG THREAD_ENTRY * thread_p,
				       UNUSED_ARG XASL_CACHE_ENTRY * ent,
				       UNUSED_ARG bool unfix_all)
{
#if defined(SERVER_MODE)
  int tran_index;

  tran_index = logtb_get_current_tran_index (thread_p);

  if (ent->tran_fix_count_array[tran_index] == 0)
    {
      /* This entry was never fixed before */
      return NO_ERROR;
    }

  /* assertion */
  if (ent->num_fixed_tran <= 0)
    {
      assert_release (0);
      ent->tran_fix_count_array[tran_index] = 0;

      ATOMIC_TAS_32 (&ent->num_fixed_tran, 0);

      return ER_FAILED;
    }

  ent->tran_fix_count_array[tran_index]--;

  /* assertion */
  if (ent->tran_fix_count_array[tran_index] < 0)
    {
      assert_release (ent->tran_fix_count_array[tran_index] >= 0);
      ent->tran_fix_count_array[tran_index] = 0;

      return ER_FAILED;
    }

  if (unfix_all && ent->tran_fix_count_array[tran_index] > 0)
    {
      /* xasl cache entry's fix/unfix count
       * in this request is not matched.
       * xasl_cache_fix function :
       *   qexec_lookup_xasl_cache_ent
       *   qexec_check_xasl_cache_ent_by_xasl
       * xasl_cache_unfix function :
       *   qexec_remove_my_tran_id_in_xasl_entry
       */
      assert_release (ent->tran_fix_count_array[tran_index] == 0);

      /* reset fix count */
      ent->tran_fix_count_array[tran_index] = 0;
    }

  if (ent->tran_fix_count_array[tran_index] == 0)
    {
      ATOMIC_INC_32 (&ent->num_fixed_tran, -1);
    }
#endif /* SERVER_MODE */

  return NO_ERROR;
}

/*
 * qexec_check_xasl_cache_ent_by_xasl () - Check the XASL cache with the XASL ID
 *   return:
 *   xasl_id(in)        :
 *   dbval_cnt(in)      :
 *   clop(in)   :
 *
 * NOTE : In SERVER_MODE. If a entry is found in the query string hash table,
 *        this function increases the fix count in the tran_fix_count_array
 *        of the xasl cache entry.
 *        This count must be decreased with qexec_remove_my_tran_id_in_xasl_entry
 *        before current request is finished.
 *
 *        A fixed xasl cache entry cannot be deleted
 *        from the xasl_id hash table when victimized.
 *
 */
XASL_CACHE_ENTRY *
qexec_check_xasl_cache_ent_by_xasl (THREAD_ENTRY * thread_p,
				    const XASL_ID * xasl_id,
				    int dbval_cnt,
				    XASL_CACHE_CLONE ** clop, bool save_clop)
{
  XASL_CACHE_ENTRY *ent;
  XASL_CACHE_CLONE *clo;

  if (xasl_id == NULL)
    {
      assert (false);
      return NULL;
    }

  if (xasl_ent_cache.max_entries <= 0)
    {
      return NULL;
    }

  ent = NULL;			/* init */

  XASL_CACHE_LOCK ();

  /* look up the hash table with the key, which is XASL ID */
  ent = (XASL_CACHE_ENTRY *) mht_get (xasl_ent_cache.xid_ht, xasl_id);

  if (ent)
    {
      if (ent->deletion_marker)
	{
	  ent = NULL;
	}

      /* check the stored time of the XASL */
      if (ent
	  && !CACHE_TIME_EQ (&(ent->xasl_id.time_stored),
			     &(xasl_id->time_stored)))
	{
	  assert ((xasl_id->time_stored.sec < ent->xasl_id.time_stored.sec)
		  || (xasl_id->time_stored.usec <
		      ent->xasl_id.time_stored.usec));

	  er_log_debug (ARG_FILE_LINE,
			"qexec_check_xasl_cache_ent_by_xasl: stored time "
			"mismatch %d sec %d usec vs %d sec %d usec\n",
			ent->xasl_id.time_stored.sec,
			ent->xasl_id.time_stored.usec,
			xasl_id->time_stored.sec, xasl_id->time_stored.usec);
	  ent = NULL;
	}

      /* check the number of parameters of the XASL */
      if (ent && dbval_cnt > 0 && ent->dbval_cnt > dbval_cnt)
	{
#if 1				/* TODO - trace me; ent is problematical */
	  assert (false);
#endif

	  er_log_debug (ARG_FILE_LINE,
			"qexec_check_xasl_cache_ent_by_xasl: dbval_cnt "
			"mismatch ent->dbval_cnt=%d vs dbval_cnt=%d\n",
			ent->dbval_cnt, dbval_cnt);
	  ent = NULL;
	}

#if defined(SERVER_MODE)
      if (!save_clop && ent)
	{
	  int tran_index = logtb_get_current_tran_index (thread_p);
	  if (ent->tran_fix_count_array[tran_index] == 0)
	    {
	      ATOMIC_INC_32 (&ent->num_fixed_tran, 1);
	    }
	  ent->tran_fix_count_array[tran_index]++;
	  assert (ent->tran_fix_count_array[tran_index] > 0);
	}
#endif

      if (!save_clop && ent && xasl_ent_cache.qstr_ht->build_lru_list)
	{
	  if (ent->qstr_ht_entry_ptr)
	    {
	      mht_adjust_lru_list (xasl_ent_cache.qstr_ht,
				   ent->qstr_ht_entry_ptr);
	      assert (mht_get (xasl_ent_cache.qstr_ht,
			       (void *) &ent->qstr_ht_key) == ent);
	    }
	  else
	    {
	      assert (false);
	    }
	}
    }

  /* check for cache clone */
  if (clop)
    {
      clo = *clop;
      /* check for cache clone */
      if (ent)
	{
	  if (clo)
	    {			/* push clone back to free_list */
	      /* append to LRU list */
	      if (xasl_clo_cache.max_clones > 0	/* enable cache clone */
		  && qexec_append_LRU_xasl_cache_clo (clo) == NO_ERROR)
		{
		  /* add to clone list */
		  clo->next = ent->clo_list;
		  ent->clo_list = clo;
		}
	      else
		{
		  /* give up; add to free_list */
		  (void) qexec_free_xasl_cache_clo (clo);
		}
	    }
	  else
	    {			/* pop clone from free_list */
	      xasl_clo_cache.counter.lookup++;	/* counter */

	      clo = ent->clo_list;
	      if (clo)
		{		/* already cloned */
		  xasl_clo_cache.counter.hit++;	/* counter */

		  /* delete from clone list */
		  ent->clo_list = clo->next;
		  clo->next = NULL;	/* cut-off */

		  /* delete from LRU list */
		  (void) qexec_delete_LRU_xasl_cache_clo (clo);
		}
	      else
		{
		  xasl_clo_cache.counter.miss++;	/* counter */

		  if (xasl_clo_cache.max_clones > 0)
		    {
		      clo = qexec_alloc_xasl_cache_clo (ent);
		    }
		}
	    }
	}
      else
	{
	  if (clo)
	    {			/* push clone back to free_list */
	      /* give up; add to free_list */
	      (void) qexec_free_xasl_cache_clo (clo);
	    }
	  else
	    {			/* pop clone from free_list */
	      xasl_clo_cache.counter.lookup++;	/* counter */

	      xasl_clo_cache.counter.miss++;	/* counter */
	    }
	}

      *clop = clo;
    }

  XASL_CACHE_UNLOCK ();

  if (!save_clop && ent == NULL)
    {
      mnt_stats_counter (thread_p, MNT_STATS_PLAN_CACHE_INVALID_XASL_ID, 1);
    }

  return ent;
}

/*
 * qexec_remove_xasl_cache_ent_by_class () - Remove the XASL cache entries by
 *                                        class OID
 *   return: NO_ERROR, or ER_code
 *   class_oid(in)      :
 */
int
qexec_remove_xasl_cache_ent_by_class (THREAD_ENTRY * thread_p,
				      const OID * class_oid, int force_remove)
{
  XASL_CACHE_ENTRY *ent;
  void *last;

  if (xasl_ent_cache.max_entries <= 0)
    {
      return NO_ERROR;
    }

  XASL_CACHE_LOCK ();

  /* for all entries in the class/serial oid hash table
     Note that mht_put2() allows mutiple data with the same key,
     so we have to use mht_get2() */
  last = NULL;
  do
    {
      /* look up the hash table with the key */
      ent = (XASL_CACHE_ENTRY *) mht_get2 (xasl_ent_cache.oid_ht, class_oid,
					   &last);
      if (ent)
	{
	  /* remove my transaction id from the entry and do compaction */
	  (void) qexec_remove_my_tran_id_in_xasl_entry (thread_p, ent, false);

	  if (qexec_delete_xasl_cache_ent (thread_p, ent, &force_remove) ==
	      NO_ERROR)
	    {
	      last = NULL;	/* for mht_get2() */
	    }
	}
    }
  while (ent);

  XASL_CACHE_UNLOCK ();

  return NO_ERROR;
}

/*
 * qexec_remove_xasl_cache_ent_by_qstr () - Remove the XASL cache entries by
 *                                       query string
 *   return: NO_ERROR, or ER_code
 *   qstr(in)   :
 *   user_oid(in)       :
 */
int
qexec_remove_xasl_cache_ent_by_qstr (THREAD_ENTRY * thread_p,
				     const char *qstr, const OID * user_oid)
{
  XASL_CACHE_ENTRY *ent;
  XASL_QSTR_HT_KEY key;

  if (xasl_ent_cache.max_entries <= 0)
    {
      return NO_ERROR;
    }

  XASL_CACHE_LOCK ();

  MAKE_XASL_QSTR_HT_KEY (key, qstr, user_oid);

  /* look up the hash table with the key, which is query string */
  ent = (XASL_CACHE_ENTRY *) mht_get (xasl_ent_cache.qstr_ht, &key);
  if (ent)
    {
      /* remove my transaction id from the entry and do compaction */
      (void) qexec_remove_my_tran_id_in_xasl_entry (thread_p, ent, false);
      (void) qexec_delete_xasl_cache_ent (thread_p, ent, NULL);
    }

  XASL_CACHE_UNLOCK ();

  return NO_ERROR;
}

/*
 * qexec_remove_xasl_cache_ent_by_xasl () - Remove the XASL cache entries by
 *                                       XASL ID
 *   return: NO_ERROR, or ER_code
 *   xasl_id(in)        :
 */
int
qexec_remove_xasl_cache_ent_by_xasl (THREAD_ENTRY * thread_p,
				     const XASL_ID * xasl_id)
{
  XASL_CACHE_ENTRY *ent;

  if (xasl_ent_cache.max_entries <= 0)
    {
      return NO_ERROR;
    }

  XASL_CACHE_LOCK ();

  /* look up the hash table with the key, which is XASL ID */
  ent = (XASL_CACHE_ENTRY *) mht_get (xasl_ent_cache.xid_ht, xasl_id);
  if (ent)
    {
      /* remove my transaction id from the entry and do compaction */
      (void) qexec_remove_my_tran_id_in_xasl_entry (thread_p, ent, false);
      (void) qexec_delete_xasl_cache_ent (thread_p, ent, NULL);
    }

  XASL_CACHE_UNLOCK ();

  return NO_ERROR;
}

/*
 * qexec_delete_xasl_cache_ent () - Delete a XASL cache entry
 *                               Can be used by mht_map_no_key() function
 *   return:
 *   data(in)   :
 *   args(in)   :
 */
static int
qexec_delete_xasl_cache_ent (THREAD_ENTRY * thread_p, void *data, void *args)
{
  XASL_CACHE_ENTRY *ent = (XASL_CACHE_ENTRY *) data;
  int rc;
  const OID *o;
  int i;
  int force_delete = 0;

  if (args)
    {
      force_delete = *((int *) args);
    }

  if (!ent)
    {
      return ER_FAILED;
    }

  if (ent->deletion_marker)
    {
      /* This entry has already been deleted */
      return ER_FAILED;
    }

#if defined(SERVER_MODE)
  if (ent->num_fixed_tran == 0 || force_delete)
#endif /* SERVER_MODE */
    {
      /* remove the entry from query string hash table */
      if (mht_rem2 (xasl_ent_cache.qstr_ht, &ent->qstr_ht_key, ent,
		    NULL, NULL) != NO_ERROR)
	{
	  er_log_debug (ARG_FILE_LINE,
			"qexec_delete_xasl_cache_ent: mht_rem2 failed for qstr %s\n",
			ent->sql_info.sql_hash_text);
	}
      ent->qstr_ht_entry_ptr = NULL;
      mnt_stats_gauge (thread_p,
		       MNT_STATS_PLAN_CACHE_QUERY_STRING_HASH_ENTRIES,
		       mht_count (xasl_ent_cache.qstr_ht));

      /* remove the entry from xasl file id hash table */
      if (mht_rem2 (xasl_ent_cache.xid_ht, &ent->xasl_id, ent,
		    NULL, NULL) != NO_ERROR)
	{
	  er_log_debug (ARG_FILE_LINE,
			"qexec_delete_xasl_cache_ent: mht_rem failed for"
			" xasl_id { first_vpid { %d %d } temp_vfid { %d %d } }\n",
			ent->xasl_id.first_vpid.pageid,
			ent->xasl_id.first_vpid.volid,
			ent->xasl_id.temp_vfid.fileid,
			ent->xasl_id.temp_vfid.volid);
	}
      mnt_stats_gauge (thread_p, MNT_STATS_PLAN_CACHE_XASL_ID_HASH_ENTRIES,
		       mht_count (xasl_ent_cache.xid_ht));

      /* remove the entries from class/serial oid hash table */
      for (i = 0, o = ent->class_oid_list; i < ent->n_oid_list; i++, o++)
	{
	  if (mht_rem2 (xasl_ent_cache.oid_ht, o, ent, NULL, NULL) !=
	      NO_ERROR)
	    {
	      er_log_debug (ARG_FILE_LINE,
			    "qexec_delete_xasl_cache_ent: mht_rem failed for"
			    " class_oid { %d %d %d }\n",
			    ent->class_oid_list[i].pageid,
			    ent->class_oid_list[i].slotid,
			    ent->class_oid_list[i].volid);
	    }
	}
      mnt_stats_gauge (thread_p, MNT_STATS_PLAN_CACHE_CLASS_OID_HASH_ENTRIES,
		       mht_count (xasl_ent_cache.oid_ht));

      /* destroy the temp file of XASL_ID */
      if (file_destroy (thread_p, &(ent->xasl_id.temp_vfid)) != NO_ERROR)
	{
	  er_log_debug (ARG_FILE_LINE,
			"qexec_delete_xasl_cache_ent: fl_destroy failed for vfid { %d %d }\n",
			ent->xasl_id.temp_vfid.fileid,
			ent->xasl_id.temp_vfid.volid);
	}

#if defined (SERVER_MODE)
      if (force_delete && ent->num_fixed_tran > 0)
	{
	  /* This entry is not removed now, just added to mark deleted list.
	   * Entries in mark deleted list will be removed later.
	   */
	  rc = qexec_mark_delete_xasl_entry (thread_p, ent, true);
	  return rc;
	}
#endif
      rc = qexec_free_xasl_cache_ent (thread_p, ent, NULL);
      xasl_ent_cache.num--;	/* counter */
    }
#if defined (SERVER_MODE)
  else
    {
      /* remove from the query string hash table to allow
         new XASL with the same query string to be registered */
      rc = NO_ERROR;
      if (mht_rem2 (xasl_ent_cache.qstr_ht, &ent->qstr_ht_key, ent,
		    NULL, NULL) != NO_ERROR)
	{
	  er_log_debug (ARG_FILE_LINE,
			"qexec_delete_xasl_cache_ent: mht_rem2 failed for qstr %s\n",
			ent->sql_info.sql_hash_text);
	  rc = ER_FAILED;
	}
      ent->qstr_ht_entry_ptr = NULL;
      mnt_stats_gauge (thread_p,
		       MNT_STATS_PLAN_CACHE_QUERY_STRING_HASH_ENTRIES,
		       mht_count (xasl_ent_cache.qstr_ht));

      if (rc == NO_ERROR)
	{
	  rc = qexec_mark_delete_xasl_entry (thread_p, ent, false);
	}
    }
#endif

  return rc;
}

#if defined (SERVER_MODE)
/*
 * qexec_remove_mark_deleted_xasl_entries :
 *
 *   return:
 *   remove_count (in) :
 */
static int
qexec_remove_mark_deleted_xasl_entries (THREAD_ENTRY * thread_p,
					int remove_count)
{
  int i;
  XASL_CACHE_MARK_DELETED_LIST *current;
  XASL_CACHE_MARK_DELETED_LIST *prev, *next;

  i = 0;
  current = xasl_ent_cache.mark_deleted_list;
  prev = NULL;

  while (current && i < remove_count)
    {
      XASL_CACHE_ENTRY *cache_ent = current->entry;
      next = current->next;

      if (cache_ent->num_fixed_tran == 0)
	{
	  /* now we can free this entry */
	  if (prev == NULL)
	    {
	      /* The first entry */
	      xasl_ent_cache.mark_deleted_list = next;
	    }
	  else
	    {
	      prev->next = next;
	    }

	  if (current == xasl_ent_cache.mark_deleted_list_tail)
	    {
	      assert (next == NULL);
	      xasl_ent_cache.mark_deleted_list_tail = prev;
	    }

	  (void) qexec_free_mark_deleted_xasl_entry (thread_p, cache_ent,
						     current->
						     removed_from_hash);
	  free_and_init (current);
	  i++;
	}
      else
	{
	  /* someone is using this entry. skip this */
	  prev = current;
	}

      current = next;
    }

#if !defined (NDEBUG)
  if (xasl_ent_cache.mark_deleted_list == NULL)
    {
      assert (xasl_ent_cache.mark_deleted_list_tail == NULL);
    }

  current = xasl_ent_cache.mark_deleted_list;

  while (current && current->next)
    {
      current = current->next;
    }

  if (current != xasl_ent_cache.mark_deleted_list_tail)
    {
      assert (0);
      xasl_ent_cache.mark_deleted_list_tail = current;
    }
#endif

  return NO_ERROR;
}

/*
 * qexec_mark_delete_xasl_entry :
 *
 *   return:
 *   ent (in) :
 *   removed_from_hash (in) :
 */
static int
qexec_mark_delete_xasl_entry (UNUSED_ARG THREAD_ENTRY * thread_p,
			      XASL_CACHE_ENTRY * ent, bool removed_from_hash)
{
  XASL_CACHE_MARK_DELETED_LIST *deleted_entry;

  if (ent == NULL)
    {
      assert (0);
      return ER_FAILED;
    }

#if !defined(NDEBUG)
  deleted_entry = xasl_ent_cache.mark_deleted_list;
  while (deleted_entry)
    {
      if (deleted_entry->entry == ent)
	{
	  /* assertion - I'm already in the list */
	  assert (0);
	  return ER_FAILED;
	}
      deleted_entry = deleted_entry->next;
    }
#endif

  deleted_entry = malloc (sizeof (XASL_CACHE_MARK_DELETED_LIST));

  if (deleted_entry == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      sizeof (XASL_CACHE_MARK_DELETED_LIST));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  assert_release (ent->deletion_marker == false);
  ent->deletion_marker = true;

  deleted_entry->entry = ent;
  deleted_entry->next = NULL;
  deleted_entry->removed_from_hash = removed_from_hash;

  if (xasl_ent_cache.mark_deleted_list_tail == NULL)
    {
      xasl_ent_cache.mark_deleted_list = deleted_entry;
    }
  else
    {
      xasl_ent_cache.mark_deleted_list_tail->next = deleted_entry;
    }

  xasl_ent_cache.mark_deleted_list_tail = deleted_entry;

  return NO_ERROR;
}

/*
 * qexec_free_mark_deleted_xasl_entry :
 *
 *   return:
 *   ent (in);
 *   remove_count (in) :
 */
static int
qexec_free_mark_deleted_xasl_entry (THREAD_ENTRY * thread_p,
				    XASL_CACHE_ENTRY * ent,
				    bool removed_from_hash)
{
  int i;
  const OID *o;

  if (!ent)
    {
      return ER_FAILED;
    }

  assert_release (ent->num_fixed_tran == 0);
  assert_release (ent->deletion_marker);

  if (removed_from_hash == false)
    {
      /* We should remove this entry from the xasl_id ht & class oid ht */
      if (mht_rem2 (xasl_ent_cache.xid_ht, &ent->xasl_id, ent,
		    NULL, NULL) != NO_ERROR)
	{
	  er_log_debug (ARG_FILE_LINE,
			"qexec_free_mark_deleted_xasl_entry: mht_rem2 failed for"
			" xasl_id { first_vpid { %d %d } temp_vfid { %d %d } }\n",
			ent->xasl_id.first_vpid.pageid,
			ent->xasl_id.first_vpid.volid,
			ent->xasl_id.temp_vfid.fileid,
			ent->xasl_id.temp_vfid.volid);
	}
      mnt_stats_gauge (thread_p, MNT_STATS_PLAN_CACHE_XASL_ID_HASH_ENTRIES,
		       mht_count (xasl_ent_cache.xid_ht));

      for (i = 0, o = ent->class_oid_list; i < ent->n_oid_list; i++, o++)
	{
	  if (mht_rem2 (xasl_ent_cache.oid_ht, o, ent, NULL, NULL) !=
	      NO_ERROR)
	    {
	      er_log_debug (ARG_FILE_LINE,
			    "qexec_free_mark_deleted_xasl_entry: mht_rem2 failed for"
			    " class_oid { %d %d %d }\n",
			    ent->class_oid_list[i].pageid,
			    ent->class_oid_list[i].slotid,
			    ent->class_oid_list[i].volid);
	    }
	}
      mnt_stats_gauge (thread_p, MNT_STATS_PLAN_CACHE_CLASS_OID_HASH_ENTRIES,
		       mht_count (xasl_ent_cache.oid_ht));

      /* destroy the temp file of XASL_ID */
      if (file_destroy (thread_p, &(ent->xasl_id.temp_vfid)) != NO_ERROR)
	{
	  er_log_debug (ARG_FILE_LINE,
			"qexec_free_mark_deleted_xasl_entry: file_destroy failed for vfid { %d %d }\n",
			ent->xasl_id.temp_vfid.fileid,
			ent->xasl_id.temp_vfid.volid);
	}
    }

  (void) qexec_free_xasl_cache_ent (thread_p, ent, NULL);
  xasl_ent_cache.num--;

  return NO_ERROR;
}
#endif

/*
 * qexec_remove_all_xasl_cache_ent_by_xasl () - Remove all XASL cache entries
 *   return: NO_ERROR, or ER_code
 */
int
qexec_remove_all_xasl_cache_ent_by_xasl (THREAD_ENTRY * thread_p)
{
  int rc;

  if (xasl_ent_cache.max_entries <= 0)
    {
      return ER_FAILED;
    }

  rc = NO_ERROR;		/* init */

  XASL_CACHE_LOCK ();

  if (mht_map_no_key (thread_p, xasl_ent_cache.qstr_ht,
		      qexec_delete_xasl_cache_ent, NULL) != NO_ERROR)
    {
      rc = ER_FAILED;
    }

#if defined (SERVER_MODE)
  if (xasl_ent_cache.mark_deleted_list)
    {
      (void) qexec_remove_mark_deleted_xasl_entries (thread_p,
						     xasl_ent_cache.num);
    }
#endif

  XASL_CACHE_UNLOCK ();

  return rc;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * replace_null_arith () -
 *   return:
 *   regu_var(in)       :
 *   set_dbval(in)      :
 */
static REGU_VARIABLE *
replace_null_arith (REGU_VARIABLE * regu_var, DB_VALUE * set_dbval)
{
  REGU_VARIABLE *ret;

  if (!regu_var || !regu_var->value.arithptr)
    {
      return NULL;
    }

  ret = replace_null_dbval (regu_var->value.arithptr->leftptr, set_dbval);
  if (ret)
    {
      return ret;
    }

  ret = replace_null_dbval (regu_var->value.arithptr->rightptr, set_dbval);
  if (ret)
    {
      return ret;
    }

  ret = replace_null_dbval (regu_var->value.arithptr->thirdptr, set_dbval);
  if (ret)
    {
      return ret;
    }

  return NULL;
}

/*
 * replace_null_dbval () -
 *   return:
 *   regu_var(in)       :
 *   set_dbval(in)      :
 */
static REGU_VARIABLE *
replace_null_dbval (REGU_VARIABLE * regu_var, DB_VALUE * set_dbval)
{
  if (!regu_var)
    {
      return NULL;
    }

  if (regu_var->type == TYPE_DBVAL)
    {
      regu_var->value.dbval = *set_dbval;
      return regu_var;
    }
  else if (regu_var->type == TYPE_INARITH || regu_var->type == TYPE_OUTARITH)
    {
      return replace_null_arith (regu_var, set_dbval);
    }

  return NULL;
}

static int *
tranid_lsearch (const int *key, int *base, int *nmemb)
{
  int *result;

  result = tranid_lfind (key, base, nmemb);
  if (result == NULL)
    {
      result = &base[(*nmemb)++];
      *result = *key;
    }

  return result;
}

static int *
tranid_lfind (const int *key, const int *base, int *nmemb)
{
  const int *result = base;
  int cnt = 0;

  while (cnt < *nmemb && *key != *result)
    {
      result++;
      cnt++;
    }

  return ((cnt < *nmemb) ? (int *) result : NULL);
}
#endif

/*
 * qexec_insert_tuple_into_list () - helper function for inserting a tuple
 *    into a list file
 *  return:
 *  list_id(in/out):
 *  xasl(in):
 *  vd(in):
 *  tplrec(in):
 */
int
qexec_insert_tuple_into_list (THREAD_ENTRY * thread_p,
			      QFILE_LIST_ID * list_id,
			      OUTPTR_LIST * outptr_list,
			      VAL_DESCR * vd, QFILE_TUPLE_RECORD * tplrec)
{
  QPROC_TPLDESCR_STATUS tpldescr_status;

  tpldescr_status = qexec_generate_tuple_descriptor (thread_p,
						     list_id, outptr_list,
						     vd);
  if (tpldescr_status == QPROC_TPLDESCR_FAILURE)
    {
      return ER_FAILED;
    }

  switch (tpldescr_status)
    {
    case QPROC_TPLDESCR_SUCCESS:
      /* generate tuple into list file page */
      if (qfile_generate_tuple_into_list (thread_p, list_id, T_NORMAL)
	  != NO_ERROR)
	{
	  return ER_FAILED;
	}
      break;

    case QPROC_TPLDESCR_RETRY_SET_TYPE:
    case QPROC_TPLDESCR_RETRY_BIG_REC:
      /* BIG QFILE_TUPLE or a SET-field is included */
      if (tplrec->tpl == NULL)
	{
	  /* allocate tuple descriptor */
	  tplrec->size = DB_PAGESIZE;
	  tplrec->tpl = (QFILE_TUPLE) malloc (DB_PAGESIZE);
	  if (tplrec->tpl == NULL)
	    {
	      return ER_FAILED;
	    }
	}

      if (qdata_copy_valptr_list_to_tuple (thread_p, list_id, outptr_list, vd,
					   tplrec) != NO_ERROR
	  || qfile_add_tuple_to_list (thread_p, list_id,
				      tplrec->tpl) != NO_ERROR)
	{
	  return ER_FAILED;
	}
      break;

    default:
      break;
    }

  return NO_ERROR;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * qexec_set_tuple_column_value () - helper function for writing a column
 *    value into a tuple
 *  return:
 *  tpl(in/out):
 *  index(in):
 *  valp(in):
 *  domain(in):
 */
int
qexec_set_tuple_column_value (QFILE_TUPLE tpl, int index,
			      DB_VALUE * valp, TP_DOMAIN * domain)
{
  QFILE_TUPLE_VALUE_FLAG flag;
  char *ptr;
  int length;
  PR_TYPE *pr_type;
  OR_BUF buf;

  flag = (QFILE_TUPLE_VALUE_FLAG) qfile_locate_tuple_value (tpl, index,
							    &ptr, &length);
  if (flag == V_BOUND)
    {
      pr_type = domain->type;
      if (pr_type == NULL)
	{
	  return ER_FAILED;
	}

      OR_BUF_INIT (buf, ptr, length);

      if ((*(pr_type->data_writeval)) (&buf, valp) != NO_ERROR)
	{
	  return ER_FAILED;
	}
    }
  else
    {
      return ER_FAILED;
    }

  return NO_ERROR;
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * qexec_get_tuple_column_value () - helper function for reading a column
 *    value from a tuple
 *  return:
 *  tpl(in):
 *  index(in):
 *  index_domain(in):
 *  valp(out):
 */
int
qexec_get_tuple_column_value (QFILE_TUPLE tpl, int index,
			      TP_DOMAIN * index_domain, DB_VALUE * valp)
{
  QFILE_TUPLE_VALUE_FLAG flag;
  char *ptr;
  int length;
  PR_TYPE *pr_type;
  OR_BUF buf;

  flag = (QFILE_TUPLE_VALUE_FLAG) qfile_locate_tuple_value (tpl, index,
							    &ptr, &length);
  if (flag == V_BOUND)
    {
      pr_type = index_domain->type;
      if (pr_type == NULL)
	{
	  return ER_FAILED;
	}

      OR_BUF_INIT (buf, ptr, length);

      if ((*(pr_type->data_readval)) (&buf, valp, index_domain,
				      -1, false) != NO_ERROR)
	{
	  return ER_FAILED;
	}
    }
  else
    {
      DB_MAKE_NULL (valp);
    }

  return NO_ERROR;
}

/*
 * qexec_gby_finalize_group_val_list () -
 *   return:
 *   gbstate(in):
 *   N(in):
 */
static void
qexec_gby_finalize_group_val_list (UNUSED_ARG THREAD_ENTRY * thread_p,
				   GROUPBY_STATE * gbstate, int N)
{
  int i;
  QPROC_DB_VALUE_LIST gby_vallist;

  if (gbstate->state != NO_ERROR)
    {
      return;
    }

  if (gbstate->g_dim == NULL || N >= gbstate->g_dim_levels)
    {
      assert (false);
      return;
    }

  if (gbstate->g_dim[N].d_flag & GROUPBY_DIM_FLAG_GROUP_BY)
    {
      assert (N == 0);
      return;			/* nop */
    }

  /* set to NULL (in the summary tuple) the columns that failed comparison */
  if (gbstate->g_val_list)
    {
      assert (N > 0);
      assert (gbstate->g_dim[N].d_flag & GROUPBY_DIM_FLAG_ROLLUP);

      i = 0;
      gby_vallist = gbstate->g_val_list->valp;

      while (gby_vallist)
	{
	  if (i >= N - 1)
	    {
	      (void) pr_clear_value (gby_vallist->val);
	      DB_MAKE_NULL (gby_vallist->val);
	    }
	  i++;
	  gby_vallist = gby_vallist->next;
	}
    }

wrapup:
  return;

  gbstate->state = er_errid ();
  goto wrapup;
}

/*
 * qexec_gby_finalize_group_dim () -
 *   return:
 *   gbstate(in):
 *   recdes(in):
 */
static int
qexec_gby_finalize_group_dim (THREAD_ENTRY * thread_p,
			      GROUPBY_STATE * gbstate, const RECDES * recdes)
{
  int i, j, nkeys, level = 0;

  qexec_gby_finalize_group (thread_p, gbstate, 0,
			    (bool) gbstate->with_rollup);
  if (gbstate->state == SORT_PUT_STOP)
    {
      goto wrapup;
    }

  /* handle the rollup groups */
  if (gbstate->with_rollup)
    {
      if (recdes)
	{
	  SORT_REC *key;

	  key = (SORT_REC *) recdes->data;
	  assert (key != NULL);

	  level = gbstate->g_dim_levels;
	  nkeys = gbstate->key_info.nkeys;	/* save */

	  /* find the first key that fails comparison;
	   * the rollup level will be key number
	   */
	  for (i = 1; i < nkeys; i++)
	    {
	      gbstate->key_info.nkeys = i;

	      if ((*gbstate->cmp_fn) (&gbstate->current_key.data, &key,
				      &gbstate->key_info) != 0)
		{
		  /* finalize rollup groups */
		  for (j = gbstate->g_dim_levels - 1; j > i; j--)
		    {
		      assert (gbstate->g_dim[j].
			      d_flag & GROUPBY_DIM_FLAG_ROLLUP);

		      qexec_gby_finalize_group (thread_p, gbstate, j, true);
#if 0				/* TODO - sus-11454 */
		      if (gbstate->state == SORT_PUT_STOP)
			{
			  goto wrapup;
			}
#endif
		      qexec_gby_start_group (thread_p, gbstate, NULL, j);
		    }
		  level = i + 1;
		  break;
		}
	    }

	  gbstate->key_info.nkeys = nkeys;	/* restore */
	}
      else
	{
	  for (j = gbstate->g_dim_levels - 1; j > 0; j--)
	    {
	      assert (gbstate->g_dim[j].d_flag & GROUPBY_DIM_FLAG_ROLLUP);

	      qexec_gby_finalize_group (thread_p, gbstate, j, true);
#if 0				/* TODO - sus-11454 */
	      if (gbstate->state == SORT_PUT_STOP)
		{
		  goto wrapup;
		}
#endif

	      qexec_gby_start_group (thread_p, gbstate, NULL, j);
	    }
	  level = gbstate->g_dim_levels;
	}

      if (gbstate->g_dim != NULL && gbstate->g_dim[0].d_agg_list != NULL)
	{
	  qfile_close_list (thread_p, gbstate->g_dim[0].d_agg_list->list_id);
	  qfile_destroy_list (thread_p,
			      gbstate->g_dim[0].d_agg_list->list_id);
	}
    }

  qexec_gby_start_group (thread_p, gbstate, recdes, 0);

wrapup:
  return level;

  gbstate->state = er_errid ();
  goto wrapup;
}

/*
 * qexec_gby_finalize_group () -
 *   return:
 *   gbstate(in):
 *   N(in):
 *   keep_list_file(in) : whether keep the list file for reuse
 */
static void
qexec_gby_finalize_group (THREAD_ENTRY * thread_p,
			  GROUPBY_STATE * gbstate, int N, bool keep_list_file)
{
  QPROC_TPLDESCR_STATUS tpldescr_status;
  XASL_NODE *xptr;
  DB_LOGICAL ev_res;
  XASL_STATE *xasl_state = gbstate->xasl_state;

  if (gbstate->state != NO_ERROR)
    {
      return;
    }

  if (gbstate->g_dim == NULL || N >= gbstate->g_dim_levels)
    {
      assert (false);
      QEXEC_GOTO_EXIT_ON_ERROR;
    }

  assert (gbstate->g_dim[N].d_flag != GROUPBY_DIM_FLAG_NONE);

  if (qdata_finalize_aggregate_list (thread_p,
				     gbstate->g_dim[N].d_agg_list,
				     &xasl_state->vd,
				     keep_list_file) != NO_ERROR)
    {
      QEXEC_GOTO_EXIT_ON_ERROR;
    }

  /* evaluate subqueries in HAVING predicate */
  for (xptr = gbstate->eptr_list; xptr; xptr = xptr->next)
    {
      if (qexec_execute_mainblock (thread_p, xptr, xasl_state) != NO_ERROR)
	{
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}
    }

  /* move aggregate values in aggregate list for predicate evaluation
     and possibly insertion in list file */
  if (gbstate->g_dim[N].d_agg_list != NULL)
    {
      AGGREGATE_TYPE *g_outp = gbstate->g_output_agg_list;
      AGGREGATE_TYPE *d_aggp = gbstate->g_dim[N].d_agg_list;

      while (g_outp != NULL && d_aggp != NULL)
	{
	  if (g_outp->function != PT_GROUPBY_NUM)
	    {
	      if (d_aggp->accumulator.value != NULL
		  && g_outp->accumulator.value != NULL)
		{
		  pr_clear_value (g_outp->accumulator.value);
		  *g_outp->accumulator.value = *d_aggp->accumulator.value;
		  /* Don't use DB_MAKE_NULL here to preserve the type information. */

		  PRIM_SET_NULL (d_aggp->accumulator.value);
		}

	      /* should not touch d_aggp->value2 */
	    }

	  g_outp = g_outp->next;
	  d_aggp = d_aggp->next;
	}
    }

  /* set to NULL (in the summary tuple) the columns that failed comparison */
  if (!(gbstate->g_dim[N].d_flag & GROUPBY_DIM_FLAG_GROUP_BY))
    {
      assert (N > 0);
      (void) qexec_gby_finalize_group_val_list (thread_p, gbstate, N);
    }

  /* evaluate HAVING predicates */
  ev_res = V_TRUE;
  if (gbstate->having_pred)
    {
      ev_res = eval_pred (thread_p, gbstate->having_pred,
			  &xasl_state->vd, NULL);
      if (ev_res == V_ERROR)
	{
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}
      else if (ev_res != V_TRUE)
	{
	  goto wrapup;
	}
    }

  assert (ev_res == V_TRUE);

  if (gbstate->grbynum_val)
    {
      /* evaluate groupby_num predicates */
      ev_res = qexec_eval_grbynum_pred (thread_p, gbstate);
      if (ev_res == V_ERROR)
	{
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}

      if (ev_res == V_TRUE)
	{
	  if (gbstate->grbynum_flag & XASL_G_GRBYNUM_FLAG_LIMIT_GT_LT)
	    {
	      gbstate->grbynum_flag &= ~XASL_G_GRBYNUM_FLAG_LIMIT_GT_LT;
	      gbstate->grbynum_flag |= XASL_G_GRBYNUM_FLAG_LIMIT_LT;
	    }
	}
      else
	{
	  if (gbstate->grbynum_flag & XASL_G_GRBYNUM_FLAG_LIMIT_LT)
	    {
	      gbstate->grbynum_flag |= XASL_G_GRBYNUM_FLAG_SCAN_STOP;
	    }
	}
      if (gbstate->grbynum_flag & XASL_G_GRBYNUM_FLAG_SCAN_STOP)
	{
	  /* reset grbynum_val for next use */
	  DB_MAKE_BIGINT (gbstate->grbynum_val, 0);
	  /* setting SORT_PUT_STOP will make 'sr_in_sort()' stop processing;
	     the caller, 'qexec_gby_put_next()', returns 'gbstate->state' */
	  gbstate->state = SORT_PUT_STOP;
	}
    }

  if (ev_res != V_TRUE)
    {
      goto wrapup;
    }

  assert (ev_res == V_TRUE);

  tpldescr_status = qexec_generate_tuple_descriptor (thread_p,
						     gbstate->output_file,
						     gbstate->g_outptr_list,
						     &xasl_state->vd);
  if (tpldescr_status == QPROC_TPLDESCR_FAILURE)
    {
      QEXEC_GOTO_EXIT_ON_ERROR;
    }

  switch (tpldescr_status)
    {
    case QPROC_TPLDESCR_SUCCESS:
      /* generate tuple into list file page */
      if (qfile_generate_tuple_into_list (thread_p,
					  gbstate->output_file,
					  T_NORMAL) != NO_ERROR)
	{
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}
      break;

    case QPROC_TPLDESCR_RETRY_SET_TYPE:
    case QPROC_TPLDESCR_RETRY_BIG_REC:
      /* BIG QFILE_TUPLE or a SET-field is included */
      if (gbstate->output_tplrec->tpl == NULL)
	{
	  /* allocate tuple descriptor */
	  gbstate->output_tplrec->size = DB_PAGESIZE;
	  gbstate->output_tplrec->tpl = (QFILE_TUPLE) malloc (DB_PAGESIZE);
	  if (gbstate->output_tplrec->tpl == NULL)
	    {
	      QEXEC_GOTO_EXIT_ON_ERROR;
	    }
	}
      if (qdata_copy_valptr_list_to_tuple (thread_p, gbstate->output_file,
					   gbstate->g_outptr_list,
					   &xasl_state->vd,
					   gbstate->output_tplrec) !=
	  NO_ERROR)
	{
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}

      if (qfile_add_tuple_to_list (thread_p, gbstate->output_file,
				   gbstate->output_tplrec->tpl) != NO_ERROR)
	{
	  QEXEC_GOTO_EXIT_ON_ERROR;
	}
      break;

    default:
      break;
    }

  gbstate->xasl->groupby_stats.rows++;

wrapup:
  /* clear agg_list, since we moved aggreate values here beforehand */
  QEXEC_CLEAR_AGG_LIST_VALUE (gbstate->g_dim[N].d_agg_list);
  return;

exit_on_error:
  gbstate->state = er_errid ();
  goto wrapup;
}

/*
 * qexec_gby_start_group_dim () -
 *   return:
 *   gbstate(in):
 *   recdes(in):
 */
static void
qexec_gby_start_group_dim (THREAD_ENTRY * thread_p,
			   GROUPBY_STATE * gbstate, const RECDES * recdes)
{
  int i;

  /* start all groups */
  for (i = 1; i < gbstate->g_dim_levels; i++)
    {
      qexec_gby_start_group (thread_p, gbstate, NULL, i);
    }
  qexec_gby_start_group (thread_p, gbstate, recdes, 0);

  return;
}

/*
 * qexec_gby_start_group () -
 *   return:
 *   gbstate(in):
 *   recdes(in):
 *   N(in): dimension ID
 */
static void
qexec_gby_start_group (THREAD_ENTRY * thread_p,
		       GROUPBY_STATE * gbstate, const RECDES * recdes, int N)
{
  XASL_STATE *xasl_state = gbstate->xasl_state;
  int error;

  if (gbstate->state != NO_ERROR)
    {
      return;
    }

  if (gbstate->g_dim == NULL || N >= gbstate->g_dim_levels)
    {
      assert (false);
      QEXEC_GOTO_EXIT_ON_ERROR;
    }

  assert (gbstate->g_dim[N].d_flag != GROUPBY_DIM_FLAG_NONE);

  if (N == 0)
    {
      /*
       * Record the new key; keep it in SORT_KEY format so we can continue
       * to use the SORTKEY_INFO version of the comparison functions.
       *
       * WARNING: the sort module doesn't seem to set recdes->area_size
       * reliably, so the only thing we can rely on is recdes->length.
       */

      /* when group by skip, we do not use the RECDES because the list is already
       * sorted
       */
      if (recdes)
	{
	  if (gbstate->current_key.area_size < recdes->length)
	    {
	      void *tmp;

	      tmp = realloc (gbstate->current_key.data, recdes->area_size);
	      if (tmp == NULL)
		{
		  QEXEC_GOTO_EXIT_ON_ERROR;
		}
	      gbstate->current_key.data = (char *) tmp;
	      gbstate->current_key.area_size = recdes->area_size;
	    }
	  memcpy (gbstate->current_key.data, recdes->data, recdes->length);
	  gbstate->current_key.length = recdes->length;
	}
    }

  /* (Re)initialize the various accumulator variables... */
  QEXEC_CLEAR_AGG_LIST_VALUE (gbstate->g_dim[N].d_agg_list);
  error =
    qdata_initialize_aggregate_list (thread_p,
				     gbstate->g_dim[N].d_agg_list,
				     gbstate->xasl_state->query_id);
  if (error != NO_ERROR)
    {
      QEXEC_GOTO_EXIT_ON_ERROR;
    }

wrapup:
  return;

exit_on_error:
  gbstate->state = er_errid ();
  goto wrapup;
}

/*
 * qexec_gby_init_group_dim () - initialize Data Set dimentions
 *   return:
 *   gbstate(in):
 */
static int
qexec_gby_init_group_dim (GROUPBY_STATE * gbstate)
{
  int i;
  AGGREGATE_TYPE *agg, *aggp, *aggr;

  if (gbstate == NULL)		/* sanity check */
    {
      assert (false);
      return ER_FAILED;
    }

#if 1				/* TODO - create Data Set; rollup, cube, grouping set */
  gbstate->g_dim_levels = 1;
  if (gbstate->with_rollup)
    {
      gbstate->g_dim_levels += gbstate->key_info.nkeys;
    }
#endif

  assert (gbstate->g_dim_levels > 0);

  gbstate->g_dim =
    (GROUPBY_DIMENSION *) malloc (gbstate->g_dim_levels *
				  sizeof (GROUPBY_DIMENSION));
  if (gbstate->g_dim == NULL)
    {
      return ER_FAILED;
    }


  /* set aggregation colunms */
  for (i = 0; i < gbstate->g_dim_levels; i++)
    {
      gbstate->g_dim[i].d_flag = GROUPBY_DIM_FLAG_NONE;

      if (i == 0)
	{
	  gbstate->g_dim[i].d_flag |= GROUPBY_DIM_FLAG_GROUP_BY;
	}
#if 1				/* TODO - set dimension flag */
      if (gbstate->with_rollup)
	{
	  gbstate->g_dim[i].d_flag |= GROUPBY_DIM_FLAG_ROLLUP;
	}
#endif
      gbstate->g_dim[i].d_flag |= GROUPBY_DIM_FLAG_CUBE;

      if (gbstate->g_output_agg_list)
	{
	  agg = gbstate->g_output_agg_list;
	  gbstate->g_dim[i].d_agg_list = aggp =
	    (AGGREGATE_TYPE *) malloc (sizeof (AGGREGATE_TYPE));
	  if (aggp == NULL)
	    {
	      return ER_FAILED;
	    }
	  memcpy (gbstate->g_dim[i].d_agg_list, agg, sizeof (AGGREGATE_TYPE));
	  gbstate->g_dim[i].d_agg_list->accumulator.value =
	    db_value_copy (agg->accumulator.value);
	  gbstate->g_dim[i].d_agg_list->accumulator.value2 =
	    db_value_copy (agg->accumulator.value2);

	  while ((agg = agg->next))
	    {
	      aggr = (AGGREGATE_TYPE *) malloc (sizeof (AGGREGATE_TYPE));
	      if (aggr == NULL)
		{
		  return ER_FAILED;
		}
	      memcpy (aggr, agg, sizeof (AGGREGATE_TYPE));
	      aggr->accumulator.value =
		db_value_copy (agg->accumulator.value);
	      aggr->accumulator.value2 =
		db_value_copy (agg->accumulator.value2);
	      aggp->next = aggr;
	      aggp = aggr;
	    }
	}
      else
	{
	  gbstate->g_dim[i].d_agg_list = NULL;
	}
    }

  return NO_ERROR;
}

/*
 * qexec_gby_clear_group_dim() - destroy aggregates lists
 *   return:
 *   gbstate(in):
 */
static void
qexec_gby_clear_group_dim (THREAD_ENTRY * thread_p, GROUPBY_STATE * gbstate)
{
  int i;
  AGGREGATE_TYPE *agg, *next_agg;

  assert (gbstate != NULL);
  assert (gbstate->g_dim != NULL);

  if (gbstate && gbstate->g_dim)
    {
      for (i = 0; i < gbstate->g_dim_levels; i++)
	{
	  agg = gbstate->g_dim[i].d_agg_list;
	  while (agg)
	    {
	      next_agg = agg->next;

	      db_value_free (agg->accumulator.value);
	      db_value_free (agg->accumulator.value2);
	      if (agg->list_id)
		{
		  /* close and destroy temporary list files */
		  qfile_close_list (thread_p, agg->list_id);
		  qfile_destroy_list (thread_p, agg->list_id);
		}

	      free_and_init (agg);

	      agg = next_agg;
	    }
	}
      free_and_init (gbstate->g_dim);
    }
}

/*
 * qexec_groupby_index() - computes group by on the fly from the index list
 *   return: NO_ERROR, or ER_code
 *   xasl(in)   :
 *   xasl_state(in)     : XASL tree state information
 *   tplrec(out) : Tuple record descriptor to store result tuples
 *
 * Note: Apply the group_by clause to the given list file to group it
 * using the specified group_by parameters.
 */
int
qexec_groupby_index (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
		     XASL_STATE * xasl_state, QFILE_TUPLE_RECORD * tplrec)
{
  QFILE_LIST_ID *list_id = xasl->list_id;
  BUILDLIST_PROC_NODE *buildlist = &xasl->proc.buildlist;
  GROUPBY_STATE gbstate;
  QFILE_LIST_SCAN_ID input_scan_id;
  int result = 0, ls_flag = 0;
  DB_VALUE *list_dbvals = NULL;
  int i, ncolumns = 0;
  QFILE_TUPLE_VALUE_TYPE_LIST *tpl_type_list = NULL;
  TP_DOMAIN *i_domain = NULL;
  DB_VALUE i_val;
  SORT_LIST *sort_col = NULL;
  bool all_cols_equal = false;
  SCAN_CODE scan_code;
  QFILE_TUPLE_RECORD tuple_rec;
  REGU_VARIABLE_LIST regu_list;
  int tuple_cnt = 0;
  struct timeval start, end;

  if (buildlist->groupby_list == NULL)
    {
      return NO_ERROR;
    }

  if (thread_is_on_trace (thread_p))
    {
      gettimeofday (&start, NULL);
      xasl->groupby_stats.run_groupby = true;
      xasl->groupby_stats.groupby_sort = false;
      xasl->groupby_stats.rows = 0;
    }

  assert (buildlist->g_with_rollup == 0);

  if (qexec_initialize_groupby_state (&gbstate, buildlist->groupby_list,
				      buildlist->g_having_pred,
				      buildlist->g_grbynum_pred,
				      buildlist->g_grbynum_val,
				      buildlist->g_grbynum_flag,
				      buildlist->eptr_list,
				      buildlist->g_agg_list,
				      buildlist->g_regu_list,
				      buildlist->g_val_list,
				      buildlist->g_outptr_list,
				      buildlist->g_with_rollup,
				      xasl,
				      xasl_state,
				      &list_id->type_list, tplrec) == NULL)
    {
      QEXEC_GOTO_EXIT_ON_ERROR;
    }

  assert (gbstate.g_dim_levels == 1);
  assert (gbstate.with_rollup == 0);

  /*
   * Create a new listfile to receive the results.
   */
  {
    QFILE_TUPLE_VALUE_TYPE_LIST output_type_list;
    QFILE_LIST_ID *output_list_id;

    if (qdata_get_valptr_type_list (thread_p, buildlist->g_outptr_list,
				    &output_type_list) != NO_ERROR)
      {
	QEXEC_GOTO_EXIT_ON_ERROR;
      }
    /* If it does not have 'order by'(xasl->orderby_list),
       then the list file to be open at here will be the last one.
       Otherwise, the last list file will be open at
       qexec_orderby_distinct().
       (Note that only one that can have 'group by' is BUILDLIST_PROC type.)
       And, the top most XASL is the other condition for the list file
       to be the last result file. */

    QFILE_SET_FLAG (ls_flag, QFILE_FLAG_ALL);

    output_list_id = qfile_open_list (thread_p, &output_type_list,
				      buildlist->after_groupby_list,
				      xasl_state->query_id, ls_flag);

    if (output_type_list.domp)
      {
	free_and_init (output_type_list.domp);
      }

    if (output_list_id == NULL)
      {
	QEXEC_GOTO_EXIT_ON_ERROR;
      }

    gbstate.output_file = output_list_id;
  }

  if (list_id->tuple_cnt == 0)
    {
      /* empty unsorted list file, no need to proceed */
      qfile_destroy_list (thread_p, list_id);
      qfile_close_list (thread_p, gbstate.output_file);
      qfile_copy_list_id (list_id, gbstate.output_file, true);
      qexec_clear_groupby_state (thread_p, &gbstate);	/* will free gbstate.output_file */

      return NO_ERROR;
    }
  else
    {
      tuple_cnt = list_id->tuple_cnt;
    }

  /*
   * Open a scan on the unsorted input file
   */
  if (qfile_open_list_scan (list_id, &input_scan_id) != NO_ERROR)
    {
      QEXEC_GOTO_EXIT_ON_ERROR;
    }
  gbstate.input_scan = &input_scan_id;

  /* ... go through all records and identify groups */

  /* count the number of columns in group by */
  for (sort_col = xasl->proc.buildlist.groupby_list; sort_col != NULL;
       sort_col = sort_col->next)
    {
      ncolumns++;
    }

  /* alloc an array to store db_values */
  list_dbvals = (DB_VALUE *) malloc (ncolumns * sizeof (DB_VALUE));
  if (list_dbvals == NULL)
    {
      QEXEC_GOTO_EXIT_ON_ERROR;
    }

  for (i = 0; i < ncolumns; i++)
    {
      DB_MAKE_NULL (&list_dbvals[i]);
    }

  while (1)
    {
      if (gbstate.state == SORT_PUT_STOP)
	{
	  break;
	}
      scan_code =
	qfile_scan_list_next (thread_p, gbstate.input_scan, &tuple_rec, PEEK);
      if (scan_code != S_SUCCESS)
	{
	  break;
	}

      /* fetch the values from the tuple according to outptr format
       * check the result in xasl->outptr_list->valptrp
       */
      all_cols_equal = true;

      tpl_type_list = &(gbstate.input_scan->list_id.type_list);
      assert (tpl_type_list != NULL);

      regu_list = xasl->outptr_list->valptrp;
      for (i = 0; i < ncolumns; i++)
	{
	  if (regu_list == NULL)
	    {
	      gbstate.state = ER_FAILED;
	      goto exit_on_error;
	    }

	  assert (i < tpl_type_list->type_cnt);
	  assert (tpl_type_list->domp[i] != NULL);
	  i_domain = tpl_type_list->domp[i];

	  if (qexec_get_tuple_column_value
	      (tuple_rec.tpl, i, i_domain, &i_val) != NO_ERROR)
	    {
	      gbstate.state = ER_FAILED;
	      goto exit_on_error;
	    }

	  if (gbstate.input_recs > 0)
	    {
	      /* compare old value with current
	       * total_order is 1. Then NULLs are equal.
	       */
	      int c = tp_value_compare (&list_dbvals[i], &i_val, 1, 1, NULL);

	      if (c != DB_EQ)
		{
		  /* must be DB_LT or DB_GT */
		  if (c != DB_LT && c != DB_GT)
		    {
		      assert_release (false);
		      gbstate.state = ER_FAILED;

		      QEXEC_GOTO_EXIT_ON_ERROR;
		    }

		  /* new group should begin, check code below */
		  all_cols_equal = false;
		}

	      db_value_clear (&list_dbvals[i]);
	    }

	  db_value_clone (&i_val, &list_dbvals[i]);

	  regu_list = regu_list->next;
	}

      if (gbstate.input_recs == 0)
	{
	  /* First record we've seen; put it out and set up the group
	   * comparison key(s).
	   */
	  qexec_gby_start_group_dim (thread_p, &gbstate, NULL);
	}
      else if (all_cols_equal)
	{
	  /* Still in the same group; accumulate the tuple and proceed,
	   * leaving the group key the same.
	   */
	}
      else
	{
	  assert (gbstate.g_dim_levels == 1);
	  assert (gbstate.with_rollup == 0);

	  /* We got a new group; finalize the group we were accumulating,
	   * and start a new group using the current key as the group key.
	   */
	  qexec_gby_finalize_group_dim (thread_p, &gbstate, NULL);
	}

      /* at here, have to COPY into gbstate
       */
      qexec_gby_agg_tuple (thread_p, &gbstate, tuple_rec.tpl, COPY);

      gbstate.input_recs++;
    }

  /* ... finish grouping */

  /* There may be one unfinished group in the output. If so, finish it */
  if (gbstate.input_recs != 0)
    {
      qexec_gby_finalize_group_dim (thread_p, &gbstate, NULL);
    }

  qfile_close_list (thread_p, gbstate.output_file);

  if (gbstate.input_scan)
    {
      qfile_close_scan (thread_p, gbstate.input_scan);
      gbstate.input_scan = NULL;
    }
  qfile_destroy_list (thread_p, list_id);
  qfile_copy_list_id (list_id, gbstate.output_file, true);

  if (thread_is_on_trace (thread_p))
    {
      gettimeofday (&end, NULL);
      ADD_TIMEVAL (xasl->groupby_stats.groupby_time, start, end);
    }

exit_on_error:

  if (list_dbvals)
    {
      for (i = 0; i < ncolumns; i++)
	{
	  db_value_clear (&list_dbvals[i]);
	}
      free_and_init (list_dbvals);
    }

  /* SORT_PUT_STOP set by 'qexec_gby_finalize_group_dim ()' isn't error */
  result = (gbstate.state == NO_ERROR || gbstate.state == SORT_PUT_STOP)
    ? NO_ERROR : ER_FAILED;

  qexec_clear_groupby_state (thread_p, &gbstate);

  return result;
}

/*
 * query_multi_range_opt_check_set_sort_col () - scans the SPEC nodes in the
 *						 XASL and resolves the sorting
 *						 info required for multiple
 *						 range search optimization
 *
 * return	 : error code
 * thread_p (in) : thread entry
 * xasl (in)     : xasl node
 */
static int
query_multi_range_opt_check_set_sort_col (THREAD_ENTRY * thread_p,
					  XASL_NODE * xasl)
{
  DB_VALUE **sort_col_out_val_ref = NULL;
  int i = 0, count = 0, index = 0, att_id, sort_index_pos;
  REGU_VARIABLE_LIST regu_list = NULL;
  SORT_LIST *orderby_list = NULL;
  int error = NO_ERROR;
  MULTI_RANGE_OPT *multi_range_opt = NULL;
  ACCESS_SPEC_TYPE *spec = NULL;
  INDX_SCAN_ID *isidp = NULL;
  BTID_INT *btid = NULL;

  if (xasl == NULL || xasl->type != BUILDLIST_PROC
      || xasl->orderby_list == NULL || xasl->spec_list == NULL)
    {
      return NO_ERROR;
    }

  /* find access spec using multi range optimization */
  spec = query_multi_range_opt_check_specs (thread_p, xasl);
  if (spec == NULL)
    {
      /* no scan with multi range search optimization was found */
      return NO_ERROR;
    }
  multi_range_opt = &spec->s_id.s.isid.multi_range_opt;
  /* initialize sort info for multi range search optimization */
  orderby_list = xasl->orderby_list;
  while (orderby_list)
    {
      count++;
      orderby_list = orderby_list->next;
    }

  /* find the addresses contained in REGU VAR for values used in sorting */
  sort_col_out_val_ref = (DB_VALUE **) malloc (count * sizeof (DB_VALUE *));
  if (sort_col_out_val_ref == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, count * sizeof (DB_VALUE *));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }
  for (orderby_list = xasl->orderby_list; orderby_list != NULL;
       orderby_list = orderby_list->next)
    {
      i = 0;
      /* get sort column from 'outptr_list' */
      for (regu_list = xasl->outptr_list->valptrp; regu_list != NULL;
	   regu_list = regu_list->next)
	{
	  if (REGU_VARIABLE_IS_FLAGED
	      (&regu_list->value, REGU_VARIABLE_HIDDEN_COLUMN))
	    {
	      continue;
	    }
	  if (i == orderby_list->pos_descr.pos_no)
	    {
	      if (regu_list->value.type == TYPE_CONSTANT)
		{
		  sort_col_out_val_ref[index++] =
		    regu_list->value.value.dbvalptr;
		}
	      break;
	    }
	  i++;
	}
    }
  if (index != count)
    {
      /* this is not supposed to happen */
      assert (0);
      goto exit_on_error;
    }

  if (multi_range_opt->no_attrs == 0)
    {
      multi_range_opt->no_attrs = count;
      multi_range_opt->is_desc_order =
	(bool *) malloc (count * sizeof (bool));
      if (multi_range_opt->is_desc_order == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, count * sizeof (bool));
	  goto exit_on_error;
	}
      multi_range_opt->sort_att_idx = (int *) malloc (count * sizeof (int));
      if (multi_range_opt->sort_att_idx == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, count * sizeof (int));
	  goto exit_on_error;
	}
    }
  else
    {
      /* is multi_range_opt already initialized? */
      assert (0);
    }

  isidp = &(spec->s_id.s.isid);
  btid = &(isidp->bt_scan.btid_int);

  for (index = 0, orderby_list = xasl->orderby_list; index < count;
       index++, orderby_list = orderby_list->next)
    {
      const DB_VALUE *valp = sort_col_out_val_ref[index];

      att_id = -1;
      sort_index_pos = -1;
      /* search the ATTR_ID regu 'fetching to' the output list regu used for
       * sorting
       */
      for (regu_list = isidp->rest_regu_list;
	   regu_list != NULL; regu_list = regu_list->next)
	{
	  if (regu_list->value.type == TYPE_ATTR_ID
	      && regu_list->value.vfetch_to == valp)
	    {
	      att_id = regu_list->value.value.attr_descr.id;
	      break;
	    }
	}
      /* search the attribute in the index attributes */
      if (att_id != -1)
	{
	  assert (btid->classrepr != NULL);
	  assert (btid->classrepr_cache_idx != -1);
	  assert (btid->indx_id != -1);

	  for (i = 0; i < btid->classrepr->indexes[btid->indx_id].n_atts; i++)
	    {
	      if (att_id ==
		  btid->classrepr->indexes[btid->indx_id].atts[i]->id)
		{
		  sort_index_pos = i;
		  break;
		}
	    }
	}
      if (sort_index_pos == -1)
	{
	  /* REGUs didn't match, at least disable the optimization */
	  multi_range_opt->use = false;
	  goto exit_on_error;
	}
      multi_range_opt->is_desc_order[index] =
	(orderby_list->s_order == S_DESC) ? true : false;
      multi_range_opt->sort_att_idx[index] = sort_index_pos;
    }

  /* disable order by in XASL for this execution */
  if (xasl->option != Q_DISTINCT && xasl->scan_ptr == NULL)
    {
      XASL_SET_FLAG (xasl, XASL_SKIP_ORDERBY_LIST);
      XASL_SET_FLAG (xasl, XASL_USES_MRO);
    }

exit:
  if (sort_col_out_val_ref)
    {
      free_and_init (sort_col_out_val_ref);
    }
  return error;

exit_on_error:
  error = ER_FAILED;
  goto exit;
}

/*
 * query_multi_range_opt_check_specs () - searches the XASL tree for the
 *					  enabled multiple range search
 *					  optimized index scan
 *
 * return		     : ACCESS_SPEC_TYPE if an index scan with multiple
 *			       range search enabled is found, NULL otherwise
 * thread_p (in)	     : thread entry
 * spec_list (in/out)	     : access spec list
 */
static ACCESS_SPEC_TYPE *
query_multi_range_opt_check_specs (UNUSED_ARG
				   THREAD_ENTRY * thread_p, XASL_NODE * xasl)
{
  ACCESS_SPEC_TYPE *spec_list;
  for (; xasl != NULL; xasl = xasl->scan_ptr)
    {
      for (spec_list = xasl->spec_list; spec_list != NULL;
	   spec_list = spec_list->next)
	{
	  if (spec_list->access != INDEX || spec_list->type != TARGET_CLASS
	      || spec_list->s_id.type != S_INDX_SCAN)
	    {
	      continue;
	    }
	  if (spec_list->s_id.s.isid.multi_range_opt.use)
	    {
	      return spec_list;
	    }
	}
    }
  return NULL;
}

/*
 * qexec_lock_table_select_for_update () - set S_LOCK on tables which will be
 *					   updated (SELECT ... FOR UPDATE)
 * return : error code or NO_ERROR
 * thread_p (in)  :
 * scan_list (in) :
 * num_shard_table (out) :
 * for_update (out) :
 *
 * Note: Used in SELECT ... FOR UPDATE
 */
static int
qexec_lock_table_select_for_update (THREAD_ENTRY * thread_p,
				    XASL_NODE * scan_list,
				    int *num_shard_table, bool * for_update)
{
  XASL_NODE *scan = NULL;
  ACCESS_SPEC_TYPE *specp = NULL;
  OID *class_oid = NULL;
  int error = NO_ERROR;
  DB_VALUE lock_val;

  for (scan = scan_list; scan != NULL; scan = scan->scan_ptr)
    {
      assert (scan->spec_list != NULL);
      assert (scan->spec_list->next == NULL);
      for (specp = scan->spec_list; specp; specp = specp->next)
	{
	  if (specp->type == TARGET_CLASS)
	    {
	      class_oid = &specp->s.cls_node.cls_oid;

	      /* lock the class */
	      DB_MAKE_OID (&lock_val, class_oid);

	      if (lock_object (thread_p, &lock_val, S_LOCK,
			       LK_UNCOND_LOCK) != LK_GRANTED)
		{
		  error = er_errid ();
		  if (error == NO_ERROR)
		    {
		      assert (false);
		      error = ER_FAILED;
		    }
		  return error;
		}

	      if (heap_classrepr_is_shard_table (thread_p,
						 DB_GET_OID (&lock_val)))
		{
		  (*num_shard_table)++;
		}

	      /* check spec validity */
	      error = er_errid ();
	      if (error != NO_ERROR)
		{
		  return error;
		}

	      if (specp->flags & ACCESS_SPEC_FLAG_FOR_UPDATE)
		{
		  *for_update = true;
		}
	    }
	}
    }

  assert (error == NO_ERROR);

  return error;
}

/*
 * qexec_lock_table_update_delete () - set S_LOCK on a table which will be
 *                                     updated or deleted
 *
 * return : error code or NO_ERROR
 * thread_p (in)  :
 * aptr_list (in)	  :
 * upd_cls (in) : query class
 * is_shard_table (in) :
 * is_catalog_table (in) :
 */
static int
qexec_lock_table_update_delete (THREAD_ENTRY * thread_p,
				XASL_NODE * aptr_list,
				UPDDEL_CLASS_INFO * upd_cls,
				bool is_shard_table,
				UNUSED_ARG bool is_catalog_table)
{
  XASL_NODE *aptr = NULL;
  ACCESS_SPEC_TYPE *specp = NULL;
  OID *class_oid = NULL;
  int error = NO_ERROR;
  DB_VALUE lock_val;
  int num_shard_table = 0;

  for (aptr = aptr_list; aptr != NULL; aptr = aptr->scan_ptr)
    {
      for (specp = aptr->spec_list; specp; specp = specp->next)
	{
	  if (specp->type == TARGET_CLASS)
	    {
	      class_oid = &specp->s.cls_node.cls_oid;

	      /* lock the class */
	      DB_MAKE_OID (&lock_val, class_oid);

	      if (lock_object (thread_p, &lock_val, S_LOCK,
			       LK_UNCOND_LOCK) != LK_GRANTED)
		{
		  error = er_errid ();
		  if (error == NO_ERROR)
		    {
		      assert (false);
		      error = ER_FAILED;
		    }
		  return error;
		}

	      if (OID_EQ (&(upd_cls->class_oid), class_oid))
		{
		  if (heap_classrepr_is_shard_table (thread_p,
						     DB_GET_OID (&lock_val)))
		    {
		      num_shard_table++;
		    }

		  /* check spec validity */
		  error = er_errid ();
		  if (error != NO_ERROR)
		    {
		      return error;
		    }
		}
	    }
	}
    }

#if 0
done:
#endif

#if 1				/* TODO - */
  if (is_shard_table)
    {
      assert (num_shard_table > 0);
    }
  else
    {
      assert (num_shard_table == 0);
    }
#endif

  assert (error == NO_ERROR);

  return error;
}

/*
 * qexec_init_internal_class () - init internal update / delete execution
 * return :
 * thread_p (in)    :
 * class (in):
 */
static int
qexec_init_internal_class (UNUSED_ARG THREAD_ENTRY * thread_p,
			   UPDDEL_CLASS_INFO_INTERNAL * class)
{
  assert (class != NULL);

  /* initialize internal structures */

  class->class_hfid = NULL;
  class->class_oid = NULL;
  class->scan_cache_inited = false;
  class->is_attr_info_inited = false;

  return NO_ERROR;
}

/*
 * qexec_clear_internal_class () - clear
 * return : void
 * thread_p (in) :
 * class (in)	 :
 */
static void
qexec_clear_internal_class (THREAD_ENTRY * thread_p,
			    UPDDEL_CLASS_INFO_INTERNAL * class)
{
  if (class->scan_cache_inited)
    {
      locator_end_force_scan_cache (thread_p, &class->scan_cache);
      class->scan_cache_inited = false;
    }

  if (class->is_attr_info_inited)
    {
      heap_attrinfo_end (thread_p, &class->attr_info);
      class->is_attr_info_inited = false;
    }
}

/*
 * qexec_upddel_setup_current_class () - setup current class info in a class
 *					 hierarchy
 * return : error code or NO_ERROR
 * thread_p (in) :
 * query_class (in) : query class information
 * internal_class (in) : internal class
 *
 * Note: this function is used for update and delete to find class hfid when
 *  the operation is performed on a class hierarchy
 */
static int
qexec_upddel_setup_current_class (THREAD_ENTRY * thread_p,
				  UPDDEL_CLASS_INFO * query_class,
				  UPDDEL_CLASS_INFO_INTERNAL * internal_class)
{
  int error = NO_ERROR;

  internal_class->class_oid = &(query_class->class_oid);
  internal_class->class_hfid = &(query_class->class_hfid);

  assert (internal_class->class_oid != NULL);
  assert (internal_class->class_hfid != NULL);

  /* Start a HEAP_SCANCACHE object on the new class. */
  if (internal_class->scan_cache_inited)
    {
      (void) locator_end_force_scan_cache (thread_p,
					   &internal_class->scan_cache);
      internal_class->scan_cache_inited = false;
    }

  error = locator_start_force_scan_cache (thread_p,
					  &internal_class->scan_cache,
					  internal_class->class_hfid,
					  0, internal_class->class_oid);
  if (error != NO_ERROR)
    {
      return error;
    }

  internal_class->scan_cache_inited = true;

  return NO_ERROR;
}

/*
 * qexec_evaluate_aggregates_optimize () - optimize aggregate evaluation
 * return : error code or NO_ERROR
 * thread_p (in) : thread entry
 * agg_list (in) : aggregate list to be evaluated
 * spec (in)	 : access spec
 * is_scan_needed (in/out) : true if scan is still needed after evaluation
 */
static int
qexec_evaluate_aggregates_optimize (THREAD_ENTRY * thread_p,
				    AGGREGATE_TYPE * agg_list,
				    ACCESS_SPEC_TYPE * spec,
				    bool * is_scan_needed)
{
  AGGREGATE_TYPE *agg_ptr;
  int error = NO_ERROR;

  for (agg_ptr = agg_list; agg_ptr; agg_ptr = agg_ptr->next)
    {
      if (!agg_ptr->flag_agg_optimize)
	{
	  /* scan is needed for this aggregate */
	  *is_scan_needed = true;
	  break;
	}
    }

  for (agg_ptr = agg_list; agg_ptr; agg_ptr = agg_ptr->next)
    {
      if (agg_ptr->flag_agg_optimize)
	{
	  /* TODO - do not optimize count aggr */
	  if (agg_ptr->function == PT_COUNT_STAR
	      || agg_ptr->function == PT_COUNT)
	    {
	      agg_ptr->flag_agg_optimize = false;
	      *is_scan_needed = true;
	      continue;
	    }
	  if (qdata_evaluate_aggregate_optimize
	      (thread_p, agg_ptr, &ACCESS_SPEC_CLS_OID (spec),
	       &ACCESS_SPEC_HFID (spec)) != NO_ERROR)
	    {
	      agg_ptr->flag_agg_optimize = false;
	      *is_scan_needed = true;
	    }
	}
    }

  return error;
}

/*
 * qexec_setup_topn_proc () - setup a top-n object
 * return : error code or NO_ERROR
 * thread_p (in) :
 * xasl (in) :
 * vd (in) :
 */
static int
qexec_setup_topn_proc (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
		       VAL_DESCR * vd)
{
  BINARY_HEAP *heap = NULL;
  DB_VALUE ubound_val;
  REGU_VARIABLE_LIST var_list = NULL;
  TOPN_TUPLES *top_n = NULL;
  int error = NO_ERROR, ubound = 0, count = 0;
  UINT64 max_size = 0;

  if (xasl->type != BUILDLIST_PROC)
    {
      return NO_ERROR;
    }

  if (xasl->orderby_list == NULL)
    {
      /* Not ordered */
      return NO_ERROR;
    }
  if (xasl->ordbynum_pred == NULL)
    {
      /* No limit specified */
      return NO_ERROR;
    }

  if (xasl->option == Q_DISTINCT)
    {
      /* We cannot handle distinct ordering */
      return NO_ERROR;
    }

  if (XASL_IS_FLAGED (xasl, XASL_SKIP_ORDERBY_LIST)
      || XASL_IS_FLAGED (xasl, XASL_USES_MRO))
    {
      return NO_ERROR;
    }

  if (xasl->proc.buildlist.groupby_list != NULL)
    {
      /* Cannot handle group by with order by */
      return NO_ERROR;
    }

  DB_MAKE_NULL (&ubound_val);
  error = qexec_get_orderbynum_upper_bound (thread_p, xasl->ordbynum_pred, vd,
					    &ubound_val);
  if (error != NO_ERROR)
    {
      return error;
    }
  if (DB_IS_NULL (&ubound_val))
    {
      return NO_ERROR;
    }
  if (DB_VALUE_TYPE (&ubound_val) != DB_TYPE_INTEGER)
    {
      TP_DOMAIN_STATUS status;

      status = tp_value_coerce (&ubound_val, &ubound_val, &tp_Integer_domain);
      if (status != DOMAIN_COMPATIBLE)
	{
	  return NO_ERROR;
	}
    }

  ubound = DB_GET_INT (&ubound_val);
  pr_clear_value (&ubound_val);

  if (ubound <= 0)
    {
      return NO_ERROR;
    }

  count = 0;
  var_list = xasl->outptr_list->valptrp;
  for (var_list = xasl->outptr_list->valptrp; var_list != NULL;
       var_list = var_list->next)
    {
      if (REGU_VARIABLE_IS_FLAGED (&var_list->value,
				   REGU_VARIABLE_HIDDEN_COLUMN))
	{
	  /* skip hidden values */
	  continue;
	}

      count++;
    }

  max_size = prm_get_bigint_value (PRM_ID_SORT_BUFFER_SIZE);

  top_n = (TOPN_TUPLES *) malloc (sizeof (TOPN_TUPLES));
  if (top_n == NULL)
    {
      error = ER_FAILED;
      goto error_return;
    }

  top_n->max_size = max_size;
  top_n->total_size = 0;

  top_n->tuples = (TOPN_TUPLE *) malloc (ubound * sizeof (TOPN_TUPLE));
  if (top_n->tuples == NULL)
    {
      error = ER_FAILED;
      goto error_return;
    }
  memset (top_n->tuples, 0, ubound * sizeof (TOPN_TUPLE));

  heap = bh_create (thread_p, ubound, qexec_topn_compare, top_n);
  if (heap == NULL)
    {
      error = ER_FAILED;
      goto error_return;
    }

  top_n->heap = heap;
  top_n->sort_items = xasl->orderby_list;
  top_n->values_count = count;

  xasl->topn_items = top_n;

  return NO_ERROR;

error_return:
  if (heap != NULL)
    {
      bh_destroy (thread_p, heap);
    }
  if (top_n != NULL)
    {
      if (top_n->tuples != NULL)
	{
	  free_and_init (top_n->tuples);
	}
      free_and_init (top_n);
    }

  return error;
}

/*
 * qexec_topn_compare () - comparison function for top-n heap
 * return : comparison result
 * left (in) :
 * right (in) :
 * arg (in) :
 */
static BH_CMP_RESULT
qexec_topn_compare (const BH_ELEM left, const BH_ELEM right, BH_CMP_ARG arg)
{
  int pos;
  SORT_LIST *key = NULL;
  TOPN_TUPLES *proc = (TOPN_TUPLES *) arg;
  TOPN_TUPLE *left_tuple = (TOPN_TUPLE *) left;
  TOPN_TUPLE *right_tuple = (TOPN_TUPLE *) right;
  BH_CMP_RESULT cmp;

  for (key = proc->sort_items; key != NULL; key = key->next)
    {
      pos = key->pos_descr.pos_no;
      cmp = qexec_topn_cmpval (&left_tuple->values[pos],
			       &right_tuple->values[pos], key);
      if (cmp == BH_EQ)
	{
	  continue;
	}
      return cmp;
    }

  return BH_EQ;
}

/*
 * qexec_topn_cmpval () - compare two values
 * return : comparison result
 * left (in)  : left value
 * right (in) : right value
 * sort_spec (in): sort spec for left and right
 *
 * Note: tp_value_compare is too complex for our case
 */
static BH_CMP_RESULT
qexec_topn_cmpval (DB_VALUE * left, DB_VALUE * right, SORT_LIST * sort_spec)
{
  int cmp;
  if (DB_IS_NULL (left))
    {
      if (DB_IS_NULL (right))
	{
	  return BH_EQ;
	}
      cmp = DB_LT;
      if ((sort_spec->s_order == S_ASC && sort_spec->s_nulls == S_NULLS_LAST)
	  || (sort_spec->s_order == S_DESC
	      && sort_spec->s_nulls == S_NULLS_FIRST))
	{
	  cmp = -cmp;
	}
    }
  else if (DB_IS_NULL (right))
    {
      cmp = DB_GT;
      if ((sort_spec->s_order == S_ASC && sort_spec->s_nulls == S_NULLS_LAST)
	  || (sort_spec->s_order == S_DESC
	      && sort_spec->s_nulls == S_NULLS_FIRST))
	{
	  cmp = -cmp;
	}
    }
  else
    {
      cmp = tp_value_compare (left, right, 1, 1, NULL);
      assert (cmp != DB_UNK);
    }
  if (sort_spec->s_order == S_DESC)
    {
      cmp = -cmp;
    }

  switch (cmp)
    {
    case DB_GT:
      return BH_GT;

    case DB_LT:
      return BH_LT;

    case DB_EQ:
      return BH_EQ;

    default:
      break;
    }

  return BH_CMP_ERROR;
}

/*
 * qexec_add_tuple_to_topn () - add a new tuple to top-n tuples
 * return : TOPN_SUCCESS if tuple was successfully processed, TOPN_OVERFLOW if
 *	    the new tuple does not fit into memory or TOPN_FAILURE on error
 * thread_p (in)  :
 * topn_items (in): topn items
 * tpldescr (in)  : new tuple
 *
 * Note: We only add a tuple here if the top-n heap has fewer than n elements
 *  or if the new tuple can replace one of the existing tuples
 */
static TOPN_STATUS
qexec_add_tuple_to_topn (THREAD_ENTRY * thread_p, TOPN_TUPLES * topn_items,
			 QFILE_TUPLE_DESCRIPTOR * tpldescr)
{
  int error = NO_ERROR;
  BH_CMP_RESULT res = BH_EQ;
  SORT_LIST *key = NULL;
  int pos = 0;
  TOPN_TUPLE *heap_max = NULL;

  assert (topn_items != NULL && tpldescr != NULL);

  if (!bh_is_full (topn_items->heap))
    {
      /* Add current tuple to heap. We haven't reached top-N yet */
      TOPN_TUPLE *tpl = NULL;
      int idx = topn_items->heap->element_count;

      if (topn_items->total_size + tpldescr->tpl_size > topn_items->max_size)
	{
	  /* abandon top-N */
	  return TOPN_OVERFLOW;
	}

      tpl = &topn_items->tuples[idx];

      /* tpl must be unused */
      assert_release (tpl->values == NULL);

      error = qdata_tuple_to_values_array (thread_p, tpldescr, &tpl->values);
      if (error != NO_ERROR)
	{
	  return TOPN_FAILURE;
	}

      tpl->values_size = tpldescr->tpl_size;
      topn_items->total_size += tpldescr->tpl_size;

      (void) bh_insert (topn_items->heap, tpl);

      return TOPN_SUCCESS;
    }

  /* We only add a tuple to the heap if it is "smaller" than the current
   * root. Rather than allocating memory for a new tuple and testing it,
   * we test the heap root directly on the outptr list and replace it
   * if we have to.
   */
  heap_max = bh_peek_max (topn_items->heap);

  assert (heap_max != NULL);

  for (key = topn_items->sort_items; key != NULL; key = key->next)
    {
      pos = key->pos_descr.pos_no;
      res = qexec_topn_cmpval (&heap_max->values[pos], tpldescr->f_valp[pos],
			       key);
      if (res == BH_EQ)
	{
	  continue;
	}
      if (res == BH_LT)
	{
	  /* skip this tuple */
	  return NO_ERROR;
	}
      break;
    }
  if (res == BH_EQ)
    {
      return TOPN_SUCCESS;
    }

  /* Test if we can accommodate the new tuple */
  if (topn_items->total_size - heap_max->values_size + tpldescr->tpl_size >
      topn_items->max_size)
    {
      /* Abandon top-N */
      return TOPN_OVERFLOW;
    }

  /* Replace heap root. We don't need the heap_max object anymore so we will
   * use it for the new tuple.
   */
  topn_items->total_size -= heap_max->values_size;
  qexec_clear_topn_tuple (thread_p, heap_max, tpldescr->f_cnt);

  error = qdata_tuple_to_values_array (thread_p, tpldescr, &heap_max->values);
  if (error != NO_ERROR)
    {
      return TOPN_FAILURE;
    }

  heap_max->values_size = tpldescr->tpl_size;
  topn_items->total_size += tpldescr->tpl_size;

  (void) bh_down_heap (topn_items->heap, 0);

  return TOPN_SUCCESS;
}

/*
 * qexec_topn_tuples_to_list_id () - put tuples from the internal heap to the
 *				   output listfile
 * return : error code or NO_ERROR
 * xasl (in) : xasl node
 */
static int
qexec_topn_tuples_to_list_id (THREAD_ENTRY * thread_p, XASL_NODE * xasl,
			      XASL_STATE * xasl_state, bool is_final)
{
  QFILE_LIST_ID *list_id = NULL;
  QFILE_TUPLE_DESCRIPTOR *tpl_descr = NULL;
  TOPN_TUPLES *topn = NULL;
  BINARY_HEAP *heap = NULL;
  REGU_VARIABLE_LIST varp = NULL;
  TOPN_TUPLE *tuple = NULL;
  int row = 0, i, value_size, values_count, error = NO_ERROR;
  ORDBYNUM_INFO ordby_info;
  DB_LOGICAL res = V_FALSE;

  /* setup ordby_info so that we can evaluate the orderby_num() predicate */
  ordby_info.xasl_state = xasl_state;
  ordby_info.ordbynum_pred = xasl->ordbynum_pred;
  ordby_info.ordbynum_flag = xasl->ordbynum_flag;
  ordby_info.ordbynum_pos_cnt = 0;
  ordby_info.ordbynum_val = xasl->ordbynum_val;
  DB_MAKE_BIGINT (ordby_info.ordbynum_val, 0);

  list_id = xasl->list_id;
  topn = xasl->topn_items;
  heap = topn->heap;
  tpl_descr = &list_id->tpl_descr;
  values_count = topn->values_count;
  xasl->orderby_stats.orderby_topnsort = true;

  /* convert binary heap to sorted array */
  bh_to_sorted_array (heap);

  /* dump all items in heap to listfile */
  if (tpl_descr->f_valp == NULL && list_id->type_list.type_cnt > 0)
    {
      size_t size = values_count * DB_SIZEOF (DB_VALUE *);

      tpl_descr->f_valp = (DB_VALUE **) malloc (size);
      if (tpl_descr->f_valp == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, size);
	  error = ER_FAILED;
	  goto cleanup;
	}
    }
  varp = xasl->outptr_list->valptrp;
  for (row = 0; row < heap->element_count; row++)
    {
      tuple = (TOPN_TUPLE *) heap->members[row];

      /* evaluate orderby_num predicate */
      res = qexec_eval_ordbynum_pred (thread_p, &ordby_info);
      if (res != V_TRUE)
	{
	  if (res == V_ERROR)
	    {
	      error = ER_FAILED;
	      goto cleanup;
	    }

	  if (is_final)
	    {
	      /* skip this tuple */
	      qexec_clear_topn_tuple (thread_p, tuple, values_count);
	      heap->members[row] = NULL;
	      continue;
	    }
	}

      tuple = (TOPN_TUPLE *) heap->members[row];
      tpl_descr->tpl_size = QFILE_TUPLE_LENGTH_SIZE;

      tpl_descr->f_cnt = 0;

      for (varp = xasl->outptr_list->valptrp; varp != NULL; varp = varp->next)
	{
	  if (REGU_VARIABLE_IS_FLAGED (&varp->value,
				       REGU_VARIABLE_HIDDEN_COLUMN))
	    {
	      continue;
	    }

	  if (varp->value.type == TYPE_ORDERBY_NUM)
	    {
	      assert (tuple->values[tpl_descr->f_cnt].need_clear == false);
	      DB_MAKE_NULL (&tuple->values[tpl_descr->f_cnt]);	/* init */
	      pr_clone_value (ordby_info.ordbynum_val,
			      &tuple->values[tpl_descr->f_cnt]);
	    }

	  tpl_descr->f_valp[tpl_descr->f_cnt] =
	    &tuple->values[tpl_descr->f_cnt];

	  value_size =
	    qdata_get_tuple_value_size_from_dbval (&tuple->
						   values[tpl_descr->f_cnt]);
	  if (value_size == ER_FAILED)
	    {
	      error = value_size;
	      goto cleanup;
	    }

	  tpl_descr->tpl_size += value_size;
	  tpl_descr->f_cnt++;
	}

      error = qfile_generate_tuple_into_list (thread_p, list_id, T_NORMAL);
      if (error != NO_ERROR)
	{
	  goto cleanup;
	}
      /* clear tuple values */
      qexec_clear_topn_tuple (thread_p, tuple, values_count);
      heap->members[row] = NULL;
    }

cleanup:
  if (tuple != NULL)
    {
      qexec_clear_topn_tuple (thread_p, tuple, values_count);
    }

  for (i = row; i < heap->element_count; i++)
    {
      if (heap->members[i] != NULL)
	{
	  tuple = (TOPN_TUPLE *) heap->members[i];
	  qexec_clear_topn_tuple (thread_p, tuple, values_count);
	  heap->members[row] = NULL;
	}
    }

  if (heap != NULL)
    {
      bh_destroy (thread_p, heap);
      topn->heap = NULL;
    }

  if (xasl->topn_items != NULL)
    {
      if (xasl->topn_items->tuples != NULL)
	{
	  free_and_init (xasl->topn_items->tuples);
	}
      free_and_init (xasl->topn_items);
    }

  if (is_final)
    {
      qfile_close_list (thread_p, list_id);
    }
  else
    {
      /* reset ORDERBY_NUM value */
      assert (DB_VALUE_TYPE (xasl->ordbynum_val) == DB_TYPE_BIGINT);
      DB_MAKE_BIGINT (xasl->ordbynum_val, 0);
    }
  return error;
}

/*
 * qexec_clear_topn_tuple () - clear values of a top-n tuple
 * return : void
 * thread_p (in)  :
 * tuple (in/out) : top-N tuple
 * count (in)	  : number of values
 */
static void
qexec_clear_topn_tuple (UNUSED_ARG THREAD_ENTRY * thread_p,
			TOPN_TUPLE * tuple, int count)
{
  int i;
  if (tuple == NULL)
    {
      return;
    }

  if (tuple->values != NULL)
    {
      for (i = 0; i < count; i++)
	{
	  pr_clear_value (&tuple->values[i]);
	}
      free_and_init (tuple->values);
    }
  tuple->values_size = 0;
}

/*
 * qexec_get_orderbynum_upper_bound - get upper bound for orderby_num
 *				      predicate
 * return: error code or NO_ERROR
 * thread_p	   : thread entry
 * pred (in)	   : orderby_num predicate
 * vd (in)	   : value descriptor
 * ubound (in/out) : upper bound
 */
static int
qexec_get_orderbynum_upper_bound (THREAD_ENTRY * thread_p, PRED_EXPR * pred,
				  VAL_DESCR * vd, DB_VALUE * ubound)
{
  int error = NO_ERROR;
  REGU_VARIABLE *lhs, *rhs;
  REL_OP op;
  DB_VALUE *val;
  int cmp;
  DB_VALUE left_bound, right_bound;

  assert_release (pred != NULL);
  assert_release (ubound != NULL);

  DB_MAKE_NULL (ubound);
  DB_MAKE_NULL (&left_bound);
  DB_MAKE_NULL (&right_bound);

  if (pred->type == T_PRED && pred->pe.pred.bool_op == B_AND)
    {
      error = qexec_get_orderbynum_upper_bound (thread_p, pred->pe.pred.lhs,
						vd, &left_bound);
      if (error != NO_ERROR)
	{
	  goto error_return;
	}

      error = qexec_get_orderbynum_upper_bound (thread_p, pred->pe.pred.rhs,
						vd, &right_bound);
      if (error != NO_ERROR)
	{
	  goto error_return;
	}

      if (DB_IS_NULL (&left_bound) && DB_IS_NULL (&right_bound))
	{
	  /* no valid bounds */
	  goto cleanup;
	}

      cmp = tp_value_compare (&left_bound, &right_bound, 1, 1, NULL);
      assert (cmp != DB_UNK);
      if (cmp == DB_GT)
	{
	  error = pr_clone_value (&left_bound, ubound);
	}
      else
	{
	  error = pr_clone_value (&right_bound, ubound);
	}

      if (error != NO_ERROR)
	{
	  goto error_return;
	}

      goto cleanup;
    }

  if (pred->type == T_EVAL_TERM)
    {
      /* This should be TYPE_CONSTANT comp TYPE_VALUE. If not, we bail out */
      lhs = pred->pe.eval_term.et.et_comp.comp_lhs;
      rhs = pred->pe.eval_term.et.et_comp.comp_rhs;
      op = pred->pe.eval_term.et.et_comp.comp_rel_op;
      if (lhs->type != TYPE_CONSTANT)
	{
	  if (lhs->type != TYPE_POS_VALUE && lhs->type != TYPE_DBVAL)
	    {
	      goto cleanup;
	    }

	  if (rhs->type != TYPE_CONSTANT)
	    {
	      goto cleanup;
	    }

	  /* reverse comparison */
	  rhs = lhs;
	  lhs = pred->pe.eval_term.et.et_comp.comp_rhs;
	  switch (op)
	    {
	    case R_GT:
	      op = R_LT;
	      break;
	    case R_GE:
	      op = R_LE;
	      break;
	    case R_LT:
	      op = R_GT;
	      break;
	    case R_LE:
	      op = R_GE;
	      break;
	    default:
	      goto cleanup;
	    }
	}
      if (rhs->type != TYPE_POS_VALUE && rhs->type != TYPE_DBVAL)
	{
	  goto cleanup;
	}

      if (op != R_LT && op != R_LE)
	{
	  /* we're only interested in orderby_num less than value */
	  goto cleanup;
	}

      error = fetch_peek_dbval (thread_p, rhs, vd, NULL, NULL, &val);
      if (error != NO_ERROR)
	{
	  goto error_return;
	}

      if (op == R_LT)
	{
	  /* add 1 so we can use R_LE */
	  DB_VALUE one_val;
	  DB_MAKE_INT (&one_val, 1);
	  error = qdata_subtract_dbval (val, &one_val, ubound);
	}
      else
	{
	  error = pr_clone_value (val, ubound);
	}

      if (error != NO_ERROR)
	{
	  goto error_return;
	}
      goto cleanup;
    }

  return error;

error_return:
  DB_MAKE_NULL (ubound);

cleanup:
  pr_clear_value (&left_bound);
  pr_clear_value (&right_bound);

  return error;
}

#if defined(SERVER_MODE)
/*
 * qexec_set_xasl_trace_to_session() - save query trace to session
 *   return:
 *   xasl(in): sort direction ascending or descending
 */
static void
qexec_set_xasl_trace_to_session (THREAD_ENTRY * thread_p, XASL_NODE * xasl)
{
  size_t sizeloc;
  char *trace_str = NULL;
  FILE *fp;
  json_t *trace;

  if (thread_p->trace_format == QUERY_TRACE_TEXT)
    {
      fp = port_open_memstream (&trace_str, &sizeloc);
      if (fp)
	{
	  qdump_print_stats_text (fp, xasl, 0);
	  port_close_memstream (fp, &trace_str, &sizeloc);
	}
    }
  else if (thread_p->trace_format == QUERY_TRACE_JSON)
    {
      trace = json_object ();
      qdump_print_stats_json (xasl, trace);
      trace_str = json_dumps (trace, JSON_INDENT (2) | JSON_PRESERVE_ORDER);

      json_object_clear (trace);
      json_decref (trace);
    }

  if (trace_str != NULL)
    {
      session_set_trace_stats (thread_p, trace_str, thread_p->trace_format);
    }
}
#endif /* SERVER_MODE */

/*
 * qexec_xasl_qstr_ht_hash () - xasl query string hash function
 *
 *   return:
 *   key(in):
 *   ht_size(in):
 */
static unsigned int
qexec_xasl_qstr_ht_hash (const void *key, unsigned int ht_size)
{
  XASL_QSTR_HT_KEY *ht_key;

  assert (key != NULL);
  ht_key = (XASL_QSTR_HT_KEY *) key;

  assert (ht_key->query_string != NULL);

  return mht_1strhash ((void *) ht_key->query_string, ht_size);
}

/*
 * qexec_xasl_qstr_ht_keys_are_equal () -
 *    xasl query string hash table key compare function
 *
 *   return:
 *   key1(in):
 *   key2(in):
 */
static int
qexec_xasl_qstr_ht_keys_are_equal (const void *key1, const void *key2)
{
  int ret = 0;			/* initialize as keys are not equal */
  XASL_QSTR_HT_KEY *ht_key1, *ht_key2;

  assert (key1 != NULL && key2 != NULL);

  ht_key1 = (XASL_QSTR_HT_KEY *) key1;
  ht_key2 = (XASL_QSTR_HT_KEY *) key2;

  assert (ht_key1->query_string != NULL && ht_key2->query_string != NULL);

  if (OID_EQ (&ht_key1->creator_oid, &ht_key2->creator_oid))
    {
      ret = mht_compare_strings_are_equal (ht_key1->query_string,
					   ht_key2->query_string);
    }

  return ret;
}
