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
 * scan_manager.c - scan management routines
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <string.h>
#include <assert.h>
#include "jansson.h"

#include "error_manager.h"
#include "memory_alloc.h"
#include "page_buffer.h"
#include "slotted_page.h"
#include "btree.h"
#include "heap_file.h"
#include "object_representation.h"
#include "object_representation_sr.h"
#include "fetch.h"
#include "list_file.h"
#include "system_parameter.h"
#include "btree_load.h"
#include "perf_monitor.h"
#include "query_manager.h"
#include "xasl_support.h"
#include "xserver_interface.h"

/* this must be the last header file included!!! */
#include "dbval.h"

#if !defined(SERVER_MODE)
#define pthread_mutex_init(a, b)
#define pthread_mutex_destroy(a)
#define pthread_mutex_lock(a)	0
#define pthread_mutex_unlock(a)
static int rv;
#endif

/* this macro is used to make sure that heap file identifier is initialized
 * properly that heap file scan routines will work properly.
 */
#define UT_CAST_TO_NULL_HEAP_OID(hfidp,oidp) \
  do { (oidp)->pageid = NULL_PAGEID; \
       (oidp)->volid = (hfidp)->vfid.volid; \
       (oidp)->slotid = NULL_SLOTID; \
  } while (0)

#define GET_NTH_OID(oid_setp, n) ((OID *)((OID *)(oid_setp) + (n)))

/* Depending on the isolation level, there are times when we may not be
 *  able to fetch a scan item.
 */
#define QPROC_OK_IF_DELETED(scan, iso) \
  ((scan) == S_DOESNT_EXIST && (iso) == TRAN_DEFAULT_ISOLATION)

typedef int QPROC_KEY_VAL_FU (KEY_VAL_RANGE * key_vals, int key_cnt);
typedef SCAN_CODE (*QP_SCAN_FUNC) (THREAD_ENTRY * thread_p, SCAN_ID * s_id);

typedef enum
{
  ROP_NA, ROP_EQ,
  ROP_GE, ROP_GT, ROP_GT_INF, ROP_GT_ADJ,
  ROP_LE, ROP_LT, ROP_LT_INF, ROP_LT_ADJ
} ROP_TYPE;

struct rop_range_struct
{
  ROP_TYPE left;
  ROP_TYPE right;
  RANGE range;
} rop_range_table[] =
{
  {
  ROP_NA, ROP_EQ, NA_NA},
  {
  ROP_GE, ROP_LE, GE_LE},
  {
  ROP_GE, ROP_LT, GE_LT},
  {
  ROP_GT, ROP_LE, GT_LE},
  {
  ROP_GT, ROP_LT, GT_LT},
  {
  ROP_GE, ROP_LT_INF, GE_INF},
  {
  ROP_GT, ROP_LT_INF, GT_INF},
  {
  ROP_GT_INF, ROP_LE, INF_LE},
  {
  ROP_GT_INF, ROP_LT, INF_LT},
  {
  ROP_GT_INF, ROP_LT_INF, INF_INF}
};

static const int rop_range_table_size =
  sizeof (rop_range_table) / sizeof (struct rop_range_struct);

#if defined(SERVER_MODE)
static pthread_mutex_t scan_Iscan_oid_buf_list_mutex =
  PTHREAD_MUTEX_INITIALIZER;
#endif
static OID *scan_Iscan_oid_buf_list = NULL;
static int scan_Iscan_oid_buf_list_count = 0;

static int scan_init_scan_pred (SCAN_PRED * scan_pred_p,
				REGU_VARIABLE_LIST regu_list,
				PRED_EXPR * pred_expr,
				PR_EVAL_FNC pr_eval_fnc);
static void scan_init_scan_attrs (SCAN_ATTRS * scan_attrs_p, int num_attrs,
				  ATTR_ID * attr_ids,
				  HEAP_CACHE_ATTRINFO * attr_cache);
static void scan_init_filter_info (FILTER_INFO * filter_info_p,
				   SCAN_PRED * scan_pred,
				   SCAN_ATTRS * scan_attrs,
				   VAL_LIST * val_list, VAL_DESCR * val_descr,
				   OID * class_oid, OR_CLASSREP * classrepr,
				   int indx_id);
static int scan_init_indx_coverage (THREAD_ENTRY * thread_p,
				    int coverage_enabled,
				    OUTPTR_LIST * output_val_list,
				    REGU_VARIABLE_LIST regu_val_list,
				    VAL_DESCR * vd, QUERY_ID query_id,
				    INDX_COV * indx_cov);
static OID *scan_alloc_oid_buf (void);
static OID *scan_alloc_iscan_oid_buf_list (void);
static void scan_free_iscan_oid_buf_list (OID * oid_buf_p);
static void rop_to_range (RANGE * range, ROP_TYPE left, ROP_TYPE right);
static void range_to_rop (ROP_TYPE * left, ROP_TYPE * rightk, RANGE range);
static ROP_TYPE compare_val_op (const DB_IDXKEY * key1,
				const ROP_TYPE op1,
				const DB_IDXKEY * key2,
				const ROP_TYPE op2, const int num_index_term);
static int key_val_compare (const void *p1, const void *p2);
static int eliminate_duplicated_keys (KEY_VAL_RANGE * key_vals, int key_cnt);
static int merge_key_ranges (KEY_VAL_RANGE * key_vals, int key_cnt);
static int reverse_key_list (KEY_VAL_RANGE * key_vals, int key_cnt);
static int check_key_vals (KEY_VAL_RANGE * key_vals, int key_cnt,
			   QPROC_KEY_VAL_FU * chk_fn);
static int scan_dbvals_to_idxkey (THREAD_ENTRY * thread_p,
				  DB_IDXKEY * retval,
				  bool * indexal, OR_INDEX * indexp,
				  int num_term, REGU_VARIABLE * func,
				  REGU_VARIABLE_LIST pk_next_regu_list,
				  VAL_DESCR * vd, int key_minmax);
static int scan_regu_key_to_index_key (THREAD_ENTRY * thread_p,
				       KEY_RANGE * key_ranges,
				       KEY_VAL_RANGE * key_val_range,
				       INDX_SCAN_ID * iscan_id,
				       VAL_DESCR * vd);
static int scan_get_index_oidset (THREAD_ENTRY * thread_p, SCAN_ID * s_id);
static void scan_init_scan_id (SCAN_ID * scan_id,
			       int fixed,
			       QPROC_FETCH_TYPE fetch_type,
			       VAL_LIST * val_list, VAL_DESCR * vd);
static int scan_init_index_key_limit (THREAD_ENTRY * thread_p,
				      INDX_SCAN_ID * isidp,
				      KEY_INFO * key_infop, VAL_DESCR * vd);
static SCAN_CODE scan_next_scan_local (THREAD_ENTRY * thread_p,
				       SCAN_ID * scan_id);
static SCAN_CODE scan_next_heap_scan (THREAD_ENTRY * thread_p,
				      SCAN_ID * scan_id);
static SCAN_CODE scan_next_index_scan (THREAD_ENTRY * thread_p,
				       SCAN_ID * scan_id);
static SCAN_CODE scan_next_index_lookup_heap (THREAD_ENTRY * thread_p,
					      SCAN_ID * scan_id,
					      INDX_SCAN_ID * isidp,
					      FILTER_INFO * data_filter,
					      TRAN_ISOLATION isolation);
static SCAN_CODE scan_next_list_scan (THREAD_ENTRY * thread_p,
				      SCAN_ID * scan_id);
static SCAN_CODE scan_handle_single_scan (THREAD_ENTRY * thread_p,
					  SCAN_ID * s_id,
					  QP_SCAN_FUNC next_scan);
static int scan_init_multi_range_optimization (THREAD_ENTRY * thread_p,
					       MULTI_RANGE_OPT *
					       multi_range_opt,
					       bool use_range_opt,
					       int max_size);
static int scan_dump_key_into_tuple (THREAD_ENTRY * thread_p,
				     INDX_SCAN_ID * iscan_id,
				     const DB_IDXKEY * key,
				     OID * oid, QFILE_TUPLE_RECORD * tplrec);
static SCAN_CODE call_get_next_index_oidset (THREAD_ENTRY * thread_p,
					     SCAN_ID * scan_id,
					     INDX_SCAN_ID * isidp);
static int scan_key_compare (const DB_IDXKEY * key1,
			     const DB_IDXKEY * key2,
			     const int num_index_term);


/*
 * scan_init_index_scan () - initialize an index scan structure with the
 *			     specified OID buffer
 * return : void
 * isidp(in)	        : index scan
 * oid_buf(in)          : OID buffer
 * oid_buf_size(in)     : OID buffer size
 */
void
scan_init_index_scan (INDX_SCAN_ID * isidp, OID * oid_buf, int oid_buf_size)
{
  if (isidp == NULL)
    {
      assert (false);
      return;
    }

  isidp->oid_list.oid_cnt = 0;
  isidp->oid_list.oidp = oid_buf;
  isidp->oid_list.oid_buf_size = oid_buf_size;
  memset ((void *) (&(isidp->indx_cov)), 0, sizeof (INDX_COV));
  isidp->key_limit_lower = -1;
  isidp->key_limit_upper = -1;
  isidp->indx_info = NULL;
  memset ((void *) (&(isidp->multi_range_opt)), 0, sizeof (MULTI_RANGE_OPT));
  isidp->need_count_only = false;
}

/*
 * scan_init_scan_pred () - initialize SCAN_PRED structure
 *   return: error_code
 */
static int
scan_init_scan_pred (SCAN_PRED * scan_pred_p, REGU_VARIABLE_LIST regu_list,
		     PRED_EXPR * pred_expr, PR_EVAL_FNC pr_eval_fnc)
{
  assert (scan_pred_p != NULL);

  scan_pred_p->regu_list = regu_list;
  scan_pred_p->pred_expr = pred_expr;
  scan_pred_p->pr_eval_fnc = pr_eval_fnc;

  if ((scan_pred_p->pred_expr != NULL && scan_pred_p->pr_eval_fnc != NULL)
      || (scan_pred_p->pred_expr == NULL && scan_pred_p->pr_eval_fnc == NULL))
    {
      ;				/* go ahead */
    }
  else
    {
      assert (false);		/* something wrong */
      return ER_FAILED;
    }

  return NO_ERROR;
}

/*
 * scan_init_scan_attrs () - initialize SCAN_ATTRS structure
 *   return: none
 */
static void
scan_init_scan_attrs (SCAN_ATTRS * scan_attrs_p, int num_attrs,
		      ATTR_ID * attr_ids, HEAP_CACHE_ATTRINFO * attr_cache)
{
  assert (scan_attrs_p != NULL);

  scan_attrs_p->num_attrs = num_attrs;
  scan_attrs_p->attr_ids = attr_ids;
  scan_attrs_p->attr_cache = attr_cache;
}

/*
 * scan_init_filter_info () - initialize FILTER_INFO structure as a data/key filter
 *   return: none
 */
static void
scan_init_filter_info (FILTER_INFO * filter_info_p, SCAN_PRED * scan_pred,
		       SCAN_ATTRS * scan_attrs, VAL_LIST * val_list,
		       VAL_DESCR * val_descr, OID * class_oid,
		       OR_CLASSREP * classrepr, int indx_id)
{
  assert (filter_info_p != NULL);

  filter_info_p->scan_pred = scan_pred;
  filter_info_p->scan_attrs = scan_attrs;
  filter_info_p->val_list = val_list;
  filter_info_p->val_descr = val_descr;
  filter_info_p->class_oid = class_oid;
  filter_info_p->classrepr = classrepr;
  filter_info_p->indx_id = indx_id;
}

/*
 * scan_init_indx_coverage () - initialize INDX_COV structure
 *   return: error code
 *
 * coverage_enabled(in): true if coverage is enabled
 * output_val_list(in): output val list
 * regu_val_list(in): regu val list
 * vd(in): val descriptor
 * query_id(in): the query id
 * indx_cov(in/out): index coverage data
 */
static int
scan_init_indx_coverage (THREAD_ENTRY * thread_p,
			 int coverage_enabled,
			 OUTPTR_LIST * output_val_list,
			 REGU_VARIABLE_LIST regu_val_list,
			 VAL_DESCR * vd,
			 QUERY_ID query_id, INDX_COV * indx_cov)
{
  int err = NO_ERROR;
  int num_membuf_pages = 0;

  if (indx_cov == NULL)
    {
      return ER_FAILED;
    }

  indx_cov->val_descr = vd;
  indx_cov->output_val_list = output_val_list;
  indx_cov->regu_val_list = regu_val_list;
  indx_cov->query_id = query_id;

  if (coverage_enabled == false)
    {
      indx_cov->type_list = NULL;
      indx_cov->list_id = NULL;
      indx_cov->tplrec = NULL;
      indx_cov->lsid = NULL;
      indx_cov->max_tuples = 0;
      return NO_ERROR;
    }

  indx_cov->type_list = (QFILE_TUPLE_VALUE_TYPE_LIST *)
    malloc (sizeof (QFILE_TUPLE_VALUE_TYPE_LIST));
  if (indx_cov->type_list == NULL)
    {
      err = ER_OUT_OF_VIRTUAL_MEMORY;
      goto exit_on_error;
    }

  if (qdata_get_valptr_type_list (thread_p,
				  output_val_list,
				  indx_cov->type_list) != NO_ERROR)
    {
      err = ER_FAILED;
      goto exit_on_error;
    }

  /*
   * Covering index scan needs large-size memory buffer in order to decrease
   * the number of times doing stop-and-resume during btree_range_search.
   * To do it, QFILE_FLAG_USE_KEY_BUFFER is introduced. If the flag is set,
   * the list file allocates PRM_INDEX_SCAN_KEY_BUFFER_SIZE memory
   * for its memory buffer, which is generally larger than prm_get_integer_value (PRM_ID_TEMP_MEM_BUFFER_SIZE).
   */
  indx_cov->list_id = qfile_open_list (thread_p, indx_cov->type_list, NULL,
				       query_id, QFILE_FLAG_USE_KEY_BUFFER);
  if (indx_cov->list_id == NULL)
    {
      err = ER_FAILED;
      goto exit_on_error;
    }

  num_membuf_pages =
    qmgr_get_temp_file_membuf_pages (indx_cov->list_id->tfile_vfid);
  assert (num_membuf_pages > 0);

  if (num_membuf_pages > 0)
    {
      indx_cov->max_tuples =
	(num_membuf_pages * IO_PAGESIZE) / BTREE_MAX_KEYLEN;
      indx_cov->max_tuples = MAX (indx_cov->max_tuples, 1);
    }
  else
    {
      indx_cov->max_tuples = IDX_COV_DEFAULT_TUPLES;
    }

  indx_cov->tplrec =
    (QFILE_TUPLE_RECORD *) malloc (sizeof (QFILE_TUPLE_RECORD));
  if (indx_cov->tplrec == NULL)
    {
      err = ER_OUT_OF_VIRTUAL_MEMORY;
      goto exit_on_error;
    }
  indx_cov->tplrec->size = 0;
  indx_cov->tplrec->tpl = NULL;

  indx_cov->lsid =
    (QFILE_LIST_SCAN_ID *) malloc (sizeof (QFILE_LIST_SCAN_ID));
  if (indx_cov->lsid == NULL)
    {
      err = ER_OUT_OF_VIRTUAL_MEMORY;
      goto exit_on_error;
    }
  indx_cov->lsid->status = S_CLOSED;

  return NO_ERROR;

exit_on_error:

  if (indx_cov->type_list != NULL)
    {
      if (indx_cov->type_list->domp != NULL)
	{
	  free_and_init (indx_cov->type_list->domp);
	}
      free_and_init (indx_cov->type_list);
    }

  if (indx_cov->list_id != NULL)
    {
      QFILE_FREE_AND_INIT_LIST_ID (indx_cov->list_id);
    }

  if (indx_cov->tplrec != NULL)
    {
      if (indx_cov->tplrec->tpl != NULL)
	{
	  free_and_init (indx_cov->tplrec->tpl);
	}
      free_and_init (indx_cov->tplrec);
    }

  if (indx_cov->lsid != NULL)
    {
      free_and_init (indx_cov->lsid);
    }

  return err;
}

/*
 * scan_init_index_key_limit () - initialize/reset index key limits
 *   return: error code
 */
static int
scan_init_index_key_limit (THREAD_ENTRY * thread_p, INDX_SCAN_ID * isidp,
			   KEY_INFO * key_infop, VAL_DESCR * vd)
{
  DB_VALUE *dbvalp;
  TP_DOMAIN *domainp = tp_domain_resolve_default (DB_TYPE_BIGINT);
  DB_TYPE orig_type;
  bool is_lower_limit_negative = false;


  if (key_infop->key_limit_l != NULL)
    {
      if (fetch_peek_dbval (thread_p, key_infop->key_limit_l, vd, NULL,
			    NULL, &dbvalp) != NO_ERROR)
	{
	  goto exit_on_error;
	}
      orig_type = DB_VALUE_DOMAIN_TYPE (dbvalp);
      if (DB_IS_NULL (dbvalp)
	  || tp_value_coerce (dbvalp, dbvalp, domainp) != DOMAIN_COMPATIBLE)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
		  pr_type_name (orig_type),
		  pr_type_name (TP_DOMAIN_TYPE (domainp)));
	  goto exit_on_error;
	}

      assert (DB_VALUE_DOMAIN_TYPE (dbvalp) == DB_TYPE_BIGINT);

      isidp->key_limit_lower = DB_GET_BIGINT (dbvalp);

      /* SELECT * from t where ROWNUM = 0 order by a: this would sometimes
       * get optimized using keylimit, if the circumstances are right.
       * in this case, the lower limit would be "0-1", effectiveley -1.
       *
       * We cannot allow that to happen, since -1 is a special value
       * meaning "there is no lower limit", and certain critical decisions
       * (such as resetting the key limit for multiple ranges) depend on
       * knowing whether or not there is a lower key limit.
       *
       * We set a flag to remember, later on, to "adjust" the key limits such
       * that, if the lower limit is negative, to return no results.
       */
      if (isidp->key_limit_lower < 0)
	{
	  is_lower_limit_negative = true;
	}
    }
  else
    {
      isidp->key_limit_lower = -1;
    }

  if (key_infop->key_limit_u != NULL)
    {
      if (fetch_peek_dbval (thread_p, key_infop->key_limit_u, vd, NULL,
			    NULL, &dbvalp) != NO_ERROR)
	{
	  goto exit_on_error;
	}
      orig_type = DB_VALUE_DOMAIN_TYPE (dbvalp);
      if (DB_IS_NULL (dbvalp)
	  || tp_value_coerce (dbvalp, dbvalp, domainp) != DOMAIN_COMPATIBLE)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
		  pr_type_name (orig_type),
		  pr_type_name (TP_DOMAIN_TYPE (domainp)));
	  goto exit_on_error;
	}

      assert (DB_VALUE_DOMAIN_TYPE (dbvalp) == DB_TYPE_BIGINT);

      isidp->key_limit_upper = DB_GET_BIGINT (dbvalp);

      /* Try to sanitize the upper value. It might have been computed from
       * operations on host variables, which are unpredictable.
       */
      if (isidp->key_limit_upper < 0)
	{
	  isidp->key_limit_upper = 0;
	}
    }
  else
    {
      isidp->key_limit_upper = -1;
    }

  if (is_lower_limit_negative && isidp->key_limit_upper > 0)
    {
      /* decrease the upper limit: key_limit_lower is negative */
      isidp->key_limit_upper += isidp->key_limit_lower;
      if (isidp->key_limit_upper < 0)
	{
	  isidp->key_limit_upper = 0;
	}
      isidp->key_limit_lower = 0;	/* reset it to something useable */
    }

  return NO_ERROR;

exit_on_error:

  return ER_FAILED;
}

/*
 * scan_alloc_oid_buf () - allocate oid buf
 *   return: pointer to alloced oid buf, NULL for error
 */
static OID *
scan_alloc_oid_buf (void)
{
  int oid_buf_size;
  OID *oid_buf_p;

  oid_buf_size = ISCAN_OID_BUFFER_SIZE;
  oid_buf_p = (OID *) malloc (oid_buf_size);
  if (oid_buf_p == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, oid_buf_size);
    }

  return oid_buf_p;
}

/*
 * scan_alloc_iscan_oid_buf_list () - allocate list of oid buf
 *   return: pointer to alloced oid buf, NULL for error
 */
static OID *
scan_alloc_iscan_oid_buf_list (void)
{
  OID *oid_buf_p;
#if defined (SERVER_MODE)
  UNUSED_VAR int rv;
#endif /* SERVER_MODE */

  oid_buf_p = NULL;

  if (scan_Iscan_oid_buf_list != NULL)
    {
      rv = pthread_mutex_lock (&scan_Iscan_oid_buf_list_mutex);
      if (scan_Iscan_oid_buf_list != NULL)
	{
	  oid_buf_p = scan_Iscan_oid_buf_list;
	  /* save previous oid buf pointer */
	  scan_Iscan_oid_buf_list = (OID *) (*(intptr_t *) oid_buf_p);
	  scan_Iscan_oid_buf_list_count--;
	}
      pthread_mutex_unlock (&scan_Iscan_oid_buf_list_mutex);
    }

  if (oid_buf_p == NULL)	/* need to alloc */
    {
      oid_buf_p = scan_alloc_oid_buf ();
    }

  return oid_buf_p;
}

/*
 * scan_free_iscan_oid_buf_list () - free the given iscan oid buf
 *   return: NO_ERROR
 */
static void
scan_free_iscan_oid_buf_list (OID * oid_buf_p)
{
#if defined (SERVER_MODE)
  UNUSED_VAR int rv;
#endif /* SERVER_MODE */

  rv = pthread_mutex_lock (&scan_Iscan_oid_buf_list_mutex);
  if (scan_Iscan_oid_buf_list_count < thread_num_worker_threads ())
    {
      /* save previous oid buf pointer */
      *(intptr_t *) oid_buf_p = (intptr_t) scan_Iscan_oid_buf_list;
      scan_Iscan_oid_buf_list = oid_buf_p;
      scan_Iscan_oid_buf_list_count++;
    }
  else
    {
      free_and_init (oid_buf_p);
    }
  pthread_mutex_unlock (&scan_Iscan_oid_buf_list_mutex);
}

/*
 * rop_to_range () - map left/right to range operator
 *   return:
 *   range(out): full-RANGE operator
 *   left(in): left-side range operator
 *   right(in): right-side range operator
 */
static void
rop_to_range (RANGE * range, ROP_TYPE left, ROP_TYPE right)
{
  int i;

  *range = NA_NA;

  for (i = 0; i < rop_range_table_size; i++)
    {
      if (left == rop_range_table[i].left
	  && right == rop_range_table[i].right)
	{
	  /* found match */
	  *range = rop_range_table[i].range;
	  break;
	}
    }
}

/*
 * range_to_rop () - map range to left/right operator
 *   return:
 *   left(out): left-side range operator
 *   right(out): right-side range operator
 *   range(in): full-RANGE operator
 */
static void
range_to_rop (ROP_TYPE * left, ROP_TYPE * right, RANGE range)
{
  int i;

  *left = ROP_NA;
  *right = ROP_NA;

  for (i = 0; i < rop_range_table_size; i++)
    {
      if (range == rop_range_table[i].range)
	{
	  /* found match */
	  *left = rop_range_table[i].left;
	  *right = rop_range_table[i].right;
	  break;
	}
    }
}

/*
 * scan_key_compare ()
 *   key1(in):
 *   key2(in):
 *   num_index_term(in):
 *   return:
 */
static int
scan_key_compare (const DB_IDXKEY * key1, const DB_IDXKEY * key2,
		  const int num_index_term)
{
  int rc = DB_UNK;

  if (key1 == NULL || key2 == NULL)
    {
      assert_release (false);
      return rc;
    }

  if (DB_IDXKEY_IS_NULL (key1))
    {
      if (DB_IDXKEY_IS_NULL (key2))
	{
	  rc = DB_EQ;
	}
      else
	{
	  rc = DB_LT;
	}
    }
  else if (DB_IDXKEY_IS_NULL (key2))
    {
      rc = DB_GT;
    }
  else
    {
      rc = pr_idxkey_compare (key1, key2, num_index_term, NULL);
    }

  return rc;
}

/*
 * compare_val_op () - compare two values specified by range operator
 *   return:
 *   key1(in):
 *   op1(in):
 *   key2(in):
 *   op2(in):
 *   num_index_term(in):
 */
static ROP_TYPE
compare_val_op (const DB_IDXKEY * key1, const ROP_TYPE op1,
		const DB_IDXKEY * key2, const ROP_TYPE op2,
		const int num_index_term)
{
  int rc;

  if (op1 == ROP_GT_INF)	/* val1 is -INF */
    {
      return (op1 == op2) ? ROP_EQ : ROP_LT;
    }
  if (op1 == ROP_LT_INF)	/* val1 is +INF */
    {
      return (op1 == op2) ? ROP_EQ : ROP_GT;
    }
  if (op2 == ROP_GT_INF)	/* val2 is -INF */
    {
      return (op2 == op1) ? ROP_EQ : ROP_GT;
    }
  if (op2 == ROP_LT_INF)	/* val2 is +INF */
    {
      return (op2 == op1) ? ROP_EQ : ROP_LT;
    }

  rc = scan_key_compare (key1, key2, num_index_term);

  if (rc == DB_EQ)
    {
      /* (val1, op1) == (val2, op2) */
      if (op1 == op2)
	{
	  return ROP_EQ;
	}
      if (op1 == ROP_EQ || op1 == ROP_GE || op1 == ROP_LE)
	{
	  if (op2 == ROP_EQ || op2 == ROP_GE || op2 == ROP_LE)
	    {
	      return ROP_EQ;
	    }
	  return (op2 == ROP_GT) ? ROP_LT_ADJ : ROP_GT_ADJ;
	}
      if (op1 == ROP_GT)
	{
	  if (op2 == ROP_EQ || op2 == ROP_GE || op2 == ROP_LE)
	    {
	      return ROP_GT_ADJ;
	    }
	  return (op2 == ROP_LT) ? ROP_GT : ROP_EQ;
	}
      if (op1 == ROP_LT)
	{
	  if (op2 == ROP_EQ || op2 == ROP_GE || op2 == ROP_LE)
	    {
	      return ROP_LT_ADJ;
	    }
	  return (op2 == ROP_GT) ? ROP_LT : ROP_EQ;
	}
    }
  else if (rc == DB_LT)
    {
      /* (val1, op1) < (val2, op2) */
      return ROP_LT;
    }
  else if (rc == DB_GT)
    {
      /* (val1, op1) > (val2, op2) */
      return ROP_GT;
    }

  /* tp_value_compare() returned error? */
  return (rc == DB_EQ) ? ROP_EQ : ROP_NA;
}

/*
 * key_val_compare () - key value sorting function
 *   return:
 *   p1 (in): pointer to key1 range
 *   p2 (in): pointer to key2 range
 */
static int
key_val_compare (const void *p1, const void *p2)
{
  int num_index_term;
  const DB_IDXKEY *p1_key, *p2_key;

  num_index_term = ((KEY_VAL_RANGE *) p1)->num_index_term;
  assert_release (num_index_term == ((KEY_VAL_RANGE *) p2)->num_index_term);

  p1_key = &((KEY_VAL_RANGE *) p1)->lower_key;
  p2_key = &((KEY_VAL_RANGE *) p2)->lower_key;

  return scan_key_compare (p1_key, p2_key, num_index_term);
}

/*
 * eliminate_duplicated_keys () - elimnate duplicated key values
 *   return: number of keys, -1 for error
 *   key_vals (in): pointer to array of KEY_VAL_RANGE structure
 *   key_cnt (in): number of keys; size of key_vals
 */
static int
eliminate_duplicated_keys (KEY_VAL_RANGE * key_vals, int key_cnt)
{
  int n;
  DB_VALUE_COMPARE_RESULT c = DB_UNK;
  KEY_VAL_RANGE *curp, *nextp;

  curp = key_vals;
  nextp = key_vals + 1;
  n = 0;
  while (key_cnt > 1 && n < key_cnt - 1)
    {
      if (DB_IDXKEY_IS_NULL (&curp->lower_key))
	{
	  if (DB_IDXKEY_IS_NULL (&nextp->lower_key))
	    {
	      c = DB_EQ;
	    }
	  else
	    {
	      c = DB_LT;
	    }
	}
      else if (DB_IDXKEY_IS_NULL (&nextp->lower_key))
	{
	  c = DB_GT;
	}
      else
	{
	  c =
	    btree_compare_key (NULL, NULL, &curp->lower_key,
			       &nextp->lower_key, NULL);
	}

      assert (c != DB_UNK);

      if (c == DB_EQ)
	{
	  db_idxkey_clear (&nextp->lower_key);
	  db_idxkey_clear (&nextp->upper_key);
	  memmove (nextp, nextp + 1,
		   sizeof (KEY_VAL_RANGE) * (key_cnt - n - 2));
	  key_cnt--;
	}
      else
	{
	  curp++;
	  nextp++;
	  n++;
	}
    }

  return key_cnt;
}

/*
 * merge_key_ranges () - merge search key ranges
 *   return: number of keys, -1 for error
 *   key_vals (in): pointer to array of KEY_VAL_RANGE structure
 *   key_cnt (in): number of keys; size of key_vals
 */
static int
merge_key_ranges (KEY_VAL_RANGE * key_vals, int key_cnt)
{
  int cur_n, next_n;
  KEY_VAL_RANGE *curp, *nextp;
  ROP_TYPE cur_op1, cur_op2, next_op1, next_op2;
  ROP_TYPE cmp_1, cmp_2, cmp_3, cmp_4;
  bool is_mergeable;

  cmp_1 = cmp_2 = cmp_3 = cmp_4 = ROP_NA;

  curp = key_vals;
  cur_n = 0;
  while (key_cnt > 1 && cur_n < key_cnt - 1)
    {
      range_to_rop (&cur_op1, &cur_op2, curp->range);

      nextp = curp + 1;
      next_n = cur_n + 1;
      while (next_n < key_cnt)
	{
	  range_to_rop (&next_op1, &next_op2, nextp->range);

	  /* check if the two key ranges are mergable */
	  is_mergeable = true;	/* init */
	  cmp_1 = cmp_2 = cmp_3 = cmp_4 = ROP_NA;

	  if (is_mergeable == true)
	    {
	      cmp_1 =
		compare_val_op (&curp->upper_key, cur_op2,
				&nextp->lower_key, next_op1,
				curp->num_index_term);
	      if (cmp_1 == ROP_NA || cmp_1 == ROP_LT)
		{
		  is_mergeable = false;	/* error or disjoint */
		}
	    }

	  if (is_mergeable == true)
	    {
	      cmp_2 =
		compare_val_op (&curp->lower_key, cur_op1,
				&nextp->upper_key, next_op2,
				curp->num_index_term);
	      if (cmp_2 == ROP_NA || cmp_2 == ROP_GT)
		{
		  is_mergeable = false;	/* error or disjoint */
		}
	    }

	  if (is_mergeable == true)
	    {
	      /* determine the lower bound of the merged key range */
	      cmp_3 =
		compare_val_op (&curp->lower_key, cur_op1,
				&nextp->lower_key, next_op1,
				curp->num_index_term);
	      if (cmp_3 == ROP_NA)
		{
		  is_mergeable = false;
		}
	    }

	  if (is_mergeable == true)
	    {
	      /* determine the upper bound of the merged key range */
	      cmp_4 =
		compare_val_op (&curp->upper_key, cur_op2,
				&nextp->upper_key, next_op2,
				curp->num_index_term);
	      if (cmp_4 == ROP_NA)
		{
		  is_mergeable = false;
		}
	    }

	  if (is_mergeable == false)
	    {
	      /* they are disjoint */
	      nextp++;
	      next_n++;
	      continue;		/* skip and go ahead */
	    }

	  /* determine the lower bound of the merged key range */
	  if (cmp_3 == ROP_GT_ADJ || cmp_3 == ROP_GT)
	    {
	      db_idxkey_clear (&curp->lower_key);

	      curp->lower_key = nextp->lower_key;	/* bitwise copy */

	      DB_IDXKEY_MAKE_NULL (&nextp->lower_key);
	      cur_op1 = next_op1;
	    }
	  else
	    {
	      db_idxkey_clear (&nextp->lower_key);
	    }

	  /* determine the upper bound of the merged key range */
	  if (cmp_4 == ROP_LT || cmp_4 == ROP_LT_ADJ)
	    {
	      db_idxkey_clear (&curp->upper_key);

	      curp->upper_key = nextp->upper_key;	/* bitwise copy */

	      DB_IDXKEY_MAKE_NULL (&nextp->upper_key);
	      cur_op2 = next_op2;
	    }
	  else
	    {
	      db_idxkey_clear (&nextp->upper_key);
	    }

	  /* determine the new range type */
	  rop_to_range (&curp->range, cur_op1, cur_op2);
	  if (curp->range == INF_INF)
	    {
	      curp->num_index_term = 0;
	      db_idxkey_clear (&curp->lower_key);
	      db_idxkey_clear (&curp->upper_key);
	    }

	  /* remove merged one(nextp) */
	  memmove (nextp, nextp + 1,
		   sizeof (KEY_VAL_RANGE) * (key_cnt - next_n - 1));
	  key_cnt--;
	}

      curp++;
      cur_n++;
    }

  return key_cnt;
}

/*
 * check_key_vals () - check key values
 *   return: number of keys, -1 for error
 *   key_vals (in): pointer to array of KEY_VAL_RANGE structure
 *   key_cnt (in): number of keys; size of key_vals
 *   chk_fn (in): check function for key_vals
 */
static int
check_key_vals (KEY_VAL_RANGE * key_vals, int key_cnt,
		QPROC_KEY_VAL_FU * key_val_fn)
{
  if (key_cnt <= 1)
    {
      return key_cnt;
    }

  qsort ((void *) key_vals, key_cnt, sizeof (KEY_VAL_RANGE), key_val_compare);

  return ((*key_val_fn) (key_vals, key_cnt));
}

/*
 * scan_dbvals_to_idxkey () -
 *   return: NO_ERROR or ER_code
 *
 *   retval (out):
 *   disk_retval (out):
 *   disk_retval_new_domainp(out):
 *   indexal (out):
 *   indexp (in):
 *   num_term (in):
 *   func (in):
 *   pk_next_regu_list (in):
 *   vd (in):
 *   key_minmax (in):
 */
static int
scan_dbvals_to_idxkey (THREAD_ENTRY * thread_p, DB_IDXKEY * retval,
		       bool * indexable, OR_INDEX * indexp,
		       int num_term, REGU_VARIABLE * func,
		       REGU_VARIABLE_LIST pk_next_regu_list,
		       VAL_DESCR * vd, int key_minmax)
{
  int ret = NO_ERROR;
  DB_VALUE *val = NULL;
  DB_TYPE col_type, val_type;
  int common_coll = -1;
  UNUSED_VAR int idx_ncols = 0;
  int i;
  REGU_VARIABLE_LIST operand;
  TP_DOMAIN *idx_dom = NULL;

  assert (indexp != NULL);

  *indexable = false;

  /* init idxkey
   */

  idx_ncols = indexp->n_atts + 1;	/* append rightmost OID type */

#if !defined(NDEBUG)
  for (i = 0; i < indexp->n_atts; i++)
    {
      idx_dom = indexp->atts[i]->domain;
      assert (idx_dom != NULL);

      assert (tp_valid_indextype (indexp->atts[i]->type));
      assert (indexp->atts[i]->type == TP_DOMAIN_TYPE (idx_dom));

      if (idx_dom->precision < 0)
	{
	  assert (false);
	  return ER_FAILED;
	}
    }
#endif /* NDEBUG */

  assert (DB_IDXKEY_IS_NULL (retval));

  operand = func->value.funcp->operand;	/* is normal case */

#if 1				/* TODO - #508 primary key next clause */
  if (pk_next_regu_list != NULL)
    {
      /* check iff valid regu list */
      for (i = 0, operand = pk_next_regu_list; operand != NULL;
	   i++, operand = operand->next)
	{
	  assert (operand->value.type == TYPE_POS_VALUE);

	  ret = fetch_peek_dbval (thread_p, &(operand->value), vd, NULL,
				  NULL, &val);
	  if (ret != NO_ERROR)
	    {
	      goto err_exit;
	    }

	  if (DB_IS_NULL (val))
	    {
	      break;		/* give up */
	    }

	  if (i == 0)
	    {
	      /* check iff valid shard key type */
	      if (DB_VALUE_DOMAIN_TYPE (val) != DB_TYPE_VARCHAR)
		{
		  assert (false);	/* not permit */
		  ret = ER_FAILED;
		  goto err_exit;
		}
	    }
	}

      if (operand != NULL)
	{
	  operand = func->value.funcp->operand;	/* is normal case */
	}
      else
	{
	  operand = pk_next_regu_list;	/* reset; is primary key next clause */

	  num_term = i;		/* reset */
	}
    }
#endif

  assert (operand != NULL);

  for (i = 0; operand != NULL; operand = operand->next, i++)
    {
      idx_dom = indexp->atts[i]->domain;
      assert (idx_dom != NULL);

      ret = fetch_copy_dbval (thread_p, &(operand->value), vd, NULL,
			      NULL, &(retval->vals[i]));
      if (ret != NO_ERROR)
	{
	  goto err_exit;
	}

      retval->size++;

      val = &(retval->vals[i]);

      if (DB_IS_NULL (val))
	{
	  /* to fix multi-column index NULL problem */
	  goto end;
	}

      assert (!DB_IS_NULL (val));

      col_type = TP_DOMAIN_TYPE (idx_dom);
      val_type = DB_VALUE_DOMAIN_TYPE (val);

      /* check iff valid type */
      if (tp_valid_indextype (val_type) == false)
	{
	  ret = ER_TP_CANT_COERCE;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ret, 2,
		  pr_type_name (val_type), pr_type_name (col_type));

	  goto err_exit;
	}

      if (col_type != val_type || col_type == DB_TYPE_NUMERIC)
	{
	  /* string col_type only permit string val_type */
	  if (TP_IS_CHAR_TYPE (col_type) && !TP_IS_CHAR_TYPE (val_type))
	    {
	      ret = ER_TP_CANT_COERCE;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ret, 2,
		      pr_type_name (val_type), pr_type_name (col_type));

	      goto err_exit;
	    }

	  /* try coerce with col domain */
	  if (TP_IS_NUMERIC_TYPE (val_type)
	      || TP_IS_DATE_OR_TIME_TYPE (val_type))
	    {
	      /* Coerce the value to index domain  */
#if 1				/* TODO - at current, do not care ret value */
	      ret = tp_value_coerce_strict (val, &(retval->vals[i]), idx_dom);
#endif
	    }
	}
      else			/* (col_type == val_type && col_type != DB_TYPE_NUMERIC) */
	{
	  /* check collation */
	  if (TP_IS_CHAR_TYPE (col_type) && TP_IS_CHAR_TYPE (val_type))
	    {
	      LANG_RT_COMMON_COLL (TP_DOMAIN_COLLATION (idx_dom),
				   DB_GET_STRING_COLLATION (val),
				   common_coll);
	      if (common_coll == -1)
		{
		  ret = ER_QSTR_INCOMPATIBLE_COLLATIONS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ret, 0);

		  goto err_exit;
		}
	    }
	}

    }

  assert (retval->size <= num_term);

  *indexable = true;

  ret = btree_coerce_idxkey (retval, indexp, num_term, key_minmax);

  return ret;

end:

  db_idxkey_clear (retval);

  return ret;

err_exit:

  if (ret == NO_ERROR && (ret = er_errid ()) == NO_ERROR)
    {
      ret = ER_FAILED;
    }

  goto end;
}

/*
 * scan_regu_key_to_index_key:
 */
static int
scan_regu_key_to_index_key (THREAD_ENTRY * thread_p,
			    KEY_RANGE * key_ranges,
			    KEY_VAL_RANGE * key_val_range,
			    INDX_SCAN_ID * iscan_id, VAL_DESCR * vd)
{
  BTREE_SCAN *BTS = NULL;
  OR_INDEX *indexp = NULL;
  bool indexable = true;
  int key_minmax;
  int count;
  int ret = NO_ERROR;
  REGU_VARIABLE_LIST requ_list;
  DB_IDXKEY *lower_key, *upper_key;
  bool part_key_desc = false;

  assert ((key_ranges->range >= GE_LE && key_ranges->range <= INF_LT)
	  || (key_ranges->range == EQ_NA));
  assert (!(key_ranges->key1 == NULL && key_ranges->key2 == NULL));

  assert (iscan_id != NULL);

#if !defined(NDEBUG)		/* TODO - #508 primary key next clause */
  if (iscan_id->pk_next_regu_list != NULL)
    {
      assert (key_ranges->range == EQ_NA || key_ranges->range == GE_LE);
      if (key_ranges->range == EQ_NA)
	{
	  assert (iscan_id->indx_info->range_type == R_KEYLIST);
	  assert (key_ranges->key2 == NULL);
	}
      else
	{
	  assert (iscan_id->indx_info->range_type == R_RANGELIST);
	  assert (key_ranges->key2 != NULL);
	}

      assert (!iscan_id->indx_info->use_desc_index);
    }
#endif

  /* pointer to index scan info. structure */
  BTS = &(iscan_id->bt_scan);

  assert (BTS->btid_int.classrepr != NULL);
  assert (BTS->btid_int.classrepr_cache_idx != -1);
  assert (BTS->btid_int.indx_id != -1);
  indexp = &(BTS->btid_int.classrepr->indexes[BTS->btid_int.indx_id]);

  if (key_ranges->key1)
    {
      assert (key_ranges->key1->type == TYPE_FUNC);
      assert (key_ranges->key1->value.funcp->ftype == F_IDXKEY);

      if (key_ranges->key1->type == TYPE_FUNC
	  && key_ranges->key1->value.funcp->ftype == F_IDXKEY)
	{
	  for (requ_list = key_ranges->key1->value.funcp->operand, count = 0;
	       requ_list; requ_list = requ_list->next)
	    {
	      count++;
	    }
	}
      else
	{
	  assert (false);	/* dead-code */
	  count = 1;
	}

      assert (key_val_range->num_index_term == 0);
      key_val_range->num_index_term = count;
    }

  if (key_ranges->key2)
    {
      assert (key_ranges->key2->type == TYPE_FUNC);
      assert (key_ranges->key2->value.funcp->ftype == F_IDXKEY);

      if (key_ranges->key2->type == TYPE_FUNC
	  && key_ranges->key2->value.funcp->ftype == F_IDXKEY)
	{
	  for (requ_list = key_ranges->key2->value.funcp->operand, count = 0;
	       requ_list; requ_list = requ_list->next)
	    {
	      count++;
	    }
	}
      else
	{
	  assert (false);	/* dead-code */
	  assert_release (key_val_range->num_index_term <= 1);
	  count = 1;
	}

      assert ((key_ranges->key1 != NULL && key_val_range->num_index_term > 0)
	      || key_val_range->num_index_term == 0);
      key_val_range->num_index_term = MAX (key_val_range->num_index_term,
					   count);
    }

  assert (key_val_range->range != INF_INF);
  assert (key_val_range->num_index_term > 0);

#if 1				/* TODO - #508 primary key next clause */
  if (iscan_id->pk_next_regu_list != NULL)
    {
      assert (key_val_range->num_index_term == 1);
      assert (!iscan_id->indx_info->use_desc_index);

      /* the last partial-key domain is desc */
      part_key_desc =
	indexp->asc_desc[key_val_range->num_index_term - 1] ? true : false;
    }
#endif

  lower_key = &(key_val_range->lower_key);
  upper_key = &(key_val_range->upper_key);

  if (key_ranges->key1)
    {
      assert (key_ranges->key1->type == TYPE_FUNC);
      assert (key_ranges->key1->value.funcp->ftype == F_IDXKEY);

      if (key_val_range->range == GT_INF || key_val_range->range == GT_LE
	  || key_val_range->range == GT_LT)
	{
	  key_minmax = BTREE_COERCE_KEY_WITH_MAX_VALUE;
	}
      else
	{
	  key_minmax = BTREE_COERCE_KEY_WITH_MIN_VALUE;
	}

      ret = scan_dbvals_to_idxkey (thread_p, lower_key,
				   &indexable, indexp,
				   key_val_range->num_index_term,
				   key_ranges->key1,
				   ((iscan_id->pk_next_regu_list != NULL
				     && !part_key_desc) ?
				    iscan_id->pk_next_regu_list : NULL),
				   vd, key_minmax);

      if (ret != NO_ERROR || indexable == false)
	{
	  key_val_range->range = NA_NA;

	  return ret;
	}
    }
  else
    {
      if (key_ranges->key2 == NULL)
	{
	  /* impossible case */
	  assert (false);

	  key_val_range->range = NA_NA;

	  return ER_FAILED;
	}

      assert (key_ranges->key2->type == TYPE_FUNC);
      assert (key_ranges->key2->value.funcp->ftype == F_IDXKEY);

      key_minmax = BTREE_COERCE_KEY_WITH_MIN_VALUE;

      assert (key_val_range->range == INF_LE
	      || key_val_range->range == INF_LT);

      ret = btree_coerce_idxkey (lower_key, indexp,
				 key_val_range->num_index_term, key_minmax);
    }

  if (key_ranges->key2)
    {
      assert (key_ranges->key2->type == TYPE_FUNC);
      assert (key_ranges->key2->value.funcp->ftype == F_IDXKEY);

      if (key_val_range->range == INF_LT || key_val_range->range == GE_LT
	  || key_val_range->range == GT_LT)
	{
	  key_minmax = BTREE_COERCE_KEY_WITH_MIN_VALUE;
	}
      else
	{
	  key_minmax = BTREE_COERCE_KEY_WITH_MAX_VALUE;
	}

      ret = scan_dbvals_to_idxkey (thread_p, upper_key,
				   &indexable, indexp,
				   key_val_range->num_index_term,
				   key_ranges->key2,
				   ((iscan_id->pk_next_regu_list != NULL
				     && part_key_desc) ?
				    iscan_id->pk_next_regu_list : NULL),
				   vd, key_minmax);

      if (ret != NO_ERROR || indexable == false)
	{
	  key_val_range->range = NA_NA;

	  return ret;
	}
    }
  else
    {
      if (key_ranges->key1 == NULL)
	{
	  /* impossible case */
	  assert (false);

	  key_val_range->range = NA_NA;

	  return ER_FAILED;
	}

      assert (key_ranges->key1->type == TYPE_FUNC);
      assert (key_ranges->key1->value.funcp->ftype == F_IDXKEY);

      key_minmax = BTREE_COERCE_KEY_WITH_MAX_VALUE;

      if (key_val_range->range == EQ_NA)
	{
	  ret = scan_dbvals_to_idxkey (thread_p, upper_key,
				       &indexable, indexp,
				       key_val_range->num_index_term,
				       key_ranges->key1,
				       ((iscan_id->pk_next_regu_list != NULL
					 && part_key_desc) ?
					iscan_id->pk_next_regu_list : NULL),
				       vd, key_minmax);
	}
      else
	{
	  assert (key_val_range->range == GE_INF
		  || key_val_range->range == GT_INF);

	  ret = btree_coerce_idxkey (upper_key, indexp,
				     key_val_range->num_index_term,
				     key_minmax);
	}
    }

  if (key_val_range->range == EQ_NA)
    {
      key_val_range->range = GE_LE;
    }

  switch (iscan_id->indx_info->range_type)
    {
    case R_KEYLIST:
      {
	/* When key received as NULL, currently this is assumed an UNBOUND
	   value and no object value in the index is equal to NULL value in
	   the index scan context. They can be equal to NULL only in the
	   "is NULL" context. */

	/* to fix multi-column index NULL problem */
	if (DB_IDXKEY_IS_NULL (lower_key))
	  {
	    key_val_range->range = NA_NA;

	    return ret;
	  }
	break;
      }

    case R_RANGELIST:
      {
	/* When key received as NULL, currently this is assumed an UNBOUND
	   value and no object value in the index is equal to NULL value in
	   the index scan context. They can be equal to NULL only in the
	   "is NULL" context. */
	if (key_val_range->range >= GE_LE && key_val_range->range <= GT_LT)
	  {
	    /* to fix multi-column index NULL problem */
	    if (DB_IDXKEY_IS_NULL (lower_key)
		|| DB_IDXKEY_IS_NULL (upper_key))
	      {
		key_val_range->range = NA_NA;

		return ret;
	      }
	    else
	      {
		int c = DB_UNK;

		c = scan_key_compare (lower_key, upper_key,
				      key_val_range->num_index_term);

		if (c == DB_UNK)
		  {
		    /* invalid type lower/upper keys
		     * ex: select ...
		     *     from ...
		     *     where int_col between 1 and 'a';
		     */

		    key_val_range->range = NA_NA;

		    return ER_FAILED;
		  }
		else if (c > 0)
		  {
		    key_val_range->range = NA_NA;

		    return ret;
		  }
	      }
	  }
	else if (key_val_range->range >= GE_INF
		 && key_val_range->range <= GT_INF)
	  {
	    /* to fix multi-column index NULL problem */
	    if (DB_IDXKEY_IS_NULL (lower_key))
	      {
		key_val_range->range = NA_NA;

		return ret;
	      }
	  }
	else if (key_val_range->range >= INF_LE
		 && key_val_range->range <= INF_LT)
	  {
	    /* to fix multi-column index NULL problem */
	    if (DB_IDXKEY_IS_NULL (upper_key))
	      {
		key_val_range->range = NA_NA;

		return ret;
	      }
	  }
	break;
      }
    default:
      assert_release (false);
      break;			/* impossible case */
    }

  return ret;
}

/*
 * scan_get_index_oidset () - Fetch the next group of set of object identifiers
 * from the index associated with the scan identifier.
 *   return: NO_ERROR, or ER_code
 *   s_id(in): Scan identifier
 *
 * Note: If you feel the need
 */
static int
scan_get_index_oidset (THREAD_ENTRY * thread_p, SCAN_ID * s_id)
{
  INDX_SCAN_ID *iscan_id = NULL;
  FILTER_INFO key_filter;
  INDX_INFO *indx_infop = NULL;
  BTREE_SCAN *BTS = NULL;
  OR_INDEX *indexp = NULL;
  int key_cnt, i;
  KEY_VAL_RANGE *key_vals;
  KEY_RANGE *key_ranges;
  RANGE range, saved_range;
  int ret = NO_ERROR;
  int n;
  bool part_key_desc = false;
  DB_IDXKEY tmp_key;

  /* pointer to INDX_SCAN_ID structure */
  iscan_id = &(s_id->s.isid);

  /* pointer to INDX_INFO in INDX_SCAN_ID structure */
  indx_infop = iscan_id->indx_info;

  /* pointer to index scan info. structure */
  BTS = &(iscan_id->bt_scan);

  assert (BTS->btid_int.classrepr != NULL);
  assert (BTS->btid_int.classrepr_cache_idx != -1);
  assert (BTS->btid_int.indx_id != -1);

  indexp = &(BTS->btid_int.classrepr->indexes[BTS->btid_int.indx_id]);

  /* number of keys */
  if (iscan_id->curr_keyno == -1)	/* very first time */
    {
      key_cnt = indx_infop->key_info.key_cnt;
    }
  else
    {
      key_cnt = iscan_id->key_cnt;
    }

  /* key values */
  key_vals = iscan_id->key_vals;

  /* key ranges */
  key_ranges = indx_infop->key_info.key_ranges;

  if (key_cnt < 1 || !key_vals || !key_ranges)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
      return ER_FAILED;
    }

  /* if it is the first time of this scan */
  if (iscan_id->curr_keyno == -1 && indx_infop->key_info.key_cnt == key_cnt)
    {
      /* make DB_VALUE key values from KEY_VALS key ranges */
      for (i = 0; i < key_cnt; i++)
	{
	  /* initialize DB_VALUE first for error case */
	  key_vals[i].range = NA_NA;

	  DB_IDXKEY_MAKE_NULL (&key_vals[i].lower_key);
	  DB_IDXKEY_MAKE_NULL (&key_vals[i].upper_key);

	  key_vals[i].num_index_term = 0;
	}

      for (i = 0; i < key_cnt; i++)
	{
	  key_vals[i].range = key_ranges[i].range;
	  if (key_vals[i].range == INF_INF)
	    {
	      continue;
	    }

	  ret = scan_regu_key_to_index_key (thread_p, &key_ranges[i],
					    &key_vals[i], iscan_id, s_id->vd);

	  if (ret != NO_ERROR)
	    {
	      goto exit_on_error;
	    }

	  assert_release (key_vals[i].num_index_term > 0);
	}

      /* eliminating duplicated keys and merging ranges are required even
         though the query optimizer does them because the search keys or
         ranges could be unbound values at optimization step such as join
         attribute */
      if (indx_infop->range_type == R_KEYLIST)
	{
	  /* eliminate duplicated keys in the search key list */
	  key_cnt = iscan_id->key_cnt = check_key_vals (key_vals, key_cnt,
							eliminate_duplicated_keys);
	}
      else if (indx_infop->range_type == R_RANGELIST)
	{
	  /* merge search key ranges */
	  key_cnt = iscan_id->key_cnt = check_key_vals (key_vals, key_cnt,
							merge_key_ranges);
	}

      /* if is order by skip and first column is descending, the order will
       * be reversed so reverse the key ranges to be desc.
       */
      if ((indx_infop->range_type == R_KEYLIST
	   || indx_infop->range_type == R_RANGELIST)
	  && ((indx_infop->orderby_desc && indx_infop->orderby_skip)
	      || (indx_infop->groupby_desc && indx_infop->groupby_skip)))
	{
	  /* in both cases we should reverse the key lists if we have a
	   * reverse order by or group by which is skipped
	   */
	  check_key_vals (key_vals, key_cnt, reverse_key_list);
	}

      if (key_cnt < 0)
	{
	  goto exit_on_error;
	}

      /* check iff need to swap lower value and upper value
       */
      for (i = 0; i < key_cnt; i++)
	{
	  if (key_vals[i].range == INF_INF)
	    {
	      continue;
	    }

	  assert_release (key_vals[i].num_index_term > 0);

	  /* the last partial-key domain is desc */
	  part_key_desc =
	    indexp->asc_desc[key_vals[i].num_index_term - 1] ? true : false;

	  /* if (scan_asc && key_desc) || (scan_desc && key_asc),
	   * then swap lower value and upper value
	   */
	  if ((!iscan_id->indx_info->use_desc_index && part_key_desc)
	      || (iscan_id->indx_info->use_desc_index && !part_key_desc))
	    {
	      tmp_key = key_vals[i].lower_key;
	      key_vals[i].lower_key = key_vals[i].upper_key;
	      key_vals[i].upper_key = tmp_key;

	      switch (key_vals[i].range)
		{
		case GT_LE:
		  {
		    key_vals[i].range = GE_LT;
		    break;
		  }
		case GE_LT:
		  {
		    key_vals[i].range = GT_LE;
		    break;
		  }
		case GE_INF:
		  {
		    key_vals[i].range = INF_LE;
		    break;
		  }
		case INF_LE:
		  {
		    key_vals[i].range = GE_INF;
		    break;
		  }
		case GT_INF:
		  {
		    key_vals[i].range = INF_LT;
		    break;
		  }
		case INF_LT:
		  {
		    key_vals[i].range = GT_INF;
		    break;
		  }
		default:
		  break;
		}
	    }
	}			/* for (i = 0; ...) */

      iscan_id->curr_keyno = 0;
    }

  /*
   * init vars to execute B+tree key range search
   */

  ret = NO_ERROR;

  /* set key filter information */
  scan_init_filter_info (&key_filter, &iscan_id->key_pred,
			 &iscan_id->key_attrs, s_id->val_list, s_id->vd,
			 &(BTS->btid_int.cls_oid),
			 BTS->btid_int.classrepr, BTS->btid_int.indx_id);
  iscan_id->oid_list.oid_cnt = 0;

  if (iscan_id->multi_range_opt.use && iscan_id->multi_range_opt.cnt > 0)
    {
      /* reset any previous results for multiple range optimization */
      int i;

      for (i = 0; i < iscan_id->multi_range_opt.cnt; i++)
	{
	  if (iscan_id->multi_range_opt.top_n_items[i] != NULL)
	    {
	      RANGE_OPT_ITEM *item;

	      item = iscan_id->multi_range_opt.top_n_items[i];

	      db_idxkey_clear (&(item->index_value));
	      free_and_init (iscan_id->multi_range_opt.top_n_items[i]);
	    }
	}

      iscan_id->multi_range_opt.cnt = 0;
    }

  /* if the end of this scan */
  if (iscan_id->curr_keyno > key_cnt)
    {
      assert (ret == NO_ERROR);
      goto end;
    }

  switch (indx_infop->range_type)
    {
    case R_KEYLIST:
      /* multiple key value search */

      /* for each key value */
      while (iscan_id->curr_keyno < key_cnt)
	{
	  /* check prerequisite condition */
	  range = key_vals[iscan_id->curr_keyno].range;

	  if (range == NA_NA)
	    {
	      /* skip this key value and continue to the next */
	      iscan_id->curr_keyno++;
	      if (iscan_id->curr_keyno < key_cnt
		  && iscan_id->key_limit_upper != -1
		  && iscan_id->key_limit_lower == -1
		  && indx_infop->key_info.key_limit_reset)
		{
		  if (scan_init_index_key_limit (thread_p, iscan_id,
						 &indx_infop->key_info,
						 s_id->vd) != NO_ERROR)
		    {
		      goto exit_on_error;
		    }
		}
	      continue;
	    }

	  n = btree_range_search (thread_p,
				  &indx_infop->indx_id.i.btid,
				  &key_vals[iscan_id->curr_keyno],
				  &key_filter, iscan_id);
	  if (n < 0)
	    {
#if 1				/* TODO - */
	      assert (n == ER_TP_CANT_COERCE || n == ER_INTERRUPTED);
#endif
	      iscan_id->oid_list.oid_cnt = -1;
	      goto exit_on_error;
	    }
	  iscan_id->oid_list.oid_cnt = n;

	  /* We only want to advance the key ptr if we've exhausted the
	     current crop of oids on the current key. */
	  if (BTREE_END_OF_SCAN (BTS))
	    {
	      iscan_id->curr_keyno++;
	      /* reset upper key limit, if flag is set */
	      if (iscan_id->curr_keyno < key_cnt
		  && iscan_id->key_limit_upper != -1
		  && iscan_id->key_limit_lower == -1
		  && indx_infop->key_info.key_limit_reset)
		{
		  if (scan_init_index_key_limit (thread_p, iscan_id,
						 &indx_infop->key_info,
						 s_id->vd) != NO_ERROR)
		    {
		      goto exit_on_error;
		    }
		}
	    }

	  if (iscan_id->multi_range_opt.use)
	    {
	      /* with multiple range optimization, we store the only the top N
	       * OIDS or index keys: the only valid exit condition from
	       * 'btree_range_search' is when the index scan has reached the end
	       * for this key */
	      if (!BTREE_END_OF_SCAN (BTS))
		{
		  assert (false);
		  goto exit_on_error;
		}
	      /* continue loop : exhaust all keys in one shot when in
	       * multiple range search optimization mode */
	      continue;
	    }
	  if (iscan_id->oid_list.oid_cnt > 0)
	    {
	      /* we've got some result */
	      break;
	    }
	}

      break;

    case R_RANGELIST:
      /* multiple range search */

      /* for each key value */
      while (iscan_id->curr_keyno < key_cnt)
	{
	  /* check prerequisite condition */
	  saved_range = range = key_vals[iscan_id->curr_keyno].range;

	  if (range == NA_NA)
	    {
	      /* skip this key value and continue to the next */
	      iscan_id->curr_keyno++;
	      if (iscan_id->curr_keyno < key_cnt
		  && iscan_id->key_limit_upper != -1
		  && iscan_id->key_limit_lower == -1
		  && indx_infop->key_info.key_limit_reset)
		{
		  if (scan_init_index_key_limit (thread_p, iscan_id,
						 &indx_infop->key_info,
						 s_id->vd) != NO_ERROR)
		    {
		      goto exit_on_error;
		    }
		}
	      continue;
	    }

	  if (range < GE_LE || range > INF_INF)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_QPROC_INVALID_XASLNODE, 0);
	      goto exit_on_error;
	    }

	  if (range == INF_INF)
	    {
	      if (key_cnt != 1)
		{
		  assert_release (0);
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_QPROC_INVALID_XASLNODE, 0);
		  goto exit_on_error;
		}

	      /* if we reached the key count limit, break */
	      if (iscan_id->curr_keyno >= key_cnt)
		{
		  iscan_id->curr_keyno++;
		  break;
		}

	      assert (iscan_id->curr_keyno == 0);
	      db_idxkey_clear (&key_vals[0].lower_key);
	      db_idxkey_clear (&key_vals[0].upper_key);

	      assert_release (key_vals[0].num_index_term == 0);
	    }

	  key_vals[iscan_id->curr_keyno].range = range;
	  n = btree_range_search (thread_p,
				  &indx_infop->indx_id.i.btid,
				  &key_vals[iscan_id->curr_keyno],
				  &key_filter, iscan_id);
	  key_vals[iscan_id->curr_keyno].range = saved_range;
	  if (n < 0)
	    {
#if 1				/* TODO - */
	      assert (n == ER_TP_CANT_COERCE || n == ER_INTERRUPTED);
#endif
	      iscan_id->oid_list.oid_cnt = -1;
	      goto exit_on_error;
	    }
	  iscan_id->oid_list.oid_cnt = n;

	  /* We only want to advance the key ptr if we've exhausted the
	     current crop of oids on the current key. */
	  if (BTREE_END_OF_SCAN (BTS))
	    {
	      iscan_id->curr_keyno++;
	      /* reset upper key limit, if flag is set */
	      if (iscan_id->curr_keyno < key_cnt
		  && iscan_id->key_limit_upper != -1
		  && iscan_id->key_limit_lower == -1
		  && indx_infop->key_info.key_limit_reset)
		{
		  if (scan_init_index_key_limit (thread_p, iscan_id,
						 &indx_infop->key_info,
						 s_id->vd) != NO_ERROR)
		    {
		      goto exit_on_error;
		    }
		}
	    }

	  if (iscan_id->multi_range_opt.use)
	    {
	      /* with multiple range optimization, we store the only the top N
	       * OIDS or index keys: the only valid exit condition from
	       * 'btree_range_search' is when the index scan has reached the end
	       * for this key */
	      assert (BTREE_END_OF_SCAN (BTS));
	      /* continue loop : exhaust all keys in one shot when in
	       * multiple range search optimization mode */
	      continue;
	    }
	  if (iscan_id->oid_list.oid_cnt > 0)
	    {
	      /* we've got some result */
	      break;
	    }
	}

      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
      goto exit_on_error;
    }

end:
  /* if the end of this scan */
  if (iscan_id->curr_keyno >= key_cnt)
    {
      for (i = 0; i < key_cnt; i++)
	{
	  db_idxkey_clear (&key_vals[i].lower_key);
	  db_idxkey_clear (&key_vals[i].upper_key);
	}

      iscan_id->curr_keyno++;	/* to prevent duplicate frees */
    }

  if (thread_is_on_trace (thread_p))
    {
      s_id->stats.read_keys += iscan_id->bt_scan.read_keys;
      iscan_id->bt_scan.read_keys = 0;
      s_id->stats.qualified_keys += iscan_id->bt_scan.qualified_keys;
      iscan_id->bt_scan.qualified_keys = 0;
    }

  return ret;

exit_on_error:
  iscan_id->curr_keyno = key_cnt;	/* set as end of this scan */

  ret = (ret == NO_ERROR
	 && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;
  goto end;
}

/*
 *
 *                    SCAN MANAGEMENT ROUTINES
 *
 */

/*
 * scan_init_scan_id () -
 *   return:
 *   scan_id(out): Scan identifier
 *   scan_op_type(in): scan operation type
 *   fixed(in):
 *   fetch_type(in):
 *   val_list(in):
 *   vd(in):
 *
 * Note: If you feel the need
 */
static void
scan_init_scan_id (SCAN_ID * scan_id, int fixed,
		   QPROC_FETCH_TYPE fetch_type,
		   VAL_LIST * val_list, VAL_DESCR * vd)
{
  scan_id->status = S_OPENED;
  scan_id->position = S_BEFORE;

  scan_id->fixed = fixed;

  scan_id->qualified_block = false;
  scan_id->fetch_type = fetch_type;
  scan_id->single_fetched = false;
  scan_id->null_fetched = false;
  scan_id->qualification = QPROC_QUALIFIED;

  /* value list and descriptor */
  scan_id->val_list = val_list;	/* points to the XASL tree */
  scan_id->vd = vd;		/* set value descriptor pointer */
  scan_id->scan_immediately_stop = false;
}

/*
 * scan_open_heap_scan () -
 *   return: NO_ERROR
 *   scan_id(out): Scan identifier
 *   readonly_scan(in):
 *   scan_op_type(in): scan operation type
 *   fixed(in):
 *   lock_hint(in):
 *   fetch_type(in):
 *   val_list(in):
 *   vd(in):
 *   cls_oid(in):
 *   hfid(in):
 *   regu_list_pred(in):
 *   pr(in):
 *   regu_list_rest(in):
 *   num_attrs_pred(in):
 *   attrids_pred(in):
 *   cache_pred(in):
 *   num_attrs_rest(in):
 *   attrids_rest(in):
 *   cache_rest(in):
 *
 * Note: If you feel the need
 */
int
scan_open_heap_scan (THREAD_ENTRY * thread_p, SCAN_ID * scan_id,
		     /* fields of SCAN_ID */
		     UNUSED_ARG int readonly_scan,
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
		     ATTR_ID * attrids_rest, HEAP_CACHE_ATTRINFO * cache_rest)
{
  HEAP_SCAN_ID *hsidp;

  if (hfid->vfid.fileid < 0)
    {
      assert (false);
      return ER_FAILED;
    }

  /* scan type is HEAP SCAN */
  scan_id->type = S_HEAP_SCAN;

  /* initialize SCAN_ID structure */
  scan_init_scan_id (scan_id, fixed, fetch_type, val_list, vd);

  /* initialize HEAP_SCAN_ID structure */
  hsidp = &scan_id->s.hsid;

  /* class object OID */
  COPY_OID (&hsidp->cls_oid, cls_oid);

  /* heap file identifier */
  hsidp->hfid = *hfid;		/* bitwise copy */

  /* OID within the heap */
  UT_CAST_TO_NULL_HEAP_OID (&hsidp->hfid, &hsidp->curr_oid);

  /* scan predicates */
  if (scan_init_scan_pred (&hsidp->scan_pred, regu_list_pred, pr,
			   ((pr) ? eval_fnc (thread_p, pr) : NULL)) !=
      NO_ERROR)
    {
      return ER_FAILED;
    }

  /* attribute information from predicates */
  scan_init_scan_attrs (&hsidp->pred_attrs, num_attrs_pred, attrids_pred,
			cache_pred);

  /* regulator vairable list for other than predicates */
  hsidp->rest_regu_list = regu_list_rest;

  /* attribute information from other than predicates */
  scan_init_scan_attrs (&hsidp->rest_attrs, num_attrs_rest, attrids_rest,
			cache_rest);

  /* flags */
  /* do not reset hsidp->caches_inited here */
  hsidp->scancache_inited = false;

  return NO_ERROR;
}

/*
 * scan_open_index_scan () -
 *   return: NO_ERROR, or ER_code
 *   scan_id(out): Scan identifier
 *   readonly_scan(in):
 *   fixed(in):
 *   fetch_type(in):
 *   val_list(in):
 *   vd(in):
 *   indx_info(in):
 *   cls_oid(in):
 *   hfid(in):
 *   regu_list_key(in):
 *   pr_key(in):
 *   regu_list_pred(in):
 *   pr(in):
 *   regu_list_rest(in):
 *   regu_list_pk_next(in):
 *   num_attrs_key(in):
 *   attrids_key(in):
 *   num_attrs_pred(in):
 *   attrids_pred(in):
 *   cache_pred(in):
 *   num_attrs_rest(in):
 *   attrids_rest(in):
 *   cache_rest(in):
 *
 * Note: If you feel the need
 */
int
scan_open_index_scan (THREAD_ENTRY * thread_p, SCAN_ID * scan_id,
		      /* fields of SCAN_ID */
		      UNUSED_ARG int readonly_scan,
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
		      HEAP_CACHE_ATTRINFO * cache_rest, QUERY_ID query_id)
{
  int ret = NO_ERROR;
  INDX_SCAN_ID *isidp;
  BTID *btid;
  BTREE_SCAN *BTS = NULL;
  int coverage_enabled;

  if (hfid->vfid.fileid < 0)
    {
      assert (false);
      return ER_FAILED;
    }

  /* scan type is INDEX SCAN */
  scan_id->type = S_INDX_SCAN;

  /* initialize SCAN_ID structure */
  scan_init_scan_id (scan_id, fixed, fetch_type, val_list, vd);

  btid = &indx_info->indx_id.i.btid;

  /* initialize INDEX_SCAN_ID structure */
  isidp = &(scan_id->s.isid);
  BTS = &(isidp->bt_scan);

  /* index information */
  isidp->indx_info = indx_info;

  /* init alloced fields */
  isidp->oid_list.oidp = NULL;

  isidp->key_vals = NULL;

  /* initialize key limits */
  if (scan_init_index_key_limit (thread_p, isidp, &indx_info->key_info, vd) !=
      NO_ERROR)
    {
      goto exit_on_error;
    }

  /* index scan info */
  BTREE_INIT_SCAN (BTS);

  /* construct BTID_INT structure */
  BTREE_INIT_BTID_INT (&(BTS->btid_int));

  BTS->btid_int.sys_btid = btid;

  /* get class representation of the index */
  COPY_OID (&(BTS->btid_int.cls_oid), cls_oid);
  BTS->btid_int.classrepr =
    heap_classrepr_get (thread_p, &(BTS->btid_int.cls_oid), NULL, 0,
			&(BTS->btid_int.classrepr_cache_idx), true);
  if (BTS->btid_int.classrepr == NULL)
    {
      assert (false);
      goto exit_on_error;
    }

  /* get the index ID which corresponds to the BTID */
  BTS->btid_int.indx_id =
    heap_classrepr_find_index_id (BTS->btid_int.classrepr,
				  BTS->btid_int.sys_btid);
  if (BTS->btid_int.indx_id < 0)
    {
      goto exit_on_error;
    }

  /* indicator whether covering index is used or not */
  coverage_enabled = (indx_info->coverage != 0);

  /* is a single range? */
  isidp->one_range = false;

  /* initial values */
  isidp->curr_keyno = -1;
  isidp->curr_oidno = -1;

  /* OID buffer */
  isidp->oid_list.oid_cnt = 0;
  isidp->oid_list.oid_buf_size = 0;
  if (coverage_enabled)
    {
      /* Covering index do not use an oid buffer. */
      isidp->oid_list.oidp = NULL;
      scan_id->stats.covered_index = true;
    }
  else
    {
      isidp->oid_list.oidp = scan_alloc_iscan_oid_buf_list ();
      if (isidp->oid_list.oidp == NULL)
	{
	  goto exit_on_error;
	}
      isidp->oid_list.oid_buf_size = ISCAN_OID_BUFFER_SIZE;
    }
  isidp->curr_oidp = isidp->oid_list.oidp;

  /* heap file identifier */
  isidp->hfid = *hfid;		/* bitwise copy */

  /* key filter */
  if (scan_init_scan_pred (&isidp->key_pred, regu_list_key, pr_key,
			   ((pr_key) ? eval_fnc (thread_p, pr_key) : NULL)) !=
      NO_ERROR)
    {
      goto exit_on_error;
    }

  /* attribute information from key filter */
  scan_init_scan_attrs (&isidp->key_attrs, num_attrs_key, attrids_key,
			cache_key);

  /* scan predicates */
  if (scan_init_scan_pred (&isidp->scan_pred, regu_list_pred, pr,
			   ((pr) ? eval_fnc (thread_p, pr) : NULL)) !=
      NO_ERROR)
    {
      goto exit_on_error;
    }

  /* attribute information from predicates */
  scan_init_scan_attrs (&isidp->pred_attrs, num_attrs_pred, attrids_pred,
			cache_pred);

  /* regulator vairable list for other than predicates */
  isidp->rest_regu_list = regu_list_rest;

  /* regulator vairable list for primary key next host_var */
  isidp->pk_next_regu_list = regu_list_pk_next;

  /* attribute information from other than predicates */
  scan_init_scan_attrs (&isidp->rest_attrs, num_attrs_rest, attrids_rest,
			cache_rest);

  /* flags */
  /* do not reset hsidp->caches_inited here */
  isidp->scancache_inited = false;

  /* convert key values in the form of REGU_VARIABLE to the form of DB_VALUE */
  isidp->key_cnt = indx_info->key_info.key_cnt;
  if (isidp->key_cnt > 0)
    {
      isidp->key_vals =
	(KEY_VAL_RANGE *) malloc (isidp->key_cnt * sizeof (KEY_VAL_RANGE));
      if (isidp->key_vals == NULL)
	{
	  goto exit_on_error;
	}
    }
  else
    {
      isidp->key_cnt = 0;
      isidp->key_vals = NULL;
    }

  if (scan_init_indx_coverage (thread_p, coverage_enabled, output_val_list,
			       regu_val_list, vd, query_id,
			       &(isidp->indx_cov)) != NO_ERROR)
    {
      goto exit_on_error;
    }

  assert (BTS->btid_int.classrepr != NULL);
  assert (BTS->btid_int.classrepr_cache_idx != -1);
  assert (BTS->btid_int.indx_id != -1);

  /* initialize multiple range search optimization structure */
  {
    bool use_multi_range_opt =
      (BTS->btid_int.classrepr->indexes[BTS->btid_int.indx_id].n_atts > 1
       && isidp->indx_info->key_info.key_limit_reset == 1
       && isidp->key_limit_upper > 0
       && isidp->key_limit_upper < DB_INT32_MAX
       && isidp->key_limit_lower == -1) ? true : false;

    if (scan_init_multi_range_optimization (thread_p,
					    &(isidp->multi_range_opt),
					    use_multi_range_opt,
					    (int) isidp->key_limit_upper) !=
	NO_ERROR)
      {
	goto exit_on_error;
      }

    scan_id->stats.multi_range_opt = isidp->multi_range_opt.use;
  }

  return ret;

exit_on_error:

#if 1				/* TODO - */
  if (BTS->btid_int.classrepr)
    {
      assert (BTS->btid_int.classrepr_cache_idx != -1);
      assert (BTS->btid_int.indx_id != -1);

      (void) heap_classrepr_free (BTS->btid_int.classrepr,
				  &(BTS->btid_int.classrepr_cache_idx));
      assert (BTS->btid_int.classrepr_cache_idx == -1);

      BTS->btid_int.classrepr = NULL;
//          BTS->btid_int.classrepr_cache_idx = -1;
//          BTS->btid_int.indx_id = -1;
    }
#endif

  if (isidp->key_vals)
    {
      free_and_init (isidp->key_vals);
    }

  /* free allocated memory for the scan */
  if (isidp->oid_list.oidp)
    {
      scan_free_iscan_oid_buf_list (isidp->oid_list.oidp);
      isidp->oid_list.oidp = NULL;
      isidp->oid_list.oid_buf_size = 0;
    }
  if (isidp->indx_cov.type_list != NULL)
    {
      if (isidp->indx_cov.type_list->domp != NULL)
	{
	  free_and_init (isidp->indx_cov.type_list->domp);
	}
      free_and_init (isidp->indx_cov.type_list);
    }
  if (isidp->indx_cov.list_id != NULL)
    {
      QFILE_FREE_AND_INIT_LIST_ID (isidp->indx_cov.list_id);
    }
  if (isidp->indx_cov.tplrec != NULL)
    {
      if (isidp->indx_cov.tplrec->tpl != NULL)
	{
	  free_and_init (isidp->indx_cov.tplrec->tpl);
	}
      free_and_init (isidp->indx_cov.tplrec);
    }
  if (isidp->indx_cov.lsid != NULL)
    {
      free_and_init (isidp->indx_cov.lsid);
    }

  return (ret == NO_ERROR
	  && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;
}

/*
 * scan_open_list_scan () -
 *   return: NO_ERROR
 *   scan_id(out): Scan identifier
 *   fetch_type(in):
 *   val_list(in):
 *   vd(in):
 *   list_id(in):
 *   regu_list_pred(in):
 *   pr(in):
 *   regu_list_rest(in):
 */
int
scan_open_list_scan (THREAD_ENTRY * thread_p, SCAN_ID * scan_id,
		     /* fields of SCAN_ID */
		     QPROC_FETCH_TYPE fetch_type,
		     VAL_LIST * val_list, VAL_DESCR * vd,
		     /* fields of LLIST_SCAN_ID */
		     QFILE_LIST_ID * list_id,
		     REGU_VARIABLE_LIST regu_list_pred,
		     PRED_EXPR * pr, REGU_VARIABLE_LIST regu_list_rest)
{
  LLIST_SCAN_ID *llsidp;

  /* scan type is LIST SCAN */
  scan_id->type = S_LIST_SCAN;

  /* initialize SCAN_ID structure */
  /* fixed = true */
  scan_init_scan_id (scan_id, true, fetch_type, val_list, vd);

  /* initialize LLIST_SCAN_ID structure */
  llsidp = &scan_id->s.llsid;

  /* list file ID */
  llsidp->list_id = list_id;	/* points to XASL tree */

  /* scan predicates */
  if (scan_init_scan_pred (&llsidp->scan_pred, regu_list_pred, pr,
			   ((pr) ? eval_fnc (thread_p, pr) : NULL)) !=
      NO_ERROR)
    {
      return ER_FAILED;
    }

  /* regulator vairable list for other than predicates */
  llsidp->rest_regu_list = regu_list_rest;

  return NO_ERROR;
}

/*
 * scan_start_scan () - Start the scan process on the given scan identifier.
 *   return: NO_ERROR, or ER_code
 *   scan_id(out): Scan identifier
 *
 * Note: If you feel the need
 */
int
scan_start_scan (THREAD_ENTRY * thread_p, SCAN_ID * scan_id)
{
  int ret = NO_ERROR;
  HEAP_SCAN_ID *hsidp;
  INDX_SCAN_ID *isidp;
  LLIST_SCAN_ID *llsidp;


  switch (scan_id->type)
    {

    case S_HEAP_SCAN:

      hsidp = &scan_id->s.hsid;
      UT_CAST_TO_NULL_HEAP_OID (&hsidp->hfid, &hsidp->curr_oid);

      /* A new argument(is_indexscan = false) is appended */
      ret = heap_scancache_start (thread_p, &hsidp->scan_cache,
				  &hsidp->hfid,
				  &hsidp->cls_oid, scan_id->fixed);
      if (ret != NO_ERROR)
	{
	  goto exit_on_error;
	}

      hsidp->scancache_inited = true;

      if (hsidp->caches_inited != true)
	{
	  hsidp->pred_attrs.attr_cache->num_values = -1;
	  ret = heap_attrinfo_start (thread_p, &hsidp->cls_oid,
				     hsidp->pred_attrs.num_attrs,
				     hsidp->pred_attrs.attr_ids,
				     hsidp->pred_attrs.attr_cache);
	  if (ret != NO_ERROR)
	    {
	      goto exit_on_error;
	    }
	  hsidp->rest_attrs.attr_cache->num_values = -1;
	  ret = heap_attrinfo_start (thread_p, &hsidp->cls_oid,
				     hsidp->rest_attrs.num_attrs,
				     hsidp->rest_attrs.attr_ids,
				     hsidp->rest_attrs.attr_cache);
	  if (ret != NO_ERROR)
	    {
	      heap_attrinfo_end (thread_p, hsidp->pred_attrs.attr_cache);
	      goto exit_on_error;
	    }
	  hsidp->caches_inited = true;
	}
      break;

    case S_INDX_SCAN:

      isidp = &scan_id->s.isid;
      /* A new argument(is_indexscan = true) is appended */
      ret = heap_scancache_start (thread_p, &isidp->scan_cache,
				  &isidp->hfid,
				  &(isidp->bt_scan.btid_int.cls_oid),
				  scan_id->fixed);
      if (ret != NO_ERROR)
	{
	  goto exit_on_error;
	}
      isidp->scancache_inited = true;
      if (isidp->caches_inited != true)
	{
	  if (isidp->key_pred.regu_list != NULL)
	    {
	      isidp->key_attrs.attr_cache->num_values = -1;
	      ret =
		heap_attrinfo_start (thread_p,
				     &(isidp->bt_scan.btid_int.cls_oid),
				     isidp->key_attrs.num_attrs,
				     isidp->key_attrs.attr_ids,
				     isidp->key_attrs.attr_cache);
	      if (ret != NO_ERROR)
		{
		  goto exit_on_error;
		}
	    }
	  isidp->pred_attrs.attr_cache->num_values = -1;
	  ret =
	    heap_attrinfo_start (thread_p, &(isidp->bt_scan.btid_int.cls_oid),
				 isidp->pred_attrs.num_attrs,
				 isidp->pred_attrs.attr_ids,
				 isidp->pred_attrs.attr_cache);
	  if (ret != NO_ERROR)
	    {
	      if (isidp->key_pred.regu_list != NULL)
		{
		  heap_attrinfo_end (thread_p, isidp->key_attrs.attr_cache);
		}
	      goto exit_on_error;
	    }
	  isidp->rest_attrs.attr_cache->num_values = -1;
	  ret =
	    heap_attrinfo_start (thread_p, &(isidp->bt_scan.btid_int.cls_oid),
				 isidp->rest_attrs.num_attrs,
				 isidp->rest_attrs.attr_ids,
				 isidp->rest_attrs.attr_cache);
	  if (ret != NO_ERROR)
	    {
	      if (isidp->key_pred.regu_list != NULL)
		{
		  heap_attrinfo_end (thread_p, isidp->key_attrs.attr_cache);
		}
	      heap_attrinfo_end (thread_p, isidp->pred_attrs.attr_cache);
	      goto exit_on_error;
	    }
	  isidp->caches_inited = true;
	}
      isidp->oid_list.oid_cnt = 0;
      isidp->curr_keyno = -1;
      isidp->curr_oidno = -1;
      BTREE_INIT_SCAN (&isidp->bt_scan);
      isidp->one_range = false;
      break;

    case S_LIST_SCAN:

      llsidp = &scan_id->s.llsid;
      /* open list file scan */
      if (qfile_open_list_scan (llsidp->list_id, &llsidp->lsid) != NO_ERROR)
	{
	  goto exit_on_error;
	}
      qfile_start_scan_fix (thread_p, &llsidp->lsid);
      break;

    default:
      assert (false);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
      goto exit_on_error;
    }				/* switch (scan_id->type) */

  /* set scan status as started */
  scan_id->position = S_BEFORE;
  scan_id->status = S_STARTED;
  scan_id->qualified_block = false;
  scan_id->single_fetched = false;
  scan_id->null_fetched = false;

  return ret;

exit_on_error:

  return (ret == NO_ERROR
	  && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;
}

/*
 * scan_reset_scan_block () - Move the scan back to the beginning point inside the current scan block.
 *   return: S_SUCCESS, S_END, S_ERROR
 *   s_id(in/out): Scan identifier
 *
 * Note: If you feel the need
 */
SCAN_CODE
scan_reset_scan_block (THREAD_ENTRY * thread_p, SCAN_ID * s_id)
{
  SCAN_CODE status = S_SUCCESS;
  INDX_COV *indx_cov_p;

  s_id->single_fetched = false;
  s_id->null_fetched = false;

  switch (s_id->type)
    {
    case S_HEAP_SCAN:
      s_id->position = S_BEFORE;
      OID_SET_NULL (&s_id->s.hsid.curr_oid);
      break;

    case S_INDX_SCAN:
      s_id->s.isid.curr_oidno = -1;
      s_id->s.isid.curr_keyno = -1;
      s_id->position = S_BEFORE;
      BTREE_INIT_SCAN (&s_id->s.isid.bt_scan);

      /* reset key limits */
      if (s_id->s.isid.indx_info)
	{
	  if (scan_init_index_key_limit (thread_p, &s_id->s.isid,
					 &s_id->s.isid.indx_info->
					 key_info, s_id->vd) != NO_ERROR)
	    {
	      status = S_ERROR;
	      break;
	    }
	}

      /* reset index covering */
      indx_cov_p = &(s_id->s.isid.indx_cov);
      if (indx_cov_p->lsid != NULL)
	{
	  qfile_close_scan (thread_p, indx_cov_p->lsid);
	}

      if (indx_cov_p->list_id != NULL)
	{
	  qfile_destroy_list (thread_p, indx_cov_p->list_id);
	  QFILE_FREE_AND_INIT_LIST_ID (indx_cov_p->list_id);

	  indx_cov_p->list_id = qfile_open_list (thread_p,
						 indx_cov_p->type_list,
						 NULL,
						 indx_cov_p->query_id, 0);
	  if (indx_cov_p->list_id == NULL)
	    {
	      status = S_ERROR;
	    }
	}
      break;

    case S_LIST_SCAN:
      /* may have scanned some already so clean up */
      qfile_end_scan_fix (thread_p, &s_id->s.llsid.lsid);
      qfile_close_scan (thread_p, &s_id->s.llsid.lsid);

      /* open list file scan for this outer row */
      if (qfile_open_list_scan (s_id->s.llsid.list_id, &s_id->s.llsid.lsid)
	  != NO_ERROR)
	{
	  status = S_ERROR;
	  break;
	}
      qfile_start_scan_fix (thread_p, &s_id->s.llsid.lsid);
      s_id->position = S_BEFORE;
      s_id->s.llsid.lsid.position = S_BEFORE;
      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
      status = S_ERROR;
      break;
    }				/* switch (s_id->type) */


  return status;
}

/*
 * scan_next_scan_block () - Move the scan to the next scan block.
 *                    If there are no more scan blocks left, S_END is returned.
 *   return: S_SUCCESS, S_END, S_ERROR
 *   s_id(in/out): Scan identifier
 *
 * Note: If you feel the need
 */
SCAN_CODE
scan_next_scan_block (UNUSED_ARG THREAD_ENTRY * thread_p, SCAN_ID * s_id)
{
  SCAN_CODE sp_scan;

  s_id->single_fetched = false;
  s_id->null_fetched = false;
  s_id->qualified_block = false;

  switch (s_id->type)
    {
    case S_HEAP_SCAN:
    case S_INDX_SCAN:
    case S_LIST_SCAN:
      sp_scan = (s_id->position == S_BEFORE) ? S_SUCCESS : S_END;
      break;

    default:
      assert (false);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
      sp_scan = S_ERROR;
      break;
    }

  return sp_scan;
}

/*
 * scan_end_scan () - End the scan process on the given scan identifier.
 *   return:
 *   scan_id(in/out): Scan identifier
 *
 * Note: If you feel the need
 */
void
scan_end_scan (THREAD_ENTRY * thread_p, SCAN_ID * scan_id)
{
  HEAP_SCAN_ID *hsidp;
  INDX_SCAN_ID *isidp;
  LLIST_SCAN_ID *llsidp;
  KEY_VAL_RANGE *key_vals;
  int i;

  if (scan_id == NULL)
    {
      return;
    }

  if ((scan_id->status == S_ENDED) || (scan_id->status == S_CLOSED))
    {
      return;
    }

  switch (scan_id->type)
    {
    case S_HEAP_SCAN:
      hsidp = &scan_id->s.hsid;

      /* do not free attr_cache here.
       * xs_clear_access_spec_list() will free attr_caches.
       */

      if (hsidp->scancache_inited)
	{
	  (void) heap_scancache_end (thread_p, &hsidp->scan_cache);
	}
      break;

    case S_INDX_SCAN:
      isidp = &scan_id->s.isid;

      /* do not free attr_cache here.
       * xs_clear_access_spec_list() will free attr_caches.
       */

      if (isidp->scancache_inited)
	{
	  (void) heap_scancache_end (thread_p, &isidp->scan_cache);
	}
      if (isidp->curr_keyno >= 0 && isidp->curr_keyno < isidp->key_cnt)
	{
	  key_vals = isidp->key_vals;
	  for (i = 0; i < isidp->key_cnt; i++)
	    {
	      db_idxkey_clear (&key_vals[i].lower_key);
	      db_idxkey_clear (&key_vals[i].upper_key);
	    }
	}
      /* clear all the used keys */
      btree_scan_clear_key (&(isidp->bt_scan));
      break;

    case S_LIST_SCAN:
      llsidp = &scan_id->s.llsid;
      qfile_end_scan_fix (thread_p, &llsidp->lsid);
      qfile_close_scan (thread_p, &llsidp->lsid);
      break;

    default:
      assert (false);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
      break;
    }

  scan_id->status = S_ENDED;
}

/*
 * scan_close_scan () - The scan identifier is closed and allocated areas and page buffers are freed.
 *   return:
 *   scan_id(in/out): Scan identifier
 *
 * Note: If you feel the need
 */
void
scan_close_scan (THREAD_ENTRY * thread_p, SCAN_ID * scan_id)
{
  INDX_SCAN_ID *isidp;
  BTREE_SCAN *BTS;

  if (scan_id == NULL || scan_id->status == S_CLOSED)
    {
      return;
    }

  switch (scan_id->type)
    {
    case S_HEAP_SCAN:
      break;

    case S_INDX_SCAN:
      isidp = &(scan_id->s.isid);
      BTS = &(isidp->bt_scan);

#if 1				/* TODO - */
      if (BTS->btid_int.classrepr)
	{
	  assert (BTS->btid_int.classrepr_cache_idx != -1);
	  assert (BTS->btid_int.indx_id != -1);

	  (void) heap_classrepr_free (BTS->btid_int.classrepr,
				      &(BTS->btid_int.classrepr_cache_idx));
	  assert (BTS->btid_int.classrepr_cache_idx == -1);

	  BTS->btid_int.classrepr = NULL;
//          BTS->btid_int.classrepr_cache_idx = -1;
//          BTS->btid_int.indx_id = -1;
	}
#endif

      if (isidp->key_vals)
	{
	  free_and_init (isidp->key_vals);
	}

      /* free allocated memory for the scan */
      if (isidp->oid_list.oidp)
	{
	  scan_free_iscan_oid_buf_list (isidp->oid_list.oidp);
	  isidp->oid_list.oidp = NULL;
	  isidp->oid_list.oid_buf_size = 0;
	}

      /* free index covering */
      if (isidp->indx_cov.lsid != NULL)
	{
	  qfile_close_scan (thread_p, isidp->indx_cov.lsid);
	  free_and_init (isidp->indx_cov.lsid);
	}
      if (isidp->indx_cov.list_id != NULL)
	{
	  qfile_close_list (thread_p, isidp->indx_cov.list_id);
	  qfile_destroy_list (thread_p, isidp->indx_cov.list_id);
	  QFILE_FREE_AND_INIT_LIST_ID (isidp->indx_cov.list_id);
	}
      if (isidp->indx_cov.type_list != NULL)
	{
	  if (isidp->indx_cov.type_list->domp != NULL)
	    {
	      free_and_init (isidp->indx_cov.type_list->domp);
	    }
	  free_and_init (isidp->indx_cov.type_list);
	}
      if (isidp->indx_cov.tplrec != NULL)
	{
	  if (isidp->indx_cov.tplrec->tpl != NULL)
	    {
	      free_and_init (isidp->indx_cov.tplrec->tpl);
	    }
	  free_and_init (isidp->indx_cov.tplrec);
	}

      /* free multiple range optimization struct */
      if (isidp->multi_range_opt.top_n_items != NULL)
	{
	  int i;

	  for (i = 0; i < isidp->multi_range_opt.size; i++)
	    {
	      if (isidp->multi_range_opt.top_n_items[i] != NULL)
		{
		  RANGE_OPT_ITEM *item;

		  item = isidp->multi_range_opt.top_n_items[i];

		  db_idxkey_clear (&(item->index_value));
		  free_and_init (isidp->multi_range_opt.top_n_items[i]);
		}
	    }
	  free_and_init (isidp->multi_range_opt.top_n_items);
	  isidp->multi_range_opt.top_n_items = NULL;
	  free_and_init (isidp->multi_range_opt.tplrec.tpl);
	  isidp->multi_range_opt.tplrec.tpl = 0;
	}
      /* free buffer */
      if (isidp->multi_range_opt.buffer != NULL)
	{
	  free_and_init (isidp->multi_range_opt.buffer);
	}
      if (isidp->multi_range_opt.sort_att_idx != NULL)
	{
	  free_and_init (isidp->multi_range_opt.sort_att_idx);
	}
      if (isidp->multi_range_opt.is_desc_order != NULL)
	{
	  free_and_init (isidp->multi_range_opt.is_desc_order);
	}
      if (isidp->multi_range_opt.sort_col_dom != NULL)
	{
	  free_and_init (isidp->multi_range_opt.sort_col_dom);
	}
      memset ((void *) (&(isidp->multi_range_opt)), 0,
	      sizeof (MULTI_RANGE_OPT));
      break;

    case S_LIST_SCAN:
      break;

    default:
      assert (false);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
      break;
    }

  scan_id->status = S_CLOSED;
}

/*
 * call_get_next_index_oidset () - Wrapper for scan_get_next_oidset, accounts
 *                                 for scan variations, such as the "index
 *                                 skip scan" optimization.
 *   return: SCAN_CODE (S_SUCCESS, S_END, S_ERROR)
 *   thread_p (in):
 *   scan_id (in/out):
 *   isidp (in/out):
 *
 * Note:
 * This function tries to obtain the next set of OIDs for the scan to consume.
 * The real heavy-lifting function is get_next_index_oidset(), which we call,
 * this one is a wrapper.
 * We also handle the case where we are called just because a new crop of OIDs
 * is needed.
 */
static SCAN_CODE
call_get_next_index_oidset (THREAD_ENTRY * thread_p, SCAN_ID * scan_id,
			    INDX_SCAN_ID * isidp)
{
  int oids_cnt;

/*
 * WHILE (true)
 * {
 *  if (iss && should-skip-to-next-iss-value)
 *    obtain-next-iss-value() or return if it does not find anything;
 *
 *  get-index-oidset();
 *
 *  if (oids count == 0) // did not find anything
 *  {
 *    should-skip-to-next-iss-value = true;
 *    if (iss)
 *      continue; // to allow the while () to fetch the next value for the
 *		// first column in the index
 *  return S_END; // BTRS returned nothing and we are not in ISS mode. Leave.
 *  }
 *
 *  break; //at least one OID found. get out of the loop.
 *}
 */

  while (1)
    {
      assert (&scan_id->s.isid == isidp);
      if (scan_get_index_oidset (thread_p, scan_id) != NO_ERROR)
	{
	  return S_ERROR;
	}

      oids_cnt = isidp->multi_range_opt.use ?
	isidp->multi_range_opt.cnt : isidp->oid_list.oid_cnt;

      if (oids_cnt == 0)
	{
	  return S_END;		/* no oids, this is the end of scan. */
	}

      /* We have at least one OID. Break the loop, allow normal processing. */
      break;
    }

  return S_SUCCESS;
}

/*
 * scan_next_scan_local () - The scan is moved to the next scan item.
 *   return: SCAN_CODE (S_SUCCESS, S_END, S_ERROR)
 *   scan_id(in/out): Scan identifier
 *
 * Note: If there are no more scan items, S_END is returned. If an error occurs, S_ERROR is returned.
 */
static SCAN_CODE
scan_next_scan_local (THREAD_ENTRY * thread_p, SCAN_ID * scan_id)
{
  SCAN_CODE status;
  bool on_trace;
  UINT64 old_fetches = 0, old_ioreads = 0;
  struct timeval scan_start, scan_end;

  on_trace = thread_is_on_trace (thread_p);
  if (on_trace)
    {
      gettimeofday (&scan_start, NULL);
      old_fetches = mnt_get_stats (thread_p, MNT_STATS_DATA_PAGE_FETCHES);
      old_ioreads = mnt_get_stats (thread_p, MNT_STATS_DATA_PAGE_IOREADS);
    }

  switch (scan_id->type)
    {
    case S_HEAP_SCAN:
      status = scan_next_heap_scan (thread_p, scan_id);
      break;

    case S_INDX_SCAN:
      status = scan_next_index_scan (thread_p, scan_id);
      break;

    case S_LIST_SCAN:
      status = scan_next_list_scan (thread_p, scan_id);
      break;

    default:
      assert (false);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
      return S_ERROR;
    }

  if (on_trace)
    {
      gettimeofday (&scan_end, NULL);
      ADD_TIMEVAL (scan_id->stats.elapsed_scan, scan_start, scan_end);
      scan_id->stats.num_fetches +=
	mnt_get_stats (thread_p, MNT_STATS_DATA_PAGE_FETCHES) - old_fetches;
      scan_id->stats.num_ioreads +=
	mnt_get_stats (thread_p, MNT_STATS_DATA_PAGE_IOREADS) - old_ioreads;
    }

  return status;
}

/*
 * scan_next_heap_scan () - The scan is moved to the next heap scan item.
 *   return: SCAN_CODE (S_SUCCESS, S_END, S_ERROR)
 *   scan_id(in/out): Scan identifier
 *
 * Note: If there are no more scan items, S_END is returned. If an error occurs, S_ERROR is returned.
 */
static SCAN_CODE
scan_next_heap_scan (THREAD_ENTRY * thread_p, SCAN_ID * scan_id)
{
  HEAP_SCAN_ID *hsidp;
  FILTER_INFO data_filter;
  RECDES recdes = RECDES_INITIALIZER;
  SCAN_CODE sp_scan;
  DB_LOGICAL ev_res;

  hsidp = &scan_id->s.hsid;

  /* set data filter information */
  scan_init_filter_info (&data_filter, &hsidp->scan_pred,
			 &hsidp->pred_attrs, scan_id->val_list,
			 scan_id->vd, &hsidp->cls_oid, NULL, -1);

  while (1)
    {
      /* regular, fixed scan */
      if (scan_id->fixed == false)
	{
	  recdes.data = NULL;
	}

      /* move forward */
      sp_scan =
	heap_next (thread_p, &hsidp->hfid, &hsidp->cls_oid,
		   &hsidp->curr_oid, &recdes, &hsidp->scan_cache,
		   scan_id->fixed);
      if (sp_scan != S_SUCCESS)
	{
	  /* scan error or end of scan */
	  return (sp_scan == S_END) ? S_END : S_ERROR;
	}

      /* evaluate the predicates to see if the object qualifies */
      scan_id->stats.read_rows++;
      ev_res = eval_data_filter (thread_p, &hsidp->curr_oid, &recdes,
				 &data_filter);
      if (ev_res == V_ERROR)
	{
	  return S_ERROR;
	}

      if (scan_id->qualification == QPROC_QUALIFIED)
	{
	  if (ev_res != V_TRUE)	/* V_FALSE || V_UNKNOWN */
	    {
	      continue;		/* not qualified, continue to the next tuple */
	    }
	}
      else if (scan_id->qualification == QPROC_NOT_QUALIFIED)
	{
	  if (ev_res != V_FALSE)	/* V_TRUE || V_UNKNOWN */
	    {
	      continue;		/* qualified, continue to the next tuple */
	    }
	}
      else if (scan_id->qualification == QPROC_QUALIFIED_OR_NOT)
	{
	  if (ev_res == V_TRUE)
	    {
	      scan_id->qualification = QPROC_QUALIFIED;
	    }
	  else if (ev_res == V_FALSE)
	    {
	      scan_id->qualification = QPROC_NOT_QUALIFIED;
	    }
	  else			/* V_UNKNOWN */
	    {
	      /* nop */
	      ;
	    }
	}
      else
	{			/* invalid value; the same as QPROC_QUALIFIED */
	  if (ev_res != V_TRUE)	/* V_FALSE || V_UNKNOWN */
	    {
	      continue;		/* not qualified, continue to the next tuple */
	    }
	}

      scan_id->stats.qualified_rows++;

      if (hsidp->rest_regu_list)
	{
	  /* read the rest of the values from the heap into the attribute
	     cache */
	  if (heap_attrinfo_read_dbvalues (thread_p, &hsidp->curr_oid,
					   &recdes,
					   hsidp->rest_attrs.attr_cache)
	      != NO_ERROR)
	    {
	      return S_ERROR;
	    }

	  /* fetch the rest of the values from the object instance */
	  if (scan_id->val_list)
	    {
	      if (fetch_val_list (thread_p, hsidp->rest_regu_list,
				  scan_id->vd,
				  &hsidp->curr_oid, NULL, NULL,
				  PEEK) != NO_ERROR)
		{
		  return S_ERROR;
		}
	    }
	}

      return S_SUCCESS;
    }
}

/*
 * scan_next_index_scan () - The scan is moved to the next index scan item.
 *   return: SCAN_CODE (S_SUCCESS, S_END, S_ERROR)
 *   scan_id(in/out): Scan identifier
 *
 * Note: If there are no more scan items, S_END is returned. If an error occurs, S_ERROR is returned.
 */
static SCAN_CODE
scan_next_index_scan (THREAD_ENTRY * thread_p, SCAN_ID * scan_id)
{
  INDX_SCAN_ID *isidp;
  FILTER_INFO data_filter;
  QFILE_TUPLE_RECORD tplrec = { NULL, 0 };
  QFILE_TUPLE_VALUE_TYPE_LIST *tpl_type_list = NULL;
  TRAN_ISOLATION isolation;
  SCAN_CODE lookup_status;
  struct timeval lookup_start, lookup_end;

  isidp = &(scan_id->s.isid);

  assert (!OID_ISNULL (&(isidp->bt_scan.btid_int.cls_oid)));

  /* multi range optimization safe guard : fall-back to normal output
   * (OID list or covering index instead of "on the fly" lists),
   * if sorting column is not yet set at this stage */
  if (isidp->multi_range_opt.use
      && isidp->multi_range_opt.sort_att_idx == NULL)
    {
#if 1				/* TODO - trace */
      assert (false);
#endif
      isidp->multi_range_opt.use = false;
      scan_id->stats.multi_range_opt = false;
    }

  /* set data filter information */
  scan_init_filter_info (&data_filter, &isidp->scan_pred,
			 &isidp->pred_attrs, scan_id->val_list,
			 scan_id->vd, &(isidp->bt_scan.btid_int.cls_oid),
			 NULL, -1);

  /* Due to the length of time that we hold onto the oid list, it is
     possible at lower isolation levels (UNCOMMITTED INSTANCES) that
     the index/heap may have changed since the oid list was read from
     the btree.  In particular, some of the instances that we are
     reading may have been deleted by the time we go to fetch them via
     heap_get ().  According to the semantics of UNCOMMITTED,
     it is okay if they are deleted out from under us and
     we can ignore the SCAN_DOESNT_EXIST error. */

  isolation = logtb_find_current_isolation (thread_p);

  while (1)
    {
      /* regular index scan */
      if (scan_id->position == S_BEFORE)
	{
	  SCAN_CODE ret;

	  ret = call_get_next_index_oidset (thread_p, scan_id, isidp);
	  if (ret != S_SUCCESS)
	    {
	      return ret;
	    }

	  if (isidp->need_count_only == true)
	    {
	      /* no more scan is needed. just return */
	      return S_SUCCESS;
	    }

	  scan_id->position = S_ON;
	  isidp->curr_oidno = 0;	/* first oid number */
	  if (isidp->multi_range_opt.use)
	    {
	      assert (isidp->curr_oidno < isidp->multi_range_opt.cnt);
	      assert (isidp->multi_range_opt.
		      top_n_items[isidp->curr_oidno] != NULL);

	      isidp->curr_oidp =
		&(isidp->multi_range_opt.
		  top_n_items[isidp->curr_oidno]->inst_oid);
	    }
	  else
	    {
	      isidp->curr_oidp =
		GET_NTH_OID (isidp->oid_list.oidp, isidp->curr_oidno);
	    }

	  if (SCAN_IS_INDEX_COVERED (isidp))
	    {
	      qfile_close_list (thread_p, isidp->indx_cov.list_id);
	      if (qfile_open_list_scan (isidp->indx_cov.list_id,
					isidp->indx_cov.lsid) != NO_ERROR)
		{
		  return S_ERROR;
		}
	    }
	}
      else if (scan_id->position == S_ON)
	{
	  int oids_cnt;
	  /* we are in the S_ON case */

	  oids_cnt = isidp->multi_range_opt.use ?
	    isidp->multi_range_opt.cnt : isidp->oid_list.oid_cnt;

	  /* if there are OIDs left */
	  if (isidp->curr_oidno < oids_cnt - 1)
	    {
	      isidp->curr_oidno++;
	      if (isidp->multi_range_opt.use)
		{
		  assert (isidp->curr_oidno < isidp->multi_range_opt.cnt);
		  assert (isidp->multi_range_opt.
			  top_n_items[isidp->curr_oidno] != NULL);

		  isidp->curr_oidp =
		    &(isidp->multi_range_opt.
		      top_n_items[isidp->curr_oidno]->inst_oid);
		}
	      else
		{
		  isidp->curr_oidp =
		    GET_NTH_OID (isidp->oid_list.oidp, isidp->curr_oidno);
		}
	    }
	  else
	    {
	      /* there are no more OIDs left. Decide what to do */

	      /* We can ignore the END OF SCAN signal if we're
	       * certain there can be more results, for instance
	       * if we have a multiple range scan */
	      if (BTREE_END_OF_SCAN (&isidp->bt_scan)
		  && isidp->indx_info->range_type != R_RANGELIST
		  && isidp->indx_info->range_type != R_KEYLIST)
		{
		  return S_END;
		}
	      else
		{
		  SCAN_CODE ret;

		  /* a list in a range is exhausted */
		  if (isidp->multi_range_opt.use)
		    {
		      /* for "on the fly" case (multi range opt),
		       * all ranges are exhausted from first
		       * shoot, force exit */
		      isidp->oid_list.oid_cnt = 0;
		      return S_END;
		    }

		  if (SCAN_IS_INDEX_COVERED (isidp))
		    {
		      /* close current list and start a new one */
		      qfile_close_scan (thread_p, isidp->indx_cov.lsid);
		      qfile_destroy_list (thread_p, isidp->indx_cov.list_id);
		      QFILE_FREE_AND_INIT_LIST_ID (isidp->indx_cov.list_id);
		      isidp->indx_cov.list_id =
			qfile_open_list (thread_p,
					 isidp->indx_cov.type_list,
					 NULL, isidp->indx_cov.query_id, 0);
		      if (isidp->indx_cov.list_id == NULL)
			{
			  return S_ERROR;
			}
		    }

		  /* if this the current scan is not done (i.e.
		   * the buffer was full and we need to fetch
		   * more rows, do not go to the next value */
		  ret = call_get_next_index_oidset (thread_p, scan_id, isidp);
		  if (ret != S_SUCCESS)
		    {
		      return ret;
		    }

		  if (isidp->need_count_only == true)
		    {
		      /* no more scan is needed. just return */
		      return S_SUCCESS;
		    }

		  isidp->curr_oidno = 0;	/* first oid number */
		  isidp->curr_oidp = isidp->oid_list.oidp;

		  if (SCAN_IS_INDEX_COVERED (isidp))
		    {
		      qfile_close_list (thread_p, isidp->indx_cov.list_id);
		      if (qfile_open_list_scan
			  (isidp->indx_cov.list_id,
			   isidp->indx_cov.lsid) != NO_ERROR)
			{
			  return S_ERROR;
			}
		    }
		}
	    }
	}
      else if (scan_id->position == S_AFTER)
	{
	  return S_END;
	}
      else
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_UNKNOWN_CRSPOS, 0);
	  return S_ERROR;
	}

      scan_id->stats.key_qualified_rows++;

      if (!SCAN_IS_INDEX_COVERED (isidp))
	{
	  mnt_stats_counter (thread_p, MNT_STATS_BTREE_NONCOVERED, 1);

	  if (thread_is_on_trace (thread_p))
	    {
	      gettimeofday (&lookup_start, NULL);
	    }

	  lookup_status = scan_next_index_lookup_heap (thread_p, scan_id,
						       isidp, &data_filter,
						       isolation);

	  if (thread_is_on_trace (thread_p))
	    {
	      gettimeofday (&lookup_end, NULL);
	      ADD_TIMEVAL (scan_id->stats.elapsed_lookup,
			   lookup_start, lookup_end);
	    }

	  if (lookup_status == S_SUCCESS)
	    {
	      scan_id->stats.data_qualified_rows++;
	    }
	  else if (lookup_status == S_DOESNT_EXIST)
	    {
	      /* not qualified, continue to the next tuple */
	      continue;
	    }
	  else
	    {
	      /* S_ERROR, S_END */
	      return lookup_status;
	    }
	}
      else
	{
	  if (isidp->multi_range_opt.use)
	    {
	      RANGE_OPT_ITEM *curr_item;

	      assert (isidp->curr_oidno < isidp->multi_range_opt.cnt);

	      curr_item =
		isidp->multi_range_opt.top_n_items[isidp->curr_oidno];
	      assert (curr_item != NULL);

	      if (scan_dump_key_into_tuple (thread_p, isidp,
					    &(curr_item->index_value),
					    isidp->curr_oidp,
					    &isidp->multi_range_opt.
					    tplrec) != NO_ERROR)
		{
		  return S_ERROR;
		}
	      tplrec.tpl = isidp->multi_range_opt.tplrec.tpl;
	      tplrec.size = isidp->multi_range_opt.tplrec.size;
	    }
	  else
	    {
	      if (qfile_scan_list_next (thread_p, isidp->indx_cov.lsid,
					&tplrec, PEEK) != S_SUCCESS)
		{
		  return S_ERROR;
		}
	    }

	  mnt_stats_counter (thread_p, MNT_STATS_BTREE_COVERED, 1);

	  if (scan_id->val_list)
	    {
	      tpl_type_list = &(isidp->indx_cov.lsid->list_id.type_list);
	      assert (tpl_type_list != NULL);

	      if (fetch_val_list (thread_p, isidp->indx_cov.regu_val_list,
				  scan_id->vd, NULL,
				  tplrec.tpl, tpl_type_list,
				  PEEK) != NO_ERROR)
		{
		  return S_ERROR;
		}
	    }
	}

      return S_SUCCESS;
    }
}

/*
 * scan_next_index_lookup_heap () - fetch heap record and evaluate data filter
 *   return: SCAN_CODE (S_SUCCESS, S_END, S_ERROR, S_DOESNT_EXIST)
 *   scan_id(in/out): Scan identifier
 *   isidp(in/out): Index scan identifier
 *   data_filter(in): data filter information
 *   isolation(in): transaction isolation level
 *
 * Note: If the tuple is not qualified for data filter, S_DOESNT_EXIST is returned.
 */
static SCAN_CODE
scan_next_index_lookup_heap (THREAD_ENTRY * thread_p, SCAN_ID * scan_id,
			     INDX_SCAN_ID * isidp, FILTER_INFO * data_filter,
			     TRAN_ISOLATION isolation)
{
  SCAN_CODE sp_scan;
  DB_LOGICAL ev_res;
  RECDES recdes = RECDES_INITIALIZER;
  INDX_INFO *indx_infop;
  BTID *btid;
  char *indx_name_p;
  char *class_name_p;

  if (scan_id->fixed == false)
    {
      recdes.data = NULL;
    }

  sp_scan = heap_get (thread_p, isidp->curr_oidp, &recdes, &isidp->scan_cache,
		      scan_id->fixed);

  if (sp_scan != S_SUCCESS)
    {
      /* check end of scan */
      if (sp_scan == S_END)
	{
	  assert (false);	/* is impossible case */
	  return S_END;
	}

      indx_infop = isidp->indx_info;
      btid = &(indx_infop->indx_id.i.btid);
      indx_name_p = NULL;
      class_name_p = NULL;

      /* check scan notification */
      if (QPROC_OK_IF_DELETED (sp_scan, isolation))
	{
	  (void) heap_get_indexname_of_btid (thread_p,
					     &(isidp->bt_scan.btid_int.
					       cls_oid), btid, &indx_name_p);

	  class_name_p =
	    heap_get_class_name (thread_p,
				 &(isidp->bt_scan.btid_int.cls_oid));

	  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
		  ER_LC_INCONSISTENT_BTREE_ENTRY_TYPE2, 11,
		  (indx_name_p) ? indx_name_p : "*UNKNOWN-INDEX*",
		  (class_name_p) ? class_name_p : "*UNKNOWN-TABLE*",
		  isidp->bt_scan.btid_int.cls_oid.volid,
		  isidp->bt_scan.btid_int.cls_oid.pageid,
		  isidp->bt_scan.btid_int.cls_oid.slotid,
		  isidp->curr_oidp->volid, isidp->curr_oidp->pageid,
		  isidp->curr_oidp->slotid, btid->vfid.volid,
		  btid->vfid.fileid, btid->root_pageid);

	  if (class_name_p)
	    {
	      free_and_init (class_name_p);
	    }

	  if (indx_name_p)
	    {
	      free_and_init (indx_name_p);
	    }

	  return S_DOESNT_EXIST;	/* continue to the next object */
	}

      /* check scan error */
      if (er_errid () == NO_ERROR)
	{
	  (void) heap_get_indexname_of_btid (thread_p,
					     &(isidp->bt_scan.btid_int.
					       cls_oid), btid, &indx_name_p);

	  class_name_p =
	    heap_get_class_name (thread_p,
				 &(isidp->bt_scan.btid_int.cls_oid));

	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LC_INCONSISTENT_BTREE_ENTRY_TYPE2, 11,
		  (indx_name_p) ? indx_name_p : "*UNKNOWN-INDEX*",
		  (class_name_p) ? class_name_p : "*UNKNOWN-TABLE*",
		  isidp->bt_scan.btid_int.cls_oid.volid,
		  isidp->bt_scan.btid_int.cls_oid.pageid,
		  isidp->bt_scan.btid_int.cls_oid.slotid,
		  isidp->curr_oidp->volid, isidp->curr_oidp->pageid,
		  isidp->curr_oidp->slotid, btid->vfid.volid,
		  btid->vfid.fileid, btid->root_pageid);

	  if (class_name_p)
	    {
	      free_and_init (class_name_p);
	    }

	  if (indx_name_p)
	    {
	      free_and_init (indx_name_p);
	    }
	}

      return S_ERROR;
    }

  /* evaluate the predicates to see if the object qualifies */
  ev_res = eval_data_filter (thread_p, isidp->curr_oidp, &recdes,
			     data_filter);
  if (ev_res == V_ERROR)
    {
      return S_ERROR;
    }

  if (scan_id->qualification == QPROC_QUALIFIED)
    {
      if (ev_res != V_TRUE)	/* V_FALSE || V_UNKNOWN */
	{
	  return S_DOESNT_EXIST;	/* not qualified, continue to the next tuple */
	}
    }
  else if (scan_id->qualification == QPROC_NOT_QUALIFIED)
    {
      if (ev_res != V_FALSE)	/* V_TRUE || V_UNKNOWN */
	{
	  return S_DOESNT_EXIST;	/* qualified, continue to the next tuple */
	}
    }
  else if (scan_id->qualification == QPROC_QUALIFIED_OR_NOT)
    {
      if (ev_res == V_TRUE)
	{
	  scan_id->qualification = QPROC_QUALIFIED;
	}
      else if (ev_res == V_FALSE)
	{
	  scan_id->qualification = QPROC_NOT_QUALIFIED;
	}
      else			/* V_UNKNOWN */
	{
	  /* nop */
	  ;
	}
    }
  else
    {				/* invalid value; the same as QPROC_QUALIFIED */
      if (ev_res != V_TRUE)	/* V_FALSE || V_UNKNOWN */
	{
	  return S_DOESNT_EXIST;	/* not qualified, continue to the next tuple */
	}
    }

  if (ev_res == V_ERROR)
    {
      return S_ERROR;
    }
  else if (ev_res != V_TRUE)	/* V_FALSE || V_UNKNOWN */
    {
      return S_DOESNT_EXIST;	/* not qualified, continue to the next tuple */
    }

  if (isidp->rest_regu_list)
    {
      /* read the rest of the values from the heap into the attribute
         cache */
      if (heap_attrinfo_read_dbvalues (thread_p, isidp->curr_oidp, &recdes,
				       isidp->rest_attrs.
				       attr_cache) != NO_ERROR)
	{
	  return S_ERROR;
	}

      /* fetch the rest of the values from the object instance */
      if (scan_id->val_list)
	{
	  if (fetch_val_list (thread_p, isidp->rest_regu_list, scan_id->vd,
			      isidp->curr_oidp, NULL, NULL, PEEK) != NO_ERROR)
	    {
	      return S_ERROR;
	    }
	}
    }

  return S_SUCCESS;
}

/*
 * scan_next_list_scan () - The scan is moved to the next list scan item.
 *   return: SCAN_CODE (S_SUCCESS, S_END, S_ERROR)
 *   scan_id(in/out): Scan identifier
 *
 * Note: If there are no more scan items, S_END is returned. If an error occurs, S_ERROR is returned.
 */
static SCAN_CODE
scan_next_list_scan (THREAD_ENTRY * thread_p, SCAN_ID * scan_id)
{
  LLIST_SCAN_ID *llsidp;
  SCAN_CODE qp_scan;
  DB_LOGICAL ev_res;
  QFILE_TUPLE_RECORD tplrec = { NULL, 0 };
  QFILE_TUPLE_VALUE_TYPE_LIST *tpl_type_list = NULL;

  llsidp = &scan_id->s.llsid;

  tplrec.size = 0;
  tplrec.tpl = (QFILE_TUPLE) NULL;

  tpl_type_list = &(llsidp->list_id->type_list);
  assert (tpl_type_list != NULL);

  while ((qp_scan = qfile_scan_list_next (thread_p, &llsidp->lsid,
					  &tplrec, PEEK)) == S_SUCCESS)
    {

      /* fetch the values for the predicate from the tuple */
      if (scan_id->val_list)
	{
	  if (fetch_val_list (thread_p, llsidp->scan_pred.regu_list,
			      scan_id->vd, NULL,
			      tplrec.tpl, tpl_type_list, PEEK) != NO_ERROR)
	    {
	      return S_ERROR;
	    }
	}

      scan_id->stats.read_rows++;

      /* evaluate the predicate to see if the tuple qualifies */
      ev_res = V_TRUE;
      if (llsidp->scan_pred.pr_eval_fnc && llsidp->scan_pred.pred_expr)
	{
	  ev_res = (*llsidp->scan_pred.pr_eval_fnc) (thread_p,
						     llsidp->
						     scan_pred.pred_expr,
						     scan_id->vd, NULL);
	  if (ev_res == V_ERROR)
	    {
	      return S_ERROR;
	    }
	}

      if (scan_id->qualification == QPROC_QUALIFIED)
	{
	  if (ev_res != V_TRUE)	/* V_FALSE || V_UNKNOWN */
	    {
	      continue;		/* not qualified, continue to the next tuple */
	    }
	}
      else if (scan_id->qualification == QPROC_NOT_QUALIFIED)
	{
	  if (ev_res != V_FALSE)	/* V_TRUE || V_UNKNOWN */
	    {
	      continue;		/* qualified, continue to the next tuple */
	    }
	}
      else if (scan_id->qualification == QPROC_QUALIFIED_OR_NOT)
	{
	  if (ev_res == V_TRUE)
	    {
	      scan_id->qualification = QPROC_QUALIFIED;
	    }
	  else if (ev_res == V_FALSE)
	    {
	      scan_id->qualification = QPROC_NOT_QUALIFIED;
	    }
	  else			/* V_UNKNOWN */
	    {
	      /* nop */
	      ;
	    }
	}
      else
	{			/* invalid value; the same as QPROC_QUALIFIED */
	  if (ev_res != V_TRUE)	/* V_FALSE || V_UNKNOWN */
	    {
	      continue;		/* not qualified, continue to the next tuple */
	    }
	}

      scan_id->stats.qualified_rows++;

      /* fetch the rest of the values from the tuple */
      if (scan_id->val_list)
	{
	  if (fetch_val_list (thread_p, llsidp->rest_regu_list,
			      scan_id->vd, NULL,
			      tplrec.tpl, tpl_type_list, PEEK) != NO_ERROR)
	    {
	      return S_ERROR;
	    }
	}

      if (llsidp->tplrecp)
	{
	  llsidp->tplrecp->size = tplrec.size;
	  llsidp->tplrecp->tpl = tplrec.tpl;
	}

      return S_SUCCESS;
    }

  return qp_scan;
}

/*
 * scan_handle_single_scan () -
 *   return: SCAN_CODE (S_SUCCESS, S_END, S_ERROR)
 *   scan_id(in/out): Scan identifier
 *
 * Note: This second order function applies the given next-scan function,
 * then enforces the fetch_type , null_fetch semantics.
 * Note that when "fetch_type", "null_fetch" is asserted, at least one
 * qualified scan item, the NULL row, is returned.
 */
static SCAN_CODE
scan_handle_single_scan (THREAD_ENTRY * thread_p, SCAN_ID * s_id,
			 QP_SCAN_FUNC next_scan)
{
  SCAN_CODE result = S_ERROR;

  if (s_id->scan_immediately_stop == true)
    {
      result = S_END;
      goto end;
    }

  switch (s_id->fetch_type)
    {
    case QPROC_FETCH_INNER:
      result = (*next_scan) (thread_p, s_id);

      if (result == S_ERROR)
	{
	  goto exit_on_error;
	}
      break;

    case QPROC_FETCH_OUTER:
      /* already returned a NULL row?
         if scan works in a left outer join mode and a NULL row has
         already fetched, return end_of_scan. */
      if (s_id->null_fetched)
	{
	  result = S_END;
	}
      else
	{
	  result = (*next_scan) (thread_p, s_id);

	  if (result == S_ERROR)
	    {
	      goto exit_on_error;
	    }

	  if (result == S_END)
	    {
	      if (!s_id->single_fetched)
		{
		  /* no qualified items, return a NULL row */
		  qdata_set_value_list_to_null (s_id->val_list);
		  s_id->null_fetched = true;
		  result = S_SUCCESS;
		}
	    }

	  if (result == S_SUCCESS)
	    {
	      s_id->single_fetched = true;
	    }
	}
      break;

    default:
      assert (false);		/* is impossible */
      break;
    }

end:
  /* maintain what is apparently suposed to be an invariant--
   * S_END implies position is "after" the scan
   */
  if (result == S_END)
    {
      s_id->position = S_AFTER;
    }

  return result;

exit_on_error:

  return S_ERROR;
}

/*
 * scan_next_scan () -
 *   return: SCAN_CODE (S_SUCCESS, S_END, S_ERROR)
 *   scan_id(in/out): Scan identifier
 */
SCAN_CODE
scan_next_scan (THREAD_ENTRY * thread_p, SCAN_ID * s_id)
{
  return scan_handle_single_scan (thread_p, s_id, scan_next_scan_local);
}


/*
 * scan_initialize () - initialize scan management routine
 *   return: NO_ERROR if all OK, ER status otherwise
 */
void
scan_initialize (void)
{
  int i;
  OID *oid_buf_p;

  scan_Iscan_oid_buf_list = NULL;
  scan_Iscan_oid_buf_list_count = 0;

  /* pre-allocate oid buf list */
  for (i = 0; i < 10; i++)
    {
      oid_buf_p = scan_alloc_oid_buf ();
      if (oid_buf_p == NULL)
	{
	  break;
	}
      /* save previous oid buf pointer */
      *(intptr_t *) oid_buf_p = (intptr_t) scan_Iscan_oid_buf_list;
      scan_Iscan_oid_buf_list = oid_buf_p;
      scan_Iscan_oid_buf_list_count++;
    }
}

/*
 * scan_finalize () - finalize scan management routine
 *   return:
 */
void
scan_finalize (void)
{
  OID *oid_buf_p;

  while (scan_Iscan_oid_buf_list != NULL)
    {
      oid_buf_p = scan_Iscan_oid_buf_list;
      /* save previous oid buf pointer */
      scan_Iscan_oid_buf_list = (OID *) (*(intptr_t *) oid_buf_p);
      free_and_init (oid_buf_p);
    }
  scan_Iscan_oid_buf_list_count = 0;
}

/*
 * reverse_key_list () - reverses the key list
 *   return: number of keys, -1 for error
 *   key_vals (in): pointer to array of KEY_VAL_RANGE structure
 *   key_cnt (in): number of keys; size of key_vals
 */
static int
reverse_key_list (KEY_VAL_RANGE * key_vals, int key_cnt)
{
  int i;
  KEY_VAL_RANGE temp;

  for (i = 0; 2 * i < key_cnt; i++)
    {
      temp = key_vals[i];
      key_vals[i] = key_vals[key_cnt - i - 1];
      key_vals[key_cnt - i - 1] = temp;
    }

  return key_cnt;
}


/*
 * scan_init_multi_range_optimization () - initialize structure for multiple
 *				range optimization
 *
 *   return: error code
 *
 * multi_range_opt(in): multiple range optimization structure
 * use_range_opt(in): to use or not optimization
 * max_size(in): size of arrays for the top N values
 */
static int
scan_init_multi_range_optimization (THREAD_ENTRY * thread_p,
				    MULTI_RANGE_OPT * multi_range_opt,
				    bool use_range_opt, int max_size)
{
  int err = NO_ERROR;

  if (multi_range_opt == NULL)
    {
      return ER_FAILED;
    }

  memset ((void *) (multi_range_opt), 0, sizeof (MULTI_RANGE_OPT));
  multi_range_opt->use = use_range_opt;
  multi_range_opt->cnt = 0;

  if (use_range_opt)
    {
      multi_range_opt->size = max_size;
      /* we don't have sort information here, just set an invalid value */
      multi_range_opt->sort_att_idx = NULL;
      multi_range_opt->is_desc_order = NULL;
      multi_range_opt->no_attrs = 0;

      multi_range_opt->top_n_items = (RANGE_OPT_ITEM **)
	malloc (max_size * sizeof (RANGE_OPT_ITEM *));
      if (multi_range_opt->top_n_items == NULL)
	{
	  err = ER_OUT_OF_VIRTUAL_MEMORY;
	  goto exit_on_error;
	}
      multi_range_opt->buffer = (RANGE_OPT_ITEM **)
	malloc (max_size * sizeof (RANGE_OPT_ITEM *));
      if (multi_range_opt->buffer == NULL)
	{
	  err = ER_OUT_OF_VIRTUAL_MEMORY;
	  goto exit_on_error;
	}
      memset (multi_range_opt->top_n_items, 0,
	      max_size * sizeof (RANGE_OPT_ITEM *));

      multi_range_opt->tplrec.size = 0;
      multi_range_opt->tplrec.tpl = NULL;

      mnt_stats_counter (thread_p, MNT_STATS_BTREE_MULTIRANGE_OPTIMIZATION,
			 1);
    }

  return err;

exit_on_error:

  if (multi_range_opt->top_n_items != NULL)
    {
      free_and_init (multi_range_opt->top_n_items);
    }
  if (multi_range_opt->buffer != NULL)
    {
      free_and_init (multi_range_opt->buffer);
    }

  return err;
}

/*
 * scan_dump_key_into_tuple () - outputs the value stored in 'key' into the
 *				 tuple 'tplrec'
 *
 *   return: error code
 *   iscan_id(in):
 *   key(in): key (as it is retreived from index)
 *   oid(in): oid (required if objects are stored in 'key')
 *   tplrec(out):
 *
 *  Note : this function is used by multiple range search optimization;
 *	   although not required here, the key should be a IDXKEY value,
 *	   when multiple range search optimization is enabled.
 */
static int
scan_dump_key_into_tuple (THREAD_ENTRY * thread_p, INDX_SCAN_ID * iscan_id,
			  const DB_IDXKEY * key,
			  OID * oid, QFILE_TUPLE_RECORD * tplrec)
{
  int error;

  if (iscan_id == NULL || iscan_id->indx_cov.val_descr == NULL
      || iscan_id->indx_cov.output_val_list == NULL
      || iscan_id->rest_attrs.attr_cache == NULL)
    {
      return ER_FAILED;
    }

  error = btree_attrinfo_read_dbvalues (thread_p, key,
					iscan_id->bt_scan.btid_int.classrepr,
					iscan_id->bt_scan.btid_int.indx_id,
					iscan_id->rest_attrs.attr_cache);
  if (error != NO_ERROR)
    {
      return error;
    }

  error = fetch_val_list (thread_p, iscan_id->rest_regu_list,
			  iscan_id->indx_cov.val_descr, oid, NULL, NULL,
			  PEEK);
  if (error != NO_ERROR)
    {
      return error;
    }

  error = qdata_copy_valptr_list_to_tuple (thread_p,
					   &(iscan_id->indx_cov.lsid->
					     list_id),
					   iscan_id->indx_cov.output_val_list,
					   iscan_id->indx_cov.val_descr,
					   tplrec);
  if (error != NO_ERROR)
    {
      return error;
    }

  return NO_ERROR;
}


#if defined (SERVER_MODE)

/*
 * scan_print_stats_json () -
 * return:
 * scan_id(in):
 */
void
scan_print_stats_json (SCAN_ID * scan_id, json_t * stats)
{
  json_t *scan, *lookup;

  if (scan_id == NULL || stats == NULL)
    {
      return;
    }

  scan = json_pack ("{s:i, s:I, s:I}",
		    "time", TO_MSEC (scan_id->stats.elapsed_scan),
		    "fetch", scan_id->stats.num_fetches,
		    "ioread", scan_id->stats.num_ioreads);

  if (scan_id->type == S_HEAP_SCAN || scan_id->type == S_LIST_SCAN)
    {
      json_object_set_new (scan, "readrows",
			   json_integer (scan_id->stats.read_rows));
      json_object_set_new (scan, "rows",
			   json_integer (scan_id->stats.qualified_rows));

      if (scan_id->type == S_HEAP_SCAN)
	{
	  json_object_set_new (stats, "heap", scan);
	}
      else
	{
	  json_object_set_new (stats, "temp", scan);
	}
    }
  else if (scan_id->type == S_INDX_SCAN)
    {
      json_object_set_new (scan, "readkeys",
			   json_integer (scan_id->stats.read_keys));
      json_object_set_new (scan, "filteredkeys",
			   json_integer (scan_id->stats.qualified_keys));
      json_object_set_new (scan, "rows",
			   json_integer (scan_id->stats.key_qualified_rows));
      json_object_set_new (stats, "btree", scan);

      if (scan_id->stats.covered_index == true)
	{
	  json_object_set_new (stats, "covered", json_true ());
	}
      else
	{
	  lookup = json_pack ("{s:i, s:i}",
			      "time", TO_MSEC (scan_id->stats.elapsed_lookup),
			      "rows", scan_id->stats.data_qualified_rows);

	  json_object_set_new (stats, "lookup", lookup);
	}

      if (scan_id->stats.multi_range_opt == true)
	{
	  json_object_set_new (stats, "mro", json_true ());
	}

    }
  else
    {
      json_object_set_new (stats, "noscan", scan);
    }
}

/*
 * scan_print_stats_text () -
 * return:
 * scan_id(in):
 */
void
scan_print_stats_text (FILE * fp, SCAN_ID * scan_id)
{
  if (scan_id == NULL)
    {
      return;
    }

  if (scan_id->type == S_HEAP_SCAN)
    {
      fprintf (fp, "(heap");
    }
  else if (scan_id->type == S_INDX_SCAN)
    {
      fprintf (fp, "(btree");
    }
  else if (scan_id->type == S_LIST_SCAN)
    {
      fprintf (fp, "(temp");
    }
  else
    {
      fprintf (fp, "(noscan");
    }

  fprintf (fp, " time: %d, fetch: %lld, ioread: %lld",
	   TO_MSEC (scan_id->stats.elapsed_scan),
	   (long long int) scan_id->stats.num_fetches,
	   (long long int) scan_id->stats.num_ioreads);

  if (scan_id->type == S_HEAP_SCAN || scan_id->type == S_LIST_SCAN)
    {
      fprintf (fp, ", readrows: %d, rows: %d)",
	       scan_id->stats.read_rows, scan_id->stats.qualified_rows);
    }
  else if (scan_id->type == S_INDX_SCAN)
    {
      fprintf (fp, ", readkeys: %d, filteredkeys: %d, rows: %d",
	       scan_id->stats.read_keys,
	       scan_id->stats.qualified_keys,
	       scan_id->stats.key_qualified_rows);

      if (scan_id->stats.covered_index == true)
	{
	  fprintf (fp, ", covered: true");
	}

      if (scan_id->stats.multi_range_opt == true)
	{
	  fprintf (fp, ", mro: true");
	}

      fprintf (fp, ")");

      if (scan_id->stats.covered_index == false)
	{
	  fprintf (fp, " (lookup time: %d, rows: %d)",
		   TO_MSEC (scan_id->stats.elapsed_lookup),
		   scan_id->stats.data_qualified_rows);
	}
    }
  else
    {
      fprintf (fp, ")");
    }
}
#endif
