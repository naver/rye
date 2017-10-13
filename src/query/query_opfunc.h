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


#ifndef _QUERY_OPFUNC_H_
#define _QUERY_OPFUNC_H_

#ident "$Id$"

#include "oid.h"
#include "dbtype.h"
#include "memory_alloc.h"
#include "storage_common.h"
#include "heap_file.h"
#include "query_evaluator.h"
#include "parse_tree.h"
#include "query_list.h"

#define UNBOUND(x) ((x)->val_flag == V_UNBOUND || (x)->type == DB_TYPE_NULL)

#define BOUND(x) (! UNBOUND(x))

#define SECONDS_OF_ONE_DAY      86400	/* 24 * 60 * 60 */
#define MILLISECONDS_OF_ONE_DAY 86400000	/* 24 * 60 * 60 * 1000 */

typedef enum
{				/* Responses to a query */
  QUERY_END = 1,		/* Normal end of query */
  GET_NEXT_LOG_PAGES,		/* log writer uses this type of request */
  END_CALLBACK			/* normal end of non-query callback */
} QUERY_SERVER_REQUEST;

typedef enum
{
  SEQUENTIAL,			/* sequential scan access */
  INDEX				/* indexed access */
} ACCESS_METHOD;

#define IS_ANY_INDEX_ACCESS(access_)  \
  ((access_) == INDEX)

typedef enum
{
  QPROC_QUALIFIED = 0,		/* fetch a qualified item; default */
  QPROC_NOT_QUALIFIED,		/* fetch a not-qualified item */
  QPROC_QUALIFIED_OR_NOT	/* fetch either a qualified or not-qualified item */
} QPROC_QUALIFICATION;

typedef enum
{
  QPROC_TPLDESCR_SUCCESS = 1,	/* success generating tuple descriptor */
  QPROC_TPLDESCR_FAILURE = 0,	/* error, give up */
  QPROC_TPLDESCR_RETRY_SET_TYPE = -1,	/* error, retry for SET data-type */
  QPROC_TPLDESCR_RETRY_BIG_REC = -2	/* error, retry for BIG RECORD */
} QPROC_TPLDESCR_STATUS;

typedef struct xasl_node XASL_NODE;

typedef struct xasl_stream XASL_STREAM;
struct xasl_stream
{
  XASL_ID *xasl_id;
  XASL_NODE_HEADER *xasl_header;

  char *xasl_stream;
  int xasl_stream_size;
};


/*
 * EXECUTION_INFO is used for receive server execution info
 * If you want get another info from server (ex. server statdump)
 * add item in this structure
 */
typedef struct execution_info EXECUTION_INFO;
struct execution_info
{
  char *sql_hash_text;		/* rewritten query string which is used as hash key */
  char *sql_user_text;		/* original query statement that user input */
  char *sql_plan_text;		/* plans for this query */
};

extern void qdata_set_value_list_to_null (VAL_LIST * val_list);
extern int qdata_copy_db_value (DB_VALUE * dbval1, DB_VALUE * dbval2);

extern int qdata_copy_db_value_to_tuple_value (DB_VALUE * dbval, char *tvalp,
					       int *tval_size);
extern int qdata_copy_valptr_list_to_tuple (THREAD_ENTRY * thread_p,
					    QFILE_LIST_ID * list_id,
					    VALPTR_LIST * valptr_list,
					    VAL_DESCR * vd,
					    QFILE_TUPLE_RECORD * tplrec);
extern QPROC_TPLDESCR_STATUS
qdata_generate_tuple_desc_for_valptr_list (THREAD_ENTRY * thread_p,
					   QFILE_LIST_ID * list_id,
					   VALPTR_LIST * valptr_list,
					   VAL_DESCR * vd,
					   QFILE_TUPLE_DESCRIPTOR * tdp);
extern int qdata_set_valptr_list_unbound (THREAD_ENTRY * thread_p,
					  VALPTR_LIST * valptr_list,
					  VAL_DESCR * vd);

extern int qdata_concatenate_dbval (THREAD_ENTRY * thread_p,
				    DB_VALUE * dbval1, DB_VALUE * dbval2,
				    DB_VALUE * res,
				    const int max_allowed_size,
				    const char *warning_context);

extern int qdata_initialize_aggregate_list (THREAD_ENTRY * thread_p,
					    AGGREGATE_TYPE * agg_list,
					    QUERY_ID query_id);
extern int qdata_aggregate_value_to_accumulator (THREAD_ENTRY * thread_p,
						 AGGREGATE_ACCUMULATOR * acc,
						 FUNC_TYPE func_type,
						 DB_VALUE * value);
extern int qdata_evaluate_aggregate_list (THREAD_ENTRY * thread_p,
					  AGGREGATE_TYPE * agg_list,
					  VAL_DESCR * vd);
extern int qdata_evaluate_aggregate_optimize (THREAD_ENTRY * thread_p,
					      AGGREGATE_TYPE * agg_ptr,
					      OID * cls_oid, HFID * hfid);
extern int qdata_finalize_aggregate_list (THREAD_ENTRY * thread_p,
					  AGGREGATE_TYPE * agg_list,
					  VAL_DESCR * vd,
					  bool keep_list_file);
extern int qdata_get_single_tuple_from_list_id (THREAD_ENTRY * thread_p,
						QFILE_LIST_ID * list_id,
						VAL_LIST * single_tuple);
extern int qdata_get_valptr_type_list (THREAD_ENTRY * thread_p,
				       VALPTR_LIST * valptr_list,
				       QFILE_TUPLE_VALUE_TYPE_LIST *
				       type_list);
extern int qdata_evaluate_function (THREAD_ENTRY * thread_p,
				    REGU_VARIABLE * func, VAL_DESCR * vd,
				    OID * obj_oid, QFILE_TUPLE tpl);

extern int prepare_query (COMPILE_CONTEXT * context, XASL_STREAM * stream);
extern int execute_query (DB_SESSION * session, PT_NODE * statement,
			  QFILE_LIST_ID ** list_idp, QUERY_FLAG flag);

extern int qdata_list_dbs (THREAD_ENTRY * thread_p, DB_VALUE * result_p);
extern int qdata_regu_list_to_regu_array (FUNCTION_TYPE * function_p,
					  const int array_size,
					  REGU_VARIABLE * regu_array[],
					  int *num_regu);
extern int qdata_get_index_cardinality (THREAD_ENTRY * thread_p,
					DB_VALUE * db_class_name,
					DB_VALUE * db_index_name,
					DB_VALUE * db_key_position,
					DB_VALUE * result_p);
extern int qdata_tuple_to_values_array (THREAD_ENTRY * thread_p,
					QFILE_TUPLE_DESCRIPTOR * tuple,
					DB_VALUE ** values);
extern int qdata_get_tuple_value_size_from_dbval (DB_VALUE * dbval_p);
#endif /* _QUERY_OPFUNC_H_ */
