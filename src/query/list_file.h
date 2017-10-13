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
 * List files (Server Side)
 */

#ifndef _LIST_FILE_H_
#define _LIST_FILE_H_

#ident "$Id$"

#include "config.h"

#include <stdio.h>

#include "dbtype.h"
#include "storage_common.h"
#include "system_parameter.h"
#include "external_sort.h"
#include "query_executor.h"
#include "query_list.h"
#include "query_evaluator.h"
#include "log_comm.h"
#include "object_domain.h"
#include "thread.h"

#define QFILE_FREE_AND_INIT_LIST_ID(list_id) \
  do {                                       \
    if (list_id != NULL)                     \
    {                                        \
      qfile_free_list_id (list_id);          \
      list_id = NULL;                        \
    }                                        \
  } while (0)

typedef struct qfile_page_header QFILE_PAGE_HEADER;
struct qfile_page_header
{
  int pg_tplcnt;		/* tuple count for the page */
  PAGEID prev_pgid;		/* previous page identifier */
  PAGEID next_pgid;		/* next page identifier */
  int lasttpl_off;		/* offset value of the last tuple */
  PAGEID ovfl_pgid;		/* overflow page identifier */
  VOLID prev_volid;		/* previous page volume identifier */
  VOLID next_volid;		/* next page volume identifier */
  VOLID ovfl_volid;		/* overflow page volume identifier */
};

/* List manipulation routines */
extern int qfile_initialize (void);
extern void qfile_finalize (void);
extern void qfile_destroy_list (THREAD_ENTRY * thread_p,
				QFILE_LIST_ID * list_id);
extern void qfile_close_list (THREAD_ENTRY * thread_p,
			      QFILE_LIST_ID * list_id);
extern int qfile_add_tuple_to_list (THREAD_ENTRY * thread_p,
				    QFILE_LIST_ID * list_id, QFILE_TUPLE tpl);
extern int qfile_add_overflow_tuple_to_list (THREAD_ENTRY * thread_p,
					     QFILE_LIST_ID * list_id,
					     PAGE_PTR ovfl_tpl_pg,
					     QFILE_LIST_ID * input_list_id);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int qfile_get_first_page (THREAD_ENTRY * thread_p,
				 QFILE_LIST_ID * list_id);
#endif

/* Copy routines */
extern int qfile_copy_list_id (QFILE_LIST_ID * dest_list_id,
			       const QFILE_LIST_ID * src_list_id,
			       bool include_sort_list);
extern QFILE_LIST_ID *qfile_clone_list_id (const QFILE_LIST_ID * list_id,
					   bool include_sort_list);

/* Free routines */
extern void qfile_free_list_id (QFILE_LIST_ID * list_id);
extern void qfile_free_sort_list (SORT_LIST * sort_list);

/* Alloc routines */
extern SORT_LIST *qfile_allocate_sort_list (int cnt);

/* sort_list related routines */
extern bool qfile_is_sort_list_covered (SORT_LIST * covering_list,
					SORT_LIST * covered_list);

/* Sorting related routines */
extern SORT_STATUS qfile_make_sort_key (THREAD_ENTRY * thread_p,
					SORTKEY_INFO * info, RECDES * key,
					QFILE_LIST_SCAN_ID * input_scan,
					QFILE_TUPLE_RECORD * tplrec);
extern QFILE_TUPLE qfile_generate_sort_tuple (SORTKEY_INFO * info,
					      SORT_REC * sort_rec,
					      RECDES * output_recdes);
extern int qfile_compare_partial_sort_record (const void *pk0,
					      const void *pk1, void *arg);
extern int qfile_compare_all_sort_record (const void *pk0, const void *pk1,
					  void *arg);
extern int qfile_get_estimated_pages_for_sorting (QFILE_LIST_ID * listid,
						  SORTKEY_INFO * info);
extern SORTKEY_INFO *qfile_initialize_sort_key_info (SORTKEY_INFO * info,
						     SORT_LIST * list,
						     QFILE_TUPLE_VALUE_TYPE_LIST
						     * types);
extern void qfile_clear_sort_key_info (SORTKEY_INFO * info);
extern QFILE_LIST_ID *qfile_sort_list_with_func (THREAD_ENTRY * thread_p,
						 QFILE_LIST_ID * list_id,
						 SORT_LIST * sort_list,
						 QUERY_OPTIONS option,
						 int ls_flag,
						 SORT_GET_FUNC * get_fn,
						 SORT_PUT_FUNC * put_fn,
						 SORT_CMP_FUNC * cmp_fn,
						 void *extra_arg, int limit,
						 bool do_close);
extern QFILE_LIST_ID *qfile_sort_list (THREAD_ENTRY * thread_p,
				       QFILE_LIST_ID * list_id,
				       SORT_LIST * sort_list,
				       QUERY_OPTIONS option, bool do_close);

/* Scan related routines */
#if defined (ENABLE_UNUSED_FUNCTION)
extern int qfile_modify_type_list (QFILE_TUPLE_VALUE_TYPE_LIST * type_list,
				   QFILE_LIST_ID * list_id);
#endif
extern void qfile_clear_list_id (QFILE_LIST_ID * list_id);

extern int qfile_store_xasl (THREAD_ENTRY * thread_p, XASL_STREAM * ctx);


extern int qfile_load_xasl (THREAD_ENTRY * thread_p, const XASL_ID * xasl_id,
			    char **xasl, int *size);
extern void qfile_load_xasl_node_header (THREAD_ENTRY * thread_p,
					 const XASL_ID * xasl_id_p,
					 XASL_NODE_HEADER * xasl_header_p);
extern QFILE_LIST_ID *qfile_open_list (THREAD_ENTRY * thread_p,
				       QFILE_TUPLE_VALUE_TYPE_LIST *
				       type_list, SORT_LIST * sort_list,
				       QUERY_ID query_id, int flag);
extern int qfile_reopen_list_as_append_mode (THREAD_ENTRY * thread_p,
					     QFILE_LIST_ID * list_id_p);
extern int qfile_save_tuple (QFILE_TUPLE_DESCRIPTOR * tuple_descr_p,
			     QFILE_TUPLE_TYPE tuple_type, char *page_p,
			     int *tuple_length_p);
extern int qfile_generate_tuple_into_list (THREAD_ENTRY * thread_p,
					   QFILE_LIST_ID * list_id,
					   QFILE_TUPLE_TYPE tpl_type);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int qfile_fast_intint_tuple_to_list (THREAD_ENTRY * thread_p,
					    QFILE_LIST_ID * list_id_p,
					    int v1, int v2);
extern int qfile_fast_intval_tuple_to_list (THREAD_ENTRY * thread_p,
					    QFILE_LIST_ID * list_id_p,
					    int v1, DB_VALUE * v2);
extern int qfile_fast_val_tuple_to_list (THREAD_ENTRY * thread_p,
					 QFILE_LIST_ID * list_id_p,
					 DB_VALUE * val);
#endif
extern int qfile_add_item_to_list (THREAD_ENTRY * thread_p, char *item,
				   int item_size, QFILE_LIST_ID * list_id);
extern QFILE_LIST_ID *qfile_combine_two_list (THREAD_ENTRY * thread_p,
					      QFILE_LIST_ID * lhs_file,
					      QFILE_LIST_ID * rhs_file,
					      int flag);
extern int qfile_reallocate_tuple (QFILE_TUPLE_RECORD * tplrec, int tpl_size);
#if defined (RYE_DEBUG)
extern void qfile_print_list (THREAD_ENTRY * thread_p,
			      QFILE_LIST_ID * list_id);
#endif
extern int qfile_get_tuple (THREAD_ENTRY * thread_p, PAGE_PTR first_page,
			    QFILE_TUPLE tuplep, QFILE_TUPLE_RECORD * tplrec,
			    QFILE_LIST_ID * list_idp);
extern void qfile_save_current_scan_tuple_position (QFILE_LIST_SCAN_ID * s_id,
						    QFILE_TUPLE_POSITION *
						    ls_tplpos);
extern SCAN_CODE qfile_jump_scan_tuple_position (THREAD_ENTRY * thread_p,
						 QFILE_LIST_SCAN_ID * s_id,
						 QFILE_TUPLE_POSITION *
						 ls_tplpos,
						 QFILE_TUPLE_RECORD * tplrec,
						 int peek);
extern int qfile_start_scan_fix (THREAD_ENTRY * thread_p,
				 QFILE_LIST_SCAN_ID * s_id);
extern int qfile_open_list_scan (QFILE_LIST_ID * list_id,
				 QFILE_LIST_SCAN_ID * s_id);
extern SCAN_CODE qfile_scan_list_next (THREAD_ENTRY * thread_p,
				       QFILE_LIST_SCAN_ID * s_id,
				       QFILE_TUPLE_RECORD * tplrec, int peek);
#if defined (ENABLE_UNUSED_FUNCTION)
extern SCAN_CODE qfile_scan_list_prev (THREAD_ENTRY * thread_p,
				       QFILE_LIST_SCAN_ID * s_id,
				       QFILE_TUPLE_RECORD * tplrec, int peek);
#endif
extern void qfile_end_scan_fix (THREAD_ENTRY * thread_p,
				QFILE_LIST_SCAN_ID * s_id);
extern void qfile_close_scan (THREAD_ENTRY * thread_p,
			      QFILE_LIST_SCAN_ID * s_id);

/* Miscellaneous */
extern QFILE_TUPLE_VALUE_FLAG qfile_locate_tuple_value (QFILE_TUPLE tpl,
							int index,
							char **tpl_val,
							int *val_size);
extern QFILE_TUPLE_VALUE_FLAG qfile_locate_tuple_value_r (QFILE_TUPLE tpl,
							  int index,
							  char **tpl_val,
							  int *val_size);
extern bool qfile_has_next_page (PAGE_PTR page_p);
#endif /* _LIST_FILE_H_ */
