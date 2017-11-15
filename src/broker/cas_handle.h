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
 * cas_handle.h -
 */

#ifndef	_CAS_HANDLE_H_
#define	_CAS_HANDLE_H_

#ident "$Id$"

#define SRV_HANDLE_QUERY_SEQ_NUM(SRV_HANDLE)    \
        ((SRV_HANDLE) ? (SRV_HANDLE)->query_seq_num : 0)

typedef struct t_query_result T_QUERY_RESULT;
struct t_query_result
{
  DB_QUERY_RESULT *result;
  int copied;
  int tuple_count;
  int num_column;
  char stmt_type;
  bool is_holdable;
};

typedef struct t_srv_handle T_SRV_HANDLE;
struct t_srv_handle
{
  int id;
  void *session;		/* query : DB_SESSION*
				   schema : schema info table pointer */
  T_QUERY_RESULT *q_result;
  char *sql_stmt;
  int num_markers;
  int max_col_size;
  int cursor_pos;
  int schema_type;
  int sch_tuple_num;
  int max_row;
  int num_classes;
  unsigned int query_seq_num;
  char prepare_flag;
  char is_pooled;
  char need_force_commit;
  char auto_commit_mode;
  char scrollable_cursor;
  bool use_plan_cache;
  bool is_fetch_completed;
  bool is_holdable;
  bool is_from_current_transaction;
};

extern int hm_new_srv_handle (T_SRV_HANDLE ** new_handle,
			      unsigned int seq_num);
extern void hm_srv_handle_free (int h_id);
extern void hm_srv_handle_free_all (bool free_holdable);
extern void hm_srv_handle_qresult_end_all (bool end_holdable);
extern T_SRV_HANDLE *hm_find_srv_handle (int h_id);
extern void hm_qresult_clear (T_QUERY_RESULT * q_result);
extern void hm_qresult_end (T_SRV_HANDLE * srv_handle, char free_flag);
extern void hm_session_free (T_SRV_HANDLE * srv_handle);

#endif /* _CAS_HANDLE_H_ */
