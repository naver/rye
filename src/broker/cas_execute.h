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
 * cas_execute.h -
 */

#ifndef	_CAS_EXECUTE_H_
#define	_CAS_EXECUTE_H_

#ident "$Id$"

#include "cas.h"
#include "cas_net_buf.h"
#include "cas_handle.h"
#include "repl_common.h"

#define ERROR_INFO_SET(ERR_CODE, ERR_INDICATOR)\
	error_info_set(ERR_CODE, ERR_INDICATOR, __FILE__, __LINE__)
#define ERROR_INFO_SET_FORCE(ERR_CODE, ERR_INDICATOR)\
	error_info_set_force(ERR_CODE, ERR_INDICATOR, __FILE__, __LINE__)
#define ERROR_INFO_SET_WITH_MSG(ERR_CODE, ERR_INDICATOR, ERR_MSG)\
	error_info_set_with_msg(ERR_CODE, ERR_INDICATOR, ERR_MSG, false, __FILE__, __LINE__)
#define NET_BUF_ERR_SET(NET_BUF)	\
	err_msg_set(NET_BUF, __FILE__, __LINE__)

extern int ux_check_connection (void);
extern int ux_database_connect (const char *db_name, const char *db_user, const char *db_passwd, char **db_err_msg);
extern int ux_database_reconnect (void);
extern int ux_change_dbuser (const char *user, const char *passwd);
extern int ux_is_database_connected (void);
extern void ux_get_current_database_name (char *buf, int bufsize);
extern int ux_prepare (char *sql_stmt, int flag, char auto_commit_mode,
                       T_NET_BUF * ne_buf, T_REQ_INFO * req_info, unsigned int query_seq_num);
extern int ux_end_tran (int tran_type, bool reset_con_status);
extern int ux_auto_commit (T_NET_BUF * CAS_FN_ARG_NET_BUF, T_REQ_INFO * CAS_FN_ARG_REQ_INFO);
extern int ux_execute (T_SRV_HANDLE * srv_handle, char flag, int max_col_size,
                       int max_row, int argc, void **argv, int group_id, T_NET_BUF *, T_REQ_INFO * req_info);
extern int ux_fetch (T_SRV_HANDLE * srv_handle, int cursor_pos,
                     int fetch_count, int result_set_index, T_NET_BUF * net_buf, T_REQ_INFO * req_info);
extern void ux_database_shutdown (void);
extern int ux_get_db_version (T_NET_BUF * net_buf);
extern int ux_execute_batch (T_SRV_HANDLE * srv_h_id, int num_execution,
                             int num_markers, int group_id, void **bind_argv, T_NET_BUF * net_buf);
extern void ux_cursor_close (T_SRV_HANDLE * srv_handle);

extern int ux_get_query_plan (const char *dump_filename, T_NET_BUF * net_buf);
extern void ux_free_result (void *res);

extern int ux_schema_info (int schema_type, char *table_name,
                           char *column_name, T_NET_BUF * net_buf, unsigned int query_seq_num);

extern bool ux_has_stmt_result_set (const RYE_STMT_TYPE stmt_type);

/*****************************
  cas_error.c function list
 *****************************/
extern void err_msg_set (T_NET_BUF * net_buf, const char *file, int line);
extern int error_info_set (int err_number, int err_indicator, const char *file, int line);
extern int error_info_set_force (int err_number, int err_indicator, const char *file, int line);
extern int error_info_set_with_msg (int err_number, int err_indicator,
                                    const char *err_msg, bool force, const char *file, int line);
extern void error_info_clear (void);
extern void set_server_aborted (bool is_aborted);
extern bool is_server_aborted (void);

/*****************************
  move from cas_log.c
 *****************************/
extern void cas_log_error_handler (unsigned int eid);
extern void cas_log_error_handler_begin (void);
extern void cas_log_error_handler_end (void);
extern void cas_log_error_handler_clear (void);
extern char *cas_log_error_handler_asprint (char *buf, size_t bufsz, bool clear);

extern void set_optimization_level (int level);
extern void reset_optimization_level_as_saved (void);

extern int get_tuple_count (T_SRV_HANDLE * srv_handle);

extern const char *schema_info_str (int schema_type);
extern int dump_repl_data (char *buf, int buf_len, int num_items, void **obj_argv);

extern int ux_update_group_id (int migrator_id, int group_id, int target, int on_off);
extern int ux_insert_gid_removed_info (int group_id);
extern int ux_delete_gid_removed_info (int group_id);
extern int ux_delete_gid_skey_info (int group_id);
extern int ux_block_globl_dml (int start_or_end);
extern int ux_send_repl_data (RP_TRAN_TYPE tran_type, int num_items, void **obj_argv);
#endif /* _CAS_EXECUTE_H_ */
