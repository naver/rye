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
 * cas_function.h -
 */

#ifndef	_CAS_FUNCTION_H_
#define	_CAS_FUNCTION_H_

#ident "$Id$"

#include "cas_net_buf.h"

typedef enum
{
  FN_KEEP_CONN = 0,
  FN_CLOSE_CONN = -1,
  FN_KEEP_SESS = -2,
  FN_GRACEFUL_DOWN = -3
} FN_RETURN;

typedef FN_RETURN (*T_SERVER_FUNC) (int, void **, T_NET_BUF *, T_REQ_INFO *);

extern FN_RETURN fn_end_tran (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info);

extern FN_RETURN fn_prepare (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info);
extern FN_RETURN fn_execute (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info);
extern FN_RETURN fn_get_db_parameter (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info);
extern FN_RETURN fn_close_req_handle (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info);
extern FN_RETURN fn_fetch (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info);
extern FN_RETURN fn_schema_info (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info);
extern FN_RETURN fn_get_db_version (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info);
extern FN_RETURN fn_execute_batch (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info);
extern FN_RETURN fn_con_close (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info);
extern FN_RETURN fn_check_cas (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info);
extern FN_RETURN fn_get_class_num_objs (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info);
extern FN_RETURN fn_cursor_close (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info);
extern FN_RETURN fn_get_query_plan (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info);
extern FN_RETURN fn_server_mode (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info);

extern FN_RETURN fn_change_dbuser (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info);
extern FN_RETURN fn_update_group_id (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info);
extern FN_RETURN fn_insert_gid_removed_info (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info);
extern FN_RETURN fn_delete_gid_removed_info (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info);
extern FN_RETURN fn_delete_gid_skey_info (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info);
extern FN_RETURN fn_block_globl_dml (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info);
extern FN_RETURN fn_send_repl_data (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info);
extern FN_RETURN fn_notify_ha_agent_state (int argc, void **argv,
                                           T_NET_BUF * net_buf, UNUSED_ARG T_REQ_INFO * req_info);
#endif /* _CAS_FUNCTION_H_ */
