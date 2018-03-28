/*
 * Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 *
 * - Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * - Neither the name of the <ORGANIZATION> nor the names of its contributors
 *   may be used to endorse or promote products derived from this software without
 *   specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */


/*
 * cci_network.h -
 */

#ifndef	_CCI_NETWORK_H_
#define	_CCI_NETWORK_H_

#ident "$Id$"

/************************************************************************
 * IMPORTED SYSTEM HEADER FILES						*
 ************************************************************************/

/************************************************************************
 * IMPORTED OTHER HEADER FILES						*
 ************************************************************************/

#include "cas_cci_internal.h"
#include "cci_handle_mng.h"

/************************************************************************
 * EXPORTED DEFINITIONS							*
 ************************************************************************/

#ifndef MIN
#define MIN(X, Y)	((X) < (Y) ? (X) : (Y))
#endif

#ifndef MAX
#define MAX(X, Y)	((X) > (Y) ? (X) : (Y))
#endif

#define CAS_ERROR_INDICATOR     -1
#define DBMS_ERROR_INDICATOR    -2

#define CAS_PROTOCOL_ERR_INDICATOR_SIZE     sizeof(int)
#define CAS_PROTOCOL_ERR_CODE_SIZE          sizeof(int)

#define CAS_PROTOCOL_ERR_INDICATOR_INDEX    0
#define CAS_PROTOCOL_ERR_CODE_INDEX         (CAS_PROTOCOL_ERR_INDICATOR_SIZE)
#define CAS_PROTOCOL_ERR_MSG_INDEX          (CAS_PROTOCOL_ERR_INDICATOR_SIZE + CAS_PROTOCOL_ERR_CODE_SIZE)

#define BROKER_HEALTH_CHECK_TIMEOUT	5000

#define	NET_RES_MSG_PTR(PTR, RESULT_MSG, RESULT_MSG_SIZE)	\
	do {							\
	  (PTR) = (RESULT_MSG) + NET_SIZE_BYTE;			\
	  (RESULT_MSG_SIZE) -= NET_SIZE_BYTE; 			\
	} while (0)

/************************************************************************
 * EXPORTED TYPE DEFINITIONS						*
 ************************************************************************/
typedef struct
{
  int *msg_body_size_ptr;
  char *info_ptr;
  char buf[MSG_HEADER_SIZE];
} MSG_HEADER;

typedef struct
{
  char *cur_p;
  int result_size;
  char buffer[1];
} T_NET_RES;

/************************************************************************
 * EXPORTED FUNCTION PROTOTYPES						*
 ************************************************************************/

extern int net_connect_srv (T_CON_HANDLE * con_handle, int host_id, int login_timeout);
extern int net_send_msg (const T_CON_HANDLE * con_handle, char *msg, int size);
extern int net_recv_msg (T_CON_HANDLE * con_handle, T_NET_RES ** net_res);
extern int net_recv_msg_timeout (T_CON_HANDLE * con_handle, T_NET_RES ** net_res, int timeout);
extern int net_cancel_request (const T_CON_HANDLE * con_handle);
extern int net_check_cas_request (T_CON_HANDLE * con_handle);
extern bool net_check_broker_alive (const T_HOST_INFO * host, const char *port_name, int timeout_msec);

extern int net_res_to_byte (char *value, T_NET_RES * net_res);
extern int net_res_to_int (int *value, T_NET_RES * net_res);
extern int net_res_to_short (short *value, T_NET_RES * net_res);
extern int net_res_to_str (char **str_ptr, int *str_size, T_NET_RES * net_res);
extern char *net_res_cur_ptr (T_NET_RES * net_res);

extern int net_shard_get_info (const T_HOST_INFO * host,
                               const char *dbname, int64_t node_version,
                               int64_t gid_version, int64_t clt_created_at,
                               int timeout_msec,
                               CCI_SHARD_NODE_INFO ** node_info,
                               CCI_SHARD_GROUPID_INFO ** groupid_info, int64_t * created_at);
extern int net_shard_init (int groupid_count, int init_num_nodes,
                           const char **init_nodes, const T_CON_HANDLE * con_handle);
extern int net_shard_node_req (char opcode, const char *node_arg, const T_CON_HANDLE * con_handle);
extern int net_shard_migration_req (char opcode, SOCKET * sock_fd,
                                    int groupid, int dest_nodeid, int num_shard_keys, const T_CON_HANDLE * con_handle);
extern int net_shard_ddl_gc_req (char opcode, SOCKET * sock_fd, const T_CON_HANDLE * con_handle);
extern int net_mgmt_shard_mgmt_info_req (const T_HOST_INFO * host,
                                         const char *local_dbname,
                                         const char *global_dbname,
                                         int nodeid, int port,
                                         int64_t nodeid_ver,
                                         int64_t groupid_ver,
                                         char *server_name_buf,
                                         int server_name_buf_size, int *server_mode, int timeout_msec);
extern int net_mgmt_launch_process_req (T_CCI_LAUNCH_RESULT * launch_result,
                                        const T_HOST_INFO * host,
                                        int launch_proc,
                                        bool recv_stdout, bool wait_child,
                                        int argc, const char **argv, int num_env, const char **envp, int timeout_msec);
extern int net_mgmt_count_launch_process (void);
extern int net_mgmt_wait_launch_process (T_CCI_LAUNCH_RESULT * launch_res, int poll_timeout);
extern int net_mgmt_connect_db_server (const T_HOST_INFO * host, const char *dbname, int timeout_msec);


/************************************************************************
 * EXPORTED VARIABLES							*
 ************************************************************************/


#endif /* _CCI_NETWORK_H_ */
