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
 * broker.h -
 */

#ifndef	_BROKER_H_
#define	_BROKER_H_

#ident "$Id$"

#include "cas_common.h"
#include "broker_shm.h"
#include "tcp.h"

#define IS_NORMAL_BROKER_OPCODE(OPCODE)			\
	((OPCODE) >= BRREQ_OP_CODE_MIN && (OPCODE) <= BRREQ_OP_CODE_MAX)

#define IS_LOCAL_MGMT_OPCODE(OPCODE)			\
	((OPCODE) >= BRREQ_OP_CODE_LOCAL_MMGT_MIN && 	\
	 (OPCODE) <= BRREQ_OP_CODE_LOCAL_MGMT_MAX)

#define IS_SHARD_MGMT_OPCODE(OPCODE)			\
	((OPCODE) >= BRREQ_OP_CODE_SHARD_MGMT_MIN &&	\
	 (OPCODE) <= BRREQ_OP_CODE_SHARD_MGMT_MAX)

#define BR_SOCKET_TIMEOUT_SEC      2

#define BR_DEFAULT_WRITE_TIMEOUT	60
#define BR_DEFAULT_READ_TIMEOUT		60

/* structures for mgmt request values */
typedef struct
{
  int node_id;
  char local_dbname[SRV_CON_DBNAME_SIZE];
  char host_ip_str[IP_ADDR_STR_LEN];
  int port;
  in_addr_t host_ip_addr;
  HA_STATE_FOR_DRIVER ha_state;
  char host_name[SHM_NODE_INFO_STR_SIZE];	/* monitoring purpose */
} T_SHARD_NODE_INFO;

typedef struct
{
  T_SHARD_NODE_INFO node_info;
  const char *dba_passwd;
} T_MGMT_REQ_ARG_SHARD_NODE_ADD;

typedef struct
{
  T_SHARD_NODE_INFO node_info;
  const char *dba_passwd;
  int drop_all_nodeid;
} T_MGMT_REQ_ARG_SHARD_NODE_DROP;

typedef struct
{
  int64_t clt_node_ver;
  int64_t clt_groupid_ver;
  int64_t clt_created_at;
} T_MGMT_REQ_ARG_SHARD_INFO;

typedef struct
{
  const char *dba_passwd;
  const char *global_dbname;
  int groupid_count;
  int init_num_node;
  T_SHARD_NODE_INFO *init_node;
} T_MGMT_REQ_ARG_SHARD_INIT;

typedef struct
{
  int mig_groupid;
  int src_nodeid;
  int dest_nodeid;
  int num_shard_keys;
  int timeout_sec;
} T_MGMT_REQ_ARG_SHARD_MIGRATION;

typedef struct
{
  int timeout_sec;
} T_MGMT_REQ_ARG_SHARD_DDL;

typedef struct
{
  int timeout_sec;
} T_MGMT_REQ_ARG_SHARD_GC;

typedef struct
{
  bool empty_node;
  bool ignore_prev_fail;
  const char *dba_passwd;
  int num_src_nodes;
  int num_dest_nodes;
  const int *src_nodeid;
  const int *dest_nodeid;
} T_MGMT_REQ_ARG_REBALANCE_REQ;

typedef struct
{
  int job_type;
} T_MGMT_REQ_ARG_REBALANCE_JOB_COUNT;

typedef struct
{
  const char *local_dbname;
  const char *global_dbname;
  short nodeid;
  int port;
  int64_t nodeid_ver;
  int64_t groupid_ver;
} T_MGMT_REQ_ARG_SHARD_MGMT_INFO;

typedef struct
{
  T_MGMT_LAUNCH_PROCESS_ID launch_process_id;
  int argc;
  int num_env;
  char **argv;
  char **envp;
} T_MGMT_REQ_ARG_LAUNCH_PROCESS;

typedef struct
{
  int which_file;
} T_MGMT_REQ_ARG_READ_RYE_FILE;

typedef struct
{
  int size;
  const char *contents;
} T_MGMT_REQ_ARG_WRITE_RYE_CONF;

typedef struct
{
  const char *proc_name;
  const char *sect_name;
  const char *key;
  const char *value;
} T_MGMT_REQ_ARG_UPDATE_CONF;

typedef struct
{
  const char *proc_name;
  const char *sect_name;
  const char *key;
} T_MGMT_REQ_ARG_GET_CONF;

typedef struct
{
  int size;
  const char *acl;
} T_MGMT_REQ_ARG_BR_ACL_RELOAD;

typedef struct
{
  const char *db_name;
} T_MGMT_REQ_ARG_CONNECT_DB_SERVER;

typedef struct
{
  const char *clt_dbname;
  void *alloc_buffer;
  union
  {
    T_MGMT_REQ_ARG_SHARD_INFO get_info_arg;
    T_MGMT_REQ_ARG_SHARD_INIT init_shard_arg;
    T_MGMT_REQ_ARG_SHARD_NODE_ADD node_add_arg;
    T_MGMT_REQ_ARG_SHARD_NODE_DROP node_drop_arg;
    T_MGMT_REQ_ARG_SHARD_MIGRATION migration_arg;
    T_MGMT_REQ_ARG_SHARD_DDL ddl_arg;
    T_MGMT_REQ_ARG_SHARD_MGMT_INFO shard_mgmt_info;
    T_MGMT_REQ_ARG_REBALANCE_REQ rebalance_req_arg;
    T_MGMT_REQ_ARG_REBALANCE_JOB_COUNT rebalance_job_count;
    T_MGMT_REQ_ARG_LAUNCH_PROCESS launch_process_arg;
    T_MGMT_REQ_ARG_SHARD_GC gc_arg;
    T_MGMT_REQ_ARG_READ_RYE_FILE read_rye_file_arg;
    T_MGMT_REQ_ARG_WRITE_RYE_CONF write_rye_conf_arg;
    T_MGMT_REQ_ARG_UPDATE_CONF update_conf_arg;
    T_MGMT_REQ_ARG_GET_CONF get_conf_arg;
    T_MGMT_REQ_ARG_BR_ACL_RELOAD br_acl_reload_arg;
    T_MGMT_REQ_ARG_CONNECT_DB_SERVER connect_db_server_arg;
    int dummy;
  } value;
} T_MGMT_REQ_ARG;
#define T_MGMT_REQ_ARG_INIT { NULL, NULL, .value.dummy = 0 };

#define MGMT_RESULT_MSG_MAX_SIZE        (ONE_K)
typedef struct
{
  int num_msg;
  int msg_size[BROKER_RESPONSE_MAX_ADDITIONAL_MSG];
  char *msg[BROKER_RESPONSE_MAX_ADDITIONAL_MSG];
  char buf[BROKER_RESPONSE_MAX_ADDITIONAL_MSG][MGMT_RESULT_MSG_MAX_SIZE];
} T_MGMT_RESULT_MSG;

extern SOCKET br_mgmt_accept (in_addr_t * clt_ip_addr);
extern SOCKET br_accept_unix_domain (in_addr_t * clt_ip_addr,
				     struct timeval *mgmt_recv_time,
				     T_BROKER_REQUEST_MSG * br_req_msg);

extern THREAD_FUNC shard_mgmt_receiver_thr_f (void *arg);
extern THREAD_FUNC local_mgmt_receiver_thr_f (void *arg);
extern int local_mgmt_init (void);
extern int shd_mg_init (int shard_mgmt_port, int local_mgmt_port,
			const char *shard_metadb);

extern int br_read_broker_request_msg (SOCKET clt_sock_fd,
				       T_BROKER_REQUEST_MSG * br_req_msg);
extern SOCKET br_connect_srv (bool is_mgmt, const T_BROKER_INFO * br_info,
			      int as_index);
extern void br_send_result_to_client (int sock, int err_code,
				      const T_MGMT_RESULT_MSG * result_msg);
extern int br_write_nbytes_to_client (SOCKET sock_fd, const char *buf,
				      int size, int timeout_sec);
extern int br_read_nbytes_from_client (SOCKET sock_fd, char *buf, int size,
				       int timeout_sec);

extern char *br_mgmt_net_add_int64 (char *net_stream, int64_t value);
extern char *br_mgmt_net_add_int (char *net_stream, int value);
extern char *br_mgmt_net_add_short (char *net_stream, short value);
extern char *br_mgmt_net_add_string (char *net_stream, const char *value);

extern int br_mgmt_get_req_arg (T_MGMT_REQ_ARG * req_arg,
				const T_BROKER_REQUEST_MSG * brreq_msg);

extern void br_mgmt_result_msg_init (T_MGMT_RESULT_MSG * result_msg);
extern void br_mgmt_result_msg_reset (T_MGMT_RESULT_MSG * result_msg);
extern int br_mgmt_result_msg_set (T_MGMT_RESULT_MSG * result_msg,
				   int msg_size, const void *msg);
extern void br_copy_shard_node_info (T_SHARD_NODE_INFO * node, int node_id,
				     const char *dbname, const char *host,
				     int port, in_addr_t host_addr,
				     HA_STATE_FOR_DRIVER ha_state,
				     const char *host_name);
extern void br_set_init_error (int err_code, int os_err_code);

extern int br_Process_flag;
extern int br_Index;
extern T_SHM_BROKER *shm_Br;
extern T_SHM_APPL_SERVER *shm_Appl;

#endif /* _BROKER_H_ */
