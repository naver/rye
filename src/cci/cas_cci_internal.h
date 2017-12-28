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
 * cas_cci_internal.h -
 */

#ifndef	_CAS_CCI_INTERNAL_H_
#define	_CAS_CCI_INTERNAL_H_

#include "cas_cci.h"
#include "repl_page_buffer.h"
#include "cci_handle_mng.h"

#define IS_CCI_NO_ERROR(ERRCODE)	((ERRCODE) >= 0)

#define LAUNCH_RESULT_USERDATA_SIZE	64
typedef struct
{
  char userdata[LAUNCH_RESULT_USERDATA_SIZE];
  int exit_status;
  int stdout_size;
  char stdout_buf[1024];
  int stderr_size;
  char stderr_buf[1024];
} T_CCI_LAUNCH_RESULT;

#define CCI_LAUNCH_RESULT_INITIALIZER	{"", 0, 0, "", 0, "" }

/* client function for replication */
typedef char *(*CCI_OR_PACK_DB_IDXKEY) (char *, DB_IDXKEY *);
typedef bool (*CCI_DB_IDXKEY_IS_NULL) (const DB_IDXKEY *);
typedef int (*CCI_OR_DB_IDXKEY_SIZE) (DB_IDXKEY * key);
typedef char *(*CCI_DB_GET_STRING) (const DB_VALUE *);

CCI_OR_PACK_DB_IDXKEY cci_or_pack_db_idxkey;
CCI_DB_IDXKEY_IS_NULL cci_db_idxkey_is_null;
CCI_OR_DB_IDXKEY_SIZE cci_or_db_idxkey_size;
CCI_DB_GET_STRING cci_db_get_string;

extern int cci_update_db_group_id (CCI_CONN * conn, int migrator_id,
				   int group_id, int target, bool on_off);
extern int cci_insert_gid_removed_info (CCI_CONN * conn, int group_id);
extern int cci_delete_gid_removed_info (CCI_CONN * conn, int group_id);
extern int cci_delete_gid_skey_info (CCI_CONN * conn, int group_id);
extern int cci_block_global_dml (CCI_CONN * conn, bool start_or_end);
extern int cci_execute_with_gid (CCI_STMT * stmt, char flag, int max_col_size,
				 int gid);

extern int cci_shard_get_info (CCI_CONN * conn,
			       CCI_SHARD_NODE_INFO ** node_info,
			       CCI_SHARD_GROUPID_INFO ** groupid_info);
extern int cci_shard_node_info_free (CCI_SHARD_NODE_INFO * ptr);
extern int cci_shard_group_info_free (CCI_SHARD_GROUPID_INFO * ptr);
#if defined(CCI_SHARD_ADMIN)
extern int cci_shard_init (CCI_CONN * conn, int groupid_count,
			   int init_num_nodes, const char **init_nodes);
extern int cci_shard_add_node (CCI_CONN * conn, const char *node_arg);
extern int cci_shard_drop_node (CCI_CONN * conn, const char *node_arg);
extern int cci_shard_ddl_start (CCI_CONN * conn);
extern int cci_shard_ddl_end (CCI_CONN * conn);
#endif
extern int cci_shard_migration_start (CCI_CONN * conn, int groupid,
				      int dest_nodeid);
extern int cci_shard_migration_end (CCI_CONN * conn, int groupid,
				    int dest_nodeid, int num_shard_keys);
extern int cci_shard_gc_start (CCI_CONN * conn);
extern int cci_shard_gc_end (CCI_CONN * conn);
extern int cci_mgmt_sync_shard_mgmt_info (const char *hostname,
					  int mgmt_port,
					  const char *local_dbname,
					  const char *global_dbname,
					  int nodeid,
					  int shard_mgmt_port,
					  int64_t nodeid_version,
					  int64_t groupid_version,
					  char *server_name_buf,
					  int server_name_buf_size,
					  int *server_mode, int timeout_msec);
extern int cci_mgmt_launch_process (T_CCI_LAUNCH_RESULT * launch_result,
				    const char *hostname, int mgmt_port,
				    int launch_proc_id,
				    bool recv_stdout, bool wait_child,
				    int argc, const char **argv, int num_env,
				    const char **envp, int timeout_msec);
extern int cci_mgmt_wait_launch_process (T_CCI_LAUNCH_RESULT * launch_result,
					 int timeout_msec);
extern int cc_mgmt_count_launch_process (void);
extern int cci_mgmt_connect_db_server (const T_HOST_INFO * host,
				       const char *dbname, int timeout_msec);

extern int cci_host_str_to_addr (const char *host_str,
				 unsigned char *ip_addr);
extern int cci_server_shard_nodeid (CCI_CONN * conn);
extern int cci_get_server_mode (CCI_CONN * conn, int *server_mode,
				unsigned int *master_addr);
extern void cci_set_client_functions (CCI_OR_PACK_DB_IDXKEY
				      pack_idxkey_func,
				      CCI_DB_IDXKEY_IS_NULL
				      idxkey_is_null_func,
				      CCI_OR_DB_IDXKEY_SIZE
				      idxkey_size_func,
				      CCI_DB_GET_STRING db_get_string_func);
extern int cci_send_repl_data (CCI_CONN * conn, CIRP_REPL_ITEM * head,
			       int num_items);
extern int cci_notify_ha_agent_state (CCI_CONN * conn, in_addr_t ip,
				      int port, int state);
extern int cci_change_dbuser (CCI_CONN * conn, const char *user,
			      const char *passwd);


#endif /* _CAS_CCI_INTERNAL_H_ */
