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
 * broker_shm.h -
 */

#ifndef _BROKER_SHM_H_
#define _BROKER_SHM_H_

#ident "$Id$"

#include <sys/types.h>
#include <semaphore.h>

#include "porting.h"
#include "release_string.h"
#include "broker_env_def.h"
#include "broker_config.h"
#include "broker_max_heap.h"
#include "cas_protocol.h"

#include "system_parameter.h"
#include "rye_shm.h"

#define 	STATE_KEEP_TRUE		1
#define		STATE_KEEP_FALSE	0

#define		UTS_STATUS_BUSY		1
#define		UTS_STATUS_IDLE		0
#define		UTS_STATUS_RESTART	2
#define 	UTS_STATUS_START	3
#define         UTS_STATUS_CON_WAIT     5
#define 	UTS_STATUS_STOP		6

#define 	MAX_NUM_UTS_ADMIN	10

#define         DEFAULT_SHM_KEY         0x3f5d1c0a

/* definition for mutex variable */
#define		SHM_MUTEX_BROKER	0
#define		SHM_MUTEX_ADMIN	1

/* con_status lock/unlock */
#define		CON_STATUS_LOCK_BROKER		0
#define		CON_STATUS_LOCK_CAS		1

#define		CON_STATUS_LOCK_INIT(AS_INFO)	\
		  br_sem_init (&((AS_INFO)->con_status_sem));

#define		CON_STATUS_LOCK_DESTROY(AS_INFO)	\
		  br_sem_destroy (&((AS_INFO)->con_status_sem));

#define		CON_STATUS_LOCK(AS_INFO, LOCK_OWNER)	\
		  br_sem_wait (&(AS_INFO)->con_status_sem);

#define		CON_STATUS_UNLOCK(AS_INFO, LOCK_OWNER)	\
		  br_sem_post (&(AS_INFO)->con_status_sem);

#define		SHM_LOG_MSG_SIZE	256

#define		APPL_NAME_LENGTH	128

#define		JOB_QUEUE_MAX_SIZE	511

#define MAX_CRYPT_STR_LENGTH            32

#define APPL_SERVER_NAME_MAX_SIZE	32

#define CAS_LOG_RESET_REOPEN          0x01
#define CAS_LOG_RESET_REMOVE            0x02

#define MAX_CONN_INFO_LENGTH    ((MAXHOSTNAMELEN + 1) * 2)	/* host1:host2 */

#define ACL_IP_BYTE_COUNT           4
#define ACL_MAX_ITEM_COUNT      50
#define ACL_MAX_IP_COUNT        256
#define ACL_MAX_DBNAME_LENGTH   (SRV_CON_DBNAME_SIZE)
#define ACL_MAX_DBUSER_LENGTH   (SRV_CON_DBUSER_SIZE)

#define APPL_SERVER_NUM_LIMIT    2048

#define SHM_BROKER_PATH_MAX      (PATH_MAX)
#define SHM_APPL_SERVER_NAME_MAX (SHM_BROKER_PATH_MAX)

#define UNUSABLE_DATABASE_MAX    (200)
#define PAIR_LIST                (2)

#define MAX_QUERY_TIMEOUT_LIMIT         86400	/* seconds; 1 day */
#define LONG_QUERY_TIME_LIMIT           (MAX_QUERY_TIMEOUT_LIMIT)
#define LONG_TRANSACTION_TIME_LIMIT     (MAX_QUERY_TIMEOUT_LIMIT)

typedef enum
{
  SERVICE_OFF = 0,
  SERVICE_ON = 1,
  SERVICE_OFF_ACK = 2,
  SERVICE_UNKNOWN = 3
} T_BROKER_SERVICE_STATUS;

typedef enum t_con_status T_CON_STATUS;
enum t_con_status
{
  CON_STATUS_OUT_TRAN = 0,
  CON_STATUS_IN_TRAN = 1,
  CON_STATUS_CLOSE = 2,
  CON_STATUS_CLOSE_AND_CONNECT = 3
};

typedef struct acl_ip_info ACL_IP_INFO;
struct acl_ip_info
{
  unsigned char ip_len;
  unsigned char ip_addr[ACL_IP_BYTE_COUNT];
  int ip_last_access_time;
};

typedef struct br_acl_info BR_ACL_INFO;
struct br_acl_info
{
  char dbname[ACL_MAX_DBNAME_LENGTH];
  char dbuser[ACL_MAX_DBUSER_LENGTH];
  int num_acl_ip_info;
  ACL_IP_INFO acl_ip_info[ACL_MAX_IP_COUNT];
};


/* NOTE: Be sure not to include any pointer type in shared memory segment
 * since the processes will not care where the shared memory segment is
 * attached
 */

/* shard mgmt, local mmgt monitoring info */

typedef struct
{
  int num_job;
} T_SHM_MGMT_QUEUE_INFO;

#define SHM_NODE_INFO_STR_SIZE 32
typedef struct
{
  int node_id;
  char local_dbname[SHM_NODE_INFO_STR_SIZE];
  char host_ip[SHM_NODE_INFO_STR_SIZE];
  char host_name[SHM_NODE_INFO_STR_SIZE];
  HA_STATE_FOR_DRIVER ha_state;
  int port;
} T_SHM_SHARD_NODE_INFO;

#define SHM_SHARD_NODE_INFO_MAX		100

typedef struct
{
  int mgmt_req_count;
  int ping_req_count;

  T_SHM_MGMT_QUEUE_INFO get_info_req_queue;
  T_SHM_MGMT_QUEUE_INFO admin_req_queue;
  T_SHM_MGMT_QUEUE_INFO wait_job_req_queue;

  int rbl_scheduled_count;
  int rbl_running_count;
  int rbl_complete_count;
  int rbl_fail_count;
  int rbl_complete_shard_keys;
  float rbl_complete_avg_time;

  int running_migrator_count;
  time_t last_sync_time;
  int num_shard_node_info;
  T_SHM_SHARD_NODE_INFO shard_node_info[SHM_SHARD_NODE_INFO_MAX];
} T_SHM_SHARD_MGMT_INFO;

#define SHM_MAX_CHILD_INFO	20
#define SHM_CHILD_INFO_CMD_LEN	256
typedef struct
{
  int pid;
  unsigned int output_file_id;
  char cmd[SHM_CHILD_INFO_CMD_LEN];
} T_LOCAL_MGMT_CHILD_PROC_INFO;

typedef struct
{
  int connect_req_count;
  int ping_req_count;
  int cancel_req_count;
  int admin_req_count;
  int error_req_count;
  T_SHM_MGMT_QUEUE_INFO admin_req_queue;
  int num_child_process;
  T_LOCAL_MGMT_CHILD_PROC_INFO child_process_info[SHM_MAX_CHILD_INFO];
} T_SHM_LOCAL_MGMT_INFO;

/* appl_server information */
typedef struct t_appl_server_info T_APPL_SERVER_INFO;
struct t_appl_server_info
{
  char cas_log_reset;
  char cas_slow_log_reset;
  char cas_err_log_reset;
  char service_flag;
  char reset_flag;
  char uts_status;		/* flag whether the uts is busy or idle */
  char client_type;
  char service_ready_flag;
  char con_status;
  char cur_keep_con;
  char cur_sql_log_mode;
  char cur_slow_log_mode;
  char cur_statement_pooling;
  char cci_default_autocommit;
  char mutex_turn;
  char mutex_flag[2];		/* for mutex */
  sem_t con_status_sem;

  short as_id;

  unsigned short cas_clt_port;
  in_addr_t cas_clt_ip_addr;

  int num_request;		/* number of request */
  int pid;			/* the process id */
  int psize;
  time_t psize_time;

  time_t last_access_time;	/* last access time */
  time_t transaction_start_time;
  time_t last_connect_time;
  time_t claimed_alive_time;	/* to check if the cas hangs */

  RYE_VERSION clt_version;

  INT64 num_requests_received;
  INT64 num_transactions_processed;
  INT64 num_queries_processed;
  INT64 num_long_queries;
  INT64 num_long_transactions;
  INT64 num_error_queries;
  INT64 num_interrupts;
  INT64 num_connect_requests;
  INT64 num_connect_rejected;
  INT64 num_restarts;

  INT64 num_select_queries;
  INT64 num_insert_queries;
  INT64 num_update_queries;
  INT64 num_delete_queries;
  INT64 num_unique_error_queries;

  int num_holdable_results;
  int cas_change_mode;

  PRM_NODE_INFO db_node;
  char client_version[SRV_CON_VER_STR_MAX_SIZE];
  char log_msg[SHM_LOG_MSG_SIZE];
  char database_name[SRV_CON_DBNAME_SIZE];
  char database_user[SRV_CON_DBUSER_SIZE];
};

/* database server */
typedef struct t_db_server T_DB_SERVER;
struct t_db_server
{
  char database_name[SRV_CON_DBNAME_SIZE];
  PRM_NODE_INFO db_node;
  int server_state;
};

typedef struct t_shm_appl_server T_SHM_APPL_SERVER;
struct t_shm_appl_server
{
  RYE_SHM_HEADER shm_header;	/* should be the first field of shm */
  int shm_key_br_global;
  char access_log;
  char sql_log_mode;
  char broker_log_mode;
  char slow_log_mode;
  char keep_connection;
  char statement_pooling;
  char access_mode;
  char cci_default_autocommit;
  char broker_log_reset;
  char connect_order_random;
  int replica_only_flag;
  int max_num_delayed_hosts_lookup;	/* max num of HA delayed hosts to lookup */

  char log_dir[CONF_LOG_FILE_LEN];
  char broker_name[BROKER_NAME_LEN];
  char appl_server_name[APPL_SERVER_NAME_MAX_SIZE];
  PRM_NODE_LIST preferred_hosts;

  in_addr_t local_ip;

  /* from br_info */
  char source_env[CONF_LOG_FILE_LEN];
  int access_log_max_size;

  int broker_port;
  int appl_server_max_size;
  int appl_server_hard_limit;
  int session_timeout;
  int query_timeout;
  int num_appl_server;
  int max_string_length;
  int job_queue_size;
  int sql_log_max_size;
  int broker_log_max_size;
  int long_query_time;		/* msec */
  int long_transaction_time;	/* msec */
  int max_prepared_stmt_count;
  int num_acl_info;
  int acl_chn;
  int cas_rctime;		/* sec */
  int unusable_databases_cnt[PAIR_LIST];
  unsigned int unusable_databases_seq;
  bool monitor_hang_flag;
  bool monitor_server_flag;
  sem_t acl_sem;

  BR_ACL_INFO acl_info[ACL_MAX_ITEM_COUNT];

  T_MAX_HEAP_NODE job_queue[JOB_QUEUE_MAX_SIZE + 1];

  T_DB_SERVER unusable_databases[PAIR_LIST][UNUSABLE_DATABASE_MAX];

  union
  {
    T_APPL_SERVER_INFO as_info[1];
    T_SHM_SHARD_MGMT_INFO shard_mgmt_info;
    T_SHM_LOCAL_MGMT_INFO local_mgmt_info;
  } info;
};

#define SHM_MAX_SHARD_VERSION_INFO_COUNT        10
#define SHM_BROKER_KEY_LEN                      20

/* shared memory information */

typedef struct t_shm_broker T_SHM_BROKER;
struct t_shm_broker
{
  RYE_SHM_HEADER shm_header;	/* should be the first field of shm */

  char broker_key[SHM_BROKER_KEY_LEN + 1];

  in_addr_t my_ip;
  uid_t owner_uid;
  int num_broker;		/* number of broker */

  int num_shard_version_info;
  struct
  {
    char local_dbname[SRV_CON_DBNAME_SIZE];
    time_t sync_time;
    int64_t shard_info_ver;	/* MAX(nodeid_ver, groupid_ver) */
  } shard_version_info[SHM_MAX_SHARD_VERSION_INFO_COUNT];

  T_BROKER_INFO br_info[1];
};

int br_sem_init (sem_t * sem_t);
int br_sem_wait (sem_t * sem_t);
int br_sem_post (sem_t * sem_t);
int br_sem_destroy (sem_t * sem_t);

T_SHM_BROKER *br_shm_init_shm_broker (int shm_key_br_gl,
				      T_BROKER_INFO * br_info, int br_num);
T_SHM_APPL_SERVER *br_shm_init_shm_as (T_BROKER_INFO * br_info_p,
				       int shm_key_br_gl);

#endif /* _BROKER_SHM_H_ */
