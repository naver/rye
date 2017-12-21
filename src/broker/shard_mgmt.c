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
 * shard_mgmt.c
 */

#ident "$Id$"

#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <arpa/inet.h>
#include <assert.h>
#include <sys/ioctl.h>
#include <poll.h>
#include <signal.h>

#include "cas_common.h"
#include "cas_protocol.h"
#include "cas_error.h"
#include "broker_config.h"
#include "cas_cci_internal.h"
#include "broker_log.h"
#include "broker.h"
#include "broker_util.h"

#include "connection_defs.h"

#define LOCAL_MGMT_REQ_TIMEOUT_MSEC	(BR_DEFAULT_WRITE_TIMEOUT * 1000)
#define DBNAME_MAX_SIZE		64
#define CCI_QUERY_TIMEOUT	5000

#define MIGRATOR_START_WAIT_TIMEOUT_MSEC	(60 * 1000)

#define COPY_GLOBAL_DBNAME(BUF)					\
    do {							\
      assert (db_Shard_info.locked);				\
      assert (sizeof (BUF) >= DBNAME_MAX_SIZE);			\
      STRNCPY (BUF, db_Sync_info.shard_db_rec.global_dbname,	\
	       DBNAME_MAX_SIZE);				\
    } while (0)

#define SHARD_DB_CHECK_INTERVAL			3
#define SHARD_DB_RESET_CHECK_TIME()			\
	do {						\
	  db_Sync_info.last_check_time = 0;		\
	} while (0)


#define APPEND_QUERY_STR(PTR, ...)		\
	do {					\
	  int n;				\
	  n = sprintf (PTR, __VA_ARGS__);	\
	  PTR += n;				\
	} while (0)

#define SQL_BUF_SIZE		1024
#define MAX_COLUMNS		10
#define MAX_BIND_VALUES		10

#define TABLE_SHARD_DB			"shard_db"
#define COL_SHARD_DB_ID			"id"
#define COL_SHARD_DB_DB_NAME		"dbname"
#define COL_SHARD_DB_GROUPID_COUNT	"groupid_count"
#define COL_SHARD_DB_GROUPID_LAST_VER	"groupid_last_ver"
#define COL_SHARD_DB_NODE_LAST_VER	"node_last_ver"
#define COL_SHARD_DB_MIG_REQ_COUNT	"mig_req_count"
#define COL_SHARD_DB_DDL_REQ_COUNT	"ddl_req_count"
#define COL_SHARD_DB_GC_REQ_COUNT	"gc_req_count"
#define COL_SHARD_DB_NODE_STATUS	"node_status"
#define COL_SHARD_DB_CREATED_AT		"created_at"
#define COL_COUNT_SHARD_DB		10

#define SHARD_DB_ID_VALUE		1

typedef enum
{
  SHARD_DB_NODE_STATUS_ALL_VALID = 0,
  SHARD_DB_NODE_STATUS_EXIST_INVALID = 1
} SHARD_DB_NODE_STATUS;

#define TABLE_SHARD_NODE		"shard_node"
#define COL_SHARD_NODE_NODEID		"nodeid"
#define COL_SHARD_NODE_DBNAME		"dbname"
#define COL_SHARD_NODE_HOST		"host"
#define COL_SHARD_NODE_PORT		"port"
#define COL_SHARD_NODE_STATUS		"status"
#define COL_SHARD_NODE_VERSION		"version"
#define COL_COUNT_SHARD_NODE		6

typedef enum
{
  ADD_NODE_STATUS_REQUESTED = 2,
  ADD_NODE_STATUS_SCHEMA_COMPLETE = 1,
  ADD_NODE_STATUS_COMPLETE = 0
} ADD_NODE_STATUS;

#define TABLE_SHARD_GROUPID		"shard_groupid"
#define COL_SHARD_GROUPID_GROUPID	"groupid"
#define COL_SHARD_GROUPID_NODEID	"nodeid"
#define COL_SHARD_GROUPID_VERSION	"version"
#define COL_COUNT_SHARD_GROUPID		3

#define TABLE_SHARD_MIGRATION		"shard_migration"
#define COL_SHARD_MIGRATION_GROUPID	"groupid"
#define COL_SHARD_MIGRATION_MGMT_HOST	"mgmt_host"
#define COL_SHARD_MIGRATION_MGMT_PID	"mgmt_pid"
#define COL_SHARD_MIGRATION_SRC_NODEID	"src_nodeid"
#define COL_SHARD_MIGRATION_DEST_NODEID	"dest_nodeid"
#define COL_SHARD_MIGRATION_STATUS	"status"
#define COL_SHARD_MIGRATION_MIG_ORDER	"mig_order"
#define COL_SHARD_MIGRATION_MODIFIED_AT	"modified_at"
#define COL_SHARD_MIGRATION_RUN_TIME	"run_time"
#define COL_SHARD_MIGRATION_SHARD_KEYS	"shard_keys"
#define COL_COUNT_SHARD_MIGRATION	10

typedef enum
{
  GROUP_MIGRATION_STATUS_UNKNOWN = 0,
  GROUP_MIGRATION_STATUS_SCHEDULED = 1,
  GROUP_MIGRATION_STATUS_MIGRATOR_RUN = 2,
  GROUP_MIGRATION_STATUS_MIGRATION_STARTED = 3,
  GROUP_MIGRATION_STATUS_COMPLETE = 4,
  GROUP_MIGRATION_STATUS_FAILED = 5
} T_GROUP_MIGRATION_STATUS;

typedef enum
{
  COLUMN_TYPE_INT = 0,
  COLUMN_TYPE_STRING,
  COLUMN_TYPE_BIGINT,
  COLUMN_TYPE_DATETIME
} T_COLUMN_TYPE;

typedef struct
{
  const char *table_name;
  int num_columns;
  struct
  {
    const char *name;
    T_COLUMN_TYPE type;
    bool is_pk;
  } columns[MAX_COLUMNS];
} T_TABLE_DEF;

T_TABLE_DEF table_Def_shard_db = {
  TABLE_SHARD_DB, COL_COUNT_SHARD_DB,
  {{COL_SHARD_DB_ID, COLUMN_TYPE_INT, true},
   {COL_SHARD_DB_DB_NAME, COLUMN_TYPE_STRING, false},
   {COL_SHARD_DB_GROUPID_COUNT, COLUMN_TYPE_INT, false},
   {COL_SHARD_DB_GROUPID_LAST_VER, COLUMN_TYPE_BIGINT, false},
   {COL_SHARD_DB_NODE_LAST_VER, COLUMN_TYPE_BIGINT, false},
   {COL_SHARD_DB_MIG_REQ_COUNT, COLUMN_TYPE_INT, false},
   {COL_SHARD_DB_DDL_REQ_COUNT, COLUMN_TYPE_INT, false},
   {COL_SHARD_DB_GC_REQ_COUNT, COLUMN_TYPE_INT, false},
   {COL_SHARD_DB_NODE_STATUS, COLUMN_TYPE_INT, false},
   {COL_SHARD_DB_CREATED_AT, COLUMN_TYPE_BIGINT, false}}
};

T_TABLE_DEF table_Def_shard_node = {
  TABLE_SHARD_NODE, COL_COUNT_SHARD_NODE,
  {{COL_SHARD_NODE_NODEID, COLUMN_TYPE_INT, true},
   {COL_SHARD_NODE_DBNAME, COLUMN_TYPE_STRING, true},
   {COL_SHARD_NODE_HOST, COLUMN_TYPE_STRING, true},
   {COL_SHARD_NODE_PORT, COLUMN_TYPE_INT, true},
   {COL_SHARD_NODE_STATUS, COLUMN_TYPE_INT, false},
   {COL_SHARD_NODE_VERSION, COLUMN_TYPE_BIGINT, false}}
};

T_TABLE_DEF table_Def_shard_groupid = {
  TABLE_SHARD_GROUPID, COL_COUNT_SHARD_GROUPID,
  {{COL_SHARD_GROUPID_GROUPID, COLUMN_TYPE_INT, true},
   {COL_SHARD_GROUPID_NODEID, COLUMN_TYPE_INT, false},
   {COL_SHARD_GROUPID_VERSION, COLUMN_TYPE_BIGINT, false}}
};

T_TABLE_DEF table_Def_shard_migration = {
  TABLE_SHARD_MIGRATION, COL_COUNT_SHARD_MIGRATION,
  {{COL_SHARD_MIGRATION_GROUPID, COLUMN_TYPE_INT, true},
   {COL_SHARD_MIGRATION_MGMT_HOST, COLUMN_TYPE_STRING, false},
   {COL_SHARD_MIGRATION_MGMT_PID, COLUMN_TYPE_INT, false},
   {COL_SHARD_MIGRATION_SRC_NODEID, COLUMN_TYPE_INT, false},
   {COL_SHARD_MIGRATION_DEST_NODEID, COLUMN_TYPE_INT, false},
   {COL_SHARD_MIGRATION_STATUS, COLUMN_TYPE_INT, false},
   {COL_SHARD_MIGRATION_MIG_ORDER, COLUMN_TYPE_INT, false},
   {COL_SHARD_MIGRATION_MODIFIED_AT, COLUMN_TYPE_BIGINT, false},
   {COL_SHARD_MIGRATION_RUN_TIME, COLUMN_TYPE_INT, false},
   {COL_SHARD_MIGRATION_SHARD_KEYS, COLUMN_TYPE_INT, false}}
};

typedef struct
{
  bool is_unique;
  const char *index_name;
  const char *table_name;
  const char *column_name[MAX_COLUMNS];
} T_INDEX_DEF;

T_INDEX_DEF shard_indexes[] = {
  {false, "shard_migration_idx1", TABLE_SHARD_MIGRATION,
   {COL_SHARD_MIGRATION_SRC_NODEID, COL_SHARD_MIGRATION_STATUS,
    COL_SHARD_MIGRATION_MIG_ORDER, COL_SHARD_MIGRATION_DEST_NODEID, NULL}}
};

#define LOCK_DB_SHARD_INFO()					\
	do {							\
	  pthread_mutex_lock (&db_Shard_info.mutex);		\
	  assert (!db_Shard_info.locked);			\
	  db_Shard_info.locked = true;				\
	} while (0)

#define UNLOCK_DB_SHARD_INFO()					\
	do {							\
	  assert (db_Shard_info.locked);			\
	  db_Shard_info.locked = false;				\
	  pthread_mutex_unlock (&db_Shard_info.mutex);		\
	} while (0)

#define BR_LOG_CCI_ERROR(COMMAND, ERR_CODE, ERR_MSG, QUERY)		\
	do {								\
	  const char *_tmp_query = QUERY;				\
	  br_log_write (BROKER_LOG_ERROR, INADDR_NONE,			\
	  		"METADB error: %s: %d, %s -- %s",		\
			COMMAND, ERR_CODE, ERR_MSG, 			\
			(_tmp_query == NULL ? "" : _tmp_query));	\
	} while (0)

/* type definitions */
typedef enum
{
  JOB_QUEUE_TYPE_GET_INFO = 0,
  JOB_QUEUE_TYPE_ADMIN = 1,
  JOB_QUEUE_TYPE_WAIT_JOB = 2,
  JOB_QUEUE_TYPE_SYNC_LOCAL_MGMT = 3,
  JOB_QUEUE_TYPE_MIGRATION_HANDLER = 4,
  JOB_QUEUE_TYPE_MIGRATOR_WAITER = 5,
  NUM_SHARD_JOB_QUEUE
} T_JOB_QUEUE_TYPE;

typedef enum
{
  THR_FUNC_COMPLETE = 1,
  THR_FUNC_COMPLETE_AND_WAIT_NEXT = 2,
  THR_FUNC_NEED_QUEUEING = 3
} T_THR_FUNC_RES;

typedef struct
{
  int (*func) (CCI_CONN *, const void *);
  void *arg;
  unsigned char next_req_opcode;
} T_SHD_MG_COMPENSATION_JOB;

typedef struct shard_mgmt_job T_SHARD_MGMT_JOB;
struct shard_mgmt_job
{
  bool is_retry;
  int clt_sock_fd;
  time_t request_time;
  in_addr_t clt_ip_addr;
  T_BROKER_REQUEST_MSG *req_msg;
  T_SHD_MG_COMPENSATION_JOB *compensation_job;
  struct shard_mgmt_job *next;
};

typedef struct shard_mgmt_job_queue T_SHARD_JOB_QUEUE;
struct shard_mgmt_job_queue
{
  T_JOB_QUEUE_TYPE job_queue_type;
  int num_workers;
  pthread_mutex_t lock;
  pthread_cond_t cond;
    THREAD_FUNC (*thr_func) (void *);
    T_THR_FUNC_RES (*job_func) (const T_SHARD_MGMT_JOB *,
				T_SHD_MG_COMPENSATION_JOB **);
  T_SHARD_JOB_QUEUE *secondary_job_queue;
  T_SHARD_MGMT_JOB *front;
  T_SHARD_MGMT_JOB *back;
  int num_job;
  T_SHM_MGMT_QUEUE_INFO *shm_queue_info;
};

typedef struct
{
  bool is_running;
  T_SHARD_JOB_QUEUE *job_queue;
} T_SHARD_MGMT_WORKER_ARG;

/* structure for db sync */

typedef struct
{
  const char *global_dbname;
  int groupid_count;
  int64_t gid_info_last_ver;
  int64_t node_info_last_ver;
  int mig_req_count;
  int ddl_req_count;
  int gc_req_count;
  int node_status;
  int64_t created_at;
  char *buf;
} T_SHARD_DB_REC;		/* shard_db table record */

#define SHARD_DB_REC_INIT_VALUE { NULL, 0, 1, 0, 0, 0, 0, 		\
				  SHARD_DB_NODE_STATUS_ALL_VALID, 0, NULL}

typedef struct
{
  T_SHARD_DB_REC shard_db_rec;

  time_t last_check_time;
  pthread_mutex_t mutex;
} T_SYNC_INFO;

typedef struct
{
  char *buf;
  int size;
} T_INFO_NET_STREAM;

typedef struct
{
  int gid;
  short nodeid;
  int64_t ver;
} T_GROUPID_INFO;

typedef struct
{
  int64_t gid_info_ver;
  T_INFO_NET_STREAM net_stream_all_info;
  T_INFO_NET_STREAM net_stream_partial_info;
  int64_t net_stream_partial_info_clt_ver;
  int group_id_count;
  T_GROUPID_INFO gid_info[1];
} T_DB_GROUPID_INFO;

typedef struct
{
  int64_t node_info_ver;
  T_INFO_NET_STREAM net_stream_node_info;
  T_INFO_NET_STREAM net_stream_node_state;
  int node_info_count;
  T_SHARD_NODE_INFO node_info[1];
} T_DB_NODE_INFO;

typedef struct
{
  T_DB_GROUPID_INFO *db_groupid_info;
  T_DB_NODE_INFO *db_node_info;
  pthread_mutex_t mutex;
  bool locked;			/* for debug */
} T_DB_SHARD_INFO;

typedef struct
{
  int index;
  T_CCI_TYPE a_type;
  const void *value;
  char flag;
} T_BIND_PARAM;

typedef struct
{
  char sql[SQL_BUF_SIZE];
  bool check_affected_rows;
  int num_bind;
  T_BIND_PARAM bind_param[MAX_BIND_VALUES];
} T_SQL_AND_PARAM;

#define SET_BIND_PARAM(INDEX, BIND_PARAM_ARR, A_TYPE, VALUE)		\
	do {								\
	  int _tmp_idx = (INDEX);					\
	  T_BIND_PARAM *_tmp_param_arr = (BIND_PARAM_ARR);		\
	  _tmp_param_arr[_tmp_idx].index = _tmp_idx + 1;		\
	  _tmp_param_arr[_tmp_idx].a_type = A_TYPE;			\
	  _tmp_param_arr[_tmp_idx].value = VALUE;			\
	  _tmp_param_arr[_tmp_idx].flag = 0;				\
	} while (0)

typedef struct
{
  T_DB_NODE_INFO *db_node_info;
  int64_t groupid_version;
  char *global_dbname;
  time_t last_sync_time;
} T_LOCAL_MGMT_SYNC_INFO;

typedef enum
{
  RBL_NODE_TYPE_UNKNOWN = 0,
  RBL_NODE_TYPE_SOURCE = 1,
  RBL_NODE_TYPE_DEST = 2
} T_RBL_NODE_TYPE;

typedef struct
{
  int nodeid;
  int count;
  T_RBL_NODE_TYPE type;
  float avail_count;
  int mig_count;
} T_REBALANCE_AMOUNT;

typedef struct
{
  int groupid;
  int src_nodeid;
  int dest_nodeid;
  int mig_order;
} T_GROUP_MIGRATION_INFO;

typedef struct
{
  int src_nodeid;
  int count_scheduled;
  int count_migrator_run;
  int count_migrator_expired;
  int count_mig_started;
  int count_complete;
  int count_failed;
  int count_all;
  int run_migration;
} T_MIGRATION_STATS;

/* static functions */

static int shd_mg_init_sync_data (void);
static int shd_mg_init_job_queue (void);
static int shd_mg_init_worker (void);

static THREAD_FUNC shd_mg_worker_thr (void *arg);
static THREAD_FUNC shd_mg_sync_local_mgmt_thr (void *arg);
static THREAD_FUNC shd_mg_migration_handler_thr (void *arg);
static THREAD_FUNC shd_mg_migrator_wait_thr (void *arg);
static void shd_mg_migration_handler_wakeup (void);
static int read_next_request (T_SHARD_MGMT_JOB * job);
static int do_compensation_job (T_SHD_MG_COMPENSATION_JOB * compensation_job);

static T_THR_FUNC_RES shd_mg_func_shard_info (const T_SHARD_MGMT_JOB *,
					      T_SHD_MG_COMPENSATION_JOB **);
static T_THR_FUNC_RES shd_mg_func_admin (const T_SHARD_MGMT_JOB * job,
					 T_SHD_MG_COMPENSATION_JOB **);
static void shd_mg_func_sync_local_mgmt (T_LOCAL_MGMT_SYNC_INFO * sync_info,
					 bool force);

static int set_admin_compensation_job (T_SHD_MG_COMPENSATION_JOB ** out,
				       const T_SHARD_MGMT_JOB * job,
				       const T_MGMT_REQ_ARG * req_arg);
static int admin_shard_init_command (T_THR_FUNC_RES * thr_func_res,
				     CCI_CONN *, const T_SHARD_DB_REC *,
				     const T_MGMT_REQ_ARG *,
				     const T_SHARD_MGMT_JOB * job);
static int shd_mg_node_add (T_THR_FUNC_RES * thr_func_res, CCI_CONN *,
			    const T_SHARD_DB_REC *, const T_MGMT_REQ_ARG *,
			    const T_SHARD_MGMT_JOB * job);
static int shd_mg_node_drop (T_THR_FUNC_RES * thr_func_res, CCI_CONN *,
			     const T_SHARD_DB_REC *, const T_MGMT_REQ_ARG *,
			     const T_SHARD_MGMT_JOB * job);
static int shd_mg_migration_start (T_THR_FUNC_RES * thr_func_res, CCI_CONN *,
				   const T_SHARD_DB_REC *,
				   const T_MGMT_REQ_ARG *,
				   const T_SHARD_MGMT_JOB * job);
static int shd_mg_migration_end (T_THR_FUNC_RES * thr_func_res,
				 CCI_CONN * conn,
				 const T_SHARD_DB_REC * shard_db_rec,
				 const T_MGMT_REQ_ARG * req_arg,
				 const T_SHARD_MGMT_JOB * job);
static int shd_mg_migration_fail (CCI_CONN * conn, const void *arg);
static int shd_mg_migration_end_internal (CCI_CONN * conn,
					  const T_MGMT_REQ_ARG_SHARD_MIGRATION
					  * mig_arg, int64_t next_gid_ver);
static int shd_mg_ddl_start (T_THR_FUNC_RES * thr_func_res, CCI_CONN * conn,
			     const T_SHARD_DB_REC * shard_db_rec,
			     const T_MGMT_REQ_ARG * req_arg,
			     const T_SHARD_MGMT_JOB * job);
static int shd_mg_ddl_end (T_THR_FUNC_RES * thr_func_res, CCI_CONN * conn,
			   const T_SHARD_DB_REC * shard_db_rec,
			   const T_MGMT_REQ_ARG * req_arg,
			   const T_SHARD_MGMT_JOB * job);
static int shd_mg_gc_start (T_THR_FUNC_RES * thr_func_res, CCI_CONN * conn,
			    const T_SHARD_DB_REC * shard_db_rec,
			    const T_MGMT_REQ_ARG * req_arg,
			    const T_SHARD_MGMT_JOB * job);
static int shd_mg_gc_end (T_THR_FUNC_RES * thr_func_res, CCI_CONN * conn,
			  const T_SHARD_DB_REC * shard_db_rec,
			  const T_MGMT_REQ_ARG * req_arg,
			  const T_SHARD_MGMT_JOB * job);
static int shd_mg_rebalance_req (T_THR_FUNC_RES * thr_func_res,
				 CCI_CONN * conn,
				 const T_SHARD_DB_REC * shard_db_rec,
				 const T_MGMT_REQ_ARG * req_arg,
				 const T_SHARD_MGMT_JOB * job);
static int shd_mg_rebalance_job_count (T_THR_FUNC_RES * thr_func_res,
				       CCI_CONN * conn,
				       const T_SHARD_DB_REC * shard_db_rec,
				       const T_MGMT_REQ_ARG * req_arg,
				       const T_SHARD_MGMT_JOB * job);
static int shd_mg_ddl_fail (CCI_CONN * conn, const void *arg);
static int shd_mg_ddl_end_internal (CCI_CONN * conn);
static int shd_mg_gc_fail (CCI_CONN * conn, const void *arg);
static int shd_mg_gc_end_internal (CCI_CONN * conn);

static void set_migrator_dba_passwd (const char *dba_passwd);
static bool is_expired_request (time_t request_time, int timeout_sec);
static int change_shrad_db_node_status (CCI_CONN * conn, int node_status);
static int node_add_commit_and_migration (CCI_CONN * conn,
					  bool * commit_success,
					  const char *global_dbname,
					  int src_nodeid,
					  const T_SHARD_NODE_INFO * dest_node,
					  const T_SHARD_NODE_INFO *
					  run_node_info,
					  bool schema_migration,
					  bool is_slave_mode_mig,
					  T_SHARD_DB_REC * new_shard_db_rec);
static int delete_shard_node_table (CCI_CONN * conn,
				    const T_SHARD_NODE_INFO * drop_node,
				    int *drop_all_nodeid,
				    int64_t next_node_ver);
static int update_shard_node_table_status (CCI_CONN * conn,
					   const T_SHARD_NODE_INFO *
					   node_info, int node_status);
static int insert_shard_node_table (CCI_CONN * conn,
				    const T_SHARD_NODE_INFO * add_node,
				    int node_status, int64_t next_node_ver);
static int insert_shard_groupid_table (CCI_CONN * conn, int all_groupid_count,
				       int init_num_node,
				       const int *init_nodeid_arr,
				       int64_t init_gid_ver);

static void shd_job_add_internal (T_SHARD_JOB_QUEUE * job_queue,
				  T_SHARD_MGMT_JOB * job, bool is_retry);
static int shd_mg_sync_local_mgmt_job_add (void);
static int shd_mg_job_queue_add (int clt_sock_fd,
				 in_addr_t clt_ip_addr,
				 const T_BROKER_REQUEST_MSG * req_msg);
static T_SHARD_MGMT_JOB *shd_mg_job_queue_remove_all (T_SHARD_JOB_QUEUE *
						      job_queue);
static void set_job_queue_job_count (T_SHARD_JOB_QUEUE * job_queue,
				     int num_job);
static void free_mgmt_job (T_SHARD_MGMT_JOB * job);
static T_SHARD_MGMT_JOB *make_new_mgmt_job (int clt_sock_fd,
					    in_addr_t clt_ip_addr,
					    const T_BROKER_REQUEST_MSG *
					    req_msg);
static T_SHARD_JOB_QUEUE *find_shard_job_queue (T_JOB_QUEUE_TYPE
						job_queue_type);

static int sync_node_info (CCI_CONN * conn, T_SHARD_DB_REC * shard_db_rec,
			   bool admin_mode);
static int sync_groupid_info (CCI_CONN * conn,
			      T_SHARD_DB_REC * shard_db_rec, bool admin_mode);
static void get_info_last_ver (CCI_CONN * conn,
			       T_SHARD_DB_REC * shard_db_rec,
			       bool admin_mode);

static int connect_metadb (CCI_CONN * conn, int cci_autocommit,
			   const char *db_user, const char *db_passwd);
static int select_all_db_info (CCI_CONN * conn_arg);
static T_DB_GROUPID_INFO *select_all_gid_info (CCI_CONN * conn_arg);
static T_DB_NODE_INFO *select_all_node_info (CCI_CONN * conn_arg);
static T_SHARD_NODE_INFO *find_node_info (T_DB_NODE_INFO * db_node_info,
					  const T_SHARD_NODE_INFO * find_node,
					  bool * exist_same_node);
static int find_min_node_id (T_DB_NODE_INFO * db_node_info);
static int check_nodeid_in_use (int nodeid,
				const T_DB_GROUPID_INFO * db_groupid_info);

static T_DB_NODE_INFO *db_node_info_alloc (int node_info_count);
static void db_node_info_free (T_DB_NODE_INFO * node_info);
static int clone_db_node_info (T_DB_NODE_INFO ** ret_db_node_info,
			       const T_DB_NODE_INFO * src_db_node_info);
static T_DB_GROUPID_INFO *db_groupid_info_alloc (int gid_info_count);
static void db_groupid_info_free (T_DB_GROUPID_INFO * db_groupid_info);

static void send_info_msg_to_client (int sock_fd, int err_code,
				     const char *shard_info_hdr,
				     int shard_into_hdr_size,
				     const char *node_info,
				     int node_info_size,
				     const char *groupid_info,
				     int groupid_info_size,
				     const char *node_state,
				     int node_state_size);
static T_INFO_NET_STREAM *make_node_info_net_stream (T_DB_NODE_INFO *);
static T_INFO_NET_STREAM *make_node_state_net_stream (T_DB_NODE_INFO *);
static T_INFO_NET_STREAM *make_groupid_info_net_stream (T_DB_GROUPID_INFO *,
							int64_t clt_ver);

static int admin_query_execute_array (CCI_CONN * conn, int num_sql,
				      const T_SQL_AND_PARAM * sql_and_param);
static int admin_query_execute (CCI_CONN * conn, bool do_prepare,
				CCI_STMT * stmt, const char *sql,
				int num_bind,
				const T_BIND_PARAM * bind_param);

static void make_query_create_global_table (T_SQL_AND_PARAM * sql_and_param,
					    const T_TABLE_DEF * table);
static void make_query_create_index (T_SQL_AND_PARAM * sql_and_param,
				     const T_INDEX_DEF * index_def);
static void make_query_create_user (T_SQL_AND_PARAM * sql_and_param);
static void make_query_change_owner (T_SQL_AND_PARAM * sql_and_param,
				     const T_TABLE_DEF * table);
static void make_query_node_drop (T_SQL_AND_PARAM * sql_and_param,
				  const T_SHARD_NODE_INFO * node_info);
static void make_query_node_drop_all (T_SQL_AND_PARAM * sql_and_param,
				      int *nodeid);
static void make_query_insert_shard_db (T_SQL_AND_PARAM * sql_and_param,
					const int *shard_db_id,
					const T_SHARD_DB_REC * shard_db_rec);
static void make_query_update_groupid (T_SQL_AND_PARAM * sql_and_param,
				       const int *groupid, const int *nodeid,
				       const int64_t * version);
static void make_query_incr_last_groupid (T_SQL_AND_PARAM * sql_and_param,
					  const int64_t * next_gid_ver);
static void make_update_query_migration_start (T_SQL_AND_PARAM *
					       sql_and_param,
					       const int64_t * cur_ts,
					       const int *next_status,
					       const int *mig_groupid,
					       const int *src_nodeid,
					       const int *dest_nodeid,
					       const int *cur_status);
static void make_update_query_migration_end (T_SQL_AND_PARAM * sql_and_param,
					     const int *next_status,
					     const int64_t * cur_ts,
					     const int *num_shard_keys,
					     const int *groupid,
					     const int *cur_status);

static void make_query_incr_mig_req_count (T_SQL_AND_PARAM * sql_and_param);
static void make_query_decr_mig_req_count (T_SQL_AND_PARAM * sql_and_param);
static void make_query_incr_ddl_req_count (T_SQL_AND_PARAM * sql_and_param);
static void make_query_decr_ddl_req_count (T_SQL_AND_PARAM * sql_and_param);
static void make_query_incr_gc_req_count (T_SQL_AND_PARAM * sql_and_param);
static void make_query_decr_gc_req_count (T_SQL_AND_PARAM * sql_and_param);
static void make_query_insert_shard_node (T_SQL_AND_PARAM * sql_and_param,
					  const T_SHARD_NODE_INFO * node_info,
					  const int *node_status,
					  const int64_t * node_version);
static void make_query_incr_node_ver (T_SQL_AND_PARAM * sql_and_param,
				      const int64_t * next_node_ver);

static const char *admin_command_str (const T_SHD_MG_COMPENSATION_JOB *);

static int make_rebalance_info (T_GROUP_MIGRATION_INFO ** ret_mig_info,
				bool empty_node,
				int rbl_src_count, const int *rbl_src_node,
				int rbl_dest_count, const int *rbl_dest_node,
				const T_DB_NODE_INFO * db_node_info,
				const T_DB_GROUPID_INFO * db_groupid_info);
static int64_t make_cur_ts_bigint (void);
static int shd_mg_run_migration (void);
static void init_drand48_buffer (struct drand48_data *rand_buf);
static int launch_migrator_process (const char *global_dbname, int groupid,
				    int src_nodeid,
				    const T_SHARD_NODE_INFO * dest_node_info,
				    const T_SHARD_NODE_INFO * run_node_info,
				    bool schema_migration,
				    bool is_slave_mode_mig,
				    T_CCI_LAUNCH_RESULT * launch_res);
static bool is_existing_nodeid (const T_DB_NODE_INFO * db_node_info,
				int target_nodeid);
static const T_SHARD_NODE_INFO *find_node_info_by_nodeid (const T_DB_NODE_INFO
							  * db_node_info,
							  int target_nodeid,
							  HA_STATE_FOR_DRIVER
							  ha_state,
							  struct drand48_data
							  *rand_buf);
static void select_migration_node (const T_SHARD_NODE_INFO ** run_mig_node,
				   const T_SHARD_NODE_INFO ** dest_node_info,
				   bool * is_slave_mode_mig,
				   const T_DB_NODE_INFO * db_node_info,
				   int mig_src_nodeid, int mig_dest_nodeid,
				   struct drand48_data *rand_buf);
static const T_GROUPID_INFO *find_groupid_info (const T_DB_GROUPID_INFO *
						db_groupid_info, int groupid);
static int check_incomplete_node (CCI_CONN * conn);
static int check_rebalance_job (CCI_CONN * conn, bool include_fail_job,
				bool rm_prev_job);
static int64_t get_next_version (const T_SHARD_DB_REC * shard_db_rec);

/* static varriables */

static T_SHARD_JOB_QUEUE shard_Job_queue[NUM_SHARD_JOB_QUEUE];
static T_SHARD_JOB_QUEUE *shard_Job_queue_migration_handler;

static T_SHARD_MGMT_WORKER_ARG *shard_Mgmt_worker_arg;
static char local_Mgmt_connect_url[SRV_CON_URL_SIZE];

static T_SYNC_INFO db_Sync_info;
static T_DB_SHARD_INFO db_Shard_info;

static char shard_Mgmt_db_user[] = SHARD_MGMT_DB_USER;
static char shard_Mgmt_db_passwd[] = "\001shard_management";

static T_SHM_SHARD_MGMT_INFO *shm_Shard_mgmt_info;
static T_BROKER_INFO *shm_Br_info;

static struct _shard_mgmt_server_info
{
  char hostname[128];
  int pid;
  int port;
  char port_str[16];
  int local_mgmt_port;
  char migrator_dba_passwd[SRV_CON_DBPASSWD_SIZE];
} shard_Mgmt_server_info;

/* shard admin function table */
typedef struct
{
  int opcode;
  int (*func) (T_THR_FUNC_RES *, CCI_CONN *, const T_SHARD_DB_REC *,
	       const T_MGMT_REQ_ARG *, const T_SHARD_MGMT_JOB *);
  bool need_sync_dbinfo;
  const char *log_msg;
} T_SHD_MG_ADMIN_FUNC_TABLE;

static T_SHD_MG_ADMIN_FUNC_TABLE shd_Mg_admin_func_table[] = {
  {BRREQ_OP_CODE_INIT, admin_shard_init_command, false,
   "SHARD_INIT"},
  {BRREQ_OP_CODE_ADD_NODE, shd_mg_node_add, true,
   "SHARD_NODE_ADD"},
  {BRREQ_OP_CODE_DROP_NODE, shd_mg_node_drop, true,
   "SHARD_NODE_DROP"},
  {BRREQ_OP_CODE_MIGRATION_START, shd_mg_migration_start, true,
   "SHARD_MIGRATION_START"},
  {BRREQ_OP_CODE_MIGRATION_END, shd_mg_migration_end, true,
   "SHARD_MIGRATION_END"},
  {BRREQ_OP_CODE_DDL_START, shd_mg_ddl_start, true,
   "DDL_START"},
  {BRREQ_OP_CODE_DDL_END, shd_mg_ddl_end, true,
   "DDL_END"},
  {BRREQ_OP_CODE_REBALANCE_REQ, shd_mg_rebalance_req, true,
   "REBALANCE_REQ"},
  {BRREQ_OP_CODE_REBALANCE_JOB_COUNT, shd_mg_rebalance_job_count, false,
   "REBALANCE_JOB_COUNT"},
  {BRREQ_OP_CODE_GC_START, shd_mg_gc_start, true, "GC_START"},
  {BRREQ_OP_CODE_GC_END, shd_mg_gc_end, true, "GC_END"},
  {0, NULL, false, NULL}
};

/*
 * shd_mg_init - initialize shard mgmt threads, job queue, db connect info.
*/
int
shd_mg_init (int shard_mgmt_port, int local_mgmt_port,
	     const char *shard_metadb)
{
  shm_Br_info = &shm_Br->br_info[br_Index];
  shm_Shard_mgmt_info = &shm_Appl->info.shard_mgmt_info;

  sprintf (local_Mgmt_connect_url,
	   "cci:rye://localhost:%d/%s:dba/%s?query_timeout=%d&connectionType=local",
	   local_mgmt_port, shard_metadb, BR_SHARD_MGMT_NAME,
	   CCI_QUERY_TIMEOUT);

  if (shd_mg_init_sync_data () < 0)
    {
      return -1;
    }

  if (shd_mg_init_job_queue () < 0)
    {
      return -1;
    }

  if (shd_mg_init_worker () < 0)
    {
      return -1;
    }

  memset (&shard_Mgmt_server_info, 0, sizeof (shard_Mgmt_server_info));

  gethostname (shard_Mgmt_server_info.hostname,
	       sizeof (shard_Mgmt_server_info.hostname) - 1);

  shard_Mgmt_server_info.pid = getpid ();
  shard_Mgmt_server_info.port = shard_mgmt_port;
  sprintf (shard_Mgmt_server_info.port_str, "%d", shard_mgmt_port);

  shard_Mgmt_server_info.local_mgmt_port = local_mgmt_port;

  return 0;
}

/*
 * shard_mgmt_receiver_thr_f - shard management receiver thread
*/
THREAD_FUNC
shard_mgmt_receiver_thr_f (UNUSED_ARG void *arg)
{
  SOCKET clt_sock_fd;
  T_BROKER_REQUEST_MSG *br_req_msg;
  int err_code = 0;
  ER_MSG_INFO *er_msg;
  struct timeval recv_time;
  in_addr_t clt_ip_addr;

  signal (SIGPIPE, SIG_IGN);

  er_msg = malloc (sizeof (ER_MSG_INFO));
  err_code = er_set_msg_info (er_msg);
  if (err_code != NO_ERROR)
    {
      return NULL;
    }

  br_req_msg = brreq_msg_alloc (BRREQ_OP_CODE_MSG_MAX_SIZE);
  if (br_req_msg == NULL)
    {
      br_Process_flag = 0;
      br_set_init_error (BR_ER_INIT_NO_MORE_MEMORY, 0);
    }

  while (br_Process_flag)
    {
      err_code = 0;

      clt_sock_fd = br_accept_unix_domain (&clt_ip_addr, &recv_time,
					   br_req_msg);

      if (IS_INVALID_SOCKET (clt_sock_fd))
	{
	  continue;
	}

      if (br_req_msg->op_code == BRREQ_OP_CODE_PING_SHARD_MGMT)
	{
	  br_send_result_to_client (clt_sock_fd, 0, NULL);
	  shm_Br_info->ping_req_count++;
	  continue;
	}

      if (!IS_SHARD_MGMT_OPCODE (br_req_msg->op_code))
	{
	  err_code = BR_ER_NOT_SHARD_MGMT_OPCODE;
	  goto end;
	}

      err_code = shd_mg_job_queue_add (clt_sock_fd, clt_ip_addr, br_req_msg);
      if (err_code < 0)
	{
	  goto end;
	}

    end:
      if (err_code < 0)
	{
	  br_send_result_to_client (clt_sock_fd, err_code, NULL);
	  shm_Br_info->connect_fail_count++;

	  RYE_CLOSE_SOCKET (clt_sock_fd);
	}
    }

  brreq_msg_free (br_req_msg);

  return NULL;
}

/*
 * shd_mg_init_sync_data - initialize SYNC_DATA structure
 */
static int
shd_mg_init_sync_data ()
{
  memset (&db_Sync_info, 0, sizeof (db_Sync_info));
  memset (&db_Shard_info, 0, sizeof (db_Shard_info));

  if (pthread_mutex_init (&db_Sync_info.mutex, NULL) < 0)
    {
      return -1;
    }
  if (pthread_mutex_init (&db_Shard_info.mutex, NULL) < 0)
    {
      return -1;
    }

  return 0;
}

/*
 * shd_mg_init_job_queue - initialize shard mgmt job queue
*/
static int
shd_mg_init_job_queue ()
{
  int idx;
  T_SHARD_JOB_QUEUE *secondary_job_queue;

  memset (shard_Job_queue, 0,
	  sizeof (T_SHARD_JOB_QUEUE) * NUM_SHARD_JOB_QUEUE);

  idx = 0;

  shard_Job_queue[idx].job_queue_type = JOB_QUEUE_TYPE_GET_INFO;
  shard_Job_queue[idx].num_workers = 1;
  shard_Job_queue[idx].thr_func = shd_mg_worker_thr;
  shard_Job_queue[idx].job_func = shd_mg_func_shard_info;
  shard_Job_queue[idx].secondary_job_queue = NULL;
  shard_Job_queue[idx].shm_queue_info =
    &shm_Shard_mgmt_info->get_info_req_queue;
  idx++;

  secondary_job_queue = &shard_Job_queue[idx];
  shard_Job_queue[idx].job_queue_type = JOB_QUEUE_TYPE_WAIT_JOB;
  shard_Job_queue[idx].num_workers = 1;
  shard_Job_queue[idx].thr_func = shd_mg_worker_thr;
  shard_Job_queue[idx].job_func = shd_mg_func_admin;
  shard_Job_queue[idx].secondary_job_queue = secondary_job_queue;
  shard_Job_queue[idx].shm_queue_info =
    &shm_Shard_mgmt_info->wait_job_req_queue;
  idx++;

  shard_Job_queue[idx].job_queue_type = JOB_QUEUE_TYPE_ADMIN;
  shard_Job_queue[idx].num_workers = 1;
  shard_Job_queue[idx].thr_func = shd_mg_worker_thr;
  shard_Job_queue[idx].job_func = shd_mg_func_admin;
  shard_Job_queue[idx].secondary_job_queue = secondary_job_queue;
  shard_Job_queue[idx].shm_queue_info = &shm_Shard_mgmt_info->admin_req_queue;
  idx++;

  shard_Job_queue[idx].job_queue_type = JOB_QUEUE_TYPE_SYNC_LOCAL_MGMT;
  shard_Job_queue[idx].num_workers = 1;
  shard_Job_queue[idx].thr_func = shd_mg_sync_local_mgmt_thr;
  shard_Job_queue[idx].job_func = NULL;
  shard_Job_queue[idx].secondary_job_queue = NULL;
  idx++;

  shard_Job_queue[idx].job_queue_type = JOB_QUEUE_TYPE_MIGRATION_HANDLER;
  shard_Job_queue[idx].num_workers = 1;
  shard_Job_queue[idx].thr_func = shd_mg_migration_handler_thr;
  shard_Job_queue[idx].job_func = NULL;
  shard_Job_queue[idx].secondary_job_queue = NULL;
  shard_Job_queue_migration_handler = &shard_Job_queue[idx];
  idx++;

  shard_Job_queue[idx].job_queue_type = JOB_QUEUE_TYPE_MIGRATOR_WAITER;
  shard_Job_queue[idx].num_workers = 1;
  shard_Job_queue[idx].thr_func = shd_mg_migrator_wait_thr;
  shard_Job_queue[idx].job_func = NULL;
  shard_Job_queue[idx].secondary_job_queue = NULL;
  idx++;

  assert (idx == NUM_SHARD_JOB_QUEUE);

  for (idx = 0; idx < NUM_SHARD_JOB_QUEUE; idx++)
    {
      if (pthread_mutex_init (&shard_Job_queue[idx].lock, NULL) < 0)
	{
	  return -1;
	}
      if (pthread_cond_init (&shard_Job_queue[idx].cond, NULL) < 0)
	{
	  return -1;
	}
    }

  return 0;
}

/*
 * shd_mg_init_worker - initialize shard mgmt threads
*/
static int
shd_mg_init_worker ()
{
  pthread_t shard_mgmt_worker;
  int num_running_worker = 0;
  int i, j;
  int idx;
  int num_workers = 0;

  for (i = 0; i < NUM_SHARD_JOB_QUEUE; i++)
    {
      num_workers += shard_Job_queue[i].num_workers;
    }

  shard_Mgmt_worker_arg =
    RYE_MALLOC (sizeof (T_SHARD_MGMT_WORKER_ARG) * num_workers);
  if (shard_Mgmt_worker_arg == NULL)
    {
      return -1;
    }

  idx = 0;
  for (i = 0; i < NUM_SHARD_JOB_QUEUE; i++)
    {
      T_SHARD_MGMT_WORKER_ARG *arg_p;

      for (j = 0; j < shard_Job_queue[i].num_workers; j++)
	{
	  arg_p = &shard_Mgmt_worker_arg[idx++];

	  arg_p->job_queue = &shard_Job_queue[i];
	  arg_p->is_running = false;

	  THREAD_BEGIN (shard_mgmt_worker, shard_Job_queue[i].thr_func,
			arg_p);
	}
    }

  while (num_workers != num_running_worker)
    {
      THREAD_SLEEP (100);

      num_running_worker = 0;

      for (i = 0; i < NUM_SHARD_JOB_QUEUE; i++)
	{
	  if (shard_Mgmt_worker_arg[i].is_running)
	    {
	      num_running_worker++;
	    }
	}
    }

  return 0;
}

/*
 * shd_mg_worker_thr - shard mgmt worker thread function
*/
static THREAD_FUNC
shd_mg_worker_thr (void *arg)
{
  T_SHARD_JOB_QUEUE *job_queue;
  T_SHARD_MGMT_WORKER_ARG *worker_arg = (T_SHARD_MGMT_WORKER_ARG *) arg;
  T_THR_FUNC_RES thr_func_res;
  T_SHARD_MGMT_JOB *job;
  int error;
  ER_MSG_INFO *er_msg;

  er_msg = malloc (sizeof (ER_MSG_INFO));
  error = er_set_msg_info (er_msg);
  if (error != NO_ERROR)
    {
      return NULL;
    }

  job_queue = worker_arg->job_queue;
  assert (job_queue != NULL);

  worker_arg->is_running = true;

  while (true)
    {
      if (job_queue->job_queue_type == JOB_QUEUE_TYPE_WAIT_JOB)
	{
	  THREAD_SLEEP (100);
	}

      pthread_mutex_lock (&job_queue->lock);

      job = shd_mg_job_queue_remove_all (job_queue);
      if (job == NULL)
	{
	  pthread_cond_wait (&job_queue->cond, &job_queue->lock);
	}

      pthread_mutex_unlock (&job_queue->lock);

      if (job == NULL)
	{
	  (*job_queue->job_func) (NULL, NULL);
	  continue;
	}

      while (job != NULL)
	{
	  T_SHARD_MGMT_JOB *next_job = job->next;
	  bool job_remove_flag = false;

	  if (job->req_msg == NULL)
	    {
	      /* wait ddl_end or migration_end request */
	      assert (job->compensation_job != NULL);

	      if (read_next_request (job) < 0)
		{
		  if (do_compensation_job (job->compensation_job) < 0)
		    {
		      assert (0);
		    }
		  else
		    {
		      job_remove_flag = true;
		    }

		  br_log_write (BROKER_LOG_NOTICE, job->clt_ip_addr,
				"%s NET_ERROR",
				admin_command_str (job->compensation_job));
		}
	    }

	  if (job_remove_flag == false && job->req_msg != NULL)
	    {
	      T_SHD_MG_COMPENSATION_JOB *compensation_job = NULL;

	      thr_func_res = (*job_queue->job_func) (job, &compensation_job);

	      if (thr_func_res == THR_FUNC_COMPLETE)
		{
		  assert (compensation_job == NULL);
		  job_remove_flag = true;
		}
	      else if (thr_func_res == THR_FUNC_COMPLETE_AND_WAIT_NEXT)
		{
		  assert (compensation_job != NULL);
		  RYE_FREE_MEM (job->req_msg);
		  job->compensation_job = compensation_job;
		}
	      else
		{
		  assert (thr_func_res == THR_FUNC_NEED_QUEUEING);
		  assert (compensation_job == NULL);
		}
	    }

	  if (job_remove_flag)
	    {
	      RYE_CLOSE_SOCKET (job->clt_sock_fd);
	      free_mgmt_job (job);
	    }
	  else
	    {
	      shd_job_add_internal (job_queue->secondary_job_queue, job,
				    true);
	    }

	  job = next_job;
	}
    }

  return NULL;
}

static void
set_shm_shard_node_info (const T_LOCAL_MGMT_SYNC_INFO * sync_info)
{
  int i;
  int node_count;

  if (sync_info->db_node_info == NULL)
    {
      return;
    }

  assert (SHM_SHARD_NODE_INFO_MAX >=
	  sync_info->db_node_info->node_info_count);

  node_count = MIN (SHM_SHARD_NODE_INFO_MAX,
		    sync_info->db_node_info->node_info_count);

  shm_Shard_mgmt_info->last_sync_time = sync_info->last_sync_time;
  shm_Shard_mgmt_info->num_shard_node_info = node_count;

  for (i = 0; i < node_count; i++)
    {
      const T_SHARD_NODE_INFO *src_node_info;
      T_SHM_SHARD_NODE_INFO *shm_node_info;

      src_node_info = &sync_info->db_node_info->node_info[i];
      shm_node_info = &shm_Shard_mgmt_info->shard_node_info[i];

      shm_node_info->node_id = src_node_info->node_id;
      shm_node_info->host_info = src_node_info->host_info;
      STRNCPY (shm_node_info->local_dbname, src_node_info->local_dbname,
	       sizeof (shm_node_info->local_dbname));
      STRNCPY (shm_node_info->host_ip, src_node_info->host_ip_str,
	       sizeof (shm_node_info->host_ip));

      if (src_node_info->host_name[0] == '\0')
	{
	  strcpy (shm_node_info->host_name, "-");
	}
      else
	{
	  STRNCPY (shm_node_info->host_name, src_node_info->host_name,
		   sizeof (shm_node_info->host_name));
	}
      shm_node_info->ha_state = src_node_info->ha_state;
    }
}

/*
 * shd_mg_sync_local_mgmt_thr - shard mgmt worker thread function
*/
static THREAD_FUNC
shd_mg_sync_local_mgmt_thr (void *arg)
{
  T_SHARD_JOB_QUEUE *job_queue;
  T_SHARD_MGMT_WORKER_ARG *worker_arg = (T_SHARD_MGMT_WORKER_ARG *) arg;
  T_SHARD_MGMT_JOB *job;
  T_LOCAL_MGMT_SYNC_INFO sync_info;
  int error;
  ER_MSG_INFO *er_msg;

  er_msg = malloc (sizeof (ER_MSG_INFO));
  error = er_set_msg_info (er_msg);
  if (error != NO_ERROR)
    {
      return NULL;
    }

  job_queue = worker_arg->job_queue;
  assert (job_queue != NULL);
  assert (job_queue->job_queue_type == JOB_QUEUE_TYPE_SYNC_LOCAL_MGMT);

  worker_arg->is_running = true;

  memset (&sync_info, 0, sizeof (T_LOCAL_MGMT_SYNC_INFO));

  while (true)
    {
      pthread_mutex_lock (&job_queue->lock);

      job = shd_mg_job_queue_remove_all (job_queue);
      if (job == NULL)
	{
	  struct timespec ts;
	  struct timeval tv;

	  gettimeofday (&tv, NULL);
	  ts.tv_sec = tv.tv_sec + 3;
	  ts.tv_nsec = tv.tv_usec * 1000;

	  pthread_cond_timedwait (&job_queue->cond, &job_queue->lock, &ts);
	}

      pthread_mutex_unlock (&job_queue->lock);

      shd_mg_func_sync_local_mgmt (&sync_info, (job == NULL ? false : true));

      while (job != NULL)
	{
	  T_SHARD_MGMT_JOB *next_job = job->next;
	  free_mgmt_job (job);
	  job = next_job;
	}

      set_shm_shard_node_info (&sync_info);
    }

  return NULL;
}

/*
 * shd_mg_migration_handler_thr -
*/
static THREAD_FUNC
shd_mg_migration_handler_thr (void *arg)
{
  T_SHARD_JOB_QUEUE *job_queue;
  T_SHARD_MGMT_WORKER_ARG *worker_arg = (T_SHARD_MGMT_WORKER_ARG *) arg;
  int num_waiting_gid = 0;
  int error;
  ER_MSG_INFO *er_msg;

  er_msg = malloc (sizeof (ER_MSG_INFO));
  error = er_set_msg_info (er_msg);
  if (error != NO_ERROR)
    {
      return NULL;
    }

  job_queue = worker_arg->job_queue;
  assert (job_queue != NULL);
  assert (job_queue->job_queue_type == JOB_QUEUE_TYPE_MIGRATION_HANDLER);

  worker_arg->is_running = true;

  while (true)
    {
      pthread_mutex_lock (&job_queue->lock);

      if (job_queue->num_job == 0)
	{
	  if (num_waiting_gid == 0)
	    {
	      pthread_cond_wait (&job_queue->cond, &job_queue->lock);
	    }
	  else
	    {
	      struct timespec ts;
	      struct timeval tv;

	      gettimeofday (&tv, NULL);
	      ts.tv_sec = tv.tv_sec + 60;
	      ts.tv_nsec = tv.tv_usec * 1000;

	      pthread_cond_timedwait (&job_queue->cond, &job_queue->lock,
				      &ts);
	    }
	}

      set_job_queue_job_count (job_queue, 0);

      pthread_mutex_unlock (&job_queue->lock);

      num_waiting_gid = shd_mg_run_migration ();
    }

  return NULL;
}

static THREAD_FUNC
shd_mg_migrator_wait_thr (void *arg)
{
  T_SHARD_MGMT_WORKER_ARG *worker_arg = (T_SHARD_MGMT_WORKER_ARG *) arg;
  int error;
  ER_MSG_INFO *er_msg;

  er_msg = malloc (sizeof (ER_MSG_INFO));
  error = er_set_msg_info (er_msg);
  if (error != NO_ERROR)
    {
      return NULL;
    }

  worker_arg->is_running = true;

  while (true)
    {
      int wait_count = cc_mgmt_count_launch_process ();

      shm_Shard_mgmt_info->running_migrator_count = wait_count;

      if (wait_count > 0)
	{
	  int error;
	  T_CCI_LAUNCH_RESULT launch_res = CCI_LAUNCH_RESULT_INITIALIZER;

	  error = cci_mgmt_wait_launch_process (&launch_res, 1000);
	  if (IS_CCI_NO_ERROR (error))
	    {
	      const char *success_fail;
	      const char *migrator_msg;

	      if (launch_res.exit_status == 0)
		{
		  success_fail = "success";
		  migrator_msg =
		    (launch_res.stdout_size > 0 ? launch_res.stdout_buf : "");
		}
	      else
		{
		  success_fail = "fail";
		  migrator_msg =
		    (launch_res.stderr_size > 0 ? launch_res.stderr_buf : "");
		}
	      br_log_write (BROKER_LOG_NOTICE, INADDR_NONE,
			    "exit migrator %s %s: status:%d (%s)",
			    launch_res.userdata, success_fail,
			    launch_res.exit_status, migrator_msg);
	    }
	}
      else
	{
	  THREAD_SLEEP (1000);
	}
    }

  return NULL;
}

static void
shd_mg_migration_handler_wakeup ()
{
  pthread_mutex_lock (&shard_Job_queue_migration_handler->lock);

  set_job_queue_job_count (shard_Job_queue_migration_handler,
			   shard_Job_queue_migration_handler->num_job + 1);

  pthread_cond_signal (&shard_Job_queue_migration_handler->cond);

  pthread_mutex_unlock (&shard_Job_queue_migration_handler->lock);
}

static int
read_next_request (T_SHARD_MGMT_JOB * job)
{
  int sock_available = 0;
  T_BROKER_REQUEST_MSG *br_req_msg;

  if (ioctl (job->clt_sock_fd, FIONREAD, &sock_available) < 0)
    {
      return -1;
    }

  if (sock_available == 0)
    {
      int n;
      struct pollfd po = { 0, 0, 0 };

      po.fd = job->clt_sock_fd;
      po.events = POLLIN;

      n = poll (&po, 1, 0);
      if (n == 1)
	{
	  if (po.revents & POLLERR || po.revents & POLLHUP)
	    {
	      return -1;
	    }
	  else if (po.revents & POLLIN)
	    {
	      char buf[1];
	      n = recv (job->clt_sock_fd, buf, 1, MSG_PEEK | MSG_DONTWAIT);
	      if (n <= 0)
		{
		  return -1;
		}
	    }
	}

      return 0;
    }

  br_req_msg = brreq_msg_alloc (BRREQ_OP_CODE_MSG_MAX_SIZE);
  if (br_req_msg == NULL)
    {
      return -1;
    }

  if (br_read_broker_request_msg (job->clt_sock_fd, br_req_msg) < 0)
    {
      RYE_FREE_MEM (br_req_msg);
      return -1;
    }
  if (br_req_msg->op_code != job->compensation_job->next_req_opcode)
    {
      RYE_FREE_MEM (br_req_msg);
      return -1;
    }

  job->req_msg = br_req_msg;
  return 0;
}

static int
do_compensation_job (T_SHD_MG_COMPENSATION_JOB * compensation_job)
{
  CCI_CONN conn;
  int res;

  if (connect_metadb (&conn, CCI_AUTOCOMMIT_FALSE, shard_Mgmt_db_user,
		      shard_Mgmt_db_passwd) < 0)
    {
      return -1;
    }

  res = (*compensation_job->func) (&conn, compensation_job->arg);

  cci_end_tran (&conn, (res < 0 ? CCI_TRAN_ROLLBACK : CCI_TRAN_COMMIT));
  cci_disconnect (&conn);

  return res;
}

static bool
check_client_dbname (const char *clt_dbname,
		     const T_SHARD_DB_REC * shard_db_rec)
{
  if (clt_dbname == NULL || shard_db_rec->global_dbname == NULL)
    {
      return false;
    }

  if (strcasecmp (clt_dbname, shard_db_rec->global_dbname) == 0)
    {
      return true;
    }
  else
    {
      return false;
    }
}

/*
 * shd_mg_func_shard_info - shard mgmt worker thread's job function
*/
static T_THR_FUNC_RES
shd_mg_func_shard_info (const T_SHARD_MGMT_JOB * job,
			UNUSED_ARG T_SHD_MG_COMPENSATION_JOB ** comp_job)
{
  const T_INFO_NET_STREAM *net_stream_node_info = NULL;
  const T_INFO_NET_STREAM *net_stream_node_state = NULL;
  const T_INFO_NET_STREAM *net_stream_gid_info = NULL;
  const char *node_info_msg;
  const char *gid_info_msg;
  const char *node_state_msg;
  int node_state_msg_size;
  int node_info_msg_size;
  int gid_info_msg_size;
  T_THR_FUNC_RES thr_func_res = THR_FUNC_COMPLETE;

  int64_t clt_node_ver;
  int64_t clt_groupid_ver;
  int64_t clt_created_at;
  int64_t svr_node_ver = 0;
  int64_t svr_groupid_ver = 0;
  int64_t svr_created_at = 0;
  int err;
  const char *clt_dbname = NULL;
  bool check_dbname_res;
  T_MGMT_REQ_ARG req_arg = T_MGMT_REQ_ARG_INIT;

  sync_node_info (NULL, NULL, false);
  sync_groupid_info (NULL, NULL, false);

  if (job == NULL)
    {
      return thr_func_res;
    }

  if (br_mgmt_get_req_arg (&req_arg, job->req_msg) < 0)
    {
      send_info_msg_to_client (job->clt_sock_fd,
			       BR_ER_COMMUNICATION, NULL, 0, NULL, 0, NULL, 0,
			       NULL, 0);
      return thr_func_res;
    }

  clt_dbname = req_arg.clt_dbname;
  clt_node_ver = req_arg.value.get_info_arg.clt_node_ver;
  clt_groupid_ver = req_arg.value.get_info_arg.clt_groupid_ver;
  clt_created_at = req_arg.value.get_info_arg.clt_created_at;

  LOCK_DB_SHARD_INFO ();

  svr_created_at = db_Sync_info.shard_db_rec.created_at;
  if (svr_created_at != clt_created_at)
    {
      /* if cached create_at value of client is invalid,
       * shard info request is first sync request, something wrong,
       *                or shard db recreated.
       * recache shard info
       */
      clt_node_ver = 0;
      clt_groupid_ver = 0;
    }

  node_info_msg = NULL;
  node_info_msg_size = 0;
  gid_info_msg = NULL;
  gid_info_msg_size = 0;
  node_state_msg = NULL;
  node_state_msg_size = 0;
  err = 0;

  check_dbname_res = check_client_dbname (clt_dbname,
					  &db_Sync_info.shard_db_rec);
  if (check_dbname_res == true)
    {
      if (db_Shard_info.db_node_info != NULL &&
	  db_Shard_info.db_node_info->node_info_ver > clt_node_ver)
	{
	  svr_node_ver = db_Shard_info.db_node_info->node_info_ver;

	  net_stream_node_info =
	    make_node_info_net_stream (db_Shard_info.db_node_info);
	  if (net_stream_node_info == NULL)
	    {
	      err = BR_ER_SHARD_INFO_NOT_AVAILABLE;
	    }
	  else
	    {
	      node_info_msg = net_stream_node_info->buf;
	      node_info_msg_size = net_stream_node_info->size;
	    }
	}

      if (db_Shard_info.db_groupid_info != NULL &&
	  db_Shard_info.db_groupid_info->gid_info_ver > clt_groupid_ver)
	{
	  svr_groupid_ver = db_Shard_info.db_groupid_info->gid_info_ver;

	  net_stream_gid_info =
	    make_groupid_info_net_stream (db_Shard_info.db_groupid_info,
					  clt_groupid_ver);
	  if (net_stream_gid_info == NULL)
	    {
	      err = BR_ER_SHARD_INFO_NOT_AVAILABLE;
	    }
	  else
	    {
	      gid_info_msg = net_stream_gid_info->buf;
	      gid_info_msg_size = net_stream_gid_info->size;
	    }
	}

      net_stream_node_state =
	make_node_state_net_stream (db_Shard_info.db_node_info);
      if (net_stream_node_state != NULL)
	{
	  node_state_msg = net_stream_node_state->buf;
	  node_state_msg_size = net_stream_node_state->size;
	}
    }

  if (err < 0)
    {
      send_info_msg_to_client (job->clt_sock_fd, err, NULL, 0, NULL, 0,
			       NULL, 0, NULL, 0);
    }
  else
    {
      char shard_info_hdr[sizeof (int64_t)];

      br_mgmt_net_add_int64 (shard_info_hdr,
			     db_Sync_info.shard_db_rec.created_at);

      send_info_msg_to_client (job->clt_sock_fd, err,
			       shard_info_hdr, sizeof (shard_info_hdr),
			       node_info_msg, node_info_msg_size,
			       gid_info_msg, gid_info_msg_size,
			       node_state_msg, node_state_msg_size);
    }

  UNLOCK_DB_SHARD_INFO ();

  if (err < 0)
    {
      br_log_write (BROKER_LOG_ERROR, job->clt_ip_addr, "GET_SHARD_INFO:%d",
		    err);
    }
  else
    {
      if (svr_created_at != clt_created_at ||
	  svr_node_ver > clt_node_ver || svr_groupid_ver > clt_groupid_ver)
	{
	  br_log_write (BROKER_LOG_NOTICE, job->clt_ip_addr,
			"created_at: %ld -> %ld, node ver: %ld -> %ld, GID ver: %ld -> %ld",
			clt_created_at, svr_created_at,
			req_arg.value.get_info_arg.clt_node_ver,
			svr_node_ver,
			req_arg.value.get_info_arg.clt_groupid_ver,
			svr_groupid_ver);
	}
    }

  RYE_FREE_MEM (req_arg.alloc_buffer);

  return thr_func_res;
}

/*
 * shd_mg_func_admin - shard mgmt worker thread's job function
*/
static T_THR_FUNC_RES
shd_mg_func_admin (const T_SHARD_MGMT_JOB * job,
		   T_SHD_MG_COMPENSATION_JOB ** compensation_job)
{
  T_BROKER_REQUEST_MSG *brreq_msg;
  int res = 0;
  int i;
  T_SHD_MG_ADMIN_FUNC_TABLE *admin_func_table;
  CCI_CONN conn_handle;
  CCI_CONN *conn = NULL;
  T_SHARD_DB_REC shard_db_rec = SHARD_DB_REC_INIT_VALUE;
  T_MGMT_REQ_ARG req_arg = T_MGMT_REQ_ARG_INIT;
  T_THR_FUNC_RES thr_func_res = THR_FUNC_COMPLETE;
  const char *db_user;
  const char *db_passwd;
  struct timeval admin_start_time;
  struct timeval admin_end_time;

  if (job == NULL)
    {
      return thr_func_res;
    }

  gettimeofday (&admin_start_time, NULL);

  brreq_msg = job->req_msg;

  admin_func_table = NULL;
  for (i = 0; shd_Mg_admin_func_table[i].func != NULL; i++)
    {
      if (shd_Mg_admin_func_table[i].opcode == brreq_msg->op_code)
	{
	  admin_func_table = &shd_Mg_admin_func_table[i];
	}
    }

  if (admin_func_table == NULL)
    {
      assert (0);
      br_log_write (BROKER_LOG_ERROR, job->clt_ip_addr, "invalid opcode");

      res = BR_ER_COMMUNICATION;
      goto admin_func_end;
    }

  res = br_mgmt_get_req_arg (&req_arg, job->req_msg);
  if (res < 0)
    {
      goto admin_func_end;
    }

  if (brreq_msg->op_code == BRREQ_OP_CODE_INIT)
    {
      T_MGMT_REQ_ARG_SHARD_INIT *init_arg = &req_arg.value.init_shard_arg;
      db_user = "dba";
      db_passwd = init_arg->dba_passwd;
    }
  else
    {
      db_user = shard_Mgmt_db_user;
      db_passwd = shard_Mgmt_db_passwd;
    }

  if (connect_metadb (&conn_handle, CCI_AUTOCOMMIT_FALSE,
		      db_user, db_passwd) < 0)
    {
      res = BR_ER_METADB;
      goto admin_func_end;
    }
  conn = &conn_handle;

  if (admin_func_table->need_sync_dbinfo)
    {
      if (sync_node_info (conn, &shard_db_rec, true) < 0 ||
	  sync_groupid_info (conn, NULL, false) < 0)
	{
	  res = BR_ER_METADB;
	  goto admin_func_end;
	}

      if (check_client_dbname (req_arg.clt_dbname, &shard_db_rec) == false)
	{
	  res = BR_ER_DBNAME_MISMATCHED;
	  goto admin_func_end;
	}
    }
  else
    {
      LOCK_DB_SHARD_INFO ();
      shard_db_rec = db_Sync_info.shard_db_rec;
      UNLOCK_DB_SHARD_INFO ();
    }

  res = (*admin_func_table->func) (&thr_func_res, conn, &shard_db_rec,
				   &req_arg, job);

admin_func_end:

  if (res >= 0 && thr_func_res == THR_FUNC_COMPLETE_AND_WAIT_NEXT)
    {
      res = set_admin_compensation_job (compensation_job, job, &req_arg);
      if (res < 0)
	{
	  thr_func_res = THR_FUNC_COMPLETE;
	}
    }

  if (conn != NULL)
    {
      cci_end_tran (conn, (res < 0 ? CCI_TRAN_ROLLBACK : CCI_TRAN_COMMIT));
      cci_disconnect (conn);
    }

  if (thr_func_res == THR_FUNC_COMPLETE ||
      thr_func_res == THR_FUNC_COMPLETE_AND_WAIT_NEXT)
    {
      br_send_result_to_client (job->clt_sock_fd, res, NULL);

      if (admin_func_table != NULL)
	{
	  int elapsed;
	  gettimeofday (&admin_end_time, NULL);

	  elapsed = (admin_end_time.tv_sec - admin_start_time.tv_sec) * 1000 +
	    (admin_end_time.tv_usec - admin_start_time.tv_usec) / 1000;

	  br_log_write (BROKER_LOG_NOTICE, job->clt_ip_addr,
			"[%s] error: %d time: %d ms",
			admin_func_table->log_msg, res, elapsed);
	}
    }

  if (res >= 0)
    {
      SHARD_DB_RESET_CHECK_TIME ();
    }

  RYE_FREE_MEM (req_arg.alloc_buffer);

  return thr_func_res;
}

static int
set_admin_compensation_job (T_SHD_MG_COMPENSATION_JOB ** out,
			    const T_SHARD_MGMT_JOB * job,
			    const T_MGMT_REQ_ARG * req_arg)
{
  T_BROKER_REQUEST_MSG *brreq_msg;
  T_SHD_MG_COMPENSATION_JOB *compensation_job;

  brreq_msg = job->req_msg;

  compensation_job = RYE_MALLOC (sizeof (T_SHD_MG_COMPENSATION_JOB));
  if (compensation_job == NULL)
    {
      return BR_ER_NO_MORE_MEMORY;
    }
  memset (compensation_job, 0, sizeof (T_SHD_MG_COMPENSATION_JOB));

  if (brreq_msg->op_code == BRREQ_OP_CODE_DDL_START)
    {
      compensation_job->func = shd_mg_ddl_fail;
      compensation_job->next_req_opcode = BRREQ_OP_CODE_DDL_END;
    }
  else if (brreq_msg->op_code == BRREQ_OP_CODE_GC_START)
    {
      compensation_job->func = shd_mg_gc_fail;
      compensation_job->next_req_opcode = BRREQ_OP_CODE_GC_END;
    }
  else if (brreq_msg->op_code == BRREQ_OP_CODE_MIGRATION_START)
    {
      compensation_job->arg =
	RYE_MALLOC (sizeof (T_MGMT_REQ_ARG_SHARD_MIGRATION));
      if (compensation_job->arg == NULL)
	{
	  RYE_FREE_MEM (compensation_job);
	  return BR_ER_NO_MORE_MEMORY;
	}

      compensation_job->func = shd_mg_migration_fail;
      compensation_job->next_req_opcode = BRREQ_OP_CODE_MIGRATION_END;
      memcpy (compensation_job->arg, &req_arg->value.migration_arg,
	      sizeof (T_MGMT_REQ_ARG_SHARD_MIGRATION));
    }
  else
    {
      RYE_FREE_MEM (compensation_job);
      return BR_ER_INTERNAL;
    }

  *out = compensation_job;

  return 0;
}

static int
make_distinct_node_array (int **distinct_node_array,
			  int num_nodes, const T_SHARD_NODE_INFO * node_info)
{
  int *distinct_arr;
  int distinct_count = 0;
  int i, j;

  distinct_arr = RYE_MALLOC (sizeof (int) * num_nodes);
  if (distinct_arr == NULL)
    {
      return -1;
    }

  for (i = 0; i < num_nodes; i++)
    {
      bool found = false;
      int nodeid = node_info[i].node_id;

      if (nodeid <= 0)
	{
	  continue;
	}

      for (j = 0; j < distinct_count; j++)
	{
	  if (distinct_arr[j] == nodeid)
	    {
	      found = true;
	      break;
	    }
	}
      if (found == false)
	{
	  distinct_arr[distinct_count++] = nodeid;
	}
    }

  if (distinct_count == 0)
    {
      RYE_FREE_MEM (distinct_arr);
    }

  *distinct_node_array = distinct_arr;
  return distinct_count;
}

static int
admin_shard_init_command (UNUSED_ARG T_THR_FUNC_RES * thr_func_res,
			  CCI_CONN * conn,
			  UNUSED_ARG const T_SHARD_DB_REC * shard_db_rec,
			  const T_MGMT_REQ_ARG * req_arg,
			  UNUSED_ARG const T_SHARD_MGMT_JOB * job)
{
  int64_t next_node_ver = 1;
  int init_node_count;
  int *init_nodeid_arr = NULL;
  T_SHARD_DB_REC init_shard_db_rec = SHARD_DB_REC_INIT_VALUE;
  const T_MGMT_REQ_ARG_SHARD_INIT *init_shard_arg;
  T_SQL_AND_PARAM sql_and_param[15];
  int num_sql;
  int shard_db_id = SHARD_DB_ID_VALUE;
  T_TABLE_DEF *create_tables[] = {
    &table_Def_shard_db,
    &table_Def_shard_node,
    &table_Def_shard_groupid,
    &table_Def_shard_migration
  };
  int i;

  init_shard_arg = &req_arg->value.init_shard_arg;

  /* get request args */
  init_shard_db_rec.global_dbname = init_shard_arg->global_dbname;
  init_shard_db_rec.groupid_count = init_shard_arg->groupid_count;
  init_shard_db_rec.created_at = make_cur_ts_bigint ();

  init_node_count = init_shard_arg->init_num_node;

  init_node_count = make_distinct_node_array (&init_nodeid_arr,
					      init_shard_arg->init_num_node,
					      init_shard_arg->init_node);
  if (init_node_count <= 0)
    {
      return BR_ER_INVALID_ARGUMENT;
    }

  /* create tables and insert shard_db table */
  num_sql = 0;

  make_query_create_user (&sql_and_param[num_sql++]);

  for (i = 0; i < (int) DIM (create_tables); i++)
    {
      make_query_create_global_table (&sql_and_param[num_sql++],
				      create_tables[i]);
      make_query_change_owner (&sql_and_param[num_sql++], create_tables[i]);

      assert (MAX_COLUMNS >= create_tables[i]->num_columns);
    }

  for (i = 0; i < (int) DIM (shard_indexes); i++)
    {
      make_query_create_index (&sql_and_param[num_sql++], &shard_indexes[i]);
    }

  make_query_insert_shard_db (&sql_and_param[num_sql++], &shard_db_id,
			      &init_shard_db_rec);

  assert ((int) DIM (sql_and_param) >= num_sql);
  if (admin_query_execute_array (conn, num_sql, sql_and_param) < 0)
    {
      RYE_FREE_MEM (init_nodeid_arr);
      return BR_ER_METADB;
    }

  if (insert_shard_groupid_table (conn,
				  init_shard_db_rec.groupid_count,
				  init_node_count, init_nodeid_arr,
				  init_shard_db_rec.gid_info_last_ver) < 0)
    {
      RYE_FREE_MEM (init_nodeid_arr);
      return BR_ER_METADB;
    }

  RYE_FREE_MEM (init_nodeid_arr);

  next_node_ver = 1;
  for (i = 0; i < init_shard_arg->init_num_node; i++)
    {
      if (insert_shard_node_table (conn, &init_shard_arg->init_node[i],
				   ADD_NODE_STATUS_COMPLETE,
				   next_node_ver) < 0)
	{
	  return BR_ER_METADB;
	}

      /* all nodes should have different node version for node ordering */
      next_node_ver++;
    }

  shd_mg_sync_local_mgmt_job_add ();

  return 0;
}

static int
shd_mg_node_add (UNUSED_ARG T_THR_FUNC_RES * thr_func_res,
		 CCI_CONN * conn,
		 const T_SHARD_DB_REC * shard_db_rec,
		 const T_MGMT_REQ_ARG * req_arg, const T_SHARD_MGMT_JOB * job)
{
  T_SHARD_NODE_INFO add_node;
  const T_SHARD_NODE_INFO *org_add_node;
  const T_SHARD_NODE_INFO *existing_node;
  int64_t next_node_ver;
  int add_node_status;
  bool exist_same_node;
  char global_dbname[DBNAME_MAX_SIZE];
  int error = 0;
  int shard_db_node_status = SHARD_DB_NODE_STATUS_ALL_VALID;
  T_SHARD_DB_REC copy_shard_db_rec;
  bool commit_success;
  const T_SHARD_NODE_INFO *del_node_info = NULL;
  int fail_recovery_error = 0;
  int mig_src_nodeid;
  const T_SHARD_NODE_INFO *run_mig_node = NULL;
  T_SHARD_NODE_INFO copy_run_mig_node;
  bool is_slave_mode_mig;

  copy_shard_db_rec = *shard_db_rec;
  shard_db_rec = &copy_shard_db_rec;

  if (shard_db_rec->node_status != SHARD_DB_NODE_STATUS_ALL_VALID)
    {
      return BR_ER_NODE_ADD_IN_PROGRESS;
    }

  org_add_node = &req_arg->value.node_add_arg.node_info;
  add_node = *org_add_node;

  LOCK_DB_SHARD_INFO ();

  COPY_GLOBAL_DBNAME (global_dbname);

  existing_node = find_node_info (db_Shard_info.db_node_info, org_add_node,
				  &exist_same_node);

  mig_src_nodeid = find_min_node_id (db_Shard_info.db_node_info);
  select_migration_node (&run_mig_node, NULL, &is_slave_mode_mig,
			 db_Shard_info.db_node_info, mig_src_nodeid,
			 org_add_node->node_id, NULL);

  if (run_mig_node != NULL)
    {
      /* run_mig_node info may be invalid after UNLOCK_DB_SHARD_INFO() */
      copy_run_mig_node = *run_mig_node;
      run_mig_node = &copy_run_mig_node;
    }
  UNLOCK_DB_SHARD_INFO ();

  if (existing_node != NULL)
    {
      return BR_ER_NODE_INFO_EXIST;
    }
  if (mig_src_nodeid > org_add_node->node_id)
    {
      return BR_ER_NODE_ADD_INVALID_SRC_NODE;
    }

  error = check_rebalance_job (conn, true, false);
  if (error < 0)
    {
      return error;
    }

  set_migrator_dba_passwd (req_arg->value.node_add_arg.dba_passwd);

  if (exist_same_node == true || mig_src_nodeid == 0)
    {
      add_node_status = ADD_NODE_STATUS_COMPLETE;
      next_node_ver = get_next_version (shard_db_rec);
    }
  else
    {
      if (run_mig_node == NULL)
	{
	  run_mig_node = &add_node;
	}

      add_node_status = ADD_NODE_STATUS_REQUESTED;
      next_node_ver = 0;
    }

  if (add_node_status != ADD_NODE_STATUS_COMPLETE)
    {
      /* change shard_db node_status */
      error = change_shrad_db_node_status (conn,
					   SHARD_DB_NODE_STATUS_EXIST_INVALID);
      if (error < 0)
	{
	  goto node_add_fail;
	}

      /* schema migration */
      error = node_add_commit_and_migration (conn, &commit_success,
					     global_dbname, mig_src_nodeid,
					     &add_node, run_mig_node, true,
					     is_slave_mode_mig,
					     &copy_shard_db_rec);


      br_log_write (BROKER_LOG_NOTICE, job->clt_ip_addr,
		    "[NODE-ADD %d:%s:%s:%d] migration schema: error=%d",
		    add_node.node_id, add_node.local_dbname,
		    add_node.host_ip_str,
		    PRM_NODE_INFO_GET_PORT (&add_node.host_info), error);

      if (commit_success)
	{
	  shard_db_node_status = SHARD_DB_NODE_STATUS_EXIST_INVALID;
	}

      if (error < 0)
	{
	  goto node_add_fail;
	}

      add_node_status = ADD_NODE_STATUS_SCHEMA_COMPLETE;

      next_node_ver = get_next_version (shard_db_rec);
    }

  assert (next_node_ver > 0);

  /* insert node info */
  error = insert_shard_node_table (conn, org_add_node, add_node_status,
				   next_node_ver);
  if (error < 0)
    {
      goto node_add_fail;
    }

  /* global table migration */
  if (add_node_status != ADD_NODE_STATUS_COMPLETE)
    {
      error = node_add_commit_and_migration (conn, &commit_success,
					     global_dbname, mig_src_nodeid,
					     &add_node, run_mig_node, false,
					     is_slave_mode_mig,
					     &copy_shard_db_rec);

      br_log_write (BROKER_LOG_NOTICE, job->clt_ip_addr,
		    "[NODE-ADD %d:%s:%s:%d] migration global table: error=%d",
		    add_node.node_id, add_node.local_dbname,
		    add_node.host_ip_str,
		    PRM_NODE_INFO_GET_PORT (&add_node.host_info), error);

      if (commit_success)
	{
	  del_node_info = org_add_node;
	}

      if (error < 0)
	{
	  goto node_add_fail;
	}

      error = update_shard_node_table_status (conn, org_add_node,
					      ADD_NODE_STATUS_COMPLETE);
      if (error < 0)
	{
	  goto node_add_fail;
	}
    }

  /* restore shard_db node_status */
  if (shard_db_node_status != SHARD_DB_NODE_STATUS_ALL_VALID)
    {
      error = change_shrad_db_node_status (conn,
					   SHARD_DB_NODE_STATUS_ALL_VALID);
      if (error < 0)
	{
	  goto node_add_fail;
	}
    }

  shd_mg_sync_local_mgmt_job_add ();

  br_log_write (BROKER_LOG_NOTICE, job->clt_ip_addr,
		"[NODE-ADD %d:%s:%s:%d] complete",
		org_add_node->node_id, org_add_node->local_dbname,
		org_add_node->host_ip_str,
		PRM_NODE_INFO_GET_PORT (&org_add_node->host_info));
  return 0;

node_add_fail:
  cci_end_tran (conn, CCI_TRAN_ROLLBACK);

  if (shard_db_node_status != SHARD_DB_NODE_STATUS_ALL_VALID)
    {
      if (del_node_info != NULL)
	{
	  if (sync_node_info (conn, &copy_shard_db_rec, true) < 0)
	    {
	      fail_recovery_error = -1;
	    }
	  else
	    {
	      next_node_ver = get_next_version (shard_db_rec);

	      if (delete_shard_node_table (conn, del_node_info, NULL,
					   next_node_ver) < 0)
		{
		  fail_recovery_error = -1;
		}
	    }
	}

      if (fail_recovery_error == 0 &&
	  change_shrad_db_node_status (conn,
				       SHARD_DB_NODE_STATUS_ALL_VALID) < 0)
	{
	  fail_recovery_error = -1;
	}

      if (fail_recovery_error == 0)
	{
	  cci_end_tran (conn, CCI_TRAN_COMMIT);
	}
    }

  br_log_write (BROKER_LOG_ERROR, job->clt_ip_addr,
		"[NODE-ADD %d:%s:%s:%d] fail: error=%d,%d",
		org_add_node->node_id, org_add_node->local_dbname,
		org_add_node->host_ip_str,
		PRM_NODE_INFO_GET_PORT (&org_add_node->host_info),
		error, fail_recovery_error);

  return error;
}

static int
change_shrad_db_node_status (CCI_CONN * conn, int node_status)
{
  T_SQL_AND_PARAM sql_and_param;

  sprintf (sql_and_param.sql, "UPDATE %s SET %s = %d WHERE %s = %d",
	   TABLE_SHARD_DB, COL_SHARD_DB_NODE_STATUS, node_status,
	   COL_SHARD_DB_ID, SHARD_DB_ID_VALUE);

  sql_and_param.num_bind = 0;
  sql_and_param.check_affected_rows = true;

  if (admin_query_execute_array (conn, 1, &sql_and_param) < 0)
    {
      return BR_ER_METADB;
    }

  return 0;
}

static int
node_add_commit_and_migration (CCI_CONN * conn, bool * commit_success,
			       const char *global_dbname, int src_nodeid,
			       const T_SHARD_NODE_INFO * dest_node_info,
			       const T_SHARD_NODE_INFO * run_node_info,
			       bool schema_migration, bool is_slave_mode_mig,
			       T_SHARD_DB_REC * new_shard_db_rec)
{
  T_CCI_LAUNCH_RESULT launch_res = CCI_LAUNCH_RESULT_INITIALIZER;
  int error;

  *commit_success = false;

  SHARD_DB_RESET_CHECK_TIME ();

  if (cci_end_tran (conn, CCI_TRAN_COMMIT) < 0)
    {
      /* migration operation may take long time.  release lock  */
      return BR_ER_METADB;
    }

  *commit_success = true;

  if (schema_migration == false)
    {
      unsigned int sleep_time = DRIVER_SHARD_INFO_SYNC_INTERVAL_SEC + 3;

      shd_mg_sync_local_mgmt_job_add ();

      /*
       * global table migration phase.
       * wait until drivers sync new node info
       */
      while (sleep_time > 0)
	{
	  sleep_time = sleep (sleep_time);
	}
    }

  error = launch_migrator_process (global_dbname, 0, src_nodeid,
				   dest_node_info, run_node_info,
				   schema_migration, is_slave_mode_mig,
				   &launch_res);
  if (error < 0)
    {
      br_log_write (BROKER_LOG_ERROR, INADDR_NONE,
		    "migrator fail: %s migration, "
		    "src_node=%d, dest_node = %d, cci error = %d",
		    (schema_migration ? "schema" : "global table"),
		    src_nodeid, dest_node_info->node_id, error);
      if (schema_migration)
	{
	  return BR_ER_SCHEMA_MIGRATION_FAIL;
	}
      else
	{
	  return BR_ER_GLOBAL_TABLE_MIGRATION_FAIL;
	}
    }

  if (launch_res.exit_status == 0)
    {
      br_log_write (BROKER_LOG_NOTICE, INADDR_NONE,
		    "exit migrator success: %s migration, "
		    "src_node=%d, dest_node = %d, "
		    "migrator@%s:%d exit status:%d (%s)",
		    (schema_migration ? "schema" : "global table"),
		    src_nodeid, dest_node_info->node_id,
		    run_node_info->host_ip_str,
		    PRM_NODE_INFO_GET_PORT (&run_node_info->host_info),
		    launch_res.exit_status,
		    launch_res.stdout_size > 0 ? launch_res.stdout_buf : "");
    }
  else
    {
      br_log_write (BROKER_LOG_ERROR, INADDR_NONE,
		    "exit migrator fail: %s migration, "
		    "src_node=%d, dest_node = %d, "
		    "migrator@%s:%d exit status:%d (%s)",
		    (schema_migration ? "schema" : "global table"),
		    src_nodeid, dest_node_info->node_id,
		    run_node_info->host_ip_str,
		    PRM_NODE_INFO_GET_PORT (&run_node_info->host_info),
		    launch_res.exit_status,
		    launch_res.stderr_size > 0 ? launch_res.stderr_buf : "");
      return BR_ER_SCHEMA_MIGRATION_FAIL;
    }

  /* re-sync node info.
     3rd parameter should be 'true' to hold global table lock */
  if (sync_node_info (conn, new_shard_db_rec, true) < 0)
    {
      return BR_ER_METADB;
    }

  return 0;
}

static int
insert_shard_node_table (CCI_CONN * conn, const T_SHARD_NODE_INFO * add_node,
			 int node_status, int64_t next_node_ver)
{
  T_SQL_AND_PARAM sql_and_param[2];
  int num_query = 0;

  assert (next_node_ver > 0);

  make_query_insert_shard_node (&sql_and_param[num_query++], add_node,
				&node_status, &next_node_ver);
  make_query_incr_node_ver (&sql_and_param[num_query++], &next_node_ver);

  if (admin_query_execute_array (conn, num_query, sql_and_param) < 0)
    {
      return BR_ER_METADB;
    }

  return 0;
}

static int
update_shard_node_table_status (CCI_CONN * conn,
				const T_SHARD_NODE_INFO * node_info,
				int node_status)
{
  T_SQL_AND_PARAM sql_and_param;
  int num_param = 0;

  sprintf (sql_and_param.sql,
	   "UPDATE %s SET %s = ? WHERE %s = ? AND %s = ? AND %s = ? AND %s = ?",
	   TABLE_SHARD_NODE, COL_SHARD_NODE_STATUS,
	   COL_SHARD_NODE_NODEID, COL_SHARD_NODE_DBNAME,
	   COL_SHARD_NODE_HOST, COL_SHARD_NODE_PORT);

  SET_BIND_PARAM (num_param++, sql_and_param.bind_param, CCI_TYPE_INT,
		  &node_status);
  SET_BIND_PARAM (num_param++, sql_and_param.bind_param, CCI_TYPE_INT,
		  &node_info->node_id);
  SET_BIND_PARAM (num_param++, sql_and_param.bind_param, CCI_TYPE_VARCHAR,
		  node_info->local_dbname);
  SET_BIND_PARAM (num_param++, sql_and_param.bind_param, CCI_TYPE_VARCHAR,
		  node_info->host_ip_str);
  SET_BIND_PARAM (num_param++, sql_and_param.bind_param, CCI_TYPE_INT,
		  &PRM_NODE_INFO_GET_PORT (&node_info->host_info));

  sql_and_param.num_bind = num_param;
  sql_and_param.check_affected_rows = true;

  if (admin_query_execute_array (conn, 1, &sql_and_param) < 0)
    {
      return BR_ER_METADB;
    }

  return 0;
}

static int
shd_mg_node_drop (UNUSED_ARG T_THR_FUNC_RES * thr_func_res,
		  CCI_CONN * conn,
		  const T_SHARD_DB_REC * shard_db_rec,
		  const T_MGMT_REQ_ARG * req_arg,
		  const T_SHARD_MGMT_JOB * job)
{
  T_SHARD_NODE_INFO drop_node_arg;
  T_SHARD_NODE_INFO *drop_node;
  int error = 0;
  int64_t next_node_ver;
  bool exist_same_node;
  int drop_all_nodeid;

  if (shard_db_rec->node_status != SHARD_DB_NODE_STATUS_ALL_VALID)
    {
      return BR_ER_NODE_ADD_IN_PROGRESS;
    }

  drop_node_arg = req_arg->value.node_drop_arg.node_info;
  drop_all_nodeid = req_arg->value.node_drop_arg.drop_all_nodeid;
  if (drop_all_nodeid > 0)
    {
      drop_node = NULL;
    }
  else
    {
      drop_node = &drop_node_arg;
    }

  LOCK_DB_SHARD_INFO ();

  if (drop_all_nodeid > 0)
    {
      error = check_nodeid_in_use (drop_all_nodeid,
				   db_Shard_info.db_groupid_info);
    }
  else
    {
      const T_SHARD_NODE_INFO *exist_node_info;
      exist_node_info = find_node_info (db_Shard_info.db_node_info,
					drop_node, &exist_same_node);
      if (exist_node_info == NULL)
	{
	  error = BR_ER_NODE_INFO_NOT_EXIST;
	}
      else if (exist_same_node == false)
	{
	  error = check_nodeid_in_use (drop_node->node_id,
				       db_Shard_info.db_groupid_info);
	}
    }

  UNLOCK_DB_SHARD_INFO ();

  if (error == 0)
    {
      error = check_rebalance_job (conn, true, false);
    }

  if (error < 0)
    {
      return error;
    }

  next_node_ver = get_next_version (shard_db_rec);

  if (delete_shard_node_table (conn, drop_node, &drop_all_nodeid,
			       next_node_ver) < 0)
    {
      if (drop_all_nodeid > 0)
	{
	  br_log_write (BROKER_LOG_ERROR, job->clt_ip_addr,
			"[NODE-DROP-ALL %d] fail", drop_all_nodeid);
	}
      else
	{
	  br_log_write (BROKER_LOG_ERROR, job->clt_ip_addr,
			"[NODE-DROP %d:%s:%s:%d] fail",
			drop_node->node_id, drop_node->local_dbname,
			drop_node->host_ip_str,
			PRM_NODE_INFO_GET_PORT (&drop_node->host_info));
	}
      return BR_ER_METADB;
    }

  shd_mg_sync_local_mgmt_job_add ();

  if (drop_all_nodeid > 0)
    {
      br_log_write (BROKER_LOG_NOTICE, job->clt_ip_addr,
		    "[NODE-DROP-ALL %d] complete", drop_all_nodeid);
    }
  else
    {
      br_log_write (BROKER_LOG_NOTICE, job->clt_ip_addr,
		    "[NODE-DROP %d:%s:%s:%d] complete",
		    drop_node->node_id, drop_node->local_dbname,
		    drop_node->host_ip_str,
		    PRM_NODE_INFO_GET_PORT (&drop_node->host_info));
    }
  return 0;
}

static int
delete_shard_node_table (CCI_CONN * conn, const T_SHARD_NODE_INFO * drop_node,
			 int *drop_all_nodeid, int64_t next_node_ver)
{
  T_SQL_AND_PARAM sql_and_param[2];

  if (drop_node == NULL)
    {
      assert (drop_all_nodeid != NULL);
      make_query_node_drop_all (&sql_and_param[0], drop_all_nodeid);
    }
  else
    {
      make_query_node_drop (&sql_and_param[0], drop_node);
    }
  make_query_incr_node_ver (&sql_and_param[1], &next_node_ver);

  if (admin_query_execute_array (conn, 2, sql_and_param) < 0)
    {
      return BR_ER_METADB;
    }

  return 0;
}

static int
shd_mg_migration_start (T_THR_FUNC_RES * thr_func_res,
			CCI_CONN * conn,
			const T_SHARD_DB_REC * shard_db_rec,
			const T_MGMT_REQ_ARG * req_arg,
			const T_SHARD_MGMT_JOB * job)
{
  const T_MGMT_REQ_ARG_SHARD_MIGRATION *mig_arg =
    &req_arg->value.migration_arg;
  int mig_groupid = mig_arg->mig_groupid;
  int dest_nodeid = mig_arg->dest_nodeid;
  int cur_status = GROUP_MIGRATION_STATUS_MIGRATOR_RUN;
  int next_status = GROUP_MIGRATION_STATUS_MIGRATION_STARTED;
  int64_t cur_ts = make_cur_ts_bigint ();

  if (shard_db_rec->ddl_req_count == 0 && shard_db_rec->gc_req_count == 0)
    {
      T_SQL_AND_PARAM sql_and_param[2];
      int src_nodeid = 0;
      bool exists_dest_node_info = false;

      LOCK_DB_SHARD_INFO ();

      if (true)
	{
	  const T_GROUPID_INFO *src_gid_info;

	  src_gid_info = find_groupid_info (db_Shard_info.db_groupid_info,
					    mig_groupid);
	  if (is_existing_nodeid (db_Shard_info.db_node_info, dest_nodeid))
	    {
	      exists_dest_node_info = true;
	    }
	  else
	    {
	      assert (false);
	    }

	  if (src_gid_info != NULL)
	    {
	      src_nodeid = src_gid_info->nodeid;
	    }
	}

      UNLOCK_DB_SHARD_INFO ();

      if (src_nodeid == 0 || src_nodeid == dest_nodeid ||
	  exists_dest_node_info == false)
	{
	  br_log_write (BROKER_LOG_ERROR, job->clt_ip_addr,
			"migration_start: groupid:%d src_nodeid = %d, dest_nodeid = %d\n",
			mig_groupid, src_nodeid, dest_nodeid);
	  return BR_ER_MIGRATION_INVALID_NODEID;
	}

      make_update_query_migration_start (&sql_and_param[0], &cur_ts,
					 &next_status,
					 &mig_groupid, &src_nodeid,
					 &dest_nodeid, &cur_status);
      make_query_incr_mig_req_count (&sql_and_param[1]);

      if (admin_query_execute_array (conn, 2, sql_and_param) < 0)
	{
	  br_log_write (BROKER_LOG_ERROR, job->clt_ip_addr,
			"migration_start fail: groupid: %d nodeid: %d -> %d\n",
			mig_groupid, src_nodeid, dest_nodeid);
	  return BR_ER_METADB;
	}

      br_log_write (BROKER_LOG_NOTICE, job->clt_ip_addr,
		    "migration_start: groupid: %d nodeid: %d -> %d\n",
		    mig_groupid, src_nodeid, dest_nodeid);

      *thr_func_res = THR_FUNC_COMPLETE_AND_WAIT_NEXT;
    }
  else
    {
      if (is_expired_request (job->request_time, mig_arg->timeout_sec))
	{
	  br_log_write (BROKER_LOG_ERROR, job->clt_ip_addr,
			"migration_start request timeout(%d)",
			mig_arg->timeout_sec);
	  return BR_ER_REQUEST_TIMEOUT;
	}
      else
	{
	  *thr_func_res = THR_FUNC_NEED_QUEUEING;
	}
    }

  return 0;
}

static int64_t
get_next_version (const T_SHARD_DB_REC * shard_db_rec)
{
  int64_t cur_last_ver;

  cur_last_ver = MAX (shard_db_rec->gid_info_last_ver,
		      shard_db_rec->node_info_last_ver);

  return (cur_last_ver + 1);
}

static int
shd_mg_migration_end (UNUSED_ARG T_THR_FUNC_RES * thr_func_res,
		      CCI_CONN * conn,
		      const T_SHARD_DB_REC * shard_db_rec,
		      const T_MGMT_REQ_ARG * req_arg,
		      UNUSED_ARG const T_SHARD_MGMT_JOB * job)
{
  const T_MGMT_REQ_ARG_SHARD_MIGRATION *mig_arg =
    &req_arg->value.migration_arg;
  int64_t next_gid_ver;

  next_gid_ver = get_next_version (shard_db_rec);

  return (shd_mg_migration_end_internal (conn, mig_arg, next_gid_ver));
}

static int
shd_mg_migration_fail (CCI_CONN * conn, const void *arg)
{
  return (shd_mg_migration_end_internal (conn, arg, 0));
}

static int
shd_mg_migration_end_internal (CCI_CONN * conn,
			       const T_MGMT_REQ_ARG_SHARD_MIGRATION * mig_arg,
			       int64_t next_gid_ver)
{
  T_SQL_AND_PARAM sql_and_param[4];
  int num_sql = 0;
  int next_status;
  int cur_status;
  int error = 0;
  int64_t cur_ts = make_cur_ts_bigint ();

  make_query_decr_mig_req_count (&sql_and_param[num_sql++]);

  if (next_gid_ver > 0)
    {
      next_status = GROUP_MIGRATION_STATUS_COMPLETE;
      cur_status = GROUP_MIGRATION_STATUS_MIGRATION_STARTED;

      make_update_query_migration_end (&sql_and_param[num_sql++],
				       &next_status, &cur_ts,
				       &mig_arg->num_shard_keys,
				       &mig_arg->mig_groupid, &cur_status);
      make_query_update_groupid (&sql_and_param[num_sql++],
				 &mig_arg->mig_groupid, &mig_arg->dest_nodeid,
				 &next_gid_ver);
      make_query_incr_last_groupid (&sql_and_param[num_sql++], &next_gid_ver);
    }
  else
    {
      next_status = GROUP_MIGRATION_STATUS_FAILED;
      make_update_query_migration_end (&sql_and_param[num_sql++],
				       &next_status, &cur_ts,
				       &mig_arg->num_shard_keys,
				       &mig_arg->mig_groupid, NULL);
    }

  if (admin_query_execute_array (conn, num_sql, sql_and_param) < 0)
    {
      cci_end_tran (conn, CCI_TRAN_ROLLBACK);
      if (next_gid_ver > 0)
	{
	  if (shd_mg_migration_end_internal (conn, mig_arg, 0) == 0)
	    {
	      cci_end_tran (conn, CCI_TRAN_COMMIT);
	    }
	}
      error = BR_ER_METADB;
    }

  if (error == 0 && next_gid_ver > 0)
    {
      shd_mg_sync_local_mgmt_job_add ();
    }

  shd_mg_migration_handler_wakeup ();

  return error;
}

static int
shd_mg_ddl_start (T_THR_FUNC_RES * thr_func_res,
		  CCI_CONN * conn,
		  const T_SHARD_DB_REC * shard_db_rec,
		  const T_MGMT_REQ_ARG * req_arg,
		  const T_SHARD_MGMT_JOB * job)
{
  T_SQL_AND_PARAM sql_and_param[1];

  if (shard_db_rec->node_status != SHARD_DB_NODE_STATUS_ALL_VALID)
    {
      return BR_ER_NODE_ADD_IN_PROGRESS;
    }

  if (job->is_retry == false)
    {
      make_query_incr_ddl_req_count (&sql_and_param[0]);
      if (admin_query_execute_array (conn, 1, sql_and_param) < 0)
	{
	  return BR_ER_METADB;
	}
    }
  else
    {
      if (is_expired_request (job->request_time,
			      req_arg->value.ddl_arg.timeout_sec))
	{
	  shd_mg_ddl_fail (conn, NULL);
	  cci_end_tran (conn, CCI_TRAN_COMMIT);

	  br_log_write (BROKER_LOG_ERROR, job->clt_ip_addr,
			"ddl_start request timeout(%d)",
			req_arg->value.ddl_arg.timeout_sec);

	  return BR_ER_REQUEST_TIMEOUT;
	}
    }

  if (shard_db_rec->mig_req_count > 0)
    {
      *thr_func_res = THR_FUNC_NEED_QUEUEING;
    }
  else
    {
      *thr_func_res = THR_FUNC_COMPLETE_AND_WAIT_NEXT;
    }

  return 0;
}

static int
shd_mg_ddl_end (UNUSED_ARG T_THR_FUNC_RES * thr_func_res,
		CCI_CONN * conn,
		UNUSED_ARG const T_SHARD_DB_REC * shard_db_rec,
		UNUSED_ARG const T_MGMT_REQ_ARG * req_arg,
		UNUSED_ARG const T_SHARD_MGMT_JOB * job)
{
  return (shd_mg_ddl_end_internal (conn));
}

static int
shd_mg_ddl_fail (CCI_CONN * conn, UNUSED_ARG const void *arg)
{
  return (shd_mg_ddl_end_internal (conn));
}

static int
shd_mg_ddl_end_internal (CCI_CONN * conn)
{
  T_SQL_AND_PARAM sql_and_param[1];

  make_query_decr_ddl_req_count (&sql_and_param[0]);
  if (admin_query_execute_array (conn, 1, sql_and_param) < 0)
    {
      return BR_ER_METADB;
    }

  return 0;
}

static int
shd_mg_gc_start (T_THR_FUNC_RES * thr_func_res,
		 CCI_CONN * conn,
		 const T_SHARD_DB_REC * shard_db_rec,
		 UNUSED_ARG const T_MGMT_REQ_ARG * req_arg,
		 UNUSED_ARG const T_SHARD_MGMT_JOB * job)
{
  T_SQL_AND_PARAM sql_and_param[1];

  if (shard_db_rec->node_status != SHARD_DB_NODE_STATUS_ALL_VALID)
    {
      return BR_ER_NODE_ADD_IN_PROGRESS;
    }


  if (shard_db_rec->mig_req_count > 0)
    {
      return BR_ER_REBALANCE_RUNNING;
    }
  else
    {
      make_query_incr_gc_req_count (&sql_and_param[0]);
      if (admin_query_execute_array (conn, 1, sql_and_param) < 0)
	{
	  return BR_ER_METADB;
	}
    }

  *thr_func_res = THR_FUNC_COMPLETE_AND_WAIT_NEXT;

  return 0;
}

static int
shd_mg_gc_end (UNUSED_ARG T_THR_FUNC_RES * thr_func_res,
	       CCI_CONN * conn,
	       UNUSED_ARG const T_SHARD_DB_REC * shard_db_rec,
	       UNUSED_ARG const T_MGMT_REQ_ARG * req_arg,
	       UNUSED_ARG const T_SHARD_MGMT_JOB * job)
{
  return (shd_mg_gc_end_internal (conn));
}

static int
shd_mg_gc_fail (CCI_CONN * conn, UNUSED_ARG const void *arg)
{
  return (shd_mg_gc_end_internal (conn));
}

static int
shd_mg_gc_end_internal (CCI_CONN * conn)
{
  T_SQL_AND_PARAM sql_and_param[1];

  make_query_decr_gc_req_count (&sql_and_param[0]);
  if (admin_query_execute_array (conn, 1, sql_and_param) < 0)
    {
      return BR_ER_METADB;
    }

  return 0;
}


static int
check_incomplete_node (CCI_CONN * conn)
{
  char sql[SQL_BUF_SIZE];
  int res;

  sprintf (sql, "SELECT 1 FROM %s WHERE %s <> %d",
	   TABLE_SHARD_NODE, COL_SHARD_NODE_STATUS, ADD_NODE_STATUS_COMPLETE);

  res = admin_query_execute (conn, true, NULL, sql, 0, NULL);

  if (res < 0)
    {
      return BR_ER_METADB;
    }
  else if (res > 0)
    {
      return BR_ER_NODE_ADD_IN_PROGRESS;
    }
  else
    {
      return 0;
    }
}

static int
check_rebalance_job (CCI_CONN * conn, bool ignore_fail_job, bool rm_prev_job)
{
  char sql[SQL_BUF_SIZE];
  int res;
  T_BIND_PARAM bind_param[1];
  int num_bind_param = 0;
  char *p = sql;
  int status_val1, status_val2;

  if (ignore_fail_job)
    {
      status_val1 = GROUP_MIGRATION_STATUS_COMPLETE;
      status_val2 = GROUP_MIGRATION_STATUS_FAILED;
    }
  else
    {
      status_val1 = GROUP_MIGRATION_STATUS_COMPLETE;
      status_val2 = GROUP_MIGRATION_STATUS_COMPLETE;
    }

  APPEND_QUERY_STR (p,
		    "SELECT 1 FROM %s WHERE %s > 0 AND %s NOT IN (%d, %d) ",
		    TABLE_SHARD_MIGRATION, COL_SHARD_MIGRATION_SRC_NODEID,
		    COL_SHARD_MIGRATION_STATUS, status_val1, status_val2);

  APPEND_QUERY_STR (p, " LIMIT 1 ");

  assert ((int) DIM (bind_param) >= num_bind_param);

  res = admin_query_execute (conn, true, NULL, sql,
			     num_bind_param, bind_param);

  if (res < 0)
    {
      return BR_ER_METADB;
    }
  else if (res > 0)
    {
      return BR_ER_REBALANCE_RUNNING;
    }

  if (rm_prev_job)
    {
      sprintf (sql, "DELETE FROM %s WHERE %s > 0 AND %s IN (%d, %d) ",
	       TABLE_SHARD_MIGRATION, COL_SHARD_MIGRATION_SRC_NODEID,
	       COL_SHARD_MIGRATION_STATUS, status_val1, status_val2);

      res = admin_query_execute (conn, true, NULL, sql, 0, NULL);
      if (res < 0)
	{
	  return BR_ER_METADB;
	}
    }

  return 0;
}

static int
mig_info_sort_func (const void *arg1, const void *arg2)
{
  const T_GROUP_MIGRATION_INFO *info1 = arg1;
  const T_GROUP_MIGRATION_INFO *info2 = arg2;

  if (info1->src_nodeid == info2->src_nodeid)
    {
      return (info1->mig_order - info2->mig_order);
    }

  return (info1->src_nodeid - info2->src_nodeid);
}

static void
shuffle_and_sort_migration_info (int mig_count,
				 T_GROUP_MIGRATION_INFO * mig_info)
{
  struct drand48_data rand_buf;
  int i, idx;
  double r;
  T_GROUP_MIGRATION_INFO tmp_info;

  if (mig_count <= 0)
    {
      return;
    }

  init_drand48_buffer (&rand_buf);

  for (i = mig_count - 1; i > 0; i--)
    {
      drand48_r (&rand_buf, &r);
      idx = (int) ((i + 1) * r);

      tmp_info = mig_info[idx];
      mig_info[idx] = mig_info[i];
      mig_info[i] = tmp_info;
    }

  for (i = 0; i < mig_count; i++)
    {
      mig_info[i].mig_order = i + 1;
    }

  qsort (mig_info, mig_count, sizeof (T_GROUP_MIGRATION_INFO),
	 mig_info_sort_func);
}

static int
shd_mg_rebalance_req (UNUSED_ARG T_THR_FUNC_RES * thr_func_res,
		      CCI_CONN * conn,
		      const T_SHARD_DB_REC * shard_db_rec,
		      const T_MGMT_REQ_ARG * req_arg,
		      UNUSED_ARG const T_SHARD_MGMT_JOB * job)
{
  int mig_count = 0;
  T_GROUP_MIGRATION_INFO *mig_info = NULL;
  const T_MGMT_REQ_ARG_REBALANCE_REQ *rbl_req_arg;
  int error = 0;

  rbl_req_arg = &req_arg->value.rebalance_req_arg;

  if (shard_db_rec->node_status != SHARD_DB_NODE_STATUS_ALL_VALID)
    {
      return BR_ER_NODE_ADD_IN_PROGRESS;
    }

  error = check_incomplete_node (conn);
  if (error < 0)
    {
      return error;
    }

  error = check_rebalance_job (conn, rbl_req_arg->ignore_prev_fail, true);
  if (error < 0)
    {
      return error;
    }

  LOCK_DB_SHARD_INFO ();

  mig_count = make_rebalance_info (&mig_info, rbl_req_arg->empty_node,
				   rbl_req_arg->num_src_nodes,
				   rbl_req_arg->src_nodeid,
				   rbl_req_arg->num_dest_nodes,
				   rbl_req_arg->dest_nodeid,
				   db_Shard_info.db_node_info,
				   db_Shard_info.db_groupid_info);

  UNLOCK_DB_SHARD_INFO ();


  if (mig_count < 0)
    {
      RYE_FREE_MEM (mig_info);
      return mig_count;
    }
  else
    {
      char sql[SQL_BUF_SIZE];
      CCI_STMT stmt;
      T_BIND_PARAM bind_param[COL_COUNT_SHARD_MIGRATION];
      int mig_status = GROUP_MIGRATION_STATUS_SCHEDULED;
      int i, res;
      int64_t cur_ts = make_cur_ts_bigint ();
      const char *mgmt_hostname = "";
      int mgmt_pid = 0;
      int run_time = 0;
      int num_shard_keys = 0;

      shuffle_and_sort_migration_info (mig_count, mig_info);

      sprintf (sql,
	       "INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
	       "        VALUES (?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ?,  ? ) ",
	       TABLE_SHARD_MIGRATION,
	       COL_SHARD_MIGRATION_GROUPID, COL_SHARD_MIGRATION_MGMT_HOST,
	       COL_SHARD_MIGRATION_MGMT_PID, COL_SHARD_MIGRATION_SRC_NODEID,
	       COL_SHARD_MIGRATION_DEST_NODEID, COL_SHARD_MIGRATION_STATUS,
	       COL_SHARD_MIGRATION_MIG_ORDER, COL_SHARD_MIGRATION_MODIFIED_AT,
	       COL_SHARD_MIGRATION_RUN_TIME, COL_SHARD_MIGRATION_SHARD_KEYS);

      error = cci_prepare (conn, &stmt, sql, 0);
      if (error < 0)
	{
	  BR_LOG_CCI_ERROR ("prepare", conn->err_buf.err_code,
			    conn->err_buf.err_msg, sql);
	  mig_count = 0;
	}

      for (i = 0; i < mig_count; i++)
	{
	  int num_bind = 0;

	  if (mig_info[i].groupid <= 0 ||
	      mig_info[i].src_nodeid <= 0 ||
	      mig_info[i].dest_nodeid <= 0 ||
	      mig_info[i].src_nodeid == mig_info[i].dest_nodeid)
	    {
	      assert (false);
	      continue;
	    }

	  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT,
			  &mig_info[i].groupid);
	  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_VARCHAR,
			  mgmt_hostname);
	  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT, &mgmt_pid);
	  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT,
			  &mig_info[i].src_nodeid);
	  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT,
			  &mig_info[i].dest_nodeid);
	  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT, &mig_status);
	  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT,
			  &mig_info[i].mig_order);
	  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_BIGINT, &cur_ts);
	  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT, &run_time);
	  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT,
			  &num_shard_keys);

	  assert (COL_COUNT_SHARD_MIGRATION == num_bind);

	  res = admin_query_execute (conn, false, &stmt, sql,
				     num_bind, bind_param);
	  if (res < 0)
	    {
	      error = res;
	      break;
	    }

	  assert (res == 1);
	}
    }

  RYE_FREE_MEM (mig_info);

  if (error < 0)
    {
      mig_count = error;
    }
  else
    {
      set_migrator_dba_passwd (rbl_req_arg->dba_passwd);

      shd_mg_migration_handler_wakeup ();
    }

  return mig_count;
}

static int
shd_mg_rebalance_job_count (UNUSED_ARG T_THR_FUNC_RES * thr_func_res,
			    CCI_CONN * conn,
			    UNUSED_ARG const T_SHARD_DB_REC * shard_db_rec,
			    const T_MGMT_REQ_ARG * req_arg,
			    UNUSED_ARG const T_SHARD_MGMT_JOB * job)
{
  char sql[SQL_BUF_SIZE];
  CCI_STMT stmt;
  int count, ind;
  const T_MGMT_REQ_ARG_REBALANCE_JOB_COUNT *job_count_arg;

  job_count_arg = &req_arg->value.rebalance_job_count;

  if (job_count_arg->job_type == MGMT_REBALANCE_JOB_COUNT_TYPE_COMPLETE)
    {
      sprintf (sql,
	       "SELECT count(*) FROM %s WHERE %s > 0 AND %s = %d ",
	       TABLE_SHARD_MIGRATION, COL_SHARD_MIGRATION_SRC_NODEID,
	       COL_SHARD_MIGRATION_STATUS, GROUP_MIGRATION_STATUS_COMPLETE);
    }
  else if (job_count_arg->job_type == MGMT_REBALANCE_JOB_COUNT_TYPE_FAILED)
    {
      sprintf (sql,
	       "SELECT count(*) FROM %s WHERE %s > 0 AND %s = %d ",
	       TABLE_SHARD_MIGRATION, COL_SHARD_MIGRATION_SRC_NODEID,
	       COL_SHARD_MIGRATION_STATUS, GROUP_MIGRATION_STATUS_FAILED);
    }
  else
    {
      sprintf (sql,
	       "SELECT count(*) FROM %s WHERE %s > 0 AND %s NOT IN (%d, %d) ",
	       TABLE_SHARD_MIGRATION, COL_SHARD_MIGRATION_SRC_NODEID,
	       COL_SHARD_MIGRATION_STATUS,
	       GROUP_MIGRATION_STATUS_COMPLETE,
	       GROUP_MIGRATION_STATUS_FAILED);
    }

  if (cci_prepare (conn, &stmt, sql, 0) < 0)
    {
      BR_LOG_CCI_ERROR ("prepare", conn->err_buf.err_code,
			conn->err_buf.err_msg, sql);
      return BR_ER_METADB;
    }

  if (cci_execute (&stmt, 0, 0) < 0)
    {
      BR_LOG_CCI_ERROR ("execute", stmt.err_buf.err_code,
			stmt.err_buf.err_msg, sql);
      return BR_ER_METADB;
    }

  if (cci_fetch_next (&stmt) < 0)
    {
      BR_LOG_CCI_ERROR ("execute", stmt.err_buf.err_code,
			stmt.err_buf.err_msg, sql);
      return BR_ER_METADB;
    }

  count = cci_get_int (&stmt, 1, &ind);
  if (count < 0 || ind < 0)
    {
      return BR_ER_METADB;
    }

  return count;
}

static void
set_migrator_dba_passwd (const char *dba_passwd)
{
  if (dba_passwd == NULL)
    {
      shard_Mgmt_server_info.migrator_dba_passwd[0] = '\0';
    }
  else
    {
      STRNCPY (shard_Mgmt_server_info.migrator_dba_passwd, dba_passwd,
	       sizeof (shard_Mgmt_server_info.migrator_dba_passwd));
    }

}

static bool
is_expired_request (time_t request_time, int timeout_sec)
{
  return ((timeout_sec > 0) &&
	  ((int) (time (NULL) - request_time) > timeout_sec));
}

static int
insert_shard_groupid_table (CCI_CONN * conn, int all_groupid_count,
			    int init_num_node, const int *init_nodeid_arr,
			    int64_t init_gid_ver)
{
  CCI_STMT stmt;
  int nodeid;
  int shard_groupid_count;
  int next_nodeid_incr;
  int i;
  T_BIND_PARAM bind_param[COL_COUNT_SHARD_GROUPID];
  char sql[SQL_BUF_SIZE];
  int error;
  int init_nodeid_arr_idx = 0;

  if (init_num_node <= 0)
    {
      assert (false);
      return BR_ER_INVALID_ARGUMENT;
    }

  shard_groupid_count = all_groupid_count / init_num_node;
  if (shard_groupid_count <= 0)
    {
      shard_groupid_count = 1;
    }
  next_nodeid_incr = shard_groupid_count;

  sprintf (sql, "INSERT INTO %s (%s, %s, %s) VALUES (?, ?, ?)",
	   TABLE_SHARD_GROUPID,
	   COL_SHARD_GROUPID_GROUPID, COL_SHARD_GROUPID_NODEID,
	   COL_SHARD_GROUPID_VERSION);

  if (cci_prepare (conn, &stmt, sql, 0) < 0)
    {
      BR_LOG_CCI_ERROR ("prepare", conn->err_buf.err_code,
			conn->err_buf.err_msg, sql);
      return BR_ER_METADB;
    }

  nodeid = init_nodeid_arr[init_nodeid_arr_idx++];

  for (i = 1; i <= all_groupid_count; i++)
    {
      int num_bind = 0;
      SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT, &i);
      SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT, &nodeid);
      SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_BIGINT, &init_gid_ver);

      assert (COL_COUNT_SHARD_GROUPID == num_bind);

      error = admin_query_execute (conn, false, &stmt, sql,
				   num_bind, bind_param);
      if (error < 0)
	{
	  return error;
	}
      else if (error == 0)
	{
	  br_log_write (BROKER_LOG_ERROR, INADDR_NONE, "%s: affected row = 0",
			sql);
	  return BR_ER_METADB;
	}

      if (i >= next_nodeid_incr)
	{
	  if (init_nodeid_arr_idx < init_num_node)
	    {
	      nodeid = init_nodeid_arr[init_nodeid_arr_idx++];
	      next_nodeid_incr += shard_groupid_count;
	    }
	}
    }

  return 0;
}

static void
make_query_create_global_table (T_SQL_AND_PARAM * sql_and_param,
				const T_TABLE_DEF * table)
{
  int i;
  char *p = sql_and_param->sql;
  int num_pk = 0;
  const char *pk[MAX_COLUMNS];

  APPEND_QUERY_STR (p, "CREATE GLOBAL TABLE %s ( ", table->table_name);

  for (i = 0; i < table->num_columns; i++)
    {
      APPEND_QUERY_STR (p, " %s ", table->columns[i].name);
      switch (table->columns[i].type)
	{
	case COLUMN_TYPE_INT:
	  APPEND_QUERY_STR (p, " INT ");
	  break;
	case COLUMN_TYPE_STRING:
	  APPEND_QUERY_STR (p, " STRING ");
	  break;
	case COLUMN_TYPE_BIGINT:
	  APPEND_QUERY_STR (p, " BIGINT ");
	  break;
	case COLUMN_TYPE_DATETIME:
	  APPEND_QUERY_STR (p, " DATETIME ");
	  break;
	default:
	  break;
	}
      APPEND_QUERY_STR (p, " NOT NULL, ");
      if (table->columns[i].is_pk)
	{
	  pk[num_pk++] = table->columns[i].name;
	}
    }

  APPEND_QUERY_STR (p, " PRIMARY KEY( ");

  for (i = 0; i < num_pk; i++)
    {
      if (i != 0)
	{
	  APPEND_QUERY_STR (p, ", ");
	}

      APPEND_QUERY_STR (p, "%s", pk[i]);
    }

  APPEND_QUERY_STR (p, ") )");

  sql_and_param->num_bind = 0;
  sql_and_param->check_affected_rows = false;
}

static void
make_query_create_index (T_SQL_AND_PARAM * sql_and_param,
			 const T_INDEX_DEF * index_def)
{
  char *p = sql_and_param->sql;
  int i;

  APPEND_QUERY_STR (p, "CREATE %s INDEX %s on %s (",
		    (index_def->is_unique ? "UNIQUE" : ""),
		    index_def->index_name, index_def->table_name);

  for (i = 0; index_def->column_name[i] != NULL; i++)
    {
      if (i != 0)
	{
	  APPEND_QUERY_STR (p, ", ");
	}

      APPEND_QUERY_STR (p, "%s", index_def->column_name[i]);
    }

  APPEND_QUERY_STR (p, ")");

  sql_and_param->num_bind = 0;
  sql_and_param->check_affected_rows = false;
}

static void
make_query_create_user (T_SQL_AND_PARAM * sql_and_param)
{
  sprintf (sql_and_param->sql, "CREATE USER %s PASSWORD '%s'",
	   shard_Mgmt_db_user, shard_Mgmt_db_passwd);

  sql_and_param->num_bind = 0;
  sql_and_param->check_affected_rows = false;
}

static void
make_query_change_owner (T_SQL_AND_PARAM * sql_and_param,
			 const T_TABLE_DEF * table)
{
  sprintf (sql_and_param->sql, "ALTER TABLE %s OWNER TO %s",
	   table->table_name, shard_Mgmt_db_user);

  sql_and_param->num_bind = 0;
  sql_and_param->check_affected_rows = false;
}

static void
make_query_insert_shard_db (T_SQL_AND_PARAM * sql_and_param,
			    const int *shard_db_id,
			    const T_SHARD_DB_REC * shard_db_rec)
{
  T_BIND_PARAM *bind_param = sql_and_param->bind_param;
  int num_bind = 0;

  sprintf (sql_and_param->sql,
	   "INSERT INTO %s (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
	   "        VALUES (?,  ?,  ?,  ?,  ?,  ?,  ?,  ?, ?, ?)",
	   TABLE_SHARD_DB,
	   COL_SHARD_DB_ID, COL_SHARD_DB_DB_NAME,
	   COL_SHARD_DB_GROUPID_COUNT, COL_SHARD_DB_GROUPID_LAST_VER,
	   COL_SHARD_DB_NODE_LAST_VER, COL_SHARD_DB_MIG_REQ_COUNT,
	   COL_SHARD_DB_DDL_REQ_COUNT, COL_SHARD_DB_GC_REQ_COUNT,
	   COL_SHARD_DB_NODE_STATUS, COL_SHARD_DB_CREATED_AT);

  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT, shard_db_id);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_VARCHAR,
		  shard_db_rec->global_dbname);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT,
		  &shard_db_rec->groupid_count);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_BIGINT,
		  &shard_db_rec->gid_info_last_ver);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_BIGINT,
		  &shard_db_rec->node_info_last_ver);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT,
		  &shard_db_rec->mig_req_count);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT,
		  &shard_db_rec->ddl_req_count);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT,
		  &shard_db_rec->gc_req_count);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT,
		  &shard_db_rec->node_status);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_BIGINT,
		  &shard_db_rec->created_at);

  assert (num_bind == COL_COUNT_SHARD_DB);

  sql_and_param->num_bind = num_bind;
  sql_and_param->check_affected_rows = true;
}

static void
make_update_query_migration_start (T_SQL_AND_PARAM * sql_and_param,
				   const int64_t * cur_ts,
				   const int *next_status,
				   const int *mig_groupid,
				   const int *src_nodeid,
				   const int *dest_nodeid,
				   const int *cur_status)
{
  T_BIND_PARAM *bind_param = sql_and_param->bind_param;
  const char *hostname = shard_Mgmt_server_info.hostname;
  const int *pid = &shard_Mgmt_server_info.pid;
  int num_bind = 0;

  sprintf (sql_and_param->sql,
	   "UPDATE %s SET %s = ?, %s = ?, %s = ?, %s = ? WHERE %s = ? AND %s = ? AND %s = ? AND %s = ?",
	   TABLE_SHARD_MIGRATION,
	   COL_SHARD_MIGRATION_MGMT_HOST, COL_SHARD_MIGRATION_MGMT_PID,
	   COL_SHARD_MIGRATION_MODIFIED_AT, COL_SHARD_MIGRATION_STATUS,
	   COL_SHARD_MIGRATION_GROUPID, COL_SHARD_MIGRATION_SRC_NODEID,
	   COL_SHARD_MIGRATION_DEST_NODEID, COL_SHARD_MIGRATION_STATUS);

  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_VARCHAR, hostname);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT, pid);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_BIGINT, cur_ts);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT, next_status);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT, mig_groupid);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT, src_nodeid);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT, dest_nodeid);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT, cur_status);

  sql_and_param->num_bind = num_bind;
  sql_and_param->check_affected_rows = true;
}

static void
make_update_query_migration_end (T_SQL_AND_PARAM * sql_and_param,
				 const int *next_status,
				 const int64_t * cur_ts,
				 const int *num_shard_keys,
				 const int *groupid, const int *cur_status)
{
  char *p = sql_and_param->sql;
  int num_bind = 0;

  APPEND_QUERY_STR (p,
		    "UPDATE %s SET %s = ?, %s = ?, %s = ? - %s, %s = ? WHERE %s = ?",
		    TABLE_SHARD_MIGRATION, COL_SHARD_MIGRATION_STATUS,
		    COL_SHARD_MIGRATION_MODIFIED_AT,
		    COL_SHARD_MIGRATION_RUN_TIME,
		    COL_SHARD_MIGRATION_MODIFIED_AT,
		    COL_SHARD_MIGRATION_SHARD_KEYS,
		    COL_SHARD_MIGRATION_GROUPID);
  if (cur_status != NULL)
    {
      APPEND_QUERY_STR (p, " AND %s = ?", COL_SHARD_MIGRATION_STATUS);
    }

  SET_BIND_PARAM (num_bind++, sql_and_param->bind_param,
		  CCI_TYPE_INT, next_status);
  SET_BIND_PARAM (num_bind++, sql_and_param->bind_param,
		  CCI_TYPE_BIGINT, cur_ts);
  SET_BIND_PARAM (num_bind++, sql_and_param->bind_param,
		  CCI_TYPE_BIGINT, cur_ts);
  SET_BIND_PARAM (num_bind++, sql_and_param->bind_param,
		  CCI_TYPE_INT, num_shard_keys);
  SET_BIND_PARAM (num_bind++, sql_and_param->bind_param,
		  CCI_TYPE_INT, groupid);

  if (cur_status != NULL)
    {
      SET_BIND_PARAM (num_bind++, sql_and_param->bind_param,
		      CCI_TYPE_INT, cur_status);
    }

  sql_and_param->num_bind = num_bind;
  sql_and_param->check_affected_rows = true;
}

static void
make_query_insert_shard_node (T_SQL_AND_PARAM * sql_and_param,
			      const T_SHARD_NODE_INFO * node_info,
			      const int *node_status,
			      const int64_t * node_version)
{
  T_BIND_PARAM *bind_param = sql_and_param->bind_param;
  int num_bind = 0;

  sprintf (sql_and_param->sql,
	   "INSERT INTO %s (%s, %s, %s, %s, %s, %s) values (?, ?, ?, ?, ?, ?)",
	   TABLE_SHARD_NODE,
	   COL_SHARD_NODE_NODEID, COL_SHARD_NODE_DBNAME, COL_SHARD_NODE_HOST,
	   COL_SHARD_NODE_PORT, COL_SHARD_NODE_STATUS,
	   COL_SHARD_NODE_VERSION);

  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT, &node_info->node_id);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_VARCHAR,
		  node_info->local_dbname);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_VARCHAR,
		  node_info->host_ip_str);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT,
		  &PRM_NODE_INFO_GET_PORT (&node_info->host_info));
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT, node_status);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_BIGINT, node_version);

  assert (COL_COUNT_SHARD_NODE == num_bind);

  sql_and_param->num_bind = num_bind;
  sql_and_param->check_affected_rows = true;
}

static void
make_query_node_drop (T_SQL_AND_PARAM * sql_and_param,
		      const T_SHARD_NODE_INFO * node_info)
{
  T_BIND_PARAM *bind_param = sql_and_param->bind_param;
  int num_bind = 0;

  sprintf (sql_and_param->sql,
	   "DELETE FROM %s WHERE %s = ? AND %s = ? AND %s = ? AND %s = ?",
	   TABLE_SHARD_NODE,
	   COL_SHARD_NODE_NODEID, COL_SHARD_NODE_DBNAME,
	   COL_SHARD_NODE_HOST, COL_SHARD_NODE_PORT);

  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT, &node_info->node_id);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_VARCHAR,
		  node_info->local_dbname);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_VARCHAR,
		  node_info->host_ip_str);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT,
		  &PRM_NODE_INFO_GET_PORT (&node_info->host_info));

  sql_and_param->num_bind = num_bind;
  sql_and_param->check_affected_rows = true;
}

static void
make_query_node_drop_all (T_SQL_AND_PARAM * sql_and_param, int *nodeid)
{
  T_BIND_PARAM *bind_param = sql_and_param->bind_param;
  int num_bind = 0;

  sprintf (sql_and_param->sql, "DELETE FROM %s WHERE %s = ?",
	   TABLE_SHARD_NODE, COL_SHARD_NODE_NODEID);

  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT, nodeid);

  sql_and_param->num_bind = num_bind;
  sql_and_param->check_affected_rows = true;
}

static void
make_query_incr_mig_req_count (T_SQL_AND_PARAM * sql_and_param)
{
  sprintf (sql_and_param->sql, "UPDATE %s SET %s = %s + 1 ",
	   TABLE_SHARD_DB,
	   COL_SHARD_DB_MIG_REQ_COUNT, COL_SHARD_DB_MIG_REQ_COUNT);

  sql_and_param->num_bind = 0;
  sql_and_param->check_affected_rows = true;
}

static void
make_query_decr_mig_req_count (T_SQL_AND_PARAM * sql_and_param)
{
  sprintf (sql_and_param->sql, "UPDATE %s SET %s = %s - 1 ",
	   TABLE_SHARD_DB,
	   COL_SHARD_DB_MIG_REQ_COUNT, COL_SHARD_DB_MIG_REQ_COUNT);

  sql_and_param->num_bind = 0;
  sql_and_param->check_affected_rows = true;
}

static void
make_query_incr_ddl_req_count (T_SQL_AND_PARAM * sql_and_param)
{
  sprintf (sql_and_param->sql, "UPDATE %s SET %s = %s + 1 ",
	   TABLE_SHARD_DB,
	   COL_SHARD_DB_DDL_REQ_COUNT, COL_SHARD_DB_DDL_REQ_COUNT);

  sql_and_param->num_bind = 0;
  sql_and_param->check_affected_rows = true;
}

static void
make_query_decr_ddl_req_count (T_SQL_AND_PARAM * sql_and_param)
{
  sprintf (sql_and_param->sql, "UPDATE %s SET %s = %s - 1 ",
	   TABLE_SHARD_DB,
	   COL_SHARD_DB_DDL_REQ_COUNT, COL_SHARD_DB_DDL_REQ_COUNT);

  sql_and_param->num_bind = 0;
  sql_and_param->check_affected_rows = true;
}

static void
make_query_incr_gc_req_count (T_SQL_AND_PARAM * sql_and_param)
{
  sprintf (sql_and_param->sql, "UPDATE %s SET %s = %s + 1 ",
	   TABLE_SHARD_DB,
	   COL_SHARD_DB_GC_REQ_COUNT, COL_SHARD_DB_GC_REQ_COUNT);

  sql_and_param->num_bind = 0;
  sql_and_param->check_affected_rows = true;
}

static void
make_query_decr_gc_req_count (T_SQL_AND_PARAM * sql_and_param)
{
  sprintf (sql_and_param->sql, "UPDATE %s SET %s = %s - 1 ",
	   TABLE_SHARD_DB,
	   COL_SHARD_DB_GC_REQ_COUNT, COL_SHARD_DB_GC_REQ_COUNT);

  sql_and_param->num_bind = 0;
  sql_and_param->check_affected_rows = true;
}

static void
make_query_update_groupid (T_SQL_AND_PARAM * sql_and_param,
			   const int *groupid, const int *nodeid,
			   const int64_t * version)
{
  int num_bind = 0;

  sprintf (sql_and_param->sql, "UPDATE %s SET %s = ? , %s = ? WHERE %s = ?",
	   TABLE_SHARD_GROUPID, COL_SHARD_GROUPID_NODEID,
	   COL_SHARD_GROUPID_VERSION, COL_SHARD_GROUPID_GROUPID);

  SET_BIND_PARAM (num_bind++, sql_and_param->bind_param, CCI_TYPE_INT,
		  nodeid);
  SET_BIND_PARAM (num_bind++, sql_and_param->bind_param, CCI_TYPE_BIGINT,
		  version);
  SET_BIND_PARAM (num_bind++, sql_and_param->bind_param, CCI_TYPE_INT,
		  groupid);

  sql_and_param->num_bind = num_bind;
  sql_and_param->check_affected_rows = true;
}

static void
make_query_incr_last_groupid (T_SQL_AND_PARAM * sql_and_param,
			      const int64_t * next_gid_ver)
{
  int num_bind = 0;

  sprintf (sql_and_param->sql, "UPDATE %s SET %s = ? ",
	   TABLE_SHARD_DB, COL_SHARD_DB_GROUPID_LAST_VER);

  SET_BIND_PARAM (num_bind++, sql_and_param->bind_param, CCI_TYPE_BIGINT,
		  next_gid_ver);

  sql_and_param->num_bind = num_bind;
  sql_and_param->check_affected_rows = true;
}

static void
make_query_incr_node_ver (T_SQL_AND_PARAM * sql_and_param,
			  const int64_t * next_node_ver)
{
  int num_bind = 0;

  sprintf (sql_and_param->sql, "UPDATE %s SET %s = ?",
	   TABLE_SHARD_DB, COL_SHARD_DB_NODE_LAST_VER);

  SET_BIND_PARAM (num_bind++, sql_and_param->bind_param, CCI_TYPE_BIGINT,
		  next_node_ver);

  sql_and_param->num_bind = num_bind;
  sql_and_param->check_affected_rows = true;
}

static int
check_fetch_indicator (const int *ind, int count)
{
  int i;
  for (i = 0; i < count; i++)
    {
      if (ind[i] < 0)
	{
	  return -1;
	}
    }

  return 0;
}

static int
shd_mg_sync_local_mgmt_job_add ()
{
  T_SHARD_JOB_QUEUE *job_queue;
  T_SHARD_MGMT_JOB *job;

  job_queue = find_shard_job_queue (JOB_QUEUE_TYPE_SYNC_LOCAL_MGMT);
  assert (job_queue != NULL);

  job = RYE_MALLOC (sizeof (T_SHARD_MGMT_JOB));
  if (job == NULL)
    {
      return BR_ER_NO_MORE_MEMORY;
    }

  memset (job, 0, sizeof (T_SHARD_MGMT_JOB));

  shd_job_add_internal (job_queue, job, false);

  return 0;
}

/*
 * shd_mg_job_queue_add -
*/
static int
shd_mg_job_queue_add (int clt_sock_fd, in_addr_t clt_ip_addr,
		      const T_BROKER_REQUEST_MSG * req_msg)
{
  T_SHARD_JOB_QUEUE *job_queue;
  T_SHARD_MGMT_JOB *job;
  T_JOB_QUEUE_TYPE job_queue_type;

  if (req_msg->op_code == BRREQ_OP_CODE_GET_SHARD_INFO)
    {
      job_queue_type = JOB_QUEUE_TYPE_GET_INFO;
    }
  else if (req_msg->op_code == BRREQ_OP_CODE_MIGRATION_END ||
	   req_msg->op_code == BRREQ_OP_CODE_DDL_END)
    {
      /* migration_end, ddl_end request will be processed in
       * migration_start, ddl_start function */
      return BR_ER_INVALID_OPCODE;
    }
  else
    {
      job_queue_type = JOB_QUEUE_TYPE_ADMIN;
    }

  job_queue = find_shard_job_queue (job_queue_type);
  if (job_queue == NULL)
    {
      return BR_ER_INVALID_OPCODE;
    }

  job = make_new_mgmt_job (clt_sock_fd, clt_ip_addr, req_msg);
  if (job == NULL)
    {
      return BR_ER_NO_MORE_MEMORY;
    }

  shd_job_add_internal (job_queue, job, false);

  return 0;
}

static void
shd_job_add_internal (T_SHARD_JOB_QUEUE * job_queue,
		      T_SHARD_MGMT_JOB * job, bool is_retry)
{
  pthread_mutex_lock (&job_queue->lock);

  if (job != NULL)
    {
      job->next = NULL;
      job->is_retry = is_retry;
      if (is_retry == false)
	{
	  job->request_time = time (NULL);
	}

      if (job_queue->front == NULL)
	{
	  job_queue->front = job_queue->back = job;
	}
      else
	{
	  job_queue->front->next = job;
	  job_queue->front = job;
	}

      set_job_queue_job_count (job_queue, job_queue->num_job + 1);
    }

  pthread_cond_signal (&job_queue->cond);

  pthread_mutex_unlock (&job_queue->lock);
}

/*
 * shd_mg_job_queue_remove)all -
*/
static T_SHARD_MGMT_JOB *
shd_mg_job_queue_remove_all (T_SHARD_JOB_QUEUE * job_queue)
{
  T_SHARD_MGMT_JOB *job;

  job = job_queue->back;

  job_queue->back = NULL;
  job_queue->front = NULL;

  set_job_queue_job_count (job_queue, 0);

  return job;
}

static void
set_job_queue_job_count (T_SHARD_JOB_QUEUE * job_queue, int num_job)
{
  job_queue->num_job = num_job;
  if (job_queue->shm_queue_info)
    {
      job_queue->shm_queue_info->num_job = num_job;
    }
}

/*
 * free_mgmt_job -
*/
static void
free_mgmt_job (T_SHARD_MGMT_JOB * job)
{
  RYE_FREE_MEM (job->req_msg);
  if (job->compensation_job != NULL)
    {
      RYE_FREE_MEM (job->compensation_job->arg);
      RYE_FREE_MEM (job->compensation_job);
    }
  RYE_FREE_MEM (job);
}

/*
 * make_new_mgmt_job -
*/
static T_SHARD_MGMT_JOB *
make_new_mgmt_job (int clt_sock_fd, in_addr_t clt_ip_addr,
		   const T_BROKER_REQUEST_MSG * req_msg)
{
  T_SHARD_MGMT_JOB *job;

  job = RYE_MALLOC (sizeof (T_SHARD_MGMT_JOB));
  if (job != NULL)
    {
      memset (job, 0, sizeof (T_SHARD_MGMT_JOB));

      job->clt_sock_fd = clt_sock_fd;
      job->clt_ip_addr = clt_ip_addr;

      job->req_msg = brreq_msg_clone (req_msg);
      if (job->req_msg == NULL)
	{
	  RYE_FREE_MEM (job);
	  return NULL;
	}
      job->next = NULL;
    }

  return job;
}

/*
 * find_shard_job_queue -
 */
static T_SHARD_JOB_QUEUE *
find_shard_job_queue (T_JOB_QUEUE_TYPE job_queue_type)
{
  int i;

  for (i = 0; i < NUM_SHARD_JOB_QUEUE; i++)
    {
      if (shard_Job_queue[i].job_queue_type == job_queue_type)
	{
	  return (&shard_Job_queue[i]);
	}
    }

  return NULL;
}

/*
 * sync_node_info
*/
static int
sync_node_info (CCI_CONN * conn, T_SHARD_DB_REC * shard_db_rec,
		bool admin_mode)
{
  int64_t node_info_last_ver;
  T_DB_NODE_INFO *new_node_info = NULL;
  T_SHARD_DB_REC tmp_shard_db_rec;

  if (shard_db_rec == NULL)
    {
      shard_db_rec = &tmp_shard_db_rec;
    }

  get_info_last_ver (conn, shard_db_rec, admin_mode);

  node_info_last_ver = shard_db_rec->node_info_last_ver;

  if (node_info_last_ver > 0)
    {
      LOCK_DB_SHARD_INFO ();

      if (db_Shard_info.db_node_info == NULL ||
	  node_info_last_ver > db_Shard_info.db_node_info->node_info_ver)
	{
	  UNLOCK_DB_SHARD_INFO ();

	  new_node_info = select_all_node_info (conn);

	  LOCK_DB_SHARD_INFO ();

	  if (new_node_info != NULL)
	    {
	      db_node_info_free (db_Shard_info.db_node_info);

	      new_node_info->node_info_ver = node_info_last_ver;
	      db_Shard_info.db_node_info = new_node_info;
	    }
	}
      else
	{
	  new_node_info = db_Shard_info.db_node_info;
	}

      UNLOCK_DB_SHARD_INFO ();
    }

  return (new_node_info == NULL ? -1 : 0);
}

/*
 * sync_groupid_info
*/
static int
sync_groupid_info (CCI_CONN * conn, T_SHARD_DB_REC * shard_db_rec,
		   bool admin_mode)
{
  int64_t gid_info_last_ver = -1;
  T_SHARD_DB_REC tmp_shard_db_rec;
  T_DB_GROUPID_INFO *new_gid_info = NULL;

  if (shard_db_rec == NULL)
    {
      shard_db_rec = &tmp_shard_db_rec;
    }

  get_info_last_ver (conn, shard_db_rec, admin_mode);

  gid_info_last_ver = shard_db_rec->gid_info_last_ver;

  if (gid_info_last_ver > 0)
    {
      LOCK_DB_SHARD_INFO ();

      if (db_Shard_info.db_groupid_info == NULL ||
	  gid_info_last_ver > db_Shard_info.db_groupid_info->gid_info_ver)
	{
	  UNLOCK_DB_SHARD_INFO ();

	  new_gid_info = select_all_gid_info (conn);

	  LOCK_DB_SHARD_INFO ();

	  if (new_gid_info != NULL)
	    {
	      db_groupid_info_free (db_Shard_info.db_groupid_info);

	      new_gid_info->gid_info_ver = gid_info_last_ver;
	      db_Shard_info.db_groupid_info = new_gid_info;

	    }
	}
      else
	{
	  new_gid_info = db_Shard_info.db_groupid_info;
	}

      UNLOCK_DB_SHARD_INFO ();
    }

  return (new_gid_info == NULL ? -1 : 0);
}

/*
 * get_info_last_ver
*/
static void
get_info_last_ver (CCI_CONN * conn, T_SHARD_DB_REC * shard_db_rec,
		   bool admin_mode)
{
  int res = 0;

  if (admin_mode ||
      time (NULL) - db_Sync_info.last_check_time > SHARD_DB_CHECK_INTERVAL)
    {
      res = select_all_db_info (conn);
    }

  if (shard_db_rec)
    {
      if (admin_mode)
	{
	  if (res == 0)
	    {
	      *shard_db_rec = db_Sync_info.shard_db_rec;
	    }
	  else
	    {
	      memset (shard_db_rec, 0, sizeof (T_SHARD_DB_REC));
	    }
	}
      else
	{
	  *shard_db_rec = db_Sync_info.shard_db_rec;
	}
    }
}

/*
 * connect_metadb
*/
static int
connect_metadb (CCI_CONN * conn, int cci_autocommit_mode,
		const char *db_user, const char *db_passwd)
{
  static int print_cci_error = 1;

  if (cci_connect (conn, local_Mgmt_connect_url, db_user, db_passwd) < 0)
    {
      if (print_cci_error > 0 || strcmp (db_user, SHARD_MGMT_DB_USER) != 0)
	{
	  BR_LOG_CCI_ERROR ("connect", conn->err_buf.err_code,
			    conn->err_buf.err_msg, NULL);
	}

      if (print_cci_error == 1)
	{
	  /* prevent too many connection error logs */
	  print_cci_error = 0;
	}
      return -1;
    }

  print_cci_error = 2;

  if (cci_set_autocommit (conn, cci_autocommit_mode) < 0)
    {
      BR_LOG_CCI_ERROR ("set_autocommit", conn->err_buf.err_code,
			conn->err_buf.err_msg, NULL);
      cci_disconnect (conn);
      return -1;
    }

  return 0;
}

/*
 * select_all_db_info
*/
static int
select_all_db_info (CCI_CONN * conn_arg)
{
  CCI_CONN tmp_conn;
  CCI_CONN *conn;
  CCI_STMT stmt;
  int err = -1;
  int res;
  char *tmp_p;
  T_SHARD_DB_REC shard_db_rec;
  char query[SQL_BUF_SIZE];
  int ind[COL_COUNT_SHARD_DB];

  if (conn_arg == NULL)
    {
      conn = &tmp_conn;
      if (connect_metadb (conn, CCI_AUTOCOMMIT_TRUE,
			  shard_Mgmt_db_user, shard_Mgmt_db_passwd) < 0)
	{
	  return -1;
	}
    }
  else
    {
      conn = conn_arg;
    }

  sprintf (query,
	   " SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s "
	   " FROM %s "
	   " WHERE %s = %d FOR UPDATE",
	   COL_SHARD_DB_DB_NAME, COL_SHARD_DB_GROUPID_COUNT,
	   COL_SHARD_DB_GROUPID_LAST_VER, COL_SHARD_DB_NODE_LAST_VER,
	   COL_SHARD_DB_MIG_REQ_COUNT, COL_SHARD_DB_DDL_REQ_COUNT,
	   COL_SHARD_DB_GC_REQ_COUNT, COL_SHARD_DB_NODE_STATUS,
	   COL_SHARD_DB_CREATED_AT,
	   TABLE_SHARD_DB, COL_SHARD_DB_ID, SHARD_DB_ID_VALUE);

  if (cci_prepare (conn, &stmt, query, 0) < 0)
    {
      BR_LOG_CCI_ERROR ("prepare", conn->err_buf.err_code,
			conn->err_buf.err_msg, query);
      goto select_all_db_info_end;
    }

  res = cci_execute (&stmt, 0, 0);
  if (res != 1)
    {
      BR_LOG_CCI_ERROR ("execute", res, stmt.err_buf.err_msg, query);
      goto select_all_db_info_end;
    }

  if (cci_fetch_next (&stmt) < 0)
    {
      BR_LOG_CCI_ERROR ("fetch_next", stmt.err_buf.err_code,
			stmt.err_buf.err_msg, query);
      goto select_all_db_info_end;
    }

  memset (ind, 0, sizeof (ind));

  tmp_p = cci_get_string (&stmt, 1, &ind[0]);
  shard_db_rec.groupid_count = cci_get_int (&stmt, 2, &ind[1]);
  shard_db_rec.gid_info_last_ver = cci_get_bigint (&stmt, 3, &ind[2]);
  shard_db_rec.node_info_last_ver = cci_get_bigint (&stmt, 4, &ind[3]);
  shard_db_rec.mig_req_count = cci_get_int (&stmt, 5, &ind[4]);
  shard_db_rec.ddl_req_count = cci_get_int (&stmt, 6, &ind[5]);
  shard_db_rec.gc_req_count = cci_get_int (&stmt, 7, &ind[6]);
  shard_db_rec.node_status = cci_get_int (&stmt, 8, &ind[7]);
  shard_db_rec.created_at = cci_get_bigint (&stmt, 9, &ind[8]);

  if (check_fetch_indicator (ind, DIM (ind)) < 0)
    {
      br_log_write (BROKER_LOG_ERROR, INADDR_NONE, "fetch error [%s]", query);
      goto select_all_db_info_end;
    }

  if (db_Sync_info.shard_db_rec.global_dbname != NULL &&
      strcmp (tmp_p, db_Sync_info.shard_db_rec.global_dbname) == 0)
    {
      shard_db_rec.global_dbname = db_Sync_info.shard_db_rec.global_dbname;;
      shard_db_rec.buf = db_Sync_info.shard_db_rec.buf;
    }
  else
    {
      RYE_ALLOC_COPY_STR (shard_db_rec.buf, tmp_p);
      shard_db_rec.global_dbname = shard_db_rec.buf;
      if (shard_db_rec.global_dbname == NULL)
	{
	  br_log_write (BROKER_LOG_ERROR, INADDR_NONE, "malloc fail [%s]",
			query);
	  goto select_all_db_info_end;
	}

      if (strcmp (shm_Br_info->shard_global_dbname,
		  shard_db_rec.global_dbname) != 0)
	{
	  assert (0);
	}
    }

  if (shard_db_rec.buf != db_Sync_info.shard_db_rec.buf)
    {
      RYE_FREE_MEM (db_Sync_info.shard_db_rec.buf);
    }

  LOCK_DB_SHARD_INFO ();
  db_Sync_info.shard_db_rec = shard_db_rec;
  UNLOCK_DB_SHARD_INFO ();

  db_Sync_info.last_check_time = time (NULL);

  err = 0;

  br_log_write (BROKER_LOG_NOTICE, INADDR_NONE,
		"shard_db info: dbname=%s, groupid_count=%d, "
		"gid_info_last_ver=%d, node_info_last_ver=%d, "
		"mig_req_count=%d, ddl_req_count=%d, gc_req_count=%d, "
		"created_at=%ld\n",
		shard_db_rec.global_dbname,
		shard_db_rec.groupid_count,
		shard_db_rec.gid_info_last_ver,
		shard_db_rec.node_info_last_ver,
		shard_db_rec.mig_req_count, shard_db_rec.ddl_req_count,
		shard_db_rec.gc_req_count, shard_db_rec.created_at);

select_all_db_info_end:

  if (conn_arg == NULL)
    {
      cci_disconnect (conn);
    }

  return err;
}

/*
 * select_all_gid_info
*/
static T_DB_GROUPID_INFO *
select_all_gid_info (CCI_CONN * conn_arg)
{
  CCI_CONN tmp_conn;
  CCI_CONN *conn;
  CCI_STMT stmt;
  int res;
  T_DB_GROUPID_INFO *db_groupid_info = NULL;
  int err;
  int row;
  char query[SQL_BUF_SIZE];

  if (conn_arg == NULL)
    {
      conn = &tmp_conn;
      if (connect_metadb (conn, CCI_AUTOCOMMIT_TRUE,
			  shard_Mgmt_db_user, shard_Mgmt_db_passwd) < 0)
	{
	  return NULL;
	}
    }
  else
    {
      conn = conn_arg;
    }

  sprintf (query, "SELECT %s, %s, %s FROM %s",
	   COL_SHARD_GROUPID_GROUPID, COL_SHARD_GROUPID_NODEID,
	   COL_SHARD_GROUPID_VERSION, TABLE_SHARD_GROUPID);

  if (cci_prepare (conn, &stmt, query, 0) < 0)
    {
      BR_LOG_CCI_ERROR ("prepare", conn->err_buf.err_code,
			conn->err_buf.err_msg, query);
      goto select_all_gid_info_end;
    }

  res = cci_execute (&stmt, 0, 0);
  if (res <= 0)
    {
      BR_LOG_CCI_ERROR ("execute", res, stmt.err_buf.err_msg, query);
      goto select_all_gid_info_end;
    }

  db_groupid_info = db_groupid_info_alloc (res);
  if (db_groupid_info == NULL)
    {
      br_log_write (BROKER_LOG_ERROR, INADDR_NONE, "malloc fail: [%s]",
		    query);
      goto select_all_gid_info_end;
    }

  row = 0;
  while (row < res)
    {
      int ind[COL_COUNT_SHARD_GROUPID];

      err = cci_fetch_next (&stmt);
      if (err == CCI_ER_NO_MORE_DATA)
	{
	  break;
	}
      if (err < 0)
	{
	  BR_LOG_CCI_ERROR ("fetch_next", stmt.err_buf.err_code,
			    stmt.err_buf.err_msg, query);
	  db_groupid_info_free (db_groupid_info);
	  db_groupid_info = NULL;
	  goto select_all_gid_info_end;
	}

      memset (ind, 0, sizeof (ind));

      db_groupid_info->gid_info[row].gid = cci_get_int (&stmt, 1, &ind[0]);
      db_groupid_info->gid_info[row].nodeid = cci_get_int (&stmt, 2, &ind[1]);
      db_groupid_info->gid_info[row].ver = cci_get_bigint (&stmt, 3, &ind[2]);

      if (check_fetch_indicator (ind, DIM (ind)) < 0)
	{
	  db_groupid_info_free (db_groupid_info);
	  db_groupid_info = NULL;
	  goto select_all_gid_info_end;
	}

      row++;
    }

  cci_close_req_handle (&stmt);

select_all_gid_info_end:

  if (conn_arg == NULL)
    {
      cci_disconnect (conn);
    }

  return db_groupid_info;
}

/*
 * select_all_node_info
*/
static T_DB_NODE_INFO *
select_all_node_info (CCI_CONN * conn_arg)
{
  CCI_CONN tmp_conn;
  CCI_CONN *conn;
  CCI_STMT stmt;
  int res;
  T_DB_NODE_INFO *node_info = NULL;
  int err;
  int row;
  char query[SQL_BUF_SIZE];

  if (conn_arg == NULL)
    {
      conn = &tmp_conn;
      if (connect_metadb (conn, CCI_AUTOCOMMIT_TRUE,
			  shard_Mgmt_db_user, shard_Mgmt_db_passwd) < 0)
	{
	  return NULL;
	}
    }
  else
    {
      conn = conn_arg;
    }

  sprintf (query,
	   "SELECT %s, %s, %s, %s FROM %s WHERE %s <> %d ORDER BY %s, %s",
	   COL_SHARD_NODE_NODEID, COL_SHARD_NODE_DBNAME,
	   COL_SHARD_NODE_HOST, COL_SHARD_NODE_PORT,
	   TABLE_SHARD_NODE,
	   COL_SHARD_NODE_STATUS, ADD_NODE_STATUS_REQUESTED,
	   COL_SHARD_NODE_NODEID, COL_SHARD_NODE_VERSION);

  if (cci_prepare (conn, &stmt, query, 0) < 0)
    {
      BR_LOG_CCI_ERROR ("prepare", conn->err_buf.err_code,
			conn->err_buf.err_msg, query);
      goto select_all_node_info_end;
    }

  res = cci_execute (&stmt, 0, 0);
  if (res <= 0)
    {
      BR_LOG_CCI_ERROR ("execute", res, stmt.err_buf.err_msg, query);
      goto select_all_node_info_end;
    }

  node_info = db_node_info_alloc (res);
  if (node_info == NULL)
    {
      br_log_write (BROKER_LOG_ERROR, INADDR_NONE, "malloc fail: [%s]",
		    query);
      goto select_all_node_info_end;
    }

  row = 0;
  while (row < res)
    {
      char *tmp_dbname;
      char *tmp_host;
      int tmp_nodeid;
      int tmp_port;
      int ind[COL_COUNT_SHARD_NODE];
      in_addr_t tmp_host_addr = INADDR_NONE;
      PRM_NODE_INFO tmp_host_info;

      err = cci_fetch_next (&stmt);
      if (err == CCI_ER_NO_MORE_DATA)
	{
	  break;
	}
      if (err < 0)
	{
	  BR_LOG_CCI_ERROR ("fetch_next", err, stmt.err_buf.err_msg, query);
	  db_node_info_free (node_info);
	  node_info = NULL;
	  goto select_all_node_info_end;
	}

      memset (ind, 0, sizeof (ind));

      tmp_nodeid = cci_get_int (&stmt, 1, &ind[0]);
      tmp_dbname = cci_get_string (&stmt, 2, &ind[1]);
      tmp_host = cci_get_string (&stmt, 3, &ind[2]);
      tmp_port = cci_get_int (&stmt, 4, &ind[3]);

      if (check_fetch_indicator (ind, DIM (ind)) < 0 ||
	  ((tmp_host_addr = inet_addr (tmp_host)) == INADDR_NONE))
	{
	  db_node_info_free (node_info);
	  node_info = NULL;
	  goto select_all_node_info_end;
	}

      if (tmp_port <= 0)
	{
	  assert (0);
	  tmp_port = shard_Mgmt_server_info.local_mgmt_port;
	}

      PRM_NODE_INFO_SET (&tmp_host_info, tmp_host_addr, tmp_port);

      br_copy_shard_node_info (&node_info->node_info[row], tmp_nodeid,
			       tmp_dbname, tmp_host, &tmp_host_info,
			       HA_STATE_FOR_DRIVER_UNKNOWN, NULL);
      row++;
    }

  cci_close_req_handle (&stmt);

select_all_node_info_end:

  if (conn_arg == NULL)
    {
      cci_disconnect (conn);
    }

  return node_info;
}

static int
find_min_node_id (T_DB_NODE_INFO * db_node_info)
{
  int i;
  int min_nodeid = 0;

  for (i = 0; i < db_node_info->node_info_count; i++)
    {
      if (min_nodeid == 0 || db_node_info->node_info[i].node_id < min_nodeid)
	{
	  min_nodeid = db_node_info->node_info[i].node_id;
	}
    }

  return min_nodeid;
}

static T_SHARD_NODE_INFO *
find_node_info (T_DB_NODE_INFO * db_node_info,
		const T_SHARD_NODE_INFO * find_node, bool * exist_same_node)
{
  int i;
  T_SHARD_NODE_INFO *node_found = NULL;

  *exist_same_node = false;

  if (db_node_info == NULL)
    {
      return NULL;
    }

  for (i = 0; i < db_node_info->node_info_count; i++)
    {
      if (find_node->node_id == db_node_info->node_info[i].node_id)
	{
	  if (strcmp (find_node->local_dbname,
		      db_node_info->node_info[i].local_dbname) == 0 &&
	      prm_is_same_node (&db_node_info->node_info[i].host_info,
				&find_node->host_info) == true)
	    {
	      node_found = &db_node_info->node_info[i];
	    }
	  else
	    {
	      *exist_same_node = true;
	    }
	}
    }

  return node_found;
}

static const T_GROUPID_INFO *
find_groupid_info (const T_DB_GROUPID_INFO * db_groupid_info, int groupid)
{
  if (db_groupid_info != NULL &&
      db_groupid_info->group_id_count >= groupid &&
      db_groupid_info->gid_info[groupid - 1].gid == groupid)
    {
      return (&db_groupid_info->gid_info[groupid - 1]);
    }
  else
    {
      return NULL;
    }
}

static int
check_nodeid_in_use (int nodeid, const T_DB_GROUPID_INFO * db_groupid_info)
{
  int i;

  if (db_groupid_info == NULL)
    {
      return BR_ER_SHARD_INFO_NOT_AVAILABLE;
    }

  for (i = 0; i < db_groupid_info->group_id_count; i++)
    {
      if (db_groupid_info->gid_info[i].nodeid == nodeid)
	{
	  return BR_ER_NODE_IN_USE;
	}
    }

  return 0;
}

/*
 * db_node_info_alloc
*/
static T_DB_NODE_INFO *
db_node_info_alloc (int node_info_count)
{
  T_DB_NODE_INFO *node_info;
  int alloc_size;

  alloc_size = sizeof (T_DB_NODE_INFO) +
    sizeof (T_SHARD_NODE_INFO) * node_info_count;

  node_info = RYE_MALLOC (alloc_size);
  if (node_info == NULL)
    {
      return NULL;
    }

  memset (node_info, 0, alloc_size);
  node_info->node_info_count = node_info_count;

  return node_info;
}

/*
 * db_node_info_free
*/
static void
db_node_info_free (T_DB_NODE_INFO * node_info)
{
  if (node_info == NULL)
    {
      return;
    }

  RYE_FREE_MEM (node_info->net_stream_node_info.buf);
  RYE_FREE_MEM (node_info->net_stream_node_state.buf);
  RYE_FREE_MEM (node_info);
}

static int
clone_db_node_info (T_DB_NODE_INFO ** ret_db_node_info,
		    const T_DB_NODE_INFO * src_node_info)
{
  T_DB_NODE_INFO *db_node_info = NULL;
  int i;

  if (src_node_info == NULL)
    {
      return BR_ER_SHARD_INFO_NOT_AVAILABLE;
    }

  db_node_info = db_node_info_alloc (src_node_info->node_info_count);
  if (db_node_info == NULL)
    {
      return BR_ER_NO_MORE_MEMORY;
    }

  for (i = 0; i < src_node_info->node_info_count; i++)
    {
      br_copy_shard_node_info (&db_node_info->node_info[i],
			       src_node_info->node_info[i].node_id,
			       src_node_info->node_info[i].local_dbname,
			       src_node_info->node_info[i].host_ip_str,
			       &src_node_info->node_info[i].host_info,
			       src_node_info->node_info[i].ha_state,
			       src_node_info->node_info[i].host_name);
    }

  *ret_db_node_info = db_node_info;
  return 0;
}

/*
 * db_groupid_info_alloc
*/
static T_DB_GROUPID_INFO *
db_groupid_info_alloc (int gid_info_count)
{
  T_DB_GROUPID_INFO *db_groupid_info;
  int alloc_size;

  alloc_size = (sizeof (T_DB_GROUPID_INFO) +
		sizeof (T_GROUPID_INFO) * gid_info_count);

  db_groupid_info = RYE_MALLOC (alloc_size);
  if (db_groupid_info == NULL)
    {
      return NULL;
    }

  memset (db_groupid_info, 0, alloc_size);
  db_groupid_info->group_id_count = gid_info_count;

  return db_groupid_info;
}

/*
 * db_groupid_info_free
*/
static void
db_groupid_info_free (T_DB_GROUPID_INFO * db_groupid_info)
{
  if (db_groupid_info == NULL)
    {
      return;
    }

  RYE_FREE_MEM (db_groupid_info->net_stream_all_info.buf);
  RYE_FREE_MEM (db_groupid_info->net_stream_partial_info.buf);
  RYE_FREE_MEM (db_groupid_info);
}

/*
 * send_info_msg_to_client -
*/
static void
send_info_msg_to_client (int sock_fd, int err_code,
			 const char *shard_info_hdr, int shard_info_hdr_size,
			 const char *node_info, int node_info_size,
			 const char *groupid_info, int groupid_info_size,
			 const char *node_state, int node_state_size)
{
  T_BROKER_RESPONSE_NET_MSG res_msg;
  int info_size[4];

  info_size[0] = shard_info_hdr_size;
  info_size[1] = node_info_size;
  info_size[2] = groupid_info_size;
  info_size[3] = node_state_size;

  brres_msg_pack (&res_msg, err_code, 4, info_size);

  br_write_nbytes_to_client (sock_fd, res_msg.msg_buffer,
			     res_msg.msg_buffer_size,
			     BR_DEFAULT_WRITE_TIMEOUT);

  if (shard_info_hdr_size > 0)
    {
      br_write_nbytes_to_client (sock_fd, shard_info_hdr, shard_info_hdr_size,
				 BR_DEFAULT_WRITE_TIMEOUT);
    }
  if (node_info_size > 0)
    {
      br_write_nbytes_to_client (sock_fd, node_info, node_info_size,
				 BR_DEFAULT_WRITE_TIMEOUT);
    }
  if (groupid_info_size > 0)
    {
      br_write_nbytes_to_client (sock_fd, groupid_info, groupid_info_size,
				 BR_DEFAULT_WRITE_TIMEOUT);
    }
  if (node_state_size > 0)
    {
      br_write_nbytes_to_client (sock_fd, node_state, node_state_size,
				 BR_DEFAULT_WRITE_TIMEOUT);
    }
}

/*
 * make_node_info_net_stream
*/
static T_INFO_NET_STREAM *
make_node_info_net_stream (T_DB_NODE_INFO * db_node_info)
{
  T_INFO_NET_STREAM *net_stream;

  if (db_node_info == NULL)
    {
      return NULL;
    }

  net_stream = &db_node_info->net_stream_node_info;
  if (net_stream->buf == NULL)
    {
      int i, size;
      char *ptr;

      size = (sizeof (int) +	/* node info count */
	      sizeof (int64_t));	/* node info version */
      for (i = 0; i < db_node_info->node_info_count; i++)
	{
	  size += (sizeof (short) + sizeof (int) * 3 +	/* node id, port, length * 2 */
		   strlen (db_node_info->node_info[i].local_dbname) + 1 +
		   strlen (db_node_info->node_info[i].host_ip_str) + 1);
	}

      net_stream->size = size;
      net_stream->buf = RYE_MALLOC (size);

      if (net_stream->buf == NULL)
	{
	  net_stream->size = 0;
	  return NULL;
	}

      ptr = net_stream->buf;

      ptr = br_mgmt_net_add_int64 (ptr, db_node_info->node_info_ver);
      ptr = br_mgmt_net_add_int (ptr, db_node_info->node_info_count);

      for (i = 0; i < db_node_info->node_info_count; i++)
	{
	  T_SHARD_NODE_INFO *shard_node_info = &db_node_info->node_info[i];

	  ptr = br_mgmt_net_add_short (ptr, shard_node_info->node_id);
	  ptr = br_mgmt_net_add_string (ptr, shard_node_info->local_dbname);
	  ptr = br_mgmt_net_add_string (ptr, shard_node_info->host_ip_str);
	  ptr =
	    br_mgmt_net_add_int (ptr,
				 PRM_NODE_INFO_GET_PORT (&shard_node_info->
							 host_info));
	}
    }

  return net_stream;
}

static T_INFO_NET_STREAM *
make_node_state_net_stream (T_DB_NODE_INFO * db_node_info)
{
  T_INFO_NET_STREAM *net_stream;

  if (db_node_info == NULL)
    {
      return NULL;
    }

  net_stream = &db_node_info->net_stream_node_state;
  if (net_stream->buf == NULL)
    {
      int size;
      int i;
      char *ptr;

      size = (sizeof (int));	/* count */
      for (i = 0; i < db_node_info->node_info_count; i++)
	{
	  size += (1 + sizeof (int) +	/* state, host len */
		   strlen (db_node_info->node_info[i].host_ip_str) + 1);
	}

      net_stream->size = size;
      net_stream->buf = RYE_MALLOC (size);

      if (net_stream->buf == NULL)
	{
	  net_stream->size = 0;
	  return NULL;
	}

      ptr = net_stream->buf;

      ptr = br_mgmt_net_add_int (ptr, db_node_info->node_info_count);

      for (i = 0; i < db_node_info->node_info_count; i++)
	{
	  T_SHARD_NODE_INFO *shard_node_info = &db_node_info->node_info[i];

	  ptr = br_mgmt_net_add_string (ptr, shard_node_info->host_ip_str);

	  *ptr = shard_node_info->ha_state;
	  ptr++;
	}
    }

  return net_stream;
}

/*
 * make_groupid_info_net_stream
*/
static T_INFO_NET_STREAM *
make_groupid_info_net_stream (T_DB_GROUPID_INFO * db_groupid_info,
			      int64_t clt_groupid_ver)
{
  T_INFO_NET_STREAM *net_stream;

  if (db_groupid_info == NULL)
    {
      return NULL;
    }

  if (clt_groupid_ver <= 0)
    {
      net_stream = &db_groupid_info->net_stream_all_info;
      if (net_stream->buf == NULL)
	{
	  int i, size;
	  char *ptr;

	  size = sizeof (int64_t) + sizeof (int) + 1;	/* last version, num gid */
	  size += (sizeof (short) * db_groupid_info->group_id_count);

	  net_stream->size = size;
	  net_stream->buf = RYE_MALLOC (size);

	  if (net_stream->buf == NULL)
	    {
	      net_stream->size = 0;
	      return NULL;
	    }

	  ptr = net_stream->buf;

	  ptr = br_mgmt_net_add_int64 (ptr, db_groupid_info->gid_info_ver);
	  ptr = br_mgmt_net_add_int (ptr, db_groupid_info->group_id_count);

	  *ptr = BR_RES_SHARD_INFO_ALL;
	  ptr++;

	  for (i = 0; i < db_groupid_info->group_id_count; i++)
	    {
	      ptr =
		br_mgmt_net_add_short (ptr,
				       db_groupid_info->gid_info[i].nodeid);
	    }
	}
    }
  else
    {
      net_stream = &db_groupid_info->net_stream_partial_info;
      if (db_groupid_info->net_stream_partial_info_clt_ver != clt_groupid_ver)
	{
	  char *net_buf;
	  char *ptr;
	  char *ptr_changed_count;
	  int i, alloc_size;
	  int changed_count;

	  alloc_size = sizeof (int64_t) + sizeof (int) + 1 + sizeof (int);
	  alloc_size += ((sizeof (short) + sizeof (int)) *
			 db_groupid_info->group_id_count);

	  net_buf = RYE_MALLOC (alloc_size);
	  if (net_buf == NULL)
	    {
	      return NULL;
	    }

	  ptr = net_buf;

	  ptr = br_mgmt_net_add_int64 (ptr, db_groupid_info->gid_info_ver);
	  ptr = br_mgmt_net_add_int (ptr, db_groupid_info->group_id_count);

	  *ptr = BR_RES_SHARD_INFO_CHANGED_ONLY;
	  ptr++;

	  ptr_changed_count = ptr;
	  ptr += sizeof (int);
	  changed_count = 0;

	  for (i = 0; i < db_groupid_info->group_id_count; i++)
	    {
	      if (db_groupid_info->gid_info[i].ver > clt_groupid_ver)
		{
		  ptr =
		    br_mgmt_net_add_int (ptr,
					 db_groupid_info->gid_info[i].gid);
		  ptr =
		    br_mgmt_net_add_short (ptr,
					   db_groupid_info->gid_info[i].
					   nodeid);
		  changed_count++;
		}
	    }

	  br_mgmt_net_add_int (ptr_changed_count, changed_count);

	  RYE_FREE_MEM (db_groupid_info->net_stream_partial_info.buf);

	  db_groupid_info->net_stream_partial_info.buf = net_buf;
	  db_groupid_info->net_stream_partial_info.size =
	    (int) (ptr - net_buf);
	  db_groupid_info->net_stream_partial_info_clt_ver = clt_groupid_ver;
	}
    }

  return net_stream;
}

static int
admin_query_execute_array (CCI_CONN * conn, int num_sql,
			   const T_SQL_AND_PARAM * sql_and_param)
{
  int i;

  for (i = 0; i < num_sql; i++)
    {
      int res;

      assert (MAX_BIND_VALUES >= sql_and_param[i].num_bind);

      res = admin_query_execute (conn, true, NULL, sql_and_param[i].sql,
				 sql_and_param[i].num_bind,
				 sql_and_param[i].bind_param);
      if (res < 0)
	{
	  return res;
	}
      else if (sql_and_param[i].check_affected_rows && res == 0)
	{
	  br_log_write (BROKER_LOG_ERROR, INADDR_NONE,
			"%s: affected row = 0", sql_and_param[i].sql);
	  return BR_ER_METADB;
	}
    }

  return 0;
}

static int
admin_query_execute (CCI_CONN * conn, bool do_prepare,
		     CCI_STMT * stmt_arg, const char *sql,
		     int num_bind, const T_BIND_PARAM * bind_param)
{
  CCI_STMT stmt_local;
  CCI_STMT *stmt;
  int res;
  int i;

  if (stmt_arg == NULL)
    {
      assert (do_prepare);
      stmt = &stmt_local;
      do_prepare = true;
    }
  else
    {
      stmt = stmt_arg;
    }

  if (do_prepare)
    {
      if (cci_prepare (conn, stmt, sql, 0) < 0)
	{
	  BR_LOG_CCI_ERROR ("prepare", conn->err_buf.err_code,
			    conn->err_buf.err_msg, sql);
	  return -1;
	}
    }

  for (i = 0; i < num_bind; i++)
    {
      if (cci_bind_param (stmt, bind_param[i].index, bind_param[i].a_type,
			  bind_param[i].value, bind_param[i].flag) < 0)
	{
	  BR_LOG_CCI_ERROR ("bind", stmt->err_buf.err_code,
			    stmt->err_buf.err_msg, sql);
	  return -1;
	}
    }

  res = cci_execute (stmt, 0, 0);

  if (res < 0)
    {
      BR_LOG_CCI_ERROR ("execute", res, stmt->err_buf.err_msg, sql);
    }

  if (stmt_arg == NULL)
    {
      cci_close_req_handle (stmt);
    }

  return res;
}

static const char *
admin_command_str (const T_SHD_MG_COMPENSATION_JOB * compensation_job)
{
  int i;
  int opcode;

  if (compensation_job == NULL)
    {
      return "UNKNOWN_JOB";
    }

  opcode = compensation_job->next_req_opcode;

  for (i = 0; shd_Mg_admin_func_table[i].func != NULL; i++)
    {
      if (shd_Mg_admin_func_table[i].opcode == opcode)
	{
	  return shd_Mg_admin_func_table[i].log_msg;
	}
    }

  return "UNKNOWN_OPCODE";
}

static int
shd_mg_propagate_shard_mgmt_port (T_LOCAL_MGMT_SYNC_INFO * sync_info)
{
  int i;
  int err;
  T_DB_NODE_INFO *db_node_info = sync_info->db_node_info;
  char server_name[128];

  for (i = 0; i < db_node_info->node_info_count; i++)
    {
      db_node_info->node_info[i].host_name[0] = '\0';
    }

  for (i = 0; i < db_node_info->node_info_count; i++)
    {
      T_SHARD_NODE_INFO *shard_node_info = &db_node_info->node_info[i];
      int ha_state;

      err = cci_mgmt_sync_shard_mgmt_info (shard_node_info->host_ip_str,
					   PRM_NODE_INFO_GET_PORT
					   (&shard_node_info->host_info),
					   shard_node_info->local_dbname,
					   sync_info->global_dbname,
					   shard_node_info->node_id,
					   shard_Mgmt_server_info.port,
					   db_node_info->node_info_ver,
					   sync_info->groupid_version,
					   server_name, sizeof (server_name),
					   &ha_state,
					   LOCAL_MGMT_REQ_TIMEOUT_MSEC);
      if (err < 0)
	{
	  ;			/* TODO - avoid compile error */
	}

      shard_node_info->ha_state = ha_state;

      STRNCPY (shard_node_info->host_name, server_name,
	       sizeof (shard_node_info->host_name));
    }

  LOCK_DB_SHARD_INFO ();

  RYE_FREE_MEM (db_Shard_info.db_node_info->net_stream_node_state.buf);
  db_Shard_info.db_node_info->net_stream_node_state.size = 0;

  for (i = 0; i < db_Shard_info.db_node_info->node_info_count; i++)
    {
      db_Shard_info.db_node_info->node_info[i].ha_state =
	HA_STATE_FOR_DRIVER_UNKNOWN;
    }

  for (i = 0; i < db_node_info->node_info_count; i++)
    {
      T_SHARD_NODE_INFO *cp_dest_node;
      bool exist_same_node = false;

      cp_dest_node = find_node_info (db_Shard_info.db_node_info,
				     &db_node_info->node_info[i],
				     &exist_same_node);
      if (cp_dest_node != NULL)
	{
	  cp_dest_node->ha_state = db_node_info->node_info[i].ha_state;
	  STRNCPY (cp_dest_node->host_name,
		   db_node_info->node_info[i].host_name,
		   sizeof (cp_dest_node->host_name));
	}
    }
  UNLOCK_DB_SHARD_INFO ();

  sync_info->last_sync_time = time (NULL);

  return 0;
}

/*
 * shd_mg_func_sync_local_mgmt -
*/
static void
shd_mg_func_sync_local_mgmt (T_LOCAL_MGMT_SYNC_INFO * sync_info, bool force)
{
  sync_node_info (NULL, NULL, force);

  LOCK_DB_SHARD_INFO ();

  if (db_Sync_info.shard_db_rec.global_dbname == NULL)
    {
      RYE_FREE_MEM (sync_info->global_dbname);
    }
  else
    {
      if (sync_info->global_dbname == NULL ||
	  strcmp (sync_info->global_dbname,
		  db_Sync_info.shard_db_rec.global_dbname) != 0)
	{
	  RYE_FREE_MEM (sync_info->global_dbname);
	  RYE_ALLOC_COPY_STR (sync_info->global_dbname,
			      db_Sync_info.shard_db_rec.global_dbname);
	}
    }

  if (sync_info->global_dbname == NULL)
    {
      db_node_info_free (sync_info->db_node_info);
      memset (sync_info, 0, sizeof (T_LOCAL_MGMT_SYNC_INFO));
    }
  else
    {
      if (db_Shard_info.db_node_info != NULL)
	{
	  if (sync_info->db_node_info == NULL ||
	      sync_info->db_node_info->node_info_ver !=
	      db_Shard_info.db_node_info->node_info_ver)
	    {
	      db_node_info_free (sync_info->db_node_info);

	      if (clone_db_node_info (&sync_info->db_node_info,
				      db_Shard_info.db_node_info) < 0)
		{
		  sync_info->db_node_info = NULL;
		}
	    }
	}

      sync_info->groupid_version =
	db_Sync_info.shard_db_rec.gid_info_last_ver;
    }

  UNLOCK_DB_SHARD_INFO ();

  if (sync_info->db_node_info != NULL &&
      (force || time (NULL) - sync_info->last_sync_time > 30))
    {
      shd_mg_propagate_shard_mgmt_port (sync_info);
    }
}

static bool
is_rebalance_node (int target_nodeid, int node_count, const int *rbl_node)
{
  int i;

  if (node_count == 0)
    {
      return true;
    }

  for (i = 0; i < node_count; i++)
    {
      if (target_nodeid > 0 && target_nodeid == rbl_node[i])
	{
	  return true;
	}
    }

  return false;
}

static int
calc_rebalance_count (int node_count, T_REBALANCE_AMOUNT * rbl_amount,
		      bool empty_node)
{
  float sum;
  int migration_count;
  int i;
  int rbl_src_node_count;
  int rbl_dest_node_count;
  int mig_dest_count;
  int mig_src_remain;

calc_start:
  sum = 0;
  rbl_src_node_count = 0;
  rbl_dest_node_count = 0;

  for (i = 0; i < node_count; i++)
    {
      if (rbl_amount[i].nodeid > 0)
	{
	  sum += rbl_amount[i].count;

	  if (rbl_amount[i].type == RBL_NODE_TYPE_SOURCE)
	    {
	      rbl_src_node_count++;
	    }
	  else if (rbl_amount[i].type == RBL_NODE_TYPE_DEST)
	    {
	      rbl_dest_node_count++;
	    }
	}
    }

  if (rbl_src_node_count == 0 || rbl_dest_node_count == 0)
    {
      return 0;
    }

  migration_count = 0;

  if (empty_node)
    {
      for (i = 0; i < node_count; i++)
	{
	  if (rbl_amount[i].nodeid > 0 &&
	      rbl_amount[i].type == RBL_NODE_TYPE_SOURCE)
	    {
	      rbl_amount[i].mig_count = rbl_amount[i].count;
	      migration_count += rbl_amount[i].count;
	    }
	}
    }
  else
    {
      int avg;

      avg = sum / (rbl_src_node_count + rbl_dest_node_count) + 0.5;

      for (i = 0; i < node_count; i++)
	{
	  int avail_count;

	  if (rbl_amount[i].nodeid > 0 &&
	      rbl_amount[i].type == RBL_NODE_TYPE_SOURCE)
	    {
	      avail_count = rbl_amount[i].count - avg;
	      if (avail_count > 0)
		{
		  rbl_amount[i].mig_count = avail_count;
		  migration_count += avail_count;
		}
	      else
		{
		  rbl_amount[i].nodeid = 0;
		  rbl_amount[i].type = RBL_NODE_TYPE_UNKNOWN;
		  goto calc_start;
		}
	    }
	}
    }

  mig_dest_count = migration_count / rbl_dest_node_count;
  mig_src_remain = migration_count;

  for (i = 0; i < node_count; i++)
    {
      if (rbl_amount[i].nodeid > 0 &&
	  rbl_amount[i].type == RBL_NODE_TYPE_DEST)
	{
	  rbl_amount[i].mig_count = mig_dest_count;
	  mig_src_remain -= mig_dest_count;
	}
    }

  while (mig_src_remain > 0)
    {
      for (i = 0; i < node_count && mig_src_remain > 0; i++)
	{
	  if (rbl_amount[i].nodeid > 0 &&
	      rbl_amount[i].type == RBL_NODE_TYPE_DEST)
	    {
	      rbl_amount[i].mig_count += 1;
	      mig_src_remain -= 1;
	    }
	}
    }

  return migration_count;
}

static void
set_rebalance_node_type (int node_count, T_REBALANCE_AMOUNT * rbl_amount,
			 int rbl_src_count, const int *rbl_src_node,
			 int rbl_dest_count, const int *rbl_dest_node)
{
  int i;

  if (rbl_src_count == 0)
    {
      for (i = 0; i < node_count; i++)
	{
	  if (is_rebalance_node (rbl_amount[i].nodeid, rbl_dest_count,
				 rbl_dest_node))
	    {
	      rbl_amount[i].type = RBL_NODE_TYPE_DEST;
	    }
	  else
	    {
	      rbl_amount[i].type = RBL_NODE_TYPE_SOURCE;
	    }
	}
    }
  else if (rbl_dest_count == 0)
    {
      for (i = 0; i < node_count; i++)
	{
	  if (is_rebalance_node (rbl_amount[i].nodeid, rbl_src_count,
				 rbl_src_node))
	    {
	      rbl_amount[i].type = RBL_NODE_TYPE_SOURCE;
	    }
	  else
	    {
	      rbl_amount[i].type = RBL_NODE_TYPE_DEST;
	    }
	}
    }
  else
    {
      for (i = 0; i < node_count; i++)
	{
	  if (is_rebalance_node (rbl_amount[i].nodeid, rbl_src_count,
				 rbl_src_node))
	    {
	      rbl_amount[i].type = RBL_NODE_TYPE_SOURCE;
	    }
	  else if (is_rebalance_node (rbl_amount[i].nodeid, rbl_dest_count,
				      rbl_dest_node))
	    {
	      rbl_amount[i].type = RBL_NODE_TYPE_DEST;
	    }
	  else
	    {
	      rbl_amount[i].nodeid = 0;	/* unknown type. ignore node id */
	    }
	}
    }
}

static int
get_groupid_count (T_REBALANCE_AMOUNT * rbl_amount,
		   const T_DB_NODE_INFO * db_node_info,
		   const T_DB_GROUPID_INFO * db_groupid_info,
		   int rbl_src_count, const int *rbl_src_node,
		   int rbl_dest_count, const int *rbl_dest_node)
{
  int node_count = 0;
  int i, j;

  /* extract all nodeid from groupid table */
  for (i = 0; i < db_groupid_info->group_id_count; i++)
    {
      bool found = false;
      for (j = 0; j < node_count; j++)
	{
	  if (db_groupid_info->gid_info[i].nodeid == rbl_amount[j].nodeid)
	    {
	      rbl_amount[j].count++;
	      found = true;
	      break;
	    }
	}

      if (found == false)
	{
	  if (node_count >= db_node_info->node_info_count)
	    {
	      assert (false);
	      return BR_ER_SHARD_INFO_NOT_AVAILABLE;
	    }

	  rbl_amount[node_count].nodeid = db_groupid_info->gid_info[i].nodeid;
	  rbl_amount[node_count].count = 1;
	  node_count++;
	}
    }

  /* add rbl_dest_nodeid if not included */
  for (i = 0; i < rbl_dest_count; i++)
    {
      bool found = false;
      for (j = 0; j < node_count; j++)
	{
	  if (rbl_dest_node[i] == rbl_amount[j].nodeid)
	    {
	      found = true;
	      break;
	    }
	}

      if (found == false)
	{
	  if (node_count >= db_node_info->node_info_count)
	    {
	      assert (false);
	      return BR_ER_SHARD_INFO_NOT_AVAILABLE;
	    }

	  rbl_amount[node_count].nodeid = rbl_dest_node[i];
	  rbl_amount[node_count].count = 0;
	  node_count++;
	}
    }

  /* exclude nodeid if the node id is neither src node nor dest node */
  for (i = 0; i < node_count; i++)
    {
      if (!is_rebalance_node (rbl_amount[i].nodeid, rbl_src_count,
			      rbl_src_node) &&
	  !is_rebalance_node (rbl_amount[i].nodeid, rbl_dest_count,
			      rbl_dest_node))
	{
	  rbl_amount[i].nodeid = 0;
	}
    }

  return node_count;
}

static int
set_mig_info_src_nodeid (T_GROUP_MIGRATION_INFO * mig_info, int offset,
			 int mig_src_nodeid, int mig_count,
			 const T_DB_GROUPID_INFO * db_groupid_info)
{
  int i;

  for (i = 0; i < db_groupid_info->group_id_count && mig_count > 0; i++)
    {
      if (db_groupid_info->gid_info[i].nodeid == mig_src_nodeid)
	{
	  mig_info[offset].groupid = db_groupid_info->gid_info[i].gid;
	  mig_info[offset].src_nodeid = mig_src_nodeid;
	  offset++;
	  mig_count--;
	}
    }

  return offset;
}

static int
set_mig_info_dest_nodeid (T_GROUP_MIGRATION_INFO * mig_info, int offset,
			  int mig_dest_nodeid, int mig_count)
{
  int i;

  for (i = 0; i < mig_count; i++)
    {
      mig_info[offset].dest_nodeid = mig_dest_nodeid;
      offset++;
    }
  return offset;
}

static int
make_rebalance_info (T_GROUP_MIGRATION_INFO ** ret_mig_info, bool empty_node,
		     int rbl_src_count, const int *rbl_src_node,
		     int rbl_dest_count, const int *rbl_dest_node,
		     const T_DB_NODE_INFO * db_node_info,
		     const T_DB_GROUPID_INFO * db_groupid_info)
{
  T_REBALANCE_AMOUNT *rbl_amount;
  int node_count;
  T_GROUP_MIGRATION_INFO *mig_info;
  int migration_count;
  int i, offset;

  if (db_node_info == NULL || db_groupid_info == NULL)
    {
      return BR_ER_SHARD_INFO_NOT_AVAILABLE;
    }

  for (i = 0; i < rbl_src_count; i++)
    {
      if (is_existing_nodeid (db_node_info, rbl_src_node[i]) == false)
	{
	  return BR_ER_NODE_INFO_NOT_EXIST;
	}
    }
  for (i = 0; i < rbl_dest_count; i++)
    {
      if (is_existing_nodeid (db_node_info, rbl_dest_node[i]) == false)
	{
	  return BR_ER_NODE_INFO_NOT_EXIST;
	}
    }


  rbl_amount = RYE_MALLOC (sizeof (T_REBALANCE_AMOUNT) *
			   db_node_info->node_info_count);
  if (rbl_amount == NULL)
    {
      return BR_ER_NO_MORE_MEMORY;
    }

  memset (rbl_amount, 0,
	  sizeof (T_REBALANCE_AMOUNT) * db_node_info->node_info_count);

  node_count = get_groupid_count (rbl_amount,
				  db_node_info, db_groupid_info,
				  rbl_src_count, rbl_src_node,
				  rbl_dest_count, rbl_dest_node);
  if (node_count < 0)
    {
      RYE_FREE_MEM (rbl_amount);
      return node_count;
    }

  set_rebalance_node_type (node_count, rbl_amount,
			   rbl_src_count, rbl_src_node,
			   rbl_dest_count, rbl_dest_node);

  migration_count = calc_rebalance_count (node_count, rbl_amount, empty_node);

  mig_info = RYE_MALLOC (sizeof (T_GROUP_MIGRATION_INFO) * migration_count);
  if (mig_info == NULL)
    {
      RYE_FREE_MEM (rbl_amount);
      return BR_ER_NO_MORE_MEMORY;
    }

  memset (mig_info, 0, sizeof (T_GROUP_MIGRATION_INFO) * migration_count);

  offset = 0;
  for (i = 0; i < node_count; i++)
    {
      if (rbl_amount[i].nodeid > 0
	  && rbl_amount[i].type == RBL_NODE_TYPE_SOURCE)
	{
	  offset = set_mig_info_src_nodeid (mig_info, offset,
					    rbl_amount[i].nodeid,
					    MIN (migration_count - offset,
						 rbl_amount[i].mig_count),
					    db_groupid_info);
	}
    }

  offset = 0;
  for (i = 0; i < node_count; i++)
    {
      if (rbl_amount[i].nodeid > 0
	  && rbl_amount[i].type == RBL_NODE_TYPE_DEST)
	{
	  offset = set_mig_info_dest_nodeid (mig_info, offset,
					     rbl_amount[i].nodeid,
					     MIN (migration_count - offset,
						  rbl_amount[i].mig_count));
	}
    }

  RYE_FREE_MEM (rbl_amount);
  *ret_mig_info = mig_info;

  return migration_count;
}

static int64_t
make_cur_ts_bigint ()
{
  struct timeval t;
  int64_t cur_ts;

  gettimeofday (&t, NULL);

  cur_ts = t.tv_sec;
  cur_ts = (cur_ts * 1000) + (t.tv_usec / 1000);

  return cur_ts;
}

static int
get_migration_stats (CCI_CONN * conn, T_MIGRATION_STATS ** ret_stats)
{
  T_BIND_PARAM bind_param[8];
  char sql[SQL_BUF_SIZE];
  CCI_STMT stmt;
  int res;
  T_MIGRATION_STATS *stats;
  int i;
  int64_t cur_ts = make_cur_ts_bigint ();
  int64_t timeout_msec = MIGRATOR_START_WAIT_TIMEOUT_MSEC;
  int status_scheduled = GROUP_MIGRATION_STATUS_SCHEDULED;
  int status_migrator_run = GROUP_MIGRATION_STATUS_MIGRATOR_RUN;
  int status_mig_started = GROUP_MIGRATION_STATUS_MIGRATION_STARTED;
  int status_complete = GROUP_MIGRATION_STATUS_COMPLETE;
  int status_failed = GROUP_MIGRATION_STATUS_FAILED;
  int num_waiting_gid = 0;
  int num_bind = 0;

  sprintf (sql,
	   "SELECT %s, "
	   "   SUM(CASE WHEN %s = ? THEN 1 ELSE 0 END), "
	   "   SUM(CASE WHEN %s = ? THEN 1 ELSE 0 END), "
	   "   SUM(CASE WHEN %s = ? AND %s < (? - ?) THEN 1 ELSE 0 END), "
	   "   SUM(CASE WHEN %s = ? THEN 1 ELSE 0 END), "
	   "   SUM(CASE WHEN %s = ? THEN 1 ELSE 0 END), "
	   "   SUM(CASE WHEN %s = ? THEN 1 ELSE 0 END), "
	   "   COUNT(*) "
	   "FROM %s GROUP BY %s",
	   COL_SHARD_MIGRATION_SRC_NODEID,
	   COL_SHARD_MIGRATION_STATUS,
	   COL_SHARD_MIGRATION_STATUS,
	   COL_SHARD_MIGRATION_STATUS, COL_SHARD_MIGRATION_MODIFIED_AT,
	   COL_SHARD_MIGRATION_STATUS,
	   COL_SHARD_MIGRATION_STATUS,
	   COL_SHARD_MIGRATION_STATUS,
	   TABLE_SHARD_MIGRATION, COL_SHARD_MIGRATION_SRC_NODEID);

  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT, &status_scheduled);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT, &status_migrator_run);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT, &status_migrator_run);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_BIGINT, &cur_ts);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_BIGINT, &timeout_msec);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT, &status_mig_started);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT, &status_complete);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT, &status_failed);

  assert ((int) DIM (bind_param) >= num_bind);

  res = admin_query_execute (conn, true, &stmt, sql, num_bind, bind_param);
  if (res < 0)
    {
      return BR_ER_METADB;
    }

  if (res == 0)
    {
      return 0;
    }

  stats = RYE_MALLOC (sizeof (T_MIGRATION_STATS) * (res + 1));
  if (stats == NULL)
    {
      return BR_ER_NO_MORE_MEMORY;
    }

  for (i = 0; i < res; i++)
    {
      int ind;
      int run_mig;

      if (cci_fetch_next (&stmt) < 0)
	{
	  for (; i < res; i++)
	    {
	      memset (&stats[i], 0, sizeof (T_MIGRATION_STATS));
	    }
	  break;
	}

      stats[i].src_nodeid = cci_get_int (&stmt, 1, &ind);
      stats[i].count_scheduled = cci_get_int (&stmt, 2, &ind);
      stats[i].count_migrator_run = cci_get_int (&stmt, 3, &ind);
      stats[i].count_migrator_expired = cci_get_int (&stmt, 4, &ind);
      stats[i].count_mig_started = cci_get_int (&stmt, 5, &ind);
      stats[i].count_complete = cci_get_int (&stmt, 6, &ind);
      stats[i].count_failed = cci_get_int (&stmt, 7, &ind);
      stats[i].count_all = cci_get_int (&stmt, 8, &ind);

      num_waiting_gid += (stats[i].count_scheduled +
			  stats[i].count_migrator_run +
			  stats[i].count_mig_started);

      assert (stats[i].count_all ==
	      (stats[i].count_scheduled + stats[i].count_migrator_run +
	       stats[i].count_mig_started + stats[i].count_complete +
	       stats[i].count_failed));

      run_mig = shm_Br_info->shard_mgmt_num_migrator;
      run_mig -= (stats[i].count_migrator_run + stats[i].count_mig_started);
      run_mig += stats[i].count_migrator_expired;
      stats[i].run_migration = MAX (run_mig, 0);
    }

  memset (&stats[res], 0, sizeof (T_MIGRATION_STATS));

  cci_close_req_handle (&stmt);

  *ret_stats = stats;
  return num_waiting_gid;
}

static int
update_status_expired_migrator (CCI_CONN * conn,
				const T_MIGRATION_STATS * mig_stats)
{
  T_BIND_PARAM bind_param[5];
  int64_t cur_ts = make_cur_ts_bigint ();
  char update_sql[SQL_BUF_SIZE];
  int next_status = GROUP_MIGRATION_STATUS_FAILED;
  int cur_status = GROUP_MIGRATION_STATUS_MIGRATOR_RUN;
  int64_t timeout_msec = MIGRATOR_START_WAIT_TIMEOUT_MSEC;
  int i, sum_expired_migrator = 0;
  int res;
  int num_bind = 0;

  for (i = 0; mig_stats[i].src_nodeid > 0; i++)
    {
      sum_expired_migrator = mig_stats[i].count_migrator_expired;
    }
  if (sum_expired_migrator <= 0)
    {
      return 0;
    }

  sprintf (update_sql,
	   "UPDATE %s SET %s = ?, %s = ? "
	   "WHERE %s > 0 and %s = ? and %s < (? - ?) ",
	   TABLE_SHARD_MIGRATION,
	   COL_SHARD_MIGRATION_STATUS, COL_SHARD_MIGRATION_MODIFIED_AT,
	   COL_SHARD_MIGRATION_SRC_NODEID,
	   COL_SHARD_MIGRATION_STATUS, COL_SHARD_MIGRATION_MODIFIED_AT);

  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT, &next_status);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_BIGINT, &cur_ts);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT, &cur_status);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_BIGINT, &cur_ts);
  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_BIGINT, &timeout_msec);

  assert ((int) DIM (bind_param) >= num_bind);

  res = admin_query_execute (conn, true, NULL, update_sql,
			     num_bind, bind_param);

  assert (res < 0 || sum_expired_migrator <= res);

  br_log_write (BROKER_LOG_NOTICE, INADDR_NONE, "migrator timeout : %d", res);

  return res;
}

static int
change_db_migration_status (CCI_CONN * conn, T_MIGRATION_STATS * mig_stats,
			    T_GROUP_MIGRATION_INFO ** ret_mig_info)
{
  int i;
  char select_sql[SQL_BUF_SIZE];
  char update_sql[SQL_BUF_SIZE];
  CCI_STMT select_stmt;
  CCI_STMT update_stmt;
  T_BIND_PARAM bind_param[3];
  int res;
  T_GROUP_MIGRATION_INFO *mig_info;
  int mig_info_count = 0;

  for (i = 0; mig_stats[i].src_nodeid > 0; i++)
    {
      assert (mig_stats[i].run_migration >= 0);
      mig_info_count += mig_stats[i].run_migration;
    }

  if (mig_info_count == 0)
    {
      return 0;
    }

  mig_info_count++;
  mig_info = RYE_MALLOC (sizeof (T_GROUP_MIGRATION_INFO) * mig_info_count);
  if (mig_info == NULL)
    {
      return BR_ER_NO_MORE_MEMORY;
    }
  memset (mig_info, 0, sizeof (T_GROUP_MIGRATION_INFO) * mig_info_count);

  sprintf (select_sql,
	   "SELECT %s, %s FROM %s WHERE %s = ? and %s = ? LIMIT ? FOR UPDATE",
	   COL_SHARD_MIGRATION_GROUPID, COL_SHARD_MIGRATION_DEST_NODEID,
	   TABLE_SHARD_MIGRATION,
	   COL_SHARD_MIGRATION_SRC_NODEID, COL_SHARD_MIGRATION_STATUS);
  sprintf (update_sql, "UPDATE %s SET %s = ?, %s = ? WHERE %s = ?  ",
	   TABLE_SHARD_MIGRATION,
	   COL_SHARD_MIGRATION_STATUS, COL_SHARD_MIGRATION_MODIFIED_AT,
	   COL_SHARD_MIGRATION_GROUPID);

  if (cci_prepare (conn, &select_stmt, select_sql, 0) < 0)
    {
      BR_LOG_CCI_ERROR ("prepare", conn->err_buf.err_code,
			conn->err_buf.err_msg, select_sql);
      RYE_FREE_MEM (mig_info);
      return BR_ER_METADB;
    }
  if (cci_prepare (conn, &update_stmt, update_sql, 0) < 0)
    {
      BR_LOG_CCI_ERROR ("prepare", conn->err_buf.err_code,
			conn->err_buf.err_msg, update_sql);
      RYE_FREE_MEM (mig_info);
      return BR_ER_METADB;
    }

  mig_info_count = 0;
  for (i = 0; mig_stats[i].src_nodeid > 0; i++)
    {
      int cur_status = GROUP_MIGRATION_STATUS_SCHEDULED;
      int tuple_idx;
      int run_migration;
      int num_bind = 0;

      run_migration = mig_stats[i].run_migration;

      if (run_migration <= 0 || mig_stats[i].count_scheduled <= 0)
	{
	  mig_stats[i].run_migration = 0;
	  continue;
	}

      SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT,
		      &mig_stats[i].src_nodeid);
      SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT, &cur_status);
      SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT, &run_migration);

      assert ((int) DIM (bind_param) >= num_bind);

      res = admin_query_execute (conn, false, &select_stmt, select_sql,
				 num_bind, bind_param);
      if (res < 0)
	{
	  RYE_FREE_MEM (mig_info);
	  return res;
	}

      for (tuple_idx = 0; tuple_idx < run_migration; tuple_idx++)
	{
	  int ind;
	  int next_status = GROUP_MIGRATION_STATUS_MIGRATOR_RUN;
	  int groupid, dest_nodeid;
	  int64_t cur_ts = make_cur_ts_bigint ();
	  int num_bind = 0;

	  if (cci_fetch_next (&select_stmt) < 0)
	    {
	      break;
	    }
	  groupid = cci_get_int (&select_stmt, 1, &ind);
	  dest_nodeid = cci_get_int (&select_stmt, 2, &ind);

	  mig_info[mig_info_count].groupid = groupid;
	  mig_info[mig_info_count].dest_nodeid = dest_nodeid;
	  mig_info[mig_info_count].src_nodeid = mig_stats[i].src_nodeid;

	  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT, &next_status);
	  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_BIGINT, &cur_ts);
	  SET_BIND_PARAM (num_bind++, bind_param, CCI_TYPE_INT, &groupid);

	  assert ((int) DIM (bind_param) >= num_bind);

	  res = admin_query_execute (conn, false, &update_stmt, update_sql,
				     num_bind, bind_param);
	  if (res < 0)
	    {
	      RYE_FREE_MEM (mig_info);
	      return res;
	    }
	  else if (res == 0)
	    {
	      assert (false);
	      break;
	    }
	  else
	    {
	      mig_info_count++;
	    }
	}

      mig_stats[i].run_migration = tuple_idx;	/* real count of mig run */
    }

  *ret_mig_info = mig_info;
  return mig_info_count;
}

static bool
is_existing_nodeid (const T_DB_NODE_INFO * db_node_info, int target_nodeid)
{
  if (find_node_info_by_nodeid (db_node_info, target_nodeid,
				HA_STATE_FOR_DRIVER_UNKNOWN, NULL) == NULL)
    {
      return false;
    }
  else
    {
      return true;
    }
}

static const T_SHARD_NODE_INFO *
find_node_info_by_nodeid (const T_DB_NODE_INFO * db_node_info,
			  int target_nodeid, HA_STATE_FOR_DRIVER ha_state,
			  struct drand48_data *rand_buf)
{
#define TARGET_NODE_FIND_MAX 4
  int i;
  const T_SHARD_NODE_INFO *target_nodes[TARGET_NODE_FIND_MAX];
  int num_found = 0;

  if (db_node_info == NULL)
    {
      return NULL;
    }

  for (i = 0; i < db_node_info->node_info_count; i++)
    {
      if (db_node_info->node_info[i].node_id == target_nodeid)
	{
	  HA_STATE_FOR_DRIVER node_ha_state;

	  switch (db_node_info->node_info[i].ha_state)
	    {
	    case HA_STATE_FOR_DRIVER_MASTER:
	    case HA_STATE_FOR_DRIVER_TO_BE_MASTER:
	      node_ha_state = HA_STATE_FOR_DRIVER_MASTER;
	      break;
	    case HA_STATE_FOR_DRIVER_SLAVE:
	    case HA_STATE_FOR_DRIVER_TO_BE_SLAVE:
	    case HA_STATE_FOR_DRIVER_REPLICA:
	      node_ha_state = HA_STATE_FOR_DRIVER_SLAVE;
	      break;
	    case HA_STATE_FOR_DRIVER_UNKNOWN:
	    default:
	      node_ha_state = HA_STATE_FOR_DRIVER_UNKNOWN;
	      break;
	    }

	  if (ha_state == HA_STATE_FOR_DRIVER_UNKNOWN ||
	      ha_state == node_ha_state)
	    {
	      target_nodes[num_found++] = &db_node_info->node_info[i];
	      if (num_found == TARGET_NODE_FIND_MAX)
		{
		  break;
		}
	    }
	}
    }

  if (num_found > 0)
    {
      if (rand_buf == NULL)
	{
	  return target_nodes[0];
	}
      else
	{
	  double r;
	  drand48_r (rand_buf, &r);
	  return target_nodes[(int) (num_found * r)];
	}
    }
  else
    {
      return NULL;
    }
}

static void
select_migration_node (const T_SHARD_NODE_INFO ** run_mig_node,
		       const T_SHARD_NODE_INFO ** dest_node_info,
		       bool * is_slave_mode_mig,
		       const T_DB_NODE_INFO * db_node_info,
		       int mig_src_nodeid, int mig_dest_nodeid,
		       struct drand48_data *rand_buf)
{
  *run_mig_node = find_node_info_by_nodeid (db_node_info,
					    mig_src_nodeid,
					    HA_STATE_FOR_DRIVER_SLAVE,
					    rand_buf);
  if (*run_mig_node != NULL)
    {
      if (dest_node_info != NULL)
	{
	  *dest_node_info = find_node_info_by_nodeid (db_node_info,
						      mig_dest_nodeid,
						      HA_STATE_FOR_DRIVER_UNKNOWN,
						      NULL);
	}

      *is_slave_mode_mig = true;

      return;
    }

  *run_mig_node = find_node_info_by_nodeid (db_node_info,
					    mig_src_nodeid,
					    HA_STATE_FOR_DRIVER_MASTER, NULL);

  if (dest_node_info != NULL)
    {
      *dest_node_info = find_node_info_by_nodeid (db_node_info,
						  mig_dest_nodeid,
						  HA_STATE_FOR_DRIVER_UNKNOWN,
						  NULL);
    }

  *is_slave_mode_mig = false;
}

static void
init_drand48_buffer (struct drand48_data *rand_buf)
{
  struct timeval t;
  gettimeofday (&t, NULL);
  srand48_r (t.tv_usec, rand_buf);
}

static int
launch_migrator_process (const char *global_dbname, int groupid,
			 int src_nodeid,
			 const T_SHARD_NODE_INFO * dest_node_info,
			 const T_SHARD_NODE_INFO * run_node_info,
			 bool schema_migration, bool is_slave_mode_mig,
			 T_CCI_LAUNCH_RESULT * launch_res)
{
  int argc;
  const char *argv[20];
  char src_nodeid_str[16];
  char dest_port_str[16];
  char dest_nodeid_str[16];
  char groupid_str[16];
  int error;
  bool wait_child;
  int timeout_msec;
  int num_env = 0;
  const char *envp[1];
  char dba_passwd_env[SRV_CON_DBPASSWD_SIZE + 10];	/* PASSWD=xxx */

  sprintf (src_nodeid_str, "%d", src_nodeid);
  sprintf (dest_nodeid_str, "%d", dest_node_info->node_id);
  sprintf (groupid_str, "%d", groupid);

  /* rye_migrator arguments. -h option will be set by local_mgmt */
  argc = 0;
  argv[argc++] = "";		/* command name. local mgmt overwrites */
  argv[argc++] = "--mgmt-port";
  argv[argc++] = shard_Mgmt_server_info.port_str;
  argv[argc++] = "--mgmt-dbname";
  argv[argc++] = global_dbname;
  argv[argc++] = "--src-node-id";
  argv[argc++] = src_nodeid_str;
  argv[argc++] = "--dst-dbname";
  argv[argc++] = dest_node_info->local_dbname;
  argv[argc++] = "--dst-node-id";
  argv[argc++] = dest_nodeid_str;

  if (schema_migration)
    {
      sprintf (dest_port_str, "%d",
	       PRM_NODE_INFO_GET_PORT (&dest_node_info->host_info));

      argv[argc++] = "--copy-schema";
      argv[argc++] = "--dst-host";
      argv[argc++] = dest_node_info->host_ip_str;
      argv[argc++] = "--dst-port";
      argv[argc++] = dest_port_str;
    }
  else
    {
      argv[argc++] = "--group-id";
      argv[argc++] = groupid_str;
    }

  if (is_slave_mode_mig)
    {
      argv[argc++] = "--run-slave";
    }

  if (schema_migration || groupid == 0)
    {
      wait_child = true;
      timeout_msec = -1;
    }
  else
    {
      wait_child = false;
      timeout_msec = LOCAL_MGMT_REQ_TIMEOUT_MSEC;
    }

  assert ((int) DIM (argv) >= argc);

  snprintf (dba_passwd_env, sizeof (dba_passwd_env), "PASSWD=%s",
	    shard_Mgmt_server_info.migrator_dba_passwd);
  envp[num_env++] = dba_passwd_env;

  assert ((int) DIM (envp) >= num_env);

  sprintf (launch_res->userdata, "%d", groupid);

  error = cci_mgmt_launch_process (launch_res, run_node_info->host_ip_str,
				   PRM_NODE_INFO_GET_PORT (&run_node_info->
							   host_info),
				   MGMT_LAUNCH_PROCESS_MIGRATOR,
				   true, wait_child,
				   argc, argv, num_env, envp, timeout_msec);
  return error;
}

static int
run_rebalance_migrator (const T_GROUP_MIGRATION_INFO * mig_info,
			const T_DB_NODE_INFO * db_node_info,
			const char *global_dbname)
{
  int i, error;
  struct drand48_data rand_buf;
  const T_SHARD_NODE_INFO *run_node_info;
  const T_SHARD_NODE_INFO *dest_node_info;
  T_CCI_LAUNCH_RESULT launch_res = CCI_LAUNCH_RESULT_INITIALIZER;
  bool is_slave_mode_mig;

  init_drand48_buffer (&rand_buf);

  for (i = 0; mig_info[i].groupid > 0; i++)
    {
      if (mig_info[i].src_nodeid <= 0 || mig_info[i].dest_nodeid <= 0 ||
	  mig_info[i].src_nodeid == mig_info[i].dest_nodeid)
	{
	  assert (0);
	  continue;
	}

      select_migration_node (&run_node_info, &dest_node_info,
			     &is_slave_mode_mig, db_node_info,
			     mig_info[i].src_nodeid, mig_info[i].dest_nodeid,
			     &rand_buf);
      if (run_node_info == NULL || dest_node_info == NULL)
	{
	  br_log_write (BROKER_LOG_ERROR, INADDR_NONE,
			"run migrator fail: cannot select migrator host. (groupid=%d, src_node=%d, dest_node = %d) selected node : %d",
			mig_info[i].groupid, mig_info[i].src_nodeid,
			mig_info[i].dest_nodeid,
			(run_node_info == NULL ? 0 : run_node_info->node_id));
	  continue;
	}

      error = launch_migrator_process (global_dbname, mig_info[i].groupid,
				       mig_info[i].src_nodeid, dest_node_info,
				       run_node_info, false,
				       is_slave_mode_mig, &launch_res);

      if (error < 0)
	{
	  br_log_write (BROKER_LOG_ERROR, INADDR_NONE,
			"run migrator fail: groupid=%d, src_node=%d, dest_node = %d",
			mig_info[i].groupid, mig_info[i].src_nodeid,
			mig_info[i].dest_nodeid);
	}
      else
	{
	  const char *ha_mode_str;
	  if (run_node_info->ha_state == HA_STATE_FOR_DRIVER_MASTER ||
	      run_node_info->ha_state == HA_STATE_FOR_DRIVER_TO_BE_MASTER)
	    {
	      ha_mode_str = "master";
	    }
	  else
	    {
	      ha_mode_str = "slave";
	    }

	  br_log_write (BROKER_LOG_NOTICE, INADDR_NONE,
			"run migrator: groupid=%d, src_node=%d, dest_node = %d "
			"- running on %s (nodeid:%d, %s)",
			mig_info[i].groupid, mig_info[i].src_nodeid,
			mig_info[i].dest_nodeid, run_node_info->host_name,
			run_node_info->node_id, ha_mode_str);
	}
    }

  return 0;
}

static void
set_shm_migration_info (const T_MIGRATION_STATS * mig_stats)
{
  int i;
  int scheduled = 0;
  int running = 0;
  int complete = 0;
  int failed = 0;

  for (i = 0; mig_stats[i].src_nodeid > 0; i++)
    {
      scheduled += mig_stats[i].count_scheduled;
      running += (mig_stats[i].count_migrator_run +
		  mig_stats[i].count_mig_started);
      complete += mig_stats[i].count_complete;
      failed += mig_stats[i].count_failed;
    }

  shm_Shard_mgmt_info->rbl_scheduled_count = scheduled;
  shm_Shard_mgmt_info->rbl_running_count = running;
  shm_Shard_mgmt_info->rbl_complete_count = complete;
  shm_Shard_mgmt_info->rbl_fail_count = failed;
}

static int
shd_mg_run_migration ()
{
  CCI_CONN conn;
  T_MIGRATION_STATS *stats = NULL;
  int num_waiting_gid = -1;
  int mig_info_count = -1;
  T_GROUP_MIGRATION_INFO *mig_info = NULL;
  int error = 0;
  T_DB_NODE_INFO *cp_node_info = NULL;
  char global_dbname[DBNAME_MAX_SIZE];

  sync_node_info (NULL, NULL, false);

  LOCK_DB_SHARD_INFO ();

  if (db_Sync_info.shard_db_rec.global_dbname == NULL)
    {
      error = BR_ER_SHARD_INFO_NOT_AVAILABLE;
    }
  else
    {
      COPY_GLOBAL_DBNAME (global_dbname);

      error = clone_db_node_info (&cp_node_info, db_Shard_info.db_node_info);
    }

  UNLOCK_DB_SHARD_INFO ();

  if (error < 0)
    {
      return error;
    }

  if (connect_metadb (&conn, CCI_AUTOCOMMIT_FALSE, shard_Mgmt_db_user,
		      shard_Mgmt_db_passwd) < 0)
    {
      db_node_info_free (cp_node_info);
      return BR_ER_METADB;
    }

  num_waiting_gid = get_migration_stats (&conn, &stats);
  if (num_waiting_gid < 0)
    {
      error = num_waiting_gid;
      goto run_migration_error;
    }

  set_shm_migration_info (stats);

  if (num_waiting_gid > 0)
    {
      mig_info_count = change_db_migration_status (&conn, stats, &mig_info);
      if (mig_info_count < 0)
	{
	  error = mig_info_count;
	  goto run_migration_error;
	}

      update_status_expired_migrator (&conn, stats);
    }

  if (cci_end_tran (&conn, CCI_TRAN_COMMIT) < 0)
    {
      error = BR_ER_METADB;
      goto run_migration_error;
    }

  cci_disconnect (&conn);

  if (mig_info_count > 0)
    {
      run_rebalance_migrator (mig_info, cp_node_info, global_dbname);
    }

  RYE_FREE_MEM (stats);
  RYE_FREE_MEM (mig_info);
  db_node_info_free (cp_node_info);

  return num_waiting_gid;

run_migration_error:

  cci_end_tran (&conn, CCI_TRAN_ROLLBACK);
  cci_disconnect (&conn);

  RYE_FREE_MEM (stats);
  RYE_FREE_MEM (mig_info);
  db_node_info_free (cp_node_info);
  return error;
}
