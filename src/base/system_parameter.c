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
 * system_parameter.c - system parameters
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include <float.h>
#include <errno.h>
#include <time.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/param.h>
#include <assert.h>
#include <ctype.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <fcntl.h>

#include "porting.h"
#include "chartype.h"
#include "misc_string.h"
#include "error_manager.h"
#include "storage_common.h"
#include "system_parameter.h"
#include "xserver_interface.h"
#include "util_func.h"
#include "log_comm.h"
#include "log_impl.h"
#include "memory_alloc.h"
#include "environment_variable.h"
#include "intl_support.h"
#include "log_manager.h"
#include "message_catalog.h"
#include "language_support.h"
#include "connection_defs.h"
#if defined (SERVER_MODE)
#include "server_support.h"
#include "boot_sr.h"
#endif /* SERVER_MODE */
#if defined (LINUX)
#include "stack_dump.h"
#endif
#include "ini_parser.h"
#include "tcp.h"
#include "heartbeat.h"
#include "repl_defs.h"
#include "utility.h"
#include "page_buffer.h"
#if !defined (CS_MODE)
#include "session.h"
#endif
#include "file_io_lock.h"

#include "fault_injection.h"
#include "rye_shm.h"


#define ER_LOG_FILE_DIR	"server"

#if !defined (CS_MODE)
static const char sysprm_error_log_file[] = "rye_server.err";
#else /* CS_MODE */
static const char sysprm_error_log_file[] = "rye_client.err";
#endif /* CS_MODE */

const char sysprm_cm_conf_file_name[] = "cm.conf";
const char sysprm_auto_conf_file_name[] = "rye-auto.conf";

typedef enum
{
  SYSPRM_UPD_KEY = 1,
  SYSPRM_DEL_PROC,
  SYSPRM_DEL_SECT,
  SYSPRM_DEL_KEY
} SYSPRM_OP;

/*
 * System variable names
 */

#define PRM_NAME_ER_LOG_DEBUG "er_log_debug"

#define PRM_NAME_ER_LOG_LEVEL "error_log_level"

#define PRM_NAME_ER_LOG_WARNING "error_log_warning"

#define PRM_NAME_ER_EXIT_ASK "inquire_on_exit"

#define PRM_NAME_ER_LOG_SIZE "error_log_size"

#define PRM_NAME_ER_LOG_FILE "error_log"

#define PRM_NAME_IO_LOCKF_ENABLE "file_lock"

#define PRM_NAME_SORT_BUFFER_SIZE "sort_buffer_size"

#define PRM_NAME_PAGE_BUFFER_SIZE "data_buffer_size"

#define PRM_NAME_PB_BUFFER_FLUSH_RATIO "data_buffer_flush_ratio"

#define PRM_NAME_HF_MAX_BESTSPACE_ENTRIES "max_bestspace_entries"

#define PRM_NAME_BT_UNFILL_FACTOR "index_unfill_factor"

#define PRM_NAME_BT_OID_BUFFER_SIZE "index_scan_oid_buffer_size"

#define PRM_NAME_BT_INDEX_SCAN_OID_ORDER "index_scan_in_oid_order"

#define PRM_NAME_BOSR_MAXTMP_SIZE "temp_file_max_size"

#define PRM_NAME_LK_TIMEOUT "lock_timeout"

#define PRM_NAME_LK_RUN_DEADLOCK_INTERVAL "deadlock_detection_interval_in_secs"

#define PRM_NAME_LOG_BUFFER_SIZE "log_buffer_size"

#define PRM_NAME_LOG_CHECKPOINT_SIZE "checkpoint_every_size"

#define PRM_NAME_LOG_CHECKPOINT_INTERVAL "checkpoint_interval"

#define PRM_NAME_LOG_CHECKPOINT_FLUSH_INTERVAL "checkpoint_flush_interval"

#define PRM_NAME_COMMIT_ON_SHUTDOWN "commit_on_shutdown"

#define PRM_NAME_SHUTDOWN_WAIT_TIME "shutdown_wait_time"

#define PRM_NAME_RSQL_AUTO_COMMIT "rsql_auto_commit"

#define PRM_NAME_WS_HASHTABLE_SIZE "initial_workspace_table_size"

#define PRM_NAME_WS_MEMORY_REPORT "workspace_memory_report"

#define PRM_NAME_TCP_PORT_ID "rye_port_id"

#define PRM_NAME_TCP_CONNECTION_TIMEOUT "connection_timeout"

#define PRM_NAME_OPTIMIZATION_LEVEL "optimization_level"

#define PRM_NAME_QO_DUMP "qo_dump"

#define PRM_NAME_THREAD_STACKSIZE "thread_stacksize"

#define PRM_NAME_IO_BACKUP_NBUFFERS "backup_buffer_pages"

#define PRM_NAME_IO_BACKUP_MAX_VOLUME_SIZE "backup_volume_max_size_bytes"

#define PRM_NAME_IO_BACKUP_SLEEP "backup_sleep"

#define PRM_NAME_MAX_PAGES_IN_TEMP_FILE_CACHE "max_pages_in_temp_file_cache"

#define PRM_NAME_MAX_ENTRIES_IN_TEMP_FILE_CACHE "max_entries_in_temp_file_cache"

#define PRM_NAME_TEMP_MEM_BUFFER_SIZE "temp_file_memory_size"

#define PRM_NAME_INDEX_SCAN_KEY_BUFFER_SIZE "index_scan_key_buffer_size"

#define PRM_NAME_ENABLE_HISTO "communication_histogram"

#define PRM_NAME_PB_NUM_LRU_CHAINS "num_LRU_chains"

#define PRM_NAME_PAGE_BG_FLUSH_INTERVAL "page_flush_interval"

#define PRM_NAME_ADAPTIVE_FLUSH_CONTROL "adaptive_flush_control"

#define PRM_NAME_MAX_FLUSH_SIZE_PER_SECOND "max_flush_size_per_second"

#define PRM_NAME_PB_SYNC_ON_FLUSH_SIZE "sync_on_flush_size"

#define PRM_NAME_PB_DEBUG_PAGE_VALIDATION_LEVEL "page_validation_level"

#define PRM_NAME_ANSI_QUOTES "ansi_quotes"

#define PRM_NAME_TEST_MODE "test_mode"

#define PRM_NAME_GROUP_CONCAT_MAX_LEN "group_concat_max_len"

#define PRM_NAME_LIKE_TERM_SELECTIVITY "like_term_selectivity"

#define PRM_NAME_SUPPRESS_FSYNC "suppress_fsync"

#define PRM_NAME_CALL_STACK_DUMP_ON_ERROR "call_stack_dump_on_error"

#define PRM_NAME_CALL_STACK_DUMP_ACTIVATION "call_stack_dump_activation_list"

#define PRM_NAME_CALL_STACK_DUMP_DEACTIVATION "call_stack_dump_deactivation_list"

#define PRM_NAME_MIGRATOR_COMPRESSED_PROTOCOL "migrator_compressed_protocol"

#define PRM_NAME_AUTO_RESTART_SERVER "auto_restart_server"

#define PRM_NAME_XASL_MAX_PLAN_CACHE_ENTRIES "max_plan_cache_entries"

#define PRM_NAME_XASL_MAX_PLAN_CACHE_CLONES "max_plan_cache_clones"

#define PRM_NAME_XASL_PLAN_CACHE_TIMEOUT "plan_cache_timeout"

#define PRM_NAME_USE_ORDERBY_SORT_LIMIT  "use_orderby_sort_limit"

#define PRM_NAME_LOGWR_COMPRESSED_PROTOCOL "log_writer_compressed_protocol"

#define PRM_NAME_HA_MODE "ha_mode"

#define PRM_NAME_HA_NODE_LIST "ha_node_list"

#define PRM_NAME_HA_REPLICA_LIST "ha_replica_list"

#define PRM_NAME_HA_DB_LIST "ha_db_list"

#define PRM_NAME_HA_COPY_LOG_BASE "ha_copy_log_base"

#define PRM_NAME_HA_COPY_SYNC_MODE "ha_copy_sync_mode"

#define PRM_NAME_HA_APPLY_MAX_MEM_SIZE "ha_apply_max_mem_size"

#define PRM_NAME_HA_PORT_ID "ha_port_id"

#define PRM_NAME_HA_INIT_TIMER "ha_init_timer"

#define PRM_NAME_HA_HEARTBEAT_INTERVAL "ha_heartbeat_interval"

#define PRM_NAME_HA_CALC_SCORE_INTERVAL "ha_calc_score_interval"

#define PRM_NAME_HA_FAILOVER_WAIT_TIME "ha_failover_wait_time"

#define PRM_NAME_HA_PROCESS_START_CONFIRM_INTERVAL "ha_process_start_confirm_interval"

#define PRM_NAME_HA_PROCESS_DEREG_CONFIRM_INTERVAL "ha_process_dereg_confirm_interval"

#define PRM_NAME_HA_MAX_PROCESS_START_CONFIRM "ha_max_process_start_confirm"

#define PRM_NAME_HA_MAX_PROCESS_DEREG_CONFIRM "ha_max_process_dereg_confirm"

#define PRM_NAME_HA_CHANGESLAVE_MAX_WAIT_TIME "ha_changeslave_max_wait_time"

#define PRM_NAME_HA_UNACCEPTABLE_PROC_RESTART_TIMEDIFF "ha_unacceptable_proc_restart_timediff"

#define PRM_NAME_SERVER_STATE_SYNC_INTERVAL "server_state_sync_interval"

#define PRM_NAME_HA_MAX_HEARTBEAT_GAP "ha_max_heartbeat_gap"

#define PRM_NAME_HA_PING_HOSTS "ha_ping_hosts"

#define PRM_NAME_HA_IGNORE_ERROR_LIST "ha_ignore_error_list"

#define PRM_NAME_HA_SQL_LOG_MAX_SIZE_IN_MB "ha_sql_log_max_size_in_mbytes"

#define PRM_NAME_HA_COPY_LOG_MAX_ARCHIVES "ha_copy_log_max_archives"

#define PRM_NAME_HA_COPY_LOG_TIMEOUT "ha_copy_log_timeout"

#define PRM_NAME_HA_REPLICA_DELAY "ha_replica_delay"

#define PRM_NAME_HA_REPLICA_TIME_BOUND "ha_replica_time_bound"

#define PRM_NAME_HA_DELAY_LIMIT "ha_delay_limit"

#define PRM_NAME_HA_DELAY_LIMIT_DELTA "ha_delay_limit_delta"

#define PRM_NAME_HA_CHECK_DISK_FAILURE_INTERVAL "ha_check_disk_failure_interval"

#define PRM_NAME_LOG_ASYNC_LOG_FLUSH_INTERVAL "async_log_flush_interval"

#define PRM_NAME_LOG_COMPRESS "log_compress"

#define PRM_NAME_BLOCK_NOWHERE_STATEMENT "block_nowhere_statement"

#define PRM_NAME_BLOCK_DDL_STATEMENT "block_ddl_statement"

#define PRM_NAME_RSQL_HISTORY_NUM "rsql_history_num"

#define PRM_NAME_LOG_TRACE_DEBUG "log_trace_debug"

#define PRM_NAME_ER_PRODUCTION_MODE "error_log_production_mode"

#define PRM_NAME_ER_STOP_ON_ERROR "stop_on_error"

#define PRM_NAME_TCP_RCVBUF_SIZE "tcp_rcvbuf_size"

#define PRM_NAME_TCP_SNDBUF_SIZE "tcp_sndbuf_size"

#define PRM_NAME_TCP_NODELAY "tcp_nodelay"

#define PRM_NAME_TCP_KEEPALIVE "tcp_keepalive"

#define PRM_NAME_RSQL_SINGLE_LINE_MODE "rsql_single_line_mode"

#define PRM_NAME_XASL_DEBUG_DUMP "xasl_debug_dump"

#define PRM_NAME_LOG_MAX_ARCHIVES "log_max_archives"

#define PRM_NAME_FORCE_REMOVE_LOG_ARCHIVES "force_remove_log_archives"

#define PRM_NAME_REMOVE_LOG_ARCHIVES_INTERVAL "remove_log_archive_interval_in_secs"

#define PRM_NAME_EVENT_HANDLER "event_handler"

#define PRM_NAME_EVENT_ACTIVATION "event_activation_list"

#define PRM_NAME_MNT_WAITING_THREAD "monitor_waiting_thread"

#define PRM_NAME_MNT_STATS_THRESHOLD "monitor_stats_threshold"

#define PRM_NAME_SESSION_STATE_TIMEOUT "session_state_timeout"

#define PRM_NAME_MULTI_RANGE_OPT_LIMIT "multi_range_optimization_limit"

#define PRM_NAME_ACCESS_IP_CONTROL "access_ip_control"

#define PRM_NAME_ACCESS_IP_CONTROL_FILE "access_ip_control_file"

#define PRM_NAME_DB_VOLUME_SIZE "db_volume_size"

#define PRM_NAME_CHECK_PEER_ALIVE "check_peer_alive"

#define PRM_NAME_SQL_TRACE_EXECUTION_PLAN "sql_trace_execution_plan"

#define PRM_NAME_SQL_TRACE_SLOW "sql_trace_slow"

#define PRM_NAME_SERVER_TRACE "server_trace"

#define PRM_NAME_LOG_TRACE_FLUSH_TIME "log_trace_flush_time"

#define PRM_NAME_GENERIC_VOL_PREALLOC_SIZE "generic_vol_prealloc_size"

#define PRM_NAME_SORT_LIMIT_MAX_COUNT "sort_limit_max_count"

#define PRM_NAME_SQL_TRACE_IOREADS "sql_trace_ioread_pages"

/* For query profile. Internal use only. */
#define PRM_NAME_QUERY_TRACE "query_trace"
#define PRM_NAME_QUERY_TRACE_FORMAT "query_trace_format"

#define PRM_NAME_MAX_RECURSION_SQL_DEPTH "max_recursion_sql_depth"

#define PRM_NAME_BTREE_MERGE_ENABLED "btree_merge_enabled"

/* For shard debugging */
#define PRM_NAME_SHARD_GROUPID_DEBUG "shard_groupid_debug"
#define PRM_NAME_SHARD_GROUPID_01     "shard_groupid_01"
#define PRM_NAME_SHARD_GROUPID_02     "shard_groupid_02"
#define PRM_NAME_SHARD_GROUPID_03     "shard_groupid_03"
#define PRM_NAME_SHARD_GROUPID_04     "shard_groupid_04"
#define PRM_NAME_SHARD_GROUPID_05     "shard_groupid_05"
#define PRM_NAME_SHARD_GROUPID_06     "shard_groupid_06"
#define PRM_NAME_SHARD_GROUPID_07     "shard_groupid_07"
#define PRM_NAME_SHARD_GROUPID_08     "shard_groupid_08"
#define PRM_NAME_SHARD_GROUPID_09     "shard_groupid_09"
#define PRM_NAME_SHARD_GROUPID_10     "shard_groupid_10"

#define PRM_NAME_OPTIMIZER_RESERVE_01 "optimizer_reserve_01"
#define PRM_NAME_OPTIMIZER_RESERVE_02 "optimizer_reserve_02"
#define PRM_NAME_OPTIMIZER_RESERVE_03 "optimizer_reserve_03"
#define PRM_NAME_OPTIMIZER_RESERVE_04 "optimizer_reserve_04"
#define PRM_NAME_OPTIMIZER_RESERVE_05 "optimizer_reserve_05"
#define PRM_NAME_OPTIMIZER_RESERVE_06 "optimizer_reserve_06"
#define PRM_NAME_OPTIMIZER_RESERVE_07 "optimizer_reserve_07"
#define PRM_NAME_OPTIMIZER_RESERVE_08 "optimizer_reserve_08"
#define PRM_NAME_OPTIMIZER_RESERVE_09 "optimizer_reserve_09"
#define PRM_NAME_OPTIMIZER_RESERVE_10 "optimizer_reserve_10"
#define PRM_NAME_OPTIMIZER_RESERVE_11 "optimizer_reserve_11"
#define PRM_NAME_OPTIMIZER_RESERVE_12 "optimizer_reserve_12"
#define PRM_NAME_OPTIMIZER_RESERVE_13 "optimizer_reserve_13"
#define PRM_NAME_OPTIMIZER_RESERVE_14 "optimizer_reserve_14"
#define PRM_NAME_OPTIMIZER_RESERVE_15 "optimizer_reserve_15"
#define PRM_NAME_OPTIMIZER_RESERVE_16 "optimizer_reserve_16"
#define PRM_NAME_OPTIMIZER_RESERVE_17 "optimizer_reserve_17"
#define PRM_NAME_OPTIMIZER_RESERVE_18 "optimizer_reserve_18"
#define PRM_NAME_OPTIMIZER_RESERVE_19 "optimizer_reserve_19"
#define PRM_NAME_OPTIMIZER_RESERVE_20 "optimizer_reserve_20"

#define PRM_NAME_OPTIMIZER_ENABLE_AGGREGATE_OPTIMIZATION "optimizer_enable_aggregate_optimization"

#define PRM_NAME_HA_REPL_FILTER_TYPE "ha_repl_filter_type"
#define PRM_NAME_HA_REPL_FILTER_FILE "ha_repl_filter_file"

#define PRM_NAME_FAULT_INJECTION "fault_injection"

#define PRM_NAME_RYE_SHM_KEY "rye_shm_key"
#define PRM_NAME_HA_MAX_LOG_APPLIER "ha_max_log_applier"

#define PRM_NAME_MAX_CLIENTS "max_clients"
#define PRM_NAME_MAX_COPYLOG_CONNECIONTS "max_copylogdb_connections"
#define PRM_NAME_MIGRATOR_MAX_REPL_DELAY "migrator_max_repl_delay"
#define PRM_NAME_HA_NODE_MYSELF "ha_node_myself"

/*
 * Note about ERROR_LIST and INTEGER_LIST type
 * ERROR_LIST type is an array of bool type with the size of -(ER_LAST_ERROR)
 * INTEGER_LIST type is an array of int type where the first element is
 * the size of the array. The max size of INTEGER_LIST is 255.
 */

/*
 * Bit masks for flag representing status words
 */

/*
 * Static flags
 */
#define PRM_EMPTY_FLAG	    0x00000000	/* empty flag */
#define PRM_USER_CHANGE     0x00000001	/* user can change, not implemented */
#define PRM_FOR_CLIENT      0x00000002	/* is for client parameter */
#define PRM_FOR_SERVER      0x00000004	/* is for server parameter */
#define PRM_HIDDEN          0x00000008	/* is hidden */
#define PRM_RELOADABLE      0x00000010	/* is reloadable */
#define PRM_TEST_CHANGE     0x00000040	/* can only be changed in the test mode */
#define PRM_FOR_HA          0x00000080	/* is for heartbeat */
#define PRM_FORCE_SERVER    0x00000100	/* client should get value from server */

#define PRM_SIZE_UNIT       0x00001000	/* has size unit interface */
#define PRM_TIME_UNIT       0x00002000	/* has time unit interface */

#define PRM_DEPRECATED      0x40000000	/* is deprecated */
#define PRM_OBSOLETED       0x80000000	/* is obsoleted */

/*
 * Dynamic flags
 */
#define PRM_SET             0x00000001	/* has been set */
#define PRM_ALLOCATED       0x00000002	/* storage has been malloc'd */
#define PRM_DEFAULT_USED    0x00000004	/* Default value has been used */

/*
 * Macros to get data type
 */
#define PRM_IS_STRING(x)          ((x)->datatype == PRM_STRING)
#define PRM_IS_INTEGER(x)         ((x)->datatype == PRM_INTEGER)
#define PRM_IS_FLOAT(x)           ((x)->datatype == PRM_FLOAT)
#define PRM_IS_BOOLEAN(x)         ((x)->datatype == PRM_BOOLEAN)
#define PRM_IS_KEYWORD(x)         ((x)->datatype == PRM_KEYWORD)
#define PRM_IS_INTEGER_LIST(x)    ((x)->datatype == PRM_INTEGER_LIST)
#define PRM_IS_BIGINT(x)          ((x)->datatype == PRM_BIGINT)

/*
 * Macros to access bit fields
 */

#define PRM_USER_CAN_CHANGE(x)    (x & PRM_USER_CHANGE)
#define PRM_IS_FOR_CLIENT(x)      (x & PRM_FOR_CLIENT)
#define PRM_IS_FOR_SERVER(x)      (x & PRM_FOR_SERVER)
#define PRM_IS_HIDDEN(x)          (x & PRM_HIDDEN)
#define PRM_IS_RELOADABLE(x)      (x & PRM_RELOADABLE)
#define PRM_TEST_CHANGE_ONLY(x)   (x & PRM_TEST_CHANGE)
#define PRM_IS_FOR_HA(x)          (x & PRM_FOR_HA)
#define PRM_GET_FROM_SERVER(x)	  (x & PRM_FORCE_SERVER)
#define PRM_HAS_SIZE_UNIT(x)      (x & PRM_SIZE_UNIT)
#define PRM_HAS_TIME_UNIT(x)      (x & PRM_TIME_UNIT)

#define PRM_IS_DEPRECATED(x)      (x & PRM_DEPRECATED)
#define PRM_IS_OBSOLETED(x)       (x & PRM_OBSOLETED)

#define PRM_IS_SET(x)             (x & PRM_SET)
#define PRM_IS_ALLOCATED(x)       (x & PRM_ALLOCATED)
#define PRM_DEFAULT_VAL_USED(x)   (x & PRM_DEFAULT_USED)

/*
 * Macros to manipulate bit fields
 */

#define PRM_CLEAR_BIT(this, here)  (here &= ~this)
#define PRM_SET_BIT(this, here)    (here |= this)

/*
 * Macros to get values
 */

#define PRM_GET_INT(x)      (*((int *) (x)))
#define PRM_GET_FLOAT(x)    (*((float *) (x)))
#define PRM_GET_STRING(x)   (*((char **) (x)))
#define PRM_GET_BOOL(x)     (*((bool *) (x)))
#define PRM_GET_INTEGER_LIST(x) (*((int **) (x)))
#define PRM_GET_BIGINT(x)     (*((INT64 *) (x)))


/*
 * Other macros
 */
#define PRM_DEFAULT_BUFFER_SIZE 256

/* initial error and integer lists */
static int int_list_initial[1] = { 0 };

/*
 * Global variables of parameters' value
 * Default values for the parameters
 * Upper and lower bounds for the parameters
 */
bool PRM_ER_LOG_DEBUG = false;
#if !defined(NDEBUG)
static bool prm_er_log_debug_default = true;
#else /* !NDEBUG */
static bool prm_er_log_debug_default = false;
#endif /* !NDEBUG */
static unsigned int prm_er_log_debug_flag = 0;

int PRM_ER_LOG_LEVEL = ER_SYNTAX_ERROR_SEVERITY;
static int prm_er_log_level_default = ER_SYNTAX_ERROR_SEVERITY;
static int prm_er_log_level_lower = ER_FATAL_ERROR_SEVERITY;
static int prm_er_log_level_upper = ER_NOTIFICATION_SEVERITY;
static unsigned int prm_er_log_level_flag = 0;

bool PRM_ER_LOG_WARNING = false;
static bool prm_er_log_warning_default = false;
static unsigned int prm_er_log_warning_flag = 0;

int PRM_ER_EXIT_ASK = ER_EXIT_DEFAULT;
static int prm_er_exit_ask_default = ER_EXIT_DEFAULT;
static unsigned int prm_er_exit_ask_flag = 0;

UINT64 PRM_ER_LOG_SIZE = (100000 * 80L);
static UINT64 prm_er_log_size_default = (100000 * 80L);
static UINT64 prm_er_log_size_lower = (100 * 80);
static unsigned int prm_er_log_size_flag = 0;

const char *PRM_ER_LOG_FILE = sysprm_error_log_file;
static const char *prm_er_log_file_default = sysprm_error_log_file;
static unsigned int prm_er_log_file_flag = 0;

bool PRM_ACCESS_IP_CONTROL = false;
static bool prm_access_ip_control_default = false;
static unsigned int prm_access_ip_control_flag = 0;

const char *PRM_ACCESS_IP_CONTROL_FILE = "";
static const char *prm_access_ip_control_file_default = "";
static unsigned int prm_access_ip_control_file_flag = 0;

bool PRM_IO_LOCKF_ENABLE = true;
static bool prm_io_lockf_enable_default = true;
static unsigned int prm_io_lockf_enable_flag = 0;

INT64 PRM_SORT_BUFFER_SIZE = 128 * IO_MAX_PAGE_SIZE;
static INT64 prm_sort_buffer_size_default = 128 * IO_MAX_PAGE_SIZE;
static INT64 prm_sort_buffer_size_lower = 1 * IO_MAX_PAGE_SIZE;
static unsigned int prm_sort_buffer_size_flag = 0;

INT64 PRM_PAGE_BUFFER_SIZE = 32768 * IO_MAX_PAGE_SIZE;
static INT64 prm_page_buffer_size_default = 32768 * IO_MAX_PAGE_SIZE;
static INT64 prm_page_buffer_size_lower = 1024 * IO_MAX_PAGE_SIZE;
static unsigned int prm_page_buffer_size_flag = 0;

float PRM_PB_BUFFER_FLUSH_RATIO = 0.01f;
static float prm_pb_buffer_flush_ratio_default = 0.01f;
static float prm_pb_buffer_flush_ratio_lower = 0.01f;
static float prm_pb_buffer_flush_ratio_upper = 0.95f;
static unsigned int prm_pb_buffer_flush_ratio_flag = 0;

int PRM_HF_MAX_BESTSPACE_ENTRIES = 1000000;
static int prm_hf_max_bestspace_entries_default = 1000000;	/* 110 M */
static unsigned int prm_hf_max_bestspace_entries_flag = 0;
static unsigned int prm_hf_max_bestspace_entries_lower = 1000;

float PRM_BT_UNFILL_FACTOR = 0.05f;
static float prm_bt_unfill_factor_default = 0.05f;
static float prm_bt_unfill_factor_lower = 0.0f;
static float prm_bt_unfill_factor_upper = 0.5f;
static unsigned int prm_bt_unfill_factor_flag = 0;

INT64 PRM_BT_OID_BUFFER_SIZE = 4 * IO_MAX_PAGE_SIZE;
static INT64 prm_bt_oid_buffer_size_default = 4 * IO_MAX_PAGE_SIZE;
static INT64 prm_bt_oid_buffer_size_lower = (INT64) (0.4f * IO_MAX_PAGE_SIZE);
static INT64 prm_bt_oid_buffer_size_upper = 16 * IO_MAX_PAGE_SIZE;
static unsigned int prm_bt_oid_buffer_size_flag = 0;

bool PRM_BT_INDEX_SCAN_OID_ORDER = false;
static bool prm_bt_index_scan_oid_order_default = false;
static unsigned int prm_bt_index_scan_oid_order_flag = 0;

INT64 PRM_BOSR_MAXTMP_SIZE = INT_MIN;
static INT64 prm_bosr_maxtmp_size_default = -1;	/* Infinite */
static unsigned int prm_bosr_maxtmp_flag = 0;

INT64 PRM_LK_TIMEOUT = -1;
static INT64 prm_lk_timeout_default = -1;	/* Infinite */
static INT64 prm_lk_timeout_lower = -1;
static INT64 prm_lk_timeout_upper = INT_MAX;
static unsigned int prm_lk_timeout_flag = 0;

float PRM_LK_RUN_DEADLOCK_INTERVAL = 1.0f;
static float prm_lk_run_deadlock_interval_default = 1.0f;
static float prm_lk_run_deadlock_interval_lower = 0.1f;
static unsigned int prm_lk_run_deadlock_interval_flag = 0;

INT64 PRM_LOG_BUFFER_SIZE = LOGPB_BUFFER_SIZE_LOWER;
static INT64 prm_log_buffer_size_default = LOGPB_BUFFER_SIZE_LOWER;
static INT64 prm_log_buffer_size_lower = LOGPB_BUFFER_SIZE_LOWER;
static unsigned int prm_log_buffer_size_flag = 0;

INT64 PRM_LOG_CHECKPOINT_SIZE = 100000 * IO_MAX_PAGE_SIZE;
static INT64 prm_log_checkpoint_size_default = 100000 * IO_MAX_PAGE_SIZE;
static INT64 prm_log_checkpoint_size_lower = 10 * IO_MAX_PAGE_SIZE;
static unsigned int prm_log_checkpoint_size_flag = 0;

INT64 PRM_LOG_CHECKPOINT_INTERVAL = 360 * ONE_SEC;
static INT64 prm_log_checkpoint_interval_default = 360 * ONE_SEC;
static INT64 prm_log_checkpoint_interval_lower = 60 * ONE_SEC;
static unsigned int prm_log_checkpoint_interval_flag = 0;

INT64 PRM_LOG_CHECKPOINT_FLUSH_INTERVAL = 1;
static INT64 prm_log_checkpoint_flush_interval_default = 1;
static INT64 prm_log_checkpoint_flush_interval_lower = 0;
static unsigned int prm_log_checkpoint_flush_interval_flag = 0;

bool PRM_COMMIT_ON_SHUTDOWN = false;
static bool prm_commit_on_shutdown_default = false;
static unsigned int prm_commit_on_shutdown_flag = 0;

INT64 PRM_SHUTDOWN_WAIT_TIME = 600 * ONE_SEC;
static INT64 prm_shutdown_wait_time_default = 600 * ONE_SEC;
static INT64 prm_shutdown_wait_time_lower = 60 * ONE_SEC;
static unsigned int prm_shutdown_wait_time_flag = 0;

bool PRM_RSQL_AUTO_COMMIT = true;
static bool prm_rsql_auto_commit_default = true;
static unsigned int prm_rsql_auto_commit_flag = 0;

int PRM_WS_HASHTABLE_SIZE = 1024;
static int prm_ws_hashtable_size_default = 1024;
static int prm_ws_hashtable_size_lower = 1024;
static unsigned int prm_ws_hashtable_size_flag = 0;

bool PRM_WS_MEMORY_REPORT = false;
static bool prm_ws_memory_report_default = false;
static unsigned int prm_ws_memory_report_flag = 0;

int PRM_TCP_PORT_ID = 1523;
static int prm_tcp_port_id_default = 1523;
static unsigned int prm_tcp_port_id_flag = 0;

int PRM_TCP_CONNECTION_TIMEOUT = 5;
static int prm_tcp_connection_timeout_default = 5;
static int prm_tcp_connection_timeout_lower = -1;
static unsigned int prm_tcp_connection_timeout_flag = 0;

int PRM_OPTIMIZATION_LEVEL = 1;
static int prm_optimization_level_default = 1;
static unsigned int prm_optimization_level_flag = 0;

bool PRM_QO_DUMP = false;
static bool prm_qo_dump_default = false;
static unsigned int prm_qo_dump_flag = 0;

INT64 PRM_THREAD_STACKSIZE = (1024 * 1024);
static INT64 prm_thread_stacksize_default = (1024 * 1024);
static INT64 prm_thread_stacksize_lower = 64 * 1024;
static INT64 prm_thread_stacksize_upper = INT_MAX;
static unsigned int prm_thread_stacksize_flag = 0;

int PRM_IO_BACKUP_NBUFFERS = 256;
static int prm_io_backup_nbuffers_default = 256;
static int prm_io_backup_nbuffers_lower = 256;
static unsigned int prm_io_backup_nbuffers_flag = 0;

INT64 PRM_IO_BACKUP_MAX_VOLUME_SIZE = 0;
static INT64 prm_io_backup_max_volume_size_default = 0;
static INT64 prm_io_backup_max_volume_size_lower = 1024 * 32;
static INT64 prm_io_backup_max_volume_size_upper = INT_MAX;
static unsigned int prm_io_backup_max_volume_size_flag = 0;

INT64 PRM_IO_BACKUP_SLEEP = 0;
static INT64 prm_io_backup_sleep_default = 0;
static INT64 prm_io_backup_sleep_lower = 0;
static unsigned int prm_io_backup_sleep_flag = 0;

int PRM_MAX_PAGES_IN_TEMP_FILE_CACHE = 1000;
static int prm_max_pages_in_temp_file_cache_default = 1000;	/* pages */
static int prm_max_pages_in_temp_file_cache_lower = 100;
static unsigned int prm_max_pages_in_temp_file_cache_flag = 0;

int PRM_MAX_ENTRIES_IN_TEMP_FILE_CACHE = 512;
static int prm_max_entries_in_temp_file_cache_default = 512;
static int prm_max_entries_in_temp_file_cache_lower = 10;
static unsigned int prm_max_entries_in_temp_file_cache_flag = 0;

INT64 PRM_TEMP_MEM_BUFFER_SIZE = 4 * IO_MAX_PAGE_SIZE;
static INT64 prm_temp_mem_buffer_size_default = 4 * IO_MAX_PAGE_SIZE;
static INT64 prm_temp_mem_buffer_size_lower = 0;
static INT64 prm_temp_mem_buffer_size_upper = 20 * IO_MAX_PAGE_SIZE;
static unsigned int prm_temp_mem_buffer_size_flag = 0;

INT64 PRM_INDEX_SCAN_KEY_BUFFER_SIZE = 20 * IO_MAX_PAGE_SIZE;
static INT64 prm_index_scan_key_buffer_size_default = 20 * IO_MAX_PAGE_SIZE;
static INT64 prm_index_scan_key_buffer_size_lower = 0;
static unsigned int prm_index_scan_key_buffer_size_flag = 0;

bool PRM_ENABLE_HISTO = false;
static bool prm_enable_histo_default = false;
static unsigned int prm_enable_histo_flag = 0;

int PRM_PB_NUM_LRU_CHAINS = 0;
static int prm_pb_num_LRU_chains_default = 0;	/* system define */
static int prm_pb_num_LRU_chains_lower = 0;
static int prm_pb_num_LRU_chains_upper = 1000;
static unsigned int prm_pb_num_LRU_chains_flag = 0;

INT64 PRM_PAGE_BG_FLUSH_INTERVAL = 0;
static INT64 prm_page_bg_flush_interval_default = 0;
static INT64 prm_page_bg_flush_interval_lower = -1;
static unsigned int prm_page_bg_flush_interval_flag = 0;

bool PRM_ADAPTIVE_FLUSH_CONTROL = true;
static bool prm_adaptive_flush_control_default = true;
static unsigned int prm_adaptive_flush_control_flag = 0;

INT64 PRM_MAX_FLUSH_SIZE_PER_SECOND = 10000 * IO_MAX_PAGE_SIZE;
static INT64 prm_max_flush_size_per_second_default = 10000 * IO_MAX_PAGE_SIZE;
static INT64 prm_max_flush_size_per_second_lower = 1 * IO_MAX_PAGE_SIZE;
static INT64 prm_max_flush_size_per_second_upper =
  (INT64) INT_MAX * IO_MAX_PAGE_SIZE;
static unsigned int prm_max_flush_size_per_second_flag = 0;

INT64 PRM_PB_SYNC_ON_FLUSH_SIZE = 200 * IO_MAX_PAGE_SIZE;
static INT64 prm_pb_sync_on_flush_size_default = 200 * IO_MAX_PAGE_SIZE;
static INT64 prm_pb_sync_on_flush_size_lower = 1 * IO_MAX_PAGE_SIZE;
static INT64 prm_pb_sync_on_flush_size_upper =
  (INT64) INT_MAX * IO_MAX_PAGE_SIZE;
static unsigned int prm_pb_sync_on_flush_size_flag = 0;

int PRM_PB_DEBUG_PAGE_VALIDATION_LEVEL = PGBUF_DEBUG_NO_PAGE_VALIDATION;
#if !defined(NDEBUG)
static int prm_pb_debug_page_validation_level_default =
  PGBUF_DEBUG_PAGE_VALIDATION_FETCH;
#else /* !NDEBUG */
static int prm_pb_debug_page_validation_level_default =
  PGBUF_DEBUG_NO_PAGE_VALIDATION;
#endif /* !NDEBUG */
static unsigned int prm_pb_debug_page_validation_level_flag = 0;

bool PRM_ANSI_QUOTES = true;
static bool prm_ansi_quotes_default = true;
static unsigned int prm_ansi_quotes_flag = 0;

bool PRM_TEST_MODE = false;
static bool prm_test_mode_default = false;
static unsigned int prm_test_mode_flag = 0;

INT64 PRM_GROUP_CONCAT_MAX_LEN = 1024;
static INT64 prm_group_concat_max_len_default = 1024;
static INT64 prm_group_concat_max_len_lower = 4;
static INT64 prm_group_concat_max_len_upper = INT_MAX;
static unsigned int prm_group_concat_max_len_flag = 0;

float PRM_LIKE_TERM_SELECTIVITY = 0.1f;
static float prm_like_term_selectivity_default = 0.1f;
static float prm_like_term_selectivity_upper = 1.0f;
static float prm_like_term_selectivity_lower = 0.0f;
static unsigned int prm_like_term_selectivity_flag = 0;

int PRM_SUPPRESS_FSYNC = 0;
static int prm_suppress_fsync_default = 0;
static int prm_suppress_fsync_upper = 100;
static int prm_suppress_fsync_lower = 0;
static unsigned int prm_suppress_fsync_flag = 0;

bool PRM_CALL_STACK_DUMP_ON_ERROR = false;
static bool prm_call_stack_dump_on_error_default = false;
static unsigned int prm_call_stack_dump_on_error_flag = 0;

int *PRM_CALL_STACK_DUMP_ACTIVATION = int_list_initial;
static bool *prm_call_stack_dump_activation_default = NULL;
static unsigned int prm_call_stack_dump_activation_flag = 0;

int *PRM_CALL_STACK_DUMP_DEACTIVATION = int_list_initial;
static bool *prm_call_stack_dump_deactivation_default = NULL;
static unsigned int prm_call_stack_dump_deactivation_flag = 0;

#if 1				/* TODO - test */
bool PRM_MIGRATOR_COMPRESSED_PROTOCOL = true;
static bool prm_migrator_compressed_protocol_default = true;
#else
bool PRM_MIGRATOR_COMPRESSED_PROTOCOL = false;
static bool prm_migrator_compressed_protocol_default = false;
#endif
static unsigned int prm_migrator_compressed_protocol_flag = 0;

bool PRM_AUTO_RESTART_SERVER = true;
static bool prm_auto_restart_server_default = true;
static unsigned int prm_auto_restart_server_flag = 0;

int PRM_XASL_MAX_PLAN_CACHE_ENTRIES = 1000;
static int prm_xasl_max_plan_cache_entries_default = 1000;
static int prm_xasl_max_plan_cache_entries_lower = 1000;
static unsigned int prm_xasl_max_plan_cache_entries_flag = 0;

int PRM_XASL_MAX_PLAN_CACHE_CLONES = 1000;
static int prm_xasl_max_plan_cache_clones_default = 1000;
static unsigned int prm_xasl_max_plan_cache_clones_flag = 0;

int PRM_XASL_PLAN_CACHE_TIMEOUT = -1;
static int prm_xasl_plan_cache_timeout_default = 60 * 60;	/* 1 hour */
static unsigned int prm_xasl_plan_cache_timeout_flag = 0;

bool PRM_USE_ORDERBY_SORT_LIMIT = true;
static bool prm_use_orderby_sort_limit_default = true;
static unsigned int prm_use_orderby_sort_limit_flag = 0;

#if 1				/* TODO - test */
bool PRM_LOGWR_COMPRESSED_PROTOCOL = true;
static bool prm_logwr_compressed_protocol_default = true;
#else
bool PRM_LOGWR_COMPRESSED_PROTOCOL = false;
static bool prm_logwr_compressed_protocol_default = false;
#endif
static unsigned int prm_logwr_compressed_protocol_flag = 0;

int PRM_HA_MODE = HA_MODE_FAIL_BACK;
static int prm_ha_mode_default = HA_MODE_FAIL_BACK;
static int prm_ha_mode_upper = HA_MODE_REPLICA;
static int prm_ha_mode_lower = HA_MODE_FAIL_BACK;
static unsigned int prm_ha_mode_flag = 0;

const char *PRM_HA_NODE_LIST = "";
static const char *prm_ha_node_list_default = NULL;
static unsigned int prm_ha_node_list_flag = 0;

const char *PRM_HA_REPLICA_LIST = "";
static const char *prm_ha_replica_list_default = NULL;
static unsigned int prm_ha_replica_list_flag = 0;

const char *PRM_HA_DB_LIST = "";
static const char *prm_ha_db_list_default = NULL;
static unsigned int prm_ha_db_list_flag = 0;

const char *PRM_HA_COPY_LOG_BASE = "";
static const char *prm_ha_copy_log_base_default = NULL;
static unsigned int prm_ha_copy_log_base_flag = 0;

const char *PRM_HA_COPY_SYNC_MODE = "";
static const char *prm_ha_copy_sync_mode_default = NULL;
static unsigned int prm_ha_copy_sync_mode_flag = 0;

int PRM_HA_APPLY_MAX_MEM_SIZE = HB_DEFAULT_APPLY_MAX_MEM_SIZE;
static int prm_ha_apply_max_mem_size_default = HB_DEFAULT_APPLY_MAX_MEM_SIZE;
static unsigned int prm_ha_apply_max_mem_size_flag = 0;

int PRM_HA_PORT_ID = HB_DEFAULT_HA_PORT_ID;
static int prm_ha_port_id_default = HB_DEFAULT_HA_PORT_ID;
static unsigned int prm_ha_port_id_flag = 0;

INT64 PRM_HA_INIT_TIMER = 10 * ONE_SEC;
static INT64 prm_ha_init_timer_default = 10 * ONE_SEC;
static INT64 prm_ha_init_timer_upper = INT_MAX;
static INT64 prm_ha_init_timer_lower = 0;
static unsigned int prm_ha_init_timer_flag = 0;

INT64 PRM_HA_HEARTBEAT_INTERVAL = 500;
static INT64 prm_ha_heartbeat_interval_default = 500;
static INT64 prm_ha_heartbeat_interval_upper = INT_MAX;
static INT64 prm_ha_heartbeat_interval_lower = 0;
static unsigned int prm_ha_heartbeat_interval_flag = 0;

INT64 PRM_HA_CALC_SCORE_INTERVAL = 3 * ONE_SEC;
static INT64 prm_ha_calc_score_interval_default = 3 * ONE_SEC;
static INT64 prm_ha_calc_score_interval_upper = INT_MAX;
static INT64 prm_ha_calc_score_interval_lower = 0;
static unsigned int prm_ha_calc_score_interval_flag = 0;

INT64 PRM_HA_FAILOVER_WAIT_TIME = 3 * ONE_SEC;
static INT64 prm_ha_failover_wait_time_default = 3 * ONE_SEC;
static INT64 prm_ha_failover_wait_time_upper = INT_MAX;
static INT64 prm_ha_failover_wait_time_lower = 0;
static unsigned int prm_ha_failover_wait_time_flag = 0;

INT64 PRM_HA_PROCESS_START_CONFIRM_INTERVAL = 3 * ONE_SEC;
static INT64 prm_ha_process_start_confirm_interval_default = 3 * ONE_SEC;
static INT64 prm_ha_process_start_confirm_interval_upper = INT_MAX;
static INT64 prm_ha_process_start_confirm_interval_lower = 0;
static unsigned int prm_ha_process_start_confirm_interval_flag = 0;

INT64 PRM_HA_PROCESS_DEREG_CONFIRM_INTERVAL = 500;
static INT64 prm_ha_process_dereg_confirm_interval_default = 500;
static INT64 prm_ha_process_dereg_confirm_interval_upper = INT_MAX;
static INT64 prm_ha_process_dereg_confirm_interval_lower = 0;
static unsigned int prm_ha_process_dereg_confirm_interval_flag = 0;

int PRM_HA_MAX_PROCESS_START_CONFIRM = 20;
static int prm_ha_max_process_start_confirm_default = 20;
static unsigned int prm_ha_max_process_start_confirm_flag = 0;

int PRM_HA_MAX_PROCESS_DEREG_CONFIRM = 120;
static int prm_ha_max_process_dereg_confirm_default = 120;
static unsigned int prm_ha_max_process_dereg_confirm_flag = 0;

INT64 PRM_HA_CHANGESLAVE_MAX_WAIT_TIME = 3 * ONE_SEC;
static INT64 prm_ha_changeslave_max_wait_time_default = 3 * ONE_SEC;
static INT64 prm_ha_changeslave_max_wait_time_upper = INT_MAX;
static INT64 prm_ha_changeslave_max_wait_time_lower = 0;
static unsigned int prm_ha_changeslave_max_wait_time_flag = 0;

INT64 PRM_HA_UNACCEPTABLE_PROC_RESTART_TIMEDIFF = 2 * ONE_MIN;
static INT64 prm_ha_unacceptable_proc_restart_timediff_default = 2 * ONE_MIN;
static unsigned int prm_ha_unacceptable_proc_restart_timediff_flag = 0;

INT64 PRM_SERVER_STATE_SYNC_INTERVAL = 5 * ONE_SEC;
static INT64 prm_server_state_sync_interval_default = 5 * ONE_SEC;
static INT64 prm_server_state_sync_interval_upper = INT_MAX;
static INT64 prm_server_state_sync_interval_lower = 0;
static unsigned int prm_server_state_sync_interval_flag = 0;

int PRM_HA_MAX_HEARTBEAT_GAP = 5;
static int prm_ha_max_heartbeat_gap_default = 5;
static unsigned int prm_ha_max_heartbeat_gap_flag = 0;

const char *PRM_HA_PING_HOSTS = "";
static const char *prm_ha_ping_hosts_default = NULL;
static unsigned int prm_ha_ping_hosts_flag = 0;

int *PRM_HA_IGNORE_ERROR_LIST = int_list_initial;
static int *prm_ha_ignore_error_list_default = NULL;
static unsigned int prm_ha_ignore_error_list_flag = 0;

int PRM_HA_SQL_LOG_MAX_SIZE_IN_MB = INT_MIN;
static int prm_ha_sql_log_max_size_in_mb_default = 50;
static int prm_ha_sql_log_max_size_in_mb_upper = 2048;
static int prm_ha_sql_log_max_size_in_mb_lower = 1;
static unsigned int prm_ha_sql_log_max_size_in_mb_flag = 0;

int PRM_HA_COPY_LOG_MAX_ARCHIVES = 1;
static int prm_ha_copy_log_max_archives_default = 1;
static int prm_ha_copy_log_max_archives_upper = INT_MAX;
static int prm_ha_copy_log_max_archives_lower = 0;
static unsigned int prm_ha_copy_log_max_archives_flag = 0;

INT64 PRM_HA_COPY_LOG_TIMEOUT = 5000;
static INT64 prm_ha_copy_log_timeout_default = 5000;
static INT64 prm_ha_copy_log_timeout_upper = INT_MAX;
static INT64 prm_ha_copy_log_timeout_lower = -1;
static unsigned int prm_ha_copy_log_timeout_flag = 0;

INT64 PRM_HA_REPLICA_DELAY = 0;
static INT64 prm_ha_replica_delay_default = 0;
static INT64 prm_ha_replica_delay_upper = (INT64) INT_MAX * ONE_SEC;
static INT64 prm_ha_replica_delay_lower = 0;
static unsigned int prm_ha_replica_delay_flag = 0;

const char *PRM_HA_REPLICA_TIME_BOUND = "";
static const char *prm_ha_replica_time_bound_default = NULL;
static unsigned int prm_ha_replica_time_bound_flag = 0;

INT64 PRM_HA_DELAY_LIMIT = 0;
static INT64 prm_ha_delay_limit_default = 0;
static INT64 prm_ha_delay_limit_upper = INT_MAX;
static INT64 prm_ha_delay_limit_lower = 0;
static unsigned int prm_ha_delay_limit_flag = 0;

INT64 PRM_HA_DELAY_LIMIT_DELTA = 0;
static INT64 prm_ha_delay_limit_delta_default = 0;
static INT64 prm_ha_delay_limit_delta_upper = INT_MAX;
static INT64 prm_ha_delay_limit_delta_lower = 0;
static unsigned int prm_ha_delay_limit_delta_flag = 0;

INT64 PRM_HA_CHECK_DISK_FAILURE_INTERVAL = 15 * ONE_SEC;
static INT64 prm_ha_check_disk_failure_interval_default = 15 * ONE_SEC;
static INT64 prm_ha_check_disk_failure_interval_upper = INT_MAX;
static INT64 prm_ha_check_disk_failure_interval_lower = 0;
static unsigned int prm_ha_check_disk_failure_interval_flag = 0;

INT64 PRM_LOG_ASYNC_LOG_FLUSH_INTERVAL = 10;
static INT64 prm_log_async_log_flush_interval_default = 10;
static INT64 prm_log_async_log_flush_interval_upper = INT_MAX;
static INT64 prm_log_async_log_flush_interval_lower = 0;
static unsigned int prm_log_async_log_flush_interval_flag = 0;

bool PRM_LOG_COMPRESS = false;
static bool prm_log_compress_default = false;
static unsigned int prm_log_compress_flag = 0;

bool PRM_BLOCK_NOWHERE_STATEMENT = false;
static bool prm_block_nowhere_statement_default = false;
static unsigned int prm_block_nowhere_statement_flag = 0;

bool PRM_BLOCK_DDL_STATEMENT = false;
static bool prm_block_ddl_statement_default = false;
static unsigned int prm_block_ddl_statement_flag = 0;

int PRM_RSQL_HISTORY_NUM = 50;
static int prm_rsql_history_num_default = 50;
static int prm_rsql_history_num_upper = 200;
static int prm_rsql_history_num_lower = 1;
static unsigned int prm_rsql_history_num_flag = 0;

bool PRM_LOG_TRACE_DEBUG = false;
static bool prm_log_trace_debug_default = false;
static unsigned int prm_log_trace_debug_flag = 0;

bool PRM_ER_PRODUCTION_MODE = true;
static bool prm_er_production_mode_default = true;
static unsigned int prm_er_production_mode_flag = 0;

int PRM_ER_STOP_ON_ERROR = 0;
static int prm_er_stop_on_error_default = 0;
static int prm_er_stop_on_error_upper = 0;
static unsigned int prm_er_stop_on_error_flag = 0;

int PRM_TCP_RCVBUF_SIZE = -1;
static int prm_tcp_rcvbuf_size_default = -1;
static unsigned int prm_tcp_rcvbuf_size_flag = 0;

int PRM_TCP_SNDBUF_SIZE = -1;
static int prm_tcp_sndbuf_size_default = -1;
static unsigned int prm_tcp_sndbuf_size_flag = 0;

bool PRM_TCP_NODELAY = false;
static bool prm_tcp_nodelay_default = false;
static unsigned int prm_tcp_nodelay_flag = 0;

bool PRM_TCP_KEEPALIVE = true;
static bool prm_tcp_keepalive_default = true;
static unsigned int prm_tcp_keepalive_flag = 0;

bool PRM_RSQL_SINGLE_LINE_MODE = false;
static bool prm_rsql_single_line_mode_default = false;
static unsigned int prm_rsql_single_line_mode_flag = 0;

bool PRM_XASL_DEBUG_DUMP = false;
static bool prm_xasl_debug_dump_default = false;
static unsigned int prm_xasl_debug_dump_flag = 0;

int PRM_LOG_MAX_ARCHIVES = INT_MAX;
static int prm_log_max_archives_default = INT_MAX;
static int prm_log_max_archives_lower = 0;
static unsigned int prm_log_max_archives_flag = 0;

bool PRM_FORCE_REMOVE_LOG_ARCHIVES = true;
static bool prm_force_remove_log_archives_default = true;
static unsigned int prm_force_remove_log_archives_flag = 0;

int PRM_REMOVE_LOG_ARCHIVES_INTERVAL = 0;
static int prm_remove_log_archives_interval_default = 0;
static int prm_remove_log_archives_interval_lower = 0;
static unsigned int prm_remove_log_archives_interval_flag = 0;

const char *PRM_EVENT_HANDLER = "";
static const char *prm_event_handler_default = NULL;
static unsigned int prm_event_handler_flag = 0;

int *PRM_EVENT_ACTIVATION = int_list_initial;
static int *prm_event_activation_default = NULL;
static unsigned int prm_event_activation_flag = 0;

int PRM_MNT_WAITING_THREAD = 0;
static int prm_mnt_waiting_thread_default = 0;
static int prm_mnt_waiting_thread_lower = 0;
static unsigned int prm_mnt_waiting_thread_flag = 0;

int *PRM_MNT_STATS_THRESHOLD = int_list_initial;
static int *prm_mnt_stats_threshold_default = NULL;
static unsigned int prm_mnt_stats_threshold_flag = 0;

int PRM_SESSION_STATE_TIMEOUT = 60 * 60 * 6;	/* 6 hours */
static int prm_session_timeout_default = 60 * 60 * 6;	/* 6 hours */
static int prm_session_timeout_lower = 60;	/* 1 minute */
static int prm_session_timeout_upper = 60 * 60 * 24 * 365;	/* 1 nonleap year */
static unsigned int prm_session_timeout_flag = 0;

int PRM_MULTI_RANGE_OPT_LIMIT = 100;
static int prm_multi_range_opt_limit_default = 100;
static int prm_multi_range_opt_limit_lower = 0;	/*disabled */
static int prm_multi_range_opt_limit_upper = 10000;
static unsigned int prm_multi_range_opt_limit_flag = 0;

INT64 PRM_DB_VOLUME_SIZE = 512ULL * ONE_M;
static INT64 prm_db_volume_size_default = 10ULL * ONE_G;	/* 10G */
static INT64 prm_db_volume_size_lower = 20ULL * ONE_M;	/* 20M */
static INT64 prm_db_volume_size_upper = 20ULL * ONE_G;	/* 20G */
static unsigned int prm_db_volume_size_flag = 0;

int PRM_CHECK_PEER_ALIVE = CSS_CHECK_PEER_ALIVE_BOTH;
static int prm_check_peer_alive_default = CSS_CHECK_PEER_ALIVE_BOTH;
static unsigned int prm_check_peer_alive_flag = 0;

INT64 PRM_GENERIC_VOL_PREALLOC_SIZE;
static INT64 prm_generic_vol_prealloc_size_default = 50ULL * ONE_M;	/* 50M */
static INT64 prm_generic_vol_prealloc_size_lower = 0ULL;
static INT64 prm_generic_vol_prealloc_size_upper = 20ULL * ONE_G;	/* 20G */
static unsigned int prm_generic_vol_prealloc_size_flag = 0;

INT64 PRM_SQL_TRACE_SLOW = -1;
static INT64 prm_sql_trace_slow_default = -1;
static INT64 prm_sql_trace_slow_lower = -1;
static INT64 prm_sql_trace_slow_upper = 24 * ONE_HOUR;
static unsigned int prm_sql_trace_slow_flag = 0;

bool PRM_SERVER_TRACE = false;
static bool prm_server_trace_default = false;
static unsigned int prm_server_trace_flag = 0;

bool PRM_SQL_TRACE_EXECUTION_PLAN = false;
static bool prm_sql_trace_execution_plan_default = false;
static unsigned int prm_sql_trace_execution_plan_flag = 0;

INT64 PRM_LOG_TRACE_FLUSH_TIME = 0;
static INT64 prm_log_trace_flush_time_default = 0;
static INT64 prm_log_trace_flush_time_lower = 0;
static unsigned int prm_log_trace_flush_time_flag = 0;

int PRM_SORT_LIMIT_MAX_COUNT = 1000;
static int prm_sort_limit_max_count_default = 1000;
static int prm_sort_limit_max_count_lower = 0;	/* disabled */
static int prm_sort_limit_max_count_upper = INT_MAX;
static unsigned int prm_sort_limit_max_count_flag = 0;

int PRM_SQL_TRACE_IOREADS = 0;
static int prm_sql_trace_ioreads_default = 0;
static int prm_sql_trace_ioreads_lower = 0;
static unsigned int prm_sql_trace_ioreads_flag = 0;

bool PRM_QUERY_TRACE = false;
static bool prm_query_trace_default = false;
static unsigned int prm_query_trace_flag = 0;

int PRM_QUERY_TRACE_FORMAT = QUERY_TRACE_TEXT;
static int prm_query_trace_format_default = QUERY_TRACE_TEXT;
static int prm_query_trace_format_lower = QUERY_TRACE_TEXT;
static int prm_query_trace_format_upper = QUERY_TRACE_JSON;
static unsigned int prm_query_trace_format_flag = 0;

int PRM_MAX_RECURSION_SQL_DEPTH = 400;
static int prm_max_recursion_sql_depth_default = 400;
static unsigned int prm_max_recursion_sql_depth_flag = 0;

bool PRM_BTREE_MERGE_ENABLED = true;
static bool prm_btree_merge_enabled_default = true;
static unsigned int prm_btree_merge_enabled_flag = 0;

/* For shard debugging */
bool PRM_SHARD_GROUPID_DEBUG = false;
static bool prm_shard_groupid_debug_default = false;
static unsigned int prm_shard_groupid_debug_flag = 0;

bool PRM_SHARD_GROUPID_01 = false;
static bool prm_shard_groupid_01_default = false;
static unsigned int prm_shard_groupid_01_flag = 0;

bool PRM_SHARD_GROUPID_02 = false;
static bool prm_shard_groupid_02_default = false;
static unsigned int prm_shard_groupid_02_flag = 0;

bool PRM_SHARD_GROUPID_03 = false;
static bool prm_shard_groupid_03_default = false;
static unsigned int prm_shard_groupid_03_flag = 0;

bool PRM_SHARD_GROUPID_04 = false;
static bool prm_shard_groupid_04_default = false;
static unsigned int prm_shard_groupid_04_flag = 0;

bool PRM_SHARD_GROUPID_05 = false;
static bool prm_shard_groupid_05_default = false;
static unsigned int prm_shard_groupid_05_flag = 0;

bool PRM_SHARD_GROUPID_06 = false;
static bool prm_shard_groupid_06_default = false;
static unsigned int prm_shard_groupid_06_flag = 0;

bool PRM_SHARD_GROUPID_07 = false;
static bool prm_shard_groupid_07_default = false;
static unsigned int prm_shard_groupid_07_flag = 0;

bool PRM_SHARD_GROUPID_08 = false;
static bool prm_shard_groupid_08_default = false;
static unsigned int prm_shard_groupid_08_flag = 0;

bool PRM_SHARD_GROUPID_09 = false;
static bool prm_shard_groupid_09_default = false;
static unsigned int prm_shard_groupid_09_flag = 0;

bool PRM_SHARD_GROUPID_10 = false;
static bool prm_shard_groupid_10_default = false;
static unsigned int prm_shard_groupid_10_flag = 0;

bool PRM_OPTIMIZER_RESERVE_01 = false;
static bool prm_optimizer_reserve_01_default = false;
static unsigned int prm_optimizer_reserve_01_flag = 0;

bool PRM_OPTIMIZER_RESERVE_02 = false;
static bool prm_optimizer_reserve_02_default = false;
static unsigned int prm_optimizer_reserve_02_flag = 0;

bool PRM_OPTIMIZER_RESERVE_03 = false;
static bool prm_optimizer_reserve_03_default = false;
static unsigned int prm_optimizer_reserve_03_flag = 0;

bool PRM_OPTIMIZER_RESERVE_04 = false;
static bool prm_optimizer_reserve_04_default = false;
static unsigned int prm_optimizer_reserve_04_flag = 0;

bool PRM_OPTIMIZER_RESERVE_05 = false;
static bool prm_optimizer_reserve_05_default = false;
static unsigned int prm_optimizer_reserve_05_flag = 0;

bool PRM_OPTIMIZER_RESERVE_06 = false;
static bool prm_optimizer_reserve_06_default = false;
static unsigned int prm_optimizer_reserve_06_flag = 0;

bool PRM_OPTIMIZER_RESERVE_07 = false;
static bool prm_optimizer_reserve_07_default = false;
static unsigned int prm_optimizer_reserve_07_flag = 0;

bool PRM_OPTIMIZER_RESERVE_08 = false;
static bool prm_optimizer_reserve_08_default = false;
static unsigned int prm_optimizer_reserve_08_flag = 0;

bool PRM_OPTIMIZER_RESERVE_09 = false;
static bool prm_optimizer_reserve_09_default = false;
static unsigned int prm_optimizer_reserve_09_flag = 0;

bool PRM_OPTIMIZER_RESERVE_10 = false;
static bool prm_optimizer_reserve_10_default = false;
static unsigned int prm_optimizer_reserve_10_flag = 0;

bool PRM_OPTIMIZER_RESERVE_11 = false;
static bool prm_optimizer_reserve_11_default = false;
static unsigned int prm_optimizer_reserve_11_flag = 0;

bool PRM_OPTIMIZER_RESERVE_12 = false;
static bool prm_optimizer_reserve_12_default = false;
static unsigned int prm_optimizer_reserve_12_flag = 0;

bool PRM_OPTIMIZER_RESERVE_13 = false;
static bool prm_optimizer_reserve_13_default = false;
static unsigned int prm_optimizer_reserve_13_flag = 0;

bool PRM_OPTIMIZER_RESERVE_14 = false;
static bool prm_optimizer_reserve_14_default = false;
static unsigned int prm_optimizer_reserve_14_flag = 0;

bool PRM_OPTIMIZER_RESERVE_15 = false;
static bool prm_optimizer_reserve_15_default = false;
static unsigned int prm_optimizer_reserve_15_flag = 0;

bool PRM_OPTIMIZER_RESERVE_16 = false;
static bool prm_optimizer_reserve_16_default = false;
static unsigned int prm_optimizer_reserve_16_flag = 0;

bool PRM_OPTIMIZER_RESERVE_17 = false;
static bool prm_optimizer_reserve_17_default = false;
static unsigned int prm_optimizer_reserve_17_flag = 0;

bool PRM_OPTIMIZER_RESERVE_18 = false;
static bool prm_optimizer_reserve_18_default = false;
static unsigned int prm_optimizer_reserve_18_flag = 0;

bool PRM_OPTIMIZER_RESERVE_19 = false;
static bool prm_optimizer_reserve_19_default = false;
static unsigned int prm_optimizer_reserve_19_flag = 0;

bool PRM_OPTIMIZER_RESERVE_20 = false;
static bool prm_optimizer_reserve_20_default = false;
static unsigned int prm_optimizer_reserve_20_flag = 0;

bool PRM_OPTIMIZER_ENABLE_AGGREGATE_OPTIMIZATION = true;
static bool prm_optimizer_enable_aggregate_optimization_default = true;
static unsigned int prm_optimizer_enable_aggregate_optimization_flag = 0;

int PRM_HA_REPL_FILTER_TYPE = REPL_FILTER_NONE;
static int prm_ha_repl_filter_type_default = REPL_FILTER_NONE;
static int prm_ha_repl_filter_type_lower = REPL_FILTER_NONE;
static int prm_ha_repl_filter_type_upper = REPL_FILTER_EXCLUDE_TBL;
static unsigned int prm_ha_repl_filter_type_flag = 0;

const char *PRM_HA_REPL_FILTER_FILE = "";
static const char *prm_ha_repl_filter_file_default = "";
static unsigned int prm_ha_repl_filter_file_flag = 0;


const char *PRM_FAULT_INJECTION = "none";
static int prm_fault_injection_flag = 0;
static const char *prm_fault_injection_default = "none";

const char *PRM_RYE_SHM_KEY = "";
static int prm_rye_shm_key_flag = 0;
static const char *prm_rye_shm_key_default = "";

int PRM_HA_MAX_LOG_APPLIER = 0;
static int prm_ha_max_log_applier_flag = 0;
static int prm_ha_max_log_applier_default = 8;
static int prm_ha_max_log_applier_lower = HA_MAX_LOG_APPLIER_LOWER;
static int prm_ha_max_log_applier_upper = HA_MAX_LOG_APPLIER_UPPER;

int PRM_MAX_CLIENTS = 10000;
static int prm_max_clients_default = 100;
static int prm_max_clients_lower = 10;
static int prm_max_clients_upper = 10000;
static unsigned int prm_max_clients_flag = 0;

int PRM_MAX_COPYLOG_CONNECTIONS = 16;
static int prm_max_copylog_conns_default = 16;
static int prm_max_copylog_conns_lower = 2;
static int prm_max_copylog_conns_upper = 64;
static unsigned int prm_max_copylog_conns_flag = 0;

int PRM_MIGRATOR_MAX_REPL_DELAY = 3 * 1000;
static int prm_migrator_max_repl_delay_default = 3 * 1000;
static int prm_migrator_max_repl_delay_lower = 0;
static int prm_migrator_max_repl_delay_upper = INT_MAX;
static unsigned int prm_migrator_max_repl_delay_flag = 0;

const char *PRM_HA_NODE_MYSELF = "";
static const char *prm_ha_node_myself_default = "";
static unsigned int prm_ha_node_myself_flag = 0;

typedef struct sysprm_param SYSPRM_PARAM;
struct sysprm_param
{
  PARAM_ID param_id;
  const char *name;		/* the keyword expected */
  unsigned int static_flag;	/* bitmask flag representing status words */
  SYSPRM_DATATYPE datatype;	/* value data type */
  unsigned int *dynamic_flag;	/* shared by both original and duplicated */
  void *default_value;		/* address of (pointer to) default value */
  void *value;			/* address of (pointer to) current value */
  void *upper_limit;		/* highest allowable value */
  void *lower_limit;		/* lowest allowable value */
  char *force_value;		/* address of (pointer to) force value string */
};

#define NUM_PRM (PRM_LAST_ID + 1)

static bool prm_Def_is_initialized = false;
static SYSPRM_PARAM prm_Def[NUM_PRM];


#define PARAM_MSG_FMT(msgid) msgcat_message (MSGCAT_CATALOG_RYE, MSGCAT_SET_PARAMETERS, (msgid))

#define GET_PRM(id) (&prm_Def[(id)])
#define GET_PRM_STATIC_FLAG(id) ((GET_PRM (id))->static_flag)
#define GET_PRM_DYNAMIC_FLAG(id) ((GET_PRM (id))->dynamic_flag)
#define GET_PRM_DATATYPE(id) ((GET_PRM (id))->datatype)

/*
 * Keyword searches do a intl_mbs_ncasecmp(), using the LENGTH OF THE TABLE KEY
 * as the limit, so make sure that overlapping keywords are ordered
 * correctly.  For example, make sure that "yes" precedes "y".
 */

typedef struct keyval KEYVAL;
struct keyval
{
  const char *key;
  int val;
};

static KEYVAL boolean_words[] = {
  {"yes", 1},
  {"y", 1},
  {"1", 1},
  {"true", 1},
  {"on", 1},
  {"no", 0},
  {"n", 0},
  {"0", 0},
  {"false", 0},
  {"off", 0}
};

static KEYVAL er_log_level_words[] = {
  {"fatal", ER_FATAL_ERROR_SEVERITY},
  {"error", ER_ERROR_SEVERITY},
  {"syntax", ER_SYNTAX_ERROR_SEVERITY},
  {"warning", ER_WARNING_SEVERITY},
  {"notification", ER_NOTIFICATION_SEVERITY}
};

static KEYVAL pgbuf_debug_page_validation_level_words[] = {
  {"fetch", PGBUF_DEBUG_PAGE_VALIDATION_FETCH},
  {"free", PGBUF_DEBUG_PAGE_VALIDATION_FREE},
  {"all", PGBUF_DEBUG_PAGE_VALIDATION_ALL}
};

static KEYVAL null_words[] = {
  {"null", 0},
  {"0", 0}
};

static KEYVAL ha_mode_words[] = {
  {HA_MODE_OFF_STR, HA_MODE_OFF},
  {"no", HA_MODE_OFF},
  {"n", HA_MODE_OFF},
  {"0", HA_MODE_OFF},
  {"false", HA_MODE_OFF},
  {"off", HA_MODE_OFF},
  {"yes", HA_MODE_FAIL_BACK},
  {"y", HA_MODE_FAIL_BACK},
  {"1", HA_MODE_FAIL_BACK},
  {"true", HA_MODE_FAIL_BACK},
  {"on", HA_MODE_FAIL_BACK},
  /*{HA_MODE_FAIL_OVER_STR, HA_MODE_FAIL_OVER}, *//* unused */
  {HA_MODE_FAIL_BACK_STR, HA_MODE_FAIL_BACK},
  /*{HA_MODE_LAZY_BACK_STR, HA_MODE_LAZY_BACK}, *//* not implemented yet */
  {HA_MODE_ROLE_CHANGE_STR, HA_MODE_ROLE_CHANGE},
  {"r", HA_MODE_REPLICA},
  {"repl", HA_MODE_REPLICA},
  {"replica", HA_MODE_REPLICA},
  {"2", HA_MODE_REPLICA}
};

static KEYVAL check_peer_alive_words[] = {
  {"none", CSS_CHECK_PEER_ALIVE_NONE},
  {"server_only", CSS_CHECK_PEER_ALIVE_SERVER_ONLY},
  {"client_only", CSS_CHECK_PEER_ALIVE_CLIENT_ONLY},
  {"both", CSS_CHECK_PEER_ALIVE_BOTH},
};

static KEYVAL query_trace_format_words[] = {
  {"text", QUERY_TRACE_TEXT},
  {"json", QUERY_TRACE_JSON},
};

static KEYVAL ha_repl_filter_type_words[] = {
  {"none", REPL_FILTER_NONE},
  {"include_table", REPL_FILTER_INCLUDE_TBL},
  {"exclude_table", REPL_FILTER_EXCLUDE_TBL}
};

static const int call_stack_dump_error_codes[] = {
  ER_GENERIC_ERROR,
  ER_IO_FORMAT_BAD_NPAGES,
  ER_IO_READ,
  ER_IO_WRITE,
  ER_PB_BAD_PAGEID,
  ER_PB_UNFIXED_PAGEPTR,
  ER_DISK_UNKNOWN_SECTOR,
  ER_DISK_UNKNOWN_PAGE,
  ER_SP_BAD_INSERTION_SLOT,
  ER_SP_UNKNOWN_SLOTID,
  ER_HEAP_UNKNOWN_OBJECT,
  ER_HEAP_BAD_RELOCATION_RECORD,
  ER_HEAP_BAD_OBJECT_TYPE,
  ER_HEAP_OVFADDRESS_CORRUPTED,
  ER_LK_PAGE_TIMEOUT,
  ER_LOG_READ,
  ER_LOG_WRITE,
  ER_LOG_PAGE_CORRUPTED,
  ER_LOG_REDO_INTERFACE,
  ER_LOG_MAYNEED_MEDIA_RECOVERY,
  ER_LOG_NOTIN_ARCHIVE,
  ER_TF_BUFFER_UNDERFLOW,
  ER_TF_BUFFER_OVERFLOW,
  ER_BTREE_UNKNOWN_KEY,
  ER_CT_UNKNOWN_CLASSID,
  ER_CT_INVALID_CLASSID,
  ER_CT_UNKNOWN_REPRID,
  ER_CT_INVALID_REPRID,
  ER_FILE_ALLOC_NOPAGES,
  ER_FILE_TABLE_CORRUPTED,
  ER_PAGE_LATCH_TIMEDOUT,
  ER_PAGE_LATCH_ABORTED,
  ER_FILE_TABLE_OVERFLOW,
  ER_HA_GENERIC_ERROR,
  ER_DESC_ISCAN_ABORTED,
  ER_SP_INVALID_HEADER,
  ER_LOG_CHECKPOINT_SKIP_INVALID_PAGE
};

static const int ha_ignore_error_codes[] = {
  ER_SM_CONSTRAINT_NOT_FOUND,
  ER_BTREE_UNIQUE_FAILED
};

typedef enum
{
  PRM_PRINT_NONE = 0,
  PRM_PRINT_NAME,
  PRM_PRINT_ID
} PRM_PRINT_MODE;

typedef enum
{
  PRM_PRINT_CURR_VAL = 0,
  PRM_PRINT_DEFAULT_VAL
} PRM_PRINT_VALUE_MODE;

static int prm_print_value (const SYSPRM_PARAM * prm, void *prm_value,
			    char *buf, size_t len);
static int prm_print (const SYSPRM_PARAM * prm, char *buf, size_t len,
		      PRM_PRINT_MODE print_mode,
		      PRM_PRINT_VALUE_MODE print_value_mode);
static int prm_print_int_value (char *buf, size_t len, int val,
				const SYSPRM_PARAM * prm);
static int prm_print_bigint_value (char *buf, size_t len, INT64 val,
				   const SYSPRM_PARAM * prm);
static int prm_print_boolean_value (char *buf, size_t len, bool val,
				    const SYSPRM_PARAM * prm);
static int prm_print_float_value (char *buf, size_t len, float val,
				  const SYSPRM_PARAM * prm);
static int prm_print_keyword_value (char *buf, size_t len, int val,
				    const SYSPRM_PARAM * prm);
static int prm_print_int_list_value (char *buf, size_t len, int *val,
				     const SYSPRM_PARAM * prm);
static int sysprm_load_and_init_internal (const char *db_name, bool reload);
static void prm_check_environment (void);
static void prm_report_bad_entry (const char *key, int line, int err,
				  const char *where);
static int prm_check_range (SYSPRM_PARAM * prm, void *value);
static int prm_set (SYSPRM_PARAM * prm, const char *value, bool set_flag);
static int prm_set_force (SYSPRM_PARAM * prm, const char *value);
static int prm_set_default (SYSPRM_PARAM * prm);
static SYSPRM_PARAM *prm_find (const char *pname, const char *section);
static const KEYVAL *prm_keyword (int val, const char *name,
				  const KEYVAL * tbl, int dim);
static int prm_tune_hostname_list (SYSPRM_PARAM * list_prm, PARAM_ID prm_id);
static int prm_tune_parameters (void);

#if !defined (SERVER_MODE)
static int prm_get_next_param_value (char **data, char **prm, char **value);
#endif
static int sysprm_compare_values (void *first_value, void *second_value,
				  unsigned int val_type);
static void sysprm_set_sysprm_value_from_parameter (SYSPRM_VALUE * prm_value,
						    SYSPRM_PARAM * prm);
static SYSPRM_ERR sysprm_generate_new_value (SYSPRM_PARAM * prm,
					     const char *value, bool check,
					     SYSPRM_VALUE * new_value);
static int sysprm_set_value (SYSPRM_PARAM * prm, SYSPRM_VALUE value,
			     bool set_flag, bool duplicate);
static void sysprm_set_system_parameter_value (SYSPRM_PARAM * prm,
					       SYSPRM_VALUE value);
static int sysprm_print_sysprm_value (PARAM_ID prm_id, SYSPRM_VALUE value,
				      char *buf, size_t len,
				      PRM_PRINT_MODE print_mode);

static void sysprm_clear_sysprm_value (SYSPRM_VALUE * value,
				       SYSPRM_DATATYPE datatype);
static char *sysprm_pack_sysprm_value (char *ptr, SYSPRM_VALUE value,
				       SYSPRM_DATATYPE datatype);
static int sysprm_packed_sysprm_value_length (SYSPRM_VALUE value,
					      SYSPRM_DATATYPE datatype,
					      int offset);
static char *sysprm_unpack_sysprm_value (char *ptr, SYSPRM_VALUE * value,
					 SYSPRM_DATATYPE datatype);
static void sysprm_initialize_prm_def ();

/*
 * sysprm_dump_parameters - Print out current system parameters
 *   return: none
 *   fp(in):
 */
static void
sysprm_initialize_prm_def ()
{
  SYSPRM_PARAM *prm;

  prm = &prm_Def[PRM_ID_ER_LOG_DEBUG];
  prm->param_id = PRM_ID_ER_LOG_DEBUG;
  prm->name = PRM_NAME_ER_LOG_DEBUG;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_SERVER | PRM_USER_CHANGE
		      | PRM_HIDDEN);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_er_log_debug_flag;
  prm->default_value = (void *) &prm_er_log_debug_default;
  prm->value = (void *) &PRM_ER_LOG_DEBUG;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_ER_LOG_LEVEL];
  prm->param_id = PRM_ID_ER_LOG_LEVEL;
  prm->name = PRM_NAME_ER_LOG_LEVEL;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_SERVER | PRM_USER_CHANGE);
  prm->datatype = PRM_KEYWORD;
  prm->dynamic_flag = &prm_er_log_level_flag;
  prm->default_value = (void *) &prm_er_log_level_default;
  prm->value = (void *) &PRM_ER_LOG_LEVEL;
  prm->upper_limit = (void *) &prm_er_log_level_upper;
  prm->lower_limit = (void *) &prm_er_log_level_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_ER_LOG_WARNING];
  prm->param_id = PRM_ID_ER_LOG_WARNING;
  prm->name = PRM_NAME_ER_LOG_WARNING;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_SERVER | PRM_USER_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_er_log_warning_flag;
  prm->default_value = (void *) &prm_er_log_warning_default;
  prm->value = (void *) &PRM_ER_LOG_WARNING;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_ER_EXIT_ASK];
  prm->param_id = PRM_ID_ER_EXIT_ASK;
  prm->name = PRM_NAME_ER_EXIT_ASK;
  prm->static_flag =
    (PRM_FOR_CLIENT | PRM_FOR_SERVER | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_er_exit_ask_flag;
  prm->default_value = (void *) &prm_er_exit_ask_default;
  prm->value = (void *) &PRM_ER_EXIT_ASK;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_ER_LOG_SIZE];
  prm->param_id = PRM_ID_ER_LOG_SIZE;
  prm->name = PRM_NAME_ER_LOG_SIZE;
  prm->static_flag =
    (PRM_FOR_CLIENT | PRM_FOR_SERVER | PRM_USER_CHANGE | PRM_SIZE_UNIT);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_er_log_size_flag;
  prm->default_value = (void *) &prm_er_log_size_default;
  prm->value = (void *) &PRM_ER_LOG_SIZE;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) &prm_er_log_size_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_ER_LOG_FILE];
  prm->param_id = PRM_ID_ER_LOG_FILE;
  prm->name = PRM_NAME_ER_LOG_FILE;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_SERVER);
  prm->datatype = PRM_STRING;
  prm->dynamic_flag = &prm_er_log_file_flag;
  prm->default_value = (void *) &prm_er_log_file_default;
  prm->value = (void *) &PRM_ER_LOG_FILE;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_ACCESS_IP_CONTROL];
  prm->param_id = PRM_ID_ACCESS_IP_CONTROL;
  prm->name = PRM_NAME_ACCESS_IP_CONTROL;
  prm->static_flag = (PRM_FOR_SERVER | PRM_USER_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_access_ip_control_flag;
  prm->default_value = (void *) &prm_access_ip_control_default;
  prm->value = (void *) &PRM_ACCESS_IP_CONTROL;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_ACCESS_IP_CONTROL_FILE];
  prm->param_id = PRM_ID_ACCESS_IP_CONTROL_FILE;
  prm->name = PRM_NAME_ACCESS_IP_CONTROL_FILE;
  prm->static_flag = (PRM_FOR_SERVER | PRM_USER_CHANGE);
  prm->datatype = PRM_STRING;
  prm->dynamic_flag = &prm_access_ip_control_file_flag;
  prm->default_value = (void *) &prm_access_ip_control_file_default;
  prm->value = (void *) &PRM_ACCESS_IP_CONTROL_FILE;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_IO_LOCKF_ENABLE];
  prm->param_id = PRM_ID_IO_LOCKF_ENABLE;
  prm->name = PRM_NAME_IO_LOCKF_ENABLE;
  prm->static_flag = (PRM_FOR_SERVER | PRM_FOR_CLIENT | PRM_HIDDEN);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_io_lockf_enable_flag;
  prm->default_value = (void *) &prm_io_lockf_enable_default;
  prm->value = (void *) &PRM_IO_LOCKF_ENABLE;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_SORT_BUFFER_SIZE];
  prm->param_id = PRM_ID_SORT_BUFFER_SIZE;
  prm->name = PRM_NAME_SORT_BUFFER_SIZE;
  prm->static_flag =
    (PRM_FOR_SERVER | PRM_USER_CHANGE | PRM_SIZE_UNIT | PRM_RELOADABLE);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_sort_buffer_size_flag;
  prm->default_value = (void *) &prm_sort_buffer_size_default;
  prm->value = (void *) &PRM_SORT_BUFFER_SIZE;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) &prm_sort_buffer_size_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_PB_BUFFER_FLUSH_RATIO];
  prm->param_id = PRM_ID_PB_BUFFER_FLUSH_RATIO;
  prm->name = PRM_NAME_PB_BUFFER_FLUSH_RATIO;
  prm->static_flag = (PRM_FOR_SERVER | PRM_HIDDEN | PRM_USER_CHANGE);
  prm->datatype = PRM_FLOAT;
  prm->dynamic_flag = &prm_pb_buffer_flush_ratio_flag;
  prm->default_value = (void *) &prm_pb_buffer_flush_ratio_default;
  prm->value = (void *) &PRM_PB_BUFFER_FLUSH_RATIO;
  prm->upper_limit = (void *) &prm_pb_buffer_flush_ratio_upper;
  prm->lower_limit = (void *) &prm_pb_buffer_flush_ratio_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_PAGE_BUFFER_SIZE];
  prm->param_id = PRM_ID_PAGE_BUFFER_SIZE;
  prm->name = PRM_NAME_PAGE_BUFFER_SIZE;
  prm->static_flag = (PRM_FOR_SERVER | PRM_SIZE_UNIT | PRM_RELOADABLE);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_page_buffer_size_flag;
  prm->default_value = (void *) &prm_page_buffer_size_default;
  prm->value = (void *) &PRM_PAGE_BUFFER_SIZE;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) &prm_page_buffer_size_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HF_MAX_BESTSPACE_ENTRIES];
  prm->param_id = PRM_ID_HF_MAX_BESTSPACE_ENTRIES;
  prm->name = PRM_NAME_HF_MAX_BESTSPACE_ENTRIES;
  prm->static_flag = (PRM_FOR_SERVER | PRM_HIDDEN | PRM_USER_CHANGE);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_hf_max_bestspace_entries_flag;
  prm->default_value = (void *) &prm_hf_max_bestspace_entries_default;
  prm->value = (void *) &PRM_HF_MAX_BESTSPACE_ENTRIES;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) &prm_hf_max_bestspace_entries_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_BT_UNFILL_FACTOR];
  prm->param_id = PRM_ID_BT_UNFILL_FACTOR;
  prm->name = PRM_NAME_BT_UNFILL_FACTOR;
  prm->static_flag = (PRM_FOR_SERVER | PRM_USER_CHANGE);
  prm->datatype = PRM_FLOAT;
  prm->dynamic_flag = &prm_bt_unfill_factor_flag;
  prm->default_value = (void *) &prm_bt_unfill_factor_default;
  prm->value = (void *) &PRM_BT_UNFILL_FACTOR;
  prm->upper_limit = (void *) &prm_bt_unfill_factor_upper;
  prm->lower_limit = (void *) &prm_bt_unfill_factor_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_BT_OID_BUFFER_SIZE];
  prm->param_id = PRM_ID_BT_OID_BUFFER_SIZE;
  prm->name = PRM_NAME_BT_OID_BUFFER_SIZE;
  prm->static_flag =
    (PRM_FOR_SERVER | PRM_USER_CHANGE | PRM_SIZE_UNIT | PRM_RELOADABLE);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_bt_oid_buffer_size_flag;
  prm->default_value = (void *) &prm_bt_oid_buffer_size_default;
  prm->value = (void *) &PRM_BT_OID_BUFFER_SIZE;
  prm->upper_limit = (void *) &prm_bt_oid_buffer_size_upper;
  prm->lower_limit = (void *) &prm_bt_oid_buffer_size_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_BT_INDEX_SCAN_OID_ORDER];
  prm->param_id = PRM_ID_BT_INDEX_SCAN_OID_ORDER;
  prm->name = PRM_NAME_BT_INDEX_SCAN_OID_ORDER;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_bt_index_scan_oid_order_flag;
  prm->default_value = (void *) &prm_bt_index_scan_oid_order_default;
  prm->value = (void *) &PRM_BT_INDEX_SCAN_OID_ORDER;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_BOSR_MAXTMP_SIZE];
  prm->param_id = PRM_ID_BOSR_MAXTMP_SIZE;
  prm->name = PRM_NAME_BOSR_MAXTMP_SIZE;
  prm->static_flag = (PRM_FOR_SERVER | PRM_SIZE_UNIT);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_bosr_maxtmp_flag;
  prm->default_value = (void *) &prm_bosr_maxtmp_size_default;
  prm->value = (void *) &PRM_BOSR_MAXTMP_SIZE;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_LK_TIMEOUT];
  prm->param_id = PRM_ID_LK_TIMEOUT;
  prm->name = PRM_NAME_LK_TIMEOUT;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_TIME_UNIT);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_lk_timeout_flag;
  prm->default_value = (void *) &prm_lk_timeout_default;
  prm->value = (void *) &PRM_LK_TIMEOUT;
  prm->upper_limit = (void *) &prm_lk_timeout_upper;
  prm->lower_limit = (void *) &prm_lk_timeout_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_LK_RUN_DEADLOCK_INTERVAL];
  prm->param_id = PRM_ID_LK_RUN_DEADLOCK_INTERVAL;
  prm->name = PRM_NAME_LK_RUN_DEADLOCK_INTERVAL;
  prm->static_flag = (PRM_FOR_SERVER | PRM_USER_CHANGE);
  prm->datatype = PRM_FLOAT;
  prm->dynamic_flag = &prm_lk_run_deadlock_interval_flag;
  prm->default_value = (void *) &prm_lk_run_deadlock_interval_default;
  prm->value = (void *) &PRM_LK_RUN_DEADLOCK_INTERVAL;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) &prm_lk_run_deadlock_interval_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_LOG_BUFFER_SIZE];
  prm->param_id = PRM_ID_LOG_BUFFER_SIZE;
  prm->name = PRM_NAME_LOG_BUFFER_SIZE;
  prm->static_flag = (PRM_FOR_SERVER | PRM_SIZE_UNIT | PRM_RELOADABLE);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_log_buffer_size_flag;
  prm->default_value = (void *) &prm_log_buffer_size_default;
  prm->value = (void *) &PRM_LOG_BUFFER_SIZE;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) &prm_log_buffer_size_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_LOG_CHECKPOINT_SIZE];
  prm->param_id = PRM_ID_LOG_CHECKPOINT_SIZE;
  prm->name = PRM_NAME_LOG_CHECKPOINT_SIZE;
  prm->static_flag =
    (PRM_FOR_SERVER | PRM_USER_CHANGE | PRM_SIZE_UNIT | PRM_RELOADABLE);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_log_checkpoint_size_flag;
  prm->default_value = (void *) &prm_log_checkpoint_size_default;
  prm->value = (void *) &PRM_LOG_CHECKPOINT_SIZE;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) &prm_log_checkpoint_size_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_LOG_CHECKPOINT_INTERVAL];
  prm->param_id = PRM_ID_LOG_CHECKPOINT_INTERVAL;
  prm->name = PRM_NAME_LOG_CHECKPOINT_INTERVAL;
  prm->static_flag = (PRM_FOR_SERVER | PRM_USER_CHANGE | PRM_TIME_UNIT);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_log_checkpoint_interval_flag;
  prm->default_value = (void *) &prm_log_checkpoint_interval_default;
  prm->value = (void *) &PRM_LOG_CHECKPOINT_INTERVAL;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) &prm_log_checkpoint_interval_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_LOG_CHECKPOINT_FLUSH_INTERVAL];
  prm->param_id = PRM_ID_LOG_CHECKPOINT_FLUSH_INTERVAL;
  prm->name = PRM_NAME_LOG_CHECKPOINT_FLUSH_INTERVAL;
  prm->static_flag =
    (PRM_FOR_SERVER | PRM_USER_CHANGE | PRM_TIME_UNIT | PRM_HIDDEN);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_log_checkpoint_flush_interval_flag;
  prm->default_value = (void *) &prm_log_checkpoint_flush_interval_default;
  prm->value = (void *) &PRM_LOG_CHECKPOINT_FLUSH_INTERVAL;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) &prm_log_checkpoint_flush_interval_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_COMMIT_ON_SHUTDOWN];
  prm->param_id = PRM_ID_COMMIT_ON_SHUTDOWN;
  prm->name = PRM_NAME_COMMIT_ON_SHUTDOWN;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_commit_on_shutdown_flag;
  prm->default_value = (void *) &prm_commit_on_shutdown_default;
  prm->value = (void *) &PRM_COMMIT_ON_SHUTDOWN;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_SHUTDOWN_WAIT_TIME];
  prm->param_id = PRM_ID_SHUTDOWN_WAIT_TIME;
  prm->name = PRM_NAME_SHUTDOWN_WAIT_TIME;
  prm->static_flag =
    (PRM_FOR_SERVER | PRM_USER_CHANGE | PRM_TIME_UNIT | PRM_HIDDEN);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_shutdown_wait_time_flag;
  prm->default_value = (void *) &prm_shutdown_wait_time_default;
  prm->value = (void *) &PRM_SHUTDOWN_WAIT_TIME;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) &prm_shutdown_wait_time_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_RSQL_AUTO_COMMIT];
  prm->param_id = PRM_ID_RSQL_AUTO_COMMIT;
  prm->name = PRM_NAME_RSQL_AUTO_COMMIT;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_rsql_auto_commit_flag;
  prm->default_value = (void *) &prm_rsql_auto_commit_default;
  prm->value = (void *) &PRM_RSQL_AUTO_COMMIT;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_WS_HASHTABLE_SIZE];
  prm->param_id = PRM_ID_WS_HASHTABLE_SIZE;
  prm->name = PRM_NAME_WS_HASHTABLE_SIZE;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_HIDDEN);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_ws_hashtable_size_flag;
  prm->default_value = (void *) &prm_ws_hashtable_size_default;
  prm->value = (void *) &PRM_WS_HASHTABLE_SIZE;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) &prm_ws_hashtable_size_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_WS_MEMORY_REPORT];
  prm->param_id = PRM_ID_WS_MEMORY_REPORT;
  prm->name = PRM_NAME_WS_MEMORY_REPORT;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_ws_memory_report_flag;
  prm->default_value = (void *) &prm_ws_memory_report_default;
  prm->value = (void *) &PRM_WS_MEMORY_REPORT;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_TCP_PORT_ID];
  prm->param_id = PRM_ID_TCP_PORT_ID;
  prm->name = PRM_NAME_TCP_PORT_ID;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_SERVER);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_tcp_port_id_flag;
  prm->default_value = (void *) &prm_tcp_port_id_default;
  prm->value = (void *) &PRM_TCP_PORT_ID;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_TCP_CONNECTION_TIMEOUT];
  prm->param_id = PRM_ID_TCP_CONNECTION_TIMEOUT;
  prm->name = PRM_NAME_TCP_CONNECTION_TIMEOUT;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_SERVER | PRM_USER_CHANGE);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_tcp_connection_timeout_flag;
  prm->default_value = (void *) &prm_tcp_connection_timeout_default;
  prm->value = (void *) &PRM_TCP_CONNECTION_TIMEOUT;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) &prm_tcp_connection_timeout_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_OPTIMIZATION_LEVEL];
  prm->param_id = PRM_ID_OPTIMIZATION_LEVEL;
  prm->name = PRM_NAME_OPTIMIZATION_LEVEL;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_optimization_level_flag;
  prm->default_value = (void *) &prm_optimization_level_default;
  prm->value = (void *) &PRM_OPTIMIZATION_LEVEL;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_QO_DUMP];
  prm->param_id = PRM_ID_QO_DUMP;
  prm->name = PRM_NAME_QO_DUMP;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_qo_dump_flag;
  prm->default_value = (void *) &prm_qo_dump_default;
  prm->value = (void *) &PRM_QO_DUMP;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_THREAD_STACKSIZE];
  prm->param_id = PRM_ID_THREAD_STACKSIZE;
  prm->name = PRM_NAME_THREAD_STACKSIZE;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_SERVER | PRM_SIZE_UNIT);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_thread_stacksize_flag;
  prm->default_value = (void *) &prm_thread_stacksize_default;
  prm->value = (void *) &PRM_THREAD_STACKSIZE;
  prm->upper_limit = (void *) &prm_thread_stacksize_upper;
  prm->lower_limit = (void *) &prm_thread_stacksize_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_IO_BACKUP_NBUFFERS];
  prm->param_id = PRM_ID_IO_BACKUP_NBUFFERS;
  prm->name = PRM_NAME_IO_BACKUP_NBUFFERS;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_SERVER | PRM_HIDDEN);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_io_backup_nbuffers_flag;
  prm->default_value = (void *) &prm_io_backup_nbuffers_default;
  prm->value = (void *) &PRM_IO_BACKUP_NBUFFERS;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) &prm_io_backup_nbuffers_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_IO_BACKUP_MAX_VOLUME_SIZE];
  prm->param_id = PRM_ID_IO_BACKUP_MAX_VOLUME_SIZE;
  prm->name = PRM_NAME_IO_BACKUP_MAX_VOLUME_SIZE;
  prm->static_flag = (PRM_FOR_SERVER | PRM_SIZE_UNIT);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_io_backup_max_volume_size_flag;
  prm->default_value = (void *) &prm_io_backup_max_volume_size_default;
  prm->value = (void *) &PRM_IO_BACKUP_MAX_VOLUME_SIZE;
  prm->upper_limit = (void *) &prm_io_backup_max_volume_size_upper;
  prm->lower_limit = (void *) &prm_io_backup_max_volume_size_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_IO_BACKUP_SLEEP];
  prm->param_id = PRM_ID_IO_BACKUP_SLEEP;
  prm->name = PRM_NAME_IO_BACKUP_SLEEP;
  prm->static_flag =
    (PRM_FOR_SERVER | PRM_USER_CHANGE | PRM_TIME_UNIT | PRM_HIDDEN);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_io_backup_sleep_flag;
  prm->default_value = (void *) &prm_io_backup_sleep_default;
  prm->value = (void *) &PRM_IO_BACKUP_SLEEP;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) &prm_io_backup_sleep_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_MAX_PAGES_IN_TEMP_FILE_CACHE];
  prm->param_id = PRM_ID_MAX_PAGES_IN_TEMP_FILE_CACHE;
  prm->name = PRM_NAME_MAX_PAGES_IN_TEMP_FILE_CACHE;
  prm->static_flag = (PRM_FOR_SERVER | PRM_HIDDEN);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_max_pages_in_temp_file_cache_flag;
  prm->default_value = (void *) &prm_max_pages_in_temp_file_cache_default;
  prm->value = (void *) &PRM_MAX_PAGES_IN_TEMP_FILE_CACHE;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) &prm_max_pages_in_temp_file_cache_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_MAX_ENTRIES_IN_TEMP_FILE_CACHE];
  prm->param_id = PRM_ID_MAX_ENTRIES_IN_TEMP_FILE_CACHE;
  prm->name = PRM_NAME_MAX_ENTRIES_IN_TEMP_FILE_CACHE;
  prm->static_flag = (PRM_FOR_SERVER | PRM_HIDDEN);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_max_entries_in_temp_file_cache_flag;
  prm->default_value = (void *) &prm_max_entries_in_temp_file_cache_default;
  prm->value = (void *) &PRM_MAX_ENTRIES_IN_TEMP_FILE_CACHE;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) &prm_max_entries_in_temp_file_cache_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_TEMP_MEM_BUFFER_SIZE];
  prm->param_id = PRM_ID_TEMP_MEM_BUFFER_SIZE;
  prm->name = PRM_NAME_TEMP_MEM_BUFFER_SIZE;
  prm->static_flag = (PRM_FOR_SERVER | PRM_USER_CHANGE | PRM_SIZE_UNIT);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_temp_mem_buffer_size_flag;
  prm->default_value = (void *) &prm_temp_mem_buffer_size_default;
  prm->value = (void *) &PRM_TEMP_MEM_BUFFER_SIZE;
  prm->upper_limit = (void *) &prm_temp_mem_buffer_size_upper;
  prm->lower_limit = (void *) &prm_temp_mem_buffer_size_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_INDEX_SCAN_KEY_BUFFER_SIZE];
  prm->param_id = PRM_ID_INDEX_SCAN_KEY_BUFFER_SIZE;
  prm->name = PRM_NAME_INDEX_SCAN_KEY_BUFFER_SIZE;
  prm->static_flag = (PRM_FOR_SERVER | PRM_SIZE_UNIT | PRM_RELOADABLE);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_index_scan_key_buffer_size_flag;
  prm->default_value = (void *) &prm_index_scan_key_buffer_size_default;
  prm->value = (void *) &PRM_INDEX_SCAN_KEY_BUFFER_SIZE;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) &prm_index_scan_key_buffer_size_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_ENABLE_HISTO];
  prm->param_id = PRM_ID_ENABLE_HISTO;
  prm->name = PRM_NAME_ENABLE_HISTO;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_enable_histo_flag;
  prm->default_value = (void *) &prm_enable_histo_default;
  prm->value = (void *) &PRM_ENABLE_HISTO;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_PB_NUM_LRU_CHAINS];
  prm->param_id = PRM_ID_PB_NUM_LRU_CHAINS;
  prm->name = PRM_NAME_PB_NUM_LRU_CHAINS;
  prm->static_flag = (PRM_FOR_SERVER | PRM_HIDDEN);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_pb_num_LRU_chains_flag;
  prm->default_value = (void *) &prm_pb_num_LRU_chains_default;
  prm->value = (void *) &PRM_PB_NUM_LRU_CHAINS;
  prm->upper_limit = (void *) &prm_pb_num_LRU_chains_upper;
  prm->lower_limit = (void *) &prm_pb_num_LRU_chains_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_PAGE_BG_FLUSH_INTERVAL];
  prm->param_id = PRM_ID_PAGE_BG_FLUSH_INTERVAL;
  prm->name = PRM_NAME_PAGE_BG_FLUSH_INTERVAL;
  prm->static_flag = (PRM_FOR_SERVER | PRM_USER_CHANGE | PRM_TIME_UNIT);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_page_bg_flush_interval_flag;
  prm->default_value = (void *) &prm_page_bg_flush_interval_default;
  prm->value = (void *) &PRM_PAGE_BG_FLUSH_INTERVAL;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) &prm_page_bg_flush_interval_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_ADAPTIVE_FLUSH_CONTROL];
  prm->param_id = PRM_ID_ADAPTIVE_FLUSH_CONTROL;
  prm->name = PRM_NAME_ADAPTIVE_FLUSH_CONTROL;
  prm->static_flag = (PRM_FOR_SERVER | PRM_USER_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_adaptive_flush_control_flag;
  prm->default_value = (void *) &prm_adaptive_flush_control_default;
  prm->value = (void *) &PRM_ADAPTIVE_FLUSH_CONTROL;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_MAX_FLUSH_SIZE_PER_SECOND];
  prm->param_id = PRM_ID_MAX_FLUSH_SIZE_PER_SECOND;
  prm->name = PRM_NAME_MAX_FLUSH_SIZE_PER_SECOND;
  prm->static_flag =
    (PRM_FOR_SERVER | PRM_USER_CHANGE | PRM_SIZE_UNIT | PRM_RELOADABLE);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_max_flush_size_per_second_flag;
  prm->default_value = (void *) &prm_max_flush_size_per_second_default;
  prm->value = (void *) &PRM_MAX_FLUSH_SIZE_PER_SECOND;
  prm->upper_limit = (void *) &prm_max_flush_size_per_second_upper;
  prm->lower_limit = (void *) &prm_max_flush_size_per_second_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_PB_SYNC_ON_FLUSH_SIZE];
  prm->param_id = PRM_ID_PB_SYNC_ON_FLUSH_SIZE;
  prm->name = PRM_NAME_PB_SYNC_ON_FLUSH_SIZE;
  prm->static_flag =
    (PRM_FOR_SERVER | PRM_USER_CHANGE | PRM_SIZE_UNIT | PRM_RELOADABLE);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_pb_sync_on_flush_size_flag;
  prm->default_value = (void *) &prm_pb_sync_on_flush_size_default;
  prm->value = (void *) &PRM_PB_SYNC_ON_FLUSH_SIZE;
  prm->upper_limit = (void *) &prm_pb_sync_on_flush_size_upper;
  prm->lower_limit = (void *) &prm_pb_sync_on_flush_size_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_PB_DEBUG_PAGE_VALIDATION_LEVEL];
  prm->param_id = PRM_ID_PB_DEBUG_PAGE_VALIDATION_LEVEL;
  prm->name = PRM_NAME_PB_DEBUG_PAGE_VALIDATION_LEVEL;
  prm->static_flag = (PRM_FOR_SERVER | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_KEYWORD;
  prm->dynamic_flag = &prm_pb_debug_page_validation_level_flag;
  prm->default_value = (void *) &prm_pb_debug_page_validation_level_default;
  prm->value = (void *) &PRM_PB_DEBUG_PAGE_VALIDATION_LEVEL;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_ANSI_QUOTES];
  prm->param_id = PRM_ID_ANSI_QUOTES;
  prm->name = PRM_NAME_ANSI_QUOTES;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_TEST_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_ansi_quotes_flag;
  prm->default_value = (void *) &prm_ansi_quotes_default;
  prm->value = (void *) &PRM_ANSI_QUOTES;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_TEST_MODE];
  prm->param_id = PRM_ID_TEST_MODE;
  prm->name = PRM_NAME_TEST_MODE;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_SERVER | PRM_HIDDEN);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_test_mode_flag;
  prm->default_value = (void *) &prm_test_mode_default;
  prm->value = (void *) &PRM_TEST_MODE;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_GROUP_CONCAT_MAX_LEN];
  prm->param_id = PRM_ID_GROUP_CONCAT_MAX_LEN;
  prm->name = PRM_NAME_GROUP_CONCAT_MAX_LEN;
  prm->static_flag = (PRM_USER_CHANGE | PRM_FOR_SERVER | PRM_SIZE_UNIT);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_group_concat_max_len_flag;
  prm->default_value = (void *) &prm_group_concat_max_len_default;
  prm->value = (void *) &PRM_GROUP_CONCAT_MAX_LEN;
  prm->upper_limit = (void *) &prm_group_concat_max_len_upper;
  prm->lower_limit = (void *) &prm_group_concat_max_len_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_LIKE_TERM_SELECTIVITY];
  prm->param_id = PRM_ID_LIKE_TERM_SELECTIVITY;
  prm->name = PRM_NAME_LIKE_TERM_SELECTIVITY;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_FLOAT;
  prm->dynamic_flag = &prm_like_term_selectivity_flag;
  prm->default_value = (void *) &prm_like_term_selectivity_default;
  prm->value = (void *) &PRM_LIKE_TERM_SELECTIVITY;
  prm->upper_limit = (void *) &prm_like_term_selectivity_upper;
  prm->lower_limit = (void *) &prm_like_term_selectivity_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_SUPPRESS_FSYNC];
  prm->param_id = PRM_ID_SUPPRESS_FSYNC;
  prm->name = PRM_NAME_SUPPRESS_FSYNC;
  prm->static_flag = (PRM_FOR_SERVER | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_suppress_fsync_flag;
  prm->default_value = (void *) &prm_suppress_fsync_default;
  prm->value = (void *) &PRM_SUPPRESS_FSYNC;
  prm->upper_limit = (void *) &prm_suppress_fsync_upper;
  prm->lower_limit = (void *) &prm_suppress_fsync_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_CALL_STACK_DUMP_ON_ERROR];
  prm->param_id = PRM_ID_CALL_STACK_DUMP_ON_ERROR;
  prm->name = PRM_NAME_CALL_STACK_DUMP_ON_ERROR;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_SERVER | PRM_USER_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_call_stack_dump_on_error_flag;
  prm->default_value = (void *) &prm_call_stack_dump_on_error_default;
  prm->value = (void *) &PRM_CALL_STACK_DUMP_ON_ERROR;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_CALL_STACK_DUMP_ACTIVATION];
  prm->param_id = PRM_ID_CALL_STACK_DUMP_ACTIVATION;
  prm->name = PRM_NAME_CALL_STACK_DUMP_ACTIVATION;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_SERVER | PRM_USER_CHANGE);
  prm->datatype = PRM_INTEGER_LIST;
  prm->dynamic_flag = &prm_call_stack_dump_activation_flag;
  prm->default_value = (void *) &prm_call_stack_dump_activation_default;
  prm->value = (void *) &PRM_CALL_STACK_DUMP_ACTIVATION;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_CALL_STACK_DUMP_DEACTIVATION];
  prm->param_id = PRM_ID_CALL_STACK_DUMP_DEACTIVATION;
  prm->name = PRM_NAME_CALL_STACK_DUMP_DEACTIVATION;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_SERVER | PRM_USER_CHANGE);
  prm->datatype = PRM_INTEGER_LIST;
  prm->dynamic_flag = &prm_call_stack_dump_deactivation_flag;
  prm->default_value = (void *) &prm_call_stack_dump_deactivation_default;
  prm->value = (void *) &PRM_CALL_STACK_DUMP_DEACTIVATION;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_MIGRATOR_COMPRESSED_PROTOCOL];
  prm->param_id = PRM_ID_MIGRATOR_COMPRESSED_PROTOCOL;
  prm->name = PRM_NAME_MIGRATOR_COMPRESSED_PROTOCOL;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_migrator_compressed_protocol_flag;
  prm->default_value = (void *) &prm_migrator_compressed_protocol_default;
  prm->value = (void *) &PRM_MIGRATOR_COMPRESSED_PROTOCOL;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_AUTO_RESTART_SERVER];
  prm->param_id = PRM_ID_AUTO_RESTART_SERVER;
  prm->name = PRM_NAME_AUTO_RESTART_SERVER;
  prm->static_flag = (PRM_FOR_SERVER | PRM_USER_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_auto_restart_server_flag;
  prm->default_value = (void *) &prm_auto_restart_server_default;
  prm->value = (void *) &PRM_AUTO_RESTART_SERVER;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_XASL_MAX_PLAN_CACHE_ENTRIES];
  prm->param_id = PRM_ID_XASL_MAX_PLAN_CACHE_ENTRIES;
  prm->name = PRM_NAME_XASL_MAX_PLAN_CACHE_ENTRIES;
  prm->static_flag = (PRM_FOR_SERVER | PRM_FORCE_SERVER);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_xasl_max_plan_cache_entries_flag;
  prm->default_value = (void *) &prm_xasl_max_plan_cache_entries_default;
  prm->value = (void *) &PRM_XASL_MAX_PLAN_CACHE_ENTRIES;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) &prm_xasl_max_plan_cache_entries_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_XASL_MAX_PLAN_CACHE_CLONES];
  prm->param_id = PRM_ID_XASL_MAX_PLAN_CACHE_CLONES;
  prm->name = PRM_NAME_XASL_MAX_PLAN_CACHE_CLONES;
  prm->static_flag = (PRM_FOR_SERVER | PRM_HIDDEN);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_xasl_max_plan_cache_clones_flag;
  prm->default_value = (void *) &prm_xasl_max_plan_cache_clones_default;
  prm->value = (void *) &PRM_XASL_MAX_PLAN_CACHE_CLONES;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_XASL_PLAN_CACHE_TIMEOUT];
  prm->param_id = PRM_ID_XASL_PLAN_CACHE_TIMEOUT;
  prm->name = PRM_NAME_XASL_PLAN_CACHE_TIMEOUT;
  prm->static_flag = (PRM_FOR_SERVER);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_xasl_plan_cache_timeout_flag;
  prm->default_value = (void *) &prm_xasl_plan_cache_timeout_default;
  prm->value = (void *) &PRM_XASL_PLAN_CACHE_TIMEOUT;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_USE_ORDERBY_SORT_LIMIT];
  prm->param_id = PRM_ID_USE_ORDERBY_SORT_LIMIT;
  prm->name = PRM_NAME_USE_ORDERBY_SORT_LIMIT;
  prm->static_flag = (PRM_FOR_SERVER | PRM_USER_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_use_orderby_sort_limit_flag;
  prm->default_value = (void *) &prm_use_orderby_sort_limit_default;
  prm->value = (void *) &PRM_USE_ORDERBY_SORT_LIMIT;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_LOGWR_COMPRESSED_PROTOCOL];
  prm->param_id = PRM_ID_LOGWR_COMPRESSED_PROTOCOL;
  prm->name = PRM_NAME_LOGWR_COMPRESSED_PROTOCOL;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_logwr_compressed_protocol_flag;
  prm->default_value = (void *) &prm_logwr_compressed_protocol_default;
  prm->value = (void *) &PRM_LOGWR_COMPRESSED_PROTOCOL;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_MODE];
  prm->param_id = PRM_ID_HA_MODE;
  prm->name = PRM_NAME_HA_MODE;
  prm->static_flag =
    (PRM_FOR_CLIENT | PRM_FOR_SERVER | PRM_FOR_HA | PRM_FORCE_SERVER);
  prm->datatype = PRM_KEYWORD;
  prm->dynamic_flag = &prm_ha_mode_flag;
  prm->default_value = (void *) &prm_ha_mode_default;
  prm->value = (void *) &PRM_HA_MODE;
  prm->upper_limit = (void *) &prm_ha_mode_upper;
  prm->lower_limit = (void *) &prm_ha_mode_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_NODE_LIST];
  prm->param_id = PRM_ID_HA_NODE_LIST;
  prm->name = PRM_NAME_HA_NODE_LIST;
  prm->static_flag =
    (PRM_FOR_SERVER | PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_RELOADABLE |
     PRM_FOR_HA);
  prm->datatype = PRM_STRING;
  prm->dynamic_flag = &prm_ha_node_list_flag;
  prm->default_value = (void *) &prm_ha_node_list_default;
  prm->value = (void *) &PRM_HA_NODE_LIST;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_REPLICA_LIST];
  prm->param_id = PRM_ID_HA_REPLICA_LIST;
  prm->name = PRM_NAME_HA_REPLICA_LIST;
  prm->static_flag =
    (PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_RELOADABLE | PRM_FOR_HA);
  prm->datatype = PRM_STRING;
  prm->dynamic_flag = &prm_ha_replica_list_flag;
  prm->default_value = (void *) &prm_ha_replica_list_default;
  prm->value = (void *) &PRM_HA_REPLICA_LIST;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_DB_LIST];
  prm->param_id = PRM_ID_HA_DB_LIST;
  prm->name = PRM_NAME_HA_DB_LIST;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_SERVER | PRM_FOR_HA);
  prm->datatype = PRM_STRING;
  prm->dynamic_flag = &prm_ha_db_list_flag;
  prm->default_value = (void *) &prm_ha_db_list_default;
  prm->value = (void *) &PRM_HA_DB_LIST;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_COPY_LOG_BASE];
  prm->param_id = PRM_ID_HA_COPY_LOG_BASE;
  prm->name = PRM_NAME_HA_COPY_LOG_BASE;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_HA);
  prm->datatype = PRM_STRING;
  prm->dynamic_flag = &prm_ha_copy_log_base_flag;
  prm->default_value = (void *) &prm_ha_copy_log_base_default;
  prm->value = (void *) &PRM_HA_COPY_LOG_BASE;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_COPY_SYNC_MODE];
  prm->param_id = PRM_ID_HA_COPY_SYNC_MODE;
  prm->name = PRM_NAME_HA_COPY_SYNC_MODE;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_HA);
  prm->datatype = PRM_STRING;
  prm->dynamic_flag = &prm_ha_copy_sync_mode_flag;
  prm->default_value = (void *) &prm_ha_copy_sync_mode_default;
  prm->value = (void *) &PRM_HA_COPY_SYNC_MODE;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_APPLY_MAX_MEM_SIZE];
  prm->param_id = PRM_ID_HA_APPLY_MAX_MEM_SIZE;
  prm->name = PRM_NAME_HA_APPLY_MAX_MEM_SIZE;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_HA);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_ha_apply_max_mem_size_flag;
  prm->default_value = (void *) &prm_ha_apply_max_mem_size_default;
  prm->value = (void *) &PRM_HA_APPLY_MAX_MEM_SIZE;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_PORT_ID];
  prm->param_id = PRM_ID_HA_PORT_ID;
  prm->name = PRM_NAME_HA_PORT_ID;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_HA);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_ha_port_id_flag;
  prm->default_value = (void *) &prm_ha_port_id_default;
  prm->value = (void *) &PRM_HA_PORT_ID;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_INIT_TIMER];
  prm->param_id = PRM_ID_HA_INIT_TIMER;
  prm->name = PRM_NAME_HA_INIT_TIMER;
  prm->static_flag =
    (PRM_FOR_CLIENT | PRM_HIDDEN | PRM_FOR_HA | PRM_TIME_UNIT);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_ha_init_timer_flag;
  prm->default_value = (void *) &prm_ha_init_timer_default;
  prm->value = (void *) &PRM_HA_INIT_TIMER;
  prm->upper_limit = (void *) &prm_ha_init_timer_upper;
  prm->lower_limit = (void *) &prm_ha_init_timer_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_HEARTBEAT_INTERVAL];
  prm->param_id = PRM_ID_HA_HEARTBEAT_INTERVAL;
  prm->name = PRM_NAME_HA_HEARTBEAT_INTERVAL;
  prm->static_flag =
    (PRM_FOR_CLIENT | PRM_HIDDEN | PRM_FOR_HA | PRM_TIME_UNIT);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_ha_heartbeat_interval_flag;
  prm->default_value = (void *) &prm_ha_heartbeat_interval_default;
  prm->value = (void *) &PRM_HA_HEARTBEAT_INTERVAL;
  prm->upper_limit = (void *) &prm_ha_heartbeat_interval_upper;
  prm->lower_limit = (void *) &prm_ha_heartbeat_interval_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_CALC_SCORE_INTERVAL];
  prm->param_id = PRM_ID_HA_CALC_SCORE_INTERVAL;
  prm->name = PRM_NAME_HA_CALC_SCORE_INTERVAL;
  prm->static_flag =
    (PRM_FOR_CLIENT | PRM_HIDDEN | PRM_FOR_HA | PRM_TIME_UNIT);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_ha_calc_score_interval_flag;
  prm->default_value = (void *) &prm_ha_calc_score_interval_default;
  prm->value = (void *) &PRM_HA_CALC_SCORE_INTERVAL;
  prm->upper_limit = (void *) &prm_ha_calc_score_interval_upper;
  prm->lower_limit = (void *) &prm_ha_calc_score_interval_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_FAILOVER_WAIT_TIME];
  prm->param_id = PRM_ID_HA_FAILOVER_WAIT_TIME;
  prm->name = PRM_NAME_HA_FAILOVER_WAIT_TIME;
  prm->static_flag =
    (PRM_FOR_CLIENT | PRM_HIDDEN | PRM_FOR_HA | PRM_TIME_UNIT);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_ha_failover_wait_time_flag;
  prm->default_value = (void *) &prm_ha_failover_wait_time_default;
  prm->value = (void *) &PRM_HA_FAILOVER_WAIT_TIME;
  prm->upper_limit = (void *) &prm_ha_failover_wait_time_upper;
  prm->lower_limit = (void *) &prm_ha_failover_wait_time_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_PROCESS_START_CONFIRM_INTERVAL];
  prm->param_id = PRM_ID_HA_PROCESS_START_CONFIRM_INTERVAL;
  prm->name = PRM_NAME_HA_PROCESS_START_CONFIRM_INTERVAL;
  prm->static_flag =
    (PRM_FOR_CLIENT | PRM_HIDDEN | PRM_FOR_HA | PRM_TIME_UNIT);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_ha_process_start_confirm_interval_flag;
  prm->default_value =
    (void *) &prm_ha_process_start_confirm_interval_default;
  prm->value = (void *) &PRM_HA_PROCESS_START_CONFIRM_INTERVAL;
  prm->upper_limit = (void *) &prm_ha_process_start_confirm_interval_upper;
  prm->lower_limit = (void *) &prm_ha_process_start_confirm_interval_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_PROCESS_DEREG_CONFIRM_INTERVAL];
  prm->param_id = PRM_ID_HA_PROCESS_DEREG_CONFIRM_INTERVAL;
  prm->name = PRM_NAME_HA_PROCESS_DEREG_CONFIRM_INTERVAL;
  prm->static_flag =
    (PRM_FOR_CLIENT | PRM_HIDDEN | PRM_FOR_HA | PRM_TIME_UNIT);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_ha_process_dereg_confirm_interval_flag;
  prm->default_value =
    (void *) &prm_ha_process_dereg_confirm_interval_default;
  prm->value = (void *) &PRM_HA_PROCESS_DEREG_CONFIRM_INTERVAL;
  prm->upper_limit = (void *) &prm_ha_process_dereg_confirm_interval_upper;
  prm->lower_limit = (void *) &prm_ha_process_dereg_confirm_interval_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_MAX_PROCESS_START_CONFIRM];
  prm->param_id = PRM_ID_HA_MAX_PROCESS_START_CONFIRM;
  prm->name = PRM_NAME_HA_MAX_PROCESS_START_CONFIRM;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_HIDDEN | PRM_FOR_HA);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_ha_max_process_start_confirm_flag;
  prm->default_value = (void *) &prm_ha_max_process_start_confirm_default;
  prm->value = (void *) &PRM_HA_MAX_PROCESS_START_CONFIRM;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_MAX_PROCESS_DEREG_CONFIRM];
  prm->param_id = PRM_ID_HA_MAX_PROCESS_DEREG_CONFIRM;
  prm->name = PRM_NAME_HA_MAX_PROCESS_DEREG_CONFIRM;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_HIDDEN | PRM_FOR_HA);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_ha_max_process_dereg_confirm_flag;
  prm->default_value = (void *) &prm_ha_max_process_dereg_confirm_default;
  prm->value = (void *) &PRM_HA_MAX_PROCESS_DEREG_CONFIRM;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_CHANGESALVE_MAX_WAIT_TIME];
  prm->param_id = PRM_ID_HA_CHANGESALVE_MAX_WAIT_TIME;
  prm->name = PRM_NAME_HA_CHANGESLAVE_MAX_WAIT_TIME;
  prm->static_flag =
    (PRM_FOR_CLIENT | PRM_HIDDEN | PRM_FOR_HA | PRM_TIME_UNIT);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_ha_changeslave_max_wait_time_flag;
  prm->default_value = (void *) &prm_ha_changeslave_max_wait_time_default;
  prm->value = (void *) &PRM_HA_CHANGESLAVE_MAX_WAIT_TIME;
  prm->upper_limit = (void *) &prm_ha_changeslave_max_wait_time_upper;
  prm->lower_limit = (void *) &prm_ha_changeslave_max_wait_time_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_UNACCEPTABLE_PROC_RESTART_TIMEDIFF];
  prm->param_id = PRM_ID_HA_UNACCEPTABLE_PROC_RESTART_TIMEDIFF;
  prm->name = PRM_NAME_HA_UNACCEPTABLE_PROC_RESTART_TIMEDIFF;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_HA | PRM_TIME_UNIT);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_ha_unacceptable_proc_restart_timediff_flag;
  prm->default_value =
    (void *) &prm_ha_unacceptable_proc_restart_timediff_default;
  prm->value = (void *) &PRM_HA_UNACCEPTABLE_PROC_RESTART_TIMEDIFF;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_SERVER_STATE_SYNC_INTERVAL];
  prm->param_id = PRM_ID_SERVER_STATE_SYNC_INTERVAL;
  prm->name = PRM_NAME_SERVER_STATE_SYNC_INTERVAL;
  prm->static_flag =
    (PRM_FOR_CLIENT | PRM_HIDDEN | PRM_FOR_HA | PRM_TIME_UNIT);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_server_state_sync_interval_flag;
  prm->default_value = (void *) &prm_server_state_sync_interval_default;
  prm->value = (void *) &PRM_SERVER_STATE_SYNC_INTERVAL;
  prm->upper_limit = (void *) &prm_server_state_sync_interval_upper;
  prm->lower_limit = (void *) &prm_server_state_sync_interval_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_MAX_HEARTBEAT_GAP];
  prm->param_id = PRM_ID_HA_MAX_HEARTBEAT_GAP;
  prm->name = PRM_NAME_HA_MAX_HEARTBEAT_GAP;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_HIDDEN | PRM_FOR_HA);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_ha_max_heartbeat_gap_flag;
  prm->default_value = (void *) &prm_ha_max_heartbeat_gap_default;
  prm->value = (void *) &PRM_HA_MAX_HEARTBEAT_GAP;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_PING_HOSTS];
  prm->param_id = PRM_ID_HA_PING_HOSTS;
  prm->name = PRM_NAME_HA_PING_HOSTS;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_RELOADABLE | PRM_FOR_HA);
  prm->datatype = PRM_STRING;
  prm->dynamic_flag = &prm_ha_ping_hosts_flag;
  prm->default_value = (void *) &prm_ha_ping_hosts_default;
  prm->value = (void *) &PRM_HA_PING_HOSTS;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_IGNORE_ERROR_LIST];
  prm->param_id = PRM_ID_HA_IGNORE_ERROR_LIST;
  prm->name = PRM_NAME_HA_IGNORE_ERROR_LIST;
  prm->static_flag =
    (PRM_FOR_CLIENT | PRM_FOR_SERVER | PRM_FOR_HA | PRM_USER_CHANGE);
  prm->datatype = PRM_INTEGER_LIST;
  prm->dynamic_flag = &prm_ha_ignore_error_list_flag;
  prm->default_value = (void *) &prm_ha_ignore_error_list_default;
  prm->value = (void *) &PRM_HA_IGNORE_ERROR_LIST;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_SQL_LOG_MAX_SIZE_IN_MB];
  prm->param_id = PRM_ID_HA_SQL_LOG_MAX_SIZE_IN_MB;
  prm->name = PRM_NAME_HA_SQL_LOG_MAX_SIZE_IN_MB;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_HA);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_ha_sql_log_max_size_in_mb_flag;
  prm->default_value = (void *) &prm_ha_sql_log_max_size_in_mb_default;
  prm->value = (void *) &PRM_HA_SQL_LOG_MAX_SIZE_IN_MB;
  prm->upper_limit = (void *) &prm_ha_sql_log_max_size_in_mb_upper;
  prm->lower_limit = (void *) &prm_ha_sql_log_max_size_in_mb_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_COPY_LOG_MAX_ARCHIVES];
  prm->param_id = PRM_ID_HA_COPY_LOG_MAX_ARCHIVES;
  prm->name = PRM_NAME_HA_COPY_LOG_MAX_ARCHIVES;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_HA);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_ha_copy_log_max_archives_flag;
  prm->default_value = (void *) &prm_ha_copy_log_max_archives_default;
  prm->value = (void *) &PRM_HA_COPY_LOG_MAX_ARCHIVES;
  prm->upper_limit = (void *) &prm_ha_copy_log_max_archives_upper;
  prm->lower_limit = (void *) &prm_ha_copy_log_max_archives_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_COPY_LOG_TIMEOUT];
  prm->param_id = PRM_ID_HA_COPY_LOG_TIMEOUT;
  prm->name = PRM_NAME_HA_COPY_LOG_TIMEOUT;
  prm->static_flag =
    (PRM_FOR_SERVER | PRM_FOR_HA | PRM_RELOADABLE | PRM_TIME_UNIT);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_ha_copy_log_timeout_flag;
  prm->default_value = (void *) &prm_ha_copy_log_timeout_default;
  prm->value = (void *) &PRM_HA_COPY_LOG_TIMEOUT;
  prm->upper_limit = (void *) &prm_ha_copy_log_timeout_upper;
  prm->lower_limit = (void *) &prm_ha_copy_log_timeout_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_REPLICA_DELAY];
  prm->param_id = PRM_ID_HA_REPLICA_DELAY;
  prm->name = PRM_NAME_HA_REPLICA_DELAY;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_HA | PRM_TIME_UNIT);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_ha_replica_delay_flag;
  prm->default_value = (void *) &prm_ha_replica_delay_default;
  prm->value = (void *) &PRM_HA_REPLICA_DELAY;
  prm->upper_limit = (void *) &prm_ha_replica_delay_upper;
  prm->lower_limit = (void *) &prm_ha_replica_delay_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_REPLICA_TIME_BOUND];
  prm->param_id = PRM_ID_HA_REPLICA_TIME_BOUND;
  prm->name = PRM_NAME_HA_REPLICA_TIME_BOUND;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_HA);
  prm->datatype = PRM_STRING;
  prm->dynamic_flag = &prm_ha_replica_time_bound_flag;
  prm->default_value = (void *) &prm_ha_replica_time_bound_default;
  prm->value = (void *) &PRM_HA_REPLICA_TIME_BOUND;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_DELAY_LIMIT];
  prm->param_id = PRM_ID_HA_DELAY_LIMIT;
  prm->name = PRM_NAME_HA_DELAY_LIMIT;
  prm->static_flag =
    (PRM_FOR_SERVER | PRM_FOR_HA | PRM_USER_CHANGE | PRM_TIME_UNIT);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_ha_delay_limit_flag;
  prm->default_value = (void *) &prm_ha_delay_limit_default;
  prm->value = (void *) &PRM_HA_DELAY_LIMIT;
  prm->upper_limit = (void *) &prm_ha_delay_limit_upper;
  prm->lower_limit = (void *) &prm_ha_delay_limit_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_DELAY_LIMIT_DELTA];
  prm->param_id = PRM_ID_HA_DELAY_LIMIT_DELTA;
  prm->name = PRM_NAME_HA_DELAY_LIMIT_DELTA;
  prm->static_flag =
    (PRM_FOR_SERVER | PRM_FOR_HA | PRM_USER_CHANGE | PRM_TIME_UNIT);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_ha_delay_limit_delta_flag;
  prm->default_value = (void *) &prm_ha_delay_limit_delta_default;
  prm->value = (void *) &PRM_HA_DELAY_LIMIT_DELTA;
  prm->upper_limit = (void *) &prm_ha_delay_limit_delta_upper;
  prm->lower_limit = (void *) &prm_ha_delay_limit_delta_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_CHECK_DISK_FAILURE_INTERVAL];
  prm->param_id = PRM_ID_HA_CHECK_DISK_FAILURE_INTERVAL;
  prm->name = PRM_NAME_HA_CHECK_DISK_FAILURE_INTERVAL;
  prm->static_flag =
    (PRM_FOR_CLIENT | PRM_FOR_HA | PRM_TIME_UNIT | PRM_RELOADABLE);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_ha_check_disk_failure_interval_flag;
  prm->default_value = (void *) &prm_ha_check_disk_failure_interval_default;
  prm->value = (void *) &PRM_HA_CHECK_DISK_FAILURE_INTERVAL;
  prm->upper_limit = (void *) &prm_ha_check_disk_failure_interval_upper;
  prm->lower_limit = (void *) &prm_ha_check_disk_failure_interval_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_LOG_ASYNC_LOG_FLUSH_INTERVAL];
  prm->param_id = PRM_ID_LOG_ASYNC_LOG_FLUSH_INTERVAL;
  prm->name = PRM_NAME_LOG_ASYNC_LOG_FLUSH_INTERVAL;
  prm->static_flag = (PRM_FOR_SERVER | PRM_USER_CHANGE | PRM_TIME_UNIT);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_log_async_log_flush_interval_flag;
  prm->default_value = (void *) &prm_log_async_log_flush_interval_default;
  prm->value = (void *) &PRM_LOG_ASYNC_LOG_FLUSH_INTERVAL;
  prm->upper_limit = (void *) &prm_log_async_log_flush_interval_upper;
  prm->lower_limit = (void *) &prm_log_async_log_flush_interval_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_LOG_COMPRESS];
  prm->param_id = PRM_ID_LOG_COMPRESS;
  prm->name = PRM_NAME_LOG_COMPRESS;
  prm->static_flag = (PRM_FOR_SERVER);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_log_compress_flag;
  prm->default_value = (void *) &prm_log_compress_default;
  prm->value = (void *) &PRM_LOG_COMPRESS;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_BLOCK_NOWHERE_STATEMENT];
  prm->param_id = PRM_ID_BLOCK_NOWHERE_STATEMENT;
  prm->name = PRM_NAME_BLOCK_NOWHERE_STATEMENT;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_block_nowhere_statement_flag;
  prm->default_value = (void *) &prm_block_nowhere_statement_default;
  prm->value = (void *) &PRM_BLOCK_NOWHERE_STATEMENT;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_BLOCK_DDL_STATEMENT];
  prm->param_id = PRM_ID_BLOCK_DDL_STATEMENT;
  prm->name = PRM_NAME_BLOCK_DDL_STATEMENT;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_block_ddl_statement_flag;
  prm->default_value = (void *) &prm_block_ddl_statement_default;
  prm->value = (void *) &PRM_BLOCK_DDL_STATEMENT;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_RSQL_HISTORY_NUM];
  prm->param_id = PRM_ID_RSQL_HISTORY_NUM;
  prm->name = PRM_NAME_RSQL_HISTORY_NUM;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_rsql_history_num_flag;
  prm->default_value = (void *) &prm_rsql_history_num_default;
  prm->value = (void *) &PRM_RSQL_HISTORY_NUM;
  prm->upper_limit = (void *) &prm_rsql_history_num_upper;
  prm->lower_limit = (void *) &prm_rsql_history_num_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_LOG_TRACE_DEBUG];
  prm->param_id = PRM_ID_LOG_TRACE_DEBUG;
  prm->name = PRM_NAME_LOG_TRACE_DEBUG;
  prm->static_flag = (PRM_FOR_SERVER | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_log_trace_debug_flag;
  prm->default_value = (void *) &prm_log_trace_debug_default;
  prm->value = (void *) &PRM_LOG_TRACE_DEBUG;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_ER_PRODUCTION_MODE];
  prm->param_id = PRM_ID_ER_PRODUCTION_MODE;
  prm->name = PRM_NAME_ER_PRODUCTION_MODE;
  prm->static_flag = (PRM_FOR_SERVER | PRM_FOR_CLIENT | PRM_USER_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_er_production_mode_flag;
  prm->default_value = (void *) &prm_er_production_mode_default;
  prm->value = (void *) &PRM_ER_PRODUCTION_MODE;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_ER_STOP_ON_ERROR];
  prm->param_id = PRM_ID_ER_STOP_ON_ERROR;
  prm->name = PRM_NAME_ER_STOP_ON_ERROR;
  prm->static_flag =
    (PRM_FOR_SERVER | PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_er_stop_on_error_flag;
  prm->default_value = (void *) &prm_er_stop_on_error_default;
  prm->value = (void *) &PRM_ER_STOP_ON_ERROR;
  prm->upper_limit = (void *) &prm_er_stop_on_error_upper;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_TCP_RCVBUF_SIZE];
  prm->param_id = PRM_ID_TCP_RCVBUF_SIZE;
  prm->name = PRM_NAME_TCP_RCVBUF_SIZE;
  prm->static_flag = (PRM_FOR_SERVER | PRM_FOR_CLIENT | PRM_HIDDEN);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_tcp_rcvbuf_size_flag;
  prm->default_value = (void *) &prm_tcp_rcvbuf_size_default;
  prm->value = (void *) &PRM_TCP_RCVBUF_SIZE;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_TCP_SNDBUF_SIZE];
  prm->param_id = PRM_ID_TCP_SNDBUF_SIZE;
  prm->name = PRM_NAME_TCP_SNDBUF_SIZE;
  prm->static_flag = (PRM_FOR_SERVER | PRM_FOR_CLIENT | PRM_HIDDEN);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_tcp_sndbuf_size_flag;
  prm->default_value = (void *) &prm_tcp_sndbuf_size_default;
  prm->value = (void *) &PRM_TCP_SNDBUF_SIZE;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_TCP_NODELAY];
  prm->param_id = PRM_ID_TCP_NODELAY;
  prm->name = PRM_NAME_TCP_NODELAY;
  prm->static_flag = (PRM_FOR_SERVER | PRM_FOR_CLIENT | PRM_HIDDEN);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_tcp_nodelay_flag;
  prm->default_value = (void *) &prm_tcp_nodelay_default;
  prm->value = (void *) &PRM_TCP_NODELAY;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_TCP_KEEPALIVE];
  prm->param_id = PRM_ID_TCP_KEEPALIVE;
  prm->name = PRM_NAME_TCP_KEEPALIVE;
  prm->static_flag = (PRM_FOR_SERVER | PRM_FOR_CLIENT);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_tcp_keepalive_flag;
  prm->default_value = (void *) &prm_tcp_keepalive_default;
  prm->value = (void *) &PRM_TCP_KEEPALIVE;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_RSQL_SINGLE_LINE_MODE];
  prm->param_id = PRM_ID_RSQL_SINGLE_LINE_MODE;
  prm->name = PRM_NAME_RSQL_SINGLE_LINE_MODE;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_rsql_single_line_mode_flag;
  prm->default_value = (void *) &prm_rsql_single_line_mode_default;
  prm->value = (void *) &PRM_RSQL_SINGLE_LINE_MODE;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_XASL_DEBUG_DUMP];
  prm->param_id = PRM_ID_XASL_DEBUG_DUMP;
  prm->name = PRM_NAME_XASL_DEBUG_DUMP;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_xasl_debug_dump_flag;
  prm->default_value = (void *) &prm_xasl_debug_dump_default;
  prm->value = (void *) &PRM_XASL_DEBUG_DUMP;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_LOG_MAX_ARCHIVES];
  prm->param_id = PRM_ID_LOG_MAX_ARCHIVES;
  prm->name = PRM_NAME_LOG_MAX_ARCHIVES;
  prm->static_flag = (PRM_FOR_SERVER | PRM_USER_CHANGE);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_log_max_archives_flag;
  prm->default_value = (void *) &prm_log_max_archives_default;
  prm->value = (void *) &PRM_LOG_MAX_ARCHIVES;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) &prm_log_max_archives_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_FORCE_REMOVE_LOG_ARCHIVES];
  prm->param_id = PRM_ID_FORCE_REMOVE_LOG_ARCHIVES;
  prm->name = PRM_NAME_FORCE_REMOVE_LOG_ARCHIVES;
  prm->static_flag = (PRM_FOR_SERVER | PRM_USER_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_force_remove_log_archives_flag;
  prm->default_value = (void *) &prm_force_remove_log_archives_default;
  prm->value = (void *) &PRM_FORCE_REMOVE_LOG_ARCHIVES;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_REMOVE_LOG_ARCHIVES_INTERVAL];
  prm->param_id = PRM_ID_REMOVE_LOG_ARCHIVES_INTERVAL;
  prm->name = PRM_NAME_REMOVE_LOG_ARCHIVES_INTERVAL;
  prm->static_flag =
    (PRM_FOR_SERVER | PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_remove_log_archives_interval_flag;
  prm->default_value = (void *) &prm_remove_log_archives_interval_default;
  prm->value = (void *) &PRM_REMOVE_LOG_ARCHIVES_INTERVAL;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) &prm_remove_log_archives_interval_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_EVENT_HANDLER];
  prm->param_id = PRM_ID_EVENT_HANDLER;
  prm->name = PRM_NAME_EVENT_HANDLER;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_SERVER);
  prm->datatype = PRM_STRING;
  prm->dynamic_flag = &prm_event_handler_flag;
  prm->default_value = (void *) &prm_event_handler_default;
  prm->value = (void *) &PRM_EVENT_HANDLER;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_EVENT_ACTIVATION];
  prm->param_id = PRM_ID_EVENT_ACTIVATION;
  prm->name = PRM_NAME_EVENT_ACTIVATION;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_SERVER);
  prm->datatype = PRM_INTEGER_LIST;
  prm->dynamic_flag = &prm_event_activation_flag;
  prm->default_value = (void *) &prm_event_activation_default;
  prm->value = (void *) &PRM_EVENT_ACTIVATION;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_MNT_WAITING_THREAD];
  prm->param_id = PRM_ID_MNT_WAITING_THREAD;
  prm->name = PRM_NAME_MNT_WAITING_THREAD;
  prm->static_flag = (PRM_FOR_SERVER | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_mnt_waiting_thread_flag;
  prm->default_value = (void *) &prm_mnt_waiting_thread_default;
  prm->value = (void *) &PRM_MNT_WAITING_THREAD;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) &prm_mnt_waiting_thread_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_MNT_STATS_THRESHOLD];
  prm->param_id = PRM_ID_MNT_STATS_THRESHOLD;
  prm->name = PRM_NAME_MNT_STATS_THRESHOLD;
  prm->static_flag = (PRM_FOR_SERVER | PRM_HIDDEN);
  prm->datatype = PRM_INTEGER_LIST;
  prm->dynamic_flag = &prm_mnt_stats_threshold_flag;
  prm->default_value = (void *) &prm_mnt_stats_threshold_default;
  prm->value = (void *) &PRM_MNT_STATS_THRESHOLD;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_SESSION_STATE_TIMEOUT];
  prm->param_id = PRM_ID_SESSION_STATE_TIMEOUT;
  prm->name = PRM_NAME_SESSION_STATE_TIMEOUT;
  prm->static_flag = (PRM_FOR_SERVER | PRM_TEST_CHANGE);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_session_timeout_flag;
  prm->default_value = (void *) &prm_session_timeout_default;
  prm->value = (void *) &PRM_SESSION_STATE_TIMEOUT;
  prm->upper_limit = (void *) &prm_session_timeout_upper;
  prm->lower_limit = (void *) &prm_session_timeout_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_MULTI_RANGE_OPT_LIMIT];
  prm->param_id = PRM_ID_MULTI_RANGE_OPT_LIMIT;
  prm->name = PRM_NAME_MULTI_RANGE_OPT_LIMIT;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_multi_range_opt_limit_flag;
  prm->default_value = (void *) &prm_multi_range_opt_limit_default;
  prm->value = (void *) &PRM_MULTI_RANGE_OPT_LIMIT;
  prm->upper_limit = (void *) &prm_multi_range_opt_limit_upper;
  prm->lower_limit = (void *) &prm_multi_range_opt_limit_lower;
  prm->force_value = (char *) NULL;

  /* All the compound parameters *must* be at the end of the array so that the
     changes they cause are not overridden by other parameters (for example in
     sysprm_load_and_init the parameters are set to their default in the order
     they are found in this array). */
  prm = &prm_Def[PRM_ID_DB_VOLUME_SIZE];
  prm->param_id = PRM_ID_DB_VOLUME_SIZE;
  prm->name = PRM_NAME_DB_VOLUME_SIZE;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_SERVER | PRM_SIZE_UNIT);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_db_volume_size_flag;
  prm->default_value = (void *) &prm_db_volume_size_default;
  prm->value = (void *) &PRM_DB_VOLUME_SIZE;
  prm->upper_limit = (void *) &prm_db_volume_size_upper;
  prm->lower_limit = (void *) &prm_db_volume_size_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_CHECK_PEER_ALIVE];
  prm->param_id = PRM_ID_CHECK_PEER_ALIVE;
  prm->name = PRM_NAME_CHECK_PEER_ALIVE;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_SERVER | PRM_USER_CHANGE);
  prm->datatype = PRM_KEYWORD;
  prm->dynamic_flag = &prm_check_peer_alive_flag;
  prm->default_value = (void *) &prm_check_peer_alive_default;
  prm->value = (void *) &PRM_CHECK_PEER_ALIVE;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_SQL_TRACE_SLOW];
  prm->param_id = PRM_ID_SQL_TRACE_SLOW;
  prm->name = PRM_NAME_SQL_TRACE_SLOW;
  prm->static_flag = (PRM_USER_CHANGE | PRM_FOR_SERVER | PRM_TIME_UNIT);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_sql_trace_slow_flag;
  prm->default_value = (void *) &prm_sql_trace_slow_default;
  prm->value = (void *) &PRM_SQL_TRACE_SLOW;
  prm->upper_limit = (void *) &prm_sql_trace_slow_upper;
  prm->lower_limit = (void *) &prm_sql_trace_slow_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_SERVER_TRACE];
  prm->param_id = PRM_ID_SERVER_TRACE;
  prm->name = PRM_NAME_SERVER_TRACE;
  prm->static_flag = (PRM_USER_CHANGE | PRM_FOR_SERVER);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_server_trace_flag;
  prm->default_value = (void *) &prm_server_trace_default;
  prm->value = (void *) &PRM_SERVER_TRACE;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_SQL_TRACE_EXECUTION_PLAN];
  prm->param_id = PRM_ID_SQL_TRACE_EXECUTION_PLAN;
  prm->name = PRM_NAME_SQL_TRACE_EXECUTION_PLAN;
  prm->static_flag = (PRM_USER_CHANGE | PRM_FOR_SERVER);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_sql_trace_execution_plan_flag;
  prm->default_value = (void *) &prm_sql_trace_execution_plan_default;
  prm->value = (void *) &PRM_SQL_TRACE_EXECUTION_PLAN;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_LOG_TRACE_FLUSH_TIME];
  prm->param_id = PRM_ID_LOG_TRACE_FLUSH_TIME;
  prm->name = PRM_NAME_LOG_TRACE_FLUSH_TIME;
  prm->static_flag = (PRM_USER_CHANGE | PRM_FOR_SERVER | PRM_TIME_UNIT);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_log_trace_flush_time_flag;
  prm->default_value = (void *) &prm_log_trace_flush_time_default;
  prm->value = (void *) &PRM_LOG_TRACE_FLUSH_TIME;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) &prm_log_trace_flush_time_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_GENERIC_VOL_PREALLOC_SIZE];
  prm->param_id = PRM_ID_GENERIC_VOL_PREALLOC_SIZE;
  prm->name = PRM_NAME_GENERIC_VOL_PREALLOC_SIZE;
  prm->static_flag = (PRM_FOR_SERVER | PRM_USER_CHANGE | PRM_SIZE_UNIT);
  prm->datatype = PRM_BIGINT;
  prm->dynamic_flag = &prm_generic_vol_prealloc_size_flag;
  prm->default_value = (void *) &prm_generic_vol_prealloc_size_default;
  prm->value = (void *) &PRM_GENERIC_VOL_PREALLOC_SIZE;
  prm->upper_limit = (void *) &prm_generic_vol_prealloc_size_upper;
  prm->lower_limit = (void *) &prm_generic_vol_prealloc_size_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_SORT_LIMIT_MAX_COUNT];
  prm->param_id = PRM_ID_SORT_LIMIT_MAX_COUNT;
  prm->name = PRM_NAME_SORT_LIMIT_MAX_COUNT;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_sort_limit_max_count_flag;
  prm->default_value = (void *) &prm_sort_limit_max_count_default;
  prm->value = (void *) &PRM_SORT_LIMIT_MAX_COUNT;
  prm->upper_limit = (void *) &prm_sort_limit_max_count_upper;
  prm->lower_limit = (void *) &prm_sort_limit_max_count_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_SQL_TRACE_IOREADS];
  prm->param_id = PRM_ID_SQL_TRACE_IOREADS;
  prm->name = PRM_NAME_SQL_TRACE_IOREADS;
  prm->static_flag = (PRM_USER_CHANGE | PRM_FOR_SERVER);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_sql_trace_ioreads_flag;
  prm->default_value = (void *) &prm_sql_trace_ioreads_default;
  prm->value = (void *) &PRM_SQL_TRACE_IOREADS;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) &prm_sql_trace_ioreads_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_QUERY_TRACE];
  prm->param_id = PRM_ID_QUERY_TRACE;
  prm->name = PRM_NAME_QUERY_TRACE;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_query_trace_flag;
  prm->default_value = (void *) &prm_query_trace_default;
  prm->value = (void *) &PRM_QUERY_TRACE;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_QUERY_TRACE_FORMAT];
  prm->param_id = PRM_ID_QUERY_TRACE_FORMAT;
  prm->name = PRM_NAME_QUERY_TRACE_FORMAT;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE);
  prm->datatype = PRM_KEYWORD;
  prm->dynamic_flag = &prm_query_trace_format_flag;
  prm->default_value = (void *) &prm_query_trace_format_default;
  prm->value = (void *) &PRM_QUERY_TRACE_FORMAT;
  prm->upper_limit = (void *) &prm_query_trace_format_upper;
  prm->lower_limit = (void *) &prm_query_trace_format_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_MAX_RECURSION_SQL_DEPTH];
  prm->param_id = PRM_ID_MAX_RECURSION_SQL_DEPTH;
  prm->name = PRM_NAME_MAX_RECURSION_SQL_DEPTH;
  prm->static_flag = (PRM_FOR_SERVER | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_max_recursion_sql_depth_flag;
  prm->default_value = (void *) &prm_max_recursion_sql_depth_default;
  prm->value = (void *) &PRM_MAX_RECURSION_SQL_DEPTH;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_BTREE_MERGE_ENABLED];
  prm->param_id = PRM_ID_BTREE_MERGE_ENABLED;
  prm->name = PRM_NAME_BTREE_MERGE_ENABLED;
  prm->static_flag = (PRM_FOR_SERVER | PRM_USER_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_btree_merge_enabled_flag;
  prm->default_value = (void *) &prm_btree_merge_enabled_default;
  prm->value = (void *) &PRM_BTREE_MERGE_ENABLED;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  /* For shard debugging */
  prm = &prm_Def[PRM_ID_SHARD_GROUPID_DEBUG];
  prm->param_id = PRM_ID_SHARD_GROUPID_DEBUG;
  prm->name = PRM_NAME_SHARD_GROUPID_DEBUG;
  prm->static_flag = (PRM_FOR_SERVER | PRM_HIDDEN | PRM_USER_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_shard_groupid_debug_flag;
  prm->default_value = (void *) &prm_shard_groupid_debug_default;
  prm->value = (void *) &PRM_SHARD_GROUPID_DEBUG;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_SHARD_GROUPID_01];
  prm->param_id = PRM_ID_SHARD_GROUPID_01;
  prm->name = PRM_NAME_SHARD_GROUPID_01;
  prm->static_flag = (PRM_FOR_SERVER | PRM_HIDDEN | PRM_USER_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_shard_groupid_01_flag;
  prm->default_value = (void *) &prm_shard_groupid_01_default;
  prm->value = (void *) &PRM_SHARD_GROUPID_01;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_SHARD_GROUPID_02];
  prm->param_id = PRM_ID_SHARD_GROUPID_02;
  prm->name = PRM_NAME_SHARD_GROUPID_02;
  prm->static_flag = (PRM_FOR_SERVER | PRM_HIDDEN | PRM_USER_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_shard_groupid_02_flag;
  prm->default_value = (void *) &prm_shard_groupid_02_default;
  prm->value = (void *) &PRM_SHARD_GROUPID_02;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_SHARD_GROUPID_03];
  prm->param_id = PRM_ID_SHARD_GROUPID_03;
  prm->name = PRM_NAME_SHARD_GROUPID_03;
  prm->static_flag = (PRM_FOR_SERVER | PRM_HIDDEN | PRM_USER_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_shard_groupid_03_flag;
  prm->default_value = (void *) &prm_shard_groupid_03_default;
  prm->value = (void *) &PRM_SHARD_GROUPID_03;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_SHARD_GROUPID_04];
  prm->param_id = PRM_ID_SHARD_GROUPID_04;
  prm->name = PRM_NAME_SHARD_GROUPID_04;
  prm->static_flag = (PRM_FOR_SERVER | PRM_HIDDEN | PRM_USER_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_shard_groupid_04_flag;
  prm->default_value = (void *) &prm_shard_groupid_04_default;
  prm->value = (void *) &PRM_SHARD_GROUPID_04;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_SHARD_GROUPID_05];
  prm->param_id = PRM_ID_SHARD_GROUPID_05;
  prm->name = PRM_NAME_SHARD_GROUPID_05;
  prm->static_flag = (PRM_FOR_SERVER | PRM_HIDDEN | PRM_USER_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_shard_groupid_05_flag;
  prm->default_value = (void *) &prm_shard_groupid_05_default;
  prm->value = (void *) &PRM_SHARD_GROUPID_05;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_SHARD_GROUPID_06];
  prm->param_id = PRM_ID_SHARD_GROUPID_06;
  prm->name = PRM_NAME_SHARD_GROUPID_06;
  prm->static_flag = (PRM_FOR_SERVER | PRM_HIDDEN | PRM_USER_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_shard_groupid_06_flag;
  prm->default_value = (void *) &prm_shard_groupid_06_default;
  prm->value = (void *) &PRM_SHARD_GROUPID_06;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_SHARD_GROUPID_07];
  prm->param_id = PRM_ID_SHARD_GROUPID_07;
  prm->name = PRM_NAME_SHARD_GROUPID_07;
  prm->static_flag = (PRM_FOR_SERVER | PRM_HIDDEN | PRM_USER_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_shard_groupid_07_flag;
  prm->default_value = (void *) &prm_shard_groupid_07_default;
  prm->value = (void *) &PRM_SHARD_GROUPID_07;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_SHARD_GROUPID_08];
  prm->param_id = PRM_ID_SHARD_GROUPID_08;
  prm->name = PRM_NAME_SHARD_GROUPID_08;
  prm->static_flag = (PRM_FOR_SERVER | PRM_HIDDEN | PRM_USER_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_shard_groupid_08_flag;
  prm->default_value = (void *) &prm_shard_groupid_08_default;
  prm->value = (void *) &PRM_SHARD_GROUPID_08;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_SHARD_GROUPID_09];
  prm->param_id = PRM_ID_SHARD_GROUPID_09;
  prm->name = PRM_NAME_SHARD_GROUPID_09;
  prm->static_flag = (PRM_FOR_SERVER | PRM_HIDDEN | PRM_USER_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_shard_groupid_09_flag;
  prm->default_value = (void *) &prm_shard_groupid_09_default;
  prm->value = (void *) &PRM_SHARD_GROUPID_09;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_SHARD_GROUPID_10];
  prm->param_id = PRM_ID_SHARD_GROUPID_10;
  prm->name = PRM_NAME_SHARD_GROUPID_10;
  prm->static_flag = (PRM_FOR_SERVER | PRM_HIDDEN | PRM_USER_CHANGE);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_shard_groupid_10_flag;
  prm->default_value = (void *) &prm_shard_groupid_10_default;
  prm->value = (void *) &PRM_SHARD_GROUPID_10;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_OPTIMIZER_RESERVE_01];
  prm->param_id = PRM_ID_OPTIMIZER_RESERVE_01;
  prm->name = PRM_NAME_OPTIMIZER_RESERVE_01;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_optimizer_reserve_01_flag;
  prm->default_value = (void *) &prm_optimizer_reserve_01_default;
  prm->value = (void *) &PRM_OPTIMIZER_RESERVE_01;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_OPTIMIZER_RESERVE_02];
  prm->param_id = PRM_ID_OPTIMIZER_RESERVE_02;
  prm->name = PRM_NAME_OPTIMIZER_RESERVE_02;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_optimizer_reserve_02_flag;
  prm->default_value = (void *) &prm_optimizer_reserve_02_default;
  prm->value = (void *) &PRM_OPTIMIZER_RESERVE_02;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_OPTIMIZER_RESERVE_03];
  prm->param_id = PRM_ID_OPTIMIZER_RESERVE_03;
  prm->name = PRM_NAME_OPTIMIZER_RESERVE_03;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_optimizer_reserve_03_flag;
  prm->default_value = (void *) &prm_optimizer_reserve_03_default;
  prm->value = (void *) &PRM_OPTIMIZER_RESERVE_03;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_OPTIMIZER_RESERVE_04];
  prm->param_id = PRM_ID_OPTIMIZER_RESERVE_04;
  prm->name = PRM_NAME_OPTIMIZER_RESERVE_04;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_optimizer_reserve_04_flag;
  prm->default_value = (void *) &prm_optimizer_reserve_04_default;
  prm->value = (void *) &PRM_OPTIMIZER_RESERVE_04;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_OPTIMIZER_RESERVE_05];
  prm->param_id = PRM_ID_OPTIMIZER_RESERVE_05;
  prm->name = PRM_NAME_OPTIMIZER_RESERVE_05;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_optimizer_reserve_05_flag;
  prm->default_value = (void *) &prm_optimizer_reserve_05_default;
  prm->value = (void *) &PRM_OPTIMIZER_RESERVE_05;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_OPTIMIZER_RESERVE_06];
  prm->param_id = PRM_ID_OPTIMIZER_RESERVE_06;
  prm->name = PRM_NAME_OPTIMIZER_RESERVE_06;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_optimizer_reserve_06_flag;
  prm->default_value = (void *) &prm_optimizer_reserve_06_default;
  prm->value = (void *) &PRM_OPTIMIZER_RESERVE_06;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_OPTIMIZER_RESERVE_07];
  prm->param_id = PRM_ID_OPTIMIZER_RESERVE_07;
  prm->name = PRM_NAME_OPTIMIZER_RESERVE_07;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_optimizer_reserve_07_flag;
  prm->default_value = (void *) &prm_optimizer_reserve_07_default;
  prm->value = (void *) &PRM_OPTIMIZER_RESERVE_07;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_OPTIMIZER_RESERVE_08];
  prm->param_id = PRM_ID_OPTIMIZER_RESERVE_08;
  prm->name = PRM_NAME_OPTIMIZER_RESERVE_08;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_optimizer_reserve_08_flag;
  prm->default_value = (void *) &prm_optimizer_reserve_08_default;
  prm->value = (void *) &PRM_OPTIMIZER_RESERVE_08;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_OPTIMIZER_RESERVE_09];
  prm->param_id = PRM_ID_OPTIMIZER_RESERVE_09;
  prm->name = PRM_NAME_OPTIMIZER_RESERVE_09;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_optimizer_reserve_09_flag;
  prm->default_value = (void *) &prm_optimizer_reserve_09_default;
  prm->value = (void *) &PRM_OPTIMIZER_RESERVE_09;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_OPTIMIZER_RESERVE_10];
  prm->param_id = PRM_ID_OPTIMIZER_RESERVE_10;
  prm->name = PRM_NAME_OPTIMIZER_RESERVE_10;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_optimizer_reserve_10_flag;
  prm->default_value = (void *) &prm_optimizer_reserve_10_default;
  prm->value = (void *) &PRM_OPTIMIZER_RESERVE_10;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_OPTIMIZER_RESERVE_11];
  prm->param_id = PRM_ID_OPTIMIZER_RESERVE_11;
  prm->name = PRM_NAME_OPTIMIZER_RESERVE_11;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_optimizer_reserve_11_flag;
  prm->default_value = (void *) &prm_optimizer_reserve_11_default;
  prm->value = (void *) &PRM_OPTIMIZER_RESERVE_11;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_OPTIMIZER_RESERVE_12];
  prm->param_id = PRM_ID_OPTIMIZER_RESERVE_12;
  prm->name = PRM_NAME_OPTIMIZER_RESERVE_12;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_optimizer_reserve_12_flag;
  prm->default_value = (void *) &prm_optimizer_reserve_12_default;
  prm->value = (void *) &PRM_OPTIMIZER_RESERVE_12;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_OPTIMIZER_RESERVE_13];
  prm->param_id = PRM_ID_OPTIMIZER_RESERVE_13;
  prm->name = PRM_NAME_OPTIMIZER_RESERVE_13;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_optimizer_reserve_13_flag;
  prm->default_value = (void *) &prm_optimizer_reserve_13_default;
  prm->value = (void *) &PRM_OPTIMIZER_RESERVE_13;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_OPTIMIZER_RESERVE_14];
  prm->param_id = PRM_ID_OPTIMIZER_RESERVE_14;
  prm->name = PRM_NAME_OPTIMIZER_RESERVE_14;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_optimizer_reserve_14_flag;
  prm->default_value = (void *) &prm_optimizer_reserve_14_default;
  prm->value = (void *) &PRM_OPTIMIZER_RESERVE_14;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_OPTIMIZER_RESERVE_15];
  prm->param_id = PRM_ID_OPTIMIZER_RESERVE_15;
  prm->name = PRM_NAME_OPTIMIZER_RESERVE_15;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_optimizer_reserve_15_flag;
  prm->default_value = (void *) &prm_optimizer_reserve_15_default;
  prm->value = (void *) &PRM_OPTIMIZER_RESERVE_15;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_OPTIMIZER_RESERVE_16];
  prm->param_id = PRM_ID_OPTIMIZER_RESERVE_16;
  prm->name = PRM_NAME_OPTIMIZER_RESERVE_16;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_optimizer_reserve_16_flag;
  prm->default_value = (void *) &prm_optimizer_reserve_16_default;
  prm->value = (void *) &PRM_OPTIMIZER_RESERVE_16;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_OPTIMIZER_RESERVE_17];
  prm->param_id = PRM_ID_OPTIMIZER_RESERVE_17;
  prm->name = PRM_NAME_OPTIMIZER_RESERVE_17;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_optimizer_reserve_17_flag;
  prm->default_value = (void *) &prm_optimizer_reserve_17_default;
  prm->value = (void *) &PRM_OPTIMIZER_RESERVE_17;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_OPTIMIZER_RESERVE_18];
  prm->param_id = PRM_ID_OPTIMIZER_RESERVE_18;
  prm->name = PRM_NAME_OPTIMIZER_RESERVE_18;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_optimizer_reserve_18_flag;
  prm->default_value = (void *) &prm_optimizer_reserve_18_default;
  prm->value = (void *) &PRM_OPTIMIZER_RESERVE_18;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_OPTIMIZER_RESERVE_19];
  prm->param_id = PRM_ID_OPTIMIZER_RESERVE_19;
  prm->name = PRM_NAME_OPTIMIZER_RESERVE_19;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_optimizer_reserve_19_flag;
  prm->default_value = (void *) &prm_optimizer_reserve_19_default;
  prm->value = (void *) &PRM_OPTIMIZER_RESERVE_19;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_OPTIMIZER_RESERVE_20];
  prm->param_id = PRM_ID_OPTIMIZER_RESERVE_20;
  prm->name = PRM_NAME_OPTIMIZER_RESERVE_20;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_optimizer_reserve_20_flag;
  prm->default_value = (void *) &prm_optimizer_reserve_20_default;
  prm->value = (void *) &PRM_OPTIMIZER_RESERVE_20;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_OPTIMIZER_ENABLE_AGGREGATE_OPTIMIZATION];
  prm->param_id = PRM_ID_OPTIMIZER_ENABLE_AGGREGATE_OPTIMIZATION;
  prm->name = PRM_NAME_OPTIMIZER_ENABLE_AGGREGATE_OPTIMIZATION;
  prm->static_flag = (PRM_FOR_SERVER | PRM_TEST_CHANGE | PRM_HIDDEN);
  prm->datatype = PRM_BOOLEAN;
  prm->dynamic_flag = &prm_optimizer_enable_aggregate_optimization_flag;
  prm->default_value =
    (void *) &prm_optimizer_enable_aggregate_optimization_default;
  prm->value = (void *) &PRM_OPTIMIZER_ENABLE_AGGREGATE_OPTIMIZATION;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_REPL_FILTER_TYPE];
  prm->param_id = PRM_ID_HA_REPL_FILTER_TYPE;
  prm->name = PRM_NAME_HA_REPL_FILTER_TYPE;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_HA);
  prm->datatype = PRM_KEYWORD;
  prm->dynamic_flag = &prm_ha_repl_filter_type_flag;
  prm->default_value = (void *) &prm_ha_repl_filter_type_default;
  prm->value = (void *) &PRM_HA_REPL_FILTER_TYPE;
  prm->upper_limit = (void *) &prm_ha_repl_filter_type_upper;
  prm->lower_limit = (void *) &prm_ha_repl_filter_type_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_REPL_FILTER_FILE];
  prm->param_id = PRM_ID_HA_REPL_FILTER_FILE;
  prm->name = PRM_NAME_HA_REPL_FILTER_FILE;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_HA);
  prm->datatype = PRM_STRING;
  prm->dynamic_flag = &prm_ha_repl_filter_file_flag;
  prm->default_value = (void *) &prm_ha_repl_filter_file_default;
  prm->value = (void *) &PRM_HA_REPL_FILTER_FILE;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_FAULT_INJECTION];
  prm->param_id = PRM_ID_FAULT_INJECTION;
  prm->name = PRM_NAME_FAULT_INJECTION;
  prm->static_flag =
    (PRM_USER_CHANGE | PRM_FOR_SERVER | PRM_FOR_CLIENT | PRM_HIDDEN);
  prm->datatype = PRM_STRING;
  prm->dynamic_flag = &prm_fault_injection_flag;
  prm->default_value = (void *) &prm_fault_injection_default;
  prm->value = (void *) &PRM_FAULT_INJECTION;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_RYE_SHM_KEY];
  prm->param_id = PRM_ID_RYE_SHM_KEY;
  prm->name = PRM_NAME_RYE_SHM_KEY;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_SERVER | PRM_FOR_HA);
  prm->datatype = PRM_STRING;
  prm->dynamic_flag = &prm_rye_shm_key_flag;
  prm->default_value = (void *) &prm_rye_shm_key_default;
  prm->value = (void *) &PRM_RYE_SHM_KEY;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_MAX_LOG_APPLIER];
  prm->param_id = PRM_ID_HA_MAX_LOG_APPLIER;
  prm->name = PRM_NAME_HA_MAX_LOG_APPLIER;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_SERVER | PRM_FOR_HA);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_ha_max_log_applier_flag;
  prm->default_value = (void *) &prm_ha_max_log_applier_default;
  prm->value = (void *) &PRM_HA_MAX_LOG_APPLIER;
  prm->upper_limit = (void *) &prm_ha_max_log_applier_upper;
  prm->lower_limit = (void *) &prm_ha_max_log_applier_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_MAX_CLIENTS];
  prm->param_id = PRM_ID_MAX_CLIENTS;
  prm->name = PRM_NAME_MAX_CLIENTS;
  prm->static_flag = (PRM_FOR_SERVER);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_max_clients_flag;
  prm->default_value = (void *) &prm_max_clients_default;
  prm->value = (void *) &PRM_MAX_CLIENTS;
  prm->upper_limit = (void *) &prm_max_clients_upper;
  prm->lower_limit = (void *) &prm_max_clients_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_MAX_COPYLOG_CONNECTIONS];
  prm->param_id = PRM_ID_MAX_COPYLOG_CONNECTIONS;
  prm->name = PRM_NAME_MAX_COPYLOG_CONNECIONTS;
  prm->static_flag = (PRM_FOR_SERVER);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_max_copylog_conns_flag;
  prm->default_value = (void *) &prm_max_copylog_conns_default;
  prm->value = (void *) &PRM_MAX_COPYLOG_CONNECTIONS;
  prm->upper_limit = (void *) &prm_max_copylog_conns_upper;
  prm->lower_limit = (void *) &prm_max_copylog_conns_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_MIGRATOR_MAX_REPL_DELAY];
  prm->param_id = PRM_ID_MIGRATOR_MAX_REPL_DELAY;
  prm->name = PRM_NAME_MIGRATOR_MAX_REPL_DELAY;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_USER_CHANGE);
  prm->datatype = PRM_INTEGER;
  prm->dynamic_flag = &prm_migrator_max_repl_delay_flag;
  prm->default_value = (void *) &prm_migrator_max_repl_delay_default;
  prm->value = (void *) &PRM_MIGRATOR_MAX_REPL_DELAY;
  prm->upper_limit = (void *) &prm_migrator_max_repl_delay_upper;
  prm->lower_limit = (void *) &prm_migrator_max_repl_delay_lower;
  prm->force_value = (char *) NULL;

  prm = &prm_Def[PRM_ID_HA_NODE_MYSELF];
  prm->param_id = PRM_ID_HA_NODE_MYSELF;
  prm->name = PRM_NAME_HA_NODE_MYSELF;
  prm->static_flag = (PRM_FOR_CLIENT | PRM_FOR_SERVER);
  prm->datatype = PRM_STRING;
  prm->dynamic_flag = &prm_ha_node_myself_flag;
  prm->default_value = (void *) &prm_ha_node_myself_default;
  prm->value = (void *) &PRM_HA_NODE_MYSELF;
  prm->upper_limit = (void *) NULL;
  prm->lower_limit = (void *) NULL;
  prm->force_value = (char *) NULL;

  prm_Def_is_initialized = true;
}

/*
 * sysprm_dump_parameters - Print out current system parameters
 *   return: none
 *   fp(in):
 */
void
sysprm_dump_parameters (FILE * fp)
{
  char buf[LINE_MAX];
  int i;
  const SYSPRM_PARAM *prm;

  if (prm_Def_is_initialized == false)
    {
      sysprm_initialize_prm_def ();
    }

  fprintf (fp, "#\n# %s\n#\n\n", sysprm_auto_conf_file_name);

  fprintf (fp, "\n# system parameters\n");
  for (i = 0; i < NUM_PRM; i++)
    {
      prm = &prm_Def[i];
      if (PRM_IS_HIDDEN (prm->static_flag)
	  || PRM_IS_OBSOLETED (prm->static_flag))
	{
	  continue;
	}
      prm_print (prm, buf, LINE_MAX, PRM_PRINT_NAME, PRM_PRINT_CURR_VAL);
      fprintf (fp, "%s\n", buf);
    }

  fprintf (fp, "\n");

  return;
}

/*
 * sysprm_dump_persist_conf_file - Print out current system parameters
 *   return: none
 *   fp(in):
 *   proc_name(in):
 *   sect_name(in):
 */
int
sysprm_dump_persist_conf_file (FILE * fp, const char *proc_name,
			       const char *sect_name)
{
  int error = NO_ERROR;
  char file_being_dealt_with[PATH_MAX];
  int conf_fd;
  FILEIO_LOCKF_TYPE lockf_type;
  json_t *root, *proc, *sect;
  json_error_t root_err;
  const char *key;
  json_t *val;
  void *p, *s, *e;
  const char *new_value;

  /* init */
  conf_fd = NULL_VOLDES;
  lockf_type = FILEIO_NOT_LOCKF;
  root = NULL;

  if (fp == NULL)
    {
      fp = stdout;
    }

  fprintf (fp, "#\n# %s\n#\n\n", sysprm_auto_conf_file_name);
  fprintf (fp,
	   "# system parameters were loaded from the files ([@section])\n");

  /* STEP 1: load file object from conf file
   */
  if (envvar_confdir_file (file_being_dealt_with, PATH_MAX,
			   sysprm_auto_conf_file_name) == NULL)
    {
      assert (false);
      goto exit_on_error;
    }

  conf_fd = fileio_open (file_being_dealt_with, O_RDONLY, 0);
  if (conf_fd == NULL_VOLDES)
    {
      /* file does not exist */
      assert (error == NO_ERROR);
      goto done;		/* OK */
    }

  error = fileio_get_lock_retry (conf_fd, file_being_dealt_with);
  if (error != NO_ERROR)
    {
      goto exit_on_error;
    }

  lockf_type = FILEIO_LOCKF;

  /* get json object */
  root = json_load_file (file_being_dealt_with, 0, &root_err);
  if (root == NULL)
    {
      char err_msg[ER_MSG_SIZE];

      /* object does not exist */
      if (root_err.line == -1 && root_err.column == -1)
	{
	  assert (error == NO_ERROR);
	  goto done;		/* OK */
	}

      snprintf (err_msg, sizeof (err_msg),
		"json_load_file: file %s, line %d, column %d, %s",
		file_being_dealt_with, root_err.line, root_err.column,
		root_err.text);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, err_msg);
      goto exit_on_error;
    }

  /* STEP 2: iter object
   */
  for (p = json_object_iter (root); p && error == NO_ERROR;
       p = json_object_iter_next (root, p))
    {
      key = json_object_iter_key (p);
      proc = json_object_iter_value (p);

      if (proc_name != NULL && strcmp (key, proc_name) != 0)
	{
	  continue;
	}

      fprintf (fp, "%s:\n", key);	/* proc_name */

      for (s = json_object_iter (proc); s && error == NO_ERROR;
	   s = json_object_iter_next (proc, s))
	{
	  key = json_object_iter_key (s);
	  sect = json_object_iter_value (s);

	  if (sect_name != NULL && strcmp (key, sect_name) != 0)
	    {
	      continue;
	    }

	  fprintf (fp, "\t%s:\n", key);	/* sect_name */

	  for (e = json_object_iter (sect); e && error == NO_ERROR;
	       e = json_object_iter_next (sect, e))
	    {
	      key = json_object_iter_key (e);
	      val = json_object_iter_value (e);

	      new_value = json_string_value (val);

	      fprintf (fp, "\t\t%s: %s\n", key, new_value);	/* key = value */
	    }
	}
    }

  fprintf (fp, "\n");

  if (error != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  (void) json_decref (root);

  if (lockf_type != FILEIO_NOT_LOCKF)
    {
      error = fileio_release_lock (conf_fd);
      assert_release (error == NO_ERROR);
    }

  if (conf_fd != NULL_VOLDES)
    {
      fileio_close (conf_fd);
    }

  return error;

exit_on_error:
  if (error == NO_ERROR)
    {
      error = ER_GENERIC_ERROR;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
			   "sysprm_dump_persist_conf_file:");
    }

  goto done;
}

/*
 * sysprm_set_er_log_file -
 *   return: void
 *   base_db_name(in): database name
 *
 */
void
sysprm_set_er_log_file (const char *db_name)
{
  char *s, *base_db_name;
  char local_db_name[DB_MAX_IDENTIFIER_LENGTH + 1];
  time_t log_time;
  struct tm log_tm, *log_tm_p = &log_tm;
  char error_log_name[PATH_MAX];
  SYSPRM_PARAM *er_log_file;

  if (db_name == NULL)
    {
      return;
    }

  er_log_file = prm_find (PRM_NAME_ER_LOG_FILE, NULL);
  if (er_log_file == NULL || PRM_IS_SET (*er_log_file->dynamic_flag))
    {
      return;
    }

  strncpy (local_db_name, db_name, DB_MAX_IDENTIFIER_LENGTH);
  local_db_name[DB_MAX_IDENTIFIER_LENGTH] = '\0';
  s = strchr (local_db_name, '@');
  if (s)
    {
      *s = '\0';
    }
  base_db_name = basename ((char *) local_db_name);
  if (base_db_name == NULL)
    {
      return;
    }

  log_time = time (NULL);
  log_tm_p = localtime_r (&log_time, &log_tm);
  if (log_tm_p != NULL)
    {
      snprintf (error_log_name, PATH_MAX - 1,
		"%s%c%s_%04d%02d%02d_%02d%02d.err", ER_LOG_FILE_DIR,
		PATH_SEPARATOR, base_db_name, log_tm_p->tm_year + 1900,
		log_tm_p->tm_mon + 1, log_tm_p->tm_mday, log_tm_p->tm_hour,
		log_tm_p->tm_min);
      prm_set (er_log_file, error_log_name, true);
    }
}

/*
 * sysprm_load_and_init_internal - Read system parameters from the init files
 *   return: NO_ERROR or ER_FAILED
 *   db_name(in): database name
 *   reload(in):
 *
 * Note: Parameters would be tuned and forced according to the internal rules.
 */
static int
sysprm_load_and_init_internal (const char *db_name, bool reload)
{
  char *base_db_name = NULL;
  char local_db_name[DB_MAX_IDENTIFIER_LENGTH + 1];
  unsigned int i;
  int r = NO_ERROR;
  char *s;

  if (prm_Def_is_initialized == false)
    {
      sysprm_initialize_prm_def ();
    }

  if (reload)
    {
      for (i = 0; i < NUM_PRM; i++)
	{
	  if (PRM_IS_RELOADABLE (prm_Def[i].static_flag))
	    {
	      if (prm_set_default (&prm_Def[i]) != NO_ERROR)
		{
		  prm_Def[i].value = (void *) NULL;
		}
	    }
	}
    }

  if (db_name == NULL)
    {
      /* initialize message catalog at here because there could be a code path
       * that did not call msgcat_init() before */
      if (msgcat_init () != NO_ERROR)
	{
	  return ER_FAILED;
	}
      base_db_name = NULL;
    }
  else
    {
      strncpy (local_db_name, db_name, DB_MAX_IDENTIFIER_LENGTH);
      local_db_name[DB_MAX_IDENTIFIER_LENGTH] = '\0';
      s = strchr (local_db_name, '@');
      if (s)
	{
	  *s = '\0';
	}
      base_db_name = basename ((char *) local_db_name);
    }

#if !defined (CS_MODE)
  if (base_db_name != NULL && reload == false)
    {
      sysprm_set_er_log_file (base_db_name);
    }
#endif /* !CS_MODE */

#if 1				/* TODO - #955 set PERSIST */
  /*
   * Read installation configuration file - $RYE_DATABASES/rye-auto.conf
   */
  r = prm_read_and_parse_server_persist_conf_file ("common", reload);

  if (r == NO_ERROR && base_db_name != NULL && *base_db_name != '\0')
    {
      char sect_name[LINE_MAX];

      snprintf (sect_name, LINE_MAX, "@%s", base_db_name);
      r = prm_read_and_parse_server_persist_conf_file (sect_name, reload);
    }

  if (r != NO_ERROR)
    {
      return r;
    }
#endif

  /*
   * If a parameter is not given, set it by default
   */
  for (i = 0; i < NUM_PRM; i++)
    {
      if (!PRM_IS_SET (*prm_Def[i].dynamic_flag)
	  && !PRM_IS_OBSOLETED (prm_Def[i].static_flag))
	{
	  if (prm_set_default (&prm_Def[i]) != NO_ERROR)
	    {
	      fprintf (stderr,
		       msgcat_message (MSGCAT_CATALOG_RYE,
				       MSGCAT_SET_PARAMETERS,
				       PRM_ERR_NO_VALUE), prm_Def[i].name);
	      assert (0);
	      return ER_FAILED;
	    }
	}
    }

  /*
   * Perform system parameter check and tuning.
   */
  prm_check_environment ();
  r = prm_tune_parameters ();
  if (r != NO_ERROR)
    {
      return r;
    }

  /*
   * Perform forced system parameter setting.
   */
  for (i = 0; i < DIM (prm_Def); i++)
    {
      if (prm_Def[i].force_value)
	{
	  prm_set (&prm_Def[i], prm_Def[i].force_value, false);
	}
    }

#if 0
  if (envvar_get ("PARAM_DUMP"))
    {
      sysprm_dump_parameters (stdout);
    }
#endif

#if !defined(NDEBUG)
  /* verify flags are not incorrect or confusing */
  for (i = 0; i < NUM_PRM; i++)
    {
      int flag = prm_Def[i].static_flag;
      if (PRM_USER_CAN_CHANGE (flag) && PRM_TEST_CHANGE_ONLY (flag))
	{
	  /* do not set both parameters:
	   * USER_CHANGE: the user can change parameter value on-line
	   * TEST_CHANGE: for QA only
	   */
	  assert (0);
	}
    }
#endif

  return NO_ERROR;
}

/*
 * sysprm_load_and_init - Read system parameters from the init files
 *   return: NO_ERROR or ER_FAILED
 *   db_name(in): database name
 *
 */
int
sysprm_load_and_init (const char *db_name)
{
  return sysprm_load_and_init_internal (db_name, false);
}

/*
 * sysprm_load_and_init_client - Read system parameters from the init files
 *				 (client version)
 *   return: NO_ERROR or ER_FAILED
 *   db_name(in): database name
 *
 */
int
sysprm_load_and_init_client (const char *db_name)
{
  return sysprm_load_and_init_internal (db_name, false);
}

/*
 * sysprm_reload_and_init - Read system parameters from the init files
 *   return: NO_ERROR or ER_FAILED
 *   db_name(in): database name
 *
 */
int
sysprm_reload_and_init (const char *db_name)
{
  return sysprm_load_and_init_internal (db_name, true);
}

/*
 * prm_read_and_parse_server_persist_conf_file - Set server system parameters
 *                                               from a conf file
 *   return: error code
 *   sect_name(in):
 *   reload(in):
 */
int
prm_read_and_parse_server_persist_conf_file (const char *sect_name,
					     const bool reload)
{
  int error = NO_ERROR;
  char file_being_dealt_with[PATH_MAX];
  int conf_fd;
  FILEIO_LOCKF_TYPE lockf_type;
  json_t *root, *proc, *sect;
  json_error_t root_err;
  const char *key;
  json_t *val;
  void *p, *s, *e;
  SYSPRM_PARAM *prm;
  const char *new_value;

  assert (sect_name != NULL);

  /* init */
  conf_fd = NULL_VOLDES;
  lockf_type = FILEIO_NOT_LOCKF;
  root = NULL;

  if (sect_name == NULL)
    {
      goto exit_on_error;
    }

  /* STEP 1: load file object from conf file
   */
  if (envvar_confdir_file (file_being_dealt_with, PATH_MAX,
			   sysprm_auto_conf_file_name) == NULL)
    {
      assert (false);
      goto exit_on_error;
    }

  conf_fd = fileio_open (file_being_dealt_with, O_RDONLY, 0);
  if (conf_fd == NULL_VOLDES)
    {
      /* file does not exist */
      assert (error == NO_ERROR);
      goto done;		/* OK */
    }

  error = fileio_get_lock_retry (conf_fd, file_being_dealt_with);
  if (error != NO_ERROR)
    {
      goto exit_on_error;
    }

  lockf_type = FILEIO_LOCKF;

  /* get json object */
  root = json_load_file (file_being_dealt_with, 0, &root_err);
  if (root == NULL)
    {
      char err_msg[ER_MSG_SIZE];

      /* object does not exist */
      if (root_err.line == -1 && root_err.column == -1)
	{
	  assert (error == NO_ERROR);
	  goto done;		/* OK */
	}

      snprintf (err_msg, sizeof (err_msg),
		"json_load_file: file %s, line %d, column %d, %s",
		file_being_dealt_with, root_err.line, root_err.column,
		root_err.text);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, err_msg);
      goto exit_on_error;
    }

  /* STEP 2: set prm value
   */
  for (p = json_object_iter (root); p && error == NO_ERROR;
       p = json_object_iter_next (root, p))
    {
      key = json_object_iter_key (p);
      proc = json_object_iter_value (p);
#if 0				/* dbg */
      fprintf (stdout, "Key: %s, Value: %s\n", key, json_string_value (proc));
#endif

      if (intl_mbs_casecmp (key, "server") != 0)
	{
	  continue;
	}

      for (s = json_object_iter (proc); s && error == NO_ERROR;
	   s = json_object_iter_next (proc, s))
	{
	  key = json_object_iter_key (s);
	  sect = json_object_iter_value (s);
#if 0				/* dbg */
	  fprintf (stdout, "Key: %s, Value: %s\n", key,
		   json_string_value (sect));
#endif

	  if (intl_mbs_casecmp (key, sect_name) != 0)
	    {
	      continue;
	    }

	  for (e = json_object_iter (sect); e && error == NO_ERROR;
	       e = json_object_iter_next (sect, e))
	    {
	      key = json_object_iter_key (e);
	      val = json_object_iter_value (e);

	      new_value = json_string_value (val);

#if 0				/* dbg */
	      fprintf (stdout, "Key: %s, Value: %s\n", key, new_value);
#endif

	      prm = prm_find (key, NULL);
	      if (prm == NULL)
		{
		  assert (false);
		  error = PRM_ERR_UNKNOWN_PARAM;
		  goto exit_on_error;
		}

	      if (reload && !PRM_IS_RELOADABLE (prm->static_flag))
		{
		  continue;
		}

	      if (PRM_IS_OBSOLETED (prm->static_flag))
		{
		  continue;
		}

	      if (PRM_IS_DEPRECATED (prm->static_flag))
		{
		  prm_report_bad_entry (key, -1,
					PRM_ERR_DEPRICATED,
					file_being_dealt_with);
		}

	      error = prm_set (prm, new_value, true);
	      if (error != NO_ERROR)
		{
		  prm_report_bad_entry (key, -1, error,
					file_being_dealt_with);
		  goto exit_on_error;
		}
	    }
	}
    }
#if 0				/* dbg */
  fflush (stdout);
#endif

  if (error != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  (void) json_decref (root);

  if (lockf_type != FILEIO_NOT_LOCKF)
    {
      error = fileio_release_lock (conf_fd);
      assert_release (error == NO_ERROR);
    }

  if (conf_fd != NULL_VOLDES)
    {
      fileio_close (conf_fd);
    }

  return error;

exit_on_error:
  if (error == NO_ERROR)
    {
      error = ER_GENERIC_ERROR;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
			   "prm_read_and_parse_server_persist_conf_file:");
    }

  goto done;
}

/*
 * prm_read_and_parse_broker_persist_conf_file - Set broker system parameters
 *                                               from a conf file
 *   return: error code
 *   ini(in):
 */
int
prm_read_and_parse_broker_persist_conf_file (INI_TABLE * ini)
{
  int error = NO_ERROR;
  char file_being_dealt_with[PATH_MAX];
  int conf_fd;
  FILEIO_LOCKF_TYPE lockf_type;
  json_t *root, *proc, *sect;
  json_error_t root_err;
  const char *key;
  json_t *val;
  void *p, *s, *e;
  int lineno = 0;
  char sect_name[LINE_MAX];
  char key_name[LINE_MAX];

  assert (ini != NULL);

  /* init */
  conf_fd = NULL_VOLDES;
  lockf_type = FILEIO_NOT_LOCKF;
  root = NULL;

  if (ini == NULL)
    {
      goto exit_on_error;
    }

  /* STEP 1: load file object from conf file
   */
  if (envvar_confdir_file (file_being_dealt_with, PATH_MAX,
			   sysprm_auto_conf_file_name) == NULL)
    {
      assert (false);
      goto exit_on_error;
    }

  conf_fd = fileio_open (file_being_dealt_with, O_RDONLY, 0);
  if (conf_fd == NULL_VOLDES)
    {
      /* file does not exist */
      assert (error == NO_ERROR);
      goto done;		/* OK */
    }

  error = fileio_get_lock_retry (conf_fd, file_being_dealt_with);
  if (error != NO_ERROR)
    {
      goto exit_on_error;
    }

  lockf_type = FILEIO_LOCKF;

  /* get json object */
  root = json_load_file (file_being_dealt_with, 0, &root_err);
  if (root == NULL)
    {
      char err_msg[ER_MSG_SIZE];

      /* object does not exist */
      if (root_err.line == -1 && root_err.column == -1)
	{
	  assert (error == NO_ERROR);
	  goto done;		/* OK */
	}

      snprintf (err_msg, sizeof (err_msg),
		"json_load_file: file %s, line %d, column %d, %s",
		file_being_dealt_with, root_err.line, root_err.column,
		root_err.text);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, err_msg);
      goto exit_on_error;
    }

  /* STEP 2: set prm value
   */
  for (p = json_object_iter (root); p && error == NO_ERROR;
       p = json_object_iter_next (root, p))
    {
      lineno++;
      key = json_object_iter_key (p);
      proc = json_object_iter_value (p);

      if (intl_mbs_casecmp (key, "broker") != 0)
	{
	  continue;
	}

#if 0				/* dbg */
      fprintf (stdout, "Key: %s, Value: %s\n", key, json_string_value (proc));
#endif

      for (s = json_object_iter (proc); s && error == NO_ERROR;
	   s = json_object_iter_next (proc, s))
	{
	  lineno++;
	  key = json_object_iter_key (s);
	  sect = json_object_iter_value (s);
#if 0				/* dbg */
	  fprintf (stdout, "\tKey: %s, Value: %s\n", key,
		   json_string_value (sect));
#endif

	  snprintf (sect_name, LINE_MAX - 1, "%s", ini_str_lower (key));
	  if (ini_table_set (ini, sect_name, NULL, lineno) < 0)
	    {
	      assert (false);
	      goto exit_on_error;
	    }

	  for (e = json_object_iter (sect); e && error == NO_ERROR;
	       e = json_object_iter_next (sect, e))
	    {
	      lineno++;
	      key = json_object_iter_key (e);
	      val = json_object_iter_value (e);
#if 0				/* dbg */
	      fprintf (stdout, "\t\tKey: %s, Value: %s\n", key,
		       json_string_value (val));
#endif

	      snprintf (key_name, LINE_MAX - 1, "%s:%s", sect_name,
			ini_str_lower (ini_str_trim (key)));
	      if (ini_table_set (ini, key_name,
				 json_string_value (val), lineno) < 0)
		{
		  assert (false);
		  goto exit_on_error;
		}
	    }
	}
    }
#if 0				/* dbg */
  fflush (stdout);
#endif

  if (error != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  (void) json_decref (root);

  if (lockf_type != FILEIO_NOT_LOCKF)
    {
      error = fileio_release_lock (conf_fd);
      assert_release (error == NO_ERROR);
    }

  if (conf_fd != NULL_VOLDES)
    {
      fileio_close (conf_fd);
    }

  return error;

exit_on_error:
  if (error == NO_ERROR)
    {
      error = ER_GENERIC_ERROR;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
			   "prm_read_and_parse_broker_persist_conf_file:");
    }

  goto done;
}

/*
 * prm_check_environment -
 *   return: none
 */
static void
prm_check_environment (void)
{
  int i;
  char buf[PRM_DEFAULT_BUFFER_SIZE];

  if (prm_Def_is_initialized == false)
    {
      sysprm_initialize_prm_def ();
    }

  for (i = 0; i < NUM_PRM; i++)
    {
      SYSPRM_PARAM *prm;
      const char *str;

      prm = &prm_Def[i];
      strncpy (buf, prm->name, sizeof (buf) - 1);
      buf[sizeof (buf) - 1] = '\0';
      ustr_upper (buf);

      str = envvar_get (buf);
      if (str && str[0])
	{
	  int error;
	  error = prm_set (prm, str, true);
	  if (error != 0)
	    {
	      prm_report_bad_entry (prm->name, -1, error, buf);
	    }
	}
    }
}

#if !defined (SERVER_MODE)
/*
 * sysprm_validate_change_parameters () - validate the parameter value changes
 *
 * return		 : SYSPRM_ERR
 * data (in)		 : string containing "parameter = value" assignments
 * persist (in)          :
 * check (in)		 : check if user can change parameter and if
 *			   parameter should also change on server. set to
 *			   false if assignments are supposed to be forced and
 *			   not checked.
 * assignments_ptr (out) : list of assignments.
 *
 * NOTE: Data string is parsed entirely and if all changes are valid a list
 *	 of SYSPRM_ASSIGN_VALUEs is generated. If any change is invalid an
 *	 error is returned and no list is generated.
 *	 If changes need to be done on server too PRM_ERR_NOT_FOR_CLIENT or
 *	 PRM_ERR_NOT_FOR_CLIENT_NO_AUTH is returned.
 */
SYSPRM_ERR
sysprm_validate_change_parameters (const char *data, const bool persist,
				   const bool check,
				   SYSPRM_ASSIGN_VALUE ** assignments_ptr)
{
  char buf[LINE_MAX], *p = NULL, *name = NULL, *value = NULL;
  SYSPRM_PARAM *prm = NULL;
  SYSPRM_ERR err = PRM_ERR_NO_ERROR;
  SYSPRM_ASSIGN_VALUE *assignments = NULL, *last_assign = NULL;
  SYSPRM_ERR change_error = PRM_ERR_NO_ERROR;

  assert (assignments_ptr != NULL);
  *assignments_ptr = NULL;

  if (!data || *data == '\0')
    {
      return PRM_ERR_BAD_VALUE;
    }

  if (intl_mbs_ncpy (buf, data, sizeof (buf)) == NULL)
    {
      return PRM_ERR_BAD_VALUE;
    }

  p = buf;
  do
    {
      /* parse data */
      SYSPRM_ASSIGN_VALUE *assign = NULL;

      /* get parameter name and value */
      err = prm_get_next_param_value (&p, &name, &value);
      if (err != PRM_ERR_NO_ERROR || name == NULL || value == NULL)
	{
	  break;
	}

      prm = prm_find (name, NULL);
      if (prm == NULL)
	{
	  err = PRM_ERR_UNKNOWN_PARAM;
	  break;
	}

#if 0				/* TODO - #955 set PERSIST; need more consideration */
      if (!check
	  || PRM_USER_CAN_CHANGE (prm->static_flag)
	  || (PRM_TEST_CHANGE_ONLY (prm->static_flag) && PRM_TEST_MODE))
	{
	  /* We allow changing the parameter value. */
	}
      else
	{
	  err = PRM_ERR_CANNOT_CHANGE;
	  break;
	}
#endif

      /* create a SYSPRM_CHANGE_VAL object */
      assign = (SYSPRM_ASSIGN_VALUE *) malloc (sizeof (SYSPRM_ASSIGN_VALUE));
      if (assign == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, sizeof (SYSPRM_ASSIGN_VALUE));
	  err = PRM_ERR_NO_MEM_FOR_PRM;
	  break;
	}
      err = sysprm_generate_new_value (prm, value, check, &assign->value);
      if (err != PRM_ERR_NO_ERROR)
	{
	  if (err == PRM_ERR_NOT_FOR_CLIENT
	      || err == PRM_ERR_NOT_FOR_CLIENT_NO_AUTH)
	    {
	      /* update change_error */
	      if (change_error != PRM_ERR_NOT_FOR_CLIENT
		  || err != PRM_ERR_NOT_FOR_CLIENT_NO_AUTH)
		{
		  /* do not replace change_error PRM_ERR_NOT_FOR_CLIENT with
		   * PRM_ERR_NOT_FOR_CLIENT_NO_AUTH
		   */
		  change_error = err;
		}
	      /* do not invalidate assignments */
	      err = PRM_ERR_NO_ERROR;
	    }
	  else
	    {
	      /* bad value */
	      free_and_init (assign);
	      break;
	    }
	}
      assign->prm_id = prm->param_id;
      assign->persist = persist;
      assign->next = NULL;

      /* append to assignments list */
      if (assignments != NULL)
	{
	  last_assign->next = assign;
	  last_assign = assign;
	}
      else
	{
	  assignments = last_assign = assign;
	}
    }
  while (p);

  if (err == PRM_ERR_NO_ERROR)
    {
      /* changes are valid, save assignments list */
      *assignments_ptr = assignments;

      /* return change_error in order to update values on server too */
      return change_error;
    }

  /* changes are not valid, clean up */
  sysprm_free_assign_values (&assignments);
  return err;
}
#endif /* !SERVER_MODE */

/*
 * sysprm_change_persist_conf_file () - update system parameter value
 *                                      into $RYE_DATABASES/rye-auto.conf
 *                                      or delete sections of the conf file
 *
 * return           : error code
 * proc_name (in)   : process name
 * sec_name (in)    : section name
 * key (in)         :
 * value (in)       :
 *
 * NOTE: json format
 * {
 *   server: {
 *     sec1: {
 *       key1: val1,
 *       key2: val2
 *     },
 *     sec2: {
 *       key1: val1
 *     }
 *   },
 *   broker: {
 *     sec1: {
 *       key1: val1,
 *       key2: val2
 *     },
 *     sec2: {
 *       key1: val1
 *     }
 *   }
 * }
 *
 * example:
 * {
 *   "server": {
 *     "common": {
 *       "error_log_level": "error",
 *       "er_log_debug": "y",
 *       "ha_node_list": "group@11.222.333.444:hostname2"
 *     }
 *   }
 * }
 */
int
sysprm_change_persist_conf_file (const char *proc_name, const char *sect_name,
				 const char *key, const char *value)
{
  int error = NO_ERROR;
  char file_being_dealt_with[PATH_MAX];
  int conf_fd;
  FILEIO_LOCKF_TYPE lockf_type;
  json_t *root, *proc, *sect;
  json_error_t root_err;
  SYSPRM_OP op;

  assert (proc_name != NULL);

  assert ((sect_name != NULL && key != NULL && value != NULL)
	  || (value == NULL));

  /* init */
  conf_fd = NULL_VOLDES;
  lockf_type = FILEIO_NOT_LOCKF;
  root = proc = sect = NULL;
  op = SYSPRM_UPD_KEY;		/* guess */

  if (proc_name == NULL)
    {
      goto exit_on_error;
    }

  if (sect_name != NULL && key != NULL && value != NULL)
    {
      op = SYSPRM_UPD_KEY;
    }
  else
    {
      assert (value == NULL);
      if (value != NULL)
	{
	  assert (false);
	  goto exit_on_error;
	}

      if (sect_name == NULL && key == NULL)
	{
	  op = SYSPRM_DEL_PROC;
	}
      else if (key == NULL)
	{
	  assert (sect_name != NULL);
	  op = SYSPRM_DEL_SECT;
	}
      else
	{
	  assert (sect_name != NULL);
	  assert (key != NULL);
	  op = SYSPRM_DEL_KEY;
	}
    }

  /* STEP 1: load file object from conf file
   */
  if (envvar_confdir_file (file_being_dealt_with, PATH_MAX,
			   sysprm_auto_conf_file_name) == NULL)
    {
      assert (false);
      goto exit_on_error;
    }

  conf_fd = fileio_open (file_being_dealt_with, O_RDWR | O_CREAT, 0644);
  if (conf_fd == NULL_VOLDES)
    {
      error = ER_IO_MOUNT_FAIL;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   error, 1, file_being_dealt_with);
      goto exit_on_error;
    }

  lockf_type =
    fileio_lock ("db-name" /* TODO */ , file_being_dealt_with, conf_fd,
		 true);
  if (lockf_type == FILEIO_NOT_LOCKF)
    {
      /* Volume seems to be mounted by someone else */
      error = er_errid ();
      assert (error != NO_ERROR);
      goto exit_on_error;
    }

  /* get json object */
  root = json_load_file (file_being_dealt_with, 0, &root_err);
  if (root == NULL)
    {
      char err_msg[ER_MSG_SIZE];

      /* object does not exist; is O_RDWR | O_CREAT */
      if (root_err.line == 1 && root_err.column == 0)
	{
	  root = json_object ();	/* create new object */
	  if (root == NULL)
	    {
	      assert (false);
	      goto exit_on_error;
	    }
	}
      else
	{
	  snprintf (err_msg, sizeof (err_msg),
		    "json_load_file: file %s, line %d, column %d, %s",
		    file_being_dealt_with, root_err.line, root_err.column,
		    root_err.text);

	  error = ER_GENERIC_ERROR;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, err_msg);
	  goto exit_on_error;
	}
    }
  assert (root != NULL);

  /* get process object */
  if (op != SYSPRM_DEL_PROC)
    {
      proc = json_object_get (root, proc_name);
      if (proc == NULL)
	{
	  proc = json_object ();
	  if (proc == NULL)
	    {
	      assert (false);
	      goto exit_on_error;
	    }
	  assert (proc != NULL);

	  if (json_object_set_new (root, proc_name, proc) != 0)
	    {
	      assert (false);
	      goto exit_on_error;
	    }
	}

      /* get section object */
      if (op != SYSPRM_DEL_SECT)
	{
	  sect = json_object_get (proc, sect_name);
	  if (sect == NULL)
	    {
	      sect = json_object ();
	      if (sect == NULL)
		{
		  assert (false);
		  goto exit_on_error;
		}
	      assert (sect != NULL);

	      if (json_object_set_new (proc, sect_name, sect) != 0)
		{
		  assert (false);
		  goto exit_on_error;
		}
	    }
	}
    }

  /* STEP 2: write new key or delete sections
   */
  assert (root != NULL);

  switch (op)
    {
    case SYSPRM_UPD_KEY:	/* update key */
      assert (proc != NULL);
      assert (sect != NULL);

      if (json_object_set_new (sect, key, json_string (value)) != 0)
	{
	  assert (false);
	  goto exit_on_error;
	}
      break;

    case SYSPRM_DEL_PROC:	/* delete process */
      assert (proc == NULL);
      assert (sect == NULL);

      (void) json_object_del (root, proc_name);
      break;

    case SYSPRM_DEL_SECT:	/* delete section */
      assert (proc != NULL);
      assert (sect == NULL);

      (void) json_object_del (proc, sect_name);
      break;

    case SYSPRM_DEL_KEY:	/* delete key */
      assert (proc != NULL);
      assert (sect != NULL);

      (void) json_object_del (sect, key);
      break;

    default:
      assert (false);		/* is impossible */
      goto exit_on_error;
      break;
    }

  /* STEP 3: write file object into conf file
   */
  if (json_dump_file (root, file_being_dealt_with,
		      JSON_INDENT (2) | JSON_PRESERVE_ORDER) != 0)
    {
      assert (false);
      goto exit_on_error;
    }

  if (error != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  (void) json_decref (root);

  if (lockf_type != FILEIO_NOT_LOCKF)
    {
      fileio_unlock (file_being_dealt_with, conf_fd, lockf_type);
    }

  if (conf_fd != NULL_VOLDES)
    {
      fileio_close (conf_fd);
    }

  return error;

exit_on_error:
  if (error == NO_ERROR)
    {
      error = ER_GENERIC_ERROR;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
			   "sysprm_change_persist_conf_file:");
    }

  goto done;
}

int
sysprm_get_persist_conf (char *value, int max_size,
			 const char *proc_name, const char *sect_name,
			 const char *key_name)
{
  int error = NO_ERROR;
  char file_being_dealt_with[PATH_MAX];
  json_t *root = NULL;
  const json_t *proc, *sect, *key;
  json_error_t root_err;
  int conf_fd = NULL_VOLDES;
  FILEIO_LOCKF_TYPE lockf_type = FILEIO_NOT_LOCKF;

  if (proc_name == NULL || sect_name == NULL || key_name == NULL)
    {
      goto exit_on_error;
    }

  /* load file object from conf file */
  if (envvar_confdir_file (file_being_dealt_with, PATH_MAX,
			   sysprm_auto_conf_file_name) == NULL)
    {
      assert (false);
      goto exit_on_error;
    }

  conf_fd = fileio_open (file_being_dealt_with, O_RDWR | O_CREAT, 0644);
  if (conf_fd == NULL_VOLDES)
    {
      error = ER_IO_MOUNT_FAIL;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   error, 1, file_being_dealt_with);
      goto exit_on_error;
    }

  lockf_type = fileio_lock ("db-name" /* TODO */ ,
			    file_being_dealt_with, conf_fd, true);
  if (lockf_type == FILEIO_NOT_LOCKF)
    {
      /* Volume seems to be mounted by someone else */
      error = er_errid ();
      assert (error != NO_ERROR);
      goto exit_on_error;
    }

  /* get json object */
  root = json_load_file (file_being_dealt_with, 0, &root_err);
  if (root == NULL)
    {
      goto exit_on_error;
    }

  /* get process object */
  proc = json_object_get (root, proc_name);
  if (proc == NULL)
    {
      goto exit_on_error;
    }

  /* get section object */
  sect = json_object_get (proc, sect_name);
  if (sect == NULL)
    {
      goto exit_on_error;
    }

  /* get key object */
  key = json_object_get (sect, key_name);
  if (key == NULL)
    {
      goto exit_on_error;
    }

  strncpy (value, json_string_value (key), max_size);
  value[max_size - 1] = '\0';

done:
  (void) json_decref (root);

  if (lockf_type != FILEIO_NOT_LOCKF)
    {
      fileio_unlock (file_being_dealt_with, conf_fd, lockf_type);
    }

  if (conf_fd != NULL_VOLDES)
    {
      fileio_close (conf_fd);
    }

  return error;

exit_on_error:
  if (error == NO_ERROR)
    {
      error = ER_GENERIC_ERROR;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
			   "sysprm_get_persist_conf:");
    }

  goto done;
}

/*
 * sysprm_change_parameter_values () - update system parameter values
 *
 * return	    : error code
 * assignments (in) : list of assignments
 * check (in)	    : check if the parameter belongs to current scope
 * set_flag (in)    : update PRM_SET flag if true
 *
 * NOTE: This function does not check if the new values are valid (e.g. in
 *	 the restricted range). First validate new values before calling this
 *	 function.
 */
int
sysprm_change_parameter_values (const SYSPRM_ASSIGN_VALUE * assignments,
				bool check, bool set_flag)
{
  int error = NO_ERROR;
  const SYSPRM_ASSIGN_VALUE *av;
  SYSPRM_PARAM *prm = NULL;
  char buf[LINE_MAX];

  for (av = assignments; av != NULL && error == NO_ERROR; av = av->next)
    {
      prm = GET_PRM (av->prm_id);

      if (check)
	{
#if 1				/* TODO - #955 set PERSIST; need more consideration */
	  if (av->persist == false)
#endif
	    {
#if defined (CS_MODE)
	      if (!PRM_IS_FOR_CLIENT (prm->static_flag))
		{
		  /* skip this assignment */
		  continue;
		}
#endif
#if defined (SERVER_MODE)
	      if (!PRM_IS_FOR_SERVER (prm->static_flag))
		{
		  /* skip this assignment */
		  continue;
		}
#endif
	    }
	}

      if (!check
#if 1				/* TODO - #955 set PERSIST; need more consideration */
	  || av->persist
#endif
	  || PRM_USER_CAN_CHANGE (prm->static_flag)
	  || (PRM_TEST_CHANGE_ONLY (prm->static_flag) && PRM_TEST_MODE))
	{
	  /* We allow changing the parameter value. */
	  sysprm_set_value (prm, av->value, set_flag, true);


#if defined (SERVER_MODE)
	  if (prm->param_id == PRM_ID_ACCESS_IP_CONTROL)
	    {
	      css_set_accessible_ip_info ();
	    }
#endif
	}

#if defined (SERVER_MODE)
      assert (av->persist == false);	/* is not sent to server */
#endif

#if 1				/* TODO - #955 set PERSIST */
      if (av->persist)
	{
	  /* compare current value with default value and update iff different */
	  if (sysprm_compare_values (prm->value, prm->default_value,
				     prm->datatype) != 0)
	    {
	      prm_print (prm, buf, LINE_MAX, PRM_PRINT_NONE,
			 PRM_PRINT_CURR_VAL);

	      error = sysprm_change_persist_conf_file ("server", "common",
						       prm->name, buf);
	    }
	  else
	    {
	      /* delete default key */
	      error = sysprm_change_persist_conf_file ("server", "common",
						       prm->name, NULL);
	    }
	}
#endif
    }

  return error;
}

/*
 * prm_print_value - Print a parameter value to the buffer
 *   return: number of chars printed
 *   prm(in): parameter
 *   prm_value(in): value
 *   buf(out): print buffer
 *   len(in): length of the buffer
 */
static int
prm_print_value (const SYSPRM_PARAM * prm, void *prm_value, char *buf,
		 size_t len)
{
  int error = NO_ERROR;
  char value_string[PRM_DEFAULT_BUFFER_SIZE];
  char *value = NULL;
  int n = 0;

  if (len == 0)
    {
      /* don't print anything */
      return 0;
    }

  assert (prm != NULL);
  assert (buf != NULL);
  assert (len > 0);

  switch (prm->datatype)
    {
    case PRM_INTEGER:
      {
	int val = PRM_GET_INT (prm_value);

	error = prm_print_int_value (value_string, sizeof (value_string),
				     val, prm);
	if (error != NO_ERROR)
	  {
	    return error;
	  }
	value = value_string;
      }
      break;
    case PRM_BIGINT:
      {
	INT64 val = PRM_GET_BIGINT (prm_value);

	error = prm_print_bigint_value (value_string, sizeof (value_string),
					val, prm);
	if (error != NO_ERROR)
	  {
	    return error;
	  }
	value = value_string;
      }
      break;
    case PRM_BOOLEAN:
      {
	bool val;

	val = PRM_GET_BOOL (prm_value);

	error = prm_print_boolean_value (value_string, sizeof (value_string),
					 val, prm);
	if (error != NO_ERROR)
	  {
	    return error;
	  }
	value = value_string;
      }
      break;
    case PRM_FLOAT:
      {
	float val;

	val = PRM_GET_FLOAT (prm_value);

	error = prm_print_float_value (value_string, sizeof (value_string),
				       val, prm);
	if (error != NO_ERROR)
	  {
	    return error;
	  }
	value = value_string;
      }
      break;
    case PRM_STRING:
      {
	n = snprintf (value_string, sizeof (value_string), "%s",
		      (PRM_GET_STRING (prm_value) ? PRM_GET_STRING (prm_value)
		       : ""));
	if (n < 0)
	  {
	    error = ER_GENERIC_ERROR;
	    er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
				 1, "invalid buffer size");
	    return error;
	  }
	value = value_string;
      }
      break;
    case PRM_KEYWORD:
      {
	int val;

	val = PRM_GET_INT (prm_value);

	error = prm_print_keyword_value (value_string, sizeof (value_string),
					 val, prm);
	if (error != NO_ERROR)
	  {
	    return error;
	  }
	value = value_string;
      }
      break;
    case PRM_INTEGER_LIST:
      {
	int *val;

	val = PRM_GET_INTEGER_LIST (prm_value);

	error = prm_print_int_list_value (value_string, sizeof (value_string),
					  val, prm);
	if (error != NO_ERROR)
	  {
	    return error;
	  }
	value = value_string;
      }
      break;
    default:
      {
	value = NULL;
      }
      break;
    }

  if (value != NULL)
    {
      n = snprintf (buf, len, "%s", value_string);
    }
  else
    {

    }

  return n;
}

/*
 * prm_print - Print a parameter to the buffer
 *   return: number of chars printed
 *   prm(in): parameter
 *   buf(out): print buffer
 *   len(in): length of the buffer
 *   print_mode(in): print name/id or just value of the parameter
 */
static int
prm_print (const SYSPRM_PARAM * prm, char *buf, size_t len,
	   PRM_PRINT_MODE print_mode, PRM_PRINT_VALUE_MODE print_value_mode)
{
  int n = 0;
  char left_side[PRM_DEFAULT_BUFFER_SIZE];
  char value_string[PRM_DEFAULT_BUFFER_SIZE];
  const char *prm_flag = "";
  void *prm_value;

  if (len == 0)
    {
      /* don't print anything */
      return 0;
    }

  memset (left_side, 0, PRM_DEFAULT_BUFFER_SIZE);

  assert (prm != NULL);
  assert (buf != NULL);
  assert (len > 0);

  if (PRM_IS_FOR_SERVER (prm->static_flag)
      && PRM_IS_FOR_CLIENT (prm->static_flag))
    {
      prm_flag = "SERVER|CLIENT";
    }
  else if (PRM_IS_FOR_SERVER (prm->static_flag))
    {
      prm_flag = "SERVER";
    }
  else if (PRM_IS_FOR_CLIENT (prm->static_flag))
    {
      prm_flag = "CLIENT";
    }

  if (print_mode == PRM_PRINT_ID)
    {
      snprintf (left_side, PRM_DEFAULT_BUFFER_SIZE, "%d(%s)=", prm->param_id,
		prm_flag);
    }
  else if (print_mode == PRM_PRINT_NAME)
    {
      snprintf (left_side, PRM_DEFAULT_BUFFER_SIZE, "%s(%s)=",
		prm->name, prm_flag);
    }

  if (print_value_mode == PRM_PRINT_DEFAULT_VAL)
    {
      prm_value = prm->default_value;
    }
  else
    {
      prm_value = prm->value;
    }

  n = prm_print_value (prm, prm_value, value_string,
		       len - strlen (left_side));
  if (n > 0)
    {
      n = snprintf (buf, len, "%s%s", left_side, value_string);
    }
  else
    {

    }

  return n;
}

/*
 * prm_print_int_value ()
 *    return: error code or string length
 *
 *    buf(out):
 *    len(in):
 *    val(in):
 *    prm(in):
 */
static int
prm_print_int_value (char *buf, size_t len, int val, const SYSPRM_PARAM * prm)
{
  int error = NO_ERROR;
  int n = 0;

  assert (buf != NULL);
  assert (len > 0);
  assert (PRM_IS_INTEGER (prm));

  if (PRM_HAS_SIZE_UNIT (prm->static_flag)
      || PRM_HAS_TIME_UNIT (prm->static_flag))
    {
      assert (false);
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_GENERIC_ERROR, 1, "prm: invalid data type");

      return ER_GENERIC_ERROR;
    }

  n = snprintf (buf, len, "%d", val);
  if (n < 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_GENERIC_ERROR, 1, "invalid buffer length");
      return ER_GENERIC_ERROR;
    }
  buf[len - 1] = '\0';

  assert (error == NO_ERROR);
  return error;
}

/*
 * prm_print_bigint_value ()
 *    return:
 *
 *    buf(out):
 *    len(in):
 *    val(in):
 *    prm(in):
 */
static int
prm_print_bigint_value (char *buf, size_t len, INT64 val,
			const SYSPRM_PARAM * prm)
{
  int error = NO_ERROR;
  int n;

  assert (buf != NULL);
  assert (len > 0);
  assert (PRM_IS_BIGINT (prm));

  if (PRM_HAS_SIZE_UNIT (prm->static_flag))
    {
      error = util_byte_to_size_string (buf, len, val);
      if (error != NO_ERROR)
	{
	  return error;
	}
    }
  else if (PRM_HAS_TIME_UNIT (prm->static_flag))
    {
      error = util_msec_to_time_string (buf, len, val);
      if (error != NO_ERROR)
	{
	  return error;
	}
    }
  else
    {
      n = snprintf (buf, len, "%ld", val);
      if (n < 0)
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_GENERIC_ERROR, 1, "invalid buffer length");
	  return ER_GENERIC_ERROR;
	}
    }

  assert (error == NO_ERROR);
  return error;
}

/*
 * prm_print_boolean_value ()
 *    return:
 *
 *    buf(out):
 *    len(in):
 *    val(in):
 *    prm(in):
 */
static int
prm_print_boolean_value (char *buf, size_t len, bool val,
			 const SYSPRM_PARAM * prm)
{
  int error = NO_ERROR;
  int n;

  assert (buf != NULL);
  assert (len > 0);
  assert (PRM_IS_BOOLEAN (prm));

  if (val == true)
    {
      n = snprintf (buf, len, "%c", 'y');
    }
  else
    {
      n = snprintf (buf, len, "%c", 'n');
    }

  if (n < 0)
    {
      error = ER_GENERIC_ERROR;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   error, 1, "invalid buffer length");
      return error;
    }

  assert (error == NO_ERROR);
  return error;
}

/*
 * prm_print_float_value ()
 *    return:
 *
 *    buf(out):
 *    len(in):
 *    val(in):
 *    prm(in):
 */
static int
prm_print_float_value (char *buf, size_t len, float val,
		       const SYSPRM_PARAM * prm)
{
  int error = NO_ERROR;
  int n = 0;

  assert (buf != NULL);
  assert (len > 0);
  assert (PRM_IS_FLOAT (prm));

  if (PRM_HAS_SIZE_UNIT (prm->static_flag)
      || PRM_HAS_TIME_UNIT (prm->static_flag))
    {
      assert (false);
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_GENERIC_ERROR, 1, "prm: invalid data type");

      return ER_GENERIC_ERROR;
    }

  n = snprintf (buf, len, "%f", val);
  if (n < 0)
    {
      error = ER_GENERIC_ERROR;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   error, 1, "invalid buffer length");
      return error;
    }

  assert (error == NO_ERROR);
  return error;
}

/*
 * prm_print_keyword_value ()
 *    return:
 *
 *    buf(out):
 *    len(in):
 *    val(in):
 *    prm(in):
 */
static int
prm_print_keyword_value (char *buf, size_t len, int val,
			 const SYSPRM_PARAM * prm)
{
  int error = NO_ERROR;
  int n;
  const KEYVAL *keyvalp = NULL;

  assert (buf != NULL);
  assert (len > 0);
  assert (PRM_IS_KEYWORD (prm));


  if (intl_mbs_casecmp (prm->name, PRM_NAME_ER_LOG_LEVEL) == 0)
    {
      keyvalp = prm_keyword (val, NULL, er_log_level_words,
			     DIM (er_log_level_words));
    }
  else if (intl_mbs_casecmp (prm->name,
			     PRM_NAME_PB_DEBUG_PAGE_VALIDATION_LEVEL) == 0)
    {
      keyvalp = prm_keyword (val, NULL,
			     pgbuf_debug_page_validation_level_words,
			     DIM (pgbuf_debug_page_validation_level_words));
    }
  else if (intl_mbs_casecmp (prm->name, PRM_NAME_HA_MODE) == 0)

    {
      keyvalp = prm_keyword (val, NULL, ha_mode_words, DIM (ha_mode_words));
    }
  else if (intl_mbs_casecmp (prm->name, PRM_NAME_CHECK_PEER_ALIVE) == 0)
    {
      keyvalp = prm_keyword (val, NULL, check_peer_alive_words,
			     DIM (check_peer_alive_words));
    }
  else if (intl_mbs_casecmp (prm->name, PRM_NAME_QUERY_TRACE_FORMAT) == 0)
    {
      keyvalp = prm_keyword (val, NULL, query_trace_format_words,
			     DIM (query_trace_format_words));
    }
  else if (intl_mbs_casecmp (prm->name, PRM_NAME_HA_REPL_FILTER_TYPE) == 0)
    {
      keyvalp = prm_keyword (val, NULL, ha_repl_filter_type_words,
			     DIM (ha_repl_filter_type_words));
    }
  else
    {
      assert (false);
    }

  if (keyvalp)
    {
      n = snprintf (buf, len, "%s", keyvalp->key);
    }
  else
    {				/* is dead-code */
      n = snprintf (buf, len, "%d", val);
    }

  if (n < 0)
    {
      error = ER_GENERIC_ERROR;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   error, 1, "invalid buffer length");
      return error;
    }

  assert (error == NO_ERROR);
  return error;
}

/*
 * prm_print_int_list_value ()
 *    return:
 *
 *    buf(out):
 *    len(in):
 *    val(in):
 *    prm(in):
 */
static int
prm_print_int_list_value (char *buf, size_t len, int *val,
			  const SYSPRM_PARAM * prm)
{
  int error = NO_ERROR;
  int n;
  int list_size, i;
  char *s;

  assert (buf != NULL);
  assert (len > 0);
  assert (PRM_IS_INTEGER_LIST (prm));


  if (val == NULL)
    {
      buf[0] = '\0';
    }
  else
    {
      list_size = val[0];

      for (s = buf, i = 1; i <= list_size; i++)
	{
	  if (i != 1)
	    {
	      n = snprintf (s, len, ",");
	      if (n < 0)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	      s += n;
	      len -= n;
	    }

	  n = snprintf (s, len, "%d", val[i]);
	  if (n < 0)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	  s += n;
	  len -= n;
	}
    }

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  error = ER_GENERIC_ERROR;
  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		       error, 1, "invalid buffer length");
  return error;
}

/*
 * sysprm_print_sysprm_value () - print sysprm_value
 *
 * return	   : length of printed string
 * prm_id (in)	   : parameter ID (to which sysprm_value belongs).
 * value (in)	   : printed sysprm_value
 * buf (in/out)	   : printing destination
 * len (in)	   : maximum size of printed string
 * print_mode (in) : PRM_PRINT_MODE
 */
static int
sysprm_print_sysprm_value (PARAM_ID prm_id, SYSPRM_VALUE value, char *buf,
			   size_t len, PRM_PRINT_MODE print_mode)
{
  int n = 0;
  int error = NO_ERROR;
  char left_side[PRM_DEFAULT_BUFFER_SIZE];
  char val1_buffer[PRM_DEFAULT_BUFFER_SIZE];
  char val2_buffer[PRM_DEFAULT_BUFFER_SIZE];
  char *val1_string = NULL, *val2_string = NULL;
  const char *prm_flag = "";
  SYSPRM_PARAM *prm = NULL;

  if (len == 0)
    {
      /* don't print anything */
      return 0;
    }

  assert (buf != NULL && len > 0);

  prm = GET_PRM (prm_id);

  if (PRM_IS_FOR_SERVER (prm->static_flag)
      && PRM_IS_FOR_CLIENT (prm->static_flag))
    {
      prm_flag = "SERVER|CLIENT";
    }
  else if (PRM_IS_FOR_SERVER (prm->static_flag))
    {
      prm_flag = "SERVER";
    }
  else if (PRM_IS_FOR_CLIENT (prm->static_flag))
    {
      prm_flag = "CLIENT";
    }

  if (print_mode == PRM_PRINT_ID)
    {
      snprintf (left_side, PRM_DEFAULT_BUFFER_SIZE, "%d(%s)=",
		prm_id, prm_flag);
    }
  else if (print_mode == PRM_PRINT_NAME)
    {
      snprintf (left_side, PRM_DEFAULT_BUFFER_SIZE, "%s(%s)=",
		prm->name, prm_flag);
    }

  switch (prm->datatype)
    {
    case PRM_INTEGER:
      {
	int val = value.i;

	error = prm_print_int_value (val1_buffer, sizeof (val1_buffer),
				     val, prm);
	if (error != NO_ERROR)
	  {
	    return error;
	  }
	val1_string = val1_buffer;

	if (PRM_IS_FOR_SERVER (prm->static_flag)
	    && PRM_IS_FOR_CLIENT (prm->static_flag))
	  {
	    val = PRM_GET_INT (prm->value);

	    error = prm_print_int_value (val2_buffer,
					 sizeof (val2_buffer), val, prm);
	    if (error != NO_ERROR)
	      {
		return error;
	      }
	    val2_string = val2_buffer;
	  }
      }
      break;
    case PRM_FLOAT:
      {
	float val = value.f;

	error = prm_print_float_value (val1_buffer, sizeof (val1_buffer),
				       val, prm);
	if (error != NO_ERROR)
	  {
	    return error;
	  }
	val1_string = val1_buffer;

	if (PRM_IS_FOR_SERVER (prm->static_flag)
	    && PRM_IS_FOR_CLIENT (prm->static_flag))
	  {
	    val = PRM_GET_FLOAT (prm->value);

	    error = prm_print_float_value (val2_buffer,
					   sizeof (val2_buffer), val, prm);
	    if (error != NO_ERROR)
	      {
		return error;
	      }
	    val2_string = val2_buffer;
	  }
      }
      break;
    case PRM_BOOLEAN:
      {
	bool val = value.b;

	error = prm_print_boolean_value (val1_buffer, sizeof (val1_buffer),
					 val, prm);
	if (error != NO_ERROR)
	  {
	    return error;
	  }
	val1_string = val1_buffer;

	if (PRM_IS_FOR_SERVER (prm->static_flag)
	    && PRM_IS_FOR_CLIENT (prm->static_flag))
	  {
	    val = PRM_GET_BOOL (prm->value);

	    error = prm_print_boolean_value (val2_buffer,
					     sizeof (val2_buffer), val, prm);
	    if (error != NO_ERROR)
	      {
		return error;
	      }
	    val2_string = val2_buffer;
	  }
      }
      break;
    case PRM_KEYWORD:
      {
	int val = value.i;

	error = prm_print_keyword_value (val1_buffer, sizeof (val1_buffer),
					 val, prm);
	if (error != NO_ERROR)
	  {
	    return error;
	  }
	val1_string = val1_buffer;

	if (PRM_IS_FOR_SERVER (prm->static_flag)
	    && PRM_IS_FOR_CLIENT (prm->static_flag))
	  {
	    val = PRM_GET_INT (prm->value);

	    error = prm_print_keyword_value (val2_buffer,
					     sizeof (val2_buffer), val, prm);
	    if (error != NO_ERROR)
	      {
		return error;
	      }
	    val2_string = val2_buffer;
	  }
      }
      break;
    case PRM_BIGINT:
      {
	INT64 val = value.bi;

	error = prm_print_bigint_value (val1_buffer, sizeof (val1_buffer),
					val, prm);
	if (error != NO_ERROR)
	  {
	    return error;
	  }
	val1_string = val1_buffer;

	if (PRM_IS_FOR_SERVER (prm->static_flag)
	    && PRM_IS_FOR_CLIENT (prm->static_flag))
	  {
	    val = PRM_GET_BIGINT (prm->value);

	    error = prm_print_bigint_value (val2_buffer,
					    sizeof (val2_buffer), val, prm);
	    if (error != NO_ERROR)
	      {
		return error;
	      }
	    val2_string = val2_buffer;
	  }
      }
      break;
    case PRM_STRING:
      {
	val1_string = value.str;

	if (PRM_IS_FOR_SERVER (prm->static_flag)
	    && PRM_IS_FOR_CLIENT (prm->static_flag))
	  {
	    val2_string = PRM_GET_STRING (prm->value);
	  }
      }
      break;
    case PRM_INTEGER_LIST:
      {
	int *val = value.integer_list;

	error = prm_print_int_list_value (val1_buffer, sizeof (val1_buffer),
					  val, prm);
	if (error != NO_ERROR)
	  {
	    return error;
	  }
	val1_string = val1_buffer;

	if (PRM_IS_FOR_SERVER (prm->static_flag)
	    && PRM_IS_FOR_CLIENT (prm->static_flag))
	  {
	    val = PRM_GET_INTEGER_LIST (prm->value);

	    error = prm_print_int_list_value (val2_buffer,
					      sizeof (val2_buffer), val, prm);
	    if (error != NO_ERROR)
	      {
		return error;
	      }
	    val2_string = val2_buffer;
	  }
      }
      break;
    default:
      {
	val1_string = val2_string = NULL;
      }
      break;
    }

  if (val2_string != NULL)
    {
      n =
	snprintf (buf, len, "%s(%s|%s)", left_side, val1_string, val2_string);
    }
  else if (val1_string != NULL)
    {
      n = snprintf (buf, len, "%s%s", left_side, val1_string);
    }
  else
    {
      n = snprintf (buf, len, "%s", left_side);
    }

  return n;
}

/*
 * sysprm_obtain_parameters () - Get parameter values
 *
 * return		: SYSPRM_ERR code.
 * data (in)	        : string containing the names of parameters.
 * prm_values_ptr (out) : list of ids and values for the parameters read from
 *			  data.
 *
 * NOTE: Multiple parameters can be obtained by providing a string like:
 *	 "param_name1; param_name2; ..."
 *	 If some values must be read from server, PRM_ERR_NOT_FOR_CLIENT is
 *	 returned.
 */
int
sysprm_obtain_parameters (char *data, SYSPRM_ASSIGN_VALUE ** prm_values_ptr)
{
  char buf[LINE_MAX], *p = NULL, *name = NULL;
  SYSPRM_PARAM *prm = NULL;
  SYSPRM_ASSIGN_VALUE *prm_value_list = NULL, *last_prm_value = NULL;
  SYSPRM_ASSIGN_VALUE *prm_value = NULL;
  SYSPRM_ERR error = PRM_ERR_NO_ERROR, scope_error = PRM_ERR_NO_ERROR;

  if (!data || *data == '\0')
    {
      return PRM_ERR_BAD_VALUE;
    }

  if (intl_mbs_ncpy (buf, data, LINE_MAX) == NULL)
    {
      return PRM_ERR_BAD_VALUE;
    }

  assert (prm_values_ptr != NULL);
  *prm_values_ptr = NULL;

  p = buf;
  do
    {
      /* read name */
      while (char_isspace (*p))
	{
	  p++;
	}
      if (*p == '\0')
	{
	  break;
	}
      name = p;

      while (*p && !char_isspace (*p) && *p != ';')
	{
	  p++;
	}

      if (*p)
	{
	  *p++ = '\0';
	  while (char_isspace (*p))
	    {
	      p++;
	    }
	  if (*p == ';')
	    {
	      p++;
	    }
	}

      prm = prm_find (name, NULL);
      if (prm == NULL)
	{
	  error = PRM_ERR_UNKNOWN_PARAM;
	  break;
	}

#if defined (CS_MODE)
      if (!PRM_IS_FOR_CLIENT (prm->static_flag)
	  && !PRM_IS_FOR_SERVER (prm->static_flag))
	{
	  error = PRM_ERR_CANNOT_CHANGE;
	  break;
	}

      if (PRM_IS_FOR_SERVER (prm->static_flag))
	{
	  /* have to read the value on server */
	  scope_error = PRM_ERR_NOT_FOR_CLIENT;
	}
#endif /* CS_MODE */

      /* create a SYSPRM_ASSING_VALUE object to store parameter value */
      prm_value =
	(SYSPRM_ASSIGN_VALUE *) malloc (sizeof (SYSPRM_ASSIGN_VALUE));
      if (prm_value == NULL)
	{
	  error = PRM_ERR_NO_MEM_FOR_PRM;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, sizeof (SYSPRM_ASSIGN_VALUE));
	  break;
	}
      prm_value->prm_id = prm->param_id;
#if 1				/* TODO - */
      prm_value->persist = false;
#endif
      prm_value->next = NULL;
#if defined (CS_MODE)
      if (PRM_IS_FOR_CLIENT (prm->static_flag)
	  && !PRM_IS_FOR_SERVER (prm->static_flag))
	{
	  /* set the value here */
	  sysprm_set_sysprm_value_from_parameter (&prm_value->value, prm);
	}
      else
	{
	  memset (&prm_value->value, 0, sizeof (SYSPRM_VALUE));
	}
#else /* CS_MODE */
      sysprm_set_sysprm_value_from_parameter (&prm_value->value, prm);
#endif /* !CS_MODE */

      /* append prm_value to prm_value_list */
      if (prm_value_list != NULL)
	{
	  last_prm_value->next = prm_value;
	  last_prm_value = prm_value;
	}
      else
	{
	  prm_value_list = last_prm_value = prm_value;
	}
    }
  while (*p);

  if (error == PRM_ERR_NO_ERROR)
    {
      /* all parameter names are valid and values can be obtained */
      *prm_values_ptr = prm_value_list;
      /* update error in order to get values from server too if needed */
      error = scope_error;
    }
  else
    {
      /* error obtaining values, clean up */
      sysprm_free_assign_values (&prm_value_list);
    }

  return error;
}

#if !defined(CS_MODE)
/*
 * xsysprm_change_server_parameters () - changes parameter values on server
 *
 * return	    : void
 * assignments (in) : list of changes
 */
void
xsysprm_change_server_parameters (const SYSPRM_ASSIGN_VALUE * assignments)
{
  (void) sysprm_change_parameter_values (assignments, true, true);
}

/*
 * xsysprm_obtain_server_parameters () - get parameter values from server
 *
 * return	   : void
 * prm_values (in) : list of parameters
 *
 * NOTE: Obtains value for parameters that are for server.
 */
void
xsysprm_obtain_server_parameters (SYSPRM_ASSIGN_VALUE * prm_values)
{
  SYSPRM_PARAM *prm = NULL;

  for (; prm_values != NULL; prm_values = prm_values->next)
    {
      prm = GET_PRM (prm_values->prm_id);

      if (PRM_IS_FOR_SERVER (prm->static_flag))
	{
	  /* set value */
	  sysprm_set_sysprm_value_from_parameter (&prm_values->value, prm);
	}
    }
}

/*
 * xsysprm_get_force_server_parameters () - obtain values for parameters
 *					    marked as PRM_FORCE_SERVER
 *
 * return : list of values
 *
 * NOTE: This is called after client registers to server.
 */
SYSPRM_ASSIGN_VALUE *
xsysprm_get_force_server_parameters (void)
{
  SYSPRM_ASSIGN_VALUE *force_values = NULL, *last_assign = NULL;
  SYSPRM_PARAM *prm = NULL;
  int i;

  for (i = 0; i < NUM_PRM; i++)
    {
      prm = GET_PRM (i);
      if (PRM_GET_FROM_SERVER (prm->static_flag))
	{
	  SYSPRM_ASSIGN_VALUE *change_val =
	    (SYSPRM_ASSIGN_VALUE *) malloc (sizeof (SYSPRM_ASSIGN_VALUE));
	  if (change_val == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1,
		      sizeof (SYSPRM_ASSIGN_VALUE));
	      goto cleanup;
	    }
	  change_val->prm_id = i;
#if 1				/* TODO - */
	  change_val->persist = false;
#endif
	  change_val->next = NULL;
	  sysprm_set_sysprm_value_from_parameter (&change_val->value, prm);
	  if (force_values != NULL)
	    {
	      last_assign->next = change_val;
	      last_assign = change_val;
	    }
	  else
	    {
	      force_values = last_assign = change_val;
	    }
	}
    }

  return force_values;

cleanup:
  sysprm_free_assign_values (&force_values);
  return NULL;
}

/*
 * xsysprm_dump_server_parameters -
 *   return: none
 *   fp(in):
 */
void
xsysprm_dump_server_parameters (FILE * outfp)
{
  sysprm_dump_parameters (outfp);
}
#endif /* !CS_MODE */

/*
 * sysprm_get_range -
 *   return:
 *   pname (in): parameter name
 *   value (in): parameter value
 */
int
sysprm_get_range (const char *pname, void *min, void *max)
{
  int error = NO_ERROR;
  SYSPRM_PARAM *prm;

  prm = prm_find (pname, NULL);
  if (prm == NULL)
    {
      return PRM_ERR_UNKNOWN_PARAM;
    }

  if (PRM_IS_INTEGER (prm))
    {
      if (prm->lower_limit)
	{
	  *((int *) min) = PRM_GET_INT (prm->lower_limit);
	}
      else
	{
	  *((int *) min) = INT_MIN;
	}

      if (prm->upper_limit)
	{
	  *((int *) max) = PRM_GET_INT (prm->upper_limit);
	}
      else
	{
	  *((int *) max) = INT_MAX;
	}
    }
  else if (PRM_IS_FLOAT (prm))
    {
      if (prm->lower_limit)
	{
	  *((float *) min) = PRM_GET_FLOAT (prm->lower_limit);
	}
      else
	{
	  *((float *) min) = FLT_MIN;
	}

      if (prm->upper_limit)
	{
	  *((float *) max) = PRM_GET_FLOAT (prm->upper_limit);
	}
      else
	{
	  *((float *) max) = FLT_MAX;
	}
    }
  else if (PRM_IS_BIGINT (prm))
    {
      if (prm->lower_limit)
	{
	  *((INT64 *) min) = PRM_GET_BIGINT (prm->lower_limit);
	}
      else
	{
	  *((INT64 *) min) = 0ULL;
	}

      if (prm->upper_limit)
	{
	  *((INT64 *) max) = PRM_GET_BIGINT (prm->upper_limit);
	}
      else
	{
	  *((INT64 *) max) = ULLONG_MAX;
	}
    }
  else
    {
      return PRM_ERR_BAD_VALUE;
    }

  if (error != NO_ERROR)
    {
      return PRM_ERR_BAD_VALUE;
    }

  return PRM_ERR_NO_ERROR;
}

/*
 * sysprm_check_range -
 *   return:
 *   pname (in): parameter name
 *   value (in): parameter value
 */
int
sysprm_check_range (const char *pname, void *value)
{
  int error = 0;
  SYSPRM_PARAM *prm;

  prm = prm_find (pname, NULL);
  if (prm == NULL)
    {
      return PRM_ERR_UNKNOWN_PARAM;
    }

  error = prm_check_range (prm, value);

  return error;
}

/*
 * prm_check_range -
 *   return:
 *   prm(in):
 *   value (in):
 */
static int
prm_check_range (SYSPRM_PARAM * prm, void *value)
{
  if (PRM_IS_INTEGER (prm) || PRM_IS_KEYWORD (prm))
    {
      int val;

      val = *((int *) value);

      if ((prm->upper_limit && PRM_GET_INT (prm->upper_limit) < val)
	  || (prm->lower_limit && PRM_GET_INT (prm->lower_limit) > val))
	{
	  return PRM_ERR_BAD_RANGE;
	}
    }
  else if (PRM_IS_FLOAT (prm))
    {
      float val;
      float lower, upper;

      lower = upper = 0;	/* to make compilers be silent */
      val = *((float *) value);

      if (prm->upper_limit)
	{
	  upper = PRM_GET_FLOAT (prm->upper_limit);
	}

      if (prm->lower_limit)
	{
	  lower = PRM_GET_FLOAT (prm->lower_limit);
	}

      if ((prm->upper_limit && upper < val)
	  || (prm->lower_limit && lower > val))
	{
	  return PRM_ERR_BAD_RANGE;
	}
    }
  else if (PRM_IS_BIGINT (prm))
    {
      INT64 val;

      val = *((INT64 *) value);

      if ((prm->upper_limit && PRM_GET_BIGINT (prm->upper_limit) < val)
	  || (prm->lower_limit && PRM_GET_BIGINT (prm->lower_limit) > val))
	{
	  return PRM_ERR_BAD_RANGE;
	}
    }
  else
    {
      return PRM_ERR_BAD_VALUE;
    }

  return PRM_ERR_NO_ERROR;
}

/*
 * sysprm_generate_new_value () - converts string into a system parameter value
 *
 * return	   : SYSPRM_ERR
 * prm (in)	   : target system parameter
 * value (in)	   : parameter value in char * format
 * check (in)	   : check if value can be changed. set to false if value
 *		     should be forced
 * new_value (out) : SYSPRM_VALUE converted from string
 */
static SYSPRM_ERR
sysprm_generate_new_value (SYSPRM_PARAM * prm, const char *value,
			   UNUSED_ARG bool check, SYSPRM_VALUE * new_value)
{
  char *end = NULL;
  char buf[LINE_MAX];
  SYSPRM_ERR ret = PRM_ERR_NO_ERROR;

  if (prm == NULL)
    {
      return PRM_ERR_UNKNOWN_PARAM;
    }
  if (value == NULL)
    {
      return PRM_ERR_BAD_VALUE;
    }

  assert (new_value != NULL);
  if (!PRM_IS_BIGINT (prm)
      && (PRM_HAS_SIZE_UNIT (prm->static_flag)
	  || PRM_HAS_TIME_UNIT (prm->static_flag)))
    {
      assert (false);
      return PRM_ERR_BAD_PARAM;
    }

#if defined (CS_MODE)
  if (check)
    {
      /* check the scope of parameter */
      if (PRM_IS_FOR_CLIENT (prm->static_flag))
	{
	  if (PRM_IS_FOR_SERVER (prm->static_flag))
	    {
	      /* the value has to be changed on server too. user has to be
	       * part of DBA group.
	       */
	      ret = PRM_ERR_NOT_FOR_CLIENT;
	    }
	}
      else
	{
	  if (PRM_IS_FOR_SERVER (prm->static_flag))
	    {
	      /* this value is only for server. user has to be DBA. */
	      ret = PRM_ERR_NOT_FOR_CLIENT;
	    }
	  else
	    {
	      /* not for client or server, cannot be changed on-line */
	      return PRM_ERR_CANNOT_CHANGE;
	    }
	}
    }
#endif /* CS_MODE */

#if defined (SERVER_MODE)
  if (check)
    {
      if (!PRM_IS_FOR_SERVER (prm->static_flag))
	{
	  return PRM_ERR_NOT_FOR_SERVER;
	}
    }
#endif /* SERVER_MODE */

  /* check iff default value
   */
  if (intl_mbs_casecmp ("DEFAULT", value) == 0)
    {
      prm_print (prm, buf, LINE_MAX, PRM_PRINT_NONE, PRM_PRINT_DEFAULT_VAL);
      value = buf;
    }

  switch (prm->datatype)
    {
    case PRM_INTEGER:
      {
	/* convert string to int */
	int val, result;

	result = parse_int (&val, value, 10);
	if (result != 0)
	  {
	    return PRM_ERR_BAD_VALUE;
	  }

	if (prm_check_range (prm, (void *) &val) != NO_ERROR)
	  {
	    return PRM_ERR_BAD_RANGE;
	  }

	new_value->i = val;
	break;
      }

    case PRM_BIGINT:
      {
	/* convert string to INT64 */
	int result;
	INT64 val;
	char *end_p;

	if (PRM_HAS_SIZE_UNIT (prm->static_flag))
	  {
	    if (util_size_string_to_byte (&val, value) != NO_ERROR)
	      {
		return PRM_ERR_BAD_VALUE;
	      }
	  }
	else if (PRM_HAS_TIME_UNIT (prm->static_flag))
	  {
	    if (util_time_string_to_msec (&val, value) != NO_ERROR)
	      {
		return PRM_ERR_BAD_VALUE;
	      }
	  }
	else
	  {
	    result = str_to_int64 (&val, &end_p, value, 10);
	    if (result != 0)
	      {
		return PRM_ERR_BAD_VALUE;
	      }
	  }

	if (prm_check_range (prm, (void *) &val) != NO_ERROR)
	  {
	    return PRM_ERR_BAD_RANGE;
	  }

	new_value->bi = val;
	break;
      }

    case PRM_FLOAT:
      {
	/* convert string to float */
	float val;

	val = (float) strtod (value, &end);
	if (end == value)
	  {
	    return PRM_ERR_BAD_VALUE;
	  }
	else if (*end != '\0')
	  {
	    return PRM_ERR_BAD_VALUE;
	  }

	if (prm_check_range (prm, (void *) &val) != NO_ERROR)
	  {
	    return PRM_ERR_BAD_RANGE;
	  }

	new_value->f = val;
	break;
      }

    case PRM_BOOLEAN:
      {
	/* convert string to boolean */
	const KEYVAL *keyvalp = NULL;

	keyvalp = prm_keyword (-1, value, boolean_words, DIM (boolean_words));
	if (keyvalp == NULL)
	  {
	    return PRM_ERR_BAD_VALUE;
	  }

	new_value->b = (bool) keyvalp->val;
	break;
      }

    case PRM_STRING:
      {
	/* duplicate string */
	char *val = NULL;

	/* check if the value is represented as a null keyword */
	if (prm_keyword (-1, value, null_words, DIM (null_words)) != NULL)
	  {
	    val = NULL;
	  }
	else
	  {
	    val = strdup (value);
	    if (val == NULL)
	      {
		er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			ER_OUT_OF_VIRTUAL_MEMORY, 1, strlen (value));
		return PRM_ERR_NO_MEM_FOR_PRM;
	      }
	  }
	new_value->str = val;
	break;
      }

    case PRM_INTEGER_LIST:
      {
	/* convert string into an array of integers */
	int *val = NULL;

	/* check if the value is represented as a null keyword */
	if (prm_keyword (-1, value, null_words, DIM (null_words)) != NULL)
	  {
	    val = NULL;
	  }
	else
	  {
	    char *s, *p;
	    char save;
	    int list_size, tmp;

	    val = calloc (1024, sizeof (int));	/* max size is 1023 */
	    if (val == NULL)
	      {
		size_t size = 1024 * sizeof (int);
		er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			ER_OUT_OF_VIRTUAL_MEMORY, 1, size);
		return PRM_ERR_NO_MEM_FOR_PRM;
	      }

	    list_size = 0;
	    s = (char *) value;
	    p = s;

	    while (true)
	      {
		if (*s == ',' || *s == '\0')
		  {
		    save = *s;
		    *s = '\0';
		    if (intl_mbs_casecmp ("DEFAULT", p) == 0)
		      {
			if (prm->param_id ==
			    PRM_ID_CALL_STACK_DUMP_ACTIVATION)
			  {
			    memcpy (&val[list_size + 1],
				    call_stack_dump_error_codes,
				    sizeof (call_stack_dump_error_codes));
			    list_size += DIM (call_stack_dump_error_codes);
			  }
			else if (prm->param_id == PRM_ID_HA_IGNORE_ERROR_LIST)
			  {
			    memcpy (&val[list_size + 1],
				    ha_ignore_error_codes,
				    sizeof (ha_ignore_error_codes));
			    list_size += DIM (ha_ignore_error_codes);
			  }
			else
			  {
			    free_and_init (val);
			    return PRM_ERR_BAD_VALUE;
			  }
		      }
		    else
		      {
			int result;

			result = parse_int (&tmp, p, 10);
			if (result != 0)
			  {
			    free_and_init (val);
			    return PRM_ERR_BAD_VALUE;
			  }
			val[++list_size] = tmp;
		      }
		    *s = save;
		    if (*s == '\0')
		      {
			break;
		      }
		    p = s + 1;
		  }
		s++;
	      }
	    /* save size in the first position */
	    val[0] = list_size;
	  }
	new_value->integer_list = val;
	break;
      }

    case PRM_KEYWORD:
      {
	/* check if string can be identified as a keyword */
	int val;
	const KEYVAL *keyvalp = NULL;

	if (intl_mbs_casecmp (prm->name, PRM_NAME_ER_LOG_LEVEL) == 0)
	  {
	    keyvalp = prm_keyword (-1, value, er_log_level_words,
				   DIM (er_log_level_words));
	  }
	else if (intl_mbs_casecmp (prm->name,
				   PRM_NAME_PB_DEBUG_PAGE_VALIDATION_LEVEL)
		 == 0)
	  {
	    keyvalp = prm_keyword (-1, value,
				   pgbuf_debug_page_validation_level_words,
				   DIM
				   (pgbuf_debug_page_validation_level_words));
	  }
	else if (intl_mbs_casecmp (prm->name, PRM_NAME_HA_MODE) == 0)
	  {
	    keyvalp = prm_keyword (-1, value, ha_mode_words,
				   DIM (ha_mode_words));
	  }
	else if (intl_mbs_casecmp (prm->name, PRM_NAME_CHECK_PEER_ALIVE) == 0)
	  {
	    keyvalp = prm_keyword (-1, value, check_peer_alive_words,
				   DIM (check_peer_alive_words));
	  }
	else if (intl_mbs_casecmp (prm->name,
				   PRM_NAME_QUERY_TRACE_FORMAT) == 0)
	  {
	    keyvalp = prm_keyword (-1, value, query_trace_format_words,
				   DIM (query_trace_format_words));
	  }
	else if (intl_mbs_casecmp (prm->name,
				   PRM_NAME_HA_REPL_FILTER_TYPE) == 0)
	  {
	    keyvalp = prm_keyword (-1, value, ha_repl_filter_type_words,
				   DIM (ha_repl_filter_type_words));
	  }
	else
	  {
	    assert (false);
	  }

	if (keyvalp)
	  {
	    val = (int) keyvalp->val;
	  }
	else
	  {
	    int result;
	    /* check if string can be converted to an integer */
	    result = parse_int (&val, value, 10);
	    if (result != 0)
	      {
		return PRM_ERR_BAD_VALUE;
	      }
	  }

	if ((prm->upper_limit && PRM_GET_INT (prm->upper_limit) < val)
	    || (prm->lower_limit && PRM_GET_INT (prm->lower_limit) > val))
	  {
	    return PRM_ERR_BAD_RANGE;
	  }
	new_value->i = val;
      }
      break;
    case PRM_NO_TYPE:
      break;

    default:
      assert (false);
    }

  return ret;
}

/*
 * prm_set () - Set a new value for parameter.
 *
 * return	 : SYSPRM_ERR code.
 * prm (in)	 : system parameter that will have its value changed.
 * value (in)	 : new value as string.
 * set_flag (in) : updates PRM_SET flag is true.
 */
static int
prm_set (SYSPRM_PARAM * prm, const char *value, bool set_flag)
{
  SYSPRM_ERR error = PRM_ERR_NO_ERROR;
  SYSPRM_VALUE new_value;

  error = sysprm_generate_new_value (prm, value, false, &new_value);
  if (error != PRM_ERR_NO_ERROR)
    {
      return error;
    }

  return sysprm_set_value (prm, new_value, set_flag, false);
}

/*
 * sysprm_set_value () - Set a new value for parameter.
 *
 * return	  : SYSPRM_ERR code
 * prm (in)       : system parameter that will have its value changed.
 * value (in)     : new values as sysprm_value
 * set_flag (in)  : updates PRM_SET flag.
 * duplicate (in) : duplicate values for data types that need memory
 *		    allocation.
 */
static int
sysprm_set_value (SYSPRM_PARAM * prm, SYSPRM_VALUE value, bool set_flag,
		  bool duplicate)
{
  if (prm == NULL)
    {
      return PRM_ERR_UNKNOWN_PARAM;
    }

  if (duplicate)
    {
      /* duplicate values for data types that need memory allocation */
      switch (prm->datatype)
	{
	case PRM_STRING:
	  if (value.str != NULL)
	    {
	      value.str = strdup (value.str);
	      if (value.str == NULL)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_OUT_OF_VIRTUAL_MEMORY, 1,
			  strlen (value.str) + 1);
		  return PRM_ERR_NO_MEM_FOR_PRM;
		}
	    }
	  break;

	case PRM_INTEGER_LIST:
	  if (value.integer_list != NULL)
	    {
	      int *integer_list = value.integer_list;
	      value.integer_list =
		(int *) malloc ((integer_list[0] + 1) * sizeof (int));
	      if (value.integer_list == NULL)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_OUT_OF_VIRTUAL_MEMORY, 1,
			  (integer_list[0] + 1) * sizeof (int));
		  return PRM_ERR_NO_MEM_FOR_PRM;
		}
	      memcpy (value.integer_list, integer_list,
		      (integer_list[0] + 1) * sizeof (int));
	    }

	default:
	  break;
	}
    }

  sysprm_set_system_parameter_value (prm, value);

  if (set_flag)
    {
      PRM_SET_BIT (PRM_SET, *prm->dynamic_flag);
      /* Indicate that the default value was not used */
      PRM_CLEAR_BIT (PRM_DEFAULT_USED, *prm->dynamic_flag);
    }

  return PRM_ERR_NO_ERROR;
}

/*
 * sysprm_set_system_parameter_value () - change a parameter value in prm_Def
 *					  array.
 *
 * return     : void.
 * prm (in)   : parameter that needs changed.
 * value (in) : new value.
 */
static void
sysprm_set_system_parameter_value (SYSPRM_PARAM * prm, SYSPRM_VALUE value)
{
  switch (prm->datatype)
    {
    case PRM_INTEGER:
    case PRM_KEYWORD:
      prm_set_integer_value (prm->param_id, value.i);
      break;

    case PRM_FLOAT:
      prm_set_float_value (prm->param_id, value.f);
      break;

    case PRM_BOOLEAN:
      prm_set_bool_value (prm->param_id, value.b);
      break;

    case PRM_STRING:
      prm_set_string_value (prm->param_id, value.str);
      break;

    case PRM_INTEGER_LIST:
      prm_set_integer_list_value (prm->param_id, value.integer_list);
      break;

    case PRM_BIGINT:
      prm_set_bigint_value (prm->param_id, value.bi);
      break;

    case PRM_NO_TYPE:
      break;
    }
}

/*
 * prm_set_force -
 *   return: NO_ERROR or error code defined in enum SYSPRM_ERR
 *   prm(in):
 *   value(in):
 */
static int
prm_set_force (SYSPRM_PARAM * prm, const char *value)
{
  if (prm->force_value)
    {
      free_and_init (PRM_GET_STRING (&prm->force_value));
    }

  prm->force_value = strdup (value);
  if (prm->force_value == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      strlen (value) + 1);
      return PRM_ERR_NO_MEM_FOR_PRM;
    }

  return NO_ERROR;
}

/*
 * prm_set_default -
 *   return: NO_ERROR or error code defined in enum SYSPRM_ERR
 *   prm(in):
 */
static int
prm_set_default (SYSPRM_PARAM * prm)
{
  if (prm == NULL)
    {
      return ER_FAILED;
    }

  if (PRM_IS_INTEGER (prm) || PRM_IS_KEYWORD (prm))
    {
      int val, *valp;

      val = PRM_GET_INT (prm->default_value);
      valp = (int *) prm->value;
      *valp = val;
    }
  if (PRM_IS_BIGINT (prm))
    {
      INT64 val, *valp;

      val = PRM_GET_BIGINT (prm->default_value);
      valp = (INT64 *) prm->value;
      *valp = val;
    }
  else if (PRM_IS_BOOLEAN (prm))
    {
      bool val, *valp;

      val = PRM_GET_BOOL (prm->default_value);
      valp = (bool *) prm->value;
      *valp = val;
    }
  else if (PRM_IS_FLOAT (prm))
    {
      float val, *valp;

      val = PRM_GET_FLOAT (prm->default_value);
      valp = (float *) prm->value;
      *valp = val;
    }
  else if (PRM_IS_STRING (prm))
    {
      char *val, **valp;

      if (PRM_IS_ALLOCATED (*prm->dynamic_flag))
	{
	  char *str = PRM_GET_STRING (prm->value);
	  free_and_init (str);
	  PRM_CLEAR_BIT (PRM_ALLOCATED, *prm->dynamic_flag);
	}

      val = *(char **) prm->default_value;
      valp = (char **) prm->value;
      *valp = val;
    }
  else if (PRM_IS_INTEGER_LIST (prm))
    {
      int *val, **valp;

      if (PRM_IS_ALLOCATED (*prm->dynamic_flag))
	{
	  int *int_list = PRM_GET_INTEGER_LIST (prm->value);

	  free_and_init (int_list);
	  PRM_CLEAR_BIT (PRM_ALLOCATED, *prm->dynamic_flag);
	}

      val = *(int **) prm->default_value;
      valp = (int **) prm->value;
      *valp = val;
    }

  /* Indicate that the default value was used */
  PRM_SET_BIT (PRM_DEFAULT_USED, *prm->dynamic_flag);

  return NO_ERROR;
}

/*
 * prm_find -
 *   return: NULL or found parameter
 *   pname(in): parameter name to find
 */
static SYSPRM_PARAM *
prm_find (const char *pname, const char *section)
{
  unsigned int i;
  const char *key;
  char buf[4096];

  if (pname == NULL)
    {
      return NULL;
    }

  if (prm_Def_is_initialized == false)
    {
      sysprm_initialize_prm_def ();
    }

  if (section != NULL)
    {
      snprintf (buf, sizeof (buf) - 1, "%s::%s", section, pname);
      key = buf;
    }
  else
    {
      key = pname;
    }

  for (i = 0; i < DIM (prm_Def); i++)
    {
      if (intl_mbs_casecmp (prm_Def[i].name, key) == 0)
	{
	  return &prm_Def[i];
	}
    }

  return NULL;
}

/*
 * sysprm_set_force -
 *   return: NO_ERROR or error code
 *   pname(in): parameter name to set
 *   pvalue(in): value to be set to the parameter
 */
int
sysprm_set_force (const char *pname, const char *pvalue)
{
  SYSPRM_PARAM *prm;

  if (pname == NULL || pvalue == NULL)
    {
      return ER_PRM_BAD_VALUE;
    }

  prm = prm_find (pname, NULL);
  if (prm == NULL)
    {
      return ER_PRM_BAD_VALUE;
    }

  if (prm_set_force (prm, pvalue) != NO_ERROR)
    {
      return ER_PRM_CANNOT_CHANGE;
    }

  return NO_ERROR;
}

#if 0
/*
 * sysprm_set_to_default -
 *   return: NO_ERROR or error code
 *   pname(in): parameter name to set to default value
 */
int
sysprm_set_to_default (const char *pname, bool set_to_force)
{
  SYSPRM_PARAM *prm;
  char val[LINE_MAX];

  if (pname == NULL)
    {
      return ER_PRM_BAD_VALUE;
    }

  prm = prm_find (pname, NULL);
  if (prm == NULL)
    {
      return ER_PRM_BAD_VALUE;
    }

  if (prm_set_default (prm) != NO_ERROR)
    {
      return ER_PRM_CANNOT_CHANGE;
    }

  if (set_to_force)
    {
      prm_print (prm, val, LINE_MAX, PRM_PRINT_NONE, PRM_PRINT_CURR_VAL);
      prm_set_force (prm, val);
    }

  return NO_ERROR;
}
#endif

/*
 * prm_keyword - Search a keyword within the keyword table
 *   return: NULL or found keyword
 *   val(in): keyword value
 *   name(in): keyword name
 *   tbl(in): keyword table
 *   dim(in): size of the table
 */
static const KEYVAL *
prm_keyword (int val, const char *name, const KEYVAL * tbl, int dim)
{
  int i;

  if (name != NULL)
    {
      for (i = 0; i < dim; i++)
	{
	  if (intl_mbs_casecmp (name, tbl[i].key) == 0)
	    {
	      return &tbl[i];
	    }
	}
    }
  else
    {
      for (i = 0; i < dim; i++)
	{
	  if (tbl[i].val == val)
	    {
	      return &tbl[i];
	    }
	}
    }

  return NULL;
}

/*
 * prm_report_bad_entry -
 *   return:
 *   line(in):
 *   err(in):
 *   where(in):
 */
static void
prm_report_bad_entry (const char *key, int line, int err, const char *where)
{
  if (line > 0)
    {
      fprintf (stderr, PARAM_MSG_FMT (PRM_ERR_BAD_LINE), key, line, where);
    }
  else if (line == 0)
    {
      fprintf (stderr, PARAM_MSG_FMT (PRM_ERR_BAD_PARAM), key, line, where);
    }
  else
    {
      fprintf (stderr, PARAM_MSG_FMT (PRM_ERR_BAD_ENV_VAR), where);
    }

  if (err > 0)
    {
      switch (err)
	{
	case PRM_ERR_DEPRICATED:
	case PRM_ERR_NO_MEM_FOR_PRM:
	  fprintf (stderr, "%s\n", PARAM_MSG_FMT (err));
	  break;

	case PRM_ERR_UNKNOWN_PARAM:
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_PRM_UNKNOWN_SYSPRM, 3,
		  key, line, where);
	  fprintf (stderr, "%s\n", PARAM_MSG_FMT (err));
	  break;

	case PRM_ERR_BAD_VALUE:
	case PRM_ERR_BAD_STRING:
	case PRM_ERR_BAD_RANGE:
	case PRM_ERR_RESET_BAD_RANGE:
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_PRM_BAD_VALUE, 1, key);
	  fprintf (stderr, "%s\n", PARAM_MSG_FMT (err));
	  break;

	default:
	  break;
	}
    }
  else
    {
      fprintf (stderr, PARAM_MSG_FMT (PRM_ERR_UNIX_ERROR), strerror (err));
    }

  fflush (stderr);
}


/*
 * sysprm_final - Clean up the storage allocated during parameter parsing
 *   return: none
 */
void
sysprm_final (void)
{
  SYSPRM_PARAM *prm;
  char **valp;
  int i;

  if (prm_Def_is_initialized == false)
    {
      sysprm_initialize_prm_def ();
    }

  for (i = 0; i < NUM_PRM; i++)
    {
      prm = &prm_Def[i];
      if (PRM_IS_ALLOCATED (*prm->dynamic_flag) && PRM_IS_STRING (prm))
	{
	  char *str = PRM_GET_STRING (prm->value);

	  free_and_init (str);
	  PRM_CLEAR_BIT (PRM_ALLOCATED, *prm->dynamic_flag);

	  valp = (char **) prm->value;
	  *valp = NULL;
	}

      /* reset all dynamic flags */
      *prm->dynamic_flag = 0;
    }
}

/*
 * prm_tune_hostname_list - Convert hostname list to IP addr list
 *
 *   return: error code
 */
static int
prm_tune_hostname_list (SYSPRM_PARAM * list_prm, PARAM_ID prm_id)
{
  const char *list_str = NULL, *p = NULL;
  char **list_pp = NULL;
  int remaining, cplen, i;
  struct in_addr node_addr;
  char *ip_addr = NULL;

  char newval[LINE_MAX];

  assert (list_prm != NULL);
  assert (prm_id == PRM_ID_HA_NODE_LIST || prm_id == PRM_ID_HA_REPLICA_LIST);

  list_str = prm_get_string_value (prm_id);

  list_pp = util_split_ha_node (list_str);
  if (list_pp == NULL)
    {
      const char *message =
	utility_get_generic_message (MSGCAT_UTIL_GENERIC_NO_MEM);

      fprintf (stderr, message);

      return ER_FAILED;
    }

  remaining = sizeof (newval) - 1;

  p = strchr (list_str, '@');
  if (p == NULL || p == list_str)
    {
      assert (false);
      if (list_pp)
	{
	  util_free_string_array (list_pp);
	  list_pp = NULL;
	}

      return ER_FAILED;
    }

  cplen = (p - list_str) + 1;	/* include '@' */
  cplen = MIN (cplen, remaining);
  strncpy (newval, list_str, cplen);
  newval[cplen] = '\0';
  remaining -= cplen;
  assert (remaining > 0);

  /* convert hostname to IP */
  for (i = 0; list_pp[i] != NULL && remaining > 0; i++)
    {
      if (i > 0)
	{
	  /* put delimiter */
	  assert (remaining > 1);
	  strncat (newval, ":", 1);
	  remaining -= 1;
	}

      assert (strlen (list_pp[i]) > 0);
      node_addr.s_addr = hostname_to_ip (list_pp[i]);
      if (node_addr.s_addr == INADDR_NONE)
	{
	  assert (false);
	  if (list_pp)
	    {
	      util_free_string_array (list_pp);
	      list_pp = NULL;
	    }

	  return ER_FAILED;
	}

      ip_addr = inet_ntoa (node_addr);
      assert (strlen (ip_addr) > 0);

      assert (remaining > strlen (ip_addr));
      strncat (newval, ip_addr, remaining);
      remaining -= strlen (ip_addr);
    }

  if (list_pp)
    {
      util_free_string_array (list_pp);
      list_pp = NULL;
    }

  prm_set (list_prm, newval, false);

  return NO_ERROR;
}

/*
 * prm_tune_parameters - Sets the values of various system parameters
 *                       depending on the value of other parameters
 *   return: error code
 *
 * Note: Used for providing a mechanism for tuning various system parameters.
 *       The parameters are only tuned if the user has not set them
 *       explictly, this can be ascertained by checking if the default
 *       value has been used.
 */
#if defined (SA_MODE) || defined (SERVER_MODE)
static int
prm_tune_parameters (void)
{
  SYSPRM_PARAM *max_plan_cache_entries_prm;
  SYSPRM_PARAM *max_plan_cache_clones_prm;
  SYSPRM_PARAM *ha_mode_prm;
  SYSPRM_PARAM *test_mode_prm;
  SYSPRM_PARAM *auto_restart_server_prm;
  SYSPRM_PARAM *ha_node_list_prm;
  SYSPRM_PARAM *ha_replica_list_prm;
  SYSPRM_PARAM *max_log_archives_prm;
  SYSPRM_PARAM *force_remove_log_archives_prm;
  SYSPRM_PARAM *call_stack_dump_activation_prm;
  SYSPRM_PARAM *ha_ignore_error_prm;
  SYSPRM_PARAM *ha_node_myself_prm;
  SYSPRM_PARAM *rye_shm_key_prm;

  char newval[LINE_MAX];
  char host_name[MAXHOSTNAMELEN];

  /* Find the parameters that require tuning */
  max_plan_cache_entries_prm =
    prm_find (PRM_NAME_XASL_MAX_PLAN_CACHE_ENTRIES, NULL);
  max_plan_cache_clones_prm =
    prm_find (PRM_NAME_XASL_MAX_PLAN_CACHE_CLONES, NULL);

  ha_mode_prm = prm_find (PRM_NAME_HA_MODE, NULL);
  test_mode_prm = prm_find (PRM_NAME_TEST_MODE, NULL);
  auto_restart_server_prm = prm_find (PRM_NAME_AUTO_RESTART_SERVER, NULL);
  ha_node_list_prm = prm_find (PRM_NAME_HA_NODE_LIST, NULL);
  ha_replica_list_prm = prm_find (PRM_NAME_HA_REPLICA_LIST, NULL);
  max_log_archives_prm = prm_find (PRM_NAME_LOG_MAX_ARCHIVES, NULL);
  force_remove_log_archives_prm =
    prm_find (PRM_NAME_FORCE_REMOVE_LOG_ARCHIVES, NULL);
  ha_node_myself_prm = prm_find (PRM_NAME_HA_NODE_MYSELF, NULL);
  rye_shm_key_prm = prm_find (PRM_NAME_RYE_SHM_KEY, NULL);

  /* check Plan Cache and Query Cache parameters */
  assert (max_plan_cache_entries_prm != NULL);

  if (max_plan_cache_entries_prm == NULL)
    {
      return ER_FAILED;
    }

  if (PRM_GET_INT (max_plan_cache_entries_prm->value) == 0)
    {
      /* 0 means disable plan cache */
      (void) prm_set (max_plan_cache_entries_prm, "-1", false);
    }

  if (PRM_GET_INT (max_plan_cache_entries_prm->value) <= 0)
    {
      /* disable all by default */
    }

  /* check parameters */
  if (ha_mode_prm == NULL || test_mode_prm == NULL
      || auto_restart_server_prm == NULL)
    {
      assert (false);

      return ER_FAILED;
    }

  if (PRM_GET_INT (ha_mode_prm->value) == HA_MODE_OFF)
    {
      return PRM_ERR_BAD_VALUE;
    }

#if defined (SA_MODE)
  /* reset to default 'active mode' */
  (void) prm_set_default (ha_mode_prm);

  if (force_remove_log_archives_prm != NULL
      && !PRM_GET_BOOL (force_remove_log_archives_prm->value))
    {
      (void) prm_set_default (max_log_archives_prm);
    }
#else /* !SERVER_MODE */
  prm_set (auto_restart_server_prm, "no", false);

  if (PRM_GET_INT (ha_mode_prm->value) == HA_MODE_REPLICA)
    {
      prm_set (force_remove_log_archives_prm, "yes", false);
    }
#endif /* SERVER_MODE */

  if (PRM_DEFAULT_VAL_USED (*rye_shm_key_prm->dynamic_flag))
    {
      sprintf (newval, "%x", DEFAULT_RYE_SHM_KEY);
      prm_set (rye_shm_key_prm, newval, false);
    }
  else
    {
      char *str_shm_key;
      int key;

      str_shm_key = PRM_GET_STRING (rye_shm_key_prm->value);
      if (parse_int (&key, str_shm_key, 16) < 0)
	{
	  return PRM_ERR_BAD_VALUE;
	}
    }

  if (ha_node_myself_prm != NULL &&
      !PRM_DEFAULT_VAL_USED (*ha_node_myself_prm->dynamic_flag))
    {
      const char *ha_node_myself = PRM_GET_STRING (ha_node_myself_prm->value);
      if (inet_addr (ha_node_myself) == INADDR_NONE)
	{
	  return ER_FAILED;
	}
    }

  if (ha_node_list_prm == NULL
      || PRM_DEFAULT_VAL_USED (*ha_node_list_prm->dynamic_flag))
    {
      if (GETHOSTNAME (host_name, sizeof (host_name)))
	{
	  strncpy (host_name, "localhost", sizeof (host_name) - 1);
	}

      snprintf (newval, sizeof (newval) - 1, "%s@%s", host_name, host_name);
      prm_set (ha_node_list_prm, newval, false);
    }
  else
    {
      if (prm_tune_hostname_list (ha_node_list_prm,
				  PRM_ID_HA_NODE_LIST) != NO_ERROR)
	{
	  return ER_FAILED;
	}
    }

  if (ha_replica_list_prm == NULL
      || PRM_DEFAULT_VAL_USED (*ha_replica_list_prm->dynamic_flag))
    {
      ;				/* nop */
    }
  else
    {
      if (prm_tune_hostname_list (ha_replica_list_prm,
				  PRM_ID_HA_REPLICA_LIST) != NO_ERROR)
	{
	  return ER_FAILED;
	}
    }

  call_stack_dump_activation_prm =
    GET_PRM (PRM_ID_CALL_STACK_DUMP_ACTIVATION);
  if (!PRM_IS_SET (*call_stack_dump_activation_prm->dynamic_flag))
    {
      int dim;
      int *integer_list = NULL;

      if (PRM_IS_ALLOCATED (*call_stack_dump_activation_prm->dynamic_flag))
	{
	  free_and_init (PRM_GET_INTEGER_LIST
			 (call_stack_dump_activation_prm->value));
	}

      dim = DIM (call_stack_dump_error_codes);
      integer_list = (int *) malloc ((dim + 1) * sizeof (int));
      if (integer_list == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, (dim + 1) * sizeof (int));
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      integer_list[0] = dim;
      memcpy (&integer_list[1], call_stack_dump_error_codes,
	      dim * sizeof (int));
      prm_set_integer_list_value (PRM_ID_CALL_STACK_DUMP_ACTIVATION,
				  integer_list);
      PRM_SET_BIT (PRM_SET, *call_stack_dump_activation_prm->dynamic_flag);
      PRM_CLEAR_BIT (PRM_DEFAULT_USED,
		     *call_stack_dump_activation_prm->dynamic_flag);
    }

  ha_ignore_error_prm = GET_PRM (PRM_ID_HA_IGNORE_ERROR_LIST);
  if (!PRM_IS_SET (*ha_ignore_error_prm->dynamic_flag))
    {
      int dim;
      int *integer_list = NULL;

      if (PRM_IS_ALLOCATED (*ha_ignore_error_prm->dynamic_flag))
	{
	  free_and_init (PRM_GET_INTEGER_LIST (ha_ignore_error_prm->value));
	}

      dim = DIM (ha_ignore_error_codes);
      integer_list = (int *) malloc ((dim + 1) * sizeof (int));
      if (integer_list == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, (dim + 1) * sizeof (int));
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      integer_list[0] = dim;
      memcpy (&integer_list[1], ha_ignore_error_codes, dim * sizeof (int));
      prm_set_integer_list_value (PRM_ID_HA_IGNORE_ERROR_LIST, integer_list);
      PRM_SET_BIT (PRM_SET, *ha_ignore_error_prm->dynamic_flag);
      PRM_CLEAR_BIT (PRM_DEFAULT_USED, *ha_ignore_error_prm->dynamic_flag);
    }

  return NO_ERROR;
}
#else /* SA_MODE || SERVER_MODE */
static int
prm_tune_parameters (void)
{
  SYSPRM_PARAM *max_plan_cache_entries_prm;
  SYSPRM_PARAM *ha_node_list_prm;
  SYSPRM_PARAM *ha_replica_list_prm;
  SYSPRM_PARAM *ha_mode_prm;
  SYSPRM_PARAM *test_mode_prm;
  SYSPRM_PARAM *ha_copy_log_timeout_prm;
  SYSPRM_PARAM *ha_check_disk_failure_interval_prm;
  SYSPRM_PARAM *ha_ignore_error_prm;
  SYSPRM_PARAM *ha_node_myself_prm;
  SYSPRM_PARAM *rye_shm_key_prm;

  char newval[LINE_MAX];
  char host_name[MAXHOSTNAMELEN];

  INT64 ha_check_disk_failure_interval_value;
  INT64 ha_copy_log_timeout_value;

  /* Find the parameters that require tuning */
  max_plan_cache_entries_prm =
    prm_find (PRM_NAME_XASL_MAX_PLAN_CACHE_ENTRIES, NULL);
  ha_node_list_prm = prm_find (PRM_NAME_HA_NODE_LIST, NULL);
  ha_replica_list_prm = prm_find (PRM_NAME_HA_REPLICA_LIST, NULL);

  ha_mode_prm = prm_find (PRM_NAME_HA_MODE, NULL);
  test_mode_prm = prm_find (PRM_NAME_TEST_MODE, NULL);
  ha_copy_log_timeout_prm = prm_find (PRM_NAME_HA_COPY_LOG_TIMEOUT, NULL);
  ha_check_disk_failure_interval_prm =
    prm_find (PRM_NAME_HA_CHECK_DISK_FAILURE_INTERVAL, NULL);
  ha_node_myself_prm = prm_find (PRM_NAME_HA_NODE_MYSELF, NULL);
  rye_shm_key_prm = prm_find (PRM_NAME_RYE_SHM_KEY, NULL);

  assert (max_plan_cache_entries_prm != NULL);
  if (max_plan_cache_entries_prm == NULL)
    {
      return ER_FAILED;
    }

  assert (rye_shm_key_prm != NULL);
  if (rye_shm_key_prm == NULL)
    {
      return ER_FAILED;
    }

  /* check Plan Cache and Query Cache parameters */
  if (PRM_GET_INT (max_plan_cache_entries_prm->value) == 0)
    {
      /* 0 means disable plan cache */
      (void) prm_set (max_plan_cache_entries_prm, "-1", false);
    }

  assert (ha_check_disk_failure_interval_prm != NULL);
  if (ha_check_disk_failure_interval_prm == NULL)
    {
      return ER_FAILED;
    }

  ha_check_disk_failure_interval_value =
    PRM_GET_BIGINT (ha_check_disk_failure_interval_prm->value);
  ha_copy_log_timeout_value = PRM_GET_BIGINT (ha_copy_log_timeout_prm->value);
  if (ha_copy_log_timeout_value == -1)
    {
      prm_set (ha_check_disk_failure_interval_prm, "0", false);
    }
  else if (ha_check_disk_failure_interval_value -
	   ha_copy_log_timeout_value <
	   HB_MIN_DIFF_CHECK_DISK_FAILURE_INTERVAL)
    {
      ha_check_disk_failure_interval_value =
	ha_copy_log_timeout_value + HB_MIN_DIFF_CHECK_DISK_FAILURE_INTERVAL;
      sprintf (newval, "%ld", ha_check_disk_failure_interval_value);
      prm_set (ha_check_disk_failure_interval_prm, newval, false);
    }

  if (PRM_DEFAULT_VAL_USED (*rye_shm_key_prm->dynamic_flag))
    {
      sprintf (newval, "%x", DEFAULT_RYE_SHM_KEY);
      prm_set (rye_shm_key_prm, newval, false);
    }

  if (ha_node_myself_prm != NULL &&
      !PRM_DEFAULT_VAL_USED (*ha_node_myself_prm->dynamic_flag))
    {
      const char *ha_node_myself = PRM_GET_STRING (ha_node_myself_prm->value);
      if (inet_addr (ha_node_myself) == INADDR_NONE)
	{
	  return ER_FAILED;
	}
    }

  if (ha_node_list_prm == NULL
      || PRM_DEFAULT_VAL_USED (*ha_node_list_prm->dynamic_flag))
    {
      if (GETHOSTNAME (host_name, sizeof (host_name)))
	{
	  strncpy (host_name, "localhost", sizeof (host_name) - 1);
	}

      snprintf (newval, sizeof (newval) - 1, "%s@%s", host_name, host_name);
      prm_set (ha_node_list_prm, newval, false);
    }
  else
    {
      if (prm_tune_hostname_list (ha_node_list_prm,
				  PRM_ID_HA_NODE_LIST) != NO_ERROR)
	{
	  return ER_FAILED;
	}
    }

  if (ha_replica_list_prm == NULL
      || PRM_DEFAULT_VAL_USED (*ha_replica_list_prm->dynamic_flag))
    {
      ;				/* nop */
    }
  else
    {
      if (prm_tune_hostname_list (ha_replica_list_prm,
				  PRM_ID_HA_REPLICA_LIST) != NO_ERROR)
	{
	  return ER_FAILED;
	}
    }

  assert (ha_mode_prm != NULL);
  assert (test_mode_prm != NULL);

  if (PRM_GET_INT (ha_mode_prm->value) == HA_MODE_OFF)
    {
      return PRM_ERR_BAD_VALUE;
    }

  ha_ignore_error_prm = GET_PRM (PRM_ID_HA_IGNORE_ERROR_LIST);
  if (!PRM_IS_SET (*ha_ignore_error_prm->dynamic_flag))
    {
      int dim;
      int *integer_list = NULL;

      if (PRM_IS_ALLOCATED (*ha_ignore_error_prm->dynamic_flag))
	{
	  free_and_init (PRM_GET_INTEGER_LIST (ha_ignore_error_prm->value));
	}

      dim = DIM (ha_ignore_error_codes);
      integer_list = (int *) malloc ((dim + 1) * sizeof (int));
      if (integer_list == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, (dim + 1) * sizeof (int));
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      integer_list[0] = dim;
      memcpy (&integer_list[1], ha_ignore_error_codes, dim * sizeof (int));
      prm_set_integer_list_value (PRM_ID_HA_IGNORE_ERROR_LIST, integer_list);
      PRM_SET_BIT (PRM_SET, *ha_ignore_error_prm->dynamic_flag);
      PRM_CLEAR_BIT (PRM_DEFAULT_USED, *ha_ignore_error_prm->dynamic_flag);
    }

  return NO_ERROR;
}
#endif /* CS_MODE */

#if defined (CS_MODE)
/*
 * sysprm_tune_client_parameters () - Synchronize system parameters marked
 *				      with PRM_FORCE_SERVER flag with server.
 *
 * return : void.
 */
void
sysprm_tune_client_parameters (void)
{
  SYSPRM_ASSIGN_VALUE *force_server_values = NULL;

  /* get values from server */
  if (sysprm_get_force_server_parameters (&force_server_values) == NO_ERROR
      && force_server_values != NULL)
    {
      /* update system parameters on client */
      (void) sysprm_change_parameter_values (force_server_values, false,
					     true);
    }

  /* free list of assign_values */
  sysprm_free_assign_values (&force_server_values);
}
#endif /* CS_MODE */

int
prm_get_master_port_id (void)
{
  return PRM_TCP_PORT_ID;
}

bool
prm_get_commit_on_shutdown (void)
{
  return PRM_COMMIT_ON_SHUTDOWN;
}

#if !defined (SERVER_MODE)
/*
 * prm_get_next_param_value - get next param=value token from a string
 *			      containing a "param1=val1;param2=val2..." list
 *   return: NO_ERROR or error code if data format is incorrect
 *   data (in): the string containing the list
 *   prm (out): parameter name
 *   val (out): parameter value
 */
static int
prm_get_next_param_value (char **data, char **prm, char **val)
{
  char *p = *data;
  char *name = NULL;
  char *value = NULL;
  int err = PRM_ERR_NO_ERROR;

  while (char_isspace (*p))
    {
      p++;
    }

  if (*p == '\0')
    {
      /* reached the end of the list */
      err = PRM_ERR_NO_ERROR;
      goto cleanup;
    }

  name = p;
  while (*p && !char_isspace (*p) && *p != '=')
    {
      p++;
    }

  if (*p == '\0')
    {
      err = PRM_ERR_BAD_VALUE;
      goto cleanup;
    }
  else if (*p == '=')
    {
      *p++ = '\0';
    }
  else
    {
      *p++ = '\0';
      while (char_isspace (*p))
	{
	  p++;
	}
      if (*p == '=')
	{
	  p++;
	}
    }

  while (char_isspace (*p))
    {
      p++;
    }
  if (*p == '\0')
    {
      err = PRM_ERR_NO_ERROR;
      goto cleanup;
    }

  value = p;

  if (*p == '"' || *p == '\'')
    {
      char *t, delim;

      delim = *p++;
      value = t = p;
      while (*t && *t != delim)
	{
	  if (*t == '\\')
	    {
	      t++;
	    }
	  *p++ = *t++;
	}
      if (*t != delim)
	{
	  err = PRM_ERR_BAD_STRING;
	  goto cleanup;
	}
    }
  else
    {
      while (*p && !char_isspace (*p) && *p != ';')
	{
	  p++;
	}
    }

  if (*p)
    {
      *p++ = '\0';
      while (char_isspace (*p))
	{
	  p++;
	}
      if (*p == ';')
	{
	  p++;
	}
    }

  *data = p;
  *val = value;
  *prm = name;

  return err;

cleanup:
  *prm = NULL;
  *val = NULL;
  *data = NULL;

  return err;
}
#endif

/*
 * prm_get_name () - returns the name of a parameter
 *
 * return      : parameter name
 * prm_id (in) : parameter id
 */
const char *
prm_get_name (PARAM_ID prm_id)
{
  assert (prm_id <= PRM_LAST_ID);

  if (prm_Def_is_initialized == false)
    {
      sysprm_initialize_prm_def ();
    }

  return prm_Def[prm_id].name;
}

/*
 * prm_get_value () - returns a pointer to the value of a system parameter
 *
 * return      : pointer to value
 * prm_id (in) : parameter id
 *
 * NOTE:
 */
void *
prm_get_value (PARAM_ID prm_id)
{
  assert (prm_id <= PRM_LAST_ID);

  if (prm_Def_is_initialized == false)
    {
      sysprm_initialize_prm_def ();
    }

#if defined (SERVER_MODE)
  assert (PRM_IS_FOR_SERVER (prm_Def[prm_id].static_flag));
#elif defined (CS_MODE)
  assert (PRM_IS_FOR_CLIENT (prm_Def[prm_id].static_flag));
#endif

  if (!PRM_IS_BIGINT (&prm_Def[prm_id])
      && (PRM_HAS_SIZE_UNIT (prm_Def[prm_id].static_flag)
	  || PRM_HAS_TIME_UNIT (prm_Def[prm_id].static_flag)))
    {
      assert (false);
    }

  return prm_Def[prm_id].value;
}

/*
 * prm_get_integer_value () - get the value of a parameter of type integer
 *
 * return      : value
 * prm_id (in) : parameter id
 *
 * NOTE: keywords are stored as integers
 */
int
prm_get_integer_value (PARAM_ID prm_id)
{
  if (prm_Def_is_initialized == false)
    {
      sysprm_initialize_prm_def ();
    }

  assert (prm_id <= PRM_LAST_ID);
  assert (PRM_IS_INTEGER (&prm_Def[prm_id])
	  || PRM_IS_KEYWORD (&prm_Def[prm_id]));

  return PRM_GET_INT (prm_get_value (prm_id));
}

/*
 * prm_get_bool_value () - get the value of a parameter of type bool
 *
 * return      : value
 * prm_id (in) : parameter id
 */
bool
prm_get_bool_value (PARAM_ID prm_id)
{
  if (prm_Def_is_initialized == false)
    {
      sysprm_initialize_prm_def ();
    }

  assert (prm_id <= PRM_LAST_ID);
  assert (PRM_IS_BOOLEAN (&prm_Def[prm_id]));

  return PRM_GET_BOOL (prm_get_value (prm_id));
}

/*
 * prm_get_float_value () - get the value of a parameter of type float
 *
 * return      : value
 * prm_id (in) : parameter id
 */
float
prm_get_float_value (PARAM_ID prm_id)
{
  if (prm_Def_is_initialized == false)
    {
      sysprm_initialize_prm_def ();
    }

  assert (prm_id <= PRM_LAST_ID);
  assert (PRM_IS_FLOAT (&prm_Def[prm_id]));

  return PRM_GET_FLOAT (prm_get_value (prm_id));
}

/*
 * prm_get_string_value () - get the value of a parameter of type string
 *
 * return      : value
 * prm_id (in) : parameter id
 */
char *
prm_get_string_value (PARAM_ID prm_id)
{
  if (prm_Def_is_initialized == false)
    {
      sysprm_initialize_prm_def ();
    }

  assert (prm_id <= PRM_LAST_ID);
  assert (PRM_IS_STRING (&prm_Def[prm_id]));

  return PRM_GET_STRING (prm_get_value (prm_id));
}

/*
 * prm_get_integer_list_value () - get the value of a parameter of type
 *				   integer list
 *
 * return      : value
 * prm_id (in) : parameter id
 */
int *
prm_get_integer_list_value (PARAM_ID prm_id)
{
  if (prm_Def_is_initialized == false)
    {
      sysprm_initialize_prm_def ();
    }

  assert (prm_id <= PRM_LAST_ID);
  assert (PRM_IS_INTEGER_LIST (&prm_Def[prm_id]));

  return PRM_GET_INTEGER_LIST (prm_get_value (prm_id));
}

/*
 * prm_get_bigint_value () - get the value of a parameter of type size
 *
 * return      : value
 * prm_id (in) : parameter id
 */
INT64
prm_get_bigint_value (PARAM_ID prm_id)
{
  if (prm_Def_is_initialized == false)
    {
      sysprm_initialize_prm_def ();
    }

  assert (prm_id <= PRM_LAST_ID);
  assert (PRM_IS_BIGINT (&prm_Def[prm_id]));

  return PRM_GET_BIGINT (prm_get_value (prm_id));
}

/*
 * prm_set_integer_value () - set a new value to a parameter of type integer
 *
 * return      : void
 * prm_id (in) : parameter id
 * value (in)  : new value
 *
 * NOTE: keywords are stored as integers
 */
void
prm_set_integer_value (PARAM_ID prm_id, int value)
{
  if (prm_Def_is_initialized == false)
    {
      sysprm_initialize_prm_def ();
    }

  assert (prm_id <= PRM_LAST_ID);
  assert (PRM_IS_INTEGER (&prm_Def[prm_id])
	  || PRM_IS_KEYWORD (&prm_Def[prm_id]));

  PRM_GET_INT (prm_Def[prm_id].value) = value;
}

/*
 * prm_set_bool_value () - set a new value to a parameter of type bool
 *
 * return      : void
 * prm_id (in) : parameter id
 * value (in)  : new value
 */
void
prm_set_bool_value (PARAM_ID prm_id, bool value)
{
  if (prm_Def_is_initialized == false)
    {
      sysprm_initialize_prm_def ();
    }

  assert (prm_id <= PRM_LAST_ID);
  assert (PRM_IS_BOOLEAN (&prm_Def[prm_id]));

  PRM_GET_BOOL (prm_Def[prm_id].value) = value;
}

/*
 * prm_set_float_value () - set a new value to a parameter of type float
 *
 * return      : void
 * prm_id (in) : parameter id
 * value (in)  : new value
 */
void
prm_set_float_value (PARAM_ID prm_id, float value)
{
  if (prm_Def_is_initialized == false)
    {
      sysprm_initialize_prm_def ();
    }

  assert (prm_id <= PRM_LAST_ID);
  assert (PRM_IS_FLOAT (&prm_Def[prm_id]));

  PRM_GET_FLOAT (prm_Def[prm_id].value) = value;
}

/*
 * prm_set_string_value () - set a new value to a parameter of type string
 *
 * return      : void
 * prm_id (in) : parameter id
 * value (in)  : new value
 */
void
prm_set_string_value (PARAM_ID prm_id, char *value)
{
  if (prm_Def_is_initialized == false)
    {
      sysprm_initialize_prm_def ();
    }

  assert (prm_id <= PRM_LAST_ID);
  assert (PRM_IS_STRING (&prm_Def[prm_id]));

  if (PRM_IS_ALLOCATED (*prm_Def[prm_id].dynamic_flag))
    {
      free_and_init (PRM_GET_STRING (prm_Def[prm_id].value));
      PRM_CLEAR_BIT (PRM_ALLOCATED, *prm_Def[prm_id].dynamic_flag);
    }
  PRM_GET_STRING (prm_Def[prm_id].value) = value;
  if (PRM_GET_STRING (prm_Def[prm_id].value) != NULL)
    {
      PRM_SET_BIT (PRM_ALLOCATED, *prm_Def[prm_id].dynamic_flag);
    }
}

/*
 * prm_set_integer_list_value () - set a new value to a parameter of type
 *				   integer list
 *
 * return      : void
 * prm_id (in) : parameter id
 * value (in)  : new value
 */
void
prm_set_integer_list_value (PARAM_ID prm_id, int *value)
{
  if (prm_Def_is_initialized == false)
    {
      sysprm_initialize_prm_def ();
    }

  assert (prm_id <= PRM_LAST_ID);
  assert (PRM_IS_INTEGER_LIST (&prm_Def[prm_id]));

  if (PRM_IS_ALLOCATED (*prm_Def[prm_id].dynamic_flag))
    {
      free_and_init (PRM_GET_INTEGER_LIST (prm_Def[prm_id].value));
      PRM_CLEAR_BIT (PRM_ALLOCATED, *prm_Def[prm_id].dynamic_flag);
    }
  PRM_GET_INTEGER_LIST (prm_Def[prm_id].value) = value;
  if (PRM_GET_INTEGER_LIST (prm_Def[prm_id].value) != NULL)
    {
      PRM_SET_BIT (PRM_ALLOCATED, *prm_Def[prm_id].dynamic_flag);
    }
}

/*
 * prm_set_bigint_value () - set a new value to a parameter of type size
 *
 * return      : void
 * prm_id (in) : parameter id
 * value (in)  : new value
 */
void
prm_set_bigint_value (PARAM_ID prm_id, INT64 value)
{
  if (prm_Def_is_initialized == false)
    {
      sysprm_initialize_prm_def ();
    }

  assert (prm_id <= PRM_LAST_ID);
  assert (PRM_IS_BIGINT (&prm_Def[prm_id]));

  PRM_GET_BIGINT (prm_Def[prm_id].value) = value;
}

/*
 * sysprm_find_err_in_integer_list () - function that searches a error_code in an
 *				     integer list
 *
 * return      : true if error_code is found, false otherwise
 * prm_id (in) : id of the system parameter that contains an integer list
 * error_code (in)  : error_code to look for
 */
bool
sysprm_find_err_in_integer_list (PARAM_ID prm_id, int error_code)
{
  int i;
  int *integer_list = prm_get_integer_list_value (prm_id);

  if (integer_list == NULL)
    {
      return false;
    }

  for (i = 1; i <= integer_list[0]; i++)
    {
      if (integer_list[i] == error_code || integer_list[i] == -error_code)
	{
	  return true;
	}
    }
  return false;
}

/*
 * sysprm_clear_sysprm_value () - Clears a SYSPRM_VALUE.
 *
 * return	 : void.
 * value (in)	 : value that needs cleared.
 * datatype (in) : data type for value.
 */
static void
sysprm_clear_sysprm_value (SYSPRM_VALUE * value, SYSPRM_DATATYPE datatype)
{
  switch (datatype)
    {
    case PRM_STRING:
      free_and_init (value->str);
      break;

    case PRM_INTEGER_LIST:
      free_and_init (value->integer_list);
      break;

    default:
      /* do nothing */
      break;
    }
}

/*
 * sysprm_pack_sysprm_value () - Packs a sysprm_value.
 *
 * return	 : pointer after the packed value.
 * ptr (in)	 : pointer to position where the value should be packed.
 * value (in)	 : sysprm_value to be packed.
 * datatype (in) : value data type.
 */
static char *
sysprm_pack_sysprm_value (char *ptr, SYSPRM_VALUE value,
			  SYSPRM_DATATYPE datatype)
{
  if (ptr == NULL)
    {
      return NULL;
    }

  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  switch (datatype)
    {
    case PRM_INTEGER:
    case PRM_KEYWORD:
      ptr = or_pack_int (ptr, value.i);
      break;

    case PRM_BOOLEAN:
      ptr = or_pack_int (ptr, value.b);
      break;

    case PRM_FLOAT:
      ptr = or_pack_float (ptr, value.f);
      break;

    case PRM_STRING:
      ptr = or_pack_string (ptr, value.str);
      break;

    case PRM_INTEGER_LIST:
      if (value.integer_list != NULL)
	{
	  int i;
	  ptr = or_pack_int (ptr, value.integer_list[0]);
	  for (i = 1; i <= value.integer_list[0]; i++)
	    {
	      ptr = or_pack_int (ptr, value.integer_list[i]);
	    }
	}
      else
	{
	  ptr = or_pack_int (ptr, -1);
	}
      break;

    case PRM_BIGINT:
      ptr = PTR_ALIGN (ptr, MAX_ALIGNMENT);
      ptr = or_pack_int64 (ptr, value.bi);
      break;

    case PRM_NO_TYPE:
      break;
    }

  return ptr;
}

/*
 * sysprm_packed_sysprm_value_length () - size of packed sysprm_value.
 *
 * return	 : size of packed sysprm_value.
 * value (in)	 : sysprm_value.
 * datatype (in) : value data type.
 * offset (in)	 : offset to pointer where sysprm_value will be packed
 *		   (required for PRM_BIGINT data type)
 */
static int
sysprm_packed_sysprm_value_length (SYSPRM_VALUE value,
				   SYSPRM_DATATYPE datatype, int offset)
{
  switch (datatype)
    {
    case PRM_INTEGER:
    case PRM_KEYWORD:
    case PRM_BOOLEAN:
      return OR_INT_SIZE;

    case PRM_FLOAT:
      return OR_FLOAT_SIZE;

    case PRM_STRING:
      return or_packed_string_length (value.str, NULL);

    case PRM_INTEGER_LIST:
      if (value.integer_list != NULL)
	{
	  return OR_INT_SIZE * (value.integer_list[0] + 1);
	}
      else
	{
	  return OR_INT_SIZE;
	}

    case PRM_BIGINT:
      /* pointer will be aligned to MAX_ALIGNMENT */
      return DB_ALIGN (offset, MAX_ALIGNMENT) - offset + OR_INT64_SIZE;

    default:
      return 0;
    }
}

/*
 * sysprm_unpack_sysprm_value () - unpacks a sysprm_value.
 *
 * return        : pointer after the unpacked sysprm_value.
 * ptr (in)      : pointer to the position where sysprm_value is packed.
 * value (out)   : pointer to unpacked sysprm_value.
 * datatype (in) : value data type.
 */
static char *
sysprm_unpack_sysprm_value (char *ptr, SYSPRM_VALUE * value,
			    SYSPRM_DATATYPE datatype)
{
  assert (value != NULL);

  if (ptr == NULL)
    {
      return NULL;
    }

  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  switch (datatype)
    {
    case PRM_INTEGER:
    case PRM_KEYWORD:
      ptr = or_unpack_int (ptr, &value->i);
      break;

    case PRM_BOOLEAN:
      {
	int temp;
	ptr = or_unpack_int (ptr, &temp);
	value->b = temp;
      }
      break;

    case PRM_FLOAT:
      ptr = or_unpack_float (ptr, &value->f);
      break;

    case PRM_STRING:
      {
	const char *str = NULL;
	ptr = or_unpack_string_nocopy (ptr, &str);
	if (str != NULL)
	  {
	    value->str = strdup (str);
	    if (value->str == NULL)
	      {
		er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			ER_OUT_OF_VIRTUAL_MEMORY, 1, strlen (str) + 1);
		return NULL;
	      }
	  }
	else
	  {
	    value->str = NULL;
	  }
      }
      break;

    case PRM_INTEGER_LIST:
      {
	int temp, i;
	ptr = or_unpack_int (ptr, &temp);
	if (temp == -1)
	  {
	    value->integer_list = NULL;
	  }
	else
	  {
	    value->integer_list = (int *) malloc ((temp + 1) * OR_INT_SIZE);
	    if (value->integer_list == NULL)
	      {
		er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			ER_OUT_OF_VIRTUAL_MEMORY, 1,
			(temp + 1) * OR_INT_SIZE);
		return NULL;
	      }
	    else
	      {
		value->integer_list[0] = temp;
		for (i = 1; i <= temp; i++)
		  {
		    ptr = or_unpack_int (ptr, &value->integer_list[i]);
		  }
	      }
	  }
      }
      break;

    case PRM_BIGINT:
      ptr = PTR_ALIGN (ptr, MAX_ALIGNMENT);
      ptr = or_unpack_int64 (ptr, &value->bi);
      break;

    default:
      break;
    }

  return ptr;
}

/*
 * sysprm_pack_assign_values () - packs a list of SYSPRM_ASSIGN_VALUEs.
 *
 * return	      : pointer after the packed list.
 * ptr (in)	      : pointer to position where the list should be packed.
 * assign_values (in) : list of sysprm_assign_values.
 */
char *
sysprm_pack_assign_values (char *ptr,
			   const SYSPRM_ASSIGN_VALUE * assign_values)
{
  const SYSPRM_ASSIGN_VALUE *av;
  char *old_ptr = ptr;
  int count;

  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  /* skip one int -> size of assign_values */
  ptr += OR_INT_SIZE;

  for (av = assign_values, count = 0; av != NULL; av = av->next, count++)
    {
      ptr = or_pack_int (ptr, av->prm_id);
#if 1				/* TODO - */
      ptr = or_pack_int (ptr, 0);	/* is not sent to server */
#else
      ptr = or_pack_int (ptr, (av->persist == true) ? 1 : 0);
#endif
      ptr = sysprm_pack_sysprm_value (ptr, av->value,
				      GET_PRM_DATATYPE (av->prm_id));
    }

  OR_PUT_INT (old_ptr, count);
  return ptr;
}

/*
 * sysprm_packed_assign_values_length () - size of packed list of
 *					   sysprm_assing_values.
 *
 * return	      : size of packed list of sysprm_assing_values.
 * assign_values (in) : list of sysprm_assing_values.
 * offset (in)	      : offset to pointer where assign values will be packed.
 */
int
sysprm_packed_assign_values_length (const SYSPRM_ASSIGN_VALUE * assign_values,
				    int offset)
{
  const SYSPRM_ASSIGN_VALUE *av;
  int size = 0;

  size += OR_INT_SIZE;		/* size of assign_values list */

  for (av = assign_values; av != NULL; av = av->next)
    {
      size += OR_INT_SIZE;	/* prm_id */
      size += OR_INT_SIZE;	/* is_persist */
      size +=
	sysprm_packed_sysprm_value_length (av->value,
					   GET_PRM_DATATYPE (av->prm_id),
					   size + offset);
    }

  return size;
}

/*
 * sysprm_unpack_assign_values () - Unpacks a list of sysprm_assign_values.
 *
 * return		   : pointer after unpacking sysprm_assing_values.
 * ptr (in)		   : pointer to the position where
 *			     sysprm_assing_values are packed.
 * assign_values_ptr (out) : pointer to the unpacked list of
 *			     sysprm_assing_values.
 */
char *
sysprm_unpack_assign_values (char *ptr,
			     SYSPRM_ASSIGN_VALUE ** assign_values_ptr)
{
  SYSPRM_ASSIGN_VALUE *assign_values = NULL, *last_av = NULL;
  SYSPRM_ASSIGN_VALUE *av = NULL;
  int i = 0, count = 0, tmp;

  assert (assign_values_ptr != NULL);
  *assign_values_ptr = NULL;

  if (ptr == NULL)
    {
      return NULL;
    }

  ASSERT_ALIGN (ptr, INT_ALIGNMENT);

  ptr = or_unpack_int (ptr, &count);
  if (count <= 0)
    {
      return ptr;
    }

  for (i = 0; i < count; i++)
    {
      av = (SYSPRM_ASSIGN_VALUE *) malloc (sizeof (SYSPRM_ASSIGN_VALUE));
      if (av == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, sizeof (SYSPRM_ASSIGN_VALUE));
	  goto error;
	}

      ptr = or_unpack_int (ptr, &tmp);
      av->prm_id = tmp;

#if 1				/* TODO - */
      ptr = or_unpack_int (ptr, &tmp);
      assert (tmp == 0);	/* is not sent to server */
      av->persist = false;
#else
      ptr = or_unpack_int (ptr, &tmp);
      assert (tmp == 1 || tmp == 0);
      av->persist = (tmp == 1) ? true : false;
#endif

      ptr = sysprm_unpack_sysprm_value (ptr, &av->value,
					GET_PRM_DATATYPE (av->prm_id));
      if (ptr == NULL)
	{
	  sysprm_free_assign_values (&av);
	  goto error;
	}
      av->next = NULL;
      if (assign_values != NULL)
	{
	  last_av->next = av;
	  last_av = av;
	}
      else
	{
	  assign_values = last_av = av;
	}
    }

  *assign_values_ptr = assign_values;
  return ptr;

error:
  sysprm_free_assign_values (&assign_values);
  return NULL;
}

/*
 * sysprm_free_assign_values () - free a list of sysprm_assign_values.
 *
 * return		      : void
 * assign_values_ptr (in/out) : pointer to list to free.
 */
void
sysprm_free_assign_values (SYSPRM_ASSIGN_VALUE ** assign_values_ptr)
{
  SYSPRM_ASSIGN_VALUE *av = NULL, *save_next = NULL;

  if (assign_values_ptr == NULL || *assign_values_ptr == NULL)
    {
      return;
    }

  av = *assign_values_ptr;

  while (av != NULL)
    {
      save_next = av->next;
      sysprm_clear_sysprm_value (&av->value, GET_PRM_DATATYPE (av->prm_id));
      free_and_init (av);
      av = save_next;
    }

  *assign_values_ptr = NULL;
}

/*
 * sysprm_compare_values () - compare two system parameter values
 *
 * return	     : comparison result (0 - equal, otherwise - different).
 * first_value (in)  : pointer to first value
 * second_value (in) : pointer to second value
 * val_type (in)     : datatype for values (make sure the values that are
 *		       compared have the same datatype)
 */
static int
sysprm_compare_values (void *first_value, void *second_value,
		       unsigned int val_type)
{
  switch (val_type)
    {
    case PRM_INTEGER:
    case PRM_KEYWORD:
      return (PRM_GET_INT (first_value) != PRM_GET_INT (second_value));

    case PRM_BOOLEAN:
      return (PRM_GET_BOOL (first_value) != PRM_GET_BOOL (second_value));

    case PRM_FLOAT:
      return (PRM_GET_FLOAT (first_value) != PRM_GET_FLOAT (second_value));

    case PRM_STRING:
      {
	char *first_str = PRM_GET_STRING (first_value);
	char *second_str = PRM_GET_STRING (second_value);

	if (first_str == NULL && second_str == NULL)
	  {
	    /* both values are null, return equal */
	    return 0;
	  }

	if (first_str == NULL || second_str == NULL)
	  {
	    /* only one is null, return different */
	    return 1;
	  }

	return intl_mbs_casecmp (first_str, second_str);
      }

    case PRM_BIGINT:
      return (PRM_GET_BIGINT (first_value) != PRM_GET_BIGINT (second_value));

    case PRM_INTEGER_LIST:
      {
	int i;
	int *first_int_list = PRM_GET_INTEGER_LIST (first_value);
	int *second_int_list = PRM_GET_INTEGER_LIST (second_value);

	if (first_int_list == NULL && second_int_list == NULL)
	  {
	    /* both values are null, return equal */
	    return 0;
	  }

	if (second_int_list == NULL || second_int_list == NULL)
	  {
	    /* only one value is null, return different */
	    return 1;
	  }

	if (first_int_list[0] != second_int_list[0])
	  {
	    /* different size for integer lists, return different */
	    return 1;
	  }

	for (i = 1; i <= second_int_list[0]; i++)
	  {
	    if (first_int_list[i] != second_int_list[i])
	      {
		/* found a different integer, return different */
		return 0;
	      }
	  }

	/* all integers are equal, return equal */
	return 1;
      }

    default:
      assert (0);
      break;
    }

  return 0;
}

/*
 * sysprm_set_sysprm_value_from_parameter () - set the value of sysprm_value
 *					       from a system parameter.
 *
 * return	  : void.
 * prm_value (in) : sysprm_value.
 * prm (in)	  : system parameter.
 */
static void
sysprm_set_sysprm_value_from_parameter (SYSPRM_VALUE * prm_value,
					SYSPRM_PARAM * prm)
{
  int size;

  switch (prm->datatype)
    {
    case PRM_INTEGER:
    case PRM_KEYWORD:
      prm_value->i = PRM_GET_INT (prm->value);
      break;
    case PRM_FLOAT:
      prm_value->f = PRM_GET_FLOAT (prm->value);
      break;
    case PRM_BOOLEAN:
      prm_value->b = PRM_GET_BOOL (prm->value);
      break;
    case PRM_BIGINT:
      prm_value->bi = PRM_GET_BIGINT (prm->value);
      break;
    case PRM_STRING:
      if (PRM_GET_STRING (prm->value) != NULL)
	{
	  prm_value->str = strdup (PRM_GET_STRING (prm->value));
	  if (prm_value->str == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1,
		      strlen (PRM_GET_STRING (prm->value)) + 1);
	    }
	}
      else
	{
	  prm_value->str = NULL;
	}
      break;
    case PRM_INTEGER_LIST:
      {
	int *integer_list = PRM_GET_INTEGER_LIST (prm->value);
	if (integer_list != NULL)
	  {
	    size = (integer_list[0] + 1) * sizeof (int);
	    prm_value->integer_list = (int *) malloc (size);
	    if (prm_value->integer_list == NULL)
	      {
		er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			ER_OUT_OF_VIRTUAL_MEMORY, 1, size);
	      }
	    else
	      {
		memcpy (prm_value->integer_list, integer_list, size);
	      }
	  }
	else
	  {
	    prm_value->integer_list = NULL;
	  }
	break;
      }
    case PRM_NO_TYPE:
      break;
    }
}

/*
 * sysprm_print_assign_values () - print list of sysprm_assign_values.
 *
 * return	   : size of printed string.
 * prm_values (in) : list of values that need printing.
 * buffer (in)	   : print destination.
 * length (in)	   : maximum allowed size for printed string.
 */
int
sysprm_print_assign_values (SYSPRM_ASSIGN_VALUE * prm_values, char *buffer,
			    int length)
{
  const SYSPRM_ASSIGN_VALUE *av;
  int n = 0;

  if (length == 0)
    {
      /* don't print anything */
      return 0;
    }

  for (av = prm_values; av != NULL; av = av->next)
    {
      n += sysprm_print_sysprm_value (av->prm_id, av->value,
				      buffer + n, length - n, PRM_PRINT_NAME);
      if (av->next)
	{
	  n += snprintf (buffer + n, length - n, "; ");
	}
    }
  return n;
}

/*
 * sysprm_print_assign_names() - print list of sysprm_assign_values.
 *    return          : size of printed string.
 *
 *    buffer (out)     : print destination.
 *    length (in)     : maximum allowed size for printed string.
 *    prm_values (in) : list of values that need printing.
 */
int
sysprm_print_assign_names (char *buffer, int length,
			   SYSPRM_ASSIGN_VALUE * prm_values)
{
  const SYSPRM_ASSIGN_VALUE *av;
  int n = 0;
  int num_printed = 0;

  if (buffer == NULL || length == 0)
    {
      /* don't print anything */
      return 0;
    }

  for (av = prm_values; av != NULL; av = av->next)
    {
      n = snprintf (buffer + num_printed, length - num_printed, "%s",
		    prm_get_name (av->prm_id));
      if (n < 0)
	{
	  assert (false);
	  return n;
	}

      num_printed += n;
      if (av->next)
	{
	  n = snprintf (buffer + num_printed, length - num_printed, "; ");
	  if (n < 0)
	    {
	      assert (false);
	      return n;
	    }

	  num_printed += n;
	}
    }

  return num_printed;
}

/*
 * sysprm_set_error () - sets an error for system parameter errors
 *
 * return    : error code
 * rc (in)   : SYSPRM_ERR error
 * data (in) : data to be printed with error
 */
int
sysprm_set_error (SYSPRM_ERR rc, const char *data)
{
  int error;

  /* first check if error was already set */
  error = er_errid ();
  if (error != NO_ERROR)
    {
      /* already set */
      return error;
    }

  if (rc != PRM_ERR_NO_ERROR)
    {
      switch (rc)
	{
	case PRM_ERR_UNKNOWN_PARAM:
	case PRM_ERR_BAD_VALUE:
	case PRM_ERR_BAD_STRING:
	case PRM_ERR_BAD_RANGE:
	  if (data)
	    {
	      error = ER_PRM_BAD_VALUE;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, data);
	    }
	  else
	    {
	      error = ER_PRM_BAD_VALUE_NO_DATA;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	    }
	  break;
	case PRM_ERR_CANNOT_CHANGE:
	case PRM_ERR_NOT_FOR_CLIENT:
	case PRM_ERR_NOT_FOR_SERVER:
	  if (data)
	    {
	      error = ER_PRM_CANNOT_CHANGE;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, data);
	    }
	  else
	    {
	      error = ER_PRM_CANNOT_CHANGE_NO_DATA;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	    }
	  break;
	case PRM_ERR_NOT_SOLE_TRAN:
	  error = ER_NOT_SOLE_TRAN;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	  break;
	case PRM_ERR_COMM_ERR:
	  error = ER_NET_SERVER_COMM_ERROR;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
		  "db_set_system_parameters");
	  break;
	case PRM_ERR_NO_MEM_FOR_PRM:
	default:
	  error = ER_GENERIC_ERROR;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
	  break;
	}
    }

  return error;
}
