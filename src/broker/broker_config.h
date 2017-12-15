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
 * broker_config.h - broker configuration utilities
 */

#ifndef _BROKER_CONFIG_H_
#define _BROKER_CONFIG_H_

#include "config.h"
#include "cas_protocol.h"
#include "environment_variable.h"
#include "system_parameter.h"

#define NUM_BUILTIN_BROKERS                     6
#define BR_LOCAL_MGMT_NAME			"_local_mgmt_"
#define BR_SHARD_MGMT_NAME			"_shard_mgmt_"
#define BR_RW_BROKER_NAME			"rw"
#define BR_RO_BROKER_NAME			"ro"
#define BR_SO_BROKER_NAME			"so"
#define BR_REPL_BROKER_NAME                     "repl"
#define SHARD_MGMT_MIN_APPL_SERVER		1
#define SHARD_MGMT_MAX_APPL_SERVER		10

#define BROKER_SECTION_NAME			"broker"
#define SHARD_MGMT_SECTION_NAME			"shard_mgmt"

#define	APPL_SERVER_CAS           0

#define IS_APPL_SERVER_TYPE_CAS(x)	(x == APPL_SERVER_CAS)
#define IS_NOT_APPL_SERVER_TYPE_CAS(x)	!IS_APPL_SERVER_TYPE_CAS(x)

#define APPL_SERVER_CAS_TYPE_NAME               "CAS"

#define MAX_BROKER_NUM          50

#define	CONF_LOG_FILE_LEN	128

#define	DEFAULT_AS_MIN_NUM	5
#define	DEFAULT_AS_MAX_NUM	40

#define	DEFAULT_SERVER_MAX_SIZE	"0"

#define	DEFAULT_SERVER_HARD_LIMIT	"1G"

#define	DEFAULT_TIME_TO_KILL	"2min"
#define SQL_LOG_TIME_MAX	-1

#define CONF_ERR_LOG_NONE       0x00
#define CONF_ERR_LOG_LOGFILE    0x01
#define CONF_ERR_LOG_BROWSER    0x02
#define CONF_ERR_LOG_BOTH       (CONF_ERR_LOG_LOGFILE | CONF_ERR_LOG_BROWSER)

#define DEFAULT_SQL_LOG_MAX_SIZE	"10M"
#define DEFAULT_BROKER_LOG_MAX_SIZE	"10M"
#define DEFAULT_LONG_QUERY_TIME         "1min"
#define DEFAULT_LONG_TRANSACTION_TIME   "1min"
#define DEFAULT_ACCESS_LOG_MAX_SIZE     "10M"
#define MAX_SQL_LOG_MAX_SIZE            2097152	/* 2G */
#define MAX_BROKER_LOG_MAX_SIZE		2097152	/* 2G */
#define MAX_ACCESS_LOG_MAX_SIZE         2097152	/* 2G */
#define DEFAULT_MAX_NUM_DELAYED_HOSTS_LOOKUP    -1

#define BROKER_NAME_LEN		64
#define BROKER_LOG_MSG_SIZE	64

#if !defined(BROKER_PATH_MAX)
#define BROKER_PATH_MAX       (PATH_MAX)
#endif

#define BROKER_INFO_PATH_MAX             (PATH_MAX)
#define BROKER_INFO_NAME_MAX             (BROKER_INFO_PATH_MAX)

typedef enum t_sql_log_mode_value T_SQL_LOG_MODE_VALUE;
enum t_sql_log_mode_value
{
  SQL_LOG_MODE_NONE = 0,
  SQL_LOG_MODE_NOTICE = 1,
  SQL_LOG_MODE_ALL = 2
};

typedef enum
{
  BROKER_LOG_MODE_OFF = 0,
  BROKER_LOG_MODE_ERROR = 1,
  BROKER_LOG_MODE_NOTICE = 2,
  BROKER_LOG_MODE_ALL = BROKER_LOG_MODE_NOTICE
} T_BROKER_LOG_MODE_VALUE;

typedef enum
{
  BROKER_LOG_ERROR = BROKER_LOG_MODE_ERROR,
  BROKER_LOG_NOTICE = BROKER_LOG_MODE_NOTICE
} T_BROKER_LOG_SEVERITY;

typedef enum t_slow_log_value T_SLOW_LOG_VALUE;
enum t_slow_log_value
{
  SLOW_LOG_MODE_OFF = 0,
  SLOW_LOG_MODE_ON = 1,
  SLOW_LOG_MODE_DEFAULT = SLOW_LOG_MODE_ON
};

typedef enum t_keep_con_value T_KEEP_CON_VALUE;
enum t_keep_con_value
{
  KEEP_CON_ON = 1,
  KEEP_CON_AUTO = 2,
  KEEP_CON_DEFAULT = KEEP_CON_AUTO
};

typedef enum t_access_mode_value T_ACCESS_MODE_VALUE;
enum t_access_mode_value
{
  READ_WRITE_ACCESS_MODE = 0,
  READ_ONLY_ACCESS_MODE = 1,
  SLAVE_ONLY_ACCESS_MODE = 2,
  REPL_ACCESS_MODE = 3,
};

/* dbi.h must be updated when a new order is added */
typedef enum t_connect_order_value T_CONNECT_ORDER_VALUE;
enum t_connect_order_value
{
  CONNECT_ORDER_SEQ = 0,
  CONNECT_ORDER_RANDOM = 1,
  CONNECT_ORDER_DEFAULT = CONNECT_ORDER_SEQ
};

typedef enum
{
  NORMAL_BROKER = 0,
  LOCAL_MGMT = 1,
  SHARD_MGMT = 2
} T_BROKER_TYPE;

typedef struct
{
  int err_code;
  int os_err_code;
} T_BR_INIT_ERROR;

typedef struct t_broker_info T_BROKER_INFO;
struct t_broker_info
{
  char broker_type;
  char service_flag;
  char appl_server;
  char auto_add_appl_server;
  char log_backup;
  char access_log;
  char sql_log_mode;
  char slow_log_mode;
  char broker_log_mode;
  char keep_connection;
  char statement_pooling;
  char access_mode;
  char name[BROKER_NAME_LEN];
  int broker_pid;
  int port;
  int appl_server_num;
  int appl_server_min_num;
  int appl_server_max_num;
  int appl_server_shm_key;
  int appl_server_max_size;
  int appl_server_hard_limit;
  int session_timeout;
  int query_timeout;
  int job_queue_size;
  int time_to_kill;
  int sql_log_max_size;
  int broker_log_max_size;
  int long_query_time;		/* msec */
  int long_transaction_time;	/* msec */
  int max_string_length;
  int num_busy_count;
  int max_prepared_stmt_count;
  int access_log_max_size;	/* kbytes */
  char source_env[CONF_LOG_FILE_LEN];
  PRM_NODE_LIST preferred_hosts;

  char ready_to_service;
  char cci_default_autocommit;

  int monitor_hang_interval;
  int hang_timeout;
  int reject_client_count;
  int connect_fail_count;
  int cancel_req_count;
  int ping_req_count;

  char monitor_server_flag;
  char monitor_hang_flag;
  char reject_client_flag;	/* reject clients due to hanging cas */

  char connect_order_random;
  int replica_only_flag;
  int max_num_delayed_hosts_lookup;	/* max num of HA delayed hosts to lookup */

  int cas_rctime;		/* sec */

  int shard_mgmt_num_migrator;
  char shard_metadb[SRV_CON_DBNAME_SIZE];
  char shard_global_dbname[SRV_CON_DBNAME_SIZE];

  time_t start_time;

  T_BR_INIT_ERROR br_init_err;
};

extern int broker_config_read (T_BROKER_INFO * br_info,
			       int *num_broker, int *broker_shm_key,
			       char *admin_log_file, char admin_flag);

extern void broker_config_dump (FILE * fp, const T_BROKER_INFO * br_info,
				int num_broker, int broker_shm_key);

extern int conf_get_value_table_on_off (const char *value);
extern int conf_get_value_sql_log_mode (const char *value);
extern int conf_get_value_broker_log_mode (const char *value);
extern int conf_get_value_keep_con (const char *value);
extern int conf_get_value_access_mode (const char *value);
extern int conf_get_value_connect_order (const char *value);

extern void dir_repath (char *path, size_t path_len);

#if defined(BROKER_VERBOSE_DEBUG)

#define BROKER_ERR(f, a...) do { \
fprintf(stdout, "[%-35s:%05d] <ERR> "f, __FILE__, __LINE__, ##a); \
} while (0);

#define BROKER_INF(f, a...)	do { \
fprintf(stdout, "[%-35s:%05d] <INF> "f, __FILE__, __LINE__, ##a); \
} while (0);

#else /* BROKER_VERBOSE_DEBUG */
#define BROKER_ERR(f, a...)
#define BROKER_INF(f, a...)
#endif

#endif /* _BROKER_CONFIG_H_ */
