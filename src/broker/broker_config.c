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
 * broker_config.c - broker configuration utilities
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <assert.h>

#include "porting.h"
#include "cas_common.h"
#include "broker_config.h"
#include "broker_shm.h"
#include "broker_filename.h"
#include "broker_util.h"
#include "util_func.h"
#include "rye_shm.h"

#include "dbi.h"

#define DEFAULT_SHARD_MGMT_NUM_MIGRATOR	2

#define DEFAULT_BROKER_SHM_KEY		(DEFAULT_RYE_SHM_KEY + DEFUALT_BROKER_SHM_KEY_BASE)
#define DEFAULT_ADMIN_LOG_FILE		"rye_broker.log"
#define DEFAULT_SESSION_TIMEOUT		"5min"
#define DEFAULT_MAX_QUERY_TIMEOUT       "0"
#define DEFAULT_JOB_QUEUE_SIZE		500
#define DEFAULT_APPL_SERVER		"CAS"
#define DEFAULT_EMPTY_STRING		"\0"
#define DEFAULT_FILE_UPLOAD_DELIMITER   "^^"
#define DEFAULT_SQL_LOG_MODE		"NOTICE"
#define DEFAULT_BROKER_LOG_MODE		"ALL"
#define DEFAULT_KEEP_CONNECTION         "AUTO"
#define DEFAULT_JDBC_CACHE_LIFE_TIME    1000
#define DEFAULT_CAS_MAX_PREPARED_STMT	2000
#define DEFAULT_MONITOR_HANG_INTERVAL   60
#define DEFAULT_HANG_TIMEOUT            60
#define DEFAULT_RECONNECT_TIME          "600s"

#define	TRUE	1
#define	FALSE	0

#define PRINT_CONF_ERROR(...)			\
  do {						\
    PRINT_AND_LOG_ERR_MSG(__VA_ARGS__); 	\
  } 						\
  while (0)

#define SHARD_MGMT_CONFIG_FREE(SHARD_MGMT_CONFIG)			\
	do {								\
	  if ((SHARD_MGMT_CONFIG)->metadb)				\
	    {								\
	      util_free_string_array((SHARD_MGMT_CONFIG)->metadb);	\
	      (SHARD_MGMT_CONFIG)->metadb = NULL;			\
	      (SHARD_MGMT_CONFIG)->num_metadb = 0;			\
	    }								\
	} while (0)

typedef struct
{
  bool has_shard_mgmt;
  int port;
  char **metadb;
  int num_metadb;
  int num_migrator;
} T_SHARD_MGMT_CONFIG;

#define INIT_SHARD_MGMT_CONFIG { false, 0, NULL, 0, 0 }

typedef struct
{
  const char *broker_name;
  T_BROKER_TYPE broker_type;
} T_BUILTIN_BROKER;

#define SET_BUILTIN_BROKER(BUILTIN_BROKER, BR_NAME, BR_TYPE)	\
	do {							\
	  T_BUILTIN_BROKER *_tmp_ptr = (BUILTIN_BROKER);	\
	  _tmp_ptr->broker_name = BR_NAME;			\
	  _tmp_ptr->broker_type = BR_TYPE;			\
	} while (0)

typedef struct t_conf_table T_CONF_TABLE;
struct t_conf_table
{
  const char *conf_str;
  int conf_value;
};

enum
{ PARAM_NO_ERROR = 0, PARAM_INVAL_SEC = 1,
  PARAM_BAD_VALUE = 2, PARAM_BAD_RANGE = 3,
  SECTION_NAME_TOO_LONG = 4
};

static int check_port_number (T_BROKER_INFO * br_info, int num_brs);
static int get_conf_value (const char *string, T_CONF_TABLE * conf_table);
static const char *get_conf_string (int value, T_CONF_TABLE * conf_table);

static T_CONF_TABLE tbl_appl_server[] = {
  {APPL_SERVER_CAS_TYPE_NAME, APPL_SERVER_CAS},
  {NULL, 0}
};

static T_CONF_TABLE tbl_on_off[] = {
  {"ON", ON},
  {"OFF", OFF},
  {NULL, 0}
};

static T_CONF_TABLE tbl_sql_log_mode[] = {
  {"ALL", SQL_LOG_MODE_ALL},
  {"ON", SQL_LOG_MODE_ALL},
  {"NOTICE", SQL_LOG_MODE_NOTICE},
  {"NONE", SQL_LOG_MODE_NONE},
  {"OFF", SQL_LOG_MODE_NONE},
  {NULL, 0}
};

static T_CONF_TABLE tbl_broker_log_mode[] = {
  {"OFF", BROKER_LOG_MODE_OFF},
  {"ERROR", BROKER_LOG_MODE_ERROR},
  {"NOTICE", BROKER_LOG_MODE_NOTICE},
  {"ALL", BROKER_LOG_MODE_ALL}
};

static T_CONF_TABLE tbl_keep_connection[] = {
  {"ON", KEEP_CON_ON},
  {"AUTO", KEEP_CON_AUTO},
  {NULL, 0}
};

static T_CONF_TABLE tbl_access_mode[] = {
  {"RW", READ_WRITE_ACCESS_MODE},
  {"RO", READ_ONLY_ACCESS_MODE},
  {"SO", SLAVE_ONLY_ACCESS_MODE},
  {"REPL", REPL_ACCESS_MODE},
  {NULL, 0}
};

static const char *tbl_conf_err_msg[] = {
  "",
  "Cannot find any section in conf file.",
  "Value type does not match parameter type.",
  "Value is out of range.",
  "Section name is too long. Section name must be less than 64."
};

/* conf files that have been loaded */
#define MAX_NUM_OF_CONF_FILE_LOADED     5
static char *conf_File_loaded[MAX_NUM_OF_CONF_FILE_LOADED];

/*
 * conf_file_has_been_loaded - record the file path that has been loaded
 *   return: none
 *   conf_path(in): path of the conf file to be recorded
 */
static void
conf_file_has_been_loaded (const char *conf_path)
{
  int i;
  assert (conf_path != NULL);

  for (i = 0; i < MAX_NUM_OF_CONF_FILE_LOADED; i++)
    {
      if (conf_File_loaded[i] == NULL)
	{
	  conf_File_loaded[i] = strdup (conf_path);
	  return;
	}
    }
}

/*
 * check_port_number - Check broker's port number
 *   return: 0 or -1 if duplicated
 *   br_info(in):
 *   num_brs(in):
 */
static int
check_port_number (T_BROKER_INFO * br_info, int num_brs)
{
  int i, j;
  int error_flag = FALSE;

  for (i = 0; i < num_brs; i++)
    {
      for (j = i + 1; j < num_brs; j++)
	{
	  if (br_info[i].port > 0 && br_info[i].port == br_info[j].port)
	    {
	      printf ("duplicated port number %d\n", br_info[i].port);
	      error_flag = TRUE;
	    }
	}
    }

  if (error_flag == TRUE)
    {
      return -1;
    }
  return 0;
}

/*
 * dir_repath - Fix path to absolute path
 *   return: void
 *   path(in/out):
 */
void
dir_repath (char *path, size_t path_len)
{
  char tmp_str[BROKER_PATH_MAX];

  trim (path);

  if (IS_ABS_PATH (path))
    {
      return;
    }

  STRNCPY (tmp_str, path, BROKER_PATH_MAX);
  snprintf (path, path_len, "%s/%s", envvar_root (), tmp_str);
}

/*
 * get_conf_value_table - get value from table
 *   return: table value or -1 if fail
 *   value(in):
 *   conf_table(in):
 */
static int
get_conf_value (const char *string, T_CONF_TABLE * conf_table)
{
  int i;

  for (i = 0; conf_table[i].conf_str != NULL; i++)
    {
      if (strcasecmp (string, conf_table[i].conf_str) == 0)
	{
	  return conf_table[i].conf_value;
	}
    }
  return -1;
}

static const char *
get_conf_string (int value, T_CONF_TABLE * conf_table)
{
  int i;

  for (i = 0; conf_table[i].conf_str != NULL; i++)
    {
      if (conf_table[i].conf_value == value)
	{
	  return conf_table[i].conf_str;
	}
    }
  return NULL;
}

static int
set_broker_conf (T_BROKER_INFO * br_info, INI_TABLE * ini,
		 T_BROKER_TYPE broker_type, const char *sec_name, int *lineno)
{
  const char *tmp_str;
  float tmp_float;
  char path_buff[BROKER_PATH_MAX];
  PRM_NODE_LIST preferred_hosts;

  tmp_str = ini_getstr (ini, sec_name, "CCI_DEFAULT_AUTOCOMMIT", "ON",
			lineno);
  br_info->cci_default_autocommit = conf_get_value_table_on_off (tmp_str);
  if (br_info->cci_default_autocommit < 0)
    {
      return -1;
    }

  tmp_str = ini_getstr (ini, sec_name, "SERVICE", "ON", lineno);
  br_info->service_flag = conf_get_value_table_on_off (tmp_str);
  if (br_info->service_flag < 0)
    {
      return -1;
    }

  tmp_str = ini_getstr (ini, sec_name, "APPL_SERVER",
			DEFAULT_APPL_SERVER, lineno);
  br_info->appl_server = get_conf_value (tmp_str, tbl_appl_server);
  if (br_info->appl_server < 0)
    {
      return -1;
    }

  br_info->appl_server_min_num = ini_getuint (ini, sec_name,
					      "MIN_NUM_APPL_SERVER",
					      DEFAULT_AS_MIN_NUM, lineno);
  br_info->appl_server_num = br_info->appl_server_min_num;
  if (br_info->appl_server_min_num > APPL_SERVER_NUM_LIMIT)
    {
      return -1;
    }

  br_info->appl_server_max_num = ini_getuint (ini, sec_name,
					      "MAX_NUM_APPL_SERVER",
					      DEFAULT_AS_MAX_NUM, lineno);
  if (br_info->appl_server_max_num > APPL_SERVER_NUM_LIMIT)
    {
      return -1;
    }

  tmp_str = ini_getstr (ini, sec_name, "APPL_SERVER_MAX_SIZE",
			DEFAULT_SERVER_MAX_SIZE, lineno);
  br_info->appl_server_max_size =
    (int) ut_size_string_to_kbyte (tmp_str, "M");
  if (br_info->appl_server_max_size < 0)
    {
      return -1;
    }

  tmp_str = ini_getstr (ini, sec_name, "APPL_SERVER_MAX_SIZE_HARD_LIMIT",
			DEFAULT_SERVER_HARD_LIMIT, lineno);
  br_info->appl_server_hard_limit =
    (int) ut_size_string_to_kbyte (tmp_str, "M");
  if (br_info->appl_server_hard_limit <= 0)
    {
      return -1;
    }

  tmp_str = ini_getstr (ini, sec_name, "SESSION_TIMEOUT",
			DEFAULT_SESSION_TIMEOUT, lineno);
  br_info->session_timeout = (int) ut_time_string_to_sec (tmp_str, "sec");
  if (br_info->session_timeout < 0)
    {
      return -1;
    }

  tmp_str = ini_getstr (ini, sec_name, "LOG_DIR", DEFAULT_LOG_DIR, lineno);
  (void) envvar_ryelogdir_file (path_buff, BROKER_PATH_MAX, tmp_str);
  MAKE_FILEPATH (br_info->log_dir, path_buff, CONF_LOG_FILE_LEN);

  br_info->max_prepared_stmt_count = ini_getint (ini, sec_name,
						 "MAX_PREPARED_STMT_COUNT",
						 DEFAULT_CAS_MAX_PREPARED_STMT,
						 lineno);
  if (br_info->max_prepared_stmt_count < 1)
    {
      return -1;
    }

  tmp_str = ini_getstr (ini, sec_name, "LOG_BACKUP", "OFF", lineno);
  br_info->log_backup = conf_get_value_table_on_off (tmp_str);
  if (br_info->log_backup < 0)
    {
      return -1;
    }

  tmp_str = ini_getstr (ini, sec_name, "SOURCE_ENV",
			DEFAULT_EMPTY_STRING, lineno);
  strcpy (br_info->source_env, tmp_str);

  tmp_str = ini_getstr (ini, sec_name, "SQL_LOG",
			DEFAULT_SQL_LOG_MODE, lineno);
  br_info->sql_log_mode = conf_get_value_sql_log_mode (tmp_str);
  if (br_info->sql_log_mode < 0)
    {
      return -1;
    }

  tmp_str = ini_getstr (ini, sec_name, "BROKER_LOG",
			DEFAULT_BROKER_LOG_MODE, lineno);
  br_info->broker_log_mode = conf_get_value_broker_log_mode (tmp_str);
  if (br_info->broker_log_mode < 0)
    {
      return -1;
    }

  tmp_str = ini_getstr (ini, sec_name, "SLOW_LOG", "ON", lineno);
  br_info->slow_log_mode = conf_get_value_table_on_off (tmp_str);
  if (br_info->slow_log_mode < 0)
    {
      return -1;
    }

  tmp_str = ini_getstr (ini, sec_name, "SQL_LOG_MAX_SIZE",
			DEFAULT_SQL_LOG_MAX_SIZE, lineno);
  br_info->sql_log_max_size = (int) ut_size_string_to_kbyte (tmp_str, "K");
  if (br_info->sql_log_max_size < 0 ||
      br_info->sql_log_max_size > MAX_SQL_LOG_MAX_SIZE)
    {
      return -1;
    }

  tmp_str = ini_getstr (ini, sec_name, "BROKER_LOG_MAX_SIZE",
			DEFAULT_BROKER_LOG_MAX_SIZE, lineno);
  br_info->broker_log_max_size = (int) ut_size_string_to_kbyte (tmp_str, "K");
  if (br_info->broker_log_max_size < 0 ||
      br_info->broker_log_max_size > MAX_SQL_LOG_MAX_SIZE)
    {
      return -1;
    }

  tmp_str = ini_getstr (ini, sec_name, "LONG_QUERY_TIME",
			DEFAULT_LONG_QUERY_TIME, lineno);
  tmp_float = (float) ut_time_string_to_sec (tmp_str, "sec");
  if (tmp_float < 0 || tmp_float > LONG_QUERY_TIME_LIMIT)
    {
      return -1;
    }
  /* change float to msec */
  br_info->long_query_time = (int) (tmp_float * 1000.0);

  tmp_str = ini_getstr (ini, sec_name, "LONG_TRANSACTION_TIME",
			DEFAULT_LONG_TRANSACTION_TIME, lineno);
  tmp_float = (float) ut_time_string_to_sec (tmp_str, "sec");
  if (tmp_float < 0 || tmp_float > LONG_TRANSACTION_TIME_LIMIT)
    {
      return -1;
    }
  /* change float to msec */
  br_info->long_transaction_time = (int) (tmp_float * 1000.0);


  tmp_str = ini_getstr (ini, sec_name, "AUTO_ADD_APPL_SERVER", "ON", lineno);
  br_info->auto_add_appl_server = conf_get_value_table_on_off (tmp_str);
  if (br_info->auto_add_appl_server < 0)
    {
      return -1;
    }

  br_info->job_queue_size = ini_getuint_max (ini, sec_name, "JOB_QUEUE_SIZE",
					     DEFAULT_JOB_QUEUE_SIZE,
					     JOB_QUEUE_MAX_SIZE, lineno);

  tmp_str = ini_getstr (ini, sec_name, "TIME_TO_KILL",
			DEFAULT_TIME_TO_KILL, lineno);
  br_info->time_to_kill = (int) ut_time_string_to_sec (tmp_str, "sec");
  if (br_info->time_to_kill < 0)
    {
      return -1;
    }

  tmp_str = ini_getstr (ini, sec_name, "ACCESS_LOG", "OFF", lineno);
  br_info->access_log = conf_get_value_table_on_off (tmp_str);
  if (br_info->access_log < 0)
    {
      return -1;
    }

  tmp_str = ini_getstr (ini, sec_name, "ACCESS_LOG_MAX_SIZE",
			DEFAULT_ACCESS_LOG_MAX_SIZE, lineno);
  br_info->access_log_max_size = (int) ut_size_string_to_kbyte (tmp_str, "K");
  if (br_info->access_log_max_size < 0 ||
      br_info->access_log_max_size > MAX_ACCESS_LOG_MAX_SIZE)
    {
      return -1;
    }

  br_info->max_string_length = ini_getint (ini, sec_name, "MAX_STRING_LENGTH",
					   -1, lineno);

  tmp_str = ini_getstr (ini, sec_name, "KEEP_CONNECTION",
			DEFAULT_KEEP_CONNECTION, lineno);
  br_info->keep_connection = conf_get_value_keep_con (tmp_str);
  if (br_info->keep_connection < 0)
    {
      return -1;
    }

  tmp_str = ini_getstr (ini, sec_name, "STATEMENT_POOLING", "ON", lineno);
  br_info->statement_pooling = conf_get_value_table_on_off (tmp_str);
  if (br_info->statement_pooling < 0)
    {
      return -1;
    }

  tmp_str = ini_getstr (ini, sec_name, "ACCESS_MODE", "RW", lineno);
  br_info->access_mode = get_conf_value (tmp_str, tbl_access_mode);
  if (br_info->access_mode < 0)
    {
      return -1;
    }

  tmp_str = ini_getstr (ini, sec_name, "REPLICA_ONLY", "OFF", lineno);
  br_info->replica_only_flag = conf_get_value_table_on_off (tmp_str);
  if (br_info->replica_only_flag < 0)
    {
      return -1;
    }

  tmp_str = ini_getstr (ini, sec_name, "PREFERRED_HOSTS",
			DEFAULT_EMPTY_STRING, lineno);
  if (prm_split_node_str (&preferred_hosts, tmp_str, false) != NO_ERROR)
    {
      return -1;
    }
  br_info->preferred_hosts = preferred_hosts;

  tmp_str = ini_getstr (ini, sec_name, "CONNECT_ORDER_RANDOM", "ON", lineno);
  br_info->connect_order_random = conf_get_value_table_on_off (tmp_str);
  if (br_info->connect_order_random < 0)
    {
      return -1;
    }

  br_info->max_num_delayed_hosts_lookup =
    ini_getint (ini, sec_name, "MAX_NUM_DELAYED_HOSTS_LOOKUP",
		DEFAULT_MAX_NUM_DELAYED_HOSTS_LOOKUP, lineno);
  if (br_info->max_num_delayed_hosts_lookup <
      DEFAULT_MAX_NUM_DELAYED_HOSTS_LOOKUP)
    {
      return -1;
    }

  tmp_str = ini_getstr (ini, sec_name, "RECONNECT_TIME",
			DEFAULT_RECONNECT_TIME, lineno);
  br_info->cas_rctime = (int) ut_time_string_to_sec (tmp_str, "sec");
  if (br_info->cas_rctime < 0)
    {
      return -1;
    }

  tmp_str = ini_getstr (ini, sec_name, "MAX_QUERY_TIMEOUT",
			DEFAULT_MAX_QUERY_TIMEOUT, lineno);
  br_info->query_timeout = (int) ut_time_string_to_sec (tmp_str, "sec");
  if (br_info->query_timeout < 0 ||
      br_info->query_timeout > MAX_QUERY_TIMEOUT_LIMIT)
    {
      return -1;
    }

  /* parameters related to checking hanging cas */
  br_info->reject_client_flag = false;

  tmp_str = ini_getstr (ini, sec_name, "ENABLE_MONITOR_HANG", "OFF", lineno);
  br_info->monitor_hang_flag = conf_get_value_table_on_off (tmp_str);
  if (br_info->monitor_hang_flag < 0)
    {
      return -1;
    }
  br_info->monitor_hang_interval = DEFAULT_MONITOR_HANG_INTERVAL;
  br_info->hang_timeout = DEFAULT_HANG_TIMEOUT;

  tmp_str = ini_getstr (ini, sec_name, "ENABLE_MONITOR_SERVER", "ON", lineno);
  br_info->monitor_server_flag = conf_get_value_table_on_off (tmp_str);
  if (br_info->monitor_server_flag < 0)
    {
      return -1;
    }

  if (br_info->appl_server_min_num > br_info->appl_server_max_num)
    {
      br_info->appl_server_max_num = br_info->appl_server_min_num;
    }

  br_info->broker_type = broker_type;
  memset (br_info->shard_metadb, 0, sizeof (br_info->shard_metadb));

  return 0;
}

static void
tune_builtin_broker_conf (T_BROKER_INFO * br_info, int num_brs, int mgmt_port,
			  const T_SHARD_MGMT_CONFIG * shard_mgmt_config)
{
  T_BROKER_INFO *tmp_br_info;
  int i, metadb_idx;

  metadb_idx = 0;
  for (i = 0; i < num_brs; i++)
    {
      if (br_info[i].broker_type != SHARD_MGMT)
	{
	  continue;
	}
      tmp_br_info = &br_info[i];

      tmp_br_info->appl_server_min_num = 0;
      tmp_br_info->appl_server_num = 0;
      tmp_br_info->appl_server_max_num = 0;
      tmp_br_info->shard_mgmt_num_migrator = shard_mgmt_config->num_migrator;
      if (metadb_idx < shard_mgmt_config->num_metadb)
	{
	  tmp_br_info->port = shard_mgmt_config->port + metadb_idx;
	  strncpy (tmp_br_info->shard_metadb,
		   shard_mgmt_config->metadb[metadb_idx],
		   sizeof (tmp_br_info->shard_metadb));
	  metadb_idx++;
	}
      else
	{
	  assert (0);
	  tmp_br_info->service_flag = OFF;
	}
    }

  for (i = 0; i < num_brs; i++)
    {
      if (br_info[i].broker_type != LOCAL_MGMT)
	{
	  continue;
	}
      tmp_br_info = &br_info[i];

      tmp_br_info->appl_server_min_num = 0;
      tmp_br_info->appl_server_num = 0;
      tmp_br_info->appl_server_max_num = 0;
      tmp_br_info->port = mgmt_port;
    }

  tmp_br_info = ut_find_broker (br_info, num_brs, BR_SHARD_MGMT_NAME,
				NORMAL_BROKER);
  if (tmp_br_info != NULL)
    {
      tmp_br_info->access_mode = get_conf_value ("RW", tbl_access_mode);
      tmp_br_info->appl_server_min_num = SHARD_MGMT_MIN_APPL_SERVER;
      tmp_br_info->appl_server_num = SHARD_MGMT_MIN_APPL_SERVER;
      tmp_br_info->appl_server_max_num = SHARD_MGMT_MAX_APPL_SERVER;
    }

  tmp_br_info = ut_find_broker (br_info, num_brs, BR_RW_BROKER_NAME,
				NORMAL_BROKER);
  tmp_br_info->access_mode = get_conf_value ("RW", tbl_access_mode);

  tmp_br_info = ut_find_broker (br_info, num_brs, BR_RO_BROKER_NAME,
				NORMAL_BROKER);
  tmp_br_info->access_mode = get_conf_value ("RO", tbl_access_mode);

  tmp_br_info = ut_find_broker (br_info, num_brs, BR_SO_BROKER_NAME,
				NORMAL_BROKER);
  tmp_br_info->access_mode = get_conf_value ("SO", tbl_access_mode);
  tmp_br_info = ut_find_broker (br_info, num_brs, BR_REPL_BROKER_NAME,
				NORMAL_BROKER);
  tmp_br_info->access_mode = get_conf_value ("REPL", tbl_access_mode);
}

static int
get_broker_section_params (INI_TABLE * ini, int *shm_key_br_gl,
			   int *mgmt_port, char *admin_log_file, int *lineno)
{
  const char *ini_string;
  char path_buff[BROKER_PATH_MAX];

  *shm_key_br_gl = ini_gethex (ini, BROKER_SECTION_NAME, "BROKER_SHM_KEY",
			       DEFAULT_BROKER_SHM_KEY, lineno);
  *mgmt_port = prm_get_local_port_id ();

  if (admin_log_file != NULL)
    {
      ini_string = ini_getstr (ini, BROKER_SECTION_NAME, "ADMIN_LOG_FILE",
			       DEFAULT_ADMIN_LOG_FILE, lineno);

      (void) envvar_ryelogdir_file (path_buff, BROKER_PATH_MAX, ini_string);
      MAKE_FILEPATH (admin_log_file, path_buff, BROKER_PATH_MAX);
    }

  return 0;
}

static int
get_shard_mgmt_config (T_SHARD_MGMT_CONFIG * shard_mgmt_config,
		       INI_TABLE * ini)
{
  bool error_flag = false;
  int lineno = 0;
  const char *tmp_str;
  char **metadb = NULL;
  int num_metadb;

  shard_mgmt_config->port = ini_getint (ini, SHARD_MGMT_SECTION_NAME,
					"SHARD_MGMT_PORT", 0, &lineno);
  if (shard_mgmt_config->port <= 0)
    {
      PRINT_CONF_ERROR ("config error, invalid SHARD_MGMT_PORT\n");
      error_flag = true;
    }

  shard_mgmt_config->num_migrator = ini_getint (ini, SHARD_MGMT_SECTION_NAME,
						"SHARD_MGMT_NUM_MIGRATOR",
						DEFAULT_SHARD_MGMT_NUM_MIGRATOR,
						&lineno);
  if (shard_mgmt_config->num_migrator <= 0)
    {
      PRINT_CONF_ERROR ("config error, invalid SHARD_MGMT_NUM_MIGRATOR\n");
      error_flag = true;
    }

  tmp_str = ini_getstr (ini, SHARD_MGMT_SECTION_NAME,
			"SHARD_MGMT_METADB", "", &lineno);
  metadb = util_split_string (tmp_str, ",");
  if (metadb == NULL)
    {
      PRINT_CONF_ERROR ("config error, invalid SHARD_MGMT_METADB\n");
      error_flag = true;
    }
  else
    {
      int i;
      for (i = 0; metadb[i] != NULL; i++)
	{
	}
      num_metadb = i;

      /* remove invalid metadb name */
      for (i = num_metadb - 1; i >= 0; i--)
	{
	  trim (metadb[i]);

	  if (metadb[i] == NULL || metadb[i][0] == '\0')
	    {
	      num_metadb--;
	      if (metadb[i])
		{
		  free (metadb[i]);
		}
	      metadb[i] = metadb[num_metadb];
	      metadb[num_metadb] = NULL;
	    }
	  else if (strlen (metadb[i]) >= SRV_CON_DBNAME_SIZE)
	    {
	      PRINT_CONF_ERROR ("config error, invalid SHARD_MGMT_METADB\n");
	      error_flag = true;
	    }
	}

      if (num_metadb > 0)
	{
	  shard_mgmt_config->num_metadb = num_metadb;
	  shard_mgmt_config->metadb = metadb;
	}
      else
	{
	  PRINT_CONF_ERROR ("config error, invalid SHARD_MGMT_METADB\n");
	  error_flag = true;
	}
    }

  if (error_flag)
    {
      if (metadb)
	{
	  util_free_string_array (metadb);
	}
      shard_mgmt_config->num_metadb = 0;
      shard_mgmt_config->metadb = NULL;
      return -1;
    }

  shard_mgmt_config->has_shard_mgmt = true;

  return 0;
}

static T_BUILTIN_BROKER *
make_builtin_broker_info (int *num_builtin_brokers,
			  const T_SHARD_MGMT_CONFIG * shard_mgmt_config)
{
  int i, num_brokers;
  T_BUILTIN_BROKER *brokers;

  num_brokers = NUM_BUILTIN_BROKERS + shard_mgmt_config->num_metadb;
  brokers =
    (T_BUILTIN_BROKER *) malloc (sizeof (T_BUILTIN_BROKER) * num_brokers);
  if (brokers == NULL)
    {
      return NULL;
    }

  num_brokers = 0;
  if (shard_mgmt_config->has_shard_mgmt)
    {
      for (i = 0; i < shard_mgmt_config->num_metadb; i++)
	{
	  SET_BUILTIN_BROKER (&brokers[num_brokers++], BR_SHARD_MGMT_NAME,
			      SHARD_MGMT);
	}
    }

  SET_BUILTIN_BROKER (&brokers[num_brokers++], BR_LOCAL_MGMT_NAME,
		      LOCAL_MGMT);
  if (shard_mgmt_config->has_shard_mgmt)
    {
      SET_BUILTIN_BROKER (&brokers[num_brokers++], BR_SHARD_MGMT_NAME,
			  NORMAL_BROKER);
    }
  SET_BUILTIN_BROKER (&brokers[num_brokers++], BR_RW_BROKER_NAME,
		      NORMAL_BROKER);
  SET_BUILTIN_BROKER (&brokers[num_brokers++], BR_RO_BROKER_NAME,
		      NORMAL_BROKER);
  SET_BUILTIN_BROKER (&brokers[num_brokers++], BR_SO_BROKER_NAME,
		      NORMAL_BROKER);
  SET_BUILTIN_BROKER (&brokers[num_brokers++], BR_REPL_BROKER_NAME,
		      NORMAL_BROKER);

  *num_builtin_brokers = num_brokers;
  return brokers;
}


/*
 * broker_config_read_internal - read and parse broker configurations
 *   return: 0 or -1 if fail
 *   br_info(in/out):
 *   num_broker(out):
 *   broker_shm_key(out):
 *   admin_log_file(out):
 *   admin_flag(in):
 *   admin_err_msg(in):
 */
static int
broker_config_read_internal (const char *conf_file,
			     T_BROKER_INFO * br_info, int *num_broker,
			     int *broker_shm_key, char *admin_log_file,
			     char admin_flag)
{
  int num_brs = 0;
  int i;
  int shm_key_br_gl = 0;
  int mgmt_port = 0;
  int error_flag;
  INI_TABLE *ini;
  int lineno = 0;
  int errcode = 0;
  T_SHARD_MGMT_CONFIG shard_mgmt_config = INIT_SHARD_MGMT_CONFIG;

#if 1				/* #955 set PERSIST */
  ini = ini_table_new (0);
  if (ini == NULL)
    {
      assert (false);
      PRINT_CONF_ERROR ("cannot init ini_table\n");
      return -1;
    }

  errcode = db_read_broker_persist_conf_file (ini);
  if (errcode < 0)
    {
      assert (false);
      PRINT_CONF_ERROR ("cannot open conf file %s\n", conf_file);
      ini_parser_free (ini);
      return -1;
    }
#endif

  /* get [broker] section vars */
  if (get_broker_section_params (ini, &shm_key_br_gl, &mgmt_port,
				 admin_log_file, &lineno) < 0)
    {
      goto conf_error;
    }

  if (ini_findsec (ini, SHARD_MGMT_SECTION_NAME))
    {
      if (get_shard_mgmt_config (&shard_mgmt_config, ini) < 0)
	{
	  goto conf_error;
	}
    }

  if (br_info != NULL)
    {
      T_BUILTIN_BROKER *builtin_brokers;
      int num_builtin_brokers;
      int i;

      builtin_brokers = make_builtin_broker_info (&num_builtin_brokers,
						  &shard_mgmt_config);
      if (builtin_brokers == NULL)
	{
	  PRINT_CONF_ERROR ("config error: memory allocation failure\n");
	  goto conf_error;
	}

      for (i = 0; i < num_builtin_brokers; i++)
	{
	  STRNCPY (br_info[num_brs].name, builtin_brokers[i].broker_name,
		   sizeof (br_info[num_brs].name));
	  if (set_broker_conf (&br_info[num_brs], NULL,
			       builtin_brokers[i].broker_type, "",
			       &lineno) < 0)
	    {
	      free (builtin_brokers);
	      errcode = PARAM_BAD_VALUE;
	      goto conf_error;
	    }
	  num_brs++;
	}

      free (builtin_brokers);
    }

  for (i = 0; i < ini->nsec && br_info != NULL; i++)
    {
      char *sec_name;
      T_BROKER_INFO *tmp_br_info;

      sec_name = ini_getsecname (ini, i, &lineno);
      if (sec_name == NULL
	  || strcasecmp (sec_name, BROKER_SECTION_NAME) == 0
	  || strcasecmp (sec_name, SHARD_MGMT_SECTION_NAME) == 0)
	{
	  continue;
	}

      /* sec_name : broker_name */
      if ((strlen (sec_name)) >= BROKER_NAME_LEN)
	{
	  errcode = SECTION_NAME_TOO_LONG;
	  goto conf_error;
	}

      tmp_br_info = ut_find_broker (br_info, num_brs, sec_name,
				    NORMAL_BROKER);
      if (tmp_br_info == NULL)
	{
	  if (num_brs >= MAX_BROKER_NUM)
	    {
	      errcode = PARAM_BAD_RANGE;
	      goto conf_error;
	    }

	  tmp_br_info = &br_info[num_brs];
	  num_brs++;

	  strcpy (tmp_br_info->name, sec_name);
	}

      if (set_broker_conf (tmp_br_info, ini, NORMAL_BROKER, sec_name,
			   &lineno) < 0)
	{
	  errcode = PARAM_BAD_VALUE;
	  goto conf_error;
	}
    }

  if (admin_flag && br_info != NULL)
    {
      tune_builtin_broker_conf (br_info, num_brs, mgmt_port,
				&shard_mgmt_config);
    }

  SHARD_MGMT_CONFIG_FREE (&shard_mgmt_config);
  ini_parser_free (ini);
  ini = NULL;

  error_flag = FALSE;

  if (shm_key_br_gl <= 0)
    {
      PRINT_CONF_ERROR ("config error, invalid BROKER_SHM_KEY\n");
      error_flag = TRUE;
    }

  if (mgmt_port <= 0)
    {
      PRINT_CONF_ERROR ("config error, invalid BROKER_PORT\n");
      error_flag = TRUE;
    }

  if (error_flag == TRUE)
    {
      goto conf_error;
    }

  if (admin_flag && br_info != NULL)
    {
      if (check_port_number (br_info, num_brs) < 0)
	{
	  goto conf_error;
	}

      for (i = 0; i < num_brs; i++)
	{
	  if (br_info[i].source_env[0] != '\0')
	    {
	      dir_repath (br_info[i].source_env, CONF_LOG_FILE_LEN);
	    }
	}
      if (admin_log_file != NULL)
	{
	  dir_repath (admin_log_file, CONF_LOG_FILE_LEN);
	}
    }

  if (num_broker != NULL)
    {
      *num_broker = num_brs;
    }

  if (broker_shm_key != NULL)
    {
      *broker_shm_key = shm_key_br_gl;
    }

  conf_file_has_been_loaded (conf_file);

  return 0;

conf_error:
  PRINT_CONF_ERROR ("Line %d in config file %s : %s\n", lineno, conf_file,
		    tbl_conf_err_msg[errcode]);

  SHARD_MGMT_CONFIG_FREE (&shard_mgmt_config);

  if (ini)
    {
      ini_parser_free (ini);
    }

  return -1;
}

/*
 * broker_config_read - read and parse broker configurations
 *   return: 0 or -1 if fail
 *   br_info(in/out):
 *   num_broker(out):
 *   broker_shm_key(out):
 *   admin_log_file(out):
 *   admin_flag(in):
 *   admin_err_msg(in):
 */
int
broker_config_read (T_BROKER_INFO * br_info,
		    int *num_broker, int *broker_shm_key,
		    char *admin_log_file, char admin_flag)
{
  int err = 0;
  char default_conf_file_path[BROKER_PATH_MAX], file_name[BROKER_PATH_MAX],
    file_being_dealt_with[BROKER_PATH_MAX];

  if (br_info != NULL)
    {
      memset (br_info, 0, sizeof (T_BROKER_INFO) * MAX_BROKER_NUM);
    }

  get_rye_file (FID_RYE_BROKER_CONF, default_conf_file_path, BROKER_PATH_MAX);

  basename_r (default_conf_file_path, file_name, BROKER_PATH_MAX);

  /* $RYE_DATABASES/rye-auto.conf */
  strcpy (file_being_dealt_with, default_conf_file_path);

  err = broker_config_read_internal (file_being_dealt_with, br_info,
				     num_broker, broker_shm_key,
				     admin_log_file, admin_flag);


  return err;
}

/*
 * broker_config_dump - print out current broker configurations
 *   return: none
 *   fp(in):
 */
void
broker_config_dump (FILE * fp, const T_BROKER_INFO * br_info,
		    int num_broker, int broker_shm_key)
{
  int i;
  const char *tmp_str;

  if (br_info == NULL || num_broker <= 0 || num_broker > MAX_BROKER_NUM
      || broker_shm_key <= 0)
    return;

  fprintf (fp, "#\n# rye-auto.conf\n#\n\n");
  fprintf (fp, "# broker parameters were loaded from the files\n");

  for (i = 0; i < MAX_NUM_OF_CONF_FILE_LOADED; i++)
    {
      if (conf_File_loaded[i] != NULL)
	{
	  fprintf (fp, "# %s\n", conf_File_loaded[i]);
	}
    }

  fprintf (fp, "\n# broker parameters\n");

  fprintf (fp, "[broker]\n");
  fprintf (fp, "BROKER_SHM_KEY\t=%x\n\n", broker_shm_key);

  for (i = 0; i < num_broker; i++)
    {
      fprintf (fp, "[%s]\n", br_info[i].name);
      tmp_str = get_conf_string (br_info[i].service_flag, tbl_on_off);
      if (tmp_str)
	{
	  fprintf (fp, "SERVICE\t\t\t=%s\n", tmp_str);
	}
      tmp_str = get_conf_string (br_info[i].appl_server, tbl_appl_server);
      if (tmp_str)
	{
	  fprintf (fp, "APPL_SERVER\t\t=%s\n", tmp_str);
	}
      fprintf (fp, "MIN_NUM_APPL_SERVER\t=%d\n",
	       br_info[i].appl_server_min_num);
      fprintf (fp, "MAX_NUM_APPL_SERVER\t=%d\n",
	       br_info[i].appl_server_max_num);
      fprintf (fp, "APPL_SERVER_MAX_SIZE\t=%d\n",
	       br_info[i].appl_server_max_size / ONE_K);
      fprintf (fp, "SESSION_TIMEOUT\t\t=%d\n", br_info[i].session_timeout);
      fprintf (fp, "LOG_DIR\t\t\t=%s\n", br_info[i].log_dir);
      tmp_str = get_conf_string (br_info[i].log_backup, tbl_on_off);
      if (tmp_str)
	{
	  fprintf (fp, "LOG_BACKUP\t\t=%s\n", tmp_str);
	}
      fprintf (fp, "SOURCE_ENV\t\t=%s\n", br_info[i].source_env);
      tmp_str = get_conf_string (br_info[i].sql_log_mode, tbl_sql_log_mode);
      if (tmp_str)
	{
	  fprintf (fp, "SQL_LOG\t\t\t=%s\n", tmp_str);
	}
      tmp_str =
	get_conf_string (br_info[i].broker_log_mode, tbl_broker_log_mode);
      if (tmp_str)
	{
	  fprintf (fp, "BROKER_LOG\t\t=%s\n", tmp_str);
	}
      tmp_str = get_conf_string (br_info[i].slow_log_mode, tbl_on_off);
      if (tmp_str)
	{
	  fprintf (fp, "SLOW_LOG\t\t=%s\n", tmp_str);
	}
      fprintf (fp, "SQL_LOG_MAX_SIZE\t=%d\n", br_info[i].sql_log_max_size);
      fprintf (fp, "BROKER_LOG_MAX_SIZE\t=%d\n",
	       br_info[i].broker_log_max_size);
      fprintf (fp, "LONG_QUERY_TIME\t\t=%.2f\n",
	       (br_info[i].long_query_time / 1000.0));
      fprintf (fp, "LONG_TRANSACTION_TIME\t=%.2f\n",
	       (br_info[i].long_transaction_time / 1000.0));
      tmp_str = get_conf_string (br_info[i].auto_add_appl_server, tbl_on_off);
      if (tmp_str)
	{
	  fprintf (fp, "AUTO_ADD_APPL_SERVER\t=%s\n", tmp_str);
	}
      fprintf (fp, "JOB_QUEUE_SIZE\t\t=%d\n", br_info[i].job_queue_size);
      fprintf (fp, "TIME_TO_KILL\t\t=%d\n", br_info[i].time_to_kill);
      tmp_str = get_conf_string (br_info[i].access_log, tbl_on_off);
      if (tmp_str)
	{
	  fprintf (fp, "ACCESS_LOG\t\t=%s\n", tmp_str);
	}
      fprintf (fp, "ACCESS_LOG_MAX_SIZE\t=%d\n",
	       (br_info[i].access_log_max_size));
      fprintf (fp, "MAX_STRING_LENGTH\t=%d\n", br_info[i].max_string_length);
      tmp_str =
	get_conf_string (br_info[i].keep_connection, tbl_keep_connection);
      if (tmp_str)
	{
	  fprintf (fp, "KEEP_CONNECTION\t\t=%s\n", tmp_str);
	}
      tmp_str = get_conf_string (br_info[i].statement_pooling, tbl_on_off);
      if (tmp_str)
	{
	  fprintf (fp, "STATEMENT_POOLING\t=%s\n", tmp_str);
	}
      tmp_str = get_conf_string (br_info[i].access_mode, tbl_access_mode);
      if (tmp_str)
	{
	  fprintf (fp, "ACCESS_MODE\t\t=%s\n", tmp_str);
	}
      tmp_str = get_conf_string (br_info[i].connect_order_random, tbl_on_off);
      if (tmp_str)
	{
	  fprintf (fp, "CONNECT_ORDER\t\t=%s\n", tmp_str);
	}

      fprintf (fp, "MAX_NUM_DELAYED_HOSTS_LOOKUP\t=%d\n",
	       br_info[i].max_num_delayed_hosts_lookup);

      fprintf (fp, "RECONNECT_TIME\t\t=%d\n", br_info[i].cas_rctime);

      tmp_str = get_conf_string (br_info[i].replica_only_flag, tbl_on_off);
      if (tmp_str)
	{
	  fprintf (fp, "REPLICA_ONLY\t\t=%s\n", tmp_str);
	}

      fprintf (fp, "MAX_QUERY_TIMEOUT\t=%d\n", br_info[i].query_timeout);

      tmp_str = get_conf_string (br_info[i].monitor_hang_flag, tbl_on_off);
      if (tmp_str)
	{
	  fprintf (fp, "ENABLE_MONITOR_HANG\t=%s\n", tmp_str);
	}
      tmp_str = get_conf_string (br_info[i].monitor_server_flag, tbl_on_off);
      if (tmp_str)
	{
	  fprintf (fp, "ENABLE_MONITOR_SERVER\t=%s\n", tmp_str);
	}
      fprintf (fp, "REJECTED_CLIENTS_COUNT\t=%d\n",
	       br_info[i].reject_client_count);

      fprintf (fp, "BROKER_PORT\t\t\t=%d\n", br_info[i].port);
      fprintf (fp, "APPL_SERVER_NUM\t\t=%d\n", br_info[i].appl_server_num);
      fprintf (fp, "APPL_SERVER_MAX_SIZE_HARD_LIMIT\t=%d\n",
	       br_info[i].appl_server_hard_limit / ONE_K);
      fprintf (fp, "MAX_PREPARED_STMT_COUNT\t=%d\n",
	       br_info[i].max_prepared_stmt_count);
      fprintf (fp, "PREFERRED_HOSTS\t\t=%d\n",
	       br_info[i].preferred_hosts.num_nodes);

      tmp_str =
	get_conf_string (br_info[i].cci_default_autocommit, tbl_on_off);
      if (tmp_str)
	{
	  fprintf (fp, "CCI_DEFAULT_AUTOCOMMIT\t=%s\n", tmp_str);
	}

      fprintf (fp, "MONITOR_HANG_INTERVAL\t=%d\n",
	       br_info[i].monitor_hang_interval);
      fprintf (fp, "HANG_TIMEOUT\t\t=%d\n", br_info[i].hang_timeout);
      tmp_str = get_conf_string (br_info[i].reject_client_flag, tbl_on_off);
      if (tmp_str)
	{
	  fprintf (fp, "REJECT_CLIENT_FLAG\t=%s\n", tmp_str);
	}

      fprintf (fp, "\n");
    }

  return;
}

/*
 * conf_get_value_table_on_off - get value from on/off table
 *   return: 0, 1 or -1 if fail
 *   value(in):
 */
int
conf_get_value_table_on_off (const char *value)
{
  return (get_conf_value (value, tbl_on_off));
}

/*
 * conf_get_value_sql_log_mode - get value from sql_log_mode table
 *   return: -1 if fail
 *   value(in):
 */
int
conf_get_value_sql_log_mode (const char *value)
{
  return (get_conf_value (value, tbl_sql_log_mode));
}

/*
 * conf_get_value_broker_log_mode - get value from broker_log_mode table
 *   return: -1 if fail
 *   value(in):
 */
int
conf_get_value_broker_log_mode (const char *value)
{
  return (get_conf_value (value, tbl_broker_log_mode));
}

/*
 * conf_get_value_keep_con - get value from keep_connection table
 *   return: -1 if fail
 *   value(in):
 */
int
conf_get_value_keep_con (const char *value)
{
  return (get_conf_value (value, tbl_keep_connection));
}

/*
 * conf_get_value_access_mode - get value from access_mode table
 *   return: -1 if fail
 *   value(in):
 */
int
conf_get_value_access_mode (const char *value)
{
  return (get_conf_value (value, tbl_access_mode));
}
