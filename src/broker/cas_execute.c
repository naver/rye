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
 * cas_execute.c -
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <unistd.h>
#include <fcntl.h>
#include <sys/time.h>
#include <assert.h>

#include "cas.h"
#include "cas_common.h"
#include "cas_execute.h"
#include "cas_network.h"
#include "cas_util.h"
#include "cas_log.h"

#include "broker_filename.h"

#include "release_string.h"
#include "perf_monitor.h"
#include "intl_support.h"
#include "language_support.h"
#include "transaction_cl.h"
#include "authenticate.h"
#include "system_parameter.h"
#include "schema_manager.h"
#include "shard_catalog.h"
#include "transform.h"
#include "object_print.h"
#include "boot_cl.h"

#include "log_impl.h"
#include "slotted_page.h"
#include "dbi.h"
#include "repl.h"

#define QUERY_BUFFER_MAX                4096

#define FK_INFO_SORT_BY_PKTABLE_NAME	1
#define FK_INFO_SORT_BY_FKTABLE_NAME	2

typedef enum
{
  NONE_TOKENS,
  SQL_STYLE_COMMENT,
  C_STYLE_COMMENT,
  CPP_STYLE_COMMENT,
  SINGLE_QUOTED_STRING,
  DOUBLE_QUOTED_STRING
} STATEMENT_STATUS;

/* borrowed from optimizer.h: OPT_LEVEL, OPTIMIZATION_ENABLED,
 *                            PLAN_DUMP_ENABLED, SIMPLE_DUMP,
 *                            DETAILED_DUMP
 */
#define CHK_OPT_LEVEL(level)                ((level) & 0xff)
#define CHK_OPTIMIZATION_ENABLED(level)     (CHK_OPT_LEVEL(level) != 0)
#define CHK_PLAN_DUMP_ENABLED(level)        ((level) >= 0x100)
#define CHK_SIMPLE_DUMP(level)              ((level) & 0x100)
#define CHK_DETAILED_DUMP(level)            ((level) & 0x200)
#define CHK_OPTIMIZATION_LEVEL_VALID(level) \
	  (CHK_OPTIMIZATION_ENABLED(level) \
	   || CHK_PLAN_DUMP_ENABLED(level) \
           || (level == 0))

#define DB_VALUE_LIST_FREE(VALUE_LIST, NUM_VALUES)		\
	do {							\
	  db_value_list_free (VALUE_LIST, NUM_VALUES);		\
	  VALUE_LIST = NULL;					\
	} while (0)

#define IS_SELECT_QUERY(type)					\
	((type) == RYE_STMT_SELECT || (type) == RYE_STMT_SELECT_UPDATE)

typedef int (*T_FETCH_FUNC) (T_SRV_HANDLE *, int, int,
			     T_NET_BUF *, T_REQ_INFO *);

typedef struct t_attr_table T_ATTR_TABLE;
struct t_attr_table
{
  const char *class_name;
  const char *attr_name;
  int precision;
  short scale;
  short attr_order;
  char *default_str_alloc_buf;
  const char *default_str;
  char domain;
  char indexed;
  char non_null;
  char unique;
  char is_key;
};

extern void histo_print (FILE * stream);
extern void histo_clear (void);

extern void set_query_timeout (T_SRV_HANDLE * srv_handle, int query_timeout);

static int netval_to_dbval (void *type, void *value, DB_VALUE * db_val);
static int cur_tuple (T_QUERY_RESULT * q_result, int max_col_size,
		      T_NET_BUF * net_buf);
static int dbval_to_net_buf (DB_VALUE * val, T_NET_BUF * net_buf,
			     int max_col_size);
static int prepare_column_list_info_set (DB_SESSION * session,
					 T_QUERY_RESULT * q_result,
					 T_NET_BUF * net_buf);
static int prepare_shard_key_info_set (DB_SESSION * session,
				       T_NET_BUF * net_buf);
static void prepare_column_info_set (T_NET_BUF * net_buf, char ut,
				     short scale, int prec,
				     const char *col_name,
				     const char *default_value,
				     char unique_key, char primary_key,
				     const char *attr_name,
				     const char *class_name, char nullable);
static char set_column_info (T_NET_BUF * net_buf, DB_QUERY_TYPE * col);

static int make_bind_value (int num_bind, void **argv, DB_VALUE ** ret_val);

/*
  fetch_xxx prototype:
  fetch_xxx(T_SRV_HANDLE *, int cursor_pos, int fetch_count, char fetch_flag,
	    int result_set_idx, T_NET_BUF *);
*/
static int fetch_result (T_SRV_HANDLE *, int, int, T_NET_BUF *, T_REQ_INFO *);
static int fetch_schema_column (T_SRV_HANDLE *, int, int, T_NET_BUF *,
				T_REQ_INFO *);
static void add_res_data_null (T_NET_BUF * net_buf, int *net_size);
static void add_res_data_bytes (T_NET_BUF * net_buf, char *str, int size,
				int *net_size);
static void add_res_data_string (T_NET_BUF * net_buf, const char *str,
				 int size, int *net_size);
static void add_res_data_int (T_NET_BUF * net_buf, int value, int *net_size);
static void add_res_data_bigint (T_NET_BUF * net_buf, DB_BIGINT value,
				 int *net_size);
static void add_res_data_double (T_NET_BUF * net_buf, double value,
				 int *net_size);
static void add_res_data_datetime (T_NET_BUF * net_buf, short yr, short mon,
				   short day, short hh, short mm, short ss,
				   short ms, int *net_size);
static void add_res_data_time (T_NET_BUF * net_buf, short hh, short mm,
			       short ss, int *net_size);
static void add_res_data_date (T_NET_BUF * net_buf, short yr, short mon,
			       short day, int *net_size);
static int execute_info_set (T_SRV_HANDLE * srv_handle, T_NET_BUF * net_buf);

static void schema_meta (char schema_type, T_NET_BUF * net_buf);
static int schema_table (char *table_name, char *column_name, T_SRV_HANDLE *);
static int schema_column (char *table_name, char *column_name,
			  T_SRV_HANDLE *);
static int schema_queryspec (char *table_name, char *column_name,
			     T_SRV_HANDLE *);
static int schema_index_info (char *table_name, char *column_name,
			      T_SRV_HANDLE *);
static int schema_table_priv (char *table_name, char *column_name,
			      T_SRV_HANDLE *);
static int schema_column_priv (char *table_name, char *column_name,
			       T_SRV_HANDLE *);
static int schema_primary_key (char *table_name, char *column_name,
			       T_SRV_HANDLE * srv_handle);
static int class_attr_info (const char *class_name, DB_ATTRIBUTE * attr,
			    T_ATTR_TABLE * attr_table);
static int sch_query_execute (T_SRV_HANDLE * srv_handle, char *sql_stmt,
			      int num_bind, DB_VALUE * bind_values);

#define check_class_chn(s) 0
static bool check_auto_commit_after_fetch_done (T_SRV_HANDLE * srv_handle);

static void update_query_execution_count (T_APPL_SERVER_INFO * as_info_p,
					  char stmt_type);
static void update_repl_execution_count (T_APPL_SERVER_INFO * as_info_p,
					 int insert_count, int update_count,
					 int delete_count, int error_count);
static bool need_reconnect_on_rctime (void);

static int set_host_variables (DB_SESSION * session, int num_bind,
			       DB_VALUE * in_values);
static char db_type_to_cas_type (int db_type);
static void db_value_list_free (DB_VALUE * value_list, int num_values);
static bool is_server_ro_tran_executable (char auto_commit, char stmt_type);
static bool is_server_autocommit_executable (char auto_commit);

static int ux_send_repl_ddl_tran (int num_item, void **obj_argv);
static int ux_send_repl_data_tran (int num_item, void **obj_argv);

static char cas_u_type[] = {
  0,				/* 0 */
  CCI_TYPE_INT,			/* DB_TYPE_INTEGER = 1 */
  0,				/* 2 */
  CCI_TYPE_DOUBLE,		/* DB_TYPE_DOUBLE = 3 */
  CCI_TYPE_VARCHAR,		/* DB_TYPE_VARCHAR = 4 */
  CCI_TYPE_VARCHAR,		/* DB_TYPE_OBJECT = 5 */
  0, 0,				/* 6 - 7 */
  CCI_TYPE_VARCHAR,		/* DB_TYPE_SEQUENCE = 8 */
  0,				/* 9 */
  CCI_TYPE_TIME,		/* DB_TYPE_TIME = 10 */
  0,				/* 11 */
  CCI_TYPE_DATE,		/* DB_TYPE_DATE = 12 */
  0, 0, 0, 0, 0, 0, 0, 0, 0,	/* 13 - 21 */
  CCI_TYPE_NUMERIC,		/* DB_TYPE_NUMERIC  = 22 */
  0,				/* 23 */
  CCI_TYPE_VARBIT,		/* DB_TYPE_VARBIT = 24 */
  0, 0, 0,			/* 25 - 27 */
  CCI_TYPE_VARCHAR,		/* DB_TYPE_RESULTSET */
  0, 0,				/* 29 - 30 */
  CCI_TYPE_BIGINT,		/* DB_TYPE_BIGINT = 31 */
  CCI_TYPE_DATETIME		/* DB_TYPE_DATETIME = 32 */
};

static char DB_Name[MAX_HA_DBINFO_LENGTH] = "";
static char DB_User[SRV_CON_DBUSER_SIZE] = "";
static char DB_Passwd[SRV_CON_DBPASSWD_SIZE] = "";
static int saved_Optimization_level = -1;

/*
 * schema info tables
 */

#define SCHEMA_INFO_INFO_STR(TYPE)     \
        schema_info_tbl[(TYPE) - 1].info_str
#define SCHEMA_INFO_EXEC_FUNC(TYPE)     \
        schema_info_tbl[(TYPE) - 1].execute_func
#define SCHEMA_INFO_FETCH_FUNC(TYPE)    \
        schema_info_tbl[(TYPE) - 1].fetch_func
#define SCHEMA_INFO_NUM_COLUMN(TYPE)    \
        schema_info_tbl[(TYPE) - 1].num_col_info
#define SCHEMA_INFO_COLUMN_INFO_TBL(TYPE)       \
        schema_info_tbl[(TYPE) - 1].col_info_tbl

#define SCH_ID_LEN	DB_MAX_IDENTIFIER_LENGTH

typedef int (*T_SCHEMA_INFO_FUNC) (char *, char *, T_SRV_HANDLE *);

typedef struct
{
  int type;
  int precision;
  const char *col_name;
} T_SCHEMA_COL_INFO;

typedef struct
{
  const char *info_str;
  T_SCHEMA_INFO_FUNC execute_func;
  T_FETCH_FUNC fetch_func;
  int num_col_info;
  T_SCHEMA_COL_INFO *col_info_tbl;
} T_SCHEMA_INFO;


static T_SCHEMA_COL_INFO schema_meta_table[] = {
  {CCI_TYPE_VARCHAR, SCH_ID_LEN, "NAME"},
  {CCI_TYPE_INT, 0, "TYPE"}
};

static T_SCHEMA_COL_INFO schema_meta_query_spec[] = {
  {CCI_TYPE_VARCHAR, SCH_ID_LEN, "NAME"}
};

static T_SCHEMA_COL_INFO schema_meta_column[] = {
  {CCI_TYPE_VARCHAR, SCH_ID_LEN, "COLUMN_NAME"},
  {CCI_TYPE_INT, 0, "DATA_TYPE"},
  {CCI_TYPE_INT, 0, "SCALE"},
  {CCI_TYPE_INT, 0, "PRECISION"},
  {CCI_TYPE_INT, 0, "INDEXED"},
  {CCI_TYPE_INT, 0, "NON_NULL"},
  {CCI_TYPE_INT, 0, "UNIQUE"},
  {CCI_TYPE_VARCHAR, 0, "DEFAULT"},
  {CCI_TYPE_INT, 0, "ORDER"},
  {CCI_TYPE_VARCHAR, SCH_ID_LEN, "TABLE_NAME"},
  {CCI_TYPE_INT, 0, "IS_KEY"}
};

static T_SCHEMA_COL_INFO schema_meta_index_info[] = {
  {CCI_TYPE_INT, 0, "TYPE"},
  {CCI_TYPE_INT, 0, "IS_UNIQUE"},
  {CCI_TYPE_VARCHAR, SCH_ID_LEN, "NAME"},
  {CCI_TYPE_INT, 0, "KEY_ORDER"},
  {CCI_TYPE_VARCHAR, SCH_ID_LEN, "COLUMN_NAME"},
  {CCI_TYPE_VARCHAR, 0, "ASC_DESC"},
  {CCI_TYPE_BIGINT, 0, "NUM_KEYS"},
  {CCI_TYPE_INT, 0, "NUM_PAGES"}
};

static T_SCHEMA_COL_INFO schema_meta_privilege[] = {
  {CCI_TYPE_VARCHAR, SCH_ID_LEN, "NAME"},
  {CCI_TYPE_VARCHAR, SCH_ID_LEN, "GRANTOR"},
  {CCI_TYPE_VARCHAR, SCH_ID_LEN, "GRANTEE"},
  {CCI_TYPE_VARCHAR, 10, "PRIVILEGE"},
  {CCI_TYPE_VARCHAR, 5, "GRANTABLE"}
};

static T_SCHEMA_COL_INFO schema_meta_pk[] = {
  {CCI_TYPE_VARCHAR, SCH_ID_LEN, "TABLE_NAME"},
  {CCI_TYPE_VARCHAR, SCH_ID_LEN, "COLUMN_NAME"},
  {CCI_TYPE_INT, 0, "KEY_SEQ"},
  {CCI_TYPE_VARCHAR, SCH_ID_LEN, "KEY_NAME"}
};

#define SET_META_TBL(TBL)       (sizeof(TBL) / sizeof(T_SCHEMA_COL_INFO)), TBL
T_SCHEMA_INFO schema_info_tbl[] = {
  /* CCI_SCH_TABLE */
  {"TABLE", schema_table, fetch_result,
   SET_META_TBL (schema_meta_table)},
  /* CCI_SCH_QUERY_SPEC */
  {"VIEW_QUERY", schema_queryspec, fetch_result,
   SET_META_TBL (schema_meta_query_spec)},
  /* CCI_SCH_COLUMN */
  {"COLUMN", schema_column, fetch_schema_column,
   SET_META_TBL (schema_meta_column)},
  /* CCI_SCH_INDEX_INFO */
  {"INDEX_INFO", schema_index_info, fetch_result,
   SET_META_TBL (schema_meta_index_info)},
  /* CCI_SCH_TABLE_PRIVILEGE */
  {"TABLE_PRIVILEGE", schema_table_priv, fetch_result,
   SET_META_TBL (schema_meta_privilege)},
  /* CCI_SCH_COLUMN_PRIVILEGE */
  {"COLUMN_PRIVILEGE", schema_column_priv, fetch_result,
   SET_META_TBL (schema_meta_privilege)},
  /* CCI_SCH_PRIMARY_KEY */
  {"PRIMRAY_KRY", schema_primary_key, fetch_result,
   SET_META_TBL (schema_meta_pk)},
};

/*****************************
  move from cas_log.c
 *****************************/
/* log error handler related fields */
typedef struct cas_error_log_handle_context_s CAS_ERROR_LOG_HANDLE_CONTEXT;
struct cas_error_log_handle_context_s
{
  unsigned int from;
  unsigned int to;
};
static CAS_ERROR_LOG_HANDLE_CONTEXT *cas_EHCTX = NULL;

int
ux_check_connection (void)
{
  if (ux_is_database_connected ())
    {
      if (db_ping_server (0, NULL) < 0)
	{
	  er_log_debug (ARG_FILE_LINE,
			"ux_check_connection: db_ping_server() error");
	  cas_sql_log_write_and_end (0, "SERVER DOWN");
	  if (as_Info->cur_statement_pooling)
	    {
	      er_log_debug (ARG_FILE_LINE,
			    "ux_check_connection: cur_statement_pooling");
	      return -1;
	    }
	  else
	    {
	      char dbname[MAX_HA_DBINFO_LENGTH];
	      char dbuser[SRV_CON_DBUSER_SIZE];
	      char dbpasswd[SRV_CON_DBPASSWD_SIZE];

	      strncpy (dbname, DB_Name, sizeof (dbname) - 1);
	      dbname[sizeof (dbname) - 1] = '\0';
	      strncpy (dbuser, DB_User, sizeof (dbuser) - 1);
	      dbuser[sizeof (dbuser) - 1] = '\0';
	      strncpy (dbpasswd, DB_Passwd, sizeof (dbpasswd) - 1);
	      dbpasswd[sizeof (dbpasswd) - 1] = '\0';

	      er_log_debug (ARG_FILE_LINE,
			    "ux_check_connection: ux_database_shutdown()"
			    " ux_database_connect(%s, %s)", dbname, dbuser);
	      ux_database_shutdown ();
	      ux_database_connect (dbname, dbuser, dbpasswd, NULL);
	    }
	}
      return 0;
    }
  else
    {
      return -1;
    }
}

int
ux_database_connect (const char *db_name, const char *db_user,
		     const char *db_passwd, char **db_err_msg)
{
  int err_code, client_type;
  const char *p = NULL;
  PRM_NODE_INFO node_connected;
  bool need_au_disable = false;

  if (db_name == NULL || db_name[0] == '\0')
    {
      return ERROR_INFO_SET (-1, CAS_ERROR_INDICATOR);
    }

  node_connected = boot_get_host_connected ();

  if (cas_get_db_connect_status () != 1	/* DB_CONNECTION_STATUS_CONNECTED */
      || DB_Name[0] == '\0'
      || strcmp (DB_Name, db_name) != 0
      || strcmp (DB_User, db_user) != 0
      || strcmp (DB_Passwd, db_passwd) != 0
      || prm_is_same_node (&as_Info->db_node, &node_connected) == false)
    {
      if (cas_get_db_connect_status () == -1)	/* DB_CONNECTION_STATUS_RESET */
	{
	  db_clear_host_connected ();
	}

      if (DB_Name[0] != '\0')
	{
	  ux_database_shutdown ();
	}

      client_type = BOOT_CLIENT_UNKNOWN;
      switch (shm_Appl->access_mode)
	{
	case READ_WRITE_ACCESS_MODE:
	  if (shm_Appl->replica_only_flag)
	    {
	      client_type = BOOT_CLIENT_RW_BROKER_REPLICA_ONLY;
	    }
	  else
	    {
	      client_type = BOOT_CLIENT_READ_WRITE_BROKER;
	    }
	  break;

	case READ_ONLY_ACCESS_MODE:
	  if (shm_Appl->replica_only_flag)
	    {
	      client_type = BOOT_CLIENT_RO_BROKER_REPLICA_ONLY;
	    }
	  else
	    {
	      client_type = BOOT_CLIENT_READ_ONLY_BROKER;
	    }
	  break;

	case SLAVE_ONLY_ACCESS_MODE:
	  if (shm_Appl->replica_only_flag)
	    {
	      client_type = BOOT_CLIENT_SO_BROKER_REPLICA_ONLY;
	    }
	  else
	    {
	      client_type = BOOT_CLIENT_SLAVE_ONLY_BROKER;
	    }
	  break;

	case REPL_ACCESS_MODE:
	  client_type = BOOT_CLIENT_REPL_BROKER;
	  break;

	default:
	  assert (false);
	  return ERROR_INFO_SET (-1, CAS_ERROR_INDICATOR);
	}
      er_log_debug (ARG_FILE_LINE,
		    "ux_database_connect: %s",
		    BOOT_CLIENT_TYPE_STRING (client_type));

      db_set_preferred_hosts (&shm_Appl->preferred_hosts);
      db_set_connect_order_random (shm_Appl->connect_order_random);
      db_set_max_num_delayed_hosts_lookup (shm_Appl->
					   max_num_delayed_hosts_lookup);

      if (client_type == BOOT_CLIENT_REPL_BROKER)
	{
	  if (strncasecmp (shm_Br_master->broker_key, db_passwd,
			   strlen (shm_Br_master->broker_key) != 0))
	    {
	      err_code = CAS_ER_REPL_AUTH;
	      goto connect_error;
	    }

	  AU_DISABLE_PASSWORDS ();
	  need_au_disable = true;
	}
      err_code = db_restart_ex (program_Name, db_name, db_user, db_passwd,
				client_type);
      if (err_code < 0)
	{
	  goto connect_error;
	}

      if (need_au_disable == true)
	{
#if 1
	  AU_RESTORE (1);	/* TODO - avoid compile error */
#else
	  int dummy_au_save;

	  AU_DISABLE (dummy_au_save);
#endif
	}

      p = strchr (db_name, '@');
      if (p)
	{
	  unsigned int cplen = (p - db_name);
	  cplen = MIN (cplen, sizeof (as_Info->database_name) - 1);

	  strncpy (as_Info->database_name, db_name, cplen);
	  as_Info->database_name[cplen] = '\0';
	}
      else
	{
	  strncpy (as_Info->database_name, db_name,
		   sizeof (as_Info->database_name) - 1);
	}
      as_Info->db_node = boot_get_host_connected ();
      as_Info->last_connect_time = time (NULL);

      strncpy (DB_Name, db_name, sizeof (DB_Name) - 1);
      strncpy (DB_User, db_user, sizeof (DB_User) - 1);
      strncpy (DB_Passwd, db_passwd, sizeof (DB_Passwd) - 1);
    }
  else
    {
      int err_code, save;
      /* Already connected to a database, make sure to clear errors from
       * previous clients
       */
      er_clear ();

      if (db_get_client_type () == BOOT_CLIENT_REPL_BROKER)
	{
	  /* don't need check password */
	  return 0;
	}

      /* check password */
      AU_DISABLE (save);
      err_code = au_perform_login (db_user, db_passwd, true);
      AU_ENABLE (save);
      if (err_code < 0)
	{
	  ux_database_shutdown ();

	  return ux_database_connect (db_name, db_user, db_passwd,
				      db_err_msg);
	}
      (void) db_find_or_create_session (db_user, program_Name);

      strncpy (DB_User, db_user, sizeof (DB_User) - 1);
      strncpy (DB_Passwd, db_passwd, sizeof (DB_Passwd) - 1);
    }

  return 0;

connect_error:
  p = db_error_string (1);
  if (p == NULL)
    {
      p = "";
    }

  if (db_err_msg)
    {
      *db_err_msg = (char *) malloc (strlen (p) + 1);
      if (*db_err_msg)
	{
	  strcpy (*db_err_msg, p);
	}
    }

  return ERROR_INFO_SET_WITH_MSG (err_code, DBMS_ERROR_INDICATOR, p);
}

int
ux_change_dbuser (const char *user, const char *passwd)
{
  int err_code;
  char dbname[MAX_HA_DBINFO_LENGTH];

  hm_srv_handle_free_all (true);

  STRNCPY (dbname, DB_Name, sizeof (dbname));

  err_code = ux_database_connect (dbname, user, passwd, NULL);
  if (err_code < 0)
    {
      ux_database_shutdown ();

      return err_code;
    }

  return 0;
}

int
ux_is_database_connected (void)
{
  return (DB_Name[0] != '\0');
}

void
ux_get_current_database_name (char *buf, int bufsize)
{
  char *p;

  STRNCPY (buf, DB_Name, bufsize);

  p = strchr (buf, '@');
  if (p != NULL)
    {
      *p = '\0';
    }
}

void
ux_database_shutdown ()
{
//  int au_save = 0;

  if (db_get_client_type () == BOOT_CLIENT_REPL_BROKER)
    {
      AU_ENABLE_PASSWORDS ();
#if 1
      AU_ENABLE (0);		/* TODO - avoid compile error */
#else
      AU_SAVE_AND_ENABLE (au_save);
#endif
    }

  db_shutdown ();
  sysprm_load_and_init (DB_Name);

  er_log_debug (ARG_FILE_LINE, "ux_database_shutdown: db_shutdown()");
  as_Info->database_name[0] = '\0';
  as_Info->db_node = prm_get_null_node_info ();
  as_Info->database_user[0] = '\0';
  as_Info->last_connect_time = 0;
  memset (DB_Name, 0, sizeof (DB_Name));
  memset (DB_User, 0, sizeof (DB_User));
  memset (DB_Passwd, 0, sizeof (DB_Passwd));
}

int
ux_prepare (char *sql_stmt, int flag, char auto_commit_mode,
	    T_NET_BUF * net_buf, T_REQ_INFO * req_info,
	    unsigned int query_seq_num)
{
  int error;
  T_SRV_HANDLE *srv_handle = NULL;
  DB_SESSION *session = NULL;
  int srv_h_id = -1;
  int err_code;
  char stmt_type;
  T_QUERY_RESULT *q_result = NULL;

  srv_h_id = hm_new_srv_handle (&srv_handle, query_seq_num);
  if (srv_h_id < 0)
    {
      err_code = srv_h_id;
      goto prepare_error;
    }
  srv_handle->schema_type = -1;
  srv_handle->auto_commit_mode = auto_commit_mode;

  RYE_ALLOC_COPY_STR (srv_handle->sql_stmt, sql_stmt);
  if (srv_handle->sql_stmt == NULL)
    {
      err_code = ERROR_INFO_SET (CAS_ER_NO_MORE_MEMORY, CAS_ERROR_INDICATOR);
      goto prepare_error;
    }

  sql_stmt = srv_handle->sql_stmt;

  session = db_open_buffer (sql_stmt);
  if (!session)
    {
      err_code = ERROR_INFO_SET (db_error_code (), DBMS_ERROR_INDICATOR);
      goto prepare_error;
    }

  error = db_compile_statement (session);
  if (error != NO_ERROR)
    {
      err_code = ERROR_INFO_SET (error, DBMS_ERROR_INDICATOR);
      goto prepare_error;
    }

  stmt_type = db_get_statement_type (session);
  if (db_is_select_for_update (session))
    {
      stmt_type = RYE_STMT_SELECT_UPDATE;
    }

  srv_handle->num_markers = db_get_host_var_count (session);
  srv_handle->prepare_flag = flag;

  net_buf_cp_int (net_buf, srv_h_id, NULL);
  net_buf_cp_byte (net_buf, stmt_type);
  net_buf_cp_int (net_buf, srv_handle->num_markers, NULL);

  q_result = (T_QUERY_RESULT *) malloc (sizeof (T_QUERY_RESULT));
  if (q_result == NULL)
    {
      err_code = ERROR_INFO_SET (CAS_ER_NO_MORE_MEMORY, CAS_ERROR_INDICATOR);
      goto prepare_error;
    }

  hm_qresult_clear (q_result);
  q_result->stmt_type = stmt_type;

  err_code = prepare_column_list_info_set (session, q_result, net_buf);
  if (err_code < 0)
    {
      RYE_FREE_MEM (q_result);
      goto prepare_error;
    }

  err_code = prepare_shard_key_info_set (session, net_buf);
  if (err_code < 0)
    {
      RYE_FREE_MEM (q_result);
      goto prepare_error;
    }


  srv_handle->session = session;
  srv_handle->q_result = q_result;

  if (flag & CCI_PREPARE_HOLDABLE)
    {
      srv_handle->is_holdable = true;
    }
  if (flag & CCI_PREPARE_FROM_MIGRATOR)
    {
      db_session_set_from_migrator (session, true);
    }

  db_get_cacheinfo (session, &srv_handle->use_plan_cache);

  return srv_h_id;

prepare_error:
  NET_BUF_ERR_SET (net_buf);

  if (auto_commit_mode == TRUE)
    {
      req_info->need_auto_commit = TRAN_AUTOROLLBACK;
    }

  sql_log_Notice_mode_flush = true;

  if (srv_handle)
    {
      hm_srv_handle_free (srv_h_id);
    }

  if (session)
    {
      db_close_session (session);
    }

  return err_code;
}

int
ux_end_tran (int tran_type, bool reset_con_status)
{
  int err_code;

  if (!as_Info->cur_statement_pooling)
    {
      if (tran_type == CCI_TRAN_COMMIT)
	{
	  hm_srv_handle_free_all (false);
	}
      else
	{
	  hm_srv_handle_free_all (true);
	}
    }
  else
    {
      if (tran_type == CCI_TRAN_COMMIT)
	{
	  /* do not close holdable results on commit */
	  hm_srv_handle_qresult_end_all (false);
	}
      else
	{
	  /* clear all queries */
	  hm_srv_handle_qresult_end_all (true);
	}
    }

  if (tran_type == CCI_TRAN_COMMIT)
    {
      err_code = db_commit_transaction ();
      er_log_debug (ARG_FILE_LINE,
		    "ux_end_tran: db_commit_transaction() = %d", err_code);
      if (err_code < 0)
	{
	  err_code = ERROR_INFO_SET (err_code, DBMS_ERROR_INDICATOR);
	}
    }
  else if (tran_type == CCI_TRAN_ROLLBACK)
    {
      err_code = db_abort_transaction ();
      er_log_debug (ARG_FILE_LINE,
		    "ux_end_tran: db_abort_transaction() = %d", err_code);
      if (err_code < 0)
	{
	  err_code = ERROR_INFO_SET (err_code, DBMS_ERROR_INDICATOR);
	}
    }
  else
    {
      err_code = ERROR_INFO_SET (CAS_ER_INTERNAL, CAS_ERROR_INDICATOR);
    }

  if (err_code >= 0)
    {
      if (reset_con_status)
	{
	  assert_release (as_Info->con_status == CON_STATUS_IN_TRAN
			  || as_Info->con_status == CON_STATUS_OUT_TRAN);
	  as_Info->con_status = CON_STATUS_OUT_TRAN;
	  as_Info->transaction_start_time = (time_t) 0;
	}
    }
  else
    {
      sql_log_Notice_mode_flush = true;
    }

  if (cas_get_db_connect_status () == -1	/* DB_CONNECTION_STATUS_RESET */
      || need_reconnect_on_rctime ())
    {
      db_clear_reconnect_reason ();
      as_Info->reset_flag = TRUE;
    }

  return err_code;
}

int
ux_execute (T_SRV_HANDLE * srv_handle, UNUSED_ARG char flag, int max_col_size,
	    int max_row, int num_bind_value, void **bind_argv,
	    int group_id, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  int err_code;
  DB_VALUE *value_list = NULL;
  int n;
  DB_QUERY_RESULT *result = NULL;
  DB_SESSION *session;
  int stmt_type;

  hm_qresult_end (srv_handle, FALSE);

  session = (DB_SESSION *) srv_handle->session;
  stmt_type = srv_handle->q_result->stmt_type;

  assert (num_bind_value == srv_handle->num_markers);

  if (num_bind_value > 0)
    {
      err_code = make_bind_value (num_bind_value, bind_argv, &value_list);
      if (err_code < 0)
	{
	  goto execute_error;
	}

      err_code = set_host_variables (session, num_bind_value, value_list);
      if (err_code != NO_ERROR)
	{
	  err_code = ERROR_INFO_SET (err_code, DBMS_ERROR_INDICATOR);
	  goto execute_error;
	}
    }

  db_session_set_holdable (srv_handle->session, srv_handle->is_holdable);
  srv_handle->is_from_current_transaction = true;

  db_session_set_autocommit_mode (session,
				  is_server_autocommit_executable
				  (srv_handle->auto_commit_mode));
  db_set_client_ro_tran (is_server_ro_tran_executable
			 (srv_handle->auto_commit_mode, stmt_type));

  query_cancel_enable_sig_handler ();

  db_session_set_groupid (session, group_id);

  n = db_execute_and_keep_statement (session, &result);

  query_cancel_disable_sig_handler ();

  db_set_client_ro_tran (false);

  update_query_execution_count (as_Info, stmt_type);

  if (n < 0)
    {
      if (query_Cancel_flag)
	{
	  n = CAS_ER_QUERY_CANCELLED_BY_USER;
	  err_code = ERROR_INFO_SET (n, CAS_ERROR_INDICATOR);
	}
      else
	{
	  if (srv_handle->is_pooled &&
	      (n == ER_QPROC_INVALID_XASLNODE || n == ER_HEAP_UNKNOWN_OBJECT))
	    {
	      err_code = ERROR_INFO_SET_FORCE (CAS_ER_STMT_POOLING,
					       CAS_ERROR_INDICATOR);
	    }
	  else
	    {
	      err_code = ERROR_INFO_SET (n, DBMS_ERROR_INDICATOR);
	    }
	}
      goto execute_error;
    }
  else if (result != NULL)
    {
      /* success; peek the values in tuples */
      (void) db_query_set_copy_tplvalue (result, 0 /* peek */ );
    }

  if (max_row > 0 && IS_SELECT_QUERY (stmt_type))
    {
      err_code = db_query_seek_tuple (result, max_row, DB_CURSOR_SEEK_SET);
      if (err_code < 0)
	{
	  err_code = ERROR_INFO_SET (err_code, DBMS_ERROR_INDICATOR);
	  goto execute_error;
	}
      else if (err_code == DB_CURSOR_END)
	{
	  n = db_query_tuple_count (result);
	}
      else
	{
	  n = max_row;
	}
      n = MIN (n, max_row);
    }

  net_buf_cp_int (net_buf, n, NULL);

  srv_handle->max_col_size = max_col_size;
  srv_handle->q_result->result = result;
  srv_handle->q_result->tuple_count = n;
  srv_handle->max_row = max_row;

  if (ux_has_stmt_result_set (stmt_type) == true)
    {
      if (srv_handle->is_holdable == true)
	{
	  srv_handle->q_result->is_holdable = true;
	  as_Info->num_holdable_results++;
	}
    }

  db_get_cacheinfo (session, &srv_handle->use_plan_cache);

  if (srv_handle->auto_commit_mode == TRUE)
    {
      req_info->need_auto_commit = TRAN_AUTOCOMMIT;
    }

  DB_VALUE_LIST_FREE (value_list, num_bind_value);

  net_buf_cp_byte (net_buf, 0);	/* clt_cache_reusable */

  err_code = execute_info_set (srv_handle, net_buf);
  if (err_code != NO_ERROR)
    {
      goto execute_error;
    }

  return err_code;

execute_error:
  NET_BUF_ERR_SET (net_buf);

  if (srv_handle->auto_commit_mode)
    {
      req_info->need_auto_commit = TRAN_AUTOROLLBACK;
    }

  sql_log_Notice_mode_flush = true;

  DB_VALUE_LIST_FREE (value_list, num_bind_value);

  return err_code;
}

int
ux_execute_batch (T_SRV_HANDLE * srv_handle, int num_execution,
		  int num_markers, int group_id,
		  void **bind_argv, T_NET_BUF * net_buf)
{
  DB_VALUE *value_list = NULL;
  int err_code;
  int res_count;
  int num_query;
  int num_query_msg_offset;
  const char *err_msg;
  DB_SESSION *session = NULL;
  DB_QUERY_RESULT *result;
  int stmt_type = -1;
  int i;

  if (srv_handle == NULL || srv_handle->schema_type >= CCI_SCH_FIRST)
    {
      err_code = ERROR_INFO_SET (CAS_ER_SRV_HANDLE, CAS_ERROR_INDICATOR);
      goto execute_batch_error;
    }

  hm_qresult_end (srv_handle, FALSE);

  session = (DB_SESSION *) srv_handle->session;

  assert (num_markers == srv_handle->num_markers);

  num_query = 0;
  net_buf_cp_int (net_buf, num_query, &num_query_msg_offset);

  for (i = 0; i < num_execution; i++)
    {
      value_list = NULL;

      num_query++;

      if (num_markers > 0)
	{
	  err_code = make_bind_value (num_markers, bind_argv, &value_list);
	  if (err_code != NO_ERROR)
	    {
	      err_code = ERROR_INFO_SET (err_code, DBMS_ERROR_INDICATOR);
	      goto exec_db_error;
	    }

	  err_code = set_host_variables (session, num_markers, value_list);
	  if (err_code != NO_ERROR)
	    {
	      err_code = ERROR_INFO_SET (err_code, DBMS_ERROR_INDICATOR);
	      goto exec_db_error;
	    }
	}

      db_session_set_autocommit_mode (session,
				      is_server_autocommit_executable
				      (srv_handle->auto_commit_mode));
      db_session_set_groupid (session, group_id);

      res_count = db_execute_and_keep_statement (session, &result);

      if (stmt_type < 0)
	{
	  stmt_type = db_get_statement_type (session);
	}
      update_query_execution_count (as_Info, stmt_type);

      if (res_count < 0)
	{
	  goto exec_db_error;
	}

      db_get_cacheinfo (session, &srv_handle->use_plan_cache);

      /* success; peek the values in tuples */
      if (result != NULL)
	{
	  (void) db_query_set_copy_tplvalue (result, 0 /* peek */ );
	}

      db_query_end (result);

      net_buf_cp_byte (net_buf, EXEC_QUERY_SUCCESS);

      net_buf_cp_int (net_buf, res_count, NULL);

      if (srv_handle->auto_commit_mode == TRUE)
	{
	  db_commit_transaction ();
	}

      goto exec_next;

    exec_db_error:
      err_code = db_error_code ();

      if (err_code < 0)
	{
	  if (srv_handle->auto_commit_mode == FALSE
	      && (ER_IS_SERVER_DOWN_ERROR (err_code)
		  || ER_IS_ABORTED_DUE_TO_DEADLOCK (err_code)))
	    {
	      err_code = ERROR_INFO_SET (err_code, DBMS_ERROR_INDICATOR);
	      goto execute_batch_error;
	    }
	  else
	    {
	      err_msg = db_error_string (1);
	      sql_log_Notice_mode_flush = true;
	    }
	}
      else
	{
	  err_code = -1;
	  err_msg = "";
	}

      net_buf_cp_byte (net_buf, EXEC_QUERY_ERROR);

      net_buf_cp_int (net_buf, DBMS_ERROR_INDICATOR, NULL);
      net_buf_cp_int (net_buf, err_code, NULL);
      net_buf_cp_int (net_buf, strlen (err_msg) + 1, NULL);
      net_buf_cp_str (net_buf, err_msg, strlen (err_msg) + 1);

      if (srv_handle->auto_commit_mode == TRUE)
	{
	  db_abort_transaction ();
	}

      if (err_code == ER_INTERRUPTED)
	{
	  break;
	}

    exec_next:
      DB_VALUE_LIST_FREE (value_list, num_markers);

      bind_argv += (num_markers * 2);
    }

  net_buf_overwrite_int (net_buf, num_query_msg_offset, num_query);

  DB_VALUE_LIST_FREE (value_list, num_markers);

  return 0;

execute_batch_error:
  NET_BUF_ERR_SET (net_buf);
  sql_log_Notice_mode_flush = true;

  DB_VALUE_LIST_FREE (value_list, num_markers);

  return err_code;
}

int
ux_fetch (T_SRV_HANDLE * srv_handle, int cursor_pos, int fetch_count,
	  int result_set_index, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  int err_code;
//  int fetch_func_index;
  T_FETCH_FUNC fetch_func;

  if (srv_handle == NULL)
    {
      err_code = ERROR_INFO_SET (CAS_ER_SRV_HANDLE, CAS_ERROR_INDICATOR);
      goto fetch_error;
    }

  if (srv_handle->schema_type < 0)
    {
//      fetch_func_index = 0;
      fetch_func = fetch_result;
    }
  else if (srv_handle->schema_type >= CCI_SCH_FIRST
	   && srv_handle->schema_type <= CCI_SCH_LAST)
    {
//      fetch_func_index = srv_handle->schema_type;
      fetch_func = SCHEMA_INFO_FETCH_FUNC (srv_handle->schema_type);
    }
  else
    {
      err_code = ERROR_INFO_SET (CAS_ER_SCHEMA_TYPE, CAS_ERROR_INDICATOR);
      goto fetch_error;
    }

  if (fetch_count <= 0)
    {
      fetch_count = 100;
    }

  err_code = (*fetch_func) (srv_handle, cursor_pos,
			    result_set_index, net_buf, req_info);

  if (err_code < 0)
    {
      goto fetch_error;
    }

  return 0;

fetch_error:
  NET_BUF_ERR_SET (net_buf);

  sql_log_Notice_mode_flush = true;
  return err_code;
}

void
ux_cursor_close (T_SRV_HANDLE * srv_handle)
{
  if (srv_handle == NULL)
    {
      return;
    }

  ux_free_result (srv_handle->q_result->result);
  srv_handle->q_result->result = NULL;

  if (srv_handle->q_result->is_holdable == true)
    {
      srv_handle->q_result->is_holdable = false;
      as_Info->num_holdable_results--;
    }
}

int
ux_get_db_version (T_NET_BUF * net_buf)
{
  const char *p;

  p = rel_version_string ();

  if (p == NULL)
    {
      net_buf_cp_int (net_buf, 0, NULL);
    }
  else
    {
      net_buf_cp_int (net_buf, strlen (p) + 1, NULL);
      net_buf_cp_str (net_buf, p, strlen (p) + 1);
    }

  return 0;
}

static int
make_bind_value (int num_bind, void **argv, DB_VALUE ** ret_val)
{
  DB_VALUE *value_list = NULL;
  int i, type_idx, val_idx;
  int err_code;

  *ret_val = NULL;

  value_list = (DB_VALUE *) RYE_MALLOC (sizeof (DB_VALUE) * num_bind);
  if (value_list == NULL)
    {
      return ERROR_INFO_SET (CAS_ER_NO_MORE_MEMORY, CAS_ERROR_INDICATOR);
    }

  memset (value_list, 0, sizeof (DB_VALUE) * num_bind);

  for (i = 0; i < num_bind; i++)
    {
      type_idx = 2 * i;
      val_idx = 2 * i + 1;
      err_code = netval_to_dbval (argv[type_idx], argv[val_idx],
				  &(value_list[i]));
      if (err_code < 0)
	{
	  DB_VALUE_LIST_FREE (value_list, i);
	  return err_code;
	}
    }

  *ret_val = value_list;

  return 0;
}

int
ux_get_query_plan (const char *plan_dump_file, T_NET_BUF * net_buf)
{
  int fd = -1;
  char read_buf[1024];
  int read_len;
  int info_size_offset;
  int cur_size;

  if (plan_dump_file)
    {
      fd = open (plan_dump_file, O_RDONLY);
    }

  if (fd < 0)
    {
      net_buf_cp_int (net_buf, 1, NULL);
      net_buf_cp_byte (net_buf, '\0');
      return 0;
    }

  net_buf_cp_int (net_buf, 0, &info_size_offset);
  cur_size = NET_BUF_CURR_SIZE (net_buf);

  while ((read_len = read (fd, read_buf, sizeof (read_buf))) > 0)
    {
      net_buf_cp_str (net_buf, read_buf, read_len);
    }
  net_buf_cp_byte (net_buf, '\0');

  net_buf_overwrite_int (net_buf, info_size_offset,
			 NET_BUF_CURR_SIZE (net_buf) - cur_size);

  close (fd);

  return 0;
}

void
ux_free_result (void *res)
{
  db_query_end ((DB_QUERY_RESULT *) res);
}

static char
db_type_to_cas_type (int db_type)
{
  if (db_type < DB_TYPE_FIRST || db_type > DB_TYPE_LAST)
    {
      return CCI_TYPE_NULL;
    }

  return (cas_u_type[db_type]);
}

int
ux_schema_info (int schema_type, char *table_name, char *column_name,
		T_NET_BUF * net_buf, unsigned int query_seq_num)
{
  int srv_h_id = 0;
  int err_code = 0;
  int num_result;
  T_SRV_HANDLE *srv_handle = NULL;
  T_SCHEMA_INFO_FUNC schema_exec_func;

  if (schema_type < CCI_SCH_FIRST || schema_type > CCI_SCH_LAST)
    {
      err_code = ERROR_INFO_SET (CAS_ER_SCHEMA_TYPE, CAS_ERROR_INDICATOR);
      goto schema_info_error;
    }

  srv_h_id = hm_new_srv_handle (&srv_handle, query_seq_num);
  if (srv_h_id < 0)
    {
      err_code = srv_h_id;
      goto schema_info_error;
    }
  srv_handle->schema_type = schema_type;

  net_buf_cp_int (net_buf, srv_h_id, NULL);
  net_buf_cp_byte (net_buf, RYE_MAX_STMT_TYPE);	/* statement type:unknown */
  net_buf_cp_int (net_buf, 0, NULL);	/* parameter count */

  schema_exec_func = SCHEMA_INFO_EXEC_FUNC (schema_type);
  num_result = (*schema_exec_func) (table_name, column_name, srv_handle);
  if (num_result < 0)
    {
      err_code = num_result;
      goto schema_info_error;
    }

  schema_meta (schema_type, net_buf);
  net_buf_cp_int (net_buf, num_result, NULL);

  return srv_h_id;

schema_info_error:
  NET_BUF_ERR_SET (net_buf);
  sql_log_Notice_mode_flush = true;
  if (srv_handle)
    {
      hm_srv_handle_free (srv_h_id);
    }

  return err_code;
}

static void
schema_meta (char schema_type, T_NET_BUF * net_buf)
{
  int num_col_info;
  T_SCHEMA_COL_INFO *col_info_tbl;
  short scale = 0;
  int i;

  num_col_info = SCHEMA_INFO_NUM_COLUMN (schema_type);
  col_info_tbl = SCHEMA_INFO_COLUMN_INFO_TBL (schema_type);

  net_buf_cp_int (net_buf, num_col_info, NULL);
  for (i = 0; i < num_col_info; i++)
    {
      prepare_column_info_set (net_buf, col_info_tbl[i].type, scale,
			       col_info_tbl[i].precision,
			       col_info_tbl[i].col_name,
			       NULL, 0, 0, NULL, NULL, 0);
    }

  prepare_shard_key_info_set (NULL, net_buf);
}

static void
prepare_column_info_set (T_NET_BUF * net_buf, char ut, short scale, int prec,
			 const char *col_name, const char *default_value,
			 char unique_key, char primary_key,
			 const char *attr_name, const char *class_name,
			 char is_non_null)
{
  const char *attr_name_p, *class_name_p;
  int attr_name_len, class_name_len;

  net_buf_column_info_set (net_buf, ut, scale, prec, col_name);

  attr_name_p = (attr_name != NULL) ? attr_name : "";
  attr_name_len = strlen (attr_name_p);

  class_name_p = (class_name != NULL) ? class_name : "";
  class_name_len = strlen (class_name_p);

  net_buf_cp_int (net_buf, attr_name_len + 1, NULL);
  net_buf_cp_str (net_buf, attr_name_p, attr_name_len + 1);

  net_buf_cp_int (net_buf, class_name_len + 1, NULL);
  net_buf_cp_str (net_buf, class_name_p, class_name_len + 1);

  if (is_non_null >= 1)
    {
      is_non_null = 1;
    }
  else if (is_non_null < 0)
    {
      is_non_null = 0;
    }

  net_buf_cp_byte (net_buf, is_non_null);

  if (default_value == NULL)
    {
      net_buf_cp_int (net_buf, 1, NULL);
      net_buf_cp_byte (net_buf, '\0');
    }
  else
    {
      int len = strlen (default_value) + 1;

      net_buf_cp_int (net_buf, len, NULL);
      net_buf_cp_str (net_buf, default_value, len);
    }

  net_buf_cp_byte (net_buf, unique_key);
  net_buf_cp_byte (net_buf, primary_key);
}

static const char *
get_column_default_as_string (DB_ATTRIBUTE * attr, char **alloced_buffer)
{
  DB_VALUE *def = NULL;
  int err;
  char *default_value_string = NULL;

  *alloced_buffer = NULL;

  /* Get default value string */
  def = db_attribute_default (attr);
  if (def == NULL)
    {
      return default_value_string;
    }

  switch (attr->default_value.default_expr)
    {
    case DB_DEFAULT_SYSDATE:
      return "SYS_DATE";
    case DB_DEFAULT_SYSDATETIME:
      return "SYS_DATETIME";
    case DB_DEFAULT_UNIX_TIMESTAMP:
      return "UNIX_TIMESTAMP";
    case DB_DEFAULT_USER:
      return "USER";
    case DB_DEFAULT_CURR_USER:
      return "CURRENT_USER";
    case DB_DEFAULT_NONE:
      break;
    }

  if (db_value_is_null (def))
    {
      return "NULL";
    }

  switch (db_value_type (def))
    {
    case DB_TYPE_UNKNOWN:
      break;

    case DB_TYPE_VARCHAR:
      {
	int def_size = DB_GET_STRING_SIZE (def);
	char *def_str_p = DB_GET_STRING (def);
	if (def_str_p)
	  {
	    char *tmp_ptr;
	    tmp_ptr = (char *) malloc (def_size + 3);
	    if (tmp_ptr != NULL)
	      {
		tmp_ptr[0] = '\'';
		memcpy (tmp_ptr + 1, def_str_p, def_size);
		tmp_ptr[def_size + 1] = '\'';
		tmp_ptr[def_size + 2] = '\0';
	      }
	    default_value_string = tmp_ptr;
	    *alloced_buffer = tmp_ptr;
	  }
      }
      break;

    default:
      {
	DB_VALUE tmp_val;

	DB_MAKE_NULL (&tmp_val);

	assert (!DB_IS_NULL (def));
	if (!DB_IS_NULL (def))
	  {
	    err = db_value_coerce (def, &tmp_val,
				   db_type_to_db_domain (DB_TYPE_VARCHAR));
	    if (err == NO_ERROR)
	      {
		char *tmp_ptr;
		int def_size = DB_GET_STRING_SIZE (&tmp_val);
		char *def_str_p = DB_GET_STRING (&tmp_val);

		tmp_ptr = (char *) malloc (def_size + 1);
		if (tmp_ptr != NULL)
		  {
		    memcpy (tmp_ptr, def_str_p, def_size);
		    tmp_ptr[def_size] = '\0';
		  }
		default_value_string = tmp_ptr;
		*alloced_buffer = tmp_ptr;
	      }
	  }

	db_value_clear (&tmp_val);
      }
      break;
    }

  return default_value_string;
}

static int
cas_adjust_precision (char cas_type, int precision)
{
  if (cas_type == CCI_TYPE_VARCHAR && shm_Appl->max_string_length >= 0)
    {
      if (precision < 0 || precision > shm_Appl->max_string_length)
	{
	  return shm_Appl->max_string_length;
	}
    }

  return precision;
}

static char
set_column_info (T_NET_BUF * net_buf, DB_QUERY_TYPE * col)
{
  DB_OBJECT *class_obj;
  DB_ATTRIBUTE *attr;
  char unique_key = 0;
  char primary_key = 0;
  const char *default_val_str = NULL;
  char *alloced_buffer = NULL;
  DB_DOMAIN *domain;
  DB_TYPE db_type;
  char cas_type;
  short scale;
  int precision;
  char *col_name;
  const char *real_attr_name;
  const char *class_name;
  char is_non_null;

  domain = db_query_format_domain (col);
  db_type = db_query_format_type (col);

  cas_type = db_type_to_cas_type (db_type);

  scale = (short) db_domain_scale (domain);

  precision = db_domain_precision (domain);
  precision = cas_adjust_precision (cas_type, precision);

  col_name = (char *) db_query_format_name (col);

  class_name = db_query_format_class_name (col);

  class_obj = sm_find_class (class_name);

  attr = db_get_attribute (class_obj, col_name);

  real_attr_name = "";

  is_non_null = (char) db_query_format_is_non_null (col);

  unique_key = db_attribute_is_unique (attr);
  primary_key = db_attribute_is_primary_key (attr);

  default_val_str = get_column_default_as_string (attr, &alloced_buffer);

  prepare_column_info_set (net_buf, cas_type, scale, precision, col_name,
			   default_val_str,
			   unique_key, primary_key,
			   real_attr_name, class_name, is_non_null);

  if (alloced_buffer)
    {
      free ((char *) alloced_buffer);
    }

  return cas_type;
}

static int
netval_to_dbval (void *net_type, void *net_value, DB_VALUE * out_val)
{
  char type;
  int err_code = 0;
  int data_size;
  DB_VALUE *db_val_ptr = out_val;

  assert (db_val_ptr != NULL);

  net_arg_get_char (&type, net_type);

  net_arg_get_size (&data_size, net_value);
  if (data_size <= 0)
    {
      type = CCI_TYPE_NULL;
      data_size = 0;
    }

  switch (type)
    {
    case CCI_TYPE_NULL:
      err_code = db_make_null (db_val_ptr);
      break;
    case CCI_TYPE_VARCHAR:
      {
	char *value, *invalid_pos = NULL;
	int val_size;
	int val_length;
	bool is_composed = false;
	INTL_CODESET codeset;

	net_arg_get_str (&value, &val_size, net_value);

	val_size--;

	codeset = lang_get_client_charset ();
	assert (codeset == INTL_CODESET_UTF8);

	if (codeset != INTL_CODESET_UTF8)
	  {
	    char msg[12];

	    off_t p = invalid_pos != NULL ? (invalid_pos - value) : 0;
	    snprintf (msg, sizeof (msg), "%llu", (long long unsigned int) p);
	    return ERROR_INFO_SET_WITH_MSG (ER_INVALID_CHAR,
					    DBMS_ERROR_INDICATOR, msg);
	  }

	intl_char_count ((unsigned char *) value, val_size, &val_length);
	err_code = db_make_varchar (db_val_ptr, val_length, value, val_size,
				    LANG_COLL_ANY);
	if (db_val_ptr != NULL
	    && TP_IS_CHAR_TYPE (DB_VALUE_DOMAIN_TYPE (db_val_ptr)))
	  {
	    db_string_put_cs_and_collation (db_val_ptr, LANG_COLL_ANY);
	  }
	db_val_ptr->need_clear = is_composed;
      }
      break;
    case CCI_TYPE_VARBIT:
      {
	char *value;
	int val_size;
	net_arg_get_str (&value, &val_size, net_value);
	err_code = db_make_varbit (db_val_ptr, val_size * 8, value,
				   val_size * 8);
      }
      break;
    case CCI_TYPE_NUMERIC:
      {
	char *value, *p;
	int val_size;
	int precision, scale;
	char tmp[BUFSIZ];

	net_arg_get_str (&value, &val_size, net_value);
	if (value != NULL)
	  {
	    STRNCPY (tmp, value, sizeof (tmp));
	  }
	tmp[val_size] = '\0';
	trim (tmp);
	precision = strlen (tmp);
	p = strchr (tmp, '.');
	if (p == NULL)
	  {
	    scale = 0;
	  }
	else
	  {
	    scale = strlen (p + 1);
	    precision--;
	  }
	if (tmp[0] == '-')
	  {
	    precision--;
	  }

	err_code =
	  db_value_domain_init (db_val_ptr, DB_TYPE_NUMERIC, precision,
				scale);
	if (err_code == 0)
	  {
	    err_code =
	      db_value_put_numeric (db_val_ptr, DB_TYPE_C_VARCHAR, tmp,
				    strlen (tmp));
	  }
      }
      break;
    case CCI_TYPE_BIGINT:
      {
	DB_BIGINT bi_val;

	net_arg_get_bigint (&bi_val, net_value);
	err_code = db_make_bigint (db_val_ptr, bi_val);
      }
      break;
    case CCI_TYPE_INT:
      {
	int i_val;

	net_arg_get_int (&i_val, net_value);
	err_code = db_make_int (db_val_ptr, i_val);
      }
      break;
    case CCI_TYPE_DOUBLE:
      {
	double d_val;
	net_arg_get_double (&d_val, net_value);
	err_code = db_make_double (db_val_ptr, d_val);
      }
      break;
    case CCI_TYPE_DATE:
      {
	short month, day, year;
	net_arg_get_date (&year, &month, &day, net_value);
	err_code = db_make_date (db_val_ptr, month, day, year);
      }
      break;
    case CCI_TYPE_TIME:
      {
	short hh, mm, ss;
	net_arg_get_time (&hh, &mm, &ss, net_value);
	err_code = db_make_time (db_val_ptr, hh, mm, ss);
      }
      break;
    case CCI_TYPE_DATETIME:
      {
	short yr, mon, day, hh, mm, ss, ms;
	DB_DATETIME dt;

	net_arg_get_datetime (&yr, &mon, &day, &hh, &mm, &ss, &ms, net_value);
	err_code = db_datetime_encode (&dt, mon, day, yr, hh, mm, ss, ms);
	if (err_code != NO_ERROR)
	  {
	    break;
	  }
	err_code = db_make_datetime (db_val_ptr, &dt);
      }
      break;

    default:
      return ERROR_INFO_SET (CAS_ER_UNKNOWN_U_TYPE, CAS_ERROR_INDICATOR);
    }

  if (err_code < 0)
    {
      return ERROR_INFO_SET (err_code, DBMS_ERROR_INDICATOR);
    }

  return data_size;
}

static int
cur_tuple (T_QUERY_RESULT * q_result, int max_col_size, T_NET_BUF * net_buf)
{
  int ncols;
  DB_VALUE val;
  int i;
  int error;
  int data_size = 0;
  DB_QUERY_RESULT *result = q_result->result;
  int err_code;

  ncols = db_query_column_count (result);
  for (i = 0; i < ncols; i++)
    {
      error = db_query_get_tuple_value (result, i, &val);

      if (error < 0)
	{
	  err_code = ERROR_INFO_SET (error, DBMS_ERROR_INDICATOR);
	  return err_code;
	}

      data_size += dbval_to_net_buf (&val, net_buf, max_col_size);
      db_value_clear (&val);
    }

  return data_size;
}

static int
dbval_to_net_buf (DB_VALUE * val, T_NET_BUF * net_buf, int max_col_size)
{
  int data_size = 0;

  if (db_value_is_null (val) == true)
    {
      add_res_data_null (net_buf, &data_size);
      return data_size;
    }

  switch (db_value_type (val))
    {
    case DB_TYPE_VARBIT:
      {
	DB_C_VARBIT bit;
	int length = 0;

	bit = db_get_varbit (val, &length);
	length = (length + 7) / 8;
	if (max_col_size > 0)
	  {
	    length = MIN (length, max_col_size);
	  }
	/* do not append NULL terminator */
	add_res_data_bytes (net_buf, bit, length, &data_size);
      }
      break;
    case DB_TYPE_VARCHAR:
      {
	DB_C_VARCHAR str;
	int bytes_size = 0;

	str = db_get_string (val);
	assert (str != NULL);
	if (str != NULL)
	  {
	    bytes_size = DB_GET_STRING_SIZE (val);
	    if (max_col_size > 0)
	      {
		bytes_size = MIN (bytes_size, max_col_size);
	      }

	    add_res_data_string (net_buf, str, bytes_size, &data_size);
	  }
      }
      break;
    case DB_TYPE_INTEGER:
      {
	int int_val;
	int_val = DB_GET_INTEGER (val);
	add_res_data_int (net_buf, int_val, &data_size);
      }
      break;
    case DB_TYPE_BIGINT:
      {
	DB_BIGINT bigint_val;
	bigint_val = db_get_bigint (val);
	add_res_data_bigint (net_buf, bigint_val, &data_size);
      }
      break;
    case DB_TYPE_DOUBLE:
      {
	double d_val;
	d_val = db_get_double (val);
	add_res_data_double (net_buf, d_val, &data_size);
      }
      break;
    case DB_TYPE_DATE:
      {
	DB_DATE *db_date;
	int yr, mon, day;
	db_date = db_get_date (val);
	db_date_decode (db_date, &mon, &day, &yr);
	add_res_data_date (net_buf, (short) yr, (short) mon, (short) day,
			   &data_size);
      }
      break;
    case DB_TYPE_TIME:
      {
	DB_DATE *time;
	int hour, minute, second;
	time = db_get_time (val);
	db_time_decode (time, &hour, &minute, &second);
	add_res_data_time (net_buf, (short) hour, (short) minute,
			   (short) second, &data_size);
      }
      break;
    case DB_TYPE_DATETIME:
      {
	DB_DATETIME *dt;
	int yr, mon, day, hh, mm, ss, ms;
	dt = db_get_datetime (val);
	db_datetime_decode (dt, &mon, &day, &yr, &hh, &mm, &ss, &ms);
	add_res_data_datetime (net_buf, (short) yr, (short) mon, (short) day,
			       (short) hh, (short) mm, (short) ss, (short) ms,
			       &data_size);
      }
      break;
    case DB_TYPE_NUMERIC:
      {
	DB_DOMAIN *char_domain;
	DB_VALUE v;
	char *str;
	int err;
	char buf[128];

	DB_MAKE_NULL (&v);

	assert (!DB_IS_NULL (val));
	if (!DB_IS_NULL (val))
	  {
	    char_domain = db_type_to_db_domain (DB_TYPE_VARCHAR);
	    err = db_value_coerce (val, &v, char_domain);
	    if (err < 0)
	      {
		net_buf_cp_int (net_buf, -1, NULL);
		data_size = NET_SIZE_INT;
	      }
	    else
	      {
		str = db_get_string (&v);
		if (str != NULL)
		  {
		    strncpy (buf, str, sizeof (buf) - 1);
		    buf[sizeof (buf) - 1] = '\0';
		    trim (buf);
		    add_res_data_string (net_buf, buf, strlen (buf),
					 &data_size);
		  }
		else
		  {
		    net_buf_cp_int (net_buf, -1, NULL);
		    data_size = NET_SIZE_INT;
		  }
		db_value_clear (&v);
	      }
	  }
      }
      break;

    default:
      net_buf_cp_int (net_buf, -1, NULL);	/* null */
      data_size = 4;
      break;
    }

  return data_size;
}

static int
fetch_result (T_SRV_HANDLE * srv_handle, int cursor_pos,
	      int result_set_idx, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  int err_code;
  int num_tuple_msg_offset;
  int num_tuple;
  int net_buf_size;
  char cas_cursor_status = CAS_CURSOR_STATUS_OPEN;
  DB_QUERY_RESULT *result;

  if (result_set_idx <= 0)
    {
      if (srv_handle->q_result == NULL)
	{
	  return ERROR_INFO_SET (CAS_ER_NO_MORE_DATA, CAS_ERROR_INDICATOR);
	}
    }
  else
    {
      return ERROR_INFO_SET (CAS_ER_NO_MORE_RESULT_SET, CAS_ERROR_INDICATOR);
    }

  result = srv_handle->q_result->result;
  if (result == NULL
      || ux_has_stmt_result_set (srv_handle->q_result->stmt_type) == false)
    {
      return ERROR_INFO_SET (CAS_ER_NO_MORE_DATA, CAS_ERROR_INDICATOR);
    }

  if (srv_handle->cursor_pos != cursor_pos)
    {
      if (cursor_pos == 1)
	{
	  err_code = db_query_first_tuple (result);
	}
      else
	{
	  err_code =
	    db_query_seek_tuple (result, cursor_pos - 1, DB_CURSOR_SEEK_SET);
	}

      if (err_code == DB_CURSOR_SUCCESS)
	{
	  net_buf_cp_int (net_buf, 0, &num_tuple_msg_offset);	/* tuple num */
	}
      else if (err_code == DB_CURSOR_END)
	{
	  net_buf_cp_int (net_buf, 0, NULL);

	  if (check_auto_commit_after_fetch_done (srv_handle) == true)
	    {
	      ux_cursor_close (srv_handle);
	      req_info->need_auto_commit = TRAN_AUTOCOMMIT;
	      cas_cursor_status = CAS_CURSOR_STATUS_CLOSED;
	    }


	  net_buf_cp_byte (net_buf, cas_cursor_status);

	  return 0;
	}
      else
	{
	  return ERROR_INFO_SET (err_code, DBMS_ERROR_INDICATOR);
	}
    }
  else
    {
      net_buf_cp_int (net_buf, 0, &num_tuple_msg_offset);
    }

  net_buf_size = NET_BUF_SIZE;

  num_tuple = 0;
  while (CHECK_NET_BUF_SIZE (net_buf, net_buf_size))
    {				/* currently, don't check fetch_count */
      net_buf_cp_int (net_buf, cursor_pos, NULL);

      err_code = cur_tuple (srv_handle->q_result, srv_handle->max_col_size,
			    net_buf);
      if (err_code < 0)
	{
	  return err_code;
	}

      num_tuple++;
      cursor_pos++;
      if (srv_handle->max_row > 0 && cursor_pos > srv_handle->max_row)
	{
	  if (check_auto_commit_after_fetch_done (srv_handle) == true)
	    {
	      ux_cursor_close (srv_handle);
	      req_info->need_auto_commit = TRAN_AUTOCOMMIT;
	      cas_cursor_status = CAS_CURSOR_STATUS_CLOSED;
	    }
	  break;
	}

      err_code = db_query_next_tuple (result);
      if (err_code == DB_CURSOR_SUCCESS)
	{
	}
      else if (err_code == DB_CURSOR_END)
	{
	  if (check_auto_commit_after_fetch_done (srv_handle) == true)
	    {
	      ux_cursor_close (srv_handle);
	      req_info->need_auto_commit = TRAN_AUTOCOMMIT;
	      cas_cursor_status = CAS_CURSOR_STATUS_CLOSED;
	    }
	  break;
	}
      else
	{
	  return ERROR_INFO_SET (err_code, DBMS_ERROR_INDICATOR);
	}
    }

  net_buf_cp_byte (net_buf, cas_cursor_status);

  net_buf_overwrite_int (net_buf, num_tuple_msg_offset, num_tuple);

  srv_handle->cursor_pos = cursor_pos;

  return 0;
}

static int
fetch_schema_column (T_SRV_HANDLE * srv_handle, int cursor_pos,
		     UNUSED_ARG int result_set_idx,
		     T_NET_BUF * net_buf, UNUSED_ARG T_REQ_INFO * req_info)
{
  int err_code;
  int num_tuple_msg_offset;
  int num_tuple;
  DB_QUERY_RESULT *result;
  DB_VALUE val_class, val_attr;
  DB_OBJECT *class_obj;
  DB_ATTRIBUTE *db_attr;
  const char *class_name, *attr_name, *p;
  T_ATTR_TABLE attr_info;

  if (srv_handle->q_result == NULL)
    {
      return ERROR_INFO_SET (CAS_ER_NO_MORE_DATA, CAS_ERROR_INDICATOR);
    }

  result = srv_handle->q_result->result;
  if (result == NULL || !IS_SELECT_QUERY (srv_handle->q_result->stmt_type))
    {
      return ERROR_INFO_SET (CAS_ER_NO_MORE_DATA, CAS_ERROR_INDICATOR);
    }

  if (srv_handle->cursor_pos != cursor_pos)
    {
      if (cursor_pos == 1)
	{
	  err_code = db_query_first_tuple (result);
	}
      else
	{
	  err_code = db_query_seek_tuple (result, cursor_pos - 1,
					  DB_CURSOR_SEEK_SET);
	}
      if (err_code == DB_CURSOR_SUCCESS)
	{
	  net_buf_cp_int (net_buf, 0, &num_tuple_msg_offset);	/* tuple num */
	}
      else if (err_code == DB_CURSOR_END)
	{
	  net_buf_cp_int (net_buf, 0, NULL);
	  return 0;
	}
      else
	{
	  return ERROR_INFO_SET (err_code, DBMS_ERROR_INDICATOR);
	}
    }
  else
    {
      net_buf_cp_int (net_buf, 0, &num_tuple_msg_offset);
    }

  num_tuple = 0;
  while (CHECK_NET_BUF_SIZE (net_buf, NET_BUF_SIZE))
    {
      net_buf_cp_int (net_buf, cursor_pos, NULL);

      err_code = db_query_get_tuple_value (result, 0, &val_class);
      if (err_code < 0)
	{
	  return ERROR_INFO_SET (err_code, DBMS_ERROR_INDICATOR);
	}

      class_name = db_get_string (&val_class);
      class_obj = sm_find_class (class_name);
      if (class_obj == NULL)
	{
	  return ERROR_INFO_SET (db_error_code (), DBMS_ERROR_INDICATOR);
	}

      err_code = db_query_get_tuple_value (result, 1, &val_attr);
      if (err_code < 0)
	{
	  return ERROR_INFO_SET (err_code, DBMS_ERROR_INDICATOR);
	}

      attr_name = db_get_string (&val_attr);
      db_attr = db_get_attribute (class_obj, attr_name);

      if (db_attr == NULL)
	{
	  return ERROR_INFO_SET (db_error_code (), DBMS_ERROR_INDICATOR);
	}

      memset (&attr_info, 0, sizeof (attr_info));
      class_attr_info (class_name, db_attr, &attr_info);

      /* 1. attr name */
      p = attr_info.attr_name;
      add_res_data_string (net_buf, p, strlen (p), NULL);

      /* 2. domain */
      add_res_data_int (net_buf, (int) attr_info.domain, NULL);

      /* 3. scale */
      add_res_data_int (net_buf, (int) attr_info.scale, NULL);

      /* 4. precision */
      add_res_data_int (net_buf, attr_info.precision, NULL);

      /* 5. indexed */
      add_res_data_int (net_buf, (int) attr_info.indexed, NULL);

      /* 6. non_null */
      add_res_data_int (net_buf, (int) attr_info.non_null, NULL);

      /* 7. unique */
      add_res_data_int (net_buf, (int) attr_info.unique, NULL);

      /* 8. default */
      p = attr_info.default_str;
      if (p == NULL)
	{
	  add_res_data_null (net_buf, NULL);
	}
      else
	{
	  add_res_data_string (net_buf, p, strlen (p), NULL);
	  if (attr_info.default_str_alloc_buf)
	    {
	      free (attr_info.default_str_alloc_buf);
	      attr_info.default_str_alloc_buf = NULL;
	    }
	}

      /* 9. order */
      add_res_data_int (net_buf, (int) attr_info.attr_order, NULL);

      /* 10. class name */
      p = attr_info.class_name;
      if (p == NULL)
	add_res_data_string (net_buf, "", 0, NULL);
      else
	add_res_data_string (net_buf, p, strlen (p), NULL);

      /* 11. is_key */
      add_res_data_int (net_buf, (int) attr_info.is_key, NULL);

      db_value_clear (&val_class);
      db_value_clear (&val_attr);

      assert (SCHEMA_INFO_NUM_COLUMN (CCI_SCH_COLUMN) == 11);

      num_tuple++;
      cursor_pos++;
      err_code = db_query_next_tuple (result);
      if (err_code == DB_CURSOR_SUCCESS)
	{
	}
      else if (err_code == DB_CURSOR_END)
	{
	  break;
	}
      else
	{
	  return ERROR_INFO_SET (err_code, DBMS_ERROR_INDICATOR);
	}
    }

  net_buf_cp_byte (net_buf, CAS_CURSOR_STATUS_OPEN);

  net_buf_overwrite_int (net_buf, num_tuple_msg_offset, num_tuple);

  srv_handle->cursor_pos = cursor_pos;

  return 0;
}

static void
add_res_data_null (T_NET_BUF * net_buf, int *net_size)
{
  net_buf_cp_int (net_buf, -1, NULL);

  if (net_size)
    {
      *net_size = NET_SIZE_INT;
    }
}

static void
add_res_data_bytes (T_NET_BUF * net_buf, char *str, int size, int *net_size)
{
  net_buf_cp_int (net_buf, size, NULL);
  /* do not append NULL terminator */
  net_buf_cp_str (net_buf, str, size);

  if (net_size)
    {
      *net_size = NET_SIZE_INT + size;
    }
}

static void
add_res_data_string (T_NET_BUF * net_buf, const char *str, int size,
		     int *net_size)
{
  net_buf_cp_int (net_buf, size + 1, NULL);	/* NULL terminator */
  net_buf_cp_str (net_buf, str, size);
  net_buf_cp_byte (net_buf, '\0');

  if (net_size)
    {
      *net_size = NET_SIZE_INT + size + NET_SIZE_BYTE;
    }
}

static void
add_res_data_int (T_NET_BUF * net_buf, int value, int *net_size)
{
  net_buf_cp_int (net_buf, NET_SIZE_INT, NULL);
  net_buf_cp_int (net_buf, value, NULL);

  if (net_size)
    {
      *net_size = NET_SIZE_INT + NET_SIZE_INT;
    }
}

static void
add_res_data_bigint (T_NET_BUF * net_buf, DB_BIGINT value, int *net_size)
{
  net_buf_cp_int (net_buf, NET_SIZE_BIGINT, NULL);
  net_buf_cp_bigint (net_buf, value, NULL);

  if (net_size)
    {
      *net_size = NET_SIZE_INT + NET_SIZE_BIGINT;
    }
}

static void
add_res_data_double (T_NET_BUF * net_buf, double value, int *net_size)
{
  net_buf_cp_int (net_buf, NET_SIZE_DOUBLE, NULL);
  net_buf_cp_double (net_buf, value);

  if (net_size)
    {
      *net_size = NET_SIZE_INT + NET_SIZE_DOUBLE;
    }
}

static void
add_res_data_datetime (T_NET_BUF * net_buf, short yr, short mon, short day,
		       short hh, short mm, short ss, short ms, int *net_size)
{
  net_buf_cp_int (net_buf, NET_SIZE_DATETIME, NULL);
  net_buf_cp_short (net_buf, yr);
  net_buf_cp_short (net_buf, mon);
  net_buf_cp_short (net_buf, day);
  net_buf_cp_short (net_buf, hh);
  net_buf_cp_short (net_buf, mm);
  net_buf_cp_short (net_buf, ss);
  net_buf_cp_short (net_buf, ms);

  if (net_size)
    {
      *net_size = NET_SIZE_INT + NET_SIZE_DATETIME;
    }
}

static void
add_res_data_time (T_NET_BUF * net_buf, short hh, short mm, short ss,
		   int *net_size)
{
  net_buf_cp_int (net_buf, NET_SIZE_TIME, NULL);
  net_buf_cp_short (net_buf, hh);
  net_buf_cp_short (net_buf, mm);
  net_buf_cp_short (net_buf, ss);

  if (net_size)
    {
      *net_size = NET_SIZE_INT + NET_SIZE_TIME;
    }
}

static void
add_res_data_date (T_NET_BUF * net_buf, short yr, short mon, short day,
		   int *net_size)
{
  net_buf_cp_int (net_buf, NET_SIZE_DATE, NULL);
  net_buf_cp_short (net_buf, yr);
  net_buf_cp_short (net_buf, mon);
  net_buf_cp_short (net_buf, day);

  if (net_size)
    {
      *net_size = NET_SIZE_INT + NET_SIZE_DATE;
    }
}

static int
prepare_column_list_info_set (DB_SESSION * session, T_QUERY_RESULT * q_result,
			      T_NET_BUF * net_buf)
{
  char stmt_type = q_result->stmt_type;

  if (IS_SELECT_QUERY (stmt_type))
    {
      char cas_type;
      int num_cols;
      int num_col_offset;
      DB_QUERY_TYPE *column_info = NULL, *col;

      column_info = db_get_query_type_list (session);
      if (column_info == NULL)
	{
	  return ERROR_INFO_SET (db_error_code (), DBMS_ERROR_INDICATOR);
	}

      num_cols = 0;
      net_buf_cp_int (net_buf, num_cols, &num_col_offset);
      for (col = column_info; col != NULL; col = db_query_format_next (col))
	{
	  cas_type = set_column_info (net_buf, col);
	  if (cas_type < CCI_TYPE_FIRST || cas_type > CCI_TYPE_LAST)
	    {
	      assert (false);
	      ;			/* TODO - avoid compile error */
	    }
	  num_cols++;
	}

      net_buf_overwrite_int (net_buf, num_col_offset, num_cols);
      if (column_info)
	{
	  db_query_format_free (column_info);
	}
      q_result->num_column = num_cols;
    }
  else if (ux_has_stmt_result_set (stmt_type))
    {
      net_buf_cp_int (net_buf, 1, NULL);	/* number of columns */
      prepare_column_info_set (net_buf, 0, 0, 0, "", "", 0, 0, "", "", 0);
    }
  else
    {
      net_buf_cp_int (net_buf, 0, NULL);	/* number of columns */
    }

  return 0;
}

static int
prepare_shard_key_info_set (DB_SESSION * session, T_NET_BUF * net_buf)
{
#define MAX_SHARD_KEYS	100
  char *shard_key_values[MAX_SHARD_KEYS];
  int shard_key_pos[MAX_SHARD_KEYS];
  int num_shard_values = 0;
  int num_shard_pos = 0;
  int num_shard_keys;
  bool is_shard_table_query = false;
  int i;

  num_shard_keys = db_get_shard_key_values (session,
					    &num_shard_values, &num_shard_pos,
					    shard_key_values, MAX_SHARD_KEYS,
					    shard_key_pos, MAX_SHARD_KEYS);

  if (num_shard_keys > 0)
    {
      is_shard_table_query = true;

      if (num_shard_keys != num_shard_values + num_shard_pos)
	{
	  /* too many shard keys or unexpected shard value.
	     same behavior with shard all query */
	  num_shard_pos = 0;
	  num_shard_values = 0;
	}
    }
  else
    {
      is_shard_table_query = db_is_shard_table_query (session);
    }

  /* is shard table query?. 0: false, 1: true */
  net_buf_cp_byte (net_buf, (is_shard_table_query ? 1 : 0));

  /* shard key value */
  net_buf_cp_int (net_buf, num_shard_values, NULL);
  for (i = 0; i < num_shard_values; i++)
    {
      int size = strlen (shard_key_values[i]) + 1;
      net_buf_cp_int (net_buf, size, NULL);
      net_buf_cp_str (net_buf, shard_key_values[i], size);
    }

  /* shard key position in bind parameter */
  net_buf_cp_int (net_buf, num_shard_pos, NULL);
  for (i = 0; i < num_shard_pos; i++)
    {
      net_buf_cp_int (net_buf, shard_key_pos[i], NULL);
    }

  cas_sql_log_write (query_seq_num_current_value (),
		     "shard_key_info: %s %d (%d %d)",
		     (is_shard_table_query ? "true" : "false"),
		     num_shard_keys, num_shard_values, num_shard_pos);

  return 0;
}

static int
execute_info_column_type (T_NET_BUF * net_buf, DB_QUERY_RESULT * result)
{
  DB_QUERY_TYPE *t;
  DB_DOMAIN *domain;
  int num_col_info = 0;
  char cas_type;
  short scale;
  int precision;

  for (t = db_get_query_type_ptr (result); t != NULL;
       t = db_query_format_next (t))
    {
      domain = db_query_format_domain (t);

      cas_type = db_type_to_cas_type (db_query_format_type (t));
      scale = db_domain_scale (domain);
      precision = db_domain_precision (domain);
      precision = cas_adjust_precision (cas_type, precision);

      net_buf_cp_byte (net_buf, cas_type);
      net_buf_cp_short (net_buf, scale);
      net_buf_cp_int (net_buf, precision, NULL);

      num_col_info++;
    }

  return num_col_info;
}

static int
execute_info_set (T_SRV_HANDLE * srv_handle, T_NET_BUF * net_buf)
{
  int tuple_count;
  char stmt_type;
  int num_col_info;
  int num_col_info_offset;

  stmt_type = srv_handle->q_result->stmt_type;
  tuple_count = srv_handle->q_result->tuple_count;

  net_buf_cp_byte (net_buf, stmt_type);
  net_buf_cp_int (net_buf, tuple_count, NULL);

  net_buf_cp_int (net_buf, 0, &num_col_info_offset);

  if (IS_SELECT_QUERY (stmt_type))
    {
      num_col_info = execute_info_column_type (net_buf,
					       srv_handle->q_result->result);
      if (num_col_info < 0)
	{
	  return num_col_info;
	}

      net_buf_overwrite_int (net_buf, num_col_info_offset, num_col_info);
    }

  return 0;
}

static int
schema_table (char *table_name_pattern, UNUSED_ARG char *column_name,
	      T_SRV_HANDLE * srv_handle)
{
  char sql_stmt[QUERY_BUFFER_MAX];
  int num_result;
  int num_bind;
  DB_VALUE bind_value[1];
  char all_pattern[] = "%";

  snprintf (sql_stmt, sizeof (sql_stmt),
	    "SELECT table_name, "
	    "       CAST(CASE WHEN MOD(is_system_table, 2) = 1 THEN 0 "
	    "                 WHEN table_type = 0 THEN 2 "
	    "                 ELSE 1 END AS INTEGER) "
	    "FROM db_table " "WHERE table_name LIKE ? ESCAPE '\\' ");

  if (table_name_pattern == NULL)
    {
      db_make_string (&bind_value[0], all_pattern);
    }
  else
    {
      ut_tolower (table_name_pattern);
      db_make_string (&bind_value[0], table_name_pattern);
    }
  num_bind = 1;

  assert (num_bind <= (int) DIM (bind_value));

  num_result = sch_query_execute (srv_handle, sql_stmt, num_bind, bind_value);

  return num_result;
}

static int
schema_column (char *table_name_pattern, char *column_name_pattern,
	       T_SRV_HANDLE * srv_handle)
{
  char sql_stmt[QUERY_BUFFER_MAX];
  int num_result;
  int num_bind;
  DB_VALUE bind_value[2];
  char all_pattern[] = "%";

  snprintf (sql_stmt, sizeof (sql_stmt),
	    "SELECT table_name, col_name FROM db_column "
	    "WHERE table_name LIKE ? ESCAPE '\\' "
	    "	AND col_name LIKE ? ESCAPE '\\' ");

  if (table_name_pattern == NULL)
    {
      db_make_string (&bind_value[0], all_pattern);
    }
  else
    {
      ut_tolower (table_name_pattern);
      db_make_string (&bind_value[0], table_name_pattern);
    }

  if (column_name_pattern == NULL)
    {
      db_make_string (&bind_value[1], all_pattern);
    }
  else
    {
      ut_tolower (column_name_pattern);
      db_make_string (&bind_value[1], column_name_pattern);
    }

  num_bind = 2;

  assert (num_bind <= (int) DIM (bind_value));

  num_result = sch_query_execute (srv_handle, sql_stmt, num_bind, bind_value);

  return num_result;
}

static int
schema_queryspec (char *table_name, UNUSED_ARG char *column_name,
		  T_SRV_HANDLE * srv_handle)
{
  char sql_stmt[QUERY_BUFFER_MAX];
  int num_result;
  int num_bind;
  DB_VALUE bind_value[1];
  char empty_name[] = "";

  snprintf (sql_stmt, sizeof (sql_stmt),
	    "SELECT spec FROM db_query_spec WHERE table_name = ?");

  if (table_name == NULL)
    {
      db_make_string (&bind_value[0], empty_name);
    }
  else
    {
      ut_tolower (table_name);
      db_make_string (&bind_value[0], table_name);
    }
  num_bind = 1;

  assert (num_bind <= (int) DIM (bind_value));

  num_result = sch_query_execute (srv_handle, sql_stmt, num_bind, bind_value);

  return num_result;
}

static int
get_table_num_objs (const char *table_name, DB_BIGINT * nobjs, int *npages)
{
  DB_OBJECT *class_obj;
  int approximation = 1;

  *nobjs = 0;
  *npages = 0;

  class_obj = sm_find_class (table_name);
  if (class_obj == NULL)
    {
      return -1;
    }

  if (db_get_class_num_objs_and_pages (class_obj, approximation,
				       nobjs, npages) < 0)
    {
      return -1;
    }

  return 0;
}

static int
schema_index_info (char *table_name, UNUSED_ARG char *column_name,
		   T_SRV_HANDLE * srv_handle)
{
/* shoud be the same value with jdbc DatabaseMetaData.tableIndexStatistic, tableIndexOther value */
#define CCI_INDEX_INFO_TYPE_TABLE_STATISTICS    0
#define CCI_INDEX_INFO_TYPE_INDEX_INFO          3

  char sql_stmt[QUERY_BUFFER_MAX];
  int num_result;
  DB_BIGINT table_nobjs;
  int table_npages;
  int num_bind;
  DB_VALUE bind_value[10];

  get_table_num_objs (table_name, &table_nobjs, &table_npages);

  ut_tolower (table_name);
  snprintf (sql_stmt, sizeof (sql_stmt),
	    "SELECT ?, 1, NULL, 0, NULL, NULL, cast(? as bigint), ? "
	    "UNION ALL "
	    "SELECT ?, a.is_unique, a.index_name, b.key_order+1, b.key_col_name, "
	    "       CASE WHEN b.asc_desc = 0 then 'A' ELSE 'D' END, "
	    "       cast(0 as bigint), 0 "
	    " FROM db_index a, db_index_key b              "
	    " WHERE a.table_name = b.table_name    "
	    "      AND a.index_name = b.index_name         "
	    "      AND a.table_name = ?                 ");

  db_make_int (&bind_value[0], CCI_INDEX_INFO_TYPE_TABLE_STATISTICS);
  db_make_bigint (&bind_value[1], table_nobjs);
  db_make_int (&bind_value[2], table_npages);
  db_make_int (&bind_value[3], CCI_INDEX_INFO_TYPE_INDEX_INFO);
  db_make_string (&bind_value[4], table_name);
  num_bind = 5;

  assert (num_bind <= (int) DIM (bind_value));

  num_result = sch_query_execute (srv_handle, sql_stmt, num_bind, bind_value);

  return num_result;
}

static int
schema_table_priv (char *table_name_pattern, UNUSED_ARG char *column_name,
		   T_SRV_HANDLE * srv_handle)
{
  char sql_stmt[QUERY_BUFFER_MAX];
  int num_result;
  char cur_user[SRV_CON_DBUSER_SIZE];
  int num_bind;
  DB_VALUE bind_value[3];
  char all_pattern[] = "%";

  STRNCPY (cur_user, DB_User, sizeof (cur_user));
  ut_toupper (cur_user);

  snprintf (sql_stmt, sizeof (sql_stmt),
	    "SELECT a.table_name, null, a.owner_name, b.priv, 'YES' "
	    "FROM "
	    "  ( "
	    "    SELECT table_name, owner_name "
	    "    FROM db_table "
	    "    WHERE is_system_table = 0 AND owner_name = ?:0 "
	    "	    AND table_name like ?:1 ESCAPE '\\' "
	    "  ) A, "
	    "  ( "
	    "    SELECT 'SELECT' FROM db_root UNION ALL "
	    "    SELECT 'INSERT' FROM db_root UNION ALL "
	    "    SELECT 'UPDATE' FROM db_root UNION ALL "
	    "    SELECT 'DELETE' FROM db_root UNION ALL "
	    "    SELECT 'ALTER' FROM db_root "
	    "  ) B(priv) "
	    "UNION ALL "
	    "SELECT a.table_name, a.owner_name, ?:0, b.priv, 'NO' "
	    "FROM "
	    "  ( "
	    "    SELECT table_name, owner_name "
	    "    FROM db_table "
	    "    WHERE is_system_table = 0 AND owner_name <> ?:0 "
	    "	    AND table_name like ?:1 ESCAPE '\\' "
	    "  ) A, "
	    "  ( "
	    "    SELECT table_name, 'SELECT' "
	    "    FROM db_auth "
	    "    WHERE grantee_name = ?:0 AND select_priv = 1 "
	    "	    AND table_name like ?:1 ESCAPE '\\' "
	    "    UNION ALL "
	    "    SELECT table_name, 'INSERT' "
	    "    FROM db_auth "
	    "    WHERE grantee_name = ?:0 AND insert_priv = 1 "
	    "	    AND table_name like ?:1 ESCAPE '\\' "
	    "    UNION ALL "
	    "    SELECT table_name, 'UPDATE' "
	    "    FROM db_auth "
	    "    WHERE grantee_name = ?:0 AND update_priv = 1 "
	    "	    AND table_name like ?:1 ESCAPE '\\' "
	    "    UNION ALL "
	    "    SELECT table_name, 'DELETE' "
	    "    FROM db_auth "
	    "    WHERE grantee_name = ?:0 AND delete_priv = 1 "
	    "	    AND table_name like ?:1 ESCAPE '\\' "
	    "    UNION ALL "
	    "    SELECT table_name, 'ALTER' "
	    "    FROM db_auth "
	    "    WHERE grantee_name = ?:0 AND alter_priv = 1 "
	    "	    AND table_name like ?:1 ESCAPE '\\' "
	    "  ) B(table_name, priv) "
	    "WHERE " "  a.table_name = b.table_name ");

  db_make_string (&bind_value[0], cur_user);
  if (table_name_pattern == NULL)
    {
      db_make_string (&bind_value[1], all_pattern);
    }
  else
    {
      ut_tolower (table_name_pattern);
      db_make_string (&bind_value[1], table_name_pattern);
    }

  num_bind = 2;
  assert (num_bind <= (int) DIM (bind_value));

  num_result = sch_query_execute (srv_handle, sql_stmt, num_bind, bind_value);

  return num_result;
}

static int
schema_column_priv (char *table_name, char *column_name_pattern,
		    T_SRV_HANDLE * srv_handle)
{
  char sql_stmt[QUERY_BUFFER_MAX];
  int num_result;
  char cur_user[SRV_CON_DBUSER_SIZE];
  int num_bind;
  DB_VALUE bind_value[3];
  char all_pattern[] = "%";

  STRNCPY (cur_user, DB_User, sizeof (cur_user));
  ut_toupper (cur_user);

  snprintf (sql_stmt, sizeof (sql_stmt),
	    "SELECT a.col_name, null, a.owner_name, b.priv, 'YES' "
	    "FROM "
	    "  ( "
	    "    SELECT c.col_name, t.owner_name "
	    "    FROM db_table t, db_column c"
	    "    WHERE t.is_system_table = 0 AND t.owner_name = ?:0 "
	    "          AND t.table_name = ?:1 AND c.col_name like ?:2 ESCAPE '\\' "
	    "          AND t.table_name = c.table_name "
	    "  ) A, "
	    "  ( "
	    "    SELECT 'SELECT' FROM db_root UNION ALL "
	    "    SELECT 'INSERT' FROM db_root UNION ALL "
	    "    SELECT 'UPDATE' FROM db_root UNION ALL "
	    "    SELECT 'DELETE' FROM db_root UNION ALL "
	    "    SELECT 'ALTER' FROM db_root "
	    "  ) B(priv) "
	    "UNION ALL "
	    "SELECT a.col_name, a.owner_name, ?:0, b.priv, 'NO' "
	    "FROM "
	    "  ( "
	    "    SELECT t.table_name, c.col_name, t.owner_name "
	    "    FROM db_table t, db_column c"
	    "    WHERE t.is_system_table = 0 AND t.owner_name <> ?:0 "
	    "	    AND t.table_name = ?:1 AND c.col_name like ?:2 ESCAPE '\\' "
	    "	    AND t.table_name = c.table_name "
	    "  ) A, "
	    "  ( "
	    "    SELECT table_name, 'SELECT' "
	    "    FROM db_auth "
	    "    WHERE grantee_name = ?:0 AND select_priv = 1 AND table_name = ?:1 "
	    "    UNION ALL "
	    "    SELECT table_name, 'INSERT' "
	    "    FROM db_auth "
	    "    WHERE grantee_name = ?:0 AND insert_priv = 1 AND table_name = ?:1 "
	    "    UNION ALL "
	    "    SELECT table_name, 'UPDATE' "
	    "    FROM db_auth "
	    "    WHERE grantee_name = ?:0 AND update_priv = 1 AND table_name = ?:1 "
	    "    UNION ALL "
	    "    SELECT table_name, 'DELETE' "
	    "    FROM db_auth "
	    "    WHERE grantee_name = ?:0 AND delete_priv = 1 AND table_name = ?:1 "
	    "    UNION ALL "
	    "    SELECT table_name, 'ALTER' "
	    "    FROM db_auth "
	    "    WHERE grantee_name = ?:0 AND alter_priv = 1 AND table_name = ?:1 "
	    "  ) B(table_name, priv) "
	    "WHERE " "  a.table_name = b.table_name ");

  db_make_string (&bind_value[0], cur_user);
  db_make_string (&bind_value[1], table_name);
  if (column_name_pattern == NULL)
    {
      db_make_string (&bind_value[2], all_pattern);
    }
  else
    {
      db_make_string (&bind_value[2], column_name_pattern);
    }
  num_bind = 3;
  assert (num_bind <= (int) DIM (bind_value));

  num_result = sch_query_execute (srv_handle, sql_stmt, num_bind, bind_value);

  return num_result;
}

static int
class_attr_info (const char *class_name, DB_ATTRIBUTE * attr,
		 T_ATTR_TABLE * attr_table)
{
  const char *p;
  int db_type;
  DB_DOMAIN *domain;
  int precision;
  short scale;

  p = db_attribute_name (attr);

  domain = db_attribute_domain (attr);
  db_type = TP_DOMAIN_TYPE (domain);

  attr_table->class_name = class_name;
  attr_table->attr_name = p;

  attr_table->domain = db_type_to_cas_type (db_type);
  precision = db_domain_precision (domain);
  scale = (short) db_domain_scale (domain);

  attr_table->scale = scale;
  attr_table->precision = precision;

  if (db_attribute_is_indexed (attr))
    {
      attr_table->indexed = 1;
    }
  else
    {
      attr_table->indexed = 0;
    }

  if (db_attribute_is_non_null (attr))
    {
      attr_table->non_null = 1;
    }
  else
    {
      attr_table->non_null = 0;
    }

  if (db_attribute_is_unique (attr))
    {
      attr_table->unique = 1;
    }
  else
    {
      attr_table->unique = 0;
    }

  attr_table->default_str =
    get_column_default_as_string (attr, &attr_table->default_str_alloc_buf);

  attr_table->attr_order = db_attribute_order (attr) + 1;

  if (db_attribute_is_primary_key (attr))
    {
      attr_table->is_key = 1;
    }
  else
    {
      attr_table->is_key = 0;
    }

  return 1;
}

static int
sch_query_execute (T_SRV_HANDLE * srv_handle, char *sql_stmt, int num_bind,
		   DB_VALUE * bind_values)
{
  DB_SESSION *session = NULL;
  int error, num_result, stmt_type;
  DB_QUERY_RESULT *result = NULL;
  T_QUERY_RESULT *q_result = NULL;
  int err_code;

  lang_set_parser_use_client_charset (false);

  session = db_open_buffer (sql_stmt);
  if (!session)
    {
      lang_set_parser_use_client_charset (true);
      return ERROR_INFO_SET (db_error_code (), DBMS_ERROR_INDICATOR);
    }

  error = db_compile_statement (session);
  if (error != NO_ERROR)
    {
      err_code = ERROR_INFO_SET (error, DBMS_ERROR_INDICATOR);
      db_close_session (session);
      return err_code;
    }

  if (num_bind > 0)
    {
      db_push_values (session, num_bind, bind_values);
    }

  stmt_type = db_get_statement_type (session);
  lang_set_parser_use_client_charset (false);
  num_result = db_execute_statement (session, &result);
  lang_set_parser_use_client_charset (true);

  update_query_execution_count (as_Info, stmt_type);

  if (num_result < 0)
    {
      err_code = ERROR_INFO_SET (num_result, DBMS_ERROR_INDICATOR);
      db_close_session (session);
      return err_code;
    }

  /* success; peek the values in tuples */
  (void) db_query_set_copy_tplvalue (result, 0 /* peek */ );

  q_result = (T_QUERY_RESULT *) malloc (sizeof (T_QUERY_RESULT));
  if (q_result == NULL)
    {
      db_query_end (result);
      db_close_session (session);
      return ERROR_INFO_SET (CAS_ER_NO_MORE_MEMORY, CAS_ERROR_INDICATOR);
    }
  hm_qresult_clear (q_result);
  q_result->stmt_type = stmt_type;
  q_result->tuple_count = num_result;
  q_result->result = result;

  srv_handle->max_col_size = -1;
  srv_handle->session = (void *) session;
  srv_handle->q_result = q_result;
  srv_handle->sql_stmt = NULL;

  return num_result;
}

static int
schema_primary_key (char *table_name, UNUSED_ARG char *column_name,
		    T_SRV_HANDLE * srv_handle)
{
  char sql_stmt[QUERY_BUFFER_MAX];
  int num_result = 0;
  int num_bind;
  DB_VALUE bind_value[1];

  ut_tolower (table_name);

  snprintf (sql_stmt, sizeof (sql_stmt),
	    "SELECT a.table_name, b.key_col_name, b.key_order+1, a.index_name"
	    " FROM db_index a, db_index_key b 		"
	    " WHERE a.table_name = b.table_name 	"
	    "      AND a.index_name = b.index_name 	"
	    "      AND a.is_primary_key = 1 	"
	    "      AND a.table_name = ? 		"
	    " ORDER BY b.key_col_name			");

  db_make_string (&bind_value[0], table_name);
  num_bind = 1;

  assert (num_bind <= (int) DIM (bind_value));

  num_result = sch_query_execute (srv_handle, sql_stmt, num_bind, bind_value);

  return num_result;
}

int
ux_auto_commit (T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{

  int err_code;
  int elapsed_sec = 0, elapsed_msec = 0;
  struct timeval commit_start;
  const char *msg;
  int tran_type;
  int timeout;

  gettimeofday (&commit_start, NULL);
  if (req_info->need_auto_commit == TRAN_AUTOCOMMIT)
    {
      msg = "auto_commit";
      tran_type = CCI_TRAN_COMMIT;
    }
  else
    {
      msg = "auto_rollback";
      tran_type = CCI_TRAN_ROLLBACK;
    }

  if (req_info->need_auto_commit == TRAN_AUTOCOMMIT ||
      req_info->need_auto_commit == TRAN_AUTOROLLBACK)
    {
      if (as_Info->cur_sql_log_mode == SQL_LOG_MODE_ALL)
	{
	  cas_sql_log_write (0, "%s", msg);
	}

      err_code = ux_end_tran (tran_type, true);

      if (as_Info->cur_sql_log_mode == SQL_LOG_MODE_ALL)
	{
	  cas_sql_log_write (0, "%s %d", msg, err_code);
	}
    }
  else
    {
      err_code = ERROR_INFO_SET (CAS_ER_INTERNAL, CAS_ERROR_INDICATOR);
    }

  if (err_code < 0)
    {
      NET_BUF_ERR_SET (net_buf);
      req_info->need_rollback = TRUE;
      sql_log_Notice_mode_flush = true;
    }
  else
    {
      req_info->need_rollback = FALSE;
    }

  timeout = ut_check_timeout (&tran_Start_time, NULL,
			      shm_Appl->long_transaction_time,
			      &elapsed_sec, &elapsed_msec);
  if (timeout >= 0)
    {
      as_Info->num_long_transactions %= MAX_DIAG_DATA_VALUE;
      as_Info->num_long_transactions++;
      sql_log_Notice_mode_flush = true;
    }

  if (err_code < 0 || sql_log_Notice_mode_flush == true)

    {
      if (as_Info->cur_sql_log_mode != SQL_LOG_MODE_ALL)
	{
	  cas_sql_log_write_with_ts (&commit_start, 0, "%s", msg);
	  cas_sql_log_write (0, "%s %d", msg, err_code);
	}
      cas_sql_log_end (true, elapsed_sec, elapsed_msec);
    }
  else
    {
      cas_sql_log_end ((as_Info->cur_sql_log_mode == SQL_LOG_MODE_ALL),
		       elapsed_sec, elapsed_msec);
    }

  gettimeofday (&tran_Start_time, NULL);
  gettimeofday (&query_Start_time, NULL);
  sql_log_Notice_mode_flush = false;

  return err_code;
}

const char *
schema_info_str (int schema_type)
{
  if (schema_type < CCI_SCH_FIRST || schema_type > CCI_SCH_LAST)
    {
      return "";
    }

  return (SCHEMA_INFO_INFO_STR (schema_type));
}

bool
ux_has_stmt_result_set (char stmt_type)
{
  switch (stmt_type)
    {
    case RYE_STMT_SELECT:
    case RYE_STMT_SELECT_UPDATE:
      return true;

    default:
      break;
    }

  return false;
}

static bool
check_auto_commit_after_fetch_done (T_SRV_HANDLE * srv_handle)
{
  if (srv_handle->auto_commit_mode == TRUE
      && srv_handle->scrollable_cursor == FALSE)
    {
      return true;
    }

  return false;
}

void
cas_set_db_connect_status (int status)
{
  db_set_connect_status (status);
}

int
cas_get_db_connect_status (void)
{
  return db_get_connect_status ();
}

void
cas_log_error_handler (unsigned int eid)
{
  if (cas_EHCTX == NULL)
    {
      return;
    }

  if (cas_EHCTX->from == 0)
    {
      cas_EHCTX->from = eid;
    }
  else
    {
      cas_EHCTX->to = eid;
    }
}

/*****************************
  move from cas_log.c
 *****************************/
void
cas_log_error_handler_begin (void)
{
  CAS_ERROR_LOG_HANDLE_CONTEXT *ectx;

  ectx = malloc (sizeof (*ectx));
  if (ectx == NULL)
    {
      return;
    }

  ectx->from = 0;
  ectx->to = 0;

  if (cas_EHCTX != NULL)
    {
      free (cas_EHCTX);
    }

  cas_EHCTX = ectx;
  (void) db_register_error_log_handler (cas_log_error_handler);
}

void
cas_log_error_handler_end (void)
{
  if (cas_EHCTX != NULL)
    {
      free (cas_EHCTX);
      cas_EHCTX = NULL;
      (void) db_register_error_log_handler (NULL);
    }
}

void
cas_log_error_handler_clear (void)
{
  if (cas_EHCTX == NULL)
    {
      return;
    }

  cas_EHCTX->from = 0;
  cas_EHCTX->to = 0;
}


char *
cas_log_error_handler_asprint (char *buf, size_t bufsz, bool clear)
{
  unsigned int from, to;

  if (buf == NULL || bufsz <= 0)
    {
      return NULL;
    }

  if (cas_EHCTX == NULL || cas_EHCTX->from == 0)
    {
      buf[0] = '\0';
      return buf;
    }

  from = cas_EHCTX->from;
  to = cas_EHCTX->to;

  if (clear)
    {
      cas_EHCTX->from = 0;
      cas_EHCTX->to = 0;
    }

  /* actual print */
  if (to != 0)
    {
      snprintf (buf, bufsz, ", EID = %u ~ %u", from, to);
    }
  else
    {
      snprintf (buf, bufsz, ", EID = %u", from);
    }

  return buf;
}

int
get_tuple_count (T_SRV_HANDLE * srv_handle)
{
  return srv_handle->q_result->tuple_count;
}

void
set_optimization_level (int level)
{
  saved_Optimization_level =
    prm_get_integer_value (PRM_ID_OPTIMIZATION_LEVEL);
  prm_set_integer_value (PRM_ID_OPTIMIZATION_LEVEL, level);
}

void
reset_optimization_level_as_saved (void)
{
  if (CHK_OPTIMIZATION_LEVEL_VALID (saved_Optimization_level))
    {
      prm_set_integer_value (PRM_ID_OPTIMIZATION_LEVEL,
			     saved_Optimization_level);
    }
  else
    {
      prm_set_integer_value (PRM_ID_OPTIMIZATION_LEVEL, 1);
    }
  saved_Optimization_level = -1;
}

static void
update_query_execution_count (T_APPL_SERVER_INFO * as_info_p, char stmt_type)
{
  assert (as_info_p != NULL);

  as_info_p->num_queries_processed %= MAX_DIAG_DATA_VALUE;
  as_info_p->num_queries_processed++;

  switch (stmt_type)
    {
    case RYE_STMT_SELECT:
    case RYE_STMT_SELECT_UPDATE:
      as_info_p->num_select_queries %= MAX_DIAG_DATA_VALUE;
      as_info_p->num_select_queries++;
      break;
    case RYE_STMT_INSERT:
      as_info_p->num_insert_queries %= MAX_DIAG_DATA_VALUE;
      as_info_p->num_insert_queries++;
      break;
    case RYE_STMT_UPDATE:
      as_info_p->num_update_queries %= MAX_DIAG_DATA_VALUE;
      as_info_p->num_update_queries++;
      break;
    case RYE_STMT_DELETE:
      as_info_p->num_delete_queries %= MAX_DIAG_DATA_VALUE;
      as_info_p->num_delete_queries++;
      break;
    default:
      break;
    }
}

static void
update_repl_execution_count (T_APPL_SERVER_INFO * as_info_p, int insert_count,
			     int update_count, int delete_count,
			     int error_count)
{
  assert (as_info_p != NULL);
  assert (insert_count >= 0 && update_count >= 0 && delete_count >= 0);

  as_info_p->num_queries_processed += (insert_count + update_count
				       + delete_count);
  as_info_p->num_queries_processed %= MAX_DIAG_DATA_VALUE;

  as_info_p->num_insert_queries += insert_count;
  as_info_p->num_insert_queries %= MAX_DIAG_DATA_VALUE;

  as_info_p->num_update_queries += update_count;
  as_info_p->num_update_queries %= MAX_DIAG_DATA_VALUE;

  as_info_p->num_delete_queries += delete_count;
  as_info_p->num_delete_queries %= MAX_DIAG_DATA_VALUE;

  as_info_p->num_error_queries += error_count;
  as_info_p->num_error_queries %= MAX_DIAG_DATA_VALUE;

}

static bool
need_reconnect_on_rctime (void)
{
  if (shm_Appl->cas_rctime > 0 && db_get_need_reconnect ())
    {
      if ((time (NULL) - as_Info->last_connect_time) > shm_Appl->cas_rctime)
	{
	  return true;
	}
    }

  return false;
}

/*
 * set_host_variables ()
 *
 *   return: error code or NO_ERROR
 *   db_session(in):
 *   num_bind(in):
 *   in_values(in):
 */
static int
set_host_variables (DB_SESSION * session, int num_bind, DB_VALUE * in_values)
{
  int err_code;

  err_code = db_push_values (session, num_bind, in_values);
  if (err_code != NO_ERROR)
    {
      err_code = ERROR_INFO_SET (err_code, DBMS_ERROR_INDICATOR);
    }

  return err_code;
}

static void
db_value_list_free (DB_VALUE * value_list, int num_values)
{
  int i;

  if (value_list)
    {
      for (i = 0; i < num_values; i++)
	{
	  db_value_clear (&(value_list[i]));
	}
      RYE_FREE_MEM (value_list);
    }
}

static bool
is_server_ro_tran_executable (char auto_commit, char stmt_type)
{
  if (auto_commit
      && stmt_type == RYE_STMT_SELECT && db_is_server_in_tran () == false)
    {
      return true;
    }
  else
    {
      return false;
    }
}

static bool
is_server_autocommit_executable (char auto_commit)
{
  if (auto_commit && db_is_server_in_tran () == false)
    {
      return true;
    }

  return false;
}

int
ux_update_group_id (int migrator_id, int group_id, int target, int on_off)
{
  return db_update_group_id (migrator_id, group_id, target, on_off);
}

int
ux_insert_gid_removed_info (int group_id)
{
  return shard_insert_ct_shard_gid_removed_info (group_id);
}

int
ux_delete_gid_removed_info (int group_id)
{
  return shard_delete_ct_shard_gid_removed_info_with_gid (group_id);
}

int
ux_delete_gid_skey_info (int group_id)
{
  return shard_delete_ct_shard_gid_skey_info_with_gid (group_id);
}

int
ux_block_globl_dml (int start_or_end)
{
  return db_block_globl_dml (start_or_end);
}

extern int locator_repl_flush_all (int *num_error);

/*
 * net_arg_get_idxkey ()-
 *   return: error code
 *
 *   key(out): this value must be cleared by db_idxkey_clear()
 *   arg(in):
 */
static int
net_arg_get_idxkey (DB_IDXKEY * key, void *arg)
{
  char *idxkey_buf = NULL;
  char *tmp_buf;
  int idxkey_len;
  int error = 0;

  net_arg_get_str (&tmp_buf, &idxkey_len, arg);
  assert (tmp_buf != NULL);

  idxkey_buf = (char *) malloc (idxkey_len);
  if (idxkey_buf == NULL)
    {
      error = ERROR_INFO_SET (CAS_ER_NO_MORE_MEMORY, DBMS_ERROR_INDICATOR);
      goto end;
    }
  memcpy (idxkey_buf, tmp_buf, idxkey_len);
  /* idxkey_buf is MAX_ALIGNMENT */
  or_unpack_db_idxkey (idxkey_buf, key);

  assert (!DB_IDXKEY_IS_NULL (key));

end:
  if (idxkey_buf != NULL)
    {
      free_and_init (idxkey_buf);
    }

  return error;
}

/*
 *
 */
static int
net_arg_get_recdes (RECDES ** recdes, void *arg)
{
  RECDES *des = NULL;
  char *rec_data;
  int rec_len;
  int error = 0;

  net_arg_get_str (&rec_data, &rec_len, arg);
  if (rec_len == 0)
    {
      *recdes = NULL;
      goto end;
    }

  des = (RECDES *) malloc (sizeof (RECDES));
  if (des == NULL)
    {
      error = ERROR_INFO_SET (CAS_ER_NO_MORE_MEMORY, CAS_ERROR_INDICATOR);
      goto end;
    }
  des->data = (char *) malloc (rec_len);
  if (des->data == NULL)
    {
      error = ERROR_INFO_SET (CAS_ER_NO_MORE_MEMORY, CAS_ERROR_INDICATOR);
      goto end;
    }

  memcpy (des->data, rec_data, rec_len);
  des->area_size = rec_len;
  des->length = rec_len;
  des->type = REC_HOME;

  *recdes = des;
  des = NULL;

end:
  if (des != NULL)
    {
      if (des->data != NULL)
	{
	  free_and_init (des->data);
	}
      free_and_init (des);
    }

  return error;
}

/*
 * net_arg_get_repl_item () -
 */
static int
net_arg_get_repl_item (CIRP_REPL_ITEM * item, int arg_idx, void **obj_argv)
{
  int len;
  int tmp_int;
  RP_DATA_ITEM *data;
  RP_CATALOG_ITEM *catalog;
  RP_DDL_ITEM *ddl;

  net_arg_get_int (&tmp_int, obj_argv[arg_idx++]);
  item->item_type = tmp_int;

  switch (item->item_type)
    {
    case RP_ITEM_TYPE_DATA:
      data = &item->info.data;

      net_arg_get_str (&data->class_name, &len, obj_argv[arg_idx++]);
      net_arg_get_idxkey (&data->key, obj_argv[arg_idx++]);
      net_arg_get_int (&tmp_int, obj_argv[arg_idx++]);
      data->rcv_index = tmp_int;
      net_arg_get_recdes (&data->recdes, obj_argv[arg_idx++]);
      assert ((data->recdes == NULL && data->rcv_index == RVREPL_DATA_DELETE)
	      || (data->recdes != NULL
		  && data->rcv_index != RVREPL_DATA_DELETE));

      LSA_SET_NULL (&data->lsa);
      LSA_SET_NULL (&data->target_lsa);
      break;
    case RP_ITEM_TYPE_CATALOG:
      catalog = &item->info.catalog;

      net_arg_get_str (&catalog->class_name, &len, obj_argv[arg_idx++]);
      net_arg_get_idxkey (&catalog->key, obj_argv[arg_idx++]);
      net_arg_get_int (&tmp_int, obj_argv[arg_idx++]);
      catalog->copyarea_op = tmp_int;
      net_arg_get_recdes (&catalog->recdes, obj_argv[arg_idx++]);
      assert (catalog->recdes != NULL);
      assert (catalog->copyarea_op == LC_FLUSH_HA_CATALOG_WRITER_UPDATE
	      || catalog->copyarea_op == LC_FLUSH_HA_CATALOG_ANALYZER_UPDATE
	      || catalog->copyarea_op == LC_FLUSH_HA_CATALOG_APPLIER_UPDATE);

      LSA_SET_NULL (&catalog->lsa);
      break;
    case RP_ITEM_TYPE_DDL:
      ddl = &item->info.ddl;

      net_arg_get_int (&tmp_int, obj_argv[arg_idx++]);
      ddl->stmt_type = tmp_int;
      net_arg_get_str (&ddl->db_user, &len, obj_argv[arg_idx++]);
      net_arg_get_str (&ddl->query, &len, obj_argv[arg_idx++]);

      ddl->ddl_type = 0;
      LSA_SET_NULL (&ddl->lsa);
      break;
    }

  return arg_idx;
}

static int
ux_send_repl_data_tran (int num_items, void **obj_argv)
{
  CIRP_REPL_ITEM item;
  int i, arg_idx;
  int error = 0;
  int op = LC_FETCH;
  int insert_count, update_count, delete_count, error_count;

  insert_count = update_count = delete_count = error_count = 0;

  arg_idx = 0;
  for (i = 0; i < num_items && error == 0; i++)
    {
      arg_idx = net_arg_get_repl_item (&item, arg_idx, obj_argv);
      if (arg_idx < 0)
	{
	  error = ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
	  break;
	}

      switch (item.item_type)
	{
	case RP_ITEM_TYPE_DDL:
	  assert (false);	/* is impossible */
	  error = ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
	  break;
	case RP_ITEM_TYPE_DATA:
	  switch (item.info.data.rcv_index)
	    {
	    case RVREPL_DATA_UPDATE:
	      op = LC_FLUSH_UPDATE;
	      update_count++;
	      break;
	    case RVREPL_DATA_INSERT:
	      op = LC_FLUSH_INSERT;
	      insert_count++;
	      break;
	    case RVREPL_DATA_DELETE:
	      op = LC_FLUSH_DELETE;
	      delete_count++;
	      break;
	    default:
	      assert (false);
	      error = ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
	      break;
	    }

	  error = ws_add_to_repl_obj_list (item.info.data.class_name,
					   &item.info.data.key,
					   item.info.data.recdes, op);
	  if (error != NO_ERROR)
	    {
	      error = ERROR_INFO_SET (error, DBMS_ERROR_INDICATOR);
	    }
	  db_idxkey_clear (&item.info.data.key);

	  break;
	case RP_ITEM_TYPE_CATALOG:
	  error = ws_add_to_repl_obj_list (item.info.catalog.class_name,
					   &item.info.catalog.key,
					   item.info.catalog.recdes,
					   item.info.catalog.copyarea_op);
	  if (error != NO_ERROR)
	    {
	      error = ERROR_INFO_SET (error, DBMS_ERROR_INDICATOR);
	    }

	  db_idxkey_clear (&item.info.catalog.key);

	  break;
	}
    }

  if (error != NO_ERROR)
    {
      insert_count = update_count = delete_count = 0;
      error_count = 1;
    }
  else
    {
      /* server side auto commit */
      error = locator_repl_flush_all (&error_count);
      if (error != NO_ERROR)
	{
	  if (error == ER_LC_PARTIALLY_FAILED_TO_FLUSH)
	    {
	      error = NO_ERROR;
	    }
	  else
	    {
	      insert_count = update_count = delete_count = 0;
	      error_count = 1;

	      error = ERROR_INFO_SET (error, DBMS_ERROR_INDICATOR);
	    }
	}
    }

  update_repl_execution_count (as_Info, insert_count, update_count,
			       delete_count, error_count);

  ws_clear_all_repl_objs ();

  return error;
}

static int
ux_send_repl_ddl_tran (int num_item, void **obj_argv)
{
  int arg_idx;
  int error = NO_ERROR;
  CIRP_REPL_ITEM item;
  char db_name[MAX_HA_DBINFO_LENGTH];
  char *save_db_name = NULL;
  DB_QUERY_RESULT *result;
  DB_QUERY_ERROR query_error;
  int num_error = 0;

  if (num_item != 2)
    {
      assert (false);		/* is impossible */
      error = ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      goto end;
    }

  arg_idx = 0;

  /* ddl info */
  arg_idx = net_arg_get_repl_item (&item, arg_idx, obj_argv);
  if (arg_idx < 0)
    {
      assert (false);		/* is impossible */
      error = ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      goto end;
    }
  if (item.item_type != RP_ITEM_TYPE_DDL)
    {
      assert (false);		/* is impossible */
      error = ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      goto end;
    }

  if (STMT_TYPE_IS_DDL (item.info.ddl.stmt_type) == false
      || db_get_client_type () != BOOT_CLIENT_REPL_BROKER)
    {
      assert (false);		/* is impossible */
      error = ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      goto end;
    }

  if ((item.info.ddl.stmt_type == RYE_STMT_CREATE_CLASS
       || item.info.ddl.stmt_type == RYE_STMT_CREATE_SERIAL
       || item.info.ddl.stmt_type == RYE_STMT_ALTER_CLASS)
      && (item.info.ddl.db_user != NULL && item.info.ddl.db_user[0] != '\0'))
    {
      strncpy (db_name, DB_Name, sizeof (db_name));
      error = ux_database_connect (db_name, item.info.ddl.db_user,
				   shm_Br_master->broker_key, NULL);
      if (error < 0)
	{
	  ux_database_shutdown ();

	  goto end;
	}
      save_db_name = db_name;
    }

  error = db_execute_with_values (item.info.ddl.query, &result,
				  &query_error, 0, NULL);
  if (error < 0 && er_la_ignore_on_error (error) == false)
    {
      error = ERROR_INFO_SET (error, DBMS_ERROR_INDICATOR);
      goto end;
    }
  error = db_query_end (result);

  /* catalog info */
  arg_idx = net_arg_get_repl_item (&item, arg_idx, obj_argv);
  if (arg_idx < 0)
    {
      assert (false);		/* is impossible */
      error = ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      goto end;
    }
  if (item.item_type != RP_ITEM_TYPE_CATALOG
      || item.info.catalog.copyarea_op != LC_FLUSH_HA_CATALOG_APPLIER_UPDATE)
    {
      assert (false);		/* is impossible */
      error = ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      goto end;
    }

  error = ws_add_to_repl_obj_list (item.info.catalog.class_name,
				   &item.info.catalog.key,
				   item.info.catalog.recdes,
				   item.info.catalog.copyarea_op);
  if (error != NO_ERROR)
    {
      error = ERROR_INFO_SET (error, DBMS_ERROR_INDICATOR);
      goto end;
    }

  db_idxkey_clear (&item.info.catalog.key);

  /* server side auto commit */
  error = locator_repl_flush_all (&num_error);
  if (error != NO_ERROR)
    {
      error = ERROR_INFO_SET (error, DBMS_ERROR_INDICATOR);
      goto end;
    }

end:
  ws_clear_all_repl_objs ();

  if (error < 0)
    {
      num_error = 1;
    }

  update_repl_execution_count (as_Info, 0, 0, 0, num_error);

  if (save_db_name != NULL)
    {
      ux_database_connect (db_name, "dba", shm_Br_master->broker_key, NULL);
    }

  return error;
}

int
ux_send_repl_data (int tran_type, int num_items, void **obj_argv)
{
  int error = NO_ERROR;

  if (db_get_client_type () != BOOT_CLIENT_REPL_BROKER)
    {
      assert (false);		/* is impossible */
      error = ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      return error;
    }

  if (tran_type == RP_TRAN_TYPE_DDL)
    {
      error = ux_send_repl_ddl_tran (num_items, obj_argv);
    }
  else
    {
      error = ux_send_repl_data_tran (num_items, obj_argv);
    }

  return error;
}
