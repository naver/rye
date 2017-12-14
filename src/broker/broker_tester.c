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
 * broker_tester.c -
 */

#ident "$Id$"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <assert.h>

#include <sys/time.h>

#include "broker_config.h"
#include "broker_util.h"
#include "broker_config.h"
#include "broker_shm.h"
#include "cas_protocol.h"
#include "cas_common.h"
#include "broker_admin_pub.h"

#include "cas_cci_internal.h"

#include "ini_parser.h"
#include "rye_api.h"

#define TESTER_ERR_MSG_SIZE		1024
#define TIME_BUF_SIZE			50

#define DEFAULT_EMPTY_STRING		"\0"
#define DEFAULT_RYE_USER_NAME		"dba"
#define DEFAULT_SQL_COMMAND		"SELECT 1"

#define PRINT_INDENT1		"    "
#define PRINT_INDENT2		"        "
#define PRINT_INDENT3		"            "
#define PRINT_TITLE_LEN		60

#define SPRINT_CCI_ERR(BUF, BUFSIZE, CCI_ERROR)				\
	do {								\
	  T_CCI_ERROR *_tmp_cci_err = (CCI_ERROR);			\
	  snprintf (BUF, BUFSIZE, "ERROR:%d,%s",			\
		    _tmp_cci_err->err_code, _tmp_cci_err->err_msg);	\
	} while (0)

#define PRINT_RESULT(...)                            \
        do {                                         \
          if (out_File_fp != NULL)                   \
          {                                          \
            fprintf (out_File_fp ,__VA_ARGS__);      \
          }                                          \
          fprintf (stdout, __VA_ARGS__);             \
        } while (0)

#define PRINT_TITLE(n, ...)                          \
        do {                                         \
          if (out_File_fp != NULL)                   \
          {                                          \
            fprintf (out_File_fp ,__VA_ARGS__);      \
          }                                          \
          n += fprintf (stdout, __VA_ARGS__);        \
        } while (0)

#define HAS_RESULT_SET(CMD_TYPE)			\
	  ((CMD_TYPE) == CCI_STMT_SELECT ||		\
	   (CMD_TYPE) == CCI_STMT_SELECT_UPDATE)

typedef struct
{
  int num_shard_mgmt;
  struct
  {
    char global_dbname[SRV_CON_DBNAME_SIZE];
    int port;
  } shard_mgmt_info[1];
} TESTER_SHARD_MGMT;

typedef struct
{
  int local_mgmt_port;
  int num_broker;
  char broker_name[1][BROKER_NAME_LEN];
} TESTER_LOCAL_MGMT;

typedef struct
{
  bool verbose_mode;
  bool test_shard_mgmt;
  const char *db_name;
  const char *db_user;
  const char *db_passwd;
  const char *command;
  const char *output_file_name;
  const char **test_broker_arg;
  TESTER_SHARD_MGMT *tester_shard;
  TESTER_LOCAL_MGMT *tester_local;
} TESTER_INFO;

static int init_tester_info (TESTER_INFO * br_tester_info);

static int execute_test_with_query (CCI_CONN * conn, const char *query,
				    bool verbose_mode);

static int get_option (TESTER_INFO * br_tester_info, int argc, char **argv);
static int print_result_set (CCI_STMT * stmt);
static void print_line (const char *ch, int num);

static void free_br_tester_info (TESTER_INFO * br_tester_info);
static bool is_number_type (T_CCI_TYPE type);

static int test_shard (const TESTER_INFO * br_tester_info);
static int test_broker (const char *host, int port, const char *dbname,
			const char *db_user, const char *db_passwd,
			const char *broker_name, const char *query,
			bool test_server_shard_nodeid, bool verbose_mode);
static int
test_local_mgmt (int num_broker, const char *host, int local_mgmt_port,
		 char broker_name[][BROKER_NAME_LEN],
		 const char *db_name, const char *db_user,
		 const char *db_passwd, const char *sql_command,
		 bool test_server_shard_nodeid, bool verbose_mode);

static FILE *out_File_fp;

int
broker_tester (int argc, char *argv[])
{
  int ret = 0;
  TESTER_INFO br_tester_info;

  if (get_option (&br_tester_info, argc, argv) < 0)
    {
      return -1;
    }

  if (init_tester_info (&br_tester_info) < 0)
    {
      return -1;
    }

  if (br_tester_info.output_file_name != NULL)
    {
      out_File_fp = fopen (br_tester_info.output_file_name, "w");
      if (out_File_fp == NULL)
	{
	  fprintf (stderr, "cannot open output file %s\n",
		   br_tester_info.output_file_name);
	  free_br_tester_info (&br_tester_info);
	  return -1;
	}
    }

  if (br_tester_info.tester_shard != NULL)
    {
      ret = test_shard (&br_tester_info);
    }
  else
    {
      ret = test_local_mgmt (br_tester_info.tester_local->num_broker,
			     "localhost",
			     br_tester_info.tester_local->local_mgmt_port,
			     br_tester_info.tester_local->broker_name,
			     br_tester_info.db_name,
			     br_tester_info.db_user,
			     br_tester_info.db_passwd,
			     br_tester_info.command,
			     false, br_tester_info.verbose_mode);
    }

  free_br_tester_info (&br_tester_info);

  if (out_File_fp != NULL)
    {
      fclose (out_File_fp);
    }

  return ret;
}

static int
get_option (TESTER_INFO * br_tester_info, int argc, char **argv)
{
  int opt;

  memset (br_tester_info, 0, sizeof (TESTER_INFO));

  br_tester_info->db_user = DEFAULT_RYE_USER_NAME;
  br_tester_info->db_passwd = DEFAULT_EMPTY_STRING;
  br_tester_info->command = DEFAULT_SQL_COMMAND;
  br_tester_info->verbose_mode = false;
  br_tester_info->test_shard_mgmt = true;

  while ((opt = getopt (argc, argv, "SLd:u:p:c:o:v")) != -1)
    {
      switch (opt)
	{
	case 'S':
	  br_tester_info->test_shard_mgmt = true;
	  break;
	case 'L':
	  br_tester_info->test_shard_mgmt = false;
	  break;
	case 'd':
	  br_tester_info->db_name = optarg;
	  break;
	case 'u':
	  br_tester_info->db_user = optarg;
	  break;
	case 'p':
	  br_tester_info->db_passwd = optarg;
	  break;
	case 'c':
	  br_tester_info->command = optarg;
	  break;
	case 'o':
	  br_tester_info->output_file_name = optarg;
	  break;
	case 'v':
	  br_tester_info->verbose_mode = true;
	  break;
	default:
	  goto usage;
	}
    }

  if (br_tester_info->test_shard_mgmt == false &&
      br_tester_info->db_name == NULL)
    {
      goto usage;
    }

  if (optind < argc)
    {
      int i, arg_idx = 0;
      br_tester_info->test_broker_arg =
	malloc (sizeof (char *) * (argc - optind + 1));

      for (i = optind; i < argc; i++)
	{
	  br_tester_info->test_broker_arg[arg_idx++] = argv[i];
	}
      br_tester_info->test_broker_arg[arg_idx] = NULL;
    }

  return 0;

usage:
  printf
    ("broker_tester [-S] [-L] [-d <database_name>] [-u <user_name>] [-p <user_password>] [-c <SQL_command>] [-o <output_file>] [-v] [<broker_name>...]\n");
  printf ("\t-S shard mgmt test mode (default)\n");
  printf ("\t-L local mgmt test mode \n");
  printf ("\t-d database-name \n");
  printf ("\t-u user name (default: \"%s\")\n", DEFAULT_RYE_USER_NAME);
  printf ("\t-p password string, (default: \"\") \n");
  printf ("\t-c SQL-command (default:\"%s\")\n", DEFAULT_SQL_COMMAND);
  printf ("\t-o ouput-file-name\n");
  printf ("\t-v verbose mode\n");

  return -1;
}

static TESTER_SHARD_MGMT *
init_tester_info_shard (T_SHM_BROKER * shm_br)
{
  int count = 0;
  TESTER_SHARD_MGMT *tester_shard;
  int malloc_size;
  int i;

  malloc_size = sizeof (TESTER_SHARD_MGMT) * shm_br->num_broker;

  tester_shard = malloc (malloc_size);
  if (tester_shard == NULL)
    {
      fprintf (stderr, "malloc error\n");
      return NULL;
    }

  memset (tester_shard, 0, malloc_size);

  for (i = 0; i < shm_br->num_broker; i++)
    {
      if (shm_br->br_info[i].broker_type == SHARD_MGMT)
	{
	  strncpy (tester_shard->shard_mgmt_info[count].global_dbname,
		   shm_br->br_info[i].shard_global_dbname,
		   SRV_CON_DBNAME_SIZE - 1);

	  tester_shard->shard_mgmt_info[count].port = shm_br->br_info[i].port;

	  count++;
	}
    }

  if (count == 0)
    {
      RYE_FREE_MEM (tester_shard);
      fprintf (stderr, "Cannot find [%s]\n", BR_SHARD_MGMT_NAME);
      return NULL;
    }

  tester_shard->num_shard_mgmt = count;

  return tester_shard;
}

static bool
is_test_broker_arg (const char *broker_name, const char **test_broker_arg)
{
  if (test_broker_arg == NULL)
    {
      return true;
    }

  for (; *test_broker_arg; test_broker_arg++)
    {
      if (strcasecmp (broker_name, *test_broker_arg) == 0)
	{
	  return true;
	}
    }

  return false;
}

static TESTER_LOCAL_MGMT *
init_tester_info_local (T_SHM_BROKER * shm_br, const char **test_broker_arg)
{
  int count = 0;
  TESTER_LOCAL_MGMT *tester_local;
  int malloc_size;
  int i;

  malloc_size = sizeof (TESTER_LOCAL_MGMT) +
    (BROKER_NAME_LEN * shm_br->num_broker);

  tester_local = malloc (malloc_size);
  if (tester_local == NULL)
    {
      fprintf (stderr, "malloc error\n");
      return NULL;
    }

  memset (tester_local, 0, malloc_size);

  for (i = 0; i < shm_br->num_broker; i++)
    {
      if (shm_br->br_info[i].broker_type == NORMAL_BROKER)
	{
	  if (is_test_broker_arg (shm_br->br_info[i].name, test_broker_arg))
	    {
	      strncpy (tester_local->broker_name[count++],
		       shm_br->br_info[i].name, BROKER_NAME_LEN - 1);
	    }
	}
      if (shm_br->br_info[i].broker_type == LOCAL_MGMT)
	{
	  tester_local->local_mgmt_port = shm_br->br_info[i].port;
	}
    }

  assert (tester_local->local_mgmt_port > 0);

  if (count == 0)
    {
      assert (test_broker_arg != NULL);

      fprintf (stderr, "Cannot find Broker ");
      for (; test_broker_arg && *test_broker_arg; test_broker_arg++)
	{
	  fprintf (stderr, "[%s] ", *test_broker_arg);
	}
      fprintf (stderr, "\n");

      RYE_FREE_MEM (tester_local);
      return NULL;
    }

  tester_local->num_broker = count;

  return tester_local;
}

static int
init_tester_info (TESTER_INFO * br_tester_info)
{
  int shm_key_br_gl = 0;
  T_SHM_BROKER *shm_br = NULL;
  int ret = 0;

  if (broker_config_read (NULL, NULL, &shm_key_br_gl, NULL, 0) < 0)
    {
      fprintf (stderr, "cannot read rye-auto.conf \n");
      return -1;
    }

  shm_br = rye_shm_attach (shm_key_br_gl, RYE_SHM_TYPE_BROKER_GLOBAL, true);
  if (shm_br == NULL)
    {
      fprintf (stderr, "master shared memory open error[0x%x]\n",
	       shm_key_br_gl);
      return -1;
    }

  br_tester_info->tester_local =
    init_tester_info_local (shm_br, br_tester_info->test_broker_arg);
  if (br_tester_info->tester_local == NULL)
    {
      ret = -1;
    }
  else
    {
      if (br_tester_info->test_shard_mgmt)
	{
	  br_tester_info->tester_shard = init_tester_info_shard (shm_br);

	  if (br_tester_info->tester_shard == NULL)
	    {
	      ret = -1;
	    }
	}
    }

  rye_shm_detach (shm_br);

  if (ret < 0)
    {
      RYE_FREE_MEM (br_tester_info->tester_local);
      RYE_FREE_MEM (br_tester_info->tester_shard);
    }

  return ret;
}

static int
execute_test_with_query (CCI_CONN * conn, const char *query,
			 bool verbose_mode)
{
  int ret;
  char query_with_hint[LINE_MAX];
  CCI_STMT stmt, *stmt_p = NULL;
  struct timeval start_time;
  char tester_err_msg[TESTER_ERR_MSG_SIZE] = "";

  snprintf (query_with_hint, sizeof (query_with_hint),
	    "%s /* broker_tester */", query);

  gettimeofday (&start_time, NULL);

  ret = cci_set_autocommit (conn, CCI_AUTOCOMMIT_FALSE);
  if (ret < 0)
    {
      SPRINT_CCI_ERR (tester_err_msg, TESTER_ERR_MSG_SIZE, &conn->err_buf);
      goto end_execute;
    }

  ret = cci_prepare (conn, &stmt, query_with_hint, 0);
  if (ret < 0)
    {
      SPRINT_CCI_ERR (tester_err_msg, TESTER_ERR_MSG_SIZE, &conn->err_buf);
      goto end_execute;
    }
  stmt_p = &stmt;

  ret = cci_execute (stmt_p, 0, 0);
  if (ret < 0)
    {
      SPRINT_CCI_ERR (tester_err_msg, TESTER_ERR_MSG_SIZE, &stmt_p->err_buf);
      goto end_execute;
    }

end_execute:

  PRINT_RESULT ("%sQUERY   [%s]    QUERY=\"%s\"\n",
		PRINT_INDENT2, (ret < 0 ? "FAIL" : "OK"), query);

  if (ret < 0)
    {
      PRINT_RESULT ("%s%s\n", PRINT_INDENT3, tester_err_msg);
    }
  else
    {
      PRINT_RESULT ("%sROW COUNT=%d, EXECUTION TIME=%.6f sec\n",
		    PRINT_INDENT3, ret,
		    timeval_diff_in_msec (NULL, &start_time) / 1000.0);

      if (verbose_mode)
	{
	  if (print_result_set (stmt_p) < 0)
	    {
	      ret = -1;
	    }
	}
    }

  if (stmt_p != NULL)
    {
      cci_close_req_handle (stmt_p);
    }

  cci_end_tran (conn, CCI_TRAN_ROLLBACK);

  return ret;
}

static void
print_line (const char *ch, int num)
{
  int i;

  if (num <= 0)
    {
      return;
    }

  for (i = 0; i < num; i++)
    {
      PRINT_RESULT (ch);
    }
  PRINT_RESULT ("\n");
}

static int
print_result_set (CCI_STMT * stmt)
{
  int i;
  int ret = 0;
  char tester_err_msg[TESTER_ERR_MSG_SIZE] = "";
  T_CCI_COL_INFO *col_info = NULL;
  T_CCI_SQLX_CMD cmd_type;
  int col_count;
  int row_count;

  col_info = cci_get_result_info (stmt, &cmd_type, &col_count);
  if (!HAS_RESULT_SET (cmd_type))
    {
      return 0;
    }

  PRINT_RESULT ("%s", PRINT_INDENT3);
  PRINT_RESULT ("<Result of SELECT Command>\n");

  if (col_info == NULL)
    {
      SPRINT_CCI_ERR (tester_err_msg, TESTER_ERR_MSG_SIZE, &stmt->err_buf);

      PRINT_RESULT ("%s", PRINT_INDENT3);
      PRINT_RESULT ("%s\n", tester_err_msg);
      return -1;
    }

  row_count = 0;
  while (1)
    {
      ret = cci_fetch_next (stmt);
      if (ret == CCI_ER_NO_MORE_DATA)
	{
	  ret = 0;
	  break;
	}

      if (ret < 0)
	{
	  SPRINT_CCI_ERR (tester_err_msg, TESTER_ERR_MSG_SIZE,
			  &stmt->err_buf);
	  PRINT_RESULT ("%s", PRINT_INDENT3);
	  PRINT_RESULT ("%s", tester_err_msg);
	  goto end;
	}

      row_count++;
      PRINT_RESULT ("%s", PRINT_INDENT3);
      PRINT_RESULT ("<%03d> ", row_count);

      for (i = 1; i < col_count + 1; i++)
	{
	  int ind;
	  const char *data;

	  if (i != 1)
	    {
	      PRINT_RESULT ("%s", PRINT_INDENT3);
	      PRINT_RESULT ("%6s", "");
	    }

	  data = cci_get_string (stmt, i, &ind);
	  if (stmt->err_buf.err_code < 0)
	    {
	      SPRINT_CCI_ERR (tester_err_msg, TESTER_ERR_MSG_SIZE,
			      &stmt->err_buf);
	      PRINT_RESULT ("%s", tester_err_msg);
	      goto end;
	    }

	  PRINT_RESULT ("%s: ", CCI_GET_RESULT_INFO_NAME (col_info, i));

	  if (data == NULL)
	    {
	      PRINT_RESULT ("NULL");
	    }
	  else if (is_number_type (CCI_GET_RESULT_INFO_TYPE (col_info, i)))
	    {
	      PRINT_RESULT ("%s", data);
	    }
	  else
	    {
	      PRINT_RESULT ("'%s'", data);
	    }
	  PRINT_RESULT ("\n");
	}
    }

end:

  return ret;
}

static bool
is_number_type (T_CCI_TYPE type)
{
  switch (type)
    {
    case CCI_TYPE_INT:
    case CCI_TYPE_DOUBLE:
    case CCI_TYPE_BIGINT:
      return true;
    default:
      return false;
    }

  return false;
}

static void
free_br_tester_info (TESTER_INFO * br_tester_info)
{
  RYE_FREE_MEM (br_tester_info->test_broker_arg);
  RYE_FREE_MEM (br_tester_info->tester_local);
  RYE_FREE_MEM (br_tester_info->tester_shard);
}

static int
make_connection (CCI_CONN * conn, const char *host, int port,
		 const char *dbname, const char *db_user,
		 const char *db_passwd, const char *broker_name,
		 bool is_shard_mgmt, bool test_server_shard_nodeid)
{
  int ret;
  char conn_url[LINE_MAX];
  char tester_err_msg[TESTER_ERR_MSG_SIZE] = "";
  char tester_server_nodeid[128] = "";
  const char *url_property;

  if (is_shard_mgmt)
    {
      url_property = "connectionType=global";
    }
  else
    {
      url_property = "connectionType=local";
    }

  if (is_shard_mgmt)
    {
      print_line ("=", PRINT_TITLE_LEN);
      PRINT_RESULT ("[SHARD MGMT] ");
    }
  else
    {
      print_line ("-", PRINT_TITLE_LEN);
      PRINT_RESULT ("%s@", PRINT_INDENT1);
    }

  PRINT_RESULT ("%s:%d/%s:%s/%s\n", host, port, dbname, db_user, broker_name);

  if (is_shard_mgmt && dbname[0] == '\0')
    {
      PRINT_RESULT ("ERROR: SHARD MGMT not initialized \n");
      return -1;
    }

  snprintf (conn_url, sizeof (conn_url), "cci:rye://%s:%d/%s:dba/%s?%s",
	    host, port, dbname, broker_name, url_property);
  ret = cci_connect (conn, conn_url, db_user, db_passwd);

  if (ret < 0)
    {
      SPRINT_CCI_ERR (tester_err_msg, TESTER_ERR_MSG_SIZE, &conn->err_buf);
    }
  else if (test_server_shard_nodeid)
    {
      int server_shard_nodeid;

      server_shard_nodeid = cci_server_shard_nodeid (conn);
      if (server_shard_nodeid <= 0)
	{
	  sprintf (tester_err_msg, "ERROR: server nodeid");
	}
      sprintf (tester_server_nodeid, "NODEID=%d", server_shard_nodeid);
    }

  PRINT_RESULT ("%sCONNECT [%s]\t %s %s\n",
		(is_shard_mgmt ? PRINT_INDENT1 : PRINT_INDENT2),
		(ret < 0 ? "FAIL" : "OK"),
		tester_server_nodeid, tester_err_msg);

  return ret;
}

static CCI_SHARD_NODE_INFO *
test_shard_info (CCI_CONN * conn, bool verbose_mode)
{
  char tester_err_msg[TESTER_ERR_MSG_SIZE] = "";
  CCI_SHARD_NODE_INFO *node_info = NULL;
  CCI_SHARD_GROUPID_INFO *groupid_info = NULL;
  int ret = 0;

  print_line ("-", PRINT_TITLE_LEN);
  PRINT_RESULT ("%s", PRINT_INDENT1);
  PRINT_RESULT ("SHARD INFO\n");

  if (cci_shard_get_info (conn, &node_info, &groupid_info) < 0)
    {
      SPRINT_CCI_ERR (tester_err_msg, TESTER_ERR_MSG_SIZE, &conn->err_buf);
      PRINT_RESULT ("%s", PRINT_INDENT2);
      PRINT_RESULT ("%s\n:", tester_err_msg);
      return NULL;
    }

  PRINT_RESULT ("%s", PRINT_INDENT2);
  PRINT_RESULT ("NODE INFO: version=%ld, count=%d\n",
		node_info->node_version, node_info->node_count);
  if (node_info->node_version <= 0 || node_info->node_count <= 0)
    {
      PRINT_RESULT ("%s", PRINT_INDENT2);
      PRINT_RESULT ("ERROR: node info\n");
      ret = -1;
      goto test_shard_info_end;
    }

  if (verbose_mode)
    {
      int i;
      for (i = 0; i < node_info->node_count; i++)
	{
	  PRINT_RESULT ("%s", PRINT_INDENT3);
	  PRINT_RESULT ("id=%d %s %s %d\n",
			node_info->node_info[i].nodeid,
			node_info->node_info[i].dbname,
			node_info->node_info[i].hostname,
			node_info->node_info[i].port);
	}
    }

  PRINT_RESULT ("%s", PRINT_INDENT2);
  PRINT_RESULT ("GROUPID INFO: version=%ld, count=%d\n",
		groupid_info->groupid_version, groupid_info->groupid_count);
  if (groupid_info->groupid_version <= 0 || groupid_info->groupid_count <= 0)
    {
      PRINT_RESULT ("%s", PRINT_INDENT2);
      PRINT_RESULT ("ERROR: groupid info\n");
      ret = -1;
      goto test_shard_info_end;
    }
  if (verbose_mode)
    {
      int i;
      for (i = 1; i <= groupid_info->groupid_count; i++)
	{
	  PRINT_RESULT ("%s", PRINT_INDENT3);
	  PRINT_RESULT ("groupid=%-6d nodeid=%d\n", i,
			groupid_info->nodeid_table[i]);
	}
    }

test_shard_info_end:
  if (ret < 0)
    {
      cci_shard_node_info_free (node_info);
      node_info = NULL;
    }

  cci_shard_group_info_free (groupid_info);

  return node_info;
}

static int
test_shard (const TESTER_INFO * br_tester_info)
{
  CCI_CONN conn;
  int ret = 0;
  int i, j;
  TESTER_SHARD_MGMT *tester_shard = br_tester_info->tester_shard;
  CCI_SHARD_NODE_INFO *node_info = NULL;
  const char *db_name = br_tester_info->db_name;
  bool exist_success = false;

  for (i = 0; i < br_tester_info->tester_shard->num_shard_mgmt; i++)
    {
      node_info = NULL;

      if (br_tester_info->db_name == NULL)
	{
	  db_name = tester_shard->shard_mgmt_info[i].global_dbname;
	}
      else
	{
	  if (strcmp (br_tester_info->db_name,
		      tester_shard->shard_mgmt_info[i].global_dbname) != 0)
	    {
	      continue;
	    }
	}

      ret = make_connection (&conn, "localhost",
			     tester_shard->shard_mgmt_info[i].port, db_name,
			     br_tester_info->db_user,
			     br_tester_info->db_passwd, "rw", true, false);
      if (ret < 0)
	{
	  goto test_end;
	}

      node_info = test_shard_info (&conn, br_tester_info->verbose_mode);
      if (node_info == NULL)
	{
	  goto test_end;
	}

      for (j = 0; j < node_info->node_count; j++)
	{
	  ret = test_local_mgmt (br_tester_info->tester_local->num_broker,
				 node_info->node_info[j].hostname,
				 node_info->node_info[j].port,
				 br_tester_info->tester_local->broker_name,
				 node_info->node_info[j].dbname,
				 br_tester_info->db_user,
				 br_tester_info->db_passwd,
				 br_tester_info->command,
				 true, br_tester_info->verbose_mode);
	  if (ret < 0)
	    {
	      goto test_end;
	    }
	}

    test_end:
      cci_shard_node_info_free (node_info);
      cci_disconnect (&conn);

      if (ret < 0)
	{
	  break;
	}

      exist_success = true;
    }

  if (exist_success == false)
    {
      ret = -1;
    }

  return ret;
}

static int
test_broker (const char *host, int port, const char *dbname,
	     const char *db_user, const char *db_passwd,
	     const char *broker_name, const char *query,
	     bool test_server_shard_nodeid, bool verbose_mode)
{
  int ret = 0;
  CCI_CONN conn;

  if (make_connection (&conn, host, port, dbname, db_user, db_passwd,
		       broker_name, false, test_server_shard_nodeid) < 0)
    {
      return -1;
    }

  ret = execute_test_with_query (&conn, query, verbose_mode);

  cci_disconnect (&conn);

  return ret;
}

static int
test_local_mgmt (int num_broker, const char *host,
		 int local_mgmt_port,
		 char broker_name[][BROKER_NAME_LEN],
		 const char *db_name, const char *db_user,
		 const char *db_passwd, const char *sql_command,
		 bool test_server_shard_nodeid, bool verbose_mode)
{
  int i;

  for (i = 0; i < num_broker; i++)
    {
      if (test_broker (host,
		       local_mgmt_port,
		       db_name,
		       db_user,
		       db_passwd,
		       broker_name[i], sql_command,
		       test_server_shard_nodeid, verbose_mode) < 0)
	{
	  return -1;
	}
    }

  return 0;
}
