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
 * rbl_move_group.c -
 */

#ident "$Id$"

#include <stdio.h>
#include <getopt.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/time.h>

#include "db.h"
#include "dbi.h"
#include "porting.h"
#include "cas_cci_internal.h"
#include "locator_cl.h"
#include "perf_monitor.h"
#include "rbl_move_group.h"
#include "rbl_sync_log.h"
#include "rbl_sync_query.h"
#include "rbl_copy_schema.h"
#include "rbl_conf.h"
#include "rbl_error_log.h"
#include "rbl_gc.h"

#define EXIT_AT_PRINT_USAGE        -1
#define EXIT_AT_CONNECTION         -2
#define EXIT_AT_COPY_SCHEMA        -3
#define EXIT_AT_REPL_DELAY         -4
#define EXIT_AT_START_NOTI         -5
#define EXIT_AT_CLEAR_DESTDB       -6
#define EXIT_AT_INIT_DATA_COPY     -7
#define EXIT_AT_INIT_LOG_SYNC      -8
#define EXIT_AT_PTHREAD_CREATE     -9
#define EXIT_AT_LOG_SYNC_ERROR     -10
#define EXIT_AT_DATA_COPY_ERROR    -11
#define EXIT_AT_BLOCK_GLOBAL_DML   -12
#define EXIT_AT_UPDATE_GID         -13
#define EXIT_AT_END_NOTI           -14
#define EXIT_AT_INSERT_GID_INFO    -15
#define EXIT_AT_DELETE_GID_INFO    -16


static struct option options[] = {
  {"mgmt-host", required_argument, NULL, 10},
  {"mgmt-port", required_argument, NULL, 11},
  {"mgmt-dbname", required_argument, NULL, 12},
  {"src-node-id", required_argument, NULL, 20},
  {"dst-node-id", required_argument, NULL, 30},
  {"dst-host", required_argument, NULL, 31},
  {"dst-port", required_argument, NULL, 32},
  {"dst-dbname", required_argument, NULL, 33},
  {"group-id", required_argument, NULL, 40},
  {"copy-schema", no_argument, NULL, 50},
  {"run-slave", no_argument, NULL, 60},
  {0, 0, 0, 0}
};

/*
 * print_usage_and_exit() -
 *
 *   return:
 */
static void
print_usage_and_exit (void)
{
  fprintf (stdout, "usage: %s [OPTIONS]\n", PROG_NAME);
  fprintf (stdout, "  options: \n");
  fprintf (stdout, "\t --mgmt-host [mgmt host name]\n");
  fprintf (stdout, "\t --mgmt-port [mgmt port]\n");
  fprintf (stdout, "\t --mgmt-dbname [mgmt db name]\n");
  fprintf (stdout, "\t --src-node-id [source node id]\n");
  fprintf (stdout, "\t --dst-node-id [target node id]\n");
  fprintf (stdout, "\t --dst-host [target node host]\n");
  fprintf (stdout, "\t --dst-port [target node port]\n");
  fprintf (stdout, "\t --dst-dbname [target node dbname]\n");
  fprintf (stdout, "\t --group-id [group id]\n");
  fprintf (stdout, "\t --copy-schema\n");
  fprintf (stdout, "\t --run-slave\n\n");

  exit (-1);
}

static THREAD_RET_T THREAD_CALLING_CONVENTION
rbl_data_copy_thread (void *arg)
{
  RBL_COPY_CONTEXT *ctx;
  int error;
  ER_MSG_INFO *er_msg;

  ctx = (RBL_COPY_CONTEXT *) arg;


  er_msg = malloc (sizeof (ER_MSG_INFO));
  error = er_set_msg_info (er_msg);
  if (error != NO_ERROR)
    {
      assert (false);
      return (THREAD_RET_T) NULL;
    }

  error = rbl_copy_group_data (ctx);
  if (error != NO_ERROR)
    {
      ctx->last_error = error;
      ctx->sync_ctx->shutdown = true;

      RBL_NOTICE (ARG_FILE_LINE, "Data copy fail: error = %d", error);
    }

  return (THREAD_RET_T) NULL;
}

int
main (int argc, char *argv[])
{
  int error, opt, opt_index = 0;
  char *mgmt_host, *dest_host, *mgmt_dbname, *dest_dbname;
  int mgmt_port, dest_port, src_node_id, dest_node_id, group_id;
  bool copy_schema = false, run_slave = false;
  CCI_CONN *mgmt_conn, *src_conn, *dest_conn;
  RBL_COPY_CONTEXT copy_ctx;
  RBL_SYNC_CONTEXT sync_ctx;
  pthread_t data_copy_th;
  int exit_code;
  struct timeval start_time, end_time, elapsed_time;

  gettimeofday (&start_time, NULL);
  mgmt_host = mgmt_dbname = dest_host = dest_dbname = NULL;
  mgmt_port = dest_port = src_node_id = dest_node_id = group_id = -1;
  memset (&copy_ctx, 0, sizeof (RBL_COPY_CONTEXT));;
  memset (&sync_ctx, 0, sizeof (RBL_SYNC_CONTEXT));;
  exit_code = NO_ERROR;

  while (true)
    {
      opt = getopt_long (argc, argv, "", options, &opt_index);
      if (opt == -1)
        {
          break;
        }

      switch (opt)
        {
        case 10:
          mgmt_host = optarg;
          break;
        case 11:
          mgmt_port = atoi (optarg);
          break;
        case 12:
          mgmt_dbname = optarg;
          break;
        case 20:
          src_node_id = atoi (optarg);
          break;
        case 30:
          dest_node_id = atoi (optarg);
          break;
        case 31:
          dest_host = optarg;
          break;
        case 32:
          dest_port = atoi (optarg);
          break;
        case 33:
          dest_dbname = optarg;
          break;
        case 40:
          group_id = atoi (optarg);
          break;
        case 50:
          copy_schema = true;
          break;
        case 60:
          run_slave = true;
          break;
        default:
          print_usage_and_exit ();
          break;
        }
    }

  if (mgmt_host == NULL || mgmt_dbname == NULL || mgmt_port < 0
      || src_node_id < 0
      || (copy_schema == true
          && (dest_host == NULL || dest_dbname == NULL || dest_port < 0))
      || (copy_schema == false && (dest_node_id < 0 || group_id < 0)))
    {
      print_usage_and_exit ();
      return EXIT_AT_PRINT_USAGE;
    }

  rbl_error_log_init ("rye_migrator", mgmt_dbname, group_id);

  RBL_NOTICE (ARG_FILE_LINE,
              "Group migration start: host = %s, port = %d, dbname = %s, "
              "source node = %d, dest node = %d, group id = %d, "
              "copy schema = %d, run slave = %d\n",
              mgmt_host, mgmt_port, mgmt_dbname, src_node_id, dest_node_id, group_id, copy_schema, run_slave);

  /* make cci connections */
  error = rbl_conf_init (mgmt_host, mgmt_port, mgmt_dbname,
                         src_node_id, dest_node_id, group_id,
                         dest_host, dest_port, dest_dbname, run_slave ? RBL_SLAVE : RBL_MASTER, copy_schema);
  if (error != NO_ERROR)
    {
      RBL_ERROR_MSG (ARG_FILE_LINE, "Node connection fail = %d\n", error);
      rbl_error_log_final (false);
      return EXIT_AT_CONNECTION;
    }

  mgmt_conn = rbl_conf_get_mgmt_conn ();
  src_conn = rbl_conf_get_srcdb_conn ();
  dest_conn = rbl_conf_get_destdb_conn (RBL_COPY);

  if (copy_schema == true)
    {
      error = rbl_copy_schema ();
      if (error != NO_ERROR)
        {
          RBL_ERROR_MSG (ARG_FILE_LINE, "Copy schema error = %d\n", error);
          exit_code = EXIT_AT_COPY_SCHEMA;
        }

      rbl_conf_final ();
      rbl_error_log_final (true);

      return exit_code;
    }

  if (run_slave == true && rbl_conf_check_repl_delay (src_conn) != NO_ERROR)
    {
      exit_code = EXIT_AT_REPL_DELAY;
      goto error_exit;
    }

  if (group_id > GLOBAL_GROUPID)
    {
      /* noti migration start to mgmt */
      error = cci_shard_migration_start (mgmt_conn, group_id, dest_node_id);
      if (error != NO_ERROR)
        {
          RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR, mgmt_conn->err_buf.err_code, mgmt_conn->err_buf.err_msg);
          exit_code = EXIT_AT_START_NOTI;
          goto error_exit;
        }
    }

  /* garbage collection for dest db */
  error = rbl_clear_destdb (dest_conn, group_id);
  if (error != NO_ERROR)
    {
      RBL_ERROR_MSG (ARG_FILE_LINE, "Clear garbage fail: error = %d", error);
      exit_code = EXIT_AT_CLEAR_DESTDB;
      goto error_exit;
    }

  /* data copy & sync log */
  if (rbl_init_copy_context (&copy_ctx, group_id, run_slave) != NO_ERROR)
    {
      exit_code = EXIT_AT_INIT_DATA_COPY;
      goto error_exit;
    }

  if (rbl_sync_log_init (&sync_ctx, group_id) != NO_ERROR)
    {
      exit_code = EXIT_AT_INIT_LOG_SYNC;
      goto error_exit;
    }

  gettimeofday (&copy_ctx.start_time, NULL);
  copy_ctx.sync_ctx = &sync_ctx;
  error = pthread_create (&data_copy_th, NULL, rbl_data_copy_thread, &copy_ctx);
  if (error != NO_ERROR)
    {
      RBL_ERROR_MSG (ARG_FILE_LINE, "pthread_create() error\n");
      exit_code = EXIT_AT_PTHREAD_CREATE;
      goto error_exit;
    }

  error = rbl_sync_log (&sync_ctx);
  if (error != NO_ERROR)
    {
      RBL_ERROR_MSG (ARG_FILE_LINE, "Log sync fail: error = %d", error);
      copy_ctx.interrupt = true;
      pthread_join (data_copy_th, NULL);
      exit_code = EXIT_AT_LOG_SYNC_ERROR;
      goto error_exit;
    }
  else if (copy_ctx.last_error != NO_ERROR)
    {
      pthread_join (data_copy_th, NULL);
      error = copy_ctx.last_error;
      exit_code = EXIT_AT_DATA_COPY_ERROR;
      goto error_exit;
    }
  pthread_join (data_copy_th, NULL);

  /* sync group id status between mgmt, source and dest db */
  if (group_id == GLOBAL_GROUPID)
    {
      error = cci_block_global_dml (dest_conn, false);
      if (error < 0)
        {
          RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR, dest_conn->err_buf.err_code, dest_conn->err_buf.err_msg);
          goto error_exit;
          exit_code = EXIT_AT_BLOCK_GLOBAL_DML;
        }
    }
  else
    {
      assert (group_id > GLOBAL_GROUPID);

      error = rbl_conf_update_dest_groupid (&copy_ctx);
      if (error != NO_ERROR)
        {
          exit_code = EXIT_AT_UPDATE_GID;
          goto error_exit;
        }

      error = cci_shard_migration_end (mgmt_conn, group_id, dest_node_id, copy_ctx.num_skeys);
      if (error != NO_ERROR)
        {
          RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR, mgmt_conn->err_buf.err_code, mgmt_conn->err_buf.err_msg);
          exit_code = EXIT_AT_END_NOTI;
          goto error_exit;
        }

      error = rbl_conf_insert_gid_removed_info_srcdb (group_id, run_slave);
      if (error != NO_ERROR)
        {
          exit_code = EXIT_AT_INSERT_GID_INFO;
          goto error_exit;
        }

      error = cci_delete_gid_removed_info (dest_conn, group_id);
      if (error != NO_ERROR)
        {
          RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR, dest_conn->err_buf.err_code, dest_conn->err_buf.err_msg);
          exit_code = EXIT_AT_DELETE_GID_INFO;
          goto error_exit;
        }
    }

  cci_end_tran (mgmt_conn, CCI_TRAN_COMMIT);
  cci_end_tran (src_conn, CCI_TRAN_COMMIT);
  cci_end_tran (dest_conn, CCI_TRAN_COMMIT);

  rbl_sync_log_final (&sync_ctx);
  rbl_conf_final ();

  gettimeofday (&end_time, NULL);
  DIFF_TIMEVAL (start_time, end_time, elapsed_time);
  RBL_NOTICE (ARG_FILE_LINE, "Group migration success: group id: %d, "
              "num_shard_keys: %d, num_rows: %d, elapsed: %ld(ms)\n",
              group_id, copy_ctx.num_skeys, copy_ctx.num_copied_rows,
              (elapsed_time.tv_sec * 1000) + (elapsed_time.tv_usec / 1000));
  fprintf (stdout, "Group migration success: group id: %d, "
           "num_shard_keys: %d, num_rows: %d, elapsed: %ld\n",
           group_id, copy_ctx.num_skeys, copy_ctx.num_copied_rows,
           (elapsed_time.tv_sec * 1000) + (elapsed_time.tv_usec / 1000));
  fflush (stdout);

  rbl_error_log_final (true);
  return exit_code;

error_exit:

  cci_end_tran (mgmt_conn, CCI_TRAN_ROLLBACK);
  cci_end_tran (src_conn, CCI_TRAN_ROLLBACK);
  cci_end_tran (dest_conn, CCI_TRAN_ROLLBACK);

  if (copy_ctx.was_gid_updated == true)
    {
      (void) rbl_conf_update_src_groupid (&copy_ctx, true, true);
    }

  rbl_sync_log_final (&sync_ctx);
  rbl_conf_final ();

  RBL_NOTICE (ARG_FILE_LINE, "Group migration fail: group id = %d, error = %d\n", group_id, error);
  rbl_error_log_final (false);

  return exit_code;
}
