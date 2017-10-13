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
 * rbl_gc_runner.c -
 */

#ident "$Id$"

#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/time.h>

#include "error_manager.h"
#include "cas_cci_internal.h"
#include "rbl_error_log.h"
#include "rbl_gc.h"

typedef struct rbl_gc_context RBL_GC_CONTEXT;
struct rbl_gc_context
{
  CCI_NODE_INFO *node;
  CCI_CONN conn;
  const char *pw;
  int max_runtime;
  pthread_t gc_thrd;
};

static struct option options[] = {
  {"host", required_argument, NULL, 'h'},
  {"port", required_argument, NULL, 'p'},
  {"dbname", required_argument, NULL, 'd'},
  {"dba-pw", required_argument, NULL, 'w'},
  {"max-runtime", required_argument, NULL, 't'},
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
  fprintf (stdout, "usage: rye_gc [OPTIONS]\n");
  fprintf (stdout, "  options: \n");
  fprintf (stdout, "\t --host [host name]\n");
  fprintf (stdout, "\t --port [port]\n");
  fprintf (stdout, "\t --dbname [db name]\n");
  fprintf (stdout, "\t --dba-pw [dba password]\n");
  fprintf (stdout, "\t --max-runtime [max runtime (sec)]\n");

  exit (-1);
}

static int
rbl_gc_connect (const char *host, int port, const char *dbname,
		const char *pw, CCI_CONN * conn)
{
  char url[256];

  sprintf (url, "cci:rye://%s:%d/%s/rw?", host, port, dbname);

  if (cci_connect (conn, url, "dba", pw ? pw : "") < 0)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		 conn->err_buf.err_code, conn->err_buf.err_msg);
      return ER_FAILED;
    }

  cci_set_autocommit (conn, CCI_AUTOCOMMIT_FALSE);

  return NO_ERROR;
}

static THREAD_RET_T THREAD_CALLING_CONVENTION
rbl_gc_thread (void *arg)
{
  RBL_GC_CONTEXT *ctx;
  CCI_NODE_INFO *node;
  int error = NO_ERROR;
  ER_MSG_INFO *er_msg;

  ctx = (RBL_GC_CONTEXT *) arg;
  node = ctx->node;

  er_msg = malloc (sizeof (ER_MSG_INFO));
  error = er_set_msg_info (er_msg);
  if (error != NO_ERROR)
    {
      assert (false);
      return (THREAD_RET_T) NULL;
    }

  error = rbl_gc_connect (node->hostname, node->port, node->dbname,
			  ctx->pw, &ctx->conn);
  if (error != NO_ERROR)
    {
      RBL_NOTICE (ARG_FILE_LINE, "DB connection fail: "
		  "host = %s, port = %d, dbname = %s, error = %d\n",
		  node->hostname, node->port, node->dbname, error);
      return (THREAD_RET_T) NULL;
    }

  error = rbl_gc_run (&ctx->conn, ctx->max_runtime);
  if (error == NO_ERROR)
    {
      RBL_NOTICE (ARG_FILE_LINE, "Garbage collect success: "
		  "host = %s, port = %d, dbname = %s\n",
		  node->hostname, node->port, node->dbname);
    }
  else
    {
      RBL_NOTICE (ARG_FILE_LINE, "Garbage collect fail: "
		  "host = %s, port = %d, dbname = %s, error = %d\n",
		  node->hostname, node->port, node->dbname, error);
    }

  cci_disconnect (&ctx->conn);

  return (THREAD_RET_T) NULL;
}

int
main (int argc, char *argv[])
{
  int error, opt, opt_index = 0;
  char *host = NULL, *dbname = NULL, *pw = NULL;
  int port = -1, max_runtime = 0;
  CCI_CONN conn;
  CCI_SHARD_NODE_INFO *nodes = NULL;
  CCI_SHARD_GROUPID_INFO *groups = NULL;
  RBL_GC_CONTEXT *ctx = NULL;
  int i, num_thrd = 0;
  bool is_notified = false;

  while (true)
    {
      opt = getopt_long (argc, argv, "", options, &opt_index);
      if (opt == -1)
	{
	  break;
	}

      switch (opt)
	{
	case 'h':
	  host = optarg;
	  break;
	case 'p':
	  port = atoi (optarg);
	  break;
	case 'd':
	  dbname = optarg;
	  break;
	case 't':
	  max_runtime = atoi (optarg);
	  break;
	case 'w':
	  pw = optarg;
	  break;
	default:
	  print_usage_and_exit ();
	  break;
	}
    }

  if (host == NULL || dbname == NULL || port < 0 || max_runtime < 0)
    {
      print_usage_and_exit ();
      return ER_FAILED;
    }

  rbl_error_log_init ("rye_gc", dbname, 0);

  RBL_NOTICE (ARG_FILE_LINE,
	      "Garbage collect start: host = %s, port = %d, dbname = %s, "
	      "max runtime = %d\n", host, port, dbname, max_runtime);

  error = rbl_gc_connect (host, port, dbname, pw, &conn);
  if (error != NO_ERROR)
    {
      RBL_ERROR_MSG (ARG_FILE_LINE, "DB connection fail = %d\n", error);
      rbl_error_log_final (false);
      return ER_FAILED;
    }

  error = cci_shard_get_info (&conn, &nodes, &groups);
  if (error != NO_ERROR)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		 conn.err_buf.err_code, conn.err_buf.err_msg);
      goto end;
    }

  error = cci_shard_gc_start (&conn);
  if (error != NO_ERROR)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		 conn.err_buf.err_code, conn.err_buf.err_msg);
      goto end;
    }
  is_notified = true;

  ctx =
    (RBL_GC_CONTEXT *) malloc (sizeof (RBL_GC_CONTEXT) * nodes->node_count);
  if (ctx == NULL)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_OUT_OF_MEMORY,
		 sizeof (RBL_GC_CONTEXT) * nodes->node_count);
      error = RBL_OUT_OF_MEMORY;
      goto end;
    }

#if 0
  for (i = 0; i < nodes->node_count; i++)
    {
      RBL_DEBUG (ARG_FILE_LINE, "Node info:\n%d %s %s %d\n",
		 nodes->node_info[i].nodeid,
		 nodes->node_info[i].dbname,
		 nodes->node_info[i].hostname, nodes->node_info[i].port);
    }
#endif

  for (i = 0; i < nodes->node_count; i++, num_thrd++)
    {
      ctx[i].node = &(nodes->node_info[i]);
      ctx[i].pw = pw;
      ctx[i].max_runtime = max_runtime;

      error = pthread_create (&(ctx[i].gc_thrd), NULL,
			      rbl_gc_thread, &(ctx[i]));
      if (error != NO_ERROR)
	{
	  RBL_ERROR_MSG (ARG_FILE_LINE, "pthread_create() error.\n");
	  break;
	}
    }

  for (i = 0; i < num_thrd; i++)
    {
      pthread_join (ctx[i].gc_thrd, NULL);
    }

end:

  if (is_notified == true)
    {
      error = cci_shard_gc_end (&conn);
      if (error != NO_ERROR)
	{
	  RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		     conn.err_buf.err_code, conn.err_buf.err_msg);
	  goto end;
	}
    }

  if (nodes != NULL)
    {
      cci_shard_node_info_free (nodes);
    }
  if (groups != NULL)
    {
      cci_shard_group_info_free (groups);
    }
  if (ctx != NULL)
    {
      free (ctx);
    }

  cci_disconnect (&conn);
  rbl_error_log_final (true);

  return error;
}
