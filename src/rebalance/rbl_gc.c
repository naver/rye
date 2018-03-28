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
 * rbl_gc.c -
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
#include "error_manager.h"
#include "transform.h"
#include "perf_monitor.h"
#include "cas_cci_internal.h"
#include "rbl_error_log.h"
#include "rbl_table_info.h"
#include "rbl_gc.h"

static int *rbl_get_all_removed_group_id (CCI_CONN * conn, int *num_ids);
static int rbl_gc_shard_table (CCI_CONN * conn, int gid);
static int rbl_gc_global_table (CCI_CONN * conn);
static CCI_STMT *rbl_make_table_gc_stmts (CCI_CONN * conn, TABLE_INFO * tables, int num_table);
static void rbl_free_table_gc_stmts (CCI_STMT * stmts, int n);
static int rbl_delete_table_rows (CCI_CONN * dest_conn, CCI_STMT * del_stmt, int gid, char *skey);

int
rbl_gc_run (CCI_CONN * conn, int max_runtime)
{
  int *gids, num_gids = 0;
  int i, error = NO_ERROR;
  struct timeval start_time, end_time, elapsed_time;

  gids = rbl_get_all_removed_group_id (conn, &num_gids);
  if (gids == NULL)
    {
      return ER_FAILED;
    }

  gettimeofday (&start_time, NULL);

  for (i = 0; i < num_gids; i++)
    {
      error = rbl_gc_shard_table (conn, gids[i]);
      if (error != NO_ERROR)
        {
          break;
        }

      error = cci_delete_gid_removed_info (conn, gids[i]);
      if (error != NO_ERROR)
        {
          RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR, conn->err_buf.err_code, conn->err_buf.err_msg);
          cci_end_tran (conn, CCI_TRAN_ROLLBACK);
          break;
        }

      error = cci_delete_gid_skey_info (conn, gids[i]);
      if (error != NO_ERROR)
        {
          RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR, conn->err_buf.err_code, conn->err_buf.err_msg);
          cci_end_tran (conn, CCI_TRAN_ROLLBACK);
          break;
        }

      cci_end_tran (conn, CCI_TRAN_COMMIT);

      if (max_runtime > 0)
        {
          gettimeofday (&end_time, NULL);
          DIFF_TIMEVAL (start_time, end_time, elapsed_time);
          if (elapsed_time.tv_sec >= max_runtime)
            {
              break;
            }
        }
    }

  return error;
}

static int *
rbl_get_all_removed_group_id (CCI_CONN * conn, int *num_ids)
{
  char sql[SQL_BUF_SIZE];
  CCI_STMT stmt;
  int ind, i, n, gid;
  int *gids = NULL;

  sprintf (sql, "SELECT gid FROM %s", CT_SHARD_GID_REMOVED_INFO_NAME);

  if (cci_prepare (conn, &stmt, sql, CCI_PREPARE_FROM_MIGRATOR) < 0)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR, conn->err_buf.err_code, conn->err_buf.err_msg);
      return NULL;
    }

  n = cci_execute (&stmt, 0, 0);
  if (n < 0)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR, stmt.err_buf.err_code, stmt.err_buf.err_msg);
      goto handle_error;
    }

  gids = (int *) malloc (sizeof (int) * n);
  if (gids == NULL)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_OUT_OF_MEMORY, sizeof (int) * n);
      goto handle_error;
    }

  for (i = 0; i < n; i++)
    {
      if (cci_fetch_next (&stmt) < 0)
        {
          RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR, stmt.err_buf.err_code, stmt.err_buf.err_msg);
          goto handle_error;
        }

      gid = cci_get_int (&stmt, 1, &ind);
      if (stmt.err_buf.err_code != CCI_ER_NO_ERROR)
        {
          RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR, stmt.err_buf.err_code, stmt.err_buf.err_msg);
          goto handle_error;
        }

      gids[i] = gid;
    }

  RBL_ASSERT (cci_fetch_next (&stmt) == CCI_ER_NO_MORE_DATA);

  cci_close_req_handle (&stmt);
  cci_end_tran (conn, CCI_TRAN_COMMIT);
  *num_ids = n;

  return gids;

handle_error:

  cci_close_req_handle (&stmt);
  cci_end_tran (conn, CCI_TRAN_ROLLBACK);

  if (gids != NULL)
    {
      free (gids);
    }

  return NULL;
}

static int
rbl_gc_shard_table (CCI_CONN * conn, int gid)
{
  TABLE_INFO *tables;
  CCI_STMT *del_stmts;
  char **skeys;
  int num_skey, num_tbl, i, j, n;

  skeys = rbl_get_shard_keys (conn, gid, &num_skey);
  if (skeys == NULL)
    {
      return ER_FAILED;
    }

  tables = rbl_get_all_shard_tables (conn, &num_tbl);
  if (tables == NULL)
    {
      rbl_free_shard_keys (skeys, num_skey);
      return ER_FAILED;
    }

  del_stmts = rbl_make_table_gc_stmts (conn, tables, num_tbl);
  if (del_stmts == NULL)
    {
      rbl_free_shard_keys (skeys, num_skey);
      rbl_free_table_info (tables, num_tbl);
      return ER_FAILED;
    }

  for (i = 0; i < num_skey; i++)
    {
      for (j = 0; j < num_tbl; j++)
        {
          n = rbl_delete_table_rows (conn, &del_stmts[j], gid, skeys[i]);
          if (n < 0)
            {
              rbl_free_shard_keys (skeys, num_skey);
              rbl_free_table_info (tables, num_tbl);
              rbl_free_table_gc_stmts (del_stmts, num_tbl);
              return ER_FAILED;
            }

          RBL_DEBUG (ARG_FILE_LINE, "Table = %s, Deleted rows = %d", tables[j].table_name, n);
        }
    }

  rbl_free_shard_keys (skeys, num_skey);
  rbl_free_table_info (tables, num_tbl);
  rbl_free_table_gc_stmts (del_stmts, num_tbl);

  return NO_ERROR;
}

static int
rbl_gc_global_table (CCI_CONN * conn)
{
  TABLE_INFO *tables;
  CCI_STMT *del_stmts;
  int num_tbl, i, n;

  tables = rbl_get_all_global_tables (conn, &num_tbl);
  if (tables == NULL)
    {
      return ER_FAILED;
    }

  del_stmts = rbl_make_table_gc_stmts (conn, tables, num_tbl);
  if (del_stmts == NULL)
    {
      rbl_free_table_info (tables, num_tbl);
      return ER_FAILED;
    }

  for (i = 0; i < num_tbl; i++)
    {
      n = rbl_delete_table_rows (conn, &del_stmts[i], GLOBAL_GROUPID, NULL);
      if (n < 0)
        {
          rbl_free_table_info (tables, num_tbl);
          rbl_free_table_gc_stmts (del_stmts, num_tbl);
          return ER_FAILED;
        }

      RBL_DEBUG (ARG_FILE_LINE, "Table = %s, Deleted rows = %d", tables[i].table_name, n);
    }

  rbl_free_table_info (tables, num_tbl);
  rbl_free_table_gc_stmts (del_stmts, num_tbl);
  return NO_ERROR;
}

static CCI_STMT *
rbl_make_table_gc_stmts (CCI_CONN * conn, TABLE_INFO * tables, int num_table)
{
  char sql[SQL_BUF_SIZE];
  CCI_STMT *del_stmts;
  int i;

  del_stmts = (CCI_STMT *) malloc (sizeof (CCI_STMT) * num_table);
  if (del_stmts == NULL)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_OUT_OF_MEMORY, sizeof (CCI_STMT) * num_table);
      return NULL;
    }

  for (i = 0; i < num_table; i++)
    {
      if (tables[i].skey_col_name == NULL)
        {
          sprintf (sql, "DELETE FROM [%s]", tables[i].table_name);
        }
      else
        {
          sprintf (sql, "DELETE FROM [%s] WHERE [%s] = ?", tables[i].table_name, tables[i].skey_col_name);
        }

      if (cci_prepare (conn, &del_stmts[i], sql, CCI_PREPARE_FROM_MIGRATOR) < 0)
        {
          RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR, conn->err_buf.err_code, conn->err_buf.err_msg);
          goto error_exit;
        }
    }

  return del_stmts;

error_exit:
  rbl_free_table_gc_stmts (del_stmts, i);
  return NULL;
}

static void
rbl_free_table_gc_stmts (CCI_STMT * stmts, int n)
{
  int i;

  if (stmts == NULL)
    {
      return;
    }

  for (i = 0; i < n; i++)
    {
      cci_close_req_handle (&stmts[i]);
    }

  free_and_init (stmts);
}

static int
rbl_delete_table_rows (CCI_CONN * dest_conn, CCI_STMT * del_stmt, int gid, char *skey)
{
  int n;

  if (skey != NULL)
    {
      if (cci_bind_param (del_stmt, 1, CCI_TYPE_VARCHAR, skey, 0) < 0)
        {
          RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR, del_stmt->err_buf.err_code, del_stmt->err_buf.err_msg);
          goto error_exit;
        }
    }

  n = cci_execute_with_gid (del_stmt, 0, 0, -gid);
  if (n < 0)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR, del_stmt->err_buf.err_code, del_stmt->err_buf.err_msg);
      goto error_exit;
    }

  cci_end_tran (dest_conn, CCI_TRAN_COMMIT);
  return n;

error_exit:
  cci_end_tran (dest_conn, CCI_TRAN_ROLLBACK);
  return ER_FAILED;
}

int
rbl_clear_destdb (CCI_CONN * conn, int gid)
{
  int error;

  if (gid == GLOBAL_GROUPID)
    {
      error = rbl_gc_global_table (conn);
    }
  else
    {
      error = rbl_gc_shard_table (conn, gid);
    }

  if (error == NO_ERROR && gid > GLOBAL_GROUPID)
    {
      error = cci_insert_gid_removed_info (conn, gid);
      if (error != NO_ERROR)
        {
          if (conn->err_buf.err_code == ER_BTREE_UNIQUE_FAILED)
            {
              error = NO_ERROR;
            }
          else
            {
              RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR, conn->err_buf.err_code, conn->err_buf.err_msg);
            }
        }
      cci_end_tran (conn, CCI_TRAN_COMMIT);
    }

  return error;
}
