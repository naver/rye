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
#include <sys/time.h>

#include "db.h"
#include "dbi.h"
#include "authenticate.h"
#include "schema_manager.h"
#include "transform.h"
#include "perf_monitor.h"
#include "cas_cci_internal.h"
#include "rbl_conf.h"
#include "rbl_error_log.h"
#include "rbl_table_info.h"
#include "rbl_move_group.h"

#define LIMIT_COUNT 1000

typedef struct pk_bind_info
{
  int col_index;
  T_CCI_TYPE type;
  union
  {
    int i;
    char *str;
    double d;
    DB_BIGINT bi;
    T_CCI_DATETIME dt;
    T_CCI_VARBIT bit;
  } val;
  bool is_null;
} PK_BIND_INFO;

typedef struct table_copy_stmt
{
  CCI_STMT sel_stmt;
  CCI_STMT ins_stmt;
  int num_cols;
  T_CCI_COL_INFO *col_info;
  int num_pk_cols;
  PK_BIND_INFO *pk_bind_info;
} TABLE_COPY_STMT;

static int rbl_copy_shard_table (RBL_COPY_CONTEXT * ctx, TABLE_INFO * tables,
				 int num_tbl);
static int rbl_copy_global_table (RBL_COPY_CONTEXT * ctx, TABLE_INFO * tables,
				  int num_tbl);
static int rbl_lock_src_global_table (RBL_COPY_CONTEXT * ctx,
				      char *table_name);
static TABLE_COPY_STMT *rbl_make_table_copy_stmt (RBL_COPY_CONTEXT * ctx,
						  TABLE_INFO * tables,
						  int num_table);
static int rbl_prepare_select_sql (CCI_CONN * conn, TABLE_INFO * table,
				   TABLE_COPY_STMT * stmt);
static int rbl_prepare_insert_sql (const char *table_name, int num_cols,
				   CCI_CONN * dest_conn, CCI_STMT * stmt);
static void rbl_free_table_copy_stmt (TABLE_COPY_STMT * info, int n);
static int rbl_copy_table_rows (RBL_COPY_CONTEXT * ctx,
				TABLE_COPY_STMT * stmt, char *skey);
static int rbl_copy_row (RBL_COPY_CONTEXT * ctx, CCI_STMT * sel_stmt,
			 CCI_STMT * ins_stmt, int num_cols,
			 T_CCI_COL_INFO * col_info);
static int rbl_bind_value (CCI_STMT * sel_stmt, T_CCI_COL_INFO * col_info,
			   CCI_STMT * ins_stmt, int index);
static int rbl_bind_null (CCI_STMT * ins_stmt, int index);
static int rbl_bind_string (CCI_STMT * sel_stmt, CCI_STMT * ins_stmt,
			    int index);
static int rbl_bind_int (CCI_STMT * sel_stmt, CCI_STMT * ins_stmt, int index);
static int rbl_bind_bigint (CCI_STMT * sel_stmt, CCI_STMT * ins_stmt,
			    int index);
static int rbl_bind_double (CCI_STMT * sel_stmt, CCI_STMT * ins_stmt,
			    int index);
static int rbl_bind_date (CCI_STMT * sel_stmt, CCI_STMT * ins_stmt,
			  int index);
static int rbl_bind_time (CCI_STMT * sel_stmt, CCI_STMT * ins_stmt,
			  int index);
static int rbl_bind_datetime (CCI_STMT * sel_stmt, CCI_STMT * ins_stmt,
			      int index);
static int rbl_bind_numeric (CCI_STMT * sel_stmt, CCI_STMT * ins_stmt,
			     int index);
static int rbl_bind_bit (CCI_STMT * sel_stmt, CCI_STMT * ins_stmt, int index);
static void rbl_dump_stats (RBL_COPY_CONTEXT * ctx);


int
rbl_init_copy_context (RBL_COPY_CONTEXT * ctx, int gid, bool run_slave)
{
  ctx->gid = gid;
  ctx->num_skeys = 0;

  ctx->src_conn = rbl_conf_get_srcdb_conn ();
  ctx->dest_conn = rbl_conf_get_destdb_conn (RBL_COPY);

  if (ctx->src_conn == NULL || ctx->dest_conn == NULL)
    {
      return ER_FAILED;
    }

  ctx->last_error = NO_ERROR;
  ctx->interrupt = false;
  ctx->was_gid_updated = false;
  ctx->run_slave = run_slave;

  return NO_ERROR;
}

int
rbl_copy_group_data (RBL_COPY_CONTEXT * ctx)
{
  int error;
  TABLE_INFO *tables;
  int num_tbl;
  bool lock_global_table = false;

  if (ctx->gid == GLOBAL_GROUPID)
    {
      tables = rbl_get_all_global_tables (ctx->src_conn, &num_tbl);
      if (tables == NULL)
	{
	  return ER_FAILED;
	}
      error = rbl_copy_global_table (ctx, tables, num_tbl);
    }
  else
    {
      tables = rbl_get_all_shard_tables (ctx->src_conn, &num_tbl);
      if (tables == NULL)
	{
	  return ER_FAILED;
	}
      error = rbl_copy_shard_table (ctx, tables, num_tbl);
    }

  if (error == NO_ERROR)
    {
      if (rbl_sync_check_delay (ctx->sync_ctx) != NO_ERROR)
	{
	  rbl_free_table_info (tables, num_tbl);
	  return ER_FAILED;
	}

      if (ctx->gid == GLOBAL_GROUPID && num_tbl > 0)
	{
	  error = rbl_lock_src_global_table (ctx, tables[0].table_name);
	  if (error != NO_ERROR)
	    {
	      rbl_free_table_info (tables, num_tbl);
	      return error;
	    }
	  lock_global_table = true;
	}

      if (ctx->run_slave == true)
	{
	  error = rbl_conf_check_repl_delay (ctx->src_conn);
	  if (error != NO_ERROR)
	    {
	      rbl_free_table_info (tables, num_tbl);
	      return error;
	    }
	}

      if (lock_global_table == true && ctx->run_slave == false)
	{
	  error = rbl_conf_update_src_groupid (ctx, false, false);
	}
      else
	{
	  error = rbl_conf_update_src_groupid (ctx, false, true);
	}
      if (error == NO_ERROR)
	{
	  ctx->was_gid_updated = true;
	}
    }

  rbl_free_table_info (tables, num_tbl);

  return error;
}

static int
rbl_copy_shard_table (RBL_COPY_CONTEXT * ctx, TABLE_INFO * tables,
		      int num_tbl)
{
  char **skeys;
  TABLE_COPY_STMT *stmts;
  int i, j, error;

  skeys = rbl_get_shard_keys (ctx->src_conn, ctx->gid, &ctx->num_skeys);
  if (skeys == NULL)
    {
      return ER_FAILED;
    }

  stmts = rbl_make_table_copy_stmt (ctx, tables, num_tbl);
  if (stmts == NULL)
    {
      rbl_free_shard_keys (skeys, ctx->num_skeys);
      return ER_FAILED;
    }

  for (i = 0; i < ctx->num_skeys; i++)
    {
      for (j = 0; j < num_tbl; j++)
	{
	  error = rbl_copy_table_rows (ctx, &stmts[j], skeys[i]);
	  if (error != NO_ERROR)
	    {
	      RBL_DEBUG (ARG_FILE_LINE,
			 "shard data migration fail: shard key=%s, table=%s\n",
			 skeys[i], tables[j].table_name);

	      rbl_free_shard_keys (skeys, ctx->num_skeys);
	      rbl_free_table_copy_stmt (stmts, num_tbl);
	      return ER_FAILED;
	    }
	}

      RBL_DEBUG (ARG_FILE_LINE,
		 "shard data migration success: shard key=%s\n", skeys[i]);

      ctx->num_copied_keys++;
      rbl_dump_stats (ctx);
    }

  rbl_free_shard_keys (skeys, ctx->num_skeys);
  rbl_free_table_copy_stmt (stmts, num_tbl);

  return NO_ERROR;
}

static int
rbl_copy_global_table (RBL_COPY_CONTEXT * ctx, TABLE_INFO * tables,
		       int num_tbl)
{
  TABLE_COPY_STMT *stmts;
  int i, error;

  stmts = rbl_make_table_copy_stmt (ctx, tables, num_tbl);
  if (stmts == NULL)
    {
      return ER_FAILED;
    }

  for (i = 0; i < num_tbl; i++)
    {
      error = rbl_copy_table_rows (ctx, &stmts[i], NULL);
      if (error != NO_ERROR)
	{
	  RBL_DEBUG (ARG_FILE_LINE,
		     "global data migration fail: table=%s\n",
		     tables[i].table_name);

	  rbl_free_table_copy_stmt (stmts, num_tbl);
	  return ER_FAILED;
	}

      rbl_dump_stats (ctx);
    }

  ctx->num_copied_keys++;
  rbl_free_table_copy_stmt (stmts, num_tbl);

  return NO_ERROR;
}

static int
rbl_lock_src_global_table (RBL_COPY_CONTEXT * ctx, char *table_name)
{
  char sql[SQL_BUF_SIZE];
  CCI_STMT stmt;
  int error = NO_ERROR;

  sprintf (sql, "SELECT * FROM [%s] LIMIT 1 FOR UPDATE", table_name);

  if (cci_prepare (ctx->src_conn, &stmt, sql, CCI_PREPARE_FROM_MIGRATOR) < 0)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		 ctx->src_conn->err_buf.err_code,
		 ctx->src_conn->err_buf.err_msg);
      return RBL_CCI_ERROR;
    }

  if (cci_execute_with_gid (&stmt, 0, 0, ctx->gid) < 0)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		 stmt.err_buf.err_code, stmt.err_buf.err_msg);
      error = RBL_CCI_ERROR;
    }

  cci_close_req_handle (&stmt);

  return error;
}

static PK_BIND_INFO *
rbl_make_pk_bind_info (CCI_CONN * conn, char *table_name, int *num_pk_cols)
{
  char sql[SQL_BUF_SIZE];
  CCI_STMT stmt;
  int ind, i, n;
  PK_BIND_INFO *pk_bind = NULL;
  int c_index;

  sprintf (sql, "SELECT c.def_order FROM db_column c, db_index_key i "
	   "WHERE c.table_name = '%s' AND c.table_name = i.table_name "
	   "AND c.col_name = i.key_col_name ORDER BY i.key_order",
	   table_name);

  if (cci_prepare (conn, &stmt, sql, CCI_PREPARE_FROM_MIGRATOR) < 0)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		 conn->err_buf.err_code, conn->err_buf.err_msg);
      return NULL;
    }

  n = cci_execute (&stmt, 0, 0);
  if (n < 0)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		 stmt.err_buf.err_code, stmt.err_buf.err_msg);
      goto handle_error;
    }

  pk_bind = (PK_BIND_INFO *) calloc (n, sizeof (PK_BIND_INFO));
  if (pk_bind == NULL)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_OUT_OF_MEMORY, sizeof (PK_BIND_INFO) * n);
      goto handle_error;
    }

  for (i = 0; i < n; i++)
    {
      if (cci_fetch_next (&stmt) < 0)
	{
	  RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		     stmt.err_buf.err_code, stmt.err_buf.err_msg);
	  goto handle_error;
	}

      c_index = cci_get_int (&stmt, 1, &ind);
      if (stmt.err_buf.err_code != CCI_ER_NO_ERROR)
	{
	  RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		     stmt.err_buf.err_code, stmt.err_buf.err_msg);
	  goto handle_error;
	}

      pk_bind[i].col_index = c_index;
      pk_bind[i].type = CCI_TYPE_NULL;
      pk_bind[i].is_null = true;
    }

  RBL_ASSERT (cci_fetch_next (&stmt) == CCI_ER_NO_MORE_DATA);

  cci_close_req_handle (&stmt);
  cci_end_tran (conn, CCI_TRAN_COMMIT);
  *num_pk_cols = n;

  return pk_bind;

handle_error:

  cci_close_req_handle (&stmt);
  cci_end_tran (conn, CCI_TRAN_ROLLBACK);

  if (pk_bind != NULL)
    {
      free (pk_bind);
    }
  assert (0);

  return NULL;

}

static TABLE_COPY_STMT *
rbl_make_table_copy_stmt (RBL_COPY_CONTEXT * ctx, TABLE_INFO * tables,
			  int num_table)
{
  TABLE_COPY_STMT *stmt_array;
  TABLE_COPY_STMT *stmt;
  int i;

  stmt_array = (TABLE_COPY_STMT *) malloc (sizeof (TABLE_COPY_STMT)
					   * num_table);
  if (stmt_array == NULL)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_OUT_OF_MEMORY,
		 sizeof (TABLE_COPY_STMT) * num_table);
      return NULL;
    }

  for (i = 0; i < num_table; i++)
    {
      stmt = &stmt_array[i];

      stmt->pk_bind_info = rbl_make_pk_bind_info (ctx->src_conn,
						  tables[i].table_name,
						  &stmt->num_pk_cols);
      if (stmt->pk_bind_info == NULL)
	{
	  goto error_exit;
	}

      if (rbl_prepare_select_sql (ctx->src_conn, &tables[i], stmt)
	  != NO_ERROR)
	{
	  goto error_exit;
	}

      if (rbl_prepare_insert_sql (tables[i].table_name, stmt->num_cols,
				  ctx->dest_conn, &stmt->ins_stmt)
	  != NO_ERROR)
	{
	  goto error_exit;
	}
    }

  return stmt_array;

error_exit:

  rbl_free_table_copy_stmt (stmt_array, i);
  return NULL;
}

static int
rbl_prepare_select_sql (CCI_CONN * conn, TABLE_INFO * table,
			TABLE_COPY_STMT * stmt)
{
  int i;
  char sql[SQL_BUF_SIZE];

  if (table->skey_col_name == NULL)
    {
      sprintf (sql, "SELECT * FROM [%s] LIMIT %d PRIMARY KEY NEXT (",
	       table->table_name, LIMIT_COUNT);
    }
  else
    {
      sprintf (sql, "SELECT * FROM [%s] WHERE [%s] = ? "
	       "LIMIT %d PRIMARY KEY NEXT (",
	       table->table_name, table->skey_col_name, LIMIT_COUNT);
    }

  for (i = 0; i < stmt->num_pk_cols; i++)
    {
      strcat (sql, "?");
      if (i < stmt->num_pk_cols - 1)
	{
	  strcat (sql, ", ");
	}
    }
  strcat (sql, ") FOR UPDATE");

  if (cci_prepare (conn, &stmt->sel_stmt, sql, CCI_PREPARE_FROM_MIGRATOR) < 0)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		 conn->err_buf.err_code, conn->err_buf.err_msg);
      return RBL_CCI_ERROR;
    }

  stmt->col_info = cci_get_result_info (&stmt->sel_stmt, NULL,
					&stmt->num_cols);
  if (stmt->col_info == NULL)
    {
      return RBL_CCI_ERROR;
    }

  return NO_ERROR;
}

static int
rbl_prepare_insert_sql (const char *table_name, int num_cols,
			CCI_CONN * dest_conn, CCI_STMT * stmt)
{
  char sql[SQL_BUF_SIZE];
  int i;

  sprintf (sql, "INSERT INTO [%s] VALUES (", table_name);

  for (i = 0; i < num_cols; i++)
    {
      strcat (sql, "?");
      if (i < num_cols - 1)
	{
	  strcat (sql, ", ");
	}
    }

  strcat (sql, ");");

  if (cci_prepare (dest_conn, stmt, sql, CCI_PREPARE_FROM_MIGRATOR) < 0)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		 dest_conn->err_buf.err_code, dest_conn->err_buf.err_msg);
      return RBL_CCI_ERROR;
    }

  return NO_ERROR;
}

static void
rbl_free_table_copy_stmt (TABLE_COPY_STMT * info, int n)
{
  int i, j;

  if (info == NULL)
    {
      return;
    }

  for (i = 0; i < n; i++)
    {
      cci_close_req_handle (&info[i].sel_stmt);
      cci_close_req_handle (&info[i].ins_stmt);

      for (j = 0; j < info[i].num_pk_cols; j++)
	{
	  if (info[i].pk_bind_info[j].is_null != true
	      && (info[i].pk_bind_info[j].type == CCI_TYPE_VARCHAR
		  || info[i].pk_bind_info[j].type == CCI_TYPE_NUMERIC))
	    {
	      free (info[i].pk_bind_info[j].val.str);
	    }
	}
      free (info[i].pk_bind_info);
    }

  free (info);
}

static void
rbl_save_last_pk_value (TABLE_COPY_STMT * stmt)
{
  int i, col_idx, ind;
  T_CCI_TYPE type;
  CCI_STMT *sel_stmt;

  sel_stmt = &stmt->sel_stmt;

  for (i = 0; i < stmt->num_pk_cols; i++)
    {
      col_idx = stmt->pk_bind_info[i].col_index + 1;
      type = CCI_GET_RESULT_INFO_TYPE (stmt->col_info, col_idx);
      stmt->pk_bind_info[i].type = type;

      switch (type)
	{
	case CCI_TYPE_VARCHAR:
	case CCI_TYPE_NUMERIC:
	  {
	    char *str;

	    if (stmt->pk_bind_info[i].is_null == false)
	      {
		free_and_init (stmt->pk_bind_info[i].val.str);
	      }

	    str = cci_get_string (sel_stmt, col_idx, &ind);
	    if (str == NULL || ind == -1)
	      {
		stmt->pk_bind_info[i].is_null = true;
	      }
	    else
	      {
		stmt->pk_bind_info[i].val.str = strdup (str);
		stmt->pk_bind_info[i].is_null = false;
	      }
	  }
	  break;

	case CCI_TYPE_INT:
	  {
	    int int_val;

	    int_val = cci_get_int (sel_stmt, col_idx, &ind);
	    if (ind == -1)
	      {
		stmt->pk_bind_info[i].is_null = true;
	      }
	    else
	      {
		stmt->pk_bind_info[i].val.i = int_val;
		stmt->pk_bind_info[i].is_null = false;
	      }
	  }
	  break;

	case CCI_TYPE_BIGINT:
	  {
	    DB_BIGINT bigint_val;

	    bigint_val = cci_get_bigint (sel_stmt, col_idx, &ind);
	    if (ind == -1)
	      {
		stmt->pk_bind_info[i].is_null = true;
	      }
	    else
	      {
		stmt->pk_bind_info[i].val.bi = bigint_val;
		stmt->pk_bind_info[i].is_null = false;
	      }
	  }
	  break;

	case CCI_TYPE_DOUBLE:
	  {
	    double d_val;

	    d_val = cci_get_double (sel_stmt, col_idx, &ind);
	    if (ind == -1)
	      {
		stmt->pk_bind_info[i].is_null = true;
	      }
	    else
	      {
		stmt->pk_bind_info[i].val.d = d_val;
		stmt->pk_bind_info[i].is_null = false;
	      }
	  }
	  break;

	case CCI_TYPE_DATE:
	case CCI_TYPE_TIME:
	case CCI_TYPE_DATETIME:
	  {
	    T_CCI_DATETIME dt;

	    dt = cci_get_datetime (sel_stmt, col_idx, &ind);
	    if (ind == -1)
	      {
		stmt->pk_bind_info[i].is_null = true;
	      }
	    else
	      {
		stmt->pk_bind_info[i].val.dt = dt;
		stmt->pk_bind_info[i].is_null = false;
	      }
	  }
	  break;

	case CCI_TYPE_VARBIT:
	  {
	    T_CCI_VARBIT bit;

	    bit = cci_get_bit (sel_stmt, col_idx, &ind);
	    if (ind == -1)
	      {
		stmt->pk_bind_info[i].is_null = true;
	      }
	    else
	      {
		stmt->pk_bind_info[i].val.bit = bit;
		stmt->pk_bind_info[i].is_null = false;
	      }
	  }
	  break;

	default:
	  RBL_ASSERT (false);
	  break;
	}
    }
}

static int
rbl_bind_pk_value (TABLE_COPY_STMT * stmt, int start_index)
{
  int i, bind_idx;
  CCI_STMT *sel_stmt;
  void *val;

  sel_stmt = &stmt->sel_stmt;

  for (i = 0, bind_idx = start_index; i < stmt->num_pk_cols; i++, bind_idx++)
    {
      if (stmt->pk_bind_info[i].is_null == true)
	{
	  if (cci_bind_param (sel_stmt, bind_idx, CCI_TYPE_NULL, NULL, 0) < 0)
	    {
	      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
			 sel_stmt->err_buf.err_code,
			 sel_stmt->err_buf.err_msg);
	      return ER_FAILED;
	    }

	  continue;
	}

      switch (stmt->pk_bind_info[i].type)
	{
	case CCI_TYPE_VARCHAR:
	case CCI_TYPE_NUMERIC:
	  val = stmt->pk_bind_info[i].val.str;
	  break;
	case CCI_TYPE_INT:
	  val = &(stmt->pk_bind_info[i].val.i);
	  break;
	case CCI_TYPE_BIGINT:
	  val = &(stmt->pk_bind_info[i].val.bi);
	  break;
	case CCI_TYPE_DOUBLE:
	  val = &(stmt->pk_bind_info[i].val.d);
	  break;
	case CCI_TYPE_DATE:
	case CCI_TYPE_TIME:
	case CCI_TYPE_DATETIME:
	  val = &(stmt->pk_bind_info[i].val.dt);
	  break;
	case CCI_TYPE_VARBIT:
	  val = &(stmt->pk_bind_info[i].val.bit);
	  break;
	default:
	  RBL_ASSERT (false);
	  return ER_FAILED;
	}

      if (cci_bind_param (sel_stmt, bind_idx, stmt->pk_bind_info[i].type,
			  val, 0) < 0)
	{
	  RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		     sel_stmt->err_buf.err_code, sel_stmt->err_buf.err_msg);
	  return ER_FAILED;
	}
    }

  return NO_ERROR;
}

static int
rbl_copy_table_rows (RBL_COPY_CONTEXT * ctx, TABLE_COPY_STMT * stmt,
		     char *skey)
{
  CCI_STMT *sel_stmt;
  int i, n, bind_index, error;

  RBL_ASSERT (cci_get_autocommit (ctx->src_conn) == CCI_AUTOCOMMIT_FALSE);

  sel_stmt = &stmt->sel_stmt;

  do
    {
      bind_index = 1;
      if (skey != NULL)
	{
	  if (cci_bind_param (sel_stmt, bind_index, CCI_TYPE_VARCHAR,
			      skey, 0) < 0)
	    {
	      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
			 sel_stmt->err_buf.err_code,
			 sel_stmt->err_buf.err_msg);
	      goto error_exit;
	    }

	  bind_index++;
	}

      error = rbl_bind_pk_value (stmt, bind_index);
      if (error != NO_ERROR)
	{
	  goto error_exit;
	}

      n = cci_execute_with_gid (sel_stmt, 0, 0, ctx->gid);
      if (n < 0)
	{
	  RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		     sel_stmt->err_buf.err_code, sel_stmt->err_buf.err_msg);
	  goto error_exit;
	}

      for (i = 0; i < n; i++)
	{
	  if (cci_fetch_next (sel_stmt) < 0)
	    {
	      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
			 sel_stmt->err_buf.err_code,
			 sel_stmt->err_buf.err_msg);
	      goto error_exit;
	    }

	  if (rbl_copy_row (ctx, sel_stmt, &stmt->ins_stmt, stmt->num_cols,
			    stmt->col_info) != NO_ERROR)
	    {
	      goto error_exit;
	    }
	}

      if (n == LIMIT_COUNT)
	{
	  rbl_save_last_pk_value (stmt);
	}

      RBL_ASSERT (cci_fetch_next (sel_stmt) == CCI_ER_NO_MORE_DATA);
    }
  while (n == LIMIT_COUNT);

  cci_end_tran (ctx->src_conn, CCI_TRAN_COMMIT);
  cci_end_tran (ctx->dest_conn, CCI_TRAN_COMMIT);

  return NO_ERROR;

error_exit:
  cci_end_tran (ctx->src_conn, CCI_TRAN_ROLLBACK);
  cci_end_tran (ctx->dest_conn, CCI_TRAN_ROLLBACK);

  return ER_FAILED;
}

static int
rbl_copy_row (RBL_COPY_CONTEXT * ctx, CCI_STMT * sel_stmt,
	      CCI_STMT * ins_stmt, int num_cols, T_CCI_COL_INFO * col_info)
{
  int i, error;

  if (ctx->interrupt == true)
    {
      RBL_ERROR_MSG (ARG_FILE_LINE, "Interrupted by log sync\n");
      return ER_INTERRUPTED;
    }

  for (i = 0; i < num_cols; i++)
    {
      if (rbl_bind_value (sel_stmt, col_info, ins_stmt, i + 1) < 0)
	{
	  return ER_FAILED;
	}
    }

  error = cci_execute_with_gid (ins_stmt, 0, 0, -ctx->gid);
  if (error < 0)
    {
      if (ins_stmt->err_buf.err_code == ER_BTREE_UNIQUE_FAILED)
	{
	  ctx->num_copied_collision++;
	}
      else
	{
	  RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		     ins_stmt->err_buf.err_code, ins_stmt->err_buf.err_msg);
	  return ER_FAILED;
	}
    }
  else
    {
      ctx->num_copied_rows++;
    }

  return NO_ERROR;
}

static int
rbl_bind_value (CCI_STMT * sel_stmt, T_CCI_COL_INFO * col_info,
		CCI_STMT * ins_stmt, int index)
{
  T_CCI_TYPE type;

  type = CCI_GET_RESULT_INFO_TYPE (col_info, index);

  switch (type)
    {
    case CCI_TYPE_NULL:
    case CCI_TYPE_VARCHAR:
      return rbl_bind_string (sel_stmt, ins_stmt, index);
    case CCI_TYPE_INT:
      return rbl_bind_int (sel_stmt, ins_stmt, index);
    case CCI_TYPE_BIGINT:
      return rbl_bind_bigint (sel_stmt, ins_stmt, index);
    case CCI_TYPE_DOUBLE:
      return rbl_bind_double (sel_stmt, ins_stmt, index);
    case CCI_TYPE_DATE:
      return rbl_bind_date (sel_stmt, ins_stmt, index);
    case CCI_TYPE_TIME:
      return rbl_bind_time (sel_stmt, ins_stmt, index);
    case CCI_TYPE_DATETIME:
      return rbl_bind_datetime (sel_stmt, ins_stmt, index);
    case CCI_TYPE_NUMERIC:
      return rbl_bind_numeric (sel_stmt, ins_stmt, index);
    case CCI_TYPE_VARBIT:
      return rbl_bind_bit (sel_stmt, ins_stmt, index);

    default:
      RBL_ASSERT (false);
      break;
    }

  return NO_ERROR;
}

static int
rbl_bind_null (CCI_STMT * ins_stmt, int index)
{
  if (cci_bind_param (ins_stmt, index, CCI_TYPE_NULL, NULL, 0) < 0)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		 ins_stmt->err_buf.err_code, ins_stmt->err_buf.err_msg);
      return ER_FAILED;
    }

  return NO_ERROR;
}

static int
rbl_bind_string (CCI_STMT * sel_stmt, CCI_STMT * ins_stmt, int index)
{
  char *str;
  int ind;

  str = cci_get_string (sel_stmt, index, &ind);
  if (ind == -1)
    {
      return rbl_bind_null (ins_stmt, index);
    }

  if (cci_bind_param (ins_stmt, index, CCI_TYPE_VARCHAR, str, 0) < 0)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		 ins_stmt->err_buf.err_code, ins_stmt->err_buf.err_msg);
      return ER_FAILED;
    }

  return NO_ERROR;
}

static int
rbl_bind_int (CCI_STMT * sel_stmt, CCI_STMT * ins_stmt, int index)
{
  int int_val;
  int ind;

  int_val = cci_get_int (sel_stmt, index, &ind);
  if (ind == -1)
    {
      return rbl_bind_null (ins_stmt, index);
    }

  if (cci_bind_param (ins_stmt, index, CCI_TYPE_INT, &int_val, 0) < 0)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		 ins_stmt->err_buf.err_code, ins_stmt->err_buf.err_msg);
      return ER_FAILED;
    }

  return NO_ERROR;
}

static int
rbl_bind_bigint (CCI_STMT * sel_stmt, CCI_STMT * ins_stmt, int index)
{
  DB_BIGINT bigint_val;
  int ind;

  bigint_val = cci_get_bigint (sel_stmt, index, &ind);
  if (ind == -1)
    {
      return rbl_bind_null (ins_stmt, index);
    }

  if (cci_bind_param (ins_stmt, index, CCI_TYPE_BIGINT, &bigint_val, 0) < 0)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		 ins_stmt->err_buf.err_code, ins_stmt->err_buf.err_msg);
      return ER_FAILED;
    }

  return NO_ERROR;
}

static int
rbl_bind_double (CCI_STMT * sel_stmt, CCI_STMT * ins_stmt, int index)
{
  double d_val;
  int ind;

  d_val = cci_get_double (sel_stmt, index, &ind);
  if (ind == -1)
    {
      return rbl_bind_null (ins_stmt, index);
    }

  if (cci_bind_param (ins_stmt, index, CCI_TYPE_DOUBLE, &d_val, 0) < 0)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		 ins_stmt->err_buf.err_code, ins_stmt->err_buf.err_msg);
      return ER_FAILED;
    }

  return NO_ERROR;
}

static int
rbl_bind_date (CCI_STMT * sel_stmt, CCI_STMT * ins_stmt, int index)
{
  T_CCI_DATETIME d;
  int ind;

  d = cci_get_datetime (sel_stmt, index, &ind);
  if (ind == -1)
    {
      return rbl_bind_null (ins_stmt, index);
    }

  if (cci_bind_param (ins_stmt, index, CCI_TYPE_DATE, &d, 0) < 0)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		 ins_stmt->err_buf.err_code, ins_stmt->err_buf.err_msg);
      return ER_FAILED;
    }

  return NO_ERROR;
}

static int
rbl_bind_time (CCI_STMT * sel_stmt, CCI_STMT * ins_stmt, int index)
{
  T_CCI_DATETIME d;
  int ind;

  d = cci_get_datetime (sel_stmt, index, &ind);
  if (ind == -1)
    {
      return rbl_bind_null (ins_stmt, index);
    }

  if (cci_bind_param (ins_stmt, index, CCI_TYPE_TIME, &d, 0) < 0)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		 ins_stmt->err_buf.err_code, ins_stmt->err_buf.err_msg);
      return ER_FAILED;
    }

  return NO_ERROR;
}

static int
rbl_bind_datetime (CCI_STMT * sel_stmt, CCI_STMT * ins_stmt, int index)
{
  T_CCI_DATETIME d;
  int ind;

  d = cci_get_datetime (sel_stmt, index, &ind);
  if (ind == -1)
    {
      return rbl_bind_null (ins_stmt, index);
    }

  if (cci_bind_param (ins_stmt, index, CCI_TYPE_DATETIME, &d, 0) < 0)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		 ins_stmt->err_buf.err_code, ins_stmt->err_buf.err_msg);
      return ER_FAILED;
    }

  return NO_ERROR;
}

static int
rbl_bind_numeric (CCI_STMT * sel_stmt, CCI_STMT * ins_stmt, int index)
{
  char *str;
  int ind;

  str = cci_get_string (sel_stmt, index, &ind);
  if (ind == -1)
    {
      return rbl_bind_null (ins_stmt, index);
    }

  if (cci_bind_param (ins_stmt, index, CCI_TYPE_NUMERIC, str, 0) < 0)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		 ins_stmt->err_buf.err_code, ins_stmt->err_buf.err_msg);
      return ER_FAILED;
    }

  return NO_ERROR;
}

static int
rbl_bind_bit (CCI_STMT * sel_stmt, CCI_STMT * ins_stmt, int index)
{
  T_CCI_VARBIT bit;
  int ind;

  bit = cci_get_bit (sel_stmt, index, &ind);
  if (ind == -1)
    {
      return rbl_bind_null (ins_stmt, index);
    }

  if (cci_bind_param (ins_stmt, index, CCI_TYPE_VARBIT, &bit, 0) < 0)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		 ins_stmt->err_buf.err_code, ins_stmt->err_buf.err_msg);
      return ER_FAILED;
    }

  return NO_ERROR;
}

static void
rbl_dump_stats (UNUSED_ARG RBL_COPY_CONTEXT * ctx)
{
#if !defined(NDEBUG)
  struct timeval end_time, elapsed_time;

  if (ctx->num_skeys == ctx->num_copied_keys
      || (ctx->num_copied_keys % 100) == 0)
    {
      gettimeofday (&end_time, NULL);
      DIFF_TIMEVAL (ctx->start_time, end_time, elapsed_time);
      if (elapsed_time.tv_sec < 1)
	{
	  return;
	}

      RBL_DEBUG (ARG_FILE_LINE, "Migration status: \n"
		 "num_shard_keys: %d\n" "num_copied_keys: %d\n"
		 "num_copied_rows: %d\n" "num_copied_collision: %d\n"
		 "num_synced_rows: %d\n" "num_synced_collision: %d\n"
		 "synced_lsa: (%ld, %d)\n" "server_lsa: (%ld, %d)\n"
		 "running_time: %lds\n" "shard_key/sec: %4.1f\n"
		 "row/sec: %d\n", ctx->num_skeys, ctx->num_copied_keys,
		 ctx->num_copied_rows, ctx->num_copied_collision,
		 ctx->sync_ctx->num_synced_rows,
		 ctx->sync_ctx->num_synced_collision,
		 ctx->sync_ctx->synced_lsa.pageid,
		 ctx->sync_ctx->synced_lsa.offset,
		 ctx->sync_ctx->server_lsa.pageid,
		 ctx->sync_ctx->server_lsa.offset, elapsed_time.tv_sec,
		 (float) ctx->num_copied_keys / elapsed_time.tv_sec,
		 (ctx->num_copied_rows +
		  ctx->sync_ctx->num_synced_rows) / elapsed_time.tv_sec);
    }
#endif
}
