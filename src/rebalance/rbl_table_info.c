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
 * rbl_table_info.c -
 */

#ident "$Id$"

#include <stdio.h>

#include "porting.h"
#include "db.h"
#include "error_manager.h"
#include "transform.h"

#include "cas_cci_internal.h"
#include "rbl_error_log.h"
#include "rbl_table_info.h"

char **
rbl_get_shard_keys (CCI_CONN * conn, int gid, int *num_keys)
{
  char sql[SQL_BUF_SIZE];
  CCI_STMT stmt;
  char **skey_array = NULL;
  char *skey;
  int n, i, ind;

  RBL_ASSERT (num_keys != NULL);

  sprintf (sql, "SELECT skey FROM [%s] WHERE gid = %d", CT_SHARD_GID_SKEY_INFO_NAME, gid);

  if (cci_prepare (conn, &stmt, sql, CCI_PREPARE_FROM_MIGRATOR) < 0)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR, conn->err_buf.err_code, conn->err_buf.err_msg);
      return NULL;
    }

  n = cci_execute (&stmt, 0, 0);
  if (n < 0)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR, stmt.err_buf.err_code, stmt.err_buf.err_msg);
      goto error_exit;
    }

  skey_array = (char **) malloc (sizeof (char *) * n);
  if (skey_array == NULL)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_OUT_OF_MEMORY, sizeof (char *) * n);
      goto error_exit;
    }

  for (i = 0; i < n; i++)
    {
      if (cci_fetch_next (&stmt) < 0)
        {
          RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR, stmt.err_buf.err_code, stmt.err_buf.err_msg);
          goto error_exit;
        }

      skey = cci_get_string (&stmt, 1, &ind);
      if (skey == NULL && stmt.err_buf.err_code != CCI_ER_NO_ERROR)
        {
          RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR, stmt.err_buf.err_code, stmt.err_buf.err_msg);
          goto error_exit;
        }

      skey_array[i] = strdup (skey);
    }

  RBL_ASSERT (cci_fetch_next (&stmt) == CCI_ER_NO_MORE_DATA);

  cci_close_req_handle (&stmt);
  cci_end_tran (conn, CCI_TRAN_COMMIT);

  *num_keys = n;
  return skey_array;

error_exit:

  cci_close_req_handle (&stmt);
  cci_end_tran (conn, CCI_TRAN_ROLLBACK);

  if (skey_array != NULL)
    {
      rbl_free_shard_keys (skey_array, n);
    }

  return NULL;
}

void
rbl_free_shard_keys (char **keys, int n)
{
  int i;

  if (keys == NULL)
    {
      return;
    }

  for (i = 0; i < n; i++)
    {
      free_and_init (keys[i]);
    }

  free_and_init (keys);
}

TABLE_INFO *
rbl_get_all_shard_tables (CCI_CONN * conn, int *num_table)
{
  char sql[SQL_BUF_SIZE];
  CCI_STMT stmt;
  int ind, i, n;
  TABLE_INFO *tables = NULL;
  char *t_name, *c_name;

  sprintf (sql, "SELECT table_name, col_name FROM db_column WHERE is_shard_key = 1");

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

  tables = (TABLE_INFO *) malloc (sizeof (TABLE_INFO) * n);
  if (tables == NULL)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_OUT_OF_MEMORY, sizeof (TABLE_INFO) * n);
      goto handle_error;
    }

  for (i = 0; i < n; i++)
    {
      if (cci_fetch_next (&stmt) < 0)
        {
          RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR, stmt.err_buf.err_code, stmt.err_buf.err_msg);
          goto handle_error;
        }

      t_name = cci_get_string (&stmt, 1, &ind);
      if (t_name == NULL && stmt.err_buf.err_code != CCI_ER_NO_ERROR)
        {
          RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR, stmt.err_buf.err_code, stmt.err_buf.err_msg);
          goto handle_error;
        }

      c_name = cci_get_string (&stmt, 2, &ind);
      if (c_name == NULL && stmt.err_buf.err_code != CCI_ER_NO_ERROR)
        {
          RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR, stmt.err_buf.err_code, stmt.err_buf.err_msg);
          goto handle_error;
        }

      tables[i].table_name = strdup (t_name);
      tables[i].skey_col_name = strdup (c_name);
    }

  RBL_ASSERT (cci_fetch_next (&stmt) == CCI_ER_NO_MORE_DATA);

  cci_close_req_handle (&stmt);
  cci_end_tran (conn, CCI_TRAN_COMMIT);
  *num_table = n;

  return tables;

handle_error:

  cci_close_req_handle (&stmt);
  cci_end_tran (conn, CCI_TRAN_ROLLBACK);

  if (tables != NULL)
    {
      rbl_free_table_info (tables, n);
    }

  return NULL;
}

TABLE_INFO *
rbl_get_all_global_tables (CCI_CONN * conn, int *num_table)
{
  char sql[SQL_BUF_SIZE];
  CCI_STMT stmt;
  TABLE_INFO *tables = NULL;
  int n, i, ind;
  char *t_name;

  sprintf (sql,
           "(SELECT table_name FROM db_table "
           " WHERE is_system_table <> 1 AND table_type = 0 "
           " AND table_name <> 'shard_db' AND table_name <> 'shard_node' "
           " AND table_name <> 'shard_groupid' AND table_name <> 'shard_migration') "
           "DIFFERENCE " "(SELECT table_name FROM db_column WHERE is_shard_key = 1)");

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

  tables = (TABLE_INFO *) malloc (sizeof (TABLE_INFO) * n);
  if (tables == NULL)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_OUT_OF_MEMORY, sizeof (TABLE_INFO) * n);
      goto handle_error;
    }

  for (i = 0; i < n; i++)
    {
      if (cci_fetch_next (&stmt) < 0)
        {
          RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR, stmt.err_buf.err_code, stmt.err_buf.err_msg);
          goto handle_error;
        }

      t_name = cci_get_string (&stmt, 1, &ind);
      if (t_name == NULL && stmt.err_buf.err_code != CCI_ER_NO_ERROR)
        {
          RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR, stmt.err_buf.err_code, stmt.err_buf.err_msg);
          goto handle_error;
        }

      tables[i].table_name = strdup (t_name);
      tables[i].skey_col_name = NULL;
    }

  RBL_ASSERT (cci_fetch_next (&stmt) == CCI_ER_NO_MORE_DATA);

  cci_close_req_handle (&stmt);
  cci_end_tran (conn, CCI_TRAN_COMMIT);
  *num_table = n;

  return tables;

handle_error:

  cci_close_req_handle (&stmt);
  cci_end_tran (conn, CCI_TRAN_ROLLBACK);

  if (tables != NULL)
    {
      rbl_free_table_info (tables, n);
    }

  return NULL;
}

void
rbl_free_table_info (TABLE_INFO * info, int n)
{
  int i;

  if (info == NULL)
    {
      return;
    }

  for (i = 0; i < n; i++)
    {
      if (info[i].table_name != NULL)
        {
          free_and_init (info[i].table_name);
        }
      if (info[i].skey_col_name != NULL)
        {
          free_and_init (info[i].skey_col_name);
        }
    }

  free_and_init (info);
}
