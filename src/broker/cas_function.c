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
 * cas_function.c -
 */

#ident "$Id$"

#include <stdio.h>
#include <stdlib.h>

#include <unistd.h>
#include <sys/time.h>
#include <assert.h>

#include "error_manager.h"
#include "network_interface_cl.h"

#include "cas_common.h"
#include "cas.h"
#include "cas_function.h"
#include "cas_network.h"
#include "cas_net_buf.h"
#include "cas_log.h"
#include "cas_handle.h"
#include "cas_util.h"
#include "cas_execute.h"

static FN_RETURN fn_prepare_internal (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info);
static FN_RETURN fn_execute_internal (int argc, void **argv,
                                      T_NET_BUF * net_buf, T_REQ_INFO * req_info, int *prepared_srv_h_id);
static const char *get_tran_type_str (int tran_type);
static void bind_value_print (T_CAS_LOG_TYPE cas_log_type, char type, void *net_value);
static const char *get_error_log_eids (int err);
static void bind_value_log (T_CAS_LOG_TYPE cas_log_type,
                            struct timeval *log_time, int argc, void **argv, unsigned int query_seq_num);
int set_query_timeout (int query_timeout, const char **from);

/* functions implemented in transaction_cl.c */
extern void tran_set_query_timeout (int);

static void update_error_query_count (T_APPL_SERVER_INFO * as_info_p, const T_ERROR_INFO * err_info_p);

static const char *tran_type_str[] = { "COMMIT", "ROLLBACK" };

static const char *type_str_tbl[CCI_TYPE_LAST + 1] = {
  "NULL",                       /* CCI_TYPE_NULL */
  "VARCHAR",                    /* CCI_TYPE_VARCHAR */
  "VARBINARY",                  /* CCI_TYPE_VARBIT */
  "NUMERIC",                    /* CCI_TYPE_NUMERIC */
  "INT",                        /* CCI_TYPE_INT */
  "DOUBLE",                     /* CCI_TYPE_DOUBLE */
  "DATE",                       /* CCI_TYPE_DATE */
  "TIME",                       /* CCI_TYPE_TIME */
  "BIGINT",                     /* CCI_TYPE_BIGINT */
  "DATETIME"                    /* CCI_TYPE_DATETIME */
};

FN_RETURN
fn_end_tran (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  char tran_type;
  int err_code;
  int elapsed_sec = 0, elapsed_msec = 0;
  struct timeval end_tran_begin, end_tran_end;
  int timeout;

  if (argc < 1)
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  net_arg_get_char (&tran_type, argv[0]);
  if (tran_type != CCI_TRAN_COMMIT && tran_type != CCI_TRAN_ROLLBACK)
    {
      ERROR_INFO_SET (CAS_ER_TRAN_TYPE, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  cas_sql_log_write (0, "end_tran %s", get_tran_type_str (tran_type));

  gettimeofday (&end_tran_begin, NULL);

  err_code = ux_end_tran ((char) tran_type, false);

  gettimeofday (&end_tran_end, NULL);
  ut_timeval_diff (&end_tran_begin, &end_tran_end, &elapsed_sec, &elapsed_msec);

  cas_sql_log_write (0, "end_tran %s%d time %d.%03d%s",
                     err_code < 0 ? "error:" : "",
                     err_Info.err_number, elapsed_sec, elapsed_msec, get_error_log_eids (err_Info.err_number));

  if (err_code < 0)
    {
      ERROR_INFO_SET (err_code, DBMS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      req_info->need_rollback = TRUE;
    }
  else
    {
      req_info->need_rollback = FALSE;
    }

  timeout = ut_check_timeout (&tran_Start_time, &end_tran_end,
                              shm_Appl->long_transaction_time, &elapsed_sec, &elapsed_msec);
  if (timeout >= 0)
    {
      as_Info->num_long_transactions %= MAX_DIAG_DATA_VALUE;
      as_Info->num_long_transactions++;
      sql_log_Notice_mode_flush = true;
    }

  if (err_code < 0 || sql_log_Notice_mode_flush == true)
    {
      cas_sql_log_end (true, elapsed_sec, elapsed_msec);
    }
  else
    {
      cas_sql_log_end ((as_Info->cur_sql_log_mode == SQL_LOG_MODE_ALL), elapsed_sec, elapsed_msec);
    }

  gettimeofday (&tran_Start_time, NULL);
  gettimeofday (&query_Start_time, NULL);
  sql_log_Notice_mode_flush = false;

  assert (as_Info->con_status == CON_STATUS_IN_TRAN);
  as_Info->con_status = CON_STATUS_OUT_TRAN;
  as_Info->transaction_start_time = (time_t) 0;
  if (as_Info->cas_log_reset)
    {
      cas_sql_log_reset ();
    }
  if (as_Info->cas_slow_log_reset)
    {
      cas_slow_log_reset ();
    }

  if (!ux_is_database_connected ())
    {
      er_log_debug (ARG_FILE_LINE, "fn_end_tran: !ux_is_database_connected()");
      return FN_CLOSE_CONN;
    }
  else if (restart_is_needed () || as_Info->reset_flag == TRUE)
    {
      er_log_debug (ARG_FILE_LINE, "fn_end_tran: restart_is_needed() || reset_flag");
      return FN_KEEP_SESS;
    }
  return FN_KEEP_CONN;
}

FN_RETURN
fn_prepare (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  return (fn_prepare_internal (argc, argv, net_buf, req_info));
}


static FN_RETURN
fn_prepare_internal (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  char *sql_stmt;
  char flag;
  char auto_commit_mode;
  int sql_size;
  int srv_h_id;
  T_SRV_HANDLE *srv_handle;
  int i;
  int num_deferred_close_handle;
  int arg_idx;

  if (argc < 4)
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  arg_idx = 0;
  net_arg_get_str (&sql_stmt, &sql_size, argv[arg_idx++]);
  net_arg_get_char (&flag, argv[arg_idx++]);
  net_arg_get_char (&auto_commit_mode, argv[arg_idx++]);
  net_arg_get_int (&num_deferred_close_handle, argv[arg_idx++]);
  if (num_deferred_close_handle > argc - arg_idx)
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  for (i = 0; i < num_deferred_close_handle; i++)
    {
      int deferred_close_handle;
      net_arg_get_int (&deferred_close_handle, argv[arg_idx++]);
      cas_sql_log_write (0, "close_req_handle srv_h_id %d", deferred_close_handle);
      hm_srv_handle_free (deferred_close_handle);
    }

  gettimeofday (&query_Start_time, NULL);

  cas_sql_log_write_nonl (query_seq_num_next_value (), "prepare %d ", flag);
  cas_log_write_string (CAS_LOG_SQL_LOG, sql_stmt, sql_size - 1, true);

  /* append query string to as_Info->log_msg */
  if (sql_stmt)
    {
      char *s, *t;
      size_t l;

      for (s = as_Info->log_msg, l = 0; *s && l < SHM_LOG_MSG_SIZE - 1; s++, l++)
        {
          /* empty body */
        }
      *s++ = ' ';
      l++;
      for (t = sql_stmt; *t && l < SHM_LOG_MSG_SIZE - 1; s++, t++, l++)
        {
          *s = *t;
        }
      *s = '\0';
    }

  srv_h_id = ux_prepare (sql_stmt, flag, auto_commit_mode, net_buf, req_info, query_seq_num_current_value ());

  srv_handle = hm_find_srv_handle (srv_h_id);

  cas_sql_log_write (query_seq_num_current_value (),
                     "prepare srv_h_id %s%d%s%s",
                     (srv_h_id < 0) ? "error:" : "",
                     (srv_h_id < 0) ? err_Info.err_number : srv_h_id,
                     (srv_handle != NULL
                      && srv_handle->use_plan_cache) ? " (PC)" : "", get_error_log_eids (err_Info.err_number));

  if (srv_h_id < 0)
    {
      update_error_query_count (as_Info, &err_Info);
    }

  return FN_KEEP_CONN;
}

FN_RETURN
fn_execute (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  FN_RETURN ret = fn_execute_internal (argc, argv, net_buf, req_info, NULL);

  return ret;
}

static void
cas_execute_log_before_execute (T_CAS_LOG_TYPE cas_log_type,
                                struct timeval *start_time,
                                T_SRV_HANDLE * srv_handle, int srv_h_id,
                                int num_bind_value, void **bind_argv, int query_timeout, const char *query_timeout_from)
{
  const char *exec_func_name = "execute";

  cas_log_write (cas_log_type,
                 CAS_LOG_FLAG_PRINT_HEADER | CAS_LOG_FLAG_PRINT_NL,
                 start_time, SRV_HANDLE_QUERY_SEQ_NUM (srv_handle),
                 "set query timeout to %d ms (%s)", query_timeout, query_timeout_from);

  cas_log_write (cas_log_type, CAS_LOG_FLAG_PRINT_HEADER, start_time,
                 SRV_HANDLE_QUERY_SEQ_NUM (srv_handle), "%s srv_h_id %d ", exec_func_name, srv_h_id);
  if (srv_handle->sql_stmt != NULL)
    {
      cas_log_write_string (cas_log_type, srv_handle->sql_stmt, strlen (srv_handle->sql_stmt), true);
      bind_value_log (cas_log_type, start_time, num_bind_value, bind_argv, SRV_HANDLE_QUERY_SEQ_NUM (srv_handle));
    }
}

static void
cas_execute_log_after_execute (T_CAS_LOG_TYPE cas_log_type,
                               T_SRV_HANDLE * srv_handle, int ret_code,
                               int elapsed_sec, int elapsed_msec,
                               int err_number_execute, const char *eid_string, const char *plan)
{
  const char *exec_func_name = "execute";
  cas_log_write (cas_log_type,
                 CAS_LOG_FLAG_PRINT_HEADER | CAS_LOG_FLAG_PRINT_NL,
                 NULL, SRV_HANDLE_QUERY_SEQ_NUM (srv_handle),
                 "%s %s%d tuple %d time %d.%03d%s%s%s",
                 exec_func_name, (ret_code < 0) ? "error:" : "",
                 err_number_execute,
                 (ret_code < 0 ? ret_code : get_tuple_count (srv_handle)),
                 elapsed_sec, elapsed_msec, "", "", eid_string);

  if (plan != NULL && plan[0] != '\0')
    {
      cas_log_write (cas_log_type,
                     CAS_LOG_FLAG_PRINT_HEADER | CAS_LOG_FLAG_PRINT_NL, NULL, 0, "slow query plan\n%s", plan);
    }
}

static FN_RETURN
fn_execute_internal (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info, int *prepared_srv_h_id)
{
  int srv_h_id;
  char flag;
  int max_col_size;
  int max_row = 0;
  int ret_code;
  char auto_commit_mode = 0;
  T_SRV_HANDLE *srv_handle;
  int elapsed_sec = 0, elapsed_msec = 0;
  struct timeval exec_begin, exec_end;
  int app_query_timeout;
  const char *eid_string;
  int err_number_execute;
  int arg_idx = 0;
  char *plan;
  int num_bind_value;
  void **bind_argv;
  int group_id;
  const char *query_timeout_from;
  int timeout;

  if (argc < 8)
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  net_arg_get_int (&srv_h_id, argv[arg_idx++]);
  net_arg_get_char (&flag, argv[arg_idx++]);
  net_arg_get_int (&max_col_size, argv[arg_idx++]);
  net_arg_get_int (&max_row, argv[arg_idx++]);
  net_arg_get_char (&auto_commit_mode, argv[arg_idx++]);
  net_arg_get_int (&app_query_timeout, argv[arg_idx++]);
  net_arg_get_int (&group_id, argv[arg_idx++]);
  net_arg_get_int (&num_bind_value, argv[arg_idx++]);

  if (num_bind_value > (argc - arg_idx) / 2)
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  bind_argv = argv + arg_idx;
  arg_idx += (num_bind_value * 2);

  if (prepared_srv_h_id != NULL)
    {
      srv_h_id = *prepared_srv_h_id;
    }

  srv_handle = hm_find_srv_handle (srv_h_id);

  if (srv_handle == NULL || srv_handle->schema_type >= CCI_SCH_FIRST)
    {
      ERROR_INFO_SET (CAS_ER_SRV_HANDLE, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  if (shm_Appl->max_string_length >= 0)
    {
      if (max_col_size <= 0 || max_col_size > shm_Appl->max_string_length)
        max_col_size = shm_Appl->max_string_length;
    }

  app_query_timeout = set_query_timeout (app_query_timeout, &query_timeout_from);

  srv_handle->auto_commit_mode = auto_commit_mode;
  srv_handle->scrollable_cursor = FALSE;

  if (srv_handle->is_pooled)
    {
      gettimeofday (&query_Start_time, NULL);
    }

  if (auto_commit_mode == FALSE || as_Info->cur_sql_log_mode == SQL_LOG_MODE_ALL)
    {
      cas_execute_log_before_execute (CAS_LOG_SQL_LOG, NULL,
                                      srv_handle, srv_h_id, num_bind_value,
                                      bind_argv, app_query_timeout, query_timeout_from);
    }

  /* append query string to as_Info->log_msg */
  if (srv_handle->sql_stmt)
    {
      char *s, *t;
      size_t l;

      for (s = as_Info->log_msg, l = 0; *s && l < SHM_LOG_MSG_SIZE - 1; s++, l++)
        {
          /* empty body */
        }
      *s++ = ' ';
      l++;
      for (t = srv_handle->sql_stmt; *t && l < SHM_LOG_MSG_SIZE - 1; s++, t++, l++)
        {
          *s = *t;
        }
      *s = '\0';
    }

  gettimeofday (&exec_begin, NULL);

  ret_code = ux_execute (srv_handle, flag, max_col_size, max_row,
                         num_bind_value, bind_argv, group_id, net_buf, req_info);
  gettimeofday (&exec_end, NULL);
  ut_timeval_diff (&exec_begin, &exec_end, &elapsed_sec, &elapsed_msec);
  eid_string = get_error_log_eids (err_Info.err_number);
  err_number_execute = err_Info.err_number;

  if (ux_has_stmt_result_set (srv_handle->q_result->stmt_type) && ret_code >= 0)
    {
      net_buf_cp_byte (net_buf, EXEC_CONTAIN_FETCH_RESULT);
      ux_fetch (srv_handle, 1, 50, 0, net_buf, req_info);
    }
  else
    {
      net_buf_cp_byte (net_buf, EXEC_NOT_CONTAIN_FETCH_RESULT);
    }

  plan = db_get_execution_plan ();

  if (auto_commit_mode == FALSE || as_Info->cur_sql_log_mode == SQL_LOG_MODE_ALL)
    {
      cas_execute_log_after_execute (CAS_LOG_SQL_LOG, srv_handle, ret_code,
                                     elapsed_sec, elapsed_msec, err_number_execute, eid_string, plan);
    }

  timeout = ut_check_timeout (&query_Start_time, &exec_end, shm_Appl->long_query_time, &elapsed_sec, &elapsed_msec);
  if (timeout >= 0 || ret_code < 0)
    {

      sql_log_Notice_mode_flush = true;

      if (timeout >= 0)
        {
          as_Info->num_long_queries %= MAX_DIAG_DATA_VALUE;
          as_Info->num_long_queries++;
        }

      if (ret_code < 0)
        {
          update_error_query_count (as_Info, &err_Info);
        }

      if (as_Info->cur_slow_log_mode == SLOW_LOG_MODE_ON)
        {
          cas_execute_log_before_execute (CAS_LOG_SLOW_LOG, &query_Start_time,
                                          srv_handle, srv_h_id,
                                          num_bind_value, bind_argv, app_query_timeout, query_timeout_from);
          cas_execute_log_after_execute (CAS_LOG_SLOW_LOG, srv_handle,
                                         ret_code, elapsed_sec, elapsed_msec, err_number_execute, eid_string, plan);
          cas_slow_log_end ();
        }

      if (auto_commit_mode == TRUE && as_Info->cur_sql_log_mode != SQL_LOG_MODE_ALL)
        {
          cas_execute_log_before_execute (CAS_LOG_SQL_LOG, &query_Start_time,
                                          srv_handle, srv_h_id,
                                          num_bind_value, bind_argv, app_query_timeout, query_timeout_from);
          cas_execute_log_after_execute (CAS_LOG_SQL_LOG, srv_handle,
                                         ret_code, elapsed_sec, elapsed_msec, err_number_execute, eid_string, plan);
        }

    }

  if (plan != NULL && plan[0] != '\0')
    {
      /* reset global plan buffer */
      db_set_execution_plan (NULL, 0);
    }

  /* set is_pooled */
  if (as_Info->cur_statement_pooling)
    {
      srv_handle->is_pooled = TRUE;
    }

  return FN_KEEP_CONN;
}

FN_RETURN
fn_get_db_parameter (int argc, void **argv, T_NET_BUF * net_buf, UNUSED_ARG T_REQ_INFO * req_info)
{
  int param_name;

  if (argc < 1)
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  net_arg_get_int (&param_name, argv[0]);

  if (param_name == CCI_PARAM_MAX_STRING_LENGTH)
    {
      int max_str_len = shm_Appl->max_string_length;

      if (max_str_len <= 0 || max_str_len > DB_MAX_STRING_LENGTH)
        max_str_len = DB_MAX_STRING_LENGTH;
      net_buf_cp_int (net_buf, max_str_len, NULL);
    }
  else
    {
      ERROR_INFO_SET (CAS_ER_PARAM_NAME, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  return FN_KEEP_CONN;
}

FN_RETURN
fn_close_req_handle (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  int srv_h_id;
  T_SRV_HANDLE *srv_handle;
  char auto_commit_mode = FALSE;

  if (argc < 2)
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  net_arg_get_int (&srv_h_id, argv[0]);
  net_arg_get_char (&auto_commit_mode, argv[1]);

  srv_handle = hm_find_srv_handle (srv_h_id);
  if (auto_commit_mode == TRUE)
    {
      req_info->need_auto_commit = TRAN_AUTOCOMMIT;
    }

  cas_sql_log_write (SRV_HANDLE_QUERY_SEQ_NUM (srv_handle), "close_req_handle srv_h_id %d", srv_h_id);

  hm_srv_handle_free (srv_h_id);

  return FN_KEEP_CONN;
}

FN_RETURN
fn_fetch (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  int srv_h_id;
  int cursor_pos;
  int fetch_count;
  int result_set_index;
  T_SRV_HANDLE *srv_handle;

  if (argc < 4)
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  net_arg_get_int (&srv_h_id, argv[0]);
  net_arg_get_int (&cursor_pos, argv[1]);
  net_arg_get_int (&fetch_count, argv[2]);
  net_arg_get_int (&result_set_index, argv[3]);

  srv_handle = hm_find_srv_handle (srv_h_id);

  if (srv_handle == NULL)
    {
      ERROR_INFO_SET (CAS_ER_SRV_HANDLE, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  cas_sql_log_write (SRV_HANDLE_QUERY_SEQ_NUM (srv_handle),
                     "fetch srv_h_id %d cursor_pos %d fetch_count %d", srv_h_id, cursor_pos, fetch_count);

  ux_fetch (srv_handle, cursor_pos, fetch_count, result_set_index, net_buf, req_info);

  return FN_KEEP_CONN;
}

FN_RETURN
fn_schema_info (int argc, void **argv, T_NET_BUF * net_buf, UNUSED_ARG T_REQ_INFO * req_info)
{
  int schema_type;
  char *arg1, *arg2;
  int flag;
  int arg1_size, arg2_size;
  int srv_h_id;

  if (argc < 4)
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  net_arg_get_int (&schema_type, argv[0]);
  net_arg_get_str (&arg1, &arg1_size, argv[1]);
  net_arg_get_str (&arg2, &arg2_size, argv[2]);
  net_arg_get_int (&flag, argv[3]);

  cas_sql_log_write (query_seq_num_next_value (),
                     "schema_info %s %s %s %d",
                     schema_info_str (schema_type), (arg1 ? arg1 : "NULL"), (arg2 ? arg2 : "NULL"), flag);

  srv_h_id = ux_schema_info (schema_type, arg1, arg2, net_buf, query_seq_num_current_value ());

  cas_sql_log_write (query_seq_num_current_value (), "schema_info srv_h_id %d", srv_h_id);

  return FN_KEEP_CONN;
}

FN_RETURN
fn_get_db_version (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  char auto_commit_mode;
  cas_sql_log_write (0, "get_version");

  if (argc < 1)
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  net_arg_get_char (&auto_commit_mode, argv[0]);

  ux_get_db_version (net_buf);

  if (auto_commit_mode == TRUE)
    {
      req_info->need_auto_commit = TRAN_AUTOCOMMIT;
    }

  return FN_KEEP_CONN;
}

FN_RETURN
fn_get_class_num_objs (UNUSED_ARG int argc, UNUSED_ARG void **argv,
                       T_NET_BUF * net_buf, UNUSED_ARG T_REQ_INFO * req_info)
{
  ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
  NET_BUF_ERR_SET (net_buf);
  return FN_KEEP_CONN;
}

FN_RETURN
fn_execute_batch (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  int srv_h_id;
  T_SRV_HANDLE *srv_handle;
  int ret_code;
  int elapsed_sec = 0, elapsed_msec = 0;
  struct timeval exec_begin, exec_end;
  const char *eid_string;
  char *plan;
  int driver_query_timeout;
  int arg_idx = 0;
  char auto_commit_mode;
  int num_bind_value;
  void **bind_argv;
  int num_execution;
  int num_markers;
  int group_id;
  const char *query_timeout_from;
  int timeout;

  if (argc < 6)
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  net_arg_get_int (&srv_h_id, argv[arg_idx++]);
  net_arg_get_int (&driver_query_timeout, argv[arg_idx++]);
  net_arg_get_char (&auto_commit_mode, argv[arg_idx++]);
  net_arg_get_int (&group_id, argv[arg_idx++]);
  net_arg_get_int (&num_execution, argv[arg_idx++]);
  net_arg_get_int (&num_markers, argv[arg_idx++]);
  net_arg_get_int (&num_bind_value, argv[arg_idx++]);

  if ((num_bind_value > (argc - arg_idx) / 2) ||
      (num_markers > 0 && num_execution * num_markers != num_bind_value) || (num_markers <= 0 && num_bind_value > 0))
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  bind_argv = &argv[arg_idx];
  arg_idx += (num_bind_value * 2);

  srv_handle = hm_find_srv_handle (srv_h_id);
  if (srv_handle == NULL)
    {
      ERROR_INFO_SET (CAS_ER_SRV_HANDLE, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  if (srv_handle->is_pooled)
    {
      gettimeofday (&query_Start_time, NULL);
    }

  /* does not support query timeout for execute_batch yet */
  driver_query_timeout = set_query_timeout (driver_query_timeout, &query_timeout_from);

  if (auto_commit_mode == TRUE)
    {
      req_info->need_auto_commit = TRAN_AUTOCOMMIT;
    }
  srv_handle->auto_commit_mode = auto_commit_mode;

  cas_sql_log_write (SRV_HANDLE_QUERY_SEQ_NUM (srv_handle),
                     "set query timeout to %d ms (%s)", driver_query_timeout, query_timeout_from);
  cas_sql_log_write_nonl (SRV_HANDLE_QUERY_SEQ_NUM (srv_handle),
                          "execute_batch srv_h_id %d %d %d ", srv_h_id, num_execution, num_bind_value);
  if (srv_handle->sql_stmt != NULL)
    {
      cas_log_write_string (CAS_LOG_SQL_LOG, srv_handle->sql_stmt, strlen (srv_handle->sql_stmt), true);
    }
  if (as_Info->cur_sql_log_mode != SQL_LOG_MODE_NONE)
    {
      bind_value_log (CAS_LOG_SQL_LOG, NULL, num_bind_value, bind_argv, SRV_HANDLE_QUERY_SEQ_NUM (srv_handle));
    }

  gettimeofday (&exec_begin, NULL);

  ret_code = ux_execute_batch (srv_handle, num_execution, num_markers, group_id, bind_argv, net_buf);

  gettimeofday (&exec_end, NULL);
  ut_timeval_diff (&exec_begin, &exec_end, &elapsed_sec, &elapsed_msec);

  eid_string = get_error_log_eids (err_Info.err_number);
  cas_sql_log_write (SRV_HANDLE_QUERY_SEQ_NUM (srv_handle),
                     "execute_batch %s%d tuple %d time %d.%03d%s%s%s",
                     (ret_code < 0) ? "error:" : "",
                     err_Info.err_number, get_tuple_count (srv_handle), elapsed_sec, elapsed_msec, "", "", eid_string);

  timeout = ut_check_timeout (&query_Start_time, &exec_end, shm_Appl->long_query_time, &elapsed_sec, &elapsed_msec);

  if (timeout >= 0 || ret_code < 0)
    {
      sql_log_Notice_mode_flush = true;

      if (timeout >= 0)
        {
          as_Info->num_long_queries %= MAX_DIAG_DATA_VALUE;
          as_Info->num_long_queries++;
        }

      if (ret_code < 0)
        {
          update_error_query_count (as_Info, &err_Info);
        }

      if (as_Info->cur_slow_log_mode == SLOW_LOG_MODE_ON)
        {
          cas_slow_log_write_nonl (&query_Start_time,
                                   SRV_HANDLE_QUERY_SEQ_NUM (srv_handle),
                                   "execute_batch srv_h_id %d %d ", srv_h_id, num_bind_value);
          if (srv_handle->sql_stmt != NULL)
            {
              cas_log_write_string (CAS_LOG_SLOW_LOG, srv_handle->sql_stmt, strlen (srv_handle->sql_stmt), true);
              bind_value_log (CAS_LOG_SQL_LOG, &query_Start_time,
                              num_bind_value, bind_argv, SRV_HANDLE_QUERY_SEQ_NUM (srv_handle));
            }
          cas_slow_log_write (NULL, SRV_HANDLE_QUERY_SEQ_NUM (srv_handle),
                              "execute_batch %s%d tuple %d time %d.%03d%s%s%s",
                              (ret_code < 0) ? "error:" : "",
                              err_Info.err_number,
                              get_tuple_count (srv_handle), elapsed_sec, elapsed_msec, "", "", eid_string);

          plan = db_get_execution_plan ();

          if (plan != NULL && plan[0] != '\0')
            {
              cas_slow_log_write (NULL, 0, "slow query plan\n%s", plan);

              /* reset global plan buffer */
              db_set_execution_plan (NULL, 0);
            }
          cas_slow_log_end ();
        }
    }

  return FN_KEEP_CONN;
}

FN_RETURN
fn_cursor_close (UNUSED_ARG int argc, void **argv, UNUSED_ARG T_NET_BUF * net_buf, UNUSED_ARG T_REQ_INFO * req_info)
{
  int srv_h_id;
  T_SRV_HANDLE *srv_handle;

  net_arg_get_int (&srv_h_id, argv[0]);

  srv_handle = hm_find_srv_handle (srv_h_id);
  if (srv_handle == NULL)
    {
      /* has already been closed */
      return FN_KEEP_CONN;
    }

  cas_sql_log_write (SRV_HANDLE_QUERY_SEQ_NUM (srv_handle), "cursor_close srv_h_id %d", srv_h_id);

  ux_cursor_close (srv_handle);

  return FN_KEEP_CONN;
}

FN_RETURN
fn_get_query_plan (int argc, void **argv, T_NET_BUF * net_buf, UNUSED_ARG T_REQ_INFO * req_info)
{
  int size, err;
  char *sql_stmt = NULL;
  DB_SESSION *session;
  char plan_dump_filename[BROKER_PATH_MAX];
  char filename[BROKER_PATH_MAX];

  if (argc < 1)
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_CONN;
    }

  net_arg_get_str (&sql_stmt, &size, argv[0]);
  if (sql_stmt == NULL)
    {
      ux_get_query_plan (NULL, net_buf);
      return FN_KEEP_CONN;
    }

  snprintf (filename, sizeof (filename), "%d.plan", (int) getpid ());
  envvar_tmpdir_file (plan_dump_filename, sizeof (plan_dump_filename), filename);
  unlink (plan_dump_filename);

  db_query_plan_dump_file (plan_dump_filename);

  set_optimization_level (514);

  session = db_open_buffer (sql_stmt);
  if (!session)
    {
      ERROR_INFO_SET (db_error_code (), DBMS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      goto end;
    }

  err = db_compile_statement (session);
  if (err != NO_ERROR)
    {
      ERROR_INFO_SET (err, DBMS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      db_close_session (session);
      goto end;
    }

  db_close_session (session);

  ux_get_query_plan (plan_dump_filename, net_buf);

  cas_sql_log_write (0, "get_query_plan %s", sql_stmt);

end:
  reset_optimization_level_as_saved ();
  unlink (plan_dump_filename);
  db_query_plan_dump_file (NULL);

  return FN_KEEP_CONN;
}

FN_RETURN
fn_con_close (UNUSED_ARG int argc, UNUSED_ARG void **argv,
              UNUSED_ARG T_NET_BUF * net_buf, UNUSED_ARG T_REQ_INFO * req_info)
{
  cas_sql_log_write (0, "con_close");
  return FN_CLOSE_CONN;
}

FN_RETURN
fn_check_cas (int argc, void **argv, T_NET_BUF * net_buf, UNUSED_ARG T_REQ_INFO * req_info)
{
  int retcode = 0;
  char *msg = NULL;

  if (argc > 0)
    {
      int msg_size;
      net_arg_get_str (&msg, &msg_size, argv[0]);
    }

  retcode = ux_check_connection ();

  cas_sql_log_write (0, "check_cas %d %s", retcode, (msg == NULL ? "" : msg));

  if (retcode < 0)
    {
      ERROR_INFO_SET (retcode, CAS_ERROR_INDICATOR);
      NET_BUF_ERR_SET (net_buf);
      return FN_KEEP_SESS;
    }
  else
    {
      return FN_KEEP_CONN;
    }
}

FN_RETURN
fn_change_dbuser (int argc, void **argv, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  char *user = NULL;
  char *passwd;
  int size;

  if (argc < 2)
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      goto change_dbuser_error;
    }

  net_arg_get_str (&user, &size, argv[0]);
  net_arg_get_str (&passwd, &size, argv[1]);

  if (user == NULL)
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      goto change_dbuser_error;
    }

  if (ux_change_dbuser (user, passwd) < 0)
    {
      goto change_dbuser_error;
    }

  net_buf_cp_int (net_buf, db_get_server_start_time (), NULL);

  req_info->need_auto_commit = TRAN_AUTOCOMMIT;

  cas_sql_log_write (0, "change_dbuser %s", user);

  return FN_KEEP_CONN;

change_dbuser_error:
  NET_BUF_ERR_SET (net_buf);
  cas_sql_log_write (0, "change_dbuser %s error:%d", (user == NULL ? "NULL" : user), err_Info.err_number);
  return FN_CLOSE_CONN;
}

static const char *
get_tran_type_str (int tran_type)
{
  if (tran_type < 1 || tran_type > 2)
    {
      return "";
    }

  return (tran_type_str[tran_type - 1]);
}

static void
bind_value_log (T_CAS_LOG_TYPE cas_log_type, struct timeval *log_time,
                int num_bind_value, void **bind_argv, unsigned int query_seq_num)
{
  char type;
  void *net_value;
  int i;

  for (i = 0; i < num_bind_value; i++)
    {
      net_arg_get_char (&type, bind_argv[i * 2]);
      net_value = bind_argv[i * 2 + 1];

      if (cas_log_type == CAS_LOG_SLOW_LOG)
        {
          cas_slow_log_write_nonl (log_time, query_seq_num, "bind %d : ", i + 1);
        }
      else
        {
          cas_sql_log_write_nonl (query_seq_num, "bind %d : ", i + 1);
        }

      if (type > CCI_TYPE_FIRST && type <= CCI_TYPE_LAST)
        {
          cas_log_write2 (cas_log_type, "%s ", type_str_tbl[(int) type]);
          bind_value_print (cas_log_type, type, net_value);
        }
      else
        {
          cas_log_write2 (cas_log_type, "NULL");
        }
      cas_log_write2 (cas_log_type, "\n");
    }
}

static void
bind_value_print (T_CAS_LOG_TYPE cas_log_type, char type, void *net_value)
{
  int data_size;

  net_arg_get_size (&data_size, net_value);
  if (data_size <= 0)
    {
      type = CCI_TYPE_NULL;
      data_size = 0;
    }

  switch (type)
    {
    case CCI_TYPE_VARCHAR:
    case CCI_TYPE_VARBIT:
    case CCI_TYPE_NUMERIC:
      {
        char *str_val;
        int val_size;
        net_arg_get_str (&str_val, &val_size, net_value);
        val_size--;
        if (type != CCI_TYPE_NUMERIC)
          {
            cas_log_write2 (cas_log_type, "(%d)", val_size);
          }

        if (as_Info->cur_sql_log_mode != SQL_LOG_MODE_ALL)
          {
            val_size = MIN (val_size, 100);
          }
        cas_log_write_string (cas_log_type, str_val, val_size, false);
      }
      break;
    case CCI_TYPE_BIGINT:
      {
        INT64 bi_val;
        net_arg_get_bigint (&bi_val, net_value);
        cas_log_write2 (cas_log_type, "%lld", (long long) bi_val);
      }
      break;
    case CCI_TYPE_INT:
      {
        int i_val;
        net_arg_get_int (&i_val, net_value);
        cas_log_write2 (cas_log_type, "%d", i_val);
      }
      break;
    case CCI_TYPE_DOUBLE:
      {
        double d_val;
        net_arg_get_double (&d_val, net_value);
        cas_log_write2 (cas_log_type, "%.15e", d_val);
      }
      break;
    case CCI_TYPE_DATE:
    case CCI_TYPE_TIME:
    case CCI_TYPE_DATETIME:
      {
        short yr, mon, day, hh, mm, ss, ms;
        net_arg_get_datetime (&yr, &mon, &day, &hh, &mm, &ss, &ms, net_value);
        if (type == CCI_TYPE_DATE)
          cas_log_write2 (cas_log_type, "%d-%d-%d", yr, mon, day);
        else if (type == CCI_TYPE_TIME)
          cas_log_write2 (cas_log_type, "%d:%d:%d", hh, mm, ss);
        else
          cas_log_write2 (cas_log_type, "%d-%d-%d %d:%d:%d.%03d", yr, mon, day, hh, mm, ss, ms);
      }
      break;

    default:
      cas_log_write2 (cas_log_type, "NULL");
      break;
    }
}

/*
 * get_error_log_eids - get error identifier string
 *    return: pointer to internal buffer
 * NOTE:
 * this function is not MT safe. Rreturned address is guaranteed to be valid
 * until next get_error_log_eids() call.
 *
 */
static const char *
get_error_log_eids (int err)
{
  static char buffer[512];

  buffer[0] = '\0';

  if (err >= 0)
    {
      return "";
    }

  cas_log_error_handler_asprint (buffer, sizeof (buffer), true);

  return buffer;
}

int
set_query_timeout (int query_timeout, const char **from)
{
  int broker_timeout_in_millis = shm_Appl->query_timeout * 1000;

  *from = "";

  if (query_timeout == 0 || broker_timeout_in_millis == 0)
    {
      tran_set_query_timeout (query_timeout + broker_timeout_in_millis);

      if (query_timeout == 0 && broker_timeout_in_millis == 0)
        {
          *from = "no limit";
          return 0;
        }
      else
        {
          *from = (query_timeout > 0 ? "from app" : "from broker");
          return (query_timeout + broker_timeout_in_millis);
        }
    }
  else if (query_timeout > broker_timeout_in_millis)
    {
      tran_set_query_timeout (broker_timeout_in_millis);
      *from = "from broker";
      return broker_timeout_in_millis;
    }
  else
    {
      tran_set_query_timeout (query_timeout);
      *from = "from app";
      return query_timeout;
    }
}

static void
update_error_query_count (T_APPL_SERVER_INFO * as_info_p, const T_ERROR_INFO * err_info_p)
{
  assert (as_info_p != NULL);
  assert (err_info_p != NULL);

  if (err_info_p->err_number != ER_QPROC_INVALID_XASLNODE)
    {
      as_info_p->num_error_queries %= MAX_DIAG_DATA_VALUE;
      as_info_p->num_error_queries++;
    }

  if (err_info_p->err_indicator == DBMS_ERROR_INDICATOR)
    {
      if (err_info_p->err_number == ER_BTREE_UNIQUE_FAILED)
        {
          as_info_p->num_unique_error_queries %= MAX_DIAG_DATA_VALUE;
          as_info_p->num_unique_error_queries++;
        }
    }
}

FN_RETURN
fn_update_group_id (int argc, void **argv, T_NET_BUF * net_buf, UNUSED_ARG T_REQ_INFO * req_info)
{
  int migrator_id, group_id = -1, target, on_off;
  int error;

  if (argc < 2)
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      goto error_exit;
    }

  net_arg_get_int (&migrator_id, argv[0]);
  net_arg_get_int (&group_id, argv[1]);
  net_arg_get_int (&target, argv[2]);
  net_arg_get_int (&on_off, argv[3]);

  error = ux_update_group_id (migrator_id, group_id, target, on_off);
  if (error < 0)
    {
      ERROR_INFO_SET (error, DBMS_ERROR_INDICATOR);
      goto error_exit;
    }

  net_buf_cp_int (net_buf, 0, NULL);

  cas_sql_log_write (0,
                     "update_group_id - migrator_id: %d, group_id: %d,"
                     "target: %d, on_off: %d", migrator_id, group_id, target, on_off);

  return FN_KEEP_CONN;

error_exit:
  NET_BUF_ERR_SET (net_buf);
  cas_sql_log_write (0, "update_group_id %d error:%d", group_id, err_Info.err_number);
  return FN_KEEP_CONN;
}

FN_RETURN
fn_insert_gid_removed_info (int argc, void **argv, T_NET_BUF * net_buf, UNUSED_ARG T_REQ_INFO * req_info)
{
  int group_id = 0;
  int error;

  if (argc != 1)
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      goto error_exit;
    }

  net_arg_get_int (&group_id, argv[0]);

  error = ux_insert_gid_removed_info (group_id);
  if (error < 0)
    {
      ERROR_INFO_SET (error, DBMS_ERROR_INDICATOR);
      goto error_exit;
    }

  net_buf_cp_int (net_buf, 0, NULL);

  cas_sql_log_write (0, "insert_gid_removed_info %d", group_id);

  return FN_KEEP_CONN;

error_exit:
  NET_BUF_ERR_SET (net_buf);
  cas_sql_log_write (0, "insert_gid_removed_info %d error:%d", group_id, err_Info.err_number);
  return FN_KEEP_CONN;
}

FN_RETURN
fn_delete_gid_removed_info (int argc, void **argv, T_NET_BUF * net_buf, UNUSED_ARG T_REQ_INFO * req_info)
{
  int group_id = 0;
  int error;

  if (argc != 1)
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      goto error_exit;
    }

  net_arg_get_int (&group_id, argv[0]);

  error = ux_delete_gid_removed_info (group_id);
  if (error < 0)
    {
      ERROR_INFO_SET (error, DBMS_ERROR_INDICATOR);
      goto error_exit;
    }

  net_buf_cp_int (net_buf, 0, NULL);

  cas_sql_log_write (0, "delete_gid_removed_info %d", group_id);

  return FN_KEEP_CONN;

error_exit:
  NET_BUF_ERR_SET (net_buf);
  cas_sql_log_write (0, "delete_gid_removed_info %d error:%d", group_id, err_Info.err_number);
  return FN_KEEP_CONN;
}

FN_RETURN
fn_delete_gid_skey_info (int argc, void **argv, T_NET_BUF * net_buf, UNUSED_ARG T_REQ_INFO * req_info)
{
  int group_id = 0;
  int error;

  if (argc != 1)
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      goto error_exit;
    }

  net_arg_get_int (&group_id, argv[0]);

  error = ux_delete_gid_skey_info (group_id);
  if (error < 0)
    {
      ERROR_INFO_SET (error, DBMS_ERROR_INDICATOR);
      goto error_exit;
    }

  net_buf_cp_int (net_buf, 0, NULL);

  cas_sql_log_write (0, "delete_gid_skey_info %d", group_id);

  return FN_KEEP_CONN;

error_exit:
  NET_BUF_ERR_SET (net_buf);
  cas_sql_log_write (0, "delete_gid_skey_info %d error:%d", group_id, err_Info.err_number);
  return FN_KEEP_CONN;
}

FN_RETURN
fn_block_globl_dml (int argc, void **argv, T_NET_BUF * net_buf, UNUSED_ARG T_REQ_INFO * req_info)
{
  int start_or_end = 0;
  int error;

  if (argc != 1)
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      goto error_exit;
    }

  net_arg_get_int (&start_or_end, argv[0]);

  error = ux_block_globl_dml (start_or_end);
  if (error < 0)
    {
      ERROR_INFO_SET (error, DBMS_ERROR_INDICATOR);
      goto error_exit;
    }

  net_buf_cp_int (net_buf, 0, NULL);

  cas_sql_log_write (0, "block_globl_dml %d", start_or_end);

  return FN_KEEP_CONN;

error_exit:
  NET_BUF_ERR_SET (net_buf);
  cas_sql_log_write (0, "block_globl_dml %d error:%d", start_or_end, err_Info.err_number);
  return FN_KEEP_CONN;
}

FN_RETURN
fn_server_mode (UNUSED_ARG int argc, UNUSED_ARG void **argv, T_NET_BUF * net_buf, UNUSED_ARG T_REQ_INFO * req_info)
{
  int server_state;
  int server_addr;

  server_state = db_get_server_state ();
  server_addr = db_get_server_addr ();

  net_buf_cp_int (net_buf, server_state, NULL);
  net_buf_cp_int (net_buf, server_addr, NULL);

  cas_sql_log_write (0, "server_state %d", server_state);
  return FN_KEEP_CONN;
}

FN_RETURN
fn_send_repl_data (int argc, void **argv, T_NET_BUF * net_buf, UNUSED_ARG T_REQ_INFO * req_info)
{
#define REPL_DATA_NUM_ARGS 5

  int num_items;
  RP_TRAN_TYPE tran_type = -1;  /* TODO - */
  int tmp_int;
  int applier_id, tran_id;
  int arg_idx = 0;
  void **obj_argv;
  int error = 0;
  int autocommit_mode = FALSE;
  struct timeval start_time;
#if !defined(NDEBUG)
  char buffer[ONE_K];
#endif

  gettimeofday (&start_time, NULL);

  if (argc < REPL_DATA_NUM_ARGS)
    {
      assert (false);
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      goto error_exit;
    }

  net_arg_get_int (&applier_id, argv[arg_idx++]);
  net_arg_get_int (&tran_id, argv[arg_idx++]);
  net_arg_get_int (&autocommit_mode, argv[arg_idx++]);
  net_arg_get_int (&tmp_int, argv[arg_idx++]);
  tran_type = (RP_TRAN_TYPE) tmp_int;
  net_arg_get_int (&num_items, argv[arg_idx++]);

  if (autocommit_mode == FALSE || as_Info->cur_sql_log_mode == SQL_LOG_MODE_ALL)
    {
      cas_sql_log_write_with_ts (&start_time, 0,
                                 "send_repl_data(%d):tran_id(%d), tran_type(%s), num items(%d)",
                                 applier_id, tran_id, RP_TRAN_TYPE_NAME (tran_type), num_items);
    }

  obj_argv = argv + arg_idx;
  error = ux_send_repl_data (tran_type, num_items, obj_argv);
  if (error < 0)
    {
      ERROR_INFO_SET (error, DBMS_ERROR_INDICATOR);
      goto error_exit;
    }
  if (autocommit_mode == TRUE)
    {
      req_info->need_auto_commit = TRAN_AUTOCOMMIT;
    }

  if (autocommit_mode == FALSE || as_Info->cur_sql_log_mode == SQL_LOG_MODE_ALL)
    {
      cas_sql_log_write (0, "send_repl_data success");
#if !defined(NDEBUG)
      dump_repl_data (buffer, sizeof (buffer), num_items, obj_argv);
      cas_sql_log_write (0, buffer);
#endif
    }

  return FN_KEEP_CONN;

error_exit:
  if (autocommit_mode == TRUE)
    {
      req_info->need_auto_commit = TRAN_AUTOCOMMIT;
    }

  update_error_query_count (as_Info, &err_Info);

  sql_log_Notice_mode_flush = true;

  if (autocommit_mode == TRUE && as_Info->cur_sql_log_mode != SQL_LOG_MODE_ALL)
    {
      cas_sql_log_write_with_ts (&start_time, 0, "send_repl_data:tran_type:%d, num items:%d", tran_type, num_items);
    }

  NET_BUF_ERR_SET (net_buf);
  cas_sql_log_write (0, "send_repl_data (error:%d)", err_Info.err_number);
#if !defined(NDEBUG)
  dump_repl_data (buffer, sizeof (buffer), num_items, obj_argv);
  cas_sql_log_write (0, buffer);
#endif

  return FN_KEEP_CONN;
}

FN_RETURN
fn_notify_ha_agent_state (int argc, void **argv, T_NET_BUF * net_buf, UNUSED_ARG T_REQ_INFO * req_info)
{
  int state, autocommit_mode = FALSE;
  char host_str[MAX_NODE_INFO_STR_LEN];
  int arg_idx = 0;
  int error;
  PRM_NODE_INFO node_info;
  in_addr_t ip;
  int port;

  if (argc != 4)
    {
      ERROR_INFO_SET (CAS_ER_ARGS, CAS_ERROR_INDICATOR);
      goto error_exit;
    }

  net_arg_get_int ((int *) &ip, argv[arg_idx++]);
  net_arg_get_int (&port, argv[arg_idx++]);
  net_arg_get_int (&state, argv[arg_idx++]);
  net_arg_get_int (&autocommit_mode, argv[arg_idx++]);

  if (port == 0)
    {
      port = prm_get_rye_port_id ();
    }
  PRM_NODE_INFO_SET (&node_info, ip, port);

  prm_node_info_to_str (host_str, sizeof (host_str), &node_info);
  cas_sql_log_write (0, "notify_ha_agent_state (host: %s, state: %d)", host_str, state);

  error = boot_notify_ha_apply_state (&node_info, state);
  if (error < 0)
    {
      ERROR_INFO_SET (error, DBMS_ERROR_INDICATOR);
      goto error_exit;
    }
  if (autocommit_mode == TRUE)
    {
      req_info->need_auto_commit = TRAN_AUTOCOMMIT;
    }

  cas_sql_log_write (0, "notify_ha_agent_state (error: %d)", error);

  return FN_KEEP_CONN;

error_exit:
  if (autocommit_mode == TRUE)
    {
      req_info->need_auto_commit = TRAN_AUTOROLLBACK;
    }

  NET_BUF_ERR_SET (net_buf);
  cas_sql_log_write (0, "notify_ha_agent_state (error: %d)", err_Info.err_number);

  return FN_KEEP_CONN;
}
