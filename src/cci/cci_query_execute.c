/*
 * Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 *
 * - Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * - Neither the name of the <ORGANIZATION> nor the names of its contributors
 *   may be used to endorse or promote products derived from this software without
 *   specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */


/*
 * cci_query_execute.c -
 */

#ident "$Id$"

/************************************************************************
 * IMPORTED SYSTEM HEADER FILES						*
 ************************************************************************/
#include "config.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/stat.h>

#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>

/************************************************************************
 * OTHER IMPORTED HEADER FILES						*
 ************************************************************************/

#include "cas_protocol.h"
#include "cci_common.h"
#include "cci_query_execute.h"
#include "cci_network.h"
#include "cci_net_buf.h"
#include "cci_handle_mng.h"
#include "cci_util.h"

#include "repl_common.h"

/************************************************************************
 * PRIVATE DEFINITIONS							*
 ************************************************************************/
#define ALLOC_COPY_BIGINT(PTR, VALUE)      \
        do {                            \
          PTR = MALLOC(sizeof(INT64));  \
          if (PTR != NULL) {            \
            *((INT64*) (PTR)) = VALUE;  \
          }                             \
        } while (0)

#define ALLOC_COPY_UBIGINT(PTR, VALUE)      \
        do {                            \
          PTR = MALLOC(sizeof(UINT64)); \
          if (PTR != NULL) {            \
            *((UINT64*) (PTR)) = VALUE; \
          }                             \
        } while (0)

#define ALLOC_COPY_INT(PTR, VALUE)	\
	do {				\
	  PTR = MALLOC(sizeof(int));	\
	  if (PTR != NULL) {		\
	    *((int*) (PTR)) = VALUE;	\
	  }				\
	} while (0)

#define ALLOC_COPY_UINT(PTR, VALUE)	\
	do {				        \
	  PTR = MALLOC(sizeof(unsigned int));	\
	  if (PTR != NULL) {		        \
	    *((unsigned int*) (PTR)) = VALUE;	\
	  }				        \
	} while (0)

#define ALLOC_COPY_DOUBLE(PTR, VALUE)	\
	do {				\
	  PTR = MALLOC(sizeof(double));	\
	  if (PTR != NULL) {		\
	    *((double*) (PTR)) = VALUE;	\
	  }				\
	} while (0)

#define ALLOC_COPY_DATE(PTR, VALUE)		\
	do {					\
	  PTR = MALLOC(sizeof(T_CCI_DATETIME));	\
	  if (PTR != NULL) {			\
	    *((T_CCI_DATETIME*) (PTR)) = VALUE;	\
	  }					\
	} while (0)

#define ALLOC_COPY_BIT(DEST, SRC, SIZE)		\
	do {					\
	  DEST = MALLOC(SIZE);			\
	  if (DEST != NULL) {			\
	    memcpy(DEST, SRC, SIZE);		\
	  }					\
	} while (0)

#define EXECUTE_ARRAY	0
#define EXECUTE_BATCH	1

#define ADD_ARG_IDXKEY(BUF, IDXKEY) add_arg_idxkey(BUF, IDXKEY)

/************************************************************************
 * PRIVATE TYPE DEFINITIONS						*
 ************************************************************************/

typedef enum
{
  FETCH_FETCH,
  FETCH_OID_GET,
  FETCH_COL_GET
} T_FETCH_TYPE;

/************************************************************************
 * PRIVATE FUNCTION PROTOTYPES						*
 ************************************************************************/

static int prepare_info_decode (T_NET_RES * net_res,
				T_REQ_HANDLE * req_handle);
static int get_cursor_pos (T_REQ_HANDLE * req_handle, int offset,
			   char origin);
static int fetch_info_decode (T_NET_RES * net_res, int num_cols,
			      T_TUPLE_VALUE ** tuple_value,
			      T_REQ_HANDLE * req_handle);

static int get_column_info (T_NET_RES * net_res,
			    T_CCI_COL_INFO ** ret_col_info);
static int get_shard_key_info (T_NET_RES * net_res);
static int schema_info_decode (T_NET_RES * net_res,
			       T_REQ_HANDLE * req_handle);

static int bind_value_set (T_CCI_TYPE a_type, char flag, const void *value,
			   int length, T_BIND_VALUE * bind_value);
static int bind_value_to_net_buf (T_NET_BUF * net_buf, char a_type,
				  const void *value, int size);
static int execute_info_decode (T_NET_RES * net_res,
				T_REQ_HANDLE * req_handle);
static int execute_batch_info_decode (T_NET_RES * net_res, char flag,
				      T_CCI_QUERY_RESULT ** qr);
static int decode_fetch_result (T_REQ_HANDLE * req_handle,
				T_NET_RES * net_res);
static int qe_close_req_handle_internal (T_REQ_HANDLE * req_handle,
					 T_CON_HANDLE * con_handle,
					 bool force_close);
static int qe_send_close_handle_msg (T_CON_HANDLE * con_handle,
				     int server_handle_id);

static int qe_get_data_datetime (T_CCI_TYPE type, char *col_value_p,
				 void *value);
static int qe_get_data_str (T_VALUE_BUF * conv_val_buf, T_CCI_TYPE type,
			    char *col_value_p, int col_val_size, void *value,
			    int *indicator);
static int qe_get_data_bigint (T_CCI_TYPE type, char *col_value_p,
			       void *value);
static int qe_get_data_int (T_CCI_TYPE type, char *col_value_p, void *value);
static int qe_get_data_double (T_CCI_TYPE type, char *col_value_p,
			       void *value);
static int qe_get_data_bit (T_CCI_TYPE type, char *col_value_p,
			    int col_val_size, void *value);
static int add_arg_idxkey (T_NET_BUF * net_buf, DB_IDXKEY * idxkey);

/************************************************************************
 * INTERFACE VARIABLES							*
 ************************************************************************/

/************************************************************************
 * PUBLIC VARIABLES							*
 ************************************************************************/

/************************************************************************
 * PRIVATE VARIABLES							*
 ************************************************************************/

/************************************************************************
 * IMPLEMENTATION OF INTERFACE FUNCTIONS 				*
 ************************************************************************/

/************************************************************************
 * IMPLEMENTATION OF PUBLIC FUNCTIONS	 				*
 ************************************************************************/

int
qe_con_close (T_CON_HANDLE * con_handle)
{
  T_NET_BUF net_buf;
  char func_code = CAS_FC_CON_CLOSE;

  if (IS_INVALID_SOCKET (con_handle->sock_fd))
    return 0;

  net_buf_init (&net_buf);
  net_buf_cp_str (&net_buf, &func_code, 1);

  if (net_buf.err_code < 0)
    goto con_close_end;

  if (net_send_msg (con_handle, net_buf.data, net_buf.data_size) < 0)
    goto con_close_end;

  net_recv_msg (con_handle, NULL);

con_close_end:

  net_buf_clear (&net_buf);
  CLOSE_SOCKET (con_handle->sock_fd);
  con_handle->sock_fd = INVALID_SOCKET;

  return 0;
}

int
qe_end_tran (T_CON_HANDLE * con_handle, char type)
{
  T_NET_BUF net_buf;
  char func_code = CAS_FC_END_TRAN;
  int err_code;
  bool keep_connection;
  time_t cur_time, failure_time;
#ifdef END_TRAN2
  char type_str[2];
#endif

  if (IS_CON_TYPE_GLOBAL (con_handle))
    {
      con_handle->con_status = CCI_CON_STATUS_OUT_TRAN;
      return CCI_ER_NO_ERROR;
    }

  if (IS_INVALID_SOCKET (con_handle->sock_fd))
    {
      con_handle->con_status = CCI_CON_STATUS_OUT_TRAN;
      return CCI_ER_COMMUNICATION;
    }

  net_buf_init (&net_buf);

  net_buf_cp_str (&net_buf, &func_code, 1);

#ifdef END_TRAN2
  type_str[0] = type_str[1] = type;
  ADD_ARG_BYTES (&net_buf, type_str, 2);
#else
  ADD_ARG_BYTES (&net_buf, &type, 1);
#endif

  if (net_buf.err_code < 0)
    {
      err_code = net_buf.err_code;
      net_buf_clear (&net_buf);
      hm_force_close_connection (con_handle);
      return err_code;
    }

  err_code = net_send_msg (con_handle, net_buf.data, net_buf.data_size);
  net_buf_clear (&net_buf);
  if (err_code < 0)
    {
      hm_force_close_connection (con_handle);
      return err_code;
    }

  err_code = net_recv_msg (con_handle, NULL);

  if (IS_CAS_STATEMENT_POOLING (con_handle))
    {
      if (type == CCI_TRAN_ROLLBACK)
	{
	  /* close all results sets */
	  hm_req_handle_close_all_resultsets (con_handle);
	}
      else
	{
	  /* close only unholdable results sets */
	  hm_req_handle_close_all_unholdable_resultsets (con_handle);
	}
    }
  else
    {
      if (type == CCI_TRAN_ROLLBACK)
	{
	  hm_req_handle_free_all (con_handle);
	}
      else
	{
	  hm_req_handle_free_all_unholdable (con_handle);
	}
    }

  keep_connection = true;

  if (con_handle->alter_hosts->cur_id > 0 &&
      con_handle->con_property.rc_time > 0)
    {
      cur_time = time (NULL);
      failure_time = con_handle->last_failure_time;

      if (failure_time > 0 &&
	  (cur_time - failure_time) > con_handle->con_property.rc_time)
	{
	  if (con_is_unreachable_host (con_handle, 0) == false)
	    {
	      keep_connection = false;
	      con_handle->alter_hosts->cur_id = 0;
	      con_handle->force_failback = 0;
	      con_handle->last_failure_time = 0;
	    }
	}
    }

  if (keep_connection == false)
    {
      hm_force_close_connection (con_handle);
    }

  con_handle->con_status = CCI_CON_STATUS_OUT_TRAN;
  return err_code;
}

int
qe_prepare (T_REQ_HANDLE * req_handle, T_CON_HANDLE * con_handle,
	    const char *sql_stmt, char flag, int reuse)
{
  T_NET_BUF net_buf;
  char func_code = CAS_FC_PREPARE;
  char autocommit_flag;
  int sql_stmt_size;
  int err_code;
  int server_handle_id;
  int remaining_time = 0;
  T_NET_RES *net_res;

  if (IS_CON_TYPE_GLOBAL (con_handle))
    {
      assert (0);
      return CCI_ER_NOT_IMPLEMENTED;
    }

  if (!reuse)
    {
      FREE_MEM (req_handle->sql_text);
      ALLOC_COPY_STR (req_handle->sql_text, sql_stmt);
    }

  if (req_handle->sql_text == NULL)
    {
      return CCI_ER_NO_MORE_MEMORY;
    }

  sql_stmt_size = strlen (req_handle->sql_text) + 1;

  net_buf_init (&net_buf);

  net_buf_cp_str (&net_buf, &func_code, 1);

  ADD_ARG_STR (&net_buf, req_handle->sql_text, sql_stmt_size);

  if (hm_get_con_handle_holdable (con_handle))
    {
      /* make sure statement is holdable */
      flag |= CCI_PREPARE_HOLDABLE;
    }
  ADD_ARG_BYTES (&net_buf, &flag, 1);

  autocommit_flag = (char) con_handle->autocommit_mode;
  ADD_ARG_BYTES (&net_buf, &autocommit_flag, 1);

  ADD_ARG_INT (&net_buf, con_handle->deferred_close_handle_count);
  if (con_handle->deferred_close_handle_count > 0)
    {
      int i;
      for (i = 0; i < con_handle->deferred_close_handle_count; i++)
	{
	  ADD_ARG_INT (&net_buf, con_handle->deferred_close_handle_list[i]);
	}
      con_handle->deferred_close_handle_count = 0;
    }

  if (net_buf.err_code < 0)
    {
      err_code = net_buf.err_code;
      net_buf_clear (&net_buf);
      return err_code;
    }

  if (TIMEOUT_IS_SET (con_handle))
    {
      remaining_time = con_handle->current_timeout;
      remaining_time -=
	(int) timeval_diff_in_msec (NULL, &con_handle->start_time);
      if (remaining_time <= 0)
	{
	  net_buf_clear (&net_buf);
	  return CCI_ER_QUERY_TIMEOUT;
	}
    }

  err_code = net_send_msg (con_handle, net_buf.data, net_buf.data_size);
  net_buf_clear (&net_buf);
  if (err_code < 0)
    {
      return err_code;
    }

  err_code = net_recv_msg_timeout (con_handle, &net_res, remaining_time);
  if (err_code < 0)
    {
      return err_code;
    }

  if (net_res_to_int (&server_handle_id, net_res) < 0)
    {
      FREE_MEM (net_res);
      return CCI_ER_COMMUNICATION;
    }

  err_code = prepare_info_decode (net_res, req_handle);
  if (err_code < 0)
    {
      FREE_MEM (net_res);
      return err_code;
    }

  FREE_MEM (net_res);

  req_handle->handle_type = HANDLE_PREPARE;
  req_handle->server_handle_id = server_handle_id;
  req_handle->cur_fetch_tuple_index = -1;
  req_handle->prepare_flag = flag;
  req_handle->cursor_pos = 0;
  req_handle->is_closed = 0;
  req_handle->valid = 1;
  req_handle->is_from_current_transaction = 1;

  if (!reuse)
    {
      if (req_handle->num_bind > 0)
	{
	  req_handle->bind_value = (T_BIND_VALUE *)
	    MALLOC (sizeof (T_BIND_VALUE) * req_handle->num_bind);

	  if (req_handle->bind_value == NULL)
	    {
	      return CCI_ER_NO_MORE_MEMORY;
	    }

	  memset (req_handle->bind_value,
		  0, sizeof (T_BIND_VALUE) * req_handle->num_bind);
	}
    }

  return CCI_ER_NO_ERROR;
}

int
qe_bind_param (T_REQ_HANDLE * req_handle, int index, T_CCI_TYPE a_type,
	       const void *value, int length, char flag)
{
  int err_code;

  index--;

  if (index < 0 || index >= req_handle->num_bind)
    {
      return CCI_ER_BIND_INDEX;
    }

  if (req_handle->bind_value[index].alloc_buffer)
    {
      FREE_MEM (req_handle->bind_value[index].alloc_buffer);
      memset (&(req_handle->bind_value[index]), 0, sizeof (T_BIND_VALUE));
    }

  req_handle->bind_value[index].a_type = a_type;

  if (a_type == CCI_TYPE_NULL || value == NULL)
    {
      req_handle->bind_value[index].value = NULL;
      return 0;
    }

  err_code = bind_value_set (a_type, flag, value, length,
			     &(req_handle->bind_value[index]));

  return err_code;
}

int
qe_execute (T_REQ_HANDLE * req_handle, T_CON_HANDLE * con_handle, char flag,
	    int max_col_size, int group_id)
{
  T_NET_BUF net_buf;
  char func_code = CAS_FC_EXECUTE;
  char autocommit_flag;
  int i;
  int err_code = 0;
  int res_count;
  char fetch_flag;
#if 0
  char scrollable_cursor = false;
#endif
  int remaining_time = 0;
  bool use_server_query_cancel = false;
  T_NET_RES *net_res;

  if (IS_CON_TYPE_GLOBAL (con_handle))
    {
      assert (0);
      return CCI_ER_NOT_IMPLEMENTED;
    }

  QUERY_RESULT_FREE (req_handle);

#if 0
  if (con_handle->autocommit_mode == CCI_AUTOCOMMIT_TRUE)
    {
      scrollable_cursor = false;
    }
  else
    {
      scrollable_cursor = true;
    }
#endif

  net_buf_init (&net_buf);

  autocommit_flag = (char) con_handle->autocommit_mode;

  if (TIMEOUT_IS_SET (con_handle))
    {
      remaining_time = con_handle->current_timeout;
      remaining_time -=
	(int) timeval_diff_in_msec (NULL, &con_handle->start_time);
      if (remaining_time <= 0)
	{
	  err_code = CCI_ER_QUERY_TIMEOUT;
	  goto execute_error;
	}
    }
  if (TIMEOUT_IS_SET (con_handle)
      && con_handle->con_property.disconnect_on_query_timeout == false)
    {
      use_server_query_cancel = true;
    }

  net_buf_cp_str (&net_buf, &func_code, 1);

  ADD_ARG_INT (&net_buf, req_handle->server_handle_id);
  ADD_ARG_BYTES (&net_buf, &flag, 1);
  ADD_ARG_INT (&net_buf, max_col_size);
  ADD_ARG_INT (&net_buf, req_handle->max_row);
  ADD_ARG_BYTES (&net_buf, &autocommit_flag, 1);
  ADD_ARG_INT (&net_buf, remaining_time);
  ADD_ARG_INT (&net_buf, group_id);
  ADD_ARG_INT (&net_buf, req_handle->num_bind);

  for (i = 0; i < req_handle->num_bind; i++)
    {
      bind_value_to_net_buf (&net_buf,
			     (char) req_handle->bind_value[i].a_type,
			     req_handle->bind_value[i].value,
			     req_handle->bind_value[i].size);
    }

  if (net_buf.err_code < 0)
    {
      err_code = net_buf.err_code;
      goto execute_error;
    }

  err_code = net_send_msg (con_handle, net_buf.data, net_buf.data_size);
  if (err_code < 0)
    {
      goto execute_error;
    }

  net_buf_clear (&net_buf);

  err_code = net_recv_msg_timeout (con_handle, &net_res,
				   ((use_server_query_cancel) ?
				    0 : remaining_time));
  if (err_code < 0)
    {
      req_handle->cas_cursor_status = CAS_CURSOR_STATUS_CLOSED;
      return err_code;
    }

  if (net_res_to_int (&res_count, net_res) < 0)
    {
      FREE_MEM (net_res);
      return CCI_ER_COMMUNICATION;
    }

  err_code = execute_info_decode (net_res, req_handle);
  if (err_code < 0)
    {
      FREE_MEM (net_res);
      return err_code;
    }

  if (req_handle->stmt_type == CCI_STMT_SELECT
      || req_handle->stmt_type == CCI_STMT_SELECT_UPDATE)
    {
      req_handle->num_tuple = res_count;
    }
  else
    {
      req_handle->num_tuple = -1;
    }

  req_handle->execute_flag = flag;

  hm_req_handle_fetch_buf_free (req_handle);
  req_handle->cursor_pos = 0;

  /* If fetch_flag is 1, executing query and fetching data
     is processed together.
     So, fetching results are included in result.
   */

  if (net_res_to_byte (&fetch_flag, net_res) < 0)
    {
      FREE_MEM (net_res);
      return CCI_ER_COMMUNICATION;
    }

  if (fetch_flag == EXEC_CONTAIN_FETCH_RESULT)
    {
      int num_tuple;

      req_handle->cursor_pos = 1;
      num_tuple = decode_fetch_result (req_handle, net_res);
      req_handle->cursor_pos = 0;
      if (num_tuple < 0)
	{
	  FREE_MEM (net_res);
	  return num_tuple;
	}
    }
  else
    {
      req_handle->cas_cursor_status = CAS_CURSOR_STATUS_CLOSED;
      FREE_MEM (net_res);
    }

  req_handle->is_closed = 0;
  req_handle->is_from_current_transaction = 1;

  return res_count;

execute_error:
  req_handle->cas_cursor_status = CAS_CURSOR_STATUS_CLOSED;
  net_buf_clear (&net_buf);
  return err_code;
}

void
qe_bind_value_free (T_REQ_HANDLE * req_handle)
{
  int i;

  if (req_handle->bind_value == NULL)
    {
      return;
    }

  for (i = 0; i < req_handle->num_bind; i++)
    {
      if (req_handle->bind_value[i].alloc_buffer)
	{
	  FREE_MEM (req_handle->bind_value[i].alloc_buffer);
	}
    }
}

int
qe_get_db_parameter (T_CON_HANDLE * con_handle, T_CCI_DB_PARAM param_name,
		     void *ret_val)
{
  T_NET_BUF net_buf;
  char func_code = CAS_FC_GET_DB_PARAMETER;
  int err_code = CCI_ER_NO_ERROR;
  int val;
  T_NET_RES *net_res;

  if (IS_CON_TYPE_GLOBAL (con_handle))
    {
      assert (0);
      return CCI_ER_NOT_IMPLEMENTED;
    }

  if (ret_val == NULL)
    {
      return CCI_ER_NO_ERROR;
    }

  net_buf_init (&net_buf);

  net_buf_cp_str (&net_buf, &func_code, 1);

  ADD_ARG_INT (&net_buf, param_name);

  if (net_buf.err_code < 0)
    {
      err_code = net_buf.err_code;
      net_buf_clear (&net_buf);
      return err_code;
    }

  err_code = net_send_msg (con_handle, net_buf.data, net_buf.data_size);
  net_buf_clear (&net_buf);
  if (err_code < 0)
    {
      return err_code;
    }

  err_code = net_recv_msg (con_handle, &net_res);
  if (err_code < 0)
    {
      return err_code;
    }

  if (net_res_to_int (&val, net_res) < 0)
    {
      FREE_MEM (net_res);
      return CCI_ER_COMMUNICATION;
    }

  memcpy (ret_val, (char *) &val, sizeof (int));

  FREE_MEM (net_res);

  return CCI_ER_NO_ERROR;
}

int
qe_close_query_result (T_REQ_HANDLE * req_handle, T_CON_HANDLE * con_handle)
{
  int err_code = 0;
  T_NET_BUF net_buf;
  char func_code = CAS_FC_CURSOR_CLOSE;

  if (IS_CON_TYPE_GLOBAL (con_handle))
    {
      assert (0);
      return CCI_ER_NOT_IMPLEMENTED;
    }

  if (!hm_get_con_handle_holdable (con_handle)
      || req_handle->cas_cursor_status == CAS_CURSOR_STATUS_CLOSED)
    {
      return err_code;
    }

  net_buf_init (&net_buf);

  net_buf_cp_str (&net_buf, &func_code, 1);
  ADD_ARG_INT (&net_buf, req_handle->server_handle_id);

  if (net_buf.err_code < 0)
    {
      err_code = net_buf.err_code;
      net_buf_clear (&net_buf);
      return err_code;
    }

  err_code = net_send_msg (con_handle, net_buf.data, net_buf.data_size);
  net_buf_clear (&net_buf);
  if (err_code < 0)
    {
      return err_code;
    }

  err_code = net_recv_msg (con_handle, NULL);

  return err_code;
}

int
qe_close_req_handle (T_REQ_HANDLE * req_handle, T_CON_HANDLE * con_handle)
{
  if (IS_CON_TYPE_GLOBAL (con_handle))
    {
      assert (0);
      return CCI_ER_NOT_IMPLEMENTED;
    }

  return qe_close_req_handle_internal (req_handle, con_handle, false);
}

static int
qe_close_req_handle_internal (T_REQ_HANDLE * req_handle,
			      T_CON_HANDLE * con_handle, bool force_close)
{
  int err_code = 0;
  int *new_deferred_close_handle_list = NULL;
  int new_deferred_max_close_handle_count;

  /* same to qe_close_con.
     when statement pool is on,
     (cci_end_tran -> cci_close_req_handle) can be appeared.
     If connection was closed in cci_end_tran (keep_connection=off),
     failure(using closed socket) returns while sending a message in this function.
     So, if sockect is closed at this point, messages must not be sent to server.
   */
  if (IS_INVALID_SOCKET (con_handle->sock_fd) || !req_handle->valid)
    {
      return 0;
    }

  if (req_handle->stmt_type == CCI_STMT_SELECT ||
      req_handle->stmt_type == CCI_STMT_SELECT_UPDATE || force_close)
    {
      goto send_close_handle_msg;
    }

  if (con_handle->deferred_close_handle_count == 0 &&
      con_handle->deferred_max_close_handle_count !=
      DEFERRED_CLOSE_HANDLE_ALLOC_SIZE)
    {
      /* shrink the list size */
      new_deferred_close_handle_list =
	(int *) REALLOC (con_handle->deferred_close_handle_list,
			 sizeof (int) * DEFERRED_CLOSE_HANDLE_ALLOC_SIZE);
      if (new_deferred_close_handle_list == NULL)
	{
	  goto send_close_handle_msg;
	}
      con_handle->deferred_max_close_handle_count =
	DEFERRED_CLOSE_HANDLE_ALLOC_SIZE;
      con_handle->deferred_close_handle_list = new_deferred_close_handle_list;
    }
  else if (con_handle->deferred_close_handle_count + 1 >
	   con_handle->deferred_max_close_handle_count)
    {
      /* grow the list size */
      new_deferred_max_close_handle_count =
	con_handle->deferred_max_close_handle_count +
	DEFERRED_CLOSE_HANDLE_ALLOC_SIZE;
      new_deferred_close_handle_list =
	(int *) REALLOC (con_handle->deferred_close_handle_list,
			 sizeof (int) * new_deferred_max_close_handle_count);
      if (new_deferred_close_handle_list == NULL)
	{
	  goto send_close_handle_msg;
	}
      con_handle->deferred_max_close_handle_count =
	new_deferred_max_close_handle_count;
      con_handle->deferred_close_handle_list = new_deferred_close_handle_list;
    }

  con_handle->deferred_close_handle_list[con_handle->
					 deferred_close_handle_count++] =
    req_handle->server_handle_id;

  return err_code;

send_close_handle_msg:

  err_code = qe_send_close_handle_msg (con_handle,
				       req_handle->server_handle_id);

  return err_code;
}

void
qe_close_req_handle_all (T_CON_HANDLE * con_handle)
{
  int i;
  T_REQ_HANDLE *req_handle;

  /* close handle in req handle table */
  for (i = 0; i < con_handle->max_req_handle; i++)
    {
      if (con_handle->req_handle_table[i] == NULL)
	{
	  continue;
	}
      req_handle = con_handle->req_handle_table[i];

      qe_close_req_handle_internal (req_handle, con_handle, false);
    }
  hm_req_handle_free_all (con_handle);
}

static int
qe_send_close_handle_msg (T_CON_HANDLE * con_handle, int server_handle_id)
{
  int err_code = 0;
  T_NET_BUF net_buf;
  char func_code = CAS_FC_CLOSE_REQ_HANDLE;
  char autocommit_flag;

  net_buf_init (&net_buf);

  net_buf_cp_str (&net_buf, &func_code, 1);
  ADD_ARG_INT (&net_buf, server_handle_id);
  autocommit_flag = (char) con_handle->autocommit_mode;
  ADD_ARG_BYTES (&net_buf, &autocommit_flag, 1);

  if (net_buf.err_code < 0)
    {
      err_code = net_buf.err_code;
      net_buf_clear (&net_buf);
      return err_code;
    }

  err_code = net_send_msg (con_handle, net_buf.data, net_buf.data_size);
  net_buf_clear (&net_buf);
  if (err_code < 0)
    {
      return err_code;
    }

  err_code = net_recv_msg (con_handle, NULL);

  return err_code;
}

int
qe_cursor (T_REQ_HANDLE * req_handle, int offset, char origin)
{
  int cursor_pos;

  if (req_handle->is_closed)
    {
      return CCI_ER_RESULT_SET_CLOSED;
    }

  if (req_handle->handle_type == HANDLE_PREPARE)
    {
      if (req_handle->stmt_type == CCI_STMT_SELECT ||
	  req_handle->stmt_type == CCI_STMT_SELECT_UPDATE)
	{
	  cursor_pos = get_cursor_pos (req_handle, offset, origin);
	  if (cursor_pos <= 0)
	    {
	      req_handle->cursor_pos = 0;
	      return CCI_ER_NO_MORE_DATA;
	    }
	  else if (cursor_pos > req_handle->num_tuple)
	    {
	      req_handle->cursor_pos = req_handle->num_tuple + 1;
	      return CCI_ER_NO_MORE_DATA;
	    }

	  req_handle->cursor_pos = cursor_pos;
	  return 0;
	}
      else
	{
	  return CCI_ER_NO_MORE_DATA;
	}
    }
  else if (req_handle->handle_type == HANDLE_SCHEMA_INFO)
    {
      cursor_pos = get_cursor_pos (req_handle, offset, origin);
      if (cursor_pos <= 0 || cursor_pos > req_handle->num_tuple)
	{
	  if (cursor_pos <= 0)
	    req_handle->cursor_pos = 0;
	  else if (cursor_pos > req_handle->num_tuple)
	    req_handle->cursor_pos = req_handle->num_tuple + 1;

	  return CCI_ER_NO_MORE_DATA;
	}
      req_handle->cursor_pos = cursor_pos;
      return 0;
    }
  else
    {
      assert (0);
    }

  return CCI_ER_NO_MORE_DATA;
}

int
qe_fetch (T_REQ_HANDLE * req_handle, T_CON_HANDLE * con_handle,
	  int result_set_index)
{
  T_NET_BUF net_buf;
  int err_code;
  char func_code = CAS_FC_FETCH;
  int num_tuple;
  T_NET_RES *net_res;

  if (IS_CON_TYPE_GLOBAL (con_handle))
    {
      assert (0);
      return CCI_ER_NOT_IMPLEMENTED;
    }

  if (req_handle->cursor_pos <= 0)
    {
      return CCI_ER_NO_MORE_DATA;
    }

  if (req_handle->is_closed)
    {
      return CCI_ER_RESULT_SET_CLOSED;
    }

  if (req_handle->fetched_tuple_begin > 0 &&
      req_handle->cursor_pos >= req_handle->fetched_tuple_begin &&
      req_handle->cursor_pos <= req_handle->fetched_tuple_end)
    {
      req_handle->cur_fetch_tuple_index = req_handle->cursor_pos -
	req_handle->fetched_tuple_begin;
      return 0;
    }

  hm_req_handle_fetch_buf_free (req_handle);

  net_buf_init (&net_buf);
  net_buf_cp_str (&net_buf, &func_code, 1);
  ADD_ARG_INT (&net_buf, req_handle->server_handle_id);
  ADD_ARG_INT (&net_buf, req_handle->cursor_pos);
  ADD_ARG_INT (&net_buf, 0);	/* fetch size. not used */
  ADD_ARG_INT (&net_buf, result_set_index);

  if (net_buf.err_code < 0)
    {
      err_code = net_buf.err_code;
      net_buf_clear (&net_buf);
      return err_code;
    }

  err_code = net_send_msg (con_handle, net_buf.data, net_buf.data_size);
  net_buf_clear (&net_buf);
  if (err_code < 0)
    return err_code;

  err_code = net_recv_msg (con_handle, &net_res);
  if (err_code < 0)
    {
      return err_code;
    }

  num_tuple = decode_fetch_result (req_handle, net_res);
  if (num_tuple < 0)
    {
      FREE_MEM (net_res);
      return num_tuple;
    }

  return 0;
}

int
qe_get_data (T_REQ_HANDLE * req_handle, int col_no,
	     int a_type, void *value, int *indicator)
{
  char *col_value_p;
  T_CCI_TYPE db_type;
  int data_size;
  int err_code;
  int num_cols;

  if (req_handle->is_closed)
    {
      return CCI_ER_RESULT_SET_CLOSED;
    }

  num_cols = req_handle->num_col_info;

  if (col_no <= 0 || col_no > num_cols)
    return CCI_ER_COLUMN_INDEX;

  if (req_handle->cur_fetch_tuple_index < 0)
    return CCI_ER_INVALID_CURSOR_POS;

  col_value_p =
    req_handle->tuple_value[req_handle->cur_fetch_tuple_index].
    column_ptr[col_no - 1];

  db_type = CCI_GET_RESULT_INFO_TYPE (req_handle->col_info, col_no);


  NET_STR_TO_INT (data_size, col_value_p);
  if (data_size <= 0)
    {
      *indicator = -1;
      return 0;
    }

  col_value_p += NET_SIZE_INT;

  if (db_type == CCI_TYPE_NULL)
    {
      char type;
      NET_STR_TO_BYTE (type, col_value_p);
      db_type = (T_CCI_TYPE) type;
      col_value_p += NET_SIZE_BYTE;
      data_size--;
    }

  switch (a_type)
    {
    case CCI_TYPE_VARCHAR:
    case CCI_TYPE_NUMERIC:
      err_code = qe_get_data_str (&(req_handle->conv_value_buffer), db_type,
				  col_value_p, data_size, value, indicator);
      break;
    case CCI_TYPE_BIGINT:
      err_code = qe_get_data_bigint (db_type, col_value_p, value);
      break;
    case CCI_TYPE_INT:
      err_code = qe_get_data_int (db_type, col_value_p, value);
      break;
    case CCI_TYPE_DOUBLE:
      err_code = qe_get_data_double (db_type, col_value_p, value);
      break;
    case CCI_TYPE_VARBIT:
      err_code = qe_get_data_bit (db_type, col_value_p, data_size, value);
      break;
    case CCI_TYPE_DATE:
    case CCI_TYPE_TIME:
    case CCI_TYPE_DATETIME:
      err_code = qe_get_data_datetime (db_type, col_value_p, value);
      break;
    default:
      return CCI_ER_ATYPE;
    }

  *indicator = (err_code < 0 ? -1 : 0);

  return err_code;
}

int
qe_schema_info (T_REQ_HANDLE * req_handle, T_CON_HANDLE * con_handle,
		int type, char *arg1, char *arg2, int flag)
{
  T_NET_BUF net_buf;
  char func_code = CAS_FC_SCHEMA_INFO;
  int err_code;
  int server_handle_id;
  T_NET_RES *net_res;
  char dummy_stmt_type;
  int dummy_parameter_count;

  if (IS_CON_TYPE_GLOBAL (con_handle))
    {
      assert (0);
      return CCI_ER_NOT_IMPLEMENTED;
    }

  net_buf_init (&net_buf);
  net_buf_cp_str (&net_buf, &func_code, 1);
  ADD_ARG_INT (&net_buf, type);

  if (arg1 == NULL)
    {
      ADD_ARG_BYTES (&net_buf, NULL, 0);
    }
  else
    {
      ADD_ARG_STR (&net_buf, arg1, strlen (arg1) + 1);
    }

  if (arg2 == NULL)
    {
      ADD_ARG_BYTES (&net_buf, NULL, 0);
    }
  else
    {
      ADD_ARG_STR (&net_buf, arg2, strlen (arg2) + 1);
    }

  ADD_ARG_INT (&net_buf, flag);

  if (1)
    {
      ADD_ARG_INT (&net_buf, 0);
    }

  if (net_buf.err_code < 0)
    {
      err_code = net_buf.err_code;
      net_buf_clear (&net_buf);
      return err_code;
    }

  err_code = net_send_msg (con_handle, net_buf.data, net_buf.data_size);
  net_buf_clear (&net_buf);
  if (err_code < 0)
    {
      return err_code;
    }

  err_code = net_recv_msg (con_handle, &net_res);
  if (err_code < 0)
    {
      return err_code;
    }

  if (net_res_to_int (&server_handle_id, net_res) < 0 ||
      net_res_to_byte (&dummy_stmt_type, net_res) < 0 ||
      net_res_to_int (&dummy_parameter_count, net_res) < 0)
    {
      FREE_MEM (net_res);
      return CCI_ER_COMMUNICATION;
    }

  err_code = schema_info_decode (net_res, req_handle);

  FREE_MEM (net_res);

  if (err_code < 0)
    {
      return err_code;
    }

  req_handle->handle_type = HANDLE_SCHEMA_INFO;
  req_handle->server_handle_id = server_handle_id;
  req_handle->cur_fetch_tuple_index = -1;
  req_handle->cursor_pos = 0;
  req_handle->valid = 1;

  return 0;
}

int
qe_get_db_version (T_CON_HANDLE * con_handle, char *out_buf, int buf_size)
{
  T_NET_BUF net_buf;
  char func_code = CAS_FC_GET_DB_VERSION;
  char autocommit_flag;
  int err_code, remaining_time = 0;
  T_NET_RES *net_res;

  if (IS_CON_TYPE_GLOBAL (con_handle))
    {
      assert (0);
      return CCI_ER_NOT_IMPLEMENTED;
    }

  net_buf_init (&net_buf);
  net_buf_cp_str (&net_buf, &func_code, 1);
  autocommit_flag = (char) con_handle->autocommit_mode;
  ADD_ARG_BYTES (&net_buf, &autocommit_flag, 1);

  if (net_buf.err_code < 0)
    {
      err_code = net_buf.err_code;
      net_buf_clear (&net_buf);
      return err_code;
    }

  if (TIMEOUT_IS_SET (con_handle))
    {
      remaining_time = con_handle->current_timeout;
      remaining_time -=
	(int) timeval_diff_in_msec (NULL, &con_handle->start_time);
      if (remaining_time <= 0)
	{
	  net_buf_clear (&net_buf);
	  return CCI_ER_QUERY_TIMEOUT;
	}
    }

  err_code = net_send_msg (con_handle, net_buf.data, net_buf.data_size);
  net_buf_clear (&net_buf);
  if (err_code < 0)
    {
      return err_code;
    }


  err_code = net_recv_msg_timeout (con_handle, &net_res, remaining_time);
  if (err_code < 0)
    {
      return err_code;
    }

  if (out_buf)
    {
      char *ptr;
      int size;

      if (net_res_to_str (&ptr, &size, net_res) < 0)
	{
	  FREE_MEM (net_res);
	  return CCI_ER_COMMUNICATION;
	}

      buf_size = MIN (buf_size - 1, size);
      strncpy (out_buf, ptr, buf_size);
      out_buf[buf_size] = '\0';
    }

  FREE_MEM (net_res);

  return err_code;
}

int
qe_execute_batch (T_REQ_HANDLE * req_handle, T_CON_HANDLE * con_handle,
		  T_CCI_QUERY_RESULT ** qr)
{
  T_NET_BUF net_buf;
  char func_code = CAS_FC_EXECUTE_BATCH;
  char autocommit_flag;
  int err_code = 0;
  T_BIND_VALUE cur_cell;
  int row, idx;
  int remaining_time = 0;
  int group_id = 0;
  T_NET_RES *net_res;

  if (IS_CON_TYPE_GLOBAL (con_handle))
    {
      assert (0);
      return CCI_ER_NOT_IMPLEMENTED;
    }

  net_buf_init (&net_buf);

  if (TIMEOUT_IS_SET (con_handle))
    {
      remaining_time = con_handle->current_timeout;
      remaining_time -=
	(int) timeval_diff_in_msec (NULL, &con_handle->start_time);
      if (remaining_time <= 0)
	{
	  net_buf_clear (&net_buf);
	  return CCI_ER_QUERY_TIMEOUT;
	}
    }

  autocommit_flag = (char) con_handle->autocommit_mode;

  net_buf_cp_str (&net_buf, &func_code, 1);

  ADD_ARG_INT (&net_buf, req_handle->server_handle_id);
  ADD_ARG_INT (&net_buf, remaining_time);
  ADD_ARG_BYTES (&net_buf, &autocommit_flag, 1);
  ADD_ARG_INT (&net_buf, group_id);
  ADD_ARG_INT (&net_buf, req_handle->bind_array_size);
  ADD_ARG_INT (&net_buf, req_handle->num_bind);
  ADD_ARG_INT (&net_buf, req_handle->bind_array_size * req_handle->num_bind);

  for (row = 0; row < req_handle->bind_array_size; row++)
    {
      for (idx = 0; idx < req_handle->num_bind; idx++)
	{
	  cur_cell.alloc_buffer = NULL;
	  cur_cell.value = NULL;
	  cur_cell.a_type = req_handle->bind_value[idx].a_type;

	  if (req_handle->bind_value[idx].value == NULL ||
	      req_handle->bind_value[idx].null_ind[row])
	    {
	      cur_cell.value = NULL;
	      cur_cell.size = 0;
	    }
	  else
	    {
	      T_CCI_TYPE a_type;
	      const void *value;
	      const void *ptr_arr;

	      a_type = req_handle->bind_value[idx].a_type;
	      err_code = 0;

	      ptr_arr = req_handle->bind_value[idx].value;

	      if (a_type == CCI_TYPE_VARCHAR || a_type == CCI_TYPE_NUMERIC)
		{
		  value = ((const char *const *) ptr_arr)[row];
		}
	      else if (a_type == CCI_TYPE_BIGINT)
		{
		  value = &((const INT64 *) ptr_arr)[row];
		}
	      else if (a_type == CCI_TYPE_INT)
		{
		  value = &((const int *) ptr_arr)[row];
		}
	      else if (a_type == CCI_TYPE_DOUBLE)
		{
		  value = &((const double *) ptr_arr)[row];
		}
	      else if (a_type == CCI_TYPE_VARBIT)
		{
		  value = &((const T_CCI_VARBIT *) ptr_arr)[row];
		}
	      else if (a_type == CCI_TYPE_DATE || a_type == CCI_TYPE_TIME ||
		       a_type == CCI_TYPE_DATETIME)
		{
		  value = &((const T_CCI_DATETIME *) ptr_arr)[row];
		}
	      else
		{
		  err_code = CCI_ER_ATYPE;
		}

	      if (err_code == 0)
		{
		  err_code = bind_value_set (a_type, CCI_BIND_PTR, value,
					     UNMEASURED_LENGTH, &cur_cell);
		}

	      if (err_code < 0)
		{
		  net_buf_clear (&net_buf);
		  return err_code;
		}
	    }
	  bind_value_to_net_buf (&net_buf, (char) cur_cell.a_type,
				 cur_cell.value, cur_cell.size);

	  FREE_MEM (cur_cell.alloc_buffer);
	}			/* end of for (idx) */
    }				/* end of for (row) */

  if (net_buf.err_code < 0)
    {
      err_code = net_buf.err_code;
      net_buf_clear (&net_buf);
      return err_code;
    }

  if (TIMEOUT_IS_SET (con_handle))
    {
      remaining_time = con_handle->current_timeout;
      remaining_time -=
	(int) timeval_diff_in_msec (NULL, &con_handle->start_time);
      if (remaining_time <= 0)
	{
	  net_buf_clear (&net_buf);
	  return CCI_ER_QUERY_TIMEOUT;
	}
    }

  err_code = net_send_msg (con_handle, net_buf.data, net_buf.data_size);
  net_buf_clear (&net_buf);
  if (err_code < 0)
    {
      return err_code;
    }

  err_code = net_recv_msg_timeout (con_handle, &net_res, 0);
  if (err_code < 0)
    {
      return err_code;
    }

  err_code = execute_batch_info_decode (net_res, EXECUTE_ARRAY, qr);

  FREE_MEM (net_res);

  return err_code;
}

void
qe_query_result_free (int num_q, T_CCI_QUERY_RESULT * qr)
{
  int i;

  if (qr)
    {
      for (i = 0; i < num_q; i++)
	FREE_MEM (qr[i].err_msg);
      FREE_MEM (qr);
    }
}

int
qe_query_result_copy (T_REQ_HANDLE * req_handle, T_CCI_QUERY_RESULT ** res_qr)
{
  T_CCI_QUERY_RESULT *qr = NULL;
  int num_query = req_handle->num_query_res;
  int i;

  *res_qr = NULL;

  if (req_handle->qr == NULL || num_query == 0)
    {
      return 0;
    }

  qr =
    (T_CCI_QUERY_RESULT *) MALLOC (sizeof (T_CCI_QUERY_RESULT) * num_query);
  if (qr == NULL)
    {
      return CCI_ER_NO_MORE_MEMORY;
    }

  for (i = 0; i < num_query; i++)
    {
      qr[i].result_count = req_handle->qr[i].result_count;
      qr[i].stmt_type = req_handle->qr[i].stmt_type;
      qr[i].err_no = req_handle->qr[i].err_no;
      ALLOC_COPY_STR (qr[i].err_msg, req_handle->qr[i].err_msg);
    }

  *res_qr = qr;

  return num_query;
}

static int
qe_get_data_str (T_VALUE_BUF * conv_val_buf, T_CCI_TYPE db_type,
		 char *col_value_p, int col_val_size, void *value,
		 int *indicator)
{
  switch (db_type)
    {
    case CCI_TYPE_VARCHAR:
    case CCI_TYPE_NUMERIC:
      {
	*((char **) value) = col_value_p;
	*indicator = col_val_size - 1;
      }
      return 0;
    case CCI_TYPE_VARBIT:
      {
	if (hm_conv_value_buf_alloc (conv_val_buf, col_val_size * 2 + 2) < 0)
	  {
	    return CCI_ER_NO_MORE_MEMORY;
	  }
	ut_bit_to_str (col_value_p, col_val_size,
		       (char *) conv_val_buf->data, col_val_size * 2 + 2);
      }
      break;
    case CCI_TYPE_BIGINT:
      {
	INT64 data;

	qe_get_data_bigint (db_type, col_value_p, &data);

	if (hm_conv_value_buf_alloc (conv_val_buf, 128) < 0)
	  {
	    return CCI_ER_NO_MORE_MEMORY;
	  }
	ut_int_to_str (data, (char *) conv_val_buf->data, 128);
      }
      break;
    case CCI_TYPE_INT:
      {
	int data;

	qe_get_data_int (db_type, col_value_p, &data);

	if (hm_conv_value_buf_alloc (conv_val_buf, 128) < 0)
	  {
	    return CCI_ER_NO_MORE_MEMORY;
	  }
	ut_int_to_str (data, (char *) conv_val_buf->data, 128);
      }
      break;
    case CCI_TYPE_DOUBLE:
      {
	double data;

	qe_get_data_double (db_type, col_value_p, &data);

	if (hm_conv_value_buf_alloc (conv_val_buf, 512) < 0)
	  {
	    return CCI_ER_NO_MORE_MEMORY;
	  }
	ut_double_to_str (data, (char *) conv_val_buf->data, 512);
      }
      break;
    case CCI_TYPE_DATE:
    case CCI_TYPE_TIME:
    case CCI_TYPE_DATETIME:
      {
	T_CCI_DATETIME data;

	qe_get_data_datetime (db_type, col_value_p, &data);

	if (hm_conv_value_buf_alloc (conv_val_buf, 128) < 0)
	  {
	    return CCI_ER_NO_MORE_MEMORY;
	  }
	ut_datetime_to_str (&data, db_type, (char *) conv_val_buf->data, 128);
      }
      break;

    default:
      return CCI_ER_TYPE_CONVERSION;
    }

  *((char **) value) = (char *) conv_val_buf->data;
  *indicator = strlen ((char *) conv_val_buf->data);

  return 0;
}

static int
qe_get_data_bigint (T_CCI_TYPE db_type, char *col_value_p, void *value)
{
  INT64 data;

  switch (db_type)
    {
    case CCI_TYPE_VARCHAR:
    case CCI_TYPE_NUMERIC:
      if (ut_str_to_bigint (col_value_p, &data) < 0)
	{
	  return CCI_ER_TYPE_CONVERSION;
	}
      break;
    case CCI_TYPE_BIGINT:
      NET_STR_TO_BIGINT (data, col_value_p);
      break;
    case CCI_TYPE_INT:
      {
	int i_val;
	NET_STR_TO_INT (i_val, col_value_p);
	data = (INT64) i_val;
	break;
      }
    case CCI_TYPE_DOUBLE:
      {
	double d_val;
	NET_STR_TO_DOUBLE (d_val, col_value_p);
	data = (INT64) d_val;
      }
      break;
    default:
      return CCI_ER_TYPE_CONVERSION;
    }

  *((INT64 *) value) = data;

  return 0;
}

static int
qe_get_data_int (T_CCI_TYPE db_type, char *col_value_p, void *value)
{
  int data;

  switch (db_type)
    {
    case CCI_TYPE_VARCHAR:
    case CCI_TYPE_NUMERIC:
      if (ut_str_to_int (col_value_p, &data) < 0)
	{
	  return CCI_ER_TYPE_CONVERSION;
	}
      break;
    case CCI_TYPE_BIGINT:
      {
	INT64 bi_val;
	NET_STR_TO_BIGINT (bi_val, col_value_p);
	data = (int) bi_val;
      }
      break;
    case CCI_TYPE_INT:
      NET_STR_TO_INT (data, col_value_p);
      break;
    case CCI_TYPE_DOUBLE:
      {
	double d_val;
	NET_STR_TO_DOUBLE (d_val, col_value_p);
	data = (int) d_val;
      }
      break;
    default:
      return CCI_ER_TYPE_CONVERSION;
    }

  *((int *) value) = data;

  return 0;
}

static int
qe_get_data_double (T_CCI_TYPE db_type, char *col_value_p, void *value)
{
  double data;

  switch (db_type)
    {
    case CCI_TYPE_VARCHAR:
    case CCI_TYPE_NUMERIC:
      if (ut_str_to_double (col_value_p, &data) < 0)
	{
	  return CCI_ER_TYPE_CONVERSION;
	}
      break;
    case CCI_TYPE_BIGINT:
      {
	INT64 bi_val;
	NET_STR_TO_BIGINT (bi_val, col_value_p);
	data = (double) bi_val;
      }
      break;
    case CCI_TYPE_INT:
      {
	int i_val;
	NET_STR_TO_INT (i_val, col_value_p);
	data = (double) i_val;
      }
      break;
    case CCI_TYPE_DOUBLE:
      {
	NET_STR_TO_DOUBLE (data, col_value_p);
      }
      break;
    default:
      return CCI_ER_TYPE_CONVERSION;
    }

  *((double *) value) = data;
  return 0;
}

static int
qe_get_data_datetime (T_CCI_TYPE db_type, char *col_value_p, void *value)
{
  T_CCI_DATETIME data;

  memset ((char *) &data, 0, sizeof (T_CCI_DATETIME));

  switch (db_type)
    {
    case CCI_TYPE_DATE:
      NET_STR_TO_DATE (data, col_value_p);
      break;
    case CCI_TYPE_TIME:
      NET_STR_TO_TIME (data, col_value_p);
      break;
    case CCI_TYPE_DATETIME:
      NET_STR_TO_DATETIME (data, col_value_p);
      break;
    default:
      return CCI_ER_TYPE_CONVERSION;
    }

  *((T_CCI_DATETIME *) value) = data;
  return 0;
}

static int
qe_get_data_bit (T_CCI_TYPE db_type, char *col_value_p, int col_val_size,
		 void *value)
{
  if (db_type == CCI_TYPE_VARBIT)
    {
      ((T_CCI_VARBIT *) value)->size = col_val_size;
      ((T_CCI_VARBIT *) value)->buf = col_value_p;
      return 0;
    }

  return CCI_ER_TYPE_CONVERSION;
}

int
qe_get_query_plan (T_CON_HANDLE * con_handle, const char *sql, char **out_buf)
{
  T_NET_BUF net_buf;
  char func_code = CAS_FC_GET_QUERY_PLAN;
  int err_code;
  T_NET_RES *net_res;

  if (IS_CON_TYPE_GLOBAL (con_handle))
    {
      assert (0);
      return CCI_ER_NOT_IMPLEMENTED;
    }

  if (sql == NULL)
    {
      if (out_buf)
	{
	  *out_buf = NULL;
	}
      return CCI_ER_NO_ERROR;
    }

  net_buf_init (&net_buf);
  net_buf_cp_str (&net_buf, &func_code, 1);
  ADD_ARG_STR (&net_buf, sql, strlen (sql) + 1);

  if (net_buf.err_code < 0)
    {
      err_code = net_buf.err_code;
      net_buf_clear (&net_buf);
      return err_code;
    }

  err_code = net_send_msg (con_handle, net_buf.data, net_buf.data_size);
  net_buf_clear (&net_buf);
  if (err_code < 0)
    return err_code;

  err_code = net_recv_msg (con_handle, &net_res);
  if (err_code < 0)
    {
      return err_code;
    }

  if (out_buf)
    {
      char *tmp_buf;
      char *ptr;
      int info_size;

      err_code = net_res_to_str (&ptr, &info_size, net_res);
      if (err_code < 0)
	{
	  FREE_MEM (net_res);
	  return err_code;
	}

      tmp_buf = (char *) MALLOC (info_size + 1);
      if (tmp_buf == NULL)
	{
	  FREE_MEM (net_res);
	  return CCI_ER_NO_MORE_MEMORY;
	}
      memcpy (tmp_buf, ptr, info_size);
      tmp_buf[info_size] = '\0';
      *out_buf = tmp_buf;
    }

  FREE_MEM (net_res);

  return err_code;
}

void
tuple_value_free (T_TUPLE_VALUE * tuple_value, int num_tuple,
		  UNUSED_ARG int num_cols)
{
  if (tuple_value)
    {
      int i;

      for (i = 0; i < num_tuple; i++)
	{
	  FREE_MEM (tuple_value[i].column_ptr);
	}
      FREE_MEM (tuple_value);
    }
}

/************************************************************************
 * IMPLEMENTATION OF PRIVATE FUNCTIONS	 				*
 ************************************************************************/

static int
prepare_info_decode (T_NET_RES * net_res, T_REQ_HANDLE * req_handle)
{
  int num_bind_info;
  int num_col_info;
  char stmt_type;
  T_CCI_COL_INFO *col_info = NULL;

  if (net_res_to_byte (&stmt_type, net_res) < 0 ||
      net_res_to_int (&num_bind_info, net_res) < 0)
    {
      return CCI_ER_COMMUNICATION;
    }

  num_col_info = get_column_info (net_res, &col_info);
  if (num_col_info < 0)
    {
      assert (col_info == NULL);
      return num_col_info;
    }

  if (get_shard_key_info (net_res) < 0)
    {
      return CCI_ER_COMMUNICATION;
    }

  req_handle->num_bind = num_bind_info;
  req_handle->num_col_info = num_col_info;
  req_handle->col_info = col_info;
  req_handle->stmt_type = (T_CCI_STMT_TYPE) stmt_type;

  return CCI_ER_NO_ERROR;
}

static int
get_cursor_pos (T_REQ_HANDLE * req_handle, int offset, char origin)
{
  if (origin == CCI_CURSOR_FIRST)
    {
      return offset;
    }
  else if (origin == CCI_CURSOR_LAST)
    {
      return (req_handle->num_tuple - offset + 1);
    }
  return (req_handle->cursor_pos + offset);
}

static int
fetch_info_decode (T_NET_RES * net_res, int num_cols,
		   T_TUPLE_VALUE ** tuple_value, T_REQ_HANDLE * req_handle)
{
  int err_code = 0;
  int num_tuple, i, j;
  T_TUPLE_VALUE *tmp_tuple_value = NULL;
  char cursor_status;

  req_handle->cas_cursor_status = CAS_CURSOR_STATUS_OPEN;

  if (net_res_to_int (&num_tuple, net_res) < 0)
    {
      return CCI_ER_COMMUNICATION;
    }

  if (num_tuple < 0)
    {
      return 0;
    }
  else if (num_tuple == 0)
    {
      if (net_res_to_byte (&cursor_status, net_res) < 0)
	{
	  return CCI_ER_COMMUNICATION;
	}

      req_handle->cas_cursor_status = cursor_status;

      return 0;
    }

  tmp_tuple_value = (T_TUPLE_VALUE *)
    MALLOC (sizeof (T_TUPLE_VALUE) * num_tuple);
  if (tmp_tuple_value == NULL)
    {
      return CCI_ER_NO_MORE_MEMORY;
    }
  memset ((char *) tmp_tuple_value, 0, sizeof (T_TUPLE_VALUE) * num_tuple);

  for (i = 0; i < num_tuple; i++)
    {
      int tuple_index;

      if (net_res_to_int (&tuple_index, net_res) < 0)
	{
	  err_code = CCI_ER_COMMUNICATION;
	  goto fetch_info_decode_error;
	}

      tmp_tuple_value[i].tuple_index = tuple_index;

      tmp_tuple_value[i].column_ptr =
	(char **) MALLOC (sizeof (char *) * num_cols);
      if (tmp_tuple_value[i].column_ptr == NULL)
	{
	  err_code = CCI_ER_NO_MORE_MEMORY;
	  goto fetch_info_decode_error;
	}
      memset (tmp_tuple_value[i].column_ptr, 0, sizeof (char *) * num_cols);

      for (j = 0; j < num_cols; j++)
	{
	  int data_size;
	  char *col_p, *dummy_ptr;

	  col_p = net_res_cur_ptr (net_res);

	  if (net_res_to_str (&dummy_ptr, &data_size, net_res) < 0)
	    {
	      err_code = CCI_ER_COMMUNICATION;
	      goto fetch_info_decode_error;
	    }

	  tmp_tuple_value[i].column_ptr[j] = col_p;
	}			/* end of for j */
    }				/* end of for i */

  if (net_res_to_byte (&cursor_status, net_res) < 0)
    {
      err_code = CCI_ER_COMMUNICATION;
      goto fetch_info_decode_error;
    }
  req_handle->cas_cursor_status = cursor_status;

  *tuple_value = tmp_tuple_value;

  return num_tuple;

fetch_info_decode_error:
  TUPLE_VALUE_FREE (tmp_tuple_value, num_tuple, num_cols);
  return err_code;
}

static int
get_column_info (T_NET_RES * net_res, T_CCI_COL_INFO ** ret_col_info)
{
  int num_col_info = 0;
  T_CCI_COL_INFO *col_info = NULL;
  int i;

  if (ret_col_info)
    {
      *ret_col_info = NULL;
    }

  if (net_res_to_int (&num_col_info, net_res) < 0 || num_col_info < 0)
    {
      return CCI_ER_COMMUNICATION;
    }

  col_info = NULL;
  if (num_col_info > 0)
    {
      col_info = (T_CCI_COL_INFO *) MALLOC (sizeof (T_CCI_COL_INFO)
					    * num_col_info);
      if (col_info == NULL)
	{
	  return CCI_ER_NO_MORE_MEMORY;
	}

      memset ((char *) col_info, 0, sizeof (T_CCI_COL_INFO) * num_col_info);

      for (i = 0; i < num_col_info; i++)
	{
	  char type;
	  short scale;
	  int precision;
	  int disp_name_size;
	  char *disp_name_ptr;
	  int attr_name_size;
	  char *attr_name_ptr;
	  int tbl_name_size;
	  char *tbl_name_ptr;
	  char is_non_null;
	  int defval_size;
	  char *defval_ptr;
	  char is_unique_key;
	  char is_primary_key;

	  if (net_res_to_byte (&type, net_res) < 0 ||
	      net_res_to_short (&scale, net_res) < 0 ||
	      net_res_to_int (&precision, net_res) < 0 ||
	      net_res_to_str (&disp_name_ptr, &disp_name_size, net_res) < 0)
	    {
	      goto get_column_info_error;
	    }

	  col_info[i].type = (T_CCI_TYPE) type;
	  col_info[i].scale = scale;
	  col_info[i].precision = precision;

	  ALLOC_N_COPY (col_info[i].col_name, disp_name_ptr, disp_name_size,
			char *);
	  if (col_info[i].col_name == NULL)
	    {
	      goto get_column_info_error;
	    }

	  if (net_res_to_str (&attr_name_ptr, &attr_name_size, net_res) < 0 ||
	      net_res_to_str (&tbl_name_ptr, &tbl_name_size, net_res) < 0 ||
	      net_res_to_byte (&is_non_null, net_res) < 0 ||
	      net_res_to_str (&defval_ptr, &defval_size, net_res) < 0 ||
	      net_res_to_byte (&is_unique_key, net_res) < 0 ||
	      net_res_to_byte (&is_primary_key, net_res) < 0)
	    {
	      goto get_column_info_error;
	    }

	  ALLOC_N_COPY (col_info[i].real_attr, attr_name_ptr, attr_name_size,
			char *);
	  ALLOC_N_COPY (col_info[i].class_name, tbl_name_ptr, tbl_name_size,
			char *);
	  col_info[i].is_non_null = is_non_null;
	  ALLOC_N_COPY (col_info[i].default_value, defval_ptr, defval_size,
			char *);
	  col_info[i].is_unique_key = is_unique_key;
	  col_info[i].is_primary_key = is_primary_key;

	  if (col_info[i].default_value == NULL)
	    {
	      goto get_column_info_error;
	    }
	}
    }

  if (ret_col_info)
    {
      *ret_col_info = col_info;
    }
  else
    {
      COL_INFO_FREE (col_info, num_col_info);
    }

  return num_col_info;

get_column_info_error:
  COL_INFO_FREE (col_info, num_col_info);
  return CCI_ER_COMMUNICATION;
}

static int
get_shard_key_info (T_NET_RES * net_res)
{
  char is_shard_table_query;
  int num_shard_values;
  int num_shard_key_pos;
  int i;

  if (net_res_to_byte (&is_shard_table_query, net_res) < 0)
    {
      goto get_shard_key_info_error;
    }

  if (net_res_to_int (&num_shard_values, net_res) < 0)
    {
      goto get_shard_key_info_error;
    }
  for (i = 0; i < num_shard_values; i++)
    {
      char *p;
      int size;

      if (net_res_to_str (&p, &size, net_res) < 0)
	{
	  goto get_shard_key_info_error;
	}
    }

  if (net_res_to_int (&num_shard_key_pos, net_res) < 0)
    {
      goto get_shard_key_info_error;
    }
  for (i = 0; i < num_shard_key_pos; i++)
    {
      int pos;

      if (net_res_to_int (&pos, net_res) < 0)
	{
	  goto get_shard_key_info_error;
	}
    }

  return 0;

get_shard_key_info_error:
  return CCI_ER_COMMUNICATION;
}

static int
schema_info_decode (T_NET_RES * net_res, T_REQ_HANDLE * req_handle)
{
  T_CCI_COL_INFO *col_info = NULL;
  int num_tuple;
  int num_col_info;

  num_col_info = get_column_info (net_res, &col_info);
  if (num_col_info < 0)
    {
      assert (col_info == NULL);
      return num_col_info;
    }

  if (get_shard_key_info (net_res) < 0)
    {
      return CCI_ER_COMMUNICATION;
    }

  if (net_res_to_int (&num_tuple, net_res) < 0)
    {
      return CCI_ER_COMMUNICATION;
    }

  req_handle->num_col_info = num_col_info;
  req_handle->col_info = col_info;
  req_handle->num_tuple = num_tuple;

  return 0;
}

static int
bind_value_set (T_CCI_TYPE a_type, char flag, const void *value,
		int length, T_BIND_VALUE * bind_value)
{
  const char *cp_src = value;
  int cp_size;

  if (a_type == CCI_TYPE_VARCHAR || a_type == CCI_TYPE_NUMERIC)
    {
      if (length == UNMEASURED_LENGTH)
	{
	  bind_value->size = strlen (value);
	}
      else
	{
	  bind_value->size = length;
	}

      cp_size = bind_value->size;

      bind_value->size += 1;	/* protocol with cas */
    }
  else if (a_type == CCI_TYPE_INT)
    {
      bind_value->size = 0;
      cp_size = sizeof (int);
    }
  else if (a_type == CCI_TYPE_BIGINT)
    {
      bind_value->size = 0;
      cp_size = sizeof (INT64);
    }
  else if (a_type == CCI_TYPE_DOUBLE)
    {
      bind_value->size = 0;
      cp_size = sizeof (double);
    }
  else if (a_type == CCI_TYPE_VARBIT)
    {
      const T_CCI_VARBIT *bit_value = (const T_CCI_VARBIT *) value;

      bind_value->size = bit_value->size;
      cp_size = bit_value->size;
      cp_src = bit_value->buf;
    }
  else if (a_type == CCI_TYPE_DATE || a_type == CCI_TYPE_TIME ||
	   a_type == CCI_TYPE_DATETIME)
    {
      bind_value->size = 0;
      cp_size = sizeof (T_CCI_DATETIME);
    }
  else
    {
      return CCI_ER_ATYPE;
    }

  if (flag == CCI_BIND_PTR)
    {
      bind_value->alloc_buffer = NULL;
      bind_value->value = value;
    }
  else
    {
      ALLOC_COPY_BIT (bind_value->alloc_buffer, cp_src, cp_size);
      bind_value->value = bind_value->alloc_buffer;
      if (bind_value->value == NULL)
	{
	  return CCI_ER_NO_MORE_MEMORY;
	}
    }

  return 0;
}

static int
bind_value_to_net_buf (T_NET_BUF * net_buf, char type, const void *value,
		       int size)
{
  if (value == NULL)
    {
      type = CCI_TYPE_NULL;
      ADD_ARG_BYTES (net_buf, &type, 1);
      ADD_ARG_BYTES (net_buf, NULL, 0);
      return 0;
    }

  ADD_ARG_BYTES (net_buf, &type, 1);

  switch (type)
    {
    case CCI_TYPE_VARCHAR:
    case CCI_TYPE_NUMERIC:
      ADD_ARG_BIND_STR (net_buf, value, size);
      break;
    case CCI_TYPE_VARBIT:
      ADD_ARG_BYTES (net_buf, value, size);
      break;
    case CCI_TYPE_BIGINT:
      ADD_ARG_BIGINT (net_buf, *((const INT64 *) value));
      break;
    case CCI_TYPE_INT:
      ADD_ARG_INT (net_buf, *((const int *) value));
      break;
    case CCI_TYPE_DOUBLE:
      ADD_ARG_DOUBLE (net_buf, *((const double *) value));
      break;
    case CCI_TYPE_DATE:
    case CCI_TYPE_TIME:
    case CCI_TYPE_DATETIME:
      ADD_ARG_DATETIME (net_buf, value);
      break;

    default:
      ADD_ARG_BYTES (net_buf, NULL, 0);
      break;
    }

  return 0;
}

static int
execute_info_decode (T_NET_RES * net_res, T_REQ_HANDLE * req_handle)
{
  int i;
  T_CCI_QUERY_RESULT *qr;
  char client_cache_reusable;
  char stmt_type;
  int res_count;
  int num_col_types;

  if (net_res_to_byte (&client_cache_reusable, net_res) < 0)
    {
      return CCI_ER_COMMUNICATION;
    }

  qr = (T_CCI_QUERY_RESULT *) MALLOC (sizeof (T_CCI_QUERY_RESULT));
  if (qr == NULL)
    {
      return CCI_ER_NO_MORE_MEMORY;
    }
  memset (qr, 0, sizeof (T_CCI_QUERY_RESULT));

  if (net_res_to_byte (&stmt_type, net_res) < 0 ||
      net_res_to_int (&res_count, net_res) < 0 ||
      net_res_to_int (&num_col_types, net_res) < 0)
    {
      FREE_MEM (qr);
      return CCI_ER_COMMUNICATION;
    }

  qr->stmt_type = stmt_type;
  qr->result_count = res_count;

  if (num_col_types > 0)
    {
      char col_type;
      short scale;
      int precision;

      if (num_col_types != req_handle->num_col_info)
	{
	  assert (0);
	  FREE_MEM (qr);
	  return CCI_ER_COMMUNICATION;
	}

      for (i = 0; i < num_col_types; i++)
	{
	  if (net_res_to_byte (&col_type, net_res) < 0 ||
	      net_res_to_short (&scale, net_res) < 0 ||
	      net_res_to_int (&precision, net_res) < 0)
	    {
	      FREE_MEM (qr);
	      return CCI_ER_COMMUNICATION;
	    }
	  req_handle->col_info[i].type = col_type;
	  req_handle->col_info[i].scale = scale;
	  req_handle->col_info[i].precision = precision;
	}
    }

  req_handle->qr = qr;
  req_handle->num_query_res = 1;

  return CCI_ER_NO_ERROR;
}

static int
execute_batch_info_decode (T_NET_RES * net_res, char flag,
			   T_CCI_QUERY_RESULT ** res_qr)
{
  int num_query;
  int i;
  T_CCI_QUERY_RESULT *qr;

  if (net_res_to_int (&num_query, net_res) < 0 || num_query < 0)
    {
      return CCI_ER_COMMUNICATION;
    }

  if (num_query == 0)
    {
      assert (0);
      *res_qr = NULL;

      return num_query;
    }

  qr =
    (T_CCI_QUERY_RESULT *) MALLOC (sizeof (T_CCI_QUERY_RESULT) * num_query);
  if (qr == NULL)
    {
      return CCI_ER_NO_MORE_MEMORY;
    }

  memset (qr, 0, sizeof (T_CCI_QUERY_RESULT) * num_query);

  for (i = 0; i < num_query; i++)
    {
      char exec_status;

      if (net_res_to_byte (&exec_status, net_res) < 0)
	{
	  qe_query_result_free (i, qr);
	  return CCI_ER_COMMUNICATION;
	}

      if (exec_status == EXEC_QUERY_ERROR)
	{
	  int srv_err_indicator;
	  int srv_err_code;
	  int srv_err_msg_size;
	  char *srv_err_msg_p;

	  if (net_res_to_int (&srv_err_indicator, net_res) < 0 ||
	      net_res_to_int (&srv_err_code, net_res) < 0 ||
	      net_res_to_str (&srv_err_msg_p, &srv_err_msg_size, net_res) < 0)
	    {
	      qe_query_result_free (i, qr);
	      return CCI_ER_COMMUNICATION;
	    }

	  qr[i].result_count = srv_err_code;
	  qr[i].err_no = srv_err_code;;

	  if (srv_err_msg_p != NULL)
	    {
	      ALLOC_N_COPY (qr[i].err_msg, srv_err_msg_p, srv_err_msg_size,
			    char *);
	    }
	}
      else
	{
	  int res_count;

	  if (flag == EXECUTE_BATCH)
	    {
	      char stmt_type;

	      if (net_res_to_byte (&stmt_type, net_res) < 0)
		{
		  qe_query_result_free (i, qr);
		  return CCI_ER_COMMUNICATION;
		}

	      qr[i].stmt_type = stmt_type;
	    }

	  if (net_res_to_int (&res_count, net_res) < 0)
	    {
	      qe_query_result_free (i, qr);
	      return CCI_ER_COMMUNICATION;
	    }

	  qr[i].result_count = res_count;
	}
    }

  *res_qr = qr;

  return num_query;
}

static int
decode_fetch_result (T_REQ_HANDLE * req_handle, T_NET_RES * net_res)
{
  int num_cols;
  int num_tuple;

  num_cols = req_handle->num_col_info;

  num_tuple = fetch_info_decode (net_res, num_cols,
				 &(req_handle->tuple_value), req_handle);
  if (num_tuple < 0)
    {
      return num_tuple;
    }

  if (num_tuple == 0)
    {
      req_handle->fetched_tuple_begin = 0;
      req_handle->fetched_tuple_end = 0;
      req_handle->net_res = net_res;
      req_handle->cur_fetch_tuple_index = -1;
    }
  else
    {
      req_handle->fetched_tuple_begin = req_handle->cursor_pos;
      req_handle->fetched_tuple_end = req_handle->cursor_pos + num_tuple - 1;
      req_handle->net_res = net_res;
      req_handle->cur_fetch_tuple_index = 0;
    }

  return num_tuple;
}

int
qe_update_db_group_id (T_CON_HANDLE * con_handle, int migrator_id,
		       int group_id, int target, bool on_off)
{
  T_NET_BUF net_buf;
  char func_code = CAS_FC_UPDATE_GROUP_ID;
  int err_code;

  if (IS_INVALID_SOCKET (con_handle->sock_fd))
    {
      con_handle->con_status = CCI_CON_STATUS_OUT_TRAN;
      return CCI_ER_COMMUNICATION;
    }

  net_buf_init (&net_buf);

  net_buf_cp_str (&net_buf, &func_code, 1);
  ADD_ARG_INT (&net_buf, migrator_id);
  ADD_ARG_INT (&net_buf, group_id);
  ADD_ARG_INT (&net_buf, target);
  ADD_ARG_INT (&net_buf, on_off);

  if (net_buf.err_code < 0)
    {
      err_code = net_buf.err_code;
      net_buf_clear (&net_buf);
      return err_code;
    }

  err_code = net_send_msg (con_handle, net_buf.data, net_buf.data_size);
  net_buf_clear (&net_buf);
  if (err_code < 0)
    {
      return err_code;
    }

  err_code = net_recv_msg (con_handle, NULL);
  return err_code;
}

int
qe_insert_gid_removed_info (T_CON_HANDLE * con_handle, int group_id)
{
  T_NET_BUF net_buf;
  char func_code = CAS_FC_INSERT_GID_REMOVED_INFO;
  int err_code;

  if (IS_INVALID_SOCKET (con_handle->sock_fd))
    {
      con_handle->con_status = CCI_CON_STATUS_OUT_TRAN;
      return CCI_ER_COMMUNICATION;
    }

  net_buf_init (&net_buf);

  net_buf_cp_str (&net_buf, &func_code, 1);
  ADD_ARG_INT (&net_buf, group_id);

  if (net_buf.err_code < 0)
    {
      err_code = net_buf.err_code;
      net_buf_clear (&net_buf);
      return err_code;
    }

  err_code = net_send_msg (con_handle, net_buf.data, net_buf.data_size);
  net_buf_clear (&net_buf);
  if (err_code < 0)
    {
      return err_code;
    }

  err_code = net_recv_msg (con_handle, NULL);
  return err_code;
}

int
qe_delete_gid_removed_info (T_CON_HANDLE * con_handle, int group_id)
{
  T_NET_BUF net_buf;
  char func_code = CAS_FC_DELETE_GID_REMOVED_INFO;
  int err_code;

  if (IS_INVALID_SOCKET (con_handle->sock_fd))
    {
      con_handle->con_status = CCI_CON_STATUS_OUT_TRAN;
      return CCI_ER_COMMUNICATION;
    }

  net_buf_init (&net_buf);

  net_buf_cp_str (&net_buf, &func_code, 1);
  ADD_ARG_INT (&net_buf, group_id);

  if (net_buf.err_code < 0)
    {
      err_code = net_buf.err_code;
      net_buf_clear (&net_buf);
      return err_code;
    }

  err_code = net_send_msg (con_handle, net_buf.data, net_buf.data_size);
  net_buf_clear (&net_buf);
  if (err_code < 0)
    {
      return err_code;
    }

  err_code = net_recv_msg (con_handle, NULL);
  return err_code;
}

int
qe_delete_gid_skey_info (T_CON_HANDLE * con_handle, int group_id)
{
  T_NET_BUF net_buf;
  char func_code = CAS_FC_DELETE_GID_SKEY_INFO;
  int err_code;

  if (IS_INVALID_SOCKET (con_handle->sock_fd))
    {
      con_handle->con_status = CCI_CON_STATUS_OUT_TRAN;
      return CCI_ER_COMMUNICATION;
    }

  net_buf_init (&net_buf);

  net_buf_cp_str (&net_buf, &func_code, 1);
  ADD_ARG_INT (&net_buf, group_id);

  if (net_buf.err_code < 0)
    {
      err_code = net_buf.err_code;
      net_buf_clear (&net_buf);
      return err_code;
    }

  err_code = net_send_msg (con_handle, net_buf.data, net_buf.data_size);
  net_buf_clear (&net_buf);
  if (err_code < 0)
    {
      return err_code;
    }

  err_code = net_recv_msg (con_handle, NULL);
  return err_code;
}

int
qe_block_global_dml (T_CON_HANDLE * con_handle, bool start_or_end)
{
  T_NET_BUF net_buf;
  char func_code = CAS_FC_BLOCK_GLOBAL_DML;
  int err_code;

  if (IS_INVALID_SOCKET (con_handle->sock_fd))
    {
      con_handle->con_status = CCI_CON_STATUS_OUT_TRAN;
      return CCI_ER_COMMUNICATION;
    }

  net_buf_init (&net_buf);

  net_buf_cp_str (&net_buf, &func_code, 1);
  ADD_ARG_INT (&net_buf, start_or_end);

  if (net_buf.err_code < 0)
    {
      err_code = net_buf.err_code;
      net_buf_clear (&net_buf);
      return err_code;
    }

  err_code = net_send_msg (con_handle, net_buf.data, net_buf.data_size);
  net_buf_clear (&net_buf);
  if (err_code < 0)
    {
      return err_code;
    }

  err_code = net_recv_msg (con_handle, NULL);
  return err_code;
}

int
qe_get_server_mode (T_CON_HANDLE * con_handle, int *mode,
		    unsigned int *master_addr)
{
  T_NET_BUF net_buf;
  char func_code = CAS_FC_SERVER_MODE;
  int err_code;
  T_NET_RES *net_res = NULL;

  if (IS_INVALID_SOCKET (con_handle->sock_fd))
    {
      return CCI_ER_COMMUNICATION;
    }

  net_buf_init (&net_buf);

  net_buf_cp_str (&net_buf, &func_code, 1);

  if (net_buf.err_code < 0)
    {
      err_code = net_buf.err_code;
      net_buf_clear (&net_buf);
      return err_code;
    }

  err_code = net_send_msg (con_handle, net_buf.data, net_buf.data_size);
  net_buf_clear (&net_buf);

  if (err_code < 0)
    {
      return err_code;
    }

  err_code = net_recv_msg (con_handle, &net_res);
  if (err_code < 0)
    {
      return err_code;
    }

  if (net_res_to_int (mode, net_res) < 0 ||
      net_res_to_int ((int *) master_addr, net_res) < 0)
    {
      err_code = CCI_ER_COMMUNICATION;
    }

  FREE_MEM (net_res);

  return err_code;
}

static int
add_arg_idxkey (T_NET_BUF * net_buf, DB_IDXKEY * idxkey)
{
  int size;
  char *buf = NULL;
  int err_code = CCI_ER_NO_ERROR;

  size = cci_or_db_idxkey_size (idxkey) + MAX_ALIGNMENT;
  buf = (char *) MALLOC (size);
  if (buf == NULL)
    {
      err_code = CCI_ER_NO_MORE_MEMORY;
      return err_code;
    }

  /* pk_buf is  MAX_ALIGNMENT */
  if (cci_or_pack_db_idxkey (buf, idxkey) == NULL)
    {
      err_code = CCI_ER_NO_MORE_MEMORY;
      FREE_MEM (buf);

      return err_code;
    }
  ADD_ARG_STR (net_buf, buf, size);

  FREE_MEM (buf);

  return err_code;
}

/*
 *
 */
static int
add_repl_item_ddl (T_NET_BUF * net_buf, RP_DDL_ITEM * ddl)
{
  int err_code = CCI_ER_NO_ERROR;

  /* ddl info */
  ADD_ARG_INT (net_buf, ddl->stmt_type);
  ADD_ARG_STR (net_buf, ddl->db_user, strlen (ddl->db_user) + 1);
  ADD_ARG_STR (net_buf, ddl->query, strlen (ddl->query) + 1);

  if (net_buf->err_code < 0)
    {
      err_code = net_buf->err_code;
    }

  return err_code;
}

/*
 *
 */
static int
add_repl_item_catalog (T_NET_BUF * net_buf, RP_CATALOG_ITEM * catalog)
{
  assert (catalog->copyarea_op == LC_FLUSH_HA_CATALOG_ANALYZER_UPDATE
	  || catalog->copyarea_op == LC_FLUSH_HA_CATALOG_APPLIER_UPDATE);
  assert (catalog->recdes != NULL);
  assert (!cci_db_idxkey_is_null (&catalog->key));

  ADD_ARG_STR (net_buf, catalog->class_name,
	       strlen (catalog->class_name) + 1);
  ADD_ARG_IDXKEY (net_buf, &catalog->key);
  ADD_ARG_INT (net_buf, catalog->copyarea_op);
  ADD_ARG_STR (net_buf, catalog->recdes->data, catalog->recdes->length);

  return CCI_ER_NO_ERROR;
}

/*
 *
 */
static int
add_repl_item_data (T_NET_BUF * net_buf, RP_DATA_ITEM * data)
{
  assert (data->class_name != NULL);
  assert (data->rcv_index == RVREPL_DATA_INSERT
	  || data->rcv_index == RVREPL_DATA_UPDATE
	  || data->rcv_index == RVREPL_DATA_DELETE);
  assert (!cci_db_idxkey_is_null (&data->key));

  ADD_ARG_STR (net_buf, data->class_name, strlen (data->class_name) + 1);
  ADD_ARG_IDXKEY (net_buf, &data->key);
  ADD_ARG_INT (net_buf, data->rcv_index);

  if (data->rcv_index == RVREPL_DATA_DELETE)
    {
      assert (data->recdes == NULL);
      ADD_ARG_STR (net_buf, NULL, 0);
    }
  else
    {
      assert (data->recdes != NULL);
      ADD_ARG_STR (net_buf, data->recdes->data, data->recdes->length);
    }

  return CCI_ER_NO_ERROR;
}

int
qe_send_repl_data (T_CON_HANDLE * con_handle, CIRP_REPL_ITEM * head,
		   int num_items)
{
  T_NET_BUF net_buf;
  char func_code = CAS_FC_SEND_REPL_DATA;
  int err_code = CCI_ER_NO_ERROR;
  CIRP_REPL_ITEM *item;
  char note[ONE_K];
  int item_count;

  if (IS_INVALID_SOCKET (con_handle->sock_fd))
    {
      con_handle->con_status = CCI_CON_STATUS_OUT_TRAN;
      return CCI_ER_COMMUNICATION;
    }

  net_buf_init (&net_buf);

  net_buf_cp_str (&net_buf, &func_code, 1);

  snprintf (note, sizeof (note), "start repl transaction");
  ADD_ARG_STR (&net_buf, note, strlen (note) + 1);
  ADD_ARG_INT (&net_buf, con_handle->autocommit_mode);
  switch (head->item_type)
    {
    case RP_ITEM_TYPE_DDL:
      ADD_ARG_INT (&net_buf, RP_TRAN_TYPE_DDL);
      break;
    case RP_ITEM_TYPE_DATA:
      ADD_ARG_INT (&net_buf, RP_TRAN_TYPE_DATA);
      break;
    case RP_ITEM_TYPE_CATALOG:
      ADD_ARG_INT (&net_buf, RP_TRAN_TYPE_CATALOG);
      break;
    default:
      assert (false);
      return CCI_ER_INVALID_ARGS;
    }
  ADD_ARG_INT (&net_buf, num_items);

  item = head;
  item_count = 0;
  while (item != NULL && err_code == CCI_ER_NO_ERROR)
    {
      ADD_ARG_INT (&net_buf, item->item_type);
      switch (item->item_type)
	{
	case RP_ITEM_TYPE_CATALOG:
	  err_code = add_repl_item_catalog (&net_buf, &item->info.catalog);
	  break;
	case RP_ITEM_TYPE_DDL:
	  err_code = add_repl_item_ddl (&net_buf, &item->info.ddl);
	  break;
	case RP_ITEM_TYPE_DATA:
	  err_code = add_repl_item_data (&net_buf, &item->info.data);
	  break;
	default:
	  assert (false);
	  return CCI_ER_INVALID_ARGS;
	}

      item = item->next;
      item_count++;
    }
  if (num_items != item_count)
    {
      assert (false);
      err_code = CCI_ER_INVALID_ARGS;
    }

  if (err_code < 0 || net_buf.err_code < 0)
    {
      if (err_code == CCI_ER_NO_ERROR)
	{
	  err_code = net_buf.err_code;
	}
      net_buf_clear (&net_buf);

      return err_code;
    }

  err_code = net_send_msg (con_handle, net_buf.data, net_buf.data_size);
  net_buf_clear (&net_buf);
  if (err_code < 0)
    {
      return err_code;
    }

  err_code = net_recv_msg (con_handle, NULL);

  return err_code;
}

int
qe_notify_ha_agent_state (T_CON_HANDLE * con_handle,
			  in_addr_t ip, int port, int state)
{
  T_NET_BUF net_buf;
  char func_code = CAS_FC_NOTIFY_HA_AGENT_STATE;
  int err_code;

  if (IS_INVALID_SOCKET (con_handle->sock_fd))
    {
      con_handle->con_status = CCI_CON_STATUS_OUT_TRAN;
      return CCI_ER_COMMUNICATION;
    }

  net_buf_init (&net_buf);

  net_buf_cp_str (&net_buf, &func_code, 1);
  ADD_ARG_INT (&net_buf, ip);
  ADD_ARG_INT (&net_buf, port);
  ADD_ARG_INT (&net_buf, state);
  ADD_ARG_INT (&net_buf, con_handle->autocommit_mode);

  if (net_buf.err_code < 0)
    {
      err_code = net_buf.err_code;
      net_buf_clear (&net_buf);
      return err_code;
    }

  err_code = net_send_msg (con_handle, net_buf.data, net_buf.data_size);
  net_buf_clear (&net_buf);
  if (err_code < 0)
    {
      return err_code;
    }

  err_code = net_recv_msg (con_handle, NULL);

  return err_code;
}

int
qe_change_dbuser (T_CON_HANDLE * con_handle, const char *user,
		  const char *passwd)
{
  char *dbuser, *dbpasswd;
  int err_code = CCI_ER_NO_ERROR;;
  char func_code = CAS_FC_CHANGE_DBUSER;
  T_NET_BUF net_buf;
  T_NET_RES *net_res;
  int server_start_time = 0;

  ALLOC_COPY_STR (dbuser, user);
  ALLOC_COPY_STR (dbpasswd, passwd);

  if (dbuser == NULL || dbpasswd == NULL)
    {
      FREE_MEM (dbuser);
      FREE_MEM (dbpasswd);
      err_code = CCI_ER_NO_MORE_MEMORY;
      goto change_dbuser_fail;
    }

  FREE_MEM (con_handle->db_user);
  con_handle->db_user = dbuser;
  FREE_MEM (con_handle->db_passwd);
  con_handle->db_passwd = dbpasswd;

  hm_invalidate_all_req_handle (con_handle);

  if (IS_INVALID_SOCKET (con_handle->sock_fd))
    {
      return CCI_ER_NO_ERROR;
    }

  net_buf_init (&net_buf);
  net_buf_cp_str (&net_buf, &func_code, 1);
  ADD_ARG_STR (&net_buf, user, strlen (user) + 1);
  ADD_ARG_STR (&net_buf, passwd, strlen (passwd) + 1);

  if (net_buf.err_code < 0)
    {
      err_code = net_buf.err_code;
      net_buf_clear (&net_buf);
      goto change_dbuser_fail;
    }

  err_code = net_send_msg (con_handle, net_buf.data, net_buf.data_size);
  net_buf_clear (&net_buf);

  if (err_code < 0)
    {
      goto change_dbuser_fail;
    }

  err_code = net_recv_msg (con_handle, &net_res);
  if (err_code < 0)
    {
      goto change_dbuser_fail;
    }

  if (net_res_to_int (&server_start_time, net_res) < 0)
    {
      FREE_MEM (net_res);
      return CCI_ER_COMMUNICATION;
    }

  FREE_MEM (net_res);

  if (con_handle->con_property.error_on_server_restart &&
      con_handle->cas_connect_info.server_start_time > 0 &&
      con_handle->cas_connect_info.server_start_time != server_start_time)
    {
      con_handle->cas_connect_info.server_start_time = 0;
      return CCI_ER_SERVER_RESTARTED;
    }

  return CCI_ER_NO_ERROR;

change_dbuser_fail:
  CLOSE_SOCKET (con_handle->sock_fd);
  con_handle->sock_fd = INVALID_SOCKET;
  assert (err_code != CCI_ER_NO_ERROR);
  return err_code;
}
