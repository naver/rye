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
 * cas_cci.c -
 */

#ident "$Id$"

/************************************************************************
 * IMPORTED SYSTEM HEADER FILES                                         *
 ************************************************************************/
#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <assert.h>
#include <sys/timeb.h>
#include <stdarg.h>

#include <unistd.h>
#include <pthread.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <signal.h>
#include <sys/socket.h>
#include <arpa/inet.h>

/************************************************************************
 * OTHER IMPORTED HEADER FILES                                          *
 ************************************************************************/

#include "porting.h"
#include "cci_common.h"
#include "cas_cci.h"
#include "cas_cci_internal.h"
#include "cci_handle_mng.h"
#include "cci_network.h"
#include "cci_query_execute.h"
#include "cas_protocol.h"
#include "cci_net_buf.h"
#include "cci_util.h"
#include "cci_log.h"
#include "error_code.h"


/************************************************************************
 * PRIVATE DEFINITIONS                                                  *
 ************************************************************************/

#define ELAPSED_MSECS(e, s) \
    ((e).tv_sec - (s).tv_sec) * 1000 + ((e).tv_usec - (s).tv_usec) / 1000

#define IS_OUT_TRAN_STATUS(CON_HANDLE) \
        (IS_INVALID_SOCKET((CON_HANDLE)->sock_fd) || \
         ((CON_HANDLE)->con_status == CCI_CON_STATUS_OUT_TRAN))

#define IS_OUT_TRAN(c) ((c)->con_status == CCI_CON_STATUS_OUT_TRAN)
#define IS_IN_TRAN(c) ((c)->con_status == CCI_CON_STATUS_IN_TRAN)
#define IS_FORCE_FAILBACK(c) ((c)->force_failback == 1)
#define IS_ER_COMMUNICATION(e) \
  ((e) == CCI_ER_COMMUNICATION || (e) == CAS_ER_COMMUNICATION)
#define IS_SERVER_DOWN(e) \
  (((e) == ER_TM_SERVER_DOWN_UNILATERALLY_ABORTED) \
   || ((e) == ER_OBJ_NO_CONNECT) || ((e) == ER_NET_SERVER_CRASHED) \
   || ((e) == ER_BO_CONNECT_FAILED))
#define IS_ER_TO_RECONNECT(e1, e2) \
  (((e1) == CCI_ER_DBMS && IS_SERVER_DOWN (e2)) ? true : IS_ER_COMMUNICATION (e1))

#define NEED_TO_RECONNECT(CON,ERR)	(IS_ER_COMMUNICATION(ERR))

#define RECONNECT_RETRY_COUNT	2

/* default value of each datesource property */
#define CCI_DS_POOL_SIZE_DEFAULT                        10
#define CCI_DS_MAX_WAIT_DEFAULT                         1000
#define CCI_DS_POOL_PREPARED_STATEMENT_DEFAULT          false
#define CCI_DS_MAX_OPEN_PREPARED_STATEMENT_DEFAULT      1000
#define CCI_DS_DISCONNECT_ON_QUERY_TIMEOUT_DEFAULT      false
#define CCI_DS_DEFAULT_AUTOCOMMIT_DEFAULT               (CCI_AUTOCOMMIT_TRUE)
#define CCI_DS_DEFAULT_ISOLATION_DEFAULT                TRAN_UNKNOWN_ISOLATION
#define CCI_DS_DEFAULT_LOCK_TIMEOUT_DEFAULT             CCI_LOCK_TIMEOUT_DEFAULT
#define CCI_DS_LOGIN_TIMEOUT_DEFAULT			(CCI_LOGIN_TIMEOUT_DEFAULT)

/* flag of con_api_pre() */
#define CON_API_PRE_FLAG_SHARD_ADMIN		1
#define CON_API_PRE_FLAG_INIT_STMT		2
#define CON_API_PRE_FLAG_FORCE_CONN		4

#define CON_API_PRE(CONN, STMT, RET_CON_HANDLE, FLAG)	\
	con_api_pre (CONN, STMT, RET_CON_HANDLE, FLAG, __func__)

#define STMT_API_PRE(STMT, RET_CON_HANDLE, RET_REQ_HANDLE) \
	stmt_api_pre (STMT, RET_CON_HANDLE, RET_REQ_HANDLE, __func__)

#define CON_STMT_API_POST(CONN, STMT, CON_HANDLE, ERROR)	\
	con_stmt_api_post (CONN, STMT, CON_HANDLE, ERROR, __func__)

/************************************************************************
 * PRIVATE FUNCTION PROTOTYPES                                          *
 ************************************************************************/

static void reset_error_buffer (T_CCI_ERROR * err_buf);
static int cas_connect (T_CON_HANDLE * con_handle);
static int cas_connect_with_ret (T_CON_HANDLE * con_handle, int *connect);
static int cas_connect_internal (T_CON_HANDLE * con_handle, int *connect);

static const char *cci_get_err_msg_internal (int error);

static T_CON_HANDLE *get_new_connection (T_ALTER_HOST * alter_hosts,
					 const char *server_list,
					 const char *port_name,
					 const char *dbname,
					 const char *dbuser,
					 const char *dbpasswd,
					 const char *property_str,
					 const T_CON_PROPERTY * con_property);
static void set_error_buffer (T_CCI_ERROR * err_buf_p,
			      int error, const char *message, ...);
static void copy_error_buffer (T_CCI_ERROR * dest_err_buf_p,
			       const T_CCI_ERROR * src_err_buf_p);
static int cci_end_tran_internal (T_CON_HANDLE * con_handle, char type);
static void get_last_error (const T_CON_HANDLE * con_handle,
			    T_CCI_ERROR * dest_err_buf);
static int con_api_pre (CCI_CONN * conn, CCI_STMT * stmt,
			T_CON_HANDLE ** ret_con_handle, int flag,
			const char *func_name);
static int stmt_api_pre (CCI_STMT * stmt, T_CON_HANDLE ** ret_con_handle,
			 T_REQ_HANDLE ** ret_req_handle,
			 const char *func_name);
static int con_stmt_api_post (CCI_CONN * conn, CCI_STMT * stmt,
			      T_CON_HANDLE * con_handle, int error,
			      const char *func_name);
static int cci_execute_internal (CCI_STMT * stmt, char flag,
				 int max_col_size, int gid);

/************************************************************************
 * INTERFACE VARIABLES                                                  *
 ************************************************************************/
/************************************************************************
 * PUBLIC VARIABLES                                                     *
 ************************************************************************/
/************************************************************************
 * PRIVATE VARIABLES                                                    *
 ************************************************************************/
static const char *cci_Build_version = MAKE_STR (BUILD_NUMBER);

static T_MUTEX con_handle_table_mutex = PTHREAD_MUTEX_INITIALIZER;
static T_MUTEX health_check_th_mutex = PTHREAD_MUTEX_INITIALIZER;

static char init_flag = 0;
static char is_health_check_th_started = 0;
static int cci_SIGPIPE_ignore = 0;

CCI_MALLOC_FUNCTION cci_malloc = malloc;
CCI_CALLOC_FUNCTION cci_calloc = calloc;
CCI_REALLOC_FUNCTION cci_realloc = realloc;
CCI_FREE_FUNCTION cci_free = free;


/************************************************************************
 * IMPLEMENTATION OF INTERFACE FUNCTIONS                                *
 ************************************************************************/

int
cci_get_version_string (char *str, size_t len)
{
  if (str)
    {
      snprintf (str, len, "%s", cci_Build_version);
    }
  return 0;
}

int
cci_get_version (int *major, int *minor, int *patch)
{
  if (major)
    {
      *major = MAJOR_VERSION;
    }
  if (minor)
    {
      *minor = MINOR_VERSION;
    }
  if (patch)
    {
      *patch = PATCH_VERSION;
    }
  return 0;
}

static int
cci_connect_internal (CCI_CONN * conn, const char *url, const char *user,
		      const char *pass, T_CON_HANDLE ** ret_con_handle)
{
  char *token[MAX_URL_MATCH_COUNT] = { NULL };
  int error = CCI_ER_NO_ERROR;
  unsigned i;

  char *url_server_info, *url_dbname;
  char *url_dbuser, *url_dbpasswd;
  char *url_port_name;
  const char *url_property;
  T_ALTER_HOST *alter_hosts = NULL;
  T_CON_HANDLE *con_handle = NULL;
  T_CON_PROPERTY con_property;

  reset_error_buffer (&conn->err_buf);

  if (url == NULL)
    {
      set_error_buffer (&conn->err_buf, CCI_ER_CONNECT, NULL);
      return CCI_ER_CONNECT;
    }

  /* The NULL is same as "". */
  if (user == NULL)
    {
      user = "";
    }

  if (pass == NULL)
    {
      pass = "";
    }

  error = cci_url_match (url, token);
  if (error != CCI_ER_NO_ERROR)
    {
      set_error_buffer (&conn->err_buf, error, NULL);
      return error;
    }

  url_server_info = token[0];
  url_dbname = token[1];
  url_dbuser = token[2];
  url_dbpasswd = token[3];
  url_port_name = token[4];
  url_property = token[5];

  ut_tolower (trim (url_port_name));

  if (*user == '\0' && url_dbuser != NULL)
    {
      user = url_dbuser + 1;
    }
  if (*pass == '\0' && url_dbpasswd != NULL)
    {
      pass = url_dbpasswd + 1;
    }

  if (url_property == NULL)
    {
      url_property = "";
    }

  if (user[0] == '\0')
    {
      /* A user don't exist in the parameter and url */
      user = URL_DEFAULT_DBUSER;
    }

  error = cci_url_get_properties (&con_property, url_property);
  if (error < 0)
    {
      set_error_buffer (&conn->err_buf, error, NULL);
      return error;
    }

  error = cci_url_get_althosts (&alter_hosts, url_server_info,
				con_property.load_balance);
  if (error < 0)
    {
      con_property_free (&con_property);
      set_error_buffer (&conn->err_buf, error, NULL);
      return error;
    }

  /* start health check thread */
  MUTEX_LOCK (health_check_th_mutex);
  if (!is_health_check_th_started)
    {
      hm_create_health_check_th ();
      is_health_check_th_started = 1;
    }
  MUTEX_UNLOCK (health_check_th_mutex);

  con_handle = get_new_connection (alter_hosts, url_server_info,
				   url_port_name, url_dbname, user, pass,
				   url_property, &con_property);
  if (con_handle == NULL)
    {
      FREE_MEM (alter_hosts);

      for (i = 0; i < MAX_URL_MATCH_COUNT; i++)
	{
	  FREE_MEM (token[i]);
	}

      con_property_free (&con_property);
      set_error_buffer (&conn->err_buf, CCI_ER_CON_HANDLE, NULL);
      return CCI_ER_CON_HANDLE;
    }

  reset_error_buffer (&con_handle->err_buf);

  SET_START_TIME_FOR_LOGIN (con_handle);
  error = cas_connect (con_handle);
  if (error < 0)
    {
      get_last_error (con_handle, &conn->err_buf);
      hm_con_handle_free (con_handle);
      goto ret;
    }

  error = qe_end_tran (con_handle, CCI_TRAN_COMMIT);
  if (error < 0)
    {
      get_last_error (con_handle, &conn->err_buf);
      hm_con_handle_free (con_handle);
      goto ret;
    }

  SET_AUTOCOMMIT_FROM_CASINFO (con_handle);
  RESET_START_TIME (con_handle);

  *ret_con_handle = con_handle;

ret:
  for (i = 0; i < MAX_URL_MATCH_COUNT; i++)
    {
      FREE_MEM (token[i]);
    }

  set_error_buffer (&conn->err_buf, error, NULL);

  return error;
}

int
cci_connect (CCI_CONN * conn, char *url, const char *user,
	     const char *password)
{
  T_CON_HANDLE *con_handle = NULL;
  int error;

  hm_set_conn_handle_id (conn, NULL);

  if (conn == NULL)
    {
      return CCI_ER_INVALID_ARGS;
    }

  error = cci_connect_internal (conn, url, user, password, &con_handle);
  if (error < 0)
    {
      return error;
    }

  hm_set_conn_handle_id (conn, con_handle);

  API_ELOG (con_handle, NULL, error, "cci_connect");

  return CCI_ER_NO_ERROR;
}

int
cci_disconnect (CCI_CONN * conn)
{
  int error = CCI_ER_NO_ERROR;
  T_CON_HANDLE *con_handle = NULL;

  error = CON_API_PRE (conn, NULL, &con_handle, 0);
  if (error < 0)
    {
      return error;
    }

  error = qe_con_close (con_handle);

  /* con_handle will be freed so con_handle->used flag keep true.
   * 3rd parameter of CON_STMT_API_POST should be NULL */
  CON_STMT_API_POST (conn, NULL, NULL, error);

  hm_con_handle_free (con_handle);

  hm_set_conn_handle_id (conn, NULL);

  return error;
}

int
cci_cancel (CCI_CONN * conn)
{
  T_CON_HANDLE *con_handle = NULL;
  int error;

  error = CON_API_PRE (conn, NULL, &con_handle, CON_API_PRE_FLAG_FORCE_CONN);
  if (error < 0)
    {
      return error;
    }

  error = net_cancel_request (con_handle);

  /* cci_cancel request do not set error buffuer
   * not to overwrite error msg written by cci_execute() */
  CON_STMT_API_POST (NULL, NULL, NULL, error);

  return error;
}

static int
cci_end_tran_internal (T_CON_HANDLE * con_handle, char type)
{
  int error = CCI_ER_NO_ERROR;

  if (IS_IN_TRAN (con_handle))
    {
      error = qe_end_tran (con_handle, type);
    }
  else if (type == CCI_TRAN_ROLLBACK)
    {
      /* even if con status is CCI_CON_STATUS_OUT_TRAN, there may be holdable
       * req_handles that remained open after commit
       * if a rollback is done after commit, these req_handles should be
       * closed or freed
       */
      if (IS_CAS_STATEMENT_POOLING (con_handle))
	{
	  hm_req_handle_close_all_resultsets (con_handle);
	}
      else
	{
	  hm_req_handle_free_all (con_handle);
	}
    }

  if (IS_OUT_TRAN (con_handle))
    {
      hm_check_rc_time (con_handle);
    }

  return error;
}

int
cci_end_tran (CCI_CONN * conn, char type)
{
  int error = CCI_ER_NO_ERROR;
  T_CON_HANDLE *con_handle = NULL;

  error = CON_API_PRE (conn, NULL, &con_handle, 0);
  if (error < 0)
    {
      return error;
    }

  error = cci_end_tran_internal (con_handle, type);

  CON_STMT_API_POST (conn, NULL, con_handle, error);

  return error;
}

static int
reset_connect (T_CON_HANDLE * con_handle, T_REQ_HANDLE * req_handle)
{
  int connect_done;
  int error;
  int old_timeout;

  reset_error_buffer (&con_handle->err_buf);
  if (req_handle != NULL)
    {
      req_handle_content_free (req_handle, 1);
    }

  /* save query timeout */
  old_timeout = con_handle->current_timeout;
  if (con_handle->current_timeout <= 0)
    {
      /* if (query_timeout <= 0) */
      con_handle->current_timeout = con_handle->con_property.login_timeout;
    }
  error = cas_connect_with_ret (con_handle, &connect_done);

  /* restore query timeout */
  con_handle->current_timeout = old_timeout;
  if (error < 0 || !connect_done)
    {
      return error;
    }

  return CCI_ER_NO_ERROR;
}

/*
 * For the purpose of re-balancing existing connections, cci_prepare,
 * cci_execute, cci_execute_batch, cci_prepare_and_execute
 * require to forcefully disconnect the current
 * con_handle when it is in the OUT_TRAN state and the time elapsed
 * after the last failure of a host is over rc_time.
 */
int
cci_prepare (CCI_CONN * conn, CCI_STMT * stmt, const char *sql_stmt,
	     char flag)
{
  int error = CCI_ER_NO_ERROR;
  T_CON_HANDLE *con_handle = NULL;
  T_REQ_HANDLE *req_handle = NULL;
  int is_first_prepare_in_tran;
  int retry_count = RECONNECT_RETRY_COUNT;

  hm_set_stmt_handle_id (stmt, NULL, NULL);

  error = CON_API_PRE (conn, stmt, &con_handle, CON_API_PRE_FLAG_INIT_STMT);
  if (error < 0)
    {
      return error;
    }

  if (sql_stmt == NULL)
    {
      error = CCI_ER_STRING_PARAM;
      goto error;
    }

  if (con_handle->con_property.log_trace_api)
    {
      CCI_LOGF_DEBUG (con_handle->con_logger, "FLAG[%d],SQL[%s]", flag,
		      sql_stmt);
    }

  error = hm_req_handle_alloc (con_handle, &req_handle);
  if (error < 0)
    {
      goto prepare_error;
    }

  if (IS_OUT_TRAN (con_handle) && IS_FORCE_FAILBACK (con_handle)
      && !IS_INVALID_SOCKET (con_handle->sock_fd))
    {
      hm_force_close_connection (con_handle);
    }
  SET_START_TIME_FOR_QUERY (con_handle, req_handle);

  is_first_prepare_in_tran = IS_OUT_TRAN (con_handle);

  error = qe_prepare (req_handle, con_handle, sql_stmt, flag, 0);

  while ((retry_count-- > 0)
	 && (IS_OUT_TRAN (con_handle) || is_first_prepare_in_tran)
	 && IS_ER_TO_RECONNECT (error, con_handle->err_buf.err_code))
    {
      if (NEED_TO_RECONNECT (con_handle, error))
	{
	  /* Finally, reset_connect will return ER_TIMEOUT */
	  error = reset_connect (con_handle, req_handle);
	  if (error != CCI_ER_NO_ERROR)
	    {
	      break;
	    }
	}

      error = qe_prepare (req_handle, con_handle, sql_stmt, flag, 0);
    }

  if (error < 0)
    {
      goto prepare_error;
    }

  RESET_START_TIME (con_handle);

  hm_set_stmt_handle_id (stmt, con_handle, req_handle);

  CON_STMT_API_POST (conn, stmt, con_handle, CCI_ER_NO_ERROR);

  return CCI_ER_NO_ERROR;

prepare_error:
  RESET_START_TIME (con_handle);

  if (req_handle)
    {
      hm_req_handle_free (con_handle, req_handle);
      req_handle = NULL;
    }

  if (error == CCI_ER_QUERY_TIMEOUT &&
      con_handle->con_property.disconnect_on_query_timeout)
    {
      CLOSE_SOCKET (con_handle->sock_fd);
      con_handle->sock_fd = INVALID_SOCKET;
      con_handle->con_status = CCI_CON_STATUS_OUT_TRAN;
    }

  if (IS_OUT_TRAN (con_handle))
    {
      hm_check_rc_time (con_handle);
    }

error:

  CON_STMT_API_POST (conn, stmt, con_handle, error);

  return error;
}

int
cci_get_bind_num (CCI_STMT * stmt)
{
  T_CON_HANDLE *con_handle = NULL;
  T_REQ_HANDLE *req_handle = NULL;
  int error;
  int num_bind;

  error = STMT_API_PRE (stmt, &con_handle, &req_handle);
  if (error < 0)
    {
      return error;
    }

  num_bind = req_handle->num_bind;

  CON_STMT_API_POST (NULL, stmt, con_handle, CCI_ER_NO_ERROR);

  return num_bind;
}

int
cci_is_holdable (CCI_STMT * stmt)
{
  T_CON_HANDLE *con_handle = NULL;
  T_REQ_HANDLE *req_handle = NULL;
  int error;
  int is_holdable;

  error = STMT_API_PRE (stmt, &con_handle, &req_handle);
  if (error < 0)
    {
      return error;
    }

  is_holdable = hm_get_req_handle_holdable (con_handle, req_handle);

  CON_STMT_API_POST (NULL, stmt, con_handle, CCI_ER_NO_ERROR);

  return is_holdable;
}

T_CCI_COL_INFO *
cci_get_result_info (CCI_STMT * stmt, T_CCI_STMT_TYPE * cmd_type, int *num)
{
  T_CON_HANDLE *con_handle = NULL;
  T_REQ_HANDLE *req_handle = NULL;
  T_CCI_COL_INFO *col_info = NULL;
  int error;

  error = STMT_API_PRE (stmt, &con_handle, &req_handle);
  if (error < 0)
    {
      return NULL;
    }

  if (cmd_type)
    {
      *cmd_type = (T_CCI_STMT_TYPE) - 1;
    }

  if (num)
    {
      *num = 0;
    }

  if (cmd_type)
    {
      *cmd_type = req_handle->stmt_type;
    }

  switch (req_handle->handle_type)
    {
    case HANDLE_PREPARE:
      if (req_handle->stmt_type == CCI_STMT_SELECT
	  || req_handle->stmt_type == CCI_STMT_SELECT_UPDATE)
	{
	  if (num)
	    {
	      *num = req_handle->num_col_info;
	    }
	  col_info = req_handle->col_info;
	}
      break;
    case HANDLE_SCHEMA_INFO:
      if (num)
	{
	  *num = req_handle->num_col_info;
	}
      col_info = req_handle->col_info;
      break;
    default:
      break;
    }

  CON_STMT_API_POST (NULL, stmt, con_handle, CCI_ER_NO_ERROR);

  return col_info;
}

int
cci_bind_param (CCI_STMT * stmt, int index, T_CCI_TYPE a_type,
		const void *value, char flag)
{
  int error;
  T_CON_HANDLE *con_handle = NULL;
  T_REQ_HANDLE *req_handle = NULL;

  error = STMT_API_PRE (stmt, &con_handle, &req_handle);
  if (error < 0)
    {
      return error;
    }

  error = qe_bind_param (req_handle, index, a_type, value,
			 UNMEASURED_LENGTH, flag);

  CON_STMT_API_POST (NULL, stmt, con_handle, error);

  return error;
}

int
cci_bind_param_ex (CCI_STMT * stmt, int index, T_CCI_TYPE a_type,
		   const void *value, int length, char flag)
{
  int error;
  T_CON_HANDLE *con_handle = NULL;
  T_REQ_HANDLE *req_handle = NULL;

  error = STMT_API_PRE (stmt, &con_handle, &req_handle);
  if (error < 0)
    {
      return error;
    }

  error = qe_bind_param (req_handle, index, a_type, value, length, flag);

  CON_STMT_API_POST (NULL, stmt, con_handle, error);

  return error;
}

int
cci_bind_param_array_size (CCI_STMT * stmt, int array_size)
{
  T_CON_HANDLE *con_handle = NULL;
  T_REQ_HANDLE *req_handle = NULL;
  int error = 0;

  error = STMT_API_PRE (stmt, &con_handle, &req_handle);
  if (error < 0)
    {
      return error;
    }

  req_handle->bind_array_size = array_size;

  CON_STMT_API_POST (NULL, stmt, con_handle, CCI_ER_NO_ERROR);

  return CCI_ER_NO_ERROR;
}

int
cci_bind_param_array (CCI_STMT * stmt, int index, T_CCI_TYPE a_type,
		      const void *value, int *null_ind)
{
  T_CON_HANDLE *con_handle = NULL;
  T_REQ_HANDLE *req_handle = NULL;
  int error = 0;

  error = STMT_API_PRE (stmt, &con_handle, &req_handle);
  if (error < 0)
    {
      return error;
    }

  if (req_handle->bind_array_size <= 0)
    {
      error = CCI_ER_BIND_ARRAY_SIZE;
    }
  else if (index <= 0 || index > req_handle->num_bind)
    {
      error = CCI_ER_BIND_INDEX;
    }
  else
    {
      index--;
      req_handle->bind_value[index].a_type = a_type;
      req_handle->bind_value[index].size = 0;
      req_handle->bind_value[index].value = value;
      req_handle->bind_value[index].null_ind = null_ind;
      req_handle->bind_value[index].alloc_buffer = NULL;
    }

  CON_STMT_API_POST (NULL, stmt, con_handle, error);

  return error;
}

/*
 * For the purpose of re-balancing existing connections, cci_prepare,
 * cci_execute, cci_execute_batch, cci_prepare_and_execute
 * require to forcefully disconnect the current
 * con_handle when it is in the OUT_TRAN state and the time elapsed
 * after the last failure of a host is over rc_time.
 */
static int
cci_execute_internal (CCI_STMT * stmt, char flag, int max_col_size, int gid)
{
  T_REQ_HANDLE *req_handle = NULL;
  T_CON_HANDLE *con_handle = NULL;
  int error = CCI_ER_NO_ERROR;
  struct timeval st, et;
  bool is_first_exec_in_tran = false;
  int retry_count = RECONNECT_RETRY_COUNT;

  error = STMT_API_PRE (stmt, &con_handle, &req_handle);
  if (error < 0)
    {
      return error;
    }

  if (con_handle->con_property.log_slow_queries)
    {
      gettimeofday (&st, NULL);
    }

  if (con_handle->con_property.log_trace_api)
    {
      CCI_LOGF_DEBUG (con_handle->con_logger, "FLAG[%d], MAX_COL_SIZE[%d]",
		      flag, max_col_size);
    }

  if (IS_OUT_TRAN (con_handle) && IS_FORCE_FAILBACK (con_handle)
      && !IS_INVALID_SOCKET (con_handle->sock_fd))
    {
      hm_force_close_connection (con_handle);
    }
  SET_START_TIME_FOR_QUERY (con_handle, req_handle);

  if (IS_CAS_STATEMENT_POOLING (con_handle) && req_handle->valid == false)
    {
      error = qe_prepare (req_handle, con_handle, req_handle->sql_text,
			  req_handle->prepare_flag, 1);
    }

  is_first_exec_in_tran = IS_OUT_TRAN (con_handle);

  if (error >= 0)
    {
      error = qe_execute (req_handle, con_handle, flag, max_col_size, gid);
    }
  while ((retry_count-- > 0)
	 && (IS_OUT_TRAN (con_handle) || is_first_exec_in_tran)
	 && IS_ER_TO_RECONNECT (error, con_handle->err_buf.err_code))
    {
      if (NEED_TO_RECONNECT (con_handle, error))
	{
	  /* Finally, reset_connect will return ER_TIMEOUT */
	  error = reset_connect (con_handle, req_handle);
	  if (error != CCI_ER_NO_ERROR)
	    {
	      break;
	    }
	}

      error = qe_prepare (req_handle, con_handle, req_handle->sql_text,
			  req_handle->prepare_flag, 1);
      if (error < 0)
	{
	  continue;
	}

      error = qe_execute (req_handle, con_handle, flag, max_col_size, gid);
    }

  /* If prepared plan is invalidated while using plan cache,
     the error, CAS_ER_STMT_POOLING, is returned.
     In this case, prepare and execute have to be executed again.
   */
  while (error == CAS_ER_STMT_POOLING
	 && IS_CAS_STATEMENT_POOLING (con_handle))
    {
      req_handle_content_free (req_handle, 1);
      error = qe_prepare (req_handle, con_handle, req_handle->sql_text,
			  req_handle->prepare_flag, 1);
      if (error < 0)
	{
	  goto execute_end;
	}
      error = qe_execute (req_handle, con_handle, flag, max_col_size, gid);
    }

execute_end:
  RESET_START_TIME (con_handle);

  if (error == CCI_ER_QUERY_TIMEOUT &&
      con_handle->con_property.disconnect_on_query_timeout)
    {
      hm_force_close_connection (con_handle);
    }

  if (IS_OUT_TRAN (con_handle))
    {
      hm_check_rc_time (con_handle);
    }

  if (con_handle->con_property.log_slow_queries)
    {
      long elapsed;

      gettimeofday (&et, NULL);
      elapsed = ELAPSED_MSECS (et, st);
      if (elapsed > con_handle->con_property.slow_query_threshold_millis)
	{
	  char svr_info_buf[256];

	  CCI_LOGF_DEBUG (con_handle->con_logger, "[CONHANDLE - %d:%d] "
			  "[CAS INFO - %s, %d, %d] "
			  "[SLOW QUERY - ELAPSED : %d] [SQL - %s]",
			  con_handle->con_handle_id.id,
			  con_handle->con_handle_id.id_seq,
			  ut_cur_host_info_to_str (svr_info_buf, con_handle),
			  CON_CAS_ID (con_handle), CON_CAS_PID (con_handle),
			  elapsed, req_handle->sql_text);
	}
    }

  CON_STMT_API_POST (NULL, stmt, con_handle, error);

  return error;
}

int
cci_execute (CCI_STMT * stmt, char flag, int max_col_size)
{
  return cci_execute_internal (stmt, flag, max_col_size, 0);
}

int
cci_execute_with_gid (CCI_STMT * stmt, char flag, int max_col_size, int gid)
{
  return cci_execute_internal (stmt, flag, max_col_size, gid);
}

/*
 * For the purpose of re-balancing existing connections, cci_prepare,
 * cci_execute, cci_execute_batch, cci_prepare_and_execute
 * require to forcefully disconnect the current
 * con_handle when it is in the OUT_TRAN state and the time elapsed
 * after the last failure of a host is over rc_time.
 */
int
cci_execute_batch (CCI_STMT * stmt, T_CCI_QUERY_RESULT ** qr)
{
  T_REQ_HANDLE *req_handle = NULL;
  T_CON_HANDLE *con_handle = NULL;
  int error = CCI_ER_NO_ERROR;
  bool is_first_exec_in_tran = false;
  int retry_count = RECONNECT_RETRY_COUNT;

  error = STMT_API_PRE (stmt, &con_handle, &req_handle);
  if (error < 0)
    {
      return error;
    }

  if (qr == NULL)
    {
      error = CCI_ER_INVALID_ARGS;
      goto execute_end;
    }

  *qr = NULL;

  if (IS_OUT_TRAN (con_handle) && IS_FORCE_FAILBACK (con_handle)
      && !IS_INVALID_SOCKET (con_handle->sock_fd))
    {
      hm_force_close_connection (con_handle);
    }
  SET_START_TIME_FOR_QUERY (con_handle, req_handle);

  if (IS_CAS_STATEMENT_POOLING (con_handle) && req_handle->valid == false)
    {
      error = qe_prepare (req_handle, con_handle, req_handle->sql_text,
			  req_handle->prepare_flag, 1);
    }

  is_first_exec_in_tran = IS_OUT_TRAN (con_handle);

  if (error >= 0)
    {
      error = qe_execute_batch (req_handle, con_handle, qr);
    }
  while ((retry_count-- > 0)
	 && (IS_OUT_TRAN (con_handle) || is_first_exec_in_tran)
	 && IS_ER_TO_RECONNECT (error, con_handle->err_buf.err_code))
    {
      if (NEED_TO_RECONNECT (con_handle, error))
	{
	  /* Finally, reset_connect will return ER_TIMEOUT */
	  error = reset_connect (con_handle, req_handle);
	  if (error != CCI_ER_NO_ERROR)
	    {
	      break;
	    }
	}

      error = qe_prepare (req_handle, con_handle, req_handle->sql_text,
			  req_handle->prepare_flag, 1);
      if (error < 0)
	{
	  continue;
	}

      error = qe_execute_batch (req_handle, con_handle, qr);
    }

  while (error == CAS_ER_STMT_POOLING
	 && IS_CAS_STATEMENT_POOLING (con_handle))
    {
      req_handle_content_free (req_handle, 1);
      error = qe_prepare (req_handle, con_handle, req_handle->sql_text,
			  req_handle->prepare_flag, 1);
      if (error < 0)
	{
	  goto execute_end;
	}
      error = qe_execute_batch (req_handle, con_handle, qr);
    }

execute_end:
  RESET_START_TIME (con_handle);

  if (error == CCI_ER_QUERY_TIMEOUT &&
      con_handle->con_property.disconnect_on_query_timeout)
    {
      hm_force_close_connection (con_handle);
    }

  if (IS_OUT_TRAN (con_handle))
    {
      hm_check_rc_time (con_handle);
    }

  CON_STMT_API_POST (NULL, stmt, con_handle, error);

  return error;
}

int
cci_get_db_parameter (CCI_CONN * conn, T_CCI_DB_PARAM param_name, void *value)
{
  T_CON_HANDLE *con_handle = NULL;
  int error = CCI_ER_NO_ERROR;
  int retry_count = RECONNECT_RETRY_COUNT;

  error = CON_API_PRE (conn, NULL, &con_handle, 0);
  if (error < 0)
    {
      return error;
    }

  error = qe_get_db_parameter (con_handle, param_name, value);
  while ((retry_count-- > 0)
	 && IS_OUT_TRAN (con_handle)
	 && IS_ER_TO_RECONNECT (error, con_handle->err_buf.err_code))
    {
      if (NEED_TO_RECONNECT (con_handle, error))
	{
	  /* Finally, reset_connect will return ER_TIMEOUT */
	  error = reset_connect (con_handle, NULL);
	  if (error != CCI_ER_NO_ERROR)
	    {
	      break;
	    }
	}

      error = qe_get_db_parameter (con_handle, param_name, value);
    }

  CON_STMT_API_POST (conn, NULL, con_handle, error);

  return error;
}

long
cci_escape_string (CCI_CONN * conn, char *to, const char *from,
		   unsigned long length)
{
  T_CON_HANDLE *con_handle = NULL;
  int error = CCI_ER_NO_ERROR;
  unsigned long i;
  char *target_ptr = to;

  if (to == NULL || from == NULL)
    {
      if (conn != NULL)
	{
	  set_error_buffer (&conn->err_buf, CCI_ER_INVALID_ARGS, NULL);
	}
      return CCI_ER_INVALID_ARGS;
    }

  error = CON_API_PRE (conn, NULL, &con_handle, 0);
  if (error < 0)
    {
      return error;
    }

  for (i = 0; i < length; i++)
    {
      if (from[i] == '\'')
	{
	  /* single-quote is converted to two-single-quote */
	  *(target_ptr) = '\'';
	  *(target_ptr + 1) = '\'';
	  target_ptr += 2;
	}
      else
	{
	  *(target_ptr) = from[i];
	  target_ptr++;
	}
    }

  /* terminating NULL char */
  *target_ptr = '\0';

  if (con_handle != NULL)
    {
      CON_STMT_API_POST (conn, NULL, con_handle, CCI_ER_NO_ERROR);
    }

  return ((long) (target_ptr - to));
}

int
cci_close_query_result (CCI_STMT * stmt)
{
  T_REQ_HANDLE *req_handle = NULL;
  T_CON_HANDLE *con_handle = NULL;
  int error = 0;

  error = STMT_API_PRE (stmt, &con_handle, &req_handle);
  if (error < 0)
    {
      return error;
    }

  error = qe_close_query_result (req_handle, con_handle);
  if (IS_ER_TO_RECONNECT (error, con_handle->err_buf.err_code))
    {
      if (IS_OUT_TRAN (con_handle))
	{
	  error = CCI_ER_NO_ERROR;
	}
    }

  if (error == CCI_ER_NO_ERROR)
    {
      error = req_close_query_result (req_handle);
    }

  CON_STMT_API_POST (NULL, stmt, con_handle, error);

  return error;
}

int
cci_close_req_handle (CCI_STMT * stmt)
{
  T_REQ_HANDLE *req_handle = NULL;
  T_CON_HANDLE *con_handle = NULL;
  int error = 0;

  error = STMT_API_PRE (stmt, &con_handle, &req_handle);
  if (error < 0)
    {
      return error;
    }

  if (req_handle->handle_type == HANDLE_PREPARE
      || req_handle->handle_type == HANDLE_SCHEMA_INFO)
    {
      error = qe_close_req_handle (req_handle, con_handle);
      if (IS_ER_TO_RECONNECT (error, con_handle->err_buf.err_code))
	{
	  if (IS_OUT_TRAN (con_handle))
	    {
	      error = CCI_ER_NO_ERROR;
	    }
	}
    }
  else
    {
      error = CCI_ER_NO_ERROR;
    }
  hm_req_handle_free (con_handle, req_handle);
  req_handle = NULL;

  if (IS_OUT_TRAN (con_handle))
    {
      hm_check_rc_time (con_handle);
    }

  CON_STMT_API_POST (NULL, stmt, con_handle, error);

  hm_set_stmt_handle_id (stmt, NULL, NULL);

  return error;
}

int
cci_fetch_size (CCI_STMT * stmt, int fetch_size)
{
  T_CON_HANDLE *con_handle = NULL;
  T_REQ_HANDLE *req_handle = NULL;
  int error;

  error = STMT_API_PRE (stmt, &con_handle, &req_handle);
  if (error < 0)
    {
      return error;
    }

  req_handle->fetch_size = fetch_size;

  CON_STMT_API_POST (NULL, stmt, con_handle, CCI_ER_NO_ERROR);

  return CCI_ER_NO_ERROR;
}

static int
cci_fetch (CCI_STMT * stmt, T_CCI_CURSOR_POS pos)
{
  T_REQ_HANDLE *req_handle = NULL;
  T_CON_HANDLE *con_handle = NULL;
  int error = CCI_ER_NO_ERROR;
  int result_set_index = 0;

  error = STMT_API_PRE (stmt, &con_handle, &req_handle);
  if (error < 0)
    {
      return error;
    }

  error = qe_cursor (req_handle, 1, pos);

  if (error != CCI_ER_NO_ERROR)
    {
      goto end;
    }

  error = qe_fetch (req_handle, con_handle, result_set_index);

  if (IS_OUT_TRAN (con_handle))
    {
      hm_check_rc_time (con_handle);
    }

end:
  CON_STMT_API_POST (NULL, stmt, con_handle, error);
  return error;
}

int
cci_fetch_next (CCI_STMT * stmt)
{
  return cci_fetch (stmt, CCI_CURSOR_CURRENT);
}

int
cci_fetch_first (CCI_STMT * stmt)
{
  return cci_fetch (stmt, CCI_CURSOR_FIRST);
}

int
cci_fetch_last (CCI_STMT * stmt)
{
  return cci_fetch (stmt, CCI_CURSOR_LAST);
}

static int
cci_get_data (CCI_STMT * stmt, int col_no, int a_type, void *value,
	      int *indicator)
{
  T_REQ_HANDLE *req_handle = NULL;
  T_CON_HANDLE *con_handle = NULL;
  int error = 0;

  error = STMT_API_PRE (stmt, &con_handle, &req_handle);
  if (error < 0)
    {
      return error;
    }

  if (indicator == NULL || value == NULL)
    {
      error = CCI_ER_INVALID_ARGS;
    }
  else
    {
      *indicator = -1;
      error = qe_get_data (req_handle, col_no, a_type, value, indicator);
    }

  CON_STMT_API_POST (NULL, stmt, con_handle, error);

  return error;
}

int
cci_get_int (CCI_STMT * stmt, int col_idx, int *indicator)
{
  int error;
  int val;

  error = cci_get_data (stmt, col_idx, CCI_TYPE_INT, &val, indicator);
  if (error != CCI_ER_NO_ERROR || *indicator == -1)
    {
      return 0;
    }

  return val;
}

INT64
cci_get_bigint (CCI_STMT * stmt, int col_idx, int *indicator)
{
  int error;
  INT64 val;

  error = cci_get_data (stmt, col_idx, CCI_TYPE_BIGINT, &val, indicator);
  if (error != CCI_ER_NO_ERROR || *indicator == -1)
    {
      return 0;
    }

  return val;
}

double
cci_get_double (CCI_STMT * stmt, int col_idx, int *indicator)
{
  int error;
  double val;

  error = cci_get_data (stmt, col_idx, CCI_TYPE_DOUBLE, &val, indicator);
  if (error != CCI_ER_NO_ERROR || *indicator == -1)
    {
      return 0.0f;
    }

  return val;
}

char *
cci_get_string (CCI_STMT * stmt, int col_idx, int *indicator)
{
  int error;
  char *val;

  error = cci_get_data (stmt, col_idx, CCI_TYPE_VARCHAR, &val, indicator);
  if (error != CCI_ER_NO_ERROR || *indicator == -1)
    {
      return NULL;
    }

  return val;
}

T_CCI_DATETIME
cci_get_datetime (CCI_STMT * stmt, int col_idx, int *indicator)
{
  int error;
  T_CCI_DATETIME val;

  error = cci_get_data (stmt, col_idx, CCI_TYPE_DATETIME, &val, indicator);
  if (error != CCI_ER_NO_ERROR || *indicator == -1)
    {
      memset (&val, 0, sizeof (T_CCI_DATETIME));
    }

  return val;
}

T_CCI_VARBIT
cci_get_bit (CCI_STMT * stmt, int col_idx, int *indicator)
{
  int error;
  T_CCI_VARBIT val;

  error = cci_get_data (stmt, col_idx, CCI_TYPE_VARBIT, &val, indicator);
  if (error != CCI_ER_NO_ERROR || *indicator == -1)
    {
      memset (&val, 0, sizeof (T_CCI_VARBIT));
    }

  return val;
}

int
cci_schema_info (CCI_CONN * conn, CCI_STMT * stmt, T_CCI_SCH_TYPE type,
		 char *arg1, char *arg2, int flag)
{
  T_CON_HANDLE *con_handle = NULL;
  T_REQ_HANDLE *req_handle = NULL;
  int error = CCI_ER_NO_ERROR;
  int retry_count = RECONNECT_RETRY_COUNT;

  hm_set_stmt_handle_id (stmt, NULL, NULL);

  error = CON_API_PRE (conn, stmt, &con_handle, CON_API_PRE_FLAG_INIT_STMT);
  if (error < 0)
    {
      return error;
    }

  error = hm_req_handle_alloc (con_handle, &req_handle);
  if (error < 0)
    {
      goto ret;
    }

  error = qe_schema_info (req_handle, con_handle, (int) type, arg1, arg2,
			  flag);
  while ((retry_count-- > 0)
	 && IS_OUT_TRAN (con_handle)
	 && IS_ER_TO_RECONNECT (error, con_handle->err_buf.err_code))
    {
      if (NEED_TO_RECONNECT (con_handle, error))
	{
	  /* Finally, reset_connect will return ER_TIMEOUT */
	  error = reset_connect (con_handle, NULL);
	  if (error != CCI_ER_NO_ERROR)
	    {
	      break;
	    }
	}

      error = qe_schema_info (req_handle, con_handle, (int) type, arg1, arg2,
			      flag);
    }

  if (error < 0)
    {
      hm_req_handle_free (con_handle, req_handle);
      req_handle = NULL;
    }

ret:
  hm_set_stmt_handle_id (stmt, con_handle, req_handle);
  CON_STMT_API_POST (conn, stmt, con_handle, error);

  return error;
}

int
cci_set_autocommit (CCI_CONN * conn, CCI_AUTOCOMMIT_MODE autocommit_mode)
{
  T_CON_HANDLE *con_handle = NULL;
  int error = 0;

  error = CON_API_PRE (conn, NULL, &con_handle, 0);
  if (error < 0)
    {
      return error;
    }

  if (autocommit_mode != con_handle->autocommit_mode
      && IS_IN_TRAN (con_handle))
    {
      error = qe_end_tran (con_handle, CCI_TRAN_COMMIT);
    }

  if (error == 0)
    {
      con_handle->autocommit_mode = autocommit_mode;
    }

  CON_STMT_API_POST (conn, NULL, con_handle, error);

  return error;
}

CCI_AUTOCOMMIT_MODE
cci_get_autocommit (CCI_CONN * conn)
{
  T_CON_HANDLE *con_handle = NULL;
  int error = 0;
  CCI_AUTOCOMMIT_MODE autocommit_mode;

  error = CON_API_PRE (conn, NULL, &con_handle, 0);
  if (error < 0)
    {
      return error;
    }

  autocommit_mode = con_handle->autocommit_mode;

  CON_STMT_API_POST (conn, NULL, con_handle, CCI_ER_NO_ERROR);

  return autocommit_mode;
}

int
cci_set_holdability (CCI_CONN * conn, int holdable)
{
  T_CON_HANDLE *con_handle = NULL;
  int error = 0;

  error = CON_API_PRE (conn, NULL, &con_handle, 0);
  if (error < 0)
    {
      return error;
    }

  if (holdable < 0 || holdable > 1)
    {
      error = CCI_ER_INVALID_HOLDABILITY;
    }
  else
    {
      hm_set_con_handle_holdable (con_handle, holdable);
      error = CCI_ER_NO_ERROR;
    }

  CON_STMT_API_POST (conn, NULL, con_handle, error);

  return error;
}

int
cci_get_holdability (CCI_CONN * conn)
{
  T_CON_HANDLE *con_handle = NULL;
  int error = 0;
  int holdability;

  error = CON_API_PRE (conn, NULL, &con_handle, 0);
  if (error < 0)
    {
      return error;
    }

  holdability = hm_get_con_handle_holdable (con_handle);

  CON_STMT_API_POST (conn, NULL, con_handle, CCI_ER_NO_ERROR);

  return holdability;
}

int
cci_get_db_version (CCI_CONN * conn, char *out_buf, int buf_size)
{
  T_CON_HANDLE *con_handle = NULL;
  int error = 0;
  int retry_count = RECONNECT_RETRY_COUNT;

  error = CON_API_PRE (conn, NULL, &con_handle, 0);
  if (error < 0)
    {
      return error;
    }

  if (out_buf && buf_size >= 1)
    {
      out_buf[0] = '\0';
    }

  SET_START_TIME_FOR_QUERY (con_handle, NULL);

  error = qe_get_db_version (con_handle, out_buf, buf_size);
  while ((retry_count-- > 0)
	 && IS_OUT_TRAN (con_handle)
	 && IS_ER_TO_RECONNECT (error, con_handle->err_buf.err_code))
    {
      if (NEED_TO_RECONNECT (con_handle, error))
	{
	  /* Finally, reset_connect will return ER_TIMEOUT */
	  error = reset_connect (con_handle, NULL);
	  if (error != CCI_ER_NO_ERROR)
	    {
	      break;
	    }
	}

      error = qe_get_db_version (con_handle, out_buf, buf_size);
    }

  CON_STMT_API_POST (conn, NULL, con_handle, error);

  RESET_START_TIME (con_handle);
  return error;
}

int
cci_query_result_free (T_CCI_QUERY_RESULT * qr, int num_q)
{
  qe_query_result_free (num_q, qr);

  return CCI_ER_NO_ERROR;
}

#if defined(CCI_SENSITIVE_CURSOR)
int
cci_fetch_buffer_clear (CCI_STMT * stmt)
{
  T_CON_HANDLE *con_handle = NULL;
  T_REQ_HANDLE *req_handle = NULL;
  int error;

  error = STMT_API_PRE (stmt, &con_handle, &req_handle);
  if (error < 0)
    {
      return error;
    }

  hm_req_handle_fetch_buf_free (req_handle);

  CON_STMT_API_POST (NULL, stmt, con_handle, CCI_ER_NO_ERROR);

  return CCI_ER_NO_ERROR;
}
#endif

int
cci_execute_result (CCI_STMT * stmt, T_CCI_QUERY_RESULT ** qr)
{
  T_REQ_HANDLE *req_handle = NULL;
  T_CON_HANDLE *con_handle = NULL;
  int error = CCI_ER_NO_ERROR;

  error = STMT_API_PRE (stmt, &con_handle, &req_handle);
  if (error < 0)
    {
      return error;
    }

  if (qr == NULL)
    {
      error = CCI_ER_INVALID_ARGS;
    }
  else
    {
      *qr = NULL;
      error = qe_query_result_copy (req_handle, qr);
    }

  CON_STMT_API_POST (NULL, stmt, con_handle, error);

  return error;
}

int
cci_set_login_timeout (CCI_CONN * conn, int timeout)
{
  T_CON_HANDLE *con_handle = NULL;
  int error = CCI_ER_NO_ERROR;

  error = CON_API_PRE (conn, NULL, &con_handle, 0);
  if (error < 0)
    {
      return error;
    }

  con_handle->con_property.login_timeout = timeout;

  CON_STMT_API_POST (conn, NULL, con_handle, CCI_ER_NO_ERROR);

  return CCI_ER_NO_ERROR;
}

int
cci_get_login_timeout (CCI_CONN * conn, int *val)
{
  T_CON_HANDLE *con_handle = NULL;
  int error = CCI_ER_NO_ERROR;

  error = CON_API_PRE (conn, NULL, &con_handle, 0);
  if (error < 0)
    {
      return error;
    }

  if (val == NULL)
    {
      error = CCI_ER_INVALID_ARGS;
    }
  else
    {
      *val = con_handle->con_property.login_timeout;
      error = CCI_ER_NO_ERROR;
    }

  CON_STMT_API_POST (conn, NULL, con_handle, error);

  return error;
}

int
cci_get_query_plan (CCI_CONN * conn, const char *sql, char **out_buf)
{
  T_CON_HANDLE *con_handle = NULL;
  int error = 0;

  error = CON_API_PRE (conn, NULL, &con_handle, 0);
  if (error < 0)
    {
      return error;
    }

  error = qe_get_query_plan (con_handle, sql, out_buf);

  CON_STMT_API_POST (conn, NULL, con_handle, error);

  return error;
}

int
cci_query_info_free (char *out_buf)
{
  FREE_MEM (out_buf);
  return 0;
}

int
cci_set_max_row (CCI_STMT * stmt, int max_row)
{
  T_CON_HANDLE *con_handle = NULL;
  T_REQ_HANDLE *req_handle = NULL;
  int error = CCI_ER_NO_ERROR;

  error = STMT_API_PRE (stmt, &con_handle, &req_handle);
  if (error < 0)
    {
      return error;
    }

  req_handle->max_row = max_row;

  CON_STMT_API_POST (NULL, stmt, con_handle, CCI_ER_NO_ERROR);

  return CCI_ER_NO_ERROR;
}

int
cci_set_query_timeout (CCI_STMT * stmt, int timeout)
{
  T_CON_HANDLE *con_handle = NULL;
  T_REQ_HANDLE *req_handle = NULL;
  int old_value;
  int error;

  error = STMT_API_PRE (stmt, &con_handle, &req_handle);
  if (error < 0)
    {
      return error;
    }

  if (timeout < 0)
    {
      timeout = 0;
    }

  old_value = req_handle->query_timeout;
  req_handle->query_timeout = timeout;

  CON_STMT_API_POST (NULL, stmt, con_handle, CCI_ER_NO_ERROR);

  return old_value;
}

int
cci_get_query_timeout (CCI_STMT * stmt)
{
  T_CON_HANDLE *con_handle = NULL;
  T_REQ_HANDLE *req_handle = NULL;
  int error;
  int timeout;

  error = STMT_API_PRE (stmt, &con_handle, &req_handle);
  if (error < 0)
    {
      return error;
    }

  timeout = req_handle->query_timeout;

  CON_STMT_API_POST (NULL, stmt, con_handle, CCI_ER_NO_ERROR);

  return timeout;
}

static const char *
cci_get_err_msg_internal (int error)
{
  switch (error)
    {
    case CCI_ER_DBMS:
    case CAS_ER_DBMS:
      return "Rye DBMS Error";

    case CCI_ER_CON_HANDLE:
      return "Invalid connection handle";

    case CCI_ER_NO_MORE_MEMORY:
      return "Memory allocation error";

    case CCI_ER_COMMUNICATION:
      return "Cannot communicate with server";

    case CCI_ER_NO_MORE_DATA:
      return "Invalid cursor position";

    case CCI_ER_TRAN_TYPE:
      return "Unknown transaction type";

    case CCI_ER_STRING_PARAM:
      return "Invalid string argument";

    case CCI_ER_TYPE_CONVERSION:
      return "Type conversion error";

    case CCI_ER_BIND_INDEX:
      return "Parameter index is out of range";

    case CCI_ER_ATYPE:
      return "Invalid T_CCI_TYPE value";

    case CCI_ER_NOT_BIND:
      return "Not used";

    case CCI_ER_PARAM_NAME:
      return "Invalid T_CCI_DB_PARAM value";

    case CCI_ER_COLUMN_INDEX:
      return "Column index is out of range";

    case CCI_ER_SCHEMA_TYPE:
      return "Not used";

    case CCI_ER_FILE:
      return "Cannot open file";

    case CCI_ER_CONNECT:
      return "Cannot connect to Rye CAS";

    case CCI_ER_ALLOC_CON_HANDLE:
      return "Cannot allocate connection handle";

    case CCI_ER_REQ_HANDLE:
      return "Cannot allocate request handle";

    case CCI_ER_INVALID_CURSOR_POS:
      return "Invalid cursor position";

    case CCI_ER_CAS:
      return "Not used";

    case CCI_ER_HOSTNAME:
      return "Unknown host name";

    case CCI_ER_BIND_ARRAY_SIZE:
      return "Array binding size is not specified";

    case CCI_ER_ISOLATION_LEVEL:
      return "Unknown transaction isolation level";

    case CCI_ER_SAVEPOINT_CMD:
      return "Invalid T_CCI_SAVEPOINT_CMD value";

    case CCI_ER_INVALID_URL:
      return "Invalid url string";

    case CCI_ER_INVALID_LOB_READ_POS:
      return "Invalid lob read position";

#if 0				/* unused */
    case CCI_ER_INVALID_LOB_HANDLE:
      return "Invalid lob handle";
#endif

    case CCI_ER_NO_PROPERTY:
      return "Cannot find a property";

      /* CCI_ER_INVALID_PROPERTY_VALUE equals to CCI_ER_PROPERTY_TYPE */
    case CCI_ER_INVALID_PROPERTY_VALUE:
      return "Invalid property value";

    case CCI_ER_LOGIN_TIMEOUT:
      return "Connection timed out";

    case CCI_ER_QUERY_TIMEOUT:
      return "Request timed out";

    case CCI_ER_RESULT_SET_CLOSED:
      return "Result set is closed";

    case CCI_ER_INVALID_HOLDABILITY:
      return "Invalid holdability mode. The only accepted values are 0 or 1";

    case CCI_ER_INVALID_ARGS:
      return "Invalid argument";

    case CCI_ER_USED_CONNECTION:
      return "This connection is used already.";

    case CAS_ER_INTERNAL:
      return "Not used";

    case CAS_ER_NO_MORE_MEMORY:
      return "Memory allocation error";

    case CAS_ER_COMMUNICATION:
      return "Cannot receive data from client";

    case CAS_ER_ARGS:
      return "Invalid argument";

    case CAS_ER_TRAN_TYPE:
      return "Invalid transaction type argument";

    case CAS_ER_SRV_HANDLE:
      return "Server handle not found";

    case CAS_ER_NUM_BIND:
      return "Invalid parameter binding value argument";

    case CAS_ER_UNKNOWN_U_TYPE:
      return "Invalid T_CCI_TYPE value";

    case CAS_ER_DB_VALUE:
      return "Cannot make DB_VALUE";

    case CAS_ER_TYPE_CONVERSION:
      return "Type conversion error";

    case CAS_ER_PARAM_NAME:
      return "Invalid T_CCI_DB_PARAM value";

    case CAS_ER_NO_MORE_DATA:
      return "Invalid cursor position";

    case CAS_ER_OPEN_FILE:
      return "Cannot open file";

    case CAS_ER_SCHEMA_TYPE:
      return "Invalid T_CCI_SCH_TYPE value";

    case CAS_ER_VERSION:
      return "Version mismatch";

    case BR_ER_FREE_SERVER:
      return "Cannot process the request.  Try again later";

    case CAS_ER_NOT_AUTHORIZED_CLIENT:
      return "Authorization error";

    case CAS_ER_QUERY_CANCEL:
      return "Cannot cancel the query";

    case CAS_ER_NO_MORE_RESULT_SET:
      return "No More Result";

    case CAS_ER_STMT_POOLING:
      return "Invalid plan";

    case CAS_ER_DBSERVER_DISCONNECTED:
      return "Cannot communicate with DB Server";

    case CAS_ER_MAX_PREPARED_STMT_COUNT_EXCEEDED:
      return "Cannot prepare more than MAX_PREPARED_STMT_COUNT statements";

    case CAS_ER_HOLDABLE_NOT_ALLOWED:
      return "Holdable results may not be updatable or sensitive";

    case CCI_ER_VALUE_OUT_OF_RANGE:
      return "Property value is out of range";

    case CCI_ER_SERVER_RESTARTED:
      return "Server restarted";

    default:
      return NULL;
    }
}

/*
  Called by PRINT_CCI_ERROR()
*/
int
cci_get_error_msg (int error, T_CCI_ERROR * cci_err, char *buf, int bufsize)
{
  const char *err_msg;

  if ((buf == NULL) || (bufsize <= 0))
    {
      return -1;
    }

  err_msg = cci_get_err_msg_internal (error);
  if (err_msg == NULL)
    {
      return -1;
    }
  else
    {
      if ((error < CCI_ER_DBMS) && (error > CCI_ER_END))
	{
	  snprintf (buf, bufsize, "CCI Error : %s", err_msg);
	}
      else if ((error < CAS_ER_DBMS) && (error >= CAS_ER_LAST))
	{
	  snprintf (buf, bufsize, "Rye CAS Error : %s", err_msg);
	}
      if ((error == CCI_ER_DBMS) || (error == CAS_ER_DBMS))
	{
	  if (cci_err == NULL)
	    {
	      snprintf (buf, bufsize, "%s ", err_msg);
	    }
	  else
	    {
	      snprintf (buf, bufsize, "%s : (%d) %s", err_msg,
			cci_err->err_code, cci_err->err_msg);
	    }
	}
    }

  return 0;
}

/*
   Called by applications.
   They don't need prefix such as "ERROR :" or "Rye CAS ERROR".
 */
int
cci_get_err_msg (int error, char *buf, int bufsize)
{
  const char *err_msg;

  if ((buf == NULL) || (bufsize <= 0))
    {
      return -1;
    }

  err_msg = cci_get_err_msg_internal (error);
  if (err_msg == NULL)
    {
      return -1;
    }
  else
    {
      snprintf (buf, bufsize, "%s", err_msg);
    }

  return 0;
}

int
cci_server_shard_nodeid (CCI_CONN * conn)
{
  T_CON_HANDLE *con_handle = NULL;
  int error = 0;
  int server_shard_nodeid;

  error = CON_API_PRE (conn, NULL, &con_handle, 0);
  if (error < 0)
    {
      return error;
    }

  server_shard_nodeid = con_handle->server_shard_nodeid;

  CON_STMT_API_POST (conn, NULL, con_handle, error);

  return server_shard_nodeid;
}

int
cci_shard_get_info (CCI_CONN * conn, CCI_SHARD_NODE_INFO ** node_info,
		    CCI_SHARD_GROUPID_INFO ** groupid_info)
{
  T_CON_HANDLE *con_handle = NULL;
  int error = 0;

  *node_info = NULL;
  *groupid_info = NULL;

  error = CON_API_PRE (conn, NULL, &con_handle, CON_API_PRE_FLAG_SHARD_ADMIN);
  if (error < 0)
    {
      return error;
    }

  error = net_shard_get_info (&con_handle->alter_hosts->host_info[0],
			      con_handle->db_name, 0, 0, 0,
			      con_handle->con_property.query_timeout,
			      node_info, groupid_info, NULL);

  CON_STMT_API_POST (conn, NULL, con_handle, error);

  return error;
}

int
cci_shard_node_info_free (CCI_SHARD_NODE_INFO * node_info)
{
  FREE_MEM (node_info);
  return CCI_ER_NO_ERROR;
}

int
cci_shard_group_info_free (CCI_SHARD_GROUPID_INFO * groupid_info)
{
  FREE_MEM (groupid_info);
  return CCI_ER_NO_ERROR;
}

#if defined(CCI_SHARD_ADMIN)
int
cci_shard_init (CCI_CONN * conn, int groupid_count, int init_num_nodes,
		const char **init_nodes)
{
  T_CON_HANDLE *con_handle = NULL;
  int error = 0;

  error = CON_API_PRE (conn, NULL, &con_handle, CON_API_PRE_FLAG_SHARD_ADMIN);
  if (error < 0)
    {
      return error;
    }

  error = net_shard_init (groupid_count, init_num_nodes, init_nodes,
			  con_handle);

  CON_STMT_API_POST (conn, NULL, con_handle, error);

  return error;
}
#endif

#if defined(CCI_SHARD_ADMIN)
int
cci_shard_add_node (CCI_CONN * conn, const char *node_arg)
{
  T_CON_HANDLE *con_handle = NULL;
  int error = 0;

  error = CON_API_PRE (conn, NULL, &con_handle, CON_API_PRE_FLAG_SHARD_ADMIN);
  if (error < 0)
    {
      return error;
    }

  error = net_shard_node_req (BRREQ_OP_CODE_ADD_NODE, node_arg, con_handle);

  CON_STMT_API_POST (conn, NULL, con_handle, error);

  return error;
}
#endif

#if defined(CCI_SHARD_ADMIN)
int
cci_shard_drop_node (CCI_CONN * conn, const char *node_arg)
{
  T_CON_HANDLE *con_handle = NULL;
  int error = 0;

  error = CON_API_PRE (conn, NULL, &con_handle, CON_API_PRE_FLAG_SHARD_ADMIN);
  if (error < 0)
    {
      return error;
    }

  error = net_shard_node_req (BRREQ_OP_CODE_DROP_NODE, node_arg, con_handle);

  CON_STMT_API_POST (conn, NULL, con_handle, error);

  return error;
}
#endif

int
cci_shard_migration_start (CCI_CONN * conn, int groupid, int dest_nodeid)
{
  T_CON_HANDLE *con_handle = NULL;
  int error = 0;

  error = CON_API_PRE (conn, NULL, &con_handle, CON_API_PRE_FLAG_SHARD_ADMIN);
  if (error < 0)
    {
      return error;
    }

  if (con_handle->shard_admin_sock_fd == INVALID_SOCKET)
    {
      error = net_shard_migration_req (BRREQ_OP_CODE_MIGRATION_START,
				       &con_handle->shard_admin_sock_fd,
				       groupid, dest_nodeid, 0, con_handle);
    }
  else
    {
      error = CCI_ER_INVALID_ARGS;
    }

  CON_STMT_API_POST (conn, NULL, con_handle, error);

  return error;
}

int
cci_shard_migration_end (CCI_CONN * conn, int groupid, int dest_nodeid,
			 int num_shard_keys)
{
  T_CON_HANDLE *con_handle = NULL;
  int error = 0;

  error = CON_API_PRE (conn, NULL, &con_handle, CON_API_PRE_FLAG_SHARD_ADMIN);
  if (error < 0)
    {
      return error;
    }

  if (con_handle->shard_admin_sock_fd == INVALID_SOCKET)
    {
      error = CCI_ER_INVALID_ARGS;
    }
  else
    {
      error = net_shard_migration_req (BRREQ_OP_CODE_MIGRATION_END,
				       &con_handle->shard_admin_sock_fd,
				       groupid, dest_nodeid, num_shard_keys,
				       con_handle);

      CLOSE_SOCKET (con_handle->shard_admin_sock_fd);
      con_handle->shard_admin_sock_fd = INVALID_SOCKET;
    }

  CON_STMT_API_POST (conn, NULL, con_handle, error);

  return error;
}

#if defined(CCI_SHARD_ADMIN)
int
cci_shard_ddl_start (CCI_CONN * conn)
{
  T_CON_HANDLE *con_handle = NULL;
  int error = 0;

  error = CON_API_PRE (conn, NULL, &con_handle, CON_API_PRE_FLAG_SHARD_ADMIN);
  if (error < 0)
    {
      return error;
    }

  if (con_handle->shard_admin_sock_fd == INVALID_SOCKET)
    {
      error = net_shard_ddl_gc_req (BRREQ_OP_CODE_DDL_START,
				    &con_handle->shard_admin_sock_fd,
				    con_handle);
    }
  else
    {
      error = CCI_ER_INVALID_ARGS;
    }

  CON_STMT_API_POST (conn, NULL, con_handle, error);

  return error;
}
#endif

#if defined(CCI_SHARD_ADMIN)
int
cci_shard_ddl_end (CCI_CONN * conn)
{
  T_CON_HANDLE *con_handle = NULL;
  int error = 0;

  error = CON_API_PRE (conn, NULL, &con_handle, CON_API_PRE_FLAG_SHARD_ADMIN);
  if (error < 0)
    {
      return error;
    }

  if (con_handle->shard_admin_sock_fd == INVALID_SOCKET)
    {
      error = CCI_ER_INVALID_ARGS;
    }
  else
    {
      error = net_shard_ddl_gc_req (BRREQ_OP_CODE_DDL_END,
				    &con_handle->shard_admin_sock_fd,
				    con_handle);

      CLOSE_SOCKET (con_handle->shard_admin_sock_fd);
      con_handle->shard_admin_sock_fd = INVALID_SOCKET;
    }

  CON_STMT_API_POST (conn, NULL, con_handle, error);

  return error;
}
#endif

int
cci_shard_gc_start (CCI_CONN * conn)
{
  T_CON_HANDLE *con_handle = NULL;
  int error = 0;

  error = CON_API_PRE (conn, NULL, &con_handle, CON_API_PRE_FLAG_SHARD_ADMIN);
  if (error < 0)
    {
      return error;
    }

  if (con_handle->shard_admin_sock_fd == INVALID_SOCKET)
    {
      error = net_shard_ddl_gc_req (BRREQ_OP_CODE_GC_START,
				    &con_handle->shard_admin_sock_fd,
				    con_handle);
    }
  else
    {
      error = CCI_ER_INVALID_ARGS;
    }

  CON_STMT_API_POST (conn, NULL, con_handle, error);

  return error;
}

int
cci_shard_gc_end (CCI_CONN * conn)
{
  T_CON_HANDLE *con_handle = NULL;
  int error = 0;

  error = CON_API_PRE (conn, NULL, &con_handle, CON_API_PRE_FLAG_SHARD_ADMIN);
  if (error < 0)
    {
      return error;
    }

  if (con_handle->shard_admin_sock_fd == INVALID_SOCKET)
    {
      error = CCI_ER_INVALID_ARGS;
    }
  else
    {
      error = net_shard_ddl_gc_req (BRREQ_OP_CODE_GC_END,
				    &con_handle->shard_admin_sock_fd,
				    con_handle);

      CLOSE_SOCKET (con_handle->shard_admin_sock_fd);
      con_handle->shard_admin_sock_fd = INVALID_SOCKET;
    }

  CON_STMT_API_POST (conn, NULL, con_handle, error);

  return error;
}

int
cci_mgmt_sync_shard_mgmt_info (const char *hostname, int mgmt_port,
			       const char *local_dbname,
			       const char *global_dbname,
			       int nodeid, int shard_mgmt_port,
			       int64_t nodeid_ver, int64_t groupid_ver,
			       char *server_name_buf,
			       int server_name_buf_size,
			       int *server_mode, int timeout_msec)
{
  T_HOST_INFO host_info;
  int error;

  if (server_name_buf_size > 0)
    {
      server_name_buf[0] = '\0';
    }
  if (server_mode != NULL)
    {
      *server_mode = HA_STATE_FOR_DRIVER_UNKNOWN;
    }

  error = ut_set_host_info (&host_info, hostname, mgmt_port);
  if (error < 0)
    {
      return error;
    }

  error = net_mgmt_shard_mgmt_info_req (&host_info, local_dbname,
					global_dbname, nodeid,
					shard_mgmt_port,
					nodeid_ver, groupid_ver,
					server_name_buf, server_name_buf_size,
					server_mode, timeout_msec);


  return error;
}

int
cci_mgmt_launch_process (T_CCI_LAUNCH_RESULT * launch_result,
			 const char *hostname, int mgmt_port,
			 int launch_proc_id, bool wait_child,
			 int argc, const char **argv,
			 int num_env, const char **envp, int timeout_msec)
{
  T_HOST_INFO host_info;
  int error;

  error = ut_set_host_info (&host_info, hostname, mgmt_port);
  if (error < 0)
    {
      return error;
    }

  error = net_mgmt_launch_process_req (launch_result, &host_info,
				       launch_proc_id,
				       (wait_child ? 1 : 0),
				       argc, argv, num_env, envp,
				       timeout_msec);

  return error;
}

int
cci_mgmt_wait_launch_process (T_CCI_LAUNCH_RESULT * launch_result,
			      int timeout_msec)
{
  return net_mgmt_wait_launch_process (launch_result, timeout_msec);
}

int
cci_mgmt_connect_db_server (const T_HOST_INFO * host, const char *dbname,
			    int timeout_msec)
{
  return net_mgmt_connect_db_server (host, dbname, timeout_msec);
}

int
cc_mgmt_count_launch_process ()
{
  return net_mgmt_count_launch_process ();
}

int
cci_host_str_to_addr (const char *host_str, unsigned char *ip_addr)
{
  return (ut_host_str_to_addr (host_str, ip_addr));
}

void
cci_set_client_functions (CCI_OR_PACK_DB_IDXKEY pack_idxkey_func,
			  CCI_DB_IDXKEY_IS_NULL idxkey_is_null_func,
			  CCI_OR_DB_IDXKEY_SIZE idxkey_size_func,
			  CCI_DB_GET_STRING db_get_string_func)
{
  cci_or_pack_db_idxkey = pack_idxkey_func;
  cci_db_idxkey_is_null = idxkey_is_null_func;
  cci_or_db_idxkey_size = idxkey_size_func;
  cci_db_get_string = db_get_string_func;
}

int
cci_send_repl_data (CCI_CONN * conn, CIRP_REPL_ITEM * head, int num_items)
{
  T_CON_HANDLE *con_handle = NULL;
  int error = 0;
  bool retry = false;

  error = CON_API_PRE (conn, NULL, &con_handle, 0);
  if (error < 0)
    {
      return error;
    }

  if (IS_OUT_TRAN (con_handle))
    {
      retry = true;
    }

  error = qe_send_repl_data (con_handle, head, num_items);

  if (retry && IS_ER_COMMUNICATION (error))
    {
      error = reset_connect (con_handle, NULL);
      if (error == CCI_ER_NO_ERROR)
	{
	  error = qe_send_repl_data (con_handle, head, num_items);
	}
    }

  CON_STMT_API_POST (conn, NULL, con_handle, error);

  return error;
}

int
cci_notify_ha_agent_state (CCI_CONN * conn, in_addr_t ip, int port, int state)
{
  T_CON_HANDLE *con_handle = NULL;
  int error = 0;

  error = CON_API_PRE (conn, NULL, &con_handle, 0);
  if (error < 0)
    {
      return error;
    }

  if (IS_OUT_TRAN (con_handle))
    {
      error = reset_connect (con_handle, NULL);
    }

  if (error == CCI_ER_NO_ERROR)
    {
      error = qe_notify_ha_agent_state (con_handle, ip, port, state);
    }

  CON_STMT_API_POST (conn, NULL, con_handle, error);

  return error;
}

/************************************************************************
 * IMPLEMENTATION OF PUBLIC FUNCTIONS                                   *
 ************************************************************************/

/************************************************************************
 * IMPLEMENTATION OF PRIVATE FUNCTIONS                                  *
 ************************************************************************/

static void
reset_error_buffer (T_CCI_ERROR * err_buf)
{
  if (err_buf != NULL)
    {
      err_buf->err_code = CCI_ER_NO_ERROR;
      err_buf->err_msg[0] = '\0';
    }
}

static int
cas_connect (T_CON_HANDLE * con_handle)
{
  int connect;

  return cas_connect_with_ret (con_handle, &connect);
}

static int
cas_connect_with_ret (T_CON_HANDLE * con_handle, int *connect)
{
  int error;

  error = cas_connect_internal (con_handle, connect);

  /* req_handle_table should be managed by list too. */
  if (((*connect) != 0) && IS_CAS_STATEMENT_POOLING (con_handle))
    {
      hm_invalidate_all_req_handle (con_handle);
    }

  return error;
}

static int
cas_connect_internal (T_CON_HANDLE * con_handle, int *connect)
{
  int error = CCI_ER_NO_ERROR;
  int i;
  int remained_time = 0;
  int retry = 0;

  assert (connect != NULL);

  *connect = 0;

  if (TIMEOUT_IS_SET (con_handle))
    {
      remained_time = (con_handle->current_timeout
		       - (int) timeval_diff_in_msec (NULL,
						     &con_handle->
						     start_time));
      if (remained_time <= 0)
	{
	  return CCI_ER_LOGIN_TIMEOUT;
	}
    }

  if (net_check_cas_request (con_handle) != 0)
    {
      if (!IS_INVALID_SOCKET (con_handle->sock_fd))
	{
	  CLOSE_SOCKET (con_handle->sock_fd);
	  con_handle->sock_fd = INVALID_SOCKET;
	  con_handle->con_status = CCI_CON_STATUS_OUT_TRAN;
	}
    }

  if (!IS_INVALID_SOCKET (con_handle->sock_fd))
    {
      return CCI_ER_NO_ERROR;
    }

  do
    {
      for (i = 0; i < con_handle->alter_hosts->count; i++)
	{
	  /* if all hosts turn out to be unreachable,
	   *  ignore host reachability and try one more time
	   */
	  if (con_is_unreachable_host (con_handle, i) == false || retry)
	    {
	      error = net_connect_srv (con_handle, i, remained_time);
	      if (error == CCI_ER_NO_ERROR)
		{
		  *connect = 1;
		  return CCI_ER_NO_ERROR;
		}

	      if (error == CCI_ER_SERVER_RESTARTED)
		{
		  return error;
		}

	      if (error == CCI_ER_COMMUNICATION
		  || error == CCI_ER_CONNECT
		  || error == CCI_ER_LOGIN_TIMEOUT
		  || error == BR_ER_FREE_SERVER)
		{
		  con_unreachable_host_add (con_handle, i);
		}
	      else
		{
		  break;
		}
	    }
	  con_handle->last_failure_time = time (NULL);
	}
      retry++;
    }
  while (retry < 2);

  if (error == CCI_ER_QUERY_TIMEOUT)
    {
      error = CCI_ER_LOGIN_TIMEOUT;
    }

  return error;
}

static T_CON_HANDLE *
get_new_connection (T_ALTER_HOST * alter_hosts,
		    const char *server_list, const char *port_name,
		    const char *db_name, const char *db_user,
		    const char *dbpasswd, const char *property_str,
		    const T_CON_PROPERTY * con_property)
{
  T_CON_HANDLE *con_handle;

  MUTEX_LOCK (con_handle_table_mutex);

  if (init_flag == 0)
    {
      hm_con_handle_table_init ();
      init_flag = 1;
    }

  con_handle = hm_con_handle_alloc (alter_hosts, server_list, port_name,
				    db_name, db_user, dbpasswd,
				    property_str, con_property);

  MUTEX_UNLOCK (con_handle_table_mutex);

  if (!cci_SIGPIPE_ignore)
    {
      signal (SIGPIPE, SIG_IGN);
      cci_SIGPIPE_ignore = 1;
    }

  return con_handle;
}

int
cci_set_allocators (CCI_MALLOC_FUNCTION malloc_func,
		    CCI_FREE_FUNCTION free_func,
		    CCI_REALLOC_FUNCTION realloc_func,
		    CCI_CALLOC_FUNCTION calloc_func)
{
  /* none or all should be set */
  if (malloc_func == NULL && free_func == NULL
      && realloc_func == NULL && calloc_func == NULL)
    {
      /* use default allocators */
      cci_malloc = malloc;
      cci_free = free;
      cci_realloc = realloc;
      cci_calloc = calloc;
    }
  else if (malloc_func == NULL || free_func == NULL
	   || realloc_func == NULL || calloc_func == NULL)
    {
      return CCI_ER_NOT_IMPLEMENTED;
    }
  else
    {
      /* use user defined allocators */
      cci_malloc = malloc_func;
      cci_free = free_func;
      cci_realloc = realloc_func;
      cci_calloc = calloc_func;
    }

  return CCI_ER_NO_ERROR;
}

static void
set_error_buffer (T_CCI_ERROR * err_buf_p,
		  int error, const char *message, ...)
{
  /* don't overwrite when err_buf->error is not equal CCI_ER_NO_ERROR */
  if (error < 0 && err_buf_p != NULL
      && err_buf_p->err_code == CCI_ER_NO_ERROR)
    {
      err_buf_p->err_code = error;

      /* Find error message from catalog when you don't give specific message */
      if (message == NULL)
	{
	  cci_get_err_msg (error, err_buf_p->err_msg,
			   sizeof (err_buf_p->err_msg));
	}
      else
	{
	  va_list args;

	  va_start (args, message);

	  vsnprintf (err_buf_p->err_msg,
		     sizeof (err_buf_p->err_msg), message, args);

	  va_end (args);
	}
    }
}

/*
 * get_last_error ()
 *   con_handle (in):
 *   dest_err_buf (out):
 */
static void
get_last_error (const T_CON_HANDLE * con_handle, T_CCI_ERROR * dest_err_buf)
{
  const char *info_type = "CAS INFO";

  if (con_handle == NULL || dest_err_buf == NULL)
    {
      return;
    }

  if (con_handle->err_buf.err_code != CCI_ER_NO_ERROR
      && con_handle->err_buf.err_msg[0] != '\0')
    {
      char svr_info_buf[256];

      dest_err_buf->err_code = con_handle->err_buf.err_code;
      snprintf (dest_err_buf->err_msg, sizeof (dest_err_buf->err_msg),
		"%s[%s-%s,%d,%d].",
		con_handle->err_buf.err_msg, info_type,
		ut_cur_host_info_to_str (svr_info_buf, con_handle),
		CON_CAS_ID (con_handle), CON_CAS_PID (con_handle));
    }
  else
    {
      copy_error_buffer (dest_err_buf, &(con_handle->err_buf));
    }

  return;
}

static void
copy_error_buffer (T_CCI_ERROR * dest_err_buf_p,
		   const T_CCI_ERROR * src_err_buf_p)
{
  if (dest_err_buf_p != NULL && src_err_buf_p != NULL)
    {
      *dest_err_buf_p = *src_err_buf_p;
    }
}

/*
 * cci_get_cas_info()
 *   info_buf (out):
 *   buf_length (in):
 *   err_buf (in):
 */
int
cci_get_cas_info (CCI_CONN * conn, char *info_buf, int buf_length)
{
  char svr_info_buf[256];
  int error = CCI_ER_NO_ERROR;
  T_CON_HANDLE *con_handle = NULL;

  error = CON_API_PRE (conn, NULL, &con_handle, 0);
  if (error < 0)
    {
      return error;
    }

  if (info_buf == NULL || buf_length <= 0)
    {
      error = CCI_ER_INVALID_ARGS;
    }
  else
    {
      snprintf (info_buf, buf_length - 1, "%s,%d,%d",
		ut_cur_host_info_to_str (svr_info_buf, con_handle),
		CON_CAS_ID (con_handle), CON_CAS_PID (con_handle));

      info_buf[buf_length - 1] = '\0';

      if (con_handle->con_property.log_trace_api)
	{
	  CCI_LOGF_DEBUG (con_handle->con_logger, "[%s]", info_buf);
	}

      error = CCI_ER_NO_ERROR;
    }

  CON_STMT_API_POST (conn, NULL, con_handle, error);

  return error;
}

int
cci_change_dbuser (CCI_CONN * conn, const char *user, const char *passwd)
{
  int error = CCI_ER_NO_ERROR;
  T_CON_HANDLE *con_handle = NULL;

  if (passwd == NULL)
    {
      passwd = "";
    }

  error = CON_API_PRE (conn, NULL, &con_handle, 0);
  if (error < 0)
    {
      return error;
    }

  if (user != NULL && strcmp (user, con_handle->db_user) != 0)
    {
      error = qe_change_dbuser (con_handle, user, passwd);
    }

  CON_STMT_API_POST (conn, NULL, con_handle, error);

  return error;
}

static int
con_api_pre (CCI_CONN * conn, CCI_STMT * stmt, T_CON_HANDLE ** ret_con_handle,
	     int flag, const char *func_name)
{
  int error;
  T_CON_HANDLE *con_handle;
  bool force_conn;
  bool flag_init_stmt;

  flag_init_stmt = ((flag & CON_API_PRE_FLAG_INIT_STMT) ? true : false);
  force_conn = ((flag & CON_API_PRE_FLAG_FORCE_CONN) ? true : false);

  if (conn == NULL)
    {
      return CCI_ER_INVALID_ARGS;
    }

  if (flag_init_stmt)
    {
      if (stmt == NULL)
	{
	  return CCI_ER_INVALID_ARGS;
	}
      reset_error_buffer (&stmt->err_buf);
    }

  reset_error_buffer (&conn->err_buf);

  error = hm_get_connection (&conn->conn_handle_id, &con_handle, force_conn);
  if (error != CCI_ER_NO_ERROR)
    {
      if (flag_init_stmt)
	{
	  set_error_buffer (&stmt->err_buf, error, NULL);
	}
      set_error_buffer (&conn->err_buf, error, NULL);
      return error;
    }

  API_SLOG (con_handle, NULL, func_name);

  if (flag & CON_API_PRE_FLAG_SHARD_ADMIN)
    {
      assert (force_conn == false);

      if (IS_CON_TYPE_LOCAL (con_handle))
	{
	  return con_stmt_api_post (conn, stmt, con_handle,
				    CCI_ER_NOT_SHARDING_CONNECTION,
				    func_name);
	}
      if (strcmp (con_handle->db_user, "dba") != 0)
	{
	  return con_stmt_api_post (conn, stmt, con_handle,
				    CCI_ER_SHARD_NOT_ALLOWED_USER, func_name);
	}
    }

  reset_error_buffer (&con_handle->err_buf);
  *ret_con_handle = con_handle;

  return CCI_ER_NO_ERROR;
}

static int
stmt_api_pre (CCI_STMT * stmt, T_CON_HANDLE ** ret_con_handle,
	      T_REQ_HANDLE ** ret_req_handle, const char *func_name)
{
  int error;
  T_CON_HANDLE *con_handle = NULL;
  T_REQ_HANDLE *req_handle = NULL;

  if (stmt == NULL)
    {
      return CCI_ER_INVALID_ARGS;
    }

  reset_error_buffer (&stmt->err_buf);

  error = hm_get_statement (stmt, &con_handle, &req_handle);
  if (error < 0)
    {
      set_error_buffer (&stmt->err_buf, error, NULL);
      return error;
    }

  reset_error_buffer (&con_handle->err_buf);

  *ret_con_handle = con_handle;
  *ret_req_handle = req_handle;

  API_SLOG (con_handle, stmt, func_name);
  return CCI_ER_NO_ERROR;
}

static int
con_stmt_api_post (CCI_CONN * conn, CCI_STMT * stmt,
		   T_CON_HANDLE * con_handle, int error,
		   const char *func_name)
{
  if (con_handle != NULL)
    {
      if (error < 0)
	{
	  set_error_buffer (&con_handle->err_buf, error, NULL);
	  if (conn != NULL)
	    {
	      get_last_error (con_handle, &conn->err_buf);
	    }
	  if (stmt != NULL)
	    {
	      get_last_error (con_handle, &stmt->err_buf);
	    }
	}

      con_handle->used = false;
    }
  else if (conn != NULL)
    {
      set_error_buffer (&conn->err_buf, error, NULL);
    }

  API_ELOG (con_handle, stmt, error, func_name);
  return error;
}

/*
 * cci_update_db_group_id()
 *   group_id (in):
 */
int
cci_update_db_group_id (CCI_CONN * conn, int migrator_id,
			int group_id, int target, bool on_off)
{
  T_CON_HANDLE *con_handle = NULL;
  int error = 0;

  error = CON_API_PRE (conn, NULL, &con_handle, 0);
  if (error < 0)
    {
      return error;
    }

  if (IS_OUT_TRAN (con_handle))
    {
      error = reset_connect (con_handle, NULL);
    }

  if (error == CCI_ER_NO_ERROR)
    {
      error = qe_update_db_group_id (con_handle, migrator_id, group_id,
				     target, on_off);
    }

  CON_STMT_API_POST (conn, NULL, con_handle, error);

  return error;
}

/*
 * cci_insert_gid_removed_info()
 *   group_id (in):
 */
int
cci_insert_gid_removed_info (CCI_CONN * conn, int group_id)
{
  T_CON_HANDLE *con_handle = NULL;
  int error = 0;

  error = CON_API_PRE (conn, NULL, &con_handle, 0);
  if (error < 0)
    {
      return error;
    }

  if (IS_OUT_TRAN (con_handle))
    {
      error = reset_connect (con_handle, NULL);
    }

  if (error == CCI_ER_NO_ERROR)
    {
      error = qe_insert_gid_removed_info (con_handle, group_id);
    }

  CON_STMT_API_POST (conn, NULL, con_handle, error);

  return error;
}

/*
 * cci_delete_gid_removed_info()
 *   group_id (in):
 */
int
cci_delete_gid_removed_info (CCI_CONN * conn, int group_id)
{
  T_CON_HANDLE *con_handle = NULL;
  int error = 0;

  error = CON_API_PRE (conn, NULL, &con_handle, 0);
  if (error < 0)
    {
      return error;
    }

  if (IS_OUT_TRAN (con_handle))
    {
      error = reset_connect (con_handle, NULL);
    }

  if (error == CCI_ER_NO_ERROR)
    {
      error = qe_delete_gid_removed_info (con_handle, group_id);
    }

  CON_STMT_API_POST (conn, NULL, con_handle, error);

  return error;
}

/*
 * cci_delete_gid_skey_info()
 *   group_id (in):
 */
int
cci_delete_gid_skey_info (CCI_CONN * conn, int group_id)
{
  T_CON_HANDLE *con_handle = NULL;
  int error = 0;

  error = CON_API_PRE (conn, NULL, &con_handle, 0);
  if (error < 0)
    {
      return error;
    }

  if (IS_OUT_TRAN (con_handle))
    {
      error = reset_connect (con_handle, NULL);
    }

  if (error == CCI_ER_NO_ERROR)
    {
      error = qe_delete_gid_skey_info (con_handle, group_id);
    }

  CON_STMT_API_POST (conn, NULL, con_handle, error);

  return error;
}

/*
 * cci_block_global_dml()
 */
int
cci_block_global_dml (CCI_CONN * conn, bool start_or_end)
{
  T_CON_HANDLE *con_handle = NULL;
  int error = 0;

  error = CON_API_PRE (conn, NULL, &con_handle, 0);
  if (error < 0)
    {
      return error;
    }

  if (IS_OUT_TRAN (con_handle))
    {
      error = reset_connect (con_handle, NULL);
    }

  if (error == CCI_ER_NO_ERROR)
    {
      error = qe_block_global_dml (con_handle, start_or_end);
    }

  CON_STMT_API_POST (conn, NULL, con_handle, error);

  return error;
}

int
cci_get_server_mode (CCI_CONN * conn, int *server_mode,
		     unsigned int *master_addr)
{
  T_CON_HANDLE *con_handle = NULL;
  int error = CCI_ER_NO_ERROR;
  int mode = -1;
  unsigned int addr = INADDR_NONE;

  error = CON_API_PRE (conn, NULL, &con_handle, 0);
  if (error < 0)
    {
      goto end;
    }

  error = qe_get_server_mode (con_handle, &mode, &addr);

  CON_STMT_API_POST (conn, NULL, con_handle, error);

end:
  if (server_mode)
    {
      *server_mode = mode;
    }

  if (master_addr)
    {
      *master_addr = addr;
    }

  return error;
}
