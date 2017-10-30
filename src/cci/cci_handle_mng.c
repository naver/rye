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
 * cci_handle_mng.c -
 */

#ident "$Id$"

/************************************************************************
 * IMPORTED SYSTEM HEADER FILES						*
 ************************************************************************/
#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <netdb.h>
#include <pthread.h>

/************************************************************************
 * OTHER IMPORTED HEADER FILES						*
 ************************************************************************/

#include "cci_common.h"
#include "cci_handle_mng.h"
#include "cas_cci.h"
#include "cci_util.h"
#include "cci_query_execute.h"
#include "cas_protocol.h"
#include "cci_network.h"
#include "cci_log.h"

/************************************************************************
 * PRIVATE DEFINITIONS							*
 ************************************************************************/

#define MAX_CON_HANDLE                  2048

#define REQ_HANDLE_ALLOC_SIZE           256

#define CCI_MAX_CONNECTION_POOL         256

/************************************************************************
 * PRIVATE TYPE DEFINITIONS						*
 ************************************************************************/

#define UNREACHABLE_HOST_MAX_COUNT	MAX_CON_HANDLE
typedef struct
{
  T_HOST_INFO host;		/* host info (ip, port) */
  char *port_name;
} T_UNREACHABLE_HOST;

static T_UNREACHABLE_HOST unreachable_host[UNREACHABLE_HOST_MAX_COUNT];
static int unreachable_host_count = 0;
T_MUTEX unreachable_host_mutex = PTHREAD_MUTEX_INITIALIZER;

#define PORT_NAME_CACHE_MAX	100
static char *port_name_cache[PORT_NAME_CACHE_MAX];
static int port_name_cache_count = 0;
T_MUTEX port_name_cache_mutex = PTHREAD_MUTEX_INITIALIZER;

/************************************************************************
 * PRIVATE FUNCTION PROTOTYPES						*
 ************************************************************************/

static int init_con_handle (T_CON_HANDLE * con_handle, int con_id,
			    T_ALTER_HOST * alter_hosts,
			    const char *server_list, const char *port_name,
			    const char *db_name, const char *db_user,
			    const char *db_passwd, const char *property_str,
			    const T_CON_PROPERTY * con_property);
static int new_req_handle_id (T_CON_HANDLE * con_handle,
			      T_REQ_HANDLE ** ret_req_handle);
static void con_handle_content_free (T_CON_HANDLE * con_handle);
static int hm_handle_id_compare (CCI_HANDLE_ID * handle1,
				 CCI_HANDLE_ID * handle2);
static bool hm_is_null_handle_id (CCI_HANDLE_ID * handle_id);
static int64_t hm_con_handle_req_id_seq_next (T_CON_HANDLE * con_handle);
static int64_t hm_con_id_seq_next (void);
static void hm_set_null_handle_id (CCI_HANDLE_ID * handle_id);

static THREAD_RET_T THREAD_CALLING_CONVENTION
hm_thread_health_checker (void *arg);

/************************************************************************
 * INTERFACE VARIABLES							*
 ************************************************************************/

/************************************************************************
 * PUBLIC VARIABLES							*
 ************************************************************************/

/************************************************************************
 * PRIVATE VARIABLES							*
 ************************************************************************/

static T_CON_HANDLE *con_handle_table[MAX_CON_HANDLE];

static int64_t con_id_seq_next = 0;

/************************************************************************
 * IMPLEMENTATION OF PUBLIC FUNCTIONS	 				*
 ************************************************************************/

char *
con_port_name_cache_put (const char *port_name)
{
  int i;
  char *cached_port_name = NULL;

  for (i = 0; i < port_name_cache_count; i++)
    {
      if (strcmp (port_name, port_name_cache[i]) == 0)
	{
	  return port_name_cache[i];
	}
    }

  /* cache not found */
  MUTEX_LOCK (port_name_cache_mutex);
  if (port_name_cache_count < PORT_NAME_CACHE_MAX)
    {
      for (i = 0; i < port_name_cache_count; i++)
	{
	  if (strcmp (port_name, port_name_cache[i]) == 0)
	    {
	      cached_port_name = port_name_cache[i];
	      break;
	    }
	}

      if (cached_port_name == NULL)
	{
	  cached_port_name = strdup (port_name);
	  if (cached_port_name != NULL)
	    {
	      port_name_cache[port_name_cache_count] = cached_port_name;
	      port_name_cache_count++;
	    }
	}
    }
  MUTEX_UNLOCK (port_name_cache_mutex);

  return cached_port_name;
}

static bool
unreachable_host_compare (const T_UNREACHABLE_HOST * unreachable_host,
			  const T_HOST_INFO * host_info,
			  const char *port_name)
{
  if (memcmp (unreachable_host->host.ip_addr, host_info->ip_addr, 4) == 0
      && unreachable_host->host.port == host_info->port
      && strcmp (unreachable_host->port_name, port_name) == 0)
    {
      return true;
    }
  else
    {
      return false;
    }
}

bool
con_is_unreachable_host (const T_CON_HANDLE * con_handle, int host_id)
{
  int i, count;
  T_UNREACHABLE_HOST tmp_unreachable_host;
  T_HOST_INFO *host_info;
  char *port_name;

  host_info = &con_handle->alter_hosts->host_info[host_id];
  port_name = con_handle->port_name;

  /* unreachable_host_count may be changed while compare.
     should copy variable and test */
  count = unreachable_host_count;

  for (i = 0; i < count; i++)
    {
      tmp_unreachable_host = unreachable_host[i];
      if (unreachable_host_compare
	  (&tmp_unreachable_host, host_info, port_name) == true)
	{
	  return true;
	}
    }

  return false;
}

void
con_unreachable_host_add (const T_CON_HANDLE * con_handle, int host_id)
{
  if (con_is_unreachable_host (con_handle, host_id))
    {
      return;
    }

  MUTEX_LOCK (unreachable_host_mutex);

  if (con_is_unreachable_host (con_handle, host_id) == false &&
      unreachable_host_count < UNREACHABLE_HOST_MAX_COUNT)
    {
      T_UNREACHABLE_HOST tmp_unreachable_host;

      tmp_unreachable_host.host = con_handle->alter_hosts->host_info[host_id];
      tmp_unreachable_host.port_name =
	con_port_name_cache_put (con_handle->port_name);

      if (tmp_unreachable_host.port_name != NULL)
	{
	  unreachable_host[unreachable_host_count++] = tmp_unreachable_host;
	}
    }

  MUTEX_UNLOCK (unreachable_host_mutex);
}

/* con_unreachable_host_remove() function should be called by
 * health_checker thread
 */
static void
con_unreachable_host_remove (const T_HOST_INFO * host_info,
			     const char *port_name)
{
  int i;

  MUTEX_LOCK (unreachable_host_mutex);

  for (i = 0; i < unreachable_host_count; i++)
    {
      if (unreachable_host_compare
	  (&unreachable_host[i], host_info, port_name) == true)
	{
	  unreachable_host[i] = unreachable_host[unreachable_host_count - 1];
	  unreachable_host_count--;
	  break;
	}
    }

  MUTEX_UNLOCK (unreachable_host_mutex);
}

void
hm_con_handle_table_init ()
{
  int i;

  for (i = 0; i < MAX_CON_HANDLE; i++)
    {
      con_handle_table[i] = NULL;
    }
}

T_CON_HANDLE *
hm_con_handle_alloc (T_ALTER_HOST * alter_hosts,
		     const char *server_list, const char *port_name,
		     const char *db_name, const char *db_user,
		     const char *db_passwd, const char *property_str,
		     const T_CON_PROPERTY * con_property)
{
  int i, handle_id;
  T_CON_HANDLE *con_handle = NULL;

  for (i = 0; i < MAX_CON_HANDLE; i++)
    {
      con_handle = con_handle_table[i];
      handle_id = i + 1;

      if (con_handle == NULL)
	{
	  con_handle = (T_CON_HANDLE *) MALLOC (sizeof (T_CON_HANDLE));
	  con_handle_table[i] = con_handle;
	  break;
	}
      else if (hm_is_null_handle_id (&con_handle->con_handle_id))
	{
	  break;
	}
      else
	{
	  con_handle = NULL;
	}
    }

  if (con_handle == NULL)
    {
      return NULL;
    }

  if (init_con_handle (con_handle, handle_id, alter_hosts, server_list,
		       port_name, db_name, db_user, db_passwd,
		       property_str, con_property) < 0)
    {
      return NULL;
    }

  hm_set_handle_id (&con_handle->con_handle_id,
		    handle_id, hm_con_id_seq_next ());

  return con_handle;
}

int
hm_con_handle_free (T_CON_HANDLE * con_handle)
{
  if (con_handle == NULL)
    {
      return CCI_ER_CON_HANDLE;
    }

  if (!IS_INVALID_SOCKET (con_handle->sock_fd))
    {
      CLOSE_SOCKET (con_handle->sock_fd);
      con_handle->sock_fd = INVALID_SOCKET;
    }

  hm_req_handle_free_all (con_handle);
  con_handle_content_free (con_handle);

  hm_set_null_handle_id (&con_handle->con_handle_id);

  return CCI_ER_NO_ERROR;
}

int
hm_req_handle_alloc (T_CON_HANDLE * con_handle,
		     T_REQ_HANDLE ** ret_req_handle)
{
  int req_handle_id;
  T_REQ_HANDLE *req_handle = NULL;

  *ret_req_handle = NULL;

  req_handle_id = new_req_handle_id (con_handle, &req_handle);
  if (req_handle_id < 0)
    {
      return (req_handle_id);
    }

  if (req_handle == NULL)
    {
      req_handle = (T_REQ_HANDLE *) MALLOC (sizeof (T_REQ_HANDLE));
      if (req_handle == NULL)
	{
	  return CCI_ER_NO_MORE_MEMORY;
	}
    }

  memset (req_handle, 0, sizeof (T_REQ_HANDLE));

  hm_set_handle_id (&req_handle->req_handle_id, req_handle_id,
		    hm_con_handle_req_id_seq_next (con_handle));

  req_handle->query_timeout = con_handle->con_property.query_timeout;

  con_handle->req_handle_table[req_handle_id - 1] = req_handle;
  ++(con_handle->req_handle_count);

  *ret_req_handle = req_handle;
  return CCI_ER_NO_ERROR;
}

T_CCI_ERROR_CODE
hm_get_connection (CCI_HANDLE_ID * conn_handle_id,
		   T_CON_HANDLE ** ret_con_handle, bool force)
{
  int connection_id;
  T_CON_HANDLE *con_handle;

  connection_id = conn_handle_id->id;

  if (connection_id < 1 || connection_id > MAX_CON_HANDLE)
    {
      return CCI_ER_CON_HANDLE;
    }

  con_handle = con_handle_table[connection_id - 1];
  if (con_handle == NULL ||
      hm_handle_id_compare (conn_handle_id, &con_handle->con_handle_id) != 0)
    {
      return CCI_ER_CON_HANDLE;
    }

  if (force == false)
    {
      while (__sync_bool_compare_and_swap (&con_handle->lock, 0, 1) == false)
	{
	}

      if (con_handle->used)
	{
	  con_handle->lock = 0;
	  return CCI_ER_USED_CONNECTION;
	}
      else
	{
	  con_handle->used = true;
	}

      con_handle->lock = 0;
    }

  *ret_con_handle = con_handle;

  return CCI_ER_NO_ERROR;
}

T_CCI_ERROR_CODE
hm_get_statement (CCI_STMT * stmt, T_CON_HANDLE ** ret_con_handle,
		  T_REQ_HANDLE ** ret_req_handle)
{
  int statement_id;
  T_CON_HANDLE *con_handle = NULL;
  T_REQ_HANDLE *req_handle = NULL;
  T_CCI_ERROR_CODE error;

  error = hm_get_connection (&stmt->conn_handle_id, &con_handle, false);
  if (error != CCI_ER_NO_ERROR)
    {
      return error;
    }

  statement_id = stmt->stmt_handle_id.id;

  if (statement_id < 1 || statement_id > con_handle->max_req_handle)
    {
      goto req_handle_err;
    }

  req_handle = con_handle->req_handle_table[statement_id - 1];
  if (req_handle == NULL ||
      hm_handle_id_compare (&stmt->stmt_handle_id,
			    &req_handle->req_handle_id) != 0)
    {
      goto req_handle_err;
    }

  *ret_con_handle = con_handle;
  *ret_req_handle = req_handle;

  return CCI_ER_NO_ERROR;

req_handle_err:
  con_handle->used = false;
  return CCI_ER_REQ_HANDLE;
}

void
hm_req_handle_free (T_CON_HANDLE * con_handle, T_REQ_HANDLE * req_handle)
{
  --(con_handle->req_handle_count);

  req_handle_content_free (req_handle, 0);

  hm_set_null_handle_id (&req_handle->req_handle_id);
}

void
hm_req_handle_free_all (T_CON_HANDLE * con_handle)
{
  int i;
  T_REQ_HANDLE *req_handle = NULL;

  for (i = 0; i < con_handle->max_req_handle; i++)
    {
      req_handle = con_handle->req_handle_table[i];
      if (req_handle == NULL)
	{
	  continue;
	}

      hm_req_handle_free (con_handle, req_handle);
    }
}

void
hm_req_handle_free_all_unholdable (T_CON_HANDLE * con_handle)
{
  int i;
  T_REQ_HANDLE *req_handle = NULL;

  for (i = 0; i < con_handle->max_req_handle; i++)
    {
      req_handle = con_handle->req_handle_table[i];
      if (req_handle == NULL)
	{
	  continue;
	}
      if ((req_handle->prepare_flag & CCI_PREPARE_HOLDABLE) != 0)
	{
	  /* do not free holdable req_handles */
	  continue;
	}

      hm_req_handle_free (con_handle, req_handle);
    }
}

void
hm_req_handle_close_all_resultsets (T_CON_HANDLE * con_handle)
{
  int i;
  T_REQ_HANDLE *req_handle = NULL;

  for (i = 0; i < con_handle->max_req_handle; i++)
    {
      req_handle = con_handle->req_handle_table[i];
      if (req_handle == NULL)
	{
	  continue;
	}

      if ((req_handle->prepare_flag & CCI_PREPARE_HOLDABLE) != 0
	  && !req_handle->is_from_current_transaction)
	{
	  continue;
	}

      req_handle->is_closed = 1;
    }
}

void
hm_req_handle_close_all_unholdable_resultsets (T_CON_HANDLE * con_handle)
{
  int i;
  T_REQ_HANDLE *req_handle = NULL;

  for (i = 0; i < con_handle->max_req_handle; i++)
    {
      req_handle = con_handle->req_handle_table[i];
      if (req_handle == NULL)
	{
	  continue;
	}

      if ((req_handle->prepare_flag & CCI_PREPARE_HOLDABLE) != 0)
	{
	  /* skip holdable req_handles */
	  req_handle->is_from_current_transaction = 0;
	  continue;
	}

      req_handle->is_closed = 1;
    }
}

void
hm_req_handle_fetch_buf_free (T_REQ_HANDLE * req_handle)
{
  int fetched_tuple;

  fetched_tuple = req_handle->fetched_tuple_end -
    req_handle->fetched_tuple_begin + 1;
  TUPLE_VALUE_FREE (req_handle->tuple_value,
		    fetched_tuple, req_handle->num_col_info);

  FREE_MEM (req_handle->net_res);
  req_handle->fetched_tuple_begin = req_handle->fetched_tuple_end = 0;
  req_handle->cur_fetch_tuple_index = -1;
}

int
hm_conv_value_buf_alloc (T_VALUE_BUF * val_buf, int size)
{
  if (size <= val_buf->size)
    {
      return 0;
    }

  FREE_MEM (val_buf->data);
  val_buf->size = 0;

  val_buf->data = MALLOC (size);
  if (val_buf->data == NULL)
    {
      return CCI_ER_NO_MORE_MEMORY;
    }

  val_buf->size = size;
  return 0;
}

void
hm_invalidate_all_req_handle (T_CON_HANDLE * con_handle)
{

  int i;
  int count = 0;
  T_REQ_HANDLE *curr_req_handle;

  for (i = 0; i < con_handle->max_req_handle; ++i)
    {
      if (count == con_handle->req_handle_count)
	{
	  break;
	}

      curr_req_handle = con_handle->req_handle_table[i];
      if (curr_req_handle == NULL)
	{
	  continue;
	}

      curr_req_handle->valid = 0;
      ++count;
    }
}

void
hm_conv_value_buf_clear (T_VALUE_BUF * val_buf)
{
  FREE_MEM (val_buf->data);
  val_buf->size = 0;
}

void
req_handle_col_info_free (T_REQ_HANDLE * req_handle)
{
  if (req_handle->col_info != NULL)
    {
      COL_INFO_FREE (req_handle->col_info, req_handle->num_col_info);
    }
}

void
req_handle_content_free (T_REQ_HANDLE * req_handle, int reuse)
{
  /*
     For reusing invalidated req handle,
     sql_text and prepare flag of req handle are needed.
     So, they must not be freed.
   */

  req_close_query_result (req_handle);
  req_handle_col_info_free (req_handle);

  if (!reuse)
    {
      FREE_MEM (req_handle->sql_text);

      qe_bind_value_free (req_handle);
      FREE_MEM (req_handle->bind_value);
    }
  req_handle->valid = 0;
}

int
req_close_query_result (T_REQ_HANDLE * req_handle)
{
  assert (req_handle != NULL);

  hm_req_handle_fetch_buf_free (req_handle);
  hm_conv_value_buf_clear (&(req_handle->conv_value_buffer));

  if (req_handle->num_query_res == 0 || req_handle->qr == NULL)
    {
      assert (req_handle->num_query_res == 0 && req_handle->qr == NULL);

      return CCI_ER_RESULT_SET_CLOSED;
    }

  QUERY_RESULT_FREE (req_handle);

  return CCI_ER_NO_ERROR;
}

void
hm_set_con_handle_holdable (T_CON_HANDLE * con_handle, int holdable)
{
  con_handle->is_holdable = holdable;
}

int
hm_get_con_handle_holdable (T_CON_HANDLE * con_handle)
{
  if (con_handle->is_holdable)
    {
      if (IS_CAS_HOLDABLE_RESULT (con_handle))
	{
	  return true;
	}
    }

  return false;
}

int
hm_get_req_handle_holdable (T_CON_HANDLE * con_handle,
			    T_REQ_HANDLE * req_handle)
{
  assert (con_handle != NULL && req_handle != NULL);

  if ((req_handle->prepare_flag & CCI_PREPARE_HOLDABLE) != 0)
    {
      if (IS_CAS_HOLDABLE_RESULT (con_handle))
	{
	  return true;
	}
    }

  return false;
}

void
hm_check_rc_time (T_CON_HANDLE * con_handle)
{
  time_t cur_time, failure_time;

  if (IS_INVALID_SOCKET (con_handle->sock_fd))
    {
      return;
    }

  if (con_handle->alter_hosts->cur_id > 0 &&
      con_handle->con_property.rc_time > 0)
    {
      cur_time = time (NULL);
      failure_time = con_handle->last_failure_time;
      if (failure_time > 0 &&
	  con_handle->con_property.rc_time < (cur_time - failure_time))
	{
	  if (con_is_unreachable_host (con_handle, 0) == false)
	    {
	      con_handle->force_failback = true;
	      con_handle->last_failure_time = 0;
	    }
	}
    }
}

void
hm_create_health_check_th (void)
{
  int rv;
  pthread_attr_t thread_attr;
  pthread_t health_check_th;

  rv = pthread_attr_init (&thread_attr);
  assert (rv == 0);
  rv = pthread_attr_setdetachstate (&thread_attr, PTHREAD_CREATE_DETACHED);
  assert (rv == 0);
  rv = pthread_attr_setscope (&thread_attr, PTHREAD_SCOPE_SYSTEM);
  assert (rv == 0);
  rv =
    pthread_create (&health_check_th, &thread_attr, hm_thread_health_checker,
		    (void *) NULL);
  assert (rv == 0);
}

void
hm_set_conn_handle_id (CCI_CONN * conn, const T_CON_HANDLE * con_handle)
{
  if (conn == NULL)
    {
      return;
    }

  if (con_handle == NULL)
    {
      hm_set_null_handle_id (&conn->conn_handle_id);
    }
  else
    {
      conn->conn_handle_id = con_handle->con_handle_id;
    }
}

void
hm_set_stmt_handle_id (CCI_STMT * stmt, const T_CON_HANDLE * con_handle,
		       const T_REQ_HANDLE * req_handle)
{
  if (stmt == NULL)
    {
      return;
    }

  if (con_handle == NULL)
    {
      hm_set_null_handle_id (&stmt->conn_handle_id);
    }
  else
    {
      stmt->conn_handle_id = con_handle->con_handle_id;
    }

  if (req_handle == NULL)
    {
      hm_set_null_handle_id (&stmt->stmt_handle_id);
    }
  else
    {
      stmt->stmt_handle_id = req_handle->req_handle_id;
    }
}

void
hm_set_handle_id (CCI_HANDLE_ID * handle_id, int id, int64_t id_seq)
{
  handle_id->id = id;
  handle_id->id_seq = id_seq;
}

/************************************************************************
 * IMPLEMENTATION OF PRIVATE FUNCTIONS	 				*
 ************************************************************************/

static int
hm_handle_id_compare (CCI_HANDLE_ID * handle1, CCI_HANDLE_ID * handle2)
{
  if (handle1->id == handle2->id && handle1->id_seq == handle2->id_seq)
    {
      return 0;
    }
  else
    {
      return -1;
    }
}

static bool
hm_is_null_handle_id (CCI_HANDLE_ID * handle_id)
{
  if (handle_id->id == 0)
    {
      return true;
    }
  else
    {
      return false;
    }
}

static void
hm_set_null_handle_id (CCI_HANDLE_ID * handle_id)
{
  memset (handle_id, 0, sizeof (CCI_HANDLE_ID));
}

static int64_t
hm_con_handle_req_id_seq_next (T_CON_HANDLE * con_handle)
{
  con_handle->req_id_seq_next++;
  return con_handle->req_id_seq_next;
}

static int64_t
hm_con_id_seq_next ()
{
  con_id_seq_next++;
  return con_id_seq_next;
}

static void *
make_con_logger (const T_CON_PROPERTY * con_property, int con_id)
{
  if (con_property->log_on_exception || con_property->log_slow_queries
      || con_property->log_trace_api || con_property->log_trace_network)
    {
      Logger logger;
      char *file = con_property->log_filename;
      char *base = con_property->log_base;
      char path[PATH_MAX];

      if (file == NULL)
	{
	  if (base == NULL)
	    {
	      snprintf (path, PATH_MAX, "cci_%04d.log", con_id);
	    }
	  else
	    {
	      snprintf (path, PATH_MAX, "%s/cci_%04d.log", base, con_id);
	    }
	}
      else
	{
	  if (base == NULL)
	    {
	      snprintf (path, PATH_MAX, "%s", file);
	    }
	  else
	    {
	      snprintf (path, PATH_MAX, "%s/%s", base, file);
	    }
	}

      logger = cci_log_get (path);
      if (logger != NULL)
	{
	  cci_log_set_level (logger, CCI_LOG_LEVEL_DEBUG);
	}

      return logger;
    }
  else
    {
      return NULL;
    }
}

static int
init_con_handle (T_CON_HANDLE * con_handle, int con_id,
		 T_ALTER_HOST * alter_hosts, const char *server_list,
		 const char *port_name, const char *db_name,
		 const char *db_user, const char *db_passwd,
		 const char *property_str,
		 const T_CON_PROPERTY * con_property)
{
  memset (con_handle, 0, sizeof (T_CON_HANDLE));

  con_handle->port_name = con_port_name_cache_put (port_name);
  if (con_handle->port_name == NULL)
    {
      return CCI_ER_NO_MORE_MEMORY;
    }
  ALLOC_COPY_STR (con_handle->server_list, server_list);
  ALLOC_COPY_STR (con_handle->db_name, db_name);
  ALLOC_COPY_STR (con_handle->db_user, db_user);
  ALLOC_COPY_STR (con_handle->db_passwd, db_passwd);
  ut_make_url (con_handle->url_for_logging, SRV_CON_URL_SIZE,
	       server_list, db_name, db_user, "********",
	       port_name, property_str);
  con_handle->sock_fd = -1;
  con_handle->isolation_level = CCI_TRAN_UNKNOWN_ISOLATION;
  con_handle->lock_timeout = CCI_LOCK_TIMEOUT_DEFAULT;
  con_handle->is_retry = 0;
  con_handle->used = false;
  con_handle->con_status = CCI_CON_STATUS_OUT_TRAN;
  con_handle->autocommit_mode = CCI_AUTOCOMMIT_TRUE;

  con_handle->max_req_handle = REQ_HANDLE_ALLOC_SIZE;
  con_handle->req_handle_table = (T_REQ_HANDLE **)
    MALLOC (sizeof (T_REQ_HANDLE *) * con_handle->max_req_handle);
  if (con_handle->req_handle_table == NULL)
    {
      FREE_MEM (con_handle->server_list);
      FREE_MEM (con_handle->db_name);
      FREE_MEM (con_handle->db_user);
      FREE_MEM (con_handle->db_passwd);
      memset (con_handle, 0, sizeof (T_CON_HANDLE));
      return CCI_ER_NO_MORE_MEMORY;
    }

  con_handle->stmt_pool = cci_mht_create (0, 1000, cci_mht_5strhash,
					  cci_mht_strcasecmpeq);
  if (con_handle->stmt_pool == NULL)
    {
      FREE_MEM (con_handle->server_list);
      FREE_MEM (con_handle->db_name);
      FREE_MEM (con_handle->db_user);
      FREE_MEM (con_handle->db_passwd);
      FREE_MEM (con_handle->req_handle_table);
      return CCI_ER_NO_MORE_MEMORY;
    }

  memset (con_handle->req_handle_table,
	  0, sizeof (T_REQ_HANDLE *) * con_handle->max_req_handle);
  con_handle->req_handle_count = 0;
  con_handle->open_prepared_statement_count = 0;

  cas_status_info_init (CON_CAS_STATUS_INFO (con_handle));

  con_handle->alter_hosts = alter_hosts;
  con_handle->force_failback = false;
  con_handle->last_failure_time = 0;
  con_handle->start_time.tv_sec = 0;
  con_handle->start_time.tv_usec = 0;
  con_handle->current_timeout = 0;

  con_handle->deferred_max_close_handle_count =
    DEFERRED_CLOSE_HANDLE_ALLOC_SIZE;
  con_handle->deferred_close_handle_list =
    (int *) MALLOC (sizeof (int) *
		    con_handle->deferred_max_close_handle_count);
  con_handle->deferred_close_handle_count = 0;

  con_handle->is_holdable = 1;

  con_handle->con_property = *con_property;
  con_handle->con_logger = make_con_logger (con_property, con_id);

  con_handle->shard_admin_sock_fd = INVALID_SOCKET;

  return 0;
}

static int
hm_req_handle_table_expand (T_CON_HANDLE * con_handle)
{
  int new_max_req_handle;
  T_REQ_HANDLE **new_req_handle_table = NULL;
  int handle_id;

  new_max_req_handle = con_handle->max_req_handle + REQ_HANDLE_ALLOC_SIZE;
  new_req_handle_table = (T_REQ_HANDLE **)
    REALLOC (con_handle->req_handle_table,
	     sizeof (T_REQ_HANDLE *) * new_max_req_handle);
  if (new_req_handle_table == NULL)
    {
      return CCI_ER_NO_MORE_MEMORY;
    }

  handle_id = con_handle->max_req_handle + 1;

  memset (new_req_handle_table + con_handle->max_req_handle, 0,
	  REQ_HANDLE_ALLOC_SIZE * sizeof (T_REQ_HANDLE *));

  con_handle->max_req_handle = new_max_req_handle;
  con_handle->req_handle_table = new_req_handle_table;

  return handle_id;
}

static int
new_req_handle_id (T_CON_HANDLE * con_handle, T_REQ_HANDLE ** ret_req_handle)
{
  int i;
  int handle_id = 0;

  for (i = 0; i < con_handle->max_req_handle; i++)
    {
      T_REQ_HANDLE *req_handle;

      req_handle = con_handle->req_handle_table[i];
      if (req_handle == NULL ||
	  hm_is_null_handle_id (&req_handle->req_handle_id))
	{
	  *ret_req_handle = req_handle;
	  return (i + 1);
	}
    }

  handle_id = hm_req_handle_table_expand (con_handle);

  return handle_id;
}

static void
con_handle_content_free (T_CON_HANDLE * con_handle)
{
  int i;

  FREE_MEM (con_handle->db_name);
  FREE_MEM (con_handle->db_user);
  FREE_MEM (con_handle->db_passwd);
  FREE_MEM (con_handle->server_list);
  con_handle->url_for_logging[0] = '\0';
  FREE_MEM (con_handle->deferred_close_handle_list);

  for (i = 0; i < con_handle->max_req_handle; i++)
    {
      FREE_MEM (con_handle->req_handle_table[i]);
    }
  FREE_MEM (con_handle->req_handle_table);

  if (con_handle->stmt_pool != NULL)
    {
      cci_mht_destroy (con_handle->stmt_pool, true, true);
    }
  FREE_MEM (con_handle->alter_hosts);
  con_property_free (&con_handle->con_property);
}

static THREAD_RET_T THREAD_CALLING_CONVENTION
hm_thread_health_checker (UNUSED_ARG void *arg)
{
  int i;
  time_t start_time;
  time_t elapsed_time;
  const T_HOST_INFO *host_info;
  const char *port_name;

  while (1)
    {
      start_time = time (NULL);
      for (i = 0; i < unreachable_host_count; i++)
	{
	  host_info = &unreachable_host[i].host;
	  port_name = unreachable_host[i].port_name;

	  if (net_check_broker_alive (host_info, port_name,
				      BROKER_HEALTH_CHECK_TIMEOUT))
	    {
	      con_unreachable_host_remove (host_info, port_name);
	    }
	}
      elapsed_time = time (NULL) - start_time;
      if (elapsed_time < MONITORING_INTERVAL)
	{
	  SLEEP_MILISEC (MONITORING_INTERVAL - elapsed_time, 0);
	}
    }
  return (THREAD_RET_T) 0;
}

void
hm_force_close_connection (T_CON_HANDLE * con_handle)
{
  con_handle->alter_hosts->cur_id = 0;
  CLOSE_SOCKET (con_handle->sock_fd);
  con_handle->sock_fd = INVALID_SOCKET;
  con_handle->con_status = CCI_CON_STATUS_OUT_TRAN;
  con_handle->force_failback = 0;
}
