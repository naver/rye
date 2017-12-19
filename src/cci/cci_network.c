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
 * cci_network.c -
 */

#ident "$Id$"

/************************************************************************
 * IMPORTED SYSTEM HEADER FILES						*
 ************************************************************************/
#include <string.h>
#include <stdlib.h>
#include <fcntl.h>
#include <errno.h>
#include <stdarg.h>

#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/time.h>
#include <poll.h>
#include <pthread.h>
#include <sys/epoll.h>

/************************************************************************
 * OTHER IMPORTED HEADER FILES						*
 ************************************************************************/

#include "cci_common.h"
#include "cas_cci_internal.h"
#include "cci_log.h"
#include "cci_network.h"
#include "cas_protocol.h"
#include "cci_query_execute.h"
#include "cci_util.h"
#include "cci_net_buf.h"

/************************************************************************
 * PRIVATE DEFINITIONS							*
 ************************************************************************/

#define WRITE_TO_SOCKET(SOCKFD, MSG, SIZE)	\
	send(SOCKFD, MSG, SIZE, 0)
#define READ_FROM_SOCKET(SOCKFD, MSG, SIZE)	\
	recv(SOCKFD, MSG, SIZE, 0)

#define SOCKET_TIMEOUT 5000	/* msec */

#define SHARD_MGMT_MSG_TPYE_SIZE	1
#define SHARD_MGMT_MSG_LONG_SIZE	8
#define SHARD_MGMT_MSG_INT_SIZE		4

#define SHARD_MGMT_RES_LONG_SIZE	8
#define SHARD_MGMT_RES_INT_SIZE		4
#define SHARD_MGMT_RES_SHORT_SIZE	2

/************************************************************************
 * PRIVATE TYPE DEFINITIONS						*
 ************************************************************************/

typedef struct
{
  int size[BROKER_RESPONSE_MAX_ADDITIONAL_MSG];
  char *msg[BROKER_RESPONSE_MAX_ADDITIONAL_MSG];
} T_BROKER_RESPONSE_ADDITIONAL_MSG;

typedef struct
{
  pthread_mutex_t mutex;
  int epoll_fd;
  int count;
} T_ASYNC_LAUNCH_EPOLL_INFO;

T_ASYNC_LAUNCH_EPOLL_INFO async_Launch_epoll_info =
  { PTHREAD_MUTEX_INITIALIZER, -1, 0 };

typedef struct
{
  int sock_fd;
  char userdata[LAUNCH_RESULT_USERDATA_SIZE];
} T_ASYNC_LAUNCH_RESULT;

/************************************************************************
 * PRIVATE FUNCTION PROTOTYPES						*
 ************************************************************************/

static int connect_srv (const T_HOST_INFO * host_info, bool do_retry,
			SOCKET * ret_sock, int login_timeout);
static void pack_db_connect_msg (T_NET_BUF * net_buf, const char *dbname,
				 const char *dbuser, const char *dbpasswd,
				 const char *url, const char *db_session_id);
static int db_connect_info_decode (T_NET_RES * net_res,
				   T_CON_HANDLE * con_handle);
static int net_recv_stream (SOCKET sock_fd, const T_CON_HANDLE * con_handle,
			    char *buf, int size, int timeout,
			    bool force_wait);
static int net_send_stream (SOCKET sock_fd, const char *buf, int size);
static void init_msg_header (MSG_HEADER * header);
static int net_send_msg_header (SOCKET sock_fd, MSG_HEADER * header);
static int net_recv_msg_header (SOCKET sock_fd,
				const T_CON_HANDLE * con_handle,
				MSG_HEADER * header, int timeout);
static bool net_peer_socket_alive (const T_CON_HANDLE * con_handle,
				   int timeout_msec);
static int net_cancel_request_internal (const T_HOST_INFO * host_info,
					char *msg, int msglen);
static T_BROKER_REQUEST_MSG *make_cas_connect_msg (const char *port_name);
static T_BROKER_REQUEST_MSG *make_ping_check_msg (const char *port_name);
static bool net_peer_alive (const T_HOST_INFO * host_info,
			    const char *port_name, int timeout_msec);
static int recv_broker_response (int srv_sock_fd,
				 const T_CON_HANDLE * con_handle,
				 T_BROKER_RESPONSE * br_res,
				 T_BROKER_RESPONSE_ADDITIONAL_MSG * res_msg,
				 int login_timeout, bool force_wait);
static T_NET_RES *net_res_alloc (int msg_size);
static int net_mgmt_req_send_recv (SOCKET * sock_fd,
				   T_BROKER_RESPONSE * br_res,
				   const T_HOST_INFO * host, bool do_retry,
				   int timeout_msec,
				   const T_BROKER_REQUEST_MSG * req_msg,
				   T_BROKER_RESPONSE_ADDITIONAL_MSG * res_msg,
				   const T_CON_HANDLE * con_handle,
				   bool force_wait);
static int net_mgmt_req_send (SOCKET * sock_fd, const T_HOST_INFO * host,
			      bool do_retry, int timeout_msec,
			      const T_BROKER_REQUEST_MSG * req_msg);
static int net_mgmt_req_recv (SOCKET sock_fd, T_BROKER_RESPONSE * br_res,
			      int timeout_msec,
			      T_BROKER_RESPONSE_ADDITIONAL_MSG * res_msg,
			      const T_CON_HANDLE * con_handle,
			      bool force_wait);

static int net_mgmt_admin_req (SOCKET * sock_fd, const T_HOST_INFO * host,
			       int timeout_msec,
			       T_BROKER_REQUEST_MSG * req_msg,
			       T_BROKER_RESPONSE_ADDITIONAL_MSG * res_msg,
			       const T_CON_HANDLE * con_handle,
			       bool force_wait, bool aync_recv);

static T_BROKER_REQUEST_MSG *make_mgmt_request_msg (char opcode, ...);

static char *realloc_mgmt_arg_buf (char *ptr, int *alloced_size,
				   int msg_size);
static int copy_mgmt_arg_type (char *ptr, int pos, char arg_type);
static int copy_mgmt_arg_int (char *ptr, int pos, int value);
static int copy_mgmt_arg_string (char *ptr, int pos, const char *value);
static int copy_mgmt_arg_int64 (char *ptr, int pos, int64_t value);
static int unpack_shard_info_hdr (int msg_size, const char *msg_ptr,
				  int64_t * created_at);
static int unpack_shard_node_info (int msg_size, const char *msg_ptr,
				   CCI_SHARD_NODE_INFO ** ret_node_info);
static int unpack_shard_groupid_info (int msg_size, const char *msg_ptr,
				      CCI_SHARD_GROUPID_INFO **
				      ret_groupid_info);
static void net_additional_msg_clear (T_BROKER_RESPONSE_ADDITIONAL_MSG *
				      res_msg);
static void net_additional_msg_free (T_BROKER_RESPONSE_ADDITIONAL_MSG *
				     res_msg);

static int async_launch_epoll_add (T_ASYNC_LAUNCH_RESULT * async_launch_res);
static int async_launch_epoll_del (T_ASYNC_LAUNCH_RESULT * async_launch_res);
static int set_launch_result (T_CCI_LAUNCH_RESULT * launch_result,
			      T_BROKER_RESPONSE_ADDITIONAL_MSG * res_msg);
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
net_connect_srv (T_CON_HANDLE * con_handle, int host_id, int login_timeout)
{
  SOCKET srv_sock_fd = -1;
  int err_code;
  T_BROKER_RESPONSE br_res;
  T_NET_BUF net_buf;
  T_HOST_INFO *host_info;
  T_BROKER_REQUEST_MSG *cas_connect_msg = NULL;

  assert (con_handle->con_status == CCI_CON_STATUS_OUT_TRAN);

  if (IS_CON_TYPE_GLOBAL (con_handle))
    {
      goto end;
    }

  cas_connect_msg = make_cas_connect_msg (con_handle->port_name);
  if (cas_connect_msg == NULL)
    {
      return CCI_ER_NO_MORE_MEMORY;
    }

  assert (host_id >= 0);

  host_info = &con_handle->alter_hosts->host_info[host_id];

  err_code = connect_srv (host_info, con_handle->is_retry,
			  &srv_sock_fd, login_timeout);
  if (err_code < 0)
    {
      goto connect_srv_error;
    }

  if (net_send_stream (srv_sock_fd, cas_connect_msg->msg_buffer,
		       BRREQ_NET_MSG_SIZE (cas_connect_msg)) < 0)
    {
      err_code = CCI_ER_COMMUNICATION;
      goto connect_srv_error;
    }

  brreq_msg_free (cas_connect_msg);
  cas_connect_msg = NULL;

  err_code = recv_broker_response (srv_sock_fd, con_handle,
				   &br_res, NULL, login_timeout, false);
  if (err_code < 0)
    {
      goto connect_srv_error;
    }

  err_code = br_res.result_code;
  if (err_code < 0)
    {
      goto connect_srv_error;
    }
  else if (err_code > 0)
    {
      assert (0);
      err_code = CCI_ER_COMMUNICATION;
      goto connect_srv_error;
    }
  else
    {
      T_NET_RES *net_res;

      net_buf_init (&net_buf);
      pack_db_connect_msg (&net_buf, con_handle->db_name,
			   con_handle->db_user, con_handle->db_passwd,
			   con_handle->url_for_logging,
			   CON_SESSION_ID (con_handle));

      if (net_buf.err_code < 0)
	{
	  err_code = net_buf.err_code;
	  net_buf_clear (&net_buf);
	  goto connect_srv_error;
	}

      con_handle->sock_fd = srv_sock_fd;

      err_code = net_send_msg (con_handle, net_buf.data, net_buf.data_size);
      if (err_code < 0)
	{
	  net_buf_clear (&net_buf);
	  goto connect_srv_error;
	}

      net_buf_clear (&net_buf);

      err_code = net_recv_msg_timeout (con_handle, &net_res, login_timeout);
      if (err_code < 0)
	{
	  goto connect_srv_error;
	}

      err_code = db_connect_info_decode (net_res, con_handle);
      if (err_code < 0)
	{
	  FREE_MEM (net_res);
	  goto connect_srv_error;
	}

      FREE_MEM (net_res);
    }

end:
  con_handle->alter_hosts->cur_id = host_id;

  if (con_handle->alter_hosts->count > 0)
    {
      con_handle->is_retry = 0;
    }
  else
    {
      con_handle->is_retry = 1;
    }

  return CCI_ER_NO_ERROR;

connect_srv_error:
  if (srv_sock_fd != INVALID_SOCKET)
    {
      CLOSE_SOCKET (srv_sock_fd);
    }
  con_handle->sock_fd = INVALID_SOCKET;
  con_handle->con_status = CCI_CON_STATUS_OUT_TRAN;
  brreq_msg_free (cas_connect_msg);
  return err_code;
}

static int
db_connect_info_decode (T_NET_RES * net_res, T_CON_HANDLE * con_handle)
{
  int sessid_size;
  char *sessid_ptr;
  int prev_server_start_time;
  T_CAS_CONNECT_INFO *cas_connect_info = &con_handle->cas_connect_info;

  prev_server_start_time = cas_connect_info->server_start_time;

  if (net_res_to_short (&cas_connect_info->svr_version.major, net_res) < 0 ||
      net_res_to_short (&cas_connect_info->svr_version.minor, net_res) < 0 ||
      net_res_to_short (&cas_connect_info->svr_version.patch, net_res) < 0 ||
      net_res_to_short (&cas_connect_info->svr_version.build, net_res) < 0 ||
      net_res_to_int (&cas_connect_info->cas_id, net_res) < 0 ||
      net_res_to_int (&cas_connect_info->cas_pid, net_res) < 0 ||
      net_res_to_str (&sessid_ptr, &sessid_size, net_res) < 0 ||
      net_res_to_byte (&cas_connect_info->dbms, net_res) < 0 ||
      net_res_to_byte (&cas_connect_info->holdable_result, net_res) < 0 ||
      net_res_to_byte (&cas_connect_info->statement_pooling, net_res) < 0 ||
      net_res_to_byte (&cas_connect_info->cci_default_autocommit,
		       net_res) < 0 ||
      net_res_to_int (&cas_connect_info->server_start_time, net_res) < 0)
    {
      return CCI_ER_COMMUNICATION;
    }

  if (con_handle->con_property.error_on_server_restart &&
      prev_server_start_time > 0 &&
      cas_connect_info->server_start_time != prev_server_start_time)
    {
      cas_connect_info->server_start_time = 0;
      return CCI_ER_SERVER_RESTARTED;
    }

  sessid_size = MIN (MAX (sessid_size, 0), DRIVER_SESSION_SIZE);
  memcpy (cas_connect_info->session_id, sessid_ptr, sessid_size);
  memset (cas_connect_info->session_id + sessid_size, 0,
	  DRIVER_SESSION_SIZE - sessid_size);

  return CCI_ER_NO_ERROR;;
}

static void
pack_db_connect_msg (T_NET_BUF * net_buf, const char *dbname,
		     const char *dbuser, const char *dbpasswd,
		     const char *url, const char *db_session_id)
{
  char client_ver[SRV_CON_VER_STR_MAX_SIZE];

  strncpy (client_ver, MAKE_STR (BUILD_NUMBER), SRV_CON_VER_STR_MAX_SIZE);

  net_buf_init (net_buf);

  net_buf_cp_byte (net_buf, 0);	/* function code. not used in db_connect */

  ADD_ARG_BYTES (net_buf, dbname, dbname == NULL ? 0 : strlen (dbname) + 1);

  ADD_ARG_BYTES (net_buf, dbuser, dbuser == NULL ? 0 : strlen (dbuser) + 1);

  ADD_ARG_BYTES (net_buf, dbpasswd,
		 dbpasswd == NULL ? 0 : strlen (dbpasswd) + 1);

  ADD_ARG_BYTES (net_buf, url, url == NULL ? 0 : strlen (url) + 1);

  ADD_ARG_BYTES (net_buf, client_ver, strlen (client_ver) + 1);

  ADD_ARG_BYTES (net_buf, db_session_id,
		 db_session_id == NULL ? 0 : DRIVER_SESSION_SIZE);
}

static int
net_cancel_request_internal (const T_HOST_INFO * host_info,
			     char *msg, int msglen)
{
  SOCKET srv_sock_fd;
  int err_code;
  T_BROKER_RESPONSE br_res;

  if (connect_srv (host_info, false, &srv_sock_fd, 0) < 0)
    {
      return CCI_ER_CONNECT;
    }

  if (net_send_stream (srv_sock_fd, msg, msglen) < 0)
    {
      err_code = CCI_ER_COMMUNICATION;
      goto cancel_error;
    }

  if (recv_broker_response (srv_sock_fd, NULL, &br_res, NULL, 0, false) < 0)
    {
      err_code = CCI_ER_COMMUNICATION;
      goto cancel_error;
    }

  err_code = br_res.result_code;
  if (err_code < 0)
    goto cancel_error;

  CLOSE_SOCKET (srv_sock_fd);
  return CCI_ER_NO_ERROR;

cancel_error:
  CLOSE_SOCKET (srv_sock_fd);
  return err_code;
}

int
net_cancel_request (const T_CON_HANDLE * con_handle)
{
  T_BROKER_REQUEST_MSG *cancel_msg;
  char *ptr;
  int error;
  int host_id;
  T_HOST_INFO *host_info;
  int opcode_msg_size;

  opcode_msg_size =
    brreq_msg_normal_broker_opcode_msg_size (con_handle->port_name,
					     sizeof (int) * 2);

  cancel_msg = brreq_msg_alloc (opcode_msg_size);
  if (cancel_msg == NULL)
    {
      return CCI_ER_NO_MORE_MEMORY;
    }

  ptr = brreq_msg_pack (cancel_msg, CAS_CLIENT_CCI,
			BRREQ_OP_CODE_QUERY_CANCEL, opcode_msg_size);

  ptr = brreq_msg_pack_port_name (ptr, con_handle->port_name);
  ptr = br_msg_pack_int (ptr, CON_CAS_ID (con_handle));
  ptr = br_msg_pack_int (ptr, CON_CAS_PID (con_handle));

  host_id = con_handle->alter_hosts->cur_id;
  host_info = &con_handle->alter_hosts->host_info[host_id];

  error = net_cancel_request_internal (host_info, cancel_msg->msg_buffer,
				       BRREQ_NET_MSG_SIZE (cancel_msg));

  brreq_msg_free (cancel_msg);

  return error;
}

int
net_check_cas_request (T_CON_HANDLE * con_handle)
{
  int err_code;
  char msg = CAS_FC_CHECK_CAS;

  API_SLOG (con_handle, NULL, "net_check_cas_request");
  err_code = net_send_msg (con_handle, &msg, 1);
  if (err_code < 0)
    {
      API_ELOG (con_handle, NULL, err_code, "net_check_cas_request");
      return err_code;
    }

  err_code = net_recv_msg (con_handle, NULL);

  API_ELOG (con_handle, NULL, err_code, "net_check_cas_request");
  return err_code;
}

int
net_send_msg (const T_CON_HANDLE * con_handle, char *msg, int size)
{
  MSG_HEADER send_msg_header;
  int err;
  struct timeval ts, te;

  init_msg_header (&send_msg_header);

  *(send_msg_header.msg_body_size_ptr) = size;
  memcpy (send_msg_header.info_ptr, CON_CAS_STATUS_INFO (con_handle),
	  CAS_STATUS_INFO_SIZE);

  /* send msg header */
  if (con_handle->con_property.log_trace_network)
    {
      gettimeofday (&ts, NULL);
    }
  err = net_send_msg_header (con_handle->sock_fd, &send_msg_header);
  if (con_handle->con_property.log_trace_network)
    {
      long elapsed;

      gettimeofday (&te, NULL);
      elapsed = ut_timeval_diff_msec (&ts, &te);
      CCI_LOGF_DEBUG (con_handle->con_logger, "[NET][W][H][S:%d][E:%d][T:%d]",
		      MSG_HEADER_SIZE, err, elapsed);
    }
  if (err < 0)
    {
      return CCI_ER_COMMUNICATION;
    }

  if (con_handle->con_property.log_trace_network)
    {
      gettimeofday (&ts, NULL);
    }
  err = net_send_stream (con_handle->sock_fd, msg, size);
  if (con_handle->con_property.log_trace_network)
    {
      long elapsed;

      gettimeofday (&te, NULL);
      elapsed = ut_timeval_diff_msec (&ts, &te);
      CCI_LOGF_DEBUG (con_handle->con_logger, "[NET][W][B][S:%d][E:%d][T:%d]",
		      size, err, elapsed);
    }
  if (err < 0)
    {
      return CCI_ER_COMMUNICATION;
    }

  return 0;
}

static void
unpack_con_handle_status_info (T_CON_HANDLE * con_handle)
{
  short nodeid;
  const char *status_info_ptr = con_handle->cas_connect_info.status_info;

  if (status_info_ptr[CAS_STATUS_INFO_IDX_STATUS] == CAS_STATUS_INACTIVE)
    {
      con_handle->con_status = CCI_CON_STATUS_OUT_TRAN;
    }
  else
    {
      con_handle->con_status = CCI_CON_STATUS_IN_TRAN;
    }

  memcpy (&nodeid, status_info_ptr + CAS_STATUS_INFO_IDX_SERVER_NODEID, 2);
  con_handle->server_shard_nodeid = htons (nodeid);
}

int
net_recv_msg_timeout (T_CON_HANDLE * con_handle, T_NET_RES ** net_res,
		      int timeout)
{
  MSG_HEADER recv_msg_header;
  int err_code;
  struct timeval ts, te;
  T_NET_RES *tmp_net_res = NULL;

  init_msg_header (&recv_msg_header);

  if (net_res)
    {
      *net_res = NULL;
    }

  if (con_handle->con_property.log_trace_network)
    {
      gettimeofday (&ts, NULL);
    }
  err_code = net_recv_msg_header (con_handle->sock_fd, con_handle,
				  &recv_msg_header, timeout);
  if (con_handle->con_property.log_trace_network)
    {
      long elapsed;

      gettimeofday (&te, NULL);
      elapsed = ut_timeval_diff_msec (&ts, &te);
      CCI_LOGF_DEBUG (con_handle->con_logger, "[NET][R][H][S:%d][E:%d][T:%d]",
		      MSG_HEADER_SIZE, err_code, elapsed);
    }
  if (err_code < 0)
    {
#if 0
      if (err_code == CCI_ER_QUERY_TIMEOUT)
	{
	  /* send cancel message */
	  net_cancel_request (con_handle);

	  if (con_handle->disconnect_on_query_timeout == false)
	    {
	      err_code = net_recv_msg_header (con_handle->sock_fd,
					      broker_port,
					      &recv_msg_header, 0);
	    }
	}
#endif

      goto error_return;
    }

  memcpy (con_handle->cas_connect_info.status_info, recv_msg_header.info_ptr,
	  CAS_STATUS_INFO_SIZE);

  unpack_con_handle_status_info (con_handle);

  if (*(recv_msg_header.msg_body_size_ptr) > 0)
    {
      char request_result;

      tmp_net_res = net_res_alloc (*(recv_msg_header.msg_body_size_ptr));
      if (tmp_net_res == NULL)
	{
	  err_code = CCI_ER_NO_MORE_MEMORY;
	  goto error_return;
	}

      if (con_handle->con_property.log_trace_network)
	{
	  gettimeofday (&ts, NULL);
	}

      err_code = net_recv_stream (con_handle->sock_fd, con_handle,
				  tmp_net_res->buffer,
				  tmp_net_res->result_size, timeout, false);

      if (con_handle->con_property.log_trace_network)
	{
	  long elapsed;

	  gettimeofday (&te, NULL);
	  elapsed = ut_timeval_diff_msec (&ts, &te);
	  CCI_LOGF_DEBUG (con_handle->con_logger,
			  "[NET][R][B][S:%d][E:%d][T:%d]",
			  *(recv_msg_header.msg_body_size_ptr), err_code,
			  elapsed);
	}

      if (err_code < 0)
	{
	  goto error_return;
	}

      err_code = net_res_to_byte (&request_result, tmp_net_res);
      if (err_code < 0)
	{
	  goto error_return;
	}

      if (request_result == ERROR_RESPONSE)
	{
	  int srv_err_indicator;
	  int srv_err_code = 0;
	  int srv_err_msg_size;
	  char *tmp_p;

	  err_code = net_res_to_int (&srv_err_indicator, tmp_net_res);
	  if (err_code < 0)
	    {
	      goto error_return;
	    }
	  err_code = net_res_to_int (&srv_err_code, tmp_net_res);
	  if (err_code < 0)
	    {
	      goto error_return;
	    }
	  err_code = net_res_to_str (&tmp_p, &srv_err_msg_size, tmp_net_res);
	  if (err_code < 0)
	    {
	      goto error_return;
	    }

	  assert (srv_err_code < 0);

	  memcpy (con_handle->err_buf.err_msg, tmp_p,
		  MIN ((int) sizeof (con_handle->err_buf.err_msg),
		       srv_err_msg_size));
	  con_handle->err_buf.err_msg[sizeof (con_handle->err_buf.err_msg) -
				      1] = '\0';
	  con_handle->err_buf.err_code = srv_err_code;

	  if (srv_err_indicator == DBMS_ERROR_INDICATOR)
	    {
	      srv_err_code = CCI_ER_DBMS;
	    }

	  FREE_MEM (tmp_net_res);

	  return srv_err_code;
	}
    }
  else
    {
      err_code = CCI_ER_COMMUNICATION;
      goto error_return;
    }

  if (net_res)
    {
      *net_res = tmp_net_res;
    }
  else
    {
      FREE_MEM (tmp_net_res);
    }

  return CCI_ER_NO_ERROR;

error_return:
  FREE_MEM (tmp_net_res);
  CLOSE_SOCKET (con_handle->sock_fd);
  con_handle->sock_fd = INVALID_SOCKET;

  return err_code;
}

int
net_recv_msg (T_CON_HANDLE * con_handle, T_NET_RES ** net_res)
{
  return net_recv_msg_timeout (con_handle, net_res, 0);
}

static T_NET_RES *
net_res_alloc (int msg_size)
{
  T_NET_RES *res;

  res = (T_NET_RES *) MALLOC (sizeof (T_NET_RES) + msg_size);
  if (res == NULL)
    {
      return NULL;
    }

  res->cur_p = res->buffer;
  res->result_size = msg_size;

  return res;
}

static bool
net_peer_alive (const T_HOST_INFO * host_info, const char *port_name,
		int timeout_msec)
{
  SOCKET sock_fd;
  int ret;
  T_BROKER_RESPONSE br_res;
  T_BROKER_REQUEST_MSG *ping_check_msg;

  ping_check_msg = make_ping_check_msg (port_name);
  if (ping_check_msg == NULL)
    {
      return false;
    }

  if (connect_srv (host_info, false, &sock_fd, timeout_msec) !=
      CCI_ER_NO_ERROR)
    {
      CLOSE_SOCKET (sock_fd);
      brreq_msg_free (ping_check_msg);
      return false;
    }

send_again:
  ret = WRITE_TO_SOCKET (sock_fd,
			 ping_check_msg->msg_buffer,
			 BRREQ_NET_MSG_SIZE (ping_check_msg));
  if (ret < 0)
    {
      if (errno == EAGAIN)
	{
	  SLEEP_MILISEC (0, 1);
	  goto send_again;
	}
      else
	{
	  brreq_msg_free (ping_check_msg);
	  CLOSE_SOCKET (sock_fd);
	  return false;
	}
    }

  brreq_msg_free (ping_check_msg);

recv_again:
  ret = recv_broker_response (sock_fd, NULL, &br_res, NULL, 0, false);
  if (ret < 0)
    {
      if (errno == EAGAIN)
	{
	  SLEEP_MILISEC (0, 1);
	  goto recv_again;
	}
      else
	{
	  CLOSE_SOCKET (sock_fd);
	  return false;
	}
    }

  CLOSE_SOCKET (sock_fd);

  return true;
}

bool
net_check_broker_alive (const T_HOST_INFO * host, const char *port_name,
			int timeout_msec)
{
  SOCKET sock_fd = INVALID_SOCKET;
  MSG_HEADER msg_header;
  char url[SRV_CON_URL_SIZE];
  bool result = false;
  const char *dbname = HEALTH_CHECK_DUMMY_DB;
  T_BROKER_RESPONSE br_res;
  T_NET_BUF net_buf;
  T_CON_HANDLE con_handle;
  T_BROKER_REQUEST_MSG *cas_connect_msg = NULL;
  char ip_addr_str[64];

  memset (&con_handle, 0, sizeof (T_CON_HANDLE));

  cas_connect_msg = make_cas_connect_msg (port_name);
  if (cas_connect_msg == NULL)
    {
      return false;
    }

  init_msg_header (&msg_header);

  if (connect_srv (host, false, &sock_fd, timeout_msec) < 0)
    {
      goto finish_health_check;
    }

  if (net_send_stream (sock_fd, cas_connect_msg->msg_buffer,
		       BRREQ_NET_MSG_SIZE (cas_connect_msg)) < 0)
    {
      goto finish_health_check;
    }

  if (recv_broker_response (sock_fd, 0, &br_res, NULL, timeout_msec, false) <
      0)
    {
      goto finish_health_check;
    }

  if (br_res.result_code < 0)
    {
      goto finish_health_check;
    }

  net_buf_init (&net_buf);

  ut_make_url (url, sizeof (url), ut_host_info_to_str (ip_addr_str, host),
	       dbname, NULL, NULL, port_name, NULL);

  pack_db_connect_msg (&net_buf, dbname, NULL, NULL, url, NULL);
  if (net_buf.err_code < 0)
    {
      net_buf_clear (&net_buf);
      goto finish_health_check;
    }

  con_handle.sock_fd = sock_fd;

  if (net_send_msg (&con_handle, net_buf.data, net_buf.data_size) < 0)
    {
      sock_fd = con_handle.sock_fd;
      net_buf_clear (&net_buf);
      goto finish_health_check;
    }

  net_buf_clear (&net_buf);

  if (net_recv_msg_timeout (&con_handle, NULL, timeout_msec) < 0)
    {
      sock_fd = con_handle.sock_fd;
      goto finish_health_check;
    }
  result = true;

finish_health_check:
  if (sock_fd != INVALID_SOCKET)
    {
      CLOSE_SOCKET (sock_fd);
    }
  brreq_msg_free (cas_connect_msg);
  return result;
}

int
net_res_to_byte (char *value, T_NET_RES * net_res)
{
  if (net_res->result_size < NET_SIZE_BYTE)
    {
      return CCI_ER_COMMUNICATION;
    }
  *value = *(net_res->cur_p);

  net_res->cur_p += NET_SIZE_BYTE;
  net_res->result_size -= NET_SIZE_BYTE;

  return CCI_ER_NO_ERROR;
}

int
net_res_to_int (int *value, T_NET_RES * net_res)
{
  int tmp_int;

  if (net_res->result_size < NET_SIZE_INT)
    {
      return CCI_ER_COMMUNICATION;
    }

  memcpy (&tmp_int, net_res->cur_p, NET_SIZE_INT);
  *value = ntohl (tmp_int);

  net_res->cur_p += NET_SIZE_INT;
  net_res->result_size -= NET_SIZE_INT;

  return CCI_ER_NO_ERROR;
}

int
net_res_to_short (short *value, T_NET_RES * net_res)
{
  short tmp_value;

  if (net_res->result_size < NET_SIZE_SHORT)
    {
      return CCI_ER_COMMUNICATION;
    }

  memcpy (&tmp_value, net_res->cur_p, NET_SIZE_SHORT);
  *value = ntohs (tmp_value);

  net_res->cur_p += NET_SIZE_SHORT;
  net_res->result_size -= NET_SIZE_SHORT;

  return CCI_ER_NO_ERROR;
}

char *
net_res_cur_ptr (T_NET_RES * net_res)
{
  return net_res->cur_p;
}

int
net_res_to_str (char **str_ptr, int *str_size, T_NET_RES * net_res)
{
  int tmp_size;

  if (net_res_to_int (&tmp_size, net_res) < 0 ||
      net_res->result_size < tmp_size)
    {
      return CCI_ER_COMMUNICATION;
    }

  if (tmp_size <= 0)
    {
      *str_ptr = NULL;
      *str_size = 0;
    }
  else
    {
      *str_ptr = net_res->cur_p;
      *str_size = tmp_size;

      net_res->cur_p += tmp_size;
      net_res->result_size -= tmp_size;
    }

  return CCI_ER_NO_ERROR;
}

int
net_shard_get_info (const T_HOST_INFO * host, const char *dbname,
		    int64_t node_version, int64_t gid_version,
		    int64_t clt_created_at, int timeout_msec,
		    CCI_SHARD_NODE_INFO ** node_info,
		    CCI_SHARD_GROUPID_INFO ** groupid_info,
		    int64_t * created_at)
{
  T_BROKER_REQUEST_MSG *shard_info_msg = NULL;
  int sock_fd = INVALID_SOCKET;
  T_BROKER_RESPONSE br_res;
  T_BROKER_RESPONSE_ADDITIONAL_MSG res_msg;
  int err_code;
  int i;

  *node_info = NULL;
  *groupid_info = NULL;

  memset (&res_msg, 0, sizeof (T_BROKER_RESPONSE_ADDITIONAL_MSG));

  shard_info_msg = make_mgmt_request_msg (BRREQ_OP_CODE_GET_SHARD_INFO,
					  MGMT_REQ_ARG_STR, dbname,
					  MGMT_REQ_ARG_INT64, node_version,
					  MGMT_REQ_ARG_INT64, gid_version,
					  MGMT_REQ_ARG_INT64, clt_created_at,
					  MGMT_REQ_ARG_END);
  if (shard_info_msg == NULL)
    {
      return CCI_ER_NO_MORE_MEMORY;
    }

  err_code = net_mgmt_req_send_recv (&sock_fd, &br_res, host, false,
				     timeout_msec, shard_info_msg, &res_msg,
				     NULL, false);
  if (err_code < 0)
    {
      goto shard_info_request_end;
    }

  err_code = unpack_shard_info_hdr (br_res.additional_message_size[0],
				    res_msg.msg[0], created_at);
  if (err_code < 0)
    {
      goto shard_info_request_end;
    }

  err_code = unpack_shard_node_info (br_res.additional_message_size[1],
				     res_msg.msg[1], node_info);
  if (err_code < 0)
    {
      goto shard_info_request_end;
    }

  err_code = unpack_shard_groupid_info (br_res.additional_message_size[2],
					res_msg.msg[2], groupid_info);
  if (err_code < 0)
    {
      goto shard_info_request_end;
    }

shard_info_request_end:
  for (i = 0; i < BROKER_RESPONSE_MAX_ADDITIONAL_MSG; i++)
    {
      FREE_MEM (res_msg.msg[i]);
    }

  if (err_code < 0)
    {
      FREE_MEM (*node_info);
      FREE_MEM (*groupid_info);
    }

  if (sock_fd != INVALID_SOCKET)
    {
      CLOSE_SOCKET (sock_fd);
    }

  brreq_msg_free (shard_info_msg);
  return err_code;
}

int
net_shard_init (int groupid_count, int init_num_nodes,
		const char **init_nodes, const T_CON_HANDLE * con_handle)
{
  T_BROKER_REQUEST_MSG *req_msg = NULL;

  req_msg = make_mgmt_request_msg (BRREQ_OP_CODE_INIT,
				   MGMT_REQ_ARG_STR, con_handle->db_passwd,
				   MGMT_REQ_ARG_STR, con_handle->db_name,
				   MGMT_REQ_ARG_INT, groupid_count,
				   MGMT_REQ_ARG_STR_ARRAY,
				   init_num_nodes, init_nodes,
				   MGMT_REQ_ARG_END);

  return (net_mgmt_admin_req (NULL, &con_handle->alter_hosts->host_info[0],
			      con_handle->con_property.query_timeout, req_msg,
			      NULL, con_handle, false, false));
}

int
net_shard_node_req (char opcode, const char *node_arg,
		    const T_CON_HANDLE * con_handle)
{
  T_BROKER_REQUEST_MSG *req_msg = NULL;

  req_msg = make_mgmt_request_msg (opcode,
				   MGMT_REQ_ARG_STR, con_handle->db_name,
				   MGMT_REQ_ARG_STR, con_handle->db_passwd,
				   MGMT_REQ_ARG_INT, 0,
				   MGMT_REQ_ARG_STR, node_arg,
				   MGMT_REQ_ARG_END);

  return (net_mgmt_admin_req (NULL, &con_handle->alter_hosts->host_info[0],
			      con_handle->con_property.query_timeout, req_msg,
			      NULL, con_handle, false, false));
}

int
net_shard_migration_req (char opcode, SOCKET * sock_fd, int groupid,
			 int dest_nodeid, int num_shard_keys,
			 const T_CON_HANDLE * con_handle)
{
  T_BROKER_REQUEST_MSG *req_msg = NULL;
  int timeout_msec = con_handle->con_property.query_timeout;

  req_msg = make_mgmt_request_msg (opcode,
				   MGMT_REQ_ARG_STR, con_handle->db_name,
				   MGMT_REQ_ARG_INT, groupid,
				   MGMT_REQ_ARG_INT, dest_nodeid,
				   MGMT_REQ_ARG_INT, num_shard_keys,
				   MGMT_REQ_ARG_INT, timeout_msec / 1000,
				   MGMT_REQ_ARG_END);

  return (net_mgmt_admin_req (sock_fd, &con_handle->alter_hosts->host_info[0],
			      timeout_msec, req_msg, NULL, con_handle,
			      false, false));
}

int
net_shard_ddl_gc_req (char opcode, SOCKET * sock_fd,
		      const T_CON_HANDLE * con_handle)
{
  T_BROKER_REQUEST_MSG *req_msg = NULL;
  int timeout_msec = con_handle->con_property.query_timeout;

  req_msg = make_mgmt_request_msg (opcode,
				   MGMT_REQ_ARG_STR, con_handle->db_name,
				   MGMT_REQ_ARG_INT, timeout_msec / 1000,
				   MGMT_REQ_ARG_END);

  return (net_mgmt_admin_req (sock_fd, &con_handle->alter_hosts->host_info[0],
			      timeout_msec, req_msg, NULL, con_handle,
			      false, false));
}

int
net_mgmt_shard_mgmt_info_req (const T_HOST_INFO * host,
			      const char *local_dbname,
			      const char *global_dbname, int nodeid,
			      int port,
			      int64_t nodeid_ver, int64_t groupid_ver,
			      char *server_name_buf, int server_name_buf_size,
			      int *server_mode, int timeout_msec)
{
  T_BROKER_REQUEST_MSG *req_msg = NULL;
  T_BROKER_RESPONSE_ADDITIONAL_MSG res_msg;
  int err_code;

  net_additional_msg_clear (&res_msg);

  req_msg = make_mgmt_request_msg (BRREQ_OP_CODE_SYNC_SHARD_MGMT_INFO,
				   MGMT_REQ_ARG_STR, local_dbname,
				   MGMT_REQ_ARG_STR, global_dbname,
				   MGMT_REQ_ARG_INT, nodeid,
				   MGMT_REQ_ARG_INT, port,
				   MGMT_REQ_ARG_INT64, nodeid_ver,
				   MGMT_REQ_ARG_INT64, groupid_ver,
				   MGMT_REQ_ARG_END);

  err_code = net_mgmt_admin_req (NULL, host, timeout_msec, req_msg, &res_msg,
				 NULL, false, false);
  if (err_code < 0)
    {
      return err_code;
    }

  if (server_name_buf_size > 0 && res_msg.size[0] > 0)
    {
      int size = MIN (res_msg.size[0], server_name_buf_size - 1);
      memcpy (server_name_buf, res_msg.msg[0], size);
      server_name_buf[size] = '\0';
    }

  if (res_msg.size[1] == sizeof (int))
    {
      char *ptr = res_msg.msg[1];

      if (server_mode != NULL)
	{
	  memcpy (server_mode, ptr, sizeof (int));
	  *server_mode = ntohl (*server_mode);
	}
    }

  net_additional_msg_free (&res_msg);

  return err_code;
}

int
net_mgmt_launch_process_req (T_CCI_LAUNCH_RESULT * launch_result,
			     const T_HOST_INFO * host,
			     int launch_proc_id, bool recv_stdout,
			     bool wait_child, int argc, const char **argv,
			     int num_env, const char **envp, int timeout_msec)
{
  T_BROKER_REQUEST_MSG *req_msg = NULL;
  T_BROKER_RESPONSE_ADDITIONAL_MSG res_msg;
  int err_code;
  SOCKET sock_fd = INVALID_SOCKET;
  bool async_recv = (wait_child ? false : true);
  bool force_wait = (wait_child ? true : false);
  T_ASYNC_LAUNCH_RESULT *async_launch_res = NULL;
  int flag = MGMT_LAUNCH_FLAG_NO_FLAG;

  if (recv_stdout == false)
    {
      flag |= MGMT_LAUNCH_FLAG_NO_RESULT;
    }

  if (!wait_child)
    {
      async_launch_res = MALLOC (sizeof (T_ASYNC_LAUNCH_RESULT));
      if (async_launch_res == NULL)
	{
	  return CCI_ER_NO_MORE_MEMORY;
	}
      memset (async_launch_res, 0, sizeof (T_ASYNC_LAUNCH_RESULT));
    }

  net_additional_msg_clear (&res_msg);

  req_msg = make_mgmt_request_msg (BRREQ_OP_CODE_LAUNCH_PROCESS,
				   MGMT_REQ_ARG_INT, launch_proc_id,
				   MGMT_REQ_ARG_INT, flag,
				   MGMT_REQ_ARG_STR_ARRAY, argc, argv,
				   MGMT_REQ_ARG_STR_ARRAY, num_env, envp,
				   MGMT_REQ_ARG_END);

  err_code = net_mgmt_admin_req (&sock_fd, host, timeout_msec, req_msg,
				 &res_msg, NULL, force_wait, async_recv);

  if (err_code < 0)
    {
      FREE_MEM (async_launch_res);
    }
  else
    {
      assert (sock_fd != INVALID_SOCKET);

      if (wait_child)
	{
	  err_code = set_launch_result (launch_result, &res_msg);
	  CLOSE_SOCKET (sock_fd);
	}
      else
	{
	  async_launch_res->sock_fd = sock_fd;
	  STRNCPY (async_launch_res->userdata, launch_result->userdata,
		   LAUNCH_RESULT_USERDATA_SIZE);

	  if (async_launch_epoll_add (async_launch_res) < 0)
	    {
	      err_code = CCI_ER_ASYNC_LAUNCH_FAIL;
	    }
	}
    }

  net_additional_msg_free (&res_msg);

  return err_code;
}

int
net_mgmt_count_launch_process ()
{
  return async_Launch_epoll_info.count;
}

int
net_mgmt_wait_launch_process (T_CCI_LAUNCH_RESULT * launch_res,
			      int poll_timeout)
{
  int n;
  struct epoll_event event;

  if (async_Launch_epoll_info.epoll_fd < 0)
    {
      return CCI_ER_ASYNC_LAUNCH_FAIL;
    }

  while (true)
    {
      n = epoll_wait (async_Launch_epoll_info.epoll_fd, &event, 1,
		      poll_timeout);
      if (n < 0)
	{
	  if (errno == EINTR)
	    {
	      continue;
	    }
	  return CCI_ER_ASYNC_LAUNCH_FAIL;
	}
      else if (n > 0)
	{
	  int error = CCI_ER_NO_ERROR;
	  T_ASYNC_LAUNCH_RESULT *async_launch_res = NULL;

	  async_launch_res = event.data.ptr;

	  if (async_launch_res == NULL)
	    {
	      assert (0);
	    }
	  else
	    {
	      if (event.events & EPOLLERR || event.events & EPOLLHUP)
		{
		  error = CCI_ER_ASYNC_LAUNCH_FAIL;
		}
	      else if (event.events & EPOLLIN)
		{
		  T_BROKER_RESPONSE br_res;
		  T_BROKER_RESPONSE_ADDITIONAL_MSG res_msg;
		  int recv_timeout_msec = 1000;

		  net_additional_msg_clear (&res_msg);
		  error = net_mgmt_req_recv (async_launch_res->sock_fd,
					     &br_res, recv_timeout_msec,
					     &res_msg, NULL, false);
		  if (error == CCI_ER_NO_ERROR)
		    {
		      error = set_launch_result (launch_res, &res_msg);
		      STRNCPY (launch_res->userdata,
			       async_launch_res->userdata,
			       LAUNCH_RESULT_USERDATA_SIZE);

		      net_additional_msg_free (&res_msg);
		    }
		}

	      if (async_launch_epoll_del (async_launch_res) < 0)
		{
		  assert (0);
		}
	      CLOSE_SOCKET (async_launch_res->sock_fd);
	      FREE_MEM (async_launch_res);
	    }
	  return error;
	}
      else
	{
	  return CCI_ER_ASYNC_LAUNCH_FAIL;
	}
    }
}

int
net_mgmt_connect_db_server (const T_HOST_INFO * host, const char *dbname,
			    int timeout_msec)
{
  T_BROKER_REQUEST_MSG *req_msg = NULL;
  T_BROKER_RESPONSE_ADDITIONAL_MSG res_msg;
  SOCKET sock_fd = INVALID_SOCKET;

  net_additional_msg_clear (&res_msg);

  req_msg = make_mgmt_request_msg (BRREQ_OP_CODE_CONNECT_DB_SERVER,
				   MGMT_REQ_ARG_STR, dbname,
				   MGMT_REQ_ARG_END);
  if (net_mgmt_admin_req (&sock_fd, host, timeout_msec, req_msg,
			  &res_msg, NULL, false, false) < 0)
    {
      return INVALID_SOCKET;
    }

  net_additional_msg_free (&res_msg);

  assert (sock_fd != INVALID_SOCKET);
  return sock_fd;
}

/************************************************************************
 * IMPLEMENTATION OF PRIVATE FUNCTIONS	 				*
 ************************************************************************/

static void
net_additional_msg_clear (T_BROKER_RESPONSE_ADDITIONAL_MSG * res_msg)
{
  memset (res_msg, 0, sizeof (T_BROKER_RESPONSE_ADDITIONAL_MSG));
}

static void
net_additional_msg_free (T_BROKER_RESPONSE_ADDITIONAL_MSG * res_msg)
{
  int i;

  for (i = 0; i < BROKER_RESPONSE_MAX_ADDITIONAL_MSG; i++)
    {
      FREE_MEM (res_msg->msg[i]);
    }
}

static int
net_mgmt_admin_req (SOCKET * sock_fd, const T_HOST_INFO * host,
		    int timeout_msec, T_BROKER_REQUEST_MSG * req_msg,
		    T_BROKER_RESPONSE_ADDITIONAL_MSG * res_msg,
		    const T_CON_HANDLE * con_handle, bool force_wait,
		    bool async_recv)
{
  T_BROKER_RESPONSE br_res;
  SOCKET tmp_sock_fd = INVALID_SOCKET;
  int err_code;

  if (req_msg == NULL)
    {
      return CCI_ER_NO_MORE_MEMORY;
    }

  if (sock_fd != NULL)
    {
      tmp_sock_fd = *sock_fd;
    }

  err_code = net_mgmt_req_send (&tmp_sock_fd, host, false, timeout_msec,
				req_msg);
  if (err_code < 0 || async_recv)
    {
      goto mgmt_admin_req_end;
    }

  err_code = net_mgmt_req_recv (tmp_sock_fd, &br_res, timeout_msec, res_msg,
				con_handle, force_wait);

mgmt_admin_req_end:
  brreq_msg_free (req_msg);

  if (tmp_sock_fd != INVALID_SOCKET)
    {
      if (sock_fd == NULL)
	{
	  CLOSE_SOCKET (tmp_sock_fd);
	}
      else if (*sock_fd == INVALID_SOCKET)
	{
	  if (err_code < 0)
	    {
	      CLOSE_SOCKET (tmp_sock_fd);
	    }
	  else
	    {
	      *sock_fd = tmp_sock_fd;
	    }
	}
      else
	{
	  /* *sock_fd != INVALID_SOCKET
	   * do nothing. caller will close socket */
	}
    }

  return err_code;
}

static int
net_mgmt_req_send_recv (SOCKET * sock_fd,
			T_BROKER_RESPONSE * br_res,
			const T_HOST_INFO * host, bool do_retry,
			int timeout_msec,
			const T_BROKER_REQUEST_MSG * req_msg,
			T_BROKER_RESPONSE_ADDITIONAL_MSG * res_msg,
			const T_CON_HANDLE * con_handle, bool force_wait)
{
  int error;

  error = net_mgmt_req_send (sock_fd, host, do_retry, timeout_msec, req_msg);
  if (error < 0)
    {
      return error;
    }

  return net_mgmt_req_recv (*sock_fd, br_res, timeout_msec, res_msg,
			    con_handle, force_wait);
}

static int
net_mgmt_req_send (SOCKET * sock_fd, const T_HOST_INFO * host,
		   bool do_retry, int timeout_msec,
		   const T_BROKER_REQUEST_MSG * req_msg)
{
  int err_code;

  if (*sock_fd == INVALID_SOCKET)
    {
      err_code = connect_srv (host, do_retry, sock_fd, timeout_msec);
      if (err_code < 0)
	{
	  return err_code;
	}
    }

  err_code = net_send_stream (*sock_fd, req_msg->msg_buffer,
			      BRREQ_NET_MSG_SIZE (req_msg));
  return err_code;
}

static int
net_mgmt_req_recv (SOCKET sock_fd, T_BROKER_RESPONSE * br_res,
		   int timeout_msec,
		   T_BROKER_RESPONSE_ADDITIONAL_MSG * res_msg,
		   const T_CON_HANDLE * con_handle, bool force_wait)
{
  int err_code;

  err_code = recv_broker_response (sock_fd, con_handle, br_res, res_msg,
				   timeout_msec, force_wait);
  if (err_code < 0)
    {
      return err_code;
    }

  if (br_res->result_code < 0)
    {
      return br_res->result_code;
    }

  return CCI_ER_NO_ERROR;
}

static int
net_recv_stream (SOCKET sock_fd, const T_CON_HANDLE * con_handle,
		 char *buf, int size, int timeout, bool force_wait)
{
  int read_len, tot_read_len = 0;
  struct pollfd po[1] = { {0, 0, 0} };
  int polling_timeout;
  int n;

  while (tot_read_len < size)
    {
      po[0].fd = sock_fd;
      po[0].events = POLLIN;

      if (timeout <= 0 || timeout > SOCKET_TIMEOUT)
	{
	  polling_timeout = SOCKET_TIMEOUT;
	}
      else
	{
	  polling_timeout = timeout;
	}

      n = poll (po, 1, polling_timeout);
      if (n == 0)
	{
	  /* select / poll return time out */
	  if (timeout > 0)
	    {
	      timeout -= SOCKET_TIMEOUT;
	      if (timeout <= 0)
		{
		  assert (tot_read_len == 0 || size == tot_read_len);
		  return CCI_ER_QUERY_TIMEOUT;
		}
	      else
		{
		  continue;
		}
	    }

	  if (force_wait == true ||
	      net_peer_socket_alive (con_handle, SOCKET_TIMEOUT) == true)
	    {
	      continue;
	    }
	  else
	    {
	      return CCI_ER_COMMUNICATION;
	    }
	}
      else if (n < 0)
	{
	  /* select / poll return error */
	  if (errno == EINTR)
	    {
	      continue;
	    }

	  return CCI_ER_COMMUNICATION;
	}
      else if (po[0].revents & POLLERR || po[0].revents & POLLHUP)
	{
	  po[0].revents = 0;
	  return CCI_ER_COMMUNICATION;
	}

      read_len = READ_FROM_SOCKET (sock_fd, buf + tot_read_len,
				   size - tot_read_len);
      if (read_len <= 0)
	{
	  return CCI_ER_COMMUNICATION;
	}

      tot_read_len += read_len;
    }

  return CCI_ER_NO_ERROR;
}

static bool
net_peer_socket_alive (const T_CON_HANDLE * con_handle, int timeout_msec)
{
  if (con_handle == NULL)
    {
      return false;
    }
  else
    {
      int cur_id = con_handle->alter_hosts->cur_id;

      return net_peer_alive (&con_handle->alter_hosts->host_info[cur_id],
			     con_handle->port_name, timeout_msec);
    }
}

static int
net_recv_msg_header (SOCKET sock_fd, const T_CON_HANDLE * con_handle,
		     MSG_HEADER * header, int timeout)
{
  int result_code;

  result_code = net_recv_stream (sock_fd, con_handle, header->buf,
				 MSG_HEADER_SIZE, timeout, false);

  if (result_code < 0)
    {
      return result_code;
    }
  *(header->msg_body_size_ptr) = ntohl (*(header->msg_body_size_ptr));

  if (*(header->msg_body_size_ptr) < 0)
    {
      return CCI_ER_COMMUNICATION;
    }
  return CCI_ER_NO_ERROR;
}

static int
net_send_msg_header (SOCKET sock_fd, MSG_HEADER * header)
{
  *(header->msg_body_size_ptr) = htonl (*(header->msg_body_size_ptr));
  if (net_send_stream (sock_fd, header->buf, MSG_HEADER_SIZE) < 0)
    {
      return CCI_ER_COMMUNICATION;
    }

  return 0;
}

static int
net_send_stream (SOCKET sock_fd, const char *msg, int size)
{
  int write_len;
  while (size > 0)
    {
      write_len = WRITE_TO_SOCKET (sock_fd, msg, size);
      if (write_len <= 0)
	{
	  return CCI_ER_COMMUNICATION;
	}
      msg += write_len;
      size -= write_len;
    }
  return 0;
}

static void
init_msg_header (MSG_HEADER * header)
{
  header->msg_body_size_ptr = (int *) (header->buf);
  header->info_ptr = (char *) (header->buf + MSG_HEADER_MSG_SIZE);

  *(header->msg_body_size_ptr) = 0;

  cas_status_info_init (header->info_ptr);
}

static int
connect_srv (const T_HOST_INFO * host_info, bool do_retry,
	     SOCKET * ret_sock, int login_timeout)
{
  struct sockaddr_in sock_addr;
  SOCKET sock_fd;
  int sock_addr_len;
  int retry_count = 0;
  int con_retry_count;
  int ret;
  int sock_opt;
  int flags;
  struct pollfd po[1] = { {0, 0, 0} };

  con_retry_count = (do_retry) ? 10 : 0;

connect_retry:

  sock_fd = socket (AF_INET, SOCK_STREAM, 0);
  if (IS_INVALID_SOCKET (sock_fd))
    {
      return CCI_ER_CONNECT;
    }

  memset (&sock_addr, 0, sizeof (struct sockaddr_in));
  sock_addr.sin_family = AF_INET;
  sock_addr.sin_port = htons ((unsigned short) host_info->port);
  memcpy (&sock_addr.sin_addr, host_info->ip_addr, 4);
  sock_addr_len = sizeof (struct sockaddr_in);

  flags = fcntl (sock_fd, F_GETFL);
  if (fcntl (sock_fd, F_SETFL, flags | O_NONBLOCK) < 0)
    {
      assert (0);
    }

  ret = connect (sock_fd, (struct sockaddr *) &sock_addr, sock_addr_len);
  if (ret < 0)
    {
      if (errno == EINPROGRESS)
	{
	  po[0].fd = sock_fd;
	  po[0].events = POLLOUT;
	  po[0].revents = 0;

	  if (login_timeout == 0)
	    {
	      login_timeout = -1;
	    }

	  ret = poll (po, 1, login_timeout);
	  if (ret == 0)
	    {
	      CLOSE_SOCKET (sock_fd);
	      return CCI_ER_LOGIN_TIMEOUT;
	    }
	  else if (ret < 0)
	    {
	      CLOSE_SOCKET (sock_fd);

	      if (retry_count < con_retry_count)
		{
		  retry_count++;
		  SLEEP_MILISEC (0, 100);
		  if (login_timeout > 0)
		    {
		      login_timeout -= 100;
		      if (login_timeout <= 0)
			{
			  return CCI_ER_LOGIN_TIMEOUT;
			}
		    }
		  goto connect_retry;
		}

	      return CCI_ER_CONNECT;
	    }
	  else
	    {
	      if (po[0].revents & POLLERR || po[0].revents & POLLHUP)
		{
		  CLOSE_SOCKET (sock_fd);
		  return CCI_ER_CONNECT;
		}
	    }
	}
      else
	{
	  CLOSE_SOCKET (sock_fd);

	  if (retry_count < con_retry_count)
	    {
	      retry_count++;
	      SLEEP_MILISEC (0, 100);

	      if (login_timeout > 0)
		{
		  login_timeout -= 100;
		  if (login_timeout <= 0)
		    {
		      return CCI_ER_LOGIN_TIMEOUT;
		    }
		}
	      goto connect_retry;
	    }

	  return CCI_ER_CONNECT;
	}
    }

  if (fcntl (sock_fd, F_SETFL, flags) < 0)
    {
      assert (0);
    }

  sock_opt = 1;
  if (setsockopt (sock_fd, IPPROTO_TCP, TCP_NODELAY, (char *) &sock_opt,
		  sizeof (sock_opt)) < 0)
    {
      assert (0);
    }

  sock_opt = 1;
  if (setsockopt (sock_fd, SOL_SOCKET, SO_KEEPALIVE, (char *) &sock_opt,
		  sizeof (sock_opt)) < 0)
    {
      assert (0);
    }

  *ret_sock = sock_fd;
  return CCI_ER_NO_ERROR;
}

static T_BROKER_REQUEST_MSG *
make_cas_connect_msg (const char *port_name)
{
  T_BROKER_REQUEST_MSG *brreq_msg;
  int opcode_msg_size;

  opcode_msg_size = brreq_msg_normal_broker_opcode_msg_size (port_name, 0);

  brreq_msg = brreq_msg_alloc (opcode_msg_size);
  if (brreq_msg)
    {
      char *ptr;
      ptr = brreq_msg_pack (brreq_msg, CAS_CLIENT_CCI,
			    BRREQ_OP_CODE_CAS_CONNECT, opcode_msg_size);
      ptr = brreq_msg_pack_port_name (ptr, port_name);
    }

  return brreq_msg;
}

static T_BROKER_REQUEST_MSG *
make_ping_check_msg (const char *port_name)
{
  int opcode_msg_size;

  opcode_msg_size = brreq_msg_normal_broker_opcode_msg_size (port_name, 0);

  T_BROKER_REQUEST_MSG *ping_check_msg = brreq_msg_alloc (opcode_msg_size);
  if (ping_check_msg)
    {
      char *ptr;
      ptr = brreq_msg_pack (ping_check_msg, CAS_CLIENT_CCI,
			    BRREQ_OP_CODE_PING, opcode_msg_size);
      ptr = brreq_msg_pack_port_name (ptr, port_name);
    }

  return ping_check_msg;
}

static T_BROKER_REQUEST_MSG *
make_mgmt_request_msg (char opcode, ...)
{
  T_BROKER_REQUEST_MSG *brreq_msg;
  int opcode_msg_size = 0;
  va_list ap;
  int num_args = 0;

  char *msg = NULL;
  int msg_alloced = 0;
  int pos = 0;
  int num_arg_offset;

  opcode_msg_size += SHARD_MGMT_MSG_INT_SIZE;
  msg = realloc_mgmt_arg_buf (msg, &msg_alloced, opcode_msg_size);
  num_arg_offset = pos;
  pos = copy_mgmt_arg_int (msg, pos, num_args);

  va_start (ap, opcode);
  while (true)
    {
      int arg_type;

      num_args++;

      arg_type = va_arg (ap, int);

      if (arg_type == MGMT_REQ_ARG_STR)
	{
	  char *str_arg = va_arg (ap, char *);

	  opcode_msg_size += (SHARD_MGMT_MSG_TPYE_SIZE +
			      SHARD_MGMT_MSG_INT_SIZE + strlen (str_arg) + 1);
	  msg = realloc_mgmt_arg_buf (msg, &msg_alloced, opcode_msg_size);
	  pos = copy_mgmt_arg_type (msg, pos, MGMT_REQ_ARG_STR);
	  pos = copy_mgmt_arg_string (msg, pos, str_arg);
	}
      else if (arg_type == MGMT_REQ_ARG_STR_ARRAY)
	{
	  int j;
	  int num_str_args = va_arg (ap, int);
	  char **str_arr_arg = va_arg (ap, char **);

	  opcode_msg_size += (SHARD_MGMT_MSG_TPYE_SIZE +
			      SHARD_MGMT_MSG_INT_SIZE);
	  msg = realloc_mgmt_arg_buf (msg, &msg_alloced, opcode_msg_size);

	  pos = copy_mgmt_arg_type (msg, pos, MGMT_REQ_ARG_STR_ARRAY);
	  pos = copy_mgmt_arg_int (msg, pos, num_str_args);

	  for (j = 0; j < num_str_args; j++)
	    {
	      opcode_msg_size += (SHARD_MGMT_MSG_INT_SIZE +
				  strlen (str_arr_arg[j]) + 1);
	      msg = realloc_mgmt_arg_buf (msg, &msg_alloced, opcode_msg_size);
	      pos = copy_mgmt_arg_string (msg, pos, str_arr_arg[j]);
	    }
	}
      else if (arg_type == MGMT_REQ_ARG_INT64)
	{
	  int64_t int64_arg = va_arg (ap, int64_t);
	  opcode_msg_size += (SHARD_MGMT_MSG_TPYE_SIZE +
			      SHARD_MGMT_MSG_LONG_SIZE);
	  msg = realloc_mgmt_arg_buf (msg, &msg_alloced, opcode_msg_size);
	  pos = copy_mgmt_arg_type (msg, pos, MGMT_REQ_ARG_INT64);
	  pos = copy_mgmt_arg_int64 (msg, pos, int64_arg);
	}
      else if (arg_type == MGMT_REQ_ARG_INT)
	{
	  int int_arg = va_arg (ap, int);
	  opcode_msg_size += (SHARD_MGMT_MSG_TPYE_SIZE +
			      SHARD_MGMT_MSG_INT_SIZE);
	  msg = realloc_mgmt_arg_buf (msg, &msg_alloced, opcode_msg_size);
	  pos = copy_mgmt_arg_type (msg, pos, MGMT_REQ_ARG_INT);
	  pos = copy_mgmt_arg_int (msg, pos, int_arg);
	}
      else
	{
	  assert (arg_type == MGMT_REQ_ARG_END);
	  opcode_msg_size += (SHARD_MGMT_MSG_TPYE_SIZE +
			      SHARD_MGMT_MSG_INT_SIZE);
	  msg = realloc_mgmt_arg_buf (msg, &msg_alloced, opcode_msg_size);
	  pos = copy_mgmt_arg_type (msg, pos, MGMT_REQ_ARG_INT);
	  pos = copy_mgmt_arg_int (msg, pos, BR_MGMT_REQ_LAST_ARG_VALUE);
	  break;
	}
    }

  copy_mgmt_arg_int (msg, num_arg_offset, num_args);

  va_end (ap);

  if (pos != opcode_msg_size || msg == NULL)
    {
      assert (0);
      FREE_MEM (msg);
      return NULL;
    }

  brreq_msg = brreq_msg_alloc (opcode_msg_size);
  if (brreq_msg)
    {
      char *ptr;

      ptr = brreq_msg_pack (brreq_msg, CAS_CLIENT_CCI,
			    opcode, opcode_msg_size);

      memcpy (ptr, msg, opcode_msg_size);
    }

  FREE_MEM (msg);

  return brreq_msg;
}

static char *
realloc_mgmt_arg_buf (char *ptr, int *alloced_size, int msg_size)
{
  if (*alloced_size < 0)
    {
      return NULL;
    }

  int new_size = (*alloced_size > 0 ? *alloced_size : 1024);

  while (new_size < msg_size)
    {
      new_size *= 2;
    }

  if (new_size != *alloced_size)
    {
      ptr = REALLOC (ptr, new_size);
      if (ptr == NULL)
	{
	  *alloced_size = -1;
	}
    }

  return ptr;
}

static int
copy_mgmt_arg_type (char *ptr, int pos, char arg_type)
{
  if (ptr == NULL)
    {
      return 0;
    }

  *(ptr + pos) = arg_type;
  return (pos + 1);
}

static int
copy_mgmt_arg_int (char *ptr, int pos, int value)
{
  if (ptr == NULL)
    {
      return 0;
    }

  value = htonl (value);
  memcpy (ptr + pos, &value, SHARD_MGMT_MSG_INT_SIZE);
  return (pos + SHARD_MGMT_MSG_INT_SIZE);
}

static int
copy_mgmt_arg_string (char *ptr, int pos, const char *value)
{
  if (ptr == NULL)
    {
      return 0;
    }

  int len = strlen (value) + 1;
  pos = copy_mgmt_arg_int (ptr, pos, len);
  memcpy (ptr + pos, value, len);
  return (pos + len);
}

static int
copy_mgmt_arg_int64 (char *ptr, int pos, int64_t value)
{
  if (ptr == NULL)
    {
      return 0;
    }

  value = htoni64 (value);
  memcpy (ptr + pos, &value, SHARD_MGMT_MSG_LONG_SIZE);
  return (pos + SHARD_MGMT_MSG_LONG_SIZE);
}

static const char *
read_shard_mgmt_result_int64 (const char *ptr, int *msg_size,
			      int64_t * ret_value)
{
  int64_t value;

  if (*msg_size < SHARD_MGMT_RES_LONG_SIZE || ptr == NULL)
    {
      return NULL;
    }
  memcpy (&value, ptr, SHARD_MGMT_RES_LONG_SIZE);
  value = ntohi64 (value);

  *ret_value = value;
  *msg_size -= SHARD_MGMT_RES_LONG_SIZE;
  return (ptr + SHARD_MGMT_RES_LONG_SIZE);
}

static const char *
read_shard_mgmt_result_short (const char *ptr, int *msg_size,
			      short *ret_value)
{
  short value;

  if (*msg_size < SHARD_MGMT_RES_SHORT_SIZE || ptr == NULL)
    {
      assert (false);
      return NULL;
    }
  memcpy (&value, ptr, SHARD_MGMT_RES_SHORT_SIZE);
  value = ntohs (value);

  *ret_value = value;
  *msg_size -= SHARD_MGMT_RES_SHORT_SIZE;
  return (ptr + SHARD_MGMT_RES_SHORT_SIZE);
}

static const char *
read_shard_mgmt_result_int (const char *ptr, int *msg_size, int *ret_value)
{
  int value;

  if (*msg_size < SHARD_MGMT_RES_INT_SIZE || ptr == NULL)
    {
      return NULL;
    }
  memcpy (&value, ptr, SHARD_MGMT_RES_INT_SIZE);
  value = ntohl (value);

  *ret_value = value;
  *msg_size -= SHARD_MGMT_RES_INT_SIZE;
  return (ptr + SHARD_MGMT_RES_INT_SIZE);
}

static const char *
read_shard_mgmt_result_string (const char *ptr, int *msg_size,
			       const char **ret_value)
{
  int str_size;

  ptr = read_shard_mgmt_result_int (ptr, msg_size, &str_size);
  if (ptr == NULL || *msg_size < str_size || str_size <= 0)
    {
      return NULL;
    }

  *ret_value = ptr;

  *msg_size -= str_size;
  return (ptr + str_size);
}

static int
unpack_shard_info_hdr (int msg_size, const char *msg_ptr,
		       int64_t * svr_created_at)
{
  const char *ptr = msg_ptr;
  int64_t created_at = 0;
  int err = CCI_ER_NO_ERROR;

  ptr = read_shard_mgmt_result_int64 (ptr, &msg_size, &created_at);

  if (ptr == NULL || created_at < 0)
    {
      err = CCI_ER_COMMUNICATION;
    }

  if (svr_created_at != NULL)
    {
      *svr_created_at = created_at;
    }

  return err;
}

static int
unpack_shard_node_info (int msg_size, const char *msg_ptr,
			CCI_SHARD_NODE_INFO ** ret_node_info)
{
  const char *ptr = msg_ptr;
  int64_t version;
  int node_count;
  CCI_SHARD_NODE_INFO *node_info;
  char *msg_start_ptr;
  int i;

  ptr = read_shard_mgmt_result_int64 (ptr, &msg_size, &version);
  ptr = read_shard_mgmt_result_int (ptr, &msg_size, &node_count);

  if (ptr == NULL || node_count <= 0 || version < 0)
    {
      return CCI_ER_COMMUNICATION;
    }

  node_info = MALLOC (sizeof (CCI_SHARD_NODE_INFO) * node_count + msg_size);
  if (node_info == NULL)
    {
      return CCI_ER_NO_MORE_MEMORY;
    }

  node_info->node_version = version;
  node_info->node_count = node_count;

  msg_start_ptr = (char *) &node_info->node_info[node_count];
  memcpy (msg_start_ptr, ptr, msg_size);

  ptr = msg_start_ptr;
  for (i = 0; i < node_count; i++)
    {
      ptr = read_shard_mgmt_result_short (ptr, &msg_size,
					  &node_info->node_info[i].nodeid);
      ptr = read_shard_mgmt_result_string (ptr, &msg_size,
					   &node_info->node_info[i].dbname);
      ptr = read_shard_mgmt_result_string (ptr, &msg_size,
					   &node_info->node_info[i].hostname);
      ptr = read_shard_mgmt_result_int (ptr, &msg_size,
					&node_info->node_info[i].port);
    }

  if (ptr == NULL)
    {
      FREE_MEM (node_info);
      return CCI_ER_COMMUNICATION;
    }

  *ret_node_info = node_info;

  return CCI_ER_NO_ERROR;
}

static int
unpack_shard_groupid_info (int msg_size, const char *msg_ptr,
			   CCI_SHARD_GROUPID_INFO ** ret_groupid_info)
{
  const char *ptr = msg_ptr;
  int64_t version;
  int groupid_count;
  CCI_SHARD_GROUPID_INFO *groupid_info;
  int i;
  char result_type;

  ptr = read_shard_mgmt_result_int64 (ptr, &msg_size, &version);
  ptr = read_shard_mgmt_result_int (ptr, &msg_size, &groupid_count);

  if (ptr == NULL || groupid_count <= 0 || version < 0 || msg_size < 1)
    {
      return CCI_ER_COMMUNICATION;
    }

  result_type = *ptr;
  msg_size--;
  ptr++;

  if (result_type != BR_RES_SHARD_INFO_ALL)
    {
      assert (0);
      return CCI_ER_COMMUNICATION;
    }

  groupid_info = MALLOC ((sizeof (CCI_SHARD_GROUPID_INFO)) +
			 (sizeof (T_CCI_NODEID) * (groupid_count + 1)));
  if (groupid_info == NULL)
    {
      return CCI_ER_NO_MORE_MEMORY;
    }

  groupid_info->groupid_version = version;
  groupid_info->groupid_count = groupid_count;

  for (i = 1; i <= groupid_count; i++)
    {
      ptr = read_shard_mgmt_result_short (ptr, &msg_size,
					  &groupid_info->nodeid_table[i]);
    }

  if (ptr == NULL)
    {
      FREE_MEM (groupid_info);
      return CCI_ER_COMMUNICATION;
    }

  *ret_groupid_info = groupid_info;

  return CCI_ER_NO_ERROR;
}

static int
recv_broker_response (int srv_sock_fd, const T_CON_HANDLE * con_handle,
		      T_BROKER_RESPONSE * br_res,
		      T_BROKER_RESPONSE_ADDITIONAL_MSG * res_msg,
		      int login_timeout, bool force_wait)
{
  int msg_size;
  char *msg;
  int ret_val;
  int i;

  ret_val = net_recv_stream (srv_sock_fd, con_handle, (char *) &msg_size, 4,
			     login_timeout, force_wait);
  if (ret_val < 0)
    {
      return ret_val;
    }

  msg_size = ntohl (msg_size);
  msg = MALLOC (msg_size);
  if (msg == NULL)
    {
      return CCI_ER_NO_MORE_MEMORY;
    }

  ret_val = net_recv_stream (srv_sock_fd, con_handle, msg, msg_size,
			     login_timeout, force_wait);
  if (ret_val < 0)
    {
      FREE_MEM (msg);
      return ret_val;
    }

  ret_val = brres_msg_unpack (br_res, msg, msg_size);

  FREE_MEM (msg);

  if (ret_val < 0)
    {
      return CCI_ER_COMMUNICATION;
    }

  for (i = 0; i < BROKER_RESPONSE_MAX_ADDITIONAL_MSG; i++)
    {
      if (br_res->additional_message_size[i] > 0)
	{
	  msg_size = br_res->additional_message_size[i];

	  msg = MALLOC (msg_size);
	  if (msg == NULL)
	    {
	      return CCI_ER_NO_MORE_MEMORY;
	    }
	  ret_val = net_recv_stream (srv_sock_fd, con_handle, msg, msg_size,
				     login_timeout, force_wait);
	  if (ret_val < 0)
	    {
	      FREE_MEM (msg);
	      return ret_val;
	    }

	  if (res_msg == NULL)
	    {
	      FREE_MEM (msg);
	    }
	  else
	    {
	      res_msg->size[i] = msg_size;
	      res_msg->msg[i] = msg;
	    }
	}
    }

  return CCI_ER_NO_ERROR;
}

static int
async_launch_epoll_ctl (int epoll_op,
			T_ASYNC_LAUNCH_RESULT * async_launch_res)
{
  int rv;
  struct epoll_event ev;

  if (async_Launch_epoll_info.epoll_fd < 0)
    {
      pthread_mutex_lock (&async_Launch_epoll_info.mutex);
      if (async_Launch_epoll_info.epoll_fd < 0)
	{
	  async_Launch_epoll_info.epoll_fd = epoll_create (100);
	}
      pthread_mutex_unlock (&async_Launch_epoll_info.mutex);
    }

  if (async_Launch_epoll_info.epoll_fd < 0)
    {
      assert (0);
      return -1;
    }

  ev.events = EPOLLET | EPOLLONESHOT | EPOLLERR | EPOLLHUP | EPOLLIN;

  ev.data.ptr = async_launch_res;

  rv = epoll_ctl (async_Launch_epoll_info.epoll_fd, epoll_op,
		  async_launch_res->sock_fd, &ev);
  if (rv < 0)
    {
      return -1;
    }

  pthread_mutex_lock (&async_Launch_epoll_info.mutex);
  if (epoll_op == EPOLL_CTL_ADD)
    {
      async_Launch_epoll_info.count++;
    }
  else if (epoll_op == EPOLL_CTL_DEL)
    {
      async_Launch_epoll_info.count--;
    }
  pthread_mutex_unlock (&async_Launch_epoll_info.mutex);

  return 0;
}

static int
async_launch_epoll_add (T_ASYNC_LAUNCH_RESULT * async_launch_res)
{
  return async_launch_epoll_ctl (EPOLL_CTL_ADD, async_launch_res);
}

static int
async_launch_epoll_del (T_ASYNC_LAUNCH_RESULT * async_launch_res)
{
  return async_launch_epoll_ctl (EPOLL_CTL_DEL, async_launch_res);
}

static int
set_launch_result (T_CCI_LAUNCH_RESULT * launch_result,
		   T_BROKER_RESPONSE_ADDITIONAL_MSG * res_msg)
{
  int status = 0;

  if (res_msg->size[0] < (int) sizeof (int))
    {
      return CCI_ER_COMMUNICATION;
    }
  else
    {
      memcpy (&status, res_msg->msg[0], sizeof (int));
      status = ntohl (status);
    }
  launch_result->exit_status = status;

  if (res_msg->size[1] > 0)
    {
      int size = MIN (res_msg->size[1],
		      (int) sizeof (launch_result->stdout_buf) - 1);
      memcpy (launch_result->stdout_buf, res_msg->msg[1], size);
      launch_result->stdout_buf[size] = '\0';
      launch_result->stdout_size = size;
    }
  else
    {
      launch_result->stdout_size = 0;
    }

  if (res_msg->size[2] > 0)
    {
      int size = MIN (res_msg->size[2],
		      (int) sizeof (launch_result->stderr_buf) - 1);
      memcpy (launch_result->stderr_buf, res_msg->msg[2], size);
      launch_result->stderr_buf[size] = '\0';
      launch_result->stderr_size = size;
    }
  else
    {
      launch_result->stderr_size = 0;
    }

  return CCI_ER_NO_ERROR;
}
