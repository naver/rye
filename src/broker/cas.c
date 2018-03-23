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
 * cas.c -
 */

#ident "$Id$"

#include <stdio.h>
#include <stdlib.h>
#include <signal.h>
#include <assert.h>

#include <unistd.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <poll.h>

#include "dbi.h"
#include "dbval.h"
#include "cas_common.h"
#include "cas.h"
#include "cas_network.h"
#include "cas_function.h"
#include "cas_net_buf.h"
#include "cas_log.h"
#include "cas_util.h"
#include "cas_execute.h"
#include "connection_support.h"
#include "perf_monitor.h"
#include "boot_cl.h"
#include "tcp.h"

#include "broker_shm.h"
#include "broker_util.h"
#include "broker_env_def.h"
#include "broker_acl.h"
#include "environment_variable.h"

static const int DEFAULT_CHECK_INTERVAL = 1;

#define FUNC_NEEDS_RESTORING_CON_STATUS(func_code) \
  (((func_code) == CAS_FC_GET_DB_PARAMETER) \
   ||((func_code) == CAS_FC_CLOSE_REQ_HANDLE) \
   ||((func_code) == CAS_FC_GET_DB_VERSION) \
   ||((func_code) == CAS_FC_CURSOR_CLOSE) \
   ||((func_code) == CAS_FC_SERVER_MODE))

static FN_RETURN process_request (SOCKET sock_fd, T_NET_BUF * net_buf,
				  T_REQ_INFO * req_info);

static int cas_main (void);
static SOCKET recv_client_fd_from_broker (SOCKET br_sock_fd,
					  int *client_ip_addr,
					  struct timeval *broker_recv_time);
static int send_broker_response (SOCKET client_sock_fd, int result_code);
static int read_db_connect_msg (T_DB_CONNECT_MSG ** db_connect_msg,
				SOCKET sock_fd);
static int cas_send_connect_reply_to_driver (SOCKET client_sock_fd,
					     bool send_server_info,
					     T_NET_BUF * net_buf);
static void cas_sig_handler (int signo);
static int cas_init (void);
static void cas_final (void);
static void cas_free (bool free_srv_handle);
static void query_cancel_sig_handler (int signo);
static void query_cancel_info_set (void);

static int cas_init_shm (void);

static int net_read_int_keep_con_auto (SOCKET clt_sock_fd,
				       MSG_HEADER * client_msg_header);
static int net_read_header_keep_con_on (SOCKET clt_sock_fd,
					MSG_HEADER * client_msg_header);
static bool check_client_alive (void);
static bool check_server_alive (const char *db_name,
				const PRM_NODE_INFO * node_info);

static int query_Sequence_num = 0;

const char *program_Name;
static int psize_At_start;

int shm_As_index;
T_SHM_APPL_SERVER *shm_Appl;
T_APPL_SERVER_INFO *as_Info;

T_SHM_BROKER *shm_Br_master;
int idx_Shm_shard_info = -1;

bool repl_Agent_connected = false;

struct timeval tran_Start_time;
struct timeval query_Start_time;
char query_Cancel_flag = 0;

bool autocommit_deferred = false;

static int con_Status_before_check_cas;
static bool is_First_request;
SOCKET new_Req_sock_fd = INVALID_SOCKET;
static int cas_Send_result_flag = TRUE;
int cas_Info_size = CAS_STATUS_INFO_SIZE;
#if defined (PROTOCOL_EXTENDS_DEBUG)
static char prev_Cas_info[CAS_STATUS_INFO_SIZE];
#endif

static char default_Db_user[] = "PUBLIC";
static char default_Db_passwd[] = "";
static char default_Url[] = "";
static char default_Version_str[] = "0.0.0.0";

T_ERROR_INFO err_Info;

typedef struct
{
  T_SERVER_FUNC func;
  const char *name;
} T_SERVER_FUNC_TABLE;

static T_SERVER_FUNC_TABLE server_Fn_table[CAS_FC_MAX - 1] = {
  /* CAS_FC_END_TRAN */
  {fn_end_tran, "end_tran"},
  /* CAS_FC_PREPARE */
  {fn_prepare, "prepare"},
  /* CAS_FC_EXECUTE */
  {fn_execute, "execute"},
  /* CAS_FC_GET_DB_PARAMETER */
  {fn_get_db_parameter, "get_db_parameter"},
  /* CAS_FC_CLOSE_REQ_HANDLE */
  {fn_close_req_handle, "close_req_handle"},
  /* CAS_FC_FETCH */
  {fn_fetch, "fetch"},
  /* CAS_FC_SCHEMA_INFO */
  {fn_schema_info, "schema_info"},
  /* CAS_FC_GET_DB_VERSION */
  {fn_get_db_version, "get_db_version"},
  /* CAS_FC_GET_CLASS_NUM_OBJS */
  {fn_get_class_num_objs, "get_class_num_objs"},
  /* CAS_FC_EXECUTE_BATCH */
  {fn_execute_batch, "execute_batch"},
  /* CAS_FC_GET_QUERY_PLAN */
  {fn_get_query_plan, "get_query_plan"},
  /* CAS_FC_CON_CLOSE */
  {fn_con_close, "con_close"},
  /* CAS_FC_CHECK_CAS */
  {fn_check_cas, "check_cas"},
  /* CAS_FC_CURSOR_CLOSE */
  {fn_cursor_close, "fn_cursor_close"},
  /* CAS_FC_CHANGE_DBUSER */
  {fn_change_dbuser, "fn_change_dbuser"},
  /* CAS_FC_UPDATE_GROUP_ID */
  {fn_update_group_id, "fn_update_group_id"},
  /* CAS_FC_INSERT_GID_REMOVED_INFO */
  {fn_insert_gid_removed_info, "fn_insert_gid_removed_info"},
  /* CAS_FC_DELETE_GID_REMOVED_INFO */
  {fn_delete_gid_removed_info, "fn_delete_gid_removed_info"},
  /* CAS_FC_DELETE_GID_SKEY_INFO */
  {fn_delete_gid_skey_info, "fn_delete_gid_skey_info"},
  /* CAS_FC_BLOCK_GLOBAL_DML */
  {fn_block_globl_dml, "fn_block_globl_dml"},
  /* CAS_FC_SERVER_MODE */
  {fn_server_mode, "fn_server_mode"},
  /* CAS_FC_SEND_REPL_DATA */
  {fn_send_repl_data, "fn_send_repl_data"},
  /* CAS_FC_NOTIFY_HA_AGENT_STATE */
  {fn_notify_ha_agent_state, "fn_notify_ha_agent_state"},
};

static T_REQ_INFO req_Info;
static SOCKET srv_Sock_fd;
static int cas_Req_count = 0;
static SOCKET client_Sock_fd = INVALID_SOCKET;

static void
cas_make_session_for_driver (char *out)
{
  size_t size = 0;
  SESSION_ID session;

  memcpy (out + size, db_get_server_session_key (), SERVER_SESSION_KEY_SIZE);
  size += SERVER_SESSION_KEY_SIZE;
  session = db_get_session_id ();
  session = htonl (session);
  memcpy (out + size, &session, sizeof (SESSION_ID));
  size += sizeof (SESSION_ID);
  memset (out + size, 0, DRIVER_SESSION_SIZE - size);
}

static void
cas_set_session_id (const char *session)
{
  if (session)
    {
      SESSION_ID id;

      id = *(const SESSION_ID *) (session + 8);
      id = ntohl (id);
      db_set_server_session_key (session);
      db_set_session_id (id);
      cas_sql_log_write_and_end (0, "session id for connection %u", id);
    }
  else
    {
      char key[] = { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF };

      cas_sql_log_write_and_end (0, "session id for connection 0");
      db_set_server_session_key (key);
      db_set_session_id (DB_EMPTY_SESSION);
    }
}

static int
cas_send_connect_reply_to_driver (SOCKET client_sock_fd,
				  bool send_server_info, T_NET_BUF * net_buf)
{
  char sessid[DRIVER_SESSION_SIZE];
  int server_start_time = 0;
  MSG_HEADER cas_msg_header;

  net_buf_clear (net_buf);

  init_msg_header (&cas_msg_header);

  if (send_server_info)
    {
      cas_make_session_for_driver (sessid);
      server_start_time = db_get_server_start_time ();
    }
  else
    {
      memset (sessid, 0, sizeof (sessid));
    }

  net_buf_cp_byte (net_buf, SUCCESS_RESPONSE);

  net_buf_cp_short (net_buf, MAJOR_VERSION);
  net_buf_cp_short (net_buf, MINOR_VERSION);
  net_buf_cp_short (net_buf, PATCH_VERSION);
  net_buf_cp_short (net_buf, BUILD_SEQ);
  net_buf_cp_int (net_buf, shm_As_index + 1, NULL);
  net_buf_cp_int (net_buf, as_Info->pid, NULL);
  net_buf_cp_int (net_buf, DRIVER_SESSION_SIZE, NULL);
  net_buf_cp_str (net_buf, sessid, DRIVER_SESSION_SIZE);
  net_buf_cp_byte (net_buf, CAS_DBMS_RYE);
  net_buf_cp_byte (net_buf, CAS_HOLDABLE_RESULT_SUPPORT);

  if (as_Info->cur_statement_pooling)
    {
      net_buf_cp_byte (net_buf, CAS_STATEMENT_POOLING_ON);
    }
  else
    {
      net_buf_cp_byte (net_buf, CAS_STATEMENT_POOLING_OFF);
    }

  if (as_Info->cci_default_autocommit)
    {
      net_buf_cp_byte (net_buf, CCI_DEFAULT_AUTOCOMMIT_ON);
    }
  else
    {
      net_buf_cp_byte (net_buf, CCI_DEFAULT_AUTOCOMMIT_OFF);
    }

  net_buf_cp_int (net_buf, server_start_time, NULL);

  if (net_buf->err_code)
    {
      net_write_error (client_sock_fd, cas_Info_size,
		       CAS_ERROR_INDICATOR, net_buf->err_code, NULL);
      return -1;
    }

  if (as_Info->con_status == CON_STATUS_IN_TRAN)
    {
      cas_msg_header.info_ptr[CAS_STATUS_INFO_IDX_STATUS] = CAS_STATUS_ACTIVE;
    }
  else
    {
      cas_msg_header.info_ptr[CAS_STATUS_INFO_IDX_STATUS] =
	CAS_STATUS_INACTIVE;
    }

  *(cas_msg_header.msg_body_size_ptr) = htonl (net_buf->data_size);
  memcpy (net_buf->data, cas_msg_header.msg_body_size_ptr,
	  NET_BUF_HEADER_MSG_SIZE);

  memcpy (net_buf->data + NET_BUF_HEADER_MSG_SIZE,
	  cas_msg_header.info_ptr, cas_Info_size);


  if (net_write_stream
      (client_sock_fd, net_buf->data, NET_BUF_CURR_SIZE (net_buf)) < 0)
    {
      return -1;
    }

  return 0;
}

int
main (int argc, char *argv[])
{
  int res = 0;

  signal (SIGTERM, cas_sig_handler);
  signal (SIGINT, cas_sig_handler);
  signal (SIGUSR1, SIG_IGN);
  signal (SIGPIPE, SIG_IGN);
  signal (SIGXFSZ, SIG_IGN);

  if (cas_init () < 0)
    {
      return -1;
    }

  program_Name = argv[0];
  if (argc == 2 && strcmp (argv[1], "--version") == 0)
    {
      printf ("%s\n", makestring (BUILD_NUMBER));
      return 0;
    }

  memset (&req_Info, 0, sizeof (req_Info));

  res = cas_main ();

  return res;
}

static int
cas_main (void)
{
  T_NET_BUF net_buf;
  SOCKET br_sock_fd;
  int err_code;
#if 0
  char *db_name, *db_user, *db_passwd, *db_sessionid;
#endif
  char port_name[BROKER_PATH_MAX];
  struct timeval broker_recv_time;
  in_addr_t client_ip_addr;
  FN_RETURN fn_ret = FN_KEEP_CONN;
  char client_ip_str[16];
  bool is_new_connection;
  T_DB_CONNECT_MSG *db_connect_msg = NULL;

#if defined (PROTOCOL_EXTENDS_DEBUG)
  prev_Cas_info[CAS_STATUS_INFO_IDX_STATUS] =
    CAS_STATUS_INFO_RESERVED_DEFAULT;
#endif

  ut_get_as_port_name (port_name, shm_Appl->broker_name, shm_As_index,
		       BROKER_PATH_MAX);

  srv_Sock_fd = net_init_env (port_name);
  if (IS_INVALID_SOCKET (srv_Sock_fd))
    {
      return -1;
    }

  net_buf_init (&net_buf);
  net_buf.data = (char *) RYE_MALLOC (NET_BUF_ALLOC_SIZE);
  if (net_buf.data == NULL)
    {
      return -1;
    }
  net_buf.alloc_size = NET_BUF_ALLOC_SIZE;

  cas_sql_log_write_and_end (0, "CAS STARTED pid %d", getpid ());

  (void) er_init (prm_get_string_value (PRM_ID_ER_LOG_FILE),
		  prm_get_integer_value (PRM_ID_ER_EXIT_ASK));
  er_log_debug (ARG_FILE_LINE, "CAS STARTED pid %d", getpid ());

  if (lang_init () != NO_ERROR)
    {
      RYE_FREE_MEM (net_buf.data);
      return -1;
    }

  unset_hang_check_time ();

  as_Info->service_ready_flag = TRUE;
  as_Info->con_status = CON_STATUS_IN_TRAN;
  as_Info->transaction_start_time = time (0);
  as_Info->cur_keep_con = KEEP_CON_DEFAULT;

  sql_log_Notice_mode_flush = false;

  psize_At_start = as_Info->psize =
    (int) (os_get_mem_size (getpid (), MEM_VSIZE) / ONE_K);

  if (shm_Appl->appl_server_max_size > shm_Appl->appl_server_hard_limit)
    {
      cas_sql_log_write_and_end (0,
				 "CONFIGURATION WARNING - the APPL_SERVER_MAX_SIZE(%dM) is greater than the APPL_SERVER_MAX_SIZE_HARD_LIMIT(%dM)",
				 shm_Appl->appl_server_max_size / ONE_K,
				 shm_Appl->appl_server_hard_limit / ONE_K);
    }

  for (;;)
    {
      error_info_clear ();

      unset_hang_check_time ();
      br_sock_fd = net_connect_client (srv_Sock_fd);

      if (IS_INVALID_SOCKET (br_sock_fd))
	{
	  goto finish_cas;
	}

      as_Info->con_status = CON_STATUS_IN_TRAN;
      as_Info->transaction_start_time = time (0);
      sql_log_Notice_mode_flush = false;

      client_ip_addr = 0;

      net_timeout_set (NET_MIN_TIMEOUT);

      client_Sock_fd = recv_client_fd_from_broker (br_sock_fd,
						   (int *) &client_ip_addr,
						   &broker_recv_time);
      RYE_CLOSE_SOCKET (br_sock_fd);
      if (client_Sock_fd == -1)
	{
	  goto finish_cas;
	}

      set_hang_check_time ();

      net_timeout_set (NET_DEFAULT_TIMEOUT);

      if (as_Info->cas_err_log_reset == CAS_LOG_RESET_REOPEN)
	{
	  er_stack_clearall ();
	  er_clear ();
	  as_Info->cas_err_log_reset = 0;
	}

      css_ip_to_str (client_ip_str, sizeof (client_ip_str), client_ip_addr);
      cas_sql_log_write_and_end (0, "CLIENT IP %s", client_ip_str);

      unset_hang_check_time ();

      if (IS_INVALID_SOCKET (client_Sock_fd))
	{
	  goto finish_cas;
	}

      if (send_broker_response (client_Sock_fd, 0) < 0)
	{
	  RYE_CLOSE_SOCKET (client_Sock_fd);
	  goto finish_cas;
	}

      req_Info.clt_version = as_Info->clt_version;

      err_code = read_db_connect_msg (&db_connect_msg, client_Sock_fd);
      if (err_code < 0)
	{
	  net_write_error (client_Sock_fd, cas_Info_size,
			   CAS_ERROR_INDICATOR, err_code, NULL);
	  RYE_CLOSE_SOCKET (client_Sock_fd);
	  goto finish_cas;
	}
      else
	{
	  char *db_err_msg = NULL;
	  struct timeval cas_start_time;
	  char connected_node_str[MAX_NODE_INFO_STR_LEN];
	  PRM_NODE_INFO connected_node;

	  gettimeofday (&cas_start_time, NULL);

	  /* Send response to broker health checker */
	  if (strcmp (db_connect_msg->db_name, HEALTH_CHECK_DUMMY_DB) == 0)
	    {
	      cas_sql_log_write_and_end (0,
					 "Incoming health check request from client.");

	      cas_send_connect_reply_to_driver (client_Sock_fd, false,
						&net_buf);
	      RYE_CLOSE_SOCKET (client_Sock_fd);

	      goto finish_cas;
	    }

	  strncpy (as_Info->client_version, db_connect_msg->client_version,
		   SRV_CON_VER_STR_MAX_SIZE);

	  cas_sql_log_write_and_end (0, "CLIENT VERSION %s",
				     as_Info->client_version);
	  cas_set_session_id (db_connect_msg->db_session_id);
	  if (db_get_session_id () != DB_EMPTY_SESSION)
	    {
	      is_new_connection = false;
	    }
	  else
	    {
	      is_new_connection = true;
	    }

	  set_hang_check_time ();

	  if (as_Info->reset_flag == TRUE)
	    {
	      er_log_debug (ARG_FILE_LINE, "main: set reset_flag");
	      cas_set_db_connect_status (-1);	/* DB_CONNECTION_STATUS_RESET */
	      as_Info->reset_flag = FALSE;
	    }

	  unset_hang_check_time ();

	  if (br_acl_check_right (shm_Appl, NULL, 0,
				  db_connect_msg->db_name,
				  db_connect_msg->db_user,
				  (unsigned char *) (&client_ip_addr)) < 0)
	    {
	      char err_msg[1024];

	      as_Info->num_connect_rejected++;

	      sprintf (err_msg, "Authorization error.(Address is rejected)");

	      net_write_error (client_Sock_fd, cas_Info_size,
			       DBMS_ERROR_INDICATOR,
			       CAS_ER_NOT_AUTHORIZED_CLIENT, err_msg);

	      set_hang_check_time ();

	      cas_sql_log_write_and_end (0,
					 "connect db %s user %s url %s - rejected",
					 db_connect_msg->db_name,
					 db_connect_msg->db_user,
					 db_connect_msg->url);

	      if (shm_Appl->access_log == ON)
		{
		  cas_access_log (&cas_start_time,
				  shm_As_index,
				  client_ip_addr,
				  db_connect_msg->db_name,
				  db_connect_msg->db_user, ACL_REJECTED);
		}

	      unset_hang_check_time ();

	      RYE_CLOSE_SOCKET (client_Sock_fd);

	      goto finish_cas;
	    }

	  err_code = ux_database_connect (db_connect_msg->db_name,
					  db_connect_msg->db_user,
					  db_connect_msg->db_passwd,
					  &db_err_msg);

	  if (err_code < 0)
	    {
	      char msg_buf[LINE_MAX];

	      net_write_error (client_Sock_fd, cas_Info_size,
			       err_Info.err_indicator,
			       err_Info.err_number, db_err_msg);

	      if (db_err_msg == NULL)
		{
		  snprintf (msg_buf, LINE_MAX,
			    "connect db %s user %s url %s, error:%d.",
			    db_connect_msg->db_name,
			    db_connect_msg->db_user, db_connect_msg->url,
			    err_Info.err_number);
		}
	      else
		{
		  snprintf (msg_buf, LINE_MAX,
			    "connect db %s user %s url %s, error:%d, %s",
			    db_connect_msg->db_name,
			    db_connect_msg->db_user, db_connect_msg->url,
			    err_Info.err_number, db_err_msg);
		}

	      cas_sql_log_write_and_end (0, msg_buf);
	      cas_slow_log_write_and_end (NULL, 0, msg_buf);

	      RYE_CLOSE_SOCKET (client_Sock_fd);
	      RYE_FREE_MEM (db_err_msg);

	      goto finish_cas;
	    }

	  RYE_FREE_MEM (db_err_msg);

	  set_hang_check_time ();

	  if (shm_Appl->access_log == ON)
	    {
	      ACCESS_LOG_TYPE type =
		(is_new_connection) ? NEW_CONNECTION : CLIENT_CHANGED;

	      cas_access_log (&cas_start_time, shm_As_index,
			      client_ip_addr,
			      db_connect_msg->db_name,
			      db_connect_msg->db_user, type);
	    }

	  connected_node = boot_get_host_connected ();
	  prm_node_info_to_str (connected_node_str,
				sizeof (connected_node_str), &connected_node);
	  cas_sql_log_write_and_end (0, "connect db %s user %s url %s"
				     " connected host %s",
				     db_connect_msg->db_name,
				     db_connect_msg->db_user,
				     db_connect_msg->url, connected_node_str);

	  as_Info->cur_keep_con = shm_Appl->keep_connection;

	  if (shm_Appl->statement_pooling)
	    {
	      as_Info->cur_statement_pooling = ON;
	    }
	  else
	    {
	      as_Info->cur_statement_pooling = OFF;
	    }

	  if (cas_send_connect_reply_to_driver (client_Sock_fd, true,
						&net_buf) < 0)
	    {
	      cas_sql_log_write_and_end (0, "ERROR - connect reply");
	      RYE_CLOSE_SOCKET (client_Sock_fd);
	      goto finish_cas;
	    }

	  as_Info->cci_default_autocommit = shm_Appl->cci_default_autocommit;
	  req_Info.need_rollback = TRUE;

	  gettimeofday (&tran_Start_time, NULL);
	  gettimeofday (&query_Start_time, NULL);

	  cas_log_error_handler_begin ();
	  con_Status_before_check_cas = -1;
	  is_First_request = true;
	  fn_ret = FN_KEEP_CONN;
	  idx_Shm_shard_info = -1;

	  while (fn_ret == FN_KEEP_CONN)
	    {
	      query_Cancel_flag = 0;

	      fn_ret = process_request (client_Sock_fd, &net_buf, &req_Info);
	      is_First_request = false;
	      cas_log_error_handler_clear ();
	      as_Info->last_access_time = time (NULL);
	    }

#if defined (PROTOCOL_EXTENDS_DEBUG)
	  prev_Cas_info[CAS_STATUS_INFO_IDX_STATUS] =
	    CAS_STATUS_INFO_RESERVED_DEFAULT;
#endif

	  if (as_Info->cur_statement_pooling)
	    {
	      hm_srv_handle_free_all (true);
	    }

	  if (ux_end_tran (CCI_TRAN_ROLLBACK, false) < 0)
	    {
	      as_Info->reset_flag = TRUE;
	    }

	  if (fn_ret != FN_KEEP_SESS)
	    {
	      db_end_session ();
	    }

	  if (as_Info->reset_flag == TRUE)
	    {
	      ux_database_shutdown ();
	      as_Info->reset_flag = FALSE;
	      cas_set_db_connect_status (-1);	/* DB_CONNECTION_STATUS_RESET */
	    }

	  cas_log_error_handler_end ();
	}

      RYE_CLOSE_SOCKET (client_Sock_fd);

    finish_cas:
      set_hang_check_time ();

      if (as_Info->con_status != CON_STATUS_CLOSE_AND_CONNECT)
	{
	  as_Info->cas_clt_ip_addr = 0;
	  as_Info->cas_clt_port = 0;
	  as_Info->client_version[0] = '\0';
	}

      as_Info->transaction_start_time = (time_t) 0;
      cas_sql_log_write_and_end (0, "disconnect");
      cas_sql_log_write_and_end (0, "STATE idle");
      cas_log_close_all ();
      cas_Req_count++;

      unset_hang_check_time ();

      if (is_server_aborted ())
	{
	  cas_final ();
	  return 0;
	}
      else
	if (!
	    (as_Info->cur_keep_con == KEEP_CON_AUTO
	     && as_Info->con_status == CON_STATUS_CLOSE_AND_CONNECT))
	{
	  if (restart_is_needed ())
	    {
	      cas_final ();
	      return 0;
	    }
	  else
	    {
	      as_Info->uts_status = UTS_STATUS_IDLE;
	    }
	}
    }

  return 0;
}

static int
send_broker_response (SOCKET client_sock_fd, int result_code)
{
  T_BROKER_RESPONSE_NET_MSG res_msg;

  brres_msg_pack (&res_msg, result_code, 0, NULL);

  /* send NO_ERROR to client */
  if (net_write_stream (client_sock_fd, res_msg.msg_buffer,
			res_msg.msg_buffer_size) < 0)
    {
      return -1;
    }

  return 0;
}

static int
read_db_connect_msg (T_DB_CONNECT_MSG ** db_connect_msg,
		     SOCKET client_sock_fd)
{
  T_DB_CONNECT_MSG *con_msg;
  void **argv = NULL;
  int argc;
  int tmp_size;
  MSG_HEADER client_msg_header;
  char func_code;

  init_msg_header (&client_msg_header);

  if (net_read_header (client_sock_fd, &client_msg_header) < 0)
    {
      return CAS_ER_COMMUNICATION;
    }

  if (*db_connect_msg == NULL
      || (*db_connect_msg)->msg_size < *(client_msg_header.msg_body_size_ptr))
    {
      RYE_FREE_MEM (*db_connect_msg);
      *db_connect_msg = RYE_MALLOC (sizeof (T_DB_CONNECT_MSG) +
				    *(client_msg_header.msg_body_size_ptr));
    }

  con_msg = *db_connect_msg;

  if (con_msg == NULL)
    {
      return CAS_ER_NO_MORE_MEMORY;
    }

  con_msg->msg_size = *(client_msg_header.msg_body_size_ptr);

  if (net_read_stream (client_sock_fd,
		       con_msg->msg_buffer, con_msg->msg_size) < 0)
    {
      return CAS_ER_COMMUNICATION;
    }

  argc = net_decode_str (con_msg->msg_buffer, con_msg->msg_size,
			 &func_code, &argv);
  if (argc < 0)
    {
      return argc;
    }

  assert (func_code == 0);

  if (argc < 5)
    {
      RYE_FREE_MEM (argv);
      return CAS_ER_COMMUNICATION;
    }

  net_arg_get_str (&con_msg->db_name, &tmp_size, argv[0]);
  if (con_msg->db_name == NULL)
    {
      RYE_FREE_MEM (argv);
      return CAS_ER_COMMUNICATION;
    }

  net_arg_get_str (&con_msg->db_user, &tmp_size, argv[1]);
  if (con_msg->db_user == NULL || con_msg->db_user[0] == '\0')
    {
      con_msg->db_user = default_Db_user;
    }

  net_arg_get_str (&con_msg->db_passwd, &tmp_size, argv[2]);
  if (con_msg->db_passwd == NULL)
    {
      con_msg->db_passwd = default_Db_passwd;
    }

  net_arg_get_str (&con_msg->url, &tmp_size, argv[3]);
  if (con_msg->url == NULL)
    {
      con_msg->url = default_Url;
    }

  net_arg_get_str (&con_msg->client_version, &tmp_size, argv[4]);
  if (con_msg->client_version == NULL)
    {
      con_msg->client_version = default_Version_str;
    }

  net_arg_get_str (&con_msg->db_session_id, &tmp_size, argv[5]);
  if (tmp_size <= (int) (SERVER_SESSION_KEY_SIZE + sizeof (SESSION_ID)))
    {
      con_msg->db_session_id = NULL;
    }

  RYE_FREE_MEM (argv);

  return 0;
}

static SOCKET
recv_client_fd_from_broker (SOCKET br_sock_fd, int *client_ip_addr,
			    struct timeval *broker_recv_time)
{
  int con_status;
  SOCKET client_sock_fd;
  int one = 1;

  if (net_read_int (br_sock_fd, &con_status) < 0)
    {
      cas_sql_log_write_and_end (0,
				 "HANDSHAKE ERROR net_read_int(con_status)");
      return -1;
    }
  if (net_write_int (br_sock_fd, as_Info->con_status) < 0)
    {
      cas_sql_log_write_and_end (0,
				 "HANDSHAKE ERROR net_write_int(con_status)");
      return -1;
    }

  client_sock_fd = css_recv_fd (br_sock_fd, client_ip_addr, broker_recv_time);
  if (client_sock_fd == -1)
    {
      cas_sql_log_write_and_end (0, "HANDSHAKE ERROR recv_fd %d",
				 client_sock_fd);
      return -1;
    }
  if (net_write_int (br_sock_fd, as_Info->uts_status) < 0)
    {
      cas_sql_log_write_and_end (0,
				 "HANDSHAKE ERROR net_write_int(uts_status)");
      RYE_CLOSE_SOCKET (client_sock_fd);
      return -1;
    }

  if (setsockopt (client_sock_fd, IPPROTO_TCP, TCP_NODELAY,
		  (char *) &one, sizeof (one)) < 0)
    {
      assert (0);
    }
  ut_set_keepalive (client_sock_fd);

  return client_sock_fd;
}

/*
 * set_hang_check_time() -
 *   Mark the current time so that cas hang checker thread
 *   in broker can monitor the status of the cas.
 *   If the time is set, ALWAYS unset it
 *   before meeting indefinite blocking operation.
 */
void
set_hang_check_time (void)
{
  if (as_Info != NULL && shm_Appl != NULL && shm_Appl->monitor_hang_flag)
    {
      as_Info->claimed_alive_time = time (NULL);
    }

  return;
}

/*
 * unset_hang_check_time -
 *   Clear the time and the cas is free from being monitored
 *   by hang checker in broker.
 */
void
unset_hang_check_time (void)
{
  if (as_Info != NULL && shm_Appl != NULL && shm_Appl->monitor_hang_flag)
    {
      as_Info->claimed_alive_time = (time_t) 0;
    }

  return;
}

static bool
check_client_alive ()
{
  bool ret = true;

  if (query_Cancel_flag == 1)
    {
      ret = false;
    }
  else if (!IS_INVALID_SOCKET (client_Sock_fd))
    {
      int n;
      struct pollfd po = { 0, 0, 0 };

      po.fd = client_Sock_fd;
      po.events = POLLIN;

      n = poll (&po, 1, 0);
      if (n == 1)
	{
	  if (po.revents & POLLERR || po.revents & POLLHUP)
	    {
	      ret = false;
	    }
	  else if (po.revents & POLLIN)
	    {
	      char buf[1];
	      n = recv (client_Sock_fd, buf, 1, MSG_PEEK | MSG_DONTWAIT);
	      if (n <= 0)
		{
		  ret = false;
		}
	    }
	}

      if (ret == false)
	{
	  query_cancel_info_set ();
	}
    }

  return ret;
}

static bool
check_server_alive (const char *db_name, const PRM_NODE_INFO * node_info)
{
  int i, u_index;

  if (as_Info != NULL && shm_Appl != NULL && shm_Appl->monitor_server_flag)
    {
      /* if db_name is NULL, use the CAS shared memory */
      if (db_name == NULL)
	{
	  db_name = as_Info->database_name;
	}
      /* if node_info is NULL, use the CAS shared memory */
      if (node_info == NULL)
	{
	  node_info = &as_Info->db_node;
	}

      u_index = shm_Appl->unusable_databases_seq % 2;

      for (i = 0; i < shm_Appl->unusable_databases_cnt[u_index]; i++)
	{
	  const char *unusable_db_name;
	  const PRM_NODE_INFO *unusable_db_host;

	  unusable_db_name =
	    shm_Appl->unusable_databases[u_index][i].database_name;
	  unusable_db_host =
	    &shm_Appl->unusable_databases[u_index][i].db_node;

	  if (strcmp (unusable_db_name, db_name) == 0 &&
	      prm_is_same_node (unusable_db_host, node_info) == true)
	    {
	      return false;
	    }
	}
    }

  return true;
}

static void
cas_sig_handler (int signo)
{
  signal (signo, SIG_IGN);
  cas_free (false);
  exit (0);
}

static void
cas_final (void)
{
  cas_free (true);
  as_Info->pid = 0;
  as_Info->uts_status = UTS_STATUS_RESTART;
  exit (0);
}

static void
cas_free (bool free_srv_handle)
{
#ifdef MEM_DEBUG
  int fd;
#endif
  int max_process_size;

  if (free_srv_handle)
    {
      if (as_Info->cur_statement_pooling)
	{
	  hm_srv_handle_free_all (true);
	}

      ux_database_shutdown ();
    }

  max_process_size = (shm_Appl->appl_server_max_size > 0) ?
    shm_Appl->appl_server_max_size : (psize_At_start * 2);
  if (as_Info->psize > max_process_size)
    {
      cas_sql_log_write_and_end (0,
				 "CAS MEMORY USAGE (%dM) HAS EXCEEDED MAX SIZE (%dM)",
				 as_Info->psize / ONE_K,
				 max_process_size / ONE_K);
    }

  if (as_Info->psize > shm_Appl->appl_server_hard_limit)
    {
      cas_sql_log_write_and_end (0,
				 "CAS MEMORY USAGE (%dM) HAS EXCEEDED HARD LIMIT (%dM)",
				 as_Info->psize / ONE_K,
				 shm_Appl->appl_server_hard_limit / ONE_K);
    }

  cas_sql_log_write_and_end (0, "CAS TERMINATED pid %d", getpid ());
  cas_log_close_all ();

#ifdef MEM_DEBUG
  fd = open ("mem_debug.log", O_CREAT | O_TRUNC | O_WRONLY, 0666);
  if (fd > 0)
    {
      malloc_dump (fd);
      close (fd);
    }
#endif
}

void
query_cancel_enable_sig_handler ()
{
  signal (SIGUSR1, query_cancel_sig_handler);
}

void
query_cancel_disable_sig_handler ()
{
  signal (SIGUSR1, SIG_IGN);
}

static void
query_cancel_sig_handler (int signo)
{
  signal (signo, SIG_IGN);

  query_cancel_info_set ();
}

static void
query_cancel_info_set ()
{
  struct timespec ts;
  INT64 query_cancel_time;

  as_Info->num_interrupts %= MAX_DIAG_DATA_VALUE;
  as_Info->num_interrupts++;

  clock_gettime (CLOCK_REALTIME, &ts);
  query_cancel_time = ts.tv_sec * 1000LL;
  query_cancel_time += (ts.tv_nsec / 1000000LL);

  cas_sql_log_set_query_cancel_time (query_cancel_time);

  query_Cancel_flag = 1;
}

static FN_RETURN
process_request (SOCKET sock_fd, T_NET_BUF * net_buf, T_REQ_INFO * req_info)
{
  MSG_HEADER client_msg_header;
  MSG_HEADER cas_msg_header;
  char *read_msg;
  char func_code;
  int argc;
  void **argv = NULL;
  int err_code;
  int con_status_to_restore, old_con_status;
  T_SERVER_FUNC server_fn;
  FN_RETURN fn_ret = FN_KEEP_CONN;

  error_info_clear ();
  init_msg_header (&client_msg_header);
  init_msg_header (&cas_msg_header);

  old_con_status = as_Info->con_status;

  unset_hang_check_time ();
  if (as_Info->cur_keep_con == KEEP_CON_AUTO)
    {
      err_code = net_read_int_keep_con_auto (sock_fd, &client_msg_header);
    }
  else
    {
      err_code = net_read_header_keep_con_on (sock_fd, &client_msg_header);

      if (as_Info->cur_keep_con == KEEP_CON_ON
	  && as_Info->con_status == CON_STATUS_OUT_TRAN)
	{
	  as_Info->con_status = CON_STATUS_IN_TRAN;
	  as_Info->transaction_start_time = time (0);
	  sql_log_Notice_mode_flush = false;
	}
    }

  if (err_code < 0)
    {
      const char *cas_log_msg = NULL;

      fn_ret = FN_CLOSE_CONN;

      if (as_Info->reset_flag)
	{
	  cas_log_msg = "RESET";
	  cas_sql_log_write_and_end (0, cas_log_msg);
	  fn_ret = FN_KEEP_SESS;
	}
      if (as_Info->con_status == CON_STATUS_CLOSE_AND_CONNECT)
	{
	  cas_log_msg = "CHANGE CLIENT";
	  fn_ret = FN_KEEP_SESS;
	}
      if (cas_log_msg == NULL)
	{
	  if (is_net_timed_out ())
	    {
	      if (as_Info->reset_flag == TRUE)
		{
		  cas_log_msg = "CONNECTION RESET";
		}
	      else
		{
		  cas_log_msg = "SESSION TIMEOUT";
		}
	    }
	  else
	    {
	      cas_log_msg = "COMMUNICATION ERROR net_read_header()";
	    }
	}
      cas_sql_log_write_and_end (0, cas_log_msg);
      return fn_ret;
    }

  if (shm_Appl->session_timeout < 0)
    net_timeout_set (NET_DEFAULT_TIMEOUT);
  else
    net_timeout_set (MIN (shm_Appl->session_timeout, NET_DEFAULT_TIMEOUT));

  read_msg = (char *) RYE_MALLOC (*(client_msg_header.msg_body_size_ptr));
  if (read_msg == NULL)
    {
      net_write_error (sock_fd, cas_Info_size,
		       CAS_ERROR_INDICATOR, CAS_ER_NO_MORE_MEMORY, NULL);
      return FN_CLOSE_CONN;
    }
  if (net_read_stream (sock_fd, read_msg,
		       *(client_msg_header.msg_body_size_ptr)) < 0)
    {
      RYE_FREE_MEM (read_msg);
      net_write_error (sock_fd, cas_Info_size,
		       CAS_ERROR_INDICATOR, CAS_ER_COMMUNICATION, NULL);
      cas_sql_log_write_and_end (0, "COMMUNICATION ERROR net_read_stream()");
      return FN_CLOSE_CONN;
    }

  argc =
    net_decode_str (read_msg,
		    *(client_msg_header.msg_body_size_ptr),
		    &func_code, &argv);
  if (argc < 0)
    {
      RYE_FREE_MEM (read_msg);
      net_write_error (sock_fd, cas_Info_size,
		       CAS_ERROR_INDICATOR, CAS_ER_COMMUNICATION, NULL);
      return FN_CLOSE_CONN;
    }

  if (func_code <= 0 || func_code >= CAS_FC_MAX)
    {
      assert (false);		/* protocol crash */

      RYE_FREE_MEM (argv);
      RYE_FREE_MEM (read_msg);
      net_write_error (sock_fd, cas_Info_size,
		       CAS_ERROR_INDICATOR, CAS_ER_COMMUNICATION, NULL);
      return FN_CLOSE_CONN;
    }

  con_status_to_restore = -1;

  if (FUNC_NEEDS_RESTORING_CON_STATUS (func_code))
    {
      if (is_First_request == true)
	{
	  /* If this request is the first request after connection established,
	   * con_status should be CON_STATUS_OUT_TRAN.
	   */
	  con_status_to_restore = CON_STATUS_OUT_TRAN;
	}
      else if (con_Status_before_check_cas != -1)
	{
	  con_status_to_restore = con_Status_before_check_cas;
	}
      else
	{
	  con_status_to_restore = old_con_status;
	}

      con_Status_before_check_cas = -1;
    }
  else if (func_code == CAS_FC_CHECK_CAS)
    {
      con_Status_before_check_cas = old_con_status;
    }
  else
    {
      con_Status_before_check_cas = -1;
    }

  STRNCPY (as_Info->log_msg, server_Fn_table[func_code - 1].name,
	   sizeof (as_Info->log_msg));

  server_fn = server_Fn_table[func_code - 1].func;

#if defined (PROTOCOL_EXTENDS_DEBUG)	/* for debug cas <-> JDBC info */
  if (prev_Cas_info[CAS_STATUS_INFO_IDX_STATUS] !=
      CAS_STATUS_INFO_RESERVED_DEFAULT)
    {
      assert (prev_Cas_info[CAS_STATUS_INFO_IDX_STATUS] ==
	      client_msg_header.info_ptr[CAS_STATUS_INFO_IDX_STATUS]);
      if (prev_Cas_info[CAS_STATUS_INFO_IDX_STATUS] !=
	  client_msg_header.info_ptr[CAS_STATUS_INFO_IDX_STATUS])
	{
	  er_log_debug (ARG_FILE_LINE,
			"[%d][PREV : %d, RECV : %d], "
			"[preffunc : %d, recvfunc : %d], [REQ: %d, REQ: %d], "
			"[JID : %d] \n", func_code - 1,
			prev_Cas_info[CAS_STATUS_INFO_IDX_STATUS],
			client_msg_header.info_ptr
			[CAS_STATUS_INFO_IDX_STATUS],
			prev_Cas_info[CAS_INFO_RESERVED_1],
			client_msg_header.info_ptr[CAS_INFO_RESERVED_1],
			prev_Cas_info[CAS_INFO_RESERVED_2],
			client_msg_header.info_ptr[CAS_INFO_RESERVED_2],
			client_msg_header.info_ptr[CAS_INFO_RESERVED_3]);
	}
    }
#endif /* end for debug */

  req_info->need_auto_commit = TRAN_NOT_AUTOCOMMIT;
  cas_Send_result_flag = TRUE;

  net_buf_clear (net_buf);
  net_buf_cp_byte (net_buf, SUCCESS_RESPONSE);

  cas_sql_log_set_query_cancel_time (0);

  set_hang_check_time ();
  fn_ret = (*server_fn) (argc, argv, net_buf, req_info);
  set_hang_check_time ();

  er_log_debug (ARG_FILE_LINE, "process_request: %s() err_code %d",
		server_Fn_table[func_code - 1].name, err_Info.err_number);

  if (con_status_to_restore != -1)
    {
      CON_STATUS_LOCK (as_Info, CON_STATUS_LOCK_CAS);
      as_Info->con_status = con_status_to_restore;
      CON_STATUS_UNLOCK (as_Info, CON_STATUS_LOCK_CAS);
    }

  if (fn_ret == FN_KEEP_CONN && net_buf->err_code == 0
      && as_Info->con_status == CON_STATUS_IN_TRAN
      && req_info->need_auto_commit != TRAN_NOT_AUTOCOMMIT)
    {
      /* no communication error and auto commit is needed */
      err_code = ux_auto_commit (net_buf, req_info);
      if (err_code < 0)
	{
	  fn_ret = FN_CLOSE_CONN;
	}
      else
	{
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
	      fn_ret = FN_CLOSE_CONN;
	    }
	  else if (restart_is_needed ())
	    {
	      fn_ret = FN_KEEP_SESS;
	    }
	}
      as_Info->num_transactions_processed %= MAX_DIAG_DATA_VALUE;
      as_Info->num_transactions_processed++;

      /* should be OUT_TRAN in auto commit */
      CON_STATUS_LOCK (as_Info, CON_STATUS_LOCK_CAS);
      if (as_Info->con_status == CON_STATUS_IN_TRAN)
	{
	  as_Info->con_status = CON_STATUS_OUT_TRAN;
	}
      CON_STATUS_UNLOCK (as_Info, CON_STATUS_LOCK_CAS);
    }

  if ((func_code == CAS_FC_EXECUTE) || (func_code == CAS_FC_SCHEMA_INFO))
    {
      as_Info->num_requests_received %= MAX_DIAG_DATA_VALUE;
      as_Info->num_requests_received++;
    }
  else if (func_code == CAS_FC_END_TRAN)
    {
      as_Info->num_transactions_processed %= MAX_DIAG_DATA_VALUE;
      as_Info->num_transactions_processed++;
    }

  as_Info->log_msg[0] = '\0';
  if (as_Info->con_status == CON_STATUS_IN_TRAN)
    {
      cas_msg_header.info_ptr[CAS_STATUS_INFO_IDX_STATUS] = CAS_STATUS_ACTIVE;
    }
  else
    {
      cas_msg_header.info_ptr[CAS_STATUS_INFO_IDX_STATUS] =
	CAS_STATUS_INACTIVE;
    }

  if (net_buf->err_code)
    {
      net_write_error (sock_fd, cas_Info_size,
		       CAS_ERROR_INDICATOR, net_buf->err_code, NULL);
      fn_ret = FN_CLOSE_CONN;
      goto exit_on_end;
    }

  if (cas_Send_result_flag && net_buf->data != NULL)
    {
#if defined (PROTOCOL_EXTENDS_DEBUG)	/* for debug cas<->jdbc info */
      cas_msg_header.info_ptr[CAS_INFO_RESERVED_1] = func_code - 1;
      cas_msg_header.info_ptr[CAS_INFO_RESERVED_2] =
	as_Info->num_requests_received % 128;
      prev_Cas_info[CAS_STATUS_INFO_IDX_STATUS] =
	cas_msg_header.info_ptr[CAS_STATUS_INFO_IDX_STATUS];
      prev_Cas_info[CAS_INFO_RESERVED_1] =
	cas_msg_header.info_ptr[CAS_INFO_RESERVED_1];
      prev_Cas_info[CAS_INFO_RESERVED_2] =
	cas_msg_header.info_ptr[CAS_INFO_RESERVED_2];
#endif /* end for debug */

      *(cas_msg_header.msg_body_size_ptr) = htonl (net_buf->data_size);
      memcpy (net_buf->data, cas_msg_header.msg_body_size_ptr,
	      NET_BUF_HEADER_MSG_SIZE);

      if (cas_Info_size > 0)
	{
	  memcpy (net_buf->data + NET_BUF_HEADER_MSG_SIZE,
		  cas_msg_header.info_ptr, cas_Info_size);
	}

      assert (NET_BUF_CURR_SIZE (net_buf) <= net_buf->alloc_size);
      if (net_write_stream (sock_fd, net_buf->data,
			    NET_BUF_CURR_SIZE (net_buf)) < 0)
	{
	  cas_sql_log_write_and_end (0,
				     "COMMUNICATION ERROR net_write_stream()");
	}
    }

  if (as_Info->reset_flag
      && ((as_Info->con_status != CON_STATUS_IN_TRAN
	   && as_Info->num_holdable_results < 1
	   && as_Info->cas_change_mode == CAS_CHANGE_MODE_AUTO)
	  || (cas_get_db_connect_status () == -1)))
    {
      er_log_debug (ARG_FILE_LINE,
		    "process_request: reset_flag && !CON_STATUS_IN_TRAN");
      fn_ret = FN_KEEP_SESS;
      goto exit_on_end;
    }

exit_on_end:

  RYE_FREE_MEM (read_msg);
  RYE_FREE_MEM (argv);

  return fn_ret;
}

static int
cas_init ()
{
  char filename[BROKER_PATH_MAX];
  char err_log_file[BROKER_PATH_MAX];

  if (cas_init_shm () < 0)
    {
      return -1;
    }

  as_pid_file_create (shm_Appl->broker_name, as_Info->as_id);

  css_register_check_server_alive_fn (check_server_alive);
  css_register_check_client_alive_fn (check_client_alive);
  css_register_server_timeout_fn (set_hang_check_time);

  snprintf (filename, sizeof (filename),
	    "%s_%d.err", shm_Appl->broker_name, shm_As_index + 1);
  envvar_ryelog_broker_errorlog_file (err_log_file, sizeof (err_log_file),
				      shm_Appl->broker_name, filename);

  (void) er_init (err_log_file, prm_get_integer_value (PRM_ID_ER_EXIT_ASK));

  if (lang_init () != NO_ERROR)
    {
      return -1;
    }
  return 0;
}

static int
net_read_int_keep_con_auto (SOCKET clt_sock_fd,
			    MSG_HEADER * client_msg_header)
{
  int ret_value = 0;

  if (as_Info->con_status == CON_STATUS_IN_TRAN)
    {
      /* holdable results have the same lifespan of a normal session */
      net_timeout_set (shm_Appl->session_timeout);
    }
  else
    {
      net_timeout_set (DEFAULT_CHECK_INTERVAL);
      new_Req_sock_fd = srv_Sock_fd;
    }

  do
    {
      if (as_Info->cas_log_reset)
	{
	  cas_sql_log_reset ();
	}
      if (as_Info->cas_slow_log_reset)
	{
	  cas_slow_log_reset ();
	}

      if (as_Info->con_status != CON_STATUS_IN_TRAN
	  && as_Info->reset_flag == TRUE)
	{
	  return -1;
	}

      if (as_Info->con_status == CON_STATUS_CLOSE
	  || as_Info->con_status == CON_STATUS_CLOSE_AND_CONNECT)
	{
	  break;
	}

      if (net_read_header (clt_sock_fd, client_msg_header) < 0)
	{
	  /* if in-transaction state, return network error */
	  if (as_Info->con_status == CON_STATUS_IN_TRAN
	      || !is_net_timed_out ())
	    {
	      ret_value = -1;
	      break;
	    }
	  /* if out-of-transaction state, check whether restart is needed */
	  if (as_Info->con_status == CON_STATUS_OUT_TRAN
	      && is_net_timed_out ())
	    {
	      if (restart_is_needed ())
		{
		  er_log_debug (ARG_FILE_LINE,
				"net_read_int_keep_con_auto: "
				"restart_is_needed()");
		  ret_value = -1;
		  break;
		}

	      if (as_Info->reset_flag == TRUE)
		{
		  ret_value = -1;
		  break;
		}
	    }
	}
      else
	{
	  break;
	}
    }
  while (1);

  new_Req_sock_fd = INVALID_SOCKET;

  CON_STATUS_LOCK (&(shm_Appl->info.as_info[shm_As_index]),
		   CON_STATUS_LOCK_CAS);

  if (as_Info->con_status == CON_STATUS_OUT_TRAN)
    {
      as_Info->num_request++;
      gettimeofday (&tran_Start_time, NULL);
    }

  if (as_Info->con_status == CON_STATUS_CLOSE
      || as_Info->con_status == CON_STATUS_CLOSE_AND_CONNECT)
    {
      ret_value = -1;
    }
  else
    {
      if (as_Info->con_status != CON_STATUS_IN_TRAN)
	{
	  as_Info->con_status = CON_STATUS_IN_TRAN;
	  as_Info->transaction_start_time = time (0);
	  sql_log_Notice_mode_flush = false;
	}
    }

  CON_STATUS_UNLOCK (&(shm_Appl->info.as_info[shm_As_index]),
		     CON_STATUS_LOCK_CAS);

  return ret_value;
}

static int
net_read_header_keep_con_on (SOCKET clt_sock_fd,
			     MSG_HEADER * client_msg_header)
{
  int ret_value = 0;
  int timeout = 0, remained_timeout = 0;

  if (as_Info->con_status == CON_STATUS_IN_TRAN)
    {
      net_timeout_set (shm_Appl->session_timeout);
    }
  else
    {
      net_timeout_set (DEFAULT_CHECK_INTERVAL);
      timeout = shm_Appl->session_timeout;
      remained_timeout = timeout;
    }

  do
    {
      if (as_Info->con_status == CON_STATUS_OUT_TRAN)
	{
	  remained_timeout -= DEFAULT_CHECK_INTERVAL;
	}

      if (net_read_header (clt_sock_fd, client_msg_header) < 0)
	{
	  /* if in-transaction state, return network error */
	  if (as_Info->con_status == CON_STATUS_IN_TRAN
	      || !is_net_timed_out ())
	    {
	      ret_value = -1;
	      break;
	    }
	  /* if out-of-transaction state, check whether restart is needed */
	  if (as_Info->con_status == CON_STATUS_OUT_TRAN
	      && is_net_timed_out ())
	    {
	      if (as_Info->reset_flag == TRUE)
		{
		  ret_value = -1;
		  break;
		}

	      if (timeout > 0 && remained_timeout <= 0)
		{
		  ret_value = -1;
		  break;
		}
	    }
	}
      else
	{
	  break;
	}
    }
  while (1);

  return ret_value;
}

int
restart_is_needed (void)
{
  int max_process_size;

  if (as_Info->num_holdable_results > 0
      || as_Info->cas_change_mode == CAS_CHANGE_MODE_KEEP)
    {
      /* we do not want to restart the CAS when there are open
         holdable results or cas_change_mode is CAS_CHANGE_MODE_KEEP */
      return 0;
    }

  max_process_size = (shm_Appl->appl_server_max_size > 0) ?
    shm_Appl->appl_server_max_size : (psize_At_start * 2);

  if (as_Info->psize > max_process_size)
    {
      return 1;
    }
  else
    {
      return 0;
    }
}

static int
cas_init_shm (void)
{
  char *p;
  int as_shm_key;
  int as_id;

  p = getenv (APPL_SERVER_SHM_KEY_STR);
  if (p == NULL)
    {
      goto return_error;
    }

  parse_int (&as_shm_key, p, 10);
  BROKER_ERR ("<CAS> APPL_SERVER_SHM_KEY_STR:[%d:%x]\n", as_shm_key,
	      as_shm_key);
  shm_Appl = rye_shm_attach (as_shm_key, RYE_SHM_TYPE_BROKER_LOCAL, false);

  if (shm_Appl == NULL)
    {
      goto return_error;
    }

  shm_Br_master = rye_shm_attach (shm_Appl->shm_key_br_global,
				  RYE_SHM_TYPE_BROKER_GLOBAL, false);
  if (shm_Br_master == NULL)
    {
      goto return_error;
    }

  p = getenv (AS_ID_ENV_STR);
  if (p == NULL)
    {
      goto return_error;
    }

  parse_int (&as_id, p, 10);
  BROKER_ERR ("<CAS> AS_ID_ENV_STR:[%d]\n", as_id);
  as_Info = &shm_Appl->info.as_info[as_id];

  shm_As_index = as_id;

  cas_log_init (shm_Appl, as_Info, as_id + 1);

  return 0;

return_error:

  if (shm_Appl)
    {
      rye_shm_detach (shm_Appl);
      shm_Appl = NULL;
    }

  if (shm_Br_master)
    {
      rye_shm_detach (shm_Br_master);
      shm_Br_master = NULL;
    }

  return -1;
}

int
query_seq_num_next_value (void)
{
  return ++query_Sequence_num;
}

int
query_seq_num_current_value (void)
{
  return query_Sequence_num;
}

int64_t
cas_shard_info_version ()
{
  int64_t shard_info_version = 0;

  if (shm_Br_master == NULL)
    {
      return shard_info_version;
    }

  if (idx_Shm_shard_info < 0)
    {
      int i;
      char local_dbname[MAX_HA_DBINFO_LENGTH];

      ux_get_current_database_name (local_dbname, sizeof (local_dbname));

      for (i = 0; i < shm_Br_master->num_shard_version_info; i++)
	{
	  if (strcmp (shm_Br_master->shard_version_info[i].local_dbname,
		      local_dbname) == 0)
	    {
	      idx_Shm_shard_info = i;
	    }
	}
    }

  if (idx_Shm_shard_info >= 0)
    {
      shard_info_version =
	shm_Br_master->shard_version_info[idx_Shm_shard_info].shard_info_ver;
    }

  return shard_info_version;
}
