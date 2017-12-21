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
 * master_request.c - master request handling module
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <signal.h>
#include <assert.h>
#include <sys/time.h>
#include <sys/param.h>
#include <netinet/in.h>
#include <pthread.h>

#include "system_parameter.h"
#include "connection_globals.h"
#include "connection_cl.h"
#include "error_manager.h"
#include "utility.h"
#include "message_catalog.h"
#include "memory_alloc.h"
#include "porting.h"
#include "release_string.h"
#include "tcp.h"
#include "master_util.h"
#include "master_request.h"
#include "client_support.h"
#include "master_heartbeat.h"
#include "object_representation.h"
#include "rye_server_shm.h"

#define IS_MASTER_SOCKET_FD(FD)		((FD) == css_Master_socket_fd)

#define HA_SERVER_FORMAT_STRING " HA-Server %s (rel %s, pid %d, nodeid %d)\n"
#define HA_REPL_FORMAT_STRING " HA-Repl %s (rel %s, pid %d, nodeid %d)\n"

typedef int (*css_master_func) (CSS_CONN_ENTRY * conn,
				unsigned short rid,
				char *buffer, int buf_size);

struct css_master_request
{
  css_master_func processing_function;
  const char *name;
  INT64 request_count;
};

static struct css_master_request css_Master_requests[MASTER_REQUEST_END];

static int css_master_request (CSS_CONN_ENTRY * conn,
			       CSS_MASTER_REQUEST request,
			       unsigned short rid,
			       char *buffer, int buf_size);

static int css_process_start_time_info (CSS_CONN_ENTRY * conn,
					unsigned short rid,
					char *buffer, int buf_size);
static int css_process_server_count_info (CSS_CONN_ENTRY * conn,
					  unsigned short rid,
					  char *buffer, int buf_size);
static int css_process_request_count_info (CSS_CONN_ENTRY * conn,
					   unsigned short rid,
					   char *buffer, int buf_size);
static int css_process_server_list_info (CSS_CONN_ENTRY * conn,
					 unsigned short rid,
					 char *buffer, int buf_size);
static int css_process_ha_ping_host_info (CSS_CONN_ENTRY * conn,
					  unsigned short rid,
					  char *buffer, int buf_size);
static int css_process_ha_node_list_info (CSS_CONN_ENTRY * conn,
					  unsigned short rid,
					  char *buffer, int buf_size);
static int css_process_ha_process_list_info (CSS_CONN_ENTRY * conn,
					     unsigned short rid,
					     char *buffer, int buf_size);
static int css_process_ha_admin_info (CSS_CONN_ENTRY * conn,
				      unsigned short rid,
				      char *buffer, int buf_size);
static int css_process_is_registered_ha_procs (CSS_CONN_ENTRY * conn,
					       unsigned short rid,
					       char *buffer, int buf_size);
static int css_process_server_state (CSS_CONN_ENTRY * conn,
				     unsigned short rid,
				     char *buffer, int buf_size);

static int css_process_shutdown (CSS_CONN_ENTRY * conn,
				 unsigned short rid,
				 char *buffer, int buf_size);

static int css_process_activate_heartbeat (CSS_CONN_ENTRY * conn,
					   unsigned short rid,
					   char *buffer, int buf_size);
static int css_process_register_ha_process (CSS_CONN_ENTRY * conn,
					    unsigned short rid,
					    char *buffer, int buf_size);
static int css_process_deact_stop_all (CSS_CONN_ENTRY * conn,
				       unsigned short rid,
				       char *buffer, int buf_size);
static int css_process_deact_confirm_stop_all (CSS_CONN_ENTRY * conn,
					       unsigned short rid,
					       char *buffer, int buf_size);
static int css_process_deactivate_heartbeat (CSS_CONN_ENTRY * conn,
					     unsigned short rid,
					     char *buffer, int buf_size);
static int css_process_deact_confirm_no_server (CSS_CONN_ENTRY * conn,
						unsigned short rid,
						char *buffer, int buf_size);
static int css_process_reconfig_heartbeat (CSS_CONN_ENTRY * conn,
					   unsigned short rid,
					   char *buffer, int buf_size);
static int css_process_changemode (CSS_CONN_ENTRY * conn,
				   unsigned short rid,
				   char *buffer, int buf_size);

static int css_process_change_ha_mode (CSS_CONN_ENTRY * conn,
				       unsigned short rid,
				       char *buffer, int buf_size);

static void css_process_kill_master (void);


/*
 * css_master_requests_init ()
 *    return:
 */
void
css_master_requests_init (void)
{
  struct css_master_request *req_p;
  unsigned int i;

  for (i = 0; i < DIM (css_Master_requests); i++)
    {
      css_Master_requests[i].processing_function = NULL;
      css_Master_requests[i].name = NULL;
      css_Master_requests[i].request_count = 0;
    }

  /* MASTER_GET_START_TIME */
  req_p = &css_Master_requests[MASTER_GET_START_TIME];
  req_p->processing_function = css_process_start_time_info;
  req_p->name = "MASTER_GET_START_TIME";

  /* MASTER_GET_SERVER_COUNT */
  req_p = &css_Master_requests[MASTER_GET_SERVER_COUNT];
  req_p->processing_function = css_process_server_count_info;
  req_p->name = "MASTER_GET_SERVER_COUNT";

  /* MASTER_GET_REQUEST_COUNT */
  req_p = &css_Master_requests[MASTER_GET_REQUEST_COUNT];
  req_p->processing_function = css_process_request_count_info;
  req_p->name = "MASTER_GET_REQUEST_COUNT";

  /* MASTER_GET_SERVER_LIST */
  req_p = &css_Master_requests[MASTER_GET_SERVER_LIST];
  req_p->processing_function = css_process_server_list_info;
  req_p->name = "MASTER_GET_SERVER_LIST";

  /* MASTER_GET_HA_PING_HOST_INFO */
  req_p = &css_Master_requests[MASTER_GET_HA_PING_HOST_INFO];
  req_p->processing_function = css_process_ha_ping_host_info;
  req_p->name = "MASTER_GET_HA_PING_HOST_INFO";

  /* MASTER_GET_HA_NODE_LIST */
  req_p = &css_Master_requests[MASTER_GET_HA_NODE_LIST];
  req_p->processing_function = css_process_ha_node_list_info;
  req_p->name = "MASTER_GET_HA_NODE_LIST";

  /* MASTER_GET_HA_PROCESS_LIST */
  req_p = &css_Master_requests[MASTER_GET_HA_PROCESS_LIST];
  req_p->processing_function = css_process_ha_process_list_info;
  req_p->name = "MASTER_GET_HA_PROCESS_LIST";

  /* MASTER_GET_HA_ADMIN_INFO */
  req_p = &css_Master_requests[MASTER_GET_HA_ADMIN_INFO];
  req_p->processing_function = css_process_ha_admin_info;
  req_p->name = "MASTER_GET_HA_ADMIN_INFO";

  /* MASTER_IS_REGISTERED_HA_PROCS */
  req_p = &css_Master_requests[MASTER_IS_REGISTERED_HA_PROCS];
  req_p->processing_function = css_process_is_registered_ha_procs;
  req_p->name = "MASTER_IS_REGISTERED_HA_PROCS";

  /* MASTER_GET_SERVER_STATE */
  req_p = &css_Master_requests[MASTER_GET_SERVER_STATE];
  req_p->processing_function = css_process_server_state;
  req_p->name = "MASTER_GET_SERVER_STATE";

  /* action functions */


  /* MASTER_START_SHUTDOWN */
  req_p = &css_Master_requests[MASTER_START_SHUTDOWN];
  req_p->processing_function = css_process_shutdown;
  req_p->name = "MASTER_START_SHUTDOWN";

  /* MASTER_ACTIVATE_HEARTBEAT */
  req_p = &css_Master_requests[MASTER_ACTIVATE_HEARTBEAT];
  req_p->processing_function = css_process_activate_heartbeat;
  req_p->name = "MASTER_ACTIVATE_HEARTBEAT";

  /* MASTER_REGISTER_HA_PROCESS */
  req_p = &css_Master_requests[MASTER_REGISTER_HA_PROCESS];
  req_p->processing_function = css_process_register_ha_process;
  req_p->name = "MASTER_REGISTER_HA_PROCESS";

  /* MASTER_DEACT_STOP_ALL */
  req_p = &css_Master_requests[MASTER_DEACT_STOP_ALL];
  req_p->processing_function = css_process_deact_stop_all;
  req_p->name = "MASTER_DEACT_STOP_ALL";

  /* MASTER_DEACT_CONFIRM_STOP_ALL */
  req_p = &css_Master_requests[MASTER_DEACT_CONFIRM_STOP_ALL];
  req_p->processing_function = css_process_deact_confirm_stop_all;
  req_p->name = "MASTER_DEACT_CONFIRM_STOP_ALL";

  /* MASTER_DEACTIVATE_HEARTBEAT */
  req_p = &css_Master_requests[MASTER_DEACTIVATE_HEARTBEAT];
  req_p->processing_function = css_process_deactivate_heartbeat;
  req_p->name = "MASTER_DEACTIVATE_HEARTBEAT";

  /* MASTER_DEACT_CONFIRM_NO_SERVER */
  req_p = &css_Master_requests[MASTER_DEACT_CONFIRM_NO_SERVER];
  req_p->processing_function = css_process_deact_confirm_no_server;
  req_p->name = "MASTER_DEACT_CONFIRM_NO_SERVER";

  /* MASTER_RECONFIG_HEARTBEAT */
  req_p = &css_Master_requests[MASTER_RECONFIG_HEARTBEAT];
  req_p->processing_function = css_process_reconfig_heartbeat;
  req_p->name = "MASTER_RECONFIG_HEARTBEAT";

  /* MASTER_CHANGEMODE */
  req_p = &css_Master_requests[MASTER_CHANGEMODE];
  req_p->processing_function = css_process_changemode;
  req_p->name = "MASTER_CHANGEMODE";


  /* from rye_server */

  /* MASTER_CHANGE_SERVER_STATE */
  req_p = &css_Master_requests[MASTER_CHANGE_SERVER_STATE];
  req_p->processing_function = css_process_change_ha_mode;
  req_p->name = "MASTER_CHANGE_SERVER_STATE";
}

/*
 * css_process_start_time_info()
 *   return: enum css_error_code (See connectino_defs.h)
 *   conn(in)
 *   recv_packet(in)
 */
static int
css_process_start_time_info (CSS_CONN_ENTRY * conn, unsigned short rid,
			     char *buffer, int buf_size)
{
  char *reply;
  UNUSED_VAR char *ptr;
  int reply_size;
  char *my_time;
  int strlen;
  int css_error = NO_ERRORS;

  if (buffer != NULL || buf_size > 0)
    {
      assert (false);
      return ERROR_WHEN_READING_SIZE;
    }

  my_time = ctime (&css_Start_time);

  reply_size = 0;
  reply_size += or_packed_string_length (my_time, &strlen);

  reply = (char *) malloc (reply_size);
  if (reply == NULL)
    {
      return CANT_ALLOC_BUFFER;
    }

  ptr = or_pack_string_with_length (reply, my_time, strlen);

  css_error = css_send_data_packet (conn, rid, 1, reply, reply_size);

  free_and_init (reply);

  return css_error;
}

/*
 * css_process_server_count_info()
 *   return: enum css_error_code (See connectino_defs.h)
 *   conn(in)
 *   recv_packet(in)
 */
static int
css_process_server_count_info (CSS_CONN_ENTRY * conn, unsigned short rid,
			       char *buffer, int buf_size)
{
  int count = 0;
  SOCKET_QUEUE_ENTRY *temp;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  if (buffer != NULL || buf_size > 0)
    {
      assert (false);
      return ERROR_WHEN_READING_SIZE;
    }

  for (temp = css_Master_socket_anchor; temp; temp = temp->next)
    {
      if (!IS_INVALID_SOCKET (temp->fd) && !IS_MASTER_SOCKET_FD (temp->fd) &&
	  temp->name &&
	  !IS_MASTER_CONN_NAME_DRIVER (temp->name)
	  && !IS_MASTER_CONN_NAME_HA_REPL (temp->name))
	{
	  count++;
	}
    }

  or_pack_int (reply, count);
  return css_send_data_packet (conn, rid, 1, reply,
			       OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * css_process_request_count_info()
 *   return: enum css_error_code (See connectino_defs.h)
 *   conn(in)
 *   recv_packet(in)
 */
static int
css_process_request_count_info (CSS_CONN_ENTRY * conn, unsigned short rid,
				char *buffer, int buf_size)
{
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  if (buffer != NULL || buf_size > 0)
    {
      assert (false);
      return ERROR_WHEN_READING_SIZE;
    }

  or_pack_int (reply, css_Total_request_count);
  return css_send_data_packet (conn, rid, 1, reply,
			       OR_ALIGNED_BUF_SIZE (a_reply));
}

static int
css_get_server_list_info (RYE_STRING * buffer)
{
  SOCKET_QUEUE_ENTRY *temp;

  for (temp = css_Master_socket_anchor; temp; temp = temp->next)
    {
      if (IS_INVALID_SOCKET (temp->fd) || IS_MASTER_SOCKET_FD (temp->fd)
	  || temp->name == NULL
	  || IS_MASTER_CONN_NAME_DRIVER (temp->name)
	  || IS_MASTER_CONN_NAME_HA_REPL (temp->name))
	{
	  continue;
	}
      /* if HA mode server */
      if (IS_MASTER_CONN_NAME_HA_SERVER (temp->name))
	{
	  short nodeid = 0;
	  char ver_string[REL_MAX_VERSION_LENGTH];

	  if (temp->conn_ptr == NULL)
	    {
	      strcpy (ver_string, "?");
	    }
	  else
	    {
	      rel_version_to_string (&temp->conn_ptr->peer_version,
				     ver_string, sizeof (ver_string));
	    }

	  rye_server_shm_get_nodeid (&nodeid, temp->name + 1);
	  rye_append_format_string (buffer, HA_SERVER_FORMAT_STRING,
				    temp->name + 1, ver_string,
				    temp->pid, nodeid);
	}
      else
	{
	  assert (false);
	  rye_append_format_string (buffer, "ERROR");
	}
    }

  return NO_ERROR;
}

/*
 * css_process_server_list_info()
 *   return: enum css_error_code (See connectino_defs.h)
 *   conn(in)
 *   recv_packet(in)
 */
static int
css_process_server_list_info (CSS_CONN_ENTRY * conn, unsigned short rid,
			      char *buffer, int buf_size)
{
  int css_error = NO_ERRORS, error = NO_ERROR;
  RYE_STRING info;
  char *reply;
  int reply_size, strlen1;

  if (buffer != NULL || buf_size > 0)
    {
      assert (false);
      return ERROR_WHEN_READING_SIZE;
    }

  error = rye_init_string (&info, 4 * ONE_K);
  if (error != NO_ERROR)
    {
      return CANT_ALLOC_BUFFER;
    }
  css_get_server_list_info (&info);


  reply_size = 0;
  reply_size += or_packed_string_length (info.buffer, &strlen1);

  reply = (char *) malloc (reply_size);
  if (reply == NULL)
    {
      return CANT_ALLOC_BUFFER;
    }

  or_pack_string_with_length (reply, info.buffer, strlen1);

  css_error = css_send_data_packet (conn, rid, 1, reply, reply_size);

  rye_free_string (&info);
  free_and_init (reply);

  return css_error;
}

/*
 * css_process_ha_ping_hosts_info()
 *   return: css error
 *
 *   conn(in)
 *   recv_packet(in)
 */
static int
css_process_ha_ping_host_info (CSS_CONN_ENTRY * conn, unsigned short rid,
			       char *buffer, int buf_size)
{
  char *ping_host_info = NULL;
  int css_error = NO_ERRORS;
  char *reply;
  int reply_size, strlen1;

  if (buffer != NULL || buf_size > 0)
    {
      assert (false);
      return ERROR_WHEN_READING_SIZE;
    }

  ping_host_info = hb_get_ping_host_info_string ();
  if (ping_host_info == NULL)
    {
      return CANT_ALLOC_BUFFER;
    }

  reply_size = 0;
  reply_size += or_packed_string_length (ping_host_info, &strlen1);

  reply = (char *) malloc (reply_size);
  if (reply == NULL)
    {
      return CANT_ALLOC_BUFFER;
    }

  or_pack_string_with_length (reply, ping_host_info, strlen1);

  css_error = css_send_data_packet (conn, rid, 1, reply, reply_size);

  free_and_init (ping_host_info);
  free_and_init (reply);

  return css_error;
}

/*
 * css_process_ha_node_list_info()
 *   return: css error
 *
 *   conn(in)
 *   recv_packet(in)
 */
static int
css_process_ha_node_list_info (CSS_CONN_ENTRY * conn, unsigned short rid,
			       char *buffer, int buf_size)
{
  char *node_info = NULL;
  int css_error = NO_ERRORS;
  char *reply;
  int reply_size, strlen1;
  int verbose_yn;

  if (buffer == NULL || buf_size < (int) sizeof (bool))
    {
      assert (false);
      return ERROR_WHEN_READING_SIZE;
    }

  or_unpack_int (buffer, &verbose_yn);

  node_info = hb_get_node_info_string ((bool) verbose_yn);
  if (node_info == NULL)
    {
      return CANT_ALLOC_BUFFER;
    }

  reply_size = 0;
  reply_size += or_packed_string_length (node_info, &strlen1);

  reply = (char *) malloc (reply_size);
  if (reply == NULL)
    {
      return CANT_ALLOC_BUFFER;
    }

  or_pack_string_with_length (reply, node_info, strlen1);

  css_error = css_send_data_packet (conn, rid, 1, reply, reply_size);

  free_and_init (node_info);
  free_and_init (reply);

  return css_error;
}

/*
 * css_process_ha_process_list_info()
 *   return: css error
 *
 *   conn(in)
 *   recv_packet(in)
 */
static int
css_process_ha_process_list_info (CSS_CONN_ENTRY * conn, unsigned short rid,
				  char *buffer, int buf_size)
{
  char *process_info = NULL;
  int css_error = NO_ERRORS;
  char *reply;
  int reply_size, strlen1;
  int verbose_yn;

  if (buffer == NULL || buf_size < (int) sizeof (bool))
    {
      assert (false);
      return ERROR_WHEN_READING_SIZE;
    }

  or_unpack_int (buffer, &verbose_yn);

  process_info = hb_get_process_info_string ((bool) verbose_yn);
  if (process_info == NULL)
    {
      return CANT_ALLOC_BUFFER;
    }

  reply_size = 0;
  reply_size += or_packed_string_length (process_info, &strlen1);

  reply = (char *) malloc (reply_size);
  if (reply == NULL)
    {
      return CANT_ALLOC_BUFFER;
    }

  or_pack_string_with_length (reply, process_info, strlen1);

  css_error = css_send_data_packet (conn, rid, 1, reply, reply_size);

  free_and_init (process_info);
  free_and_init (reply);

  return css_error;
}

/*
 * css_process_ha_admin_info()
 *   return: css error
 *
 *   conn(in)
 *   recv_packet(in)
 */
static int
css_process_ha_admin_info (CSS_CONN_ENTRY * conn, unsigned short rid,
			   char *buffer, int buf_size)
{
  char *admin_info = NULL;
  int css_error = NO_ERRORS;
  char *reply;
  int reply_size, strlen1;

  if (buffer != NULL || buf_size > 0)
    {
      assert (false);
      return ERROR_WHEN_READING_SIZE;
    }

  admin_info = hb_get_admin_info_string ();
  if (admin_info == NULL)
    {
      return CANT_ALLOC_BUFFER;
    }

  reply_size = 0;
  reply_size += or_packed_string_length (admin_info, &strlen1);

  reply = (char *) malloc (reply_size);
  if (reply == NULL)
    {
      free_and_init (admin_info);
      return CANT_ALLOC_BUFFER;
    }

  or_pack_string_with_length (reply, admin_info, strlen1);

  css_error = css_send_data_packet (conn, rid, 1, reply, reply_size);

  free_and_init (admin_info);
  free_and_init (reply);

  return css_error;
}

/*
 * css_process_is_registered_ha_procs()
 *   return: css error
 *
 *   conn(in)
 *   recv_packet(in)
 */
static int
css_process_is_registered_ha_procs (CSS_CONN_ENTRY * conn, unsigned short rid,
				    char *buffer, int buf_size)
{
  int css_error = NO_ERRORS;
  const char *result = NULL;
  char *reply;
  int reply_size, strlen1;

  if (buffer != NULL || buf_size > 0)
    {
      assert (false);
      return ERROR_WHEN_READING_SIZE;
    }

  if (hb_is_registered_processes (conn))
    {
      result = HA_REQUEST_SUCCESS;
    }
  else
    {
      result = HA_REQUEST_FAILURE;
    }

  reply_size = 0;
  reply_size += or_packed_string_length (result, &strlen1);

  reply = (char *) malloc (reply_size);
  if (reply == NULL)
    {
      return CANT_ALLOC_BUFFER;
    }

  or_pack_string_with_length (reply, result, strlen1);

  css_error = css_send_data_packet (conn, rid, 1, reply, reply_size);

  free_and_init (reply);

  return css_error;
}

/*
 * css_process_server_state()
 *   return: css error
 *
 *   conn(in)
 *   recv_packet(in)
 */
static int
css_process_server_state (CSS_CONN_ENTRY * conn, unsigned short rid,
			  char *buffer, int buf_size)
{
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  int server_state = 0;
  SOCKET_QUEUE_ENTRY *temp;
  const char *server_name;

  if (buffer == NULL || buf_size <= 0)
    {
      assert (false);
      return ERROR_WHEN_READING_SIZE;
    }

  or_unpack_string_nocopy (buffer, &server_name);

  temp = css_return_entry_of_server (server_name, css_Master_socket_anchor);
  if (temp == NULL || IS_INVALID_SOCKET (temp->fd))
    {
      server_state = HA_STATE_DEAD;
      goto send_to_client;
    }

  server_state = hb_return_proc_state_by_fd (temp->fd);

send_to_client:

  or_pack_int (reply, server_state);
  return css_send_data_packet (conn, rid, 1, reply,
			       OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * css_process_shutdown()
 *   return: css error
 *
 *   conn(in)
 *   recv_packet(in)
 */
static int
css_process_shutdown (UNUSED_ARG CSS_CONN_ENTRY * conn,
		      UNUSED_ARG unsigned short rid,
		      char *buffer, int buf_size)
{
  int timeout;
  SOCKET_QUEUE_ENTRY *temp;
  char reply_buffer[MASTER_TO_SRV_MSG_SIZE];

  if (buffer == NULL || buf_size < (int) sizeof (int))
    {
      assert (false);
      return ERROR_WHEN_READING_SIZE;
    }

  or_unpack_int (buffer, &timeout);

  snprintf (reply_buffer, MASTER_TO_SRV_MSG_SIZE,
	    msgcat_message (MSGCAT_CATALOG_UTILS, MSGCAT_UTIL_SET_MASTER,
			    MASTER_MSG_GOING_DOWN), timeout);
  reply_buffer[sizeof (reply_buffer) - 1] = '\0';

  for (temp = css_Master_socket_anchor; temp; temp = temp->next)
    {
      /* do not send shutdown command to master and connector, only to servers:
       * cause connector crash
       */
      if (!IS_INVALID_SOCKET (temp->fd) && !IS_MASTER_SOCKET_FD (temp->fd)
	  && temp->name
	  && !IS_MASTER_CONN_NAME_DRIVER (temp->name)
	  && !IS_MASTER_CONN_NAME_HA_SERVER (temp->name)
	  && !IS_MASTER_CONN_NAME_HA_REPL (temp->name))
	{
	  assert (false);

	  /* Normal rye_server no longer exists. */
	  kill (temp->pid, SIGKILL);
	}
    }

  if (css_Master_timeout == NULL)
    {
      css_Master_timeout =
	(struct timeval *) malloc (sizeof (struct timeval));
    }

  /* check again to be sure allocation was successful */
  if (css_Master_timeout)
    {
      css_Master_timeout->tv_sec = 0;
      css_Master_timeout->tv_usec = 0;

      if (time ((time_t *) & css_Master_timeout->tv_sec) == (time_t) (-1))
	{
	  free_and_init (css_Master_timeout);
	}
      else
	{
	  css_Master_timeout->tv_sec += timeout * 60;
	}
    }

  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
	  ERR_CSS_MINFO_MESSAGE, 1, reply_buffer);

  css_process_kill_master ();

  return NO_ERRORS;
}

/*
 * css_process_activate_heartbeat()
 *   return: css error
 *
 *   conn(in)
 *   recv_packet(in)
 */
static int
css_process_activate_heartbeat (CSS_CONN_ENTRY * conn, unsigned short rid,
				char *buffer, int buf_size)
{
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  int error = NO_ERROR;
  int result;
  char error_string[LINE_MAX];
  HA_CONF ha_conf;

  er_log_debug (ARG_FILE_LINE, "hb_activate_heartbeat!!");

  if (buffer != NULL || buf_size > 0)
    {
      assert (false);
      return ERROR_WHEN_READING_SIZE;
    }

  error = hb_check_request_eligibility (conn->fd, &result);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  assert (result == HB_HC_ELIGIBLE_LOCAL);

  error = hb_activate_heartbeat ();
  if (error != NO_ERROR)
    {
      snprintf (error_string, LINE_MAX, "%s.", HB_RESULT_FAILURE_STR);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_HB_COMMAND_EXECUTION, 2, HB_CMD_ACTIVATE_STR, error_string);

      GOTO_EXIT_ON_ERROR;
    }

  memset (&ha_conf, 0, sizeof (HA_CONF));
  error = util_make_ha_conf (&ha_conf);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = hb_process_start (&ha_conf);
  if (error != NO_ERROR)
    {
      snprintf (error_string, LINE_MAX, "%s.", HB_RESULT_FAILURE_STR);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_HB_COMMAND_EXECUTION, 2, HB_CMD_ACTIVATE_STR, error_string);

      util_free_ha_conf (&ha_conf);
      GOTO_EXIT_ON_ERROR;
    }
  util_free_ha_conf (&ha_conf);

  snprintf (error_string, LINE_MAX, "%s.", HB_RESULT_SUCCESS_STR);
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	  ER_HB_COMMAND_EXECUTION, 2, HB_CMD_ACTIVATE_STR, error_string);

  or_pack_int (reply, NO_ERROR);
  return css_send_data_packet (conn, rid, 1, reply,
			       OR_ALIGNED_BUF_SIZE (a_reply));

exit_on_error:
  assert (error != NO_ERROR);

  or_pack_int (reply, error);
  return css_send_data_packet (conn, rid, 1, reply,
			       OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * css_process_register_ha_process()
 *   return: css error
 *
 *   conn(in)
 *   recv_packet(in)
 */
static int
css_process_register_ha_process (CSS_CONN_ENTRY * conn,
				 UNUSED_ARG unsigned short rid,
				 char *buffer, int buf_size)
{
  HBP_PROC_REGISTER *proc_reg;
  int error = NO_ERROR;

  if (buffer == NULL || buf_size < (int) sizeof (HBP_PROC_REGISTER))
    {
      assert (false);
      return ERROR_WHEN_READING_SIZE;
    }

  proc_reg = (HBP_PROC_REGISTER *) buffer;
  error = hb_resource_register_new_proc (proc_reg, conn);
  if (error != NO_ERROR)
    {
      return SERVER_ABORTED;
    }

  return NO_ERRORS;
}

/*
 * css_process_deact_stop_all()
 *   return: css error
 *
 *   conn(in)
 *   recv_packet(in)
 */

static int
css_process_deact_stop_all (CSS_CONN_ENTRY * conn, unsigned short rid,
			    char *buffer, int buf_size)
{
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  char request_from[MAXHOSTNAMELEN] = "";
  int result;
  int error = NO_ERROR;
  int deact_immediately;

  if (buffer == NULL || buf_size <= 0)
    {
      assert (false);
      return ERROR_WHEN_READING_SIZE;
    }

  or_unpack_int (buffer, &deact_immediately);

  error = hb_check_request_eligibility (conn->fd, &result);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (result == HB_HC_ELIGIBLE_REMOTE)
    {
      if (css_get_peer_name (conn->fd, request_from,
			     sizeof (request_from)) != 0)
	{
	  snprintf (request_from, sizeof (request_from), "UNKNOWN");
	}

      hb_disable_er_log (HB_NOLOG_REMOTE_STOP, "deactivation request from %s",
			 request_from);
    }

  if (hb_is_deactivation_started () == false)
    {
      hb_start_deactivate_server_info ();

      if ((bool) deact_immediately == true)
	{
	  hb_Deactivate_immediately = true;
	}
      else
	{
	  hb_Deactivate_immediately = false;
	}

      error = hb_prepare_deactivate_heartbeat ();
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  or_pack_int (reply, NO_ERROR);
  return css_send_data_packet (conn, rid, 1, reply,
			       OR_ALIGNED_BUF_SIZE (a_reply));

exit_on_error:
  assert (error != NO_ERROR);

  or_pack_int (reply, error);
  return css_send_data_packet (conn, rid, 1, reply,
			       OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * css_process_deact_confirm_stop_all()
 *   return: css error
 *
 *   conn(in)
 *   recv_packet(in)
 */
static int
css_process_deact_confirm_stop_all (CSS_CONN_ENTRY * conn, unsigned short rid,
				    char *buffer, int buf_size)
{
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);

  if (buffer != NULL || buf_size > 0)
    {
      assert (false);
      return ERROR_WHEN_READING_SIZE;
    }

  or_pack_int (reply, (int) hb_is_deactivation_ready ());

  return css_send_data_packet (conn, rid, 1, reply,
			       OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * css_process_deactivate_heartbeat()
 *   return: css error
 *
 *   conn(in)
 *   recv_packet(in)
 */
static int
css_process_deactivate_heartbeat (CSS_CONN_ENTRY * conn, unsigned short rid,
				  char *buffer, int buf_size)
{
  char *reply;
  int reply_size, strlen1;
  int error = NO_ERROR, css_error = NO_ERRORS;
  int result;
  const char *message;
  char *ptr;
  char request_from[MAXHOSTNAMELEN] = "";

  if (buffer != NULL || buf_size > 0)
    {
      assert (false);
      return ERROR_WHEN_READING_SIZE;
    }

  er_log_debug (ARG_FILE_LINE, "hb_deactivate_heartbeat!!");

  error = hb_check_request_eligibility (conn->fd, &result);
  if (error != NO_ERROR)
    {
      goto send_to_client;
    }

  if (result == HB_HC_ELIGIBLE_REMOTE)
    {
      if (css_get_peer_name (conn->fd, request_from,
			     sizeof (request_from)) != 0)
	{
	  snprintf (request_from, sizeof (request_from), "UNKNOWN");
	}

      hb_disable_er_log (HB_NOLOG_REMOTE_STOP, "deactivation request from %s",
			 request_from);
    }

  error = hb_deactivate_heartbeat ();
  if (error != NO_ERROR)
    {
      goto send_to_client;
    }

  if (hb_get_deactivating_server_count () > 0)
    {
      message = msgcat_message (MSGCAT_CATALOG_UTILS,
				MSGCAT_UTIL_SET_MASTER,
				MASTER_MSG_FAILOVER_FINISHED);

    }
  else
    {
      message = HA_EMPTY_BUFFER;
    }

send_to_client:
  if (error != NO_ERROR)
    {
      message = HA_EMPTY_BUFFER;
    }

  reply_size = 0;
  reply_size += OR_INT_SIZE;
  reply_size += or_packed_string_length (message, &strlen1);

  reply = (char *) malloc (reply_size);
  if (reply == NULL)
    {
      return CANT_ALLOC_BUFFER;
    }
  ptr = or_pack_int (reply, error);
  ptr = or_pack_string_with_length (ptr, message, strlen1);

  css_error = css_send_data_packet (conn, rid, 1, reply, reply_size);

  free_and_init (reply);

  return css_error;
}

/*
 * css_process_deact_confirm_no_server()
 *   return: css error
 *
 *   conn(in)
 *   recv_packet(in)
 */
static int
css_process_deact_confirm_no_server (CSS_CONN_ENTRY * conn,
				     unsigned short rid, char *buffer,
				     int buf_size)
{
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  bool result;

  if (buffer != NULL || buf_size > 0)
    {
      assert (false);
      return ERROR_WHEN_READING_SIZE;
    }

  if (hb_get_deactivating_server_count () == 0)
    {
      result = true;

      hb_finish_deactivate_server_info ();
    }
  else
    {
      result = false;
    }

  or_pack_int (reply, (int) result);

  return css_send_data_packet (conn, rid, 1, reply,
			       OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * css_process_reconfig_heartbeat()
 *   return: css error
 *
 *   conn(in)
 *   recv_packet(in)
 */
static int
css_process_reconfig_heartbeat (CSS_CONN_ENTRY * conn, unsigned short rid,
				char *buffer, int buf_size)
{
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  int error = NO_ERROR, css_error = NO_ERRORS;
  int result;

  er_log_debug (ARG_FILE_LINE, "hb_reconfig_heartbeat!!");
  if (buffer != NULL || buf_size > 0)
    {
      assert (false);
      return ERROR_WHEN_READING_SIZE;
    }

  error = hb_check_request_eligibility (conn->fd, &result);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  assert (result == HB_HC_ELIGIBLE_LOCAL);

  error = hb_reconfig_heartbeat ();
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  or_pack_int (reply, NO_ERROR);
  return css_send_data_packet (conn, rid, 1, reply,
			       OR_ALIGNED_BUF_SIZE (a_reply));

  return css_error;

exit_on_error:
  assert (error != NO_ERROR);

  or_pack_int (reply, error);
  return css_send_data_packet (conn, rid, 1, reply,
			       OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * css_process_changemode()
 *   return: css error
 *
 *   conn(in)
 *   recv_packet(in)
 */
static int
css_process_changemode (CSS_CONN_ENTRY * conn, unsigned short rid,
			char *buffer, int buf_size)
{
  OR_ALIGNED_BUF (OR_INT_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  char *ptr;
  int error = NO_ERROR;
  int tmp_int;
  HA_STATE req_node_state;
  bool force;

  if (buffer == NULL || buf_size <= 0)
    {
      assert (false);
      return ERROR_WHEN_READING_SIZE;
    }

  ptr = or_unpack_int (buffer, &tmp_int);
  req_node_state = (HA_STATE) tmp_int;
  or_unpack_int (ptr, &tmp_int);
  force = (bool) tmp_int;

  error = hb_changemode (req_node_state, force);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  or_pack_int (reply, NO_ERROR);
  return css_send_data_packet (conn, rid, 1, reply,
			       OR_ALIGNED_BUF_SIZE (a_reply));

exit_on_error:
  assert (error != NO_ERROR);

  or_pack_int (reply, error);
  return css_send_data_packet (conn, rid, 1, reply,
			       OR_ALIGNED_BUF_SIZE (a_reply));
}

/*
 * css_process_change_ha_mode()
 *   return: css error
 *
 *   conn(in)
 *   recv_packet(in)
 */
static int
css_process_change_ha_mode (CSS_CONN_ENTRY * conn,
			    UNUSED_ARG unsigned short rid,
			    char *buffer, int buf_size)
{
  int *state = NULL;

  if (buffer == NULL || buf_size < (int) sizeof (int))
    {
      assert (false);
      return ERROR_WHEN_READING_SIZE;
    }

  state = (int *) buffer;

  *state = ntohl (*state);
  hb_resource_receive_changemode (conn, *state);

  return NO_ERRORS;
}


/*
 * css_process_kill_master()
 *   return: none
 */
static void
css_process_kill_master (void)
{
  char sock_path[PATH_MAX];

  css_shutdown_socket (css_Master_socket_fd);

  css_get_master_domain_path (sock_path, PATH_MAX, false);
  unlink (sock_path);

  hb_resource_shutdown_and_cleanup ();

  hb_cluster_shutdown_and_cleanup ();

  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_HB_STOPPED, 0);

  exit (1);
}

/*
 * css_process_start_shutdown()
 *   return: none
 *   sock_entq(in)
 *   timeout(in) : sec
 *   buffer(in)
 */
void
css_process_start_shutdown (SOCKET_QUEUE_ENTRY * sock_entq, int timeout,
			    char *buffer)
{
  unsigned short rid;

  timeout = htonl (timeout);

  css_send_command_packet (sock_entq->conn_ptr, SERVER_START_SHUTDOWN, &rid,
			   2, (char *) &timeout, sizeof (int),
			   buffer, strlen (buffer) + 1);
}

/*
 * css_master_request () -
 *    return: css error
 *
 *    conn(in):
 *    request(in):
 *    recv_packet(in):
 */
static int
css_master_request (CSS_CONN_ENTRY * conn, CSS_MASTER_REQUEST request,
		    unsigned short rid, char *buffer, int buf_size)
{
  struct css_master_request *req;
  int css_error = NO_ERRORS;

  req = &css_Master_requests[request];

  er_log_debug (ARG_FILE_LINE, "master request(%d):%s\n", request, req->name);

  assert (req->processing_function != NULL);

  req->request_count++;
  css_error = req->processing_function (conn, rid, buffer, buf_size);

  return css_error;
}

/*
 * css_master_request_handler() - information server main loop
 *   return: none
 *   sock_ent(in)
 */
void
css_master_request_handler (SOCKET_QUEUE_ENTRY * sock_ent)
{
  CSS_NET_PACKET *recv_packet = NULL;
  CSS_CONN_ENTRY *conn = NULL;
  char *buffer = NULL;
  int buf_size;
  int css_error;
  CSS_MASTER_REQUEST request;
  unsigned short rid;


  conn = sock_ent->conn_ptr;

  if (css_recv_command_packet (conn, &recv_packet) != NO_ERRORS)
    {
      er_log_debug (ARG_FILE_LINE, "receive error request.\n");

      hb_cleanup_conn_and_start_process (conn);
      return;
    }

  if (recv_packet->header.function_code < MASTER_GET_START_TIME
      || recv_packet->header.function_code >= MASTER_REQUEST_END)
    {
      er_log_debug (ARG_FILE_LINE,
		    "invalid function code(%d). \n",
		    recv_packet->header.function_code);

      css_net_packet_free (recv_packet);

      hb_cleanup_conn_and_start_process (conn);
      return;
    }

  request = recv_packet->header.function_code;
  rid = recv_packet->header.request_id;

  buffer = NULL;
  buf_size = css_net_packet_get_recv_size (recv_packet, 0);
  if (buf_size > 0)
    {
      buffer = css_net_packet_get_buffer (recv_packet, 0, -1, false);
    }

  css_error = css_master_request (conn, request, rid, buffer, buf_size);
  css_net_packet_free (recv_packet);

  if (css_error != NO_ERRORS)
    {
      er_log_debug (ARG_FILE_LINE,
		    "receive error request. (error:%d). \n", css_error);

      hb_cleanup_conn_and_start_process (conn);
    }
}
