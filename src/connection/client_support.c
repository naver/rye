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
 * client_support.c - higher level of interface routines to the client
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <signal.h>
#include <sys/param.h>
#include <syslog.h>
#include <assert.h>

#include "porting.h"
#include "connection_globals.h"
#include "connection_defs.h"
#include "connection_cl.h"
#include "connection_less.h"
#include "tcp.h"
#include "transaction_cl.h"
#include "error_manager.h"
#include "client_support.h"

static void (*css_Previous_sigpipe_handler) (int sig_no) = NULL;
/* TODO: M2 - remove css_Errno */
int css_Errno = 0;
CSS_MAP_ENTRY *css_Client_anchor;

static void css_internal_server_shutdown (void);
static void css_handle_pipe_shutdown (int sig);
static void css_set_pipe_signal (void);

/*
 * css_internal_server_shutdown() -
 *   return:
 */
static void
css_internal_server_shutdown (void)
{
  syslog (LOG_ALERT, "Lost connection to server\n");
}

/*
 * css_handle_pipe_shutdown() -
 *   return:
 *   sig(in):
 */
static void
css_handle_pipe_shutdown (int sig)
{
  CSS_CONN_ENTRY *conn;
  CSS_MAP_ENTRY *entry;

  conn = css_find_exception_conn ();
  if (conn != NULL)
    {
      entry = css_return_entry_from_conn (conn, css_Client_anchor);
      if (entry != NULL)
	{
	  css_remove_queued_connection_by_entry (entry, &css_Client_anchor);
	}
      css_internal_server_shutdown ();
    }
  else
    {
      /* Avoid an infinite loop by checking if the previous handle is myself */
      if (css_Previous_sigpipe_handler != NULL &&
	  css_Previous_sigpipe_handler != css_handle_pipe_shutdown)
	{
	  (*css_Previous_sigpipe_handler) (sig);
	}
    }
}

/*
 * css_set_pipe_signal() - sets up the signal handling mechanism
 *   return:
 *
 * Note: Note that we try to find out if there are any previous handlers.
 *       If so, make note of them so that we can pass on errors on fds that
 *       we do not know.
 */
static void
css_set_pipe_signal (void)
{
  css_Previous_sigpipe_handler = os_set_signal_handler (SIGPIPE,
							css_handle_pipe_shutdown);
  if ((css_Previous_sigpipe_handler == SIG_IGN)
      || (css_Previous_sigpipe_handler == SIG_ERR)
      || (css_Previous_sigpipe_handler == SIG_DFL)
#if !defined(LINUX)
      || (css_Previous_sigpipe_handler == SIG_HOLD)
#endif /* not LINUX */
    )
    {
      css_Previous_sigpipe_handler = NULL;
    }
}

/*
 * css_client_init() - initialize the network portion of the client interface
 *   return:
 *   sockid(in): sSocket number for remote host
 *   alloc_function(in): function for memory allocation
 *   free_function(in): function to return memory
 *   oob_function(in): function to call on receipt of an out of band message
 *   server_name(in):
 *   host_name(in):
 */
int
css_client_init (int sockid, const char *server_name, const char *host_name)
{
  CSS_CONN_ENTRY *conn;
  int error = NO_ERROR;

  css_Service_id = sockid;
  css_set_pipe_signal ();

  conn = css_connect_to_rye_server (host_name, server_name);
  if (conn != NULL)
    {
      css_queue_connection (conn, host_name, &css_Client_anchor);
    }
  else
    {
      error = er_errid ();
      if (error == NO_ERROR)
	{
	  error = ER_NET_CANT_CONNECT_SERVER;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 2,
		  server_name, host_name);
	}
    }

  return error;
}

/*
 * css_send_error_to_server() - send an error buffer to the server
 *   return:
 *   host(in): name of the server machine
 *   eid(in): enquiry id
 *   buffer(in): data buffer to queue for expected data.
 *   buffer_size(in): size of data buffer
 */
int
css_send_error_to_server (char *host, unsigned int eid,
			  char *buffer, int buffer_size)
{
  CSS_MAP_ENTRY *entry;

  entry = css_return_open_entry (host, &css_Client_anchor);
  if (entry != NULL)
    {
      entry->conn->tran_index = tm_Tran_index;
      css_Errno = css_send_error_packet (entry->conn, CSS_RID_FROM_EID (eid),
					 buffer, buffer_size);
      if (css_Errno == NO_ERRORS)
	{
	  return 0;
	}
      else
	{
	  css_remove_queued_connection_by_entry (entry, &css_Client_anchor);
	  return css_Errno;
	}
    }

  css_Errno = SERVER_WAS_NOT_FOUND;
  return css_Errno;
}

/*
 * css_send_data_to_server_v () - send a data buffer to the server
 *   return:
 *   host(in): name of the server machine
 *   rid(in): request id
 */
int
css_send_data_to_server_v (char *host, unsigned short rid,
			   int num_buffers, va_list args)
{
  CSS_MAP_ENTRY *entry;

  entry = css_return_open_entry (host, &css_Client_anchor);
  if (entry == NULL)
    {
      css_Errno = SERVER_WAS_NOT_FOUND;
      return css_Errno;
    }

  entry->conn->tran_index = tm_Tran_index;

  css_Errno = css_send_data_packet_v (entry->conn, rid, num_buffers, args);

  if (css_Errno != NO_ERRORS)
    {
      css_remove_queued_connection_by_entry (entry, &css_Client_anchor);
    }

  return css_Errno;
}

#if defined (ENABLE_UNUSED_FUNCTION)
int
css_send_data_to_server (char *host, unsigned short rid, int num_buffers, ...)
{
  int rc;
  va_list args;

  va_start (args, num_buffers);

  rc = css_send_data_to_server (host, rid, num_buffers, args);

  va_end (args);

  return rc;
}
#endif

/*
 * css_terminate() - "gracefully" terminate all requests
 *   server_error(in):
 *   return: void
 */
void
css_terminate (bool server_error)
{
  while (css_Client_anchor)
    {
      if (server_error && css_Client_anchor->conn)
	{
	  css_Client_anchor->conn->status = CONN_CLOSING;
	}
      css_send_close_request (css_Client_anchor->conn);
      css_remove_queued_connection_by_entry (css_Client_anchor,
					     &css_Client_anchor);
    }

  /*
   * If there was a previous signal handler. restore it at this point.
   */
  if (css_Previous_sigpipe_handler != NULL)
    {
      (void) os_set_signal_handler (SIGPIPE, css_Previous_sigpipe_handler);
      css_Previous_sigpipe_handler = NULL;
    }
}

/*
 * css_send_request_to_server_v () - send a request to server
 *   return:
 *   host(in): name of the remote host
 *   request(in): the request to send to the server.
 *   num_args(in): number of data args
 *   ...(in): buffer and size
 *
 * Note: This routine will allow the client to send a request and data
 *       buffers to the server
 */
unsigned int
css_send_request_to_server_v (char *host, int request, int num_args,
			      va_list args)
{
  CSS_MAP_ENTRY *entry;
  unsigned short rid;

  entry = css_return_open_entry (host, &css_Client_anchor);
  if (entry != NULL)
    {
      entry->conn->tran_index = tm_Tran_index;

      css_Errno = css_send_command_packet_v (entry->conn, request, &rid,
					     num_args, args);

      if (css_Errno == NO_ERRORS)
	{
	  return (css_make_eid (entry->id, rid));
	}
      else
	{
	  css_remove_queued_connection_by_entry (entry, &css_Client_anchor);
	  return 0;
	}
    }

  css_Errno = SERVER_WAS_NOT_FOUND;
  return 0;
}

#if defined (ENABLE_UNUSED_FUNCTION)
unsigned int
css_send_request_to_server (char *host, int request, int num_args, ...)
{
  va_list args;
  unsigned int rc;

  va_start (args, num_args);

  rc = css_send_request_to_server_v (host, request, num_args, args);

  va_end (args);

  return rc;
}
#endif

int
css_recv_data_from_server (CSS_NET_PACKET ** recv_packet,
			   CSS_CONN_ENTRY * conn, int rid,
			   int timeout, int num_buffers, ...)
{
  va_list args;
  int rc;

  va_start (args, num_buffers);

  rc = css_recv_data_from_server_v (recv_packet, conn, rid, timeout,
				    num_buffers, args);

  va_end (args);

  return rc;
}

int
css_recv_data_from_server_v (CSS_NET_PACKET ** recv_packet,
			     CSS_CONN_ENTRY * conn, int rid,
			     int timeout, int num_buffers, va_list args)
{
  CSS_NET_PACKET *tmp_recv_packet = NULL;
  bool er_set_flag = false;
  int css_error;

  if (num_buffers < 0)
    {
      /* special case. do not receive result */
      return NO_ERRORS;
    }

  if (recv_packet)
    {
      *recv_packet = NULL;
    }

  while (true)
    {
      va_list tmp_args;

      va_copy (tmp_args, args);

      css_error = css_net_packet_recv_v (&tmp_recv_packet, conn,
					 timeout, num_buffers, tmp_args);

      va_end (tmp_args);

      if (css_error != NO_ERRORS)
	{
	  break;
	}

      if (tmp_recv_packet->header.request_id == rid)
	{
	  if (tmp_recv_packet->header.packet_type == DATA_TYPE)
	    {
	      if (recv_packet)
		{
		  *recv_packet = tmp_recv_packet;
		}
	      else
		{
		  css_net_packet_free (tmp_recv_packet);
		}
	      break;
	    }
	  else if (tmp_recv_packet->header.packet_type == ERROR_TYPE)
	    {
	      if (er_set_flag == false)
		{
		  char *err;
		  assert (tmp_recv_packet->header.num_buffers == 1);
		  err = css_net_packet_get_buffer (tmp_recv_packet, 0, -1,
						   false);
		  assert (err != NULL);
		  er_set_area_error (err);
		  er_set_flag = true;
		}
	    }
	  else if (tmp_recv_packet->header.packet_type == ABORT_TYPE)
	    {
	      css_net_packet_free (tmp_recv_packet);
	      return SERVER_ABORTED;
	    }
	}
      else
	{
	  /* this case happens if signal handler call db_shutdown()
	   * while some query is executing
	   */
	  // assert (false);
	}

      css_net_packet_free (tmp_recv_packet);
    }

  return css_error;
}

int
css_recv_error_from_server (CSS_CONN_ENTRY * conn, int rid,
			    char **error_area, int *error_length, int timeout)
{
  int css_error;
  CSS_NET_PACKET *tmp_recv_packet = NULL;

  while (true)
    {
      css_error = css_net_packet_recv (&tmp_recv_packet, conn, timeout, 0);

      if (css_error != NO_ERRORS)
	{
	  break;
	}

      if (tmp_recv_packet->header.request_id != rid)
	{
	  assert (false);	/* TODO: NEED TEST */
	}
      else if (tmp_recv_packet->header.packet_type == ERROR_TYPE)
	{
	  if (error_area)
	    {
	      *error_length = css_net_packet_get_recv_size (tmp_recv_packet,
							    0);
	      *error_area = css_net_packet_get_buffer (tmp_recv_packet, 0,
						       -1, true);
	      if (*error_area == NULL)
		{
		  css_error = ERROR_ON_READ;
		}
	    }
	  break;
	}
      else if (tmp_recv_packet->header.packet_type == ABORT_TYPE)
	{
	  css_error = SERVER_ABORTED;
	  break;
	}
      else
	{
	  assert (false);
	}

      css_net_packet_free (tmp_recv_packet);
      tmp_recv_packet = NULL;
    }

  css_net_packet_free (tmp_recv_packet);

  return css_error;
}
