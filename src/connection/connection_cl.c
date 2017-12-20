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
 * connection_cl.c - general interface routines needed to support
 *                   the client and server interaction
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <assert.h>
#include <math.h>

#include <sys/time.h>
#include <sys/ioctl.h>
#include <sys/uio.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>


#include "porting.h"
#include "error_manager.h"
#include "connection_globals.h"
#include "memory_alloc.h"
#include "system_parameter.h"
#include "environment_variable.h"
#include "tcp.h"
#include "connection_cl.h"
#include "client_support.h"


#ifdef PACKET_TRACE
#define TRACE(string, arg1)        \
        do {                       \
          er_log_debug(ARG_FILE_LINE, string, arg1);  \
        }                          \
        while (0);
#else /* PACKET_TRACE */
#define TRACE(string, arg1)
#endif /* PACKET_TRACE */

/* the queue anchor for all the connection structures */
static CSS_CONN_ENTRY *css_Conn_anchor = NULL;
static int css_Client_id = 0;

static void css_initialize_conn (CSS_CONN_ENTRY * conn, SOCKET fd);
static void css_close_conn (CSS_CONN_ENTRY * conn);
static void css_dealloc_conn (CSS_CONN_ENTRY * conn);

/*
 * css_shutdown_conn () -
 *   return: void
 *   conn(in/out):
 *
 * To close down a connection and make sure that the fd gets
 * set to -1 so we don't try to shutdown the socket more than once.
 *
 */
void
css_shutdown_conn (CSS_CONN_ENTRY * conn)
{
  if (!IS_INVALID_SOCKET (conn->fd))
    {
      css_shutdown_socket (conn->fd);
      conn->fd = INVALID_SOCKET;
    }
  conn->status = CONN_CLOSED;
}

/*
 * css_initialize_conn () -
 *   return: void
 *   conn(in/out):
 *   fd(in):
 */
static void
css_initialize_conn (CSS_CONN_ENTRY * conn, SOCKET fd)
{
  conn->request_id = 0;
  conn->fd = fd;
  conn->status = CONN_OPEN;
  conn->client_id = ++css_Client_id;
  conn->tran_index = -1;
  conn->is_server_in_tran = false;
  conn->conn_reset_on_commit = false;
  conn->is_client_ro_tran = false;
  conn->server_shard_nodeid = 0;
  memset (&conn->peer_version, 0, sizeof (RYE_VERSION));
}

/*
 * css_make_conn () -
 *   return:
 *   fd(in):
 */
CSS_CONN_ENTRY *
css_make_conn (SOCKET fd)
{
  CSS_CONN_ENTRY *conn;

  conn = (CSS_CONN_ENTRY *) malloc (sizeof (CSS_CONN_ENTRY));
  if (conn == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, sizeof (CSS_CONN_ENTRY));
      return NULL;
    }

  css_initialize_conn (conn, fd);
  conn->next = css_Conn_anchor;
  css_Conn_anchor = conn;

  return conn;
}

/*
 * css_close_conn () -
 *   return: void
 *   conn(in):
 */
static void
css_close_conn (CSS_CONN_ENTRY * conn)
{
  if (conn && !IS_INVALID_SOCKET (conn->fd))
    {
      css_shutdown_conn (conn);
      css_initialize_conn (conn, -1);
    }
}

/*
 * css_dealloc_conn () -
 *   return: void
 *   conn(in/out):
 */
static void
css_dealloc_conn (CSS_CONN_ENTRY * conn)
{
  CSS_CONN_ENTRY *p, *previous;

  for (p = previous = css_Conn_anchor; p; previous = p, p = p->next)
    {
      if (p == conn)
	{
	  if (p == css_Conn_anchor)
	    {
	      css_Conn_anchor = p->next;
	    }
	  else
	    {
	      previous->next = p->next;
	    }
	  break;
	}
    }

  if (p)
    {
      free_and_init (conn);
    }
}

/*
 * css_free_conn () -
 *   return: void
 *   conn(in/out):
 */
void
css_free_conn (CSS_CONN_ENTRY * conn)
{
  css_close_conn (conn);
  css_dealloc_conn (conn);
}

/*
 * css_find_exception_conn () -
 *   return:
 */
CSS_CONN_ENTRY *
css_find_exception_conn (void)
{
  return NULL;
}

/*
 * css_find_conn_from_fd () - find the connection associated with the current
 *                            socket descriptor
 *   return: conn or NULL
 *   fd(in): Socket fd
 */
CSS_CONN_ENTRY *
css_find_conn_from_fd (SOCKET fd)
{
  CSS_CONN_ENTRY *p;

  for (p = css_Conn_anchor; p; p = p->next)
    {
      if (p->fd == fd)
	{
	  return p;
	}
    }

  return NULL;
}

/*
 * css_get_request_id () - return the next valid request id
 *   return:
 *   conn(in):
 */
unsigned short
css_get_request_id (CSS_CONN_ENTRY * conn)
{
  conn->request_id++;
  if (conn->request_id == 0)
    {
      conn->request_id++;
    }
  return conn->request_id;
}

/*
 * css_test_for_open_conn () - test to see if the connection is still open
 *   return:
 *   conn(in):
 */
int
css_test_for_open_conn (CSS_CONN_ENTRY * conn)
{
  return (conn && conn->status == CONN_OPEN);
}

/*
 * css_send_close_request () - close an open connection
 *   return:
 *   conn(in):
 */
int
css_send_close_request (CSS_CONN_ENTRY * conn)
{
  if (!conn || conn->status == CONN_CLOSED)
    {
      return CONNECTION_CLOSED;
    }

  if (conn->status == CONN_OPEN)
    {
      css_send_close_packet (conn, 0);
    }

  css_shutdown_conn (conn);

  return NO_ERRORS;
}

/*
 * css_common_connect_cl () - actually try to make a connection to a server
 *   return: CSS_ERROR
 */
int
css_common_connect_cl (const PRM_NODE_INFO * node_info, CSS_CONN_ENTRY * conn,
		       int connect_type, const char *dbname,
		       const char *packed_name, int packed_name_len,
		       int timeout, unsigned short *rid, bool send_magic)
{
  SOCKET fd;
  int css_error = NO_ERRORS;

  fd = css_tcp_client_open (node_info, connect_type, dbname, timeout * 1000);

  if (!IS_INVALID_SOCKET (fd))
    {
      conn->fd = fd;

      if (send_magic == true)
	{
	  css_error = css_send_magic (conn);
	}

      if (css_error == NO_ERRORS)
	{
	  css_error = css_send_command_packet (conn, connect_type, rid, 1,
					       packed_name, packed_name_len);
	}
    }
  else
    {
      char hostname[256];
      prm_node_info_to_str (hostname, sizeof (hostname), node_info);
      if (errno == ETIMEDOUT)
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ERR_CSS_TCP_CONNECT_TIMEDOUT, 2, hostname,
			       timeout);
	}
      else
	{
	  if (connect_type == SVR_CONNECT_TYPE_TO_SERVER ||
	      connect_type == SVR_CONNECT_TYPE_TRANSFER_CONN)
	    {
	      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
				   ER_NET_CANT_CONNECT_SERVER, 2,
				   dbname, hostname);
	    }
	  else
	    {
	      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
				   ERR_CSS_TCP_CANNOT_CONNECT_TO_MASTER, 1,
				   hostname);
	    }
	}
      css_error = REQUEST_REFUSED;
    }

  return css_error;
}

/*
 * css_connect_to_rye_server () - make a new connection to a server
 */
CSS_CONN_ENTRY *
css_connect_to_rye_server (const PRM_NODE_INFO * node_info,
			   const char *server_name, int connect_type)
{
  CSS_CONN_ENTRY *conn;
  int css_error;
  int reason;
  unsigned short rid;
  int reply[2];
  const char *packed_name;
  int packed_name_len;
  int timeout = prm_get_integer_value (PRM_ID_TCP_CONNECTION_TIMEOUT);

  packed_name = server_name;
  packed_name_len = (server_name == NULL ? 0 : strlen (server_name) + 1);

  conn = css_make_conn (-1);
  if (conn == NULL)
    {
      return NULL;
    }

  if (css_common_connect_cl (node_info, conn, connect_type, server_name,
			     packed_name, packed_name_len, timeout, &rid,
			     true) != NO_ERRORS)
    {
      goto exit;
    }

  css_error = css_recv_data_from_server (NULL, conn, rid, -1,
					 1, (char *) reply, sizeof (int) * 2);
  if (css_error != NO_ERRORS)
    {
      goto exit;
    }

  reason = ntohl (reply[0]);

  if (reason == SERVER_CONNECTED)
    {
      return conn;
    }
  else
    {
      int error_length;
      char *error_area;

      error_area = NULL;
      if (css_recv_error_from_server (conn, rid, &error_area,
				      &error_length, -1) == NO_ERRORS)
	{
	  if (error_area != NULL)
	    {
	      er_set_area_error ((void *) error_area);
	      free_and_init (error_area);
	    }
	}
    }

exit:
  css_free_conn (conn);
  return NULL;
}

/*
 * css_connect_to_master_for_info () - connect to the master server
 *
 * Note: This will allow the client to extract information from the master,
 *       as well as modify runtime parameters.
 */
CSS_CONN_ENTRY *
css_connect_to_master_for_info (const PRM_NODE_INFO * node_info,
				unsigned short *rid)
{
  return (css_connect_to_master_timeout (node_info, 0, rid));
}

/*
 * css_connect_to_master_timeout () - connect to the master server
 *   return:
 *   timeout(in): timeout in milli-seconds
 *   rid(out):
 *
 * Note: This will allow the client to extract information from the master,
 *       as well as modify runtime parameters.
 */
CSS_CONN_ENTRY *
css_connect_to_master_timeout (const PRM_NODE_INFO * node_info, int timeout,
			       unsigned short *rid)
{
  CSS_CONN_ENTRY *conn;
  double time = timeout;

  conn = css_make_conn (INVALID_SOCKET);
  if (conn == NULL)
    {
      return NULL;
    }

  time = ceil (time / 1000);

  if (css_common_connect_cl (node_info, conn, SVR_CONNECT_TYPE_MASTER_INFO,
			     NULL, NULL, 0,
			     (int) time, rid, true) == NO_ERRORS)
    {
      return conn;
    }
  else
    {
      css_free_conn (conn);
      return NULL;
    }
}

/*
 * css_send_request_to_master
 */
int
css_send_request_to_master (CSS_CONN_ENTRY * conn, CSS_MASTER_REQUEST request,
			    int timeout, int num_send_buffers,
			    int num_recv_buffers, ...)
{
  va_list args, tmp_args;
  unsigned short rid;
  char **recv_buffer = NULL;
  int *recv_size = NULL;
  int css_error = NO_ERRORS;
  CSS_NET_PACKET *recv_packet = NULL;

  if ((num_send_buffers != 0 && num_send_buffers != 1)
      || (num_recv_buffers != 0 && num_recv_buffers != 1))
    {
      assert (false);
      return ERROR_ON_READ;
    }

  va_start (args, num_recv_buffers);
  css_error = css_send_command_packet_v (conn, request, &rid,
					 num_send_buffers, args);
  if (css_error != NO_ERRORS)
    {
      rid = 0;
    }
  va_end (args);
  if (css_error != NO_ERRORS)
    {
      return css_error;
    }

  if (num_recv_buffers == 0)
    {
      return NO_ERRORS;
    }

  css_error = css_recv_data_from_server (&recv_packet, conn, rid, timeout, 0);
  if (css_error != NO_ERRORS)
    {
      return css_error;
    }

  va_start (tmp_args, num_recv_buffers);

  if (num_send_buffers == 1)
    {
      va_arg (tmp_args, char *);
      va_arg (tmp_args, int);
    }

  if (num_recv_buffers == 1)
    {
      recv_buffer = va_arg (tmp_args, char **);
      recv_size = va_arg (tmp_args, int *);

      *recv_buffer = css_net_packet_get_buffer (recv_packet, 0, -1, true);
      *recv_size = css_net_packet_get_recv_size (recv_packet, 0);
    }

  css_net_packet_free (recv_packet);

  va_end (tmp_args);

  return css_error;
}

/*
 * css_does_master_exist () -
 */
bool
css_does_master_exist ()
{
  SOCKET fd;
  PRM_NODE_INFO node_info = prm_get_myself_node_info ();

  /* Don't waste time retrying between master to master connections */
  fd =
    css_tcp_client_open (&node_info, SVR_CONNECT_TYPE_MASTER_INFO, NULL,
			 1000);
  if (!IS_INVALID_SOCKET (fd))
    {
      css_shutdown_socket (fd);
      return true;
    }
  else
    {
      return false;
    }
}
