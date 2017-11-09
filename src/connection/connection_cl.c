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

static CSS_CONN_ENTRY *css_common_connect (const char *host_name,
					   CSS_CONN_ENTRY * conn,
					   int connect_type,
					   const char *server_name,
					   int server_name_length,
					   int port, int timeout,
					   unsigned short *rid,
					   bool send_magic);
static CSS_CONN_ENTRY *css_server_connect (const char *host_name,
					   CSS_CONN_ENTRY * conn,
					   const char *server_name,
					   unsigned short *rid);
static CSS_CONN_ENTRY *css_server_connect_part_two (const char *host_name,
						    CSS_CONN_ENTRY * conn,
						    int port_id,
						    unsigned short *rid);
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
  memset (&conn->peer_version, 0, sizeof (CSS_VERSION));
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
 * css_receive_request () - "blocking" read for a new request
 *   return:
 *   conn(in):
 *   recv_packet(out):
 */
int
css_receive_request (CSS_CONN_ENTRY * conn, CSS_NET_PACKET ** recv_packet)
{
  int rc;
  CSS_NET_PACKET *tmp_recv_packet = NULL;

  if (recv_packet == NULL)
    {
      assert (false);

      return CONNECTION_CLOSED;
    }

  rc = css_net_packet_recv (&tmp_recv_packet, conn, -1, 0);
  if (rc != NO_ERRORS)
    {
      return rc;
    }

  if (tmp_recv_packet->header.packet_type != COMMAND_TYPE)
    {
      css_net_packet_free (tmp_recv_packet);
      return WRONG_PACKET_TYPE;
    }

  *recv_packet = tmp_recv_packet;

  TRACE ("in css_receive_request, received request: %d\n",
	 tmp_recv_packet->header.function_code);

  return (rc);
}

/*
 * css_common_connect () - actually try to make a connection to a server
 *   return:
 *   host_name(in):
 *   conn(in/out):
 *   connect_type(in):
 *   server_name(in):
 *   server_name_length(in):
 *   port(in):
 *   timeout(in): timeout in seconds
 *   rid(out):
 */
static CSS_CONN_ENTRY *
css_common_connect (const char *host_name, CSS_CONN_ENTRY * conn,
		    int connect_type, const char *server_name,
		    int server_name_length, int port, int timeout,
		    unsigned short *rid, bool send_magic)
{
  SOCKET fd;

  if (timeout > 0)
    {
      /* timeout in milli-seconds in css_tcp_client_open_with_timeout() */
      fd = css_tcp_client_open_with_timeout (host_name, port, timeout * 1000);
    }
  else
    {
      fd = css_tcp_client_open_with_retry (host_name, port, true);
    }

  if (!IS_INVALID_SOCKET (fd))
    {
      int css_error;

      conn->fd = fd;

      if (send_magic == true && css_send_magic (conn) != NO_ERRORS)
	{
	  return NULL;
	}

      css_error = css_send_command_packet (conn, connect_type, rid, 1,
					   server_name, server_name_length);
      if (css_error == NO_ERRORS)
	{
	  return conn;
	}
    }
  else if (errno == ETIMEDOUT)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ERR_CSS_TCP_CONNECT_TIMEDOUT, 2, host_name,
			   timeout);
    }
  else
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ERR_CSS_TCP_CANNOT_CONNECT_TO_MASTER, 1,
			   host_name);
    }

  return NULL;
}

/*
 * css_server_connect () - actually try to make a connection to a server
 *   return:
 *   host_name(in):
 *   conn(in):
 *   server_name(in):
 *   rid(out):
 */
static CSS_CONN_ENTRY *
css_server_connect (const char *host_name, CSS_CONN_ENTRY * conn,
		    const char *server_name, unsigned short *rid)
{
  int length;

  if (server_name)
    {
      length = strlen (server_name) + 1;
    }
  else
    {
      length = 0;
    }

  /* timeout in second in css_common_connect() */
  return (css_common_connect
	  (host_name, conn, MASTER_CONN_TYPE_TO_SERVER, server_name, length,
	   css_Service_id,
	   prm_get_integer_value (PRM_ID_TCP_CONNECTION_TIMEOUT), rid, true));
}

/* New style server connection function that uses an explicit port id */

/*
 * css_server_connect_part_two () -
 *   return:
 *   host_name(in):
 *   conn(in):
 *   port_id(in):
 *   rid(in):
 */
static CSS_CONN_ENTRY *
css_server_connect_part_two (const char *host_name, CSS_CONN_ENTRY * conn,
			     int port_id, unsigned short *rid)
{
  int reason = -1;
  CSS_CONN_ENTRY *return_status;

  return_status = NULL;

  /*
   * Use css_common_connect with the server's port id, since we already
   * know we'll be connecting to the right server, don't bother sending
   * the server name.
   */
  /* timeout in second in css_common_connect() */
  if (css_common_connect
      (host_name, conn, MASTER_CONN_TYPE_TO_SERVER, NULL, 0, port_id,
       prm_get_integer_value (PRM_ID_TCP_CONNECTION_TIMEOUT), rid,
       false) != NULL)
    {
      /* now ask for a reply from the server */
      if (css_recv_data_from_server
	  (NULL, conn, *rid, -1, 1, (char *) &reason,
	   sizeof (int)) == NO_ERRORS)
	{
	  reason = ntohl (reason);
	  if (reason == SERVER_CONNECTED)
	    {
	      return_status = conn;
	    }

	  /* we shouldn't have to deal with SERVER_STARTED responses here ? */
	}
    }

  return return_status;
}

/*
 * css_connect_to_master_server () - connect to the master from the server
 *   return:
 *   master_port_id(in):
 *   server_name(in):
 *   name_length(in):
 *
 * Note: The server name argument is actually a combination of two strings,
 *       the server name and the server version
 */
CSS_CONN_ENTRY *
css_connect_to_master_server (int master_port_id,
			      const char *server_name, int name_length)
{
  char hname[MAXHOSTNAMELEN];
  CSS_CONN_ENTRY *conn;
  unsigned short rid;
  int response, response_buff;
  char *pname;
  int datagram_fd, socket_fd;

  css_Service_id = master_port_id;
  if (GETHOSTNAME (hname, MAXHOSTNAMELEN) == 0)
    {
      conn = css_make_conn (0);
      if (conn == NULL)
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ERR_CSS_ERROR_DURING_SERVER_CONNECT, 1,
			       server_name);
	  return NULL;
	}

      if (css_common_connect
	  (hname, conn, MASTER_CONN_TYPE_HB_PROC, server_name, name_length,
	   master_port_id, 0, &rid, true) == NULL)
	{
	  css_free_conn (conn);
	  return NULL;
	}
      else
	{
	  if (css_recv_data_from_server (NULL, conn, rid, -1, 1,
					 (char *) &response_buff,
					 sizeof (int)) == NO_ERRORS)
	    {
	      response = ntohl (response_buff);

	      TRACE
		("connect_to_master received %d as response from master\n",
		 response);

	      switch (response)
		{
		case SERVER_ALREADY_EXISTS:
		  css_free_conn (conn);

		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ERR_CSS_SERVER_ALREADY_EXISTS, 1, server_name);
		  return NULL;

		case SERVER_REQUEST_ACCEPTED:
		  /* send the "pathname" for the datagram */
		  /* be sure to open the datagram first.  */
		  pname = tempnam (NULL, "rye");
		  if (pname)
		    {
		      if (css_tcp_setup_server_datagram (pname, &socket_fd)
			  && css_send_data_packet (conn, rid, 1, pname,
						   strlen (pname) + 1) ==
			  NO_ERRORS
			  && css_tcp_listen_server_datagram (socket_fd,
							     &datagram_fd))
			{
			  (void) unlink (pname);
			  /* don't use free_and_init on pname since it came from tempnam() */
			  free (pname);
			  css_free_conn (conn);
			  close (socket_fd);
			  return (css_make_conn (datagram_fd));
			}
		      else
			{
			  /* don't use free_and_init on pname since it came from tempnam() */
			  free (pname);
			  er_set_with_oserror (ER_ERROR_SEVERITY,
					       ARG_FILE_LINE,
					       ERR_CSS_ERROR_DURING_SERVER_CONNECT,
					       1, server_name);
			  css_free_conn (conn);
			  return NULL;
			}
		    }
		  else
		    {
		      /* Could not create the temporary file */
		      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
					   ERR_CSS_ERROR_DURING_SERVER_CONNECT,
					   1, server_name);
		      css_free_conn (conn);
		      return NULL;
		    }
		}
	    }
	}
      css_free_conn (conn);
    }

  return NULL;
}

/*
 * css_connect_to_rye_server () - make a new connection to a server
 *   return:
 *   host_name(in):
 *   server_name(in):
 */
CSS_CONN_ENTRY *
css_connect_to_rye_server (const char *host_name, const char *server_name)
{
  CSS_CONN_ENTRY *conn;
  int css_error;
  int reason, port_id;
  int retry_count;
  unsigned short rid;
  int reply[2];

  conn = css_make_conn (-1);
  if (conn == NULL)
    {
      return NULL;
    }

  retry_count = 0;
  if (css_server_connect (host_name, conn, server_name, &rid) == NULL)
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

  switch (reason)
    {
    case SERVER_CONNECTED:
      return conn;

    case SERVER_STARTED:
      if (++retry_count > 20)
	{
	  break;
	}
      else
	{
	  css_close_conn (conn);
	}
      break;

    case SERVER_CONNECTED_NEW:
      port_id = ntohl (reply[1]);
      css_close_conn (conn);

      if (css_server_connect_part_two (host_name, conn, port_id, &rid))
	{
	  return conn;
	}
      break;

    case SERVER_IS_RECOVERING:
    case SERVER_CLIENTS_EXCEEDED:
    case SERVER_INACCESSIBLE_IP:
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
	break;
      }
    case SERVER_NOT_FOUND:
    case SERVER_HANG:
    default:
      break;
    }

exit:
  css_free_conn (conn);
  return NULL;
}

/*
 * css_connect_to_master_for_info () - connect to the master server
 *   return:
 *   host_name(in):
 *   port_id(in):
 *   rid(out):
 *
 * Note: This will allow the client to extract information from the master,
 *       as well as modify runtime parameters.
 */
CSS_CONN_ENTRY *
css_connect_to_master_for_info (const char *host_name, int port_id,
				unsigned short *rid)
{
  return (css_connect_to_master_timeout (host_name, port_id, 0, rid));
}

/*
 * css_connect_to_master_timeout () - connect to the master server
 *   return:
 *   host_name(in):
 *   port_id(in):
 *   timeout(in): timeout in milli-seconds
 *   rid(out):
 *
 * Note: This will allow the client to extract information from the master,
 *       as well as modify runtime parameters.
 */
CSS_CONN_ENTRY *
css_connect_to_master_timeout (const char *host_name, int port_id,
			       int timeout, unsigned short *rid)
{
  CSS_CONN_ENTRY *conn;
  double time = timeout;

  conn = css_make_conn (0);
  if (conn == NULL)
    {
      return NULL;
    }

  time = ceil (time / 1000);

  return (css_common_connect (host_name, conn, MASTER_CONN_TYPE_INFO, NULL, 0,
			      port_id, (int) time, rid, true));
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
 *   return:
 *   port_id(in):
 */
bool
css_does_master_exist (int port_id)
{
  SOCKET fd;

  /* Don't waste time retrying between master to master connections */
  fd = css_tcp_client_open_with_retry ("localhost", port_id, false);
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
