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
 * connection_support.c - general networking function
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>
#include <assert.h>

#include <sys/time.h>
#include <sys/ioctl.h>
#include <sys/uio.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fcntl.h>
#include <poll.h>

#include "porting.h"
#include "error_manager.h"
#include "connection_globals.h"
#include "memory_alloc.h"
#include "environment_variable.h"
#include "system_parameter.h"
#include "boot_sr.h"
#include "tcp.h"

#if defined(SERVER_MODE)
#include "connection_sr.h"
#include "server_support.h"
#else
#include "connection_cl.h"
#include "client_support.h"
#endif

#if defined(CS_MODE)
#include "network_interface_cl.h"
#endif

#include "storage_common.h"
#include "heap_file.h"
#include "dbval.h"
#include "db_date.h"
#include "heartbeat.h"

#include "rye_server_shm.h"

#if !defined (SERVER_MODE)
#define pthread_mutex_init(a, b)
#define pthread_mutex_destroy(a)
#define pthread_mutex_lock(b) 0
#define pthread_mutex_unlock(a)
#endif /* !SERVER_MODE */

#define INITIAL_IP_NUM 16


#define IO_VECTOR_SET_PTR(IOV_PTR, BUFF_PTR, LEN)		\
	do {							\
	  struct iovec *_tmp_iov_ptr = (IOV_PTR);		\
	  _tmp_iov_ptr->iov_base = (BUFF_PTR);	\
	  _tmp_iov_ptr->iov_len = (LEN);			\
	} while (0)

static const int CSS_TCP_MIN_NUM_RETRIES = 3;
#define CSS_TRUNCATE_BUFFER_SIZE    512

#if !defined (SERVER_MODE)
static CSS_SERVER_TIMEOUT_FN css_server_timeout_fn = NULL;

CSS_CHECK_SERVER_ALIVE_FN css_check_server_alive_fn = NULL;
CSS_CHECK_CLIENT_ALIVE_FN css_check_client_alive_fn = NULL;
#endif /* !SERVER_MODE */

#if !defined(SERVER_MODE)
static int css_sprintf_conn_infoids (SOCKET fd, const char **client_user_name,
				     const char **client_host_name,
				     int *client_pid);
#endif
static int css_vector_send (SOCKET fd, struct iovec *vec[], int *len,
			    int bytes_written, int timeout);
#if defined (ENABLE_UNUSED_FUNCTION)
static void css_set_io_vector (struct iovec *vec1_p, struct iovec *vec2_p,
			       const char *buff, int len, int *templen);
#endif
static int css_send_io_vector (CSS_CONN_ENTRY * conn, struct iovec *vec_p,
			       ssize_t total_len, int vector_length,
			       int timeout);
static int css_vector_recv (SOCKET fd, struct iovec *vec, int vec_count,
			    int nbytes, int timeout);

static void css_set_net_header (NET_HEADER * header_p, char packet_type,
				short function_code, int request_id,
				CSS_CONN_ENTRY * conn,
				int num_buffers, int *buffer_sizes);
static void css_set_net_header_hton (NET_HEADER * dest_p, NET_HEADER * src_p);
static void css_set_net_header_ntoh (NET_HEADER * dest_p, NET_HEADER * src_p);

#if defined(SERVER_MODE)
static char *css_trim_str (char *str);
#endif

static int css_net_packet_send (CSS_CONN_ENTRY * conn,
				CSS_NET_PACKET * send_packet);


static CSS_NET_PACKET *css_net_packet_alloc (CSS_CONN_ENTRY * conn,
					     NET_HEADER * net_header);
static void css_net_packet_buffer_free (CSS_NET_PACKET * net_packet,
					int idx, bool free_mem);
static char *css_net_packet_buffer_alloc (CSS_NET_PACKET * net_packet,
					  int idx, int size);

#if !defined(SERVER_MODE)
static int
css_sprintf_conn_infoids (SOCKET fd, const char **client_user_name,
			  const char **client_host_name, int *client_pid)
{
  CSS_CONN_ENTRY *conn;
  static char user_name[L_cuserid] = { '\0' };
  static char host_name[MAXHOSTNAMELEN] = { '\0' };
  static int pid;
  int tran_index = -1;

  conn = css_find_conn_from_fd (fd);

  if (conn != NULL && conn->tran_index != -1)
    {
      if (getuserid (user_name, L_cuserid) == NULL)
	{
	  strcpy (user_name, "");
	}

      if (GETHOSTNAME (host_name, MAXHOSTNAMELEN) != 0)
	{
	  strcpy (host_name, "???");
	}

      pid = getpid ();

      *client_user_name = user_name;
      *client_host_name = host_name;
      *client_pid = pid;
      tran_index = conn->tran_index;
    }

  return tran_index;
}
#endif /* SERVER_MODE */

#if !defined(SERVER_MODE)
static void
css_set_networking_error (SOCKET fd)
{
  const char *client_user_name;
  const char *client_host_name;
  int client_pid;
  int client_tranindex;

  client_tranindex = css_sprintf_conn_infoids (fd, &client_user_name,
					       &client_host_name,
					       &client_pid);

  if (client_tranindex != -1)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_CSS_RECV_OR_SEND, 5, fd,
			   client_tranindex, client_user_name,
			   client_host_name, client_pid);
    }
}
#endif

/*
 * css_vector_send() -
 *   return: size of sent if success, or error code
 *   fd(in): socket descripter
 *   vec(in): vector buffer
 *   len(in): vector length
 *   bytes_written(in):
 *   timeout(in): timeout value in milli-seconds
 */
static int
css_vector_send (SOCKET fd, struct iovec *vec[], int *len, int bytes_written,
		 int timeout)
{
  int i, n;
  struct pollfd po[1] = { {0, 0, 0} };

  if (fd < 0)
    {
      er_log_debug (ARG_FILE_LINE, "css_vector_send: fd < 0");
      errno = EINVAL;
      return -1;
    }

  if (bytes_written > 0)
    {
#ifdef RYE_DEBUG
      er_log_debug (ARG_FILE_LINE,
		    "css_vector_send: retry called for %d\n", bytes_written);
#endif
      for (i = 0; i < *len; i++)
	{
	  if ((*vec)[i].iov_len <= (size_t) bytes_written)
	    {
	      bytes_written -= (*vec)[i].iov_len;
	    }
	  else
	    {
	      break;
	    }
	}
      (*vec)[i].iov_len -= bytes_written;
      (*vec)[i].iov_base = ((char *) ((*vec)[i].iov_base)) + bytes_written;

      (*vec) += i;
      *len -= i;
    }

  while (true)
    {
      po[0].fd = fd;
      po[0].events = POLLOUT;
      po[0].revents = 0;
      n = poll (po, 1, timeout);
      if (n < 0)
	{
	  if (errno == EINTR)
	    {
	      continue;
	    }
	  return -1;
	}
      else if (n == 0)
	{
	  /* 0 means it timed out and no fd is changed. */
	  errno = ETIMEDOUT;
	  return -1;
	}
      else
	{
	  if (po[0].revents & POLLERR || po[0].revents & POLLHUP)
	    {
	      errno = EINVAL;
	      return -1;
	    }
	}

    write_again:
      n = writev (fd, *vec, *len);
      if (n > 0)
	{
	  return n;
	}
      else if (n == 0)
	{
	  return 0;		/* ??? */
	}
      else
	{
	  if (errno == EINTR)
	    {
	      goto write_again;
	    }
	  if (errno == EAGAIN)
	    {
	      continue;
	    }
#if !defined (SERVER_MODE)
	  css_set_networking_error (fd);
#endif /* !SERVER_MODE */
#if defined(RYE_DEBUG)
	  er_log_debug (ARG_FILE_LINE,
			"css_vector_send: returning error n %d, errno %d\n",
			n, errno);
#endif
	  return n;		/* error, return < 0 */
	}
    }

  return -1;
}

#if defined (ENABLE_UNUSED_FUNCTION)
static void
css_set_io_vector (struct iovec *vec1_p, struct iovec *vec2_p,
		   const char *buff, int len, int *templen)
{
  *templen = htonl (len);
  vec1_p->iov_base = (caddr_t) templen;
  vec1_p->iov_len = sizeof (int);
  vec2_p->iov_base = (caddr_t) buff;
  vec2_p->iov_len = len;
}
#endif

/*
 * css_send_io_vector -
 *   return:
 *   conn(in):
 *   vec_p(in):
 *   total_len(in):
 *   vector_length(in):
 *   timeout(in): timeout value in milli-seconds
 */
static int
css_send_io_vector (CSS_CONN_ENTRY * conn, struct iovec *vec_p,
		    ssize_t total_len, int vector_length, int timeout)
{
  int rc;

  rc = 0;
  while (total_len > 0)
    {
      rc = css_vector_send (conn->fd, &vec_p, &vector_length, rc, timeout);
      if (rc < 0)
	{
	  css_shutdown_conn (conn);
	  return ERROR_ON_WRITE;
	}
      total_len -= rc;
    }

  return NO_ERRORS;
}

static void
css_set_net_header (NET_HEADER * header_p, char packet_type,
		    short function_code, int request_id,
		    CSS_CONN_ENTRY * conn, int num_buffers, int *buffer_sizes)
{
  int i;

  header_p->packet_type = packet_type;
  header_p->tran_index = conn->tran_index;
  header_p->request_id = request_id;
  header_p->function_code = function_code;
  header_p->is_server_in_tran = conn->is_server_in_tran;
  header_p->reset_on_commit = conn->conn_reset_on_commit;
#if defined(SERVER_MODE)
  header_p->is_client_ro_tran = false;
  header_p->server_shard_nodeid = svr_shm_get_nodeid ();
#else
  header_p->is_client_ro_tran = conn->is_client_ro_tran;
  header_p->server_shard_nodeid = conn->server_shard_nodeid;
#endif
  header_p->num_buffers = num_buffers;

  for (i = 0; i < num_buffers; i++)
    {
      header_p->buffer_sizes[i] = buffer_sizes[i];
    }
}

static void
css_set_net_header_hton (NET_HEADER * dest_p, NET_HEADER * src_p)
{
  int i;
  int num_buffers = src_p->num_buffers;

  dest_p->packet_type = src_p->packet_type;
  dest_p->tran_index = htonl (src_p->tran_index);
  dest_p->request_id = htonl (src_p->request_id);
  dest_p->function_code = htons (src_p->function_code);
  dest_p->is_server_in_tran = src_p->is_server_in_tran;
  dest_p->reset_on_commit = src_p->reset_on_commit;
  dest_p->is_client_ro_tran = src_p->is_client_ro_tran;
  dest_p->num_buffers = htonl (src_p->num_buffers);
  dest_p->server_shard_nodeid = htons (src_p->server_shard_nodeid);

  for (i = 0; i < num_buffers; i++)
    {
      dest_p->buffer_sizes[i] = htonl (src_p->buffer_sizes[i]);
    }
}

static void
css_set_net_header_ntoh (NET_HEADER * dest_p, NET_HEADER * src_p)
{
  int i;

  dest_p->packet_type = src_p->packet_type;
  dest_p->tran_index = ntohl (src_p->tran_index);
  dest_p->request_id = ntohl (src_p->request_id);
  dest_p->function_code = ntohs (src_p->function_code);
  dest_p->is_server_in_tran = src_p->is_server_in_tran;
  dest_p->reset_on_commit = src_p->reset_on_commit;
  dest_p->is_client_ro_tran = src_p->is_client_ro_tran;
  dest_p->num_buffers = ntohl (src_p->num_buffers);
  dest_p->server_shard_nodeid = ntohs (src_p->server_shard_nodeid);

  for (i = 0; i < dest_p->num_buffers; i++)
    {
      dest_p->buffer_sizes[i] = ntohl (src_p->buffer_sizes[i]);
    }
}

/*
 * css_recv_command_packet () - "blocking" read for a new request
 */
int
css_recv_command_packet (CSS_CONN_ENTRY * conn, CSS_NET_PACKET ** recv_packet)
{
  int css_error;
  CSS_NET_PACKET *tmp_recv_packet = NULL;

  assert (recv_packet != NULL);

  css_error = css_net_packet_recv (&tmp_recv_packet, conn, -1, 0);
  if (css_error != NO_ERRORS)
    {
      return css_error;
    }

  if (tmp_recv_packet->header.packet_type != COMMAND_TYPE)
    {
      css_net_packet_free (tmp_recv_packet);
      return WRONG_PACKET_TYPE;
    }

  *recv_packet = tmp_recv_packet;

  er_log_debug (ARG_FILE_LINE,
		"in css_recv_command_packet, received request: %d\n",
		tmp_recv_packet->header.function_code);

  return NO_ERRORS;
}

/*
 * css_send_data_packet() - transfer a data packet to the client.
 *   return: enum css_error_code (See connectino_defs.h)
 *   conn(in): connection entry
 *   rid(in): request id
 *   num_buffers(in) : number of buffers
 *   ...(in): buffer ptrs and buffer sizes for data will be sent
 */
int
css_send_data_packet (CSS_CONN_ENTRY * conn, unsigned short rid,
		      int num_buffers, ...)
{
  va_list args;
  int rc;

  va_start (args, num_buffers);

  rc = css_send_data_packet_v (conn, rid, num_buffers, args);

  va_end (args);

  return rc;
}

int
css_send_data_packet_v (CSS_CONN_ENTRY * conn, unsigned short rid,
			int num_buffers, va_list data_args)
{
  int i;
  CSS_NET_PACKET send_packet;
  int buffer_sizes[CSS_NET_PACKET_MAX_BUFFERS];

  if (!conn || conn->status != CONN_OPEN)
    {
      return (CONNECTION_CLOSED);
    }

  assert (num_buffers <= CSS_NET_PACKET_MAX_BUFFERS);

  for (i = 0; i < num_buffers; i++)
    {
      send_packet.buffer[i].data_ptr = va_arg (data_args, char *);
      buffer_sizes[i] = va_arg (data_args, int);
    }

  css_set_net_header (&(send_packet.header), DATA_TYPE, 0, rid,
		      conn, num_buffers, buffer_sizes);

  return (css_net_packet_send (conn, &send_packet));
}

int
css_send_command_packet (CSS_CONN_ENTRY * conn, int request,
			 unsigned short *request_id, int num_buffers, ...)
{
  va_list args;
  int rc;

  va_start (args, num_buffers);

  rc =
    css_send_command_packet_v (conn, request, request_id, num_buffers, args);

  va_end (args);

  return rc;
}

int
css_send_command_packet_v (CSS_CONN_ENTRY * conn, int request,
			   unsigned short *request_id,
			   int num_buffers, va_list data_args)
{
  int rc;
  int i;
  CSS_NET_PACKET send_packet;
  int buffer_sizes[CSS_NET_PACKET_MAX_BUFFERS];
  unsigned short rid;

  if (!conn || conn->status != CONN_OPEN)
    {
      return (CONNECTION_CLOSED);
    }

  assert (num_buffers <= CSS_NET_PACKET_MAX_BUFFERS);

  if (request_id)
    {
      rid = css_get_request_id (conn);
      *request_id = rid;
    }
  else
    {
      rid = 0;
    }


  for (i = 0; i < num_buffers; i++)
    {
      send_packet.buffer[i].data_ptr = va_arg (data_args, char *);
      buffer_sizes[i] = va_arg (data_args, int);
    }

  css_set_net_header (&(send_packet.header), COMMAND_TYPE, request, rid,
		      conn, num_buffers, buffer_sizes);

  rc = css_net_packet_send (conn, &send_packet);

  return rc;
}

#if defined (SERVER_MODE)
int
css_send_abort_packet (CSS_CONN_ENTRY * conn, unsigned short rid)
{
  CSS_NET_PACKET send_packet;

  if (!conn || conn->status != CONN_OPEN)
    {
      return (CONNECTION_CLOSED);
    }

  css_set_net_header (&(send_packet.header), ABORT_TYPE, 0, rid,
		      conn, 0, NULL);

  return (css_net_packet_send (conn, &send_packet));
}
#endif

int
css_send_close_packet (CSS_CONN_ENTRY * conn, unsigned short rid)
{
  CSS_NET_PACKET send_packet;

  if (!conn || conn->status != CONN_OPEN)
    {
      return (CONNECTION_CLOSED);
    }

  css_set_net_header (&(send_packet.header), CLOSE_TYPE, 0, rid,
		      conn, 0, NULL);

  return (css_net_packet_send (conn, &send_packet));
}

int
css_send_error_packet (CSS_CONN_ENTRY * conn, unsigned short rid,
		       const char *buffer, int buffer_size)
{
  CSS_NET_PACKET send_packet;
  int buffer_sizes[1];

  if (!conn || conn->status != CONN_OPEN)
    {
      return (CONNECTION_CLOSED);
    }

  send_packet.buffer[0].data_ptr = buffer;
  buffer_sizes[0] = buffer_size;

  css_set_net_header (&(send_packet.header), ERROR_TYPE, 0, rid,
		      conn, 1, buffer_sizes);

  return (css_net_packet_send (conn, &send_packet));
}

#if !defined (SERVER_MODE)
/*
 * css_register_check_server_alive_fn () - regist the callback function
 *
 *   return: void
 *   callback_fn(in):
 */
void
css_register_check_server_alive_fn (CSS_CHECK_SERVER_ALIVE_FN callback_fn)
{
  css_check_server_alive_fn = callback_fn;
}

void
css_register_check_client_alive_fn (CSS_CHECK_CLIENT_ALIVE_FN callback_fn)
{
  css_check_client_alive_fn = callback_fn;
}
#endif /* !SERVER_MODE */

#if 0
int
css_send_master_request (int request, CSS_NET_PACKET ** recv_packet,
			 int num_send_buffers, int num_recv_buffers, ...)
{

}
#endif

/*
 * css_ha_state_string
 */
const char *
css_ha_state_string (HA_STATE server_state)
{
  switch (server_state)
    {
    case HA_STATE_NA:
      return "na";
    case HA_STATE_UNKNOWN:
      return HA_STATE_UNKNOWN_STR;
    case HA_STATE_MASTER:
      return HA_STATE_MASTER_STR;
    case HA_STATE_TO_BE_MASTER:
      return HA_STATE_TO_BE_MASTER_STR;
    case HA_STATE_SLAVE:
      return HA_STATE_SLAVE_STR;
    case HA_STATE_TO_BE_SLAVE:
      return HA_STATE_TO_BE_SLAVE_STR;
    case HA_STATE_REPLICA:
      return HA_STATE_REPLICA_STR;
    case HA_STATE_DEAD:
      return HA_STATE_DEAD_STR;
    default:
      assert (false);
      break;
    }

  return "invalid";
}

/*
 * css_ha_mode_string
 */
const char *
css_ha_mode_string (HA_MODE mode)
{
  switch (mode)
    {
    case HA_MODE_OFF:
      assert (false);		/* is impossible */
      return HA_MODE_OFF_STR;
    case HA_MODE_FAIL_OVER:
    case HA_MODE_FAIL_BACK:
    case HA_MODE_LAZY_BACK:
    case HA_MODE_ROLE_CHANGE:
      return HA_MODE_ON_STR;
    case HA_MODE_REPLICA:
      return HA_MODE_REPLICA_STR;
    }
  return "invalid";
}

/*
 * css_ha_filestat_string
 */
const char *
css_ha_filestat_string (LOG_HA_FILESTAT ha_file_state)
{
  switch (ha_file_state)
    {
    case LOG_HA_FILESTAT_CLEAR:
      return "CLEAR";
    case LOG_HA_FILESTAT_ARCHIVED:
      return "ARCHIVED";
    case LOG_HA_FILESTAT_SYNCHRONIZED:
      return "SYNCHRONIZED";
    default:
      return "UNKNOWN";
    }
}

#if !defined (SERVER_MODE)
void
css_register_server_timeout_fn (CSS_SERVER_TIMEOUT_FN callback_fn)
{
  css_server_timeout_fn = callback_fn;
}
#endif /* !SERVER_MODE */

#if defined(SERVER_MODE)
int
css_check_ip (IP_INFO * ip_info, unsigned char *address)
{
  int i;

  assert (ip_info && address);

  for (i = 0; i < ip_info->num_list; i++)
    {
      int address_index = i * IP_BYTE_COUNT;

      if (ip_info->address_list[address_index] == 0)
	{
	  return NO_ERROR;
	}
      else if (memcmp ((void *) &ip_info->address_list[address_index + 1],
		       (void *) address,
		       ip_info->address_list[address_index]) == 0)
	{
	  return NO_ERROR;
	}
    }

  return ER_INACCESSIBLE_IP;
}

int
css_free_ip_info (IP_INFO * ip_info)
{
  if (ip_info)
    {
      free_and_init (ip_info->address_list);
      free (ip_info);
    }

  return NO_ERROR;
}

int
css_read_ip_info (IP_INFO ** out_ip_info, char *filename)
{
  char buf[32];
  FILE *fd_ip_list;
  IP_INFO *ip_info;
  const char *dbname;
  int ip_address_list_buffer_size;
  unsigned char i;
  bool is_current_db_section;

  if (out_ip_info == NULL)
    {
      return ER_FAILED;
    }

  fd_ip_list = fopen (filename, "r");

  if (fd_ip_list == NULL)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_OPEN_ACCESS_LIST_FILE, 1, filename);
      return ER_OPEN_ACCESS_LIST_FILE;
    }

  is_current_db_section = false;

  ip_info = (IP_INFO *) malloc (sizeof (IP_INFO));
  if (ip_info == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, sizeof (IP_INFO));
      fclose (fd_ip_list);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  ip_info->num_list = 0;
  ip_address_list_buffer_size = INITIAL_IP_NUM * IP_BYTE_COUNT;
  ip_info->address_list =
    (unsigned char *) malloc (ip_address_list_buffer_size);

  if (ip_info->address_list == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, ip_address_list_buffer_size);
      goto error;
    }

  dbname = boot_db_name ();

  while (fgets (buf, 32, fd_ip_list))
    {
      char *token, *p, *save = NULL;
      int address_index;

      p = strchr (buf, '#');
      if (p != NULL)
	{
	  *p = '\0';
	}

      css_trim_str (buf);
      if (buf[0] == '\0')
	{
	  continue;
	}
      assert (strlen (buf) > 0);

      if (is_current_db_section == false &&
	  strncmp (buf, "[@", 2) == 0 && buf[strlen (buf) - 1] == ']')
	{
	  buf[strlen (buf) - 1] = '\0';
	  if (strcasecmp (dbname, buf + 2) == 0)
	    {
	      is_current_db_section = true;
	      continue;
	    }
	}

      if (is_current_db_section == false)
	{
	  continue;
	}

      if (strncmp (buf, "[@", 2) == 0 && buf[strlen (buf) - 1] == ']')
	{
	  buf[strlen (buf) - 1] = '\0';
	  if (strcasecmp (dbname, buf + 2) != 0)
	    {
	      break;
	    }
	}

      token = strtok_r (buf, ".", &save);

      address_index = ip_info->num_list * IP_BYTE_COUNT;

      if (address_index >= ip_address_list_buffer_size)
	{
	  ip_address_list_buffer_size *= 2;
	  ip_info->address_list =
	    (unsigned char *) realloc (ip_info->address_list,
				       ip_address_list_buffer_size);
	  if (ip_info->address_list == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1,
		      ip_address_list_buffer_size);
	      goto error;
	    }
	}

      for (i = 0; i < 4; i++)
	{
	  if (token == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_INVALID_ACCESS_IP_CONTROL_FILE_FORMAT, 1, filename);
	      goto error;
	    }

	  if (strcmp (token, "*") == 0)
	    {
	      break;
	    }
	  else
	    {
	      int adr = 0, result;

	      result = parse_int (&adr, token, 10);

	      if (result != 0 || adr > 255 || adr < 0)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_INVALID_ACCESS_IP_CONTROL_FILE_FORMAT, 1,
			  filename);
		  goto error;
		}

	      ip_info->address_list[address_index + 1 + i] =
		(unsigned char) adr;
	    }

	  token = strtok_r (NULL, ".", &save);

	  if (i == 3 && token != NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_INVALID_ACCESS_IP_CONTROL_FILE_FORMAT, 1, filename);
	      goto error;
	    }
	}
      ip_info->address_list[address_index] = i;
      ip_info->num_list++;
    }

  fclose (fd_ip_list);

  *out_ip_info = ip_info;

  return 0;

error:
  fclose (fd_ip_list);
  css_free_ip_info (ip_info);
  return er_errid ();
}

static char *
css_trim_str (char *str)
{
  char *p, *s;

  if (str == NULL)
    {
      return (str);
    }

  for (s = str; *s != '\0' && (*s == ' ' || *s == '\t' || *s == '\n' || *s
			       == '\r'); s++)
    {
      ;
    }

  if (*s == '\0')
    {
      *str = '\0';
      return (str);
    }

  /* *s must be a non-white char */
  for (p = s; *p != '\0'; p++)
    {
      ;
    }
  for (p--; *p == ' ' || *p == '\t' || *p == '\n' || *p == '\r'; p--)
    {
      ;
    }
  *++p = '\0';

  if (s != str)
    {
      memmove (str, s, strlen (s) + 1);
    }

  return (str);
}
#endif

/*
 * css_send_magic () - send magic
 */
int
css_send_magic (CSS_CONN_ENTRY * conn)
{
  int css_errors;
  int timeout;
  CSS_NET_PACKET *recv_packet = NULL;
  RYE_VERSION *peer_version = &conn->peer_version;
  RYE_VERSION my_version = rel_cur_version ();

  OR_ALIGNED_BUF (OR_VERSION_SIZE) a_send_buf;
  char *send_ptr = OR_ALIGNED_BUF_START (a_send_buf);
  int send_buf_size = OR_VERSION_SIZE;

  OR_ALIGNED_BUF (OR_INT_SIZE + OR_VERSION_SIZE) a_recv_buf;
  char *recv_ptr = OR_ALIGNED_BUF_START (a_recv_buf);
  int recv_buf_size = (OR_INT_SIZE + OR_VERSION_SIZE);
  char *ptr;

  or_pack_version (send_ptr, &my_version);

  css_errors = css_send_data_packet (conn, 0, 2,
				     css_Net_magic, sizeof (css_Net_magic),
				     send_ptr, send_buf_size);
  if (css_errors != NO_ERRORS)
    {
      return css_errors;
    }

  timeout = prm_get_integer_value (PRM_ID_TCP_CONNECTION_TIMEOUT) * 1000;

  if (css_net_packet_recv (&recv_packet, conn, timeout, 1,
			   recv_ptr, recv_buf_size) != NO_ERRORS)
    {
      return ERROR_ON_READ;
    }

  if (css_net_packet_get_recv_size (recv_packet, 0) != recv_buf_size)
    {
      css_errors = ERROR_ON_READ;
    }
  else
    {
      ptr = or_unpack_int (recv_ptr, &css_errors);
      ptr = or_unpack_version (ptr, peer_version);
    }

  css_net_packet_free (recv_packet);

  return css_errors;
}

/*
 * css_check_magic () - 
 */
int
css_check_magic (CSS_CONN_ENTRY * conn)
{
  int timeout;
  CSS_NET_PACKET *recv_packet = NULL;
  char magic[sizeof (css_Net_magic)];
  RYE_VERSION *peer_version = &conn->peer_version;
  int css_errors;

  OR_ALIGNED_BUF (OR_VERSION_SIZE) a_recv_buf;
  char *recv_ptr = OR_ALIGNED_BUF_START (a_recv_buf);
  int recv_size = OR_VERSION_SIZE;

  timeout = prm_get_integer_value (PRM_ID_TCP_CONNECTION_TIMEOUT) * 1000;

  css_errors = css_net_packet_recv (&recv_packet, conn, timeout, 2,
				    magic, sizeof (css_Net_magic),
				    recv_ptr, recv_size);
  if (css_errors != NO_ERRORS)
    {
      return css_errors;
    }

  if (css_net_packet_get_recv_size (recv_packet, 0) != sizeof (css_Net_magic)
      || memcmp (magic, css_Net_magic, sizeof (css_Net_magic)) != 0
      || css_net_packet_get_recv_size (recv_packet, 1) != recv_size)
    {
      css_errors = ERROR_ON_READ;
    }
  else
    {
      RYE_VERSION my_version = rel_cur_version ();

      OR_ALIGNED_BUF (OR_INT_SIZE + OR_VERSION_SIZE) a_send_buf;
      char *send_ptr = OR_ALIGNED_BUF_START (a_send_buf);
      int send_size = (OR_INT_SIZE + OR_VERSION_SIZE);
      char *ptr;

      or_unpack_version (recv_ptr, peer_version);

      if (my_version.major != peer_version->major)
	{
	  css_errors = NOT_COMPATIBLE_VERSION;
	}

      ptr = or_pack_int (send_ptr, css_errors);
      ptr = or_pack_version (ptr, &my_version);

      if (css_send_data_packet (conn, 0, 1, send_ptr, send_size) != NO_ERRORS)
	{
	  css_errors = ERROR_ON_WRITE;
	}
    }

  css_net_packet_free (recv_packet);

  return css_errors;
}

static int
css_net_packet_send (CSS_CONN_ENTRY * conn, CSS_NET_PACKET * send_packet)
{
  struct iovec iov[CSS_NET_PACKET_MAX_BUFFERS + 1];
  struct iovec *iovp;
  int total_len = 0;
  int i;
  NET_HEADER net_header;

#if defined(SERVER_MODE)
  css_epoll_set_check (conn, false);
#endif

  iovp = iov;

#if defined(VALGRIND)
  memset (iov, 0, sizeof (iov));
  memset (&net_header, 0, sizeof (NET_HEADER));
#endif

  css_set_net_header_hton (&net_header, &(send_packet->header));

  IO_VECTOR_SET_PTR (iovp++, &net_header, sizeof (NET_HEADER));
  total_len += sizeof (NET_HEADER);

  for (i = 0; i < send_packet->header.num_buffers; i++)
    {
      IO_VECTOR_SET_PTR (iovp++, send_packet->buffer[i].data_ptr,
			 send_packet->header.buffer_sizes[i]);
      total_len += send_packet->header.buffer_sizes[i];
    }

  return (css_send_io_vector (conn, iov, total_len,
			      send_packet->header.num_buffers + 1, -1));
}

int
css_net_packet_recv (CSS_NET_PACKET ** recv_packet, CSS_CONN_ENTRY * conn,
		     int timeout, int num_buffers, ...)
{
  va_list args;
  int rc;

  va_start (args, num_buffers);
  rc = css_net_packet_recv_v (recv_packet, conn, timeout, num_buffers, args);
  va_end (args);
  return rc;
}

int
css_net_packet_recv_v (CSS_NET_PACKET ** recv_packet, CSS_CONN_ENTRY * conn,
		       int timeout, int num_buffers, va_list data_args)
{
  int nbytes;
  int i;
  NET_HEADER net_header_received, net_header;
  struct iovec iov[CSS_NET_PACKET_MAX_BUFFERS];
  struct iovec *iovp;
  int total_read;
  char *arg_buffers[CSS_NET_PACKET_MAX_BUFFERS];
  int arg_buffer_sizes[CSS_NET_PACKET_MAX_BUFFERS];
  CSS_NET_PACKET *tmp_recv_packet;

  if (conn == NULL || conn->status != CONN_OPEN)
    {
      return CONNECTION_CLOSED;
    }

  /* read header */
  iovp = iov;
  IO_VECTOR_SET_PTR (iovp++, &net_header_received, sizeof (NET_HEADER));
  nbytes = css_vector_recv (conn->fd, iov, 1, sizeof (NET_HEADER), timeout);
  if (nbytes < 0 || nbytes != sizeof (NET_HEADER))
    {
      return ERROR_WHEN_READING_SIZE;
    }

  css_set_net_header_ntoh (&net_header, &net_header_received);

  assert (net_header.num_buffers <= CSS_NET_PACKET_MAX_BUFFERS);

  assert (num_buffers <= CSS_NET_PACKET_MAX_BUFFERS);

  /* get buffer pointer and size supplied by caller function */
  if (net_header.packet_type == COMMAND_TYPE ||
      net_header.packet_type == DATA_TYPE)
    {
      for (i = 0; i < num_buffers; i++)
	{
	  arg_buffers[i] = va_arg (data_args, char *);
	  arg_buffer_sizes[i] = va_arg (data_args, int);
	}
      for (; i < CSS_NET_PACKET_MAX_BUFFERS; i++)
	{
	  arg_buffers[i] = NULL;
	  arg_buffer_sizes[i] = 0;
	}
    }
  else
    {
      for (i = 0; i < CSS_NET_PACKET_MAX_BUFFERS; i++)
	{
	  arg_buffers[i] = NULL;
	  arg_buffer_sizes[i] = 0;
	}
    }

  tmp_recv_packet = css_net_packet_alloc (conn, &net_header);
  if (tmp_recv_packet == NULL)
    {
      return CANT_ALLOC_BUFFER;
    }

  /* read data */
  iovp = iov;
  total_read = 0;

  if (net_header.num_buffers > 0)
    {
      char *iov_ptr;

      for (i = 0; i < net_header.num_buffers; i++)
	{
	  if (net_header.buffer_sizes[i] == 0)
	    {
	      arg_buffers[i] = NULL;
	      iov_ptr = NULL;
	    }
	  else if (arg_buffers[i] != NULL &&
		   (arg_buffer_sizes[i] < 0 ||
		    arg_buffer_sizes[i] >= net_header.buffer_sizes[i]))
	    {
	      /* receive data to arg buffer if arg buffer has sufficient room */
	      iov_ptr = arg_buffers[i];
	    }
	  else
	    {
	      css_net_packet_buffer_alloc (tmp_recv_packet, i,
					   net_header.buffer_sizes[i]);
	      if (tmp_recv_packet->buffer[i].data_ptr == NULL)
		{
		  css_net_packet_free (tmp_recv_packet);
		  return CANT_ALLOC_BUFFER;
		}

	      iov_ptr = (char *) tmp_recv_packet->buffer[i].data_ptr;
	    }

	  IO_VECTOR_SET_PTR (iovp++, iov_ptr, net_header.buffer_sizes[i]);
	  total_read += net_header.buffer_sizes[i];
	}

      nbytes = css_vector_recv (conn->fd, iov, net_header.num_buffers,
				total_read, timeout);
      if (nbytes < 0 || nbytes != total_read)
	{
	  css_net_packet_free (tmp_recv_packet);
	  return ERROR_WHEN_READING_SIZE;
	}
    }

  /* fill arg buffer if arg buffer is not used to receive data */
  for (i = 0; i < net_header.num_buffers; i++)
    {
      if (arg_buffers[i] != NULL &&
	  arg_buffer_sizes[i] >= 0 &&
	  arg_buffer_sizes[i] < net_header.buffer_sizes[i])
	{
	  memcpy (arg_buffers[i], tmp_recv_packet->buffer[i].data_ptr,
		  arg_buffer_sizes[i]);
	}
    }

  conn->is_server_in_tran = net_header.is_server_in_tran;
  conn->conn_reset_on_commit = net_header.reset_on_commit;
  conn->is_client_ro_tran = net_header.is_client_ro_tran;
  conn->server_shard_nodeid = net_header.server_shard_nodeid;

  *recv_packet = tmp_recv_packet;

  return NO_ERRORS;
}

void
css_net_packet_free (CSS_NET_PACKET * net_packet)
{
  int i;

  if (net_packet)
    {
      for (i = 0; i < net_packet->header.num_buffers; i++)
	{
	  css_net_packet_buffer_free (net_packet, i, true);
	}
      free (net_packet);
    }
}

static CSS_NET_PACKET *
css_net_packet_alloc (UNUSED_ARG CSS_CONN_ENTRY * conn,
		      NET_HEADER * net_header)
{
  CSS_NET_PACKET *tmp_net_packet;

  tmp_net_packet = malloc (sizeof (CSS_NET_PACKET));
  if (tmp_net_packet != NULL)
    {
      tmp_net_packet->header = *net_header;
      memset (tmp_net_packet->buffer, 0, sizeof (tmp_net_packet->buffer));
    }

  return tmp_net_packet;
}

/*
 * css_net_packet_buffer_free() - free alloced buffer of NET_PACKET
 *  free_mem: true: free memory in this function
 *            false : alloced buffer will be freed in calller function of
 *		      css_net_packet_get_buffer()
 */
static void
css_net_packet_buffer_free (CSS_NET_PACKET * net_packet, int idx,
			    bool free_mem)
{
  if (free_mem == true && net_packet->buffer[idx].data_ptr != NULL)
    {
      free ((void *) net_packet->buffer[idx].data_ptr);
    }
  net_packet->buffer[idx].data_ptr = NULL;
}

static char *
css_net_packet_buffer_alloc (CSS_NET_PACKET * net_packet, int idx, int size)
{
  net_packet->buffer[idx].data_ptr = malloc (size);
  return (char *) net_packet->buffer[idx].data_ptr;
}

char *
css_net_packet_get_buffer (CSS_NET_PACKET * net_packet, int index,
			   int expected_size, bool reset_ptr)
{
  char *ptr;

  assert (index < CSS_NET_PACKET_MAX_BUFFERS);

  if (net_packet->header.num_buffers <= index ||
      net_packet->header.buffer_sizes[index] <= 0)
    {
      return NULL;
    }
  else if (expected_size >= 0 &&
	   net_packet->header.buffer_sizes[index] < expected_size)
    {
      assert (false);
      return NULL;
    }

  ptr = (char *) net_packet->buffer[index].data_ptr;

  if (reset_ptr)
    {
      css_net_packet_buffer_free (net_packet, index, false);
    }

  return ptr;
}

int
css_net_packet_get_recv_size (CSS_NET_PACKET * net_packet, int index)
{
  assert (index < CSS_NET_PACKET_MAX_BUFFERS);

  if (net_packet->header.num_buffers <= index)
    {
      return 0;
    }

  return net_packet->header.buffer_sizes[index];
}


static int
css_vector_recv (SOCKET fd, struct iovec *vec, int vec_count, int nbytes,
		 int timeout)
{
  int nleft, n;

  struct pollfd po[1] = { {0, 0, 0} };
  int time_unit, elapsed;

  if (timeout < 0)
    {
      timeout = INT_MAX;
    }
  time_unit = timeout > 5000 ? 5000 : timeout;
  elapsed = time_unit;

  if (fd < 0)
    {
      er_log_debug (ARG_FILE_LINE, "css_vector_recv: fd < 0");
      errno = EINVAL;
      return -1;
    }

  if (nbytes <= 0)
    {
      return 0;
    }

  nleft = nbytes;
  do
    {
      po[0].fd = fd;
      po[0].events = POLLIN;
      po[0].revents = 0;
      n = poll (po, 1, time_unit);
      if (n == 0)
	{
	  if (timeout > elapsed)
	    {
#if defined (CS_MODE)
	      if (CHECK_SERVER_IS_ALIVE ())
		{
		  if (css_peer_alive (fd, time_unit) == false)
		    {
		      return -1;
		    }

		  if (css_check_server_alive_fn != NULL)
		    {
		      if (css_check_server_alive_fn (NULL, NULL) == false)
			{
			  return -1;
			}
		    }
		}
	      if (css_check_client_alive_fn != NULL)
		{
		  if (css_check_client_alive_fn () == false)
		    {
		      return -1;
		    }
		}
#endif /* CS_MODE */
	      elapsed += time_unit;
	      continue;
	    }
	  else
	    {
	      return -1;
	    }
	}
      else if (n < 0)
	{
	  if (errno == EINTR)
	    {
#if !defined (SERVER_MODE)
	      if (css_server_timeout_fn != NULL)
		{
		  css_server_timeout_fn ();
		}
	      if (css_check_client_alive_fn != NULL)
		{
		  if (css_check_client_alive_fn () == false)
		    {
		      return -1;
		    }
		}
#endif /* !SERVER_MODE */
	      continue;
	    }
	  return -1;
	}
      else
	{
	  if (po[0].revents & POLLERR || po[0].revents & POLLHUP)
	    {
	      errno = EINVAL;
	      return -1;
	    }
	}

    read_again:
      n = readv (fd, vec, vec_count);

      if (n == 0)
	{
	  break;
	}

      if (n < 0)
	{
	  if (errno == EAGAIN)
	    {
	      continue;
	    }
	  if (errno == EINTR)
	    {
#if !defined (SERVER_MODE)
	      if (css_check_client_alive_fn != NULL)
		{
		  if (css_check_client_alive_fn () == false)
		    {
		      return -1;
		    }
		}
#endif /* !SERVER_MODE */
	      goto read_again;
	    }

#if !defined (SERVER_MODE)
	  css_set_networking_error (fd);
#endif /* !SERVER_MODE */
#if defined(RYE_DEBUG)
	  er_log_debug (ARG_FILE_LINE,
			"css_vector_recv: returning error n %d, errno %d\n",
			n, errno);
#endif
	  return n;		/* error, return < 0 */
	}
      nleft -= n;

      if (nleft > 0)
	{
	  int i;
	  for (i = 0; i < vec_count; i++)
	    {
	      if (vec[i].iov_len <= (size_t) n)
		{
		  n -= vec[i].iov_len;
		}
	      else
		{
		  break;
		}
	    }
	  vec[i].iov_len -= n;
	  vec[i].iov_base = ((char *) (vec[i].iov_base)) + n;

	  vec += i;
	  vec_count -= i;
	}
    }
  while (nleft > 0);

  return (nbytes - nleft);	/*  return >= 0 */
}

bool
css_is_client_ro_tran (UNUSED_ARG THREAD_ENTRY * thread_p)
{
#if defined(SERVER_MODE)
  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  if (thread_p != NULL && thread_p->conn_entry != NULL
      && thread_p->tran_index != LOG_SYSTEM_TRAN_INDEX)
    {
      assert (thread_p->tran_index == LOG_SYSTEM_TRAN_INDEX
	      || thread_p->tran_index == thread_p->conn_entry->tran_index);

      return thread_p->conn_entry->is_client_ro_tran;
    }
#endif

  return false;
}

/*
 * css_pack_server_name_for_hb_register () 
 */
static char *
css_pack_server_name_for_hb_register (int *name_length, HB_PROC_TYPE type,
				      const char *server_name,
				      const char *log_path)
{
  char *packed_name = NULL;
  const char *env_name = NULL;
  char pid_string[16];
  char *ptr;
  int n_len, l_len, e_len, p_len;

  *name_length = 0;

  env_name = envvar_root ();

  if (server_name == NULL || env_name == NULL)
    {
      return NULL;
    }

  snprintf (pid_string, sizeof (pid_string), "%d", getpid ());

  n_len = strlen (server_name) + 1;
  l_len = (log_path) ? strlen (log_path) + 1 : 0;
  e_len = strlen (env_name) + 1;
  p_len = strlen (pid_string) + 1;
  *name_length = n_len + 1 + l_len + e_len + p_len;

  packed_name = malloc (*name_length);
  if (packed_name == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, (*name_length));
      return NULL;
    }

  ptr = packed_name;

  if (type == HB_PTYPE_REPLICATION)
    {
      *ptr = MASTER_CONN_NAME_HA_REPL;
    }
  else if (type == HB_PTYPE_SERVER)
    {
      *ptr = MASTER_CONN_NAME_HA_SERVER;
    }
  else
    {
      assert (0);
      free_and_init (packed_name);
      return NULL;
    }
  ptr++;

  memcpy (ptr, server_name, n_len);
  ptr += n_len;

  if (l_len)
    {
      *(ptr - 1) = ':';
      memcpy (ptr, log_path, l_len);
      ptr += l_len;
    }

  memcpy (ptr, env_name, e_len);
  ptr += e_len;

  memcpy (ptr, pid_string, p_len);
  ptr += p_len;

  return packed_name;
}

/*
 * css_register_to_master () - register to the master 
 */
CSS_CONN_ENTRY *
css_register_to_master (HB_PROC_TYPE type,
			const char *server_name, const char *log_path)
{
  CSS_CONN_ENTRY *conn;
  unsigned short rid;
  int response;
  int css_error;
  char *packed_name = NULL;
  int name_length;
  PRM_NODE_INFO node_info = prm_get_myself_node_info ();

  conn = css_make_conn (INVALID_SOCKET);
  if (conn == NULL)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ERR_CSS_ERROR_DURING_SERVER_CONNECT, 1,
			   server_name);
      return NULL;
    }

  packed_name = css_pack_server_name_for_hb_register (&name_length,
						      type,
						      server_name, log_path);
#if defined(SERVER_MODE)
  css_error = css_common_connect_sr (conn, &rid, &node_info,
				     SVR_CONNECT_TYPE_MASTER_HB_PROC,
				     packed_name, name_length);
  if (css_error == NO_ERRORS)
    {
      css_error = css_recv_data_packet_from_client (NULL, conn, rid, -1, 1,
						    (char *) &response,
						    sizeof (int));
    }
#else
  css_error = css_common_connect_cl (&node_info, conn,
				     SVR_CONNECT_TYPE_MASTER_HB_PROC,
				     NULL, packed_name, name_length,
				     0, &rid, true);
  if (css_error == NO_ERRORS)
    {
      css_error = css_recv_data_from_server (NULL, conn, rid, -1, 1,
					     (char *) &response,
					     sizeof (int));
    }
#endif

  free_and_init (packed_name);

  if (css_error == NO_ERRORS)
    {
      response = ntohl (response);

      er_log_debug (ARG_FILE_LINE,
		    "css_register_to_master received %d as response from master\n",
		    response);

      switch (response)
	{
	case SERVER_ALREADY_EXISTS:
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ERR_CSS_SERVER_ALREADY_EXISTS, 1, server_name);
	  break;

	case SERVER_REQUEST_ACCEPTED:
	  response = 0;
	  if (css_send_data_packet (conn, rid, 1, &response,
				    sizeof (int)) == NO_ERRORS)
	    {
	      return conn;
	    }
	  else
	    {
	      er_set_with_oserror (ER_ERROR_SEVERITY,
				   ARG_FILE_LINE,
				   ERR_CSS_ERROR_DURING_SERVER_CONNECT,
				   1, server_name);
	    }
	  break;
	}
    }

  css_free_conn (conn);
  return NULL;
}
