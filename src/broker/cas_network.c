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
 * cas_network.c -
 */

#ident "$Id$"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <assert.h>

#include <sys/time.h>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/un.h>
#include <poll.h>

#include "porting.h"
#include "dbi.h"
#include "cas_common.h"
#include "cas_network.h"
#include "cas.h"
#include "broker_env_def.h"
#include "cas_execute.h"
#include "error_code.h"
#include "broker_util.h"

#define SELECT_MASK	fd_set

static int write_buffer (SOCKET sock_fd, const char *buf, int size);
static int read_buffer (SOCKET sock_fd, char *buf, int size);

static void set_net_timeout_flag (void);
static void unset_net_timeout_flag (void);

static bool net_timeout_flag = false;

static char net_error_flag;
static int net_timeout = NET_DEFAULT_TIMEOUT;

SOCKET
net_init_env (char *port_name)
{
  int one = 1;
  SOCKET sock_fd;
  int sock_addr_len;
  struct sockaddr_un sock_addr;

  /* get a Unix stream socket */
  sock_fd = socket (AF_UNIX, SOCK_STREAM, 0);
  if (IS_INVALID_SOCKET (sock_fd))
    {
      return INVALID_SOCKET;
    }
  if ((setsockopt (sock_fd, SOL_SOCKET, SO_REUSEADDR, (char *) &one, sizeof (one))) < 0)
    {
      RYE_CLOSE_SOCKET (sock_fd);
      return INVALID_SOCKET;
    }

  memset (&sock_addr, 0, sizeof (struct sockaddr_un));
  sock_addr.sun_family = AF_UNIX;
  snprintf (sock_addr.sun_path, sizeof (sock_addr.sun_path), "%s", port_name);
  sock_addr_len = strlen (sock_addr.sun_path) + sizeof (sock_addr.sun_family) + 1;

  if (bind (sock_fd, (struct sockaddr *) &sock_addr, sock_addr_len) < 0)
    {
      RYE_CLOSE_SOCKET (sock_fd);
      return INVALID_SOCKET;
    }

  if (listen (sock_fd, 3) < 0)
    {
      RYE_CLOSE_SOCKET (sock_fd);
      return INVALID_SOCKET;
    }

  return (sock_fd);
}

SOCKET
net_connect_client (SOCKET srv_sock_fd)
{
  socklen_t clt_sock_addr_len;
  SOCKET clt_sock_fd;
  struct sockaddr_in clt_sock_addr;

  clt_sock_addr_len = sizeof (clt_sock_addr);
  clt_sock_fd = accept (srv_sock_fd, (struct sockaddr *) &clt_sock_addr, &clt_sock_addr_len);

  if (IS_INVALID_SOCKET (clt_sock_fd))
    return INVALID_SOCKET;

  net_error_flag = 0;
  return clt_sock_fd;
}

int
net_write_stream (SOCKET sock_fd, const char *buf, int size)
{
  while (size > 0)
    {
      int write_len;

      write_len = write_buffer (sock_fd, buf, size);

      if (write_len <= 0)
        {
#ifdef _DEBUG
          printf ("write error\n");
#endif
          return -1;
        }
      buf += write_len;
      size -= write_len;
    }
  return 0;
}

int
net_read_stream (SOCKET sock_fd, char *buf, int size)
{
  while (size > 0)
    {
      int read_len;

      read_len = read_buffer (sock_fd, buf, size);

      if (read_len <= 0)
        {
#ifdef _DEBUG
          if (!is_net_timed_out ())
            printf ("read error %d\n", read_len);
#endif
          return -1;
        }
      buf += read_len;
      size -= read_len;
    }

  return 0;
}

int
net_read_header (SOCKET sock_fd, MSG_HEADER * header)
{
  int retval = 0;

  if (cas_Info_size > 0)
    {
      retval = net_read_stream (sock_fd, header->buf, MSG_HEADER_SIZE);
      *(header->msg_body_size_ptr) = ntohl (*(header->msg_body_size_ptr));
    }
  else
    {
      retval = net_read_int (sock_fd, header->msg_body_size_ptr);
    }

  return retval;
}

void
init_msg_header (MSG_HEADER * header)
{
  short server_nodeid;
  int64_t shard_info_ver;

  header->msg_body_size_ptr = (int *) (header->buf);
  header->info_ptr = (char *) (header->buf + MSG_HEADER_MSG_SIZE);

  *(header->msg_body_size_ptr) = 0;

  cas_status_info_init (header->info_ptr);

  server_nodeid = db_server_shard_nodeid ();
  server_nodeid = htons (server_nodeid);
  memcpy (header->info_ptr + CAS_STATUS_INFO_IDX_SERVER_NODEID, &server_nodeid, 2);

  shard_info_ver = cas_shard_info_version ();
  shard_info_ver = net_htoni64 (shard_info_ver);
  memcpy (header->info_ptr + CAS_STATUS_INFO_IDX_SHARD_INFO_VER, &shard_info_ver, 8);
}


int
net_write_int (SOCKET sock_fd, int value)
{
  value = htonl (value);

  return (write_buffer (sock_fd, (const char *) (&value), 4));
}

int
net_read_int (SOCKET sock_fd, int *value)
{
  if (net_read_stream (sock_fd, (char *) value, 4) < 0)
    return (-1);

  *value = ntohl (*value);
  return 0;
}

int
net_decode_str (char *msg, int msg_size, char *func_code, void ***ret_argv)
{
  int remain_size = msg_size;
  char *cur_p = msg;
  char *argp;
  int i_val;
  void **argv = NULL;
  int argc = 0;
  int alloc_argc = 0;

  *ret_argv = (void **) NULL;

  if (remain_size < 1)
    return CAS_ER_COMMUNICATION;

  *func_code = *cur_p;
  cur_p += 1;
  remain_size -= 1;

  while (remain_size > 0)
    {
      if (remain_size < 4)
        {
          RYE_FREE_MEM (argv);
          return CAS_ER_COMMUNICATION;
        }
      argp = cur_p;
      memcpy ((char *) &i_val, cur_p, 4);
      i_val = ntohl (i_val);
      remain_size -= 4;
      cur_p += 4;

      if (remain_size < i_val)
        {
          RYE_FREE_MEM (argv);
          return CAS_ER_COMMUNICATION;
        }

      argc++;
      if (argc > alloc_argc)
        {
          alloc_argc += 10;
          argv = (void **) RYE_REALLOC (argv, sizeof (void *) * alloc_argc);
          if (argv == NULL)
            {
              return CAS_ER_NO_MORE_MEMORY;
            }
        }

      argv[argc - 1] = argp;

      cur_p += i_val;
      remain_size -= i_val;
    }

  *ret_argv = argv;
  return argc;
}

void
net_timeout_set (int timeout_sec)
{
  net_timeout = timeout_sec;
}

static int
read_buffer (SOCKET sock_fd, char *buf, int size)
{
  int read_len = -1;
#if defined(ASYNC_MODE)
  struct pollfd po[2] = { {0, 0, 0}, {0, 0, 0} };
  int timeout, po_size, n;
#endif /* ASYNC_MODE */

  unset_net_timeout_flag ();
  if (net_error_flag)
    {
      return -1;
    }

#if defined(ASYNC_MODE)
  timeout = net_timeout < 0 ? -1 : net_timeout * 1000;

  po[0].fd = sock_fd;
  po[0].events = POLLIN;
  po_size = 1;

  if (!IS_INVALID_SOCKET (new_Req_sock_fd))
    {
      po[1].fd = new_Req_sock_fd;
      po[1].events = POLLIN;
      po_size = 2;
    }

retry_poll:
  n = poll (po, po_size, timeout);
  if (n < 0)
    {
      if (errno == EINTR)
        {
          goto retry_poll;
        }
      else
        {
          net_error_flag = 1;
          return -1;
        }
    }
  else if (n == 0)
    {
      /* TIMEOUT */
      set_net_timeout_flag ();
      return -1;
    }
  else
    {
      if (!IS_INVALID_SOCKET (new_Req_sock_fd) && (po[1].revents & POLLIN))
        {
          /* CHANGE CLIENT */
          return -1;
        }
      if (po[0].revents & POLLERR || po[0].revents & POLLHUP)
        {
          read_len = -1;
        }
      else if (po[0].revents & POLLIN)
        {
#endif /* ASYNC_MODE */
          /* RECEIVE NEW REQUEST */
          read_len = READ_FROM_SOCKET (sock_fd, buf, size);
#if defined(ASYNC_MODE)
        }
    }
#endif /* ASYNC_MODE */

  if (read_len <= 0)
    {
      net_error_flag = 1;
    }
  return read_len;
}

static int
write_buffer (SOCKET sock_fd, const char *buf, int size)
{
  int write_len = -1;
#ifdef ASYNC_MODE
  struct pollfd po[1] = { {0, 0, 0} };
  int timeout, n;

  timeout = net_timeout < 0 ? -1 : net_timeout * 1000;
#endif /* ASYNC_MODE */

  if (net_error_flag || IS_INVALID_SOCKET (sock_fd))
    {
      return -1;
    }

#ifdef ASYNC_MODE
  po[0].fd = sock_fd;
  po[0].events = POLLOUT;

retry_poll:
  n = poll (po, 1, timeout);
  if (n < 0)
    {
      if (errno == EINTR)
        {
          goto retry_poll;
        }
      else
        {
          net_error_flag = 1;
          return -1;
        }
    }
  else if (n == 0)
    {
      /* TIMEOUT */
      net_error_flag = 1;
      return -1;
    }
  else
    {
      if (po[0].revents & POLLERR || po[0].revents & POLLHUP)
        {
          write_len = -1;
        }
      else if (po[0].revents & POLLOUT)
        {
#endif /* ASYNC_MODE */
          write_len = WRITE_TO_SOCKET (sock_fd, buf, size);
#if defined(ASYNC_MODE)
        }
    }
#endif /* ASYNC_MODE */

  if (write_len <= 0)
    {
      net_error_flag = 1;
    }
  return write_len;
}

bool
is_net_timed_out (void)
{
  return net_timeout_flag;
}

static void
set_net_timeout_flag (void)
{
  net_timeout_flag = true;
}

static void
unset_net_timeout_flag (void)
{
  net_timeout_flag = false;
}

void
net_write_error (int sock, int cas_info_size, int indicator, int code, char *msg)
{
  size_t len;
  size_t err_msg_len = 0;
  char result = ERROR_RESPONSE;
  MSG_HEADER cas_msg_header;

  assert (code < 0);

  err_msg_len = (msg == NULL ? 0 : strlen (msg) + 1);

  len = NET_SIZE_BYTE + NET_SIZE_INT * 3;
  len += err_msg_len;

  init_msg_header (&cas_msg_header);

  net_write_int (sock, len);

  net_write_stream (sock, cas_msg_header.info_ptr, cas_info_size);

  net_write_stream (sock, &result, 1);

  net_write_int (sock, indicator);

  net_write_int (sock, code);

  net_write_int (sock, err_msg_len);

  net_write_stream (sock, msg, err_msg_len);
}
