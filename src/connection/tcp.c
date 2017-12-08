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
 * tcp.c - Open a TCP connection
 */

#ident "$Id$"

#include "config.h"

#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>
#include <signal.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/param.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/ip_icmp.h>
#include <arpa/inet.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <netinet/tcp.h>
#include <assert.h>

#include "porting.h"
#if defined(SERVER_MODE)
#include "connection_error.h"
#include "connection_sr.h"
#else /* SERVER_MODE */
#include "connection_cl.h"
#endif /* SERVER_MODE */
#include "error_manager.h"
#include "connection_globals.h"
#include "system_parameter.h"
#include "environment_variable.h"
#include "tcp.h"
#include "cas_cci_internal.h"

#define HOST_ID_ARRAY_SIZE 8	/* size of the host_id string */
#define TCP_MIN_NUM_RETRIES 3
#if !defined(INADDR_NONE)
#define INADDR_NONE 0xffffffff
#endif /* !INADDR_NONE */

#define SEND_FD_CONTROL_LEN (sizeof(struct cmsghdr) + sizeof(int))
typedef struct
{
  int int_val;
  struct timeval recv_time;
} SEND_FD_SENDMSG;

static const int css_Maximum_server_count = 50;

#define SET_NONBLOCKING(fd) { \
      int flags = fcntl (fd, F_GETFL); \
      flags |= O_NONBLOCK; \
      fcntl (fd, F_SETFL, flags); \
}

static void css_sockopt (SOCKET sd);
static int css_sockaddr (const PRM_NODE_INFO * node_info, int connect_type,
			 const char *dbname, struct sockaddr *saddr,
			 socklen_t * slen);

static void
css_get_domain_path_internal (char *path_buf, int buf_len,
			      const char *name, const char *ext)
{
  char filename[PATH_MAX];

  assert (name != NULL);
  assert (ext != NULL);

  snprintf (filename, PATH_MAX, "%s.%s", name, ext);
  envvar_socket_file (path_buf, buf_len, filename);

  er_log_debug (ARG_FILE_LINE, "sock_path=%s\n", path_buf);
}

void
css_get_master_domain_path (char *path_buf, int buf_len, bool is_lock_file)
{
  if (is_lock_file)
    {
      css_get_domain_path_internal (path_buf, buf_len, "rye_master", "lock");
    }
  else
    {
      css_get_domain_path_internal (path_buf, buf_len, "rye_master", "sock");
    }
}

void
css_get_server_domain_path (char *path_buf, int buf_len, const char *dbname)
{
  css_get_domain_path_internal (path_buf, buf_len, "rye_server", dbname);
}

static void
css_sockopt (SOCKET sd)
{
  int bool_value = 1;

  if (prm_get_integer_value (PRM_ID_TCP_RCVBUF_SIZE) > 0)
    {
      setsockopt (sd, SOL_SOCKET, SO_RCVBUF,
		  (int *) prm_get_value (PRM_ID_TCP_RCVBUF_SIZE),
		  sizeof (int));
    }

  if (prm_get_integer_value (PRM_ID_TCP_SNDBUF_SIZE) > 0)
    {
      setsockopt (sd, SOL_SOCKET, SO_SNDBUF,
		  (int *) prm_get_value (PRM_ID_TCP_SNDBUF_SIZE),
		  sizeof (int));
    }

  if (prm_get_bool_value (PRM_ID_TCP_NODELAY))
    {
      setsockopt (sd, IPPROTO_TCP, TCP_NODELAY,
		  (const char *) &bool_value, sizeof (bool_value));
    }

  if (prm_get_bool_value (PRM_ID_TCP_KEEPALIVE))
    {
      setsockopt (sd, SOL_SOCKET, SO_KEEPALIVE,
		  (const char *) &bool_value, sizeof (bool_value));
    }
}

/*
 * css_sockaddr()
 */
static int
css_sockaddr (const PRM_NODE_INFO * node_info, int connect_type,
	      const char *dbname, struct sockaddr *saddr, socklen_t * slen)
{
  if (prm_is_myself_node_info (node_info))
    {
      struct sockaddr_un unix_saddr;
      char sock_path[PATH_MAX];

      if (connect_type == SVR_CONNECT_TYPE_TO_SERVER ||
	  connect_type == SVR_CONNECT_TYPE_TRANSFER_CONN)
	{
	  css_get_server_domain_path (sock_path, sizeof (sock_path), dbname);
	}
      else
	{
	  css_get_master_domain_path (sock_path, sizeof (sock_path), false);
	}

      memset ((void *) &unix_saddr, 0, sizeof (unix_saddr));
      unix_saddr.sun_family = AF_UNIX;
      strncpy (unix_saddr.sun_path, sock_path,
	       sizeof (unix_saddr.sun_path) - 1);
      *slen = sizeof (unix_saddr);
      memcpy ((void *) saddr, (void *) &unix_saddr, *slen);

      return AF_UNIX;
    }
  else
    {
      struct sockaddr_in tcp_saddr;
      in_addr_t in_addr = PRM_NODE_INFO_GET_IP (node_info);
      int port = PRM_NODE_INFO_GET_PORT (node_info);

      memset ((void *) &tcp_saddr, 0, sizeof (tcp_saddr));
      tcp_saddr.sin_family = AF_INET;
      tcp_saddr.sin_port = htons (port);
      memcpy (&tcp_saddr.sin_addr, &in_addr, sizeof (in_addr));

      *slen = sizeof (tcp_saddr);
      memcpy ((void *) saddr, (void *) &tcp_saddr, *slen);

      return AF_INET;
    }
}

/*
 * css_tcp_client_open () -
 */
SOCKET
css_tcp_client_open (const PRM_NODE_INFO * node_info, int connect_type,
		     const char *dbname, int timeout)
{
  SOCKET sd = -1;
  struct sockaddr *saddr;
  socklen_t slen;
  int n;
  struct pollfd po[1] = { {0, 0, 0} };
  union
  {
    struct sockaddr_in in;
    struct sockaddr_un un;
  } saddr_buf;

  if (timeout < 0)
    {
      timeout = 5000;
    }

  saddr = (struct sockaddr *) &saddr_buf;
  if (css_sockaddr (node_info, connect_type, dbname, saddr, &slen) == AF_INET)
    {
      T_HOST_INFO cci_host_info;
      in_addr_t ip;
      ip = PRM_NODE_INFO_GET_IP (node_info);
      memcpy (cci_host_info.ip_addr, &ip, sizeof (in_addr_t));
      cci_host_info.port = PRM_NODE_INFO_GET_PORT (node_info);
      sd = cci_mgmt_connect_db_server (&cci_host_info, dbname, timeout);
    }
  else
    {
      sd = socket (saddr->sa_family, SOCK_STREAM, 0);
    }

  if (sd < 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ERR_CSS_TCP_CANNOT_CREATE_SOCKET, 0);
      return INVALID_SOCKET;
    }
  else
    {
      css_sockopt (sd);
      SET_NONBLOCKING (sd);
    }

again_eintr:
  n = connect (sd, saddr, slen);
  if (n == 0)
    {
      /* connection is established immediately */
      return sd;
    }
  if (errno == EINTR)
    {
      goto again_eintr;
    }

  if (errno != EINPROGRESS)
    {
      close (sd);
      er_log_debug (ARG_FILE_LINE, "css_tcp_client_open :"
		    "connect failed with errno %d", errno);
      return INVALID_SOCKET;
    }

retry_poll:
  po[0].fd = sd;
  po[0].events = POLLOUT;
  po[0].revents = 0;
  n = poll (po, 1, timeout);
  if (n < 0)
    {
      if (errno == EINTR)
	{
	  goto retry_poll;
	}

      er_log_debug (ARG_FILE_LINE, "css_tcp_client_open :"
		    "poll failed errno %d", errno);
      close (sd);
      return INVALID_SOCKET;
    }
  else if (n == 0)
    {
      /* 0 means it timed out and no fd is changed */
      errno = ETIMEDOUT;
      close (sd);
      er_log_debug (ARG_FILE_LINE, "css_tcp_client_open :"
		    "poll failed with timeout %d", timeout);
      return INVALID_SOCKET;
    }

  /* has connection been established? */
  slen = sizeof (n);
  if (getsockopt (sd, SOL_SOCKET, SO_ERROR, (void *) &n, &slen) < 0)
    {
      er_log_debug (ARG_FILE_LINE, "css_tcp_client_open :"
		    "getsockopt failed errno %d", errno);
      close (sd);
      return INVALID_SOCKET;
    }
  if (n != 0)
    {
      er_log_debug (ARG_FILE_LINE, "css_tcp_client_open :"
		    "connection failed errno %d", n);
      close (sd);
      return INVALID_SOCKET;
    }

  return sd;
}

/*
 * css_tcp_master_open () -
 *   return:
 *   port(in):
 *   sockfd(in):
 */
int
css_tcp_master_open (SOCKET * res_sockfd)
{
  struct sockaddr_un unix_srv_addr;
  int retry_count = 0;
  int reuseaddr_flag = 1;
  struct stat unix_socket_stat;
  char sock_path[PATH_MAX];
  SOCKET sock_fd;

  *res_sockfd = INVALID_SOCKET;

  css_get_master_domain_path (sock_path, PATH_MAX, false);

  unix_srv_addr.sun_family = AF_UNIX;
  strncpy (unix_srv_addr.sun_path, sock_path,
	   sizeof (unix_srv_addr.sun_path) - 1);

  if (access (sock_path, F_OK) == 0)
    {
      if (stat (sock_path, &unix_socket_stat) == -1)
	{
	  /* stat() failed */
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ERR_CSS_UNIX_DOMAIN_SOCKET_FILE_EXIST, 1,
			       sock_path);
	  return ERR_CSS_UNIX_DOMAIN_SOCKET_FILE_EXIST;
	}
      if (!S_ISSOCK (unix_socket_stat.st_mode))
	{
	  /* not socket file */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ERR_CSS_UNIX_DOMAIN_SOCKET_FILE_EXIST, 1, sock_path);
	  return ERR_CSS_UNIX_DOMAIN_SOCKET_FILE_EXIST;
	}
      if (unlink (sock_path) == -1)
	{
	  /* unlink() failed */
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ERR_CSS_UNIX_DOMAIN_SOCKET_FILE_EXIST, 1,
			       sock_path);
	  return ERR_CSS_UNIX_DOMAIN_SOCKET_FILE_EXIST;
	}
    }

retry2:

  sock_fd = socket (AF_UNIX, SOCK_STREAM, 0);
  if (IS_INVALID_SOCKET (sock_fd))
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ERR_CSS_TCP_CANNOT_CREATE_STREAM, 0);
      return ERR_CSS_TCP_CANNOT_CREATE_STREAM;
    }

  if (setsockopt (sock_fd, SOL_SOCKET, SO_REUSEADDR,
		  (char *) &reuseaddr_flag, sizeof (reuseaddr_flag)) < 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ERR_CSS_TCP_BIND_ABORT, 0);
      css_shutdown_socket (sock_fd);
      return ERR_CSS_TCP_BIND_ABORT;
    }

  if (bind (sock_fd, (struct sockaddr *) &unix_srv_addr,
	    sizeof (unix_srv_addr)) < 0)
    {
      if (errno == EADDRINUSE && retry_count <= 5)
	{
	  retry_count++;
	  css_shutdown_socket (sock_fd);
	  (void) sleep (1);
	  goto retry2;
	}
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ERR_CSS_TCP_BIND_ABORT, 0);
      css_shutdown_socket (sock_fd);
      return ERR_CSS_TCP_BIND_ABORT;
    }

  if (listen (sock_fd, css_Maximum_server_count) != 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ERR_CSS_TCP_ACCEPT_ERROR, 0);
      css_shutdown_socket (sock_fd);
      return ERR_CSS_TCP_ACCEPT_ERROR;
    }

  ioctl (sock_fd, FIOCLEX, 0);

  *res_sockfd = sock_fd;
  return NO_ERROR;
}

/*
 * css_master_accept() - master accept of a request from a client
 *   return:
 *   sockfd(in):
 */
SOCKET
css_master_accept (SOCKET sockfd)
{
  struct sockaddr sa;
  static SOCKET new_sockfd;
  socklen_t clilen;
  int boolean = 1;

  while (true)
    {
      clilen = sizeof (sa);
      new_sockfd = accept (sockfd, &sa, &clilen);

      if (IS_INVALID_SOCKET (new_sockfd))
	{
	  if (errno == EINTR)
	    {
	      errno = 0;
	      continue;
	    }

	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ERR_CSS_TCP_ACCEPT_ERROR, 0);
	  return INVALID_SOCKET;
	}

      break;
    }

  if (sa.sa_family == AF_INET)
    {
      setsockopt (sockfd, IPPROTO_TCP, TCP_NODELAY, (char *) &boolean,
		  sizeof (boolean));
    }

  return new_sockfd;
}

/*
 * css_tcp_setup_server_datagram() - server datagram open support
 *   return:
 *   pathname(in):
 *   sockfd(in):
 *
 * Note: This will let the master server open a unix domain socket to the
 *       server to pass internet domain socket fds to the server. It returns
 *       the new socket fd
 */
bool
css_tcp_setup_server_datagram (char *pathname, SOCKET * sockfd)
{
  int servlen;
  struct sockaddr_un serv_addr;

  unlink (pathname);

  *sockfd = socket (AF_UNIX, SOCK_STREAM, 0);
  if (IS_INVALID_SOCKET (*sockfd))
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ERR_CSS_TCP_DATAGRAM_SOCKET, 0);
      return false;
    }

  memset ((void *) &serv_addr, 0, sizeof (serv_addr));
  serv_addr.sun_family = AF_UNIX;
  strncpy (serv_addr.sun_path, pathname, sizeof (serv_addr.sun_path) - 1);
  servlen = strlen (pathname) + 1 + sizeof (serv_addr.sun_family);

  if (bind (*sockfd, (struct sockaddr *) &serv_addr, servlen) < 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ERR_CSS_TCP_DATAGRAM_BIND, 0);
      return false;
    }

  /*
   * some operating system does not set the permission for unix domain socket.
   * so a server can't connect to master which is initiated by other user.
   */
#if defined(LINUX)
  chmod (pathname, S_IRWXU | S_IRWXG | S_IRWXO);
#endif /* LINUX */

  if (listen (*sockfd, 5) != 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ERR_CSS_TCP_ACCEPT_ERROR, 0);
      return false;
    }

  return true;
}

/*
 * css_tcp_master_datagram() - master side of the datagram interface
 *   return:
 *   path_name(in):
 *   sockfd(in):
 */
bool
css_tcp_master_datagram (char *path_name, SOCKET * sockfd)
{
  int servlen;
  struct sockaddr_un serv_addr;
  bool will_retry = true;
  int success = -1;
  int num_retries = 0;

  memset ((void *) &serv_addr, 0, sizeof (serv_addr));
  serv_addr.sun_family = AF_UNIX;
  strncpy (serv_addr.sun_path, path_name, sizeof (serv_addr.sun_path) - 1);
  serv_addr.sun_path[sizeof (serv_addr.sun_path) - 1] = '\0';
  servlen = strlen (serv_addr.sun_path) + 1 + sizeof (serv_addr.sun_family);

  do
    {
      /*
       * If we get an ECONNREFUSED from the connect, we close the socket, and
       * retry again. This is needed since the backlog parameter of the SUN
       * machine is too small (See man page of listen...see BUG section).
       * To avoid a possible infinite loop, we only retry few times
       */
      *sockfd = socket (AF_UNIX, SOCK_STREAM, 0);
      if (IS_INVALID_SOCKET (*sockfd))
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ERR_CSS_TCP_DATAGRAM_SOCKET, 0);
	  return false;
	}

    again_eintr:
      success = connect (*sockfd, (struct sockaddr *) &serv_addr, servlen);
      if (success < 0)
	{
	  if (errno == EINTR)
	    {
	      goto again_eintr;
	    }

	  if (errno == ECONNREFUSED || errno == ETIMEDOUT)
	    {

	      if (num_retries > TCP_MIN_NUM_RETRIES)
		{
		  will_retry = false;
		}
	      else
		{
		  will_retry = true;
		  num_retries++;
		}
	    }
	  else
	    {
	      will_retry = false;	/* Don't retry */
	    }

	  close (*sockfd);
	  *sockfd = INVALID_SOCKET;
	  (void) sleep (1);
	  continue;
	}
      break;
    }
  while (success < 0 && will_retry == true);


  if (success < 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ERR_CSS_TCP_DATAGRAM_CONNECT, 0);
#if defined(RYE_DEBUG)
      er_log_debug (ARG_FILE_LINE,
		    "Failed with number of retries = %d during connection\n",
		    num_retries);
#endif /* RYE_DEBUG */
      return false;
    }

  if (num_retries > 0)
    {
#if defined(RYE_DEBUG)
      er_log_debug (ARG_FILE_LINE,
		    "Connected after number of retries = %d\n", num_retries);
#endif /* RYE_DEBUG */
    }

  return true;
}

/*
 * css_open_new_socket_from_master() - the message interface to the master
 *                                     server
 *   return:
 *   fd(in):
 *   rid(in):
 */
SOCKET
css_recv_fd (SOCKET fd, int *int_val, struct timeval * recv_time)
{
  int new_fd = 0, rc;
  struct iovec iov[1];
  struct msghdr msg;
  int pid;
  union
  {
    struct cmsghdr cm;
    char control[SEND_FD_CONTROL_LEN];
  } control_un;
  struct cmsghdr *cmptr = NULL;
  SEND_FD_SENDMSG send_msg;
  int *dataptr;

  iov[0].iov_base = (char *) &send_msg;
  iov[0].iov_len = sizeof (SEND_FD_SENDMSG);
  msg.msg_iov = iov;
  msg.msg_iovlen = 1;
  msg.msg_name = (caddr_t) NULL;
  msg.msg_namelen = 0;
  msg.msg_control = control_un.control;
  msg.msg_controllen = SEND_FD_CONTROL_LEN;
  msg.msg_flags = 0;
  cmptr = CMSG_FIRSTHDR (&msg);
  rc = recvmsg (fd, &msg, 0);

  if (rc < (int) SEND_FD_CONTROL_LEN)
    {
#ifdef _DEBUG
      printf ("recvmsg failed. errno = %d. str=%s\n", errno,
	      strerror (errno));
#endif
      return INVALID_SOCKET;
    }

  *int_val = send_msg.int_val;
  if (recv_time)
    {
      *recv_time = send_msg.recv_time;
    }

  pid = getpid ();
  dataptr = (int *) CMSG_DATA (cmptr);
  new_fd = *dataptr;

#ifdef SYSV
  ioctl (new_fd, SIOCSPGRP, (caddr_t) & pid);
#elif !defined(VMS)
  fcntl (new_fd, F_SETOWN, pid);
#endif

  return (new_fd);
}

/*
 * css_transfer_fd() - send the fd of a new client request to a server
 *   return:
 *   server_fd(in):
 *   client_fd(in):
 *   rid(in):
 */
int
css_transfer_fd (SOCKET server_fd, SOCKET client_fd, int int_val,
		 const struct timeval *recv_time)
{
  struct iovec iov[1];
  struct msghdr msg;
  int num_bytes;
  union
  {
    struct cmsghdr cm;
    char control[SEND_FD_CONTROL_LEN];
  } control_un;
  struct cmsghdr *cmptr;
  SEND_FD_SENDMSG send_msg;
  int *dataptr;
  struct timeval tmp_timeval;

  if (recv_time == NULL)
    {
      gettimeofday (&tmp_timeval, NULL);
      recv_time = &tmp_timeval;
    }

  /* set send message */
  send_msg.int_val = int_val;
  send_msg.recv_time = *recv_time;

  /* Pass the fd to the server */
  iov[0].iov_base = (char *) &send_msg;
  iov[0].iov_len = sizeof (SEND_FD_SENDMSG);
  msg.msg_iov = iov;
  msg.msg_iovlen = 1;
  msg.msg_namelen = 0;
  msg.msg_name = (caddr_t) 0;
  msg.msg_control = control_un.control;
  msg.msg_controllen = SEND_FD_CONTROL_LEN;
  msg.msg_flags = 0;

  cmptr = CMSG_FIRSTHDR (&msg);
  cmptr->cmsg_level = SOL_SOCKET;
  cmptr->cmsg_type = SCM_RIGHTS;
  cmptr->cmsg_len = SEND_FD_CONTROL_LEN;
  dataptr = (int *) CMSG_DATA (cmptr);
  *dataptr = client_fd;

  num_bytes = sendmsg (server_fd, &msg, 0);

  if (num_bytes < (int) SEND_FD_CONTROL_LEN)
    {
      return (-1);
    }
  return (num_bytes);
}

/*
 * css_shutdown_socket() -
 *   return:
 *   fd(in):
 */
void
css_shutdown_socket (SOCKET fd)
{
  int rc;

  if (!IS_INVALID_SOCKET (fd))
    {
    again_eintr:
      rc = close (fd);
      if (rc != 0)
	{
	  if (errno == EINTR)
	    {
	      goto again_eintr;
	    }
#if defined(RYE_DEBUG)
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_GENERIC_ERROR, 1, "");
#endif /* RYE_DEBUG */
	}
    }
}

int
css_get_max_socket_fds (void)
{
  return (int) sysconf (_SC_OPEN_MAX);
}

/*
 * css_peer_alive() - check if the peer is alive or not
 *                    Try to ping the peer or connect to the port 7 (ECHO)
 *    return: true or false
 *    sd(in): socket descriptor connected to the peer
 *    timeout(in): timeout in mili seconds
 */
bool
css_peer_alive (SOCKET sd, int timeout)
{
  SOCKET nsd;
  int n;
  socklen_t size;
  struct sockaddr_in saddr;
  socklen_t slen;
  struct pollfd po[1];

#if defined (CS_MODE)
  er_log_debug (ARG_FILE_LINE, "The css_peer_alive() is calling.");
#endif

  slen = sizeof (saddr);
  if (getpeername (sd, (struct sockaddr *) &saddr, &slen) < 0)
    {
      er_log_debug (ARG_FILE_LINE,
		    "css_peer_alive: returning errno %d from getpeername()\n",
		    errno);
      return false;
    }

  /* if Unix domain socket, the peer(=local) is alive always */
  if (saddr.sin_family != AF_INET)
    {
      return true;
    }

#if 0				/* temporarily disabled */
  /* try to make raw socket to ping the peer */
  if ((nsd = socket (AF_INET, SOCK_RAW, IPPROTO_ICMP)) >= 0)
    {
      return (css_ping (nsd, &saddr, timeout) == 0);
    }
#endif
  /* failed to make a ICMP socket; try to connect to the port ECHO */
  if ((nsd = socket (AF_INET, SOCK_STREAM, 0)) < 0)
    {
      er_log_debug (ARG_FILE_LINE,
		    "css_peer_alive: errno %d from socket(SOCK_STREAM)\n",
		    errno);
      return false;
    }

  /* make the socket non blocking so we can use select */
  SET_NONBLOCKING (nsd);

  saddr.sin_port = htons (7);	/* port ECHO */
  n = connect (nsd, (struct sockaddr *) &saddr, slen);

  /*
   * Connection will be established or refused immediately.
   * Either way it means that the peer host is alive.
   */
  if (n == 0 || (n < 0 && errno == ECONNREFUSED))
    {
      close (nsd);
      return true;
    }

  switch (errno)
    {
    case EINPROGRESS:		/* non-blocking, asynchronously */
      break;
    case ENETUNREACH:		/* network unreachable */
    case EAFNOSUPPORT:		/* address family not supported */
    case EADDRNOTAVAIL:	/* address is not available on the remote machine */
    case EINVAL:		/* on some linux, connecting to the loopback */
      er_log_debug (ARG_FILE_LINE,
		    "css_peer_alive: errno %d from connect()\n", errno);
      close (nsd);
      return false;
    default:			/* otherwise, connection failed */
      er_log_debug (ARG_FILE_LINE,
		    "css_peer_alive: errno %d from connect()\n", errno);
      close (nsd);
      return false;
    }

retry_poll:
  po[0].fd = nsd;
  po[0].events = POLLOUT;
  po[0].revents = 0;
  n = poll (po, 1, timeout);
  if (n < 0)
    {
      if (errno == EINTR)
	{
	  goto retry_poll;
	}
      er_log_debug (ARG_FILE_LINE, "css_peer_alive: errno %d from poll()\n",
		    errno);
      close (nsd);
      return false;
    }
  else if (n == 0)
    {
      er_log_debug (ARG_FILE_LINE, "css_peer_alive: timed out %d\n", timeout);
      close (nsd);
      return false;
    }

  /* has connection been established? */
  size = sizeof (n);
  if (getsockopt (nsd, SOL_SOCKET, SO_ERROR, (void *) &n, &size) < 0)
    {
      er_log_debug (ARG_FILE_LINE,
		    "css_peer_alive: getsockopt() return error %d\n", errno);
      close (nsd);
      return false;
    }

  if (n == 0 || n == ECONNREFUSED)
    {
      close (nsd);
      return true;
    }

  er_log_debug (ARG_FILE_LINE, "css_peer_alive: errno %d from connect()\n",
		n);
  close (nsd);
  return false;
}

/*
 * css_get_peer_name() - get the hostname of the peer socket
 *   return: 0 if success; otherwise errno
 *   hostname(in): buffer for hostname
 *   len(in): size of the hostname buffer
 */
int
css_get_peer_name (SOCKET sockfd, char *hostname, size_t len)
{
  union
  {
    struct sockaddr_in in;
    struct sockaddr_un un;
  } saddr_buf;
  struct sockaddr *saddr;
  socklen_t saddr_len;

  saddr = (struct sockaddr *) &saddr_buf;
  saddr_len = sizeof (saddr_buf);
  if (getpeername (sockfd, saddr, &saddr_len) != 0)
    {
      return errno;
    }
  return getnameinfo (saddr, saddr_len, hostname, len, NULL, 0, NI_NOFQDN);
}

int
css_ip_to_str (char *buf, int size, in_addr_t ip)
{
  unsigned char *p = (unsigned char *) &ip;
  return snprintf (buf, size, "%d.%d.%d.%d", p[0], p[1], p[2], p[3]);
}
