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

#define HOST_ID_ARRAY_SIZE 8	/* size of the host_id string */
#define TCP_MIN_NUM_RETRIES 3
#define CONTROLLEN (sizeof(struct cmsghdr) + sizeof(int))
#if !defined(INADDR_NONE)
#define INADDR_NONE 0xffffffff
#endif /* !INADDR_NONE */

static const int css_Maximum_server_count = 50;

#define SET_NONBLOCKING(fd) { \
      int flags = fcntl (fd, F_GETFL); \
      flags |= O_NONBLOCK; \
      fcntl (fd, F_SETFL, flags); \
}

static void css_sockopt (SOCKET sd);
static int css_sockaddr (const char *host, int port, struct sockaddr *saddr,
			 socklen_t * slen);

void
css_get_master_domain_path (char *path_buf, int buf_len)
{
  char sock_dir[PATH_MAX];

  envvar_vardir_file (sock_dir, PATH_MAX, "RYE_SOCK");
  snprintf (path_buf, buf_len, "%s/%s%d", sock_dir, envvar_prefix (),
	    prm_get_master_port_id ());

  er_log_debug (ARG_FILE_LINE, "sock_path=%s\n", path_buf);
}

/*
 * css_tcp_client_open () -
 *   return:
 *   host(in):
 *   port(in):
 */
SOCKET
css_tcp_client_open (const char *host, int port)
{
  SOCKET fd;

  fd = css_tcp_client_open_with_retry (host, port, true);
  if (IS_INVALID_SOCKET (fd))
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ERR_CSS_TCP_CANNOT_CONNECT_TO_MASTER, 1, host);
    }
  return fd;
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

in_addr_t
css_host_ip_addr ()
{
  const char *host_myself;

  host_myself = prm_get_string_value (PRM_ID_HA_NODE_MYSELF);

  if (host_myself == NULL || host_myself[0] == '\0')
    {
      char host_name[MAXHOSTNAMELEN];

      if (GETHOSTNAME (host_name, sizeof (host_name)))
	{
	  return INADDR_NONE;
	}

      return hostname_to_ip (host_name);
    }
  else
    {
      return inet_addr (host_myself);
    }
}

/*
 * css_sockaddr()
 *   return:
 *   host(in):
 *   port(in):
 *   saddr(out):
 *   slen(out):
 */
static int
css_sockaddr (const char *host, int port, struct sockaddr *saddr,
	      socklen_t * slen)
{
  in_addr_t in_addr;

  in_addr = hostname_to_ip (host);
  if (in_addr == INADDR_NONE)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ERR_CSS_TCP_HOST_NAME_ERROR, 1, host);
      return ERR_CSS_TCP_HOST_NAME_ERROR;
    }

  /*
   * Compare with the TCP address with localhost.
   * If it is, use Unix domain socket rather than TCP for the performance
   */
  if (in_addr == inet_addr ("127.0.0.1"))
    {
      struct sockaddr_un unix_saddr;
      char sock_path[PATH_MAX];

      css_get_master_domain_path (sock_path, PATH_MAX);

      memset ((void *) &unix_saddr, 0, sizeof (unix_saddr));
      unix_saddr.sun_family = AF_UNIX;
      strncpy (unix_saddr.sun_path, sock_path,
	       sizeof (unix_saddr.sun_path) - 1);
      *slen = sizeof (unix_saddr);
      memcpy ((void *) saddr, (void *) &unix_saddr, *slen);
    }
  else
    {
      struct sockaddr_in tcp_saddr;

      memset ((void *) &tcp_saddr, 0, sizeof (tcp_saddr));
      tcp_saddr.sin_family = AF_INET;
      tcp_saddr.sin_port = htons (port);
      memcpy (&tcp_saddr.sin_addr, &in_addr, sizeof (in_addr));

      *slen = sizeof (tcp_saddr);
      memcpy ((void *) saddr, (void *) &tcp_saddr, *slen);
    }

  return NO_ERROR;
}

/*
 * css_tcp_client_open_with_retry () -
 *   return:
 *   host(in):
 *   port(in):
 *   willretry(in):
 */
SOCKET
css_tcp_client_open_with_retry (const char *host, int port, bool will_retry)
{
  SOCKET sd = INVALID_SOCKET;
  struct sockaddr *saddr;
  socklen_t slen;
  time_t start_contime;
  int nsecs, sleep_nsecs = 1;
  int success, num_retries = 0;
  union
  {
    struct sockaddr_in in;
    struct sockaddr_un un;
  } saddr_buf;

  assert (host != NULL);
  assert (port > 0);

  saddr = (struct sockaddr *) &saddr_buf;
  if (css_sockaddr (host, port, saddr, &slen) != NO_ERROR)
    {
      return INVALID_SOCKET;
    }

  start_contime = time (NULL);
  do
    {
      sd = socket (saddr->sa_family, SOCK_STREAM, 0);
      if (IS_INVALID_SOCKET (sd))
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ERR_CSS_TCP_CANNOT_CREATE_SOCKET, 0);
	  return INVALID_SOCKET;
	}
      else
	{
	  css_sockopt (sd);
	}

      /*
       * If we get an ECONNREFUSED from the connect, we close the socket, and
       * retry again. This is needed since the backlog parameter of the SUN
       * machine is too small (See man page of listen...see BUG section).
       * To avoid a possible infinite loop, we only retry five times
       */
    again_eintr:
      if ((success = connect (sd, saddr, slen)) == 0)
	{
	  /* connection is established successfully */
	  break;
	}
      if (errno == EINTR)
	{
	  goto again_eintr;
	}

      if ((errno == ECONNREFUSED || errno == ETIMEDOUT) && will_retry == true)
	{
	  nsecs = (int) difftime (time (NULL), start_contime);
	  nsecs -= prm_get_integer_value (PRM_ID_TCP_CONNECTION_TIMEOUT);
	  if (nsecs >= 0 && num_retries > TCP_MIN_NUM_RETRIES)
	    {
	      will_retry = false;
	    }
	  else
	    {
	      /*
	       * Wait a little bit to change the load of the server.
	       * Don't wait for more than 1/2 min or the timeout period
	       */
	      if (sleep_nsecs > 30)
		{
		  sleep_nsecs = 30;
		}

	      /*
	       * Sleep only when we have not timed out. That is, when nsecs is
	       * negative.
	       */
	      if (nsecs < 0 && sleep_nsecs > (-nsecs))
		{
		  sleep_nsecs = -nsecs;
		}
	      if (nsecs < 0)
		{
		  (void) sleep (sleep_nsecs);
		  sleep_nsecs *= 2;	/* Go 1, 2, 4, 8, etc */
		}
	      num_retries++;
	    }
	}
      else
	{
	  will_retry = false;	/* Don't retry */
	}

      /*
       * According to the Sun man page of connect & listen. When a connect
       * was forcefully rejected. The calling program must close the
       * socket descriptor, before another connect is retried.
       *
       * The server's host is probably overloaded. Sleep for a while, then
       * try again. We sleep a different number of seconds between 1 and 30
       * to avoid having the same situation with other processes that could
       * have reached the timeout/refuse connection.
       *
       * The sleep time is guessing that the server is not going to be
       * overloaded by connections in that amount of time.
       *
       * Similar things are suggested by R. Stevens Unix Network programming
       * book. See Remote Command execution example in Chapter 14
       *
       * See connect and listen MAN pages.
       */
      close (sd);
      sd = INVALID_SOCKET;
    }
  while (success < 0 && will_retry == true);

  if (success < 0)
    {
#if defined(RYE_DEBUG)
      er_log_debug (ARG_FILE_LINE, "css_tcp_client_open_with_retry:"
		    "connection failed with retries %d errno %d\n",
		    num_retries, errno);
#endif /* RYE_DEBUG */
      return INVALID_SOCKET;
    }

  return sd;
}

/*
 * css_tcp_client_open_with_timeout() -
 *   return: socket descriptor
 *   host(in): host name
 *   port(in): port no
 *   timeout(in): timeout in milli-seconds
 */
SOCKET
css_tcp_client_open_with_timeout (const char *host, int port, int timeout)
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

  assert (host != NULL);
  assert (port > 0);
  assert (timeout >= 0);

  saddr = (struct sockaddr *) &saddr_buf;
  if (css_sockaddr (host, port, saddr, &slen) != NO_ERROR)
    {
      return INVALID_SOCKET;
    }

  sd = socket (saddr->sa_family, SOCK_STREAM, 0);
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
      er_log_debug (ARG_FILE_LINE, "css_tcp_client_open_with_timeout:"
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

      er_log_debug (ARG_FILE_LINE, "css_tcp_client_open_with_timeout:"
		    "poll failed errno %d", errno);
      close (sd);
      return INVALID_SOCKET;
    }
  else if (n == 0)
    {
      /* 0 means it timed out and no fd is changed */
      errno = ETIMEDOUT;
      close (sd);
      er_log_debug (ARG_FILE_LINE, "css_tcp_client_open_with_timeout:"
		    "poll failed with timeout %d", timeout);
      return INVALID_SOCKET;
    }

  /* has connection been established? */
  slen = sizeof (n);
  if (getsockopt (sd, SOL_SOCKET, SO_ERROR, (void *) &n, &slen) < 0)
    {
      er_log_debug (ARG_FILE_LINE, "css_tcp_client_open_with_timeout:"
		    "getsockopt failed errno %d", errno);
      close (sd);
      return INVALID_SOCKET;
    }
  if (n != 0)
    {
      er_log_debug (ARG_FILE_LINE, "css_tcp_client_open_with_timeout:"
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
css_tcp_master_open (int port, SOCKET * sockfd)
{
  struct sockaddr_in tcp_srv_addr;	/* server's internet socket addr */
  struct sockaddr_un unix_srv_addr;
  int retry_count = 0;
  int reuseaddr_flag = 1;
  struct stat unix_socket_stat;
  char sock_path[PATH_MAX];

  /*
   * We have to create a socket ourselves and bind our well-known address to it.
   */

  memset ((void *) &tcp_srv_addr, 0, sizeof (tcp_srv_addr));
  tcp_srv_addr.sin_family = AF_INET;
  tcp_srv_addr.sin_addr.s_addr = htonl (INADDR_ANY);

  if (port > 0)
    {
      tcp_srv_addr.sin_port = htons (port);
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ERR_CSS_TCP_PORT_ERROR, 0);
      return ERR_CSS_TCP_PORT_ERROR;
    }

  css_get_master_domain_path (sock_path, PATH_MAX);

  unix_srv_addr.sun_family = AF_UNIX;
  strncpy (unix_srv_addr.sun_path, sock_path,
	   sizeof (unix_srv_addr.sun_path) - 1);

  /*
   * Create the socket and Bind our local address so that any
   * client may send to us.
   */

retry:
  /*
   * Allow the new master to rebind the Rye port even if there are
   * clients with open connections from previous masters.
   */

  sockfd[0] = socket (AF_INET, SOCK_STREAM, 0);
  if (IS_INVALID_SOCKET (sockfd[0]))
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ERR_CSS_TCP_CANNOT_CREATE_STREAM, 0);
      return ERR_CSS_TCP_CANNOT_CREATE_STREAM;
    }

  if (setsockopt (sockfd[0], SOL_SOCKET, SO_REUSEADDR,
		  (char *) &reuseaddr_flag, sizeof (reuseaddr_flag)) < 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ERR_CSS_TCP_BIND_ABORT, 0);
      css_shutdown_socket (sockfd[0]);
      return ERR_CSS_TCP_BIND_ABORT;
    }

  if (bind (sockfd[0], (struct sockaddr *) &tcp_srv_addr,
	    sizeof (tcp_srv_addr)) < 0)
    {
      if (errno == EADDRINUSE && retry_count <= 5)
	{
	  retry_count++;
	  css_shutdown_socket (sockfd[0]);
	  (void) sleep (1);
	  goto retry;
	}
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ERR_CSS_TCP_BIND_ABORT, 0);
      css_shutdown_socket (sockfd[0]);
      return ERR_CSS_TCP_BIND_ABORT;
    }

  /*
   * And set the listen parameter, telling the system that we're
   * ready to accept incoming connection requests.
   */
  if (listen (sockfd[0], css_Maximum_server_count) != 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ERR_CSS_TCP_ACCEPT_ERROR, 0);
      css_shutdown_socket (sockfd[0]);
      return ERR_CSS_TCP_ACCEPT_ERROR;
    }

  /*
   * Since the master now forks /M drivers, make sure we do a close
   * on exec on the socket.
   */
  ioctl (sockfd[0], FIOCLEX, 0);

  if (access (sock_path, F_OK) == 0)
    {
      if (stat (sock_path, &unix_socket_stat) == -1)
	{
	  /* stat() failed */
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ERR_CSS_UNIX_DOMAIN_SOCKET_FILE_EXIST, 1,
			       sock_path);
	  css_shutdown_socket (sockfd[0]);
	  return ERR_CSS_UNIX_DOMAIN_SOCKET_FILE_EXIST;
	}
      if (!S_ISSOCK (unix_socket_stat.st_mode))
	{
	  /* not socket file */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ERR_CSS_UNIX_DOMAIN_SOCKET_FILE_EXIST, 1, sock_path);
	  css_shutdown_socket (sockfd[0]);
	  return ERR_CSS_UNIX_DOMAIN_SOCKET_FILE_EXIST;
	}
      if (unlink (sock_path) == -1)
	{
	  /* unlink() failed */
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ERR_CSS_UNIX_DOMAIN_SOCKET_FILE_EXIST, 1,
			       sock_path);
	  css_shutdown_socket (sockfd[0]);
	  return ERR_CSS_UNIX_DOMAIN_SOCKET_FILE_EXIST;
	}
    }

retry2:

  sockfd[1] = socket (AF_UNIX, SOCK_STREAM, 0);
  if (IS_INVALID_SOCKET (sockfd[1]))
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ERR_CSS_TCP_CANNOT_CREATE_STREAM, 0);
      css_shutdown_socket (sockfd[0]);
      return ERR_CSS_TCP_CANNOT_CREATE_STREAM;
    }

  if (setsockopt (sockfd[1], SOL_SOCKET, SO_REUSEADDR,
		  (char *) &reuseaddr_flag, sizeof (reuseaddr_flag)) < 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ERR_CSS_TCP_BIND_ABORT, 0);
      css_shutdown_socket (sockfd[0]);
      css_shutdown_socket (sockfd[1]);
      return ERR_CSS_TCP_BIND_ABORT;
    }

  if (bind (sockfd[1], (struct sockaddr *) &unix_srv_addr,
	    sizeof (unix_srv_addr)) < 0)
    {
      if (errno == EADDRINUSE && retry_count <= 5)
	{
	  retry_count++;
	  css_shutdown_socket (sockfd[1]);
	  (void) sleep (1);
	  goto retry2;
	}
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ERR_CSS_TCP_BIND_ABORT, 0);
      css_shutdown_socket (sockfd[0]);
      css_shutdown_socket (sockfd[1]);
      return ERR_CSS_TCP_BIND_ABORT;
    }

  if (listen (sockfd[1], css_Maximum_server_count) != 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ERR_CSS_TCP_ACCEPT_ERROR, 0);
      css_shutdown_socket (sockfd[0]);
      css_shutdown_socket (sockfd[1]);
      return ERR_CSS_TCP_ACCEPT_ERROR;
    }

  ioctl (sockfd[1], FIOCLEX, 0);

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
 * css_tcp_listen_server_datagram() - verifies that the pipe to the master has
 *                                    been setup properly
 *   return:
 *   sockfd(in):
 *   newfd(in):
 */
bool
css_tcp_listen_server_datagram (SOCKET sockfd, SOCKET * newfd)
{
  socklen_t clilen;
  struct sockaddr_un cli_addr;
  int boolean = 1;

  clilen = sizeof (cli_addr);

  while (true)
    {
      *newfd = accept (sockfd, (struct sockaddr *) &cli_addr, &clilen);
      if (IS_INVALID_SOCKET (*newfd) < 0)
	{
	  if (errno == EINTR)
	    {
	      continue;
	    }

	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ERR_CSS_TCP_DATAGRAM_ACCEPT, 0);
	  return false;
	}

      break;
    }

  setsockopt (sockfd, IPPROTO_TCP, TCP_NODELAY, (char *) &boolean,
	      sizeof (boolean));

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
css_open_new_socket_from_master (SOCKET fd, unsigned short *rid)
{
  unsigned short req_id;
  SOCKET new_fd = INVALID_SOCKET;
  int rc;
  struct iovec iov[1];
  struct msghdr msg;
  int pid;
  static struct cmsghdr *cmptr = NULL;
  SOCKET *dataptr;

  iov[0].iov_base = (char *) &req_id;
  iov[0].iov_len = sizeof (unsigned short);
  msg.msg_iov = iov;
  msg.msg_iovlen = 1;
  msg.msg_name = (caddr_t) NULL;
  msg.msg_namelen = 0;
  if (cmptr == NULL
      && (cmptr = (struct cmsghdr *) malloc (CONTROLLEN)) == NULL)
    {
      return INVALID_SOCKET;
    }
  msg.msg_control = (void *) cmptr;
  msg.msg_controllen = CONTROLLEN;

  rc = recvmsg (fd, &msg, 0);
  if (rc < 0)
    {
      TPRINTF ("recvmsg failed for fd = %d\n", rc);
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ERR_CSS_TCP_RECVMSG, 0);
      return INVALID_SOCKET;
    }

  *rid = ntohs (req_id);

  pid = getpid ();
  dataptr = (SOCKET *) CMSG_DATA (cmptr);
  new_fd = *dataptr;

#ifdef SYSV
  ioctl (new_fd, SIOCSPGRP, (caddr_t) & pid);
#else /* not SYSV */
  fcntl (new_fd, F_SETOWN, pid);
#endif /* not SYSV */

  css_sockopt (new_fd);
  return new_fd;
}

/*
 * css_transfer_fd() - send the fd of a new client request to a server
 *   return:
 *   server_fd(in):
 *   client_fd(in):
 *   rid(in):
 */
bool
css_transfer_fd (SOCKET server_fd, SOCKET client_fd, unsigned short rid)
{
  unsigned short req_id;
  struct iovec iov[1];
  struct msghdr msg;
  static struct cmsghdr *cmptr = NULL;

  req_id = htons (rid);

  /* Pass the fd to the server */
  iov[0].iov_base = (char *) &req_id;
  iov[0].iov_len = sizeof (unsigned short);
  msg.msg_iov = iov;
  msg.msg_iovlen = 1;
  msg.msg_namelen = 0;
  msg.msg_name = (caddr_t) 0;
  if (cmptr == NULL
      && (cmptr = (struct cmsghdr *) malloc (CONTROLLEN)) == NULL)
    {
      return false;
    }
  cmptr->cmsg_level = SOL_SOCKET;
  cmptr->cmsg_type = SCM_RIGHTS;
  cmptr->cmsg_len = CONTROLLEN;
  msg.msg_control = (void *) cmptr;
  msg.msg_controllen = CONTROLLEN;
  *(SOCKET *) CMSG_DATA (cmptr) = client_fd;

  if (sendmsg (server_fd, &msg, 0) < 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ERR_CSS_TCP_PASSING_FD, 0);
      return false;
    }

  return true;
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

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * css_gethostid() - returns the 32 bit host identifier for this machine
 *   return: 32 bit host identifier
 *
 * Note: Used for host key validation and some other purposes. Uses gethostid()
 *       on the Sun machines, for the rest, it tries to determine the IP
 *       address and encodes that as a 32 bit value.
 */
unsigned int
css_gethostid (void)
{
  unsigned int id = 0;

#ifdef HAVE_GETHOSTID
  id = (unsigned int) gethostid ();
#endif /* !HAVE_GETHOSTID */
  return id;
}
#endif

/*
 * css_open_server_connection_socket() -
 *   return:
 *
 * Note: Stub functions for the new-style connection protocol.
 *       Eventually should try to support these on non-NT platforms.
 *       See also wintcp.c
 */
int
css_open_server_connection_socket (void)
{
  return -1;
}

/*
 * css_close_server_connection_socket() -
 *   return; void
 *
 * Note: Stub functions for the new-style connection protocol.
 *       Eventually should try to support these on non-NT platforms.
 *       See also wintcp.c
 */
void
css_close_server_connection_socket (void)
{
}

int
css_get_max_socket_fds (void)
{
  return (int) sysconf (_SC_OPEN_MAX);
}

#define SET_NONBLOCKING(fd) { \
      int flags = fcntl (fd, F_GETFL); \
      flags |= O_NONBLOCK; \
      fcntl (fd, F_SETFL, flags); \
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

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * css_get_sock_name() - get the hostname of the socket
 *   return: 0 if success; otherwise errno
 *   hostname(in): buffer for hostname
 *   len(in): size of the hostname buffer
 */
int
css_get_sock_name (SOCKET sockfd, char *hostname, size_t len)
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
  if (getsockname (sockfd, saddr, &saddr_len) != 0)
    {
      return errno;
    }
  return getnameinfo (saddr, saddr_len, hostname, len, NULL, 0, NI_NOFQDN);
}
#endif
