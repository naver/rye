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
 * master.c - master main
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <time.h>

#include <sys/types.h>
#include <sys/wait.h>
#include <sys/time.h>
#include <netdb.h>
#include <sys/param.h>
#include <sys/resource.h>
#include <netinet/in.h>
#include <unistd.h>
#include <fcntl.h>
#include <syslog.h>
#include <pthread.h>

#include "utility.h"
#include "porting.h"
#include "error_manager.h"
#include "language_support.h"
#include "connection_globals.h"
#include "connection_cl.h"
#include "tcp.h"
#include "master_util.h"
#include "master_request.h"
#include "master_heartbeat.h"
#include "environment_variable.h"
#include "message_catalog.h"
#include "dbi.h"
#include "util_func.h"
#include "client_support.h"
#include "rye_shm.h"
#include "rye_master_shm.h"
#include "file_io.h"

static void css_master_error (const char *error_string);
static int css_master_timeout (void);
static int css_master_init (SOCKET * clientfd);
static void css_reject_server_request (CSS_CONN_ENTRY * conn,
				       unsigned short rid, int reason);
static int css_accept_server_request (CSS_CONN_ENTRY * conn,
				      unsigned short rid);
static void css_register_new_server (CSS_CONN_ENTRY * conn,
				     unsigned short rid, char *server_name,
				     int server_name_length);
static void css_process_new_connection (SOCKET fd);
static int css_enroll_read_sockets (SOCKET_QUEUE_ENTRY * anchor_p,
				    fd_set * fd_var);
static int css_enroll_write_sockets (SOCKET_QUEUE_ENTRY * anchor_p,
				     fd_set * fd_var);
static int css_enroll_exception_sockets (SOCKET_QUEUE_ENTRY * anchor_p,
					 fd_set * fd_var);

static int css_enroll_master_read_sockets (fd_set * fd_var);
static int css_enroll_master_write_sockets (fd_set * fd_var);
static int css_enroll_master_exception_sockets (fd_set * fd_var);
static void css_master_select_error (void);
static void css_check_master_socket_input (int *count, fd_set * fd_var);
static void css_check_master_socket_output (void);
static int css_check_master_socket_exception (fd_set * fd_var);
static void css_master_loop (void);
static void css_free_entry (SOCKET_QUEUE_ENTRY * entry_p);

static void css_daemon_start (void);

struct timeval *css_Master_timeout = NULL;
int css_Master_timeout_value_in_seconds = 4;
int css_Master_timeout_value_in_microseconds = 500;
#if defined(DEBUG)
static int css_Active_server_count = 0;
#endif

time_t css_Start_time;
int css_Total_request_count = 0;

/* socket for incoming client requests */
SOCKET css_Master_socket_fd = INVALID_SOCKET;

int css_Master_lock_fd = NULL_VOLDES;
/* This is the queue anchor of sockets used by the Master server. */
SOCKET_QUEUE_ENTRY *css_Master_socket_anchor = NULL;
pthread_mutex_t css_Master_socket_anchor_lock;

/*
 * css_master_error() - print error message to syslog or console
 *   return: none
 *   error_string(in)
 *
 * Note: Errors encountered by the master will always be printed.
 */
static void
css_master_error (const char *error_string)
{
  syslog (LOG_ALERT, "Master process: %s %s\n", error_string,
	  errno > 0 ? strerror (errno) : "");
}

/*
 * css_master_timeout()
 *   return: 0 if css_Master_timeout time is expired,
 *           otherwise 1
 *
 * Note:
 *   This is to handle the case when nothing is happening. We will check the
 *   process status to see if the server is still running. If not, then we
 *   remove our entry.
 */
static int
css_master_timeout (void)
{
  SOCKET_QUEUE_ENTRY *temp;
  struct timeval timeout;

  /* check for timeout */
  if (css_Master_timeout &&
      time ((time_t *) & timeout.tv_sec) != (time_t) - 1 &&
      css_Master_timeout->tv_sec < timeout.tv_sec)
    {
      return (0);
    }

  /* On the WINDOWS, it is not clear if we will ever have spawned child
   * processes, at least initially.  There don't appear to be any
   * similarly named "wait" functions in the MSVC runtime library.
   */
  pthread_mutex_lock (&css_Master_socket_anchor_lock);
  for (temp = css_Master_socket_anchor; temp; temp = temp->next)
    {
      if (kill (temp->pid, 0) && errno == ESRCH)
	{
	  hb_cleanup_conn_and_start_process (temp->conn_ptr);

	  break;
	}
    }
  pthread_mutex_unlock (&css_Master_socket_anchor_lock);

  return (1);
}

/*
 * css_master_cleanup() - cleanup the socket on the port-id
 *   return: none
 *   sig(in)
 *
 * Note: It will not be called if we are killed by an outside process
 */
void
css_master_cleanup (UNUSED_ARG int sig)
{
  char sock_path[PATH_MAX];

  css_shutdown_socket (css_Master_socket_fd);

  css_get_master_domain_path (sock_path, PATH_MAX, false);
  unlink (sock_path);

  exit (1);
}

/*
 * css_master_init() - setup the signal handling routines and attempt to
 *                     bind the socket address
 *   return: 1 if success, otherwise 0
 *   cservice(in)
 *   clientfd(out)
 */
static int
css_master_init (SOCKET * clientfd)
{
  (void) os_set_signal_handler (SIGSTOP, css_master_cleanup);
  if (os_set_signal_handler (SIGTERM, css_master_cleanup) == SIG_ERR ||
      os_set_signal_handler (SIGINT, css_master_cleanup) == SIG_ERR ||
      os_set_signal_handler (SIGPIPE, SIG_IGN) == SIG_ERR ||
      os_set_signal_handler (SIGCHLD, SIG_IGN) == SIG_ERR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
      return (0);
    }

  pthread_mutex_init (&css_Master_socket_anchor_lock, NULL);

  return (css_tcp_master_open (clientfd));
}

/*
 * css_reject_server_request() - Sends a reject message to a server request
 *   return: none
 *   conn(in)
 *   reason(in)
 */
static void
css_reject_server_request (CSS_CONN_ENTRY * conn, unsigned short rid,
			   int reason)
{
  int reject_reason;

  reject_reason = htonl (reason);
  css_send_data_packet (conn, rid, 1, (char *) &reject_reason, sizeof (int));
}

/*
 * css_accept_server_request() - Accepts a connect request from a server
 *   return: none
 *   conn(in)
 *   rid(in)
 */
static int
css_accept_server_request (CSS_CONN_ENTRY * conn, unsigned short rid)
{
  CSS_NET_PACKET *recv_packet = NULL;
  int error = ER_FAILED;
  int accept_reason;

  accept_reason = htonl (SERVER_REQUEST_ACCEPTED);
  css_send_data_packet (conn, rid, 1, (char *) &accept_reason, sizeof (int));

  if (css_recv_data_from_server (&recv_packet, conn, rid, -1, 1,
				 &error, sizeof (int)) != NO_ERRORS)
    {
      return ER_FAILED;
    }

  error = ntohl (error);

  css_net_packet_free (recv_packet);

  return error;
}

/*
 * css_register_new_server() - register a new server by reading the server name
 *   return: none
 *   conn(in)
 *   rid(in)
 *
 * Note: This will allow us to pass fds for future requests to the server.
 */
static void
css_register_new_server (CSS_CONN_ENTRY * conn, unsigned short rid,
			 char *server_name, int server_name_length)
{
  SOCKET_QUEUE_ENTRY *entry;
  int length;

  entry = css_return_entry_of_server (server_name, css_Master_socket_anchor);

  if (entry != NULL && !IS_INVALID_SOCKET (entry->fd))
    {
      /* reject a server with a duplicate name */
      css_reject_server_request (conn, rid, SERVER_ALREADY_EXISTS);
      css_free_conn (conn);
      return;
    }

  /* accept a server that was auto-started */
  if (css_accept_server_request (conn, rid) != NO_ERROR)
    {
      assert (false);
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ERR_CSS_ERROR_DURING_SERVER_CONNECT, 1,
			   server_name);
      css_free_conn (conn);
      return;
    }

  if (entry == NULL)
    {
#if defined(DEBUG)
      css_Active_server_count++;
#endif
      entry = css_add_request_to_socket_queue (conn, server_name, conn->fd,
					       READ_WRITE, 0,
					       &css_Master_socket_anchor);
    }
  else
    {
      css_free_conn (entry->conn_ptr);
      entry->fd = conn->fd;
      entry->conn_ptr = conn;
    }

  if (entry == NULL)
    {
      assert (false);
      css_free_conn (conn);
      return;
    }

  length = strlen (server_name) + 1;
  if (length < server_name_length)
    {
      server_name += length;

      if (entry->env_var != NULL)
	{
	  free (entry->env_var);
	}
      entry->env_var = (char *) malloc (strlen (server_name) + 1);
      if (entry->env_var != NULL)
	{
	  strcpy (entry->env_var, server_name);
	}

      server_name += strlen (server_name) + 1;

      entry->pid = atoi (server_name);
    }
}

/*
 * Master server to Slave server communication support routines.
 */

/*
 * css_process_new_connection()
 *   return: none
 *   fd(in)
 *
 * Note:
 *   Selects the appropriate handler based on the type of connection. We can
 *   support a client request (to connect to a server), a server request (to
 *   register itself) and an information client request (master control client).
 */
static void
css_process_new_connection (SOCKET fd)
{
  CSS_CONN_ENTRY *conn;
  SVR_CONNECT_TYPE conn_type;
  unsigned short request_id;
  CSS_NET_PACKET *recv_packet = NULL;

  css_Total_request_count++;
  conn = css_make_conn (fd);
  if (conn == NULL)
    {
      return;
    }

  if (css_check_magic (conn) != NO_ERRORS)
    {
      css_free_conn (conn);
      return;
    }

  if (css_recv_command_packet (conn, &recv_packet) == NO_ERRORS)
    {
      char *server_name;
      int server_name_length;

      server_name = css_net_packet_get_buffer (recv_packet, 0, -1, false);
      server_name_length = css_net_packet_get_recv_size (recv_packet, 0);

      conn_type = recv_packet->header.function_code;
      request_id = recv_packet->header.request_id;
      switch (conn_type)
	{
	case SVR_CONNECT_TYPE_MASTER_INFO:	/* request for information */
	  css_add_request_to_socket_queue (conn, NULL, fd,
					   READ_WRITE, 0,
					   &css_Master_socket_anchor);
	  break;
	case SVR_CONNECT_TYPE_MASTER_HB_PROC:	/* request from a new server or new repl */
	  css_register_new_server (conn, request_id, server_name,
				   server_name_length);
	  /* conn is reused or freed in css_register_new_server()
	   * do not call css_free_conn() here */
	  break;
	default:
	  assert (0);
	  css_free_conn (conn);
	  break;
	}

      css_net_packet_free (recv_packet);
    }
  else
    {
      css_free_conn (conn);
    }
}

/*
 * css_enroll_read_sockets() - Sets the fd positions in fd_set for the
 *                                    input fds we are interested in reading
 *   return: none
 *
 *   anchor_p(in)
 *   fd_var(out)
 */
static int
css_enroll_read_sockets (SOCKET_QUEUE_ENTRY * anchor_p, fd_set * fd_var)
{
  SOCKET_QUEUE_ENTRY *temp;
  int max_fd = 0;

  FD_ZERO (fd_var);
  for (temp = anchor_p; temp; temp = temp->next)
    {
      if (!IS_INVALID_SOCKET (temp->fd) && temp->fd_type != WRITE_ONLY)
	{
	  FD_SET (temp->fd, fd_var);
	  if (temp->fd > max_fd)
	    {
	      max_fd = temp->fd;
	    }
	}
    }

  return max_fd;
}

/*
 * css_enroll_master_read_sockets() -
 *   return: none
 *
 *   fd_var(out)
 */
static int
css_enroll_master_read_sockets (fd_set * fd_var)
{
  return css_enroll_read_sockets (css_Master_socket_anchor, fd_var);
}

/*
 * css_enroll_write_sockets() - Sets the fd positions in fd_set for the
 *                      input fds we are interested in writing (none presently)
 *   return: none
 *
 *   anchor_p(in)
 *   fd_var(out)
 */
static int
css_enroll_write_sockets (UNUSED_ARG SOCKET_QUEUE_ENTRY * anchor_p,
			  fd_set * fd_var)
{
  FD_ZERO (fd_var);
  return 0;
}

/*
 * css_enroll_master_write_sockets() -
 *
 *   return: none
 *   fd_var(out)
 */
static int
css_enroll_master_write_sockets (fd_set * fd_var)
{
  return css_enroll_write_sockets (css_Master_socket_anchor, fd_var);
}

/*
 * css_enroll_exception_sockets() - Sets the fd positions in fd_set
 *            for the input fds we are interested in detecting error conditions
 *   return: last fd
 *
 *   anchor_p(int)
 *   fd_var(out)
 */
static int
css_enroll_exception_sockets (SOCKET_QUEUE_ENTRY * anchor_p, fd_set * fd_var)
{
  SOCKET_QUEUE_ENTRY *temp;
  int max_fd = 0;

  FD_ZERO (fd_var);
  for (temp = anchor_p; temp; temp = temp->next)
    {
      if (!IS_INVALID_SOCKET (temp->fd))
	{
	  FD_SET (temp->fd, fd_var);
	  if (temp->fd > max_fd)
	    {
	      max_fd = temp->fd;
	    }
	}
    }

  return max_fd;
}

/*
 * css_enroll_master_exception_sockets()
 *   return: last fd
 *   fd_var(out)
 */
static int
css_enroll_master_exception_sockets (fd_set * fd_var)
{
  return css_enroll_exception_sockets (css_Master_socket_anchor, fd_var);
}

/*
 * css_master_select_error() - Check status of all known fds and remove those
 *                             that are closed
 *   return: none
 */
static void
css_master_select_error (void)
{
  SOCKET_QUEUE_ENTRY *temp;

again:
  pthread_mutex_lock (&css_Master_socket_anchor_lock);
  for (temp = css_Master_socket_anchor; temp; temp = temp->next)
    {
      if (!IS_INVALID_SOCKET (temp->fd) && fcntl (temp->fd, F_GETFL, 0) < 0)
	{
	  hb_cleanup_conn_and_start_process (temp->conn_ptr);
	  pthread_mutex_unlock (&css_Master_socket_anchor_lock);

	  goto again;
	}
    }

  pthread_mutex_unlock (&css_Master_socket_anchor_lock);
}


/*
 * css_check_master_socket_input() - checks if there is input from a client
 *                                   that must be processed
 *   return: none
 *   count(in/out)
 *   fd_var(in/out)
 */
static void
css_check_master_socket_input (int *count, fd_set * fd_var)
{
  SOCKET_QUEUE_ENTRY *temp, *next;
  SOCKET new_fd;

  pthread_mutex_lock (&css_Master_socket_anchor_lock);

  for (temp = css_Master_socket_anchor; *count && temp; temp = next)
    {
      next = temp->next;
      if (!IS_INVALID_SOCKET (temp->fd) && FD_ISSET (temp->fd, fd_var))
	{
	  FD_CLR (temp->fd, fd_var);
	  (*count)--;
	  if (temp->fd == css_Master_socket_fd)
	    {
	      new_fd = css_master_accept (temp->fd);
	      if (!IS_INVALID_SOCKET (new_fd))
		{
		  css_process_new_connection (new_fd);
		}
	    }
	  else if (!IS_INVALID_SOCKET (temp->fd))
	    {
	      css_master_request_handler (temp);

	      /* stop loop in case an error caused temp to be deleted */
	      break;
	    }
	}
    }

  pthread_mutex_unlock (&css_Master_socket_anchor_lock);
}

/*
 * css_check_master_socket_output()
 *   return: none
 *
 * Note: currently no-op.
 */
static void
css_check_master_socket_output (void)
{
}

/*
 * css_check_master_socket_exception() - Checks for exception conditions for
 *                                       open sockets
 *   return: 0 if master will terminate, otherwise 1.
 *   fd_var(in/out)
 */
static int
css_check_master_socket_exception (fd_set * fd_var)
{
  SOCKET_QUEUE_ENTRY *temp;

again:
  pthread_mutex_lock (&css_Master_socket_anchor_lock);
  for (temp = css_Master_socket_anchor; temp; temp = temp->next)
    {
      if (!IS_INVALID_SOCKET (temp->fd)
	  && ((FD_ISSET (temp->fd, fd_var)
	       || (fcntl (temp->fd, F_GETFL, 0) < 0)
	       || (temp->error_p == true)
	       || (temp->conn_ptr == NULL)
	       || (temp->conn_ptr->status == CONN_CLOSED))))
	{
#if defined(DEBUG)
	  if (css_Active_server_count > 0)
	    {
	      css_Active_server_count--;
	    }
#endif
	  FD_CLR (temp->fd, fd_var);
	  if (temp->fd == css_Master_socket_fd)
	    {
	      pthread_mutex_unlock (&css_Master_socket_anchor_lock);

	      return (0);
	    }
	  hb_cleanup_conn_and_start_process (temp->conn_ptr);

	  pthread_mutex_unlock (&css_Master_socket_anchor_lock);

	  goto again;
	}
    }

  pthread_mutex_unlock (&css_Master_socket_anchor_lock);

  return (1);
}

/*
 * css_master_loop() - main loop for master
 *   return: none
 */
static void
css_master_loop (void)
{
  fd_set read_fd, write_fd, exception_fd;
  static struct timeval timeout;
  int rc, run_code;

  run_code = 1;
  while (run_code)
    {
      int max_fd, max_fd1, max_fd2, max_fd3;

      max_fd1 = css_enroll_master_read_sockets (&read_fd);
      max_fd2 = css_enroll_master_write_sockets (&write_fd);
      max_fd3 = css_enroll_master_exception_sockets (&exception_fd);

      max_fd = MAX (MAX (max_fd1, max_fd2), max_fd3);

      timeout.tv_sec = css_Master_timeout_value_in_seconds;
      timeout.tv_usec = css_Master_timeout_value_in_microseconds;

      rc = select (max_fd + 1, &read_fd, &write_fd, &exception_fd, &timeout);
      switch (rc)
	{
	case 0:
	  run_code = css_master_timeout ();
	  break;
	case -1:
	  /* switch error */
	  css_master_select_error ();
	  break;
	default:
	  css_check_master_socket_input (&rc, &read_fd);
	  css_check_master_socket_output ();
	  run_code = css_check_master_socket_exception (&exception_fd);
	  break;
	}
    }
}

static int
is_master_running ()
{
  char master_lock_file[PATH_MAX];
  int fd = NULL_VOLDES;
  bool file_locked = false;
  char buf[256];

  if (css_does_master_exist ())
    {
      goto running;
    }

  /* create dir */
  envvar_socket_file (master_lock_file, sizeof (master_lock_file), "");
  rye_mkdir (master_lock_file, 0755);

  css_get_master_domain_path (master_lock_file,
			      sizeof (master_lock_file), true);
  fd = fileio_open (master_lock_file, O_RDWR | O_CREAT, 0666);
  if (fd == NULL_VOLDES)
    {
      goto running;
    }

  if (fileio_get_lock_retry (fd, master_lock_file) != NO_ERROR)
    {
      goto running;
    }

  file_locked = true;

  snprintf (buf, sizeof (buf), "%d\n", getpid ());
  lseek (fd, 0, SEEK_SET);
  write (fd, buf, strlen (buf) + 1);

  css_Master_lock_fd = fd;

  return false;

running:
  if (fd != NULL_VOLDES)
    {
      if (file_locked)
	{
	  fileio_release_lock (fd);
	}
      fileio_close (fd);
    }
  return true;
}

/*
 * main() - master main function
 *   return: EXIT_SUCCESS/EXIT_FAILURE
 */
int
main (int argc, char **argv)
{
  CSS_CONN_ENTRY *conn;
  static const char *suffix = "_master.err";
  char hostname[MAXHOSTNAMELEN + sizeof (suffix)];
  int status = EXIT_SUCCESS;
  const char *msg_format;

  if (utility_initialize () != NO_ERROR)
    {
      return EXIT_FAILURE;
    }

  if (GETHOSTNAME (hostname, MAXHOSTNAMELEN) == 0)
    {
      /* css_gethostname won't null-terminate if the name is
       * overlong.  Put in a guaranteed null-terminator of our
       * own so that strcat doesn't go wild.
       */
      hostname[MAXHOSTNAMELEN] = '\0';
      strcat (hostname, suffix);
    }

  if (db_initialize () != NO_ERROR)
    {
      PRINT_AND_LOG_ERR_MSG ("Failed to initialize.\n");
      status = EXIT_FAILURE;
      goto cleanup;
    }

  if (sysprm_load_and_init ((argc > 1) ? argv[1] : NULL) != NO_ERROR)
    {
      msg_format = msgcat_message (MSGCAT_CATALOG_UTILS,
				   MSGCAT_UTIL_SET_MASTER,
				   MASTER_MSG_NO_PARAMETERS);
      css_master_error (msg_format);
      util_log_write_errstr (msg_format);
      status = EXIT_FAILURE;
      goto cleanup;
    }

  if (is_master_running ())
    {
      msg_format = msgcat_message (MSGCAT_CATALOG_UTILS,
				   MSGCAT_UTIL_SET_MASTER,
				   MASTER_MSG_DUPLICATE);
      util_log_write_errstr (msg_format, argv[0]);
      status = EXIT_FAILURE;
      goto cleanup;
    }

  TPRINTF (msgcat_message (MSGCAT_CATALOG_UTILS,
			   MSGCAT_UTIL_SET_MASTER, MASTER_MSG_STARTING), 0);

  er_stack_clearall ();
  er_clear ();

  if (envvar_get ("NO_DAEMON") == NULL)
    {
      css_daemon_start ();
    }

  (void) utility_initialize ();

  time (&css_Start_time);

  css_master_requests_init ();

  if (css_master_init (&css_Master_socket_fd) != NO_ERROR)
    {
      PRINT_AND_LOG_ERR_MSG ("%s: %s\n", argv[0], db_error_string (1));
      css_master_error (msgcat_message (MSGCAT_CATALOG_UTILS,
					MSGCAT_UTIL_SET_MASTER,
					MASTER_MSG_PROCESS_ERROR));
      status = EXIT_FAILURE;
      goto cleanup;
    }

  if (envvar_get ("NO_DAEMON") != NULL)
    {
      (void) os_set_signal_handler (SIGINT, css_master_cleanup);
    }

  /*
   * shm key info init.
   */
  if (master_shm_initialize () != NO_ERROR)
    {
      status = EXIT_FAILURE;
      goto cleanup;
    }

  if (hb_master_init () != NO_ERROR)
    {
      status = EXIT_FAILURE;
      goto cleanup;
    }

  conn = css_make_conn (css_Master_socket_fd);
  css_add_request_to_socket_queue (conn, NULL, css_Master_socket_fd,
				   READ_WRITE, 0, &css_Master_socket_anchor);

  css_master_loop ();
  css_master_cleanup (SIGINT);
  css_master_error (msgcat_message (MSGCAT_CATALOG_UTILS,
				    MSGCAT_UTIL_SET_MASTER,
				    MASTER_MSG_EXITING));

cleanup:

  master_shm_final ();

  db_finalize ();

  return status;
}

/*
 * These are the queuing routines for the Master socket queue (which is the
 * queue used by the Master Server) and the Open socket queue (which is the
 * queue used by the servers).
 */
/*
 * css_free_entry() -
 *   return: void
 *   entry_p(in/out):
 */
static void
css_free_entry (SOCKET_QUEUE_ENTRY * entry_p)
{
  if (entry_p->conn_ptr)
    {
      css_free_conn (entry_p->conn_ptr);
    }

  if (entry_p->name)
    {
      free_and_init (entry_p->name);
    }
  if (entry_p->env_var)
    {
      free_and_init (entry_p->env_var);
    }

  /* entry->fd has already been closed by css_free_conn */
  free_and_init (entry_p);
}

/*
 * css_remove_entry_by_conn() -
 *   return: void
 *   conn_p(in):
 *   anchor_p(out):
 */
void
css_remove_entry_by_conn (CSS_CONN_ENTRY * conn_p,
			  SOCKET_QUEUE_ENTRY ** anchor_p)
{
  SOCKET_QUEUE_ENTRY *p, *q;

  if (conn_p == NULL)
    {
      return;
    }

  for (p = *anchor_p, q = NULL; p; q = p, p = p->next)
    {
      if (p->conn_ptr != conn_p)
	{
	  continue;
	}

      if (p == *anchor_p)
	{
	  *anchor_p = p->next;
	}
      else
	{
	  q->next = p->next;
	}

      css_free_entry (p);
      break;
    }
}

/*
 * css_add_request_to_socket_queue() -
 *   return:
 *   conn_p(in):
 *   name_p(in):
 *   fd(in):
 *   fd_type(in):
 *   pid(in):
 *   anchor_p(out):
 */
SOCKET_QUEUE_ENTRY *
css_add_request_to_socket_queue (CSS_CONN_ENTRY * conn_p,
				 char *name_p, SOCKET fd, int fd_type,
				 int pid, SOCKET_QUEUE_ENTRY ** anchor_p)
{
  SOCKET_QUEUE_ENTRY *p;

  p = (SOCKET_QUEUE_ENTRY *) malloc (sizeof (SOCKET_QUEUE_ENTRY));
  if (p == NULL)
    {
      return NULL;
    }

  p->conn_ptr = conn_p;
  p->fd = fd;

  if (name_p)
    {
      p->name = (char *) malloc (strlen (name_p) + 1);
      if (p->name)
	{
	  strcpy (p->name, name_p);
	}
    }
  else
    {
      p->name = NULL;
    }

  p->env_var = NULL;
  p->fd_type = fd_type;
  p->queue_p = 0;
  p->error_p = FALSE;
  p->pid = pid;
  p->db_error = 0;
  p->next = *anchor_p;
  *anchor_p = p;

  return p;
}

/*
 * css_return_entry_of_server() -
 *   return:
 *   name_p(in):
 *   anchor_p(in):
 */
SOCKET_QUEUE_ENTRY *
css_return_entry_of_server (const char *name_p, SOCKET_QUEUE_ENTRY * anchor_p)
{
  SOCKET_QUEUE_ENTRY *p;

  if (name_p == NULL)
    {
      return NULL;
    }

  for (p = anchor_p; p; p = p->next)
    {
      if (p->name && strcmp (p->name, name_p) == 0)
	{
	  return p;
	}

      /* if HA server exist */
      if (p->name && (IS_MASTER_CONN_NAME_HA_SERVER (p->name)))
	{
	  if (strcmp ((char *) (p->name + 1), name_p) == 0)
	    {
	      return p;
	    }
	}
    }

  return NULL;
}

/*
 * css_return_entry_by_conn() -
 *   return:
 *   conn(in):
 *   anchor_p(in):
 */
SOCKET_QUEUE_ENTRY *
css_return_entry_by_conn (CSS_CONN_ENTRY * conn_p,
			  SOCKET_QUEUE_ENTRY ** anchor_p)
{
  SOCKET_QUEUE_ENTRY *p;

  if (conn_p == NULL)
    {
      return NULL;
    }

  for (p = *anchor_p; p; p = p->next)
    {
      if (p->conn_ptr != conn_p)
	{
	  continue;
	}

      return (p);
    }

  return NULL;
}


/*
 * css_daemon_start() - detach a process from login session context
 *   return: none
 */
static void
css_daemon_start (void)
{
  int childpid;
#if defined (sun)
  struct rlimit rlp;
#endif /* sun */
  int ppid = getpid ();


  /* If we were started by init (process 1) from the /etc/inittab file
   * there's no need to detatch.
   * This test is unreliable due to an unavoidable ambiguity
   * if the process is started by some other process and orphaned
   * (i.e., if the parent process terminates before we get here).
   */

  if (getppid () == 1)
    {
      goto out;
    }

  /*
   * Ignore the terminal stop signals (BSD).
   */

#ifdef SIGTTOU
  if (os_set_signal_handler (SIGTTOU, SIG_IGN) == SIG_ERR)
    {
      exit (0);
    }
#endif
#ifdef SIGTTIN
  if (os_set_signal_handler (SIGTTIN, SIG_IGN) == SIG_ERR)
    {
      exit (0);
    }
#endif
#ifdef SIGTSTP
  if (os_set_signal_handler (SIGTSTP, SIG_IGN) == SIG_ERR)
    {
      exit (0);
    }
#endif

  /*
   * Call fork and have the parent exit.
   * This does several things. First, if we were started as a simple shell
   * command, having the parent terminate makes the shell think that the
   * command is done. Second, the child process inherits the process group ID
   * of the parent but gets a new process ID, so we are guaranteed that the
   * child is not a process group leader, which is a prerequirement for the
   * call to setsid
   */

  childpid = fork ();
  if (childpid < 0)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ERR_CSS_CANNOT_FORK, 0);
    }
  else if (childpid > 0)
    {
      exit (0);			/* parent goes bye-bye */
    }
  else
    {
      /*
       * Wait until the parent process has finished. Coded with polling since
       * the parent should finish immediately. SO, it is unlikely that we are
       * going to loop at all.
       */
      while (getppid () == ppid)
	{
	  sleep (1);
	}
    }

  /*
   * Create a new session and make the child process the session leader of
   * the new session, the process group leader of the new process group.
   * The child process has no controlling terminal.
   */

  if (os_set_signal_handler (SIGHUP, SIG_IGN) == SIG_ERR)
    {
      exit (0);			/* fail to immune from pgrp leader death */
    }

  setsid ();

out:

  errno = 0;			/* Reset errno from last close */

  /*
   * The file mode creation mask that is inherited could be set to deny
   * certain permissions. Therefore, clear the file mode creation mask.
   */

  umask (0);
}
