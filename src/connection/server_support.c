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
 * server_support.c - server interface
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>
#include <sys/socket.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <assert.h>
#include <sys/epoll.h>

#include "porting.h"
#include "thread.h"
#include "memory_alloc.h"
#include "boot_sr.h"
#include "connection_defs.h"
#include "connection_globals.h"
#include "release_string.h"
#include "system_parameter.h"
#include "environment_variable.h"
#include "error_manager.h"
#include "thread.h"
#include "connection_error.h"
#include "message_catalog.h"
#include "critical_section.h"
#include "lock_manager.h"
#include "log_manager.h"
#include "network.h"
#include "tcp.h"
#include "connection_sr.h"
#include "xserver_interface.h"
#include "server_support.h"
#include "utility.h"
#include "heartbeat.h"
#include "network_interface_sr.h"
#include "rye_shm.h"
#include "rye_master_shm.h"
#include "rye_server_shm.h"

#define CSS_WAIT_COUNT 5	/* # of retry to connect to master */
#define CSS_GOING_DOWN_IMMEDIATELY "Server going down immediately"

#define SockError    -1

static struct timeval css_Shutdown_timeout = { 0, 0 };
static char *css_Master_server_name = NULL;	/* database identifier */
static CSS_CONN_ENTRY *css_Master_conn;
static IP_INFO *css_Server_accessible_ip_info;
static char *ip_list_file_name = NULL;
static char ip_file_real_path[PATH_MAX];

/* server's state for HA feature */
static bool ha_Repl_delay_detected = false;


typedef struct
{
  CSS_JOB_ENTRY *front;
  CSS_JOB_ENTRY *back;

  int count;
} CSS_JOB_ENTRY_LIST;

typedef struct job_queue JOB_QUEUE;
struct job_queue
{
  pthread_mutex_t job_lock;
  pthread_cond_t job_cond;
  CSS_JOB_ENTRY_LIST job_list;
  CSS_JOB_ENTRY_LIST free_list;
  int num_job;
  int num_run_threads;
  int max_workers;
  INT64 num_requests;
};

static JOB_QUEUE css_Job_queue[JOB_QUEUE_TYPE_MAX];

typedef struct
{
  int epoll_fd;
  int count;
  bool shutdown;
} CSS_EPOLL_INFO;

static CSS_EPOLL_INFO *css_Epoll_info;

static void css_empty_job_queue (void);
static void css_setup_server_loop (void);
static int css_check_conn (CSS_CONN_ENTRY * p);
static void css_set_shutdown_timeout (INT64 timeout);
static int css_get_master_request (CSS_CONN_ENTRY * conn,
				   CSS_NET_PACKET ** recv_packet);
static void css_process_connect_request (void);
static int css_process_master_request (CSS_CONN_ENTRY * conn);
static void css_process_new_client (SOCKET master_fd);
static void css_process_change_server_ha_mode_request (char *data,
						       int datasize);

static void css_close_connection_to_master (void);
static void css_close_server_listen_socket (void);
static void dummy_sigurg_handler (int sig);
static int css_check_accessibility (SOCKET new_fd);

static void css_epoll_stop (void);
static int css_epoll_init (void);
static int css_epoll_ctl (int epoll_fd, int epoll_op, CSS_CONN_ENTRY * conn);

static void css_job_entry_list_init (CSS_JOB_ENTRY_LIST * ptr);
static void css_job_entry_list_add (CSS_JOB_ENTRY_LIST * ptr,
				    CSS_JOB_ENTRY * item);
static CSS_JOB_ENTRY *css_job_entry_list_remove (CSS_JOB_ENTRY_LIST * ptr);
static int css_job_entry_add (JOB_QUEUE * job_queue, CSS_JOB_ENTRY * new_job);
static int css_job_entry_get (JOB_QUEUE * job_queue,
			      CSS_JOB_ENTRY * job_entry);
static int css_con_close_handler (THREAD_ENTRY * thread_p,
				  CSS_THREAD_ARG arg);
static int css_accept_new_client (unsigned short rid, CSS_CONN_ENTRY * conn);

/*
 * css_init_job_queue () -
 *   return:
 */
int
css_init_job_queue (void)
{
  JOB_QUEUE_TYPE q_type;
  int r;
  JOB_QUEUE *qptr;

  /* initialize job queue */
  for (q_type = JOB_QUEUE_TYPE_MIN; q_type < JOB_QUEUE_TYPE_MAX; q_type++)
    {
      qptr = &css_Job_queue[q_type];

      qptr->num_run_threads = 0;
      qptr->num_job = 0;
      qptr->num_requests = 0;

      r = pthread_mutex_init (&qptr->job_lock, NULL);
      if (r != 0)
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_CSS_PTHREAD_MUTEX_INIT, 0);
	  return ER_CSS_PTHREAD_MUTEX_INIT;
	}
      r = pthread_cond_init (&qptr->job_cond, NULL);
      if (r != 0)
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_CSS_PTHREAD_COND_INIT, 0);
	  return ER_CSS_PTHREAD_COND_INIT;
	}


      css_job_entry_list_init (&qptr->job_list);
      css_job_entry_list_init (&qptr->free_list);

      qptr->max_workers = thread_max_workers_by_queue_type (q_type);
      assert (qptr->max_workers > 0);
    }

  return NO_ERROR;
}

#if 0
void
css_job_queue_check (FILE * out_fp)
{
  JOB_QUEUE_TYPE q_type;
  JOB_QUEUE *q_ptr;

  if (css_Job_queue[0].num_requests <= 0)
    {
      return;
    }

  for (q_type = JOB_QUEUE_TYPE_MIN; q_type < JOB_QUEUE_TYPE_MAX; q_type++)
    {
      q_ptr = &css_Job_queue[q_type];

      if (out_fp != NULL)
	{
	  fprintf (out_fp,
		   "JQ(%d) : "
		   "run_threads = %d, "
		   "num_job = %d, "
		   "num_requests = %ld, "
		   "\n",
		   q_type, q_ptr->num_run_threads,
		   q_ptr->num_job, q_ptr->num_requests);
	}
    }

  if (out_fp != NULL)
    {
      fprintf (out_fp, "\n");
    }
}
#endif

int
css_incr_num_run_thread (JOB_QUEUE_TYPE q_type)
{
  pthread_mutex_lock (&css_Job_queue[q_type].job_lock);
  css_Job_queue[q_type].num_run_threads++;
  pthread_mutex_unlock (&css_Job_queue[q_type].job_lock);

  return 0;
}

int
css_decr_num_run_thread (JOB_QUEUE_TYPE q_type)
{
  pthread_mutex_lock (&css_Job_queue[q_type].job_lock);
  css_Job_queue[q_type].num_run_threads--;
  pthread_mutex_unlock (&css_Job_queue[q_type].job_lock);

  return 0;
}

/*
 * css_wakeup_all_jobq_waiters () -
 *   return:
 */
void
css_wakeup_all_jobq_waiters (void)
{
  JOB_QUEUE_TYPE q_type;

  for (q_type = JOB_QUEUE_TYPE_MIN; q_type < JOB_QUEUE_TYPE_MAX; q_type++)
    {
      pthread_mutex_lock (&css_Job_queue[q_type].job_lock);
      pthread_cond_broadcast (&css_Job_queue[q_type].job_cond);
      pthread_mutex_unlock (&css_Job_queue[q_type].job_lock);
    }
}

/*
 * css_add_to_job_queue () - add new job to job queue and wakeup worker
 *   return: error code
 */
int
css_add_to_job_queue (JOB_QUEUE_TYPE q_type, CSS_JOB_ENTRY * job_entry)
{
  job_entry->next = NULL;

  pthread_mutex_lock (&css_Job_queue[q_type].job_lock);

  if (css_job_entry_add (&css_Job_queue[q_type], job_entry) != NO_ERROR)
    {
      pthread_mutex_unlock (&css_Job_queue[q_type].job_lock);
      return ER_FAILED;
    }

  pthread_cond_signal (&css_Job_queue[q_type].job_cond);

  pthread_mutex_unlock (&css_Job_queue[q_type].job_lock);

  return NO_ERROR;
}

/*
 * css_get_new_job() - fetch a job from the queue or cond wait

 *   return: ER_FILED - no new job.
             NO_ERROR - fetch a job
 */
int
css_get_new_job (JOB_QUEUE_TYPE q_type, CSS_JOB_ENTRY * ret_job_entry)
{
  THREAD_ENTRY *thrd = thread_get_thread_entry_info ();
  int error;
  JOB_QUEUE *job_queue;

  job_queue = &css_Job_queue[q_type];

  pthread_mutex_lock (&job_queue->job_lock);

  css_Job_queue[q_type].num_run_threads--;

  error = css_job_entry_get (job_queue, ret_job_entry);
  if (error != NO_ERROR && !thrd->shutdown)
    {
      pthread_cond_wait (&job_queue->job_cond, &job_queue->job_lock);

      error = css_job_entry_get (&css_Job_queue[q_type], ret_job_entry);
    }

  css_Job_queue[q_type].num_run_threads++;

  pthread_mutex_unlock (&job_queue->job_lock);

  if (error == NO_ERROR)
    {
      ret_job_entry->next = NULL;
    }

  pthread_mutex_lock (&thrd->tran_index_lock);

  return error;
}

/*
 * css_job_entry_get() - fetch a job entry.
 *   return: ER_FAILED - no new job
             NO_ERROR :
 */
static int
css_job_entry_get (JOB_QUEUE * job_queue, CSS_JOB_ENTRY * ret_job_entry)
{
  CSS_JOB_ENTRY *job_entry_p = NULL;
  int error = ER_FAILED;

  job_entry_p = css_job_entry_list_remove (&job_queue->job_list);

  if (job_entry_p != NULL)
    {
      job_queue->num_job--;

      *ret_job_entry = *job_entry_p;

      css_job_entry_list_add (&job_queue->free_list, job_entry_p);

      error = NO_ERROR;
    }

  return error;
}

/*
 * css_job_entry_add() - add new job to job queue
 *   return: error code
 */
static int
css_job_entry_add (JOB_QUEUE * job_queue, CSS_JOB_ENTRY * new_job)
{
  CSS_JOB_ENTRY *job_entry_p = NULL;

  job_entry_p = css_job_entry_list_remove (&job_queue->free_list);
  if (job_entry_p == NULL)
    {
      job_entry_p = (CSS_JOB_ENTRY *) malloc (sizeof (CSS_JOB_ENTRY));
      if (job_entry_p == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (CSS_JOB_ENTRY));

	  return ER_FAILED;
	}
    }

  *job_entry_p = *new_job;

  css_job_entry_list_add (&job_queue->job_list, job_entry_p);

  job_queue->num_job++;
  job_queue->num_requests++;

  return NO_ERROR;
}

/*
 * css_empty_job_queue() - delete all job from the job queue
 *   return:
 */
static void
css_empty_job_queue ()
{
  JOB_QUEUE_TYPE q_type;
  CSS_JOB_ENTRY *p = NULL;

  css_epoll_stop ();

  for (q_type = JOB_QUEUE_TYPE_MIN; q_type < JOB_QUEUE_TYPE_MAX; q_type++)
    {
      pthread_mutex_lock (&css_Job_queue[q_type].job_lock);

      do
	{
	  p = css_job_entry_list_remove (&css_Job_queue[q_type].job_list);
	  if (p == NULL)
	    {
	      break;
	    }

	  css_job_entry_list_add (&css_Job_queue[q_type].free_list, p);
	}
      while (p != NULL);

      pthread_mutex_unlock (&css_Job_queue[q_type].job_lock);
    }
}

/*
 * css_final_job_queue() -
 *   return:
 */
void
css_final_job_queue (void)
{
  CSS_JOB_ENTRY *p;
  JOB_QUEUE_TYPE q_type;

  css_empty_job_queue ();

  for (q_type = JOB_QUEUE_TYPE_MIN; q_type < JOB_QUEUE_TYPE_MAX; q_type++)
    {
      pthread_mutex_lock (&css_Job_queue[q_type].job_lock);

      do
	{
	  p = css_job_entry_list_remove (&css_Job_queue[q_type].free_list);
	  if (p == NULL)
	    {
	      break;
	    }

	  free_and_init (p);
	}
      while (p != NULL);

      pthread_mutex_unlock (&css_Job_queue[q_type].job_lock);
    }
}

/*
 * css_check_conn() -
 *   return:
 *   p(in):
 */
static int
css_check_conn (CSS_CONN_ENTRY * p)
{
  int status = 0;

  if (fcntl (p->fd, F_GETFL, status) < 0 || p->status != CONN_OPEN)
    {
      return ER_FAILED;
    }

  return NO_ERROR;
}

/*
 * css_set_shutdown_timeout() -
 *   return:
 *   timeout(in):
 */
static void
css_set_shutdown_timeout (INT64 timeout)
{
  if (gettimeofday (&css_Shutdown_timeout, NULL) == 0)
    {
      css_Shutdown_timeout.tv_sec += (timeout / 1000);
    }
  return;
}

/*
 * css_setup_server_loop() -
 *   return:
 */
static void
css_setup_server_loop (void)
{
  int r, run_code = 1, nfds = 0;
  struct pollfd po[] = { {0, 0, 0}, {0, 0, 0} };

  (void) os_set_signal_handler (SIGPIPE, SIG_IGN);
  (void) os_set_signal_handler (SIGFPE, SIG_IGN);

  assert (css_Listen_conn != NULL);
  assert (!IS_INVALID_SOCKET (css_Listen_conn->fd));
  assert (css_Master_conn != NULL);
  assert (!IS_INVALID_SOCKET (css_Master_conn->fd));

  while (run_code)
    {
      po[0].fd = css_Master_conn->fd;
      po[0].events = POLLIN;
      po[0].revents = 0;
      po[1].fd = css_Listen_conn->fd;
      po[1].events = POLLIN;
      po[1].revents = 0;
      nfds = 2;

      /* select() sets timeout value to 0 or waited time */
      r = poll (po, nfds, 5000);
      if (r < 0)
	{
	  if (css_check_conn (css_Master_conn) < 0)
	    {
	      break;
	    }
	}
      else if (r > 0)
	{
	  if (po[0].revents & POLLERR || po[0].revents & POLLHUP)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_HB_PROCESS_EVENT, 2,
		      "Error on master connection", "");
	      break;
	    }
	  else if (po[1].revents & POLLERR || po[1].revents & POLLHUP)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_HB_PROCESS_EVENT, 2,
		      "Error on server connection", "");
	      break;
	    }

	  if (po[0].revents & POLLIN)
	    {
	      run_code = css_process_master_request (css_Master_conn);
	      if (run_code == 0)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_HB_PROCESS_EVENT, 2,
			  "Disconnected with the rye_master and will shut itself down",
			  "");
		}
	    }

	  if (po[1].revents & POLLIN)
	    {
	      css_process_connect_request ();
	    }
	}
    }

  css_close_connection_to_master ();
  css_close_server_listen_socket ();

  css_set_shutdown_timeout (prm_get_bigint_value (PRM_ID_SHUTDOWN_WAIT_TIME));

  /* going down, so stop dispatching request */
  css_empty_job_queue ();
}

/*
 * css_get_master_request () -
 *   return:
 */
static int
css_get_master_request (CSS_CONN_ENTRY * conn, CSS_NET_PACKET ** recv_packet)
{
  CSS_NET_PACKET *tmp_recv_packet = NULL;

  if (css_net_packet_recv (&tmp_recv_packet, conn, -1, 0) != NO_ERRORS)
    {
      return ER_FAILED;
    }

  if (tmp_recv_packet->header.packet_type == COMMAND_TYPE)
    {
      *recv_packet = tmp_recv_packet;
      return NO_ERROR;
    }

  css_net_packet_free (tmp_recv_packet);
  return ER_FAILED;
}

/*
 * css_process_connect_request () -
 */
static void
css_process_connect_request ()
{
  SOCKET cli_fd = INVALID_SOCKET;
  CSS_CONN_ENTRY *conn = NULL;
  CSS_NET_PACKET *recv_packet = NULL;
  enum css_master_conn_type conn_type;
  unsigned short rid;

  cli_fd = css_master_accept (css_Listen_conn->fd);
  if (IS_INVALID_SOCKET (cli_fd))
    {
      goto error;
    }

  if ((conn = css_make_conn (cli_fd)) == NULL ||
      css_check_magic (conn) != NO_ERRORS ||
      css_recv_command_packet (conn, &recv_packet) != NO_ERRORS)
    {
      goto error;
    }

  conn_type = recv_packet->header.function_code;
  rid = recv_packet->header.request_id;

  if (conn_type == MASTER_CONN_TYPE_TO_SERVER)
    {
      if (css_accept_new_client (rid, conn) != NO_ERROR)
	{
	  goto error;
	}

      css_net_packet_free (recv_packet);
      er_log_debug (ARG_FILE_LINE, "css_process_connect_request");
      return;
    }

error:
  if (conn == NULL)
    {
      css_shutdown_socket (cli_fd);
    }
  else
    {
      css_free_conn (conn);
    }
  css_net_packet_free (recv_packet);
}

/*
 * css_accept_new_client () -
 */
static int
css_accept_new_client (unsigned short rid, CSS_CONN_ENTRY * conn)
{
  int reason;
  CSS_JOB_ENTRY job_entry;

  reason = htonl (SERVER_CONNECTED);

  css_send_data_packet (conn, rid, 1, (char *) &reason, sizeof (int));

  css_insert_into_active_conn_list (conn);

  CSS_JOB_ENTRY_SET (job_entry, conn, css_internal_request_handler, conn);

  return (css_add_to_job_queue (JOB_QUEUE_CLIENT, &job_entry));
}

/*
 * css_process_master_request () -
 *   return:
 *   master_fd(in):
 *   read_fd_var(in):
 *   exception_fd_var(in):
 */
static int
css_process_master_request (CSS_CONN_ENTRY * conn)
{
  int r;
  CSS_NET_PACKET *recv_packet = NULL;
  char *data = NULL;
  int datasize;
  CSS_MASTER_TO_SERVER_REQUEST request;

  if (css_get_master_request (conn, &recv_packet) != NO_ERROR)
    {
      return 0;
    }

  datasize = css_net_packet_get_recv_size (recv_packet, 0);
  if (datasize > 0)
    {
      data = css_net_packet_get_buffer (recv_packet, 0, -1, false);
    }

  r = 1;

  request = recv_packet->header.function_code;
  switch (request)
    {
    case SERVER_START_NEW_CLIENT:
      css_process_new_client (conn->fd);
      break;

    case SERVER_START_SHUTDOWN:
      r = 0;
      break;

    case SERVER_CHANGE_HA_MODE:
      css_process_change_server_ha_mode_request (data, datasize);
      break;

    default:
      assert (false);
      /* master do not respond */
      r = 0;
      break;
    }

  css_net_packet_free (recv_packet);

  return r;
}

/*
 * css_process_new_client () -
 *   return:
 *   master_fd(in):
 *   read_fd_var(in/out):
 *   exception_fd_var(in/out):
 */
static void
css_process_new_client (SOCKET master_fd)
{
  SOCKET new_fd;
  int reason;
  CSS_CONN_ENTRY *conn;
  unsigned short rid;
  CSS_CONN_ENTRY temp_conn;
  void *area;
  char buffer[1024];
  int length = 1024;

  /* receive new socket descriptor from the master */
  new_fd = css_open_new_socket_from_master (master_fd, &rid);
  if (IS_INVALID_SOCKET (new_fd))
    {
      return;
    }

  if (prm_get_bool_value (PRM_ID_ACCESS_IP_CONTROL) == true &&
      css_check_accessibility (new_fd) != NO_ERROR)
    {
      css_initialize_conn (&temp_conn, new_fd);

      reason = htonl (SERVER_INACCESSIBLE_IP);
      css_send_data_packet (&temp_conn, rid, 1,
			    (char *) &reason, (int) sizeof (int));

      area = er_get_area_error (buffer, &length);

      css_send_error_packet (&temp_conn, rid, (const char *) area, length);
      css_shutdown_conn (&temp_conn);
      er_clear ();
      return;
    }

  conn = css_make_conn (new_fd);
  if (conn == NULL)
    {
      css_initialize_conn (&temp_conn, new_fd);

      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_CSS_CLIENTS_EXCEEDED,
	      1, NUM_NORMAL_TRANS);
      reason = htonl (SERVER_CLIENTS_EXCEEDED);
      css_send_data_packet (&temp_conn, rid, 1,
			    (char *) &reason, (int) sizeof (int));

      area = er_get_area_error (buffer, &length);

      css_send_error_packet (&temp_conn, rid, (const char *) area, length);
      css_shutdown_conn (&temp_conn);
      er_clear ();
      return;
    }

  if (css_accept_new_client (rid, conn) != NO_ERROR)
    {
      css_shutdown_conn (conn);
      css_free_conn (conn);
    }
  er_log_debug (ARG_FILE_LINE, "css_process_new_client()");
}

/*
 * css_process_change_server_ha_mode_request() -
 *   return:
 */
static void
css_process_change_server_ha_mode_request (char *data, int datasize)
{
  HA_STATE server_state = HA_STATE_NA;
  bool force;
  THREAD_ENTRY *thread_p;

  if (data == NULL || datasize < (int) (sizeof (int) * 2))
    {
      server_state = HA_STATE_NA;
    }
  else
    {
      server_state = *((int *) data);
      data += sizeof (int);
      force = *((int *) data);
    }

  thread_p = thread_get_thread_entry_info ();
  assert (thread_p != NULL);

  if (css_change_ha_server_state (thread_p, server_state, force) != NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ERR_CSS_ERROR_FROM_SERVER,
	      1, "Cannot change server HA mode");
    }

  server_state = htonl ((int) svr_shm_get_server_state ());

  css_send_heartbeat_request (css_Master_conn, MASTER_CHANGE_SERVER_STATE,
			      1, (char *) &server_state,
			      sizeof (server_state));
}

/*
 * css_close_connection_to_master() -
 *   return:
 */
static void
css_close_connection_to_master (void)
{
  if (css_Master_conn != NULL)
    {
      css_shutdown_conn (css_Master_conn);
    }
  css_Master_conn = NULL;
}

/*
 * css_close_server_listen_socket() -
 */
static void
css_close_server_listen_socket (void)
{
  if (css_Listen_conn != NULL)
    {
      css_shutdown_conn (css_Listen_conn);
    }
  css_Listen_conn = NULL;
}

/*
 * css_shutdown_timeout() -
 *   return:
 */
bool
css_is_shutdown_timeout_expired (void)
{
  struct timeval timeout;

  /* css_Shutdown_timeout is set by shutdown request */
  if (css_Shutdown_timeout.tv_sec != 0 && gettimeofday (&timeout, NULL) == 0)
    {
      if (css_Shutdown_timeout.tv_sec <= timeout.tv_sec)
	{
	  return true;
	}
    }

  return false;
}

/*
 * dummy_sigurg_handler () - SIGURG signal handling thread
 *   return:
 *   sig(in):
 */
static void
dummy_sigurg_handler (UNUSED_ARG int sig)
{
}

/*
 * css_connection_handler_thread () - Accept/process request from
 *                                    one client
 *   return:
 *   arg(in):
 *
 * Note: One server thread per one client
 */
THREAD_RET_T THREAD_CALLING_CONVENTION
css_connection_handler_thread (void *arg_p)
{
  THREAD_ENTRY *tsd_ptr;
  CSS_EPOLL_INFO *epoll_info;

  tsd_ptr = (THREAD_ENTRY *) arg_p;

  thread_set_thread_entry_info (tsd_ptr);

  tsd_ptr->type = TT_CON_HANDLER;	/* server thread */
  tsd_ptr->conn_entry = NULL;
  tsd_ptr->tran_index = -1;
  tsd_ptr->status = TS_RUN;

  epoll_info = &css_Epoll_info[tsd_ptr->index_in_group];

  /* check if socket has error or client is down */
  while (tsd_ptr->shutdown == false && epoll_info->shutdown == false)
    {
#define EPOLL_MAX_EVENTS 1024
      int i, n;
      int poll_timeout = 1000;
      struct epoll_event event[EPOLL_MAX_EVENTS];
      CSS_JOB_ENTRY new_job;
      int check_time;

      check_time = time (NULL);
      n = epoll_wait (epoll_info->epoll_fd, event, EPOLL_MAX_EVENTS,
		      poll_timeout);
      if (n < 0)
	{
	  if (errno == EINTR)
	    {
	      continue;
	    }
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_SYSTEM_CALL, 1, "epoll_wait()");
	  break;
	}

      for (i = 0; i < n; i++)
	{
	  bool conn_err_flag = false;
	  CSS_CONN_ENTRY *conn = NULL;

	  conn = (CSS_CONN_ENTRY *) event[i].data.ptr;
	  if (conn != NULL && conn->epoll_check_err &&
	      check_time > conn->epoll_check_time)
	    {
	      if (event[i].events & EPOLLERR || event[i].events & EPOLLHUP)
		{
		  conn_err_flag = true;
		}
	      else if (event[i].events & EPOLLIN)
		{
		  char buf[1];
		  int r;

		  r = recv (conn->fd, buf, 1, MSG_PEEK | MSG_DONTWAIT);
		  if (r <= 0)
		    {
		      conn_err_flag = true;
		    }
		}

	      if (conn_err_flag)
		{
		  conn->con_close_handler_activated = true;

		  if (conn->status == CONN_OPEN)
		    {
		      css_epoll_del_conn (conn);

		      CSS_JOB_ENTRY_SET (new_job, conn,
					 css_con_close_handler, conn);

		      if (css_add_to_job_queue (JOB_QUEUE_CLOSE, &new_job) !=
			  NO_ERROR)
			{
			  assert (false);

			  net_server_conn_down (tsd_ptr, conn, true);
			}
		    }
		  else
		    {
		      conn->con_close_handler_activated = false;
		    }
		}
	    }
	}

      thread_sleep (100);
    }

  if (epoll_info->epoll_fd > 0)
    {
      close (epoll_info->epoll_fd);
    }

  er_stack_clearall ();
  er_clear ();

  tsd_ptr->status = TS_DEAD;

  return (THREAD_RET_T) 0;
}

/*
 * css_oob_handler_thread() -
 *   return:
 *   arg(in):
 */
void *
css_oob_handler_thread (void *arg)
{
  THREAD_ENTRY *thrd_entry;
  int sig;
  sigset_t sigurg_mask;
  struct sigaction act;

  thrd_entry = (THREAD_ENTRY *) arg;

  /* wait until THREAD_CREATE finish */
  pthread_mutex_lock (&thrd_entry->th_entry_lock);
  pthread_mutex_unlock (&thrd_entry->th_entry_lock);

  thread_set_thread_entry_info (thrd_entry);
  thrd_entry->status = TS_RUN;

  sigemptyset (&sigurg_mask);
  sigaddset (&sigurg_mask, SIGURG);

  memset (&act, 0, sizeof (act));
  act.sa_handler = dummy_sigurg_handler;
  sigaction (SIGURG, &act, NULL);

  pthread_sigmask (SIG_UNBLOCK, &sigurg_mask, NULL);

  while (!thrd_entry->shutdown)
    {
      sigwait (&sigurg_mask, &sig);
    }
  thrd_entry->status = TS_DEAD;

  return NULL;
}

/*
 * css_block_all_active_conn() - Before shutdown, stop all server thread
 *   return:
 *
 * Note:  All communication will be stopped
 */
void
css_block_all_active_conn (unsigned short stop_phase)
{
  CSS_CONN_ENTRY *conn;

  if (csect_enter (NULL, CSECT_CSS_ACTIVE_CONN, INF_WAIT) != NO_ERROR)
    {
      assert (false);
      return;
    }

  for (conn = css_Active_conn_anchor; conn != NULL; conn = conn->next)
    {
      if (conn->stop_phase != stop_phase)
	{
	  continue;
	}
      css_end_server_request (conn);
      if (!IS_INVALID_SOCKET (conn->fd) && conn->fd != css_Master_conn->fd)
	{
	  conn->stop_talk = true;
	  logtb_set_tran_index_interrupt (NULL, conn->tran_index, 1);
	}
    }

  csect_exit (CSECT_CSS_ACTIVE_CONN);
}

/*
 * css_internal_request_handler() -
 *   return:
 *   arg(in):
 *
 * Note: This routine is "registered" to be called when a new request is
 *       initiated by the client.
 *
 *       To now support multiple concurrent requests from the same client,
 *       check if a request is actually sent on the socket. If data was sent
 *       (not a request), then just return and the scheduler will wake up the
 *       thread that is blocking for data.
 */
int
css_internal_request_handler (THREAD_ENTRY * thread_p, CSS_THREAD_ARG arg)
{
  CSS_CONN_ENTRY *conn;
  unsigned short rid;
  unsigned int eid;
  int request, size = 0;
  char *buffer = NULL;
  int local_tran_index;
  int status = CSS_UNPLANNED_SHUTDOWN;
  CSS_NET_PACKET *recv_packet = NULL;

  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  local_tran_index = logtb_get_current_tran_index (thread_p);

  pthread_mutex_unlock (&thread_p->tran_index_lock);

  conn = (CSS_CONN_ENTRY *) arg;
  if (conn == NULL)
    {
      assert (0);
      return status;
    }

  thread_p->conn_entry = conn;

  css_epoll_set_check (conn, false);
  css_epoll_add_conn (conn);

  while (conn->status == CONN_OPEN && !thread_p->shutdown)
    {
      assert (conn->epoll_check_err == false);

      struct pollfd po[1] = { {0, 0, 0} };
      int n;

      po[0].fd = conn->fd;
      po[0].events = POLLIN;
      po[0].revents = 0;
      n = poll (po, 1, 1000);
      if (n == 0)
	{
	  continue;
	}
      else if (n < 0)
	{
	  if (errno == EINTR)
	    {
	      continue;
	    }
	  break;
	}
      else
	{
	  if (po[0].revents & POLLERR || po[0].revents & POLLHUP)
	    {
	      break;
	    }
	}


      recv_packet = NULL;
      if (css_recv_command_packet (conn, &recv_packet) != NO_ERRORS)
	{
	  conn->status = CONN_CLOSING;
	  break;
	}

      if (recv_packet->header.packet_type != COMMAND_TYPE)
	{
	  css_net_packet_free (recv_packet);
	  conn->status = CONN_CLOSING;
	  break;
	}

      pthread_mutex_lock (&thread_p->tran_index_lock);
      thread_p->tran_index = recv_packet->header.tran_index;
      pthread_mutex_unlock (&thread_p->tran_index_lock);

      css_epoll_set_check (conn, true);

      request = recv_packet->header.function_code;
      rid = recv_packet->header.request_id;

      size = css_net_packet_get_recv_size (recv_packet, 0);
      if (size > 0)
	{
	  buffer = css_net_packet_get_buffer (recv_packet, 0, -1, false);
	}

      /* 1. change thread's transaction id to this connection's */
      thread_p->recv_packet = recv_packet;

      assert (conn->tran_index == -1 ||
	      conn->tran_index == recv_packet->header.tran_index);

      conn->tran_index = recv_packet->header.tran_index;

      eid = css_return_eid_from_conn (conn, rid);
      /* 2. change thread's client, rid, tran_index for this request */
      thread_set_info (thread_p, conn->client_id, eid, conn->tran_index);

      /* 3. Call server_request() function */
      status = net_server_request (thread_p, eid, request, size, buffer);

      css_net_packet_free (recv_packet);

      thread_p->recv_packet = NULL;

      thread_p->check_interrupt = true;
      memset (&(thread_p->event_stats), 0, sizeof (EVENT_STAT));
      thread_p->on_trace = false;
      thread_p->check_groupid = true;
    }

  css_epoll_del_conn (conn);

  if (thread_p->shutdown == true)
    {
      THREAD_SLEEP (10);
    }

  css_shutdown_conn (conn);
  net_server_conn_down (thread_p, conn, false);

  thread_set_info (thread_p, -1, 0, local_tran_index);

  return status;
}

static int
css_con_close_handler (THREAD_ENTRY * thread_p, CSS_THREAD_ARG arg)
{
  int status = CSS_UNPLANNED_SHUTDOWN;
  int local_tran_index;
  CSS_CONN_ENTRY *conn;
  unsigned int eid = 0;

  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  local_tran_index = logtb_get_current_tran_index (thread_p);

  conn = (CSS_CONN_ENTRY *) arg;

  thread_p->tran_index = conn->tran_index;

  pthread_mutex_unlock (&thread_p->tran_index_lock);

  thread_set_info (thread_p, conn->client_id, eid, conn->tran_index);

  net_server_conn_down (thread_p, conn, true);
  conn->con_close_handler_activated = false;

  thread_set_info (thread_p, -1, 0, local_tran_index);

  return status;
}

/*
 * css_init() -
 *
 * Note: This routine is the entry point for the server interface. Once this
 *       routine is called, control will not return to the caller until the
 *       server/scheduler is stopped.
 */
int
css_init (const char *server_name)
{
  THREAD_ENTRY *thread_p;
  CSS_CONN_ENTRY *conn;
  int status = ER_FAILED;

  if (server_name == NULL)
    {
      return ER_FAILED;
    }

  if (css_epoll_init () != NO_ERROR)
    {
      return ER_FAILED;
    }

  /* startup worker/daemon threads */
  status = thread_start_workers ();
  if (status != NO_ERROR)
    {
      if (status == ER_CSS_PTHREAD_CREATE)
	{
	  /* thread creation error */
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_THREAD_STACK,
		  1, thread_num_total_threads ());
	}
      return ER_FAILED;
    }

  conn = css_register_to_master (HB_PTYPE_SERVER, server_name, NULL);
  if (conn != NULL)
    {
      char pname[PATH_MAX];
      int socket_fd;

      css_get_server_domain_path (pname, sizeof (pname), server_name);
      if (css_tcp_setup_server_datagram (pname, &socket_fd) == false)
	{
	  goto shutdown;
	}
      css_Listen_conn = css_make_conn (socket_fd);

      /* insert conn into active conn list */
      css_insert_into_active_conn_list (conn);

      css_Master_server_name = strdup (server_name);
      css_Master_conn = conn;

      status = hb_register_to_master (css_Master_conn, HB_PTYPE_SERVER);
      if (status != NO_ERROR)
	{
	  fprintf (stderr, "failed to heartbeat register.\n");
	  goto shutdown;
	}

      css_setup_server_loop ();

      status = NO_ERROR;
    }

  /*
   * start to shutdown server
   */
shutdown:
  /* stop threads */
  thread_stop_active_workers (THREAD_STOP_WORKERS_EXCEPT_LOGWR);

  /* we should flush all append pages before stop log writer */
  thread_p = thread_get_thread_entry_info ();
  assert_release (thread_p != NULL);

  LOG_CS_ENTER (thread_p);
  logpb_flush_pages_direct (thread_p);

#if !defined(NDEBUG)
  pthread_mutex_lock (&log_Gl.prior_info.prior_lsa_mutex);
  if (!LSA_EQ (&log_Gl.append.nxio_lsa, &log_Gl.prior_info.prior_lsa))
    {
      LOG_PRIOR_NODE *node;

      assert (LSA_LT (&log_Gl.append.nxio_lsa, &log_Gl.prior_info.prior_lsa));
      node = log_Gl.prior_info.prior_list_header;
      while (node != NULL)
	{
	  assert (node->log_header.trid == LOG_SYSTEM_TRANID);
	  node = node->next;
	}
    }
  pthread_mutex_unlock (&log_Gl.prior_info.prior_lsa_mutex);
#endif

  LOG_CS_EXIT ();

  thread_stop_active_workers (THREAD_STOP_LOGWR);

  css_close_connection_to_master ();
  css_close_server_listen_socket ();

  if (css_Master_server_name)
    {
      free_and_init (css_Master_server_name);
    }

  return status;
}

int
css_send_reply_to_client (CSS_CONN_ENTRY * conn, unsigned int eid,
			  int num_buffers, ...)
{
  int css_error;
  va_list args;

  assert (conn != NULL);

  va_start (args, num_buffers);

  css_error = css_send_data_packet_v (conn, CSS_RID_FROM_EID (eid),
				      num_buffers, args);

  va_end (args);

  if (css_error == NO_ERRORS)
    {
      return NO_ERROR;
    }
  else
    {
      /* TODO: erset */
      return ER_FAILED;
    }
}

/*
 * css_send_abort_to_client() - send an abort message to the client
 *   return:
 *   eid(in): enquiry id
 */
unsigned int
css_send_abort_to_client (CSS_CONN_ENTRY * conn, unsigned int eid)
{
  int rc = 0;

  assert (conn != NULL);

  rc = css_send_abort_request (conn, CSS_RID_FROM_EID (eid));

  conn->status = CONN_CLOSING;

  return (rc == NO_ERRORS) ? 0 : rc;
}

/*
 * css_end_server_request() - terminates the request from the client
 *   return:
 *   conn(in/out):
 */
void
css_end_server_request (CSS_CONN_ENTRY * conn)
{
  conn->status = CONN_CLOSING;
}

/*
 * css_set_client_version() - 
 */
void
css_set_client_version (THREAD_ENTRY * thread_p, const RYE_VERSION * version)
{
  CSS_CONN_ENTRY *conn;

  assert (thread_p != NULL);

  conn = thread_p->conn_entry;
  if (conn != NULL)
    {
      conn->peer_version = *version;
    }
}

bool
css_is_ha_repl_delayed (void)
{
  return ha_Repl_delay_detected;
}

void
css_set_ha_repl_delayed (void)
{
  ha_Repl_delay_detected = true;
}

void
css_unset_ha_repl_delayed (void)
{
  ha_Repl_delay_detected = false;
}

/*
 * css_transit_ha_server_state - request to transit the current HA server
 *                               state to the required state
 *   return: new state changed if successful or HA_STATE_NA
 *   req_state(in): the state for the server to transit
 *
 */
static HA_STATE
css_transit_ha_server_state (UNUSED_ARG THREAD_ENTRY * thread_p,
			     HA_STATE curr_server_state,
			     HA_STATE req_server_state)
{
  /*
   *
   * ha server state transition
   *
   * row    : current state
   * column : request state
   * value  : next state
   * -------------------------------------------------------------------------------
   *                | UNKNOWN  MASTER        TO_BE_MASTER  SLAVE        TO_BE_SLAVE
   * -------------------------------------------------------------------------------
   *  UNKNOWN       | UNKNOWN  MASTER        TO_BE_MASTER  SLAVE        TO_BE_SLAVE
   *  MASTER        | N/A      MASTER        N/A           TO_BE_SLAVE  TO_BE_SLAVE
   *  TO_BE_MASTER  | N/A      MASTER        TO_BE_MASTER  N/A          N/A
   *  SLAVE         | N/A      TO_BE_MASTER  N/A           SLAVE        N/A
   *  TO_BE_SLAVE   | N/A      N/A           N/A           SLAVE        TO_BE_SLAVE
   * --------------------------------------------------------------------------------------------
   */
  static HA_STATE server_State_Comp[5][5] = {
    /* UNKNOWN */
    {HA_STATE_UNKNOWN, HA_STATE_MASTER, HA_STATE_MASTER, HA_STATE_SLAVE,
     HA_STATE_SLAVE},
    /* MASTER */
    {HA_STATE_NA, HA_STATE_MASTER, HA_STATE_NA, HA_STATE_TO_BE_SLAVE,
     HA_STATE_TO_BE_SLAVE},
    /* TO_BE_MASTER */
    {HA_STATE_NA, HA_STATE_MASTER, HA_STATE_TO_BE_MASTER, HA_STATE_NA,
     HA_STATE_NA},
    /* SLAVE */
    {HA_STATE_NA, HA_STATE_TO_BE_MASTER, HA_STATE_NA, HA_STATE_SLAVE,
     HA_STATE_NA},
    /* TO_BE_SLAVE */
    {HA_STATE_NA, HA_STATE_NA, HA_STATE_NA, HA_STATE_SLAVE,
     HA_STATE_TO_BE_SLAVE}
  };

  if ((curr_server_state < HA_STATE_UNKNOWN
       || curr_server_state > HA_STATE_TO_BE_SLAVE)
      || (req_server_state < HA_STATE_UNKNOWN
	  || req_server_state > HA_STATE_TO_BE_SLAVE))
    {
      return HA_STATE_NA;
    }

  return server_State_Comp[curr_server_state][req_server_state];
}

/*
 * css_check_ha_server_state_for_client
 *   return: NO_ERROR or errno
 *   whence(in): 0: others, 1: register_client, 2: unregister_client
 */
int
css_check_ha_server_state_for_client (UNUSED_ARG THREAD_ENTRY * thread_p,
				      UNUSED_ARG int whence)
{
#define FROM_OTHERS             0
#define FROM_REGISTER_CLIENT    1
#define FROM_UNREGISTER_CLIENT  2
  int err = NO_ERROR;
  HA_STATE server_state;

  server_state = svr_shm_get_server_state ();

  switch (server_state)
    {
    case HA_STATE_TO_BE_MASTER:
    case HA_STATE_TO_BE_SLAVE:
      /* Server accepts clients even though it is in a to-be-master and to-be-slave state */
      break;

    default:
      break;
    }

  return err;
}

/*
 * css_change_ha_server_state - change the server's HA state
 *   return: NO_ERROR or ER_FAILED
 *   state(in): new state for server to be
 *   force(in): force to change
 */
int
css_change_ha_server_state (THREAD_ENTRY * thread_p,
			    HA_STATE req_server_state, bool force)
{
  HA_STATE curr_server_state, new_server_state;
  int error = NO_ERROR;
  bool need_change_server_state = false;

  assert (req_server_state >= HA_STATE_UNKNOWN
	  && req_server_state <= HA_STATE_DEAD);

  curr_server_state = svr_shm_get_server_state ();
  er_log_debug (ARG_FILE_LINE,
		"css_change_ha_server_state: ha_server_state %s "
		"state %s force %c \n",
		css_ha_state_string (curr_server_state),
		css_ha_state_string (req_server_state), (force ? 't' : 'f'));

  if (req_server_state == curr_server_state)
    {
      /* no chage server state */
      return NO_ERROR;
    }

  csect_enter (thread_p, CSECT_HA_SERVER_STATE, INF_WAIT);

  error = svr_shm_sync_node_info_to_repl ();
  if (error != NO_ERROR)
    {
      csect_exit (CSECT_HA_SERVER_STATE);
      return error;
    }

  curr_server_state = svr_shm_get_server_state ();
  if (req_server_state == curr_server_state)
    {
      /* no chage server state */
      csect_exit (CSECT_HA_SERVER_STATE);
      return NO_ERROR;
    }

  new_server_state = css_transit_ha_server_state (thread_p, curr_server_state,
						  req_server_state);
  if (new_server_state == HA_STATE_NA)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
	      "Invalid req_server_state");
      csect_exit (CSECT_HA_SERVER_STATE);

      return error;
    }

  need_change_server_state = true;
  switch (new_server_state)
    {
    case HA_STATE_TO_BE_MASTER:
    case HA_STATE_TO_BE_SLAVE:
      break;

    case HA_STATE_MASTER:
      if (curr_server_state == HA_STATE_TO_BE_MASTER)
	{
	  /* If log appliers have changed their state to done,
	   * go directly to active mode */
	  if (force == false && svr_shm_check_repl_done () == false)
	    {
	      need_change_server_state = false;
	    }
	}
      break;

    case HA_STATE_SLAVE:
      if (curr_server_state == HA_STATE_TO_BE_SLAVE)
	{
	  if (force == false
	      && logtb_count_active_write_clients (thread_p) > 0)
	    {
	      need_change_server_state = false;
	    }

	  logtb_shutdown_write_normal_clients (thread_p, force);
	}
      break;

    default:
      assert (false);

      new_server_state = HA_STATE_NA;
      need_change_server_state = false;
      break;
    }

  if (need_change_server_state == true)
    {
      if (new_server_state == HA_STATE_MASTER)
	{
	  er_log_debug (ARG_FILE_LINE, "css_change_ha_server_state: "
			"logtb_enable_update() \n");
	  logtb_enable_update (thread_p);
	}
      else if (new_server_state == HA_STATE_SLAVE)
	{
	  er_log_debug (ARG_FILE_LINE, "css_change_ha_server_state: "
			"logtb_disable_update() \n");
	  logtb_disable_update (thread_p);
	}

      svr_shm_set_server_state (new_server_state);

      /* append a dummy log record for LFT to wake LWTs up */
      log_append_ha_server_state (thread_p, new_server_state);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_CSS_SERVER_HA_MODE_CHANGE, 2,
	      css_ha_state_string (curr_server_state),
	      css_ha_state_string (svr_shm_get_server_state ()));
    }

  csect_exit (CSECT_HA_SERVER_STATE);

  return (new_server_state != HA_STATE_NA) ? NO_ERROR : ER_FAILED;
}

/*
 * css_notify_ha_apply_state - notify the log applier's HA state
 *   return: NO_ERROR or ER_FAILED
 *   state(in): new state to be recorded
 */
int
css_notify_ha_apply_state (THREAD_ENTRY * thread_p,
			   const PRM_NODE_INFO * node_info,
			   HA_APPLY_STATE state)
{
  int error = NO_ERROR;
  char host[256];

  assert (state >= HA_APPLY_STATE_UNREGISTERED
	  && state <= HA_APPLY_STATE_ERROR);

  css_ip_to_str (host, sizeof (host), node_info->ip);
  er_log_debug (ARG_FILE_LINE,
		"css_notify_ha_apply_state: node %s:%d state %s\n",
		host, node_info->port, css_ha_applier_state_string (state));

  error = csect_enter (thread_p, CSECT_HA_SERVER_STATE, INF_WAIT);
  if (error != NO_ERROR)
    {
      return error;
    }

  error = svr_shm_set_repl_info (node_info, state);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  csect_exit (CSECT_HA_SERVER_STATE);

  return NO_ERROR;

exit_on_error:
  assert (error != NO_ERROR);

  csect_exit (CSECT_HA_SERVER_STATE);

  return error;
}

#if defined(SERVER_MODE)
static int
css_check_accessibility (SOCKET new_fd)
{
  socklen_t saddr_len;
  struct sockaddr_in clt_sock_addr;
  unsigned char *ip_addr;
  int err_code;

  saddr_len = sizeof (clt_sock_addr);

  if (getpeername (new_fd,
		   (struct sockaddr *) &clt_sock_addr, &saddr_len) != 0)
    {
      return ER_FAILED;
    }

  ip_addr = (unsigned char *) &(clt_sock_addr.sin_addr);

  if (clt_sock_addr.sin_family == AF_UNIX ||
      (ip_addr[0] == 127 && ip_addr[1] == 0 &&
       ip_addr[2] == 0 && ip_addr[3] == 1))
    {
      return NO_ERROR;
    }

  if (css_Server_accessible_ip_info == NULL)
    {
      char ip_str[32];

      sprintf (ip_str, "%d.%d.%d.%d", (unsigned char) ip_addr[0],
	       ip_addr[1], ip_addr[2], ip_addr[3]);

      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_INACCESSIBLE_IP, 1, ip_str);

      return ER_INACCESSIBLE_IP;
    }

  csect_enter_as_reader (NULL, CSECT_ACL, INF_WAIT);
  err_code = css_check_ip (css_Server_accessible_ip_info, ip_addr);
  csect_exit (CSECT_ACL);

  if (err_code != NO_ERROR)
    {
      char ip_str[32];

      sprintf (ip_str, "%d.%d.%d.%d", (unsigned char) ip_addr[0],
	       ip_addr[1], ip_addr[2], ip_addr[3]);

      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_INACCESSIBLE_IP, 1, ip_str);
    }

  return err_code;
}

int
css_set_accessible_ip_info ()
{
  int ret_val;
  IP_INFO *tmp_accessible_ip_info;

  if (prm_get_string_value (PRM_ID_ACCESS_IP_CONTROL_FILE) == NULL)
    {
      css_Server_accessible_ip_info = NULL;
      return NO_ERROR;
    }

  if (prm_get_string_value (PRM_ID_ACCESS_IP_CONTROL_FILE)[0] ==
      PATH_SEPARATOR)
    {
      ip_list_file_name =
	(char *) prm_get_string_value (PRM_ID_ACCESS_IP_CONTROL_FILE);
    }
  else
    {
      ip_list_file_name =
	envvar_confdir_file (ip_file_real_path, PATH_MAX,
			     prm_get_string_value
			     (PRM_ID_ACCESS_IP_CONTROL_FILE));
    }

  ret_val = css_read_ip_info (&tmp_accessible_ip_info, ip_list_file_name);
  if (ret_val == NO_ERROR)
    {
      csect_enter (NULL, CSECT_ACL, INF_WAIT);

      if (css_Server_accessible_ip_info != NULL)
	{
	  css_free_accessible_ip_info ();
	}
      css_Server_accessible_ip_info = tmp_accessible_ip_info;

      csect_exit (CSECT_ACL);
    }

  return ret_val;
}

int
css_free_accessible_ip_info ()
{
  int ret_val;

  ret_val = css_free_ip_info (css_Server_accessible_ip_info);
  css_Server_accessible_ip_info = NULL;

  return ret_val;
}

void
xacl_dump (THREAD_ENTRY * thread_p, FILE * outfp)
{
  int i, j;

  if (outfp == NULL)
    {
      outfp = stdout;
    }

  fprintf (outfp, "access_ip_control=%s\n",
	   (prm_get_bool_value (PRM_ID_ACCESS_IP_CONTROL) ? "yes" : "no"));
  fprintf (outfp, "access_ip_control_file=%s\n",
	   (ip_list_file_name != NULL) ? ip_list_file_name : "NULL");

  if (prm_get_bool_value (PRM_ID_ACCESS_IP_CONTROL) == false
      || css_Server_accessible_ip_info == NULL)
    {
      return;
    }

  csect_enter_as_reader (thread_p, CSECT_ACL, INF_WAIT);

  for (i = 0; i < css_Server_accessible_ip_info->num_list; i++)
    {
      int address_index = i * IP_BYTE_COUNT;

      for (j = 0;
	   j < css_Server_accessible_ip_info->
	   address_list[address_index]; j++)
	{
	  fprintf (outfp, "%d%s",
		   css_Server_accessible_ip_info->
		   address_list[address_index + j + 1],
		   ((j != 3) ? "." : ""));
	}
      if (j != 4)
	{
	  fprintf (outfp, "*");
	}
      fprintf (outfp, "\n");
    }

  fprintf (outfp, "\n");
  csect_exit (CSECT_ACL);

  return;
}

int
xacl_reload (UNUSED_ARG THREAD_ENTRY * thread_p)
{
  return css_set_accessible_ip_info ();
}
#endif

static void
css_epoll_stop ()
{
  int i;
  int num_con_handlers;

  num_con_handlers = thread_num_con_handler_threads ();

  for (i = 0; i < num_con_handlers; i++)
    {
      css_Epoll_info[i].shutdown = true;
    }
}

static int
css_epoll_init ()
{
  int i;
  int num_con_handlers;
  int size;

  num_con_handlers = thread_num_con_handler_threads ();

  size = sizeof (CSS_EPOLL_INFO) * num_con_handlers;

  css_Epoll_info = malloc (size);
  if (css_Epoll_info == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, size);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  for (i = 0; i < num_con_handlers; i++)
    {
      css_Epoll_info[i].epoll_fd = epoll_create (100);
      if (css_Epoll_info[i].epoll_fd < 0)
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_SYSTEM_CALL, 1, "epoll_create()");

	  return ER_SYSTEM_CALL;
	}

      css_Epoll_info[i].count = 0;
      css_Epoll_info[i].shutdown = false;
    }

  return NO_ERROR;
}

static int
css_epoll_ctl (int epoll_fd, int epoll_op, CSS_CONN_ENTRY * conn)
{
  int rv;
  struct epoll_event ev;
  int sock_fd = conn->fd;

  ev.events = EPOLLERR | EPOLLHUP | EPOLLIN;

  ev.data.ptr = conn;

  rv = epoll_ctl (epoll_fd, epoll_op, sock_fd, &ev);
  if (rv < 0)
    {
      char buf[1024];

      conn->status = CONN_CLOSING;

      sprintf (buf, "epoll_ctl(%d)", epoll_op);

      er_set_with_oserror (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
			   ER_SYSTEM_CALL, 1, buf);
      return ER_SYSTEM_CALL;
    }

  return NO_ERROR;
}

void
css_epoll_set_check (CSS_CONN_ENTRY * conn, bool check_conn_err)
{
  if (conn == NULL)
    {
      return;
    }

  conn->epoll_check_time = time (NULL);
  conn->epoll_check_err = check_conn_err;
}

int
css_epoll_add_conn (CSS_CONN_ENTRY * conn)
{
  int i, min_index = 0;
  int num_con_handlers;
  int epoll_fd;

  if (conn == NULL)
    {
      return NO_ERROR;
    }

  /* find the epoll_fd that has fewer connextions */
  num_con_handlers = thread_num_con_handler_threads ();
  for (i = 1; i < num_con_handlers; i++)
    {
      if (css_Epoll_info[min_index].count > css_Epoll_info[i].count)
	{
	  min_index = i;
	}
    }

  css_Epoll_info[min_index].count++;
  epoll_fd = css_Epoll_info[min_index].epoll_fd;
  conn->epoll_info_index = min_index;

  return (css_epoll_ctl (epoll_fd, EPOLL_CTL_ADD, conn));
}

int
css_epoll_del_conn (CSS_CONN_ENTRY * conn)
{
  int index;

  if (conn == NULL)
    {
      return NO_ERROR;
    }

  index = conn->epoll_info_index;

  assert (index < thread_num_con_handler_threads ());

  css_Epoll_info[index].count--;
  if (css_Epoll_info[index].count < 0)
    {
      css_Epoll_info[index].count = MAX (css_Epoll_info[index].count, 0);
    }

  return (css_epoll_ctl (css_Epoll_info[index].epoll_fd, EPOLL_CTL_DEL,
			 conn));
}

/*
 * css_job_entry_list_init() - list initialization
 *   return: 0 if success, or error code
 */
static void
css_job_entry_list_init (CSS_JOB_ENTRY_LIST * list)
{
  memset (list, 0, sizeof (CSS_JOB_ENTRY_LIST));
}

/*
 * css_job_entry_list_add() - add an element to last of the list
 *   ptr(in/out): list
 *   data: data to add
 */
static void
css_job_entry_list_add (CSS_JOB_ENTRY_LIST * list, CSS_JOB_ENTRY * data)
{
  if (data == NULL)
    {
      assert (0);
      return;
    }

  data->next = NULL;

  if (list->front == NULL)
    {
      list->front = data;
      list->back = data;
    }
  else
    {
      list->front->next = data;
      list->front = data;
    }
}

/*
 * css_job_entry_list_remove() - remove the first entry of the list
 *   return: removed data
 *   ptr(in/out): list
 */
static CSS_JOB_ENTRY *
css_job_entry_list_remove (CSS_JOB_ENTRY_LIST * list)
{
  void *data;

  if (list->back == NULL)
    {
      return NULL;
    }

  data = list->back;

  list->back = list->back->next;
  if (list->back == NULL)
    {
      list->front = NULL;
    }

  return data;
}
