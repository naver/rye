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
 * broker.c -
 */

#ident "$Id$"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <errno.h>
#include <fcntl.h>
#include <assert.h>

#include <pthread.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/time.h>
#include <sys/un.h>

#ifdef BROKER_DEBUG
#include <sys/time.h>
#endif

#include "connection_defs.h"
#include "connection_cl.h"
#include "client_support.h"

#include "system_parameter.h"
#include "databases_file.h"
#include "util_func.h"

#include "cas_error.h"
#include "cas_common.h"
#include "broker_env_def.h"
#include "broker_shm.h"
#include "broker_util.h"
#include "broker_filename.h"

#include "broker_send_fd.h"
#include "broker_recv_fd.h"
#include "broker_log.h"
#include "broker.h"
#include "cas_cci_internal.h"
#include "language_support.h"

#include "rye_master_shm.h"

#include "dbdef.h"

#ifdef BROKER_RESTART_DEBUG
#define		PS_CHK_PERIOD		30
#else
#define		PS_CHK_PERIOD		600
#endif

#define		BUFFER_SIZE		ONE_K

#define		ENV_BUF_INIT_SIZE	512
#define		ALIGN_ENV_BUF_SIZE(X)	\
	((((X) + ENV_BUF_INIT_SIZE) / ENV_BUF_INIT_SIZE) * ENV_BUF_INIT_SIZE)

#define         MONITOR_SERVER_INTERVAL 5

#define JOB_COUNT_MAX		1000000

/* num of collecting counts per monitoring interval */
#define NUM_COLLECT_COUNT_PER_INTVL     4
#define HANG_COUNT_THRESHOLD_RATIO      0.5

#if defined(BYTE_ORDER_BIG_ENDIAN)
#define net_htoni64(X)          X
#else
static int64_t net_htoni64 (int64_t from);
#endif
#define net_ntohi64(X)          net_htoni64(X)

/* server state */
enum SERVER_STATE
{
  SERVER_STATE_UNKNOWN = 0,
  SERVER_STATE_DEAD = 1,
  SERVER_STATE_DEREGISTERED = 2,
  SERVER_STATE_STARTED = 3,
  SERVER_STATE_NOT_REGISTERED = 4,
  SERVER_STATE_REGISTERED = 5,
  SERVER_STATE_REGISTERED_AND_TO_BE_STANDBY = 6,
  SERVER_STATE_REGISTERED_AND_ACTIVE = 7,
  SERVER_STATE_REGISTERED_AND_TO_BE_ACTIVE = 8
};

static void cleanup (int signo);
static int init_mgmt_socket (void);
static int init_service_broker_socket (void);
static int broker_init_shm (void);

static void cas_monitor_worker (T_APPL_SERVER_INFO * as_info_p, int br_index,
				int as_index, int *busy_uts);
static void psize_check_worker (T_APPL_SERVER_INFO * as_info_p, int br_index,
				int as_index);

static THREAD_FUNC receiver_thr_f (void *arg);
static THREAD_FUNC dispatch_thr_f (void *arg);
static THREAD_FUNC psize_check_thr_f (void *arg);
static THREAD_FUNC cas_monitor_thr_f (void *arg);
static THREAD_FUNC hang_check_thr_f (void *arg);
static THREAD_FUNC server_monitor_thr_f (void *arg);

static int run_appl_server (T_APPL_SERVER_INFO * as_info_p, int br_index,
			    int as_index);
static int stop_appl_server (T_APPL_SERVER_INFO * as_info_p, int br_index,
			     int as_index);
static void restart_appl_server (T_APPL_SERVER_INFO * as_info_p, int br_index,
				 int as_index);

static int find_idle_cas (void);
static int find_drop_as_index (void);
static int find_add_as_index (void);
static bool broker_add_new_cas (void);
static bool broker_drop_one_cas_by_time_to_kill (void);

static void check_cas_log (const char *br_name,
			   T_APPL_SERVER_INFO * as_info_p, int as_index);
static void get_as_sql_log_filename (char *log_filename,
				     const char *broker_name, int as_index);
static void get_as_slow_log_filename (char *log_filename,
				      const char *broker_name, int as_index);

static int insert_db_server_check_list (T_DB_SERVER * list_p,
					int check_list_cnt,
					const char *db_name,
					const char *db_host);

T_SHM_BROKER *shm_Br = NULL;
int br_Index = -1;
int br_Process_flag = 1;
T_SHM_APPL_SERVER *shm_Appl;

static SOCKET br_Listen_sock_fd;

static T_BROKER_INFO *br_Info_p = NULL;

static pthread_cond_t clt_Table_cond;
static pthread_mutex_t clt_Table_mutex;
static pthread_mutex_t run_Appl_mutex;
static pthread_mutex_t broker_Shm_mutex;

static char run_Appl_server_flag = 0;

static int current_Dropping_as_index = -1;

static int num_Busy_uts = 0;

static int max_Open_fd = 128;

static int hold_Job = 0;

static T_BR_INIT_ERROR br_Init_error = { 0, 0 };

int
main ()
{
  pthread_t receiver_thread;
  pthread_t dispatch_thread;
  pthread_t cas_monitor_thread;
  pthread_t psize_check_thread;
  pthread_t hang_check_thread;
  pthread_t server_monitor_thread;

  int error;

  er_init ("broker.err", ER_EXIT_DEFAULT);

  error = lang_init ();
  if (error != NO_ERROR)
    {
      goto error1;
    }

  error = broker_init_shm ();
  if (error)
    {
      goto error1;
    }

  signal (SIGTERM, cleanup);
  signal (SIGINT, cleanup);
  signal (SIGCHLD, SIG_IGN);
  signal (SIGPIPE, SIG_IGN);

  cci_set_client_functions (or_pack_db_idxkey, db_idxkey_is_null,
			    or_db_idxkey_size, db_get_string);

  pthread_cond_init (&clt_Table_cond, NULL);
  pthread_mutex_init (&clt_Table_mutex, NULL);
  pthread_mutex_init (&run_Appl_mutex, NULL);
  pthread_mutex_init (&broker_Shm_mutex, NULL);

  if (shm_Br->br_info[br_Index].broker_type == SHARD_MGMT ||
      shm_Br->br_info[br_Index].broker_type == LOCAL_MGMT)
    {
      if (init_mgmt_socket () == -1)
	{
	  goto error1;
	}
    }
  else
    {
      if (init_service_broker_socket () == -1)
	{
	  goto error1;
	}
    }

  br_log_init (shm_Appl);
  set_rye_file (FID_LOG_DIR, shm_Appl->log_dir,
		shm_Br->br_info[br_Index].name);

  sysprm_load_and_init (NULL);

  shm_Br->br_info[br_Index].start_time = time (NULL);

  while (shm_Br->br_info[br_Index].ready_to_service != true)
    {
      THREAD_SLEEP (200);
    }

  if (shm_Br->br_info[br_Index].broker_type == SHARD_MGMT)
    {
      T_BROKER_INFO *local_mgmt_br_info;
      local_mgmt_br_info = ut_find_broker (shm_Br->br_info,
					   shm_Br->num_broker,
					   BR_LOCAL_MGMT_NAME, LOCAL_MGMT);

      if (local_mgmt_br_info == NULL ||
	  shd_mg_init (shm_Br->br_info[br_Index].port,
		       local_mgmt_br_info->port,
		       shm_Br->br_info[br_Index].shard_metadb) < 0)
	{
	  br_set_init_error (BR_ER_INIT_SHARD_MGMT_INIT_FAIL, 0);
	  RYE_CLOSE_SOCKET (br_Listen_sock_fd);
	  goto error1;
	}

      THREAD_BEGIN (receiver_thread, shard_mgmt_receiver_thr_f, NULL);
    }
  else if (shm_Br->br_info[br_Index].broker_type == LOCAL_MGMT)
    {
      int num_mgmt_thr = 4;
      int i;

      signal (SIGCHLD, SIG_DFL);

      if (local_mgmt_init () < 0)
	{
	  br_set_init_error (BR_ER_INIT_LOCAL_MGMT_INIT_FAIL, 0);
	  goto error1;
	}

      for (i = 0; i < num_mgmt_thr; i++)
	{
	  THREAD_BEGIN (receiver_thread, local_mgmt_receiver_thr_f, NULL);
	}
    }
  else
    {
      THREAD_BEGIN (receiver_thread, receiver_thr_f, NULL);

      THREAD_BEGIN (dispatch_thread, dispatch_thr_f, NULL);
      THREAD_BEGIN (cas_monitor_thread, cas_monitor_thr_f, NULL);
      THREAD_BEGIN (server_monitor_thread, server_monitor_thr_f, NULL);

    }

  THREAD_BEGIN (psize_check_thread, psize_check_thr_f, NULL);
  THREAD_BEGIN (hang_check_thread, hang_check_thr_f, NULL);

  if (br_Process_flag)
    {
      if (shm_Br && br_Index >= 0)
	{
	  assert (br_Init_error.err_code == 0);
	  memset (&shm_Br->br_info[br_Index].br_init_err, 0,
		  sizeof (T_BR_INIT_ERROR));
	}
    }

  while (br_Process_flag)
    {
      THREAD_SLEEP (100);

      if (shm_Br->br_info[br_Index].auto_add_appl_server == OFF)
	{
	  continue;
	}

      broker_drop_one_cas_by_time_to_kill ();
    }				/* end of while (br_Process_flag) */

error1:
  if (shm_Br && br_Index >= 0)
    {
      assert (br_Init_error.err_code != 0);
      shm_Br->br_info[br_Index].br_init_err = br_Init_error;
    }

  return -1;
}

static void
cleanup (int signo)
{
  signal (signo, SIG_IGN);

  br_Process_flag = 0;
  RYE_CLOSE_SOCKET (br_Listen_sock_fd);
  exit (0);
}

void
br_send_result_to_client (int sock_fd, int err_code,
			  const T_MGMT_RESULT_MSG * result_msg)
{
  T_BROKER_RESPONSE_NET_MSG res_msg;

  if (result_msg == NULL)
    {
      brres_msg_pack (&res_msg, err_code, 0, NULL);
    }
  else
    {
      brres_msg_pack (&res_msg, err_code, result_msg->num_msg,
		      result_msg->msg_size);
    }
  br_write_nbytes_to_client (sock_fd, res_msg.msg_buffer,
			     res_msg.msg_buffer_size,
			     BR_DEFAULT_WRITE_TIMEOUT);

  if (result_msg != NULL)
    {
      int i;

      for (i = 0; i < result_msg->num_msg; i++)
	{
	  br_write_nbytes_to_client (sock_fd, result_msg->msg[i],
				     result_msg->msg_size[i],
				     BR_DEFAULT_WRITE_TIMEOUT);
	}
    }
}

static THREAD_FUNC
receiver_thr_f (UNUSED_ARG void *arg)
{
  T_SOCKLEN clt_sock_addr_len;
  struct sockaddr_in clt_sock_addr;
  SOCKET clt_sock_fd;
  SOCKET mgmt_sock_fd;
  int job_queue_size;
  T_MAX_HEAP_NODE *job_queue;
  T_MAX_HEAP_NODE new_job;
  int job_count;
  int one = 1;
  T_BROKER_REQUEST_MSG *br_req_msg;
  int timeout;
  int client_ip_addr;
  struct timeval mgmt_recv_time;
  int error;
  ER_MSG_INFO *er_msg;

  er_msg = malloc (sizeof (ER_MSG_INFO));
  error = er_set_msg_info (er_msg);
  if (error != NO_ERROR)
    {
      return NULL;
    }

  job_queue_size = shm_Appl->job_queue_size;
  job_queue = shm_Appl->job_queue;
  job_count = 1;

  signal (SIGPIPE, SIG_IGN);

  timeout = 5;
  if (setsockopt (br_Listen_sock_fd, IPPROTO_TCP, TCP_DEFER_ACCEPT,
		  (char *) &timeout, sizeof (timeout)) < 0)
    {
      // assert (0);
    }

  br_req_msg = brreq_msg_alloc (BRREQ_OP_CODE_MSG_MAX_SIZE);
  if (br_req_msg == NULL)
    {
      br_Process_flag = 0;
      br_set_init_error (BR_ER_INIT_NO_MORE_MEMORY, 0);
    }

  while (br_Process_flag)
    {
      clt_sock_addr_len = sizeof (clt_sock_addr);
      mgmt_sock_fd = accept (br_Listen_sock_fd,
			     (struct sockaddr *) &clt_sock_addr,
			     &clt_sock_addr_len);
      if (IS_INVALID_SOCKET (mgmt_sock_fd))
	{
	  continue;
	}

      if (shm_Br->br_info[br_Index].monitor_hang_flag
	  && shm_Br->br_info[br_Index].reject_client_flag)
	{
	  shm_Br->br_info[br_Index].reject_client_count++;
	  RYE_CLOSE_SOCKET (mgmt_sock_fd);
	  continue;
	}

      if (br_read_broker_request_msg (mgmt_sock_fd, br_req_msg) < 0 ||
	  !IS_NORMAL_BROKER_OPCODE (br_req_msg->op_code))
	{
	  RYE_CLOSE_SOCKET (mgmt_sock_fd);
	  shm_Br->br_info[br_Index].connect_fail_count++;
	  continue;
	}

      clt_sock_fd = recv_fd (mgmt_sock_fd, &client_ip_addr, &mgmt_recv_time);
      if (clt_sock_fd < 0)
	{
	  RYE_CLOSE_SOCKET (mgmt_sock_fd);
	  shm_Br->br_info[br_Index].connect_fail_count++;
	  continue;
	}

      br_write_nbytes_to_client (mgmt_sock_fd, (char *) &one, sizeof (one),
				 BR_SOCKET_TIMEOUT_SEC);
      RYE_CLOSE_SOCKET (mgmt_sock_fd);

      if (fcntl (clt_sock_fd, F_SETFL, FNDELAY) < 0)
	{
	  RYE_CLOSE_SOCKET (mgmt_sock_fd);
	  shm_Br->br_info[br_Index].connect_fail_count++;
	  continue;
	}

      setsockopt (clt_sock_fd, IPPROTO_TCP, TCP_NODELAY, (char *) &one,
		  sizeof (one));
      ut_set_keepalive (clt_sock_fd);


      if (br_req_msg->op_code == BRREQ_OP_CODE_CAS_CONNECT)
	{
	  if (job_queue[0].id == job_queue_size)
	    {
	      br_send_result_to_client (clt_sock_fd, BR_ER_FREE_SERVER, NULL);
	      RYE_CLOSE_SOCKET (clt_sock_fd);
	      shm_Br->br_info[br_Index].connect_fail_count++;
	      continue;
	    }

	  if (max_Open_fd < clt_sock_fd)
	    {
	      max_Open_fd = clt_sock_fd;
	    }

	  job_count = (job_count >= JOB_COUNT_MAX) ? 1 : job_count + 1;
	  new_job.id = job_count;
	  new_job.clt_sock_fd = clt_sock_fd;
	  new_job.recv_time = mgmt_recv_time;
	  new_job.priority = 0;
	  new_job.port = ntohs (0);
	  memcpy (new_job.ip_addr, &client_ip_addr, 4);
	  new_job.clt_type = br_req_msg->clt_type;
	  new_job.clt_version = br_req_msg->clt_version;

	  while (1)
	    {
	      pthread_mutex_lock (&clt_Table_mutex);
	      if (max_heap_insert (job_queue, job_queue_size, &new_job) < 0)
		{
		  pthread_mutex_unlock (&clt_Table_mutex);
		  THREAD_SLEEP (100);
		}
	      else
		{
		  pthread_cond_signal (&clt_Table_cond);
		  pthread_mutex_unlock (&clt_Table_mutex);
		  break;
		}
	    }
	}
      else if (br_req_msg->op_code == BRREQ_OP_CODE_PING)
	{
	  br_send_result_to_client (clt_sock_fd, 0, NULL);
	  RYE_CLOSE_SOCKET (clt_sock_fd);
	  shm_Br->br_info[br_Index].ping_req_count++;
	}
      else if (br_req_msg->op_code == BRREQ_OP_CODE_QUERY_CANCEL)
	{
	  int ret_code = CAS_ER_QUERY_CANCEL;

	  int cas_id, cas_pid;
	  const char *cancel_msg;
	  int msg_remain;

	  brreq_msg_unpack_port_name (br_req_msg, &cancel_msg, &msg_remain);

	  if (cancel_msg == NULL || msg_remain < 8)
	    {
	      cas_id = -1;
	      cas_pid = -1;
	    }
	  else
	    {
	      memcpy ((char *) &cas_id, cancel_msg, 4);
	      memcpy ((char *) &cas_pid, cancel_msg + 4, 4);
	      cas_id = ntohl (cas_id) - 1;
	      cas_pid = ntohl (cas_pid);
	    }

	  if (cas_id >= 0
	      && cas_id < shm_Br->br_info[br_Index].appl_server_max_num
	      && shm_Appl->info.as_info[cas_id].service_flag == SERVICE_ON
	      && shm_Appl->info.as_info[cas_id].pid == cas_pid
	      && shm_Appl->info.as_info[cas_id].uts_status == UTS_STATUS_BUSY
	      && memcmp (&shm_Appl->info.as_info[cas_id].cas_clt_ip,
			 &client_ip_addr, 4) == 0)
	    {
	      ret_code = 0;
	      kill (cas_pid, SIGUSR1);
	    }

	  br_send_result_to_client (clt_sock_fd, ret_code, NULL);

	  RYE_CLOSE_SOCKET (clt_sock_fd);
	  shm_Br->br_info[br_Index].cancel_req_count++;
	}
      else
	{
	  br_send_result_to_client (clt_sock_fd, CAS_ER_COMMUNICATION, NULL);
	  RYE_CLOSE_SOCKET (clt_sock_fd);
	  shm_Br->br_info[br_Index].connect_fail_count++;
	}
    }

  brreq_msg_free (br_req_msg);

  return NULL;
}

int
br_read_broker_request_msg (SOCKET clt_sock_fd,
			    T_BROKER_REQUEST_MSG * br_req_msg)
{
  char *msg_buffer = br_req_msg->msg_buffer;

  if (br_read_nbytes_from_client (clt_sock_fd, msg_buffer, BRREQ_MSG_SIZE,
				  BR_DEFAULT_READ_TIMEOUT) < 0)
    {
      return -1;
    }

  if (brreq_msg_unpack (br_req_msg) < 0)
    {
      return -1;
    }

  if (br_read_nbytes_from_client (clt_sock_fd, br_req_msg->op_code_msg,
				  br_req_msg->op_code_msg_size,
				  BR_DEFAULT_READ_TIMEOUT) < 0)
    {
      return -1;
    }

  return 0;
}

static THREAD_FUNC
dispatch_thr_f (UNUSED_ARG void *arg)
{
  T_MAX_HEAP_NODE *job_queue;
  T_MAX_HEAP_NODE cur_job;
  SOCKET srv_sock_fd;

  int as_index, i;

  int error;
  ER_MSG_INFO *er_msg;

  er_msg = malloc (sizeof (ER_MSG_INFO));
  error = er_set_msg_info (er_msg);
  if (error != NO_ERROR)
    {
      return NULL;
    }

  job_queue = shm_Appl->job_queue;

  while (br_Process_flag)
    {
      for (i = 0; i < shm_Br->br_info[br_Index].appl_server_max_num; i++)
	{
	  if (shm_Appl->info.as_info[i].service_flag == SERVICE_OFF)
	    {
	      if (shm_Appl->info.as_info[i].uts_status == UTS_STATUS_IDLE)
		shm_Appl->info.as_info[i].service_flag = SERVICE_OFF_ACK;
	    }
	}

      pthread_mutex_lock (&clt_Table_mutex);
      if (max_heap_delete (job_queue, &cur_job) < 0)
	{
	  struct timespec ts;
	  struct timeval tv;
	  int r;

	  gettimeofday (&tv, NULL);
	  ts.tv_sec = tv.tv_sec;
	  ts.tv_nsec = (tv.tv_usec + 30000) * 1000;
	  if (ts.tv_nsec > 1000000000)
	    {
	      ts.tv_sec += 1;
	      ts.tv_nsec -= 1000000000;
	    }
	  r = pthread_cond_timedwait (&clt_Table_cond, &clt_Table_mutex, &ts);
	  if (r != 0)
	    {
	      pthread_mutex_unlock (&clt_Table_mutex);
	      continue;
	    }
	  r = max_heap_delete (job_queue, &cur_job);
	  assert (r == 0);
	}

      hold_Job = 1;
      max_heap_incr_priority (job_queue);
      pthread_mutex_unlock (&clt_Table_mutex);

    retry:
      while (1)
	{
	  as_index = find_idle_cas ();
	  if (as_index < 0)
	    {
	      if (broker_add_new_cas ())
		{
		  continue;
		}
	      else
		{
		  THREAD_SLEEP (30);
		}
	    }
	  else
	    {
	      break;
	    }
	}

      hold_Job = 0;

      shm_Appl->info.as_info[as_index].num_connect_requests++;

      shm_Appl->info.as_info[as_index].clt_version = cur_job.clt_version;
      shm_Appl->info.as_info[as_index].client_type = cur_job.clt_type;
      memcpy (shm_Appl->info.as_info[as_index].cas_clt_ip, cur_job.ip_addr,
	      4);
      shm_Appl->info.as_info[as_index].cas_clt_port = cur_job.port;

      srv_sock_fd = br_connect_srv (shm_Br->br_info[br_Index].name,
				    false, as_index);

      if (!IS_INVALID_SOCKET (srv_sock_fd))
	{
	  int ip_addr;
	  int ret_val;
	  int con_status, uts_status;

	  con_status = htonl (shm_Appl->info.as_info[as_index].con_status);

	  if (br_write_nbytes_to_client (srv_sock_fd, (char *) &con_status,
					 sizeof (int),
					 BR_SOCKET_TIMEOUT_SEC) < 0)
	    {
	      RYE_CLOSE_SOCKET (srv_sock_fd);
	      goto retry;
	    }

	  ret_val = br_read_nbytes_from_client (srv_sock_fd,
						(char *) &con_status,
						sizeof (int),
						BR_SOCKET_TIMEOUT_SEC);
	  if ((ret_val < 0) || ntohl (con_status) != CON_STATUS_IN_TRAN)
	    {
	      RYE_CLOSE_SOCKET (srv_sock_fd);
	      goto retry;
	    }

	  memcpy (&ip_addr, cur_job.ip_addr, 4);
	  ret_val = send_fd (srv_sock_fd, cur_job.clt_sock_fd, ip_addr,
			     &cur_job.recv_time);
	  if (ret_val > 0)
	    {
	      ret_val = br_read_nbytes_from_client (srv_sock_fd,
						    (char *) &uts_status,
						    sizeof (int),
						    BR_SOCKET_TIMEOUT_SEC);
	    }
	  RYE_CLOSE_SOCKET (srv_sock_fd);

	  if (ret_val < 0)
	    {
	      br_send_result_to_client (cur_job.clt_sock_fd,
					BR_ER_FREE_SERVER, NULL);
	    }
	  else
	    {
	      shm_Appl->info.as_info[as_index].num_request++;
	    }
	}
      else
	{
	  goto retry;
	}

      RYE_CLOSE_SOCKET (cur_job.clt_sock_fd);
    }

  return NULL;
}

SOCKET
br_mgmt_accept (unsigned char *clt_ip_addr)
{
  T_SOCKLEN clt_sock_addr_len;
  struct sockaddr_in clt_sock_addr;
  SOCKET clt_sock_fd;
  int one = 1;

  clt_sock_addr_len = sizeof (clt_sock_addr);
  clt_sock_fd = accept (br_Listen_sock_fd, (struct sockaddr *) &clt_sock_addr,
			&clt_sock_addr_len);
  if (IS_INVALID_SOCKET (clt_sock_fd))
    {
      return INVALID_SOCKET;
    }

  memcpy (clt_ip_addr, &(clt_sock_addr.sin_addr), 4);

  if (fcntl (clt_sock_fd, F_SETFL, FNDELAY) < 0)
    {
      RYE_CLOSE_SOCKET (clt_sock_fd);
      return INVALID_SOCKET;
    }

  if (setsockopt (clt_sock_fd, IPPROTO_TCP, TCP_NODELAY, (char *) &one,
		  sizeof (one)) < 0)
    {
      assert (0);
    }

  ut_set_keepalive (clt_sock_fd);

  return clt_sock_fd;
}

static int
init_mgmt_socket (void)
{
  int n;
  int one = 1;
  unsigned short port;
  struct sockaddr_in sock_addr;
  int sock_addr_len;
  int timeout = 5;

  port = (unsigned short) shm_Br->br_info[br_Index].port;

  /* get a Unix stream socket */
  br_Listen_sock_fd = socket (AF_INET, SOCK_STREAM, 0);
  if (IS_INVALID_SOCKET (br_Listen_sock_fd))
    {
      br_set_init_error (BR_ER_INIT_CANT_CREATE_SOCKET, errno);
      return (-1);
    }
  if ((setsockopt (br_Listen_sock_fd, SOL_SOCKET, SO_REUSEADDR, (char *) &one,
		   sizeof (one))) < 0)
    {
      br_set_init_error (BR_ER_INIT_CANT_CREATE_SOCKET, errno);
      return (-1);
    }
  memset (&sock_addr, 0, sizeof (struct sockaddr_in));
  sock_addr.sin_family = AF_INET;
  sock_addr.sin_port = htons (port);
  sock_addr_len = sizeof (struct sockaddr_in);

  n = INADDR_ANY;
  memcpy (&sock_addr.sin_addr, &n, sizeof (int));

  if (bind (br_Listen_sock_fd, (struct sockaddr *) &sock_addr, sock_addr_len)
      < 0)
    {
      br_set_init_error (BR_ER_INIT_CANT_BIND, errno);
      return (-1);
    }

  if (listen (br_Listen_sock_fd, shm_Appl->job_queue_size) < 0)
    {
      br_set_init_error (BR_ER_INIT_CANT_BIND, 0);
      return (-1);
    }

  if (setsockopt (br_Listen_sock_fd, IPPROTO_TCP, TCP_DEFER_ACCEPT,
		  (char *) &timeout, sizeof (timeout)) < 0)
    {
      assert (0);
    }

  return (0);
}

static int
init_service_broker_socket (void)
{
  int one = 1;
  int sock_addr_len;
  struct sockaddr_un sock_addr;

  /* get a Unix stream socket */
  br_Listen_sock_fd = socket (AF_UNIX, SOCK_STREAM, 0);
  if (IS_INVALID_SOCKET (br_Listen_sock_fd))
    {
      br_set_init_error (BR_ER_INIT_CANT_CREATE_SOCKET, errno);
      return INVALID_SOCKET;
    }
  if ((setsockopt (br_Listen_sock_fd, SOL_SOCKET, SO_REUSEADDR, (char *) &one,
		   sizeof (one))) < 0)
    {
      br_set_init_error (BR_ER_INIT_CANT_CREATE_SOCKET, errno);
      RYE_CLOSE_SOCKET (br_Listen_sock_fd);
      return -1;
    }

  memset (&sock_addr, 0, sizeof (struct sockaddr_un));
  sock_addr.sun_family = AF_UNIX;

  ut_get_broker_port_name (sock_addr.sun_path, shm_Br->br_info[br_Index].name,
			   sizeof (sock_addr.sun_path));
  sock_addr_len =
    strlen (sock_addr.sun_path) + sizeof (sock_addr.sun_family) + 1;

  if (bind (br_Listen_sock_fd, (struct sockaddr *) &sock_addr, sock_addr_len)
      < 0)
    {
      br_set_init_error (BR_ER_INIT_CANT_BIND, 0);
      RYE_CLOSE_SOCKET (br_Listen_sock_fd);
      return -1;
    }

  if (listen (br_Listen_sock_fd, 3) < 0)
    {
      br_set_init_error (BR_ER_INIT_CANT_BIND, 0);
      RYE_CLOSE_SOCKET (br_Listen_sock_fd);
      return -1;
    }

  return (0);
}

static bool
broker_add_new_cas (void)
{
  int cur_appl_server_num;
  int add_as_index;
  int pid;

  cur_appl_server_num = shm_Br->br_info[br_Index].appl_server_num;

  /* ADD UTS */
  if (cur_appl_server_num >= shm_Br->br_info[br_Index].appl_server_max_num)
    {
      return false;
    }

  add_as_index = find_add_as_index ();
  if (add_as_index < 0)
    {
      return false;
    }

  pid = run_appl_server (&(shm_Appl->info.as_info[add_as_index]), br_Index,
			 add_as_index);
  if (pid <= 0)
    {
      return false;
    }

  pthread_mutex_lock (&broker_Shm_mutex);
  shm_Appl->info.as_info[add_as_index].pid = pid;
  shm_Appl->info.as_info[add_as_index].psize =
    (int) (os_get_mem_size (pid, MEM_VSIZE) / ONE_K);
  shm_Appl->info.as_info[add_as_index].psize_time = time (NULL);
  shm_Appl->info.as_info[add_as_index].uts_status = UTS_STATUS_IDLE;
  shm_Appl->info.as_info[add_as_index].service_flag = SERVICE_ON;
  shm_Appl->info.as_info[add_as_index].reset_flag = FALSE;

  memset (&shm_Appl->info.as_info[add_as_index].cas_clt_ip[0], 0x0,
	  sizeof (shm_Appl->info.as_info[add_as_index].cas_clt_ip));
  shm_Appl->info.as_info[add_as_index].cas_clt_port = 0;
  shm_Appl->info.as_info[add_as_index].client_version[0] = '\0';

  (shm_Br->br_info[br_Index].appl_server_num)++;
  (shm_Appl->num_appl_server)++;
  pthread_mutex_unlock (&broker_Shm_mutex);

  return true;
}

static bool
broker_drop_one_cas_by_time_to_kill (void)
{
  int cur_appl_server_num, wait_job_cnt;
  int drop_as_index;
  T_APPL_SERVER_INFO *drop_as_info;

  /* DROP UTS */
  cur_appl_server_num = shm_Br->br_info[br_Index].appl_server_num;
  wait_job_cnt = shm_Appl->job_queue[0].id + hold_Job;
  wait_job_cnt -= (cur_appl_server_num - num_Busy_uts);

  if (cur_appl_server_num <= shm_Br->br_info[br_Index].appl_server_min_num
      || wait_job_cnt > 0)
    {
      return false;
    }

  drop_as_index = find_drop_as_index ();
  if (drop_as_index < 0)
    {
      return false;
    }

  pthread_mutex_lock (&broker_Shm_mutex);
  current_Dropping_as_index = drop_as_index;
  drop_as_info = &shm_Appl->info.as_info[drop_as_index];
  drop_as_info->service_flag = SERVICE_OFF_ACK;
  pthread_mutex_unlock (&broker_Shm_mutex);

  CON_STATUS_LOCK (drop_as_info, CON_STATUS_LOCK_BROKER);
  if (drop_as_info->uts_status == UTS_STATUS_IDLE)
    {
      /* do nothing */
    }
  else if (drop_as_info->cur_keep_con == KEEP_CON_AUTO
	   && drop_as_info->uts_status == UTS_STATUS_BUSY
	   && drop_as_info->con_status == CON_STATUS_OUT_TRAN
	   && time (NULL) - drop_as_info->last_access_time >
	   shm_Br->br_info[br_Index].time_to_kill)
    {
      drop_as_info->con_status = CON_STATUS_CLOSE;
    }
  else
    {
      drop_as_info->service_flag = SERVICE_ON;
      drop_as_index = -1;
    }
  CON_STATUS_UNLOCK (drop_as_info, CON_STATUS_LOCK_BROKER);

  if (drop_as_index >= 0)
    {
      pthread_mutex_lock (&broker_Shm_mutex);
      (shm_Br->br_info[br_Index].appl_server_num)--;
      (shm_Appl->num_appl_server)--;
      pthread_mutex_unlock (&broker_Shm_mutex);

      stop_appl_server (drop_as_info, br_Index, drop_as_index);
    }
  current_Dropping_as_index = -1;

  return true;
}

/*
 * run_appl_server () -
 *   return: pid
 *   as_info_p(in): T_APPL_SERVER_INFO
 *   br_index(in): broker index
 *   as_index(in): cas index
 *
 * Note: activate CAS
 */
static int
run_appl_server (T_APPL_SERVER_INFO * as_info_p, int br_index, int as_index)
{
  char appl_name[APPL_SERVER_NAME_MAX_SIZE];
  int pid;
  char argv0[128];
  int i;
  char path[BROKER_PATH_MAX];
  char as_id_env_str[32];
  char appl_server_shm_key_env_str[32];

  while (1)
    {
      pthread_mutex_lock (&run_Appl_mutex);
      if (run_Appl_server_flag)
	{
	  pthread_mutex_unlock (&run_Appl_mutex);
	  THREAD_SLEEP (100);
	  continue;
	}
      else
	{
	  run_Appl_server_flag = 1;
	  pthread_mutex_unlock (&run_Appl_mutex);
	  break;
	}
    }

  as_info_p->service_ready_flag = FALSE;

  signal (SIGCHLD, SIG_IGN);

  ut_get_as_port_name (path, shm_Br->br_info[br_index].name, as_index,
		       BROKER_PATH_MAX);
  unlink (path);

  pid = fork ();
  if (pid == 0)
    {
      signal (SIGCHLD, SIG_DFL);

      for (i = 3; i <= max_Open_fd; i++)
	{
	  close (i);
	}
      strcpy (appl_name, shm_Appl->appl_server_name);

      sprintf (appl_server_shm_key_env_str, "%s=%d", APPL_SERVER_SHM_KEY_STR,
	       shm_Br->br_info[br_index].appl_server_shm_key);
      putenv (appl_server_shm_key_env_str);

      snprintf (as_id_env_str, sizeof (as_id_env_str),
		"%s=%d", AS_ID_ENV_STR, as_index);
      putenv (as_id_env_str);

      snprintf (argv0, sizeof (argv0) - 1, "%s_%s_%d",
		shm_Br->br_info[br_index].name, appl_name, as_index + 1);

      execle (appl_name, argv0, NULL, environ);

      exit (0);
    }

  CON_STATUS_LOCK_DESTROY (as_info_p);
  CON_STATUS_LOCK_INIT (as_info_p);
  if (ut_is_appl_server_ready (pid, &as_info_p->service_ready_flag))
    {
      as_info_p->transaction_start_time = (time_t) 0;
      as_info_p->num_restarts++;
    }
  else
    {
      pid = -1;
    }

  run_Appl_server_flag = 0;

  return pid;
}

/*
 * stop_appl_server () -
 *   return: NO_ERROR
 *   as_info_p(in): T_APPL_SERVER_INFO
 *   br_index(in): broker index
 *   as_index(in): cas index
 *
 * Note: inactivate CAS
 */
static int
stop_appl_server (T_APPL_SERVER_INFO * as_info_p, int br_index, int as_index)
{
  ut_kill_as_process (as_info_p->pid, shm_Br->br_info[br_index].name,
		      as_index);

  as_info_p->pid = 0;
  as_info_p->last_access_time = time (NULL);
  as_info_p->transaction_start_time = (time_t) 0;

  return 0;
}

/*
 * restart_appl_server () -
 *   return: void
 *   as_info_p(in): T_APPL_SERVER_INFO
 *   br_index(in): broker index
 *   as_index(in): cas index
 *
 * Note: inactivate and activate CAS
 */
static void
restart_appl_server (T_APPL_SERVER_INFO * as_info_p, int br_index,
		     int as_index)
{
  int new_pid;
  int r;

  as_info_p->psize = (int) (os_get_mem_size (as_info_p->pid,
					     MEM_VSIZE) / ONE_K);
  if (as_info_p->psize > 1)
    {
      as_info_p->psize_time = time (NULL);
    }
  else
    {
      char pid_file_name[BROKER_PATH_MAX];
      FILE *fp;
      int old_pid;

      ut_get_as_pid_name (pid_file_name, shm_Br->br_info[br_index].name,
			  as_index, BROKER_PATH_MAX);

      fp = fopen (pid_file_name, "r");
      if (fp)
	{
	  r = fscanf (fp, "%d", &old_pid);
	  if (r != 1)
	    {
	      assert (false);
	      ;			/* TODO - avoid compile error */
	    }

	  fclose (fp);

	  as_info_p->psize = (int) (os_get_mem_size (old_pid,
						     MEM_VSIZE) / ONE_K);
	  if (as_info_p->psize > 1)
	    {
	      as_info_p->pid = old_pid;
	      as_info_p->psize_time = time (NULL);
	    }
	  else
	    {
	      unlink (pid_file_name);
	    }
	}
    }

  if (as_info_p->psize <= 0)
    {
      if (as_info_p->pid > 0)
	{
	  ut_kill_as_process (as_info_p->pid, shm_Br->br_info[br_index].name,
			      as_index);
	}

      new_pid = run_appl_server (as_info_p, br_index, as_index);
      as_info_p->pid = new_pid;
    }
}

static int
read_from_client (SOCKET sock_fd, char *buf, int size, int timeout_sec)
{
  int read_len = -1;
  int nfound;
  struct pollfd po[1] = { {0, 0, 0} };
  int poll_timeout;

  poll_timeout = timeout_sec < 0 ? -1 : timeout_sec * 1000;

  po[0].fd = sock_fd;
  po[0].events = POLLIN;

retry_poll:
  nfound = poll (po, 1, poll_timeout);
  if (nfound < 0)
    {
      if (errno == EINTR)
	{
	  goto retry_poll;
	}
      else
	{
	  return -1;
	}
    }
  else if (nfound == 0)
    {
      return -1;
    }
  else
    {
      if (po[0].revents & POLLIN)
	{
	  read_len = READ_FROM_SOCKET (sock_fd, buf, size);
	}
      else if (po[0].revents & POLLERR || po[0].revents & POLLHUP)
	{
	  return -1;
	}
    }

  if (read_len <= 0)
    {
      return -1;
    }

  return read_len;
}

static int
write_to_client (SOCKET sock_fd, const char *buf, int size, int timeout_sec)
{
  int write_len = -1;
  int nfound;
  struct pollfd po[1] = { {0, 0, 0} };
  int poll_timeout;

  if (IS_INVALID_SOCKET (sock_fd))
    return -1;

  poll_timeout = timeout_sec < 0 ? -1 : timeout_sec * 1000;

  po[0].fd = sock_fd;
  po[0].events = POLLOUT;

retry_poll:
  nfound = poll (po, 1, poll_timeout);
  if (nfound < 0)
    {
      if (errno == EINTR)
	{
	  goto retry_poll;
	}
      else
	{
	  return -1;
	}
    }
  else if (nfound == 0)
    {
      return -1;
    }
  else
    {
      if (po[0].revents & POLLERR || po[0].revents & POLLHUP)
	{
	  return -1;
	}
      else if (po[0].revents & POLLOUT)
	{
	  write_len = WRITE_TO_SOCKET (sock_fd, buf, size);
	}
    }

  if (write_len <= 0)
    {
      return -1;
    }

  return write_len;
}

int
br_write_nbytes_to_client (SOCKET sock_fd, const char *buf, int size,
			   int timeout_sec)
{
  int write_len;

  while (size > 0)
    {
      write_len = write_to_client (sock_fd, buf, size, timeout_sec);

      if (write_len <= 0)
	{
	  return -1;
	}

      buf += write_len;
      size -= write_len;
    }

  return 0;
}

int
br_read_nbytes_from_client (SOCKET sock_fd, char *buf, int size,
			    int timeout_sec)
{
  int read_len;

  while (size > 0)
    {
      read_len = read_from_client (sock_fd, buf, size, timeout_sec);
      if (read_len <= 0)
	{
	  return -1;
	}

      buf += read_len;
      size -= read_len;
    }

  return 0;
}

SOCKET
br_connect_srv (const char *br_name, bool is_mgmt, int as_index)
{
  int sock_addr_len;
  struct sockaddr_un sock_addr;
  SOCKET srv_sock_fd;
  int one = 1;
  char retry_count = 0;

retry:

  srv_sock_fd = socket (AF_UNIX, SOCK_STREAM, 0);
  if (IS_INVALID_SOCKET (srv_sock_fd))
    return INVALID_SOCKET;

  memset (&sock_addr, 0, sizeof (struct sockaddr_un));
  sock_addr.sun_family = AF_UNIX;

  if (is_mgmt)
    {
      ut_get_broker_port_name (sock_addr.sun_path, br_name,
			       sizeof (sock_addr.sun_path));
    }
  else
    {
      ut_get_as_port_name (sock_addr.sun_path, br_name, as_index,
			   sizeof (sock_addr.sun_path));
    }

  sock_addr_len =
    strlen (sock_addr.sun_path) + sizeof (sock_addr.sun_family) + 1;

  if (connect (srv_sock_fd, (struct sockaddr *) &sock_addr, sock_addr_len) <
      0)
    {
      if (!is_mgmt && retry_count < 1)
	{
	  int new_pid;

	  ut_kill_as_process (shm_Appl->info.as_info[as_index].pid,
			      shm_Br->br_info[br_Index].name, as_index);

	  new_pid = run_appl_server (&(shm_Appl->info.as_info[as_index]),
				     br_Index, as_index);
	  shm_Appl->info.as_info[as_index].pid = new_pid;
	  retry_count++;
	  RYE_CLOSE_SOCKET (srv_sock_fd);
	  goto retry;
	}
      RYE_CLOSE_SOCKET (srv_sock_fd);
      return INVALID_SOCKET;
    }

  if (setsockopt (srv_sock_fd, IPPROTO_TCP, TCP_NODELAY, (char *) &one,
		  sizeof (one)) < 0)
    {
      // assert (0);
    }

  return srv_sock_fd;
}

/*
 * cas_monitor_worker () -
 *   return: void
 *   as_info_p(in): T_APPL_SERVER_INFO
 *   br_index(in): broker index
 *   as_index(in): cas index
 *   busy_uts(out): counting UTS_STATUS_BUSY status cas
 *
 * Note: monitoring CAS
 */
static void
cas_monitor_worker (T_APPL_SERVER_INFO * as_info_p, int br_index,
		    int as_index, int *busy_uts)
{
  int new_pid;
  int restart_flag = OFF;

  if (as_info_p->service_flag != SERVICE_ON)
    {
      return;
    }
  if (as_info_p->uts_status == UTS_STATUS_BUSY)
    {
      (*busy_uts)++;
    }

  if (as_info_p->psize > shm_Appl->appl_server_hard_limit)
    {
      as_info_p->uts_status = UTS_STATUS_RESTART;
    }

/*  if (as_info_p->service_flag != SERVICE_ON)
        continue;  */
  /* check cas process status and restart it */

  if (as_info_p->uts_status == UTS_STATUS_BUSY)
    {
      restart_flag = ON;
    }

  if (restart_flag)
    {
      if (kill (as_info_p->pid, 0) < 0)
	{
	  restart_appl_server (as_info_p, br_index, as_index);
	  as_info_p->uts_status = UTS_STATUS_IDLE;
	}
    }

  if (as_info_p->uts_status == UTS_STATUS_RESTART)
    {
      stop_appl_server (as_info_p, br_index, as_index);
      new_pid = run_appl_server (as_info_p, br_index, as_index);

      as_info_p->pid = new_pid;
      as_info_p->uts_status = UTS_STATUS_IDLE;
    }
}

static THREAD_FUNC
cas_monitor_thr_f (UNUSED_ARG void *ar)
{
  int i, tmp_num_busy_uts;
  int error;
  ER_MSG_INFO *er_msg;

  er_msg = malloc (sizeof (ER_MSG_INFO));
  error = er_set_msg_info (er_msg);
  if (error != NO_ERROR)
    {
      return NULL;
    }

  while (br_Process_flag)
    {
      tmp_num_busy_uts = 0;
      for (i = 0; i < shm_Br->br_info[br_Index].appl_server_max_num; i++)
	{
	  cas_monitor_worker (&(shm_Appl->info.as_info[i]), br_Index,
			      i, &tmp_num_busy_uts);
	}

      num_Busy_uts = tmp_num_busy_uts;
      shm_Br->br_info[br_Index].num_busy_count = num_Busy_uts;
      THREAD_SLEEP (100);
    }

  return NULL;
}

static CSS_CONN_ENTRY *
connect_to_master_for_server_monitor (const char *db_name,
				      const char *db_host)
{
  int port_id;
  unsigned short rid;

  if (sysprm_load_and_init (db_name) != NO_ERROR)
    {
      return NULL;
    }

  port_id = prm_get_master_port_id ();
  if (port_id <= 0)
    {
      return NULL;
    }

  /* timeout : 5000 milliseconds */
  return (css_connect_to_master_timeout (db_host, port_id, 5000, &rid));
}

static int
get_server_state_from_master (CSS_CONN_ENTRY * conn, const char *db_name)
{
  int css_error = NO_ERRORS;
  char *request = NULL;
  int request_size, strlen1;
  char *reply = NULL;
  int reply_size;
  int server_state = SERVER_STATE_UNKNOWN;

  if (conn == NULL)
    {
      return SERVER_STATE_UNKNOWN;
    }

  request_size = or_packed_string_length (db_name, &strlen1);
  request = (char *) malloc (request_size);
  if (request == NULL)
    {
      return SERVER_STATE_UNKNOWN;
    }
  or_pack_string_with_length (request, db_name, strlen1);

  /* timeout : 5000 milliseconds */
  css_error = css_send_request_to_master (conn, MASTER_GET_SERVER_STATE,
					  5000, 1, 1,
					  request, request_size, &reply,
					  &reply_size);
  if (css_error != NO_ERRORS || reply == NULL || reply_size <= 0)
    {
      free_and_init (request);

      return SERVER_STATE_UNKNOWN;
    }

  or_unpack_int (reply, &server_state);

  free_and_init (reply);
  free_and_init (request);

  return server_state;
}

static int
insert_db_server_check_list (T_DB_SERVER * list_p,
			     int check_list_cnt, const char *db_name,
			     const char *db_host)
{
  int i;

  for (i = 0; i < check_list_cnt && i < UNUSABLE_DATABASE_MAX; i++)
    {
      if (strcmp (db_name, list_p[i].database_name) == 0
	  && strcmp (db_host, list_p[i].database_host) == 0)
	{
	  return check_list_cnt;
	}
    }

  if (i == UNUSABLE_DATABASE_MAX)
    {
      return UNUSABLE_DATABASE_MAX;
    }

  strncpy (list_p[i].database_name, db_name, SRV_CON_DBNAME_SIZE - 1);
  strncpy (list_p[i].database_host, db_host, MAXHOSTNAMELEN - 1);
  list_p[i].server_state = HA_STATE_UNKNOWN;

  return i + 1;
}

static THREAD_FUNC
server_monitor_thr_f (UNUSED_ARG void *arg)
{
  int i, j, cnt;
  int u_index;
  int check_list_cnt = 0;
  T_APPL_SERVER_INFO *as_info_p;
  T_DB_SERVER *check_list;
  CSS_CONN_ENTRY *conn = NULL;
  char **ha_hosts = NULL;
  int num_hosts = 0;
  char **preferred_hosts;
  char *unusable_db_name;
  char *unusable_db_host;
  char busy_cas_db_name[SRV_CON_DBNAME_SIZE];
  int error;
  ER_MSG_INFO *er_msg;

  er_msg = malloc (sizeof (ER_MSG_INFO));
  error = er_set_msg_info (er_msg);
  if (error != NO_ERROR)
    {
      return NULL;
    }

  check_list = malloc (sizeof (T_DB_SERVER) * UNUSABLE_DATABASE_MAX);

  while (br_Process_flag)
    {
      if (!shm_Appl->monitor_server_flag
	  || shm_Br->br_info[br_Index].appl_server != APPL_SERVER_CAS
	  || check_list == NULL)
	{
	  shm_Appl->unusable_databases_seq = 0;
	  memset (shm_Appl->unusable_databases_cnt, 0,
		  sizeof (shm_Appl->unusable_databases_cnt));
	  THREAD_SLEEP (MONITOR_SERVER_INTERVAL * 1000);
	  continue;
	}

      if (ha_hosts != NULL)
	{
	  cfg_free_hosts (ha_hosts);
	}
      ha_hosts = cfg_get_hosts_from_prm (&num_hosts);

      /* 1. collect server check list */
      check_list_cnt = 0;
      u_index = shm_Appl->unusable_databases_seq % 2;

      for (i = 0; i < shm_Appl->unusable_databases_cnt[u_index]; i++)
	{
	  unusable_db_name =
	    shm_Appl->unusable_databases[u_index][i].database_name;
	  unusable_db_host =
	    shm_Appl->unusable_databases[u_index][i].database_host;

	  check_list_cnt =
	    insert_db_server_check_list (check_list, check_list_cnt,
					 unusable_db_name, unusable_db_host);
	}

      for (i = 0; i < shm_Br->br_info[br_Index].appl_server_max_num; i++)
	{
	  as_info_p = &(shm_Appl->info.as_info[i]);
	  if (as_info_p->uts_status == UTS_STATUS_BUSY)
	    {
	      strncpy (busy_cas_db_name, as_info_p->database_name,
		       SRV_CON_DBNAME_SIZE - 1);

	      if (busy_cas_db_name[0] != '\0')
		{
		  preferred_hosts =
		    util_split_string (shm_Appl->preferred_hosts, ":");
		  if (preferred_hosts != NULL)
		    {
		      for (j = 0; preferred_hosts[j] != NULL; j++)
			{
			  check_list_cnt =
			    insert_db_server_check_list (check_list,
							 check_list_cnt,
							 busy_cas_db_name,
							 preferred_hosts[j]);
			}

		      util_free_string_array (preferred_hosts);
		    }

		  for (j = 0; j < num_hosts; j++)
		    {
		      check_list_cnt =
			insert_db_server_check_list (check_list,
						     check_list_cnt,
						     busy_cas_db_name,
						     ha_hosts[j]);
		    }
		}
	    }
	}

      /* 2. check server state */
      for (i = 0; i < check_list_cnt; i++)
	{
	  conn =
	    connect_to_master_for_server_monitor (check_list[i].database_name,
						  check_list[i].
						  database_host);

	  check_list[i].server_state =
	    get_server_state_from_master (conn, check_list[i].database_name);
	  if (conn != NULL)
	    {
	      css_free_conn (conn);
	      conn = NULL;
	    }
	}

      /* 3. record server state to the shared memory */
      cnt = 0;
      u_index = (shm_Appl->unusable_databases_seq + 1) % 2;

      for (i = 0; i < check_list_cnt; i++)
	{
	  if (check_list[i].server_state == HA_STATE_UNKNOWN
	      || check_list[i].server_state == HA_STATE_NA)
	    {
	      strncpy (shm_Appl->unusable_databases[u_index][cnt].
		       database_name, check_list[i].database_name,
		       SRV_CON_DBNAME_SIZE - 1);
	      strncpy (shm_Appl->unusable_databases[u_index][cnt].
		       database_host, check_list[i].database_host,
		       MAXHOSTNAMELEN - 1);
	      cnt++;
	    }
	}

      shm_Appl->unusable_databases_cnt[u_index] = cnt;
      shm_Appl->unusable_databases_seq++;

      THREAD_SLEEP (MONITOR_SERVER_INTERVAL * 1000);
    }

  cfg_free_hosts (ha_hosts);
  free_and_init (check_list);

  return NULL;
}

static THREAD_FUNC
hang_check_thr_f (UNUSED_ARG void *ar)
{
  unsigned int cur_index;
  int cur_hang_count;
  T_BROKER_INFO *br_info_p;
  time_t cur_time;
  int collect_count_interval;
  int hang_count[NUM_COLLECT_COUNT_PER_INTVL] = { 0, 0, 0, 0 };
  float avg_hang_count;
  bool br_log_hang;
  bool cas_hang;

  int i;
  T_APPL_SERVER_INFO *as_info_p;

  int error;
  ER_MSG_INFO *er_msg;

  er_msg = malloc (sizeof (ER_MSG_INFO));
  error = er_set_msg_info (er_msg);
  if (error != NO_ERROR)
    {
      return NULL;
    }

  THREAD_SLEEP (shm_Br->br_info[br_Index].monitor_hang_interval * 1000);

  br_info_p = &(shm_Br->br_info[br_Index]);
  cur_hang_count = 0;
  cur_index = 0;
  avg_hang_count = 0.0;
  collect_count_interval =
    br_info_p->monitor_hang_interval / NUM_COLLECT_COUNT_PER_INTVL;

  while (br_Process_flag)
    {
      br_log_hang = false;
      cas_hang = false;

      if (br_log_hang_time () > br_info_p->hang_timeout)
	{
	  br_log_hang = true;
	}

      if (shm_Br->br_info[br_Index].monitor_hang_flag)
	{
	  cur_time = time (NULL);

	  for (i = 0; i < br_info_p->appl_server_max_num; i++)
	    {
	      as_info_p = &(shm_Appl->info.as_info[i]);

	      if ((as_info_p->service_flag != SERVICE_ON)
		  || as_info_p->claimed_alive_time == 0)
		{
		  continue;
		}
	      if ((br_info_p->hang_timeout <
		   cur_time - as_info_p->claimed_alive_time))
		{
		  cur_hang_count++;
		}
	    }

	  hang_count[cur_index] = cur_hang_count;

	  avg_hang_count =
	    ut_get_avg_from_array (hang_count, NUM_COLLECT_COUNT_PER_INTVL);

	  cas_hang =
	    (avg_hang_count >=
	     (float) br_info_p->appl_server_num * HANG_COUNT_THRESHOLD_RATIO);

	  cur_index = (cur_index + 1) % NUM_COLLECT_COUNT_PER_INTVL;
	  cur_hang_count = 0;
	}

      if (br_log_hang || cas_hang)
	{
	  br_info_p->reject_client_flag = 1;
	}
      else
	{
	  br_info_p->reject_client_flag = 0;
	}

      THREAD_SLEEP (collect_count_interval * 1000);
    }

  return NULL;
}

/*
 * psize_check_worker () -
 *   return: void
 *   as_info_p(in): T_APPL_SERVER_INFO
 *   br_index(in): broker index
 *   as_index(in): cas index
 *
 * Note: check cas psize and cas log
 */
static void
psize_check_worker (T_APPL_SERVER_INFO * as_info_p, int br_index,
		    int as_index)
{
  if (as_info_p->service_flag != SERVICE_ON)
    {
      return;
    }

  as_info_p->psize = (int) (os_get_mem_size (as_info_p->pid,
					     MEM_VSIZE) / ONE_K);

  check_cas_log (shm_Br->br_info[br_index].name, as_info_p, as_index);
}


static THREAD_FUNC
psize_check_thr_f (UNUSED_ARG void *ar)
{
  int i;
  int error;
  ER_MSG_INFO *er_msg;

  er_msg = malloc (sizeof (ER_MSG_INFO));
  error = er_set_msg_info (er_msg);
  if (error != NO_ERROR)
    {
      return NULL;
    }

  while (br_Process_flag)
    {
      br_log_check ();

      for (i = 0; i < shm_Br->br_info[br_Index].appl_server_max_num; i++)
	{
	  psize_check_worker (&(shm_Appl->info.as_info[i]), br_Index, i);
	}

      THREAD_SLEEP (1000);
    }

  return NULL;
}

/*
 * check_cas_log () -
 *   return: void
 *   br_name(in): broker name
 *   as_info_p(in): T_APPL_SERVER_INFO
 *   as_index(in): cas index
 * Note: check cas log and recreate
 */
static void
check_cas_log (const char *br_name, T_APPL_SERVER_INFO * as_info_p,
	       int as_index)
{
  char log_filename[BROKER_PATH_MAX];

  if (IS_NOT_APPL_SERVER_TYPE_CAS (shm_Br->br_info[br_Index].appl_server))
    {
      return;
    }

  if (as_info_p->cur_sql_log_mode != SQL_LOG_MODE_NONE)
    {
      get_as_sql_log_filename (log_filename, br_name, as_index);

      if (access (log_filename, F_OK) < 0)
	{
	  FILE *fp;
	  fp = fopen (log_filename, "a");
	  if (fp != NULL)
	    {
	      fclose (fp);
	    }
	  as_info_p->cas_log_reset = CAS_LOG_RESET_REOPEN;
	}
    }

  if (as_info_p->cur_slow_log_mode != SLOW_LOG_MODE_OFF)
    {
      get_as_slow_log_filename (log_filename, br_name, as_index);

      if (access (log_filename, F_OK) < 0)
	{
	  FILE *fp;
	  fp = fopen (log_filename, "a");
	  if (fp != NULL)
	    {
	      fclose (fp);
	    }
	  as_info_p->cas_slow_log_reset = CAS_LOG_RESET_REOPEN;
	}
    }
}

static int
find_idle_cas (void)
{
  int i;
  int idle_cas_id = -1;
  time_t max_wait_time;
  int wait_cas_id;
  time_t cur_time = time (NULL);

  pthread_mutex_lock (&broker_Shm_mutex);

  wait_cas_id = -1;
  max_wait_time = 0;

  for (i = 0; i < shm_Br->br_info[br_Index].appl_server_max_num; i++)
    {
      if (shm_Appl->info.as_info[i].service_flag != SERVICE_ON)
	{
	  continue;
	}
      if (shm_Appl->info.as_info[i].uts_status == UTS_STATUS_IDLE)
	{
	  if (kill (shm_Appl->info.as_info[i].pid, 0) == 0)
	    {
	      idle_cas_id = i;
	      wait_cas_id = -1;
	      break;
	    }
	  else
	    {
	      shm_Appl->info.as_info[i].uts_status = UTS_STATUS_RESTART;
	      continue;
	    }
	}
      if (shm_Br->br_info[br_Index].appl_server_num ==
	  shm_Br->br_info[br_Index].appl_server_max_num
	  && shm_Appl->info.as_info[i].uts_status == UTS_STATUS_BUSY
	  && shm_Appl->info.as_info[i].cur_keep_con == KEEP_CON_AUTO
	  && shm_Appl->info.as_info[i].con_status == CON_STATUS_OUT_TRAN
	  && shm_Appl->info.as_info[i].num_holdable_results < 1
	  && shm_Appl->info.as_info[i].cas_change_mode ==
	  CAS_CHANGE_MODE_AUTO)
	{
	  time_t wait_time =
	    cur_time - shm_Appl->info.as_info[i].last_access_time;
	  if (wait_time > max_wait_time || wait_cas_id == -1)
	    {
	      max_wait_time = wait_time;
	      wait_cas_id = i;
	    }
	}
    }

  if (wait_cas_id >= 0)
    {
      CON_STATUS_LOCK (&(shm_Appl->info.as_info[wait_cas_id]),
		       CON_STATUS_LOCK_BROKER);
      if (shm_Appl->info.as_info[wait_cas_id].con_status ==
	  CON_STATUS_OUT_TRAN
	  && shm_Appl->info.as_info[wait_cas_id].num_holdable_results < 1
	  && shm_Appl->info.as_info[wait_cas_id].cas_change_mode ==
	  CAS_CHANGE_MODE_AUTO)
	{
	  idle_cas_id = wait_cas_id;
	  shm_Appl->info.as_info[wait_cas_id].con_status =
	    CON_STATUS_CLOSE_AND_CONNECT;
	}
      CON_STATUS_UNLOCK (&(shm_Appl->info.as_info[wait_cas_id]),
			 CON_STATUS_LOCK_BROKER);
    }

  if (idle_cas_id < 0)
    {
      pthread_mutex_unlock (&broker_Shm_mutex);
      return -1;
    }

  shm_Appl->info.as_info[idle_cas_id].uts_status = UTS_STATUS_BUSY;
  pthread_mutex_unlock (&broker_Shm_mutex);

  return idle_cas_id;
}

static int
find_drop_as_index (void)
{
  int i, drop_as_index, exist_idle_cas;
  time_t max_wait_time, wait_time;

  pthread_mutex_lock (&broker_Shm_mutex);
  if (IS_NOT_APPL_SERVER_TYPE_CAS (shm_Br->br_info[br_Index].appl_server))
    {
      drop_as_index = shm_Br->br_info[br_Index].appl_server_num - 1;
      wait_time =
	time (NULL) - shm_Appl->info.as_info[drop_as_index].last_access_time;
      if (shm_Appl->info.as_info[drop_as_index].uts_status == UTS_STATUS_IDLE
	  && wait_time > shm_Br->br_info[br_Index].time_to_kill)
	{
	  pthread_mutex_unlock (&broker_Shm_mutex);
	  return drop_as_index;
	}
      pthread_mutex_unlock (&broker_Shm_mutex);
      return -1;
    }

  drop_as_index = -1;
  max_wait_time = -1;
  exist_idle_cas = 0;

  for (i = shm_Br->br_info[br_Index].appl_server_max_num - 1; i >= 0; i--)
    {
      if (shm_Appl->info.as_info[i].service_flag != SERVICE_ON)
	continue;

      wait_time = time (NULL) - shm_Appl->info.as_info[i].last_access_time;

      if (shm_Appl->info.as_info[i].uts_status == UTS_STATUS_IDLE)
	{
	  if (wait_time > shm_Br->br_info[br_Index].time_to_kill)
	    {
	      drop_as_index = i;
	      break;
	    }
	  else
	    {
	      exist_idle_cas = 1;
	      drop_as_index = -1;
	    }
	}

      if (shm_Appl->info.as_info[i].uts_status == UTS_STATUS_BUSY
	  && shm_Appl->info.as_info[i].con_status == CON_STATUS_OUT_TRAN
	  && shm_Appl->info.as_info[i].num_holdable_results < 1
	  && shm_Appl->info.as_info[i].cas_change_mode == CAS_CHANGE_MODE_AUTO
	  && wait_time > max_wait_time
	  && wait_time > shm_Br->br_info[br_Index].time_to_kill
	  && exist_idle_cas == 0)
	{
	  max_wait_time = wait_time;
	  drop_as_index = i;
	}
    }

  pthread_mutex_unlock (&broker_Shm_mutex);

  return drop_as_index;
}

static int
find_add_as_index ()
{
  int i;

  pthread_mutex_lock (&broker_Shm_mutex);
  for (i = 0; i < shm_Br->br_info[br_Index].appl_server_max_num; i++)
    {
      if (shm_Appl->info.as_info[i].service_flag == SERVICE_OFF_ACK
	  && current_Dropping_as_index != i)
	{
	  pthread_mutex_unlock (&broker_Shm_mutex);
	  return i;
	}
    }

  pthread_mutex_unlock (&broker_Shm_mutex);
  return -1;
}

static int
broker_init_shm (void)
{
  char *p;
  int master_shm_key, as_shm_key;

  p = getenv (MASTER_SHM_KEY_ENV_STR);
  if (p == NULL)
    {
      br_set_init_error (BR_ER_INIT_SHM_OPEN, 0);
      goto return_error;
    }
  parse_int (&master_shm_key, p, 10);
  BROKER_ERR ("<BROKER> MASTER_SHM_KEY_ENV_STR:[%d:%x]\n", master_shm_key,
	      master_shm_key);

  shm_Br = rye_shm_attach (master_shm_key, RYE_SHM_TYPE_BROKER_GLOBAL, false);
  if (shm_Br == NULL)
    {
      br_set_init_error (BR_ER_INIT_SHM_OPEN, 0);
      goto return_error;
    }

  br_Index = -1;
  p = getenv (BROKER_INDEX_ENV_STR);
  if (p != NULL)
    {
      parse_int (&br_Index, p, 10);
    }

  if (br_Index == -1)
    {
      br_set_init_error (BR_ER_INIT_CANT_CREATE_SOCKET, 0);
      goto return_error;
    }
  br_Info_p = &shm_Br->br_info[br_Index];

  as_shm_key = br_Info_p->appl_server_shm_key;
  BROKER_ERR ("<BROKER> APPL_SERVER_SHM_KEY_STR:[%d:%x]\n", as_shm_key,
	      as_shm_key);

  shm_Appl = rye_shm_attach (as_shm_key, RYE_SHM_TYPE_BROKER_LOCAL, false);
  if (shm_Appl == NULL)
    {
      br_set_init_error (BR_ER_INIT_SHM_OPEN, 0);
      goto return_error;
    }

  return 0;

return_error:
  return -1;
}

static void
get_as_sql_log_filename (char *log_filename, const char *broker_name,
			 int as_index)
{
  char dirname[BROKER_PATH_MAX];

  get_rye_file (FID_SQL_LOG_DIR, dirname, BROKER_PATH_MAX);

  snprintf (log_filename, BROKER_PATH_MAX, "%s%s_%d.sql.log", dirname,
	    broker_name, as_index + 1);
}

static void
get_as_slow_log_filename (char *log_filename, const char *broker_name,
			  int as_index)
{
  char dirname[BROKER_PATH_MAX];

  get_rye_file (FID_SLOW_LOG_DIR, dirname, BROKER_PATH_MAX);

  snprintf (log_filename, BROKER_PATH_MAX, "%s%s_%d.slow.log", dirname,
	    broker_name, as_index + 1);
}

/*
 * br_mgmt_net_add_int64
*/
char *
br_mgmt_net_add_int64 (char *net_stream, int64_t value)
{
  int64_t tmp_int64;

  tmp_int64 = net_htoni64 (value);
  memcpy (net_stream, &tmp_int64, sizeof (tmp_int64));
  return (net_stream + sizeof (tmp_int64));
}

/*
 * br_mgmt_net_add_int
*/
char *
br_mgmt_net_add_int (char *net_stream, int value)
{
  int tmp_int;

  tmp_int = htonl (value);
  memcpy (net_stream, &tmp_int, sizeof (tmp_int));

  return (net_stream + sizeof (tmp_int));
}

/*
 * br_mgmt_net_add_short
*/
char *
br_mgmt_net_add_short (char *net_stream, short value)
{
  short tmp_short;

  tmp_short = htons (value);
  memcpy (net_stream, &tmp_short, sizeof (tmp_short));

  return (net_stream + sizeof (tmp_short));
}

/*
 * br_mgmt_net_add_string
*/
char *
br_mgmt_net_add_string (char *net_stream, const char *value)
{
  int tmp_int;
  int str_size;

  str_size = strlen (value) + 1;
  tmp_int = htonl (str_size);
  memcpy (net_stream, &tmp_int, sizeof (tmp_int));
  net_stream += sizeof (int);

  memcpy (net_stream, value, str_size);
  return (net_stream + str_size);
}

#if !defined(BYTE_ORDER_BIG_ENDIAN)
/*
 * net_htoni64
*/
static int64_t
net_htoni64 (int64_t from)
{
  int64_t to;
  char *p, *q;

  p = (char *) &from;
  q = (char *) &to;

  q[0] = p[7];
  q[1] = p[6];
  q[2] = p[5];
  q[3] = p[4];
  q[4] = p[3];
  q[5] = p[2];
  q[6] = p[1];
  q[7] = p[0];

  return to;
}
#endif

static const char *
br_mgmt_read_req_arg_type (char *type, const char *ptr, int *remain_size)
{
  if (ptr == NULL || *remain_size < 1)
    {
      return NULL;
    }

  *type = *ptr;

  *remain_size -= 1;
  return (ptr + 1);
}

static const char *
br_mgmt_read_req_arg_int64 (int64_t * value, const char *ptr,
			    int *remain_size)
{
  int64_t tmp_value;

  if (ptr == NULL || *remain_size < (int) sizeof (int64_t))
    {
      *value = 0;
      return NULL;
    }

  memcpy (&tmp_value, ptr, sizeof (int64_t));
  *value = net_ntohi64 (tmp_value);

  *remain_size -= sizeof (int64_t);

  return (ptr + sizeof (int64_t));
}

static const char *
br_mgmt_read_req_arg_int (int *value, const char *ptr, int *remain_size)
{
  int tmp_value;

  if (ptr == NULL || *remain_size < (int) sizeof (int))
    {
      *value = 0;
      return NULL;
    }

  memcpy (&tmp_value, ptr, sizeof (int));
  *value = ntohl (tmp_value);

  *remain_size -= sizeof (int);

  return (ptr + sizeof (int));
}

static const char *
br_mgmt_read_req_arg_string (const char **value, const char *ptr,
			     int *remain_size)
{
  int size;

  *value = NULL;

  ptr = br_mgmt_read_req_arg_int (&size, ptr, remain_size);

  if (ptr == NULL || *remain_size < size || size <= 0)
    {
      return NULL;
    }

  *value = ptr;

  *remain_size -= size;

  return (ptr + size);
}

static const char *
br_mgmt_read_req_arg_str_arr (int *num_values, const char ***value,
			      const char *ptr, int *remain_size)
{
  int i;
  const char **str_arr;

  ptr = br_mgmt_read_req_arg_int (num_values, ptr, remain_size);
  if (ptr == NULL || *num_values < 0)
    {
      return NULL;
    }

  str_arr =
    (const char **) RYE_MALLOC (sizeof (const char *) * (*num_values));
  if (str_arr == NULL)
    {
      return NULL;
    }

  for (i = 0; i < *num_values; i++)
    {
      ptr = br_mgmt_read_req_arg_string (&str_arr[i], ptr, remain_size);
    }

  if (ptr == NULL)
    {
      RYE_FREE_MEM (str_arr);
      return NULL;
    }

  *value = str_arr;

  return ptr;
}

static const char *
br_mgmt_read_req_arg_int_arr (int *num_values, int **value,
			      const char *ptr, int *remain_size)
{
  int i;
  int *int_arr;

  ptr = br_mgmt_read_req_arg_int (num_values, ptr, remain_size);
  if (ptr == NULL || *num_values < 0)
    {
      return NULL;
    }

  int_arr = (int *) RYE_MALLOC (sizeof (int) * (*num_values));
  if (int_arr == NULL)
    {
      return NULL;
    }

  for (i = 0; i < *num_values; i++)
    {
      ptr = br_mgmt_read_req_arg_int (&int_arr[i], ptr, remain_size);
    }

  if (ptr == NULL)
    {
      RYE_FREE_MEM (int_arr);
      return NULL;
    }

  *value = int_arr;

  return ptr;
}

static int
str_to_int (int *ret_p, const char *str)
{
  int val;
  char *end_p;

  if (str == NULL)
    {
      return -1;
    }

  val = strtol (str, &end_p, 10);
  if (*end_p == '\0' && end_p != str)
    {
      *ret_p = val;
      return 0;
    }
  else
    {
      return -1;
    }
}

typedef struct
{
  char arg_type;
  union
  {
    int int_val;
    int64_t int64_val;
    const char *str_val;
    struct
    {
      int num_str;
      const char **values;
    } str_arr;
    struct
    {
      int num_values;
      int *values;
    } int_arr;
  } v;
} T_MGMT_REQ_ARG_CONTAINER;
#define MGMT_ARG_INT_VALUE(PTR)		((PTR)->v.int_val)
#define MGMT_ARG_BOOL_VALUE(PTR)	(MGMT_ARG_INT_VALUE(PTR) ? true : false)
#define MGMT_ARG_INT64_VALUE(PTR)	((PTR)->v.int64_val)
#define MGMT_ARG_STRING_VALUE(PTR)	((PTR)->v.str_val)
#define MGMT_ARG_STR_ARR_SIZE(PTR)	((PTR)->v.str_arr.num_str)
#define MGMT_ARG_STR_ARR_VALUE(PTR)	((PTR)->v.str_arr.values)
#define MGMT_ARG_INT_ARR_SIZE(PTR)	((PTR)->v.int_arr.num_values)
#define MGMT_ARG_INT_ARR_VALUE(PTR)	((PTR)->v.int_arr.values)

typedef int (*T_REQ_ARG_READ_FUNC) (T_MGMT_REQ_ARG * req_arg,
				    int num_args,
				    const T_MGMT_REQ_ARG_CONTAINER * args);

typedef struct
{
  int opcode;
  /*const char *(*func) (int *, T_MGMT_REQ_ARG *, const char *, int *);
   */
  T_REQ_ARG_READ_FUNC func;
} T_REQ_ARG_READ_FUNC_TABLE;

static int req_arg_get_shard_info (T_MGMT_REQ_ARG * req_arg, int num_args,
				   const T_MGMT_REQ_ARG_CONTAINER * args);
static int req_arg_shard_init (T_MGMT_REQ_ARG * req_arg, int num_args,
			       const T_MGMT_REQ_ARG_CONTAINER * args);
static int req_arg_node_add (T_MGMT_REQ_ARG * req_arg, int num_args,
			     const T_MGMT_REQ_ARG_CONTAINER * args);
static int req_arg_node_drop (T_MGMT_REQ_ARG * req_arg, int num_args,
			      const T_MGMT_REQ_ARG_CONTAINER * args);
static int req_arg_migration_start_end (T_MGMT_REQ_ARG * req_arg,
					int num_args,
					const T_MGMT_REQ_ARG_CONTAINER *
					args);
static int req_arg_ddl_start_end (T_MGMT_REQ_ARG * req_arg, int num_args,
				  const T_MGMT_REQ_ARG_CONTAINER * args);
static int req_arg_gc_start_end (T_MGMT_REQ_ARG * req_arg, int num_args,
				 const T_MGMT_REQ_ARG_CONTAINER * args);
static int req_arg_rebalance_req (T_MGMT_REQ_ARG * req_arg, int num_args,
				  const T_MGMT_REQ_ARG_CONTAINER * args);
static int req_arg_rebalance_job_count (T_MGMT_REQ_ARG * req_arg,
					int num_args,
					const T_MGMT_REQ_ARG_CONTAINER *
					args);
static int req_arg_sync_shard_mgmt_info (T_MGMT_REQ_ARG * req_arg,
					 int num_args,
					 const T_MGMT_REQ_ARG_CONTAINER *
					 args);
static int req_arg_launch_process (T_MGMT_REQ_ARG * req_arg, int num_args,
				   const T_MGMT_REQ_ARG_CONTAINER * args);
static int req_arg_read_rye_file (T_MGMT_REQ_ARG * req_arg, int num_args,
				  const T_MGMT_REQ_ARG_CONTAINER * args);
static int req_arg_write_rye_conf (T_MGMT_REQ_ARG * req_arg, int num_args,
				   const T_MGMT_REQ_ARG_CONTAINER * args);
static int req_arg_update_conf (T_MGMT_REQ_ARG * req_arg, int num_args,
				const T_MGMT_REQ_ARG_CONTAINER * args);
static int req_arg_get_conf (T_MGMT_REQ_ARG * req_arg, int num_args,
			     const T_MGMT_REQ_ARG_CONTAINER * args);
static int req_arg_br_acl_reload (T_MGMT_REQ_ARG * req_arg, int num_args,
				  const T_MGMT_REQ_ARG_CONTAINER * args);
static int req_arg_get_no_arg (T_MGMT_REQ_ARG * req_arg, int num_args,
			       const T_MGMT_REQ_ARG_CONTAINER * args);

static T_REQ_ARG_READ_FUNC_TABLE req_Arg_read_func_table[] = {
  {BRREQ_OP_CODE_GET_SHARD_INFO, req_arg_get_shard_info},
  {BRREQ_OP_CODE_INIT, req_arg_shard_init},
  {BRREQ_OP_CODE_ADD_NODE, req_arg_node_add},
  {BRREQ_OP_CODE_DROP_NODE, req_arg_node_drop},
  {BRREQ_OP_CODE_MIGRATION_START, req_arg_migration_start_end},
  {BRREQ_OP_CODE_MIGRATION_END, req_arg_migration_start_end},
  {BRREQ_OP_CODE_DDL_START, req_arg_ddl_start_end},
  {BRREQ_OP_CODE_DDL_END, req_arg_ddl_start_end},
  {BRREQ_OP_CODE_GC_START, req_arg_gc_start_end},
  {BRREQ_OP_CODE_GC_END, req_arg_gc_start_end},
  {BRREQ_OP_CODE_REBALANCE_REQ, req_arg_rebalance_req},
  {BRREQ_OP_CODE_REBALANCE_JOB_COUNT, req_arg_rebalance_job_count},
  {BRREQ_OP_CODE_SYNC_SHARD_MGMT_INFO, req_arg_sync_shard_mgmt_info},
  {BRREQ_OP_CODE_LAUNCH_PROCESS, req_arg_launch_process},
  {BRREQ_OP_CODE_GET_SHARD_MGMT_INFO, req_arg_get_no_arg},
  {BRREQ_OP_CODE_NUM_SHARD_VERSION_INFO, req_arg_get_no_arg},
  {BRREQ_OP_CODE_READ_RYE_FILE, req_arg_read_rye_file},
  {BRREQ_OP_CODE_WRITE_RYE_CONF, req_arg_write_rye_conf},
  {BRREQ_OP_CODE_UPDATE_CONF, req_arg_update_conf},
  {BRREQ_OP_CODE_DELETE_CONF, req_arg_get_conf},
  {BRREQ_OP_CODE_GET_CONF, req_arg_get_conf},
  {BRREQ_OP_CODE_BR_ACL_RELOAD, req_arg_br_acl_reload},
  {0, NULL}
};

static int
node_arg_str_to_node_info (T_SHARD_NODE_INFO * node_info, const char *arg_str)
{
  char *save_ptr = NULL;
  char *str_id;
  char *str_port;
  char *local_dbname;
  char *host_str;
  unsigned char ip_addr[4];
  char *cp_arg_str;
  int error = 0;
  int node_id, port;
  char host_ip[IP_ADDR_STR_LEN];

  RYE_ALLOC_COPY_STR (cp_arg_str, arg_str);
  if (cp_arg_str == NULL)
    {
      return -1;
    }

  str_id = strtok_r (cp_arg_str, ":", &save_ptr);
  local_dbname = strtok_r (NULL, ":", &save_ptr);
  host_str = strtok_r (NULL, ":", &save_ptr);
  str_port = strtok_r (NULL, ":", &save_ptr);

  if (local_dbname == NULL || host_str == NULL ||
      strlen (local_dbname) >= SRV_CON_DBNAME_SIZE ||
      str_to_int (&node_id, str_id) < 0 || str_to_int (&port, str_port) < 0)
    {
      error = -1;
      goto end;
    }

  if (strcasecmp (host_str, "localhost") == 0 ||
      strcmp (host_str, "127.0.0.1") == 0)
    {
      memcpy (ip_addr, shm_Br->my_ip_addr, 4);
    }
  else
    {
      if (cci_host_str_to_addr (host_str, ip_addr) < 0)
	{
	  error = -1;
	  goto end;
	}
    }
  ut_get_ipv4_string (host_ip, IP_ADDR_STR_LEN, ip_addr);

  br_copy_shard_node_info (node_info, node_id, local_dbname, host_ip, port,
			   INADDR_NONE, HA_STATE_FOR_DRIVER_UNKNOWN, NULL);

end:
  RYE_FREE_MEM (cp_arg_str);
  return error;
}

static int
check_mgmt_req_arg (int num_args, const T_MGMT_REQ_ARG_CONTAINER * args,
		    int expect_args, ...)
{
  va_list ap;
  int i;
  int error = 0;

  if (num_args < expect_args)
    {
      return -1;
    }

  va_start (ap, expect_args);

  for (i = 0; i < expect_args; i++)
    {
      int arg_type = va_arg (ap, int);

      if (arg_type != args[i].arg_type)
	{
	  error = -1;
	  break;
	}
    }

  va_end (ap);

  return error;
}

static int
req_arg_get_shard_info (T_MGMT_REQ_ARG * req_arg,
			int num_args, const T_MGMT_REQ_ARG_CONTAINER * args)
{
  T_MGMT_REQ_ARG_SHARD_INFO *get_info_arg = &req_arg->value.get_info_arg;

  if (check_mgmt_req_arg (num_args, args, 4, MGMT_REQ_ARG_STR,
			  MGMT_REQ_ARG_INT64, MGMT_REQ_ARG_INT64,
			  MGMT_REQ_ARG_INT64) < 0)
    {
      return BR_ER_INVALID_ARGUMENT;
    }

  req_arg->clt_dbname = MGMT_ARG_STRING_VALUE (&args[0]);
  get_info_arg->clt_node_ver = MGMT_ARG_INT64_VALUE (&args[1]);
  get_info_arg->clt_groupid_ver = MGMT_ARG_INT64_VALUE (&args[2]);
  get_info_arg->clt_created_at = MGMT_ARG_INT64_VALUE (&args[3]);

  return 0;
}

static int
req_arg_shard_init (T_MGMT_REQ_ARG * req_arg,
		    int num_args, const T_MGMT_REQ_ARG_CONTAINER * args)
{
  T_MGMT_REQ_ARG_SHARD_INIT *init_arg = &req_arg->value.init_shard_arg;
  int i;
  int num_init_nodes;
  const char **init_nodes;

  if (check_mgmt_req_arg (num_args, args, 4, MGMT_REQ_ARG_STR,
			  MGMT_REQ_ARG_STR, MGMT_REQ_ARG_INT,
			  MGMT_REQ_ARG_STR_ARRAY) < 0)
    {
      return BR_ER_INVALID_ARGUMENT;
    }

  init_arg->dba_passwd = MGMT_ARG_STRING_VALUE (&args[0]);
  init_arg->global_dbname = MGMT_ARG_STRING_VALUE (&args[1]);
  init_arg->groupid_count = MGMT_ARG_INT_VALUE (&args[2]);
  num_init_nodes = MGMT_ARG_STR_ARR_SIZE (&args[3]);
  init_nodes = MGMT_ARG_STR_ARR_VALUE (&args[3]);

  if (init_arg->groupid_count <= 0 || num_init_nodes <= 0)
    {
      return BR_ER_INVALID_ARGUMENT;
    }

  req_arg->alloc_buffer = RYE_MALLOC (sizeof (T_SHARD_NODE_INFO) *
				      num_init_nodes);
  if (req_arg->alloc_buffer == NULL)
    {
      return BR_ER_NO_MORE_MEMORY;
    }

  init_arg->init_num_node = num_init_nodes;
  init_arg->init_node = req_arg->alloc_buffer;

  for (i = 0; i < num_init_nodes; i++)
    {
      if (node_arg_str_to_node_info (&init_arg->init_node[i],
				     init_nodes[i]) < 0)
	{
	  return BR_ER_INVALID_ARGUMENT;
	}
    }

  return 0;
}

static int
req_arg_node_add (T_MGMT_REQ_ARG * req_arg,
		  int num_args, const T_MGMT_REQ_ARG_CONTAINER * args)
{
  const char *node_info;
  T_MGMT_REQ_ARG_SHARD_NODE_ADD *node_add_arg = &req_arg->value.node_add_arg;

  if (check_mgmt_req_arg (num_args, args, 3, MGMT_REQ_ARG_STR,
			  MGMT_REQ_ARG_STR, MGMT_REQ_ARG_STR) < 0)
    {
      return BR_ER_INVALID_ARGUMENT;
    }

  req_arg->clt_dbname = MGMT_ARG_STRING_VALUE (&args[0]);
  node_add_arg->dba_passwd = MGMT_ARG_STRING_VALUE (&args[1]);
  node_info = MGMT_ARG_STRING_VALUE (&args[2]);

  if (node_arg_str_to_node_info (&node_add_arg->node_info, node_info) < 0)
    {
      return BR_ER_INVALID_ARGUMENT;
    }

  return 0;
}

static int
req_arg_node_drop (T_MGMT_REQ_ARG * req_arg,
		   int num_args, const T_MGMT_REQ_ARG_CONTAINER * args)
{
  const char *node_info;
  T_MGMT_REQ_ARG_SHARD_NODE_DROP *node_drop_arg;

  node_drop_arg = &req_arg->value.node_drop_arg;

  if (check_mgmt_req_arg (num_args, args, 4, MGMT_REQ_ARG_STR,
			  MGMT_REQ_ARG_STR, MGMT_REQ_ARG_INT,
			  MGMT_REQ_ARG_STR) < 0)
    {
      return BR_ER_INVALID_ARGUMENT;
    }

  req_arg->clt_dbname = MGMT_ARG_STRING_VALUE (&args[0]);
  node_drop_arg->dba_passwd = MGMT_ARG_STRING_VALUE (&args[1]);
  node_drop_arg->drop_all_nodeid = MGMT_ARG_INT_VALUE (&args[2]);
  node_info = MGMT_ARG_STRING_VALUE (&args[3]);

  if (node_drop_arg->drop_all_nodeid > 0)
    {
      memset (&node_drop_arg->node_info, 0,
	      sizeof (node_drop_arg->node_info));
    }
  else
    {
      if (node_arg_str_to_node_info (&node_drop_arg->node_info,
				     node_info) < 0)
	{
	  return BR_ER_INVALID_ARGUMENT;
	}
    }

  return 0;
}

static int
req_arg_migration_start_end (T_MGMT_REQ_ARG * req_arg,
			     int num_args,
			     const T_MGMT_REQ_ARG_CONTAINER * args)
{
  T_MGMT_REQ_ARG_SHARD_MIGRATION *mig_arg = &req_arg->value.migration_arg;

  if (check_mgmt_req_arg (num_args, args, 5, MGMT_REQ_ARG_STR,
			  MGMT_REQ_ARG_INT, MGMT_REQ_ARG_INT,
			  MGMT_REQ_ARG_INT, MGMT_REQ_ARG_INT) < 0)
    {
      return BR_ER_INVALID_ARGUMENT;
    }

  req_arg->clt_dbname = MGMT_ARG_STRING_VALUE (&args[0]);
  mig_arg->mig_groupid = MGMT_ARG_INT_VALUE (&args[1]);
  mig_arg->dest_nodeid = MGMT_ARG_INT_VALUE (&args[2]);
  mig_arg->num_shard_keys = MGMT_ARG_INT_VALUE (&args[3]);
  mig_arg->timeout_sec = MGMT_ARG_INT_VALUE (&args[4]);

  return 0;
}

static int
req_arg_ddl_start_end (T_MGMT_REQ_ARG * req_arg,
		       int num_args, const T_MGMT_REQ_ARG_CONTAINER * args)
{
  T_MGMT_REQ_ARG_SHARD_DDL *ddl_arg = &req_arg->value.ddl_arg;

  if (check_mgmt_req_arg (num_args, args, 2, MGMT_REQ_ARG_STR,
			  MGMT_REQ_ARG_INT) < 0)
    {
      return BR_ER_INVALID_ARGUMENT;
    }

  req_arg->clt_dbname = MGMT_ARG_STRING_VALUE (&args[0]);
  ddl_arg->timeout_sec = MGMT_ARG_INT_VALUE (&args[1]);

  return 0;
}

static int
req_arg_gc_start_end (T_MGMT_REQ_ARG * req_arg,
		      int num_args, const T_MGMT_REQ_ARG_CONTAINER * args)
{
  T_MGMT_REQ_ARG_SHARD_GC *gc_arg = &req_arg->value.gc_arg;

  if (check_mgmt_req_arg (num_args, args, 2, MGMT_REQ_ARG_STR,
			  MGMT_REQ_ARG_INT) < 0)
    {
      return BR_ER_INVALID_ARGUMENT;
    }

  req_arg->clt_dbname = MGMT_ARG_STRING_VALUE (&args[0]);
  gc_arg->timeout_sec = MGMT_ARG_INT_VALUE (&args[1]);

  return 0;
}

static int
req_arg_rebalance_req (T_MGMT_REQ_ARG * req_arg,
		       int num_args, const T_MGMT_REQ_ARG_CONTAINER * args)
{
  T_MGMT_REQ_ARG_REBALANCE_REQ *arg = &req_arg->value.rebalance_req_arg;
  int i;
  int *nodes_ptr;
  int rebalance_type;
  int num_src_nodes;
  int num_dest_nodes;
  const int *src_nodes;
  const int *dest_nodes;

  if (check_mgmt_req_arg (num_args, args, 6, MGMT_REQ_ARG_STR,
			  MGMT_REQ_ARG_INT, MGMT_REQ_ARG_INT,
			  MGMT_REQ_ARG_STR, MGMT_REQ_ARG_INT_ARRAY,
			  MGMT_REQ_ARG_INT_ARRAY) < 0)
    {
      return BR_ER_INVALID_ARGUMENT;
    }

  req_arg->clt_dbname = MGMT_ARG_STRING_VALUE (&args[0]);
  rebalance_type = MGMT_ARG_INT_VALUE (&args[1]);
  arg->ignore_prev_fail = MGMT_ARG_BOOL_VALUE (&args[2]);
  arg->dba_passwd = MGMT_ARG_STRING_VALUE (&args[3]);
  num_src_nodes = MGMT_ARG_INT_ARR_SIZE (&args[4]);
  src_nodes = MGMT_ARG_INT_ARR_VALUE (&args[4]);
  num_dest_nodes = MGMT_ARG_INT_ARR_SIZE (&args[5]);
  dest_nodes = MGMT_ARG_INT_ARR_VALUE (&args[5]);

  if (num_src_nodes < 0 || num_dest_nodes < 0 ||
      (num_src_nodes + num_dest_nodes == 0))
    {
      return BR_ER_INVALID_ARGUMENT;
    }

  assert (rebalance_type == BRREQ_REBALANCE_TYPE_EMPTY_NODE ||
	  rebalance_type == BRREQ_REBALANCE_TYPE_REBALANCE);
  arg->empty_node =
    (rebalance_type == BRREQ_REBALANCE_TYPE_EMPTY_NODE ? true : false);

  req_arg->alloc_buffer = RYE_MALLOC (sizeof (int) *
				      (num_src_nodes + num_dest_nodes));
  if (req_arg->alloc_buffer == NULL)
    {
      return BR_ER_NO_MORE_MEMORY;
    }

  nodes_ptr = req_arg->alloc_buffer;

  for (i = 0; i < num_src_nodes; i++)
    {
      if (src_nodes[i] <= 0)
	{
	  return BR_ER_INVALID_ARGUMENT;
	}
      nodes_ptr[i] = src_nodes[i];
    }
  for (i = 0; i < num_dest_nodes; i++)
    {
      if (dest_nodes[i] <= 0)
	{
	  return BR_ER_INVALID_ARGUMENT;
	}
      nodes_ptr[num_src_nodes + i] = dest_nodes[i];
    }

  arg->num_src_nodes = num_src_nodes;
  if (num_src_nodes == 0)
    {
      arg->src_nodeid = NULL;
    }
  else
    {
      arg->src_nodeid = req_arg->alloc_buffer;
    }

  arg->num_dest_nodes = num_dest_nodes;
  if (num_dest_nodes == 0)
    {
      arg->dest_nodeid = NULL;
    }
  else
    {
      arg->dest_nodeid = (int *) (((char *) req_arg->alloc_buffer) +
				  (sizeof (int) * num_src_nodes));
    }

  return 0;
}

static int
req_arg_rebalance_job_count (T_MGMT_REQ_ARG * req_arg,
			     int num_args,
			     const T_MGMT_REQ_ARG_CONTAINER * args)
{
  T_MGMT_REQ_ARG_REBALANCE_JOB_COUNT *job_count;

  job_count = &req_arg->value.rebalance_job_count;

  if (check_mgmt_req_arg (num_args, args, 2, MGMT_REQ_ARG_STR,
			  MGMT_REQ_ARG_INT) < 0)
    {
      return BR_ER_INVALID_ARGUMENT;
    }

  req_arg->clt_dbname = MGMT_ARG_STRING_VALUE (&args[0]);
  job_count->job_type = MGMT_ARG_INT_VALUE (&args[1]);

  return 0;
}

static int
req_arg_sync_shard_mgmt_info (T_MGMT_REQ_ARG * req_arg,
			      int num_args,
			      const T_MGMT_REQ_ARG_CONTAINER * args)
{
  T_MGMT_REQ_ARG_SHARD_MGMT_INFO *info = &req_arg->value.shard_mgmt_info;

  if (check_mgmt_req_arg (num_args, args, 6, MGMT_REQ_ARG_STR,
			  MGMT_REQ_ARG_STR, MGMT_REQ_ARG_INT,
			  MGMT_REQ_ARG_INT, MGMT_REQ_ARG_INT64,
			  MGMT_REQ_ARG_INT64, MGMT_REQ_ARG_STR) < 0)
    {
      return BR_ER_INVALID_ARGUMENT;
    }

  req_arg->clt_dbname = NULL;
  info->local_dbname = MGMT_ARG_STRING_VALUE (&args[0]);
  info->global_dbname = MGMT_ARG_STRING_VALUE (&args[1]);
  info->nodeid = (short) MGMT_ARG_INT_VALUE (&args[2]);
  info->port = MGMT_ARG_INT_VALUE (&args[3]);
  info->nodeid_ver = MGMT_ARG_INT64_VALUE (&args[4]);
  info->groupid_ver = MGMT_ARG_INT64_VALUE (&args[5]);

  return 0;
}

static int
req_arg_launch_process (T_MGMT_REQ_ARG * req_arg,
			int num_args, const T_MGMT_REQ_ARG_CONTAINER * args)
{
  T_MGMT_REQ_ARG_LAUNCH_PROCESS *launch_arg;
  int launch_proc_id = -1;
  const char **argv;
  const char **envp;
  int i;
  int envp_offset;
  int argc;
  int num_env;
  const char **alloc_buffer;

  launch_arg = &req_arg->value.launch_process_arg;

  if (check_mgmt_req_arg (num_args, args, 3, MGMT_REQ_ARG_INT,
			  MGMT_REQ_ARG_STR_ARRAY, MGMT_REQ_ARG_STR_ARRAY) < 0)
    {
      return BR_ER_INVALID_ARGUMENT;
    }

  req_arg->clt_dbname = NULL;
  launch_proc_id = MGMT_ARG_INT_VALUE (&args[0]);
  argc = MGMT_ARG_STR_ARR_SIZE (&args[1]);
  argv = MGMT_ARG_STR_ARR_VALUE (&args[1]);
  num_env = MGMT_ARG_STR_ARR_SIZE (&args[2]);
  envp = MGMT_ARG_STR_ARR_VALUE (&args[2]);

  if (launch_proc_id < MGMT_LAUNCH_PROCESS_ID_MIN ||
      launch_proc_id > MGMT_LAUNCH_PROCESS_ID_MAX || argc < 1 || num_env < 0)
    {
      return BR_ER_INVALID_ARGUMENT;
    }

  alloc_buffer =
    (const char **) RYE_MALLOC (sizeof (char *) * (argc + num_env));
  if (alloc_buffer == NULL)
    {
      return BR_ER_NO_MORE_MEMORY;
    }

  for (i = 0; i < argc; i++)
    {
      alloc_buffer[i] = argv[i];
    }

  for (i = 0; i < num_env; i++)
    {
      alloc_buffer[argc + i] = envp[i];
    }

  envp_offset = sizeof (char *) * argc;

  req_arg->alloc_buffer = alloc_buffer;
  launch_arg->argc = argc;
  launch_arg->argv = (char **) req_arg->alloc_buffer;
  launch_arg->num_env = num_env;
  launch_arg->envp =
    (char **) (((char *) req_arg->alloc_buffer) + envp_offset);
  launch_arg->launch_process_id = launch_proc_id;

  return 0;
}

static int
req_arg_read_rye_file (T_MGMT_REQ_ARG * req_arg,
		       int num_args, const T_MGMT_REQ_ARG_CONTAINER * args)
{
  T_MGMT_REQ_ARG_READ_RYE_FILE *arg_read_rye_file;

  arg_read_rye_file = &req_arg->value.read_rye_file_arg;

  if (check_mgmt_req_arg (num_args, args, 1, MGMT_REQ_ARG_INT) < 0)
    {
      return BR_ER_INVALID_ARGUMENT;
    }

  arg_read_rye_file->which_file = MGMT_ARG_INT_VALUE (&args[0]);

  return 0;
}

static int
req_arg_write_rye_conf (T_MGMT_REQ_ARG * req_arg,
			int num_args, const T_MGMT_REQ_ARG_CONTAINER * args)
{
  T_MGMT_REQ_ARG_WRITE_RYE_CONF *arg_rye_conf;

  arg_rye_conf = &req_arg->value.write_rye_conf_arg;

  if (check_mgmt_req_arg (num_args, args, 2,
			  MGMT_REQ_ARG_INT, MGMT_REQ_ARG_STR) < 0)
    {
      return BR_ER_INVALID_ARGUMENT;
    }

  arg_rye_conf->size = MGMT_ARG_INT_VALUE (&args[0]);
  arg_rye_conf->contents = MGMT_ARG_STRING_VALUE (&args[1]);

  if (arg_rye_conf->size <= 0)
    {
      return BR_ER_INVALID_ARGUMENT;
    }

  return 0;
}

static int
req_arg_update_conf (T_MGMT_REQ_ARG * req_arg,
		     int num_args, const T_MGMT_REQ_ARG_CONTAINER * args)
{
  T_MGMT_REQ_ARG_UPDATE_CONF *update_conf_arg;

  update_conf_arg = &req_arg->value.update_conf_arg;

  if (check_mgmt_req_arg (num_args, args, 4, MGMT_REQ_ARG_STR,
			  MGMT_REQ_ARG_STR, MGMT_REQ_ARG_STR,
			  MGMT_REQ_ARG_STR) < 0)
    {
      return BR_ER_INVALID_ARGUMENT;
    }

  update_conf_arg->proc_name = MGMT_ARG_STRING_VALUE (&args[0]);
  update_conf_arg->sect_name = MGMT_ARG_STRING_VALUE (&args[1]);
  update_conf_arg->key = MGMT_ARG_STRING_VALUE (&args[2]);
  update_conf_arg->value = MGMT_ARG_STRING_VALUE (&args[3]);

  return 0;
}

static int
req_arg_get_conf (T_MGMT_REQ_ARG * req_arg,
		  int num_args, const T_MGMT_REQ_ARG_CONTAINER * args)
{
  T_MGMT_REQ_ARG_GET_CONF *get_conf_arg;
  int num_part;

  get_conf_arg = &req_arg->value.get_conf_arg;

  if (check_mgmt_req_arg (num_args, args, 4, MGMT_REQ_ARG_INT,
			  MGMT_REQ_ARG_STR, MGMT_REQ_ARG_STR,
			  MGMT_REQ_ARG_STR) < 0)
    {
      return BR_ER_INVALID_ARGUMENT;
    }

  num_part = MGMT_ARG_INT_VALUE (&args[0]);
  get_conf_arg->proc_name = MGMT_ARG_STRING_VALUE (&args[1]);
  get_conf_arg->sect_name = MGMT_ARG_STRING_VALUE (&args[2]);
  get_conf_arg->key = MGMT_ARG_STRING_VALUE (&args[3]);

  if (get_conf_arg->proc_name[0] == '\0')
    {
      get_conf_arg->proc_name = NULL;
    }
  if (get_conf_arg->sect_name[0] == '\0')
    {
      get_conf_arg->sect_name = NULL;
    }
  if (get_conf_arg->key[0] == '\0')
    {
      get_conf_arg->key = NULL;
    }

  if ((get_conf_arg->proc_name == NULL) ||
      (num_part >= 2 && get_conf_arg->sect_name == NULL) ||
      (num_part >= 3 && get_conf_arg->key == NULL))
    {
      return BR_ER_INVALID_ARGUMENT;
    }

  return 0;
}

static int
req_arg_br_acl_reload (T_MGMT_REQ_ARG * req_arg,
		       int num_args, const T_MGMT_REQ_ARG_CONTAINER * args)
{
  T_MGMT_REQ_ARG_BR_ACL_RELOAD *br_acl_reload_arg;

  br_acl_reload_arg = &req_arg->value.br_acl_reload_arg;

  if (check_mgmt_req_arg (num_args, args, 2, MGMT_REQ_ARG_INT,
			  MGMT_REQ_ARG_STR) < 0)
    {
      return BR_ER_INVALID_ARGUMENT;
    }

  br_acl_reload_arg->size = MGMT_ARG_INT_VALUE (&args[0]);
  br_acl_reload_arg->acl = MGMT_ARG_STRING_VALUE (&args[1]);

  if (br_acl_reload_arg->size <= 0 || br_acl_reload_arg->acl[0] == '\0')
    {
      return BR_ER_INVALID_ARGUMENT;
    }

  return 0;
}

static int
req_arg_get_no_arg (UNUSED_ARG T_MGMT_REQ_ARG * req_arg,
		    int num_args, const T_MGMT_REQ_ARG_CONTAINER * args)
{
  if (check_mgmt_req_arg (num_args, args, 0) < 0)
    {
      return BR_ER_INVALID_ARGUMENT;
    }

  return 0;
}

int
br_mgmt_get_req_arg (T_MGMT_REQ_ARG * req_arg,
		     const T_BROKER_REQUEST_MSG * brreq_msg)
{
  int opcode;
  const char *ptr;
  int msg_size;
  int i;
  T_REQ_ARG_READ_FUNC req_arg_read_func = NULL;
  int error = 0;
  int num_args;
  T_MGMT_REQ_ARG_CONTAINER *arg_container = NULL;

  opcode = brreq_msg->op_code;

  ptr = brreq_msg->op_code_msg;
  msg_size = brreq_msg->op_code_msg_size;

  req_arg->alloc_buffer = NULL;

  for (i = 0; req_Arg_read_func_table[i].func != NULL; i++)
    {
      if (req_Arg_read_func_table[i].opcode == opcode)
	{
	  req_arg_read_func = req_Arg_read_func_table[i].func;
	}
    }

  if (req_arg_read_func == NULL)
    {
      return BR_ER_INVALID_OPCODE;
    }

  ptr = br_mgmt_read_req_arg_int (&num_args, ptr, &msg_size);
  if (ptr == NULL || num_args < 0)
    {
      return BR_ER_INVALID_ARGUMENT;
    }

  arg_container = RYE_MALLOC (sizeof (T_MGMT_REQ_ARG_CONTAINER) * num_args);
  if (arg_container == NULL)
    {
      return BR_ER_NO_MORE_MEMORY;
    }
  memset (arg_container, 0, sizeof (T_MGMT_REQ_ARG_CONTAINER) * num_args);

  for (i = 0; i < num_args && error == 0; i++)
    {
      ptr = br_mgmt_read_req_arg_type (&arg_container[i].arg_type,
				       ptr, &msg_size);
      switch (arg_container[i].arg_type)
	{
	case MGMT_REQ_ARG_INT:
	  ptr = br_mgmt_read_req_arg_int (&arg_container[i].v.int_val,
					  ptr, &msg_size);
	  break;
	case MGMT_REQ_ARG_INT64:
	  ptr = br_mgmt_read_req_arg_int64 (&arg_container[i].v.int64_val,
					    ptr, &msg_size);
	  break;
	case MGMT_REQ_ARG_STR:
	  ptr = br_mgmt_read_req_arg_string (&arg_container[i].v.str_val,
					     ptr, &msg_size);
	  break;
	case MGMT_REQ_ARG_STR_ARRAY:
	  {
	    int num_str = 0;
	    const char **values = NULL;
	    ptr = br_mgmt_read_req_arg_str_arr (&num_str, &values,
						ptr, &msg_size);
	    arg_container[i].v.str_arr.num_str = num_str;
	    arg_container[i].v.str_arr.values = values;
	  }
	  break;
	case MGMT_REQ_ARG_INT_ARRAY:
	  {
	    int num_values = 0;
	    int *values = NULL;
	    ptr = br_mgmt_read_req_arg_int_arr (&num_values, &values,
						ptr, &msg_size);
	    arg_container[i].v.int_arr.num_values = num_values;
	    arg_container[i].v.int_arr.values = values;
	  }
	  break;
	default:
	  error = BR_ER_INVALID_ARGUMENT;
	  break;
	}
    }

  if (arg_container[num_args - 1].arg_type != MGMT_REQ_ARG_INT ||
      arg_container[num_args - 1].v.int_val != BR_MGMT_REQ_LAST_ARG_VALUE)
    {
      error = BR_ER_INVALID_ARGUMENT;
    }

  if (error == 0)
    {
      error = (*req_arg_read_func) (req_arg, num_args - 1, arg_container);
    }


  if (error < 0)
    {
      RYE_FREE_MEM (req_arg->alloc_buffer);
    }

  if (arg_container != NULL)
    {
      for (i = 0; i < num_args; i++)
	{
	  if (arg_container[i].arg_type == MGMT_REQ_ARG_STR_ARRAY)
	    {
	      RYE_FREE_MEM (arg_container[i].v.str_arr.values);
	    }
	  else if (arg_container[i].arg_type == MGMT_REQ_ARG_INT_ARRAY)
	    {
	      RYE_FREE_MEM (arg_container[i].v.int_arr.values);
	    }
	}
      RYE_FREE_MEM (arg_container);
    }

  return error;
}

void
br_mgmt_result_msg_init (T_MGMT_RESULT_MSG * result_msg)
{
  memset (result_msg, 0, sizeof (T_MGMT_RESULT_MSG));
  br_mgmt_result_msg_reset (result_msg);
}

void
br_mgmt_result_msg_reset (T_MGMT_RESULT_MSG * result_msg)
{
  int i;

  for (i = 0; i < BROKER_RESPONSE_MAX_ADDITIONAL_MSG; i++)
    {
      if (result_msg->msg[i] != NULL &&
	  result_msg->msg[i] != result_msg->buf[i])
	{
	  RYE_FREE_MEM (result_msg->msg[i]);
	}

      result_msg->msg[i] = result_msg->buf[i];
    }

  result_msg->num_msg = 0;
}

int
br_mgmt_result_msg_set (T_MGMT_RESULT_MSG * result_msg, int msg_size,
			const void *msg)
{
  int index = result_msg->num_msg;

  result_msg->msg_size[index] = msg_size;

  if (msg_size > MGMT_RESULT_MSG_MAX_SIZE)
    {
      result_msg->msg[index] = RYE_MALLOC (msg_size);
      if (result_msg->msg[index] == NULL)
	{
	  return BR_ER_NO_MORE_MEMORY;
	}
    }

  memcpy (result_msg->msg[index], msg, msg_size);

  result_msg->num_msg++;

  return 0;
}

void
br_copy_shard_node_info (T_SHARD_NODE_INFO * node, int node_id,
			 const char *dbname, const char *host, int port,
			 in_addr_t host_addr, HA_STATE_FOR_DRIVER ha_state,
			 const char *host_name)
{
  node->node_id = node_id;
  node->port = port;

  assert (strlen (dbname) < SRV_CON_DBNAME_SIZE);
  assert (strlen (host) < IP_ADDR_STR_LEN);

  STRNCPY (node->local_dbname, dbname, sizeof (node->local_dbname));
  STRNCPY (node->host_ip_str, host, sizeof (node->host_ip_str));

  if (host_addr == INADDR_NONE)
    {
      host_addr = inet_addr (host);
    }
  assert (host_addr != INADDR_NONE);
  node->host_ip_addr = host_addr;

  node->ha_state = ha_state;

  if (host_name == NULL)
    {
      node->host_name[0] = '\0';
    }
  else
    {
      STRNCPY (node->host_name, host_name, sizeof (node->host_name) - 1);
    }
}

void
br_set_init_error (int err_code, int os_err_code)
{
  br_Init_error.err_code = err_code;
  br_Init_error.os_err_code = os_err_code;
}
