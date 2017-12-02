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
 * local_mgmt.c -
 */

#ident "$Id$"

#include <stdio.h>
#include <signal.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <assert.h>
#include <arpa/inet.h>
#include <sys/stat.h>
#include <fcntl.h>

#include "system_parameter.h"
#include "cas_protocol.h"
#include "cas_common.h"
#include "broker_config.h"
#include "broker.h"
#include "cas_error.h"
#include "broker_util.h"
#include "broker_send_fd.h"
#include "rye_master_shm.h"
#include "broker_filename.h"
#include "cas_cci_internal.h"
#include "error_manager.h"
#include "master_heartbeat.h"
#include "dbi.h"

#define LOCK_SHM() 	pthread_mutex_lock (&shm_Lock)
#define UNLOCK_SHM() 	pthread_mutex_unlock (&shm_Lock)
static pthread_mutex_t shm_Lock;

#define FORK_EXEC_ERROR	99

typedef struct local_mgmt_job T_LOCAL_MGMT_JOB;
struct local_mgmt_job
{
  int clt_sock_fd;
  in_addr_t clt_ip;
  union
  {
    T_BROKER_REQUEST_MSG *req_msg;
    T_LOCAL_MGMT_CHILD_PROC_INFO *child_info;
  } info;
  struct local_mgmt_job *next;
};

typedef struct local_mgmt_job_queue T_LOCAL_MGMT_JOB_QUEUE;
struct local_mgmt_job_queue
{
  int num_workers;
  pthread_mutex_t lock;
  pthread_cond_t cond;
  T_LOCAL_MGMT_JOB *front;
  T_LOCAL_MGMT_JOB *back;
  int num_job;
  T_SHM_MGMT_QUEUE_INFO *shm_queue_info;
};

static int local_mgmt_init_child_process_queue (void);
static int local_mg_transfer_driver_req (SOCKET * clt_sock_fd,
					 in_addr_t clt_ip_addr,
					 const T_BROKER_REQUEST_MSG *
					 br_req_msg,
					 const char *transfer_broker_name);

static T_LOCAL_MGMT_JOB_QUEUE *local_mg_create_job_queue (void);
static T_LOCAL_MGMT_JOB_QUEUE *local_mg_init_mgmt_job_queue (void);
static THREAD_FUNC local_mg_child_process_waiter (void *arg);
static THREAD_FUNC local_mg_admin_worker (void *arg);

static T_LOCAL_MGMT_JOB *local_mg_process_queue_remove_by_pid (int pid);
static T_LOCAL_MGMT_JOB *local_mg_job_queue_remove (T_LOCAL_MGMT_JOB_QUEUE *
						    job_queue);
static int local_mg_job_queue_add (T_LOCAL_MGMT_JOB_QUEUE * job_queue,
				   SOCKET * clt_sock_fd,
				   in_addr_t clt_ip_addr,
				   T_BROKER_REQUEST_MSG * req_msg,
				   T_LOCAL_MGMT_CHILD_PROC_INFO * child_info,
				   bool need_mutex_lock);

static void make_child_output_filename (char *infile, int infile_bufsize,
					bool mkinfile,
					char *outfile, int outfile_bufsize,
					char *errfile, int errfile_bufsize,
					unsigned int seq);
static int local_mg_sync_shard_mgmt_info (T_LOCAL_MGMT_JOB * job,
					  const T_MGMT_REQ_ARG * req_arg,
					  T_MGMT_RESULT_MSG * result_msg);
static int local_mg_admin_launch_process (T_LOCAL_MGMT_JOB * job,
					  const T_MGMT_REQ_ARG * req_arg,
					  T_MGMT_RESULT_MSG * result_msg);
static int local_mg_get_shard_mgmt_info (T_LOCAL_MGMT_JOB * job,
					 const T_MGMT_REQ_ARG * req_arg,
					 T_MGMT_RESULT_MSG * result_msg);
static int local_mg_num_shard_version_info (T_LOCAL_MGMT_JOB * job,
					    const T_MGMT_REQ_ARG *
					    req_arg,
					    T_MGMT_RESULT_MSG * result_msg);
static int local_mg_read_rye_file (T_LOCAL_MGMT_JOB * job,
				   const T_MGMT_REQ_ARG *
				   req_arg, T_MGMT_RESULT_MSG * result_msg);
static int local_mg_write_rye_conf (T_LOCAL_MGMT_JOB * job,
				    const T_MGMT_REQ_ARG * req_arg,
				    T_MGMT_RESULT_MSG * result_msg);
static int local_mg_update_conf (T_LOCAL_MGMT_JOB * job,
				 const T_MGMT_REQ_ARG * req_arg,
				 T_MGMT_RESULT_MSG * result_msg);
static int local_mg_delete_conf (T_LOCAL_MGMT_JOB * job,
				 const T_MGMT_REQ_ARG * req_arg,
				 T_MGMT_RESULT_MSG * result_msg);
static int local_mg_get_conf (T_LOCAL_MGMT_JOB * job,
			      const T_MGMT_REQ_ARG * req_arg,
			      T_MGMT_RESULT_MSG * result_msg);
static int local_mg_br_acl_reload (T_LOCAL_MGMT_JOB * job,
				   const T_MGMT_REQ_ARG * req_arg,
				   T_MGMT_RESULT_MSG * result_msg);
static void shm_copy_child_process_info (void);
static char *local_mgmt_pack_str (char *ptr, char *str, int len);
static char *local_mgmt_pack_int (char *ptr, int value);
static int file_write (const char *filename, const char *buf, int size);

typedef struct
{
  int opcode;
  int (*func) (T_LOCAL_MGMT_JOB *, const T_MGMT_REQ_ARG *,
	       T_MGMT_RESULT_MSG *);
  const char *log_msg;
} T_LOCAL_MG_ADMIN_FUNC_TABLE;

static T_LOCAL_MG_ADMIN_FUNC_TABLE local_Mg_admin_func_table[] = {
  {BRREQ_OP_CODE_SYNC_SHARD_MGMT_INFO, local_mg_sync_shard_mgmt_info,
   "SHARD_MGMT_INFO"},
  {BRREQ_OP_CODE_LAUNCH_PROCESS, local_mg_admin_launch_process,
   "LAUNCH_PROCESS"},
  {BRREQ_OP_CODE_GET_SHARD_MGMT_INFO, local_mg_get_shard_mgmt_info,
   "GET_SHARD_MGMT_INFO"},
  {BRREQ_OP_CODE_NUM_SHARD_VERSION_INFO, local_mg_num_shard_version_info,
   "GET_NUM_SHARD_VERSION_INFO"},
  {BRREQ_OP_CODE_READ_RYE_FILE, local_mg_read_rye_file,
   "READ_RYE_CONF"},
  {BRREQ_OP_CODE_WRITE_RYE_CONF, local_mg_write_rye_conf,
   "READ_RYE_CONF"},
  {BRREQ_OP_CODE_UPDATE_CONF, local_mg_update_conf,
   "UPDATE_CONF"},
  {BRREQ_OP_CODE_DELETE_CONF, local_mg_delete_conf,
   "DELETE_CONF"},
  {BRREQ_OP_CODE_GET_CONF, local_mg_get_conf,
   "GET_CONF"},
  {BRREQ_OP_CODE_BR_ACL_RELOAD, local_mg_br_acl_reload,
   "BR_ACL_RELOAD"},
  {-1, NULL, NULL}
};

static T_SHM_LOCAL_MGMT_INFO *shm_Local_mgmt_info;
static T_LOCAL_MGMT_JOB_QUEUE *child_Process_queue;
static unsigned int child_Outfile_seq = 0;

static struct _local_mgmt_server_info
{
  char hostname[128];
} local_Mgmt_server_info;

int
local_mgmt_init ()
{
  if (local_mgmt_init_child_process_queue () < 0)
    {
      return -1;
    }

  memset (&local_Mgmt_server_info, 0, sizeof (local_Mgmt_server_info));
  gethostname (local_Mgmt_server_info.hostname,
	       sizeof (local_Mgmt_server_info.hostname) - 1);
  return 0;
}

static int
local_mgmt_init_child_process_queue ()
{
  pthread_t thr;

  child_Process_queue = local_mg_create_job_queue ();
  if (child_Process_queue == NULL)
    {
      return -1;
    }

  THREAD_BEGIN (thr, local_mg_child_process_waiter, NULL);

  return 0;
}

THREAD_FUNC
local_mgmt_receiver_thr_f (UNUSED_ARG void *arg)
{
  SOCKET clt_sock_fd;
  T_BROKER_REQUEST_MSG *br_req_msg;
  int err_code = 0;
  ER_MSG_INFO *er_msg;
  in_addr_t clt_ip_addr;
  T_LOCAL_MGMT_JOB_QUEUE *mgmt_job_queue;

  signal (SIGPIPE, SIG_IGN);

  er_msg = malloc (sizeof (ER_MSG_INFO));
  err_code = er_set_msg_info (er_msg);
  if (err_code != NO_ERROR)
    {
      return NULL;
    }

  shm_Local_mgmt_info = &shm_Appl->info.local_mgmt_info;

  mgmt_job_queue = local_mg_init_mgmt_job_queue ();
  if (mgmt_job_queue == NULL)
    {
      br_Process_flag = 0;
      br_set_init_error (BR_ER_INIT_LOCAL_MGMT_INIT_FAIL, 0);
      return NULL;
    }

  br_req_msg = brreq_msg_alloc (BRREQ_OP_CODE_MSG_MAX_SIZE);
  if (br_req_msg == NULL)
    {
      br_Process_flag = 0;
      br_set_init_error (BR_ER_INIT_NO_MORE_MEMORY, 0);
    }

  while (br_Process_flag)
    {
      err_code = 0;

      clt_sock_fd = br_mgmt_accept (&clt_ip_addr);
      if (IS_INVALID_SOCKET (clt_sock_fd))
	{
	  continue;
	}

      if (br_read_broker_request_msg (clt_sock_fd, br_req_msg) < 0)
	{
	  shm_Local_mgmt_info->error_req_count++;
	  err_code = CAS_ER_COMMUNICATION;
	  goto end;
	}

      if (IS_NORMAL_BROKER_OPCODE (br_req_msg->op_code))
	{
	  const char *transfer_broker_name;
	  transfer_broker_name =
	    brreq_msg_unpack_port_name (br_req_msg, NULL, NULL);

	  err_code = local_mg_transfer_driver_req (&clt_sock_fd, clt_ip_addr,
						   br_req_msg,
						   transfer_broker_name);

	  if (br_req_msg->op_code == BRREQ_OP_CODE_CAS_CONNECT)
	    {
	      shm_Local_mgmt_info->connect_req_count++;
	    }
	  else if (br_req_msg->op_code == BRREQ_OP_CODE_PING)
	    {
	      shm_Local_mgmt_info->ping_req_count++;
	    }
	  else if (br_req_msg->op_code == BRREQ_OP_CODE_QUERY_CANCEL)
	    {
	      shm_Local_mgmt_info->cancel_req_count++;
	    }
	}
      else if (IS_LOCAL_MGMT_OPCODE (br_req_msg->op_code))
	{
	  T_BROKER_REQUEST_MSG *clone_req_msg = brreq_msg_clone (br_req_msg);

	  if (clone_req_msg == NULL)
	    {
	      err_code = BR_ER_NO_MORE_MEMORY;
	    }
	  else
	    {
	      err_code = local_mg_job_queue_add (mgmt_job_queue,
						 &clt_sock_fd, clt_ip_addr,
						 clone_req_msg, NULL, true);
	    }
	  shm_Local_mgmt_info->admin_req_count++;
	}
      else
	{
	  shm_Local_mgmt_info->error_req_count++;
	  err_code = CAS_ER_COMMUNICATION;
	}

    end:
      if (err_code < 0)
	{
	  br_send_result_to_client (clt_sock_fd, err_code, NULL);
	  shm_Br->br_info[br_Index].connect_fail_count++;
	}

      RYE_CLOSE_SOCKET (clt_sock_fd);
    }

  brreq_msg_free (br_req_msg);

  return NULL;
}

static int
local_mg_transfer_driver_req (SOCKET * clt_sock_fd, in_addr_t clt_ip_addr,
			      const T_BROKER_REQUEST_MSG * br_req_msg,
			      const char *transfer_broker_name)
{
  SOCKET br_sock_fd;
  int status;
  struct timeval recv_time;
  const T_BROKER_INFO *br_info_service_broker;

  gettimeofday (&recv_time, NULL);

  br_info_service_broker = ut_find_broker (shm_Br->br_info,
					   shm_Br->num_broker,
					   transfer_broker_name,
					   NORMAL_BROKER);
  if (br_info_service_broker == NULL)
    {
      assert (false);
      return BR_ER_BROKER_NOT_FOUND;
    }

  br_sock_fd = br_connect_srv (br_info_service_broker->name, true, -1);
  if (IS_INVALID_SOCKET (br_sock_fd))
    {
      return BR_ER_BROKER_NOT_FOUND;
    }

  if (br_write_nbytes_to_client (br_sock_fd, br_req_msg->msg_buffer,
				 BRREQ_MSG_SIZE +
				 br_req_msg->op_code_msg_size,
				 BR_SOCKET_TIMEOUT_SEC) < 0)
    {
      RYE_CLOSE_SOCKET (br_sock_fd);
      return BR_ER_BROKER_NOT_FOUND;
    }

  if (send_fd (br_sock_fd, *clt_sock_fd, clt_ip_addr, &recv_time) < 0)
    {
      RYE_CLOSE_SOCKET (br_sock_fd);
      return BR_ER_BROKER_NOT_FOUND;
    }

  br_read_nbytes_from_client (br_sock_fd, (char *) &status,
			      sizeof (int), BR_SOCKET_TIMEOUT_SEC);

  RYE_CLOSE_SOCKET (br_sock_fd);
  RYE_CLOSE_SOCKET (*clt_sock_fd);

  return 0;
}

static T_LOCAL_MGMT_JOB_QUEUE *
local_mg_create_job_queue ()
{
  T_LOCAL_MGMT_JOB_QUEUE *job_queue;

  job_queue = RYE_MALLOC (sizeof (T_LOCAL_MGMT_JOB_QUEUE));

  if (job_queue != NULL)
    {
      memset (job_queue, 0, sizeof (T_LOCAL_MGMT_JOB_QUEUE));

      if (pthread_mutex_init (&job_queue->lock, NULL) < 0 ||
	  pthread_cond_init (&job_queue->cond, NULL) < 0)
	{
	  RYE_FREE_MEM (job_queue);
	  return NULL;
	}
    }

  return job_queue;
}

static T_LOCAL_MGMT_JOB_QUEUE *
local_mg_init_mgmt_job_queue ()
{
  T_LOCAL_MGMT_JOB_QUEUE *mgmt_job_queue;
  pthread_t mgmt_admin_worker;
  int i;

  if (pthread_mutex_init (&shm_Lock, NULL) < 0)
    {
      return NULL;
    }

  mgmt_job_queue = local_mg_create_job_queue ();
  if (mgmt_job_queue == NULL)
    {
      return NULL;
    }

  mgmt_job_queue->num_workers = 1;
  mgmt_job_queue->shm_queue_info = &shm_Local_mgmt_info->admin_req_queue;

  for (i = 0; i < mgmt_job_queue->num_workers; i++)
    {
      THREAD_BEGIN (mgmt_admin_worker, local_mg_admin_worker, mgmt_job_queue);
    }

  return mgmt_job_queue;
}

static int
read_outfile (const char *filename, char *result_buf, int result_buf_size)
{
  int read_len = 0;
  int fd;

  fd = open (filename, O_RDONLY);
  if (fd >= 0)
    {
      read_len = read (fd, result_buf, result_buf_size - 1);
      if (read_len < 0)
	{
	  read_len = 0;
	}

      close (fd);
    }

  return read_len;
}

static void
set_child_process_result (T_MGMT_RESULT_MSG * result_msg,
			  T_LOCAL_MGMT_CHILD_PROC_INFO * child_proc_info)
{
  char infile[BROKER_PATH_MAX];
  char outfile[BROKER_PATH_MAX];
  char errfile[BROKER_PATH_MAX];
  char out_buf[MGMT_RESULT_MSG_MAX_SIZE];
  int out_len = 0;
  char err_buf[MGMT_RESULT_MSG_MAX_SIZE];
  int err_len = 0;

  make_child_output_filename (infile, sizeof (infile), false,
			      outfile, sizeof (outfile),
			      errfile, sizeof (errfile),
			      child_proc_info->output_file_id);

  out_len = read_outfile (outfile, out_buf, sizeof (out_buf));
  err_len = read_outfile (errfile, err_buf, sizeof (err_buf));

  br_mgmt_result_msg_set (result_msg, out_len, out_buf);
  br_mgmt_result_msg_set (result_msg, err_len, err_buf);

  unlink (infile);
  unlink (outfile);
  unlink (errfile);
}

static THREAD_FUNC
local_mg_child_process_waiter (UNUSED_ARG void *arg)
{
  T_LOCAL_MGMT_JOB_QUEUE *job_queue = child_Process_queue;
  T_MGMT_RESULT_MSG result_msg;
  int error;
  ER_MSG_INFO *er_msg;

  er_msg = malloc (sizeof (ER_MSG_INFO));
  error = er_set_msg_info (er_msg);
  if (error != NO_ERROR)
    {
      return NULL;
    }

  br_mgmt_result_msg_init (&result_msg);

  while (br_Process_flag)
    {
      bool exist_child_process = false;
      int child_pid;
      int child_exit_status;

      pthread_mutex_lock (&job_queue->lock);

      if (job_queue->num_job > 0)
	{
	  exist_child_process = true;
	}
      else
	{
	  pthread_cond_wait (&job_queue->cond, &job_queue->lock);
	}

      pthread_mutex_unlock (&job_queue->lock);

      if (exist_child_process == false)
	{
	  continue;
	}

      child_pid = waitpid (-1, &child_exit_status, 0);

      if (child_pid > 0)
	{
	  T_LOCAL_MGMT_JOB *job;

	  pthread_mutex_lock (&job_queue->lock);

	  job = local_mg_process_queue_remove_by_pid (child_pid);
	  shm_copy_child_process_info ();

	  pthread_mutex_unlock (&job_queue->lock);

	  assert (job);

	  if (job != NULL && !IS_INVALID_SOCKET (job->clt_sock_fd))
	    {
	      int exit_status;
	      if (WIFEXITED (child_exit_status))
		{
		  exit_status = WEXITSTATUS (child_exit_status);
		  if (exit_status == FORK_EXEC_ERROR)
		    {
		      exit_status = MGMT_LAUNCH_ERROR_EXEC_FAIL;
		    }
		}
	      else
		{
		  exit_status = MGMT_LAUNCH_ERROR_ABNORMALLY_TERMINATED;
		}

	      exit_status = htonl (exit_status);
	      br_mgmt_result_msg_set (&result_msg, sizeof (int),
				      &exit_status);

	      set_child_process_result (&result_msg, job->info.child_info);

	      br_send_result_to_client (job->clt_sock_fd, 0, &result_msg);
	      RYE_CLOSE_SOCKET (job->clt_sock_fd);

	      br_mgmt_result_msg_reset (&result_msg);
	    }

	  if (job != NULL)
	    {
	      RYE_FREE_MEM (job->info.child_info);
	      RYE_FREE_MEM (job);
	    }
	}
    }

  return NULL;
}

static THREAD_FUNC
local_mg_admin_worker (void *arg)
{
  T_LOCAL_MGMT_JOB_QUEUE *job_queue = (T_LOCAL_MGMT_JOB_QUEUE *) arg;
  T_LOCAL_MGMT_JOB *job;
  int i;
  T_LOCAL_MG_ADMIN_FUNC_TABLE *admin_func;
  int err_code;
  T_MGMT_REQ_ARG req_arg;
  T_MGMT_RESULT_MSG result_msg;
  int error;
  ER_MSG_INFO *er_msg;

  er_msg = malloc (sizeof (ER_MSG_INFO));
  error = er_set_msg_info (er_msg);
  if (error != NO_ERROR)
    {
      return NULL;
    }

  br_mgmt_result_msg_init (&result_msg);

  while (br_Process_flag)
    {
      pthread_mutex_lock (&job_queue->lock);

      job = local_mg_job_queue_remove (job_queue);
      if (job == NULL)
	{
	  pthread_cond_wait (&job_queue->cond, &job_queue->lock);
	}

      pthread_mutex_unlock (&job_queue->lock);

      if (job != NULL)
	{
	  admin_func = NULL;
	  for (i = 0; local_Mg_admin_func_table[i].func != NULL; i++)
	    {
	      if (local_Mg_admin_func_table[i].opcode ==
		  job->info.req_msg->op_code)
		{
		  admin_func = &local_Mg_admin_func_table[i];
		  break;
		}
	    }

	  if (admin_func == NULL)
	    {
	      err_code = BR_ER_INVALID_OPCODE;
	    }
	  else
	    {
	      err_code = br_mgmt_get_req_arg (&req_arg, job->info.req_msg);

	      if (err_code >= 0)
		{
		  err_code = (*admin_func->func) (job, &req_arg, &result_msg);
		}

	      RYE_FREE_MEM (req_arg.alloc_buffer);
	    }

	  if (!IS_INVALID_SOCKET (job->clt_sock_fd))
	    {
	      br_send_result_to_client (job->clt_sock_fd, err_code,
					&result_msg);
	      RYE_CLOSE_SOCKET (job->clt_sock_fd);
	    }

	  RYE_FREE_MEM (job->info.req_msg);
	  RYE_FREE_MEM (job);

	  br_mgmt_result_msg_reset (&result_msg);
	}
    }

  return NULL;
}

static void
set_job_queue_job_count (T_LOCAL_MGMT_JOB_QUEUE * job_queue, int num_job)
{
  job_queue->num_job = num_job;
  if (job_queue->shm_queue_info)
    {
      job_queue->shm_queue_info->num_job = num_job;
    }
}

static int
local_mg_job_queue_add (T_LOCAL_MGMT_JOB_QUEUE * job_queue,
			SOCKET * clt_sock_fd,
			in_addr_t clt_ip_addr,
			T_BROKER_REQUEST_MSG * req_msg,
			T_LOCAL_MGMT_CHILD_PROC_INFO * child_info,
			bool need_mutex_lock)
{
  T_LOCAL_MGMT_JOB *job = NULL;

  if (req_msg != NULL || child_info != NULL)
    {
      job = RYE_MALLOC (sizeof (T_LOCAL_MGMT_JOB));
      if (job != NULL)
	{
	  memset (job, 0, sizeof (T_LOCAL_MGMT_JOB));

	  job->clt_sock_fd = *clt_sock_fd;
	  job->clt_ip = clt_ip_addr;

	  if (req_msg != NULL)
	    {
	      job->info.req_msg = req_msg;
	    }
	  else
	    {
	      job->info.child_info = child_info;
	    }
	}

      if (job == NULL)
	{
	  return BR_ER_NO_MORE_MEMORY;
	}
    }

  if (need_mutex_lock)
    {
      pthread_mutex_lock (&job_queue->lock);
    }

  if (job != NULL)
    {
      job->next = NULL;

      if (job_queue->front == NULL)
	{
	  job_queue->front = job_queue->back = job;
	}
      else
	{
	  job_queue->front->next = job;
	  job_queue->front = job;
	}
      set_job_queue_job_count (job_queue, job_queue->num_job + 1);
    }

  pthread_cond_signal (&job_queue->cond);

  if (need_mutex_lock)
    {
      pthread_mutex_unlock (&job_queue->lock);
    }

  *clt_sock_fd = INVALID_SOCKET;

  return 0;
}

static T_LOCAL_MGMT_JOB *
local_mg_job_queue_remove (T_LOCAL_MGMT_JOB_QUEUE * job_queue)
{
  T_LOCAL_MGMT_JOB *job;

  job = job_queue->back;
  if (job == NULL)
    {
      return NULL;
    }

  job_queue->back = job->next;

  if (job_queue->back == NULL)
    {
      job_queue->front = NULL;
    }

  job->next = NULL;
  set_job_queue_job_count (job_queue, job_queue->num_job - 1);

  return job;
}

static T_LOCAL_MGMT_JOB *
local_mg_process_queue_remove_by_pid (int pid)
{
  T_LOCAL_MGMT_JOB_QUEUE *job_queue = child_Process_queue;
  T_LOCAL_MGMT_JOB *job;

  if (job_queue->back->info.child_info->pid == pid)
    {
      job = local_mg_job_queue_remove (job_queue);
    }
  else
    {
      T_LOCAL_MGMT_JOB *prev;

      prev = job_queue->back;
      job = prev->next;

      while (job)
	{
	  if (job->info.child_info->pid == pid)
	    {
	      prev->next = job->next;
	      if (prev->next == NULL)
		{
		  job_queue->front = prev;
		}
	      job->next = NULL;
	      set_job_queue_job_count (job_queue, job_queue->num_job - 1);
	      break;
	    }

	  prev = job;
	  job = prev->next;
	}
    }

  assert (job != NULL);

  return job;
}

static void
set_shm_shard_info_version (const T_MGMT_REQ_ARG * req_arg)
{
  int i;

  for (i = 0; i < SHM_MAX_SHARD_VERSION_INFO_COUNT; i++)
    {
      int64_t last_version;

      last_version = MAX (req_arg->value.shard_mgmt_info.nodeid_ver,
			  req_arg->value.shard_mgmt_info.groupid_ver);

      if (strcmp (shm_Br->shard_version_info[i].local_dbname,
		  req_arg->value.shard_mgmt_info.local_dbname) == 0)
	{
	  if (shm_Br->shard_version_info[i].shard_info_ver <= last_version)
	    {
	      shm_Br->shard_version_info[i].shard_info_ver = last_version;
	      shm_Br->shard_version_info[i].sync_time = time (NULL);
	    }

	  break;
	}

      if (shm_Br->shard_version_info[i].local_dbname[0] == '\0')
	{

	  strncpy (shm_Br->shard_version_info[i].local_dbname,
		   req_arg->value.shard_mgmt_info.local_dbname,
		   sizeof (shm_Br->shard_version_info[i].local_dbname) - 1);


	  shm_Br->shard_version_info[i].shard_info_ver = last_version;
	  shm_Br->shard_version_info[i].sync_time = time (NULL);
	  shm_Br->num_shard_version_info++;

	  break;
	}
    }
}

static int
local_mg_sync_shard_mgmt_info (T_LOCAL_MGMT_JOB * job,
			       const T_MGMT_REQ_ARG * req_arg,
			       UNUSED_ARG T_MGMT_RESULT_MSG * result_msg)
{
  HA_STATE server_state;
  PRM_NODE_INFO shard_mgmt_node_info;

  prm_set_node_info (&shard_mgmt_node_info,
		     job->clt_ip, req_arg->value.shard_mgmt_info.port);

  rye_master_shm_add_shard_mgmt_info (req_arg->value.shard_mgmt_info.
				      local_dbname,
				      req_arg->value.shard_mgmt_info.
				      global_dbname,
				      req_arg->value.shard_mgmt_info.nodeid,
				      &shard_mgmt_node_info);

  LOCK_SHM ();

  set_shm_shard_info_version (req_arg);

  UNLOCK_SHM ();

  br_mgmt_result_msg_set (result_msg,
			  strlen (local_Mgmt_server_info.hostname) + 1,
			  local_Mgmt_server_info.hostname);

  if (rye_master_shm_get_server_state
      (req_arg->value.shard_mgmt_info.local_dbname,
       &server_state) == NO_ERROR)
    {
      int ha_state_for_driver;

      switch (server_state)
	{
	case HA_STATE_MASTER:
	  ha_state_for_driver = HA_STATE_FOR_DRIVER_MASTER;
	  break;
	case HA_STATE_TO_BE_MASTER:
	  ha_state_for_driver = HA_STATE_FOR_DRIVER_TO_BE_MASTER;
	  break;
	case HA_STATE_SLAVE:
	  ha_state_for_driver = HA_STATE_FOR_DRIVER_SLAVE;
	  break;
	case HA_STATE_TO_BE_SLAVE:
	  ha_state_for_driver = HA_STATE_FOR_DRIVER_TO_BE_SLAVE;
	  break;
	case HA_STATE_REPLICA:
	  ha_state_for_driver = HA_STATE_FOR_DRIVER_REPLICA;
	  break;
	default:
	  ha_state_for_driver = HA_STATE_FOR_DRIVER_UNKNOWN;
	  break;
	}

      ha_state_for_driver = htonl (ha_state_for_driver);
      br_mgmt_result_msg_set (result_msg, sizeof (int), &ha_state_for_driver);
    }

  return 0;
}

static void
copy_launch_proc_cmd (char *buf, int bufsize, int argc, char **argv)
{
  int i, n;

  buf[bufsize - 1] = '\0';
  bufsize--;

  for (i = 0; i < argc && bufsize > 0; i++)
    {
      n = snprintf (buf, bufsize, "%s ", argv[i]);
      buf += n;
      bufsize -= n;
    }
}

static void
make_child_output_filename (char *infile, int infile_bufsize, bool mkinfile,
			    char *outfile, int outfile_bufsize,
			    char *errfile, int errfile_bufsize,
			    unsigned int seq)
{
  char dirname[BROKER_PATH_MAX];

  get_rye_file (FID_CAS_TMP_DIR, dirname, BROKER_PATH_MAX);

  snprintf (infile, infile_bufsize, "%s/local_mgmt.%u.in", dirname, seq);
  if (mkinfile)
    {
      int fd;
      fd = open (infile, O_CREAT | O_TRUNC | O_WRONLY, 0666);
      if (fd >= 0)
	{
	  close (fd);
	}
    }

  snprintf (outfile, outfile_bufsize, "%s/local_mgmt.%u.out", dirname, seq);
  snprintf (errfile, errfile_bufsize, "%s/local_mgmt.%u.err", dirname, seq);
}

static int
local_mg_admin_launch_process (T_LOCAL_MGMT_JOB * job,
			       const T_MGMT_REQ_ARG * req_arg,
			       UNUSED_ARG T_MGMT_RESULT_MSG * result_msg)
{
#define MAX_APPEND_ARGS	2
  const T_MGMT_REQ_ARG_LAUNCH_PROCESS *launch_arg;
  char cmd[BROKER_PATH_MAX] = "";
  int pid;
  char argv0[BROKER_PATH_MAX] = "";
  char **argv;
  int argc;
  char caller_host[32];
  char migrator_mgmt_host_opt[] = "--mgmt-host";
  int error = 0;
  int i;
  T_LOCAL_MGMT_CHILD_PROC_INFO *child_info = NULL;
  const char *rye_root_dir;

  launch_arg = &req_arg->value.launch_process_arg;

  assert (launch_arg->argc > 0);

  argv =
    RYE_MALLOC (sizeof (char *) * (launch_arg->argc + 2 + MAX_APPEND_ARGS));
  if (argv == NULL)
    {
      return BR_ER_NO_MORE_MEMORY;
    }

  child_info = RYE_MALLOC (sizeof (T_LOCAL_MGMT_CHILD_PROC_INFO));
  if (child_info == NULL)
    {
      RYE_FREE_MEM (argv);
      return BR_ER_NO_MORE_MEMORY;
    }

  argv[0] = argv0;
  for (i = 1; i < launch_arg->argc; i++)
    {
      argv[i] = launch_arg->argv[i];
    }
  argc = launch_arg->argc;
  argv[argc] = NULL;

  ut_get_ipv4_string (caller_host, sizeof (caller_host), job->clt_ip);

  rye_root_dir = envvar_root ();

  if (launch_arg->launch_process_id == MGMT_LAUNCH_PROCESS_MIGRATOR)
    {
      const char *rye_migrator_name = "rye_migrator";

      sprintf (cmd, "%s/bin/%s", rye_root_dir, rye_migrator_name);
      sprintf (argv0, "%s_%s", rye_migrator_name, caller_host);

      argv[argc++] = migrator_mgmt_host_opt;
      argv[argc++] = caller_host;
      argv[argc] = NULL;
    }
  else if (launch_arg->launch_process_id == MGMT_LAUNCH_PROCESS_RYE_COMMAND)
    {
      const char *rye_command = launch_arg->argv[0];

      sprintf (cmd, "%s/bin/%s", rye_root_dir, rye_command);
      sprintf (argv0, "%s", rye_command);
    }
  else
    {
      RYE_FREE_MEM (argv);
      RYE_FREE_MEM (child_info);
      return BR_ER_LAUNCH_PROCESS;
    }

  assert (cmd[0] != '\0');
  assert (argv0[0] != '\0');

  signal (SIGCHLD, SIG_DFL);

  pthread_mutex_lock (&child_Process_queue->lock);

  if (child_Outfile_seq == 0)
    {
      child_Outfile_seq++;
    }
  child_info->output_file_id = child_Outfile_seq++;

  pid = fork ();
  if (pid == 0)
    {
      char child_infile[BROKER_PATH_MAX];
      char child_outfile[BROKER_PATH_MAX];
      char child_errfile[BROKER_PATH_MAX];
      int fd_stdin = -1;
      int fd_stdout = -1;
      int fd_stderr = -1;
      char working_dir[BROKER_PATH_MAX];

      get_rye_file (FID_CAS_TMP_DIR, working_dir, BROKER_PATH_MAX);
      chdir (working_dir);

      for (i = 3; i < 256; i++)
	{
	  close (i);
	}

      for (i = 0; i < launch_arg->num_env; i++)
	{
	  putenv (launch_arg->envp[i]);
	}

      if (child_info->output_file_id > 0)
	{
	  make_child_output_filename (child_infile, sizeof (child_infile),
				      true,
				      child_outfile, sizeof (child_outfile),
				      child_errfile, sizeof (child_errfile),
				      child_info->output_file_id);
	  fd_stdin = open (child_infile, O_RDONLY, 0666);
	  if (fd_stdin != -1 && fd_stdin != 0)
	    {
	      dup2 (fd_stdin, 0);
	      close (fd_stdin);
	    }

	  fd_stdout = open (child_outfile, O_CREAT | O_TRUNC | O_WRONLY,
			    0666);
	  if (fd_stdout != -1 && fd_stdout != 1)
	    {
	      dup2 (fd_stdout, 1);
	      close (fd_stdout);
	    }

	  fd_stderr = open (child_errfile, O_CREAT | O_TRUNC | O_WRONLY,
			    0666);
	  if (fd_stderr != -1 && fd_stderr != 2)
	    {
	      dup2 (fd_stderr, 2);
	      close (fd_stderr);
	    }
	}

      execv (cmd, argv);

      exit (FORK_EXEC_ERROR);
    }

  if (pid < 0)
    {
      error = BR_ER_LAUNCH_PROCESS;
    }
  else
    {
      child_info->pid = pid;
      copy_launch_proc_cmd (child_info->cmd, sizeof (child_info->cmd), argc,
			    argv);

      error = local_mg_job_queue_add (child_Process_queue,
				      &job->clt_sock_fd, job->clt_ip,
				      NULL, child_info, false);

      shm_copy_child_process_info ();
    }

  pthread_mutex_unlock (&child_Process_queue->lock);

  RYE_FREE_MEM (argv);

  if (error < 0)
    {
      RYE_FREE_MEM (child_info);
    }

  return error;
}

static void
shm_copy_child_process_info ()
{
  int num_child = 0;
  T_LOCAL_MGMT_JOB *job = child_Process_queue->back;

  while (job != NULL && num_child < SHM_MAX_CHILD_INFO)
    {
      if (job->info.child_info != NULL)
	{
	  shm_Local_mgmt_info->child_process_info[num_child++] =
	    *job->info.child_info;
	}
      job = job->next;
    }

  shm_Local_mgmt_info->num_child_process = num_child;
}

static int
local_mg_get_shard_mgmt_info (UNUSED_ARG T_LOCAL_MGMT_JOB * job,
			      UNUSED_ARG const T_MGMT_REQ_ARG * req_arg,
			      T_MGMT_RESULT_MSG * result_msg)
{
  int i;
  int msg_size;
  char *send_msg;
  char *p;
  int num_shard_mgmt = 0;

  msg_size = sizeof (int);

  for (i = 0; i < shm_Br->num_broker; i++)
    {
      if (shm_Br->br_info[i].broker_type == SHARD_MGMT)
	{
	  msg_size += strlen (shm_Br->br_info[i].shard_global_dbname);
	  msg_size += (1 + sizeof (int) * 3);
	  num_shard_mgmt++;
	}
    }

  send_msg = RYE_MALLOC (msg_size);
  if (send_msg == NULL)
    {
      return BR_ER_NO_MORE_MEMORY;
    }

  p = local_mgmt_pack_int (send_msg, num_shard_mgmt);

  for (i = 0; i < shm_Br->num_broker; i++)
    {
      if (shm_Br->br_info[i].broker_type == SHARD_MGMT)
	{
	  int dbname_len;

	  dbname_len = (strlen (shm_Br->br_info[i].shard_global_dbname) + 1);

	  p = local_mgmt_pack_int (p, dbname_len + sizeof (int) * 2);
	  p = local_mgmt_pack_str (p, shm_Br->br_info[i].shard_global_dbname,
				   dbname_len);
	  p = local_mgmt_pack_int (p, shm_Br->br_info[i].port);
	}
    }

  assert (msg_size == (int) (p - send_msg));

  br_mgmt_result_msg_set (result_msg, msg_size, send_msg);

  RYE_FREE_MEM (send_msg);

  return 0;
}

static int
local_mg_num_shard_version_info (UNUSED_ARG T_LOCAL_MGMT_JOB * job,
				 UNUSED_ARG const T_MGMT_REQ_ARG * req_arg,
				 T_MGMT_RESULT_MSG * result_msg)
{
  int num_shard_version_info;

  num_shard_version_info = shm_Br->num_shard_version_info;
  if (num_shard_version_info <= 0)
    {
      if (time (NULL) - shm_Br->br_info[br_Index].start_time <= 60)
	{
	  num_shard_version_info = -1;
	}
      else
	{
	  num_shard_version_info = 0;
	}
    }

  num_shard_version_info = htonl (num_shard_version_info);
  br_mgmt_result_msg_set (result_msg, sizeof (int), &num_shard_version_info);

  return 0;
}

static char *
get_rye_conf_path (char *path, size_t size)
{
  return envvar_confdir_file (path, size, sysprm_auto_conf_file_name);
}

static int
local_mg_read_rye_file (UNUSED_ARG T_LOCAL_MGMT_JOB * job,
			UNUSED_ARG const T_MGMT_REQ_ARG * req_arg,
			T_MGMT_RESULT_MSG * result_msg)
{
  const T_MGMT_REQ_ARG_READ_RYE_FILE *arg_read_rye_file;
  char rye_file_path[PATH_MAX];
  struct stat stat_buf;
  char *contents;
  int file_size, read_len;

  arg_read_rye_file = &req_arg->value.read_rye_file_arg;

  rye_file_path[0] = '\0';

  if (arg_read_rye_file->which_file == READ_RYE_FILE_RYE_CONF)
    {
      get_rye_conf_path (rye_file_path, PATH_MAX);
    }
  else if (arg_read_rye_file->which_file == READ_RYE_FILE_BR_ACL)
    {
      get_rye_file (FID_ACCESS_CONTROL_FILE, rye_file_path,
		    sizeof (rye_file_path));
    }

  if (rye_file_path[0] == '\0' || stat (rye_file_path, &stat_buf) < 0
      || stat_buf.st_size <= 0)
    {
      return BR_ER_SEND_RYE_FILE;
    }

  file_size = stat_buf.st_size;
  contents = RYE_MALLOC (file_size + 1);
  read_len = read_outfile (rye_file_path, contents, file_size + 1);
  if (read_len != file_size)
    {
      assert (false);
      ;				/* TODO - avoid compile error */
    }

  br_mgmt_result_msg_set (result_msg, file_size, contents);

  RYE_FREE_MEM (contents);

  return 0;
}

static int
local_mg_write_rye_conf (UNUSED_ARG T_LOCAL_MGMT_JOB * job,
			 const T_MGMT_REQ_ARG * req_arg,
			 UNUSED_ARG T_MGMT_RESULT_MSG * result_msg)
{
  const T_MGMT_REQ_ARG_WRITE_RYE_CONF *arg_write_conf;
  int fd;
  int write_len;
  char rye_conf_path[PATH_MAX];

  arg_write_conf = &req_arg->value.write_rye_conf_arg;

  if (get_rye_conf_path (rye_conf_path, PATH_MAX) == NULL)
    {
      return BR_ER_RYE_CONF;
    }

  fd = open (rye_conf_path, O_WRONLY | O_CREAT | O_TRUNC, 0644);
  if (fd < 0)
    {
      return BR_ER_RYE_CONF;
    }

  write_len = write (fd, arg_write_conf->contents, arg_write_conf->size);

  close (fd);

  if (write_len == arg_write_conf->size)
    {
      return 0;
    }
  else
    {
      assert (0);
      return BR_ER_RYE_CONF;
    }
}

static int
local_mg_update_conf (UNUSED_ARG T_LOCAL_MGMT_JOB * job,
		      const T_MGMT_REQ_ARG * req_arg,
		      UNUSED_ARG T_MGMT_RESULT_MSG * result_msg)
{
  const T_MGMT_REQ_ARG_UPDATE_CONF *arg_update_conf;
  int err;

  arg_update_conf = &req_arg->value.update_conf_arg;

  err = db_update_persist_conf_file (arg_update_conf->proc_name,
				     arg_update_conf->sect_name,
				     arg_update_conf->key,
				     arg_update_conf->value);

  if (err == NO_ERROR)
    {
      return 0;
    }
  else
    {
      assert (0);
      return BR_ER_RYE_CONF;
    }
}

static int
local_mg_delete_conf (UNUSED_ARG T_LOCAL_MGMT_JOB * job,
		      const T_MGMT_REQ_ARG * req_arg,
		      UNUSED_ARG T_MGMT_RESULT_MSG * result_msg)
{
  const T_MGMT_REQ_ARG_GET_CONF *arg_delete_conf;
  int err;

  arg_delete_conf = &req_arg->value.get_conf_arg;

  if (arg_delete_conf->key != NULL)
    {
      err = db_delete_key_persist_conf_file (arg_delete_conf->proc_name,
					     arg_delete_conf->sect_name,
					     arg_delete_conf->key);
    }
  else if (arg_delete_conf->sect_name != NULL)
    {
      err = db_delete_sect_persist_conf_file (arg_delete_conf->proc_name,
					      arg_delete_conf->sect_name);
    }
  else
    {
      err = db_delete_proc_persist_conf_file (arg_delete_conf->proc_name);
    }

  if (err == NO_ERROR)
    {
      return 0;
    }
  else
    {
      assert (0);
      return BR_ER_RYE_CONF;
    }
}

static int
local_mg_get_conf (UNUSED_ARG T_LOCAL_MGMT_JOB * job,
		   const T_MGMT_REQ_ARG * req_arg,
		   T_MGMT_RESULT_MSG * result_msg)
{
  const T_MGMT_REQ_ARG_GET_CONF *arg_get_conf;
  char param_value[1024];

  arg_get_conf = &req_arg->value.get_conf_arg;

  if (sysprm_get_persist_conf (param_value, sizeof (param_value),
			       arg_get_conf->proc_name,
			       arg_get_conf->sect_name,
			       arg_get_conf->key) == NO_ERROR)
    {
      br_mgmt_result_msg_set (result_msg, strlen (param_value) + 1,
			      param_value);
      return 0;
    }

  return BR_ER_RYE_CONF;
}

static int
local_mg_br_acl_reload (UNUSED_ARG T_LOCAL_MGMT_JOB * job,
			const T_MGMT_REQ_ARG * req_arg,
			UNUSED_ARG T_MGMT_RESULT_MSG * result_msg)
{
  const T_MGMT_REQ_ARG_BR_ACL_RELOAD *arg_br_acl_reload;
  char tmp_file[BROKER_PATH_MAX] = "";
  int n;
  int childpid;

  arg_br_acl_reload = &req_arg->value.br_acl_reload_arg;

  get_rye_file (FID_CAS_TMP_DIR, tmp_file, sizeof (tmp_file));
  n = strlen (tmp_file);
  n +=
    snprintf (tmp_file + n, sizeof (tmp_file) - n, "acl.tmp.%d", getpid ());
  if (n >= (int) sizeof (tmp_file) - 1)
    {
      return BR_ER_BR_ACL_RELOAD;
    }

  if (file_write (tmp_file, arg_br_acl_reload->acl,
		  arg_br_acl_reload->size) < 0)
    {
      return BR_ER_BR_ACL_RELOAD;
    }

  childpid = fork ();
  if (childpid < 0)
    {
      return BR_ER_BR_ACL_RELOAD;
    }
  else if (childpid == 0)
    {
      char rye_cmd[BROKER_PATH_MAX];

      for (n = 3; n < 256; n++)
	{
	  close (n);
	}

      snprintf (rye_cmd, sizeof (rye_cmd), "%s/bin/rye", envvar_root ());

      execl (rye_cmd, rye_cmd, "broker", "acl", "reload", tmp_file, NULL);

      exit (-1);
    }
  else
    {
      int child_exit_status;

      if (waitpid (childpid, &child_exit_status, 0) < 0)
	{
	  return BR_ER_BR_ACL_RELOAD;
	}

      return (child_exit_status == 0 ? 0 : BR_ER_BR_ACL_RELOAD);
    }
}

static int
file_write (const char *filename, const char *buf, int size)
{
  int fd;
  int n;

  fd = open (filename, O_CREAT | O_TRUNC | O_WRONLY, 0666);
  if (fd < 0)
    {
      return -1;
    }

  n = write (fd, buf, size);

  close (fd);

  return (n == size ? 0 : -1);
}

static char *
local_mgmt_pack_int (char *ptr, int value)
{
  int tmp = htonl (value);
  memcpy (ptr, &tmp, sizeof (int));
  return (ptr + sizeof (int));
}

static char *
local_mgmt_pack_str (char *ptr, char *str, int len)
{
  ptr = local_mgmt_pack_int (ptr, len);
  memcpy (ptr, str, len);
  return (ptr + len);
}
