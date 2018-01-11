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
 * master_heartbeat.c - heartbeat module in rye_master
 */

#ident "$Id$"


#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <signal.h>
#include <errno.h>
#include <sys/wait.h>
#include <assert.h>

#include <unistd.h>
#include <fcntl.h>
#include <pthread.h>

#include "dbi.h"
#include "porting.h"
#include "tcp.h"
#include "object_representation.h"
#include "connection_cl.h"
#include "master_util.h"
#include "master_heartbeat.h"
#include "master_request.h"
#include "heartbeat.h"
#include "message_catalog.h"
#include "utility.h"
#include "broker_admin_pub.h"
#include "rye_shm.h"
#include "rye_master_shm.h"
#include "rye_server_shm.h"
#include "cas_cci.h"
#include "ds_queue.h"

#include "cas_cci_internal.h"

#include "fault_injection.h"

#define HB_INFO_STR_MAX         8192
#define SERVER_DEREG_MAX_POLL_COUNT 10

typedef struct hb_deactivate_info HB_DEACTIVATE_INFO;
struct hb_deactivate_info
{
  int *server_pid_list;
  int server_count;
  bool info_started;
};

/* list */
static void hb_list_add (HB_LIST ** p, HB_LIST * n);
static void hb_list_remove (HB_LIST * n);
static void hb_list_move (HB_LIST ** dest_pp, HB_LIST ** source_pp);

/* jobs */
static void hb_add_timeval (struct timeval *tv_p, unsigned int msec);
static int hb_compare_timeval (struct timeval *arg1, struct timeval *arg2);
static const char *hb_strtime (char *s, unsigned int max,
			       struct timeval *tv_p);

static int hb_job_queue (HB_JOB * jobs, unsigned int job_type,
			 HB_JOB_ARG * arg, unsigned int msec);
static HB_JOB_ENTRY *hb_job_dequeue (HB_JOB * jobs);
static void hb_job_set_expire_and_reorder (HB_JOB * jobs,
					   unsigned int job_type,
					   unsigned int msec);
static void hb_job_shutdown (HB_JOB * jobs);


/* cluster jobs */
static void hb_cluster_job_init (HB_JOB_ARG * arg);
static void hb_cluster_job_heartbeat (HB_JOB_ARG * arg);
static void hb_cluster_job_calc_score (HB_JOB_ARG * arg);
static void hb_cluster_job_failover (HB_JOB_ARG * arg);
static void hb_cluster_job_failback (HB_JOB_ARG * arg);
static void hb_cluster_job_changeslave (HB_JOB_ARG * arg);
static void hb_cluster_job_changemode_force (HB_JOB_ARG * arg);
static void hb_cluster_job_check_ping (HB_JOB_ARG * arg);
static void hb_cluster_job_check_valid_ping_server (HB_JOB_ARG * arg);
static void hb_cluster_job_demote (HB_JOB_ARG * arg);

static void hb_cluster_request_heartbeat_to_all (void);
static void hb_cluster_send_heartbeat (bool is_req,
				       const PRM_NODE_INFO * node);
static void hb_cluster_receive_heartbeat (char *buffer, int len,
					  struct sockaddr_in *from,
					  socklen_t from_len);
static bool hb_cluster_is_isolated (void);
static bool hb_cluster_is_received_heartbeat_from_all (void);
static bool hb_cluster_check_valid_ping_server (void);

static int hb_cluster_calc_score (void);

static void hb_set_net_header (HBP_HEADER * header, unsigned char type,
			       bool is_req, unsigned short len,
			       unsigned int seq,
			       const PRM_NODE_INFO * dest_host);
static int hb_sockaddr (const PRM_NODE_INFO * node, struct sockaddr *saddr,
			socklen_t * slen);

/* common */
static int hb_check_ping (const char *host);

/* cluster jobs queue */
static HB_JOB_ENTRY *hb_cluster_job_dequeue (void);
static int hb_cluster_job_queue_debug (const char *fname, int line,
				       unsigned int job_type,
				       HB_JOB_ARG * arg, unsigned int msec);
#define hb_cluster_job_queue(job_type,arg,msec) \
        hb_cluster_job_queue_debug (ARG_FILE_LINE, job_type, arg, msec)

static int hb_cluster_job_set_expire_and_reorder (unsigned int job_type,
						  unsigned int msec);
static void hb_cluster_job_shutdown (void);

/* cluster node */
static HB_NODE_ENTRY *hb_add_node_to_cluster (PRM_NODE_INFO * node,
					      unsigned short priority);
static void hb_remove_node (HB_NODE_ENTRY * entry_p);
static void hb_cluster_remove_all_nodes (HB_NODE_ENTRY * first);
static HB_NODE_ENTRY *hb_return_node_by_name (const PRM_NODE_INFO *
					      node_info);
static HB_NODE_ENTRY *hb_return_node_by_name_except_me (const PRM_NODE_INFO *
							node_info);

static int hb_cluster_load_group_and_node_list (void);

/* ping host related functions */
static HB_PING_HOST_ENTRY *hb_add_ping_host (char *host_name);
static void hb_remove_ping_host (HB_PING_HOST_ENTRY * entry_p);
static void hb_cluster_remove_all_ping_hosts (HB_PING_HOST_ENTRY * first);

/* resource jobs */
static void hb_resource_job_proc_start (HB_JOB_ARG * arg);
static void hb_resource_job_confirm_start (HB_JOB_ARG * arg);
static void hb_resource_job_sync_server_state (HB_JOB_ARG * arg);
static void hb_resource_job_demote_start_shutdown (HB_JOB_ARG * arg);
static void hb_resource_job_demote_confirm_shutdown (HB_JOB_ARG * arg);
static void hb_resource_job_cleanup_all (HB_JOB_ARG * arg);
static void hb_resource_job_confirm_cleanup_all (HB_JOB_ARG * arg);
static void hb_resource_job_change_groupid_bitmap (HB_JOB_ARG * arg);

static void hb_resource_demote_start_shutdown_server_proc (void);
static bool hb_resource_demote_confirm_shutdown_server_proc (void);
static void hb_resource_demote_kill_server_proc (void);

/* resource job queue */
static HB_JOB_ENTRY *hb_resource_job_dequeue (void);
static int hb_resource_job_queue_debug (const char *fname, int line,
					unsigned int job_type,
					HB_JOB_ARG * arg, unsigned int msec);
#define hb_resource_job_queue(job_type, arg, msec) \
  hb_resource_job_queue_debug (ARG_FILE_LINE, job_type, arg, msec)

static int
hb_resource_job_set_expire_and_reorder (unsigned int job_type,
					unsigned int msec);

static void hb_resource_job_shutdown (void);

/* resource process */
static HB_PROC_ENTRY *hb_alloc_new_proc (void);
static void hb_remove_proc (HB_PROC_ENTRY * entry_p);
static void hb_remove_all_procs (HB_PROC_ENTRY * first);

static HB_PROC_ENTRY *hb_return_proc_by_args (char *args);
static HB_PROC_ENTRY *hb_return_proc_by_fd (int sfd);
static HB_PROC_ENTRY *hb_find_proc_by_server_state (HA_STATE server_state);
static void hb_proc_make_arg (char **arg, char *argv);

/* resource process connection */
static int hb_resource_sync_server_state (HB_PROC_ENTRY * proc, bool force);
static void hb_resource_get_server_eof (void);
static bool hb_resource_check_server_log_grow (void);

static int hb_server_start (const HA_CONF * ha_conf, char *db_name);
static int hb_repl_start (const HA_CONF * ha_conf);
static int hb_repl_stop (void);

static void *hb_thread_cluster_worker (void *arg);
static void *hb_thread_cluster_reader (void *arg);
static void *hb_thread_resource_worker (void *arg);
static void *hb_thread_check_disk_failure (void *arg);

#if 0
static int hb_get_new_job (HBNEW_JOB_ENTRY * ret_job_entry);
static void *hb_thread_worker (UNUSED_ARG void *arg);
#endif

/* initializer */
static int hb_cluster_initialize (void);
static int hb_cluster_job_initialize (void);
static int hb_resource_initialize (void);
static int hb_resource_job_initialize (void);
static int hb_job_request_initialize ();
static int hb_thread_initialize (void);

/* terminator */
static void hb_resource_cleanup (void);
static void hb_resource_shutdown_all_ha_procs (void);
static void hb_cluster_cleanup (void);
static void hb_kill_process (pid_t * pids, int count);

/* process command */
static const char *hb_process_state_string (int pstate);
static const char *hb_ping_result_string (int ping_result);

static int hb_reload_config (PRM_NODE_LIST * removed_nodes);

static int hb_help_sprint_processes_info (char *buffer, int max_length);
static int hb_help_sprint_nodes_info (char *buffer, int max_length);
static int hb_help_sprint_ping_host_info (char *buffer, int max_length);

static int hb_get_process_info (char *info, int max_size,
				HB_PROC_ENTRY * proc, int verbose_yn);
static int hb_remove_copylog (const HA_CONF * ha_conf,
			      const PRM_NODE_LIST * rm_nodes);
static int hb_remove_catalog_info (const HA_CONF * ha_conf,
				   const PRM_NODE_LIST * removed_nodes);

static void hb_cluster_set_node_state (HB_NODE_ENTRY * node,
				       HA_STATE node_state);
static void hb_cluster_set_node_version (HB_NODE_ENTRY * node,
					 const RYE_VERSION * node_version);
static void hb_shm_reset_hb_node (void);
static void shm_master_update_server_state (HB_PROC_ENTRY * proc);

static HB_CLUSTER *hb_Cluster = NULL;
static HB_RESOURCE *hb_Resource = NULL;
static HB_JOB *cluster_JobQ = NULL;
static HB_JOB *resource_JobQ = NULL;

typedef struct hb_job_entry_list HB_JOB_ENTRY_LIST;
struct hb_job_entry_list
{
  HBNEW_JOB_ENTRY *front;
  HBNEW_JOB_ENTRY *back;

  int count;
};

typedef struct hb_job_queue HB_JOB_QUEUE;
struct hb_job_queue
{
  pthread_mutex_t job_lock;
  pthread_cond_t job_cond;
  RQueue job_list;
  int num_run_threads;
  INT64 num_requests;
  bool shutdown;
};

#if 0
static HB_JOB_QUEUE hb_Job_queue = {
  PTHREAD_MUTEX_INITIALIZER,
  PTHREAD_COND_INITIALIZER,
  {{NULL, NULL, 0}},
  0, 0, false
};
#endif

bool hb_Deactivate_immediately = false;

static char hb_Nolog_event_msg[LINE_MAX] = "";

static HB_DEACTIVATE_INFO hb_Deactivate_info = { NULL, 0, false };

static bool hb_Is_activated = true;

struct hb_request
{
  HB_REQ_FUNC req_func;
  const char *name;
};

static struct hb_request hb_Requests[HB_JOB_MAX];

static pthread_mutex_t css_Master_er_log_enable_lock =
  PTHREAD_MUTEX_INITIALIZER;
static bool css_Master_er_log_enabled = true;

#define HA_NODE_INFO_FORMAT_STRING       \
	" HA-Node Info (current %s, state %s)\n"
#define HA_NODE_FORMAT_STRING            \
	"   Node %s (priority %d, state %s)\n"
#define HA_NODE_SCORE_FORMAT_STRING      \
        "    - score %d\n"
#define HA_NODE_HEARTBEAT_GAP_FORMAT_STRING      \
        "    - missed heartbeat %d\n"

#define HA_PROCESS_INFO_FORMAT_STRING    \
	" HA-Process Info (master pid %d, state %s)\n"
#define HA_SERVER_PROCESS_FORMAT_STRING  \
	"   Server %s (pid %d, state %s_and_%s)\n"
#define HA_COPYLOG_PATH_FORMAT_STRING \
        "   Log-path (pid %d, %s)\n"
#define HA_COPYLOG_PROCESS_FORMAT_STRING \
	"       Copylogdb (state %s)\n"
#define HA_APPLYLOG_PROCESS_FORMAT_STRING        \
	"       Applylogdb (state %s)\n"
#define HA_APPLYLOG_FORMAT_STRING        \
        "       Applylogdb (count: %d)\n"
#define HA_ANALYZELOG_PROCESS_FORMAT_STRING        \
        "       Analyzelogdb (state %s)\n"
#define HA_REPLICATION_PROCESS_FORMAT_STRING        \
        "   HA %s (pid %d, log-path %s, state %s)\n"
#define HA_PROCESS_EXEC_PATH_FORMAT_STRING       \
        "        - exec-path [%s] \n"
#define HA_PROCESS_ARGV_FORMAT_STRING            \
        "        - argv      [%s] \n"
#define HA_PROCESS_REGISTER_TIME_FORMAT_STRING     \
        "        - registered-time   %s\n"
#define HA_PROCESS_DEREGISTER_TIME_FORMAT_STRING   \
        "        - deregistered-time %s\n"
#define HA_PROCESS_SHUTDOWN_TIME_FORMAT_STRING     \
        "        - shutdown-time     %s\n"
#define HA_PROCESS_START_TIME_FORMAT_STRING        \
        "        - start-time        %s\n"

#define HA_PING_HOSTS_INFO_FORMAT_STRING       \
        " HA-Ping Host Info (PING check %s)\n"
#define HA_PING_HOSTS_FORMAT_STRING        \
          "   %-20s %s\n"

#define HA_ADMIN_INFO_FORMAT_STRING                \
        " HA-Admin Info\n"
#define HA_ADMIN_INFO_NOLOG_FORMAT_STRING        \
        "  Error Logging: disabled\n"
#define HA_ADMIN_INFO_NOLOG_EVENT_FORMAT_STRING  \
        "    %s\n"
/*
 * linked list
 */
/*
 * hb_list_add() -
 *   return: none
 *
 *   prev(in):
 *   entry(in/out):
 */
static void
hb_list_add (HB_LIST ** p, HB_LIST * n)
{
  n->next = *(p);
  if (n->next)
    {
      n->next->prev = &(n->next);
    }
  n->prev = p;
  *(p) = n;
}

/*
 * hb_list_remove() -
 *   return: none
 *   entry(in):
 */
static void
hb_list_remove (HB_LIST * n)
{
  if (n->prev)
    {
      *(n->prev) = n->next;
      if (*(n->prev))
	{
	  n->next->prev = n->prev;
	}
    }
  n->next = NULL;
  n->prev = NULL;
}

/*
 * hb_list_move() -
 *   return: none
 *   dest_pp(in):
 *   source_pp(in):
 */
static void
hb_list_move (HB_LIST ** dest_pp, HB_LIST ** source_pp)
{
  *dest_pp = *source_pp;
  if (*dest_pp)
    {
      (*dest_pp)->prev = dest_pp;
    }

  *source_pp = NULL;
}

/*
 * job common
 */

/*
 * hb_add_timeval() -
 *
 *   return: none
 *   tv_p(in/out):
 *   msec(in):
 */
static void
hb_add_timeval (struct timeval *tv_p, unsigned int msec)
{
  if (tv_p == NULL)
    {
      return;
    }

  tv_p->tv_sec += (msec / 1000);
  tv_p->tv_usec += ((msec % 1000) * 1000);
}

/*
 * hb_compare_timeval() -
 *   return: (1)  if arg1 > arg2
 *           (0)  if arg1 = arg2
 *           (-1) if arg1 < arg2
 *
 *   arg1(in):
 *   arg2(in):
 */
static int
hb_compare_timeval (struct timeval *arg1, struct timeval *arg2)
{
  if (arg1 == NULL && arg2 == NULL)
    {
      return 0;
    }
  if (arg1 == NULL)
    {
      return -1;
    }
  if (arg2 == NULL)
    {
      return 1;
    }

  if (arg1->tv_sec > arg2->tv_sec)
    {
      return 1;
    }
  else if (arg1->tv_sec == arg2->tv_sec)
    {
      if (arg1->tv_usec > arg2->tv_usec)
	{
	  return 1;
	}
      else if (arg1->tv_usec == arg2->tv_usec)
	{
	  return 0;
	}
      else
	{
	  return -1;
	}
    }
  else
    {
      return -1;
    }
}

/*
 * hb_strtime() -
 *
 *   return: time string
 *   s(in):
 *   max(in):
 *   tv_p(in):
 */
static const char *
hb_strtime (char *s, unsigned int max, struct timeval *tv_p)
{
  if (s == NULL || max < 256 || tv_p == NULL || tv_p->tv_sec == 0)
    {
      goto error_return;
    }

  *s = '\0';

  if (er_datetime (tv_p, s, max) < 0)
    {
      assert (false);
      goto error_return;
    }

  return (const char *) s;

error_return:

  return (const char *) "0000-00-00 00:00:00.000";
}

/*
 * hb_job_queue() - enqueue a job to the queue sorted by expire time
 *   return: NO_ERROR or ER_FAILED
 *
 *   jobs(in):
 *   job_type(in):
 *   arg(in):
 *   msec(in):
 */
static int
hb_job_queue (HB_JOB * jobs, unsigned int job_type, HB_JOB_ARG * arg,
	      unsigned int msec)
{
  HB_JOB_ENTRY **job;
  HB_JOB_ENTRY *new_job;
  struct timeval now;

  new_job = (HB_JOB_ENTRY *) malloc (sizeof (HB_JOB_ENTRY));
  if (new_job == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (HB_JOB_ENTRY));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  gettimeofday (&now, NULL);
  hb_add_timeval (&now, msec);

  new_job->prev = NULL;
  new_job->next = NULL;
  new_job->type = job_type;
  new_job->arg = arg;
  memcpy ((void *) &(new_job->expire), (void *) &now,
	  sizeof (struct timeval));

  pthread_mutex_lock (&jobs->lock);
  for (job = &(jobs->jobs); *job; job = &((*job)->next))
    {
      /*
       * compare expire time of new job and current job
       * until new job's expire is larger than current's
       */
      if (hb_compare_timeval (&((*job)->expire), &now) <= 0)
	{
	  continue;
	}
      break;
    }
  hb_list_add ((HB_LIST **) job, (HB_LIST *) new_job);

  pthread_mutex_unlock (&jobs->lock);

  return NO_ERROR;
}

/*
 * hb_job_dequeue() - dequeue a job from queue expiration time
 *                    is smaller than current time
 *   return: pointer to heartbeat job entry
 *
 *   jobs(in):
 */
static HB_JOB_ENTRY *
hb_job_dequeue (HB_JOB * jobs)
{
  struct timeval now;
  HB_JOB_ENTRY *job;

  gettimeofday (&now, NULL);

  pthread_mutex_lock (&jobs->lock);
  if (jobs->shutdown == true)
    {
      pthread_mutex_unlock (&jobs->lock);
      return NULL;
    }

  job = jobs->jobs;
  if (job == NULL)
    {
      pthread_mutex_unlock (&jobs->lock);
      return NULL;
    }

  if (hb_compare_timeval (&now, &job->expire) >= 0)
    {
      hb_list_remove ((HB_LIST *) job);
    }
  else
    {
      pthread_mutex_unlock (&jobs->lock);
      return NULL;
    }
  pthread_mutex_unlock (&jobs->lock);

  return job;
}

/*
 * hb_job_set_expire_and_reorder - set expiration time of the first job which match job_type
 *                                 reorder job with expiration time changed
 *   return: none
 *
 *   jobs(in):
 *   job_type(in):
 *   msec(in):
 */
static void
hb_job_set_expire_and_reorder (HB_JOB * jobs, unsigned int job_type,
			       unsigned int msec)
{
  HB_JOB_ENTRY **job = NULL;
  HB_JOB_ENTRY *target_job = NULL;
  struct timeval now;

  gettimeofday (&now, NULL);
  hb_add_timeval (&now, msec);

  pthread_mutex_lock (&jobs->lock);

  if (jobs->shutdown == true)
    {
      pthread_mutex_unlock (&jobs->lock);
      return;
    }

  for (job = &(jobs->jobs); *job; job = &((*job)->next))
    {
      if ((*job)->type == job_type)
	{
	  target_job = *job;
	  break;
	}
    }

  if (target_job == NULL)
    {
      pthread_mutex_unlock (&jobs->lock);
      return;
    }

  memcpy ((void *) &(target_job->expire), (void *) &now,
	  sizeof (struct timeval));

  /*
   * so now we change target job's turn to adjust sorted queue
   */
  hb_list_remove ((HB_LIST *) target_job);

  for (job = &(jobs->jobs); *job; job = &((*job)->next))
    {
      /*
       * compare expiration time of target job and current job
       * until target job's expire is larger than current's
       */
      if (hb_compare_timeval (&((*job)->expire), &(target_job->expire)) > 0)
	{
	  break;
	}
    }

  hb_list_add ((HB_LIST **) job, (HB_LIST *) target_job);

  pthread_mutex_unlock (&jobs->lock);

  return;
}

/*
 * hb_job_shutdown() - clear job queue and stop job worker thread
 *   return: none
 *
 *   jobs(in):
 */
static void
hb_job_shutdown (HB_JOB * jobs)
{
  HB_JOB_ENTRY *job, *job_next;

  pthread_mutex_lock (&jobs->lock);
  for (job = jobs->jobs; job; job = job_next)
    {
      job_next = job->next;

      hb_list_remove ((HB_LIST *) job);
      free_and_init (job);
    }
  jobs->shutdown = true;
  pthread_mutex_unlock (&jobs->lock);
}


/*
 *cluster node job actions
 */

/*
 * hb_cluster_job_init() -
 *   return: none
 *
 *   arg(in):
 */
static void
hb_cluster_job_init (HB_JOB_ARG * arg)
{
  int error;

  error = hb_cluster_job_queue (HB_CJOB_HEARTBEAT, NULL,
				HB_JOB_TIMER_IMMEDIATELY);
  assert (error == NO_ERROR);

  error = hb_cluster_job_queue (HB_CJOB_CHECK_VALID_PING_SERVER, NULL,
				HB_JOB_TIMER_IMMEDIATELY);
  assert (error == NO_ERROR);

  error = hb_cluster_job_queue (HB_CJOB_CALC_SCORE, NULL,
				prm_get_bigint_value (PRM_ID_HA_INIT_TIMER));
  assert (error == NO_ERROR);

  if (arg)
    {
      free_and_init (arg);
    }
}

/*
 * hb_cluster_job_heartbeat() - send heartbeat request to other nodes
 *   return: none
 *
 *   jobs(in):
 */
static void
hb_cluster_job_heartbeat (HB_JOB_ARG * arg)
{
  int error;

  pthread_mutex_lock (&hb_Cluster->lock);

  if (hb_Cluster->hide_to_demote == false)
    {
      hb_cluster_request_heartbeat_to_all ();
    }

  pthread_mutex_unlock (&hb_Cluster->lock);
  error = hb_cluster_job_queue (HB_CJOB_HEARTBEAT, NULL,
				prm_get_bigint_value
				(PRM_ID_HA_HEARTBEAT_INTERVAL));
  assert (error == NO_ERROR);

  if (arg)
    {
      free_and_init (arg);
    }
  return;
}

/*
 * hb_cluster_is_isolated() -
 *   return: whether current node is isolated or not
 *
 */
static bool
hb_cluster_is_isolated (void)
{
  HB_NODE_ENTRY *node;
  for (node = hb_Cluster->nodes; node; node = node->next)
    {
      if (node->node_state == HA_STATE_REPLICA)
	{
	  continue;
	}

      if (hb_Cluster->myself != node && node->node_state != HA_STATE_UNKNOWN)
	{
	  return false;
	}
    }
  return true;
}

/*
 * hb_cluster_is_received_heartbeat_from_all() -
 *   return: whether current node received heartbeat from all node
 */
static bool
hb_cluster_is_received_heartbeat_from_all (void)
{
  HB_NODE_ENTRY *node;
  struct timeval now;
  unsigned int heartbeat_confirm_time;

  heartbeat_confirm_time =
    prm_get_bigint_value (PRM_ID_HA_HEARTBEAT_INTERVAL);

  gettimeofday (&now, NULL);

  for (node = hb_Cluster->nodes; node; node = node->next)
    {
      if (hb_Cluster->myself != node
	  && HB_GET_ELAPSED_TIME (now,
				  node->last_recv_hbtime) >
	  heartbeat_confirm_time)
	{
	  return false;
	}
    }
  return true;
}

/*
 *
 */
static HB_JOB_ARG *
hb_alloc_cluster_job_arg (void)
{
  HB_JOB_ARG *job_arg;

  job_arg = (HB_JOB_ARG *) malloc (sizeof (HB_JOB_ARG));
  if (job_arg == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, sizeof (HB_JOB_ARG));
      return NULL;
    }

  job_arg->cluster_job_arg.ping_check_count = 0;
  job_arg->cluster_job_arg.retries = 0;

  return job_arg;
}

/*
 *
 */
static HB_JOB_ARG *
hb_alloc_resource_job_arg (int pid, char *args, int max_retries)
{
  HB_JOB_ARG *job_arg;
  job_arg = (HB_JOB_ARG *) malloc (sizeof (HB_JOB_ARG));
  if (job_arg == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, sizeof (HB_JOB_ARG));
      return NULL;
    }
  job_arg->resource_job_arg.pid = pid;

  job_arg->resource_job_arg.args[0] = '\0';
  if (args != NULL)
    {
      memcpy ((void *) job_arg->resource_job_arg.args, args,
	      sizeof (job_arg->resource_job_arg.args));
    }
  job_arg->resource_job_arg.max_retries = max_retries;

  job_arg->resource_job_arg.retries = 0;

  return job_arg;
}

/*
 * hb_cluster_job_calc_score() -
 *   return: none
 *
 *   jobs(in):
 */
static void
hb_cluster_job_calc_score (HB_JOB_ARG * arg)
{
  int error;
  int num_master;
  unsigned int failover_wait_time;
  HB_JOB_ARG *job_arg;
  char hb_info_str[HB_INFO_STR_MAX];

  if (arg != NULL)
    {
      assert (false);
      free_and_init (arg);
    }

  hb_info_str[0] = '\0';	/* init */

  pthread_mutex_lock (&hb_Cluster->lock);

  num_master = hb_cluster_calc_score ();
  hb_Cluster->is_isolated = hb_cluster_is_isolated ();

  if (hb_Cluster->node_state == HA_STATE_REPLICA
      || hb_Cluster->hide_to_demote == true)
    {
      goto calc_end;
    }

  /* case : check whether master has been isolated */
  if (hb_Cluster->node_state == HA_STATE_MASTER)
    {
      if (hb_Cluster->is_isolated == true)
	{
	  /*check ping if Ping host exist */
	  pthread_mutex_unlock (&hb_Cluster->lock);

	  job_arg = hb_alloc_cluster_job_arg ();
	  if (job_arg)
	    {
	      error = hb_cluster_job_queue (HB_CJOB_CHECK_PING, job_arg,
					    HB_JOB_TIMER_IMMEDIATELY);
	      assert (error == NO_ERROR);
	    }

	  return;
	}
    }

  /* case : split-brain */
  if ((num_master > 1)
      && (hb_Cluster->master && hb_Cluster->myself
	  && hb_Cluster->myself->node_state == HA_STATE_MASTER
	  && hb_Cluster->master->priority != hb_Cluster->myself->priority))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HB_NODE_EVENT, 1,
	      "More than one master detected and failback will be initiated");

      hb_help_sprint_nodes_info (hb_info_str, HB_INFO_STR_MAX);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HB_NODE_EVENT, 1,
	      hb_info_str);

      if (hb_Cluster->num_ping_hosts > 0)
	{
	  hb_help_sprint_ping_host_info (hb_info_str, HB_INFO_STR_MAX);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HB_NODE_EVENT,
		  1, hb_info_str);
	}

      pthread_mutex_unlock (&hb_Cluster->lock);

      error = hb_cluster_job_queue (HB_CJOB_FAILBACK, NULL,
				    HB_JOB_TIMER_IMMEDIATELY);
      assert (error == NO_ERROR);

      return;
    }

  /* case : failover */
  if ((hb_Cluster->node_state == HA_STATE_SLAVE)
      && (hb_Cluster->master && hb_Cluster->myself
	  && hb_Cluster->master->priority == hb_Cluster->myself->priority))
    {
      hb_Cluster->node_state = HA_STATE_TO_BE_MASTER;
      hb_cluster_request_heartbeat_to_all ();

      pthread_mutex_unlock (&hb_Cluster->lock);

      job_arg = hb_alloc_cluster_job_arg ();
      if (job_arg != NULL)
	{
	  error = hb_cluster_job_queue (HB_CJOB_CHECK_PING, job_arg,
					HB_JOB_TIMER_WAIT_100_MILLISECOND);
	  assert (error == NO_ERROR);
	}
      else
	{
	  THREAD_SLEEP (HB_JOB_TIMER_WAIT_100_MILLISECOND);

	  if (hb_cluster_is_received_heartbeat_from_all () == true)
	    {
	      failover_wait_time = HB_JOB_TIMER_WAIT_500_MILLISECOND;
	    }
	  else
	    {
	      /* If current node didn't receive heartbeat from some node, wait for some time */
	      failover_wait_time =
		prm_get_bigint_value (PRM_ID_HA_FAILOVER_WAIT_TIME);
	    }

	  error =
	    hb_cluster_job_queue (HB_CJOB_FAILOVER, NULL, failover_wait_time);
	  assert (error == NO_ERROR);

	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HB_NODE_EVENT,
		  1,
		  "A failover attempted to make the current node a master");
	}

      return;
    }

  /* case : change slave */
  if (hb_Cluster->node_state == HA_STATE_TO_BE_SLAVE && num_master == 1)
    {
      hb_Cluster->node_state = HA_STATE_SLAVE;
      hb_cluster_set_node_state (hb_Cluster->myself, HA_STATE_SLAVE);

      hb_cluster_request_heartbeat_to_all ();

      pthread_mutex_unlock (&hb_Cluster->lock);

      pthread_mutex_lock (&hb_Resource->lock);
      hb_Resource->node_state = HA_STATE_SLAVE;
      pthread_mutex_unlock (&hb_Resource->lock);

      error = hb_cluster_job_queue (HB_CJOB_CALC_SCORE, NULL,
				    prm_get_bigint_value
				    (PRM_ID_HA_CALC_SCORE_INTERVAL));

      return;
    }

  hb_help_sprint_nodes_info (hb_info_str, HB_INFO_STR_MAX);
  er_log_debug (ARG_FILE_LINE, "%s", hb_info_str);

calc_end:
  pthread_mutex_unlock (&hb_Cluster->lock);

  error = hb_cluster_job_queue (HB_CJOB_CALC_SCORE, NULL,
				prm_get_bigint_value
				(PRM_ID_HA_CALC_SCORE_INTERVAL));
  assert (error == NO_ERROR);

  return;
}

/*
 * hb_cluster_job_check_ping() -
 *   return: none
 *
 *   jobs(in):
 */
static void
hb_cluster_job_check_ping (HB_JOB_ARG * arg)
{
  int error;
  int ping_try_count = 0;
  bool ping_success = false;
  int ping_result;
  unsigned int failover_wait_time;
  HB_CLUSTER_JOB_ARG *clst_arg = (arg) ? &(arg->cluster_job_arg) : NULL;
  HB_PING_HOST_ENTRY *ping_host;

  pthread_mutex_lock (&hb_Cluster->lock);

  if (clst_arg == NULL || hb_Cluster->num_ping_hosts == 0
      || hb_Cluster->is_ping_check_enabled == false)
    {
      /* If Ping Host is either empty or marked invalid, MASTER->MASTER, SLAVE->MASTER.
       * It may cause split-brain problem.
       */
      if (hb_Cluster->node_state == HA_STATE_MASTER)
	{
	  goto ping_check_cancel;
	}
    }
  else
    {
      for (ping_host = hb_Cluster->ping_hosts; ping_host;
	   ping_host = ping_host->next)
	{
	  ping_result = hb_check_ping (ping_host->host_name);

	  ping_host->ping_result = ping_result;
	  if (ping_result == HB_PING_SUCCESS)
	    {
	      ping_try_count++;
	      ping_success = true;
	      break;
	    }
	  else if (ping_result == HB_PING_FAILURE)
	    {
	      ping_try_count++;
	    }
	}

      if (hb_Cluster->node_state == HA_STATE_MASTER)
	{
	  if (ping_try_count == 0 || ping_success == true)
	    {
	      goto ping_check_cancel;
	    }
	}
      else
	{
	  if (ping_try_count > 0 && ping_success == false)
	    {
	      goto ping_check_cancel;
	    }
	}

      if ((++clst_arg->ping_check_count) < HB_MAX_PING_CHECK)
	{
	  /* Try ping test again */
	  pthread_mutex_unlock (&hb_Cluster->lock);

	  error =
	    hb_cluster_job_queue (HB_CJOB_CHECK_PING, arg,
				  HB_JOB_TIMER_IMMEDIATELY);
	  assert (error == NO_ERROR);

	  return;
	}
    }

  /* Now, we have tried ping test over HB_MAX_PING_CHECK times.
   * (or Slave's ping host is either empty or invalid.)
   * So, we can determine this node's next job (failover or failback).
   */

  hb_cluster_request_heartbeat_to_all ();

  pthread_mutex_unlock (&hb_Cluster->lock);

  if (hb_Cluster->node_state == HA_STATE_MASTER)
    {
      /* If this node is Master, do failback */
      error =
	hb_cluster_job_queue (HB_CJOB_FAILBACK, NULL,
			      HB_JOB_TIMER_IMMEDIATELY);
      assert (error == NO_ERROR);
    }
  else
    {
      /* If this node is Slave, do failover */
      if (hb_cluster_is_received_heartbeat_from_all () == true)
	{
	  failover_wait_time = HB_JOB_TIMER_WAIT_500_MILLISECOND;
	}
      else
	{
	  /* If current node didn't receive heartbeat from some node, wait for some time */
	  failover_wait_time =
	    prm_get_bigint_value (PRM_ID_HA_FAILOVER_WAIT_TIME);
	}
      error =
	hb_cluster_job_queue (HB_CJOB_FAILOVER, NULL, failover_wait_time);
      assert (error == NO_ERROR);
    }

  if (arg)
    {
      free_and_init (arg);
    }

  return;

ping_check_cancel:
/* if this node is a master, then failback is cancelled */

  if (hb_Cluster->node_state != HA_STATE_MASTER)
    {
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
	      ER_HB_NODE_EVENT, 1, "Failover cancelled by ping check");
      hb_Cluster->node_state = HA_STATE_SLAVE;
    }
  hb_cluster_request_heartbeat_to_all ();

  pthread_mutex_unlock (&hb_Cluster->lock);

  /* do calc_score job again */
  error =
    hb_cluster_job_queue (HB_CJOB_CALC_SCORE, NULL,
			  prm_get_bigint_value
			  (PRM_ID_HA_CALC_SCORE_INTERVAL));
  assert (error == NO_ERROR);

  if (arg)
    {
      free_and_init (arg);
    }

  return;
}


/*
 * hb_cluster_job_failover() -
 *   return: none
 *
 *   jobs(in):
 */
static void
hb_cluster_job_failover (HB_JOB_ARG * arg)
{
  int error = NO_ERROR;
  UNUSED_VAR int num_master;
  char hb_info_str[HB_INFO_STR_MAX];

  hb_info_str[0] = '\0';	/* init */

  pthread_mutex_lock (&hb_Cluster->lock);

  num_master = hb_cluster_calc_score ();

  if (hb_Cluster->master != NULL && hb_Cluster->myself != NULL
      && hb_Cluster->master->priority == hb_Cluster->myself->priority)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HB_NODE_EVENT, 1,
	      "Failover completed");
      hb_Cluster->node_state = HA_STATE_MASTER;
      hb_Resource->node_state = HA_STATE_MASTER;

      error =
	hb_resource_job_set_expire_and_reorder (HB_RJOB_SYNC_SERVER_STATE,
						HB_JOB_TIMER_IMMEDIATELY);
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HB_NODE_EVENT, 1,
	      "Failover cancelled");
      hb_Cluster->node_state = HA_STATE_SLAVE;
    }

  hb_help_sprint_nodes_info (hb_info_str, HB_INFO_STR_MAX);
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HB_NODE_EVENT, 1, hb_info_str);

  if (hb_Cluster->num_ping_hosts > 0)
    {
      hb_help_sprint_ping_host_info (hb_info_str, HB_INFO_STR_MAX);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HB_NODE_EVENT, 1,
	      hb_info_str);
    }

  hb_cluster_request_heartbeat_to_all ();
  pthread_mutex_unlock (&hb_Cluster->lock);

  error = hb_cluster_job_queue (HB_CJOB_CALC_SCORE, NULL,
				prm_get_bigint_value
				(PRM_ID_HA_CALC_SCORE_INTERVAL));
  assert (error == NO_ERROR);

  if (arg)
    {
      free_and_init (arg);
    }
  return;
}

/*
 * hb_cluster_job_demote() -
 *      it waits for new master to be elected.
 *      hb_resource_job_demote_start_shutdown must be proceeded
 *      before this job.
 *   return: none
 *
 *   arg(in):
 */
static void
hb_cluster_job_demote (HB_JOB_ARG * arg)
{
  int error;
  HB_NODE_ENTRY *node;
  HB_CLUSTER_JOB_ARG *clst_arg = (arg) ? &(arg->cluster_job_arg) : NULL;
  char hb_info_str[HB_INFO_STR_MAX];

  if (arg == NULL || clst_arg == NULL)
    {
      er_log_debug (ARG_FILE_LINE,
		    "invalid arg or proc_arg. "
		    "(arg:%p, proc_arg:%p). \n", arg, clst_arg);
      return;
    }

  hb_info_str[0] = '\0';	/* init */

  pthread_mutex_lock (&hb_Cluster->lock);

  if (clst_arg->retries == 0)
    {
      assert (hb_Cluster->node_state == HA_STATE_MASTER);
      assert (hb_Resource->node_state == HA_STATE_SLAVE);

      /* send state (HA_STATE_UNKNOWN) to other nodes for making other node be master */
      hb_Cluster->node_state = HA_STATE_UNKNOWN;
      hb_cluster_request_heartbeat_to_all ();

      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HB_NODE_EVENT, 1,
	      "Waiting for a new node to be elected as master");
    }

  hb_Cluster->hide_to_demote = true;
  hb_Cluster->node_state = HA_STATE_SLAVE;
  hb_cluster_set_node_state (hb_Cluster->myself, HA_STATE_SLAVE);

  if (hb_Cluster->is_isolated == true
      || ++(clst_arg->retries) > HB_MAX_WAIT_FOR_NEW_MASTER)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HB_NODE_EVENT, 1,
	      "Failed to find a new master node and it changes "
	      "its role back to master again");
      hb_Cluster->hide_to_demote = false;

      pthread_mutex_unlock (&hb_Cluster->lock);

      if (arg)
	{
	  free_and_init (arg);
	}
      return;
    }

  for (node = hb_Cluster->nodes; node; node = node->next)
    {
      if (node->node_state == HA_STATE_MASTER)
	{
	  assert (node != hb_Cluster->myself);

	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HB_NODE_EVENT,
		  1, "Found a new master node");

	  hb_help_sprint_nodes_info (hb_info_str, HB_INFO_STR_MAX);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HB_NODE_EVENT,
		  1, hb_info_str);

	  if (hb_Cluster->num_ping_hosts > 0)
	    {
	      hb_help_sprint_ping_host_info (hb_info_str, HB_INFO_STR_MAX);
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_HB_NODE_EVENT, 1, hb_info_str);
	    }

	  hb_Cluster->hide_to_demote = false;

	  pthread_mutex_unlock (&hb_Cluster->lock);

	  if (arg)
	    {
	      free_and_init (arg);
	    }
	  return;
	}
    }

  pthread_mutex_unlock (&hb_Cluster->lock);

  error =
    hb_cluster_job_queue (HB_CJOB_DEMOTE, arg, HB_JOB_TIMER_WAIT_A_SECOND);

  if (error != NO_ERROR)
    {
      assert (false);
      free_and_init (arg);
    }
  return;
}

/*
 * hb_cluster_job_failback () -
 *   return: none
 *
 *   jobs(in):
 *
 *   NOTE: this job waits for servers to be killed.
 *   Therefore, be aware that adding this job to queue might
 *   temporarily prevent cluster_job_calc or any other cluster
 *   jobs following this one from executing at regular intervals
 *   as intended.
 */
static void
hb_cluster_job_failback (HB_JOB_ARG * arg)
{
  int error, count = 0;
  char hb_info_str[HB_INFO_STR_MAX];
  HB_PROC_ENTRY *proc;
  pid_t *pids = NULL, *tmp_pids = NULL;
  size_t size;
  bool emergency_kill_enabled = false;

  hb_info_str[0] = '\0';	/* init */

  pthread_mutex_lock (&hb_Cluster->lock);

  hb_Cluster->node_state = HA_STATE_SLAVE;
  hb_cluster_set_node_state (hb_Cluster->myself, HA_STATE_SLAVE);

  hb_cluster_request_heartbeat_to_all ();

  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HB_NODE_EVENT, 1,
	  "This master will become a slave and rye_server will be restarted");

  hb_help_sprint_nodes_info (hb_info_str, HB_INFO_STR_MAX);
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HB_NODE_EVENT, 1, hb_info_str);

  if (hb_Cluster->num_ping_hosts > 0)
    {
      hb_help_sprint_ping_host_info (hb_info_str, HB_INFO_STR_MAX);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HB_NODE_EVENT, 1,
	      hb_info_str);
    }

  pthread_mutex_unlock (&hb_Cluster->lock);

  pthread_mutex_lock (&hb_Resource->lock);
  hb_Resource->node_state = HA_STATE_SLAVE;

  proc = hb_Resource->procs;
  while (proc)
    {
      if (proc->type != HB_PTYPE_SERVER)
	{
	  proc = proc->next;
	  continue;
	}

      if (emergency_kill_enabled == false)
	{
	  size = sizeof (pid_t) * (count + 1);
	  tmp_pids = (pid_t *) realloc (pids, size);
	  if (tmp_pids == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, size);

	      /*
	       * in case that memory allocation fails,
	       * kill all rye_server processes with SIGKILL
	       */
	      emergency_kill_enabled = true;
	      proc = hb_Resource->procs;
	      continue;
	    }
	  pids = tmp_pids;
	  pids[count++] = proc->pid;
	}
      else
	{
	  assert (proc->pid > 0);
	  if (proc->pid > 0)
	    {
	      kill (proc->pid, SIGKILL);
	    }
	}
      proc = proc->next;
    }

  pthread_mutex_unlock (&hb_Resource->lock);

  if (emergency_kill_enabled == false)
    {
      hb_kill_process (pids, count);
    }

  if (pids)
    {
      free_and_init (pids);
    }

  error = hb_cluster_job_queue (HB_CJOB_CALC_SCORE, NULL,
				prm_get_bigint_value
				(PRM_ID_HA_CALC_SCORE_INTERVAL));
  assert (error == NO_ERROR);

  if (arg)
    {
      free_and_init (arg);
    }
  return;
}

/*
 * hb_cluster_job_changeslave () -
 *   return: none
 *
 *   jobs(in):
 *
 */
static void
hb_cluster_job_changeslave (HB_JOB_ARG * arg)
{
  int error;
  char hb_info_str[HB_INFO_STR_MAX];

  hb_info_str[0] = '\0';	/* init */

  pthread_mutex_lock (&hb_Cluster->lock);

  hb_Cluster->node_state = HA_STATE_TO_BE_SLAVE;
  hb_cluster_set_node_state (hb_Cluster->myself, HA_STATE_TO_BE_SLAVE);

  hb_cluster_request_heartbeat_to_all ();

  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HB_NODE_EVENT, 1,
	  "This master will become a slave and rye_server will be restarted");

  hb_help_sprint_nodes_info (hb_info_str, HB_INFO_STR_MAX);
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HB_NODE_EVENT, 1, hb_info_str);

  if (hb_Cluster->num_ping_hosts > 0)
    {
      hb_help_sprint_ping_host_info (hb_info_str, HB_INFO_STR_MAX);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HB_NODE_EVENT, 1,
	      hb_info_str);
    }

  pthread_mutex_unlock (&hb_Cluster->lock);

  pthread_mutex_lock (&hb_Resource->lock);
  hb_Resource->node_state = HA_STATE_TO_BE_SLAVE;
  pthread_mutex_unlock (&hb_Resource->lock);

  error = hb_cluster_job_queue (HB_CJOB_CALC_SCORE, NULL,
				prm_get_bigint_value
				(PRM_ID_HA_CALC_SCORE_INTERVAL));
  assert (error == NO_ERROR);

  if (arg)
    {
      free_and_init (arg);
    }

  return;
}

/*
 * hb_cluster_job_changemode_force () -
 *   return: none
 *
 *   jobs(in):
 *
 */
static void
hb_cluster_job_changemode_force (HB_JOB_ARG * arg)
{
  int error;
  char hb_info_str[HB_INFO_STR_MAX];
  HB_PROC_ENTRY *proc;
  bool change_server_state = false;

  hb_info_str[0] = '\0';	/* init */

  pthread_mutex_lock (&css_Master_socket_anchor_lock);
  pthread_mutex_lock (&hb_Resource->lock);

#if !defined(NDEBUG)
  if (hb_Resource->procs)
    {
      hb_help_sprint_processes_info (hb_info_str, HB_INFO_STR_MAX);
      er_log_debug (ARG_FILE_LINE, "%s", hb_info_str);

      hb_help_sprint_nodes_info (hb_info_str, HB_INFO_STR_MAX);
      er_log_debug (ARG_FILE_LINE, "%s", hb_info_str);
    }
#endif

  for (proc = hb_Resource->procs; proc; proc = proc->next)
    {
      if (proc->type != HB_PTYPE_SERVER
	  || proc->state != HB_PSTATE_REGISTERED)
	{
	  continue;
	}

      if (hb_Resource->node_state == HA_STATE_TO_BE_SLAVE
	  || hb_Resource->node_state == HA_STATE_TO_BE_MASTER
	  || hb_Resource->node_state != proc->server_state)
	{
	  if ((hb_Resource->node_state == HA_STATE_SLAVE
	       && proc->server_state == HA_STATE_TO_BE_SLAVE)
	      || (hb_Resource->node_state == HA_STATE_MASTER
		  && proc->server_state == HA_STATE_TO_BE_MASTER))
	    {
	      er_log_debug (ARG_FILE_LINE,
			    "send changemode force request. "
			    "(node_state:%s, pid:%d, proc_state:%s, server_state:%s). \n",
			    HA_STATE_NAME (hb_Resource->node_state),
			    proc->pid, hb_process_state_string (proc->state),
			    HA_STATE_NAME (proc->server_state));

	      error = hb_resource_sync_server_state (proc, true);
	      if (NO_ERROR != error)
		{
		  /* TODO : if error */
		}
	    }

	  change_server_state = true;
	}
    }

#if !defined(NDEBUG)
  if (hb_Resource->procs)
    {
      hb_help_sprint_processes_info (hb_info_str, HB_INFO_STR_MAX);
      er_log_debug (ARG_FILE_LINE, "%s", hb_info_str);

      hb_help_sprint_nodes_info (hb_info_str, HB_INFO_STR_MAX);
      er_log_debug (ARG_FILE_LINE, "%s", hb_info_str);
    }
#endif

  pthread_mutex_unlock (&hb_Resource->lock);
  pthread_mutex_unlock (&css_Master_socket_anchor_lock);

  if (change_server_state)
    {
      error = hb_cluster_job_queue (HB_CJOB_CHANGEMODE_FORCE, NULL,
				    HB_JOB_TIMER_WAIT_A_SECOND);
    }

  if (arg)
    {
      free_and_init (arg);
    }

  return;
}

/*
 * hb_cluster_check_valid_ping_server() -
 *   return: whether a valid ping host exists or not
 *
 * NOTE: it returns true when no ping host is specified.
 */
static bool
hb_cluster_check_valid_ping_server (void)
{
  HB_PING_HOST_ENTRY *ping_host;
  bool valid_ping_host_exists = false;

  if (hb_Cluster->num_ping_hosts == 0)
    {
      return true;
    }

  for (ping_host = hb_Cluster->ping_hosts; ping_host;
       ping_host = ping_host->next)
    {
      ping_host->ping_result = hb_check_ping (ping_host->host_name);

      if (ping_host->ping_result == HB_PING_SUCCESS)
	{
	  valid_ping_host_exists = true;
	}
    }

  return valid_ping_host_exists;
}

/*
 * hb_cluster_job_check_valid_ping_server() -
 *   return: none
 *
 *   jobs(in):
 */
static void
hb_cluster_job_check_valid_ping_server (UNUSED_ARG HB_JOB_ARG * arg)
{
  int error;
  bool valid_ping_host_exists;
  char buf[LINE_MAX];
  int check_interval = HB_DEFAULT_CHECK_VALID_PING_SERVER_INTERVAL;

  pthread_mutex_lock (&hb_Cluster->lock);

  if (hb_Cluster->num_ping_hosts == 0)
    {
      goto check_end;
    }

  valid_ping_host_exists = hb_cluster_check_valid_ping_server ();
  if (valid_ping_host_exists == false && hb_cluster_is_isolated () == false)
    {
      check_interval = HB_TEMP_CHECK_VALID_PING_SERVER_INTERVAL;

      if (hb_Cluster->is_ping_check_enabled == true)
	{
	  hb_Cluster->is_ping_check_enabled = false;
	  snprintf (buf, LINE_MAX,
		    "Validity check for PING failed on all hosts "
		    "and PING check is now temporarily disabled.");
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HB_NODE_EVENT, 1, buf);
	}
    }
  else if (valid_ping_host_exists == true)
    {
      if (hb_Cluster->is_ping_check_enabled == false)
	{
	  hb_Cluster->is_ping_check_enabled = true;
	  snprintf (buf, LINE_MAX, "Validity check for PING succeeded "
		    "and PING check is now enabled.");
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HB_NODE_EVENT, 1, buf);
	}
    }

check_end:
  pthread_mutex_unlock (&hb_Cluster->lock);

  error =
    hb_cluster_job_queue (HB_CJOB_CHECK_VALID_PING_SERVER, NULL,
			  check_interval);

  assert (error == NO_ERROR);

  return;
}

/*
 * cluster common
 */

/*
 * hb_cluster_calc_score() -
 *   return: number of master nodes in heartbeat cluster
 */
static int
hb_cluster_calc_score (void)
{
  int num_master = 0;
  short min_score = HB_NODE_SCORE_UNKNOWN;
  HB_NODE_ENTRY *node;
  struct timeval now;

  if (hb_Cluster == NULL)
    {
      er_log_debug (ARG_FILE_LINE, "hb_Cluster is null. \n");
      return ER_FAILED;
    }

  hb_cluster_set_node_state (hb_Cluster->myself, hb_Cluster->node_state);
  gettimeofday (&now, NULL);

  for (node = hb_Cluster->nodes; node; node = node->next)
    {
      /* If this node does not receive heartbeat message over
       * than prm_get_integer_value (PRM_ID_HA_MAX_HEARTBEAT_GAP) times,
       * (or sufficient time has been elapsed from
       * the last received heartbeat message time),
       * this node does not know what other node state is.
       */
      if (node->heartbeat_gap >
	  prm_get_integer_value (PRM_ID_HA_MAX_HEARTBEAT_GAP)
	  || (!HB_IS_INITIALIZED_TIME (node->last_recv_hbtime)
	      && HB_GET_ELAPSED_TIME (now,
				      node->last_recv_hbtime) >
	      prm_get_bigint_value (PRM_ID_HA_CALC_SCORE_INTERVAL)))
	{
	  node->heartbeat_gap = 0;
	  node->last_recv_hbtime.tv_sec = 0;
	  node->last_recv_hbtime.tv_usec = 0;
	  hb_cluster_set_node_state (node, HA_STATE_UNKNOWN);
	}

      switch (node->node_state)
	{
	case HA_STATE_MASTER:
	  {
	    node->score = node->priority | HB_NODE_SCORE_MASTER;
	  }
	  break;
	case HA_STATE_TO_BE_MASTER:
	  {
	    node->score = node->priority | HB_NODE_SCORE_TO_BE_MASTER;
	  }
	  break;
	case HA_STATE_SLAVE:
	  {
	    node->score = node->priority | HB_NODE_SCORE_SLAVE;
	  }
	  break;
	case HA_STATE_TO_BE_SLAVE:
	  {
	    node->score = node->priority | HB_NODE_SCORE_TO_BE_SLAVE;
	  }
	  break;
	case HA_STATE_REPLICA:
	case HA_STATE_UNKNOWN:
	default:
	  {
	    node->score = node->priority | HB_NODE_SCORE_UNKNOWN;
	  }
	  break;
	}

      if (node->score < min_score)
	{
	  hb_Cluster->master = node;
	  min_score = node->score;
	}

      if (node->score < (short) HB_NODE_SCORE_TO_BE_MASTER)
	{
	  num_master++;
	}
    }

  return num_master;
}

/*
 * hb_cluster_request_heartbeat_to_all() -
 *   return: none
 *
 */
static void
hb_cluster_request_heartbeat_to_all (void)
{
  HB_NODE_ENTRY *node;

  if (hb_Cluster == NULL)
    {
      er_log_debug (ARG_FILE_LINE, "hb_Cluster is null. \n");
      return;
    }

  for (node = hb_Cluster->nodes; node; node = node->next)
    {
      if (prm_is_same_node (&hb_Cluster->my_node_info,
			    &node->node_info) == true)
	{
	  continue;
	}

      hb_cluster_send_heartbeat (true, &node->node_info);
      node->heartbeat_gap++;
    }

  return;
}

/*
 * hb_cluster_send_heartbeat() -
 *   return: none
 */
static void
hb_cluster_send_heartbeat (bool is_req, const PRM_NODE_INFO * node)
{
  HBP_HEADER *hbp_header;
  char buffer[HB_BUFFER_SZ + MAX_ALIGNMENT], *p;
  char *aligned_buffer;
  size_t packet_len;
  int msg_len;
  int send_len;
  RYE_VERSION my_version = rel_cur_version ();

  struct sockaddr_in saddr;
  socklen_t saddr_len;

  /* construct destination address */
  memset ((void *) &saddr, 0, sizeof (saddr));
  if (hb_sockaddr (node, (struct sockaddr *) &saddr, &saddr_len) != NO_ERROR)
    {
      er_log_debug (ARG_FILE_LINE, "hb_sockaddr failed. \n");
      return;
    }


  memset ((void *) buffer, 0, sizeof (buffer));
  aligned_buffer = PTR_ALIGN (buffer, MAX_ALIGNMENT);

  hbp_header = (HBP_HEADER *) aligned_buffer;

  msg_len = OR_INT_SIZE + OR_VERSION_SIZE;

  hb_set_net_header (hbp_header, HBP_CLUSTER_HEARTBEAT, is_req,
		     msg_len, 0, node);

  p = (char *) (hbp_header + 1);
  p = or_pack_int (p, hb_Cluster->node_state);
  p = or_pack_version (p, &my_version);

  packet_len = sizeof (HBP_HEADER) + msg_len;

  send_len = sendto (hb_Cluster->sfd, (void *) aligned_buffer, packet_len, 0,
		     (struct sockaddr *) &saddr, saddr_len);
  if (send_len <= 0)
    {
      er_log_debug (ARG_FILE_LINE, "sendto failed. \n");
    }

  return;
}


/*
 * hb_cluster_receive_heartbeat() -
 *   return: none
 *
 *   buffer(in):
 *   len(in):
 *   from(in):
 *   from_len(in):
 */
static void
hb_cluster_receive_heartbeat (char *buffer, int len,
			      UNUSED_ARG struct sockaddr_in *from,
			      UNUSED_ARG socklen_t from_len)
{
  HBP_HEADER *hbp_header;
  HB_NODE_ENTRY *node;
  char *p;

  int node_state;
  bool is_state_changed = false;

  hbp_header = (HBP_HEADER *) (buffer);

  pthread_mutex_lock (&hb_Cluster->lock);
  if (hb_Cluster->shutdown)
    {
      pthread_mutex_unlock (&hb_Cluster->lock);
      return;
    }

  /* validate receive message */
  if (prm_is_same_node (&hb_Cluster->my_node_info,
			&hbp_header->dest_host) == false)
    {
      char dest_host[MAX_NODE_INFO_STR_LEN];
      char my_host[MAX_NODE_INFO_STR_LEN];
      prm_node_info_to_str (dest_host, sizeof (dest_host),
			    &hbp_header->dest_host);
      prm_node_info_to_str (my_host, sizeof (my_host),
			    &hb_Cluster->my_node_info);

      er_log_debug (ARG_FILE_LINE, "hostname mismatch. "
		    "(host_name:{%s}, dest_host_name:{%s}).\n",
		    my_host, dest_host);
      pthread_mutex_unlock (&hb_Cluster->lock);
      return;
    }

  if (len != (int) (sizeof (*hbp_header) + hbp_header->len))
    {
      er_log_debug (ARG_FILE_LINE, "size mismatch. "
		    "(len:%d, msg_size:%d).\n", len,
		    (sizeof (*hbp_header) + hbp_header->len));
      pthread_mutex_unlock (&hb_Cluster->lock);
      return;
    }

  switch (hbp_header->type)
    {
    case HBP_CLUSTER_HEARTBEAT:
      {
	RYE_VERSION node_version;
	RYE_VERSION my_version = rel_cur_version ();

	p = (char *) (hbp_header + 1);
	p = or_unpack_int (p, &node_state);
	p = or_unpack_version (p, &node_version);

	if (node_state < 0 || node_state >= HA_STATE_MAX)
	  {
	    er_log_debug (ARG_FILE_LINE,
			  "receive heartbeat have unknown node state. "
			  "(state:%u).\n", node_state);
	    break;
	  }

	/*
	 * if heartbeat group id is mismatch, ignore heartbeat
	 */
	if (rel_check_net_compatible (&my_version,
				      &node_version) == REL_NOT_COMPATIBLE ||
	    strcmp (hbp_header->group_id, hb_Cluster->group_id))
	  {
	    break;
	  }

	/*
	 * must send heartbeat response in order to avoid split-brain
	 * when heartbeat configuration changed
	 */
	if (hbp_header->r && hb_Cluster->hide_to_demote == false)
	  {
	    hb_cluster_send_heartbeat (false, &hbp_header->orig_host);
	  }

	node = hb_return_node_by_name_except_me (&hbp_header->orig_host);
	if (node == NULL)
	  {
	    char orig_host[MAX_NODE_INFO_STR_LEN];
	    prm_node_info_to_str (orig_host, sizeof (orig_host),
				  &hbp_header->orig_host);

	    er_log_debug (ARG_FILE_LINE,
			  "receive heartbeat have unknown host_name. "
			  "(host_name:{%s}).\n", orig_host);
	    break;
	  }

	if (node->node_state == HA_STATE_MASTER
	    && node->node_state != (HA_STATE) node_state)
	  {
	    is_state_changed = true;
	  }

	hb_cluster_set_node_state (node, (unsigned short) node_state);
	hb_cluster_set_node_version (node, &node_version);
	node->heartbeat_gap = MAX (0, (node->heartbeat_gap - 1));
	gettimeofday (&node->last_recv_hbtime, NULL);
      }
      break;
    default:
      {
	er_log_debug (ARG_FILE_LINE, "unknown heartbeat message. "
		      "(type:%d). \n", hbp_header->type);
      }
      break;

    }

  pthread_mutex_unlock (&hb_Cluster->lock);

  if (is_state_changed == true)
    {
      er_log_debug (ARG_FILE_LINE, "peer node node_state has been changed.");
      hb_cluster_job_set_expire_and_reorder (HB_CJOB_CALC_SCORE,
					     HB_JOB_TIMER_IMMEDIATELY);
    }

  return;
}

/*
 * hb_set_net_header() -
 *   return: none
 *
 *   header(out):
 *   type(in):
 *   is_req(in):
 *   len(in):
 *   seq(in):
 *   dest_host_name(in):
 */
static void
hb_set_net_header (HBP_HEADER * header, unsigned char type, bool is_req,
		   unsigned short len, unsigned int seq,
		   const PRM_NODE_INFO * dest_host)
{
  header->type = type;
  header->r = (is_req) ? 1 : 0;
  header->len = len;
  header->seq = seq;
  strncpy (header->group_id, hb_Cluster->group_id,
	   sizeof (header->group_id) - 1);
  header->group_id[sizeof (header->group_id) - 1] = '\0';
  header->dest_host = *dest_host;
  header->orig_host = hb_Cluster->my_node_info;
}

/*
 * hb_sockaddr() -
 */
static int
hb_sockaddr (const PRM_NODE_INFO * node, struct sockaddr *saddr,
	     socklen_t * slen)
{
  struct sockaddr_in udp_saddr;

  /*
   * Construct address for UDP socket
   */
  memset ((void *) &udp_saddr, 0, sizeof (udp_saddr));
  udp_saddr.sin_family = AF_INET;
  udp_saddr.sin_port = htons (PRM_NODE_INFO_GET_PORT (node));

  udp_saddr.sin_addr.s_addr = PRM_NODE_INFO_GET_IP (node);

  *slen = sizeof (udp_saddr);
  memcpy ((void *) saddr, (void *) &udp_saddr, *slen);

  return NO_ERROR;
}


/*
 * cluster job queue
 */

/*
 * hb_cluster_job_dequeue() -
 *   return: pointer to cluster job entry
 */
static HB_JOB_ENTRY *
hb_cluster_job_dequeue (void)
{
  return hb_job_dequeue (cluster_JobQ);
}

/*
 * hb_cluster_job_queue_debug() -
 *   return: NO_ERROR or ER_FAILED
 *
 *   job_type(in):
 *   arg(in):
 *   msec(in):
 */
static int
hb_cluster_job_queue_debug (const char *fname, int line,
			    unsigned int job_type, HB_JOB_ARG * arg,
			    unsigned int msec)
{
  if (job_type >= HB_RJOB_PROC_START)
    {
      er_log_debug (ARG_FILE_LINE,
		    "unknown job type. (job_type:%d).\n", job_type);
      return ER_FAILED;
    }
  er_log_debug (ARG_FILE_LINE, "hb_cluster_job_queue(%s,%d)"
		"-job_type(%s), msec(%d)", fname, line,
		hb_Requests[job_type].name, msec);

  return hb_job_queue (cluster_JobQ, job_type, arg, msec);
}

/*
 * hb_cluster_job_set_expire_and_reorder() -
 *   return: NO_ERROR or ER_FAILED
 *
 *   job_type(in):
 *   msec(in):
 */
static int
hb_cluster_job_set_expire_and_reorder (unsigned int job_type,
				       unsigned int msec)
{
  if (job_type >= HB_RJOB_PROC_START)
    {
      er_log_debug (ARG_FILE_LINE,
		    "unknown job type. (job_type:%d).\n", job_type);
      return ER_FAILED;
    }

  hb_job_set_expire_and_reorder (cluster_JobQ, job_type, msec);

  return NO_ERROR;
}

/*
 * hb_cluster_job_shutdown() -
 *   return: pointer to cluster job entry
 */
static void
hb_cluster_job_shutdown (void)
{
  return hb_job_shutdown (cluster_JobQ);
}


/*
 * cluster node
 */

/*
 * hb_add_node_to_cluster() -
 *   return: pointer to heartbeat node entry
 */
static HB_NODE_ENTRY *
hb_add_node_to_cluster (PRM_NODE_INFO * node, unsigned short priority)
{
  HB_NODE_ENTRY *p;
  HB_NODE_ENTRY **first_pp;

  if (node == NULL)
    {
      return NULL;
    }

  p = (HB_NODE_ENTRY *) malloc (sizeof (HB_NODE_ENTRY));
  if (p)
    {
      p->node_info = *node;
      p->priority = priority;
      p->node_state = HA_STATE_UNKNOWN;
      p->score = 0;
      p->heartbeat_gap = 0;
      p->last_recv_hbtime.tv_sec = 0;
      p->last_recv_hbtime.tv_usec = 0;
      p->node_version = rel_null_version ();

      p->next = NULL;
      p->prev = NULL;
      first_pp = &hb_Cluster->nodes;
      hb_list_add ((HB_LIST **) first_pp, (HB_LIST *) p);
    }

  return (p);
}

/*
 * hb_remove_node() -
 *   return: none
 *
 *   entry_p(in):
 */
static void
hb_remove_node (HB_NODE_ENTRY * entry_p)
{
  if (entry_p)
    {
      hb_list_remove ((HB_LIST *) entry_p);
      free_and_init (entry_p);
    }
  return;
}

/*
 * hb_cluster_remove_all_nodes() -
 *   return: none
 *
 *   first(in):
 */
static void
hb_cluster_remove_all_nodes (HB_NODE_ENTRY * first)
{
  HB_NODE_ENTRY *node, *next_node;

  for (node = first; node; node = next_node)
    {
      next_node = node->next;
      hb_remove_node (node);
    }
}

/*
 * hb_add_ping_host() -
 *   return: pointer to ping host entry
 *
 *   host_name(in):
 */
static HB_PING_HOST_ENTRY *
hb_add_ping_host (char *host_name)
{
  HB_PING_HOST_ENTRY *p;
  HB_PING_HOST_ENTRY **first_pp;

  if (host_name == NULL)
    {
      return NULL;
    }

  p = (HB_PING_HOST_ENTRY *) malloc (sizeof (HB_PING_HOST_ENTRY));
  if (p)
    {
      strncpy (p->host_name, host_name, sizeof (p->host_name) - 1);
      p->host_name[sizeof (p->host_name) - 1] = '\0';
      p->ping_result = HB_PING_UNKNOWN;
      p->next = NULL;
      p->prev = NULL;

      first_pp = &hb_Cluster->ping_hosts;

      hb_list_add ((HB_LIST **) first_pp, (HB_LIST *) p);
    }

  return (p);
}

/*
 * hb_remove_ping_host() -
 *   return: none
 *
 *   entry_p(in):
 */
static void
hb_remove_ping_host (HB_PING_HOST_ENTRY * entry_p)
{
  if (entry_p)
    {
      hb_list_remove ((HB_LIST *) entry_p);
      free_and_init (entry_p);
    }
  return;
}

/*
 * hb_cluster_remove_all_ping_hosts() -
 *   return: none
 *
 *   first(in):
 */
static void
hb_cluster_remove_all_ping_hosts (HB_PING_HOST_ENTRY * first)
{
  HB_PING_HOST_ENTRY *host, *next_host;

  for (host = first; host; host = next_host)
    {
      next_host = host->next;
      hb_remove_ping_host (host);
    }
}

/*
 * hb_cluster_load_ping_host_list() -
 *   return: number of ping hosts
 *
 *   host_list(in):
 */
static int
hb_cluster_load_ping_host_list (char *ha_ping_host_list)
{
  int num_hosts = 0;
  char host_list[LINE_MAX];
  char *host_p, *host_pp;

  if (ha_ping_host_list == NULL)
    {
      return 0;
    }

  strncpy (host_list, ha_ping_host_list, LINE_MAX);
  host_list[LINE_MAX - 1] = '\0';

  host_p = strtok_r (host_list, " ,:", &host_pp);
  while (host_p != NULL)
    {
      hb_add_ping_host (host_p);
      num_hosts++;

      host_p = strtok_r (NULL, " ,:", &host_pp);
    }

  return num_hosts;
}

/*
 * hb_return_node_by_name() -
 *   return: pointer to heartbeat node entry
 *
 *   name(in):
 */
static HB_NODE_ENTRY *
hb_return_node_by_name (const PRM_NODE_INFO * node_info)
{
  HB_NODE_ENTRY *node;

  for (node = hb_Cluster->nodes; node; node = node->next)
    {
      if (prm_is_same_node (node_info, &node->node_info) == true)
	{
	  return (node);
	}
    }

  return NULL;
}

/*
 * hb_return_node_by_name_except_me() -
 *   return: pointer to heartbeat node entry
 *
 *   name(in):
 */
static HB_NODE_ENTRY *
hb_return_node_by_name_except_me (const PRM_NODE_INFO * node_info)
{
  HB_NODE_ENTRY *node;

  for (node = hb_Cluster->nodes; node; node = node->next)
    {
      if (prm_is_same_node (node_info, &node->node_info) == true &&
	  prm_is_same_node (node_info, &hb_Cluster->my_node_info) == false)
	{
	  return (node);
	}
    }

  return NULL;
}

/*
 * hb_cluster_load_group_and_node_list() -
 *   return: number of cluster nodes
 *
 *   host_list(in):
 */
static int
hb_cluster_load_group_and_node_list ()
{
  HB_NODE_ENTRY *node;
  PRM_NODE_LIST ha_node_list;
  PRM_NODE_LIST ha_replica_list;
  int i;

  prm_get_ha_node_list (&ha_node_list);
  prm_get_ha_replica_list (&ha_replica_list);

  hb_Cluster->myself = NULL;

  strncpy (hb_Cluster->group_id, ha_node_list.hb_group_id,
	   sizeof (hb_Cluster->group_id) - 1);
  hb_Cluster->group_id[sizeof (hb_Cluster->group_id) - 1] = '\0';

  for (i = 0; i < ha_node_list.num_nodes; i++)
    {
      node = hb_add_node_to_cluster (&ha_node_list.nodes[i], i + 1);
      if (node)
	{
	  if (prm_is_same_node (&node->node_info,
				&hb_Cluster->my_node_info) == true)
	    {
	      hb_Cluster->myself = node;
	    }
	}
    }

  if (hb_Cluster->node_state == HA_STATE_REPLICA
      && hb_Cluster->myself != NULL)
    {
      er_log_debug (ARG_FILE_LINE,
		    "myself should be in the ha_replica_list. \n");
      return ER_FAILED;
    }

  if (ha_replica_list.num_nodes > 0)
    {
      if (strcmp (hb_Cluster->group_id, ha_replica_list.hb_group_id) != 0)
	{
	  er_log_debug (ARG_FILE_LINE,
			"different group id ('ha_node_list', 'ha_replica_list') \n");
	  return ER_FAILED;
	}

      for (i = 0; i < ha_replica_list.num_nodes; i++)
	{
	  node = hb_add_node_to_cluster (&ha_replica_list.nodes[i],
					 HB_REPLICA_PRIORITY);
	  if (node)
	    {
	      if (prm_is_same_node (&node->node_info,
				    &hb_Cluster->my_node_info) == true)
		{
		  hb_Cluster->myself = node;
		  hb_Cluster->node_state = HA_STATE_REPLICA;
		}
	    }
	}
    }

  if (hb_Cluster->myself == NULL)
    {
      er_log_debug (ARG_FILE_LINE, "cannot find myself. \n");
      return ER_FAILED;
    }

  return ha_node_list.num_nodes + ha_replica_list.num_nodes;
}

/*
 * resource process job actions
 */

/*
 * make_shard_groupid_bitmap () - recv shard groupid info and make bitmap.
			      called by hb_resource_job_change_groupid_bitmap()
 *   return: error code.
 *
 *   bitmap(out):
 *   local_dbname(in):
 */
static int
make_shard_groupid_bitmap (SERVER_SHM_SHARD_INFO * shard_info,
			   const char *local_dbname)
{
  CCI_CONN conn;
  char url[1024];
  CCI_SHARD_NODE_INFO *node_info = NULL;
  CCI_SHARD_GROUPID_INFO *groupid_info = NULL;
  char global_dbname[RYE_SHD_MGMT_TABLE_DBNAME_SIZE];
  short nodeid = 0;
  const unsigned char *shard_mgmt_ip;
  int shard_mgmt_port;
  int error = NO_ERROR;
  int i;
  PRM_NODE_INFO shard_mgmt_node_info;

  /* init out-param */
  shard_info->groupid_bitmap_size = -1;
  shard_info->num_groupid = 0;
  shard_info->nodeid = -1;

  if (master_shm_get_shard_mgmt_info (local_dbname, global_dbname, &nodeid,
				      &shard_mgmt_node_info) < 0)
    {
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "Invalid shard mgmt info");
      return error;
    }

  shard_mgmt_ip =
    (const unsigned char *) &(PRM_NODE_INFO_GET_IP (&shard_mgmt_node_info));
  shard_mgmt_port = PRM_NODE_INFO_GET_PORT (&shard_mgmt_node_info);
  snprintf (url, sizeof (url),
	    "cci:rye://%d.%d.%d.%d:%d/%s:dba/rw?queryTimeout=1000&connectionType=global",
	    shard_mgmt_ip[0], shard_mgmt_ip[1], shard_mgmt_ip[2],
	    shard_mgmt_ip[3], shard_mgmt_port, global_dbname);

  if (cci_connect (&conn, url, "", "") < 0)
    {
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "can't connect shard mgmt");
      return error;
    }

  if (cci_shard_get_info (&conn, &node_info, &groupid_info) < 0)
    {
      cci_disconnect (&conn);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "can't get node and groupid info");
      return error;
    }

  /* check current dbname is valid */
  if (groupid_info == NULL || groupid_info->groupid_version <= 0
      || node_info == NULL || node_info->node_version <= 0
      || groupid_info->groupid_count + 1 > GROUPID_MAX)
    {
      cci_disconnect (&conn);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "invalid node or groupid info");
      return error;
    }

  for (i = 0; i < node_info->node_count; i++)
    {
      if (strcmp (node_info->node_info[i].dbname, local_dbname) == 0)
	{
	  if (node_info->node_info[i].nodeid == nodeid)
	    {
	      shard_info->groupid_bitmap_size =
		((groupid_info->groupid_count + 1) / BITS_IN_BYTE) + 1;
	      shard_info->groupid_bitmap_size =
		DB_ALIGN (shard_info->groupid_bitmap_size, INT_ALIGNMENT);

	      shard_info->nodeid = nodeid;
	      shard_info->num_groupid = groupid_info->groupid_count + 1;
	    }
	  break;
	}
    }

  if (shard_info->groupid_bitmap_size > 0)
    {
      int i;

      /* clear 0-th groupid bit */
      SHARD_CLEAR_GROUP_BIT (shard_info->groupid_bitmap, 0);	/* GLOBAL_GROUPID */

      for (i = 1; i <= groupid_info->groupid_count; i++)
	{
	  if (groupid_info->nodeid_table[i] == nodeid)
	    {
	      SHARD_ENABLE_GROUP_BIT (shard_info->groupid_bitmap, i);
	    }
	  else
	    {
	      SHARD_CLEAR_GROUP_BIT (shard_info->groupid_bitmap, i);
	    }
	}
    }

  cci_shard_node_info_free (node_info);
  cci_shard_group_info_free (groupid_info);

  cci_disconnect (&conn);

  assert (error == NO_ERROR);
  return NO_ERROR;
}

/*
 * hb_resource_job_change_groupid_bitmap () - chagne groupid bitmap of rye_server
 *   return: none
 *
 *   arg(in):
 */
static void
hb_resource_job_change_groupid_bitmap (UNUSED_ARG HB_JOB_ARG * arg)
{
  HB_PROC_ENTRY *proc;
  int i;
  int send_bitmap_count;
  SERVER_SHM_SHARD_INFO shard_info;
  char local_dbname[RYE_SHD_MGMT_TABLE_SIZE][HB_MAX_SZ_PROC_ARGV];
  int error = NO_ERROR;

  send_bitmap_count = 0;

  /* find server that needs to send groupid bitmap */
  pthread_mutex_lock (&hb_Resource->lock);

  for (proc = hb_Resource->procs; proc; proc = proc->next)
    {
      if (send_bitmap_count == RYE_SHD_MGMT_TABLE_SIZE)
	{
	  break;
	}

      if (proc->type == HB_PTYPE_SERVER && proc->sync_groupid_bitmap == false)
	{
	  STRNCPY (local_dbname[send_bitmap_count], proc->argv[1],
		   HB_MAX_SZ_PROC_ARGV);
	  send_bitmap_count++;
	}
    }

  pthread_mutex_unlock (&hb_Resource->lock);

  /* get groupid info from shard mgmt and make stream.
   * this routine should not be inside hb_Resource->lock mutex
   * because shard mgmt's new db connection request may be blocked */
  for (i = 0; i < send_bitmap_count; i++)
    {
      shard_info.groupid_bitmap_size = -1;
      error = make_shard_groupid_bitmap (&shard_info, local_dbname[i]);
      if (error != NO_ERROR)
	{
	  continue;
	}
      if (shard_info.groupid_bitmap_size < 0)
	{
	  assert (false);
	  continue;
	}

      /* send bitmap stream to server */
      pthread_mutex_lock (&hb_Resource->lock);

      for (proc = hb_Resource->procs; proc; proc = proc->next)
	{
	  if (proc->type == HB_PTYPE_SERVER
	      && proc->sync_groupid_bitmap == false)
	    {
	      if (strcmp (local_dbname[i], proc->argv[1]) == 0)
		{
		  error = rye_server_shm_set_groupid_bitmap (&shard_info,
							     local_dbname[i]);
		  if (error == NO_ERROR)
		    {
		      proc->sync_groupid_bitmap = true;
		    }
		  break;
		}
	    }
	}

      pthread_mutex_unlock (&hb_Resource->lock);
    }

  return;
}

/*
 * hb_resource_job_confirm_cleanup_all () - confirm that all HA processes are shutdown
 *   for deactivating heartbeat
 *   return: none
 *
 *   arg(in):
 */
static void
hb_resource_job_confirm_cleanup_all (HB_JOB_ARG * arg)
{
  int error;
  HB_RESOURCE_JOB_ARG *resource_job_arg;
  HB_PROC_ENTRY *proc, *proc_next;
  char error_string[LINE_MAX] = "";
  int num_connected_rsc = 0;

  resource_job_arg = (arg) ? &(arg->resource_job_arg) : NULL;

  if (arg == NULL || resource_job_arg == NULL)
    {
      er_log_debug (ARG_FILE_LINE,
		    "invalid arg or resource_job_arg. (arg:%p, resource_job_arg:%p). \n",
		    arg, resource_job_arg);
      return;
    }

  pthread_mutex_lock (&hb_Resource->lock);

  if (++(resource_job_arg->retries) > resource_job_arg->max_retries
      || hb_Deactivate_immediately == true)
    {
      for (proc = hb_Resource->procs; proc; proc = proc_next)
	{
	  assert (proc->state == HB_PSTATE_DEREGISTERED);
	  assert (proc->pid > 0);

	  proc_next = proc->next;

	  if (proc->pid > 0 && (kill (proc->pid, 0) == 0 || errno != ESRCH))
	    {
	      snprintf (error_string, LINE_MAX, "(pid: %d, args:%s)",
			proc->pid, proc->args);
	      if (hb_Deactivate_immediately == true)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_HB_PROCESS_EVENT, 2,
			  "Immediate shutdown requested. Process killed",
			  error_string);
		}
	      else
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_HB_PROCESS_EVENT, 2,
			  "No response to shutdown request. Process killed",
			  error_string);
		}

	      kill (proc->pid, SIGKILL);
	    }

	  hb_Resource->num_procs--;
	  hb_remove_proc (proc);
	  proc = NULL;
	}

      assert (hb_Resource->num_procs == 0);
      goto end_confirm_cleanup;
    }

  for (proc = hb_Resource->procs; proc; proc = proc_next)
    {
      assert (proc->state == HB_PSTATE_DEREGISTERED);
      assert (proc->pid > 0);

      proc_next = proc->next;

      if (proc->type != HB_PTYPE_SERVER)
	{
	  if (proc->pid <= 0 || (kill (proc->pid, 0) != 0 && errno == ESRCH))
	    {
	      hb_Resource->num_procs--;
	      hb_remove_proc (proc);
	      proc = NULL;
	    }
	  else
	    {
	      kill (proc->pid, SIGTERM);
	    }
	}
      else
	{
	  if (proc->pid <= 0 || (kill (proc->pid, 0) != 0 && errno == ESRCH))
	    {
	      hb_Resource->num_procs--;
	      hb_remove_proc (proc);
	      proc = NULL;
	      continue;
	    }
	}

      if (proc != NULL && proc->conn != NULL)
	{
	  num_connected_rsc++;
	}

      assert (hb_Resource->num_procs >= 0);
    }

  if (hb_Resource->num_procs == 0 || num_connected_rsc == 0)
    {
      goto end_confirm_cleanup;
    }

  pthread_mutex_unlock (&hb_Resource->lock);

  error = hb_resource_job_queue (HB_RJOB_CONFIRM_CLEANUP_ALL, arg,
				 prm_get_bigint_value
				 (PRM_ID_HA_PROCESS_DEREG_CONFIRM_INTERVAL));

  if (error != NO_ERROR)
    {
      assert (false);
      free_and_init (arg);
    }

  return;

end_confirm_cleanup:
  pthread_mutex_unlock (&hb_Resource->lock);

  if (arg != NULL)
    {
      free_and_init (arg);
    }

  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_HB_PROCESS_EVENT,
	  2, "ready to deactivate heartbeat", "");
  return;
}

/*
 * hb_resource_job_cleanup_all () - shutdown all HA processes including rye_server
 *   for deactivating heartbeat
 *   return: none
 *
 *   arg(in):
 */
static void
hb_resource_job_cleanup_all (UNUSED_ARG HB_JOB_ARG * arg)
{
  int i, error;
  HB_PROC_ENTRY *proc;
  HB_JOB_ARG *job_arg;

  pthread_mutex_lock (&css_Master_socket_anchor_lock);
  pthread_mutex_lock (&hb_Resource->lock);

  if (hb_Deactivate_immediately == false)
    {
      /* register Rye server pid */
      hb_Deactivate_info.server_pid_list =
	(int *) calloc (hb_Resource->num_procs, sizeof (int));

      for (i = 0, proc = hb_Resource->procs; proc; proc = proc->next)
	{
	  if (proc->conn && proc->type == HB_PTYPE_SERVER)
	    {
	      hb_Deactivate_info.server_pid_list[i] = proc->pid;
	      i++;
	    }
	}

      hb_Deactivate_info.server_count = i;

      assert (hb_Resource->num_procs >= i);
    }

  hb_resource_shutdown_all_ha_procs ();

  job_arg = hb_alloc_resource_job_arg (-1, NULL,
				       prm_get_integer_value
				       (PRM_ID_HA_MAX_PROCESS_DEREG_CONFIRM));
  if (job_arg == NULL)
    {
      pthread_mutex_unlock (&hb_Resource->lock);
      pthread_mutex_unlock (&css_Master_socket_anchor_lock);

      return;
    }

  pthread_mutex_unlock (&hb_Resource->lock);
  pthread_mutex_unlock (&css_Master_socket_anchor_lock);

  error = hb_resource_job_queue (HB_RJOB_CONFIRM_CLEANUP_ALL, job_arg,
				 HB_JOB_TIMER_WAIT_500_MILLISECOND);

  if (error != NO_ERROR)
    {
      assert (false);
      free_and_init (job_arg);
    }

  return;
}

/*
 * hb_resource_job_proc_start () -
 *   return: none
 *
 *   arg(in):
 */
static void
hb_resource_job_proc_start (HB_JOB_ARG * arg)
{
  int error;
  char error_string[LINE_MAX] = "";
  pid_t pid;
  struct timeval now;
  HB_PROC_ENTRY *proc;
  HB_RESOURCE_JOB_ARG *proc_arg = (arg) ? &(arg->resource_job_arg) : NULL;
  char *argv[HB_MAX_NUM_PROC_ARGV] = { NULL, };

  if (arg == NULL || proc_arg == NULL)
    {
      er_log_debug (ARG_FILE_LINE,
		    "invalid arg or proc_arg. (arg:%p, proc_arg:%p). \n",
		    arg, proc_arg);
      return;
    }

  pthread_mutex_lock (&hb_Resource->lock);
  proc = hb_return_proc_by_args (proc_arg->args);
  if (proc == NULL || proc->state == HB_PSTATE_DEREGISTERED)
    {
      pthread_mutex_unlock (&hb_Resource->lock);
      free_and_init (arg);
      return;
    }

  if (proc->being_shutdown)
    {
      assert (proc_arg->pid > 0);
      if (proc_arg->pid <= 0 || (kill (proc_arg->pid, 0) && errno == ESRCH))
	{
	  proc->being_shutdown = false;
	}
      else
	{
	  pthread_mutex_unlock (&hb_Resource->lock);
	  error = hb_resource_job_queue (HB_RJOB_PROC_START, arg,
					 HB_JOB_TIMER_WAIT_A_SECOND);
	  if (error != NO_ERROR)
	    {
	      assert (false);
	      free_and_init (arg);
	    }
	  return;
	}
    }

  gettimeofday (&now, NULL);
  if (HB_GET_ELAPSED_TIME (now, proc->frtime) < HB_PROC_RECOVERY_DELAY_TIME)
    {
      er_log_debug (ARG_FILE_LINE,
		    "delay the restart of the process. (arg:%p, proc_arg:%p). \n",
		    arg, proc_arg);

      pthread_mutex_unlock (&hb_Resource->lock);
      error = hb_resource_job_queue (HB_RJOB_PROC_START, arg,
				     HB_JOB_TIMER_WAIT_A_SECOND);
      if (error != NO_ERROR)
	{
	  assert (false);
	  free_and_init (arg);
	}
      return;
    }

  snprintf (error_string, LINE_MAX, "(args:%s)", proc->args);
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HB_PROCESS_EVENT, 2,
	  "Restart the process", error_string);

  hb_proc_make_arg (argv, (char *) proc->argv);

  pid = fork ();
  if (pid < 0)
    {
      pthread_mutex_unlock (&hb_Resource->lock);

      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ERR_CSS_CANNOT_FORK, 0);

      error = hb_resource_job_queue (HB_RJOB_PROC_START, arg,
				     HB_JOB_TIMER_WAIT_A_SECOND);
      if (error != NO_ERROR)
	{
	  assert (false);
	  free_and_init (arg);
	}
      return;
    }
  else if (pid == 0)
    {
      int max_fd, i;
      char proc_id_env_str[ONE_K];
      char argv0[PATH_MAX];
      const char *exe_name;

      max_fd = css_get_max_socket_fds ();
      for (i = 3; i <= max_fd; i++)
	{
	  close (i);
	}

      sprintf (proc_id_env_str, "%s=%d", HB_PROC_ID_STR, getpid ());
      putenv (proc_id_env_str);

      if (proc->type == HB_PTYPE_SERVER)
	{
	  exe_name = UTIL_SERVER_NAME;
	}
      else if (proc->type == HB_PTYPE_REPLICATION)
	{
	  exe_name = UTIL_REPL_NAME;
	}
      else
	{
	  assert (0);
	  exe_name = NULL;
	}
      if (exe_name != NULL)
	{
	  envvar_process_name (argv0, sizeof (argv0), exe_name);
	  argv[0] = argv0;
	}

      error = execv (proc->exec_path, argv);

      exit (0);
    }
  else
    {
      proc->pid = pid;
      proc->state = HB_PSTATE_STARTED;
      gettimeofday (&proc->stime, NULL);
    }

  pthread_mutex_unlock (&hb_Resource->lock);

  error = hb_resource_job_queue (HB_RJOB_CONFIRM_START, arg,
				 prm_get_bigint_value
				 (PRM_ID_HA_PROCESS_START_CONFIRM_INTERVAL));
  if (error != NO_ERROR)
    {
      assert (false);
      free_and_init (arg);
    }

  return;
}

/*
 * hb_resource_demote_start_shutdown_server_proc() -
 *      send shutdown request to server
 *   return: none
 *
 */
static void
hb_resource_demote_start_shutdown_server_proc (void)
{
  HB_PROC_ENTRY *proc;
  SOCKET_QUEUE_ENTRY *sock_entq;
  char buffer[MASTER_TO_SRV_MSG_SIZE];

  for (proc = hb_Resource->procs; proc; proc = proc->next)
    {
      /* leave processes other than rye_server */
      if (!HB_PROC_IS_MASTER_SERVER (proc))
	{
	  continue;
	}

      if (proc->server_hang)
	{
	  /* terminate a hang server process immediately */
	  assert (proc->pid > 0);
	  if (proc->pid > 0 && (kill (proc->pid, 0) == 0 || errno != ESRCH))
	    {
	      kill (proc->pid, SIGKILL);
	    }
	  continue;
	}

      sock_entq = css_return_entry_by_conn (proc->conn,
					    &css_Master_socket_anchor);
      assert_release (sock_entq == NULL || sock_entq->name != NULL);
      if (sock_entq != NULL && sock_entq->name != NULL)
	{
	  memset (buffer, 0, sizeof (buffer));
	  snprintf (buffer, sizeof (buffer) - 1,
		    msgcat_message (MSGCAT_CATALOG_UTILS,
				    MSGCAT_UTIL_SET_MASTER,
				    MASTER_MSG_SERVER_STATUS),
		    sock_entq->name + 1, 0);

	  css_process_start_shutdown (sock_entq, 0, buffer);
	  proc->being_shutdown = true;
	}
    }
  return;
}

/*
 * hb_resource_demote_confirm_shutdown_server_proc() -
 *      confirm that server process is shutdown
 *   return: whether all active, to-be-active server proc's are shutdown
 *
 */
static bool
hb_resource_demote_confirm_shutdown_server_proc (void)
{
  HB_PROC_ENTRY *proc;

  for (proc = hb_Resource->procs; proc; proc = proc->next)
    {
      if (proc->server_hang)
	{
	  /* don't wait for a hang server process that has already been terminated */
	  continue;
	}

      if (HB_PROC_IS_MASTER_SERVER (proc))
	{
	  return false;
	}
    }
  return true;
}

/*
 * hb_resource_demote_kill_server_proc() -
 *      kill server process in an active or to-be-active state
 *   return: none
 *
 */
static void
hb_resource_demote_kill_server_proc (void)
{
  HB_PROC_ENTRY *proc;
  char error_string[LINE_MAX] = "";

  for (proc = hb_Resource->procs; proc; proc = proc->next)
    {
      if (!HB_PROC_IS_MASTER_SERVER (proc))
	{
	  continue;
	}

      assert (proc->pid > 0);
      if (proc->pid > 0 && (kill (proc->pid, 0) == 0 || errno != ESRCH))
	{
	  snprintf (error_string, LINE_MAX, "(pid: %d, args:%s)",
		    proc->pid, proc->args);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_HB_PROCESS_EVENT, 2,
		  "No response to shutdown request. Process killed",
		  error_string);

	  kill (proc->pid, SIGKILL);
	}
    }
}

/*
 * hb_resource_job_demote_confirm_shutdown() -
 *      prepare for demoting master
 *      it checks if every active server process is shutdown
 *      if so, it assigns demote cluster job
 *   return: none
 *
 *   arg(in):
 */
static void
hb_resource_job_demote_confirm_shutdown (HB_JOB_ARG * arg)
{
  int error;
  HB_JOB_ARG *job_arg;
  HB_RESOURCE_JOB_ARG *proc_arg = (arg) ? &(arg->resource_job_arg) : NULL;

  if (arg == NULL || proc_arg == NULL)
    {
      er_log_debug (ARG_FILE_LINE,
		    "invalid arg or proc_arg. (arg:%p, proc_arg:%p). \n",
		    arg, proc_arg);
      return;
    }

  pthread_mutex_lock (&hb_Resource->lock);

  if (++(proc_arg->retries) > proc_arg->max_retries)
    {
      hb_resource_demote_kill_server_proc ();
      goto demote_confirm_shutdown_end;
    }

  if (hb_resource_demote_confirm_shutdown_server_proc () == false)
    {
      pthread_mutex_unlock (&hb_Resource->lock);

      error =
	hb_resource_job_queue (HB_RJOB_DEMOTE_CONFIRM_SHUTDOWN, arg,
			       prm_get_bigint_value
			       (PRM_ID_HA_PROCESS_DEREG_CONFIRM_INTERVAL));

      assert (error == NO_ERROR);

      return;
    }

demote_confirm_shutdown_end:
  pthread_mutex_unlock (&hb_Resource->lock);

  job_arg = hb_alloc_cluster_job_arg ();
  if (job_arg == NULL)
    {
      if (arg != NULL)
	{
	  free_and_init (arg);
	}
      css_master_cleanup (SIGTERM);
      return;
    }

  error = hb_cluster_job_queue (HB_CJOB_DEMOTE, job_arg,
				HB_JOB_TIMER_IMMEDIATELY);
  if (error != NO_ERROR)
    {
      assert (false);
      free_and_init (job_arg);
    }

  if (arg != NULL)
    {
      free_and_init (arg);
    }

  return;
}

/*
 * hb_resource_job_demote_start_shutdown() -
 *      prepare for demoting master
 *      it shuts down working active server processes
 *   return: none
 *
 *   arg(in):
 */
static void
hb_resource_job_demote_start_shutdown (HB_JOB_ARG * arg)
{
  int error;
  HB_JOB_ARG *job_arg;

  pthread_mutex_lock (&css_Master_socket_anchor_lock);
  pthread_mutex_lock (&hb_Resource->lock);

  hb_resource_demote_start_shutdown_server_proc ();

  pthread_mutex_unlock (&hb_Resource->lock);
  pthread_mutex_unlock (&css_Master_socket_anchor_lock);

  job_arg = hb_alloc_resource_job_arg (-1, NULL,
				       prm_get_integer_value
				       (PRM_ID_HA_MAX_PROCESS_DEREG_CONFIRM));
  if (job_arg == NULL)
    {
      if (arg != NULL)
	{
	  free_and_init (arg);
	}
      css_master_cleanup (SIGTERM);
      return;
    }

  error = hb_resource_job_queue (HB_RJOB_DEMOTE_CONFIRM_SHUTDOWN, job_arg,
				 prm_get_bigint_value
				 (PRM_ID_HA_PROCESS_DEREG_CONFIRM_INTERVAL));
  if (error != NO_ERROR)
    {
      assert (false);
      free_and_init (job_arg);
    }

  if (arg)
    {
      free_and_init (arg);
    }
  return;
}

/*
 * hb_resource_job_confirm_start() -
 *   return: none
 *
 *   arg(in):
 */
static void
hb_resource_job_confirm_start (HB_JOB_ARG * arg)
{
  int error, rv;
  char error_string[LINE_MAX] = "";
  bool retry = true;
  HB_PROC_ENTRY *proc = NULL;
  HB_RESOURCE_JOB_ARG *proc_arg = (arg) ? &(arg->resource_job_arg) : NULL;
  char hb_info_str[HB_INFO_STR_MAX];

  if (arg == NULL || proc_arg == NULL)
    {
      er_log_debug (ARG_FILE_LINE,
		    "invalid arg or proc_arg. (arg:%p, proc_arg:%p). \n",
		    arg, proc_arg);
      return;
    }

  hb_info_str[0] = '\0';	/* init */

  rv = pthread_mutex_lock (&hb_Resource->lock);
  proc = hb_return_proc_by_args (proc_arg->args);
  if (proc == NULL || proc->state == HB_PSTATE_DEREGISTERED)
    {
      pthread_mutex_unlock (&hb_Resource->lock);
      free_and_init (arg);
      return;
    }

  if (++(proc_arg->retries) > proc_arg->max_retries)
    {
      snprintf (error_string, LINE_MAX,
		"(exceed max retry count for pid: %d, args:%s)", proc->pid,
		proc->args);

      if (hb_Resource->node_state == HA_STATE_MASTER
	  && proc->type == HB_PTYPE_SERVER
	  && hb_Cluster->is_isolated == false)
	{
	  hb_Resource->node_state = HA_STATE_SLAVE;
	  pthread_mutex_unlock (&hb_Resource->lock);

	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_HB_PROCESS_EVENT, 2,
		  "Failed to restart the process "
		  "and the current node will be demoted", error_string);

	  /* keep checking problematic process */
	  proc_arg->retries = 0;
	  error =
	    hb_resource_job_queue (HB_RJOB_CONFIRM_START, arg,
				   prm_get_bigint_value
				   (PRM_ID_HA_PROCESS_START_CONFIRM_INTERVAL));
	  if (error != NO_ERROR)
	    {
	      free_and_init (arg);
	      assert (false);
	    }

	  /* shutdown working server processes to change its role to slave */
	  error =
	    hb_resource_job_queue (HB_RJOB_DEMOTE_START_SHUTDOWN, NULL,
				   HB_JOB_TIMER_IMMEDIATELY);
	  assert (error == NO_ERROR);

	  return;
	}
      else
	{
	  pthread_mutex_unlock (&hb_Resource->lock);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_HB_PROCESS_EVENT, 2,
		  "Keep checking to confirm the completion of the process startup",
		  error_string);
	  proc_arg->retries = 0;
	  error = hb_resource_job_queue (HB_RJOB_CONFIRM_START, arg,
					 prm_get_bigint_value
					 (PRM_ID_HA_PROCESS_START_CONFIRM_INTERVAL));
	  if (error != NO_ERROR)
	    {
	      assert (false);
	      free_and_init (arg);
	    }
	  return;
	}
    }

  assert (proc->pid > 0);
  rv = kill (proc->pid, 0);
  if (rv != 0)
    {
      pthread_mutex_unlock (&hb_Resource->lock);
      if (errno == ESRCH)
	{
	  snprintf (error_string, LINE_MAX,
		    "(process not found, expected pid: %d, args:%s)",
		    proc->pid, proc->args);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_HB_PROCESS_EVENT, 2, "Failed to restart process",
		  error_string);

	  error = hb_resource_job_queue (HB_RJOB_PROC_START, arg,
					 HB_JOB_TIMER_WAIT_A_SECOND);
	  if (error != NO_ERROR)
	    {
	      assert (false);
	      free_and_init (arg);
	    }
	}
      else
	{
	  error = hb_resource_job_queue (HB_RJOB_CONFIRM_START, arg,
					 prm_get_bigint_value
					 (PRM_ID_HA_PROCESS_START_CONFIRM_INTERVAL));
	  if (error != NO_ERROR)
	    {
	      assert (false);
	      free_and_init (arg);
	    }
	}
      return;
    }

  if (proc->state == HB_PSTATE_NOT_REGISTERED)
    {
      if (proc->type == HB_PTYPE_SERVER)
	{
	  proc->state = HB_PSTATE_REGISTERED;
	  proc->server_state = HA_STATE_SLAVE;
	}
      else
	{
	  proc->state = HB_PSTATE_REGISTERED;
	  proc->server_state = HA_STATE_UNKNOWN;
	}

      retry = false;
    }

  pthread_mutex_unlock (&hb_Resource->lock);

  shm_master_update_server_state (proc);

  hb_help_sprint_processes_info (hb_info_str, HB_INFO_STR_MAX);
  er_log_debug (ARG_FILE_LINE, "%s", hb_info_str);

  if (retry)
    {
      error = hb_resource_job_queue (HB_RJOB_CONFIRM_START, arg,
				     prm_get_bigint_value
				     (PRM_ID_HA_PROCESS_START_CONFIRM_INTERVAL));
      if (error != NO_ERROR)
	{
	  assert (false);
	  free_and_init (arg);
	}
      return;
    }

  free_and_init (arg);

  return;
}

/*
 * hb_resource_job_sync_server_state() -
 *   return: none
 *
 *   arg(in):
 */
static void
hb_resource_job_sync_server_state (HB_JOB_ARG * arg)
{
  int error;
  HB_PROC_ENTRY *proc;
  char hb_info_str[HB_INFO_STR_MAX];
  bool force = false;

  hb_info_str[0] = '\0';	/* init */

  pthread_mutex_lock (&css_Master_socket_anchor_lock);
  pthread_mutex_lock (&hb_Resource->lock);

  for (proc = hb_Resource->procs; proc; proc = proc->next)
    {
      if (proc->type != HB_PTYPE_SERVER
	  || proc->state != HB_PSTATE_REGISTERED)
	{
	  continue;
	}

      if (hb_Resource->node_state != proc->server_state)
	{
	  /* TODO : send heartbeat changemode request */
	  er_log_debug (ARG_FILE_LINE,
			"send changemode request. "
			"(node_state:%s, pid:%d, proc_state:%s, server_state:%s). \n",
			HA_STATE_NAME (hb_Resource->node_state),
			proc->pid, hb_process_state_string (proc->state),
			HA_STATE_NAME (proc->server_state));

	  force = false;
	  if (hb_Resource->node_state == HA_STATE_MASTER
	      && proc->server_state == HA_STATE_TO_BE_SLAVE)
	    {
	      force = true;
	    }

	  error = hb_resource_sync_server_state (proc, force);
	  if (NO_ERROR != error)
	    {
	      /* TODO : if error */
	    }
	}
    }

  if (hb_Resource->procs)
    {
      hb_help_sprint_processes_info (hb_info_str, HB_INFO_STR_MAX);
      er_log_debug (ARG_FILE_LINE, "%s", hb_info_str);
    }

  pthread_mutex_unlock (&hb_Resource->lock);
  pthread_mutex_unlock (&css_Master_socket_anchor_lock);

  error = hb_resource_job_queue (HB_RJOB_SYNC_SERVER_STATE, NULL,
				 prm_get_bigint_value
				 (PRM_ID_SERVER_STATE_SYNC_INTERVAL));
  assert (error == NO_ERROR);

  if (arg)
    {
      free_and_init (arg);
    }
  return;
}

static void
hb_resource_job_reload_nodes (UNUSED_ARG HB_JOB_ARG * arg)
{
  HA_CONF *ha_conf = NULL;
  int error = NO_ERROR;
  PRM_NODE_LIST removed_nodes;

  error = hb_reload_config (&removed_nodes);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = hb_repl_stop ();
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  ha_conf = (HA_CONF *) calloc (1, sizeof (HA_CONF));
  if (ha_conf == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, sizeof (HA_CONF));
      GOTO_EXIT_ON_ERROR;
    }

  error = util_make_ha_conf (ha_conf);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = hb_remove_copylog (ha_conf, &removed_nodes);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  error = hb_remove_catalog_info (ha_conf, &removed_nodes);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = hb_process_start (ha_conf);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (ha_conf != NULL)
    {
      util_free_ha_conf (ha_conf);
      free_and_init (ha_conf);
    }

  assert (error == NO_ERROR);
  return;

exit_on_error:
  assert (error != NO_ERROR);

  if (ha_conf != NULL)
    {
      util_free_ha_conf (ha_conf);
      free_and_init (ha_conf);
    }

  return;
}

/*
 * resource job queue
 */

/*
 * hb_resource_job_dequeue() -
 *   return: pointer to resource job entry
 *
 */
static HB_JOB_ENTRY *
hb_resource_job_dequeue (void)
{
  return hb_job_dequeue (resource_JobQ);
}

/*
 * hb_resource_job_queue_debug() -
 *   return: NO_ERROR or ER_FAILED
 *
 *   job_type(in):
 *   arg(in):
 *   msec(in):
 */
static int
hb_resource_job_queue_debug (const char *fname, int line,
			     unsigned int job_type, HB_JOB_ARG * arg,
			     unsigned int msec)
{
  if (job_type < HB_RJOB_PROC_START || job_type >= HB_JOB_MAX)
    {
      er_log_debug (ARG_FILE_LINE,
		    "unknown job type. (job_type:%d).\n", job_type);
      return ER_FAILED;
    }
  er_log_debug (ARG_FILE_LINE, "hb_resource_job_queue (%s,%d)-"
		"job_type(%s), msec(%d)", fname, line,
		hb_Requests[job_type].name, msec);

  return hb_job_queue (resource_JobQ, job_type, arg, msec);
}

/*
 * hb_resource_job_set_expire_and_reorder() -
 *   return: NO_ERROR or ER_FAILED
 *
 *   job_type(in):
 *   msec(in):
 */
static int
hb_resource_job_set_expire_and_reorder (unsigned int job_type,
					unsigned int msec)
{
  if (job_type < HB_RJOB_PROC_START || job_type >= HB_JOB_MAX)
    {
      assert (false);
      er_log_debug (ARG_FILE_LINE,
		    "unknown job type. (job_type:%d).\n", job_type);
      return ER_FAILED;
    }

  hb_job_set_expire_and_reorder (resource_JobQ, job_type, msec);

  return NO_ERROR;
}

/*
 * hb_resource_job_shutdown() -
 *   return: none
 *
 */
static void
hb_resource_job_shutdown (void)
{
  return hb_job_shutdown (resource_JobQ);
}

/*
 * resource process
 */

/*
 * hb_alloc_new_proc() -
 *   return: pointer to resource process entry
 *
 */
static HB_PROC_ENTRY *
hb_alloc_new_proc (void)
{
  HB_PROC_ENTRY *p;
  HB_PROC_ENTRY **first_pp;

  p = (HB_PROC_ENTRY *) malloc (sizeof (HB_PROC_ENTRY));
  if (p)
    {
      memset ((void *) p, 0, sizeof (HB_PROC_ENTRY));
      p->state = HB_PSTATE_UNKNOWN;
      p->server_state = HA_STATE_UNKNOWN;
      p->next = NULL;
      p->prev = NULL;
      p->sync_groupid_bitmap = false;
      p->being_shutdown = false;
      p->server_hang = false;
      LSA_SET_NULL (&p->prev_eof);
      LSA_SET_NULL (&p->curr_eof);

      first_pp = &hb_Resource->procs;
      hb_list_add ((HB_LIST **) first_pp, (HB_LIST *) p);
    }

  return (p);
}

/*
 * hb_remove_proc() -
 *   return: none
 *
 *   entry_p(in):
 */
static void
hb_remove_proc (HB_PROC_ENTRY * entry_p)
{
  if (entry_p)
    {
      hb_list_remove ((HB_LIST *) entry_p);
      free_and_init (entry_p);
    }
  return;
}

/*
 * hb_remove_all_procs() -
 *   return: none
 *
 *   first(in):
 */
static void
hb_remove_all_procs (HB_PROC_ENTRY * first)
{
  HB_PROC_ENTRY *proc, *next_proc;

  for (proc = first; proc; proc = next_proc)
    {
      next_proc = proc->next;
      hb_remove_proc (proc);
    }
}

/*
 * hb_return_proc_by_args() -
 *   return: pointer to resource process entry
 *
 *   args(in):
 */
static HB_PROC_ENTRY *
hb_return_proc_by_args (char *args)
{
  HB_PROC_ENTRY *proc;

  for (proc = hb_Resource->procs; proc; proc = proc->next)
    {
      if (strcmp (proc->args, args) == 0)
	{
	  return proc;
	}
    }

  return NULL;
}

/*
 * hb_return_proc_by_fd() -
 *   return: pointer to resource process entry
 *
 *   sfd(in):
 */
static HB_PROC_ENTRY *
hb_return_proc_by_fd (int sfd)
{
  HB_PROC_ENTRY *proc;

  for (proc = hb_Resource->procs; proc; proc = proc->next)
    {
      if (proc->sfd == sfd)
	{
	  return proc;
	}
    }

  return NULL;
}

/*
 * hb_proc_make_arg() -
 *   return: none
 *
 *   arg(out):
 *   argv(in):
 */
static void
hb_proc_make_arg (char **arg, char *argv)
{
  int i;

  for (i = 0; i < HB_MAX_NUM_PROC_ARGV;
       i++, arg++, argv += HB_MAX_SZ_PROC_ARGV)
    {
      if ((*argv == 0))
	{
	  break;
	}

      (*arg) = argv;
    }
  return;
}

/*
 * hb_find_proc_by_server_state() -
 *   return: pointer to resource process entry
 *
 *   server_state(in):
 */
static HB_PROC_ENTRY *
hb_find_proc_by_server_state (HA_STATE server_state)
{
  HB_PROC_ENTRY *proc;

  for (proc = hb_Resource->procs; proc; proc = proc->next)
    {
      if (proc->type == HB_PTYPE_SERVER
	  && proc->state == HB_PSTATE_REGISTERED
	  && proc->server_state == server_state)
	{
	  return proc;
	}
    }

  return NULL;
}


/*
 * resource process connection
 */

/*
 * hb_cleanup_conn_and_start_process() -
 *   return: none
 *
 *   conn(in):
 */
void
hb_cleanup_conn_and_start_process (CSS_CONN_ENTRY * conn)
{
  int error;
  char error_string[LINE_MAX] = "";
  HB_PROC_ENTRY *proc;
  HB_JOB_ARG *job_arg;
  SOCKET sfd;

  if (conn == NULL)
    {
      return;
    }

  sfd = conn->fd;
  css_remove_entry_by_conn (conn, &css_Master_socket_anchor);

  if (hb_Resource == NULL)
    {
      return;
    }

  pthread_mutex_lock (&hb_Resource->lock);
  proc = hb_return_proc_by_fd (sfd);
  if (proc == NULL)
    {
      pthread_mutex_unlock (&hb_Resource->lock);
      return;
    }

  proc->conn = NULL;
  proc->sfd = INVALID_SOCKET;

  if (proc->state < HB_PSTATE_REGISTERED)
    {
      er_log_debug (ARG_FILE_LINE, "unexpected process's state. "
		    "(fd:%d, pid:%d, state:%d, args:{%s}). \n", sfd,
		    proc->pid, proc->state, proc->args);

      pthread_mutex_unlock (&hb_Resource->lock);
      return;
    }

  gettimeofday (&proc->ktime, NULL);

  snprintf (error_string, LINE_MAX, "(pid:%d, args:%s)", proc->pid,
	    proc->args);

  if (proc->being_shutdown)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HB_PROCESS_EVENT, 2,
	      "Process shutdown detected", error_string);
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HB_PROCESS_EVENT, 2,
	      "Process failure detected", error_string);
    }

  if (hb_Resource->node_state == HA_STATE_MASTER
      && proc->type == HB_PTYPE_SERVER && hb_Cluster->is_isolated == false)
    {
      if (HB_GET_ELAPSED_TIME (proc->ktime, proc->rtime) <
	  prm_get_bigint_value (PRM_ID_HA_UNACCEPTABLE_PROC_RESTART_TIMEDIFF))
	{
	  /* demote the current node */
	  hb_Resource->node_state = HA_STATE_SLAVE;

	  snprintf (error_string, LINE_MAX, "(args:%s)", proc->args);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_HB_PROCESS_EVENT, 2,
		  "Process failure repeated within a short period of time. "
		  "The current node will be demoted", error_string);

	  /* shutdown working server processes to change its role to slave */
	  error =
	    hb_resource_job_queue (HB_RJOB_DEMOTE_START_SHUTDOWN, NULL,
				   HB_JOB_TIMER_IMMEDIATELY);
	  assert (error == NO_ERROR);
	}
    }

  job_arg = hb_alloc_resource_job_arg (proc->pid, proc->args,
				       prm_get_integer_value
				       (PRM_ID_HA_MAX_PROCESS_START_CONFIRM));
  if (job_arg == NULL)
    {
      pthread_mutex_unlock (&hb_Resource->lock);

      return;
    }

  proc->state = HB_PSTATE_DEAD;
  proc->server_state = HA_STATE_UNKNOWN;
  proc->server_hang = false;
  LSA_SET_NULL (&proc->prev_eof);
  LSA_SET_NULL (&proc->curr_eof);

  pthread_mutex_unlock (&hb_Resource->lock);

  shm_master_update_server_state (proc);

  error = hb_resource_job_queue (HB_RJOB_PROC_START, job_arg,
				 HB_JOB_TIMER_WAIT_A_SECOND);
  assert (error == NO_ERROR);

  return;
}

/*
 * hb_is_registered_processes() -
 *   return: none
 *
 *   conn(in):
 */
bool
hb_is_registered_processes (CSS_CONN_ENTRY * conn)
{
  HB_PROC_ENTRY *proc;
  bool is_all_registered = true;

  if (hb_Resource == NULL)
    {
      return false;
    }

  (void) pthread_mutex_lock (&hb_Resource->lock);
  if (hb_Resource->shutdown)
    {
      pthread_mutex_unlock (&hb_Resource->lock);
      css_remove_entry_by_conn (conn, &css_Master_socket_anchor);
      return false;
    }

  for (proc = hb_Resource->procs; proc; proc = proc->next)
    {
      if (proc->state < HB_PSTATE_REGISTERED)
	{
	  is_all_registered = false;
	  break;
	}
    }

  (void) pthread_mutex_unlock (&hb_Resource->lock);

  return is_all_registered;
}

/*
 * hb_resource_register_new_proc()-
 *    return: NO_ERROR or ER_FAILED
 *
 *    proc_reg(in):
 *    conn(in):
 */
int
hb_resource_register_new_proc (HBP_PROC_REGISTER * proc_reg,
			       CSS_CONN_ENTRY * conn)
{
  char err_msg[LINE_MAX] = "";
  HB_PROC_ENTRY *proc = NULL;
  int proc_state;
  int error = NO_ERROR;
  HB_JOB_ARG *job_arg = NULL;

  if (hb_Resource == NULL)
    {
      return ER_FAILED;
    }

  pthread_mutex_lock (&hb_Resource->lock);
  if (hb_Resource->shutdown)
    {
      pthread_mutex_unlock (&hb_Resource->lock);

      return ER_FAILED;
    }

  proc = hb_return_proc_by_args (proc_reg->args);
  if (proc == NULL)
    {
      proc = hb_alloc_new_proc ();
      if (proc == NULL)
	{
	  error = ER_FAILED;
	  GOTO_EXIT_ON_ERROR;
	}

      proc_state = HB_PSTATE_REGISTERED;	/* first register */
      proc->frtime.tv_sec = 0;
      proc->frtime.tv_usec = 0;
    }
  else
    {
      if (proc->state != HB_PSTATE_STARTED)
	{
	  /* already registered */
	  proc_state = HB_PSTATE_UNKNOWN;
	  error = ER_FAILED;
	  GOTO_EXIT_ON_ERROR;
	}

      /* restarted by heartbeat */
      proc_state = HB_PSTATE_NOT_REGISTERED;
      if (proc->pid != (int) proc_reg->pid || kill (proc->pid, 0) != 0)
	{
	  error = ER_FAILED;
	  GOTO_EXIT_ON_ERROR;
	}
    }

  assert (proc_state == HB_PSTATE_REGISTERED
	  || proc_state == HB_PSTATE_NOT_REGISTERED);

  proc->state = proc_state;
  if (conn != NULL)
    {
      proc->sfd = conn->fd;
      proc->conn = conn;
    }
  else
    {
      proc->sfd = INVALID_SOCKET;
      proc->conn = NULL;
    }
  gettimeofday (&proc->rtime, NULL);
  proc->changemode_gap = 0;
  proc->server_hang = false;

  proc->sync_groupid_bitmap = false;
  hb_resource_job_queue (HB_RJOB_CHANGE_GROUPID_BITMAP, NULL,
			 HB_JOB_TIMER_IMMEDIATELY);

  if (proc->state == HB_PSTATE_REGISTERED)
    {
      proc->pid = proc_reg->pid;
      proc->type = proc_reg->type;
      if (proc->type == HB_PTYPE_SERVER)
	{
	  proc->server_state = HA_STATE_SLAVE;
	}
      memcpy ((void *) &proc->exec_path[0],
	      (void *) &proc_reg->exec_path[0], sizeof (proc->exec_path));
      memcpy ((void *) &proc->args[0],
	      (void *) &proc_reg->args[0], sizeof (proc->args));
      memcpy ((void *) &proc->argv[0],
	      (void *) &proc_reg->argv[0], sizeof (proc->argv));
      hb_Resource->num_procs++;

      job_arg = hb_alloc_resource_job_arg (proc->pid, proc->args,
					   prm_get_integer_value
					   (PRM_ID_HA_MAX_PROCESS_START_CONFIRM));
      if (job_arg == NULL)
	{
	  hb_remove_proc (proc);
	  proc = NULL;

	  error = er_errid ();
	  GOTO_EXIT_ON_ERROR;
	}

      proc->state = HB_PSTATE_DEAD;
      proc->server_state = HA_STATE_UNKNOWN;
      proc->server_hang = false;
      LSA_SET_NULL (&proc->prev_eof);
      LSA_SET_NULL (&proc->curr_eof);
    }

  snprintf (err_msg, LINE_MAX,
	    "%s (pid:%d, proc state:%s, server state:%s, args:%s)",
	    HB_RESULT_SUCCESS_STR, proc_reg->pid,
	    hb_process_state_string (proc->state),
	    HA_STATE_NAME (proc->server_state), proc_reg->args);
  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_HB_PROCESS_EVENT, 2,
	  "Registered as local process entries", err_msg);

  pthread_mutex_unlock (&hb_Resource->lock);

  shm_master_update_server_state (proc);

  if (job_arg != NULL)
    {
      error = hb_resource_job_queue (HB_RJOB_PROC_START, job_arg,
				     HB_JOB_TIMER_WAIT_A_SECOND);
      assert (error == NO_ERROR);
    }

  return NO_ERROR;

exit_on_error:

  assert (error != NO_ERROR);

  pthread_mutex_unlock (&hb_Resource->lock);

  if (proc != NULL)
    {
      snprintf (err_msg, LINE_MAX,
		"%s (expected pid: %d, pid:%d, proc state:%s, server state:%s, args:%s)",
		HB_RESULT_FAILURE_STR, proc->pid, proc_reg->pid,
		hb_process_state_string (proc->state),
		HA_STATE_NAME (proc->server_state), proc_reg->args);
    }
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HB_PROCESS_EVENT, 2,
	  "Registered as local process entries", err_msg);

  return ER_FAILED;
}

/*
 * hb_resource_sync_server_state -
 *   return: none
 *
 *   proc(in):
 */
static int
hb_resource_sync_server_state (HB_PROC_ENTRY * proc, bool force)
{
  int error = NO_ERROR;
  int server_state;
  int sig = 0;
  char error_string[LINE_MAX] = "";
  int request[2];

  if (proc->conn == NULL)
    {
      return ER_FAILED;
    }

  if (proc->changemode_gap == HB_MAX_CHANGEMODE_DIFF_TO_TERM)
    {
      sig = SIGTERM;
    }
  else if (proc->changemode_gap >= HB_MAX_CHANGEMODE_DIFF_TO_KILL)
    {
      sig = SIGKILL;
    }

  if (sig)
    {
      assert (proc->pid > 0);
      if (proc->pid > 0 && (kill (proc->pid, 0) == 0 || errno != ESRCH))
	{
	  snprintf (error_string, sizeof (error_string),
		    "process does not respond for a long time. kill pid %d signal %d.",
		    proc->pid, sig);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_HB_PROCESS_EVENT, 2, "Process failure detected",
		  error_string);
	  kill (proc->pid, sig);
	}
      return ER_FAILED;
    }

  switch (hb_Resource->node_state)
    {
    case HA_STATE_MASTER:
      server_state = HA_STATE_MASTER;
      break;
    case HA_STATE_TO_BE_SLAVE:
    case HA_STATE_SLAVE:
      server_state = HA_STATE_SLAVE;
      break;
    default:
      return ER_FAILED;
    }

  /* request[0]: server state, request[1]: force */
  request[0] = server_state;
  request[1] = force;
  error = css_send_heartbeat_request (proc->conn, SERVER_CHANGE_HA_MODE, 1,
				      (char *) request, sizeof (request));
  if (NO_ERRORS != error)
    {
      snprintf (error_string, LINE_MAX,
		"Failed to send changemode request to the server. "
		"(state:%d[%s], args:[%s], pid:%d)",
		server_state, HA_STATE_NAME (server_state), proc->args,
		proc->pid);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HA_GENERIC_ERROR, 1,
	      error_string);

      return ER_FAILED;
    }

  snprintf (error_string, LINE_MAX,
	    "Send changemode request to the server. "
	    "(state:%d[%s], args:[%s], pid:%d)",
	    server_state, HA_STATE_NAME (server_state), proc->args,
	    proc->pid);
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HA_GENERIC_ERROR, 1,
	  error_string);

  return NO_ERROR;
}

/*
 * hb_resource_receive_changemode -
 *   return: none
 *
 *   conn(in):
 */
void
hb_resource_receive_changemode (CSS_CONN_ENTRY * conn, int server_state)
{
  int sfd;
  HB_PROC_ENTRY *proc;
  char error_string[LINE_MAX] = "";

  if (hb_Resource == NULL)
    {
      return;
    }

  sfd = conn->fd;
  pthread_mutex_lock (&hb_Cluster->lock);
  pthread_mutex_lock (&hb_Resource->lock);
  proc = hb_return_proc_by_fd (sfd);
  if (proc == NULL || proc->state == HB_PSTATE_DEREGISTERED)
    {
      pthread_mutex_unlock (&hb_Resource->lock);
      pthread_mutex_unlock (&hb_Cluster->lock);
      return;
    }

  snprintf (error_string, LINE_MAX,
	    "Receive changemode response from the server. "
	    "(server_state:%d[%s], args:[%s], pid:%d)", server_state,
	    HA_STATE_NAME (server_state), proc->args, proc->pid);
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HA_GENERIC_ERROR, 1,
	  error_string);

  if (proc->state != HB_PSTATE_REGISTERED)
    {
      assert (false);
      pthread_mutex_unlock (&hb_Resource->lock);
      pthread_mutex_unlock (&hb_Cluster->lock);
      return;
    }

  proc->server_state = server_state;

  proc->changemode_gap = 0;

  pthread_mutex_unlock (&hb_Resource->lock);
  pthread_mutex_unlock (&hb_Cluster->lock);

  shm_master_update_server_state (proc);

  return;
}

/*
 * hb_resource_check_server_log_grow() -
 *      check if active server is alive
 *   return: none
 *
 */
static bool
hb_resource_check_server_log_grow (void)
{
  int dead_cnt = 0;
  HB_PROC_ENTRY *proc;

  for (proc = hb_Resource->procs; proc; proc = proc->next)
    {
      if (!HB_PROC_IS_MASTER_SERVER (proc)
	  || proc->server_state != HA_STATE_MASTER
	  || proc->server_hang == true)
	{
	  continue;
	}

      if (LSA_ISNULL (&proc->curr_eof) == true)
	{
	  continue;
	}

      if (LSA_GT (&proc->curr_eof, &proc->prev_eof) == true)
	{
	  LSA_COPY (&proc->prev_eof, &proc->curr_eof);
	}
      else
	{
	  proc->server_hang = true;
	  dead_cnt++;
	}
    }

  if (dead_cnt > 0)
    {
      return false;
    }

  return true;
}

/*
 * hb_resource_get_server_eof -
 *   return: none
 *
 *   proc(in):
 */
static void
hb_resource_get_server_eof (void)
{
  LOG_LSA eof_lsa;
  HB_PROC_ENTRY *proc;
  int error = NO_ERROR;

  if (hb_Resource->node_state != HA_STATE_MASTER)
    {
      return;
    }

  error = FI_TEST_ARG_INT (NULL, FI_TEST_HB_SLOW_DISK, 30, 0);
  if (error != NO_ERROR)
    {
      return;
    }

  for (proc = hb_Resource->procs; proc; proc = proc->next)
    {
      if (!HB_PROC_IS_MASTER_SERVER (proc)
	  || proc->server_state != HA_STATE_MASTER)
	{
	  continue;
	}

      error = rye_server_shm_get_eof_lsa (&eof_lsa, proc->argv[1]);
      if (error != NO_ERROR || LSA_ISNULL (&eof_lsa))
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR,
		  1, "fail get_eof_lsa");
	  break;
	}

      LSA_COPY (&proc->curr_eof, &eof_lsa);
    }

  return;
}

/*
 * heartbeat worker threads
 */

/*
 * hb_thread_cluster_worker -
 *   return: none
 *
 *   arg(in):
 */
static void *
hb_thread_cluster_worker (UNUSED_ARG void *arg)
{
  HB_JOB_ENTRY *job;
  struct hb_request *req_p;
  ER_MSG_INFO *er_msg;

  er_msg = malloc (sizeof (ER_MSG_INFO));
  if (er_set_msg_info (er_msg) != NO_ERROR)
    {
      assert (false);
      return NULL;
    }

  while (cluster_JobQ->shutdown == false)
    {
      while ((job = hb_cluster_job_dequeue ()) != NULL)
	{
	  req_p = &hb_Requests[job->type];

	  assert (req_p->req_func != NULL);
	  if (req_p->req_func != NULL)
	    {
	      req_p->req_func (job->arg);
	    }
	  free_and_init (job);
	}

      THREAD_SLEEP (10);
    }

  return NULL;
}

/*
 * hb_thread_cluster_reader -
 *   return: none
 *
 *   arg(in):
 */
static void *
hb_thread_cluster_reader (UNUSED_ARG void *arg)
{
  int error;
  ER_MSG_INFO *er_msg;
  SOCKET sfd;
  char buffer[HB_BUFFER_SZ + MAX_ALIGNMENT], *aligned_buffer;
  int len;
  struct pollfd po[1] = {
    {
     0, 0, 0}
  };

  struct sockaddr_in from;
  socklen_t from_len;

  er_msg = malloc (sizeof (ER_MSG_INFO));
  error = er_set_msg_info (er_msg);
  if (error != NO_ERROR)
    {
      assert (false);
      return NULL;
    }

  aligned_buffer = PTR_ALIGN (buffer, MAX_ALIGNMENT);
  sfd = hb_Cluster->sfd;
  while (hb_Cluster->shutdown == false)
    {
      po[0].fd = sfd;
      po[0].events = POLLIN;
      error = poll (po, 1, 1);
      if (error <= 0)
	{
	  continue;
	}

      if ((po[0].revents & POLLIN) && sfd == hb_Cluster->sfd)
	{
	  from_len = sizeof (from);
	  len =
	    recvfrom (sfd, (void *) aligned_buffer, HB_BUFFER_SZ,
		      0, (struct sockaddr *) &from, &from_len);
	  if (len > 0)
	    {
	      hb_cluster_receive_heartbeat (aligned_buffer, len, &from,
					    from_len);
	    }

	  error =
	    FI_TEST_ARG_INT (NULL, FI_TEST_HB_SLOW_HEARTBEAT_MESSAGE, 5000,
			     0);
	}
    }

  return NULL;
}

/*
 * hb_thread_resource_worker -
 *   return: none
 *
 *   arg(in):
 */
static void *
hb_thread_resource_worker (UNUSED_ARG void *arg)
{
  HB_JOB_ENTRY *job;
  struct hb_request *req_p;
  ER_MSG_INFO *er_msg;

  er_msg = malloc (sizeof (ER_MSG_INFO));
  if (er_set_msg_info (er_msg) != NO_ERROR)
    {
      assert (false);
      return NULL;
    }

  while (resource_JobQ->shutdown == false)
    {
      while ((job = hb_resource_job_dequeue ()) != NULL)
	{
	  req_p = &hb_Requests[job->type];

	  assert (req_p->req_func != NULL);
	  if (req_p->req_func != NULL)
	    {
	      req_p->req_func (job->arg);
	    }
	  free_and_init (job);
	}

      THREAD_SLEEP (10);
    }

  return NULL;
}

#if 0
/*
 * hb_thread_worker -
 *   return: none
 *
 *   arg(in):
 */
static void *
hb_thread_worker (UNUSED_ARG void *arg)
{
  HBNEW_JOB_ENTRY new_job;
  ER_MSG_INFO *er_msg;

  er_msg = malloc (sizeof (ER_MSG_INFO));
  if (er_set_msg_info (er_msg) != NO_ERROR)
    {
      assert (false);
      return NULL;
    }

  while (hb_Job_queue.shutdown == false)
    {
      if (hb_get_new_job (&new_job) != NO_ERROR)
	{
	  continue;
	}

      assert (new_job.func != NULL);
      if (new_job.func != NULL)
	{
	  new_job.func (&new_job.arg);
	}
    }

  return NULL;
}

/*
 * hb_get_new_job() - fetch a job from the queue or cond wait
 *   return: ER_FILED - no new job, NO_ERROR - fetch a job
 *
 *   ret_job_entry(out):
 */
static int
hb_get_new_job (HBNEW_JOB_ENTRY * ret_job_entry)
{
  HBNEW_JOB_ENTRY *new_job;

  pthread_mutex_lock (&hb_Job_queue.job_lock);

  hb_Job_queue.num_run_threads--;

  while (hb_Job_queue.shutdown == false)
    {
      new_job =
	(HBNEW_JOB_ENTRY *) Rye_queue_dequeue (&hb_Job_queue.job_list);
      if (new_job != NULL)
	{
	  break;
	}
      pthread_cond_wait (&hb_Job_queue.job_cond, &hb_Job_queue.job_lock);
    }

  hb_Job_queue.num_run_threads++;

  pthread_mutex_unlock (&hb_Job_queue.job_lock);

  if (new_job == NULL)
    {
      return ER_FAILED;
    }

  *ret_job_entry = *new_job;

  free (new_job);

  return NO_ERROR;
}

/*
 * hb_append_new_job -
 *   return:
 *
 *   new_job(in):
 */
static int
hb_append_new_job (HBNEW_JOB_ENTRY * new_job)
{
  pthread_mutex_lock (&hb_Job_queue.job_lock);

  if (Rye_queue_enqueue (&hb_Job_queue.job_list, new_job) == NULL)
    {
      pthread_mutex_unlock (&hb_Job_queue.job_lock);
      return ER_FAILED;
    }

  pthread_cond_signal (&hb_Job_queue.job_cond);

  pthread_mutex_unlock (&hb_Job_queue.job_lock);

  return NO_ERROR;
}
#endif

/*
 * hb_thread_resource_worker -
 *   return: none
 *
 *   arg(in):
 */
static void *
hb_thread_check_disk_failure (UNUSED_ARG void *arg)
{
  int error;
  ER_MSG_INFO *er_msg;
  int interval;
  INT64 remaining_time_msecs = 0;
  bool need_demote_shutdown;

  er_msg = malloc (sizeof (ER_MSG_INFO));
  error = er_set_msg_info (er_msg);
  if (error != NO_ERROR)
    {
      assert (false);
      return NULL;
    }

  while (hb_Resource->shutdown == false)
    {
      need_demote_shutdown = false;

      interval = prm_get_bigint_value (PRM_ID_HA_CHECK_DISK_FAILURE_INTERVAL);
      if (interval > 0 && remaining_time_msecs <= 0)
	{
	  pthread_mutex_lock (&css_Master_socket_anchor_lock);
	  pthread_mutex_lock (&hb_Cluster->lock);
	  pthread_mutex_lock (&hb_Resource->lock);

	  if (hb_Cluster->is_isolated == false
	      && hb_Resource->node_state == HA_STATE_MASTER)
	    {
	      if (hb_resource_check_server_log_grow () == false)
		{
		  /* be silent to avoid blocking write operation on disk */
		  hb_disable_er_log (HB_NOLOG_DEMOTE_ON_DISK_FAIL, NULL);
		  hb_Resource->node_state = HA_STATE_SLAVE;

		  need_demote_shutdown = true;
		}
	    }

	  if (hb_Resource->node_state == HA_STATE_MASTER)
	    {
	      hb_resource_get_server_eof ();
	    }

	  pthread_mutex_unlock (&hb_Resource->lock);
	  pthread_mutex_unlock (&hb_Cluster->lock);
	  pthread_mutex_unlock (&css_Master_socket_anchor_lock);

	  if (need_demote_shutdown == true)
	    {
	      error = hb_resource_job_queue (HB_RJOB_DEMOTE_START_SHUTDOWN,
					     NULL, HB_JOB_TIMER_IMMEDIATELY);
	      assert (error == NO_ERROR);
	    }

	  remaining_time_msecs = interval;
	}

      THREAD_SLEEP (HB_DISK_FAILURE_CHECK_TIMER);
      if (interval > 0)
	{
	  remaining_time_msecs -= HB_DISK_FAILURE_CHECK_TIMER;
	}
    }

  return NULL;
}

/*
 * hb_thread_check_groupid_bitmap -
 *   return: none
 *
 *   arg(in):
 */
static void *
hb_thread_check_groupid_bitmap (UNUSED_ARG void *arg)
{
  HB_PROC_ENTRY *proc;
  ER_MSG_INFO *er_msg;

  er_msg = malloc (sizeof (ER_MSG_INFO));
  if (er_set_msg_info (er_msg) != NO_ERROR)
    {
      return NULL;
    }

  while (hb_Resource->shutdown == false)
    {
      for (proc = hb_Resource->procs; proc; proc = proc->next)
	{
	  if (proc->type == HB_PTYPE_SERVER
	      && proc->sync_groupid_bitmap == false)
	    {
	      break;
	    }
	}

      if (proc != NULL)
	{
	  hb_resource_job_queue (HB_RJOB_CHANGE_GROUPID_BITMAP, NULL,
				 HB_JOB_TIMER_IMMEDIATELY);
	}

      THREAD_SLEEP (1000);
    }

  return NULL;
}

/*
 * master heartbeat initializer
 */

/*
 * hb_cluster_job_initialize -
 *   return: NO_ERROR or ER_FAILED
 *
 */
static int
hb_cluster_job_initialize (void)
{
  int error;

  if (cluster_JobQ == NULL)
    {
      cluster_JobQ = (HB_JOB *) malloc (sizeof (HB_JOB));
      if (cluster_JobQ == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (HB_JOB));
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      pthread_mutex_init (&cluster_JobQ->lock, NULL);
    }

  pthread_mutex_lock (&cluster_JobQ->lock);
  cluster_JobQ->shutdown = false;
  cluster_JobQ->num_jobs = 0;
  cluster_JobQ->jobs = NULL;
  pthread_mutex_unlock (&cluster_JobQ->lock);

  error = hb_cluster_job_queue (HB_CJOB_INIT, NULL, HB_JOB_TIMER_IMMEDIATELY);
  if (error != NO_ERROR)
    {
      assert (false);
      return ER_FAILED;
    }

  return NO_ERROR;
}


/*
 * hb_cluster_initialize -
 *   return: NO_ERROR or ER_FAILED
 *
 */
static int
hb_cluster_initialize ()
{
  struct sockaddr_in udp_saddr;
  PRM_NODE_INFO my_node_info;

  if (hb_Cluster == NULL)
    {
      hb_Cluster = (HB_CLUSTER *) malloc (sizeof (HB_CLUSTER));
      if (hb_Cluster == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (HB_CLUSTER));
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      memset (hb_Cluster, 0, sizeof (HB_CLUSTER));

      pthread_mutex_init (&hb_Cluster->lock, NULL);
    }

  my_node_info = prm_get_myself_node_info ();
  if (PRM_NODE_INFO_GET_IP (&my_node_info) == INADDR_NONE)
    {
      er_log_debug (ARG_FILE_LINE, "Failed to resolve IP address");
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_BO_UNABLE_TO_FIND_HOSTNAME, 0);
      return ER_BO_UNABLE_TO_FIND_HOSTNAME;
    }

  pthread_mutex_lock (&hb_Cluster->lock);
  hb_Cluster->shutdown = false;
  hb_Cluster->hide_to_demote = false;
  hb_Cluster->is_isolated = false;
  hb_Cluster->is_ping_check_enabled = true;
  hb_Cluster->sfd = INVALID_SOCKET;
  hb_Cluster->my_node_info = my_node_info;
  if (prm_get_integer_value (PRM_ID_HA_MODE) == HA_MODE_REPLICA)
    {
      hb_Cluster->node_state = HA_STATE_REPLICA;
    }
  else
    {
      hb_Cluster->node_state = HA_STATE_SLAVE;
    }
  hb_Cluster->master = NULL;
  hb_Cluster->myself = NULL;
  hb_Cluster->nodes = NULL;

  hb_Cluster->ping_hosts = NULL;

  hb_Cluster->num_nodes = hb_cluster_load_group_and_node_list ();
  if (hb_Cluster->num_nodes < 1)
    {
      er_log_debug (ARG_FILE_LINE,
		    "hb_Cluster->num_nodes is smaller than '1'. (num_nodes=%d). \n",
		    hb_Cluster->num_nodes);
      pthread_mutex_unlock (&hb_Cluster->lock);

      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_PRM_BAD_VALUE, 1,
	      prm_get_name (PRM_ID_HA_NODE_LIST));
      return ER_PRM_BAD_VALUE;
    }

  hb_Cluster->num_ping_hosts =
    hb_cluster_load_ping_host_list (prm_get_string_value
				    (PRM_ID_HA_PING_HOSTS));

  if (hb_cluster_check_valid_ping_server () == false)
    {
      char buf[LINE_MAX];

      snprintf (buf, sizeof (buf),
		"Validity check for PING failed on all hosts ");
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_HB_NODE_EVENT,
	      1, buf);
    }

  /* initialize udp socket */
  hb_Cluster->sfd = socket (AF_INET, SOCK_DGRAM, 0);
  if (hb_Cluster->sfd < 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ERR_CSS_TCP_DATAGRAM_SOCKET, 0);
      pthread_mutex_unlock (&hb_Cluster->lock);
      return ERR_CSS_TCP_DATAGRAM_SOCKET;
    }

  memset ((void *) &udp_saddr, 0, sizeof (udp_saddr));
  udp_saddr.sin_family = AF_INET;
  udp_saddr.sin_addr.s_addr = htonl (INADDR_ANY);
  udp_saddr.sin_port = htons (prm_get_rye_port_id ());

  if (bind (hb_Cluster->sfd, (struct sockaddr *) &udp_saddr,
	    sizeof (udp_saddr)) < 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ERR_CSS_TCP_DATAGRAM_BIND, 0);
      pthread_mutex_unlock (&hb_Cluster->lock);
      return ERR_CSS_TCP_DATAGRAM_BIND;
    }

  hb_shm_reset_hb_node ();

  pthread_mutex_unlock (&hb_Cluster->lock);

  return NO_ERROR;
}

/*
 * hb_resource_initialize -
 *   return: NO_ERROR or ER_FAILED
 *
 */
static int
hb_resource_initialize (void)
{
  if (hb_Resource == NULL)
    {
      hb_Resource = (HB_RESOURCE *) malloc (sizeof (HB_RESOURCE));
      if (hb_Resource == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (HB_RESOURCE));
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      pthread_mutex_init (&hb_Resource->lock, NULL);
    }

  pthread_mutex_lock (&hb_Resource->lock);
  hb_Resource->shutdown = false;
  hb_Resource->node_state = HA_STATE_SLAVE;
  hb_Resource->num_procs = 0;
  hb_Resource->procs = NULL;
  pthread_mutex_unlock (&hb_Resource->lock);

  return NO_ERROR;
}

/*
 * hb_job_request_initialize ()
 */
static int
hb_job_request_initialize ()
{
  struct hb_request *req_p;
  int i;

  for (i = 0; i < (int) DIM (hb_Requests); i++)
    {
      hb_Requests[i].req_func = NULL;
      hb_Requests[i].name = "";
    }

  /*
   * hb cluster job functions
   */

  /* hb_cluster_job_init */
  req_p = &hb_Requests[HB_CJOB_INIT];
  req_p->req_func = hb_cluster_job_init;
  req_p->name = "HB_CJOB_INIT";

  /* hb_cluster_job_heartbeat */
  req_p = &hb_Requests[HB_CJOB_HEARTBEAT];
  req_p->req_func = hb_cluster_job_heartbeat;
  req_p->name = "HB_CJOB_HEARTBEAT";

  /* hb_cluster_job_calc_score */
  req_p = &hb_Requests[HB_CJOB_CALC_SCORE];
  req_p->req_func = hb_cluster_job_calc_score;
  req_p->name = "HB_CJOB_CALC_SCORE";

  /* hb_cluster_job_check_ping */
  req_p = &hb_Requests[HB_CJOB_CHECK_PING];
  req_p->req_func = hb_cluster_job_check_ping;
  req_p->name = "HB_CJOB_CHECK_PING";

  /* hb_cluster_job_failover */
  req_p = &hb_Requests[HB_CJOB_FAILOVER];
  req_p->req_func = hb_cluster_job_failover;
  req_p->name = "HB_CJOB_FAILOVER";

  /* hb_cluster_job_failback */
  req_p = &hb_Requests[HB_CJOB_FAILBACK];
  req_p->req_func = hb_cluster_job_failback;
  req_p->name = "HB_CJOB_FAILBACK";

  /* hb_cluster_job_change_slave */
  req_p = &hb_Requests[HB_CJOB_CHANGE_SLAVE];
  req_p->req_func = hb_cluster_job_changeslave;
  req_p->name = "HB_CJOB_CHANGE_SLAVE";

  /* hb_cluster_job_changemode_force */
  req_p = &hb_Requests[HB_CJOB_CHANGEMODE_FORCE];
  req_p->req_func = hb_cluster_job_changemode_force;
  req_p->name = "HB_CJOB_CHANGEMODE_FORCE";

  /* hb_cluster_job_check_valid_ping_server */
  req_p = &hb_Requests[HB_CJOB_CHECK_VALID_PING_SERVER];
  req_p->req_func = hb_cluster_job_check_valid_ping_server;
  req_p->name = "HB_CJOB_CHECK_VALID_PING_SERVER";

  /* hb_cluster_job_demote */
  req_p = &hb_Requests[HB_CJOB_DEMOTE];
  req_p->req_func = hb_cluster_job_demote;
  req_p->name = "HB_CJOB_DEMOTE";

  /*
   * hb resource job functions
   */

  /* hb_resource_job_proc_start */
  req_p = &hb_Requests[HB_RJOB_PROC_START];
  req_p->req_func = hb_resource_job_proc_start;
  req_p->name = "HB_RJOB_PROC_START";

  /* hb_resource_job_confirm_start */
  req_p = &hb_Requests[HB_RJOB_CONFIRM_START];
  req_p->req_func = hb_resource_job_confirm_start;
  req_p->name = "HB_RJOB_CONFIRM_START";

  /* hb_resource_job_sync_server_state */
  req_p = &hb_Requests[HB_RJOB_SYNC_SERVER_STATE];
  req_p->req_func = hb_resource_job_sync_server_state;
  req_p->name = "HB_RJOB_SYNC_SERVER_STATE";

  /* hb_resource_job_demote_start_shutdown */
  req_p = &hb_Requests[HB_RJOB_DEMOTE_START_SHUTDOWN];
  req_p->req_func = hb_resource_job_demote_start_shutdown;
  req_p->name = "HB_RJOB_DEMOTE_START_SHUTDOWN";

  /* hb_resource_job_demote_confirm_shutdown */
  req_p = &hb_Requests[HB_RJOB_DEMOTE_CONFIRM_SHUTDOWN];
  req_p->req_func = hb_resource_job_demote_confirm_shutdown;
  req_p->name = "HB_RJOB_DEMOTE_CONFIRM_SHUTDOWN";

  /* hb_resource_job_cleanup_all */
  req_p = &hb_Requests[HB_RJOB_CLEANUP_ALL];
  req_p->req_func = hb_resource_job_cleanup_all;
  req_p->name = "HB_RJOB_CLEANUP_ALL";

  /* hb_resource_job_confirm_cleanup_all */
  req_p = &hb_Requests[HB_RJOB_CONFIRM_CLEANUP_ALL];
  req_p->req_func = hb_resource_job_confirm_cleanup_all;
  req_p->name = "HB_RJOB_CONFIRM_CLEANUP_ALL";

  /* hb_resource_job_change_groupid_bitmap */
  req_p = &hb_Requests[HB_RJOB_CHANGE_GROUPID_BITMAP];
  req_p->req_func = hb_resource_job_change_groupid_bitmap;
  req_p->name = "HB_RJOB_CHANGE_GROUPID_BITMAP";

  /* hb_resource_job_reload_nodes */
  req_p = &hb_Requests[HB_RJOB_RELOAD_NODES];
  req_p->req_func = hb_resource_job_reload_nodes;
  req_p->name = "HB_RJOB_RELOAD_NODES";

  return NO_ERROR;
}

/*
 * hb_resource_job_initialize -
 *   return: NO_ERROR or ER_FAILED
 *
 */
static int
hb_resource_job_initialize ()
{
  int error;

  if (resource_JobQ == NULL)
    {
      resource_JobQ = (HB_JOB *) malloc (sizeof (HB_JOB));
      if (resource_JobQ == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (HB_JOB));
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      pthread_mutex_init (&resource_JobQ->lock, NULL);
    }

  pthread_mutex_lock (&resource_JobQ->lock);
  resource_JobQ->shutdown = false;
  resource_JobQ->num_jobs = 0;
  resource_JobQ->jobs = NULL;
  pthread_mutex_unlock (&resource_JobQ->lock);

  error = hb_resource_job_queue (HB_RJOB_SYNC_SERVER_STATE, NULL,
				 prm_get_bigint_value
				 (PRM_ID_HA_INIT_TIMER) +
				 prm_get_bigint_value
				 (PRM_ID_HA_FAILOVER_WAIT_TIME));
  if (error != NO_ERROR)
    {
      assert (false);
      return ER_FAILED;
    }

  return NO_ERROR;
}

/*
 * hb_thread_initialize -
 *   return: NO_ERROR or ER_FAILED
 *
 */
static int
hb_thread_initialize (void)
{
  int rv;

  pthread_attr_t thread_attr;
#if defined(_POSIX_THREAD_ATTR_STACKSIZE)
  size_t ts_size;
#endif
//  pthread_t worker_th;
  pthread_t cluster_worker_th;
  pthread_t resource_worker_th;
  pthread_t check_disk_failure_th;
  pthread_t check_groupid_bitmap_th;
//  int i, max_workers = 10;

  rv = pthread_attr_init (&thread_attr);
  if (rv != 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_CSS_PTHREAD_ATTR_INIT, 0);
      return ER_CSS_PTHREAD_ATTR_INIT;
    }

  rv = pthread_attr_setdetachstate (&thread_attr, PTHREAD_CREATE_DETACHED);
  if (rv != 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_CSS_PTHREAD_ATTR_SETDETACHSTATE, 0);
      return ER_CSS_PTHREAD_ATTR_SETDETACHSTATE;
    }

  rv = pthread_attr_setscope (&thread_attr, PTHREAD_SCOPE_SYSTEM);
  if (rv != 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_CSS_PTHREAD_ATTR_SETSCOPE, 0);
      return ER_CSS_PTHREAD_ATTR_SETSCOPE;
    }

  /* Sun Solaris allocates 1M for a thread stack, and it is quite enough */
#if defined(_POSIX_THREAD_ATTR_STACKSIZE)
  rv = pthread_attr_getstacksize (&thread_attr, &ts_size);
  if (ts_size < (size_t) prm_get_bigint_value (PRM_ID_THREAD_STACKSIZE))
    {
      rv = pthread_attr_setstacksize (&thread_attr,
				      prm_get_bigint_value
				      (PRM_ID_THREAD_STACKSIZE));
      if (rv != 0)
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_CSS_PTHREAD_ATTR_SETSTACKSIZE, 0);
	  return ER_CSS_PTHREAD_ATTR_SETSTACKSIZE;
	}

      pthread_attr_getstacksize (&thread_attr, &ts_size);
    }
#endif /* _POSIX_THREAD_ATTR_STACKSIZE */

#if 0
  /* don't remove:happy */
  for (i = 0; i < max_workers; i++)
    {
      rv = pthread_create (&worker_th, &thread_attr, hb_thread_worker, NULL);
      if (rv != 0)
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_CSS_PTHREAD_CREATE, 0);
	  return ER_CSS_PTHREAD_CREATE;
	}
    }
#endif

  rv = pthread_create (&cluster_worker_th, &thread_attr,
		       hb_thread_cluster_reader, NULL);
  if (rv != 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_CSS_PTHREAD_CREATE, 0);
      return ER_CSS_PTHREAD_CREATE;
    }

  rv = pthread_create (&cluster_worker_th, &thread_attr,
		       hb_thread_cluster_worker, NULL);
  if (rv != 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_CSS_PTHREAD_CREATE, 0);
      return ER_CSS_PTHREAD_CREATE;
    }

  rv = pthread_create (&resource_worker_th, &thread_attr,
		       hb_thread_resource_worker, NULL);
  if (rv != 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_CSS_PTHREAD_CREATE, 0);
      return ER_CSS_PTHREAD_CREATE;
    }

  rv = pthread_create (&check_disk_failure_th, &thread_attr,
		       hb_thread_check_disk_failure, NULL);
  if (rv != 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_CSS_PTHREAD_CREATE, 0);
      return ER_CSS_PTHREAD_CREATE;
    }

  rv = pthread_create (&check_groupid_bitmap_th, &thread_attr,
		       hb_thread_check_groupid_bitmap, NULL);
  if (rv != 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_CSS_PTHREAD_CREATE, 0);
      return ER_CSS_PTHREAD_CREATE;
    }

  /* destroy thread_attribute */
  rv = pthread_attr_destroy (&thread_attr);
  if (rv != 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_CSS_PTHREAD_ATTR_DESTROY, 0);
      return ER_CSS_PTHREAD_ATTR_DESTROY;
    }

  return NO_ERROR;
}

/*
 * hb_master_init -
 *   return: NO_ERROR or ER_FAILED,...
 *
 */
int
hb_master_init (void)
{
  int error;

  hb_enable_er_log ();

  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_HB_STARTED, 0);

  sysprm_reload_and_init (NULL);
  error = hb_cluster_initialize ();
  if (error != NO_ERROR)
    {
      er_log_debug (ARG_FILE_LINE, "hb_cluster_initialize failed. "
		    "(error=%d). \n", error);
      util_log_write_errstr ("%s\n", db_error_string (3));
      goto error_return;
    }

  error = hb_cluster_job_initialize ();
  if (error != NO_ERROR)
    {
      er_log_debug (ARG_FILE_LINE, "hb_cluster_job_initialize failed. "
		    "(error=%d). \n", error);
      util_log_write_errstr ("%s\n", db_error_string (3));
      goto error_return;
    }

  error = hb_resource_initialize ();
  if (error != NO_ERROR)
    {
      er_log_debug (ARG_FILE_LINE, "hb_resource_initialize failed. "
		    "(error=%d). \n", error);
      util_log_write_errstr ("%s\n", db_error_string (3));
      goto error_return;
    }

  error = hb_job_request_initialize ();
  if (error != NO_ERROR)
    {
      er_log_debug (ARG_FILE_LINE,
		    "hb_job_request_initialize failed. "
		    "(error=%d). \n", error);
      util_log_write_errstr ("%s\n", db_error_string (3));
      goto error_return;
    }

  error = hb_resource_job_initialize ();
  if (error != NO_ERROR)
    {
      er_log_debug (ARG_FILE_LINE,
		    "hb_resource_job_initialize failed. "
		    "(error=%d). \n", error);
      util_log_write_errstr ("%s\n", db_error_string (3));
      goto error_return;
    }

  error = hb_thread_initialize ();
  if (error != NO_ERROR)
    {
      er_log_debug (ARG_FILE_LINE, "hb_thread_initialize failed. "
		    "(error=%d). \n", error);
      util_log_write_errstr ("%s\n", db_error_string (3));
      goto error_return;
    }

  hb_Deactivate_immediately = false;

  return NO_ERROR;

error_return:
  if (hb_Cluster && hb_Cluster->shutdown == false)
    {
      hb_cluster_cleanup ();
    }

  if (cluster_JobQ && cluster_JobQ->shutdown == false)
    {
      hb_cluster_job_shutdown ();
    }

  if (hb_Resource && hb_Resource->shutdown == false)
    {
      hb_resource_cleanup ();
    }

  if (resource_JobQ && resource_JobQ->shutdown == false)
    {
      hb_resource_job_shutdown ();
    }

  return error;
}

/*
 * terminator
 */

/*
 * hb_resource_shutdown_all_ha_procs() -
 *   return:
 *
 */
static void
hb_resource_shutdown_all_ha_procs (void)
{
  HB_PROC_ENTRY *proc;
  SOCKET_QUEUE_ENTRY *sock_ent;
  char buffer[MASTER_TO_SRV_MSG_SIZE];

  /* set process state to deregister and close connection  */
  for (proc = hb_Resource->procs; proc; proc = proc->next)
    {
      if (proc->conn)
	{
	  if (proc->type != HB_PTYPE_SERVER)
	    {
	      css_remove_entry_by_conn (proc->conn,
					&css_Master_socket_anchor);
	      proc->conn = NULL;
	      proc->sfd = INVALID_SOCKET;
	    }
	  else
	    {
	      /* In case of HA server, just send shutdown request */
	      sock_ent =
		css_return_entry_by_conn (proc->conn,
					  &css_Master_socket_anchor);
	      assert_release (sock_ent == NULL || sock_ent->name != NULL);
	      if (sock_ent && sock_ent->name)
		{
		  memset (buffer, 0, sizeof (buffer));
		  snprintf (buffer, sizeof (buffer) - 1,
			    msgcat_message (MSGCAT_CATALOG_UTILS,
					    MSGCAT_UTIL_SET_MASTER,
					    MASTER_MSG_SERVER_STATUS),
			    sock_ent->name + 1, 0);

		  css_process_start_shutdown (sock_ent, 0, buffer);
		}
	      else
		{
		  proc->conn = NULL;
		  proc->sfd = INVALID_SOCKET;
		}
	    }
	}
      else
	{
	  er_log_debug (ARG_FILE_LINE,
			"invalid socket-queue entry. (pid:%d).\n", proc->pid);
	}

      proc->state = HB_PSTATE_DEREGISTERED;
      proc->server_state = HA_STATE_UNKNOWN;

      shm_master_update_server_state (proc);
    }

  return;
}

/*
 * hb_resource_cleanup() -
 *   return:
 *
 */
static void
hb_resource_cleanup (void)
{
  HB_PROC_ENTRY *proc;

  pthread_mutex_lock (&hb_Resource->lock);

  hb_resource_shutdown_all_ha_procs ();

  for (proc = hb_Resource->procs; proc; proc = proc->next)
    {
      if (proc->conn && proc->pid > 0)
	{
	  kill (proc->pid, SIGKILL);
	}
    }

  hb_remove_all_procs (hb_Resource->procs);
  hb_Resource->procs = NULL;
  hb_Resource->num_procs = 0;
  hb_Resource->node_state = HA_STATE_UNKNOWN;
  hb_Resource->shutdown = true;
  pthread_mutex_unlock (&hb_Resource->lock);

  return;
}

/*
 * hb_resource_shutdown_and_cleanup() -
 *   return:
 *
 */
void
hb_resource_shutdown_and_cleanup (void)
{
  if (hb_Resource && resource_JobQ)
    {
      hb_resource_job_shutdown ();
      hb_resource_cleanup ();
    }
}

/*
 * hb_cluster_cleanup() -
 *   return:
 *
 */
static void
hb_cluster_cleanup (void)
{
  HB_NODE_ENTRY *node;

  pthread_mutex_lock (&hb_Cluster->lock);
  hb_Cluster->node_state = HA_STATE_UNKNOWN;

  for (node = hb_Cluster->nodes; node; node = node->next)
    {
      if (prm_is_same_node (&hb_Cluster->my_node_info,
			    &node->node_info) == true)
	{
	  continue;
	}

      hb_cluster_send_heartbeat (true, &node->node_info);
      node->heartbeat_gap++;
    }

  hb_cluster_remove_all_nodes (hb_Cluster->nodes);
  hb_Cluster->nodes = NULL;
  hb_Cluster->master = NULL;
  hb_Cluster->myself = NULL;
  hb_Cluster->shutdown = true;
  if (hb_Cluster->sfd != INVALID_SOCKET)
    {
      close (hb_Cluster->sfd);
      hb_Cluster->sfd = INVALID_SOCKET;
    }

  hb_cluster_remove_all_ping_hosts (hb_Cluster->ping_hosts);
  hb_Cluster->ping_hosts = NULL;
  hb_Cluster->num_ping_hosts = 0;

  pthread_mutex_unlock (&hb_Cluster->lock);
}

/*
 * hb_cluster_cleanup() -
 *   return:
 *
 */
void
hb_cluster_shutdown_and_cleanup (void)
{
  if (hb_Cluster && cluster_JobQ)
    {
      hb_cluster_job_shutdown ();
      hb_cluster_cleanup ();
    }
}

/*
 * hb_process_state_string -
 *   return: process state sring
 *
 *   ptype(in):
 *   pstate(in):
 */
const char *
hb_process_state_string (int pstate)
{
  switch (pstate)
    {
    case HB_PSTATE_UNKNOWN:
      return HB_PSTATE_UNKNOWN_STR;
    case HB_PSTATE_DEAD:
      return HB_PSTATE_DEAD_STR;
    case HB_PSTATE_DEREGISTERED:
      return HB_PSTATE_DEREGISTERED_STR;
    case HB_PSTATE_STARTED:
      return HB_PSTATE_STARTED_STR;
    case HB_PSTATE_NOT_REGISTERED:
      return HB_PSTATE_NOT_REGISTERED_STR;
    case HB_PSTATE_REGISTERED:
      return HB_PSTATE_REGISTERED_STR;
    }

  return "invalid";
}

/*
 * hb_ping_result_string -
 *   return: ping result string
 *
 *   ping_result(in):
 */
const char *
hb_ping_result_string (int ping_result)
{
  switch (ping_result)
    {
    case HB_PING_UNKNOWN:
      return HB_PING_UNKNOWN_STR;
    case HB_PING_SUCCESS:
      return HB_PING_SUCCESS_STR;
    case HB_PING_USELESS_HOST:
      return HB_PING_USELESS_HOST_STR;
    case HB_PING_SYS_ERR:
      return HB_PING_SYS_ERR_STR;
    case HB_PING_FAILURE:
      return HB_PING_FAILURE_STR;
    }

  return "invalid";
}

/*
 * hb_reload_config -
 *   return: NO_ERROR or ER_FAILED
 *
 *  removed_hosts(out):
 *  num_removed_hosts(out):
 */
static int
hb_reload_config (PRM_NODE_LIST * rm_modes)
{
  int old_num_nodes, old_num_ping_hosts, error;
  HB_NODE_ENTRY *old_nodes;
  HB_NODE_ENTRY *old_node, *old_myself, *old_master, *new_node;
  HB_PING_HOST_ENTRY *old_ping_hosts;
  bool found_node = false;
  char host_name[MAX_NODE_INFO_STR_LEN];

  rm_modes->num_nodes = 0;

  if (hb_Cluster == NULL)
    {
      return ER_FAILED;
    }

  sysprm_reload_and_init (NULL);

  pthread_mutex_lock (&hb_Cluster->lock);

  /* backup old ping hosts */
  hb_list_move ((HB_LIST **) & old_ping_hosts,
		(HB_LIST **) & hb_Cluster->ping_hosts);
  old_num_ping_hosts = hb_Cluster->num_ping_hosts;

  hb_Cluster->ping_hosts = NULL;

  /* backup old node list */
  hb_list_move ((HB_LIST **) & old_nodes, (HB_LIST **) & hb_Cluster->nodes);
  old_myself = hb_Cluster->myself;
  old_master = hb_Cluster->master;
  old_num_nodes = hb_Cluster->num_nodes;

  hb_Cluster->nodes = NULL;

  /* reload ping hosts */
  hb_Cluster->num_ping_hosts =
    hb_cluster_load_ping_host_list (prm_get_string_value
				    (PRM_ID_HA_PING_HOSTS));

  if (hb_cluster_check_valid_ping_server () == false)
    {
      char buf[LINE_MAX];

      snprintf (buf, sizeof (buf),
		"Validity check for PING failed on all hosts ");
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_HB_NODE_EVENT,
	      1, buf);
    }

  /* reload node list */
  hb_Cluster->num_nodes = hb_cluster_load_group_and_node_list ();

  if (hb_Cluster->num_nodes < 1 ||
      (hb_Cluster->master &&
       hb_return_node_by_name (&hb_Cluster->master->node_info) == NULL))
    {
      error = ER_PRM_BAD_VALUE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
	      prm_get_name (PRM_ID_HA_NODE_LIST));

      goto reconfig_error;
    }

  /* find removed nodes */
  for (old_node = old_nodes; old_node; old_node = old_node->next)
    {
      found_node = false;
      for (new_node = hb_Cluster->nodes; new_node; new_node = new_node->next)
	{
	  if (prm_is_same_node (&new_node->node_info,
				&old_node->node_info) == true)
	    {
	      found_node = true;
	      break;
	    }
	}

      if (found_node == false)
	{
	  /* append remove node list */
	  rm_modes->nodes[rm_modes->num_nodes] = old_node->node_info;
	  rm_modes->num_nodes++;
	}
    }

  for (new_node = hb_Cluster->nodes; new_node; new_node = new_node->next)
    {
      for (old_node = old_nodes; old_node; old_node = old_node->next)
	{
	  if (prm_is_same_node (&new_node->node_info,
				&old_node->node_info) == false)
	    {
	      continue;
	    }
	  if (old_master &&
	      prm_is_same_node (&new_node->node_info,
				&old_master->node_info) == true)
	    {
	      hb_Cluster->master = new_node;
	    }
	  new_node->node_state = old_node->node_state;
	  new_node->score = old_node->score;
	  new_node->heartbeat_gap = old_node->heartbeat_gap;
	  new_node->last_recv_hbtime.tv_sec =
	    old_node->last_recv_hbtime.tv_sec;
	  new_node->last_recv_hbtime.tv_usec =
	    old_node->last_recv_hbtime.tv_usec;

	  /* mark node wouldn't deregister */
	  old_node->node_info = prm_get_null_node_info ();
	}
    }

  hb_cluster_job_set_expire_and_reorder (HB_CJOB_CHECK_VALID_PING_SERVER,
					 HB_JOB_TIMER_IMMEDIATELY);

  hb_shm_reset_hb_node ();

  /* clean up ping host backup */
  if (old_ping_hosts != NULL)
    {
      hb_cluster_remove_all_ping_hosts (old_ping_hosts);
    }

  /* clean up node list backup */
  if (old_nodes)
    {
      hb_cluster_remove_all_nodes (old_nodes);
    }
  pthread_mutex_unlock (&hb_Cluster->lock);

  return NO_ERROR;

reconfig_error:
  if (hb_Cluster->master)
    {
      prm_node_info_to_str (host_name, sizeof (host_name),
			    &hb_Cluster->master->node_info);
    }
  else
    {
      strcpy (host_name, "-");
    }
  er_log_debug (ARG_FILE_LINE, "reconfigure heartebat failed. "
		"(num_nodes:%d, master:{%s}).\n",
		hb_Cluster->num_nodes, host_name);

/* restore ping hosts */
  hb_Cluster->num_ping_hosts = old_num_ping_hosts;

  hb_cluster_remove_all_ping_hosts (hb_Cluster->ping_hosts);

  hb_list_move ((HB_LIST **) & hb_Cluster->ping_hosts,
		(HB_LIST **) & old_ping_hosts);

  /* restore node list */
  hb_cluster_remove_all_nodes (hb_Cluster->nodes);
  hb_Cluster->myself = old_myself;
  hb_Cluster->master = old_master;
  hb_Cluster->num_nodes = old_num_nodes;

  hb_list_move ((HB_LIST **) & hb_Cluster->nodes, (HB_LIST **) & old_nodes);

  pthread_mutex_unlock (&hb_Cluster->lock);

  return error;
}

/*
 * hb_get_admin_info_string -
 *   return: admin info string
 *
 */
char *
hb_get_admin_info_string (void)
{
  int buf_size = 0;
  char *p, *last;
  char *str = NULL;

  pthread_mutex_lock (&css_Master_er_log_enable_lock);

  if (css_Master_er_log_enabled == true || hb_Nolog_event_msg[0] == '\0')
    {
      pthread_mutex_unlock (&css_Master_er_log_enable_lock);
      return NULL;
    }

  buf_size = strlen (HA_ADMIN_INFO_FORMAT_STRING);
  buf_size += strlen (HA_ADMIN_INFO_NOLOG_FORMAT_STRING);
  buf_size += strlen (HA_ADMIN_INFO_NOLOG_EVENT_FORMAT_STRING);
  buf_size += strlen (hb_Nolog_event_msg);

  str = (char *) malloc (sizeof (char) * buf_size);
  if (str == NULL)
    {
      pthread_mutex_unlock (&css_Master_er_log_enable_lock);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (char) * buf_size);
      return NULL;
    }
  *str = '\0';

  p = str;
  last = p + buf_size;

  p += snprintf (p, MAX ((last - p), 0), HA_ADMIN_INFO_FORMAT_STRING);
  p += snprintf (p, MAX ((last - p), 0), HA_ADMIN_INFO_NOLOG_FORMAT_STRING);
  p +=
    snprintf (p, MAX ((last - p), 0), HA_ADMIN_INFO_NOLOG_EVENT_FORMAT_STRING,
	      hb_Nolog_event_msg);

  pthread_mutex_unlock (&css_Master_er_log_enable_lock);

  return str;
}

/*
 * hb_get_ping_host_info_string -
 *   return: host info string
 *
 */
char *
hb_get_ping_host_info_string (void)
{
  int buf_size = 0, required_size = 0;
  char *str;
  char *p, *last;
  bool valid_ping_host_exists;
  bool is_ping_check_enabled = true;
  HB_PING_HOST_ENTRY *ping_host;

  if (hb_Cluster == NULL)
    {
      return NULL;
    }

  pthread_mutex_lock (&hb_Cluster->lock);

  if (hb_Cluster->num_ping_hosts == 0)
    {
      pthread_mutex_unlock (&hb_Cluster->lock);
      return NULL;
    }

  /* refresh ping host info */
  valid_ping_host_exists = hb_cluster_check_valid_ping_server ();

  if (valid_ping_host_exists == false && hb_cluster_is_isolated () == false)
    {
      is_ping_check_enabled = false;
    }

  if (is_ping_check_enabled != hb_Cluster->is_ping_check_enabled)
    {
      hb_cluster_job_set_expire_and_reorder (HB_CJOB_CHECK_VALID_PING_SERVER,
					     HB_JOB_TIMER_IMMEDIATELY);
    }

  required_size = strlen (HA_PING_HOSTS_INFO_FORMAT_STRING);
  required_size += 7;		/* length of ping check status */

  buf_size += required_size;

  required_size = strlen (HA_PING_HOSTS_FORMAT_STRING);
  required_size += MAXHOSTNAMELEN;
  required_size += HB_PING_STR_SIZE;	/* length of ping test result */
  required_size *= hb_Cluster->num_ping_hosts;

  buf_size += required_size;

  str = (char *) malloc (sizeof (char) * buf_size);
  if (str == NULL)
    {
      pthread_mutex_unlock (&hb_Cluster->lock);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (char) * buf_size);
      return NULL;
    }
  *str = '\0';

  p = str;
  last = p + buf_size;

  p += snprintf (p, MAX ((last - p), 0), HA_PING_HOSTS_INFO_FORMAT_STRING,
		 is_ping_check_enabled ? "enabled" : "disabled");

  for (ping_host = hb_Cluster->ping_hosts; ping_host;
       ping_host = ping_host->next)
    {
      p += snprintf (p, MAX ((last - p), 0), HA_PING_HOSTS_FORMAT_STRING,
		     ping_host->host_name,
		     hb_ping_result_string (ping_host->ping_result));
    }

  pthread_mutex_unlock (&hb_Cluster->lock);

  return str;
}

/*
 * hb_get_node_info_string -
 *   return: none
 *
 *   str(out):
 *   verbose_yn(in):
 */
char *
hb_get_node_info_string (bool verbose_yn)
{
  HB_NODE_ENTRY *node;
  int buf_size = 0, required_size = 0;
  char *p, *last;
  char *str;
  char host_name[MAX_NODE_INFO_STR_LEN];

  if (hb_Cluster == NULL)
    {
      return NULL;
    }

  required_size = strlen (HA_NODE_INFO_FORMAT_STRING);
  required_size += MAXHOSTNAMELEN;	/* length of node name */
  required_size += HA_STATE_STR_SZ;	/* length of node state */
  buf_size += required_size;

  required_size = strlen (HA_NODE_FORMAT_STRING);
  required_size += MAXHOSTNAMELEN;	/* length of node name */
  required_size += 5;		/* length of priority */
  required_size += HA_STATE_STR_SZ;	/* length of node state */
  if (verbose_yn)
    {
      required_size += strlen (HA_NODE_SCORE_FORMAT_STRING);
      required_size += 6;	/* length of score      */
      required_size += strlen (HA_NODE_HEARTBEAT_GAP_FORMAT_STRING);
      required_size += 6;	/* length of missed heartbeat */
    }

  pthread_mutex_lock (&hb_Cluster->lock);

  required_size *= hb_Cluster->num_nodes;
  buf_size += required_size;

  str = (char *) malloc (sizeof (char) * buf_size);
  if (str == NULL)
    {
      pthread_mutex_unlock (&hb_Cluster->lock);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (char) * buf_size);
      return NULL;
    }
  *str = '\0';

  p = str;
  last = p + buf_size;

  prm_node_info_to_str (host_name, sizeof (host_name),
			&hb_Cluster->my_node_info);
  p += snprintf (p, MAX ((last - p), 0), HA_NODE_INFO_FORMAT_STRING,
		 host_name, HA_STATE_NAME (hb_Cluster->node_state));

  for (node = hb_Cluster->nodes; node; node = node->next)
    {
      prm_node_info_to_str (host_name, sizeof (host_name), &node->node_info);
      p += snprintf (p, MAX ((last - p), 0), HA_NODE_FORMAT_STRING,
		     host_name, node->priority,
		     HA_STATE_NAME (node->node_state));
      if (verbose_yn)
	{
	  p += snprintf (p, MAX ((last - p), 0), HA_NODE_SCORE_FORMAT_STRING,
			 node->score);
	  p += snprintf (p, MAX ((last - p), 0),
			 HA_NODE_HEARTBEAT_GAP_FORMAT_STRING,
			 node->heartbeat_gap);
	}
    }
  pthread_mutex_unlock (&hb_Cluster->lock);

  return str;
}

/*
 * hb_get_process_info_string -
 *   return: none
 *
 *   str(out):
 *   verbose_yn(in):
 */
char *
hb_get_process_info_string (bool verbose_yn)
{
  HB_PROC_ENTRY *proc;
  SOCKET_QUEUE_ENTRY *sock_entq;
  int buf_size = 0, len = 0;
  char *p, *last;
  char *str = NULL;

  if (hb_Resource == NULL)
    {
      return NULL;
    }

  pthread_mutex_lock (&hb_Resource->lock);

  if (verbose_yn == true)
    {
      buf_size = ONE_K + 2 * ONE_K * hb_Resource->num_procs;
    }
  else
    {
      buf_size = ONE_K + ONE_K * hb_Resource->num_procs;
    }

retry_memory:
  if (str != NULL)
    {
      free_and_init (str);

      buf_size = buf_size * 2;
    }

  str = (char *) malloc (buf_size);
  if (str == NULL)
    {
      pthread_mutex_unlock (&hb_Resource->lock);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, buf_size);
      return NULL;
    }
  *str = '\0';

  p = str;
  last = p + buf_size;

  len = snprintf (p, MAX ((last - p), 0), HA_PROCESS_INFO_FORMAT_STRING,
		  getpid (), HA_STATE_NAME (hb_Resource->node_state));
  if (len < 0)
    {
      goto retry_memory;
    }
  p += len;

  for (proc = hb_Resource->procs; proc; proc = proc->next)
    {
      sock_entq = css_return_entry_by_conn (proc->conn,
					    &css_Master_socket_anchor);
      assert_release (sock_entq == NULL || sock_entq->name != NULL);
      if (sock_entq == NULL || sock_entq->name == NULL)
	{
	  continue;
	}

      len = hb_get_process_info (p, MAX ((last - p), 0), proc, verbose_yn);
      if (len < 0)
	{
	  if (len == ER_OUT_OF_VIRTUAL_MEMORY)
	    {
	      goto retry_memory;
	    }

	  pthread_mutex_unlock (&hb_Resource->lock);

	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
	  return NULL;
	}
      p += len;
    }

  pthread_mutex_unlock (&hb_Resource->lock);

  return str;
}

/*
 * hb_get_process_info ()-
 *    return: len
 *
 *    info(in/out):
 *    max_size(in):
 *    proc(in):
 *    verbose_yn(in):
 */
static int
hb_get_process_info (char *info, int max_size, HB_PROC_ENTRY * proc,
		     int verbose_yn)
{
  int len = -1, total_len;

  total_len = 0;
  switch (proc->type)
    {
    case HB_PTYPE_SERVER:
      len = snprintf (info, MAX (max_size - total_len, 0),
		      HA_SERVER_PROCESS_FORMAT_STRING,
		      proc->argv[1], proc->pid,
		      hb_process_state_string (proc->state),
		      HA_STATE_NAME (proc->server_state));
      break;
    case HB_PTYPE_REPLICATION:
      len =
	snprintf (info, MAX (max_size - total_len, 0),
		  HA_REPLICATION_PROCESS_FORMAT_STRING, proc->argv[3],
		  proc->pid, proc->argv[2],
		  hb_process_state_string (proc->state));
      break;
    default:
      break;
    }
  if (len < 0)
    {
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }
  info += len;
  total_len += len;

  if (verbose_yn)
    {
      char r_time[256], d_time[256], k_time[256], s_time[256];
      const char *format[] = {
	HA_PROCESS_EXEC_PATH_FORMAT_STRING,
	HA_PROCESS_ARGV_FORMAT_STRING,
	HA_PROCESS_REGISTER_TIME_FORMAT_STRING,
	HA_PROCESS_DEREGISTER_TIME_FORMAT_STRING,
	HA_PROCESS_SHUTDOWN_TIME_FORMAT_STRING,
	HA_PROCESS_START_TIME_FORMAT_STRING
      };
      const char *values[6];
      int i = 0;

      values[i++] = proc->exec_path;
      values[i++] = proc->args;
      values[i++] = hb_strtime (r_time, sizeof (r_time), &proc->rtime);
      values[i++] = hb_strtime (d_time, sizeof (d_time), &proc->dtime);
      values[i++] = hb_strtime (k_time, sizeof (k_time), &proc->ktime);
      values[i++] = hb_strtime (s_time, sizeof (s_time), &proc->stime);

      if (i != DIM (format))
	{
	  assert (false);

	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
	  return ER_GENERIC_ERROR;
	}

      for (i = 0; i < (int) DIM (format); i++)
	{
	  len = snprintf (info, MAX (max_size - total_len, 0),
			  format[i], values[i]);
	  if (len < 0)
	    {
	      return ER_OUT_OF_VIRTUAL_MEMORY;
	    }
	  info += len;
	  total_len += len;
	}
    }

  return total_len;
}

/*
 * hb_kill_process - kill a list of processes
 *   return: none
 *
 */
static void
hb_kill_process (pid_t * pids, int count)
{
  int error;
  int i = 0, j = 0;
  int max_retries, wait_time_in_secs;
  int signum = SIGTERM;
  bool finished;

  max_retries = 20;
  wait_time_in_secs = 3;
  for (i = 0; i < max_retries; i++)
    {
      finished = true;
      for (j = 0; j < count; j++)
	{
	  if (pids[j] > 0)
	    {
	      error = kill (pids[j], signum);
	      if (error != 0 && errno == ESRCH)
		{
		  pids[j] = 0;
		}
	      else
		{
		  finished = false;
		}
	    }
	}
      if (finished == true)
	{
	  return;
	}
      signum = 0;
      THREAD_SLEEP (wait_time_in_secs * 1000);
    }

  for (j = 0; j < count; j++)
    {
      if (pids[j] > 0)
	{
	  kill (pids[j], SIGKILL);
	}
    }

  return;
}

/*
 * hb_reconfig_heartbeat -
 *   return: none
 *
 */
int
hb_reconfig_heartbeat (void)
{
  int error = NO_ERROR;
  char error_string[LINE_MAX] = "";

  if (hb_Cluster == NULL || hb_Resource == NULL)
    {
      assert (false);

      return ER_FAILED;
    }

  error = hb_resource_job_queue (HB_RJOB_RELOAD_NODES, NULL,
				 HB_JOB_TIMER_IMMEDIATELY);
  if (error != NO_ERROR)
    {
      assert (false);
      return error;
    }

  snprintf (error_string, LINE_MAX, "Rye heartbeat reload.");
  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
	  ER_HB_COMMAND_EXECUTION, 2, HB_CMD_RELOAD_STR, error_string);

  return NO_ERROR;
}

/*
 * hb_changemode -
 *   return: none
 *
 */
int
hb_changemode (HA_STATE req_node_state, bool force)
{
  int error = NO_ERROR;
  char error_string[LINE_MAX] = "";
  int changeslave_timeout = 0;
  HB_PROC_ENTRY *proc = NULL;

  if (hb_Cluster == NULL || hb_Resource == NULL)
    {
      assert (false);

      return ER_FAILED;
    }

  if (force == true)
    {
      error = hb_cluster_job_queue (HB_CJOB_CHANGEMODE_FORCE, NULL,
				    HB_JOB_TIMER_IMMEDIATELY);
    }
  else if (req_node_state == HA_STATE_SLAVE)
    {
      pthread_mutex_lock (&hb_Resource->lock);
      proc = hb_find_proc_by_server_state (HA_STATE_TO_BE_MASTER);
      pthread_mutex_unlock (&hb_Resource->lock);
      if (proc != NULL)
	{
	  return ER_FAILED;
	}

      error = hb_cluster_job_queue (HB_CJOB_CHANGE_SLAVE, NULL,
				    HB_JOB_TIMER_IMMEDIATELY);
      changeslave_timeout = prm_get_bigint_value
	(PRM_ID_HA_CHANGESLAVE_MAX_WAIT_TIME);
      error = hb_cluster_job_queue (HB_CJOB_CHANGEMODE_FORCE, NULL,
				    changeslave_timeout);
    }
  else
    {
      assert (false);
      error = ER_FAILED;
    }

  if (error != NO_ERROR)
    {
      assert (false);
      return error;
    }

  snprintf (error_string, LINE_MAX, "Rye heartbeat changemode.");
  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
	  ER_HB_COMMAND_EXECUTION, 2, HB_CMD_CHANGEMODE_STR, error_string);

  return NO_ERROR;
}

/*
 * hb_prepare_deactivate_heartbeat - shutdown all HA processes
 *      to deactivate heartbeat
 *   return:
 *
 */
int
hb_prepare_deactivate_heartbeat (void)
{
  int error = NO_ERROR;
  char error_string[LINE_MAX] = "";

  if (hb_Cluster == NULL || hb_Resource == NULL)
    {
      return ER_FAILED;
    }

  pthread_mutex_lock (&hb_Resource->lock);
  if (hb_Resource->shutdown == true)
    {
      /* resources have already been cleaned up */
      pthread_mutex_unlock (&hb_Resource->lock);

      return NO_ERROR;
    }

  hb_Resource->shutdown = true;
  pthread_mutex_unlock (&hb_Resource->lock);

  error =
    hb_resource_job_queue (HB_RJOB_CLEANUP_ALL, NULL,
			   HB_JOB_TIMER_IMMEDIATELY);

  if (error != NO_ERROR)
    {
      assert (false);
    }
  else
    {
      snprintf (error_string, LINE_MAX,
		"Rye heartbeat starts to shutdown all HA processes.");
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
	      ER_HB_COMMAND_EXECUTION, 2, HB_CMD_DEACTIVATE_STR,
	      error_string);
    }

  return error;
}

/*
 * hb_deactivate_heartbeat -
 *   return:
 *
 */
int
hb_deactivate_heartbeat (void)
{
  char error_string[LINE_MAX] = "";

  if (hb_Cluster == NULL)
    {
      return ER_FAILED;
    }

  if (hb_Is_activated == false)
    {
      snprintf (error_string, LINE_MAX,
		"%s. (Rye heartbeat feature already deactivated)",
		HB_RESULT_FAILURE_STR);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_HB_COMMAND_EXECUTION, 2, HB_CMD_DEACTIVATE_STR,
	      error_string);

      return NO_ERROR;
    }

  if (hb_Resource != NULL && resource_JobQ != NULL)
    {
      hb_resource_shutdown_and_cleanup ();
    }

  if (hb_Cluster != NULL && cluster_JobQ != NULL)
    {
      hb_cluster_shutdown_and_cleanup ();
    }

  hb_Is_activated = false;

  snprintf (error_string, LINE_MAX, "%s.", HB_RESULT_SUCCESS_STR);
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HB_COMMAND_EXECUTION, 2,
	  HB_CMD_DEACTIVATE_STR, error_string);

  return NO_ERROR;
}

/*
 * hb_activate_heartbeat -
 *   return: none
 *
 *   str(out):
 */
int
hb_activate_heartbeat (void)
{
  int error = NO_ERROR;
  char error_string[LINE_MAX] = "";

  er_log_debug (ARG_FILE_LINE, "hb_activate_heartbeat!!");

  if (hb_Cluster == NULL)
    {
      return ER_FAILED;
    }

  /* unfinished job of deactivation exists */
  if (hb_Deactivate_info.info_started == true)
    {
      error = ER_HB_COMMAND_EXECUTION;
      snprintf (error_string, LINE_MAX,
		"%s. (Rye heartbeat feature is being deactivated)",
		HB_RESULT_FAILURE_STR);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      error, 2, HB_CMD_ACTIVATE_STR, error_string);
      return error;
    }

  if (hb_Is_activated == true)
    {
      error = ER_HB_COMMAND_EXECUTION;
      snprintf (error_string, LINE_MAX,
		"%s. (Rye heartbeat feature already activated)",
		HB_RESULT_FAILURE_STR);
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
	      error, 2, HB_CMD_ACTIVATE_STR, error_string);
      return NO_ERROR;
    }

  if (rye_shm_destroy_all_server_shm () != NO_ERROR)
    {
      error = ER_HB_COMMAND_EXECUTION;
      snprintf (error_string, LINE_MAX,
		"%s. (Rye rye_shm_key was not found)", HB_RESULT_FAILURE_STR);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      error, 2, HB_CMD_ACTIVATE_STR, error_string);
      return error;
    }

  error = hb_master_init ();
  if (error != NO_ERROR)
    {
      return error;
    }

  hb_Is_activated = true;

  snprintf (error_string, LINE_MAX, "%s.", HB_RESULT_SUCCESS_STR);
  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
	  ER_HB_COMMAND_EXECUTION, 2, HB_CMD_ACTIVATE_STR, error_string);

  return NO_ERROR;
}

/*
 * hb_process_start()-
 *   return: error code
 *
 *   ha_conf(in):
 */
int
hb_process_start (const HA_CONF * ha_conf)
{
  int error = NO_ERROR;

  error = hb_server_start (ha_conf, NULL);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = hb_repl_start (ha_conf);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  return NO_ERROR;

exit_on_error:
  assert (error != NO_ERROR);

  return error;
}

/*
 *
 */
static int
hb_server_start (const HA_CONF * ha_conf, char *db_name)
{
  HBP_PROC_REGISTER proc_reg;
  int num_db_found, i;
  char **dbs;
  int error = NO_ERROR;

  num_db_found = 0;
  dbs = ha_conf->db_names;
  for (i = 0; dbs[i] != NULL; i++)
    {
      if (db_name != NULL && strcmp (db_name, dbs[i]))
	{
	  continue;
	}
      num_db_found++;

      error = hb_make_hbp_register (&proc_reg, ha_conf, HB_PTYPE_SERVER,
				    HB_PCMD_START, dbs[i], NULL);
      if (error != NO_ERROR)
	{
	  return error;
	}

      hb_resource_register_new_proc (&proc_reg, NULL);
    }

  return NO_ERROR;
}

static int
hb_remove_copylog (const HA_CONF * ha_conf, const PRM_NODE_LIST * rm_nodes)
{
  int i, j;
  char **dbs;
  char log_path[PATH_MAX];
  char active_name[PATH_MAX];

  dbs = ha_conf->db_names;
  for (i = 0; dbs[i] != NULL; i++)
    {
      for (j = 0; j < rm_nodes->num_nodes; j++)
	{
	  char host[MAX_NODE_INFO_STR_LEN];
	  prm_node_info_to_str (host, sizeof (host), &rm_nodes->nodes[j]);
	  ha_make_log_path (log_path, sizeof (log_path),
			    ha_conf->node_conf[0].copy_log_base, dbs[i],
			    host);
	  assert (strlen (log_path) > 0);
	  fileio_make_log_active_name (active_name, log_path, dbs[i]);

	  fileio_unformat (NULL, active_name);
	}
    }

  return NO_ERROR;
}

static int
hb_remove_host_from_catalog (CCI_CONN * conn, const PRM_NODE_INFO * host_info)
{
  char query[ONE_K];
  const char *query_format[] = {
    "delete from db_log_writer where host_ip='%s';",
    "delete from db_log_applier where host_ip='%s';",
    "delete from db_log_analyzer where host_ip='%s';"
  };
  CCI_STMT stmt;
  int i, error;
  char host_key_str[MAX_NODE_INFO_STR_LEN];

  prm_node_info_to_str (host_key_str, sizeof (host_key_str), host_info);

  for (i = 0; i < (int) DIM (query_format); i++)
    {
      snprintf (query, sizeof (query), query_format[i], host_key_str);
      error = cci_prepare (conn, &stmt, query, 0);
      if (error < 0)
	{
	  error = ER_GENERIC_ERROR;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
		  conn->err_buf.err_msg);
	  return error;
	}
      error = cci_execute (&stmt, 0, 0);
      if (error < 0)
	{
	  error = ER_GENERIC_ERROR;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
		  stmt.err_buf.err_msg);
	  return error;
	}
    }

  return NO_ERROR;
}

static int
hb_remove_catalog_info (const HA_CONF * ha_conf,
			const PRM_NODE_LIST * rm_nodes)
{
  int error = NO_ERROR;
  int i, j;
  char **dbs;
  char *broker_key = NULL;
  int portid;
  CCI_CONN conn;
  char url[ONE_K];

  error = broker_get_local_mgmt_info (&broker_key, &portid);
  if (error < 0)
    {
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "not found local_mgmt broker");

      GOTO_EXIT_ON_ERROR;
    }

  dbs = ha_conf->db_names;
  for (i = 0; dbs[i] != NULL; i++)
    {
      snprintf (url, sizeof (url),
		"cci:rye://localhost:%d/%s/repl?connectionType=local",
		portid, dbs[i]);
      error = cci_connect (&conn, url, "dba", broker_key);
      if (error < 0)
	{
	  error = ER_GENERIC_ERROR;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
		  conn.err_buf.err_msg);
	  GOTO_EXIT_ON_ERROR;
	}
      error = cci_set_autocommit (&conn, CCI_AUTOCOMMIT_FALSE);
      if (error < 0)
	{
	  error = ER_GENERIC_ERROR;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
		  conn.err_buf.err_msg);

	  cci_disconnect (&conn);

	  GOTO_EXIT_ON_ERROR;
	}

      for (j = 0; error == NO_ERROR && j < rm_nodes->num_nodes; j++)
	{
	  error = hb_remove_host_from_catalog (&conn, &rm_nodes->nodes[j]);
	}
      if (error == NO_ERROR)
	{
	  cci_end_tran (&conn, CCI_TRAN_COMMIT);
	}

      cci_disconnect (&conn);
    }

  if (broker_key != NULL)
    {
      free_and_init (broker_key);
    }


  return NO_ERROR;

exit_on_error:
  assert (error != NO_ERROR);

  if (broker_key != NULL)
    {
      free_and_init (broker_key);
    }

  return error;
}

/*
 *
 */
static int
hb_repl_start (const HA_CONF * ha_conf)
{
  HBP_PROC_REGISTER proc_reg;
  int num_db_found = 0, num_node_found = 0;
  int i, j, num_nodes;
  char **dbs;
  HA_NODE_CONF *nc;
  int error = NO_ERROR;

  num_nodes = ha_conf->num_node_conf;
  dbs = ha_conf->db_names;
  nc = ha_conf->node_conf;
  for (i = 0; dbs[i] != NULL; i++)
    {
      num_db_found++;

      for (j = 0; j < num_nodes; j++)
	{
	  if (prm_is_myself_node_info (&nc[j].node))
	    {
	      continue;
	    }
	  num_node_found++;

	  error = hb_make_hbp_register (&proc_reg, ha_conf,
					HB_PTYPE_REPLICATION,
					HB_PCMD_START, dbs[i], &nc[j].node);
	  if (error != NO_ERROR)
	    {
	      return error;
	    }

	  hb_resource_register_new_proc (&proc_reg, NULL);
	}
    }

  return NO_ERROR;
}

/*
 * hb_repl_stop ()
 *   return: error code
 *
 */
static int
hb_repl_stop (void)
{
  HB_PROC_ENTRY *proc, *proc_next;
  char error_string[LINE_MAX] = "";
  bool found_ha_proc;
  int retry_count = 0;
  int max_retries, interval, sig;

  pthread_mutex_lock (&css_Master_socket_anchor_lock);
  pthread_mutex_lock (&hb_Resource->lock);

  /* set process state to deregister and close connection  */
  for (proc = hb_Resource->procs; proc; proc = proc->next)
    {
      if (proc->conn)
	{
	  if (proc->type == HB_PTYPE_REPLICATION)
	    {
	      css_remove_entry_by_conn (proc->conn,
					&css_Master_socket_anchor);
	      proc->conn = NULL;
	      proc->sfd = INVALID_SOCKET;

	      proc->state = HB_PSTATE_DEREGISTERED;
	    }
	}
      else
	{
	  er_log_debug (ARG_FILE_LINE,
			"invalid socket-queue entry. (pid:%d).\n", proc->pid);
	}
    }
  pthread_mutex_unlock (&hb_Resource->lock);
  pthread_mutex_unlock (&css_Master_socket_anchor_lock);


  max_retries = prm_get_integer_value (PRM_ID_HA_MAX_PROCESS_START_CONFIRM);
  interval = prm_get_bigint_value (PRM_ID_HA_PROCESS_DEREG_CONFIRM_INTERVAL);
  retry_count = 0;

  do
    {
      found_ha_proc = false;
      if (retry_count++ < max_retries)
	{
	  sig = SIGTERM;
	}
      else
	{
	  sig = SIGKILL;
	}

      pthread_mutex_lock (&hb_Resource->lock);

      /* kill process and remove process resource */
      for (proc = hb_Resource->procs; proc; proc = proc_next)
	{
	  proc_next = proc->next;

	  if (proc->type == HB_PTYPE_REPLICATION)
	    {
	      assert (proc->pid > 0);

	      if (proc->pid > 0
		  && (kill (proc->pid, 0) == 0 || errno != ESRCH))
		{
		  snprintf (error_string, LINE_MAX, "(pid: %d, args:%s)",
			    proc->pid, proc->args);
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_HB_PROCESS_EVENT, 2,
			  "Immediate shutdown requested. Process killed",
			  error_string);

		  kill (proc->pid, sig);
		  found_ha_proc = true;
		}
	      else
		{
		  hb_Resource->num_procs--;
		  hb_remove_proc (proc);
		  proc = NULL;
		}
	    }
	}

      pthread_mutex_unlock (&hb_Resource->lock);

      if (found_ha_proc == true)
	{
	  THREAD_SLEEP (interval);
	}
    }
  while (found_ha_proc == true);

  return NO_ERROR;
}

/*
 * common
 */

void
hb_enable_er_log (void)
{
  pthread_mutex_lock (&css_Master_er_log_enable_lock);

  css_Master_er_log_enabled = true;
  hb_Nolog_event_msg[0] = '\0';

  pthread_mutex_unlock (&css_Master_er_log_enable_lock);
  return;
}

void
hb_disable_er_log (int reason, const char *msg_fmt, ...)
{
  va_list args;
  char *p, *last;
  const char *event_name;
  char time_str[256];
  struct timeval curr_time;

  pthread_mutex_lock (&css_Master_er_log_enable_lock);

  if (css_Master_er_log_enabled == false)
    {
      pthread_mutex_unlock (&css_Master_er_log_enable_lock);
      return;
    }

  if (reason == HB_NOLOG_DEMOTE_ON_DISK_FAIL)
    {
      event_name = "DEMOTE ON DISK FAILURE";
    }
  else if (reason == HB_NOLOG_REMOTE_STOP)
    {
      event_name = "REMOTE STOP";
    }
  else
    {
      pthread_mutex_unlock (&css_Master_er_log_enable_lock);
      return;
    }

  css_Master_er_log_enabled = false;

  p = hb_Nolog_event_msg;
  last = hb_Nolog_event_msg + sizeof (hb_Nolog_event_msg);

  gettimeofday (&curr_time, NULL);

  p += snprintf (p, MAX ((last - p), 0), "[%s][%s]",
		 hb_strtime (time_str, sizeof (time_str), &curr_time),
		 event_name);

  if (msg_fmt != NULL)
    {
      va_start (args, msg_fmt);
      vsnprintf (p, MAX ((last - p), 0), msg_fmt, args);
      va_end (args);
    }

  pthread_mutex_unlock (&css_Master_er_log_enable_lock);
  return;
}

/*
 * hb_check_ping -
 *   return : int
 *
 */
static int
hb_check_ping (const char *host)
{
#define PING_COMMAND_FORMAT \
"ping -w 1 -c 1 %s >/dev/null 2>&1; " \
"echo $?"

  char ping_command[256], result_str[16];
  char buf[128];
  char *end_p;
  int result = 0;
  int ping_result;
  FILE *fp;
  HB_NODE_ENTRY *node;
  PRM_NODE_INFO node_info;

  if (FI_TEST_ARG_INT (NULL, FI_TEST_HB_SLOW_PING_HOST, 30, 0) != NO_ERROR)
    {
      return HB_PING_FAILURE;
    }

  /* If host_p is in the cluster node, then skip to check */

  rp_host_str_to_node_info (&node_info, host);

  for (node = hb_Cluster->nodes; node; node = node->next)
    {
      if (PRM_NODE_INFO_GET_IP (&node_info) ==
	  PRM_NODE_INFO_GET_IP (&node->node_info))
	{
	  /* PING Host is same as cluster's host name */
	  snprintf (buf, sizeof (buf), "Useless PING host name %s", host);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HB_NODE_EVENT, 1, buf);
	  return HB_PING_USELESS_HOST;
	}
    }

  snprintf (ping_command, sizeof (ping_command), PING_COMMAND_FORMAT, host);
  fp = popen (ping_command, "r");
  if (fp == NULL)
    {
      /* ping open fail */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_HB_NODE_EVENT, 1, "PING command pork failed");
      return HB_PING_SYS_ERR;
    }

  if (fgets (result_str, sizeof (result_str), fp) == NULL)
    {
      pclose (fp);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_HB_NODE_EVENT, 1, "Can't get PING result");
      return HB_PING_SYS_ERR;
    }

  result_str[sizeof (result_str) - 1] = 0;

  pclose (fp);

  result = str_to_int32 (&ping_result, &end_p, result_str, 10);
  if (result != 0 || ping_result != NO_ERROR)
    {
      /* ping failed */
      snprintf (buf, sizeof (buf), "PING failed for host %s", host);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HB_NODE_EVENT, 1, buf);

      return HB_PING_FAILURE;

    }

  return HB_PING_SUCCESS;
}

static int
hb_help_sprint_ping_host_info (char *buffer, int max_length)
{
  HB_PING_HOST_ENTRY *ping_host;
  char *p, *last;

  if (*buffer != '\0')
    {
      memset (buffer, 0, max_length);
    }

  p = buffer;
  last = buffer + max_length;

  p += snprintf (p, MAX ((last - p), 0), "HA Ping Host Info\n");
  p += snprintf (p, MAX ((last - p), 0), "=============================="
		 "==================================================\n");

  p +=
    snprintf (p, MAX ((last - p), 0),
	      " * PING check is %s\n",
	      hb_Cluster->is_ping_check_enabled ? "enabled" : "disabled");
  p +=
    snprintf (p, MAX ((last - p), 0),
	      "------------------------------"
	      "--------------------------------------------------\n");
  p +=
    snprintf (p, MAX ((last - p), 0), "%-20s %-20s\n",
	      "hostname", "PING check result");
  p +=
    snprintf (p, MAX ((last - p), 0),
	      "------------------------------"
	      "--------------------------------------------------\n");
  for (ping_host = hb_Cluster->ping_hosts; ping_host;
       ping_host = ping_host->next)
    {
      p +=
	snprintf (p, MAX ((last - p), 0), "%-20s %-20s\n",
		  ping_host->host_name,
		  hb_ping_result_string (ping_host->ping_result));
    }
  p += snprintf (p, MAX ((last - p), 0), "=============================="
		 "==================================================\n");

  return p - buffer;
}

static int
hb_help_sprint_nodes_info (char *buffer, int max_length)
{
  HB_NODE_ENTRY *node;
  char *p, *last;
  char host_name[MAX_NODE_INFO_STR_LEN];

  if (*buffer != '\0')
    {
      memset (buffer, 0, max_length);
    }

  p = buffer;
  last = buffer + max_length;

  p += snprintf (p, MAX ((last - p), 0), "HA Node Info\n");
  p += snprintf (p, MAX ((last - p), 0), "=============================="
		 "==================================================\n");
  prm_node_info_to_str (host_name, sizeof (host_name),
			&hb_Cluster->my_node_info);
  p += snprintf (p, MAX ((last - p), 0),
		 " * group_id : %s   host_name : %s   state : %s \n",
		 hb_Cluster->group_id, host_name,
		 HA_STATE_NAME (hb_Cluster->node_state));
  p +=
    snprintf (p, MAX ((last - p), 0),
	      "------------------------------"
	      "--------------------------------------------------\n");
  p +=
    snprintf (p, MAX ((last - p), 0), "%-20s %-10s %-15s %-10s %-20s\n",
	      "name", "priority", "state", "score", "missed heartbeat");
  p +=
    snprintf (p, MAX ((last - p), 0),
	      "------------------------------"
	      "--------------------------------------------------\n");

  for (node = hb_Cluster->nodes; node; node = node->next)
    {
      prm_node_info_to_str (host_name, sizeof (host_name), &node->node_info);
      p += snprintf (p, MAX ((last - p), 0),
		     "%-20s %-10u %-15s %-10d %-20d\n",
		     host_name, node->priority,
		     HA_STATE_NAME (node->node_state), node->score,
		     node->heartbeat_gap);
    }

  p += snprintf (p, MAX ((last - p), 0), "=============================="
		 "==================================================\n");
  p += snprintf (p, MAX ((last - p), 0), "\n");

  return p - buffer;
}

static int
hb_help_sprint_processes_info (char *buffer, int max_length)
{
  HB_PROC_ENTRY *proc;
  char *p, *last;

  if (*buffer != '\0')
    {
      memset (buffer, 0, max_length);
    }

  p = buffer;
  last = p + max_length;

  p += snprintf (p, MAX ((last - p), 0), "HA Process Info\n");

  p += snprintf (p, MAX ((last - p), 0), "=============================="
		 "==================================================\n");
  p += snprintf (p, MAX ((last - p), 0), " * state : %s \n",
		 HA_STATE_NAME (hb_Resource->node_state));
  p += snprintf (p, MAX ((last - p), 0), "------------------------------"
		 "--------------------------------------------------\n");
  p += snprintf (p, MAX ((last - p), 0), "%-10s %-22s %-15s %-10s %-15s\n",
		 "pid", "state", "type", "socket fd", "server_state");
  p += snprintf (p, MAX ((last - p), 0),
		 "------------------------------"
		 "--------------------------------------------------\n");

  for (proc = hb_Resource->procs; proc; proc = proc->next)
    {
      if (proc->state == HB_PSTATE_UNKNOWN)
	continue;

      p +=
	snprintf (p, MAX ((last - p), 0), "%-10d %-22s %-15s %-10d %-15s\n",
		  proc->pid, hb_process_state_string (proc->state),
		  hb_process_type_string (proc->type), proc->sfd,
		  HA_STATE_NAME (proc->server_state));
    }

  p += snprintf (p, MAX ((last - p), 0), "=============================="
		 "==================================================\n");
  p += snprintf (p, MAX ((last - p), 0), "\n");

  return p - buffer;
}

int
hb_check_request_eligibility (SOCKET sd, int *result)
{
  char error_string[LINE_MAX];
  char request_from[MAXHOSTNAMELEN] = "";
  struct sockaddr_in req_addr;
  struct in_addr node_addr;
  socklen_t req_addr_len;
  HB_NODE_ENTRY *node;
  int error;

  *result = HB_HC_FAILED;

  req_addr_len = sizeof (req_addr);

  if (getpeername (sd, (struct sockaddr *) &req_addr, &req_addr_len) < 0)
    {
      *result = HB_HC_FAILED;

      goto end;
    }

  /* from localhost */
  if (req_addr.sin_family == AF_UNIX)
    {
      *result = HB_HC_ELIGIBLE_LOCAL;

      goto end;
    }

  pthread_mutex_lock (&hb_Cluster->lock);

  *result = HB_HC_UNAUTHORIZED;
  for (node = hb_Cluster->nodes; node; node = node->next)
    {
      node_addr.s_addr = PRM_NODE_INFO_GET_IP (&node->node_info);
      if (node_addr.s_addr == INADDR_NONE)
	{
	  char host_name[MAX_NODE_INFO_STR_LEN];
	  prm_node_info_to_str (host_name, sizeof (host_name),
				&node->node_info);
	  er_log_debug (ARG_FILE_LINE,
			"Failed to resolve IP address of %s", host_name);
	  *result = HB_HC_FAILED;
	  continue;
	}

      if (memcmp (&req_addr.sin_addr, &node_addr,
		  sizeof (struct in_addr)) == 0)
	{
	  *result = HB_HC_ELIGIBLE_REMOTE;

	  break;
	}
    }
  pthread_mutex_unlock (&hb_Cluster->lock);

end:
  error = NO_ERROR;
  if (*result == HB_HC_FAILED)
    {
      error = ER_GENERIC_ERROR;
      snprintf (error_string, LINE_MAX,
		"%s.(failed to check eligibility of request)",
		HB_RESULT_FAILURE_STR);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, error_string);
    }
  else if (*result == HB_HC_UNAUTHORIZED)
    {
      error = ER_GENERIC_ERROR;

      if (css_get_peer_name (sd, request_from, sizeof (request_from)) != 0)
	{
	  snprintf (request_from, sizeof (request_from), "UNKNOWN");
	}

      snprintf (error_string, LINE_MAX,
		"%s.(request from unauthorized host %s)",
		HB_RESULT_FAILURE_STR, request_from);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR,
	      1, error_string);
    }

  return error;
}

/*
 * hb_start_deactivate_server_info -
 *     Initialize hb_Server_deactivate_info,
 *     and set info_started flag to true.
 *
 *   return: none
 */
void
hb_start_deactivate_server_info (void)
{
  assert (hb_Deactivate_info.info_started == false);

  if (hb_Deactivate_info.server_pid_list != NULL)
    {
      free_and_init (hb_Deactivate_info.server_pid_list);
    }

  hb_Deactivate_info.server_count = 0;
  hb_Deactivate_info.info_started = true;
}

bool
hb_is_deactivation_started (void)
{
  return hb_Deactivate_info.info_started;
}

bool
hb_is_deactivation_ready (void)
{
  HB_PROC_ENTRY *proc;

  pthread_mutex_lock (&hb_Resource->lock);
  for (proc = hb_Resource->procs; proc; proc = proc->next)
    {
      if (proc->conn != NULL)
	{
	  pthread_mutex_unlock (&hb_Resource->lock);
	  return false;
	}
      assert (proc->sfd == INVALID_SOCKET);
    }
  pthread_mutex_unlock (&hb_Resource->lock);

  return true;
}


/*
 * hb_get_deactivating_server_count -
 *
 *   return: none
 */
int
hb_get_deactivating_server_count (void)
{
  int i, num_active_server = 0;

  if (hb_Deactivate_info.info_started == true)
    {
      for (i = 0; i < hb_Deactivate_info.server_count; i++)
	{
	  if (hb_Deactivate_info.server_pid_list[i] > 0)
	    {
	      if (kill (hb_Deactivate_info.server_pid_list[i], 0) != 0
		  && errno == ESRCH)
		{
		  /* server was terminated */
		  hb_Deactivate_info.server_pid_list[i] = 0;
		}
	      else
		{
		  num_active_server++;
		}
	    }
	}

      return num_active_server;
    }

  return 0;
}

/*
 * hb_finish_deactivate_server_info -
 *     clear hb_Server_deactivate_info.
 *     and set info_started flag to false.
 *
 *   return: none
 */
void
hb_finish_deactivate_server_info (void)
{
  if (hb_Deactivate_info.server_pid_list != NULL)
    {
      free_and_init (hb_Deactivate_info.server_pid_list);
    }

  hb_Deactivate_info.server_count = 0;
  hb_Deactivate_info.info_started = false;
}

/*
 * hb_return_proc_state_by_fd() -
 *   return: process state
 *
 *   sfd(in):
 */
int
hb_return_proc_state_by_fd (int sfd)
{
  int state = 0;
  HB_PROC_ENTRY *proc;

  if (hb_Resource == NULL)
    {
      return HB_PSTATE_UNKNOWN;
    }

  pthread_mutex_lock (&hb_Resource->lock);
  proc = hb_return_proc_by_fd (sfd);
  if (proc == NULL)
    {
      pthread_mutex_unlock (&hb_Resource->lock);
      return HB_PSTATE_DEAD;
    }

  state = (int) proc->server_state;

  if (proc->server_hang)
    {
      state = HB_PSTATE_DEAD;
    }
  pthread_mutex_unlock (&hb_Resource->lock);

  return state;
}

/*
 * hb_is_hang_process() -
 *   return:
 *
 *   sfd(in):
 */
bool
hb_is_hang_process (int sfd)
{
  HB_PROC_ENTRY *proc;

  if (hb_Resource == NULL)
    {
      return false;
    }

  pthread_mutex_lock (&hb_Resource->lock);
  proc = hb_return_proc_by_fd (sfd);
  if (proc == NULL)
    {
      pthread_mutex_unlock (&hb_Resource->lock);
      return false;
    }

  if (proc->server_hang)
    {
      pthread_mutex_unlock (&hb_Resource->lock);
      return true;
    }
  pthread_mutex_unlock (&hb_Resource->lock);

  return false;
}

/*
 * hb_cluster_set_node_state() -
 */
static void
hb_cluster_set_node_state (HB_NODE_ENTRY * node, HA_STATE node_state)
{
  /* 1. change shm node state */
  if (node->node_state != node_state)
    {
      master_shm_set_node_state (&node->node_info, node_state);
    }

  /* 2. update node's state */
  node->node_state = node_state;
}

static void
hb_cluster_set_node_version (HB_NODE_ENTRY * node,
			     const RYE_VERSION * node_version)
{
  master_shm_set_node_version (&node->node_info, node_version);
  node->node_version = *node_version;
}

/*
 * hb_shm_reset_hb_node ()
 *   return: none
 */
static void
hb_shm_reset_hb_node (void)
{
  HB_NODE_ENTRY *node;
  int num_nodes;
  RYE_SHM_HA_NODE ha_nodes[SHM_MAX_HA_NODE_LIST];

  num_nodes = 0;
  for (node = hb_Cluster->nodes; node; node = node->next)
    {
      ha_nodes[num_nodes].node_info = node->node_info;
      ha_nodes[num_nodes].node_state = node->node_state;
      ha_nodes[num_nodes].priority = node->priority;

      if (hb_Cluster->myself == node)
	{
	  ha_nodes[num_nodes].is_localhost = true;
	}
      else
	{
	  ha_nodes[num_nodes].is_localhost = false;
	}

      num_nodes++;
    }

  master_shm_reset_hb_nodes (ha_nodes, num_nodes);
}

static void
shm_master_update_server_state (HB_PROC_ENTRY * proc)
{
  if (proc->type != HB_PTYPE_SERVER)
    {
      return;
    }

  rye_server_shm_set_state (proc->argv[1], proc->server_state);
}
