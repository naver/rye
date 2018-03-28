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
 * master_heartbeat.h - heartbeat module in rye_master
 */

#ifndef _MASTER_HEARTBEAT_H_
#define _MASTER_HEARTBEAT_H_

#ident "$Id$"


#include "system_parameter.h"
#include "porting.h"
#include "master_util.h"
#include "heartbeat.h"
#include "tcp.h"

/* ping result */
enum HB_PING_RESULT
{
  HB_PING_UNKNOWN = -1,
  HB_PING_SUCCESS = 0,
  HB_PING_USELESS_HOST = 1,
  HB_PING_SYS_ERR = 2,
  HB_PING_FAILURE = 3
};

#define HB_PING_UNKNOWN_STR          "UNKNOWN"
#define HB_PING_SUCCESS_STR          "SUCCESS"
#define HB_PING_USELESS_HOST_STR     "SKIPPED"
#define HB_PING_SYS_ERR_STR          "ERROR"
#define HB_PING_FAILURE_STR          "FAILURE"
#define HB_PING_STR_SIZE             (7)

/* heartbeat jobs */
enum HB_JOBS
{
  HB_CJOB_INIT = 0,
  HB_CJOB_HEARTBEAT = 1,
  HB_CJOB_CALC_SCORE = 2,
  HB_CJOB_CHECK_PING = 3,
  HB_CJOB_FAILOVER = 4,
  HB_CJOB_FAILBACK = 5,
  HB_CJOB_CHECK_VALID_PING_SERVER = 6,
  HB_CJOB_DEMOTE = 7,
  HB_CJOB_CHANGE_SLAVE = 8,
  HB_CJOB_CHANGEMODE_FORCE = 9,

  HB_RJOB_PROC_START = 100,
  HB_RJOB_CONFIRM_START = 101,
  HB_RJOB_SYNC_SERVER_STATE = 102,
  HB_RJOB_DEMOTE_START_SHUTDOWN = 103,
  HB_RJOB_DEMOTE_CONFIRM_SHUTDOWN = 104,
  HB_RJOB_CLEANUP_ALL = 105,
  HB_RJOB_CONFIRM_CLEANUP_ALL = 106,
  HB_RJOB_CHANGE_GROUPID_BITMAP = 107,
  HB_RJOB_RELOAD_NODES = 108,
  HB_JOB_MAX
};

/*  heartbet resource process state
 *  When change this, must be change the SERVER_STATE.
 *  broker.c : enum SERVER_STATE */
typedef enum _hb_proc_state HB_PROC_STATE;
enum _hb_proc_state
{
  HB_PSTATE_UNKNOWN = 0,
  HB_PSTATE_DEAD = 1,
  HB_PSTATE_DEREGISTERED = 2,
  HB_PSTATE_STARTED = 3,
  HB_PSTATE_NOT_REGISTERED = 4,
  HB_PSTATE_REGISTERED = 5
};

#define HB_PSTATE_UNKNOWN_STR                   "unknown"
#define HB_PSTATE_DEAD_STR                      "dead"
#define HB_PSTATE_DEREGISTERED_STR              "deregistered"
#define HB_PSTATE_STARTED_STR                   "started"
#define HB_PSTATE_NOT_REGISTERED_STR            "not_registered"
#define HB_PSTATE_REGISTERED_STR                "registered"

#define HB_REPLICA_PRIORITY                     0x7FFF

/* heartbeat node score bitmask */
#define HB_NODE_SCORE_MASTER                    0x8000
#define HB_NODE_SCORE_TO_BE_MASTER              0x9000
#define HB_NODE_SCORE_SLAVE                     0xA000
#define HB_NODE_SCORE_TO_BE_SLAVE               0xB000
#define HB_NODE_SCORE_UNKNOWN                   0x7FFF

#define HB_BUFFER_SZ                            (4096)
#define HB_MAX_NUM_NODES                        (8)
#define HB_MAX_NUM_RESOURCE_PROC                (16)
#define HB_MAX_PING_CHECK                       (3)
#define HB_MAX_WAIT_FOR_NEW_MASTER              (60)
#define HB_MAX_CHANGEMODE_DIFF_TO_TERM		(12)
#define HB_MAX_CHANGEMODE_DIFF_TO_KILL		(24)

/* various strings for er_set */
#define HB_RESULT_SUCCESS_STR                   "Success"
#define HB_RESULT_FAILURE_STR                   "Failure"

#define HB_CMD_ACTIVATE_STR                     "activate"
#define HB_CMD_DEACTIVATE_STR                   "deactivate"
#define HB_CMD_DEREGISTER_STR                   "deregister"
#define HB_CMD_RELOAD_STR                       "reload"
#define HB_CMD_CHANGEMODE_STR                   "changemode"

enum HB_HOST_CHECK_RESULT
{
  HB_HC_ELIGIBLE_LOCAL,
  HB_HC_ELIGIBLE_REMOTE,
  HB_HC_UNAUTHORIZED,
  HB_HC_FAILED
};

enum HB_NOLOG_REASON
{
  HB_NOLOG_DEMOTE_ON_DISK_FAIL,
  HB_NOLOG_REMOTE_STOP,
  HB_NOLOG_MAX = HB_NOLOG_REMOTE_STOP
};

/* time related macro */
#define HB_GET_ELAPSED_TIME(end_time, start_time) \
            ((double)(end_time.tv_sec - start_time.tv_sec) * 1000 + \
             (end_time.tv_usec - start_time.tv_usec)/1000.0)

#define HB_IS_INITIALIZED_TIME(arg_time) \
            ((arg_time.tv_sec == 0 && arg_time.tv_usec == 0) ? 1 : 0)

#define HB_PROC_RECOVERY_DELAY_TIME		(30* 1000)      /* milli-second */

/* heartbeat list */
typedef struct hb_list HB_LIST;
struct hb_list
{
  HB_LIST *next;
  HB_LIST **prev;
};

/* heartbeat node entries */
typedef struct hb_node_entry HB_NODE_ENTRY;
struct hb_node_entry
{
  HB_NODE_ENTRY *next;
  HB_NODE_ENTRY **prev;

  PRM_NODE_INFO node_info;
  unsigned short priority;
  HA_STATE node_state;
  short score;
  short heartbeat_gap;

  struct timeval last_recv_hbtime;      /* last received heartbeat time */
  RYE_VERSION node_version;
};

/* heartbeat ping host entries */
typedef struct hb_ping_host_entry HB_PING_HOST_ENTRY;
struct hb_ping_host_entry
{
  HB_PING_HOST_ENTRY *next;
  HB_PING_HOST_ENTRY **prev;

  char host_name[MAXHOSTNAMELEN];
  int ping_result;
};

/* herartbeat cluster */
typedef struct hb_cluster HB_CLUSTER;
struct hb_cluster
{
  pthread_mutex_t lock;

  SOCKET sfd;

  HA_STATE node_state;
  char group_id[HB_MAX_GROUP_ID_LEN];
  PRM_NODE_INFO my_node_info;

  int num_nodes;
  HB_NODE_ENTRY *nodes;

  HB_NODE_ENTRY *myself;
  HB_NODE_ENTRY *master;

  bool shutdown;
  bool hide_to_demote;
  bool is_isolated;
  bool is_ping_check_enabled;

  HB_PING_HOST_ENTRY *ping_hosts;
  int num_ping_hosts;
};

/* heartbeat processs entries */
typedef struct hb_proc_entry HB_PROC_ENTRY;
struct hb_proc_entry
{
  HB_PROC_ENTRY *next;
  HB_PROC_ENTRY **prev;

  HB_PROC_STATE state;          /* process state */
  HB_PROC_TYPE type;
  HA_STATE server_state;

  int sfd;

  int pid;
  char exec_path[HB_MAX_SZ_PROC_EXEC_PATH];
  char args[HB_MAX_SZ_PROC_ARGS];
  char argv[HB_MAX_NUM_PROC_ARGV][HB_MAX_SZ_PROC_ARGV];

  struct timeval frtime;        /* first registered time */
  struct timeval rtime;         /* registerd time */
  struct timeval dtime;         /* deregistered time */
  struct timeval ktime;         /* shutdown time */
  struct timeval stime;         /* start time */

  unsigned short changemode_rid;
  unsigned short changemode_gap;

  LOG_LSA prev_eof;
  LOG_LSA curr_eof;

  CSS_CONN_ENTRY *conn;

  bool sync_groupid_bitmap;

  bool being_shutdown;          /* whether the proc is being shut down */
  bool server_hang;
};

/* heartbeat resources */
typedef struct hb_resource HB_RESOURCE;
struct hb_resource
{
  pthread_mutex_t lock;

  HA_STATE node_state;

  int num_procs;
  HB_PROC_ENTRY *procs;

  bool shutdown;
};

/* heartbeat cluster job argument */
typedef struct hb_cluster_job_arg HB_CLUSTER_JOB_ARG;
struct hb_cluster_job_arg
{
  unsigned int ping_check_count;
  unsigned int retries;         /* job retries */
};

/* heartbeat resource job argument */
typedef struct hb_resource_job_arg HB_RESOURCE_JOB_ARG;
struct hb_resource_job_arg
{
  int pid;                      /* process id */

  char args[HB_MAX_SZ_PROC_ARGS];       /* args */

  int retries;                  /* job retries */
  int max_retries;              /* job max retries */

};

/* heartbeat job argument */
typedef union hb_job_arg HB_JOB_ARG;
union hb_job_arg
{
  HB_CLUSTER_JOB_ARG cluster_job_arg;
  HB_RESOURCE_JOB_ARG resource_job_arg;
};

typedef void (*HB_REQ_FUNC) (HB_JOB_ARG *);

/* timer job queue entries */
typedef struct hb_job_entry HB_JOB_ENTRY;
struct hb_job_entry
{
  HB_JOB_ENTRY *next;
  HB_JOB_ENTRY **prev;

  unsigned int type;

  struct timeval expire;

  HB_JOB_ARG *arg;
};

typedef struct hbnew_job_entry HBNEW_JOB_ENTRY;
struct hbnew_job_entry
{
  struct timeval expire;

  int request_id;
  HB_REQ_FUNC func;

  int arg_type;
  HB_JOB_ARG arg;
};

/* timer job queue */
typedef struct hb_job HB_JOB;
struct hb_job
{
  pthread_mutex_t lock;

  unsigned short num_jobs;
  HB_JOB_ENTRY *jobs;

  bool shutdown;
};

#define HB_PROC_IS_MASTER_SERVER(proc)                       \
  (    (proc)->type == HB_PTYPE_SERVER                       \
    && (proc)->state == HB_PSTATE_REGISTERED                 \
    && ((proc)->server_state == HA_STATE_MASTER              \
        || (proc)->server_state == HA_STATE_TO_BE_MASTER))

extern bool hb_Deactivate_immediately;

extern int hb_master_init (void);
extern void hb_resource_shutdown_and_cleanup (void);
extern void hb_cluster_shutdown_and_cleanup (void);

extern void hb_cleanup_conn_and_start_process (CSS_CONN_ENTRY * conn);

extern char *hb_get_node_info_string (bool verbose_yn);
extern char *hb_get_process_info_string (bool verbose_yn);
extern char *hb_get_ping_host_info_string (void);
extern char *hb_get_admin_info_string (void);

extern int hb_reconfig_heartbeat (void);
extern int hb_changemode (HA_STATE req_node_state, bool force);
extern int hb_prepare_deactivate_heartbeat (void);
extern int hb_deactivate_heartbeat (void);
extern int hb_activate_heartbeat (void);

extern int hb_process_start (const HA_CONF * ha_conf);

extern bool hb_is_registered_processes (CSS_CONN_ENTRY * conn);
extern int hb_resource_register_new_proc (HBP_PROC_REGISTER * proc_reg, CSS_CONN_ENTRY * conn);
extern void hb_resource_receive_changemode (CSS_CONN_ENTRY * conn, int state);

extern int hb_check_request_eligibility (SOCKET sd, int *result);
extern void hb_start_deactivate_server_info (void);
extern int hb_get_deactivating_server_count (void);
extern bool hb_is_deactivation_started (void);
extern bool hb_is_deactivation_ready (void);
extern void hb_finish_deactivate_server_info (void);

extern void hb_enable_er_log (void);
extern void hb_disable_er_log (int reason, const char *msg_fmt, ...);

extern int hb_return_proc_state_by_fd (int sfd);
extern bool hb_is_hang_process (int sfd);
#endif /* _MASTER_HEARTBEAT_H_ */
