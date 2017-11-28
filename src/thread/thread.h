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
 * thread.h - threads wrapper for pthreads and WIN32 threads.
 */

#ifndef _THREAD_H_
#define _THREAD_H_

#ident "$Id$"

#if defined(SERVER_MODE)
#include <sys/types.h>
#include <pthread.h>

#include "porting.h"
#include "error_manager.h"
//#include "adjustable_array.h"
#include "system_parameter.h"
#endif /* SERVER_MODE */

#if !defined(SERVER_MODE)
#define THREAD_GET_CURRENT_ENTRY_INDEX(thrd) thread_get_current_entry_index()

extern int thread_Recursion_depth;

#define thread_get_thread_entry_info()  (NULL)
#define thread_num_worker_threads()  (1)
#define thread_num_total_threads()   (1)
#define thread_get_current_entry_index() (0)
#define thread_trace_on(thread_p)
#define thread_set_trace_format(thread_p, format)
#define thread_is_on_trace(thread_p) (false)
#define thread_set_clear_trace(thread_p, clear)
#define thread_need_clear_trace(thread_p) (false)
#define thread_get_sort_stats_active(thread_p) (false)
#define thread_set_sort_stats_active(thread_p, flag)

#define thread_get_recursion_depth(thread_p) (thread_Recursion_depth)
#define thread_inc_recursion_depth(thread_p) (thread_Recursion_depth ++)
#define thread_dec_recursion_depth(thread_p) (thread_Recursion_depth --)
#define thread_clear_recursion_depth(thread_p) (thread_Recursion_depth = 0)

#define server_stats_dump(fp)
// #define server_stats_add_wait_time(thread_p, stats_type, sub_type, wait_start)
// #define server_stats_set_current_wait_time(thread_p, stats_type, wait_start)
// #define server_stats_add_current_wait_time(thread_p, stats_type, sub_type)

#define thread_mnt_track_push(thread_p, item, status)
#define thread_mnt_track_pop(thread_p, status)
#define thread_mnt_track_dump(thread_p)
#define thread_mnt_track_counter(thread_p, value, exec_time)

typedef void THREAD_ENTRY;
#else /* !SERVER_MODE */

#define THREAD_GET_CURRENT_ENTRY_INDEX(thrd) \
  ((thrd) ? (thrd)->index : thread_get_current_entry_index())

typedef struct thread_entry THREAD_ENTRY;

typedef void *CSS_THREAD_ARG;

typedef int (*CSS_THREAD_FN) (THREAD_ENTRY * thrd, CSS_THREAD_ARG);

enum
{ TS_DEAD = 0, TS_FREE, TS_RUN, TS_WAIT, TS_CHECK };
enum
{ THREAD_RESUME_NONE = 0,
  THREAD_RESUME_DUE_TO_INTERRUPT = 1,
  THREAD_RESUME_DUE_TO_SHUTDOWN = 2,
  THREAD_PGBUF_SUSPENDED = 3,
  THREAD_PGBUF_RESUMED = 4,
  THREAD_JOB_QUEUE_SUSPENDED = 5,
  THREAD_JOB_QUEUE_RESUMED = 6,
  THREAD_CSECT_READER_SUSPENDED = 7,
  THREAD_CSECT_READER_RESUMED = 8,
  THREAD_CSECT_WRITER_SUSPENDED = 9,
  THREAD_CSECT_WRITER_RESUMED = 10,
  THREAD_CSECT_PROMOTER_SUSPENDED = 11,
  THREAD_CSECT_PROMOTER_RESUMED = 12,
  THREAD_CSS_QUEUE_SUSPENDED = 13,
  THREAD_CSS_QUEUE_RESUMED = 14,
  THREAD_QMGR_ACTIVE_QRY_SUSPENDED = 15,
  THREAD_QMGR_ACTIVE_QRY_RESUMED = 16,
  THREAD_QMGR_MEMBUF_PAGE_SUSPENDED = 17,
  THREAD_QMGR_MEMBUF_PAGE_RESUMED = 18,
  THREAD_HEAP_CLSREPR_SUSPENDED = 19,
  THREAD_HEAP_CLSREPR_RESUMED = 20,
  THREAD_LOCK_SUSPENDED = 21,
  THREAD_LOCK_RESUMED = 22,
  THREAD_LOGWR_SUSPENDED = 23,
  THREAD_LOGWR_RESUMED = 24
};
enum
{ TT_MASTER, TT_CON_HANDLER, TT_WORKER, TT_DAEMON, TT_NONE };

enum
{ THREAD_STOP_WORKERS_EXCEPT_LOGWR, THREAD_STOP_LOGWR };

/* stats for event logging */
typedef struct event_stat EVENT_STAT;
struct event_stat
{
  /* slow query stats */
  struct timeval cs_waits;
  struct timeval lock_waits;
  struct timeval latch_waits;

  /* temp volume expand stats */
  struct timeval temp_expand_time;
  int temp_expand_pages;

  /* save PRM_ID_SQL_TRACE_SLOW for performance */
  bool trace_slow_query;

  /* log flush thread wait time */
  int trace_log_flush_time;
};

#if 0
typedef enum server_stats_type SERVER_STATS_TYPE;
enum server_stats_type
{
  SERVER_STATS_CS
};

/* stats for server trace */
typedef struct server_trace_stat SERVER_TRACE_STAT;
struct server_trace_stat
{
  /* save PRM_ID_SERVER_TRACE for performance */
  bool server_trace;

  struct timeval current_wait_time;

  struct timeval cs_total_wait_time;
  struct timeval *cs_wait_time;
};
#endif

/*
 * fetches sub-info
 */
#define THREAD_MNT_TRACK_MAX 10

typedef struct thread_mnt_track THREAD_MNT_TRACK;
struct thread_mnt_track
{
  int item;			/* MNT_SERVER_ITEM */
};

typedef enum
{
  JOB_QUEUE_CLIENT = 0,
  JOB_QUEUE_CLOSE,
  JOB_QUEUE_BACKUP_READ,

  JOB_QUEUE_NA,

  JOB_QUEUE_TYPE_MIN = 0,
  JOB_QUEUE_TYPE_MAX = JOB_QUEUE_NA
} JOB_QUEUE_TYPE;

struct thread_entry
{
  int index;			/* thread entry index */
  int index_in_group;		/* thread index in each group
				   (con_handlers, worker, daemon) */
  JOB_QUEUE_TYPE job_queue_type;	/* waiting job queue type for worker thread */
  int type;			/* thread type */
  pthread_t tid;		/* thread id */
  int client_id;		/* client id whom this thread is responding */
  int tran_index;		/* tran index to which this thread belongs */
  pthread_mutex_t tran_index_lock;
  unsigned int rid;		/* request id which this thread is processing */
  int status;			/* thread status */

  pthread_mutex_t th_entry_lock;	/* latch for this thread entry */
  pthread_mutex_t th_job_lock;	/* mutex for job queue handling */
  pthread_cond_t wakeup_cond;	/* wakeup condition */

  HL_HEAPID private_heap_id;	/* id of thread private memory allocator */
#if defined (ENABLE_UNUSED_FUNCTION)
  ADJ_ARRAY *cnv_adj_buffer[3];	/* conversion buffer */
#endif

  struct css_conn_entry *conn_entry;	/* conn entry ptr */

  ER_MSG_INFO er_msg;		/* error msg info */

  void *xasl_unpack_info_ptr;	/* XASL_UNPACK_INFO * */
  int xasl_errcode;		/* xasl errorcode */
  int xasl_recursion_depth;

  unsigned int rand_seed;	/* seed for rand_r() */
  struct drand48_data rand_buf;	/* seed for lrand48_r(), drand48_r() */

  char qp_num_buf[81];		/* buffer which contains number as
				   string form;
				   used in the qp/numeric_db_value_print() */

  int resume_status;		/* resume status */
  int request_latch_mode;	/* for page latch support */
  int request_fix_count;
  bool victim_request_fail;
  bool interrupted;		/* is this request/transaction interrupted ? */
  bool shutdown;		/* is server going down? */
  bool check_interrupt;		/* check_interrupt == false, during
				   fl_alloc* function call. */
  bool check_page_validation;	/* check_page_validation == false, during
				   btree_handle_prev_leaf_after_locking()
				   or btree_handle_curr_leaf_after_locking()
				   function call. */
  struct thread_entry *next_wait_thrd;

  void *lockwait;
  INT64 lockwait_stime;		/* time in milliseconds */
  int lockwait_msecs;		/* time in milliseconds */
  int lockwait_state;
  void *query_entry;
  struct thread_entry *tran_next_wait;
  struct thread_entry *worker_thrd_list;	/* worker thrd on jobq list */

  void *log_zip_undo;
  void *log_zip_redo;
  char *log_data_ptr;
  int log_data_length;

  EVENT_STAT event_stats;

#if 0
  SERVER_TRACE_STAT server_stats;
#endif

  int mnt_track_top;
  THREAD_MNT_TRACK mnt_track_stack[THREAD_MNT_TRACK_MAX];

  /* for query profile */
  int trace_format;
  bool on_trace;
  bool clear_trace;
  bool sort_stats_active;

#if !defined(NDEBUG)
  struct fi_test_item *fi_test_array;
#endif

  int last_error_position;
  const char *last_error_file_name;
  void *recv_packet;

  bool check_groupid;		/* check_groupid == false, iff client is
				 * HA applier, migrator, group GC */
};

#define DOES_THREAD_RESUME_DUE_TO_SHUTDOWN(thread_p) \
  ((thread_p)->resume_status == THREAD_RESUME_DUE_TO_INTERRUPT && \
   (thread_p)->interrupted == true)

typedef struct daemon_thread_monitor DAEMON_THREAD_MONITOR;
struct daemon_thread_monitor
{
  int thread_index;
  bool is_available;
  bool is_running;
  int nrequestors;
  pthread_mutex_t lock;
  pthread_cond_t cond;
};

#define DAEMON_THREAD_MONITOR_INITIALIZER  \
  {0, false, false, 0, PTHREAD_MUTEX_INITIALIZER,PTHREAD_COND_INITIALIZER}

extern DAEMON_THREAD_MONITOR thread_Log_flush_thread;

extern int thread_set_thread_entry_info (THREAD_ENTRY * entry);

extern THREAD_ENTRY *thread_get_thread_entry_info (void);

extern int thread_initialize_manager (void);
extern int thread_start_workers (void);
extern int thread_stop_active_workers (unsigned short stop_phase);
extern int thread_stop_active_daemons (void);
extern int thread_kill_all_workers (void);
extern void thread_final_manager (void);
extern void thread_slam_tran_index (THREAD_ENTRY * thread_p, int tran_index);
extern int thread_unlock_entry (THREAD_ENTRY * p);
extern int thread_suspend_wakeup_and_unlock_entry (THREAD_ENTRY * p,
						   int suspended_reason);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int thread_suspend_timeout_wakeup_and_unlock_entry (THREAD_ENTRY * p,
							   struct timespec *t,
							   int
							   suspended_reason);
extern int thread_suspend_wakeup_and_unlock_entry_with_tran_index (int
								   tran_index,
								   int
								   suspended_reason);
#endif
extern int thread_wakeup (THREAD_ENTRY * p, int resume_reason);
extern int thread_check_suspend_reason_and_wakeup (THREAD_ENTRY * thread_p,
						   int resume_reason,
						   int suspend_reason);

extern int thread_wakeup_already_had_mutex (THREAD_ENTRY * p,
					    int resume_reason);
extern int thread_wakeup_with_tran_index (int tran_index, int resume_reason);

#if defined (ENABLE_UNUSED_FUNCTION)
extern ADJ_ARRAY *css_get_cnv_adj_buffer (int idx);
extern void css_set_cnv_adj_buffer (int idx, ADJ_ARRAY * buffer);
extern int thread_is_manager_initialized (void);
extern void thread_waiting_for_function (THREAD_ENTRY * thread_p,
					 CSS_THREAD_FN func,
					 CSS_THREAD_ARG arg);
#endif
extern void thread_sleep (double);
extern void thread_get_info_threads (int *num_total_threads,
				     int *num_worker_threads,
				     int *num_free_threads,
				     int *num_suspended_threads);
extern int thread_num_worker_threads (void);
extern int thread_num_total_threads (void);
extern int thread_num_con_handler_threads (void);
extern int thread_max_workers_by_queue_type (JOB_QUEUE_TYPE q_type);
extern int thread_max_backup_readers (void);

extern int thread_get_client_id (THREAD_ENTRY * thread_p);
extern unsigned int thread_get_comm_request_id (THREAD_ENTRY * thread_p);
#if defined (ENABLE_UNUSED_FUNCTION)
extern void thread_set_comm_request_id (unsigned int rid);
#endif
extern THREAD_ENTRY *thread_find_entry_by_tran_index_except_me (int
								tran_index);
extern int thread_get_current_entry_index (void);
#if defined (ENABLE_UNUSED_FUNCTION)
extern void thread_wakeup_session_control_thread (void);
extern void thread_wakeup_check_ha_delay_info_thread (void);
#endif
extern struct css_conn_entry *thread_get_current_conn_entry (void);
extern int thread_has_threads (THREAD_ENTRY * caller, int tran_index,
			       int client_id);
extern bool thread_set_check_interrupt (THREAD_ENTRY * thread_p, bool flag);
extern bool thread_set_check_groupid (THREAD_ENTRY * thread_p, bool flag);
extern bool thread_set_check_page_validation (THREAD_ENTRY * thread_p,
					      bool flag);
extern void thread_wakeup_deadlock_detect_thread (void);
extern void thread_wakeup_log_flush_thread (void);
extern void thread_wakeup_page_flush_thread (void);
#if defined (ENABLE_UNUSED_FUNCTION)
extern void thread_wakeup_flush_control_thread (void);
#endif
extern void thread_wakeup_checkpoint_thread (void);
extern void thread_wakeup_purge_archive_logs_thread (void);
extern void thread_wakeup_oob_handler_thread (void);
extern void thread_wakeup_auto_volume_expansion_thread (void);

extern bool thread_is_page_flush_thread_available (void);

extern bool thread_auto_volume_expansion_thread_is_running (void);

extern THREAD_ENTRY *thread_find_first_lockwait_entry (int *thrd_index);
extern THREAD_ENTRY *thread_find_next_lockwait_entry (int *thrd_index);
extern THREAD_ENTRY *thread_find_entry_by_index (int thrd_index);
extern int thread_get_lockwait_entry (int tran_index, THREAD_ENTRY ** array);


extern int thread_suspend_with_other_mutex (THREAD_ENTRY * p,
					    pthread_mutex_t * mutexp,
					    int timeout, struct timespec *to,
					    int suspended_reason);
extern void thread_print_entry_info (THREAD_ENTRY * p);
extern void thread_dump_threads (void);
extern bool thread_get_check_interrupt (THREAD_ENTRY * thread_p);
extern bool thread_get_check_groupid (THREAD_ENTRY * thread_p);
extern bool thread_get_check_page_validation (THREAD_ENTRY * thread_p);

extern int xthread_kill_tran_index (THREAD_ENTRY * thread_p,
				    int kill_tran_index,
				    const char *kill_user,
				    const char *kill_host, int kill_pid);

extern HL_HEAPID css_get_private_heap (THREAD_ENTRY * thread_p);
extern HL_HEAPID css_set_private_heap (THREAD_ENTRY * thread_p,
				       HL_HEAPID heap_id);

extern void thread_set_info (THREAD_ENTRY * thread_p, int client_id, int rid,
			     int tran_index);
extern void thread_trace_on (THREAD_ENTRY * thread_p);
extern void thread_set_trace_format (THREAD_ENTRY * thread_p, int format);
extern bool thread_is_on_trace (THREAD_ENTRY * thread_p);
extern void thread_set_clear_trace (THREAD_ENTRY * thread_p, bool clear);
extern bool thread_need_clear_trace (THREAD_ENTRY * thread_p);
extern bool thread_get_sort_stats_active (THREAD_ENTRY * thread_p);
extern bool thread_set_sort_stats_active (THREAD_ENTRY * thread_p, bool flag);

extern int thread_get_recursion_depth (THREAD_ENTRY * thread_p);
extern void thread_inc_recursion_depth (THREAD_ENTRY * thread_p);
extern void thread_dec_recursion_depth (THREAD_ENTRY * thread_p);
extern void thread_clear_recursion_depth (THREAD_ENTRY * thread_p);

extern INT64 thread_get_log_clock_msec (void);

extern int server_stats_dump (FILE * fp);
#if 0
extern int server_stats_add_wait_time (THREAD_ENTRY * thread_p,
				       SERVER_STATS_TYPE stats_type,
				       int sub_type,
				       struct timeval *wait_start);
extern int server_stats_set_current_wait_time (THREAD_ENTRY * thread_p,
					       SERVER_STATS_TYPE stats_type,
					       struct timeval *wait_start);
extern int server_stats_add_current_wait_time (THREAD_ENTRY * thread_p,
					       SERVER_STATS_TYPE stats_type,
					       int sub_type);
#endif

extern bool thread_is_auto_volume_expansion_thread_available (void);

extern int thread_lock_entry (THREAD_ENTRY * thread_p);

extern void thread_mnt_track_push (THREAD_ENTRY * thread_p, int item,
				   int *status);
extern THREAD_MNT_TRACK *thread_mnt_track_pop (THREAD_ENTRY * thread_p,
					       int *status);
extern void thread_mnt_track_dump (THREAD_ENTRY * thread_p);
extern void thread_mnt_track_counter (THREAD_ENTRY * thread_p, INT64 value,
				      UINT64 exec_time);

#endif /* SERVER_MODE */

#endif /* _THREAD_H_ */
