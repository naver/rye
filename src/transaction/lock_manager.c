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
 * lock_manager.c - lock management module (at the server)
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <string.h>
#include <time.h>
#include <assert.h>

#include "porting.h"
#include "xserver_interface.h"
#include "lock_manager.h"
#include "system_parameter.h"
#include "memory_alloc.h"
#include "oid.h"
#include "storage_common.h"
#include "log_manager.h"
#include "transaction_sr.h"
#include "wait_for_graph.h"
#include "critical_section.h"
#include "memory_hash.h"
#include "locator.h"
#include "perf_monitor.h"
#include "page_buffer.h"
#include "message_catalog.h"
#include "environment_variable.h"
#include "log_impl.h"
#include "thread.h"
#include "query_manager.h"
#include "event_log.h"
#include "object_print.h"
#include "transform.h"

#ifndef DB_NA
#define DB_NA           2
#endif
extern int lock_Comp[5][5];

#if defined (SERVER_MODE)

/* thread is lock-waiting ? */
#define LK_IS_LOCKWAIT_THREAD(thrd)                             \
        ((thrd)->lockwait != NULL                               \
         && (thrd)->lockwait_state == (int)LOCK_SUSPENDED)

/* transaction wait for only some msecs ? */
#define LK_CAN_TIMEOUT(msecs)    ((msecs) != LK_INFINITE_WAIT)

/* is younger transaction ? */
#define LK_ISYOUNGER(young_tranid, old_tranid) (young_tranid > old_tranid)

/* Defines for printing lock activity messages */
#define LK_MSG_LOCK_HELPER(entry, msgnum)                               \
  fprintf(stdout, \
      msgcat_message (MSGCAT_CATALOG_RYE, MSGCAT_SET_LOCK, msgnum)), \
      (entry)->tran_index, LOCK_TO_LOCKMODE_STRING((entry)->granted_mode),\
      (entry)->res_head->oid->volid, (entry)->res_head->oid->pageid,    \
      (entry)->oid->slotid);

#define LK_MSG_LOCK_ACQUIRED(entry)                                     \
  LK_MSG_LOCK_HELPER(entry, MSGCAT_LK_OID_LOCK_ACQUIRED)

#define LK_MSG_LOCK_CONVERTED(entry)                                    \
  LK_MSG_LOCK_HELPER(entry, MSGCAT_LK_OID_LOCK_CONVERTED)

#define LK_MSG_LOCK_WAITFOR(entry)                                      \
  LK_MSG_LOCK_HELPER(entry, MSGCAT_LK_OID_LOCK_WAITFOR)

#define LK_MSG_LOCK_RELEASE(entry)                                      \
  LK_MSG_LOCK_HELPER(entry, MSGCAT_LK_OID_LOCK_RELEASE)

#define LK_MSG_LOCK_DEMOTE(entry)                                       \
  LK_MSG_LOCK_HELPER(entry, MSGCAT_LK_OID_LOCK_DEMOTE)

#define EXPAND_WAIT_FOR_ARRAY_IF_NEEDED()                               \
  do                                                                    \
  {                                                                     \
    if (nwaits == max_waits)                                            \
      {                                                                 \
	if (wait_for == wait_for_buf)                                   \
	  {                                                             \
	    t = (int *) malloc (sizeof (int) * max_waits * 2);          \
	    if (t != NULL)                                              \
	      {                                                         \
		memcpy (t, wait_for, sizeof (int) * max_waits);    \
              }                                                         \
          }                                                             \
	else                                                            \
	  {                                                             \
	    t = (int *) realloc (wait_for, sizeof (int) * max_waits * 2);\
          }                                                             \
	if (t != NULL)                                                  \
	  {                                                             \
            wait_for = t;                                               \
            max_waits *= 2;                                             \
          }                                                             \
	else                                                            \
	  {                                                             \
            goto set_error;                                             \
          }                                                             \
      }                                                                 \
  }                                                                     \
  while (0)
#endif /* SERVER_MODE */

#define RESOURCE_ALLOC_WAIT_TIME 10	/* 10 msec */
#define MAX_NUM_LOCKS_DUMP_TO_EVENT_LOG 100

/* state of suspended threads */
typedef enum
{
  LOCK_SUSPENDED,		/* Thread has been suspended */
  LOCK_RESUMED,			/* Thread has been resumed */
  LOCK_RESUMED_TIMEOUT,		/* Thread has been resumed and notified of
				   lock timeout */
  LOCK_RESUMED_DEADLOCK_TIMEOUT,	/* Thread has been resumed and notified of
					   lock timeout because the current transaction
					   is selected as a deadlock victim */
  LOCK_RESUMED_ABORTED,		/* Thread has been resumed, however
				   it must be aborted because of a deadlock */
#if defined (ENABLE_UNUSED_FUNCTION)
  LOCK_RESUMED_ABORTED_FIRST,	/* in case of the first aborted thread */
  LOCK_RESUMED_ABORTED_OTHER,	/* in case of other aborted threads */
#endif
  LOCK_RESUMED_INTERRUPT
} LOCK_WAIT_STATE;

/*
 * Message id in the set MSGCAT_SET_LOCK
 * in the message catalog MSGCAT_CATALOG_RYE (file rye.msg).
 */
#define MSGCAT_LK_NEWLINE                       1
#define MSGCAT_LK_SUSPEND_TRAN                  2
#define MSGCAT_LK_RESUME_TRAN                   3
#define MSGCAT_LK_OID_LOCK_ACQUIRED             4
#define MSGCAT_LK_VPID_LOCK_ACQUIRED            5
#define MSGCAT_LK_OID_LOCK_CONVERTED            6
#define MSGCAT_LK_VPID_LOCK_CONVERTED           7
#define MSGCAT_LK_OID_LOCK_WAITFOR              8
#define MSGCAT_LK_VPID_LOCK_WAITFOR             9
#define MSGCAT_LK_OID_LOCK_RELEASE              10
#define MSGCAT_LK_VPID_LOCK_RELEASE             11
#define MSGCAT_LK_OID_LOCK_DEMOTE               12
#define MSGCAT_LK_VPID_LOCK_DEMOTE              13
#define MSGCAT_LK_RES_OID                       14
#define MSGCAT_LK_RES_ROOT_CLASS_TYPE           15
#define MSGCAT_LK_RES_CLASS_TYPE                16
#define MSGCAT_LK_RES_INSTANCE_TYPE             17
#define MSGCAT_LK_RES_UNKNOWN_TYPE              18
#define MSGCAT_LK_RES_TOTAL_MODE                19
#define MSGCAT_LK_RES_LOCK_COUNT                20
#define MSGCAT_LK_RES_NON_BLOCKED_HOLDER_HEAD   21
#define MSGCAT_LK_RES_BLOCKED_HOLDER_HEAD       22
#define MSGCAT_LK_RES_BLOCKED_WAITER_HEAD       23
#define MSGCAT_LK_RES_NON2PL_RELEASED_HEAD      24
#define MSGCAT_LK_RES_NON_BLOCKED_HOLDER_ENTRY  25
#define MSGCAT_LK_RES_NON_BLOCKED_HOLDER_ENTRY_WITH_GRANULE 26
#define MSGCAT_LK_RES_BLOCKED_HOLDER_ENTRY      27
#define MSGCAT_LK_RES_BLOCKED_HOLDER_ENTRY_WITH_GRANULE 28
#define MSGCAT_LK_RES_BLOCKED_WAITER_ENTRY      29
#define MSGCAT_LK_RES_NON2PL_RELEASED_ENTRY     30
#define MSGCAT_LK_RES_VPID                      31
#define MSGCAT_LK_DUMP_LOCK_TABLE               32
#define MSGCAT_LK_DUMP_TRAN_IDENTIFIERS         33
#define MSGCAT_LK_DUMP_TRAN_ISOLATION           34
#define MSGCAT_LK_DUMP_TRAN_STATE               35
#define MSGCAT_LK_DUMP_TRAN_TIMEOUT_PERIOD      36
#define MSGCAT_LK_DEADLOCK_ABORT_HDR            37
#define MSGCAT_LK_DEADLOCK_ABORT                38
#define MSGCAT_LK_DEADLOCK_TIMEOUT_HDR          39
#define MSGCAT_LK_DEADLOCK_TIMEOUT              40
#define MSGCAT_LK_DEADLOCK_FUN_HDR              41
#define MSGCAT_LK_DEADLOCK_FUN                  42
#define MSGCAT_LK_RES_INDEX_KEY_TYPE            43
#define MSGCAT_LK_INDEXNAME                     44
#define MSGCAT_LK_RES_VALUE                     45
#define MSGCAT_LK_RES_VALUE_TYPE                46
#define MSGCAT_LK_LASTONE                       47

#if defined(SERVER_MODE)

typedef struct lk_lockinfo LK_LOCKINFO;
struct lk_lockinfo
{
  OID *org_oidp;
  OID oid;
  OID class_oid;
  LOCK lock;
};

/* TWFG (transaction wait-for graph) entry and edge */
typedef struct lk_WFG_node LK_WFG_NODE;
struct lk_WFG_node
{
  int first_edge;
  int candidate;
  int current;
  int ancestor;
  INT64 thrd_wait_stime;
  int tran_edge_seq_num;
  bool checked_by_deadlock_detector;
  bool DL_victim;
};

typedef struct lk_WFG_edge LK_WFG_EDGE;
struct lk_WFG_edge
{
  int to_tran_index;
  int edge_seq_num;
  int holder_flag;
  int next;
  INT64 edge_wait_stime;
};

typedef struct lk_deadlock_victim LK_DEADLOCK_VICTIM;
struct lk_deadlock_victim
{
  /* following two fields are used for only global deadlock detection */
  int (*cycle_fun) (int tran_index, void *args);
  void *args;			/* Arguments to be passed to cycle_fun */

  int tran_index;		/* Index of selected victim */
  TRANID tranid;		/* Transaction identifier   */
  int can_timeout;		/* Is abort or timeout      */

  int num_trans_in_cycle;	/* # of transaction in cycle */
  int *tran_index_in_cycle;	/* tran_index array for transaction in cycle */
};

/*
 * Lock Hash Entry Structure
 */
typedef struct lk_hash LK_HASH;
struct lk_hash
{
  pthread_mutex_t hash_mutex;	/* hash mutex of the hash chain */
  LK_RES *hash_next;		/* next resource in the hash chain */
};

/*
 * Lock Entry Block Structure
 */
typedef struct lk_entry_block LK_ENTRY_BLOCK;
struct lk_entry_block
{
  LK_ENTRY_BLOCK *next_block;	/* next lock entry block */
  LK_ENTRY *block;		/* lk_entry block */
  int count;			/* # of entries in lock entry block */
};

/*
 * Lock Resource Block Structure
 */
typedef struct lk_res_block LK_RES_BLOCK;
struct lk_res_block
{
  LK_RES_BLOCK *next_block;	/* next lock resource block */
  LK_RES *block;		/* lk_res block */
  int count;			/* # of entries in lock res block */
};

/*
 * Transaction Lock Entry Structure
 */
typedef struct lk_tran_lock LK_TRAN_LOCK;
struct lk_tran_lock
{
  /* transaction lock hold lists */
  pthread_mutex_t hold_mutex;	/* mutex for hold lists */
  LK_ENTRY *inst_hold_list;	/* instance lock hold list */
  int inst_hold_count;		/* # of entries in inst_hold_list */

  LK_ENTRY *waiting;		/* waiting lock entry */
};

/*
 * Lock Manager Global Data Structure
 */

typedef struct lk_global_data LK_GLOBAL_DATA;
struct lk_global_data
{
  /* object lock table including hash table */
  int max_obj_locks;		/* max # of object locks */
  int obj_hash_size;		/* size of object hash table */
  LK_HASH *obj_hash_table;	/* object lock hash table */
  pthread_mutex_t obj_res_block_list_mutex;
  pthread_mutex_t obj_entry_block_list_mutex;
  pthread_mutex_t obj_free_res_list_mutex;
  pthread_mutex_t obj_free_entry_list_mutex;
  LK_RES_BLOCK *obj_res_block_list;	/* lk_res_block list */
  LK_ENTRY_BLOCK *obj_entry_block_list;	/* lk_entry_block list */
  LK_RES *obj_free_res_list;	/* free lk_res list */
  LK_ENTRY *obj_free_entry_list;	/* free lk_entry list */
  int num_obj_res_allocated;

  /* transaction lock table */
  int num_trans;		/* # of transactions */
  LK_TRAN_LOCK *tran_lock_table;	/* transaction lock hold table */

  /* deadlock detection related fields */
  pthread_mutex_t DL_detection_mutex;
  struct timeval last_deadlock_run;	/* last deadlock detetion time */
  LK_WFG_NODE *TWFG_node;	/* transaction WFG node */
  LK_WFG_EDGE *TWFG_edge;	/* transaction WFG edge */
  int max_TWFG_edge;
  int TWFG_free_edge_idx;
  int global_edge_seq_num;

  /* miscellaneous things */
  short no_victim_case_count;
  bool verbose_mode;
#if defined(LK_DUMP)
  int dump_level;
#endif				/* LK_DUMP */
};

LK_GLOBAL_DATA lk_Gl = {
  0, 0, NULL,
  PTHREAD_MUTEX_INITIALIZER, PTHREAD_MUTEX_INITIALIZER,
  PTHREAD_MUTEX_INITIALIZER, PTHREAD_MUTEX_INITIALIZER,
  NULL, NULL, NULL, NULL, 0, 0, NULL,
  PTHREAD_MUTEX_INITIALIZER, {0, 0}, NULL, NULL, 0, 0, 0,
  0, false
#if defined(LK_DUMP)
    , 0
#endif /* LK_DUMP */
};

/* size of each data structure */
static const int SIZEOF_LK_LOCKINFO = sizeof (LK_LOCKINFO);
static const int SIZEOF_LK_WFG_NODE = sizeof (LK_WFG_NODE);
static const int SIZEOF_LK_WFG_EDGE = sizeof (LK_WFG_EDGE);
static const int SIZEOF_LK_TRAN_LOCK = sizeof (LK_TRAN_LOCK);

static const int SIZEOF_LK_ENTRY = sizeof (LK_ENTRY);
static const int SIZEOF_LK_RES = sizeof (LK_RES);
static const int SIZEOF_LK_HASH = sizeof (LK_HASH);
static const int SIZEOF_LK_ENTRY_BLOCK = sizeof (LK_ENTRY_BLOCK);
static const int SIZEOF_LK_RES_BLOCK = sizeof (LK_RES_BLOCK);

/* minimum # of locks that are required */
/* TODO : change const */
#define LK_MIN_OBJECT_LOCKS  (MAX_NTRANS * 300)

/* the ratio in the number of lock entries for each entry type */
static const int LK_HASH_RATIO = 8;
static const int LK_RES_RATIO = 1;
static const int LK_ENTRY_RATIO = 5;

/* the lock entry expansion count */
/* TODO : change const */
#define LK_MORE_RES_COUNT  (MAX_NTRANS * 20 * LK_RES_RATIO)
#define LK_MORE_ENTRY_COUNT (MAX_NTRANS * 20 * LK_ENTRY_RATIO)

/* miscellaneous constants */
static const int LK_SLEEP_MAX_COUNT = 3;
/* TODO : change const */
#define LK_MAX_VICTIM_COUNT  300

/* transaction WFG edge related constants */
static const int LK_MIN_TWFG_EDGE_COUNT = 200;
/* TODO : change const */
#define LK_MID_TWFG_EDGE_COUNT 1000
/* TODO : change const */
#define LK_MAX_TWFG_EDGE_COUNT (MAX_NTRANS * MAX_NTRANS)

#define DEFAULT_WAIT_USERS	10
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
static LK_WFG_EDGE TWFG_edge_block[LK_MID_TWFG_EDGE_COUNT];
static LK_DEADLOCK_VICTIM victims[LK_MAX_VICTIM_COUNT];
static int victim_count;
#else /* !SERVER_MODE */
static int lk_Standalone_has_xlock = 0;
#define LK_SET_STANDALONE_XLOCK(lock)					      \
  do {									      \
    if ((lock) == X_LOCK)						      \
      {									      \
	lk_Standalone_has_xlock = true;					      \
      }									      \
  } while (0)
#endif /* !SERVER_MODE */

static DB_VALUE system_Ddl_key;	/* is system scope; not shard key scope */

static DB_VALUE shard_Catalog_key;	/* is shard key scope */
static DB_VALUE shard_Global_key;	/* is shard key scope */

#if defined(SERVER_MODE)
static void lock_initialize_entry (LK_ENTRY * entry_ptr);
static void lock_initialize_entry_as_granted (LK_ENTRY * entry_ptr,
					      int tran_index,
					      struct lk_res *res, LOCK lock);
static void lock_initialize_entry_as_blocked (LK_ENTRY * entry_ptr,
					      THREAD_ENTRY * thread_p,
					      int tran_index,
					      struct lk_res *res, LOCK lock);
static void lock_initialize_resource (struct lk_res *res_ptr);
static void lock_initialize_resource_as_allocated (THREAD_ENTRY * thread_p,
						   struct lk_res *res_ptr,
						   const DB_VALUE * val,
						   LOCK lock);
static int lock_initialize_tran_lock_table (void);
static int lock_initialize_object_hash_table (void);
static int lock_initialize_object_lock_res_list (void);
static int lock_initialize_object_lock_entry_list (void);
static int lock_initialize_deadlock_detection (void);
static LK_RES *lock_alloc_resource (void);
static void lock_free_resource (THREAD_ENTRY * thread_p, LK_RES * res_ptr);
static int lock_alloc_resource_block (void);
static LK_ENTRY *lock_alloc_entry (void);
static void lock_free_entry (LK_ENTRY * entry_ptr);
static int lock_alloc_entry_block (void);
static int lock_dealloc_resource (THREAD_ENTRY * thread_p, LK_RES * res_ptr);
static void lock_insert_into_tran_hold_list (LK_ENTRY * entry_ptr);
static int lock_delete_from_tran_hold_list (LK_ENTRY * entry_ptr);
#if defined (SERVER_MODE)
static LK_ENTRY *lock_find_tran_hold_entry (int tran_index,
					    const DB_VALUE * val);
#endif
static void lock_position_holder_entry (LK_RES * res_ptr,
					LK_ENTRY * entry_ptr);
static void lock_set_error_for_timeout (THREAD_ENTRY * thread_p,
					LK_ENTRY * entry_ptr);
#if defined (ENABLE_UNUSED_FUNCTION)
static void lock_set_error_for_aborted (LK_ENTRY * entry_ptr,
					TRAN_ABORT_REASON abort_reason);
#endif
static LOCK_WAIT_STATE lock_suspend (THREAD_ENTRY * thread_p,
				     LK_ENTRY * entry_ptr, int wait_msecs);
static void lock_resume (LK_ENTRY * entry_ptr, int state);
static bool lock_wakeup_deadlock_victim_timeout (int tran_index);
#if defined (ENABLE_UNUSED_FUNCTION)
static bool lock_wakeup_deadlock_victim_aborted (int tran_index);
#endif
static void lock_grant_blocked_holder (THREAD_ENTRY * thread_p,
				       LK_RES * res_ptr);
static int lock_grant_blocked_waiter (THREAD_ENTRY * thread_p,
				      LK_RES * res_ptr);
static void lock_grant_blocked_waiter_partial (THREAD_ENTRY * thread_p,
					       LK_RES * res_ptr,
					       LK_ENTRY * from_whom);
static int lock_internal_perform_lock_object (THREAD_ENTRY * thread_p,
					      int tran_index,
					      const DB_VALUE * val,
					      LOCK lock, int wait_msecs,
					      LK_ENTRY ** entry_addr_ptr);
static void lock_internal_perform_unlock_object (THREAD_ENTRY * thread_p,
						 LK_ENTRY * entry_ptr,
						 int release_flag);
static int lock_add_WFG_edge (int from_tran_index, int to_tran_index,
			      int holder_flag, INT64 edge_wait_stime);
static void lock_select_deadlock_victim (THREAD_ENTRY * thread_p,
					 int s, int t);
#if defined (ENABLE_UNUSED_FUNCTION)
static void lock_dump_deadlock_victims (THREAD_ENTRY * thread_p,
					FILE * outfile);
#endif
static int lock_compare_lock_info (const void *lockinfo1,
				   const void *lockinfo2);
static float lock_wait_msecs_to_secs (int msecs);
static void lock_dump_resource (THREAD_ENTRY * thread_p, FILE * outfp,
				LK_RES * res_ptr);
#if defined (ENABLE_UNUSED_FUNCTION)
#if defined(SERVER_MODE)
static void lock_event_log_tran_locks (THREAD_ENTRY * thread_p,
				       FILE * log_fp, int tran_index);
#endif
#endif
static void lock_event_log_blocked_lock (THREAD_ENTRY * thread_p,
					 FILE * log_fp, LK_ENTRY * entry);
static void lock_event_log_blocking_locks (THREAD_ENTRY * thread_p,
					   FILE * log_fp,
					   LK_ENTRY * wait_entry);
static void lock_event_log_lock_info (THREAD_ENTRY * thread_p, FILE * log_fp,
				      LK_ENTRY * entry);
static void lock_event_set_tran_wait_entry (int tran_index, LK_ENTRY * entry);
static void lock_event_set_xasl_id_to_entry (int tran_index,
					     LK_ENTRY * entry);
#endif /* SERVER_MODE */


#if defined(SERVER_MODE)
/* initialize lock entry as free state */
static void
lock_initialize_entry (LK_ENTRY * entry_ptr)
{
  entry_ptr->tran_index = -1;
  entry_ptr->thrd_entry = NULL;
  entry_ptr->res_head = NULL;
  entry_ptr->granted_mode = NULL_LOCK;
  entry_ptr->blocked_mode = NULL_LOCK;
  entry_ptr->next = NULL;
  entry_ptr->tran_next = NULL;
  entry_ptr->tran_prev = NULL;
  entry_ptr->bind_index_in_tran = -1;
  XASL_ID_SET_NULL (&entry_ptr->xasl_id);
}

/* initialize lock entry as granted state */
static void
lock_initialize_entry_as_granted (LK_ENTRY * entry_ptr, int tran_index,
				  struct lk_res *res, LOCK lock)
{
  entry_ptr->tran_index = tran_index;
  entry_ptr->res_head = res;
  entry_ptr->granted_mode = lock;
  entry_ptr->blocked_mode = NULL_LOCK;
  entry_ptr->count = 1;
  entry_ptr->next = NULL;
  entry_ptr->tran_next = NULL;
  entry_ptr->tran_prev = NULL;

  lock_event_set_xasl_id_to_entry (tran_index, entry_ptr);
}

/* initialize lock entry as blocked state */
static void
lock_initialize_entry_as_blocked (LK_ENTRY * entry_ptr,
				  THREAD_ENTRY * thread_p, int tran_index,
				  struct lk_res *res, LOCK lock)
{
  entry_ptr->tran_index = tran_index;
  entry_ptr->thrd_entry = thread_p;
  entry_ptr->res_head = res;
  entry_ptr->granted_mode = NULL_LOCK;
  entry_ptr->blocked_mode = lock;
  entry_ptr->count = 1;
  entry_ptr->next = NULL;
  entry_ptr->tran_next = NULL;
  entry_ptr->tran_prev = NULL;

  lock_event_set_xasl_id_to_entry (tran_index, entry_ptr);
}

/* initialize lock resource as free state */
static void
lock_initialize_resource (struct lk_res *res_ptr)
{
  pthread_mutex_init (&(res_ptr->res_mutex), NULL);
  DB_MAKE_NULL (&(res_ptr->val));
  res_ptr->total_holders_mode = NULL_LOCK;
  res_ptr->total_waiters_mode = NULL_LOCK;
  res_ptr->holder = NULL;
  res_ptr->waiter = NULL;
  res_ptr->hash_next = NULL;
}

/* initialize lock resource as allocated state */
static void
lock_initialize_resource_as_allocated (UNUSED_ARG THREAD_ENTRY * thread_p,
				       struct lk_res *res_ptr,
				       const DB_VALUE * val, LOCK lock)
{
  (void) db_value_clone (val, &(res_ptr->val));
  res_ptr->total_holders_mode = lock;
  res_ptr->total_waiters_mode = NULL_LOCK;
  res_ptr->holder = NULL;
  res_ptr->waiter = NULL;
}
#endif /* SERVER_MODE */

/*
 *  Private Functions Group 1: initalize and finalize major structures
 *
 *   - lock_init_tran_lock_table()
 *   - lock_init_object_hash_table()
 *   - lock_init_object_lock_res_list()
 *   - lock_init_object_lock_entry_list()
 *   - lock_init_deadlock_detection()
 */

#if defined(SERVER_MODE)
/*
 * lock_initialize_tran_lock_table - Initialize the transaction lock hold table.
 *
 * return: error code
 *
 * Note:This function allocates the transaction lock hold table and
 *     initializes the table.
 */
static int
lock_initialize_tran_lock_table (void)
{
  LK_TRAN_LOCK *tran_lock;	/* pointer to transaction hold entry */
  int i;			/* loop variable                     */

  /* initialize the number of transactions */
  lk_Gl.num_trans = MAX_NTRANS;

  /* allocate memory space for transaction lock table */
  lk_Gl.tran_lock_table =
    (LK_TRAN_LOCK *) malloc (SIZEOF_LK_TRAN_LOCK * lk_Gl.num_trans);
  if (lk_Gl.tran_lock_table == (LK_TRAN_LOCK *) NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, (SIZEOF_LK_TRAN_LOCK * lk_Gl.num_trans));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  /* initialize all the entries of transaction lock table */
  memset (lk_Gl.tran_lock_table, 0, SIZEOF_LK_TRAN_LOCK * lk_Gl.num_trans);
  for (i = 0; i < lk_Gl.num_trans; i++)
    {
      tran_lock = &lk_Gl.tran_lock_table[i];
      pthread_mutex_init (&tran_lock->hold_mutex, NULL);
    }

  return NO_ERROR;
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_initialize_object_hash_table - Initializes the object lock hash table
 *
 * return: error code
 *
 * Note:This function initializes an object lock hash table.
 */
static int
lock_initialize_object_hash_table (void)
{
#define LK_INITIAL_OBJECT_LOCK_TABLE_SIZE       10000
  LK_HASH *hash_anchor;		/* pointer to object lock hash entry */
  int i;

  lk_Gl.max_obj_locks = LK_INITIAL_OBJECT_LOCK_TABLE_SIZE;

  /* allocate an object lock hash table */
  if (lk_Gl.max_obj_locks > LK_MIN_OBJECT_LOCKS)
    {
      lk_Gl.obj_hash_size = lk_Gl.max_obj_locks * LK_HASH_RATIO;
    }
  else
    {
      lk_Gl.obj_hash_size = LK_MIN_OBJECT_LOCKS * LK_HASH_RATIO;
    }

  lk_Gl.obj_hash_table =
    (LK_HASH *) malloc (SIZEOF_LK_HASH * lk_Gl.obj_hash_size);
  if (lk_Gl.obj_hash_table == (LK_HASH *) NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, (SIZEOF_LK_HASH * lk_Gl.obj_hash_size));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  /* initialize all buckets of the object lock hash table */
  for (i = 0; i < lk_Gl.obj_hash_size; i++)
    {
      hash_anchor = &lk_Gl.obj_hash_table[i];
      pthread_mutex_init (&hash_anchor->hash_mutex, NULL);
      hash_anchor->hash_next = (LK_RES *) NULL;
    }

  return NO_ERROR;
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_initialize_object_lock_res_list - Initializes the object lock resource list
 *
 * return: error code
 *
 * Note:
 *     This function initializes following two lists.
 *     1. a list of object lock resource block
 *        => each block has object lock resource block.
 *     2. a list of freed object lock resource entries.
 */
static int
lock_initialize_object_lock_res_list (void)
{
  LK_RES_BLOCK *res_block;	/* pointer to lock resource block */
  LK_RES *res_ptr = NULL;	/* pointer to lock entry          */
  int i;

  /* allocate an object lock resource block */
  res_block = (LK_RES_BLOCK *) malloc (SIZEOF_LK_RES_BLOCK);
  if (res_block == (LK_RES_BLOCK *) NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, SIZEOF_LK_RES_BLOCK);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  /* initialize the object lock resource block */
  res_block->next_block = (LK_RES_BLOCK *) NULL;
  res_block->count = MAX ((lk_Gl.max_obj_locks * LK_RES_RATIO), 1);
  res_block->block = (LK_RES *) malloc (SIZEOF_LK_RES * res_block->count);
  if (res_block->block == (LK_RES *) NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, (SIZEOF_LK_RES * res_block->count));
      free_and_init (res_block);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  /* initialize the object lock resource in the block */
  for (i = 0; i < res_block->count; i++)
    {
      res_ptr = &res_block->block[i];
      lock_initialize_resource (res_ptr);
      res_ptr->hash_next = &res_block->block[i + 1];
    }
  res_ptr->hash_next = (LK_RES *) NULL;

  /* initialize the object lock resource node list */
  pthread_mutex_init (&lk_Gl.obj_res_block_list_mutex, NULL);
  lk_Gl.obj_res_block_list = res_block;

  /* initialize the object lock resource free list */
  pthread_mutex_init (&lk_Gl.obj_free_res_list_mutex, NULL);
  lk_Gl.obj_free_res_list = &res_block->block[0];

  lk_Gl.num_obj_res_allocated = 0;

  return NO_ERROR;
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lockk_initialize_object_lock_entry_list - Initializes the object lock entry list
 *
 * return: error code
 *
 * Note:
 *     This function initializes following two lists.
 *     1. a list of object lock entry block
 *        => each node has object lock entry block.
 *     2. a list of freed object lock entries.
 */
static int
lock_initialize_object_lock_entry_list (void)
{
  LK_ENTRY_BLOCK *entry_block;	/* pointer to lock entry block */
  LK_ENTRY *entry_ptr = NULL;	/* pointer to lock entry  */
  int i;

  /* allocate an object lock entry block */
  entry_block = (LK_ENTRY_BLOCK *) malloc (SIZEOF_LK_ENTRY_BLOCK);
  if (entry_block == (LK_ENTRY_BLOCK *) NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, SIZEOF_LK_ENTRY_BLOCK);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  /* initialize the object lock entry block */
  entry_block->next_block = (LK_ENTRY_BLOCK *) NULL;
  entry_block->count = MAX ((lk_Gl.max_obj_locks * LK_ENTRY_RATIO), 1);
  entry_block->block =
    (LK_ENTRY *) malloc (SIZEOF_LK_ENTRY * entry_block->count);
  if (entry_block->block == (LK_ENTRY *) NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, (SIZEOF_LK_ENTRY * entry_block->count));
      free_and_init (entry_block);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  /* initialize the object lock entries in the block */
  for (i = 0; i < entry_block->count; i++)
    {
      entry_ptr = (LK_ENTRY *)
	(((char *) entry_block->block) + SIZEOF_LK_ENTRY * i);
      lock_initialize_entry (entry_ptr);
      entry_ptr->next = (LK_ENTRY *) (((char *) entry_ptr) + SIZEOF_LK_ENTRY);
    }
  entry_ptr->next = (LK_ENTRY *) NULL;


  /* initialize the object lock entry block list */
  pthread_mutex_init (&lk_Gl.obj_entry_block_list_mutex, NULL);
  lk_Gl.obj_entry_block_list = entry_block;

  /* initialize the object lock entry free list */
  pthread_mutex_init (&lk_Gl.obj_free_entry_list_mutex, NULL);
  lk_Gl.obj_free_entry_list = &entry_block->block[0];

  return NO_ERROR;
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_initialize_deadlock_detection - Initializes transaction wait-for graph.
 *
 * return: error code
 *
 * Note:This function initializes the transaction waif-for graph.
 */
static int
lock_initialize_deadlock_detection (void)
{
  int i;

  pthread_mutex_init (&lk_Gl.DL_detection_mutex, NULL);
  gettimeofday (&lk_Gl.last_deadlock_run, NULL);

  /* allocate transaction WFG node table */
  lk_Gl.TWFG_node =
    (LK_WFG_NODE *) malloc (SIZEOF_LK_WFG_NODE * lk_Gl.num_trans);
  if (lk_Gl.TWFG_node == (LK_WFG_NODE *) NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, (SIZEOF_LK_WFG_NODE * lk_Gl.num_trans));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }
  /* initialize transaction WFG node table */
  for (i = 0; i < lk_Gl.num_trans; i++)
    {
      lk_Gl.TWFG_node[i].DL_victim = false;
      lk_Gl.TWFG_node[i].checked_by_deadlock_detector = false;
      lk_Gl.TWFG_node[i].thrd_wait_stime = 0;
    }

  /* initialize other related fields */
  lk_Gl.TWFG_edge = (LK_WFG_EDGE *) NULL;
  lk_Gl.max_TWFG_edge = 0;
  lk_Gl.TWFG_free_edge_idx = -1;
  lk_Gl.global_edge_seq_num = 0;

  return NO_ERROR;
}
#endif /* SERVER_MODE */

/*
 *  Private Functions Group: lock resource and entry management
 *   - lk_alloc_resource()
 *   - lk_free_resource()
 *   - lk_alloc_entry()
 *   - lk_free_entry()
 *   - lk_dealloc_resource()
 */

#if defined(SERVER_MODE)
/*
 * lock_alloc_resource - Allocate a lock resource entry
 *
 * return:  an allocated lock resource entry or NULL
 *
 * Note:This function allocates a lock resource entry and returns it.
 *     At first, it allocates the lock resource entry
 *     from the free list of lock resource entries.
 *     If the free list is empty, this function allocates a new set of
 *     lock resource entries, connects them into the free list and then
 *     allocates one entry from the free list.
 */
static LK_RES *
lock_alloc_resource (void)
{
  int count_try_alloc_entry;
  int count_try_alloc_table;
  LK_RES *res_ptr;
  int rv;

  /* The caller is holding a hash mutex. The reason for holding
   * the hash mutex is to prevent other transactions from
   * allocating lock resource entry on the same lock resource.
   */
  /* 1. allocate a lock resource entry from the free list if possible */
  count_try_alloc_entry = 0;

try_alloc_entry_again:

  rv = pthread_mutex_lock (&lk_Gl.obj_free_res_list_mutex);

  if (lk_Gl.obj_free_res_list != (LK_RES *) NULL)
    {
      res_ptr = lk_Gl.obj_free_res_list;
      lk_Gl.obj_free_res_list = res_ptr->hash_next;
      lk_Gl.num_obj_res_allocated++;

      pthread_mutex_unlock (&lk_Gl.obj_free_res_list_mutex);
#if defined(LK_DUMP)
      if (lk_Gl.dump_level >= 2)
	{
	  fprintf (stderr, "LK_DUMP::lk_alloc_resource() = %p\n", res_ptr);
	}
#endif /* LK_DUMP */
      return res_ptr;
    }

#if 1				/* TODO -trace */
  assert (false);
#endif

  if (count_try_alloc_entry < LK_SLEEP_MAX_COUNT)
    {
      pthread_mutex_unlock (&lk_Gl.obj_free_res_list_mutex);

      count_try_alloc_entry++;

      (void) thread_sleep (RESOURCE_ALLOC_WAIT_TIME);
      goto try_alloc_entry_again;
    }

  /* Currently, the current thread is holding
   * both hash_mutex and obj_free_res_list_mutex.
   */
  count_try_alloc_table = 0;

try_alloc_table_again:

  /* 2. if the free list is empty, allocate a lock resource block */
  if (lock_alloc_resource_block () == NO_ERROR)
    {
      /* Now, the lock resource free list is not empty. */
      res_ptr = lk_Gl.obj_free_res_list;
      lk_Gl.obj_free_res_list = res_ptr->hash_next;
      lk_Gl.num_obj_res_allocated++;

      pthread_mutex_unlock (&lk_Gl.obj_free_res_list_mutex);
#if defined(LK_DUMP)
      if (lk_Gl.dump_level >= 2)
	{
	  fprintf (stderr, "LK_DUMP::lk_alloc_resource() = %p\n", res_ptr);
	}
#endif /* LK_DUMP */
      return res_ptr;
    }

  /*
   * Memory allocation fails.: What should we do ??
   *  - notify DBA or applications of current situation.
   */
  if (count_try_alloc_table < LK_SLEEP_MAX_COUNT)
    {
      /* we should notify DBA or applications of insufficient memory space */
      count_try_alloc_table++;

      (void) thread_sleep (RESOURCE_ALLOC_WAIT_TIME);
      goto try_alloc_table_again;
    }

  pthread_mutex_unlock (&lk_Gl.obj_free_res_list_mutex);

#if defined(LK_DUMP)
  if (lk_Gl.dump_level >= 2)
    {
      fprintf (stderr, "LK_DUMP::lk_alloc_resource() = NULL\n");
    }
#endif /* LK_DUMP */
  return (LK_RES *) NULL;
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_free_resource - Free the given lock resource entry
 *
 * return:
 *
 *   res_ptr(in):
 *
 * Note:
 *     This functions initializes the lock resource entry as freed state
 *     and returns it into free lock resource entry list.
 */
static void
lock_free_resource (UNUSED_ARG THREAD_ENTRY * thread_p, LK_RES * res_ptr)
{
  int rv;

#if defined(LK_DUMP)
  if (lk_Gl.dump_level >= 2)
    {
      fprintf (stderr, "LK_DUMP::lk_free_resource(%p)\n", res_ptr);
    }
#endif /* LK_DUMP */
  /* The caller is not holding any mutex. */

  /* set the lock resource entry as free state */
  (void) db_value_clear (&res_ptr->val);
  DB_MAKE_NULL (&res_ptr->val);

  /* connect it into the free list of lock resource entries */
  rv = pthread_mutex_lock (&lk_Gl.obj_free_res_list_mutex);

  res_ptr->hash_next = lk_Gl.obj_free_res_list;
  lk_Gl.obj_free_res_list = res_ptr;
  lk_Gl.num_obj_res_allocated--;

  pthread_mutex_unlock (&lk_Gl.obj_free_res_list_mutex);

}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_alloc_resource_block - Allocate a lock resource block
 *
 * return: error code
 *     the reason of failure: memory allocation failure
 *
 * Note:This function allocates a lock resource block which has
 *     a set of lock resource entries, connects the block into the resource
 *     block list, initializes the lock resource entries, and then
 *     connects them into the free list.
 */
static int
lock_alloc_resource_block (void)
{
  LK_RES_BLOCK *res_block;
  int i;
  LK_RES *res_ptr = NULL;
  int rv;

  /* The caller is holding a hash mutex and resource free list mutex */

  /* allocate an object lock resource block */
  res_block = (LK_RES_BLOCK *) malloc (SIZEOF_LK_RES_BLOCK);
  if (res_block == (LK_RES_BLOCK *) NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      (SIZEOF_LK_RES_BLOCK));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  res_block->count = MAX (LK_MORE_RES_COUNT, 1);
  res_block->block = (LK_RES *) malloc (SIZEOF_LK_RES * res_block->count);
  if (res_block->block == (LK_RES *) NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      (SIZEOF_LK_RES * res_block->count));
      free_and_init (res_block);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  /* initialize the object lock resource in the block */
  for (i = 0; i < res_block->count; i++)
    {
      res_ptr = &res_block->block[i];
      lock_initialize_resource (res_ptr);
      res_ptr->hash_next = &res_block->block[i + 1];
    }
  res_ptr->hash_next = (LK_RES *) NULL;

  /* connect the allocated node into the node list */
  rv = pthread_mutex_lock (&lk_Gl.obj_res_block_list_mutex);

  res_block->next_block = lk_Gl.obj_res_block_list;
  lk_Gl.obj_res_block_list = res_block;

  pthread_mutex_unlock (&lk_Gl.obj_res_block_list_mutex);

  /* connet the allocated entries into the free list */
  res_ptr->hash_next = lk_Gl.obj_free_res_list;
  lk_Gl.obj_free_res_list = &res_block->block[0];

  return NO_ERROR;
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_alloc_entry - Allocate a lock entry
 *
 * return: LK_ENTRY *
 *     allocated lock entry or NULL
 *
 * Note:This function allocates a lock entry and returns it.
 *     At first, it allocates the lock entry
 *     from the free list of lock entries.
 *     If the free list is empty, this function allocates
 *     a set of lock entries, connects them into the free list
 *     and then allocates one entry from the free list.
 */
static LK_ENTRY *
lock_alloc_entry (void)
{
  int count_try_alloc_entry;
  int count_try_alloc_table;
  LK_ENTRY *entry_ptr;
  int rv;

  /* The caller is holding a resource mutex */

  /* 1. allocate an lock entry from the free list */
  count_try_alloc_entry = 0;

  rv = pthread_mutex_lock (&lk_Gl.obj_free_entry_list_mutex);
  while (lk_Gl.obj_free_entry_list == (LK_ENTRY *) NULL
	 && count_try_alloc_entry < LK_SLEEP_MAX_COUNT)
    {
      pthread_mutex_unlock (&lk_Gl.obj_free_entry_list_mutex);
      count_try_alloc_entry++;

      (void) thread_sleep (RESOURCE_ALLOC_WAIT_TIME);
      rv = pthread_mutex_lock (&lk_Gl.obj_free_entry_list_mutex);
    }

  if (lk_Gl.obj_free_entry_list != (LK_ENTRY *) NULL)
    {
      entry_ptr = lk_Gl.obj_free_entry_list;
      lk_Gl.obj_free_entry_list = entry_ptr->next;
      pthread_mutex_unlock (&lk_Gl.obj_free_entry_list_mutex);
#if defined(LK_DUMP)
      if (lk_Gl.dump_level >= 2)
	{
	  fprintf (stderr, "LK_DUMP::lk_alloc_entry() = %p\n", entry_ptr);
	}
#endif /* LK_DUMP */
      return entry_ptr;
    }

#if 1				/* TODO - trace */
  assert (false);
#endif

  /* Currently, the current thread is holding
   * both res_mutex and obj_free_entry_list_mutex.
   */
  count_try_alloc_table = 0;

  /* 2. if the free list is empty, allocate a new lock entry block */
  while (lock_alloc_entry_block () != NO_ERROR
	 && count_try_alloc_table < LK_SLEEP_MAX_COUNT)
    {
      /*
       * Memory allocation fails.: What should we do ??
       *  - notify DBA or applications of current situation.
       */

      /* should notify DBA or applications of insufficient memory space */
      count_try_alloc_table++;

      (void) thread_sleep (RESOURCE_ALLOC_WAIT_TIME);	/* sleep: 0.01 second */
    }

  if (count_try_alloc_table < LK_SLEEP_MAX_COUNT)
    {
      /* Now, the free list is not empty. */
      entry_ptr = lk_Gl.obj_free_entry_list;
      lk_Gl.obj_free_entry_list = entry_ptr->next;
      pthread_mutex_unlock (&lk_Gl.obj_free_entry_list_mutex);
#if defined(LK_DUMP)
      if (lk_Gl.dump_level >= 2)
	{
	  fprintf (stderr, "LK_DUMP::lk_alloc_entry() = %p\n", entry_ptr);
	}
#endif /* LK_DUMP */
      return entry_ptr;
    }

  pthread_mutex_unlock (&lk_Gl.obj_free_entry_list_mutex);

#if defined(LK_DUMP)
  if (lk_Gl.dump_level >= 2)
    {
      fprintf (stderr, "LK_DUMP::lk_alloc_entry() = NULL\n");
    }
#endif /* LK_DUMP */
  return (LK_ENTRY *) NULL;
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_free_entry - Free the given lock entry
 *
 * return:
 *
 *   entry_ptr(in):
 *
 * Note:This functions initializes the lock entry as freed state
 *     and returns it into free lock entry list.
 */
static void
lock_free_entry (LK_ENTRY * entry_ptr)
{
  int rv;

#if defined(LK_DUMP)
  if (lk_Gl.dump_level >= 2)
    {
      fprintf (stderr, "LK_DUMP::lk_free_entry(%p)\n", entry_ptr);
    }
#endif /* LK_DUMP */
  /* The caller is holding a resource mutex */

  /* set the lock entry as free state */
  entry_ptr->tran_index = -1;

  /* connect it into free entry list */
  rv = pthread_mutex_lock (&lk_Gl.obj_free_entry_list_mutex);
  entry_ptr->next = lk_Gl.obj_free_entry_list;
  lk_Gl.obj_free_entry_list = entry_ptr;
  pthread_mutex_unlock (&lk_Gl.obj_free_entry_list_mutex);
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_alloc_entry_block - Allocate a lock entry block
 *
 * return: error code
 *
 * Note:This function allocates a lock entry block which has a set of lock
 *     entries, connects the block into the block list, initializes the lock
 *     entries, and then connects them into the free list.
 */
static int
lock_alloc_entry_block (void)
{
  LK_ENTRY_BLOCK *entry_block;
  LK_ENTRY *entry_ptr = NULL;
  int i, rv;

  /* The caller is holding a resource mutex */

  /* allocate an object lock entry block */
  entry_block = (LK_ENTRY_BLOCK *) malloc (SIZEOF_LK_ENTRY_BLOCK);
  if (entry_block == (LK_ENTRY_BLOCK *) NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      (SIZEOF_LK_ENTRY_BLOCK));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  entry_block->count = MAX (LK_MORE_ENTRY_COUNT, 1);
  entry_block->block =
    (LK_ENTRY *) malloc (SIZEOF_LK_ENTRY * entry_block->count);
  if (entry_block->block == (LK_ENTRY *) NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      (SIZEOF_LK_ENTRY * entry_block->count));
      free_and_init (entry_block);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  /* initialize the object lock entry block */
  for (i = 0; i < entry_block->count; i++)
    {
      entry_ptr = (LK_ENTRY *)
	(((char *) entry_block->block) + SIZEOF_LK_ENTRY * i);
      lock_initialize_entry (entry_ptr);
      entry_ptr->next = (LK_ENTRY *) (((char *) entry_ptr) + SIZEOF_LK_ENTRY);
    }
  entry_ptr->next = (LK_ENTRY *) NULL;

  /* connect the allocated node into the entry block */
  rv = pthread_mutex_lock (&lk_Gl.obj_entry_block_list_mutex);
  entry_block->next_block = lk_Gl.obj_entry_block_list;
  lk_Gl.obj_entry_block_list = entry_block;
  pthread_mutex_unlock (&lk_Gl.obj_entry_block_list_mutex);

  /* connect the allocated entries into the free list */
  entry_ptr->next = lk_Gl.obj_free_entry_list;
  lk_Gl.obj_free_entry_list = &entry_block->block[0];

  return NO_ERROR;
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_dealloc_resource - Deallocate lock resource entry
 *
 * return: error code
 *
 *   res_ptr(in):
 *
 * Note:This function deallocates the given lock resource entry
 *     from lock hash table.
 */
static int
lock_dealloc_resource (THREAD_ENTRY * thread_p, LK_RES * res_ptr)
{
  bool res_mutex_hold;
  int ret_val;
  unsigned int hash_index;
  LK_HASH *hash_anchor;
  LK_RES *prev, *curr;
  int rv;

  /* The caller is holding a resource mutex, currently. */
  res_mutex_hold = true;

  /* no holders and no waiters */
  /* remove the lock resource entry from the lock hash chain */
  hash_index = mht_get_hash_number (lk_Gl.obj_hash_size, &res_ptr->val);
  hash_anchor = &lk_Gl.obj_hash_table[hash_index];

  /* conditional mutex hold request */
  ret_val = pthread_mutex_trylock (&hash_anchor->hash_mutex);
  if (ret_val != 0)
    {				/* I could not hold the hash_mutex */
      if (ret_val != EBUSY)
	{
	  pthread_mutex_unlock (&res_ptr->res_mutex);
	  er_set_with_oserror (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_CSS_PTHREAD_MUTEX_TRYLOCK, 0);
	  return ER_CSS_PTHREAD_MUTEX_TRYLOCK;
	}
      /* Someone is holding the hash_mutex */
      pthread_mutex_unlock (&res_ptr->res_mutex);
      rv = pthread_mutex_lock (&hash_anchor->hash_mutex);

      res_mutex_hold = false;
    }

  /* Now holding hash_mutex. find the resource entry */
  prev = (LK_RES *) NULL;
  curr = hash_anchor->hash_next;
  while (curr != (LK_RES *) NULL)
    {
      if (curr == res_ptr)
	{
	  break;
	}
      prev = curr;
      curr = curr->hash_next;
    }

  if (curr == (LK_RES *) NULL)
    {
      /*
       * Case 1: The lock resource entry does not exist in the hash chain.
       *         This case could be occur when some other transactions got
       *         the lock resource entry and deallocated it in the meanwhile
       *         of releasing the res_mutex and holding the hash_mutex.
       *         The deallocated lock resource entry can be either
       *         1) in the free list of lock resource entries, or
       *         2) in some other lock hash chain.
       */
      /* release all the mutexes */
      pthread_mutex_unlock (&hash_anchor->hash_mutex);
      if (res_mutex_hold == true)	/* This might be always false */
	{
	  pthread_mutex_unlock (&res_ptr->res_mutex);
	}
      return NO_ERROR;
    }

  /*
   * Case 2: The lock resource entry does exist in the hash chain.
   *         The lock resource entry may contain either
   *         1) the OID that the transaction wants to unlock, or
   *         2) some other OID.
   */
  if (res_mutex_hold == false)
    {
      /* hold the res_mutex conditionally */
      ret_val = pthread_mutex_trylock (&res_ptr->res_mutex);
      if (ret_val != 0)
	{			/* could not hold the res_mutex */
	  if (ret_val != EBUSY)
	    {
	      pthread_mutex_unlock (&hash_anchor->hash_mutex);
	      er_set_with_oserror (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
				   ER_CSS_PTHREAD_MUTEX_TRYLOCK, 0);
	      return ER_CSS_PTHREAD_MUTEX_TRYLOCK;
	    }
	  /* Someone wants to use this: OK, I will quit! */
	  pthread_mutex_unlock (&hash_anchor->hash_mutex);
	  return NO_ERROR;
	}
      /* check if the resource entry is empty again. */
      if (res_ptr->holder != NULL || res_ptr->waiter != NULL)
	{
	  /* Someone already got this: OK, I will quit! */
	  pthread_mutex_unlock (&res_ptr->res_mutex);
	  pthread_mutex_unlock (&hash_anchor->hash_mutex);
	  return NO_ERROR;
	}
    }

  /* I hold hash_mutex and res_mutex
   * remove the resource entry from the hash chain
   */
  if (prev == (LK_RES *) NULL)
    {
      hash_anchor->hash_next = res_ptr->hash_next;
    }
  else
    {
      prev->hash_next = res_ptr->hash_next;
    }

  /* release hash_mutex and res_mutex */
  pthread_mutex_unlock (&hash_anchor->hash_mutex);
  pthread_mutex_unlock (&res_ptr->res_mutex);

  /* initialize the lock descriptor entry as a freed entry
   * This is performed within lk_free_resource()
   * free the lock resource entry
   */
  lock_free_resource (thread_p, res_ptr);

  return NO_ERROR;
}
#endif /* SERVER_MODE */

/*
 *  Private Functions Group: transaction lock list related functios
 *   - lk_insert_into_tran_hold_list()
 *   - lk_delete_from_tran_hold_list()
 */

#if defined(SERVER_MODE)
/*
 * lock_insert_into_tran_hold_list - Insert the given lock entry
 *                      into the transaction lock hold list
 *
 * return: nothing
 *
 *   entry_ptr(in):
 *
 * Note:This function inserts the given lock entry into the transaction lock
 *     hold list. The given lock entry was included in the lock holder
 *     list. That is, The lock is held by the transaction.
 */
static void
lock_insert_into_tran_hold_list (LK_ENTRY * entry_ptr)
{
  LK_TRAN_LOCK *tran_lock;
  int rv;

  /* The caller is holding a resource mutex */

  tran_lock = &lk_Gl.tran_lock_table[entry_ptr->tran_index];
  rv = pthread_mutex_lock (&tran_lock->hold_mutex);

  if (tran_lock->inst_hold_list != NULL)
    {
      tran_lock->inst_hold_list->tran_prev = entry_ptr;
    }

  entry_ptr->tran_next = tran_lock->inst_hold_list;
  tran_lock->inst_hold_list = entry_ptr;
  tran_lock->inst_hold_count++;

  pthread_mutex_unlock (&tran_lock->hold_mutex);
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_delete_from_tran_hold_list - Delted the given lock entry
 *                      from the transaction lock hold list
 *
 * return: error code
 *
 *   entry_ptr(in):
 *
 * Note:This functions finds the given lock entry in the transaction
 *     lock hold list and then deletes it from the lock hold list.
 */
static int
lock_delete_from_tran_hold_list (LK_ENTRY * entry_ptr)
{
  LK_TRAN_LOCK *tran_lock;
  int rv;
  int error_code = NO_ERROR;

  /* The caller is holding a resource mutex */

  tran_lock = &lk_Gl.tran_lock_table[entry_ptr->tran_index];
  rv = pthread_mutex_lock (&tran_lock->hold_mutex);

  if (tran_lock->inst_hold_list == entry_ptr)
    {
      tran_lock->inst_hold_list = entry_ptr->tran_next;
      if (entry_ptr->tran_next)
	{
	  entry_ptr->tran_next->tran_prev = NULL;
	}
    }
  else
    {
      if (entry_ptr->tran_prev)
	{
	  entry_ptr->tran_prev->tran_next = entry_ptr->tran_next;
	}
      if (entry_ptr->tran_next)
	{
	  entry_ptr->tran_next->tran_prev = entry_ptr->tran_prev;
	}
    }
  tran_lock->inst_hold_count--;

  pthread_mutex_unlock (&tran_lock->hold_mutex);

  return error_code;
}
#endif /* SERVER_MODE */

/*
 *  Private Functions Group: lock entry addition related functions
 *   - lk_position_holder_entry()
 */

#if defined(SERVER_MODE)
/*
 * lock_position_holder_entry - Position given lock entry in the lock
 *                                holder list of given lock resource
 *
 * return:
 *
 *   res_ptr(in):
 *   entry_ptr(in):
 *
 * Note:This function positions the given lock entry
 *     in the lock holder list of the given lock resource
 *     according to Upgrader Positioning Rule(UPR).
 *
 *     NOTE that the granted_mode and blocked_mode of the given lock
 *          entry must be set before this function is called.
 */
static void
lock_position_holder_entry (LK_RES * res_ptr, LK_ENTRY * entry_ptr)
{
  LK_ENTRY *prev, *i;
  LK_ENTRY *ta, *tap;
  LK_ENTRY *tb, *tbp;
  LK_ENTRY *tc, *tcp;
  int compat1, compat2;

  /* find the position where the lock entry to be inserted */
  if (entry_ptr->blocked_mode == NULL_LOCK)
    {
      /* case 1: when block_mode is NULL_LOCK */
      prev = (LK_ENTRY *) NULL;
      i = res_ptr->holder;
      while (i != (LK_ENTRY *) NULL)
	{
	  if (i->blocked_mode == NULL_LOCK)
	    {
	      break;
	    }
	  prev = i;
	  i = i->next;
	}
    }
  else
    {
      /* case 2: when block_mode is not NULL_LOCK */
      /* find ta, tb, tc among other holders */
      ta = tb = tc = (LK_ENTRY *) NULL;
      tap = tbp = tcp = (LK_ENTRY *) NULL;

      prev = (LK_ENTRY *) NULL;
      i = res_ptr->holder;
      while (i != (LK_ENTRY *) NULL)
	{
	  if (i->blocked_mode != NULL_LOCK)
	    {
	      assert (entry_ptr->blocked_mode >= NULL_LOCK
		      && entry_ptr->granted_mode >= NULL_LOCK);
	      assert (i->blocked_mode >= NULL_LOCK
		      && i->granted_mode >= NULL_LOCK);

	      compat1 = lock_Comp[entry_ptr->blocked_mode][i->blocked_mode];
	      assert (compat1 != DB_NA);

	      if (ta == NULL && compat1 == true)
		{
		  ta = i;
		  tap = prev;
		}

	      compat1 = lock_Comp[entry_ptr->blocked_mode][i->granted_mode];
	      assert (compat1 != DB_NA);

	      compat2 = lock_Comp[i->blocked_mode][entry_ptr->granted_mode];
	      assert (compat2 != DB_NA);

	      if (ta == NULL && tb == NULL
		  && compat1 == true && compat2 == false)
		{
		  tb = i;
		  tbp = prev;
		}
	    }
	  else
	    {
	      if (tc == NULL)
		{
		  tc = i;
		  tcp = prev;
		}
	    }
	  prev = i;
	  i = i->next;
	}
      if (ta != NULL)
	{
	  prev = tap;
	}
      else if (tb != NULL)
	{
	  prev = tbp;
	}
      else if (tc != NULL)
	{
	  prev = tcp;
	}
    }

  /* insert the given lock entry into the found position */
  if (prev == NULL)
    {
      entry_ptr->next = res_ptr->holder;
      res_ptr->holder = entry_ptr;
    }
  else
    {
      entry_ptr->next = prev->next;
      prev->next = entry_ptr;
    }
}
#endif /* SERVER_MODE */


/*
 *  Private Functions Group: timeout related functions
 *
 *   - lock_set_error_for_timeout()
 *   - lock_set_error_for_aborted()
 *   - lock_suspend(), lock_resume()
 *   - lock_wakeup_deadlock_victim_timeout()
 *   - lock_wakeup_deadlock_victim_aborted()
 */

#if defined(SERVER_MODE)
/*
 * lock_set_error_for_timeout - Set error for lock timeout
 *
 * return:
 *
 *   entry_ptr(in): pointer to the lock entry for waiting
 *
 * Note:Set error code for lock timeout
 */
static void
lock_set_error_for_timeout (THREAD_ENTRY * thread_p, LK_ENTRY * entry_ptr)
{
  char *client_prog_name;	/* Client program name for transaction  */
  char *client_user_name;	/* Client user name for transaction     */
  char *client_host_name;	/* Client host for transaction          */
  int client_pid;		/* Client process id for transaction    */
  char *waitfor_client_users_default = (char *) "";
  char *waitfor_client_users;	/* Waitfor users                        */
  char *classname;		/* Name of the class                    */
  int n, i, nwaits, max_waits = DEFAULT_WAIT_USERS;
  int wait_for_buf[DEFAULT_WAIT_USERS];
  int *wait_for = wait_for_buf, *t;
  LK_ENTRY *entry;
  LK_RES *res_ptr = NULL;
  int unit_size = LOG_USERNAME_MAX + MAXHOSTNAMELEN + PATH_MAX + 20 + 4;
  char *ptr;
  int rv;
  bool free_mutex_flag = false;
  bool isdeadlock_timeout = false;
  int compat1, compat2;
  OID *class_oid;
  char tmpbuf[32];

  /* Find the users that transaction is waiting for */
  waitfor_client_users = waitfor_client_users_default;
  nwaits = 0;

  assert (entry_ptr->granted_mode >= NULL_LOCK
	  && entry_ptr->blocked_mode >= NULL_LOCK);

  /* Dump all the tran. info. which this tran. is waiting for */
  res_ptr = entry_ptr->res_head;
  wait_for[0] = NULL_TRAN_INDEX;

  rv = pthread_mutex_lock (&res_ptr->res_mutex);
  free_mutex_flag = true;
  for (entry = res_ptr->holder; entry != NULL; entry = entry->next)
    {
      if (entry == entry_ptr)
	{
	  continue;
	}

      assert (entry->granted_mode >= NULL_LOCK
	      && entry->blocked_mode >= NULL_LOCK);
      compat1 = lock_Comp[entry->granted_mode][entry_ptr->blocked_mode];
      compat2 = lock_Comp[entry->blocked_mode][entry_ptr->blocked_mode];
      assert (compat1 != DB_NA && compat2 != DB_NA);

      if (compat1 == false || compat2 == false)
	{
	  EXPAND_WAIT_FOR_ARRAY_IF_NEEDED ();
	  wait_for[nwaits++] = entry->tran_index;
	}
    }

  for (entry = res_ptr->waiter; entry != NULL; entry = entry->next)
    {
      if (entry == entry_ptr)
	{
	  continue;
	}

      assert (entry->granted_mode >= NULL_LOCK
	      && entry->blocked_mode >= NULL_LOCK);
      compat1 = lock_Comp[entry->blocked_mode][entry_ptr->blocked_mode];
      assert (compat1 != DB_NA);

      if (compat1 == false)
	{
	  EXPAND_WAIT_FOR_ARRAY_IF_NEEDED ();
	  wait_for[nwaits++] = entry->tran_index;
	}
    }

  pthread_mutex_unlock (&res_ptr->res_mutex);
  free_mutex_flag = false;

  if (nwaits == 0
      || (waitfor_client_users =
	  (char *) malloc (unit_size * nwaits)) == NULL)
    {
      waitfor_client_users = waitfor_client_users_default;
    }
  else
    {
      for (ptr = waitfor_client_users, i = 0; i < nwaits; i++)
	{
	  (void) logtb_find_client_name_host_pid (wait_for[i],
						  &client_prog_name,
						  &client_user_name,
						  &client_host_name,
						  &client_pid);
	  n =
	    sprintf (ptr, "%s%s@%s|%s(%d)", ((i == 0) ? "" : ", "),
		     client_user_name, client_host_name, client_prog_name,
		     client_pid);
	  ptr += n;
	}
    }

set_error:

  if (wait_for != wait_for_buf)
    {
      free_and_init (wait_for);
    }

  if (free_mutex_flag)
    {
      pthread_mutex_unlock (&res_ptr->res_mutex);
      free_mutex_flag = false;
    }

  /* get the client information of current transaction */
  (void) logtb_find_client_name_host_pid (entry_ptr->tran_index,
					  &client_prog_name,
					  &client_user_name,
					  &client_host_name, &client_pid);

  if (entry_ptr->thrd_entry != NULL
      && ((entry_ptr->thrd_entry->lockwait_state ==
	   LOCK_RESUMED_DEADLOCK_TIMEOUT)
#if defined (ENABLE_UNUSED_FUNCTION)
	  || (entry_ptr->thrd_entry->lockwait_state ==
	      LOCK_RESUMED_ABORTED_OTHER)
#endif
      ))
    {
      isdeadlock_timeout = true;
    }

  switch (DB_VALUE_TYPE (&entry_ptr->res_head->val))
    {
    case DB_TYPE_OID:
      class_oid = DB_GET_OID (&entry_ptr->res_head->val);
      classname = heap_get_class_name (thread_p, class_oid);

      if (classname != NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ((isdeadlock_timeout) ? ER_LK_OBJECT_DL_TIMEOUT_CLASS_MSG :
		   ER_LK_OBJECT_TIMEOUT_CLASS_MSG), 7, entry_ptr->tran_index,
		  client_user_name, client_host_name, client_pid,
		  LOCK_TO_LOCKMODE_STRING (entry_ptr->blocked_mode),
		  classname, waitfor_client_users);
	  free_and_init (classname);
	}
      else
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ((isdeadlock_timeout) ? ER_LK_OBJECT_DL_TIMEOUT_SIMPLE_MSG :
		   ER_LK_OBJECT_TIMEOUT_SIMPLE_MSG), 9, entry_ptr->tran_index,
		  client_user_name, client_host_name, client_pid,
		  LOCK_TO_LOCKMODE_STRING (entry_ptr->blocked_mode),
		  class_oid->volid, class_oid->pageid, class_oid->slotid,
		  waitfor_client_users);
	}
      break;

    case DB_TYPE_VARCHAR:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ((isdeadlock_timeout) ? ER_LK_OBJECT_DL_TIMEOUT_VALUE_MSG :
	       ER_LK_OBJECT_TIMEOUT_VALUE_MSG), 7, entry_ptr->tran_index,
	      client_user_name, client_host_name, client_pid,
	      LOCK_TO_LOCKMODE_STRING (entry_ptr->blocked_mode),
	      DB_GET_STRING (&entry_ptr->res_head->val),
	      waitfor_client_users);
      break;

    case DB_TYPE_INTEGER:
      sprintf (tmpbuf, "%d", DB_GET_INT (&entry_ptr->res_head->val));
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ((isdeadlock_timeout) ? ER_LK_OBJECT_DL_TIMEOUT_VALUE_MSG :
	       ER_LK_OBJECT_TIMEOUT_VALUE_MSG), 7, entry_ptr->tran_index,
	      client_user_name, client_host_name, client_pid,
	      LOCK_TO_LOCKMODE_STRING (entry_ptr->blocked_mode),
	      tmpbuf, waitfor_client_users);
      break;
    case DB_TYPE_BIGINT:
      sprintf (tmpbuf, "%ld", DB_GET_BIGINT (&entry_ptr->res_head->val));
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ((isdeadlock_timeout) ? ER_LK_OBJECT_DL_TIMEOUT_VALUE_MSG :
	       ER_LK_OBJECT_TIMEOUT_VALUE_MSG), 7, entry_ptr->tran_index,
	      client_user_name, client_host_name, client_pid,
	      LOCK_TO_LOCKMODE_STRING (entry_ptr->blocked_mode),
	      tmpbuf, waitfor_client_users);
      break;
    default:
      break;
    }

  if (waitfor_client_users
      && waitfor_client_users != waitfor_client_users_default)
    {
      free_and_init (waitfor_client_users);
    }

  if (isdeadlock_timeout == false)
    {
      FILE *log_fp;

      log_fp = event_log_start (thread_p, "LOCK_TIMEOUT");
      if (log_fp == NULL)
	{
	  return;
	}

      lock_event_log_blocked_lock (thread_p, log_fp, entry_ptr);
      lock_event_log_blocking_locks (thread_p, log_fp, entry_ptr);

      event_log_end (thread_p);
    }
}
#endif /* SERVER_MODE */

#if defined (ENABLE_UNUSED_FUNCTION)
#if defined(SERVER_MODE)
/*
 * lock_set_error_for_aborted - Set error for unilaterally aborted
 *
 * return:
 *
 *   entry_ptr(in): pointer to the entry for waiting
 *
 * Note:set error code for unilaterally aborted deadlock victim
 */
static void
lock_set_error_for_aborted (LK_ENTRY * entry_ptr,
			    TRAN_ABORT_REASON abort_reason)
{
  char *client_prog_name;	/* Client user name for transaction  */
  char *client_user_name;	/* Client user name for transaction  */
  char *client_host_name;	/* Client host for transaction       */
  int client_pid;		/* Client process id for transaction */
  LOG_TDES *tdes;

#if 1				/* TODO - trace */
  assert (false);
#endif

  (void) logtb_find_client_name_host_pid (entry_ptr->tran_index,
					  &client_prog_name,
					  &client_user_name,
					  &client_host_name, &client_pid);
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_UNILATERALLY_ABORTED, 4,
	  entry_ptr->tran_index, client_user_name, client_host_name,
	  client_pid);

  tdes = LOG_FIND_TDES (entry_ptr->tran_index);
  assert (tdes != NULL);
  if (tdes)
    {
      tdes->tran_abort_reason = abort_reason;
    }
}
#endif /* SERVER_MODE */
#endif /* ENABLE_UNUSED_FUNCTION */

#if defined(SERVER_MODE)
/*
 * lock_suspend - Suspend current thread (transaction)
 *
 * return: LOCK_WAIT_STATE (state of resumption)
 *
 *   entry_ptr(in): lock entry for lock waiting
 *   wait_msecs(in): lock wait milliseconds
 */
static LOCK_WAIT_STATE
lock_suspend (THREAD_ENTRY * thread_p, LK_ENTRY * entry_ptr, int wait_msecs)
{
  THREAD_ENTRY *p;
  struct timeval tv;
#if defined (ENABLE_UNUSED_FUNCTION)
  int client_id;
#endif
  LOG_TDES *tdes;

  /* The caller is holding the thread entry mutex */

  if (lk_Gl.verbose_mode == true)
    {
      char *__client_prog_name;	/* Client program name for transaction */
      char *__client_user_name;	/* Client user name for transaction    */
      char *__client_host_name;	/* Client host for transaction         */
      int __client_pid;		/* Client process id for transaction   */

      fflush (stderr);
      fflush (stdout);
      logtb_find_client_name_host_pid (entry_ptr->tran_index,
				       &__client_prog_name,
				       &__client_user_name,
				       &__client_host_name, &__client_pid);
      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_RYE,
				       MSGCAT_SET_LOCK,
				       MSGCAT_LK_SUSPEND_TRAN),
	       entry_ptr->thrd_entry->index, entry_ptr->tran_index,
	       __client_prog_name, __client_user_name, __client_host_name,
	       __client_pid);
      fflush (stdout);
    }

  /* register lock wait info. into the thread entry */
  entry_ptr->thrd_entry->lockwait = (void *) entry_ptr;
  gettimeofday (&tv, NULL);
  entry_ptr->thrd_entry->lockwait_stime =
    (tv.tv_sec * 1000000LL + tv.tv_usec) / 1000LL;
  entry_ptr->thrd_entry->lockwait_msecs = wait_msecs;
  entry_ptr->thrd_entry->lockwait_state = (int) LOCK_SUSPENDED;

  lk_Gl.TWFG_node[entry_ptr->tran_index].thrd_wait_stime =
    entry_ptr->thrd_entry->lockwait_stime;

  /* wakeup the dealock detect thread */
  thread_wakeup_deadlock_detect_thread ();

  tdes = logtb_get_current_tdes (thread_p);

#if !defined (NDEBUG) && defined (SERVER_MODE)
  /* assert - I'm not a deadlock-victim thread */
  assert (tdes->tran_abort_reason == TRAN_NORMAL);
#endif

  if (tdes)
    {
      tdes->waiting_for_res = entry_ptr->res_head;
    }

  lock_event_set_tran_wait_entry (entry_ptr->tran_index, entry_ptr);

  /* suspend the worker thread (transaction) */
  thread_suspend_wakeup_and_unlock_entry (entry_ptr->thrd_entry,
					  THREAD_LOCK_SUSPENDED);

  lk_Gl.TWFG_node[entry_ptr->tran_index].thrd_wait_stime = 0;

  if (tdes)
    {
      tdes->waiting_for_res = NULL;
    }

  lock_event_set_tran_wait_entry (entry_ptr->tran_index, NULL);

  if (entry_ptr->thrd_entry->resume_status == THREAD_RESUME_DUE_TO_INTERRUPT)
    {
      /* a shutdown thread wakes me up */
      return LOCK_RESUMED_INTERRUPT;
    }
  else if (entry_ptr->thrd_entry->resume_status != THREAD_LOCK_RESUMED)
    {
      /* wake up with other reason */
      assert (0);

      return LOCK_RESUMED_INTERRUPT;
    }
  else
    {
      assert (entry_ptr->thrd_entry->resume_status == THREAD_LOCK_RESUMED);
    }

  thread_lock_entry (entry_ptr->thrd_entry);
  while (entry_ptr->thrd_entry->tran_next_wait)
    {
      p = entry_ptr->thrd_entry->tran_next_wait;
      entry_ptr->thrd_entry->tran_next_wait = p->tran_next_wait;
      p->tran_next_wait = NULL;
      thread_wakeup (p, THREAD_LOCK_RESUMED);
    }
  thread_unlock_entry (entry_ptr->thrd_entry);

  /* The thread has been awaken
   * Before waking up the thread, the waker cleared the lockwait field
   * of the thread entry and set lockwait_state field of it to the resumed
   * state while holding the thread entry mutex. After the wakeup,
   * no one can update the lockwait releated fields of the thread entry.
   * Therefore, waken-up thread can read the values of lockwait related
   * fields of its own thread entry without holding thread entry mutex.
   */

  switch ((LOCK_WAIT_STATE) (entry_ptr->thrd_entry->lockwait_state))
    {
    case LOCK_RESUMED:
      /* The lock entry has already been moved to the holder list */
      return LOCK_RESUMED;

#if defined (ENABLE_UNUSED_FUNCTION)
    case LOCK_RESUMED_ABORTED_FIRST:
      /* The lock entry does exist within the blocked holder list
       * or blocked waiter list. Therefore, current thread must disconnect
       * it from the list.
       */
      if (logtb_is_current_active (thread_p) == true)
	{
	  /* set error code */
	  lock_set_error_for_aborted (entry_ptr, TRAN_ABORT_DUE_DEADLOCK);

	  /* wait until other threads finish their works
	   * A css_server_thread is always running for this transaction.
	   * so, wait until thread_has_threads() becomes 1 (except me)
	   */
	  if (thread_has_threads (thread_p, entry_ptr->tran_index,
				  thread_get_client_id (thread_p)) >= 1)
	    {
	      logtb_set_tran_index_interrupt (thread_p,
					      entry_ptr->tran_index, true);
	      while (1)
		{
		  thread_sleep (10);	/* sleep 10 msec */
		  thread_wakeup_with_tran_index (entry_ptr->tran_index,
						 THREAD_RESUME_DUE_TO_INTERRUPT);

		  client_id = thread_get_client_id (thread_p);
		  if (thread_has_threads (thread_p, entry_ptr->tran_index,
					  client_id) == 0)
		    {
		      break;
		    }
		}
	      logtb_set_tran_index_interrupt (thread_p,
					      entry_ptr->tran_index, false);
	    }
	}
      else
	{
	  /* We are already aborting, fall through.
	   * Don't do double aborts that could cause an infinite loop.
	   */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_ABORT_TRAN_TWICE, 1,
		  entry_ptr->tran_index);
	  /* er_log_debug(ARG_FILE_LINE, "lk_suspend: Likely a system error."
	     "Trying to abort a transaction twice.\n"); */

	  /* Since we deadlocked during an abort,
	   * forcibly remove all page latches of this transaction and
	   * hope this transaction is the cause of the logjam.
	   * We are hoping that this frees things
	   * just enough to let other transactions continue.
	   * Note it is not be safe to unlock object locks this way.
	   */
	  pgbuf_unfix_all (thread_p);
	}
      return LOCK_RESUMED_ABORTED;

    case LOCK_RESUMED_ABORTED_OTHER:
      /* The lock entry does exist within the blocked holder list
       * or blocked waiter list. Therefore, current thread must diconnect
       * it from the list.
       */
      /* If two or more threads, which were executing for one transaction,
       * are selected as deadlock victims, one of them is charged of the
       * transaction abortion and the other threads are notified of timeout.
       */
      (void) lock_set_error_for_timeout (thread_p, entry_ptr);
      return LOCK_RESUMED_DEADLOCK_TIMEOUT;
#endif

    case LOCK_RESUMED_DEADLOCK_TIMEOUT:
      (void) lock_set_error_for_timeout (thread_p, entry_ptr);
      return LOCK_RESUMED_DEADLOCK_TIMEOUT;

    case LOCK_RESUMED_TIMEOUT:
      /* The lock entry does exist within the blocked holder list
       * or blocked waiter list. Therefore, current thread must diconnect
       * it from the list.
       *
       * An error is ONLY set when the caller was willing to wait.
       * entry_ptr->thrd_entry->lockwait_msecs > 0 */
      (void) lock_set_error_for_timeout (thread_p, entry_ptr);
      return LOCK_RESUMED_TIMEOUT;

    case LOCK_RESUMED_INTERRUPT:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_INTERRUPTED, 0);
      return LOCK_RESUMED_INTERRUPT;

    case LOCK_SUSPENDED:
    default:
      /* Probabely, the waiting structure has not been removed
       * from the waiting hash table. May be a system error.
       */
      (void) lock_set_error_for_timeout (thread_p, entry_ptr);
      return LOCK_RESUMED_TIMEOUT;
    }
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lk_resume - Resume the thread (transaction)
 *
 * return:
 *
 *   entry_ptr(in):
 *   state(in): resume state
 */
static void
lock_resume (LK_ENTRY * entry_ptr, int state)
{
  /* The caller is holding the thread entry mutex */
  /* The caller has identified the fact that lockwait is not NULL.
   * that is, the thread is suspended.
   */
  if (lk_Gl.verbose_mode == true)
    {
      char *__client_prog_name;	/* Client program name for transaction */
      char *__client_user_name;	/* Client user name for transaction    */
      char *__client_host_name;	/* Client host for transaction         */
      int __client_pid;		/* Client process id for transaction   */

      fflush (stderr);
      fflush (stdout);
      (void) logtb_find_client_name_host_pid (entry_ptr->tran_index,
					      &__client_prog_name,
					      &__client_user_name,
					      &__client_host_name,
					      &__client_pid);
      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_RYE,
				       MSGCAT_SET_LOCK,
				       MSGCAT_LK_RESUME_TRAN),
	       entry_ptr->tran_index, entry_ptr->tran_index,
	       __client_prog_name, __client_user_name, __client_host_name,
	       __client_pid);
      fflush (stdout);
    }

  /* Before wake up the thread,
   * clears lockwait field and set lockwait_state with the given state.
   */
  entry_ptr->thrd_entry->lockwait = (void *) NULL;
  entry_ptr->thrd_entry->lockwait_state = (int) state;

  /* wakes up the thread and release the thread entry mutex */
  entry_ptr->thrd_entry->resume_status = THREAD_LOCK_RESUMED;
  pthread_cond_signal (&entry_ptr->thrd_entry->wakeup_cond);
  thread_unlock_entry (entry_ptr->thrd_entry);
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_wakeup_deadlock_victim_timeout - Wake up the deadlock victim while notifying timeout
 *
 * return: true  if the transaction is treated as deadlock victim or
 *     false if the transaction is not treated as deadlock victim.
 *              in this case, the transaction has already been waken up
 *              by other threads with other purposes(ex. lock is granted)
 *
 *   tran_index(in): deadlock victim transaction
 *
 * Note:The given transaction was selected as a deadlock victim in the last
 *     deadlock detection. The deadlock victim is waked up and noitified of
 *     timeout by this function if the deadlock victim is still suspended.
 */
static bool
lock_wakeup_deadlock_victim_timeout (int tran_index)
{
  THREAD_ENTRY *thrd_array[10];
  int thrd_count, i;
  THREAD_ENTRY *thrd_ptr;
  bool wakeup_first = false;

  assert (false);		/* is impossible */

  thrd_count = thread_get_lockwait_entry (tran_index, &thrd_array[0]);
  for (i = 0; i < thrd_count; i++)
    {
      thrd_ptr = thrd_array[i];
      (void) thread_lock_entry (thrd_ptr);
      if (thrd_ptr->tran_index == tran_index
	  && LK_IS_LOCKWAIT_THREAD (thrd_ptr))
	{
	  /* wake up the thread while notifying timeout */
	  lock_resume ((LK_ENTRY *) thrd_ptr->lockwait,
		       LOCK_RESUMED_DEADLOCK_TIMEOUT);
	  wakeup_first = true;
	}
      else
	{
	  if (thrd_ptr->lockwait != NULL
	      || thrd_ptr->lockwait_state == (int) LOCK_SUSPENDED)
	    {
	      /* some strange lock wait state.. */
	      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		      ER_LK_STRANGE_LOCK_WAIT, 5, thrd_ptr->lockwait,
		      thrd_ptr->lockwait_state, thrd_ptr->index,
		      thrd_ptr->tid, thrd_ptr->tran_index);
	    }
	  /* The current thread has already been waken up by other threads.
	   * The current thread might be granted the lock. or with any other
	   * reason....... even if it is a thread of the deadlock victim.
	   */
	  /* release the thread entry mutex */
	  (void) thread_unlock_entry (thrd_ptr);
	}
    }
  return wakeup_first;
}
#endif /* SERVER_MODE */

#if defined (ENABLE_UNUSED_FUNCTION)
#if defined(SERVER_MODE)
/*
 * lock_wakeup_deadlock_victim_aborted - Wake up the deadlock victim while notifying aborted
 *
 * return: true  if the transaction is treated as deadlock victim or
 *     false if the transaction is not treated as deadlock victim.
 *              in this case, the transaction has already been waken up
 *              by other threads with other purposes(ex. lock is granted)
 *
 *   tran_index(in): deadlock victim transaction
 *
 * Note:The given transaction was selected as a deadlock victim in the last
 *     deadlock detection. The deadlock victim is waked up and noitified of
 *     abortion by this function if the deadlock victim is still suspended.
 */
static bool
lock_wakeup_deadlock_victim_aborted (int tran_index)
{
  THREAD_ENTRY *thrd_array[10];
  int thrd_count, i;
  THREAD_ENTRY *thrd_ptr;
  bool wakeup_first = false;

  assert (false);		/* is impossible */

  thrd_count = thread_get_lockwait_entry (tran_index, &thrd_array[0]);
  for (i = 0; i < thrd_count; i++)
    {
      thrd_ptr = thrd_array[i];
      (void) thread_lock_entry (thrd_ptr);
      if (thrd_ptr->tran_index == tran_index
	  && LK_IS_LOCKWAIT_THREAD (thrd_ptr))
	{
	  /* wake up the thread while notifying deadlock victim */
	  if (wakeup_first == false)
	    {
	      /* The current transaction is really aborted.
	       * Therefore, other threads of the current transaction must quit
	       * their executions and return to client.
	       * Then the first waken-up thread must be charge of the rollback
	       * of the current transaction.
	       */
	      /* set the transaction as deadlock victim */
	      lk_Gl.TWFG_node[tran_index].DL_victim = true;
	      lock_resume ((LK_ENTRY *) thrd_ptr->lockwait,
			   LOCK_RESUMED_ABORTED_FIRST);
	      wakeup_first = true;
	    }
	  else
	    {
	      lock_resume ((LK_ENTRY *) thrd_ptr->lockwait,
			   LOCK_RESUMED_ABORTED_OTHER);
	    }
	}
      else
	{
	  if (thrd_ptr->lockwait != NULL
	      || thrd_ptr->lockwait_state == (int) LOCK_SUSPENDED)
	    {
	      /* some strange lock wait state.. */
	      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		      ER_LK_STRANGE_LOCK_WAIT, 5, thrd_ptr->lockwait,
		      thrd_ptr->lockwait_state, thrd_ptr->index,
		      thrd_ptr->tid, thrd_ptr->tran_index);
	    }
	  /* The current thread has already been waken up by other threads.
	   * The current thread might have held the lock. or with any other
	   * reason....... even if it is a thread of the deadlock victim.
	   */
	  /* release the thread entry mutex */
	  (void) thread_unlock_entry (thrd_ptr);
	}
    }
  return wakeup_first;
}
#endif /* SERVER_MODE */
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 *  Private Functions Group: grant lock requests of blocked threads
 *   - lock_grant_blocked_holder()
 *   - lock_grant_blocked_waiter()
 *   - lock_grant_blocked_waiter_partial()
 */

#if defined(SERVER_MODE)
/*
 * lock_grant_blocked_holder - Grant blocked holders
 *
 * return:
 *
 *   res_ptr(in): This function grants blocked holders whose blocked lock mode is
 *     compatible with all the granted lock mode of non-blocked holders.
 */
static void
lock_grant_blocked_holder (THREAD_ENTRY * thread_p, LK_RES * res_ptr)
{
  LK_ENTRY *prev_check;
  LK_ENTRY *check, *i, *prev;
  LOCK mode;
  int compat;

  /* The caller is holding a resource mutex */

  prev_check = NULL;
  check = res_ptr->holder;
  while (check != NULL && check->blocked_mode != NULL_LOCK)
    {
      /* there are some blocked holders */
      mode = NULL_LOCK;
      for (i = check->next; i != NULL; i = i->next)
	{
	  assert (i->granted_mode >= NULL_LOCK && mode >= NULL_LOCK);
	  mode = lock_Conv[i->granted_mode][mode];
	  assert (mode != NA_LOCK);
	}

      assert (check->blocked_mode >= NULL_LOCK);
      compat = lock_Comp[check->blocked_mode][mode];
      assert (compat != DB_NA);

      if (compat == false)
	{
	  break;		/* stop the granting */
	}

      /* compatible: grant it */

      /* hold the thread entry mutex */
      (void) thread_lock_entry (check->thrd_entry);

      /* check if the thread is still waiting on a lock */
      if (LK_IS_LOCKWAIT_THREAD (check->thrd_entry))
	{
	  /* the thread is still waiting on a lock */

	  /* reposition the lock entry according to UPR */
	  for (prev = check, i = check->next; i != NULL;)
	    {
	      if (i->blocked_mode == NULL_LOCK)
		{
		  break;
		}
	      prev = i;
	      i = i->next;
	    }
	  if (prev != check)
	    {			/* reposition it */
	      /* remove it */
	      if (prev_check == NULL)
		{
		  res_ptr->holder = check->next;
		}
	      else
		{
		  prev_check->next = check->next;
		}
	      /* insert it */
	      check->next = prev->next;
	      prev->next = check;
	    }

	  /* change granted_mode and blocked_mode */
	  check->granted_mode = check->blocked_mode;
	  check->blocked_mode = NULL_LOCK;

	  /* Record number of acquired locks */
	  mnt_stats_counter (thread_p, MNT_STATS_OBJECT_LOCKS_ACQUIRED, 1);
#if defined(LK_TRACE_OBJECT)
	  LK_MSG_LOCK_ACQUIRED (entry_ptr);
#endif /* LK_TRACE_OBJECT */
	  /* wake up the blocked holder */
	  lock_resume (check, LOCK_RESUMED);
	}
      else
	{
	  if (check->thrd_entry->lockwait != NULL
	      || check->thrd_entry->lockwait_state == (int) LOCK_SUSPENDED)
	    {
	      /* some strange lock wait state.. */
	      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		      ER_LK_STRANGE_LOCK_WAIT, 5, check->thrd_entry->lockwait,
		      check->thrd_entry->lockwait_state,
		      check->thrd_entry->index, check->thrd_entry->tid,
		      check->thrd_entry->tran_index);
	    }
	  /* The thread is not waiting for a lock, currently.
	   * That is, the thread has already been waked up by timeout,
	   * deadlock victim or interrupt. In this case,
	   * we have nothing to do since the thread itself will remove
	   * this lock entry.
	   */
	  (void) thread_unlock_entry (check->thrd_entry);
	  prev_check = check;
	}

      if (prev_check == NULL)
	{
	  check = res_ptr->holder;
	}
      else
	{
	  check = prev_check->next;
	}
    }

}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_grant_blocked_waiter - Grant blocked waiters
 *
 * return:
 *
 *   res_ptr(in): This function grants blocked waiters whose blocked lock mode is
 *     compatible with the total mode of lock holders.
 */
static int
lock_grant_blocked_waiter (THREAD_ENTRY * thread_p, LK_RES * res_ptr)
{
  LK_ENTRY *prev_check;
  LK_ENTRY *check, *i;
  LOCK mode;
  bool change_total_waiters_mode = false;
  int error_code = NO_ERROR;
  int compat;

  /* The caller is holding a resource mutex */

  prev_check = NULL;
  check = res_ptr->waiter;
  while (check != NULL)
    {
      assert (check->blocked_mode >= NULL_LOCK
	      && res_ptr->total_holders_mode >= NULL_LOCK);
      compat = lock_Comp[check->blocked_mode][res_ptr->total_holders_mode];
      assert (compat != DB_NA);

      if (compat == false)
	{
	  break;		/* stop the granting */
	}

      /* compatible: grant it */
      /* hold the thread entry mutex */
      (void) thread_lock_entry (check->thrd_entry);

      /* check if the thread is still waiting for a lock */
      if (LK_IS_LOCKWAIT_THREAD (check->thrd_entry))
	{
	  /* The thread is still waiting for a lock. */
	  change_total_waiters_mode = true;

	  /* remove the lock entry from the waiter */
	  if (prev_check == NULL)
	    {
	      res_ptr->waiter = check->next;
	    }
	  else
	    {
	      prev_check->next = check->next;
	    }

	  /* change granted_mode and blocked_mode of the entry */
	  check->granted_mode = check->blocked_mode;
	  check->blocked_mode = NULL_LOCK;

	  /* position the lock entry in the holder list */
	  lock_position_holder_entry (res_ptr, check);

	  /* change total_holders_mode */
	  assert (check->granted_mode >= NULL_LOCK
		  && res_ptr->total_holders_mode >= NULL_LOCK);
	  res_ptr->total_holders_mode =
	    lock_Conv[check->granted_mode][res_ptr->total_holders_mode];
	  assert (res_ptr->total_holders_mode != NA_LOCK);

	  /* insert the lock entry into transaction hold list. */
	  lock_insert_into_tran_hold_list (check);

	  /* Record number of acquired locks */
	  mnt_stats_counter (thread_p, MNT_STATS_OBJECT_LOCKS_ACQUIRED, 1);
#if defined(LK_TRACE_OBJECT)
	  LK_MSG_LOCK_ACQUIRED (entry_ptr);
#endif /* LK_TRACE_OBJECT */

	  /* wake up the blocked waiter */
	  lock_resume (check, LOCK_RESUMED);
	}
      else
	{
	  if (check->thrd_entry->lockwait != NULL
	      || check->thrd_entry->lockwait_state == (int) LOCK_SUSPENDED)
	    {
	      /* some strange lock wait state.. */
	      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		      ER_LK_STRANGE_LOCK_WAIT, 5, check->thrd_entry->lockwait,
		      check->thrd_entry->lockwait_state,
		      check->thrd_entry->index, check->thrd_entry->tid,
		      check->thrd_entry->tran_index);
	      error_code = ER_LK_STRANGE_LOCK_WAIT;
	    }
	  /* The thread is not waiting on the lock, currently.
	   * That is, the thread has already been waken up
	   * by lock timeout, deadlock victim or interrupt.
	   * In this case, we have nothing to do
	   * since the thread itself will remove this lock entry.
	   */
	  (void) thread_unlock_entry (check->thrd_entry);
	  prev_check = check;
	}

      if (prev_check == NULL)
	{
	  check = res_ptr->waiter;
	}
      else
	{
	  check = prev_check->next;
	}
    }

  if (change_total_waiters_mode == true)
    {
      mode = NULL_LOCK;
      for (i = res_ptr->waiter; i != NULL; i = i->next)
	{
	  assert (i->blocked_mode >= NULL_LOCK && mode >= NULL_LOCK);
	  mode = lock_Conv[i->blocked_mode][mode];
	  assert (mode != NA_LOCK);
	}
      res_ptr->total_waiters_mode = mode;
    }

  return error_code;
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_grant_blocked_waiter_partial - Grant blocked waiters partially
 *
 * return:
 *
 *   res_ptr(in):
 *   from_whom(in):
 *
 * Note:This function grants blocked waiters that are located from from_whom
 *     to the end of waiter list whose blocked lock mode is compatible with
 *     all the blocked mode of the previous lock waiters and the total mode
 *     of lock holders.
 */
static void
lock_grant_blocked_waiter_partial (THREAD_ENTRY * thread_p, LK_RES * res_ptr,
				   LK_ENTRY * from_whom)
{
  LK_ENTRY *prev_check;
  LK_ENTRY *check, *i;
  LOCK mode;
  int compat;

  /* the caller is holding a resource mutex */

  mode = NULL_LOCK;
  prev_check = (LK_ENTRY *) NULL;
  check = res_ptr->waiter;
  while (check != from_whom)
    {
      assert (check->blocked_mode >= NULL_LOCK && mode >= NULL_LOCK);
      mode = lock_Conv[check->blocked_mode][mode];
      assert (mode != NA_LOCK);

      prev_check = check;
      check = check->next;
    }

  /* check = from_whom; */
  while (check != NULL)
    {
      assert (check->blocked_mode >= NULL_LOCK && mode >= NULL_LOCK);
      compat = lock_Comp[check->blocked_mode][mode];
      assert (compat != DB_NA);

      if (compat != true)
	{
	  break;
	}

      assert (check->blocked_mode >= NULL_LOCK
	      && res_ptr->total_holders_mode >= NULL_LOCK);
      compat = lock_Comp[check->blocked_mode][res_ptr->total_holders_mode];
      assert (compat != DB_NA);

      if (compat == false)
	{
	  assert (check->blocked_mode >= NULL_LOCK && mode >= NULL_LOCK);
	  mode = lock_Conv[check->blocked_mode][mode];
	  assert (mode != NA_LOCK);

	  prev_check = check;
	  check = check->next;
	  continue;
	}

      /* compatible: grant it */
      (void) thread_lock_entry (check->thrd_entry);
      if (LK_IS_LOCKWAIT_THREAD (check->thrd_entry))
	{
	  /* the thread is waiting on a lock */
	  /* remove the lock entry from the waiter */
	  if (prev_check == (LK_ENTRY *) NULL)
	    {
	      res_ptr->waiter = check->next;
	    }
	  else
	    {
	      prev_check->next = check->next;
	    }

	  /* change granted_mode and blocked_mode of the entry */
	  check->granted_mode = check->blocked_mode;
	  check->blocked_mode = NULL_LOCK;

	  /* position the lock entry into the holder list */
	  lock_position_holder_entry (res_ptr, check);

	  /* change total_holders_mode */
	  assert (check->granted_mode >= NULL_LOCK
		  && res_ptr->total_holders_mode >= NULL_LOCK);
	  res_ptr->total_holders_mode =
	    lock_Conv[check->granted_mode][res_ptr->total_holders_mode];
	  assert (res_ptr->total_holders_mode != NA_LOCK);

	  /* insert into transaction lock hold list */
	  lock_insert_into_tran_hold_list (check);

	  /* Record number of acquired locks */
	  mnt_stats_counter (thread_p, MNT_STATS_OBJECT_LOCKS_ACQUIRED, 1);
#if defined(LK_TRACE_OBJECT)
	  LK_MSG_LOCK_ACQUIRED (entry_ptr);
#endif /* LK_TRACE_OBJECT */

	  /* wake up the blocked waiter (correctness must be checked) */
	  lock_resume (check, LOCK_RESUMED);
	}
      else
	{
	  if (check->thrd_entry->lockwait != NULL
	      || check->thrd_entry->lockwait_state == (int) LOCK_SUSPENDED)
	    {
	      /* some strange lock wait state.. */
	      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		      ER_LK_STRANGE_LOCK_WAIT, 5, check->thrd_entry->lockwait,
		      check->thrd_entry->lockwait_state,
		      check->thrd_entry->index, check->thrd_entry->tid,
		      check->thrd_entry->tran_index);
	    }
	  /* The thread is not waiting on the lock. That is, the thread has
	   * already been waken up by lock timeout, deadlock victim or interrupt.
	   * In this case, we have nothing to do since the thread itself
	   * will remove this lock entry.
	   */
	  (void) thread_unlock_entry (check->thrd_entry);

	  /* change prev_check */
	  assert (check->blocked_mode >= NULL_LOCK && mode >= NULL_LOCK);
	  mode = lock_Conv[check->blocked_mode][mode];
	  assert (mode != NA_LOCK);

	  prev_check = check;
	}

      if (prev_check == NULL)
	{
	  check = res_ptr->waiter;
	}
      else
	{
	  check = prev_check->next;
	}
    }

  if (check == NULL)
    {
      res_ptr->total_waiters_mode = mode;
    }
  else
    {
      mode = NULL_LOCK;
      for (i = res_ptr->waiter; i != NULL; i = i->next)
	{
	  assert (i->blocked_mode >= NULL_LOCK && mode >= NULL_LOCK);
	  mode = lock_Conv[i->blocked_mode][mode];
	  assert (mode != NA_LOCK);
	}
      res_ptr->total_waiters_mode = mode;
    }

}
#endif /* SERVER_MODE */

/*
 *  Private Functions Group: major functions for locking and unlocking
 *
 *   - lk_internal_lock_object_instant()
 *   - lk_internal_lock_object()
 *   - lk_internal_unlock_object()
 */

#if defined(SERVER_MODE)
static void
stat_counter_lock (THREAD_ENTRY * thread_p, DB_TYPE val_type,
		   const DB_VALUE * val, UINT64 start_time)
{
  if (val_type == DB_TYPE_OID)
    {
      mnt_stats_counter_with_time (thread_p, MNT_STATS_CLASS_LOCKS_REQUEST, 1,
				   start_time);
    }
  else if (db_value_compare (val, &system_Ddl_key) == DB_EQ)
    {
      mnt_stats_counter_with_time (thread_p, MNT_STATS_DDL_LOCKS_REQUESTS, 1,
				   start_time);
    }
  else if (db_value_compare (val, &shard_Catalog_key) == DB_EQ)
    {
      mnt_stats_counter_with_time (thread_p, MNT_STATS_CATALOG_LOCKS_REQUEST,
				   1, start_time);
    }
  else if (db_value_compare (val, &shard_Global_key) == DB_EQ)
    {
      mnt_stats_counter_with_time (thread_p, MNT_STATS_GLOBAL_LOCKS_REQUEST,
				   1, start_time);
    }
  else
    {
      mnt_stats_counter_with_time (thread_p, MNT_STATS_SHARD_LOCKS_REQUEST, 1,
				   start_time);
    }
}

/*
 * lock_internal_perform_lock_object - Performs actual object lock operation
 *
 * return: one of following values
 *              LK_GRANTED
 *              LK_NOTGRANTED_DUE_ABORTED
 *              LK_NOTGRANTED_DUE_TIMEOUT
 *              LK_NOTGRANTED_DUE_ERROR
 *
 *   tran_index(in):
 *   oid(in):
 *   class_oid(in):
 *   lock(in):
 *   wait_msecs(in):
 *   entry_addr_ptr(in):
 *   class_entry(in):
 *
 * Note:lock an object whose id is pointed by oid with given lock mode 'lock'.
 *
 *     If cond_flag is true and the object has already been locked
 *     by other transaction, then return LK_NOTGRANTED;
 *     else this transaction is suspended until it can acquire the lock.
 */
static int
lock_internal_perform_lock_object (THREAD_ENTRY * thread_p, int tran_index,
				   const DB_VALUE * val, LOCK lock,
				   int wait_msecs, LK_ENTRY ** entry_addr_ptr)
{
  TRAN_ISOLATION isolation;
  unsigned int hash_index;
  LK_HASH *hash_anchor;
  int ret_val;
  LOCK group_mode, old_mode, new_mode;	/* lock mode */
  LK_RES *res_ptr;
  LK_ENTRY *entry_ptr = (LK_ENTRY *) NULL;
  LK_ENTRY *wait_entry_ptr = (LK_ENTRY *) NULL;
  LK_ENTRY *prev, *curr, *i;
  bool lock_conversion = false;
  THREAD_ENTRY *thrd_entry;
  int rv;
  int compat;
  DB_TYPE val_type;
  UINT64 perf_start;

  assert (!DB_IS_NULL (val));
  assert (pgbuf_get_num_hold_cnt (thread_p,
				  PAGE_UNKNOWN /* all page */ ) == 0);

  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  thrd_entry = thread_p;

  new_mode = group_mode = old_mode = NULL_LOCK;

  /* isolation */
  isolation = logtb_find_isolation (tran_index);

  /* initialize */
  *entry_addr_ptr = (LK_ENTRY *) NULL;
  val_type = DB_VALUE_TYPE (val);

  PERF_MON_GET_CURRENT_TIME (perf_start);

start:

  /* check if the lockable object is in object lock table */
  hash_index = mht_get_hash_number (lk_Gl.obj_hash_size, val);
  hash_anchor = &lk_Gl.obj_hash_table[hash_index];

  /* hold hash_mutex */
  rv = pthread_mutex_lock (&hash_anchor->hash_mutex);

  /* find the lockable object in the hash chain */
  res_ptr = hash_anchor->hash_next;
  for (; res_ptr != (LK_RES *) NULL; res_ptr = res_ptr->hash_next)
    {
      if (DB_VALUE_TYPE (&res_ptr->val) == val_type
	  && db_value_compare (&res_ptr->val, val) == DB_EQ)
	{
	  break;
	}
    }

  if (res_ptr == (LK_RES *) NULL)
    {
      /* the lockable object is NOT in the hash chain */
      /* the lock request can be granted. */

      /* allocate a lock resource entry */
      res_ptr = lock_alloc_resource ();
      if (res_ptr == (LK_RES *) NULL)
	{
	  pthread_mutex_unlock (&hash_anchor->hash_mutex);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_ALLOC_RESOURCE,
		  1, "lock resource entry");
	  return LK_NOTGRANTED_DUE_ERROR;
	}
      /* initialize the lock resource entry */
      lock_initialize_resource_as_allocated (thread_p, res_ptr, val,
					     NULL_LOCK);

      /* hold res_mutex */
      rv = pthread_mutex_lock (&res_ptr->res_mutex);

      /* allocate a lock entry */
      entry_ptr = lock_alloc_entry ();
      if (entry_ptr == (LK_ENTRY *) NULL)
	{
	  pthread_mutex_unlock (&res_ptr->res_mutex);
	  pthread_mutex_unlock (&hash_anchor->hash_mutex);
	  lock_free_resource (thread_p, res_ptr);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_ALLOC_RESOURCE,
		  1, "lock heap entry");
	  return LK_NOTGRANTED_DUE_ERROR;
	}
      /* initialize the lock entry as granted state */
      lock_initialize_entry_as_granted (entry_ptr, tran_index, res_ptr, lock);

      /* add the lock entry into the holder list */
      res_ptr->holder = entry_ptr;

      /* add the lock entry into the transaction hold list */
      lock_insert_into_tran_hold_list (entry_ptr);

      res_ptr->total_holders_mode = lock;

      /* Record number of acquired locks */
      mnt_stats_counter (thread_p, MNT_STATS_OBJECT_LOCKS_ACQUIRED, 1);
#if defined(LK_TRACE_OBJECT)
      LK_MSG_LOCK_ACQUIRED (entry_ptr);
#endif /* LK_TRACE_OBJECT */

      /* connect the lock resource entry into the hash chain */
      res_ptr->hash_next = hash_anchor->hash_next;
      hash_anchor->hash_next = res_ptr;

      /* release all mutexes */
      pthread_mutex_unlock (&res_ptr->res_mutex);
      pthread_mutex_unlock (&hash_anchor->hash_mutex);

      *entry_addr_ptr = entry_ptr;

      stat_counter_lock (thread_p, val_type, val, perf_start);
      return LK_GRANTED;
    }

  /* the lockable object exists in the hash chain
   * So, check whether I am a holder of the object.
   */

  /* hold lock resource mutex */
  rv = pthread_mutex_lock (&res_ptr->res_mutex);
  /* release lock hash mutex */
  pthread_mutex_unlock (&hash_anchor->hash_mutex);

  /* Note: I am holding res_mutex only */

  /* find the lock entry of current transaction */
  entry_ptr = res_ptr->holder;
  while (entry_ptr != (LK_ENTRY *) NULL)
    {
      if (entry_ptr->tran_index == tran_index)
	{
	  break;
	}
      entry_ptr = entry_ptr->next;
    }

  if (entry_ptr == NULL)
    {
      /* The object exists in the hash chain &
       * I am not a lock holder of the lockable object.
       */
      /* 1. I am not a holder & my request can be granted. */
      assert (lock >= NULL_LOCK && res_ptr->total_waiters_mode >= NULL_LOCK
	      && res_ptr->total_holders_mode >= NULL_LOCK);
      compat = lock_Comp[lock][res_ptr->total_holders_mode];
      assert (compat != DB_NA);
#if 1				/* TODO - FIXME_LOCK */
      if (compat != true)
	{
	  /* check the compatibility with other holders' granted mode */
	  new_mode = lock;
	  group_mode = NULL_LOCK;
	  for (i = res_ptr->holder; i != (LK_ENTRY *) NULL; i = i->next)
	    {
	      assert (i->granted_mode >= NULL_LOCK
		      && group_mode >= NULL_LOCK);
	      group_mode = lock_Conv[i->granted_mode][group_mode];
	      assert (group_mode != NA_LOCK);
	    }

	  assert (new_mode >= NULL_LOCK && group_mode >= NULL_LOCK);
	  compat = lock_Comp[new_mode][group_mode];
	  assert (compat != DB_NA);
	}
#endif

      if (compat == true)
	{
	  /* allocate a lock entry */
	  entry_ptr = lock_alloc_entry ();
	  if (entry_ptr == (LK_ENTRY *) NULL)
	    {
	      pthread_mutex_unlock (&res_ptr->res_mutex);
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_ALLOC_RESOURCE,
		      1, "lock heap entry");
	      return LK_NOTGRANTED_DUE_ERROR;
	    }
	  /* initialize the lock entry as granted state */
	  lock_initialize_entry_as_granted (entry_ptr, tran_index, res_ptr,
					    lock);

	  /* add the lock entry into the holder list */
	  lock_position_holder_entry (res_ptr, entry_ptr);

	  /* change total_holders_mode (total mode of holder list) */
	  assert (lock >= NULL_LOCK
		  && res_ptr->total_holders_mode >= NULL_LOCK);
	  res_ptr->total_holders_mode =
	    lock_Conv[lock][res_ptr->total_holders_mode];
	  assert (res_ptr->total_holders_mode != NA_LOCK);

	  /* add the lock entry into the transaction hold list */
	  lock_insert_into_tran_hold_list (entry_ptr);

	  /* Record number of acquired locks */
	  mnt_stats_counter (thread_p, MNT_STATS_OBJECT_LOCKS_ACQUIRED, 1);
#if defined(LK_TRACE_OBJECT)
	  LK_MSG_LOCK_ACQUIRED (entry_ptr);
#endif /* LK_TRACE_OBJECT */

	  pthread_mutex_unlock (&res_ptr->res_mutex);
	  *entry_addr_ptr = entry_ptr;

	  stat_counter_lock (thread_p, val_type, val, perf_start);
	  return LK_GRANTED;
	}

      if (wait_msecs == LK_ZERO_WAIT || wait_msecs == LK_FORCE_ZERO_WAIT)
	{
	  pthread_mutex_unlock (&res_ptr->res_mutex);
	  if (wait_msecs == LK_ZERO_WAIT)
	    {
	      if (entry_ptr == NULL)
		{
		  entry_ptr = lock_alloc_entry ();
		  if (entry_ptr == (LK_ENTRY *) NULL)
		    {
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_LK_ALLOC_RESOURCE, 1, "lock heap entry");
		      return LK_NOTGRANTED_DUE_ERROR;
		    }
		  lock_initialize_entry_as_blocked (entry_ptr, thread_p,
						    tran_index, res_ptr,
						    lock);
		}
	      (void) lock_set_error_for_timeout (thread_p, entry_ptr);

	      lock_free_entry (entry_ptr);
	    }
	  return LK_NOTGRANTED_DUE_TIMEOUT;
	}

      /* check if another thread is waiting for the same resource
       */
      wait_entry_ptr = res_ptr->waiter;
      while (wait_entry_ptr != (LK_ENTRY *) NULL)
	{
	  if (wait_entry_ptr->tran_index == tran_index)
	    {
	      break;
	    }
	  wait_entry_ptr = wait_entry_ptr->next;
	}

      if (wait_entry_ptr != NULL)
	{
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		  ER_LK_MANY_LOCK_WAIT_TRAN, 1, tran_index);
	  thread_lock_entry (thrd_entry);
	  thread_lock_entry (wait_entry_ptr->thrd_entry);
	  if (wait_entry_ptr->thrd_entry->lockwait == NULL)
	    {
	      /*  */
	      thread_unlock_entry (wait_entry_ptr->thrd_entry);
	      thread_unlock_entry (thrd_entry);
	      pthread_mutex_unlock (&res_ptr->res_mutex);
	      goto start;
	    }

	  thrd_entry->tran_next_wait =
	    wait_entry_ptr->thrd_entry->tran_next_wait;
	  wait_entry_ptr->thrd_entry->tran_next_wait = thrd_entry;

	  thread_unlock_entry (wait_entry_ptr->thrd_entry);
	  pthread_mutex_unlock (&res_ptr->res_mutex);

	  thread_suspend_wakeup_and_unlock_entry (thrd_entry,
						  THREAD_LOCK_SUSPENDED);
	  if (entry_ptr)
	    {
	      if (entry_ptr->thrd_entry->resume_status ==
		  THREAD_RESUME_DUE_TO_INTERRUPT)
		{
		  /* a shutdown thread wakes me up */
		  return LK_NOTGRANTED_DUE_ERROR;
		}
	      else if (entry_ptr->thrd_entry->resume_status !=
		       THREAD_LOCK_RESUMED)
		{
		  /* wake up with other reason */
		  assert (0);

		  return LK_NOTGRANTED_DUE_ERROR;
		}
	      else
		{
		  assert (entry_ptr->thrd_entry->resume_status ==
			  THREAD_LOCK_RESUMED);
		}
	    }

	  goto start;
	}

      /* allocate a lock entry. */
      entry_ptr = lock_alloc_entry ();
      if (entry_ptr == (LK_ENTRY *) NULL)
	{
	  pthread_mutex_unlock (&res_ptr->res_mutex);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_ALLOC_RESOURCE,
		  1, "lock heap entry");
	  return LK_NOTGRANTED_DUE_ERROR;
	}
      /* initialize the lock entry as blocked state */
      lock_initialize_entry_as_blocked (entry_ptr, thread_p, tran_index,
					res_ptr, lock);

      /* append the lock request at the end of the waiter */
      prev = (LK_ENTRY *) NULL;
      for (i = res_ptr->waiter; i != (LK_ENTRY *) NULL;)
	{
	  prev = i;
	  i = i->next;
	}
      if (prev == (LK_ENTRY *) NULL)
	{
	  res_ptr->waiter = entry_ptr;
	}
      else
	{
	  prev->next = entry_ptr;
	}

      /* change total_waiters_mode (total mode of waiting waiter) */
      assert (lock >= NULL_LOCK && res_ptr->total_waiters_mode >= NULL_LOCK);
      res_ptr->total_waiters_mode =
	lock_Conv[lock][res_ptr->total_waiters_mode];
      assert (res_ptr->total_waiters_mode != NA_LOCK);

      goto blocked;

    }				/* end of a new lock request */

  /* The object exists in the hash chain &
   * I am a lock holder of the lockable object.
   */
  lock_conversion = true;
  old_mode = entry_ptr->granted_mode;
  assert (lock >= NULL_LOCK && entry_ptr->granted_mode >= NULL_LOCK);
  new_mode = lock_Conv[lock][entry_ptr->granted_mode];
  assert (new_mode != NA_LOCK);

  if (new_mode == entry_ptr->granted_mode)
    {
      /* a request with either a less exclusive or an equal mode of lock */
      entry_ptr->count += 1;

      pthread_mutex_unlock (&res_ptr->res_mutex);
      mnt_stats_counter (thread_p, MNT_STATS_OBJECT_LOCKS_RE_REQUESTED, 1);
      *entry_addr_ptr = entry_ptr;

      stat_counter_lock (thread_p, val_type, val, perf_start);
      return LK_GRANTED;
    }

  /* check the compatibility with other holders' granted mode */
  group_mode = NULL_LOCK;
  for (i = res_ptr->holder; i != (LK_ENTRY *) NULL; i = i->next)
    {
      if (i != entry_ptr)
	{
	  assert (i->granted_mode >= NULL_LOCK && group_mode >= NULL_LOCK);
	  group_mode = lock_Conv[i->granted_mode][group_mode];
	  assert (group_mode != NA_LOCK);
	}
    }

  assert (new_mode >= NULL_LOCK && group_mode >= NULL_LOCK);
  compat = lock_Comp[new_mode][group_mode];
  assert (compat != DB_NA);

  if (compat == true)
    {
      entry_ptr->granted_mode = new_mode;
      entry_ptr->count += 1;

      assert (lock >= NULL_LOCK && res_ptr->total_holders_mode >= NULL_LOCK);
      res_ptr->total_holders_mode =
	lock_Conv[lock][res_ptr->total_holders_mode];
      assert (res_ptr->total_holders_mode != NA_LOCK);

      pthread_mutex_unlock (&res_ptr->res_mutex);

      goto lock_conversion_treatement;
    }

  /* I am a holder & my request cannot be granted. */
  if (wait_msecs == LK_ZERO_WAIT || wait_msecs == LK_FORCE_ZERO_WAIT)
    {
      pthread_mutex_unlock (&res_ptr->res_mutex);
      if (wait_msecs == LK_ZERO_WAIT)
	{
	  LK_ENTRY *p = lock_alloc_entry ();

	  if (p != NULL)
	    {
	      lock_initialize_entry_as_blocked (p, thread_p, tran_index,
						res_ptr, lock);
	      lock_set_error_for_timeout (thread_p, p);
	      lock_free_entry (p);
	    }
	}
      return LK_NOTGRANTED_DUE_TIMEOUT;
    }

  /* Upgrader Positioning Rule (UPR) */

  /* check if another thread is waiting for the same resource
   */
  if (entry_ptr->blocked_mode != NULL_LOCK)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_LK_MANY_LOCK_WAIT_TRAN,
	      1, tran_index);
      thread_lock_entry (thrd_entry);
      thread_lock_entry (entry_ptr->thrd_entry);

      if (entry_ptr->thrd_entry->lockwait == NULL)
	{
	  thread_unlock_entry (entry_ptr->thrd_entry);
	  thread_unlock_entry (thrd_entry);
	  pthread_mutex_unlock (&res_ptr->res_mutex);
	  goto start;
	}

      thrd_entry->tran_next_wait = entry_ptr->thrd_entry->tran_next_wait;
      entry_ptr->thrd_entry->tran_next_wait = thrd_entry;

      thread_unlock_entry (entry_ptr->thrd_entry);

      pthread_mutex_unlock (&res_ptr->res_mutex);

      thread_suspend_wakeup_and_unlock_entry (thrd_entry,
					      THREAD_LOCK_SUSPENDED);
      if (thrd_entry->resume_status == THREAD_RESUME_DUE_TO_INTERRUPT)
	{
	  /* a shutdown thread wakes me up */
	  return LK_NOTGRANTED_DUE_ERROR;
	}
      else if (thrd_entry->resume_status != THREAD_LOCK_RESUMED)
	{
	  /* wake up with other reason */
	  assert (0);

	  return LK_NOTGRANTED_DUE_ERROR;
	}
      else
	{
	  assert (thrd_entry->resume_status == THREAD_LOCK_RESUMED);
	}

      goto start;
    }

  entry_ptr->blocked_mode = new_mode;
  entry_ptr->count += 1;
  entry_ptr->thrd_entry = thread_p;

#if 1				/* TODO - FIXME_LOCK */
  assert (lock >= NULL_LOCK && res_ptr->total_holders_mode >= NULL_LOCK);
  res_ptr->total_holders_mode = lock_Conv[lock][res_ptr->total_holders_mode];
  assert (res_ptr->total_holders_mode != NA_LOCK);
#endif

  /* remove the lock entry from the holder list */
  prev = (LK_ENTRY *) NULL;
  curr = res_ptr->holder;
  while ((curr != (LK_ENTRY *) NULL) && (curr != entry_ptr))
    {
      prev = curr;
      curr = curr->next;
    }
  if (prev == (LK_ENTRY *) NULL)
    {
      res_ptr->holder = entry_ptr->next;
    }
  else
    {
      prev->next = entry_ptr->next;
    }

  /* position the lock entry in the holder list according to UPR */
  lock_position_holder_entry (res_ptr, entry_ptr);

blocked:

  /* LK_CANWAIT(wait_msecs) : wait_msecs > 0 */
  mnt_stats_counter (thread_p, MNT_STATS_OBJECT_LOCKS_WAITS, 1);
#if defined(LK_TRACE_OBJECT)
  LK_MSG_LOCK_WAITFOR (entry_ptr);
#endif /* LK_TRACE_OBJECT */

  (void) thread_lock_entry (entry_ptr->thrd_entry);
  pthread_mutex_unlock (&res_ptr->res_mutex);
  ret_val = lock_suspend (thread_p, entry_ptr, wait_msecs);
  if (ret_val != LOCK_RESUMED)
    {
      /* Following three cases are possible.
       * 1. lock timeout 2. deadlock victim  3. interrupt
       * In any case, current thread must remove the wait info.
       */
      lock_internal_perform_unlock_object (thread_p, entry_ptr, false);

      if (ret_val == LOCK_RESUMED_ABORTED)
	{
	  return LK_NOTGRANTED_DUE_ABORTED;
	}
      else if (ret_val == LOCK_RESUMED_INTERRUPT)
	{
	  return LK_NOTGRANTED_DUE_ERROR;
	}
      else			/* LOCK_RESUMED_TIMEOUT || LOCK_SUSPENDED */
	{
	  return LK_NOTGRANTED_DUE_TIMEOUT;
	}
    }

  /* The transaction now got the lock on the object */
lock_conversion_treatement:

  *entry_addr_ptr = entry_ptr;
  stat_counter_lock (thread_p, val_type, val, perf_start);
  return LK_GRANTED;
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_internal_perform_unlock_object - Performs actual object unlock operation
 *
 * return:
 *
 *   entry_ptr(in):
 *   release_flag(in):
 *
 * Note:Unlock a lock specified by entry_ptr.
 *     Therefore, for the 2 phase locking, the caller must unlock from leaf
 *     to root or atomically all locks to which the transaction is related.
 *
 *     if release_flag is true, release the lock item.
 *     Otherwise, just decrement the lock count for supporting isolation level.
 */
static void
lock_internal_perform_unlock_object (THREAD_ENTRY * thread_p,
				     LK_ENTRY * entry_ptr, int release_flag)
{
  int tran_index;
  LK_RES *res_ptr;
  LK_ENTRY *i;
  LK_ENTRY *prev, *curr;
  LK_ENTRY *from_whom;
  LOCK mode;
  int rv;

#if defined(LK_DUMP)
  if (lk_Gl.dump_level >= 1)
    {
      fprintf (stderr,
	       "LK_DUMP::lk_internal_unlock_object()\n"
	       "  tran(%2d) : oid(%2d|%3d|%3d), class_oid(%2d|%3d|%3d), LOCK(%7s)\n",
	       entry_ptr->tran_index,
	       entry_ptr->res_head->oid.volid,
	       entry_ptr->res_head->oid.pageid,
	       entry_ptr->res_head->oid.slotid,
	       entry_ptr->res_head->class_oid.volid,
	       entry_ptr->res_head->class_oid.pageid,
	       entry_ptr->res_head->class_oid.slotid,
	       LOCK_TO_LOCKMODE_STRING (entry_ptr->granted_mode));
    }
#endif /* LK_DUMP */

  if (entry_ptr == (LK_ENTRY *) NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_BAD_ARGUMENT, 2,
	      "lk_internal_unlock_object", "NULL entry pointer");
      return;
    }

  tran_index = logtb_get_current_tran_index (thread_p);
  if (entry_ptr->tran_index != tran_index)
    {
      assert (false);
      return;
    }

  if (release_flag == false)
    {
      entry_ptr->count--;

      if (entry_ptr->blocked_mode == NULL_LOCK && entry_ptr->count > 0)
	{
	  return;
	}
    }

  /* hold resource mutex */
  res_ptr = entry_ptr->res_head;
  rv = pthread_mutex_lock (&res_ptr->res_mutex);

  /* check if the transaction is in the holder list */
  prev = (LK_ENTRY *) NULL;
  curr = res_ptr->holder;
  while (curr != (LK_ENTRY *) NULL)
    {
      if (curr->tran_index == tran_index)
	{
	  break;
	}
      prev = curr;
      curr = curr->next;
    }

  if (curr == (LK_ENTRY *) NULL)
    {
      /* the transaction is not in the holder list, check the waiter. */
      prev = (LK_ENTRY *) NULL;
      curr = res_ptr->waiter;
      while (curr != (LK_ENTRY *) NULL)
	{
	  if (curr->tran_index == tran_index)
	    {
	      break;
	    }
	  prev = curr;
	  curr = curr->next;
	}

      if (curr != (LK_ENTRY *) NULL)
	{
	  /* get the next lock waiter */
	  from_whom = curr->next;

	  /* remove the lock entry from the waiter */
	  if (prev == (LK_ENTRY *) NULL)
	    {
	      res_ptr->waiter = curr->next;
	    }
	  else
	    {
	      prev->next = curr->next;
	    }

	  /* free the lock entry */
	  lock_free_entry (curr);

	  if (from_whom != (LK_ENTRY *) NULL)
	    {
	      /* grant blocked waiter & change total_waiters_mode */
	      lock_grant_blocked_waiter_partial (thread_p, res_ptr,
						 from_whom);
	    }
	  else
	    {
	      /* change only total_waiters_mode */
	      mode = NULL_LOCK;
	      for (i = res_ptr->waiter; i != (LK_ENTRY *) NULL; i = i->next)
		{
		  assert (i->blocked_mode >= NULL_LOCK && mode >= NULL_LOCK);
		  mode = lock_Conv[i->blocked_mode][mode];
		  assert (mode != NA_LOCK);
		}
	      res_ptr->total_waiters_mode = mode;
	    }
	}
      else
	{
	  assert (false);
	}

      pthread_mutex_unlock (&res_ptr->res_mutex);

      return;
    }

  /* The transaction is in the holder list. Consult the holder list. */

  /* remove the entry from the holder list */
  if (prev == (LK_ENTRY *) NULL)
    {
      res_ptr->holder = curr->next;
    }
  else
    {
      prev->next = curr->next;
    }

  if (release_flag == false && curr->count > 0)
    {
      /* The current transaction was a blocked holder.
       * lock timeout is called or it is selected as a deadlock victim
       */
      curr->blocked_mode = NULL_LOCK;
      lock_position_holder_entry (res_ptr, entry_ptr);
    }
  else
    {
      /* remove the lock entry from the transaction lock hold list */
      (void) lock_delete_from_tran_hold_list (curr);

      /* free the lock entry */
      lock_free_entry (curr);
    }

  /* change total_holders_mode */
  mode = NULL_LOCK;
  for (i = res_ptr->holder; i != (LK_ENTRY *) NULL; i = i->next)
    {
      assert (i->granted_mode >= NULL_LOCK && mode >= NULL_LOCK);
      mode = lock_Conv[i->granted_mode][mode];
      assert (mode != NA_LOCK);

#if 1				/* TODO - FIXME_LOCK */
      assert (i->blocked_mode >= NULL_LOCK && mode >= NULL_LOCK);
      mode = lock_Conv[i->blocked_mode][mode];
      assert (mode != NA_LOCK);
#endif
    }
  res_ptr->total_holders_mode = mode;

  if (res_ptr->holder == NULL && res_ptr->waiter == NULL)
    {
      /* if resource entry is empty, deallocate it. */
      (void) lock_dealloc_resource (thread_p, res_ptr);
    }
  else
    {
      /* grant blocked holders and blocked waiters */
      lock_grant_blocked_holder (thread_p, res_ptr);

      (void) lock_grant_blocked_waiter (thread_p, res_ptr);
      pthread_mutex_unlock (&res_ptr->res_mutex);
    }
}
#endif /* SERVER_MODE */

/*
 *  Private Functions Group: local deadlock detection and resolution
 *
 *   - lk_add_WFG_edge()
 */

#if defined(SERVER_MODE)
/*
 * lock_add_WFG_edge -
 *
 * return: error code
 *
 *   from_tran_index(in): waiting transaction index
 *   to_tran_index(in): waited transaction index
 *   holder_flag(in): true(if to_tran_index is Holder), false(otherwise)
 *   edge_wait_stime(in):
 *
 * Note:add an edge to WFG which represents that
 *     'from_tran_index' transaction waits for 'to_tran_index' transaction.
 */
static int
lock_add_WFG_edge (int from_tran_index, int to_tran_index,
		   int holder_flag, INT64 edge_wait_stime)
{
  int prev, curr;
  int i;
  int alloc_idx;
  char *temp_ptr;

  /* check if the transactions has been selected as victims */
  /* Note that the transactions might be old deadlock victims */
  if (lk_Gl.TWFG_node[from_tran_index].DL_victim == true
      || lk_Gl.TWFG_node[to_tran_index].DL_victim == true)
    {
      return NO_ERROR;
    }

  /* increment global edge sequence number */
  lk_Gl.global_edge_seq_num++;

  if (lk_Gl.TWFG_node[from_tran_index].checked_by_deadlock_detector == false)
    {
      /* a new transaction started */
      if (lk_Gl.TWFG_node[from_tran_index].first_edge != -1)
	{
	  prev = -1;
	  curr = lk_Gl.TWFG_node[from_tran_index].first_edge;
	  while (curr != -1)
	    {
	      prev = curr;
	      curr = lk_Gl.TWFG_edge[curr].next;
	    }
	  lk_Gl.TWFG_edge[prev].next = lk_Gl.TWFG_free_edge_idx;
	  lk_Gl.TWFG_free_edge_idx =
	    lk_Gl.TWFG_node[from_tran_index].first_edge;
	  lk_Gl.TWFG_node[from_tran_index].first_edge = -1;
	}
      lk_Gl.TWFG_node[from_tran_index].checked_by_deadlock_detector = true;
      lk_Gl.TWFG_node[from_tran_index].tran_edge_seq_num =
	lk_Gl.global_edge_seq_num;
    }

  if (lk_Gl.TWFG_node[to_tran_index].checked_by_deadlock_detector == false)
    {
      /* a new transaction started */
      if (lk_Gl.TWFG_node[to_tran_index].first_edge != -1)
	{
	  prev = -1;
	  curr = lk_Gl.TWFG_node[to_tran_index].first_edge;
	  while (curr != -1)
	    {
	      prev = curr;
	      curr = lk_Gl.TWFG_edge[curr].next;
	    }
	  lk_Gl.TWFG_edge[prev].next = lk_Gl.TWFG_free_edge_idx;
	  lk_Gl.TWFG_free_edge_idx =
	    lk_Gl.TWFG_node[to_tran_index].first_edge;
	  lk_Gl.TWFG_node[to_tran_index].first_edge = -1;
	}
      lk_Gl.TWFG_node[to_tran_index].checked_by_deadlock_detector = true;
      lk_Gl.TWFG_node[to_tran_index].tran_edge_seq_num =
	lk_Gl.global_edge_seq_num;
    }

  /* NOTE the following description..
   * According to the above code, whenever it is identified that
   * a transaction has been terminated during deadlock detection,
   * the transaction is checked again as a new transaction. And,
   * the current edge is based on the current active transactions.
   */

  if (lk_Gl.TWFG_free_edge_idx == -1)
    {				/* too many WFG edges */
      if (lk_Gl.max_TWFG_edge == LK_MIN_TWFG_EDGE_COUNT)
	{
	  lk_Gl.max_TWFG_edge = LK_MID_TWFG_EDGE_COUNT;
	  for (i = LK_MIN_TWFG_EDGE_COUNT; i < lk_Gl.max_TWFG_edge; i++)
	    {
	      lk_Gl.TWFG_edge[i].to_tran_index = -1;
	      lk_Gl.TWFG_edge[i].next = (i + 1);
	    }
	  lk_Gl.TWFG_edge[lk_Gl.max_TWFG_edge - 1].next = -1;
	  lk_Gl.TWFG_free_edge_idx = LK_MIN_TWFG_EDGE_COUNT;
	}
      else if (lk_Gl.max_TWFG_edge == LK_MID_TWFG_EDGE_COUNT)
	{
	  temp_ptr = (char *) lk_Gl.TWFG_edge;
	  lk_Gl.max_TWFG_edge = LK_MAX_TWFG_EDGE_COUNT;
	  lk_Gl.TWFG_edge =
	    (LK_WFG_EDGE *) malloc (SIZEOF_LK_WFG_EDGE * lk_Gl.max_TWFG_edge);
	  if (lk_Gl.TWFG_edge == (LK_WFG_EDGE *) NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1,
		      (SIZEOF_LK_WFG_EDGE * lk_Gl.max_TWFG_edge));
	      return ER_OUT_OF_VIRTUAL_MEMORY;	/* no method */
	    }
	  (void) memcpy ((char *) lk_Gl.TWFG_edge, temp_ptr,
			 (SIZEOF_LK_WFG_EDGE * LK_MID_TWFG_EDGE_COUNT));
	  for (i = LK_MID_TWFG_EDGE_COUNT; i < lk_Gl.max_TWFG_edge; i++)
	    {
	      lk_Gl.TWFG_edge[i].to_tran_index = -1;
	      lk_Gl.TWFG_edge[i].next = (i + 1);
	    }
	  lk_Gl.TWFG_edge[lk_Gl.max_TWFG_edge - 1].next = -1;
	  lk_Gl.TWFG_free_edge_idx = LK_MID_TWFG_EDGE_COUNT;
	}
      else
	{
#if defined(RYE_DEBUG)
	  er_log_debug (ARG_FILE_LINE, "So many TWFG edges are used..\n");
#endif /* RYE_DEBUG */
	  return ER_FAILED;	/* no method */
	}
    }

  /* allocate free WFG edge */
  alloc_idx = lk_Gl.TWFG_free_edge_idx;
  lk_Gl.TWFG_free_edge_idx = lk_Gl.TWFG_edge[alloc_idx].next;

  /* set WFG edge with given information */
  lk_Gl.TWFG_edge[alloc_idx].to_tran_index = to_tran_index;
  lk_Gl.TWFG_edge[alloc_idx].edge_seq_num = lk_Gl.global_edge_seq_num;
  lk_Gl.TWFG_edge[alloc_idx].holder_flag = holder_flag;
  lk_Gl.TWFG_edge[alloc_idx].edge_wait_stime = edge_wait_stime;

  /* connect the WFG edge into WFG */
  lk_Gl.TWFG_edge[alloc_idx].next =
    lk_Gl.TWFG_node[from_tran_index].first_edge;
  lk_Gl.TWFG_node[from_tran_index].first_edge = alloc_idx;

  return NO_ERROR;
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_select_deadlock_victim -
 *
 * return:
 *
 *   s(in):
 *   t(in):
 *
 * Note:
 */
static void
lock_select_deadlock_victim (THREAD_ENTRY * thread_p, int s, int t)
{
  LK_WFG_NODE *TWFG_node;
  LK_WFG_EDGE *TWFG_edge;
  TRANID tranid;
  int can_timeout;
  int i, u, v, w, n;
  bool false_dd_cycle = false;
  bool lock_holder_found = false;
  bool inact_trans_found = false;
  int tot_WFG_nodes;
#if defined(RYE_DEBUG)
  int num_WFG_nodes;
  int WFG_nidx;
  int tran_index_area[20];
  int *tran_index_set = &tran_index_area[0];
#endif
  char *cycle_info_string;
  char *ptr;
  int num_tran_in_cycle;
  int unit_size = LOG_USERNAME_MAX + MAXHOSTNAMELEN + PATH_MAX + 10;
  char *client_prog_name, *client_user_name, *client_host_name;
  int client_pid;
  int next_node;
  int *tran_index_in_cycle = NULL;

  assert (false);		/* is impossible */

  /* simple notation */
  TWFG_node = lk_Gl.TWFG_node;
  TWFG_edge = lk_Gl.TWFG_edge;

  /*
   * check if current deadlock cycle is false deadlock cycle
   */
  tot_WFG_nodes = 0;
  if (TWFG_node[t].current == -1)
    {
      /* old WFG edge : remove it */
      TWFG_edge[TWFG_node[s].current].to_tran_index = -2;
      false_dd_cycle = true;
    }
  else
    {
      if (TWFG_node[t].checked_by_deadlock_detector == false
	  || TWFG_node[t].thrd_wait_stime == 0
	  || (TWFG_node[t].thrd_wait_stime >
	      TWFG_edge[TWFG_node[t].current].edge_wait_stime))
	{
	  /* old transaction, not lockwait state, or incorrect WFG edge */
	  /* remove all outgoing edges */
	  TWFG_node[t].first_edge = -1;
	  TWFG_node[t].current = -1;
	  /* remove incoming edge */
	  TWFG_edge[TWFG_node[s].current].to_tran_index = -2;
	  false_dd_cycle = true;
	}
      else
	{
	  if (TWFG_edge[TWFG_node[s].current].edge_seq_num
	      < TWFG_node[t].tran_edge_seq_num)
	    {
	      /* old WFG edge : remove it */
	      TWFG_edge[TWFG_node[s].current].to_tran_index = -2;
	      false_dd_cycle = true;
	    }
	  else
	    {
	      tot_WFG_nodes += 1;
	    }
	}
    }
  for (v = s; v != t;)
    {
      u = lk_Gl.TWFG_node[v].ancestor;
      if (TWFG_node[v].current == -1)
	{
	  /* old WFG edge : remove it */
	  TWFG_edge[TWFG_node[u].current].to_tran_index = -2;
	  false_dd_cycle = true;
	}
      else
	{
	  if (TWFG_node[v].checked_by_deadlock_detector == false
	      || TWFG_node[v].thrd_wait_stime == 0
	      || (TWFG_node[v].thrd_wait_stime >
		  TWFG_edge[TWFG_node[v].current].edge_wait_stime))
	    {
	      /* old transaction, not lockwait state, or incorrect WFG edge */
	      /* remove all outgoing edges */
	      TWFG_node[v].first_edge = -1;
	      TWFG_node[v].current = -1;
	      /* remove incoming edge */
	      TWFG_edge[TWFG_node[u].current].to_tran_index = -2;
	      false_dd_cycle = true;
	    }
	  else
	    {
	      if (TWFG_edge[TWFG_node[u].current].edge_seq_num
		  < TWFG_node[v].tran_edge_seq_num)
		{
		  /* old WFG edge : remove it */
		  TWFG_edge[TWFG_node[u].current].to_tran_index = -2;
		  false_dd_cycle = true;
		}
	      else
		{
		  tot_WFG_nodes += 1;
		}
	    }
	}
      v = u;
    }

  if (false_dd_cycle == true)
    {				/* clear deadlock cycle */
      for (v = s; v != t;)
	{
	  w = TWFG_node[v].ancestor;
	  TWFG_node[v].ancestor = -1;
	  v = w;
	}
      return;
    }

  /*
     Victim Selection Strategy
     1) Must be lock holder.
     2) Must be active transaction.
     3) Prefer a transaction with a closer timeout.
     4) Prefer the youngest transaction.
   */
#if defined(RYE_DEBUG)
  num_WFG_nodes = tot_WFG_nodes;
  if (num_WFG_nodes > 20)
    {
      tran_index_set = (int *) malloc (sizeof (int) * num_WFG_nodes);
      if (tran_index_set == NULL)
	{
	  num_WFG_nodes = 20;
	  tran_index_set = &tran_index_area[0];
	}
    }
  WFG_nidx = 0;

  if (TWFG_node[t].checked_by_deadlock_detector == false)
    {
      er_log_debug (ARG_FILE_LINE,
		    "transaction(index=%d) is old in deadlock cycle\n", t);
    }
#endif /* RYE_DEBUG */
  if (TWFG_edge[TWFG_node[s].current].holder_flag)
    {
      tranid = logtb_find_tranid (t);
      if (logtb_is_active (thread_p, tranid) == false)
	{
	  victims[victim_count].tran_index = NULL_TRAN_INDEX;
	  inact_trans_found = true;
#if defined(RYE_DEBUG)
	  er_log_debug (ARG_FILE_LINE,
			"Inactive transaction is found in a deadlock cycle\n"
			"(tran_index=%d, tranid=%d, state=%s)\n",
			t, tranid, log_state_string (logtb_find_state (t)));
	  tran_index_set[WFG_nidx] = t;
	  WFG_nidx += 1;
#endif /* RYE_DEBUG */
	}
      else
	{
	  victims[victim_count].tran_index = t;
	  victims[victim_count].tranid = tranid;
	  victims[victim_count].can_timeout =
	    LK_CAN_TIMEOUT (logtb_find_wait_msecs (t));
	  lock_holder_found = true;
	}
    }
  else
    {
      victims[victim_count].tran_index = NULL_TRAN_INDEX;
#if defined(RYE_DEBUG)
      tran_index_set[WFG_nidx] = t;
      WFG_nidx += 1;
#endif
    }

  victims[victim_count].tran_index_in_cycle = NULL;
  victims[victim_count].num_trans_in_cycle = 0;

  num_tran_in_cycle = 1;
  for (v = s; v != t; v = TWFG_node[v].ancestor)
    {
      num_tran_in_cycle++;
    }

  cycle_info_string = (char *) malloc (unit_size * num_tran_in_cycle);
  tran_index_in_cycle = (int *) malloc (sizeof (int) * num_tran_in_cycle);

  if (cycle_info_string != NULL && tran_index_in_cycle != NULL)
    {
      int i;

      ptr = cycle_info_string;

      for (i = 0, v = s; i < num_tran_in_cycle;
	   i++, v = TWFG_node[v].ancestor)
	{
	  (void) logtb_find_client_name_host_pid (v,
						  &client_prog_name,
						  &client_user_name,
						  &client_host_name,
						  &client_pid);

	  n = snprintf (ptr, unit_size, "%s%s@%s|%s(%d)",
			((v == s) ? "" : ", "), client_user_name,
			client_host_name, client_prog_name, client_pid);
	  ptr += n;
	  assert_release (ptr <
			  cycle_info_string + unit_size * num_tran_in_cycle);

	  tran_index_in_cycle[i] = v;
	}
    }

  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
	  ER_LK_DEADLOCK_CYCLE_DETECTED, 1,
	  (cycle_info_string) ? cycle_info_string : "");

  if (cycle_info_string != NULL)
    {
      free_and_init (cycle_info_string);
    }

  for (v = s; v != t;)
    {
#if defined(RYE_DEBUG)
      if (TWFG_node[v].checked_by_deadlock_detector == false)
	{
	  er_log_debug (ARG_FILE_LINE,
			"transaction(index=%d) is old in deadlock cycle\n",
			v);
	}
#endif /* RYE_DEBUG */
      if (TWFG_node[v].candidate == true)
	{
	  tranid = logtb_find_tranid (v);
	  if (logtb_is_active (thread_p, tranid) == false)
	    {
	      inact_trans_found = true;
#if defined(RYE_DEBUG)
	      er_log_debug (ARG_FILE_LINE,
			    "Inactive transaction is found in a deadlock cycle\n"
			    "(tran_index=%d, tranid=%d, state=%s)\n",
			    v, tranid,
			    log_state_string (logtb_find_state (v)));
	      tran_index_set[WFG_nidx] = v;
	      WFG_nidx += 1;
#endif /* RYE_DEBUG */
	    }
	  else
	    {
	      lock_holder_found = true;
	      can_timeout = LK_CAN_TIMEOUT (logtb_find_wait_msecs (v));
	      if (victims[victim_count].tran_index == NULL_TRAN_INDEX
		  || (victims[victim_count].can_timeout == false
		      && can_timeout == true)
		  || (victims[victim_count].can_timeout == can_timeout
		      && LK_ISYOUNGER (tranid, victims[victim_count].tranid)))
		{
		  victims[victim_count].tran_index = v;
		  victims[victim_count].tranid = tranid;
		  victims[victim_count].can_timeout = can_timeout;
		}
	    }
	}
#if defined(RYE_DEBUG)
      else
	{			/* TWFG_node[v].candidate == false */
	  tran_index_set[WFG_nidx] = v;
	  WFG_nidx += 1;
	}
#endif
      v = TWFG_node[v].ancestor;
    }

  if (victims[victim_count].tran_index != NULL_TRAN_INDEX)
    {
#if defined(RYE_DEBUG)
      if (TWFG_node[victims[victim_count].tran_index].
	  checked_by_deadlock_detector == false)
	{
	  er_log_debug (ARG_FILE_LINE,
			"victim(index=%d) is old in deadlock cycle\n",
			victims[victim_count].tran_index);
	}
#endif /* RYE_DEBUG */
      TWFG_node[victims[victim_count].tran_index].current = -1;
      victims[victim_count].tran_index_in_cycle = tran_index_in_cycle;
      victims[victim_count].num_trans_in_cycle = num_tran_in_cycle;
      victim_count++;
    }
  else
    {
      /* We can't find active holder.
       * In this case, this cycle is regarded as a false deadlock.
       */
      for (i = 0, v = s; i < num_tran_in_cycle;
	   v = TWFG_node[v].ancestor, i++)
	{
	  assert_release (TWFG_node[v].current >= 0
			  && TWFG_node[v].current < lk_Gl.max_TWFG_edge);

	  next_node = TWFG_edge[TWFG_node[v].current].to_tran_index;

	  if (TWFG_node[next_node].checked_by_deadlock_detector == false
	      || TWFG_node[next_node].thrd_wait_stime == 0
	      || TWFG_node[next_node].thrd_wait_stime >
	      TWFG_edge[TWFG_node[next_node].current].edge_wait_stime)
	    {
	      /* The edge from v to next_node is removed(false edge). */
	      TWFG_node[next_node].first_edge = -1;
	      TWFG_node[next_node].current = -1;
	      TWFG_edge[TWFG_node[v].current].to_tran_index = -2;
	      TWFG_node[v].current = TWFG_edge[TWFG_node[v].current].next;
	      break;
	    }
	}

      if (i == num_tran_in_cycle)
	{
	  /* can't find false edge */
	  TWFG_edge[TWFG_node[s].current].to_tran_index = -2;
	  TWFG_node[s].current = TWFG_edge[TWFG_node[s].current].next;
	}

      if (tran_index_in_cycle != NULL)
	{
	  free_and_init (tran_index_in_cycle);
	}

#if defined(RYE_DEBUG)
      er_log_debug (ARG_FILE_LINE, "No victim in deadlock cycle....\n");
      if (lock_holder_found == false)
	{
	  er_log_debug (ARG_FILE_LINE,
			"Any Lock holder is not found in deadlock cycle.\n");
	}
      if (inact_trans_found == true)
	{
	  er_log_debug (ARG_FILE_LINE,
			"Inactive transactions are found in deadlock cycle.\n");
	}
      er_log_debug (ARG_FILE_LINE,
		    "total_edges=%d, free_edge_idx=%d, global_edge_seq=%d\n",
		    lk_Gl.max_TWFG_edge, lk_Gl.TWFG_free_edge_idx,
		    lk_Gl.global_edge_seq_num);
      er_log_debug (ARG_FILE_LINE,
		    "# of WFG nodes in deadlock cycle = %d (%d printed)\n",
		    tot_WFG_nodes, num_WFG_nodes);
      for (WFG_nidx = 0; WFG_nidx < num_WFG_nodes; WFG_nidx++)
	{
	  er_log_debug (ARG_FILE_LINE, "%3d ", tran_index_set[WFG_nidx]);
	  if ((WFG_nidx + 1) == num_WFG_nodes || (WFG_nidx % 10) == 9)
	    {
	      er_log_debug (ARG_FILE_LINE, "\n");
	    }
	}
#endif /* RYE_DEBUG */
    }

  for (v = s; v != t;)
    {
      w = TWFG_node[v].ancestor;
      TWFG_node[v].ancestor = -1;
      v = w;
    }

#if defined(RYE_DEBUG)
  if (tran_index_set != &tran_index_area[0])
    {
      assert (num_WFG_nodes > 20);
      free_and_init (tran_index_set);
    }
#endif
}
#endif /* SERVER_MODE */

#if defined (ENABLE_UNUSED_FUNCTION)
#if defined(SERVER_MODE)
/*
 * lock_dump_deadlock_victims -
 *
 * return:
 */
static void
lock_dump_deadlock_victims (THREAD_ENTRY * thread_p, FILE * outfile)
{
  int k, count;

  fprintf (outfile, "*** Deadlock Victim Information ***\n");
  fprintf (outfile, "Victim count = %d\n", victim_count);
  /* print aborted transactions (deadlock victims) */
  fprintf (outfile, msgcat_message (MSGCAT_CATALOG_RYE,
				    MSGCAT_SET_LOCK,
				    MSGCAT_LK_DEADLOCK_ABORT_HDR));
  count = 0;
  for (k = 0; k < victim_count; k++)
    {
      if (!victims[k].can_timeout)
	{
	  fprintf (outfile, msgcat_message (MSGCAT_CATALOG_RYE,
					    MSGCAT_SET_LOCK,
					    MSGCAT_LK_DEADLOCK_ABORT),
		   victims[k].tran_index);
	  if ((count % 10) == 9)
	    {
	      fprintf (outfile, msgcat_message (MSGCAT_CATALOG_RYE,
						MSGCAT_SET_LOCK,
						MSGCAT_LK_NEWLINE));
	    }
	  count++;
	}
    }
  fprintf (outfile, msgcat_message (MSGCAT_CATALOG_RYE,
				    MSGCAT_SET_LOCK, MSGCAT_LK_NEWLINE));
  /* print timeout transactions (deadlock victims) */
  fprintf (outfile, msgcat_message (MSGCAT_CATALOG_RYE,
				    MSGCAT_SET_LOCK,
				    MSGCAT_LK_DEADLOCK_TIMEOUT_HDR));
  count = 0;
  for (k = 0; k < victim_count; k++)
    {
      if (victims[k].can_timeout)
	{
	  fprintf (outfile, msgcat_message (MSGCAT_CATALOG_RYE,
					    MSGCAT_SET_LOCK,
					    MSGCAT_LK_DEADLOCK_TIMEOUT),
		   victims[k].tran_index);
	  if ((count % 10) == 9)
	    {
	      fprintf (outfile, msgcat_message (MSGCAT_CATALOG_RYE,
						MSGCAT_SET_LOCK,
						MSGCAT_LK_NEWLINE));
	    }
	  count++;
	}
    }

  xlock_dump (thread_p, outfile);
}
#endif /* SERVER_MODE */
#endif

/*
 *  Private Functions Group: miscellaneous functions
 *
 *   - lk_lockinfo_compare()
 *   - lk_dump_res()
 *   - lk_consistent_res()
 *   - lk_consistent_tran_lock()
 */

#if defined(SERVER_MODE)
/*
 * lock_compare_lock_info -
 *
 * return:
 *
 *   lockinfo1(in):
 *   lockinfo2(in):
 *
 * Note:compare two OID of lockable objects.
 */
static int
lock_compare_lock_info (const void *lockinfo1, const void *lockinfo2)
{
  const OID *oid1;
  const OID *oid2;

  oid1 = &(((LK_LOCKINFO *) (lockinfo1))->oid);
  oid2 = &(((LK_LOCKINFO *) (lockinfo2))->oid);

  return oid_compare (oid1, oid2);
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_wait_msecs_to_secs -
 *
 * return: seconds
 *
 *   msecs(in): milliseconds
 */
static float
lock_wait_msecs_to_secs (int msecs)
{
  if (msecs > 0)
    {
      return (float) msecs / 1000;
    }

  return (float) msecs;
}
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
/*
 * lock_dump_resource - Dump locks acquired on a resource
 *
 * return:
 *
 *   outfp(in): FILE stream where to dump the lock resource entry.
 *   res_ptr(in): pointer to lock resource entry
 *
 * Note:Dump contents of the lock resource entry pointed by res_ptr.
 */
static void
lock_dump_resource (THREAD_ENTRY * thread_p, FILE * outfp, LK_RES * res_ptr)
{
  LK_ENTRY *entry_ptr;
  char *classname;		/* Name of the class */
  int num_holders, num_blocked_holders, num_waiters;
  char time_val[CTIME_MAX];
  int val_type;
  OID *class_oid;
  char tmpbuf[32];

  memset (time_val, 0, sizeof (time_val));

  val_type = DB_VALUE_TYPE (&res_ptr->val);

  /* dump object type related information */
  switch (val_type)
    {
    case DB_TYPE_OID:
      /* dump object identifier */
      class_oid = DB_GET_OID (&res_ptr->val);

      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_RYE,
				      MSGCAT_SET_LOCK, MSGCAT_LK_RES_OID),
	       class_oid->volid, class_oid->pageid, class_oid->slotid);

      classname = heap_get_class_name (thread_p, class_oid);
      if (classname == NULL)
	{
	  /* We must stop processing if an interrupt occurs */
	  if (er_errid () == ER_INTERRUPTED)
	    {
	      return;
	    }
	}
      else
	{
	  fprintf (outfp, msgcat_message (MSGCAT_CATALOG_RYE,
					  MSGCAT_SET_LOCK,
					  MSGCAT_LK_RES_CLASS_TYPE),
		   classname);
	  free_and_init (classname);
	}
      break;

    case DB_TYPE_VARCHAR:
      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_RYE,
				      MSGCAT_SET_LOCK,
				      MSGCAT_LK_RES_VALUE),
	       DB_GET_STRING (&res_ptr->val));
      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_RYE,
				      MSGCAT_SET_LOCK,
				      MSGCAT_LK_RES_VALUE_TYPE), "STRING");
      break;

    case DB_TYPE_INTEGER:
      sprintf (tmpbuf, "%d", DB_GET_INT (&res_ptr->val));
      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_RYE,
				      MSGCAT_SET_LOCK,
				      MSGCAT_LK_RES_VALUE), tmpbuf);

      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_RYE,
				      MSGCAT_SET_LOCK,
				      MSGCAT_LK_RES_VALUE_TYPE), "INTEGER");
      break;

    case DB_TYPE_BIGINT:
      sprintf (tmpbuf, "%ld", DB_GET_BIGINT (&res_ptr->val));
      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_RYE,
				      MSGCAT_SET_LOCK,
				      MSGCAT_LK_RES_VALUE), tmpbuf);

      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_RYE,
				      MSGCAT_SET_LOCK,
				      MSGCAT_LK_RES_VALUE_TYPE), "BIGINT");
      break;

    default:
      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_RYE,
				      MSGCAT_SET_LOCK,
				      MSGCAT_LK_RES_UNKNOWN_TYPE));
    }

  /* dump total modes of holders and waiters */
  fprintf (outfp, msgcat_message (MSGCAT_CATALOG_RYE,
				  MSGCAT_SET_LOCK,
				  MSGCAT_LK_RES_TOTAL_MODE),
	   LOCK_TO_LOCKMODE_STRING (res_ptr->total_holders_mode),
	   LOCK_TO_LOCKMODE_STRING (res_ptr->total_waiters_mode));

  num_holders = num_blocked_holders = 0;
  if (res_ptr->holder != (LK_ENTRY *) NULL)
    {
      entry_ptr = res_ptr->holder;
      while (entry_ptr != NULL)
	{
	  if (entry_ptr->blocked_mode == NULL_LOCK)
	    {
	      num_holders++;
	    }
	  else
	    {
	      num_blocked_holders++;
	    }
	  entry_ptr = entry_ptr->next;
	}
    }
  num_waiters = 0;
  if (res_ptr->waiter != (LK_ENTRY *) NULL)
    {
      entry_ptr = res_ptr->waiter;
      while (entry_ptr != (LK_ENTRY *) NULL)
	{
	  num_waiters++;
	  entry_ptr = entry_ptr->next;
	}
    }

  fprintf (outfp, msgcat_message (MSGCAT_CATALOG_RYE,
				  MSGCAT_SET_LOCK,
				  MSGCAT_LK_RES_LOCK_COUNT), num_holders,
	   num_blocked_holders, num_waiters);

  /* dump holders */
  if (num_holders > 0)
    {
      /* dump non blocked holders */
      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_RYE,
				      MSGCAT_SET_LOCK,
				      MSGCAT_LK_RES_NON_BLOCKED_HOLDER_HEAD));
      entry_ptr = res_ptr->holder;
      while (entry_ptr != (LK_ENTRY *) NULL)
	{
	  if (entry_ptr->blocked_mode == NULL_LOCK)
	    {
	      fprintf (outfp,
		       msgcat_message (MSGCAT_CATALOG_RYE,
				       MSGCAT_SET_LOCK,
				       MSGCAT_LK_RES_NON_BLOCKED_HOLDER_ENTRY),
		       "", entry_ptr->tran_index,
		       LOCK_TO_LOCKMODE_STRING (entry_ptr->granted_mode),
		       entry_ptr->count);
	    }
	  entry_ptr = entry_ptr->next;
	}
    }

  if (num_blocked_holders > 0)
    {
      /* dump blocked holders */
      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_RYE,
				      MSGCAT_SET_LOCK,
				      MSGCAT_LK_RES_BLOCKED_HOLDER_HEAD));
      entry_ptr = res_ptr->holder;
      while (entry_ptr != (LK_ENTRY *) NULL)
	{
	  if (entry_ptr->blocked_mode != NULL_LOCK)
	    {
	      time_t stime =
		(time_t) (entry_ptr->thrd_entry->lockwait_stime / 1000LL);
	      if (ctime_r (&stime, time_val) == NULL)
		{
		  strcpy (time_val, "???");
		}

	      assert (strlen (time_val) > 0);
	      if (time_val[strlen (time_val) - 1] == '\n')
		{
		  time_val[strlen (time_val) - 1] = 0;
		}

	      fprintf (outfp,
		       msgcat_message (MSGCAT_CATALOG_RYE,
				       MSGCAT_SET_LOCK,
				       MSGCAT_LK_RES_BLOCKED_HOLDER_ENTRY),
		       "", entry_ptr->tran_index,
		       LOCK_TO_LOCKMODE_STRING (entry_ptr->granted_mode),
		       entry_ptr->count, "",
		       LOCK_TO_LOCKMODE_STRING (entry_ptr->blocked_mode),
		       "", time_val, "",
		       lock_wait_msecs_to_secs (entry_ptr->thrd_entry->
						lockwait_msecs));
	    }
	  entry_ptr = entry_ptr->next;
	}
    }

  /* dump blocked waiters */
  if (res_ptr->waiter != (LK_ENTRY *) NULL)
    {
      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_RYE,
				      MSGCAT_SET_LOCK,
				      MSGCAT_LK_RES_BLOCKED_WAITER_HEAD));
      entry_ptr = res_ptr->waiter;
      while (entry_ptr != (LK_ENTRY *) NULL)
	{
	  time_t stime =
	    (time_t) (entry_ptr->thrd_entry->lockwait_stime / 1000LL);
	  (void) ctime_r (&stime, time_val);
	  if (time_val[strlen (time_val) - 1] == '\n')
	    {
	      time_val[strlen (time_val) - 1] = 0;
	    }
	  fprintf (outfp, msgcat_message (MSGCAT_CATALOG_RYE,
					  MSGCAT_SET_LOCK,
					  MSGCAT_LK_RES_BLOCKED_WAITER_ENTRY),
		   "", entry_ptr->tran_index,
		   LOCK_TO_LOCKMODE_STRING (entry_ptr->blocked_mode), "",
		   time_val, "",
		   lock_wait_msecs_to_secs (entry_ptr->thrd_entry->
					    lockwait_msecs));
	  entry_ptr = entry_ptr->next;
	}
    }

  fprintf (outfp, msgcat_message (MSGCAT_CATALOG_RYE,
				  MSGCAT_SET_LOCK, MSGCAT_LK_NEWLINE));

}
#endif /* SERVER_MODE */

/*
 * lock_initialize - Initialize the lock manager
 *
 * return: error code
 *
 *   estimate_nobj_locks(in): estimate_nobj_locks(useless)
 *
 * Note:Initialize the lock manager memory structures.
 */
int
lock_initialize (void)
{
#if !defined (SERVER_MODE)
  lk_Standalone_has_xlock = false;

  DB_MAKE_STRING (&system_Ddl_key, "__SYSTEM_DDL_KEY__");

  DB_MAKE_STRING (&shard_Catalog_key, "__SHARD_CATALOG_KEY__");
  DB_MAKE_STRING (&shard_Global_key, "__SHARD_GLOBAL_KEY__");

  return NO_ERROR;
#else /* !SERVER_MODE */
  const char *env_value;
  int error_code = NO_ERROR;

  error_code = lock_initialize_tran_lock_table ();
  if (error_code != NO_ERROR)
    {
      goto error;
    }
  error_code = lock_initialize_object_hash_table ();
  if (error_code != NO_ERROR)
    {
      goto error;
    }
  error_code = lock_initialize_object_lock_res_list ();
  if (error_code != NO_ERROR)
    {
      goto error;
    }
  error_code = lock_initialize_object_lock_entry_list ();
  if (error_code != NO_ERROR)
    {
      goto error;
    }
  error_code = lock_initialize_deadlock_detection ();
  if (error_code != NO_ERROR)
    {
      goto error;
    }

  /* initialize some parameters */
#if defined(RYE_DEBUG)
  lk_Gl.verbose_mode = true;
  lk_Gl.no_victim_case_count = 0;
#else /* !RYE_DEBUG */
  env_value = envvar_get ("LK_VERBOSE_SUSPENDED");
  if (env_value != NULL)
    {
      lk_Gl.verbose_mode = (bool) atoi (env_value);
      if (lk_Gl.verbose_mode != false)
	{
	  lk_Gl.verbose_mode = true;
	}
    }
  lk_Gl.no_victim_case_count = 0;
#endif /* !RYE_DEBUG */

#if defined(LK_DUMP)
  lk_Gl.dump_level = 0;
  env_value = envvar_get ("LK_DUMP_LEVEL");
  if (env_value != NULL)
    {
      lk_Gl.dump_level = atoi (env_value);
      if (lk_Gl.dump_level < 0 || lk_Gl.dump_level > 3)
	{
	  lk_Gl.dump_level = 0;
	}
    }
#endif /* LK_DUMP */

  DB_MAKE_STRING (&system_Ddl_key, "__SYSTEM_DDL_KEY__");

  DB_MAKE_STRING (&shard_Catalog_key, "__SHARD_CATALOG_KEY__");
  DB_MAKE_STRING (&shard_Global_key, "__SHARD_GLOBAL_KEY__");

  return error_code;

error:
  (void) lock_finalize ();
  return error_code;
#endif /* !SERVER_MODE */
}

/*
 * lock_finalize - Finalize the lock manager
 *
 * return: nothing
 *
 * Note:This function finalize the lock manager.
 *     Memory structures of the lock manager are deallocated.
 */
void
lock_finalize (void)
{
#if !defined (SERVER_MODE)
  lk_Standalone_has_xlock = false;
#else /* !SERVER_MODE */
  LK_RES_BLOCK *res_block;	/* pointer to lock resource table node */
  LK_ENTRY_BLOCK *entry_block;	/* pointer to lock entry block */
  LK_TRAN_LOCK *tran_lock;
  LK_HASH *hash_anchor;
  int i;

  /* Release all the locks and awake all transactions */
  /* TODO: Why ? */
  /* transaction deadlock information table */
  /* deallocate memory space for the transaction deadlock info. */
  if (lk_Gl.TWFG_node != (LK_WFG_NODE *) NULL)
    {
      free_and_init (lk_Gl.TWFG_node);
    }

  /* transaction lock information table */
  /* deallocate memory space for transaction lock table */
  if (lk_Gl.tran_lock_table != (LK_TRAN_LOCK *) NULL)
    {
      for (i = 0; i < lk_Gl.num_trans; i++)
	{
	  tran_lock = &lk_Gl.tran_lock_table[i];
	  pthread_mutex_destroy (&tran_lock->hold_mutex);
	}
      free_and_init (lk_Gl.tran_lock_table);
    }
  /* reset the number of transactions */
  lk_Gl.num_trans = 0;

  /* object lock entry list */
  /* deallocate memory space for object lock entry block list */
  while (lk_Gl.obj_entry_block_list != (LK_ENTRY_BLOCK *) NULL)
    {
      entry_block = lk_Gl.obj_entry_block_list;
      lk_Gl.obj_entry_block_list = entry_block->next_block;
      if (entry_block->block != (LK_ENTRY *) NULL)
	{
	  free_and_init (entry_block->block);
	}
      free_and_init (entry_block);
    }
  pthread_mutex_destroy (&lk_Gl.obj_res_block_list_mutex);
  pthread_mutex_destroy (&lk_Gl.obj_free_res_list_mutex);
  /* reset object lock entry block list */
  lk_Gl.obj_free_entry_list = (LK_ENTRY *) NULL;

  /* object lock resource list */
  /* deallocate memory space for object lock resource table node list */
  while (lk_Gl.obj_res_block_list != (LK_RES_BLOCK *) NULL)
    {
      res_block = lk_Gl.obj_res_block_list;
      lk_Gl.obj_res_block_list = res_block->next_block;
      if (res_block->block != (LK_RES *) NULL)
	{
	  for (i = 0; i < res_block->count; i++)
	    {
	      pthread_mutex_destroy (&res_block->block[i].res_mutex);
	    }
	  free_and_init (res_block->block);
	}
      free_and_init (res_block);
    }
  pthread_mutex_destroy (&lk_Gl.obj_entry_block_list_mutex);
  pthread_mutex_destroy (&lk_Gl.obj_free_entry_list_mutex);
  pthread_mutex_destroy (&lk_Gl.DL_detection_mutex);
  /* reset object lock resource free list */
  lk_Gl.obj_free_res_list = (LK_RES *) NULL;

  /* object lock hash table */
  /* deallocate memory space for object lock hash table */
  if (lk_Gl.obj_hash_table != (LK_HASH *) NULL)
    {
      for (i = 0; i < lk_Gl.obj_hash_size; i++)
	{
	  hash_anchor = &lk_Gl.obj_hash_table[i];
	  pthread_mutex_destroy (&hash_anchor->hash_mutex);
	}
      free_and_init (lk_Gl.obj_hash_table);
    }
  /* reset max number of object locks */
  lk_Gl.obj_hash_size = 0;
  lk_Gl.max_obj_locks = 0;
#endif /* !SERVER_MODE */
}

/*
 * lock_object - Lock an object with NULL btid
 *
 * return: one of following values)
 *     LK_GRANTED
 *     LK_NOTGRANTED_DUE_ABORTED
 *     LK_NOTGRANTED_DUE_TIMEOUT
 *     LK_NOTGRANTED_DUE_ERROR
 *
 *   oid(in): Identifier of object(instance, class, root class) to lock
 *   class_oid(in): Identifier of the class instance of the given object
 *   lock(in): Requested lock mode
 *   cond_flag(in):
 *
 */
int
lock_object (UNUSED_ARG THREAD_ENTRY * thread_p,
	     UNUSED_ARG const DB_VALUE * val, LOCK lock,
	     UNUSED_ARG int cond_flag)
{
#if !defined (SERVER_MODE)
  LK_SET_STANDALONE_XLOCK (lock);
  return LK_GRANTED;
#else /* !SERVER_MODE */
  int tran_index;
  int wait_msecs;
  int granted;
  LK_ENTRY *inst_entry = NULL;

  if (val == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_BAD_ARGUMENT, 2,
	      "lock_object", "NULL OID pointer");
      return LK_NOTGRANTED_DUE_ERROR;
    }


  if (lock == NULL_LOCK)
    {
      return LK_GRANTED;
    }

  tran_index = logtb_get_current_tran_index (thread_p);
  if (cond_flag == LK_COND_LOCK)	/* conditional request */
    {
      wait_msecs = LK_FORCE_ZERO_WAIT;
    }
  else
    {
      wait_msecs = logtb_find_wait_msecs (tran_index);
    }

  granted =
    lock_internal_perform_lock_object (thread_p, tran_index, val,
				       lock, wait_msecs, &inst_entry);

  return granted;
#endif /* !SERVER_MODE */
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * lock_object_wait_msecs - Lock an object
 *
 * return: one of following values)
 *     LK_GRANTED
 *     LK_NOTGRANTED_DUE_ABORTED
 *     LK_NOTGRANTED_DUE_TIMEOUT
 *     LK_NOTGRANTED_DUE_ERROR
 *
 *   oid(in): Identifier of object(instance, class, root class) to lock
 *   class_oid(in): Identifier of the class instance of the given object
 *   lock(in): Requested lock mode
 *   cond_flag(in):
 *   wait_msecs(in):
 *
 */
int
lock_object_wait_msecs (THREAD_ENTRY * thread_p, const DB_VALUE * val,
			LOCK lock, int cond_flag, int wait_msecs)
{
#if !defined (SERVER_MODE)
  LK_SET_STANDALONE_XLOCK (lock);
  return LK_GRANTED;
#else /* !SERVER_MODE */
  int old_wait_msecs = xlogtb_reset_wait_msecs (thread_p, wait_msecs);
  int lock_result = lock_object (thread_p, val, lock, cond_flag);

  xlogtb_reset_wait_msecs (thread_p, old_wait_msecs);

  return lock_result;
#endif
}
#endif

/*
 * lock_objects_lock_set - Lock many objects
 *
 * return: one of following values
 *     LK_GRANTED
 *     LK_NOTGRANTED_DUE_ABORTED
 *     LK_NOTGRANTED_DUE_TIMEOUT
 *     LK_NOTGRANTED_DUE_ERROR
 *
 *   lockset(in/out): Request the lock of many objects
 *
 */
int
lock_objects_lock_set (UNUSED_ARG THREAD_ENTRY * thread_p,
		       LC_LOCKSET * lockset)
{
#if !defined (SERVER_MODE)
  LK_SET_STANDALONE_XLOCK (lockset->reqobj_class_lock);
  return LK_GRANTED;
#else /* !SERVER_MODE */
  int tran_index;
  int wait_msecs;
  TRAN_ISOLATION isolation;
  LK_LOCKINFO *cls_lockinfo = NULL;
  LK_LOCKINFO *ins_lockinfo = NULL;
  LC_LOCKSET_REQOBJ *reqobjects;	/* Description of one instance to
					 * lock
					 */
  LC_LOCKSET_CLASSOF *reqclasses;	/* Description of one class of a
					 * requested object to lock
					 */
  LOCK reqobj_class_lock;
  int cls_count;
  int ins_count;
  int granted, i;
  LK_ENTRY *class_entry = NULL;
  DB_VALUE lock_val;

  if (lockset == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_BAD_ARGUMENT, 2,
	      "lock_objects_lock_set", "NULL lockset pointer");
      return LK_NOTGRANTED_DUE_ERROR;
    }

  tran_index = logtb_get_current_tran_index (thread_p);
  wait_msecs = logtb_find_wait_msecs (tran_index);
  isolation = logtb_find_isolation (tran_index);

  /* We do not want to rollback the transaction in the event of a deadlock.
   * For now, let's just wait a long time. If deadlock, the transaction is
   * going to be notified of lock timeout instead of aborted.
   */
  if (lockset->quit_on_errors == false && wait_msecs == LK_INFINITE_WAIT)
    {
      wait_msecs = INT_MAX;	/* will be notified of lock timeout */
    }

  /* prepare cls_lockinfo and ins_lockinfo array */
  cls_lockinfo =
    (LK_LOCKINFO *) malloc (SIZEOF_LK_LOCKINFO * lockset->num_reqobjs);
  if (cls_lockinfo == (LK_LOCKINFO *) NULL)
    {
      return LK_NOTGRANTED_DUE_ERROR;
    }

  ins_lockinfo =
    (LK_LOCKINFO *) malloc (SIZEOF_LK_LOCKINFO * lockset->num_reqobjs);
  if (ins_lockinfo == (LK_LOCKINFO *) NULL)
    {
      free_and_init (cls_lockinfo);
      return LK_NOTGRANTED_DUE_ERROR;
    }

  reqobjects = lockset->objects;
  reqclasses = lockset->classes;

  reqobj_class_lock = lockset->reqobj_class_lock;

  /* build cls_lockinfo and ins_lockinfo array */
  cls_count = ins_count = 0;

  for (i = 0; i < lockset->num_reqobjs; i++)
    {
      if (OID_ISNULL (&reqobjects[i].oid)
	  || reqobjects[i].class_index < 0
	  || reqobjects[i].class_index > lockset->num_classes_of_reqobjs)
	{
	  continue;
	}

      if (OID_IS_ROOTOID (&reqclasses[reqobjects[i].class_index].oid))
	{
	  /* requested object: class => build cls_lockinfo[cls_count] */
	  COPY_OID (&cls_lockinfo[cls_count].oid, &reqobjects[i].oid);
	  cls_lockinfo[cls_count].org_oidp = &reqobjects[i].oid;
	  cls_lockinfo[cls_count].lock = reqobj_class_lock;
	  /* increment cls_count */
	  cls_count++;
	}
#if 0
      else
	{
#if 1				/* TODO - */
	  assert (false);	/* is impossible */
#endif
	  /* requested object: instance => build cls_lockinfo[cls_count] */
	  COPY_OID (&ins_lockinfo[ins_count].oid, &reqobjects[i].oid);
	  COPY_OID (&ins_lockinfo[ins_count].class_oid,
		    &reqclasses[reqobjects[i].class_index].oid);
	  ins_lockinfo[ins_count].org_oidp = &reqobjects[i].oid;
	  ins_lockinfo[ins_count].lock = X_LOCK;	/* TODO - */
	  /* increment ins_count */
	  ins_count++;
	}
#endif
    }

  if (cls_count > 0)
    {
      /* sort the cls_lockinfo to avoid deadlock */
      if (cls_count > 1)
	{
	  (void) qsort (cls_lockinfo, cls_count, SIZEOF_LK_LOCKINFO,
			lock_compare_lock_info);
	}

      for (i = 0; i < cls_count; i++)
	{
	  /* hold the locks on the given class objects */
	  DB_MAKE_OID (&lock_val, &cls_lockinfo[i].oid);
	  granted = lock_internal_perform_lock_object (thread_p,
						       tran_index,
						       &lock_val,
						       cls_lockinfo[i].lock,
						       wait_msecs,
						       &class_entry);
	  if (granted != LK_GRANTED)
	    {
	      if (lockset->quit_on_errors == false
		  && granted == LK_NOTGRANTED_DUE_TIMEOUT)
		{
		  OID_SET_NULL (cls_lockinfo[i].org_oidp);
		  continue;
		}
	      goto error;
	    }
	}
    }

  /* release memory space for cls_lockinfo and ins_lockinfo */
  free_and_init (cls_lockinfo);
  free_and_init (ins_lockinfo);

  return LK_GRANTED;

error:
  free_and_init (cls_lockinfo);
  free_and_init (ins_lockinfo);

  return granted;
#endif /* !SERVER_MODE */
}

/*
 * lock_classes_lock_hint - Lock many classes that has been hinted
 *
 * return: one of following values
 *     LK_GRANTED
 *     LK_NOTGRANTED_DUE_ABORTED
 *     LK_NOTGRANTED_DUE_TIMEOUT
 *     LK_NOTGRANTED_DUE_ERROR
 *
 *   lockhint(in): description of hinted classes
 *
 */
int
lock_classes_lock_hint (UNUSED_ARG THREAD_ENTRY * thread_p,
			LC_LOCKHINT * lockhint)
{
#if !defined (SERVER_MODE)
  int i;

  for (i = 0; i < lockhint->num_classes; i++)
    {
      if (lockhint->classes[i].lock == X_LOCK)
	{
	  lk_Standalone_has_xlock = true;
	  break;
	}
    }
  return LK_GRANTED;
#else /* !SERVER_MODE */
  int tran_index;
  int wait_msecs;
  TRAN_ISOLATION isolation;
  LK_LOCKINFO *cls_lockinfo = NULL;
  LK_ENTRY *class_entry = NULL;
  int cls_count;
  int granted, i;
  DB_VALUE lock_val;

  if (lockhint == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_BAD_ARGUMENT, 2,
	      "lock_classes_lock_hint", "NULL lockhint pointer");
      return LK_NOTGRANTED_DUE_ERROR;
    }

  /* If there is nothing to lock, returns */
  if (lockhint->num_classes <= 0)
    {
      return LK_GRANTED;
    }

  tran_index = logtb_get_current_tran_index (thread_p);
  wait_msecs = logtb_find_wait_msecs (tran_index);
  isolation = logtb_find_isolation (tran_index);

  /* We do not want to rollback the transaction in the event of a deadlock.
   * For now, let's just wait a long time. If deadlock, the transaction is
   * going to be notified of lock timeout instead of aborted.
   */
  if (lockhint->quit_on_errors == false && wait_msecs == LK_INFINITE_WAIT)
    {
      wait_msecs = INT_MAX;	/* to be notified of lock timeout  */
    }

  /* prepare cls_lockinfo array */
  cls_lockinfo =
    (LK_LOCKINFO *) malloc (SIZEOF_LK_LOCKINFO * lockhint->num_classes);
  if (cls_lockinfo == (LK_LOCKINFO *) NULL)
    {
      return LK_NOTGRANTED_DUE_ERROR;
    }

  /* Define the desired locks for all classes */
  /* get class_oids and class_locks */
  cls_count = 0;
  for (i = 0; i < lockhint->num_classes; i++)
    {
      if (OID_ISNULL (&lockhint->classes[i].oid)
	  || lockhint->classes[i].lock == NULL_LOCK)
	{
	  continue;
	}

      /* build cls_lockinfo[cls_count] */
      COPY_OID (&cls_lockinfo[cls_count].oid, &lockhint->classes[i].oid);
      cls_lockinfo[cls_count].org_oidp = &lockhint->classes[i].oid;
      cls_lockinfo[cls_count].lock = lockhint->classes[i].lock;
      cls_count++;
    }

  /* sort class oids before hold the locks in order to avoid deadlocks */
  if (cls_count > 1)
    {
      (void) qsort (cls_lockinfo, cls_count, SIZEOF_LK_LOCKINFO,
		    lock_compare_lock_info);
    }

  for (i = 0; i < cls_count; i++)
    {
      /* hold the lock on the given class. */
      DB_MAKE_OID (&lock_val, &cls_lockinfo[i].oid);
      granted = lock_internal_perform_lock_object (thread_p, tran_index,
						   &lock_val,
						   cls_lockinfo[i].lock,
						   wait_msecs, &class_entry);

      if (granted != LK_GRANTED)
	{
	  if (lockhint->quit_on_errors == false
	      && granted == LK_NOTGRANTED_DUE_TIMEOUT)
	    {
	      OID_SET_NULL (cls_lockinfo[i].org_oidp);
	      continue;
	    }
	  goto error;
	}
    }

  /* release memory space for cls_lockinfo */
  free_and_init (cls_lockinfo);

  return LK_GRANTED;

error:
  free_and_init (cls_lockinfo);

  return granted;
#endif /* !SERVER_MODE */
}

/*
 * lock_unlock_object - Unlock an object according to transaction isolation level
 *
 * return: nothing..
 *
 *   val(in): Identifier of instance to lock
 *   unlock_type(in):
 *
 */
void
lock_unlock_object (UNUSED_ARG THREAD_ENTRY * thread_p,
		    UNUSED_ARG const DB_VALUE * val,
		    UNUSED_ARG LK_UNLOCK_TYPE unlock_type)
{
#if !defined (SERVER_MODE)
  return;
#else /* !SERVER_MODE */
  int tran_index;		/* transaction table index */
  LK_ENTRY *entry_ptr;
  int release_flag;

  if (val == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LK_BAD_ARGUMENT, 2,
	      "lk_unlock_object", "NULL OID pointer");
      return;
    }

  /* get transaction table index */
  tran_index = logtb_get_current_tran_index (thread_p);

  entry_ptr = lock_find_tran_hold_entry (tran_index, val);

  if (entry_ptr != NULL)
    {
      release_flag = false;
      if (unlock_type == LK_UNLOCK_TYPE_FORCE)
	{
	  release_flag = true;
	}

      lock_internal_perform_unlock_object (thread_p, entry_ptr, release_flag);
    }

#endif /* !SERVER_MODE */
}

/*
 * lock_unlock_all - Release all locks of current transaction
 *
 * return: nothing
 *
 * Note:Release all locks acquired by the current transaction.
 *
 *      This function must be called at the end of the transaction.
 */
void
lock_unlock_all (THREAD_ENTRY * thread_p)
{
#if !defined (SERVER_MODE)
  lk_Standalone_has_xlock = false;
  pgbuf_unfix_all (thread_p);

  return;
#else /* !SERVER_MODE */
  int tran_index;
  LK_TRAN_LOCK *tran_lock;
  LK_ENTRY *entry_ptr;

  tran_index = logtb_get_current_tran_index (thread_p);
  tran_lock = &lk_Gl.tran_lock_table[tran_index];

  /* remove all instance locks */
  entry_ptr = tran_lock->inst_hold_list;
  while (entry_ptr != (LK_ENTRY *) NULL)
    {
      lock_internal_perform_unlock_object (thread_p, entry_ptr, true);
      entry_ptr = tran_lock->inst_hold_list;
    }

  lock_clear_deadlock_victim (tran_index);

  pgbuf_unfix_all (thread_p);
#endif /* !SERVER_MODE */
}

#if defined (SERVER_MODE)
static LK_ENTRY *
lock_find_tran_hold_entry (UNUSED_ARG int tran_index,
			   UNUSED_ARG const DB_VALUE * val)
{
  unsigned int hash_index;
  LK_HASH *hash_anchor;
  LK_RES *res_ptr;
  LK_ENTRY *entry_ptr;
  int rv;

  hash_index = mht_get_hash_number (lk_Gl.obj_hash_size, val);
  hash_anchor = &lk_Gl.obj_hash_table[hash_index];

  rv = pthread_mutex_lock (&hash_anchor->hash_mutex);
  res_ptr = hash_anchor->hash_next;
  for (; res_ptr != NULL; res_ptr = res_ptr->hash_next)
    {
      if (db_value_compare (&res_ptr->val, val) == DB_EQ)
	{
	  break;
	}
    }

  if (res_ptr == NULL)
    {
      pthread_mutex_unlock (&hash_anchor->hash_mutex);
      return NULL;
    }

  rv = pthread_mutex_lock (&res_ptr->res_mutex);
  pthread_mutex_unlock (&hash_anchor->hash_mutex);

  entry_ptr = res_ptr->holder;
  for (; entry_ptr != NULL; entry_ptr = entry_ptr->next)
    {
      if (entry_ptr->tran_index == tran_index)
	{
	  break;
	}
    }

  pthread_mutex_unlock (&res_ptr->res_mutex);
  return entry_ptr;
}
#endif

/*
 * lock_has_xlock - Does transaction have an exclusive lock on any resource ?
 *
 * return:
 *
 * Note:Find if the current transaction has any kind of exclusive lock
 *     on any lock resource.
 */
bool
lock_has_xlock (UNUSED_ARG THREAD_ENTRY * thread_p)
{
#if !defined (SERVER_MODE)
  return lk_Standalone_has_xlock;
#else /* !SERVER_MODE */
  int tran_index;
  LK_TRAN_LOCK *tran_lock;
  int rv;

  tran_index = logtb_get_current_tran_index (thread_p);
  tran_lock = &lk_Gl.tran_lock_table[tran_index];
  rv = pthread_mutex_lock (&tran_lock->hold_mutex);

  /* TODO: check shard key lock */

  pthread_mutex_unlock (&tran_lock->hold_mutex);

  return false;
#endif /* !SERVER_MODE */
}

/*
 * lock_get_current_lock -
 *
 * return: lock
 *
 *   oid_val(in):
 *
 */
LOCK
lock_get_current_lock (UNUSED_ARG THREAD_ENTRY * thread_p,
		       UNUSED_ARG DB_VALUE * val)
{
#if !defined (SERVER_MODE)
  return NULL_LOCK;
#else
  LK_TRAN_LOCK *tran_lock;
  LK_ENTRY *lock_entry;
  int rv;
  int tran_index;
  LOCK lock = NULL_LOCK;

  /* The caller is holding a resource mutex */
  tran_index = logtb_get_current_tran_index (thread_p);

  tran_lock = &lk_Gl.tran_lock_table[tran_index];
  rv = pthread_mutex_lock (&tran_lock->hold_mutex);

  lock_entry = tran_lock->inst_hold_list;
  while (lock_entry != NULL)
    {
      if (DB_VALUE_TYPE (&lock_entry->res_head->val) == DB_VALUE_TYPE (val)
	  && db_value_compare (&lock_entry->res_head->val, val) == DB_EQ)
	{
	  lock = lock_entry->granted_mode;
	  break;
	}
      lock_entry = lock_entry->tran_next;
    }

  pthread_mutex_unlock (&tran_lock->hold_mutex);

  return lock;
#endif
}

/*
 * lock_force_timeout_lock_wait_transactions - All lock-wait transactions
 *                               are forced to timeout
 *
 * return: nothing
 *
 * Note:All lock-waiting transacions are forced to timeout.
 *     For this task, all lock-waiting threads are searched and
 *     then the threads are forced to timeout.
 */
void
lock_force_timeout_lock_wait_transactions (UNUSED_ARG unsigned short
					   stop_phase)
{
#if !defined (SERVER_MODE)
  return;
#else /* !SERVER_MODE */
  int i;
  THREAD_ENTRY *thrd;
  CSS_CONN_ENTRY *conn_p;

  for (i = 1; i < thread_num_total_threads (); i++)
    {
      thrd = thread_find_entry_by_index (i);

      conn_p = thrd->conn_entry;
      if ((stop_phase == THREAD_STOP_LOGWR && conn_p == NULL)
	  || (conn_p && conn_p->stop_phase != stop_phase))
	{
	  continue;
	}

      (void) thread_lock_entry (thrd);
      if (LK_IS_LOCKWAIT_THREAD (thrd))
	{
	  /* wake up the thread */
	  lock_resume ((LK_ENTRY *) thrd->lockwait, LOCK_RESUMED_TIMEOUT);
	}
      else
	{
	  if (thrd->lockwait != NULL
	      || thrd->lockwait_state == (int) LOCK_SUSPENDED)
	    {
	      /* some strange lock wait state.. */
	      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		      ER_LK_STRANGE_LOCK_WAIT, 5, thrd->lockwait,
		      thrd->lockwait_state, thrd->index, thrd->tid,
		      thrd->tran_index);
	    }
	  /* release the thread entry mutex */
	  (void) thread_unlock_entry (thrd);
	}
    }
  return;
#endif /* !SERVER_MODE */
}

/*
 * lock_force_timeout_expired_wait_transactions - Transaction is timeout if its waiting time has
 *                           expired or it is interrupted
 *
 * return: true if the thread was timed out or
 *                       false if the thread was not timed out.
 *
 *   thrd_entry(in): thread entry pointer
 *
 * Note:If the given thread is waiting on a lock to be granted, and
 *     either its expiration time has expired or it is interrupted,
 *     the thread is timed-out.
 *     If NULL is given, it applies to all threads.
 */
bool
lock_force_timeout_expired_wait_transactions (UNUSED_ARG void *thrd_entry)
{
#if !defined (SERVER_MODE)
  return true;
#else /* !SERVER_MODE */
  int i;
  bool ignore;
  THREAD_ENTRY *thrd;

  if (thrd_entry != NULL)
    {
      thrd = (THREAD_ENTRY *) thrd_entry;
      (void) thread_lock_entry (thrd);
      if (LK_IS_LOCKWAIT_THREAD (thrd))
	{
	  struct timeval tv;
	  INT64 etime;
	  (void) gettimeofday (&tv, NULL);
	  etime = (tv.tv_sec * 1000000LL + tv.tv_usec) / 1000LL;
	  if (LK_CAN_TIMEOUT (thrd->lockwait_msecs)
	      && etime - thrd->lockwait_stime > thrd->lockwait_msecs)
	    {
	      /* wake up the thread */
	      lock_resume ((LK_ENTRY *) thrd->lockwait, LOCK_RESUMED_TIMEOUT);
	      return true;
	    }
	  else if (logtb_is_interrupted_tran (NULL, true, &ignore,
					      thrd->tran_index))
	    {
	      /* wake up the thread */
	      lock_resume ((LK_ENTRY *) thrd->lockwait,
			   LOCK_RESUMED_INTERRUPT);
	      return true;
	    }
	  else
	    {
	      /* release the thread entry mutex */
	      (void) thread_unlock_entry (thrd);
	      return false;
	    }
	}
      else
	{
	  if (thrd->lockwait != NULL
	      || thrd->lockwait_state == (int) LOCK_SUSPENDED)
	    {
	      /* some strange lock wait state.. */
	      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		      ER_LK_STRANGE_LOCK_WAIT, 5, thrd->lockwait,
		      thrd->lockwait_state, thrd->index, thrd->tid,
		      thrd->tran_index);
	    }
	  /* release the thread entry mutex */
	  (void) thread_unlock_entry (thrd);
	  return false;
	}
    }
  else
    {
      for (i = 1; i < thread_num_total_threads (); i++)
	{
	  thrd = thread_find_entry_by_index (i);
	  (void) thread_lock_entry (thrd);
	  if (LK_IS_LOCKWAIT_THREAD (thrd))
	    {
	      struct timeval tv;
	      INT64 etime;
	      (void) gettimeofday (&tv, NULL);
	      etime = (tv.tv_sec * 1000000LL + tv.tv_usec) / 1000LL;
	      if ((LK_CAN_TIMEOUT (thrd->lockwait_msecs)
		   && etime - thrd->lockwait_stime > thrd->lockwait_msecs)
		  || logtb_is_interrupted_tran (NULL, true, &ignore,
						thrd->tran_index))
		{
		  /* wake up the thread */
		  lock_resume ((LK_ENTRY *) thrd->lockwait,
			       LOCK_RESUMED_TIMEOUT);
		}
	      else
		{
		  /* release the thread entry mutex */
		  (void) thread_unlock_entry (thrd);
		}
	    }
	  else
	    {
	      if (thrd->lockwait != NULL
		  || thrd->lockwait_state == (int) LOCK_SUSPENDED)
		{
		  /* some strange lock wait state.. */
		  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
			  ER_LK_STRANGE_LOCK_WAIT, 5, thrd->lockwait,
			  thrd->lockwait_state, thrd->index, thrd->tid,
			  thrd->tran_index);
		}
	      /* release the thread entry mutex */
	      (void) thread_unlock_entry (thrd);
	    }
	}
      return true;
    }
#endif /* !SERVER_MODE */
}

/*
 * lock_check_local_deadlock_detection - Check local deadlock detection interval
 *
 * return:
 *
 * Note:check if the local deadlock detection should be performed.
 */
bool
lock_check_local_deadlock_detection (void)
{
#if !defined (SERVER_MODE)
  return false;
#else /* !SERVER_MODE */
  struct timeval now, elapsed;
  double elapsed_sec;

  /* check deadlock detection interval */
  gettimeofday (&now, NULL);
  DIFF_TIMEVAL (lk_Gl.last_deadlock_run, now, elapsed);
  /* add 0.01 for the processing time by deadlock detection */
  elapsed_sec = elapsed.tv_sec + (elapsed.tv_usec / 1000) + 0.01;
  if (elapsed_sec <= prm_get_float_value (PRM_ID_LK_RUN_DEADLOCK_INTERVAL))
    {
      return false;
    }
  else
    {
      return true;
    }
#endif /* !SERVER_MODE */
}

/*
 * lock_detect_local_deadlock - Run the local deadlock detection
 *
 * return: nothing
 *
 * Note:Run the deadlock detection. For every cycle either timeout or
 *     abort a transaction. The timeout option is always preferred over
 *     the unilaterally abort option. When the unilaterally abort option
 *     is exercised, the youngest transaction in the cycle is selected.
 *     The youngest transaction is hopefully the one that has done less work.
 *
 *     First, allocate heaps for WFG table from local memory.
 *     Check whether deadlock(s) have been occurred or not.
 *
 *     Deadlock detection is peformed via exhaustive loop construction
 *     which indicates the wait-for-relationship.
 *     If deadlock is detected,
 *     the first transaction which enables a cycle
 *     when scanning from the first of object lock table to the last of it.
 *
 *     The deadlock of victims are waken up and aborted by themselves.
 *
 *     Last, free WFG framework.
 */
void
lock_detect_local_deadlock (UNUSED_ARG THREAD_ENTRY * thread_p)
{
#if !defined (SERVER_MODE)
  return;
#else /* !SERVER_MODE */
  int k, s, t;
  LK_RES_BLOCK *res_block;
  LK_RES *res_ptr;
  LK_ENTRY *hi, *hj;
  LK_WFG_NODE *TWFG_node;
  LK_WFG_EDGE *TWFG_edge;
  int i, rv;
  int compat1, compat2;
#if defined (ENABLE_UNUSED_FUNCTION)
  FILE *log_fp;
#endif

  /* initialize deadlock detection related structures */

  /* initialize transaction WFG node table..
   * The current transaction might be old deadlock victim.
   * And, the transaction may have not been aborted, until now.
   * Even if the transaction(old deadlock victim) has not been aborted,
   * set checked_by_deadlock_detector of the transaction to true.
   */
  for (i = 1; i < lk_Gl.num_trans; i++)
    {
      lk_Gl.TWFG_node[i].first_edge = -1;
      lk_Gl.TWFG_node[i].tran_edge_seq_num = 0;
      lk_Gl.TWFG_node[i].checked_by_deadlock_detector = true;
    }

  /* initialize transaction WFG edge table */
  lk_Gl.TWFG_edge = &TWFG_edge_block[0];
  lk_Gl.max_TWFG_edge = LK_MIN_TWFG_EDGE_COUNT;	/* initial value */
  for (i = 0; i < LK_MIN_TWFG_EDGE_COUNT; i++)
    {
      lk_Gl.TWFG_edge[i].to_tran_index = -1;
      lk_Gl.TWFG_edge[i].next = (i + 1);
    }
  lk_Gl.TWFG_edge[lk_Gl.max_TWFG_edge - 1].next = -1;
  lk_Gl.TWFG_free_edge_idx = 0;

  /* initialize global_edge_seq_num */
  lk_Gl.global_edge_seq_num = 0;

  /* initialize victim count */
  victim_count = 0;		/* used as index of victims array */

  /* hold the deadlock detection mutex */
  rv = pthread_mutex_lock (&lk_Gl.DL_detection_mutex);

  /* lock-wait edge construction for object lock resource table */
  rv = pthread_mutex_lock (&lk_Gl.obj_res_block_list_mutex);

  for (res_block = lk_Gl.obj_res_block_list;
       res_block != NULL; res_block = res_block->next_block)
    {
      for (k = 0; k < res_block->count; k++)
	{
	  /* If the holder list is empty, then the waiter list is also empty.
	   * Therefore, res_block->block[k].waiter == NULL cannot be checked.
	   */
	  if (res_block->block[k].holder == (LK_ENTRY *) NULL)
	    {
	      if (res_block->block[k].waiter == (LK_ENTRY *) NULL)
		{
		  continue;
		}
	    }

	  res_ptr = &res_block->block[k];

	  /* hold resource mutex */
	  rv = pthread_mutex_lock (&res_ptr->res_mutex);

	  if (res_ptr->holder == NULL)
	    {
	      if (res_ptr->waiter == NULL)
		{
		  pthread_mutex_unlock (&res_ptr->res_mutex);
		  continue;
		}
	      else
		{
#if defined(RYE_DEBUG)
		  FILE *lk_fp;
		  time_t cur_time;
		  char time_val[CTIME_MAX];

		  lk_fp = fopen ("lock_waiter_only_info.log", "a");
		  if (lk_fp != NULL)
		    {
		      cur_time = time (NULL);
		      (void) ctime_r (&cur_time, time_val);
		      fprintf (lk_fp,
			       "##########################################\n");
		      fprintf (lk_fp, "# current time: %s\n", time_val);
		      lock_dump_resource (lk_fp, res_ptr);
		      fprintf (lk_fp,
			       "##########################################\n");
		      fclose (lk_fp);
		    }
#endif /* RYE_DEBUG */
		  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
			  ER_LK_LOCK_WAITER_ONLY, 1,
			  "lock_waiter_only_info.log");

		  if (res_ptr->total_holders_mode != NULL_LOCK)
		    {
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_LK_TOTAL_HOLDERS_MODE, 1,
			      res_ptr->total_holders_mode);
		      res_ptr->total_holders_mode = NULL_LOCK;
		    }
		  (void) lock_grant_blocked_waiter (thread_p, res_ptr);
		}
	    }

	  /* among holders */
	  for (hi = res_ptr->holder; hi != NULL; hi = hi->next)
	    {
	      if (hi->blocked_mode == NULL_LOCK)
		{
		  break;
		}
	      for (hj = hi->next; hj != NULL; hj = hj->next)
		{
		  assert (hi->granted_mode >= NULL_LOCK
			  && hi->blocked_mode >= NULL_LOCK);
		  assert (hj->granted_mode >= NULL_LOCK
			  && hj->blocked_mode >= NULL_LOCK);

		  compat1 = lock_Comp[hj->blocked_mode][hi->granted_mode];
		  compat2 = lock_Comp[hj->blocked_mode][hi->blocked_mode];
		  assert (compat1 != DB_NA && compat2 != DB_NA);

		  if (compat1 == false || compat2 == false)
		    {
		      (void) lock_add_WFG_edge (hj->tran_index,
						hi->tran_index, true,
						hj->thrd_entry->
						lockwait_stime);
		    }

		  compat1 = lock_Comp[hi->blocked_mode][hj->granted_mode];
		  assert (compat1 != DB_NA);

		  if (compat1 == false)
		    {
		      (void) lock_add_WFG_edge (hi->tran_index,
						hj->tran_index, true,
						hi->thrd_entry->
						lockwait_stime);
		    }
		}
	    }

	  /* from waiters in the waiter to holders */
	  for (hi = res_ptr->holder; hi != NULL; hi = hi->next)
	    {
	      for (hj = res_ptr->waiter; hj != NULL; hj = hj->next)
		{
		  assert (hi->granted_mode >= NULL_LOCK
			  && hi->blocked_mode >= NULL_LOCK);
		  assert (hj->granted_mode >= NULL_LOCK
			  && hj->blocked_mode >= NULL_LOCK);

		  compat1 = lock_Comp[hj->blocked_mode][hi->granted_mode];
		  compat2 = lock_Comp[hj->blocked_mode][hi->blocked_mode];
		  assert (compat1 != DB_NA && compat2 != DB_NA);

		  if (compat1 == false || compat2 == false)
		    {
		      (void) lock_add_WFG_edge (hj->tran_index,
						hi->tran_index, true,
						hj->thrd_entry->
						lockwait_stime);
		    }
		}
	    }

	  /* from waiters in the waiter to other waiters in the waiter */
	  for (hi = res_ptr->waiter; hi != NULL; hi = hi->next)
	    {
	      for (hj = hi->next; hj != NULL; hj = hj->next)
		{
		  assert (hj->blocked_mode >= NULL_LOCK
			  && hi->blocked_mode >= NULL_LOCK);

		  compat1 = lock_Comp[hj->blocked_mode][hi->blocked_mode];
		  assert (compat1 != DB_NA);

		  if (compat1 == false)
		    {
		      (void) lock_add_WFG_edge (hj->tran_index,
						hi->tran_index, false,
						hj->thrd_entry->
						lockwait_stime);
		    }
		}
	    }

	  /* release resource mutex */
	  pthread_mutex_unlock (&res_ptr->res_mutex);
	}
    }

  pthread_mutex_unlock (&lk_Gl.obj_res_block_list_mutex);

  /* release DL detection mutex */
  pthread_mutex_unlock (&lk_Gl.DL_detection_mutex);

  /* simple notation for using in the following statements */
  TWFG_node = lk_Gl.TWFG_node;
  TWFG_edge = lk_Gl.TWFG_edge;

  /*
   * deadlock detection and victim selection
   */

  for (k = 1; k < lk_Gl.num_trans; k++)
    {
      TWFG_node[k].current = TWFG_node[k].first_edge;
      TWFG_node[k].ancestor = -1;
    }
  for (k = 1; k < lk_Gl.num_trans; k++)
    {
      if (TWFG_node[k].current == -1)
	{
	  continue;
	}
      s = k;
      TWFG_node[s].ancestor = -2;
      for (; s != -2;)
	{
	  if (TWFG_node[s].checked_by_deadlock_detector == false
	      || TWFG_node[s].thrd_wait_stime == 0
	      || (TWFG_node[s].current != -1
		  && (TWFG_node[s].thrd_wait_stime >
		      TWFG_edge[TWFG_node[s].current].edge_wait_stime)))
	    {
	      /* A new transaction started */
	      TWFG_node[s].first_edge = -1;
	      TWFG_node[s].current = -1;
	    }

	  if (TWFG_node[s].current == -1)
	    {
	      t = TWFG_node[s].ancestor;
	      TWFG_node[s].ancestor = -1;
	      s = t;
	      if (s != -2 && TWFG_node[s].current != -1)
		{
		  assert_release (TWFG_node[s].current >= 0
				  && TWFG_node[s].current <
				  lk_Gl.max_TWFG_edge);
		  TWFG_node[s].current = TWFG_edge[TWFG_node[s].current].next;
		}
	      continue;
	    }

	  assert_release (TWFG_node[s].current >= 0
			  && TWFG_node[s].current < lk_Gl.max_TWFG_edge);

	  t = TWFG_edge[TWFG_node[s].current].to_tran_index;

	  if (t == -2)
	    {			/* old WFG edge */
	      TWFG_node[s].current = TWFG_edge[TWFG_node[s].current].next;
	      continue;
	    }

	  if (TWFG_node[t].current == -1)
	    {
	      TWFG_edge[TWFG_node[s].current].to_tran_index = -2;
	      TWFG_node[s].current = TWFG_edge[TWFG_node[s].current].next;
	      continue;
	    }

	  if (TWFG_node[t].checked_by_deadlock_detector == false
	      || TWFG_node[t].thrd_wait_stime == 0
	      || TWFG_node[t].thrd_wait_stime >
	      TWFG_edge[TWFG_node[t].current].edge_wait_stime)
	    {
	      TWFG_node[t].first_edge = -1;
	      TWFG_node[t].current = -1;
	      TWFG_edge[TWFG_node[s].current].to_tran_index = -2;
	      TWFG_node[s].current = TWFG_edge[TWFG_node[s].current].next;
	      continue;
	    }

	  if (TWFG_edge[TWFG_node[s].current].edge_seq_num <
	      TWFG_node[t].tran_edge_seq_num)
	    {			/* old WFG edge */
	      TWFG_edge[TWFG_node[s].current].to_tran_index = -2;
	      TWFG_node[s].current = TWFG_edge[TWFG_node[s].current].next;
	      continue;
	    }

	  if (TWFG_node[t].ancestor != -1)
	    {
	      /* A deadlock cycle is found */
	      assert (false);	/* is impossible */
	      lock_select_deadlock_victim (thread_p, s, t);
	      if (victim_count >= LK_MAX_VICTIM_COUNT)
		{
		  goto final;
		}
	    }
	  else
	    {
	      TWFG_node[t].ancestor = s;
	      TWFG_node[t].candidate =
		TWFG_edge[TWFG_node[s].current].holder_flag;
	    }
	  s = t;
	}
    }

final:

  assert (victim_count == 0);

#if defined (ENABLE_UNUSED_FUNCTION)
  if (victim_count > 0)
    {
      size_t size_loc;
      char *ptr;
      FILE *fp = port_open_memstream (&ptr, &size_loc);

      if (fp)
	{
	  lock_dump_deadlock_victims (thread_p, fp);
	  port_close_memstream (fp, &ptr, &size_loc);

	  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
		  ER_LK_DEADLOCK_SPECIFIC_INFO, 1, ptr);

	  if (ptr != NULL)
	    {
	      free (ptr);
	    }
	}
    }

  /* dump deadlock cycle to event log file */
  for (k = 0; k < victim_count; k++)
    {
      if (victims[k].tran_index_in_cycle == NULL)
	{
	  continue;
	}

      log_fp = event_log_start (thread_p, "DEADLOCK");
      if (log_fp != NULL)
	{
	  for (i = 0; i < victims[k].num_trans_in_cycle; i++)
	    {
	      tran_index = victims[k].tran_index_in_cycle[i];
	      event_log_print_client_info (tran_index, 0);
	      lock_event_log_tran_locks (thread_p, log_fp, tran_index);
	    }

	  event_log_end (thread_p);
	}

      free_and_init (victims[k].tran_index_in_cycle);
    }

  /* Now solve the deadlocks (cycles) by executing the cycle resolution
   * function (e.g., aborting victim)
   */
  for (k = 0; k < victim_count; k++)
    {
      if (victims[k].can_timeout)
	{
	  (void) lock_wakeup_deadlock_victim_timeout (victims[k].tran_index);
	}
      else
	{
	  (void) lock_wakeup_deadlock_victim_aborted (victims[k].tran_index);
	}
    }
#endif

  /* deallocate memory space used for deadlock detection */
  if (lk_Gl.max_TWFG_edge > LK_MID_TWFG_EDGE_COUNT)
    {
      free_and_init (lk_Gl.TWFG_edge);
    }

  if (victim_count == 0)
    {
      if (lk_Gl.no_victim_case_count < 60)
	{
	  lk_Gl.no_victim_case_count += 1;
	}
      else
	{
	  int worker_threads = 0;
	  int suspended_threads = 0;
	  int thrd_index;
	  THREAD_ENTRY *thrd_ptr;

	  /* Make sure that we have threads available for another client
	   * to execute, otherwise Panic...
	   */
	  thread_get_info_threads (NULL, &worker_threads, NULL,
				   &suspended_threads);
	  if (worker_threads == suspended_threads)
	    {
	      /* We must timeout at least one thread, so other clients can execute,
	       * otherwise, the server will hang.
	       */
	      thrd_ptr = thread_find_first_lockwait_entry (&thrd_index);
	      while (thrd_ptr != NULL)
		{
		  if (lock_wakeup_deadlock_victim_timeout
		      (thrd_ptr->tran_index) == true)
		    {
		      break;
		    }
		  thrd_ptr = thread_find_next_lockwait_entry (&thrd_index);
		}

	      if (thrd_ptr != NULL)
		{
		  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
			  ER_LK_NOTENOUGH_ACTIVE_THREADS, 3, worker_threads,
			  logtb_get_number_assigned_tran_indices (),
			  thrd_ptr->tran_index);
		}
	    }
	  lk_Gl.no_victim_case_count = 0;
	}
    }

  /* save the last deadlock run time */
  gettimeofday (&lk_Gl.last_deadlock_run, NULL);

  return;
#endif /* !SERVER_MODE */
}

/*
 * xlock_dump - Dump the contents of lock table
 *
 * return: nothing
 *
 *   outfp(in): FILE stream where to dump the lock table. If NULL is given,
 *            it is dumped to stdout.
 *
 * Note:Dump the lock and waiting tables for both objects and pages.
 *              That is, the lock activity of the datbase. It may be useful
 *              for finding concurrency problems and locking bottlenecks on
 *              an application, so that you can set the appropiate isolation
 *              level or modify the design of the application.
 */
void
xlock_dump (UNUSED_ARG THREAD_ENTRY * thread_p, UNUSED_ARG FILE * outfp)
{
#if !defined (SERVER_MODE)
  return;
#else /* !SERVER_MODE */
  char *client_prog_name;	/* Client program name for tran */
  char *client_user_name;	/* Client user name for tran    */
  char *client_host_name;	/* Client host for tran         */
  int client_pid;		/* Client process id for tran   */
  TRAN_ISOLATION isolation;	/* Isolation for client tran    */
  TRAN_STATE state;
  int wait_msecs;
  int old_wait_msecs = 0;	/* Old transaction lock wait    */
  int tran_index;
  int num_res;
  LK_RES_BLOCK *res_block;
  int hash_index;
  LK_HASH *hash_anchor;
  LK_RES *res_ptr;
  LK_RES *res_prev;
  int rv;
  float lock_timeout_sec;
  char lock_timeout_string[64];

  if (outfp == NULL)
    {
      outfp = stdout;
    }

  fprintf (outfp, msgcat_message (MSGCAT_CATALOG_RYE,
				  MSGCAT_SET_LOCK, MSGCAT_LK_NEWLINE));
  fprintf (outfp, msgcat_message (MSGCAT_CATALOG_RYE,
				  MSGCAT_SET_LOCK,
				  MSGCAT_LK_DUMP_LOCK_TABLE),
	   prm_get_float_value (PRM_ID_LK_RUN_DEADLOCK_INTERVAL));

  /* Don't get block from anything when dumping object lock table. */
  old_wait_msecs = xlogtb_reset_wait_msecs (thread_p, LK_FORCE_ZERO_WAIT);

  /* Dump some information about all transactions */
  fprintf (outfp, msgcat_message (MSGCAT_CATALOG_RYE,
				  MSGCAT_SET_LOCK, MSGCAT_LK_NEWLINE));
  for (tran_index = 0; tran_index < lk_Gl.num_trans; tran_index++)
    {
      if (logtb_find_client_name_host_pid (tran_index, &client_prog_name,
					   &client_user_name,
					   &client_host_name,
					   &client_pid) != NO_ERROR)
	{
	  /* Likely this index is not assigned */
	  continue;
	}
      isolation = logtb_find_isolation (tran_index);
      state = logtb_find_state (tran_index);
      wait_msecs = logtb_find_wait_msecs (tran_index);
      lock_timeout_sec = lock_wait_msecs_to_secs (wait_msecs);

      if (lock_timeout_sec > 0)
	{
	  sprintf (lock_timeout_string, ": %.2f", lock_timeout_sec);
	}
      else if ((int) lock_timeout_sec == LK_ZERO_WAIT
	       || (int) lock_timeout_sec == LK_FORCE_ZERO_WAIT)
	{
	  sprintf (lock_timeout_string, ": No wait");
	}
      else if ((int) lock_timeout_sec == LK_INFINITE_WAIT)
	{
	  sprintf (lock_timeout_string, ": Infinite wait");
	}
      else
	{
	  assert_release (0);
	  sprintf (lock_timeout_string, ": %d", (int) lock_timeout_sec);
	}

      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_RYE,
				      MSGCAT_SET_LOCK,
				      MSGCAT_LK_DUMP_TRAN_IDENTIFIERS),
	       tran_index, client_prog_name, client_user_name,
	       client_host_name, client_pid);
      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_RYE,
				      MSGCAT_SET_LOCK,
				      MSGCAT_LK_DUMP_TRAN_ISOLATION),
	       log_isolation_string (isolation));
      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_RYE,
				      MSGCAT_SET_LOCK,
				      MSGCAT_LK_DUMP_TRAN_STATE),
	       log_state_string (state));
      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_RYE,
				      MSGCAT_SET_LOCK,
				      MSGCAT_LK_DUMP_TRAN_TIMEOUT_PERIOD),
	       lock_timeout_string);
      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_RYE,
				      MSGCAT_SET_LOCK, MSGCAT_LK_NEWLINE));
    }

  /* dump object lock table */
  num_res = 0;
  rv = pthread_mutex_lock (&lk_Gl.obj_res_block_list_mutex);
  res_block = lk_Gl.obj_res_block_list;
  while (res_block != (LK_RES_BLOCK *) NULL)
    {
      num_res += res_block->count;
      res_block = res_block->next_block;
    }
  pthread_mutex_unlock (&lk_Gl.obj_res_block_list_mutex);

  fprintf (outfp, "Object Lock Table:\n");
  fprintf (outfp, "\tCurrent number of objects which are locked    = %d\n",
	   lk_Gl.num_obj_res_allocated);
  fprintf (outfp,
	   "\tMaximum number of objects which can be locked = %d\n\n",
	   num_res);

  for (hash_index = 0; hash_index < lk_Gl.obj_hash_size; hash_index++)
    {
      hash_anchor = &lk_Gl.obj_hash_table[hash_index];
      rv = pthread_mutex_lock (&hash_anchor->hash_mutex);
      res_ptr = hash_anchor->hash_next;
      res_prev = NULL;
      while (res_ptr != (LK_RES *) NULL)
	{
	  rv = pthread_mutex_lock (&res_ptr->res_mutex);

	  if (res_ptr->holder == NULL && res_ptr->waiter == NULL)
	    {
	      if (res_prev == (LK_RES *) NULL)
		{
		  hash_anchor->hash_next = res_ptr->hash_next;
		}
	      else
		{
		  res_prev->hash_next = res_ptr->hash_next;
		}

	      pthread_mutex_unlock (&res_ptr->res_mutex);
	      lock_free_resource (thread_p, res_ptr);

	      if (res_prev == (LK_RES *) NULL)
		{
		  res_ptr = hash_anchor->hash_next;
		  continue;
		}
	      else
		{
		  res_ptr = res_prev->hash_next;
		  continue;
		}
	    }

	  lock_dump_resource (thread_p, outfp, res_ptr);

	  pthread_mutex_unlock (&res_ptr->res_mutex);
	  res_prev = res_ptr;
	  res_ptr = res_ptr->hash_next;
	}
      pthread_mutex_unlock (&hash_anchor->hash_mutex);
    }

  /* Reset the wait back to the way it was */
  (void) xlogtb_reset_wait_msecs (thread_p, old_wait_msecs);

  return;
#endif /* !SERVER_MODE */
}

/*
 * lock_get_number_object_locks - Number of object lock entries
 *
 * return:
 *
 * Note:Find the number of total object lock entries of all
 *              transactions
 */
unsigned int
lock_get_number_object_locks (void)
{
#if defined(SA_MODE)
  return 0;
#else
  return lk_Gl.num_obj_res_allocated;
#endif
}

/* lock_clear_deadlock_victim:
 *
 * tran_index(in):
 */
void
lock_clear_deadlock_victim (UNUSED_ARG int tran_index)
{
#if !defined (SERVER_MODE)
  return;
#else /* !SERVER_MODE */
  int rv;

  /* communication with deadlock detector */
  if (lk_Gl.TWFG_node[tran_index].checked_by_deadlock_detector)
    {
      lk_Gl.TWFG_node[tran_index].checked_by_deadlock_detector = false;
    }
  if (lk_Gl.TWFG_node[tran_index].DL_victim)
    {
      rv = pthread_mutex_lock (&lk_Gl.DL_detection_mutex);
      lk_Gl.TWFG_node[tran_index].DL_victim = false;
      pthread_mutex_unlock (&lk_Gl.DL_detection_mutex);
    }
#endif /* !SERVER_MODE */
}

/*
 * lock_get_lock_holder_tran_index -
 *
 * return:
 *  out_buf(out):
 *  waiter_index(in):
 *  res (in):
 *
 *  note : caller must free *out_buf.
 */
int
lock_get_lock_holder_tran_index (UNUSED_ARG THREAD_ENTRY * thread_p,
				 char **out_buf, UNUSED_ARG int waiter_index,
				 LK_RES * res)
{
#if !defined (SERVER_MODE)
  if (res == NULL)
    {
      return NO_ERROR;
    }

  if (out_buf == NULL)
    {
      assert_release (0);
      return ER_FAILED;
    }

  *out_buf = NULL;

  return NO_ERROR;

#else

#define HOLDER_ENTRY_LENGTH (12)
  int rv;
  LK_ENTRY *holder, *waiter;
  int holder_number = 0;
  int buf_size, n, remained_size;
  bool is_valid = false;	/* validation check */
  char *buf, *p;

  if (res == NULL)
    {
      return NO_ERROR;
    }

  if (out_buf == NULL)
    {
      assert_release (0);
      return ER_FAILED;
    }

  *out_buf = NULL;

  rv = pthread_mutex_lock (&res->res_mutex);
  if (rv != 0)
    {
      return ER_FAILED;
    }

  if (DB_IS_NULL (&res->val))
    {
      pthread_mutex_unlock (&res->res_mutex);
      return NO_ERROR;
    }

  waiter = res->waiter;
  while (waiter != NULL)
    {
      if (waiter->tran_index == waiter_index)
	{
	  is_valid = true;
	  break;
	}
      waiter = waiter->next;
    }

  if (is_valid == false)
    {
      holder = res->holder;
      while (holder != NULL)
	{
	  if (holder->blocked_mode != NULL_LOCK
	      && holder->tran_index == waiter_index)
	    {
	      is_valid = true;
	      break;
	    }
	  holder = holder->next;
	}
    }

  if (is_valid == false)
    {
      /* not a valid waiter of this resource */
      pthread_mutex_unlock (&res->res_mutex);
      return NO_ERROR;
    }

  holder = res->holder;
  while (holder != NULL)
    {
      if (holder->tran_index != waiter_index)
	{
	  holder_number++;
	}
      holder = holder->next;
    }

  if (holder_number == 0)
    {
      pthread_mutex_unlock (&res->res_mutex);
      return NO_ERROR;
    }

  buf_size = holder_number * HOLDER_ENTRY_LENGTH + 1;
  buf = (char *) malloc (sizeof (char) * buf_size);

  if (buf == NULL)
    {
      pthread_mutex_unlock (&res->res_mutex);

      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, buf_size);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  remained_size = buf_size;
  p = buf;

  /* write first holder index */
  holder = res->holder;
  while (holder && holder->tran_index == waiter_index)
    {
      holder = holder->next;
    }

  assert_release (holder != NULL);

  n = snprintf (p, remained_size, "%d", holder->tran_index);
  remained_size -= n;
  p += n;
  assert_release (remained_size >= 0);

  /* write remained holder index */
  holder = holder->next;
  while (holder != NULL)
    {
      if (holder->tran_index != waiter_index)
	{
	  n = snprintf (p, remained_size, ", %d", holder->tran_index);
	  remained_size -= n;
	  p += n;
	  assert_release (remained_size >= 0);
	}
      holder = holder->next;
    }

  *out_buf = buf;

  pthread_mutex_unlock (&res->res_mutex);

  return NO_ERROR;
#endif
}

/*
 * lock dump to event log file (lock timeout, deadlock)
 */

#if defined(SERVER_MODE)

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * lock_event_log_tran_locks - dump transaction locks to event log file
 *   return:
 *   thread_p(in):
 *   log_fp(in):
 *   tran_index(in):
 *
 *   note: for deadlock
 */
static void
lock_event_log_tran_locks (THREAD_ENTRY * thread_p, FILE * log_fp,
			   int tran_index)
{
  int rv, i, indent = 2;
  LK_TRAN_LOCK *tran_lock;
  LK_ENTRY *entry;

  assert (csect_check_own (thread_p, CSECT_EVENT_LOG_FILE) == 1);

  fprintf (log_fp, "hold:\n");

  tran_lock = &lk_Gl.tran_lock_table[tran_index];
  rv = pthread_mutex_lock (&tran_lock->hold_mutex);

  entry = tran_lock->inst_hold_list;
  for (i = 0; entry != NULL && i < MAX_NUM_LOCKS_DUMP_TO_EVENT_LOG;
       entry = entry->tran_next, i++)
    {
      fprintf (log_fp, "%*clock: %s", indent, ' ',
	       LOCK_TO_LOCKMODE_STRING (entry->granted_mode));

      lock_event_log_lock_info (thread_p, log_fp, entry);

      event_log_sql_string (thread_p, log_fp, &entry->xasl_id, indent);
      event_log_bind_values (log_fp, tran_index, entry->bind_index_in_tran);

      fprintf (log_fp, "\n");
    }

  if (entry != NULL)
    {
      fprintf (log_fp, "%*c...\n", indent, ' ');
    }

  entry = tran_lock->waiting;
  if (entry != NULL)
    {
      fprintf (log_fp, "wait:\n");
      fprintf (log_fp, "%*clock: %s", indent, ' ',
	       LOCK_TO_LOCKMODE_STRING (entry->blocked_mode));

      lock_event_log_lock_info (thread_p, log_fp, entry);

      event_log_sql_string (thread_p, log_fp, &entry->xasl_id, indent);
      event_log_bind_values (log_fp, tran_index, entry->bind_index_in_tran);

      fprintf (log_fp, "\n");
    }

  pthread_mutex_unlock (&tran_lock->hold_mutex);
}
#endif

/*
 * lock_event_log_blocked_lock - dump lock waiter info to event log file
 *   return:
 *   thread_p(in):
 *   log_fp(in):
 *   entry(in):
 *
 *   note: for lock timeout
 */
static void
lock_event_log_blocked_lock (THREAD_ENTRY * thread_p, FILE * log_fp,
			     LK_ENTRY * entry)
{
  int indent = 2;

  assert (csect_check_own (thread_p, CSECT_EVENT_LOG_FILE) == 1);

  fprintf (log_fp, "waiter:\n");
  event_log_print_client_info (entry->tran_index, indent);

  fprintf (log_fp, "%*clock: %s", indent, ' ',
	   LOCK_TO_LOCKMODE_STRING (entry->blocked_mode));
  lock_event_log_lock_info (thread_p, log_fp, entry);

  event_log_sql_string (thread_p, log_fp, &entry->xasl_id, indent);
  event_log_bind_values (log_fp, entry->tran_index,
			 entry->bind_index_in_tran);
  fprintf (log_fp, "\n");
}

/*
 * lock_event_log_blocking_locks - dump lock blocker info to event log file
 *   return:
 *   thread_p(in):
 *   log_fp(in):
 *   wait_entry(in):
 *
 *   note: for lock timeout
 */
static void
lock_event_log_blocking_locks (THREAD_ENTRY * thread_p, FILE * log_fp,
			       LK_ENTRY * wait_entry)
{
  LK_ENTRY *entry;
  LK_RES *res_ptr = NULL;
  int compat1, compat2, rv, indent = 2;

  assert (csect_check_own (thread_p, CSECT_EVENT_LOG_FILE) == 1);

  res_ptr = wait_entry->res_head;
  rv = pthread_mutex_lock (&res_ptr->res_mutex);

  fprintf (log_fp, "blocker:\n");

  for (entry = res_ptr->holder; entry != NULL; entry = entry->next)
    {
      if (entry == wait_entry)
	{
	  continue;
	}

      compat1 = lock_Comp[entry->granted_mode][wait_entry->blocked_mode];
      compat2 = lock_Comp[entry->blocked_mode][wait_entry->blocked_mode];

      if (compat1 == false || compat2 == false)
	{
	  event_log_print_client_info (entry->tran_index, indent);

	  fprintf (log_fp, "%*clock: %s", indent, ' ',
		   LOCK_TO_LOCKMODE_STRING (entry->granted_mode));
	  lock_event_log_lock_info (thread_p, log_fp, entry);

	  event_log_sql_string (thread_p, log_fp, &entry->xasl_id, indent);
	  event_log_bind_values (log_fp, entry->tran_index,
				 entry->bind_index_in_tran);
	  fprintf (log_fp, "\n");
	}
    }

  for (entry = res_ptr->waiter; entry != NULL; entry = entry->next)
    {
      if (entry == wait_entry)
	{
	  continue;
	}

      compat1 = lock_Comp[entry->blocked_mode][wait_entry->blocked_mode];

      if (compat1 == false)
	{
	  event_log_print_client_info (entry->tran_index, indent);

	  fprintf (log_fp, "%*clock: %s", indent, ' ',
		   LOCK_TO_LOCKMODE_STRING (entry->granted_mode));
	  lock_event_log_lock_info (thread_p, log_fp, entry);

	  event_log_sql_string (thread_p, log_fp, &entry->xasl_id, indent);
	  event_log_bind_values (log_fp, entry->tran_index,
				 entry->bind_index_in_tran);
	  fprintf (log_fp, "\n");
	}
    }

  pthread_mutex_unlock (&res_ptr->res_mutex);
}

/*
 * lock_event_log_lock_info - dump lock resource info to event log file
 *   return:
 *   thread_p(in):
 *   log_fp(in):
 *   entry(in):
 */
static void
lock_event_log_lock_info (THREAD_ENTRY * thread_p, FILE * log_fp,
			  LK_ENTRY * entry)
{
  LK_RES *res_ptr;
  char *classname;
  OID *class_oid;

  assert (csect_check_own (thread_p, CSECT_EVENT_LOG_FILE) == 1);

  res_ptr = entry->res_head;

  switch (DB_VALUE_TYPE (&res_ptr->val))
    {
    case DB_TYPE_OID:
      class_oid = DB_GET_OID (&res_ptr->val);
      fprintf (log_fp, " (oid=%d|%d|%d", class_oid->volid,
	       class_oid->pageid, class_oid->slotid);

      classname = heap_get_class_name (thread_p, class_oid);
      if (classname != NULL)
	{
	  fprintf (log_fp, ", table=%s", classname);
	  free_and_init (classname);
	}
      break;

    case DB_TYPE_VARCHAR:
      fprintf (log_fp, " (value=%s", DB_GET_STRING (&res_ptr->val));
      break;

    case DB_TYPE_INTEGER:
      fprintf (log_fp, " (value=%d", DB_GET_INT (&res_ptr->val));
      break;

    case DB_TYPE_BIGINT:
      fprintf (log_fp, " (value=%ld", DB_GET_BIGINT (&res_ptr->val));
      break;
    default:
      break;
    }

  fprintf (log_fp, ")\n");
}

/*
 * lock_event_set_tran_wait_entry - save the lock entry tran is waiting
 *   return:
 *   entry(in):
 */
static void
lock_event_set_tran_wait_entry (int tran_index, LK_ENTRY * entry)
{
  LK_TRAN_LOCK *tran_lock;
  int rv;

  tran_lock = &lk_Gl.tran_lock_table[tran_index];
  rv = pthread_mutex_lock (&tran_lock->hold_mutex);

  tran_lock->waiting = entry;

  if (entry != NULL)
    {
      lock_event_set_xasl_id_to_entry (tran_index, entry);
    }

  pthread_mutex_unlock (&tran_lock->hold_mutex);
}

/*
 * lock_event_set_xasl_id_to_entry - save the xasl id related lock entry
 *   return:
 *   entry(in):
 */
static void
lock_event_set_xasl_id_to_entry (int tran_index, LK_ENTRY * entry)
{
  LOG_TDES *tdes;

  tdes = LOG_FIND_TDES (tran_index);
  if (tdes != NULL && !XASL_ID_IS_NULL (&tdes->xasl_id))
    {
      if (tdes->num_exec_queries <= MAX_NUM_EXEC_QUERY_HISTORY)
	{
	  entry->bind_index_in_tran = tdes->num_exec_queries - 1;
	}
      else
	{
	  entry->bind_index_in_tran = -1;
	}

      XASL_ID_COPY (&entry->xasl_id, &tdes->xasl_id);
    }
  else
    {
      XASL_ID_SET_NULL (&entry->xasl_id);
      entry->bind_index_in_tran = -1;
    }
}
#endif /* SERVER_MODE */

OID table_Oid_gid_skey =
  { NULL_PAGEID, NULL_SLOTID, NULL_VOLID, NULL_GROUPID };
OID table_Oid_applier =
  { NULL_PAGEID, NULL_SLOTID, NULL_VOLID, NULL_GROUPID };
/*
 * lock_shard_key_lock () -
 *    return: error coce
 *
 *    shrad_groupid(in):
 *    req_shard_key(in):
 *    class_oid(in):
 *    is_shard_table(in):
 *    is_catalog_table(in):
 *    for_update(in):
 */
int
lock_shard_key_lock (THREAD_ENTRY * thread_p, int shard_groupid,
		     DB_VALUE * req_shard_key, OID * class_oid,
		     bool is_shard_table, bool is_catalog_table,
		     bool for_update)
{
  DB_VALUE *shard_key;
  LOG_TDES *tdes;
  int error = NO_ERROR;
  char key1_buf[256], key2_buf[256];
  LC_FIND_CLASSNAME status;

  tdes = logtb_get_current_tdes (thread_p);
  if (tdes == NULL)
    {
      assert (false);
      return ER_FAILED;
    }

  /* defense code; check iff valid shard_groupid for DML */
  if (shard_groupid == NULL_GROUPID)
    {
      assert (false);
      error = ER_SHARD_INVALID_GROUPID;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, shard_groupid);
      return error;
    }

  if (shard_groupid == GLOBAL_GROUPID && class_oid != NULL)
    {
      if (OID_ISNULL (&table_Oid_gid_skey))
	{
	  status = xlocator_find_class_oid (thread_p,
					    CT_SHARD_GID_SKEY_INFO_NAME,
					    &table_Oid_gid_skey, NULL_LOCK);
	  if (status == LC_CLASSNAME_ERROR || status == LC_CLASSNAME_DELETED)
	    {
	      error = ER_LC_UNKNOWN_CLASSNAME;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
		      CT_SHARD_GID_SKEY_INFO_NAME);

	      return error;
	    }

	}
      if (OID_ISNULL (&table_Oid_applier))
	{
	  status = xlocator_find_class_oid (thread_p,
					    CT_LOG_APPLIER_NAME,
					    &table_Oid_applier, NULL_LOCK);
	  if (status == LC_CLASSNAME_ERROR || status == LC_CLASSNAME_DELETED)
	    {
	      error = ER_LC_UNKNOWN_CLASSNAME;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
		      CT_SHARD_GID_SKEY_INFO_NAME);

	      return error;
	    }
	}

      if (OID_EQ (class_oid, &table_Oid_gid_skey)
	  || OID_EQ (class_oid, &table_Oid_applier))
	{
	  /* This tables do not need lock */
	  return NO_ERROR;
	}
    }

  /* do shard group id validation
   * permit minus group id for migrator
   */
  shard_key = req_shard_key;

  if (is_shard_table == true)
    {
      /* is DML for shard table */

      /* check iff valid shard_key for DML */
      if (shard_key == NULL
	  || DB_IS_NULL (shard_key)
	  || db_value_compare (shard_key, &system_Ddl_key) == DB_EQ
	  || is_catalog_table == true)
	{
	  assert (false);
	  error = ER_GENERIC_ERROR;	/* TODO - */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
		  "qexec_lock_table_and_shard_key(): Invalid shard_key");
	  return error;
	}

      assert (shard_groupid != GLOBAL_GROUPID);
      if (shard_groupid == GLOBAL_GROUPID)
	{
	  error = ER_SHARD_INVALID_GROUPID;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, shard_groupid);
	  return error;
	}

      assert (!is_catalog_table);

      /* check iff has shardkey group id ownership */
      if (shard_groupid < GLOBAL_GROUPID)
	{
	  ;			/* is DML derived from migrator, GC */
	}
      else if (shard_groupid == GLOBAL_GROUPID
	       || !SHARD_GROUP_OWN (thread_p, shard_groupid))
	{
	  error = ER_SHARD_INVALID_GROUPID;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, shard_groupid);
	  return error;
	}

      /* check iff is in DDL; only permit shard catalog lock */
      if (lock_get_current_lock (thread_p, &system_Ddl_key) != NULL_LOCK)
	{
	  assert (false);
	  help_sprint_value (&system_Ddl_key, key1_buf, 255);
	  key1_buf[255] = 0;
	  help_sprint_value (shard_key, key2_buf, 255);
	  key2_buf[255] = 0;

	  error = ER_SHARD_CANT_LOCK_TWO_SHARD_KEY_A_TRAN;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 2, key1_buf,
		  key2_buf);
	  return error;
	}
    }
  else
    {
      /* is DML to catalog table
         or DML to global table
         or DML derived from DDL */

      /* check iff valid shard_key for DML */
      if (shard_key != NULL)
	{
	  assert (false);
	  error = ER_GENERIC_ERROR;	/* TODO - */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
		  "qexec_lock_table_and_shard_key(): Invalid shard_key");
	  return error;
	}

      /* check iff DML to system table */
      assert (shard_key == NULL);
      if (is_catalog_table == true)
	{
	  shard_key = &shard_Catalog_key;	/* set shard catalog lock */
	}
      else
	{
	  shard_key = &shard_Global_key;	/* set shard global lock */
	}

      assert (shard_groupid == GLOBAL_GROUPID);
      if (shard_groupid != GLOBAL_GROUPID)
	{
	  error = ER_SHARD_INVALID_GROUPID;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, shard_groupid);
	  return error;
	}

      /* check iff is in DDL; only permit shard catalog lock */
      if (lock_get_current_lock (thread_p, &system_Ddl_key) != NULL_LOCK)
	{
	  if (db_value_compare (shard_key, &shard_Catalog_key) != DB_EQ)
	    {
	      assert (false);
	      help_sprint_value (&system_Ddl_key, key1_buf, 255);
	      key1_buf[255] = 0;
	      help_sprint_value (shard_key, key2_buf, 255);
	      key2_buf[255] = 0;

	      error = ER_SHARD_CANT_LOCK_TWO_SHARD_KEY_A_TRAN;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 2, key1_buf,
		      key2_buf);
	      return error;
	    }
	}
    }

  /* acquire shardkey lock; System_key, Noshard_key, shard_key
   */
  assert (db_value_compare (shard_key, &system_Ddl_key) != DB_EQ);

  if (DB_IS_NULL (&tdes->tran_shard_key))
    {
      /* is DML; INSERT, DELETE, UPDATE, SELECT ... FOR UPDATE
       */
      if (for_update == true)
	{
	  if (lock_object (thread_p, shard_key, X_LOCK, LK_UNCOND_LOCK) !=
	      LK_GRANTED)
	    {
	      error = er_errid ();
	      if (error == NO_ERROR)
		{
#if 0				/* TODO - currently disable; crash at server stop */
		  assert (false);
#endif
		  error = ER_FAILED;
		}
	      return error;
	    }
	}

      TR_TABLE_LOCK (thread_p);

      pr_clone_value (shard_key, &tdes->tran_shard_key);
      tdes->tran_group_id = shard_groupid;

      TR_TABLE_UNLOCK (thread_p);

      /* re-check iff has shardkey group id ownership */
      if (shard_groupid > GLOBAL_GROUPID
	  && !SHARD_GROUP_OWN (thread_p, shard_groupid))
	{
	  error = ER_SHARD_INVALID_GROUPID;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, shard_groupid);
	  return error;
	}
    }
  else
    {
      assert (db_value_compare (&tdes->tran_shard_key, &system_Ddl_key) !=
	      DB_EQ);

      assert (DB_VALUE_TYPE (&tdes->tran_shard_key) ==
	      DB_VALUE_TYPE (shard_key));
      if (db_value_compare (&tdes->tran_shard_key, shard_key) != DB_EQ)
	{
	  help_sprint_value (&tdes->tran_shard_key, key1_buf, 255);
	  key1_buf[255] = 0;
	  help_sprint_value (shard_key, key2_buf, 255);
	  key2_buf[255] = 0;

	  error = ER_SHARD_CANT_LOCK_TWO_SHARD_KEY_A_TRAN;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 2, key1_buf,
		  key2_buf);
	  return error;
	}
    }

  assert (error == NO_ERROR);

  return NO_ERROR;
}

/*
 * xlock_system_ddl_lock ()
 *
 */
int
xlock_system_ddl_lock (THREAD_ENTRY * thread_p)
{
  LOG_TDES *tdes;
  int error = NO_ERROR;
  LOG_LSA min_lsa;
  bool dummy_param = false;
  int lock_error;

  tdes = logtb_get_current_tdes (thread_p);
  if (tdes == NULL)
    {
      error = ER_LOG_UNKNOWN_TRANINDEX;
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
	      error, 1, logtb_get_current_tran_index (thread_p));

      GOTO_EXIT_ON_ERROR;
    }

  /* check iff acquired any shardkey lock */
  if (!DB_IS_NULL (&tdes->tran_shard_key))
    {
      char key1_buf[256], key2_buf[256];

      assert (false);		/* is impossible */

      help_sprint_value (&tdes->tran_shard_key, key1_buf, 255);
      key1_buf[255] = 0;
      help_sprint_value (&system_Ddl_key, key2_buf, 255);
      key2_buf[255] = 0;

      error = ER_SHARD_CANT_LOCK_TWO_SHARD_KEY_A_TRAN;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 2, key1_buf, key2_buf);

      GOTO_EXIT_ON_ERROR;
    }

  tdes->type = TRAN_TYPE_DDL;

  lock_error = lock_object (thread_p, &system_Ddl_key,
			    X_LOCK, LK_UNCOND_LOCK);
  if (lock_error != LK_GRANTED)
    {
      assert (lock_error == LK_NOTGRANTED_DUE_ERROR);

      error = er_errid ();

      GOTO_EXIT_ON_ERROR;
    }

  TR_TABLE_LOCK (thread_p);
  logtb_find_smallest_begin_lsa_without_ddl_tran (thread_p, &min_lsa,
						  &dummy_param);
  if (LSA_ISNULL (&min_lsa))
    {
      prior_lsa_get_current_lsa (thread_p, &min_lsa);
    }
  assert (!LSA_ISNULL (&min_lsa));
  logtb_set_commit_lsa (&min_lsa);
  TR_TABLE_UNLOCK (thread_p);

  return error;

exit_on_error:

  if (error == NO_ERROR)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
	      "Invalid error code");
    }

  return error;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * xlock_change_class_xlock_to_ulock () -
 *
 * return: NO_ERROR or error code
 *
 * class_oid(in):
 */
int
xlock_change_class_xlock_to_ulock (THREAD_ENTRY * thread_p, OID * class_oid)
{
  int error = NO_ERROR;
  DB_VALUE oid_val;
  int lock_error;

  DB_MAKE_OID (&oid_val, class_oid);

#if defined(SERVER_MODE)
  if (lock_get_current_lock (thread_p, &oid_val) != X_LOCK)
    {
      assert (false);
      error = ER_GENERIC_ERROR;	/* TODO - */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
	      "xlock_change_class_xlock_to_ulock(): Not acquired X_LOCK on table");
      return error;
    }
#else
  assert (false);		/* should be avoided */
  error = ER_GENERIC_ERROR;	/* TODO - */
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
	  "xlock_change_class_xlock_to_ulock(): Not acquired X_LOCK on table");
  return error;
#endif

  lock_unlock_object (thread_p, &oid_val, LK_UNLOCK_TYPE_FORCE);

  lock_error = lock_object (thread_p, &oid_val, U_LOCK, LK_UNCOND_LOCK);
  if (lock_error != LK_GRANTED)
    {
      error = er_errid ();
      if (error == NO_ERROR)
	{
	  assert (error != NO_ERROR);
	  error = ER_GENERIC_ERROR;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
	}
    }

  return error;
}
#endif
