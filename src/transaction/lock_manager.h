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
 * 	Overview: LOCK MANAGMENT MODULE (AT THE SERVER) -- Interface --
 *
 */

#ifndef _LOCK_MANAGER_H_
#define _LOCK_MANAGER_H_

#ident "$Id$"

#include "config.h"

#include <time.h>
#include <stdio.h>

#include "error_manager.h"
#include "oid.h"
#include "storage_common.h"
#include "locator.h"
#include "log_comm.h"

#if defined(SERVER_MODE)
#include "connection_error.h"
#endif /* SERVER_MODE */
#include "thread.h"

enum
{
  LK_GRANTED = 1,
  LK_NOTGRANTED = 2,
  LK_NOTGRANTED_DUE_ABORTED = 3,
  LK_NOTGRANTED_DUE_TIMEOUT = 4,
  LK_NOTGRANTED_DUE_ERROR = 5,
  LK_GRANTED_PUSHINSET_LOCKONE = 6,
  LK_GRANTED_PUSHINSET_RELOCKALL = 7
};

enum
{
/* Value to timeout immediately.. not wait */
  LK_ZERO_WAIT = 0,
/* Value to wait forever */
  LK_INFINITE_WAIT = -1,
/* Value to force a timeout without setting errors */
  LK_FORCE_ZERO_WAIT = -2
};

enum
{ LK_UNCOND_LOCK, LK_COND_LOCK };

typedef enum
{
  LK_UNLOCK_TYPE_FORCE, LK_UNLOCK_TYPE_NORMAL
} LK_UNLOCK_TYPE;

typedef enum
{
  NO_KEY_LOCK_ESCALATION = 0,
  NEED_KEY_LOCK_ESCALATION = 1,
  KEY_LOCK_ESCALATED = 2
} KEY_LOCK_ESCALATION;

/*****************************/
/* Lock Heap Entry Structure */
/*****************************/
typedef struct lk_entry LK_ENTRY;
struct lk_entry
{
#if defined(SERVER_MODE)
  struct lk_res *res_head;      /* back to resource entry           */
  THREAD_ENTRY *thrd_entry;     /* thread entry pointer             */
  int tran_index;               /* transaction table index          */
  LOCK granted_mode;            /* granted lock mode                */
  LOCK blocked_mode;            /* blocked lock mode                */
  int count;                    /* number of lock requests          */
  LK_ENTRY *next;               /* next entry                       */
  LK_ENTRY *tran_next;          /* list of locks that trans. holds  */
  LK_ENTRY *tran_prev;          /* list of locks that trans. holds  */
  int bind_index_in_tran;
  XASL_ID xasl_id;
#else                           /* not SERVER_MODE */
  int dummy;
#endif                          /* not SERVER_MODE */
};

#if 0
/* type of locking resource */
typedef enum
{
  LOCK_RESOURCE_INSTANCE,       /* An instance resource */
  LOCK_RESOURCE_CLASS,          /* A class resource */
  LOCK_RESOURCE_ROOT_CLASS,     /* A root class resource */
  LOCK_RESOURCE_OBJECT          /* An object resource */
} LOCK_RESOURCE_TYPE;
#endif

/*
 * Lock Resource Entry Structure
 */
typedef struct lk_res LK_RES;
struct lk_res
{
  pthread_mutex_t res_mutex;    /* resource mutex */
  DB_VALUE val;
  LOCK total_holders_mode;      /* total mode of the holders */
  LOCK total_waiters_mode;      /* total mode of the waiters */
  LK_ENTRY *holder;             /* lock holder list */
  LK_ENTRY *waiter;             /* lock waiter list */
  LK_RES *hash_next;            /* for hash chain */
};

extern int lock_initialize (void);
extern void lock_finalize (void);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int lock_object_wait_msecs (THREAD_ENTRY * thread_p,
                                   const DB_VALUE * val, LOCK lock, int cond_flag, int wait_msecs);
#endif
extern int lock_object (THREAD_ENTRY * thread_p, const DB_VALUE * val, LOCK lock, int cond_flag);
extern int lock_objects_lock_set (THREAD_ENTRY * thread_p, LC_LOCKSET * lockset);
extern int lock_classes_lock_hint (THREAD_ENTRY * thread_p, LC_LOCKHINT * lockhint);
extern void lock_unlock_object (THREAD_ENTRY * thread_p, const DB_VALUE * val, LK_UNLOCK_TYPE unlock_type);
extern void lock_unlock_all (THREAD_ENTRY * thread_p);
extern bool lock_has_xlock (THREAD_ENTRY * thread_p);
extern LOCK lock_get_current_lock (THREAD_ENTRY * thread_p, DB_VALUE * oid_val);
extern void lock_force_timeout_lock_wait_transactions (unsigned short stop_phase);
extern bool lock_force_timeout_expired_wait_transactions (void *thrd_entry);
extern bool lock_check_local_deadlock_detection (void);
extern void lock_detect_local_deadlock (THREAD_ENTRY * thread_p);
extern void lock_clear_deadlock_victim (int tran_index);
extern int lock_get_lock_holder_tran_index (THREAD_ENTRY * thread_p, char **out_buf, int waiter_index, LK_RES * res);
extern int lock_shard_key_lock (THREAD_ENTRY * thread_p,
                                int shard_groupid, DB_VALUE * req_shard_key,
                                OID * class_oid, bool is_shard_table, bool is_catalog_table, bool for_update);
extern int xlock_system_ddl_lock (THREAD_ENTRY * thread_p);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int xlock_change_class_xlock_to_ulock (THREAD_ENTRY * thread_p, OID * class_oid);
#endif
extern unsigned int lock_get_number_object_locks (void);

#endif /* _LOCK_MANAGER_H_ */
