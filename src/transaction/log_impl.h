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
 * log_impl.h -
 *
 */

#ifndef _LOG_IMPL_H_
#define _LOG_IMPL_H_

#ident "$Id$"

#include "config.h"

#include <signal.h>
#include <assert.h>

#include "storage_common.h"
#include "log_comm.h"
#include "log_compress.h"
#include "recovery.h"
#include "porting.h"
#include "critical_section.h"
#include "release_string.h"
#include "file_io.h"
#include "thread.h"
#include "query_list.h"
#include "lock_manager.h"
#include "connection_globals.h"
#include "backup.h"

#define NUM_NORMAL_TRANS (prm_get_integer_value (PRM_ID_MAX_CLIENTS))
#define NUM_SYSTEM_TRANS 1
#define NUM_NON_SYSTEM_TRANS (css_get_max_conn ())
#define MAX_NTRANS \
  (NUM_NON_SYSTEM_TRANS + NUM_SYSTEM_TRANS)

#if defined(SERVER_MODE)
#define LOG_CS_ENTER(thread_p) \
        csect_enter((thread_p), CSECT_LOG, INF_WAIT)
#define LOG_CS_ENTER_READ_MODE(thread_p) \
        csect_enter_as_reader((thread_p), CSECT_LOG, INF_WAIT)
#define LOG_CS_DEMOTE(thread_p) \
        csect_demote((thread_p), CSECT_LOG, INF_WAIT)
#define LOG_CS_PROMOTE(thread_p) \
        csect_promote((thread_p), CSECT_LOG, INF_WAIT)
#define LOG_CS_EXIT() \
        csect_exit(CSECT_LOG)

#define TR_TABLE_LOCK(thread_p) \
        logtb_lock_tran_table(thread_p)
#define TR_TABLE_UNLOCK(thread_p) \
        logtb_unlock_tran_table(thread_p)

#define LOG_ARCHIVE_CS_ENTER(thread_p)                                       \
        csect_enter_critical_section (thread_p, &log_Gl.archive.archives_cs, \
                                      INF_WAIT)
#define LOG_ARCHIVE_CS_ENTER_READ_MODE(thread_p)                             \
        csect_enter_critical_section_as_reader (thread_p,                    \
                                                &log_Gl.archive.archives_cs, \
                                                INF_WAIT)
#define LOG_ARCHIVE_CS_EXIT() \
        csect_exit_critical_section (&log_Gl.archive.archives_cs)

#else /* SERVER_MODE */
#define LOG_CS_ENTER(thread_p)
#define LOG_CS_ENTER_READ_MODE(thread_p)
#define LOG_CS_DEMOTE(thread_p)
#define LOG_CS_PROMOTE(thread_p)
#define LOG_CS_EXIT()

#define TR_TABLE_LOCK(thread_p)
#define TR_TABLE_UNLOCK(thread_p)

#define LOG_ARCHIVE_CS_ENTER(thread_p)
#define LOG_ARCHIVE_CS_ENTER_READ_MODE(thread_p)
#define LOG_ARCHIVE_CS_EXIT()
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
#define LOG_CS_OWN(thread_p) (csect_check_own (thread_p, CSECT_LOG) >= 1)
#define LOG_CS_OWN_WRITE_MODE(thread_p) (csect_check_own (thread_p, CSECT_LOG) == 1)
#define LOG_CS_OWN_READ_MODE(thread_p) (csect_check_own (thread_p, CSECT_LOG) == 2)

#define LOG_ARCHIVE_CS_OWN(thread_p)                 \
        (csect_check_own_critical_section (thread_p, \
           &log_Gl.archive.archives_cs) >= 1)
#define LOG_ARCHIVE_CS_OWN_WRITE_MODE(thread_p)     \
       (csect_check_own_critical_section (thread_p, \
           &log_Gl.archive.archives_cs) == 1)
#define LOG_ARCHIVE_CS_OWN_READ_MODE(thread_p)      \
       (csect_check_own_critical_section (thread_p, \
           &log_Gl.archive.archives_cs) == 2)

#else /* SERVER_MODE */
#define LOG_CS_OWN(thread_p) (true)
#define LOG_CS_OWN_WRITE_MODE(thread_p) (true)
#define LOG_CS_OWN_READ_MODE(thread_p) (true)

#define LOG_ARCHIVE_CS_OWN(thread_p) (true)
#define LOG_ARCHIVE_CS_OWN_WRITE_MODE(thread_p) (true)
#define LOG_ARCHIVE_CS_OWN_READ_MODE(thread_p) (true)
#endif /* !SERVER_MODE */

#define SHARD_GROUP_OWN(thread_p, gid) logtb_shard_group_check_own(thread_p, gid)

#define LOG_ESTIMATE_NACTIVE_TRANS      100	/* Estimate num of trans */
#define LOG_ESTIMATE_NOBJ_LOCKS         977	/* Estimate num of locks */

/* Log data area */
#define LOGAREA_SIZE (LOG_PAGESIZE - SSIZEOF(LOG_HDRPAGE))

/* check if group commit is active */
#define LOG_IS_GROUP_COMMIT_ACTIVE() \
  (prm_get_bigint_value (PRM_ID_LOG_ASYNC_LOG_FLUSH_INTERVAL) > 0)

#define LOG_RESET_APPEND_LSA(lsa)                \
  do {                                           \
    LSA_COPY(&log_Gl.hdr.append_lsa, lsa);       \
    LSA_COPY(&log_Gl.prior_info.prior_lsa, lsa); \
  } while(0)

#define LOG_RESET_PREV_LSA(lsa)                \
  do {                                           \
    LSA_COPY(&log_Gl.append.prev_lsa, lsa);       \
    LSA_COPY(&log_Gl.prior_info.prev_lsa, lsa); \
  } while(0)

#define LOG_APPEND_PTR() ((char *)log_Gl.append.log_pgptr->area +             \
                          log_Gl.hdr.append_lsa.offset)

#define LOG_GET_LOG_RECORD_HEADER(log_page_p, lsa)               \
     ((LOG_RECORD_HEADER *)((log_page_p)->area + (lsa)->offset))

#define LOG_READ_ALIGN(thread_p, lsa, log_pgptr)                                        \
  do {                                                                        \
    (lsa)->offset = DB_ALIGN((lsa)->offset, DOUBLE_ALIGNMENT);                                   \
    while ((lsa)->offset >= (int)LOGAREA_SIZE) {                              \
      assert(log_pgptr != NULL);                                              \
      (lsa)->pageid++;                                                        \
      if ((logpb_fetch_page((thread_p), (lsa)->pageid, log_pgptr)) == NULL)                 \
        logpb_fatal_error((thread_p), true, ARG_FILE_LINE, "LOG_READ_ALIGN");               \
      (lsa)->offset -= LOGAREA_SIZE;                                        \
      (lsa)->offset = DB_ALIGN((lsa)->offset, DOUBLE_ALIGNMENT);                               \
    }                                                                         \
  } while(0)

#define LOG_READ_ADD_ALIGN(thread_p, add, lsa, log_pgptr)                               \
  do {                                                                        \
    (lsa)->offset += (add);                                                   \
    LOG_READ_ALIGN((thread_p), (lsa), (log_pgptr));                                       \
  } while(0)

#define LOG_READ_ADVANCE_WHEN_DOESNT_FIT(thread_p, length, lsa, log_pgptr)              \
  do                                                                          \
    if ((lsa)->offset + (int)(length) >= (int)LOGAREA_SIZE) {                 \
      assert(log_pgptr != NULL);                                              \
      (lsa)->pageid++;                                                        \
      if ((logpb_fetch_page((thread_p), (lsa)->pageid, log_pgptr)) == NULL)                 \
        logpb_fatal_error((thread_p), true, ARG_FILE_LINE,                                  \
                        "LOG_READ_ADVANCE_WHEN_DOESNT_FIT");                  \
      (lsa)->offset = 0;                                                      \
    }                                                                         \
  while(0)


#define LOG_SYSTEM_TRAN_INDEX 0	/* The recovery system transaction index */
#define LOG_SYSTEM_TRANID     0	/* The recovery system transaction       */

#define LOG_FIND_TDES(tran_index)                                             \
  (((tran_index) >= 0 && (tran_index) < log_Gl.trantable.num_total_indices)   \
   ? log_Gl.trantable.all_tdes[(tran_index)] : NULL)

#define LOG_IS_CLIENT_ACTIVE(tdes)                                            \
  ((tdes != NULL                                                              \
    && tdes->tran_index != LOG_SYSTEM_TRAN_INDEX                              \
    && tdes->client_id != NULL_CLIENT_ID                                      \
    && tdes->tran_index != NULL_TRAN_INDEX) ? true : false)

#define LOG_ISTRAN_ACTIVE(tdes)                                               \
  ((tdes)->state == TRAN_ACTIVE && LOG_ISRESTARTED())

#define LOG_ISTRAN_COMMITTED(tdes)                                            \
  ((tdes)->state == TRAN_UNACTIVE_COMMITTED                             ||    \
   (tdes)->state == TRAN_UNACTIVE_WILL_COMMIT                           ||    \
   (tdes)->state == TRAN_UNACTIVE_COMMITTED_WITH_POSTPONE)

#define LOG_ISTRAN_ABORTED(tdes)                                              \
  ((tdes)->state == TRAN_UNACTIVE_ABORTED                               ||    \
   (tdes)->state == TRAN_UNACTIVE_UNILATERALLY_ABORTED)


#define MAXLOGNAME          (30 - 12)

#define LOGPB_HEADER_PAGE_ID             (-9)	/* The first log page in the infinite
						   log sequence. It is always kept
						   on the active portion of the log.
						   Log records are not stored on this
						   page. This page is backed up in
						   all archive logs
						 */
#define LOGPB_IO_NPAGES                  4

#define LOGPB_BUFFER_SIZE_LOWER        (2*ONE_M)

#define LOGWR_COPY_LOG_BUFFER_NPAGES      128

#define LOG_READ_NEXT_TRANID (log_Gl.hdr.next_trid)

#define LOG_ISRESTARTED() (log_Gl.rcv_phase == LOG_RESTARTED)

/* special action for log applier */
#if defined (SERVER_MODE)
#define LOG_IS_HA_CLIENT(client_type)                   \
  ((client_type) == BOOT_CLIENT_LOG_COPIER             \
    || (client_type) == BOOT_CLIENT_REPL_BROKER)

#define LOG_CHECK_REPL_BROKER(thread_p) \
  (logtb_find_current_client_type(thread_p) == BOOT_CLIENT_REPL_BROKER)
#define LOG_CHECK_RBL_MIGRATOR(thread_p) \
  (logtb_find_current_client_type(thread_p) == BOOT_CLIENT_RBL_MIGRATOR)
#else
#define LOG_IS_HA_CLIENT(client_type) (0)

#define LOG_CHECK_REPL_BROKER(thread_p) (0)
#define LOG_CHECK_RBL_MIGRATOR(thread_p) (0)
#endif /* !SERVER_MODE */

#if !defined(_DB_DISABLE_MODIFICATIONS_)
#define _DB_DISABLE_MODIFICATIONS_
extern int db_Disable_modifications;
#endif /* _DB_DISABLE_MODIFICATIONS_ */

#ifndef CHECK_MODIFICATION_NO_RETURN
#if defined (SA_MODE)
#define CHECK_MODIFICATION_NO_RETURN(thread_p, error) \
  error = NO_ERROR;
#else /* SA_MODE */
#define CHECK_MODIFICATION_NO_RETURN(thread_p, error)                             \
  do                                                                              \
    {                                                                             \
      int mod_disabled;                                                           \
      mod_disabled = logtb_is_tran_modification_disabled (thread_p);              \
      if (mod_disabled)                                                           \
        {                                                                         \
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_DB_NO_MODIFICATIONS, 0);   \
  	  er_log_debug (ARG_FILE_LINE, "tdes->disable_modification = %d\n",       \
  		        mod_disabled);                                            \
	  error = ER_DB_NO_MODIFICATIONS;                                         \
        }                                                                         \
      else                                                                        \
        {                                                                         \
	  error = NO_ERROR;                                                       \
        }                                                                         \
    }                                                                             \
  while (0)
#endif /* !SA_MODE */
#endif /* CHECK_MODIFICATION_NO_RETURN */

/*
 * Message id in the set MSGCAT_SET_LOG
 * in the message catalog MSGCAT_CATALOG_RYE (file rye.msg).
 */
#define MSGCAT_LOG_STARTS                               1
#define MSGCAT_LOG_LOGARCHIVE_NEEDED                    2
#define MSGCAT_LOG_BACKUPINFO_NEEDED                    3
#define MSGCAT_LOG_NEWLOCATION                          4
#define MSGCAT_LOG_LOGINFO_COMMENT                      5
#define MSGCAT_LOG_LOGINFO_COMMENT_ARCHIVE_NONEEDED     6
#define MSGCAT_LOG_LOGINFO_COMMENT_MANY_ARCHIVES_NONEEDED 7
#define MSGCAT_LOG_LOGINFO_COMMENT_FROM_RENAMED         8
#define MSGCAT_LOG_LOGINFO_ARCHIVE                      9
#define MSGCAT_LOG_LOGINFO_KEYWORD_ARCHIVE              10
#define MSGCAT_LOG_LOGINFO_REMOVE_REASON                11
#define MSGCAT_LOG_LOGINFO_ACTIVE                       12
#define MSGCAT_LOG_FINISH_COMMIT                        13
#define MSGCAT_LOG_FINISH_ABORT                         14
#define MSGCAT_LOG_INCOMPLTE_MEDIA_RECOVERY             15
#define MSGCAT_LOG_RESETLOG_DUE_INCOMPLTE_MEDIA_RECOVERY 16
#define MSGCAT_LOG_DATABASE_BACKUP_WAS_TAKEN            17
#define MSGCAT_LOG_MEDIACRASH_NOT_IMPORTANT             18
#define MSGCAT_LOG_DELETE_BKVOLS                        19
#define MSGCAT_LOG_ENTER_Y2_CONFIRM                     20
#define MSGCAT_LOG_BACKUP_HALTED_BY_USER                21
#define MSGCAT_LOG_LOGINFO_ARCHIVES_NEEDED_FOR_RESTORE  22
#define MSGCAT_LOG_LOGINFO_PENDING_ARCHIVES_RELEASED    23
#define MSGCAT_LOG_LOGINFO_NOTPENDING_ARCHIVE_COMMENT   24
#define MSGCAT_LOG_LOGINFO_MULT_NOTPENDING_ARCHIVES_COMMENT 25
#define MSGCAT_LOG_READ_ERROR_DURING_RESTORE            26
#define MSGCAT_LOG_INPUT_RANGE_ERROR                    27
#define MSGCAT_LOG_UPTODATE_ERROR                       28
#define MSGCAT_LOG_LOGINFO_COMMENT_UNUSED_ARCHIVE_NAME	29
#define MSGCAT_LOG_MAX_ARCHIVES_HAS_BEEN_EXCEEDED	30

#define MAX_NUM_EXEC_QUERY_HISTORY                      100

typedef enum log_flush LOG_FLUSH;
enum log_flush
{ LOG_DONT_NEED_FLUSH, LOG_NEED_FLUSH };
typedef enum log_setdirty LOG_SETDIRTY;
enum log_setdirty
{ LOG_DONT_SET_DIRTY, LOG_SET_DIRTY };
typedef enum log_getnewtrid LOG_GETNEWTRID;
enum log_getnewtrid
{ LOG_DONT_NEED_NEWTRID, LOG_NEED_NEWTRID };

/*
 * Specify up to int bits of permanent status indicators.
 * Restore in progress is the only one so far, the rest are reserved
 * for future use.  Note these must be specified and used as mask values
 * to test and set individual bits.
 */
enum LOG_PSTATUS
{
  LOG_PSTAT_CLEAR = 0x00,
  LOG_PSTAT_BACKUP_INPROGRESS = 0x01,	/* only one backup at a time */
  LOG_PSTAT_RESTORE_INPROGRESS = 0x02,	/* unset upon successful restore */
  LOG_PSTAT_HDRFLUSH_INPPROCESS = 0x04	/* need to flush log header */
};

enum LOG_PRIOR_LSA_LOCK
{
  LOG_PRIOR_LSA_WITHOUT_LOCK = 0,
  LOG_PRIOR_LSA_WITH_LOCK = 1
};

typedef struct log_clientids LOG_CLIENTIDS;
struct log_clientids		/* see BOOT_CLIENT_CREDENTIAL */
{
  int client_type;
  char client_info[DB_MAX_IDENTIFIER_LENGTH + 1];
  char db_user[LOG_USERNAME_MAX];
  char program_name[PATH_MAX + 1];
  char login_name[L_cuserid + 1];
  char host_name[MAXHOSTNAMELEN + 1];
  int process_id;
};
#define LOG_CLIENTIDS_INITIALIZER  { -1, {'0'}, {'0'}, {'0'}, {'0'}, {'0'}, 0 }
/*
 * LOG PAGE
 */

typedef struct log_hdrpage LOG_HDRPAGE;
struct log_hdrpage
{
  LOG_PAGEID logical_pageid;	/* Logical pageid in infinite log                 */
  PGLENGTH offset;		/* Offset of first log record in this page.
				   This may be useful when previous log page
				   is corrupted and an archive of that page does
				   not exist. Instead of losing the whole log
				   because of such bad page, we could salvage the
				   log starting at the offset address, that is,
				   at the next log record
				 */
  short dummy1;			/* Dummy field for 8byte align */
  int dummy2;			/* Dummy field for 8byte align */
};

/* WARNING:
 * Don't use sizeof(LOG_PAGE) or of any structure that contains it
 * Use macro LOG_PAGESIZE instead.
 * It is also bad idea to allocate a variable for LOG_PAGE on the stack.
 */

typedef struct log_page LOG_PAGE;
struct log_page
{				/* The log page */
  LOG_HDRPAGE hdr;
  char area[1];
};

/*
 * Flush information shared by LFT and normal transaction.
 * Transaction in commit phase has to flush all toflush array's pages.
 */
typedef struct log_flush_info LOG_FLUSH_INFO;
struct log_flush_info
{
  /* Size of array to log append pages to flush */
  int max_toflush;

  /* Number of log append pages that can be flush
   * Not all of the append pages may be full.
   */
  int num_toflush;

  /* A sorted order of log append free pages to flush */
  LOG_PAGE **toflush;

#if defined(SERVER_MODE)
  /* for protecting LOG_FLUSH_INFO */
  pthread_mutex_t flush_mutex;
#endif				/* SERVER_MODE */
};

typedef struct log_group_commit_info LOG_GROUP_COMMIT_INFO;
struct log_group_commit_info
{
  /* group commit waiters count */
  pthread_mutex_t gc_mutex;
  pthread_cond_t gc_cond;
};

#define LOG_GROUP_COMMIT_INFO_INITIALIZER                     \
  {PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER}

typedef enum logwr_mode LOGWR_MODE;
enum logwr_mode
{
  LOGWR_MODE_ASYNC = 1,
  LOGWR_MODE_SEMISYNC,
  LOGWR_MODE_SYNC
};

#define LOGWR_MODE_NAME(mode)                         \
  ((mode) == LOGWR_MODE_SYNC ? "sync" :               \
   (mode) == LOGWR_MODE_ASYNC ? "async" : "semisync")

typedef enum logwr_status LOGWR_STATUS;
enum logwr_status
{
  LOGWR_STATUS_WAIT,
  LOGWR_STATUS_FETCH,
  LOGWR_STATUS_DONE,
  LOGWR_STATUS_DELAY,
  LOGWR_STATUS_ERROR
};

typedef struct logwr_entry LOGWR_ENTRY;
struct logwr_entry
{
  THREAD_ENTRY *thread_p;
  LOG_PAGEID fpageid;
  LOGWR_MODE mode;
  LOGWR_STATUS status;
  LOG_LSA eof_lsa;
  LOG_LSA last_sent_eof_lsa;
  LOG_LSA tmp_last_sent_eof_lsa;
  INT64 start_copy_time;
  LOGWR_ENTRY *next;
};

typedef struct logwr_info LOGWR_INFO;
struct logwr_info
{
  LOGWR_ENTRY *writer_list;
  pthread_mutex_t wr_list_mutex;
  pthread_cond_t flush_start_cond;
  pthread_mutex_t flush_start_mutex;
  pthread_cond_t flush_wait_cond;
  pthread_mutex_t flush_wait_mutex;
  pthread_cond_t flush_end_cond;
  pthread_mutex_t flush_end_mutex;
  bool flush_completed;

  /* to measure the time spent by the last LWT delaying LFT */
  bool trace_last_writer;
  LOG_CLIENTIDS last_writer_client_info;
  INT64 last_writer_elapsed_time;
};

#define LOGWR_INFO_INITIALIZER                                 \
  {NULL,                                                       \
    PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER,       \
    PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER,       \
    PTHREAD_MUTEX_INITIALIZER, PTHREAD_COND_INITIALIZER,       \
    PTHREAD_MUTEX_INITIALIZER,                                 \
    false, false,                                              \
    LOG_CLIENTIDS_INITIALIZER,                                 \
    0                                                          \
   }

typedef struct log_append_info LOG_APPEND_INFO;
struct log_append_info
{
  int vdes;			/* Volume descriptor of active log            */
  LOG_LSA nxio_lsa;		/* Lowest log sequence number which has not
				 * been written to disk (for WAL).
				 */
  LOG_LSA prev_lsa;		/* Address of last append log record          */
  LOG_PAGE *log_pgptr;		/* The log page which is fixed                */
  LOG_PAGE *delayed_free_log_pgptr;	/* Delay freeing a log append page       */

};

#define LOG_APPEND_INFO_INITIALIZER                           \
  {                                                           \
    /* vdes */                                                \
    NULL_VOLDES,                                              \
    /* nxio_lsa */                                            \
    {NULL_PAGEID, NULL_OFFSET},                               \
    /* prev_lsa */                                            \
    {NULL_PAGEID, NULL_OFFSET},                               \
    /* log_pgptr */                                           \
    NULL,                                                     \
    /* delayed_free_log_pgptr */                              \
    NULL}

struct log_topops_addresses
{
  LOG_LSA lastparent_lsa;	/* The last address of the parent transaction.
				 * This is needed for undo of the top system
				 * action
				 */
  LOG_LSA posp_lsa;		/* The first address of a pospone log record
				 * for top system operation. We add this since
				 * it is reset during recovery to the last
				 * reference pospone address.
				 */
};

typedef struct log_topops_stack LOG_TOPOPS_STACK;
struct log_topops_stack
{
  int max;			/* Size of stack                   */
  int last;			/* Last entry in stack             */
  struct log_topops_addresses *stack;	/* Stack for push and pop of top
					   system actions
					 */
};

typedef struct modified_class_entry MODIFIED_CLASS_ENTRY;
struct modified_class_entry
{
  MODIFIED_CLASS_ENTRY *next;
  OID class_oid;
  LOG_LSA last_modified_lsa;
};

typedef enum tran_abort_reason TRAN_ABORT_REASON;
enum tran_abort_reason
{
  TRAN_NORMAL = 0,
  TRAN_ABORT_DUE_DEADLOCK = 1,
  TRAN_ABORT_DUE_ROLLBACK_ON_ESCALATION = 2
};

typedef struct log_tdes LOG_TDES;
struct log_tdes
{				/* Transaction descriptor */
  int tran_index;		/* Index onto transaction table          */
  TRANID trid;			/* Transaction identifier                */
  TRAN_STATE state;		/* Transaction state (e.g., Active,
				   aborted)
				 */
  TRAN_TYPE type;		/* transaction type */
  int wait_msecs;		/* Wait until this number of milliseconds
				   for locks; also see xlogtb_reset_wait_msecs
				 */
  LOG_LSA begin_lsa;		/* start lsa of transaction      */
  LOG_LSA last_lsa;		/* Last log record address of
				   transaction
				 */
  LOG_LSA undo_nxlsa;		/* Next log record address of transaction
				   for UNDO purposes. Needed since
				   compensating log records are logged
				   during UNDO
				 */
  LOG_LSA posp_nxlsa;		/* Next address of a postpone record to be
				   executed. Most of the time is the first
				   address of a postpone log record
				 */
  LOG_LSA savept_lsa;		/* Address of last savepoint             */
  LOG_LSA topop_lsa;		/* Address of last top operation         */
  LOG_LSA tail_topresult_lsa;	/* Address of last partial abort/commit  */
  int client_id;		/* unique client id */
  LOG_CLIENTIDS client;		/* Client identification            */
  CSS_CRITICAL_SECTION cs_topop;	/* critical section to serialize
					   system top operations
					 */
  LOG_TOPOPS_STACK topops;	/* Active top system operations.
				   Used for system permanent nested
				   operations which are independent from
				   current transaction outcome.
				 */
  volatile sig_atomic_t interrupt;
  /* Set to one when the current execution
     must be stopped somehow. We stop it by
     sending an error message during fetching
     of a page.
   */
  MODIFIED_CLASS_ENTRY *modified_class_list;	/* List of classes made dirty. */

  int num_transient_classnames;	/* # of transient classnames by this
				   transaction
				 */
  int num_repl_records;		/* # of replication records */
  int cur_repl_record;		/* # of replication records */
  int append_repl_recidx;	/* index of append replication records */
  struct log_repl *repl_records;	/* replication records */
  LOG_LSA repl_insert_lsa;	/* insert target lsa */
  LOG_LSA repl_update_lsa;	/* update target lsa */
  void *first_save_entry;	/* first save entry for the transaction */

  int num_new_files;		/* # of new files created */
  int num_new_tmp_files;	/* # of new FILE_TMP files created */
  int suppress_replication;	/* suppress writing replication logs when flag is set */

  INT64 query_timeout;		/* a query should be executed before query_timeout time. */

  INT64 query_start_time;
  INT64 tran_start_time;
  XASL_ID xasl_id;		/* xasl id of current query */
  LK_RES *waiting_for_res;	/* resource that i'm waiting for */

  int disable_modifications;	/* db_Disable_modification for each tran */

  TRAN_ABORT_REASON tran_abort_reason;

  /* bind values of executed queries in transaction */
  int num_exec_queries;
  DB_VALUE_ARRAY bind_history[MAX_NUM_EXEC_QUERY_HISTORY];

  DB_VALUE tran_shard_key;
  int tran_group_id;

  BK_BACKUP_SESSION *bk_session;
};

typedef struct working_tran WORKING_TRAN;
struct working_tran
{
  int tran_index;

  WORKING_TRAN *next;
};

typedef struct log_addr_tdesarea LOG_ADDR_TDESAREA;
struct log_addr_tdesarea
{
  LOG_TDES *tdesarea;
  LOG_ADDR_TDESAREA *next;
};

/* Transaction Table */
typedef struct trantable TRANTABLE;
struct trantable
{
  int num_total_indices;	/* Number of total transaction indices     */
  int num_assigned_indices;	/* Number of assigned transaction indices
				 * (i.e., number of active thread/clients)
				 */
  /* Number of prepared participant loose end
   * indices
   */
  int hint_free_index;		/* Hint for a free index                   */
  /* Number of transactions that must be
   * interrupted
   */
  volatile sig_atomic_t num_interrupts;

  /* ordered by begin_lsa */
  WORKING_TRAN *working_tran_list_head;
  WORKING_TRAN *working_tran_list_tail;
  struct log_addr_tdesarea *area;	/* Contiguous area to transaction
					 * descriptors
					 */
  LOG_TDES **all_tdes;		/* Pointers to all transaction descriptors */
};

#define TRANTABLE_INITIALIZER \
  {0, 0, 0, 0, NULL, NULL, NULL, NULL}

typedef struct copy_log_header COPY_LOG_HEADER;
struct copy_log_header
{
  LOG_PAGEID fpageid;		/* Logical pageid at physical location 1 in
				 * copy active log
				 */
  DKNPAGES npages;
  LOG_PAGEID last_flushed_pageid;
  LOG_PAGEID nxarv_pageid;	/* Next logical page to archive             */
  int nxarv_num;		/* Next log archive number                  */
  int last_deleted_arv_num;	/* Last deleted archive number              */


  int server_state;
  int file_status;
  int perm_status;		/* Reserved for future expansion and
				 * permanent status indicators,
				 * e.g. to mark RESTORE_IN_PROGRESS
				 */
};

#define COPY_LOG_HEADER_INITIALIZER                        \
{                                                          \
  /* fpageid, npages, last_flushed_pageid, nxarv_pageid */ \
  NULL_PAGEID, 0, NULL_PAGEID, NULL_PAGEID,   \
  /* nxarv_num, last_deleted_arv_num */                    \
  0, -1,                                                   \
  /* ha_server_state, ha_file_status */                    \
  0, 0,                                                    \
  /* perm_status */                                        \
  0                                                        \
}

/*
 * LOG HEADER INFORMATION
 */
typedef struct log_header LOG_HEADER;
struct log_header
{				/* Log header information */
  char log_magic[RYE_MAGIC_MAX_LENGTH];	/* Magic value for file/magic Unix
					 * utility
					 */
  INT32 dummy;			/* for 8byte align */
  INT64 db_creation;		/* Database creation time. For safety reasons,
				 * this value is set on all volumes and the
				 * log. The value is generated by the log
				 * manager
				 */
  RYE_VERSION db_version;	/* Compatibility of the database against the
				 * current release of Rye
				 */
  PGLENGTH db_iopagesize;	/* Size of pages in the database. For safety
				 * reasons this value is recorded in the log
				 * to make sure that the database is always
				 * run with the same page size
				 */
  PGLENGTH db_logpagesize;	/* Size of log pages in the database. */
  int is_shutdown;		/* Was the log shutdown ?                   */
  TRANID next_trid;		/* Next Transaction identifier              */
  int avg_ntrans;		/* Number of average transactions           */
  int avg_nlocks;		/* Average number of object locks           */
  DKNPAGES npages;		/* Number of pages in the active log portion.
				 * Does not include the log header page.
				 */
  INT8 db_charset;
  INT8 dummy2;			/* Dummy fields for 8byte align */
  INT8 dummy3;
  INT8 dummy4;
  LOG_PAGEID fpageid;		/* Logical pageid at physical location 1 in
				 * active log
				 */
  LOG_LSA append_lsa;		/* Current append location                  */
  LOG_LSA chkpt_lsa;		/* Lowest log sequence address to start the
				 * recovery process
				 */
  LOG_PAGEID nxarv_pageid;	/* Next logical page to archive             */
  LOG_PHY_PAGEID nxarv_phy_pageid;	/* Physical location of logical page to
					 * archive
					 */
  int nxarv_num;		/* Next log archive number                  */
  int last_arv_num_for_syscrashes;	/* Last log archive needed for system
					 * crashes
					 */
  int last_deleted_arv_num;	/* Last deleted archive number              */
  LOG_LSA bkup_level_lsa;	/* Lsa of backup level 0                    */
  char prefix_name[MAXLOGNAME];	/* Log prefix name                          */

  LOG_LSA sof_lsa;		/* Start of Lsa */
  LOG_LSA eof_lsa;		/* End of Lsa */
  LOG_LSA smallest_lsa_at_last_chkpt;

  COPY_LOG_HEADER ha_info;
};

#define LOG_HEADER_INITIALIZER                   \
  {                                              \
     /* magic */                                 \
     {'0'},                                      \
     0, 0,                                       \
     /* db_version */                            \
     RYE_NULL_VERSION,                           \
     0, 0, 0,                                    \
     /* next_trid */                             \
     NULL_TRANID,                                \
     0, 0, 0,					 \
     /* db_charset */				 \
     0,						 \
     0, 0, 0, 0,				 \
     /* append_lsa */                            \
     {NULL_PAGEID, NULL_OFFSET},                 \
     /* chkpt_lsa */                             \
     {NULL_PAGEID, NULL_OFFSET},                 \
     /* nxarv_pageid */                          \
     0,                                          \
     /* nxarv_phy_pageid */                      \
     0,                                          \
     0, 0, 0,                                    \
     /* bkup_level_lsa */                        \
     {NULL_PAGEID, NULL_OFFSET},                 \
     /* prefix_name */                           \
     {'0'},                                      \
     /* sof_lsa */                               \
     {NULL_PAGEID, NULL_OFFSET},                 \
     /* eof_lsa */                               \
     {NULL_PAGEID, NULL_OFFSET},                 \
     /* smallest_lsa_at_last_chkpt */            \
     {NULL_PAGEID, NULL_OFFSET},                 \
     COPY_LOG_HEADER_INITIALIZER                 \
  }

struct log_arv_header
{				/* Log archive header information */
  char magic[RYE_MAGIC_MAX_LENGTH];	/* Magic value for file/magic Unix
					 * utility     */
  INT32 dummy;			/* for 8byte align */
  INT64 db_creation;		/* Database creation time. For safety reasons,
				 * this value is set on all volumes and the
				 * log. The value is generated by the log
				 * manager
				 */
  TRANID next_trid;		/* Next Transaction identifier              */
  DKNPAGES npages;		/* Number of pages in the archive log       */
  LOG_PAGEID fpageid;		/* Logical pageid at physical location 1 in
				 * archive log
				 */
  int arv_num;			/* The archive number                       */
  INT32 dummy2;			/* Dummy field for 8byte align */
};

#define LOG_ARV_HEADER_INITIALIZER              \
  { /* magic */ {'0'},                          \
    0, 0, 0, 0, 0, 0, 0}

typedef enum log_rectype LOG_RECTYPE;
enum log_rectype
{
  /* In order of likely of appearance in the log */
  LOG_SMALLER_LOGREC_TYPE = 0,	/* A lower bound check             */

  LOG_UNDOREDO_DATA = 1,	/* An undo and redo data record    */
  LOG_UNDO_DATA = 2,		/* Only undo data                  */
  LOG_REDO_DATA = 3,		/* Only redo data                  */
  LOG_DBEXTERN_REDO_DATA = 4,	/* Only database external redo data */
  LOG_POSTPONE = 5,		/* Postpone redo data              */
  LOG_RUN_POSTPONE = 6,		/* Run/redo a postpone data. Only
				   for transactions committed with
				   postpone operations
				 */
  LOG_COMPENSATE = 7,		/* Compensation record (compensate a
				   undo record of an aborted tran)
				 */
  LOG_LCOMPENSATE = 8,		/* Compensation record (compensate a
				   logical undo of an aborted tran)
				 */
  LOG_COMMIT_WITH_POSTPONE = 9,	/* Committing server postpone
				   operations
				 */
  LOG_COMMIT = 10,		/* A commit record                 */
  LOG_COMMIT_TOPOPE_WITH_POSTPONE = 11,	/* Committing server top system
					   postpone operations
					 */
  LOG_COMMIT_TOPOPE = 12,	/* A partial commit record, usually
				   from a top system operation
				 */
  LOG_ABORT = 13,		/* An abort record                 */
  LOG_ABORT_TOPOPE = 14,	/* A partial abort record, usually
				   from a top system operation or
				   partial rollback to a save point
				 */
  LOG_START_CHKPT = 15,		/* Start a checkpoint              */
  LOG_END_CHKPT = 16,		/* Checkpoint information          */
  LOG_SAVEPOINT = 17,		/* A user savepoint record         */
  LOG_END_OF_LOG = 18,		/* End of log                      */
  LOG_DUMMY_HEAD_POSTPONE = 19,	/* A dummy log record. No-op       */
  LOG_DUMMY_CRASH_RECOVERY = 20,	/* A dummy log record which indicate
					   the start of crash recovery.
					   No-op
					 */

  LOG_DUMMY_FILLPAGE_FORARCHIVE = 21,	/* Indicates logical end of current
					   page so it could be archived
					   safely. No-op
					   This record is not generated no more.
					   It's kept for backward compatibility.
					 */
  LOG_REPLICATION_DATA = 22,	/* Replicaion log for insert, delete or update */
  LOG_REPLICATION_SCHEMA = 23,	/* Replicaion log for schema, index or system catalog updates */
  LOG_DIFF_UNDOREDO_DATA = 24,	/* diff undo redo data             */
  LOG_DUMMY_HA_SERVER_STATE = 25,	/* HA server state */
  LOG_DUMMY_OVF_RECORD = 26,	/* indicator of the first part of an overflow record */
  LOG_DUMMY_OVF_RECORD_DEL = 27,	/* indicator of an overflow record delete */
  LOG_DUMMY_RECORD = 28,	/* A dummy log record. No-op       */
  LOG_DUMMY_UPDATE_GID_BITMAP = 29,	/* group id bitmap update */
  LOG_LARGER_LOGREC_TYPE	/* A higher bound for checks       */
};

typedef enum log_repl_flush LOG_REPL_FLUSH;
enum log_repl_flush
{
  LOG_REPL_DONT_NEED_FLUSH = -1,	/* no flush */
  LOG_REPL_COMMIT_NEED_FLUSH = 0,	/* log must be flushed at commit */
  LOG_REPL_NEED_FLUSH = 1	/* log must be flushed at commit
				 *  and rollback
				 */
};

typedef struct log_repl LOG_REPL_RECORD;
struct log_repl
{
  LOG_RECTYPE repl_type;	/* LOG_REPLICATION_DATA or LOG_REPLICATION_SCHEMA */
  LOG_RCVINDEX rcvindex;
  OID inst_oid;
  LOG_LSA lsa;
  char *repl_data;		/* the content of the replication log record */
  int length;
  LOG_REPL_FLUSH must_flush;
};

/* Description of a log record */
typedef struct log_rec_header LOG_RECORD_HEADER;
struct log_rec_header
{
  LOG_LSA prev_tranlsa;		/* Address of previous log record for the
				 * same transaction
				 */
  LOG_LSA back_lsa;		/* Backward log address                      */
  LOG_LSA forw_lsa;		/* Forward  log address                      */
  TRANID trid;			/* Transaction identifier of the log record  */
  LOG_RECTYPE type;		/* Log record type (e.g., commit, abort)     */
};

/* Common information of log data records */
struct log_data
{
  LOG_RCVINDEX rcvindex;	/* Index to recovery function                */
  PAGEID pageid;		/* Pageid of recovery data                   */
  PGLENGTH offset;		/* offset of recovery data in pageid         */
  VOLID volid;			/* Volume identifier of recovery data        */
  int gid;			/* Group id of record (for group migrator)   */
};

/* Information of undo_redo log records */
struct log_undoredo
{
  struct log_data data;		/* Location of recovery data                 */
  int ulength;			/* Length of undo data                       */
  int rlength;			/* Length of redo data                       */
};

/* Information of undo log records */
struct log_undo
{
  struct log_data data;		/* Location of recovery data                 */
  int length;			/* Length of undo data                       */
};

/* Information of redo log records */
struct log_redo
{
  struct log_data data;		/* Location of recovery data                 */
  int length;			/* Length of redo data                       */
};

/* Information of database external redo log records */
struct log_dbout_redo
{
  LOG_RCVINDEX rcvindex;	/* Index to recovery function                */
  int length;			/* Length of redo data                       */
};

/* Information of a compensating log records */
struct log_compensate
{
  struct log_data data;		/* Location of recovery data                 */
  LOG_LSA undo_nxlsa;		/* Address of next log record to undo        */
  int length;			/* Length of compensating data               */
};

/*
 * Information of a logical compensate record which marks the end of logical
 * undo
 */
struct log_logical_compensate
{
  LOG_RCVINDEX rcvindex;	/* Index to recovery function                */
  LOG_LSA undo_nxlsa;		/* Address of next log record to undo        */
};

/* This entry is included during commit */
struct log_start_postpone
{
  LOG_LSA posp_lsa;
};

/* This entry is included during the commit of top system operations */
struct log_topope_start_postpone
{
  LOG_LSA lastparent_lsa;	/* The last address of the parent transaction. */
  LOG_LSA posp_lsa;		/* Address where the first postpone operation
				 * start
				 */
};

/* Information of execution of a postpone data */
struct log_run_postpone
{
  struct log_data data;		/* Location of recovery data                 */
  LOG_LSA ref_lsa;		/* Address of the original postpone record   */
  int length;			/* Length of redo data                       */
};

/* A checkpoint record */
struct log_chkpt
{
  LOG_LSA redo_lsa;		/* Oldest LSA of dirty data page in page
				 * buffers
				 */
  int ntrans;			/* Number of active transactions             */
  int ntops;			/* Total number of system operations         */
};

/* replication log structure */
struct log_replication
{
  LOG_LSA lsa;
  int length;
  int rcvindex;
};

/* Transaction descriptor */
struct log_chkpt_trans
{
  TRANID trid;			/* Transaction identifier */
  TRAN_STATE state;		/* Transaction state (e.g., Active, aborted)  */
  LOG_LSA begin_lsa;		/* start lsa of transaction */
  LOG_LSA last_lsa;		/* Last log record address of transaction */
  LOG_LSA undo_nxlsa;		/* Next log record address of transaction for
				 * UNDO purposes. Needed since compensating log
				 * records are logged during UNDO
				 */
  LOG_LSA posp_nxlsa;		/* First address of a postpone record */
  LOG_LSA savept_lsa;		/* Address of last savepoint */
  LOG_LSA tail_topresult_lsa;	/* Address of last partial abort/commit */
  char user_name[LOG_USERNAME_MAX];	/* Name of the client */

};

struct log_chkpt_topops_commit_posp
{
  TRANID trid;			/* Transaction identifier */
  LOG_LSA lastparent_lsa;	/* The last address of the parent transaction.
				 * This is needed for undo of the top system
				 * action
				 */
  LOG_LSA posp_lsa;		/* The first address of a pospone log record
				 * for top system operation. We add this since
				 * it is reset during recovery to the last
				 * reference pospone address.
				 */

};

/* Hint where to start looking for client actions either for commit or abort */
struct log_topope_start_client
{
  LOG_LSA lastparent_lsa;	/* The last address of the parent transaction. */
  LOG_LSA lsa;			/* Starting page with a client record          */
};

/*
 * Information of execution of a client undo or postpone data. This record
 * is needed since we do not have compensating records and run postpone
 * records for client actions
 */
struct log_run_client
{
  LOG_LSA nxlsa;		/* Check for the next undo client record to check */
};

struct log_savept
{
  LOG_LSA prv_savept;		/* Previous savepoint record */
  int length;			/* Savepoint name */
};

struct log_topop_result
{
  LOG_LSA lastparent_lsa;	/* Next log record address of transaction for
				 * UNDO purposes. Last address before the top
				 * action
				 */
  LOG_LSA prv_topresult_lsa;	/* Previous top action (either, partial abort
				 * or partial commit) address
				 */
};

/* Log the time of termination of transaction */
struct log_donetime
{
  INT64 at_time;		/* Database creation time. For safety reasons */
};

/* Log the change of the server's HA state */
struct log_ha_server_state
{
  int server_state;		/* server_state */
  int dummy;			/* dummy for alignment */

  INT64 at_time;		/* time recorded by active server, Time in Milli seconds */
};

struct log_gid_bitmap_update
{
  int migrator_id;		/* migrator id */
  int group_id;			/* group_id */
  int target;			/* 0: master, 1: slave */
  int on_off;			/* on or off */
};

typedef struct log_crumb LOG_CRUMB;
struct log_crumb
{
  int length;
  const void *data;
};

/* state of recovery process */
typedef enum log_recvphase LOG_RECVPHASE;
enum log_recvphase
{
  LOG_RESTARTED,		/* Normal processing.. recovery has been
				   executed.
				 */
  LOG_RECOVERY_ANALYSIS_PHASE,	/* Start recovering. Find the transactions
				   that were active at the time of the crash
				 */
  LOG_RECOVERY_REDO_PHASE,	/* Redoing phase */
  LOG_RECOVERY_UNDO_PHASE,	/* Undoing phase */
};

struct log_archives
{
  int vdes;			/* Last archived accessed        */
  struct log_arv_header hdr;	/* The log archive header        */
  int max_unav;			/* Max size of unavailable array */
  int next_unav;		/* Last unavailable entry        */
  int *unav_archives;		/* Unavailable archives          */
  CSS_CRITICAL_SECTION archives_cs;
};

#define LOG_ARCHIVES_INITIALIZER                     \
  {NULL_VOLDES,                                      \
   LOG_ARV_HEADER_INITIALIZER,                       \
   0, 0,                                             \
   /* unav_archives */                               \
   NULL,                                             \
   CSS_CRITICAL_SECTION_INITIALIZER }

typedef struct background_archiving_info BACKGROUND_ARCHIVING_INFO;
struct background_archiving_info
{
  LOG_PAGEID start_page_id;
  LOG_PAGEID current_page_id;
  LOG_PAGEID last_sync_pageid;
  int vdes;
};

#define BACKGROUND_ARCHIVING_INFO_INITIALIZER                     \
  {NULL_PAGEID, NULL_PAGEID, NULL_PAGEID, NULL_VOLDES}

typedef struct log_data_addr LOG_DATA_ADDR;
struct log_data_addr
{
  const VFID *vfid;		/* File where the page belong or NULL when the page is not
				 * associated with a file */
  PAGE_PTR pgptr;
  PGLENGTH offset;		/* Offset or slot */
  int gid;			/* Group id of record (for group migrator) */
};

#define LOG_ADDR_INITIALIZER    {NULL, NULL, NULL_OFFSET, NULL_GROUPID}
#define LOG_ADDR_SET(addr, v, p, o)      \
  do {                                   \
    (addr)->vfid = (v);                  \
    (addr)->pgptr = (p);                 \
    (addr)->offset = (o);                \
    (addr)->gid = NULL_GROUPID;          \
  } while(0)

typedef struct log_prior_node LOG_PRIOR_NODE;
struct log_prior_node
{
  LOG_RECORD_HEADER log_header;
  LOG_LSA start_lsa;		/* for assertion */

  /* data header info */
  int data_header_length;
  char *data_header;

  /* data info */
  int ulength;
  char *udata;
  int rlength;
  char *rdata;

  LOG_PRIOR_NODE *next;
};

typedef struct log_prior_lsa_info LOG_PRIOR_LSA_INFO;
struct log_prior_lsa_info
{
  LOG_LSA prior_lsa;
  LOG_LSA prev_lsa;

  /* list */
  LOG_PRIOR_NODE *prior_list_header;
  LOG_PRIOR_NODE *prior_list_tail;

  INT64 list_size;		/* bytes */

  /* flush list */
  LOG_PRIOR_NODE *prior_flush_list_header;

  pthread_mutex_t prior_lsa_mutex;
};

#define LOG_PRIOR_LSA_INFO_INITIALIZER                     \
  {                                                        \
    /* prior_lsa */                                        \
    {NULL_PAGEID, NULL_OFFSET},                            \
    /* prev_lsa */                                         \
    {NULL_PAGEID, NULL_OFFSET},                            \
    /* list */                                             \
    NULL, NULL,                                            \
    /* list_size */                                        \
    0,                                                     \
    /* prior_flush_list_header */                          \
    NULL,                                                  \
    /* prior_lsa_mutex */                                  \
    PTHREAD_MUTEX_INITIALIZER                              \
  }

/* Global structure to trantable, log buffer pool, etc */
typedef struct log_global LOG_GLOBAL;
struct log_global
{
  TRANTABLE trantable;		/* Transaction table           */
  struct log_append_info append;	/* The log append info         */
  LOG_PRIOR_LSA_INFO prior_info;
  struct log_header hdr;	/* The log header              */
  struct log_archives archive;	/* Current archive information */
  LOG_PAGEID run_nxchkpt_atpageid;
#if defined(SERVER_MODE)
  LOG_LSA flushed_lsa_lower_bound;	/* lsa */
  pthread_mutex_t chkpt_lsa_lock;
#endif				/* SERVER_MODE */
  LOG_LSA chkpt_redo_lsa;
  DKNPAGES chkpt_every_npages;	/* How frequent a checkpoint
				   should be taken ?
				 */
  LOG_RECVPHASE rcv_phase;	/* Phase of the recovery       */
  LOG_LSA rcv_phase_lsa;	/* LSA of phase (e.g. Restart) */

#if defined(SERVER_MODE)
  int backup_in_progress;
#endif				/* SERVER_MODE */
  LOG_LSA commit_lsa;

  /* Buffer for log hdr I/O, size : SIZEOF_LOG_PAGE_SIZE */
  LOG_PAGE *loghdr_pgptr;

  /* Flush information for dirty log pages */
  LOG_FLUSH_INFO flush_info;

  /* group commit information */
  LOG_GROUP_COMMIT_INFO group_commit_info;
  /* remote log writer information */
  LOGWR_INFO writer_info;
  /* background log archiving info */
  BACKGROUND_ARCHIVING_INFO bg_archive_info;
};

/* Global structure to trantable, log buffer pool, etc   */
typedef struct log_pb_global_data LOG_PB_GLOBAL_DATA;
struct log_pb_global_data
{
  bool log_zip_support;

  MHT_TABLE *ht;		/* Hash table from logical pageid
				   to log buffer
				 */
  struct log_bufarea *poolarea;	/* Contiguous area of buffers */
  struct log_buffer **pool;	/* Log buffer pool           */
  int num_buffers;		/* Number of log buffers     */
  int clock_hand;		/* Clock hand                */

#if defined(SERVER_MODE)
  CSS_CRITICAL_SECTION lpb_cs;
#else				/* !SERVER_MODE */
  LOG_ZIP *log_zip_undo;
  LOG_ZIP *log_zip_redo;
  char *log_data_ptr;
  int log_data_length;
#endif				/* !SERVER_MODE */
};

/* logging statistics */
typedef struct log_logging_stat LOG_LOGGING_STAT;
struct log_logging_stat
{
  /* logpb_next_append_page() call count */
  unsigned long total_append_page_count;
  /* last created page id for logging */
  LOG_PAGEID last_append_pageid;
  /* time taken to use a page for logging */
  double use_append_page_sec;

  /* total delayed page count */
  unsigned long total_delayed_page_count;
  /* last delayed page id */
  LOG_PAGEID last_delayed_pageid;

  /* log buffer full count */
  unsigned long log_buffer_full_count;
  /* log buffer expand count */
  unsigned long log_buffer_expand_count;
  /* log buffer flush count by replacement */
  unsigned long log_buffer_flush_count_by_replacement;

  /* normal flush */
  /* logpb_flush_all_append_pages() call count */
  unsigned long flushall_append_pages_call_count;
  /* pages count to flush in logpb_flush_all_append_pages() */
  unsigned long last_flush_count_by_trans;
  /* total pages count to flush in logpb_flush_all_append_pages() */
  unsigned long total_flush_count_by_trans;
  /* time taken to flush in logpb_flush_all_append_pages() */
  double last_flush_sec_by_trans;
  /* total time taken to flush in logpb_flush_all_append_pages() */
  double total_flush_sec_by_trans;
  /* logpb_flush_pages_direct() count */
  unsigned long direct_flush_count;

  /* logpb_flush_header() call count */
  unsigned long flush_hdr_call_count;
  /* page count to flush in logpb_flush_header() */
  double last_flush_hdr_sec_by_LFT;
  /* total page count to flush in logpb_flush_header() */
  double total_flush_hdr_sec_by_LFT;

  /* total sync count */
  unsigned long total_sync_count;

  /* commit count */
  unsigned long commit_count;
  /* group commit count */
  unsigned long last_group_commit_count;
  /* total group commit count */
  unsigned long total_group_commit_count;

  /* commit count while using a log page */
  unsigned long last_commit_count_while_using_a_page;
  /* total commit count while using a log page */
  unsigned long total_commit_count_while_using_a_page;

  /* commit count included logpb_flush_all_append_pages */
  unsigned long last_commit_count_in_flush_pages;
  /* total commit count included logpb_flush_all_append_pages */
  unsigned long total_commit_count_in_flush_pages;

  /* group commit request count */
  unsigned long gc_commit_request_count;

  /* wait time for group commit */
  double gc_total_wait_time;

  /* flush count in group commit mode by LFT */
  unsigned long gc_flush_count;
};

#if !defined(SERVER_MODE)
extern int log_Tran_index;	/* Index onto transaction table for
				   current thread of execution (client)
				 */
#endif /* !SERVER_MODE */

extern LOG_GLOBAL log_Gl;

extern LOG_PB_GLOBAL_DATA log_Pb;

extern LOG_LOGGING_STAT log_Stat;

#if defined(HAVE_ATOMIC_BUILTINS)
/* Current time in seconds */
extern UINT64 log_Clock_msec;
#endif /* HAVE_ATOMIC_BUILTINS */

/* Name of the database and logs */
extern char log_Path[];
extern char log_Archive_path[];
extern char log_Prefix[];

extern const char *log_Db_fullname;
extern char log_Name_active[];
extern char log_Name_info[];
extern char log_Name_volinfo[];
extern char log_Name_bg_archive[];
extern char log_Name_removed_archive[];

extern int logpb_initialize_pool (THREAD_ENTRY * thread_p);
extern void logpb_finalize_pool (void);
extern bool logpb_is_initialize_pool (void);
extern void logpb_invalidate_pool (THREAD_ENTRY * thread_p);
extern LOG_PAGE *logpb_create (THREAD_ENTRY * thread_p, LOG_PAGEID pageid);
extern LOG_PAGE *log_pbfetch (LOG_PAGEID pageid);
extern void logpb_set_dirty (THREAD_ENTRY * thread_p, LOG_PAGE * log_pgptr,
			     int free_page);
extern int logpb_flush_page (THREAD_ENTRY * thread_p, LOG_PAGE * log_pgptr,
			     int free_page);
extern void logpb_free_page (THREAD_ENTRY * thread_p, LOG_PAGE * log_pgptr);
extern void logpb_free_without_mutex (LOG_PAGE * log_pgptr);
extern LOG_PAGEID logpb_get_page_id (LOG_PAGE * log_pgptr);
extern int logpb_print_hash_entry (FILE * outfp, const void *key,
				   void *ent, void *ignore);
extern int logpb_initialize_header (THREAD_ENTRY * thread_p,
				    struct log_header *loghdr,
				    const char *prefix_logname,
				    DKNPAGES npages, INT64 * db_creation);
extern LOG_PAGE *logpb_create_header_page (THREAD_ENTRY * thread_p);
extern void logpb_fetch_header (THREAD_ENTRY * thread_p,
				struct log_header *hdr);
extern void logpb_fetch_header_with_buffer (THREAD_ENTRY * thread_p,
					    struct log_header *hdr,
					    LOG_PAGE * log_pgptr);
extern void logpb_flush_header (THREAD_ENTRY * thread_p);
extern LOG_PAGE *logpb_fetch_page (THREAD_ENTRY * thread_p, LOG_PAGEID pageid,
				   LOG_PAGE * log_pgptr);
extern LOG_PAGE *logpb_copy_page_from_log_buffer (THREAD_ENTRY * thread_p,
						  LOG_PAGEID pageid,
						  LOG_PAGE * log_pgptr);
#if defined (ENABLE_UNUSED_FUNCTION)
extern LOG_PAGE *logpb_copy_page_from_file (THREAD_ENTRY * thread_p,
					    LOG_PAGEID pageid,
					    LOG_PAGE * log_pgptr);
#endif
extern LOG_PAGE *logpb_read_page_from_file (THREAD_ENTRY * thread_p,
					    LOG_PAGEID pageid,
					    LOG_PAGE * log_pgptr);
extern int logpb_read_page_from_active_log (THREAD_ENTRY * thread_p,
					    LOG_PAGEID pageid, int num_pages,
					    LOG_PAGE * log_pgptr);
extern LOG_PAGE *logpb_write_page_to_disk (THREAD_ENTRY * thread_p,
					   LOG_PAGE * log_pgptr,
					   LOG_PAGEID logical_pageid);
extern PGLENGTH logpb_find_header_parameters (THREAD_ENTRY * thread_p,
					      const char *db_fullname,
					      const char *logpath,
					      const char *prefix_logname,
					      PGLENGTH * io_page_size,
					      PGLENGTH * log_page_size,
					      INT64 * db_creation,
					      RYE_VERSION * db_version,
					      int *db_charset);
extern LOG_PAGE *logpb_fetch_start_append_page (THREAD_ENTRY * thread_p);
extern LOG_PAGE *logpb_fetch_start_append_page_new (THREAD_ENTRY * thread_p);
extern void logpb_flush_pages_direct (THREAD_ENTRY * thread_p);
extern void logpb_flush_pages (THREAD_ENTRY * thread_p, LOG_LSA * flush_lsa);
extern void logpb_invalid_all_append_pages (THREAD_ENTRY * thread_p);
extern void logpb_flush_log_for_wal (THREAD_ENTRY * thread_p,
				     const LOG_LSA * lsa_ptr);
extern LOG_PRIOR_NODE *prior_lsa_alloc_and_copy_data (THREAD_ENTRY * thread_p,
						      LOG_RECTYPE rec_type,
						      LOG_RCVINDEX rcvindex,
						      LOG_DATA_ADDR * addr,
						      int ulength,
						      const char *udata,
						      int rlength,
						      const char *rdata);
extern LOG_PRIOR_NODE *prior_lsa_alloc_and_copy_crumbs (THREAD_ENTRY *
							thread_p,
							LOG_RECTYPE rec_type,
							LOG_RCVINDEX rcvindex,
							LOG_DATA_ADDR * addr,
							const int num_ucrumbs,
							const LOG_CRUMB *
							ucrumbs,
							const int num_rcrumbs,
							const LOG_CRUMB *
							rcrumbs);
extern void prior_lsa_free_node (THREAD_ENTRY * thread_p,
				 LOG_PRIOR_NODE * node);
extern LOG_LSA prior_lsa_next_record (THREAD_ENTRY * thread_p,
				      LOG_PRIOR_NODE * node, LOG_TDES * tdes);
extern LOG_LSA prior_lsa_next_record_with_lock (THREAD_ENTRY * thread_p,
						LOG_PRIOR_NODE * node,
						LOG_TDES * tdes);
extern int prior_lsa_get_current_lsa (THREAD_ENTRY * thread_p,
				      LOG_LSA * current_lsa);

#if defined (ENABLE_UNUSED_FUNCTION)
extern void logpb_remove_append (LOG_TDES * tdes);
#endif
extern void logpb_create_log_info (const char *logname_info,
				   const char *db_fullname);
extern bool logpb_find_volume_info_exist (void);
extern int logpb_create_volume_info (const char *db_fullname);
extern int logpb_recreate_volume_info (THREAD_ENTRY * thread_p);
extern VOLID logpb_add_volume (const char *db_fullname,
			       VOLID new_volid,
			       const char *new_volfullname,
			       DISK_VOLPURPOSE new_volpurpose);
extern int logpb_scan_volume_info (THREAD_ENTRY * thread_p,
				   const char *db_fullname,
				   VOLID ignore_volid, VOLID start_volid,
				   int (*fun) (THREAD_ENTRY * thread_p,
					       VOLID xvolid,
					       const char *vlabel,
					       void *args), void *args);
extern LOG_PHY_PAGEID logpb_to_physical_pageid (LOG_PAGEID logical_pageid);
extern bool logpb_is_page_in_archive (LOG_PAGEID pageid);
extern int logpb_get_archive_number (THREAD_ENTRY * thread_p,
				     LOG_PAGEID pageid);
extern void logpb_decache_archive_info (THREAD_ENTRY * thread_p);
extern LOG_PAGE *logpb_fetch_from_archive (THREAD_ENTRY * thread_p,
					   LOG_PAGEID pageid,
					   LOG_PAGE * log_pgptr,
					   int *ret_arv_num,
					   struct log_arv_header *arv_hdr,
					   bool is_fatal);
extern void logpb_archive_active_log (THREAD_ENTRY * thread_p);
extern void logpb_remove_archive_logs (THREAD_ENTRY * thread_p,
				       const char *info_reason);
extern int logpb_remove_archive_logs_exceed_limit (THREAD_ENTRY * thread_p,
						   int max_count);
extern void logpb_copy_from_log (THREAD_ENTRY * thread_p, char *area,
				 int length, LOG_LSA * log_lsa,
				 LOG_PAGE * log_pgptr);
extern int logpb_initialize_log_names (THREAD_ENTRY * thread_p,
				       const char *db_fullname,
				       const char *logpath,
				       const char *prefix_logname);
extern bool logpb_exist_log (THREAD_ENTRY * thread_p, const char *db_fullname,
			     const char *logpath, const char *prefix_logname);
#if defined(SERVER_MODE)
extern void logpb_do_checkpoint (void);
#endif /* SERVER_MODE */
extern LOG_PAGEID logpb_checkpoint (THREAD_ENTRY * thread_p);
extern void logpb_dump_checkpoint_trans (FILE * out_fp, int length,
					 void *data);
extern void logpb_dump_checkpoint_topops (FILE * out_fp, int length,
					  void *data);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int logpb_rename_all_volumes_files (THREAD_ENTRY * thread_p,
					   VOLID num_perm_vols,
					   const char *to_db_fullname,
					   const char *to_logpath,
					   const char *to_prefix_logname);
#endif
extern int logpb_delete (THREAD_ENTRY * thread_p, VOLID num_perm_vols,
			 const char *db_fullname, const char *logpath,
			 const char *prefix_logname, bool force_delete);
extern int logpb_check_exist_any_volumes (THREAD_ENTRY * thread_p,
					  const char *db_fullname,
					  const char *logpath,
					  const char *prefix_logname,
					  char *first_vol, bool * is_exist);
extern void logpb_fatal_error (THREAD_ENTRY * thread_p, bool logexit,
			       const char *file_name, const int lineno,
			       const char *fmt, ...);
extern void logpb_fatal_error_exit_immediately_wo_flush (THREAD_ENTRY *
							 thread_p,
							 const char
							 *file_name,
							 const int lineno,
							 const char *fmt,
							 ...);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int logpb_check_and_reset_temp_lsa (THREAD_ENTRY * thread_p,
					   VOLID volid);
#endif
extern void logpb_initialize_arv_page_info_table (void);
extern void logpb_initialize_logging_statistics (void);
extern int logpb_background_archiving (THREAD_ENTRY * thread_p);
extern void xlogpb_dump_stat (FILE * outfp);

extern void logpb_dump (FILE * out_fp);

extern void log_recovery (THREAD_ENTRY * thread_p, int ismedia_crash,
			  time_t * stopat, LOG_LSA * stopat_lsa);
extern LOG_LSA *log_startof_nxrec (THREAD_ENTRY * thread_p, LOG_LSA * lsa,
				   bool canuse_forwaddr);

extern void *logtb_realloc_topops_stack (LOG_TDES * tdes, int num_elms);
extern void logtb_define_trantable (THREAD_ENTRY * thread_p,
				    int num_expected_tran_indices,
				    int num_expected_locks);
extern int logtb_define_trantable_log_latch (THREAD_ENTRY * thread_p,
					     int num_expected_tran_indices);
extern void logtb_undefine_trantable (THREAD_ENTRY * thread_p);
extern int logtb_get_number_assigned_tran_indices (void);
extern int logtb_get_number_of_total_tran_indices (void);
#if defined(ENABLE_UNUSED_FUNCTION)
extern bool logtb_am_i_sole_tran (THREAD_ENTRY * thread_p);
extern void logtb_i_am_not_sole_tran (void);
#endif
extern bool logtb_am_i_dba_client (THREAD_ENTRY * thread_p);
extern int
logtb_assign_tran_index (THREAD_ENTRY * thread_p,
			 const BOOT_CLIENT_CREDENTIAL * client_credential,
			 TRAN_STATE * current_state, int wait_msecs);
extern LOG_TDES *logtb_rv_find_allocate_tran_index (THREAD_ENTRY * thread_p,
						    TRANID trid,
						    const LOG_LSA * log_lsa);
extern void logtb_release_tran_index (THREAD_ENTRY * thread_p,
				      int tran_index);
extern void logtb_free_tran_index (THREAD_ENTRY * thread_p, int tran_index);
extern void logtb_free_tran_index_with_undo_lsa (THREAD_ENTRY * thread_p,
						 const LOG_LSA * undo_lsa);
extern void logtb_clear_tdes (THREAD_ENTRY * thread_p, LOG_TDES * tdes);
extern int logtb_get_new_tran_id (THREAD_ENTRY * thread_p, LOG_TDES * tdes);
extern int logtb_start_transaction_if_needed (THREAD_ENTRY * thread_p);
extern int logtb_find_tran_index (THREAD_ENTRY * thread_p, TRANID trid);
extern TRANID logtb_find_tranid (int tran_index);
extern TRANID logtb_find_current_tranid (THREAD_ENTRY * thread_p);
extern const char *logtb_find_current_ha_status (THREAD_ENTRY * thread_p);
extern int logtb_find_current_shard_groupid (THREAD_ENTRY * thread_p);
extern char *logtb_find_current_shard_lockname (THREAD_ENTRY * thread_p);
extern void logtb_set_client_ids_all (LOG_CLIENTIDS * client, int client_type,
				      const char *client_info,
				      const char *db_user,
				      const char *program_name,
				      const char *login_name,
				      const char *host_name, int process_id);
extern void logtb_shutdown_write_normal_clients (THREAD_ENTRY * thread_p,
						 bool force);
extern int logtb_count_active_write_clients (THREAD_ENTRY * thread_p);
extern int logtb_find_client_name_host_pid (int tran_index,
					    char **client_prog_name,
					    char **client_user_name,
					    char **client_host_name,
					    int *client_pid);
extern int logtb_get_current_client_ids (THREAD_ENTRY * thread_p,
					 LOG_CLIENTIDS * client_info);
extern int logtb_find_current_client_type (THREAD_ENTRY * thread_p);
extern char *logtb_find_current_client_name (THREAD_ENTRY * thread_p);
extern char *logtb_find_current_client_hostname (THREAD_ENTRY * thread_p);
#if defined(RYE_DEBUG)
extern LOG_LSA *logtb_find_current_tran_lsa (THREAD_ENTRY * thread_p);
#endif /* RYE_DEBUG */
extern TRAN_STATE logtb_find_state (int tran_index);
extern int logtb_find_wait_msecs (int tran_index);
extern int logtb_find_current_wait_msecs (THREAD_ENTRY * thread_p);
extern TRAN_ISOLATION logtb_find_isolation (int tran_index);
extern TRAN_ISOLATION logtb_find_current_isolation (THREAD_ENTRY * thread_p);
extern bool logtb_set_tran_index_interrupt (THREAD_ENTRY * thread_p,
					    int tran_index, int set);
extern bool logtb_set_suppress_repl_on_transaction (THREAD_ENTRY * thread_p,
						    int tran_index, int set);
extern bool logtb_is_interrupted (THREAD_ENTRY * thread_p, bool clear,
				  bool * continue_checking);
extern bool logtb_is_interrupted_tran (THREAD_ENTRY * thread_p, bool clear,
				       bool * continue_checking,
				       int tran_index);
extern bool logtb_is_active (THREAD_ENTRY * thread_p, TRANID trid);
extern bool logtb_is_current_active (THREAD_ENTRY * thread_p);
extern bool logtb_is_current_system_tran (THREAD_ENTRY * thread_p);
#if defined (ENABLE_UNUSED_FUNCTION)
extern bool logtb_istran_finished (THREAD_ENTRY * thread_p, TRANID trid);
#endif
extern void logtb_disable_update (THREAD_ENTRY * thread_p);
extern void logtb_enable_update (THREAD_ENTRY * thread_p);
extern void logtb_set_to_system_tran_index (THREAD_ENTRY * thread_p);
extern void logtb_set_current_tran_index (THREAD_ENTRY * thread_p,
					  int tran_index);
extern int logtb_get_current_tran_index (THREAD_ENTRY * thread_p);
extern LOG_TDES *logtb_get_current_tdes (THREAD_ENTRY * thread_p);
extern LOG_LSA *logtb_find_unilaterally_largest_undo_lsa (THREAD_ENTRY *
							  thread_p);
extern int logtb_find_smallest_begin_lsa_without_ddl_tran (THREAD_ENTRY *
							   thread_p,
							   LOG_LSA * min_lsa,
							   bool *
							   exists_ddl_tran);
extern int logtb_is_tran_modification_disabled (THREAD_ENTRY * thread_p);
extern void logtb_get_commit_lsa (LOG_LSA * commit_lsa_p);
extern void logtb_set_commit_lsa (LOG_LSA * lsa);
extern int logtb_commit_lsa (THREAD_ENTRY * thread_p);
/* For Debugging */
extern void xlogtb_dump_trantable (THREAD_ENTRY * thread_p, FILE * out_fp);


extern bool logpb_need_wal (const LOG_LSA * lsa);
extern void logpb_get_nxio_lsa (LOG_LSA * lsa_p);
#if 0				/* TODO - do not delete me */
extern const char *logpb_perm_status_to_string (enum LOG_PSTATUS val);
#endif

#if defined (SERVER_MODE)
extern int logtb_lock_tran_table (THREAD_ENTRY * thread_p);
extern int logtb_unlock_tran_table (THREAD_ENTRY * thread_p);
#endif /* SERVER_MODE */
extern bool logtb_shard_group_check_own (THREAD_ENTRY * thread_p, int gid);
extern int logtb_is_blocked_global_dml (void);

extern int logpb_prior_lsa_append_all_list (THREAD_ENTRY * thread_p);

extern void logtb_set_backup_session (THREAD_ENTRY * thread_p,
				      BK_BACKUP_SESSION * session);
extern BK_BACKUP_SESSION *logtb_get_backup_session (THREAD_ENTRY * thread_p);

#endif /* _LOG_IMPL_H_ */
