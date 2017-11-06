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
 * perf_monitor.h - Monitor execution statistics at Client
 */

#ifndef _PERF_MONITOR_H_
#define _PERF_MONITOR_H_

#ident "$Id$"

#include <stdio.h>
#include <time.h>
#include <sys/time.h>

#include "memory_alloc.h"
#include "thread.h"

/* EXPORTED GLOBAL DEFINITIONS */
#define MAX_DIAG_DATA_VALUE     0xfffffffffffffLL

#ifndef DIFF_TIMEVAL
#define DIFF_TIMEVAL(start, end, elapsed) \
    do { \
      (elapsed).tv_sec = (end).tv_sec - (start).tv_sec; \
      (elapsed).tv_usec = (end).tv_usec - (start).tv_usec; \
      if ((elapsed).tv_usec < 0) \
        { \
          (elapsed).tv_sec--; \
          (elapsed).tv_usec += 1000000; \
        } \
    } while (0)
#endif

#define ADD_TIMEVAL(total, start, end) do {             \
  (total).tv_usec +=                                    \
    ((end).tv_usec - (start).tv_usec) >= 0 ?            \
      ((end).tv_usec - (start).tv_usec)                 \
    : (1000000 + ((end).tv_usec - (start).tv_usec));    \
  (total).tv_sec +=                                     \
    ((end).tv_usec - (start).tv_usec) >= 0 ?            \
      ((end).tv_sec - (start).tv_sec)                   \
    : ((end).tv_sec - (start).tv_sec-1);                \
  (total).tv_sec +=                                     \
    (total).tv_usec/1000000;                            \
  (total).tv_usec %= 1000000;                           \
} while(0)

#define ADD_WAIT_TIMEVAL(total, wait_time) do {         \
  (total).tv_usec += (wait_time).tv_usec;               \
  (total).tv_sec += (wait_time).tv_sec;                 \
  (total).tv_sec += (total).tv_usec/1000000;            \
  (total).tv_usec %= 1000000;                           \
} while(0)

#define INIT_TIMEVAL(time_val) ((time_val).tv_sec = 0, (time_val).tv_usec = 0)

#define TO_MSEC(elapsed) \
  ((int)((elapsed.tv_sec * 1000) + (int) (elapsed.tv_usec / 1000)))

#if defined (EnableThreadMonitoring)
#define MONITOR_WAITING_THREAD(elpased) \
    (prm_get_integer_value (PRM_ID_MNT_WAITING_THREAD) > 0 \
     && ((elpased).tv_sec * 1000 + (elpased).tv_usec / 1000) \
         > prm_get_integer_value (PRM_ID_MNT_WAITING_THREAD))
#endif

typedef enum
{
  /* Statistics at disk level */
  MNT_STATS_DISK_SECTOR_ALLOCS,
  MNT_STATS_DISK_SECTOR_DEALLOCS,
  MNT_STATS_DISK_PAGE_ALLOCS,
  MNT_STATS_DISK_PAGE_DEALLOCS,

  /* Statistics at file io level */
  MNT_STATS_FILE_CREATES,
  MNT_STATS_FILE_REMOVES,
  MNT_STATS_FILE_IOREADS,
  MNT_STATS_FILE_IOWRITES,
  MNT_STATS_FILE_IOSYNCHES,

  /* Statistics at page level */
  MNT_STATS_DATA_PAGE_FETCHES,
#if 1				/* fetches sub-info */
  MNT_STATS_DATA_PAGE_FETCHES_FTAB,	/* file allocset table page             */
  MNT_STATS_DATA_PAGE_FETCHES_HEAP,	/* heap page                            */
  MNT_STATS_DATA_PAGE_FETCHES_HEAP_HEADER,	/* heap page header                     */
  MNT_STATS_DATA_PAGE_FETCHES_VOLHEADER,	/* volume header page                   */
  MNT_STATS_DATA_PAGE_FETCHES_VOLBITMAP,	/* volume bitmap page                   */
  MNT_STATS_DATA_PAGE_FETCHES_XASL,	/* xasl stream page                     */
  MNT_STATS_DATA_PAGE_FETCHES_QRESULT,	/* query result page                    */
  MNT_STATS_DATA_PAGE_FETCHES_EHASH,	/* ehash bucket/dir page                */
  MNT_STATS_DATA_PAGE_FETCHES_LARGEOBJ,	/* large object/dir page                */
  MNT_STATS_DATA_PAGE_FETCHES_OVERFLOW,	/* overflow page (with ovf_keyval)      */
  MNT_STATS_DATA_PAGE_FETCHES_AREA,	/* area page                            */
  MNT_STATS_DATA_PAGE_FETCHES_CATALOG,	/* catalog page                         */
  MNT_STATS_DATA_PAGE_FETCHES_CATALOG_OVF,	/* catalog overflow page                         */
  MNT_STATS_DATA_PAGE_FETCHES_BTREE,	/* b+tree index page                    */
  MNT_STATS_DATA_PAGE_FETCHES_FORMAT,	/* disk_format                    */
#endif

  MNT_STATS_DATA_PAGE_DIRTIES,
  MNT_STATS_DATA_PAGE_IOREADS,
  MNT_STATS_DATA_PAGE_IOWRITES,
  MNT_STATS_DATA_PAGE_VICTIMS,
  MNT_STATS_DATA_PAGE_IOWRITES_FOR_REPLACEMENT,

  /* Statistics at log level */
  MNT_STATS_LOG_PAGE_IOREADS,
  MNT_STATS_LOG_PAGE_IOWRITES,
  MNT_STATS_LOG_APPEND_RECORDS,
  MNT_STATS_LOG_ARCHIVES,
  MNT_STATS_LOG_CHECKPOINTS,
  MNT_STATS_LOG_WALS,

  /* Statistics at lock level */
  MNT_STATS_DDL_LOCKS_REQUESTS,
  MNT_STATS_CLASS_LOCKS_REQUEST,
  MNT_STATS_CATALOG_LOCKS_REQUEST,
  MNT_STATS_GLOBAL_LOCKS_REQUEST,
  MNT_STATS_SHARD_LOCKS_REQUEST,
  MNT_STATS_PAGE_LOCKS_ACQUIRED,
  MNT_STATS_OBJECT_LOCKS_ACQUIRED,
  MNT_STATS_PAGE_LOCKS_CONVERTED,
  MNT_STATS_OBJECT_LOCKS_CONVERTED,
  MNT_STATS_PAGE_LOCKS_RE_REQUESTED,
  MNT_STATS_OBJECT_LOCKS_RE_REQUESTED,
  MNT_STATS_PAGE_LOCKS_WAITS,
  MNT_STATS_OBJECT_LOCKS_WAITS,

  /* Transaction Management level */
  MNT_STATS_TRAN_COMMITS,
  MNT_STATS_TRAN_ROLLBACKS,
  MNT_STATS_TRAN_SAVEPOINTS,
  MNT_STATS_TRAN_TOPOPS,
  MNT_STATS_TRAN_INTERRUPTS,

  /* Statistics at btree level */
  MNT_STATS_BTREE_INSERTS,
  MNT_STATS_BTREE_DELETES,
  MNT_STATS_BTREE_UPDATES,
  MNT_STATS_BTREE_LOAD_DATA,
  MNT_STATS_BTREE_COVERED,
  MNT_STATS_BTREE_NONCOVERED,
  MNT_STATS_BTREE_RESUMES,
  MNT_STATS_BTREE_MULTIRANGE_OPTIMIZATION,
  MNT_STATS_BTREE_SPLITS,
  MNT_STATS_BTREE_MERGES,
  MNT_STATS_BTREE_PAGE_ALLOCS,
  MNT_STATS_BTREE_PAGE_DEALLOCS,

  /* Execution statistics for the query manager */
  MNT_STATS_QUERY_SELECTS,
  MNT_STATS_QUERY_INSERTS,
  MNT_STATS_QUERY_DELETES,
  MNT_STATS_QUERY_UPDATES,
  MNT_STATS_QUERY_SSCANS,
  MNT_STATS_QUERY_ISCANS,
  MNT_STATS_QUERY_LSCANS,
  MNT_STATS_QUERY_METHSCANS,
  MNT_STATS_QUERY_NLJOINS,
  MNT_STATS_QUERY_MJOINS,
  MNT_STATS_QUERY_OBJFETCHES,
  MNT_STATS_QUERY_HOLDABLE_CURSORS,

  /* execution statistics for external sort */
  MNT_STATS_SORT_IO_PAGES,
  MNT_STATS_SORT_DATA_PAGES,

  /* Network Communication level */
  MNT_STATS_NETWORK_REQUESTS,

  /* Statistics at Flush Control */
  MNT_STATS_ADAPTIVE_FLUSH_PAGES,
  MNT_STATS_ADAPTIVE_FLUSH_LOG_PAGES,
  MNT_STATS_ADAPTIVE_FLUSH_MAX_PAGES,

  /* Prior LSA */
  MNT_STATS_PRIOR_LSA_LIST_SIZE,
  MNT_STATS_PRIOR_LSA_LIST_MAXED,
  MNT_STATS_PRIOR_LSA_LIST_REMOVED,

  /* Heap best space info */
  MNT_STATS_HEAP_STATS_BESTSPACE_ENTRIES,
  MNT_STATS_HEAP_STATS_BESTSPACE_MAXED,

  /* HA info */
  MNT_STATS_HA_LAST_FLUSHED_PAGEID,
  MNT_STATS_HA_EOF_PAGEID,
  MNT_STATS_HA_CURRENT_PAGEID,
  MNT_STATS_HA_REQUIRED_PAGEID,
  MNT_STATS_HA_REPLICATION_DELAY,

  /* Plan cache */
  MNT_STATS_PLAN_CACHE_ADD,
  MNT_STATS_PLAN_CACHE_LOOKUP,
  MNT_STATS_PLAN_CACHE_HIT,
  MNT_STATS_PLAN_CACHE_MISS,
  MNT_STATS_PLAN_CACHE_FULL,
  MNT_STATS_PLAN_CACHE_DELETE,
  MNT_STATS_PLAN_CACHE_INVALID_XASL_ID,
  MNT_STATS_PLAN_CACHE_QUERY_STRING_HASH_ENTRIES,
  MNT_STATS_PLAN_CACHE_XASL_ID_HASH_ENTRIES,
  MNT_STATS_PLAN_CACHE_CLASS_OID_HASH_ENTRIES,

  MNT_STATS_DATA_PAGE_BUFFER_HIT_RATIO,	/* should be the last member */

  MNT_SIZE_OF_SERVER_EXEC_STATS
} MNT_SERVER_ITEM;

/*
 * Server execution statistic structure
 */
typedef struct mnt_server_exec_stats MNT_SERVER_EXEC_STATS;
struct mnt_server_exec_stats
{
  INT64 values[MNT_SIZE_OF_SERVER_EXEC_STATS];
  UINT64 acc_time[MNT_SIZE_OF_SERVER_EXEC_STATS];
};

#if defined(CS_MODE) || defined(SA_MODE)
extern int mnt_start_stats (bool for_all_trans);
extern int mnt_stop_stats (void);
extern void mnt_reset_stats (void);
extern void mnt_print_stats (FILE * stream);
extern void mnt_print_global_stats (FILE * stream, bool cumulative,
				    const char *substr, const char *db_name);
#endif /* CS_MODE || SA_MODE */

#if defined(SERVER_MODE) || defined (SA_MODE)
#if defined(X86)
#define PERF_MON_GET_CURRENT_TIME(VAR)					\
	do {								\
	  unsigned int lo, hi;						\
	  __asm__ __volatile__ ("rdtsc":"=a" (lo), "=d" (hi));		\
	  (VAR) = ((UINT64) lo) | (((UINT64) hi) << 32);		\
	} while (0)
#else
#define PERF_MON_GET_CURRENT_TIME(VAR)					\
	do {								\
	  (VAR) = 0;							\
	} while (0)
#endif

#define mnt_stats_event_on(THREAD_P,ITEM)	\
	mnt_stats_counter(thread_p,ITEM,1)
#define mnt_stats_event_off(THREAD_P,ITEM)	\
	mnt_stats_counter(thread_p,ITEM,-1)
extern void mnt_stats_counter (THREAD_ENTRY * thread_p, MNT_SERVER_ITEM item,
			       INT64 value);
extern void mnt_stats_counter_with_time (THREAD_ENTRY * thread_p,
					 MNT_SERVER_ITEM item, INT64 value,
					 UINT64 start_time);
extern void mnt_stats_gauge (THREAD_ENTRY * thread_p, MNT_SERVER_ITEM item,
			     INT64 value);
extern INT64 mnt_get_stats (THREAD_ENTRY * thread_p, MNT_SERVER_ITEM item);

extern void mnt_server_dump_stats_to_buffer (const MNT_SERVER_EXEC_STATS *
					     stats, char *buffer,
					     int buf_size,
					     const char *substr);
#else /* SERVER_MODE || SA_MODE */
#define mnt_stats_counter(THREAD_P,ITEM,VALUE)
#define mnt_stats_counter_with_time(THREAD_P,ITEM,VALUE,START_TIME)
#define mnt_stats_event_on(THREAD_P,ITEM)
#define mnt_stats_event_off(THREAD_P,ITEM)
#define PERF_MON_GET_CURRENT_TIME(VAR)
#endif /* CS_MODE */

extern int mnt_diff_stats (MNT_SERVER_EXEC_STATS * diff_stats,
			   MNT_SERVER_EXEC_STATS * new_stats,
			   MNT_SERVER_EXEC_STATS * old_stats);
extern bool mnt_stats_is_cumulative (MNT_SERVER_ITEM item);
extern bool mnt_stats_is_collecting_time (MNT_SERVER_ITEM item);

#endif /* _PERF_MONITOR_H_ */
