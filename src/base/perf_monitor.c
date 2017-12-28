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
 * perf_monitor.c - Monitor execution statistics at Client
 * 					Monitor execution statistics
 *                  Monitor execution statistics at Server
 * 					diag server module
 */

#include <stdio.h>
#include <time.h>
#include <assert.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <string.h>
#include "perf_monitor.h"
#include "monitor.h"
#include "network_interface_cl.h"
#include "error_manager.h"
#include "rye_server_shm.h"

#include "thread.h"
#include "log_impl.h"

#if !defined(CS_MODE)
#include "xserver_interface.h"
#endif /* !CS_MODE */

#if defined(CS_MODE) || defined(SA_MODE)
/* Client execution statistic structure */
typedef struct mnt_client_stat_info MNT_CLIENT_STAT_INFO;
struct mnt_client_stat_info
{
  time_t cpu_start_usr_time;
  time_t cpu_start_sys_time;
  time_t elapsed_start_time;
};

static MNT_CLIENT_STAT_INFO mnt_Stat_info;
#endif

#if defined(CS_MODE) || defined(SA_MODE)
static void mnt_get_current_times (time_t * cpu_usr_time,
				   time_t * cpu_sys_time,
				   time_t * elapsed_time);
#endif


#if defined(CS_MODE) || defined(SA_MODE)
/*
 * mnt_start_stats - Start collecting client execution statistics
 *   return: NO_ERROR or ER_FAILED
 */
int
mnt_start_stats (UNUSED_ARG bool for_all_trans)
{
  memset (&mnt_Stat_info, 0, sizeof (mnt_Stat_info));
  return NO_ERROR;
}

/*
 * mnt_stop_stats - Stop collecting client execution statistics
 *   return: NO_ERROR or ER_FAILED
 */
int
mnt_stop_stats (void)
{
  return NO_ERROR;
}

/*
 * mnt_reset_stats - Reset client statistics
 *   return: none
 */
void
mnt_reset_stats (void)
{
  mnt_get_current_times (&mnt_Stat_info.cpu_start_usr_time,
			 &mnt_Stat_info.cpu_start_sys_time,
			 &mnt_Stat_info.elapsed_start_time);
}

/*
 * mnt_get_current_times - Get current CPU and elapsed times
 *   return:
 *   cpu_user_time(out):
 *   cpu_sys_time(out):
 *   elapsed_time(out):
 *
 * Note:
 */
static void
mnt_get_current_times (time_t * cpu_user_time, time_t * cpu_sys_time,
		       time_t * elapsed_time)
{
  struct rusage rusage;
  *cpu_user_time = 0;
  *cpu_sys_time = 0;
  *elapsed_time = 0;
  *elapsed_time = time (NULL);
  if (getrusage (RUSAGE_SELF, &rusage) == 0)
    {
      *cpu_user_time = rusage.ru_utime.tv_sec;
      *cpu_sys_time = rusage.ru_stime.tv_sec;
    }
}

#endif /* CS_MODE || SA_MODE */

#if defined(SERVER_MODE) || defined(SA_MODE)

/*
 * mnt_server_copy_stats - Copy recorded server statistics for the current
 *                          transaction index
 *   return: none
 *   to_stats(out): buffer to copy
 */
void
mnt_server_copy_stats (THREAD_ENTRY * thread_p, MONITOR_STATS * to_stats)
{
  int tran_index;

  assert (to_stats != NULL);

  tran_index = logtb_get_current_tran_index (thread_p);
  assert (tran_index >= 0);
  monitor_copy_stats (NULL, to_stats, tran_index + 1);
}

/*
 * mnt_stats_counter -
 */
void
mnt_stats_counter (THREAD_ENTRY * thread_p, MNT_SERVER_ITEM item, INT64 value)
{
  int tran_index;
  assert (item != MNT_STATS_DATA_PAGE_FETCHES);
  assert (item < MNT_SIZE_OF_SERVER_EXEC_STATS);

  tran_index = logtb_get_current_tran_index (thread_p);
  if (tran_index < 0)
    {
      return;
    }

  monitor_stats_counter (tran_index + 1, item, value);
}

/*
 * mnt_stats_counter_with_time -
 */
void
mnt_stats_counter_with_time (THREAD_ENTRY * thread_p, MNT_SERVER_ITEM item,
			     INT64 value, UINT64 start_time)
{
  int tran_index;
  MNT_SERVER_ITEM parent_item;
  assert (item != MNT_STATS_DATA_PAGE_FETCHES);
  assert (item < MNT_SIZE_OF_SERVER_EXEC_STATS);

  tran_index = logtb_get_current_tran_index (thread_p);
  if (tran_index < 0)
    {
      return;
    }

  monitor_stats_counter_with_time (tran_index + 1, item, value, start_time);
  parent_item = MNT_GET_PARENT_ITEM_FETCHES (item);
  if (parent_item != item)
    {
      assert (parent_item == MNT_STATS_DATA_PAGE_FETCHES);
      monitor_stats_counter_with_time (tran_index + 1, parent_item, value,
				       start_time);

      thread_mnt_track_counter (thread_p, value, start_time);
    }
}

/*
 * mnt_stats_gauge -
 */
void
mnt_stats_gauge (THREAD_ENTRY * thread_p, MNT_SERVER_ITEM item, INT64 value)
{
  int tran_index;
  assert (item < MNT_SIZE_OF_SERVER_EXEC_STATS);

  tran_index = logtb_get_current_tran_index (thread_p);
  if (tran_index < 0)
    {
      return;
    }

  monitor_stats_gauge (tran_index + 1, item, value);
}

/*
 * mnt_get_stats_with_time -
 */
INT64
mnt_get_stats_with_time (THREAD_ENTRY * thread_p, MNT_SERVER_ITEM item,
			 UINT64 * acc_time)
{
  int tran_index;

  tran_index = logtb_get_current_tran_index (thread_p);
  assert (tran_index >= 0);

  return monitor_get_stats_with_time (acc_time, tran_index + 1, item);
}

/*
 * mnt_get_stats -
 */
INT64
mnt_get_stats (THREAD_ENTRY * thread_p, MNT_SERVER_ITEM item)
{
  return mnt_get_stats_with_time (thread_p, item, NULL);
}

#endif /* SERVER_MODE || SA_MODE */

/*
 * mnt_calc_hit_ratio - Do post processing of server statistics
 *   return: none
 *   stats(in/out): server statistics block to be processed
 *   num_stats(in):
 */
void
mnt_calc_hit_ratio (MONITOR_STATS * stats, int num_stats)
{
  if (MNT_STATS_DATA_PAGE_FETCHES >= num_stats
      || MNT_STATS_DATA_PAGE_BUFFER_HIT_RATIO >= num_stats
      || MNT_STATS_DATA_PAGE_IOREADS >= num_stats)
    {
      assert (false);
      return;
    }

  if (stats[MNT_STATS_DATA_PAGE_FETCHES].value == 0)
    {
      stats[MNT_STATS_DATA_PAGE_BUFFER_HIT_RATIO].value = 100 * 100;
    }
  else
    {
      stats[MNT_STATS_DATA_PAGE_BUFFER_HIT_RATIO].value =
	((stats[MNT_STATS_DATA_PAGE_FETCHES].value
	  - stats[MNT_STATS_DATA_PAGE_IOREADS].value) * 100 * 100)
	/ stats[MNT_STATS_DATA_PAGE_FETCHES].value;
    }
}

/*
 * mnt_csect_type_to_server_item -
 *   return: MNT_SERVER_ITEM
 *
 *   ctype(in):
 */
MNT_SERVER_ITEM
mnt_csect_type_to_server_item (const CSECT_TYPE ctype)
{
  assert (ctype < CSECT_LAST);
  switch (ctype)
    {
    case CSECT_ER_LOG_FILE:	/* 0 */
      return MNT_STATS_CSECT_ER_LOG_FILE;
    case CSECT_ER_MSG_CACHE:	/* 1 */
      return MNT_STATS_CSECT_ER_MSG_CACHE;
    case CSECT_WFG:		/* 2 */
      return MNT_STATS_CSECT_WFG;
    case CSECT_LOG:		/* 3 */
      return MNT_STATS_CSECT_LOG;
    case CSECT_LOG_BUFFER:	/* 4 */
      return MNT_STATS_CSECT_LOG_BUFFER;
    case CSECT_LOG_ARCHIVE:	/* 5 */
      return MNT_STATS_CSECT_LOG_ARCHIVE;
    case CSECT_LOCATOR_SR_CLASSNAME_TABLE:	/* 6 */
      return MNT_STATS_CSECT_SR_LOCATOR_CLASSNAME_TABLE;
    case CSECT_FILE_NEWFILE:	/* 7 */
      return MNT_STATS_CSECT_FILE_NEWFILE;
    case CSECT_QPROC_QUERY_TABLE:	/* 8 */
      return MNT_STATS_CSECT_QPROC_QUERY_TABLE;
    case CSECT_BOOT_SR_DBPARM:	/* 9 */
      return MNT_STATS_CSECT_BOOT_SR_DBPARM;
    case CSECT_DISK_REFRESH_GOODVOL:	/* 10 */
      return MNT_STATS_CSECT_DISK_REFRESH_GOODVOL;
    case CSECT_CNV_FMT_LEXER:	/* 11 */
      return MNT_STATS_CSECT_CNV_FMT_LEXER;
    case CSECT_CT_OID_TABLE:	/* 12 */
      return MNT_STATS_CSECT_CT_OID_TABLE;
    case CSECT_HA_SERVER_STATE:	/* 13 */
      return MNT_STATS_CSECT_HA_SERVER_STATE;
    case CSECT_SESSION_STATE:	/* 14 */
      return MNT_STATS_CSECT_SESSION_STATE;
    case CSECT_ACL:		/* 15 */
      return MNT_STATS_CSECT_ACL;
    case CSECT_EVENT_LOG_FILE:	/* 16 */
      return MNT_STATS_CSECT_EVENT_LOG_FILE;
    case CSECT_ACCESS_STATUS:	/* 17 */
      return MNT_STATS_CSECT_ACCESS_STATUS;
    case CSECT_TEMPFILE_CACHE:	/* 18 */
      return MNT_STATS_CSECT_TEMPFILE_CACHE;
    case CSECT_CSS_ACTIVE_CONN:	/* 19 */
      return MNT_STATS_CSECT_CSS_ACTIVE_CONN;
    case CSECT_CSS_FREE_CONN:	/* 20 */
      return MNT_STATS_CSECT_CSS_FREE_CONN;
    case CSECT_UNKNOWN:	/* 21 */
      assert (false);
      return MNT_STATS_CSECT_UNKNOWN;
    default:
      break;
    }

  assert (false);
  return MNT_STATS_CSECT_UNKNOWN;
}

/*
 * mnt_csect_type_to_server_item_waits -
 *   return: MNT_SERVER_ITEM
 *
 *   ctype(in):
 */
MNT_SERVER_ITEM
mnt_csect_type_to_server_item_waits (const CSECT_TYPE ctype)
{
  assert (ctype < CSECT_LAST);
  switch (ctype)
    {
    case CSECT_ER_LOG_FILE:	/* 0 */
      return MNT_STATS_CSECT_WAITS_ER_LOG_FILE;
    case CSECT_ER_MSG_CACHE:	/* 1 */
      return MNT_STATS_CSECT_WAITS_ER_MSG_CACHE;
    case CSECT_WFG:		/* 2 */
      return MNT_STATS_CSECT_WAITS_WFG;
    case CSECT_LOG:		/* 3 */
      return MNT_STATS_CSECT_WAITS_LOG;
    case CSECT_LOG_BUFFER:	/* 4 */
      return MNT_STATS_CSECT_WAITS_LOG_BUFFER;
    case CSECT_LOG_ARCHIVE:	/* 5 */
      return MNT_STATS_CSECT_WAITS_LOG_ARCHIVE;
    case CSECT_LOCATOR_SR_CLASSNAME_TABLE:	/* 6 */
      return MNT_STATS_CSECT_WAITS_SR_LOCATOR_CLASSNAME_TABLE;
    case CSECT_FILE_NEWFILE:	/* 7 */
      return MNT_STATS_CSECT_WAITS_FILE_NEWFILE;
    case CSECT_QPROC_QUERY_TABLE:	/* 8 */
      return MNT_STATS_CSECT_WAITS_QPROC_QUERY_TABLE;
    case CSECT_BOOT_SR_DBPARM:	/* 9 */
      return MNT_STATS_CSECT_WAITS_BOOT_SR_DBPARM;
    case CSECT_DISK_REFRESH_GOODVOL:	/* 10 */
      return MNT_STATS_CSECT_WAITS_DISK_REFRESH_GOODVOL;
    case CSECT_CNV_FMT_LEXER:	/* 11 */
      return MNT_STATS_CSECT_WAITS_CNV_FMT_LEXER;
    case CSECT_CT_OID_TABLE:	/* 12 */
      return MNT_STATS_CSECT_WAITS_CT_OID_TABLE;
    case CSECT_HA_SERVER_STATE:	/* 13 */
      return MNT_STATS_CSECT_WAITS_HA_SERVER_STATE;
    case CSECT_SESSION_STATE:	/* 14 */
      return MNT_STATS_CSECT_WAITS_SESSION_STATE;
    case CSECT_ACL:		/* 15 */
      return MNT_STATS_CSECT_WAITS_ACL;
    case CSECT_EVENT_LOG_FILE:	/* 16 */
      return MNT_STATS_CSECT_WAITS_EVENT_LOG_FILE;
    case CSECT_ACCESS_STATUS:	/* 17 */
      return MNT_STATS_CSECT_WAITS_ACCESS_STATUS;
    case CSECT_TEMPFILE_CACHE:	/* 18 */
      return MNT_STATS_CSECT_WAITS_TEMPFILE_CACHE;
    case CSECT_CSS_ACTIVE_CONN:	/* 19 */
      return MNT_STATS_CSECT_WAITS_CSS_ACTIVE_CONN;
    case CSECT_CSS_FREE_CONN:	/* 20 */
      return MNT_STATS_CSECT_WAITS_CSS_FREE_CONN;
    case CSECT_UNKNOWN:	/* 21 */
      assert (false);
      return MNT_STATS_CSECT_WAITS_UNKNOWN;
    default:
      break;
    }

  assert (false);
  return MNT_STATS_CSECT_WAITS_UNKNOWN;
}

/*
 * mnt_server_item_fetches_to_page_ptype -
 *   return: PAGE_TYPE
 *
 *   item(in):
 */
PAGE_TYPE
mnt_server_item_fetches_to_page_ptype (const MNT_SERVER_ITEM item)
{
  switch (item)
    {
    case MNT_STATS_DATA_PAGE_FETCHES_FILE_HEADER:	/* 1 file header page             */
      return PAGE_FILE_HEADER;
    case MNT_STATS_DATA_PAGE_FETCHES_FILE_TAB:	/* 2 file allocset table page             */
      return PAGE_FILE_TAB;
    case MNT_STATS_DATA_PAGE_FETCHES_HEAP_HEADER:	/* 3 heap header page                            */
      return PAGE_HEAP_HEADER;
    case MNT_STATS_DATA_PAGE_FETCHES_HEAP:	/* 4 heap page                            */
      return PAGE_HEAP;
    case MNT_STATS_DATA_PAGE_FETCHES_VOLHEADER:	/* 5 volume header page                   */
      return PAGE_VOLHEADER;
    case MNT_STATS_DATA_PAGE_FETCHES_VOLBITMAP:	/* 6 volume bitmap page                   */
      return PAGE_VOLBITMAP;
    case MNT_STATS_DATA_PAGE_FETCHES_XASL:	/* 7 xasl stream page                     */
      return PAGE_XASL;
    case MNT_STATS_DATA_PAGE_FETCHES_QRESULT:	/* 8 query result page                    */
      return PAGE_QRESULT;
    case MNT_STATS_DATA_PAGE_FETCHES_EHASH:	/* 9 ehash bucket/dir page                */
      return PAGE_EHASH;
    case MNT_STATS_DATA_PAGE_FETCHES_OVERFLOW:	/* 10 overflow page (with ovf_keyval)      */
      return PAGE_OVERFLOW;
    case MNT_STATS_DATA_PAGE_FETCHES_AREA:	/* 11 area page                            */
      return PAGE_AREA;
    case MNT_STATS_DATA_PAGE_FETCHES_CATALOG:	/* 12 catalog page                         */
      return PAGE_CATALOG;
    case MNT_STATS_DATA_PAGE_FETCHES_BTREE_ROOT:	/* 13 b+tree index root page                    */
      return PAGE_BTREE_ROOT;
    case MNT_STATS_DATA_PAGE_FETCHES_BTREE:	/* 14 b+tree index page                    */
      return PAGE_BTREE;
    case MNT_STATS_DATA_PAGE_FETCHES_UNKNOWN:	/* 0 unknown                     */
      return PAGE_UNKNOWN;
    default:
      break;
    }

  assert (false);
  return PAGE_UNKNOWN;
}

/*
 * mnt_page_ptype_to_server_item_fetches -
 *   return: MNT_SERVER_ITEM
 *
 *   ptype(in):
 */
MNT_SERVER_ITEM
mnt_page_ptype_to_server_item_fetches (const PAGE_TYPE ptype)
{
  assert (ptype < PAGE_LAST);
  switch (ptype)
    {
    case PAGE_FILE_HEADER:	/* 1 file header page                     */
      return MNT_STATS_DATA_PAGE_FETCHES_FILE_HEADER;
    case PAGE_FILE_TAB:	/* 2 file allocset table page             */
      return MNT_STATS_DATA_PAGE_FETCHES_FILE_TAB;
    case PAGE_HEAP_HEADER:	/* 3 heap header page               */
      return MNT_STATS_DATA_PAGE_FETCHES_HEAP_HEADER;
    case PAGE_HEAP:		/* 4 heap page                            */
      return MNT_STATS_DATA_PAGE_FETCHES_HEAP;
    case PAGE_VOLHEADER:	/* 5 volume header page                   */
      return MNT_STATS_DATA_PAGE_FETCHES_VOLHEADER;
    case PAGE_VOLBITMAP:	/* 6 volume bitmap page                   */
      return MNT_STATS_DATA_PAGE_FETCHES_VOLBITMAP;
    case PAGE_XASL:		/* 7 xasl stream page                     */
      return MNT_STATS_DATA_PAGE_FETCHES_XASL;
    case PAGE_QRESULT:		/* 8 query result page                    */
      return MNT_STATS_DATA_PAGE_FETCHES_QRESULT;
    case PAGE_EHASH:		/* 9 ehash bucket/dir page                */
      return MNT_STATS_DATA_PAGE_FETCHES_EHASH;
    case PAGE_OVERFLOW:	/* 10 overflow page                        */
      return MNT_STATS_DATA_PAGE_FETCHES_OVERFLOW;
    case PAGE_AREA:		/* 11 area page                            */
      return MNT_STATS_DATA_PAGE_FETCHES_AREA;
    case PAGE_CATALOG:		/* 12 catalog page                         */
      return MNT_STATS_DATA_PAGE_FETCHES_CATALOG;
    case PAGE_BTREE_ROOT:	/* 13 b+tree index root page               */
      return MNT_STATS_DATA_PAGE_FETCHES_BTREE_ROOT;
    case PAGE_BTREE:		/* 14 b+tree index page                    */
      return MNT_STATS_DATA_PAGE_FETCHES_BTREE;
    case PAGE_UNKNOWN:		/* 0 used for initialized page            */
      return MNT_STATS_DATA_PAGE_FETCHES_UNKNOWN;
    default:
      break;
    }

  assert (false);
  return MNT_STATS_DATA_PAGE_FETCHES_UNKNOWN;
}

/*
 * mnt_page_ptype_to_server_item_fetches_waits -
 *   return: MNT_SERVER_ITEM
 *
 *   ptype(in):
 */
MNT_SERVER_ITEM
mnt_page_ptype_to_server_item_fetches_waits (const PAGE_TYPE ptype)
{
  assert (ptype < PAGE_LAST);
  switch (ptype)
    {
    case PAGE_FILE_HEADER:	/* 1 file header page                     */
      return MNT_STATS_DATA_PAGE_FETCHES_WAITS_FILE_HEADER;
    case PAGE_FILE_TAB:	/* 2 file allocset table page             */
      return MNT_STATS_DATA_PAGE_FETCHES_WAITS_FILE_TAB;
    case PAGE_HEAP_HEADER:	/* 3 heap header page               */
      return MNT_STATS_DATA_PAGE_FETCHES_WAITS_HEAP_HEADER;
    case PAGE_HEAP:		/* 4 heap page                            */
      return MNT_STATS_DATA_PAGE_FETCHES_WAITS_HEAP;
    case PAGE_VOLHEADER:	/* 5 volume header page                   */
      return MNT_STATS_DATA_PAGE_FETCHES_WAITS_VOLHEADER;
    case PAGE_VOLBITMAP:	/* 6 volume bitmap page                   */
      return MNT_STATS_DATA_PAGE_FETCHES_WAITS_VOLBITMAP;
    case PAGE_XASL:		/* 7 xasl stream page                     */
      return MNT_STATS_DATA_PAGE_FETCHES_WAITS_XASL;
    case PAGE_QRESULT:		/* 8 query result page                    */
      return MNT_STATS_DATA_PAGE_FETCHES_WAITS_QRESULT;
    case PAGE_EHASH:		/* 9 ehash bucket/dir page                */
      return MNT_STATS_DATA_PAGE_FETCHES_WAITS_EHASH;
    case PAGE_OVERFLOW:	/* 10 overflow page                        */
      return MNT_STATS_DATA_PAGE_FETCHES_WAITS_OVERFLOW;
    case PAGE_AREA:		/* 11 area page                            */
      return MNT_STATS_DATA_PAGE_FETCHES_WAITS_AREA;
    case PAGE_CATALOG:		/* 12 catalog page                         */
      return MNT_STATS_DATA_PAGE_FETCHES_WAITS_CATALOG;
    case PAGE_BTREE_ROOT:	/* 13 b+tree index root page               */
      return MNT_STATS_DATA_PAGE_FETCHES_WAITS_BTREE_ROOT;
    case PAGE_BTREE:		/* 14 b+tree index page                    */
      return MNT_STATS_DATA_PAGE_FETCHES_WAITS_BTREE;
    case PAGE_UNKNOWN:		/* 0 used for initialized page            */
      return MNT_STATS_DATA_PAGE_FETCHES_WAITS_UNKNOWN;
    default:
      break;
    }

  assert (false);
  return MNT_STATS_DATA_PAGE_FETCHES_WAITS_UNKNOWN;
}

/*
 * mnt_clock_to_time -
 *   return:
 *
 *   acc_time(in):
 */
UINT64
mnt_clock_to_time (const UINT64 acc_time)
{
  /* TODO - under construction
   *
   */

  return acc_time;
}
