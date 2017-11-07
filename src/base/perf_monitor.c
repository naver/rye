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
#include "network_interface_cl.h"
#include "error_manager.h"
#include "rye_server_shm.h"

#include "thread.h"
#include "log_impl.h"

#if !defined(CS_MODE)
#include "xserver_interface.h"
#endif /* !CS_MODE */

typedef enum
{
  MNT_STATS_VALUE_COUNTER,
  MNT_STATS_VALUE_COUNTER_WITH_TIME,
  MNT_STATS_VALUE_GAUGE,
  MNT_STATS_VALUE_EVENT
} MNT_STATS_VALUE_TYPE;

#define IS_CUMMULATIVE_VALUE(TYPE)	\
	((TYPE) == MNT_STATS_VALUE_COUNTER || (TYPE) == MNT_STATS_VALUE_COUNTER_WITH_TIME)
#define IS_COLLECTING_TIME(TYPE)	\
	((TYPE) == MNT_STATS_VALUE_COUNTER_WITH_TIME)

typedef struct
{
  const char *name;
  MNT_STATS_VALUE_TYPE value_type;
} MNT_EXEC_STATS_INFO;

static MNT_EXEC_STATS_INFO mnt_Stats_info[MNT_SIZE_OF_SERVER_EXEC_STATS] = {
  /* MNT_STATS_DISK_SECTOR_ALLOCS */
  {"Num_disk_sector_allocs", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_DISK_SECTOR_DEALLOCS */
  {"Num_disk_sector_deallocs", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_DISK_PAGE_ALLOCS */
  {"Num_disk_page_allocs", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_DISK_PAGE_DEALLOCS */
  {"Num_disk_page_deallocs", MNT_STATS_VALUE_COUNTER_WITH_TIME},

  /* MNT_STATS_FILE_CREATES */
  {"Num_file_creates", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_FILE_REMOVES */
  {"Num_file_removes", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_FILE_IOREADS */
  {"Num_file_ioreads", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_FILE_IOWRITES */
  {"Num_file_iowrites", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_FILE_IOSYNCHES */
  {"Num_file_iosynches", MNT_STATS_VALUE_COUNTER},

  /* MNT_STATS_DATA_PAGE_FETCHES */
  {"Num_data_page_fetches", MNT_STATS_VALUE_COUNTER_WITH_TIME},
#if 1				/* fetches sub-info */
  {"Num_data_page_fetches_file_header", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  {"Num_data_page_fetches_file_table", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  {"Num_data_page_fetches_heap_header", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  {"Num_data_page_fetches_heap", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  {"Num_data_page_fetches_heap_relocation",
   MNT_STATS_VALUE_COUNTER_WITH_TIME},
  {"Num_data_page_fetches_heap_bestspace_sync",
   MNT_STATS_VALUE_COUNTER_WITH_TIME},
  {"Num_data_page_fetches_vol_header", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  {"Num_data_page_fetches_vol_bitmap", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  {"Num_data_page_fetches_xasl", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  {"Num_data_page_fetches_qresult", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  {"Num_data_page_fetches_ehash", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  {"Num_data_page_fetches_largeobj", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  {"Num_data_page_fetches_overflow", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  {"Num_data_page_fetches_area", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  {"Num_data_page_fetches_catalog", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  {"Num_data_page_fetches_catalog_ovf", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  {"Num_data_page_fetches_btree", MNT_STATS_VALUE_COUNTER_WITH_TIME},

  {"Num_data_page_fetches_log_postpone", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  {"Num_data_page_fetches_disk_format", MNT_STATS_VALUE_COUNTER_WITH_TIME},
#endif

  /* MNT_STATS_DATA_PAGE_DIRTIES */
  {"Num_data_page_dirties", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_DATA_PAGE_IOREADS */
  {"Num_data_page_ioreads", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_DATA_PAGE_IOWRITES */
  {"Num_data_page_iowrites", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_DATA_PAGE_VICTIMS */
  {"Num_data_page_victims", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_DATA_PAGE_IOWRITES_FOR_REPLACEMENT */
  {"Num_data_page_iowrites_for_replacement", MNT_STATS_VALUE_COUNTER},

  /* MNT_STATS_LOG_PAGE_IOREADS */
  {"Num_log_page_ioreads", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_LOG_PAGE_IOWRITES */
  {"Num_log_page_iowrites", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_LOG_APPEND_RECORDS */
  {"Num_log_append_records", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_LOG_ARCHIVES */
  {"Num_log_archives", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_LOG_CHECKPOINTS */
  {"Num_log_checkpoints", MNT_STATS_VALUE_EVENT},
  /* MNT_STATS_LOG_WALS */
  {"Num_log_wals", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_DDL_LOCKS_REQUESTS */
  {"Num_ddl_locks_requests", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_CLASS_LOCKS_REQUEST */
  {"Num_class_locks_request", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_CATALOG_LOCKS_REQUEST */
  {"Num_catalog_locks_request", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_GLOBAL_LOCKS_REQUEST */
  {"Num_global_locks_request", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_SHARD_LOCKS_REQUEST */
  {"Num_shard_locks_request", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_PAGE_LOCKS_ACQUIRED */
  {"Num_page_locks_acquired", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_OBJECT_LOCKS_ACQUIRED */
  {"Num_object_locks_acquired", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_PAGE_LOCKS_CONVERTED */
  {"Num_page_locks_converted", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_OBJECT_LOCKS_CONVERTED */
  {"Num_object_locks_converted", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_PAGE_LOCKS_RE_REQUESTED */
  {"Num_page_locks_re-requested", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_OBJECT_LOCKS_RE_REQUESTED */
  {"Num_object_locks_re-requested", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_PAGE_LOCKS_WAITS */
  {"Num_page_locks_waits", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_OBJECT_LOCKS_WAITS */
  {"Num_object_locks_waits", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_TRAN_COMMITS */
  {"Num_tran_commits", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_TRAN_ROLLBACKS */
  {"Num_tran_rollbacks", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_TRAN_SAVEPOINTS */
  {"Num_tran_savepoints", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_TRAN_TOPOPS */
  {"Num_tran_topops", MNT_STATS_VALUE_EVENT},
  /* MNT_STATS_TRAN_INTERRUPTS */
  {"Num_tran_interrupts", MNT_STATS_VALUE_COUNTER},

  /* MNT_STATS_BTREE_INSERTS */
  {"Num_btree_inserts", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_BTREE_DELETES */
  {"Num_btree_deletes", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_BTREE_UPDATES */
  {"Num_btree_updates", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_BTREE_LOAD_DATA */
  {"Num_btree_load_data", MNT_STATS_VALUE_EVENT},
  /* MNT_STATS_BTREE_COVERED */
  {"Num_btree_covered", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_BTREE_NONCOVERED */
  {"Num_btree_noncovered", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_BTREE_RESUMES */
  {"Num_btree_resumes", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_BTREE_MULTIRANGE_OPTIMIZATION */
  {"Num_btree_multirange_optimization", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_BTREE_SPLITS */
  {"Num_btree_splits", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_BTREE_MERGES */
  {"Num_btree_merges", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_BTREE_PAGE_ALLOCS */
  {"Num_btree_page_allocs", MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_BTREE_PAGE_DEALLOCS */
  {"Num_btree_page_deallocs", MNT_STATS_VALUE_COUNTER_WITH_TIME},

  /* MNT_STATS_QUERY_SELECTS */
  {"Num_query_selects", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_QUERY_INSERTS */
  {"Num_query_inserts", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_QUERY_DELETES */
  {"Num_query_deletes", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_QUERY_UPDATES */
  {"Num_query_updates", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_QUERY_SSCANS */
  {"Num_query_sscans", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_QUERY_ISCANS */
  {"Num_query_iscans", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_QUERY_LSCANS */
  {"Num_query_lscans", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_QUERY_METHSCANS */
  {"Num_query_methscans", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_QUERY_NLJOINS */
  {"Num_query_nljoins", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_QUERY_MJOINS */
  {"Num_query_mjoins", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_QUERY_OBJFETCHES */
  {"Num_query_objfetches", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_QUERY_HOLDABLE_CURSORS */
  {"Num_query_holdable_cursors", MNT_STATS_VALUE_GAUGE},
  /* MNT_STATS_SORT_IO_PAGES */
  {"Num_sort_io_pages", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_SORT_DATA_PAGES */
  {"Num_sort_data_pages", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_NETWORK_REQUESTS */
  {"Num_network_requests", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_ADAPTIVE_FLUSH_PAGES */
  {"Num_adaptive_flush_pages", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_ADAPTIVE_FLUSH_LOG_PAGES */
  {"Num_adaptive_flush_log_pages", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_ADAPTIVE_FLUSH_MAX_PAGES */
  {"Num_adaptive_flush_max_pages", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_PRIOR_LSA_LIST_SIZE */
  {"Num_prior_lsa_list_size", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_PRIOR_LSA_LIST_MAXED */
  {"Num_prior_lsa_list_maxed", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_PRIOR_LSA_LIST_REMOVED */
  {"Num_prior_lsa_list_removed", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_HEAP_STATS_BESTSPACE_ENTRIES */
  {"Num_heap_stats_bestspace_entries", MNT_STATS_VALUE_GAUGE},
  /* MNT_STATS_HEAP_STATS_BESTSPACE_MAXED */
  {"Num_heap_stats_bestspace_maxed", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_HA_LAST_FLUSHED_PAGEID */
  {"Num_ha_last_flushed_pageid", MNT_STATS_VALUE_GAUGE},
  /* MNT_STATS_HA_EOF_PAGEID */
  {"Num_ha_eof_pageid", MNT_STATS_VALUE_GAUGE},
  /* MNT_STATS_HA_CURRENT_PAGEID */
  {"Num_ha_current_pageid", MNT_STATS_VALUE_GAUGE},
  /* MNT_STATS_HA_REQUIRED_PAGEID */
  {"Num_ha_required_pageid", MNT_STATS_VALUE_GAUGE},
  /* MNT_STATS_HA_REPLICATION_DELAY */
  {"Time_ha_replication_delay", MNT_STATS_VALUE_GAUGE},
  /* MNT_STATS_PLAN_CACHE_ADD */
  {"Num_plan_cache_add", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_PLAN_CACHE_LOOKUP */
  {"Num_plan_cache_lookup", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_PLAN_CACHE_HIT */
  {"Num_plan_cache_hit", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_PLAN_CACHE_MISS */
  {"Num_plan_cache_miss", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_PLAN_CACHE_FULL */
  {"Num_plan_cache_full", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_PLAN_CACHE_DELETE */
  {"Num_plan_cache_delete", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_PLAN_CACHE_INVALID_XASL_ID */
  {"Num_plan_cache_invalid_xasl_id", MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_PLAN_CACHE_QUERY_STRING_HASH_ENTRIES */
  {"Num_plan_cache_query_string_hash_entries", MNT_STATS_VALUE_GAUGE},
  /* MNT_STATS_PLAN_CACHE_XASL_ID_HASH_ENTRIES */
  {"Num_plan_cache_xasl_id_hash_entries", MNT_STATS_VALUE_GAUGE},
  /* MNT_STATS_PLAN_CACHE_CLASS_OID_HASH_ENTRIES */
  {"Num_plan_cache_class_oid_hash_entries", MNT_STATS_VALUE_GAUGE},
  /* MNT_STATS_DATA_PAGE_BUFFER_HIT_RATIO */
  {"Data_page_buffer_hit_ratio", MNT_STATS_VALUE_COUNTER}
};

#if defined(CS_MODE) || defined(SA_MODE)
/* Client execution statistic structure */
typedef struct mnt_client_stat_info MNT_CLIENT_STAT_INFO;
struct mnt_client_stat_info
{
  time_t cpu_start_usr_time;
  time_t cpu_start_sys_time;
  time_t elapsed_start_time;
  MNT_SERVER_EXEC_STATS base_server_stats;
  MNT_SERVER_EXEC_STATS old_global_stats;
};

static MNT_CLIENT_STAT_INFO mnt_Stat_info;
#endif

#if defined(CS_MODE) || defined(SA_MODE)
static void mnt_server_dump_stats (const MNT_SERVER_EXEC_STATS * stats,
				   FILE * stream, const char *substr);
static void mnt_get_current_times (time_t * cpu_usr_time,
				   time_t * cpu_sys_time,
				   time_t * elapsed_time);
#endif
static void mnt_calc_hit_ratio (MNT_SERVER_EXEC_STATS * stats);

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
  MNT_SERVER_EXEC_STATS cur_server_stats;

  mnt_get_current_times (&mnt_Stat_info.cpu_start_usr_time,
			 &mnt_Stat_info.cpu_start_sys_time,
			 &mnt_Stat_info.elapsed_start_time);

  mnt_server_copy_stats (&cur_server_stats);
  mnt_Stat_info.base_server_stats = cur_server_stats;
}

/*
 * mnt_print_stats - Print the current client statistics
 *   return:
 *   stream(in): if NULL is given, stdout is used
 */
void
mnt_print_stats (FILE * stream)
{
  MNT_SERVER_EXEC_STATS diff_result;
  MNT_SERVER_EXEC_STATS cur_server_stats;
  time_t cpu_total_usr_time;
  time_t cpu_total_sys_time;
  time_t elapsed_total_time;

  if (stream == NULL)
    {
      stream = stdout;
    }

  if (mnt_server_copy_stats (&cur_server_stats) == NO_ERROR)
    {
      mnt_get_current_times (&cpu_total_usr_time, &cpu_total_sys_time,
			     &elapsed_total_time);

      fprintf (stream, "\n *** CLIENT EXECUTION STATISTICS ***\n");

      fprintf (stream, "System CPU (sec)              = %10d\n",
	       (int) (cpu_total_sys_time - mnt_Stat_info.cpu_start_sys_time));
      fprintf (stream, "User CPU (sec)                = %10d\n",
	       (int) (cpu_total_usr_time - mnt_Stat_info.cpu_start_usr_time));
      fprintf (stream, "Elapsed (sec)                 = %10d\n",
	       (int) (elapsed_total_time - mnt_Stat_info.elapsed_start_time));

      if (mnt_diff_stats (&diff_result, &cur_server_stats,
			  &mnt_Stat_info.base_server_stats) == NO_ERROR)
	{
	  mnt_server_dump_stats (&diff_result, stream, NULL);
	}
    }
}

/*
 * mnt_print_global_stats - Print the global statistics
 *   return:
 *   stream(in): if NULL is given, stdout is used
 */
void
mnt_print_global_stats (FILE * stream, bool cumulative, const char *substr,
			const char *dbname)
{
  MNT_SERVER_EXEC_STATS diff_result;
  MNT_SERVER_EXEC_STATS cur_global_stats;
  int err = NO_ERROR;

  if (stream == NULL)
    {
      stream = stdout;
    }

  if (dbname == NULL)
    {
      err = mnt_server_copy_global_stats (&cur_global_stats);
    }
  else
    {
      err = rye_server_shm_get_global_stats (&cur_global_stats, dbname);
    }

  if (err == NO_ERROR)
    {
      if (cumulative)
	{
	  mnt_server_dump_stats (&cur_global_stats, stream, substr);
	}
      else
	{
	  if (mnt_diff_stats (&diff_result,
			      &cur_global_stats,
			      &mnt_Stat_info.old_global_stats) == NO_ERROR)
	    {
	      mnt_server_dump_stats (&diff_result, stream, substr);
	    }
	  mnt_Stat_info.old_global_stats = cur_global_stats;
	}
    }
}

/*
 * mnt_server_dump_stats - Print the given server statistics
 *   return: none
 *   stats(in) server statistics to print
 *   stream(in): if NULL is given, stdout is used
 */
static void
mnt_server_dump_stats (const MNT_SERVER_EXEC_STATS * stats, FILE * stream,
		       const char *substr)
{
  unsigned int i;
  const char *s;

  if (stream == NULL)
    {
      stream = stdout;
    }

  fprintf (stream, "\n *** SERVER EXECUTION STATISTICS *** \n");

  for (i = 0; i < MNT_SIZE_OF_SERVER_EXEC_STATS - 1; i++)
    {
      if (substr != NULL)
	{
	  s = strstr (mnt_Stats_info[i].name, substr);
	}
      else
	{
	  s = mnt_Stats_info[i].name;
	}
      if (s)
	{
	  fprintf (stream, "%-29s = %10lld\n", mnt_Stats_info[i].name,
		   (long long) stats->values[i]);
	}
    }

  fprintf (stream, "\n *** OTHER STATISTICS *** \n");

  fprintf (stream, "Data_page_buffer_hit_ratio    = %10.2f\n",
	   (float) stats->values[MNT_STATS_DATA_PAGE_BUFFER_HIT_RATIO] / 100);
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
 * xmnt_server_copy_stats - Copy recorded server statistics for the current
 *                          transaction index
 *   return: none
 *   to_stats(out): buffer to copy
 */
void
xmnt_server_copy_stats (THREAD_ENTRY * thread_p,
			MNT_SERVER_EXEC_STATS * to_stats)
{
  int tran_index;

  assert (to_stats != NULL);

  tran_index = logtb_get_current_tran_index (thread_p);
  assert (tran_index >= 0);

  svr_shm_copy_stats (tran_index, to_stats);
}

/*
 * xmnt_server_copy_global_stats - Copy recorded system wide statistics
 *   return: none
 *   to_stats(out): buffer to copy
 */
void
xmnt_server_copy_global_stats (UNUSED_ARG THREAD_ENTRY * thread_p,
			       MNT_SERVER_EXEC_STATS * to_stats)
{
  assert (to_stats != NULL);

  svr_shm_copy_global_stats (to_stats);
}

/*
 * mnt_stats_counter -
 */
void
mnt_stats_counter (THREAD_ENTRY * thread_p, MNT_SERVER_ITEM item, INT64 value)
{
  int tran_index;

  assert (item < MNT_SIZE_OF_SERVER_EXEC_STATS);
  assert (mnt_Stats_info[item].value_type == MNT_STATS_VALUE_COUNTER ||
	  mnt_Stats_info[item].value_type == MNT_STATS_VALUE_EVENT);

  tran_index = logtb_get_current_tran_index (thread_p);
  svr_shm_stats_counter (tran_index, item, value, 0);
}

/*
 * mnt_stats_counter_with_time -
 */
void
mnt_stats_counter_with_time (THREAD_ENTRY * thread_p, MNT_SERVER_ITEM item,
			     INT64 value, UINT64 start_time)
{
  int tran_index;
  UINT64 end_time;

  assert (item < MNT_SIZE_OF_SERVER_EXEC_STATS);
  assert (mnt_Stats_info[item].value_type ==
	  MNT_STATS_VALUE_COUNTER_WITH_TIME);

  PERF_MON_GET_CURRENT_TIME (end_time);

  tran_index = logtb_get_current_tran_index (thread_p);
  svr_shm_stats_counter (tran_index, item, value, end_time - start_time);
}

/*
 * mnt_stats_gauge -
 */
void
mnt_stats_gauge (THREAD_ENTRY * thread_p, MNT_SERVER_ITEM item, INT64 value)
{
  int tran_index;

  assert (item < MNT_SIZE_OF_SERVER_EXEC_STATS);
  assert (mnt_Stats_info[item].value_type == MNT_STATS_VALUE_GAUGE);

  tran_index = logtb_get_current_tran_index (thread_p);
  svr_shm_stats_gauge (tran_index, item, value);
}

/*
 * mnt_get_stats -
 */
INT64
mnt_get_stats (THREAD_ENTRY * thread_p, MNT_SERVER_ITEM item)
{
  int tran_index;

  tran_index = logtb_get_current_tran_index (thread_p);
  assert (tran_index >= 0);

  return svr_shm_get_stats (tran_index, item);
}

/*
 * mnt_server_dump_stats_to_buffer -
 *   return: none
 *   stats(in) server statistics to print
 *   buffer(in):
 *   buf_size(in):
 *   substr(in):
 */
void
mnt_server_dump_stats_to_buffer (const MNT_SERVER_EXEC_STATS * stats,
				 char *buffer, int buf_size,
				 const char *substr)
{
  unsigned int i;
  int ret;
  int remained_size;
  const char *s;
  char *p;

  if (buffer == NULL || buf_size <= 0)
    {
      return;
    }

  p = buffer;
  remained_size = buf_size - 1;
  ret =
    snprintf (p, remained_size, "\n *** SERVER EXECUTION STATISTICS *** \n");
  remained_size -= ret;
  p += ret;

  if (remained_size <= 0)
    {
      return;
    }

  for (i = 0; i < MNT_SIZE_OF_SERVER_EXEC_STATS - 1; i++)
    {
      if (substr != NULL)
	{
	  s = strstr (mnt_Stats_info[i].name, substr);
	}
      else
	{
	  s = mnt_Stats_info[i].name;
	}
      if (s)
	{
	  ret = snprintf (p, remained_size, "%-29s = %10lld\n",
			  mnt_Stats_info[i].name,
			  (long long) stats->values[i]);
	  remained_size -= ret;
	  p += ret;
	  if (remained_size <= 0)
	    {
	      return;
	    }
	}
    }

  snprintf (p, remained_size, "\n *** OTHER STATISTICS *** \n"
	    "Data_page_buffer_hit_ratio    = %10.2f\n",
	    (float) stats->values[MNT_STATS_DATA_PAGE_BUFFER_HIT_RATIO] /
	    100);
  buffer[buf_size - 1] = '\0';
}
#endif /* SERVER_MODE || SA_MODE */

/*
 * mnt_calc_hit_ratio - Do post processing of server statistics
 *   return: none
 *   stats(in/out): server statistics block to be processed
 */
static void
mnt_calc_hit_ratio (MNT_SERVER_EXEC_STATS * stats)
{
  if (stats->values[MNT_STATS_DATA_PAGE_FETCHES] == 0)
    {
      stats->values[MNT_STATS_DATA_PAGE_BUFFER_HIT_RATIO] = 100 * 100;
    }
  else
    {
      stats->values[MNT_STATS_DATA_PAGE_BUFFER_HIT_RATIO] =
	(stats->values[MNT_STATS_DATA_PAGE_FETCHES] -
	 stats->values[MNT_STATS_DATA_PAGE_IOREADS]) * 100 * 100 /
	stats->values[MNT_STATS_DATA_PAGE_FETCHES];
    }
}

/*
 * mnt_diff_stats -
 */
int
mnt_diff_stats (MNT_SERVER_EXEC_STATS * diff_stats,
		MNT_SERVER_EXEC_STATS * new_stats,
		MNT_SERVER_EXEC_STATS * old_stats)
{
  int i;

  if (!diff_stats || !new_stats || !old_stats)
    {
      assert (0);
      return ER_FAILED;
    }

  for (i = 0; i < MNT_SIZE_OF_SERVER_EXEC_STATS; i++)
    {
      if (IS_CUMMULATIVE_VALUE (mnt_Stats_info[i].value_type))
	{
	  if (new_stats->values[i] >= old_stats->values[i])
	    {
	      diff_stats->values[i] =
		(new_stats->values[i] - old_stats->values[i]);
	    }
	  else
	    {
	      diff_stats->values[i] = 0;
	    }
	}
      else
	{
	  diff_stats->values[i] = new_stats->values[i];
	}
    }

  mnt_calc_hit_ratio (diff_stats);

  return NO_ERROR;
}

/*
 * mnt_stats_is_cumulative: -
 */
bool
mnt_stats_is_cumulative (MNT_SERVER_ITEM item)
{
  if (item < MNT_SIZE_OF_SERVER_EXEC_STATS)
    {
      return (IS_CUMMULATIVE_VALUE (mnt_Stats_info[item].value_type));
    }
  else
    {
      assert (0);
      return false;
    }
}

/*
 * mnt_stats_is_collecting_time: -
 */
bool
mnt_stats_is_collecting_time (MNT_SERVER_ITEM item)
{
  if (item < MNT_SIZE_OF_SERVER_EXEC_STATS)
    {
      return (IS_COLLECTING_TIME (mnt_Stats_info[item].value_type));
    }
  else
    {
      assert (0);
      return false;
    }
}
