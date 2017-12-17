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

typedef enum
{
  MNT_STATS_VALUE_COUNTER,
  MNT_STATS_VALUE_COUNTER_WITH_TIME,
  MNT_STATS_VALUE_GAUGE,
  MNT_STATS_VALUE_EVENT
} MNT_STATS_VALUE_TYPE;

typedef struct
{
  const char *name;
  const int level;
  MNT_STATS_VALUE_TYPE value_type;
} MNT_EXEC_STATS_INFO;

#if 0
static MNT_EXEC_STATS_INFO mnt_Stats_info[MNT_SIZE_OF_SERVER_EXEC_STATS] = {
  /* MNT_STATS_SQL_TRACE_LOCK_WAITS */
  {"Num_sql_trace_lock_waits", 0, MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_SQL_TRACE_LATCH_WAITS */
  {"Num_sql_trace_latch_waits", 0, MNT_STATS_VALUE_COUNTER_WITH_TIME},

#if 1				/* csect sub-info */
  {"Num_csect_er_log_file", 0, MNT_STATS_VALUE_COUNTER},	/* 0 */
  {"Num_csect_er_msg_cache", 0, MNT_STATS_VALUE_COUNTER},	/* 1 */
  {"Num_csect_wfg", 0, MNT_STATS_VALUE_COUNTER},	/* 2 */
  {"Num_csect_log", 0, MNT_STATS_VALUE_COUNTER},	/* 3 */
  {"Num_csect_log_buffer", 0, MNT_STATS_VALUE_COUNTER},	/* 4 */
  {"Num_csect_log_archive", 0, MNT_STATS_VALUE_COUNTER},	/* 5 */
  {"Num_csect_sr_locator_classname_table", 0, MNT_STATS_VALUE_COUNTER},	/* 6 */
  {"Num_csect_file_newfile", 0, MNT_STATS_VALUE_COUNTER},	/* 7 */
  {"Num_csect_qproc_query_table", 0, MNT_STATS_VALUE_COUNTER},	/* 8 */
  {"Num_csect_boot_sr_dbparm", 0, MNT_STATS_VALUE_COUNTER},	/* 9 */
  {"Num_csect_disk_refresh_goodvol", 0, MNT_STATS_VALUE_COUNTER},	/* 10 */
  {"Num_csect_cnf_fmt_lexer", 0, MNT_STATS_VALUE_COUNTER},	/* 11 */
  {"Num_csect_ct_oid_table", 0, MNT_STATS_VALUE_COUNTER},	/* 12 */
  {"Num_csect_ha_server_state", 0, MNT_STATS_VALUE_COUNTER},	/* 13 */
  {"Num_csect_session_state", 0, MNT_STATS_VALUE_COUNTER},	/* 14 */
  {"Num_csect_acl", 0, MNT_STATS_VALUE_COUNTER},	/* 15 */
  {"Num_csect_event_log_file", 0, MNT_STATS_VALUE_COUNTER},	/* 16 */
  {"Num_csect_access_status", 0, MNT_STATS_VALUE_COUNTER},	/* 17 */
  {"Num_csect_tempfile_cache", 0, MNT_STATS_VALUE_COUNTER},	/* 18 */
  {"Num_csect_css_active_conn", 0, MNT_STATS_VALUE_COUNTER},	/* 19 */
  {"Num_csect_css_free_conn", 0, MNT_STATS_VALUE_COUNTER},	/* 20 */
  {"Num_csect_unknown", 0, MNT_STATS_VALUE_COUNTER},	/* 21 */

  {"Num_csect_waits_er_log_file", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 0 */
  {"Num_csect_waits_er_msg_cache", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 1 */
  {"Num_csect_waits_wfg", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 2 */
  {"Num_csect_waits_log", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 3 */
  {"Num_csect_waits_log_buffer", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 4 */
  {"Num_csect_waits_log_archive", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 5 */
  {"Num_csect_waits_sr_locator_classname_table", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 6 */
  {"Num_csect_waits_file_newfile", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 7 */
  {"Num_csect_waits_qproc_query_table", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 8 */
  {"Num_csect_waits_boot_sr_dbparm", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 9 */
  {"Num_csect_waits_disk_refresh_goodvol", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 10 */
  {"Num_csect_waits_cnf_fmt_lexer", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 11 */
  {"Num_csect_waits_ct_oid_table", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 12 */
  {"Num_csect_waits_ha_server_state", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 13 */
  {"Num_csect_waits_session_state", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 14 */
  {"Num_csect_waits_acl", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 15 */
  {"Num_csect_waits_event_log_file", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 16 */
  {"Num_csect_waits_access_status", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 17 */
  {"Num_csect_waits_tempfile_cache", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 18 */
  {"Num_csect_waits_css_active_conn", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 19 */
  {"Num_csect_waits_css_free_conn", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 20 */
  {"Num_csect_waits_unknown", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 21 */
#endif

  /* MNT_STATS_DISK_SECTOR_ALLOCS */
  {"Num_disk_sector_allocs", 0, MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_DISK_SECTOR_DEALLOCS */
  {"Num_disk_sector_deallocs", 0, MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_DISK_PAGE_ALLOCS */
  {"Num_disk_page_allocs", 0, MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_DISK_PAGE_DEALLOCS */
  {"Num_disk_page_deallocs", 0, MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_DISK_TEMP_EXPAND */
  {"Num_disk_temp_expand", 0, MNT_STATS_VALUE_COUNTER_WITH_TIME},

  /* MNT_STATS_FILE_CREATES */
  {"Num_file_creates", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_FILE_REMOVES */
  {"Num_file_removes", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_FILE_IOREADS */
  {"Num_file_ioreads", 0, MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_FILE_IOWRITES */
  {"Num_file_iowrites", 0, MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_FILE_IOSYNCHES */
  {"Num_file_iosynches", 0, MNT_STATS_VALUE_COUNTER},

  /* MNT_STATS_DATA_PAGE_FETCHES */
  {"Num_data_page_fetches", 0, MNT_STATS_VALUE_COUNTER_WITH_TIME},
#if 1				/* fetches sub-info */
  {"Num_data_page_fetches_file_header", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 1 */
  {"Num_data_page_fetches_file_tab", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 2 */
  {"Num_data_page_fetches_heap_header", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 3 */
  {"Num_data_page_fetches_heap", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 4 */
  {"Num_data_page_fetches_volheader", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 5 */
  {"Num_data_page_fetches_volbitmap", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 6 */
  {"Num_data_page_fetches_xasl", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 7 */
  {"Num_data_page_fetches_qresult", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 8 */
  {"Num_data_page_fetches_ehash", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 9 */
  {"Num_data_page_fetches_overflow", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 10 */
  {"Num_data_page_fetches_area", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 11 */
  {"Num_data_page_fetches_catalog", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 12 */
  {"Num_data_page_fetches_btree_root", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 13 */
  {"Num_data_page_fetches_btree", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 14 */
  {"Num_data_page_fetches_unknown", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 0 */

  {"Num_data_page_fetches_waits_file_header", 2, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 1 */
  {"Num_data_page_fetches_waits_file_tab", 2, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 2 */
  {"Num_data_page_fetches_waits_heap_header", 2, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 3 */
  {"Num_data_page_fetches_waits_heap", 2, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 4 */
  {"Num_data_page_fetches_waits_volheader", 2, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 5 */
  {"Num_data_page_fetches_waits_volbitmap", 2, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 6 */
  {"Num_data_page_fetches_waits_xasl", 2, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 7 */
  {"Num_data_page_fetches_waits_qresult", 2, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 8 */
  {"Num_data_page_fetches_waits_ehash", 2, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 9 */
  {"Num_data_page_fetches_waits_overflow", 2, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 10 */
  {"Num_data_page_fetches_waits_area", 2, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 11 */
  {"Num_data_page_fetches_waits_catalog", 2, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 12 */
  {"Num_data_page_fetches_waits_btree_root", 2, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 13 */
  {"Num_data_page_fetches_waits_btree", 2, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 14 */
  {"Num_data_page_fetches_waits_unknown", 2, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 0 */

  {"Num_data_page_fetches_track_file_allocset_alloc_pages", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 15 */
  {"Num_data_page_fetches_track_file_alloc_pages", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 16 */
  {"Num_data_page_fetches_track_file_dealloc_page", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 17 */
  {"Num_data_page_fetches_track_heap_find_best_page", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 18 */
  {"Num_data_page_fetches_track_heap_bestspace_sync", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 19 */
  {"Num_data_page_fetches_track_heap_ovf_insert", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 20 */
  {"Num_data_page_fetches_track_heap_ovf_update", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 21 */
  {"Num_data_page_fetches_track_heap_ovf_delete", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 22 */
  {"Num_data_page_fetches_track_btree_merge_level", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 23 */
  {"Num_data_page_fetches_track_btree_load_data", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 24 */
  {"Num_data_page_fetches_track_pgbuf_flush_checkpoint", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 25 */
  {"Num_data_page_fetches_track_log_rollback", 1, MNT_STATS_VALUE_COUNTER_WITH_TIME},	/* 26 */
#endif

  /* MNT_STATS_DATA_PAGE_DIRTIES */
  {"Num_data_page_dirties", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_DATA_PAGE_IOREADS */
  {"Num_data_page_ioreads", 0, MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_DATA_PAGE_IOWRITES */
  {"Num_data_page_iowrites", 0, MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_DATA_PAGE_VICTIMS */
  {"Num_data_page_victims", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_DATA_PAGE_IOWRITES_FOR_REPLACEMENT */
  {"Num_data_page_iowrites_for_replacement", 0, MNT_STATS_VALUE_COUNTER},

  /* MNT_STATS_LOG_PAGE_IOREADS */
  {"Num_log_page_ioreads", 0, MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_LOG_PAGE_IOWRITES */
  {"Num_log_page_iowrites", 0, MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_LOG_APPEND_RECORDS */
  {"Num_log_append_records", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_LOG_ARCHIVES */
  {"Num_log_archives", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_LOG_CHECKPOINTS */
  {"Num_log_checkpoints", 0, MNT_STATS_VALUE_EVENT},
  /* MNT_STATS_LOG_WALS */
  {"Num_log_wals", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_DDL_LOCKS_REQUESTS */
  {"Num_ddl_locks_requests", 0, MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_CLASS_LOCKS_REQUEST */
  {"Num_class_locks_request", 0, MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_CATALOG_LOCKS_REQUEST */
  {"Num_catalog_locks_request", 0, MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_GLOBAL_LOCKS_REQUEST */
  {"Num_global_locks_request", 0, MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_SHARD_LOCKS_REQUEST */
  {"Num_shard_locks_request", 0, MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_PAGE_LOCKS_ACQUIRED */
  {"Num_page_locks_acquired", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_OBJECT_LOCKS_ACQUIRED */
  {"Num_object_locks_acquired", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_PAGE_LOCKS_CONVERTED */
  {"Num_page_locks_converted", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_OBJECT_LOCKS_CONVERTED */
  {"Num_object_locks_converted", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_PAGE_LOCKS_RE_REQUESTED */
  {"Num_page_locks_re-requested", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_OBJECT_LOCKS_RE_REQUESTED */
  {"Num_object_locks_re-requested", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_PAGE_LOCKS_WAITS */
  {"Num_page_locks_waits", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_OBJECT_LOCKS_WAITS */
  {"Num_object_locks_waits", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_TRAN_COMMITS */
  {"Num_tran_commits", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_TRAN_ROLLBACKS */
  {"Num_tran_rollbacks", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_TRAN_SAVEPOINTS */
  {"Num_tran_savepoints", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_TRAN_TOPOPS */
  {"Num_tran_topops", 0, MNT_STATS_VALUE_EVENT},
  /* MNT_STATS_TRAN_INTERRUPTS */
  {"Num_tran_interrupts", 0, MNT_STATS_VALUE_COUNTER},

  /* MNT_STATS_BTREE_INSERTS */
  {"Num_btree_inserts", 0, MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_BTREE_DELETES */
  {"Num_btree_deletes", 0, MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_BTREE_UPDATES */
  {"Num_btree_updates", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_BTREE_LOAD_DATA */
  {"Num_btree_load_data", 0, MNT_STATS_VALUE_EVENT},
  /* MNT_STATS_BTREE_COVERED */
  {"Num_btree_covered", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_BTREE_NONCOVERED */
  {"Num_btree_noncovered", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_BTREE_RESUMES */
  {"Num_btree_resumes", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_BTREE_MULTIRANGE_OPTIMIZATION */
  {"Num_btree_multirange_optimization", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_BTREE_SPLITS */
  {"Num_btree_splits", 0, MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_BTREE_MERGES */
  {"Num_btree_merges", 0, MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_BTREE_PAGE_ALLOCS */
  {"Num_btree_page_allocs", 0, MNT_STATS_VALUE_COUNTER_WITH_TIME},
  /* MNT_STATS_BTREE_PAGE_DEALLOCS */
  {"Num_btree_page_deallocs", 0, MNT_STATS_VALUE_COUNTER_WITH_TIME},

  /* MNT_STATS_QUERY_SELECTS */
  {"Num_query_selects", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_QUERY_INSERTS */
  {"Num_query_inserts", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_QUERY_DELETES */
  {"Num_query_deletes", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_QUERY_UPDATES */
  {"Num_query_updates", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_QUERY_SSCANS */
  {"Num_query_sscans", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_QUERY_ISCANS */
  {"Num_query_iscans", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_QUERY_LSCANS */
  {"Num_query_lscans", 0, MNT_STATS_VALUE_COUNTER},
#if 0
  /* MNT_STATS_QUERY_METHSCANS */
  {"Num_query_methscans", 0, MNT_STATS_VALUE_COUNTER},
#endif
  /* MNT_STATS_QUERY_NLJOINS */
  {"Num_query_nljoins", 0, MNT_STATS_VALUE_COUNTER},
#if 0
  /* MNT_STATS_QUERY_MJOINS */
  {"Num_query_mjoins", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_QUERY_OBJFETCHES */
  {"Num_query_objfetches", 0, MNT_STATS_VALUE_COUNTER},
#endif
  /* MNT_STATS_QUERY_HOLDABLE_CURSORS */
  {"Num_query_holdable_cursors", 0, MNT_STATS_VALUE_GAUGE},
  /* MNT_STATS_SORT_IO_PAGES */
  {"Num_sort_io_pages", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_SORT_DATA_PAGES */
  {"Num_sort_data_pages", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_NETWORK_REQUESTS */
  {"Num_network_requests", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_ADAPTIVE_FLUSH_PAGES */
  {"Num_adaptive_flush_pages", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_ADAPTIVE_FLUSH_LOG_PAGES */
  {"Num_adaptive_flush_log_pages", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_ADAPTIVE_FLUSH_MAX_PAGES */
  {"Num_adaptive_flush_max_pages", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_PRIOR_LSA_LIST_SIZE */
  {"Num_prior_lsa_list_size", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_PRIOR_LSA_LIST_MAXED */
  {"Num_prior_lsa_list_maxed", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_PRIOR_LSA_LIST_REMOVED */
  {"Num_prior_lsa_list_removed", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_HEAP_STATS_BESTSPACE_ENTRIES */
  {"Num_heap_stats_bestspace_entries", 0, MNT_STATS_VALUE_GAUGE},
  /* MNT_STATS_HEAP_STATS_BESTSPACE_MAXED */
  {"Num_heap_stats_bestspace_maxed", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_HA_LAST_FLUSHED_PAGEID */
  {"Num_ha_last_flushed_pageid", 0, MNT_STATS_VALUE_GAUGE},
  /* MNT_STATS_HA_EOF_PAGEID */
  {"Num_ha_eof_pageid", 0, MNT_STATS_VALUE_GAUGE},
  /* MNT_STATS_HA_CURRENT_PAGEID */
  {"Num_ha_current_pageid", 0, MNT_STATS_VALUE_GAUGE},
  /* MNT_STATS_HA_REQUIRED_PAGEID */
  {"Num_ha_required_pageid", 0, MNT_STATS_VALUE_GAUGE},
  /* MNT_STATS_HA_REPLICATION_DELAY */
  {"Time_ha_replication_delay", 0, MNT_STATS_VALUE_GAUGE},
  /* MNT_STATS_PLAN_CACHE_ADD */
  {"Num_plan_cache_add", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_PLAN_CACHE_LOOKUP */
  {"Num_plan_cache_lookup", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_PLAN_CACHE_HIT */
  {"Num_plan_cache_hit", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_PLAN_CACHE_MISS */
  {"Num_plan_cache_miss", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_PLAN_CACHE_FULL */
  {"Num_plan_cache_full", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_PLAN_CACHE_DELETE */
  {"Num_plan_cache_delete", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_PLAN_CACHE_INVALID_XASL_ID */
  {"Num_plan_cache_invalid_xasl_id", 0, MNT_STATS_VALUE_COUNTER},
  /* MNT_STATS_PLAN_CACHE_QUERY_STRING_HASH_ENTRIES */
  {"Num_plan_cache_query_string_hash_entries", 0, MNT_STATS_VALUE_GAUGE},
  /* MNT_STATS_PLAN_CACHE_XASL_ID_HASH_ENTRIES */
  {"Num_plan_cache_xasl_id_hash_entries", 0, MNT_STATS_VALUE_GAUGE},
  /* MNT_STATS_PLAN_CACHE_CLASS_OID_HASH_ENTRIES */
  {"Num_plan_cache_class_oid_hash_entries", 0, MNT_STATS_VALUE_GAUGE},
  /* MNT_STATS_DATA_PAGE_BUFFER_HIT_RATIO */
  {"Data_page_buffer_hit_ratio", 0, MNT_STATS_VALUE_COUNTER}
};
#endif

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

static void mnt_server_stats_to_monitor_stats (MONITOR_STATS * monitor_stats,
					       const MNT_SERVER_EXEC_STATS *
					       server_stats);
static void mnt_monitor_stats_to_server_stats (MNT_SERVER_EXEC_STATS *
					       server_stats,
					       const MONITOR_STATS *
					       monitor_stats);


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

#if 0				/* TODO: remove me if you don't need this function */
      if (mnt_diff_stats (&diff_result, &cur_server_stats,
			  &mnt_Stat_info.base_server_stats) == NO_ERROR)
	{
	  mnt_server_dump_stats (&diff_result, stream, NULL);
	}
#endif
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
  char stat_buf[4 * ONE_K];

  if (stream == NULL)
    {
      stream = stdout;
    }

  stat_buf[0] = '\0';
  mnt_server_dump_stats_to_buffer (stats, stat_buf, sizeof (stat_buf),
				   substr);

  fprintf (stream, "%s", stat_buf);
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
mnt_get_current_times (time_t *
		       cpu_user_time,
		       time_t * cpu_sys_time, time_t * elapsed_time)
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

/*
 * mnt_server_stats_to_monitor_stats
 *    return:
 *
 *    monitor_stats(out):
 *    server_stats(in):
 */
static void
mnt_server_stats_to_monitor_stats (MONITOR_STATS * monitor_stats,
				   const MNT_SERVER_EXEC_STATS * server_stats)
{
  int i;
  for (i = 0; i < MNT_SIZE_OF_SERVER_EXEC_STATS; i++)
    {
      monitor_stats[i].value = server_stats->values[i];
      monitor_stats[i].acc_time = server_stats->acc_time[i];
    }
}

/*
 * mnt_monitor_stats_to_server_stats ()
 *   return:
 *
 *   server_stats(out):
 *   monitor_stats(in):
 */
static void
mnt_monitor_stats_to_server_stats (MNT_SERVER_EXEC_STATS * server_stats,
				   const MONITOR_STATS * monitor_stats)
{
  int i;
  for (i = 0; i < MNT_SIZE_OF_SERVER_EXEC_STATS; i++)
    {
      server_stats->values[i] = monitor_stats[i].value;
      server_stats->acc_time[i] = monitor_stats[i].acc_time;
    }
}

#if defined(SERVER_MODE) || defined(SA_MODE)

/*
 * xmnt_server_copy_stats - Copy recorded server statistics for the current
 *                          transaction index
 *   return: none
 *   to_stats(out): buffer to copy
 */
void
xmnt_server_copy_stats (THREAD_ENTRY *
			thread_p, MNT_SERVER_EXEC_STATS * to_stats)
{
  MONITOR_STATS monitor_stats[MNT_SIZE_OF_SERVER_EXEC_STATS];
  int tran_index;
  assert (to_stats != NULL);
  tran_index = logtb_get_current_tran_index (thread_p);
  assert (tran_index >= 0);
  monitor_copy_stats (NULL, monitor_stats,
		      MNT_SIZE_OF_SERVER_EXEC_STATS, tran_index + 1);
  mnt_monitor_stats_to_server_stats (to_stats, monitor_stats);
}

/*
 * xmnt_server_copy_global_stats - Copy recorded system wide statistics
 *   return: none
 *   to_stats(out): buffer to copy
 */
void
xmnt_server_copy_global_stats (UNUSED_ARG
			       THREAD_ENTRY
			       * thread_p, MNT_SERVER_EXEC_STATS * to_stats)
{
  MONITOR_STATS monitor_stats[MNT_SIZE_OF_SERVER_EXEC_STATS];

  assert (to_stats != NULL);

  monitor_copy_global_stats (NULL, monitor_stats,
			     MNT_SIZE_OF_SERVER_EXEC_STATS);
  mnt_monitor_stats_to_server_stats (to_stats, monitor_stats);
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
mnt_stats_counter_with_time (THREAD_ENTRY
			     * thread_p,
			     MNT_SERVER_ITEM
			     item, INT64 value, UINT64 start_time)
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
mnt_get_stats_with_time (THREAD_ENTRY *
			 thread_p, MNT_SERVER_ITEM item, UINT64 * acc_time)
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
#if 0
  MONITOR_STATS monitor_stats[MNT_SIZE_OF_SERVER_EXEC_STATS];
#endif
  char header[ONE_K], tail[ONE_K];
  if (buffer == NULL || buf_size <= 0)
    {
      return;
    }

  snprintf (header, sizeof (header),
	    "\n *** SERVER EXECUTION STATISTICS *** \n");
  snprintf (tail, sizeof (tail),
	    "\n *** OTHER STATISTICS *** \n"
	    "Data_page_buffer_hit_ratio    = %10.2f\n",
	    (float) stats->
	    values[MNT_STATS_DATA_PAGE_BUFFER_HIT_RATIO] / 100);

  snprintf (buffer, buf_size, "%s%s", header, tail);

#if 0				/* TODO: remove me if you don't need this function */
  mnt_server_stats_to_monitor_stats (monitor_stats, stats);
  monitor_dump_stats_to_buffer (buffer,
				buf_size,
				monitor_stats,
				MNT_SIZE_OF_SERVER_EXEC_STATS,
				header, tail, substr);
#endif
}

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
      stats->
	values
	[MNT_STATS_DATA_PAGE_BUFFER_HIT_RATIO] =
	(stats->
	 values[MNT_STATS_DATA_PAGE_FETCHES] -
	 stats->
	 values[MNT_STATS_DATA_PAGE_IOREADS]) *
	100 * 100 / stats->values[MNT_STATS_DATA_PAGE_FETCHES];
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

#if 0				/* TODO: remove me if you don't need this function */
  for (i = 0; i < MNT_SIZE_OF_SERVER_EXEC_STATS; i++)
    {
      /* calc diff values */
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

      /* calc diff acc_time */
      assert (new_stats->acc_time[i] >= old_stats->acc_time[i]);
      if (new_stats->acc_time[i] >= old_stats->acc_time[i])
	{
	  diff_stats->acc_time[i] =
	    (new_stats->acc_time[i] - old_stats->acc_time[i]);
	}
      else
	{
	  diff_stats->acc_time[i] = 0;
	}
    }
#endif

  mnt_calc_hit_ratio (diff_stats);
  return NO_ERROR;
}

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

UINT64
mnt_clock_to_time (const UINT64 acc_time)
{
  /* TODO - under construction
   *
   */

  return acc_time;
}
