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
 * monitor.c -
 */

#include <stdio.h>
#include <time.h>
#include <assert.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <string.h>
#include "monitor.h"
#include "perf_monitor.h"
#include "rye_master_shm.h"

#define MONITOR_IS_CUMMULATIVE_VALUE(TYPE)	\
	((TYPE) == MONITOR_STATS_VALUE_COUNTER || (TYPE) == MONITOR_STATS_VALUE_COUNTER_WITH_TIME)
#define MONITOR_IS_COLLECTING_TIME(TYPE)	\
	((TYPE) == MONITOR_STATS_VALUE_COUNTER_WITH_TIME)


MONITOR_INFO mntCollector = {
  -1, RYE_SHM_TYPE_UNKNOWN, MONITOR_TYPE_COLLECTOR, 0, NULL, NULL
};

static MONITOR_STATS_INFO *monitor_init_server_stats_info (int num_stats);
static MONITOR_STATS_INFO *monitor_init_stats_info (int *num_stats,
						    RYE_SHM_TYPE shm_type);
static MONITOR_INFO *monitor_create_viewer (const char *name, int shm_key,
					    RYE_SHM_TYPE shm_type);

/*
 * monitor_make_server_name
 *   return:
 *
 *   monitor_name(out):
 *   db_name(in):
 */
void
monitor_make_server_name (char *monitor_name, const char *db_name)
{
  sprintf (monitor_name, "%s%s", db_name, MONITOR_SUFFIX_SERVER);
}

/*
 *
 */
static MONITOR_STATS_INFO *
monitor_init_stats_info (int *num_stats, RYE_SHM_TYPE shm_type)
{
  MONITOR_STATS_INFO *stats_info = NULL;

  switch (shm_type)
    {
    case RYE_SHM_TYPE_MONITOR_SERVER:
      stats_info =
	monitor_init_server_stats_info (MNT_SIZE_OF_SERVER_EXEC_STATS);
      if (stats_info == NULL)
	{
	  return NULL;
	}
      *num_stats = MNT_SIZE_OF_SERVER_EXEC_STATS;
      break;
    default:
      assert (false);
      stats_info = NULL;
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR,
	      1, "Invalid RYE_SHM_TYPE");
      break;
    }

  return stats_info;
}

/*
 *
 */
static MONITOR_STATS_INFO *
monitor_init_server_stats_info (int num_stats)
{
  MONITOR_STATS_INFO *stats_info;
  MONITOR_STATS_INFO *info;
  int size, i;

  if (num_stats != MNT_SIZE_OF_SERVER_EXEC_STATS)
    {
      assert (false);
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR,
	      1, "Invalid number of monitor stats info");
      return NULL;
    }

  size = sizeof (MONITOR_STATS_INFO) * num_stats;
  stats_info = (MONITOR_STATS_INFO *) malloc (size);
  if (stats_info == NULL)
    {
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, size);
      return NULL;
    }
  memset (stats_info, 0, size);

  info = &stats_info[MNT_STATS_SQL_TRACE_LOCK_WAITS];
  info->name = "Num_sql_trace_lock_waits";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_SQL_TRACE_LATCH_WAITS];
  info->name = "Num_sql_trace_latch_waits";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_CSECT_ER_LOG_FILE];
  info->name = "Num_csect_er_log_file";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_CSECT_ER_MSG_CACHE];
  info->name = "Num_csect_er_msg_cache";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_CSECT_WFG];
  info->name = "Num_csect_wfg";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_CSECT_LOG];
  info->name = "Num_csect_log";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_CSECT_LOG_BUFFER];
  info->name = "Num_csect_log_buffer";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_CSECT_LOG_ARCHIVE];
  info->name = "Num_csect_log_archive";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_CSECT_SR_LOCATOR_CLASSNAME_TABLE];
  info->name = "Num_csect_sr_locator_classname_table";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_CSECT_FILE_NEWFILE];
  info->name = "Num_csect_file_newfile";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_CSECT_QPROC_QUERY_TABLE];
  info->name = "Num_csect_qproc_query_table";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_CSECT_BOOT_SR_DBPARM];
  info->name = "Num_csect_boot_sr_dbparm";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_CSECT_DISK_REFRESH_GOODVOL];
  info->name = "Num_csect_disk_refresh_goodvol";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_CSECT_DISK_REFRESH_GOODVOL];
  info->name = "Num_csect_disk_refresh_goodvol";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_CSECT_CNV_FMT_LEXER];
  info->name = "Num_csect_cnf_fmt_lexer";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_CSECT_CT_OID_TABLE];
  info->name = "Num_csect_ct_oid_table";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_CSECT_HA_SERVER_STATE];
  info->name = "Num_csect_ha_server_state";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_CSECT_SESSION_STATE];
  info->name = "Num_csect_session_state";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_CSECT_ACL];
  info->name = "Num_csect_acl";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_CSECT_EVENT_LOG_FILE];
  info->name = "Num_csect_event_log_file";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_CSECT_ACCESS_STATUS];
  info->name = "Num_csect_access_status";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_CSECT_TEMPFILE_CACHE];
  info->name = "Num_csect_tempfile_cache";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_CSECT_CSS_ACTIVE_CONN];
  info->name = "Num_csect_css_active_conn";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_CSECT_CSS_FREE_CONN];
  info->name = "Num_csect_css_free_conn";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_CSECT_UNKNOWN];
  info->name = "Num_csect_unknown";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_CSECT_WAITS_ER_LOG_FILE];
  info->name = "Num_csect_waits_er_log_file";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_CSECT_WAITS_ER_MSG_CACHE];
  info->name = "Num_csect_waits_er_msg_cache";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_CSECT_WAITS_WFG];
  info->name = "Num_csect_waits_wfg";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_CSECT_WAITS_LOG];
  info->name = "Num_csect_waits_log";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_CSECT_WAITS_LOG_BUFFER];
  info->name = "Num_csect_waits_log_buffer";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_CSECT_WAITS_LOG_ARCHIVE];
  info->name = "Num_csect_waits_log_archive";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_CSECT_WAITS_SR_LOCATOR_CLASSNAME_TABLE];
  info->name = "Num_csect_waits_sr_locator_classname_table";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_CSECT_WAITS_FILE_NEWFILE];
  info->name = "Num_csect_waits_file_newfile";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_CSECT_WAITS_QPROC_QUERY_TABLE];
  info->name = "Num_csect_waits_qproc_query_table";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_CSECT_WAITS_BOOT_SR_DBPARM];
  info->name = "Num_csect_waits_boot_sr_dbparm";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_CSECT_WAITS_DISK_REFRESH_GOODVOL];
  info->name = "Num_csect_waits_disk_refresh_goodvol";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_CSECT_WAITS_CNV_FMT_LEXER];
  info->name = "Num_csect_waits_cnf_fmt_lexer";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_CSECT_WAITS_CT_OID_TABLE];
  info->name = "Num_csect_waits_ct_oid_table";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_CSECT_WAITS_HA_SERVER_STATE];
  info->name = "Num_csect_waits_ha_server_state";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_CSECT_WAITS_SESSION_STATE];
  info->name = "Num_csect_waits_session_state";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_CSECT_WAITS_ACL];
  info->name = "Num_csect_waits_acl";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_CSECT_WAITS_EVENT_LOG_FILE];
  info->name = "Num_csect_waits_event_log_file";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_CSECT_WAITS_ACCESS_STATUS];
  info->name = "Num_csect_waits_access_status";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_CSECT_WAITS_TEMPFILE_CACHE];
  info->name = "Num_csect_waits_tempfile_cache";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_CSECT_WAITS_CSS_ACTIVE_CONN];
  info->name = "Num_csect_waits_css_active_conn";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_CSECT_WAITS_CSS_FREE_CONN];
  info->name = "Num_csect_waits_css_free_conn";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_CSECT_WAITS_UNKNOWN];
  info->name = "Num_csect_waits_unknown";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  /* Statistics at disk level */
  info = &stats_info[MNT_STATS_DISK_SECTOR_ALLOCS];
  info->name = "Num_disk_sector_allocs";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DISK_SECTOR_DEALLOCS];
  info->name = "Num_disk_sector_deallocs";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DISK_PAGE_ALLOCS];
  info->name = "Num_disk_page_allocs";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DISK_PAGE_DEALLOCS];
  info->name = "Num_disk_page_deallocs";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DISK_TEMP_EXPAND];
  info->name = "Num_disk_temp_expand";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  /* Statistics at file io level */
  info = &stats_info[MNT_STATS_FILE_CREATES];
  info->name = "Num_file_creates";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_FILE_REMOVES];
  info->name = "Num_file_removes";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_FILE_IOREADS];
  info->name = "Num_file_ioreads";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_FILE_IOWRITES];
  info->name = "Num_file_iowrites";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_FILE_IOSYNCHES];
  info->name = "Num_file_iosynches";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  /* Statistics at page level */
  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES];
  info->name = "Num_data_page_fetches";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_FILE_HEADER];
  info->name = "Num_data_page_fetches_file_header";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_FILE_TAB];
  info->name = "Num_data_page_fetches_file_tab";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_HEAP_HEADER];
  info->name = "Num_data_page_fetches_heap_header";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_HEAP];
  info->name = "Num_data_page_fetches_heap";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_VOLHEADER];
  info->name = "Num_data_page_fetches_volheader";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_VOLBITMAP];
  info->name = "Num_data_page_fetches_volbitmap";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_XASL];
  info->name = "Num_data_page_fetches_xasl";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_QRESULT];
  info->name = "Num_data_page_fetches_qresult";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_EHASH];
  info->name = "Num_data_page_fetches_ehash";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_OVERFLOW];
  info->name = "Num_data_page_fetches_overflow";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_AREA];
  info->name = "Num_data_page_fetches_area";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_CATALOG];
  info->name = "Num_data_page_fetches_catalog";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_BTREE_ROOT];
  info->name = "Num_data_page_fetches_btree_root";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_BTREE];
  info->name = "Num_data_page_fetches_btree";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_UNKNOWN];
  info->name = "Num_data_page_fetches_unknown";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_WAITS_FILE_HEADER];
  info->name = "Num_data_page_fetches_waits_file_header";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_WAITS_FILE_TAB];
  info->name = "Num_data_page_fetches_waits_file_tab";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_WAITS_HEAP_HEADER];
  info->name = "Num_data_page_fetches_waits_heap_header";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_WAITS_HEAP];
  info->name = "Num_data_page_fetches_waits_heap";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_WAITS_VOLHEADER];
  info->name = "Num_data_page_fetches_waits_volheader";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_WAITS_VOLBITMAP];
  info->name = "Num_data_page_fetches_waits_volbitmap";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_WAITS_XASL];
  info->name = "Num_data_page_fetches_waits_xasl";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_WAITS_QRESULT];
  info->name = "Num_data_page_fetches_waits_qresult";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_WAITS_EHASH];
  info->name = "Num_data_page_fetches_waits_ehash";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_WAITS_OVERFLOW];
  info->name = "Num_data_page_fetches_waits_overflow";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_WAITS_AREA];
  info->name = "Num_data_page_fetches_waits_area";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_WAITS_CATALOG];
  info->name = "Num_data_page_fetches_waits_catalog";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_WAITS_BTREE_ROOT];
  info->name = "Num_data_page_fetches_waits_btree_root";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_WAITS_BTREE];
  info->name = "Num_data_page_fetches_waits_btree";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_WAITS_UNKNOWN];
  info->name = "Num_data_page_fetches_waits_unknown";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info =
    &stats_info[MNT_STATS_DATA_PAGE_FETCHES_TRACK_FILE_ALLOCSET_ALLOC_PAGES];
  info->name = "Num_data_page_fetches_track_file_allocset_alloc_pages";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_TRACK_FILE_ALLOC_PAGES];
  info->name = "Num_data_page_fetches_track_file_alloc_pages";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_TRACK_FILE_DEALLOC_PAGE];
  info->name = "Num_data_page_fetches_track_file_dealloc_page";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_TRACK_HEAP_FIND_BEST_PAGE];
  info->name = "Num_data_page_fetches_track_heap_find_best_page";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_TRACK_HEAP_BESTSPACE_SYNC];
  info->name = "Num_data_page_fetches_track_heap_bestspace_sync";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_TRACK_HEAP_OVF_INSERT];
  info->name = "Num_data_page_fetches_track_heap_ovf_insert";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_TRACK_HEAP_OVF_UPDATE];
  info->name = "Num_data_page_fetches_track_heap_ovf_update";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_TRACK_HEAP_OVF_DELETE];
  info->name = "Num_data_page_fetches_track_heap_ovf_delete";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_TRACK_BTREE_MERGE_LEVEL];
  info->name = "Num_data_page_fetches_track_btree_merge_level";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_TRACK_BTREE_LOAD_DATA];
  info->name = "Num_data_page_fetches_track_btree_load_data";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info =
    &stats_info[MNT_STATS_DATA_PAGE_FETCHES_TRACK_PGBUF_FLUSH_CHECKPOINT];
  info->name = "Num_data_page_fetches_track_pgbuf_flush_checkpoint";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_FETCHES_TRACK_LOG_ROLLBACK];
  info->name = "Num_data_page_fetches_track_log_rollback";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_DIRTIES];
  info->name = "Num_data_page_dirties";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_DATA_PAGE_IOREADS];
  info->name = "Num_data_page_ioreads";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_IOWRITES];
  info->name = "Num_data_page_iowrites";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_DATA_PAGE_VICTIMS];
  info->name = "Num_data_page_victims";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_DATA_PAGE_IOWRITES_FOR_REPLACEMENT];
  info->name = "Num_data_page_iowrites_for_replacement";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  /* Statistics at log level */
  info = &stats_info[MNT_STATS_LOG_PAGE_IOREADS];
  info->name = "Num_log_page_ioreads";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_LOG_PAGE_IOWRITES];
  info->name = "Num_log_page_iowrites";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_LOG_APPEND_RECORDS];
  info->name = "Num_log_append_records";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_LOG_ARCHIVES];
  info->name = "Num_log_archives";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_LOG_CHECKPOINTS];
  info->name = "Num_log_checkpoints";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_EVENT;

  info = &stats_info[MNT_STATS_LOG_WALS];
  info->name = "Num_log_wals";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  /* Statistics at lock level */
  info = &stats_info[MNT_STATS_DDL_LOCKS_REQUESTS];
  info->name = "Num_ddl_locks_requests";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_CLASS_LOCKS_REQUEST];
  info->name = "Num_class_locks_request";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_CATALOG_LOCKS_REQUEST];
  info->name = "Num_catalog_locks_request";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_GLOBAL_LOCKS_REQUEST];
  info->name = "Num_global_locks_request";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_SHARD_LOCKS_REQUEST];
  info->name = "Num_shard_locks_request";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_PAGE_LOCKS_ACQUIRED];
  info->name = "Num_log_wals";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_OBJECT_LOCKS_ACQUIRED];
  info->name = "Num_page_locks_acquired";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_PAGE_LOCKS_CONVERTED];
  info->name = "Num_object_locks_acquired";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_OBJECT_LOCKS_CONVERTED];
  info->name = "Num_page_locks_converted";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_PAGE_LOCKS_RE_REQUESTED];
  info->name = "Num_page_locks_re-requested";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_OBJECT_LOCKS_RE_REQUESTED];
  info->name = "Num_object_locks_re-requested";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_PAGE_LOCKS_WAITS];
  info->name = "Num_page_locks_waits";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_OBJECT_LOCKS_WAITS];
  info->name = "Num_object_locks_waits";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  /* Transaction Management level */
  info = &stats_info[MNT_STATS_TRAN_COMMITS];
  info->name = "Num_tran_commits";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_TRAN_ROLLBACKS];
  info->name = "Num_tran_rollbacks";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_TRAN_SAVEPOINTS];
  info->name = "Num_tran_savepoints";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_TRAN_TOPOPS];
  info->name = "Num_tran_topops";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_TRAN_INTERRUPTS];
  info->name = "Num_tran_interrupts";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  /* Statistics at btree level */
  info = &stats_info[MNT_STATS_BTREE_INSERTS];
  info->name = "Num_btree_inserts";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_BTREE_DELETES];
  info->name = "Num_btree_deletes";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_BTREE_UPDATES];
  info->name = "Num_btree_updates";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_BTREE_LOAD_DATA];
  info->name = "Num_btree_load_data";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_EVENT;

  info = &stats_info[MNT_STATS_BTREE_COVERED];
  info->name = "Num_btree_covered";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_BTREE_NONCOVERED];
  info->name = "Num_btree_noncovered";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_BTREE_RESUMES];
  info->name = "Num_btree_resumes";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_BTREE_MULTIRANGE_OPTIMIZATION];
  info->name = "Num_btree_multirange_optimization";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_BTREE_SPLITS];
  info->name = "Num_btree_splits";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_BTREE_MERGES];
  info->name = "Num_btree_merges";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_BTREE_PAGE_ALLOCS];
  info->name = "Num_btree_page_allocs";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  info = &stats_info[MNT_STATS_BTREE_PAGE_DEALLOCS];
  info->name = "Num_btree_page_deallocs";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER_WITH_TIME;

  /* Execution statistics for the query manager */
  info = &stats_info[MNT_STATS_QUERY_SELECTS];
  info->name = "Num_query_selects";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_QUERY_INSERTS];
  info->name = "Num_query_inserts";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_QUERY_DELETES];
  info->name = "Num_query_deletes";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_QUERY_UPDATES];
  info->name = "Num_query_updates";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_QUERY_SSCANS];
  info->name = "Num_query_sscans";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_QUERY_ISCANS];
  info->name = "Num_query_iscans";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_QUERY_LSCANS];
  info->name = "Num_query_lscans";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_QUERY_NLJOINS];
  info->name = "Num_query_nljoins";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_QUERY_HOLDABLE_CURSORS];
  info->name = "Num_query_holdable_cursors";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_GAUGE;

  /* execution statistics for external sort */
  info = &stats_info[MNT_STATS_SORT_IO_PAGES];
  info->name = "Num_sort_io_pages";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_SORT_DATA_PAGES];
  info->name = "Num_sort_data_pages";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  /* Network Communication level */
  info = &stats_info[MNT_STATS_NETWORK_REQUESTS];
  info->name = "Num_network_requests";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  /* Statistics at Flush Control */
  info = &stats_info[MNT_STATS_ADAPTIVE_FLUSH_PAGES];
  info->name = "Num_adaptive_flush_pages";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_ADAPTIVE_FLUSH_LOG_PAGES];
  info->name = "Num_adaptive_flush_log_pages";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_ADAPTIVE_FLUSH_MAX_PAGES];
  info->name = "Num_adaptive_flush_max_pages";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  /* Prior LSA */
  info = &stats_info[MNT_STATS_PRIOR_LSA_LIST_SIZE];
  info->name = "Num_prior_lsa_list_size";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_PRIOR_LSA_LIST_MAXED];
  info->name = "Num_prior_lsa_list_maxed";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_PRIOR_LSA_LIST_REMOVED];
  info->name = "Num_prior_lsa_list_removed";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  /* Heap best space info */
  info = &stats_info[MNT_STATS_HEAP_STATS_BESTSPACE_ENTRIES];
  info->name = "Num_heap_stats_bestspace_entries";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_GAUGE;

  info = &stats_info[MNT_STATS_HEAP_STATS_BESTSPACE_MAXED];
  info->name = "Num_heap_stats_bestspace_maxed";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  /* HA info */
  info = &stats_info[MNT_STATS_HA_LAST_FLUSHED_PAGEID];
  info->name = "Num_ha_last_flushed_pageid";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_GAUGE;

  info = &stats_info[MNT_STATS_HA_EOF_PAGEID];
  info->name = "Num_ha_eof_pageid";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_GAUGE;

  info = &stats_info[MNT_STATS_HA_CURRENT_PAGEID];
  info->name = "Num_ha_current_pageid";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_GAUGE;

  info = &stats_info[MNT_STATS_HA_REQUIRED_PAGEID];
  info->name = "Num_ha_required_pageid";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_GAUGE;

  info = &stats_info[MNT_STATS_HA_REPLICATION_DELAY];
  info->name = "Time_ha_replication_delay";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_GAUGE;

  /* Plan cache */
  info = &stats_info[MNT_STATS_PLAN_CACHE_ADD];
  info->name = "Num_plan_cache_add";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_PLAN_CACHE_LOOKUP];
  info->name = "Num_plan_cache_lookup";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_PLAN_CACHE_HIT];
  info->name = "Num_plan_cache_hit";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_PLAN_CACHE_MISS];
  info->name = "Num_plan_cache_miss";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_PLAN_CACHE_FULL];
  info->name = "Num_plan_cache_full";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_PLAN_CACHE_DELETE];
  info->name = "Num_plan_cache_delete";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_PLAN_CACHE_INVALID_XASL_ID];
  info->name = "Num_plan_cache_invalid_xasl_id";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

  info = &stats_info[MNT_STATS_PLAN_CACHE_QUERY_STRING_HASH_ENTRIES];
  info->name = "Num_plan_cache_query_string_hash_entries";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_GAUGE;

  info = &stats_info[MNT_STATS_PLAN_CACHE_XASL_ID_HASH_ENTRIES];
  info->name = "Num_plan_cache_xasl_id_hash_entries";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_GAUGE;

  info = &stats_info[MNT_STATS_PLAN_CACHE_CLASS_OID_HASH_ENTRIES];
  info->name = "Num_plan_cache_class_oid_hash_entries";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_GAUGE;

  info = &stats_info[MNT_STATS_DATA_PAGE_BUFFER_HIT_RATIO];
  info->name = "Data_page_buffer_hit_ratio";
  info->level = 0;
  info->value_type = MONITOR_STATS_VALUE_COUNTER;

#if !defined(NDEBUG)
  for (i = 0; i < num_stats; i++)
    {
      assert (stats_info[i].name != NULL);
    }
#endif

  return stats_info;
}

/******************************************************************
 * MONITOR COLLECTOR
 ******************************************************************/

/*
 * monitor_create_collector
 */
int
monitor_create_collector (const char *name, int num_monitors,
			  RYE_SHM_TYPE shm_type)
{
  RYE_MONITOR_SHM *shm_p = NULL;
  MONITOR_STATS_INFO *stats_info = NULL;
  int num_stats;
  int shm_size, shm_key = -1;
  int error = NO_ERROR;

  if (strlen (name) >= SHM_NAME_SIZE)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "Invalid monitor name length");
      goto exit_on_error;
    }

  stats_info = monitor_init_stats_info (&num_stats, shm_type);
  if (stats_info == NULL)
    {
      error = er_errid ();
      goto exit_on_error;
    }

  shm_key = rye_master_shm_get_new_shm_key (name, shm_type);
  if (shm_key < 0)
    {
      error = er_errid ();
      goto exit_on_error;
    }

  if (rye_shm_check_shm (shm_key, shm_type, false) == shm_type)
    {
      rye_shm_destroy (shm_key);
    }

  /* global stats + monitor stats */
  shm_size = (sizeof (RYE_MONITOR_SHM)
	      + sizeof (MONITOR_STATS) * num_stats * (num_monitors + 1));
  shm_p = rye_shm_create (shm_key, shm_size, shm_type);
  if (shm_p == NULL)
    {
      error = er_errid ();
      goto exit_on_error;
    }

  shm_p->num_stats = num_stats;
  shm_p->num_monitors = num_monitors;
  strncpy (shm_p->name, name, SHM_NAME_SIZE);
  shm_p->name[SHM_NAME_SIZE - 1] = '\0';

  shm_p->shm_header.status = RYE_SHM_VALID;

  mntCollector.num_stats = num_stats;
  mntCollector.shm_key = shm_key;
  mntCollector.shm_type = shm_type;
  mntCollector.info = stats_info;
  mntCollector.data = shm_p;
  mntCollector.type = MONITOR_TYPE_COLLECTOR;

  return NO_ERROR;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "Invalid error code");
    }

  if (shm_p != NULL)
    {
      rye_shm_detach (shm_p);
      shm_p = NULL;
    }
  if (shm_key != -1)
    {
      rye_shm_destroy (shm_key);
      shm_key = -1;
    }

  if (stats_info != NULL)
    {
      free_and_init (stats_info);
    }

  return error;
}

/*
 * monitor_stats_counter -
 */
void
monitor_stats_counter (int mnt_id, int item, INT64 value)
{
  monitor_stats_counter_with_time (mnt_id, item, value, 0);
}

/*
 * monitor_stats_counter_with_time ()-
 *   return:
 *
 *   mnt_id(in):
 *   item(in):
 *   value(in):
 *   exec_time(in):
 */
void
monitor_stats_counter_with_time (int mnt_id, int item, INT64 value,
				 UINT64 start_time)
{
  UNUSED_VAR INT64 after_value;
  UNUSED_VAR UINT64 after_exec_time;
  UINT64 end_time, exec_time;
  int item_index;

  if (mntCollector.info == NULL || mntCollector.data == NULL
      || mntCollector.type != MONITOR_TYPE_COLLECTOR)
    {
      return;
    }

  if (item < 0 || item >= mntCollector.num_stats
      || mnt_id <= 0 || mnt_id > mntCollector.data->num_monitors
      || (mntCollector.info[item].value_type != MONITOR_STATS_VALUE_COUNTER
	  && mntCollector.info[item].value_type !=
	  MONITOR_STATS_VALUE_COUNTER_WITH_TIME
	  && mntCollector.info[item].value_type != MONITOR_STATS_VALUE_EVENT))
    {
      assert (false);
      return;
    }

  if (mntCollector.info[item].value_type ==
      MONITOR_STATS_VALUE_COUNTER_WITH_TIME)
    {
      assert (start_time != 0);

      MONITOR_GET_CURRENT_TIME (end_time);
      exec_time = end_time - start_time;
    }
  else
    {
      assert (start_time == 0);
      exec_time = 0;
    }

  /* thread stats */
  item_index = mnt_id * mntCollector.num_stats + item;
  after_value =
    ATOMIC_INC_64 (&mntCollector.data->stats[item_index].value, value);
  after_exec_time =
    ATOMIC_INC_64 (&mntCollector.data->stats[item_index].acc_time, exec_time);

  /* global stats */
  after_value = ATOMIC_INC_64 (&mntCollector.data->stats[item].value, value);
  after_exec_time =
    ATOMIC_INC_64 (&mntCollector.data->stats[item].acc_time, exec_time);
}

/*
 * monitor_stats_gauge -
 */
void
monitor_stats_gauge (int mnt_id, int item, INT64 value)
{
  UNUSED_VAR INT64 after_value;
  int item_index;

  if (mntCollector.info == NULL || mntCollector.data == NULL
      || mntCollector.type != MONITOR_TYPE_COLLECTOR)
    {
      return;
    }

  if (item < 0 || item >= mntCollector.num_stats
      || mnt_id <= 0 || mnt_id > mntCollector.data->num_monitors
      || mntCollector.info[item].value_type != MONITOR_STATS_VALUE_GAUGE)
    {
      assert (false);
      return;
    }

  /* thread stats */
  item_index = mnt_id * mntCollector.num_stats + item;
  after_value =
    ATOMIC_TAS_64 (&mntCollector.data->stats[item_index].value, value);


  /* global stats */
  after_value = ATOMIC_TAS_64 (&mntCollector.data->stats[item].value, value);
}

/*
 * monitor_get_stats_with_time
 *   return: stats value
 *
 *   acc_time(out):
 *   tread_index(in):
 *   item(in):
 */
INT64
monitor_get_stats_with_time (UINT64 * acc_time, int mnt_id, int item)
{
  int item_index;

  if (mntCollector.info == NULL || mntCollector.data == NULL
      || mntCollector.type != MONITOR_TYPE_COLLECTOR)
    {
      assert (false);
      return 0;
    }

  if (item < 0 || item >= mntCollector.num_stats
      || mnt_id <= 0 || mnt_id > mntCollector.data->num_monitors)
    {
      assert (false);
      return 0;
    }

  /* thread stats */
  item_index = mnt_id * mntCollector.num_stats + item;
  if (acc_time != NULL)
    {
      *acc_time = mntCollector.data->stats[item_index].acc_time;
    }

  return mntCollector.data->stats[item_index].value;
}

/*
 * monitor_get_stats_with_time
 *   return: stats value
 *
 *   tread_index(in):
 *   item(in):
 */
INT64
monitor_get_stats (int mnt_id, int item)
{
  return monitor_get_stats_with_time (NULL, mnt_id, item);
}


/**********************************************************
 * MONITOR VIEWER
 **********************************************************/

/*
 * monitor_create_viewer_from_name -
 *   return: MONITOR_INFO
 *
 *   name(in):
 *   shm_type(in):
 */
MONITOR_INFO *
monitor_create_viewer_from_name (const char *name, RYE_SHM_TYPE shm_type)
{
  if (strlen (name) >= SHM_NAME_SIZE)
    {
      assert (false);
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR,
	      1, "Invalid monitor name length");

      return NULL;
    }

  return monitor_create_viewer (name, -1, shm_type);
}

/*
 * monitor_create_viewer_from_key -
 *   return: MONITOR_INFO
 *
 *   shm_key(in):
 *   shm_type(in):
 */
MONITOR_INFO *
monitor_create_viewer_from_key (int shm_key, RYE_SHM_TYPE shm_type)
{
  return monitor_create_viewer (NULL, shm_key, shm_type);
}

/*
 * monitor_create_viewer -
 *   return: MONITOR_INFO
 *
 *   name(in):
 *   shm_key(in):
 *   shm_type(in):
 */
static MONITOR_INFO *
monitor_create_viewer (const char *name, int shm_key, RYE_SHM_TYPE shm_type)
{
  MONITOR_INFO *monitor = NULL;
  RYE_MONITOR_SHM *shm_p = NULL;
  MONITOR_STATS_INFO *stats_info = NULL;
  int num_stats;
  int error = NO_ERROR;

  if (name == NULL && shm_key < 0)
    {
      assert (false);
      error = ER_GENERIC_ERROR;
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "Invalid arguments");
      goto exit_on_error;
    }

  monitor = (MONITOR_INFO *) malloc (sizeof (MONITOR_INFO));
  if (monitor == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, sizeof (MONITOR_INFO));
      goto exit_on_error;
    }

  stats_info = monitor_init_stats_info (&num_stats, shm_type);
  if (stats_info == NULL)
    {
      error = er_errid ();
      goto exit_on_error;
    }

  if (name != NULL)
    {
      shm_key = rye_master_shm_get_shm_key (name, shm_type);
    }
  assert (shm_key > 0);

  shm_p = rye_shm_attach (shm_key, shm_type, true);
  if (shm_p == NULL)
    {
      error = er_errid ();
      goto exit_on_error;
    }

  if (shm_p->num_stats != num_stats || shm_p->shm_header.shm_key != shm_key)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "Invalid monitor shm ");
      goto exit_on_error;
    }

  monitor->num_stats = num_stats;
  monitor->shm_key = shm_key;
  monitor->shm_type = shm_type;
  monitor->info = stats_info;
  monitor->data = shm_p;
  monitor->type = MONITOR_TYPE_VIEWER;

  return monitor;

exit_on_error:
  if (shm_p != NULL)
    {
      rye_shm_detach (shm_p);
      shm_p = NULL;
    }
  if (stats_info != NULL)
    {
      free_and_init (stats_info);
    }
  if (monitor != NULL)
    {
      free_and_init (monitor);
    }

  return NULL;
}

/*
 * monitor_stats_is_cumulative -
 *   return: bool
 *
 *   monitor(in):
 *   item(in):
 */
bool
monitor_stats_is_cumulative (MONITOR_INFO * monitor, int item)
{
  if (monitor == NULL || monitor->info == NULL || monitor->shm_key <= 0)
    {
      assert (false);
      return -1;
    }

  return IS_CUMMULATIVE_VALUE (monitor->info[item].value_type);
}

/*
 * monitor_stats_is_collecting_time: -
 *   return: bool
 *
 *   monitor(in):
 *   item(in):
 */
bool
monitor_stats_is_collecting_time (MONITOR_INFO * monitor, int item)
{
  if (monitor == NULL || monitor->info == NULL || monitor->shm_key <= 0)
    {
      assert (false);
      return -1;
    }

  return (IS_COLLECTING_TIME (monitor->info[item].value_type));
}

/*
 * monitor_open_viewer_data -
 *    return: error code
 *
 *    monitor(in/out):
 *    num_stats(in):
 */
int
monitor_open_viewer_data (MONITOR_INFO * monitor, int num_stats)
{
  RYE_MONITOR_SHM *shm_p = NULL;

  if (monitor == NULL || monitor->info == NULL || monitor->shm_key <= 0
      || monitor->type != MONITOR_TYPE_VIEWER)
    {
      assert (false);
      return ER_FAILED;
    }

  if (monitor->data != NULL)
    {
      shm_p = monitor->data;
    }
  else
    {
      shm_p = rye_shm_attach (monitor->shm_key, monitor->shm_type, true);
      if (shm_p == NULL)
	{
	  return ER_FAILED;
	}
    }

  if (num_stats != shm_p->num_stats || num_stats != monitor->num_stats)
    {
      assert (false);

      rye_shm_detach (shm_p);
      monitor->data = NULL;
      return ER_FAILED;
    }

  monitor->data = shm_p;

  return NO_ERROR;
}

/*
 * monitor_close_viewer_data -
 *    return: error code
 *
 *    monitor(in/out):
 *    num_stats(in):
 */
int
monitor_close_viewer_data (MONITOR_INFO * monitor, int num_stats)
{
  if (monitor == NULL || monitor->info == NULL || monitor->shm_key <= 0)
    {
      assert (false);
      return ER_FAILED;
    }

  if (monitor->data != NULL && monitor->data->num_stats == num_stats)
    {
      rye_shm_detach (monitor->data);
      monitor->data = NULL;
    }

  return NO_ERROR;
}

/*
 * monitor_copy_stats ()-
 *   return:
 *
 *   monitor(in/out):
 *   to_stats(out):
 *   num_stats(in):
 *   int mnt_id(in):
 */
int
monitor_copy_stats (MONITOR_INFO * monitor,
		    MONITOR_STATS * to_stats, int num_stats, int mnt_id)
{
  RYE_MONITOR_SHM *shm_p = NULL;
  int error = NO_ERROR;

  if (monitor == NULL)
    {
      assert (mntCollector.type == MONITOR_TYPE_COLLECTOR);
      shm_p = mntCollector.data;
    }
  else
    {
      error = monitor_open_viewer_data (monitor, num_stats);
      if (error != NO_ERROR)
	{
	  return error;
	}
      shm_p = monitor->data;
    }

  if (shm_p == NULL || mnt_id <= 0 || mnt_id > shm_p->num_monitors)
    {
      assert (false);

      return ER_FAILED;
    }

  /* copy thread stats */
  memcpy (to_stats, &shm_p->stats[mnt_id * num_stats],
	  sizeof (MONITOR_STATS) * num_stats);

  return error;
}

/*
 * monitor_copy_global_stats ()-
 *   return:
 *
 *   monitor(in/out):
 *   to_stats(out):
 *   num_stats(in):
 */
int
monitor_copy_global_stats (MONITOR_INFO * monitor,
			   MONITOR_STATS * to_stats, int num_stats)
{
  RYE_MONITOR_SHM *shm_p = NULL;
  int error = NO_ERROR;

  if (monitor == NULL)
    {
      assert (mntCollector.type == MONITOR_TYPE_COLLECTOR);
      shm_p = mntCollector.data;
    }
  else
    {
      error = monitor_open_viewer_data (monitor, num_stats);
      if (error != NO_ERROR)
	{
	  return error;
	}
      shm_p = monitor->data;
    }
  if (shm_p == NULL)
    {
      assert (false);

      return ER_FAILED;
    }

  /* copy global stats */
  memcpy (to_stats, &shm_p->stats[0], sizeof (MONITOR_STATS) * num_stats);

  return error;
}

/*
 * monitor_diff_stats -
 *    return: error code
 *
 *    monitor(in/out):
 *    diff_stats(out):
 *    new_stats(in):
 *    old_stats(in):
 *    num_stats(in):
 */
int
monitor_diff_stats (MONITOR_INFO * monitor, MONITOR_STATS * diff_stats,
		    MONITOR_STATS * new_stats, MONITOR_STATS * old_stats,
		    int num_stats)
{
  int i;
  if (diff_stats == NULL || new_stats == NULL || old_stats == NULL)
    {
      assert (0);
      if (diff_stats != NULL)
	{
	  memset (diff_stats, 0, sizeof (MONITOR_STATS) * num_stats);
	}
      return ER_FAILED;
    }

  if (monitor == NULL)
    {
      assert (mntCollector.type == MONITOR_TYPE_COLLECTOR);
      monitor = &mntCollector;
    }
  if (monitor->num_stats != num_stats)
    {
      assert (false);
      memset (diff_stats, 0, sizeof (MONITOR_STATS) * num_stats);
      return ER_FAILED;
    }

  for (i = 0; i < num_stats; i++)
    {
      /* calc diff values */
      if (IS_CUMMULATIVE_VALUE (monitor->info[i].value_type))
	{
	  if (new_stats[i].value >= old_stats[i].value)
	    {
	      diff_stats[i].value = (new_stats[i].value - old_stats[i].value);
	    }
	  else
	    {
	      diff_stats[i].value = 0;
	    }
	}
      else
	{
	  diff_stats[i].value = new_stats[i].value;
	}

      /* calc diff acc_time */
      assert (new_stats[i].acc_time >= old_stats[i].acc_time);
      if (new_stats[i].acc_time >= old_stats[i].acc_time)
	{
	  diff_stats[i].acc_time =
	    (new_stats[i].acc_time - old_stats[i].acc_time);
	}
      else
	{
	  diff_stats[i].acc_time = 0;
	}
    }

  return NO_ERROR;
}

/*
 * monitor_dump_stats
 *   return:
 *
 *   monitor(in/out):
 *   stream(out):
 *   stats(in):
 *   num_stats(in):
 *   header(in):
 *   tail(in):
 *   substr(in):
 */
void
monitor_dump_stats (MONITOR_INFO * monitor, FILE * stream,
		    MONITOR_STATS * stats, int num_stats,
		    const char *header, const char *tail, const char *substr)
{
  char stat_buf[4 * ONE_K];

  if (header != NULL)
    {
      fprintf (stream, "%s", header);
    }

  stat_buf[0] = '\0';
  monitor_dump_stats_to_buffer (monitor, stat_buf, sizeof (stat_buf), stats,
				num_stats, NULL, NULL, substr);
  fprintf (stream, "%s", stat_buf);

  if (tail != NULL)
    {
      fprintf (stream, "%s", tail);
    }
}

/*
 * monitor_dump_stats_to_buffer ()
 *   return:
 *
 *   monitor(in/out):
 *   buffer(out):
 *   buf_size(in):
 *   stats(in):
 *   num_stats(in):
 *   header(in):
 *   tail(in):
 *   substr(in):
 */
void
monitor_dump_stats_to_buffer (MONITOR_INFO * monitor,
			      char *buffer, int buf_size,
			      MONITOR_STATS * stats, int num_stats,
			      const char *header, const char *tail,
			      const char *substr)
{
  int i;
  int ret;
  int remained_size;
  const char *name, *s;
  int level;
  char *p;

  if (buffer == NULL || buf_size <= 0)
    {
      assert (false);
      return;
    }

  if (monitor == NULL)
    {
      assert (mntCollector.type == MONITOR_TYPE_COLLECTOR);
      monitor = &mntCollector;
    }
  if (monitor->num_stats != num_stats)
    {
      assert (false);
      return;
    }

  p = buffer;
  remained_size = buf_size - 1;
  if (header != NULL)
    {
      ret = snprintf (p, remained_size, "%s", header);
      remained_size -= ret;
      p += ret;
    }

  if (remained_size <= 0)
    {
      return;
    }

  for (i = 0; i < num_stats - 1; i++)
    {
      name = monitor->info[i].name;
      level = monitor->info[i].level;

      if (substr != NULL)
	{
	  s = strstr (name, substr);
	}
      else
	{
	  s = name;
	}

      if (s != NULL)
	{
	  /* sub-info indent */
	  while (level > 0)
	    {
	      ret = snprintf (p, remained_size, "   ");
	      remained_size -= ret;
	      p += ret;
	      if (remained_size <= 0)
		{
		  return;
		}
	      level--;
	    }

	  ret = snprintf (p, remained_size, "%-29s = %10lld\n",
			  name, (long long) stats[i].value);
	  remained_size -= ret;
	  p += ret;
	  if (remained_size <= 0)
	    {
	      return;
	    }
	}
    }

  if (tail != NULL)
    {
      ret = snprintf (p, remained_size, "%s", tail);
      remained_size -= ret;
      p += ret;
    }

  buffer[buf_size - 1] = '\0';
}
