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


MONITOR_INFO *mntCollector = NULL;

static MONITOR_STATS_META *monitor_create_server_stats_meta (void);
static MONITOR_STATS_META *monitor_create_repl_stats_meta (void);
static MONITOR_STATS_META *monitor_create_stats_meta (MONITOR_TYPE
						      monitor_type);
static MONITOR_INFO *monitor_create_viewer (const char *name, int shm_key);
static void monitor_dump_stats_normal (char *buffer, int buf_size,
				       MONITOR_INFO * monitor,
				       MONITOR_STATS * stats,
				       const char *substr);
static void monitor_dump_stats_csv_header (char *buffer, int buf_size,
					   MONITOR_INFO * monitor,
					   const char *substr);
static void monitor_dump_stats_csv (char *buffer, int buf_size,
				    MONITOR_INFO * monitor,
				    MONITOR_STATS * stats,
				    const char *substr);
/*
 * monitor_make_name
 *   return:
 *
 *   monitor_name(out):
 *   db_name(in):
 */
void
monitor_make_name (char *monitor_name, const char *name)
{
  sprintf (monitor_name, "%s%s", name, MONITOR_SUFFIX);
}

/*
 * monitor_get_num_stats -
 *   return: number of stats
 *
 *   monitor_type(in):
 */
static int
monitor_get_num_stats (MONITOR_TYPE monitor_type)
{
  switch (monitor_type)
    {
    case MONITOR_TYPE_SERVER:
      return MNT_SIZE_OF_SERVER_EXEC_STATS;
    case MONITOR_TYPE_REPL:
      return MNT_SIZE_OF_REPL_EXEC_STATS;
    default:
      return 0;
    }
}

/*
 * monitor_create_shm -
 *   return: monitor_shm
 *
 *   monitor_type(in):
 */
static RYE_MONITOR_SHM *
monitor_create_shm (const char *name, int num_monitors,
		    MONITOR_TYPE monitor_type)
{
  RYE_MONITOR_SHM *shm_p = NULL;
  int shm_key = -1;
  int num_stats, shm_size;

  shm_key = rye_master_shm_get_new_shm_key (name, RYE_SHM_TYPE_MONITOR);
  if (shm_key < 0)
    {
      goto exit_on_error;
    }

  if (rye_shm_check_shm (shm_key, RYE_SHM_TYPE_MONITOR,
			 false) == RYE_SHM_TYPE_MONITOR)
    {
      rye_shm_destroy (shm_key);
    }

  num_stats = monitor_get_num_stats (monitor_type);
  /* global stats + monitor stats */
  shm_size = (sizeof (RYE_MONITOR_SHM)
	      + sizeof (MONITOR_STATS) * num_stats * (num_monitors + 1));
  shm_p = rye_shm_create (shm_key, shm_size, RYE_SHM_TYPE_MONITOR);
  if (shm_p == NULL)
    {
      goto exit_on_error;
    }

  strncpy (shm_p->name, name, SHM_NAME_SIZE);
  shm_p->name[SHM_NAME_SIZE - 1] = '\0';
  shm_p->monitor_type = monitor_type;
  shm_p->num_stats = num_stats;
  shm_p->num_monitors = num_monitors;

  shm_p->shm_header.status = RYE_SHM_VALID;

  return shm_p;

exit_on_error:
  if (shm_p != NULL)
    {
      rye_shm_detach (shm_p);
      shm_p = NULL;
    }

  return NULL;
}

/*
 * monitor_create_stats_meta -
 *   return: stats_meta
 *
 *   monitor_type(in):
 */
static MONITOR_STATS_META *
monitor_create_stats_meta (MONITOR_TYPE monitor_type)
{
  MONITOR_STATS_META *meta_p = NULL;

  switch (monitor_type)
    {
    case MONITOR_TYPE_SERVER:
      meta_p = monitor_create_server_stats_meta ();
      break;
    case MONITOR_TYPE_REPL:
      meta_p = monitor_create_repl_stats_meta ();
      break;
    default:
      assert (false);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR,
	      1, "Invalid MONITOR_TYPE");
      break;
    }

#if !defined(NDEBUG)
  {
    int i;

    for (i = 0; i < meta_p->num_stats; i++)
      {
	assert (meta_p->info[i].name[0] != '\0');
      }
  }
#endif

  return meta_p;
}

/*
 * init_monitor_stats_info -
 *   return:
 *
 *   shm_p(in/out):
 *   item(in):
 *   metric_name(in):
 *   item_name(in):
 *   value_type(in):
 */
static void
init_monitor_stats_info (MONITOR_STATS_INFO * info, int item,
			 const char *name,
			 MONITOR_STATS_VALUE_TYPE value_type)
{
  assert (info != NULL);

  info[item].name = name;
  info[item].value_type = value_type;
}

/*
 * monitor_create_server_stats_meta -
 *   return: error code
 *
 *   shm_p(in/out):
 */
static MONITOR_STATS_META *
monitor_create_server_stats_meta (void)
{
  MONITOR_STATS_META *meta_p = NULL;
  MONITOR_STATS_INFO *info_p = NULL;
  int size;
  int num_stats;

  num_stats = monitor_get_num_stats (MONITOR_TYPE_SERVER);
  size = (sizeof (MONITOR_STATS_META)
	  + num_stats * sizeof (MONITOR_STATS_INFO));
  meta_p = (MONITOR_STATS_META *) malloc (size);
  if (meta_p == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, size);
      return NULL;
    }
  meta_p->monitor_type = MONITOR_TYPE_SERVER;
  meta_p->num_stats = num_stats;
  info_p = meta_p->info;

  /* sql trace */
  init_monitor_stats_info (info_p, MNT_STATS_SQL_TRACE_LOCK_WAITS,
			   "sql_trace_lock_waits",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_SQL_TRACE_LATCH_WAITS,
			   "sql_trace_latch_waits",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  /* csec counter info */
  init_monitor_stats_info (info_p, MNT_STATS_CSECT_ER_LOG_FILE,
			   "csect_er_log_file", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_ER_MSG_CACHE,
			   "csect_er_msg_cache", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_WFG,
			   "csect_wfg", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_LOG,
			   "csect_log", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_LOG_BUFFER,
			   "csect_log_buffer", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_LOG_ARCHIVE,
			   "csect_log_archive", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p,
			   MNT_STATS_CSECT_SR_LOCATOR_CLASSNAME_TABLE,
			   "csect_sr_locator_classname_table",
			   MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_FILE_NEWFILE,
			   "csect_file_newfile", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_QPROC_QUERY_TABLE,
			   "csect_qproc_query_table",
			   MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_BOOT_SR_DBPARM,
			   "csect_boot_sr_dbparm",
			   MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_DISK_REFRESH_GOODVOL,
			   "csect_disk_refresh_goodvol",
			   MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_CNV_FMT_LEXER,
			   "csect_cnf_fmt_lexer",
			   MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_CT_OID_TABLE,
			   "csect_ct_oid_table", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_HA_SERVER_STATE,
			   "csect_ha_server_state",
			   MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_SESSION_STATE,
			   "csect_session_state",
			   MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_ACL,
			   "csect_acl", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_EVENT_LOG_FILE,
			   "csect_event_log_file",
			   MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_ACCESS_STATUS,
			   "csect_access_status",
			   MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_TEMPFILE_CACHE,
			   "csect_tempfile_cache",
			   MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_CSS_ACTIVE_CONN,
			   "csect_css_active_conn",
			   MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_CSS_FREE_CONN,
			   "csect_css_free_conn",
			   MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_UNKNOWN,
			   "csect_unknown", MONITOR_STATS_VALUE_COUNTER);

  /* csec wait time info */
  init_monitor_stats_info (info_p, MNT_STATS_CSECT_WAITS_ER_LOG_FILE,
			   "csect_waits_er_log_file",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_WAITS_ER_MSG_CACHE,
			   "csect_waits_er_msg_cache",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_WAITS_WFG,
			   "csect_waits_wfg",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_WAITS_LOG,
			   "csect_waits_log",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_WAITS_LOG_BUFFER,
			   "csect_waits_log_buffer",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_WAITS_LOG_ARCHIVE,
			   "csect_waits_log_archive",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p,
			   MNT_STATS_CSECT_WAITS_SR_LOCATOR_CLASSNAME_TABLE,
			   "csect_waits_sr_locator_classname_table",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_WAITS_FILE_NEWFILE,
			   "csect_waits_file_newfile",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p,
			   MNT_STATS_CSECT_WAITS_QPROC_QUERY_TABLE,
			   "csect_waits_qproc_query_table",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_WAITS_BOOT_SR_DBPARM,
			   "csect_waits_boot_sr_dbparm",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p,
			   MNT_STATS_CSECT_WAITS_DISK_REFRESH_GOODVOL,
			   "csect_waits_disk_refresh_goodvol",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_WAITS_CNV_FMT_LEXER,
			   "csect_waits_cnf_fmt_lexer",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_WAITS_CT_OID_TABLE,
			   "csect_waits_ct_oid_table",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_WAITS_HA_SERVER_STATE,
			   "csect_waits_ha_server_state",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_WAITS_SESSION_STATE,
			   "csect_waits_session_state",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_WAITS_ACL,
			   "csect_waits_acl",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_WAITS_EVENT_LOG_FILE,
			   "csect_waits_event_log_file",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_WAITS_ACCESS_STATUS,
			   "csect_waits_access_status",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_WAITS_TEMPFILE_CACHE,
			   "csect_waits_tempfile_cache",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_WAITS_CSS_ACTIVE_CONN,
			   "csect_waits_css_active_conn",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_WAITS_CSS_FREE_CONN,
			   "csect_waits_css_free_conn",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_CSECT_WAITS_UNKNOWN,
			   "csect_waits_unknown",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  /* Statistics at disk level */
  init_monitor_stats_info (info_p, MNT_STATS_DISK_SECTOR_ALLOCS,
			   "disk_sector_allocs",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_DISK_SECTOR_DEALLOCS,
			   "disk_sector_deallocs",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_DISK_PAGE_ALLOCS,
			   "disk_page_allocs",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_DISK_PAGE_DEALLOCS,
			   "disk_page_deallocs",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_DISK_TEMP_EXPAND,
			   "disk_temp_expand",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  /* Statistics at file io level */
  init_monitor_stats_info (info_p, MNT_STATS_FILE_CREATES,
			   "file_creates", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_FILE_REMOVES,
			   "file_removes", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_FILE_IOREADS,
			   "file_ioreads",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_FILE_IOWRITES,
			   "file_iowrites",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_FILE_IOSYNCHES,
			   "file_iosynches", MONITOR_STATS_VALUE_COUNTER);

  /* Statistics at page level */
  init_monitor_stats_info (info_p, MNT_STATS_DATA_PAGE_FETCHES,
			   "datapage_fetch",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p,
			   MNT_STATS_DATA_PAGE_FETCHES_FILE_HEADER,
			   "datapage_fetch_file_header",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_DATA_PAGE_FETCHES_FILE_TAB,
			   "datapage_fetch_file_tab",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p,
			   MNT_STATS_DATA_PAGE_FETCHES_HEAP_HEADER,
			   "datapage_fetch_heap_header",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_DATA_PAGE_FETCHES_HEAP,
			   "datapage_fetch_heap",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_DATA_PAGE_FETCHES_VOLHEADER,
			   "datapage_fetch_volheader",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_DATA_PAGE_FETCHES_VOLBITMAP,
			   "datapage_fetch_volbitmap",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_DATA_PAGE_FETCHES_XASL,
			   "datapage_fetch_xasl",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_DATA_PAGE_FETCHES_QRESULT,
			   "datapage_fetch_qresult",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_DATA_PAGE_FETCHES_EHASH,
			   "datapage_fetch_ehash",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_DATA_PAGE_FETCHES_OVERFLOW,
			   "datapage_fetch_overflow",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_DATA_PAGE_FETCHES_AREA,
			   "datapage_fetch_area",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_DATA_PAGE_FETCHES_CATALOG,
			   "datapage_fetch_catalog",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_DATA_PAGE_FETCHES_BTREE_ROOT,
			   "datapage_fetch_btree_root",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_DATA_PAGE_FETCHES_BTREE,
			   "datapage_fetch_btree",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_DATA_PAGE_FETCHES_UNKNOWN,
			   "datapage_fetch_unknown",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p,
			   MNT_STATS_DATA_PAGE_FETCHES_WAITS_FILE_HEADER,
			   "datapage_fetch_waits_file_header",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p,
			   MNT_STATS_DATA_PAGE_FETCHES_WAITS_FILE_TAB,
			   "datapage_fetch_waits_file_tab",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p,
			   MNT_STATS_DATA_PAGE_FETCHES_WAITS_HEAP_HEADER,
			   "datapage_fetch_waits_heap_header",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_DATA_PAGE_FETCHES_WAITS_HEAP,
			   "datapage_fetch_waits_heap",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p,
			   MNT_STATS_DATA_PAGE_FETCHES_WAITS_VOLHEADER,
			   "datapage_fetch_waits_volheader",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p,
			   MNT_STATS_DATA_PAGE_FETCHES_WAITS_VOLBITMAP,
			   "datapage_fetch_waits_volbitmap",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_DATA_PAGE_FETCHES_WAITS_XASL,
			   "datapage_fetch_waits_xasl",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p,
			   MNT_STATS_DATA_PAGE_FETCHES_WAITS_QRESULT,
			   "datapage_fetch_waits_qresult",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p,
			   MNT_STATS_DATA_PAGE_FETCHES_WAITS_EHASH,
			   "datapage_fetch_waits_ehash",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p,
			   MNT_STATS_DATA_PAGE_FETCHES_WAITS_OVERFLOW,
			   "datapage_fetch_waits_overflow",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_DATA_PAGE_FETCHES_WAITS_AREA,
			   "datapage_fetch_waits_area",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p,
			   MNT_STATS_DATA_PAGE_FETCHES_WAITS_CATALOG,
			   "datapage_fetch_waits_catalog",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p,
			   MNT_STATS_DATA_PAGE_FETCHES_WAITS_BTREE_ROOT,
			   "datapage_fetch_waits_btree_root",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p,
			   MNT_STATS_DATA_PAGE_FETCHES_WAITS_BTREE,
			   "datapage_fetch_waits_btree",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p,
			   MNT_STATS_DATA_PAGE_FETCHES_WAITS_UNKNOWN,
			   "datapage_fetch_waits_unknown",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p,
			   MNT_STATS_DATA_PAGE_FETCHES_TRACK_FILE_ALLOCSET_ALLOC_PAGES,
			   "datapage_fetch_track_file_allocset_alloc_pages",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p,
			   MNT_STATS_DATA_PAGE_FETCHES_TRACK_FILE_ALLOC_PAGES,
			   "datapage_fetch_track_file_alloc_pages",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p,
			   MNT_STATS_DATA_PAGE_FETCHES_TRACK_FILE_DEALLOC_PAGE,
			   "datapage_fetch_track_file_dealloc_page",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p,
			   MNT_STATS_DATA_PAGE_FETCHES_TRACK_HEAP_FIND_BEST_PAGE,
			   "datapage_fetch_track_heap_find_best_page",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p,
			   MNT_STATS_DATA_PAGE_FETCHES_TRACK_HEAP_BESTSPACE_SYNC,
			   "datapage_fetch_track_heap_bestspace_sync",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p,
			   MNT_STATS_DATA_PAGE_FETCHES_TRACK_HEAP_OVF_INSERT,
			   "datapage_fetch_track_heap_ovf_insert",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p,
			   MNT_STATS_DATA_PAGE_FETCHES_TRACK_HEAP_OVF_UPDATE,
			   "datapage_fetch_track_heap_ovf_update",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p,
			   MNT_STATS_DATA_PAGE_FETCHES_TRACK_HEAP_OVF_DELETE,
			   "datapage_fetch_track_heap_ovf_delete",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p,
			   MNT_STATS_DATA_PAGE_FETCHES_TRACK_BTREE_MERGE_LEVEL,
			   "datapage_fetch_track_btree_merge_level",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p,
			   MNT_STATS_DATA_PAGE_FETCHES_TRACK_BTREE_LOAD_DATA,
			   "datapage_fetch_track_btree_load_data",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p,
			   MNT_STATS_DATA_PAGE_FETCHES_TRACK_PGBUF_FLUSH_CHECKPOINT,
			   "datapage_fetch_track_pgbuf_flush_checkpoint",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p,
			   MNT_STATS_DATA_PAGE_FETCHES_TRACK_LOG_ROLLBACK,
			   "datapage_fetch_track_log_rollback",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_DATA_PAGE_DIRTIES,
			   "datapage_dirties", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_DATA_PAGE_IOREADS,
			   "datapage_ioreads",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_DATA_PAGE_IOWRITES,
			   "datapage_iowrites",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_DATA_PAGE_VICTIMS,
			   "datapage_victims", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p,
			   MNT_STATS_DATA_PAGE_IOWRITES_FOR_REPLACEMENT,
			   "datapage_iowrites_for_replacement",
			   MONITOR_STATS_VALUE_COUNTER);

  /* Statistics at log level */
  init_monitor_stats_info (info_p, MNT_STATS_LOG_PAGE_IOREADS,
			   "logpage_ioreads",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_LOG_PAGE_IOWRITES,
			   "logpage_iowrites",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_LOG_APPEND_RECORDS,
			   "logpage_append_records",
			   MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_LOG_ARCHIVES,
			   "logpage_archives", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_LOG_CHECKPOINTS,
			   "event_checkpoints", MONITOR_STATS_VALUE_EVENT);

  init_monitor_stats_info (info_p, MNT_STATS_LOG_WALS,
			   "logpage_wals", MONITOR_STATS_VALUE_COUNTER);

  /* Statistics at lock level */
  init_monitor_stats_info (info_p, MNT_STATS_DDL_LOCKS_REQUESTS,
			   "lock_ddl", MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_CLASS_LOCKS_REQUEST,
			   "lock_table",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_CATALOG_LOCKS_REQUEST,
			   "lock_catalog",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_GLOBAL_LOCKS_REQUEST,
			   "lock_global",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_SHARD_LOCKS_REQUEST,
			   "lock_shard",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_PAGE_LOCKS_ACQUIRED,
			   "lock_page", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_OBJECT_LOCKS_ACQUIRED,
			   "lock_object", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_PAGE_LOCKS_CONVERTED,
			   "lock_page_lock_converted",
			   MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_OBJECT_LOCKS_CONVERTED,
			   "lock_objects_lock_converted",
			   MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_PAGE_LOCKS_RE_REQUESTED,
			   "lock_page_locks_re-requested",
			   MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_OBJECT_LOCKS_RE_REQUESTED,
			   "lock_object_locks_re-requested",
			   MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_PAGE_LOCKS_WAITS,
			   "lock_page_locks_waits",
			   MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_OBJECT_LOCKS_WAITS,
			   "lock_object_locks_waits",
			   MONITOR_STATS_VALUE_COUNTER);

  /* Transaction Management level */
  init_monitor_stats_info (info_p, MNT_STATS_TRAN_COMMITS,
			   "tran_commit", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_TRAN_ROLLBACKS,
			   "tran_rollback", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_TRAN_SAVEPOINTS,
			   "tran_savepoint", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_TRAN_TOPOPS,
			   "tran_topop", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_TRAN_INTERRUPTS,
			   "tran_interrupt", MONITOR_STATS_VALUE_COUNTER);

  /* Statistics at btree level */
  init_monitor_stats_info (info_p, MNT_STATS_BTREE_INSERTS,
			   "btree_insert",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_BTREE_DELETES,
			   "btree_delete",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_BTREE_UPDATES,
			   "btree_update", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_BTREE_LOAD_DATA,
			   "event_create_index", MONITOR_STATS_VALUE_EVENT);

  init_monitor_stats_info (info_p, MNT_STATS_BTREE_COVERED,
			   "btree_covered", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_BTREE_NONCOVERED,
			   "btree_noncovered", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_BTREE_RESUMES,
			   "btree_resume", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p,
			   MNT_STATS_BTREE_MULTIRANGE_OPTIMIZATION,
			   "btree_multirange_optimization",
			   MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_BTREE_SPLITS,
			   "btree_splits",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_BTREE_MERGES,
			   "btree_merge",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_BTREE_PAGE_ALLOCS,
			   "btree_page_allocs",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  init_monitor_stats_info (info_p, MNT_STATS_BTREE_PAGE_DEALLOCS,
			   "btree_page_deallocs",
			   MONITOR_STATS_VALUE_COUNTER_WITH_TIME);

  /* Execution statistics for the query manager */
  init_monitor_stats_info (info_p, MNT_STATS_QUERY_SELECTS,
			   "query_select", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_QUERY_INSERTS,
			   "query_insert", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_QUERY_DELETES,
			   "query_delete", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_QUERY_UPDATES,
			   "query_update", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_QUERY_SSCANS,
			   "query_sscan", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_QUERY_ISCANS,
			   "query_iscan", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_QUERY_LSCANS,
			   "query_lscans", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_QUERY_NLJOINS,
			   "query_nljoins", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_QUERY_HOLDABLE_CURSORS,
			   "query_holdable_cursors",
			   MONITOR_STATS_VALUE_GAUGE);

  /* execution statistics for external sort */
  init_monitor_stats_info (info_p, MNT_STATS_SORT_IO_PAGES,
			   "sort_iopage", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_SORT_DATA_PAGES,
			   "sort_data_page", MONITOR_STATS_VALUE_COUNTER);

  /* Network Communication level */
  init_monitor_stats_info (info_p, MNT_STATS_NETWORK_REQUESTS,
			   "network_request", MONITOR_STATS_VALUE_COUNTER);

  /* Statistics at Flush Control */
  init_monitor_stats_info (info_p, MNT_STATS_ADAPTIVE_FLUSH_PAGES,
			   "flush_control_data_page",
			   MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_ADAPTIVE_FLUSH_LOG_PAGES,
			   "flush_control_log_page",
			   MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_ADAPTIVE_FLUSH_MAX_PAGES,
			   "flush_control_max_page",
			   MONITOR_STATS_VALUE_COUNTER);

  /* Prior LSA */
  init_monitor_stats_info (info_p, MNT_STATS_PRIOR_LSA_LIST_SIZE,
			   "prior_lsa_size", MONITOR_STATS_VALUE_GAUGE);

  init_monitor_stats_info (info_p, MNT_STATS_PRIOR_LSA_LIST_MAXED,
			   "prior_lsa_maxed", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_PRIOR_LSA_LIST_REMOVED,
			   "prior_lsa_removed", MONITOR_STATS_VALUE_COUNTER);

  /* Heap best space info */
  init_monitor_stats_info (info_p, MNT_STATS_HEAP_STATS_BESTSPACE_ENTRIES,
			   "heap_bestspace_entry", MONITOR_STATS_VALUE_GAUGE);

  init_monitor_stats_info (info_p, MNT_STATS_HEAP_STATS_BESTSPACE_MAXED,
			   "heap_bestspace_maxed",
			   MONITOR_STATS_VALUE_COUNTER);

  /* Plan cache */
  init_monitor_stats_info (info_p, MNT_STATS_PLAN_CACHE_ADD,
			   "plan_cache_add", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_PLAN_CACHE_LOOKUP,
			   "plan_cache_lookup", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_PLAN_CACHE_HIT,
			   "plan_cache_hit", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_PLAN_CACHE_MISS,
			   "plan_cache_miss", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_PLAN_CACHE_FULL,
			   "plan_cache_full", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_PLAN_CACHE_DELETE,
			   "plan_cache_delete", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p, MNT_STATS_PLAN_CACHE_INVALID_XASL_ID,
			   "plan_cache_invalid", MONITOR_STATS_VALUE_COUNTER);

  init_monitor_stats_info (info_p,
			   MNT_STATS_PLAN_CACHE_QUERY_STRING_HASH_ENTRIES,
			   "plan_cache_query_string_entry",
			   MONITOR_STATS_VALUE_GAUGE);

  init_monitor_stats_info (info_p,
			   MNT_STATS_PLAN_CACHE_XASL_ID_HASH_ENTRIES,
			   "plan_cache_xasl_id_entry",
			   MONITOR_STATS_VALUE_GAUGE);

  init_monitor_stats_info (info_p,
			   MNT_STATS_PLAN_CACHE_CLASS_OID_HASH_ENTRIES,
			   "plan_cache_table_entry",
			   MONITOR_STATS_VALUE_GAUGE);

  init_monitor_stats_info (info_p, MNT_STATS_DATA_PAGE_BUFFER_HIT_RATIO,
			   "buffer_hit_ratio", MONITOR_STATS_VALUE_COUNTER);

  return meta_p;
}

/*
 * monitor_create_repl_stats_meta -
 *   return: error code
 *
 *   shm_p(in/out):
 */
static MONITOR_STATS_META *
monitor_create_repl_stats_meta (void)
{
  MONITOR_STATS_META *meta_p = NULL;
  MONITOR_STATS_INFO *info_p = NULL;
  int size;
  int num_stats;

  num_stats = monitor_get_num_stats (MONITOR_TYPE_REPL);
  size = (sizeof (MONITOR_STATS_META)
	  + num_stats * sizeof (MONITOR_STATS_INFO));
  meta_p = (MONITOR_STATS_META *) malloc (size);
  if (meta_p == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, size);
      return NULL;
    }
  meta_p->monitor_type = MONITOR_TYPE_REPL;
  meta_p->num_stats = num_stats;

  info_p = meta_p->info;

  init_monitor_stats_info (info_p, MNT_RP_EOF_PAGEID,
			   "ha_eof_pageid", MONITOR_STATS_VALUE_GAUGE);

  init_monitor_stats_info (info_p, MNT_RP_RECEIVED_GAP,
			   "ha_received_gap", MONITOR_STATS_VALUE_GAUGE);

  init_monitor_stats_info (info_p, MNT_RP_RECEIVED_PAGEID,
			   "ha_received_pageid", MONITOR_STATS_VALUE_GAUGE);

  init_monitor_stats_info (info_p, MNT_RP_FLUSHED_GAP,
			   "ha_flushed_gap", MONITOR_STATS_VALUE_GAUGE);

  init_monitor_stats_info (info_p, MNT_RP_FLUSHED_PAGEID,
			   "ha_flushed_pageid", MONITOR_STATS_VALUE_GAUGE);

  init_monitor_stats_info (info_p, MNT_RP_CURRENT_GAP,
			   "ha_current_gap", MONITOR_STATS_VALUE_GAUGE);

  init_monitor_stats_info (info_p, MNT_RP_CURRENT_PAGEID,
			   "ha_current_pageid", MONITOR_STATS_VALUE_GAUGE);

  init_monitor_stats_info (info_p, MNT_RP_REQUIRED_GAP,
			   "ha_required_gap", MONITOR_STATS_VALUE_GAUGE);

  init_monitor_stats_info (info_p, MNT_RP_REQUIRED_PAGEID,
			   "ha_required_pageid", MONITOR_STATS_VALUE_GAUGE);

  init_monitor_stats_info (info_p, MNT_RP_DELAY,
			   "ha_delay_time", MONITOR_STATS_VALUE_GAUGE);

  init_monitor_stats_info (info_p, MNT_RP_QUEUE_FULL,
			   "ha_queue_full", MONITOR_STATS_VALUE_COUNTER);


  init_monitor_stats_info (info_p, MNT_RP_INSERT,
			   "ha_insert", MONITOR_STATS_VALUE_COUNTER);
  init_monitor_stats_info (info_p, MNT_RP_UPDATE,
			   "ha_update", MONITOR_STATS_VALUE_COUNTER);
  init_monitor_stats_info (info_p, MNT_RP_DELETE,
			   "ha_delete", MONITOR_STATS_VALUE_COUNTER);
  init_monitor_stats_info (info_p, MNT_RP_DDL,
			   "ha_ddl", MONITOR_STATS_VALUE_COUNTER);
  init_monitor_stats_info (info_p, MNT_RP_COMMIT,
			   "ha_commit", MONITOR_STATS_VALUE_COUNTER);
  init_monitor_stats_info (info_p, MNT_RP_FAIL,
			   "ha_fail", MONITOR_STATS_VALUE_COUNTER);

  return meta_p;
}

/*
 * monitor_final_monitor_info -
 *   return:
 *
 *   monitor(in/out):
 */
static void
monitor_final_monitor_info (MONITOR_INFO * monitor)
{
  if (monitor == NULL)
    {
      return;
    }

  if (monitor->meta != NULL)
    {
      free_and_init (monitor->meta);
    }

  if (monitor->monitor_shm != NULL)
    {
      rye_shm_detach (monitor->monitor_shm);
      monitor->monitor_shm = NULL;
    }

  free_and_init (monitor);
}

/******************************************************************
 * MONITOR COLLECTOR
 ******************************************************************/

/*
 * monitor_create_collector
 *   return: error code
 *
 *   name(in):
 *   num_monitors(in):
 *   num_stats(in):
 */
int
monitor_create_collector (const char *name, int num_monitors,
			  MONITOR_TYPE monitor_type)
{
  RYE_MONITOR_SHM *shm_p = NULL;
  MONITOR_STATS_META *stats_meta = NULL;
  int error = NO_ERROR;

  if (mntCollector != NULL)
    {
      assert (false);
      free_and_init (mntCollector);
    }

  if (strlen (name) >= SHM_NAME_SIZE)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "Invalid monitor name length");
      goto exit_on_error;
    }

  shm_p = monitor_create_shm (name, num_monitors, monitor_type);
  if (shm_p == NULL)
    {
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "Cannot create shared memory");
      goto exit_on_error;
    }

  stats_meta = monitor_create_stats_meta (monitor_type);
  if (stats_meta == NULL)
    {
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "Cannot create stats_meta");
      goto exit_on_error;
    }

  mntCollector = (MONITOR_INFO *) malloc (sizeof (MONITOR_INFO));
  if (mntCollector == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, sizeof (MONITOR_INFO));
      goto exit_on_error;
    }

  mntCollector->shm_key = shm_p->shm_header.shm_key;
  mntCollector->meta = stats_meta;
  mntCollector->monitor_shm = shm_p;

  return NO_ERROR;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "Invalid error code");
    }

  if (stats_meta != NULL)
    {
      free_and_init (stats_meta);
    }

  if (shm_p != NULL)
    {
      rye_shm_detach (shm_p);
      shm_p = NULL;
    }

  if (mntCollector != NULL)
    {
      free_and_init (mntCollector);
    }

  return error;
}

/*
 * monitor_final_collector -
 *   return: error code
 */
void
monitor_final_collector (void)
{
  monitor_final_monitor_info (mntCollector);

  mntCollector = NULL;
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
  MONITOR_STATS_INFO *info = NULL;
  MONITOR_STATS *stats = NULL;
  int item_index;
  UNUSED_VAR INT64 after_value;
  UNUSED_VAR UINT64 after_exec_time;
  UINT64 end_time, exec_time;

  if (mntCollector == NULL || mntCollector->meta == NULL
      || mntCollector->monitor_shm == NULL)
    {
      return;
    }

  if (item < 0 || item >= mntCollector->monitor_shm->num_stats
      || mnt_id <= 0 || mnt_id > mntCollector->monitor_shm->num_monitors)
    {
      assert (false);
      return;
    }

  info = &mntCollector->meta->info[item];

  if (info->value_type != MONITOR_STATS_VALUE_COUNTER
      && info->value_type != MONITOR_STATS_VALUE_COUNTER_WITH_TIME
      && info->value_type != MONITOR_STATS_VALUE_EVENT)
    {
      assert (false);
      return;
    }

  if (info->value_type == MONITOR_STATS_VALUE_COUNTER_WITH_TIME)
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
  item_index = mnt_id * mntCollector->monitor_shm->num_stats + item;
  stats = &mntCollector->monitor_shm->stats[item_index];

  after_value = ATOMIC_INC_64 (&stats->value, value);
  after_exec_time = ATOMIC_INC_64 (&stats->acc_time, exec_time);

  /* global stats */
  stats = &mntCollector->monitor_shm->stats[item];
  after_value = ATOMIC_INC_64 (&stats->value, value);
  after_exec_time = ATOMIC_INC_64 (&stats->acc_time, exec_time);
}

/*
 * monitor_stats_gauge -
 */
void
monitor_stats_gauge (int mnt_id, int item, INT64 value)
{
  MONITOR_STATS_INFO *info = NULL;
  MONITOR_STATS *stats = NULL;
  int item_index;
  UNUSED_VAR INT64 after_value;

  if (mntCollector == NULL || mntCollector->meta == NULL
      || mntCollector->monitor_shm == NULL)
    {
      return;
    }

  if (item < 0 || item >= mntCollector->monitor_shm->num_stats
      || mnt_id <= 0 || mnt_id > mntCollector->monitor_shm->num_monitors)
    {
      assert (false);
      return;
    }

  info = &mntCollector->meta->info[item];

  if (info->value_type != MONITOR_STATS_VALUE_GAUGE)
    {
      assert (false);
      return;
    }

  /* thread stats */
  item_index = mnt_id * mntCollector->monitor_shm->num_stats + item;
  stats = &mntCollector->monitor_shm->stats[item_index];
  after_value = ATOMIC_TAS_64 (&stats->value, value);


  /* global stats */
  stats = &mntCollector->monitor_shm->stats[item];
  after_value = ATOMIC_TAS_64 (&stats->value, value);
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
  MONITOR_STATS *stats = NULL;
  int item_index;

  if (mntCollector == NULL || mntCollector->meta == NULL
      || mntCollector->monitor_shm == NULL)
    {
      return 0;
    }

  if (item < 0 || item >= mntCollector->monitor_shm->num_stats
      || mnt_id <= 0 || mnt_id > mntCollector->monitor_shm->num_monitors)
    {
      assert (false);
      return 0;
    }

  /* thread stats */
  item_index = mnt_id * mntCollector->monitor_shm->num_stats + item;

  stats = &mntCollector->monitor_shm->stats[item_index];
  if (acc_time != NULL)
    {
      *acc_time = stats->acc_time;
    }

  return stats->value;
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
 */
MONITOR_INFO *
monitor_create_viewer_from_name (const char *name)
{
  if (strlen (name) >= SHM_NAME_SIZE)
    {
      assert (false);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR,
	      1, "Invalid monitor name length");

      return NULL;
    }

  return monitor_create_viewer (name, -1);
}

/*
 * monitor_create_viewer_from_key -
 *   return: MONITOR_INFO
 *
 *   shm_key(in):
 */
MONITOR_INFO *
monitor_create_viewer_from_key (int shm_key)
{
  return monitor_create_viewer (NULL, shm_key);
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
monitor_create_viewer (const char *name, int shm_key)
{
  MONITOR_INFO *monitor = NULL;
  RYE_MONITOR_SHM *shm_p = NULL;
  MONITOR_STATS_META *stats_meta = NULL;
  int error = NO_ERROR;

  if (name == NULL && shm_key < 0)
    {
      assert (false);
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "Invalid arguments");
      goto exit_on_error;
    }

  if (name != NULL)
    {
      shm_key = rye_master_shm_get_shm_key (name, RYE_SHM_TYPE_MONITOR);
    }

  shm_p = rye_shm_attach (shm_key, RYE_SHM_TYPE_MONITOR, true);
  if (shm_p == NULL)
    {
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "fail shm_attach");
      goto exit_on_error;
    }

  stats_meta = monitor_create_stats_meta (shm_p->monitor_type);
  if (stats_meta == NULL)
    {
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
	      "fail create_stats_info");
      goto exit_on_error;
    }

  monitor = (MONITOR_INFO *) malloc (sizeof (MONITOR_INFO));
  if (monitor == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, sizeof (MONITOR_INFO));
      goto exit_on_error;
    }

  monitor->shm_key = shm_key;
  monitor->meta = stats_meta;
  monitor->monitor_shm = shm_p;

  return monitor;

exit_on_error:
  if (shm_p != NULL)
    {
      rye_shm_detach (shm_p);
      shm_p = NULL;
    }
  if (stats_meta != NULL)
    {
      free_and_init (stats_meta);
    }
  if (monitor != NULL)
    {
      free_and_init (monitor);
    }

  return NULL;
}

/*
 * monitor_final_viewer
 */
void
monitor_final_viewer (MONITOR_INFO * monitor_info)
{
  monitor_final_monitor_info (monitor_info);
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
  if (monitor == NULL || monitor->meta == NULL || monitor->shm_key <= 0
      || item < 0 || item >= monitor->meta->num_stats)
    {
      assert (false);
      return false;
    }

  return IS_CUMMULATIVE_VALUE (monitor->meta->info[item].value_type);
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
  if (monitor == NULL || monitor->meta == NULL || monitor->shm_key <= 0
      || item < 0 || item >= monitor->meta->num_stats)
    {
      assert (false);
      return false;
    }

  return (IS_COLLECTING_TIME (monitor->meta->info[item].value_type));
}

/*
 * monitor_open_viewer_data -
 *    return: error code
 *
 *    monitor(in/out):
 */
int
monitor_open_viewer_data (MONITOR_INFO * monitor)
{
  RYE_MONITOR_SHM *shm_p = NULL;

  if (monitor == NULL || monitor->meta == NULL || monitor->shm_key <= 0)
    {
      assert (false);
      return ER_FAILED;
    }

  if (monitor->monitor_shm != NULL)
    {
      shm_p = monitor->monitor_shm;
    }
  else
    {
      shm_p = rye_shm_attach (monitor->shm_key, RYE_SHM_TYPE_MONITOR, true);
      if (shm_p == NULL)
	{
	  return ER_FAILED;
	}
    }

  if (monitor->meta->monitor_type != shm_p->monitor_type)
    {
      assert (false);

      rye_shm_detach (shm_p);

      monitor->monitor_shm = NULL;

      return ER_FAILED;
    }

  monitor->monitor_shm = shm_p;

  return NO_ERROR;
}

/*
 * monitor_close_viewer_data -
 *    return: error code
 *
 *    monitor(in/out):
 */
int
monitor_close_viewer_data (MONITOR_INFO * monitor)
{
  if (monitor == NULL || monitor->meta == NULL || monitor->shm_key <= 0)
    {
      assert (false);
      return ER_FAILED;
    }

  if (monitor->monitor_shm != NULL)
    {
      rye_shm_detach (monitor->monitor_shm);
      monitor->monitor_shm = NULL;
    }

  return NO_ERROR;
}

/*
 * monitor_copy_stats ()-
 *   return:
 *
 *   monitor(in/out):
 *   to_stats(out):
 *   int mnt_id(in):
 */
int
monitor_copy_stats (MONITOR_INFO * monitor,
		    MONITOR_STATS * to_stats, int mnt_id)
{
  RYE_MONITOR_SHM *shm_p = NULL;
  MONITOR_STATS_META *meta_p = NULL;
  int error = NO_ERROR;

  if (to_stats == NULL)
    {
      assert (false);
      return ER_FAILED;
    }

  if (monitor == NULL)
    {
      shm_p = mntCollector->monitor_shm;
      meta_p = mntCollector->meta;
    }
  else
    {
      error = monitor_open_viewer_data (monitor);
      if (error != NO_ERROR)
	{
	  return error;
	}
      shm_p = monitor->monitor_shm;
      meta_p = monitor->meta;
    }

  if (shm_p == NULL || meta_p == NULL
      || mnt_id <= 0 || mnt_id > shm_p->num_monitors)
    {
      assert (false);

      return ER_FAILED;
    }

  /* copy thread stats, start point: monitor_shm, copy length: meta */
  memcpy (to_stats, &shm_p->stats[mnt_id * shm_p->num_stats],
	  sizeof (MONITOR_STATS) * meta_p->num_stats);

  return error;
}

/*
 * monitor_copy_global_stats ()-
 *   return:
 *
 *   monitor(in/out):
 *   to_stats(out):
 */
int
monitor_copy_global_stats (MONITOR_INFO * monitor, MONITOR_STATS * to_stats)
{
  RYE_MONITOR_SHM *shm_p = NULL;
  MONITOR_STATS_META *meta_p = NULL;
  int error = NO_ERROR;

  if (to_stats == NULL)
    {
      assert (false);
      return ER_FAILED;
    }

  if (monitor == NULL)
    {
      shm_p = mntCollector->monitor_shm;
      meta_p = mntCollector->meta;
    }
  else
    {
      error = monitor_open_viewer_data (monitor);
      if (error != NO_ERROR)
	{
	  return error;
	}
      shm_p = monitor->monitor_shm;
      meta_p = monitor->meta;
    }
  if (shm_p == NULL || meta_p == NULL)
    {
      assert (false);

      return ER_FAILED;
    }

  /* copy global stats */
  memcpy (to_stats, &shm_p->stats[0],
	  sizeof (MONITOR_STATS) * meta_p->num_stats);

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
 */
int
monitor_diff_stats (MONITOR_INFO * monitor, MONITOR_STATS * diff_stats,
		    MONITOR_STATS * new_stats, MONITOR_STATS * old_stats)
{
  MONITOR_STATS_META *meta_p = NULL;
  int i;

  if (diff_stats == NULL || new_stats == NULL || old_stats == NULL)
    {
      assert (0);
      return ER_FAILED;
    }

  if (monitor == NULL)
    {
      monitor = mntCollector;
    }
  if (monitor == NULL || monitor->meta == NULL)
    {
      assert (0);
      return ER_FAILED;
    }

  meta_p = monitor->meta;
  for (i = 0; i < meta_p->num_stats; i++)
    {
      /* calc diff values */
      if (IS_CUMMULATIVE_VALUE (meta_p->info[i].value_type))
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
 *   stream(out):
 *   monitor(in):
 *   stats(in):
 *   header(in):
 *   tail(in):
 *   substr(in):
 */
void
monitor_dump_stats (FILE * stream, MONITOR_INFO * monitor,
		    MONITOR_STATS * cur_stats, MONITOR_STATS * old_stats,
		    int cumulative, MONITOR_DUMP_TYPE dump_type,
		    const char *substr,
		    void (*calc_func) (MONITOR_STATS * stats, int num_stats))
{
  MONITOR_STATS *diff_stats = NULL;
  char stat_buf[16 * ONE_K];
  int num_stats;

  stat_buf[0] = '\0';

  if (monitor == NULL)
    {
      monitor = mntCollector;
    }
  if (monitor->meta == NULL)
    {
      return;
    }
  num_stats = monitor->meta->num_stats;

  if (dump_type == MNT_DUMP_TYPE_CSV_HEADER)
    {
      monitor_dump_stats_to_buffer (monitor, stat_buf, sizeof (stat_buf),
				    NULL, dump_type, substr);
    }
  else
    {

      if (cumulative)
	{
	  if (calc_func != NULL)
	    {
	      calc_func (cur_stats, num_stats);
	    }
	  monitor_dump_stats_to_buffer (monitor, stat_buf, sizeof (stat_buf),
					cur_stats, dump_type, substr);
	}
      else
	{
	  diff_stats =
	    (MONITOR_STATS *) malloc (sizeof (MONITOR_STATS) * num_stats);
	  if (diff_stats == NULL)
	    {
	      return;
	    }

	  if (monitor_diff_stats (monitor, diff_stats, cur_stats,
				  old_stats) != NO_ERROR)
	    {
	      free_and_init (diff_stats);
	      return;
	    }

	  if (calc_func != NULL)
	    {
	      calc_func (diff_stats, num_stats);
	    }
	  monitor_dump_stats_to_buffer (monitor, stat_buf, sizeof (stat_buf),
					diff_stats, dump_type, substr);
	}
    }

  fprintf (stream, "%s", stat_buf);

  if (diff_stats != NULL)
    {
      free_and_init (diff_stats);
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
 *   header(in):
 *   tail(in):
 *   substr(in):
 */
void
monitor_dump_stats_to_buffer (MONITOR_INFO * monitor,
			      char *buffer, int buf_size,
			      MONITOR_STATS * stats,
			      MONITOR_DUMP_TYPE dump_type, const char *substr)
{
  if (buffer == NULL || buf_size <= 0)
    {
      assert (false);
      return;
    }

  if (monitor == NULL)
    {
      monitor = mntCollector;
    }

  switch (dump_type)
    {
    case MNT_DUMP_TYPE_NORMAL:
      monitor_dump_stats_normal (buffer, buf_size, monitor, stats, substr);
      break;
    case MNT_DUMP_TYPE_CSV_HEADER:
      monitor_dump_stats_csv_header (buffer, buf_size, monitor, substr);
      break;
    case MNT_DUMP_TYPE_CSV_DATA:
      monitor_dump_stats_csv (buffer, buf_size, monitor, stats, substr);
      break;
    default:
      assert (false);
      break;
    }
}

/*
 * monitor_dump_stats_normal -
 *   return:
 *
 *   buffer(out):
 *   buf_size(in):
 *   monitor(in):
 *   stats(in):
 *   substr(in):
 */
static void
monitor_dump_stats_normal (char *buffer, int buf_size,
			   MONITOR_INFO * monitor, MONITOR_STATS * stats,
			   const char *substr)
{
  MONITOR_STATS_META *meta_p;
  char time_array[256];
  char *out;
  const char *str;
  int remained_size, ret;
  int i;

  if (buffer == NULL || monitor == NULL || stats == NULL)
    {
      assert (false);
      return;
    }

  out = buffer;
  remained_size = buf_size - 1;

  er_datetime (NULL, time_array, sizeof (time_array));
  ret = snprintf (out, remained_size, "%s\n", time_array);
  remained_size -= ret;
  out += ret;
  if (remained_size <= 0)
    {
      return;
    }

  meta_p = monitor->meta;
  for (i = 0; i < meta_p->num_stats; i++)
    {
      if (substr != NULL)
	{
	  str = strstr (meta_p->info[i].name, substr);
	}
      else
	{
	  str = meta_p->info[i].name;
	}

      if (str != NULL)
	{
	  ret = snprintf (out, remained_size, "%-29s = %10lld\n",
			  meta_p->info[i].name, (long long) stats[i].value);
	  remained_size -= ret;
	  out += ret;
	  if (remained_size <= 0)
	    {
	      return;
	    }
	}
    }

  buffer[buf_size - 1] = '\0';
}

/*
 * monitor_dump_stats_csv_header -
 *   return:
 *
 *   buffer(out):
 *   buf_size(in):
 *   monitor(in):
 *   stats(in):
 *   substr(in):
 */
static void
monitor_dump_stats_csv_header (char *buffer, int buf_size,
			       MONITOR_INFO * monitor, const char *substr)
{
  MONITOR_STATS_META *meta_p;
  char *out;
  const char *str;
  int remained_size, ret;
  int i;

  if (buffer == NULL || monitor == NULL)
    {
      assert (false);
      return;
    }

  out = buffer;
  remained_size = buf_size - 1;

  ret = snprintf (out, remained_size, ",time");
  remained_size -= ret;
  out += ret;
  if (remained_size <= 0)
    {
      return;
    }

  meta_p = monitor->meta;
  for (i = 0; i < meta_p->num_stats; i++)
    {
      if (substr != NULL)
	{
	  str = strstr (meta_p->info[i].name, substr);
	}
      else
	{
	  str = meta_p->info[i].name;
	}

      if (str != NULL)
	{
	  ret = snprintf (out, remained_size, ",%s", meta_p->info[i].name);
	  remained_size -= ret;
	  out += ret;
	  if (remained_size <= 0)
	    {
	      return;
	    }
	}
    }

  buffer[buf_size - 1] = '\0';
}

/*
 * monitor_dump_stats_csv -
 *   return:
 *
 *   buffer(out):
 *   buf_size(in):
 *   monitor(in):
 *   stats(in):
 *   substr(in):
 */
static void
monitor_dump_stats_csv (char *buffer, int buf_size,
			MONITOR_INFO * monitor, MONITOR_STATS * stats,
			const char *substr)
{
  MONITOR_STATS_META *meta_p = NULL;
  char time_array[256];
  char *out;
  const char *str;
  int remained_size, ret;
  int i;

  if (buffer == NULL || monitor == NULL || stats == NULL)
    {
      assert (false);
      return;
    }

  out = buffer;
  remained_size = buf_size - 1;

  er_datetime (NULL, time_array, sizeof (time_array));
  ret = snprintf (out, remained_size, ",%s", time_array);
  remained_size -= ret;
  out += ret;
  if (remained_size <= 0)
    {
      return;
    }

  meta_p = monitor->meta;
  for (i = 0; i < meta_p->num_stats; i++)
    {
      if (substr != NULL)
	{
	  str = strstr (meta_p->info[i].name, substr);
	}
      else
	{
	  str = meta_p->info[i].name;
	}

      if (str != NULL)
	{
	  ret = snprintf (out, remained_size, ",%ld", stats[i].value);
	  remained_size -= ret;
	  out += ret;
	  if (remained_size <= 0)
	    {
	      return;
	    }
	}
    }

  buffer[buf_size - 1] = '\0';
}
