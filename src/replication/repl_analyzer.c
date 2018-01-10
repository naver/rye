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
 * repl_analyzer.c -
 */

#ident "$Id$"

#include <unistd.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <fcntl.h>
#include <errno.h>

#include "connection_support.h"
#include "network_interface_cl.h"
#include "transform.h"
#include "util_func.h"
#include "file_io_lock.h"

#include "cas_common.h"

#include "cci_util.h"
#include "cas_cci_internal.h"

#include "repl_page_buffer.h"

#include "repl.h"
#include "repl_common.h"
#include "repl_analyzer.h"
#include "repl_applier.h"
#include "repl_queue.h"

#include "rye_master_shm.h"

#define LA_TRAN_TABLE_MHT_SIZE                  100000
#define LA_LOCK_SUFFIX                          "_lgla__lock"


#define MAX_COMMITTED_ARRAY                     1024

typedef struct cirp_tran CIRP_TRAN;
struct cirp_tran
{
  int tranid;
  LOG_LSA tran_start_lsa;
  LOG_LSA tran_end_lsa;		/* LOG_UNLOCK_COMMIT */
  LOG_LSA repl_start_lsa;
  int applier_index;
  CIRP_REPL_ITEM *repl_item;
};

typedef struct _cirp_tran_committed_array CIRP_TRAN_COMMITTED_ARRAY;
struct _cirp_tran_committed_array
{
  LOG_LSA lowest_committed_lsa;
  int num_tran;
  int applier_index;

  int tran_ids[MAX_COMMITTED_ARRAY];
};

#define RP_IS_BLOCKED_ITEM(item)                                                           \
   ( ((item)->item_type == RP_ITEM_TYPE_DDL && (item)->info.ddl.ddl_type == REPL_BLOCKED_DDL)   \
      || ((item)->item_type == RP_ITEM_TYPE_DATA && (item)->info.data.groupid == GLOBAL_GROUPID)\
   )

static void cirp_init_tran (CIRP_TRAN * tran);
static int cirp_anlz_update_progress_from_appliers (CIRP_ANALYZER_INFO *
						    analyzer);
static void cirp_free_tran_by_tranid (CIRP_ANALYZER_INFO * analyzer,
				      int tranid);
static int cirp_change_state (CIRP_ANALYZER_INFO * analyzer,
			      HA_STATE curr_node_state);

static bool cirp_anlz_is_any_applier_busy (void);
static int cirp_anlz_assign_repl_item (int tranid, int rectype,
				       LOG_LSA * commit_lsa);
static CIRP_TRAN *cirp_find_tran (int tranid);
static int cirp_add_tran (CIRP_TRAN ** tran, int tranid);
static int cirp_anlz_get_applier_index (int *index,
					const DB_VALUE * shard_key,
					int num_appliers);
static int cirp_check_duplicated (int *lockf_vdes, const char *logpath,
				  const char *dbname);

static int rp_set_repl_log (LOG_PAGE * log_pgptr, int log_type, int tranid,
			    const LOG_LSA * lsa);

static int cirp_lock_dbname (CIRP_ANALYZER_INFO * analyzer);
static int cirp_unlock_dbname (CIRP_ANALYZER_INFO * analyzer,
			       bool clear_owner);

static int cirp_anlz_log_commit (void);
static int cirp_delay_replica (time_t eot_time);
static int cirp_analyze_log_record (LOG_RECORD_HEADER * lrec,
				    LOG_LSA final, LOG_PAGE * pg_ptr);
static int cirp_find_committed_tran (const void *key, void *data, void *args);
static int cirp_free_tran (const void *key, void *data, void *args);
static int cirp_find_lowest_tran_start_lsa (const void *key, void *data,
					    void *args);
static INT64 rp_get_source_applied_time (RQueue * q_applied_time,
					 LOG_LSA * required_lsa);
static int rp_applied_time_node_free (void *node, UNUSED_ARG void *data);
static int cirp_change_analyzer_status (CIRP_ANALYZER_INFO * analyzer,
					CIRP_AGENT_STATUS status);
static int rp_set_schema_log (CIRP_BUF_MGR * buf_mgr, CIRP_TRAN * tran,
			      LOG_PAGE * log_pgptr, const LOG_LSA * lsa);
static int rp_set_data_log (CIRP_BUF_MGR * buf_mgr, CIRP_TRAN * tran,
			    LOG_PAGE * log_pgptr, const LOG_LSA * lsa);
static int rp_set_gid_bitmap_log (CIRP_TRAN * tran, const LOG_LSA * lsa);

/*
 * cirp_get_analyzer_status ()-
 *   return: ha agent status
 *
 *   analyzer(in):
 */
CIRP_AGENT_STATUS
cirp_get_analyzer_status (CIRP_ANALYZER_INFO * analyzer)
{
  CIRP_AGENT_STATUS status;

  pthread_mutex_lock (&analyzer->lock);
  status = analyzer->status;
  pthread_mutex_unlock (&analyzer->lock);

  return status;
}

/*
 * cirp_change_analyzer_status ()-
 *    return: NO_ERROR
 *
 *    analyzer(in/out):
 *    status(in):
 */
static int
cirp_change_analyzer_status (CIRP_ANALYZER_INFO * analyzer,
			     CIRP_AGENT_STATUS status)
{
  pthread_mutex_lock (&analyzer->lock);
  analyzer->status = status;
  pthread_mutex_unlock (&analyzer->lock);

  return NO_ERROR;
}

/*
 * cirp_init_tran ()
 *
 *   apply(out):
 */
static void
cirp_init_tran (CIRP_TRAN * tran)
{
  assert (tran);

  LSA_SET_NULL (&tran->tran_start_lsa);
  LSA_SET_NULL (&tran->tran_end_lsa);
  LSA_SET_NULL (&tran->repl_start_lsa);
  tran->tranid = 0;
  tran->applier_index = -1;
  tran->repl_item = NULL;

  return;
}

/*
 * cirp_free_tran()-
 *    return: NO_ERROR
 *
 *    key(in):
 *    data(in/out):
 *    args(in):
 */
static int
cirp_free_tran (UNUSED_ARG const void *key, void *data, UNUSED_ARG void *args)
{
  CIRP_TRAN *tran;

  tran = (CIRP_TRAN *) data;

  if (tran->repl_item != NULL)
    {
      cirp_free_repl_item (tran->repl_item);
      tran->repl_item = NULL;
    }

  RYE_FREE_MEM (tran);

  return NO_ERROR;
}

/*
 * cirp_find_committed_tran()-
 *    return: error code
 *
 *    key(in):
 *    data(in):
 *    args(in):
 */
static int
cirp_find_committed_tran (UNUSED_ARG const void *key, void *data, void *args)
{
  CIRP_TRAN *tran;
  CIRP_TRAN_COMMITTED_ARRAY *committed_tran;

  tran = (CIRP_TRAN *) data;
  committed_tran = (CIRP_TRAN_COMMITTED_ARRAY *) args;

  if (tran->tranid > 0
      && tran->applier_index == committed_tran->applier_index
      && !LSA_ISNULL (&tran->tran_end_lsa)
      && LSA_LE (&tran->tran_end_lsa, &committed_tran->lowest_committed_lsa))
    {
      committed_tran->tran_ids[committed_tran->num_tran] = tran->tranid;
      committed_tran->num_tran++;

      if (committed_tran->num_tran >= MAX_COMMITTED_ARRAY)
	{
	  return ER_FAILED;
	}
    }

  return NO_ERROR;
}

/*
 * cirp_find_lowest_tran_start_lsa()-
 *    return: NO_ERROR
 *
 *    key(in):
 *    data(in):
 *    args(out): LOG_LSA type
 */
static int
cirp_find_lowest_tran_start_lsa (UNUSED_ARG const void *key,
				 void *data, void *args)
{
  CIRP_TRAN *tran;
  LOG_LSA *lowest_tran_start_lsa;

  tran = (CIRP_TRAN *) data;
  lowest_tran_start_lsa = (LOG_LSA *) args;

  if (LSA_ISNULL (lowest_tran_start_lsa)
      || LSA_GT (lowest_tran_start_lsa, &tran->tran_start_lsa))
    {
      LSA_COPY (lowest_tran_start_lsa, &tran->tran_start_lsa);
    }

  return NO_ERROR;
}

/*
 * rp_get_source_applied_time -
 *   return: applied time
 *
 *   q_applied_time(in/out):
 *   required_lsa(in):
 */
static INT64
rp_get_source_applied_time (RQueue * q_applied_time, LOG_LSA * required_lsa)
{
  INT64 applied_time = 0;
  RP_APPLIED_TIME_NODE *node = NULL;

  if (q_applied_time == NULL || required_lsa == NULL)
    {
      assert (false);
      return 0;
    }

  while (true)
    {
      node = (RP_APPLIED_TIME_NODE *) Rye_queue_get_first (q_applied_time);
      if (node == NULL || LSA_GT (&node->applied_lsa, required_lsa))
	{
	  break;
	}

      node = Rye_queue_dequeue (q_applied_time);
      assert (node != NULL && LSA_LE (&node->applied_lsa, required_lsa));
      applied_time = node->applied_time;

      free_and_init (node);
    }

  return applied_time;
}

/*
 * cirp_anlz_update_progress_from_appliers ()-
 *    return: error code
 *
 *    analyzer(in/out):
 */
static int
cirp_anlz_update_progress_from_appliers (CIRP_ANALYZER_INFO * analyzer)
{
  int i, j;
  LOG_LSA lowest_tran_start_lsa;
  LOG_LSA applier_committed_lsa;
  CIRP_TRAN_COMMITTED_ARRAY committed_tran;
  int error = NO_ERROR;
  CIRP_APPLIER_INFO *applier = NULL;
  INT64 source_applied_time;
  struct timespec cur_time;

  for (i = 0; i < Repl_Info->num_applier; i++)
    {
      applier = &Repl_Info->applier_info[i];
      error = cirp_appl_get_committed_lsa (applier, &applier_committed_lsa);
      if (error != NO_ERROR)
	{
	  return error;
	}

      if (LSA_ISNULL (&applier_committed_lsa))
	{
	  continue;
	}

      do
	{
	  committed_tran.num_tran = 0;
	  committed_tran.applier_index = i;
	  LSA_COPY (&committed_tran.lowest_committed_lsa,
		    &applier_committed_lsa);
	  (void) mht_map (analyzer->tran_table,
			  cirp_find_committed_tran, &committed_tran);

	  for (j = 0; j < committed_tran.num_tran; j++)
	    {
	      mht_rem (analyzer->tran_table, &committed_tran.tran_ids[j],
		       cirp_free_tran, NULL);
	    }
	}
      while (committed_tran.num_tran >= MAX_COMMITTED_ARRAY);
    }

  LSA_SET_NULL (&lowest_tran_start_lsa);
  error = mht_map (analyzer->tran_table,
		   cirp_find_lowest_tran_start_lsa, &lowest_tran_start_lsa);
  if (error != NO_ERROR)
    {
      return error;
    }

  if (LSA_ISNULL (&lowest_tran_start_lsa))
    {
      LSA_COPY (&analyzer->ct.required_lsa, &analyzer->current_lsa);
    }
  else
    {
      assert (LSA_LE (&analyzer->ct.required_lsa, &lowest_tran_start_lsa));
      if (LSA_LE (&analyzer->ct.required_lsa, &lowest_tran_start_lsa))
	{
	  LSA_COPY (&analyzer->ct.required_lsa, &lowest_tran_start_lsa);
	}
    }

  source_applied_time = rp_get_source_applied_time (analyzer->q_applied_time,
						    &analyzer->ct.
						    required_lsa);
  if (source_applied_time > 0)
    {
      analyzer->ct.source_applied_time = source_applied_time;


      monitor_stats_gauge (MNT_RP_ANALYZER_ID, MNT_RP_APPLIED_TIME,
			   source_applied_time);

      clock_gettime (CLOCK_REALTIME, &cur_time);
      monitor_stats_gauge (MNT_RP_ANALYZER_ID, MNT_RP_DELAY,
			   timespec_to_msec (&cur_time)
			   - source_applied_time);
    }

  monitor_stats_gauge (MNT_RP_ANALYZER_ID, MNT_RP_REQUIRED_PAGEID,
		       analyzer->ct.required_lsa.pageid);

  monitor_stats_gauge (MNT_RP_COPIER_ID, MNT_RP_REQUIRED_GAP,
		       monitor_get_stats (MNT_RP_ANALYZER_ID,
					  MNT_RP_CURRENT_PAGEID)
		       - monitor_get_stats (MNT_RP_ANALYZER_ID,
					    MNT_RP_REQUIRED_PAGEID));

  er_log_debug (ARG_FILE_LINE,
		"update progress:lowest_tran_start_lsa:%lld,%d, "
		"current_lsa:%lld,%d, "
		"required_lsa:%lld,%d ",
		(long long) lowest_tran_start_lsa.pageid,
		lowest_tran_start_lsa.offset,
		(long long) analyzer->current_lsa.pageid,
		analyzer->current_lsa.offset,
		(long long) analyzer->ct.required_lsa.pageid,
		analyzer->ct.required_lsa.offset);

  assert (error == NO_ERROR);
  return error;
}

/*
 * cirp_free_tran_by_tranid() - free tran using tranid
 *   return: none
 *
 *   analyzer(in/out):
 *   tranid: transaction id
 *
 * Note:
 */
static void
cirp_free_tran_by_tranid (CIRP_ANALYZER_INFO * analyzer, int tranid)
{
  mht_rem (analyzer->tran_table, &tranid, cirp_free_tran, NULL);

  return;
}

/*
 * cirp_change_state()-
 *    return: error code
 */
static int
cirp_change_state (CIRP_ANALYZER_INFO * analyzer, HA_STATE curr_node_state)
{
  int error = NO_ERROR;
  HA_APPLY_STATE new_state = HA_APPLY_STATE_NA;
  char buffer[ONE_K];
  CIRP_BUF_MGR *buf_mgr = NULL;
  LOG_HEADER *log_hdr = NULL;
  int server_mode;

  buf_mgr = &analyzer->buf_mgr;
  log_hdr = buf_mgr->act_log.log_hdr;

  if (analyzer->last_node_state == curr_node_state
      && analyzer->last_ha_file_status == log_hdr->ha_info.file_status
      && analyzer->last_is_end_of_record == analyzer->is_end_of_record)
    {
      /* there are no need to change */
      return NO_ERROR;
    }

  if (analyzer->last_node_state != curr_node_state)
    {
      char host_str[MAX_NODE_INFO_STR_LEN];
      prm_node_info_to_str (host_str, sizeof (host_str), &buf_mgr->host_info);
      snprintf (buffer, ONE_K,
		"change the state of HA Node (%s@%s) from '%s' to '%s'",
		buf_mgr->prefix_name, host_str,
		HA_STATE_NAME (analyzer->last_node_state),
		HA_STATE_NAME (curr_node_state));

      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
	      ER_HA_GENERIC_ERROR, 1, buffer);
    }

  analyzer->last_node_state = curr_node_state;
  analyzer->last_ha_file_status = log_hdr->ha_info.file_status;
  analyzer->last_is_end_of_record = analyzer->is_end_of_record;

  /* check log file status */
  if (analyzer->is_end_of_record == true
      && (log_hdr->ha_info.file_status == LOG_HA_FILESTAT_SYNCHRONIZED))
    {
      /* check server's state with log header */
      switch (curr_node_state)
	{
	case HA_STATE_MASTER:
	case HA_STATE_TO_BE_SLAVE:
	case HA_STATE_TO_BE_MASTER:
	  if (analyzer->apply_state != HA_APPLY_STATE_WORKING)
	    {
	      /* notify to slave db */
	      new_state = HA_APPLY_STATE_WORKING;
	    }
	  break;

	case HA_STATE_UNKNOWN:
	case HA_STATE_DEAD:
	case HA_STATE_SLAVE:
	  if (analyzer->apply_state != HA_APPLY_STATE_DONE)
	    {
	      /* wait until transactions are all committed */
	      error = cci_get_server_mode (&analyzer->conn,
					   &server_mode, NULL);
	      if (error < 0)
		{
		  REPL_SET_GENERIC_ERROR (error, "CCI ERROR(%d), %s",
					  analyzer->conn.err_buf.err_code,
					  analyzer->conn.err_buf.err_msg);

		  GOTO_EXIT_ON_ERROR;
		}

	      if (server_mode == HA_STATE_TO_BE_MASTER)
		{
		  while (cirp_anlz_is_any_applier_busy () == true)
		    {
		      if (rp_need_restart () == true)
			{
			  REPL_SET_GENERIC_ERROR (error, "need_restart");
			  GOTO_EXIT_ON_ERROR;
			}
		      THREAD_SLEEP (10);
		    }
		}

	      /* notify to slave db */
	      new_state = HA_APPLY_STATE_DONE;
	    }
	  break;
	default:
	  assert (false);
	  REPL_SET_GENERIC_ERROR (error, "BUG. Unknown HA_STATE");
	  GOTO_EXIT_ON_ERROR;
	}
    }
  else
    {
      switch (curr_node_state)
	{
	case HA_STATE_MASTER:
	case HA_STATE_TO_BE_SLAVE:
	  if (analyzer->apply_state != HA_APPLY_STATE_WORKING
	      && analyzer->apply_state != HA_APPLY_STATE_RECOVERING)
	    {
	      new_state = HA_APPLY_STATE_RECOVERING;
	    }
	  break;
	case HA_STATE_UNKNOWN:
	case HA_STATE_SLAVE:
	case HA_STATE_DEAD:
	  if (analyzer->apply_state != HA_APPLY_STATE_DONE
	      && analyzer->apply_state != HA_APPLY_STATE_RECOVERING)
	    {
	      new_state = HA_APPLY_STATE_RECOVERING;
	    }
	  break;
	case HA_STATE_TO_BE_MASTER:
	  /* no op. */
	  new_state = HA_APPLY_STATE_NA;
	  break;
	default:
	  assert (false);
	  REPL_SET_GENERIC_ERROR (error, "BUG. Unknown HA_STATE");
	  GOTO_EXIT_ON_ERROR;
	}
    }

  if (analyzer->apply_state != new_state && new_state != HA_APPLY_STATE_NA)
    {
      /* force commit when state is changing */
      error = cirp_anlz_log_commit ();
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      error =
	cci_notify_ha_agent_state (&analyzer->conn,
				   PRM_NODE_INFO_GET_IP (&analyzer->ct.
							 host_info),
				   PRM_NODE_INFO_GET_PORT (&analyzer->ct.
							   host_info),
				   new_state);
      if (error != NO_ERROR)
	{
	  error = ER_HA_LA_FAILED_TO_CHANGE_STATE;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 2,
		  HA_APPLY_STATE_NAME (analyzer->apply_state),
		  HA_APPLY_STATE_NAME (new_state));
	  GOTO_EXIT_ON_ERROR;
	}

      snprintf (buffer, sizeof (buffer),
		"change log apply state from '%s' to '%s'. "
		"last required_lsa: %lld|%lld",
		HA_APPLY_STATE_NAME (analyzer->apply_state),
		HA_APPLY_STATE_NAME (new_state),
		(long long) analyzer->ct.required_lsa.pageid,
		(long long) analyzer->ct.required_lsa.offset);
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
	      ER_HA_GENERIC_ERROR, 1, buffer);

      analyzer->apply_state = new_state;
    }

  return error;

exit_on_error:

  if (error == NO_ERROR)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "Invalid error code");
    }

  return error;
}

/*
 * cirp_final_analyzer ()
 *    return: NO_ERROR
 *
 *    analyzer(in/out):
 */
int
cirp_final_analyzer (CIRP_ANALYZER_INFO * analyzer)
{
  int error = NO_ERROR;

  if (Repl_Info == NULL)
    {
      assert (false);

      return NO_ERROR;
    }

  if (analyzer->tran_table != NULL)
    {
      mht_clear (analyzer->tran_table, cirp_free_tran, NULL);
      mht_destroy (analyzer->tran_table);
      analyzer->tran_table = NULL;
    }

  if (analyzer->log_path_lockf_vdes != NULL_VOLDES)
    {
      fileio_close (analyzer->log_path_lockf_vdes);
      analyzer->log_path_lockf_vdes = NULL_VOLDES;
    }

  if (analyzer->db_lockf_vdes != NULL_VOLDES)
    {
      error = cirp_unlock_dbname (analyzer, false);
      assert (error == NO_ERROR);
      analyzer->db_lockf_vdes = NULL_VOLDES;
    }

  cirp_logpb_final (&analyzer->buf_mgr);

  pthread_mutex_destroy (&analyzer->lock);

  return NO_ERROR;
}

/*
 * rp_applied_time_node_free -
 *   return: error code
 *
 *   node(in/out):
 *   data(in/out):
 */
static int
rp_applied_time_node_free (void *node, UNUSED_ARG void *data)
{
  RP_APPLIED_TIME_NODE *tmp;

  tmp = (RP_APPLIED_TIME_NODE *) node;

  free_and_init (tmp);

  return NO_ERROR;
}

/*
 * cirp_clear_analyzer ()
 *    return: NO_ERROR
 *
 *    analyzer(in/out):
 */
int
cirp_clear_analyzer (CIRP_ANALYZER_INFO * analyzer)
{
  if (Repl_Info == NULL)
    {
      assert (false);

      return NO_ERROR;
    }

  if (analyzer->tran_table != NULL)
    {
      mht_clear (analyzer->tran_table, cirp_free_tran, NULL);
    }

  /* master info */
  analyzer->last_is_end_of_record = false;
  analyzer->is_end_of_record = false;
  analyzer->last_node_state = HA_STATE_NA;
  analyzer->is_role_changed = false;

  if (analyzer->q_applied_time != NULL)
    {
      Rye_queue_free_full (analyzer->q_applied_time,
			   rp_applied_time_node_free);
      free_and_init (analyzer->q_applied_time);
    }

  memset (&analyzer->conn, 0, sizeof (CCI_CONN));

  LSA_SET_NULL (&analyzer->current_lsa);
  memset (&analyzer->ct, 0, sizeof (CIRP_CT_LOG_ANALYZER));

  return NO_ERROR;
}

/*
 * cirp_init_analyzer ()
 *   return: error code
 *
 *   database_name(in):
 *   log_path(in):
 */
int
cirp_init_analyzer (CIRP_ANALYZER_INFO * analyzer,
		    const char *database_name, const char *log_path)
{
  int error = NO_ERROR;

  analyzer->status = CIRP_AGENT_INIT;
  analyzer->apply_state = HA_APPLY_STATE_UNREGISTERED;

  analyzer->last_is_end_of_record = false;
  analyzer->is_end_of_record = false;
  analyzer->last_node_state = HA_STATE_NA;
  analyzer->is_role_changed = false;

  analyzer->db_lockf_vdes = NULL_VOLDES;
  analyzer->log_path_lockf_vdes = NULL_VOLDES;

  if (pthread_mutex_init (&analyzer->lock, NULL) < 0)
    {
      error = ER_CSS_PTHREAD_MUTEX_INIT;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

      GOTO_EXIT_ON_ERROR;
    }

  analyzer->tran_table = mht_create ("Analyzer Transaction Table",
				     LA_TRAN_TABLE_MHT_SIZE,
				     mht_numhash, mht_compare_ints_are_equal);
  if (analyzer->tran_table == NULL)
    {
      error = er_errid ();
      GOTO_EXIT_ON_ERROR;
    }

  error = cirp_logpb_initialize (&analyzer->buf_mgr, database_name, log_path);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = cirp_check_duplicated (&analyzer->log_path_lockf_vdes,
				 log_path, analyzer->buf_mgr.prefix_name);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  analyzer->buf_mgr.db_logpagesize = IO_MAX_PAGE_SIZE;

  memset (&analyzer->conn, 0, sizeof (CCI_CONN));

  LSA_SET_NULL (&analyzer->current_lsa);
  memset (&analyzer->ct, 0, sizeof (CIRP_CT_LOG_ANALYZER));

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  assert (error != NO_ERROR);

  cirp_final_analyzer (analyzer);

  return error;
}

/*
 * cirp_check_duplicated()-
 *    return: error code
 *
 *    lockf_vdes(out):
 *    logpath(in):
 *    dbname(in):
 */
static int
cirp_check_duplicated (int *lockf_vdes, const char *logpath,
		       const char *dbname)
{
  char lock_path[PATH_MAX];
  FILEIO_LOCKF_TYPE lockf_type = FILEIO_NOT_LOCKF;

  sprintf (lock_path, "%s%s%s%s", logpath,
	   FILEIO_PATH_SEPARATOR (logpath), dbname, LA_LOCK_SUFFIX);

  *lockf_vdes = fileio_open (lock_path, O_RDWR | O_CREAT, 0644);
  if (*lockf_vdes == NULL_VOLDES)
    {
      er_log_debug (ARG_FILE_LINE, "unable to open lock_file (%s)",
		    lock_path);
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_IO_MOUNT_FAIL, 1, lock_path);
      return ER_IO_MOUNT_FAIL;
    }

  lockf_type = fileio_lock_la_log_path (dbname, lock_path, *lockf_vdes);
  if (lockf_type == FILEIO_NOT_LOCKF)
    {
      er_log_debug (ARG_FILE_LINE, "unable to wlock lock_file (%s)",
		    lock_path);
      fileio_close (*lockf_vdes);
      *lockf_vdes = NULL_VOLDES;
      return ER_FAILED;
    }

  return NO_ERROR;
}

/*
 * cirp_anlz_is_any_applier_busy()-
 *    return:
 */
static bool
cirp_anlz_is_any_applier_busy (void)
{
  int i;

  /* skip DDL_APPLIER_INDEX */
  for (i = GLOBAL_APPLIER_INDEX; i < Repl_Info->num_applier; i++)
    {
      if (cirp_analyzer_is_applier_busy (i) == true)
	{
	  er_log_debug (ARG_FILE_LINE, "cirp_anlz_is_any_applier_busy: "
			"applier %d is busy", i);
	  return true;
	}
    }
  return false;
}

/*
 * cirp_anlz_assign_repl_item () -
 *   return: error code
 *
 *   commit_lsa(in):
 *   final_pageid(in):
 */
static int
cirp_anlz_assign_repl_item (int tranid, int rectype, LOG_LSA * commit_lsa)
{

  int error = NO_ERROR;
  CIRP_TRAN *tran;
  bool wait_tran_commit = false;
  CIRP_ANALYZER_INFO *analyzer = NULL;

  assert (rectype == LOG_COMMIT);

  analyzer = &Repl_Info->analyzer_info;

  tran = cirp_find_tran (tranid);
  if (tran == NULL)
    {
      return NO_ERROR;
    }

  if (rectype == LOG_ABORT
      || (rectype == LOG_COMMIT && LSA_ISNULL (&tran->repl_start_lsa)))
    {
      cirp_free_tran_by_tranid (analyzer, tranid);

      return NO_ERROR;
    }
  assert (tran->repl_item != NULL && tran->repl_item->next == NULL);
  assert (rp_is_valid_repl_item (tran->repl_item));

  error = cirp_lock_dbname (analyzer);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  LSA_COPY (&tran->tran_end_lsa, commit_lsa);

  wait_tran_commit = false;
  if (RP_IS_BLOCKED_ITEM (tran->repl_item))
    {
      assert (tran->applier_index == DDL_APPLIER_INDEX
	      || tran->applier_index == GLOBAL_APPLIER_INDEX);

      wait_tran_commit = true;

      /* wait until previous transactions are all committed */
      while (cirp_anlz_is_any_applier_busy () == true)
	{
	  if (rp_need_restart () == true)
	    {
	      REPL_SET_GENERIC_ERROR (error, "need_restart");
	      GOTO_EXIT_ON_ERROR;
	    }
	  THREAD_SLEEP (10);
	}
    }

  er_log_debug (ARG_FILE_LINE,
		"push an item (tranid: %d) to applier %d,tran_start(%lld,%d)"
		"wait_tran_commit:%d",
		tran->tranid, tran->applier_index,
		(long long) tran->tran_start_lsa.pageid,
		tran->tran_start_lsa.offset, wait_tran_commit);

  do
    {
      error = cirp_analyzer_item_push (tran->applier_index,
				       tran->tranid,
				       &tran->tran_start_lsa,
				       commit_lsa, &tran->repl_start_lsa);

      /* log item queue is full */
      if (error == ER_HA_JOB_QUEUE_FULL)
	{
	  monitor_stats_counter (MNT_RP_ANALYZER_ID, MNT_RP_QUEUE_FULL, 1);

	  error = cirp_analyzer_wait_for_queue (tran->applier_index);
	  if (error == NO_ERROR)
	    {
	      /* goto item push */
	      error = ER_HA_JOB_QUEUE_FULL;
	    }
	}
    }
  while (error == ER_HA_JOB_QUEUE_FULL);

  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (wait_tran_commit == true)
    {
      /* wait until current transaction is committed */
      error = cirp_analyzer_wait_tran_commit (tran->applier_index,
					      &tran->repl_start_lsa);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  er_log_debug (ARG_FILE_LINE,
		"assign an item (tranid: %d) to applier %d,tran_start(%lld,%d)",
		tran->tranid, tran->applier_index,
		(long long) tran->tran_start_lsa.pageid,
		tran->tran_start_lsa.offset);

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  assert (error != NO_ERROR);

  return error;
}

/*
 * cirp_find_tran() - return the apply list for the target
 *                             transaction id
 *   return: pointer to the target apply list
 *   tranid(in): the target transaction id
 *
 * Note:
 */
static CIRP_TRAN *
cirp_find_tran (int tranid)
{
  return mht_get (Repl_Info->analyzer_info.tran_table, &tranid);
}

/*
 * cirp_add_tran() - return the apply list for the target
 *                             transaction id
 *   return: error code
 *
 *   tran(out): pointer to the target apply list
 *   tranid(in): the target transaction id
 *
 * Note:
 *     When we apply the transaction logs to the slave, we have to take them
 *     in turns of commit order.
 *     So, each slave maintains the apply list per transaction.
 *     And an apply list has one or more replication item.
 *     When the APPLY thread meets the "LOG COMMIT" record, it finds out
 *     the apply list of the target transaction, and apply the replication
 *     items to the slave orderly.
 */
static int
cirp_add_tran (CIRP_TRAN ** tran, int tranid)
{
  CIRP_TRAN *new_tran = NULL;
  int error = NO_ERROR;

  if (tran == NULL)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "Invalid argument");

      return error;
    }
  *tran = NULL;

  new_tran = cirp_find_tran (tranid);
  if (new_tran != NULL)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "Exists transaction info");
      return error;
    }

  new_tran = (CIRP_TRAN *) RYE_MALLOC (sizeof (CIRP_TRAN));
  if (new_tran == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, sizeof (CIRP_TRAN));

      GOTO_EXIT_ON_ERROR;
    }

  cirp_init_tran (new_tran);
  new_tran->tranid = tranid;

  if (mht_put (Repl_Info->analyzer_info.tran_table,
	       &new_tran->tranid, new_tran) == NULL)
    {
      error = er_errid ();

      GOTO_EXIT_ON_ERROR;
    }

  *tran = new_tran;

  assert (error == NO_ERROR);
  return NO_ERROR;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "Invalid error code");
    }

  if (new_tran != NULL)
    {
      RYE_FREE_MEM (new_tran);
    }

  return error;
}

/*
 * cirp_anlz_get_applier_index() - hash shard key to get
 *                                 applier index
 *   return: NO_ERROR or error code
 *
 *   index(out):
 *   shard_key(in):
 *   num_appliers(in):
 */
static int
cirp_anlz_get_applier_index (int *index, const DB_VALUE * shard_key,
			     int num_appliers)
{
  if (db_value_type (shard_key) != DB_TYPE_VARCHAR)
    {
      int error = NO_ERROR;

      assert (false);
      REPL_SET_GENERIC_ERROR (error, "Invalid shard_key type");

      return error;
    }

  *index = (mht_get_hash_number (num_appliers - 1, shard_key) + 1);

  return NO_ERROR;
}

/*
 * rp_set_schema_log -
 *   return: error code
 *
 *   buf_mgr(in/out):
 *   tran(in/out):
 *   log_pgptr(in):
 *   lsa(in):
 */
static int
rp_set_schema_log (CIRP_BUF_MGR * buf_mgr, CIRP_TRAN * tran,
		   LOG_PAGE * log_pgptr, const LOG_LSA * lsa)
{
  CIRP_REPL_ITEM *item = NULL;
  int error = NO_ERROR;

  error = rp_new_repl_item_ddl (&item, lsa);
  if (error != NO_ERROR)
    {
      return error;
    }

  error = rp_make_repl_schema_item_from_log (buf_mgr, item, log_pgptr, lsa);
  if (error != NO_ERROR)
    {
      cirp_free_repl_item (item);
      return error;
    }

  assert (tran->repl_item == NULL);
  tran->repl_item = item;
  tran->applier_index = DDL_APPLIER_INDEX;

  er_log_debug (ARG_FILE_LINE, "make schema: lsa(%ld,%ld), query(%s)",
		lsa->pageid, lsa->offset, item->info.ddl.query);

  assert (error == NO_ERROR);
  return NO_ERROR;
}

/*
 * rp_set_data_log -
 *   return: error code
 *
 *   buf_mgr(in/out):
 *   tran(in/out):
 *   log_pgptr(in):
 *   lsa(in):
 */
static int
rp_set_data_log (CIRP_BUF_MGR * buf_mgr, CIRP_TRAN * tran,
		 LOG_PAGE * log_pgptr, const LOG_LSA * lsa)
{
  CIRP_REPL_ITEM *item = NULL;
  RP_DATA_ITEM *data_item = NULL;
  int error = NO_ERROR;

  error = rp_new_repl_item_data (&item, lsa);
  if (error != NO_ERROR)
    {
      return error;
    }
  error = rp_make_repl_data_item_from_log (buf_mgr, item, log_pgptr, lsa);
  if (error != NO_ERROR)
    {
      cirp_free_repl_item (item);
      return error;
    }

  assert (tran->repl_item == NULL);
  tran->repl_item = item;


  data_item = &item->info.data;

  assert (!DB_IDXKEY_IS_NULL (&data_item->key));

  if (data_item->groupid == GLOBAL_GROUPID)
    {
      if (strcasecmp (data_item->class_name,
		      CT_SHARD_GID_SKEY_INFO_NAME) == 0)
	{
	  /* PK -> (group_id, shard_key) */
	  data_item->groupid = DB_GET_INTEGER (&(data_item->key.vals[0]));
	  error = cirp_anlz_get_applier_index (&tran->applier_index,
					       &(data_item->key.vals[1]),
					       Repl_Info->num_applier);
	  if (error != NO_ERROR)
	    {
	      return error;
	    }

	  assert (DDL_APPLIER_INDEX < tran->applier_index
		  && tran->applier_index < Repl_Info->num_applier);
	}
      else
	{
	  tran->applier_index = GLOBAL_APPLIER_INDEX;
	}
    }
  else
    {
      /* PK -> (shard_key, ... ) */
      error = cirp_anlz_get_applier_index (&tran->applier_index,
					   &(data_item->key.vals[0]),
					   Repl_Info->num_applier);
      if (error != NO_ERROR)
	{
	  return error;
	}
      assert (DDL_APPLIER_INDEX < tran->applier_index
	      && tran->applier_index < Repl_Info->num_applier);
    }

  assert (error == NO_ERROR);
  return NO_ERROR;
}

/*
 * rp_set_gid_bitmap_log -
 *   return: error code
 *
 *   tran(in/out):
 *   lsa(in):
 */
static int
rp_set_gid_bitmap_log (CIRP_TRAN * tran, const LOG_LSA * lsa)
{
  CIRP_REPL_ITEM *item = NULL;
  RP_DDL_ITEM *ddl_item = NULL;
  int error = NO_ERROR;

  error = rp_new_repl_item_ddl (&item, lsa);
  if (error != NO_ERROR)
    {
      return error;
    }

  assert (tran->repl_item == NULL);
  tran->repl_item = item;
  tran->applier_index = DDL_APPLIER_INDEX;

  ddl_item = &item->info.ddl;
  ddl_item->ddl_type = REPL_BLOCKED_DDL;

  er_log_debug (ARG_FILE_LINE, "gid_bitmap_update: cirp_set_repl_log");

  assert (error == NO_ERROR);
  return NO_ERROR;
}

/*
 * rp_set_repl_log() - insert the replication item into the apply list
 *   return: NO_ERROR or error code
 *   log_pgptr : pointer to the log page
 *   tranid: the target transaction id
 *   lsa  : the target LSA of the log
 *
 * Note:
 *     APPLY thread traverses the transaction log pages, and finds out the
 *     REPLICATION LOG record. If it meets the REPLICATION LOG record,
 *     it adds that record to the apply list for later use.
 *     When the APPLY thread meets the LOG COMMIT record, it applies the
 *     inserted REPLICAION LOG records to the slave.
 */
static int
rp_set_repl_log (LOG_PAGE * log_pgptr,
		 int log_type, int tranid, const LOG_LSA * lsa)
{
  CIRP_TRAN *tran = NULL;
  CIRP_BUF_MGR *buf_mgr = NULL;
  int error = NO_ERROR;

  buf_mgr = &Repl_Info->analyzer_info.buf_mgr;

  tran = cirp_find_tran (tranid);
  if (tran == NULL)
    {
      er_log_debug (ARG_FILE_LINE,
		    "fail to find out %d transaction in apply list", tranid);
      return NO_ERROR;
    }

  if (!LSA_ISNULL (&tran->repl_start_lsa))
    {
      assert (tran->repl_item != NULL && tran->repl_item->next == NULL);

      return NO_ERROR;
    }

  assert (tran->repl_item == NULL);

  switch (log_type)
    {
    case LOG_REPLICATION_SCHEMA:
      error = rp_set_schema_log (buf_mgr, tran, log_pgptr, lsa);
      if (error != NO_ERROR)
	{
	  return error;
	}
      break;
    case LOG_REPLICATION_DATA:
      error = rp_set_data_log (buf_mgr, tran, log_pgptr, lsa);
      if (error != NO_ERROR)
	{
	  return error;
	}
      break;
    case LOG_DUMMY_UPDATE_GID_BITMAP:
      error = rp_set_gid_bitmap_log (tran, lsa);
      if (error != NO_ERROR)
	{
	  return error;
	}
      break;

    default:
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "Invalid log type!");
      return error;
    }

  assert (LSA_ISNULL (&tran->repl_start_lsa));
  LSA_COPY (&tran->repl_start_lsa, lsa);

  er_log_debug (ARG_FILE_LINE, "set repl log: index:%d,"
		"tran_start_lsa:%lld,%d, repl_start_lsa:%lld,%d",
		tran->applier_index,
		(long long) tran->tran_start_lsa.pageid,
		tran->tran_start_lsa.offset,
		(long long) tran->repl_start_lsa.pageid,
		tran->repl_start_lsa.offset);

  assert (error == NO_ERROR);
  return error;
}

/*
 * cirp_lock_dbname () -
 *    return: error code
 *
 *    analyzer(in/out):
 */
static int
cirp_lock_dbname (CIRP_ANALYZER_INFO * analyzer)
{
  FILEIO_LOCKF_TYPE result;

  if (analyzer->db_lockf_vdes != NULL_VOLDES)
    {
      return NO_ERROR;
    }

  result = FILEIO_NOT_LOCKF;
  while (rp_need_restart () == false)
    {
      result = fileio_lock_la_dbname (&analyzer->db_lockf_vdes,
				      analyzer->buf_mgr.prefix_name,
				      analyzer->buf_mgr.log_path);
      if (result == FILEIO_LOCKF)
	{
	  break;
	}
      THREAD_SLEEP (1000);
    }

  assert (rp_need_restart () == true
	  || analyzer->db_lockf_vdes != NULL_VOLDES);

  if (result != FILEIO_LOCKF)
    {
      int error = NO_ERROR;

      REPL_SET_GENERIC_ERROR (error, "need_restart");

      return error;
    }

  analyzer->is_role_changed = false;

  return NO_ERROR;
}

/*
 * cirp_unlock_dbname () -
 *    return: error code
 *
 *    analyzer(in/out):
 *    clear_owner(in):
 */
static int
cirp_unlock_dbname (CIRP_ANALYZER_INFO * analyzer, bool clear_owner)
{
  int error = NO_ERROR;
  int result;

  if (analyzer->db_lockf_vdes == NULL_VOLDES)
    {
      return NO_ERROR;
    }

  result = fileio_unlock_la_dbname (&analyzer->db_lockf_vdes,
				    analyzer->buf_mgr.prefix_name,
				    clear_owner);
  if (result == FILEIO_LOCKF)
    {
      error = er_errid ();

      assert (error != NO_ERROR);
      return error == NO_ERROR ? ER_FAILED : error;
    }

  analyzer->is_role_changed = false;

  if (clear_owner)
    {
      er_log_debug (ARG_FILE_LINE, "unlock_dbname(sleep 60secs)");

      THREAD_SLEEP (3 * 1000);
    }

  return error;
}

/*
 * cirp_anlz_log_commit() -
 *   return: NO_ERROR or error code
 */
static int
cirp_anlz_log_commit (void)
{
  int error = NO_ERROR;
  CIRP_CT_LOG_ANALYZER tmp_analyzer_data;

  CIRP_ANALYZER_INFO *analyzer;

  analyzer = &Repl_Info->analyzer_info;

  error = cirp_anlz_update_progress_from_appliers (analyzer);
  if (error != NO_ERROR)
    {
      return error;
    }

  error = pthread_mutex_lock (&analyzer->lock);
  if (error != NO_ERROR)
    {
      error = ER_CSS_PTHREAD_MUTEX_LOCK;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

      return error;
    }

  memcpy (&tmp_analyzer_data, &analyzer->ct, sizeof (CIRP_CT_LOG_ANALYZER));

  pthread_mutex_unlock (&analyzer->lock);

  error = rpct_update_log_analyzer (&analyzer->conn, &tmp_analyzer_data);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  return error;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "Invalid error code");
    }

  return error;
}

/*
 * cirp_delay_replica () -
 *    return: error code
 *
 *    eot_time(in):
 */
static int
cirp_delay_replica (time_t eot_time)
{
  int error = NO_ERROR;
  static int ha_mode = -1;
  static int replica_delay = -1;
  static time_t replica_time_bound = -1;
  static char *replica_time_bound_str = (void *) -1;
  char buffer[LINE_MAX];

  /* check iff the first time */
  if (ha_mode < 0)
    {
      ha_mode = prm_get_integer_value (PRM_ID_HA_MODE);
      assert (ha_mode != HA_MODE_OFF);
    }

  if (replica_delay < 0)
    {
      replica_delay =
	prm_get_bigint_value (PRM_ID_HA_REPLICA_DELAY) / ONE_SEC;
    }

  if (replica_time_bound_str == (void *) -1)
    {
      replica_time_bound_str =
	prm_get_string_value (PRM_ID_HA_REPLICA_TIME_BOUND);
    }

  if (ha_mode == HA_MODE_REPLICA)
    {
      assert (false);

      if (replica_time_bound_str != NULL)
	{
	  if (replica_time_bound == -1)
	    {
	      replica_time_bound =
		util_str_to_time_since_epoch (replica_time_bound_str);
	      assert (replica_time_bound != 0);
	    }

	  if (eot_time >= replica_time_bound)
	    {
	      error = cirp_anlz_log_commit ();
	      if (error != NO_ERROR)
		{
		  return error;
		}

	      snprintf (buffer, sizeof (buffer),
			"applylogdb paused since it reached "
			"a log record committed on master at %s or later.\n"
			"Adjust or remove %s and restart applylogdb to resume",
			replica_time_bound_str,
			prm_get_name (PRM_ID_HA_REPLICA_TIME_BOUND));
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_HA_GENERIC_ERROR, 1, buffer);

	      /* applylogdb waits indefinitely */
	      select (0, NULL, NULL, NULL, NULL);
	    }
	}
      else if (replica_delay > 0)
	{
	  while ((time (NULL) - eot_time) < replica_delay)
	    {
	      THREAD_SLEEP (100);
	    }
	}
    }

  return NO_ERROR;
}

/*
 * cirp_analyze_log_record ()
 *    return:error code
 *
 *    lrec(in):
 *    final(in):
 *    pgptr(in):
 */
static int
cirp_analyze_log_record (LOG_RECORD_HEADER * lrec,
			 LOG_LSA final, LOG_PAGE * pg_ptr)
{
  CIRP_TRAN *tran = NULL;
  int error = NO_ERROR;
  char buffer[256];
  time_t eot_time;
  CIRP_BUF_MGR *buf_mgr = NULL;
  CIRP_ANALYZER_INFO *analyzer;

  if (lrec == NULL || pg_ptr == NULL
      || final.pageid != pg_ptr->hdr.logical_pageid)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "Invalid arguments");
      GOTO_EXIT_ON_ERROR;
    }

  analyzer = &Repl_Info->analyzer_info;
  buf_mgr = &analyzer->buf_mgr;

  if (lrec->trid == NULL_TRANID
      || LSA_GT (&lrec->prev_tranlsa, &final)
      || LSA_GT (&lrec->back_lsa, &final))
    {
      assert (false);
      if (lrec->type != LOG_END_OF_LOG)
	{
	  error = ER_HA_LA_INVALID_REPL_LOG_RECORD;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  error, 10,
		  final.pageid, final.offset,
		  lrec->forw_lsa.pageid, lrec->forw_lsa.offset,
		  lrec->back_lsa.pageid, lrec->back_lsa.offset,
		  lrec->trid,
		  lrec->prev_tranlsa.pageid, lrec->prev_tranlsa.offset,
		  lrec->type);

	  GOTO_EXIT_ON_ERROR;
	}
    }

  if ((lrec->type != LOG_END_OF_LOG
       && lrec->type != LOG_DUMMY_HA_SERVER_STATE)
      && (lrec->trid != LOG_SYSTEM_TRANID)
      && (LSA_ISNULL (&lrec->prev_tranlsa)))
    {
      error = cirp_add_tran (&tran, lrec->trid);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      assert (LSA_ISNULL (&tran->repl_start_lsa));
      assert (LSA_ISNULL (&tran->tran_start_lsa));
      assert (LSA_ISNULL (&tran->tran_end_lsa));

      LSA_COPY (&tran->tran_start_lsa, &final);
    }

  analyzer->is_end_of_record = false;
  switch (lrec->type)
    {
    case LOG_END_OF_LOG:
      assert (false);

      analyzer->is_end_of_record = true;

      error = ER_INTERRUPTED;

      GOTO_EXIT_ON_ERROR;
      break;

    case LOG_REPLICATION_DATA:
    case LOG_REPLICATION_SCHEMA:
    case LOG_DUMMY_UPDATE_GID_BITMAP:
      /* add the replication log to the target transaction */
      error = rp_set_repl_log (pg_ptr, lrec->type, lrec->trid, &final);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      break;

    case LOG_COMMIT:
      /* apply the replication log to the slave */
      error = cirp_log_get_eot_time (buf_mgr, &eot_time, pg_ptr, final);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      assert (eot_time > 0);

      /* in case of delayed/time-bound replication */
      error = cirp_delay_replica (eot_time);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      error = cirp_anlz_assign_repl_item (lrec->trid, LOG_COMMIT, &final);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      break;

    case LOG_ABORT:
      cirp_free_tran_by_tranid (analyzer, lrec->trid);

      break;

    case LOG_DUMMY_CRASH_RECOVERY:
      snprintf (buffer, sizeof (buffer),
		"process log record (type:%d). "
		"skip this log record. LSA: %lld|%d",
		lrec->type, (long long int) final.pageid, final.offset);
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
	      ER_HA_GENERIC_ERROR, 1, buffer);
      break;

    case LOG_END_CHKPT:
      break;

    case LOG_DUMMY_HA_SERVER_STATE:
      {
	struct log_ha_server_state state;
	RP_APPLIED_TIME_NODE *node;

	error = cirp_log_get_ha_server_state (&state, pg_ptr, final);
	if (error != NO_ERROR)
	  {
	    GOTO_EXIT_ON_ERROR;
	  }
	node =
	  (RP_APPLIED_TIME_NODE *) malloc (sizeof (RP_APPLIED_TIME_NODE));
	if (node == NULL)
	  {
	    error = ER_OUT_OF_VIRTUAL_MEMORY;
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
		    1, sizeof (RP_APPLIED_TIME_NODE));
	    GOTO_EXIT_ON_ERROR;
	  }
	node->applied_time = state.at_time;
	LSA_COPY (&node->applied_lsa, &final);

	Rye_queue_enqueue (analyzer->q_applied_time, node);

	if (state.server_state != HA_STATE_SLAVE
	    && state.server_state != HA_STATE_MASTER
	    && state.server_state != HA_STATE_TO_BE_SLAVE)
	  {
	    if (Repl_Info->analyzer_info.db_lockf_vdes != NULL_VOLDES)
	      {
		char host_str[MAX_NODE_INFO_STR_LEN];
		prm_node_info_to_str (host_str, sizeof (host_str),
				      &buf_mgr->host_info);
		snprintf (buffer, sizeof (buffer),
			  "the state of HA server (%s@%s) is changed to %s",
			  buf_mgr->prefix_name, host_str,
			  HA_STATE_NAME (state.server_state));

		er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
			ER_NOTIFY_MESSAGE, 1, buffer);

		analyzer->is_role_changed = true;
	      }
	  }
      }
      break;

    default:
      break;
    }				/* switch(lrec->type) */

  /*
   * if this is the final record of the archive log..
   * we have to fetch the next page. So, increase the pageid,
   * but we don't know the exact offset of the next record.
   * the offset would be adjusted after getting the next log page
   */
  if (lrec->forw_lsa.pageid == -1
      || lrec->type <= LOG_SMALLER_LOGREC_TYPE
      || lrec->type >= LOG_LARGER_LOGREC_TYPE)
    {
      error = ER_HA_LA_INVALID_REPL_LOG_RECORD;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      error, 10,
	      final.pageid, final.offset,
	      lrec->forw_lsa.pageid, lrec->forw_lsa.offset,
	      lrec->back_lsa.pageid, lrec->back_lsa.offset,
	      lrec->trid,
	      lrec->prev_tranlsa.pageid, lrec->prev_tranlsa.offset,
	      lrec->type);

      GOTO_EXIT_ON_ERROR;
    }

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "Invalid error code");
      return error;
    }

  return error;
}

/*
 * analyzer_main() - analyze the transaction log
 *      and dispatch replication items to appliers
 *   return: int
 *
 * Note:
 */
void *
analyzer_main (void *arg)
{
  int error = NO_ERROR;
  ER_MSG_INFO *th_er_msg_info;
  struct log_header *log_hdr;
  CIRP_LOGPB *log_buf = NULL;
  LOG_PAGE *pg_ptr;
  LOG_RECORD_HEADER *lrec = NULL;
  LOG_LSA final_lsa;
  struct timeval commit_time, current_time;
  int eof_busy_wait_count = 0;
  int EOF_MAX_BUSY_WAIT = 10;
  INT64 diff_msec;
  CIRP_ANALYZER_INFO *analyzer = NULL;
  CIRP_BUF_MGR *buf_mgr = NULL;
  CIRP_THREAD_ENTRY *th_entry = NULL;
  char err_msg[ER_MSG_SIZE];
  int retry_count = 0;
  HA_STATE curr_node_state = HA_STATE_UNKNOWN;

  th_entry = (CIRP_THREAD_ENTRY *) arg;

  analyzer = &Repl_Info->analyzer_info;
  buf_mgr = &analyzer->buf_mgr;

  th_er_msg_info = malloc (sizeof (ER_MSG_INFO));
  error = er_set_msg_info (th_er_msg_info);
  if (error != NO_ERROR)
    {
      RP_SET_AGENT_NEED_SHUTDOWN ();

      cirp_change_analyzer_status (analyzer, CIRP_AGENT_DEAD);

      free_and_init (th_er_msg_info);

      return NULL;
    }

  /* wait until thread_create finish */
  error = pthread_mutex_lock (&th_entry->th_lock);
  if (error != NO_ERROR)
    {
      error = ER_CSS_PTHREAD_MUTEX_LOCK;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

      RP_SET_AGENT_NEED_SHUTDOWN ();

      cirp_change_analyzer_status (analyzer, CIRP_AGENT_DEAD);

      free_and_init (th_er_msg_info);

      return NULL;
    }
  pthread_mutex_unlock (&th_entry->th_lock);

  while (REPL_NEED_SHUTDOWN () == false)
    {
      if (rp_check_appliers_status (CIRP_AGENT_INIT) == false)
	{
	  THREAD_SLEEP (100);
	  continue;
	}
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_NOTIFY_MESSAGE, 1,
	      "All Agent Stopped");

      rp_disconnect_agents ();

      rp_clear_need_restart ();

      error = cirp_connect_agents (th_entry->arg->db_name);
      if (error != NO_ERROR)
	{
	  THREAD_SLEEP (1000);
	  continue;
	}

      /* find out the last log applied LSA */
      error = cirp_get_repl_info_from_catalog (analyzer);
      if (error != NO_ERROR)
	{
	  THREAD_SLEEP (1000);
	  continue;
	}

      /* initialize */
      LSA_COPY (&final_lsa, &analyzer->ct.required_lsa);
      LSA_COPY (&analyzer->current_lsa, &final_lsa);

      monitor_stats_gauge (MNT_RP_ANALYZER_ID, MNT_RP_CURRENT_PAGEID,
			   analyzer->current_lsa.pageid);

      monitor_stats_gauge (MNT_RP_COPIER_ID, MNT_RP_CURRENT_GAP,
			   monitor_get_stats (MNT_RP_FLUSHER_ID,
					      MNT_RP_FLUSHED_PAGEID)
			   - monitor_get_stats (MNT_RP_ANALYZER_ID,
						MNT_RP_CURRENT_PAGEID));

      monitor_stats_gauge (MNT_RP_ANALYZER_ID, MNT_RP_REQUIRED_PAGEID,
			   analyzer->ct.required_lsa.pageid);

      monitor_stats_gauge (MNT_RP_COPIER_ID, MNT_RP_REQUIRED_GAP,
			   monitor_get_stats (MNT_RP_ANALYZER_ID,
					      MNT_RP_CURRENT_PAGEID)
			   - monitor_get_stats (MNT_RP_ANALYZER_ID,
						MNT_RP_REQUIRED_PAGEID));

      snprintf (err_msg, sizeof (err_msg),
		"All Agent Start. required_lsa: %lld|%d."
		"current LSA: %lld|%d.",
		(long long) analyzer->ct.required_lsa.pageid,
		analyzer->ct.required_lsa.offset,
		(long long) analyzer->current_lsa.pageid,
		analyzer->current_lsa.offset);
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_NOTIFY_MESSAGE, 1,
	      err_msg);

      error = rp_start_all_applier ();
      if (error != NO_ERROR)
	{
	  THREAD_SLEEP (1000);
	  continue;
	}

      /* decache all */
      cirp_logpb_decache_range (buf_mgr, 0, LOGPAGEID_MAX);

      gettimeofday (&commit_time, NULL);

      retry_count = 0;
      /* start loop analyzation */
      while (!LSA_ISNULL (&final_lsa) && rp_need_restart () == false)
	{
	  /* check and change state */
	  error = rye_master_shm_get_node_state (&curr_node_state,
						 &analyzer->ct.host_info);
	  if (error != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	  if (curr_node_state == HA_STATE_NA)
	    {
	      THREAD_SLEEP (100);
	      continue;
	    }

	  error = cirp_change_state (analyzer, curr_node_state);
	  if (error != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

	  /* release all page buffers */
	  cirp_logpb_release_all (buf_mgr, NULL_PAGEID);

	  /* don't move cirp_logpb_act_log_fetch_hdr ()
	   * and another function don't call cirp_logpb_act_log_fetch_hdr() */
	  error = cirp_logpb_act_log_fetch_hdr (buf_mgr);
	  if (error != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	  log_hdr = buf_mgr->act_log.log_hdr;

	  if (log_hdr->ha_info.last_flushed_pageid <= 0)
	    {
	      er_log_debug (ARG_FILE_LINE,
			    "Wait: received log page from rye_server."
			    "(last_flushed_pageid:%lld, final:%lld)",
			    (long long) log_hdr->ha_info.
			    last_flushed_pageid,
			    (long long) final_lsa.pageid);
	      THREAD_SLEEP (50);
	      continue;
	    }

	  /* check log hdr's master state */
	  if (analyzer->apply_state == HA_APPLY_STATE_DONE
	      && (curr_node_state != HA_STATE_MASTER)
	      && (curr_node_state != HA_STATE_TO_BE_SLAVE))
	    {
	      /* if there's no replication log to be applied,
	       * we should release dbname lock */
	      error = cirp_unlock_dbname (analyzer, true);
	      assert (error == NO_ERROR);

	      if (curr_node_state != HA_STATE_UNKNOWN)
		{
		  er_log_debug (ARG_FILE_LINE,
				"lowest required page id is %lld",
				(long long int) analyzer->ct.
				required_lsa.pageid);

		  error = cirp_anlz_log_commit ();
		  if (error != NO_ERROR)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }
		}

	      analyzer->apply_state = HA_APPLY_STATE_RECOVERING;

	      THREAD_SLEEP (50);
	      continue;
	    }

	  /* check for end of log */
	  if (LSA_GE (&final_lsa, &log_hdr->eof_lsa))
	    {
	      analyzer->is_end_of_record = true;
	      if (++eof_busy_wait_count > EOF_MAX_BUSY_WAIT)
		{
		  eof_busy_wait_count = 0;
		  THREAD_SLEEP (50);
		}
	      continue;
	    }

	  /* get the target page from log */
	  error = cirp_logpb_get_page_buffer (buf_mgr, &log_buf,
					      final_lsa.pageid);
	  if (error != NO_ERROR || log_buf == NULL)
	    {
	      assert (error != NO_ERROR && log_buf == NULL);
	      if (error == NO_ERROR)
		{
		  assert (false);

		  error = ER_GENERIC_ERROR;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
			  1, "Invalid return value");
		}

	      /* request page is greater then last_flushed_pageid.(in log_header) */
	      if (error == ER_HA_LOG_PAGE_DOESNOT_EXIST)
		{
		  er_log_debug (ARG_FILE_LINE,
				"requested pageid (%lld) is greater than "
				"last received pageid (%lld) in log header",
				(long long) final_lsa.pageid,
				(long long) log_hdr->ha_info.
				last_flushed_pageid);
		  /*
		   * does not received log page from rye_server
		   * or active log was archived.
		   */
		  er_log_debug (ARG_FILE_LINE,
				"but retry again...(pageid:%lld)",
				(long long int) final_lsa.pageid);
		  THREAD_SLEEP (50 + (retry_count * 50));

		  if (retry_count++ < LA_GET_PAGE_RETRY_COUNT)
		    {
		      error = NO_ERROR;
		      er_clear ();
		      continue;
		    }
		}

	      GOTO_EXIT_ON_ERROR;
	    }
	  retry_count = 0;

	  LSA_COPY (&analyzer->current_lsa, &final_lsa);

	  monitor_stats_gauge (MNT_RP_ANALYZER_ID, MNT_RP_CURRENT_PAGEID,
			       final_lsa.pageid);

	  monitor_stats_gauge (MNT_RP_COPIER_ID, MNT_RP_CURRENT_GAP,
			       monitor_get_stats (MNT_RP_FLUSHER_ID,
						  MNT_RP_FLUSHED_PAGEID)
			       - monitor_get_stats (MNT_RP_ANALYZER_ID,
						    MNT_RP_CURRENT_PAGEID));

	  /* a loop for each page */
	  pg_ptr = &(log_buf->log_page);
	  while (final_lsa.pageid == log_buf->pageid
		 && analyzer->is_role_changed == false
		 && rp_need_restart () == false)
	    {
	      /* adjust the offset when the offset is 0.
	       * If we read final log record from the archive,
	       * we don't know the exact offset of the next record,
	       * In this case, we set the offset as 0, increase the pageid.
	       * So, before getting the log record, check the offset and
	       * adjust it
	       */
	      if (final_lsa.offset == 0 || final_lsa.offset == NULL_OFFSET)
		{
		  assert (final_lsa.offset == 0);
		  assert (log_buf->log_page.hdr.offset == 0);

		  final_lsa.offset = log_buf->log_page.hdr.offset;
		}
	      assert (final_lsa.pageid
		      <= log_hdr->ha_info.last_flushed_pageid);

	      lrec = LOG_GET_LOG_RECORD_HEADER (pg_ptr, &final_lsa);

	      /* check for end of log */
	      if (LSA_GE (&final_lsa, &log_hdr->eof_lsa)
		  || lrec->type == LOG_END_OF_LOG)
		{
		  analyzer->is_end_of_record = true;
		  break;
		}

	      if (!CIRP_IS_VALID_LSA (buf_mgr, &final_lsa)
		  || !CIRP_IS_VALID_LOG_RECORD (buf_mgr, lrec))
		{
		  cirp_logpb_release (buf_mgr, log_buf->pageid);
		  log_buf = NULL;

		  /* may be log archived */
		  error = ER_LOG_PAGE_CORRUPTED;
		  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
			  error, 1, final_lsa.pageid);

		  GOTO_EXIT_ON_ERROR;
		}

	      /* process the log record */
	      error = cirp_analyze_log_record (lrec, final_lsa, pg_ptr);
	      if (error != NO_ERROR)
		{
		  if (error == ER_HA_LOG_PAGE_DOESNOT_EXIST)
		    {
		      /*
		       * does not received log page from rye_server
		       * or active log was archived.
		       */
		      break;
		    }

		  assert (error != ER_LOG_PAGE_CORRUPTED);

		  cirp_logpb_release (buf_mgr, log_buf->pageid);
		  log_buf = NULL;

		  GOTO_EXIT_ON_ERROR;
		}

	      /* set the next record */
	      LSA_COPY (&final_lsa, &lrec->forw_lsa);
	    }			/* a loop for each page */
	  assert (error == NO_ERROR || error == ER_HA_LOG_PAGE_DOESNOT_EXIST);
	  error = NO_ERROR;
	  er_clear ();

	  cirp_logpb_release (buf_mgr, log_buf->pageid);
	  log_buf = NULL;

	  /* commit */
	  gettimeofday (&current_time, NULL);
	  diff_msec = timeval_diff_in_msec (&current_time, &commit_time);
	  if (diff_msec > 1000 || analyzer->is_role_changed == true)
	    {
	      error = cirp_anlz_log_commit ();
	      if (error != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	      if (analyzer->is_role_changed == true)
		{
		  error = cirp_unlock_dbname (analyzer, true);
		  if (error != NO_ERROR)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }
		}

	      error = cirp_logpb_remove_archive_log (buf_mgr,
						     analyzer->ct.
						     required_lsa.pageid);
	      if (error != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}

	      commit_time = current_time;
	    }

	}			/* end loop analyzation   */

      /* Fall through */
      assert (error == NO_ERROR);

    exit_on_error:

      snprintf (err_msg, sizeof (err_msg),
		"Analyzer Retry(ERROR:%d). required_lsa: %lld|%d."
		"current LSA: %lld|%d.",
		error,
		(long long) analyzer->ct.required_lsa.pageid,
		analyzer->ct.required_lsa.offset,
		(long long) analyzer->current_lsa.pageid,
		analyzer->current_lsa.offset);
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_NOTIFY_MESSAGE, 1,
	      err_msg);

      RP_SET_AGENT_NEED_RESTART ();

      /* restart analyzer */
      cirp_change_analyzer_status (analyzer, CIRP_AGENT_INIT);
    }

  RP_SET_AGENT_NEED_SHUTDOWN ();

  cirp_change_analyzer_status (analyzer, CIRP_AGENT_DEAD);

  snprintf (err_msg, sizeof (err_msg),
	    "Analyzer Exit. required_lsa: %lld|%d."
	    "current LSA: %lld|%d.",
	    (long long) analyzer->ct.required_lsa.pageid,
	    analyzer->ct.required_lsa.offset,
	    (long long) analyzer->current_lsa.pageid,
	    analyzer->current_lsa.offset);

  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_NOTIFY_MESSAGE,
	  1, err_msg);

  free_and_init (th_er_msg_info);

  return NULL;
}
