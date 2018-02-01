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
 * log_recovery.c -
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <assert.h>

#include "porting.h"
#include "log_manager.h"
#include "log_impl.h"
#include "log_comm.h"
#include "recovery.h"
#include "boot_sr.h"
#include "disk_manager.h"
#include "page_buffer.h"
#include "file_io.h"
#include "storage_common.h"
#include "error_manager.h"
#include "memory_alloc.h"
#include "system_parameter.h"
#include "message_catalog.h"
#if defined(SERVER_MODE)
#include "connection_error.h"
#include "thread.h"
#endif /* SERVER_MODE */
#include "log_compress.h"


static void
log_rv_undo_record (THREAD_ENTRY * thread_p, LOG_LSA * log_lsa,
		    LOG_PAGE * log_page_p, LOG_RCVINDEX rcvindex,
		    const VPID * rcv_vpid, LOG_RCV * rcv,
		    const LOG_LSA * rcv_lsa_ptr, LOG_TDES * tdes,
		    LOG_ZIP * undo_unzip_ptr);
static void
log_rv_redo_record (THREAD_ENTRY * thread_p, LOG_LSA * log_lsa,
		    LOG_PAGE * log_page_p,
		    int (*redofun) (THREAD_ENTRY * thread_p, LOG_RCV *),
		    LOG_RCV * rcv, LOG_LSA * rcv_lsa_ptr,
		    int undo_length, char *undo_data,
		    LOG_ZIP * redo_unzip_ptr);
static bool log_rv_find_checkpoint (THREAD_ENTRY * thread_p, VOLID volid,
				    LOG_LSA * rcv_lsa);
static bool log_rv_get_unzip_log_data (THREAD_ENTRY * thread_p, int length,
				       LOG_LSA * log_lsa,
				       LOG_PAGE * log_page_p,
				       LOG_ZIP * undo_unzip_ptr);
static int log_rv_analysis_undo_redo (THREAD_ENTRY * thread_p, int tran_id,
				      LOG_LSA * log_lsa);
static int log_rv_analysis_dummy_head_postpone (THREAD_ENTRY * thread_p,
						int tran_id,
						LOG_LSA * log_lsa);
static int log_rv_analysis_postpone (THREAD_ENTRY * thread_p, int tran_id,
				     LOG_LSA * log_lsa);
static int log_rv_analysis_run_postpone (THREAD_ENTRY * thread_p, int tran_id,
					 LOG_LSA * log_lsa,
					 LOG_PAGE * log_page_p,
					 LOG_LSA * check_point);
static int log_rv_analysis_compensate (THREAD_ENTRY * thread_p, int tran_id,
				       LOG_LSA * log_lsa,
				       LOG_PAGE * log_page_p);
static int log_rv_analysis_lcompensate (THREAD_ENTRY * thread_p, int tran_id,
					LOG_LSA * log_lsa,
					LOG_PAGE * log_page_p);
#if defined(ENABLE_UNUSED_FUNCTION)
static int log_rv_analysis_will_commit (THREAD_ENTRY * thread_p, int tran_id,
					LOG_LSA * log_lsa);
#endif
static int log_rv_analysis_commit_with_postpone (THREAD_ENTRY * thread_p,
						 int tran_id,
						 LOG_LSA * log_lsa,
						 LOG_PAGE * log_page_p);
static int log_rv_analysis_commit_topope_with_postpone (THREAD_ENTRY *
							thread_p, int tran_id,
							LOG_LSA * log_lsa,
							LOG_PAGE *
							log_page_p);
static int log_rv_analysis_complete (THREAD_ENTRY * thread_p, int tran_id,
				     LOG_LSA * log_lsa, LOG_PAGE * log_page_p,
				     LOG_LSA * prev_lsa,
				     bool is_media_crash,
				     const time_t * stopat,
				     const LOG_LSA * stopat_lsa,
				     bool * did_incom_recovery);
static int log_rv_analysis_complte_topope (THREAD_ENTRY * thread_p,
					   int tran_id, LOG_LSA * log_lsa);
static int log_rv_analysis_start_checkpoint (LOG_LSA * log_lsa,
					     LOG_LSA * start_lsa,
					     bool * may_use_checkpoint);
static int log_rv_analysis_end_checkpoint (THREAD_ENTRY * thread_p,
					   LOG_LSA * log_lsa,
					   LOG_PAGE * log_page_p,
					   LOG_LSA * check_point,
					   LOG_LSA * start_redo_lsa,
					   bool * may_use_checkpoint);
static int log_rv_analysis_save_point (THREAD_ENTRY * thread_p, int tran_id,
				       LOG_LSA * log_lsa);
static int log_rv_analysis_log_end (int tran_id, LOG_LSA * log_lsa);
static void
log_rv_analysis_record (THREAD_ENTRY * thread_p, LOG_RECTYPE log_type,
			int tran_id, LOG_LSA * log_lsa, LOG_PAGE * log_page_p,
			LOG_LSA * check_point, LOG_LSA * prev_lsa,
			LOG_LSA * start_lsa, LOG_LSA * start_redo_lsa,
			bool is_media_crash,
			time_t * stopat, const LOG_LSA * stopat_lsa,
			bool * did_incom_recovery, bool * may_use_checkpoint);
static void log_recovery_analysis (THREAD_ENTRY * thread_p,
				   LOG_LSA * start_lsa,
				   LOG_LSA * start_redolsa,
				   LOG_LSA * end_redo_lsa, int ismedia_crash,
				   time_t * stopat,
				   const LOG_LSA * stopat_lsa,
				   bool * did_incom_recovery,
				   INT64 * num_redo_log_records);
static void log_recovery_redo (THREAD_ENTRY * thread_p,
			       const LOG_LSA * start_redolsa,
			       const LOG_LSA * end_redo_lsa);
static void log_recovery_finish_all_postpone (THREAD_ENTRY * thread_p);
static void log_recovery_undo (THREAD_ENTRY * thread_p);
static void
log_recovery_notpartof_archives (THREAD_ENTRY * thread_p, int start_arv_num,
				 const char *info_reason);
static bool log_unformat_ahead_volumes (THREAD_ENTRY * thread_p, VOLID volid,
					VOLID * start_volid);
static void log_recovery_notpartof_volumes (THREAD_ENTRY * thread_p);
static void log_recovery_resetlog (THREAD_ENTRY * thread_p,
				   LOG_LSA * new_append_lsa,
				   bool is_new_append_page,
				   LOG_LSA * last_lsa);
static int log_recovery_find_first_postpone (THREAD_ENTRY * thread_p,
					     LOG_LSA * ret_lsa,
					     LOG_LSA *
					     start_postpone_lsa,
					     LOG_TDES * tdes);
/*
 * CRASH RECOVERY PROCESS
 */

/*
 * log_rv_undo_record - EXECUTE AN UNDO RECORD
 *
 * return: nothing
 *
 *   log_lsa(in/out): Log address identifier containing the log record
 *   log_page_p(in/out): Pointer to page where data starts (Set as a side
 *              effect to the page where data ends)
 *   rcvindex(in): Index to recovery functions
 *   rcv_vpid(in): Address of page to recover
 *   rcv(in/out): Recovery structure for recovery function
 *   rcv_undo_lsa(in): Address of the undo record
 *   tdes(in/out): State structure of transaction undoing data
 *   undo_unzip_ptr(in):
 *
 * NOTE:Execute an undo log record during restart recovery undo phase.
 *              A compensating log record for operation page level logging is
 *              written by the current function. For logical level logging,
 *              the undo function is responsible to log a redo record, which
 *              is converted into a compensating record by the log manager.
 *
 *              This function is very similar than log_rollback_rec, however,
 *              page locking is not done.. and the transaction index that is
 *              running is set to the one in the tdes. Probably, this
 *              function should have not been duplicated for the above few
 *              things.
 */
static void
log_rv_undo_record (THREAD_ENTRY * thread_p, LOG_LSA * log_lsa,
		    LOG_PAGE * log_page_p, LOG_RCVINDEX rcvindex,
		    const VPID * rcv_vpid, LOG_RCV * rcv,
		    UNUSED_ARG const LOG_LSA * rcv_undo_lsa, LOG_TDES * tdes,
		    LOG_ZIP * undo_unzip_ptr)
{
  char *area = NULL;
  LOG_LSA logical_undo_nxlsa;
  TRAN_STATE save_state;	/* The current state of the transaction. Must be
				 * returned to this state
				 */
  bool is_zip = false;
  logtb_set_current_tran_index (thread_p, tdes->tran_index);

  assert (rcvindex < RCV_INDEX_END);

  /*
   * Fetch the page for physical log records. The page is not locked since the
   * recovery process is the only one running. If the page does not exist
   * anymore or there are problems fetching the page, continue anyhow, so that
   * compensating records are logged.
   */

  if (RCV_IS_LOGICAL_LOG (rcv_vpid, rcvindex)
      || (disk_isvalid_page (thread_p, rcv_vpid->volid,
			     rcv_vpid->pageid) != DISK_VALID))
    {
      rcv->pgptr = NULL;
    }
  else
    {
      rcv->pgptr = pgbuf_fix (thread_p, rcv_vpid, OLD_PAGE, PGBUF_LATCH_WRITE,
			      PGBUF_UNCONDITIONAL_LATCH, PAGE_UNKNOWN);
    }

  /* GET BEFORE DATA */

  /*
   * If data is contained in only one buffer, pass pointer directly.
   * Otherwise, allocate a contiguous area, copy the data and pass this area.
   * At the end deallocate the area.
   */

  if (ZIP_CHECK (rcv->length))
    {				/* check compress data */
      rcv->length = (int) GET_ZIP_LEN (rcv->length);	/* convert compress length */
      is_zip = true;
    }

  if (log_lsa->offset + rcv->length < (int) LOGAREA_SIZE)
    {
      rcv->data = (char *) log_page_p->area + log_lsa->offset;
      log_lsa->offset += rcv->length;
    }
  else
    {
      /* Need to copy the data into a contiguous area */
      area = (char *) malloc (rcv->length);
      if (area == NULL)
	{
	  logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			     "log_rv_undo_record");
	  if (rcv->pgptr != NULL)
	    {
	      pgbuf_unfix (thread_p, rcv->pgptr);
	    }
	  return;
	}
      /* Copy the data */
      logpb_copy_from_log (thread_p, area, rcv->length, log_lsa, log_page_p);
      rcv->data = area;
    }

  if (is_zip)
    {
      if (log_unzip (undo_unzip_ptr, rcv->length, (char *) rcv->data))
	{
	  rcv->length = (int) undo_unzip_ptr->data_length;
	  rcv->data = (char *) undo_unzip_ptr->log_data;
	}
      else
	{
	  logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			     "log_rv_undo_record");
	}
    }

  /* Now call the UNDO recovery function */
  if (rcv->pgptr != NULL || RCV_IS_LOGICAL_LOG (rcv_vpid, rcvindex))
    {

      /*
       * Write a compensating log record for operation page level logging.
       * For logical level logging, the recovery undo function must log an
       * redo/CLR log to describe the undo.
       */

      if (!RCV_IS_LOGICAL_LOG (rcv_vpid, rcvindex))
	{
	  log_append_compensate (thread_p, rcvindex, rcv_vpid,
				 rcv->offset, rcv->pgptr, rcv->length,
				 rcv->data, tdes);
	  (void) (*RV_fun[rcvindex].undofun) (thread_p, rcv);
	}
      else
	{
	  /*
	   * Logical logging. The undo function is responsible for logging the
	   * needed undo and redo records to make the logical undo operation
	   * atomic.
	   * The recovery manager sets a dummy compensating record, to fix the
	   * undo_nx_lsa record at crash recovery time.
	   */
	  LSA_COPY (&logical_undo_nxlsa, &tdes->undo_nxlsa);
	  save_state = tdes->state;

	  /*
	   * A system operation is needed since the postpone operations of an
	   * undo log must be done at the end of the logical undo. Without this
	   * if there is a crash, we will be in trouble since we will not be able
	   * to undo a postpone operation.
	   */
	  if (log_start_system_op (thread_p) == NULL)
	    {
	      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
				 "log_rv_undo_record");
	      if (area != NULL)
		{
		  free_and_init (area);
		}
	      if (rcv->pgptr != NULL)
		{
		  pgbuf_unfix (thread_p, rcv->pgptr);
		}
	      return;
	    }

	  (void) (*RV_fun[rcvindex].undofun) (thread_p, rcv);
	  log_end_system_op (thread_p, LOG_RESULT_TOPOP_COMMIT);
	  tdes->state = save_state;
	  /*
	   * Now add the dummy logical compensating record. This mark the end of
	   * the logical operation.
	   */
	  log_append_logical_compensate (thread_p, rcvindex, tdes,
					 &logical_undo_nxlsa);
	}
    }
  else
    {
      log_append_compensate (thread_p, rcvindex, rcv_vpid,
			     rcv->offset, NULL, rcv->length, rcv->data, tdes);
      /*
       * Unable to fetch page of volume... May need media recovery on such
       * page
       */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOG_MAYNEED_MEDIA_RECOVERY,
	      1, fileio_get_volume_label (rcv_vpid->volid, PEEK));
    }

  if (area != NULL)
    {
      free_and_init (area);
    }

  if (rcv->pgptr != NULL)
    {
      pgbuf_unfix (thread_p, rcv->pgptr);
    }
}

/*
 * log_rv_redo_record - EXECUTE A REDO RECORD
 *
 * return: nothing
 *
 *   log_lsa(in/out): Log address identifier containing the log record
 *   log_page_p(in/out): Pointer to page where data starts (Set as a side
 *               effect to the page where data ends)
 *   redofun(in): Function to invoke to redo the data
 *   rcv(in/out): Recovery structure for recovery function(Set as a side
 *               effect)
 *   rcv_lsa_ptr(in): Reset data page (rcv->pgptr) to this LSA
 *   undo_length(in):
 *   undo_data(in):
 *   redo_unzip_ptr(in):
 *
 * NOTE: Execute a redo log record.
 */
static void
log_rv_redo_record (THREAD_ENTRY * thread_p, LOG_LSA * log_lsa,
		    LOG_PAGE * log_page_p,
		    int (*redofun) (THREAD_ENTRY * thread_p, LOG_RCV *),
		    LOG_RCV * rcv, LOG_LSA * rcv_lsa_ptr,
		    int undo_length, char *undo_data,
		    LOG_ZIP * redo_unzip_ptr)
{
  char *area = NULL;
  bool is_zip = false;

  /* Note the the data page rcv->pgptr has been fetched by the caller */

  /*
   * If data is contained in only one buffer, pass pointer directly.
   * Otherwise, allocate a contiguous area, copy the data and pass this area.
   * At the end deallocate the area.
   */

  if (ZIP_CHECK (rcv->length))
    {
      rcv->length = (int) GET_ZIP_LEN (rcv->length);
      is_zip = true;
    }

  if (log_lsa->offset + rcv->length < (int) LOGAREA_SIZE)
    {
      rcv->data = (char *) log_page_p->area + log_lsa->offset;
      log_lsa->offset += rcv->length;
    }
  else
    {
      area = (char *) malloc (rcv->length);
      if (area == NULL)
	{
	  logpb_fatal_error (thread_p, true, ARG_FILE_LINE, "log_rvredo_rec");
	  return;
	}
      /* Copy the data */
      logpb_copy_from_log (thread_p, area, rcv->length, log_lsa, log_page_p);
      rcv->data = area;
    }

  if (is_zip)
    {
      if (log_unzip (redo_unzip_ptr, rcv->length, (char *) rcv->data))
	{
	  if ((undo_length > 0) && (undo_data != NULL))
	    {
	      (void) log_diff (undo_length, undo_data,
			       redo_unzip_ptr->data_length,
			       redo_unzip_ptr->log_data);
	      rcv->length = (int) redo_unzip_ptr->data_length;
	      rcv->data = (char *) redo_unzip_ptr->log_data;
	    }
	  else
	    {
	      rcv->length = (int) redo_unzip_ptr->data_length;
	      rcv->data = (char *) redo_unzip_ptr->log_data;
	    }
	}
      else
	{
	  logpb_fatal_error (thread_p, true, ARG_FILE_LINE, "log_rvredo_rec");
	}
    }

  if (redofun != NULL)
    {
      (void) (*redofun) (thread_p, rcv);
    }
  else
    {
      er_log_debug (ARG_FILE_LINE, "log_rvredo_rec: WARNING.. There is not a"
		    " REDO function to execute. May produce recovery problems.");
    }

  if (rcv->pgptr != NULL)
    {
      (void) pgbuf_set_lsa (thread_p, rcv->pgptr, rcv_lsa_ptr);
    }

  if (area != NULL)
    {
      free_and_init (area);
    }
}

/*
 * log_rv_find_checkpoint - FIND RECOVERY CHECKPOINT
 *
 * return: true
 *
 *   volid(in): Volume identifier
 *   rcv_lsa(in/out): Recovery log sequence address
 *
 * NOTE: Find the recovery checkpoint address of the given volume. If
 *              it is smaller than rcv_lsa, rcv_lsa is reset to such value.
 */
static bool
log_rv_find_checkpoint (THREAD_ENTRY * thread_p, VOLID volid,
			LOG_LSA * rcv_lsa)
{
  LOG_LSA chkpt_lsa;		/* Checkpoint LSA of volume */
  UNUSED_VAR int ret = NO_ERROR;

  ret = disk_get_checkpoint (thread_p, volid, &chkpt_lsa);
  if (LSA_ISNULL (rcv_lsa) || LSA_LT (&chkpt_lsa, rcv_lsa))
    {
      LSA_COPY (rcv_lsa, &chkpt_lsa);
    }

  return true;
}

/*
 * get_log_data - GET UNZIP LOG DATA FROM LOG
 *
 * return:
 *
 *   length(in): log data size
 *   log_lsa(in/out): Log address identifier containing the log record
 *   log_page_p(in): Log page pointer where LSA is located
 *   undo_unzip_ptr(in):
 *
 * NOTE:if log_data is unzip data return LOG_ZIP data
 *               else log_data is zip data return unzip log data
 */
static bool
log_rv_get_unzip_log_data (THREAD_ENTRY * thread_p, int length,
			   LOG_LSA * log_lsa, LOG_PAGE * log_page_p,
			   LOG_ZIP * undo_unzip_ptr)
{
  char *ptr;			/* Pointer to data to be printed            */
  char *area = NULL;
  bool is_zip = false;

  /*
   * If data is contained in only one buffer, pass pointer directly.
   * Otherwise, allocate a contiguous area,
   * copy the data and pass this area. At the end deallocate the area
   */

  if (ZIP_CHECK (length))
    {
      length = (int) GET_ZIP_LEN (length);
      is_zip = true;
    }

  if (log_lsa->offset + length < (int) LOGAREA_SIZE)
    {
      /* Data is contained in one buffer */
      ptr = (char *) log_page_p->area + log_lsa->offset;
      log_lsa->offset += length;
    }
  else
    {
      /* Need to copy the data into a contiguous area */
      area = (char *) malloc (length);
      if (area == NULL)
	{
	  return false;
	}
      /* Copy the data */
      logpb_copy_from_log (thread_p, area, length, log_lsa, log_page_p);
      ptr = area;
    }

  if (is_zip)
    {
      if (!log_unzip (undo_unzip_ptr, length, ptr))
	{
	  if (area != NULL)
	    {
	      free_and_init (area);
	    }
	  return false;
	}
    }
  else
    {
      undo_unzip_ptr->data_length = length;
      memcpy (undo_unzip_ptr->log_data, ptr, length);
    }
  LOG_READ_ALIGN (thread_p, log_lsa, log_page_p);

  if (area != NULL)
    {
      free_and_init (area);
    }

  return true;
}

/*
 * log_recovery - Recover information
 *
 * return: nothing
 *
 *   ismedia_crash(in):Are we recovering from a media crash ?
 *   stopat(in):
 *   stopat_lsa(in): derived from make_slave
 *
 */
void
log_recovery (THREAD_ENTRY * thread_p, int ismedia_crash,
	      time_t * stopat, LOG_LSA * stopat_lsa)
{
  LOG_TDES *rcv_tdes;		/* Tran. descriptor for the recovery phase */
  LOG_RECORD_HEADER *eof;	/* End of the log record                   */
  LOG_LSA rcv_lsa;		/* Where to start the recovery             */
  LOG_LSA start_redolsa;	/* Where to start redo phase               */
  LOG_LSA end_redo_lsa;		/* Where to stop the redo phase            */
  bool did_incom_recovery;
  INT64 num_redo_log_records;

  assert (LOG_CS_OWN_WRITE_MODE (thread_p));
  assert (logtb_get_current_tran_index (thread_p) == LOG_SYSTEM_TRAN_INDEX);

  rcv_tdes = logtb_get_current_tdes (thread_p);
  if (rcv_tdes == NULL)
    {
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_LOG_UNKNOWN_TRANINDEX, 1,
	      logtb_get_current_tran_index (thread_p));
      logpb_fatal_error (thread_p, true, ARG_FILE_LINE, "log_recovery");
      return;
    }

  rcv_tdes->state = TRAN_RECOVERY;

  /* Find the starting LSA for the analysis phase */

  LSA_COPY (&rcv_lsa, &log_Gl.hdr.chkpt_lsa);
  if (ismedia_crash == true)
    {
      /*
       * Media crash, we may have to start from an older checkpoint...
       * check disk headers
       */
      (void) fileio_map_mounted (thread_p,
				 (bool (*)(THREAD_ENTRY *, VOLID, void *))
				 log_rv_find_checkpoint, &rcv_lsa);
    }
  else
    {
      /*
       * We do incomplete recovery only when we are coming from a media crash.
       * That is, we are restarting from a backup
       */
      assert (stopat == NULL);
      assert (stopat_lsa == NULL);

      if (stopat != NULL)
	{
	  *stopat = -1;		/* is impossible */
	}
      if (stopat_lsa != NULL)
	{
	  LSA_SET_NULL (stopat_lsa);	/* is impossible */
	}
    }

  /*
   * First,  ANALYSIS the log to find the state of the transactions
   * Second, REDO going forward
   * Last,   UNDO going backwards
   */

  log_Gl.rcv_phase = LOG_RECOVERY_ANALYSIS_PHASE;
  log_recovery_analysis (thread_p, &rcv_lsa, &start_redolsa, &end_redo_lsa,
			 ismedia_crash, stopat, stopat_lsa,
			 &did_incom_recovery, &num_redo_log_records);

  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
	  ER_LOG_RECOVERY_STARTED, 3, num_redo_log_records,
	  start_redolsa.pageid, end_redo_lsa.pageid);

  assert (stopat_lsa == NULL || LSA_LE (stopat_lsa, &end_redo_lsa));

  LSA_COPY (&log_Gl.chkpt_redo_lsa, &start_redolsa);

  logtb_set_to_system_tran_index (thread_p);
  if (logpb_fetch_start_append_page (thread_p) == NULL)
    {
      logpb_fatal_error (thread_p, true, ARG_FILE_LINE, "log_recovery");
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
	      ER_LOG_RECOVERY_FINISHED, 0);
      return;
    }

  if (did_incom_recovery == false)
    {
      /* Read the End of file record to find out the previous address */
      eof = (LOG_RECORD_HEADER *) LOG_APPEND_PTR ();
      LOG_RESET_PREV_LSA (&eof->back_lsa);
    }

  log_append_empty_record (thread_p, LOG_DUMMY_CRASH_RECOVERY, NULL);

  /*
   * Save the crash point lsa for use during the remaining recovery
   * phases.
   */
  LSA_COPY (&log_Gl.rcv_phase_lsa, &rcv_tdes->last_lsa);

  /* Redo phase */
  log_Gl.rcv_phase = LOG_RECOVERY_REDO_PHASE;

  logtb_set_to_system_tran_index (thread_p);

  log_recovery_redo (thread_p, &start_redolsa, &end_redo_lsa);
  boot_reset_db_parm (thread_p);

  /* Undo phase */
  log_Gl.rcv_phase = LOG_RECOVERY_UNDO_PHASE;

  logtb_set_to_system_tran_index (thread_p);

  log_recovery_undo (thread_p);
  boot_reset_db_parm (thread_p);

  if (did_incom_recovery == true)
    {
      log_recovery_notpartof_volumes (thread_p);
    }

  /* Client loose ends */
  rcv_tdes->state = TRAN_ACTIVE;

  logtb_set_to_system_tran_index (thread_p);

  /* Dismount any archive and checkpoint the database */
  logpb_decache_archive_info (thread_p);

  /* Flush all dirty pages */
  logpb_flush_pages_direct (thread_p);

  logpb_flush_header (thread_p);

  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_LOG_RECOVERY_FINISHED,
	  0);
}

/*
 * log_rv_analysis_undo_redo -
 *
 * return: error code
 *
 *   tran_id(in):
 *   lsa(in/out):
 * Note:
 */
static int
log_rv_analysis_undo_redo (THREAD_ENTRY * thread_p, int tran_id,
			   LOG_LSA * log_lsa)
{
  LOG_TDES *tdes;

  /*
   * If this is the first time, the transaction is seen. Assign a new
   * index to describe it and assume that the transaction was active
   * at the time of the crash, and thus it will be unilateraly
   * aborted. The truth of this statement will be find reading the
   * rest of the log
   */
  tdes = logtb_rv_find_allocate_tran_index (thread_p, tran_id, log_lsa);
  if (tdes == NULL)
    {
      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			 "log_rv_analysis_undo_redo");
      return ER_FAILED;
    }

  /* New tail and next to undo */
  LSA_COPY (&tdes->last_lsa, log_lsa);
  LSA_COPY (&tdes->undo_nxlsa, &tdes->last_lsa);

  return NO_ERROR;
}

/*
 * log_rv_analysis_dummy_head_postpone -
 *
 * return: error code
 *
 *   tran_id(in):
 *   lsa(in/out):
 * Note:
 */
static int
log_rv_analysis_dummy_head_postpone (THREAD_ENTRY * thread_p, int tran_id,
				     LOG_LSA * log_lsa)
{
  LOG_TDES *tdes;

  /*
   * If this is the first time, the transaction is seen. Assign a new
   * index to describe it and assume that the transaction was active
   * at the time of the crash, and thus it will be unilateraly
   * aborted. The truth of this statement will be find reading the
   * rest of the log
   */
  tdes = logtb_rv_find_allocate_tran_index (thread_p, tran_id, log_lsa);
  if (tdes == NULL)
    {
      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			 "log_rv_analysis_dummy_head_postpone");
      return ER_FAILED;
    }

  /* New tail and next to undo */
  LSA_COPY (&tdes->last_lsa, log_lsa);
  LSA_COPY (&tdes->undo_nxlsa, &tdes->last_lsa);

  /* if first postpone, then set address late */
  if (LSA_ISNULL (&tdes->posp_nxlsa))
    {
      LSA_COPY (&tdes->posp_nxlsa, &tdes->last_lsa);
    }

  return NO_ERROR;
}

/*
 * log_rv_analysis_postpone -
 *
 * return: error code
 *
 *   tran_id(in):
 *   lsa(in/out):
 *
 * Note:
 */
static int
log_rv_analysis_postpone (THREAD_ENTRY * thread_p, int tran_id,
			  LOG_LSA * log_lsa)
{
  LOG_TDES *tdes;

  /*
   * If this is the first time, the transaction is seen. Assign a new
   * index to describe it and assume that the transaction was active
   * at the time of the crash, and thus it will be unilateraly
   * aborted. The truth of this statement will be find reading the
   * rest of the log
   */
  tdes = logtb_rv_find_allocate_tran_index (thread_p, tran_id, log_lsa);
  if (tdes == NULL)
    {
      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			 "log_rv_analysis_postpone");
      return ER_FAILED;
    }

  /* if first postpone, then set address early */
  if (LSA_ISNULL (&tdes->posp_nxlsa))
    {
      LSA_COPY (&tdes->posp_nxlsa, &tdes->last_lsa);
    }

  /* New tail and next to undo */
  LSA_COPY (&tdes->last_lsa, log_lsa);
  LSA_COPY (&tdes->undo_nxlsa, &tdes->last_lsa);

  return NO_ERROR;
}

/*
 * log_rv_analysis_run_postpone -
 *
 * return: error code
 *
 *   tran_id(in):
 *   lsa(in/out):
 *   log_page_p(in/out):
 *   check_point(in/out):
 *
 * Note:
 */
static int
log_rv_analysis_run_postpone (THREAD_ENTRY * thread_p, int tran_id,
			      LOG_LSA * log_lsa, LOG_PAGE * log_page_p,
			      LOG_LSA * check_point)
{
  LOG_TDES *tdes;
  struct log_run_postpone *run_posp;

  tdes = logtb_rv_find_allocate_tran_index (thread_p, tran_id, log_lsa);
  if (tdes == NULL)
    {
      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			 "log_rv_analysis_run_postpone");
      return ER_FAILED;
    }

  if (tdes->state != TRAN_UNACTIVE_WILL_COMMIT
      && tdes->state != TRAN_UNACTIVE_COMMITTED_WITH_POSTPONE
      && tdes->state != TRAN_UNACTIVE_TOPOPE_COMMITTED_WITH_POSTPONE)
    {
      /*
       * If we are comming from a checkpoint this is the result of a
       * system error since the transaction must have been already in
       * one of these states.
       * If we are not comming from a checkpoint, it is likely that
       * we are in a commit point of either a transaction or a top
       * operation.
       */
      if (!LSA_ISNULL (check_point))
	{
	  er_log_debug (ARG_FILE_LINE,
			"log_recovery_analysis: SYSTEM ERROR\n"
			" Incorrect state = %s\n at log_rec at %lld|%d\n"
			" for transaction = %d (index %d).\n"
			" State should have been either of\n"
			" %s\n %s\n %s\n", log_state_string (tdes->state),
			(long long int) log_lsa->pageid, log_lsa->offset,
			tdes->trid, tdes->tran_index,
			log_state_string (TRAN_UNACTIVE_WILL_COMMIT),
			log_state_string
			(TRAN_UNACTIVE_COMMITTED_WITH_POSTPONE),
			log_state_string
			(TRAN_UNACTIVE_TOPOPE_COMMITTED_WITH_POSTPONE));
	}
      /*
       * Continue the execution by guessing that the transaction has
       * been committed
       */
      if (tdes->topops.last == -1)
	{
	  tdes->state = TRAN_UNACTIVE_COMMITTED_WITH_POSTPONE;
	}
      else
	{
	  tdes->state = TRAN_UNACTIVE_TOPOPE_COMMITTED_WITH_POSTPONE;
	}
    }

  if (tdes->state == TRAN_UNACTIVE_COMMITTED_WITH_POSTPONE)
    {
      /* Nothing to undo */
      LSA_SET_NULL (&tdes->undo_nxlsa);
    }

  LSA_COPY (&tdes->last_lsa, log_lsa);

  /*
   * Need to read the log_run_postpone record to reset the posp_nxlsa
   * of transaction or top action to the value of log_ref
   */

  /* Read the DATA HEADER */
  LOG_READ_ADD_ALIGN (thread_p, sizeof (LOG_RECORD_HEADER), log_lsa,
		      log_page_p);
  LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p,
				    sizeof (struct log_run_postpone), log_lsa,
				    log_page_p);

  run_posp = (struct log_run_postpone *) ((char *) log_page_p->area
					  + log_lsa->offset);

  if (tdes->state == TRAN_UNACTIVE_TOPOPE_COMMITTED_WITH_POSTPONE)
    {
      /* Reset start of postpone transaction for the top action */
      LSA_COPY (&tdes->topops.stack[tdes->topops.last].posp_lsa,
		&run_posp->ref_lsa);
    }
  else
    {
      assert (tdes->state == TRAN_UNACTIVE_WILL_COMMIT
	      || tdes->state == TRAN_UNACTIVE_COMMITTED_WITH_POSTPONE);

      /* Reset start of postpone transaction */
      LSA_COPY (&tdes->posp_nxlsa, &run_posp->ref_lsa);
    }

  return NO_ERROR;
}

/*
 * log_rv_analysis_compensate -
 *
 * return: error code
 *
 *   tran_id(in):
 *   lsa(in/out):
 *   log_page_p(in/out):
 *
 * Note:
 */
static int
log_rv_analysis_compensate (THREAD_ENTRY * thread_p, int tran_id,
			    LOG_LSA * log_lsa, LOG_PAGE * log_page_p)
{
  LOG_TDES *tdes;
  struct log_compensate *compensate;

  /*
   * If this is the first time, the transaction is seen. Assign a new
   * index to describe it and assume that the transaction was active
   * at the time of the crash, and thus it will be unilateraly
   * aborted. The truth of this statement will be find reading the
   * rest of the log
   */
  tdes = logtb_rv_find_allocate_tran_index (thread_p, tran_id, log_lsa);
  if (tdes == NULL)
    {
      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			 "log_rv_analysis_compensate");
      return ER_FAILED;
    }

  /*
   * Need to read the compensating record to set the next undo address
   */

  /* Read the DATA HEADER */
  LOG_READ_ADD_ALIGN (thread_p, sizeof (LOG_RECORD_HEADER), log_lsa,
		      log_page_p);
  LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p, sizeof (struct log_compensate),
				    log_lsa, log_page_p);

  compensate = (struct log_compensate *) ((char *) log_page_p->area
					  + log_lsa->offset);
  LSA_COPY (&tdes->undo_nxlsa, &compensate->undo_nxlsa);

  return NO_ERROR;
}

/*
 * log_rv_analysis_lcompensate -
 *
 * return: error code
 *
 *   tran_id(in):
 *   lsa(in/out):
 *   log_page_p(in/out):
 *
 * Note:
 */
static int
log_rv_analysis_lcompensate (THREAD_ENTRY * thread_p, int tran_id,
			     LOG_LSA * log_lsa, LOG_PAGE * log_page_p)
{
  LOG_TDES *tdes;
  struct log_logical_compensate *logical_comp;

  /*
   * If this is the first time, the transaction is seen. Assign a new
   * index to describe it and assume that the transaction was active
   * at the time of the crash, and thus it will be unilateraly
   * aborted. The truth of this statement will be find reading the
   * rest of the log
   */
  tdes = logtb_rv_find_allocate_tran_index (thread_p, tran_id, log_lsa);
  if (tdes == NULL)
    {
      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			 "log_rv_analysis_lcompensate");
      return ER_FAILED;
    }

  /*
   * Need to read the compensating record to set the next undo address
   */

  /* Read the DATA HEADER */
  LOG_READ_ADD_ALIGN (thread_p, sizeof (LOG_RECORD_HEADER), log_lsa,
		      log_page_p);
  LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p,
				    sizeof (struct log_logical_compensate),
				    log_lsa, log_page_p);

  logical_comp = ((struct log_logical_compensate *)
		  ((char *) log_page_p->area + log_lsa->offset));
  LSA_COPY (&tdes->undo_nxlsa, &logical_comp->undo_nxlsa);

  return NO_ERROR;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * log_rv_analysis_will_commit -
 *
 * return: error code
 *
 *   tran_id(in):
 *   lsa(in/out):
 *
 * Note:
 */
static int
log_rv_analysis_will_commit (THREAD_ENTRY * thread_p, int tran_id,
			     LOG_LSA * log_lsa)
{
  LOG_TDES *tdes;

  /*
   * If this is the first time, the transaction is seen. Assign a new
   * index to describe it. The transaction was in the process of
   * getting committed at this point.
   */
  tdes = logtb_rv_find_allocate_tran_index (thread_p, tran_id, log_lsa);
  if (tdes == NULL)
    {
      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			 "log_rv_analysis_will_commit");
      return ER_FAILED;
    }

  tdes->state = TRAN_UNACTIVE_WILL_COMMIT;

  /* Nothing to undo */
  LSA_SET_NULL (&tdes->undo_nxlsa);
  LSA_COPY (&tdes->last_lsa, log_lsa);

  return NO_ERROR;
}
#endif

/*
 * log_rv_analysis_commit_with_postpone -
 *
 * return: error code
 *
 *   tran_id(in):
 *   lsa(in/out):
 *   log_page_p(in/out):
 *
 * Note:
 */
static int
log_rv_analysis_commit_with_postpone (THREAD_ENTRY * thread_p, int tran_id,
				      LOG_LSA * log_lsa,
				      LOG_PAGE * log_page_p)
{
  LOG_TDES *tdes;
  struct log_start_postpone *start_posp;

  /*
   * If this is the first time, the transaction is seen. Assign a new
   * index to describe it. The transaction was in the process of
   * getting committed at this point.
   */
  tdes = logtb_rv_find_allocate_tran_index (thread_p, tran_id, log_lsa);
  if (tdes == NULL)
    {
      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			 "log_rv_analysis_commit_with_postpone");
      return ER_FAILED;
    }

  tdes->state = TRAN_UNACTIVE_COMMITTED_WITH_POSTPONE;

  /* Nothing to undo */
  LSA_SET_NULL (&tdes->undo_nxlsa);
  LSA_COPY (&tdes->last_lsa, log_lsa);

  /*
   * Need to read the start postpone record to set the postpone address
   * of the transaction
   */
  LOG_READ_ADD_ALIGN (thread_p, sizeof (LOG_RECORD_HEADER), log_lsa,
		      log_page_p);
  LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p,
				    sizeof (struct log_start_postpone),
				    log_lsa, log_page_p);

  start_posp =
    (struct log_start_postpone *) ((char *) log_page_p->area +
				   log_lsa->offset);
  LSA_COPY (&tdes->posp_nxlsa, &start_posp->posp_lsa);

  return NO_ERROR;
}

/*
 * log_rv_analysis_commit_topope_with_postpone -
 *
 * return: error code
 *
 *   tran_id(in):
 *   lsa(in/out):
 *   log_page_p(in/out):
 *
 * Note:
 */
static int
log_rv_analysis_commit_topope_with_postpone (THREAD_ENTRY * thread_p,
					     int tran_id, LOG_LSA * log_lsa,
					     LOG_PAGE * log_page_p)
{
  LOG_TDES *tdes;
  struct log_topope_start_postpone *top_start_posp;

  /*
   * If this is the first time, the transaction is seen. Assign a new
   * index to describe it. A top system operation was in the process
   * of getting committed at this point.
   */
  tdes = logtb_rv_find_allocate_tran_index (thread_p, tran_id, log_lsa);
  if (tdes == NULL)
    {
      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			 "log_rv_analysis_commit_topope_with_postpone");
      return ER_FAILED;
    }

  tdes->state = TRAN_UNACTIVE_TOPOPE_COMMITTED_WITH_POSTPONE;

  LSA_COPY (&tdes->last_lsa, log_lsa);
  LSA_COPY (&tdes->undo_nxlsa, &tdes->last_lsa);

  /*
   * Need to read the start postpone record to set the start address
   * of top system operation
   */
  LOG_READ_ADD_ALIGN (thread_p, sizeof (LOG_RECORD_HEADER), log_lsa,
		      log_page_p);
  LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p,
				    sizeof (struct log_topope_start_postpone),
				    log_lsa, log_page_p);
  top_start_posp =
    ((struct log_topope_start_postpone *) ((char *) log_page_p->area +
					   log_lsa->offset));

  if (tdes->topops.max == 0 || (tdes->topops.last + 1) >= tdes->topops.max)
    {
      if (logtb_realloc_topops_stack (tdes, 1) == NULL)
	{
	  /* Out of memory */
	  logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			     "log_recovery_analysis");
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
    }

  /*
   * NOTE if tdes->topops.last >= 0, there is an already
   * defined top system operation. However, I do not think so
   * do to the nested fashion of top system operations. Outer
   * top nested system operations will come later in the log.
   */

  if (tdes->topops.last == -1)
    {
      tdes->topops.last++;
    }

  LSA_COPY (&tdes->topops.stack[tdes->topops.last].lastparent_lsa,
	    &top_start_posp->lastparent_lsa);
  LSA_COPY (&tdes->topops.stack[tdes->topops.last].posp_lsa,
	    &top_start_posp->posp_lsa);

  return NO_ERROR;
}

/*
 * log_rv_analysis_complete -
 *
 * return: error code
 *
 *   tran_id(in):
 *   log_lsa(in/out):
 *   log_page_p(in/out):
 *   lsa(in/out):
 *   is_media_crash(in):
 *   stopat(in):
 *   stopat_lsa(in): derived from make_slave
 *   did_incom_recovery(in/out):
 *
 * Note:
 */
static int
log_rv_analysis_complete (THREAD_ENTRY * thread_p, int tran_id,
			  LOG_LSA * log_lsa, LOG_PAGE * log_page_p,
			  LOG_LSA * prev_lsa,
			  bool is_media_crash, const time_t * stopat,
			  const LOG_LSA * stopat_lsa,
			  bool * did_incom_recovery)
{
  struct log_donetime *donetime;
  int tran_index;
  time_t last_at_time;
  char time_val[CTIME_MAX];
  LOG_LSA record_header_lsa;
  bool found_stopat;

  found_stopat = false;		/* init */

  /*
   * The transaction has been fully completed. therefore, it was not
   * active at the time of the crash
   */
  tran_index = logtb_find_tran_index (thread_p, tran_id);

  if (is_media_crash != true)
    {
      goto end;
    }

  LSA_COPY (&record_header_lsa, log_lsa);

  /*
   * Need to read the donetime record to find out if we need to stop
   * the recovery at this point.
   */
  LOG_READ_ADD_ALIGN (thread_p, sizeof (LOG_RECORD_HEADER), log_lsa,
		      log_page_p);
  LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p, sizeof (struct log_donetime),
				    log_lsa, log_page_p);

  donetime = (struct log_donetime *) ((char *) log_page_p->area
				      + log_lsa->offset);
  last_at_time = (time_t) donetime->at_time;
  if (stopat != NULL && *stopat != (time_t) (-1)
      && difftime (*stopat, last_at_time) < 0)
    {
      found_stopat = true;
    }
  else if (stopat_lsa != NULL && !LSA_ISNULL (stopat_lsa)
	   && LSA_LE (stopat_lsa, log_lsa))
    {
      found_stopat = true;
    }

  if (found_stopat)
    {
#if !defined(NDEBUG)
      if (prm_get_bool_value (PRM_ID_LOG_TRACE_DEBUG))
	{
	  fprintf (stdout,
		   msgcat_message (MSGCAT_CATALOG_RYE,
				   MSGCAT_SET_LOG, MSGCAT_LOG_STARTS));
	  (void) ctime_r (&last_at_time, time_val);
	  fprintf (stdout,
		   msgcat_message (MSGCAT_CATALOG_RYE,
				   MSGCAT_SET_LOG,
				   MSGCAT_LOG_INCOMPLTE_MEDIA_RECOVERY),
		   record_header_lsa.pageid, record_header_lsa.offset,
		   time_val);
	  fprintf (stdout,
		   msgcat_message (MSGCAT_CATALOG_RYE, MSGCAT_SET_LOG,
				   MSGCAT_LOG_STARTS));
	  fflush (stdout);
	}
#endif /* !NDEBUG */
      /*
       * Reset the log active and stop the recovery process at this
       * point. Before reseting the log, make sure that we are not
       * holding a page.
       */
      log_lsa->pageid = NULL_PAGEID;
      log_recovery_resetlog (thread_p, &record_header_lsa, false, prev_lsa);
      *did_incom_recovery = true;

      return NO_ERROR;
    }

end:

  /*
   * The transaction has been fully completed. Therefore, it was not
   * active at the time of the crash
   */
  if (tran_index != NULL_TRAN_INDEX)
    {
      logtb_free_tran_index (thread_p, tran_index);
    }

  return NO_ERROR;
}

/*
 * log_rv_analysis_complte_topope -
 *
 * return: error code
 *
 *   tran_id(in):
 *   lsa(in/out):
 *
 * Note:
 */
static int
log_rv_analysis_complte_topope (THREAD_ENTRY * thread_p, int tran_id,
				LOG_LSA * log_lsa)
{
  LOG_TDES *tdes;

  /*
   * The top system action is declared as finished. Pop it from the
   * stack of finished actions
   */
  tdes = logtb_rv_find_allocate_tran_index (thread_p, tran_id, log_lsa);
  if (tdes == NULL)
    {
      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			 "log_rv_analysis_complte_topope");
      return ER_FAILED;
    }

  LSA_COPY (&tdes->last_lsa, log_lsa);
  LSA_COPY (&tdes->undo_nxlsa, &tdes->last_lsa);

  if (tdes->topops.last >= 0)
    {
      tdes->topops.last--;
    }

  LSA_COPY (&tdes->tail_topresult_lsa, log_lsa);
  tdes->state = TRAN_UNACTIVE_UNILATERALLY_ABORTED;

  return NO_ERROR;
}

/*
 * log_rv_analysis_start_checkpoint -
 *
 * return: error code
 *
 *   lsa(in/out):
 *   start_lsa(in/out):
 *   may_use_checkpoint(in/out):
 *
 * Note:
 */
static int
log_rv_analysis_start_checkpoint (LOG_LSA * log_lsa, LOG_LSA * start_lsa,
				  bool * may_use_checkpoint)
{
  /*
   * Use the checkpoint record only if it is the first record in the
   * analysis. If it is not, it is likely that we are restarting from
   * crashes when the multimedia crashes were off. We skip the
   * checkpoint since it can contain stuff which does not exist any
   * longer.
   */

  if (LSA_EQ (log_lsa, start_lsa))
    {
      *may_use_checkpoint = true;
    }

  return NO_ERROR;
}

/*
 * log_rv_analysis_end_checkpoint -
 *
 * return: error code
 *
 *   lsa(in/out):
 *   log_page_p(in/out):
 *   check_point(in/out):
 *   start_redo_lsa(in/out):
 *   may_use_checkpoint(in/out):
 *
 * Note:
 */
static int
log_rv_analysis_end_checkpoint (THREAD_ENTRY * thread_p, LOG_LSA * log_lsa,
				LOG_PAGE * log_page_p, LOG_LSA * check_point,
				LOG_LSA * start_redo_lsa,
				bool * may_use_checkpoint)
{
  LOG_TDES *tdes;
  struct log_chkpt *tmp_chkpt;
  struct log_chkpt chkpt;
  struct log_chkpt_trans *chkpt_trans;
  struct log_chkpt_trans *chkpt_one;
  struct log_chkpt_topops_commit_posp *chkpt_topops;
  struct log_chkpt_topops_commit_posp *chkpt_topone;
  int size;
  void *area;
  int i;

  /*
   * Use the checkpoint record only if it is the first record in the
   * analysis. If it is not, it is likely that we are restarting from
   * crashes when the multimedia crashes were off. We skip the
   * checkpoint since it can contain stuff which does not exist any
   * longer.
   */

  if (*may_use_checkpoint == false)
    {
      return NO_ERROR;
    }
  *may_use_checkpoint = false;

  /*
   * Read the checkpoint record information to find out the
   * start_redolsa and the active transactions
   */

  LSA_COPY (check_point, log_lsa);

  /* Read the DATA HEADER */
  LOG_READ_ADD_ALIGN (thread_p, sizeof (LOG_RECORD_HEADER), log_lsa,
		      log_page_p);
  LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p, sizeof (struct log_chkpt),
				    log_lsa, log_page_p);
  tmp_chkpt =
    (struct log_chkpt *) ((char *) log_page_p->area + log_lsa->offset);
  chkpt = *tmp_chkpt;

  /* GET THE CHECKPOINT TRANSACTION INFORMATION */
  LOG_READ_ADD_ALIGN (thread_p, sizeof (struct log_chkpt), log_lsa,
		      log_page_p);

  /* Now get the data of active transactions */

  area = NULL;
  size = sizeof (struct log_chkpt_trans) * chkpt.ntrans;
  if (log_lsa->offset + size < (int) LOGAREA_SIZE)
    {
      chkpt_trans =
	(struct log_chkpt_trans *) ((char *) log_page_p->area +
				    log_lsa->offset);
      log_lsa->offset += size;
    }
  else
    {
      /* Need to copy the data into a contiguous area */
      area = malloc (size);
      if (area == NULL)
	{
	  logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			     "log_recovery_analysis");
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
      /* Copy the data */
      logpb_copy_from_log (thread_p, (char *) area, size, log_lsa,
			   log_page_p);
      chkpt_trans = (struct log_chkpt_trans *) area;
    }

  /* Add the transactions to the transaction table */
  for (i = 0; i < chkpt.ntrans; i++)
    {
      /*
       * If this is the first time, the transaction is seen. Assign a
       * new index to describe it and assume that the transaction was
       * active at the time of the crash, and thus it will be
       * unilateraly aborted. The truth of this statement will be find
       * reading the rest of the log
       */
      tdes =
	logtb_rv_find_allocate_tran_index (thread_p, chkpt_trans[i].trid,
					   log_lsa);
      if (tdes == NULL)
	{
	  if (area != NULL)
	    {
	      free_and_init (area);
	    }

	  logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			     "log_recovery_analysis");
	  return ER_FAILED;
	}
      chkpt_one = &chkpt_trans[i];

      /*
       * Clear the transaction since it may have old stuff in it.
       * Use the one that is find in the checkpoint record
       */
      logtb_clear_tdes (thread_p, tdes);
      tdes->trid = chkpt_one->trid;

      if (chkpt_one->state == TRAN_ACTIVE
	  || chkpt_one->state == TRAN_UNACTIVE_ABORTED)
	{
	  tdes->state = TRAN_UNACTIVE_UNILATERALLY_ABORTED;
	}
      else
	{
	  tdes->state = chkpt_one->state;
	}
      LSA_COPY (&tdes->begin_lsa, &chkpt_one->begin_lsa);
      LSA_COPY (&tdes->last_lsa, &chkpt_one->last_lsa);
      LSA_COPY (&tdes->undo_nxlsa, &chkpt_one->undo_nxlsa);
      LSA_COPY (&tdes->posp_nxlsa, &chkpt_one->posp_nxlsa);
      LSA_COPY (&tdes->savept_lsa, &chkpt_one->savept_lsa);
      LSA_COPY (&tdes->tail_topresult_lsa, &chkpt_one->tail_topresult_lsa);
      logtb_set_client_ids_all (&tdes->client, 0, NULL, chkpt_one->user_name,
				NULL, NULL, NULL, -1);
    }

  if (area != NULL)
    {
      free_and_init (area);
    }

  /*
   * Now add top system operations that were in the process of
   * commit to this transactions
   */

  if (chkpt.ntops > 0)
    {
      size = sizeof (struct log_chkpt_topops_commit_posp) * chkpt.ntops;
      if (log_lsa->offset + size < (int) LOGAREA_SIZE)
	{
	  chkpt_topops = ((struct log_chkpt_topops_commit_posp *)
			  ((char *) log_page_p->area + log_lsa->offset));
	  log_lsa->offset += size;
	}
      else
	{
	  /* Need to copy the data into a contiguous area */
	  area = malloc (size);
	  if (area == NULL)
	    {
	      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
				 "log_recovery_analysis");
	      return ER_OUT_OF_VIRTUAL_MEMORY;
	    }
	  /* Copy the data */
	  logpb_copy_from_log (thread_p, (char *) area, size, log_lsa,
			       log_page_p);
	  chkpt_topops = (struct log_chkpt_topops_commit_posp *) area;
	}

      /* Add the top system operations to the transactions */

      for (i = 0; i < chkpt.ntops; i++)
	{
	  chkpt_topone = &chkpt_topops[i];
	  tdes =
	    logtb_rv_find_allocate_tran_index (thread_p, chkpt_topone->trid,
					       log_lsa);
	  if (tdes == NULL)
	    {
	      if (area != NULL)
		{
		  free_and_init (area);
		}

	      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
				 "log_recovery_analysis");
	      return ER_FAILED;
	    }

	  if (tdes->topops.max == 0 ||
	      (tdes->topops.last + 1) >= tdes->topops.max)
	    {
	      if (logtb_realloc_topops_stack (tdes, chkpt.ntops) == NULL)
		{
		  if (area != NULL)
		    {
		      free_and_init (area);
		    }

		  logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
				     "log_recovery_analysis");
		  return ER_OUT_OF_VIRTUAL_MEMORY;
		}
	    }

	  tdes->topops.last++;
	  LSA_COPY (&tdes->topops.stack[tdes->topops.last].lastparent_lsa,
		    &chkpt_topone->lastparent_lsa);
	  LSA_COPY (&tdes->topops.stack[tdes->topops.last].posp_lsa,
		    &chkpt_topone->posp_lsa);
	}
    }

  if (LSA_LT (&chkpt.redo_lsa, start_redo_lsa))
    {
      LSA_COPY (start_redo_lsa, &chkpt.redo_lsa);
    }

  if (area != NULL)
    {
      free_and_init (area);
    }

  return NO_ERROR;
}

/*
 * log_rv_analysis_save_point -
 *
 * return: error code
 *
 *   tran_id(in):
 *   lsa(in/out):
 *
 * Note:
 */
static int
log_rv_analysis_save_point (THREAD_ENTRY * thread_p, int tran_id,
			    LOG_LSA * log_lsa)
{
  LOG_TDES *tdes;

  /*
   * If this is the first time, the transaction is seen. Assign a new
   * index to describe it and assume that the transaction was active
   * at the time of the crash, and thus it will be unilateraly
   * aborted. The truth of this statement will be find reading the
   * rest of the log
   */
  tdes = logtb_rv_find_allocate_tran_index (thread_p, tran_id, log_lsa);
  if (tdes == NULL)
    {
      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			 "log_rv_analysis_save_point");
      return ER_FAILED;
    }

  /* New tail, next to undo and savepoint */
  LSA_COPY (&tdes->last_lsa, log_lsa);
  LSA_COPY (&tdes->undo_nxlsa, &tdes->last_lsa);
  LSA_COPY (&tdes->savept_lsa, &tdes->last_lsa);

  return NO_ERROR;
}

/*
 * log_rv_analysis_log_end -
 *
 * return: error code
 *
 *   tran_id(in):
 *   lsa(in/out):
 *
 * Note:
 */
static int
log_rv_analysis_log_end (int tran_id, LOG_LSA * log_lsa)
{
  if (!logpb_is_page_in_archive (log_lsa->pageid))
    {
      /*
       * Reset the log header for the recovery undo operation
       */
      LOG_RESET_APPEND_LSA (log_lsa);
      log_Gl.hdr.next_trid = tran_id;
    }

  return NO_ERROR;
}

/*
 * log_rv_analysis_record -
 *
 * return: error code
 *
 *
 * Note:
 */
static void
log_rv_analysis_record (THREAD_ENTRY * thread_p, LOG_RECTYPE log_type,
			int tran_id, LOG_LSA * log_lsa, LOG_PAGE * log_page_p,
			LOG_LSA * checkpoint_lsa, LOG_LSA * prev_lsa,
			LOG_LSA * start_lsa, LOG_LSA * start_redo_lsa,
			bool is_media_crash, time_t * stopat,
			const LOG_LSA * stopat_lsa,
			bool * did_incom_recovery, bool * may_use_checkpoint)
{
  switch (log_type)
    {
    case LOG_UNDOREDO_DATA:
    case LOG_DIFF_UNDOREDO_DATA:
    case LOG_UNDO_DATA:
    case LOG_REDO_DATA:
    case LOG_DBEXTERN_REDO_DATA:
      (void) log_rv_analysis_undo_redo (thread_p, tran_id, log_lsa);
      break;

    case LOG_DUMMY_HEAD_POSTPONE:
      (void) log_rv_analysis_dummy_head_postpone (thread_p, tran_id, log_lsa);
      break;

    case LOG_POSTPONE:
      (void) log_rv_analysis_postpone (thread_p, tran_id, log_lsa);
      break;

    case LOG_RUN_POSTPONE:
      (void) log_rv_analysis_run_postpone (thread_p, tran_id, log_lsa,
					   log_page_p, checkpoint_lsa);
      break;

    case LOG_COMPENSATE:
      (void) log_rv_analysis_compensate (thread_p, tran_id, log_lsa,
					 log_page_p);
      break;

    case LOG_LCOMPENSATE:
      (void) log_rv_analysis_lcompensate (thread_p, tran_id, log_lsa,
					  log_page_p);
      break;

    case LOG_COMMIT_WITH_POSTPONE:
      (void) log_rv_analysis_commit_with_postpone (thread_p, tran_id, log_lsa,
						   log_page_p);
      break;

    case LOG_COMMIT_TOPOPE_WITH_POSTPONE:
      (void) log_rv_analysis_commit_topope_with_postpone (thread_p, tran_id,
							  log_lsa,
							  log_page_p);
      break;

    case LOG_COMMIT:
    case LOG_ABORT:
      (void) log_rv_analysis_complete (thread_p, tran_id, log_lsa, log_page_p,
				       prev_lsa, is_media_crash,
				       stopat, stopat_lsa,
				       did_incom_recovery);
      break;

    case LOG_COMMIT_TOPOPE:
    case LOG_ABORT_TOPOPE:
      log_rv_analysis_complte_topope (thread_p, tran_id, log_lsa);
      break;

    case LOG_START_CHKPT:
      log_rv_analysis_start_checkpoint (log_lsa, start_lsa,
					may_use_checkpoint);
      break;

    case LOG_END_CHKPT:
      log_rv_analysis_end_checkpoint (thread_p, log_lsa, log_page_p,
				      checkpoint_lsa, start_redo_lsa,
				      may_use_checkpoint);
      break;

    case LOG_SAVEPOINT:
      (void) log_rv_analysis_save_point (thread_p, tran_id, log_lsa);
      break;

    case LOG_END_OF_LOG:
      (void) log_rv_analysis_log_end (tran_id, log_lsa);
      break;

    case LOG_DUMMY_CRASH_RECOVERY:
    case LOG_DUMMY_FILLPAGE_FORARCHIVE:	/* for backward compatibility */
    case LOG_REPLICATION_DATA:
    case LOG_REPLICATION_SCHEMA:
    case LOG_DUMMY_HA_SERVER_STATE:
    case LOG_DUMMY_OVF_RECORD:
    case LOG_DUMMY_OVF_RECORD_DEL:
    case LOG_DUMMY_RECORD:
    case LOG_DUMMY_UPDATE_GID_BITMAP:
      break;

    case LOG_SMALLER_LOGREC_TYPE:
    case LOG_LARGER_LOGREC_TYPE:
    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOG_PAGE_CORRUPTED,
	      1, log_lsa->pageid);
      break;
    }
}

/*
 * log_recovery_analysis - FIND STATE OF TRANSACTIONS AT SYSTEM CRASH
 *
 * return: nothing
 *
 *   start_lsa(in): Starting address for the analysis phase
 *   start_redolsa(in/out): Starting address for redo phase
 *   end_redo_lsa(in):
 *   ismedia_crash(in): Are we recovering from a media crash ?
 *   stopat(in/out): Where to stop the recovery process.
 *                   (It may be set as a side-effect to the location of last
 *                    recovery transaction).
 *   stopat_lsa(in/out): derived from make_slave
 *   did_incom_recovery(in):
 *
 * NOTE: The recovery analysis phase scans the log forward since the
 *              last checkpoint record reflected in the log and the data
 *              volumes. The transaction table and the starting address for
 *              redo phase is created. When this phase is finished, we know
 *              the transactions that need to be unilateraly aborted (active)
 *              and the transactions that have to be completed due to postpone
 *              actions and client loose ends.
 */

static void
log_recovery_analysis (THREAD_ENTRY * thread_p, LOG_LSA * start_lsa,
		       LOG_LSA * start_redo_lsa, LOG_LSA * end_redo_lsa,
		       int is_media_crash, time_t * stopat,
		       const LOG_LSA * stopat_lsa, bool * did_incom_recovery,
		       INT64 * num_redo_log_records)
{
  LOG_LSA checkpoint_lsa = { -1, -1 };
  LOG_LSA lsa;			/* LSA of log record to analyse */
  char log_pgbuf[IO_MAX_PAGE_SIZE + MAX_ALIGNMENT], *aligned_log_pgbuf;
  LOG_PAGE *log_page_p = NULL;	/* Log page pointer where LSA
				 * is located
				 */
  LOG_LSA log_lsa, prev_lsa;
  LOG_RECTYPE log_rtype;	/* Log record type            */
  LOG_RECORD_HEADER *log_rec = NULL;	/* Pointer to log record      */
  time_t last_at_time = -1;
#if !defined(NDEBUG)
  char time_val[CTIME_MAX];
#endif /* !NDEBUG */
  bool may_use_checkpoint = false;
  TRANID tran_id;

  aligned_log_pgbuf = PTR_ALIGN (log_pgbuf, MAX_ALIGNMENT);

  if (num_redo_log_records != NULL)
    {
      *num_redo_log_records = 0;
    }

  /*
   * Find the committed, aborted, and unilaterally aborted (active)
   * transactions at system crash
   */

  LSA_COPY (&lsa, start_lsa);

  LSA_COPY (start_redo_lsa, &lsa);
  LSA_COPY (end_redo_lsa, &lsa);
  LSA_COPY (&prev_lsa, &lsa);
  *did_incom_recovery = false;

  log_page_p = (LOG_PAGE *) aligned_log_pgbuf;

  while (!LSA_ISNULL (&lsa))
    {
      /* Fetch the page where the LSA record to undo is located */
      log_lsa.pageid = lsa.pageid;
      if (logpb_fetch_page (thread_p, log_lsa.pageid, log_page_p) == NULL)
	{
	  if (is_media_crash == true)
	    {
	      if (stopat != NULL)
		{
		  *stopat = last_at_time;
		}

#if !defined(NDEBUG)
	      if (prm_get_bool_value (PRM_ID_LOG_TRACE_DEBUG))
		{
		  fprintf (stdout,
			   msgcat_message (MSGCAT_CATALOG_RYE,
					   MSGCAT_SET_LOG,
					   MSGCAT_LOG_STARTS));
		  (void) ctime_r (&last_at_time, time_val);
		  fprintf (stdout,
			   msgcat_message (MSGCAT_CATALOG_RYE,
					   MSGCAT_SET_LOG,
					   MSGCAT_LOG_INCOMPLTE_MEDIA_RECOVERY),
			   end_redo_lsa->pageid, end_redo_lsa->offset,
			   ((last_at_time == -1) ? "???...\n" : time_val));
		  fprintf (stdout,
			   msgcat_message (MSGCAT_CATALOG_RYE,
					   MSGCAT_SET_LOG,
					   MSGCAT_LOG_STARTS));
		  fflush (stdout);
		}
#endif /* !NDEBUG */
	      /* if previous log record exists,
	       * reset tdes->tail_lsa/undo_nxlsa as previous of end_redo_lsa */
	      if (log_rec != NULL)
		{
		  LOG_TDES *last_log_tdes;

		  last_log_tdes = LOG_FIND_TDES (logtb_find_tran_index
						 (thread_p, log_rec->trid));
		  if (last_log_tdes != NULL)
		    {
		      LSA_COPY (&last_log_tdes->last_lsa,
				&log_rec->prev_tranlsa);
		      LSA_COPY (&last_log_tdes->undo_nxlsa,
				&log_rec->prev_tranlsa);
		    }
		}
	      log_recovery_resetlog (thread_p, &lsa, true, end_redo_lsa);
	      *did_incom_recovery = true;

	      if (stopat_lsa != NULL)
		{
		  assert (!LSA_ISNULL (stopat_lsa));
		  assert (false);
		  logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
				     "log_recovery_analysis");
		}

	    }
	  else
	    {
	      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
				 "log_recovery_analysis");
	    }

	  return;
	}

      /* Check all log records in this phase */
      while (!LSA_ISNULL (&lsa) && lsa.pageid == log_lsa.pageid)
	{
	  /*
	   * If an offset is missing, it is because an incomplete log record was
	   * archived. This log_record was completed later. Thus, we have to
	   * find the offset by searching for the next log_record in the page
	   */
	  if (lsa.offset == NULL_OFFSET)
	    {
	      lsa.offset = log_page_p->hdr.offset;
	      if (lsa.offset == NULL_OFFSET)
		{
		  /* Continue with next pageid */
		  if (logpb_is_page_in_archive (log_lsa.pageid))
		    {
		      lsa.pageid = log_lsa.pageid + 1;
		    }
		  else
		    {
		      lsa.pageid = NULL_PAGEID;
		    }
		  continue;
		}
	    }

	  /* Find the log record */
	  log_lsa.offset = lsa.offset;
	  log_rec = LOG_GET_LOG_RECORD_HEADER (log_page_p, &log_lsa);

	  tran_id = log_rec->trid;
	  log_rtype = log_rec->type;

	  /*
	   * Save the address of last redo log record.
	   * Get the address of next log record to scan
	   */

	  LSA_COPY (end_redo_lsa, &lsa);
	  LSA_COPY (&lsa, &log_rec->forw_lsa);

	  /*
	   * If the next page is NULL_PAGEID and the current page is an archive
	   * page, this is not the end of the log. This situation happens when an
	   * incomplete log record is archived. Thus, its forward address is NULL.
	   * Note that we have to set lsa.pageid here since the log_lsa.pageid value
	   * can be changed (e.g., the log record is stored in two pages: an
	   * archive page, and an active page. Later, we try to modify it whenever
	   * is possible.
	   */

	  if (LSA_ISNULL (&lsa) && logpb_is_page_in_archive (log_lsa.pageid))
	    {
	      lsa.pageid = log_lsa.pageid + 1;
	    }

	  if (!LSA_ISNULL (&lsa) && log_lsa.pageid != NULL_PAGEID
	      && (lsa.pageid < log_lsa.pageid
		  || (lsa.pageid == log_lsa.pageid
		      && lsa.offset <= log_lsa.offset)))
	    {
	      /* It seems to be a system error. Maybe a loop in the log */
	      er_log_debug (ARG_FILE_LINE,
			    "log_recovery_analysis: ** System error:"
			    " It seems to be a loop in the log\n."
			    " Current log_rec at %lld|%d. Next log_rec at %lld|%d\n",
			    (long long int) log_lsa.pageid, log_lsa.offset,
			    (long long int) lsa.pageid, lsa.offset);
	      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
				 "log_recovery_analysis");
	      LSA_SET_NULL (&lsa);
	      break;
	    }

	  if (LSA_ISNULL (&lsa) && log_rtype != LOG_END_OF_LOG
	      && *did_incom_recovery == false)
	    {
	      LOG_RESET_APPEND_LSA (end_redo_lsa);
	      if (log_startof_nxrec (thread_p, &log_Gl.hdr.append_lsa, true)
		  == NULL)
		{
		  /* We may destroy a record */
		  LOG_RESET_APPEND_LSA (end_redo_lsa);
		}
	      else
		{
		  LOG_RESET_APPEND_LSA (&log_Gl.hdr.append_lsa);

		  /*
		   * Reset the forward address of current record to next record,
		   * and then flush the page.
		   */
		  LSA_COPY (&log_rec->forw_lsa, &log_Gl.hdr.append_lsa);

		  assert (log_lsa.pageid == log_page_p->hdr.logical_pageid);
		  logpb_write_page_to_disk (thread_p, log_page_p,
					    log_lsa.pageid);
		}
	      er_log_debug (ARG_FILE_LINE,
			    "log_recovery_analysis: ** WARNING:"
			    " An end of the log record was not found."
			    " Will Assume = %lld|%d and Next Trid = %d\n",
			    (long long int) log_Gl.hdr.append_lsa.pageid,
			    log_Gl.hdr.append_lsa.offset, tran_id);
	      log_Gl.hdr.next_trid = tran_id;
	    }

	  if (num_redo_log_records)
	    {
	      switch (log_rtype)
		{
		  /* count redo log */
		case LOG_UNDOREDO_DATA:
		case LOG_DIFF_UNDOREDO_DATA:
		case LOG_DBEXTERN_REDO_DATA:
		case LOG_RUN_POSTPONE:
		case LOG_COMPENSATE:
		  (*num_redo_log_records)++;
		  break;
		default:
		  break;
		}
	    }

	  log_rv_analysis_record (thread_p, log_rtype, tran_id, &log_lsa,
				  log_page_p, &checkpoint_lsa, &prev_lsa,
				  start_lsa, start_redo_lsa,
				  is_media_crash, stopat, stopat_lsa,
				  did_incom_recovery, &may_use_checkpoint);
	  if (*did_incom_recovery == true)
	    {
	      LSA_SET_NULL (&lsa);
	      break;
	    }
	  if (LSA_EQ (end_redo_lsa, &lsa))
	    {
	      assert_release (!LSA_EQ (end_redo_lsa, &lsa));
	      LSA_SET_NULL (&lsa);
	      break;
	    }

	  LSA_COPY (&prev_lsa, end_redo_lsa);

	  /*
	   * We can fix the lsa.pageid in the case of log_records without forward
	   * address at this moment.
	   */
	  if (lsa.offset == NULL_OFFSET && lsa.pageid != NULL_PAGEID
	      && lsa.pageid < log_lsa.pageid)
	    {
	      lsa.pageid = log_lsa.pageid;
	    }
	}
    }

  return;
}

static PAGE_TYPE
rv_rcvindex_page_type (LOG_RCVINDEX rcvindex)
{
  switch (rcvindex)
    {
    case RVBT_GET_NEWROOT:
      return PAGE_BTREE_ROOT;
    case RVBT_GET_NEWPAGE:
      return PAGE_BTREE;
    case RVCT_NEWPAGE:
      return PAGE_CATALOG;
    case RVDK_FORMAT:
      return PAGE_VOLHEADER;
    case RVDK_INITMAP:
      return PAGE_VOLBITMAP;
    case RVEH_INIT_DIR:
    case RVEH_INIT_NEW_DIR_PAGE:
    case RVEH_INIT_BUCKET:
      return PAGE_EHASH;
    case RVFL_FHDR:
      return PAGE_FILE_HEADER;
    case RVFL_FTAB_CHAIN:
      return PAGE_FILE_TAB;
    case RVHF_NEWHDR:
      return PAGE_HEAP_HEADER;
    case RVHF_NEWPAGE:
      return PAGE_HEAP;
    case RVOVF_NEWPAGE_INSERT:
    case RVOVF_PAGE_UPDATE:
      return PAGE_OVERFLOW;

    default:
      break;
    }

  assert (false);

  return PAGE_UNKNOWN;
}

/*
 * log_recovery_redo - SCAN FORWARD REDOING DATA
 *
 * return: nothing
 *
 *   start_redolsa(in): Starting address for recovery redo phase
 *   end_redo_lsa(in):
 *
 * NOTE:In the redo phase, updates that are not reflected in the
 *              database are repeated for not only the committed transaction
 *              but also for all aborted transactions and the transactions
 *              that were in progress at the time of the failure. This phase
 *              reestablishes the state of the database as of the time of the
 *              failure. The redo phase starts by scanning the log records
 *              from the redo LSA address determined in the analysis phase.
 *              When a redoable record is found, a check is done to find out
 *              if the redo action is already reflected in the page. If it is
 *              not, then the redo actions are executed. A redo action can be
 *              skipped if the LSA of the affected page is greater or equal
 *              than that of the LSA of the log record. Any postpone actions
 *              (after commit actions) of committed transactions that have not
 *              been executed are done. Loose_ends of client actions that have
 *              not been done are postponed until the client is restarted.
 *              At the end of the recovery phase, all data dirty pages are
 *              flushed.
 *              The redo of aborted transactions are undone executing its
 *              respective compensating log records.
 */
static void
log_recovery_redo (THREAD_ENTRY * thread_p, const LOG_LSA * start_redolsa,
		   const LOG_LSA * end_redo_lsa)
{
  LOG_LSA lsa;			/* LSA of log record to redo  */
  char log_pgbuf[IO_MAX_PAGE_SIZE + MAX_ALIGNMENT], *aligned_log_pgbuf;
  LOG_PAGE *log_pgptr = NULL;	/* Log page pointer where LSA
				 * is located
				 */
  LOG_LSA log_lsa;
  LOG_RECORD_HEADER *log_rec;	/* Pointer to log record        */
  struct log_undoredo *undoredo;	/* Undo_redo log record         */
  struct log_redo *redo;	/* Redo log record              */
  struct log_dbout_redo *dbout_redo;	/* A external redo log record   */
  struct log_compensate *compensate;	/* Compensating log record      */
  struct log_run_postpone *run_posp;	/* A run postpone action        */
  LOG_RCV rcv;			/* Recovery structure           */
  VPID rcv_vpid;		/* VPID of data to recover      */
  LOG_RCVINDEX rcvindex;	/* Recovery index function      */
  LOG_LSA rcv_lsa;		/* Address of redo log record   */
  LOG_LSA *rcv_page_lsaptr;	/* LSA of data page for log
				 * record to redo
				 */
  int temp_length;
  int tran_index;
  LOG_ZIP *undo_unzip_ptr = NULL;
  LOG_ZIP *redo_unzip_ptr = NULL;
  bool is_diff_rec;
  PAGE_TYPE ptype;

  aligned_log_pgbuf = PTR_ALIGN (log_pgbuf, MAX_ALIGNMENT);

  /*
   * GO FORWARD, redoing records of all transactions including aborted ones.
   *
   * Compensating records undo the redo of already executed undo records.
   * Transactions that were active at the time of the crash are aborted
   * during the log_recovery_undo phase
   */

  LSA_COPY (&lsa, start_redolsa);

  /* Defense for illegal start_redolsa */
  if ((lsa.offset + (int) sizeof (LOG_RECORD_HEADER)) >= LOGAREA_SIZE)
    {
      assert (false);
      /* move first record of next page */
      lsa.pageid++;
      lsa.offset = NULL_OFFSET;
    }

  log_pgptr = (LOG_PAGE *) aligned_log_pgbuf;

  undo_unzip_ptr = log_zip_alloc (LOGAREA_SIZE, false);
  redo_unzip_ptr = log_zip_alloc (LOGAREA_SIZE, false);

  if (undo_unzip_ptr == NULL || redo_unzip_ptr == NULL)
    {
      if (undo_unzip_ptr)
	{
	  log_zip_free (undo_unzip_ptr);
	}
      if (redo_unzip_ptr)
	{
	  log_zip_free (redo_unzip_ptr);
	}
      logpb_fatal_error (thread_p, true, ARG_FILE_LINE, "log_recovery_redo");
      return;
    }

  while (!LSA_ISNULL (&lsa))
    {
      /* Fetch the page where the LSA record to undo is located */
      log_lsa.pageid = lsa.pageid;
      if (logpb_fetch_page (thread_p, log_lsa.pageid, log_pgptr) == NULL)
	{
	  if (end_redo_lsa != NULL
	      && (LSA_ISNULL (end_redo_lsa) || LSA_GE (&lsa, end_redo_lsa)))
	    {
	      log_zip_free (undo_unzip_ptr);
	      log_zip_free (redo_unzip_ptr);
	      return;
	    }
	  else
	    {
	      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
				 "log_recovery_redo");
	      log_zip_free (undo_unzip_ptr);
	      log_zip_free (redo_unzip_ptr);
	      return;
	    }
	}

      /* Check all log records in this phase */
      while (lsa.pageid == log_lsa.pageid)
	{
	  /*
	   * Do we want to stop the recovery redo process at this time ?
	   */
	  if (end_redo_lsa != NULL && !LSA_ISNULL (end_redo_lsa)
	      && LSA_GE (&lsa, end_redo_lsa))
	    {
	      LSA_SET_NULL (&lsa);
	      break;
	    }

	  /*
	   * If an offset is missing, it is because we archive an incomplete
	   * log record. This log_record was completed later. Thus, we have to
	   * find the offset by searching for the next log_record in the page
	   */
	  if (lsa.offset == NULL_OFFSET
	      && (lsa.offset = log_pgptr->hdr.offset) == NULL_OFFSET)
	    {
	      /* Continue with next pageid */
	      if (logpb_is_page_in_archive (log_lsa.pageid))
		{
		  lsa.pageid = log_lsa.pageid + 1;
		}
	      else
		{
		  lsa.pageid = NULL_PAGEID;
		}
	      continue;
	    }

	  /* Find the log record */
	  log_lsa.offset = lsa.offset;
	  log_rec = LOG_GET_LOG_RECORD_HEADER (log_pgptr, &log_lsa);

	  /* Get the address of next log record to scan */
	  LSA_COPY (&lsa, &log_rec->forw_lsa);

	  /*
	   * If the next page is NULL_PAGEID and the current page is an archive
	   * page, this is not the end, this situation happens when an incomplete
	   * log record is archived. Thus, its forward address is NULL.
	   * Note that we have to set lsa.pageid here since the log_lsa.pageid value
	   * can be changed (e.g., the log record is stored in an archive page and
	   * in an active page. Later, we try to modify it whenever is possible.
	   */

	  if (LSA_ISNULL (&lsa) && logpb_is_page_in_archive (log_lsa.pageid))
	    {
	      lsa.pageid = log_lsa.pageid + 1;
	    }

	  if (!LSA_ISNULL (&lsa) && log_lsa.pageid != NULL_PAGEID
	      && (lsa.pageid < log_lsa.pageid
		  || (lsa.pageid == log_lsa.pageid
		      && lsa.offset <= log_lsa.offset)))
	    {
	      /* It seems to be a system error. Maybe a loop in the log */
	      er_log_debug (ARG_FILE_LINE,
			    "log_recovery_redo: ** System error:"
			    " It seems to be a loop in the log\n."
			    " Current log_rec at %lld|%d. Next log_rec at %lld|%d\n",
			    (long long int) log_lsa.pageid, log_lsa.offset,
			    (long long int) lsa.pageid, lsa.offset);
	      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
				 "log_recovery_redo");
	      LSA_SET_NULL (&lsa);
	      break;
	    }

	  switch (log_rec->type)
	    {
	    case LOG_UNDOREDO_DATA:
	    case LOG_DIFF_UNDOREDO_DATA:
	      if (log_rec->type == LOG_DIFF_UNDOREDO_DATA)
		{
		  is_diff_rec = true;
		}
	      else
		{
		  is_diff_rec = false;
		}

	      /* REDO the record if needed */
	      LSA_COPY (&rcv_lsa, &log_lsa);

	      /* Get the DATA HEADER */
	      LOG_READ_ADD_ALIGN (thread_p, sizeof (LOG_RECORD_HEADER),
				  &log_lsa, log_pgptr);
	      LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p,
						sizeof (struct log_undoredo),
						&log_lsa, log_pgptr);
	      undoredo =
		(struct log_undoredo *) ((char *) log_pgptr->area +
					 log_lsa.offset);

	      /* Do we need to redo anything ? */

	      /*
	       * Fetch the page for physical log records and check if redo
	       * is needed by comparing the log sequence numbers
	       */

	      rcv_vpid.volid = undoredo->data.volid;
	      rcv_vpid.pageid = undoredo->data.pageid;

	      rcv.pgptr = NULL;
	      /* If the page does not exit, there is nothing to redo */
	      if (rcv_vpid.pageid != NULL_PAGEID
		  && rcv_vpid.volid != NULL_VOLID)
		{
		  if (disk_isvalid_page (thread_p, rcv_vpid.volid,
					 rcv_vpid.pageid) != DISK_VALID)
		    {
		      break;
		    }

		  if (RCV_IS_NEWPG_LOG (undoredo->data.rcvindex))
		    {
		      ptype = rv_rcvindex_page_type (undoredo->data.rcvindex);
		    }
		  else
		    {
		      ptype = PAGE_UNKNOWN;
		    }

		  rcv.pgptr = pgbuf_fix (thread_p, &rcv_vpid, OLD_PAGE,
					 PGBUF_LATCH_WRITE,
					 PGBUF_UNCONDITIONAL_LATCH, ptype);
		  if (rcv.pgptr == NULL)
		    {
		      break;
		    }

		  if (RCV_IS_NEWPG_LOG (undoredo->data.rcvindex))
		    {
		      assert (ptype != PAGE_UNKNOWN);
		      (void) pgbuf_set_page_ptype (thread_p, rcv.pgptr, ptype);	/* reset */
		    }
		}

	      if (rcv.pgptr != NULL)
		{
		  rcv_page_lsaptr = pgbuf_get_lsa (rcv.pgptr);
		  /*
		   * Do we need to execute the redo operation ?
		   * If page_lsa >= lsa... already updated. In this case make sure
		   * that the redo is not far from the end_redo_lsa
		   */
		  if (LSA_LE (&rcv_lsa, rcv_page_lsaptr)
		      && (end_redo_lsa == NULL || LSA_ISNULL (end_redo_lsa)
			  || LSA_LE (rcv_page_lsaptr, end_redo_lsa)))
		    {
		      /* It is already done */
		      pgbuf_unfix (thread_p, rcv.pgptr);
		      break;
		    }
		}
	      else
		{
		  rcv_page_lsaptr = NULL;
		}

	      temp_length = undoredo->ulength;

	      rcvindex = undoredo->data.rcvindex;
	      rcv.length = undoredo->rlength;
	      rcv.offset = undoredo->data.offset;
	      assert (rcvindex < RCV_INDEX_END);


	      LOG_READ_ADD_ALIGN (thread_p, sizeof (struct log_undoredo),
				  &log_lsa, log_pgptr);

	      if (is_diff_rec)
		{
		  /* Get Undo data */
		  if (!log_rv_get_unzip_log_data
		      (thread_p, temp_length, &log_lsa, log_pgptr,
		       undo_unzip_ptr))
		    {
		      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
					 "log_recovery_redo");
		    }
		}
	      else
		{
		  temp_length = (int) GET_ZIP_LEN (temp_length);
		  /* Undo Data Pass */
		  if (log_lsa.offset + temp_length < (int) LOGAREA_SIZE)
		    {
		      log_lsa.offset += temp_length;
		    }
		  else
		    {
		      while (temp_length > 0)
			{
			  if (temp_length + log_lsa.offset >=
			      (int) LOGAREA_SIZE)
			    {
			      temp_length -=
				LOGAREA_SIZE - (int) (log_lsa.offset);
			      assert (log_pgptr != NULL);
			      if ((logpb_fetch_page
				   (thread_p, ++log_lsa.pageid,
				    log_pgptr)) == NULL)
				{
				  logpb_fatal_error (thread_p, true,
						     ARG_FILE_LINE,
						     "log_recovery_redo");
				  log_zip_free (undo_unzip_ptr);
				  log_zip_free (redo_unzip_ptr);
				  return;
				}
			      log_lsa.offset = 0;
			      LOG_READ_ALIGN (thread_p, &log_lsa, log_pgptr);
			    }
			  else
			    {
			      log_lsa.offset += temp_length;
			      temp_length = 0;
			    }
			}
		    }
		}

	      /* GET AFTER DATA */
	      LOG_READ_ALIGN (thread_p, &log_lsa, log_pgptr);
#if !defined(NDEBUG)
	      if (prm_get_bool_value (PRM_ID_LOG_TRACE_DEBUG))
		{
		  fprintf (stdout,
			   "TRACE REDOING[1]: LSA = %lld|%d, Rv_index = %s,\n"
			   "      volid = %d, pageid = %d, offset = %d,\n",
			   (long long int) rcv_lsa.pageid, rcv_lsa.offset,
			   rv_rcvindex_string (rcvindex), rcv_vpid.volid,
			   rcv_vpid.pageid, rcv.offset);
		  if (rcv_page_lsaptr != NULL)
		    {
		      fprintf (stdout, "      page_lsa = %lld|%d\n",
			       (long long int) rcv_page_lsaptr->pageid,
			       rcv_page_lsaptr->offset);
		    }
		  else
		    {
		      fprintf (stdout, "      page_lsa = %d|%d\n", -1, -1);
		    }
		  fflush (stdout);
		}
#endif /* !NDEBUG */

	      if (is_diff_rec)
		{
		  /* XOR Process */
		  log_rv_redo_record (thread_p, &log_lsa,
				      log_pgptr, RV_fun[rcvindex].redofun,
				      &rcv, &rcv_lsa,
				      (int) undo_unzip_ptr->data_length,
				      (char *) undo_unzip_ptr->log_data,
				      redo_unzip_ptr);
		}
	      else
		{
		  log_rv_redo_record (thread_p, &log_lsa,
				      log_pgptr, RV_fun[rcvindex].redofun,
				      &rcv, &rcv_lsa, 0, NULL,
				      redo_unzip_ptr);
		}
	      if (rcv.pgptr != NULL)
		{
		  pgbuf_unfix (thread_p, rcv.pgptr);
		}
	      break;

	    case LOG_REDO_DATA:
	      LSA_COPY (&rcv_lsa, &log_lsa);

	      /* Get the DATA HEADER */
	      LOG_READ_ADD_ALIGN (thread_p, sizeof (LOG_RECORD_HEADER),
				  &log_lsa, log_pgptr);
	      LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p,
						sizeof (struct log_redo),
						&log_lsa, log_pgptr);

	      redo =
		(struct log_redo *) ((char *) log_pgptr->area +
				     log_lsa.offset);

	      /* Do we need to redo anything ? */

	      /*
	       * Fetch the page for physical log records and check if redo
	       * is needed by comparing the log sequence numbers
	       */

	      rcv_vpid.volid = redo->data.volid;
	      rcv_vpid.pageid = redo->data.pageid;

	      rcv.pgptr = NULL;
	      /* If the page does not exit, there is nothing to redo */
	      if (rcv_vpid.pageid != NULL_PAGEID
		  && rcv_vpid.volid != NULL_VOLID)
		{
		  if (disk_isvalid_page (thread_p, rcv_vpid.volid,
					 rcv_vpid.pageid) != DISK_VALID)
		    {
		      break;
		    }

		  if (RCV_IS_NEWPG_LOG (redo->data.rcvindex))
		    {
		      ptype = rv_rcvindex_page_type (redo->data.rcvindex);
		    }
		  else
		    {
		      ptype = PAGE_UNKNOWN;
		    }

		  rcv.pgptr = pgbuf_fix (thread_p, &rcv_vpid, OLD_PAGE,
					 PGBUF_LATCH_WRITE,
					 PGBUF_UNCONDITIONAL_LATCH, ptype);
		  if (rcv.pgptr == NULL)
		    {
		      break;
		    }

		  if (RCV_IS_NEWPG_LOG (redo->data.rcvindex))
		    {
		      assert (ptype != PAGE_UNKNOWN);
		      (void) pgbuf_set_page_ptype (thread_p, rcv.pgptr, ptype);	/* reset */
		    }

		}

	      if (rcv.pgptr != NULL)
		{
		  rcv_page_lsaptr = pgbuf_get_lsa (rcv.pgptr);
		  /*
		   * Do we need to execute the redo operation ?
		   * If page_lsa >= rcv_lsa... already updated
		   */
		  if (LSA_LE (&rcv_lsa, rcv_page_lsaptr)
		      && (end_redo_lsa == NULL || LSA_ISNULL (end_redo_lsa)
			  || LSA_LE (rcv_page_lsaptr, end_redo_lsa)))
		    {
		      /* It is already done */
		      pgbuf_unfix (thread_p, rcv.pgptr);
		      break;
		    }
		}
	      else
		{
		  rcv.pgptr = NULL;
		  rcv_page_lsaptr = NULL;
		}

	      rcvindex = redo->data.rcvindex;
	      rcv.length = redo->length;
	      rcv.offset = redo->data.offset;
	      assert (rcvindex < RCV_INDEX_END);


	      /* GET AFTER DATA */
	      LOG_READ_ADD_ALIGN (thread_p, sizeof (struct log_redo),
				  &log_lsa, log_pgptr);

#if !defined(NDEBUG)
	      if (prm_get_bool_value (PRM_ID_LOG_TRACE_DEBUG))
		{
		  fprintf (stdout,
			   "TRACE REDOING[2]: LSA = %lld|%d, Rv_index = %s,\n"
			   "      volid = %d, pageid = %d, offset = %d,\n",
			   (long long int) rcv_lsa.pageid, rcv_lsa.offset,
			   rv_rcvindex_string (rcvindex), rcv_vpid.volid,
			   rcv_vpid.pageid, rcv.offset);
		  if (rcv_page_lsaptr != NULL)
		    {
		      fprintf (stdout, "      page_lsa = %lld|%d\n",
			       (long long int) rcv_page_lsaptr->pageid,
			       rcv_page_lsaptr->offset);
		    }
		  else
		    {
		      fprintf (stdout, "      page_lsa = %lld|%d\n", -1LL,
			       -1);
		    }
		  fflush (stdout);
		}
#endif /* !NDEBUG */

	      log_rv_redo_record (thread_p, &log_lsa,
				  log_pgptr, RV_fun[rcvindex].redofun, &rcv,
				  &rcv_lsa, 0, NULL, redo_unzip_ptr);

	      if (rcv.pgptr != NULL)
		{
		  pgbuf_unfix (thread_p, rcv.pgptr);
		}
	      break;

	    case LOG_DBEXTERN_REDO_DATA:
	      LSA_COPY (&rcv_lsa, &log_lsa);

	      /* Get the DATA HEADER */
	      LOG_READ_ADD_ALIGN (thread_p, sizeof (LOG_RECORD_HEADER),
				  &log_lsa, log_pgptr);
	      LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p,
						sizeof (struct
							log_dbout_redo),
						&log_lsa, log_pgptr);

	      dbout_redo = ((struct log_dbout_redo *)
			    ((char *) log_pgptr->area + log_lsa.offset));

	      VPID_SET_NULL (&rcv_vpid);
	      rcv.offset = -1;
	      rcv.pgptr = NULL;

	      rcvindex = dbout_redo->rcvindex;
	      rcv.length = dbout_redo->length;
	      assert (rcvindex < RCV_INDEX_END);

	      /* GET AFTER DATA */
	      LOG_READ_ADD_ALIGN (thread_p, sizeof (struct log_dbout_redo),
				  &log_lsa, log_pgptr);

#if !defined(NDEBUG)
	      if (prm_get_bool_value (PRM_ID_LOG_TRACE_DEBUG))
		{
		  fprintf (stdout,
			   "TRACE EXT REDOING[3]: LSA = %lld|%d, Rv_index = %s\n",
			   (long long int) rcv_lsa.pageid, rcv_lsa.offset,
			   rv_rcvindex_string (rcvindex));
		  fflush (stdout);
		}
#endif /* !NDEBUG */

	      log_rv_redo_record (thread_p, &log_lsa,
				  log_pgptr, RV_fun[rcvindex].redofun, &rcv,
				  &rcv_lsa, 0, NULL, NULL);
	      break;

	    case LOG_RUN_POSTPONE:
	      LSA_COPY (&rcv_lsa, &log_lsa);

	      /* Get the DATA HEADER */
	      LOG_READ_ADD_ALIGN (thread_p, sizeof (LOG_RECORD_HEADER),
				  &log_lsa, log_pgptr);
	      LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p,
						sizeof (struct
							log_run_postpone),
						&log_lsa, log_pgptr);
	      run_posp =
		(struct log_run_postpone *) ((char *) log_pgptr->area +
					     log_lsa.offset);

	      /* Do we need to redo anything ? */

	      /*
	       * Fetch the page for physical log records and check if redo
	       * is needed by comparing the log sequence numbers
	       */

	      rcv_vpid.volid = run_posp->data.volid;
	      rcv_vpid.pageid = run_posp->data.pageid;

	      rcv.pgptr = NULL;
	      /* If the page does not exit, there is nothing to redo */
	      if (rcv_vpid.pageid != NULL_PAGEID
		  && rcv_vpid.volid != NULL_VOLID)
		{
		  if (disk_isvalid_page (thread_p, rcv_vpid.volid,
					 rcv_vpid.pageid) != DISK_VALID)
		    {
		      break;
		    }
		  rcv.pgptr = pgbuf_fix (thread_p, &rcv_vpid, OLD_PAGE,
					 PGBUF_LATCH_WRITE,
					 PGBUF_UNCONDITIONAL_LATCH,
					 PAGE_UNKNOWN);
		  if (rcv.pgptr == NULL)
		    {
		      break;
		    }
		}

	      if (rcv.pgptr != NULL)
		{
		  rcv_page_lsaptr = pgbuf_get_lsa (rcv.pgptr);
		  /*
		   * Do we need to execute the redo operation ?
		   * If page_lsa >= rcv_lsa... already updated
		   */
		  if (LSA_LE (&rcv_lsa, rcv_page_lsaptr)
		      && (end_redo_lsa == NULL || LSA_ISNULL (end_redo_lsa)
			  || LSA_LE (rcv_page_lsaptr, end_redo_lsa)))
		    {
		      /* It is already done */
		      pgbuf_unfix (thread_p, rcv.pgptr);
		      break;
		    }
		}
	      else
		{
		  rcv.pgptr = NULL;
		  rcv_page_lsaptr = NULL;
		}

	      rcvindex = run_posp->data.rcvindex;
	      rcv.length = run_posp->length;
	      rcv.offset = run_posp->data.offset;
	      assert (rcvindex < RCV_INDEX_END);

	      LOG_READ_ADD_ALIGN (thread_p, sizeof (struct log_run_postpone),
				  &log_lsa, log_pgptr);
	      /* GET AFTER DATA */
	      LOG_READ_ALIGN (thread_p, &log_lsa, log_pgptr);

#if !defined(NDEBUG)
	      if (prm_get_bool_value (PRM_ID_LOG_TRACE_DEBUG))
		{
		  fprintf (stdout,
			   "TRACE REDOING[4]: LSA = %lld|%d, Rv_index = %s,\n"
			   "      volid = %d, pageid = %d, offset = %d,\n",
			   (long long int) rcv_lsa.pageid, rcv_lsa.offset,
			   rv_rcvindex_string (rcvindex), rcv_vpid.volid,
			   rcv_vpid.pageid, rcv.offset);
		  if (rcv_page_lsaptr != NULL)
		    {
		      fprintf (stdout, "      page_lsa = %lld|%d\n",
			       (long long int) rcv_page_lsaptr->pageid,
			       rcv_page_lsaptr->offset);
		    }
		  else
		    {
		      fprintf (stdout, "      page_lsa = %lld|%d\n", -1LL,
			       -1);
		    }
		  fflush (stdout);
		}
#endif /* !NDEBUG */

	      log_rv_redo_record (thread_p, &log_lsa,
				  log_pgptr, RV_fun[rcvindex].redofun, &rcv,
				  &rcv_lsa, 0, NULL, NULL);

	      if (rcv.pgptr != NULL)
		{
		  pgbuf_unfix (thread_p, rcv.pgptr);
		}
	      break;

	    case LOG_COMPENSATE:
	      LSA_COPY (&rcv_lsa, &log_lsa);

	      /* Get the DATA HEADER */
	      LOG_READ_ADD_ALIGN (thread_p, sizeof (LOG_RECORD_HEADER),
				  &log_lsa, log_pgptr);
	      LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p,
						sizeof (struct
							log_compensate),
						&log_lsa, log_pgptr);
	      compensate =
		(struct log_compensate *) ((char *) log_pgptr->area +
					   log_lsa.offset);
	      /* Do we need to redo anything ? */

	      /*
	       * Fetch the page for physical log records and check if redo
	       * is needed by comparing the log sequence numbers
	       */

	      rcv_vpid.volid = compensate->data.volid;
	      rcv_vpid.pageid = compensate->data.pageid;

	      rcv.pgptr = NULL;
	      /* If the page does not exit, there is nothing to redo */
	      if (rcv_vpid.pageid != NULL_PAGEID
		  && rcv_vpid.volid != NULL_VOLID)
		{
		  if (disk_isvalid_page
		      (thread_p, rcv_vpid.volid,
		       rcv_vpid.pageid) != DISK_VALID)
		    {
		      break;
		    }
		  rcv.pgptr = pgbuf_fix (thread_p, &rcv_vpid, OLD_PAGE,
					 PGBUF_LATCH_WRITE,
					 PGBUF_UNCONDITIONAL_LATCH,
					 PAGE_UNKNOWN);
		  if (rcv.pgptr == NULL)
		    {
		      break;
		    }
		}

	      if (rcv.pgptr != NULL)
		{
		  rcv_page_lsaptr = pgbuf_get_lsa (rcv.pgptr);
		  /*
		   * Do we need to execute the redo operation ?
		   * If page_lsa >= rcv_lsa... already updated
		   */
		  if (LSA_LE (&rcv_lsa, rcv_page_lsaptr)
		      && (end_redo_lsa == NULL || LSA_ISNULL (end_redo_lsa)
			  || LSA_LE (rcv_page_lsaptr, end_redo_lsa)))
		    {
		      /* It is already done */
		      pgbuf_unfix (thread_p, rcv.pgptr);
		      break;
		    }
		}
	      else
		{
		  rcv.pgptr = NULL;
		  rcv_page_lsaptr = NULL;
		}

	      rcvindex = compensate->data.rcvindex;
	      rcv.length = compensate->length;
	      rcv.offset = compensate->data.offset;
	      assert (rcvindex < RCV_INDEX_END);

	      LOG_READ_ADD_ALIGN (thread_p, sizeof (struct log_compensate),
				  &log_lsa, log_pgptr);
	      /* GET COMPENSATING DATA */
	      LOG_READ_ALIGN (thread_p, &log_lsa, log_pgptr);
#if !defined(NDEBUG)
	      if (prm_get_bool_value (PRM_ID_LOG_TRACE_DEBUG))
		{
		  fprintf (stdout,
			   "TRACE REDOING[5]: LSA = %lld|%d, Rv_index = %s,\n"
			   "      volid = %d, pageid = %d, offset = %d,\n",
			   (long long int) rcv_lsa.pageid, rcv_lsa.offset,
			   rv_rcvindex_string (rcvindex), rcv_vpid.volid,
			   rcv_vpid.pageid, rcv.offset);
		  if (rcv_page_lsaptr != NULL)
		    {
		      fprintf (stdout, "      page_lsa = %lld|%d\n",
			       (long long int) rcv_page_lsaptr->pageid,
			       rcv_page_lsaptr->offset);
		    }
		  else
		    {
		      fprintf (stdout, "      page_lsa = %lld|%d\n", -1LL,
			       -1);
		    }
		  fflush (stdout);
		}
#endif /* !NDEBUG */

	      log_rv_redo_record (thread_p, &log_lsa,
				  log_pgptr, RV_fun[rcvindex].undofun, &rcv,
				  &rcv_lsa, 0, NULL, NULL);
	      if (rcv.pgptr != NULL)
		{
		  pgbuf_unfix (thread_p, rcv.pgptr);
		}
	      break;

	    case LOG_COMMIT:
	    case LOG_ABORT:
	      tran_index = logtb_find_tran_index (thread_p, log_rec->trid);
	      if (tran_index != NULL_TRAN_INDEX
		  && tran_index != LOG_SYSTEM_TRAN_INDEX)
		{
#if !defined (NDEBUG)
		  LOG_TDES *tdes;

		  tdes = LOG_FIND_TDES (tran_index);
		  assert (tdes != NULL);
		  assert (tdes->state != TRAN_ACTIVE);
#endif
		  logtb_free_tran_index (thread_p, tran_index);
		}

	      break;

	    case LOG_UNDO_DATA:
	    case LOG_LCOMPENSATE:
	    case LOG_DUMMY_HEAD_POSTPONE:
	    case LOG_POSTPONE:
	    case LOG_COMMIT_WITH_POSTPONE:
	    case LOG_COMMIT_TOPOPE_WITH_POSTPONE:
	    case LOG_COMMIT_TOPOPE:
	    case LOG_ABORT_TOPOPE:
	    case LOG_START_CHKPT:
	    case LOG_END_CHKPT:
	    case LOG_SAVEPOINT:
	    case LOG_DUMMY_CRASH_RECOVERY:
	    case LOG_DUMMY_FILLPAGE_FORARCHIVE:	/* for backward compatibility */
	    case LOG_REPLICATION_DATA:
	    case LOG_REPLICATION_SCHEMA:
	    case LOG_DUMMY_HA_SERVER_STATE:
	    case LOG_DUMMY_OVF_RECORD:
	    case LOG_DUMMY_OVF_RECORD_DEL:
	    case LOG_DUMMY_RECORD:
	    case LOG_DUMMY_UPDATE_GID_BITMAP:
	    case LOG_END_OF_LOG:
	      break;

	    case LOG_SMALLER_LOGREC_TYPE:
	    case LOG_LARGER_LOGREC_TYPE:
	    default:
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOG_PAGE_CORRUPTED,
		      1, log_lsa.pageid);
	      if (LSA_EQ (&lsa, &log_lsa))
		{
		  LSA_SET_NULL (&lsa);
		}
	      break;
	    }

	  /*
	   * We can fix the lsa.pageid in the case of log_records without forward
	   * address at this moment.
	   */
	  if (lsa.offset == NULL_OFFSET && lsa.pageid != NULL_PAGEID
	      && lsa.pageid < log_lsa.pageid)
	    {
	      lsa.pageid = log_lsa.pageid;
	    }
	}
    }

  log_zip_free (undo_unzip_ptr);
  log_zip_free (redo_unzip_ptr);

  /* Now finish all postpone operations */
  log_recovery_finish_all_postpone (thread_p);

  /* Flush all dirty pages */
  logpb_flush_pages_direct (thread_p);

  logpb_flush_header (thread_p);
  (void) pgbuf_flush_all (thread_p, NULL_VOLID);

  return;
}

/*
 * log_recovery_finish_all_postpone - FINISH COMMITTING TRANSACTIONS WITH
 *                                   UNFINISH POSTPONE ACTIONS
 *
 * return: nothing
 *
 * NOTE:Finish the committing of transactions which have been declared
 *              as committed, but not all their postpone actions are done.
 *              This happens when there is a crash in the middle of a
 *              log_commit_with_postpone and log_commit.
 *              This function should be called after the log_recovery_redo
 *              function.
 */
static void
log_recovery_finish_all_postpone (THREAD_ENTRY * thread_p)
{
  int i;
  int save_tran_index;
  LOG_TDES *tdes;		/* Transaction descriptor */
  LOG_LSA first_postpone_to_apply;

  /* Finish committig transactions with unfinished postpone actions */

  save_tran_index = logtb_get_current_tran_index (thread_p);

  for (i = 0; i < log_Gl.trantable.num_total_indices; i++)
    {
      tdes = LOG_FIND_TDES (i);
      if (tdes != NULL && tdes->trid != NULL_TRANID
	  && (tdes->state == TRAN_UNACTIVE_WILL_COMMIT
	      || tdes->state == TRAN_UNACTIVE_COMMITTED_WITH_POSTPONE
	      || tdes->state == TRAN_UNACTIVE_TOPOPE_COMMITTED_WITH_POSTPONE
	      || tdes->state == TRAN_UNACTIVE_COMMITTED))
	{
	  LSA_SET_NULL (&first_postpone_to_apply);

	  logtb_set_current_tran_index (thread_p, i);

	  if (tdes->state == TRAN_UNACTIVE_WILL_COMMIT
	      || tdes->state == TRAN_UNACTIVE_COMMITTED_WITH_POSTPONE)
	    {
	      /*
	       * The transaction was the one that was committing
	       */
	      log_recovery_find_first_postpone (thread_p,
						&first_postpone_to_apply,
						&tdes->posp_nxlsa, tdes);

	      log_do_postpone (thread_p, tdes, &first_postpone_to_apply,
			       LOG_COMMIT_WITH_POSTPONE);

	      (void) log_complete (thread_p, tdes, LOG_COMMIT,
				   LOG_DONT_NEED_NEWTRID);
	      logtb_free_tran_index (thread_p, tdes->tran_index);
	    }
	  else if (tdes->state == TRAN_UNACTIVE_COMMITTED)
	    {
	      (void) log_complete (thread_p, tdes, LOG_COMMIT,
				   LOG_DONT_NEED_NEWTRID);
	      logtb_free_tran_index (thread_p, tdes->tran_index);
	    }
	  else
	    {
	      /*
	       * A top system operation of the transaction was the one that was
	       * committing
	       */
	      log_recovery_find_first_postpone (thread_p,
						&first_postpone_to_apply,
						&tdes->topops.
						stack[tdes->topops.
						      last].posp_lsa, tdes);

	      log_do_postpone (thread_p, tdes,
			       &first_postpone_to_apply,
			       LOG_COMMIT_TOPOPE_WITH_POSTPONE);
	      LSA_SET_NULL (&tdes->topops.stack[tdes->topops.last].posp_lsa);
	      (void) log_end_system_op (thread_p, LOG_RESULT_TOPOP_COMMIT);
	      LSA_COPY (&tdes->undo_nxlsa, &tdes->last_lsa);
	      if (tdes->topops.last < 0)
		{
		  tdes->state = TRAN_UNACTIVE_UNILATERALLY_ABORTED;
		}
	      /*
	       * The rest of the transaction will be aborted using the recovery undo
	       * process
	       */
	    }
	}
    }

  logtb_set_current_tran_index (thread_p, save_tran_index);
}

/*
 * log_recovery_undo - SCAN BACKWARDS UNDOING DATA
 *
 * return: nothing
 *
 */
static void
log_recovery_undo (THREAD_ENTRY * thread_p)
{
  LOG_LSA *lsa_ptr;		/* LSA of log record to undo  */
  char log_pgbuf[IO_MAX_PAGE_SIZE + MAX_ALIGNMENT], *aligned_log_pgbuf;
  LOG_PAGE *log_pgptr = NULL;	/* Log page pointer where LSA
				 * is located
				 */
  LOG_LSA log_lsa;
  LOG_RECORD_HEADER *log_rec;	/* Pointer to log record      */
  struct log_undoredo *undoredo;	/* Undo_redo log record       */
  struct log_undo *undo;	/* Undo log record            */
  struct log_compensate *compensate;	/* Compensating log record    */
  struct log_logical_compensate *logical_comp;	/* end of a logical undo   */
  struct log_topop_result *top_result;	/* Result of top system ope   */
  LOG_RCVINDEX rcvindex;	/* Recovery index function    */
  LOG_RCV rcv;			/* Recovery structure         */
  VPID rcv_vpid;		/* VPID of data to recover    */
  LOG_LSA rcv_lsa;		/* Address of redo log record */
  LOG_LSA prev_tranlsa;		/* prev LSA of transaction    */
  LOG_TDES *tdes;		/* Transaction descriptor     */
  int last_tranlogrec;		/* Is this last log record ?  */
  int tran_index;
  LOG_ZIP *undo_unzip_ptr = NULL;

  aligned_log_pgbuf = PTR_ALIGN (log_pgbuf, MAX_ALIGNMENT);

  /*
   * Remove from the list of transaction to abort, those that have finished
   * when the crash happens, so it does not remain dangling in the transaction
   * table.
   */

  for (tran_index = 0;
       tran_index < log_Gl.trantable.num_total_indices; tran_index++)
    {
      if (tran_index != LOG_SYSTEM_TRAN_INDEX)
	{
	  tdes = LOG_FIND_TDES (tran_index);
	  if (tdes != NULL
	      && tdes->trid != NULL_TRANID
	      && (tdes->state == TRAN_UNACTIVE_UNILATERALLY_ABORTED
		  || tdes->state == TRAN_UNACTIVE_ABORTED)
	      && LSA_ISNULL (&tdes->undo_nxlsa))
	    {
	      (void) log_complete (thread_p, tdes, LOG_ABORT,
				   LOG_DONT_NEED_NEWTRID);
	      logtb_free_tran_index (thread_p, tran_index);
	    }
	}
    }

  /*
   * GO BACKWARDS, undoing records
   */

  /* Find the largest LSA to undo */
  lsa_ptr = logtb_find_unilaterally_largest_undo_lsa (thread_p);

  log_pgptr = (LOG_PAGE *) aligned_log_pgbuf;

  undo_unzip_ptr = log_zip_alloc (LOGAREA_SIZE, false);
  if (undo_unzip_ptr == NULL)
    {
      logpb_fatal_error (thread_p, true, ARG_FILE_LINE, "log_recovery_undo");
      return;
    }

  while (lsa_ptr != NULL && !LSA_ISNULL (lsa_ptr))
    {
      /* Fetch the page where the LSA record to undo is located */
      log_lsa.pageid = lsa_ptr->pageid;
      if (logpb_fetch_page (thread_p, log_lsa.pageid, log_pgptr) == NULL)
	{
	  log_zip_free (undo_unzip_ptr);

	  logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			     "log_recovery_undo");
	  return;
	}

      /* Check all log records in this phase */
      while (lsa_ptr != NULL && lsa_ptr->pageid == log_lsa.pageid)
	{
	  /* Find the log record */
	  log_lsa.offset = lsa_ptr->offset;
	  log_rec = LOG_GET_LOG_RECORD_HEADER (log_pgptr, &log_lsa);

	  LSA_COPY (&prev_tranlsa, &log_rec->prev_tranlsa);

	  tran_index = logtb_find_tran_index (thread_p, log_rec->trid);
	  if (tran_index == NULL_TRAN_INDEX)
	    {
	      assert (false);
	      logtb_free_tran_index_with_undo_lsa (thread_p, lsa_ptr);

	      /* Find the next log record to undo */
	      lsa_ptr = logtb_find_unilaterally_largest_undo_lsa (thread_p);
	      continue;
	    }
	  logtb_set_current_tran_index (thread_p, tran_index);

	  tdes = LOG_FIND_TDES (tran_index);
	  if (tdes == NULL)
	    {
	      assert (false);
	      /* This looks like a system error in the analysis phase */
	      logtb_free_tran_index_with_undo_lsa (thread_p, lsa_ptr);

	      /* Find the next log record to undo */
	      lsa_ptr = logtb_find_unilaterally_largest_undo_lsa (thread_p);
	      continue;
	    }

	  LSA_COPY (&tdes->undo_nxlsa, &prev_tranlsa);

	  switch (log_rec->type)
	    {
	    case LOG_UNDOREDO_DATA:
	    case LOG_DIFF_UNDOREDO_DATA:
	      LSA_COPY (&rcv_lsa, &log_lsa);
	      /*
	       * The transaction was active at the time of the crash. The
	       * transaction is unilaterally aborted by the system
	       */
	      if (prev_tranlsa.pageid == NULL_PAGEID)
		{
		  last_tranlogrec = true;
		}
	      else
		{
		  last_tranlogrec = false;
		}

	      /* Get the DATA HEADER */
	      LOG_READ_ADD_ALIGN (thread_p, sizeof (LOG_RECORD_HEADER),
				  &log_lsa, log_pgptr);
	      LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p,
						sizeof (struct
							log_undoredo),
						&log_lsa, log_pgptr);

	      undoredo =
		(struct log_undoredo *) ((char *) log_pgptr->area +
					 log_lsa.offset);
	      rcvindex = undoredo->data.rcvindex;
	      rcv.length = undoredo->ulength;
	      rcv.offset = undoredo->data.offset;
	      rcv_vpid.volid = undoredo->data.volid;
	      rcv_vpid.pageid = undoredo->data.pageid;

	      LOG_READ_ADD_ALIGN (thread_p, sizeof (struct log_undoredo),
				  &log_lsa, log_pgptr);
#if !defined(NDEBUG)
	      if (prm_get_bool_value (PRM_ID_LOG_TRACE_DEBUG))
		{
		  fprintf (stdout,
			   "TRACE UNDOING[1]: LSA = %lld|%d, Rv_index = %s,\n"
			   "      volid = %d, pageid = %d, offset = %d,\n",
			   (long long int) rcv_lsa.pageid, rcv_lsa.offset,
			   rv_rcvindex_string (rcvindex), rcv_vpid.volid,
			   rcv_vpid.pageid, rcv.offset);
		  fflush (stdout);
		}
#endif /* !NDEBUG */

	      log_rv_undo_record (thread_p, &log_lsa, log_pgptr,
				  rcvindex, &rcv_vpid, &rcv, &rcv_lsa,
				  tdes, undo_unzip_ptr);

	      /* Is this the end of the transaction ? */
	      if (last_tranlogrec == true)
		{
		  (void) log_complete (thread_p, tdes, LOG_ABORT,
				       LOG_DONT_NEED_NEWTRID);
		  logtb_free_tran_index (thread_p, tran_index);
		  tdes = NULL;
		}
	      break;

	    case LOG_UNDO_DATA:
	      LSA_COPY (&rcv_lsa, &log_lsa);
	      /*
	       * The transaction was active at the time of the crash. The
	       * transaction is unilaterally aborted by the system
	       */
	      if (prev_tranlsa.pageid == NULL_PAGEID)
		{
		  last_tranlogrec = true;
		}
	      else
		{
		  last_tranlogrec = false;
		}

	      /* Get the DATA HEADER */
	      LOG_READ_ADD_ALIGN (thread_p, sizeof (LOG_RECORD_HEADER),
				  &log_lsa, log_pgptr);
	      LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p,
						sizeof (struct log_undo),
						&log_lsa, log_pgptr);

	      undo =
		(struct log_undo *) ((char *) log_pgptr->area +
				     log_lsa.offset);
	      rcvindex = undo->data.rcvindex;
	      rcv.length = undo->length;
	      rcv.offset = undo->data.offset;
	      rcv_vpid.volid = undo->data.volid;
	      rcv_vpid.pageid = undo->data.pageid;

	      LOG_READ_ADD_ALIGN (thread_p, sizeof (struct log_undo),
				  &log_lsa, log_pgptr);

#if !defined(NDEBUG)
	      if (prm_get_bool_value (PRM_ID_LOG_TRACE_DEBUG))
		{
		  fprintf (stdout,
			   "TRACE UNDOING[2]: LSA = %lld|%d, Rv_index = %s,\n"
			   "      volid = %d, pageid = %d, offset = %d,\n",
			   (long long int) rcv_lsa.pageid, rcv_lsa.offset,
			   rv_rcvindex_string (rcvindex), rcv_vpid.volid,
			   rcv_vpid.pageid, rcv.offset);
		  fflush (stdout);
		}
#endif /* !NDEBUG */
	      log_rv_undo_record (thread_p, &log_lsa, log_pgptr,
				  rcvindex, &rcv_vpid, &rcv, &rcv_lsa,
				  tdes, undo_unzip_ptr);

	      /* Is this the end of the transaction ? */
	      if (last_tranlogrec == true)
		{
		  (void) log_complete (thread_p, tdes, LOG_ABORT,
				       LOG_DONT_NEED_NEWTRID);
		  logtb_free_tran_index (thread_p, tran_index);
		  tdes = NULL;
		}
	      break;

	    case LOG_REDO_DATA:
	    case LOG_DBEXTERN_REDO_DATA:
	    case LOG_DUMMY_HEAD_POSTPONE:
	    case LOG_POSTPONE:
	    case LOG_SAVEPOINT:
	    case LOG_REPLICATION_DATA:
	    case LOG_REPLICATION_SCHEMA:
	    case LOG_DUMMY_HA_SERVER_STATE:
	    case LOG_DUMMY_OVF_RECORD:
	    case LOG_DUMMY_OVF_RECORD_DEL:
	    case LOG_DUMMY_UPDATE_GID_BITMAP:
	    case LOG_DUMMY_RECORD:
	      /* Not for UNDO .. Go to previous record */

	      /* Is this the end of the transaction ? */
	      if (LSA_ISNULL (&prev_tranlsa))
		{
		  (void) log_complete (thread_p, tdes, LOG_ABORT,
				       LOG_DONT_NEED_NEWTRID);
		  logtb_free_tran_index (thread_p, tran_index);
		  tdes = NULL;
		}
	      break;

	    case LOG_COMPENSATE:
	      /* Only for REDO .. Go to next undo record
	       * Need to read the compensating record to set the next
	       * undo address
	       */

	      /* Get the DATA HEADER */
	      LOG_READ_ADD_ALIGN (thread_p, sizeof (LOG_RECORD_HEADER),
				  &log_lsa, log_pgptr);
	      LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p,
						sizeof (struct
							log_compensate),
						&log_lsa, log_pgptr);
	      compensate =
		(struct log_compensate *) ((char *) log_pgptr->area +
					   log_lsa.offset);
	      /* Is this the end of the transaction ? */
	      if (LSA_ISNULL (&compensate->undo_nxlsa))
		{
		  (void) log_complete (thread_p, tdes, LOG_ABORT,
				       LOG_DONT_NEED_NEWTRID);
		  logtb_free_tran_index (thread_p, tran_index);
		  tdes = NULL;
		}
	      else
		{
		  LSA_COPY (&prev_tranlsa, &compensate->undo_nxlsa);
		}
	      break;

	    case LOG_LCOMPENSATE:
	      /* Only for REDO .. Go to next undo record
	       * Need to read the compensating record to set the next
	       * undo address
	       */

	      /* Get the DATA HEADER */
	      LOG_READ_ADD_ALIGN (thread_p, sizeof (LOG_RECORD_HEADER),
				  &log_lsa, log_pgptr);
	      LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p,
						sizeof (struct
							log_logical_compensate),
						&log_lsa, log_pgptr);
	      logical_comp =
		((struct log_logical_compensate *) ((char *) log_pgptr->
						    area + log_lsa.offset));

	      /* Is this the end of the transaction ? */
	      if (LSA_ISNULL (&logical_comp->undo_nxlsa))
		{
		  (void) log_complete (thread_p, tdes, LOG_ABORT,
				       LOG_DONT_NEED_NEWTRID);
		  logtb_free_tran_index (thread_p, tran_index);
		  tdes = NULL;
		}
	      else
		{
		  LSA_COPY (&prev_tranlsa, &logical_comp->undo_nxlsa);
		}
	      break;

	    case LOG_COMMIT_TOPOPE:
	    case LOG_ABORT_TOPOPE:
	      /*
	       * We found a system top operation that should be skipped from
	       * rollback
	       */

	      /* Read the DATA HEADER */
	      LOG_READ_ADD_ALIGN (thread_p, sizeof (LOG_RECORD_HEADER),
				  &log_lsa, log_pgptr);
	      LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p,
						sizeof (struct
							log_topop_result),
						&log_lsa, log_pgptr);
	      top_result =
		((struct log_topop_result *) ((char *) log_pgptr->area +
					      log_lsa.offset));
	      /* Is this the end of the transaction ? */
	      if (LSA_ISNULL (&top_result->lastparent_lsa))
		{
		  (void) log_complete (thread_p, tdes, LOG_ABORT,
				       LOG_DONT_NEED_NEWTRID);
		  logtb_free_tran_index (thread_p, tran_index);
		  tdes = NULL;
		}
	      else
		{
		  LSA_COPY (&prev_tranlsa, &top_result->lastparent_lsa);
		}
	      break;

	    case LOG_RUN_POSTPONE:
	    case LOG_COMMIT_WITH_POSTPONE:
	    case LOG_COMMIT:
	    case LOG_COMMIT_TOPOPE_WITH_POSTPONE:
	    case LOG_ABORT:
	    case LOG_START_CHKPT:
	    case LOG_END_CHKPT:
	    case LOG_DUMMY_CRASH_RECOVERY:
	    case LOG_DUMMY_FILLPAGE_FORARCHIVE:	/* for backward compatibility */
	    case LOG_END_OF_LOG:
	      /* This looks like a system error in the analysis phase */
	      /* Remove the transaction from the recovery process */
	      (void) log_complete (thread_p, tdes, LOG_ABORT,
				   LOG_DONT_NEED_NEWTRID);
	      logtb_free_tran_index (thread_p, tran_index);
	      tdes = NULL;
	      break;

	    case LOG_SMALLER_LOGREC_TYPE:
	    case LOG_LARGER_LOGREC_TYPE:
	    default:
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_LOG_PAGE_CORRUPTED, 1, log_lsa.pageid);

	      /*
	       * Remove the transaction from the recovery process
	       */
	      (void) log_complete (thread_p, tdes, LOG_ABORT,
				   LOG_DONT_NEED_NEWTRID);
	      logtb_free_tran_index (thread_p, tran_index);
	      tdes = NULL;
	      break;

	    }			/* switch */

	  /* Just in case, it was changed */
	  if (tdes != NULL)
	    {
	      LSA_COPY (&tdes->undo_nxlsa, &prev_tranlsa);
	    }

	  /* Find the next log record to undo */
	  lsa_ptr = logtb_find_unilaterally_largest_undo_lsa (thread_p);
	}
    }

  log_zip_free (undo_unzip_ptr);

  /* Flush all dirty pages */

  logpb_flush_pages_direct (thread_p);

  logpb_flush_header (thread_p);
  (void) pgbuf_flush_all (thread_p, NULL_VOLID);

  return;
}

/*
 * log_recovery_notpartof_archives - REMOVE ARCHIVES THAT ARE NOT PART OF
 *                                 DATABASE (USED for PARTIAL RECOVERY)
 *
 * return: nothing..
 *
 *   start_arv_num(in): Start removing archives at this point.
 *   info_reason(in): Reason for removal
 *
 * NOTE: Remove archives that are not part of database any longer.
 *              This happen when we do partial recovery.
 */
static void
log_recovery_notpartof_archives (THREAD_ENTRY * thread_p, int start_arv_num,
				 const char *info_reason)
{
  char logarv_name[PATH_MAX];	/* Archive name  */
  char logarv_name_first[PATH_MAX];	/* Archive name  */
  int i;
  const char *catmsg;
  int error_code;



  if (log_Gl.append.vdes != NULL_VOLDES)
    {
      /*
       * Trust the current log header to remove any archives that are not
       * needed due to partial recovery
       */
      for (i = start_arv_num; i <= log_Gl.hdr.nxarv_num - 1; i++)
	{
	  fileio_make_log_archive_name (logarv_name, log_Archive_path,
					log_Prefix, i);
	  fileio_unformat (thread_p, logarv_name);
	}
    }
  else
    {
      /*
       * We don't know where to stop. Stop when an archive is not in the OS
       * This may not be good enough.
       */
      for (i = start_arv_num; i <= INT_MAX; i++)
	{
	  fileio_make_log_archive_name (logarv_name, log_Archive_path,
					log_Prefix, i);
	  if (fileio_is_volume_exist (logarv_name) == false)
	    {
	      if (i > start_arv_num)
		{
		  fileio_make_log_archive_name (logarv_name, log_Archive_path,
						log_Prefix, i - 1);
		}
	      break;
	    }
	  fileio_unformat (thread_p, logarv_name);
	}
    }

  if (info_reason != NULL)
    /*
     * Note if start_arv_num == i, we break from the loop and did not remove
     * anything
     */
    if (start_arv_num != i)
      {
	if (start_arv_num == i - 1)
	  {
	    catmsg = msgcat_message (MSGCAT_CATALOG_RYE,
				     MSGCAT_SET_LOG,
				     MSGCAT_LOG_LOGINFO_REMOVE_REASON);
	    if (catmsg == NULL)
	      {
		catmsg = "REMOVE: %d %s to %d %s.\nREASON: %s\n";
	      }
	    error_code =
	      log_dump_log_info (log_Name_info, true, catmsg,
				 start_arv_num,
				 fileio_get_base_file_name (logarv_name),
				 start_arv_num,
				 fileio_get_base_file_name (logarv_name),
				 info_reason);
	    if (error_code != NO_ERROR && error_code != ER_LOG_MOUNT_FAIL)
	      {
		return;
	      }
	  }
	else
	  {
	    fileio_make_log_archive_name (logarv_name_first, log_Archive_path,
					  log_Prefix, start_arv_num);

	    catmsg = msgcat_message (MSGCAT_CATALOG_RYE,
				     MSGCAT_SET_LOG,
				     MSGCAT_LOG_LOGINFO_REMOVE_REASON);
	    if (catmsg == NULL)
	      {
		catmsg = "REMOVE: %d %s to %d %s.\nREASON: %s\n";
	      }
	    error_code =
	      log_dump_log_info (log_Name_info, true, catmsg,
				 start_arv_num,
				 fileio_get_base_file_name
				 (logarv_name_first), i - 1,
				 fileio_get_base_file_name (logarv_name),
				 info_reason);
	    if (error_code != NO_ERROR && error_code != ER_LOG_MOUNT_FAIL)
	      {
		return;
	      }
	  }
      }

  log_Gl.hdr.last_deleted_arv_num = (start_arv_num == i) ? i : i - 1;


  if (log_Gl.append.vdes != NULL_VOLDES)
    {
      logpb_flush_header (thread_p);	/* to get rid off archives */
    }

}

/*
 * log_unformat_ahead_volumes -
 *
 * return:
 *
 *   volid(in):
 *   start_volid(in):
 *
 * NOTE:
 */
static bool
log_unformat_ahead_volumes (THREAD_ENTRY * thread_p, VOLID volid,
			    VOLID * start_volid)
{
  bool result = true;
  char *vlabel = NULL;

  if (volid != NULL_VOLID && volid >= *start_volid)
    {
      /* This volume is not part of the database any longer */
      if (pgbuf_invalidate_all (thread_p, volid) != NO_ERROR)
	{
	  result = false;
	}
      else
	{
	  vlabel = fileio_get_volume_label (volid, ALLOC_COPY);
	  fileio_unformat (thread_p, vlabel);
	}
    }
  return result;
}

/*
 * log_recovery_notpartof_volumes -
 *
 * return:
 *
 * NOTE:
 */
static void
log_recovery_notpartof_volumes (THREAD_ENTRY * thread_p)
{
  const char *ext_path;
  const char *ext_name;
  int vdes;
  VOLID start_volid;
  VOLID volid;
  char vol_fullname[PATH_MAX];
  INT64 vol_dbcreation;		/* Database creation time in volume */
  char *alloc_extpath = NULL;
  UNUSED_VAR int ret = NO_ERROR;

  start_volid = boot_find_next_permanent_volid (thread_p);

  /*
   * FIRST: ASSUME VOLUME INFORMATION WAS AHEAD OF US.
   * Start removing mounted volumes that are not part of the database any
   * longer due to partial recovery point. Note that these volumes were
   * mounted before the recovery started.
   */

  (void) fileio_map_mounted (thread_p,
			     (bool (*)(THREAD_ENTRY *, VOLID, void *))
			     log_unformat_ahead_volumes, &start_volid);

  /*
   * SECOND: ASSUME RIGHT VOLUME INFORMATION.
   * Remove any volumes that are laying around on disk
   */

  /*
   * Get the name of the extension: ext_path|dbname|"ext"|volid
   */

  /* Use the directory where the primary volume is located */
  alloc_extpath = (char *) malloc (PATH_MAX);
  if (alloc_extpath != NULL)
    {
      ext_path = fileio_get_directory_path (alloc_extpath, log_Db_fullname);
      if (ext_path == NULL)
	{
	  alloc_extpath[0] = '\0';
	  ext_path = alloc_extpath;
	}
    }
  else
    {
      assert (false);
      ext_path = "";		/* Pointer to a null terminated string */
    }

  ext_name = fileio_get_base_file_name (log_Db_fullname);
  /*
   * We don't know where to stop. Stop when an archive is not in the OS
   */

  for (volid = start_volid; volid < LOG_MAX_DBVOLID; volid++)
    {
      fileio_make_volume_ext_name (vol_fullname, ext_path, ext_name, volid);
      if (fileio_is_volume_exist (vol_fullname) == false)
	{
	  break;
	}

      vdes =
	fileio_mount (thread_p, log_Db_fullname, vol_fullname, volid, false,
		      false);
      if (vdes != NULL_VOLDES)
	{
	  ret = disk_get_creation_time (thread_p, volid, &vol_dbcreation);
	  fileio_dismount (thread_p, vdes);
	  if (difftime ((time_t) vol_dbcreation,
			(time_t) log_Gl.hdr.db_creation) != 0)
	    {
	      /* This volume does not belong to given database */
	      ;			/* NO-OP */
	    }
	  else
	    {
	      fileio_unformat (thread_p, vol_fullname);
	    }
	}
    }

  if (alloc_extpath)
    {
      free_and_init (alloc_extpath);
    }

  (void) logpb_recreate_volume_info (thread_p);

}

/*
 * log_recovery_resetlog -
 *
 * return:
 *
 *   new_appendlsa(in):
 *
 * NOTE:
 */
static void
log_recovery_resetlog (THREAD_ENTRY * thread_p, LOG_LSA * new_append_lsa,
		       bool is_new_append_page, LOG_LSA * last_lsa)
{
  char newappend_pgbuf[IO_MAX_PAGE_SIZE + MAX_ALIGNMENT];
  char *aligned_newappend_pgbuf;
  LOG_PAGE *newappend_pgptr = NULL;
  int arv_num;
  const char *catmsg;
  char *catmsg_dup;
  int ret = NO_ERROR;

  assert (LOG_CS_OWN_WRITE_MODE (thread_p));
  assert (last_lsa != NULL);

  aligned_newappend_pgbuf = PTR_ALIGN (newappend_pgbuf, MAX_ALIGNMENT);

  if (log_Gl.append.vdes != NULL_VOLDES && log_Gl.append.log_pgptr != NULL)
    {
      logpb_flush_pages_direct (thread_p);
      logpb_invalid_all_append_pages (thread_p);
    }

  if (LSA_ISNULL (new_append_lsa))
    {
      log_Gl.hdr.append_lsa.pageid = 0;
      log_Gl.hdr.append_lsa.offset = 0;
      LOG_RESET_APPEND_LSA (&log_Gl.hdr.append_lsa);
    }
  else
    {
      if (log_Gl.append.vdes == NULL_VOLDES
	  || (log_Gl.hdr.fpageid > new_append_lsa->pageid
	      && new_append_lsa->offset > 0))
	{
	  /*
	   * We are going to rest the header of the active log to the past.
	   * Save the content of the new append page since it has to be
	   * transfered to the new location. This is needed since we may not
	   * start at location zero.
	   *
	   * We need to destroy any log archive createded after this point
	   */

	  newappend_pgptr = (LOG_PAGE *) aligned_newappend_pgbuf;

	  if ((logpb_fetch_page (thread_p, new_append_lsa->pageid,
				 newappend_pgptr)) == NULL)
	    {
	      newappend_pgptr = NULL;
	    }
	}
      LOG_RESET_APPEND_LSA (new_append_lsa);
    }

  LSA_COPY (&log_Gl.hdr.chkpt_lsa, &log_Gl.hdr.append_lsa);
  log_Gl.hdr.is_shutdown = false;

  logpb_invalidate_pool (thread_p);

  if (log_Gl.append.vdes == NULL_VOLDES
      || log_Gl.hdr.fpageid > log_Gl.hdr.append_lsa.pageid)
    {
      LOG_PAGE *loghdr_pgptr = NULL;
      LOG_PAGE *append_pgptr = NULL;

      /*
       * Don't have the log active, or we are going to the past
       */
      arv_num = logpb_get_archive_number (thread_p,
					  log_Gl.hdr.append_lsa.pageid - 1);
      if (arv_num == -1)
	{
	  logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			     "log_recovery_resetlog");
	  return;
	}
      arv_num = arv_num + 1;

      catmsg = msgcat_message (MSGCAT_CATALOG_RYE,
			       MSGCAT_SET_LOG,
			       MSGCAT_LOG_RESETLOG_DUE_INCOMPLTE_MEDIA_RECOVERY);
      catmsg_dup = strdup (catmsg);
      if (catmsg_dup != NULL)
	{
	  log_recovery_notpartof_archives (thread_p, arv_num, catmsg_dup);
	  free_and_init (catmsg_dup);
	}
      else
	{
	  /* NOTE: catmsg..may get corrupted if the function calls the catalog */
	  log_recovery_notpartof_archives (thread_p, arv_num, catmsg);
	}

      log_Gl.hdr.fpageid = log_Gl.hdr.append_lsa.pageid;
      log_Gl.hdr.nxarv_pageid = log_Gl.hdr.append_lsa.pageid;
      log_Gl.hdr.nxarv_phy_pageid =
	logpb_to_physical_pageid (log_Gl.hdr.nxarv_pageid);
      log_Gl.hdr.nxarv_num = arv_num;
      log_Gl.hdr.last_arv_num_for_syscrashes = -1;
      log_Gl.hdr.last_deleted_arv_num = -1;

      if (log_Gl.append.vdes == NULL_VOLDES)
	{
#if 1				/* TODO - trace */
	  assert (false);
#endif

	  /* Create the log active since we do not have one */
	  ret = disk_get_creation_time (thread_p, LOG_DBFIRST_VOLID,
					&log_Gl.hdr.db_creation);
	  if (ret != NO_ERROR)
	    {
	      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
				 "log_recovery_resetlog");
	      return;
	    }

	  log_Gl.append.vdes = fileio_format (thread_p, log_Db_fullname,
					      log_Name_active,
					      LOG_DBLOG_ACTIVE_VOLID,
					      log_get_num_pages_for_creation
					      (-1), false, true, false,
					      LOG_PAGESIZE, 0, false);

	  if (log_Gl.append.vdes != NULL_VOLDES)
	    {
	      loghdr_pgptr = logpb_create_header_page (thread_p);
	    }

	  if (log_Gl.append.vdes == NULL_VOLDES || loghdr_pgptr == NULL)
	    {
	      if (loghdr_pgptr != NULL)
		{
		  logpb_free_page (thread_p, loghdr_pgptr);
		}
	      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
				 "log_recovery_resetlog");
	      return;
	    }

	  /*
	   * Flush the header page and first append page so that we can record
	   * the header page on it
	   */
	  if (logpb_flush_page (thread_p, loghdr_pgptr, FREE) != NO_ERROR)
	    {
	      logpb_fatal_error (thread_p, false, ARG_FILE_LINE,
				 "log_recovery_resetlog");
	    }
	}

      append_pgptr = logpb_create (thread_p, log_Gl.hdr.fpageid);
      if (append_pgptr == NULL)
	{
	  logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			     "log_recovery_resetlog");
	  return;
	}
      if (logpb_flush_page (thread_p, append_pgptr, FREE) != NO_ERROR)
	{
	  logpb_fatal_error (thread_p, false, ARG_FILE_LINE,
			     "log_recovery_resetlog");
	  return;
	}
    }
  else
    {
      /*
       * There is already a log active and the new append location is in the
       * current range. Leave the log as it is, just reset the append location.
       */
      if (log_Gl.hdr.nxarv_pageid >= log_Gl.hdr.append_lsa.pageid)
	{
	  log_Gl.hdr.nxarv_pageid = log_Gl.hdr.append_lsa.pageid;
	  log_Gl.hdr.nxarv_phy_pageid =
	    logpb_to_physical_pageid (log_Gl.hdr.nxarv_pageid);
	}
    }

  /*
   * Fetch the append page and write it with and end of log mark.
   * Then, free the page, same for the header page.
   */

  if (is_new_append_page == true)
    {
      if (logpb_fetch_start_append_page_new (thread_p) == NULL)
	{
	  logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			     "log_recovery_resetlog");
	  return;
	}
    }
  else
    {
      if (logpb_fetch_start_append_page (thread_p) != NULL)
	{
	  if (newappend_pgptr != NULL && log_Gl.append.log_pgptr != NULL)
	    {
	      memcpy ((char *) log_Gl.append.log_pgptr,
		      (char *) newappend_pgptr, LOG_PAGESIZE);
	      logpb_set_dirty (thread_p, log_Gl.append.log_pgptr, DONT_FREE);
	    }
	  logpb_flush_pages_direct (thread_p);
	}
    }

  LOG_RESET_PREV_LSA (last_lsa);

  logpb_flush_header (thread_p);
  logpb_decache_archive_info (thread_p);

  return;
}

/*
 * log_startof_nxrec - FIND START OF NEXT RECORD (USED FOR PARTIAL RECOVERY)
 *
 * return: lsa or NULL in case of error
 *
 *   lsa(in):  Starting address. Set as a side effect to next address
 *   canuse_forwaddr(in): Use forward address if available
 *
 * NOTE:Find start address of next record either by looking to forward
 *              address or by scanning the current record.
 */
LOG_LSA *
log_startof_nxrec (THREAD_ENTRY * thread_p, LOG_LSA * lsa,
		   bool canuse_forwaddr)
{
  char log_pgbuf[IO_MAX_PAGE_SIZE + MAX_ALIGNMENT], *aligned_log_pgbuf;
  LOG_PAGE *log_pgptr = NULL;	/* Log page pointer where
				 * LSA is located
				 */
  LOG_LSA log_lsa;
  LOG_RECTYPE type;		/* Log record type           */
  LOG_RECORD_HEADER *log_rec;	/* Pointer to log record     */
  struct log_undoredo *undoredo;	/* Undo_redo log record      */
  struct log_undo *undo;	/* Undo log record           */
  struct log_redo *redo;	/* Redo log record           */
  struct log_dbout_redo *dbout_redo;	/* A external redo log record */
  struct log_savept *savept;	/* A savepoint log record    */
  struct log_compensate *compensate;	/* Compensating log record   */
  struct log_run_postpone *run_posp;	/* A run postpone action     */
  struct log_chkpt *chkpt;	/* Checkpoint log record     */
  struct log_replication *repl_log;

  int undo_length;		/* Undo length               */
  int redo_length;		/* Redo length               */
  int repl_log_length;

  aligned_log_pgbuf = PTR_ALIGN (log_pgbuf, MAX_ALIGNMENT);

  if (LSA_ISNULL (lsa))
    {
      return NULL;
    }

  log_pgptr = (LOG_PAGE *) aligned_log_pgbuf;

  if (logpb_fetch_page (thread_p, lsa->pageid, log_pgptr) == NULL)
    {
      fprintf (stdout, " Error reading page %lld... Quit\n",
	       (long long int) lsa->pageid);
      goto error;
    }

  /*
   * If offset is missing, it is because we archive an incomplete
   * log record or we start dumping the log not from its first page. We
   * have to find the offset by searching for the next log_record in the page
   */
  if (lsa->offset == NULL_OFFSET)
    {
      lsa->offset = log_pgptr->hdr.offset;
      if (lsa->offset == NULL_OFFSET)
	{
	  goto error;
	}
    }

  LSA_COPY (&log_lsa, lsa);
  log_rec = LOG_GET_LOG_RECORD_HEADER (log_pgptr, &log_lsa);
  type = log_rec->type;

  if (canuse_forwaddr == true)
    {
      /*
       * Use forward address of current log record
       */
      LSA_COPY (lsa, &log_rec->forw_lsa);
      if (LSA_ISNULL (lsa) && logpb_is_page_in_archive (log_lsa.pageid))
	{
	  lsa->pageid = log_lsa.pageid + 1;
	}

      if (!LSA_ISNULL (lsa))
	{
	  return lsa;
	}
    }

  /* Advance the pointer to log_rec data */

  LOG_READ_ADD_ALIGN (thread_p, sizeof (LOG_RECORD_HEADER), &log_lsa,
		      log_pgptr);
  switch (type)
    {
    case LOG_UNDOREDO_DATA:
    case LOG_DIFF_UNDOREDO_DATA:
      {
	/* Read the DATA HEADER */
	LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p,
					  sizeof (struct log_undoredo),
					  &log_lsa, log_pgptr);
	undoredo =
	  (struct log_undoredo *) ((char *) log_pgptr->area + log_lsa.offset);

	undo_length = (int) GET_ZIP_LEN (undoredo->ulength);
	redo_length = (int) GET_ZIP_LEN (undoredo->rlength);

	LOG_READ_ADD_ALIGN (thread_p, sizeof (struct log_undoredo), &log_lsa,
			    log_pgptr);
	LOG_READ_ADD_ALIGN (thread_p, undo_length, &log_lsa, log_pgptr);
	LOG_READ_ADD_ALIGN (thread_p, redo_length, &log_lsa, log_pgptr);
	break;
      }

    case LOG_UNDO_DATA:
      {
	/* Read the DATA HEADER */
	LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p, sizeof (struct log_undo),
					  &log_lsa, log_pgptr);
	undo =
	  (struct log_undo *) ((char *) log_pgptr->area + log_lsa.offset);

	undo_length = (int) GET_ZIP_LEN (undo->length);

	LOG_READ_ADD_ALIGN (thread_p, sizeof (struct log_undo), &log_lsa,
			    log_pgptr);
	LOG_READ_ADD_ALIGN (thread_p, undo_length, &log_lsa, log_pgptr);
	break;
      }

    case LOG_REDO_DATA:
    case LOG_POSTPONE:
    case LOG_DUMMY_OVF_RECORD_DEL:
      {
	/* Read the DATA HEADER */
	LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p, sizeof (struct log_redo),
					  &log_lsa, log_pgptr);
	redo =
	  (struct log_redo *) ((char *) log_pgptr->area + log_lsa.offset);
	redo_length = (int) GET_ZIP_LEN (redo->length);

	LOG_READ_ADD_ALIGN (thread_p, sizeof (struct log_redo), &log_lsa,
			    log_pgptr);
	LOG_READ_ADD_ALIGN (thread_p, redo_length, &log_lsa, log_pgptr);
	break;
      }

    case LOG_RUN_POSTPONE:
      {
	/* Read the DATA HEADER */
	LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p,
					  sizeof (struct log_run_postpone),
					  &log_lsa, log_pgptr);
	run_posp =
	  (struct log_run_postpone *) ((char *) log_pgptr->area +
				       log_lsa.offset);
	redo_length = run_posp->length;

	LOG_READ_ADD_ALIGN (thread_p, sizeof (struct log_run_postpone),
			    &log_lsa, log_pgptr);
	LOG_READ_ADD_ALIGN (thread_p, redo_length, &log_lsa, log_pgptr);
	break;
      }

    case LOG_DBEXTERN_REDO_DATA:
      {
	/* Read the data header */
	LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p,
					  sizeof (struct log_dbout_redo),
					  &log_lsa, log_pgptr);
	dbout_redo =
	  ((struct log_dbout_redo *) ((char *) log_pgptr->area +
				      log_lsa.offset));
	redo_length = dbout_redo->length;

	LOG_READ_ADD_ALIGN (thread_p, sizeof (struct log_dbout_redo),
			    &log_lsa, log_pgptr);
	LOG_READ_ADD_ALIGN (thread_p, redo_length, &log_lsa, log_pgptr);
	break;
      }

    case LOG_COMPENSATE:
      {
	/* Read the DATA HEADER */
	LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p,
					  sizeof (struct log_compensate),
					  &log_lsa, log_pgptr);
	compensate =
	  (struct log_compensate *) ((char *) log_pgptr->area +
				     log_lsa.offset);
	redo_length = compensate->length;

	LOG_READ_ADD_ALIGN (thread_p, sizeof (struct log_compensate),
			    &log_lsa, log_pgptr);
	LOG_READ_ADD_ALIGN (thread_p, redo_length, &log_lsa, log_pgptr);
	break;
      }

    case LOG_LCOMPENSATE:
      {
	/* Read the DATA HEADER */
	LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p,
					  sizeof (struct
						  log_logical_compensate),
					  &log_lsa, log_pgptr);

	LOG_READ_ADD_ALIGN (thread_p, sizeof (struct log_logical_compensate),
			    &log_lsa, log_pgptr);
	break;
      }

    case LOG_COMMIT_WITH_POSTPONE:
      {
	/* Read the DATA HEADER */
	LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p,
					  sizeof (struct log_start_postpone),
					  &log_lsa, log_pgptr);

	LOG_READ_ADD_ALIGN (thread_p, sizeof (struct log_start_postpone),
			    &log_lsa, log_pgptr);
	break;
      }

    case LOG_COMMIT:
    case LOG_ABORT:
      {
	/* Read the DATA HEADER */
	LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p,
					  sizeof (struct log_donetime),
					  &log_lsa, log_pgptr);

	LOG_READ_ADD_ALIGN (thread_p, sizeof (struct log_donetime), &log_lsa,
			    log_pgptr);
	break;
      }

    case LOG_COMMIT_TOPOPE_WITH_POSTPONE:
      {
	/* Read the DATA HEADER */
	LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p,
					  sizeof (struct
						  log_topope_start_postpone),
					  &log_lsa, log_pgptr);

	LOG_READ_ADD_ALIGN (thread_p,
			    sizeof (struct log_topope_start_postpone),
			    &log_lsa, log_pgptr);
	break;
      }

    case LOG_COMMIT_TOPOPE:
    case LOG_ABORT_TOPOPE:
      {
	/* Read the DATA HEADER */
	LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p,
					  sizeof (struct log_topop_result),
					  &log_lsa, log_pgptr);

	LOG_READ_ADD_ALIGN (thread_p, sizeof (struct log_topop_result),
			    &log_lsa, log_pgptr);
	break;
      }

    case LOG_END_CHKPT:
      {
	/* Read the DATA HEADER */
	LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p, sizeof (struct log_chkpt),
					  &log_lsa, log_pgptr);
	chkpt =
	  (struct log_chkpt *) ((char *) log_pgptr->area + log_lsa.offset);
	undo_length = sizeof (struct log_chkpt_trans) * chkpt->ntrans;
	redo_length = (sizeof (struct log_chkpt_topops_commit_posp) *
		       chkpt->ntops);

	LOG_READ_ADD_ALIGN (thread_p, sizeof (struct log_chkpt), &log_lsa,
			    log_pgptr);
	LOG_READ_ADD_ALIGN (thread_p, undo_length, &log_lsa, log_pgptr);
	if (redo_length > 0)
	  LOG_READ_ADD_ALIGN (thread_p, redo_length, &log_lsa, log_pgptr);
	break;
      }

    case LOG_SAVEPOINT:
      {
	/* Read the DATA HEADER */
	LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p,
					  sizeof (struct log_savept),
					  &log_lsa, log_pgptr);
	savept =
	  (struct log_savept *) ((char *) log_pgptr->area + log_lsa.offset);
	undo_length = savept->length;

	LOG_READ_ADD_ALIGN (thread_p, sizeof (struct log_savept), &log_lsa,
			    log_pgptr);
	LOG_READ_ADD_ALIGN (thread_p, undo_length, &log_lsa, log_pgptr);
	break;
      }

    case LOG_START_CHKPT:
    case LOG_DUMMY_HEAD_POSTPONE:
    case LOG_DUMMY_CRASH_RECOVERY:
    case LOG_DUMMY_OVF_RECORD:
    case LOG_DUMMY_RECORD:
    case LOG_END_OF_LOG:
      break;

    case LOG_REPLICATION_DATA:
    case LOG_REPLICATION_SCHEMA:
      LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p,
					sizeof (struct log_replication),
					&log_lsa, log_pgptr);

      repl_log = (struct log_replication *) ((char *) log_pgptr->area
					     + log_lsa.offset);
      repl_log_length = (int) GET_ZIP_LEN (repl_log->length);

      LOG_READ_ADD_ALIGN (thread_p, sizeof (struct log_replication),
			  &log_lsa, log_pgptr);
      LOG_READ_ADD_ALIGN (thread_p, repl_log_length, &log_lsa, log_pgptr);
      break;

    case LOG_DUMMY_HA_SERVER_STATE:
      LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p,
					sizeof (struct log_ha_server_state),
					&log_lsa, log_pgptr);
      LOG_READ_ADD_ALIGN (thread_p, sizeof (struct log_ha_server_state),
			  &log_lsa, log_pgptr);
      break;

    case LOG_DUMMY_UPDATE_GID_BITMAP:
      LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p,
					sizeof (struct log_gid_bitmap_update),
					&log_lsa, log_pgptr);
      LOG_READ_ADD_ALIGN (thread_p, sizeof (struct log_gid_bitmap_update),
			  &log_lsa, log_pgptr);
      break;

    case LOG_DUMMY_FILLPAGE_FORARCHIVE:	/* for backward compatibility */
      {
	/* Get to start of next page */
	LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p, LOG_PAGESIZE, &log_lsa,
					  log_pgptr);
	break;
      }

    case LOG_SMALLER_LOGREC_TYPE:
    case LOG_LARGER_LOGREC_TYPE:
    default:
      break;
    }				/* switch */

  /* Make sure you point to beginning of a next record */
  LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p, sizeof (LOG_RECORD_HEADER),
				    &log_lsa, log_pgptr);

  LSA_COPY (lsa, &log_lsa);

  return lsa;

error:

  return NULL;
}

/*
 * log_recovery_find_first_postpone -
 *      Find the first postpone log lsa to be applied.
 *
 * return: error code
 *
 *   ret_lsa(out):
 *   start_postpone_lsa(in):
 *   tdes(in):
 *   pospone_type(in):
 *
 */
static int
log_recovery_find_first_postpone (THREAD_ENTRY * thread_p,
				  LOG_LSA * ret_lsa,
				  LOG_LSA * start_postpone_lsa,
				  LOG_TDES * tdes)
{
  LOG_LSA end_postpone_lsa;
  LOG_LSA start_seek_lsa;
  LOG_LSA *end_seek_lsa;
  LOG_LSA next_start_seek_lsa;
  LOG_LSA log_lsa;
  LOG_LSA forward_lsa;
  LOG_LSA next_postpone_lsa;
  struct log_run_postpone *run_posp;

  char log_pgbuf[IO_MAX_PAGE_SIZE + MAX_ALIGNMENT];
  char *aligned_log_pgbuf;
  LOG_PAGE *log_pgptr = NULL;
  LOG_RECORD_HEADER *log_rec;
  bool isdone;

  LOG_TOPOP_RANGE nxtop_array[LOG_TOPOP_STACK_INIT_SIZE];
  LOG_TOPOP_RANGE *nxtop_stack = NULL;
  LOG_TOPOP_RANGE *nxtop_range = NULL;
  int nxtop_count = 0;
  bool start_postpone_lsa_wasapplied = false;

  assert (ret_lsa && start_postpone_lsa && tdes);

  LSA_SET_NULL (ret_lsa);

  if (log_is_in_crash_recovery () == false
      || (tdes->state != TRAN_UNACTIVE_WILL_COMMIT
	  && tdes->state != TRAN_UNACTIVE_COMMITTED_WITH_POSTPONE
	  && tdes->state != TRAN_UNACTIVE_TOPOPE_COMMITTED_WITH_POSTPONE))
    {
      assert (0);
      return ER_FAILED;
    }

  if (LSA_ISNULL (start_postpone_lsa))
    {
      return NO_ERROR;
    }

  LSA_SET_NULL (&next_postpone_lsa);

  aligned_log_pgbuf = PTR_ALIGN (log_pgbuf, MAX_ALIGNMENT);
  log_pgptr = (LOG_PAGE *) aligned_log_pgbuf;

  LSA_COPY (&end_postpone_lsa, &tdes->last_lsa);
  LSA_COPY (&next_start_seek_lsa, start_postpone_lsa);

  nxtop_stack = nxtop_array;
  nxtop_count = log_get_next_nested_top (thread_p, tdes, start_postpone_lsa,
					 &nxtop_stack);

  while (!LSA_ISNULL (&next_start_seek_lsa))
    {
      LSA_COPY (&start_seek_lsa, &next_start_seek_lsa);

      if (nxtop_count > 0)
	{
	  nxtop_count--;
	  nxtop_range = &(nxtop_stack[nxtop_count]);

	  if (LSA_LT (&start_seek_lsa, &(nxtop_range->start_lsa)))
	    {
	      end_seek_lsa = &(nxtop_range->start_lsa);
	      LSA_COPY (&next_start_seek_lsa, &(nxtop_range->end_lsa));
	    }
	  else if (LSA_EQ (&start_seek_lsa, &(nxtop_range->end_lsa)))
	    {
	      end_seek_lsa = &end_postpone_lsa;
	      LSA_SET_NULL (&next_start_seek_lsa);
	    }
	  else
	    {
	      LSA_COPY (&next_start_seek_lsa, &(nxtop_range->end_lsa));
	      continue;
	    }
	}
      else
	{
	  end_seek_lsa = &end_postpone_lsa;
	  LSA_SET_NULL (&next_start_seek_lsa);
	}

      /*
       * Start doing postpone operation for this range
       */

      LSA_COPY (&forward_lsa, &start_seek_lsa);

      isdone = false;
      while (!LSA_ISNULL (&forward_lsa) && !isdone)
	{
	  /* Fetch the page where the postpone LSA record is located */
	  log_lsa.pageid = forward_lsa.pageid;
	  if (logpb_fetch_page (thread_p, log_lsa.pageid, log_pgptr) == NULL)
	    {
	      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
				 "log_recovery_find_first_postpone");
	      goto end;
	    }

	  while (forward_lsa.pageid == log_lsa.pageid && !isdone)
	    {
	      if (LSA_GT (&forward_lsa, end_seek_lsa))
		{
		  /* Finish at this point */
		  isdone = true;
		  break;
		}
	      /*
	       * If an offset is missing, it is because we archive an incomplete
	       * log record. This log_record was completed later.
	       * Thus, we have to find the offset by searching
	       * for the next log_record in the page.
	       */
	      if (forward_lsa.offset == NULL_OFFSET)
		{
		  forward_lsa.offset = log_pgptr->hdr.offset;
		  if (forward_lsa.offset == NULL_OFFSET)
		    {
		      /* Continue at next pageid */
		      if (logpb_is_page_in_archive (log_lsa.pageid))
			{
			  forward_lsa.pageid = log_lsa.pageid + 1;
			}
		      else
			{
			  forward_lsa.pageid = NULL_PAGEID;
			}
		      continue;
		    }
		}

	      log_lsa.offset = forward_lsa.offset;
	      log_rec = LOG_GET_LOG_RECORD_HEADER (log_pgptr, &log_lsa);

	      /* Find the next log record in the log */
	      LSA_COPY (&forward_lsa, &log_rec->forw_lsa);

	      if (forward_lsa.pageid == NULL_PAGEID
		  && logpb_is_page_in_archive (log_lsa.pageid))
		{
		  forward_lsa.pageid = log_lsa.pageid + 1;
		}

	      if (log_rec->trid == tdes->trid)
		{
		  switch (log_rec->type)
		    {
		    case LOG_RUN_POSTPONE:
		      LOG_READ_ADD_ALIGN (thread_p,
					  sizeof (LOG_RECORD_HEADER),
					  &log_lsa, log_pgptr);

		      LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p,
							sizeof (struct
								log_run_postpone),
							&log_lsa, log_pgptr);

		      run_posp =
			(struct log_run_postpone *) ((char *) log_pgptr->
						     area + log_lsa.offset);

		      if (LSA_EQ (start_postpone_lsa, &run_posp->ref_lsa))
			{
			  /* run_postpone_log of start_postpone is found,
			   * next_postpone_lsa is the first postpone
			   * to be applied.
			   */
			  start_postpone_lsa_wasapplied = true;
			  isdone = true;
			}
		      break;

		    case LOG_POSTPONE:
		      if (LSA_ISNULL (&next_postpone_lsa)
			  && !LSA_EQ (start_postpone_lsa, &log_lsa))
			{
			  /* remember next postpone_lsa */
			  LSA_COPY (&next_postpone_lsa, &log_lsa);
			}
		      break;

		    case LOG_END_OF_LOG:
		      if (forward_lsa.pageid == NULL_PAGEID
			  && logpb_is_page_in_archive (log_lsa.pageid))
			{
			  forward_lsa.pageid = log_lsa.pageid + 1;
			}
		      break;

		    default:
		      break;
		    }
		}

	      /*
	       * We can fix the lsa.pageid in the case of log_records without
	       * forward address at this moment.
	       */

	      if (forward_lsa.offset == NULL_OFFSET
		  && forward_lsa.pageid != NULL_PAGEID
		  && forward_lsa.pageid < log_lsa.pageid)
		{
		  forward_lsa.pageid = log_lsa.pageid;
		}
	    }
	}
    }

end:
  if (nxtop_stack != nxtop_array && nxtop_stack != NULL)
    {
      free_and_init (nxtop_stack);
    }

  if (start_postpone_lsa_wasapplied == false)
    {
      LSA_COPY (ret_lsa, start_postpone_lsa);
    }
  else
    {
      LSA_COPY (ret_lsa, &next_postpone_lsa);
    }

  return NO_ERROR;
}
