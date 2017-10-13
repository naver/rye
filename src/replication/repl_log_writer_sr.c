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
 * repl_log_writer.c -
 */

#ident "$Id$"

#include "config.h"

#include <assert.h>

#include "page_buffer.h"
#include "log_impl.h"
#include "memory_alloc.h"
#include "server_support.h"
#include "thread.h"
#include "network_interface_sr.h"
#include "repl_log_writer_sr.h"

#include "rye_server_shm.h"

#define LOGWR_THREAD_SUSPEND_TIMEOUT 	10

static int logwr_register_writer_entry (LOGWR_ENTRY ** wr_entry_p,
					THREAD_ENTRY * thread_p,
					LOG_PAGEID fpageid, int mode);
static bool logwr_unregister_writer_entry (LOGWR_ENTRY * wr_entry,
					   int status);
static int logwr_pack_log_pages (THREAD_ENTRY * thread_p, char *logpg_area,
				 LOG_ZIP * zip_logpg, int *logpg_used_size,
				 int *status, LOGWR_ENTRY * entry,
				 INT64 * send_pageid, int *num_page,
				 int *file_status);
static void logwr_write_end (THREAD_ENTRY * thread_p,
			     LOGWR_INFO * writer_info, LOGWR_ENTRY * entry,
			     int status);
static void logwr_set_eof_lsa (THREAD_ENTRY * thread_p, LOGWR_ENTRY * entry);
static bool logwr_is_delayed (THREAD_ENTRY * thread_p, LOGWR_ENTRY * entry);
static void logwr_update_last_sent_eof_lsa (LOGWR_ENTRY * entry);

/*
 * logwr_register_writer_entry -
 *
 * return:
 *
 *   wr_entry_p(out):
 *   id(in):
 *   fpageid(in):
 *   mode(in):
 *
 * Note:
 */
static int
logwr_register_writer_entry (LOGWR_ENTRY ** wr_entry_p,
			     THREAD_ENTRY * thread_p,
			     LOG_PAGEID fpageid, int mode)
{
  LOGWR_ENTRY *entry;
  int rv;
  LOGWR_INFO *writer_info = &log_Gl.writer_info;

  *wr_entry_p = NULL;
  rv = pthread_mutex_lock (&writer_info->wr_list_mutex);

  entry = writer_info->writer_list;
  while (entry)
    {
      if (entry->thread_p == thread_p)
	{
	  break;
	}
      entry = entry->next;
    }

  if (entry == NULL)
    {
      entry = malloc (sizeof (LOGWR_ENTRY));
      if (entry == NULL)
	{
	  pthread_mutex_unlock (&writer_info->wr_list_mutex);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, sizeof (LOGWR_ENTRY));
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      entry->thread_p = thread_p;
      entry->fpageid = fpageid;
      entry->mode = mode;
      entry->start_copy_time = 0;

      entry->status = LOGWR_STATUS_DELAY;
      LSA_SET_NULL (&entry->eof_lsa);
      LSA_SET_NULL (&entry->last_sent_eof_lsa);
      LSA_SET_NULL (&entry->tmp_last_sent_eof_lsa);

      entry->next = writer_info->writer_list;
      writer_info->writer_list = entry;
    }
  else
    {
      entry->fpageid = fpageid;
      entry->mode = mode;
      if (entry->status != LOGWR_STATUS_DELAY)
	{
	  entry->status = LOGWR_STATUS_WAIT;
	  entry->start_copy_time = 0;
	}
    }

  pthread_mutex_unlock (&writer_info->wr_list_mutex);
  *wr_entry_p = entry;

  return NO_ERROR;
}

/*
 * logwr_unregister_writer_entry -
 *
 * return:
 *
 *   wr_entry(in):
 *   status(in):
 *
 * Note:
 */
static bool
logwr_unregister_writer_entry (LOGWR_ENTRY * wr_entry, int status)
{
  LOGWR_ENTRY *entry;
  bool is_all_done;
  int rv;
  LOGWR_INFO *writer_info = &log_Gl.writer_info;

  rv = pthread_mutex_lock (&writer_info->wr_list_mutex);

  wr_entry->status = status;

  entry = writer_info->writer_list;
  while (entry)
    {
      if (entry->status == LOGWR_STATUS_FETCH)
	{
	  break;
	}
      entry = entry->next;
    }

  is_all_done = (entry == NULL) ? true : false;

  if (status == LOGWR_STATUS_ERROR)
    {
      LOGWR_ENTRY *prev_entry = NULL;
      entry = writer_info->writer_list;
      while (entry)
	{
	  if (entry == wr_entry)
	    {
	      if (entry == writer_info->writer_list)
		{
		  writer_info->writer_list = entry->next;
		}
	      else
		{
		  prev_entry->next = entry->next;
		}
	      free_and_init (entry);
	      break;
	    }
	  prev_entry = entry;
	  entry = entry->next;
	}
    }
  pthread_mutex_unlock (&writer_info->wr_list_mutex);

  return is_all_done;
}

/*
 * logwr_pack_log_pages -
 *
 * return:
 *
 *   thread_p(in):
 *   logpg_area(in):
 *   zip_logpg(in):
 *   logpg_used_size(out):
 *   status(out): LOGWR_STATUS_DONE, LOGWR_STATUS_DELAY or LOGWR_STATUS_ERROR
 *   entry(in):
 *   send_pageid(out):
 *   num_page(out):
 *   file_status(out):
 *
 * Note:
 */
static int
logwr_pack_log_pages (THREAD_ENTRY * thread_p,
		      char *logpg_area, LOG_ZIP * zip_logpg,
		      int *logpg_used_size, int *status, LOGWR_ENTRY * entry,
		      INT64 * send_pageid, int *num_page, int *file_status)
{
  LOG_PAGEID fpageid, lpageid, pageid;
  char *p;
  int area_size;
  LOG_PAGE *log_pgptr;
  INT64 num_logpgs;
  bool is_hdr_page_only;
  int ha_file_status;
  int error_code;

  struct log_arv_header arvhdr;
  struct log_header *hdr_ptr;
  LOG_LSA eof_lsa;

  fpageid = NULL_PAGEID;
  lpageid = NULL_PAGEID;
  ha_file_status = LOG_HA_FILESTAT_CLEAR;

  is_hdr_page_only = (entry->fpageid == LOGPB_HEADER_PAGE_ID);

  LOG_CS_ENTER_READ_MODE (thread_p);

  if (LSA_ISNULL (&entry->eof_lsa))
    {
      LSA_COPY (&eof_lsa, &log_Gl.hdr.eof_lsa);
    }
  else
    {
      LSA_COPY (&eof_lsa, &entry->eof_lsa);
    }

  if (!is_hdr_page_only)
    {
      /* Find the first pageid to be packed */
      fpageid = entry->fpageid;
      if (fpageid <= 0)
	{
	  /* In case of first request(copy archive) from the log writer,
	     pack all archive pages to be flushed until now */
	  fpageid = -fpageid;
	  if (logpb_is_page_in_archive (fpageid))
	    {
	      if (logpb_fetch_from_archive (thread_p, fpageid, NULL, NULL,
					    &arvhdr, false) == NULL)
		{
		  error_code = ER_FAILED;
		  LOG_CS_EXIT ();
		  goto error;
		}
	      fpageid = arvhdr.fpageid;
	      log_Gl.hdr.ha_info.nxarv_num = arvhdr.arv_num;
	      log_Gl.hdr.ha_info.nxarv_pageid = fpageid;
	    }
	  else
	    {
	      fpageid = log_Gl.hdr.nxarv_pageid;
	      log_Gl.hdr.ha_info.nxarv_num = log_Gl.hdr.nxarv_num;
	      log_Gl.hdr.ha_info.nxarv_pageid = fpageid;
	    }
	}

      /* Find the last pageid which is bounded by several limitations */
      if (!logpb_is_page_in_archive (fpageid))
	{
	  lpageid = eof_lsa.pageid;
	}
      else
	{
	  /* If the fpageid is in archive log,
	     fetch the page and the header page in the archive */

	  if (logpb_fetch_from_archive (thread_p, fpageid, NULL, NULL,
					&arvhdr, false) == NULL)
	    {
	      error_code = ER_FAILED;
	      LOG_CS_EXIT ();
	      goto error;
	    }

	  /* Reset the lpageid with the last pageid in the archive */
	  lpageid = arvhdr.fpageid + arvhdr.npages - 1;
	  assert (lpageid < eof_lsa.pageid);

	  if (fpageid == arvhdr.fpageid)
	    {
	      ha_file_status = LOG_HA_FILESTAT_ARCHIVED;
	    }
	}

      /* Pack the pages which can be in the page area of Log Writer */
      if ((lpageid - fpageid + 1) > (LOGWR_COPY_LOG_BUFFER_NPAGES - 1))
	{
	  lpageid = fpageid + (LOGWR_COPY_LOG_BUFFER_NPAGES - 1) - 1;
	}
      if (lpageid == eof_lsa.pageid)
	{
	  ha_file_status = LOG_HA_FILESTAT_SYNCHRONIZED;
	}
    }

  /* Set the server status on the header information */
  log_Gl.hdr.ha_info.server_state = svr_shm_get_server_state ();
  log_Gl.hdr.ha_info.file_status = ha_file_status;

  /* Allocate the log page area */
  num_logpgs = (is_hdr_page_only) ? 1 : (int) ((lpageid - fpageid + 1) + 1);

  assert (lpageid >= fpageid);
  assert (num_logpgs <= LOGWR_COPY_LOG_BUFFER_NPAGES);
  assert (lpageid <= eof_lsa.pageid);

  p = logpg_area;

  /* Fill the header page */
  log_pgptr = (LOG_PAGE *) p;
  log_pgptr->hdr = log_Gl.loghdr_pgptr->hdr;
  memcpy (log_pgptr->area, &log_Gl.hdr, sizeof (log_Gl.hdr));

  hdr_ptr = (struct log_header *) (log_pgptr->area);
  LSA_COPY (&hdr_ptr->eof_lsa, &eof_lsa);

  p += LOG_PAGESIZE;

  LOG_CS_EXIT ();

  /* Fill the page array with the pages to send */
  if (!is_hdr_page_only)
    {
      for (pageid = fpageid; pageid >= 0 && pageid <= lpageid; pageid++)
	{
	  log_pgptr = (LOG_PAGE *) p;
	  if (logpb_copy_page_from_log_buffer
	      (thread_p, pageid, log_pgptr) == NULL)
	    {
	      error_code = ER_FAILED;
	      goto error;
	    }

	  assert (pageid == (log_pgptr->hdr.logical_pageid));
	  p += LOG_PAGESIZE;
	}
    }

  *logpg_used_size = area_size = (int) (p - logpg_area);

  if (zip_logpg != NULL && *logpg_used_size > 0)
    {
      bool is_zipped = false;

      is_zipped = log_zip (zip_logpg, *logpg_used_size, logpg_area);

      if (is_zipped)
	{
	  area_size = zip_logpg->data_length;
	  assert (area_size > 0);

	  er_log_debug (ARG_FILE_LINE,
			"logwr_pack_log_pages, LZO %d -> %d (%5.2f%%)\n",
			*logpg_used_size, area_size,
			((double) area_size / *logpg_used_size) * 100);

	  *logpg_used_size = MAKE_ZIP_LEN (area_size);
	  memcpy (logpg_area, zip_logpg->log_data, area_size);
	}
      else
	{
	  er_log_debug (ARG_FILE_LINE,
			"logwr_pack_log_pages, LZO %d -> %d (100.00%)\n",
			*logpg_used_size, *logpg_used_size);
	}
    }

  /* In case that EOL exists at lpageid */
  if (!is_hdr_page_only && (lpageid >= eof_lsa.pageid))
    {
      *status = LOGWR_STATUS_DONE;
      LSA_COPY (&entry->tmp_last_sent_eof_lsa, &eof_lsa);
    }
  else
    {
      *status = LOGWR_STATUS_DELAY;
      entry->tmp_last_sent_eof_lsa.pageid = lpageid;
      entry->tmp_last_sent_eof_lsa.offset = NULL_OFFSET;
    }
  if (send_pageid != NULL)
    {
      *send_pageid = (is_hdr_page_only) ? 0 : fpageid;
    }
  if (num_page != NULL)
    {
      *num_page = (is_hdr_page_only) ? 0 : (lpageid - fpageid + 1);
    }
  if (file_status != NULL)
    {
      *file_status = ha_file_status;
    }

  er_log_debug (ARG_FILE_LINE,
		"logwr_pack_log_pages, fpageid(%lld), lpageid(%lld), num_pages(%lld), area_size(%d)"
		"\n status(%d), delayed_free_log_pgptr(%p)\n",
		fpageid, lpageid, num_logpgs, area_size,
		entry->status, log_Gl.append.delayed_free_log_pgptr);

  return NO_ERROR;

error:

  *logpg_used_size = 0;
  *status = LOGWR_STATUS_ERROR;

  return error_code;
}

static void
logwr_write_end (THREAD_ENTRY * thread_p, LOGWR_INFO * writer_info,
		 LOGWR_ENTRY * entry, int status)
{
  int rv;
  int prev_status;
  INT64 saved_start_time;

  rv = pthread_mutex_lock (&writer_info->flush_end_mutex);

  prev_status = entry->status;
  saved_start_time = entry->start_copy_time;

  if (entry != NULL && logwr_unregister_writer_entry (entry, status))
    {
      if (prev_status == LOGWR_STATUS_FETCH
	  && writer_info->trace_last_writer == true)
	{
	  assert (saved_start_time > 0);
	  writer_info->last_writer_elapsed_time =
	    thread_get_log_clock_msec () - saved_start_time;

	  logtb_get_current_client_ids (thread_p,
					&writer_info->
					last_writer_client_info);
	}
      pthread_cond_signal (&writer_info->flush_end_cond);
    }
  pthread_mutex_unlock (&writer_info->flush_end_mutex);
  return;
}

static void
logwr_set_eof_lsa (THREAD_ENTRY * thread_p, LOGWR_ENTRY * entry)
{
  if (LSA_ISNULL (&entry->eof_lsa))
    {
      LOG_CS_ENTER (thread_p);
      LSA_COPY (&entry->eof_lsa, &log_Gl.hdr.eof_lsa);
      LOG_CS_EXIT ();
    }

  return;
}

static bool
logwr_is_delayed (THREAD_ENTRY * thread_p, LOGWR_ENTRY * entry)
{
  logwr_set_eof_lsa (thread_p, entry);

  if (entry == NULL
      || LSA_ISNULL (&entry->last_sent_eof_lsa)
      || LSA_GE (&entry->last_sent_eof_lsa, &entry->eof_lsa))
    {
      return false;
    }
  return true;
}

static void
logwr_update_last_sent_eof_lsa (LOGWR_ENTRY * entry)
{
  if (entry)
    {
      LSA_COPY (&entry->last_sent_eof_lsa, &entry->tmp_last_sent_eof_lsa);
    }
  return;
}

/*
 * xlogwr_get_log_pages -
 *
 * return:
 *
 *   thread_p(in):
 *   first_pageid(in):
 *   mode(in):
 *   compressed_protocol(in):
 *
 * Note:
 */
int
xlogwr_get_log_pages (THREAD_ENTRY * thread_p, LOG_PAGEID first_pageid,
		      LOGWR_MODE mode, bool compressed_protocol)
{
  LOGWR_ENTRY *entry;
  char *logpg_area;
  LOG_ZIP *zip_logpg = NULL;
  int logpg_used_size;
  LOG_PAGEID next_fpageid;
  LOGWR_MODE next_mode;
  LOGWR_MODE orig_mode = LOGWR_MODE_ASYNC;
  int status;
  int timeout;
  int rv;
  int error_code;
  bool need_cs_exit_after_send = true;
  struct timespec to;
  LOGWR_INFO *writer_info = &log_Gl.writer_info;
  INT64 send_pageid = 0;
  int send_num_page = 0, ha_file_status = LOG_HA_FILESTAT_CLEAR;

  logpg_used_size = 0;
  logpg_area =
    (char *) malloc ((LOGWR_COPY_LOG_BUFFER_NPAGES * LOG_PAGESIZE));
  if (logpg_area == NULL)
    {
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  /* compressed_protocol is passed by client */
  if (compressed_protocol)
    {
      zip_logpg =
	log_zip_alloc ((LOGWR_COPY_LOG_BUFFER_NPAGES * LOG_PAGESIZE), true);
      if (zip_logpg == NULL)
	{
	  free_and_init (logpg_area);
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
    }
  else
    {
      zip_logpg = NULL;
    }

  if (thread_p->conn_entry)
    {
      thread_p->conn_entry->stop_phase = THREAD_STOP_LOGWR;
    }

  while (true)
    {
      /* In case that a non-ASYNC mode client internally uses ASYNC mode */
      orig_mode = MAX (mode, orig_mode);

      er_log_debug (ARG_FILE_LINE,
		    "[tid:%ld] xlogwr_get_log_pages, fpageid(%lld), mode(%s), comprssed_protocol(%d)\n",
		    thread_p->tid, first_pageid, LOGWR_MODE_NAME (mode),
		    compressed_protocol);

      /* Register the writer at the list and wait until LFT start to work */
      rv = pthread_mutex_lock (&writer_info->flush_start_mutex);
      error_code = logwr_register_writer_entry (&entry, thread_p,
						first_pageid, mode);
      if (error_code != NO_ERROR)
	{
	  pthread_mutex_unlock (&writer_info->flush_start_mutex);
	  status = LOGWR_STATUS_ERROR;
	  goto error;
	}

      if (entry->status == LOGWR_STATUS_WAIT)
	{
	  bool continue_checking = true;

	  if (mode == LOGWR_MODE_ASYNC)
	    {
	      timeout = LOGWR_THREAD_SUSPEND_TIMEOUT;
	      to.tv_sec = time (NULL) + timeout;
	      to.tv_nsec = 0;
	    }
	  else
	    {
	      timeout = INF_WAIT;
	      to.tv_sec = to.tv_nsec = 0;
	    }

	  rv = thread_suspend_with_other_mutex (thread_p,
						&writer_info->
						flush_start_mutex, timeout,
						&to, THREAD_LOGWR_SUSPENDED);
	  if (rv == ER_CSS_PTHREAD_COND_TIMEDOUT)
	    {
	      pthread_mutex_unlock (&writer_info->flush_start_mutex);

	      rv = pthread_mutex_lock (&writer_info->flush_end_mutex);
	      if (logwr_unregister_writer_entry (entry, LOGWR_STATUS_DELAY))
		{
		  pthread_cond_signal (&writer_info->flush_end_cond);
		}
	      pthread_mutex_unlock (&writer_info->flush_end_mutex);

	      continue;
	    }
	  else if (rv == ER_CSS_PTHREAD_MUTEX_LOCK
		   || rv == ER_CSS_PTHREAD_MUTEX_UNLOCK
		   || rv == ER_CSS_PTHREAD_COND_WAIT)
	    {
	      pthread_mutex_unlock (&writer_info->flush_start_mutex);

	      error_code = ER_FAILED;
	      status = LOGWR_STATUS_ERROR;
	      goto error;
	    }

	  pthread_mutex_unlock (&writer_info->flush_start_mutex);

	  if (logtb_is_interrupted (thread_p, false, &continue_checking))
	    {
	      /* interrupted, shutdown or connection has gone. */
	      error_code = ER_INTERRUPTED;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 0);
	      status = LOGWR_STATUS_ERROR;
	      goto error;
	    }
	  else if (thread_p->resume_status == THREAD_RESUME_DUE_TO_INTERRUPT)
	    {
	      if (logwr_is_delayed (thread_p, entry))
		{
		  logwr_write_end (thread_p, writer_info, entry,
				   LOGWR_STATUS_DELAY);
		  continue;
		}

	      error_code = ER_INTERRUPTED;
	      status = LOGWR_STATUS_ERROR;
	      goto error;
	    }
	  else if (thread_p->resume_status != THREAD_LOGWR_RESUMED)
	    {
	      error_code = ER_FAILED;
	      status = LOGWR_STATUS_ERROR;
	      goto error;
	    }
	}
      else
	{
	  assert (entry->status == LOGWR_STATUS_DELAY);
	  pthread_mutex_unlock (&writer_info->flush_start_mutex);
	}

      if (thread_p->resume_status == THREAD_RESUME_DUE_TO_INTERRUPT)
	{
	  logwr_set_eof_lsa (thread_p, entry);
	}

      /* Send the log pages to be flushed until now */
      error_code = logwr_pack_log_pages (thread_p, logpg_area, zip_logpg,
					 &logpg_used_size, &status, entry,
					 &send_pageid, &send_num_page,
					 &ha_file_status);
      if (error_code != NO_ERROR)
	{
	  error_code = ER_HA_LW_FAILED_GET_LOG_PAGE;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  error_code, 1, first_pageid);

	  status = LOGWR_STATUS_ERROR;
	  goto error;
	}

      /* wait until LFT finishes flushing */
      rv = pthread_mutex_lock (&writer_info->flush_wait_mutex);

      if (entry->status == LOGWR_STATUS_FETCH
	  && writer_info->flush_completed == false)
	{
	  rv =
	    pthread_cond_wait (&writer_info->flush_wait_cond,
			       &writer_info->flush_wait_mutex);
	  assert_release (writer_info->flush_completed == true);
	}
      rv = pthread_mutex_unlock (&writer_info->flush_wait_mutex);

      if (entry->status == LOGWR_STATUS_FETCH)
	{
	  rv = pthread_mutex_lock (&writer_info->wr_list_mutex);
	  entry->start_copy_time = thread_get_log_clock_msec ();
	  pthread_mutex_unlock (&writer_info->wr_list_mutex);
	}

      /* In case of async mode, unregister the writer and wakeup LFT to finish */
      /*
         The result mode is the following.

         transition \ req mode |  req_sync   req_async
         -----------------------------------------
         delay -> delay    |  n/a        ASYNC
         delay -> done     |  n/a        SYNC
         wait -> delay     |  SYNC       ASYNC
         wait -> done      |  SYNC       ASYNC
       */

      if (orig_mode == LOGWR_MODE_ASYNC
	  || (mode == LOGWR_MODE_ASYNC &&
	      (entry->status != LOGWR_STATUS_DELAY
	       || status != LOGWR_STATUS_DONE)))
	{
	  logwr_write_end (thread_p, writer_info, entry, status);
	  need_cs_exit_after_send = false;
	}

      error_code = xlog_send_log_pages_to_client (thread_p, logpg_area,
						  logpg_used_size,
						  send_pageid, send_num_page,
						  ha_file_status);
      if (error_code != NO_ERROR)
	{
	  status = LOGWR_STATUS_ERROR;
	  goto error;
	}

      /* Get the next request from the client and reset the arguments */
      error_code = xlog_get_page_request_with_reply (thread_p, &next_fpageid,
						     &next_mode);
      if (error_code != NO_ERROR)
	{
	  status = LOGWR_STATUS_ERROR;
	  goto error;
	}

      logwr_update_last_sent_eof_lsa (entry);

      /* In case of sync mode, unregister the writer and wakeup LFT to finish */
      if (need_cs_exit_after_send)
	{
	  logwr_write_end (thread_p, writer_info, entry, status);
	}

      /* Reset the arguments for the next request */
      first_pageid = next_fpageid;
      mode = next_mode;
      need_cs_exit_after_send = true;
    }

  free_and_init (logpg_area);
  if (zip_logpg != NULL)
    {
      log_zip_free (zip_logpg);
      zip_logpg = NULL;
    }

  assert_release (false);
  return ER_FAILED;

error:

  er_log_debug (ARG_FILE_LINE,
		"[tid:%ld] xlogwr_get_log_pages, error(%d)\n",
		thread_p->tid, error_code);

  logwr_write_end (thread_p, writer_info, entry, status);

  free_and_init (logpg_area);
  if (zip_logpg != NULL)
    {
      log_zip_free (zip_logpg);
      zip_logpg = NULL;
    }

  return error_code;
}

/*
 * logwr_get_min_copied_fpageid -
 *
 * return:
 *
 * Note:
 */
LOG_PAGEID
logwr_get_min_copied_fpageid (void)
{
  LOGWR_INFO *writer_info = &log_Gl.writer_info;
  LOGWR_ENTRY *entry;
  int num_entries = 0;
  LOG_PAGEID min_fpageid = LOGPAGEID_MAX;
  int rv;

  rv = pthread_mutex_lock (&writer_info->wr_list_mutex);

  entry = writer_info->writer_list;
  while (entry)
    {
      if (min_fpageid > entry->fpageid)
	{
	  min_fpageid = entry->fpageid;
	}
      entry = entry->next;
      num_entries++;
    }

  pthread_mutex_unlock (&writer_info->wr_list_mutex);

  if (min_fpageid == LOGPAGEID_MAX || min_fpageid == LOGPB_HEADER_PAGE_ID)
    {
      min_fpageid = NULL_PAGEID;
    }

  return (min_fpageid);
}

static void
logwr_remove_writer_entry (LOGWR_ENTRY * wr_entry)
{
  LOGWR_INFO *writer_info = &log_Gl.writer_info;
  LOGWR_ENTRY *entry, *prev_entry;
  int rv;

  rv = pthread_mutex_lock (&writer_info->wr_list_mutex);

  entry = writer_info->writer_list;
  while (entry)
    {
      if (entry == wr_entry)
	{
	  if (entry == writer_info->writer_list)
	    {
	      writer_info->writer_list = entry->next;
	    }
	  else
	    {
	      prev_entry->next = entry->next;
	    }
	  free_and_init (entry);
	  break;
	}

      prev_entry = entry;
      entry = entry->next;
    }

  pthread_mutex_unlock (&writer_info->wr_list_mutex);
}

/*
 * xmigrator_get_log_pages -
 *
 * return:
 *
 *   thread_p(in):
 *   first_pageid(in):
 *   compressed_protocol(in):
 *
 * Note:
 */
int
xmigrator_get_log_pages (THREAD_ENTRY * thread_p, LOG_PAGEID first_pageid,
			 bool compressed_protocol)
{
  LOGWR_ENTRY *entry;
  char *logpg_area;
  LOG_ZIP *zip_logpg = NULL;
  int logpg_used_size;
  LOGWR_MODE mode = LOGWR_MODE_ASYNC;
  int status;
  int timeout;
  int rv;
  int error_code;
  struct timespec to;
  LOGWR_INFO *writer_info = &log_Gl.writer_info;

  logpg_used_size = 0;
  logpg_area =
    (char *) malloc ((LOGWR_COPY_LOG_BUFFER_NPAGES * LOG_PAGESIZE));
  if (logpg_area == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      (LOGWR_COPY_LOG_BUFFER_NPAGES * LOG_PAGESIZE));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  /* compressed_protocol is passed by client */
  if (compressed_protocol)
    {
      zip_logpg =
	log_zip_alloc ((LOGWR_COPY_LOG_BUFFER_NPAGES * LOG_PAGESIZE), true);
      if (zip_logpg == NULL)
	{
	  free_and_init (logpg_area);
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
    }
  else
    {
      zip_logpg = NULL;
    }

  if (thread_p->conn_entry)
    {
      thread_p->conn_entry->stop_phase = THREAD_STOP_LOGWR;
    }

  while (true)
    {
      er_log_debug (ARG_FILE_LINE,
		    "[tid:%ld] xmigrator_get_log_pages, fpageid(%lld), mode(%s), compressed_protocol(%d)\n",
		    thread_p->tid, first_pageid, LOGWR_MODE_NAME (mode),
		    compressed_protocol);

      /* Register the writer at the list and wait until LFT start to work */
      rv = pthread_mutex_lock (&writer_info->flush_start_mutex);
      error_code = logwr_register_writer_entry (&entry, thread_p,
						first_pageid, mode);
      if (error_code != NO_ERROR)
	{
	  pthread_mutex_unlock (&writer_info->flush_start_mutex);
	  status = LOGWR_STATUS_ERROR;
	  goto error;
	}

      if (entry->status == LOGWR_STATUS_WAIT)
	{
	  bool continue_checking = true;

	  if (mode == LOGWR_MODE_ASYNC)
	    {
	      timeout = LOGWR_THREAD_SUSPEND_TIMEOUT;
	      to.tv_sec = time (NULL) + timeout;
	      to.tv_nsec = 0;
	    }
	  else
	    {
	      timeout = INF_WAIT;
	      to.tv_sec = to.tv_nsec = 0;
	    }

	  rv = thread_suspend_with_other_mutex (thread_p,
						&writer_info->
						flush_start_mutex, timeout,
						&to, THREAD_LOGWR_SUSPENDED);
	  if (rv == ER_CSS_PTHREAD_COND_TIMEDOUT)
	    {
	      pthread_mutex_unlock (&writer_info->flush_start_mutex);

	      rv = pthread_mutex_lock (&writer_info->flush_end_mutex);
	      if (logwr_unregister_writer_entry (entry, LOGWR_STATUS_DELAY))
		{
		  pthread_cond_signal (&writer_info->flush_end_cond);
		}
	      pthread_mutex_unlock (&writer_info->flush_end_mutex);

	      continue;
	    }
	  else if (rv == ER_CSS_PTHREAD_MUTEX_LOCK
		   || rv == ER_CSS_PTHREAD_MUTEX_UNLOCK
		   || rv == ER_CSS_PTHREAD_COND_WAIT)
	    {
	      pthread_mutex_unlock (&writer_info->flush_start_mutex);

	      error_code = ER_FAILED;
	      status = LOGWR_STATUS_ERROR;
	      goto error;
	    }

	  pthread_mutex_unlock (&writer_info->flush_start_mutex);

	  if (logtb_is_interrupted (thread_p, false, &continue_checking))
	    {
	      /* interrupted, shutdown or connection has gone. */
	      error_code = ER_INTERRUPTED;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 0);
	      status = LOGWR_STATUS_ERROR;
	      goto error;
	    }
	  else if (thread_p->resume_status == THREAD_RESUME_DUE_TO_INTERRUPT)
	    {
	      if (logwr_is_delayed (thread_p, entry))
		{
		  logwr_write_end (thread_p, writer_info, entry,
				   LOGWR_STATUS_DELAY);
		  continue;
		}

	      error_code = ER_INTERRUPTED;
	      status = LOGWR_STATUS_ERROR;
	      goto error;
	    }
	  else if (thread_p->resume_status != THREAD_LOGWR_RESUMED)
	    {
	      error_code = ER_FAILED;
	      status = LOGWR_STATUS_ERROR;
	      goto error;
	    }
	}
      else
	{
	  assert (entry->status == LOGWR_STATUS_DELAY);
	  pthread_mutex_unlock (&writer_info->flush_start_mutex);
	}

      if (thread_p->resume_status == THREAD_RESUME_DUE_TO_INTERRUPT)
	{
	  logwr_set_eof_lsa (thread_p, entry);
	}

      /* Send the log pages to be flushed until now */
      error_code =
	logwr_pack_log_pages (thread_p, logpg_area, zip_logpg,
			      &logpg_used_size, &status, entry, NULL, NULL,
			      NULL);
      if (error_code != NO_ERROR)
	{
	  error_code = ER_HA_LW_FAILED_GET_LOG_PAGE;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1,
		  first_pageid);

	  status = LOGWR_STATUS_ERROR;
	  goto error;
	}

      /* wait until LFT finishes flushing */
      rv = pthread_mutex_lock (&writer_info->flush_wait_mutex);

      if (entry->status == LOGWR_STATUS_FETCH
	  && writer_info->flush_completed == false)
	{
	  rv =
	    pthread_cond_wait (&writer_info->flush_wait_cond,
			       &writer_info->flush_wait_mutex);
	  assert_release (writer_info->flush_completed == true);
	}
      rv = pthread_mutex_unlock (&writer_info->flush_wait_mutex);

      if (entry->status == LOGWR_STATUS_FETCH)
	{
	  rv = pthread_mutex_lock (&writer_info->wr_list_mutex);
	  entry->start_copy_time = thread_get_log_clock_msec ();
	  pthread_mutex_unlock (&writer_info->wr_list_mutex);
	}

      assert (entry->status != LOGWR_STATUS_ERROR);
      logwr_write_end (thread_p, writer_info, entry, status);

      error_code = xlog_send_log_pages_to_migrator (thread_p, logpg_area,
						    logpg_used_size, mode);
      if (error_code != NO_ERROR)
	{
	  status = LOGWR_STATUS_ERROR;
	  goto error;
	}

      logwr_remove_writer_entry (entry);
      break;
    }

  free_and_init (logpg_area);
  if (zip_logpg != NULL)
    {
      log_zip_free (zip_logpg);
      zip_logpg = NULL;
    }

  return NO_ERROR;

error:

  er_log_debug (ARG_FILE_LINE,
		"[tid:%ld] xmigrator_get_log_pages, error(%d)\n",
		thread_p->tid, error_code);

  logwr_write_end (thread_p, writer_info, entry, status);

  free_and_init (logpg_area);
  if (zip_logpg != NULL)
    {
      log_zip_free (zip_logpg);
      zip_logpg = NULL;
    }

  return error_code;
}
