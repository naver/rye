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
					LOGWR_INFO * writer_info,
					THREAD_ENTRY * thread_p,
					LOG_PAGEID fpageid);
static int logwr_pack_log_pages (THREAD_ENTRY * thread_p, char *logpg_area,
				 LOG_ZIP * zip_logpg, int *logpg_used_size,
				 int *status, LOGWR_ENTRY * entry,
				 INT64 * eof_pageid, INT64 * send_pageid,
				 int *num_page,
				 LOG_HA_FILESTAT * file_status);
static void logwr_write_start (LOGWR_INFO * writer_info, LOGWR_ENTRY * entry);
static void logwr_write_end (THREAD_ENTRY * thread_p,
			     LOGWR_INFO * writer_info, LOGWR_ENTRY * entry);
static void logwr_unregister_writer_entry (LOGWR_INFO * writer_info,
					   LOGWR_ENTRY * wr_entry);


/*
 * logwr_register_writer_entry -
 *
 * return:
 *
 *   wr_entry_p(out):
 *   writer_info(in/out):
 *   thread_p(in):
 *   fpageid(in):
 *
 * Note:
 */
static int
logwr_register_writer_entry (LOGWR_ENTRY ** wr_entry_p,
			     LOGWR_INFO * writer_info,
			     THREAD_ENTRY * thread_p, LOG_PAGEID fpageid)
{
  LOGWR_ENTRY *entry;

  *wr_entry_p = NULL;

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
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, sizeof (LOGWR_ENTRY));
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      entry->thread_p = thread_p;
      entry->fpageid = fpageid;
      entry->start_copy_time = 0;

      entry->status = LOGWR_STATUS_DELAY;
      LSA_SET_NULL (&entry->last_sent_eof_lsa);

      entry->next = writer_info->writer_list;
      writer_info->writer_list = entry;
    }
  else
    {
      entry->fpageid = fpageid;
      if (entry->status == LOGWR_STATUS_DONE)
	{
	  entry->status = LOGWR_STATUS_WAIT;
	  entry->start_copy_time = 0;
	}
    }

  *wr_entry_p = entry;

  return NO_ERROR;
}

/*
 * logwr_unregister_writer_entry
 */
static void
logwr_unregister_writer_entry (LOGWR_INFO * writer_info,
			       LOGWR_ENTRY * wr_entry)
{
  LOGWR_ENTRY *entry, *prev_entry;

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
		      INT64 * eof_pageid, INT64 * send_pageid, int *num_page,
		      LOG_HA_FILESTAT * file_status)
{
  LOG_PAGEID fpageid, lpageid, pageid;
  char *p;
  int area_size;
  LOG_PAGE *log_pgptr;
  INT64 num_logpgs;
  bool is_hdr_page_only;
  LOG_HA_FILESTAT ha_file_status;
  int error_code;

  struct log_arv_header arvhdr;
  struct log_header *hdr_ptr;
  LOG_LSA eof_lsa;

  fpageid = NULL_PAGEID;
  lpageid = NULL_PAGEID;
  ha_file_status = LOG_HA_FILESTAT_CLEAR;

  is_hdr_page_only = (entry->fpageid == LOGPB_HEADER_PAGE_ID);

  LOG_CS_ENTER_READ_MODE (thread_p);

  LSA_COPY (&eof_lsa, &log_Gl.hdr.eof_lsa);

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
	  LSA_COPY (&entry->last_sent_eof_lsa, &eof_lsa);
	}
    }

  /* Set the server status on the header information */
  log_Gl.hdr.ha_info.server_state = svr_shm_get_server_state ();
  log_Gl.hdr.ha_info.file_status = (int) ha_file_status;

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
    }
  else
    {
      *status = LOGWR_STATUS_DELAY;
    }
  if (send_pageid != NULL)
    {
      *send_pageid = (is_hdr_page_only) ? 0 : fpageid;
    }
  if (eof_pageid != NULL)
    {
      *eof_pageid = eof_lsa.pageid;
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
		"logwr_pack_log_pages, fpageid(%lld), lpageid(%lld), "
		"num_pages(%lld), status:%s, fa_file_status:%s, "
		"send eof(%ld,%d)\n",
		fpageid, lpageid, num_logpgs,
		LOGWR_STATUS_NAME (*status),
		LOG_HA_FILESTAT_NAME (ha_file_status),
		eof_lsa.pageid, eof_lsa.offset);

  return NO_ERROR;

error:

  *logpg_used_size = 0;
  *status = LOGWR_STATUS_ERROR;

  return error_code;
}

/*
 * logwr_write_start ()
 *   return:
 *
 *   writer_info(in):
 *   entry(in/out):
 */
static void
logwr_write_start (LOGWR_INFO * writer_info, LOGWR_ENTRY * entry)
{
  if (entry == NULL)
    {
      assert (false);
      return;
    }

  if (entry->status == LOGWR_STATUS_FETCH
      && writer_info->trace_last_writer == true)
    {
      entry->start_copy_time = thread_get_log_clock_msec ();
    }
}

/*
 * logwr_write_end ()
 *    return:
 *
 *    writer_info(in):
 *    entry(in/out):
 */
static void
logwr_write_end (THREAD_ENTRY * thread_p, LOGWR_INFO * writer_info,
		 LOGWR_ENTRY * entry)
{
  if (entry == NULL)
    {
      assert (false);
      return;
    }

  if (entry->status == LOGWR_STATUS_FETCH
      && writer_info->trace_last_writer == true)
    {
      assert (entry->start_copy_time > 0);

      writer_info->last_writer_elapsed_time =
	thread_get_log_clock_msec () - entry->start_copy_time;

      logtb_get_current_client_ids (thread_p,
				    &writer_info->last_writer_client_info);
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
 *   compressed_protocol(in):
 *
 * Note:
 */
int
xlogwr_get_log_pages (THREAD_ENTRY * thread_p, LOG_PAGEID first_pageid,
		      bool compressed_protocol)
{
  LOGWR_ENTRY *entry = NULL;
  char *logpg_area;
  LOG_ZIP *zip_logpg = NULL;
  int logpg_used_size;
  LOG_PAGEID next_fpageid;
  int status;
  int rv;
  int error_code;
  LOGWR_INFO *writer_info = &log_Gl.writer_info;
  INT64 send_pageid = 0, eof_pageid = 0;
  int send_num_page = 0;
  LOG_HA_FILESTAT ha_file_status = LOG_HA_FILESTAT_CLEAR;

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

  entry = NULL;
  while (true)
    {
      /* Register the writer at the list and wait until LFT start to work */
      rv = pthread_mutex_lock (&writer_info->wr_list_mutex);

      error_code = logwr_register_writer_entry (&entry, writer_info, thread_p,
						first_pageid);
      if (error_code != NO_ERROR)
	{
	  pthread_mutex_unlock (&writer_info->wr_list_mutex);
	  status = LOGWR_STATUS_ERROR;
	  GOTO_EXIT_ON_ERROR;
	}
      er_log_debug (ARG_FILE_LINE,
		    "[tid:%ld] xlogwr_get_log_pages, fpageid(%lld),"
		    " entry->statue(%s), comprssed_protocol(%d)\n",
		    thread_p->tid, first_pageid,
		    LOGWR_STATUS_NAME (entry->status), compressed_protocol);

      assert (entry->status == LOGWR_STATUS_DELAY
	      || entry->status == LOGWR_STATUS_WAIT
	      || entry->status == LOGWR_STATUS_FETCH);
      if (entry->status == LOGWR_STATUS_WAIT)
	{
	  rv = thread_suspend_with_other_mutex (thread_p,
						&writer_info->
						wr_list_mutex, INF_WAIT,
						NULL, THREAD_LOGWR_SUSPENDED);
	  if (rv != NO_ERROR
	      || thread_p->resume_status != THREAD_LOGWR_RESUMED)
	    {
	      pthread_mutex_unlock (&writer_info->wr_list_mutex);

	      error_code = (rv != NO_ERROR) ? rv : ER_FAILED;
	      status = LOGWR_STATUS_ERROR;
	      GOTO_EXIT_ON_ERROR;
	    }

	  if (entry->status != LOGWR_STATUS_FETCH)
	    {
	      pthread_mutex_unlock (&writer_info->wr_list_mutex);
	      continue;
	    }

	  logwr_write_start (writer_info, entry);
	}

      pthread_mutex_unlock (&writer_info->wr_list_mutex);

      /* Send the log pages to be flushed until now */
      error_code = logwr_pack_log_pages (thread_p, logpg_area, zip_logpg,
					 &logpg_used_size, &status, entry,
					 &eof_pageid, &send_pageid,
					 &send_num_page, &ha_file_status);
      if (error_code != NO_ERROR)
	{
	  error_code = ER_HA_LW_FAILED_GET_LOG_PAGE;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  error_code, 1, first_pageid);

	  status = LOGWR_STATUS_ERROR;
	  GOTO_EXIT_ON_ERROR;
	}

      /* wait until LFT finishes flushing */
      rv = pthread_mutex_lock (&writer_info->wr_list_mutex);

      while (entry->status == LOGWR_STATUS_FETCH
	     && writer_info->flush_completed == false)
	{
	  rv = pthread_cond_wait (&writer_info->wr_list_cond,
				  &writer_info->wr_list_mutex);
	}

      pthread_mutex_unlock (&writer_info->wr_list_mutex);

      error_code = xlog_send_log_pages_to_client (thread_p, logpg_area,
						  logpg_used_size,
						  eof_pageid, send_pageid,
						  send_num_page,
						  ha_file_status);
      if (error_code != NO_ERROR)
	{
	  status = LOGWR_STATUS_ERROR;
	  GOTO_EXIT_ON_ERROR;
	}

      /* Get the next request from the client and reset the arguments */
      error_code = xlog_get_page_request_with_reply (thread_p, &next_fpageid);
      if (error_code != NO_ERROR)
	{
	  status = LOGWR_STATUS_ERROR;
	  GOTO_EXIT_ON_ERROR;
	}

      if (status == LOGWR_STATUS_DONE)
	{
	  rv = pthread_mutex_lock (&writer_info->wr_list_mutex);

	  entry->status = status;
	  logwr_write_end (thread_p, writer_info, entry);

	  pthread_cond_signal (&writer_info->wr_list_cond);

	  pthread_mutex_unlock (&writer_info->wr_list_mutex);
	}

      /* Reset the arguments for the next request */
      first_pageid = next_fpageid;
    }

  assert (false);

exit_on_error:
  assert (error_code != NO_ERROR);

  er_log_debug (ARG_FILE_LINE,
		"[tid:%ld] xlogwr_get_log_pages, error(%d)\n",
		thread_p->tid, error_code);

  if (entry != NULL)
    {
      rv = pthread_mutex_lock (&writer_info->wr_list_mutex);

      entry->status = LOGWR_STATUS_ERROR;
      logwr_unregister_writer_entry (writer_info, entry);

      pthread_mutex_unlock (&writer_info->wr_list_mutex);
    }

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
  UNUSED_VAR int rv;

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

/*
 * logwr_find_copy_completed_entry -
 *
 * return:
 *
 * Note:
 */
LOGWR_ENTRY *
logwr_find_copy_completed_entry (LOGWR_INFO * writer_info)
{
  LOG_LSA *eof = &log_Gl.hdr.eof_lsa;
  LOGWR_ENTRY *entry, *found_entry;

  entry = writer_info->writer_list;
  found_entry = NULL;
  while (entry != NULL)
    {
      if ((entry->status == LOGWR_STATUS_WAIT
	   || entry->status == LOGWR_STATUS_DONE)
	  && LSA_GE (&entry->last_sent_eof_lsa, eof))
	{
	  found_entry = entry;
	  break;
	}
      entry = entry->next;
    }

  return found_entry;
}

/*
 * logwr_find_entry_status -
 *
 * return:
 *
 * Note:
 */
LOGWR_ENTRY *
logwr_find_entry_status (LOGWR_INFO * writer_info, LOGWR_STATUS status)
{
  LOGWR_ENTRY *entry, *found_entry;

  entry = writer_info->writer_list;
  found_entry = NULL;
  while (entry != NULL)
    {
      if (entry->status == status)
	{
	  found_entry = entry;
	  break;
	}
      entry = entry->next;
    }

  return found_entry;
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
  LOGWR_ENTRY *entry = NULL;
  char *logpg_area;
  LOG_ZIP *zip_logpg = NULL;
  int logpg_used_size;
  int status;
  UNUSED_VAR int rv;
  int error_code;
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

  if (thread_p->conn_entry != NULL)
    {
      thread_p->conn_entry->stop_phase = THREAD_STOP_LOGWR;
    }

  er_log_debug (ARG_FILE_LINE,
		"[tid:%ld] xmigrator_get_log_pages, fpageid(%lld), compressed_protocol(%d)\n",
		thread_p->tid, first_pageid, compressed_protocol);

  /* Register the writer at the list and wait until LFT start to work */
  rv = pthread_mutex_lock (&writer_info->wr_list_mutex);

  error_code = logwr_register_writer_entry (&entry, writer_info, thread_p,
					    first_pageid);
  if (error_code != NO_ERROR)
    {
      pthread_mutex_unlock (&writer_info->wr_list_mutex);
      status = LOGWR_STATUS_ERROR;
      goto error;
    }
  assert (entry != NULL);

  if (entry->status != LOGWR_STATUS_DELAY)
    {
      assert (false);

      pthread_mutex_unlock (&writer_info->wr_list_mutex);

      error_code = ER_FAILED;
      status = LOGWR_STATUS_ERROR;
      goto error;
    }

  pthread_mutex_unlock (&writer_info->wr_list_mutex);


  /* Send the log pages to be flushed until now */
  error_code = logwr_pack_log_pages (thread_p, logpg_area, zip_logpg,
				     &logpg_used_size, &status, entry,
				     NULL, NULL, NULL, NULL);
  if (error_code != NO_ERROR)
    {
      error_code = ER_HA_LW_FAILED_GET_LOG_PAGE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1, first_pageid);

      status = LOGWR_STATUS_ERROR;
      goto error;
    }

  error_code = xlog_send_log_pages_to_migrator (thread_p, logpg_area,
						logpg_used_size);
  if (error_code != NO_ERROR)
    {
      status = LOGWR_STATUS_ERROR;
      goto error;
    }

  rv = pthread_mutex_lock (&writer_info->wr_list_mutex);
  logwr_unregister_writer_entry (writer_info, entry);
  pthread_mutex_unlock (&writer_info->wr_list_mutex);

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

  rv = pthread_mutex_lock (&writer_info->wr_list_mutex);
  logwr_unregister_writer_entry (writer_info, entry);
  pthread_mutex_unlock (&writer_info->wr_list_mutex);

  free_and_init (logpg_area);
  if (zip_logpg != NULL)
    {
      log_zip_free (zip_logpg);
      zip_logpg = NULL;
    }

  return error_code;
}
