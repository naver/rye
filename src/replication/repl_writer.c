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
 * repl_writer.c -
 */

#ident "$Id$"

#include "config.h"

#include <assert.h>
#include <sys/ipc.h>
#include <time.h>

#include "log_impl.h"

#include "memory_alloc.h"
#include "error_manager.h"
#include "porting.h"
#include "environment_variable.h"
#include "message_catalog.h"
#include "misc_string.h"
#include "intl_support.h"
#include "system_parameter.h"
#include "network_interface_cl.h"
#include "connection_support.h"
#include "dbi.h"
#include "network.h"
#include "utility.h"
#include "heartbeat.h"

#include "ds_queue.h"

#include "cas_common.h"

#include "cci_util.h"

#include "repl.h"
#include "repl_common.h"
#include "repl_writer.h"
#include "repl_catalog.h"
#include "repl_analyzer.h"

#include "fault_injection.h"




#define LOGWR_THREAD_SUSPEND_TIMEOUT 	10

CIRP_LOGWR_GLOBAL cirpwr_Gl = {
  /* ha_info */
  COPY_LOG_HEADER_INITIALIZER,
  /* loghdr_pgptr */
  NULL,
  /* db_name */
  {'0'}
  ,
  /* host info */
  PRM_NODE_INFO_INITIALIZER,
  /* log_path */
  {'0'}
  ,
  /* loginf_path */
  {'0'}
  ,
  /* active_name */
  {'0'}
  ,
  /* append_vdes */
  NULL_VOLDES,
  /* last_received_pageid, last_received_file_status */
  NULL_PAGEID, 0,
  /* recv log queue */
  PTHREAD_MUTEX_INITIALIZER,
  PTHREAD_COND_INITIALIZER,
  NULL,
  NULL,
  /* unzip_area */
  NULL,
  /* logpg_area */
  NULL,
  /* logpg_area_size */
  0,
  /* logpg_fill_size */
  0,
  /* toflush */
  NULL,
  /* max_toflush */
  0,
  /* num_toflush */
  0,
  /* action */
  CIRPWR_ACTION_NONE,
  /* last_arv_lpageid */
  NULL_PAGEID,
  /* background archiving info */
  BACKGROUND_ARCHIVING_INFO_INITIALIZER,
  /* bg_archive_name */
  {'0'}
};

static int cirpwr_flush_header_page (void);
static int cirpwr_get_log_header (void);
static LOG_PHY_PAGEID cirpwr_to_physical_pageid (LOG_PAGEID logical_pageid);

static int cirpwr_fetch_header_page (void);
static int cirpwr_fetch_log_page (LOG_PAGE * log_pgptr, LOG_PAGEID pageid);
static int cirpwr_writev_append_pages (LOG_PAGE ** to_flush, DKNPAGES npages);
static int cirpwr_flush_all_append_pages (void);
static int cirpwr_set_hdr_and_flush_info (void);

static int cirpwr_check_archive_info ();
static int cirpwr_archive_active_log (void);
static int rp_log_header_validate (const LOG_HEADER * log_hdr,
				   const char *db_name);

static int cirpwr_get_log_pages (LOGWR_CONTEXT * ctx_ptr);
static int net_client_request_with_cirpwr_context (LOGWR_CONTEXT * ctx_ptr,
						   int request, char *argbuf,
						   int argsize,
						   char *replybuf,
						   int replysize);
static int net_client_cirpwr_get_next_log_pages (RECV_Q_NODE * node);
static LOG_PAGEID cirpwr_get_fpageid (LOG_PAGEID current_pageid,
				      const LOG_HEADER * head);

static int rpwr_alloc_recv_node (RECV_Q_NODE ** node);
static int cirpwr_copy_all_omitted_pages (LOG_PAGEID fpageid);

static int cirpwr_change_copier_status (CIRP_WRITER_INFO * writer_info,
					CIRP_AGENT_STATUS status);
static int cirpwr_change_writer_status (CIRP_WRITER_INFO * writer_info,
					CIRP_AGENT_STATUS status);


/*
 * cirpwr_to_physical_pageid -
 *
 * return:
 *   logical_pageid(in):
 * Note:
 */
LOG_PHY_PAGEID
cirpwr_to_physical_pageid (LOG_PAGEID logical_pageid)
{
  LOG_PHY_PAGEID phy_pageid;

  if (logical_pageid == LOGPB_HEADER_PAGE_ID)
    {
      phy_pageid = 0;
    }
  else
    {
      LOG_PAGEID tmp_pageid;

      tmp_pageid = logical_pageid - cirpwr_Gl.ha_info.fpageid;

      assert (cirpwr_Gl.ha_info.npages > 0);
      if (tmp_pageid >= cirpwr_Gl.ha_info.npages)
	{
	  tmp_pageid %= cirpwr_Gl.ha_info.npages;
	}
      else if (tmp_pageid < 0)
	{
	  tmp_pageid = (cirpwr_Gl.ha_info.npages
			- ((-tmp_pageid) % cirpwr_Gl.ha_info.npages));
	}
      tmp_pageid++;
      if (tmp_pageid > cirpwr_Gl.ha_info.npages)
	{
	  tmp_pageid %= cirpwr_Gl.ha_info.npages;
	}

      assert (tmp_pageid <= PAGEID_MAX);
      phy_pageid = (LOG_PHY_PAGEID) tmp_pageid;
    }

  return phy_pageid;
}

/*
 * cirpwr_fetch_header_page -
 *
 * return:
 *   log_pgptr(out):
 *   pageid(in):
 * Note:
 */
static int
cirpwr_fetch_header_page (void)
{
  CIRP_WRITER_INFO *writer;
  int error = NO_ERROR;

  error = cirpwr_fetch_log_page (cirpwr_Gl.loghdr_pgptr,
				 LOGPB_HEADER_PAGE_ID);
  if (error != NO_ERROR)
    {
      return error;
    }

  assert (cirpwr_Gl.loghdr_pgptr == (LOG_PAGE *) cirpwr_Gl.logpg_area);
  assert (cirpwr_Gl.loghdr_pgptr->hdr.logical_pageid == LOGPB_HEADER_PAGE_ID);

  writer = &Repl_Info->writer_info;
  error = pthread_mutex_lock (&writer->lock);
  if (error != NO_ERROR)
    {
      error = ER_CSS_PTHREAD_MUTEX_LOCK;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

      return error;
    }
  memcpy (writer->hdr_page, cirpwr_Gl.loghdr_pgptr, IO_MAX_PAGE_SIZE);
  pthread_mutex_unlock (&writer->lock);

  return NO_ERROR;
}

/*
 * cirpwr_fetch_log_page -
 *
 * return:
 *   log_pgptr(out):
 *   pageid(in):
 * Note:
 */
static int
cirpwr_fetch_log_page (LOG_PAGE * log_pgptr, LOG_PAGEID pageid)
{
  LOG_PHY_PAGEID phy_pageid;

  assert (log_pgptr != NULL);

  /*
   * Page is contained in the active log.
   * Find the corresponding physical page and read the page form disk.
   */
  phy_pageid = cirpwr_to_physical_pageid (pageid);
  if (fileio_read (NULL, cirpwr_Gl.append_vdes, log_pgptr, phy_pageid,
		   LOG_PAGESIZE) == NULL)
    {
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_LOG_READ, 3, pageid, phy_pageid, cirpwr_Gl.active_name);
      return ER_LOG_READ;
    }
  else
    {
      if (log_pgptr->hdr.logical_pageid != pageid)
	{
	  assert (false);
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LOG_PAGE_CORRUPTED, 1, pageid);
	  return ER_LOG_PAGE_CORRUPTED;
	}
    }

  return NO_ERROR;
}

/*
 * cirp_final_writer ()
 *    return: NO_ERROR
 *
 *    analyzer(in/out):
 */
int
cirp_final_writer (CIRP_WRITER_INFO * writer)
{
  if (Repl_Info == NULL)
    {
      assert (false);

      return NO_ERROR;
    }

  if (writer->hdr_page != NULL)
    {
      free_and_init (writer->hdr_page);
    }
  writer->reader_count = 0;
  writer->is_archiving = false;

  pthread_mutex_lock (&writer->lock);
  writer->copier_status = CIRP_AGENT_DEAD;
  writer->writer_status = CIRP_AGENT_DEAD;
  pthread_mutex_unlock (&writer->lock);

  pthread_mutex_destroy (&writer->lock);

  pthread_cond_destroy (&writer->cond);

  return NO_ERROR;
}

/*
 * cirp_init_writer()-
 *   return: error code
 *
 *   writer(out):
 */
int
cirp_init_writer (CIRP_WRITER_INFO * writer)
{
  int error = NO_ERROR;

  if (pthread_mutex_init (&writer->lock, NULL) < 0)
    {
      error = ER_CSS_PTHREAD_MUTEX_INIT;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

      GOTO_EXIT_ON_ERROR;
    }
  if (pthread_cond_init (&writer->cond, NULL) < 0)
    {
      error = ER_CSS_PTHREAD_COND_INIT;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      GOTO_EXIT_ON_ERROR;
    }

  writer->hdr_page = (LOG_PAGE *) malloc (IO_MAX_PAGE_SIZE);
  if (writer->hdr_page == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
			   1, IO_MAX_PAGE_SIZE);

      GOTO_EXIT_ON_ERROR;
    }
  writer->reader_count = 0;
  writer->is_archiving = false;

  cirpwr_change_copier_status (writer, CIRP_AGENT_INIT);
  cirpwr_change_writer_status (writer, CIRP_AGENT_INIT);

  memset (&writer->ct, 0, sizeof (CIRP_CT_LOG_WRITER));

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  assert (error != NO_ERROR);

  return error;
}

/*
 * cirpwr_initialize - Initialize cirpwr_Gl structure
 *
 * return:
 *
 *   db_name(in):
 *   log_path(in):
 *
 * Note:
 */
int
cirpwr_initialize (const char *db_name, const char *log_path)
{
  int log_nbuffers;
  char *at_char = NULL;
  int error = NO_ERROR;
  int i;


  /* set the db name and log path */
  strncpy (cirpwr_Gl.db_name, db_name, PATH_MAX - 1);
  cirpwr_Gl.db_name[PATH_MAX - 1] = '\0';
  at_char = strchr (cirpwr_Gl.db_name, '@');
  if (at_char != NULL)
    {
      if (rp_host_str_to_node_info (&cirpwr_Gl.host_info,
				    at_char + 1) != NO_ERROR)
	{
	  assert (0);
	}
      *at_char = '\0';
    }
  strncpy (cirpwr_Gl.log_path, log_path, PATH_MAX - 1);

  /* set the active log file path */
  fileio_make_log_active_name (cirpwr_Gl.active_name, log_path,
			       cirpwr_Gl.db_name);
  /* set the log info file path */
  fileio_make_log_info_name (cirpwr_Gl.loginf_path, log_path,
			     cirpwr_Gl.db_name);
  /* background archive file path */
  fileio_make_log_archive_temp_name (cirpwr_Gl.bg_archive_name, log_path,
				     cirpwr_Gl.db_name);
  log_nbuffers = LOGWR_COPY_LOG_BUFFER_NPAGES + 1;

  if (cirpwr_Gl.unzip_area == NULL)
    {
      cirpwr_Gl.unzip_area =
	log_zip_alloc (log_nbuffers * LOG_PAGESIZE, false);
      if (cirpwr_Gl.unzip_area == NULL)
	{
	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
		  log_nbuffers * LOG_PAGESIZE);
	  cirpwr_Gl.logpg_area_size = 0;
	  return error;
	}
    }
  if (cirpwr_Gl.recv_log_queue == NULL)
    {
      cirpwr_Gl.recv_log_queue = Rye_queue_new ();
      if (cirpwr_Gl.recv_log_queue == NULL)
	{
	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
		  sizeof (RQueue));
	  return error;
	}
    }
  if (cirpwr_Gl.free_list == NULL)
    {
      cirpwr_Gl.free_list = Rye_queue_new ();
      if (cirpwr_Gl.free_list == NULL)
	{
	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
		  sizeof (RQueue));
	  return error;
	}
    }

  if (cirpwr_Gl.logpg_area == NULL)
    {
      cirpwr_Gl.logpg_area_size = log_nbuffers * LOG_PAGESIZE;
      cirpwr_Gl.logpg_area = RYE_MALLOC (cirpwr_Gl.logpg_area_size);
      if (cirpwr_Gl.logpg_area == NULL)
	{
	  if (cirpwr_Gl.unzip_area != NULL)
	    {
	      log_zip_free (cirpwr_Gl.unzip_area);
	      cirpwr_Gl.unzip_area = NULL;
	    }
	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
		  1, cirpwr_Gl.logpg_area_size);
	  cirpwr_Gl.logpg_area_size = 0;
	  return error;
	}
    }
  cirpwr_Gl.loghdr_pgptr = (LOG_PAGE *) cirpwr_Gl.logpg_area;

  if (cirpwr_Gl.toflush != NULL)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, " ");
      return error;
    }

  cirpwr_Gl.max_toflush = log_nbuffers - 1;
  cirpwr_Gl.toflush = (LOG_PAGE **) calloc (cirpwr_Gl.max_toflush,
					    sizeof (LOG_PAGE *));
  if (cirpwr_Gl.toflush == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, cirpwr_Gl.max_toflush * sizeof (cirpwr_Gl.toflush));
      cirpwr_Gl.max_toflush = 0;
      return error;
    }
  for (i = 0; i < cirpwr_Gl.max_toflush; i++)
    {
      cirpwr_Gl.toflush[i] = NULL;
    }

  return NO_ERROR;
}

/*
 * cirpwr_create_active_log()-
 *   return: error code
 */
int
cirpwr_create_active_log (CCI_CONN * conn)
{
  CIRP_CT_LOG_ANALYZER ct;
  LOG_HEADER *m_log_hdr;
  int error = NO_ERROR;
  struct timeval current_time;

  assert (cirpwr_Gl.append_vdes == NULL_VOLDES);

  er_log_debug (ARG_FILE_LINE, "create active log");

  error = cirpwr_get_log_header ();
  if (error != NO_ERROR)
    {
      return error;
    }
  m_log_hdr = (LOG_HEADER *) (cirpwr_Gl.loghdr_pgptr->area);
  error = rp_log_header_validate (m_log_hdr, cirpwr_Gl.db_name);
  if (error != NO_ERROR)
    {
      return error;
    }

  error = rpct_get_log_analyzer (conn, &ct, &cirpwr_Gl.host_info);
  if (error != NO_ERROR && error != CCI_ER_NO_MORE_DATA)
    {
      return error;
    }
  if (error == CCI_ER_NO_MORE_DATA)
    {
      er_log_debug (ARG_FILE_LINE, "init log info from log header."
		    "sof_lsa(%lld,%d)", (long long) m_log_hdr->sof_lsa.pageid,
		    m_log_hdr->sof_lsa.offset);

      /* insert new log analyzer info */

      memset (&ct, 0, sizeof (CIRP_CT_LOG_ANALYZER));
      ct.host_info = cirpwr_Gl.host_info;

      /* set first record */
      LSA_COPY (&ct.current_lsa, &m_log_hdr->sof_lsa);
      LSA_COPY (&ct.required_lsa, &m_log_hdr->sof_lsa);

      gettimeofday (&current_time, NULL);
      ct.start_time = timeval_to_msec (&current_time);
      ct.source_applied_time = ct.start_time;

      ct.creation_time = m_log_hdr->db_creation * 1000;
      ct.queue_full = 0;

      error = rpct_insert_log_analyzer (conn, &ct);
      if (error != NO_ERROR)
	{
	  return error;
	}
    }

  cirpwr_Gl.ha_info.fpageid = cirpwr_get_fpageid (ct.required_lsa.pageid,
						  m_log_hdr);
  cirpwr_Gl.ha_info.npages = m_log_hdr->npages;
  cirpwr_Gl.last_received_pageid = -ct.required_lsa.pageid;
  cirpwr_Gl.ha_info.last_flushed_pageid = -ct.required_lsa.pageid;
  cirpwr_Gl.ha_info.nxarv_pageid = NULL_PAGEID;
  cirpwr_Gl.ha_info.nxarv_num = -1;
  cirpwr_Gl.ha_info.last_deleted_arv_num = -1;

  cirpwr_Gl.ha_info.file_status = m_log_hdr->ha_info.file_status;
  cirpwr_Gl.ha_info.server_state = m_log_hdr->ha_info.server_state;
  cirpwr_Gl.ha_info.perm_status = m_log_hdr->ha_info.perm_status;

  /* Create a new active log */
  cirpwr_Gl.append_vdes = fileio_format (NULL,
					 cirpwr_Gl.db_name,
					 cirpwr_Gl.active_name,
					 LOG_DBLOG_ACTIVE_VOLID,
					 m_log_hdr->npages + 1,
					 true, true,
					 false, LOG_PAGESIZE, 0, false);
  if (cirpwr_Gl.append_vdes == NULL_VOLDES)
    {
      /* Unable to create an active log */
      error = ER_IO_FORMAT_FAIL;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   error, 3, cirpwr_Gl.active_name, -1, -1);

      return error;
    }

  error = cirpwr_flush_header_page ();

  return error;
}

/*
 * cirpwr_get_fpageid()
 *   return: first page id
 *
 *   current_pageid(in):
 *   head(in):
 */
static LOG_PAGEID
cirpwr_get_fpageid (LOG_PAGEID current_pageid, const LOG_HEADER * head)
{
  LOG_PAGEID fpageid;

  fpageid = head->fpageid;
  while (current_pageid < fpageid)
    {
      fpageid -= head->npages;
    }

  return fpageid;
}

/*
 * cirpwr_read_active_log_info -
 *
 * return:
 *
 * Note:
 */
int
cirpwr_init_copy_log_info (void)
{
  int error = NO_ERROR;
  BACKGROUND_ARCHIVING_INFO *bg_arv_info;
  LOG_HEADER *m_log_hdr;
  int vdes;

  m_log_hdr = (LOG_HEADER *) (cirpwr_Gl.loghdr_pgptr->area);

  if (cirpwr_Gl.append_vdes == NULL_VOLDES)
    {
      /* Mount the active log and read the log header */
      cirpwr_Gl.append_vdes = fileio_mount (NULL, cirpwr_Gl.db_name,
					    cirpwr_Gl.active_name,
					    LOG_DBLOG_ACTIVE_VOLID,
					    true, false);
      if (cirpwr_Gl.append_vdes == NULL_VOLDES)
	{
	  /* Unable to create an active log */
	  error = ER_IO_FORMAT_FAIL;
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       error, 3, cirpwr_Gl.active_name, -1, -1);

	  return error;
	}

      error = cirpwr_fetch_header_page ();
      if (error != NO_ERROR)
	{
	  return error;
	}

      error = rp_log_header_validate (m_log_hdr, cirpwr_Gl.db_name);
      if (error != NO_ERROR)
	{
	  return error;
	}

      memcpy (&cirpwr_Gl.ha_info, &m_log_hdr->ha_info,
	      sizeof (COPY_LOG_HEADER));
    }
  cirpwr_Gl.last_received_pageid = cirpwr_Gl.ha_info.last_flushed_pageid;
  cirpwr_Gl.last_received_file_status = cirpwr_Gl.ha_info.file_status;

  cirpwr_Gl.action = CIRPWR_ACTION_NONE;

  vdes = fileio_format (NULL, cirpwr_Gl.db_name,
			cirpwr_Gl.bg_archive_name,
			LOG_DBLOG_BG_ARCHIVE_VOLID,
			m_log_hdr->npages + 1,
			false, false, false, LOG_PAGESIZE, 0, false);
  if (vdes == NULL_VOLDES)
    {
      REPL_SET_GENERIC_ERROR (error,
			      "Unable to create temporary archive log");
      return error;
    }

  /* set background archiving info */
  bg_arv_info = &cirpwr_Gl.bg_archive_info;

  bg_arv_info->start_page_id = cirpwr_Gl.ha_info.nxarv_pageid;
  bg_arv_info->current_page_id = NULL_PAGEID;
  bg_arv_info->last_sync_pageid = NULL_PAGEID;
  bg_arv_info->vdes = vdes;

  return NO_ERROR;
}

/*
 * rp_log_header_validate()
 *   return: error code
 *
 *   log_hdr(in):
 *   db_name(in):
 */
static int
rp_log_header_validate (const LOG_HEADER * log_hdr, const char *db_name)
{
  char err_msg[ER_MSG_SIZE];
  int error = NO_ERROR;

  /* check magic */
  if (strncmp (log_hdr->log_magic,
	       RYE_MAGIC_LOG_ACTIVE, RYE_MAGIC_MAX_LENGTH) != 0)
    {
      REPL_SET_GENERIC_ERROR (error, "invalid magic(%s)", log_hdr->log_magic);

      return error;
    }

  /* check prefix name */
  if (log_hdr->prefix_name[0] != '\0'
      && strncmp (log_hdr->prefix_name, db_name,
		  strlen (log_hdr->prefix_name)) != 0)
    {
      /* ER_LOG_INCOMPATIBLE_PREFIX_NAME */
      snprintf (err_msg, sizeof (err_msg),
		"NOTIFICATION: The prefix name '%s' is not the same as"
		" '%s' on the log disk. The log may have been renamed"
		" outside the database domain.",
		db_name, log_hdr->prefix_name);

      /*
       * This looks like the log or the log was renamed. Incompatible
       * prefix name with the prefix stored on disk
       */
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
	      ER_LOG_INCOMPATIBLE_PREFIX_NAME, 2,
	      db_name, log_hdr->prefix_name);
      /* Continue anyhow */
    }

  /* check log page size */
  if (log_hdr->db_logpagesize != IO_MAX_PAGE_SIZE)
    {
      REPL_SET_GENERIC_ERROR (error, "invalid log page size(%d)",
			      log_hdr->db_logpagesize);

      return error;
    }

  if (log_hdr->db_iopagesize != IO_MAX_PAGE_SIZE)
    {
      REPL_SET_GENERIC_ERROR (error, "invalid data page size(%d)",
			      log_hdr->db_iopagesize);

      return error;
    }

  assert (error == NO_ERROR);
  return error;
}

/*
 *
 */
static int
cirpwr_rye_queue_node_free (void *node, UNUSED_ARG void *data)
{
  RECV_Q_NODE *tmp;

  tmp = (RECV_Q_NODE *) node;

  if (tmp->data != NULL)
    {
      free_and_init (tmp->data);
      tmp->area_length = tmp->length = 0;
    }
  free_and_init (tmp);

  return NO_ERROR;
}

/*
 * cirpwr_finalize -
 *
 * return:
 * Note:
 */
void
cirpwr_finalize (void)
{
  er_log_debug (ARG_FILE_LINE, "finalize cirpwr_GL");

  if (cirpwr_Gl.unzip_area != NULL)
    {
      log_zip_free (cirpwr_Gl.unzip_area);
      cirpwr_Gl.unzip_area = NULL;
    }
  if (cirpwr_Gl.recv_log_queue != NULL)
    {
      Rye_queue_free_full (cirpwr_Gl.recv_log_queue,
			   cirpwr_rye_queue_node_free);
      free_and_init (cirpwr_Gl.recv_log_queue);
    }
  if (cirpwr_Gl.free_list != NULL)
    {
      Rye_queue_free_full (cirpwr_Gl.free_list, cirpwr_rye_queue_node_free);
      free_and_init (cirpwr_Gl.free_list);
    }

  RYE_FREE_MEM (cirpwr_Gl.logpg_area);
  cirpwr_Gl.logpg_area_size = 0;
  cirpwr_Gl.logpg_fill_size = 0;
  cirpwr_Gl.loghdr_pgptr = NULL;

  RYE_FREE_MEM (cirpwr_Gl.toflush);
  cirpwr_Gl.max_toflush = 0;
  cirpwr_Gl.num_toflush = 0;

  if (cirpwr_Gl.append_vdes != NULL_VOLDES)
    {
      fileio_dismount (NULL, cirpwr_Gl.append_vdes);
      cirpwr_Gl.append_vdes = NULL_VOLDES;
    }
  cirpwr_Gl.action = CIRPWR_ACTION_NONE;

  if (cirpwr_Gl.bg_archive_info.vdes != NULL_VOLDES)
    {
      fileio_dismount (NULL, cirpwr_Gl.bg_archive_info.vdes);
      cirpwr_Gl.bg_archive_info.vdes = NULL_VOLDES;
    }
}

/*
 * cirpwr_set_hdr_and_flush_info -
 *
 * return:
 * Note:
 */
static int
cirpwr_set_hdr_and_flush_info (void)
{
  LOG_PAGE *log_pgptr = NULL, *last_pgptr;
  char *p;
  int num_toflush = 0;
  LOG_HEADER *m_log_hdr;;
  COPY_LOG_HEADER *ha_info = NULL;
  LOG_PAGEID fpageid;
  BACKGROUND_ARCHIVING_INFO *bg_info;
  int error = NO_ERROR;

  /* Set the flush information */
  p = cirpwr_Gl.logpg_area + LOG_PAGESIZE;
  while (p < (cirpwr_Gl.logpg_area + cirpwr_Gl.logpg_fill_size))
    {
      log_pgptr = (LOG_PAGE *) p;
      cirpwr_Gl.toflush[num_toflush++] = log_pgptr;
      p += LOG_PAGESIZE;
    }

  if (num_toflush == 0)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, " ");
      return error;
    }

  last_pgptr = log_pgptr;
  cirpwr_Gl.num_toflush = num_toflush;

  ha_info = &cirpwr_Gl.ha_info;
  m_log_hdr = (LOG_HEADER *) (cirpwr_Gl.loghdr_pgptr->area);
  fpageid = cirpwr_Gl.toflush[0]->hdr.logical_pageid;
  if (ha_info->nxarv_pageid == NULL_PAGEID || ha_info->nxarv_num == -1)
    {
      /* first request */
      ha_info->nxarv_pageid = m_log_hdr->ha_info.nxarv_pageid;
      ha_info->nxarv_num = m_log_hdr->ha_info.nxarv_num;
      ha_info->last_deleted_arv_num = ha_info->nxarv_num - 1;

      /* set background archive info */
      bg_info = &cirpwr_Gl.bg_archive_info;
      bg_info->start_page_id = ha_info->nxarv_pageid;
      bg_info->current_page_id = NULL_PAGEID;
      bg_info->last_sync_pageid = NULL_PAGEID;
    }

  /* Set the header and action information */
  assert (last_pgptr != NULL);
  assert (cirpwr_Gl.loghdr_pgptr == (LOG_PAGE *) cirpwr_Gl.logpg_area);

  /* Check if it need archiving */
  if (m_log_hdr->ha_info.file_status == LOG_HA_FILESTAT_ARCHIVED
      && ha_info->last_flushed_pageid >= 0)
    {
      assert (ha_info->last_flushed_pageid == fpageid
	      || ha_info->last_flushed_pageid + 1 == fpageid);

      /* Do delayed archiving */
      cirpwr_Gl.action |= CIRPWR_ACTION_ARCHIVING;
      cirpwr_Gl.last_arv_lpageid = fpageid - 1;
    }
  else if ((ha_info->nxarv_num + 1 == m_log_hdr->nxarv_num)
	   && (fpageid >= m_log_hdr->nxarv_pageid))
    {
      cirpwr_Gl.action |= CIRPWR_ACTION_ARCHIVING;
      cirpwr_Gl.last_arv_lpageid = m_log_hdr->nxarv_pageid - 1;

      assert (cirpwr_Gl.last_arv_lpageid + 1 == fpageid);
    }

  /*
   * LWT sets the archiving flag at the time when it sends new active page
   * after archiving finished, so that logwr_archive_active_log() should
   * be executed before logwr_flush_all_append_pages().
   */
  if (cirpwr_Gl.action & CIRPWR_ACTION_ARCHIVING)
    {
      CIRP_WRITER_INFO *writer = NULL;
      struct timespec wakeup_time;

      writer = &Repl_Info->writer_info;

      error = pthread_mutex_lock (&writer->lock);
      if (error != NO_ERROR)
	{
	  error = ER_CSS_PTHREAD_MUTEX_LOCK;
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

	  return error;
	}

      while (writer->reader_count > 0)
	{
	  if (rp_need_shutdown (ARG_FILE_LINE) == true)
	    {
	      pthread_mutex_unlock (&writer->lock);

	      REPL_SET_GENERIC_ERROR (error, "NEED SHUTDOWN");
	      return error;
	    }
	  clock_gettime (CLOCK_REALTIME, &wakeup_time);
	  wakeup_time = timespec_add_msec (&wakeup_time, 10);
	  pthread_cond_timedwait (&writer->cond, &writer->lock, &wakeup_time);
	}

      writer->is_archiving = true;
      error = cirpwr_archive_active_log ();
      writer->is_archiving = false;
      if (error != NO_ERROR)
	{
	  pthread_mutex_unlock (&writer->lock);
	  return error;
	}

      cirpwr_Gl.action &= ~CIRPWR_ACTION_ARCHIVING;

      pthread_mutex_unlock (&writer->lock);

      error = cirpwr_flush_header_page ();
      if (error != NO_ERROR)
	{
	  return error;
	}
    }

  ha_info->last_flushed_pageid = last_pgptr->hdr.logical_pageid;
  assert (ha_info->last_flushed_pageid <= m_log_hdr->eof_lsa.pageid);

  return NO_ERROR;
}

/*
 * cirpwr_copy_all_omitted_pages -
 *   return: error code
 *
 *   fpageid(in):
 */
static int
cirpwr_copy_all_omitted_pages (LOG_PAGEID fpageid)
{
  char page_buffer[IO_MAX_PAGE_SIZE + MAX_ALIGNMENT];
  LOG_PAGE *log_pgptr = NULL;
  LOG_PAGEID pageid;
  LOG_PHY_PAGEID phy_pageid;
  int error = NO_ERROR;

  assert (cirpwr_Gl.bg_archive_info.vdes != NULL_VOLDES);

  if (cirpwr_Gl.bg_archive_info.current_page_id > fpageid)
    {
      return NO_ERROR;
    }

  log_pgptr = (LOG_PAGE *) PTR_ALIGN (page_buffer, MAX_ALIGNMENT);

  pageid = cirpwr_Gl.bg_archive_info.current_page_id;
  if (pageid == NULL_PAGEID)
    {
      pageid = cirpwr_Gl.bg_archive_info.start_page_id;
    }

  for (; pageid <= fpageid; pageid++)
    {
      error = cirpwr_fetch_log_page (log_pgptr, pageid);
      if (error != NO_ERROR)
	{
	  return error;
	}

      phy_pageid = (LOG_PHY_PAGEID) (pageid -
				     cirpwr_Gl.bg_archive_info.
				     start_page_id + 1);
      if (fileio_write (NULL, cirpwr_Gl.bg_archive_info.vdes, log_pgptr,
			phy_pageid, LOG_PAGESIZE) == NULL)
	{
	  error = er_errid ();
	  if (error == ER_IO_WRITE_OUT_OF_SPACE)
	    {
	      error = ER_LOG_WRITE_OUT_OF_SPACE;
	      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		      error, 4,
		      pageid, phy_pageid, cirpwr_Gl.bg_archive_name,
		      LOG_PAGESIZE);
	    }
	  else
	    {
	      error = ER_LOG_WRITE;
	      er_set_with_oserror (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
				   error, 3,
				   pageid, phy_pageid,
				   cirpwr_Gl.bg_archive_name);
	    }

	  return error;
	}

      cirpwr_Gl.bg_archive_info.current_page_id = pageid;
    }

  assert (error == NO_ERROR);
  return error;
}

/*
 * logwr_fetch_append_pages -
 *   return: error code
 *
 *   to_flush(in):
 *   npages(in):
 * Note:
 */
static int
cirpwr_writev_append_pages (LOG_PAGE ** to_flush, DKNPAGES npages)
{
  LOG_PAGEID fpageid;
  LOG_PHY_PAGEID phy_pageid;
  int error = NO_ERROR;

  if (npages <= 0)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "invalid npages");
      return error;
    }

  fpageid = to_flush[0]->hdr.logical_pageid;

  /* 1. archive temp write */
  /* check archive temp descriptor */
  if (cirpwr_Gl.bg_archive_info.vdes == NULL_VOLDES)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "invalid temporary archive log file");
      return error;
    }

  error = cirpwr_copy_all_omitted_pages (fpageid - 1);
  if (error != NO_ERROR)
    {
      assert (false);
      return error;
    }

  if (cirpwr_Gl.bg_archive_info.current_page_id == NULL_PAGEID)
    {
      if (fpageid != cirpwr_Gl.bg_archive_info.start_page_id)
	{
	  assert (false);

	  REPL_SET_GENERIC_ERROR (error,
				  "invalid temporary archive log file");
	  return error;
	}
    }
  else
    {
      if (fpageid != cirpwr_Gl.bg_archive_info.current_page_id
	  && fpageid != cirpwr_Gl.bg_archive_info.current_page_id + 1)
	{
	  assert (false);

	  REPL_SET_GENERIC_ERROR (error,
				  "invalid temporary archive log file");
	  return error;
	}
    }


  phy_pageid = (LOG_PHY_PAGEID) (fpageid -
				 cirpwr_Gl.bg_archive_info.start_page_id + 1);
  if (fileio_writev (NULL, cirpwr_Gl.bg_archive_info.vdes, (void **) to_flush,
		     phy_pageid, npages, LOG_PAGESIZE) == NULL)
    {
      error = er_errid ();
      if (error == ER_IO_WRITE_OUT_OF_SPACE)
	{
	  error = ER_LOG_WRITE_OUT_OF_SPACE;
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		  error, 4,
		  fpageid, phy_pageid, cirpwr_Gl.bg_archive_name,
		  LOG_PAGESIZE);
	}
      else
	{
	  error = ER_LOG_WRITE;
	  er_set_with_oserror (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
			       error, 3,
			       fpageid, phy_pageid,
			       cirpwr_Gl.bg_archive_name);
	}

      return error;
    }

  cirpwr_Gl.bg_archive_info.current_page_id = fpageid + (npages - 1);
  er_log_debug (ARG_FILE_LINE,
		"background archiving  current_page_id[%lld], fpageid[%lld, %lld], npages[%d]",
		cirpwr_Gl.bg_archive_info.current_page_id, fpageid,
		phy_pageid, npages);

  FI_TEST_ARG_INT (NULL, FI_TEST_REPL_RANDOM_EXIT, 10000, 0);

  /* 2. active write */
  phy_pageid = cirpwr_to_physical_pageid (fpageid);
  if (fileio_writev (NULL, cirpwr_Gl.append_vdes, (void **) to_flush,
		     phy_pageid, npages, LOG_PAGESIZE) == NULL)
    {
      error = er_errid ();
      if (error == ER_IO_WRITE_OUT_OF_SPACE)
	{
	  error = ER_LOG_WRITE_OUT_OF_SPACE;
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		  error, 4,
		  fpageid, phy_pageid, cirpwr_Gl.active_name, LOG_PAGESIZE);
	}
      else
	{
	  error = ER_LOG_WRITE;
	  er_set_with_oserror (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
			       error, 3,
			       fpageid, phy_pageid, cirpwr_Gl.active_name);
	}

      return error;
    }

  er_log_debug (ARG_FILE_LINE,
		"active log:  hdr fpageid[%lld], fpageid[%lld, %lld], npages[%d]",
		cirpwr_Gl.ha_info.fpageid, fpageid, phy_pageid, npages);

  assert (error == NO_ERROR);
  return error;
}

/*
 * logwr_flush_all_append_pages -
 *
 * return:
 * Note:
 */
static int
cirpwr_flush_all_append_pages (void)
{
  LOG_PAGE *pgptr, *prv_pgptr;
  LOG_PAGEID pageid, prv_pageid;
  LOG_HEADER *m_log_hdr = NULL;
  int idxflush;
  bool need_sync;
  int flush_page_count;
  int i;
  int error = NO_ERROR;

  idxflush = -1;
  prv_pgptr = NULL;
  need_sync = false;
  flush_page_count = 0;

  for (i = 0; i < cirpwr_Gl.num_toflush; i++)
    {
      pgptr = cirpwr_Gl.toflush[i];

      if (idxflush != -1 && prv_pgptr != NULL)
	{
	  /*
	   * This append log page should be dirty and contiguous to previous
	   * append page. If it is not, we need to flush the accumulated pages
	   * up to this point, and then start accumulating pages again.
	   */
	  pageid = pgptr->hdr.logical_pageid;
	  prv_pageid = prv_pgptr->hdr.logical_pageid;

	  if ((pageid != prv_pageid + 1)
	      || (cirpwr_to_physical_pageid (pageid)
		  != cirpwr_to_physical_pageid (prv_pageid) + 1))
	    {
	      m_log_hdr = (LOG_HEADER *) (cirpwr_Gl.loghdr_pgptr->area);
	      /*
	       * Has the log been cycled ?
	       */
	      if (cirpwr_to_physical_pageid
		  (cirpwr_Gl.toflush[idxflush]->hdr.logical_pageid) == 1)
		{
		  cirpwr_Gl.ha_info.fpageid += m_log_hdr->npages;
		  assert (cirpwr_Gl.ha_info.fpageid
			  == cirpwr_Gl.toflush[idxflush]->hdr.logical_pageid);
		  assert (cirpwr_Gl.ha_info.fpageid % m_log_hdr->npages == 0);
		}

	      /*
	       * This page is not contiguous.
	       *
	       * Flush the accumulated contiguous pages
	       */
	      error =
		cirpwr_writev_append_pages (&cirpwr_Gl.toflush[idxflush],
					    i - idxflush);
	      if (error != NO_ERROR)
		{
		  return error;
		}

	      need_sync = true;

	      /*
	       * Start over the accumulation of pages
	       */

	      flush_page_count += i - idxflush;
	      idxflush = -1;
	    }
	}

      if (idxflush == -1)
	{
	  /*
	   * This page should be included in the flush
	   */
	  idxflush = i;
	}

      /* prv_pgptr was not pgptr's previous buffer.
       * prv_pgptr was the first buffer to flush,
       * so only 2 continous pages always were flushed together.
       */
      prv_pgptr = pgptr;
    }

  /*
   * If there are any accumulated pages, flush them at this point
   */

  if (idxflush != -1)
    {
      int page_toflush = cirpwr_Gl.num_toflush - idxflush;

      /* last countious pages */
      error = cirpwr_writev_append_pages (&cirpwr_Gl.toflush[idxflush],
					  page_toflush);
      if (error != NO_ERROR)
	{
	  return error;
	}

      need_sync = true;
      flush_page_count += page_toflush;
      pgptr = cirpwr_Gl.toflush[idxflush + page_toflush - 1];
    }

  /*
   * Make sure that all of the above log writes are synchronized with any
   * future log writes. That is, the pages should be stored on physical disk.
   */
  if (need_sync == true
      && fileio_synchronize (NULL, cirpwr_Gl.append_vdes,
			     cirpwr_Gl.active_name) == NULL_VOLDES)
    {
      return er_errid ();
    }

  /* It's for dual write. */
  if (need_sync == true)
    {
      if (fileio_synchronize (NULL, cirpwr_Gl.bg_archive_info.vdes,
			      cirpwr_Gl.bg_archive_name) == NULL_VOLDES)
	{
	  return er_errid ();
	}
      cirpwr_Gl.bg_archive_info.last_sync_pageid
	= cirpwr_Gl.bg_archive_info.current_page_id;
    }

  er_log_debug (ARG_FILE_LINE,
		"cirpwr_write_log_pages, flush_page_count(%d), %d->%d\n",
		flush_page_count, cirpwr_Gl.toflush[0]->hdr.logical_pageid,
		cirpwr_Gl.toflush[cirpwr_Gl.num_toflush -
				  1]->hdr.logical_pageid);

  /* Initialize flush info */
  for (i = 0; i < cirpwr_Gl.num_toflush; i++)
    {
      cirpwr_Gl.toflush[i] = NULL;
    }
  cirpwr_Gl.num_toflush = 0;

  return NO_ERROR;
}

/*
 * cirpwr_flush_header_page -
 *
 * return:
 * Note:
 */
static int
cirpwr_flush_header_page (void)
{
  LOG_PAGEID logical_pageid;
  LOG_PHY_PAGEID phy_pageid;
  char buffer[ONE_K];
  int error = NO_ERROR;
  LOG_HEADER *m_log_hdr = NULL;
  CIRP_WRITER_INFO *writer = NULL;

  writer = &Repl_Info->writer_info;

  if (cirpwr_Gl.loghdr_pgptr == NULL)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "invalid parameter");
      return error;
    }

  /* flush current archiving status */
  assert (cirpwr_Gl.loghdr_pgptr->hdr.logical_pageid == LOGPB_HEADER_PAGE_ID);
  assert (cirpwr_Gl.loghdr_pgptr == (LOG_PAGE *) cirpwr_Gl.logpg_area);

  m_log_hdr = (LOG_HEADER *) (cirpwr_Gl.loghdr_pgptr->area);
  if (cirpwr_Gl.ha_info.server_state != m_log_hdr->ha_info.server_state)
    {
      char host_str[MAX_NODE_INFO_STR_LEN];
      if (PRM_NODE_INFO_GET_IP (&cirpwr_Gl.host_info) == INADDR_NONE)
	{
	  strcpy (host_str, "unknown");
	}
      else
	{
	  prm_node_info_to_str (host_str, sizeof (host_str),
				&cirpwr_Gl.host_info);
	}
      snprintf (buffer, ONE_K,
		"change the state of HA server (%s@%s) from '%s' to '%s'",
		cirpwr_Gl.db_name, host_str,
		HA_STATE_NAME (cirpwr_Gl.ha_info.server_state),
		HA_STATE_NAME (m_log_hdr->ha_info.server_state));

      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_NOTIFY_MESSAGE,
	      1, buffer);
    }

  cirpwr_Gl.ha_info.file_status = m_log_hdr->ha_info.file_status;
  cirpwr_Gl.ha_info.server_state = m_log_hdr->ha_info.server_state;
  cirpwr_Gl.ha_info.perm_status = m_log_hdr->ha_info.perm_status;

  memcpy (&m_log_hdr->ha_info, &cirpwr_Gl.ha_info, sizeof (COPY_LOG_HEADER));

  logical_pageid = LOGPB_HEADER_PAGE_ID;
  phy_pageid = cirpwr_to_physical_pageid (logical_pageid);

  /* cirpwr_Gl.append_vdes is only changed
   * while starting or finishing or recovering server.
   * So, log cs is not needed.
   */
  if (fileio_write (NULL, cirpwr_Gl.append_vdes, cirpwr_Gl.loghdr_pgptr,
		    phy_pageid, LOG_PAGESIZE) == NULL
      || fileio_synchronize (NULL, cirpwr_Gl.append_vdes,
			     cirpwr_Gl.active_name) == NULL_VOLDES)
    {

      if (er_errid () == ER_IO_WRITE_OUT_OF_SPACE)
	{
	  error = ER_LOG_WRITE_OUT_OF_SPACE;
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		  error, 4, logical_pageid, phy_pageid,
		  cirpwr_Gl.active_name, LOG_PAGESIZE);
	}
      else
	{
	  error = ER_LOG_WRITE;
	  er_set_with_oserror (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
			       error, 3, logical_pageid, phy_pageid,
			       cirpwr_Gl.active_name);
	}

      return error;
    }

  error = pthread_mutex_lock (&writer->lock);
  if (error != NO_ERROR)
    {
      error = ER_CSS_PTHREAD_MUTEX_LOCK;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

      return error;
    }
  memcpy (writer->hdr_page, cirpwr_Gl.loghdr_pgptr, IO_MAX_PAGE_SIZE);
  pthread_mutex_unlock (&writer->lock);

  er_log_debug (ARG_FILE_LINE,
		"cirpwr_flush_header_page, ha_server_state=%s, ha_file_status=%s\n"
		"last_flushed_pageid(%lld), nxarv_pageid(%lld), nxarv_num(%d)\n",
		HA_STATE_NAME (cirpwr_Gl.ha_info.
			       server_state),
		css_ha_filestat_string (cirpwr_Gl.ha_info.file_status),
		(long long) cirpwr_Gl.ha_info.last_flushed_pageid,
		(long long) cirpwr_Gl.ha_info.nxarv_pageid,
		cirpwr_Gl.ha_info.nxarv_num);


  assert (error == NO_ERROR);
  return error;
}

/*
 * logwr_check_archive_info ()
 */
static int
cirpwr_check_archive_info ()
{
  BACKGROUND_ARCHIVING_INFO *bg_arv_info;
  COPY_LOG_HEADER *ha_info;
  int error = NO_ERROR;

  bg_arv_info = &cirpwr_Gl.bg_archive_info;
  ha_info = &cirpwr_Gl.ha_info;

  if (bg_arv_info->vdes == NULL_VOLDES)
    {
      REPL_SET_GENERIC_ERROR (error, "invalid temporary archive log file");
      return error;
    }

  if (bg_arv_info->start_page_id != ha_info->nxarv_pageid)
    {
      REPL_SET_GENERIC_ERROR (error,
			      "bg_arv_info->start_page_id(%lld), ha_info->nxarv_pageid(%lld)",
			      (long long) bg_arv_info->start_page_id,
			      (long long) ha_info->fpageid);
      return error;
    }
  if (bg_arv_info->current_page_id < cirpwr_Gl.last_arv_lpageid)
    {
      REPL_SET_GENERIC_ERROR (error,
			      "bg_arv_info->current_page_id(%lld), cirpwr_Gl.last_arv_lpageid(%lld)",
			      (long long) bg_arv_info->current_page_id,
			      (long long) cirpwr_Gl.last_arv_lpageid);
      return error;
    }

  return NO_ERROR;
}

/*
 * logwr_archive_active_log -
 *
 * return:
 * Note:
 */
static int
cirpwr_archive_active_log (void)
{
  char archive_name[PATH_MAX] = {
    '\0'
  };
  struct log_arv_header *arvhdr;
  char log_pgbuf[IO_MAX_PAGE_SIZE * LOGPB_IO_NPAGES + MAX_ALIGNMENT];
  LOG_PAGE *arv_hdr_pgptr = NULL;
  const char *catmsg;
  char buffer[LINE_MAX];
  BACKGROUND_ARCHIVING_INFO *bg_arv_info;
  LOG_HEADER *m_log_hdr = NULL;
  UNUSED_VAR CIRP_WRITER_INFO *writer = NULL;
  int error = NO_ERROR;

  writer = &Repl_Info->writer_info;

  bg_arv_info = &cirpwr_Gl.bg_archive_info;

  if (bg_arv_info->start_page_id >= cirpwr_Gl.last_arv_lpageid)
    {
      /* archive file already created */
      assert ((cirpwr_Gl.ha_info.nxarv_pageid
	       == cirpwr_Gl.last_arv_lpageid + 1)
	      && (bg_arv_info->start_page_id
		  == cirpwr_Gl.last_arv_lpageid + 1));

      return NO_ERROR;
    }

  error = cirpwr_copy_all_omitted_pages (cirpwr_Gl.last_arv_lpageid);
  if (error != NO_ERROR)
    {
      assert (false);
      GOTO_EXIT_ON_ERROR;
    }

  error = cirpwr_check_archive_info ();
  if (error != NO_ERROR)
    {
      assert (false);
      GOTO_EXIT_ON_ERROR;
    }

  arv_hdr_pgptr = (LOG_PAGE *) PTR_ALIGN (log_pgbuf, MAX_ALIGNMENT);

  arv_hdr_pgptr->hdr.logical_pageid = LOGPB_HEADER_PAGE_ID;
  arv_hdr_pgptr->hdr.offset = NULL_OFFSET;

  m_log_hdr = (LOG_HEADER *) (cirpwr_Gl.loghdr_pgptr->area);

  /* Construct the archive log header */
  arvhdr = (struct log_arv_header *) arv_hdr_pgptr->area;
  strncpy (arvhdr->magic, RYE_MAGIC_LOG_ARCHIVE, RYE_MAGIC_MAX_LENGTH);
  arvhdr->db_creation = m_log_hdr->db_creation;
  arvhdr->next_trid = NULL_TRANID;
  arvhdr->fpageid = bg_arv_info->start_page_id;
  arvhdr->arv_num = cirpwr_Gl.ha_info.nxarv_num;
  arvhdr->npages = (DKNPAGES) (cirpwr_Gl.last_arv_lpageid
			       - bg_arv_info->start_page_id + 1);

  /*
   * Now create the archive and start copying pages
   */

  snprintf (buffer, sizeof (buffer), "log archiving started for archive %03d",
	    arvhdr->arv_num);
  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_HA_GENERIC_ERROR, 1,
	  buffer);

  fileio_make_log_archive_name (archive_name, cirpwr_Gl.log_path,
				cirpwr_Gl.db_name, arvhdr->arv_num);

  if (fileio_write (NULL, bg_arv_info->vdes, arv_hdr_pgptr,
		    0, LOG_PAGESIZE) == NULL)
    {
      /* Error archiving header page into archive */
      error = ER_LOG_WRITE;
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOG_WRITE, 3,
	      0, 0, fileio_get_base_file_name (archive_name));
      GOTO_EXIT_ON_ERROR;
    }

  fileio_dismount (NULL, bg_arv_info->vdes);
  bg_arv_info->vdes = NULL_VOLDES;

  if (fileio_rename (NULL_VOLID, cirpwr_Gl.bg_archive_name,
		     archive_name) == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }


  /* Update archive info */
  cirpwr_Gl.ha_info.nxarv_pageid = cirpwr_Gl.last_arv_lpageid + 1;
  cirpwr_Gl.ha_info.nxarv_num++;
  assert (cirpwr_Gl.ha_info.nxarv_num <= m_log_hdr->nxarv_num);

  bg_arv_info->start_page_id = cirpwr_Gl.ha_info.nxarv_pageid;
  bg_arv_info->current_page_id = NULL_PAGEID;
  bg_arv_info->last_sync_pageid = NULL_PAGEID;
  bg_arv_info->vdes = fileio_format (NULL, cirpwr_Gl.db_name,
				     cirpwr_Gl.bg_archive_name,
				     LOG_DBLOG_BG_ARCHIVE_VOLID,
				     m_log_hdr->npages, false, false,
				     false, LOG_PAGESIZE, 0, false);
  if (bg_arv_info->vdes == NULL_VOLDES)
    {
      REPL_SET_GENERIC_ERROR (error,
			      "Unable to create temporary archive log");
      GOTO_EXIT_ON_ERROR;
    }

  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_LOG_ARCHIVE_CREATED, 3,
	  fileio_get_base_file_name (archive_name), arvhdr->fpageid,
	  arvhdr->fpageid + arvhdr->npages - 1);

  catmsg = msgcat_message (MSGCAT_CATALOG_RYE,
			   MSGCAT_SET_LOG, MSGCAT_LOG_LOGINFO_ARCHIVE);
  if (catmsg == NULL)
    {
      catmsg = "ARCHIVE: %d %s %lld %lld\n";
    }
  error = log_dump_log_info (cirpwr_Gl.loginf_path, false, catmsg,
			     arvhdr->arv_num,
			     fileio_get_base_file_name (archive_name),
			     arvhdr->fpageid,
			     arvhdr->fpageid + arvhdr->npages - 1);
  er_log_debug (ARG_FILE_LINE,
		"logwr_archive_active_log, arv_num(%d), fpageid(%lld) lpageid(%lld)\n",
		arvhdr->arv_num, arvhdr->fpageid,
		arvhdr->fpageid + arvhdr->npages - 1);

  FI_TEST_ARG_INT (NULL, FI_TEST_REPL_RANDOM_EXIT, 2, 0);

  return NO_ERROR;

exit_on_error:
  assert (error != NO_ERROR);

  return error;
}

/*
 * cirpwr_write_log_pages -
 *
 * return:
 * Note:
 */
int
cirpwr_write_log_pages (void)
{
  int error;
  struct timeval curtime;
  static struct timeval last_flush_time = { 0, 0 };
  INT64 diff_msec;
  LOG_HEADER *m_log_hdr;
  bool has_writer_mutex = false;
  CIRP_WRITER_INFO *writer = NULL;

  writer = &Repl_Info->writer_info;

  assert (cirpwr_Gl.append_vdes != NULL_VOLDES);

  if (cirpwr_Gl.num_toflush <= 0)
    {
      return NO_ERROR;
    }
  m_log_hdr = (LOG_HEADER *) (cirpwr_Gl.loghdr_pgptr->area);

  gettimeofday (&curtime, NULL);
  diff_msec = timeval_diff_in_msec (&curtime, &last_flush_time);

  if (!(cirpwr_Gl.action & CIRPWR_ACTION_ARCHIVING)
      && !(cirpwr_Gl.action & CIRPWR_ACTION_FORCE_FLUSH)
      && cirpwr_Gl.num_toflush == 1
      && diff_msec < 1000
      && (cirpwr_Gl.ha_info.last_flushed_pageid == m_log_hdr->eof_lsa.pageid))
    {
      return NO_ERROR;
    }

  error = cirpwr_flush_all_append_pages ();
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  FI_TEST_ARG_INT (NULL, FI_TEST_REPL_RANDOM_EXIT, 1000, 0);

  error = cirpwr_flush_header_page ();
  if (error != NO_ERROR)
    {
      return error;
    }

  error = pthread_mutex_lock (&writer->lock);
  if (error != NO_ERROR)
    {
      error = ER_CSS_PTHREAD_MUTEX_LOCK;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

      GOTO_EXIT_ON_ERROR;
    }
  has_writer_mutex = true;

  assert (!LSA_ISNULL (&m_log_hdr->eof_lsa));
  assert (writer->ct.last_flushed_pageid <= m_log_hdr->eof_lsa.pageid);

  writer->ct.last_flushed_pageid = cirpwr_Gl.ha_info.last_flushed_pageid;
  writer->ct.last_received_time = curtime.tv_sec;

  LSA_COPY (&writer->ct.eof_lsa, &m_log_hdr->eof_lsa);

  pthread_mutex_unlock (&writer->lock);
  has_writer_mutex = false;

  last_flush_time = curtime;

  cirpwr_Gl.action = CIRPWR_ACTION_NONE;

  return NO_ERROR;

exit_on_error:
  if (has_writer_mutex == true)
    {
      pthread_mutex_unlock (&writer->lock);
    }

  if (error == NO_ERROR)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "Invalid error code");
    }

  return error;
}

/*
 * cirpwr_get_log_header
 *    return:
 */
static int
cirpwr_get_log_header ()
{
  int error = NO_ERROR;
  int compressed_protocol;
  LOGWR_CONTEXT ctx = {
    -1, 0, false
  };
  OR_ALIGNED_BUF (OR_INT_SIZE * 2 + OR_INT64_SIZE) a_request;
  OR_ALIGNED_BUF (OR_INT_SIZE * 5 + OR_INT64_SIZE * 2) a_reply;
  char *request, *reply;
  char *ptr;
  char *logpg_area = NULL;

  if (prm_get_bool_value (PRM_ID_LOGWR_COMPRESSED_PROTOCOL))
    {
      compressed_protocol = 1;
    }
  else
    {
      compressed_protocol = 0;
    }

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  /* HEADER PAGE REQUEST */
  ptr = or_pack_int64 (request, LOGPB_HEADER_PAGE_ID);
  ptr = or_pack_int (ptr, NO_ERROR);
  ptr = or_pack_int (ptr, compressed_protocol);

  error = net_client_get_log_header (&ctx, request,
				     OR_ALIGNED_BUF_SIZE (a_request), reply,
				     OR_ALIGNED_BUF_SIZE (a_reply),
				     (char **) &logpg_area);
  if (error != NO_ERROR)
    {
      RYE_FREE_MEM (logpg_area);
      return error;
    }

  assert (logpg_area != NULL);

  assert (cirpwr_Gl.loghdr_pgptr == (LOG_PAGE *) cirpwr_Gl.logpg_area);
  memcpy (cirpwr_Gl.loghdr_pgptr, logpg_area, LOG_PAGESIZE);

  assert (cirpwr_Gl.loghdr_pgptr->hdr.logical_pageid == LOGPB_HEADER_PAGE_ID);

  RYE_FREE_MEM (logpg_area);

  /* END REQUEST */
  ptr = or_pack_int64 (request, LOGPB_HEADER_PAGE_ID);
  /* send ER_GENERIC_ERROR to make LWT not wait for more page requests */
  ptr = or_pack_int (ptr, ER_GENERIC_ERROR);
  ptr = or_pack_int (ptr, compressed_protocol);

  error = net_client_get_log_header (&ctx, request,
				     OR_ALIGNED_BUF_SIZE (a_request), reply,
				     OR_ALIGNED_BUF_SIZE (a_reply),
				     (char **) &logpg_area);
  RYE_FREE_MEM (logpg_area);

  return error;
}

/*
 * log_copier_main -
 *
 * return: NO_ERROR if successful, error_code otherwise
 *
 * Note:
 */
void *
log_copier_main (void *arg)
{
  LOGWR_CONTEXT ctx = {
    -1, 0, false
  };
  int error = NO_ERROR;
  ER_MSG_INFO *th_er_msg = NULL;
  CIRP_THREAD_ENTRY *th_entry = NULL;
  char err_msg[ER_MSG_SIZE];
  int retry_count = 0;
  LOG_HEADER *m_hdr;
  RECV_Q_NODE *node;

  th_entry = (CIRP_THREAD_ENTRY *) arg;

  th_er_msg = malloc (sizeof (ER_MSG_INFO));
  error = er_set_msg_info (th_er_msg);
  if (error != NO_ERROR)
    {
      RP_SET_AGENT_NEED_SHUTDOWN ();
      cirpwr_change_copier_status (&Repl_Info->writer_info, CIRP_AGENT_DEAD);

      free_and_init (th_er_msg);
      return NULL;
    }

  /* wait until thread_create finish */
  error = pthread_mutex_lock (&th_entry->th_lock);
  pthread_mutex_unlock (&th_entry->th_lock);

  assert (th_entry->th_type == CIRP_THREAD_COPIER);

  snprintf (err_msg, sizeof (err_msg),
	    "Writer Start: last_pageid(%lld)",
	    (long long) cirpwr_Gl.ha_info.last_flushed_pageid);
  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_NOTIFY_MESSAGE, 1,
	  err_msg);

  while (REPL_NEED_SHUTDOWN () == false)
    {
      /* connect rye_server */
      if (db_get_connect_status () != DB_CONNECTION_STATUS_CONNECTED)
	{
	  error = cirp_connect_copylogdb (th_entry->arg->db_name, false);
	  if (error != NO_ERROR)
	    {
	      int recv_q_node_count, wakeup_interval = 100;
	      struct timespec wakeup_time;

	      m_hdr = (LOG_HEADER *) (cirpwr_Gl.loghdr_pgptr->area);
	      if (m_hdr->ha_info.server_state != HA_STATE_DEAD)
		{
		  error = rpwr_alloc_recv_node (&node);
		  if (error == NO_ERROR)
		    {
		      node->server_status = HA_STATE_DEAD;
		      pthread_mutex_lock (&cirpwr_Gl.recv_q_lock);
		      Rye_queue_enqueue (cirpwr_Gl.recv_log_queue, node);
		      pthread_cond_signal (&cirpwr_Gl.recv_q_cond);
		      pthread_mutex_unlock (&cirpwr_Gl.recv_q_lock);

		      do
			{
			  clock_gettime (CLOCK_REALTIME, &wakeup_time);
			  wakeup_time = timespec_add_msec (&wakeup_time,
							   wakeup_interval);

			  pthread_mutex_lock (&cirpwr_Gl.recv_q_lock);
			  pthread_cond_timedwait (&cirpwr_Gl.recv_q_cond,
						  &cirpwr_Gl.recv_q_lock,
						  &wakeup_time);
			  recv_q_node_count =
			    cirpwr_Gl.recv_log_queue->list.count;
			  pthread_mutex_unlock (&cirpwr_Gl.recv_q_lock);
			}
		      while (recv_q_node_count > HB_RECV_Q_MAX_COUNT
			     && REPL_NEED_SHUTDOWN () == false);
		    }
		}
	      THREAD_SLEEP (100);

	      continue;
	    }
	}

      /* copy log pages */
      while (ctx.shutdown == false && REPL_NEED_SHUTDOWN () == false)
	{
	  error = cirpwr_get_log_pages (&ctx);
	  if (error != NO_ERROR)
	    {
	      ctx.last_error = error;

	      if (error == ER_NET_SERVER_CRASHED)
		{
		  /* Write the server is dead at the log header */
		  er_log_debug (ARG_FILE_LINE, "SERVER CRASHED");

		  m_hdr = (LOG_HEADER *) (cirpwr_Gl.loghdr_pgptr->area);
		  if (m_hdr->ha_info.server_state != HA_STATE_DEAD)
		    {
		      error = rpwr_alloc_recv_node (&node);
		      if (error == NO_ERROR)
			{
			  node->server_status = HA_STATE_DEAD;
			  pthread_mutex_lock (&cirpwr_Gl.recv_q_lock);
			  Rye_queue_enqueue (cirpwr_Gl.recv_log_queue, node);
			  pthread_cond_signal (&cirpwr_Gl.recv_q_cond);
			  pthread_mutex_unlock (&cirpwr_Gl.recv_q_lock);
			}
		    }

		  ctx.shutdown = true;
		  break;
		}
	      else if (error == ER_HA_LW_FAILED_GET_LOG_PAGE)
		{
		  ctx.shutdown = true;

		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_HB_PROCESS_EVENT, 2,
			  "Encountered an unrecoverable error "
			  "and will shut itself down", "");
		  break;
		}
	      else if (error == ER_HA_LA_SHARED_MEM_RESET
		       || error == ER_HA_LA_INVALID_SHARED_MEM)
		{
		  ctx.shutdown = true;
		  break;
		}
	    }
	}

      if (ctx.rc != -1)
	{
	  net_client_logwr_send_end_msg (ctx.rc, ER_FAILED);
	}

      /* clear connection */
      db_shutdown ();

      assert (db_get_connect_status () != DB_CONNECTION_STATUS_CONNECTED);

      ctx.rc = -1;
      ctx.last_error = 0;
      ctx.shutdown = false;

      retry_count++;

      snprintf (err_msg, sizeof (err_msg),
		"Writer Retry: last_pageid(%ld)",
		cirpwr_Gl.ha_info.last_flushed_pageid);
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_NOTIFY_MESSAGE, 1,
	      err_msg);
    }

  RP_SET_AGENT_NEED_SHUTDOWN ();
  cirpwr_change_copier_status (&Repl_Info->writer_info, CIRP_AGENT_DEAD);

  snprintf (err_msg, sizeof (err_msg), "Writer Exit: last_pageid(%ld)",
	    cirpwr_Gl.ha_info.last_flushed_pageid);
  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_NOTIFY_MESSAGE, 1,
	  err_msg);

  free_and_init (th_er_msg);

  return NULL;
}

/*
 * log_writer_main -
 *
 * return: NO_ERROR if successful, error_code otherwise
 *
 *   db_name(in): database name to copy the log file
 *   log_path(in): file pathname to copy the log file
 *
 * Note:
 */
void *
log_writer_main (void *arg)
{
  int error = NO_ERROR;
  ER_MSG_INFO *th_er_msg_info;
  CIRP_THREAD_ENTRY *th_entry = NULL;
  char err_msg[ER_MSG_SIZE];
  RECV_Q_NODE *node;
  struct timespec wakeup_time;
  int wakeup_interval = 100;	/* msec */

  th_entry = (CIRP_THREAD_ENTRY *) arg;

  th_er_msg_info = malloc (sizeof (ER_MSG_INFO));
  error = er_set_msg_info (th_er_msg_info);
  if (error != NO_ERROR)
    {
      RP_SET_AGENT_NEED_SHUTDOWN ();
      cirpwr_change_writer_status (&Repl_Info->writer_info, CIRP_AGENT_DEAD);

      free_and_init (th_er_msg_info);
      return NULL;
    }

  /* wait until thread_create finish */
  error = pthread_mutex_lock (&th_entry->th_lock);
  pthread_mutex_unlock (&th_entry->th_lock);

  assert (th_entry->th_type == CIRP_THREAD_FLUSHER);

  while (REPL_NEED_SHUTDOWN () == false)
    {
      clock_gettime (CLOCK_REALTIME, &wakeup_time);

      wakeup_time = timespec_add_msec (&wakeup_time, wakeup_interval);

      pthread_mutex_lock (&cirpwr_Gl.recv_q_lock);
      pthread_cond_timedwait (&cirpwr_Gl.recv_q_cond, &cirpwr_Gl.recv_q_lock,
			      &wakeup_time);
      pthread_mutex_unlock (&cirpwr_Gl.recv_q_lock);

      while (1)
	{
	  pthread_mutex_lock (&cirpwr_Gl.recv_q_lock);
	  node = (RECV_Q_NODE *) Rye_queue_dequeue (cirpwr_Gl.recv_log_queue);
	  pthread_mutex_unlock (&cirpwr_Gl.recv_q_lock);
	  if (node == NULL)
	    {
	      break;
	    }

	  (void) net_client_cirpwr_get_next_log_pages (node);

	  pthread_mutex_lock (&cirpwr_Gl.recv_q_lock);
	  pthread_cond_signal (&cirpwr_Gl.recv_q_cond);
	  pthread_mutex_unlock (&cirpwr_Gl.recv_q_lock);
	}
    }

  RP_SET_AGENT_NEED_SHUTDOWN ();
  cirpwr_change_writer_status (&Repl_Info->writer_info, CIRP_AGENT_DEAD);

  snprintf (err_msg, sizeof (err_msg), "Flusher Exit: last_pageid(%ld)",
	    cirpwr_Gl.ha_info.last_flushed_pageid);
  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_NOTIFY_MESSAGE, 1,
	  err_msg);

  free_and_init (th_er_msg_info);

  return NULL;
}

/*
 * cirpwr_change_copier_status ()
 *   return: NO_ERROR
 *
 *   writer_info(in/out):
 *   status(in):
 */
static int
cirpwr_change_copier_status (CIRP_WRITER_INFO * writer_info,
			     CIRP_AGENT_STATUS status)
{
  pthread_mutex_lock (&writer_info->lock);
  writer_info->copier_status = status;
  pthread_mutex_unlock (&writer_info->lock);

  return NO_ERROR;
}

/*
 * cirpwr_change_writer_status ()
 *   return: NO_ERROR
 *
 *   writer_info(in/out):
 *   status(in):
 */
static int
cirpwr_change_writer_status (CIRP_WRITER_INFO * writer_info,
			     CIRP_AGENT_STATUS status)
{
  pthread_mutex_lock (&writer_info->lock);
  writer_info->writer_status = status;
  pthread_mutex_unlock (&writer_info->lock);

  return NO_ERROR;
}

/*
 * cirpwr_get_copier_status ()
 *   return: NO_ERROR
 *
 *   writer_info(in):
 */
CIRP_AGENT_STATUS
cirpwr_get_copier_status (CIRP_WRITER_INFO * writer_info)
{
  CIRP_AGENT_STATUS status;

  pthread_mutex_lock (&writer_info->lock);
  status = writer_info->copier_status;
  pthread_mutex_unlock (&writer_info->lock);

  return status;
}

/*
 * cirpwr_get_writer_status ()
 *   return: NO_ERROR
 *
 *   writer_info(in):
 */
CIRP_AGENT_STATUS
cirpwr_get_writer_status (CIRP_WRITER_INFO * writer_info)
{
  CIRP_AGENT_STATUS status;

  pthread_mutex_lock (&writer_info->lock);
  status = writer_info->writer_status;
  pthread_mutex_unlock (&writer_info->lock);

  return status;
}

/*
 * cirpwr_get_log_pages -
 *
 * return:
 *
 *   rc_ptr(in/out): request context
 *
 * NOTE:
 */
static int
cirpwr_get_log_pages (LOGWR_CONTEXT * ctx_ptr)
{
  OR_ALIGNED_BUF (OR_INT64_SIZE + OR_INT_SIZE * 2) a_request;
  OR_ALIGNED_BUF (OR_INT64_SIZE * 2 + OR_INT_SIZE * 5) a_reply;
  char *request, *reply;
  char *ptr;
  LOG_PAGEID first_pageid_torecv;
  LOG_HEADER *log_hdr = NULL;
  int compressed_protocol;
  int error = NO_ERROR;

  log_hdr = (LOG_HEADER *) (cirpwr_Gl.loghdr_pgptr->area);

  assert (!LSA_ISNULL (&log_hdr->eof_lsa));

  first_pageid_torecv = cirpwr_Gl.last_received_pageid;
  if (first_pageid_torecv <= 0)
    {
      /* received first pageid of active or archive log */
      ;
    }
  else if (cirpwr_Gl.last_received_file_status ==
	   LOG_HA_FILESTAT_SYNCHRONIZED)
    {
      ;
    }
  else
    {
      first_pageid_torecv = first_pageid_torecv + 1;
    }

  if (prm_get_bool_value (PRM_ID_LOGWR_COMPRESSED_PROTOCOL))
    {
      compressed_protocol = 1;
    }
  else
    {
      compressed_protocol = 0;
    }

  er_log_debug (ARG_FILE_LINE,
		"cirpwr_get_log_pages, fpageid(%lld),  compressed_protocol(%d)",
		first_pageid_torecv, compressed_protocol);

  request = OR_ALIGNED_BUF_START (a_request);
  reply = OR_ALIGNED_BUF_START (a_reply);

  ptr = or_pack_int64 (request, first_pageid_torecv);
  ptr = or_pack_int (ptr, ctx_ptr->last_error);
  ptr = or_pack_int (ptr, compressed_protocol);

  error = net_client_request_with_cirpwr_context (ctx_ptr,
						  NET_SERVER_LOGWR_GET_LOG_PAGES,
						  request,
						  OR_ALIGNED_BUF_SIZE
						  (a_request), reply,
						  OR_ALIGNED_BUF_SIZE
						  (a_reply));

  return error;
}

/*
 * rpwr_alloc_recv_node ()-
 *   return: error code
 *
 *   node(out):recv queue node
 */
static int
rpwr_alloc_recv_node (RECV_Q_NODE ** node)
{
  RECV_Q_NODE *q_node;
  int size;
  int error = NO_ERROR;

  if (node == NULL)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "Invalid arguments");
      return error;
    }
  *node = NULL;

  pthread_mutex_lock (&cirpwr_Gl.recv_q_lock);
  q_node = Rye_queue_dequeue (cirpwr_Gl.free_list);
  pthread_mutex_unlock (&cirpwr_Gl.recv_q_lock);
  if (q_node == NULL)
    {
      size = sizeof (RECV_Q_NODE);
      q_node = (RECV_Q_NODE *) malloc (size);
      if (q_node == NULL)
	{
	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, size);
	  return error;
	}

      size = LOGWR_COPY_LOG_BUFFER_NPAGES * LOG_PAGESIZE;
      q_node->data = (char *) malloc (size);
      if (q_node->data == NULL)
	{
	  free (q_node);

	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, size);
	  return error;
	}
      q_node->area_length = size;
    }

  q_node->fpageid = 0;
  q_node->length = 0;
  q_node->num_page = 0;

  *node = q_node;

  assert (error == NO_ERROR);
  return NO_ERROR;
}

/*
 * net_client_request_with_cirpwr_context -
 *
 * return:
 * Note:
 */
static int
net_client_request_with_cirpwr_context (LOGWR_CONTEXT * ctx_ptr,
					int request,
					char *argbuf, int argsize,
					char *replybuf, int replysize)
{
  unsigned int eid;
  int error;
  int request_error;
  char *ptr;
  QUERY_SERVER_REQUEST server_request;
  int server_request_num;
  int recv_q_node_count = 0;
  struct timespec wakeup_time;
  int wakeup_interval = 100;
  CSS_NET_PACKET *recv_packet;
  RECV_Q_NODE *node = NULL;

  error = 0;

  if (ctx_ptr->rc == -1)
    {
      error = net_client_request_send_msg (&eid, request, 1, argbuf, argsize);

      if (error != NO_ERROR)
	{
	  return error;
	}
      ctx_ptr->rc = eid;
    }
  else
    {
      /* It sends the same request with new arguments */
      eid = ctx_ptr->rc;
      error = net_client_data_send_msg (eid, 1, argbuf, argsize);

      if (error != NO_ERROR)
	{
	  return error;
	}
    }

  error = rpwr_alloc_recv_node (&node);
  if (error != NO_ERROR)
    {
      return error;
    }

  recv_packet = NULL;
  error = net_client_request_recv_msg (&recv_packet, eid, -1, 2,
				       replybuf, replysize,
				       node->data, node->area_length);
  if (error != NO_ERROR)
    {
      pthread_mutex_lock (&cirpwr_Gl.recv_q_lock);
      Rye_queue_enqueue (cirpwr_Gl.free_list, node);
      node = NULL;
      pthread_mutex_unlock (&cirpwr_Gl.recv_q_lock);

      return error;
    }

  assert (node != NULL);

  ptr = or_unpack_int (replybuf, &server_request_num);
  server_request = (QUERY_SERVER_REQUEST) server_request_num;

  switch (server_request)
    {
    case GET_NEXT_LOG_PAGES:
      {
	int length;
	INT64 pageid, eof_pageid;
	int num_page, file_status, server_status;
	int data_recv_size;

	ptr = or_unpack_int (ptr, &length);
	ptr = or_unpack_int64 (ptr, &pageid);
	ptr = or_unpack_int64 (ptr, &eof_pageid);
	ptr = or_unpack_int (ptr, &num_page);
	ptr = or_unpack_int (ptr, &file_status);
	ptr = or_unpack_int (ptr, &server_status);

	if (pageid < 0 || num_page < 0)
	  {
	    assert (false);

	    error = ER_NET_SERVER_CRASHED;
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

	    css_net_packet_free (recv_packet);
	    assert (node != NULL);
	    pthread_mutex_lock (&cirpwr_Gl.recv_q_lock);
	    Rye_queue_enqueue (cirpwr_Gl.free_list, node);
	    node = NULL;
	    pthread_mutex_unlock (&cirpwr_Gl.recv_q_lock);

	    return error;
	  }

	node->server_status = server_status;
	node->fpageid = pageid;
	node->num_page = num_page;
	node->length = length;

	cirpwr_Gl.last_received_pageid = pageid + num_page - 1;
	cirpwr_Gl.last_received_file_status = file_status;

	pthread_mutex_lock (&cirpwr_Gl.recv_q_lock);
	Rye_queue_enqueue (cirpwr_Gl.recv_log_queue, node);
	node = NULL;
	recv_q_node_count = cirpwr_Gl.recv_log_queue->list.count;
	pthread_cond_signal (&cirpwr_Gl.recv_q_cond);
	pthread_mutex_unlock (&cirpwr_Gl.recv_q_lock);

	monitor_stats_gauge (MNT_RP_COPIER_ID, MNT_RP_LAST_RECEIVED_PAGEID,
			     cirpwr_Gl.last_received_pageid);
	monitor_stats_gauge (MNT_RP_COPIER_ID, MNT_RP_EOF_PAGEID, eof_pageid);

	while (recv_q_node_count > HB_RECV_Q_MAX_COUNT
	       && REPL_NEED_SHUTDOWN () == false)
	  {
	    clock_gettime (CLOCK_REALTIME, &wakeup_time);

	    wakeup_time = timespec_add_msec (&wakeup_time, wakeup_interval);

	    pthread_mutex_lock (&cirpwr_Gl.recv_q_lock);
	    pthread_cond_timedwait (&cirpwr_Gl.recv_q_cond,
				    &cirpwr_Gl.recv_q_lock, &wakeup_time);
	    recv_q_node_count = cirpwr_Gl.recv_log_queue->list.count;
	    pthread_mutex_unlock (&cirpwr_Gl.recv_q_lock);
	  }

	assert (length <= cirpwr_Gl.logpg_area_size);

	data_recv_size = css_net_packet_get_recv_size (recv_packet, 1);
	if (data_recv_size < length)
	  {
	    assert (false);

	    error = ER_NET_SERVER_CRASHED;
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	    ctx_ptr->shutdown = true;
	  }
      }
      break;
    case END_CALLBACK:
      ptr = or_unpack_int (ptr, &request_error);
      if (request_error != ctx_ptr->last_error)
	{
	  /* By server error or shutdown */
	  error = request_error;
	  if (error != ER_HA_LW_FAILED_GET_LOG_PAGE)
	    {
	      error = ER_NET_SERVER_CRASHED;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	    }
	}

      ctx_ptr->shutdown = true;
      break;
    default:
      /* TODO: handle the unknown request as an error */
      error = ER_NET_SERVER_DATA_RECEIVE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

      ctx_ptr->shutdown = true;
      break;
    }

  css_net_packet_free (recv_packet);
  if (node != NULL)
    {
      pthread_mutex_lock (&cirpwr_Gl.recv_q_lock);
      Rye_queue_enqueue (cirpwr_Gl.free_list, node);
      node = NULL;
      pthread_mutex_unlock (&cirpwr_Gl.recv_q_lock);
    }

  return (error);
}

/*
 * net_client_cirp_get_next_log_pages -
 *
 */
static int
net_client_cirpwr_get_next_log_pages (RECV_Q_NODE * node)
{
  int error;
  int length = 0;

  assert (node != NULL);

  if (node->server_status == HA_STATE_DEAD)
    {
      LOG_HEADER *m_hdr;

      m_hdr = (LOG_HEADER *) (cirpwr_Gl.loghdr_pgptr->area);
      m_hdr->ha_info.server_state = HA_STATE_DEAD;
      cirpwr_Gl.action |= CIRPWR_ACTION_FORCE_FLUSH;
    }
  else
    {
      if (!ZIP_CHECK (node->length))
	{
	  memcpy (cirpwr_Gl.logpg_area, node->data, node->length);
	  length = node->length;
	}
      else
	{
	  length = (int) GET_ZIP_LEN (node->length);

	  if (!log_unzip (cirpwr_Gl.unzip_area, length, node->data))
	    {
	      assert (false);	/* TODO - */
	      error = ER_IO_LZO_DECOMPRESS_FAIL;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	    }

	  memcpy (cirpwr_Gl.logpg_area,
		  cirpwr_Gl.unzip_area->log_data,
		  (int) cirpwr_Gl.unzip_area->data_length);

	  length = cirpwr_Gl.unzip_area->data_length;
	}

      cirpwr_Gl.logpg_fill_size = length;

      error = cirpwr_set_hdr_and_flush_info ();
      if (error != NO_ERROR)
	{
	  return error;
	}
    }

  error = cirpwr_write_log_pages ();
  if (error != NO_ERROR)
    {
      return error;
    }
  monitor_stats_gauge (MNT_RP_FLUSHER_ID, MNT_RP_LAST_FLUSHED_PAGEID,
		       cirpwr_Gl.ha_info.last_flushed_pageid);

  if (cirpwr_Gl.action & CIRPWR_ACTION_FORCE_FLUSH)
    {
      /* don't flush log header in cirpwr_write_log_pages() */
      (void) cirpwr_flush_header_page ();
      cirpwr_Gl.action = CIRPWR_ACTION_NONE;
    }

  node->length = 0;
  pthread_mutex_lock (&cirpwr_Gl.recv_q_lock);
  Rye_queue_enqueue (cirpwr_Gl.free_list, node);
  pthread_mutex_unlock (&cirpwr_Gl.recv_q_lock);

  return error;
}
