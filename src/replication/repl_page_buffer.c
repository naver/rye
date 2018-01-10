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
 * repl_page_buffer.c -
 */

#ident "$Id$"

#include <stdio.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>

#include "db.h"
#include "config.h"
#include "file_io.h"
#include "storage_common.h"
#include "repl_log.h"

#include "repl_page_buffer.h"
#include "repl.h"


#define ARV_NUM_INITIAL_VAL	(-1)

/* static functions */
static LOG_PHY_PAGEID cirp_to_phy_pageid (CIRP_BUF_MGR * buf_mgr,
					  LOG_PAGEID logical_pageid);

static PRM_NODE_INFO cirp_get_hostname_from_log_path (const char *log_path,
						      const char *db_name);


static int cirp_log_io_read (char *vname, int vdes, void *io_pgptr,
			     LOG_PHY_PAGEID pageid, int pagesize);

static void cirp_logpb_compress_final (CIRP_BUF_MGR * buf_mgr);
static int cirp_logpb_compress_init (CIRP_BUF_MGR * buf_mgr);

static void cirp_logpb_act_log_final (CIRP_BUF_MGR * buf_mgr);
static int cirp_logpb_act_log_init (CIRP_BUF_MGR * buf_mgr,
				    const char *log_path,
				    const char *prefix_name);
static void cirp_logpb_act_log_close (CIRP_BUF_MGR * buf_mgr);
static int cirp_logpb_act_log_open (CIRP_BUF_MGR * buf_mgr);

static void cirp_logpb_arv_log_final (CIRP_BUF_MGR * buf_mgr);
static int cirp_logpb_arv_log_init (CIRP_BUF_MGR * buf_mgr);
static int cirp_logpb_arv_log_close (CIRP_BUF_MGR * buf_mgr);
static void cirp_logpb_arv_log_remove (CIRP_BUF_MGR * buf_mgr, int arv_num);
static int cirp_logpb_find_last_deleted_arv_num (CIRP_BUF_MGR * buf_mgr);

static int cirp_logpb_arv_log_open (CIRP_BUF_MGR * buf_mgr, int arv_num);
static int cirp_logpb_arv_log_fetch_hdr (CIRP_BUF_MGR * buf_mgr, int arv_num);
static int cirp_logpb_arv_log_find_arv_num (CIRP_BUF_MGR * buf_mgr,
					    int *arv_num, LOG_PAGEID pageid);
static int cirp_logpb_arv_log_find_arv_num_internal (CIRP_BUF_MGR *
						     buf_mgr, int *arv_num,
						     LOG_PAGEID pageid);
static int cirp_logpb_remove_archive_log_internal (CIRP_BUF_MGR *
						   buf_mgr, int first_arv_num,
						   int last_arv_num,
						   int max_arv_cnt_to_delete);

static void cirp_logpb_validate_last_deleted_arv_num (CIRP_BUF_MGR * buf_mgr);


static void cirp_logpb_cache_final (CIRP_BUF_MGR * buf_mgr);
static int cirp_logpb_cache_init (CIRP_BUF_MGR * buf_mgr);

static int cirp_logpb_fetch (CIRP_BUF_MGR * buf_mgr,
			     LOG_PAGEID pageid, CIRP_LOGPB * logpb);
static int cirp_logpb_fetch_from_archive (CIRP_BUF_MGR * buf_mgr,
					  LOG_PAGEID pageid, char *data);
static int cirp_logpb_fetch_from_active (CIRP_BUF_MGR * buf_mgr,
					 LOG_PAGEID pageid, char *data);
static int cirp_logpb_replace_buffer (CIRP_BUF_MGR * buf_mgr,
				      CIRP_LOGPB ** out_logpb,
				      LOG_PAGEID pageid);
static int cirp_logpb_expand_buffer (CIRP_BUF_MGR * buf_mgr);
static void cirp_logpb_clear_logpb (CIRP_LOGPB * logpb);

static int cirp_logpb_init_buffer_manager (CIRP_BUF_MGR * buf_mgr);
static void cirp_logpb_common_final (CIRP_BUF_MGR * buf_mgr);
static int cirp_logpb_common_init (CIRP_BUF_MGR * buf_mgr,
				   const char *db_name, const char *log_path);
#if defined (ENABLE_UNUSED_FUNCTION)
static const char *cirp_logtype_to_string (LOG_RECTYPE type);
#endif

static void cirp_clear_recdes_pool (CIRP_BUF_MGR * buf_mgr);
static int cirp_init_recdes_pool (CIRP_BUF_MGR * buf_mgr, int page_size,
				  int num_recdes);


/*
 * cirp_get_hostname_from_log_path () -
 *    return: hostname
 *
 *    log_path(in):
 *    db_name(in):
 */
static PRM_NODE_INFO
cirp_get_hostname_from_log_path (const char *log_path, const char *db_name)
{
  char *hostname = NULL;
  const char *p;
  PRM_NODE_INFO host_info = prm_get_null_node_info ();

  if (log_path == NULL)
    {
      goto end;
    }

  p = log_path;
  p += (strlen (log_path) - 1);

  /* log_path: "path/dbname_hostname/" */
  if (*p == '/')
    {
      p--;
    }

  while (*p != '/')
    {
      p--;
      if (p == log_path)
	{
	  goto end;
	}
    }

  hostname = strstr (p, db_name);
  if (hostname == NULL)
    {
      goto end;
    }

  hostname += strlen (db_name);
  if (*hostname != '_')
    {
      goto end;
    }

  hostname++;

end:
  rp_host_str_to_node_info (&host_info, hostname);
  return host_info;
}

/*
 * cirp_to_phy_pageid()-
 *    return: physical log page id
 *
 *    logical_pageid(in):
 */
static LOG_PHY_PAGEID
cirp_to_phy_pageid (CIRP_BUF_MGR * buf_mgr, LOG_PAGEID logical_pageid)
{
  struct log_header *log_hdr = NULL;
  LOG_PHY_PAGEID phy_pageid;

  log_hdr = buf_mgr->act_log.log_hdr;

  if (logical_pageid == LOGPB_HEADER_PAGE_ID)
    {
      phy_pageid = 0;
    }
  else
    {
      LOG_PAGEID tmp_pageid;

      assert (log_hdr != NULL);

      tmp_pageid = logical_pageid - log_hdr->ha_info.fpageid;
      if (tmp_pageid >= log_hdr->npages)
	{
	  tmp_pageid %= log_hdr->npages;
	}
      else if (tmp_pageid < 0)
	{
	  tmp_pageid = log_hdr->npages - ((-tmp_pageid) % log_hdr->npages);
	}
      tmp_pageid++;
      if (tmp_pageid > log_hdr->npages)
	{
	  tmp_pageid %= log_hdr->npages;
	}

      assert (tmp_pageid <= PAGEID_MAX);
      phy_pageid = (LOG_PHY_PAGEID) tmp_pageid;
    }

  return phy_pageid;
}

/*
 * cirp_log_io_read_with_max_retries()-
 *    return: error code
 *
 *    vname(in):
 *    vdes(in):
 *    io_pgptr(out):
 *    pageid(in):
 *    pagesize(in):
 */
static int
cirp_log_io_read_with_max_retries (char *vname, int vdes,
				   void *io_pgptr, LOG_PHY_PAGEID pageid,
				   int pagesize)
{
  int nbytes;
  int remain_bytes = pagesize;
  off64_t offset = ((off64_t) pagesize) * ((off64_t) pageid);
  char *current_ptr = (char *) io_pgptr;
  int error = NO_ERROR;

  assert (vdes != NULL_VOLDES);

  if (lseek64 (vdes, offset, SEEK_SET) == -1)
    {
      error = ER_IO_READ;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   error, 2, pageid, vname);
      return error;
    }

  while (remain_bytes > 0)
    {
      /* Read the desired page */
      nbytes = read (vdes, current_ptr, remain_bytes);

      if (nbytes == 0)
	{
	  /*
	   * This is an end of file.
	   */
	  assert (false);

	  error = ER_PB_BAD_PAGEID;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 2, pageid, vname);

	  return error;
	}
      else if (nbytes < 0)
	{
	  assert (false);

	  if (errno == EINTR)
	    {
	      continue;
	    }
	  else
	    {
	      error = ER_IO_READ;
	      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
				   error, 2, pageid, vname);
	      return error;
	    }
	}

      remain_bytes -= nbytes;
      current_ptr += nbytes;
    }

  if (remain_bytes > 0)
    {
      assert (false);

      error = ER_IO_READ;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   error, 2, pageid, vname);
      return error;
    }

  return NO_ERROR;
}

/*
 * cirp_log_io_read()-
 *    return:error code
 *
 *    vname(in):
 *    vdes(in):
 *    io_pgptr(out):
 *    pageid(in):
 *    pagesize(in):
 */
static int
cirp_log_io_read (char *vname, int vdes,
		  void *io_pgptr, LOG_PHY_PAGEID pageid, int pagesize)
{
  return cirp_log_io_read_with_max_retries (vname, vdes, io_pgptr, pageid,
					    pagesize);
}

/*
 * cirp_logpb_compress_final()-
 *    return:
 */
static void
cirp_logpb_compress_final (CIRP_BUF_MGR * buf_mgr)
{
  if (buf_mgr->rec_type != NULL)
    {
      free_and_init (buf_mgr->rec_type);
    }

  if (buf_mgr->undo_unzip != NULL)
    {
      log_zip_free (buf_mgr->undo_unzip);
      buf_mgr->undo_unzip = NULL;
    }

  if (buf_mgr->redo_unzip != NULL)
    {
      log_zip_free (buf_mgr->redo_unzip);
      buf_mgr->redo_unzip = NULL;
    }

  return;
}

/*
 * cirp_logpb_compress_init () -
 *   return: error code
 *
 */
static int
cirp_logpb_compress_init (CIRP_BUF_MGR * buf_mgr)
{
  if (lzo_init () != LZO_E_OK)
    {
      er_log_debug (ARG_FILE_LINE, "Failed to initialize lzo");
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1,
	      __FILE__);
      return ER_GENERIC_ERROR;
    }

  /* FIXME-notout: */
  assert (buf_mgr->rec_type == NULL);
  buf_mgr->rec_type = (char *) malloc (DB_SIZEOF (INT16));
  if (buf_mgr->rec_type == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, DB_SIZEOF (INT16));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  assert (buf_mgr->undo_unzip == NULL);
  buf_mgr->undo_unzip = log_zip_alloc (IO_MAX_PAGE_SIZE, false);
  if (buf_mgr->undo_unzip == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, IO_MAX_PAGE_SIZE);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  assert (buf_mgr->redo_unzip == NULL);
  buf_mgr->redo_unzip = log_zip_alloc (IO_MAX_PAGE_SIZE, false);
  if (buf_mgr->redo_unzip == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, IO_MAX_PAGE_SIZE);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  return NO_ERROR;
}

/*
 * cirp_logpb_act_log_final() -
 *    return:
 */
static void
cirp_logpb_act_log_final (CIRP_BUF_MGR * buf_mgr)
{
  CIRP_ACT_LOG *act_log = &buf_mgr->act_log;

  cirp_logpb_act_log_close (buf_mgr);

  if (act_log->hdr_page != NULL)
    {
      free_and_init (act_log->hdr_page);
    }

  act_log->path[0] = '\0';

  return;
}

/*
 * cirp_logpb_act_log_init () -
 *    return: error code
 *
 *    log_path(in):
 *    prefix_name(in):
 */
static int
cirp_logpb_act_log_init (CIRP_BUF_MGR * buf_mgr, const char *log_path,
			 const char *prefix_name)
{
  CIRP_ACT_LOG *act_log = &buf_mgr->act_log;

  assert (act_log->path[0] == '\0'
	  && act_log->vdes == NULL_VOLDES
	  && act_log->hdr_page == NULL && act_log->log_hdr == NULL);

  fileio_make_log_active_name (act_log->path, log_path, prefix_name);

  act_log->hdr_page = (LOG_PAGE *) malloc (IO_MAX_PAGE_SIZE);
  if (act_log->hdr_page == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, IO_MAX_PAGE_SIZE);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  return NO_ERROR;
}

/*
 * cirp_logpb_act_log_close()-
 *    return:
 *
 */
static void
cirp_logpb_act_log_close (CIRP_BUF_MGR * buf_mgr)
{
  CIRP_ACT_LOG *act_log = NULL;

  act_log = &buf_mgr->act_log;
  if (act_log->vdes != NULL_VOLDES)
    {
      fileio_close (act_log->vdes);
      act_log->vdes = NULL_VOLDES;
    }
  act_log->log_hdr = NULL;

  return;
}

/*
 * cirp_logpb_act_log_open()-
 *    return: error code
 */
static int
cirp_logpb_act_log_open (CIRP_BUF_MGR * buf_mgr)
{
  int error = NO_ERROR;
  CIRP_ACT_LOG *act_log = NULL;

  act_log = &buf_mgr->act_log;
  if (act_log->vdes != NULL_VOLDES)
    {
      assert (act_log->path[0] != '\0' && act_log->log_hdr != NULL);
      return NO_ERROR;
    }

  act_log->vdes = fileio_open (act_log->path, O_RDONLY, 0);
  if (act_log->vdes == NULL_VOLDES)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_LOG_MOUNT_FAIL, 1, act_log->path);
      error = ER_LOG_MOUNT_FAIL;
      goto error_rtn;
    }

  return NO_ERROR;

error_rtn:
  cirp_logpb_act_log_close (buf_mgr);

  return error;
}

/*
 * cirp_logpb_act_log_fetch_hdr ()
 *    return: error code
 */
int
cirp_logpb_act_log_fetch_hdr (CIRP_BUF_MGR * buf_mgr)
{
  int error;
  CIRP_ACT_LOG *act_log = NULL;
  CIRP_WRITER_INFO *writer = NULL;

  writer = &Repl_Info->writer_info;
  act_log = &buf_mgr->act_log;

  if (act_log->vdes == NULL_VOLDES)
    {
      error = cirp_logpb_act_log_open (buf_mgr);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  if (act_log->log_hdr != NULL)
    {
      cirp_logpb_decache_range (buf_mgr,
				act_log->log_hdr->ha_info.last_flushed_pageid,
				LOGPAGEID_MAX);
    }

  error = pthread_mutex_lock (&writer->lock);
  if (error != NO_ERROR)
    {
      error = ER_CSS_PTHREAD_MUTEX_LOCK;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

      GOTO_EXIT_ON_ERROR;
    }

  memcpy (act_log->hdr_page, writer->hdr_page, IO_MAX_PAGE_SIZE);

  pthread_mutex_unlock (&writer->lock);

  act_log->log_hdr = (LOG_HEADER *) act_log->hdr_page->area;

  assert (error == NO_ERROR);
  return NO_ERROR;

exit_on_error:
  assert (error != NO_ERROR);

  fileio_close (act_log->vdes);
  act_log->vdes = NULL_VOLDES;

  return error;
}

/*
 * cirp_logpb_arv_log_final() -
 *    return:
 */
static void
cirp_logpb_arv_log_final (CIRP_BUF_MGR * buf_mgr)
{
  CIRP_ARV_LOG *arv_log = &buf_mgr->arv_log;

  (void) cirp_logpb_arv_log_close (buf_mgr);
  if (arv_log->hdr_page != NULL)
    {
      free_and_init (arv_log->hdr_page);
    }
  arv_log->last_deleted_arv_num = ARV_NUM_INITIAL_VAL - 1;

  return;
}

/*
 * cirp_logpb_arv_log_init () -
 *    return: error code
 *
 */
static int
cirp_logpb_arv_log_init (CIRP_BUF_MGR * buf_mgr)
{
  CIRP_ARV_LOG *arv_log;

  arv_log = &buf_mgr->arv_log;

  assert (arv_log->arv_num == ARV_NUM_INITIAL_VAL
	  && arv_log->path[0] == '\0'
	  && arv_log->vdes == NULL_VOLDES
	  && arv_log->hdr_page == NULL
	  && arv_log->log_hdr == NULL
	  && arv_log->last_deleted_arv_num == (ARV_NUM_INITIAL_VAL - 1));

  arv_log->hdr_page = (LOG_PAGE *) malloc (IO_MAX_PAGE_SIZE);
  if (arv_log->hdr_page == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, IO_MAX_PAGE_SIZE);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  return NO_ERROR;
}

/*
 * cirp_logpb_arv_log_close()-
 *    return: NO_ERROR
 *
 */
static int
cirp_logpb_arv_log_close (CIRP_BUF_MGR * buf_mgr)
{
  CIRP_ARV_LOG *arv_log = NULL;

  arv_log = &buf_mgr->arv_log;

  arv_log->arv_num = ARV_NUM_INITIAL_VAL;
  arv_log->path[0] = '\0';
  if (arv_log->vdes != NULL_VOLDES)
    {
      fileio_close (arv_log->vdes);
      arv_log->vdes = NULL_VOLDES;
    }
  arv_log->log_hdr = NULL;

  return NO_ERROR;
}

/*
 * cirp_logpb_arv_log_remove()-
 *    return:
 *
 *    arv_num(in):
 */
static void
cirp_logpb_arv_log_remove (CIRP_BUF_MGR * buf_mgr, int arv_num)
{
  char archive_name[PATH_MAX];
  CIRP_ARV_LOG *arv_log = NULL;

  arv_log = &buf_mgr->arv_log;
  if (arv_num < 0)
    {
      assert (false);
      return;
    }

  if (arv_log->arv_num == arv_num)
    {
      cirp_logpb_arv_log_close (buf_mgr);
    }

  fileio_make_log_archive_name (archive_name, buf_mgr->log_path,
				buf_mgr->prefix_name, arv_num);
  fileio_unformat (NULL, archive_name);

  return;
}

/*
 * cirp_logpb_arv_log_open()-
 *    return: error code
 *
 *    arv_num(in):
 */
static int
cirp_logpb_arv_log_open (CIRP_BUF_MGR * buf_mgr, int arv_num)
{
  int error = NO_ERROR;
  CIRP_ARV_LOG *arv_log = NULL;

  arv_log = &buf_mgr->arv_log;

  if (arv_num < 0)
    {
      assert (false);
      return ER_FAILED;
    }

  if (arv_log->arv_num == arv_num)
    {
      return NO_ERROR;
    }
  else
    {
      (void) cirp_logpb_arv_log_close (buf_mgr);
    }

  assert (buf_mgr->log_path[0] != '\0' && buf_mgr->prefix_name[0] != '\0');
  fileio_make_log_archive_name (arv_log->path,
				buf_mgr->log_path,
				buf_mgr->prefix_name, arv_num);

  arv_log->vdes = fileio_open (arv_log->path, O_RDONLY, 0);
  if (arv_log->vdes == NULL_VOLDES)
    {
      error = ER_LOG_MOUNT_FAIL;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   error, 1, arv_log->path);

      GOTO_EXIT_ON_ERROR;
    }

  arv_log->arv_num = arv_num;

  return error;

exit_on_error:
  cirp_logpb_arv_log_close (buf_mgr);

  return error;
}

/*
 * cirp_logpb_arv_log_fetch_hdr()-
 *    return: error code
 *
 *    arv_num(in):
 */
static int
cirp_logpb_arv_log_fetch_hdr (CIRP_BUF_MGR * buf_mgr, int arv_num)
{
  int error = NO_ERROR;
  CIRP_ARV_LOG *arv_log = &buf_mgr->arv_log;

  error = cirp_logpb_arv_log_open (buf_mgr, arv_num);
  if (error != NO_ERROR)
    {
      goto error_rtn;
    }

  error = cirp_log_io_read_with_max_retries (arv_log->path, arv_log->vdes,
					     arv_log->hdr_page, 0,
					     buf_mgr->db_logpagesize);
  if (error != NO_ERROR)
    {
      assert (false);

      if (error == ER_PB_BAD_PAGEID)
	{
	  goto error_rtn;
	}
      else
	{
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_LOG_READ, 3,
		  LOGPB_HEADER_PAGE_ID, 0, arv_log->path);
	  error = ER_LOG_READ;
	  goto error_rtn;
	}
    }

  arv_log->log_hdr = (struct log_arv_header *) arv_log->hdr_page->area;

  /* validate archive log header */
  if (strncmp (arv_log->log_hdr->magic,
	       RYE_MAGIC_LOG_ARCHIVE, RYE_MAGIC_MAX_LENGTH) != 0
      || arv_log->log_hdr->arv_num != arv_num)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_LOG_READ, 3,
	      LOGPB_HEADER_PAGE_ID, 0, arv_log->path);
      error = ER_LOG_READ;
      goto error_rtn;
    }

  return NO_ERROR;

error_rtn:

  cirp_logpb_arv_log_close (buf_mgr);
  return error;
}

/*
 * cirp_logpb_arv_log_find_arv_num_internal()-
 *    return: error code
 *
 *    arv_num(out):
 *    pageid(in):
 */
static int
cirp_logpb_arv_log_find_arv_num_internal (CIRP_BUF_MGR * buf_mgr,
					  int *arv_num, LOG_PAGEID pageid)
{
  int error;
  DKNPAGES npages;
  LOG_PAGEID fpageid;
  CIRP_ACT_LOG *act_log = NULL;
  CIRP_ARV_LOG *arv_log = NULL;
  struct log_arv_header *arv_log_hdr = NULL;
  int left, right;
  int find_arv_num;

  act_log = &buf_mgr->act_log;
  arv_log = &buf_mgr->arv_log;

  if (arv_log->last_deleted_arv_num < ARV_NUM_INITIAL_VAL)
    {
      cirp_logpb_validate_last_deleted_arv_num (buf_mgr);
      if (arv_log->last_deleted_arv_num < ARV_NUM_INITIAL_VAL)
	{
	  assert (false);

	  /* FIXME-notout: error logging */
	  return ER_FAILED;
	}
    }

  if (*arv_num > ARV_NUM_INITIAL_VAL)
    {
      find_arv_num = right = left = *arv_num;
    }
  else
    {
      left = MAX (0, arv_log->last_deleted_arv_num + 1);
      right = act_log->log_hdr->ha_info.nxarv_num - 1;
      find_arv_num = right;
    }

  do
    {
      error = cirp_logpb_arv_log_fetch_hdr (buf_mgr, find_arv_num);
      if (error != NO_ERROR)
	{
	  error = ER_LOG_NOTIN_ARCHIVE;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, pageid);
	  return error;
	}

      arv_log_hdr = buf_mgr->arv_log.log_hdr;
      assert (arv_log_hdr != NULL);

      fpageid = arv_log_hdr->fpageid;
      npages = arv_log_hdr->npages;

      if (pageid >= fpageid && pageid < fpageid + npages)
	{
	  *arv_num = find_arv_num;
	  return NO_ERROR;
	}
      else if (pageid < fpageid)
	{
	  right = find_arv_num - 1;
	  find_arv_num = CEIL_PTVDIV ((left + right), 2);
	}
      else if (pageid >= fpageid + npages)
	{
	  left = find_arv_num + 1;
	  find_arv_num = CEIL_PTVDIV ((left + right), 2);
	}
    }
  while (find_arv_num >= 0
	 && left <= right
	 && find_arv_num < act_log->log_hdr->ha_info.nxarv_num);

  error = ER_LOG_NOTIN_ARCHIVE;
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, pageid);
  return error;
}

/*
 * cirp_logpb_arv_log_find_arv_num()-
 *    retur: error code
 *
 *    arv_num(out):
 *    pageid(in):
 */
static int
cirp_logpb_arv_log_find_arv_num (CIRP_BUF_MGR * buf_mgr, int *arv_num,
				 LOG_PAGEID pageid)
{
  int error;
  struct log_arv_header *arv_log_hdr = NULL;
  LOG_PAGEID fpageid;
  int npages;

  assert (buf_mgr->act_log.log_hdr != NULL);

  *arv_num = ARV_NUM_INITIAL_VAL;

  if (buf_mgr->arv_log.log_hdr != NULL)
    {
      assert (buf_mgr->arv_log.vdes != NULL_VOLDES);

      arv_log_hdr = buf_mgr->arv_log.log_hdr;
      fpageid = arv_log_hdr->fpageid;
      npages = arv_log_hdr->npages;

      if (pageid >= fpageid && pageid < fpageid + npages)
	{
	  assert (arv_log_hdr->arv_num == buf_mgr->arv_log.arv_num);

	  *arv_num = arv_log_hdr->arv_num;
	  return NO_ERROR;
	}

      /* guess */
      if (pageid >= fpageid)
	{
	  *arv_num = buf_mgr->arv_log.arv_num + (pageid - fpageid) / npages;
	}
      else
	{
	  *arv_num = buf_mgr->arv_log.arv_num
	    - ((fpageid - pageid) / npages + 1);
	}
    }

retry_search:
  error = cirp_logpb_arv_log_find_arv_num_internal (buf_mgr, arv_num, pageid);
  if (error != NO_ERROR)
    {
      if (error == ER_LOG_NOTIN_ARCHIVE)
	{
	  if (*arv_num != ARV_NUM_INITIAL_VAL)
	    {
	      /* binary search */
	      *arv_num = ARV_NUM_INITIAL_VAL;
	      goto retry_search;
	    }
	  else
	    {
	      /* did not refresh log header */
	      error = ER_HA_LOG_PAGE_DOESNOT_EXIST;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	    }
	}

      return error;
    }

  assert (*arv_num >= 0);
  return NO_ERROR;
}

/*
 * cirp_logpb_find_last_deleted_arv_num()-
 *    return: archive number
 */
static int
cirp_logpb_find_last_deleted_arv_num (CIRP_BUF_MGR * buf_mgr)
{
  int arv_log_num;
  char arv_log_path[PATH_MAX];
  int arv_log_vdes = NULL_VOLDES;

  arv_log_num = buf_mgr->act_log.log_hdr->ha_info.nxarv_num - 1;
  while (arv_log_num >= 0)
    {
      /* make archive_name */
      fileio_make_log_archive_name (arv_log_path,
				    buf_mgr->log_path,
				    buf_mgr->prefix_name, arv_log_num);

      /* open the archive file */
      arv_log_vdes = fileio_open (arv_log_path, O_RDONLY, 0);
      if (arv_log_vdes == NULL_VOLDES)
	{
	  break;
	}

      fileio_close (arv_log_vdes);
      arv_log_num--;
    }

  return arv_log_num;
}

/*
 * cirp_logpb_validate_last_deleted_arv_num()-
 *    return:
 */
static void
cirp_logpb_validate_last_deleted_arv_num (CIRP_BUF_MGR * buf_mgr)
{
  int arv_num;
  CIRP_ARV_LOG *arv_log = &buf_mgr->arv_log;

  arv_num = cirp_logpb_find_last_deleted_arv_num (buf_mgr);
  assert (arv_num >= ARV_NUM_INITIAL_VAL);
  if (arv_num > arv_log->last_deleted_arv_num)
    {
      arv_log->last_deleted_arv_num = arv_num;
    }

  return;
}

/*
 * cirp_logpb_remove_archive_log_internal()-
 *    return: last removed archive number
 *
 *    first_arv_num(in):
 *    last_arv_num(in):
 *    max_arv_cnt_to_delete(in):
 */
static int
cirp_logpb_remove_archive_log_internal (CIRP_BUF_MGR * buf_mgr,
					int first_arv_num, int last_arv_num,
					int max_arv_cnt_to_delete)
{
  int i;
  const char *info_reason, *catmsg;
  char first_archive_name[PATH_MAX];
  char last_archive_name[PATH_MAX];

  if ((int) (first_arv_num + max_arv_cnt_to_delete) >= 0)
    {
      last_arv_num = MIN (last_arv_num,
			  first_arv_num + max_arv_cnt_to_delete);
    }

  if (first_arv_num > last_arv_num)
    {
      assert (false);
      return buf_mgr->arv_log.last_deleted_arv_num;
    }

  for (i = first_arv_num; i <= last_arv_num; i++)
    {
      cirp_logpb_arv_log_remove (buf_mgr, i);
    }

  info_reason = msgcat_message (MSGCAT_CATALOG_RYE,
				MSGCAT_SET_LOG,
				MSGCAT_LOG_MAX_ARCHIVES_HAS_BEEN_EXCEEDED);
  if (info_reason == NULL)
    {
      info_reason = "Number of active log archives has been exceeded"
	" the max desired number.";
    }
  catmsg = msgcat_message (MSGCAT_CATALOG_RYE,
			   MSGCAT_SET_LOG, MSGCAT_LOG_LOGINFO_REMOVE_REASON);
  if (catmsg == NULL)
    {
      catmsg = "REMOVE: %d %s to %d %s.\nREASON: %s\n";
    }

  fileio_make_log_archive_name (first_archive_name, buf_mgr->log_path,
				buf_mgr->prefix_name, first_arv_num);
  fileio_make_log_archive_name (last_archive_name, buf_mgr->log_path,
				buf_mgr->prefix_name, last_arv_num);
  log_dump_log_info (buf_mgr->log_info_path, false, catmsg,
		     first_arv_num,
		     fileio_get_base_file_name (first_archive_name),
		     last_arv_num,
		     fileio_get_base_file_name (last_archive_name),
		     info_reason);

  return last_arv_num;
}

/*
 * cirp_logpb_remove_archive_log()-
 *   return: error code
 *
 *   req_pageid(in):
 */
int
cirp_logpb_remove_archive_log (CIRP_BUF_MGR * buf_mgr, LOG_PAGEID req_pageid)
{
  int error = NO_ERROR;
  CIRP_ACT_LOG *act_log = NULL;
  CIRP_ARV_LOG *arv_log = NULL;
  int req_arv_num = ARV_NUM_INITIAL_VAL;
  int first_arv_num, last_arv_num;
  int last_deleted_arv_num;
  int max_arv_cnt_to_delete;
  int cnt_curr_archives, cnt_remain_archives;
  int timediff;
  time_t now;
  int rm_arv_intv_in_secs;
  int max_archives;

  act_log = &buf_mgr->act_log;
  arv_log = &buf_mgr->arv_log;

  if (act_log->log_hdr == NULL)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
	      "invalid function arguments");
      return error;
    }

  rm_arv_intv_in_secs =
    prm_get_integer_value (PRM_ID_REMOVE_LOG_ARCHIVES_INTERVAL);
  max_archives = prm_get_integer_value (PRM_ID_HA_COPY_LOG_MAX_ARCHIVES);

  if (max_archives < 0
      || max_archives == INT_MAX || req_pageid == NULL_PAGEID)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "invalid prm");

      return error;
    }

  if (buf_mgr->last_nxarv_num == ARV_NUM_INITIAL_VAL)
    {
      buf_mgr->last_nxarv_num = act_log->log_hdr->ha_info.nxarv_num;
    }

  now = time (NULL);
  max_arv_cnt_to_delete = 0;
  if (rm_arv_intv_in_secs == 0)
    {
      if (buf_mgr->last_nxarv_num != act_log->log_hdr->ha_info.nxarv_num)
	{
	  max_arv_cnt_to_delete = INT_MAX;
	}
    }
  else
    {
      timediff = now - buf_mgr->last_arv_deleted_time;
      if (timediff > rm_arv_intv_in_secs)
	{
	  max_arv_cnt_to_delete = 1;
	}
    }

  if (max_arv_cnt_to_delete == 0)
    {
      return NO_ERROR;
    }

  if (arv_log->last_deleted_arv_num < ARV_NUM_INITIAL_VAL)
    {
      cirp_logpb_validate_last_deleted_arv_num (buf_mgr);
    }

  if (CIRP_LOG_IS_IN_ARCHIVE (buf_mgr, req_pageid))
    {
      error = cirp_logpb_arv_log_find_arv_num (buf_mgr, &req_arv_num,
					       req_pageid);
      if (error != NO_ERROR)
	{
	  /* not found archive */
	  assert (error == ER_HA_LOG_PAGE_DOESNOT_EXIST);

	  return NO_ERROR;
	}
    }
  else
    {
      req_arv_num = act_log->log_hdr->ha_info.nxarv_num;
    }
  assert (req_arv_num >= 0);

  cnt_curr_archives = (act_log->log_hdr->ha_info.nxarv_num
		       - arv_log->last_deleted_arv_num - 1);
  cnt_remain_archives = MAX (max_archives,
			     act_log->log_hdr->ha_info.nxarv_num
			     - req_arv_num);
  if (cnt_curr_archives <= cnt_remain_archives)
    {
      return NO_ERROR;
    }

  first_arv_num = arv_log->last_deleted_arv_num + 1;
  last_arv_num = (act_log->log_hdr->ha_info.nxarv_num - 1
		  - cnt_remain_archives);
  assert (first_arv_num <= last_arv_num);

  last_deleted_arv_num = cirp_logpb_remove_archive_log_internal (buf_mgr,
								 first_arv_num,
								 last_arv_num,
								 max_arv_cnt_to_delete);
  assert (last_deleted_arv_num >= arv_log->last_deleted_arv_num);
  if (last_deleted_arv_num > arv_log->last_deleted_arv_num)
    {
      arv_log->last_deleted_arv_num = last_deleted_arv_num;
      buf_mgr->last_nxarv_num = act_log->log_hdr->ha_info.nxarv_num;
      buf_mgr->last_arv_deleted_time = now;
    }

  return NO_ERROR;
}

/*
 * cirp_logpb_cache_final () -
 *    return:
 */
static void
cirp_logpb_cache_final (CIRP_BUF_MGR * buf_mgr)
{
  CIRP_LOGPB_CACHE *cache = &buf_mgr->cache;
  CIRP_LOGPB_AREA *area, *next;;

  if (cache->hash_table != NULL)
    {
      mht_destroy (cache->hash_table);
      cache->hash_table = NULL;
    }
  cache->num_buffer = 0;
  if (cache->buffer != NULL)
    {
      free_and_init (cache->buffer);
    }

  area = cache->area_head;
  while (area != NULL)
    {
      next = area->next;
      free_and_init (area);
      area = next;
    }
  cache->area_head = NULL;

  return;
}

/*
 * cirp_logpb_cache_init () -
 *    return: error code
 *
 */
static int
cirp_logpb_cache_init (CIRP_BUF_MGR * buf_mgr)
{
  int error;
  CIRP_LOGPB_CACHE *cache = &buf_mgr->cache;

  error = cirp_logpb_expand_buffer (buf_mgr);
  if (error != NO_ERROR)
    {
      return error;
    }

  cache->hash_table = mht_create ("cache log buffer hash table for applying"
				  " replication log",
				  cache->num_buffer * 8, mht_logpageidhash,
				  mht_compare_logpageids_are_equal);
  if (cache->hash_table == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      cache->num_buffer * 8);
      error = ER_OUT_OF_VIRTUAL_MEMORY;
    }

  return error;
}

/*
 * cirp_logpb_fetch_from_archive()-
 *    return: error code
 *
 *    pageid(in):
 *    data(out):
 */
static int
cirp_logpb_fetch_from_archive (CIRP_BUF_MGR * buf_mgr,
			       LOG_PAGEID pageid, char *data)
{
  int error = NO_ERROR;
  int arv_num;
  LOG_PHY_PAGEID phy_pageid;
  CIRP_ARV_LOG *arv_log = &buf_mgr->arv_log;

  assert (CIRP_LOG_IS_IN_ARCHIVE (buf_mgr, pageid) == true);

  error = cirp_logpb_arv_log_find_arv_num (buf_mgr, &arv_num, pageid);
  if (error != NO_ERROR)
    {
      return error;
    }

  if (arv_log->vdes == NULL_VOLDES || arv_num < 0
      || arv_log->arv_num != arv_num)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "Invalid return value");
      return error;
    }

retry:
  assert (arv_log->path[0] != '\0' && arv_log->vdes != NULL_VOLDES);

  phy_pageid = pageid - arv_log->log_hdr->fpageid + 1;
  assert (phy_pageid >= 0 && phy_pageid <= arv_log->log_hdr->npages);

  error = cirp_log_io_read_with_max_retries (arv_log->path,
					     arv_log->vdes,
					     data,
					     phy_pageid,
					     buf_mgr->db_logpagesize);
  if (error != NO_ERROR)
    {
      assert (false);

      if (error == ER_PB_BAD_PAGEID)
	{
	  cirp_logpb_arv_log_close (buf_mgr);
	  goto retry;
	}
      else
	{
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_LOG_READ, 3,
		  pageid, phy_pageid, arv_log->path);

	  return ER_LOG_READ;
	}
    }

  assert (error == NO_ERROR);
  return error;
}

/*
 * cirp_logpb_fetch_from_active()-
 *    return: error code
 *
 *    pageid(in):
 *    data(out):
 */
static int
cirp_logpb_fetch_from_active (CIRP_BUF_MGR * buf_mgr,
			      LOG_PAGEID pageid, char *data)
{
  int error;
  LOG_PHY_PAGEID phy_pageid = NULL_PAGEID;

  assert (CIRP_LOG_IS_IN_ARCHIVE (buf_mgr, pageid) == false);

  phy_pageid = cirp_to_phy_pageid (buf_mgr, pageid);
  assert (phy_pageid >= 0);

  error = cirp_log_io_read (buf_mgr->act_log.path,
			    buf_mgr->act_log.vdes,
			    data, phy_pageid, buf_mgr->db_logpagesize);
  if (error != NO_ERROR)
    {
      return error;
    }

  return NO_ERROR;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * cirp_logtype_to_string()-
 *   return: type name
 *
 *   type(in):
 */
static const char *
cirp_logtype_to_string (LOG_RECTYPE type)
{
  switch (type)
    {
    case LOG_UNDOREDO_DATA:
      return "LOG_UNDOREDO_DATA";

    case LOG_DIFF_UNDOREDO_DATA:	/* LOG DIFF undo and redo data */
      return "LOG_DIFF_UNDOREDO_DATA";

    case LOG_UNDO_DATA:
      return "LOG_UNDO_DATA";

    case LOG_REDO_DATA:
      return "LOG_REDO_DATA";

    case LOG_DBEXTERN_REDO_DATA:
      return "LOG_DBEXTERN_REDO_DATA";

    case LOG_DUMMY_HEAD_POSTPONE:
      return "LOG_DUMMY_HEAD_POSTPONE";

    case LOG_POSTPONE:
      return "LOG_POSTPONE";

    case LOG_RUN_POSTPONE:
      return "LOG_RUN_POSTPONE";

    case LOG_COMPENSATE:
      return "LOG_COMPENSATE";

    case LOG_LCOMPENSATE:
      return "LOG_LCOMPENSATE";

    case LOG_COMMIT_WITH_POSTPONE:
      return "LOG_COMMIT_WITH_POSTPONE";

    case LOG_COMMIT:
      return "LOG_COMMIT";

    case LOG_COMMIT_TOPOPE_WITH_POSTPONE:
      return "LOG_COMMIT_TOPOPE_WITH_POSTPONE";

    case LOG_COMMIT_TOPOPE:
      return "LOG_COMMIT_TOPOPE";

    case LOG_ABORT:
      return "LOG_ABORT";

    case LOG_ABORT_TOPOPE:
      return "LOG_ABORT_TOPOPE";

    case LOG_START_CHKPT:
      return "LOG_START_CHKPT";

    case LOG_END_CHKPT:
      return "LOG_END_CHKPT";

    case LOG_SAVEPOINT:
      return "LOG_SAVEPOINT";

    case LOG_DUMMY_CRASH_RECOVERY:
      return "LOG_DUMMY_CRASH_RECOVERY";

      /*
       * This record is not generated no more.
       * It's kept for backward compatibility.
       */
    case LOG_DUMMY_FILLPAGE_FORARCHIVE:
      return "LOG_DUMMY_FILLPAGE_FORARCHIVE";

    case LOG_END_OF_LOG:
      return "LOG_END_OF_LOG";

    case LOG_REPLICATION_DATA:
      return "LOG_REPLICATION_DATA";
    case LOG_REPLICATION_SCHEMA:
      return "LOG_REPLICATION_SCHEMA";

    case LOG_DUMMY_HA_SERVER_STATE:
      return "LOG_DUMMY_HA_SERVER_STATE";
    case LOG_DUMMY_OVF_RECORD:
    case LOG_DUMMY_OVF_RECORD_DEL:
      return "LOG_DUMMY_OVF_RECORD";
    case LOG_DUMMY_RECORD:
      return "LOG_DUMMY_RECORD";
    case LOG_DUMMY_UPDATE_GID_BITMAP:
      return "LOG_DUMMY_UPDATE_GID_BITMAP";

    case LOG_SMALLER_LOGREC_TYPE:
    case LOG_LARGER_LOGREC_TYPE:
      break;
    }

  return "UNKNOWN_LOG_REC_TYPE";

}

int
repl_dump_page (FILE * out_fp, LOG_PAGE * log_page)
{
  LOG_LSA lsa;
  LOG_RECORD_HEADER *log_rec;

  lsa.pageid = log_page->hdr.logical_pageid;
  lsa.offset = log_page->hdr.offset;

  while (lsa.pageid == log_page->hdr.logical_pageid)
    {
      log_rec = LOG_GET_LOG_RECORD_HEADER (log_page, &lsa);

      fprintf (out_fp, "\nLSA = %lld|%d, Forw log = %lld|%d,"
	       " Backw log = %lld|%d,\n"
	       "     Trid = %d, Prev tran logrec = %lld|%d\n"
	       "     Type = %s",
	       (long long) lsa.pageid, lsa.offset,
	       (long long) log_rec->forw_lsa.pageid, log_rec->forw_lsa.offset,
	       (long long) log_rec->back_lsa.pageid, log_rec->back_lsa.offset,
	       log_rec->trid,
	       (long long) log_rec->prev_tranlsa.pageid,
	       log_rec->prev_tranlsa.offset,
	       cirp_logtype_to_string (log_rec->type));

      LSA_COPY (&lsa, &log_rec->forw_lsa);
    }

  return NO_ERROR;
}
#endif

/*
 * cirp_logpb_fetch ()
 *    return:error code
 *
 *    pageid(in):
 *    logpb(in/out):
 */
static int
cirp_logpb_fetch (CIRP_BUF_MGR * buf_mgr, LOG_PAGEID pageid,
		  CIRP_LOGPB * logpb)
{
  int error = NO_ERROR;
  CIRP_ACT_LOG *act_log;
  CIRP_WRITER_INFO *writer;
  bool has_mutex = false;

  writer = &Repl_Info->writer_info;
  act_log = &buf_mgr->act_log;

  if (pageid > act_log->log_hdr->append_lsa.pageid
      || pageid > act_log->log_hdr->eof_lsa.pageid
      || pageid > act_log->log_hdr->ha_info.last_flushed_pageid)
    {
      error = ER_HA_LOG_PAGE_DOESNOT_EXIST;
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, error, 0);

      GOTO_EXIT_ON_ERROR;
    }

  if (CIRP_LOG_IS_IN_ARCHIVE (buf_mgr, pageid))
    {
      error = cirp_logpb_fetch_from_archive (buf_mgr, pageid,
					     (char *) &logpb->log_page);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      logpb->in_archive = true;
    }
  else
    {
      error = pthread_mutex_lock (&writer->lock);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      assert (writer->is_archiving == false);

      writer->reader_count++;

      pthread_mutex_unlock (&writer->lock);

      has_mutex = true;

      error = cirp_logpb_fetch_from_active (buf_mgr, pageid,
					    (char *) &logpb->log_page);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      logpb->in_archive = false;

      error = pthread_mutex_lock (&writer->lock);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      assert (writer->is_archiving == false);
      writer->reader_count--;
      pthread_cond_signal (&writer->cond);

      pthread_mutex_unlock (&writer->lock);
      has_mutex = false;
    }

  if (logpb->log_page.hdr.logical_pageid != pageid)
    {
      /* did not refresh log header */
      error = ER_HA_LOG_PAGE_DOESNOT_EXIST;

      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

      GOTO_EXIT_ON_ERROR;
    }

  logpb->pageid = pageid;

  assert (error == NO_ERROR);
  return error;

exit_on_error:

  if (error == NO_ERROR)
    {
      assert (false);
      error = ER_GENERIC_ERROR;
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "invalid error code");
    }

  if (has_mutex == true)
    {
      pthread_mutex_lock (&writer->lock);
      writer->reader_count--;
      pthread_cond_signal (&writer->cond);
      pthread_mutex_unlock (&writer->lock);
      has_mutex = false;
    }

  return error;
}

/*
 * cirp_logpb_replace_buffer()-
 *    return: error code
 *
 *    logpb(out):
 *    pageid(in):
 */
static int
cirp_logpb_replace_buffer (CIRP_BUF_MGR * buf_mgr, CIRP_LOGPB ** out_logpb,
			   LOG_PAGEID pageid)
{
  int error = NO_ERROR;
  CIRP_LOGPB_CACHE *cache;
  CIRP_LOGPB *logpb = NULL;
  int i, num_recently_freed, found;
  static unsigned int last = 0;

  if (out_logpb == NULL)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
	      "Invalid arguments");
      return error;
    }
  *out_logpb = NULL;

  cache = &buf_mgr->cache;

  found = -1;
  while (found < 0)
    {
      num_recently_freed = 0;

      for (i = 0; i < cache->num_buffer; i++)
	{
	  last = ((last + 1) % cache->num_buffer);
	  logpb = cache->buffer[last];
	  if (logpb->num_fixed == 0)
	    {
	      if (logpb->recently_freed == true)
		{
		  logpb->recently_freed = false;
		  num_recently_freed++;
		}
	      else
		{
		  found = last;
		  break;
		}
	    }
	}

      if (found >= 0)
	{
	  if (logpb->pageid != NULL_PAGEID)
	    {
	      (void) mht_rem (cache->hash_table, &logpb->pageid, NULL, NULL);
	    }

	  cirp_logpb_clear_logpb (logpb);

	  error = cirp_logpb_fetch (buf_mgr, pageid, logpb);
	  if (error != NO_ERROR)
	    {
	      cirp_logpb_clear_logpb (logpb);
	      return error;
	    }

	  *out_logpb = logpb;

	  assert (logpb->pageid > NULL_PAGEID);
	  assert (error == NO_ERROR);
	  return NO_ERROR;
	}

      if (num_recently_freed > 0)
	{
	  continue;
	}

      error = cirp_logpb_expand_buffer (buf_mgr);
      if (error != NO_ERROR)
	{
	  return error;
	}
    }

  assert (false);
  error = ER_GENERIC_ERROR;
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "fatal error");

  return error;
}

/*
 * cirp_logpb_get_page_buffer_debug()-
 *    return: log buffer
 *
 *    out_logpb(out):
 *    pageid(in):
 *    file_name(in):
 *    line_number(in):
 */
int
cirp_logpb_get_page_buffer_debug (CIRP_BUF_MGR * buf_mgr,
				  CIRP_LOGPB ** out_logpb,
				  LOG_PAGEID pageid,
				  UNUSED_ARG const char *file_name,
				  UNUSED_ARG int line_number)
{
  CIRP_LOGPB_CACHE *cache;
  CIRP_LOGPB *logpb = NULL;
  int error = NO_ERROR;

  cache = &buf_mgr->cache;

  if (out_logpb == NULL || pageid < NULL_PAGEID)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
	      "Invalid arguments");
      return error;
    }

  *out_logpb = NULL;

  logpb = (CIRP_LOGPB *) mht_get (cache->hash_table, (void *) &pageid);
  if (logpb == NULL)
    {
      error = cirp_logpb_replace_buffer (buf_mgr, &logpb, pageid);
      if (error != NO_ERROR || logpb == NULL)
	{
	  assert (error != NO_ERROR && logpb == NULL);
	  if (error == NO_ERROR)
	    {
	      REPL_SET_GENERIC_ERROR (error, "Invalid return value");
	    }

	  return error;
	}

      (void) mht_rem (cache->hash_table, &logpb->pageid, NULL, NULL);

      if (mht_put (cache->hash_table, &logpb->pageid, logpb) == NULL)
	{
	  cirp_logpb_clear_logpb (logpb);

	  REPL_SET_GENERIC_ERROR (error, "memory hash table error");
	  return error;
	}

      logpb->num_fixed = 1;
    }
  else
    {
      logpb->num_fixed++;
    }

  if (logpb->log_page.hdr.logical_pageid != pageid)
    {
      assert (false);

      (void) mht_rem (cache->hash_table, &logpb->pageid, NULL, NULL);
      cirp_logpb_clear_logpb (logpb);

      REPL_SET_GENERIC_ERROR (error, "Invalid log page");
      return error;
    }

#if !defined(NDEBUG)
  {
    strncpy (logpb->fix_file_name, file_name, 1024);
    logpb->fix_line_number = line_number;
  }
#endif

  *out_logpb = logpb;

  assert (error == NO_ERROR);
  return NO_ERROR;
}

/*
 * cirp_logpb_get_log_page_debug()-
 *   return: log page
 *
 *   log_page(out):
 *   pageid(in):
 *   file_name(in):
 *   line_number(in):
 */
#if !defined(NDEBUG)
int
cirp_logpb_get_log_page_debug (CIRP_BUF_MGR * buf_mgr,
			       LOG_PAGE ** log_page,
			       LOG_PAGEID pageid,
			       UNUSED_ARG const char *file_name,
			       UNUSED_ARG int line_number)
#else
int
cirp_logpb_get_log_page (CIRP_BUF_MGR * buf_mgr,
			 LOG_PAGE ** log_page, LOG_PAGEID pageid)
#endif
{
  CIRP_LOGPB *logpb = NULL;
  int error = NO_ERROR;

  if (pageid == NULL_PAGEID || log_page == NULL)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "Invalid arguments");
      return error;
    }
  *log_page = NULL;

  error = cirp_logpb_get_page_buffer (buf_mgr, &logpb, pageid);
  if (error != NO_ERROR || logpb == NULL)
    {
      assert (error != NO_ERROR && logpb == NULL);

      if (error == NO_ERROR)
	{
	  assert (false);
	  error = ER_FAILED;
	}
      return error;
    }
  assert (logpb != NULL);

#if !defined(NDEBUG)
  {
    strncpy (logpb->fix_file_name, file_name, 1024);
    logpb->fix_line_number = line_number;
  }
#endif

  *log_page = &logpb->log_page;

  assert (error == NO_ERROR);
  return NO_ERROR;
}

/*
 * cirp_logpb_release_debug()-
 *    return: error code
 *
 *    pageid(in):
 *    file_name(in):
 *    line_number(in):
 */
int
cirp_logpb_release_debug (CIRP_BUF_MGR * buf_mgr, LOG_PAGEID pageid,
			  UNUSED_ARG const char *file_name,
			  UNUSED_ARG int line_number)
{
  CIRP_LOGPB_CACHE *cache;
  CIRP_LOGPB *logpb = NULL;
  int error = NO_ERROR;

  cache = &buf_mgr->cache;

  assert (pageid >= NULL_PAGEID);
  if (pageid == NULL_PAGEID)
    {
      /* already released */
      return NO_ERROR;
    }

  logpb = (CIRP_LOGPB *) mht_get (cache->hash_table, (void *) &pageid);
  if (logpb == NULL)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "repl log buffer error");
      return error;
    }

  logpb->num_fixed--;
  if (logpb->num_fixed < 0)
    {
      assert (false);
      logpb->num_fixed = 0;
    }
  logpb->recently_freed = true;

#if !defined(NDEBUG)
  {
    strncpy (logpb->unfix_file_name, file_name, 1024);
    logpb->unfix_line_number = line_number;
  }
#endif

  assert (error == NO_ERROR);
  return NO_ERROR;
}

/*
 * cirp_logpb_release_all ()
 *    return NO_ERROR;
 *
 *    exclude_pageid(in):
 */
int
cirp_logpb_release_all (CIRP_BUF_MGR * buf_mgr, LOG_PAGEID exclude_pageid)
{
  int i;
  CIRP_LOGPB_CACHE *cache;
  CIRP_LOGPB *logpb;

  cache = &buf_mgr->cache;

  for (i = 0; i < cache->num_buffer; i++)
    {
      logpb = cache->buffer[i];
      if (logpb->pageid == exclude_pageid)
	{
	  continue;
	}

      if (logpb->num_fixed > 0)
	{
	  assert (false);

	  logpb->num_fixed = 0;
	  logpb->recently_freed = true;
	}
    }

  return NO_ERROR;
}

/*
 * cirp_logpb_decache_range()-
 *    return: NO_ERROR
 *
 *    from(in):
 *    to(in):
 */
int
cirp_logpb_decache_range (CIRP_BUF_MGR * buf_mgr, LOG_PAGEID from,
			  LOG_PAGEID to)
{
  int i;
  CIRP_LOGPB_CACHE *cache;
  CIRP_LOGPB *logpb;

  cache = &buf_mgr->cache;

  for (i = 0; i < cache->num_buffer; i++)
    {
      logpb = cache->buffer[i];

      if (logpb->pageid == NULL_PAGEID
	  || logpb->pageid < from || logpb->pageid > to)
	{
	  continue;
	}

      (void) mht_rem (cache->hash_table, &logpb->pageid, NULL, NULL);

      cirp_logpb_clear_logpb (logpb);
    }

  return NO_ERROR;
}

/*
 * cirp_logpb_expand_buffer()-
 *    return: error code
 */
static int
cirp_logpb_expand_buffer (CIRP_BUF_MGR * buf_mgr)
{
  int error = NO_ERROR;
  int i, size, new_num_buffer, bufferid;
  CIRP_LOGPB_CACHE *cache = NULL;
  CIRP_LOGPB_AREA *new_area = NULL;
  CIRP_LOGPB **new_buffer = NULL;

  cache = &buf_mgr->cache;

  size = ((SIZEOF_CIRP_LOGPB * CIRP_LOGPB_AREA_SIZE)
	  + DB_SIZEOF (CIRP_LOGPB_AREA));
  new_area = (CIRP_LOGPB_AREA *) malloc (size);
  if (new_area == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, size);

      GOTO_EXIT_ON_ERROR;
    }
  memset ((void *) new_area, 0, size);

  new_num_buffer = cache->num_buffer + CIRP_LOGPB_AREA_SIZE;
  new_buffer = (CIRP_LOGPB **) realloc (cache->buffer,
					new_num_buffer
					* DB_SIZEOF (CIRP_LOGPB *));
  if (new_buffer == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;

      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, new_num_buffer * DB_SIZEOF (CIRP_LOGPB *));

      GOTO_EXIT_ON_ERROR;
    }

  new_area->area = ((CIRP_LOGPB *) ((char *) new_area
				    + DB_SIZEOF (CIRP_LOGPB_AREA)));
  new_area->next = cache->area_head;
  for (i = 0, bufferid = cache->num_buffer; i < CIRP_LOGPB_AREA_SIZE;
       i++, bufferid++)
    {
      new_buffer[bufferid] = (CIRP_LOGPB *) ((char *) new_area->area
					     + i * SIZEOF_CIRP_LOGPB);

      cirp_logpb_clear_logpb (new_buffer[bufferid]);
    }

  cache->num_buffer = new_num_buffer;
  cache->buffer = new_buffer;
  cache->area_head = new_area;

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  assert (error != NO_ERROR);

  if (new_area != NULL)
    {
      free_and_init (new_area);
    }

  return error;
}

/*
 * rp_log_read_advance_when_doesnt_fit -
 *   return: error code
 *
 *   pgptr(in/out):
 *   pageid(in/out):
 *   offset(in/out):
 *   length(in):
 *   org_pgptr(in):
 *
 */
int
rp_log_read_advance_when_doesnt_fit (CIRP_BUF_MGR * buf_mgr,
				     LOG_PAGE ** pgptr, LOG_PAGEID * pageid,
				     PGLENGTH * offset, int length,
				     LOG_PAGE * org_pgptr)
{
  int error = NO_ERROR;

  if ((*offset) + length >= CIRP_LOGAREA_SIZE (buf_mgr))
    {
      if ((org_pgptr) != (*pgptr))
	{
	  cirp_logpb_release (buf_mgr, (*pgptr)->hdr.logical_pageid);
	}

      *pageid = *pageid + 1;
      error = cirp_logpb_get_log_page (buf_mgr, pgptr, *pageid);
      if (error != NO_ERROR || (*pgptr) == NULL)
	{
	  assert (error != NO_ERROR && (*pgptr) == NULL);

	  return error;
	}
      (*offset) = 0;
    }

  return NO_ERROR;
}

/*
 * rp_log_read_align ()
 *   return: error code
 *
 *   pgptr(in/out):
 *   pageid(in/out):
 *   offset(in/out):
 *   org_pgptr(in):
 */
int
rp_log_read_align (CIRP_BUF_MGR * buf_mgr, LOG_PAGE ** pgptr,
		   LOG_PAGEID * pageid, PGLENGTH * offset,
		   LOG_PAGE * org_pgptr)
{
  int error = NO_ERROR;

  *offset = DB_ALIGN (*offset, MAX_ALIGNMENT);
  while ((*offset) >= CIRP_LOGAREA_SIZE (buf_mgr))
    {
      if ((*pgptr) != org_pgptr)
	{
	  cirp_logpb_release (buf_mgr, (*pgptr)->hdr.logical_pageid);
	}

      *pageid = *pageid + 1;
      error = cirp_logpb_get_log_page (buf_mgr, pgptr, *pageid);
      if (error != NO_ERROR || (*pgptr) == NULL)
	{
	  assert (error != NO_ERROR && (*pgptr) == NULL);
	  return error;
	}

      *offset -= CIRP_LOGAREA_SIZE (buf_mgr);
      *offset = DB_ALIGN (*offset, MAX_ALIGNMENT);
    }

  return NO_ERROR;
}

/*
 * rp_log_read_add_align ()
 *   return: error code
 *
 *   pgptr(in/out):
 *   pageid(in/out):
 *   offset(in/out):
 *   add_length(in):
 *   org_pgptr(in):

 */
int
rp_log_read_add_align (CIRP_BUF_MGR * buf_mgr, LOG_PAGE ** pgptr,
		       LOG_PAGEID * pageid, PGLENGTH * offset, int add_length,
		       LOG_PAGE * org_pgptr)
{
  *offset += add_length;
  return rp_log_read_align (buf_mgr, pgptr, pageid, offset, org_pgptr);
}

/*
 * cirp_logpb_clear_logpb()-
 *    return:
 *
 *    logpb(out):
 */
static void
cirp_logpb_clear_logpb (CIRP_LOGPB * logpb)
{
  logpb->num_fixed = 0;
  logpb->recently_freed = false;
  logpb->in_archive = false;
  logpb->pageid = NULL_PAGEID;

  logpb->log_page.hdr.logical_pageid = NULL_PAGEID;
  logpb->log_page.hdr.offset = NULL_OFFSET;

  return;
}

/*
 * cirp_logpb_common_final()-
 *    return:
 */
static void
cirp_logpb_common_final (CIRP_BUF_MGR * buf_mgr)
{
  buf_mgr->log_path[0] = '\0';
  buf_mgr->prefix_name[0] = '\0';
  buf_mgr->host_info = prm_get_null_node_info ();
  buf_mgr->log_info_path[0] = '\0';

  buf_mgr->last_nxarv_num = ARV_NUM_INITIAL_VAL;
  buf_mgr->last_arv_deleted_time = 0;

  return;
}

/*
 * cirp_logpb_init_buffer_manager()-
 *    return:
 *
 *    buf_msg(out):
 */
static int
cirp_logpb_init_buffer_manager (CIRP_BUF_MGR * buf_mgr)
{
  assert (buf_mgr != NULL);

  buf_mgr->log_path[0] = '\0';
  buf_mgr->prefix_name[0] = '\0';
  buf_mgr->host_info = prm_get_null_node_info ();
  buf_mgr->log_info_path[0] = '\0';

  buf_mgr->act_log.path[0] = '\0';
  buf_mgr->act_log.vdes = NULL_VOLDES;
  buf_mgr->act_log.hdr_page = NULL;
  buf_mgr->act_log.log_hdr = NULL;

  buf_mgr->arv_log.arv_num = -1;
  buf_mgr->arv_log.path[0] = '\0';
  buf_mgr->arv_log.vdes = NULL_VOLDES;
  buf_mgr->arv_log.hdr_page = NULL;
  buf_mgr->arv_log.log_hdr = NULL;
  buf_mgr->arv_log.last_deleted_arv_num = ARV_NUM_INITIAL_VAL - 1;

  buf_mgr->cache.hash_table = NULL;
  buf_mgr->cache.num_buffer = 0;
  buf_mgr->cache.buffer = 0;
  buf_mgr->cache.area_head = NULL;

  buf_mgr->db_logpagesize = 0;

  buf_mgr->rec_type = NULL;
  buf_mgr->undo_unzip = NULL;
  buf_mgr->redo_unzip = NULL;

  buf_mgr->last_nxarv_num = ARV_NUM_INITIAL_VAL;
  buf_mgr->last_arv_deleted_time = 0;

  return NO_ERROR;
}

/*
 * cirp_logpb_common_init
 */
static int
cirp_logpb_common_init (CIRP_BUF_MGR * buf_mgr, const char *db_name,
			const char *log_path)
{
  char *p;

  cirp_logpb_init_buffer_manager (buf_mgr);

  strncpy (buf_mgr->log_path, log_path, sizeof (buf_mgr->log_path) - 1);

  strncpy (buf_mgr->prefix_name, db_name, sizeof (buf_mgr->prefix_name) - 1);
  buf_mgr->prefix_name[sizeof (buf_mgr->prefix_name) - 1] = '\0';
  p = strchr (buf_mgr->prefix_name, '@');
  if (p != NULL)
    {
      *p = '\0';
    }

  buf_mgr->host_info = cirp_get_hostname_from_log_path (log_path,
							buf_mgr->prefix_name);

  fileio_make_log_info_name (buf_mgr->log_info_path, log_path,
			     buf_mgr->prefix_name);

  buf_mgr->last_nxarv_num = ARV_NUM_INITIAL_VAL;
  buf_mgr->last_arv_deleted_time = 0;

  return NO_ERROR;
}

/*
 * cirp_logpb_final() -
 *    return:
 */
void
cirp_logpb_final (CIRP_BUF_MGR * buf_mgr)
{
  if (buf_mgr->is_initialized == false)
    {
      return;
    }

  cirp_logpb_cache_final (buf_mgr);
  cirp_logpb_arv_log_final (buf_mgr);
  cirp_logpb_act_log_final (buf_mgr);
  cirp_logpb_compress_final (buf_mgr);
  cirp_logpb_common_final (buf_mgr);
  cirp_clear_recdes_pool (buf_mgr);


  buf_mgr->is_initialized = false;

  return;
}

/*
 * cirp_clear_recdes_pool() - free allocated memory in recdes pool
 *                          and clear recdes pool info
 *   return: error code
 */
static void
cirp_clear_recdes_pool (CIRP_BUF_MGR * buf_mgr)
{
  int i;
  CIRP_RECDES_POOL *pool = NULL;
  RECDES *recdes;

  pool = &buf_mgr->la_recdes_pool;

  if (pool->is_initialized == false)
    {
      return;
    }

  if (pool->recdes_arr != NULL)
    {
      for (i = 0; i < pool->num_recdes; i++)
	{
	  recdes = &pool->recdes_arr[i];
	  if (recdes->area_size > pool->db_page_size)
	    {
	      free_and_init (recdes->data);
	    }
	}
      free_and_init (pool->recdes_arr);
    }

  if (pool->area != NULL)
    {
      free_and_init (pool->area);
    }

  pool->db_page_size = 0;
  pool->next_idx = 0;
  pool->num_recdes = 0;
  pool->is_initialized = false;

  return;
}

/*
 * cirp_realloc_recdes_data() - realloc area of given recdes
 *   return: error code
 *
 *   buf_mgr(in/out):
 *   recdes(in/out):
 *   data_size(in):
 *
 */
int
cirp_realloc_recdes_data (CIRP_BUF_MGR * buf_mgr, RECDES * recdes,
			  int data_size)
{
  CIRP_RECDES_POOL *pool = NULL;
  int error = NO_ERROR;

  pool = &buf_mgr->la_recdes_pool;

  if (pool->is_initialized == false)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "Invalid arguments");
      return error;
    }

  if (recdes->area_size < data_size)
    {
      if (recdes->area_size > pool->db_page_size)
	{
	  /* recdes->data was realloced by previous operation */
	  free_and_init (recdes->data);
	}

      recdes->data = (char *) malloc (data_size);
      if (recdes->data == NULL)
	{
	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, data_size);
	  return error;
	}

      recdes->area_size = data_size;
    }
  recdes->length = 0;

  assert (error == NO_ERROR);
  return NO_ERROR;
}

/*
 * cirp_init_recdes_pool() - initialize recdes pool
 *   return:
 *
 *   bug_mgr(out):
 *   page_size(in):
 *   num_recdes(in):
 *
 * Note:
 */
static int
cirp_init_recdes_pool (CIRP_BUF_MGR * buf_mgr, int page_size, int num_recdes)
{
  int i;
  char *p;
  CIRP_RECDES_POOL *pool;
  RECDES *recdes;

  assert (page_size >= IO_MIN_PAGE_SIZE && page_size <= IO_MAX_PAGE_SIZE);

  pool = &buf_mgr->la_recdes_pool;

  if (pool->is_initialized == false)
    {
      pool->area = (char *) malloc (page_size * num_recdes);
      if (pool->area == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OUT_OF_VIRTUAL_MEMORY, 1, page_size * num_recdes);
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      pool->recdes_arr = (RECDES *) malloc (sizeof (RECDES) * num_recdes);
      if (pool->recdes_arr == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (RECDES) * num_recdes);
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      p = pool->area;
      for (i = 0; i < num_recdes; i++)
	{
	  recdes = &pool->recdes_arr[i];

	  recdes->data = p;
	  recdes->area_size = page_size;
	  recdes->length = 0;

	  p += page_size;
	}

      pool->db_page_size = page_size;
      pool->num_recdes = num_recdes;
      pool->is_initialized = true;
    }
  else if (pool->db_page_size != page_size || pool->num_recdes != num_recdes)
    {
      assert (false);
      cirp_clear_recdes_pool (buf_mgr);

      return cirp_init_recdes_pool (buf_mgr, page_size, num_recdes);
    }

  pool->next_idx = 0;

  return NO_ERROR;
}

/*
 * cirp_logpb_initialize() -
 *    return: error code
 *
 *    db_name(in):
 *    log_path(in):
 */
int
cirp_logpb_initialize (CIRP_BUF_MGR * buf_mgr, const char *db_name,
		       const char *log_path)
{
  int error;

  if (buf_mgr->is_initialized == true)
    {
      return NO_ERROR;
    }

  error = cirp_logpb_common_init (buf_mgr, db_name, log_path);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  buf_mgr->is_initialized = true;


  error = cirp_logpb_compress_init (buf_mgr);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = cirp_logpb_act_log_init (buf_mgr, (const char *) buf_mgr->log_path,
				   (const char *) buf_mgr->prefix_name);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = cirp_logpb_arv_log_init (buf_mgr);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = cirp_logpb_cache_init (buf_mgr);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = cirp_init_recdes_pool (buf_mgr, IO_MAX_PAGE_SIZE,
				 LA_MAX_UNFLUSHED_REPL_ITEMS);
  if (error != NO_ERROR)
    {
      er_log_debug (ARG_FILE_LINE, "Cannot initialize recdes pool");

      GOTO_EXIT_ON_ERROR;
    }

  assert (error == NO_ERROR);
  return NO_ERROR;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);
      REPL_SET_GENERIC_ERROR (error, "Invalid error code");
    }

  cirp_logpb_final (buf_mgr);

  return error;
}

/*
 * rp_assign_recdes_from_pool() - get a recdes from pool
 *   return: error code
 *
 *   buf_mgr(in/out):
 *   rec(out): a recdes having area with size of db page size
 *
 * Note: if a recdes that is about to be assigned has an area
 * greater than db page size, then it first frees the area.
 */
int
rp_assign_recdes_from_pool (CIRP_BUF_MGR * buf_mgr, RECDES ** rec)
{
  CIRP_RECDES_POOL *pool;
  RECDES *recdes;
  int error = NO_ERROR;

  if (buf_mgr == NULL || rec == NULL)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "Invalid arguments");
      return error;
    }
  *rec = NULL;

  pool = &buf_mgr->la_recdes_pool;

  if (pool->is_initialized == false)
    {
      REPL_SET_GENERIC_ERROR (error, "log buffer pool not initialized");

      return error;
    }

  recdes = &pool->recdes_arr[pool->next_idx];
  assert (recdes != NULL && recdes->data != NULL);

  if (recdes->area_size > pool->db_page_size)
    {
      /* recdes->data was realloced by previous operation */
      free_and_init (recdes->data);

      recdes->data = pool->area + pool->db_page_size * pool->next_idx;
      recdes->area_size = pool->db_page_size;
    }

  recdes->length = 0;
  pool->next_idx++;
  pool->next_idx %= pool->num_recdes;

  *rec = recdes;

  assert (error == NO_ERROR);
  return NO_ERROR;
}


/*
 * cirp_log_get_eot_time() - get the timestamp of End of Transaction
 *   return:error code
 *
 *   donetime(out):
 *   pgptr(in):
 *   lsa(in):
 */
int
cirp_log_get_eot_time (CIRP_BUF_MGR * buf_mgr, time_t * donetime,
		       LOG_PAGE * org_pgptr, LOG_LSA lsa)
{
  int error = NO_ERROR;
  LOG_PAGEID pageid;
  PGLENGTH offset;
  LOG_PAGE *pgptr;

  assert (donetime != NULL);

  /* init out parameter */
  *donetime = 0;

  pageid = lsa.pageid;
  offset = lsa.offset + DB_SIZEOF (LOG_RECORD_HEADER);

  pgptr = org_pgptr;
  error = rp_log_read_align (buf_mgr, &pgptr, &pageid, &offset, org_pgptr);
  if (error != NO_ERROR || pgptr == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = rp_log_read_advance_when_doesnt_fit (buf_mgr, &pgptr, &pageid,
					       &offset,
					       SSIZEOF (struct log_donetime),
					       org_pgptr);
  if (error != NO_ERROR || pgptr == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  *donetime =
    ((struct log_donetime *) ((char *) pgptr->area + offset))->at_time;

  if (pgptr != NULL && pgptr != org_pgptr)
    {
      cirp_logpb_release (buf_mgr, pgptr->hdr.logical_pageid);
    }

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "Invalid error code");
    }

  if (pgptr != NULL && pgptr != org_pgptr)
    {
      cirp_logpb_release (buf_mgr, pgptr->hdr.logical_pageid);
    }

  return error;
}

/*
 * cirp_log_get_ha_server_state ()
 *    return: error code
 *
 *    state(out):
 *    pgptr(in):
 *    lsa(in):
 */
int
cirp_log_get_ha_server_state (struct log_ha_server_state *state,
			      LOG_PAGE * org_pgptr, LOG_LSA lsa)
{
  int error = NO_ERROR;
  LOG_PAGEID pageid;
  PGLENGTH offset;
  int length;
  LOG_PAGE *pgptr = NULL;

  CIRP_BUF_MGR *buf_mgr = NULL;

  buf_mgr = &Repl_Info->analyzer_info.buf_mgr;

  if (state == NULL || org_pgptr == NULL)
    {
      assert (false);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "Invalid arguments");
      return error;
    }

  pageid = lsa.pageid;
  offset = DB_SIZEOF (LOG_RECORD_HEADER) + lsa.offset;
  pgptr = org_pgptr;

  length = DB_SIZEOF (struct log_ha_server_state);
  error = rp_log_read_advance_when_doesnt_fit (buf_mgr, &pgptr, &pageid,
					       &offset, length, org_pgptr);
  if (error != NO_ERROR || pgptr == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  *state = *((struct log_ha_server_state *) ((char *) pgptr->area + offset));

  if (pgptr != NULL && pgptr != org_pgptr)
    {
      cirp_logpb_release (buf_mgr, pgptr->hdr.logical_pageid);
    }

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  assert (error != NO_ERROR);

  if (pgptr != NULL && pgptr != org_pgptr)
    {
      cirp_logpb_release (buf_mgr, pgptr->hdr.logical_pageid);
    }

  return error;
}


/*
 * cirp_log_copy_fromlog ()
 *    return:error code
 *
 *    rec_type(out):
 *    area(out):
 *    length(in):
 *    log_pageid(in):
 *    log_offset(in):
 *    log_pgptr(in):
 */
int
cirp_log_copy_fromlog (CIRP_BUF_MGR * buf_mgr, char *rec_type,
		       char *area, int length,
		       LOG_PAGEID log_pageid, PGLENGTH log_offset,
		       LOG_PAGE * org_pgptr)
{
  int rec_length = (int) sizeof (INT16);
  int copy_length;		/* Length to copy into area */
  int t_length;			/* target length  */
  int area_offset = 0;		/* The area offset */
  int error = NO_ERROR;
  LOG_PAGE *pgptr;

  pgptr = org_pgptr;

  /* filter the record type */
  /* NOTES : in case of overflow page, we don't need to fetch the rectype */
  if (rec_type != NULL)
    {
      while (rec_length > 0)
	{
	  error = rp_log_read_advance_when_doesnt_fit (buf_mgr, &pgptr,
						       &log_pageid,
						       &log_offset, 0,
						       org_pgptr);
	  if (error != NO_ERROR || pgptr == NULL)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

	  copy_length =
	    ((log_offset + rec_length <=
	      CIRP_LOGAREA_SIZE (buf_mgr)) ? rec_length :
	     CIRP_LOGAREA_SIZE (buf_mgr) - log_offset);
	  memcpy (rec_type + area_offset, (char *) (pgptr)->area + log_offset,
		  copy_length);
	  rec_length -= copy_length;
	  area_offset += copy_length;
	  log_offset += copy_length;
	  length -= copy_length;
	}

      /* skip class_oid */
      rec_length = (int) sizeof (OID);
      while (rec_length > 0)
	{
	  error = rp_log_read_advance_when_doesnt_fit (buf_mgr, &pgptr,
						       &log_pageid,
						       &log_offset, 0,
						       org_pgptr);
	  if (error != NO_ERROR || pgptr == NULL)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

	  if ((log_offset + rec_length) <= CIRP_LOGAREA_SIZE (buf_mgr))
	    {
	      copy_length = rec_length;
	    }
	  else
	    {
	      copy_length = CIRP_LOGAREA_SIZE (buf_mgr) - log_offset;
	    }

	  rec_length -= copy_length;
	  log_offset += copy_length;
	  length -= copy_length;
	}
    }

  area_offset = 0;
  t_length = length;

  /* The log data is not contiguous */
  while (t_length > 0)
    {
      error = rp_log_read_advance_when_doesnt_fit (buf_mgr, &pgptr,
						   &log_pageid, &log_offset,
						   0, org_pgptr);
      if (error != NO_ERROR || pgptr == NULL)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      if ((log_offset + t_length) <= CIRP_LOGAREA_SIZE (buf_mgr))
	{
	  copy_length = t_length;
	}
      else
	{
	  copy_length = CIRP_LOGAREA_SIZE (buf_mgr) - log_offset;
	}

      memcpy (area + area_offset, (char *) (pgptr)->area + log_offset,
	      copy_length);
      t_length -= copy_length;
      area_offset += copy_length;
      log_offset += copy_length;
    }
  assert (error == NO_ERROR && pgptr != NULL);

  if (pgptr != org_pgptr)
    {
      cirp_logpb_release (buf_mgr, pgptr->hdr.logical_pageid);
    }

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "Invalid error code");
    }

  if (pgptr != NULL && pgptr != org_pgptr)
    {
      cirp_logpb_release (buf_mgr, pgptr->hdr.logical_pageid);
    }

  return error;
}

/*
 * cirp_make_repl_item_from_log ()
 *    return: repl_item
 *
 *    repl_item(out):
 *    log_pgptr(in):
 *    log_type(in):
 *    tranid(in):
 *    lsa(in):
 */
int
rp_make_repl_data_item_from_log (CIRP_BUF_MGR * buf_mgr,
				 CIRP_REPL_ITEM * repl_item,
				 LOG_PAGE * org_pgptr, const LOG_LSA * lsa)
{
  int error = NO_ERROR;
  struct log_replication *repl_log;
  LOG_PAGE *pgptr;
  PGLENGTH offset;
  LOG_PAGEID pageid;
  int length;			/* type change PGLENGTH -> int */
  char *ptr;
  RP_DATA_ITEM *data;
  char *area = NULL;

  assert (buf_mgr != NULL && repl_item != NULL
	  && org_pgptr != NULL && lsa != NULL);
  assert (repl_item->item_type == RP_ITEM_TYPE_DATA);

  pgptr = org_pgptr;
  pageid = lsa->pageid;
  offset = lsa->offset + DB_SIZEOF (LOG_RECORD_HEADER);
  length = DB_SIZEOF (struct log_replication);

  error = rp_log_read_align (buf_mgr, &pgptr, &pageid, &offset, org_pgptr);
  if (error != NO_ERROR || pgptr == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = rp_log_read_advance_when_doesnt_fit (buf_mgr, &pgptr, &pageid,
					       &offset, length, org_pgptr);
  if (error != NO_ERROR || pgptr == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  repl_log = (struct log_replication *) ((char *) pgptr->area + offset);
  offset += length;
  length = repl_log->length;

  error = rp_log_read_align (buf_mgr, &pgptr, &pageid, &offset, org_pgptr);
  if (error != NO_ERROR || pgptr == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  area = (char *) malloc (length);
  if (area == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, length);
      GOTO_EXIT_ON_ERROR;
    }

  error = cirp_log_copy_fromlog (buf_mgr, NULL, area, length, pageid,
				 offset, pgptr);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  data = &repl_item->info.data;

  LSA_COPY (&data->target_lsa, &repl_log->lsa);
  data->rcv_index = repl_log->rcvindex;

  ptr = or_unpack_int (area, &data->groupid);
  ptr = or_unpack_string (ptr, &data->class_name);
  assert (ptr != NULL);
  ptr = or_unpack_db_idxkey (ptr, &data->key);
  assert (ptr != NULL);

  if (area)
    {
      free_and_init (area);
    }

  if (pgptr != NULL && pgptr != org_pgptr)
    {
      cirp_logpb_release (buf_mgr, pgptr->hdr.logical_pageid);
    }

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);
      REPL_SET_GENERIC_ERROR (error, "Invalid error code");
    }

  if (area != NULL)
    {
      free_and_init (area);
    }

  if (pgptr != NULL && pgptr != org_pgptr)
    {
      cirp_logpb_release (buf_mgr, pgptr->hdr.logical_pageid);
    }

  return error;
}

/*
 * cirp_make_repl_schema_item_from_log ()
 *    return: repl_item
 *
 *    repl_item(out):
 *    log_pgptr(in):
 *    log_type(in):
 *    tranid(in):
 *    lsa(in):
 */
int
rp_make_repl_schema_item_from_log (CIRP_BUF_MGR * buf_mgr,
				   CIRP_REPL_ITEM * repl_item,
				   LOG_PAGE * org_pgptr, const LOG_LSA * lsa)
{
  int error = NO_ERROR;
  struct log_replication *repl_log;
  LOG_PAGE *pgptr;
  PGLENGTH offset;
  LOG_PAGEID pageid;
  int length;			/* type change PGLENGTH -> int */
  char *ptr;
  RP_DDL_ITEM *ddl;
  const char *class_name;
  int tmp;
  char *area = NULL;

  assert (buf_mgr != NULL && repl_item != NULL
	  && org_pgptr != NULL && lsa != NULL);
  assert (repl_item->item_type == RP_ITEM_TYPE_DDL);

  pgptr = org_pgptr;
  pageid = lsa->pageid;
  offset = DB_SIZEOF (LOG_RECORD_HEADER) + lsa->offset;
  length = DB_SIZEOF (struct log_replication);

  error = rp_log_read_align (buf_mgr, &pgptr, &pageid, &offset, org_pgptr);
  if (error != NO_ERROR || pgptr == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = rp_log_read_advance_when_doesnt_fit (buf_mgr, &pgptr, &pageid,
					       &offset, length, org_pgptr);
  if (error != NO_ERROR || pgptr == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  repl_log = (struct log_replication *) ((char *) pgptr->area + offset);
  offset += length;
  length = repl_log->length;

  error = rp_log_read_align (buf_mgr, &pgptr, &pageid, &offset, org_pgptr);
  if (error != NO_ERROR || pgptr == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  area = (char *) malloc (length);
  if (area == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, length);
      GOTO_EXIT_ON_ERROR;
    }

  error = cirp_log_copy_fromlog (buf_mgr, NULL, area, length, pageid,
				 offset, pgptr);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  ddl = &repl_item->info.ddl;
  assert (LSA_EQ (lsa, &ddl->lsa));

  ptr = or_unpack_int (area, &tmp);
  ddl->stmt_type = tmp;
  ptr = or_unpack_int (ptr, &ddl->ddl_type);
  ptr = or_unpack_string_nocopy (ptr, &class_name);
  ptr = or_unpack_string (ptr, &ddl->query);
  ptr = or_unpack_string (ptr, &ddl->db_user);

  if (area)
    {
      free_and_init (area);
    }

  if (pgptr != NULL && pgptr != org_pgptr)
    {
      cirp_logpb_release (buf_mgr, pgptr->hdr.logical_pageid);
    }

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);
      REPL_SET_GENERIC_ERROR (error, "Invalid error code");
    }

  if (area != NULL)
    {
      free_and_init (area);
    }

  if (pgptr != NULL && pgptr != org_pgptr)
    {
      cirp_logpb_release (buf_mgr, pgptr->hdr.logical_pageid);
    }

  return error;
}

/*
 * cirp_log_get_gid_bitmap_update ()
 *    return: error code
 *
 *    gbu(out):
 *    pgptr(in):
 *    lsa(in):
 */
int
cirp_log_get_gid_bitmap_update (CIRP_BUF_MGR * buf_mgr,
				struct log_gid_bitmap_update *gbu,
				LOG_PAGE * org_pgptr, const LOG_LSA * lsa)
{
  int error = NO_ERROR;
  LOG_PAGEID pageid;
  PGLENGTH offset;
  int length;
  LOG_PAGE *pgptr = NULL;

  if (gbu == NULL || org_pgptr == NULL)
    {
      assert (false);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "Invalid arguments");
      return error;
    }

  pageid = lsa->pageid;
  offset = lsa->offset + DB_SIZEOF (LOG_RECORD_HEADER);
  pgptr = org_pgptr;

  length = DB_SIZEOF (struct log_gid_bitmap_update);
  error = rp_log_read_advance_when_doesnt_fit (buf_mgr, &pgptr, &pageid,
					       &offset, length, org_pgptr);
  if (error != NO_ERROR || pgptr == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (pgptr != NULL)
    {
      *gbu =
	*((struct log_gid_bitmap_update *) ((char *) pgptr->area + offset));
      er_log_debug (ARG_FILE_LINE, "gid_bitmap_update: %d, %d, %d, %d",
		    gbu->migrator_id, gbu->group_id, gbu->target,
		    gbu->on_off);

      if (pgptr != org_pgptr)
	{
	  cirp_logpb_release (buf_mgr, pgptr->hdr.logical_pageid);
	}
    }

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  assert (error != NO_ERROR);

  if (pgptr != NULL && pgptr != org_pgptr)
    {
      cirp_logpb_release (buf_mgr, pgptr->hdr.logical_pageid);
    }

  return error;
}
