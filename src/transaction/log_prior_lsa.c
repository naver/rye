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
 * log_page_buffer.c -
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <time.h>
#include <limits.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdarg.h>

#include <sys/param.h>

#include <assert.h>

#include "porting.h"
#include "connection_defs.h"
#include "thread.h"
#include "log_impl.h"
#include "log_manager.h"
#include "log_comm.h"
#include "repl_log_writer_sr.h"
#include "lock_manager.h"
#include "boot_sr.h"
#if !defined(SERVER_MODE)
#include "boot_cl.h"
#include "transaction_cl.h"
#else /* !SERVER_MODE */
#include "connection_defs.h"
#include "connection_sr.h"
#endif
#include "page_buffer.h"
#include "file_io.h"
#include "disk_manager.h"
#include "error_manager.h"
#include "xserver_interface.h"
#include "perf_monitor.h"
#include "storage_common.h"
#include "system_parameter.h"
#include "memory_alloc.h"
#include "memory_hash.h"
#include "release_string.h"
#include "message_catalog.h"
#include "environment_variable.h"
#include "util_func.h"
#include "errno.h"
#include "tcp.h"
#include "log_compress.h"
#include "event_log.h"

#if !defined(SERVER_MODE)
#define pthread_mutex_init(a, b)
#define pthread_mutex_destroy(a)
#define pthread_mutex_lock(a)	0
#define pthread_mutex_unlock(a)
#undef  COND_INIT
#define COND_INIT(a)
#undef  COND_BROADCAST
#define COND_BROADCAST(a)
#undef  COND_DESTROY
#define COND_DESTROY(a)
#endif /* !SERVER_MODE */

#define LOG_PRIOR_LSA_LIST_MAX_SIZE() \
  ((INT64) log_Pb.num_buffers * (INT64) LOG_PAGESIZE)	/* 1 is guessing factor */

#define LOG_PRIOR_LSA_LAST_APPEND_OFFSET()  LOGAREA_SIZE

#define LOG_PRIOR_LSA_APPEND_ALIGN()                                       \
  do {                                                                             \
    log_Gl.prior_info.prior_lsa.offset = DB_ALIGN(log_Gl.prior_info.prior_lsa.offset, DOUBLE_ALIGNMENT); \
    if (log_Gl.prior_info.prior_lsa.offset >= (int)LOGAREA_SIZE)                    \
      log_Gl.prior_info.prior_lsa.pageid++, log_Gl.prior_info.prior_lsa.offset = 0;            \
  } while(0)

#define LOG_PRIOR_LSA_APPEND_ADVANCE_WHEN_DOESNOT_FIT(length)  \
  do {                                                                   \
    if (log_Gl.prior_info.prior_lsa.offset + (int)(length) >= (int)LOGAREA_SIZE)    \
      log_Gl.prior_info.prior_lsa.pageid++, log_Gl.prior_info.prior_lsa.offset = 0;            \
  } while(0)

#define LOG_PRIOR_LSA_APPEND_ADD_ALIGN(add)                    \
  do {                                                                   \
    log_Gl.prior_info.prior_lsa.offset += (add);                                    \
    LOG_PRIOR_LSA_APPEND_ALIGN();                              \
  } while(0)

static LOG_ZIP *logpb_get_zip_undo (THREAD_ENTRY * thread_p);
static LOG_ZIP *logpb_get_zip_redo (THREAD_ENTRY * thread_p);
static char *logpb_get_data_ptr (THREAD_ENTRY * thread_p);
static bool logpb_realloc_data_ptr (THREAD_ENTRY * thread_p, int length);

static int prior_lsa_copy_undo_data_to_node (LOG_PRIOR_NODE * node,
					     int length, const char *data);
static int prior_lsa_copy_redo_data_to_node (LOG_PRIOR_NODE * node,
					     int length, const char *data);
static int prior_lsa_copy_undo_crumbs_to_node (LOG_PRIOR_NODE * node,
					       int num_crumbs,
					       const LOG_CRUMB * crumbs);
static int prior_lsa_copy_redo_crumbs_to_node (LOG_PRIOR_NODE * node,
					       int num_crumbs,
					       const LOG_CRUMB * crumbs);
static void prior_lsa_start_append (THREAD_ENTRY * thread_p,
				    LOG_PRIOR_NODE * node, LOG_TDES * tdes);
static void prior_lsa_end_append (THREAD_ENTRY * thread_p,
				  LOG_PRIOR_NODE * node);
static void prior_lsa_append_data (int length);
static int prior_lsa_gen_undoredo_record (THREAD_ENTRY * thread_p,
					  LOG_PRIOR_NODE * node,
					  LOG_RCVINDEX rcvindex,
					  LOG_DATA_ADDR * addr,
					  int undo_length,
					  const char *undo_data,
					  int redo_length,
					  const char *redo_data);
static int prior_lsa_gen_undo_record (THREAD_ENTRY * thread_p,
				      LOG_PRIOR_NODE * node,
				      LOG_RCVINDEX rcvindex,
				      LOG_DATA_ADDR * addr, int length,
				      const char *data);
static int prior_lsa_gen_redo_record (THREAD_ENTRY * thread_p,
				      LOG_PRIOR_NODE * node,
				      LOG_RCVINDEX rcvindex,
				      LOG_DATA_ADDR * addr, int length,
				      const char *data);
static int prior_lsa_gen_postpone_record (THREAD_ENTRY * thread_p,
					  LOG_PRIOR_NODE * node,
					  LOG_RCVINDEX rcvindex,
					  LOG_DATA_ADDR * addr, int length,
					  const char *data);
static int prior_lsa_gen_dbout_redo_record (THREAD_ENTRY * thread_p,
					    LOG_PRIOR_NODE * node,
					    LOG_RCVINDEX rcvindex, int length,
					    const char *data);
static int prior_lsa_gen_record (THREAD_ENTRY * thread_p,
				 LOG_PRIOR_NODE * node, LOG_RECTYPE rec_type,
				 int length, const char *data);
static int prior_lsa_gen_undoredo_record_from_crumbs (THREAD_ENTRY * thread_p,
						      LOG_PRIOR_NODE * node,
						      LOG_RCVINDEX rcvindex,
						      LOG_DATA_ADDR * addr,
						      int num_ucrumbs,
						      const LOG_CRUMB *
						      ucrumbs,
						      int num_rcrumbs,
						      const LOG_CRUMB *
						      rcrumbs);
static int prior_lsa_gen_undo_record_from_crumbs (THREAD_ENTRY * thread_p,
						  LOG_PRIOR_NODE * node,
						  LOG_RCVINDEX rcvindex,
						  LOG_DATA_ADDR * addr,
						  int num_crumbs,
						  const LOG_CRUMB * crumbs);
static int prior_lsa_gen_redo_record_from_crumbs (THREAD_ENTRY * thread_p,
						  LOG_PRIOR_NODE * node,
						  LOG_RCVINDEX rcvindex,
						  LOG_DATA_ADDR * addr,
						  int num_crumbs,
						  const LOG_CRUMB * crumbs);
static LOG_LSA prior_lsa_next_record_internal (THREAD_ENTRY * thread_p,
					       LOG_PRIOR_NODE * node,
					       LOG_TDES * tdes,
					       int with_lock);
static int prior_lsa_get_max_record_length (THREAD_ENTRY * thread_p,
					    LOG_PRIOR_NODE * node);

#if defined (ENABLE_UNUSED_FUNCTION)
static void logpb_append_archives_delete_pend_to_log_info (int first,
							   int last);
#endif

static void logpb_dump_log_header (FILE * outfp);
static void logpb_dump_parameter (FILE * outfp);
static void logpb_dump_runtime (FILE * outfp);

/*
 * prior_lsa_copy_undo_data_to_node -
 *
 * return: error code or NO_ERROR
 *
 *   node(in/out):
 *   length(in):
 *   data(in):
 */
static int
prior_lsa_copy_undo_data_to_node (LOG_PRIOR_NODE * node,
				  int length, const char *data)
{
  if (length <= 0 || data == NULL)
    {
      return NO_ERROR;
    }

  node->udata = (char *) malloc (length);
  if (node->udata == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, length);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  memcpy (node->udata, data, length);

  node->ulength = length;

  return NO_ERROR;
}

/*
 * prior_lsa_copy_redo_data_to_node -
 *
 * return: error code or NO_ERROR
 *
 *   node(in/out):
 *   length(in):
 *   data(in):
 */
static int
prior_lsa_copy_redo_data_to_node (LOG_PRIOR_NODE * node,
				  int length, const char *data)
{
  if (length <= 0 || data == NULL)
    {
      return NO_ERROR;
    }

  node->rdata = (char *) malloc (length);
  if (node->rdata == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, length);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  memcpy (node->rdata, data, length);

  node->rlength = length;

  return NO_ERROR;
}

/*
 * prior_lsa_copy_undo_crumbs_to_node -
 *
 * return: error code or NO_ERROR
 *
 *   node(in/out):
 *   num_crumbs(in):
 *   crumbs(in):
 */
static int
prior_lsa_copy_undo_crumbs_to_node (LOG_PRIOR_NODE * node,
				    int num_crumbs, const LOG_CRUMB * crumbs)
{
  int i, length;
  char *ptr;

  assert ((num_crumbs == 0 && crumbs == NULL)
	  || (num_crumbs != 0 && crumbs != NULL));

  for (i = 0, length = 0; i < num_crumbs; i++)
    {
      length += crumbs[i].length;
    }

  assert (node->udata == NULL);
  if (length > 0)
    {
      node->udata = (char *) malloc (length);
      if (node->udata == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, length);
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      ptr = node->udata;
      for (i = 0; i < num_crumbs; i++)
	{
	  memcpy (ptr, crumbs[i].data, crumbs[i].length);
	  ptr += crumbs[i].length;
	}
    }

  node->ulength = length;

  return NO_ERROR;
}

/*
 * prior_lsa_copy_redo_crumbs_to_node -
 *
 * return: error code or NO_ERROR
 *
 *   node(in/out):
 *   num_crumbs(in):
 *   crumbs(in):
 */
static int
prior_lsa_copy_redo_crumbs_to_node (LOG_PRIOR_NODE * node,
				    int num_crumbs, const LOG_CRUMB * crumbs)
{
  int i, length;
  char *ptr;

  assert ((num_crumbs == 0 && crumbs == NULL)
	  || (num_crumbs != 0 && crumbs != NULL));
  for (i = 0, length = 0; i < num_crumbs; i++)
    {
      length += crumbs[i].length;
    }

  assert (node->rdata == NULL);
  if (length > 0)
    {
      node->rdata = (char *) malloc (length);
      if (node->rdata == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, length);
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      ptr = node->rdata;
      for (i = 0; i < num_crumbs; i++)
	{
	  memcpy (ptr, crumbs[i].data, crumbs[i].length);
	  ptr += crumbs[i].length;
	}
    }

  node->rlength = length;

  return NO_ERROR;
}

/*
 * prior_lsa_gen_undoredo_record -
 *
 * return: error code or NO_ERROR
 *
 *   node(in/out):
 *   rcvindex(in):
 *   addr(in):
 *   undo_length(in):
 *   undo_data(in):
 *   redo_length(in):
 *   redo_data(in):
 */
static int
prior_lsa_gen_undoredo_record (THREAD_ENTRY * thread_p,
			       LOG_PRIOR_NODE * node,
			       LOG_RCVINDEX rcvindex,
			       LOG_DATA_ADDR * addr, int undo_length,
			       const char *undo_data, int redo_length,
			       const char *redo_data)
{
  struct log_undoredo *undoredo;
  VPID *vpid;
  int error = NO_ERROR;
  bool is_diff, is_undo_zip, is_redo_zip;
  LOG_ZIP *zip_undo, *zip_redo;
  char *data_ptr = NULL;

  zip_undo = logpb_get_zip_undo (thread_p);
  zip_redo = logpb_get_zip_redo (thread_p);

  is_diff = false;
  is_undo_zip = false;
  is_redo_zip = false;

  /* log compress */
  if (log_Pb.log_zip_support && zip_undo && zip_redo)
    {				/* disable_log_compress = 0 in rye-auto.conf */
      if ((undo_length > 0) && (redo_length > 0)
	  && logpb_realloc_data_ptr (thread_p, redo_length))
	{
	  data_ptr = logpb_get_data_ptr (thread_p);
	}

      if (data_ptr)
	{
	  (void) memcpy (data_ptr, redo_data, redo_length);
	  (void) log_diff (undo_length, undo_data, redo_length, data_ptr);

	  is_undo_zip = log_zip (zip_undo, undo_length, undo_data);
	  is_redo_zip = log_zip (zip_redo, redo_length, data_ptr);

	  if (is_redo_zip)
	    {
	      is_diff = true;	/* log rec type : LOG_DIFF_UNDOREDO_DATA */
	    }
	}
      else
	{
	  if (undo_length > 0)
	    {
	      is_undo_zip = log_zip (zip_undo, undo_length, undo_data);
	    }
	  if (redo_length > 0)
	    {
	      is_redo_zip = log_zip (zip_redo, redo_length, redo_data);
	    }
	}
    }

  if (is_diff)
    {
      node->log_header.type = LOG_DIFF_UNDOREDO_DATA;
    }
  else
    {
      node->log_header.type = LOG_UNDOREDO_DATA;
    }

  /* log data copy */
  node->data_header_length = sizeof (struct log_undoredo);
  node->data_header = (char *) malloc (node->data_header_length);
  if (node->data_header == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      node->data_header_length);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  undoredo = (struct log_undoredo *) node->data_header;

  undoredo->data.rcvindex = rcvindex;
  if (addr->pgptr != NULL)
    {
      vpid = pgbuf_get_vpid_ptr (addr->pgptr);
      undoredo->data.pageid = vpid->pageid;
      undoredo->data.volid = vpid->volid;
    }
  else
    {
      undoredo->data.pageid = NULL_PAGEID;
      undoredo->data.volid = NULL_VOLID;
    }
  undoredo->data.offset = addr->offset;
  undoredo->data.gid = addr->gid;

  if (is_undo_zip)
    {
      undoredo->ulength = MAKE_ZIP_LEN (zip_undo->data_length);
      error = prior_lsa_copy_undo_data_to_node (node,
						zip_undo->data_length,
						(char *) zip_undo->log_data);
    }
  else
    {
      undoredo->ulength = undo_length;
      error = prior_lsa_copy_undo_data_to_node (node, undo_length, undo_data);
    }
  if (error != NO_ERROR)
    {
      return error;
    }

  if (is_redo_zip)
    {
      undoredo->rlength = MAKE_ZIP_LEN (zip_redo->data_length);
      error = prior_lsa_copy_redo_data_to_node (node,
						zip_redo->data_length,
						(char *) zip_redo->log_data);
    }
  else
    {
      undoredo->rlength = redo_length;
      error = prior_lsa_copy_redo_data_to_node (node, redo_length, redo_data);
    }

  return error;
}

/*
 * prior_lsa_gen_undoredo_record_from_crumbs -
 *
 * return: error code or NO_ERROR
 *
 *   node(in/out):
 *   rcvindex(in):
 *   addr(in):
 *   num_ucrumbs(in):
 *   ucrumbs(in):
 *   num_rcrumbs(in):
 *   rcrumbs(in):
 */
static int
prior_lsa_gen_undoredo_record_from_crumbs (THREAD_ENTRY * thread_p,
					   LOG_PRIOR_NODE * node,
					   LOG_RCVINDEX rcvindex,
					   LOG_DATA_ADDR * addr,
					   int num_ucrumbs,
					   const LOG_CRUMB * ucrumbs,
					   int num_rcrumbs,
					   const LOG_CRUMB * rcrumbs)
{
  struct log_undoredo *undoredo;
  VPID *vpid;
  int error_code = NO_ERROR;

  int i;
  int ulength, rlength;
  char *data_ptr = NULL, *tmp_ptr;
  char *undo_data = NULL, *redo_data = NULL;
  LOG_ZIP *zip_undo, *zip_redo;

  bool is_undo_zip, is_redo_zip, is_diff;

  is_undo_zip = false;
  is_redo_zip = false;
  is_diff = false;

  zip_undo = logpb_get_zip_undo (thread_p);
  zip_redo = logpb_get_zip_redo (thread_p);

  ulength = 0;
  for (i = 0; i < num_ucrumbs; i++)
    {
      ulength += ucrumbs[i].length;
    }

  rlength = 0;
  for (i = 0; i < num_rcrumbs; i++)
    {
      rlength += rcrumbs[i].length;
    }

  if (log_Pb.log_zip_support && zip_undo && zip_redo)
    {
      if (logpb_realloc_data_ptr (thread_p, ulength + rlength))
	{
	  data_ptr = logpb_get_data_ptr (thread_p);
	}

      if (data_ptr)
	{
	  if (ulength > 0)
	    {
	      undo_data = data_ptr;
	      tmp_ptr = undo_data;

	      for (i = 0; i < num_ucrumbs; i++)
		{
		  memcpy (tmp_ptr, ucrumbs[i].data, ucrumbs[i].length);
		  tmp_ptr += ucrumbs[i].length;
		}
	    }

	  if (rlength > 0)
	    {
	      redo_data = data_ptr + ulength;
	      tmp_ptr = redo_data;

	      for (i = 0; i < num_rcrumbs; i++)
		{
		  (void) memcpy (tmp_ptr, rcrumbs[i].data, rcrumbs[i].length);
		  tmp_ptr += rcrumbs[i].length;
		}
	    }

	  if (ulength > 0 && rlength > 0)
	    {
	      (void) log_diff (ulength, undo_data, rlength, redo_data);

	      is_undo_zip = log_zip (zip_undo, ulength, undo_data);
	      is_redo_zip = log_zip (zip_redo, rlength, redo_data);

	      if (is_redo_zip)
		{
		  is_diff = true;
		}
	    }
	  else
	    {
	      if (ulength > 0)
		{
		  is_undo_zip = log_zip (zip_undo, ulength, undo_data);
		}
	      if (rlength > 0)
		{
		  is_redo_zip = log_zip (zip_redo, rlength, redo_data);
		}
	    }
	}
    }

  if (is_diff)
    {
      node->log_header.type = LOG_DIFF_UNDOREDO_DATA;
    }
  else
    {
      node->log_header.type = LOG_UNDOREDO_DATA;
    }

  node->data_header = (char *) malloc (sizeof (struct log_undoredo));
  if (node->data_header == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      sizeof (struct log_undoredo));
      error_code = ER_OUT_OF_VIRTUAL_MEMORY;
      goto error;
    }
  node->data_header_length = sizeof (struct log_undoredo);

  undoredo = (struct log_undoredo *) node->data_header;

  undoredo->data.rcvindex = rcvindex;
  if (addr->pgptr != NULL)
    {
      vpid = pgbuf_get_vpid_ptr (addr->pgptr);
      undoredo->data.pageid = vpid->pageid;
      undoredo->data.volid = vpid->volid;
    }
  else
    {
      undoredo->data.pageid = NULL_PAGEID;
      undoredo->data.volid = NULL_VOLID;
    }
  undoredo->data.offset = addr->offset;
  undoredo->data.gid = addr->gid;
  undoredo->ulength = ulength;
  undoredo->rlength = rlength;

  if (is_undo_zip)
    {
      undoredo->ulength = MAKE_ZIP_LEN (zip_undo->data_length);
      error_code = prior_lsa_copy_undo_data_to_node (node,
						     zip_undo->data_length,
						     (char *) zip_undo->
						     log_data);
    }
  else
    {
      error_code = prior_lsa_copy_undo_crumbs_to_node (node,
						       num_ucrumbs, ucrumbs);
    }
  if (is_redo_zip)
    {
      undoredo->rlength = MAKE_ZIP_LEN (zip_redo->data_length);
      error_code = prior_lsa_copy_redo_data_to_node (node,
						     zip_redo->data_length,
						     (char *) zip_redo->
						     log_data);
    }
  else
    {
      error_code = prior_lsa_copy_redo_crumbs_to_node (node,
						       num_rcrumbs, rcrumbs);
    }
  if (error_code != NO_ERROR)
    {
      goto error;
    }

  return error_code;

error:
  if (node->data_header != NULL)
    {
      free_and_init (node->data_header);
    }
  if (node->udata != NULL)
    {
      free_and_init (node->udata);
    }
  if (node->rdata != NULL)
    {
      free_and_init (node->rdata);
    }

  return error_code;
}

/*
 * prior_lsa_gen_undo_record -
 *
 * return: error code or NO_ERROR
 *
 *   node(in/out):
 *   rcvindex(in):
 *   addr(in):
 *   length(in):
 *   data(in):
 */
static int
prior_lsa_gen_undo_record (THREAD_ENTRY * thread_p,
			   LOG_PRIOR_NODE * node,
			   LOG_RCVINDEX rcvindex,
			   LOG_DATA_ADDR * addr, int length, const char *data)
{
  struct log_undo *undo;
  VPID *vpid;
  int error = NO_ERROR;
  bool is_zipped = false;
  LOG_ZIP *zip_undo;

  /* Log compress Process */
  zip_undo = logpb_get_zip_undo (thread_p);

  if (log_Pb.log_zip_support && zip_undo && (length > 0))
    {
      is_zipped = log_zip (zip_undo, length, data);
    }

  node->data_header_length = sizeof (struct log_undo);
  node->data_header = (char *) malloc (node->data_header_length);
  if (node->data_header == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, node->data_header_length);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  undo = (struct log_undo *) node->data_header;

  undo->data.rcvindex = rcvindex;
  if (addr->pgptr != NULL)
    {
      vpid = pgbuf_get_vpid_ptr (addr->pgptr);
      undo->data.pageid = vpid->pageid;
      undo->data.volid = vpid->volid;
    }
  else
    {
      undo->data.pageid = NULL_PAGEID;
      undo->data.volid = NULL_VOLID;
    }
  undo->data.offset = addr->offset;
  undo->data.gid = NULL_GROUPID;

  if (is_zipped)
    {
      undo->length = MAKE_ZIP_LEN (zip_undo->data_length);
      error = prior_lsa_copy_undo_data_to_node (node, zip_undo->data_length,
						(char *) zip_undo->log_data);
    }
  else
    {
      undo->length = length;
      error = prior_lsa_copy_undo_data_to_node (node, length, data);
    }

  return error;
}

/*
 * prior_lsa_gen_undo_record_from_crumbs -
 *
 * return: error code or NO_ERROR
 *
 *   node(in/out):
 *   rcvindex(in):
 *   addr(in):
 *   num_crumbs(in):
 *   crumbs(in):
 */
static int
prior_lsa_gen_undo_record_from_crumbs (THREAD_ENTRY * thread_p,
				       LOG_PRIOR_NODE * node,
				       LOG_RCVINDEX rcvindex,
				       LOG_DATA_ADDR * addr,
				       int num_crumbs,
				       const LOG_CRUMB * crumbs)
{
  struct log_undo *undo;
  VPID *vpid;
  int error_code = NO_ERROR;
  int i = 0;
  char *tmp_ptr;
  char *undo_data = NULL;
  bool is_zipped = false;
  int total_length = 0;
  LOG_ZIP *zip_undo;
  char *data_ptr = NULL;

  zip_undo = logpb_get_zip_undo (thread_p);

  for (i = 0; i < num_crumbs; i++)
    {
      total_length += crumbs[i].length;
    }

  if (log_Pb.log_zip_support && zip_undo)
    {
      if (total_length > 0 && logpb_realloc_data_ptr (thread_p, total_length))
	{
	  data_ptr = logpb_get_data_ptr (thread_p);
	}

      if (data_ptr)
	{
	  undo_data = data_ptr;
	  tmp_ptr = undo_data;
	  for (i = 0; i < num_crumbs; i++)
	    {
	      memcpy (tmp_ptr, crumbs[i].data, crumbs[i].length);
	      tmp_ptr += crumbs[i].length;
	    }

	  is_zipped = log_zip (zip_undo, total_length, undo_data);
	}
    }

  node->data_header = (char *) malloc (sizeof (struct log_undo));
  if (node->data_header == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, sizeof (struct log_undo));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }
  node->data_header_length = sizeof (struct log_undo);

  undo = (struct log_undo *) node->data_header;

  undo->data.rcvindex = rcvindex;
  if (addr->pgptr != NULL)
    {
      vpid = pgbuf_get_vpid_ptr (addr->pgptr);
      undo->data.pageid = vpid->pageid;
      undo->data.volid = vpid->volid;
    }
  else
    {
      undo->data.pageid = NULL_PAGEID;
      undo->data.volid = NULL_VOLID;
    }
  undo->data.offset = addr->offset;
  undo->data.gid = NULL_GROUPID;

  if (is_zipped)
    {
      undo->length = MAKE_ZIP_LEN (zip_undo->data_length);

      prior_lsa_copy_undo_data_to_node (node, zip_undo->data_length,
					(char *) zip_undo->log_data);
    }
  else
    {
      undo->length = total_length;

      error_code = prior_lsa_copy_undo_crumbs_to_node (node, num_crumbs,
						       crumbs);
    }

  return error_code;
}

/*
 * prior_lsa_gen_redo_record -
 *
 * return: error code or NO_ERROR
 *
 *   node(in/out):
 *   rcvindex(in):
 *   addr(in):
 *   length(in):
 *   data(in):
 */
static int
prior_lsa_gen_redo_record (THREAD_ENTRY * thread_p,
			   LOG_PRIOR_NODE * node, LOG_RCVINDEX rcvindex,
			   LOG_DATA_ADDR * addr, int length, const char *data)
{
  struct log_redo *redo;
  VPID *vpid;
  int error = NO_ERROR;
  bool is_zipped = false;
  LOG_ZIP *zip_redo;

  zip_redo = logpb_get_zip_redo (thread_p);

  if (log_Pb.log_zip_support && zip_redo && (length > 0))
    {
      is_zipped = log_zip (zip_redo, length, data);
    }

  node->data_header_length = sizeof (struct log_redo);
  node->data_header = (char *) malloc (node->data_header_length);
  if (node->data_header == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, node->data_header_length);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }
  redo = (struct log_redo *) node->data_header;

  redo->data.rcvindex = rcvindex;
  if (addr->pgptr != NULL)
    {
      vpid = pgbuf_get_vpid_ptr (addr->pgptr);
      redo->data.pageid = vpid->pageid;
      redo->data.volid = vpid->volid;
    }
  else
    {
      redo->data.pageid = NULL_PAGEID;
      redo->data.volid = NULL_VOLID;
    }
  redo->data.offset = addr->offset;
  redo->data.gid = addr->gid;

  if (is_zipped)
    {
      redo->length = MAKE_ZIP_LEN (zip_redo->data_length);
      error = prior_lsa_copy_redo_data_to_node (node, zip_redo->data_length,
						(char *) zip_redo->log_data);
    }
  else
    {
      redo->length = length;
      error = prior_lsa_copy_redo_data_to_node (node, redo->length, data);
    }

  return error;
}

/*
 * prior_lsa_gen_postpone_record -
 *
 * return: error code or NO_ERROR
 *
 *   node(in/out):
 *   rcvindex(in):
 *   addr(in):
 *   length(in):
 *   data(in):
 */
static int
prior_lsa_gen_postpone_record (UNUSED_ARG THREAD_ENTRY * thread_p,
			       LOG_PRIOR_NODE * node, LOG_RCVINDEX rcvindex,
			       LOG_DATA_ADDR * addr, int length,
			       const char *data)
{
  struct log_redo *redo;
  VPID *vpid;
  int error_code = NO_ERROR;

  node->data_header_length = sizeof (struct log_redo);
  node->data_header = (char *) malloc (node->data_header_length);
  if (node->data_header == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, node->data_header_length);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }
  redo = (struct log_redo *) node->data_header;

  redo->data.rcvindex = rcvindex;
  if (addr->pgptr != NULL)
    {
      vpid = pgbuf_get_vpid_ptr (addr->pgptr);
      redo->data.pageid = vpid->pageid;
      redo->data.volid = vpid->volid;
    }
  else
    {
      redo->data.pageid = NULL_PAGEID;
      redo->data.volid = NULL_VOLID;
    }
  redo->data.offset = addr->offset;
  redo->data.gid = NULL_GROUPID;

  redo->length = length;
  error_code = prior_lsa_copy_redo_data_to_node (node, redo->length, data);

  return error_code;
}

/*
 * prior_lsa_gen_dbout_redo_record -
 *
 * return: error code or NO_ERROR
 *
 *   node(in/out):
 *   rcvindex(in):
 *   length(in):
 *   data(in):
 */
static int
prior_lsa_gen_dbout_redo_record (UNUSED_ARG THREAD_ENTRY * thread_p,
				 LOG_PRIOR_NODE * node, LOG_RCVINDEX rcvindex,
				 int length, const char *data)
{
  struct log_dbout_redo *dbout_redo;
  int error_code = NO_ERROR;

  node->data_header_length = sizeof (struct log_dbout_redo);
  node->data_header = (char *) malloc (node->data_header_length);
  if (node->data_header == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, node->data_header_length);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }
  dbout_redo = (struct log_dbout_redo *) node->data_header;

  dbout_redo->rcvindex = rcvindex;
  dbout_redo->length = length;

  error_code =
    prior_lsa_copy_redo_data_to_node (node, dbout_redo->length, data);

  return error_code;
}

/*
 * prior_lsa_gen_end_chkpt_record -
 *
 * return: error code or NO_ERROR
 *
 *   node(in/out):
 *   tran_length(in):
 *   tran_data(in):
 *   topop_length(in):
 *   topop_data(in):
 */
static int
prior_lsa_gen_end_chkpt_record (UNUSED_ARG THREAD_ENTRY * thread_p,
				LOG_PRIOR_NODE * node, int tran_length,
				const char *tran_data, int topop_length,
				const char *topop_data)
{
  int error_code = NO_ERROR;

  node->data_header_length = sizeof (struct log_chkpt);
  node->data_header = (char *) malloc (node->data_header_length);
  if (node->data_header == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, node->data_header_length);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  if (tran_length > 0)
    {
      error_code = prior_lsa_copy_undo_data_to_node (node, tran_length,
						     tran_data);
    }
  if (topop_length > 0)
    {
      error_code = prior_lsa_copy_redo_data_to_node (node, topop_length,
						     topop_data);
    }

  return error_code;
}

/*
 * prior_lsa_gen_record -
 *
 * return: error code or NO_ERROR
 *
 *   node(in/out):
 *   rec_type(in):
 *   length(in):
 *   data(in):
 */
static int
prior_lsa_gen_record (UNUSED_ARG THREAD_ENTRY * thread_p,
		      LOG_PRIOR_NODE * node, LOG_RECTYPE rec_type, int length,
		      const char *data)
{
  int error_code = NO_ERROR;

  node->data_header_length = 0;
  switch (rec_type)
    {
    case LOG_DUMMY_HEAD_POSTPONE:
#if 0
      /*
       * This isn't generated no more
       * (It was changed in logpb_archive_active_log)
       */
    case LOG_DUMMY_FILLPAGE_FORARCHIVE:
#endif
    case LOG_DUMMY_CRASH_RECOVERY:
    case LOG_DUMMY_OVF_RECORD:
    case LOG_DUMMY_RECORD:
    case LOG_START_CHKPT:
      assert (length == 0 && data == NULL);
      break;

    case LOG_RUN_POSTPONE:
      node->data_header_length = sizeof (struct log_run_postpone);
      break;

    case LOG_COMPENSATE:
      node->data_header_length = sizeof (struct log_compensate);
      break;

    case LOG_LCOMPENSATE:
      assert (length == 0 && data == NULL);
      node->data_header_length = sizeof (struct log_logical_compensate);
      break;

    case LOG_DUMMY_HA_SERVER_STATE:
      assert (length == 0 && data == NULL);
      node->data_header_length = sizeof (struct log_ha_server_state);
      break;

    case LOG_SAVEPOINT:
      node->data_header_length = sizeof (struct log_savept);
      break;

    case LOG_COMMIT_WITH_POSTPONE:
      node->data_header_length = sizeof (struct log_start_postpone);
      break;

    case LOG_COMMIT_TOPOPE_WITH_POSTPONE:
      node->data_header_length = sizeof (struct log_topope_start_postpone);
      break;

    case LOG_COMMIT:
    case LOG_ABORT:
      assert (length == 0 && data == NULL);
      node->data_header_length = sizeof (struct log_donetime);
      break;

    case LOG_COMMIT_TOPOPE:
    case LOG_ABORT_TOPOPE:
      assert (length == 0 && data == NULL);
      node->data_header_length = sizeof (struct log_topop_result);
      break;

    case LOG_REPLICATION_DATA:
    case LOG_REPLICATION_SCHEMA:
      node->data_header_length = sizeof (struct log_replication);
      break;

    case LOG_END_CHKPT:
      node->data_header_length = sizeof (struct log_chkpt);
      break;

    case LOG_DUMMY_UPDATE_GID_BITMAP:
      assert (length == 0 && data == NULL);
      node->data_header_length = sizeof (struct log_gid_bitmap_update);
      break;

    default:
      break;
    }

  if (node->data_header_length > 0)
    {
      node->data_header = (char *) malloc (node->data_header_length);
      if (node->data_header == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OUT_OF_VIRTUAL_MEMORY, 1, node->data_header_length);
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
    }

  if (length > 0)
    {
      error_code = prior_lsa_copy_undo_data_to_node (node, length, data);
    }

  return error_code;
}

/*
 * prior_lsa_gen_redo_record_from_crumbs -
 *
 * return: error code or NO_ERROR
 *
 *   node(in/out):
 *   rcvindex(in):
 *   addr(in):
 *   num_crumbs(in):
 *   crumbs(in):
 */
static int
prior_lsa_gen_redo_record_from_crumbs (THREAD_ENTRY * thread_p,
				       LOG_PRIOR_NODE * node,
				       LOG_RCVINDEX rcvindex,
				       LOG_DATA_ADDR * addr,
				       int num_crumbs,
				       const LOG_CRUMB * crumbs)
{
  struct log_redo *redo;
  VPID *vpid;
  int error = NO_ERROR;
  int i = 0;
  char *tmp_ptr;
  char *redo_data = NULL;
  bool is_zipped = false;
  int total_length = 0;
  LOG_ZIP *zip_redo;
  char *data_ptr = NULL;

  zip_redo = logpb_get_zip_redo (thread_p);

  for (i = 0; i < num_crumbs; i++)
    {
      total_length += crumbs[i].length;
    }

  if (log_Pb.log_zip_support && zip_redo)
    {
      if (total_length > 0 && logpb_realloc_data_ptr (thread_p, total_length))
	{
	  data_ptr = logpb_get_data_ptr (thread_p);
	}

      if (data_ptr)
	{
	  redo_data = data_ptr;
	  tmp_ptr = redo_data;
	  for (i = 0; i < num_crumbs; i++)
	    {
	      memcpy (tmp_ptr, crumbs[i].data, crumbs[i].length);
	      tmp_ptr += crumbs[i].length;
	    }
	  is_zipped = log_zip (zip_redo, total_length, redo_data);
	}
    }

  node->data_header_length = sizeof (struct log_redo);
  node->data_header = (char *) malloc (node->data_header_length);
  if (node->data_header == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, node->data_header_length);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  redo = (struct log_redo *) node->data_header;

  redo->data.rcvindex = rcvindex;
  if (addr->pgptr != NULL)
    {
      vpid = pgbuf_get_vpid_ptr (addr->pgptr);
      redo->data.pageid = vpid->pageid;
      redo->data.volid = vpid->volid;
    }
  else
    {
      redo->data.pageid = NULL_PAGEID;
      redo->data.volid = NULL_VOLID;
    }
  redo->data.offset = addr->offset;
  redo->data.gid = addr->gid;

  if (is_zipped)
    {
      redo->length = MAKE_ZIP_LEN (zip_redo->data_length);
      error = prior_lsa_copy_redo_data_to_node (node, zip_redo->data_length,
						(char *) zip_redo->log_data);
    }
  else
    {
      redo->length = total_length;
      error = prior_lsa_copy_redo_crumbs_to_node (node, num_crumbs, crumbs);
    }

  return error;
}

/*
 * prior_lsa_get_max_record_length
 *    return: record length or error code
 *
 *    node(in):
 */
static int
prior_lsa_get_max_record_length (UNUSED_ARG THREAD_ENTRY * thread_p,
				 LOG_PRIOR_NODE * node)
{
  int length = 0;

  if (node == NULL)
    {
      assert (false);
      return ER_FAILED;
    }
  assert (node->data_header_length >= 0
	  && node->ulength >= 0 && node->rlength >= 0);

  length = (sizeof (LOG_RECORD_HEADER) + DOUBLE_ALIGNMENT
	    + node->data_header_length + DOUBLE_ALIGNMENT
	    + node->ulength + DOUBLE_ALIGNMENT
	    + node->rlength + DOUBLE_ALIGNMENT);

  return length;
}

/*
 * prior_lsa_alloc_and_copy_data -
 *
 * return: new node
 *
 *   rec_type(in):
 *   rcvindex(in):
 *   addr(in):
 *   ulength(in):
 *   udata(in):
 *   rlength(in):
 *   rdata(in):
 */
LOG_PRIOR_NODE *
prior_lsa_alloc_and_copy_data (THREAD_ENTRY * thread_p,
			       LOG_RECTYPE rec_type,
			       LOG_RCVINDEX rcvindex,
			       LOG_DATA_ADDR * addr,
			       int ulength, const char *udata,
			       int rlength, const char *rdata)
{
  LOG_PRIOR_NODE *node;
  int error = NO_ERROR;
  int len = 0;

  node = (LOG_PRIOR_NODE *) malloc (sizeof (LOG_PRIOR_NODE));
  if (node == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, sizeof (LOG_PRIOR_NODE));
      return NULL;
    }

  node->log_header.type = rec_type;

  node->data_header = NULL;
  node->ulength = 0;
  node->udata = NULL;
  node->rlength = 0;
  node->rdata = NULL;
  node->next = NULL;

  switch (rec_type)
    {
    case LOG_UNDOREDO_DATA:
    case LOG_DIFF_UNDOREDO_DATA:
      error = prior_lsa_gen_undoredo_record (thread_p, node,
					     rcvindex,
					     addr, ulength,
					     udata, rlength, rdata);
      break;

    case LOG_UNDO_DATA:
      error = prior_lsa_gen_undo_record (thread_p, node, rcvindex,
					 addr, ulength, udata);
      break;

    case LOG_REDO_DATA:
    case LOG_DUMMY_OVF_RECORD_DEL:
      error = prior_lsa_gen_redo_record (thread_p, node,
					 rcvindex, addr, rlength, rdata);
      break;

    case LOG_DBEXTERN_REDO_DATA:
      error = prior_lsa_gen_dbout_redo_record (thread_p, node,
					       rcvindex, rlength, rdata);
      break;

    case LOG_POSTPONE:
      assert (ulength == 0 && udata == NULL);

      error = prior_lsa_gen_postpone_record (thread_p, node,
					     rcvindex, addr, rlength, rdata);
      break;

    case LOG_END_CHKPT:
      assert (addr == NULL);
      error = prior_lsa_gen_end_chkpt_record (thread_p, node,
					      ulength, udata, rlength, rdata);
      break;

    case LOG_RUN_POSTPONE:
    case LOG_COMPENSATE:
    case LOG_SAVEPOINT:

    case LOG_DUMMY_HEAD_POSTPONE:

#if 0
      /*
       * This isn't generated no more
       * (It was changed in logpb_archive_active_log)
       */
    case LOG_DUMMY_FILLPAGE_FORARCHIVE:
#endif

    case LOG_DUMMY_CRASH_RECOVERY:
    case LOG_DUMMY_HA_SERVER_STATE:
    case LOG_DUMMY_OVF_RECORD:
    case LOG_DUMMY_RECORD:
    case LOG_DUMMY_UPDATE_GID_BITMAP:

    case LOG_LCOMPENSATE:
    case LOG_COMMIT_WITH_POSTPONE:
    case LOG_COMMIT_TOPOPE_WITH_POSTPONE:
    case LOG_COMMIT:
    case LOG_ABORT:
    case LOG_COMMIT_TOPOPE:
    case LOG_ABORT_TOPOPE:
    case LOG_REPLICATION_DATA:
    case LOG_REPLICATION_SCHEMA:
    case LOG_START_CHKPT:
      assert (rlength == 0 && rdata == NULL);

      error = prior_lsa_gen_record (thread_p, node, rec_type, ulength, udata);
      break;

    default:
      break;
    }
  assert (node != NULL);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  len = prior_lsa_get_max_record_length (thread_p, node);
  if (len > LOG_MAX_RECORD_LENGTH)
    {
      char err_msg[ER_MSG_SIZE];

      assert (false);

      snprintf (err_msg, sizeof (err_msg),
		"log record size(%d) is greater than max log record size (%lld).",
		len, (long long) LOG_MAX_RECORD_LENGTH);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, err_msg);

      GOTO_EXIT_ON_ERROR;
    }

  return node;

exit_on_error:
  assert (error != NO_ERROR);
  assert (node != NULL);

  prior_lsa_free_node (thread_p, node);

  return NULL;
}

/*
 * prior_lsa_alloc_and_copy_crumbs -
 *
 * return: new node
 *
 *   rec_type(in):
 *   rcvindex(in):
 *   addr(in):
 *   num_ucrumbs(in):
 *   ucrumbs(in):
 *   num_rcrumbs(in):
 *   rcrumbs(in):
 */
LOG_PRIOR_NODE *
prior_lsa_alloc_and_copy_crumbs (THREAD_ENTRY * thread_p,
				 LOG_RECTYPE rec_type, LOG_RCVINDEX rcvindex,
				 LOG_DATA_ADDR * addr, const int num_ucrumbs,
				 const LOG_CRUMB * ucrumbs,
				 const int num_rcrumbs,
				 const LOG_CRUMB * rcrumbs)
{
  LOG_PRIOR_NODE *node;
  int error = NO_ERROR;
  int len = 0;

  node = (LOG_PRIOR_NODE *) malloc (sizeof (LOG_PRIOR_NODE));
  if (node == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, sizeof (LOG_PRIOR_NODE));
      return NULL;
    }

  node->log_header.type = rec_type;

  node->data_header_length = 0;
  node->data_header = NULL;
  node->ulength = 0;
  node->udata = NULL;
  node->rlength = 0;
  node->rdata = NULL;
  node->next = NULL;

  switch (rec_type)
    {
    case LOG_UNDOREDO_DATA:
    case LOG_DIFF_UNDOREDO_DATA:
      error = prior_lsa_gen_undoredo_record_from_crumbs (thread_p,
							 node,
							 rcvindex,
							 addr,
							 num_ucrumbs,
							 ucrumbs,
							 num_rcrumbs,
							 rcrumbs);
      break;

    case LOG_UNDO_DATA:
      error = prior_lsa_gen_undo_record_from_crumbs (thread_p, node,
						     rcvindex, addr,
						     num_ucrumbs, ucrumbs);
      break;

    case LOG_REDO_DATA:
      error = prior_lsa_gen_redo_record_from_crumbs (thread_p, node,
						     rcvindex, addr,
						     num_rcrumbs, rcrumbs);
      break;

    default:
      break;
    }

  assert (node != NULL);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  len = prior_lsa_get_max_record_length (thread_p, node);
  if (len > LOG_MAX_RECORD_LENGTH)
    {
      char err_msg[ER_MSG_SIZE];

      assert (false);

      snprintf (err_msg, sizeof (err_msg),
		"log record size(%d) is greater than max log record size (%lld).",
		len, (long long) LOG_MAX_RECORD_LENGTH);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, err_msg);

      GOTO_EXIT_ON_ERROR;
    }

  return node;

exit_on_error:
  assert (error != NO_ERROR);
  assert (node != NULL);

  if (node->data_header != NULL)
    {
      free_and_init (node->data_header);
    }
  if (node->udata != NULL)
    {
      free_and_init (node->udata);
    }
  if (node->rdata != NULL)
    {
      free_and_init (node->rdata);
    }
  free_and_init (node);

  return NULL;
}

/*
 * prior_lsa_free_node -
 *
 * return:
 *
 *   node(in/out):
 */
void
prior_lsa_free_node (UNUSED_ARG THREAD_ENTRY * thread_p,
		     LOG_PRIOR_NODE * node)
{
  if (node != NULL)
    {
      if (node->data_header != NULL)
	{
	  free_and_init (node->data_header);
	}
      if (node->udata != NULL)
	{
	  free_and_init (node->udata);
	}
      if (node->rdata != NULL)
	{
	  free_and_init (node->rdata);
	}

      free_and_init (node);
    }
}

/*
 * prior_lsa_next_record_internal -
 *
 * return: start lsa of log record
 *
 *   node(in/out):
 *   tdes(in/out):
 *   with_lock(in):
 */
static LOG_LSA
prior_lsa_next_record_internal (THREAD_ENTRY * thread_p,
				LOG_PRIOR_NODE * node, LOG_TDES * tdes,
				int with_lock)
{
  LOG_LSA start_lsa;
  int rv;

  assert (tdes->tran_index != NULL_TRAN_INDEX);
  assert (tdes->trid != NULL_TRANID);

  if (with_lock == LOG_PRIOR_LSA_WITHOUT_LOCK)
    {
      rv = pthread_mutex_lock (&log_Gl.prior_info.prior_lsa_mutex);
    }

  prior_lsa_start_append (thread_p, node, tdes);

  LSA_COPY (&start_lsa, &node->start_lsa);

  LOG_PRIOR_LSA_APPEND_ADVANCE_WHEN_DOESNOT_FIT (node->data_header_length);
  LOG_PRIOR_LSA_APPEND_ADD_ALIGN (node->data_header_length);

  if (node->ulength > 0)
    {
      prior_lsa_append_data (node->ulength);
    }

  if (node->rlength > 0)
    {
      prior_lsa_append_data (node->rlength);
    }

  /* END append */
  prior_lsa_end_append (thread_p, node);

  if (log_Gl.prior_info.prior_list_tail == NULL)
    {
      log_Gl.prior_info.prior_list_header = node;
      log_Gl.prior_info.prior_list_tail = node;
    }
  else
    {
      log_Gl.prior_info.prior_list_tail->next = node;
      log_Gl.prior_info.prior_list_tail = node;
    }

  log_Gl.prior_info.list_size += (sizeof (LOG_PRIOR_NODE) + node->data_header_length + node->ulength + node->rlength);	/* bytes */

  if (with_lock == LOG_PRIOR_LSA_WITHOUT_LOCK)
    {
      pthread_mutex_unlock (&log_Gl.prior_info.prior_lsa_mutex);

      if (log_Gl.prior_info.list_size >= LOG_PRIOR_LSA_LIST_MAX_SIZE ())
	{
	  mnt_stats_counter (thread_p, MNT_STATS_PRIOR_LSA_LIST_MAXED, 1);

#if defined(SERVER_MODE)
	  if (!log_is_in_crash_recovery ())
	    {
	      thread_wakeup_log_flush_thread ();

	      THREAD_SLEEP (1);	/* 1msec */
	    }
	  else
	    {
	      LOG_CS_ENTER (thread_p);
	      logpb_prior_lsa_append_all_list (thread_p);
	      LOG_CS_EXIT ();
	    }
#else
	  LOG_CS_ENTER (thread_p);
	  logpb_prior_lsa_append_all_list (thread_p);
	  LOG_CS_EXIT ();
#endif
	}
    }

#if defined(SERVER_MODE)
  if (logtb_get_current_tran_index (thread_p) == LOG_SYSTEM_TRAN_INDEX)
    {
      /*
       * Is this the first log record of transaction ?
       */
      TR_TABLE_LOCK (thread_p);
      if (LSA_ISNULL (&tdes->begin_lsa))
	{
	  LSA_COPY (&tdes->begin_lsa, &start_lsa);
	}
      TR_TABLE_UNLOCK (thread_p);
    }
  else
    {
      assert (!LSA_ISNULL (&tdes->begin_lsa));
    }

#else
  /*
   * Is this the first log record of transaction ?
   */
  TR_TABLE_LOCK (thread_p);
  if (LSA_ISNULL (&tdes->begin_lsa))
    {
      LSA_COPY (&tdes->begin_lsa, &start_lsa);
    }
  TR_TABLE_UNLOCK (thread_p);
#endif

  return start_lsa;
}

/*
 * prior_lsa_get_current_lsa -
 *
 *    return: current prior_lsa
 */
int
prior_lsa_get_current_lsa (UNUSED_ARG THREAD_ENTRY * thread_p,
			   LOG_LSA * current_lsa)
{
  int rv;

  rv = pthread_mutex_lock (&log_Gl.prior_info.prior_lsa_mutex);

  LSA_COPY (current_lsa, &log_Gl.prior_info.prior_lsa);

  pthread_mutex_unlock (&log_Gl.prior_info.prior_lsa_mutex);

  return NO_ERROR;
}

/*
 * prior_lsa_next_record -
 *
 *   return:
 *
 *   node(in):
 *   tdes(in/out):
 */
LOG_LSA
prior_lsa_next_record (THREAD_ENTRY * thread_p, LOG_PRIOR_NODE * node,
		       LOG_TDES * tdes)
{
  return prior_lsa_next_record_internal (thread_p, node, tdes,
					 LOG_PRIOR_LSA_WITHOUT_LOCK);
}

/*
 * prior_lsa_next_record_with_lock -
 *
 *   return:
 *
 *   node(in):
 *   tdes(in/out):
 */
LOG_LSA
prior_lsa_next_record_with_lock (THREAD_ENTRY * thread_p,
				 LOG_PRIOR_NODE * node, LOG_TDES * tdes)
{
  return prior_lsa_next_record_internal (thread_p, node, tdes,
					 LOG_PRIOR_LSA_WITH_LOCK);
}

/*
 * prior_lsa_start_append -
 *
 *   node(in/out):
 *   tdes(in):
 */
static void
prior_lsa_start_append (UNUSED_ARG THREAD_ENTRY * thread_p,
			LOG_PRIOR_NODE * node, LOG_TDES * tdes)
{
  /* Does the new log record fit in this page ? */
  LOG_PRIOR_LSA_APPEND_ADVANCE_WHEN_DOESNOT_FIT (sizeof (LOG_RECORD_HEADER));

  node->log_header.trid = tdes->trid;

  /*
   * Link the record with the previous transaction record for quick undos.
   * Link the record backward for backward traversal of the log.
   */
  LSA_COPY (&node->start_lsa, &log_Gl.prior_info.prior_lsa);

  LSA_COPY (&node->log_header.prev_tranlsa, &tdes->last_lsa);
  LSA_COPY (&node->log_header.back_lsa, &log_Gl.prior_info.prev_lsa);
  LSA_SET_NULL (&node->log_header.forw_lsa);

  /*
   * Remember the address of new append record
   */
  LSA_COPY (&tdes->last_lsa, &log_Gl.prior_info.prior_lsa);
  LSA_COPY (&tdes->undo_nxlsa, &log_Gl.prior_info.prior_lsa);

  LSA_COPY (&log_Gl.prior_info.prev_lsa, &log_Gl.prior_info.prior_lsa);
  /*
   * Set the page dirty, increase and align the append offset
   */
  LOG_PRIOR_LSA_APPEND_ADD_ALIGN (sizeof (LOG_RECORD_HEADER));

#if 0
  /*
   * LOG_DUMMY_FILLPAGE_FORARCHIVE isn't generated no more
   * (It was changed in logpb_archive_active_log)
   * so, this check is not required.
   */

  if (node->log_header.type == LOG_DUMMY_FILLPAGE_FORARCHIVE)
    {
      /*
       * Get to start of next page if not already advanced ... Note
       * this record type is only safe during backups.
       */
      if (node->start_lsa.pageid == log_Gl.prior_info.prev_lsa.pageid)
	{
	  LOG_PRIOR_LSA_APPEND_ADVANCE_WHEN_DOESNOT_FIT (LOG_PAGESIZE);
	}
    }
#endif

}

static void
prior_lsa_append_data (int length)
{
  int copy_length;		/* Amount of contiguos data that can be copied        */
  int current_offset;
  int last_offset;

  if (length != 0)
    {
      /*
       * Align if needed,
       * don't set it dirty since this function has not updated
       */
      LOG_PRIOR_LSA_APPEND_ALIGN ();

      current_offset = log_Gl.prior_info.prior_lsa.offset;
      last_offset = LOG_PRIOR_LSA_LAST_APPEND_OFFSET ();

      /* Does data fit completely in current page ? */
      if ((current_offset + length) >= last_offset)
	{
	  while (length > 0)
	    {
	      if (current_offset >= last_offset)
		{
		  /*
		   * Get next page and set the current one dirty
		   */
		  log_Gl.prior_info.prior_lsa.pageid++;
		  log_Gl.prior_info.prior_lsa.offset = 0;

		  current_offset = 0;
		  last_offset = LOG_PRIOR_LSA_LAST_APPEND_OFFSET ();
		}
	      /* Find the amount of contiguous data that can be copied */
	      if (current_offset + length >= last_offset)
		{
		  copy_length = CAST_BUFLEN (last_offset - current_offset);
		}
	      else
		{
		  copy_length = length;
		}

	      current_offset += copy_length;
	      length -= copy_length;
	      log_Gl.prior_info.prior_lsa.offset += copy_length;
	    }
	}
      else
	{
	  log_Gl.prior_info.prior_lsa.offset += length;
	}
      /*
       * Align the data for future appends.
       * Indicate that modifications were done
       */
      LOG_PRIOR_LSA_APPEND_ALIGN ();
    }
}

/*
 * prior_lsa_end_append -
 *
 * return:
 *
 *   node(in/out):
 */
static void
prior_lsa_end_append (UNUSED_ARG THREAD_ENTRY * thread_p,
		      LOG_PRIOR_NODE * node)
{
  LOG_PRIOR_LSA_APPEND_ALIGN ();
  LOG_PRIOR_LSA_APPEND_ADVANCE_WHEN_DOESNOT_FIT (sizeof (LOG_RECORD_HEADER));

  LSA_COPY (&node->log_header.forw_lsa, &log_Gl.prior_info.prior_lsa);
}

/*
 * logpb_realloc_data_ptr -
 *
 * return:
 *
 *   data_length(in):
 *   length(in):
 *
 * NOTE:
 */
static bool
logpb_realloc_data_ptr (UNUSED_ARG THREAD_ENTRY * thread_p, int length)
{
  char *data_ptr;
  int alloc_len;
#if defined (SERVER_MODE)
  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  if (thread_p == NULL)
    {
      return false;
    }

  if (thread_p->log_data_length < length)
    {
      alloc_len = ((int) CEIL_PTVDIV (length, IO_PAGESIZE)) * IO_PAGESIZE;

      data_ptr = (char *) realloc (thread_p->log_data_ptr, alloc_len);
      if (data_ptr == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OUT_OF_VIRTUAL_MEMORY, 1, alloc_len);
	  if (thread_p->log_data_ptr)
	    {
	      free_and_init (thread_p->log_data_ptr);
	    }
	  thread_p->log_data_length = 0;
	  return false;
	}
      else
	{
	  thread_p->log_data_ptr = data_ptr;
	  thread_p->log_data_length = alloc_len;
	}
    }
  return true;
#else
  if (log_Pb.log_data_length < length)
    {
      alloc_len = ((int) CEIL_PTVDIV (length, IO_PAGESIZE)) * IO_PAGESIZE;

      data_ptr = (char *) realloc (log_Pb.log_data_ptr, alloc_len);
      if (data_ptr == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, alloc_len);
	  if (log_Pb.log_data_ptr)
	    {
	      free_and_init (log_Pb.log_data_ptr);
	    }
	  log_Pb.log_data_length = 0;
	  return false;
	}
      else
	{
	  log_Pb.log_data_ptr = data_ptr;
	  log_Pb.log_data_length = alloc_len;
	}
    }

  return true;
#endif
}

static LOG_ZIP *
logpb_get_zip_undo (UNUSED_ARG THREAD_ENTRY * thread_p)
{
#if defined (SERVER_MODE)
  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  if (thread_p == NULL)
    {
      return NULL;
    }
  else
    {
      if (thread_p->log_zip_undo == NULL)
	{
	  thread_p->log_zip_undo = log_zip_alloc (IO_PAGESIZE, true);
	}
      return thread_p->log_zip_undo;
    }
#else
  return log_Pb.log_zip_undo;
#endif
}

static LOG_ZIP *
logpb_get_zip_redo (UNUSED_ARG THREAD_ENTRY * thread_p)
{
#if defined (SERVER_MODE)
  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  if (thread_p == NULL)
    {
      return NULL;
    }
  else
    {
      if (thread_p->log_zip_redo == NULL)
	{
	  thread_p->log_zip_redo = log_zip_alloc (IO_PAGESIZE, true);
	}
      return thread_p->log_zip_redo;
    }
#else
  return log_Pb.log_zip_redo;
#endif
}

/*
 *
 */
static char *
logpb_get_data_ptr (UNUSED_ARG THREAD_ENTRY * thread_p)
{
#if defined (SERVER_MODE)
  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  if (thread_p == NULL)
    {
      return NULL;
    }
  else
    {
      if (thread_p->log_data_ptr == NULL)
	{
	  thread_p->log_data_length = IO_PAGESIZE * 2;
	  thread_p->log_data_ptr =
	    (char *) malloc (thread_p->log_data_length);

	  if (thread_p->log_data_ptr == NULL)
	    {
	      thread_p->log_data_length = 0;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, thread_p->log_data_length);
	    }
	}
      return thread_p->log_data_ptr;
    }
#else
  return log_Pb.log_data_ptr;
#endif
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * logpb_append_archives_delete_pend_to_log_info -  Record pending delete
 *                                                  of one or more archive
 *
 * return: nothing
 *
 *   first(in): number of the first archive captured
 *   last(in): number of the last archive in the range
 *
 * NOTE: This routine makes an entry into the loginfo file that the
 *   given log archives have been "captured" by a backup and therefore cannot
 *   be deleted at this time (DELETE PENDING).
 */
static void
logpb_append_archives_delete_pend_to_log_info (int first, int last)
{
  const char *catmsg;
  char logarv_name[PATH_MAX];	/* Archive name */
  char logarv_name_first[PATH_MAX];	/* Archive name */
  int error_code;

  catmsg = msgcat_message (MSGCAT_CATALOG_RYE,
			   MSGCAT_SET_LOG,
			   MSGCAT_LOG_LOGINFO_ARCHIVES_NEEDED_FOR_RESTORE);
  if (catmsg == NULL)
    {
      catmsg = "DELETE POSTPONED: Archives %d %s to %d %s \n"
	"are no longer needed unless a restore from current backup occurs.\n";
    }

  fileio_make_log_archive_name (logarv_name, log_Archive_path,
				log_Prefix, last);

  if (first == last)
    {
      error_code = log_dump_log_info (log_Name_info, true, catmsg, first,
				      fileio_get_base_file_name (logarv_name),
				      last, logarv_name);
    }
  else
    {
      fileio_make_log_archive_name (logarv_name_first, log_Archive_path,
				    log_Prefix, first);
      error_code = log_dump_log_info (log_Name_info, true, catmsg, first,
				      fileio_get_base_file_name
				      (logarv_name_first), last, logarv_name);
    }
  if (error_code != NO_ERROR && error_code != ER_LOG_MOUNT_FAIL)
    {
      return;
    }
}
#endif

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * logpb_check_and_reset_temp_lsa -
 *
 * return:
 *
 *   volid(in):
 *
 * NOTE:
 */
int
logpb_check_and_reset_temp_lsa (THREAD_ENTRY * thread_p, VOLID volid)
{
  VPID vpid;
  PAGE_PTR pgptr;

  vpid.volid = volid;
  vpid.pageid = 0;
  pgptr = pgbuf_fix (thread_p, &vpid, OLD_PAGE, PGBUF_LATCH_WRITE,
		     PGBUF_UNCONDITIONAL_LATCH);
  if (pgptr == NULL)
    {
      return ER_FAILED;
    }

  if (xdisk_get_purpose (thread_p, volid) == DISK_PERMVOL_TEMP_PURPOSE)
    {
      pgbuf_reset_temp_lsa (pgptr);
      pgbuf_set_dirty (thread_p, pgptr, FREE);
    }
  else
    {
      pgbuf_unfix (thread_p, pgptr);
    }

  return NO_ERROR;
}
#endif

/*
 * logpb_dump_log_header - dump log header
 *
 * return: Nothing
 *
 *   outfp(in):  file descriptor
 *
 * NOTE:
 */
static void
logpb_dump_log_header (FILE * outfp)
{
  fprintf (outfp, "Log Header:\n");

  fprintf (outfp, "\tfirst log page id : %lld\n",
	   (long long int) log_Gl.hdr.fpageid);

  fprintf (outfp, "\tcurrent log append lsa : (%lld, %d)\n",
	   (long long int) log_Gl.hdr.append_lsa.pageid,
	   log_Gl.hdr.append_lsa.offset);

  fprintf (outfp, "\tlast log append lsa : (%lld, %d)\n",
	   (long long int) log_Gl.append.prev_lsa.pageid,
	   log_Gl.append.prev_lsa.offset);

  fprintf (outfp,
	   "\tlowest lsa which hasn't been written to disk : (%lld, %d)\n",
	   (long long int) log_Gl.append.nxio_lsa.pageid,
	   log_Gl.append.nxio_lsa.offset);

  fprintf (outfp, "\tcheckpoint lsa : (%lld, %d)\n",
	   (long long int) log_Gl.hdr.chkpt_lsa.pageid,
	   log_Gl.hdr.chkpt_lsa.offset);

  fprintf (outfp, "\tnext archive page id : %lld\n",
	   (long long int) log_Gl.hdr.nxarv_pageid);

  fprintf (outfp, "\tnext archive physical page id : %lld\n",
	   (long long int) log_Gl.hdr.nxarv_phy_pageid);

  fprintf (outfp, "\tnext archive number : %d\n", log_Gl.hdr.nxarv_num);

  fprintf (outfp, "\tlast archive number needed for system crashes : %d\n",
	   log_Gl.hdr.last_arv_num_for_syscrashes);

  fprintf (outfp, "\tlast archive number deleted : %d\n",
	   log_Gl.hdr.last_deleted_arv_num);

  fprintf (outfp, "\tbackup level lsa : (%lld, %d)\n",
	   (long long int) log_Gl.hdr.bkup_level_lsa.pageid,
	   log_Gl.hdr.bkup_level_lsa.offset);
}

/*
 * logpb_dump_parameter - dump logging parameter
 *
 * return: Nothing
 *
 *   outfp(in): file descriptor
 *
 * NOTE:
 */
static void
logpb_dump_parameter (FILE * outfp)
{
  fprintf (outfp, "Log Parameters:\n");

  fprintf (outfp, "\tasync_log_flush_interval : %lld\n",
	   (long long int) prm_get_bigint_value (PRM_ID_LOG_ASYNC_LOG_FLUSH_INTERVAL));
}

/*
 * logpb_dump_runtime - dump runtime logging information
 *
 * return: Nothing
 *
 *   outfp(in): file descriptor
 *
 * NOTE:
 */
static void
logpb_dump_runtime (FILE * outfp)
{
  long temp = 1;

  fprintf (outfp, "Log Statistics:\n");

  fprintf (outfp, "\ttotal flush count = %ld\n",
	   log_Stat.flushall_append_pages_call_count);

  fprintf (outfp, "\tgroup commit flush count= %ld\n",
	   log_Stat.gc_flush_count);

  fprintf (outfp, "\tdirect flush count= %ld\n", log_Stat.direct_flush_count);

  fprintf (outfp, "\tgroup commit request count = %ld\n",
	   log_Stat.gc_commit_request_count);

  if (log_Stat.flushall_append_pages_call_count != 0)
    {
      temp = (log_Stat.flushall_append_pages_call_count
	      - log_Stat.direct_flush_count);
    }

  fprintf (outfp, "\tgroup commit grouping rate = %f\n",
	   (double) log_Stat.gc_commit_request_count / temp);

  temp = 1;
  if (log_Stat.gc_commit_request_count != 0)
    {
      temp = log_Stat.gc_commit_request_count;
    }

  fprintf (outfp, "\tavg group commit wait time = %f\n",
	   log_Stat.gc_total_wait_time / temp);

  fprintf (outfp, "\ttotal commit count = %ld\n", log_Stat.commit_count);

  fprintf (outfp, "\ttotal allocated log pages count = %ld\n",
	   log_Stat.total_append_page_count);

  fprintf (outfp, "\tlog buffer full count = %ld\n",
	   log_Stat.log_buffer_full_count);

  fprintf (outfp, "\tlog buffer flush count by replacement = %ld\n",
	   log_Stat.log_buffer_flush_count_by_replacement);

  fprintf (outfp, "\tlog buffer expand count = %ld\n",
	   log_Stat.log_buffer_expand_count);
}

/*
 * xlogpb_dump_stat - dump logging information
 *
 * return: Nothing
 *
 *   outfp(in): file descriptor
 *
 * NOTE:
 */
void
xlogpb_dump_stat (FILE * outfp)
{
  logpb_dump_parameter (outfp);
  logpb_dump_log_header (outfp);
  logpb_dump_runtime (outfp);
}

/*
 * logpb_background_archiving -
 *
 * return:
 *
 * NOTE: this function is called by log_initialize_internal only
 *       (in server startup time)
 */
int
logpb_background_archiving (THREAD_ENTRY * thread_p)
{
  char log_pgbuf[IO_MAX_PAGE_SIZE * LOGPB_IO_NPAGES + MAX_ALIGNMENT];
  char *aligned_log_pgbuf;
  LOG_PAGE *log_pgptr;
  LOG_PAGEID page_id, last_page_id;
  LOG_PHY_PAGEID phy_pageid;
  int num_pages = 0;
  int vdes;
  int error_code = NO_ERROR;
  BACKGROUND_ARCHIVING_INFO *bg_arv_info;

  aligned_log_pgbuf = PTR_ALIGN (log_pgbuf, MAX_ALIGNMENT);
  log_pgptr = (LOG_PAGE *) aligned_log_pgbuf;

  bg_arv_info = &log_Gl.bg_archive_info;
  vdes = bg_arv_info->vdes;
  if (vdes == NULL_VOLDES)
    {
      return NO_ERROR;
    }

  last_page_id = log_Gl.hdr.chkpt_lsa.pageid - 1;
  page_id = bg_arv_info->current_page_id;
  phy_pageid = (LOG_PHY_PAGEID) (page_id - bg_arv_info->start_page_id + 1);

  /* Now start dumping the current active pages to archive */
  for (; page_id <= last_page_id;
       page_id += num_pages, phy_pageid += num_pages)
    {
      num_pages = MIN (LOGPB_IO_NPAGES, (int) (last_page_id - page_id + 1));

      num_pages = logpb_read_page_from_active_log (thread_p, page_id,
						   num_pages, log_pgptr);
      if (num_pages <= 0)
	{
	  error_code = er_errid ();
	  goto error;
	}

      if (fileio_write_pages (thread_p, vdes, (char *) log_pgptr, phy_pageid,
			      num_pages, LOG_PAGESIZE) == NULL)
	{
	  error_code = ER_LOG_WRITE;
	  goto error;
	}

      bg_arv_info->current_page_id = page_id + num_pages;
    }

error:
  if (error_code == ER_LOG_WRITE || error_code == ER_LOG_READ)
    {
      fileio_dismount (thread_p, bg_arv_info->vdes);
      bg_arv_info->vdes = NULL_VOLDES;
      bg_arv_info->start_page_id = NULL_PAGEID;
      bg_arv_info->current_page_id = NULL_PAGEID;
      bg_arv_info->last_sync_pageid = NULL_PAGEID;

      er_log_debug (ARG_FILE_LINE,
		    "background archiving error, hdr->start_page_id = %d, "
		    "hdr->current_page_id = %d, error:%d\n",
		    bg_arv_info->start_page_id,
		    bg_arv_info->current_page_id, error_code);
    }

  er_log_debug (ARG_FILE_LINE,
		"logpb_background_archiving end, hdr->start_page_id = %d, "
		"hdr->current_page_id = %d\n",
		bg_arv_info->start_page_id, bg_arv_info->current_page_id);

  return error_code;
}

/*
 * logpb_need_wal -
 */
bool
logpb_need_wal (const LOG_LSA * lsa)
{
  LOG_LSA nxio_lsa;

  logpb_get_nxio_lsa (&nxio_lsa);

  if (LSA_LE (&nxio_lsa, lsa))
    {
      return true;
    }
  else
    {
      return false;
    }
}

#if 0				/* TODO - do not delete me */
/*
 * logpb_perm_status_to_string() - return the string alias of enum value
 *
 *   return: constant string
 *
 *   val(in): the enum value
 */
const char *
logpb_perm_status_to_string (enum LOG_PSTATUS val)
{
  switch (val)
    {
    case LOG_PSTAT_CLEAR:
      return "LOG_PSTAT_CLEAR";
    case LOG_PSTAT_BACKUP_INPROGRESS:
      return "LOG_PSTAT_BACKUP_INPROGRESS";
    case LOG_PSTAT_RESTORE_INPROGRESS:
      return "LOG_PSTAT_RESTORE_INPROGRESS";
    case LOG_PSTAT_HDRFLUSH_INPPROCESS:
      return "LOG_PSTAT_HDRFLUSH_INPPROCESS";
    }
  return "UNKNOWN_LOG_PSTATUS";
}
#endif
