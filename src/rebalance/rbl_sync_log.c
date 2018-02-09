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
 * rbl_sync_change.c -
 */

#ident "$Id$"

#include <stdlib.h>
#include <string.h>

#include "memory_alloc.h"
#include "storage_common.h"
#include "log_impl.h"
#include "log_compress.h"
#include "recovery.h"
#include "network_interface_cl.h"
#include "locator_cl.h"
#include "transform_cl.h"
#include "object_accessor.h"
#include "schema_manager.h"
#include "class_object.h"
#include "parser.h"
#include "object_print.h"
#include "memory_hash.h"
#include "rbl_error_log.h"
#include "rbl_sync_query.h"
#include "rbl_sync_log.h"

#define RBL_LOGAREA_SIZE       (LOG_PAGESIZE - SSIZEOF(LOG_HDRPAGE))
#define REC_TYPE_SIZE   ((int) sizeof (INT16))
#define CLASS_OID_SIZE  ((int) sizeof (OID))

typedef struct overflow_first_part OVERFLOW_FIRST_PART;
struct overflow_first_part
{
  VPID next_vpid;
  OID class_oid;
  int length;
  char data[1];			/* Really more than one */
};

typedef struct overflow_rest_part OVERFLOW_REST_PART;
struct overflow_rest_part
{
  VPID next_vpid;
  char data[1];			/* Really more than one */
};

typedef struct rbl_overflow_data RBL_OVERFLOW_DATA;
struct rbl_overflow_data
{
  OID class_oid;
  RECDES recdes;
  int copyed_length;
  TRANID tran_id;
  bool done_first_part;
};

static DB_OBJLIST *all_Tables = NULL;
static MHT_TABLE *ht_Tran_ovfl_rec = NULL;

static LOG_PAGE *rbl_next_log_page (RBL_SYNC_CONTEXT * ctx);
static int rbl_copy_data_from_log (RBL_SYNC_CONTEXT * ctx, char *area,
				   int length);

int
rbl_sync_log_init (RBL_SYNC_CONTEXT * ctx, int gid)
{
  int i, error;

  ctx->migrator_id = getpid ();
  ctx->gid = gid;

  ctx->num_log_pages = 0;
  ctx->max_log_pages = LOGWR_COPY_LOG_BUFFER_NPAGES;

  ctx->unzip_area =
    log_zip_alloc ((ctx->max_log_pages + 1) * LOG_PAGESIZE, false);
  if (ctx->unzip_area == NULL)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_OUT_OF_MEMORY,
		 (ctx->max_log_pages + 1) * LOG_PAGESIZE);
      ctx->logpg_area_size = 0;
      return RBL_OUT_OF_MEMORY;
    }

  /* area size = logGl.hdr page + max log pages */
  ctx->logpg_area_size = (ctx->max_log_pages + 1) * LOG_PAGESIZE;
  ctx->logpg_area = (char *) malloc (ctx->logpg_area_size);
  if (ctx->logpg_area == NULL)
    {
      if (ctx->unzip_area != NULL)
	{
	  log_zip_free (ctx->unzip_area);
	  ctx->unzip_area = NULL;
	}
      RBL_ERROR (ARG_FILE_LINE, RBL_OUT_OF_MEMORY, ctx->logpg_area_size);
      return RBL_OUT_OF_MEMORY;
    }

  ctx->log_pages =
    (LOG_PAGE **) malloc (ctx->max_log_pages * sizeof (LOG_PAGE *));
  if (ctx->log_pages == NULL)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_OUT_OF_MEMORY,
		 ctx->max_log_pages * sizeof (LOG_PAGE **));
      return RBL_OUT_OF_MEMORY;
    }

  for (i = 0; i < ctx->max_log_pages; i++)
    {
      ctx->log_pages[i] = NULL;
    }

  error = locator_get_eof_lsa (&ctx->final_lsa);
  if (error != NO_ERROR)
    {
      return error;
    }
  LSA_COPY (&ctx->server_lsa, &ctx->final_lsa);
  LSA_SET_NULL (&ctx->synced_lsa);

  ctx->undo_unzip = log_zip_alloc (IO_MAX_PAGE_SIZE, false);
  if (ctx->undo_unzip == NULL)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_OUT_OF_MEMORY, IO_MAX_PAGE_SIZE);
      return RBL_OUT_OF_MEMORY;
    }

  ctx->redo_unzip = log_zip_alloc (IO_MAX_PAGE_SIZE, false);
  if (ctx->redo_unzip == NULL)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_OUT_OF_MEMORY, IO_MAX_PAGE_SIZE);
      return RBL_OUT_OF_MEMORY;
    }

  all_Tables = sm_fetch_all_classes (S_LOCK);
  if (all_Tables == NULL)
    {
      return er_errid ();
    }

  ht_Tran_ovfl_rec =
    mht_create ("Tran Overflow Record", 1024, rbl_tranid_hash,
		rbl_compare_tranid_are_equal);
  if (ht_Tran_ovfl_rec == NULL)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_OUT_OF_MEMORY, 1024);
      return RBL_OUT_OF_MEMORY;
    }

  ctx->start_time = time (NULL);
  ctx->total_log_pages = 0;
  ctx->delay = 0;

  return NO_ERROR;
}

void
rbl_sync_log_final (RBL_SYNC_CONTEXT * ctx)
{
  if (ctx->unzip_area != NULL)
    {
      log_zip_free (ctx->unzip_area);
      ctx->unzip_area = NULL;
    }

  if (ctx->logpg_area != NULL)
    {
      free (ctx->logpg_area);
    }
  if (ctx->log_pages != NULL)
    {
      free (ctx->log_pages);
    }
  if (ctx->undo_unzip != NULL)
    {
      log_zip_free (ctx->undo_unzip);
    }
  if (ctx->redo_unzip != NULL)
    {
      log_zip_free (ctx->redo_unzip);
    }
  if (all_Tables != NULL)
    {
      db_objlist_free (all_Tables);
    }
  if (ht_Tran_ovfl_rec != NULL)
    {
      (void) mht_destroy (ht_Tran_ovfl_rec);
    }
}

/*
 * rbl_get_next_log_pages -
 *
 * return:
 * Note:
 */
static void
rbl_arrange_log_pages (RBL_SYNC_CONTEXT * ctx_ptr)
{
  LOG_PAGE *log_pgptr = NULL;
  struct log_header *hdr_ptr;
  char *p;
  int num_page = 0;
  LOG_PAGEID start_pageid;

  /* the first page has logGl.hdr */
  log_pgptr = (LOG_PAGE *) (ctx_ptr->logpg_area);
  hdr_ptr = (struct log_header *) (log_pgptr->area);
  LSA_COPY (&ctx_ptr->server_lsa, &hdr_ptr->eof_lsa);

  p = ctx_ptr->logpg_area + LOG_PAGESIZE;
  while (p < (ctx_ptr->logpg_area + ctx_ptr->logpg_fill_size))
    {
      log_pgptr = (LOG_PAGE *) p;
      ctx_ptr->log_pages[num_page++] = log_pgptr;
      p += LOG_PAGESIZE;
    }

  ctx_ptr->num_log_pages = num_page;
  RBL_ASSERT (ctx_ptr->num_log_pages <= ctx_ptr->max_log_pages);
  ctx_ptr->total_log_pages += num_page;

  start_pageid = ctx_ptr->last_recv_pageid;
  ctx_ptr->last_recv_pageid = log_pgptr->hdr.logical_pageid;
  ctx_ptr->delay = ctx_ptr->server_lsa.pageid - ctx_ptr->final_lsa.pageid;

  RBL_ASSERT (ctx_ptr->last_recv_pageid > 0);
  RBL_ASSERT (ctx_ptr->last_recv_pageid <= ctx_ptr->server_lsa.pageid);

  RBL_DEBUG (ARG_FILE_LINE, "Get Log Pages: start_pageid =%lld, "
	     "num_log_pages = %d, last_recv_pageid = %lld, "
	     "server_lsa = (%lld, %d)\n",
	     start_pageid, ctx_ptr->num_log_pages, ctx_ptr->last_recv_pageid,
	     ctx_ptr->server_lsa.pageid, ctx_ptr->server_lsa.offset);
}


static int
rbl_next_log_page_when_dosent_fit (RBL_SYNC_CONTEXT * ctx, int length)
{
  int error = NO_ERROR;

  if (ctx->cur_offset + length >= RBL_LOGAREA_SIZE)
    {
      ctx->cur_page = rbl_next_log_page (ctx);
      if (ctx->cur_page == NULL)
	{
	  error = RBL_LOG_PAGE_ERROR;
	}

      ctx->cur_offset = 0;
    }

  return error;
}

static int
rbl_log_read_align (RBL_SYNC_CONTEXT * ctx)
{
  int error = NO_ERROR;
  PGLENGTH offset;

  offset = DB_ALIGN (ctx->cur_offset, MAX_ALIGNMENT);

  while (offset >= RBL_LOGAREA_SIZE)
    {
      ctx->cur_page = rbl_next_log_page (ctx);
      if (ctx->cur_page == NULL)
	{
	  error = RBL_LOG_PAGE_ERROR;
	}

      offset -= RBL_LOGAREA_SIZE;
      offset = DB_ALIGN (offset, MAX_ALIGNMENT);
    }

  ctx->cur_offset = offset;
  return error;
}

static int
rbl_log_read_add_align (RBL_SYNC_CONTEXT * ctx, PGLENGTH add)
{
  ctx->cur_offset += add;
  return rbl_log_read_align (ctx);
}

static LOG_PAGE *
rbl_next_log_page (RBL_SYNC_CONTEXT * ctx)
{
  LOG_PAGE *pg_ptr;
  int error;

  if (ctx->cur_page_index >= ctx->num_log_pages)
    {
      ctx->last_recv_pageid++;
      RBL_ASSERT (ctx->last_recv_pageid > 0);

      error = rbl_get_log_pages (ctx);
      if (error != NO_ERROR)
	{
	  RBL_ERROR (ARG_FILE_LINE, RBL_LOG_PAGE_ERROR,
		     ctx->last_recv_pageid, error);
	  return NULL;
	}

      rbl_arrange_log_pages (ctx);
      ctx->cur_page_index = 0;
    }

  pg_ptr = ctx->log_pages[ctx->cur_page_index];
  ctx->cur_pageid = pg_ptr->hdr.logical_pageid;
  ctx->cur_page_index++;

  return pg_ptr;
}

static int
rbl_get_undoredo_diff (RBL_SYNC_CONTEXT * ctx, bool * is_undo_zip,
		       char **undo_data, int *undo_length)
{
  LOG_ZIP *undo_unzip_data = NULL;
  int error;

  undo_unzip_data = ctx->undo_unzip;
  if (ZIP_CHECK (*undo_length))
    {
      *is_undo_zip = true;
      *undo_length = GET_ZIP_LEN (*undo_length);
    }

  *undo_data = (char *) malloc (*undo_length);
  if (*undo_data == NULL)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_OUT_OF_MEMORY, *undo_length);
      return RBL_OUT_OF_MEMORY;
    }

  error = rbl_copy_data_from_log (ctx, *undo_data, *undo_length);
  if (error != NO_ERROR)
    {
      free_and_init (*undo_data);
      return error;
    }

  if (*is_undo_zip && *undo_length > 0)
    {
      if (!log_unzip (undo_unzip_data, *undo_length, *undo_data))
	{
	  free_and_init (*undo_data);

	  RBL_ERROR_MSG (ARG_FILE_LINE, "Failed to decompress log page\n");
	  return RBL_LOG_DECOMPRESS_FAIL;
	}
    }

  error = rbl_log_read_align (ctx);
  if (error != NO_ERROR)
    {
      free_and_init (*undo_data);
      return error;
    }

  return NO_ERROR;
}

/*
 * la_get_zipped_data () - get zipped data
 *   return: error code
 */
static int
rbl_get_zipped_data (RBL_SYNC_CONTEXT * ctx, char *undo_data, int undo_length,
		     bool is_diff, bool is_undo_zip, INT16 * rec_type,
		     OID * class_oid, char **data, int *length)
{
  int redo_length = 0;
  int offset = 0;
  int data_length = 0;

  LOG_ZIP *undo_unzip_data = NULL;
  LOG_ZIP *redo_unzip_data = NULL;

  undo_unzip_data = ctx->undo_unzip;
  redo_unzip_data = ctx->redo_unzip;

  if (is_diff)
    {
      if (is_undo_zip)
	{
	  undo_length = undo_unzip_data->data_length;
	  redo_length = redo_unzip_data->data_length;

	  (void) log_diff (undo_length,
			   undo_unzip_data->
			   log_data, redo_length, redo_unzip_data->log_data);
	}
      else
	{

	  redo_length = redo_unzip_data->data_length;
	  (void) log_diff (undo_length,
			   undo_data, redo_length, redo_unzip_data->log_data);
	}
    }
  else
    {
      redo_length = redo_unzip_data->data_length;
    }

  memcpy (rec_type, (ctx->redo_unzip)->log_data, REC_TYPE_SIZE);
  offset += REC_TYPE_SIZE;

  memcpy (class_oid, (ctx->redo_unzip)->log_data + offset, CLASS_OID_SIZE);
  offset += CLASS_OID_SIZE;

  RBL_ASSERT (class_oid->groupid == ctx->gid);
  data_length = redo_length - offset;

  *data = (char *) malloc (data_length);
  if (*data == NULL)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_OUT_OF_MEMORY, data_length);
      *length = 0;
      return RBL_OUT_OF_MEMORY;
    }

  memcpy (*data, (ctx->redo_unzip)->log_data + offset, data_length);
  *length = data_length;

  return NO_ERROR;
}

static int
rbl_copy_data_from_log (RBL_SYNC_CONTEXT * ctx, char *area, int length)
{
  int copy_length;
  int area_offset = 0;
  int error = NO_ERROR;

  while (length > 0)
    {
      error = rbl_next_log_page_when_dosent_fit (ctx, 0);
      if (error != NO_ERROR)
	{
	  break;
	}

      copy_length = ((ctx->cur_offset + length <= RBL_LOGAREA_SIZE) ?
		     length : RBL_LOGAREA_SIZE - ctx->cur_offset);
      memcpy (area + area_offset,
	      (char *) ctx->cur_page->area + ctx->cur_offset, copy_length);
      length -= copy_length;
      area_offset += copy_length;
      ctx->cur_offset += copy_length;
    }

  return error;
}

static int
rbl_get_redo_data (RBL_SYNC_CONTEXT * ctx, struct log_undoredo *undoredo,
		   bool is_diff, RECDES * recdes, OID * class_oid)
{
  int length;
  int error = NO_ERROR;
  bool is_undo_zip = false;
  int undo_length = 0;
  char *undo_data = NULL;
  char *data, *zip_data;
  INT16 rec_type;

  undo_length = undoredo->ulength;

  if (is_diff)
    {
      error = rbl_get_undoredo_diff (ctx, &is_undo_zip, &undo_data,
				     &undo_length);
    }
  else
    {
      error = rbl_log_read_add_align (ctx, GET_ZIP_LEN (undo_length));
    }

  if (error != NO_ERROR)
    {
      return error;
    }

  length = GET_ZIP_LEN (undoredo->rlength);
  if (ZIP_CHECK (undoredo->rlength))
    {
      zip_data = (char *) malloc (length);
      if (zip_data == NULL)
	{
	  if (undo_data != NULL)
	    {
	      free_and_init (undo_data);
	    }

	  RBL_ERROR (ARG_FILE_LINE, RBL_OUT_OF_MEMORY, length);
	  return RBL_OUT_OF_MEMORY;
	}

      /* Get Zip Data */
      error = rbl_copy_data_from_log (ctx, zip_data, length);
      if (error != NO_ERROR)
	{
	  if (undo_data != NULL)
	    {
	      free_and_init (undo_data);
	    }
	  free_and_init (zip_data);

	  return error;
	}

      if (!log_unzip (ctx->redo_unzip, length, zip_data))
	{
	  if (undo_data != NULL)
	    {
	      free_and_init (undo_data);
	    }
	  free_and_init (zip_data);
	  RBL_ERROR_MSG (ARG_FILE_LINE, "Failed to decompress log page\n");
	  return RBL_LOG_DECOMPRESS_FAIL;
	}

      error = rbl_get_zipped_data (ctx, undo_data, undo_length, is_diff,
				   is_undo_zip, &rec_type, class_oid,
				   &data, &length);
      free_and_init (zip_data);
    }
  else
    {
      error = rbl_copy_data_from_log (ctx, (char *) &rec_type, REC_TYPE_SIZE);
      if (error != NO_ERROR)
	{
	  if (undo_data != NULL)
	    {
	      free_and_init (undo_data);
	    }
	  return error;
	}

      error = rbl_copy_data_from_log (ctx, (char *) class_oid,
				      CLASS_OID_SIZE);
      if (error != NO_ERROR)
	{
	  if (undo_data != NULL)
	    {
	      free_and_init (undo_data);
	    }
	  return error;
	}

      RBL_ASSERT (class_oid->groupid == ctx->gid);

      length -= (REC_TYPE_SIZE + CLASS_OID_SIZE);
      data = (char *) malloc (length);
      if (data == NULL)
	{
	  if (undo_data != NULL)
	    {
	      free_and_init (undo_data);
	    }

	  RBL_ERROR (ARG_FILE_LINE, RBL_OUT_OF_MEMORY, length);
	  return RBL_OUT_OF_MEMORY;
	}

      error = rbl_copy_data_from_log (ctx, data, length);
      if (error != NO_ERROR)
	{
	  if (undo_data != NULL)
	    {
	      free_and_init (undo_data);
	    }
	  free_and_init (data);

	  return error;
	}
    }

  if (undo_data != NULL)
    {
      free_and_init (undo_data);
    }

  recdes->data = data;
  recdes->area_size = length;
  recdes->length = length;
  recdes->type = rec_type;

  return error;

}

static int
rbl_make_ovfl_data (UNUSED_ARG RBL_SYNC_CONTEXT * ctx, TRANID trid)
{
  RBL_OVERFLOW_DATA *ovfl_rec;

  ovfl_rec = (RBL_OVERFLOW_DATA *) malloc (sizeof (RBL_OVERFLOW_DATA));
  if (ovfl_rec == NULL)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_OUT_OF_MEMORY,
		 sizeof (RBL_OVERFLOW_DATA));
      return RBL_OUT_OF_MEMORY;
    }

  ovfl_rec->tran_id = trid;
  ovfl_rec->recdes.data = NULL;
  ovfl_rec->done_first_part = false;

  if (mht_put (ht_Tran_ovfl_rec, &ovfl_rec->tran_id, ovfl_rec) == NULL)
    {
      free_and_init (ovfl_rec);
      return ER_FAILED;
    }

  return NO_ERROR;
}

static int
rbl_free_ovfl_data (UNUSED_ARG const void *key, void *data,
		    UNUSED_ARG void *args)
{
  RBL_OVERFLOW_DATA *ovfl_rec;

  ovfl_rec = (RBL_OVERFLOW_DATA *) data;

  if (ovfl_rec->recdes.data != NULL)
    {
      free (ovfl_rec->recdes.data);
    }
  free (ovfl_rec);

  return NO_ERROR;
}

static int
rbl_get_ovfl_redo_data (RBL_SYNC_CONTEXT * ctx, struct log_redo *redo,
			TRANID trid, bool * is_completed)
{
  int length;
  char *raw_data;
  const char *p;
  int offset, area_len;
  RBL_OVERFLOW_DATA *ovfl_rec;
  const OVERFLOW_FIRST_PART *first;
  UNUSED_VAR const OVERFLOW_REST_PART *rest;
  int error;

  *is_completed = false;

  ovfl_rec = (RBL_OVERFLOW_DATA *) mht_get (ht_Tran_ovfl_rec, &trid);
  if (ovfl_rec == NULL)
    {
      /* case of not meet LOG_DUMMY_OVF_RECORD first
       * skip this record */
      return NO_ERROR;
    }

  length = GET_ZIP_LEN (redo->length);
  raw_data = (char *) malloc (length);
  if (raw_data == NULL)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_OUT_OF_MEMORY, length);
      return RBL_OUT_OF_MEMORY;
    }

  error = rbl_copy_data_from_log (ctx, raw_data, length);
  if (error != NO_ERROR)
    {
      free_and_init (raw_data);
      return error;
    }

  if (ZIP_CHECK (redo->length))
    {
      if (!log_unzip (ctx->redo_unzip, length, raw_data))
	{
	  RBL_ERROR_MSG (ARG_FILE_LINE, "Failed to decompress log page\n");
	  free_and_init (raw_data);

	  return RBL_LOG_DECOMPRESS_FAIL;
	}

      p = (const char *) ctx->redo_unzip->log_data;
      length = ctx->redo_unzip->data_length;
    }
  else
    {
      p = raw_data;
    }

  if (ovfl_rec->done_first_part == false)
    {
      first = (const OVERFLOW_FIRST_PART *) p;
      RBL_ASSERT (first->class_oid.groupid == ctx->gid);
      ovfl_rec->class_oid = first->class_oid;

      ovfl_rec->recdes.data = (char *) malloc (first->length);
      if (ovfl_rec->recdes.data == NULL)
	{
	  RBL_ERROR (ARG_FILE_LINE, RBL_OUT_OF_MEMORY, first->length);
	  free_and_init (raw_data);

	  return RBL_OUT_OF_MEMORY;
	}

      ovfl_rec->recdes.type = REC_BIGONE;
      ovfl_rec->recdes.area_size = first->length;
      ovfl_rec->recdes.length = first->length;
      ovfl_rec->copyed_length = 0;
      ovfl_rec->done_first_part = true;
      offset = offsetof (OVERFLOW_FIRST_PART, data);
    }
  else
    {
      offset = offsetof (OVERFLOW_REST_PART, data);
      rest = (const OVERFLOW_REST_PART *) p;
    }

  area_len = length - offset;
  RBL_ASSERT (ovfl_rec->recdes.data != NULL);
  memcpy (ovfl_rec->recdes.data + ovfl_rec->copyed_length,
	  p + offset, area_len);
  ovfl_rec->copyed_length += area_len;

  if (ovfl_rec->copyed_length == ovfl_rec->recdes.length)
    {
      *is_completed = true;
    }

  free_and_init (raw_data);

  return NO_ERROR;
}

static int
rbl_get_ovfl_redo_record (RBL_SYNC_CONTEXT * ctx, struct log_redo *redo,
			  RECDES * recdes, OID * class_oid)
{
  int length;
  char *raw_data;
  const char *p;
  int error;

  length = GET_ZIP_LEN (redo->length);
  raw_data = (char *) malloc (length);
  if (raw_data == NULL)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_OUT_OF_MEMORY, length);
      return RBL_OUT_OF_MEMORY;
    }

  error = rbl_copy_data_from_log (ctx, raw_data, length);
  if (error != NO_ERROR)
    {
      free_and_init (raw_data);
      return error;
    }

  if (ZIP_CHECK (redo->length))
    {
      if (!log_unzip (ctx->redo_unzip, length, raw_data))
	{
	  RBL_ERROR_MSG (ARG_FILE_LINE, "Failed to decompress log page\n");
	  free_and_init (raw_data);

	  return RBL_LOG_DECOMPRESS_FAIL;
	}

      p = (const char *) ctx->redo_unzip->log_data;
      length = ctx->redo_unzip->data_length;
    }
  else
    {
      p = raw_data;
    }

  memcpy (class_oid, p, CLASS_OID_SIZE);
  RBL_ASSERT (class_oid->groupid == ctx->gid);

  recdes->type = REC_BIGONE;
  recdes->area_size = recdes->length = length - CLASS_OID_SIZE;

  recdes->data = (char *) malloc (recdes->length);
  if (recdes->data == NULL)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_OUT_OF_MEMORY, recdes->length);
      free_and_init (raw_data);

      return RBL_OUT_OF_MEMORY;
    }

  memcpy (recdes->data, p + CLASS_OID_SIZE, recdes->length);

  free_and_init (raw_data);

  return NO_ERROR;
}

static int
rbl_get_undo_data (RBL_SYNC_CONTEXT * ctx, struct log_undoredo *undoredo,
		   UNUSED_ARG bool is_diff, RECDES * recdes, OID * class_oid)
{
  int length;
  int error = NO_ERROR;
  bool is_undo_zip = false;
  int undo_length = 0;
  char *undo_data = NULL;
  char *data, *p;
  INT16 rec_type;
  int offset = 0;

  undo_length = undoredo->ulength;

  error = rbl_get_undoredo_diff (ctx, &is_undo_zip, &undo_data, &undo_length);
  if (error != NO_ERROR)
    {
      return error;
    }

  if (is_undo_zip)
    {
      p = (char *) ctx->undo_unzip->log_data;
      length = (ctx->undo_unzip)->data_length;
    }
  else
    {
      p = undo_data;
      length = undo_length;
    }

  RBL_ASSERT (p != NULL);

  memcpy (&rec_type, p, REC_TYPE_SIZE);
  offset += REC_TYPE_SIZE;

  memcpy (class_oid, p + offset, CLASS_OID_SIZE);
  offset += CLASS_OID_SIZE;

  RBL_ASSERT (class_oid->groupid == ctx->gid);

  length -= offset;
  data = (char *) malloc (length);
  if (data == NULL)
    {
      if (undo_data != NULL)
	{
	  free_and_init (undo_data);
	}

      RBL_ERROR (ARG_FILE_LINE, RBL_OUT_OF_MEMORY, length);
      return RBL_OUT_OF_MEMORY;
    }

  memcpy (data, p + offset, length);

  if (undo_data != NULL)
    {
      free_and_init (undo_data);
    }

  recdes->data = data;
  recdes->area_size = length;
  recdes->length = length;
  recdes->type = rec_type;

  return error;
}

static int
rbl_get_undoredo_info (RBL_SYNC_CONTEXT * ctx, struct log_undoredo *undoredo)
{
  int length;
  int error = NO_ERROR;

  ctx->cur_offset = DB_SIZEOF (LOG_RECORD_HEADER) + ctx->final_lsa.offset;

  error = rbl_log_read_align (ctx);
  if (error != NO_ERROR)
    {
      return error;
    }

  length = DB_SIZEOF (struct log_undoredo);
  error = rbl_next_log_page_when_dosent_fit (ctx, length);

  if (error != NO_ERROR)
    {
      return error;
    }

  *undoredo =
    *((struct log_undoredo *) ((char *) ctx->cur_page->area +
			       ctx->cur_offset));

#if 0
  printf ("undo_length = %d\n", (*undoredo)->ulength);
  printf ("redo_length = %d\n", (*undoredo)->rlength);
  printf ("rcv_index = %d\n", (*undoredo)->data.rcvindex);
#endif

  error = rbl_log_read_add_align (ctx, length);
  if (error != NO_ERROR)
    {
      return error;
    }

  return error;
}

static int
rbl_get_redo_info (RBL_SYNC_CONTEXT * ctx, struct log_redo *redo)
{
  int length;
  int error = NO_ERROR;

  ctx->cur_offset = DB_SIZEOF (LOG_RECORD_HEADER) + ctx->final_lsa.offset;

  error = rbl_log_read_align (ctx);
  if (error != NO_ERROR)
    {
      return error;
    }

  length = DB_SIZEOF (struct log_redo);
  error = rbl_next_log_page_when_dosent_fit (ctx, length);

  if (error != NO_ERROR)
    {
      return error;
    }

  *redo =
    *((struct log_redo *) ((char *) ctx->cur_page->area + ctx->cur_offset));

#if 0
  printf ("redo_length = %d\n", (*redo)->length);
  printf ("rcv_index = %d\n", (*redo)->data.rcvindex);
#endif

  error = rbl_log_read_add_align (ctx, length);
  if (error != NO_ERROR)
    {
      return error;
    }

  return error;
}

static int
rbl_log_get_updated_gid (RBL_SYNC_CONTEXT * ctx,
			 struct log_gid_bitmap_update *gid_update)
{
  int length;
  int error = NO_ERROR;

  ctx->cur_offset = DB_SIZEOF (LOG_RECORD_HEADER) + ctx->final_lsa.offset;

  error = rbl_log_read_align (ctx);
  if (error != NO_ERROR)
    {
      return error;
    }

  length = DB_SIZEOF (struct log_gid_bitmap_update);
  error = rbl_next_log_page_when_dosent_fit (ctx, length);

  if (error != NO_ERROR)
    {
      return error;
    }

  *gid_update = *((struct log_gid_bitmap_update *)
		  ((char *) ctx->cur_page->area + ctx->cur_offset));

  error = rbl_log_read_add_align (ctx, length);
  if (error != NO_ERROR)
    {
      return error;
    }

  return error;
}

static PARSER_VARCHAR *
rbl_print_insert_att_values (PARSER_CONTEXT * parser,
			     SM_CLASS * class_, MOBJ obj)
{
  PARSER_VARCHAR *buffer = NULL;
  SM_ATTRIBUTE *att;
  char *mem;
  int c;
  UNUSED_VAR int error;
  DB_VALUE v;

  c = 0;
  for (att = class_->ordered_attributes; att != NULL; att = att->order_link)
    {
      mem = (char *) (((char *) obj) + att->offset);
      error = PRIM_GETMEM (att->sma_domain->type, att->sma_domain, mem, &v);

      buffer = describe_value (parser, buffer, &v);

      if (c < class_->att_count - 1)
	{
	  buffer = pt_append_nulstring (parser, buffer, ",");
	}

      pr_clear_value (&v);
      c++;
    }

  return buffer;
}

static char *
rbl_write_insert_sql (SM_CLASS * class_, MOBJ obj)
{
  PARSER_CONTEXT *parser;
  PARSER_VARCHAR *buffer = NULL;
  PARSER_VARCHAR *att_values;
  char *sql;
  int sql_len;

  parser = parser_create_parser ();

  att_values = rbl_print_insert_att_values (parser, class_, obj);

  buffer = pt_append_nulstring (parser, buffer, "INSERT INTO [");
  buffer = pt_append_nulstring (parser, buffer, class_->header.name);
  buffer = pt_append_nulstring (parser, buffer, "] VALUES (");
  buffer = pt_append_varchar (parser, buffer, att_values);
  buffer = pt_append_nulstring (parser, buffer, ");");

  sql_len = pt_get_varchar_length (buffer);
  RBL_ASSERT (sql_len > 0);

  sql = (char *) malloc (sql_len + 1);
  if (sql == NULL)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_OUT_OF_MEMORY, sql_len + 1);
      parser_free_parser (parser);
      return NULL;
    }

  memcpy (sql, (const char *) pt_get_varchar_bytes (buffer), sql_len);
  sql[sql_len] = '\0';

  parser_free_parser (parser);
  return sql;
}

static char *
rbl_write_replace_sql (SM_CLASS * class_, MOBJ obj)
{
  PARSER_CONTEXT *parser;
  PARSER_VARCHAR *buffer = NULL;
  PARSER_VARCHAR *att_values;
  char *sql;
  int sql_len;

  parser = parser_create_parser ();

  att_values = rbl_print_insert_att_values (parser, class_, obj);

  buffer = pt_append_nulstring (parser, buffer, "REPLACE INTO [");
  buffer = pt_append_nulstring (parser, buffer, class_->header.name);
  buffer = pt_append_nulstring (parser, buffer, "] VALUES (");
  buffer = pt_append_varchar (parser, buffer, att_values);
  buffer = pt_append_nulstring (parser, buffer, ");");

  sql_len = pt_get_varchar_length (buffer);
  RBL_ASSERT (sql_len > 0);

  sql = (char *) malloc (sql_len + 1);
  if (sql == NULL)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_OUT_OF_MEMORY, sql_len + 1);
      parser_free_parser (parser);
      return NULL;
    }

  memcpy (sql, (const char *) pt_get_varchar_bytes (buffer), sql_len);
  sql[sql_len] = '\0';

  parser_free_parser (parser);
  return sql;
}

static bool
rbl_is_pk_attribute (SM_CLASS_CONSTRAINT * pk, SM_ATTRIBUTE * att)
{
  int i;

  for (i = 0; i < pk->num_atts; i++)
    {
      if (att->id == pk->attributes[i]->id)
	{
	  return true;
	}
    }

  return false;
}

static PARSER_VARCHAR *
rbl_print_pk (PARSER_CONTEXT * parser, SM_CLASS * class_, MOBJ obj)
{
  PARSER_VARCHAR *buffer = NULL;
  SM_ATTRIBUTE *att;
  char *mem;
  int c;
  UNUSED_VAR int error;
  SM_CLASS_CONSTRAINT *pk_cons;
  DB_VALUE v;

  pk_cons = classobj_find_class_primary_key (class_);
  if (pk_cons == NULL || pk_cons->attributes == NULL
      || pk_cons->attributes[0] == NULL)
    {
      return NULL;
    }

  c = 0;
  for (att = class_->ordered_attributes; att != NULL; att = att->order_link)
    {
      if (rbl_is_pk_attribute (pk_cons, att) == false)
	{
	  continue;
	}

      if (c > 0)
	{
	  buffer = pt_append_nulstring (parser, buffer, " AND ");
	}

      mem = (char *) (((char *) obj) + att->offset);
      error = PRIM_GETMEM (att->sma_domain->type, att->sma_domain, mem, &v);

      buffer = pt_append_nulstring (parser, buffer, "[");
      buffer = pt_append_nulstring (parser, buffer, att->name);
      buffer = pt_append_nulstring (parser, buffer, "]");
      buffer = pt_append_nulstring (parser, buffer, " = ");
      buffer = describe_value (parser, buffer, &v);

      pr_clear_value (&v);
      c++;
    }

  return buffer;
}


static char *
rbl_write_delete_sql (SM_CLASS * class_, MOBJ obj)
{
  PARSER_CONTEXT *parser;
  PARSER_VARCHAR *buffer = NULL, *pkey;
  char *sql;
  int sql_len;

  parser = parser_create_parser ();

  pkey = rbl_print_pk (parser, class_, obj);
  if (pkey == NULL)
    {
      parser_free_parser (parser);
      return NULL;
    }

  buffer = pt_append_nulstring (parser, buffer, "DELETE FROM [");
  buffer = pt_append_nulstring (parser, buffer, class_->header.name);
  buffer = pt_append_nulstring (parser, buffer, "] WHERE ");
  buffer = pt_append_varchar (parser, buffer, pkey);
  buffer = pt_append_nulstring (parser, buffer, ";");

  sql_len = pt_get_varchar_length (buffer);
  RBL_ASSERT (sql_len > 0);

  sql = (char *) malloc (sql_len + 1);
  if (sql == NULL)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_OUT_OF_MEMORY, sql_len + 1);
      parser_free_parser (parser);
      return NULL;
    }

  memcpy (sql, (const char *) pt_get_varchar_bytes (buffer), sql_len);
  sql[sql_len] = '\0';

  parser_free_parser (parser);
  return sql;
}

static DB_OBJECT *
rbl_get_find_table (OID * class_oid)
{
  DB_OBJLIST *t;
  DB_OBJECT *class_obj = NULL;

  for (t = all_Tables; t != NULL; t = t->next)
    {
      if (sm_is_system_table (t->op))
	{
	  continue;
	}

      if (OID_EQ (&(t->op->ws_oid), class_oid))
	{
	  class_obj = t->op;
	  break;
	}
    }

  return class_obj;
}

static char *
rbl_make_sql (RECDES * recdes, OID * class_oid, LOG_RCVINDEX rcvindex)
{
  int au_save;
  DB_OBJECT *class_obj;
  MOBJ mclass, obj = NULL;
  SM_CLASS *class_;
  int ignore;
  char *sql = NULL;

  class_obj = rbl_get_find_table (class_oid);
  if (class_obj == NULL)
    {
      return NULL;
    }

  mclass = locator_fetch_class (class_obj, S_LOCK);
  if (mclass == NULL)
    {
      return NULL;
    }

  class_ = (SM_CLASS *) mclass;
  if (strcasecmp (class_->header.name, "shard_db") == 0
      || strcasecmp (class_->header.name, "shard_node") == 0
      || strcasecmp (class_->header.name, "shard_groupid") == 0
      || strcasecmp (class_->header.name, "shard_migration") == 0)
    {
      return NULL;
    }

  AU_SAVE_AND_DISABLE (au_save);

  obj = tf_disk_to_mem (mclass, recdes, &ignore);
  if (obj == NULL)
    {
      AU_RESTORE (au_save);
      return NULL;
    }

  switch (rcvindex)
    {
    case RVHF_INSERT:
    case RVOVF_NEWPAGE_INSERT:
      if (recdes->type == REC_NEWHOME)
	{
	  sql = rbl_write_replace_sql (class_, obj);
	}
      else
	{
	  sql = rbl_write_insert_sql (class_, obj);
	}
      break;
    case RVHF_UPDATE:
    case RVOVF_PAGE_UPDATE:
      sql = rbl_write_replace_sql (class_, obj);
      break;
    case RVHF_DELETE:
      sql = rbl_write_delete_sql (class_, obj);
      break;
    default:
      RBL_ASSERT (0);
      break;
    }

  AU_RESTORE (au_save);
  if (obj != NULL)
    {
      obj_free_memory (class_, obj);
    }

  return sql;
}

static int
rbl_analyze_log_record (RBL_SYNC_CONTEXT * ctx, LOG_RECORD_HEADER * lrec)
{
  struct log_undoredo undoredo;
  struct log_redo redo;
  struct log_gid_bitmap_update gid_update;
  bool is_diff, is_completed;
  char *sql = NULL;
  RECDES recdes = RECDES_INITIALIZER;
  OID class_oid;
  RBL_OVERFLOW_DATA *ovfl_rec;
  int error;

  switch (lrec->type)
    {
    case LOG_UNDOREDO_DATA:
    case LOG_DIFF_UNDOREDO_DATA:
      error = rbl_get_undoredo_info (ctx, &undoredo);
      if (error != NO_ERROR)
	{
	  RBL_ASSERT (0);
	  return error;
	}

      if (undoredo.data.gid != ctx->gid)
	{
	  break;
	}

      is_diff = (lrec->type == LOG_DIFF_UNDOREDO_DATA);

      if (undoredo.data.rcvindex == RVHF_INSERT
	  || undoredo.data.rcvindex == RVHF_UPDATE)
	{
	  error = rbl_get_redo_data (ctx, &undoredo, is_diff, &recdes,
				     &class_oid);
	  if (error != NO_ERROR)
	    {
	      RBL_ASSERT (0);
	      return error;
	    }

	  RBL_ASSERT (recdes.data != NULL);
	  sql = rbl_make_sql (&recdes, &class_oid, undoredo.data.rcvindex);
	  free (recdes.data);
	}
      else if (undoredo.data.rcvindex == RVHF_DELETE)
	{
	  error = rbl_get_undo_data (ctx, &undoredo, is_diff, &recdes,
				     &class_oid);
	  if (error != NO_ERROR)
	    {
	      RBL_ASSERT (0);
	      return error;
	    }

	  RBL_ASSERT (recdes.data != NULL);
	  sql = rbl_make_sql (&recdes, &class_oid, undoredo.data.rcvindex);
	  free (recdes.data);
	}
      else
	{
	  return NO_ERROR;
	}

      if (sql != NULL)
	{
	  error = rbl_tran_list_add (lrec->trid, sql);
	  if (error != NO_ERROR)
	    {
	      return error;
	    }
	}

      break;

    case LOG_REDO_DATA:
      error = rbl_get_redo_info (ctx, &redo);
      if (error != NO_ERROR)
	{
	  RBL_ASSERT (0);
	  return error;
	}

      if (redo.data.gid != ctx->gid)
	{
	  break;
	}

      if (redo.data.rcvindex == RVOVF_NEWPAGE_INSERT
	  || redo.data.rcvindex == RVOVF_PAGE_UPDATE)
	{
	  error = rbl_get_ovfl_redo_data (ctx, &redo, lrec->trid,
					  &is_completed);
	  if (error != NO_ERROR)
	    {
	      mht_rem (ht_Tran_ovfl_rec, &lrec->trid, rbl_free_ovfl_data,
		       NULL);
	      return error;
	    }

	  if (is_completed == true)
	    {
	      ovfl_rec = (RBL_OVERFLOW_DATA *) mht_get (ht_Tran_ovfl_rec,
							&lrec->trid);
	      if (ovfl_rec == NULL)
		{
		  RBL_ASSERT (0);
		  return ER_FAILED;
		}

	      RBL_ASSERT (ovfl_rec->recdes.data != NULL);

	      /* in case of overflow update, rcvindex is RVOVF_NEWPAGE_INSERT.
	       * 3rd parameter of rbl_make_sql() should be RVOVF_PAGE_UPDATE
	       * that will generate REPLACE query
	       */
	      sql = rbl_make_sql (&ovfl_rec->recdes, &ovfl_rec->class_oid,
				  RVOVF_PAGE_UPDATE);

	      mht_rem (ht_Tran_ovfl_rec, &lrec->trid, rbl_free_ovfl_data,
		       NULL);

	      if (sql != NULL)
		{
		  error = rbl_tran_list_add (lrec->trid, sql);
		  if (error != NO_ERROR)
		    {
		      return error;
		    }
		}
	    }
	}

      break;

    case LOG_DUMMY_OVF_RECORD:
      error = rbl_make_ovfl_data (ctx, lrec->trid);
      if (error != NO_ERROR)
	{
	  return error;
	}
      break;

    case LOG_DUMMY_OVF_RECORD_DEL:
      error = rbl_get_redo_info (ctx, &redo);
      if (error != NO_ERROR)
	{
	  return error;
	}

      RBL_ASSERT (redo.data.rcvindex == RVHF_DELETE);
      if (redo.data.gid != ctx->gid)
	{
	  break;
	}

      error = rbl_get_ovfl_redo_record (ctx, &redo, &recdes, &class_oid);
      if (error != NO_ERROR)
	{
	  return error;
	}

      RBL_ASSERT (recdes.data != NULL);
      sql = rbl_make_sql (&recdes, &class_oid, redo.data.rcvindex);
      free (recdes.data);

      if (sql != NULL)
	{
	  error = rbl_tran_list_add (lrec->trid, sql);
	  if (error != NO_ERROR)
	    {
	      return error;
	    }
	}

      break;

    case LOG_DUMMY_UPDATE_GID_BITMAP:
      error = rbl_log_get_updated_gid (ctx, &gid_update);
      if (error != NO_ERROR)
	{
	  return error;
	}

      if (gid_update.migrator_id == ctx->migrator_id
	  && gid_update.group_id == ctx->gid)
	{
	  ctx->shutdown = true;
	}
      break;

    case LOG_COMMIT:
      mht_rem (ht_Tran_ovfl_rec, &lrec->trid, rbl_free_ovfl_data, NULL);
      error = rbl_sync_execute_query (ctx, lrec->trid, ctx->gid);
      if (error != NO_ERROR)
	{
	  return error;
	}

      break;

    case LOG_ABORT:
      mht_rem (ht_Tran_ovfl_rec, &lrec->trid, rbl_free_ovfl_data, NULL);
      rbl_clear_tran_list (lrec->trid);
      break;

    default:
      break;
    }

  return NO_ERROR;
}

static int
rbl_analyze_log_pages (RBL_SYNC_CONTEXT * ctx, bool * meet_end_of_log)
{
  LOG_RECORD_HEADER lrec;
  int error;

  ctx->cur_page_index = 0;
  while (ctx->cur_page_index < ctx->num_log_pages)
    {
      ctx->cur_page = rbl_next_log_page (ctx);
      if (ctx->cur_page == NULL)
	{
	  RBL_ASSERT (0);
	  return RBL_LOG_PAGE_ERROR;
	}

      if (LSA_ISNULL (&ctx->synced_lsa))
	{
	  /* read the first log record in the page */
	  ctx->final_lsa.offset = ctx->cur_page->hdr.offset;
	}

      RBL_ASSERT (ctx->final_lsa.pageid >= ctx->cur_pageid);
      while (ctx->final_lsa.pageid == ctx->cur_pageid)
	{
	  lrec =
	    *(LOG_GET_LOG_RECORD_HEADER (ctx->cur_page, &ctx->final_lsa));

	  RBL_DEBUG (ARG_FILE_LINE, "Log Type = %d, Forw_LSA = (%lld, %d)",
		     lrec.type, lrec.forw_lsa.pageid, lrec.forw_lsa.offset);

	  if (lrec.type == LOG_END_OF_LOG
	      || LSA_EQ (&ctx->final_lsa, &ctx->server_lsa))
	    {
	      *meet_end_of_log = true;
	      return NO_ERROR;
	    }

	  if (lrec.trid == NULL_TRANID
	      || LSA_GE (&lrec.prev_tranlsa, &ctx->final_lsa)
	      || LSA_GE (&lrec.back_lsa, &ctx->final_lsa)
	      || LSA_LE (&lrec.forw_lsa, &ctx->final_lsa))
	    {
	      RBL_ASSERT (0);
	      return ER_LOG_PAGE_CORRUPTED;
	    }

	  error = rbl_analyze_log_record (ctx, &lrec);
	  if (error != NO_ERROR)
	    {
	      RBL_ASSERT (0);
	      return error;
	    }

	  if (ctx->shutdown == true)
	    {
	      return NO_ERROR;
	    }

	  LSA_COPY (&ctx->synced_lsa, &ctx->final_lsa);
	  LSA_COPY (&ctx->final_lsa, &lrec.forw_lsa);
	}
    }

  return NO_ERROR;
}

/*
 * rbl_sync_log -
 *
 * return: NO_ERROR if successful, error_code otherwise
 *
 * Note:
 */
int
rbl_sync_log (RBL_SYNC_CONTEXT * ctx)
{
  int error = NO_ERROR;
  bool meet_end_of_log = false;

  error = rbl_sync_query_init ();
  if (error != NO_ERROR)
    {
      return error;
    }

  while (ctx->shutdown == false)
    {
      ctx->last_recv_pageid = ctx->final_lsa.pageid;
      RBL_ASSERT (ctx->last_recv_pageid > 0);

      error = rbl_get_log_pages (ctx);
      if (error != NO_ERROR)
	{
	  RBL_ERROR (ARG_FILE_LINE, RBL_LOG_PAGE_ERROR,
		     ctx->last_recv_pageid, error);
	  break;
	}

      rbl_arrange_log_pages (ctx);
      error = rbl_analyze_log_pages (ctx, &meet_end_of_log);

      if (error != NO_ERROR)
	{
	  RBL_NOTICE (ARG_FILE_LINE,
		      "Log page analysis fail: error = %d", error);
	  assert (0);
	  break;
	}

      if (meet_end_of_log == true)
	{
	  THREAD_SLEEP (100);
	}
    }

  return error;
}

int
rbl_sync_check_delay (RBL_SYNC_CONTEXT * ctx)
{
  int run_time, lps;
  INT64 max_delay_msec;
  int retry = 0;

  max_delay_msec = prm_get_bigint_value (PRM_ID_MIGRATOR_MAX_REPL_DELAY);

  while (true)
    {
      run_time = time (NULL) - ctx->start_time;

      if (run_time < 1 || ctx->total_log_pages == 0)
	{
	  break;
	}

      lps = (ctx->total_log_pages / run_time) + 1;
      RBL_DEBUG (ARG_FILE_LINE,
		 "rbl_sync_check_delay: lps = %d, delay = %d\n", lps,
		 ctx->delay);

      if (ctx->delay <= ctx->max_log_pages
	  || ctx->delay <= (lps * (max_delay_msec / 1000.0f)))
	{
	  break;
	}

      if (++retry >= 100)
	{
	  RBL_ERROR_MSG (ARG_FILE_LINE,
			 "Give up migration due to log sync delay : "
			 "log_pages/sec = %d, delay = %d\n", lps, ctx->delay);
	  return ER_FAILED;
	}

      THREAD_SLEEP (1000);
    }

  return NO_ERROR;
}
