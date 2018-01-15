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
 * repl_applier.c -
 */

#ident "$Id$"

#include <sys/time.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <errno.h>

#include "db.h"
#include "error_manager.h"
#include "locator_cl.h"
#include "transform.h"
#include "object_print.h"

#include "cas_common.h"

#include "cci_util.h"
#include "cas_cci_internal.h"

#include "repl_common.h"
#include "repl.h"
#include "repl_applier.h"

#include "monitor.h"


#define LA_MAX_NUM_UNCOMMITED_TRAN		((CIRP_TRAN_Q_SIZE)/2)

typedef struct la_ovf_first_part LA_OVF_FIRST_PART;
struct la_ovf_first_part
{
  VPID next_vpid;
  OID class_oid;
  int length;
  char data[1];			/* Really more than one */
};

typedef struct la_ovf_rest_parts LA_OVF_REST_PARTS;
struct la_ovf_rest_parts
{
  VPID next_vpid;
  char data[1];			/* Really more than one */
};

/* use overflow page list to reduce memory copy overhead. */
typedef struct la_ovf_page_list LA_OVF_PAGE_LIST;
struct la_ovf_page_list
{
  char *rec_type;		/* record type */
  char *data;			/* overflow page data: header + real data */
  int length;			/* total length of data */
  LA_OVF_PAGE_LIST *next;	/* next page */
};


static int cirp_get_undoredo_diff (CIRP_BUF_MGR * buf_mgr,
				   LOG_PAGE ** org_pgptr, LOG_PAGEID * pageid,
				   PGLENGTH * offset, bool * is_undo_zip,
				   char **undo_data, int *undo_length);
static char *cirp_get_zipped_data (CIRP_BUF_MGR * buf_mgr, char *undo_data,
				   int undo_length, bool is_diff,
				   bool is_undo_zip, bool is_overflow,
				   char **rec_type, char **data, int *length);
static int cirp_get_log_data (CIRP_BUF_MGR * buf_mgr,
			      LOG_RECORD_HEADER * lrec, LOG_LSA * lsa,
			      LOG_PAGE * org_pgptr,
			      unsigned int match_rcvindex,
			      unsigned int *rcvindex, void **logs,
			      char **rec_type, char **data, int *d_length);
static int cirp_get_overflow_recdes (CIRP_BUF_MGR * buf_mgr,
				     LOG_RECORD_HEADER * log_record,
				     void *logs, RECDES * recdes,
				     unsigned int rcvindex);
static int cirp_get_relocation_recdes (CIRP_BUF_MGR * buf_mgr,
				       LOG_RECORD_HEADER * lrec,
				       LOG_PAGE * org_pgptr,
				       void **logs, char **rec_type,
				       RECDES * recdes);
static int cirp_get_recdes (CIRP_BUF_MGR * buf_mgr, LOG_LSA * lsa,
			    LOG_PAGE * org_pgptr, RECDES * recdes,
			    unsigned int *rcvindex, char *rec_type);

static int cirp_add_repl_object (CIRP_APPLIER_INFO * applier,
				 CIRP_REPL_ITEM * item);
static int cirp_flush_repl_items (CIRP_APPLIER_INFO * applier,
				  bool immediate);
static int cirp_free_repl_item_list (CIRP_APPLIER_INFO * applier);

static int cirp_apply_delete_log (CIRP_APPLIER_INFO * applier,
				  RP_DATA_ITEM * item);
static int cirp_apply_insert_log (CIRP_APPLIER_INFO * applier,
				  RP_DATA_ITEM * item);
static int cirp_apply_update_log (CIRP_APPLIER_INFO * applier,
				  RP_DATA_ITEM * item);
static int cirp_apply_schema_log (CIRP_APPLIER_INFO * applier,
				  CIRP_REPL_ITEM * item);

static int rp_appl_apply_repl_item (CIRP_APPLIER_INFO * applier,
				    LOG_PAGE * log_pgptr, int log_type,
				    LOG_LSA * final_lsa);
static int cirp_appl_apply_log_record (CIRP_APPLIER_INFO * applier,
				       LOG_LSA * commit_lsa,
				       LOG_RECORD_HEADER * lrec,
				       LOG_LSA final_lsa, LOG_PAGE * pg_ptr);
static int cirp_appl_commit_transaction (CIRP_APPLIER_INFO * applier,
					 LOG_LSA * commit_lsa);
static int cirp_applier_update_progress (CIRP_CT_LOG_APPLIER * ct_data,
					 LOG_LSA * committed_lsa);
static int cirp_applier_clear_repl_delay (CIRP_APPLIER_INFO * applier);
static int cirp_get_applier_data (CIRP_APPLIER_INFO * applier,
				  CIRP_CT_LOG_APPLIER * ct_data);
static int cirp_set_applier_data (CIRP_APPLIER_INFO * applier,
				  CIRP_CT_LOG_APPLIER * ct_data);
static int cirp_change_applier_status (CIRP_APPLIER_INFO * applier,
				       CIRP_AGENT_STATUS status);
static int rp_appl_apply_schema_item (CIRP_APPLIER_INFO * applier,
				      LOG_PAGE * log_pgptr,
				      const LOG_LSA * final_lsa);
static int rp_appl_apply_data_item (CIRP_APPLIER_INFO * applier,
				    LOG_PAGE * log_pgptr,
				    const LOG_LSA * final_lsa);
static int rp_appl_apply_gid_bitmap_item (CIRP_APPLIER_INFO * applier,
					  LOG_PAGE * log_pgptr,
					  const LOG_LSA * final_lsa);

/*
 * cirp_get_applier_status ()-
 *   return: ha agent status
 *
 *   analyzer(in):
 */
CIRP_AGENT_STATUS
cirp_get_applier_status (CIRP_APPLIER_INFO * applier)
{
  CIRP_AGENT_STATUS status;

  pthread_mutex_lock (&applier->lock);
  status = applier->status;
  pthread_mutex_unlock (&applier->lock);

  return status;
}

/*
 * cirp_change_applier_status ()-
 *    return: NO_ERROR
 *
 *    analyzer(in/out):
 *    status(in):
 */
static int
cirp_change_applier_status (CIRP_APPLIER_INFO * applier,
			    CIRP_AGENT_STATUS status)
{
  pthread_mutex_lock (&applier->lock);
  applier->status = status;
  pthread_mutex_unlock (&applier->lock);

  return NO_ERROR;
}

/*
 * cirp_get_undoredo_diff() - get undo/redo diff data
 *   return: next log page pointer
 */
static int
cirp_get_undoredo_diff (CIRP_BUF_MGR * buf_mgr, LOG_PAGE ** org_pgptr,
			LOG_PAGEID * pageid,
			PGLENGTH * offset, bool * is_undo_zip,
			char **undo_data, int *undo_length)
{
  int error = NO_ERROR;

  LOG_ZIP *undo_unzip_data = NULL;

  LOG_PAGE *temp_pg;
  LOG_PAGEID temp_pageid;
  PGLENGTH temp_offset;

  /* FIXME-notout: */
  undo_unzip_data = buf_mgr->undo_unzip;

  temp_pg = *org_pgptr;
  temp_pageid = *pageid;
  temp_offset = *offset;

  if (ZIP_CHECK (*undo_length))
    {				/* Undo data is Zip Check */
      *is_undo_zip = true;
      *undo_length = GET_ZIP_LEN (*undo_length);
    }

  *undo_data = (char *) RYE_MALLOC (*undo_length);
  if (*undo_data == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, *undo_length);
      GOTO_EXIT_ON_ERROR;
    }

  /* get undo data for XOR process */
  error = cirp_log_copy_fromlog (buf_mgr, NULL, *undo_data, *undo_length,
				 *pageid, *offset, *org_pgptr);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (*is_undo_zip && *undo_length > 0)
    {
      if (!log_unzip (undo_unzip_data, *undo_length, *undo_data))
	{
	  error = ER_IO_LZO_DECOMPRESS_FAIL;

	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	  GOTO_EXIT_ON_ERROR;
	}
    }
  error = rp_log_read_add_align (buf_mgr, &temp_pg, &temp_pageid,
				 &temp_offset, *undo_length, temp_pg);
  if (error != NO_ERROR || temp_pg == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  *org_pgptr = temp_pg;
  *pageid = temp_pageid;
  *offset = temp_offset;

  return error;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "Invalid error code");
    }

  RYE_FREE_MEM (*undo_data);

  return error;
}

/*
 * cirp_get_zipped_data () - get zipped data
 *   return: error code
 */
static char *
cirp_get_zipped_data (CIRP_BUF_MGR * buf_mgr, char *undo_data,
		      int undo_length, bool is_diff, bool is_undo_zip,
		      bool is_overflow, char **rec_type, char **data,
		      int *length)
{
  int redo_length = 0;
  int rec_len = 0;
  int skip_len = DB_SIZEOF (OID);	/* to skip class_oid */

  LOG_ZIP *undo_unzip_data = NULL;
  LOG_ZIP *redo_unzip_data = NULL;

  /* FIXME-notout: */
  undo_unzip_data = buf_mgr->undo_unzip;
  redo_unzip_data = buf_mgr->redo_unzip;

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

  if (rec_type)
    {
      rec_len = DB_SIZEOF (INT16);
      *length = redo_length - rec_len - skip_len;
    }
  else
    {
      *length = redo_length;
    }

  if (is_overflow)
    {
      RYE_FREE_MEM (*data);

      *data = RYE_MALLOC (*length);
      if (*data == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, *length);
	  *length = 0;
	  return NULL;
	}
    }

  if (rec_type)
    {
      /* FIXME-notout: */
      memcpy (*rec_type, (buf_mgr->redo_unzip)->log_data, rec_len);
      memcpy (*data, (buf_mgr->redo_unzip)->log_data + rec_len + skip_len,
	      *length);
    }
  else
    {
      /* FIXME-notout: */
      memcpy (*data, (buf_mgr->redo_unzip)->log_data, redo_length);
    }

  return *data;
}

/*
 * cirp_get_log_data() - get the data area of log record
 *   return: error code
 *   lrec (in) : target log record
 *   lsa (in) : the LSA of the target log record
 *   pgptr (in) : the start log page pointer
 *   match_rcvindex (in) : index
 *   rcvindex : recovery index to be returned
 *   logs : the specialized log info
 *   rec_type : the type of RECDES
 *   data : the log data
 *   d_length : the length of data
 *
 * Note: get the data area, and rcvindex, length of data for the
 *              given log record
 */
static int
cirp_get_log_data (CIRP_BUF_MGR * buf_mgr, LOG_RECORD_HEADER * lrec,
		   LOG_LSA * lsa, LOG_PAGE * org_pgptr,
		   unsigned int match_rcvindex, unsigned int *rcvindex,
		   void **logs, char **rec_type, char **data, int *d_length)
{
  LOG_PAGE *pgptr = NULL;
  UNUSED_VAR LOG_PAGE *old_pg;
  PGLENGTH offset;
  int length;			/* type change PGLENGTH -> int */
  LOG_PAGEID pageid;
  int error = NO_ERROR;
  struct log_undoredo *undoredo;
  struct log_redo *redo;

  bool is_undo_zip = false;
  int zip_len = 0;
  int undo_length = 0;
  int temp_length = 0;
  char *undo_data = NULL;

  bool is_overflow = false;
  bool is_diff = false;

  pgptr = org_pgptr;

  offset = DB_SIZEOF (LOG_RECORD_HEADER) + lsa->offset;
  pageid = lsa->pageid;

  error = rp_log_read_align (buf_mgr, &pgptr, &pageid, &offset, org_pgptr);
  if (error != NO_ERROR || pgptr == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  switch (lrec->type)
    {
    case LOG_UNDOREDO_DATA:
    case LOG_DIFF_UNDOREDO_DATA:
      is_diff = (lrec->type == LOG_DIFF_UNDOREDO_DATA) ? true : false;

      length = DB_SIZEOF (struct log_undoredo);
      old_pg = pgptr;
      error = rp_log_read_advance_when_doesnt_fit (buf_mgr, &pgptr, &pageid,
						   &offset, length,
						   org_pgptr);
      if (error != NO_ERROR || pgptr == NULL)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      undoredo = (struct log_undoredo *) ((char *) pgptr->area + offset);

      undo_length = undoredo->ulength;	/* undo log length */
      temp_length = undoredo->rlength;	/* for the replication, we just need
					 * the redo data */
      length = GET_ZIP_LEN (undoredo->rlength);
      assert (length != 0);

      if (match_rcvindex == 0 || undoredo->data.rcvindex == match_rcvindex)
	{
	  if (rcvindex)
	    {
	      *rcvindex = undoredo->data.rcvindex;
	    }
	  if (logs)
	    {
	      *logs = (void *) undoredo;
	    }
	}
      else if (logs)
	{
	  *logs = (void *) NULL;
	}

      error = rp_log_read_add_align (buf_mgr, &pgptr, &pageid, &offset,
				     DB_SIZEOF (*undoredo), org_pgptr);
      if (error != NO_ERROR || pgptr == NULL)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      if (is_diff)
	{			/* XOR Redo Data */
	  error = cirp_get_undoredo_diff (buf_mgr, &pgptr, &pageid, &offset,
					  &is_undo_zip, &undo_data,
					  &undo_length);
	}
      else
	{
	  error = rp_log_read_add_align (buf_mgr, &pgptr, &pageid, &offset,
					 GET_ZIP_LEN (undo_length),
					 org_pgptr);
	}
      if (error != NO_ERROR || pgptr == NULL)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      break;

    case LOG_REDO_DATA:
      length = DB_SIZEOF (struct log_redo);
      error = rp_log_read_advance_when_doesnt_fit (buf_mgr, &pgptr,
						   &pageid, &offset, length,
						   org_pgptr);
      if (error != NO_ERROR || pgptr == NULL)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      redo = (struct log_redo *) ((char *) pgptr->area + offset);
      temp_length = redo->length;
      length = GET_ZIP_LEN (redo->length);

      if (match_rcvindex == 0 || redo->data.rcvindex == match_rcvindex)
	{
	  if (logs)
	    {
	      *logs = (void *) redo;
	    }
	  if (rcvindex)
	    {
	      *rcvindex = redo->data.rcvindex;
	    }
	}
      else if (logs)
	{
	  *logs = (void *) NULL;
	}

      error = rp_log_read_add_align (buf_mgr, &pgptr, &pageid, &offset,
				     DB_SIZEOF (*redo), org_pgptr);
      if (error != NO_ERROR || pgptr == NULL)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      break;

    default:
      if (logs)
	{
	  *logs = NULL;
	}

      assert (false);

      REPL_SET_GENERIC_ERROR (error, "Invalid log record");
      GOTO_EXIT_ON_ERROR;
    }
  assert (error == NO_ERROR);

  if (*data == NULL)
    {
      /* general cases, use the pre-allocated buffer */
      *data = RYE_MALLOC (length);
      is_overflow = true;

      if (*data == NULL)
	{
	  *d_length = 0;

	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, length);
	  GOTO_EXIT_ON_ERROR;
	}
    }

  if (ZIP_CHECK (temp_length))
    {
      zip_len = GET_ZIP_LEN (temp_length);
      assert (zip_len != 0);

      /* Get Zip Data */
      error = cirp_log_copy_fromlog (buf_mgr, NULL, *data, zip_len, pageid,
				     offset, pgptr);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      if (log_unzip (buf_mgr->redo_unzip, zip_len, *data) == false)
	{
	  error = ER_IO_LZO_DECOMPRESS_FAIL;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	  GOTO_EXIT_ON_ERROR;
	}

      *data = cirp_get_zipped_data (buf_mgr, undo_data, undo_length, is_diff,
				    is_undo_zip, is_overflow, rec_type, data,
				    &length);
      if (*data == NULL)
	{
	  error = er_errid ();
	  GOTO_EXIT_ON_ERROR;
	}
    }
  else
    {
      /* Get Redo Data */
      error = cirp_log_copy_fromlog (buf_mgr,
				     rec_type ? *rec_type : NULL, *data,
				     length, pageid, offset, pgptr);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  *d_length = length;

  RYE_FREE_MEM (undo_data);

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

  RYE_FREE_MEM (undo_data);

  if (pgptr != NULL && pgptr != org_pgptr)
    {
      cirp_logpb_release (buf_mgr, pgptr->hdr.logical_pageid);
    }

  return error;
}

/*
 * cirp_get_overflow_recdes() - prepare the overflow page update
 *   return: NO_ERROR or error code
 *
 *   bug_mgr(in/out):
 *   log_record(in):
 *   logs(in):
 *   recdes(in/out)
 *   rcvindex(in):
 */
static int
cirp_get_overflow_recdes (CIRP_BUF_MGR * buf_mgr,
			  LOG_RECORD_HEADER * log_record, void *logs,
			  RECDES * recdes, unsigned int rcvindex)
{
  LOG_LSA current_lsa;
  LOG_PAGE *current_log_page;
  LOG_RECORD_HEADER *current_log_record;
  LA_OVF_PAGE_LIST *ovf_list_head = NULL;
  LA_OVF_PAGE_LIST *ovf_list_tail = NULL;
  LA_OVF_PAGE_LIST *ovf_list_data = NULL;
  void *log_info;
  UNUSED_VAR VPID prev_vpid;
  bool first = true;
  int copyed_len;
  int area_len;
  int area_offset;
  int error = NO_ERROR;
  int length = 0;

  LSA_COPY (&current_lsa, &log_record->prev_tranlsa);
  prev_vpid.pageid = ((struct log_undoredo *) logs)->data.pageid;
  prev_vpid.volid = ((struct log_undoredo *) logs)->data.volid;

  while (!LSA_ISNULL (&current_lsa))
    {
      error = cirp_logpb_get_log_page (buf_mgr, &current_log_page,
				       current_lsa.pageid);
      if (error != NO_ERROR || current_log_page == NULL)
	{
	  assert (error != NO_ERROR && current_log_page == NULL);

	  if (error == NO_ERROR)
	    {
	      assert (false);

	      REPL_SET_GENERIC_ERROR (error, "Invalid return value");
	    }
	  GOTO_EXIT_ON_ERROR;
	}

      current_log_record = LOG_GET_LOG_RECORD_HEADER (current_log_page,
						      &current_lsa);
      if (!CIRP_IS_VALID_LOG_RECORD (buf_mgr, current_log_record))
	{
	  REPL_SET_GENERIC_ERROR (error, "Invalid log record");
	  GOTO_EXIT_ON_ERROR;
	}
      if (current_log_record->trid != log_record->trid
	  || current_log_record->type == LOG_DUMMY_OVF_RECORD)
	{
	  /* end overflow record */
	  cirp_logpb_release (buf_mgr, current_log_page->hdr.logical_pageid);
	  current_log_page = NULL;
	  break;
	}
      else if (current_log_record->type == LOG_REDO_DATA)
	{
	  /* process only LOG_REDO_DATA */

	  ovf_list_data = ((LA_OVF_PAGE_LIST *)
			   RYE_MALLOC (DB_SIZEOF (LA_OVF_PAGE_LIST)));
	  if (ovf_list_data == NULL)
	    {
	      /* malloc failed */
	      error = ER_OUT_OF_VIRTUAL_MEMORY;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      error, 1, DB_SIZEOF (LA_OVF_PAGE_LIST));
	      GOTO_EXIT_ON_ERROR;
	    }

	  memset (ovf_list_data, 0, DB_SIZEOF (LA_OVF_PAGE_LIST));
	  error = cirp_get_log_data (buf_mgr, current_log_record,
				     &current_lsa, current_log_page, rcvindex,
				     NULL, &log_info, NULL,
				     &ovf_list_data->data,
				     &ovf_list_data->length);
	  if (error != NO_ERROR)
	    {
	      RYE_FREE_MEM (ovf_list_data->data);
	      RYE_FREE_MEM (ovf_list_data);
	      GOTO_EXIT_ON_ERROR;
	    }

	  if (log_info != NULL && ovf_list_data->data != NULL)
	    {
	      /* add to linked-list */
	      if (ovf_list_head == NULL)
		{
		  ovf_list_head = ovf_list_tail = ovf_list_data;
		}
	      else
		{
		  ovf_list_data->next = ovf_list_head;
		  ovf_list_head = ovf_list_data;
		}

	      length += ovf_list_data->length;
	    }
	  else
	    {
	      RYE_FREE_MEM (ovf_list_data->data);
	      RYE_FREE_MEM (ovf_list_data);
	    }
	}
      LSA_COPY (&current_lsa, &current_log_record->prev_tranlsa);

      cirp_logpb_release (buf_mgr, current_log_page->hdr.logical_pageid);
      current_log_page = NULL;
    }

  assert (recdes != NULL);

  error = cirp_realloc_recdes_data (buf_mgr, recdes, length);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* make record description */
  copyed_len = 0;
  while (ovf_list_head)
    {
      ovf_list_data = ovf_list_head;
      ovf_list_head = ovf_list_head->next;

      if (first)
	{
	  area_offset = offsetof (LA_OVF_FIRST_PART, data);
	  first = false;
	}
      else
	{
	  area_offset = offsetof (LA_OVF_REST_PARTS, data);
	}
      area_len = ovf_list_data->length - area_offset;
      memcpy (recdes->data + copyed_len, ovf_list_data->data + area_offset,
	      area_len);
      copyed_len += area_len;

      RYE_FREE_MEM (ovf_list_data->data);
      RYE_FREE_MEM (ovf_list_data);
    }

  recdes->length = length;

  assert (error == NO_ERROR);
  return error;

exit_on_error:

  if (error == NO_ERROR)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "Invalid error code");
    }

  if (current_log_page != NULL)
    {
      cirp_logpb_release (buf_mgr, current_log_page->hdr.logical_pageid);
      current_log_page = NULL;
    }

  while (ovf_list_head)
    {
      ovf_list_data = ovf_list_head;
      ovf_list_head = ovf_list_head->next;
      RYE_FREE_MEM (ovf_list_data->data);
      RYE_FREE_MEM (ovf_list_data);
    }

  return error;
}

/*
 * cirp_get_relocation_recdes()-
 *   return: error code
 *
 *  lrec(in):
 *  pgptr(in):
 *  match_recindex(in):
 *  logs(out):
 *  rec_type(out):
 *  recdes(out):
 *
 */
static int
cirp_get_relocation_recdes (CIRP_BUF_MGR * buf_mgr,
			    LOG_RECORD_HEADER * lrec, LOG_PAGE * org_pgptr,
			    void **logs, char **rec_type, RECDES * recdes)
{
  LOG_RECORD_HEADER *tmp_lrec;
  unsigned int rcvindex;
  LOG_PAGE *pg = org_pgptr;
  LOG_LSA lsa;
  int error = NO_ERROR;

  LSA_COPY (&lsa, &lrec->prev_tranlsa);
  if (!LSA_ISNULL (&lsa))
    {
      error = cirp_logpb_get_log_page (buf_mgr, &pg, lsa.pageid);
      if (error != NO_ERROR || pg == NULL)
	{
	  assert (error != NO_ERROR && pg == NULL);

	  if (error == NO_ERROR)
	    {
	      assert (false);

	      REPL_SET_GENERIC_ERROR (error, "Invalid return value");
	    }

	  return error;
	}
      tmp_lrec = LOG_GET_LOG_RECORD_HEADER (pg, &lsa);
      if (tmp_lrec->trid != lrec->trid
	  || !CIRP_IS_VALID_LOG_RECORD (buf_mgr, tmp_lrec))
	{
	  error = ER_LOG_PAGE_CORRUPTED;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, lsa.pageid);
	}
      else
	{
	  error = cirp_get_log_data (buf_mgr, tmp_lrec, &lsa, pg,
				     RVHF_INSERT, &rcvindex, logs,
				     rec_type,
				     &recdes->data, &recdes->length);
	}
      cirp_logpb_release (buf_mgr, pg->hdr.logical_pageid);
    }
  else
    {
      assert (false);

      error = ER_LOG_PAGE_CORRUPTED;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, lsa.pageid);
    }

  return error;
}

/*
 * cirp_get_recdes() - get the record description from the log file
 *   return: NO_ERROR or error code
 *
 *    pgptr: point to the target log page
 *    old_recdes(out): old record description (output)
 *    recdes(out): record description (output)
 *    rcvindex(out): recovery index (output)
 *    log_data: log data area
 *    ovf_yn(out)  : true if the log data is in overflow page
 *
 * Note:
 *     To replicate the data, we have to filter the record descripion
 *     from the log record. This function retrieves the record description
 *     for the given lsa.
 */
static int
cirp_get_recdes (CIRP_BUF_MGR * buf_mgr, LOG_LSA * lsa, LOG_PAGE * org_pgptr,
		 RECDES * recdes, unsigned int *rcvindex, char *rec_type)
{
  LOG_RECORD_HEADER *lrec;
  LOG_PAGE *pg;
  int error = NO_ERROR;
  void *logs = NULL;

  pg = org_pgptr;
  lrec = LOG_GET_LOG_RECORD_HEADER (pg, lsa);
  if (!CIRP_IS_VALID_LOG_RECORD (buf_mgr, lrec))
    {
      REPL_SET_GENERIC_ERROR (error, "Invalid log record");
      return error;
    }

  error = cirp_get_log_data (buf_mgr, lrec, lsa, pg, 0, rcvindex,
			     &logs, &rec_type, &recdes->data,
			     &recdes->length);
  if (error != NO_ERROR || logs == NULL)
    {
      er_log_debug (ARG_FILE_LINE,
		    "cannot get log record from LSA(%d|%d)",
		    lsa->pageid, lsa->offset);
      if (error != NO_ERROR)
	{
	  error = ER_FAILED;
	}

      return error;
    }

  recdes->type = *(INT16 *) (rec_type);

  /* Now.. we have to process overflow pages */
  if (*rcvindex == RVOVF_CHANGE_LINK)
    {
      /* if overflow page update */
      error = cirp_get_overflow_recdes (buf_mgr, lrec, logs, recdes,
					RVOVF_PAGE_UPDATE);
      recdes->type = REC_BIGONE;
    }
  else if (recdes->type == REC_BIGONE)
    {
      /* if overflow page insert */
      error = cirp_get_overflow_recdes (buf_mgr, lrec, logs, recdes,
					RVOVF_NEWPAGE_INSERT);
    }
  else if (*rcvindex == RVHF_INSERT && recdes->type == REC_ASSIGN_ADDRESS)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "invalid record type");
    }
  else if (*rcvindex == RVHF_UPDATE && recdes->type == REC_RELOCATION)
    {
      error = cirp_get_relocation_recdes (buf_mgr, lrec, pg,
					  &logs, &rec_type, recdes);
      if (error == NO_ERROR)
	{
	  recdes->type = *(INT16 *) (rec_type);
	}
    }

  assert (error != NO_ERROR || or_grp_id (recdes) >= GLOBAL_GROUPID);

  return error;
}

/*
 * cirp_flush_repl_items() - flush stored repl items to server
 *   return: NO_ERROR or error code
 *   immediate(in): whether to immediately flush or not
 *
 * Note:
 */
static int
cirp_flush_repl_items (CIRP_APPLIER_INFO * applier, bool immediate)
{
  int error = NO_ERROR;
  bool need_flush = false;

  if (applier->num_unflushed == 0)
    {
      return NO_ERROR;
    }

  if ((applier->head->item_type == RP_ITEM_TYPE_DDL
       && applier->num_unflushed != 2)
      || (applier->head->item_type == RP_ITEM_TYPE_CATALOG
	  && applier->num_unflushed != 1))
    {
      assert (false);
      REPL_SET_GENERIC_ERROR (error, "invalid repl items");
      return error;
    }

  if (applier->num_unflushed >= LA_MAX_UNFLUSHED_REPL_ITEMS - 2)
    {
      cci_set_autocommit (&applier->conn, CCI_AUTOCOMMIT_FALSE);
      need_flush = true;
    }
  else if (immediate == true)
    {
      need_flush = true;
    }

  if (need_flush == true)
    {
      assert (rp_is_valid_repl_item (applier->head));

      error = cci_send_repl_data (&applier->conn, applier->head,
				  applier->num_unflushed);
      if (error < 0)
	{
	  monitor_stats_counter (MNT_RP_APPLIER_BASE_ID + applier->ct.id,
				 MNT_RP_FAIL, 1);

	  REPL_SET_GENERIC_ERROR (error, "cci error(%d), msg:%s",
				  applier->conn.err_buf.err_code,
				  applier->conn.err_buf.err_msg);
	}
    }

  if (immediate == true)
    {
      cci_set_autocommit (&applier->conn, CCI_AUTOCOMMIT_TRUE);
    }

  if (need_flush == true && error == NO_ERROR)
    {
      cirp_free_repl_item_list (applier);
    }

  return error;
}

/*
 * cirp_add_repl_object()-
 *   return: error code
 *
 *   applier(in/out):
 *   item(in):
 */
static int
cirp_add_repl_object (CIRP_APPLIER_INFO * applier, CIRP_REPL_ITEM * item)
{
  if (applier->tail == NULL)
    {
      applier->head = item;
      applier->tail = item;
    }
  else
    {
      applier->tail->next = item;
      applier->tail = item;
    }

  applier->num_unflushed++;

  return NO_ERROR;
}

/*
 * cirp_free_repl_item_list()-
 *   return: error code
 *
 *   applier(in/out):
 */
static int
cirp_free_repl_item_list (CIRP_APPLIER_INFO * applier)
{
  CIRP_REPL_ITEM *repl_item;

  while (applier->head != NULL)
    {
      repl_item = applier->head;
      applier->head = repl_item->next;

      cirp_free_repl_item (repl_item);
      repl_item = NULL;

      applier->num_unflushed--;
    }
  applier->tail = NULL;

  assert (applier->num_unflushed == 0);

  return NO_ERROR;
}

/*
 * cirp_apply_delete_log() - apply the delete log to the target slave
 *   return: NO_ERROR or error code
 *   item(in): replication item
 *
 * Note:
 */
static int
cirp_apply_delete_log (CIRP_APPLIER_INFO * applier, RP_DATA_ITEM * data_item)
{
  int error;
  char buf[256];

  error = cirp_flush_repl_items (applier, false);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  monitor_stats_counter (MNT_RP_APPLIER_BASE_ID + applier->ct.id,
			 MNT_RP_DELETE, 1);

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  assert (error != NO_ERROR);

  help_sprint_idxkey (&data_item->key, buf, sizeof (buf) - 1);

  er_stack_push ();
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	  ER_HA_LA_FAILED_TO_APPLY_DELETE, 4, data_item->class_name, buf,
	  error, "internal client error.");
  er_stack_pop ();

  return error;
}

/*
 * cirp_apply_insert_log() - apply the insert log to the target slave
 *   return: NO_ERROR or error code
 *   item : replication item
 *
 * Note:
 *      Apply the insert log to the target slave.
 *      . get the target log page
 *      . get the record description
 *      . fetch the class info
 *      . create a replication object to be flushed and add it to a link
 */
static int
cirp_apply_insert_log (CIRP_APPLIER_INFO * applier, RP_DATA_ITEM * item)
{
  int error = NO_ERROR;
  LOG_PAGE *pgptr = NULL;
  unsigned int rcvindex;
  RECDES *recdes;
  LOG_PAGEID old_pageid = NULL_PAGEID;
  char buf[255];
  CIRP_BUF_MGR *buf_mgr = NULL;

  assert (applier != NULL && item != NULL);

  buf_mgr = &applier->buf_mgr;

  error = cirp_flush_repl_items (applier, false);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* get the target log page */
  old_pageid = item->target_lsa.pageid;
  error = cirp_logpb_get_log_page (buf_mgr, &pgptr, old_pageid);
  if (error != NO_ERROR || pgptr == NULL)
    {
      assert (error != NO_ERROR && pgptr == NULL);

      if (error == NO_ERROR)
	{
	  assert (false);

	  REPL_SET_GENERIC_ERROR (error, "Invalid return value");
	}

      GOTO_EXIT_ON_ERROR;
    }

  error = rp_assign_recdes_from_pool (buf_mgr, &recdes);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* retrieve the target record description */
  /* FIXME-notout: */
  error = cirp_get_recdes (buf_mgr, &item->target_lsa,
			   pgptr, recdes, &rcvindex, buf_mgr->rec_type);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  assert ((or_grp_id (recdes) == item->groupid)
	  || (strcasecmp (item->class_name, CT_SHARD_GID_SKEY_INFO_NAME) == 0
	      && or_grp_id (recdes) == GLOBAL_GROUPID
	      && item->groupid >= GLOBAL_GROUPID));

  if (recdes->type == REC_ASSIGN_ADDRESS || recdes->type == REC_RELOCATION)
    {
      REPL_SET_GENERIC_ERROR (error, "apply_insert : rectype.type = %d\n",
			      recdes->type);

      GOTO_EXIT_ON_ERROR;
    }
  if (rcvindex != RVHF_INSERT)
    {
      REPL_SET_GENERIC_ERROR (error, "apply_insert : rcvindex = %d\n",
			      rcvindex);

      GOTO_EXIT_ON_ERROR;
    }

  item->recdes = recdes;

  monitor_stats_counter (MNT_RP_APPLIER_BASE_ID + applier->ct.id,
			 MNT_RP_INSERT, 1);

  cirp_logpb_release (buf_mgr, pgptr->hdr.logical_pageid);

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  assert (error != NO_ERROR);

  help_sprint_idxkey (&item->key, buf, sizeof (buf) - 1);

  er_stack_push ();
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	  ER_HA_LA_FAILED_TO_APPLY_INSERT, 4, item->class_name, buf,
	  error, "internal client error.");
  er_stack_pop ();

  if (pgptr != NULL)
    {
      cirp_logpb_release (buf_mgr, pgptr->hdr.logical_pageid);
    }

  return error;
}

/*
 * cirp_apply_update_log() - apply the update log to the target slave using server side update
 *   return: NO_ERROR or error code
 *   item : replication item
 *
 * Note:
 *      Apply the update log to the target slave.
 *      . get the target log page
 *      . get the record description
 *      . fetch the class info
 *      . create a replication object to be flushed and add it to a link
 */
static int
cirp_apply_update_log (CIRP_APPLIER_INFO * applier, RP_DATA_ITEM * item)
{
  int error = NO_ERROR;
  unsigned int rcvindex;
  RECDES *recdes;
  LOG_PAGE *pgptr = NULL;
  LOG_PAGEID old_pageid = NULL_PAGEID;
  char buf[255];
  CIRP_BUF_MGR *buf_mgr = NULL;

  assert (applier != NULL && item != NULL);

  buf_mgr = &applier->buf_mgr;

  error = cirp_flush_repl_items (applier, false);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* get the target log page */
  old_pageid = item->target_lsa.pageid;
  error = cirp_logpb_get_log_page (buf_mgr, &pgptr, old_pageid);
  if (error != NO_ERROR || pgptr == NULL)
    {
      assert (error != NO_ERROR && pgptr == NULL);

      if (error == NO_ERROR)
	{
	  assert (false);

	  REPL_SET_GENERIC_ERROR (error, "Invalid return value");
	}

      GOTO_EXIT_ON_ERROR;
    }

  error = rp_assign_recdes_from_pool (buf_mgr, &recdes);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* retrieve the target record description */
  /* FIXME-notout: */
  error = cirp_get_recdes (buf_mgr, &item->target_lsa, pgptr,
			   recdes, &rcvindex, buf_mgr->rec_type);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  assert (or_grp_id (recdes) == item->groupid);

  if (recdes->type == REC_ASSIGN_ADDRESS || recdes->type == REC_RELOCATION)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (rcvindex != RVHF_UPDATE && rcvindex != RVOVF_CHANGE_LINK)
    {
      REPL_SET_GENERIC_ERROR (error, "apply_update : rcvindex = %d\n",
			      rcvindex);

      GOTO_EXIT_ON_ERROR;
    }

  item->recdes = recdes;

  monitor_stats_counter (MNT_RP_APPLIER_BASE_ID + applier->ct.id,
			 MNT_RP_UPDATE, 1);

  cirp_logpb_release (buf_mgr, pgptr->hdr.logical_pageid);

  return error;

exit_on_error:
  assert (error != NO_ERROR);

  help_sprint_idxkey (&item->key, buf, sizeof (buf) - 1);

  er_stack_push ();
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	  ER_HA_LA_FAILED_TO_APPLY_UPDATE, 4, item->class_name, buf,
	  error, "internal client error.");
  er_stack_pop ();

  if (pgptr != NULL)
    {
      cirp_logpb_release (buf_mgr, pgptr->hdr.logical_pageid);
    }

  return error;
}

/*
 * cirp_apply_schema_log () -
 *    return: error code
 *
 *    item(in):
 */
static int
cirp_apply_schema_log (CIRP_APPLIER_INFO * applier, CIRP_REPL_ITEM * item)
{
  int error = NO_ERROR;

  if (item->item_type != RP_ITEM_TYPE_DDL)
    {
      assert (false);
      ;				/* TODO - avoid compiler warning */
    }

  error = cirp_flush_repl_items (applier, false);

  monitor_stats_counter (MNT_RP_APPLIER_BASE_ID + applier->ct.id,
			 MNT_RP_DDL, 1);

  return error;
}

/*
 * rp_appl_apply_schema_item -
 *   return: error code
 *
 *   applier(in/out):
 *   log_pgptr(in):
 *   final_lsa(in):
 */
static int
rp_appl_apply_schema_item (CIRP_APPLIER_INFO * applier,
			   LOG_PAGE * log_pgptr, const LOG_LSA * final_lsa)
{
  CIRP_REPL_ITEM *item = NULL;
  int error = NO_ERROR;

  error = rp_new_repl_item_ddl (&item, final_lsa);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  error = rp_make_repl_schema_item_from_log (&applier->buf_mgr, item,
					     log_pgptr, final_lsa);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = cirp_apply_schema_log (applier, item);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = cirp_add_repl_object (applier, item);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  item = NULL;

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);
      REPL_SET_GENERIC_ERROR (error, "Invalid error code");
    }

  if (item != NULL)
    {
      cirp_free_repl_item (item);
      item = NULL;
    }
  return error;
}

/*
 * rp_appl_apply_data_item -
 *   return: error code
 *
 *   applier(in/out):
 *   log_pgptr(in):
 *   final_lsa(in):
 */
static int
rp_appl_apply_data_item (CIRP_APPLIER_INFO * applier,
			 LOG_PAGE * log_pgptr, const LOG_LSA * final_lsa)
{
  CIRP_REPL_ITEM *item = NULL;
  RP_DATA_ITEM *data = NULL;
  int error = NO_ERROR;

  error = rp_new_repl_item_data (&item, final_lsa);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  error = rp_make_repl_data_item_from_log (&applier->buf_mgr, item,
					   log_pgptr, final_lsa);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  data = &item->info.data;
  switch (data->rcv_index)
    {
    case RVREPL_DATA_UPDATE:
      error = cirp_apply_update_log (applier, data);
      break;

    case RVREPL_DATA_INSERT:
      error = cirp_apply_insert_log (applier, data);
      break;

    case RVREPL_DATA_DELETE:
      error = cirp_apply_delete_log (applier, data);
      break;

    default:
      assert (false);

      REPL_SET_GENERIC_ERROR (error,
			      "rp_appl_apply_repl_item : rcv_index %d "
			      "lsa(%lld,%d) target lsa(%lld,%d)\n",
			      data->rcv_index,
			      (long long) data->lsa.pageid,
			      data->lsa.offset,
			      (long long) data->target_lsa.pageid,
			      data->target_lsa.offset);
    }
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = cirp_add_repl_object (applier, item);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  item = NULL;

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);
      REPL_SET_GENERIC_ERROR (error, "Invalid error code");
    }

  if (item != NULL)
    {
      cirp_free_repl_item (item);
      item = NULL;
    }
  return error;
}

/*
 * rp_appl_apply_gid_bitmap_item -
 *   return: error code
 *
 *   applier(in/out):
 *   log_pgptr(in):
 *   final_lsa(in):
 */
static int
rp_appl_apply_gid_bitmap_item (CIRP_APPLIER_INFO * applier,
			       LOG_PAGE * log_pgptr,
			       const LOG_LSA * final_lsa)
{
  struct log_gid_bitmap_update gbu;
  int error = NO_ERROR;

  error = cirp_log_get_gid_bitmap_update (&applier->buf_mgr, &gbu,
					  log_pgptr, final_lsa);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (gbu.target == 0)
    {
      error = cci_update_db_group_id (&applier->conn, gbu.migrator_id,
				      gbu.group_id, 1 /* slave */ ,
				      gbu.on_off);
      if (error < 0)
	{
	  REPL_SET_GENERIC_ERROR (error, applier->conn.err_buf.err_msg);
	  GOTO_EXIT_ON_ERROR;
	}
    }

  assert (error == NO_ERROR);
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
 * rp_appl_apply_repl_item () -
 *    return: error code
 *
 *    log_pgptr(in):
 *    log_type(in):
 *    final_lsa(in):
 */
static int
rp_appl_apply_repl_item (CIRP_APPLIER_INFO * applier,
			 LOG_PAGE * log_pgptr, int log_type,
			 LOG_LSA * final_lsa)
{
  int error = NO_ERROR;

  assert (log_type == LOG_REPLICATION_DATA
	  || log_type == LOG_REPLICATION_SCHEMA
	  || log_type == LOG_DUMMY_UPDATE_GID_BITMAP);

  switch (log_type)
    {
    case LOG_REPLICATION_SCHEMA:
      error = rp_appl_apply_schema_item (applier, log_pgptr, final_lsa);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      break;
    case LOG_REPLICATION_DATA:
      error = rp_appl_apply_data_item (applier, log_pgptr, final_lsa);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      break;
    case LOG_DUMMY_UPDATE_GID_BITMAP:
      error = rp_appl_apply_gid_bitmap_item (applier, log_pgptr, final_lsa);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      break;
    default:
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "invalid replication log type");
      GOTO_EXIT_ON_ERROR;
      break;
    }

  assert (error == NO_ERROR);
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
 * cirp_appl_apply_log_record ()
 *    return: error code
 *
 *    commit_lsa(out):
 *    lrec(in):
 *    final_lsa(in):
 *    pg_ptr(in):
 */
static int
cirp_appl_apply_log_record (CIRP_APPLIER_INFO * applier,
			    LOG_LSA * commit_lsa, LOG_RECORD_HEADER * lrec,
			    LOG_LSA final_lsa, LOG_PAGE * pg_ptr)
{
  int error = NO_ERROR;

  assert_release (lrec->type != LOG_END_OF_LOG);

  if (lrec->type == LOG_REPLICATION_DATA
      || lrec->type == LOG_REPLICATION_SCHEMA
      || lrec->type == LOG_DUMMY_UPDATE_GID_BITMAP)
    {
      error = rp_appl_apply_repl_item (applier, pg_ptr, lrec->type,
				       &final_lsa);
    }
  else if (lrec->type == LOG_COMMIT)
    {
      LSA_COPY (commit_lsa, &final_lsa);
    }

  return error;
}

/*
 * cirp_get_applier_data ()
 *   return: error code
 *
 *   log_applier(out):
 */
static int
cirp_get_applier_data (CIRP_APPLIER_INFO * applier,
		       CIRP_CT_LOG_APPLIER * ct_data)
{
  int error = NO_ERROR;

  error = pthread_mutex_lock (&applier->lock);
  if (error != NO_ERROR)
    {
      error = ER_CSS_PTHREAD_MUTEX_LOCK;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

      GOTO_EXIT_ON_ERROR;
    }

  memcpy (ct_data, &applier->ct, sizeof (CIRP_CT_LOG_APPLIER));

  error = pthread_mutex_unlock (&applier->lock);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  assert (error == NO_ERROR);
  return error;

exit_on_error:

  assert (error != NO_ERROR);
  return error;
}

/*
 * cirp_set_applier_data ()
 *   return: error code
 *
 *   log_applier(in):
 */
static int
cirp_set_applier_data (CIRP_APPLIER_INFO * applier,
		       CIRP_CT_LOG_APPLIER * ct_data)
{
  int error = NO_ERROR;

  error = pthread_mutex_lock (&applier->lock);
  if (error != NO_ERROR)
    {
      error = ER_CSS_PTHREAD_MUTEX_LOCK;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

      GOTO_EXIT_ON_ERROR;
    }

  memcpy (&applier->ct, ct_data, sizeof (CIRP_CT_LOG_APPLIER));

  pthread_mutex_unlock (&applier->lock);

  assert (error == NO_ERROR);
  return error;

exit_on_error:

  assert (error != NO_ERROR);
  return error;
}

/*
 * cirp_appl_commit_transaction ()
 *   return: error code
 *
 *   commit_lsa(in):
 */
static int
cirp_appl_commit_transaction (CIRP_APPLIER_INFO * applier,
			      LOG_LSA * commit_lsa)
{
  int error = NO_ERROR;
  CIRP_CT_LOG_APPLIER ct_data;
  LOG_LSA null_lsa;
  CIRP_REPL_ITEM *item;
  RP_CATALOG_ITEM *catalog;
  RECDES *recdes;

  if (LSA_ISNULL (commit_lsa))
    {
      /* nothing to commit */
      return NO_ERROR;
    }

  LSA_SET_NULL (&null_lsa);

  error = cirp_get_applier_data (applier, &ct_data);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  monitor_stats_counter (MNT_RP_APPLIER_BASE_ID + applier->ct.id,
			 MNT_RP_COMMIT, 1);

  error = cirp_applier_update_progress (&ct_data, commit_lsa);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = rp_new_repl_catalog_item (&item, commit_lsa);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  assert (item->item_type == RP_ITEM_TYPE_CATALOG);
  catalog = &item->info.catalog;

  error = rp_assign_recdes_from_pool (&applier->buf_mgr, &recdes);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  assert (recdes != NULL);

  error = rpct_applier_to_catalog_item (catalog, recdes, &ct_data);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  catalog->copyarea_op = LC_FLUSH_HA_CATALOG_APPLIER_UPDATE;
  error = cirp_add_repl_object (applier, item);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* server side auto commit */
  error = cirp_flush_repl_items (applier, true);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = cirp_set_applier_data (applier, &ct_data);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* update boundary so that committed items can be dismissed */
  cirp_applier_clear_committed_item (applier);

  applier->num_uncommitted_tran = 0;

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);
      REPL_SET_GENERIC_ERROR (error, "Invalid error code");
    }

  cci_end_tran (&applier->conn, CCI_TRAN_ROLLBACK);

  return error;
}

/*
 * cirp_applier_update_progress () -
 *   return: error code
 *
 *   committed_lsa(in):
 */
static int
cirp_applier_update_progress (CIRP_CT_LOG_APPLIER * ct_data,
			      LOG_LSA * committed_lsa)
{
  LSA_COPY (&ct_data->committed_lsa, committed_lsa);

  return NO_ERROR;
}

/*
 * cirp_applier_clear_repl_delay () -
 *   return: error code
 */
static int
cirp_applier_clear_repl_delay (CIRP_APPLIER_INFO * applier)
{
  CIRP_CT_LOG_APPLIER ct_data;
  int error = NO_ERROR;

  error = pthread_mutex_lock (&applier->lock);
  if (error != NO_ERROR)
    {
      error = ER_CSS_PTHREAD_MUTEX_LOCK;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

      return error;
    }

  memcpy (&ct_data, &applier->ct, sizeof (CIRP_CT_LOG_APPLIER));

  pthread_mutex_unlock (&applier->lock);

  error = rpct_update_log_applier (&applier->conn, &ct_data);

  /* update boundary so that committed items can be dismissed */
  cirp_applier_clear_committed_item (applier);

  return error;
}

/*
 * cirp_init_applier()-
 *    return: error code
 *
 *    applier(out):
 *    database_name(in):
 *    log_path(in):
 */
int
cirp_init_applier (CIRP_APPLIER_INFO * applier,
		   const char *database_name, const char *log_path)
{
  int error = NO_ERROR;

  applier->status = CIRP_AGENT_INIT;
  applier->analyzer_status = CIRP_AGENT_INIT;

  if (pthread_mutex_init (&applier->lock, NULL) < 0)
    {
      error = ER_CSS_PTHREAD_MUTEX_INIT;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

      GOTO_EXIT_ON_ERROR;
    }
  if (pthread_cond_init (&applier->cond, NULL) < 0)
    {
      error = ER_CSS_PTHREAD_COND_INIT;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      GOTO_EXIT_ON_ERROR;
    }

  error = cirp_logpb_initialize (&applier->buf_mgr, database_name, log_path);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  applier->buf_mgr.db_logpagesize = IO_MAX_PAGE_SIZE;

  applier->head = NULL;
  applier->tail = NULL;
  applier->num_unflushed = 0;
  applier->num_uncommitted_tran = 0;

  cirp_clear_applier (applier);

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  assert (error != NO_ERROR);

  return error;
}

/*
 * cirp_clear_applier()-
 *    return: error code
 *
 *    applier(in/out):
 */
int
cirp_clear_applier (CIRP_APPLIER_INFO * applier)
{
  memset (&applier->conn, 0, sizeof (CCI_CONN));

  memset (&applier->ct, 0, sizeof (CIRP_CT_LOG_APPLIER));

  cirp_free_repl_item_list (applier);
  assert (applier->num_unflushed == 0);

  applier->num_uncommitted_tran = 0;

  memset (&applier->logq, 0, sizeof (CIRP_TRAN_Q));

  return NO_ERROR;
}

/*
 * cirp_final_applier ()-
 *   return: NO_ERROR
 *
 *   applier(in/out):
 */
int
cirp_final_applier (CIRP_APPLIER_INFO * applier)
{
  pthread_mutex_destroy (&applier->lock);
  pthread_cond_destroy (&applier->cond);

  cirp_free_repl_item_list (applier);
  cirp_logpb_final (&applier->buf_mgr);

  return NO_ERROR;
}

static int
rp_applier_wait_start (CIRP_APPLIER_INFO * applier)
{
  int error = NO_ERROR;
  int wakeup_interval = 100;	/* msecs */

  error = pthread_mutex_lock (&applier->lock);
  if (error != NO_ERROR)
    {
      error = ER_CSS_PTHREAD_MUTEX_LOCK;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

      RP_SET_AGENT_NEED_SHUTDOWN ();

      return error;
    }

  while (applier->status == CIRP_AGENT_INIT)
    {
      error = cirp_pthread_cond_timedwait (&applier->cond,
					   &applier->lock, wakeup_interval);
      if (error != NO_ERROR)
	{
	  pthread_mutex_unlock (&applier->lock);

	  return error;
	}
    }

  pthread_mutex_unlock (&applier->lock);

  return NO_ERROR;
}

/*
 * applier_main()-
 *    return: error code
 */
void *
applier_main (void *arg)
{
  int error = NO_ERROR;
  ER_MSG_INFO *th_er_msg_info;
  struct timeval time_commit;
  CIRP_Q_ITEM *repl_log_item;
  CIRP_LOGPB *log_buf = NULL;
  LOG_PAGE *pg_ptr;
  struct log_header *log_hdr = NULL;
  LOG_RECORD_HEADER *lrec = NULL;
  LOG_LSA final_lsa, commit_lsa;
  int applier_index = -1;
  CIRP_APPLIER_INFO *applier = NULL;
  CIRP_BUF_MGR *buf_mgr = NULL;
  CIRP_THREAD_ENTRY *th_entry = NULL;
  char err_msg[ER_MSG_SIZE];

  th_entry = (CIRP_THREAD_ENTRY *) arg;

  applier_index = th_entry->applier_index;

  applier = &Repl_Info->applier_info[applier_index];
  buf_mgr = &applier->buf_mgr;

  th_er_msg_info = malloc (sizeof (ER_MSG_INFO));
  error = er_set_msg_info (th_er_msg_info);
  if (error != NO_ERROR)
    {
      assert (false);
      RP_SET_AGENT_NEED_SHUTDOWN ();
      cirp_change_applier_status (applier, CIRP_AGENT_DEAD);

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
      cirp_change_applier_status (applier, CIRP_AGENT_DEAD);

      free_and_init (th_er_msg_info);
      return NULL;
    }
  pthread_mutex_unlock (&th_entry->th_lock);

  while (REPL_NEED_SHUTDOWN () == false)
    {
      error = rp_applier_wait_start (applier);
      if (error != NO_ERROR)
	{
	  continue;
	}
      /* decache all */
      cirp_logpb_decache_range (&applier->buf_mgr, 0, LOGPAGEID_MAX);

      gettimeofday (&time_commit, NULL);
      LSA_SET_NULL (&commit_lsa);
      LSA_SET_NULL (&final_lsa);

      snprintf (err_msg, sizeof (err_msg),
		"Applier-%d Start: committed_lsa(%lld,%d)", applier_index,
		(long long) applier->ct.committed_lsa.pageid,
		applier->ct.committed_lsa.offset);
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_NOTIFY_MESSAGE, 1,
	      err_msg);

      while (rp_need_restart () == false)
	{
	  assert (LSA_ISNULL (&commit_lsa));

	  error = cirp_applier_item_pop (applier, &repl_log_item);
	  if (error != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	  if (repl_log_item == NULL)
	    {
	      /* queue is empty */

	      /* server side auto commit */
	      error = cirp_applier_clear_repl_delay (applier);
	      if (error != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	      error = cirp_applier_wait_for_queue (applier);
	      if (error != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}

	      continue;
	    }

	  if (LSA_ISNULL (&repl_log_item->tran_start_lsa)
	      || LSA_ISNULL (&repl_log_item->committed_lsa)
	      || LSA_ISNULL (&repl_log_item->repl_start_lsa))
	    {
	      assert (false);

	      REPL_SET_GENERIC_ERROR (error, "Invalid REPL_ITEM("
				      "tran_start(%lld, %lld), committed_lsa(%lld,%lld), "
				      "repl_start_lsa(%lld, %lld)",
				      (long long) repl_log_item->
				      tran_start_lsa.pageid,
				      (long long) repl_log_item->
				      tran_start_lsa.offset,
				      (long long) repl_log_item->
				      committed_lsa.pageid,
				      (long long) repl_log_item->
				      committed_lsa.offset,
				      (long long) repl_log_item->
				      repl_start_lsa.pageid,
				      (long long) repl_log_item->
				      repl_start_lsa.offset);

	      GOTO_EXIT_ON_ERROR;
	    }

	  if (LSA_LE (&repl_log_item->committed_lsa,
		      &applier->ct.committed_lsa))
	    {
	      /* already applied */
	      /* update boundary so that committed items can be dismissed */
	      cirp_applier_clear_committed_item (applier);
	      continue;
	    }

	  LSA_COPY (&final_lsa, &repl_log_item->repl_start_lsa);

	  /* a loop for each transaction */
	  LSA_SET_NULL (&commit_lsa);
	  while (!LSA_ISNULL (&final_lsa) && LSA_ISNULL (&commit_lsa)
		 && rp_need_restart () == false)
	    {
	      /* defense code */
	      cirp_logpb_release_all (buf_mgr, NULL_PAGEID);

	      /* don't move cirp_logpb_act_log_fetch_hdr ()
	       * and another function don't call cirp_logpb_act_log_fetch_hdr() */
	      error = cirp_logpb_act_log_fetch_hdr (buf_mgr);
	      if (error != NO_ERROR)
		{
		  GOTO_EXIT_ON_ERROR;
		}
	      log_hdr = buf_mgr->act_log.log_hdr;

	      error = cirp_logpb_get_page_buffer (buf_mgr, &log_buf,
						  final_lsa.pageid);
	      if (error != NO_ERROR || log_buf == NULL)
		{
		  if (error == NO_ERROR)
		    {
		      assert (false);

		      REPL_SET_GENERIC_ERROR (error, "Invalid return value");
		    }
		  GOTO_EXIT_ON_ERROR;
		}

	      /* a loop for each page */
	      pg_ptr = &(log_buf->log_page);
	      while (final_lsa.pageid == log_buf->pageid
		     && rp_need_restart () == false
		     && LSA_LT (&final_lsa, &log_hdr->eof_lsa))
		{
		  if (final_lsa.offset == 0
		      || final_lsa.offset == NULL_OFFSET)
		    {
		      assert (final_lsa.offset == 0);
		      assert (log_buf->log_page.hdr.offset == 0);

		      final_lsa.offset = log_buf->log_page.hdr.offset;
		    }

		  assert (final_lsa.pageid
			  <= log_hdr->ha_info.last_flushed_pageid);

		  lrec = LOG_GET_LOG_RECORD_HEADER (pg_ptr, &final_lsa);
		  if (!CIRP_IS_VALID_LSA (buf_mgr, &final_lsa)
		      || !CIRP_IS_VALID_LOG_RECORD (buf_mgr, lrec))
		    {
		      assert (false);
		      cirp_logpb_release (buf_mgr, log_buf->pageid);
		      log_buf = NULL;

		      error = ER_LOG_PAGE_CORRUPTED;
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      error, 1, final_lsa.pageid);

		      GOTO_EXIT_ON_ERROR;
		    }

		  if (lrec->trid != repl_log_item->trid)
		    {
		      /* set the next record */
		      LSA_COPY (&final_lsa, &lrec->forw_lsa);
		      continue;
		    }

		  error = cirp_appl_apply_log_record (applier, &commit_lsa,
						      lrec, final_lsa,
						      pg_ptr);
		  if (error == ER_HA_LOG_PAGE_DOESNOT_EXIST)
		    {
		      /*
		       * does not received log page from rye_server
		       * or active log was archived.
		       */
		      break;
		    }
		  else if (error != NO_ERROR)
		    {
		      cirp_logpb_release (buf_mgr, log_buf->pageid);
		      log_buf = NULL;
		      GOTO_EXIT_ON_ERROR;
		    }

		  /* set the next record */
		  LSA_COPY (&final_lsa, &lrec->forw_lsa);

		  if (!LSA_ISNULL (&commit_lsa))
		    {
		      /* end transaction */
		      assert (lrec->type == LOG_COMMIT);

		      applier->num_uncommitted_tran++;
		      break;
		    }
		}		/* end loop for each page */
	      assert (error == NO_ERROR
		      || error == ER_HA_LOG_PAGE_DOESNOT_EXIST);

	      cirp_logpb_release (buf_mgr, log_buf->pageid);
	      log_buf = NULL;
	    }			/* end loop for each transaction */

	  error = cirp_appl_commit_transaction (applier, &commit_lsa);
	  if (error != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	  LSA_SET_NULL (&commit_lsa);

	}			/* end loop */

      /* Fall through */
      assert (error == NO_ERROR);

    exit_on_error:

      snprintf (err_msg, sizeof (err_msg),
		"Applier-%d Retry(ERROR:%d): committed_lsa(%lld,%d)",
		applier_index, error,
		(long long) applier->ct.committed_lsa.pageid,
		applier->ct.committed_lsa.offset);
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_NOTIFY_MESSAGE, 1,
	      err_msg);

      RP_SET_AGENT_NEED_RESTART ();


      /* restart applier */
      cirp_change_applier_status (applier, CIRP_AGENT_INIT);
    }

  RP_SET_AGENT_NEED_SHUTDOWN ();
  cirp_change_applier_status (applier, CIRP_AGENT_DEAD);

  snprintf (err_msg, sizeof (err_msg),
	    "Applier-%d Exit: committed_lsa(%lld,%d)", applier_index,
	    (long long) applier->ct.committed_lsa.pageid,
	    applier->ct.committed_lsa.offset);
  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_NOTIFY_MESSAGE, 1,
	  err_msg);

  free_and_init (th_er_msg_info);

  return NULL;
}

/*
 * cirp_appl_get_committed_lsa()-
 *    return: error code
 *
 *    committed_lsa(out):
 */
int
cirp_appl_get_committed_lsa (CIRP_APPLIER_INFO * applier,
			     LOG_LSA * committed_lsa)
{
  int error = NO_ERROR;

  assert (committed_lsa != NULL);

  error = pthread_mutex_lock (&applier->lock);
  if (error != NO_ERROR)
    {
      error = ER_CSS_PTHREAD_MUTEX_LOCK;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

      return error;
    }

  LSA_COPY (committed_lsa, &applier->ct.committed_lsa);

  pthread_mutex_unlock (&applier->lock);

  assert (error == NO_ERROR);
  return NO_ERROR;
}
