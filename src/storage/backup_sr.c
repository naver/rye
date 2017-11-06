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
 * backup_sr.c - backup module (at server)
 */

#ident "$Id$"

#include "config.h"

#include <stdlib.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <limits.h>
#include <assert.h>
#include <unistd.h>
#include <sys/time.h>

#include "porting.h"
#include "storage_common.h"
#include "file_io.h"
#include "memory_alloc.h"
#include "error_manager.h"
#include "critical_section.h"
#include "system_parameter.h"
#include "databases_file.h"
#include "message_catalog.h"
#include "util_func.h"
#include "perf_monitor.h"
#include "environment_variable.h"
#include "page_buffer.h"
#include "connection_error.h"
#include "release_string.h"
#include "xserver_interface.h"
#include "log_manager.h"
#include "perf_monitor.h"
#include "server_support.h"
#include "connection_error.h"
#include "network_interface_sr.h"
#include "intl_support.h"
#include "backup_sr.h"

static ssize_t bk_read_backup (THREAD_ENTRY * thread_p,
			       BK_BACKUP_SESSION * session, int pageid);
static int bk_compress_backup_node (BK_NODE * node,
				    BK_BACKUP_HEADER * backup_hdr);
static int bk_write_backup_node (THREAD_ENTRY * thread_p,
				 BK_BACKUP_SESSION * session,
				 BK_NODE * node,
				 BK_BACKUP_HEADER * backup_hdr);
static int bk_send_backup (THREAD_ENTRY * thread_p,
			   BK_BACKUP_SESSION * session_p,
			   BK_PACKET_TYPE packet_type,
			   char *buffer_p, int nbytes, int unzip_bytes);
static void bk_finalize_backup_thread (BK_BACKUP_SESSION * session_p);
static int bk_read_backup_volume (THREAD_ENTRY * thread_p,
				  BK_BACKUP_SESSION * session);
static FILEIO_TYPE bk_write_backup_volume (THREAD_ENTRY * thread_p,
					   BK_BACKUP_SESSION * session);
static int bk_logpb_backup_for_volume (THREAD_ENTRY * thread_p, VOLID volid,
				       LOG_LSA * chkpt_lsa,
				       BK_BACKUP_SESSION * session);
static int bk_backup_needed_archive_logs (THREAD_ENTRY * thread_p,
					  BK_BACKUP_SESSION * session,
					  int first_arv_num,
					  int last_arv_num);
static int bk_init_backup_header (BK_BACKUP_HEADER * backup_header_p,
				  LOG_LSA * backup_checkpoint_lsa_p,
				  int do_compress, int make_slave);

/*
 * bk_finalize_backup_thread() -
 *    return: void
 *
 *    session_p(in/out):
 *    zip_method(in):
 */
static void
bk_finalize_backup_thread (BK_BACKUP_SESSION * session_p)
{
  BK_THREAD_INFO *tp;
  BK_QUEUE *qp;
  BK_NODE *node, *node_next;
  int rv;

  tp = &session_p->read_thread_info;
  qp = &tp->io_queue;

  if (tp->initialized == false)
    {
      return;
    }

  rv = pthread_mutex_destroy (&tp->mtx);
  if (rv != 0)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_CSS_PTHREAD_MUTEX_DESTROY, 0);
    }

  rv = pthread_cond_destroy (&tp->rcv);
  if (rv != 0)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_CSS_PTHREAD_COND_DESTROY, 0);
    }

  rv = pthread_cond_destroy (&tp->wcv);
  if (rv != 0)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_CSS_PTHREAD_COND_DESTROY, 0);
    }

  while (qp->size > 0)
    {
      node = bk_delete_queue_head (qp);
      (void) bk_free_node (qp, node);
    }

  for (node = qp->free_list; node; node = node_next)
    {
      node_next = node->next;
      switch (session_p->bkuphdr->zip_method)
	{
	case BK_ZIP_LZO1X_METHOD:
	  if (node->wrkmem != NULL)
	    {
	      free_and_init (node->wrkmem);
	    }

	  if (node->zip_page != NULL)
	    {
	      free_and_init (node->zip_page);
	    }
	  break;
	default:
	  break;
	}

      if (node->area != NULL)
	{
	  free_and_init (node->area);
	}

      if (node != NULL)
	{
	  free_and_init (node);
	}
    }

  qp->free_list = NULL;
  tp->initialized = false;
}

/*
 * bk_compress_backup_node () -
 *   return:
 *   node(in):
 *   backup_hdr(in):
 */
static int
bk_compress_backup_node (BK_NODE * node_p, BK_BACKUP_HEADER * backup_header_p)
{
  int error = NO_ERROR;
  int rv;

  if (!node_p || !backup_header_p)
    {
      goto exit_on_error;
    }

  assert (node_p->nread >= 0);

  switch (backup_header_p->zip_method)
    {
    case BK_ZIP_LZO1X_METHOD:
      if (backup_header_p->zip_level == BK_ZIP_LZO1X_999_LEVEL)
	{
	  /* best reduction */
	  rv = lzo1x_999_compress ((lzo_bytep) node_p->area,
				   (lzo_uint) node_p->nread,
				   node_p->zip_page->buf,
				   &node_p->zip_page->buf_len,
				   node_p->wrkmem);
	}
      else
	{
	  /* best speed */
	  rv = lzo1x_1_compress ((lzo_bytep) node_p->area,
				 (lzo_uint) node_p->nread,
				 node_p->zip_page->buf,
				 &node_p->zip_page->buf_len, node_p->wrkmem);
	}
      if (rv != LZO_E_OK
	  || (node_p->zip_page->buf_len >
	      (size_t) (node_p->nread + node_p->nread / 16 + 64 + 3)))
	{
	  /* this should NEVER happen */
	  error = ER_IO_LZO_COMPRESS_FAIL;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 4,
		  backup_header_p->zip_method,
		  bk_get_zip_method_string (backup_header_p->zip_method),
		  backup_header_p->zip_level,
		  bk_get_zip_level_string (backup_header_p->zip_level));
#if defined(RYE_DEBUG)
	  fprintf (stdout, "internal error - compression failed: %d, "
		   "node->pageid = %d, node->nread = %d, "
		   "node->zip_page->buf_len = %d, "
		   "node->nread + node->nread / 16 + 64 + 3 = %d\n",
		   rv, node_p->pageid, node_p->nread,
		   node_p->zip_page->buf_len,
		   node_p->nread + node_p->nread / 16 + 64 + 3);
#endif /* RYE_DEBUG */
	  goto exit_on_error;
	}

      if (node_p->zip_page->buf_len < (size_t) node_p->nread)
	{
	  /* already write compressed block */
	  ;
	}
      else
	{
	  /* not compressible - write uncompressed block */
	  node_p->zip_page->buf_len = (lzo_uint) node_p->nread;
	  memcpy (node_p->zip_page->buf, node_p->area, node_p->nread);
	}
      break;
    default:
      break;
    }

exit_on_end:

  return error;
exit_on_error:

  if (error == NO_ERROR)
    {
      error = ER_FAILED;
    }
  goto exit_on_end;
}

/*
 * bk_write_backup_node () -
 *   return:
 *   thread_p(in):
 *   session_p(in/out):
 *   node_p(in):
 *   backup_header_p(in):
 */
static int
bk_write_backup_node (THREAD_ENTRY * thread_p, BK_BACKUP_SESSION * session_p,
		      BK_NODE * node_p, BK_BACKUP_HEADER * backup_header_p)
{
  int error = NO_ERROR;
  char *buffer_p;
  int nbytes;

  if (!session_p || !node_p || !backup_header_p)
    {
      goto exit_on_error;
    }

  switch (backup_header_p->zip_method)
    {
    case BK_ZIP_LZO1X_METHOD:
      buffer_p = (char *) node_p->zip_page;
      nbytes = sizeof (lzo_uint) + node_p->zip_page->buf_len;
      break;
    default:
      buffer_p = (char *) node_p->area;
      nbytes = node_p->nread;
      break;
    }

  if (bk_send_backup (thread_p, session_p, BK_PACKET_DATA, buffer_p, nbytes,
		      node_p->nread) != NO_ERROR)
    {
      goto exit_on_error;
    }

exit_on_end:

  return error;

exit_on_error:

  if (error == NO_ERROR)
    {
      error = ER_FAILED;
    }
  goto exit_on_end;
}

/*
 * bk_send_backup () - Write the number of indicated bytes from the dbfile
 *                      area to to the backup destination
 *   return:
 *   session(in/out): The session array
 *   towrite_nbytes(in): Number of bytes that must be written
 */
static int
bk_send_backup (THREAD_ENTRY * thread_p, BK_BACKUP_SESSION * session_p,
		BK_PACKET_TYPE packet_type, char *buffer_p, int nbytes,
		int unzip_bytes)
{
  OR_ALIGNED_BUF (BK_PACKET_HDR_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  char *ptr;
  int error;

  if (nbytes < 1)
    {
      return NO_ERROR;
    }

  assert (session_p->send_buf_size >= nbytes);

  ptr = or_pack_int (reply, packet_type);
  ptr = or_pack_int (ptr, unzip_bytes);

  error = css_send_reply_to_client (thread_p->conn_entry,
				    session_p->rid, 1, reply,
				    OR_ALIGNED_BUF_SIZE (a_reply));
  if (error != NO_ERROR)
    {
      return ER_FAILED;
    }

  /* send the data */
  error = css_send_reply_to_client (thread_p->conn_entry,
				    session_p->rid, 1, buffer_p, nbytes);
  if (error != NO_ERROR)
    {
      return ER_FAILED;
    }

  return NO_ERROR;
}

/*
 * bk_read_backup_volume () -
 *   return:
 *   session(in/out):
 */
static int
bk_read_backup_volume (THREAD_ENTRY * thread_p, BK_BACKUP_SESSION * session_p)
{
  BK_THREAD_INFO *thread_info_p;
  BK_QUEUE *queue_p;
  BK_NODE *node_p = NULL;
  int rv;
  bool need_unlock = false;
  BK_BACKUP_HEADER *backup_header_p;
  BK_BACKUP_PAGE *save_area_p;

  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
      if (thread_p == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
	  return ER_FAILED;
	}
    }

  if (!session_p)
    {
      return ER_FAILED;
    }

  thread_info_p = &session_p->read_thread_info;
  queue_p = &thread_info_p->io_queue;
  /* thread service routine has tran_index_lock,
   * and should release before it is working
   */
  pthread_mutex_unlock (&thread_p->tran_index_lock);
  thread_p->tran_index = thread_info_p->tran_index;
#if defined(RYE_DEBUG)
  fprintf (stdout, "start io_backup_volume_read, session = %p\n", session_p);
#endif /* RYE_DEBUG */
  backup_header_p = session_p->bkuphdr;
  node_p = NULL;		/* init */
  while (1)
    {
      rv = pthread_mutex_lock (&thread_info_p->mtx);
      while (thread_info_p->io_type == FILEIO_WRITE)
	{
	  pthread_cond_wait (&thread_info_p->rcv, &thread_info_p->mtx);
	}

      if (thread_info_p->io_type == FILEIO_ERROR_INTERRUPT)
	{
	  need_unlock = true;
	  node_p = NULL;
	  goto exit_on_error;
	}

      /* get one page from queue head and do write */
      if (node_p)
	{
	  node_p->writeable = true;
	  thread_info_p->io_type = FILEIO_WRITE;
	  pthread_cond_signal (&thread_info_p->wcv);	/* wake up write thread */
	  while (thread_info_p->io_type == FILEIO_WRITE)
	    {
	      pthread_cond_wait (&thread_info_p->rcv, &thread_info_p->mtx);
	    }

	  if (thread_info_p->io_type == FILEIO_ERROR_INTERRUPT)
	    {
	      need_unlock = true;
	      node_p = NULL;
	      goto exit_on_error;
	    }
	}

      /* check EOF */
      if (thread_info_p->pageid >= thread_info_p->from_npages)
	{
	  thread_info_p->end_r_threads++;
	  if (thread_info_p->end_r_threads >= thread_info_p->act_r_threads)
	    {
	      thread_info_p->io_type = FILEIO_WRITE;
	      pthread_cond_signal (&thread_info_p->wcv);	/* wake up write thread */
	    }
	  pthread_mutex_unlock (&thread_info_p->mtx);
	  break;
	}

      /* alloc queue node */
      node_p = bk_allocate_node (queue_p, backup_header_p);
      if (node_p == NULL)
	{
	  thread_info_p->io_type = FILEIO_ERROR_INTERRUPT;
	  need_unlock = true;
	  goto exit_on_error;
	}

      /* read one page from Disk sequentially */

      save_area_p = session_p->dbfile.area;	/* save link */
      session_p->dbfile.area = node_p->area;
      node_p->pageid = thread_info_p->pageid;
      node_p->writeable = false;	/* init */
      node_p->nread = bk_read_backup (thread_p, session_p, node_p->pageid);
      session_p->dbfile.area = save_area_p;	/* restore link */
      if (node_p->nread == -1)
	{
	  thread_info_p->io_type = FILEIO_ERROR_INTERRUPT;
	  need_unlock = true;
	  goto exit_on_error;
	}
      else if (node_p->nread == 0)
	{
	  /* This could be an error since we estimated more pages.
	     End of file/volume. */
	  thread_info_p->io_type = FILEIO_ERROR_INTERRUPT;
	  need_unlock = true;
	  goto exit_on_error;
	}

      /* Have to allow other threads to run and check for interrupts
         from the user (i.e. Ctrl-C ) */
      if ((thread_info_p->pageid % FILEIO_CHECK_FOR_INTERRUPT_INTERVAL) == 0
	  && pgbuf_is_log_check_for_interrupts (thread_p) == true)
	{
#if defined(RYE_DEBUG)
	  fprintf (stdout, "io_backup_volume_read interrupt\n");
#endif /* RYE_DEBUG */
	  thread_info_p->io_type = FILEIO_ERROR_INTERRUPT;
	  need_unlock = true;
	  goto exit_on_error;
	}

      (void) bk_append_queue (queue_p, node_p);

#if defined(RYE_DEBUG)
      fprintf (stdout, "read_thread from_npages = %d, pageid = %d\n",
	       thread_info_p->from_npages, thread_info_p->pageid);
#endif /* RYE_DEBUG */
      thread_info_p->pageid++;
      pthread_mutex_unlock (&thread_info_p->mtx);
      if (node_p)
	{
	  node_p->nread += BK_BACKUP_PAGE_OVERHEAD;

#if defined(RYE_DEBUG)
	  fprintf (stdout, "bk_read_backup_volume: %d\t%d,\t%d\n",
		   ((BK_BACKUP_PAGE *) (node_p->area))->iopageid,
		   *(PAGEID *) (((char *) (node_p->area)) +
				offsetof (BK_BACKUP_PAGE,
					  iopage) +
				backup_header_p->bkpagesize),
		   backup_header_p->bkpagesize);
#endif

	  if (backup_header_p->zip_method != BK_ZIP_NONE_METHOD
	      && bk_compress_backup_node (node_p,
					  backup_header_p) != NO_ERROR)
	    {
	      thread_info_p->io_type = FILEIO_ERROR_INTERRUPT;
	      need_unlock = false;
	      node_p = NULL;
	      goto exit_on_error;
	    }
	}
    }

#if defined(RYE_DEBUG)
  fprintf (stdout, "end io_backup_volume_read\n");
#endif /* RYE_DEBUG */
  return NO_ERROR;
exit_on_error:

  /* set error info */
  if (thread_info_p->errid == NO_ERROR)
    {
      thread_info_p->errid = er_errid ();
    }

  thread_info_p->end_r_threads++;
  if (thread_info_p->end_r_threads >= thread_info_p->act_r_threads)
    {
      pthread_cond_signal (&thread_info_p->wcv);	/* wake up write thread */
    }

  if (need_unlock)
    {
      pthread_mutex_unlock (&thread_info_p->mtx);
    }

  if (node_p != NULL)
    {
      (void) bk_free_node (queue_p, node_p);
    }

#if defined(RYE_DEBUG)
  fprintf (stdout, "end io_backup_volume_read\n");
#endif /* RYE_DEBUG */
  return ER_FAILED;
}

/*
 * bk_write_backup_volume () -
 *   return:
 *   session(in/out):
 */
static FILEIO_TYPE
bk_write_backup_volume (THREAD_ENTRY * thread_p,
			BK_BACKUP_SESSION * session_p)
{
  BK_THREAD_INFO *thread_info_p;
  BK_QUEUE *queue_p;
  BK_NODE *node_p;
  int rv;
  bool need_unlock = false;
  BK_BACKUP_HEADER *backup_header_p;

  if (!session_p)
    {
      return FILEIO_WRITE;
    }

  thread_info_p = &session_p->read_thread_info;
  queue_p = &thread_info_p->io_queue;
#if defined(RYE_DEBUG)
  fprintf (stdout, "start io_backup_volume_write\n");
#endif /* RYE_DEBUG */
  backup_header_p = session_p->bkuphdr;
  rv = pthread_mutex_lock (&thread_info_p->mtx);
  while (1)
    {
      while (thread_info_p->io_type == FILEIO_READ)
	{
	  pthread_cond_wait (&thread_info_p->wcv, &thread_info_p->mtx);
	}

      if (thread_info_p->io_type == FILEIO_ERROR_INTERRUPT)
	{
	  need_unlock = true;
	  goto exit_on_error;
	}

      /* do write */
      while (queue_p->head && queue_p->head->writeable == true)
	{
	  /* delete the head node of the queue */
	  node_p = bk_delete_queue_head (queue_p);
	  if (node_p == NULL)
	    {
	      thread_info_p->io_type = FILEIO_ERROR_INTERRUPT;
	    }
	  else
	    {
	      rv = bk_write_backup_node (thread_p, session_p, node_p,
					 backup_header_p);
	      if (rv != NO_ERROR)
		{
		  thread_info_p->io_type = FILEIO_ERROR_INTERRUPT;
		}
#if defined(RYE_DEBUG)
	      fprintf (stdout,
		       "write_thread node->pageid = %d, node->nread = %d\n",
		       node_p->pageid, node_p->nread);
#endif /* RYE_DEBUG */

	      /* free node */
	      (void) bk_free_node (queue_p, node_p);
	    }

	  if (thread_info_p->io_type == FILEIO_ERROR_INTERRUPT)
	    {
	      need_unlock = true;
	      goto exit_on_error;
	    }
	}

      thread_info_p->io_type = FILEIO_READ;	/* reset */
      /* check EOF */
      if (thread_info_p->end_r_threads >= thread_info_p->act_r_threads)
	{
	  /* only write thread alive */
	  pthread_mutex_unlock (&thread_info_p->mtx);
	  break;
	}

      pthread_cond_broadcast (&thread_info_p->rcv);	/* wake up all read threads */
    }

#if defined(RYE_DEBUG)
  fprintf (stdout, "end io_backup_volume_write\n");
#endif /* RYE_DEBUG */
exit_on_end:

  return thread_info_p->io_type;
exit_on_error:

  /* set error info */
  if (er_errid () == NO_ERROR)
    {
      switch (thread_info_p->errid)
	{
	case ER_INTERRUPTED:
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_INTERRUPTED, 0);
	  break;
	default:		/* give up to handle this case */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LOG_DBBACKUP_FAIL, 1, backup_header_p->db_name);
	  break;
	}
    }

  if (thread_info_p->end_r_threads >= thread_info_p->act_r_threads)
    {
      /* only write thread alive
       * an error (i.e, INTERRUPT) was broken out, so terminate the all threads.
       * But, I am the last one, and all the readers are terminated
       */
      pthread_mutex_unlock (&thread_info_p->mtx);
      goto exit_on_end;
    }

  /* wake up all read threads and wait for all killed */
  pthread_cond_broadcast (&thread_info_p->rcv);
  pthread_cond_wait (&thread_info_p->wcv, &thread_info_p->mtx);
  pthread_mutex_unlock (&thread_info_p->mtx);
  goto exit_on_end;
}

static int
bk_start_backup_thread (THREAD_ENTRY * thread_p,
			BK_BACKUP_SESSION * session_p,
			BK_THREAD_INFO * thread_info_p,
			int from_npages, BK_QUEUE * queue_p)
{
  CSS_CONN_ENTRY *conn_p;
//  int conn_index;
  int i;

  /* Initialize global MT variables */
  thread_info_p->end_r_threads = 0;
  thread_info_p->pageid = 0;
  thread_info_p->from_npages = from_npages;
  thread_info_p->io_type = FILEIO_READ;
  thread_info_p->errid = NO_ERROR;
  thread_info_p->tran_index = logtb_get_current_tran_index (thread_p);
  /* start read threads */
  conn_p = thread_get_current_conn_entry ();
//  conn_index = (conn_p) ? conn_p->idx : 0;
  for (i = 1; i <= thread_info_p->act_r_threads; i++)
    {
      CSS_JOB_ENTRY new_job;

      CSS_JOB_ENTRY_SET (new_job, conn_p,
			 (CSS_THREAD_FN) bk_read_backup_volume,
			 (CSS_THREAD_ARG) session_p);

      if (css_add_to_job_queue (JOB_QUEUE_BACKUP_READ, &new_job) != NO_ERROR)
	{
	  return ER_FAILED;
	}
    }

  /* work as write thread */
  (void) bk_write_backup_volume (thread_p, session_p);
  /* at here, finished all read threads
     check error, interrupt */
  if (thread_info_p->io_type == FILEIO_ERROR_INTERRUPT || queue_p->size != 0)
    {
      return ER_FAILED;
    }

  return NO_ERROR;
}

/*
 * bk_backup_volume_internal () - Include the given database volume/file as part of
 *                       the backup
 *   return:
 *   session(in/out): The session array
 *   from_vlabel(in): Name of the database volume/file to include
 *   from_volid(in): Identifier of the database volume/file to include
 *   last_page(in): stop backing up this volume after this page
 *
 * Note: Information about the database volume/file is recorded, so it
 *       can be recreated (e.g., name and space).
 *       If this is an incremental backup, only pages that have been
 *       updated since the previous backup are backed up, unless a
 *       specific request is given to backup all pages.
 *       Last_page can shorten the number of pages saved (i.e. for
 *       temp volumes, we do not need to backup the entire volume).
 *
 *       1)   The pages are backed up as they are currently stored on disk,
 *            that is, we do not use the page buffer pool for this operation
 *            since we do not want to disturbe the normal access patern of
 *            clients in the page buffer pool.
 *       2)   We open the file/volume instead of using the actual vdes, so
 *            that we avoid a bunch of lseeks.
 */
static int
bk_backup_volume_internal (THREAD_ENTRY * thread_p,
			   BK_BACKUP_SESSION * session_p,
			   const char *from_vol_label_p,
			   VOLID from_vol_id, PAGEID last_page)
{
  struct stat from_stbuf;
  int from_npages, npages;
  int page_id;
  int nread;
  BK_VOL_HEADER_IN_BACKUP *file_header_p;
  BK_THREAD_INFO *thread_info_p;
  BK_QUEUE *queue_p = NULL;
  BK_BACKUP_PAGE *save_area_p;
  BK_NODE *node_p = NULL;
  BK_BACKUP_HEADER *backup_header_p;
  int rv;
  bool is_need_vol_closed;
  OR_ALIGNED_BUF (BK_PACKET_HDR_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  char *ptr;

  /*
   * Backup the pages as they are stored on disk (i.e., don't care if they
   * are stored on the page buffer pool any longer). We do not use the page
   * buffer pool since we do not want to remove important pages that are
   * used by clients.
   * We also open the file/volume instead of using the one currently
   * available since we do not want to be doing a lot of seeks.
   * Remember that we can be preempted.
   */
  session_p->dbfile.vlabel = from_vol_label_p;
  session_p->dbfile.volid = from_vol_id;
  session_p->dbfile.vdes = NULL_VOLDES;
  is_need_vol_closed = false;
  if (from_vol_id == LOG_DBLOG_ACTIVE_VOLID)
    {
      session_p->dbfile.vdes =
	fileio_get_volume_descriptor (LOG_DBLOG_ACTIVE_VOLID);
    }

  if (session_p->dbfile.vdes == NULL_VOLDES)
    {
      session_p->dbfile.vdes =
	fileio_open (session_p->dbfile.vlabel, O_RDONLY, 0);
      if (session_p->dbfile.vdes == NULL_VOLDES)
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_IO_MOUNT_FAIL, 1, session_p->dbfile.vlabel);
	  goto error;
	}
      is_need_vol_closed = true;
    }

  if (fstat (session_p->dbfile.vdes, &from_stbuf) == -1)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_IO_MOUNT_FAIL, 1, session_p->dbfile.vlabel);
      goto error;
    }

  if (S_ISREG (from_stbuf.st_mode))
    {
      /* regular file */
      session_p->dbfile.nbytes = from_stbuf.st_size;
    }
  else
    {
      assert (false);
    }

  /* print the number divided by volume pagesize */
  npages = (int) CEIL_PTVDIV (session_p->dbfile.nbytes, IO_PAGESIZE);
  backup_header_p = session_p->bkuphdr;

  /* set the number divied by backup pagesize */
  if (last_page >= 0 && last_page < npages)
    {
      from_npages = CEIL_PTVDIV ((last_page + 1) * IO_PAGESIZE,
				 backup_header_p->bkpagesize);
    }
  else
    {
      from_npages = (int) CEIL_PTVDIV (session_p->dbfile.nbytes,
				       backup_header_p->bkpagesize);
    }

  /* Write a backup file header which identifies this volume/file on the
     backup.  File headers do not use the extra pageid_copy field. */
  session_p->dbfile.area->iopageid = BK_BACKUP_FILE_START_PAGE_ID;
  file_header_p =
    (BK_VOL_HEADER_IN_BACKUP *) (&session_p->dbfile.area->iopage);
  file_header_p->volid = session_p->dbfile.volid;
  file_header_p->nbytes = session_p->dbfile.nbytes;
  strncpy (file_header_p->vlabel, session_p->dbfile.vlabel, PATH_MAX);
  nread = BK_VOL_HEADER_IN_BACKUP_PAGE_SIZE;

  if (bk_send_backup (thread_p, session_p, BK_PACKET_VOL_START,
		      (char *) session_p->dbfile.area, nread,
		      nread) != NO_ERROR)
    {
      goto error;
    }


#if defined(RYE_DEBUG)
  /* How about adding a backup verbose option ... to print this sort of
   * information as the backup is progressing?  A DBA could later
   * compare the information thus gathered with a restore -t option
   * to verify the integrity of the archive.
   */
  fprintf (stdout,
	   msgcat_message (MSGCAT_CATALOG_RYE, MSGCAT_SET_IO,
			   MSGCAT_FILEIO_BKUP_FILE),
	   file_header_p->vlabel, file_header_p->volid,
	   file_header_p->nbytes,
	   CEIL_PTVDIV (file_header_p->nbytes, IO_PAGESIZE));
  fprintf (stdout, "\n");
#endif /* RYE_DEBUG */

  /* Now start reading each page and writing each page to the backup. */

  thread_info_p = &session_p->read_thread_info;
  queue_p = &thread_info_p->io_queue;
  /* set the number of activated read threads */
  thread_info_p->act_r_threads = MAX (thread_info_p->num_threads - 1, 0);
  thread_info_p->act_r_threads = MIN (thread_info_p->act_r_threads,
				      from_npages);
  thread_info_p->act_r_threads = MIN (thread_info_p->act_r_threads,
				      thread_max_backup_readers ());

#if 1				/* TODO - */
  /* at here, disable multi-thread usage for fast Vol copy */
  assert (thread_info_p->num_threads == 1);
  assert (thread_info_p->act_r_threads == 0);
#endif

  if (thread_info_p->act_r_threads > 0)
    {
#if 1
      assert (false);		/* not permit */
#endif
      if (bk_start_backup_thread (thread_p, session_p, thread_info_p,
				  from_npages, queue_p) != NO_ERROR)
	{
	  goto error;
	}
    }
  else
    {
      for (page_id = 0; page_id < from_npages; page_id++)
	{
	  /* Have to allow other threads to run and check for interrupts
	     from the user (i.e. Ctrl-C ) */
	  if ((page_id % FILEIO_CHECK_FOR_INTERRUPT_INTERVAL) == 0
	      && pgbuf_is_log_check_for_interrupts (thread_p) == true)
	    {
	      goto error;
	    }

	  /* alloc queue node */
	  node_p = bk_allocate_node (queue_p, backup_header_p);
	  if (node_p == NULL)
	    {
	      goto error;
	    }

	  /* read one page sequentially */
	  save_area_p = session_p->dbfile.area;	/* save link */
	  session_p->dbfile.area = node_p->area;
	  node_p->pageid = page_id;
	  node_p->nread = bk_read_backup (thread_p, session_p,
					  node_p->pageid);
	  session_p->dbfile.area = save_area_p;	/* restore link */
	  if (node_p->nread == -1)
	    {
	      goto error;
	    }
	  else if (node_p->nread == 0)
	    {
	      /* This could be an error since we estimated more pages.
	         End of file/volume. */
	      (void) bk_free_node (queue_p, node_p);
	      node_p = NULL;
	      break;
	    }

	  /* Backup the content of this page along with its page
	     identifier */
	  node_p->nread += BK_BACKUP_PAGE_OVERHEAD;

#if defined(RYE_DEBUG)
	  fprintf (stdout, "bk_backup_volume_internal: %d\t%d,\t%d\n",
		   ((BK_BACKUP_PAGE *) (node_p->area))->iopageid,
		   *(PAGEID *) (((char *) (node_p->area)) +
				offsetof (BK_BACKUP_PAGE,
					  iopage) +
				backup_header_p->bkpagesize),
		   backup_header_p->bkpagesize);
#endif

	  if (backup_header_p->zip_method != BK_ZIP_NONE_METHOD
	      && bk_compress_backup_node (node_p,
					  backup_header_p) != NO_ERROR)
	    {
	      goto error;
	    }

	  rv = bk_write_backup_node (thread_p, session_p, node_p,
				     backup_header_p);
	  if (rv != NO_ERROR)
	    {
	      goto error;
	    }

	  /* free node */
	  (void) bk_free_node (queue_p, node_p);
	  node_p = NULL;
	}
    }

  /* End of FILE */

  if (session_p->bkuphdr->make_slave == false)
    {
      /* alloc queue node */
      node_p = bk_allocate_node (queue_p, backup_header_p);
      if (node_p == NULL)
	{
	  goto error;
	}

      node_p->nread = backup_header_p->bkpagesize + BK_BACKUP_PAGE_OVERHEAD;
      memset (&node_p->area->iopage, '\0', backup_header_p->bkpagesize);
      BK_SET_BACKUP_PAGE_ID (node_p->area,
			     BK_BACKUP_FILE_END_PAGE_ID,
			     backup_header_p->bkpagesize);

#if defined(RYE_DEBUG)
      fprintf (stdout, "io_backup_volume: %d\t%d,\t%d\n",
	       ((BK_BACKUP_PAGE *) (node_p->area))->iopageid,
	       *(PAGEID *) (((char *) (node_p->area)) +
			    offsetof (BK_BACKUP_PAGE,
				      iopage) + backup_header_p->bkpagesize),
	       backup_header_p->bkpagesize);
#endif

      if (backup_header_p->zip_method != BK_ZIP_NONE_METHOD
	  && bk_compress_backup_node (node_p, backup_header_p) != NO_ERROR)
	{
	  goto error;
	}

      rv = bk_write_backup_node (thread_p, session_p, node_p,
				 backup_header_p);
      if (rv != NO_ERROR)
	{
	  goto error;
	}

      /* free node */
      (void) bk_free_node (queue_p, node_p);
      node_p = NULL;
    }

  /* Close the database volume/file */
  if (is_need_vol_closed == true)
    {
      fileio_close (session_p->dbfile.vdes);
    }

  session_p->dbfile.vdes = NULL_VOLDES;
  session_p->dbfile.volid = NULL_VOLID;
  session_p->dbfile.nbytes = -1;
  session_p->dbfile.vlabel = NULL;

  ptr = or_pack_int (reply, BK_PACKET_VOL_END);
  ptr = or_pack_int (ptr, 0);	/* dummy unzip_bytes */

  css_send_reply_to_client (thread_p->conn_entry, session_p->rid, 1,
			    reply, OR_ALIGNED_BUF_SIZE (a_reply));

  return NO_ERROR;

error:
  if (is_need_vol_closed == true)
    {
      fileio_close (session_p->dbfile.vdes);
    }

  if (node_p != NULL)
    {
      (void) bk_free_node (queue_p, node_p);
    }

  session_p->dbfile.vdes = NULL_VOLDES;
  session_p->dbfile.volid = NULL_VOLID;
  session_p->dbfile.nbytes = -1;
  session_p->dbfile.vlabel = NULL;
  return ER_FAILED;
}

/*
 * bk_read_backup () - Read a database page from the current database
 *                     volume/file that is backed up
 *   return:
 *   session(in/out): The session array
 *   pageid(in): The page from which we are reading
 *
 * Note: If we run into an end of file, we filled the page with nulls. This is
 *       needed since we write full pages to back up destination. Without this,
 *       we will not be able to know how much to read since not necessarily
 *       the whole volume/file is backed up.
 */
static ssize_t
bk_read_backup (THREAD_ENTRY * thread_p,
		BK_BACKUP_SESSION * session_p, int page_id)
{
  int io_page_size = session_p->bkuphdr->bkpagesize;
  ssize_t nread, nbytes;
  char *buffer_p;

  /* Read until you acumulate io_pagesize or the EOF mark is reached. */
  nread = 0;
  BK_SET_BACKUP_PAGE_ID (session_p->dbfile.area, page_id, io_page_size);

#if defined(RYE_DEBUG)
  fprintf (stdout, "bk_read_backup: %d\t%d,\t%d\n",
	   ((BK_BACKUP_PAGE *) (session_p->dbfile.area))->iopageid,
	   *(PAGEID *) (((char *) (session_p->dbfile.area)) +
			offsetof (BK_BACKUP_PAGE,
				  iopage) + io_page_size), io_page_size);
#endif

  buffer_p = (char *) &session_p->dbfile.area->iopage;
  while (nread < io_page_size)
    {
      /* Read the desired amount of bytes */
      nbytes = pread (session_p->dbfile.vdes, buffer_p, io_page_size - nread,
		      FILEIO_GET_FILE_SIZE (io_page_size, page_id) + nread);
      if (nbytes == -1)
	{
	  if (errno != EINTR)
	    {
	      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
				   ER_IO_READ, 2,
				   BK_GET_BACKUP_PAGE_ID
				   (session_p->dbfile.area),
				   session_p->dbfile.vlabel);
	      return -1;
	    }
	}
      else if (nbytes == 0)
	{
	  if (nread > 0 && session_p->bkuphdr->make_slave == false)
	    {
	      /*
	       * We have a file that it is not multiples of io_pagesize.
	       * We need to add a filler. otherwise, we will not be able to
	       * find other files.
	       */
	      memset (buffer_p, '\0', io_page_size - nread);
	      nread = io_page_size;
	    }
	  break;
	}
      nread += nbytes;
      buffer_p += nbytes;
    }

  /* Backup Thread is reading data/log pages slowly to avoid IO burst */
  if (session_p->dbfile.volid == LOG_DBLOG_ACTIVE_VOLID
      || (session_p->dbfile.volid == LOG_DBLOG_ARCHIVE_VOLID
	  && LOG_CS_OWN_WRITE_MODE (thread_p)))
    {
      ;				/* go ahead */
    }
  else
    {
      int sleep_msecs;

      if (session_p->sleep_msecs > 0)	/* priority 1 */
	{
	  sleep_msecs = session_p->sleep_msecs;
	}
      else if (prm_get_bigint_value (PRM_ID_IO_BACKUP_SLEEP) > 0)	/* priority 2 */
	{
	  sleep_msecs = prm_get_bigint_value (PRM_ID_IO_BACKUP_SLEEP);
	}
      else
	{
	  sleep_msecs = 0;
	}

      if (sleep_msecs > 0)
	{
	  sleep_msecs =
	    (int) (((double) sleep_msecs) / (ONE_M / io_page_size));

	  if (sleep_msecs > 0)
	    {
	      THREAD_SLEEP (sleep_msecs);
	    }
	}
    }

  return nread;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * bk_os_sysconf () -
 *   return:
 */
static int
bk_os_sysconf (void)
{
  long nprocs = -1;

#if defined(_SC_NPROCESSORS_ONLN)
  nprocs = sysconf (_SC_NPROCESSORS_ONLN);
#elif defined(_SC_NPROC_ONLN)
  nprocs = sysconf (_SC_NPROC_ONLN);
#elif defined(_SC_CRAY_NCPU)
  nprocs = sysconf (_SC_CRAY_NCPU);
#else
  ;				/* give up */
#endif
  return (nprocs > 1) ? (int) nprocs : 1;
}
#endif

/*
 * bk_logpb_backup_for_volume - Execute a full backup for the given volume
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   volid(in): The identifier of the volume to backup
 *   chkpt_lsa(in): Checkpoint of the backup process
 *   session(in/out): The session array which is set as a side effect.
 *   only_updated(in):
 *
 * NOTE: The volume with the given volume identifier is backed up on
 *              the given fullname_backup. The backup that is taken is a fuzzy
 *              snapshot of the volume since updates to the volume may be in
 *              progress. Thus, the backup may contain some uncommitted data.
 *              Even worse, the backup is performed using the disk directly.
 *              That is, we copy the pages of the disk without going through
 *              the page buffer pool to avoid disrupting the locality of the
 *              page buffers (e.g., a query executed by another transaction)
 *              For temp volumes, incremental backups are not allowed (because
 *              of logging issues) so always save the system pages of temp
 *              volumes.
 */
static int
bk_logpb_backup_for_volume (THREAD_ENTRY * thread_p, VOLID volid,
			    LOG_LSA * chkpt_lsa, BK_BACKUP_SESSION * session)
{
  DISK_VOLPURPOSE volpurpose;
  PAGEID vol_sys_lastpage;
  int error_code = NO_ERROR;

  /*
   * Determine the purpose of the volume.  For most volumes we need to
   * backup every page, but for temporary volumes we only need the system
   * pages.
   */
  if (xdisk_get_purpose_and_sys_lastpage (thread_p, volid, &volpurpose,
					  &vol_sys_lastpage) == NULL_VOLID)
    {
      error_code = ER_FAILED;
      return error_code;
    }
  else
    {
      vol_sys_lastpage = -1;	/* must backup entire volume */
    }

  /*
   * Reset the checkpoint of the volume to backup and flush all its dirty
   * pages, so that the the backup reflects the actual state of the volume
   * as much as possible
   */
  error_code = disk_set_checkpoint (thread_p, volid, chkpt_lsa);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  LOG_CS_ENTER (thread_p);
  logpb_flush_pages_direct (thread_p);
  LOG_CS_EXIT ();

  error_code = pgbuf_flush_all_unfixed (thread_p, volid);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  /*
   * Create the backup file/volume and copy the content onto it.
   *
   * Note that the copy is done from disk to disk. The page buffer pool is
   * not used. Thus, some more recent version of some copied pages may be
   * present in the buffer pool. We copy the database without using the page
   * buffer pool to avoid disrupting the locality of pages in the page
   * buffer pool and avoid the overhead of calling the page buffer manager.
   */

  error_code =
    bk_backup_volume_internal (thread_p, session,
			       fileio_get_volume_label (volid, PEEK), volid,
			       vol_sys_lastpage);

  return error_code;
}

/*
 * bk_init_backup_header () - Start a backup session
 *   return: error code
 */
static int
bk_init_backup_header (BK_BACKUP_HEADER * backup_header_p,
		       LOG_LSA * backup_checkpoint_lsa_p, int do_compress,
		       int make_slave)
{
  backup_header_p->iopageid = BK_BACKUP_START_PAGE_ID;
  strncpy (backup_header_p->magic, RYE_MAGIC_DATABASE_BACKUP,
	   RYE_MAGIC_MAX_LENGTH);
  strncpy (backup_header_p->db_release, rel_release_string (),
	   REL_MAX_RELEASE_LENGTH);
  strncpy (backup_header_p->db_name,
	   fileio_get_base_file_name (log_Db_fullname), PATH_MAX);
  backup_header_p->db_creation = log_Gl.hdr.db_creation;
  backup_header_p->db_iopagesize = IO_PAGESIZE;
  backup_header_p->db_compatibility = rel_disk_compatible ();

  LSA_COPY (&backup_header_p->chkpt_lsa, backup_checkpoint_lsa_p);

  LSA_SET_NULL (&(backup_header_p->backuptime_lsa));
  backup_header_p->end_time = -1;

  backup_header_p->start_time = time (NULL);
  backup_header_p->bkpagesize = backup_header_p->db_iopagesize;
  backup_header_p->bkpagesize *= FILEIO_FULL_LEVEL_EXP;

  if (do_compress == true)
    {
      if (lzo_init () != LZO_E_OK)
	{
	  goto error;
	}
      backup_header_p->zip_method = BK_ZIP_LZO1X_METHOD;
      backup_header_p->zip_level = BK_ZIP_LZO1X_DEFAULT_LEVEL;
    }
  else
    {
      backup_header_p->zip_method = BK_ZIP_NONE_METHOD;
      backup_header_p->zip_level = BK_ZIP_NONE_LEVEL;
    }

  backup_header_p->make_slave = make_slave;

  return NO_ERROR;

error:
  return ER_FAILED;
}

int
xbk_prepare_backup (THREAD_ENTRY * thread_p, int num_threads,
		    int do_compress,
		    int sleep_msecs, int make_slave,
		    BK_BACKUP_SESSION * session)
{
  LOG_LSA chkpt_lsa;		/* Checkpoint address where the
				 * backup process starts */
  int rv;
  time_t wait_checkpoint_begin_time;

  bool print_backupdb_waiting_reason = false;
  memset (session, 0, sizeof (BK_BACKUP_SESSION));
#if 1
  session->bkup.vdes = NULL_VOLDES;
  session->bkup.vlabel = NULL;
  session->bkup.iosize = -1;
  session->bkup.count = 0;
  session->bkup.voltotalio = 0;
  session->bkup.alltotalio = 0;
  session->bkup.buffer = session->bkup.ptr = NULL;
#endif

  /* Initialization gives us some useful information about the
   * backup location.
   */
  if (bk_init_backup_vol_buffer (session, num_threads, sleep_msecs) !=
      NO_ERROR)
    {
      goto error;
    }

  session->num_perm_vols = xboot_find_number_permanent_volumes (thread_p);

  /*
   * Determine the first log archive that will be needed to insure
   * consistency if we are forced to restore the database with nothing
   * but this backup.
   * first_arv_needed may need to be based on what archive chkpt_lsa is in.
   */
  LOG_CS_ENTER (thread_p);
  if (log_Gl.backup_in_progress == true)
    {
      LOG_CS_EXIT ();
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_LOG_BKUP_DUPLICATE_REQUESTS, 0);
      goto error;
    }

  log_Gl.backup_in_progress = true;
  LOG_CS_EXIT ();

  print_backupdb_waiting_reason = false;
  wait_checkpoint_begin_time = time (NULL);
loop:
  LOG_CS_ENTER (thread_p);
  /* check if checkpoint is in progress */
  if (log_Gl.run_nxchkpt_atpageid == NULL_PAGEID)
    {
      bool continue_check;
      LOG_CS_EXIT ();
      if (print_backupdb_waiting_reason == false)
	{
	  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
		  ERR_CSS_MINFO_MESSAGE, 1,
		  "[ Database backup will start after checkpointing is complete. ]\n\n");
	  print_backupdb_waiting_reason = true;
	}
      /* wait until checkpoint process is finished */

      /* interrupt check */
      if (thread_get_check_interrupt (thread_p) == true)
	{
	  if (logtb_is_interrupted (thread_p, true, &continue_check) == true)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_INTERRUPTED, 0);
	      goto error;
	    }
	}

      thread_sleep (1000);	/* 1000 msec */
      goto loop;
    }

  if (print_backupdb_waiting_reason == true)
    {
      char msg[256];

      sprintf (msg,
	       "[ Database backup has been suspended for %lld seconds due to checkpoint. ]\n\n",
	       (long long int) (time (NULL) - wait_checkpoint_begin_time));
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ERR_CSS_MINFO_MESSAGE,
	      1, msg);
    }

  /* block checkpoint process */
  session->saved_run_nxchkpt_atpageid = log_Gl.run_nxchkpt_atpageid;
  log_Gl.run_nxchkpt_atpageid = NULL_PAGEID;

  if (log_Gl.hdr.last_arv_num_for_syscrashes > -1)
    {
      session->first_arv_needed = log_Gl.hdr.last_arv_num_for_syscrashes;
    }
  else
    {
      session->first_arv_needed = log_Gl.hdr.nxarv_num;
    }

  /* Get the current checkpoint address */
  rv = pthread_mutex_lock (&log_Gl.chkpt_lsa_lock);
  LSA_COPY (&chkpt_lsa, &log_Gl.hdr.chkpt_lsa);
  pthread_mutex_unlock (&log_Gl.chkpt_lsa_lock);

  LOG_CS_EXIT ();

  session->bkuphdr = (BK_BACKUP_HEADER *) malloc (BK_BACKUP_HEADER_IO_SIZE);
  if (session->bkuphdr == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      BK_BACKUP_HEADER_IO_SIZE);

      goto error;
    }

  if (bk_init_backup_header (session->bkuphdr, &chkpt_lsa,
			     do_compress, make_slave) != NO_ERROR)
    {
      goto error;
    }

  logtb_set_backup_session (thread_p, session);

  return NO_ERROR;

error:

  bk_finalize_backup_thread (session);
  bk_abort_backup_server (session);

  if (session != NULL)
    {
      free_and_init (session);
    }
  return ER_FAILED;
}

int
xbk_backup_volume (THREAD_ENTRY * thread_p, int rid, int buf_size)
{
  BK_BACKUP_SESSION *session;
  const char *from_vlabel;	/* Name of volume to backup (FROM) */
  char vol_backup[PATH_MAX];
  int error_code;
  int volid;
  OR_ALIGNED_BUF (BK_PACKET_HDR_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  char *ptr;

  session = logtb_get_backup_session (thread_p);
  session->send_buf_size = buf_size;
  session->rid = rid;

  assert (session->read_thread_info.num_threads == 1);

  /* Backup every volume */
  for (volid = LOG_DBVOLINFO_VOLID; volid < session->num_perm_vols; volid++)
    {
      if (volid == NULL_VOLID)
	{
	  continue;
	}

      switch (volid)
	{
	case LOG_DBVOLINFO_VOLID:
	  fileio_make_volume_info_name (vol_backup, log_Db_fullname);
	  from_vlabel = vol_backup;
	  break;
	case LOG_DBLOG_INFO_VOLID:
	case LOG_DBLOG_BKUPINFO_VOLID:
	  /*
	   * These information volumes are backed-up at the very end.
	   */
	  continue;
	case LOG_DBLOG_ACTIVE_VOLID:
	  /*
	   * Archiving log active must be done after all data volumes
	   * have been backed up (in order to insure we get all of the
	   * log records needed to restore consistency).
	   */
	  continue;
	default:
	  from_vlabel = fileio_get_volume_label (volid, PEEK);
	  break;
	}

      if (volid >= LOG_DBFIRST_VOLID)
	{
	  error_code =
	    bk_logpb_backup_for_volume (thread_p, volid,
					&session->bkuphdr->chkpt_lsa,
					session);
	  if (error_code != NO_ERROR)
	    {
	      goto error;
	    }
	}
      else
	{
	  error_code =
	    bk_backup_volume_internal (thread_p, session, from_vlabel, volid,
				       -1);
	  if (error_code != NO_ERROR)
	    {
	      goto error;
	    }
	}
    }

  ptr = or_pack_int (reply, BK_PACKET_VOLS_BACKUP_END);
  ptr = or_pack_int (ptr, 0);	/* dummy unzip_bytes */

  css_send_reply_to_client (thread_p->conn_entry, session->rid, 1,
			    reply, OR_ALIGNED_BUF_SIZE (a_reply));

  return NO_ERROR;

error:

  bk_finalize_backup_thread (session);
  bk_abort_backup_server (session);

  return ER_FAILED;
}

int
xbk_backup_log_volume (THREAD_ENTRY * thread_p, int rid, int buf_size,
		       int delete_unneeded_logarchives)
{
  BK_BACKUP_SESSION *session;
  int last_arv_needed = -1;	/* backups, some arv are needed   */
  const char *catmsg;
  int error_code;
  OR_ALIGNED_BUF (BK_PACKET_HDR_SIZE) a_reply;
  char *reply = OR_ALIGNED_BUF_START (a_reply);
  char *ptr;
  int rv;
  bool continue_check;

  session = logtb_get_backup_session (thread_p);
  session->send_buf_size = buf_size;
  session->rid = rid;

  assert (session->read_thread_info.num_threads == 1);

  /*
   * Only when in client/server, we may need to force an archive
   * of the current log active if there were any active transactions
   * before or during the backup.
   * This is to insure we have enough log records to restore consistency
   * to the database in the event a restore with no other log archives
   * is needed.
   */
  LOG_CS_ENTER (thread_p);
  if (LSA_LT (&session->bkuphdr->chkpt_lsa, &log_Gl.hdr.append_lsa)
      || log_Gl.hdr.append_lsa.pageid > log_Gl.hdr.nxarv_pageid)
    {
      logpb_archive_active_log (thread_p);
    }

  last_arv_needed = log_Gl.hdr.nxarv_num - 1;
  LOG_CS_EXIT ();

  if (last_arv_needed >= session->first_arv_needed)
    {
      error_code = bk_backup_needed_archive_logs (thread_p, session,
						  session->first_arv_needed,
						  last_arv_needed);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}
    }

  /* acquire backuptime_lsa
   */
  assert (LSA_ISNULL (&(session->bkuphdr->backuptime_lsa)));
  assert (session->bkuphdr->end_time == -1);

  while (true)
    {
      TR_TABLE_LOCK (thread_p);

      /* wait for all working TXs */
      if (xlogtb_exist_working_tran (thread_p, NULL_GROUPID) == false)
	{
	  rv = pthread_mutex_lock (&log_Gl.prior_info.prior_lsa_mutex);

	  LSA_COPY (&(session->bkuphdr->backuptime_lsa),
		    &(log_Gl.prior_info.prior_lsa));
	  assert (!LSA_ISNULL (&(session->bkuphdr->backuptime_lsa)));

	  pthread_mutex_unlock (&log_Gl.prior_info.prior_lsa_mutex);

	  TR_TABLE_UNLOCK (thread_p);
	  break;
	}

      TR_TABLE_UNLOCK (thread_p);
      thread_sleep (10);

      /* interrupt check */
      if (thread_get_check_interrupt (thread_p) == true)
	{
	  if (logtb_is_interrupted (thread_p, true, &continue_check) == true)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_INTERRUPTED, 0);
	      goto error;
	    }
	}
    }

  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_LOG_BACKUP_CS_ENTER,
	  1, log_Name_active);

  LOG_CS_ENTER (thread_p);

  /* flush to acquired backuptime_lsa
   */
  logpb_flush_pages_direct (thread_p);

  /* backup the archive logs created during backup existing archive logs */
  while (last_arv_needed < log_Gl.hdr.nxarv_num - 1)
    {
      LOG_CS_EXIT ();

      error_code = bk_backup_needed_archive_logs (thread_p, session,
						  last_arv_needed + 1,
						  log_Gl.hdr.nxarv_num - 1);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}

      LOG_CS_ENTER (thread_p);

      last_arv_needed = log_Gl.hdr.nxarv_num - 1;
    }

  assert (LOG_CS_OWN (thread_p));

  if (fileio_is_volume_exist (log_Name_info) == true)
    {
      error_code =
	bk_backup_volume_internal (thread_p, session, log_Name_info,
				   LOG_DBLOG_INFO_VOLID, -1);
      if (error_code != NO_ERROR)
	{
	  LOG_CS_EXIT ();
	  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
		  ER_LOG_BACKUP_CS_EXIT, 1, log_Name_active);
	  goto error;
	}
    }

  LSA_COPY (&log_Gl.hdr.bkup_level_lsa, &session->bkuphdr->chkpt_lsa);

  /* Now indicate how many volumes were backed up */
  logpb_flush_header (thread_p);

  error_code =
    bk_backup_volume_internal (thread_p, session, log_Name_active,
			       LOG_DBLOG_ACTIVE_VOLID, -1);
  if (error_code != NO_ERROR)
    {
      LOG_CS_EXIT ();
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
	      ER_LOG_BACKUP_CS_EXIT, 1, log_Name_active);
      goto error;
    }

  if (delete_unneeded_logarchives != false)
    {
      catmsg = msgcat_message (MSGCAT_CATALOG_RYE,
			       MSGCAT_SET_LOG,
			       MSGCAT_LOG_DATABASE_BACKUP_WAS_TAKEN);
      assert (catmsg != NULL);

      if (catmsg)
	{
	  logpb_remove_archive_logs (thread_p, catmsg);
	}
    }

  LOG_CS_EXIT ();

  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_LOG_BACKUP_CS_EXIT,
	  1, log_Name_active);

  session->bkuphdr->end_time = (INT64) time (NULL);
  assert (session->bkuphdr->end_time > 0);

  ptr = or_pack_int (reply, BK_PACKET_LOGS_BACKUP_END);
  ptr = or_pack_int (ptr, 0);	/* dummy unzip_bytes */

  ptr = or_pack_log_lsa (ptr, &(session->bkuphdr->backuptime_lsa));
  ptr = PTR_ALIGN (ptr, MAX_ALIGNMENT);
  ptr = or_pack_int64 (ptr, session->bkuphdr->end_time);

  css_send_reply_to_client (thread_p->conn_entry, session->rid, 1,
			    reply, OR_ALIGNED_BUF_SIZE (a_reply));

  bk_finalize_backup_thread (session);
  bk_abort_backup_server (session);

  return NO_ERROR;

error:

  bk_abort_backup_server (session);

  return ER_FAILED;
}

/*
 * bk_backup_needed_archive_logs - Backup active log archives
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   session(in): The session array which is set as a side effect.
 *   first_arv_num(in): last arv num to archive (inclusive)
 *   last_arv_num(in): last arv num to archive (inclusive)
 *
 * NOTE: Determine which active log records will be required to fully restore
 *   this backup in the event recovery is needed.  This probably includes
 *   the active log archives as well as at least some portion of the
 *   log active.  Force a log archive of the active log, to insure that we
 *   have the necessary log records to restore if this backup is "fuzzy".
 */
static int
bk_backup_needed_archive_logs (THREAD_ENTRY * thread_p,
			       BK_BACKUP_SESSION * session,
			       int first_arv_num, int last_arv_num)
{
  int i;
  char logarv_name[PATH_MAX];	/* Archive name               */
  int error_code = NO_ERROR;

//  assert (!LOG_CS_OWN (thread_p));

  for (i = first_arv_num;
       i >= 0 && i <= last_arv_num && error_code == NO_ERROR; i++)
    {
      /* Backup this archive volume */
      fileio_make_log_archive_name (logarv_name, log_Archive_path, log_Prefix,
				    i);

      error_code = bk_backup_volume_internal (thread_p, session, logarv_name,
					      LOG_DBLOG_ARCHIVE_VOLID, -1);
    }

  return error_code;
}
