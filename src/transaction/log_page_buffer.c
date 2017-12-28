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
#include "db.h"			/* for db_Connect_status */
#include "log_compress.h"
#include "event_log.h"
#include "rye_server_shm.h"

#if !defined(SERVER_MODE)
#define pthread_mutex_init(a, b)
#define pthread_mutex_destroy(a)
#define pthread_mutex_lock(a)	0
#define pthread_mutex_unlock(a)
static int rv;
#undef  COND_INIT
#define COND_INIT(a)
#undef  COND_BROADCAST
#define COND_BROADCAST(a)
#undef  COND_DESTROY
#define COND_DESTROY(a)
#endif /* !SERVER_MODE */

#define LOGPB_FIND_BUFPTR(bufid) log_Pb.pool[(bufid)]
#define LOGPB_FIND_NBUFFER_FROM_CONT_BUFFERS(setbufs, nbuf) \
  ((struct log_buffer *) ((SIZEOF_LOG_BUFFER * (nbuf)) + (char *)(setbufs)))

/* PAGES OF ACTIVE LOG PORTION */
#define LOGPB_HEADER_PAGE_ID             (-9)	/* The first log page in the infinite
						   log sequence. It is always kept
						   on the active portion of the log.
						   Log records are not stored on this
						   page. This page is backed up in
						   all archive logs
						 */
#define LOGPB_NEXT_ARCHIVE_PAGE_ID    (log_Gl.hdr.nxarv_pageid)
#define LOGPB_FIRST_ACTIVE_PAGE_ID    (log_Gl.hdr.fpageid)
#define LOGPB_LAST_ACTIVE_PAGE_ID     (log_Gl.hdr.nxarv_pageid +                 \
                                    LOGPB_ACTIVE_NPAGES - 1)


/*
 * TRANSLATING LOGICAL LOG PAGES (I.E., PAGES IN THE INFINITE LOG) TO PHYSICAL
 * PAGES IN THE CURRENT LOG FILE
 */
#define LOGPB_PHYSICAL_HEADER_PAGE_ID    0

#define LOGPB_IS_FIRST_PHYSICAL_PAGE(pageid) (logpb_to_physical_pageid(pageid) == 1)

/* ARCHIVE LOG PAGES */
#define LOGPB_IS_ARCHIVE_PAGE(pageid) \
  ((pageid) != LOGPB_HEADER_PAGE_ID && (pageid) < LOGPB_NEXT_ARCHIVE_PAGE_ID)
#define LOGPB_AT_NEXT_ARCHIVE_PAGE_ID(pageid)  \
  (logpb_to_physical_pageid(pageid) == log_Gl.hdr.nxarv_phy_pageid)

#define ARV_PAGE_INFO_TABLE_SIZE    256

#define LOG_LAST_APPEND_PTR() ((char *)log_Gl.append.log_pgptr->area +        \
                               LOGAREA_SIZE)

#define LOG_APPEND_ALIGN(thread_p, current_setdirty)                                    \
  do {                                                                        \
    if ((current_setdirty) == LOG_SET_DIRTY)                                  \
      logpb_set_dirty((thread_p), log_Gl.append.log_pgptr, DONT_FREE);        \
    log_Gl.hdr.append_lsa.offset = DB_ALIGN(log_Gl.hdr.append_lsa.offset, DOUBLE_ALIGNMENT);                    \
    if (log_Gl.hdr.append_lsa.offset >= (int)LOGAREA_SIZE)                    \
      logpb_next_append_page((thread_p), LOG_DONT_SET_DIRTY);                               \
  } while(0)

#define LOG_APPEND_ADVANCE_WHEN_DOESNOT_FIT(thread_p, length)                           \
  do {                                                                        \
    if (log_Gl.hdr.append_lsa.offset + (int)(length) >= (int)LOGAREA_SIZE)    \
      logpb_next_append_page((thread_p), LOG_DONT_SET_DIRTY);                               \
  } while(0)

#define LOG_APPEND_SETDIRTY_ADD_ALIGN(thread_p, add)                                    \
  do {                                                                        \
    log_Gl.hdr.append_lsa.offset += (add);                                    \
    LOG_APPEND_ALIGN((thread_p), LOG_SET_DIRTY);                                          \
  } while(0)

#define LOG_PREV_APPEND_PTR()                                                 \
  ((log_Gl.append.delayed_free_log_pgptr != NULL)                             \
   ? ((char *)log_Gl.append.delayed_free_log_pgptr->area +                    \
      log_Gl.append.prev_lsa.offset)                                          \
   : ((char *)log_Gl.append.log_pgptr->area +                                 \
      log_Gl.append.prev_lsa.offset))


/* LOG BUFFER STRUCTURE */

/* WARNING:
 * Don't use sizeof(struct log_buffer) or of any structure that contains it
 * Use macro SIZEOF_LOG_BUFFER instead.
 * It is also bad idea to create a variable for this on the stack.
 */
typedef struct log_buffer LOG_BUFFER;
struct log_buffer
{
  LOG_PAGEID pageid;		/* Logical page of the log. (Page identifier of
				 * the infinite log)
				 */
  LOG_PHY_PAGEID phy_pageid;	/* Physical pageid for the active log portion */
  int fcnt;			/* Fix count */
  int ipool;			/* Buffer pool index. Used to optimize the Clock
				 * algorithm and to find the address of buffer given
				 * the page address
				 */
  bool dirty;			/* Is page dirty */
  bool recently_freed;		/* Reference value 0/1 used by the clock
				 * algorithm
				 */
  bool flush_running;		/* Is page beging flushed ? */

  bool dummy_for_align;		/* Dummy field for 8byte alignment of log page */
  LOG_PAGE logpage;		/* The actual buffered log page */
};

struct log_bufarea
{				/* A buffer area */
  struct log_buffer *bufarea;
  struct log_bufarea *next;
};

/* callback function for scan pages to flush */
typedef void (*log_buffer_apply_func) (struct log_buffer * bufptr);

typedef struct arv_page_info
{
  int arv_num;
  LOG_PAGEID start_pageid;
  LOG_PAGEID end_pageid;
} ARV_PAGE_INFO;

typedef struct
{
  ARV_PAGE_INFO page_info[ARV_PAGE_INFO_TABLE_SIZE];
  int rear;
  int item_count;
} ARV_LOG_PAGE_INFO_TABLE;

#define SIZEOF_LOG_BUFFER  (LOG_PAGESIZE + offsetof(struct log_buffer, logpage))

#define LOG_GET_LOG_BUFFER_PTR(log_pgptr)                       \
  ((struct log_buffer *) ((char *)(log_pgptr) - offsetof(struct log_buffer, logpage)))

static const int LOG_BKUP_HASH_NUM_PAGEIDS = 1000;
/* MIN AND MAX BUFFERS */
#define LOG_MAX_NUM_CONTIGUOUS_BUFFERS \
  ((unsigned int)(INT_MAX / (5 * SIZEOF_LOG_BUFFER)))

#define LOG_MAX_LOGINFO_LINE (PATH_MAX * 4)

/* skip prompting for archive log location */
#if defined(SERVER_MODE)
int log_default_input_for_archive_log_location = 0;
#else
int log_default_input_for_archive_log_location = -1;
#endif

LOG_PB_GLOBAL_DATA log_Pb = {
  false, NULL, NULL, NULL, 0, 0
#if !defined(SERVER_MODE)
    , NULL, NULL, NULL, 0
#endif /* !SERVER_MODE */
};

LOG_LOGGING_STAT log_Stat;
static ARV_LOG_PAGE_INFO_TABLE logpb_Arv_page_info_table;

LOG_LSA NULL_LSA = { NULL_PAGEID, NULL_OFFSET };

/*
 * Functions
 */

static int logpb_expand_pool (int num_new_buffers);
static struct log_buffer *logpb_replace (THREAD_ENTRY * thread_p,
					 bool * retry);
static LOG_PAGE *logpb_fix_page (THREAD_ENTRY * thread_p, LOG_PAGEID pageid,
				 int fetch_mode);
static bool logpb_is_dirty (THREAD_ENTRY * thread_p, LOG_PAGE * log_pgptr);
#if !defined(NDEBUG)
static bool logpb_is_any_dirty (void);
#endif /* !NDEBUG */
static void logpb_dump_information (FILE * out_fp);
static void logpb_dump_to_flush_page (FILE * out_fp);
static void logpb_dump_pages (FILE * out_fp);
static LOG_PAGE **logpb_writev_append_pages (THREAD_ENTRY * thread_p,
					     LOG_PAGE ** to_flush,
					     DKNPAGES npages);
static int logpb_get_guess_archive_num (THREAD_ENTRY * thread_p,
					LOG_PAGEID pageid);
static void logpb_set_unavailable_archive (int arv_num);
static bool logpb_is_archive_available (int arv_num);
#if defined (ENABLE_UNUSED_FUNCTION)
static int logpb_get_remove_archive_num (THREAD_ENTRY * thread_p,
					 LOG_PAGEID safe_pageid,
					 int archive_num);
#endif
static int
logpb_remove_archive_logs_internal (THREAD_ENTRY * thread_p,
				    int first, int last,
				    const char *info_reason);
static void logpb_append_archives_removed_to_log_info (int first, int last,
						       const char
						       *info_reason);
static int logpb_verify_length (const char *db_fullname, const char *log_path,
				const char *log_prefix);
#if defined (ENABLE_UNUSED_FUNCTION)
static int logpb_start_where_path (const char *to_db_fullname);
static int logpb_next_where_path (const char *to_db_fullname,
				  int num_perm_vols,
				  VOLID volid, char *from_volname,
				  char *to_volname);
#endif
static bool logpb_check_if_exists (const char *fname, char *first_vol);
#if 0
static int logpb_must_archive_last_log_page (THREAD_ENTRY * thread_p);
#endif
static int logpb_initialize_flush_info (void);
static void logpb_finalize_flush_info (void);
static void logpb_finalize_writer_info (void);
static void logpb_reset_clock_hand (int buffer_index);
static void logpb_move_next_clock_hand (void);
static void logpb_unfix_page (struct log_buffer *bufptr);
static void logpb_initialize_log_buffer (LOG_BUFFER * log_buffer_p);

static void logpb_write_toflush_pages_to_archive (THREAD_ENTRY * thread_p);
static int logpb_add_archive_page_info (THREAD_ENTRY * thread_p,
					int arv_num, LOG_PAGEID start_page,
					LOG_PAGEID end_page);
static int logpb_get_archive_num_from_info_table (THREAD_ENTRY * thread_p,
						  LOG_PAGEID page_id);

static int logpb_flush_all_append_pages (THREAD_ENTRY * thread_p);
static int logpb_append_next_record (THREAD_ENTRY * thread_p,
				     LOG_PRIOR_NODE * ndoe);

static void logpb_start_append (THREAD_ENTRY * thread_p,
				LOG_RECORD_HEADER * header);
static void logpb_end_append (THREAD_ENTRY * thread_p,
			      LOG_RECORD_HEADER * header);
static void logpb_append_data (THREAD_ENTRY * thread_p, int length,
			       const char *data);
static void logpb_next_append_page (THREAD_ENTRY * thread_p,
				    LOG_SETDIRTY current_setdirty);
static LOG_PRIOR_NODE *prior_lsa_remove_prior_list (THREAD_ENTRY * thread_p);
static int logpb_append_prior_lsa_list (THREAD_ENTRY * thread_p,
					LOG_PRIOR_NODE * list);
static LOG_PAGE *logpb_copy_page (THREAD_ENTRY * thread_p,
				  LOG_PAGEID pageid, LOG_PAGE * log_pgptr);

static void logpb_fatal_error_internal (THREAD_ENTRY * thread_p,
					bool log_exit, bool need_flush,
					const char *file_name,
					const int lineno, const char *fmt,
					va_list ap);

static void logpb_set_nxio_lsa (LOG_LSA * lsa);

/*
 * FUNCTIONS RELATED TO LOG BUFFERING
 *
 */

/*
 * logpb_reset_clock_hand - reset clock hand of log page buffer
 *
 * return: nothing
 *
 * NOTE:
 *
 */
static void
logpb_reset_clock_hand (int buffer_index)
{
  if (buffer_index >= log_Pb.num_buffers || buffer_index < 0)
    {
      log_Pb.clock_hand = 0;
    }
  else
    {
      log_Pb.clock_hand = buffer_index;
    }
}

/*
 * logpb_move_next_clock_hand - move next clock hand of log page buffer
 *
 * return: nothing
 *
 * NOTE:
 *
 */
static void
logpb_move_next_clock_hand (void)
{
  log_Pb.clock_hand++;
  if (log_Pb.clock_hand >= log_Pb.num_buffers)
    {
      log_Pb.clock_hand = 0;
    }
}

/*
 * logpb_unfix_page - unfix of log page buffer
 *
 * return: nothing
 *
 *   bufptr(in/oiut):
 *
 * NOTE:
 *
 */
static void
logpb_unfix_page (struct log_buffer *bufptr)
{
  bufptr->fcnt--;
  if (bufptr->fcnt < 0)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOG_FREEING_TOO_MUCH, 0);
      bufptr->fcnt = 0;
    }

  bufptr->recently_freed = true;
}

/*
 * logpb_initialize_log_buffer -
 *
 * return: nothing
 *
 *   log_buffer_p(in/oiut):
 *
 * NOTE:
 *
 */
static void
logpb_initialize_log_buffer (LOG_BUFFER * log_buffer_p)
{
  log_buffer_p->pageid = NULL_PAGEID;
  log_buffer_p->phy_pageid = NULL_PAGEID;
  log_buffer_p->fcnt = 0;
  log_buffer_p->recently_freed = false;
  log_buffer_p->dirty = false;
  log_buffer_p->flush_running = false;
  /*
   * Scramble the content of buffers. This is done for debugging
   * reasons to make sure that a user of a buffer does not assume
   * that buffers are initialized to zero. For safty reasons, the
   * buffers are initialized to zero, instead of scrambled, when
   * not in debugging mode.
   */
  MEM_REGION_INIT (&log_buffer_p->logpage, LOG_PAGESIZE);
  log_buffer_p->logpage.hdr.logical_pageid = NULL_PAGEID;
  log_buffer_p->logpage.hdr.offset = NULL_OFFSET;
}

/*
 * logpb_expand_pool - Expand log buffer pool
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 *   num_new_buffers(in): Number of new buffers (expansion) or -1.
 *
 * NOTE:Expand the log buffer pool with the given number of buffers.
 *              If a zero or a negative value is given, the function expands
 *              the buffer pool with a default porcentage of the currently
 *              size.
 */
static int
logpb_expand_pool (int num_new_buffers)
{
  struct log_buffer *log_bufptr;	/* Pointer to array of buffers */
  struct log_bufarea *area;	/* Contiguous area for buffers */
  int bufid, i;			/* Buffer index                */
  int total_buffers;		/* Total num buffers           */
  int size;			/* Size of area to allocate    */
  float expand_rate;
  struct log_buffer **buffer_pool;
  int error_code = NO_ERROR;

  assert (LOG_CS_OWN_WRITE_MODE (NULL));

  csect_enter (NULL, CSECT_LOG_BUFFER, INF_WAIT);

  if (num_new_buffers <= 0)
    {
      /*
       * Calculate a default expansion for the buffer pool.
       */
      if (log_Pb.num_buffers > 0)
	{
	  if (log_Pb.num_buffers > 100)
	    {
	      expand_rate = 0.10f;
	    }
	  else
	    {
	      expand_rate = 0.20f;
	    }
	  num_new_buffers = (int) (((float) log_Pb.num_buffers * expand_rate)
				   + 0.9);
	}
      else
	{
	  num_new_buffers =
	    prm_get_bigint_value (PRM_ID_LOG_BUFFER_SIZE) / LOG_PAGESIZE;
	}
    }

  while ((unsigned int) num_new_buffers > LOG_MAX_NUM_CONTIGUOUS_BUFFERS)
    {
      /* Note that we control overflow of size in this way */
      csect_exit (CSECT_LOG_BUFFER);
      error_code = logpb_expand_pool (LOG_MAX_NUM_CONTIGUOUS_BUFFERS);
      if (error_code != NO_ERROR)
	{
	  return error_code;
	}

      csect_enter (NULL, CSECT_LOG_BUFFER, INF_WAIT);

      num_new_buffers -= LOG_MAX_NUM_CONTIGUOUS_BUFFERS;
    }

  if (num_new_buffers > 0)
    {
      total_buffers = log_Pb.num_buffers + num_new_buffers;

      /*
       * Allocate an area for the buffers, set the address of each buffer
       * and keep the address of the buffer area for deallocation purposes at a
       * later time.
       */
      size = ((num_new_buffers * SIZEOF_LOG_BUFFER)
	      + sizeof (struct log_bufarea));

      area = (struct log_bufarea *) malloc (size);
      if (area == NULL)
	{
	  csect_exit (CSECT_LOG_BUFFER);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, size);
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      /* allocate a pointer array to point to each buffer */
      buffer_pool = (struct log_buffer **) realloc (log_Pb.pool,
						    total_buffers *
						    sizeof (*log_Pb.pool));
      if (buffer_pool == NULL)
	{
	  free_and_init (area);

	  csect_exit (CSECT_LOG_BUFFER);

	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, total_buffers * sizeof (*log_Pb.pool));
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      memset (area, 1, size);

      area->bufarea = ((struct log_buffer *) ((char *) area +
					      sizeof (struct log_bufarea)));
      area->next = log_Pb.poolarea;

      /* Initialize every new buffer */
      for (i = 0, bufid = log_Pb.num_buffers; i < num_new_buffers;
	   bufid++, i++)
	{
	  log_bufptr = LOGPB_FIND_NBUFFER_FROM_CONT_BUFFERS (area->bufarea,
							     i);
	  buffer_pool[bufid] = log_bufptr;
	  logpb_initialize_log_buffer (log_bufptr);
	  log_bufptr->ipool = bufid;
	}

      log_Pb.pool = buffer_pool;
      logpb_reset_clock_hand (log_Pb.num_buffers);
      log_Pb.poolarea = area;
      log_Pb.num_buffers = total_buffers;
    }

  csect_exit (CSECT_LOG_BUFFER);

  log_Stat.log_buffer_expand_count++;

  return NO_ERROR;
}

/*
 * logpb_initialize_pool - Initialize the log buffer pool
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 * NOTE:Initialize the log buffer pool. All resident pages are
 *              invalidated.
 */
int
logpb_initialize_pool (THREAD_ENTRY * thread_p)
{
  int error_code = NO_ERROR;
  LOG_GROUP_COMMIT_INFO *group_commit_info = &log_Gl.group_commit_info;
  LOGWR_INFO *writer_info = &log_Gl.writer_info;

  if (lzo_init () == LZO_E_OK)
    {				/* lzo library init */
      if (!prm_get_bool_value (PRM_ID_LOG_COMPRESS))
	{
	  log_Pb.log_zip_support = false;
	}
      else
	{
#if defined(SERVER_MODE)
	  log_Pb.log_zip_support = true;
#else
	  log_Pb.log_zip_undo = log_zip_alloc (IO_PAGESIZE, true);
	  log_Pb.log_zip_redo = log_zip_alloc (IO_PAGESIZE, true);
	  log_Pb.log_data_length = IO_PAGESIZE * 2;
	  log_Pb.log_data_ptr = (char *) malloc (log_Pb.log_data_length);
	  if (log_Pb.log_data_ptr == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, log_Pb.log_data_length);
	    }

	  if (log_Pb.log_zip_undo == NULL || log_Pb.log_zip_redo == NULL
	      || log_Pb.log_data_ptr == NULL)
	    {
	      log_Pb.log_zip_support = false;
	      if (log_Pb.log_zip_undo)
		{
		  log_zip_free (log_Pb.log_zip_undo);
		  log_Pb.log_zip_undo = NULL;
		}
	      if (log_Pb.log_zip_redo)
		{
		  log_zip_free (log_Pb.log_zip_redo);
		  log_Pb.log_zip_redo = NULL;
		}
	      if (log_Pb.log_data_ptr)
		{
		  free_and_init (log_Pb.log_data_ptr);
		  log_Pb.log_data_length = 0;
		}
	    }
	  else
	    {
	      log_Pb.log_zip_support = true;
	    }
#endif
	}
    }
  else
    {
      log_Pb.log_zip_support = false;
    }

  assert (LOG_CS_OWN_WRITE_MODE (thread_p));

  if (log_Pb.pool != NULL)
    {
      logpb_finalize_pool ();
    }

  assert (log_Pb.pool == NULL);
  assert (log_Pb.poolarea == NULL);
  assert (log_Pb.ht == NULL);

  log_Pb.num_buffers = 0;
  log_Pb.pool = NULL;
  log_Pb.poolarea = NULL;

  /*
   * Create an area to keep the number of desired buffers
   */
  error_code = logpb_expand_pool (-1);
  if (error_code != NO_ERROR)
    {
      goto error;
    }

  log_Pb.ht = mht_create ("Log buffer pool hash table",
			  log_Pb.num_buffers * 8, mht_logpageidhash,
			  mht_compare_logpageids_are_equal);
  if (log_Pb.ht == NULL)
    {
      error_code = ER_OUT_OF_VIRTUAL_MEMORY;
      goto error;
    }

  error_code = logpb_initialize_flush_info ();
  if (error_code != NO_ERROR)
    {
      goto error;
    }

  pthread_mutex_init (&log_Gl.chkpt_lsa_lock, NULL);

  pthread_cond_init (&group_commit_info->gc_cond, NULL);
  pthread_mutex_init (&group_commit_info->gc_mutex, NULL);

  pthread_mutex_init (&writer_info->wr_list_mutex, NULL);
  pthread_cond_init (&writer_info->wr_list_cond, NULL);

  return error_code;

error:

  logpb_finalize_pool ();
  logpb_fatal_error (thread_p, false, ARG_FILE_LINE, "log_pbpool_init");

  return error_code;
}

/*
 * logpb_finalize_pool - TERMINATES THE LOG BUFFER POOL
 *
 * return: nothing
 *
 * NOTE:Terminate the log buffer pool. All log resident pages are
 *              invalidated.
 */
void
logpb_finalize_pool (void)
{
  struct log_bufarea *area;	/* Buffer area to free */

  assert (LOG_CS_OWN_WRITE_MODE (NULL));

  csect_enter (NULL, CSECT_LOG_BUFFER, INF_WAIT);
  if (log_Pb.pool != NULL)
    {
      if (log_Gl.append.log_pgptr != NULL)
	{
	  logpb_free_without_mutex (log_Gl.append.log_pgptr);
	  log_Gl.append.log_pgptr = NULL;
	  log_Gl.append.delayed_free_log_pgptr = NULL;
	}
      logpb_set_nxio_lsa (&NULL_LSA);
      LSA_SET_NULL (&log_Gl.append.prev_lsa);
      /* copy log_Gl.append.prev_lsa to log_Gl.prior_info.prev_lsa */
      LOG_RESET_PREV_LSA (&log_Gl.append.prev_lsa);

      /*
       * Remove hash table
       */
      if (log_Pb.ht != NULL)
	{
	  mht_destroy (log_Pb.ht);
	  log_Pb.ht = NULL;
	}

      free_and_init (log_Pb.pool);
      log_Pb.num_buffers = 0;
    }

  /*
   * Remove all the buffer pool areas
   */
  while ((area = log_Pb.poolarea) != NULL)
    {
      log_Pb.poolarea = area->next;
      free_and_init (area);
    }

  csect_exit (CSECT_LOG_BUFFER);

  logpb_finalize_flush_info ();

  pthread_mutex_destroy (&log_Gl.chkpt_lsa_lock);

  pthread_mutex_destroy (&log_Gl.group_commit_info.gc_mutex);
  pthread_cond_destroy (&log_Gl.group_commit_info.gc_cond);

  logpb_finalize_writer_info ();

  if (log_Pb.log_zip_support)
    {
#if defined (SERVER_MODE)
#else
      if (log_Pb.log_zip_undo)
	{
	  log_zip_free (log_Pb.log_zip_undo);
	  log_Pb.log_zip_undo = NULL;
	}
      if (log_Pb.log_zip_redo)
	{
	  log_zip_free (log_Pb.log_zip_redo);
	  log_Pb.log_zip_redo = NULL;
	}
      if (log_Pb.log_data_ptr)
	{
	  free_and_init (log_Pb.log_data_ptr);
	  log_Pb.log_data_length = 0;
	}
#endif
    }
}

/*
 * logpb_is_initialize_pool - Find out if buffer pool has been initialized
 *
 * return:
 *
 * NOTE:Find out if the buffer pool has been initialized.
 */
bool
logpb_is_initialize_pool (void)
{
  assert (LOG_CS_OWN_WRITE_MODE (NULL));
  if (log_Pb.pool != NULL)
    {
      return true;
    }
  else
    {
      return false;
    }
}

/*
 * logpb_invalidate_pool - Invalidate all buffers in buffer pool
 *
 * return: Pointer to the page or NULL
 *
 * NOTE:Invalidate all unfixed buffers in the buffer pool.
 *              This is needed when we reset the log header information.
 */
void
logpb_invalidate_pool (THREAD_ENTRY * thread_p)
{
  register struct log_buffer *log_bufptr;	/* A log buffer */
  int i;

  assert (LOG_CS_OWN_WRITE_MODE (thread_p));

  if (log_Pb.pool == NULL)
    {
      return;
    }

  /*
   * Flush any append dirty buffers at this moment.
   * Then, invalidate any buffer that it is not fixed and dirty
   */
  logpb_flush_pages_direct (thread_p);

  csect_enter (thread_p, CSECT_LOG_BUFFER, INF_WAIT);

  for (i = 0; i < log_Pb.num_buffers; i++)
    {
      log_bufptr = LOGPB_FIND_BUFPTR (i);
      if ((log_bufptr->pageid == LOGPB_HEADER_PAGE_ID
	   || log_bufptr->pageid > NULL_PAGEID)
	  && log_bufptr->fcnt <= 0 && log_bufptr->dirty == false)
	{
	  (void) mht_rem (log_Pb.ht, &log_bufptr->pageid, NULL, NULL);
	  logpb_initialize_log_buffer (log_bufptr);
	  logpb_reset_clock_hand (log_bufptr->ipool);
	}
    }

  csect_exit (CSECT_LOG_BUFFER);
}

/*
 * logpb_replace - Find a page to replace
 *
 * return: struct log_buffer *
 *                       retry : true if need to retry, false if not
 *
 *   retry(in/out): true if need to retry, false if not
 *
 */
static struct log_buffer *
logpb_replace (THREAD_ENTRY * thread_p, bool * retry)
{
  register struct log_buffer *log_bufptr = NULL;	/* A log buffer */
  int bufid;
  int ixpool = -1;
  int num_unfixed = 1;
  int error_code = NO_ERROR;

  assert (retry != NULL);

  *retry = false;

  num_unfixed = log_Pb.num_buffers;

  while (ixpool == -1 && num_unfixed > 0)
    {
      /* Recalculate the num_unfixed to avoid infinite loops in case of error */
      num_unfixed = 0;

      for (bufid = 0; bufid < log_Pb.num_buffers; bufid++)
	{
	  log_bufptr = LOGPB_FIND_BUFPTR (log_Pb.clock_hand);
	  logpb_move_next_clock_hand ();
	  /*
	   * Can we replace the current buffer. That is, is it unfixed ?
	   */
	  if ((log_bufptr->fcnt <= 0) && (!log_bufptr->flush_running))
	    {
	      num_unfixed++;
	      /*
	       * Has this buffer recently been freed ?
	       */
	      if (log_bufptr->recently_freed)
		{
		  /* Set it to false for new cycle */
		  log_bufptr->recently_freed = false;
		}
	      else
		{
		  /*
		   * Replace current buffer.
		   * Force the log if the current page is dirty.
		   */
		  ixpool = log_bufptr->ipool;
		  if (log_bufptr->dirty == true)
		    {
		      csect_exit (CSECT_LOG_BUFFER);

		      assert (LOG_CS_OWN_WRITE_MODE (thread_p));
		      log_Stat.log_buffer_flush_count_by_replacement++;
		      logpb_flush_all_append_pages (thread_p);

		      csect_enter (thread_p, CSECT_LOG_BUFFER, INF_WAIT);
		      *retry = true;
		      return NULL;
		    }
		  break;
		}
	    }
	}
    }

  assert (log_bufptr != NULL);

  /*
   * The page is not resident or we are requesting a buffer for working
   * purposes.
   */

  if (ixpool == -1)
    {
      /*
       * All log buffers are fixed. There are many concurrent transactions being
       * aborted at the same time or it is likely that there is a bug in the
       * system (e.g., buffer are not being freed).
       */
      csect_exit (CSECT_LOG_BUFFER);

      error_code = logpb_expand_pool (-1);

      if (error_code != NO_ERROR)
	{
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LOG_ALL_BUFFERS_FIXED, 0);
	  error_code = ER_LOG_ALL_BUFFERS_FIXED;
	  *retry = false;
	}
      else
	{
	  *retry = true;
	}
      log_bufptr = NULL;

      csect_enter (thread_p, CSECT_LOG_BUFFER, INF_WAIT);
    }

  return log_bufptr;
}

/*
 * logpb_create - Create a log page on a log buffer
 *
 * return: Pointer to the page or NULL
 *
 *   pageid(in): Page identifier
 *
 * NOTE:Creates the log page identified by pageid on a log buffer and
 *              return such buffer.
 *              Just intializes log buffer hdr,
 *              To read a page from disk is not needed.
 */
LOG_PAGE *
logpb_create (THREAD_ENTRY * thread_p, LOG_PAGEID pageid)
{
  return logpb_fix_page (thread_p, pageid, NEW_PAGE);
}

/*
 * logpb_fix_page - Fetch a log page
 *
 * return: Pointer to the page or NULL
 *
 *   pageid(in): Page identifier
 *   fetch_mode(in): Is this a new log page ?. That is, can we avoid the I/O
 *
 * NOTE:Fetch the log page identified by pageid into a log buffer and
 *              return such buffer. The page is guaranteed to stay cached in
 *              the buffer until the page is released by the caller.
 *              If the page is not resident in the log buffer pool, the oldest
 *              log page that is not fixed is replaced. The rationale for this
 *              is that if we need to go back to rollback other transactions,
 *              it is likely that we need the earliest log pages and not the
 *              oldest one. A very old page may be in the log buffer pool as a
 *              consequence of a previous rollback. This does not provide
 *              problems to the restart recovery since we hardly go back to
 *              the same page.
 */
static LOG_PAGE *
logpb_fix_page (THREAD_ENTRY * thread_p, LOG_PAGEID pageid, int fetch_mode)
{
  struct log_buffer *log_bufptr;	/* A log buffer */
  LOG_PHY_PAGEID phy_pageid = NULL_PAGEID;	/* The corresponding
						 * physical page
						 */
  bool retry;

  assert (pageid != NULL_PAGEID);
  assert ((fetch_mode == NEW_PAGE) || (fetch_mode == OLD_PAGE));
  assert (LOG_CS_OWN_WRITE_MODE (thread_p));

  csect_enter (thread_p, CSECT_LOG_BUFFER, INF_WAIT);

  log_bufptr = (struct log_buffer *) mht_get (log_Pb.ht, &pageid);
  if (log_bufptr == NULL)
    {
      /*
       * Page is not resident. Find a replacement buffer for it
       */

      while (1)
	{
	  log_bufptr = logpb_replace (thread_p, &retry);
	  if (log_bufptr == NULL)
	    {
	      if (retry)
		{
		  continue;
		}
	      else
		{
		  goto error;
		}
	    }
	  else
	    {
	      break;
	    }
	}

      assert (log_bufptr != NULL);

      /*
       * Remove the current page from the hash table and get the desired
       * page from disk when the page is an old page
       */
      if (log_bufptr->pageid != NULL_PAGEID)
	{
	  (void) mht_rem (log_Pb.ht, &log_bufptr->pageid, NULL, NULL);
	}

      /* Fix the page and mark its pageid invalid */
      log_bufptr->pageid = NULL_PAGEID;
      log_bufptr->fcnt++;

      phy_pageid = logpb_to_physical_pageid (pageid);

      if (fetch_mode == NEW_PAGE)
	{
	  log_bufptr->logpage.hdr.logical_pageid = pageid;
	  log_bufptr->logpage.hdr.offset = NULL_OFFSET;
	}
      else
	{
	  if (logpb_read_page_from_file (thread_p, pageid,
					 &log_bufptr->logpage) == NULL)
	    {
	      goto error;
	    }
	}

      /* Recall the page in the buffer pool, and hash the identifier */
      log_bufptr->pageid = pageid;
      log_bufptr->phy_pageid = phy_pageid;

      if (mht_put (log_Pb.ht, &log_bufptr->pageid, log_bufptr) == NULL)
	{
	  logpb_initialize_log_buffer (log_bufptr);
	  log_bufptr = NULL;
	}
    }
  else
    {
      log_bufptr->fcnt++;
    }

  csect_exit (CSECT_LOG_BUFFER);

  assert (log_bufptr != NULL);

  /* for debugging in release mode  */
  if (log_bufptr == NULL)
    {
      return NULL;
    }

  assert (((UINTPTR) (log_bufptr->logpage.area) % 8) == 0);
  return &(log_bufptr->logpage);

error:

  if (log_bufptr != NULL)
    {
      logpb_initialize_log_buffer (log_bufptr);
      logpb_reset_clock_hand (log_bufptr->ipool);
    }
  csect_exit (CSECT_LOG_BUFFER);

  return NULL;
}

/*
 * logpb_set_dirty - Mark the current page dirty
 *
 * return: nothing
 *
 *   log_pgptr(in): Log page pointer
 *   free_page(in): Free the page too ? Valid values:  FREE, DONT_FREE
 *
 * NOTE:Mark the current log page as dirty and optionally free the
 *              page.
 */
void
logpb_set_dirty (UNUSED_ARG THREAD_ENTRY * thread_p, LOG_PAGE * log_pgptr,
		 int free_page)
{
  struct log_buffer *bufptr;	/* Log buffer associated with given page */

  /* Get the address of the buffer from the page. */
  bufptr = LOG_GET_LOG_BUFFER_PTR (log_pgptr);

#if defined(RYE_DEBUG)
  if (bufptr->pageid != LOGPB_HEADER_PAGE_ID
      && (bufptr->pageid < LOGPB_NEXT_ARCHIVE_PAGE_ID
	  || bufptr->pageid > LOGPB_LAST_ACTIVE_PAGE_ID))
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_LOG_FLUSHING_UNUPDATABLE,
	      1, bufptr->pageid);
    }
#endif /* RYE_DEBUG */

  csect_enter (thread_p, CSECT_LOG_BUFFER, INF_WAIT);

  bufptr->dirty = true;
  if (free_page == FREE)
    {
      logpb_unfix_page (bufptr);
    }

  csect_exit (CSECT_LOG_BUFFER);
}

/*
 * logpb_is_dirty - Find if current log page pointer is dirty
 *
 * return:
 *
 *   log_pgptr(in): Log page pointer
 *
 * NOTE:Find if the current log page is dirty.
 */
static bool
logpb_is_dirty (UNUSED_ARG THREAD_ENTRY * thread_p, LOG_PAGE * log_pgptr)
{
  struct log_buffer *bufptr;	/* Log buffer associated with given page */
  bool is_dirty;

  /* Get the address of the buffer from the page. */
  bufptr = LOG_GET_LOG_BUFFER_PTR (log_pgptr);

  csect_enter (thread_p, CSECT_LOG_BUFFER, INF_WAIT);

  is_dirty = (bool) bufptr->dirty;

  csect_exit (CSECT_LOG_BUFFER);

  return is_dirty;
}

#if !defined(NDEBUG)
/*
 * logpb_is_any_dirty - FIND IF ANY LOG BUFFER IS DIRTY
 *
 * return:
 *
 * NOTE:Find if any log buffer is dirty.
 */
static bool
logpb_is_any_dirty (void)
{
  register struct log_buffer *bufptr;	/* A log buffer */
  int i;
  bool ret;

  csect_enter (NULL, CSECT_LOG_BUFFER, INF_WAIT);

  ret = false;
  for (i = 0; i < log_Pb.num_buffers; i++)
    {
      bufptr = LOGPB_FIND_BUFPTR (i);
      if (bufptr->dirty == true)
	{
	  ret = true;
	  break;
	}
    }

  csect_exit (CSECT_LOG_BUFFER);

  return ret;
}
#endif /* !NDEBUG */

/*
 * logpb_flush_page - Flush a page of the active portion of the log to disk
 *
 * return: nothing
 *
 *   log_pgptr(in): Log page pointer
 *   free_page(in): Free the page too ? Valid values:  FREE, DONT_FREE
 *
 * NOTE:The log page (of the active portion of the log) associated
 *              with pageptr is written out to disk and is optionally freed.
 */
int
logpb_flush_page (THREAD_ENTRY * thread_p, LOG_PAGE * log_pgptr,
		  int free_page)
{
  struct log_buffer *bufptr;	/* Log buffer associated with given page */

  /* Get the address of the buffer from the page. */
  bufptr = LOG_GET_LOG_BUFFER_PTR (log_pgptr);

  assert (LOG_CS_OWN_WRITE_MODE (thread_p));

  csect_enter (thread_p, CSECT_LOG_BUFFER, INF_WAIT);

  if (bufptr->dirty == true)
    {
      /*
       * The buffer is dirty, flush it
       */

      /*
       * Even when the log has been open with the o_sync option, force a sync
       * since some Operationg system (HP) seems that does not have the effect
       * of forcing the page to disk without doing fync
       */

      if (logpb_write_page_to_disk (thread_p, log_pgptr, bufptr->pageid) ==
	  NULL)
	{
	  goto error;
	}
      else
	{
	  bufptr->dirty = false;
	}
    }

  if (free_page == FREE)
    {
      logpb_unfix_page (bufptr);
    }

  csect_exit (CSECT_LOG_BUFFER);

  return NO_ERROR;

error:
  csect_exit (CSECT_LOG_BUFFER);

  return ER_FAILED;
}

/*
 * logpb_free_page - Free a log page
 *
 * return: nothing
 *
 *   log_pgptr(in):Log page pointer
 *
 * NOTE:Free the log buffer where the page associated with log_pgptr
 *              resides. The page is subject to replacement, if not fixed by
 *              other thread of execution.
 */
void
logpb_free_page (UNUSED_ARG THREAD_ENTRY * thread_p, LOG_PAGE * log_pgptr)
{
  csect_enter (thread_p, CSECT_LOG_BUFFER, INF_WAIT);

  logpb_free_without_mutex (log_pgptr);

  csect_exit (CSECT_LOG_BUFFER);
}

/*
 * logpb_free_without_mutex - Free a log page without mutex
 *
 * return: nothing
 *
 *   log_pgptr(in): Log page pointer
 *
 * NOTE:see logpb_free_page
 */
void
logpb_free_without_mutex (LOG_PAGE * log_pgptr)
{
  struct log_buffer *bufptr;	/* Log buffer associated with given page */

  /* Get the address of the buffer from the page. */
  bufptr = LOG_GET_LOG_BUFFER_PTR (log_pgptr);

  if (bufptr->pageid == NULL_PAGEID)
    {
      /*
       * Freeing a buffer used as working area. This buffer can be used for
       * replacement immediately.
       */
      logpb_reset_clock_hand (bufptr->ipool);
    }

  logpb_unfix_page (bufptr);
}

/*
 * logpb_get_page_id - Logical pageid of log buffer/page
 *
 * return: pageid
 *
 *   log_pgptr(in): Log page pointer
 *
 * NOTE:The page identifier of the given log page/buffer.
 *              The page is always fix when this funtion is called.
 *              In replacement, the page cannot be replaced because fix > 0.
 *              So, it isn't needed to lock mutex.
 */
LOG_PAGEID
logpb_get_page_id (LOG_PAGE * log_pgptr)
{
  struct log_buffer *bufptr;	/* Log buffer associated with given page */

  bufptr = LOG_GET_LOG_BUFFER_PTR (log_pgptr);

  assert (bufptr->fcnt > 0);
  return bufptr->pageid;
}

/*
 * logpb_dump - DUMP THE LOG PAGE BUFFER POOL
 *
 * return: nothing
 *
 * NOTE:Dump the log page buffer pool. This function is used for
 *              debugging purposes.
 */
void
logpb_dump (FILE * out_fp)
{
  if (log_Pb.pool == NULL)
    {
      return;
    }

  csect_enter (NULL, CSECT_LOG_BUFFER, INF_WAIT);

  logpb_dump_information (out_fp);

  if (log_Gl.flush_info.num_toflush > 0)
    {
      logpb_dump_to_flush_page (out_fp);
    }

  (void) fprintf (out_fp, "\n\n");
  (void) fprintf (out_fp, "Buf Log_Pageid Phy_pageid Drt Rct Fcnt Bufaddr"
		  "   Pagearea    HDR:Pageid offset\n");

  logpb_dump_pages (out_fp);

  csect_exit (CSECT_LOG_BUFFER);
}

/*
 * logpb_dump_information -
 *
 * return: nothing
 *
 * NOTE:
 */
static void
logpb_dump_information (FILE * out_fp)
{
  long long int delayed_free, append;

  fprintf (out_fp, "\n\n ** DUMP OF LOG BUFFER POOL INFORMATION **\n\n");

  fprintf (out_fp, "\nHash table dump\n");
  mht_dump (out_fp, log_Pb.ht, false, logpb_print_hash_entry, NULL);
  fprintf (out_fp, "\n\n");

  fprintf (out_fp,
	   " Next IO_LSA = %lld|%d, Current append LSA = %lld|%d,"
	   " Prev append LSA = %lld|%d\n"
	   " Prior LSA = %lld|%d, Prev prior LSA = %lld|%d\n\n",
	   (long long int) log_Gl.append.nxio_lsa.pageid,
	   log_Gl.append.nxio_lsa.offset,
	   (long long int) log_Gl.hdr.append_lsa.pageid,
	   log_Gl.hdr.append_lsa.offset,
	   (long long int) log_Gl.append.prev_lsa.pageid,
	   log_Gl.append.prev_lsa.offset,
	   (long long int) log_Gl.prior_info.prior_lsa.pageid,
	   log_Gl.prior_info.prior_lsa.offset,
	   (long long int) log_Gl.prior_info.prev_lsa.pageid,
	   log_Gl.prior_info.prev_lsa.offset);

  if (log_Gl.append.delayed_free_log_pgptr == NULL)
    {
      delayed_free = NULL_PAGEID;
    }
  else
    {
      delayed_free = logpb_get_page_id (log_Gl.append.delayed_free_log_pgptr);
    }

  if (log_Gl.append.log_pgptr == NULL)
    {
      append = NULL_PAGEID;
    }
  else
    {
      append = logpb_get_page_id (log_Gl.append.log_pgptr);
    }

  fprintf (out_fp,
	   " Append to_flush array: max = %d, num_active = %d\n"
	   " Delayed free page = %lld, Current append page = %lld\n",
	   log_Gl.flush_info.max_toflush,
	   log_Gl.flush_info.num_toflush, delayed_free, append);
}

/*
 * logpb_dump_to_flush_page -
 *
 * return: nothing
 *
 * NOTE:
 */
static void
logpb_dump_to_flush_page (FILE * out_fp)
{
  int i;
  struct log_buffer *log_bufptr;
  LOG_FLUSH_INFO *flush_info = &log_Gl.flush_info;

  (void) fprintf (out_fp, " Candidate append pages to flush are:\n");

  for (i = 0; i < flush_info->num_toflush; i++)
    {
      log_bufptr = LOG_GET_LOG_BUFFER_PTR (flush_info->toflush[i]);
      if (i != 0)
	{
	  if ((i % 10) == 0)
	    {
	      fprintf (out_fp, ",\n");
	    }
	  else
	    {
	      fprintf (out_fp, ",");
	    }
	}
      fprintf (out_fp, " %4lld", (long long int) log_bufptr->pageid);
    }

  fprintf (out_fp, "\n");
}

/*
 * logpb_dump_pages -
 *
 * return: nothing
 *
 * NOTE:
 */
static void
logpb_dump_pages (FILE * out_fp)
{
  int i;
  struct log_buffer *log_bufptr;

  for (i = 0; i < log_Pb.num_buffers; i++)
    {
      log_bufptr = LOGPB_FIND_BUFPTR (i);
      if (log_bufptr->pageid == NULL_PAGEID && log_bufptr->fcnt <= 0)
	{
	  /* ***
	   *** (void)fprintf(stdout, "%3d ..\n", i);
	   */
	  continue;
	}
      else
	{
	  fprintf (out_fp, "%3d %10lld %10d %3d %3d %4d  %p %p-%p"
		   " %4s %5lld %5d\n",
		   i, (long long) log_bufptr->pageid, log_bufptr->phy_pageid,
		   log_bufptr->dirty, log_bufptr->recently_freed,
		   log_bufptr->fcnt, (void *) log_bufptr,
		   (void *) (&log_bufptr->logpage),
		   (void *) (&log_bufptr->logpage.area[LOGAREA_SIZE - 1]), "",
		   (long long) log_bufptr->logpage.hdr.logical_pageid,
		   log_bufptr->logpage.hdr.offset);
	}
    }
  (void) fprintf (out_fp, "\n");
}


/*
 * logpb_print_hash_entry - Print a hash entry
 *
 * return: always return true.
 *
 *   outfp(in): FILE stream where to dump the entry.
 *   key(in): The pageid
 *   ent(in): The Buffer pointer
 *   ignore(in): Nothing..
 *
 * NOTE:A page hash table entry is dumped.
 */
int
logpb_print_hash_entry (FILE * outfp, const void *key, void *ent,
			UNUSED_ARG void *ignore)
{
  const LOG_PAGEID *pageid = (const LOG_PAGEID *) key;
  struct log_buffer *log_bufptr = (struct log_buffer *) ent;

  fprintf (outfp, "Pageid = %5lld, Address = %p\n",
	   (long long int) (*pageid), (void *) log_bufptr);

  return (true);
}

/*
 * logpb_initialize_header - Initialize log header structure
 *
 * return: nothing
 *
 *   loghdr(in/out): Log header structure
 *   prefix_logname(in): Name of the log volumes. It is usually set the same as
 *                      database name. For example, if the value is equal to
 *                      "db", the names of the log volumes created are as
 *                      follow:
 *                      Active_log      = db_logactive
 *                      Archive_logs    = db_logarchive.0
 *                                        db_logarchive.1
 *                                             .
 *                                             .
 *                                             .
 *                                        db_logarchive.n
 *                      Log_information = db_loginfo
 *                      Database Backup = db_backup
 *   npages(in): Size of active log in pages
 *   db_creation(in): Database creation time.
 *
 * NOTE:Initialize a log header structure.
 */
int
logpb_initialize_header (struct log_header *loghdr,
			 const char *prefix_logname, DKNPAGES npages,
			 INT64 * db_creation)
{
  assert (loghdr != NULL);

  strncpy (loghdr->log_magic, RYE_MAGIC_LOG_ACTIVE, RYE_MAGIC_MAX_LENGTH);

  if (db_creation != NULL)
    {
      loghdr->db_creation = *db_creation;
    }
  else
    {
      loghdr->db_creation = -1;
    }

  loghdr->db_version = rel_cur_version ();
  loghdr->db_iopagesize = IO_PAGESIZE;
  loghdr->db_logpagesize = LOG_PAGESIZE;
  loghdr->is_shutdown = true;
  loghdr->next_trid = LOG_SYSTEM_TRANID + 1;
  loghdr->avg_ntrans = LOG_ESTIMATE_NACTIVE_TRANS;
  loghdr->avg_nlocks = LOG_ESTIMATE_NOBJ_LOCKS;
  loghdr->npages = npages - 1;	/* Hdr pg is stolen */
  loghdr->db_charset = lang_charset ();
  loghdr->fpageid = 0;
  loghdr->append_lsa.pageid = loghdr->fpageid;
  loghdr->append_lsa.offset = 0;
  LSA_COPY (&loghdr->chkpt_lsa, &loghdr->append_lsa);
  loghdr->nxarv_pageid = loghdr->fpageid;
  loghdr->nxarv_phy_pageid = 1;
  loghdr->nxarv_num = 0;
  loghdr->last_arv_num_for_syscrashes = -1;
  loghdr->last_deleted_arv_num = -1;
  LSA_SET_NULL (&loghdr->bkup_level_lsa);
  if (prefix_logname != NULL)
    {
      STRNCPY (loghdr->prefix_name, prefix_logname, MAXLOGNAME);
    }
  else
    {
      loghdr->prefix_name[0] = '\0';
    }
  loghdr->ha_info.perm_status = LOG_PSTAT_CLEAR;

  loghdr->ha_info.server_state = HA_STATE_UNKNOWN;
  loghdr->ha_info.file_status = -1;
  LSA_COPY (&loghdr->sof_lsa, &loghdr->append_lsa);
  LSA_SET_NULL (&loghdr->eof_lsa);
  LSA_SET_NULL (&loghdr->smallest_lsa_at_last_chkpt);

  return NO_ERROR;
}

/*
 * logpb_create_header_page - Create log header page
 *
 * return: Pointer to the page or NULL
 *
 * NOTE:Create the log header page.
 */
LOG_PAGE *
logpb_create_header_page (THREAD_ENTRY * thread_p)
{
  assert (LOG_CS_OWN_WRITE_MODE (thread_p));
  return logpb_create (thread_p, LOGPB_HEADER_PAGE_ID);
}

/*
 * logpb_fetch_header - Fetch log header
 *
 * return: nothing
 *
 *   hdr(in/out): Pointer where log header is stored
 *
 * NOTE:Read the log header into the area pointed by hdr.
 */
void
logpb_fetch_header (THREAD_ENTRY * thread_p, struct log_header *hdr)
{
  assert (hdr != NULL);
  assert (LOG_CS_OWN_WRITE_MODE (thread_p));
  assert (log_Gl.loghdr_pgptr != NULL);

  logpb_fetch_header_with_buffer (thread_p, hdr, log_Gl.loghdr_pgptr);

  /* sync append_lsa to prior_lsa */
  LOG_RESET_APPEND_LSA (&log_Gl.hdr.append_lsa);
}

/*
 * logpb_fetch_header_with_buffer - Fetch log header using given buffer
 *
 * return: nothing
 *
 *   hdr(in/out): Pointer where log header is stored
 *   log_pgptr(in/out): log page buffer ptr
 *
 * NOTE:Read the log header into the area pointed by hdr
 */
void
logpb_fetch_header_with_buffer (THREAD_ENTRY * thread_p,
				struct log_header *hdr, LOG_PAGE * log_pgptr)
{
  struct log_header *log_hdr;	/* The log header  */

  assert (hdr != NULL);
  assert (LOG_CS_OWN_WRITE_MODE (thread_p));
  assert (log_pgptr != NULL);

  if ((logpb_fetch_page (thread_p, LOGPB_HEADER_PAGE_ID, log_pgptr)) == NULL)
    {
      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			 "log_fetch_hdr_with_buf");
      /* This statement should not be reached */
      (void) logpb_initialize_header (hdr, NULL, 0, NULL);
      return;
    }

  log_hdr = (struct log_header *) (log_pgptr->area);
  *hdr = *log_hdr;

  assert (log_pgptr->hdr.logical_pageid == LOGPB_HEADER_PAGE_ID);
  assert (log_pgptr->hdr.offset == NULL_OFFSET);
}

/*
 * logpb_flush_header - Flush log header
 *
 * return: nothing
 *
 * NOTE:Flush out the log header from the global variable log_Gl.hdr
 *              to disk. Note append pages are not flushed.
 */
void
logpb_flush_header (THREAD_ENTRY * thread_p)
{
  struct log_header *log_hdr;

  assert (LOG_CS_OWN_WRITE_MODE (thread_p));
  assert (log_Gl.loghdr_pgptr != NULL);

  log_hdr = (struct log_header *) (log_Gl.loghdr_pgptr->area);
  *log_hdr = log_Gl.hdr;

  log_Gl.loghdr_pgptr->hdr.logical_pageid = LOGPB_HEADER_PAGE_ID;
  log_Gl.loghdr_pgptr->hdr.offset = NULL_OFFSET;

  logpb_write_page_to_disk (thread_p, log_Gl.loghdr_pgptr,
			    LOGPB_HEADER_PAGE_ID);

  log_Stat.flush_hdr_call_count++;
}

/*
 * logpb_fetch_page - Fetch a exist_log page using local buffer
 *
 * return: Pointer to the page or NULL
 *
 *   pageid(in): Page identifier
 *   log_pgptr(in): Page buffer to copy
 *
 * NOTE:Fetch the log page identified by pageid into a log buffer and
 *              return such buffer.
 *              If there is the page in hash table, copy it to buffer
 *              and return it.
 *              If not, read log page from log.
 */
LOG_PAGE *
logpb_fetch_page (THREAD_ENTRY * thread_p, LOG_PAGEID pageid,
		  LOG_PAGE * log_pgptr)
{
  LOG_PAGE *ret_pgptr = NULL;

  assert (log_pgptr != NULL);
  assert (pageid != NULL_PAGEID);

  /*
   * This If block ensure belows,
   *  case 1. log page (of pageid) is in log page buffer (not prior_lsa list)
   *  case 2. EOL record which is written temporarily by
   *          logpb_flush_all_append_pages is cleared so there is no EOL
   *          in log page (in delayed_free_log_pgptr)
   */
  if (pageid >= log_Gl.hdr.append_lsa.pageid	/* for case 1 */
      || pageid >= log_Gl.append.prev_lsa.pageid)	/* for case 2 */
    {
      LOG_CS_ENTER (thread_p);

      assert (LSA_LE (&log_Gl.append.prev_lsa, &log_Gl.hdr.append_lsa));

      /*
       * copy prior lsa list to log page buffer to ensure that required
       * pageid is in log page buffer
       */
      if (pageid >= log_Gl.hdr.append_lsa.pageid)	/* retry with mutex */
	{
	  logpb_prior_lsa_append_all_list (thread_p);
	}

      /*
       * calling logpb_copy_page in LOG_CS boundary ensures exclusive running
       * with logpb_flush_all_append_pages.
       */
      ret_pgptr = logpb_copy_page (thread_p, pageid, log_pgptr);
      LOG_CS_EXIT ();

      return ret_pgptr;
    }

  ret_pgptr = logpb_copy_page (thread_p, pageid, log_pgptr);

  return ret_pgptr;
}

/*
 * logpb_copy_page_from_log_buffer -
 *
 * return: Pointer to the page or NULL
 *
 */
LOG_PAGE *
logpb_copy_page_from_log_buffer (THREAD_ENTRY * thread_p, LOG_PAGEID pageid,
				 LOG_PAGE * log_pgptr)
{
  LOG_PAGE *ret_pgptr = NULL;

  assert (log_pgptr != NULL);
  assert (pageid != NULL_PAGEID);
  assert (pageid <= log_Gl.hdr.append_lsa.pageid);

  ret_pgptr = logpb_copy_page (thread_p, pageid, log_pgptr);

  return ret_pgptr;
}

/*
 * logpb_copy_page - copy a exist_log page using local buffer
 *
 * return: Pointer to the page or NULL
 *
 *   pageid(in): Page identifier
 *   log_pgptr(in): Page buffer to copy
 *
 * NOTE:Fetch the log page identified by pageid into a log buffer and
 *              return such buffer.
 *              If there is the page in hash table, copy it to buffer
 *              and return it.
 *              If not, read log page from log.
 */
static LOG_PAGE *
logpb_copy_page (THREAD_ENTRY * thread_p, LOG_PAGEID pageid,
		 LOG_PAGE * log_pgptr)
{
  register struct log_buffer *log_bufptr = NULL;
  LOG_PAGE *ret_pgptr = NULL;

  assert (log_pgptr != NULL);
  assert (pageid != NULL_PAGEID);

  LOG_CS_ENTER_READ_MODE (thread_p);

  csect_enter (thread_p, CSECT_LOG_BUFFER, INF_WAIT);

  log_bufptr = (struct log_buffer *) mht_get (log_Pb.ht, &pageid);
  if (log_bufptr != NULL)
    {
      memcpy (log_pgptr, &log_bufptr->logpage, LOG_PAGESIZE);
      ret_pgptr = log_pgptr;
    }

  csect_exit (CSECT_LOG_BUFFER);

  if (log_bufptr == NULL)
    {
      ret_pgptr = logpb_read_page_from_file (thread_p, pageid, log_pgptr);
    }

  LOG_CS_EXIT ();

  return ret_pgptr;
}

/*
 * logpb_read_page_from_file - Fetch a exist_log page from log files
 *
 * return: Pointer to the page or NULL
 *
 *   pageid(in): Page identifier
 *   log_pgptr(in): Page buffer to read
 *
 * NOTE:read the log page identified by pageid into a buffer
 *              from from archive or active log.
 */
LOG_PAGE *
logpb_read_page_from_file (THREAD_ENTRY * thread_p, LOG_PAGEID pageid,
			   LOG_PAGE * log_pgptr)
{
  assert (log_pgptr != NULL);
  assert (pageid != NULL_PAGEID);
  assert (LOG_CS_OWN (thread_p));

  if (logpb_is_page_in_archive (pageid)
      && (LOG_ISRESTARTED () == false
	  || (pageid + LOGPB_ACTIVE_NPAGES) <= log_Gl.hdr.append_lsa.pageid))
    {
      if (logpb_fetch_from_archive (thread_p, pageid,
				    log_pgptr, NULL, NULL, true) == NULL)
	{
	  return NULL;
	}
    }
  else
    {
      LOG_PHY_PAGEID phy_pageid;
      UINT64 perf_start;
      /*
       * Page is contained in the active log.
       * Find the corresponding physical page and read the page form disk.
       */
      phy_pageid = logpb_to_physical_pageid (pageid);

      PERF_MON_GET_CURRENT_TIME (perf_start);

      if (fileio_read (thread_p, log_Gl.append.vdes, log_pgptr, phy_pageid,
		       LOG_PAGESIZE) == NULL)
	{
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LOG_READ, 3, pageid, phy_pageid, log_Name_active);
	  return NULL;
	}
      else
	{
	  mnt_stats_counter_with_time (thread_p, MNT_STATS_LOG_PAGE_IOREADS,
				       1, perf_start);

	  if (log_pgptr->hdr.logical_pageid != pageid)
	    {
	      /* Clean the buffer... since it may be corrupted */
	      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_LOG_PAGE_CORRUPTED, 1, pageid);
	      return NULL;
	    }
	}
    }

  /* keep old function's usage */
  return log_pgptr;
}

/*
 * logpb_read_page_from_active_log -
 *
 *   return:
 *
 *   pageid(in):
 *   num_pages(in):
 *   log_pgptr(out):
 */
int
logpb_read_page_from_active_log (THREAD_ENTRY * thread_p, LOG_PAGEID pageid,
				 int num_pages, LOG_PAGE * log_pgptr)
{
  LOG_PHY_PAGEID phy_start_pageid;
  UINT64 perf_start;

  assert (log_pgptr != NULL);
  assert (pageid != NULL_PAGEID);
  assert (num_pages > 0);

  /*
   * Page is contained in the active log.
   * Find the corresponding physical page and read the page from disk.
   */
  phy_start_pageid = logpb_to_physical_pageid (pageid);
  num_pages = MIN (num_pages, LOGPB_ACTIVE_NPAGES - phy_start_pageid + 1);

  PERF_MON_GET_CURRENT_TIME (perf_start);

  if (fileio_read_pages (thread_p, log_Gl.append.vdes, (char *) log_pgptr,
			 phy_start_pageid, num_pages, LOG_PAGESIZE) == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOG_READ, 3, pageid,
	      phy_start_pageid, log_Name_active);
      return -1;
    }
  else
    {
      mnt_stats_counter_with_time (thread_p, MNT_STATS_LOG_PAGE_IOREADS, 1,
				   perf_start);

      if (log_pgptr->hdr.logical_pageid != pageid)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOG_PAGE_CORRUPTED,
		  1, pageid);
	  return -1;
	}
    }

  return num_pages;
}

/*
 * logpb_write_page_to_disk - writes and syncs a log page to disk
 *
 * return: nothing
 *
 *   log_pgptr(in): Log page pointer
 *   logical_pageid(in): logical page id
 *
 * NOTE:writes and syncs a log page to disk
 */
LOG_PAGE *
logpb_write_page_to_disk (THREAD_ENTRY * thread_p, LOG_PAGE * log_pgptr,
			  LOG_PAGEID logical_pageid)
{
  int nbytes;
  LOG_PHY_PAGEID phy_pageid;

  assert (log_pgptr != NULL);

  phy_pageid = logpb_to_physical_pageid (logical_pageid);

  /* log_Gl.append.vdes is only changed
   * while starting or finishing or recovering server.
   * So, log cs is not needed.
   */
#if 1				/* yaw */
  if (fileio_write (thread_p, log_Gl.append.vdes, log_pgptr, phy_pageid,
		    LOG_PAGESIZE) == NULL)
#else
  if (fileio_write (thread_p, log_Gl.append.vdes, log_pgptr, phy_pageid,
		    LOG_PAGESIZE) == NULL
      || fileio_synchronize (thread_p, log_Gl.append.vdes,
			     log_Name_active) == NULL_VOLDES)
#endif
    {
      if (er_errid () == ER_IO_WRITE_OUT_OF_SPACE)
	{
	  nbytes = log_Gl.hdr.db_logpagesize;
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LOG_WRITE_OUT_OF_SPACE, 4, logical_pageid,
		  phy_pageid, log_Name_active, nbytes);
	}
      else
	{
	  er_set_with_oserror (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_LOG_WRITE, 3,
			       logical_pageid, phy_pageid, log_Name_active);
	}

      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			 "logpb_write_page_to_disk");
      return NULL;
    }

  return log_pgptr;
}

/*
 * logpb_find_header_parameters - Find some database creation parameters
 *
 * return: iopagesize or -1
 *
 *   db_fullname(in): Full name of the database
 *   logpath(in): Directory where the log volumes reside
 *   prefix_logname(in): Name of the log volumes. It is usually set as database
 *                      name. For example, if the value is equal to "db", the
 *                      names of the log volumes created are as follow:
 *                      Active_log      = db_logactive
 *                      Archive_logs    = db_logarchive.0
 *                                        db_logarchive.1
 *                                             .
 *                                             .
 *                                             .
 *                                        db_logarchive.n
 *                      Log_information = db_loginfo
 *                      Database Backup = db_backup
 *   db_iopagesize(in): Set as a side effect to iopagesize
 *   db_creation(in): Set as a side effect to time of database creation
 *   db_versionb(in): Set as a side effect to database disk compatibility
 *   db_charset(in): Set as a side effect to database charset
 *
 * NOTE:Find some database creation parameters such as pagesize,
 *              creation time, and disk compatability.
 */
PGLENGTH
logpb_find_header_parameters (THREAD_ENTRY * thread_p,
			      const char *db_fullname, const char *logpath,
			      const char *prefix_logname,
			      PGLENGTH * io_page_size,
			      PGLENGTH * log_page_size,
			      INT64 * creation_time, RYE_VERSION * db_version,
			      int *db_charset)
{
  struct log_header hdr;	/* Log header */
  char log_pgbuf[IO_MAX_PAGE_SIZE + MAX_ALIGNMENT], *aligned_log_pgbuf;
  LOG_PAGE *log_pgptr = NULL;
  int error_code = NO_ERROR;

  aligned_log_pgbuf = PTR_ALIGN (log_pgbuf, MAX_ALIGNMENT);

  assert (LOG_CS_OWN_WRITE_MODE (thread_p));

  /* Is the system restarted ? */
  if (log_Gl.trantable.area != NULL && log_Gl.append.log_pgptr != NULL)
    {
      *io_page_size = log_Gl.hdr.db_iopagesize;
      *log_page_size = log_Gl.hdr.db_logpagesize;
      *creation_time = log_Gl.hdr.db_creation;
      *db_version = log_Gl.hdr.db_version;

      if (IO_PAGESIZE != *io_page_size || LOG_PAGESIZE != *log_page_size)
	{
	  if (db_set_page_size (*io_page_size, *log_page_size) != NO_ERROR)
	    {
	      goto error;
	    }

	  logpb_finalize_pool ();
	  error_code =
	    logtb_define_trantable_log_latch (thread_p,
					      log_Gl.trantable.
					      num_total_indices);
	  if (error_code != NO_ERROR)
	    {
	      goto error;
	    }
	  error_code = logpb_initialize_pool (thread_p);
	  if (error_code != NO_ERROR)
	    {
	      goto error;
	    }
	  if (logpb_fetch_start_append_page (thread_p) == NULL)
	    {
	      goto error;
	    }
	}
      return *io_page_size;
    }

  /* System is not restarted. Read the header from disk */

  error_code = logpb_initialize_log_names (thread_p, db_fullname, logpath,
					   prefix_logname);
  if (error_code != NO_ERROR)
    {
      goto error;
    }

  /*
   * Avoid setting errors at this moment related to existance of files.
   */

  if (fileio_is_volume_exist (log_Name_active) == false)
    {
      error_code = ER_FAILED;
      goto error;
    }

  log_Gl.append.vdes = fileio_mount (thread_p, db_fullname, log_Name_active,
				     LOG_DBLOG_ACTIVE_VOLID, true, false);
  if (log_Gl.append.vdes == NULL_VOLDES)
    {
      error_code = ER_FAILED;
      goto error;
    }

  error_code = logpb_initialize_pool (thread_p);
  if (error_code != NO_ERROR)
    {
      goto error;
    }

  log_pgptr = (LOG_PAGE *) aligned_log_pgbuf;

  logpb_fetch_header_with_buffer (thread_p, &hdr, log_pgptr);
  logpb_finalize_pool ();

  fileio_dismount (thread_p, log_Gl.append.vdes);
  log_Gl.append.vdes = NULL_VOLDES;

  *io_page_size = hdr.db_iopagesize;
  *log_page_size = hdr.db_logpagesize;
  *creation_time = hdr.db_creation;
  *db_version = hdr.db_version;
  *db_charset = (int) hdr.db_charset;

  /*
   * Make sure that the log is a log file and that it is compatible with the
   * running database and system
   */

  if (strcmp (hdr.prefix_name, prefix_logname) != 0)
    {
      /*
       * This does not look like the log or the log was renamed. Incompatible
       * prefix name with the prefix stored on disk
       */
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
	      ER_LOG_INCOMPATIBLE_PREFIX_NAME, 2,
	      log_Name_active, hdr.prefix_name);
      /* Continue anyhow.. */
    }

  /* only check for incompatibility here, this will be done again in
   * log_xinit which will run the compatibility functions if there are any.
   */
  if (rel_check_disk_compatible (db_version) != REL_COMPATIBLE)
    {
      char ver_string[REL_MAX_VERSION_LENGTH];
      rel_version_to_string (db_version, ver_string, sizeof (ver_string));

      log_Gl.hdr.db_version = *db_version;
      error_code = ER_LOG_INCOMPATIBLE_DATABASE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 2,
	      ver_string, rel_version_string ());
      goto error;
    }

  if (IO_PAGESIZE != *io_page_size || LOG_PAGESIZE != *log_page_size)
    {
      if (db_set_page_size (*io_page_size, *log_page_size) != NO_ERROR)
	{
	  error_code = ER_FAILED;
	  goto error;
	}
      else
	{
	  error_code = sysprm_reload_and_init (NULL);
	  if (error_code != NO_ERROR)
	    {
	      goto error;
	    }

	  error_code =
	    logtb_define_trantable_log_latch (thread_p,
					      log_Gl.trantable.
					      num_total_indices);
	  if (error_code != NO_ERROR)
	    {
	      goto error;
	    }
	}
    }

  return *io_page_size;

error:
  *io_page_size = -1;
  *log_page_size = -1;
  *creation_time = 0;
  *db_version = rel_null_version ();

  return *io_page_size;
}

/*
 *
 *       	       FUNCTIONS RELATED TO APPEND PAGES
 *
 */

/*
 * logpb_fetch_start_append_page - FETCH THE START APPEND PAGE
 *
 * return: Pointer to the page or NULL
 *
 * NOTE:Fetch the start append page.
 */
LOG_PAGE *
logpb_fetch_start_append_page (THREAD_ENTRY * thread_p)
{
  int flag = OLD_PAGE;
  bool need_flush;
#if defined(SERVER_MODE)
  UNUSED_VAR int rv;
#endif /* SERVER_MODE */
  LOG_FLUSH_INFO *flush_info = &log_Gl.flush_info;

  assert (LOG_CS_OWN_WRITE_MODE (thread_p));

  /* detect empty log (page and offset of zero) */
  if ((log_Gl.hdr.append_lsa.pageid == 0)
      && (log_Gl.hdr.append_lsa.offset == 0))
    {
      flag = NEW_PAGE;
    }

  if (log_Gl.append.log_pgptr != NULL)
    {
      /*
       * Somehow we already have an append page, flush all current append page
       * and start form scratch
       */
      logpb_invalid_all_append_pages (thread_p);
    }

  /*
   * Fetch the start append page and set delayed free to NULL.
   */

  log_Gl.append.delayed_free_log_pgptr = NULL;
  log_Gl.append.log_pgptr =
    logpb_fix_page (thread_p, log_Gl.hdr.append_lsa.pageid, flag);
  if (log_Gl.append.log_pgptr == NULL)
    {
      return NULL;
    }

  logpb_set_nxio_lsa (&log_Gl.hdr.append_lsa);
  /*
   * Save this log append page as an active page to be flushed at a later
   * time if the page is modified (dirty).
   * We must save the log append pages in the order that they are defined
   * and need to be flushed.
   */

  need_flush = false;

  rv = pthread_mutex_lock (&flush_info->flush_mutex);

  flush_info->toflush[flush_info->num_toflush] = log_Gl.append.log_pgptr;
  flush_info->num_toflush++;

  if (flush_info->num_toflush >= flush_info->max_toflush)
    {
      /*
       * Force the dirty pages including the current one at this moment
       */
      need_flush = true;
    }
  pthread_mutex_unlock (&flush_info->flush_mutex);

  if (need_flush)
    {
      logpb_flush_pages_direct (thread_p);
    }

  return log_Gl.append.log_pgptr;
}

/*
 * logpb_fetch_start_append_page_new - FETCH THE NEW START APPEND PAGE
 *
 * return: Pointer to the page or NULL
 */
LOG_PAGE *
logpb_fetch_start_append_page_new (THREAD_ENTRY * thread_p)
{
  assert (LOG_CS_OWN_WRITE_MODE (thread_p));

  log_Gl.append.delayed_free_log_pgptr = NULL;
  log_Gl.append.log_pgptr = logpb_fix_page (thread_p,
					    log_Gl.hdr.append_lsa.pageid,
					    NEW_PAGE);
  if (log_Gl.append.log_pgptr == NULL)
    {
      return NULL;
    }

  logpb_set_nxio_lsa (&log_Gl.hdr.append_lsa);

  return log_Gl.append.log_pgptr;
}

/*
 * logpb_next_append_page - Fetch next append page
 *
 * return: nothing
 *
 *   current_setdirty(in): Set the current append page dirty ?
 *
 * NOTE:Fetch the next append page.
 *              If the current append page contains the beginning of the log
 *              record being appended (i.e., log record did fit on current
 *              append page), the freeing of this page is delayed until the
 *              record is completely appended/logged. This is needed since
 *              every log record has a forward pointer to next log record
 *              (i.e., next append address). In addition, we must avoid
 *              flushing this page to disk (e.g., page replacement),
 *              otherwise, during crash recovery we could try to read a log
 *              record that has never been finished and the end of the log may
 *              not be detected. That is, the log would be corrupted.
 *
 *              If the current append page does not contain the beginning of
 *              the log record, the page can be freed and flushed at any time.
 *
 *              If the next page to archive is located at the physical
 *              location of the desired append page, a set of log pages is
 *              archived, so we can continue the append operations.
 */
static void
logpb_next_append_page (THREAD_ENTRY * thread_p,
			LOG_SETDIRTY current_setdirty)
{
  bool need_flush;
#if defined(SERVER_MODE)
  UNUSED_VAR int rv;
#endif /* SERVER_MODE */
  LOG_FLUSH_INFO *flush_info = &log_Gl.flush_info;

  assert (LOG_CS_OWN_WRITE_MODE (thread_p));

  if (current_setdirty == LOG_SET_DIRTY)
    {
      logpb_set_dirty (thread_p, log_Gl.append.log_pgptr, DONT_FREE);
    }

  /*
   * If a log append page is already delayed. We can free the current page
   * since it is not the first one. (i.e., end of log can be detected since
   * previous page has not been released).
   */

  if (log_Gl.append.delayed_free_log_pgptr != NULL)
    {
      /*
       * The free of an append page has already been fdelayed. Thus, the current
       * page cannot be the first page of the append record that expands several
       * log pages.
       */
      logpb_free_page (thread_p, log_Gl.append.log_pgptr);
    }
  else
    {
      /*
       * We have not delayed freeing an append page. Therefore, the current
       * log append page is a candidate for a page holding a new append log
       * record.
       */

      if (logpb_is_dirty (thread_p, log_Gl.append.log_pgptr) == true)
	{
	  log_Gl.append.delayed_free_log_pgptr = log_Gl.append.log_pgptr;
	  /* assert(current_setdirty == LOG_SET_DIRTY); */
	}
      else
	{
	  logpb_free_page (thread_p, log_Gl.append.log_pgptr);
	}

    }

  log_Gl.append.log_pgptr = NULL;

  log_Gl.hdr.append_lsa.pageid++;
  log_Gl.hdr.append_lsa.offset = 0;

  /*
   * Is the next logical page to archive, currently located at the physical
   * location of the next logical append page ? (Remember the log is a RING).
   * If so, we need to archive the log from the next logical page to archive
   * up to the closest page that does not hold the current append log record.
   */

  if (LOGPB_AT_NEXT_ARCHIVE_PAGE_ID (log_Gl.hdr.append_lsa.pageid))
    {
      /* The log must be archived */
      logpb_archive_active_log (thread_p);
    }

  /*
   * Has the log been cycled ?
   */
  if (LOGPB_IS_FIRST_PHYSICAL_PAGE (log_Gl.hdr.append_lsa.pageid))
    {
      log_Gl.hdr.fpageid = log_Gl.hdr.append_lsa.pageid;
      assert (log_Gl.hdr.fpageid % LOGPB_ACTIVE_NPAGES == 0);

      /* Flush the header to save updates by archiving. */
      logpb_flush_header (thread_p);
    }

  /*
   * Fetch the next page as a newly defined append page. Append pages are
   * always new pages
   */

  log_Gl.append.log_pgptr =
    logpb_create (thread_p, log_Gl.hdr.append_lsa.pageid);
  if (log_Gl.append.log_pgptr == NULL)
    {
      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			 "log_next_append_page");
      /* This statement should not be reached */
      return;
    }

  /*
   * Save this log append page as an active page to be flushed at a later
   * time if the page is modified (dirty).
   * We must save the log append pages in the order that they are defined
   * and need to be flushed.
   */

  rv = pthread_mutex_lock (&flush_info->flush_mutex);

  flush_info->toflush[flush_info->num_toflush] = log_Gl.append.log_pgptr;
  flush_info->num_toflush++;

  need_flush = false;
  if (flush_info->num_toflush >= flush_info->max_toflush)
    {
      need_flush = true;
    }

  pthread_mutex_unlock (&flush_info->flush_mutex);

  if (need_flush)
    {
      logpb_flush_all_append_pages (thread_p);
    }

  log_Stat.total_append_page_count++;
}

/*
 * log_writev_append_pages - Write a set of sequential pages
 *
 * return: to_flush or NULL
 *
 *   to_flush(in): Array to address of content of pages to flush
 *   npages(in): Number of pages to flush
 *
 * NOTE:Flush to disk a set of log contiguous pages.
 */
static LOG_PAGE **
logpb_writev_append_pages (THREAD_ENTRY * thread_p, LOG_PAGE ** to_flush,
			   DKNPAGES npages)
{
  struct log_buffer *bufptr;
  LOG_PHY_PAGEID phy_pageid;

  /* In this point, flush buffer cannot be replaced by trans.
   * So, bufptr's pageid and phy_pageid are not changed.
   */

  if (npages > 0)
    {
      bufptr = LOG_GET_LOG_BUFFER_PTR (to_flush[0]);
      phy_pageid = bufptr->phy_pageid;

      if (fileio_writev (thread_p, log_Gl.append.vdes, (void **) to_flush,
			 phy_pageid, npages, LOG_PAGESIZE) == NULL)
	{
	  if (er_errid () == ER_IO_WRITE_OUT_OF_SPACE)
	    {
	      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_LOG_WRITE_OUT_OF_SPACE, 4,
		      bufptr->pageid, phy_pageid, log_Name_active,
		      log_Gl.hdr.db_logpagesize);
	    }
	  else
	    {
	      er_set_with_oserror (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
				   ER_LOG_WRITE, 3,
				   bufptr->pageid, phy_pageid,
				   log_Name_active);
	    }
	  to_flush = NULL;
	}
    }

  return to_flush;
}

/*
 * logpb_write_toflush_pages_to_archive - Background archiving
 *
 * NOTE : write flushed pages to temporary archiving volume
 * (which will be renamed to real archiving volume) at this time.
 * but don't write last page because it will be modified & flushed again.
 * in error case, dismount temp archiving volume and give up background
 * archiving.
 */
static void
logpb_write_toflush_pages_to_archive (THREAD_ENTRY * thread_p)
{
  int i;
  LOG_PAGEID pageid, prev_lsa_pageid;
  LOG_PHY_PAGEID phy_pageid;
  char log_pgbuf[IO_MAX_PAGE_SIZE + MAX_ALIGNMENT];
  LOG_PAGE *log_pgptr = NULL;
  struct log_buffer *bufptr;

  LOG_FLUSH_INFO *flush_info = &log_Gl.flush_info;
  BACKGROUND_ARCHIVING_INFO *bg_arv_info = &log_Gl.bg_archive_info;

  if (log_Gl.bg_archive_info.vdes == NULL_VOLDES
      || flush_info->num_toflush <= 1)
    {
      return;
    }

  pageid = bg_arv_info->current_page_id;
  prev_lsa_pageid = log_Gl.append.prev_lsa.pageid;
  i = 0;
  while (pageid < prev_lsa_pageid && i < flush_info->num_toflush)
    {
      bufptr = LOG_GET_LOG_BUFFER_PTR (flush_info->toflush[i]);
      if (pageid > bufptr->pageid)
	{
	  assert_release (pageid <= bufptr->pageid);
	  fileio_dismount (thread_p, bg_arv_info->vdes);
	  bg_arv_info->vdes = NULL_VOLDES;
	  return;
	}
      else if (pageid < bufptr->pageid)
	{
	  /* to flush all omitted pages by the previous archiving */
	  log_pgptr = (LOG_PAGE *) PTR_ALIGN (log_pgbuf, MAX_ALIGNMENT);
	  if (logpb_fetch_page (thread_p, pageid, log_pgptr) == NULL)
	    {
	      fileio_dismount (thread_p, bg_arv_info->vdes);
	      bg_arv_info->vdes = NULL_VOLDES;
	      return;
	    }
	}
      else
	{
	  log_pgptr = flush_info->toflush[i];
	  i++;
	}

      phy_pageid = pageid - bg_arv_info->start_page_id + 1;
      assert_release (phy_pageid > 0);
      if (fileio_write (thread_p, bg_arv_info->vdes, log_pgptr,
			phy_pageid, LOG_PAGESIZE) == NULL)
	{
	  fileio_dismount (thread_p, bg_arv_info->vdes);
	  bg_arv_info->vdes = NULL_VOLDES;
	  return;
	}

      pageid++;
      bg_arv_info->current_page_id = pageid;
    }

  assert_release (bg_arv_info->current_page_id >=
		  bg_arv_info->last_sync_pageid);
  if ((bg_arv_info->current_page_id - bg_arv_info->last_sync_pageid)
      > (prm_get_bigint_value (PRM_ID_PB_SYNC_ON_FLUSH_SIZE) / IO_PAGESIZE))
    {
      fileio_synchronize (thread_p, bg_arv_info->vdes, log_Name_bg_archive);
      bg_arv_info->last_sync_pageid = bg_arv_info->current_page_id;
    }
}

/*
 * logpb_append_next_record -
 *
 * return: NO_ERROR
 *
 *   node(in):
 */
static int
logpb_append_next_record (THREAD_ENTRY * thread_p, LOG_PRIOR_NODE * node)
{
  if (!LSA_EQ (&node->start_lsa, &log_Gl.hdr.append_lsa))
    {
      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			 "logpb_append_next_record");
    }

  logpb_start_append (thread_p, &node->log_header);

  if (node->data_header != NULL)
    {
      LOG_APPEND_ADVANCE_WHEN_DOESNOT_FIT (thread_p,
					   node->data_header_length);
      logpb_append_data (thread_p, node->data_header_length,
			 node->data_header);
    }

  if (node->udata != NULL)
    {
      logpb_append_data (thread_p, node->ulength, node->udata);
    }

  if (node->rdata != NULL)
    {
      logpb_append_data (thread_p, node->rlength, node->rdata);
    }

  logpb_end_append (thread_p, &node->log_header);

  return NO_ERROR;
}

/*
 * logpb_append_prior_lsa_list -
 *
 * return: NO_ERROR
 *
 *   list(in/out):
 */
static int
logpb_append_prior_lsa_list (THREAD_ENTRY * thread_p, LOG_PRIOR_NODE * list)
{
  LOG_PRIOR_NODE *node;

  assert (LOG_CS_OWN_WRITE_MODE (thread_p));

  /* append prior_flush_list */
  assert (log_Gl.prior_info.prior_flush_list_header == NULL);
  log_Gl.prior_info.prior_flush_list_header = list;

  /* append log buffer */
  while (log_Gl.prior_info.prior_flush_list_header != NULL)
    {
      node = log_Gl.prior_info.prior_flush_list_header;
      log_Gl.prior_info.prior_flush_list_header = node->next;

      logpb_append_next_record (thread_p, node);

      prior_lsa_free_node (thread_p, node);
    }

  return NO_ERROR;
}

/*
 * prior_lsa_remove_prior_list:
 *
 * return: prior list
 *
 */
static LOG_PRIOR_NODE *
prior_lsa_remove_prior_list (UNUSED_ARG THREAD_ENTRY * thread_p)
{
  LOG_PRIOR_NODE *prior_list;

  assert (LOG_CS_OWN_WRITE_MODE (thread_p));

  prior_list = log_Gl.prior_info.prior_list_header;

  log_Gl.prior_info.prior_list_header = NULL;
  log_Gl.prior_info.prior_list_tail = NULL;
  log_Gl.prior_info.list_size = 0;

  return prior_list;
}

/*
 * logpb_prior_lsa_append_all_list:
 *
 * return: NO_ERROR
 *
 */
int
logpb_prior_lsa_append_all_list (THREAD_ENTRY * thread_p)
{
  LOG_PRIOR_NODE *prior_list;
  INT64 current_size;
  UNUSED_VAR int rv;

  assert (LOG_CS_OWN_WRITE_MODE (thread_p));

  rv = pthread_mutex_lock (&log_Gl.prior_info.prior_lsa_mutex);
  current_size = log_Gl.prior_info.list_size;
  prior_list = prior_lsa_remove_prior_list (thread_p);
  pthread_mutex_unlock (&log_Gl.prior_info.prior_lsa_mutex);

  if (prior_list != NULL)
    {
      mnt_stats_counter (thread_p, MNT_STATS_PRIOR_LSA_LIST_SIZE, current_size / ONE_K);	/* kbytes */
      mnt_stats_counter (thread_p, MNT_STATS_PRIOR_LSA_LIST_REMOVED, 1);

      logpb_append_prior_lsa_list (thread_p, prior_list);
    }

  return NO_ERROR;
}

/*
 * logpb_flush_all_append_pages - Flush log append pages
 *
 * return: 1 : log flushed, 0 : do not need log flush, < 0 : error code
 *
 */
static int
logpb_flush_all_append_pages (THREAD_ENTRY * thread_p)
{
  LOG_RECORD_HEADER *tmp_eof = NULL;	/* End of log record */
  struct log_buffer *bufptr;	/* The current buffer log append page
				 * scanned
				 */
  struct log_buffer *prv_bufptr;	/* The previous buffer log append page
					 * scanned
					 */
  int last_idxflush;		/* The smallest dirty append log page to
				 * flush. This is the last one to flush.
				 */
  int idxflush;			/* An index into the first log page buffer
				 * to flush
				 */
  bool need_sync;		/* How we flush anything ? */

  int i;
  bool need_flush = true;
  int error_code = NO_ERROR;
  int flush_page_count = 0;
  bool hold_flush_mutex = false, hold_lpb_cs = false;
  LOG_FLUSH_INFO *flush_info = &log_Gl.flush_info;
#if defined(SERVER_MODE)
  LOGWR_INFO *writer_info = &log_Gl.writer_info;
#endif

  LOG_RECORD_HEADER save_record = {
    {NULL_PAGEID, NULL_OFFSET},	/* prev_tranlsa */
    {NULL_PAGEID, NULL_OFFSET},	/* back_lsa */
    {NULL_PAGEID, NULL_OFFSET},	/* forw_lsa */
    NULL_TRANID,		/* trid */
    LOG_SMALLER_LOGREC_TYPE	/* type */
  };				/* Save last record */

#if defined(SERVER_MODE)
  INT64 flush_start_time = 0;
  INT64 flush_completed_time = 0;
  INT64 all_writer_thr_end_time = 0;
#endif

#if defined(SERVER_MODE)
  UNUSED_VAR int rv;
  LOGWR_ENTRY *entry;
#endif /* SERVER_MODE */
  UINT64 perf_start;

  assert (LOG_CS_OWN_WRITE_MODE (thread_p));

  rv = pthread_mutex_lock (&flush_info->flush_mutex);
  hold_flush_mutex = true;

  if (flush_info->num_toflush < 1)
    {
      need_flush = false;
    }
  if (flush_info->num_toflush == 1)
    {
      /*
       * Don't need to do anything if the page is not dirty.
       *
       * This block is used to avoid updating the last page with an
       * end of file log when it is not needed at all.
       */

      bufptr = LOG_GET_LOG_BUFFER_PTR (flush_info->toflush[0]);
      assert (bufptr->fcnt > 0);

      if (!logpb_is_dirty (thread_p, flush_info->toflush[0]))
	{
	  need_flush = false;
	}
    }

  pthread_mutex_unlock (&flush_info->flush_mutex);
  hold_flush_mutex = false;

  if (!need_flush)
    {
      return 0;
    }

#if !defined(NDEBUG)
  {
    const char *env_value;
    int verbose_dump = -1;

    /*
     * Do we want to dump the buffer pool for future verification
     */
    env_value = envvar_get ("LOG_FLUSH_VERBOSE_DUMP");
    if (env_value != NULL)
      {
	verbose_dump = atoi (env_value) != 0 ? 1 : 0;
      }
    else
      {
	verbose_dump = 0;
      }

    if (verbose_dump != 0)
      {
	fprintf (stdout, "\n DUMP BEFORE FLUSHING APPEND PAGES\n");
	logpb_dump (stdout);
      }
  }
#endif

#if defined(SERVER_MODE)
  if (thread_p && thread_p->type != TT_DAEMON)
    {
      /* set event logging parameter */
      thread_p->event_stats.trace_log_flush_time =
	prm_get_bigint_value (PRM_ID_LOG_TRACE_FLUSH_TIME);
    }
#endif /* SERVER_MODE */

  /*
   * Add an end of log marker to detect the end of the log.
   * The marker should be added at the end of the log if there is not any
   * delayed free page. That is, if we are not in the middle of appending
   * a new log record. Otherwise, we need to change the label of the last
   * append record as log end record. Flush and then check it back.
   */

  if (log_Gl.append.delayed_free_log_pgptr != NULL)
    {
      /*
       * Flush all log append records on such page except the current log
       * record which has not been finished. Save the log record type of
       * this record, overwrite an eof record on such position, and flush
       * the page. Then, restore the record back on the page and change
       * the current append log sequence address
       */

      tmp_eof = (LOG_RECORD_HEADER *) LOG_PREV_APPEND_PTR ();
      save_record = *tmp_eof;

      /* Overwrite it with an end of log marker */
      LSA_SET_NULL (&tmp_eof->forw_lsa);
      tmp_eof->type = LOG_END_OF_LOG;
      LSA_COPY (&log_Gl.hdr.eof_lsa, &log_Gl.append.prev_lsa);

      logpb_set_dirty (thread_p, log_Gl.append.delayed_free_log_pgptr,
		       DONT_FREE);
    }
  else
    {
      /*
       * Add an end of log marker to detect the end of the log. Don't advance the
       * log address, the log end of file is overwritten at a later point.
       */
      LOG_RECORD_HEADER eof;

      eof.trid = LOG_READ_NEXT_TRANID;
      LSA_SET_NULL (&eof.prev_tranlsa);
      LSA_COPY (&eof.back_lsa, &log_Gl.append.prev_lsa);
      LSA_SET_NULL (&eof.forw_lsa);
      eof.type = LOG_END_OF_LOG;

      logpb_start_append (thread_p, &eof);
    }

  /*
   * Now flush all contiguous log append dirty pages. The first log append
   * dirty page is flushed at the end, so we can synchronize it with the
   * rest.
   */

#if defined(SERVER_MODE)
  /* It changes the status of waiting log writer threads and wakes them up */
  assert (hold_flush_mutex == false);
  LOG_CS_DEMOTE (thread_p);

  rv = pthread_mutex_lock (&writer_info->wr_list_mutex);

  writer_info->flush_completed = false;

  if (thread_p != NULL && thread_p->event_stats.trace_log_flush_time > 0)
    {
      flush_start_time = thread_get_log_clock_msec ();

      memset (&writer_info->last_writer_client_info, 0,
	      sizeof (LOG_CLIENTIDS));

      writer_info->trace_last_writer = true;
      writer_info->last_writer_elapsed_time = 0;
      writer_info->last_writer_client_info.client_type = BOOT_CLIENT_UNKNOWN;
    }

  entry = writer_info->writer_list;
  while (entry)
    {
      if (entry->status == LOGWR_STATUS_WAIT)
	{
	  entry->status = LOGWR_STATUS_FETCH;
	  thread_wakeup_with_tran_index (entry->thread_p->tran_index,
					 THREAD_LOGWR_RESUMED);
	}
      entry = entry->next;
    }

  pthread_mutex_unlock (&writer_info->wr_list_mutex);
#endif /* SERVER_MODE */

  idxflush = -1;
  last_idxflush = -1;
  prv_bufptr = NULL;
  need_sync = false;

  rv = pthread_mutex_lock (&flush_info->flush_mutex);
  hold_flush_mutex = true;

  csect_enter (thread_p, CSECT_LOG_BUFFER, INF_WAIT);
  hold_lpb_cs = true;

  /* Record number of writes in statistics */
  PERF_MON_GET_CURRENT_TIME (perf_start);

  for (i = 0; i < flush_info->num_toflush; i++)
    {
      bufptr = LOG_GET_LOG_BUFFER_PTR (flush_info->toflush[i]);

      /*
       * Make sure that we have found the smallest dirty append page to flush
       * which should be flushed at the end.
       */
      assert (!bufptr->flush_running);
      if (last_idxflush == -1)
	{
	  if (bufptr->dirty == true)
	    {
	      /* We have found the smallest dirty page */
	      last_idxflush = i;
	      prv_bufptr = bufptr;
	    }
	  continue;
	}

      if (idxflush != -1 && prv_bufptr != NULL)
	{
	  /*
	   * This append log page should be dirty and contiguous to previous
	   * append page. If it is not, we need to flush the accumulated pages
	   * up to this point, and then start accumulating pages again.
	   */
	  if ((bufptr->dirty == false)
	      || (bufptr->pageid != (prv_bufptr->pageid + 1))
	      || (bufptr->phy_pageid != (prv_bufptr->phy_pageid + 1)))
	    {
	      /*
	       * This page is not contiguous or it is not dirty.
	       *
	       * Flush the accumulated contiguous pages
	       */
	      csect_exit (CSECT_LOG_BUFFER);
	      hold_lpb_cs = false;

	      if (logpb_writev_append_pages (thread_p,
					     &(flush_info->toflush[idxflush]),
					     i - idxflush) == NULL)
		{
		  error_code = ER_FAILED;
		  goto error;
		}
	      else
		{
		  need_sync = true;
		  /*
		   * Start over the accumulation of pages
		   */

		  flush_page_count += i - idxflush;
		  idxflush = -1;
		}

	      csect_enter (thread_p, CSECT_LOG_BUFFER, INF_WAIT);
	      hold_lpb_cs = true;
	    }
	}

      if ((idxflush == -1) && (bufptr->dirty == true))
	{

	  /*
	   * This page should be included in the flush
	   */
	  idxflush = i;
	}

      /* prv_bufptr was not bufptr's previous buffer.
       * prv_bufptr was the first buffer to flush, so only 2 continous pages always were flushed together.
       */
      prv_bufptr = bufptr;

    }

  mnt_stats_counter_with_time (thread_p, MNT_STATS_LOG_PAGE_IOWRITES,
			       flush_info->num_toflush, perf_start);

  /*
   * If there are any accumulated pages, flush them at this point
   */

  csect_exit (CSECT_LOG_BUFFER);
  hold_lpb_cs = false;

  if (idxflush != -1)
    {
      int pageToFlush = flush_info->num_toflush - idxflush;

      /* last countious pages */
      if (logpb_writev_append_pages (thread_p,
				     &(flush_info->toflush[idxflush]),
				     pageToFlush) == NULL)
	{
	  error_code = ER_FAILED;
	  goto error;
	}
      else
	{
	  need_sync = true;
	  flush_page_count += pageToFlush;
	}
    }

  /*
   * Make sure that all of the above log writes are synchronized with any
   * future log writes. That is, the pages should be stored on physical disk.
   */

  if (need_sync == true)
    {
      log_Stat.total_sync_count++;
      if (prm_get_integer_value (PRM_ID_SUPPRESS_FSYNC) == 0
	  || (log_Stat.total_sync_count %
	      prm_get_integer_value (PRM_ID_SUPPRESS_FSYNC) == 0))
	{
	  if (fileio_synchronize (thread_p,
				  log_Gl.append.vdes,
				  log_Name_active) == NULL_VOLDES)
	    {
	      error_code = ER_FAILED;
	      goto error;
	    }
	}
    }
  assert (last_idxflush != -1);
  if (last_idxflush != -1)
    {
      /*
       * Now flush and sync the first log append dirty page
       */

      ++flush_page_count;
      if (logpb_writev_append_pages (thread_p,
				     &(flush_info->toflush[last_idxflush]),
				     1) == NULL)
	{
	  error_code = ER_FAILED;
	  goto error;
	}

      log_Stat.total_sync_count++;
      if (prm_get_integer_value (PRM_ID_SUPPRESS_FSYNC) == 0
	  || (log_Stat.total_sync_count %
	      prm_get_integer_value (PRM_ID_SUPPRESS_FSYNC) == 0))
	{
	  if (fileio_synchronize (thread_p,
				  log_Gl.append.vdes,
				  log_Name_active) == NULL_VOLDES)
	    {
	      error_code = ER_FAILED;
	      goto error;
	    }
	}
    }

  /* dual writing (Background archiving) */
  logpb_write_toflush_pages_to_archive (thread_p);

  /*
   * Now indicate that buffers of the append log pages are not dirty
   * any more.
   */

  csect_enter (thread_p, CSECT_LOG_BUFFER, INF_WAIT);
  hold_lpb_cs = true;

  for (i = 0; i < flush_info->num_toflush; i++)
    {
      bufptr = LOG_GET_LOG_BUFFER_PTR (flush_info->toflush[i]);
      bufptr->dirty = false;
    }

  csect_exit (CSECT_LOG_BUFFER);
  hold_lpb_cs = false;

#if !defined(NDEBUG)
  if (prm_get_bool_value (PRM_ID_LOG_TRACE_DEBUG)
      && logpb_is_any_dirty () == true)
    {
      er_log_debug (ARG_FILE_LINE, "logpb_flush_all_append_pages: Log Buffer"
		    " contains dirty pages\n");
      logpb_dump (stdout);
      fflush (stdout);
    }
#endif

  if (flush_info->num_toflush == flush_info->max_toflush)
    {
      log_Stat.log_buffer_full_count++;
    }

  /*
   * Change the log sequence address to indicate the next append address to
   * flush and synchronize
   */

  if (log_Gl.append.delayed_free_log_pgptr != NULL)
    {
      /*
       * Restore the log append record
       */
      assert (tmp_eof != NULL);
      if (tmp_eof)
	{
	  *tmp_eof = save_record;
	}

      logpb_set_dirty (thread_p, log_Gl.append.delayed_free_log_pgptr,
		       DONT_FREE);

      flush_info->toflush[0] = log_Gl.append.delayed_free_log_pgptr;
      flush_info->num_toflush = 1;
      logpb_set_nxio_lsa (&log_Gl.append.prev_lsa);
    }
  else
    {
      flush_info->num_toflush = 0;

      logpb_set_nxio_lsa (&log_Gl.hdr.append_lsa);
    }

  assert (LSA_EQ (&log_Gl.append.nxio_lsa, &log_Gl.hdr.eof_lsa));
  svr_shm_set_eof (&log_Gl.hdr.eof_lsa);

  if (log_Gl.append.log_pgptr != NULL)
    {
      /* Add the append page */
      flush_info->toflush[flush_info->num_toflush] = log_Gl.append.log_pgptr;
      flush_info->num_toflush++;
    }

  log_Stat.flushall_append_pages_call_count++;
  log_Stat.last_flush_count_by_trans = flush_page_count;
  log_Stat.total_flush_count_by_trans += flush_page_count;

  pthread_mutex_unlock (&flush_info->flush_mutex);
  hold_flush_mutex = false;

#if defined(SERVER_MODE)
  /* it sends signal to LWT to notify that flush is completed */
  rv = pthread_mutex_lock (&writer_info->wr_list_mutex);

  if (thread_p != NULL && thread_p->event_stats.trace_log_flush_time > 0)
    {
      flush_completed_time = thread_get_log_clock_msec ();
    }

  writer_info->flush_completed = true;
  rv = pthread_cond_broadcast (&writer_info->wr_list_cond);

  /* It waits until all log writer threads are done */
  while (logwr_find_entry_status (writer_info, LOGWR_STATUS_FETCH) != NULL
	 && logwr_find_copy_completed_entry (writer_info) == NULL)
    {
      struct timespec to;

      clock_gettime (CLOCK_REALTIME, &to);
      to = timespec_add_msec (&to, 100);
      rv = pthread_cond_timedwait (&writer_info->wr_list_cond,
				   &writer_info->wr_list_mutex, &to);
    }

  writer_info->trace_last_writer = false;

  if (thread_p != NULL && thread_p->event_stats.trace_log_flush_time > 0)
    {
      all_writer_thr_end_time = thread_get_log_clock_msec ();

      if (all_writer_thr_end_time - flush_start_time >
	  thread_p->event_stats.trace_log_flush_time)
	{
	  event_log_log_flush_thr_wait (thread_p, flush_page_count,
					&writer_info->
					last_writer_client_info,
					all_writer_thr_end_time -
					flush_start_time,
					all_writer_thr_end_time -
					flush_completed_time,
					writer_info->
					last_writer_elapsed_time);
	}
    }

  pthread_mutex_unlock (&writer_info->wr_list_mutex);

  assert (hold_flush_mutex == false);
  LOG_CS_PROMOTE (thread_p);
#endif /* SERVER_MODE */

#if defined(SERVER_MODE)
  if (thread_p && thread_p->type != TT_DAEMON)
    {
      /* reset event logging parameter */
      thread_p->event_stats.trace_log_flush_time = 0;
    }
#endif /* SERVER_MODE */

  return 1;

error:

  if (hold_lpb_cs)
    {
      csect_exit (CSECT_LOG_BUFFER);
    }
  if (hold_flush_mutex)
    {
      pthread_mutex_unlock (&flush_info->flush_mutex);
    }

  logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
		     "logpb_flush_all_append_pages");

#if defined(SERVER_MODE)
  if (thread_p && thread_p->type != TT_DAEMON)
    {
      /* reset event logging parameter */
      thread_p->event_stats.trace_log_flush_time = 0;
    }
#endif /* SERVER_MODE */

  return error_code;
}

/*
 * logpb_flush_pages_direct - flush all pages by itself.
 *
 * return: nothing
 *
 */
void
logpb_flush_pages_direct (THREAD_ENTRY * thread_p)
{
  assert (LOG_CS_OWN_WRITE_MODE (thread_p));

  logpb_prior_lsa_append_all_list (thread_p);
  (void) logpb_flush_all_append_pages (thread_p);
  log_Stat.direct_flush_count++;
}

/*
 * logpb_flush_pages - FLUSH LOG APPEND PAGES
 *
 * return: nothing
 *
 *   flush_lsa(in):
 *
 * NOTE:There are 2 cases to commit.
 *   ASYNC_LOG_FLUSH_INTERVAL
 *                       > O : group commit, just return
 *                       = 0 : normal commit, wakeup LFT and wait
 */
void
logpb_flush_pages (THREAD_ENTRY * thread_p, UNUSED_ARG LOG_LSA * flush_lsa)
{
#if !defined(SERVER_MODE)
  LOG_CS_ENTER (thread_p);
  logpb_flush_pages_direct (thread_p);
  LOG_CS_EXIT ();
#else /* SERVER_MODE */
  UNUSED_VAR int rv;
  struct timespec wakeup_time = { 0, 0 };
  int max_wait_time_in_msec = 1000;
  bool group_commit;

  LOG_LSA nxio_lsa;

  LOG_GROUP_COMMIT_INFO *group_commit_info = &log_Gl.group_commit_info;

  assert (flush_lsa != NULL && !LSA_ISNULL (flush_lsa));

  if (!LOG_ISRESTARTED () || flush_lsa == NULL || LSA_ISNULL (flush_lsa))
    {
      LOG_CS_ENTER (thread_p);
      logpb_flush_pages_direct (thread_p);
      LOG_CS_EXIT ();

      return;
    }
  assert (!LOG_CS_OWN_WRITE_MODE (thread_p));

  if (thread_Log_flush_thread.is_available == false)
    {
      LOG_CS_ENTER (thread_p);
      logpb_flush_pages_direct (thread_p);
      LOG_CS_EXIT ();

      return;
    }

  group_commit = LOG_IS_GROUP_COMMIT_ACTIVE ();
  if (group_commit == true)
    {
      /* group commit, just return */

      log_Stat.gc_commit_request_count++;
    }
  else
    {
      /* normal commit: wakeup LFT and wait */

      logpb_get_nxio_lsa (&nxio_lsa);

      while (LSA_LT (&nxio_lsa, flush_lsa))
	{
	  clock_gettime (CLOCK_REALTIME, &wakeup_time);
	  wakeup_time =
	    timespec_add_msec (&wakeup_time, max_wait_time_in_msec);

	  rv = pthread_mutex_lock (&group_commit_info->gc_mutex);
	  logpb_get_nxio_lsa (&nxio_lsa);
	  if (LSA_GE (&nxio_lsa, flush_lsa))
	    {
	      pthread_mutex_unlock (&group_commit_info->gc_mutex);
	      break;
	    }

	  thread_wakeup_log_flush_thread ();

	  (void) pthread_cond_timedwait (&group_commit_info->gc_cond,
					 &group_commit_info->gc_mutex,
					 &wakeup_time);
	  pthread_mutex_unlock (&group_commit_info->gc_mutex);

	  logpb_get_nxio_lsa (&nxio_lsa);
	}
    }

#endif /* SERVER_MODE */
}

/*
 * logpb_invalid_all_append_pages - Invalidate all append pages
 *
 * return: nothing
 *
 * NOTE:Invalidate and free all append pages. Before invalidating the
 *              pages if their are dirty, they are flushed.
 */
void
logpb_invalid_all_append_pages (THREAD_ENTRY * thread_p)
{
#if defined(SERVER_MODE)
  UNUSED_VAR int rv;
#endif /* SERVER_MODE */
  LOG_FLUSH_INFO *flush_info = &log_Gl.flush_info;

  assert (LOG_CS_OWN_WRITE_MODE (thread_p));

  if (log_Gl.append.log_pgptr != NULL)
    {
      /*
       * Somehow we already have an append page, flush all current append page
       * and start form scratch
       */
      logpb_flush_pages_direct (thread_p);
      logpb_free_page (thread_p, log_Gl.append.log_pgptr);
      log_Gl.append.log_pgptr = NULL;
    }

  rv = pthread_mutex_lock (&flush_info->flush_mutex);

  flush_info->num_toflush = 0;
  flush_info->toflush[flush_info->num_toflush] = NULL;

  pthread_mutex_unlock (&flush_info->flush_mutex);
}

/*
 * logpb_flush_log_for_wal - Flush log if needed
 *
 * return: nothing
 *
 *   lsa_ptr(in): Force all log records up to this lsa
 *
 * NOTE:Flush the log up to given log sequence address according to
 *              the WAL rule.
 *              The page buffer manager must call this function whenever a
 *              page is about to be flushed due to a page replacement.
 */
void
logpb_flush_log_for_wal (THREAD_ENTRY * thread_p, const LOG_LSA * lsa_ptr)
{
  if (logpb_need_wal (lsa_ptr))
    {
      mnt_stats_counter (thread_p, MNT_STATS_LOG_WALS, 1);

      LOG_CS_ENTER (thread_p);
      logpb_flush_pages_direct (thread_p);
      LOG_CS_EXIT ();
    }
}

/*
 *
 *       	   FUNCTIONS RELATED TO DATA APPEND
 *
 */

/*
 * logpb_start_append - Start appending a new log record
 *
 * return: nothing
 *
 *   header(in):
 *
 * NOTE:
 */
static void
logpb_start_append (THREAD_ENTRY * thread_p, LOG_RECORD_HEADER * header)
{
  LOG_RECORD_HEADER *log_rec;	/* Log record */
  UNUSED_VAR LOG_PAGEID initial_pageid;

  assert (LOG_CS_OWN_WRITE_MODE (thread_p));

  /* Record number of append log record in statistics */
  mnt_stats_counter (thread_p, MNT_STATS_LOG_APPEND_RECORDS, 1);

  /* Does the new log record fit in this page ? */
  LOG_APPEND_ADVANCE_WHEN_DOESNOT_FIT (thread_p, sizeof (LOG_RECORD_HEADER));

  if (!LSA_EQ (&header->back_lsa, &log_Gl.append.prev_lsa))
    {
      logpb_fatal_error (thread_p, true, ARG_FILE_LINE, "logpb_start_append");
    }

  initial_pageid = log_Gl.hdr.append_lsa.pageid;
  assert (log_Gl.append.log_pgptr != NULL);

  log_rec = (LOG_RECORD_HEADER *) LOG_APPEND_PTR ();
  *log_rec = *header;

  /*
   * If the header of the append page does not have the offset set to the
   * first log record, this is the first log record in the page, set to it.
   */

  if (log_Gl.append.log_pgptr->hdr.offset == NULL_OFFSET)
    {
      log_Gl.append.log_pgptr->hdr.offset = log_Gl.hdr.append_lsa.offset;
    }

  if (log_rec->type == LOG_END_OF_LOG)
    {
      LSA_COPY (&log_Gl.hdr.eof_lsa, &log_Gl.hdr.append_lsa);

      logpb_set_dirty (thread_p, log_Gl.append.log_pgptr, DONT_FREE);
    }
  else
    {
      LSA_COPY (&log_Gl.append.prev_lsa, &log_Gl.hdr.append_lsa);

      /*
       * Set the page dirty, increase and align the append offset
       */
      LOG_APPEND_SETDIRTY_ADD_ALIGN (thread_p, sizeof (LOG_RECORD_HEADER));
    }
#if 0
  /*
   * LOG_DUMMY_FILLPAGE_FORARCHIVE isn't generated no more
   * (It was changed in logpb_archive_active_log)
   * so, this check is not required.
   */

  log_rec = (LOG_RECORD_HEADER *) LOG_PREV_APPEND_PTR ();
  if (log_rec->type == LOG_DUMMY_FILLPAGE_FORARCHIVE)
    {
      /*
       * Get to start of next page if not already advanced ... Note
       * this record type is only safe during backups.
       */
      if (initial_pageid == log_Gl.hdr.append_lsa.pageid)
	{
	  LOG_APPEND_ADVANCE_WHEN_DOESNOT_FIT (thread_p, LOG_PAGESIZE);
	}
    }
#endif
}

/*
 * logpb_append_data - Append data
 *
 * return: nothing
 *
 *   length(in): Length of data to append
 *   data(in):  Data to append
 *
 * NOTE:Append data as part of current log record.
 */
static void
logpb_append_data (THREAD_ENTRY * thread_p, int length, const char *data)
{
  int copy_length;		/* Amount of contiguos data that can be copied        */
  char *ptr;			/* Pointer for copy data into log append buffer       */
  char *last_ptr;		/* Pointer to last portion available to copy into log
				 * append buffer
				 */

  assert (LOG_CS_OWN_WRITE_MODE (thread_p));

  if (length != 0 && data != NULL)
    {
      /*
       * Align if needed,
       * don't set it dirty since this function has not updated
       */
      LOG_APPEND_ALIGN (thread_p, LOG_DONT_SET_DIRTY);

      ptr = LOG_APPEND_PTR ();
      last_ptr = LOG_LAST_APPEND_PTR ();

      /* Does data fit completely in current page ? */
      if ((ptr + length) >= last_ptr)
	{
	  while (length > 0)
	    {
	      if (ptr >= last_ptr)
		{
		  /*
		   * Get next page and set the current one dirty
		   */
		  logpb_next_append_page (thread_p, LOG_SET_DIRTY);
		  ptr = LOG_APPEND_PTR ();
		  last_ptr = LOG_LAST_APPEND_PTR ();
		}
	      /* Find the amount of contiguous data that can be copied */
	      if (ptr + length >= last_ptr)
		{
		  copy_length = CAST_BUFLEN (last_ptr - ptr);
		}
	      else
		{
		  copy_length = length;
		}
	      memcpy (ptr, data, copy_length);
	      ptr += copy_length;
	      data += copy_length;
	      length -= copy_length;
	      log_Gl.hdr.append_lsa.offset += copy_length;
	    }
	}
      else
	{
	  memcpy (ptr, data, length);
	  log_Gl.hdr.append_lsa.offset += length;
	}
      /*
       * Align the data for future appends.
       * Indicate that modifications were done
       */
      LOG_APPEND_ALIGN (thread_p, LOG_SET_DIRTY);
    }
}

/*
 * logpb_end_append - Finish appending a log record
 *
 * return: nothing
 *
 *   flush(in): Is it a requirement to flush the log ?
 *   force_flush(in):
 *
 * NOTE:  Finish appending a log record. If the log record was appended
 *              in several log buffers, these buffers are flushed and freed.
 *              Only one append buffer will remain pin (fetched) in memory.
 *              If the log record was appended in only one buffer, the buffer
 *              is not flushed unless the caller requested flushing (e.g.,
 *              for a log_commit record).
 */
static void
logpb_end_append (THREAD_ENTRY * thread_p, LOG_RECORD_HEADER * header)
{
  assert (LOG_CS_OWN_WRITE_MODE (thread_p));

  LOG_APPEND_ALIGN (thread_p, LOG_DONT_SET_DIRTY);
  LOG_APPEND_ADVANCE_WHEN_DOESNOT_FIT (thread_p, sizeof (LOG_RECORD_HEADER));

  /*
   * Find the log_rec portion of the append record, it may not be in the
   * current append buffer since it can be stored in several buffers. Then,
   * make the log_rec point to next future append record, unless it is
   * the special record type used for archives created during backups
   * that cannot have a forward lsa and must waste the remaining space
   * on the current page.
   */
  assert (LSA_EQ (&header->forw_lsa, &log_Gl.hdr.append_lsa));

  if (log_Gl.append.delayed_free_log_pgptr != NULL)
    {
      assert (logpb_is_dirty (thread_p,
			      log_Gl.append.delayed_free_log_pgptr));

      logpb_set_dirty (thread_p, log_Gl.append.delayed_free_log_pgptr, FREE);
      log_Gl.append.delayed_free_log_pgptr = NULL;
    }
  else
    {
      logpb_set_dirty (thread_p, log_Gl.append.log_pgptr, DONT_FREE);
    }
}

/*
 *
 *       	   FUNCTIONS RELATED TO LOG INFORMATION FILE
 *
 */

/*
 * logpb_create_log_info - Create a log information file
 *
 * return: nothing
 *
 *   logname_info(in): Name of the log information file
 *   db_fullname(in): Name of the database or NULL (defualt to current one)
 *
 * NOTE: Creates a log information file. This file is used as a help
 *              for the DBA of what things has been archived and what archive
 *              logs are not needed during normal restart recovery (i.e.,
 *              other than media crash).
 */
void
logpb_create_log_info (const char *logname_info, const char *db_fullname)
{
  FILE *fp;			/* Pointer to file */
  const char *catmsg;
  const char *db_name = db_fullname;
  int error_code = NO_ERROR;

  /* Create the information file */
  fp = fopen (logname_info, "w");
  if (fp != NULL)
    {
      fclose (fp);
      catmsg = msgcat_message (MSGCAT_CATALOG_RYE,
			       MSGCAT_SET_LOG, MSGCAT_LOG_LOGINFO_COMMENT);
      if (db_name == NULL)
	{
	  db_name = log_Db_fullname;
	}
      if (catmsg == NULL)
	{
	  catmsg = "COMMENT: %s for database %s\n";
	}
      error_code = log_dump_log_info (logname_info, false, catmsg,
				      RYE_MAGIC_LOG_INFO,
				      fileio_get_base_file_name (db_name));
      if (error_code != NO_ERROR)
	{
	  return;
	}

      (void) logpb_add_volume (db_fullname, LOG_DBLOG_INFO_VOLID,
			       logname_info, DISK_UNKNOWN_PURPOSE);
    }
}


/*
 * logpb_get_guess_archive_num - Guess archive number
 *
 * return: arvnum or -1
 *
 *   pageid(in): Desired page
 *
 * NOTE: Guess the archive number where the desired page is archived by
 *              searching the log information file.
 */
static int
logpb_get_guess_archive_num (THREAD_ENTRY * thread_p, LOG_PAGEID pageid)
{
  FILE *fp;
  char line[LOG_MAX_LOGINFO_LINE];
  int arv_num = -1;
  int last_arvnum = -1;
  int next_arvnum;
  bool isfound = false;
  LOG_PAGEID from_pageid;
  LOG_PAGEID to_pageid;
  long long int f, t;

  assert (LOG_CS_OWN (thread_p));

  arv_num = logpb_get_archive_num_from_info_table (thread_p, pageid);

  if (arv_num >= 0)
    {
      return arv_num;
    }

  /*
   * Guess by looking into the log information file. This is just a guess
   */
  fp = fopen (log_Name_info, "r");
  if (fp != NULL)
    {
      while (fgets (line, LOG_MAX_LOGINFO_LINE, fp) != NULL)
	{
	  if (strstr (line + TIME_SIZE_OF_DUMP_LOG_INFO,
		      msgcat_message (MSGCAT_CATALOG_RYE,
				      MSGCAT_SET_LOG,
				      MSGCAT_LOG_LOGINFO_KEYWORD_ARCHIVE))
	      == line + TIME_SIZE_OF_DUMP_LOG_INFO)
	    {
	      /* A candidate for a guess */
	      if (sscanf (line + TIME_SIZE_OF_DUMP_LOG_INFO,
			  "%*s %d %*s %lld %lld", &next_arvnum, &f, &t) == 3)
		{
		  from_pageid = f;
		  to_pageid = t;

		  last_arvnum = next_arvnum;

		  if (pageid < from_pageid)
		    {
		      /*
		       * keep looking.
		       * There is likely a hole in the archive process due to media
		       * crashes off or the log information contains some missing
		       * entries.
		       */
		      continue;
		    }

		  arv_num = next_arvnum;

		  if (pageid >= from_pageid && pageid <= to_pageid)
		    {
		      /* Found the page in this archive */
		      isfound = true;
		      break;
		    }
		}
	    }
	}
      fclose (fp);
    }

  if (arv_num == -1)
    {
      /*
       * If I have a log active, use it to find out a better archive number
       * for initial search
       */
      if (log_Gl.append.vdes != NULL_VOLDES)
	{
	  arv_num = (int) (pageid / LOGPB_ACTIVE_NPAGES);
	}
      else
	{
	  /*
	   * We do not have a clue what it is available. Don't have log active
	   * and likely we did not have backups.
	   * Must trace for available archive volumes
	   */
	  arv_num = 0;
	}
    }
  else if (isfound == false && last_arvnum == arv_num
	   && log_Gl.append.vdes != NULL_VOLDES)
    {
      /*
       * The log archive was chopped somehow.
       */
      arv_num = log_Gl.hdr.nxarv_num - 1;
    }

  /* Insure that we never pick one larger than the next one to be created */
  if (arv_num >= log_Gl.hdr.nxarv_num)
    {
      arv_num = log_Gl.hdr.nxarv_num - 1;
    }

  return arv_num;
}

/*
 * logpb_find_volume_info_exist - Find if volume information exists ?
 *
 * return:
 *
 * NOTE: Find if volume information exist.
 */
bool
logpb_find_volume_info_exist (void)
{
  return fileio_is_volume_exist (log_Name_volinfo);
}

/*
 * logpb_create_volume_info - Create the volume information and add first volume
 *
 * return: NO_ERROR or error code
 *
 *   db_fullname(in): Name of the database or NULL (defualt to current one)
 *
 * NOTE: Create the volume information and add the first volume.
 */
int
logpb_create_volume_info (const char *db_fullname)
{
  char vol_fullname[PATH_MAX];
  char *volinfo_fullname;
  FILE *volinfo_fp = NULL;
  const char *volinfo_name;

  if (db_fullname != NULL)
    {
      fileio_make_volume_info_name (vol_fullname, db_fullname);
      volinfo_fullname = vol_fullname;
    }
  else
    {
      volinfo_fullname = log_Name_volinfo;
    }

  volinfo_fp = fopen (volinfo_fullname, "w");
  if (volinfo_fp == NULL)
    {
      /* Unable to create the database volume information */
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_BO_CANNOT_CREATE_VOL, 2,
			   volinfo_fullname, db_fullname);
      return ER_BO_CANNOT_CREATE_VOL;
    }
  /*
   * Write information about:
   * the active log and the first volume of the database
   * in the volume information file
   */
  volinfo_name = fileio_get_base_file_name (volinfo_fullname);
  fprintf (volinfo_fp, "%4d %s\n", LOG_DBVOLINFO_VOLID, volinfo_name);

  fflush (volinfo_fp);
  fclose (volinfo_fp);

  return NO_ERROR;
}

/*
 * logpb_recreate_volume_info - Recreate the database volume information
 *
 * return: NO_ERROR if all OK, ER_ status otherwise
 *
 * NOTE: Recreate the database volume information from the internal
 *              information that is stored in each volume.
 */
int
logpb_recreate_volume_info (THREAD_ENTRY * thread_p)
{
  VOLID next_volid = LOG_DBFIRST_VOLID;	/* Next volume identifier */
  char next_vol_fullname[PATH_MAX];	/* Next volume name       */
  int error_code = NO_ERROR;

  error_code = logpb_create_volume_info (NULL);
  if (error_code != NO_ERROR)
    {
      goto error;
    }
  if (logpb_add_volume (NULL, LOG_DBLOG_INFO_VOLID, log_Name_info,
			DISK_UNKNOWN_PURPOSE) != LOG_DBLOG_INFO_VOLID)
    {
      error_code = ER_FAILED;
      goto error;
    }
  if (logpb_add_volume (NULL, LOG_DBLOG_ACTIVE_VOLID, log_Name_active,
			DISK_UNKNOWN_PURPOSE) != LOG_DBLOG_ACTIVE_VOLID)
    {
      error_code = ER_FAILED;
      goto error;
    }

  /* First the primary volume, then the rest of the volumes */
  next_volid = LOG_DBFIRST_VOLID;
  STRNCPY (next_vol_fullname, log_Db_fullname, PATH_MAX);

  do
    {
      if (logpb_add_volume (NULL, next_volid, next_vol_fullname,
			    DISK_PERMVOL_GENERIC_PURPOSE) != next_volid)
	{
	  error_code = ER_FAILED;
	  goto error;
	}

      if (disk_get_link (thread_p, log_Db_fullname,
			 next_volid, next_vol_fullname) == NULL)
	{
	  error_code = ER_FAILED;
	  goto error;
	}

      next_volid++;
    }
  while (next_vol_fullname[0] != '\0');

  return error_code;

  /* ****** */
error:
  (void) remove (log_Name_volinfo);
  return error_code;
}

/*
 * logpb_add_volume - Add a new volume entry to the volume information
 *
 * return: new_volid or NULL_VOLID
 *
 *   db_fullname(in):
 *   new_volid(in): New volume identifier
 *   new_volfullname(in): New volume name
 *   new_volpurpose(in): Purpose of new volume
 *
 * NOTE: Add a new entry to the volume information
 */
VOLID
logpb_add_volume (const char *db_fullname, VOLID new_volid,
		  const char *new_volfullname, DISK_VOLPURPOSE new_volpurpose)
{
  char vol_fullname[PATH_MAX];
  char *volinfo_fullname;
  FILE *volinfo_fp = NULL;
  const char *new_volname;

  if (new_volpurpose == DISK_TEMPVOL_TEMP_PURPOSE)
    {
      return new_volid;		/* nop */
    }

  if (db_fullname != NULL)
    {
      fileio_make_volume_info_name (vol_fullname, db_fullname);
      volinfo_fullname = vol_fullname;
    }
  else
    {
      volinfo_fullname = log_Name_volinfo;
    }

  volinfo_fp = fopen (volinfo_fullname, "a");
  if (volinfo_fp != NULL)
    {
      /* Write information about this volume in the volume information file */
      new_volname = fileio_get_base_file_name (new_volfullname);
      fprintf (volinfo_fp, "%4d %s\n", new_volid, new_volname);
      fflush (volinfo_fp);
      fclose (volinfo_fp);

      return new_volid;
    }
  else
    {
      return NULL_VOLID;
    }

  return new_volid;
}

/*
 * logpb_scan_volume_info - Scan the volume information entries
 *
 * return: number of entries or -1 in case of error.
 *
 *   db_fullname(in):
 *   ignore_volid(in): Don't call function with this volume
 *   start_volid(in): Scan should start at this point.
 *   fun(in): Function to be called on each entry
 *   args(in): Additional arguments to be passed to function
 *
 * NOTE: Scan the volume information entries calling the given function
 *              on each entry.
 */
int
logpb_scan_volume_info (THREAD_ENTRY * thread_p, const char *db_fullname,
			VOLID ignore_volid, VOLID start_volid,
			int (*fun) (THREAD_ENTRY * thread_p, VOLID xvolid,
				    const char *vlabel, void *args),
			void *args)
{
  char xxvolinfo_fullname[PATH_MAX];
  char *volinfo_fullname;
  FILE *volinfo_fp = NULL;	/* Pointer to new volinfo */
  char vol_name[PATH_MAX];
  char *temp_path;
  char temp_path_buf[PATH_MAX];
  char temp_vol_fullname[PATH_MAX];	/* Next volume name       */
  VOLID volid = LOG_DBFIRST_VOLID - 1;	/* Next volume identifier */
  int read_int_volid;
  VOLID num_vols = 0;
  bool start_scan = false;
  char format_string[64];

  assert (db_fullname != NULL);

  fileio_make_volume_info_name (xxvolinfo_fullname, db_fullname);
  volinfo_fullname = xxvolinfo_fullname;

  volinfo_fp = fopen (volinfo_fullname, "r");
  if (volinfo_fp == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_BO_CANNOT_FINE_VOLINFO, 1,
	      volinfo_fullname);
      return -1;
    }

  sprintf (format_string, "%%d %%%ds", PATH_MAX - 1);
  while (true)
    {
      if (fscanf (volinfo_fp, format_string, &read_int_volid, vol_name) != 2)
	{
	  break;
	}

      temp_path = fileio_get_directory_path (temp_path_buf, db_fullname);

      /* Get the absolute path name */
      COMPOSE_FULL_NAME (temp_vol_fullname, sizeof (temp_vol_fullname),
			 temp_path, vol_name);

      if ((volid + 1) != NULL_VOLID
	  && (volid + 1) != (VOLID) read_int_volid && num_vols != 0)
	{
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_BO_UNSORTED_VOLINFO,
		  4, volinfo_fullname, num_vols, read_int_volid,
		  temp_vol_fullname);
	  num_vols = -1;
	  break;
	}
      volid = (VOLID) read_int_volid;

      if (volid == NULL_VOLID)
	{
	  continue;
	}

      if (start_scan == false)
	{
	  if (start_volid == read_int_volid)
	    {
	      start_scan = true;
	    }
	  else
	    {
	      continue;
	    }
	}

      if (volid != ignore_volid)
	{
	  if (((*fun) (thread_p, volid, temp_vol_fullname, args)) != NO_ERROR)
	    {
	      break;
	    }

	  num_vols++;
	}
    }

  fclose (volinfo_fp);

  return num_vols;
}

/*
 *
 *       	   FUNCTIONS RELATED TO LOG ARCHIVES
 *
 */

/*
 * logpb_to_physical_pageid - Find physical page identifier of given logic page
 *
 * return: phy page identifier
 *
 *   logical_pageid(in): logical_pageid: Logical log page
 *
 * NOTE: Returns the physical page identifier associated with given
 *              logical page.
 */
LOG_PHY_PAGEID
logpb_to_physical_pageid (LOG_PAGEID logical_pageid)
{
  LOG_PHY_PAGEID phy_pageid;

  if (logical_pageid == LOGPB_HEADER_PAGE_ID)
    {
      phy_pageid = LOGPB_PHYSICAL_HEADER_PAGE_ID;
    }
  else
    {
      LOG_PAGEID tmp_pageid;

      tmp_pageid = logical_pageid - LOGPB_FIRST_ACTIVE_PAGE_ID;
      if (tmp_pageid >= LOGPB_ACTIVE_NPAGES)
	{
	  tmp_pageid %= LOGPB_ACTIVE_NPAGES;
	}
      else if (tmp_pageid < 0)
	{
	  tmp_pageid =
	    LOGPB_ACTIVE_NPAGES - ((-tmp_pageid) % LOGPB_ACTIVE_NPAGES);
	}

      tmp_pageid++;
      if (tmp_pageid > LOGPB_ACTIVE_NPAGES)
	{
	  tmp_pageid %= LOGPB_ACTIVE_NPAGES;
	}

      assert (tmp_pageid <= PAGEID_MAX);
      phy_pageid = (LOG_PHY_PAGEID) tmp_pageid;
    }

  return phy_pageid;
}

/*
 * logpb_is_page_in_archive - Is the given page an archive page ?
 *
 * return:
 *
 *   pageid(in): Log page identifier
 *
 * NOTE:Find if given page is an archive page identifier.
 */
bool
logpb_is_page_in_archive (LOG_PAGEID pageid)
{
  return LOGPB_IS_ARCHIVE_PAGE (pageid);
}

/*
 * logpb_get_archive_number - Archive location of given page
 *
 * return: archive number
 *
 *   pageid(in): The desired logical page
 *
 * NOTE: Find in what archive the page is located or in what archive
 *              the page should have been located.
 */
int
logpb_get_archive_number (THREAD_ENTRY * thread_p, LOG_PAGEID pageid)
{
  int arv_num = 0;

  if (logpb_fetch_from_archive (thread_p, pageid, NULL, &arv_num, NULL,
				false) == NULL)
    {
      return -1;
    }

  if (arv_num < 0)
    {
      arv_num = 0;
    }

  return arv_num;
}

/*
 * logpb_set_unavailable_archive - Cache that given archive is unavailable
 *
 * return: nothing
 *
 *   arv_num(in): Log archive number
 *
 * NOTE: Record that give archive is unavialble.
 */
static void
logpb_set_unavailable_archive (int arv_num)
{
  int *ptr;
  int size;

  assert (LOG_CS_OWN_WRITE_MODE (NULL)
	  || (LOG_CS_OWN_READ_MODE (NULL)
	      && LOG_ARCHIVE_CS_OWN_WRITE_MODE (NULL)));

  if (log_Gl.archive.unav_archives == NULL)
    {
      size = sizeof (*log_Gl.archive.unav_archives) * 10;
      ptr = (int *) malloc (size);
      if (ptr == NULL)
	{
	  return;
	}
      log_Gl.archive.max_unav = 10;
      log_Gl.archive.next_unav = 0;
      log_Gl.archive.unav_archives = ptr;
    }
  else
    {
      if ((log_Gl.archive.next_unav + 1) >= log_Gl.archive.max_unav)
	{
	  size = (sizeof (*log_Gl.archive.unav_archives) *
		  (log_Gl.archive.max_unav + 10));
	  ptr = (int *) realloc (log_Gl.archive.unav_archives, size);
	  if (ptr == NULL)
	    {
	      return;
	    }
	  log_Gl.archive.max_unav += 10;
	  log_Gl.archive.unav_archives = ptr;
	}
    }

  log_Gl.archive.unav_archives[log_Gl.archive.next_unav++] = arv_num;
}

/*
 * logpb_decache_archive_info - Decache any archive log memory information
 *
 * return: nothing
 *
 * NOTE: Decache any archive log memory information.
 */
void
logpb_decache_archive_info (THREAD_ENTRY * thread_p)
{
  assert (LOG_CS_OWN_WRITE_MODE (NULL)
	  || (LOG_CS_OWN_READ_MODE (NULL)
	      && LOG_ARCHIVE_CS_OWN_WRITE_MODE (NULL)));

  if (log_Gl.archive.vdes != NULL_VOLDES)
    {
      fileio_dismount (thread_p, log_Gl.archive.vdes);
      log_Gl.archive.vdes = NULL_VOLDES;
    }
  if (log_Gl.archive.unav_archives != NULL)
    {
      free_and_init (log_Gl.archive.unav_archives);
      log_Gl.archive.max_unav = 0;
      log_Gl.archive.next_unav = 0;
    }
}

/*
 * log_isarchive_available - Is given archive available ?
 *
 * return: true/false
 *        true: means that the archive may be available.
 *       false: it is known that archive is not available.
 *
 *   arv_num(in): Log archive number
 *
 * NOTE:Find if the current archive is available.
 */
static bool
logpb_is_archive_available (int arv_num)
{
  int i;

  assert (LOG_CS_OWN (NULL));

  if (arv_num >= log_Gl.hdr.nxarv_num || arv_num < 0)
    {
      return false;
    }

  if (log_Gl.archive.unav_archives != NULL)
    {
      for (i = 0; i < log_Gl.archive.next_unav; i++)
	{
	  if (log_Gl.archive.unav_archives[i] == arv_num)
	    {
	      return false;
	    }
	}
    }

  return true;
}

/*
 * log_fetch_from_archive - Fetch a log page from the log archives
 *
 * return: log_pgptr or NULL (in case of error)
 *
 *   pageid(in): The desired logical page
 *   log_pgptr(in): Place to return the log page
 *   arv_num(in): Set to archive number where page was found or where page
 *                 should have been found.
 *
 * NOTE: Fetch a log page from archive logs.
 */
LOG_PAGE *
logpb_fetch_from_archive (THREAD_ENTRY * thread_p, LOG_PAGEID pageid,
			  LOG_PAGE * log_pgptr, int *ret_arv_num,
			  struct log_arv_header * ret_arv_hdr, bool is_fatal)
{
  char hdr_pgbuf[IO_MAX_PAGE_SIZE + MAX_ALIGNMENT], *aligned_hdr_pgbuf;
  char log_pgbuf[IO_MAX_PAGE_SIZE + MAX_ALIGNMENT], *aligned_log_pgbuf;
  struct log_arv_header *arv_hdr;
  LOG_PAGE *hdr_pgptr;
  LOG_PHY_PAGEID phy_pageid = NULL_PAGEID;
  char arv_name[PATH_MAX];
  const char *tmp_arv_name;
  int arv_num, vdes;
  int direction = 0, retry;
  bool has_guess_arvnum = false, first_time = true;
  int error_code = NO_ERROR;
  char format_string[64];

  assert (LOG_CS_OWN (thread_p));

  LOG_ARCHIVE_CS_ENTER (thread_p);

  aligned_hdr_pgbuf = PTR_ALIGN (hdr_pgbuf, MAX_ALIGNMENT);
  aligned_log_pgbuf = PTR_ALIGN (log_pgbuf, MAX_ALIGNMENT);

#if !defined(NDEBUG)
  if (prm_get_bool_value (PRM_ID_LOG_TRACE_DEBUG))
    {
      fprintf (stdout, "\n **log_fetch_from_archive has been called on"
	       " pageid = %lld ** \n", (long long int) pageid);
      fflush (stdout);
    }
#endif

  hdr_pgptr = (LOG_PAGE *) aligned_hdr_pgbuf;
  if (log_pgptr == NULL)
    {
      log_pgptr = (LOG_PAGE *) aligned_log_pgbuf;
    }
  if (ret_arv_num == NULL)
    {
      ret_arv_num = &arv_num;
    }

  if (log_Gl.archive.vdes == NULL_VOLDES)
    {
      if (log_Gl.hdr.nxarv_num <= 0)
	{
	  /* We do not have any archives */
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LOG_NOTIN_ARCHIVE, 1, pageid);

	  LOG_ARCHIVE_CS_EXIT ();
	  return NULL;
	}

      /*
       * Guess the archive where that page is stored
       */

      has_guess_arvnum = true;
      *ret_arv_num = logpb_get_guess_archive_num (thread_p, pageid);
      fileio_make_log_archive_name (arv_name, log_Archive_path, log_Prefix,
				    *ret_arv_num);

      error_code = ER_FAILED;
      if (logpb_is_archive_available (*ret_arv_num) == true
	  && fileio_is_volume_exist (arv_name) == true)
	{
	  vdes = fileio_mount (thread_p, log_Db_fullname, arv_name,
			       LOG_DBLOG_ARCHIVE_VOLID, false, false);
	  if (vdes != NULL_VOLDES)
	    {
	      if (fileio_read (thread_p, vdes, hdr_pgptr, 0, LOG_PAGESIZE)
		  == NULL)
		{
		  fileio_dismount (thread_p, vdes);
		  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOG_READ,
			  3, 0, 0, fileio_get_base_file_name (arv_name));

		  LOG_ARCHIVE_CS_EXIT ();
		  return NULL;
		}
	      error_code = NO_ERROR;
	      arv_hdr = (struct log_arv_header *) hdr_pgptr->area;
	      if (log_Gl.append.vdes != NULL_VOLDES)
		{
		  if (difftime64 ((time_t) arv_hdr->db_creation,
				  (time_t) log_Gl.hdr.db_creation) != 0)
		    {
		      /*
		       * This volume does not belong to the database. For now, assume
		       * that it is not only. Later, we will give this error to user
		       */
		      fileio_dismount (thread_p, vdes);
		      vdes = NULL_VOLDES;
		      arv_hdr = NULL;
		    }
		}
	    }
	}

      if (error_code != NO_ERROR)
	{
	  /*
	   * The volume is not online. Ask for it later (below). But first try to
	   * make the best guess for the archive number.
	   */
	  vdes = NULL_VOLDES;
	  arv_hdr = NULL;
	}
    }
  else
    {
      vdes = log_Gl.archive.vdes;
      arv_hdr = &log_Gl.archive.hdr;
      *ret_arv_num = arv_hdr->arv_num;
    }

  sprintf (format_string, "%%%ds", PATH_MAX - 1);

  log_Gl.archive.vdes = NULL_VOLDES;
  while (true)
    {
      UINT64 perf_start;

      /* Is the page in current archive log ? */
      if (arv_hdr != NULL
	  && pageid >= arv_hdr->fpageid
	  && pageid <= arv_hdr->fpageid + arv_hdr->npages - 1)
	{
	  /* Find location of logical page in the archive log */
	  phy_pageid = (LOG_PHY_PAGEID) (pageid - arv_hdr->fpageid + 1);

	  /* Record number of reads in statistics */
	  PERF_MON_GET_CURRENT_TIME (perf_start);

	  if (fileio_read (thread_p, vdes, log_pgptr, phy_pageid,
			   LOG_PAGESIZE) == NULL)
	    {
	      /* Error reading archive page */
	      tmp_arv_name = fileio_get_volume_label_by_fd (vdes, PEEK);
	      fileio_dismount (thread_p, vdes);
	      log_Gl.archive.vdes = NULL_VOLDES;
	      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOG_READ, 3,
		      pageid, phy_pageid, tmp_arv_name);

	      LOG_ARCHIVE_CS_EXIT ();
	      return NULL;
	    }

	  mnt_stats_counter_with_time (thread_p, MNT_STATS_LOG_PAGE_IOREADS,
				       1, perf_start);

	  /* Cast the archive information. May be used again */
	  if (arv_hdr != &log_Gl.archive.hdr)
	    {
	      log_Gl.archive.hdr = *arv_hdr;
	    }
	  log_Gl.archive.vdes = vdes;
	  break;
	}
      else
	{
	  /* If any archive dismount it */
	  if (vdes != NULL_VOLDES)
	    {
	      fileio_dismount (thread_p, vdes);
	      vdes = NULL_VOLDES;
	    }

	  if (has_guess_arvnum == false)
	    {
	      has_guess_arvnum = true;
	      retry = logpb_get_guess_archive_num (thread_p, pageid);
	      if (retry != *ret_arv_num)
		{
		  *ret_arv_num = retry;
		}
	    }
	  else
	    {
	      if (direction == 0)
		{
		  /*
		   * Define the direction by looking for desired page
		   */
		  if (arv_hdr != NULL)
		    {
		      if (pageid < arv_hdr->fpageid)
			{
			  /* Try older archives */
			  direction = -1;
			}
		      else
			{
			  /* Try newer archives */
			  direction = 1;
			}
		    }
		  else
		    {
		      if (first_time != true)
			{
			  if (log_Gl.append.vdes == NULL_VOLDES)
			    {
			      direction = 1;
			    }
			  else
			    {
			      /*
			       * Start looking from the last archive.
			       * Optimized for UNDO.. This is not so bad since this branch
			       * will be reached only when the guess archive is not
			       * available.
			       */
			      *ret_arv_num = log_Gl.hdr.nxarv_num;
			      direction = -1;
			    }
			}
		    }
		}

	      if (arv_hdr != NULL)
		{
		  if (direction == -1)
		    {
		      /*
		       * Try an older archive.
		       * The page that I am looking MUST be smaller than the first
		       * page in current archive
		       */
		      if (pageid < arv_hdr->fpageid)
			{
			  *ret_arv_num -= 1;
			}
		      else
			{
			  *ret_arv_num = -1;
			}
		    }
		  else
		    {
		      /* Try a newer archive.
		       * The page that I am looking MUST be larger than the last page in
		       * current archive
		       */
		      if (pageid > arv_hdr->fpageid + arv_hdr->npages - 1)
			{
			  *ret_arv_num += 1;
			}
		      else
			{
			  *ret_arv_num = log_Gl.hdr.nxarv_num;
			}
		    }
		}
	      else
		{
		  /*
		   * The archive number is not increased the first time in the loop,
		   * so we can ask for it when it is not available.
		   */
		  if (first_time != true)
		    {
		      /*
		       * If we do not have the log active, we don't really know how to
		       * continue, we could be looping forever.
		       */
		      if (log_Gl.append.vdes == NULL_VOLDES)
			{
			  *ret_arv_num = -1;
			}
		      else
			{
			  *ret_arv_num = *ret_arv_num + direction;
			}
		    }
		}

	      first_time = false;
	      if (*ret_arv_num < 0 || *ret_arv_num == log_Gl.hdr.nxarv_num)
		{
		  /* Unable to find page in archive */
		  if (log_Gl.append.vdes != NULL_VOLDES)
		    {
		      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_LOG_NOTIN_ARCHIVE, 1, pageid);
		    }
		  else
		    {
		      /*
		       * This is likely an incomplete recovery (restore).
		       * We do not have the active log and we are looking for a log page
		       */
		      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
			      ER_LOG_NOTIN_ARCHIVE, 1, pageid);
		    }

		  LOG_ARCHIVE_CS_EXIT ();

		  return NULL;
		}
	    }

	  if (logpb_is_archive_available (*ret_arv_num) == false)
	    {
	      arv_hdr = NULL;
	      continue;
	    }

	  fileio_make_log_archive_name (arv_name, log_Archive_path,
					log_Prefix, *ret_arv_num);
	  retry = 3;
	  while (retry != 0 && retry != 1
		 && (vdes = fileio_mount (thread_p, log_Db_fullname, arv_name,
					  LOG_DBLOG_ARCHIVE_VOLID,
					  false, false)) == NULL_VOLDES)
	    {
	      char line_buf[PATH_MAX * 2];
	      bool is_in_crash_recovery;

	      is_in_crash_recovery = log_is_in_crash_recovery ();

	      /*
	       * The archive is not online.
	       */
	      if (is_in_crash_recovery == true)
		{
		  fprintf (stdout, "%s\n", er_msg ());
		}

	    retry_prompt:
	      if (log_default_input_for_archive_log_location >= 0)
		{
		  retry = log_default_input_for_archive_log_location;
		  if (retry == 1 && is_in_crash_recovery == true)
		    {
		      fprintf (stdout,
			       "Continue without present archive. (Partial recovery).\n");
		    }
		}
	      else
		{
		  fprintf (stdout, msgcat_message (MSGCAT_CATALOG_RYE,
						   MSGCAT_SET_LOG,
						   MSGCAT_LOG_STARTS));
		  fprintf (stdout, msgcat_message (MSGCAT_CATALOG_RYE,
						   MSGCAT_SET_LOG,
						   MSGCAT_LOG_LOGARCHIVE_NEEDED),
			   arv_name);
		  fprintf (stdout, msgcat_message (MSGCAT_CATALOG_RYE,
						   MSGCAT_SET_LOG,
						   MSGCAT_LOG_STARTS));

		  if (fgets (line_buf, PATH_MAX, stdin) == NULL)
		    {
		      retry = 0;	/* EOF */
		    }
		  else if (sscanf (line_buf, "%d", &retry) != 1)
		    {
		      retry = -1;	/* invalid input */
		    }
		}

	      switch (retry)
		{
		case 0:	/* quit */
		  logpb_set_unavailable_archive (*ret_arv_num);
		  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_LOG_NOTIN_ARCHIVE, 1, pageid);
		  if (is_fatal)
		    {
		      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
					 "log_fetch_from_archive");
		    }

		  LOG_ARCHIVE_CS_EXIT ();

		  return NULL;

		case 1:	/* Not available */
		  logpb_set_unavailable_archive (*ret_arv_num);
		  break;

		case 3:	/* Relocate */
		  fprintf (stdout, msgcat_message (MSGCAT_CATALOG_RYE,
						   MSGCAT_SET_LOG,
						   MSGCAT_LOG_NEWLOCATION));
		  if (fgets (line_buf, PATH_MAX, stdin) == 0
		      || (sscanf (line_buf, format_string, arv_name) != 1))
		    {
		      fileio_make_log_archive_name (arv_name,
						    log_Archive_path,
						    log_Prefix, *ret_arv_num);
		    }
		  break;

		case 2:	/* Retry */
		  break;

		default:	/* Something strange.  Get user to try again. */
		  fprintf (stdout, msgcat_message (MSGCAT_CATALOG_RYE,
						   MSGCAT_SET_LOG,
						   MSGCAT_LOG_INPUT_RANGE_ERROR),
			   0, 3);
		  goto retry_prompt;
		}
	    }

	  if (vdes != NULL_VOLDES)
	    {
	      UINT64 perf_start;
	      /* Read header page and make sure the page is here */

	      /* Record number of reads in statistics */
	      PERF_MON_GET_CURRENT_TIME (perf_start);

	      if (fileio_read (thread_p, vdes, hdr_pgptr, 0, LOG_PAGESIZE)
		  == NULL)
		{
		  fileio_dismount (thread_p, vdes);
		  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOG_READ,
			  3, 0, 0, fileio_get_base_file_name (arv_name));

		  LOG_ARCHIVE_CS_EXIT ();

		  return NULL;
		}

	      mnt_stats_counter_with_time (thread_p,
					   MNT_STATS_LOG_PAGE_IOREADS, 1,
					   perf_start);

	      arv_hdr = (struct log_arv_header *) hdr_pgptr->area;
	      if (log_Gl.append.vdes != NULL_VOLDES)
		{
		  if (difftime64 ((time_t) arv_hdr->db_creation,
				  (time_t) log_Gl.hdr.db_creation) != 0)
		    {
		      /*
		       * This volume does not belong to the database. For now, assume
		       * that it is not only. Later, we will give this error to user
		       */
		      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_LOG_DOESNT_CORRESPOND_TO_DATABASE, 1,
			      fileio_get_base_file_name (arv_name));
		      arv_hdr = NULL;
		    }
		}
	    }
	  else
	    {
	      arv_hdr = NULL;
	    }
	}
    }

  assert (log_pgptr != NULL && *ret_arv_num != -1 && arv_hdr != NULL);
  if (ret_arv_hdr != NULL)
    {
      *ret_arv_hdr = *arv_hdr;
    }

  LOG_ARCHIVE_CS_EXIT ();

  return log_pgptr;
}


/*
 * logpb_archive_active_log - Archive the active portion of the log
 *
 * return: nothing
 *
 * NOTE: The active portion of the log is archived from the next log
 *              archive page to the previous log page of the current append
 *              log record, to the next log archive.
 */
void
logpb_archive_active_log (THREAD_ENTRY * thread_p)
{
  char arv_name[PATH_MAX] = { '\0' };	/* Archive name        */
  LOG_PAGE *malloc_arv_hdr_pgptr = NULL;	/* Archive header page
						 * PTR
						 */
  struct log_arv_header *arvhdr;	/* Archive header      */
  BACKGROUND_ARCHIVING_INFO *bg_arv_info;
  char log_pgbuf[IO_MAX_PAGE_SIZE * LOGPB_IO_NPAGES + MAX_ALIGNMENT];
  char *aligned_log_pgbuf;
  LOG_PAGE *log_pgptr = NULL;
  LOG_PAGEID pageid, last_pageid;
  LOG_PHY_PAGEID ar_phy_pageid;
  int vdes = NULL_VOLDES;
  const char *catmsg;
  int error_code = NO_ERROR;
  int num_pages = 0;

  aligned_log_pgbuf = PTR_ALIGN (log_pgbuf, MAX_ALIGNMENT);

  assert (LOG_CS_OWN_WRITE_MODE (thread_p));

#if defined(SERVER_MODE)
  thread_wakeup_purge_archive_logs_thread ();
#else
  logpb_remove_archive_logs_exceed_limit (thread_p, 0);
#endif

  if (log_Gl.hdr.nxarv_pageid >= log_Gl.hdr.append_lsa.pageid)
    {
      er_log_debug (ARG_FILE_LINE, "log_archive_active_log: WARNING "
		    "Trying to archive ONLY the append page"
		    " which is incomplete\n");
      return;
    }

  bg_arv_info = &log_Gl.bg_archive_info;
  if (log_Gl.archive.vdes != NULL_VOLDES)
    {
      assert (LOG_CS_OWN_WRITE_MODE (thread_p)
	      || (LOG_CS_OWN_READ_MODE (thread_p)
		  && LOG_ARCHIVE_CS_OWN_WRITE_MODE (thread_p)));

      fileio_dismount (thread_p, log_Gl.archive.vdes);
      log_Gl.archive.vdes = NULL_VOLDES;
    }

  malloc_arv_hdr_pgptr = (LOG_PAGE *) malloc (LOG_PAGESIZE);
  if (malloc_arv_hdr_pgptr == NULL)
    {
      goto error;
    }
  memset (malloc_arv_hdr_pgptr, 0, LOG_PAGESIZE);

  /* Must force the log here to avoid nasty side effects */
  logpb_flush_all_append_pages (thread_p);

  malloc_arv_hdr_pgptr->hdr.logical_pageid = LOGPB_HEADER_PAGE_ID;
  malloc_arv_hdr_pgptr->hdr.offset = NULL_OFFSET;

  /* Construct the archive log header */
  arvhdr = (struct log_arv_header *) malloc_arv_hdr_pgptr->area;
  strncpy (arvhdr->magic, RYE_MAGIC_LOG_ARCHIVE, RYE_MAGIC_MAX_LENGTH);
  arvhdr->db_creation = log_Gl.hdr.db_creation;
  arvhdr->next_trid = log_Gl.hdr.next_trid;
  arvhdr->arv_num = log_Gl.hdr.nxarv_num;

  /*
   * All pages must be archived... even the ones with unactive log records
   * This is the desired parameter to support multimedia crashes.
   *
   *
   * Note that the npages field does not include the previous lsa page
   *
   */
  arvhdr->fpageid = log_Gl.hdr.nxarv_pageid;
  last_pageid = log_Gl.append.prev_lsa.pageid - 1;

#if 0
  /*
   * logpb_must_archive_last_log_page can call logpb_archive_active_log again
   * and then, log_Gl.hdr could be changed and it make trouble. (assert or shutdown)
   *
   * so, new behavior of logpb_backup is fixed as don't copy incomplete last page
   * as the result, this code block is commented.
   */
  /*
   * When forcing an archive for backup purposes, it is imperative that
   * every single log record make it into the archive including the
   * current page.  This often means archiving an incomplete page.
   * To archive the last page in a way that recovery analysis will
   * realize it is incomplete, requires a dummy record with no forward
   * lsa pointer.  It also requires the the next record after that
   * be appended to a new page (which will happen automatically).
   */
  if (logpb_must_archive_last_log_page (thread_p) != NO_ERROR)
    {
      goto error;
    }
#endif

  if (last_pageid < arvhdr->fpageid)
    {
      last_pageid = arvhdr->fpageid;
    }

  arvhdr->npages = (DKNPAGES) (last_pageid - arvhdr->fpageid + 1);

  /*
   * Now create the archive and start copying pages
   */

  mnt_stats_counter (thread_p, MNT_STATS_LOG_ARCHIVES, 1);

  fileio_make_log_archive_name (arv_name, log_Archive_path,
				log_Prefix, log_Gl.hdr.nxarv_num);

  if (bg_arv_info->vdes != NULL_VOLDES)
    {
      vdes = bg_arv_info->vdes;
    }
  else
    {
      vdes = fileio_format (thread_p, log_Db_fullname, arv_name,
			    LOG_DBLOG_ARCHIVE_VOLID, arvhdr->npages + 1,
			    false, false, false, LOG_PAGESIZE, 0, false);
      if (vdes == NULL_VOLDES)
	{
	  /* Unable to create archive log to archive */
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LOG_CREATE_LOGARCHIVE_FAIL, 3,
		  fileio_get_base_file_name (arv_name), arvhdr->fpageid,
		  arvhdr->fpageid + arvhdr->npages - 1);
	  goto error;
	}
    }

  er_log_debug (ARG_FILE_LINE,
		"logpb_archive_active_log, arvhdr->fpageid = %lld\n",
		arvhdr->fpageid);

  if (fileio_write (thread_p, vdes, malloc_arv_hdr_pgptr, 0, LOG_PAGESIZE)
      == NULL)
    {
      /* Error archiving header page into archive */
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOG_WRITE, 3,
	      0, 0, fileio_get_base_file_name (arv_name));
      goto error;
    }

  if (bg_arv_info->vdes != NULL_VOLDES
      && arvhdr->fpageid == bg_arv_info->start_page_id)
    {
      pageid = bg_arv_info->current_page_id;
      ar_phy_pageid = (LOG_PHY_PAGEID) (bg_arv_info->current_page_id
					- bg_arv_info->start_page_id + 1);
    }
  else
    {
      assert (bg_arv_info->vdes == NULL_VOLDES);

      pageid = arvhdr->fpageid;
      ar_phy_pageid = 1;
    }

  log_pgptr = (LOG_PAGE *) aligned_log_pgbuf;

  /* Now start dumping the current active pages to archive */
  for (; pageid <= last_pageid;
       pageid += num_pages, ar_phy_pageid += num_pages)
    {
      num_pages = MIN (LOGPB_IO_NPAGES, last_pageid - pageid + 1);
      num_pages = logpb_read_page_from_active_log (thread_p, pageid,
						   num_pages, log_pgptr);
      if (num_pages <= 0)
	{
	  goto error;
	}

      if (fileio_write_pages (thread_p, vdes, (char *) log_pgptr,
			      ar_phy_pageid, num_pages, LOG_PAGESIZE) == NULL)
	{
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOG_WRITE, 3,
		  pageid, ar_phy_pageid,
		  fileio_get_base_file_name (arv_name));
	  goto error;
	}
    }

  if (bg_arv_info->vdes != NULL_VOLDES)
    {
      fileio_dismount (thread_p, vdes);
      vdes = NULL_VOLDES;
      bg_arv_info->vdes = NULL_VOLDES;

      /* rename _lgar_t to _lgar[number] name */
      if (fileio_rename (NULL_VOLID, log_Name_bg_archive, arv_name) == NULL)
	{
	  goto error;
	}

      vdes = fileio_mount (thread_p, log_Db_fullname, arv_name,
			   LOG_DBLOG_ARCHIVE_VOLID, 0, false);
      if (vdes == NULL_VOLDES)
	{
	  goto error;
	}
    }
  else
    {
      /*
       * Make sure that the whole log archive is in physical storage at this
       * moment
       */
      if (fileio_synchronize (thread_p, vdes, arv_name) == NULL_VOLDES)
	{
	  goto error;
	}
    }

  /* The last archive needed for system crashes */
  if (log_Gl.hdr.last_arv_num_for_syscrashes == -1)
    {
      log_Gl.hdr.last_arv_num_for_syscrashes = log_Gl.hdr.nxarv_num;
    }

  log_Gl.hdr.nxarv_num++;
  log_Gl.hdr.nxarv_pageid = last_pageid + 1;
  log_Gl.hdr.nxarv_phy_pageid =
    logpb_to_physical_pageid (log_Gl.hdr.nxarv_pageid);

  /* Flush the log header to reflect the archive */
  logpb_flush_header (thread_p);

#if 0
  if (prm_get_integer_value (PRM_ID_SUPPRESS_FSYNC) != 0)
    {
      fileio_synchronize (thread_p, log_Gl.append.vdes);
    }
#endif

  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_LOG_ARCHIVE_CREATED, 3,
	  fileio_get_base_file_name (arv_name), arvhdr->fpageid, last_pageid);

  /* Cast the archive information. May be used again */

  assert (LOG_CS_OWN_WRITE_MODE (thread_p)
	  || (LOG_CS_OWN_READ_MODE (thread_p)
	      && LOG_ARCHIVE_CS_OWN_WRITE_MODE (thread_p)));

  log_Gl.archive.hdr = *arvhdr;	/* Copy of structure */
  log_Gl.archive.vdes = vdes;

  catmsg = msgcat_message (MSGCAT_CATALOG_RYE,
			   MSGCAT_SET_LOG, MSGCAT_LOG_LOGINFO_ARCHIVE);
  if (catmsg == NULL)
    {
      catmsg = "ARCHIVE: %d %s %lld %lld\n";
    }
  error_code = log_dump_log_info (log_Name_info, true, catmsg,
				  log_Gl.hdr.nxarv_num - 1,
				  fileio_get_base_file_name (arv_name),
				  arvhdr->fpageid, last_pageid);
  if (error_code != NO_ERROR && error_code != ER_LOG_MOUNT_FAIL)
    {
      goto error;
    }

  (void) logpb_add_archive_page_info (thread_p, log_Gl.hdr.nxarv_num - 1,
				      arvhdr->fpageid, last_pageid);

#if defined(SERVER_MODE)
  {
    LOG_PAGEID min_fpageid = logwr_get_min_copied_fpageid ();

    if (min_fpageid != NULL_PAGEID)
      {
	int unneeded_arvnum = -1;
	if (min_fpageid >= arvhdr->fpageid)
	  {
	    unneeded_arvnum = arvhdr->arv_num - 1;
	  }
	else
	  {
	    struct log_arv_header min_arvhdr;
	    if (logpb_fetch_from_archive (thread_p, min_fpageid,
					  NULL, NULL, &min_arvhdr,
					  false) != NULL)
	      {
		unneeded_arvnum = min_arvhdr.arv_num - 1;
	      }
	  }
	if (unneeded_arvnum >= 0)
	  {
	    char unneeded_logarv_name[PATH_MAX];
	    fileio_make_log_archive_name (unneeded_logarv_name,
					  log_Archive_path, log_Prefix,
					  unneeded_arvnum);

	    catmsg = msgcat_message (MSGCAT_CATALOG_RYE,
				     MSGCAT_SET_LOG,
				     MSGCAT_LOG_LOGINFO_COMMENT_UNUSED_ARCHIVE_NAME);
	    if (catmsg == NULL)
	      {
		catmsg = "29 COMMENT: Log archive %s,"
		  " which contains log pages before %lld,"
		  " is not needed any longer by any HA utilities.\n";
	      }
	    error_code =
	      log_dump_log_info (log_Name_info, true,
				 catmsg,
				 fileio_get_base_file_name
				 (unneeded_logarv_name), min_fpageid);
	    if (error_code != NO_ERROR && error_code != ER_LOG_MOUNT_FAIL)
	      {
		goto error;
	      }
	  }
      }
  }

#endif /* SERVER_MODE */


  /* rename removed archive log file to reuse it */
  os_rename_file (log_Name_removed_archive, log_Name_bg_archive);

  bg_arv_info->vdes = fileio_format (thread_p, log_Db_fullname,
				     log_Name_bg_archive,
				     LOG_DBLOG_BG_ARCHIVE_VOLID,
				     LOGPB_ACTIVE_NPAGES, false,
				     false, false, LOG_PAGESIZE, 0, true);
  if (bg_arv_info->vdes != NULL_VOLDES)
    {
      bg_arv_info->start_page_id = log_Gl.hdr.nxarv_pageid;
      bg_arv_info->current_page_id = log_Gl.hdr.nxarv_pageid;
      bg_arv_info->last_sync_pageid = log_Gl.hdr.nxarv_pageid;
    }
  else
    {
      bg_arv_info->start_page_id = NULL_PAGEID;
      bg_arv_info->current_page_id = NULL_PAGEID;
      bg_arv_info->last_sync_pageid = NULL_PAGEID;

      er_log_debug (ARG_FILE_LINE,
		    "Unable to create temporary archive log %s\n",
		    log_Name_bg_archive);
    }

  er_log_debug (ARG_FILE_LINE,
		"logpb_archive_active_log end, arvhdr->fpageid = %lld, "
		"arvhdr->npages = %d\n", arvhdr->fpageid, arvhdr->npages);

  free_and_init (malloc_arv_hdr_pgptr);

  return;

  /* ********* */
error:

  if (malloc_arv_hdr_pgptr != NULL)
    {
      free_and_init (malloc_arv_hdr_pgptr);
    }

  if (vdes != NULL_VOLDES)
    {
      fileio_dismount (thread_p, vdes);
      fileio_unformat (thread_p, arv_name);
    }

  if (bg_arv_info->vdes != NULL_VOLDES && bg_arv_info->vdes != vdes)
    {
      fileio_dismount (thread_p, bg_arv_info->vdes);
    }
  fileio_unformat (thread_p, log_Name_bg_archive);
  bg_arv_info->vdes = NULL_VOLDES;

  logpb_fatal_error (thread_p, true, ARG_FILE_LINE, "log_archive_active_log");
}

int
logpb_remove_archive_logs_exceed_limit (THREAD_ENTRY * thread_p,
					int max_count)
{
  int first_arv_num_to_delete = -1;
  int last_arv_num_to_delete = -1;
#if defined(SERVER_MODE)
  LOG_PAGEID min_copied_pageid;
  int min_copied_arv_num;
#endif /* SERVER_MODE */
  int num_remove_arv_num;
  int log_max_archives = prm_get_integer_value (PRM_ID_LOG_MAX_ARCHIVES);
  const char *catmsg;
  int deleted_count = 0;

  if (log_max_archives == INT_MAX)
    {
      return deleted_count;
    }

  LOG_CS_ENTER (thread_p);

  if (!prm_get_bool_value (PRM_ID_FORCE_REMOVE_LOG_ARCHIVES))
    {
#if defined(SERVER_MODE)
      min_copied_pageid = logwr_get_min_copied_fpageid ();
      if (min_copied_pageid == NULL_PAGEID)
	{
	  LOG_CS_EXIT ();
	  return deleted_count;
	}

      if (logpb_is_page_in_archive (min_copied_pageid))
	{
	  min_copied_arv_num =
	    logpb_get_archive_number (thread_p, min_copied_pageid);
	  if (min_copied_arv_num == -1)
	    {
	      LOG_CS_EXIT ();
	      return deleted_count;
	    }
	  else if (min_copied_arv_num > 1)
	    {
	      min_copied_arv_num--;
	    }

	  num_remove_arv_num =
	    MAX (log_max_archives, log_Gl.hdr.nxarv_num - min_copied_arv_num);
	}
      else
	{
	  num_remove_arv_num = log_max_archives;
	}
#else /* SERVER_MODE */
      num_remove_arv_num = log_max_archives;
#endif
    }
  else
    {
      num_remove_arv_num = log_max_archives;
    }

  if ((log_Gl.hdr.nxarv_num - (log_Gl.hdr.last_deleted_arv_num + 1))
      > num_remove_arv_num)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
	      ER_LOG_MAX_ARCHIVES_HAS_BEEN_EXCEEDED, 1, num_remove_arv_num);

      /* Remove the log archives at this point */
      first_arv_num_to_delete = log_Gl.hdr.last_deleted_arv_num + 1;
      last_arv_num_to_delete = log_Gl.hdr.nxarv_num - num_remove_arv_num;

      if (log_Gl.hdr.last_arv_num_for_syscrashes != -1)
	{
	  last_arv_num_to_delete =
	    MIN (last_arv_num_to_delete,
		 log_Gl.hdr.last_arv_num_for_syscrashes);
	}

      if (max_count > 0)
	{
	  /* check max count for deletion */
	  last_arv_num_to_delete =
	    MIN (last_arv_num_to_delete, first_arv_num_to_delete + max_count);
	}

      last_arv_num_to_delete--;
      if (last_arv_num_to_delete >= first_arv_num_to_delete)
	{
	  log_Gl.hdr.last_deleted_arv_num = last_arv_num_to_delete;
	  logpb_flush_header (thread_p);	/* to get rid of archives */
	}
    }

  LOG_CS_EXIT ();

  if (last_arv_num_to_delete >= 0
      && last_arv_num_to_delete >= first_arv_num_to_delete)
    {
      catmsg = msgcat_message (MSGCAT_CATALOG_RYE,
			       MSGCAT_SET_LOG,
			       MSGCAT_LOG_MAX_ARCHIVES_HAS_BEEN_EXCEEDED);
      if (catmsg == NULL)
	{
	  catmsg = "Number of active log archives has been exceeded"
	    " the max desired number.";
	}
      deleted_count = logpb_remove_archive_logs_internal (thread_p,
							  first_arv_num_to_delete,
							  last_arv_num_to_delete,
							  catmsg);
    }

  return deleted_count;
}

/*
 * logpb_remove_archive_logs - Remove all unactive log archives
 *
 * return: nothing
 *
 *   info_reason(in):
 *
 * NOTE: Archive that are not needed for system crashes are removed.
 *       That these archives may be needed for media crash recovery.
 *       Therefore, it is important that the user copy these archives
 *       to tape. Check the log information file.
 */
void
logpb_remove_archive_logs (THREAD_ENTRY * thread_p, const char *info_reason)
{
#if !defined(SERVER_MODE)
  LOG_LSA flush_upto_lsa;	/* Flush data pages up to LSA */
#endif /* !SERVER_MODE */
  LOG_LSA newflush_upto_lsa;	/* Next to be flush           */
  int first_deleted_arv_num;
  int last_deleted_arv_num;

  assert (LOG_CS_OWN_WRITE_MODE (thread_p));

  /* Close any log archives that are opened */
  if (log_Gl.archive.vdes != NULL_VOLDES)
    {
      assert (LOG_CS_OWN_WRITE_MODE (thread_p)
	      || (LOG_CS_OWN_READ_MODE (thread_p)
		  && LOG_ARCHIVE_CS_OWN_WRITE_MODE (thread_p)));

      fileio_dismount (thread_p, log_Gl.archive.vdes);
      log_Gl.archive.vdes = NULL_VOLDES;
    }

#if defined(SERVER_MODE)
  LSA_COPY (&newflush_upto_lsa, &log_Gl.flushed_lsa_lower_bound);
#else /* SERVER_MODE */

  flush_upto_lsa.pageid = LOGPB_NEXT_ARCHIVE_PAGE_ID;
  flush_upto_lsa.offset = NULL_OFFSET;

  pgbuf_flush_checkpoint (thread_p, &flush_upto_lsa, NULL,
			  &newflush_upto_lsa);

  if ((!LSA_ISNULL (&newflush_upto_lsa)
       && LSA_LT (&newflush_upto_lsa, &flush_upto_lsa))
      || fileio_synchronize_all (thread_p, false) != NO_ERROR)
    {
      /* Cannot remove the archives at this moment */
      return;
    }

  if (log_Gl.run_nxchkpt_atpageid != NULL_PAGEID)
    {
      if (LSA_LT (&log_Gl.hdr.chkpt_lsa, &flush_upto_lsa))
	{
	  /*
	   * Reset the checkpoint record to the first possible active page and
	   * flush the log header before the archives are removed
	   */
	  LSA_COPY (&log_Gl.hdr.chkpt_lsa, &flush_upto_lsa);
	  logpb_flush_header (thread_p);
	}
    }
#endif /* SERVER_MODE */

  last_deleted_arv_num = log_Gl.hdr.last_arv_num_for_syscrashes;
  if (last_deleted_arv_num == -1)
    {
      last_deleted_arv_num = log_Gl.hdr.nxarv_num;
    }

  last_deleted_arv_num--;

  if (log_Gl.hdr.last_deleted_arv_num + 1 > last_deleted_arv_num)
    {
      /* Nothing to remove */
      return;
    }

  first_deleted_arv_num = log_Gl.hdr.last_deleted_arv_num + 1;
  if (last_deleted_arv_num >= 0)
    {
      logpb_remove_archive_logs_internal (thread_p, first_deleted_arv_num,
					  last_deleted_arv_num, info_reason);

      log_Gl.hdr.last_deleted_arv_num = last_deleted_arv_num;
      logpb_flush_header (thread_p);	/* to get rid of archives */
    }
}

/*
 * logpb_add_archive_page_info -
 *
 * return: 0
 *
 *   thread_p(in)  :
 *   arv_num(in)   :
 *   start_page(in):
 *   end_page(in)  :
 *
 * NOTE:
 */
static int
logpb_add_archive_page_info (UNUSED_ARG THREAD_ENTRY * thread_p,
			     int arv_num, LOG_PAGEID start_page,
			     LOG_PAGEID end_page)
{
  int rear;

  assert (LOG_CS_OWN (NULL));

  rear = logpb_Arv_page_info_table.rear;

  rear = (rear + 1) % ARV_PAGE_INFO_TABLE_SIZE;

  if (logpb_Arv_page_info_table.item_count < ARV_PAGE_INFO_TABLE_SIZE)
    {
      logpb_Arv_page_info_table.item_count++;
    }

  logpb_Arv_page_info_table.rear = rear;
  logpb_Arv_page_info_table.page_info[rear].arv_num = arv_num;
  logpb_Arv_page_info_table.page_info[rear].start_pageid = start_page;
  logpb_Arv_page_info_table.page_info[rear].end_pageid = end_page;

  return 0;
}

/*
 * logpb_get_archive_num_from_info_table -
 *
 * return: archive_number or -1
 *
 *   thread_p(in)  :
 *   page_id(in)   :
 *
 * NOTE:
 */
static int
logpb_get_archive_num_from_info_table (UNUSED_ARG THREAD_ENTRY * thread_p,
				       LOG_PAGEID page_id)
{
  int i, count;

  assert (LOG_CS_OWN (NULL));

  for (i = logpb_Arv_page_info_table.rear, count = 0;
       count < logpb_Arv_page_info_table.item_count;
       i = ((i == 0) ? ARV_PAGE_INFO_TABLE_SIZE - 1 : i - 1), count++)
    {
      if (logpb_Arv_page_info_table.page_info[i].start_pageid <= page_id
	  && page_id <= logpb_Arv_page_info_table.page_info[i].end_pageid)
	{
	  return logpb_Arv_page_info_table.page_info[i].arv_num;
	}
    }

  return -1;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * logpb_get_remove_archive_num -
 *
 * return:
 *
 *   safe_pageid(in):
 *   archive_num(in):
 *
 * NOTE:
 */
static int
logpb_get_remove_archive_num (THREAD_ENTRY * thread_p, LOG_PAGEID safe_pageid,
			      int archive_num)
{
  struct log_arv_header *arvhdr;
  char arv_name[PATH_MAX];
  char arv_hdr_pgbuf[IO_MAX_PAGE_SIZE + MAX_ALIGNMENT],
    *aligned_arv_hdr_pgbuf;
  LOG_PAGE *arv_hdr_pgptr;
  int vdes, arv_num;

  assert (LOG_CS_OWN (NULL));

  arv_num = logpb_get_archive_num_from_info_table (thread_p, safe_pageid);

  if (arv_num >= 0)
    {
      /* find the largest number that can remove */
      archive_num = arv_num - 1;
    }

  aligned_arv_hdr_pgbuf = PTR_ALIGN (arv_hdr_pgbuf, MAX_ALIGNMENT);
  arv_hdr_pgptr = (LOG_PAGE *) aligned_arv_hdr_pgbuf;

  while (archive_num >= 0)
    {
      fileio_make_log_archive_name (arv_name, log_Archive_path,
				    log_Prefix, archive_num);
      /* open the archive file */
      if (logpb_is_archive_available (archive_num) == true
	  && fileio_is_volume_exist (arv_name) == true
	  && ((vdes = fileio_mount (thread_p, log_Db_fullname, arv_name,
				    LOG_DBLOG_ARCHIVE_VOLID, false, false))
	      != NULL_VOLDES))
	{
	  if (fileio_read (thread_p, vdes, arv_hdr_pgptr, 0, LOG_PAGESIZE)
	      == NULL)
	    {
	      fileio_dismount (thread_p, vdes);
	      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOG_READ, 3,
		      0, 0, fileio_get_base_file_name (arv_name));
	      return -1;
	    }
	  fileio_dismount (thread_p, vdes);

	  arvhdr = (struct log_arv_header *) arv_hdr_pgptr->area;
	  if (safe_pageid > arvhdr->fpageid + arvhdr->npages)
	    {
	      break;
	    }
	}

      vdes = 0;
      archive_num--;
    }

  return archive_num;
}
#endif

/*
 * log_remove_archive_logs_internal - Remove all unactive log archives
 *
 * return: nothing
 *
 *   first(in): first archive to be deleted
 *   last(in): last archive to be deleted
 *   info_reason(in): message describing the reason the archives are being deleted
 *   check_backup(in): when true, avoid deleting archives needed for a restore
 *
 * NOTE: This routine does the actual deletion and notifies the user
 *   via the lginf file.  This routine does not do any logical verification
 *   to insure that the archives are no longer needed, so the caller must be
 *   explicitly careful and insure that there are no circumstances under
 *   which those archives could possibly be needed before they are deleted.
 *   Common reasons why they might be needed include a media crash or a
 *   restore from a fuzzy backup.
 */
static int
logpb_remove_archive_logs_internal (THREAD_ENTRY * thread_p, int first,
				    int last, const char *info_reason)
{
  char logarv_name[PATH_MAX];
  int i;
  bool append_log_info = false;
  int deleted_count = 0;

  for (i = first; i <= last; i++)
    {
      fileio_make_log_archive_name (logarv_name, log_Archive_path, log_Prefix,
				    i);
#if defined(SERVER_MODE)
      if (boot_Server_status == BOOT_SERVER_UP)
	{
	  fileio_unformat_and_rename (thread_p, logarv_name,
				      log_Name_removed_archive);
	}
      else
	{
	  fileio_unformat (thread_p, logarv_name);
	}
#else
      fileio_unformat (thread_p, logarv_name);
#endif
      append_log_info = true;
      deleted_count++;
    }

  if (append_log_info)
    {
      logpb_append_archives_removed_to_log_info (first, last, info_reason);
    }

  return deleted_count;
}

/*
 * logpb_append_archives_removed_to_log_info - Record deletion of one or more archives
 *
 * return: nothing
 *
 *   first(in): first archive to be deleted
 *   last(in): last archive to be deleted
 *   info_reason(in): message describing the reason the archives are being deleted
 *
 * NOTE: This routine makes an entry into the loginfo file that the
 *   given log archives have been removed.
 */
static void
logpb_append_archives_removed_to_log_info (int first, int last,
					   const char *info_reason)
{
  const char *catmsg;
  char logarv_name[PATH_MAX];
  char logarv_name_first[PATH_MAX];
  int error_code;

  if (info_reason != NULL)
    {
      catmsg = msgcat_message (MSGCAT_CATALOG_RYE,
			       MSGCAT_SET_LOG,
			       MSGCAT_LOG_LOGINFO_REMOVE_REASON);
      if (catmsg == NULL)
	{
	  catmsg = "REMOVE: %d %s to %d %s.\nREASON: %s\n";
	}

      fileio_make_log_archive_name (logarv_name, log_Archive_path, log_Prefix,
				    last);
      if (first == last)
	{
	  error_code =
	    log_dump_log_info (log_Name_info, true, catmsg,
			       first,
			       fileio_get_base_file_name (logarv_name), last,
			       fileio_get_base_file_name (logarv_name),
			       info_reason);
	}
      else
	{
	  fileio_make_log_archive_name (logarv_name_first, log_Archive_path,
					log_Prefix, first);
	  error_code =
	    log_dump_log_info (log_Name_info, true, catmsg,
			       first,
			       fileio_get_base_file_name (logarv_name_first),
			       last, fileio_get_base_file_name (logarv_name),
			       info_reason);
	}
      if (error_code != NO_ERROR && error_code != ER_LOG_MOUNT_FAIL)
	{
	  return;
	}
    }
}

/*
 *
 *       	   FUNCTIONS RELATED TO MISCELANEOUS I/O
 *
 */

/*
 * logpb_copy_from_log: Copy a portion of the log
 *
 * arguments:
 *         area: Area where the portion of the log is copied.
 *               (Set as a side effect)
 *  area_length: the length to copy
 *   log_lsa: log address of the log data to copy
 *               (May be set as a side effect)
 *    log_pgptr: the buffer containing the log page
 *               (May be set as a side effect)
 *
 * returns/side-effects: nothing
 *    area is set as a side effect.
 *    log_lsa, and log_pgptr are set as a side effect.
 *
 * description: Copy "length" bytes of the log starting at log_lsa->pageid,
 *              log_offset onto the given area.
 * NOTE:        The location of the log is updated to point to the end of the
 *              data.
 */
void
logpb_copy_from_log (THREAD_ENTRY * thread_p, char *area, int length,
		     LOG_LSA * log_lsa, LOG_PAGE * log_page_p)
{
  int copy_length;		/* Length to copy into area */
  int area_offset;		/* The area offset          */

  /*
   * If the data is contained in only one buffer, copy the data in one
   * statement, otherwise, copy it in parts
   */

  if (log_lsa->offset + length < (int) LOGAREA_SIZE)
    {
      /* The log data is contiguos */
      memcpy (area, (char *) log_page_p->area + log_lsa->offset, length);
      log_lsa->offset += length;
    }
  else
    {
      /* The log data is not contiguos */
      area_offset = 0;
      while (length > 0)
	{
	  LOG_READ_ADVANCE_WHEN_DOESNT_FIT (thread_p, 0, log_lsa, log_page_p);
	  if (log_lsa->offset + length < (int) LOGAREA_SIZE)
	    {
	      copy_length = length;
	    }
	  else
	    {
	      copy_length = LOGAREA_SIZE - (int) (log_lsa->offset);
	    }
	  memcpy (area + area_offset,
		  (char *) log_page_p->area + log_lsa->offset, copy_length);
	  length -= copy_length;
	  area_offset += copy_length;
	  log_lsa->offset += copy_length;
	}
    }
}

/*
 * logpb_verify_length - Verify db and log lengths
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   db_fullname(in): Full name of the database
 *   log_path(in): Directory where the log volumes reside
 *   log_prefix(in): Name of the log volumes. It is usually set as database
 *                   name. For example, if the value is equal to "db", the
 *                   names of the log volumes created are as follow:
 *                      Active_log      = db_logactive
 *                      Archive_logs    = db_logarchive.0
 *                                        db_logarchive.1
 *                                             .
 *                                             .
 *                                             .
 *                                        db_logarchive.n
 *                      Log_information = db_loginfo
 *                      Database Backup = db_backup
 *
 * NOTE: Make sure that any database name will not be overpassed.
 */
static int
logpb_verify_length (const char *db_fullname, const char *log_path,
		     const char *log_prefix)
{
  int volmax_suffix;
  int length;
  const char *dbname;
  long int filename_max;
  long int pathname_max;

  volmax_suffix = fileio_get_volume_max_suffix ();

  dbname = fileio_get_base_file_name (db_fullname);
  if (fileio_get_max_name (db_fullname, &filename_max, &pathname_max) < 0)
    {
      return ER_FAILED;
    }

  if (pathname_max > DB_MAX_PATH_LENGTH)
    {
      pathname_max = DB_MAX_PATH_LENGTH;
    }

  /*
   * Make sure that names of volumes (information and log volumes), that is,
   * OS files will not exceed the maximum allowed value.
   */

  if ((int) (strlen (dbname) + 1 + volmax_suffix) > filename_max)
    {
      /* The name of the volume is too long */
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_LOG_NOFULL_DATABASE_NAME_IS_TOO_LONG, 2,
	      dbname, filename_max - volmax_suffix - 1);
      return ER_LOG_NOFULL_DATABASE_NAME_IS_TOO_LONG;
    }

  if ((int) (strlen (log_prefix) + 1) > filename_max
      || (int) (strlen (log_prefix) + 1) > MAXLOGNAME)
    {
      /* Bad prefix log name */
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_LOG_PREFIX_NAME_IS_TOO_LONG, 2,
	      log_prefix, ((MAXLOGNAME > filename_max)
			   ? filename_max - 1 : MAXLOGNAME - 1));
      return ER_LOG_PREFIX_NAME_IS_TOO_LONG;
    }

  /*
   * Make sure that the length for the volume is OK
   */

  if ((int) (strlen (db_fullname) + 1 + volmax_suffix) > pathname_max)
    {
      /* The full name of the database is too long */
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_BO_FULL_DATABASE_NAME_IS_TOO_LONG, 3, NULL, db_fullname,
	      strlen (db_fullname) + 1, pathname_max);
      return ER_BO_FULL_DATABASE_NAME_IS_TOO_LONG;
    }

  /*
   * First create a new log for the new database. This log is not a copy of
   * of the old log; it is a newly created one.
   */

  if (log_path != NULL)
    {
      length = strlen (log_path) + strlen (log_prefix) + 2;
    }
  else
    {
      length = strlen (log_prefix) + 1;
    }

  if (length + volmax_suffix > pathname_max)
    {
      /*
       * Database name is too long.
       * Path + prefix < pathname_max - 2
       */
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOG_NAME_IS_TOO_LONG,
	      3, log_path, log_prefix, pathname_max - 2);
      return ER_LOG_NAME_IS_TOO_LONG;
    }

  return NO_ERROR;
}

/*
 * logpb_initialize_log_names - Initialize the names of log volumes and files
 *
 * return: nothing
 *
 *   db_fullname(in): Full name of the database
 *   logpath(in): Directory where the log volumes reside
 *   prefix_logname(in): Name of the log volumes. It is usually set as database
 *                      name. For example, if the value is equal to "db", the
 *                      names of the log volumes created are as follow:
 *                      Active_log      = db_logactive
 *                      Archive_logs    = db_logarchive.0
 *                                        db_logarchive.1
 *                                             .
 *                                             .
 *                                             .
 *                                        db_logarchive.n
 *                      Log_information = db_loginfo
 *                      Database Backup = db_backup
 *
 * NOTE:Initialize name of log volumes and files
 */
int
logpb_initialize_log_names (THREAD_ENTRY * thread_p, const char *db_fullname,
			    const char *logpath, const char *prefix_logname)
{
  int error_code = NO_ERROR;

  error_code = logpb_verify_length (db_fullname, logpath, prefix_logname);
  if (error_code != NO_ERROR)
    {
      /* Names are too long */
      logpb_fatal_error (thread_p, true, ARG_FILE_LINE,
			 "logpb_initialize_log_names");

      return error_code;
    }

  /* Save the log Path */

  if (logpath != NULL)
    {
      strcpy (log_Path, logpath);
    }
  else
    {
      strcpy (log_Path, ".");
    }

  strcpy (log_Archive_path, log_Path);

  /* Save the log Prefix */
  STRNCPY (log_Prefix, prefix_logname, PATH_MAX);

  /*
   * Build Name of log active
   */
  fileio_make_log_active_name (log_Name_active, log_Path, log_Prefix);
  fileio_make_log_info_name (log_Name_info, log_Path, log_Prefix);
  fileio_make_volume_info_name (log_Name_volinfo, db_fullname);
  fileio_make_log_archive_temp_name (log_Name_bg_archive, log_Archive_path,
				     log_Prefix);
  fileio_make_removed_log_archive_name (log_Name_removed_archive,
					log_Archive_path, log_Prefix);
  log_Db_fullname = db_fullname;

  return error_code;
}

/*
 * logpb_exist_log - Find if given log exists
 *
 * return:
 *
 *   db_fullname(in): Full name of the database
 *   logpath(in): Directory where the log volumes reside
 *   prefix_logname(in): Name of the log volumes. It is usually set as database
 *                   name. For example, if the value is equal to "db", the
 *                   names of the log volumes created are as follow:
 *                      Active_log      = db_logactive
 *                      Archive_logs    = db_logarchive.0
 *                                        db_logarchive.1
 *                                             .
 *                                             .
 *                                             .
 *                                        db_logarchive.n
 *                      Log_information = db_loginfo
 *                      Database Backup = db_backup
 *
 * NOTE:Find if the log associated with the aboive database exists.
 */
bool
logpb_exist_log (THREAD_ENTRY * thread_p, const char *db_fullname,
		 const char *logpath, const char *prefix_logname)
{
  /* Is the system restarted ? */
  if (!logpb_is_initialize_pool ())
    {
      if (logpb_initialize_log_names (thread_p, db_fullname, logpath,
				      prefix_logname) != NO_ERROR)
	{
	  return false;
	}
    }

  return fileio_is_volume_exist (log_Name_active);
}

#if defined(SERVER_MODE)
/*
 * logpb_do_checkpoint -
 *
 * return:
 *
 * NOTE:
 */
void
logpb_do_checkpoint (void)
{
  thread_wakeup_checkpoint_thread ();
}
#endif /* SERVER_MODE */

/*
 * logpb_checkpoint - Execute a fuzzy checkpoint
 *
 * return: pageid where a redo will start
 *
 */
LOG_PAGEID
logpb_checkpoint (THREAD_ENTRY * thread_p)
{
  LOG_TDES *tdes;		/* System transaction descriptor */
  LOG_TDES *act_tdes;		/* Transaction descriptor of an
				 * active transaction
				 */
  struct log_chkpt *chkpt, tmp_chkpt;	/* Checkpoint log records       */
  struct log_chkpt_trans *chkpt_trans;	/* Checkpoint tdes              */
  struct log_chkpt_trans *chkpt_one;	/* Checkpoint tdes for one tran */
  struct log_chkpt_topops_commit_posp *chkpt_topops;	/* Checkpoint top system
							 * operations that are in
							 * commit postpone mode
							 */
  struct log_chkpt_topops_commit_posp *chkpt_topone;	/* One top system ope   */
  LOG_LSA chkpt_lsa;		/* copy of log_Gl.hdr.chkpt_lsa */
  LOG_LSA chkpt_redo_lsa;	/* copy of log_Gl.chkpt_redo_lsa */
  LOG_LSA newchkpt_lsa;		/* New address of the checkpoint
				 * record
				 */
  LOG_LSA smallest_lsa;
  unsigned int nobj_locks;	/* Avg number of locks          */
  char logarv_name[PATH_MAX];	/* Archive name       */
  char logarv_name_first[PATH_MAX];	/* Archive name */
  int ntrans;			/* Number of trans              */
  int ntops;			/* Number of total active top
				 * actions
				 */
  int length_all_chkpt_trans;
  size_t length_all_tops = 0;
  int i, j;
  const char *catmsg;
  int num_perm_vols;
  VOLID volid;
  UNUSED_VAR int rv;
  int error_code = NO_ERROR;
  LOG_PAGEID smallest_pageid;
  int first_arv_num_not_needed;
  int last_arv_num_not_needed;
  LOG_PRIOR_NODE *node;
  void *ptr;
  int save_tran_index = NULL_TRAN_INDEX;

  save_tran_index = logtb_get_current_tran_index (thread_p);
  logtb_set_to_system_tran_index (thread_p);

  LOG_CS_ENTER (thread_p);

#if defined(SERVER_MODE)
  if (BO_IS_SERVER_RESTARTED () && log_Gl.run_nxchkpt_atpageid == NULL_PAGEID)
    {
      LOG_CS_EXIT ();

      logtb_set_current_tran_index (thread_p, save_tran_index);
      return NULL_PAGEID;
    }

  /*
   * Critical section is entered several times to allow other transaction to
   * use the log manger
   */
#endif /* SERVER_MODE */

  mnt_stats_event_on (thread_p, MNT_STATS_LOG_CHECKPOINTS);

  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_LOG_CHECKPOINT_STARTED,
	  2, log_Gl.hdr.chkpt_lsa.pageid, log_Gl.chkpt_redo_lsa.pageid);
  er_log_debug (ARG_FILE_LINE, "start checkpoint\n");


  /*
   * Indicate that the checkpoint process is running. Don't run another one,
   * until we are done with the present one.
   */
  log_Gl.run_nxchkpt_atpageid = NULL_PAGEID;

  tdes = logtb_get_current_tdes (thread_p);
  if (tdes == NULL)
    {
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_LOG_UNKNOWN_TRANINDEX, 1,
	      logtb_get_current_tran_index (thread_p));

      LOG_CS_EXIT ();

      mnt_stats_event_off (thread_p, MNT_STATS_LOG_CHECKPOINTS);
      logtb_set_current_tran_index (thread_p, save_tran_index);
      return NULL_PAGEID;
    }

  /*
   * FLUSH all append LOG PAGES and flush all DIRTY DATA PAGES whose LSA
   * are SMALLER OR EQUAL than newchkpt_lsa value and find the next redo
   * point.
   */

  rv = pthread_mutex_lock (&log_Gl.chkpt_lsa_lock);
  LSA_COPY (&chkpt_lsa, &log_Gl.hdr.chkpt_lsa);
  LSA_COPY (&chkpt_redo_lsa, &log_Gl.chkpt_redo_lsa);
  pthread_mutex_unlock (&log_Gl.chkpt_lsa_lock);

  logpb_flush_pages_direct (thread_p);

  /* MARK THE CHECKPOINT PROCESS */
  node = prior_lsa_alloc_and_copy_data (thread_p,
					LOG_START_CHKPT,
					RV_NOT_DEFINED, NULL,
					0, NULL, 0, NULL);
  if (node == NULL)
    {
      LOG_CS_EXIT ();

      mnt_stats_event_off (thread_p, MNT_STATS_LOG_CHECKPOINTS);
      logtb_set_current_tran_index (thread_p, save_tran_index);
      return NULL_PAGEID;
    }

  prior_lsa_next_record (thread_p, node, tdes);

  /*
   * Modify log header to record present checkpoint. The header is flushed
   * later
   */

  LSA_COPY (&newchkpt_lsa, &tdes->last_lsa);

  LOG_CS_EXIT ();

  er_log_debug (ARG_FILE_LINE,
		"logpb_checkpoint: call pgbuf_flush_checkpoint()\n");
  if (pgbuf_flush_checkpoint (thread_p, &newchkpt_lsa, &chkpt_redo_lsa,
			      &tmp_chkpt.redo_lsa) != NO_ERROR)
    {
      LOG_CS_ENTER (thread_p);
      goto exit_on_error;
    }

  er_log_debug (ARG_FILE_LINE,
		"logpb_checkpoint: call fileio_synchronize_all()\n");
  if (fileio_synchronize_all (thread_p, false) != NO_ERROR)
    {
      LOG_CS_ENTER (thread_p);
      goto exit_on_error;
    }

  LOG_CS_ENTER (thread_p);

  if (LSA_ISNULL (&tmp_chkpt.redo_lsa))
    {
      LSA_COPY (&tmp_chkpt.redo_lsa, &newchkpt_lsa);
    }

  assert (LSA_LE (&tmp_chkpt.redo_lsa, &newchkpt_lsa));

#if defined(SERVER_MODE)
  /* Save lower bound of flushed lsa */
  if (!LSA_ISNULL (&tmp_chkpt.redo_lsa))
    {
      if (LSA_ISNULL (&log_Gl.flushed_lsa_lower_bound)
	  || LSA_GT (&tmp_chkpt.redo_lsa, &log_Gl.flushed_lsa_lower_bound))
	LSA_COPY (&log_Gl.flushed_lsa_lower_bound, &tmp_chkpt.redo_lsa);
    }
#endif /* SERVER_MODE */

  TR_TABLE_LOCK (thread_p);

  /* allocate memory space for the transaction descriptors */
  tmp_chkpt.ntrans = log_Gl.trantable.num_assigned_indices;
  length_all_chkpt_trans = sizeof (*chkpt_trans) * tmp_chkpt.ntrans;

  chkpt_trans = (struct log_chkpt_trans *) malloc (length_all_chkpt_trans);
  if (chkpt_trans == NULL)
    {
      TR_TABLE_UNLOCK (thread_p);
      goto exit_on_error;
    }

  rv = pthread_mutex_lock (&log_Gl.prior_info.prior_lsa_mutex);

  /* CHECKPOINT THE TRANSACTION TABLE */

  LSA_SET_NULL (&smallest_lsa);
  for (i = 0, ntrans = 0, ntops = 0;
       i < log_Gl.trantable.num_total_indices; i++)
    {
      /*
       * Don't checkpoint current system transaction. That is, the one of
       * checkpoint process
       */
      if (i == LOG_SYSTEM_TRAN_INDEX)
	{
	  continue;
	}
      act_tdes = LOG_FIND_TDES (i);
      if (act_tdes != NULL && act_tdes->trid != NULL_TRANID
	  && !LSA_ISNULL (&act_tdes->last_lsa))
	{
	  assert (ntrans < tmp_chkpt.ntrans);

	  chkpt_one = &chkpt_trans[ntrans];
	  chkpt_one->trid = act_tdes->trid;
	  chkpt_one->state = act_tdes->state;
	  LSA_COPY (&chkpt_one->begin_lsa, &act_tdes->begin_lsa);
	  LSA_COPY (&chkpt_one->last_lsa, &act_tdes->last_lsa);
	  if (chkpt_one->state == TRAN_UNACTIVE_ABORTED)
	    {
	      /*
	       * Transaction is in the middle of an abort, since rollback does
	       * is not run in a critical section. Set the undo point to be the
	       * same as its tail. The recovery process will read the last
	       * record which is likely a compensating one, and find where to
	       * continue a rollback operation.
	       */
	      LSA_COPY (&chkpt_one->undo_nxlsa, &act_tdes->last_lsa);
	    }
	  else
	    {
	      LSA_COPY (&chkpt_one->undo_nxlsa, &act_tdes->undo_nxlsa);
	    }

	  LSA_COPY (&chkpt_one->posp_nxlsa, &act_tdes->posp_nxlsa);
	  LSA_COPY (&chkpt_one->savept_lsa, &act_tdes->savept_lsa);
	  LSA_COPY (&chkpt_one->tail_topresult_lsa,
		    &act_tdes->tail_topresult_lsa);
	  STRNCPY (chkpt_one->user_name, act_tdes->client.db_user,
		   LOG_USERNAME_MAX);
	  ntrans++;
	  if (act_tdes->topops.last >= 0
	      && (act_tdes->state ==
		  TRAN_UNACTIVE_TOPOPE_COMMITTED_WITH_POSTPONE))
	    {
	      ntops += act_tdes->topops.last + 1;
	    }

	  if (LSA_ISNULL (&smallest_lsa)
	      || LSA_GT (&smallest_lsa, &act_tdes->begin_lsa))
	    {
	      LSA_COPY (&smallest_lsa, &act_tdes->begin_lsa);
	    }
	}
    }

  /*
   * Reset the structure to the correct number of transactions and
   * recalculate the length
   */
  tmp_chkpt.ntrans = ntrans;
  length_all_chkpt_trans = sizeof (*chkpt_trans) * tmp_chkpt.ntrans;

  /*
   * Scan again if there were any top system operations in the process of
   * being committed.
   * NOTE that we checkpoint top system operations only when there are in the
   * process of commit. Not knownledge of top system operations that are not
   * in the process of commit is required since if there is a crash, the system
   * operation is aborted as part of the transaction.
   */

  chkpt_topops = NULL;
  if (ntops > 0)
    {
      tmp_chkpt.ntops = log_Gl.trantable.num_assigned_indices;
      length_all_tops = sizeof (*chkpt_topops) * tmp_chkpt.ntops;
      chkpt_topops =
	(struct log_chkpt_topops_commit_posp *) malloc (length_all_tops);
      if (chkpt_topops == NULL)
	{
	  free_and_init (chkpt_trans);
	  pthread_mutex_unlock (&log_Gl.prior_info.prior_lsa_mutex);
	  TR_TABLE_UNLOCK (thread_p);
	  goto exit_on_error;
	}

      /* CHECKPOINTING THE TOP ACTIONS */
      for (i = 0, ntrans = 0, ntops = 0;
	   i < log_Gl.trantable.num_total_indices; i++)
	{
	  /*
	   * Don't checkpoint current system transaction. That is, the one of
	   * checkpoint process
	   */
	  if (i == LOG_SYSTEM_TRAN_INDEX)
	    {
	      continue;
	    }

	  act_tdes = LOG_FIND_TDES (i);
	  if (act_tdes != NULL && act_tdes->trid != NULL_TRANID)
	    {
	      for (j = 0; j < act_tdes->topops.last + 1; j++)
		{
		  switch (act_tdes->state)
		    {
		    case TRAN_UNACTIVE_TOPOPE_COMMITTED_WITH_POSTPONE:
		      if (ntops >= tmp_chkpt.ntops)
			{
			  tmp_chkpt.ntops +=
			    log_Gl.trantable.num_assigned_indices;
			  length_all_tops =
			    sizeof (*chkpt_topops) * tmp_chkpt.ntops;
			  ptr = realloc (chkpt_topops, length_all_tops);
			  if (ptr == NULL)
			    {
			      free_and_init (chkpt_trans);
			      pthread_mutex_unlock (&log_Gl.prior_info.
						    prior_lsa_mutex);
			      TR_TABLE_UNLOCK (thread_p);
			      goto exit_on_error;
			    }
			  chkpt_topops =
			    (struct log_chkpt_topops_commit_posp *) ptr;
			}

		      chkpt_topone = &chkpt_topops[ntops];
		      chkpt_topone->trid = act_tdes->trid;
		      LSA_COPY (&chkpt_topone->lastparent_lsa,
				&act_tdes->topops.stack[j].lastparent_lsa);
		      LSA_COPY (&chkpt_topone->posp_lsa,
				&act_tdes->topops.stack[j].posp_lsa);
		      ntops++;
		      break;
		    default:
		      continue;
		    }
		}
	    }
	}
    }

  assert (sizeof (*chkpt_topops) * ntops <= length_all_tops);
  tmp_chkpt.ntops = ntops;
  length_all_tops = sizeof (*chkpt_topops) * tmp_chkpt.ntops;

  node = prior_lsa_alloc_and_copy_data (thread_p, LOG_END_CHKPT,
					RV_NOT_DEFINED, NULL,
					length_all_chkpt_trans,
					(char *) chkpt_trans,
					length_all_tops,
					(char *) chkpt_topops);
  if (node == NULL)
    {
      free_and_init (chkpt_trans);

      if (chkpt_topops != NULL)
	{
	  free_and_init (chkpt_topops);
	}
      pthread_mutex_unlock (&log_Gl.prior_info.prior_lsa_mutex);
      TR_TABLE_UNLOCK (thread_p);

      LOG_CS_EXIT ();

      mnt_stats_event_off (thread_p, MNT_STATS_LOG_CHECKPOINTS);
      logtb_set_current_tran_index (thread_p, save_tran_index);
      return NULL_PAGEID;
    }

  chkpt = (struct log_chkpt *) node->data_header;
  *chkpt = tmp_chkpt;

  prior_lsa_next_record_with_lock (thread_p, node, tdes);

  pthread_mutex_unlock (&log_Gl.prior_info.prior_lsa_mutex);

  TR_TABLE_UNLOCK (thread_p);

  free_and_init (chkpt_trans);

  /* Any topops to log ? */
  if (chkpt_topops != NULL)
    {
      free_and_init (chkpt_topops);
    }

  /*
   * END append
   * Flush the page since we are going to flush the log header which
   * reflects the new location of the last checkpoint log record
   */
  logpb_flush_pages_direct (thread_p);
  er_log_debug (ARG_FILE_LINE,
		"logpb_checkpoint: call logpb_flush_all_append_pages()\n");

  /*
   * Flush the log data header and update all checkpoints in volumes to
   * point to new checkpoint
   */

  /* Average the number of active transactions and locks */
  nobj_locks = lock_get_number_object_locks ();
  log_Gl.hdr.avg_ntrans = (log_Gl.hdr.avg_ntrans + ntrans) >> 1;
  log_Gl.hdr.avg_nlocks = (log_Gl.hdr.avg_nlocks + nobj_locks) >> 1;

  /* Flush the header */
  rv = pthread_mutex_lock (&log_Gl.chkpt_lsa_lock);
  if (LSA_LT (&log_Gl.hdr.chkpt_lsa, &newchkpt_lsa))
    {
      LSA_COPY (&log_Gl.hdr.chkpt_lsa, &newchkpt_lsa);
    }
  LSA_COPY (&chkpt_lsa, &log_Gl.hdr.chkpt_lsa);

  if (LSA_ISNULL (&smallest_lsa))
    {
      LSA_COPY (&log_Gl.hdr.smallest_lsa_at_last_chkpt,
		&log_Gl.hdr.chkpt_lsa);
    }
  else
    {
      LSA_COPY (&log_Gl.hdr.smallest_lsa_at_last_chkpt, &smallest_lsa);
    }
  LSA_COPY (&log_Gl.chkpt_redo_lsa, &tmp_chkpt.redo_lsa);

  pthread_mutex_unlock (&log_Gl.chkpt_lsa_lock);

  er_log_debug (ARG_FILE_LINE,
		"logpb_checkpoint: call logpb_flush_header()\n");
  logpb_flush_header (thread_p);

  /*
   * Exit from the log critical section since we are going to call another
   * module which may be blocked on a lock.
   */

  LOG_CS_EXIT ();

  /*
   * Record the checkpoint address on every volume header
   */

  num_perm_vols = xboot_find_number_permanent_volumes (thread_p);

  for (volid = LOG_DBFIRST_VOLID; volid < num_perm_vols; volid++)
    {
      (void) disk_set_checkpoint (thread_p, volid, &chkpt_lsa);
    }

  /*
   * Get the critical section again, so we can check if any archive can be
   * declare as un-needed
   */

  LOG_CS_ENTER (thread_p);

  /*
   * If the log archives are not needed for any normal undos and redos,
   * indicate so. However, the log archives may be needed during media
   * crash recovery.
   */
  smallest_pageid = NULL_PAGEID;
  first_arv_num_not_needed = last_arv_num_not_needed = -1;

  if (log_Gl.hdr.last_arv_num_for_syscrashes != log_Gl.hdr.nxarv_num
      && log_Gl.hdr.last_arv_num_for_syscrashes != -1)
    {
#if defined(SERVER_MODE)
      smallest_pageid = MIN (log_Gl.flushed_lsa_lower_bound.pageid,
			     tmp_chkpt.redo_lsa.pageid);
#else
      smallest_pageid = tmp_chkpt.redo_lsa.pageid;
#endif

      if (smallest_lsa.pageid != NULL_PAGEID)
	{
	  smallest_pageid = MIN (smallest_pageid, smallest_lsa.pageid);
	}

      if (logpb_is_page_in_archive (smallest_pageid))
	{
	  int arv_num;

	  if ((logpb_fetch_from_archive (thread_p, smallest_pageid,
					 NULL, &arv_num, NULL, true) == NULL)
	      || (arv_num <= log_Gl.hdr.last_arv_num_for_syscrashes))
	    {
	      first_arv_num_not_needed = last_arv_num_not_needed = -1;
	    }
	  else
	    {
	      first_arv_num_not_needed =
		log_Gl.hdr.last_arv_num_for_syscrashes;
	      last_arv_num_not_needed = arv_num - 1;
	    }
	}
      else
	{
	  first_arv_num_not_needed = log_Gl.hdr.last_arv_num_for_syscrashes;
	  last_arv_num_not_needed = log_Gl.hdr.nxarv_num - 1;
	}

      if (first_arv_num_not_needed != -1)
	{
	  log_Gl.hdr.last_arv_num_for_syscrashes =
	    last_arv_num_not_needed + 1;

	  /* Close any log archives that are opened */
	  if (log_Gl.archive.vdes != NULL_VOLDES)
	    {
	      assert (LOG_CS_OWN_WRITE_MODE (thread_p)
		      || (LOG_CS_OWN_READ_MODE (thread_p)
			  && LOG_ARCHIVE_CS_OWN_WRITE_MODE (thread_p)));

	      fileio_dismount (thread_p, log_Gl.archive.vdes);
	      log_Gl.archive.vdes = NULL_VOLDES;
	    }

	  /* This is OK since we have already flushed the log header page */
	  if (first_arv_num_not_needed == last_arv_num_not_needed)
	    {
	      fileio_make_log_archive_name (logarv_name, log_Archive_path,
					    log_Prefix,
					    first_arv_num_not_needed);
	      catmsg = msgcat_message (MSGCAT_CATALOG_RYE,
				       MSGCAT_SET_LOG,
				       MSGCAT_LOG_LOGINFO_COMMENT_ARCHIVE_NONEEDED);
	      if (catmsg == NULL)
		{
		  catmsg =
		    "COMMENT: Log archive %s is not needed any longer"
		    " unless a database media crash occurs.\n";
		}
	      error_code =
		log_dump_log_info (log_Name_info, true,
				   catmsg,
				   fileio_get_base_file_name (logarv_name));
	    }
	  else
	    {
	      fileio_make_log_archive_name (logarv_name_first,
					    log_Archive_path, log_Prefix,
					    first_arv_num_not_needed);
	      fileio_make_log_archive_name (logarv_name, log_Archive_path,
					    log_Prefix,
					    last_arv_num_not_needed);

	      catmsg = msgcat_message (MSGCAT_CATALOG_RYE,
				       MSGCAT_SET_LOG,
				       MSGCAT_LOG_LOGINFO_COMMENT_MANY_ARCHIVES_NONEEDED);
	      if (catmsg == NULL)
		{
		  catmsg = "COMMENT: Log archives from %s to %s are not"
		    " needed any longer unless a database media crash occurs.\n";
		}
	      error_code =
		log_dump_log_info (log_Name_info, true, catmsg,
				   fileio_get_base_file_name
				   (logarv_name_first),
				   fileio_get_base_file_name (logarv_name));
	    }
	  if (error_code != NO_ERROR && error_code != ER_LOG_MOUNT_FAIL)
	    {
	      goto exit_on_error;
	    }

	  logpb_flush_header (thread_p);	/* Yes, one more time, to get rid of archives */
	}
    }

  /*
   * Checkpoint process is not running any longer. Indicate when do we expect
   * it to run.
   */

  log_Gl.run_nxchkpt_atpageid = (log_Gl.hdr.append_lsa.pageid +
				 log_Gl.chkpt_every_npages);

  LOG_CS_EXIT ();

#if 0
  /* have to sync log vol, data vol */
  fileio_synchronize_all (thread_p, true /* include_log */ );
#endif

  mnt_stats_event_off (thread_p, MNT_STATS_LOG_CHECKPOINTS);

  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_LOG_CHECKPOINT_FINISHED,
	  2, log_Gl.hdr.chkpt_lsa.pageid, log_Gl.chkpt_redo_lsa.pageid);
  er_log_debug (ARG_FILE_LINE, "end checkpoint\n");

  logtb_set_current_tran_index (thread_p, save_tran_index);

  return tmp_chkpt.redo_lsa.pageid;

  /* ******** */
exit_on_error:
  assert (LOG_CS_OWN_WRITE_MODE (thread_p));
  /* to immediately execute the next checkpoint. */
  log_Gl.run_nxchkpt_atpageid = log_Gl.hdr.append_lsa.pageid;
  LOG_CS_EXIT ();

  mnt_stats_event_off (thread_p, MNT_STATS_LOG_CHECKPOINTS);
  logtb_set_current_tran_index (thread_p, save_tran_index);
  return NULL_PAGEID;
}

/*
 * logpb_dump_checkpoint_trans - Dump checkpoint transactions
 *
 * return: nothing
 *
 *   length(in): Length to dump in bytes
 *   data(in): The data being logged
 *
 * NOTE: Dump a checkpoint transactions structure.
 */
void
logpb_dump_checkpoint_trans (FILE * out_fp, int length, void *data)
{
  int ntrans, i;
  struct log_chkpt_trans *chkpt_trans, *chkpt_one;	/* Checkpoint tdes */

  chkpt_trans = (struct log_chkpt_trans *) data;
  ntrans = length / sizeof (*chkpt_trans);

  /* Start dumping each checkpoint transaction descriptor */

  for (i = 0; i < ntrans; i++)
    {
      chkpt_one = &chkpt_trans[i];
      fprintf (out_fp,
	       "     Trid = %d, State = %s,\n"
	       "        Head_lsa = %lld|%d, Tail_lsa = %lld|%d, UndoNxtLSA = %lld|%d,\n"
	       "        Postpone_lsa = %lld|%d, Save_lsa = %lld|%d,"
	       " Tail_topresult_lsa = %lld|%d,\n"
	       "        Client_User: (name = %s)\n",
	       chkpt_one->trid, log_state_string (chkpt_one->state),
	       (long long int) chkpt_one->begin_lsa.pageid,
	       chkpt_one->begin_lsa.offset,
	       (long long int) chkpt_one->last_lsa.pageid,
	       chkpt_one->last_lsa.offset,
	       (long long int) chkpt_one->undo_nxlsa.pageid,
	       chkpt_one->undo_nxlsa.offset,
	       (long long int) chkpt_one->posp_nxlsa.pageid,
	       chkpt_one->posp_nxlsa.offset,
	       (long long int) chkpt_one->savept_lsa.pageid,
	       chkpt_one->savept_lsa.offset,
	       (long long int) chkpt_one->tail_topresult_lsa.pageid,
	       chkpt_one->tail_topresult_lsa.offset, chkpt_one->user_name);
    }
  (void) fprintf (out_fp, "\n");
}

/*
 * logpb_dump_checkpoint_topops - DUMP CHECKPOINT OF TOP SYSTEM OPERATIONS
 *
 * return: nothing
 *
 *   length(in): Length to dump in bytes
 *   data(in): The data being logged
 *
 * NOTE: Dump the checkpoint top system operation structure.
 */
void
logpb_dump_checkpoint_topops (FILE * out_fp, int length, void *data)
{
  int ntops, i;
  struct log_chkpt_topops_commit_posp *chkpt_topops;	/* Checkpoint top system
							 * operations that are in
							 * commit postpone mode
							 */
  struct log_chkpt_topops_commit_posp *chkpt_topone;	/* One top system ope   */

  chkpt_topops = (struct log_chkpt_topops_commit_posp *) data;
  ntops = length / sizeof (*chkpt_topops);

  /* Start dumping each checkpoint top system operation */

  for (i = 0; i < ntops; i++)
    {
      chkpt_topone = &chkpt_topops[i];
      fprintf (out_fp,
	       "     Trid = %d, Lastparent_lsa = %lld|%d, Postpone_lsa = %lld|%d\n",
	       chkpt_topone->trid,
	       (long long int) chkpt_topone->lastparent_lsa.pageid,
	       chkpt_topone->lastparent_lsa.offset,
	       (long long int) chkpt_topone->posp_lsa.pageid,
	       chkpt_topone->posp_lsa.offset);
    }
  (void) fprintf (out_fp, "\n");
}

/*
 * logpb_delete - Delete all log files and database backups
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   num_perm_vols(in):
 *   db_fullname(in): Full name of the database
 *   logpath(in): Directory where the log volumes reside
 *   prefix_logname(in): Name of the log volumes. It is usually set as database
 *                      name. For example, if the value is equal to "db", the
 *                      names of the log volumes created are as follow:
 *                      Active_log      = db_logactive
 *                      Archive_logs    = db_logarchive.0
 *                                        db_logarchive.1
 *                                             .
 *                                             .
 *                                             .
 *                                        db_logarchive.n
 *                      Log_information = db_loginfo
 *                      Database Backup = db_backup
 *   force_delete(in):
 *
 * NOTE:All log volumes (active, archives) and database backups that
 *              are accessible (i.e., located on disk) are removed from the
 *              system. This is a very dangerous operation since the database
 *              cannot be recovered after this operation is done. It is
 *              recommended to backup the database and put the backup on tape
 *              or outside the log and backup directories before this
 *              operation is done.
 *
 *              This function must be run offline. That is, it should not be
 *              run when there are multiusers in the system.
 */
int
logpb_delete (THREAD_ENTRY * thread_p, VOLID num_perm_vols,
	      const char *db_fullname, const char *logpath,
	      const char *prefix_logname, bool force_delete)
{
  char *vlabel;			/* Name of volume     */
  char volinfo_fullname[PATH_MAX];	/* Name of volume     */
  struct log_header disk_hdr;	/* Log header area    */
  struct log_header *loghdr;	/* Log header pointer */
  VOLID volid;
  FILE *db_volinfo_fp = NULL;
  int read_int_volid;
  int i;
  int error_code = NO_ERROR;
  char format_string[64];
  char vol_name[PATH_MAX];
  char *temp_path;
  char temp_path_buf[PATH_MAX];
  char temp_vol_fullname[PATH_MAX];

  /*
   * FIRST: Destroy data volumes of the database.
   * That is, the log, and information files are not removed at this point.
   */

  /* If the system is not restarted, read the header directly from disk */
  if (num_perm_vols < 0 || log_Gl.trantable.area == NULL
      || log_Pb.pool == NULL)
    {
      /*
       * The system is not restarted. Read the log header from disk and remove
       * the data volumes by reading the database volume information
       */

      er_clear ();
      error_code =
	logpb_initialize_log_names (thread_p, db_fullname, logpath,
				    prefix_logname);
      if (error_code != NO_ERROR)
	{
	  return error_code;
	}

      if (fileio_is_volume_exist (log_Name_active) == false
	  || (log_Gl.append.vdes =
	      fileio_mount (thread_p, db_fullname, log_Name_active,
			    LOG_DBLOG_ACTIVE_VOLID, true,
			    false)) == NULL_VOLDES)
	{
	  /* Unable to mount the active log */
	  if (er_errid () == ER_IO_MOUNT_LOCKED)
	    {
	      return ER_IO_MOUNT_LOCKED;
	    }
	  else
	    {
	      loghdr = NULL;
	    }
	}
      else
	{
	  char log_pgbuf[IO_MAX_PAGE_SIZE + MAX_ALIGNMENT];
	  char *aligned_log_pgbuf;
	  LOG_PAGE *log_pgptr;

	  aligned_log_pgbuf = PTR_ALIGN (log_pgbuf, MAX_ALIGNMENT);
	  log_pgptr = (LOG_PAGE *) aligned_log_pgbuf;

	  /* Initialize the buffer pool, so we can read the header */
	  if (log_Pb.pool == NULL)
	    {
	      error_code = logpb_initialize_pool (thread_p);
	      if (error_code != NO_ERROR)
		{
		  return error_code;
		}
	    }
	  logpb_fetch_header_with_buffer (thread_p, &disk_hdr, log_pgptr);
	  logpb_finalize_pool ();
	  fileio_dismount (thread_p, log_Gl.append.vdes);
	  log_Gl.append.vdes = NULL_VOLDES;
	  loghdr = &disk_hdr;
	  /*
	   * Make sure that the log is a log file and that it is compatible
	   * with the running database and system
	   */
	  if (rel_check_disk_compatible (&loghdr->db_version) !=
	      REL_COMPATIBLE)
	    {
	      loghdr = NULL;
	    }
	  else if (loghdr->db_iopagesize != IO_PAGESIZE
		   || loghdr->db_logpagesize != LOG_PAGESIZE)
	    {
	      /* Pagesize is incorrect,...reset it and call again... */
	      if (db_set_page_size (loghdr->db_iopagesize,
				    loghdr->db_logpagesize) != NO_ERROR)
		{
		  loghdr = NULL;
		}
	      else
		{
		  error_code =
		    logtb_define_trantable_log_latch (thread_p, -1);
		  if (error_code != NO_ERROR)
		    {
		      return error_code;
		    }
		  error_code =
		    logpb_delete (thread_p, num_perm_vols, db_fullname,
				  logpath, prefix_logname, force_delete);
		  return error_code;
		}
	    }
	}

      /*
       * DESTROY DATA VOLUMES using the database volume information since
       * the database system is not restarted.
       *
       * NOTE: only data volumes are removed, logs, and information files
       *       are not removed at this point.
       */

      fileio_make_volume_info_name (volinfo_fullname, db_fullname);
      sprintf (format_string, "%%d %%%ds", PATH_MAX - 1);

      db_volinfo_fp = fopen (volinfo_fullname, "r");
      if (db_volinfo_fp != NULL)
	{
	  while (true)
	    {
	      if (fscanf (db_volinfo_fp, format_string, &read_int_volid,
			  vol_name) != 2)
		{
		  break;
		}

	      volid = (VOLID) read_int_volid;
	      /*
	       * Remove data volumes at this point
	       */
	      switch (volid)
		{
		case LOG_DBVOLINFO_VOLID:
		case LOG_DBLOG_INFO_VOLID:
		case LOG_DBLOG_BKUPINFO_VOLID:
		case LOG_DBLOG_ACTIVE_VOLID:
		  continue;
		default:
		  temp_path =
		    fileio_get_directory_path (temp_path_buf, db_fullname);

		  /* Get the absolute path name */
		  COMPOSE_FULL_NAME (temp_vol_fullname,
				     sizeof (temp_vol_fullname), temp_path,
				     vol_name);

		  fileio_unformat (thread_p, temp_vol_fullname);
		}
	    }

	  fclose (db_volinfo_fp);
	}
      else
	{
	  /* Destory at least the database main volume */
	  fileio_unformat (thread_p, db_fullname);
	}
    }
  else
    {
      loghdr = &log_Gl.hdr;
      /*
       * DESTROY DATA VOLUMES
       */
      for (volid = LOG_DBFIRST_VOLID; volid < num_perm_vols; volid++)
	{
	  vlabel = fileio_get_volume_label (volid, ALLOC_COPY);
	  if (vlabel != NULL)
	    {
	      (void) pgbuf_invalidate_all (thread_p, volid);
	      fileio_dismount (thread_p,
			       fileio_get_volume_descriptor (volid));
	      fileio_unformat (thread_p, vlabel);
	      free (vlabel);
	    }
	}
    }

  /* Destroy the database volume information */
  fileio_make_volume_info_name (volinfo_fullname, db_fullname);
  fileio_unformat (thread_p, volinfo_fullname);

  /*
   * THIRD: Destroy log active, online log archives, and log information
   */

  /* If there is any archive current mounted, dismount the archive */
  if (log_Gl.trantable.area != NULL && log_Gl.append.log_pgptr != NULL
      && log_Gl.archive.vdes != NULL_VOLDES)
    {
      fileio_dismount (thread_p, log_Gl.archive.vdes);
      log_Gl.archive.vdes = NULL_VOLDES;
    }

  /* Destroy online log archives */
  if (loghdr != NULL)
    {
      for (i = loghdr->last_deleted_arv_num + 1; i < loghdr->nxarv_num; i++)
	{
	  fileio_make_log_archive_name (temp_vol_fullname, log_Archive_path,
					log_Prefix, i);
	  fileio_unformat (thread_p, temp_vol_fullname);
	}
    }

  /* Destroy temporary log archive */
  fileio_unformat (thread_p, log_Name_bg_archive);
  log_Gl.bg_archive_info.vdes = NULL_VOLDES;
  /* Destroy temporary removed log archived */
  fileio_unformat (thread_p, log_Name_removed_archive);

  /* Now undefine all pages */
  if (log_Gl.trantable.area != NULL && log_Gl.append.log_pgptr != NULL)
    {
      logpb_finalize_pool ();
      (void) pgbuf_invalidate_all (thread_p, NULL_VOLID);
      logtb_undefine_trantable (thread_p);
      if (log_Gl.append.vdes != NULL_VOLDES)
	{
	  fileio_dismount (thread_p, log_Gl.append.vdes);
	  log_Gl.append.vdes = NULL_VOLDES;
	}
      log_Gl.archive.vdes = NULL_VOLDES;
    }

  fileio_unformat (thread_p, log_Name_active);
  fileio_unformat (thread_p, log_Name_info);

  return NO_ERROR;
}

/*
 * logpb_check_if_exists -
 *
 * return:
 *
 *   fname(in):
 *   first_vol(in):
 *
 * NOTE:
 */
static bool
logpb_check_if_exists (const char *fname, char *first_vol)
{
  struct stat stat_buf;

  if (stat (fname, &stat_buf) != 0)
    {
      return false;		/* not exist */
    }
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_BO_VOLUME_EXISTS, 1, fname);
  if (first_vol[0] == 0)
    {
      strcpy (first_vol, fname);
    }
  return true;
}

/*
 * logpb_check_exist_any_volumes - check existence of DB files
 *
 * return: NO_ERROR or error code
 *
 *   db_fullname(in): Full name of the database
 *   logpath(in): Directory where the log volumes reside
 *   prefix_logname(in): Name of the log volumes.
 *   first_vol(in):
 *
 * NOTE: All log volumes (active, archives) and database backups that
 *              are accessible (i.e., located on disk) are checked
 */
int
logpb_check_exist_any_volumes (THREAD_ENTRY * thread_p,
			       const char *db_fullname, const char *logpath,
			       const char *prefix_logname, char *first_vol,
			       bool * is_exist)
{
  int exist_cnt;
  int error_code = NO_ERROR;

  exist_cnt = 0;
  first_vol[0] = 0;

  *is_exist = false;

  error_code =
    logpb_initialize_log_names (thread_p, db_fullname, logpath,
				prefix_logname);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }
  exist_cnt += logpb_check_if_exists (db_fullname, first_vol) ? 1 : 0;
  exist_cnt += logpb_check_if_exists (log_Name_active, first_vol) ? 1 : 0;
  exist_cnt += logpb_check_if_exists (log_Name_info, first_vol) ? 1 : 0;
  exist_cnt += logpb_check_if_exists (log_Name_volinfo, first_vol) ? 1 : 0;

  if (exist_cnt > 0)
    {
      *is_exist = true;
    }
  else
    {
      *is_exist = false;
    }

  return error_code;
}

/*
 *
 *       		       LOG FATAL ERRORS
 *
 */

/*
 * logpb_fatal_error - Log error
 *
 * return: nothing
 *
 *   log_exit(in):
 *   file_name(in):
 *   lineno(in):
 *   fmt(in):
 *   va_alist(in): Variable number of arguments (just like fprintf)
 *
 * NOTE: An error was found during logging. A short error message is
 *              produced on the stderr describing the error. Currently, the
 *              database is exited.
 */
void
logpb_fatal_error (THREAD_ENTRY * thread_p, bool log_exit,
		   const char *file_name, const int lineno,
		   const char *fmt, ...)
{
  va_list ap;

  va_start (ap, fmt);
  logpb_fatal_error_internal (thread_p, log_exit, true, file_name, lineno,
			      fmt, ap);
  va_end (ap);
}

void
logpb_fatal_error_exit_immediately_wo_flush (THREAD_ENTRY * thread_p,
					     const char *file_name,
					     const int lineno,
					     const char *fmt, ...)
{
  va_list ap;

  va_start (ap, fmt);
  logpb_fatal_error_internal (thread_p, true, false, file_name, lineno,
			      fmt, ap);
  va_end (ap);
}

static void
logpb_fatal_error_internal (THREAD_ENTRY * thread_p, bool log_exit,
			    bool need_flush, const char *file_name,
			    const int lineno, const char *fmt, va_list ap)
{
  const char *msglog;
  char msg[LINE_MAX];

  /* call er_set() to print call stack to the log */
  vsnprintf (msg, LINE_MAX, fmt, ap);
  er_set (ER_FATAL_ERROR_SEVERITY, file_name, lineno, ER_LOG_FATAL_ERROR, 1,
	  msg);

  /*
   * Flush any unfixed, dirty pages before the system exits. This is done
   * to make sure that all committed actions are reflected on disk.
   * Unfortunately, we may be placing some uncommitted action od disk. This
   * will be fixed by our recovery process. Note if the user runs the pathdb,
   * utility after this, the uncommitted actions will be considered as
   * committed.
   */

  if (log_exit == true && need_flush == true
      && log_Gl.append.log_pgptr != NULL)
    {
      /* Flush up to the smaller of the previous LSA record or the
       * previous flushed append page.
       */
      LOG_LSA tmp_lsa1, tmp_lsa2;
      static int in_fatal = false;

      if (in_fatal == false)
	{
	  in_fatal = true;

	  if (log_Gl.append.prev_lsa.pageid < log_Gl.append.nxio_lsa.pageid)
	    {
	      LSA_COPY (&tmp_lsa1, &log_Gl.append.prev_lsa);
	    }
	  else
	    {
	      /* TODO : valid code ?? */
	      /*
	         if ((tmp_lsa1.pageid = log_Gl.append.nxio_lsa.pageid - 1) < 0)
	         tmp_lsa1.pageid = 0;
	       */
	      tmp_lsa1.pageid = 0;
	    }

	  /*
	   * Flush as much as you can without forcing the current unfinish log
	   * record.
	   */
	  (void) pgbuf_flush_checkpoint (thread_p, &tmp_lsa1, NULL,
					 &tmp_lsa2);
	  in_fatal = false;
	}
    }

  fileio_synchronize_all (thread_p, false);

  fflush (stderr);
  fflush (stdout);

  fprintf (stderr, "\n--->>>\n*** FATAL ERROR *** \n");

  fprintf (stderr, "%s\n", er_msg ());

  /*
   * If error message log is different from terminal or /dev/null..indicate
   * that additional information can be found in the error log file
   */
  msglog = er_get_msglog_filename ();
  if (msglog != NULL && strcmp (msglog, "/dev/null") != 0)
    {
      fprintf (stderr,
	       "Please consult error_log file = %s for additional"
	       " information\n", msglog);
    }

  fflush (stderr);
  fflush (stdout);

  if (log_exit == true)
    {
      fprintf (stderr, "... ABORT/EXIT IMMEDIATELY ...<<<---\n");

#if defined(SERVER_MODE)
      boot_donot_shutdown_server_at_exit ();
      boot_server_status (BOOT_SERVER_DOWN);
#else /* SERVER_MODE */
      /*
       * The following crap is added to the standalone version to avoid the
       * client to continue accessing the database system in presence of
       * call on exit functions of the applications.
       */
      boot_donot_shutdown_client_at_exit ();
      tran_cache_tran_settings (NULL_TRAN_INDEX, -1);
      db_Connect_status = DB_CONNECTION_STATUS_NOT_CONNECTED;
#endif /* SERVER_MODE */

#if defined(NDEBUG)
      exit (EXIT_FAILURE);
#else /* NDEBUG */
      /* debugging purpose */
      abort ();
#endif /* NDEBUG */
    }
}

/*
 * logpb_initialize_flush_info - initialize flush information
 *
 * return: nothing
 *
 * NOTE:
 */
static int
logpb_initialize_flush_info (void)
{
  int error = NO_ERROR;
  LOG_FLUSH_INFO *flush_info = &log_Gl.flush_info;

  if (flush_info->toflush != NULL)
    {
      logpb_finalize_flush_info ();
    }
  assert (flush_info->toflush == NULL);

  flush_info->max_toflush = log_Pb.num_buffers - 1;
  flush_info->num_toflush = 0;
  flush_info->toflush = (LOG_PAGE **) calloc (log_Pb.num_buffers,
					      sizeof (LOG_PAGE *));
  if (flush_info->toflush == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      log_Pb.num_buffers * sizeof (LOG_PAGE *));
      error = ER_OUT_OF_VIRTUAL_MEMORY;
    }

  pthread_mutex_init (&flush_info->flush_mutex, NULL);

  return error;
}

/*
 * logpb_finalize_flush_info - Destroy flush information
 *
 * return: nothing
 *
 * NOTE:
 */
static void
logpb_finalize_flush_info (void)
{
#if defined(SERVER_MODE)
  UNUSED_VAR int rv;
#endif /* SERVER_MODE */
  LOG_FLUSH_INFO *flush_info = &log_Gl.flush_info;

  rv = pthread_mutex_lock (&flush_info->flush_mutex);

  if (flush_info->toflush != NULL)
    {
      free_and_init (flush_info->toflush);
    }

  flush_info->max_toflush = 0;
  flush_info->num_toflush = 0;

  pthread_mutex_unlock (&flush_info->flush_mutex);
  pthread_mutex_destroy (&flush_info->flush_mutex);
}

/*
 * logpb_finalize_writer_info - Destroy writer information
 *
 * return: nothing
 *
 * NOTE:
 */
static void
logpb_finalize_writer_info (void)
{
#if defined (SERVER_MODE)
  UNUSED_VAR int rv;
#endif
  LOGWR_ENTRY *entry, *next_entry;
  LOGWR_INFO *writer_info = &log_Gl.writer_info;

  rv = pthread_mutex_lock (&writer_info->wr_list_mutex);
  entry = writer_info->writer_list;
  while (entry)
    {
      next_entry = entry->next;
      free (entry);
      entry = next_entry;
    }
  writer_info->writer_list = NULL;
  pthread_mutex_unlock (&writer_info->wr_list_mutex);

  pthread_mutex_destroy (&writer_info->wr_list_mutex);
  pthread_cond_destroy (&writer_info->wr_list_cond);
}

/*
 * logpb_initialize_arv_page_info_table - Initialize archive log page table
 *
 * return: nothing
 *
 * NOTE:
 */
void
logpb_initialize_arv_page_info_table (void)
{
  memset (&logpb_Arv_page_info_table, 0, sizeof (ARV_LOG_PAGE_INFO_TABLE));
  logpb_Arv_page_info_table.rear = -1;
}

/*
 * logpb_initialize_logging_statistics - Initialize logging statistics
 *
 * return: nothing
 *
 * NOTE:
 */
void
logpb_initialize_logging_statistics (void)
{
  memset (&log_Stat, 0, sizeof (LOG_LOGGING_STAT));
}

/*
 * logpb_get_nxio_lsa -
 *
 */
void
logpb_get_nxio_lsa (LOG_LSA * nxio_lsa_p)
{
  volatile INT64 tmp_int64;

  tmp_int64 = ATOMIC_INC_64 ((INT64 *) (&log_Gl.append.nxio_lsa), 0);
  memcpy (nxio_lsa_p, (LOG_LSA *) (&tmp_int64), sizeof (LOG_LSA));
}

/*
 * logpb_set_nxio_lsa -
 */
static void
logpb_set_nxio_lsa (LOG_LSA * lsa)
{
  UINT64 tmp_int64;

  tmp_int64 = *((INT64 *) (lsa));
  tmp_int64 = ATOMIC_TAS_64 ((INT64 *) (&log_Gl.append.nxio_lsa), tmp_int64);
}
