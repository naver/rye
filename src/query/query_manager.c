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
 * query_manager.c - Query manager module
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <string.h>

#include "storage_common.h"
#include "system_parameter.h"
#include "xserver_interface.h"
#include "error_manager.h"
#include "log_manager.h"
#if defined(SERVER_MODE)
#include "log_impl.h"
#endif /* SERVER_MODE */
#include "critical_section.h"
#include "wait_for_graph.h"
#include "page_buffer.h"
#include "query_manager.h"
#include "query_opfunc.h"
#include "session.h"

#if defined (SERVER_MODE)
#include "connection_defs.h"
#include "connection_error.h"
#endif
#include "thread.h"
#include "md5.h"

#ifndef SERVER_MODE

#define pthread_mutex_init(a, b)
#define pthread_mutex_destroy(a)
#define pthread_mutex_lock(a)	0
#define pthread_mutex_unlock(a)
static int rv;

#define qmgr_initialize_mutex(a)
#define qmgr_destroy_mutex(a)
#define qmgr_lock_mutex(a, b)
#define qmgr_unlock_mutex(a)
#endif

#define QMGR_MAX_QUERY_ENTRY_PER_TRAN   100

#define TEMP_FILE_DEFAULT_PAGES         10

#define QMGR_TEMP_FILE_FREE_LIST_SIZE   100

#define QMGR_NUM_TEMP_FILE_LISTS        (TEMP_FILE_MEMBUF_NUM_TYPES)

#define QMGR_SQL_ID_LENGTH      13

/* We have two valid types of membuf used by temporary file. */
#define QMGR_IS_VALID_MEMBUF_TYPE(m)    ((m) == TEMP_FILE_MEMBUF_NORMAL \
    || (m) == TEMP_FILE_MEMBUF_KEY_BUFFER)

typedef enum qmgr_page_type QMGR_PAGE_TYPE;
enum qmgr_page_type
{
  QMGR_UNKNOWN_PAGE,
  QMGR_MEMBUF_PAGE,
  QMGR_TEMP_FILE_PAGE
};

/*
 *       		     ALLOCATION STRUCTURES
 *
 * A resource mechanism used to effectively handle memory allocation for the
 * query entry structures.
 */

#if defined (SERVER_MODE)
/* This struct is used when implements recursive mutex. */
typedef struct qmgr_mutex QMGR_MUTEX;
struct qmgr_mutex
{
  pthread_t owner;		/* mutex owner */
  unsigned int lock_count;	/* how many times we acquired mutex */
  pthread_mutex_t lock;
  pthread_cond_t not_busy_cond;
  unsigned int nwaits;		/* the number of waiters */
};
#endif

typedef struct qmgr_tran_entry QMGR_TRAN_ENTRY;
struct qmgr_tran_entry
{
#if defined (SERVER_MODE)
  QMGR_MUTEX lock;
#endif
  QMGR_TRAN_STATUS trans_stat;	/* transaction status */
  int query_id_generator;	/* global query identifier count */

  int num_query_entries;	/* number of allocated query entries */

  QMGR_QUERY_ENTRY *query_entry_list_p;	/* linked list of query entries */
  QMGR_QUERY_ENTRY *free_query_entry_list_p;	/* free query entry list */
};

typedef struct qmgr_temp_file_list QMGR_TEMP_FILE_LIST;
struct qmgr_temp_file_list
{
  pthread_mutex_t mutex;
  QMGR_TEMP_FILE *list;
  int count;
};
/*
 * Global query table variable used to keep track of query entries and
 * the anchor for the out of space in the temp vol WFG.
 */
typedef struct qmgr_query_table QMGR_QUERY_TABLE;
struct qmgr_query_table
{
  QMGR_TRAN_ENTRY *tran_entries_p;	/* list of transaction entries */
  int num_trans;		/* size of trans_ind[] */

  /* temp file free list info */
  QMGR_TEMP_FILE_LIST temp_file_list[QMGR_NUM_TEMP_FILE_LISTS];
};

QMGR_QUERY_TABLE qmgr_Query_table = { NULL, 0,
  {{PTHREAD_MUTEX_INITIALIZER, NULL, 0}, {PTHREAD_MUTEX_INITIALIZER, NULL, 0}}
};

#if !defined(SERVER_MODE)
static struct drand48_data qmgr_rand_buf;
#endif

/*
 * 			QM_MUTEX_LOCK/UNLOCK : recursive mutex
 *
 * Solaris 2.7 seems that does not support recursive mutex. There is option
 * which names PTHREAD_MUTEX_RECURSIVE. But it does not work.
 */
#if defined (SERVER_MODE)
static void qmgr_check_mutex_error (int r, const char *file, int line);
static void qmgr_initialize_mutex (QMGR_MUTEX * qm_mutex);
static void qmgr_destroy_mutex (QMGR_MUTEX * qm_mutex);
static void qmgr_lock_mutex (THREAD_ENTRY * thread_p, QMGR_MUTEX * qm_mutex);
static void qmgr_unlock_mutex (QMGR_MUTEX * qm_mutex);
#endif

static QMGR_PAGE_TYPE qmgr_get_page_type (PAGE_PTR page_p,
					  QMGR_TEMP_FILE * temp_file_p);
static void qmgr_put_page_header (PAGE_PTR page_p,
				  QFILE_PAGE_HEADER * header_p);

static QMGR_QUERY_ENTRY *qmgr_allocate_query_entry (THREAD_ENTRY * thread_p,
						    QMGR_TRAN_ENTRY *
						    tran_entry_p);
static void qmgr_free_query_entry (THREAD_ENTRY * thread_p,
				   QMGR_TRAN_ENTRY * tran_entry_p,
				   QMGR_QUERY_ENTRY * q_ptr);
static void qmgr_deallocate_query_entries (THREAD_ENTRY * thread_p,
					   QMGR_QUERY_ENTRY * q_ptr);
static void qmgr_add_query_entry (THREAD_ENTRY * thread_p,
				  QMGR_QUERY_ENTRY * q_ptr, int trans_ind);
static QMGR_QUERY_ENTRY *qmgr_find_query_entry (QMGR_QUERY_ENTRY *
						query_list_p,
						QUERY_ID query_id);
static void qmgr_delete_query_entry (THREAD_ENTRY * thread_p,
				     QUERY_ID query_id, int trans_ind);
static void qmgr_free_tran_entries (THREAD_ENTRY * thread_p);

static int xqmgr_unpack_xasl_tree (THREAD_ENTRY * thread_p,
				   const XASL_ID * xasl_id_p,
				   char *xasl_stream, int xasl_stream_size,
				   XASL_CACHE_CLONE * cache_clone_p,
				   XASL_NODE ** xasl_tree);
static PAGE_PTR qmgr_get_external_file_page (THREAD_ENTRY * thread_p,
					     VPID * vpid,
					     QMGR_TEMP_FILE * vfid);
static int qmgr_free_query_temp_file_by_query_entry (THREAD_ENTRY * thread_p,
						     QMGR_QUERY_ENTRY * qptr,
						     int tran_idx);
static QMGR_TEMP_FILE *qmgr_allocate_tempfile_with_buffer (int
							   num_buffer_pages);

#if defined (SERVER_MODE)
#if defined (ENABLE_UNUSED_FUNCTION)
static XASL_NODE *qmgr_find_leaf (XASL_NODE * xasl);
#endif
static QFILE_LIST_ID *qmgr_process_query (THREAD_ENTRY * thread_p,
					  const XASL_ID * xasl_id,
					  char *xasl_stream,
					  int xasl_stream_size,
					  int dbval_count,
					  DB_VALUE * dbvals_p,
					  QUERY_FLAG flag,
					  XASL_CACHE_CLONE * cache_clone_p,
					  QMGR_QUERY_ENTRY * query_p,
					  QMGR_TRAN_ENTRY * tran_entry_p,
					  int shard_groupid,
					  DB_VALUE * shard_key);
static void qmgr_reset_query_exec_info (int tran_index);
static void qmgr_set_query_exec_info_to_tdes (int tran_index,
					      int query_timeout,
					      const XASL_ID * xasl_id);
#endif

static void qmgr_initialize_temp_file_list (QMGR_TEMP_FILE_LIST *
					    temp_file_list_p,
					    QMGR_TEMP_FILE_MEMBUF_TYPE
					    membuf_type);
static void qmgr_finalize_temp_file_list (QMGR_TEMP_FILE_LIST *
					  temp_file_list_p);
static QMGR_TEMP_FILE *qmgr_get_temp_file_from_list (QMGR_TEMP_FILE_LIST *
						     temp_file_list_p);
static void qmgr_put_temp_file_into_list (QMGR_TEMP_FILE * temp_file_p);

static int copy_bind_value_to_tdes (THREAD_ENTRY * thread_p,
				    int num_bind_vals, DB_VALUE * bind_vals);

/*
 * qmgr_get_page_type () -
 *
 *   return: QMGR_PAGE_TYPE
 *
 *   page_p(in):
 *   temp_file_p(in):
 */
static QMGR_PAGE_TYPE
qmgr_get_page_type (PAGE_PTR page_p, QMGR_TEMP_FILE * temp_file_p)
{
  if (temp_file_p != NULL
      && temp_file_p->membuf_last >= 0
      && temp_file_p->membuf
      && page_p >= temp_file_p->membuf[0]
      && page_p <= temp_file_p->membuf[temp_file_p->membuf_last])
    {
      return QMGR_MEMBUF_PAGE;
    }

  if (temp_file_p != NULL)
    {
      PAGE_PTR begin_page = NULL, end_page = NULL;

      begin_page = (PAGE_PTR) ((PAGE_PTR) temp_file_p->membuf +
			       DB_ALIGN (sizeof (PAGE_PTR) *
					 temp_file_p->membuf_npages,
					 MAX_ALIGNMENT));
      end_page = begin_page + temp_file_p->membuf_npages * DB_PAGESIZE;
      if (begin_page <= page_p && page_p <= end_page)
	{
	  /* defense code */
	  assert_release (false);
	  return QMGR_UNKNOWN_PAGE;
	}
    }

  return QMGR_TEMP_FILE_PAGE;
}

static void
qmgr_put_page_header (PAGE_PTR page_p, QFILE_PAGE_HEADER * header_p)
{
  OR_PUT_INT ((page_p) + QFILE_TUPLE_COUNT_OFFSET, (header_p)->pg_tplcnt);
  OR_PUT_INT ((page_p) + QFILE_PREV_PAGE_ID_OFFSET, (header_p)->prev_pgid);
  OR_PUT_INT ((page_p) + QFILE_NEXT_PAGE_ID_OFFSET, (header_p)->next_pgid);
  OR_PUT_INT ((page_p) + QFILE_LAST_TUPLE_OFFSET, (header_p)->lasttpl_off);
  OR_PUT_INT ((page_p) + QFILE_OVERFLOW_PAGE_ID_OFFSET,
	      (header_p)->ovfl_pgid);
  OR_PUT_SHORT ((page_p) + QFILE_PREV_VOL_ID_OFFSET, (header_p)->prev_volid);
  OR_PUT_SHORT ((page_p) + QFILE_NEXT_VOL_ID_OFFSET, (header_p)->next_volid);
  OR_PUT_SHORT ((page_p) + QFILE_OVERFLOW_VOL_ID_OFFSET,
		(header_p)->ovfl_volid);
#if !defined(NDEBUG)
  /* suppress valgrind UMW error */
  memset (page_p + QFILE_RESERVED_OFFSET, 0,
	  QFILE_PAGE_HEADER_SIZE - QFILE_RESERVED_OFFSET);
#endif
}


#if defined (SERVER_MODE)
static void
qmgr_check_mutex_error (int r, const char *file, int line)
{
  if (r != 0)
    {
      fprintf (stderr, "Error at %s(%d) : (%s)\n **** THREAD EXIT ****\n",
	       file, line, "Mutex operation error");
      pthread_exit (NULL);
    }
}

/*
 * qmgr_initialize_mutex () -
 *   return:
 *   qm_mutex(in)       :
 */
static void
qmgr_initialize_mutex (QMGR_MUTEX * mutex_p)
{
  int r;

  mutex_p->owner = (pthread_t) 0;	/* null thread id */
  mutex_p->lock_count = 0;

  r = pthread_mutex_init (&mutex_p->lock, NULL);
  qmgr_check_mutex_error (r, __FILE__, __LINE__);

  r = pthread_cond_init (&mutex_p->not_busy_cond, NULL);
  qmgr_check_mutex_error (r, __FILE__, __LINE__);
  mutex_p->nwaits = 0;
}

/*
 * qmgr_destroy_mutex () -
 *   return:
 *   qm_mutex(in)       :
 */
static void
qmgr_destroy_mutex (QMGR_MUTEX * mutex_p)
{
  mutex_p->owner = (pthread_t) 0;
  pthread_mutex_destroy (&mutex_p->lock);
  pthread_cond_destroy (&mutex_p->not_busy_cond);
}

/*
 * qmgr_lock_mutex () -
 *   return:
 *   qm_mutex(in)       :
 */
static void
qmgr_lock_mutex (THREAD_ENTRY * thread_p, QMGR_MUTEX * mutex_p)
{
  int r;

  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  r = pthread_mutex_lock (&mutex_p->lock);
  qmgr_check_mutex_error (r, __FILE__, __LINE__);

  /* If other thread owns this mutex wait until released */
  if (mutex_p->lock_count > 0 && mutex_p->owner != thread_p->tid)
    {
      do
	{
	  mutex_p->nwaits++;
	  pthread_cond_wait (&mutex_p->not_busy_cond, &mutex_p->lock);
	  mutex_p->nwaits--;
	}
      while (mutex_p->lock_count != 0);
    }

  mutex_p->owner = thread_p->tid;
  mutex_p->lock_count++;

  pthread_mutex_unlock (&mutex_p->lock);
}

/*
 * qmgr_unlock_mutex () -
 *   return:
 *   qm_mutex(in)       :
 */
static void
qmgr_unlock_mutex (QMGR_MUTEX * mutex_p)
{
  int r;

  r = pthread_mutex_lock (&mutex_p->lock);
  qmgr_check_mutex_error (r, __FILE__, __LINE__);

  if (--mutex_p->lock_count == 0)
    {
      mutex_p->owner = (pthread_t) 0;
      if (mutex_p->nwaits > 0)	/* there is an waiter */
	{
	  pthread_cond_signal (&mutex_p->not_busy_cond);
	}
    }

  pthread_mutex_unlock (&mutex_p->lock);
}
#endif

static void
qmgr_mark_query_as_completed (QMGR_QUERY_ENTRY * query_p)
{
#if defined (SERVER_MODE)
  int rv;

  rv = pthread_mutex_lock (&query_p->lock);

  query_p->query_mode = QUERY_COMPLETED;
  query_p->interrupt = false;
  query_p->propagate_interrupt = true;

  if (query_p->nwaits > 0)
    {
      pthread_cond_signal (&query_p->cond);
    }

  pthread_mutex_unlock (&query_p->lock);
#else
  query_p->query_mode = QUERY_COMPLETED;
  query_p->interrupt = false;
  query_p->propagate_interrupt = true;
#endif
}

/*
 * qmgr_allocate_query_entry () -
 *   return: QMGR_QUERY_ENTRY * or NULL
 *
 * Note: Allocate a query_entry structure from the free
 * list of query_entry structures if any, or by malloc to allocate a new
 * a structure.
 */
static QMGR_QUERY_ENTRY *
qmgr_allocate_query_entry (UNUSED_ARG THREAD_ENTRY * thread_p,
			   QMGR_TRAN_ENTRY * tran_entry_p)
{
  QMGR_QUERY_ENTRY *query_p;

  query_p = tran_entry_p->free_query_entry_list_p;

  if (query_p)
    {
      tran_entry_p->free_query_entry_list_p = query_p->next;
    }
  else if (QMGR_MAX_QUERY_ENTRY_PER_TRAN < tran_entry_p->num_query_entries)
    {
      return NULL;
    }
  else
    {
      query_p = (QMGR_QUERY_ENTRY *) malloc (sizeof (QMGR_QUERY_ENTRY));
      if (query_p == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, sizeof (QMGR_QUERY_ENTRY));
	  return NULL;
	}

      query_p->list_id = NULL;

#if defined (SERVER_MODE)
      pthread_mutex_init (&query_p->lock, NULL);
      pthread_cond_init (&query_p->cond, NULL);
      query_p->nwaits = 0;
#endif

      tran_entry_p->num_query_entries++;
    }

  /* assign query id */
  if (tran_entry_p->query_id_generator >= SHRT_MAX - 2)	/* overflow happened */
    {
      tran_entry_p->query_id_generator = 0;
    }
  query_p->query_id = ++tran_entry_p->query_id_generator;

  /* initialize per query temp file VFID structure */
  query_p->next = NULL;
  query_p->qe_temp_vfid = NULL;
  query_p->qe_num_temp = 0;
  query_p->total_count = 0;
  XASL_ID_SET_NULL (&query_p->xasl_id);
  query_p->xasl_ent = NULL;
  query_p->list_id = NULL;
  query_p->errid = NO_ERROR;
  query_p->er_msg = NULL;
  query_p->interrupt = false;
  query_p->propagate_interrupt = true;
  query_p->query_flag = 0;
  query_p->is_holdable = false;
  VPID_SET_NULL (&query_p->save_vpid);	/* Setup default for save_vpid */

  return query_p;
}

/*
 * qmgr_free_query_entry () -
 *   return:
 *   q_ptr(in)  : Query entry structure to be freed
 *
 * Note: Free the query_entry structure by putting it to the free
 * query_entry structure list if there are not many in the list,
 * or by calling db_free.
 */
static void
qmgr_free_query_entry (UNUSED_ARG THREAD_ENTRY * thread_p,
		       QMGR_TRAN_ENTRY * tran_entry_p,
		       QMGR_QUERY_ENTRY * query_p)
{
#if defined (SERVER_MODE)
  if (query_p->er_msg)
    {
      free_and_init (query_p->er_msg);
    }
#endif

  if (query_p->list_id)
    {
      QFILE_FREE_AND_INIT_LIST_ID (query_p->list_id);
    }

  query_p->next = NULL;

  query_p->next = tran_entry_p->free_query_entry_list_p;
  tran_entry_p->free_query_entry_list_p = query_p;
}

/*
 * qmgr_deallocate_query_entries () -
 *   return:
 *   q_ptr(in)  : Query Entry Pointer
 *
 * Note: Free the area allocated for the query entry list
 */
static void
qmgr_deallocate_query_entries (UNUSED_ARG THREAD_ENTRY * thread_p,
			       QMGR_QUERY_ENTRY * query_p)
{
  QMGR_QUERY_ENTRY *p;

  while (query_p)
    {
      p = query_p;
      query_p = query_p->next;

#if defined (SERVER_MODE)
      if (p->er_msg)
	{
	  free_and_init (p->er_msg);
	}

      pthread_mutex_destroy (&p->lock);
      pthread_cond_destroy (&p->cond);
#endif

      if (p->list_id)
	{
	  QFILE_FREE_AND_INIT_LIST_ID (p->list_id);
	}

      free_and_init (p);
    }
}

/*
 * qmgr_add_query_entry () -
 *   return:
 *   q_ptr(in)  : Query Entry Pointer
 *   trans_ind(in)      : this transaction index
 *
 * Note: Add the given query entry to the list of query entries for the
 * current transaction.
 */
static void
qmgr_add_query_entry (THREAD_ENTRY * thread_p, QMGR_QUERY_ENTRY * query_p,
		      int tran_index)
{
  QMGR_TRAN_ENTRY *tran_entry_p;

  if (tran_index == NULL_TRAN_INDEX)
    {
      tran_index = logtb_get_current_tran_index (thread_p);
    }

  assert (qmgr_Query_table.tran_entries_p != NULL);
  assert (qmgr_Query_table.num_trans > tran_index);

  tran_entry_p = &qmgr_Query_table.tran_entries_p[tran_index];

  qmgr_lock_mutex (thread_p, &tran_entry_p->lock);

  if (tran_entry_p->trans_stat == QMGR_TRAN_NULL ||
      tran_entry_p->trans_stat == QMGR_TRAN_TERMINATED)
    {
      tran_entry_p->trans_stat = QMGR_TRAN_RUNNING;
      tran_entry_p->query_entry_list_p = query_p;
    }
  else
    {
      query_p->next = tran_entry_p->query_entry_list_p;
      tran_entry_p->query_entry_list_p = query_p;
    }

  qmgr_unlock_mutex (&tran_entry_p->lock);
}

static QMGR_QUERY_ENTRY *
qmgr_find_query_entry (QMGR_QUERY_ENTRY * query_list_p, QUERY_ID query_id)
{
  QMGR_QUERY_ENTRY *query_p;

  query_p = query_list_p;
  while (query_p && query_p->query_id != query_id)
    {
      query_p = query_p->next;
    }

  return query_p;
}

/*
 * qmgr_get_query_entry () -
 *   return: QMGR_QUERY_ENTRY *
 *   query_id(in)       : query identifier
 *   trans_ind(in)      : this transaction index(NULL_TRAN_INDEX for unknown)
 *
 * Note: Return the query entry pointer for the given query identifier
 * or NULL if the query entry is not found.
 */
QMGR_QUERY_ENTRY *
qmgr_get_query_entry (THREAD_ENTRY * thread_p, QUERY_ID query_id,
		      int tran_index)
{
  QMGR_QUERY_ENTRY *query_p = NULL;
  QMGR_TRAN_ENTRY *tran_entry_p;

  /*
   * The code for finding the query_entry pointer is in-lined in
   * xqmgr_end_query and qmgr_interrupt_query to avoid calling this function.
   */

  if (qmgr_Query_table.tran_entries_p == NULL)
    {
      return query_p;
    }

  if (tran_index == NULL_TRAN_INDEX)
    {
      tran_index = logtb_get_current_tran_index (thread_p);
    }

  assert (qmgr_Query_table.tran_entries_p != NULL);
  assert (qmgr_Query_table.num_trans > tran_index);

  tran_entry_p = &qmgr_Query_table.tran_entries_p[tran_index];

  qmgr_lock_mutex (thread_p, &tran_entry_p->lock);
  query_p = qmgr_find_query_entry (tran_entry_p->query_entry_list_p,
				   query_id);
  if (query_p != NULL)
    {
      qmgr_unlock_mutex (&tran_entry_p->lock);
      return query_p;
    }

#if 0
  /* TODO:[happy] */
  assert (false);
#endif

  /* Maybe it is a holdable result and we'll find it in the session state
   * object. In order to be able to use this result, we need to create
   * a new entry for this query in the transaction query entries and copy
   * result information from the session.
   */
  query_p = qmgr_allocate_query_entry (thread_p, tran_entry_p);
  qmgr_unlock_mutex (&tran_entry_p->lock);
  if (query_p == NULL)
    {
      return NULL;
    }

  query_p->query_id = query_id;
  if (xsession_load_query_entry_info (thread_p, query_p) != NO_ERROR)
    {
      qmgr_lock_mutex (thread_p, &tran_entry_p->lock);
      qmgr_free_query_entry (thread_p, tran_entry_p, query_p);
      qmgr_unlock_mutex (&tran_entry_p->lock);

      query_p = NULL;
      return NULL;
    }

#if defined (SERVER_MODE)
  /* mark this query as belonging to this transaction */
  if (thread_p != NULL)
    {
      query_p->tid = thread_p->tid;
    }
#endif

  /* add it to this transaction also */
  qmgr_add_query_entry (thread_p, query_p, tran_index);

  return query_p;
}

/*
 * qmgr_delete_query_entry () -
 *   return:
 *   query_id(in)       : query identifier
 *   trans_ind(in)      : this transaction index(NULL_TRAN_INDEX for unknown)
 *
 * Note: Delete the query entry for the given query identifier from the
 * query entry list for the current transaction.
 */
static void
qmgr_delete_query_entry (THREAD_ENTRY * thread_p, QUERY_ID query_id,
			 int tran_index)
{
  QMGR_QUERY_ENTRY *query_p = NULL, *prev_query_p = NULL;
  QMGR_TRAN_ENTRY *tran_entry_p;

  if (qmgr_Query_table.tran_entries_p == NULL)
    {
      return;
    }

  if (tran_index == NULL_TRAN_INDEX)
    {
      tran_index = logtb_get_current_tran_index (thread_p);
    }

  assert (qmgr_Query_table.tran_entries_p != NULL);
  assert (qmgr_Query_table.num_trans > tran_index);

  tran_entry_p = &qmgr_Query_table.tran_entries_p[tran_index];

  qmgr_lock_mutex (thread_p, &tran_entry_p->lock);

  prev_query_p = NULL;
  query_p = tran_entry_p->query_entry_list_p;

  while (query_p && query_p->query_id != query_id)
    {
      prev_query_p = query_p;
      query_p = query_p->next;
    }

  if (query_p == NULL)
    {
      qmgr_unlock_mutex (&tran_entry_p->lock);
      return;
    }

  if (prev_query_p == NULL)
    {
      /* entry is the first entry */
      tran_entry_p->query_entry_list_p = query_p->next;

      if (tran_entry_p->query_entry_list_p == NULL)
	{
	  tran_entry_p->trans_stat = QMGR_TRAN_TERMINATED;
	}
    }
  else
    {
      prev_query_p->next = query_p->next;
    }

  qmgr_free_query_entry (thread_p, tran_entry_p, query_p);
  qmgr_unlock_mutex (&tran_entry_p->lock);
}

static void
qmgr_initialize_tran_entry (QMGR_TRAN_ENTRY * tran_entry_p)
{
  tran_entry_p->trans_stat = QMGR_TRAN_NULL;
  tran_entry_p->query_id_generator = 0;
  tran_entry_p->num_query_entries = 0;
  tran_entry_p->query_entry_list_p = NULL;
  tran_entry_p->free_query_entry_list_p = NULL;

  qmgr_initialize_mutex (&tran_entry_p->lock);
}

/*
 * qmgr_allocate_tran_entries () -
 *   return: int (NO_ERROR or ER_FAILED)
 *   trans_cnt(in)      : Number of transactions
 *
 * Note: Allocates(Reallocates) the area pointed by the query manager
 * transaction index pointer
 */
int
qmgr_allocate_tran_entries (UNUSED_ARG THREAD_ENTRY * thread_p, int count)
{
  QMGR_TRAN_ENTRY *tran_entry_p;
  int i;

#if defined (SERVER_MODE)
  count = MAX (count, MAX_NTRANS);
#endif

  /* enter critical section, this prevents another to perform malloc/init */
  if (csect_enter (thread_p, CSECT_QPROC_QUERY_TABLE, INF_WAIT) != NO_ERROR)
    {
      return ER_FAILED;
    }

  if (qmgr_Query_table.tran_entries_p == NULL)
    {
      qmgr_Query_table.tran_entries_p =
	(QMGR_TRAN_ENTRY *) malloc (count * sizeof (QMGR_TRAN_ENTRY));

      if (qmgr_Query_table.tran_entries_p == NULL)
	{
	  csect_exit (CSECT_QPROC_QUERY_TABLE);
	  return ER_FAILED;
	}

      qmgr_Query_table.num_trans = count;

      /* initialize newly allocated areas */
      tran_entry_p = qmgr_Query_table.tran_entries_p;
      for (i = 0; i < qmgr_Query_table.num_trans; i++)
	{
	  qmgr_initialize_tran_entry (tran_entry_p);
	  tran_entry_p++;
	}

      csect_exit (CSECT_QPROC_QUERY_TABLE);

      return NO_ERROR;
    }

  if (count <= qmgr_Query_table.num_trans)
    {
      csect_exit (CSECT_QPROC_QUERY_TABLE);
      return NO_ERROR;
    }

  qmgr_Query_table.tran_entries_p =
    (QMGR_TRAN_ENTRY *) realloc (qmgr_Query_table.tran_entries_p,
				 count * sizeof (QMGR_TRAN_ENTRY));
  if (qmgr_Query_table.tran_entries_p == NULL)
    {
      csect_exit (CSECT_QPROC_QUERY_TABLE);
      return ER_FAILED;
    }

  tran_entry_p =
    (QMGR_TRAN_ENTRY *) qmgr_Query_table.tran_entries_p +
    qmgr_Query_table.num_trans;
  for (i = qmgr_Query_table.num_trans; i < count; i++)
    {
      qmgr_initialize_tran_entry (tran_entry_p);
      tran_entry_p++;
    }

  qmgr_Query_table.num_trans = count;

  csect_exit (CSECT_QPROC_QUERY_TABLE);

  return NO_ERROR;
}

/*
 * qmgr_free_tran_entries () -
 *   return:
 *
 * Note: frees the area pointed by the query manager transaction index pointer.
 */
static void
qmgr_free_tran_entries (THREAD_ENTRY * thread_p)
{
  QMGR_TRAN_ENTRY *tran_entry_p;
  int i;

  if (qmgr_Query_table.tran_entries_p == NULL)
    {
      return;
    }

  tran_entry_p = qmgr_Query_table.tran_entries_p;
  for (i = 0; i < qmgr_Query_table.num_trans; i++)
    {
      qmgr_lock_mutex (thread_p, &tran_entry_p->lock);
      qmgr_deallocate_query_entries (thread_p,
				     tran_entry_p->query_entry_list_p);
      qmgr_deallocate_query_entries (thread_p,
				     tran_entry_p->free_query_entry_list_p);
      qmgr_unlock_mutex (&tran_entry_p->lock);

      qmgr_destroy_mutex (&tran_entry_p->lock);
      tran_entry_p++;
    }

  free_and_init (qmgr_Query_table.tran_entries_p);
  qmgr_Query_table.num_trans = 0;
}

#if defined (RYE_DEBUG)
static const char *
qmgr_get_tran_status_string (QMGR_TRAN_STATUS stat)
{
  switch (stat)
    {
    case QMGR_TRAN_NULL:
      return "QMGR_TRAN_NULL";
    case QMGR_TRAN_RUNNING:
      return "QMGR_TRAN_NULL";
    case QMGR_TRAN_DELAYED_START:
      return "QMGR_TRAN_DELAYED_START";
    case QMGR_TRAN_WAITING:
      return "QMGR_TRAN_WAITING";
    case QMGR_TRAN_RESUME_TO_DEALLOCATE:
      return "QMGR_TRAN_RESUME_TO_DEALLOCATE";
    case QMGR_TRAN_RESUME_DUE_DEADLOCK:
      return "QMGR_TRAN_RESUME_DUE_DEADLOCK";
    case QMGR_TRAN_TERMINATED:
      return "QMGR_TRAN_TERMINATED";
    default:
      return "QMGR_UNKNOWN";
    }
}

static void
qmgr_dump_query_entry (QMGR_QUERY_ENTRY * query_p)
{
  QMGR_TEMP_FILE *temp_vfid_p;
  QFILE_LIST_ID *list_id_p;
  int i;

  fprintf (stdout, "\t\tQuery Entry Structures:\n");
  fprintf (stdout, "\t\tquery_id: %lld\n", (long long) query_p->query_id);
  fprintf (stdout, "\t\txasl_id: {{%d, %d}, {%d, %d}}\n",
	   query_p->xasl_id.first_vpid.pageid,
	   query_p->xasl_id.first_vpid.volid,
	   query_p->xasl_id.temp_vfid.fileid,
	   query_p->xasl_id.temp_vfid.volid);
  fprintf (stdout, "\t\tlist_id: %p\n", (void *) query_p->list_id);

  if (query_p->list_id)
    {
      list_id_p = query_p->list_id;
      fprintf (stdout,
	       "\t\t{type_list: {%d, %p}, tuple_cnt: %d, page_cnt: %d,\n"
	       "\t first_vpid: {%d, %d}, last_vpid: {%d, %d},\n"
	       "\t last_pgptr: %p, last_offset: %d, lasttpl_len: %d}\n",
	       list_id_p->type_list.type_cnt,
	       (void *) list_id_p->type_list.domp,
	       list_id_p->tuple_cnt,
	       list_id_p->page_cnt,
	       list_id_p->first_vpid.pageid,
	       list_id_p->first_vpid.volid,
	       list_id_p->last_vpid.pageid,
	       list_id_p->last_vpid.volid,
	       list_id_p->last_pgptr,
	       list_id_p->last_offset, list_id_p->lasttpl_len);
    }

  if (query_p->qe_temp_vfid)
    {
      temp_vfid_p = query_p->qe_temp_vfid;

      do
	{
	  fprintf (stdout, "\t\tfile_vfid: %p\n", (void *) &temp_vfid_p);
	  fprintf (stdout, "\t\tvpid_array_index: %d\n",
		   temp_vfid_p->vpid_index);
	  fprintf (stdout, "\t\tvpid_array_count: %d\n",
		   temp_vfid_p->vpid_count);

	  if (temp_vfid_p->vpid_index != -1)
	    {
	      for (i = 1; i < temp_vfid_p->vpid_count; i++)
		{
		  fprintf (stdout, "\t\tvpid_array[%d]:\n", i);
		  fprintf (stdout, "\t\t\tpage_id: %d\n",
			   temp_vfid_p->vpid_array[i].pageid);
		  fprintf (stdout, "\t\t\tvol_id: %d\n",
			   temp_vfid_p->vpid_array[i].volid);
		}
	    }

	  temp_vfid_p = temp_vfid_p->next;
	}
      while (temp_vfid_p != query_p->qe_temp_vfid);
    }

  fprintf (stdout, "\t\tnext: %p\n\n", (void *) query_p->next);
}

/*
 * qmgr_dump () -
 *   return:
 *
 * Note: Dump query manager table for debugging purposes.
 */
void
qmgr_dump (void)
{
  QMGR_TRAN_ENTRY *tran_entry_p;
  QMGR_QUERY_ENTRY *query_p;
  int waiting_count, running_count;
  int i;

  /* Get statistics from query manager table */
  waiting_count = running_count = 0;

  tran_entry_p = qmgr_Query_table.tran_entries_p;
  for (i = 0; i < qmgr_Query_table.num_trans; i++)
    {
      if (tran_entry_p->trans_stat == QMGR_TRAN_WAITING)
	{
	  waiting_count++;
	}
      else if (tran_entry_p->trans_stat == QMGR_TRAN_RUNNING)
	{
	  running_count++;
	}

      tran_entry_p++;
    }

  fprintf (stdout, "\n\tQUERY MANAGER TRANSACTION STRUCTURES: \n");
  fprintf (stdout, "\t===================================== \n");
  fprintf (stdout, "\tTrans_cnt: %d\n", qmgr_Query_table.num_trans);
  fprintf (stdout, "\tWait_trans_cnt: %d\n", waiting_count);
  fprintf (stdout, "\tRun_trans_cnt: %d\n", running_count);
  fprintf (stdout, "\n\tTransaction index array: \n");
  fprintf (stdout, "\t------------------------ \n");

  tran_entry_p = qmgr_Query_table.tran_entries_p;
  for (i = 0; i < qmgr_Query_table.num_trans; i++)
    {
      fprintf (stdout, "\tTrans_ind: %d\n", i);
      fprintf (stdout, "\tTrans_stat: %s\n",
	       qmgr_get_tran_status_string (tran_entry_p->trans_stat));

      fprintf (stdout, "\tTrans_query_entries:\n");

      for (query_p = tran_entry_p->query_entry_list_p; query_p;
	   query_p = query_p->next)
	{
	  qmgr_dump_query_entry (query_p);
	}

      fprintf (stdout, "\t------------------------ \n");
      tran_entry_p++;
    }
}
#endif

/*
 * qmgr_initialize () -
 *   return: int (NO_ERROR or ER_FAILED)
 *
 * Note: Initializes the query manager and the query file manager
 * global variables.
 */
int
qmgr_initialize (THREAD_ENTRY * thread_p)
{
  int total_tran_indices;
#if !defined(SERVER_MODE)
  struct timeval t;
#endif

  if (csect_enter (thread_p, CSECT_QPROC_QUERY_TABLE, INF_WAIT) != NO_ERROR)
    {
      return ER_FAILED;
    }

  if (qmgr_Query_table.tran_entries_p == NULL)
    {
      total_tran_indices = logtb_get_number_of_total_tran_indices ();
      if (qmgr_allocate_tran_entries (thread_p, total_tran_indices) !=
	  NO_ERROR)
	{
	  csect_exit (CSECT_QPROC_QUERY_TABLE);
	  return ER_FAILED;
	}
    }

  if (qmgr_Query_table.temp_file_list[TEMP_FILE_MEMBUF_NORMAL].list != NULL)
    {
      qmgr_finalize_temp_file_list (&qmgr_Query_table.temp_file_list
				    [TEMP_FILE_MEMBUF_NORMAL]);
    }
  qmgr_initialize_temp_file_list (&qmgr_Query_table.temp_file_list
				  [TEMP_FILE_MEMBUF_NORMAL],
				  TEMP_FILE_MEMBUF_NORMAL);

  if (qmgr_Query_table.temp_file_list[TEMP_FILE_MEMBUF_KEY_BUFFER].list !=
      NULL)
    {
      qmgr_finalize_temp_file_list (&qmgr_Query_table.temp_file_list
				    [TEMP_FILE_MEMBUF_KEY_BUFFER]);
    }
  qmgr_initialize_temp_file_list (&qmgr_Query_table.temp_file_list
				  [TEMP_FILE_MEMBUF_KEY_BUFFER],
				  TEMP_FILE_MEMBUF_KEY_BUFFER);

  csect_exit (CSECT_QPROC_QUERY_TABLE);

  qfile_initialize ();

#if defined (SERVER_MODE)
  numeric_init_power_value_string ();
#endif

  srand48 ((long) time (NULL));

#if !defined(SERVER_MODE)
  gettimeofday (&t, NULL);
  srand48_r ((long) t.tv_usec, &qmgr_rand_buf);
#endif

  scan_initialize ();

  return NO_ERROR;
}

/*
 * qmgr_finalize () -
 *   return:
 *
 * Note: Finalizes the query manager functioning by deallocating the
 * memory area pointed by transaction index list pointer.
 */
void
qmgr_finalize (THREAD_ENTRY * thread_p)
{
  int i;

  scan_finalize ();
  qfile_finalize ();

  if (csect_enter (thread_p, CSECT_QPROC_QUERY_TABLE, INF_WAIT) != NO_ERROR)
    {
      return;
    }

  qmgr_free_tran_entries (thread_p);

  qmgr_Query_table.tran_entries_p = NULL;

  for (i = 0; i < QMGR_NUM_TEMP_FILE_LISTS; i++)
    {
      qmgr_finalize_temp_file_list (&qmgr_Query_table.temp_file_list[i]);
    }
  csect_exit (CSECT_QPROC_QUERY_TABLE);
}

/*
 * xqmgr_prepare_query () - Prepares a query for later (and repetitive) execution
 *   return:  XASL file id which contains the XASL stream
 *   thrd(in)   :
 *   context (in)      : query string; used for hash key of the XASL cache
 *   user_oid(in)       :
 *   stream(in/out)     : XASL stream, size, xasl_id & xasl_header info
 *                        set to NULL if you want to look up the XASL cache
 *
 * Note: Store the given XASL stream into the XASL file and return its file id.
 * The XASL file is a temporay file, ..
 * If NULL is given as the input argument xasl_stream, this function will look
 * up the XASL cache, and return the cached XASL file id if found. If not found,
 * NULL will be returned.
 */
XASL_ID *
xqmgr_prepare_query (THREAD_ENTRY * thread_p,
		     COMPILE_CONTEXT * context,
		     XASL_STREAM * stream, const OID * user_oid_p)
{
  XASL_CACHE_ENTRY *cache_entry_p;
  char *p;
  int header_size;
  int i;
  OID creator_oid, *class_oid_list_p = NULL;
  int n_oid_list, *tcard_list_p = NULL;
  int dbval_cnt;
  XASL_ID temp_xasl_id;

  /* If xasl_stream is NULL, it means that the client requested looking up
     the XASL cache to know there's a reusable execution plan (XASL) for
     this query. The XASL is stored as a file so that the XASL file id
     (XASL_ID) will be returned if found in the cache. */

  if (stream->xasl_stream == NULL)
    {
      XASL_ID_SET_NULL (stream->xasl_id);

      /* lookup the XASL cache with the query string as the key */
      cache_entry_p =
	qexec_lookup_xasl_cache_ent (thread_p, context->sql_hash_text,
				     user_oid_p);

      if (cache_entry_p)
	{
	  XASL_ID_COPY (stream->xasl_id, &(cache_entry_p->xasl_id));
	  if (stream->xasl_header)
	    {
	      /* also xasl header was requested */
	      qfile_load_xasl_node_header (thread_p, stream->xasl_id,
					   stream->xasl_header);
	    }

	  (void) qexec_remove_my_tran_id_in_xasl_entry (thread_p,
							cache_entry_p, true);
	}

      return stream->xasl_id;
    }

  /* xasl_stream is given. It means that the client generated a XASL for
     this query and requested to store it. As a matter of course, the XASL
     cache will be updated after saving the XASL stream into the file.
     The XASL file id (XASL_ID) will be returned if all right. */

  /* at this time, I'd like to look up once again because it is possible
     that the other competing thread which is running the same query has
     updated the cache before me */
  cache_entry_p =
    qexec_lookup_xasl_cache_ent (thread_p, context->sql_hash_text,
				 user_oid_p);
  if (cache_entry_p)
    {
      er_log_debug (ARG_FILE_LINE,
		    "xqmgr_prepare_query: second qexec_lookup_xasl_cache_ent "
		    "qstmt %s\n", context->sql_hash_text);
      XASL_ID_COPY (stream->xasl_id, &(cache_entry_p->xasl_id));

      if (cache_entry_p)
	{
	  (void) qexec_remove_my_tran_id_in_xasl_entry (thread_p,
							cache_entry_p, true);
	}
      goto exit_on_end;
    }

  if (qfile_store_xasl (thread_p, stream) == 0)
    {
      er_log_debug (ARG_FILE_LINE,
		    "xqmgr_prepare_query: qfile_store_xasl failed\n");
      goto exit_on_error;
    }

  /* save the returned XASL_ID for check later */
  XASL_ID_COPY (&temp_xasl_id, stream->xasl_id);

  /* get some information from the XASL stream */
  p = or_unpack_int ((char *) stream->xasl_stream, &header_size);
  p = or_unpack_int (p, &dbval_cnt);
  p = or_unpack_oid (p, &creator_oid);
  p = or_unpack_int (p, &n_oid_list);

  if (n_oid_list > 0)
    {
      class_oid_list_p = (OID *) malloc (sizeof (OID) * n_oid_list);
      tcard_list_p = (int *) malloc (sizeof (int) * n_oid_list);
      if (class_oid_list_p == NULL || tcard_list_p == NULL)
	{
	  goto exit_on_error;
	}

      for (i = 0; i < n_oid_list; i++)
	{
	  p = or_unpack_oid (p, &class_oid_list_p[i]);
	}
      for (i = 0; i < n_oid_list; i++)
	{
	  p = or_unpack_int (p, &tcard_list_p[i]);
	}
    }
  else
    {
      class_oid_list_p = NULL;
      tcard_list_p = NULL;
    }

  cache_entry_p =
    qexec_update_xasl_cache_ent (thread_p, context, stream,
				 &creator_oid, n_oid_list,
				 class_oid_list_p, tcard_list_p, dbval_cnt);
  if (cache_entry_p == NULL)
    {
      XASL_ID *xasl_id = stream->xasl_id;
      er_log_debug (ARG_FILE_LINE,
		    "xsm_query_prepare: qexec_update_xasl_cache_ent failed xasl_id { first_vpid { %d %d } temp_vfid { %d %d } }\n",
		    xasl_id->first_vpid.pageid, xasl_id->first_vpid.volid,
		    xasl_id->temp_vfid.fileid, xasl_id->temp_vfid.volid);
      (void) file_destroy (thread_p, &xasl_id->temp_vfid);

      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);

      goto exit_on_error;
    }

  /* check whether qexec_update_xasl_cache_ent() changed the XASL_ID */
  if (!XASL_ID_EQ (&temp_xasl_id, stream->xasl_id))
    {
      XASL_ID *xasl_id = stream->xasl_id;
      er_log_debug (ARG_FILE_LINE,
		    "xqmgr_prepare_query: qexec_update_xasl_cache_ent changed xasl_id { first_vpid { %d %d } temp_vfid { %d %d } } to xasl_id { first_vpid { %d %d } temp_vfid { %d %d } }\n",
		    temp_xasl_id.first_vpid.pageid,
		    temp_xasl_id.first_vpid.volid,
		    temp_xasl_id.temp_vfid.fileid,
		    temp_xasl_id.temp_vfid.volid,
		    xasl_id->first_vpid.pageid, xasl_id->first_vpid.volid,
		    xasl_id->temp_vfid.fileid, xasl_id->temp_vfid.volid);
      /* the other competing thread which is running the has updated the cache
         very after the moment of the previous check;
         simply abandon my XASL file */
      (void) file_destroy (thread_p, &temp_xasl_id.temp_vfid);
    }

exit_on_end:

  if (class_oid_list_p)
    {
      free_and_init (class_oid_list_p);
    }
  if (tcard_list_p)
    {
      free_and_init (tcard_list_p);
    }

  return stream->xasl_id;

exit_on_error:

  stream->xasl_id = NULL;
  goto exit_on_end;
}

/*
 * xqmgr_unpack_xasl_tree () -
 *   return: if successful, return 0, otherwise non-zero error code
 *   thread_p(in)         :
 *   xasl_id_p(in)        : XASL file id that was a result of prepare_query()
 *   xasl_stream      :
 *   xasl_stream_size :
 *   cache_clone_p       :
 *   xasl_tree(out)      : pointer to where to return the
 *                         root of the unpacked XASL tree
 *   xasl_unpack_info_ptr(out)   : pointer to where to return the pack info
 *
 * Note: load xasl stream and
 *       unpack the linear byte stream in disk representation to an XASL tree.
 *
 * Note: the caller is responsible for freeing the memory of
 * xasl_unpack_info_ptr. The free function is stx_free_xasl_unpack_info().
 */
static int
xqmgr_unpack_xasl_tree (THREAD_ENTRY * thread_p,
			const XASL_ID * xasl_id,
			char *xasl_stream, int xasl_stream_size,
			XASL_CACHE_CLONE * cache_clone_p,
			XASL_NODE ** xasl_tree)
{
  char *xstream;
  int xstream_size;
  int ret = NO_ERROR;

  assert (xasl_id != NULL);

  xstream = NULL;
  xstream_size = 0;

  /* not found clone */
  if (cache_clone_p == NULL || cache_clone_p->xasl == NULL)
    {
      assert (xasl_stream == NULL);
      assert (xasl_stream_size == 0);

      /* load the XASL stream from the file of xasl_id */
      if (qfile_load_xasl (thread_p, xasl_id, &xstream, &xstream_size) == 0)
	{
	  er_log_debug (ARG_FILE_LINE,
			"xqmgr_unpack_xasl_tree: qfile_load_xasl failed"
			" xasl_id { first_vpid { %d %d } "
			"temp_vfid { %d %d } }\n",
			xasl_id->first_vpid.pageid,
			xasl_id->first_vpid.volid,
			xasl_id->temp_vfid.fileid, xasl_id->temp_vfid.volid);

	  goto exit_on_error;
	}

      /* unpack the XASL stream to the XASL tree for execution */
      if (stx_map_stream_to_xasl (thread_p, xasl_tree,
				  xstream, xstream_size) != NO_ERROR)
	{
	  goto exit_on_error;
	}

      if (cache_clone_p)
	{
	  assert (cache_clone_p->xasl == NULL);

	  /* save unpacked XASL tree info */
	  cache_clone_p->xasl = *xasl_tree;
	}
    }
  else
    {
      /* get previously unpacked XASL tree info */
      *xasl_tree = cache_clone_p->xasl;
#if defined(SERVER_MODE)
      (void) css_set_private_heap (thread_p,
				   cache_clone_p->xasl->private_heap_id);
#endif
    }

  assert (*xasl_tree != NULL);

end:

  /* free xasl_stream allocated in the qfile_load_xasl() */
  if (xstream)
    {
      free_and_init (xstream);
    }

  return ret;

exit_on_error:

  if (ret == NO_ERROR)
    {
      ret = ER_FAILED;
    }
  goto end;
}

/*
 * qmgr_process_query () - Execute a prepared query as sync mode
 *   return: query result file id
 *   thread_p(in)   :
 *   xasl_id(in)        : XASL file id that was a result of prepare_query()
 *   xasl_stream(in)        :
 *   xasl_stream_size(in)        :
 *   dbval_count(in)      : number of host variables
 *   dbvals_p(in) : array of host variables (query input parameters)
 *   flag(in)  : flag to determine if this is an asynchronous query
 *   cache_clone_p  :
 *   query_p        :
 *   tran_entry_p   :
 *   shard_groupid(in)    :
 *   shard_key(in)  :
 *
 * Note1: The query result is returned through a list id (actually the list
 * file). Query id is put for further refernece to this query entry.
 * If there's an error, NULL will be returned.
 *
 * Note2: It is the caller's responsibility to free output QFILE_LIST_ID
 * by calling QFILE_FREE_AND_INIT_LIST_ID().
 */
static QFILE_LIST_ID *
qmgr_process_query (THREAD_ENTRY * thread_p,
		    const XASL_ID * xasl_id,
		    char *xasl_stream, int xasl_stream_size,
		    int dbval_count, DB_VALUE * dbvals_p,
		    QUERY_FLAG flag,
		    XASL_CACHE_CLONE * cache_clone_p,
		    QMGR_QUERY_ENTRY * query_p,
		    QMGR_TRAN_ENTRY * tran_entry_p, int shard_groupid,
		    DB_VALUE * shard_key)
{
  XASL_NODE *xasl_p;
  QFILE_LIST_ID *list_id;
  bool req_from_migrator = false;

  assert (xasl_id != NULL);
  assert (query_p != NULL);
  assert (tran_entry_p != NULL);

  xasl_p = NULL;
  list_id = NULL;

  if (xqmgr_unpack_xasl_tree
      (thread_p, xasl_id, xasl_stream, xasl_stream_size, cache_clone_p,
       &xasl_p) != NO_ERROR)
    {
      goto exit_on_error;
    }

  if (xasl_id)
    {
      /* check the number of the host variables for this XASL */
      if (xasl_p->dbval_cnt > dbval_count)
	{
	  er_log_debug (ARG_FILE_LINE,
			"qmgr_process_query: dbval_cnt mismatch %d vs %d\n",
			xasl_p->dbval_cnt, dbval_count);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE,
		  0);
	  goto exit_on_error;
	}
    }

  if (flag & REQUEST_FROM_MIGRATOR)
    {
      req_from_migrator = true;
    }

  /* execute the query with the value list, if any */
  query_p->list_id = qexec_execute_query (thread_p, xasl_p,
					  dbval_count, dbvals_p,
					  query_p->query_id, shard_groupid,
					  shard_key, req_from_migrator);

  if (cache_clone_p == NULL)
    {
      (void) qexec_clear_xasl (thread_p, xasl_p, true);
    }

  /* Note: qexec_execute_query() returns listid (NOT NULL)
     even if an error was occurred.
     We should check the error condition and free listid. */
  if (query_p->errid < 0)
    {				/* error has occurred */
      if (query_p->list_id)
	{
	  QFILE_FREE_AND_INIT_LIST_ID (query_p->list_id);
	}
      /* error occurred during executing the query */
      goto exit_on_error;
    }

  assert (query_p->list_id != NULL);

  /* allocate new QFILE_LIST_ID to be returned as the result and copy from
     the query result; the caller is responsible to free this */
  list_id = qfile_clone_list_id (query_p->list_id, false);
  if (list_id == NULL)
    {
      goto exit_on_error;
    }
  assert (list_id->sort_list == NULL);

  list_id->last_pgptr = NULL;

end:

  return list_id;

exit_on_error:

  assert (list_id == NULL);
  goto end;
}

static bool
qmgr_set_xasl_trace (UNUSED_ARG THREAD_ENTRY * thread_p, QUERY_FLAG * flag_p)
{
  bool xasl_trace;

  xasl_trace = IS_XASL_TRACE_TEXT (*flag_p) || IS_XASL_TRACE_JSON (*flag_p);

  if (xasl_trace == true)
    {
      thread_trace_on (thread_p);
#if defined (ENABLE_UNUSED_FUNCTION)
      xmnt_server_start_stats (thread_p, false);
#endif

      if (IS_XASL_TRACE_TEXT (*flag_p))
	{
	  thread_set_trace_format (thread_p, QUERY_TRACE_TEXT);
	}
      else if (IS_XASL_TRACE_JSON (*flag_p))
	{
	  thread_set_trace_format (thread_p, QUERY_TRACE_JSON);
	}
    }

  return xasl_trace;
}

/*
 * xqmgr_execute_query () - Execute a prepared query
 *   return: query result file id
 *   thrd(in)   :
 *   xasl_id(in)        : XASL file id that was a result of prepare_query()
 *   query_idp(out)     : query id to be used for getting results
 *   dbval_count(in)      : number of host variables
 *   dbval_p(in) : array of host variables (query input parameters)
 *   flagp(in)  : flag to determine if this is an asynchronous query
 *   query_timeout(in) : query_timeout in millisec.
 *   shard_groupid(in) :
 *   shard_key(in) :
 *
 * Note1: The query result is returned through a list id (actually the list
 * file). Query id is put for further refernece to this query entry.
 * If there's an error, NULL will be returned.
 *
 * Note2: It is the caller's responsibility to free output QFILE_LIST_ID
 * by calling QFILE_FREE_AND_INIT_LIST_ID().
 */
QFILE_LIST_ID *
xqmgr_execute_query (THREAD_ENTRY * thread_p,
		     const XASL_ID * xasl_id_p, QUERY_ID * query_id_p,
		     int dbval_count, void *dbval_p, QUERY_FLAG * flag_p,
		     UNUSED_ARG int query_timeout, int shard_groupid,
		     DB_VALUE * shard_key,
		     XASL_CACHE_ENTRY ** ret_cache_entry_p)
{
  XASL_CACHE_ENTRY *xasl_cache_entry_p;
  DB_VALUE *dbvals_p;
#if defined (SERVER_MODE)
  DB_VALUE *dbval;
  char *data;
  int i;
#endif
  QMGR_QUERY_ENTRY *query_p;
  int tran_index = -1;
  QMGR_TRAN_ENTRY *tran_entry_p;
  QFILE_LIST_ID *list_id_p;
  XASL_CACHE_CLONE *cache_clone_p;
  bool xasl_trace;

  assert (xasl_id_p != NULL);

  query_p = NULL;
  *query_id_p = -1;
  list_id_p = NULL;
  xasl_cache_entry_p = NULL;

  dbvals_p = NULL;
#if defined (SERVER_MODE)
  data = (char *) dbval_p;
#endif

  assert (shard_groupid != NULL_GROUPID);
  if (shard_key != NULL)
    {
      assert (!DB_IS_NULL (shard_key));
      assert (TP_IS_CHAR_TYPE (DB_VALUE_DOMAIN_TYPE (shard_key)));
    }
  else
    {
      assert (shard_groupid == GLOBAL_GROUPID);
    }

  assert (thread_get_recursion_depth (thread_p) == 0);

  xasl_trace = qmgr_set_xasl_trace (thread_p, flag_p);

  /* Check the existence of the given XASL. If someone marked it
     to be deleted, then remove it if possible. */
  cache_clone_p = NULL;		/* mark as pop */
  xasl_cache_entry_p = qexec_check_xasl_cache_ent_by_xasl (thread_p,
							   xasl_id_p,
							   dbval_count,
							   &cache_clone_p,
							   false);
  if (xasl_cache_entry_p == NULL)
    {
      /* It doesn't be there or was marked to be deleted. */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_XASLNODE, 0);
      return NULL;
    }

  if (ret_cache_entry_p)
    {
      *ret_cache_entry_p = xasl_cache_entry_p;
    }

#if defined (SERVER_MODE)
  if (dbval_count)
    {
      char *ptr;

      assert (data != NULL);

      dbvals_p = (DB_VALUE *) malloc (sizeof (DB_VALUE) * dbval_count);
      if (dbvals_p == NULL)
	{
	  goto exit_on_error;
	}

      /* unpack DB_VALUEs from the received data */
      ptr = data;
      for (i = 0, dbval = dbvals_p; i < dbval_count; i++, dbval++)
	{
	  ptr = or_unpack_db_value (ptr, dbval);
	}
    }
#else
  dbvals_p = (DB_VALUE *) dbval_p;
#endif

  /* If it is not inhibited from getting the cached result, inspect the list
     cache (query result cache) and get the list file id(QFILE_LIST_ID) to be
     returned to the client if it is in there. The list cache will be
     searched with the XASL cache entry of the target query that is obtained
     from the XASL_ID, because all results of the query with different
     parameters (host variables - DB_VALUES) are linked at
     the XASL cache entry. */

  if (copy_bind_value_to_tdes (thread_p, dbval_count, dbvals_p) != NO_ERROR)
    {
      goto exit_on_error;
    }

  /* Make an query entry */
  /* mark that this transaction is running a query */
  tran_index = logtb_get_current_tran_index (thread_p);

  assert (qmgr_Query_table.tran_entries_p != NULL);
  assert (qmgr_Query_table.num_trans > tran_index);

  tran_entry_p = &qmgr_Query_table.tran_entries_p[tran_index];

#if defined (SERVER_MODE)
  qmgr_lock_mutex (thread_p, &tran_entry_p->lock);
  tran_entry_p->trans_stat = QMGR_TRAN_RUNNING;

  /* allocate a new query entry */
  query_p = qmgr_allocate_query_entry (thread_p, tran_entry_p);
  qmgr_unlock_mutex (&tran_entry_p->lock);

  /* set a timeout if necessary */
  qmgr_set_query_exec_info_to_tdes (tran_index, query_timeout, xasl_id_p);
#else
  /* allocate a new query entry */
  query_p = qmgr_allocate_query_entry (thread_p, tran_entry_p);
#endif

  if (query_p == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QM_QENTRY_RUNOUT, 1,
	      QMGR_MAX_QUERY_ENTRY_PER_TRAN);
      goto exit_on_error;
    }

  /* initialize query entry */
  XASL_ID_COPY (&query_p->xasl_id, xasl_id_p);
  query_p->xasl_ent = xasl_cache_entry_p;
  query_p->query_mode = SYNC_MODE;
  query_p->query_flag = *flag_p;
#if defined (SERVER_MODE)
  if (thread_p != NULL)
    {
      query_p->tid = thread_p->tid;
    }
#endif
  if (*flag_p & RESULT_HOLDABLE)
    {
      query_p->is_holdable = true;
    }
  else
    {
      query_p->is_holdable = false;
    }

  /* add the entry to the query table */
  qmgr_add_query_entry (thread_p, query_p, tran_index);

  /* to return query id */
  *query_id_p = query_p->query_id;

  /* If the result didn't come from the cache, build the execution plan
     (XASL tree) from the cached(stored) XASL stream. */

  list_id_p = qmgr_process_query (thread_p, xasl_id_p,
				  NULL, 0,
				  dbval_count, dbvals_p, *flag_p,
				  cache_clone_p, query_p, tran_entry_p,
				  shard_groupid, shard_key);
  if (list_id_p == NULL)
    {
      goto exit_on_error;
    }

  /* everything is ok, mark that the query is completed */
  qmgr_mark_query_as_completed (query_p);

end:

#if defined (SERVER_MODE)
  if (dbvals_p)
    {
      for (i = 0, dbval = dbvals_p; i < dbval_count; i++, dbval++)
	{
	  db_value_clear (dbval);
	}
      free_and_init (dbvals_p);
    }
#endif

  /* save XASL tree */
  if (cache_clone_p)
    {
      (void) qexec_check_xasl_cache_ent_by_xasl (thread_p, xasl_id_p, -1,
						 &cache_clone_p, true);
    }

#if defined (SERVER_MODE)
  qmgr_reset_query_exec_info (tran_index);
#endif

  return list_id_p;

exit_on_error:

  if (query_p)
    {
      /* mark that the query is completed and then delete this query entry */
      qmgr_mark_query_as_completed (query_p);

      if (qmgr_free_query_temp_file_by_query_entry (thread_p,
						    query_p,
						    tran_index) != NO_ERROR)
	{
	  assert (er_errid () == ER_INTERRUPTED);
	}

      qmgr_delete_query_entry (thread_p, query_p->query_id, tran_index);
    }

  *query_id_p = 0;

  /* free QFILE_LIST_ID */
  if (list_id_p)
    {
      QFILE_FREE_AND_INIT_LIST_ID (list_id_p);
    }

  if (xasl_trace == true)
    {
#if defined (ENABLE_UNUSED_FUNCTION)
      xmnt_server_stop_stats (thread_p);
#endif
    }

  goto end;
}

/*
 * copy_bind_value_to_tdes - copy bind values to transaction descriptor
 * return:
 *   thread_p(in):
 *   num_bind_vals(in):
 *   bind_vals(in):
 */
static int
copy_bind_value_to_tdes (THREAD_ENTRY * thread_p, int num_bind_vals,
			 DB_VALUE * bind_vals)
{
  LOG_TDES *tdes;
  DB_VALUE *vals;
  int i;

  tdes = logtb_get_current_tdes (thread_p);
  if (tdes == NULL)
    {
      return ER_FAILED;
    }

  if (tdes != NULL && tdes->num_exec_queries < MAX_NUM_EXEC_QUERY_HISTORY)
    {
      tdes->bind_history[tdes->num_exec_queries].vals = NULL;
      tdes->bind_history[tdes->num_exec_queries].size = num_bind_vals;

      if (num_bind_vals > 0)
	{
	  vals = (DB_VALUE *) malloc (sizeof (DB_VALUE) * num_bind_vals);
	  if (vals == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1,
		      sizeof (DB_VALUE) * num_bind_vals);

	      return ER_OUT_OF_VIRTUAL_MEMORY;
	    }

	  for (i = 0; i < num_bind_vals; i++)
	    {
	      DB_MAKE_NULL (&vals[i]);
	      pr_clone_value (&bind_vals[i], &vals[i]);
	    }

	  tdes->bind_history[tdes->num_exec_queries].vals = vals;
	}
    }

  tdes->num_exec_queries++;
  return NO_ERROR;
}

/*
 * xqmgr_end_query () -
 *   return: int (NO_ERROR or ER_FAILED)
 *   thrd(in)   : this thread handle
 *   query_id(in)       : Query Identifier
 *
 * Note: The query result file is destroyed for the specified query.
 * If the query is not repetitive, this calls also removes the
 * query entry from the server query table and invalidates the
 * query identifier. If the query result file destruction fails,
 * ER_FAILED code is returned, but still query entry is removed
 * query identifier is invalidated for unrepetitive queries.
 */
int
xqmgr_end_query (THREAD_ENTRY * thread_p, QUERY_ID query_id)
{
  QMGR_QUERY_ENTRY *query_p = NULL;
  QMGR_TRAN_ENTRY *tran_entry_p = NULL;
  int tran_index, rc = NO_ERROR;
#if defined (SERVER_MODE)
  int rv;
#endif

  tran_index = logtb_get_current_tran_index (thread_p);

  /* get query entry */
  if (qmgr_Query_table.tran_entries_p == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_QPROC_UNKNOWN_QUERYID,
	      1, query_id);
      return ER_FAILED;
    }

  assert (qmgr_Query_table.tran_entries_p != NULL);
  assert (qmgr_Query_table.num_trans > tran_index);

  tran_entry_p = &qmgr_Query_table.tran_entries_p[tran_index];

  qmgr_lock_mutex (thread_p, &tran_entry_p->lock);

  query_p = qmgr_find_query_entry (tran_entry_p->query_entry_list_p,
				   query_id);
  if (query_p == NULL)
    {
      /* maybe this is:
       *    a completed empty query entry: DML or SELECT's empty result
       * or a holdable result and we'll find it in the session state object
       */
      xsession_remove_query_entry_info (thread_p, query_id);
      qmgr_unlock_mutex (&tran_entry_p->lock);
      return NO_ERROR;
    }

  if (query_p->is_holdable)
    {
      /* We also need to remove the associated query from the session.
         The call below will not destroy the associated list files */
      xsession_clear_query_entry_info (thread_p, query_id);
    }

#if defined (SERVER_MODE)
  rv = pthread_mutex_lock (&query_p->lock);
  if (query_p->query_mode != QUERY_COMPLETED)
    {
      logtb_set_tran_index_interrupt (thread_p, tran_index, true);
      query_p->interrupt = true;
      query_p->propagate_interrupt = false;
      qmgr_unlock_mutex (&tran_entry_p->lock);
      query_p->nwaits++;

      rv = pthread_cond_wait (&query_p->cond, &query_p->lock);
      query_p->nwaits--;

      query_p->interrupt = false;
      logtb_set_tran_index_interrupt (thread_p, tran_index, false);

      pthread_mutex_unlock (&query_p->lock);
    }
  else
    {
      pthread_mutex_unlock (&query_p->lock);
      qmgr_unlock_mutex (&tran_entry_p->lock);
    }
#endif

  /* destroy query result list file */
  if (query_p->list_id)
    {
      QFILE_FREE_AND_INIT_LIST_ID (query_p->list_id);

      /* free external volumes, if any */
      rc = qmgr_free_query_temp_file_by_query_entry (thread_p, query_p,
						     tran_index);
    }

  XASL_ID_SET_NULL (&query_p->xasl_id);
  qmgr_delete_query_entry (thread_p, query_p->query_id, tran_index);

  return rc;
}

/*
 * xqmgr_drop_query_plan () - Drop the stored query plan
 *   return: NO_ERROR or ER_FAILED
 *   qstmt(in)      : query string; used for hash key of the XASL cache
 *   user_oid(in)       :
 *   xasl_id(in)        : XASL file id which contains the XASL stream
 *
 * Note: Delete the XASL cache specified by either the query string or the XASL
 * file id upon request of the client.
 */
int
xqmgr_drop_query_plan (THREAD_ENTRY * thread_p, const char *qstmt,
		       const OID * user_oid_p, const XASL_ID * xasl_id_p)
{
  if (qstmt && user_oid_p)
    {
      /* delete the XASL cache entry */
      if (qexec_remove_xasl_cache_ent_by_qstr (thread_p, qstmt,
					       user_oid_p) != NO_ERROR)
	{
	  er_log_debug (ARG_FILE_LINE,
			"xqmgr_drop_query_plan: xs_remove_xasl_cache_ent_by_qstr failed for query_str %s\n",
			qstmt);
	  return ER_FAILED;
	}
    }

  if (xasl_id_p)
    {
      /* delete the XASL cache entry */
      if (qexec_remove_xasl_cache_ent_by_xasl (thread_p, xasl_id_p) !=
	  NO_ERROR)
	{
	  er_log_debug (ARG_FILE_LINE,
			"xqmgr_drop_query_plan: xs_remove_xasl_cache_ent_by_xasl failed for xasl_id { first_vpid { %d %d } temp_vfid { %d %d } }\n",
			xasl_id_p->first_vpid.pageid,
			xasl_id_p->first_vpid.volid,
			xasl_id_p->temp_vfid.fileid,
			xasl_id_p->temp_vfid.volid);
	  return ER_FAILED;
	}
    }

  return NO_ERROR;
}

/*
 * xqmgr_drop_all_query_plans () - Drop all the stored query plans
 *   return: NO_ERROR or ER_FAILED
 *
 * Note: Clear all XASL cache entries out upon request of the client.
 */
int
xqmgr_drop_all_query_plans (THREAD_ENTRY * thread_p)
{
  return qexec_remove_all_xasl_cache_ent_by_xasl (thread_p);
}

/*
 * xqmgr_dump_query_plans () - Dump the content of the XASL cache
 *   return:
 *   outfp(in)  :
 */
void
xqmgr_dump_query_plans (THREAD_ENTRY * thread_p, FILE * out_fp)
{
  (void) qexec_dump_xasl_cache_internal (thread_p, out_fp, 7);
}

/*
 *       	       TRANSACTION COORDINATION ROUTINES
 */

/*
 * qmgr_clear_trans_wakeup () -
 *   return:
 *   tran_index(in)     : Log Transaction index
 *   tran_died(in)      : Flag to indicate if the transaction has died
 *   is_abort(in)       :
 *
 * Note: This routine is called by the transaction manager and perfoms
 * a clean_up processing for the given transaction index. For
 * each non-repetitive query (that is not currently executing)
 * issued by the transaction, it
 * destroys the query result file, the XASL tree plan and
 * invalidates the query entry(identifier). For each repetitive
 * query issued by the transaction, it destroys the query result
 * file, however it destroys the XASL tree plan and the query
 * entry(identifier), only if the transaction has died. The XASL
 * tree plan for repetitive queries is kept for aborted
 * transactions because it can still be used by the transaction
 * to execute queries.
 */
void
qmgr_clear_trans_wakeup (THREAD_ENTRY * thread_p, int tran_index,
			 bool is_tran_died, bool is_abort)
{
  QMGR_QUERY_ENTRY *query_p, *t;
  QMGR_TRAN_ENTRY *tran_entry_p;
#if defined (SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */

  /* for bulletproofing check if tran_index is a valid index,
   * note that normally this should never happen...
   */
  if (tran_index >= qmgr_Query_table.num_trans
#if defined (SERVER_MODE)
      || tran_index == LOG_SYSTEM_TRAN_INDEX
#endif
    )
    {
#ifdef QP_DEBUG
      er_log_debug (ARG_FILE_LINE, "qmgr_clear_trans_wakeup:"
		    "Invalid transaction index %d called...\n", tran_index);
#endif
      return;
    }

  assert (qmgr_Query_table.tran_entries_p != NULL);
  assert (qmgr_Query_table.num_trans > tran_index);

  tran_entry_p = &qmgr_Query_table.tran_entries_p[tran_index];

  if (tran_entry_p->query_entry_list_p == NULL)
    {
      return;
    }

  qmgr_lock_mutex (thread_p, &tran_entry_p->lock);
  query_p = tran_entry_p->query_entry_list_p;

#if defined (SERVER_MODE)
  /* interrupt all active queries */
  while (query_p != NULL)
    {
      rv = pthread_mutex_lock (&query_p->lock);

      if (query_p->query_mode != QUERY_COMPLETED)
	{
	  logtb_set_tran_index_interrupt (thread_p, tran_index, true);
	  query_p->interrupt = true;
	}

      pthread_mutex_unlock (&query_p->lock);
      query_p = query_p->next;
    }
#endif

#if defined (SERVER_MODE)
/* check if all active queries are finished */
again:
  query_p = tran_entry_p->query_entry_list_p;
  if (query_p == NULL)
    {
      qmgr_unlock_mutex (&tran_entry_p->lock);
      return;
    }

  while (query_p != NULL)
    {
      rv = pthread_mutex_lock (&query_p->lock);
      /* If I'm the async_query executor, "q_ptr->query_mode" will be set to
       * QUERY_COMPLETED by me. Do you feel uneasy about skipping my query ?
       * Q : Why don't you use "thread_get_thread_entry_info()->tid" instead of
       * "THREAD_ID()" ? */
      if (query_p->query_mode != QUERY_COMPLETED
	  && query_p->tid != pthread_self ())
	{
	  qmgr_unlock_mutex (&tran_entry_p->lock);

	  query_p->nwaits++;
	  pthread_cond_wait (&query_p->cond, &query_p->lock);
	  query_p->nwaits--;
	  pthread_mutex_unlock (&query_p->lock);

	  qmgr_lock_mutex (thread_p, &tran_entry_p->lock);
	  goto again;
	}
      pthread_mutex_unlock (&query_p->lock);
      query_p = query_p->next;
    }
#endif

  query_p = tran_entry_p->query_entry_list_p;
  if (query_p == NULL)
    {
      qmgr_unlock_mutex (&tran_entry_p->lock);
      return;
    }

  while (query_p)
    {
      if (query_p->is_holdable && !is_abort && !is_tran_died)
	{
	  /* this is a commit and we have to add the result to the holdable
	     queries list */
	  xsession_store_query_entry_info (thread_p, query_p);
	  /* reset result info */
	  query_p->list_id = NULL;
	  query_p->qe_temp_vfid = NULL;
	}
      /* destroy the query result if not destroyed yet */
      if (query_p->list_id)
	{
	  qfile_close_list (thread_p, query_p->list_id);
	  QFILE_FREE_AND_INIT_LIST_ID (query_p->list_id);
	}

      /* Note: In cases of abort, the qm must delete its own
       * tempfiles otherwise the file manager will delete them out from
       * under us, leaving us with dangling list_id's and such.  See the
       * functions file_new_destroy_all_tmp and log_abort_local.
       */
      if (query_p->qe_temp_vfid != NULL)
	{
	  qmgr_free_query_temp_file_by_query_entry (thread_p, query_p,
						    tran_index);
	}

      XASL_ID_SET_NULL (&query_p->xasl_id);
      /* if there were external volumes created for the transaction,
       * free them so that they can be used by coming transactions.
       */
      if (qmgr_free_query_temp_file_by_query_entry (thread_p, query_p,
						    tran_index) != NO_ERROR)
	{
#ifdef QP_DEBUG
	  er_log_debug (ARG_FILE_LINE, "qm_clear_trans_wakeup: "
			"External volume deletion failed.\n");
#endif
	}
      /* remove query entry */
      t = query_p;

      tran_entry_p->query_entry_list_p = query_p->next;
      query_p = tran_entry_p->query_entry_list_p;

      qmgr_free_query_entry (thread_p, tran_entry_p, t);
    }

  if (tran_entry_p->query_entry_list_p == NULL)
    {
      tran_entry_p->trans_stat = QMGR_TRAN_TERMINATED;
    }

  qmgr_unlock_mutex (&tran_entry_p->lock);
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * qmgr_get_tran_status () -
 *   return:
 *   tran_index(in)     :
 */
QMGR_TRAN_STATUS
qmgr_get_tran_status (THREAD_ENTRY * thread_p, int tran_index)
{
  if (tran_index >= 0)
    {
      tran_index = logtb_get_current_tran_index (thread_p);
    }

  assert (qmgr_Query_table.tran_entries_p != NULL);
  assert (qmgr_Query_table.num_trans > tran_index);

  return qmgr_Query_table.tran_entries_p[tran_index].trans_stat;
}

/*
 * qmgr_set_tran_status () -
 *   return:
 *   tran_index(in)     :
 *   trans_status(in)   :
 */
void
qmgr_set_tran_status (THREAD_ENTRY * thread_p, int tran_index,
		      QMGR_TRAN_STATUS trans_status)
{
  if (tran_index >= 0)
    {
      tran_index = logtb_get_current_tran_index (thread_p);
    }

  assert (qmgr_Query_table.tran_entries_p != NULL);
  assert (qmgr_Query_table.num_trans > tran_index);

  qmgr_Query_table.tran_entries_p[tran_index].trans_stat = trans_status;
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 *       	     PAGE ALLOCATION/DEALLOCATION ROUTINES
 */

/*
 * qmgr_get_old_page () -
 *   return:
 *   vpidp(in)  :
 *   tfile_vfidp(in)    :
 */
PAGE_PTR
qmgr_get_old_page (THREAD_ENTRY * thread_p, VPID * vpid_p,
		   QMGR_TEMP_FILE * tfile_vfid_p)
{
  int tran_index;
  PAGE_PTR page_p;
#if defined(SERVER_MODE)
  bool dummy;
#endif /* SERVER_MODE */

  if (vpid_p->volid == NULL_VOLID && tfile_vfid_p == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_TEMP_FILE,
	      1, logtb_get_current_tran_index (thread_p));
      return NULL;
    }

  if (vpid_p->volid == NULL_VOLID)
    {
      /* return memory buffer */
      tran_index = logtb_get_current_tran_index (thread_p);

      if (vpid_p->pageid >= 0 && vpid_p->pageid <= tfile_vfid_p->membuf_last)
	{
	  /* interrupt check */
#if defined (SERVER_MODE)
	  if (thread_get_check_interrupt (thread_p) == true
	      && logtb_is_interrupted_tran (thread_p, true, &dummy,
					    tran_index) == true)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_INTERRUPTED, 0);
	      page_p = NULL;
	    }
	  else
	    {
	      page_p = tfile_vfid_p->membuf[vpid_p->pageid];
	    }
#else
	  page_p = tfile_vfid_p->membuf[vpid_p->pageid];
#endif
	}
      else
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_INVALID_TEMP_FILE, 1, tran_index);
	  page_p = NULL;
	}
    }
  else
    {
      /* return temp file page */
      page_p = pgbuf_fix (thread_p, vpid_p, OLD_PAGE,
			  PGBUF_LATCH_WRITE, PGBUF_UNCONDITIONAL_LATCH);
    }

  return page_p;
}

/*
 * qmgr_free_old_page () -
 *   return:
 *   page_ptr(in)       :
 *   tfile_vfidp(in)    :
 */
void
qmgr_free_old_page (THREAD_ENTRY * thread_p, PAGE_PTR page_p,
		    QMGR_TEMP_FILE * tfile_vfid_p)
{
  QMGR_PAGE_TYPE page_type;

  if (page_p == NULL)
    {
      assert (0);
      return;
    }
  if (tfile_vfid_p == NULL)
    {
      pgbuf_unfix (thread_p, page_p);
      return;
    }

  page_type = qmgr_get_page_type (page_p, tfile_vfid_p);
  if (page_type == QMGR_UNKNOWN_PAGE)
    {
      assert (false);
      return;
    }

  if (page_type == QMGR_TEMP_FILE_PAGE)
    {
      /* The list files came from list file cache have no tfile_vfid_p. */
      pgbuf_unfix (thread_p, page_p);
    }
#if defined (SERVER_MODE)
  else
    {
      assert (page_type == QMGR_MEMBUF_PAGE);
    }
#endif
}

/*
 * qmgr_set_dirty_page () -
 *   return:
 *   page_ptr(in)       :
 *   free_page(in)      :
 *   addrp(in)  :
 *   tfile_vfidp(in)    :
 */
void
qmgr_set_dirty_page (THREAD_ENTRY * thread_p, PAGE_PTR page_p,
		     int free_page, LOG_DATA_ADDR * addr_p,
		     QMGR_TEMP_FILE * tfile_vfid_p)
{
  QMGR_PAGE_TYPE page_type;

  page_type = qmgr_get_page_type (page_p, tfile_vfid_p);
  if (page_type == QMGR_UNKNOWN_PAGE)
    {
      assert (false);
      return;
    }

  if (page_type == QMGR_TEMP_FILE_PAGE)
    {
      log_skip_logging_set_lsa (thread_p, addr_p);
      pgbuf_set_dirty (thread_p, page_p, free_page);
    }
#if defined (SERVER_MODE)
  else if (free_page == FREE)
    {
      assert (page_type == QMGR_MEMBUF_PAGE);
    }
#endif
}

/*
 * qmgr_get_new_page () -
 *   return: PAGE_PTR
 *   vpidp(in)  : Set to the allocated real page identifier
 *   tfile_vfidp(in)    : Query Associated with the XASL tree
 *
 * Note: A new query file page is allocated and returned. The page
 * fetched and returned, is not locked. This routine is called
 * succesively to allocate pages for the query result files (list
 * files) or XASL tree files.
 * If an error occurs, NULL pointer is returned.
 */
PAGE_PTR
qmgr_get_new_page (THREAD_ENTRY * thread_p, VPID * vpid_p,
		   QMGR_TEMP_FILE * tfile_vfid_p)
{
  PAGE_PTR page_p;

  if (tfile_vfid_p == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_TEMP_FILE,
	      1, logtb_get_current_tran_index (thread_p));
      return NULL;
    }

  /* first page, return memory buffer instead real temp file page */
  if (tfile_vfid_p->membuf != NULL
      && tfile_vfid_p->membuf_last < tfile_vfid_p->membuf_npages - 1)
    {
      vpid_p->volid = NULL_VOLID;
      vpid_p->pageid = ++(tfile_vfid_p->membuf_last);
      return tfile_vfid_p->membuf[tfile_vfid_p->membuf_last];
    }

  /* memory buffer is exhausted; create temp file */
  if (VFID_ISNULL (&tfile_vfid_p->temp_vfid))
    {
      if (file_create_tmp (thread_p, &tfile_vfid_p->temp_vfid,
			   TEMP_FILE_DEFAULT_PAGES, NULL) == NULL)
	{
	  vpid_p->pageid = NULL_PAGEID;
	  return NULL;
	}
      tfile_vfid_p->temp_file_type = FILE_TMP;
      tfile_vfid_p->last_free_page_index =
	file_get_numpages (thread_p, &tfile_vfid_p->temp_vfid, NULL, NULL,
			   NULL) - 1;
    }

  /* try to get pages from an external temp file */
  page_p = qmgr_get_external_file_page (thread_p, vpid_p, tfile_vfid_p);
  if (page_p == NULL)
    {
      /* more temp file page is unavailable; cause error to stop the query */
      vpid_p->pageid = NULL_PAGEID;
      if (er_errid () == ER_FILE_NOT_ENOUGH_PAGES_IN_VOLUME)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_OUT_OF_TEMP_SPACE, 0);
	}
    }

  return page_p;
}

/*
 * qmgr_get_external_file_page () -
 *   return: PAGE_PTR
 *   vpid(in)   : Set to the allocated virtual page identifier
 *   tmp_vfid(in)       : tempfile_vfid struct pointer
 *
 * Note: This function tries to allocate a new page from an external
 * query file, fetchs and returns the page pointer. Since,
 * pages are not shared by different transactions, it does not
 * lock the page on fetching. If it can not allocate a new page,
 * necessary error code is set and NULL pointer is returned.
 */
static PAGE_PTR
qmgr_get_external_file_page (THREAD_ENTRY * thread_p, VPID * vpid_p,
			     QMGR_TEMP_FILE * tmp_vfid_p)
{
  PAGE_PTR page_p;
  int nthpg;
  LOG_DATA_ADDR addr = LOG_ADDR_INITIALIZER;
  DKNPAGES num_pages = TEMP_FILE_DEFAULT_PAGES;
  QFILE_PAGE_HEADER page_header =
    { 0, NULL_PAGEID, NULL_PAGEID, 0, NULL_PAGEID,
    NULL_VOLID, NULL_VOLID, NULL_VOLID
  };

  /*
   * If there are existing pages allocated in the vpid_array[], use them
   * Currently we use file_find_nthpages to allocate xx vpids which are then stored in
   * the query_entry structure. vpid_count is the actual # of pages returned
   * by file_find_nthpages and used to compare against the vpid_index to avoid handing
   * out garbage.
   */

  if (tmp_vfid_p->vpid_index != -1)
    {
      page_p = pgbuf_fix (thread_p,
			  &(tmp_vfid_p->vpid_array[tmp_vfid_p->vpid_index]),
			  NEW_PAGE, PGBUF_LATCH_WRITE,
			  PGBUF_UNCONDITIONAL_LATCH);
      if (page_p == NULL)
	{
	  VPID_SET_NULL (vpid_p);
	  return NULL;
	}

      tmp_vfid_p->curr_free_page_index++;
      *vpid_p = tmp_vfid_p->vpid_array[tmp_vfid_p->vpid_index++];
      qmgr_put_page_header (page_p, &page_header);

      LOG_ADDR_SET (&addr, &tmp_vfid_p->temp_vfid, page_p,
		    -1 /* irrelevant */ );
      log_skip_logging_set_lsa (thread_p, &addr);	/* ignore logging */

      pgbuf_set_dirty (thread_p, page_p, DONT_FREE);

      if (tmp_vfid_p->vpid_index >= tmp_vfid_p->vpid_count)
	{
	  tmp_vfid_p->vpid_index = -1;
	}

      return page_p;
    }

  if (tmp_vfid_p->curr_free_page_index > tmp_vfid_p->last_free_page_index)
    {
      /* existing temporary file needs to be expanded */

      /*
       * allocate next extent of pages for the file
       * Don't care about initializing the pages
       */
      if (file_alloc_pages_as_noncontiguous
	  (thread_p, &tmp_vfid_p->temp_vfid, vpid_p, &nthpg, num_pages,
	   NULL, NULL, NULL, NULL) == NULL)
	{
	  /* if error was no more pages, ignore this expected error code */
	  if (er_errid () != ER_FILE_NOT_ENOUGH_PAGES_IN_VOLUME)
	    {
	      return NULL;
	    }

	  /* Find out how many pages are available and ask for that many pages.
	   * Since we are in a multi-user environment, we have no guarantee that
	   * we'll be able to get the number of pages maxpgs_could_allocate()
	   * returns.  Also right before we call maxpgs, another user may free
	   * up a bunch of pages, so only ask for the min of num_pages and
	   * our current request.
	   */
	  num_pages =
	    file_find_maxpages_allocable (thread_p, &tmp_vfid_p->temp_vfid);
	  if (num_pages <= 0)
	    {
	      return NULL;
	    }

	  /*
	   * allocate next extent of pages for the file
	   * Don't care about initializing the pages
	   */
	  if (file_alloc_pages_as_noncontiguous (thread_p,
						 &tmp_vfid_p->temp_vfid,
						 vpid_p, &nthpg, num_pages,
						 NULL, NULL, NULL,
						 NULL) == NULL)
	    {
	      return NULL;
	    }
	}

      /* reset file page indices information */
      tmp_vfid_p->last_free_page_index += num_pages;
    }

  /* fetch and return the external volume page */
  tmp_vfid_p->vpid_count =
    file_find_nthpages (thread_p, &tmp_vfid_p->temp_vfid,
			tmp_vfid_p->vpid_array,
			tmp_vfid_p->curr_free_page_index,
			QMGR_VPID_ARRAY_SIZE);
  if (tmp_vfid_p->vpid_count == -1)
    {
      tmp_vfid_p->vpid_index = -1;
      return NULL;
    }

  tmp_vfid_p->curr_free_page_index++;
  tmp_vfid_p->vpid_index = 0;
  tmp_vfid_p->total_count += num_pages;

  page_p = pgbuf_fix (thread_p,
		      &(tmp_vfid_p->vpid_array[tmp_vfid_p->vpid_index]),
		      NEW_PAGE, PGBUF_LATCH_WRITE, PGBUF_UNCONDITIONAL_LATCH);
  if (page_p == NULL)
    {
      VPID_SET_NULL (vpid_p);
      return NULL;
    }

  *vpid_p = tmp_vfid_p->vpid_array[tmp_vfid_p->vpid_index++];
  qmgr_put_page_header (page_p, &page_header);

  if (tmp_vfid_p->vpid_index >= tmp_vfid_p->vpid_count)
    {
      tmp_vfid_p->vpid_index = -1;
    }

  LOG_ADDR_SET (&addr, &tmp_vfid_p->temp_vfid, page_p, -1 /* irrelevant */ );
  log_skip_logging_set_lsa (thread_p, &addr);	/* ignore logging */

  pgbuf_set_dirty (thread_p, page_p, DONT_FREE);

  return page_p;
}

static QMGR_TEMP_FILE *
qmgr_allocate_tempfile_with_buffer (int num_buffer_pages)
{
  size_t size;
  QMGR_TEMP_FILE *tempfile_p;

  size = DB_ALIGN (sizeof (QMGR_TEMP_FILE), MAX_ALIGNMENT);
  size += DB_ALIGN (sizeof (PAGE_PTR) * num_buffer_pages, MAX_ALIGNMENT);
  size += DB_PAGESIZE * num_buffer_pages;

  tempfile_p = (QMGR_TEMP_FILE *) malloc (size);
  if (tempfile_p == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      size);
    }
  memset (tempfile_p, 0x00, size);

  return tempfile_p;
}

/*
 * qmgr_create_new_temp_file () -
 *   return:
 *   query_id(in)       :
 */
QMGR_TEMP_FILE *
qmgr_create_new_temp_file (THREAD_ENTRY * thread_p,
			   QUERY_ID query_id,
			   QMGR_TEMP_FILE_MEMBUF_TYPE membuf_type)
{
  QMGR_QUERY_ENTRY *query_p;
  QMGR_TRAN_ENTRY *tran_entry_p;
  int tran_index, i, num_buffer_pages;
  QMGR_TEMP_FILE *tfile_vfid_p, *temp;
  PAGE_PTR page_p;
  QFILE_PAGE_HEADER pgheader = { 0, NULL_PAGEID, NULL_PAGEID, 0, NULL_PAGEID,
    NULL_VOLID, NULL_VOLID, NULL_VOLID
  };

  assert (QMGR_IS_VALID_MEMBUF_TYPE (membuf_type));
  if (!QMGR_IS_VALID_MEMBUF_TYPE (membuf_type))
    {
      return NULL;
    }

  num_buffer_pages = (membuf_type == TEMP_FILE_MEMBUF_NORMAL) ?
    prm_get_bigint_value (PRM_ID_TEMP_MEM_BUFFER_SIZE) / IO_PAGESIZE :
    prm_get_bigint_value (PRM_ID_INDEX_SCAN_KEY_BUFFER_SIZE) / IO_PAGESIZE;

  tfile_vfid_p =
    qmgr_get_temp_file_from_list (&qmgr_Query_table.temp_file_list
				  [membuf_type]);
  if (tfile_vfid_p == NULL)
    {
      tfile_vfid_p = qmgr_allocate_tempfile_with_buffer (num_buffer_pages);
    }
  else
    {
      assert (tfile_vfid_p->membuf_npages >= 0);
      num_buffer_pages = tfile_vfid_p->membuf_npages;	/* TODO - */
    }

  if (tfile_vfid_p == NULL)
    {
      return NULL;
    }

  tfile_vfid_p->membuf = (PAGE_PTR *) ((PAGE_PTR) tfile_vfid_p +
				       DB_ALIGN (sizeof (QMGR_TEMP_FILE),
						 MAX_ALIGNMENT));

  /* initialize tfile_vfid */
  VFID_SET_NULL (&tfile_vfid_p->temp_vfid);
  tfile_vfid_p->temp_file_type = FILE_TMP;
  tfile_vfid_p->curr_free_page_index = 0;
  tfile_vfid_p->last_free_page_index = -1;
  tfile_vfid_p->vpid_index = -1;
  tfile_vfid_p->vpid_count = 0;
  tfile_vfid_p->membuf_npages = num_buffer_pages;
  tfile_vfid_p->membuf_type = membuf_type;

  for (i = 0; i < QMGR_VPID_ARRAY_SIZE; i++)
    {
      VPID_SET_NULL (&tfile_vfid_p->vpid_array[i]);
    }

  tfile_vfid_p->total_count = 0;
  tfile_vfid_p->membuf_last = -1;
  page_p = (PAGE_PTR) ((PAGE_PTR) tfile_vfid_p->membuf +
		       DB_ALIGN (sizeof (PAGE_PTR) *
				 tfile_vfid_p->membuf_npages, MAX_ALIGNMENT));

  for (i = 0; i < tfile_vfid_p->membuf_npages; i++)
    {
      tfile_vfid_p->membuf[i] = page_p;
      qmgr_put_page_header (page_p, &pgheader);
      page_p += DB_PAGESIZE;
    }

  tran_index = logtb_get_current_tran_index (thread_p);

  assert (qmgr_Query_table.tran_entries_p != NULL);
  assert (qmgr_Query_table.num_trans > tran_index);

  tran_entry_p = &qmgr_Query_table.tran_entries_p[tran_index];

  qmgr_lock_mutex (thread_p, &tran_entry_p->lock);
  /* find query entry */
  if (qmgr_Query_table.tran_entries_p != NULL)
    {
      query_p = qmgr_find_query_entry (tran_entry_p->query_entry_list_p,
				       query_id);
    }
  else
    {
      query_p = NULL;
    }

  if (query_p == NULL)
    {
      free_and_init (tfile_vfid_p);
      qmgr_unlock_mutex (&tran_entry_p->lock);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_UNKNOWN_QUERYID, 1,
	      query_id);
      return NULL;
    }

  /* chain allocated tfile_vfid to the query_entry */
  temp = query_p->qe_temp_vfid;
  query_p->qe_temp_vfid = tfile_vfid_p;
  if (temp != NULL)
    {
      /* link to the list */
      tfile_vfid_p->next = temp;
      tfile_vfid_p->prev = temp->prev;
      tfile_vfid_p->prev->next = tfile_vfid_p;
      temp->prev = tfile_vfid_p;
    }
  else
    {
      /* Add transaction to wfg as a holder of temporary file space, but
         only do so for the first temp file that we create.  From the wfg's
         point of view, there's no difference between holding one file or
         holding one hundred. */
      tfile_vfid_p->next = tfile_vfid_p;
      tfile_vfid_p->prev = tfile_vfid_p;
    }
  /* increase the counter of query entry */
  query_p->qe_num_temp++;

  qmgr_unlock_mutex (&tran_entry_p->lock);

  return tfile_vfid_p;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * qmgr_create_result_file () - create a temporary file for query result
 *   return:
 *   query_id(in)       :
 */
QMGR_TEMP_FILE *
qmgr_create_result_file (THREAD_ENTRY * thread_p, QUERY_ID query_id)
{
  QMGR_QUERY_ENTRY *query_p;
  int tran_index, i;
  QMGR_TEMP_FILE *tfile_vfid_p, *temp;
  QMGR_TRAN_ENTRY *tran_entry_p;

  /* Allocate a tfile_vfid and create a temporary file for query result */

  tfile_vfid_p = (QMGR_TEMP_FILE *) malloc (sizeof (QMGR_TEMP_FILE));
  if (tfile_vfid_p == NULL)
    {
      return NULL;
    }

  VFID_SET_NULL (&tfile_vfid_p->temp_vfid);

  if (file_create_queryarea (thread_p, &tfile_vfid_p->temp_vfid,
			     TEMP_FILE_DEFAULT_PAGES,
			     "Query result file") == NULL)
    {
      free_and_init (tfile_vfid_p);
      return NULL;
    }

  tfile_vfid_p->temp_file_type = FILE_QUERY_AREA;
  /* initialize the allocated tfile_vfid */
  tfile_vfid_p->curr_free_page_index = 0;
  tfile_vfid_p->last_free_page_index =
    file_get_numpages (thread_p, &tfile_vfid_p->temp_vfid, NULL, NULL,
		       NULL) - 1;
  tfile_vfid_p->vpid_index = -1;
  tfile_vfid_p->vpid_count = 0;

  for (i = 0; i < QMGR_VPID_ARRAY_SIZE; i++)
    {
      VPID_SET_NULL (&tfile_vfid_p->vpid_array[i]);
    }

  tfile_vfid_p->total_count = 0;
  tfile_vfid_p->membuf_last =
    (prm_get_integer_value (PRM_ID_TEMP_MEM_BUFFER_PAGES) / IO_PAGESIZE) - 1;
  tfile_vfid_p->membuf = NULL;
  tfile_vfid_p->membuf_npages = 0;
  tfile_vfid_p->membuf_type = TEMP_FILE_MEMBUF_NONE;

  /* Find the query entry and chain the created temp file to the entry */

  tran_index = logtb_get_current_tran_index (thread_p);

  assert (qmgr_Query_table.tran_entries_p != NULL);
  assert (qmgr_Query_table.num_trans > tran_index);

  tran_entry_p = &(qmgr_Query_table.tran_entries_p[tran_index]);

  /* lock the query entry table until the end of this function */
  qmgr_lock_mutex (thread_p, &tran_entry_p->lock);
  /* find the query entry */
  if (qmgr_Query_table.tran_entries_p != NULL)
    {
      query_p = qmgr_find_query_entry (tran_entry_p->query_entry_list_p,
				       query_id);
    }
  else
    {
      query_p = NULL;
    }

  if (query_p == NULL)
    {
      /* query entry is not found */
      qmgr_unlock_mutex (&tran_entry_p->lock);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_UNKNOWN_QUERYID,
	      1, query_id);
      free_and_init (tfile_vfid_p);
      return NULL;
    }

  /* chain the tfile_vfid to the query_entry->qe_temp_vfid */
  temp = query_p->qe_temp_vfid;
  query_p->qe_temp_vfid = tfile_vfid_p;
  if (temp != NULL)
    {
      /* insert into the head of the double linked list */
      tfile_vfid_p->next = temp;
      tfile_vfid_p->prev = temp->prev;
      tfile_vfid_p->prev->next = tfile_vfid_p;
      temp->prev = tfile_vfid_p;
    }
  else
    {
      /* first one */
      tfile_vfid_p->next = tfile_vfid_p;
      tfile_vfid_p->prev = tfile_vfid_p;
    }
  /* increase the counter of query entry */
  query_p->qe_num_temp++;

  qmgr_unlock_mutex (&tran_entry_p->lock);

  return tfile_vfid_p;
}

/*
 * qmgr_free_query_temp_file () -
 *   return: int (NO_ERROR or ER_FAILED)
 *   query_id(in) : Query ID to determine what temp file (if any) to destroy
 *
 * Note: Destroy the external temporary file used, if any.
 */
int
qmgr_free_query_temp_file (THREAD_ENTRY * thread_p, QUERY_ID query_id)
{
  QMGR_QUERY_ENTRY *query_p;
  QMGR_TRAN_ENTRY *tran_entry_p;
  int tran_index, rc;
  QMGR_TEMP_FILE *tfile_vfid_p, *temp;
#if defined (SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */

  tran_index = logtb_get_current_tran_index (thread_p);

  assert (qmgr_Query_table.tran_entries_p != NULL);
  assert (qmgr_Query_table.num_trans > tran_index);

  tran_entry_p = &qmgr_Query_table.tran_entries_p[tran_index];

  qmgr_lock_mutex (thread_p, &tran_entry_p->lock);

  if (qmgr_Query_table.tran_entries_p != NULL)
    {
      query_p =
	qmgr_find_query_entry (tran_entry_p->query_entry_list_p, query_id);
    }
  else
    {
      query_p = NULL;
    }

  if (query_p == NULL)
    {
      /* if this is a streaming query and it is terminating */
      qmgr_unlock_mutex (&tran_entry_p->lock);
      return NO_ERROR;
    }

  rc = NO_ERROR;
  if (query_p->temp_vfid)
    {
      tfile_vfid_p = query_p->temp_vfid;
      tfile_vfid_p->prev->next = NULL;

      while (tfile_vfid_p)
	{
	  if (!(tfile_vfid_p->temp_file_type == FILE_QUERY_AREA
		&& query_p->errid >= 0)
	      && !VFID_ISNULL (&tfile_vfid_p->temp_vfid))
	    {
	      if (file_destroy (thread_p, &tfile_vfid_p->temp_vfid) !=
		  NO_ERROR)
		{
		  /* stop; return error */
		  rc = ER_FAILED;
		}
	    }
	  temp = tfile_vfid_p;
	  tfile_vfid_p = tfile_vfid_p->next;

	  if (temp->temp_file_type != FILE_QUERY_AREA)
	    {
	      rv =
		pthread_mutex_lock (&qmgr_Query_table.temp_file_free_mutex);

	      /* add to the free list */
	      if (qmgr_Query_table.temp_file_free_count <
		  QMGR_TEMP_FILE_FREE_LIST_SIZE)
		{
		  temp->prev = NULL;
		  temp->next = qmgr_Query_table.temp_file_free_list;
		  qmgr_Query_table.temp_file_free_list = temp;
		  qmgr_Query_table.temp_file_free_count++;
		  temp = NULL;
		}

	      pthread_mutex_unlock (&qmgr_Query_table.temp_file_free_mutex);
	    }

	  /* free too many temp_file */
	  if (temp)
	    {
	      free_and_init (temp);
	    }
	}
      query_p->temp_vfid = NULL;
    }

  qmgr_unlock_mutex (&tran_entry_p->lock);
  return rc;
}
#endif

/*
 * qmgr_free_temp_file_list () - free temporary files in tfile_vfid_p
 * return : error code or NO_ERROR
 * thread_p (in) :
 * tfile_vfid_p (in)  : temporary files list
 * query_id (in)      : query id
 * is_error (in)      : true if query was unsuccessful
 */
int
qmgr_free_temp_file_list (THREAD_ENTRY * thread_p,
			  QMGR_TEMP_FILE * tfile_vfid_p,
			  UNUSED_ARG QUERY_ID query_id, bool is_error)
{
  QMGR_TEMP_FILE *temp = NULL;
  int rc = NO_ERROR, fd_ret = NO_ERROR;

  /* make sure temp file list is not cyclic */
  assert (tfile_vfid_p->prev == NULL || tfile_vfid_p->prev->next == NULL);

  while (tfile_vfid_p)
    {
      fd_ret = NO_ERROR;
      if ((tfile_vfid_p->temp_file_type != FILE_QUERY_AREA || is_error)
	  && !VFID_ISNULL (&tfile_vfid_p->temp_vfid))
	{
	  fd_ret = file_destroy (thread_p, &tfile_vfid_p->temp_vfid);
	  if (fd_ret != NO_ERROR)
	    {
	      /* set error but continue with the destroy process */
	      rc = ER_FAILED;
	    }
	}

      temp = tfile_vfid_p;
      tfile_vfid_p = tfile_vfid_p->next;

      if (temp->temp_file_type != FILE_QUERY_AREA)
	{
	  qmgr_put_temp_file_into_list (temp);
	}
      else
	{
	  free_and_init (temp);
	}
    }

  return rc;
}

/*
 * qmgr_free_query_temp_file_by_query_entry () -
 *   return: int (NO_ERROR or ER_FAILED)
 *   query_entryp(in)   : Query entry ptr to determine what temp file (if any)
 *                        to destroy
 *   tran_idx(in)       :
 *
 * Note: Destroy the external temporary file used, if any.
 */
static int
qmgr_free_query_temp_file_by_query_entry (THREAD_ENTRY * thread_p,
					  QMGR_QUERY_ENTRY * query_p,
					  int tran_index)
{
  int rc;
  QMGR_TEMP_FILE *tfile_vfid_p;
  QMGR_TRAN_ENTRY *tran_entry_p;

  if (query_p == NULL || qmgr_Query_table.tran_entries_p == NULL)
    {
      return NO_ERROR;
    }

  if (tran_index == NULL_TRAN_INDEX)
    {
      tran_index = logtb_get_current_tran_index (thread_p);
    }

  assert (qmgr_Query_table.tran_entries_p != NULL);
  assert (qmgr_Query_table.num_trans > tran_index);

  tran_entry_p = &qmgr_Query_table.tran_entries_p[tran_index];

  qmgr_lock_mutex (thread_p, &tran_entry_p->lock);

  rc = NO_ERROR;
  if (query_p->qe_temp_vfid)
    {
      bool is_error = (query_p->errid < 0);
      tfile_vfid_p = query_p->qe_temp_vfid;
      tfile_vfid_p->prev->next = NULL;

      rc = qmgr_free_temp_file_list (thread_p, tfile_vfid_p,
				     query_p->query_id, is_error);

      query_p->qe_temp_vfid = NULL;
    }

  qmgr_unlock_mutex (&tran_entry_p->lock);
  return rc;
}

/*
 * qmgr_free_list_temp_file () -
 *   return: int (NO_ERROR or ER_FAILED)
 *   query_id(in) : Query ID to determine what temp file (if any) to destroy
 *   tfile_vfidp(in): Address of QMGR_TEMP_FILE
 *
 * Note: Destroy the external temporary file used, if any.  The caller
 * is responsible for setting pointers to this tmp_vfid to NULL afterwards.
 */
int
qmgr_free_list_temp_file (THREAD_ENTRY * thread_p, QUERY_ID query_id,
			  QMGR_TEMP_FILE * tfile_vfid_p)
{
  QMGR_QUERY_ENTRY *query_p;
  QMGR_TRAN_ENTRY *tran_entry_p;
  int tran_index, rc;

  tran_index = logtb_get_current_tran_index (thread_p);

  assert (qmgr_Query_table.tran_entries_p != NULL);
  assert (qmgr_Query_table.num_trans > tran_index);

  tran_entry_p = &qmgr_Query_table.tran_entries_p[tran_index];

  qmgr_lock_mutex (thread_p, &tran_entry_p->lock);

  if (qmgr_Query_table.tran_entries_p != NULL)
    {
      query_p = qmgr_find_query_entry (tran_entry_p->query_entry_list_p,
				       query_id);
    }
  else
    {
      query_p = NULL;
    }

  if (query_p == NULL)
    {
      /* if this is a streaming query and it is terminating */
      qmgr_unlock_mutex (&tran_entry_p->lock);
      return NO_ERROR;
    }

  rc = NO_ERROR;
  if (query_p->qe_temp_vfid)
    {
      if (!VFID_ISNULL (&tfile_vfid_p->temp_vfid))
	{
	  if (file_destroy (thread_p, &tfile_vfid_p->temp_vfid) != NO_ERROR)
	    {
	      /* stop; return error */
	      rc = ER_FAILED;
	    }
	  VFID_SET_NULL (&tfile_vfid_p->temp_vfid);
	}

      if (query_p->qe_temp_vfid->next == query_p->qe_temp_vfid)
	{
	  query_p->qe_temp_vfid = NULL;
	}
      else
	{
	  tfile_vfid_p->next->prev = tfile_vfid_p->prev;
	  tfile_vfid_p->prev->next = tfile_vfid_p->next;
	  if (query_p->qe_temp_vfid == tfile_vfid_p)
	    {
	      query_p->qe_temp_vfid = tfile_vfid_p->next;
	    }
	}

      if (tfile_vfid_p->temp_file_type != FILE_QUERY_AREA)
	{
	  qmgr_put_temp_file_into_list (tfile_vfid_p);
	}
      else if (tfile_vfid_p)
	{
	  /* free too many temp_file */
	  free_and_init (tfile_vfid_p);
	}
    }

  qmgr_unlock_mutex (&tran_entry_p->lock);
  return NO_ERROR;
}

/*
 * xqmgr_get_query_info () -
 *   return:
 *   query_id(in)       :
 */
int
xqmgr_get_query_info (THREAD_ENTRY * thread_p, QUERY_ID query_id)
{
  QMGR_QUERY_ENTRY *query_p = NULL;

#if defined (SERVER_MODE)
  /* if query execution has error. */
  if (qmgr_get_query_error_with_id (thread_p, query_id) < 0)
    {
      qmgr_set_query_error (thread_p, query_id);
      return -1;
    }
#endif

  query_p = qmgr_get_query_entry (thread_p, query_id, NULL_TRAN_INDEX);

  if (query_p == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_UNKNOWN_QUERYID,
	      1, query_id);
      return -1;
    }

  /* Errors are handled by sqmgr_get_query_info */
  if (query_p->list_id)
    {
      return (query_p->list_id->tuple_cnt);
    }
  else
    {
      return -1;
    }
}

/*
 * qmgr_get_area_error_async () -
 *   return:
 *   length(in) :
 *   count(in)  :
 *   query_id(in)       :
 */
void *
qmgr_get_area_error_async (THREAD_ENTRY * thread_p, int *length_p,
			   int count, QUERY_ID query_id)
{
  int len;
  char *area, *ptr;
  QMGR_QUERY_ENTRY *query_p;
#if 0
  VPID vpid;
#endif
  int s_len, strlen;
  int errid;
  char *er_msg;
#if 0
  PAGE_PTR last_pgptr = NULL;
#endif
#if defined (SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */

  query_p = qmgr_get_query_entry (thread_p, query_id, NULL_TRAN_INDEX);
  if (query_p == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_UNKNOWN_QUERYID,
	      1, query_id);
      return NULL;
    }


  rv = pthread_mutex_lock (&query_p->lock);
  errid = query_p->errid;
  er_msg = query_p->er_msg;
  pthread_mutex_unlock (&query_p->lock);

  if (er_msg == NULL || errid == NO_ERROR)
    {
      *length_p = len = (OR_INT_SIZE * 5);
      strlen = 0;
    }
  else
    {
      s_len = or_packed_string_length (er_msg, &strlen);
      *length_p = len = (OR_INT_SIZE * 4) + s_len;
    }

  area = (char *) malloc (len);
  if (area == NULL)
    {
      *length_p = 0;
      return NULL;
    }

  ptr = area;

  OR_PUT_INT (ptr, len);
  ptr += OR_INT_SIZE;
  OR_PUT_INT (ptr, 1);		/* done == 1 */
  ptr += OR_INT_SIZE;
  OR_PUT_INT (ptr, count);
  ptr += OR_INT_SIZE;
  /*
   * Now copy the information
   */
  OR_PUT_INT (ptr, (int) (errid));
  ptr += OR_INT_SIZE;
  or_pack_string_with_length (ptr, er_msg, strlen);

  return area;
}

/*
 * qmgr_interrupt_query () -
 *   return:
 *   query_id(in)       :
 */
bool
qmgr_interrupt_query (THREAD_ENTRY * thread_p, QUERY_ID query_id)
{
  QMGR_QUERY_ENTRY *query_p = NULL;
  QMGR_TRAN_ENTRY *tran_entry_p;
  int tran_index;
  int is_interrupted;

  /*
   * get query entry - This is done in-line to avoid qmgr_get_query_entry
   * from returning NULL when the query is being interrupted
   */
  if (qmgr_Query_table.tran_entries_p == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_UNKNOWN_QUERYID,
	      1, query_id);
      return true;
    }

  tran_index = logtb_get_current_tran_index (thread_p);

  assert (qmgr_Query_table.tran_entries_p != NULL);
  assert (qmgr_Query_table.num_trans > tran_index);

  tran_entry_p = &qmgr_Query_table.tran_entries_p[tran_index];

  qmgr_lock_mutex (thread_p, &tran_entry_p->lock);

  query_p = qmgr_find_query_entry (tran_entry_p->query_entry_list_p,
				   query_id);

  if (query_p == NULL)
    {
      qmgr_unlock_mutex (&tran_entry_p->lock);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_UNKNOWN_QUERYID,
	      1, query_id);
      return true;
    }
  else
    {
      is_interrupted = query_p->interrupt;
      qmgr_unlock_mutex (&tran_entry_p->lock);
    }

  return (is_interrupted);
}

#if defined (SERVER_MODE)
/*
 * qmgr_get_query_error_with_id () -
 *   return:
 *   query_id(in)       :
 */
int
qmgr_get_query_error_with_id (THREAD_ENTRY * thread_p, QUERY_ID query_id)
{
  QMGR_QUERY_ENTRY *query_p = NULL;

  query_p = qmgr_get_query_entry (thread_p, query_id, NULL_TRAN_INDEX);

  if (query_p == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_UNKNOWN_QUERYID,
	      (int) query_id);
      return ER_QPROC_UNKNOWN_QUERYID;
    }

  return qmgr_get_query_error_with_entry (query_p);
}
#endif

/*
 * qmgr_get_query_error_with_entry () -
 *   return:
 *   query_entryp(in)   :
 */
int
qmgr_get_query_error_with_entry (QMGR_QUERY_ENTRY * query_p)
{
  int errid;
  char *er_msg;
  char *error_area, *p;
#if defined (SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */

  rv = pthread_mutex_lock (&query_p->lock);
  errid = query_p->errid;
  er_msg = query_p->er_msg;
  pthread_mutex_unlock (&query_p->lock);

  if (errid < 0)
    {
      p = error_area =
	(char *) malloc (3 * OR_INT_SIZE + strlen (er_msg) + 1);

      if (error_area)
	{
	  p = or_pack_int (p, errid);
	  p = or_pack_int (p, ER_ERROR_SEVERITY);
	  p = or_pack_int (p, strlen (er_msg) + 1);
	  strcpy (p, er_msg);

	  er_set_area_error (error_area);
	  free_and_init (error_area);
	}
    }

  return errid;
}

/*
 * qmgr_set_query_error () - set current thread's error to query entry
 *   return:
 *   query_id(in)       :
 */
void
qmgr_set_query_error (THREAD_ENTRY * thread_p, QUERY_ID query_id)
{
  QMGR_QUERY_ENTRY *query_p;
#if defined (SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */

  query_p = qmgr_get_query_entry (thread_p, query_id, NULL_TRAN_INDEX);
  if (query_p != NULL)
    {
      rv = pthread_mutex_lock (&query_p->lock);
      if (query_p->errid != NO_ERROR)
	{
	  /* if an error was already set, don't overwrite it */
	  pthread_mutex_unlock (&query_p->lock);
	  return;
	}

      query_p->errid = er_errid ();
      if (query_p->errid != NO_ERROR)
	{
#if defined (SERVER_MODE)
	  char *ptr = (char *) er_msg ();

	  if (ptr != NULL)
	    {
	      query_p->er_msg = strdup (ptr);
	    }
	  else
	    {
	      query_p->er_msg = NULL;
	    }
#else
	  query_p->er_msg = (char *) er_msg ();
#endif
	}
      else
	{
	  query_p->er_msg = NULL;
	}

      pthread_mutex_unlock (&query_p->lock);
    }
}

#if defined (ENABLE_UNUSED_FUNCTION)
#if defined (SERVER_MODE)
/*
 * qmgr_find_leaf () -
 *   return:
 *   xasl(in)   :
 */
static XASL_NODE *
qmgr_find_leaf (XASL_NODE * xasl_p)
{
  /* Search down the left side until a BUILDLIST_PROC node is found */
  for (xasl_p = xasl_p->proc.union_.left; xasl_p;
       xasl_p = xasl_p->proc.union_.left)
    {
      if (xasl_p->type == BUILDLIST_PROC)
	{
	  break;
	}
    }

  return xasl_p;
}
#endif /* (SERVER_MODE) */
#endif

void
qmgr_setup_empty_list_file (char *page_p)
{
  QFILE_PAGE_HEADER header;

  header.pg_tplcnt = 0;
  header.lasttpl_off = QFILE_PAGE_HEADER_SIZE;
  header.prev_pgid = header.next_pgid = header.ovfl_pgid = NULL_PAGEID;
  header.prev_volid = header.next_volid = header.ovfl_volid = NULL_VOLID;

  qmgr_put_page_header (page_p, &header);
}

/*
 * qmgr_initialize_temp_file_list () -
 *   return: none
 *   temp_file_list_p(in): temporary file list to be initialized
 *   membuf_type(in):
 */
void
qmgr_initialize_temp_file_list (QMGR_TEMP_FILE_LIST * temp_file_list_p,
				QMGR_TEMP_FILE_MEMBUF_TYPE membuf_type)
{
  int i, num_buffer_pages;
  QMGR_TEMP_FILE *temp_file_p;
#if defined(SERVER_MODE)
  int rv;
#endif

  assert (temp_file_list_p != NULL
	  && QMGR_IS_VALID_MEMBUF_TYPE (membuf_type));
  if (temp_file_list_p == NULL || !QMGR_IS_VALID_MEMBUF_TYPE (membuf_type))
    {
      return;
    }

  num_buffer_pages = (membuf_type == TEMP_FILE_MEMBUF_NORMAL) ?
    prm_get_bigint_value (PRM_ID_TEMP_MEM_BUFFER_SIZE) / IO_PAGESIZE :
    prm_get_bigint_value (PRM_ID_INDEX_SCAN_KEY_BUFFER_SIZE) / IO_PAGESIZE;

  pthread_mutex_init (&temp_file_list_p->mutex, NULL);
  rv = pthread_mutex_lock (&temp_file_list_p->mutex);
  temp_file_list_p->list = NULL;

  for (i = 0; i < QMGR_TEMP_FILE_FREE_LIST_SIZE; i++)
    {
      temp_file_p = qmgr_allocate_tempfile_with_buffer (num_buffer_pages);
      if (temp_file_p == NULL)
	{
	  break;
	}
      /* add to the free list */
      temp_file_p->prev = NULL;
      temp_file_p->next = temp_file_list_p->list;
      temp_file_p->membuf_npages = num_buffer_pages;
      temp_file_p->membuf_type = membuf_type;
      temp_file_list_p->list = temp_file_p;
    }

  temp_file_list_p->count = i;

  pthread_mutex_unlock (&temp_file_list_p->mutex);
}

/*
 * qmgr_finalize_temp_file_list () -
 *   return: none
 *   temp_file_list_p(in): temporary file list to be finalized
 */
void
qmgr_finalize_temp_file_list (QMGR_TEMP_FILE_LIST * temp_file_list_p)
{
  QMGR_TEMP_FILE *temp_file_p;

  assert (temp_file_list_p != NULL);
  if (temp_file_list_p == NULL)
    {
      return;
    }

  while (temp_file_list_p->list)
    {
      temp_file_p = temp_file_list_p->list;
      temp_file_list_p->list = temp_file_p->next;
      free_and_init (temp_file_p);
    }
  temp_file_list_p->count = 0;
  pthread_mutex_destroy (&temp_file_list_p->mutex);
}

/*
 * qmgr_get_temp_file_from_list () -
 *   return: temporary file
 *   temp_file_list_p(in): temporary file list
 */
QMGR_TEMP_FILE *
qmgr_get_temp_file_from_list (QMGR_TEMP_FILE_LIST * temp_file_list_p)
{
  QMGR_TEMP_FILE *temp_file_p = NULL;
#if defined(SERVER_MODE)
  int rv;
#endif
  assert (temp_file_list_p != NULL);
  if (temp_file_list_p == NULL)
    {
      return NULL;
    }

  rv = pthread_mutex_lock (&temp_file_list_p->mutex);

  /* delete from the free list */
  if (temp_file_list_p->list)
    {
      temp_file_p = temp_file_list_p->list;
      temp_file_list_p->list = temp_file_p->next;
      temp_file_p->prev = temp_file_p->next = NULL;
      temp_file_list_p->count--;
    }

  pthread_mutex_unlock (&temp_file_list_p->mutex);

  return temp_file_p;
}

/*
 * qmgr_put_temp_file_into_list () -
 *   return: none
 *   temp_file_list_p(in): temporary file list
 */
void
qmgr_put_temp_file_into_list (QMGR_TEMP_FILE * temp_file_p)
{
  QMGR_TEMP_FILE_LIST *temp_file_list_p;
#if defined(SERVER_MODE)
  int rv;
#endif
  assert (temp_file_p != NULL);
  if (temp_file_p == NULL)
    {
      return;
    }

  temp_file_p->membuf_last = -1;

  if (QMGR_IS_VALID_MEMBUF_TYPE (temp_file_p->membuf_type))
    {
      temp_file_list_p =
	&qmgr_Query_table.temp_file_list[temp_file_p->membuf_type];

      rv = pthread_mutex_lock (&temp_file_list_p->mutex);

      /* add to the free list */
      if (temp_file_list_p->count < QMGR_TEMP_FILE_FREE_LIST_SIZE)
	{
	  temp_file_p->prev = NULL;
	  temp_file_p->next = temp_file_list_p->list;
	  temp_file_list_p->list = temp_file_p;
	  temp_file_list_p->count++;
	  temp_file_p = NULL;
	}

      pthread_mutex_unlock (&temp_file_list_p->mutex);
    }
  if (temp_file_p)
    {
      free_and_init (temp_file_p);
    }
}

/*
 * qmgr_get_temp_file_membuf_pages () -
 *   return: number of membuf pages belonging to the temporary file
 *   temp_file_list_p(in): temporary file
 */
int
qmgr_get_temp_file_membuf_pages (QMGR_TEMP_FILE * temp_file_p)
{
  assert (temp_file_p != NULL);
  if (temp_file_p == NULL)
    {
      return -1;
    }
  return temp_file_p->membuf_npages;
}

#if defined (SERVER_MODE)
/*
 * qmgr_set_query_exec_info_to_tdes () - calculate timeout and set to transaction
 *                                     descriptor
 *   return: void
 *   tran_index(in):
 *   query_timeout(in): milli seconds
 */
static void
qmgr_set_query_exec_info_to_tdes (int tran_index, int query_timeout,
				  const XASL_ID * xasl_id)
{
  LOG_TDES *tdes_p;

  tdes_p = LOG_FIND_TDES (tran_index);
  assert (tdes_p != NULL);
  if (tdes_p != NULL)
    {
      /* We use log_Clock_msec instead of calling gettimeofday
       * if the system supports atomic built-ins.
       */
      tdes_p->query_start_time = thread_get_log_clock_msec ();

      if (query_timeout > 0)
	{
	  tdes_p->query_timeout = tdes_p->query_start_time + query_timeout;
	}
      else if (query_timeout == 0)
	{
	  tdes_p->query_timeout = 0;
	}
      else if (query_timeout != -1)
	{
	  /* already expired */
	  tdes_p->query_timeout = tdes_p->query_start_time;
	}
      else
	{
	  /*
	   * query_timeout == -1
	   * This means that the query is not the first of a bundle of queries.
	   * We will apply a timeout to the bundle, not each query.
	   * Actually CAS always sends -1 in this case.
	   */
	}
      if (tdes_p->tran_start_time == 0)
	{
	  /* set transaction start time, if this is the first query */
	  tdes_p->tran_start_time = tdes_p->query_start_time;
	}
      if (xasl_id != NULL)
	{
	  XASL_ID_COPY (&tdes_p->xasl_id, xasl_id);
	}
    }
}

/*
 * qmgr_reset_query_exec_info () - reset query_start_time and xasl_id of tdes
 *   return: void
 *   tran_index(in):
 */
static void
qmgr_reset_query_exec_info (int tran_index)
{
  LOG_TDES *tdes_p;

  tdes_p = LOG_FIND_TDES (tran_index);
  assert (tdes_p != NULL);
  if (tdes_p != NULL)
    {
      tdes_p->query_start_time = 0;
      XASL_ID_SET_NULL (&tdes_p->xasl_id);
      tdes_p->query_timeout = 0;
    }
}
#endif

/*
 * qmgr_get_sql_id ()
 *   return: error_code
 *   sql_id_buf(out):
 *   buf_size(in):
 *   query(in):
 *   sql_len(in):
 *
 *   note : caller must free sql_id_buf
 *
 *   Rye SQL_ID is generated from md5 hash_value.
 *   The last 13 hexa-digit string of md5-hash(32 hexa-digit) string.
 *   Oracle's SQL_ID is also generated from md5 hash-value.
 *   But they uses the last 8 hexa-digit to generate 13-digit string.
 *   So the SQL_ID of a query is different in Rye and ORACLE,
 *   even though the length is same.
 */
int
qmgr_get_sql_id (UNUSED_ARG THREAD_ENTRY * thread_p, char **sql_id_buf,
		 char *query, int sql_len)
{
  char hashstring[32 + 1] = { '\0' };
  char *ret_buf;

  if (sql_id_buf == NULL)
    {
      assert_release (0);
      return ER_FAILED;
    }

  ret_buf = (char *) malloc (sizeof (char) * (QMGR_SQL_ID_LENGTH + 1));
  if (ret_buf == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, (QMGR_SQL_ID_LENGTH + 1));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  md5_buffer (query, sql_len, hashstring);	/* 16 bytes hash value */

  md5_hash_to_hex (hashstring, hashstring);

  /* copy last 13 hexa-digit to ret_buf */
  strncpy (ret_buf, hashstring + 19, QMGR_SQL_ID_LENGTH);
  ret_buf[QMGR_SQL_ID_LENGTH] = '\0';

  *sql_id_buf = ret_buf;

  return NO_ERROR;
}

/* qmgr_get_rand_buf() : return the drand48_data reference
 * thread_p(in):
 */
struct drand48_data *
qmgr_get_rand_buf (UNUSED_ARG THREAD_ENTRY * thread_p)
{
#if defined(SERVER_MODE)
  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }
  return &thread_p->rand_buf;
#else
  return &qmgr_rand_buf;
#endif
}
