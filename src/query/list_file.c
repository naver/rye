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
 * list_file.c - Query List File Manager
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <string.h>
#include <math.h>
#include <search.h>
#include <stddef.h>

#include "porting.h"
#include "error_manager.h"
#include "db.h"
#include "memory_alloc.h"
#include "page_buffer.h"
#include "lock_manager.h"
#include "external_sort.h"
#include "file_io.h"
#include "file_manager.h"
#include "disk_manager.h"
#include "slotted_page.h"
#include "critical_section.h"
#include "boot_sr.h"
#include "transaction_sr.h"
#include "wait_for_graph.h"
#include "environment_variable.h"
#include "xserver_interface.h"
#if defined(SERVER_MODE)
#include "thread.h"
#include "connection_error.h"
#endif /* SERVER_MODE */
#include "query_manager.h"
#include "object_primitive.h"
#include "system_parameter.h"
#include "memory_hash.h"
#include "object_print.h"

/* this must be the last header file included!!! */
#include "dbval.h"

/* TODO */
#if !defined(SERVER_MODE)
#define pthread_mutex_init(a, b)
#define pthread_mutex_destroy(a)
#define pthread_mutex_lock(a)	0
#define pthread_mutex_unlock(a)
static int rv;

#define thread_sleep(a)
#endif /* not SERVER_MODE */

#define QFILE_CHECK_LIST_FILE_IS_CLOSED(list_id)

#define QFILE_DEFAULT_PAGES 4
#define QFILE_PAGE_EXTENDS  4

#ifdef SERVER_MODE
#define LS_PUT_NEXT_VPID_NULL_ASYNC(ptr) { \
   OR_PUT_INT((ptr) + QFILE_NEXT_PAGE_ID_OFFSET, NULL_PAGEID_ASYNC); \
   OR_PUT_SHORT((ptr) + QFILE_NEXT_VOL_ID_OFFSET, NULL_VOLID); }
#else
#define LS_PUT_NEXT_VPID_NULL_ASYNC(ptr) { \
   OR_PUT_INT((ptr) + QFILE_NEXT_PAGE_ID_OFFSET, NULL_PAGEID); \
   OR_PUT_SHORT((ptr) + QFILE_NEXT_VOL_ID_OFFSET, NULL_VOLID); }
#endif

typedef SCAN_CODE (*ADVANCE_FUCTION) (THREAD_ENTRY * thread_p,
				      QFILE_LIST_SCAN_ID *,
				      QFILE_TUPLE_RECORD *,
				      QFILE_LIST_SCAN_ID *,
				      QFILE_TUPLE_RECORD *,
				      QFILE_TUPLE_VALUE_TYPE_LIST *);

#if defined(SERVER_MODE)
static pthread_mutex_t qfile_Free_sort_list_mutex = PTHREAD_MUTEX_INITIALIZER;
#endif /* SERVER_MODE */
static SORT_LIST *qfile_Free_sort_list = NULL;
static int qfile_Free_sort_list_total = 0;
static int qfile_Free_sort_list_count = 0;

/*
 * Query File Manager Constants/Global Variables
 */
static int qfile_Xasl_page_size;
static int qfile_Max_tuple_page_size;

static int qfile_get_sort_list_size (SORT_LIST * sort_list);
static int qfile_compare_tuple_values (QFILE_TUPLE tplp1, QFILE_TUPLE tplp2,
				       TP_DOMAIN * domain, int *cmp);
static int qfile_unify_types (QFILE_LIST_ID * list_id1,
			      const QFILE_LIST_ID * list_id2);
#if defined (RYE_DEBUG)
static void qfile_print_tuple (QFILE_TUPLE_VALUE_TYPE_LIST * type_list,
			       QFILE_TUPLE tpl);
#endif
static void qfile_initialize_page_header (PAGE_PTR page_p);
static bool qfile_is_first_tuple (QFILE_LIST_ID * list_id_p);
static bool qfile_is_last_page_full (QFILE_LIST_ID * list_id_p,
				     int tuple_length, bool is_ovf_page);
static void qfile_set_dirty_page (THREAD_ENTRY * thread_p, PAGE_PTR page_p,
				  int free_page, QMGR_TEMP_FILE * vfid_p);
static PAGE_PTR qfile_allocate_new_page (THREAD_ENTRY * thread_p,
					 QFILE_LIST_ID * list_id_p,
					 PAGE_PTR page_p, bool is_ovf_page);
static PAGE_PTR qfile_allocate_new_ovf_page (THREAD_ENTRY * thread_p,
					     QFILE_LIST_ID * list_id_p,
					     PAGE_PTR page_p,
					     PAGE_PTR prev_page_p,
					     int tuple_length, int offset,
					     int *tuple_page_size_p);
static int qfile_allocate_new_page_if_need (THREAD_ENTRY * thread_p,
					    QFILE_LIST_ID * list_id_p,
					    PAGE_PTR * page_p,
					    int tuple_length,
					    bool is_ovf_page);
static void qfile_add_tuple_to_list_id (QFILE_LIST_ID * list_id_p,
					PAGE_PTR page_p, int tuple_length,
					int offset);
static int qfile_save_single_bound_item_tuple (QFILE_TUPLE_DESCRIPTOR *
					       tuple_descr_p, char *tuple_p,
					       char *page_p,
					       int tuple_length);
static int qfile_save_normal_tuple (QFILE_TUPLE_DESCRIPTOR * tuple_descr_p,
				    char *tuple_p, char *page_p,
				    int tuple_length);
static int qfile_save_sort_key_tuple (QFILE_TUPLE_DESCRIPTOR * tuple_descr_p,
				      char *tuple_p, char *page_p,
				      int tuple_length);

#if defined (ENABLE_UNUSED_FUNCTION)
#if defined(SERVER_MODE)
static int qfile_get_list_page_with_waiting (THREAD_ENTRY * thread_p,
					     QMGR_WAIT_ARGS * arg);
#endif /* SERVER_MODE */
#endif

static int qfile_compare_tuple_helper (QFILE_TUPLE lhs, QFILE_TUPLE rhs,
				       QFILE_TUPLE_VALUE_TYPE_LIST * types,
				       int *cmp);
static SCAN_CODE qfile_advance_single (THREAD_ENTRY * thread_p,
				       QFILE_LIST_SCAN_ID * next_scan,
				       QFILE_TUPLE_RECORD * next_tpl,
				       QFILE_LIST_SCAN_ID * last_scan,
				       QFILE_TUPLE_RECORD * last_tpl,
				       QFILE_TUPLE_VALUE_TYPE_LIST * types);
static SCAN_CODE qfile_advance_group (THREAD_ENTRY * thread_p,
				      QFILE_LIST_SCAN_ID * next_scan,
				      QFILE_TUPLE_RECORD * next_tpl,
				      QFILE_LIST_SCAN_ID * last_scan,
				      QFILE_TUPLE_RECORD * last_tpl,
				      QFILE_TUPLE_VALUE_TYPE_LIST * types);
static int qfile_add_one_tuple (THREAD_ENTRY * thread_p, QFILE_LIST_ID * dst,
				QFILE_TUPLE lhs, QFILE_TUPLE rhs);
#if defined (ENABLE_UNUSED_FUNCTION)
static int qfile_add_two_tuple (THREAD_ENTRY * thread_p, QFILE_LIST_ID * dst,
				QFILE_TUPLE lhs, QFILE_TUPLE rhs);
#endif
static int qfile_advance (THREAD_ENTRY * thread_p,
			  ADVANCE_FUCTION advance_func,
			  QFILE_TUPLE_RECORD * side_p,
			  QFILE_TUPLE_RECORD * last_side_p,
			  QFILE_LIST_SCAN_ID * scan_p,
			  QFILE_LIST_SCAN_ID * last_scan_p,
			  QFILE_LIST_ID * side_file_p, int *have_side_p);
static int qfile_copy_tuple (THREAD_ENTRY * thread_p,
			     QFILE_LIST_ID * to_list_id_p,
			     QFILE_LIST_ID * from_list_id_p);
static void qfile_close_and_free_list_file (THREAD_ENTRY * thread_p,
					    QFILE_LIST_ID * list_id);

static QFILE_LIST_ID *qfile_union_list (THREAD_ENTRY * thread_p,
					QFILE_LIST_ID * list_id1,
					QFILE_LIST_ID * list_id2, int flag);

static SORT_STATUS qfile_get_next_sort_item (THREAD_ENTRY * thread_p,
					     RECDES * recdes, void *arg);
static int qfile_put_next_sort_item (THREAD_ENTRY * thread_p,
				     const RECDES * recdes, void *arg);
static SORT_INFO *qfile_initialize_sort_info (SORT_INFO * info,
					      QFILE_LIST_ID * listid,
					      SORT_LIST * sort_list);
static void qfile_clear_sort_info (SORT_INFO * info);
#if defined (ENABLE_UNUSED_FUNCTION)
static int qfile_copy_list_pages (THREAD_ENTRY * thread_p,
				  VPID * old_first_vpidp,
				  QMGR_TEMP_FILE * old_tfile_vfidp,
				  VPID * new_first_vpidp,
				  VPID * new_last_vpidp,
				  QMGR_TEMP_FILE * new_tfile_vfidp);
#endif
static int qfile_get_tuple_from_current_list (THREAD_ENTRY * thread_p,
					      QFILE_LIST_SCAN_ID * scan_id_p,
					      QFILE_TUPLE_RECORD *
					      tuple_record_p);

static SCAN_CODE qfile_scan_next (THREAD_ENTRY * thread_p,
				  QFILE_LIST_SCAN_ID * s_id);
static SCAN_CODE qfile_scan_prev (THREAD_ENTRY * thread_p,
				  QFILE_LIST_SCAN_ID * s_id);
static SCAN_CODE qfile_retrieve_tuple (THREAD_ENTRY * thread_p,
				       QFILE_LIST_SCAN_ID * scan_id_p,
				       QFILE_TUPLE_RECORD * tuple_record_p,
				       int peek);
static SCAN_CODE qfile_scan_list (THREAD_ENTRY * thread_p,
				  QFILE_LIST_SCAN_ID * scan_id_p,
				  SCAN_CODE (*scan_func) (THREAD_ENTRY *
							  thread_p,
							  QFILE_LIST_SCAN_ID
							  *),
				  QFILE_TUPLE_RECORD * tuple_record_p,
				  int peek);
static int qfile_compare_with_null_value (int o0, int o1,
					  SUBKEY_INFO key_info);

#if defined (ENABLE_UNUSED_FUNCTION)
/* qfile_modify_type_list () -
 *   return:
 *   type_list(in):
 *   list_id(out):
 */
int
qfile_modify_type_list (QFILE_TUPLE_VALUE_TYPE_LIST * type_list_p,
			QFILE_LIST_ID * list_id_p)
{
  size_t type_list_size;

  list_id_p->type_list.type_cnt = type_list_p->type_cnt;

  list_id_p->type_list.domp = NULL;
  if (list_id_p->type_list.type_cnt != 0)
    {
      type_list_size =
	list_id_p->type_list.type_cnt * DB_SIZEOF (TP_DOMAIN *);
      list_id_p->type_list.domp = (TP_DOMAIN **) malloc (type_list_size);
      if (list_id_p->type_list.domp == NULL)
	{
	  return ER_FAILED;
	}

      memcpy (list_id_p->type_list.domp, type_list_p->domp, type_list_size);
    }

  list_id_p->tpl_descr.f_valp = NULL;
  return NO_ERROR;
}
#endif

/*
 * qfile_copy_list_id - Copy contents of source list_id into destination list_id
 *  return: NO_ERROR or ER_FAILED
 *  dest_list_id(out): destination list_id
 *  src_list_id(in): source list_id
 *  include_sort_list(in):
 */
int
qfile_copy_list_id (QFILE_LIST_ID * dest_list_id_p,
		    const QFILE_LIST_ID * src_list_id_p,
		    bool is_include_sort_list)
{
  size_t type_list_size;

  memcpy (dest_list_id_p, src_list_id_p, DB_SIZEOF (QFILE_LIST_ID));

  /* copy domain info of type list */
  dest_list_id_p->type_list.domp = NULL;
  if (dest_list_id_p->type_list.type_cnt > 0)
    {
      type_list_size =
	dest_list_id_p->type_list.type_cnt * sizeof (TP_DOMAIN *);
      dest_list_id_p->type_list.domp = (TP_DOMAIN **) malloc (type_list_size);

      if (dest_list_id_p->type_list.domp == NULL)
	{
	  return ER_FAILED;
	}

      memcpy (dest_list_id_p->type_list.domp, src_list_id_p->type_list.domp,
	      type_list_size);
    }

  /* copy sort list */
  dest_list_id_p->sort_list = NULL;
  if (is_include_sort_list && src_list_id_p->sort_list)
    {
      SORT_LIST *src, *dest = NULL;
      int len;

      len = qfile_get_sort_list_size (src_list_id_p->sort_list);
      if (len > 0)
	{
	  dest = qfile_allocate_sort_list (len);
	  if (dest == NULL)
	    {
	      free_and_init (dest_list_id_p->type_list.domp);
	      return ER_FAILED;
	    }
	}

      /* copy sort list item */
      for (src = src_list_id_p->sort_list, dest_list_id_p->sort_list = dest;
	   src != NULL && dest != NULL; src = src->next, dest = dest->next)
	{
	  dest->s_order = src->s_order;
	  dest->s_nulls = src->s_nulls;
	  dest->pos_descr.pos_no = src->pos_descr.pos_no;
	}
    }
  else
    {
      dest_list_id_p->sort_list = NULL;
    }

  memset (&dest_list_id_p->tpl_descr, 0, sizeof (QFILE_TUPLE_DESCRIPTOR));
  return NO_ERROR;
}

/*
 * qfile_clone_list_id () - Clone (allocate and copy) the list_id
 *   return: cloned list id
 *   list_id(in): source list id
 *   incluse_sort_list(in):
 */
QFILE_LIST_ID *
qfile_clone_list_id (const QFILE_LIST_ID * list_id_p,
		     bool is_include_sort_list)
{
  QFILE_LIST_ID *cloned_id_p;

  /* allocate new LIST_ID to be returned */
  cloned_id_p = (QFILE_LIST_ID *) malloc (DB_SIZEOF (QFILE_LIST_ID));
  if (cloned_id_p)
    {
      if (qfile_copy_list_id (cloned_id_p, list_id_p, is_include_sort_list) !=
	  NO_ERROR)
	{
	  free_and_init (cloned_id_p);
	}
    }

  return cloned_id_p;
}

/*
 * qfile_clear_list_id () -
 *   list_id(in/out): List identifier
 *
 * Note: The allocated areas inside the area pointed by the list_id is
 *       freed and the area pointed by list_id is set to null values.
 */
void
qfile_clear_list_id (QFILE_LIST_ID * list_id_p)
{
  if (list_id_p->tpl_descr.f_valp)
    {
      free_and_init (list_id_p->tpl_descr.f_valp);
    }

  if (list_id_p->sort_list)
    {
      qfile_free_sort_list (list_id_p->sort_list);
      list_id_p->sort_list = NULL;
    }

  if (list_id_p->type_list.domp != NULL)
    {
      free_and_init (list_id_p->type_list.domp);
    }

  QFILE_CLEAR_LIST_ID (list_id_p);
}

/*
 * qfile_free_list_id () -
 *   return:
 *   list_id(in/out): List identifier
 *
 * Note: The allocated areas inside the area pointed by the list_id and
 *       the area itself pointed by the list_id are freed.
 */
void
qfile_free_list_id (QFILE_LIST_ID * list_id_p)
{
  /* This function is remained for debugging purpose.
   * Do not call this function directly.
   * Use QFILE_FREE_AND_INIT_LIST_ID macro.
   */
  qfile_clear_list_id (list_id_p);
  free (list_id_p);
}


/*
 * qfile_free_sort_list () -
 *   return:
 *   sort_list(in): Sort item list pointer
 *
 * Note: The area allocated for sort_list is freed.
 */
void
qfile_free_sort_list (SORT_LIST * sort_list_p)
{
  SORT_LIST *p;
  int count;
#if defined(SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */

  if (sort_list_p == NULL)
    {
      return;
    }

  for (p = sort_list_p, count = 1; p->next != NULL; p = p->next)
    {
      count++;
    }

  rv = pthread_mutex_lock (&qfile_Free_sort_list_mutex);

  /* TODO: introduce other param rather than MAX_THREADS */
  if (qfile_Free_sort_list_count < thread_num_worker_threads ())
    {
      p->next = qfile_Free_sort_list;
      qfile_Free_sort_list = sort_list_p;
      qfile_Free_sort_list_count += count;

      pthread_mutex_unlock (&qfile_Free_sort_list_mutex);
    }
  else
    {
      qfile_Free_sort_list_total -= count;
      pthread_mutex_unlock (&qfile_Free_sort_list_mutex);

      while (sort_list_p != NULL)
	{
	  p = sort_list_p;
	  sort_list_p = p->next;
	  free_and_init (p);
	}
    }
}

/*
 * qfile_allocate_sort_list () -
 *   return: sort item list, or NULL
 *   cnt(in): Number of nodes in the list
 *
 * Note: A linked list of cnt sort structure nodes is allocated and returned.
 *
 * Note: Only qfile_free_sort_list function should be used to free the area
 *       since the linked list is allocated in a contigous region.
 */
SORT_LIST *
qfile_allocate_sort_list (int count)
{
  SORT_LIST *s, *p;
  int i;
  int num_remains;
#if defined(SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */

  if (count <= 0)
    {
      return NULL;
    }

  /* allocate complete list */
  if (qfile_Free_sort_list != NULL)
    {
      rv = pthread_mutex_lock (&qfile_Free_sort_list_mutex);

      if (qfile_Free_sort_list != NULL)
	{
	  if (count <= qfile_Free_sort_list_count)
	    {
	      s = p = qfile_Free_sort_list;
	      for (i = 1; i < count; i++)
		{
		  p = p->next;
		}

	      qfile_Free_sort_list = p->next;
	      qfile_Free_sort_list_count -= count;
	      pthread_mutex_unlock (&qfile_Free_sort_list_mutex);
	      p->next = NULL;
	    }
	  else
	    {
	      num_remains = count - qfile_Free_sort_list_count;
	      s = qfile_Free_sort_list;
	      qfile_Free_sort_list = NULL;
	      qfile_Free_sort_list_count = 0;
	      pthread_mutex_unlock (&qfile_Free_sort_list_mutex);

	      for (i = 0; i < num_remains; i++)
		{
		  p = (SORT_LIST *) malloc (sizeof (SORT_LIST));
		  if (p == NULL)
		    {
		      qfile_free_sort_list (s);
		      return NULL;
		    }
		  p->next = s;
		  s = p;
		}
	      if (i > 0)
		{
		  rv = pthread_mutex_lock (&qfile_Free_sort_list_mutex);
		  qfile_Free_sort_list_total += i;
		  pthread_mutex_unlock (&qfile_Free_sort_list_mutex);
		}
	    }
	  return s;
	}

      pthread_mutex_unlock (&qfile_Free_sort_list_mutex);
    }

  s = NULL;
  for (i = 0; i < count; i++)
    {
      p = (SORT_LIST *) malloc (sizeof (SORT_LIST));
      if (p == NULL)
	{
	  qfile_free_sort_list (s);
	  return NULL;
	}

      p->next = s;
      s = p;
    }

  if (i > 0)
    {
      rv = pthread_mutex_lock (&qfile_Free_sort_list_mutex);
      qfile_Free_sort_list_total += count;
      pthread_mutex_unlock (&qfile_Free_sort_list_mutex);
    }

  return s;
}

/*
 * qfile_get_sort_list_size () -
 *   return: the number of sort_list item
 *   sort_list(in): sort item list pointer
 *
 */
static int
qfile_get_sort_list_size (SORT_LIST * sort_list_p)
{
  SORT_LIST *s;
  int len = 0;

  for (s = sort_list_p; s; s = s->next)
    {
      ++len;
    }

  return len;
}

/*
 * qfile_is_sort_list_covered () -
 *   return: true or false
 *   covering_list(in): covering sort item list pointer
 *   covered_list(in): covered sort item list pointer
 *
 * Note: if covering_list covers covered_list returns true.
 *       otherwise, returns false.
 */
bool
qfile_is_sort_list_covered (SORT_LIST * covering_list_p,
			    SORT_LIST * covered_list_p)
{
  SORT_LIST *s1, *s2;

  if (covered_list_p == NULL)
    {
      return false;
    }

  for (s1 = covering_list_p, s2 = covered_list_p;
       s1 && s2; s1 = s1->next, s2 = s2->next)
    {
      if (s1->s_order != s2->s_order
	  || s1->s_nulls != s2->s_nulls
	  || s1->pos_descr.pos_no != s2->pos_descr.pos_no)
	{
	  return false;
	}
    }

  if (s1 == NULL && s2)
    {
      return false;
    }
  else
    {
      return true;
    }
}

/*
 * qfile_compare_tuple_values () -
 *   return: NO_ERROR or error code
 *   tpl1(in): First tuple value
 *   tpl2(in): Second tuple value
 *   domain(in): both tuple values must be of the same domain
 *
 * Note: This routine checks if two tuple values are equal.
 *       Coercion is not done.
 *       If both values are UNBOUND, the values are treated as equal.
 */
static int
qfile_compare_tuple_values (QFILE_TUPLE tuple1, QFILE_TUPLE tuple2,
			    TP_DOMAIN * domain_p, int *compare_result)
{
  OR_BUF buf;
  DB_VALUE dbval1, dbval2;
  int length1, length2;
  PR_TYPE *pr_type_p;
  bool is_copy;
  DB_TYPE type = TP_DOMAIN_TYPE (domain_p);
  int rc;

  pr_type_p = domain_p->type;
  is_copy = false;
  is_copy = pr_is_set_type (type) ? true : false;
  length1 = QFILE_GET_TUPLE_VALUE_LENGTH (tuple1);

  /* zero length means NULL */
  if (length1 == 0)
    {
      db_make_null (&dbval1);
    }
  else
    {
      or_init (&buf, (char *) tuple1 + QFILE_TUPLE_VALUE_HEADER_SIZE,
	       length1);
      rc =
	(*(pr_type_p->data_readval)) (&buf, &dbval1, domain_p, -1, is_copy);
      if (rc != NO_ERROR)
	{
	  return ER_FAILED;
	}
    }

  length2 = QFILE_GET_TUPLE_VALUE_LENGTH (tuple2);

  /* zero length means NULL */
  if (length2 == 0)
    {
      db_make_null (&dbval2);
    }
  else
    {
      if (length1 == 0)
	{
	  /* dbval1 is NULL value */
	  assert (DB_IS_NULL (&dbval1));
	}
      else
	{
	  assert (type != DB_TYPE_VARIABLE);

	  or_init (&buf, (char *) tuple2 + QFILE_TUPLE_VALUE_HEADER_SIZE,
		   length2);
	  rc =
	    (*(pr_type_p->data_readval)) (&buf, &dbval2, domain_p, -1,
					  is_copy);
	  if (rc != NO_ERROR)
	    {
	      return ER_FAILED;
	    }
	}
    }

  if (length1 == 0 && length2 == 0)
    {
      *compare_result = DB_EQ;	/* NULL values compare equal in this routine */
    }
  else if (length1 == 0)
    {
      *compare_result = DB_LT;
    }
  else if (length2 == 0)
    {
      *compare_result = DB_GT;
    }
  else
    {
      *compare_result = (*(pr_type_p->cmpval)) (&dbval1, &dbval2, 0, 1,
						domain_p->collation_id);
    }

  if (is_copy)
    {
      pr_clear_value (&dbval1);
      pr_clear_value (&dbval2);
    }

  return NO_ERROR;
}

/*
 * qfile_unify_types () -
 *   return:
 *   list_id1(in/out): Destination list identifier
 *   list_id2(in): Source list identifier
 *
 * Note: For every destination type which is DB_TYPE_NULL,
 *       set it to the source type.
 *       This should probably set an error for non-null mismatches.
 */
static int
qfile_unify_types (QFILE_LIST_ID * list_id1_p,
		   const QFILE_LIST_ID * list_id2_p)
{
  int i;
  int max_count = list_id1_p->type_list.type_cnt;
  TP_DOMAIN *dom1, *dom2;
  DB_TYPE type1, type2;

  if (max_count != list_id2_p->type_list.type_cnt)
    {
      /* error, but is ignored for now. */
      if (max_count > list_id2_p->type_list.type_cnt)
	{
	  max_count = list_id2_p->type_list.type_cnt;
	}
    }

  for (i = 0; i < max_count; i++)
    {
      dom1 = list_id1_p->type_list.domp[i];
      dom2 = list_id2_p->type_list.domp[i];
      assert (dom1 != NULL);
      assert (dom2 != NULL);

      type1 = TP_DOMAIN_TYPE (dom1);
      type2 = TP_DOMAIN_TYPE (dom2);

      if (type1 == DB_TYPE_NULL || type1 == DB_TYPE_VARIABLE)
	{
	  list_id1_p->type_list.domp[i] = list_id2_p->type_list.domp[i];
	  continue;
	}
      else if (type2 == DB_TYPE_NULL || type2 == DB_TYPE_VARIABLE)
	{
	  continue;
	}

      /* do type checking
       */
      if (dom1 == dom2)
	{
	  /* OK for the same domain */
	}
      else if (type1 == type2 && type1 == DB_TYPE_VARCHAR)
	{
	  /* OK for variable string types with different precision */
	}
      else if (type1 == type2 && type1 == DB_TYPE_NUMERIC
#if 0				/* TODO - refer qdata_coerce_result_to_domain() */
	       && dom1->precision == dom2->precision
#endif
	       && dom1->scale == dom2->scale)
	{
	  /* OK for numeric types with same scale */
	}
      else
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_INCOMPATIBLE_TYPES, 0);
	  return ER_QPROC_INCOMPATIBLE_TYPES;
	}
    }

  return NO_ERROR;
}

/*
 * qfile_locate_tuple_value () -
 *   return: V_BOUND/V_UNBOUND
 *   tpl(in): tuple
 *   index(in): value position number
 *   tpl_val(out): set to point to the specified value
 *   val_size(out): set to the value size
 *
 * Note: Sets the tpl_val pointer to the specified value.
 *
 * Note: The index validity check must be done by the caller.
 */
QFILE_TUPLE_VALUE_FLAG
qfile_locate_tuple_value (QFILE_TUPLE tuple, int index, char **tuple_value_p,
			  int *value_size_p)
{
  tuple += QFILE_TUPLE_LENGTH_SIZE;
  return qfile_locate_tuple_value_r (tuple, index, tuple_value_p,
				     value_size_p);
}

/*
 * qfile_locate_tuple_value_r () -
 *   return: V_BOUND/V_UNBOUND
 *   tpl(in): tuple
 *   index(in): value position number
 *   tpl_val(out): set to point to the specified value
 *   val_size(out): set to the value size
 *
 * Note: The index validity check must be done by the caller.
 */
QFILE_TUPLE_VALUE_FLAG
qfile_locate_tuple_value_r (QFILE_TUPLE tuple, int index,
			    char **tuple_value_p, int *value_size_p)
{
  int i;

  for (i = 0; i < index; i++)
    {
      tuple +=
	QFILE_TUPLE_VALUE_HEADER_SIZE + QFILE_GET_TUPLE_VALUE_LENGTH (tuple);
    }

  *tuple_value_p = tuple + QFILE_TUPLE_VALUE_HEADER_SIZE;
  *value_size_p = QFILE_GET_TUPLE_VALUE_LENGTH (tuple);

  if (QFILE_GET_TUPLE_VALUE_FLAG (tuple) == V_UNBOUND)
    {
      return V_UNBOUND;
    }
  else
    {
      return V_BOUND;
    }
}

#if defined (RYE_DEBUG)
/*
 * qfile_print_tuple () - Prints the tuple content associated with the type list
 *   return: none
 *   type_list(in): type list
 *   tpl(in): tuple
 * Note: Each tuple start is aligned with MAX_ALIGNMENT
 *       Each tuple value header is aligned with MAX_ALIGNMENT,
 *       Each tuple value is aligned with MAX_ALIGNMENT
 */
static void
qfile_print_tuple (QFILE_TUPLE_VALUE_TYPE_LIST * type_list_p,
		   QFILE_TUPLE tuple)
{
  DB_VALUE dbval;
  PR_TYPE *pr_type_p;
  int i;
  char *tuple_p;
  OR_BUF buf;

  db_make_null (&dbval);

  if (type_list_p == NULL || type_list_p->type_cnt <= 0)
    {
      return;
    }

  fprintf (stdout, "\n{ ");
  tuple_p = (char *) tuple + QFILE_TUPLE_LENGTH_SIZE;

  for (i = 0; i < type_list_p->type_cnt; i++)
    {
      if (QFILE_GET_TUPLE_VALUE_FLAG (tuple_p) == V_BOUND)
	{
	  pr_type_p = type_list_p->domp[i]->type;
	  or_init (&buf, tuple_p + QFILE_TUPLE_VALUE_HEADER_SIZE,
		   QFILE_GET_TUPLE_VALUE_LENGTH (tuple_p));
	  (*(pr_type_p->readval)) (&buf, &dbval, type_list_p->domp[i], -1,
				   true, NULL, 0);

	  (*(pr_type_p->fptrfunc)) (stdout, &dbval);
	  if (pr_is_set_type (pr_type_p->id))
	    {
	      pr_clear_value (&dbval);
	    }
	}
      else
	{
	  fprintf (stdout, "VALUE_UNBOUND");
	}

      if (i != type_list_p->type_cnt - 1)
	{
	  fprintf (stdout, " , ");
	}

      tuple_p +=
	QFILE_TUPLE_VALUE_HEADER_SIZE +
	QFILE_GET_TUPLE_VALUE_LENGTH (tuple_p);
    }

  fprintf (stdout, " }\n");
}
#endif

static void
qfile_initialize_page_header (PAGE_PTR page_p)
{
  OR_PUT_INT (page_p + QFILE_TUPLE_COUNT_OFFSET, 0);
  OR_PUT_INT (page_p + QFILE_PREV_PAGE_ID_OFFSET, NULL_PAGEID);
  OR_PUT_INT (page_p + QFILE_NEXT_PAGE_ID_OFFSET, NULL_PAGEID);
  OR_PUT_INT (page_p + QFILE_LAST_TUPLE_OFFSET, 0);
  OR_PUT_INT (page_p + QFILE_OVERFLOW_PAGE_ID_OFFSET, NULL_PAGEID);
  OR_PUT_SHORT (page_p + QFILE_PREV_VOL_ID_OFFSET, NULL_VOLID);
  OR_PUT_SHORT (page_p + QFILE_NEXT_VOL_ID_OFFSET, NULL_VOLID);
  OR_PUT_SHORT (page_p + QFILE_OVERFLOW_VOL_ID_OFFSET, NULL_VOLID);
#if !defined(NDEBUG)
  /* suppress valgrind UMW error */
  memset (page_p + QFILE_RESERVED_OFFSET, 0,
	  QFILE_PAGE_HEADER_SIZE - QFILE_RESERVED_OFFSET);
#endif
}

/*
 * qfile_store_xasl () - Store the XASL stream into the temporary file
 *   return: number of pages or ER_FAILED
 *   stream(in/out): xasl_stream & xasl_stream_size in, xasl_id out
 *                   stream->xasl_id is a pointer to XASL_ID that
 *                   will be set to XASL file id set to XASL file id;
 *                   first_vpid and temp_vfid
 */
int
qfile_store_xasl (THREAD_ENTRY * thread_p, XASL_STREAM * stream)
{
  VPID *cur_vpid_p, prev_vpid, vpid_array[QFILE_PAGE_EXTENDS];
  DKNPAGES nth_page, num_pages;
  PAGE_PTR cur_page_p, prev_page_p;
  int xasl_page_size, total_pages, page_index, n;
  struct timeval time_stored;

  XASL_ID *xasl_id = stream->xasl_id;
  int xasl_stream_size = stream->xasl_stream_size;
  char *xasl_stream = stream->xasl_stream;

  XASL_ID_SET_NULL (xasl_id);

  if (file_create_queryarea (thread_p, &xasl_id->temp_vfid,
			     QFILE_DEFAULT_PAGES, "XASL stream file") == NULL)
    {
      return 0;
    }

  page_index = QFILE_PAGE_EXTENDS;
  total_pages = nth_page = n = 0;
  num_pages =
    file_get_numpages (thread_p, &xasl_id->temp_vfid, NULL, NULL, NULL);

  VPID_SET_NULL (&prev_vpid);
  prev_page_p = NULL;

  while (xasl_stream_size > 0)
    {
      if (page_index >= QFILE_PAGE_EXTENDS || page_index >= n)
	{
	  if (nth_page >= num_pages)
	    {
	      if (file_alloc_pages_as_noncontiguous (thread_p,
						     &xasl_id->temp_vfid,
						     vpid_array, &nth_page,
						     QFILE_PAGE_EXTENDS, NULL,
						     NULL, NULL,
						     NULL) == NULL)
		{
		  goto error;
		}

	      num_pages =
		file_get_numpages (thread_p, &xasl_id->temp_vfid, NULL, NULL,
				   NULL);
	    }

	  n = file_find_nthpages (thread_p, &xasl_id->temp_vfid, vpid_array,
				  nth_page, QFILE_PAGE_EXTENDS);
	  if (n < 0)
	    {
	      goto error;
	    }

	  total_pages += n;
	  nth_page += n;
	  page_index = 0;
	}

      cur_vpid_p = &vpid_array[page_index];
      cur_page_p = pgbuf_fix (thread_p, cur_vpid_p, NEW_PAGE,
			      PGBUF_LATCH_WRITE, PGBUF_UNCONDITIONAL_LATCH,
			      MNT_STATS_DATA_PAGE_FETCHES_XASL);
      if (cur_page_p == NULL)
	{
	  goto error;
	}

      qfile_initialize_page_header (cur_page_p);
      pgbuf_set_dirty (thread_p, cur_page_p, DONT_FREE);

      xasl_page_size = MIN (xasl_stream_size, qfile_Xasl_page_size);
      if (prev_page_p == NULL)
	{
	  /* this is the first page */
	  xasl_id->first_vpid = *cur_vpid_p;
	  QFILE_PUT_XASL_PAGE_SIZE (cur_page_p, xasl_stream_size);
	}
      else
	{
	  /* other than first page */
	  QFILE_PUT_XASL_PAGE_SIZE (cur_page_p, xasl_page_size);
	  QFILE_PUT_PREV_VPID (cur_page_p, &prev_vpid);
	  QFILE_PUT_NEXT_VPID (prev_page_p, cur_vpid_p);

	  pgbuf_set_dirty (thread_p, prev_page_p, FREE);
	  prev_page_p = NULL;
	}

      memcpy (cur_page_p + QFILE_PAGE_HEADER_SIZE, xasl_stream,
	      xasl_page_size);

      xasl_stream_size -= xasl_page_size;
      xasl_stream += xasl_page_size;
      prev_page_p = cur_page_p;
      prev_vpid = *cur_vpid_p;
      page_index++;
    }

  if (prev_page_p)
    {
      pgbuf_set_dirty (thread_p, prev_page_p, FREE);
      prev_page_p = NULL;
    }

  /* save stored time */
  (void) gettimeofday (&time_stored, NULL);
  CACHE_TIME_MAKE (&xasl_id->time_stored, &time_stored);

  return total_pages;

error:
  if (prev_page_p)
    {
      pgbuf_unfix_and_init (thread_p, prev_page_p);
    }
  file_destroy (thread_p, &xasl_id->temp_vfid);
  XASL_ID_SET_NULL (xasl_id);

  return 0;
}

/*
 * qfile_load_xasl_node_header () - Load XASL node header from xasl stream
 *
 * return	       : void
 * thread_p (in)       : thread entry
 * xasl_id_p (in)      : XASL file id
 * xasl_header_p (out) : pointer to XASL node header
 */
void
qfile_load_xasl_node_header (THREAD_ENTRY * thread_p,
			     const XASL_ID * xasl_id_p,
			     XASL_NODE_HEADER * xasl_header_p)
{
  char *xasl_stream = NULL;
  PAGE_PTR xasl_page_p = NULL;

  if (xasl_header_p == NULL)
    {
      /* cannot save XASL node header */
      return;
    }
  /* initialize XASL node header */
  INIT_XASL_NODE_HEADER (xasl_header_p);

  if (xasl_id_p == NULL || XASL_ID_IS_NULL (xasl_id_p))
    {
      /* cannot obtain XASL stream */
      return;
    }

  /* get XASL stream page */
  xasl_page_p =
    pgbuf_fix (thread_p, &xasl_id_p->first_vpid, OLD_PAGE, PGBUF_LATCH_READ,
	       PGBUF_UNCONDITIONAL_LATCH, MNT_STATS_DATA_PAGE_FETCHES_XASL);
  if (xasl_page_p == NULL)
    {
      return;
    }
  xasl_stream = xasl_page_p + QFILE_PAGE_HEADER_SIZE;
  /* get XASL node header from stream */
  (void) stx_map_stream_to_xasl_node_header (thread_p, xasl_header_p,
					     xasl_stream);
  pgbuf_unfix_and_init (thread_p, xasl_page_p);
}

/*
 * qfile_load_xasl () - Load the XASL stream from the temporary file
 *   return: number of pages or ER_FAILED
 *   xasl_id(in): XASL file id
 *   xasl(out): XASL stream
 *   size(out): size of XASL stream
 */
int
qfile_load_xasl (THREAD_ENTRY * thread_p, const XASL_ID * xasl_id_p,
		 char **xasl_p, int *size_p)
{
  PAGE_PTR cur_page_p;
  VPID next_vpid;
  char *p;
  int s, xasl_page_size, total_pages;

  cur_page_p = pgbuf_fix (thread_p, &xasl_id_p->first_vpid, OLD_PAGE,
			  PGBUF_LATCH_READ, PGBUF_UNCONDITIONAL_LATCH,
			  MNT_STATS_DATA_PAGE_FETCHES_XASL);
  if (cur_page_p == NULL)
    {
      return 0;
    }

  *size_p = QFILE_GET_XASL_PAGE_SIZE (cur_page_p);
  if (*size_p <= 0 || (*xasl_p = (char *) malloc (*size_p)) == NULL)
    {
      pgbuf_unfix_and_init (thread_p, cur_page_p);
      return 0;
    }

  total_pages = 0;
  s = *size_p;
  p = *xasl_p;

  do
    {
      total_pages++;
      xasl_page_size = QFILE_GET_XASL_PAGE_SIZE (cur_page_p);
      xasl_page_size = MIN (xasl_page_size, qfile_Xasl_page_size);
      QFILE_GET_NEXT_VPID (&next_vpid, cur_page_p);

      memcpy (p, cur_page_p + QFILE_PAGE_HEADER_SIZE, xasl_page_size);

      s -= xasl_page_size;
      p += xasl_page_size;

      pgbuf_unfix_and_init (thread_p, cur_page_p);
      if (!VPID_ISNULL (&next_vpid))
	{
	  cur_page_p = pgbuf_fix (thread_p, &next_vpid, OLD_PAGE,
				  PGBUF_LATCH_READ,
				  PGBUF_UNCONDITIONAL_LATCH,
				  MNT_STATS_DATA_PAGE_FETCHES_XASL);
	  if (cur_page_p == NULL)
	    {
	      free_and_init (*xasl_p);
	      return 0;
	    }
	}
    }
  while (s > 0 && !VPID_ISNULL (&next_vpid));

  if (cur_page_p != NULL)
    {
      pgbuf_unfix_and_init (thread_p, cur_page_p);
    }

  return total_pages;
}

/*
 * qfile_initialize () -
 *   return: int (true : successful initialization,
 *                false: unsuccessful initialization)
 *
 * Note: This routine initializes the query file manager structures
 * and global variables.
 */
int
qfile_initialize (void)
{
  SORT_LIST *sort_list_p;
  int i;
#if defined(SERVER_MODE)
  int rv;
#endif /* SERVER_MODE */

  qfile_Max_tuple_page_size = QFILE_MAX_TUPLE_SIZE_IN_PAGE;
  qfile_Xasl_page_size = spage_max_record_size () - QFILE_PAGE_HEADER_SIZE;

  rv = pthread_mutex_lock (&qfile_Free_sort_list_mutex);

  if (qfile_Free_sort_list == NULL)
    {
      /* TODO: introduce param rather than 10 */
      for (i = 0; i < 10; i++)
	{
	  sort_list_p = (SORT_LIST *) malloc (sizeof (SORT_LIST));
	  if (sort_list_p == NULL)
	    {
	      break;
	    }

	  sort_list_p->next = qfile_Free_sort_list;
	  qfile_Free_sort_list = sort_list_p;
	}

      qfile_Free_sort_list_total = i;
      qfile_Free_sort_list_count = i;
    }

  pthread_mutex_unlock (&qfile_Free_sort_list_mutex);

  return true;
}

/* qfile_finalize () -
 *   return:
 */
void
qfile_finalize (void)
{
  SORT_LIST *sort_list_p;

  while (qfile_Free_sort_list != NULL)
    {
      sort_list_p = qfile_Free_sort_list;
      qfile_Free_sort_list = sort_list_p->next;
      sort_list_p->next = NULL;
      qfile_Free_sort_list_count--;
      qfile_Free_sort_list_total--;
      free_and_init (sort_list_p);
    }

  assert (qfile_Free_sort_list_count == 0);
  assert (qfile_Free_sort_list_total == 0);
}

/*
 * qfile_open_list () -
 *   return: QFILE_LIST_ID *, or NULL
 *   type_list(in/out): type list for the list file to be created
 *   sort_list(in): sort info for the list file to be created
 *   query_id(in): query id associated with this list file
 *   flag(in): {QFILE_FLAG_RESULT_FILE, QFILE_FLAG_DISTINCT, QFILE_FLAG_ALL}
 *             whether to do 'all' or 'distinct' operation
 *
 * Note: A list file is created by using the specified type list and
 *       the list file identifier is set. The first page of the list
 *       file is allocated only when the first tuple is inserted to
 *       list file, if any.
 *	 A 'SORT_LIST' is associated to the output list file according to
 *	 'sort_list_p' input argument (if not null), or created if the
 *	 QFILE_FLAG_DISTINCT flag is specified; if neither QFILE_FLAG_DISTINCT
 *	 or 'sort_list_p' are supplied, no SORT_LIST is associated.
 *
 */
QFILE_LIST_ID *
qfile_open_list (THREAD_ENTRY * thread_p,
		 QFILE_TUPLE_VALUE_TYPE_LIST * type_list_p,
		 SORT_LIST * sort_list_p, QUERY_ID query_id, int flag)
{
  QFILE_LIST_ID *list_id_p;
  int len, i;
  SORT_LIST *src_sort_list_p, *dest_sort_list_p;
  size_t type_list_size;

  list_id_p = (QFILE_LIST_ID *) malloc (sizeof (QFILE_LIST_ID));
  if (list_id_p == NULL)
    {
      return NULL;
    }

  QFILE_CLEAR_LIST_ID (list_id_p);
  list_id_p->tuple_cnt = 0;
  list_id_p->page_cnt = 0;
  list_id_p->type_list.type_cnt = type_list_p->type_cnt;
  list_id_p->query_id = query_id;

  if (QFILE_IS_FLAG_SET (flag, QFILE_FLAG_USE_KEY_BUFFER))
    {
      list_id_p->tfile_vfid =
	qmgr_create_new_temp_file (thread_p, query_id,
				   TEMP_FILE_MEMBUF_KEY_BUFFER);
    }
  else
    {
      list_id_p->tfile_vfid =
	qmgr_create_new_temp_file (thread_p, query_id,
				   TEMP_FILE_MEMBUF_NORMAL);
    }

  if (list_id_p->tfile_vfid == NULL)
    {
      free_and_init (list_id_p);
      return NULL;
    }

  VFID_COPY (&(list_id_p->temp_vfid), &(list_id_p->tfile_vfid->temp_vfid));
  list_id_p->type_list.domp = NULL;

  if (list_id_p->type_list.type_cnt != 0)
    {
      type_list_size =
	list_id_p->type_list.type_cnt * DB_SIZEOF (TP_DOMAIN *);
      list_id_p->type_list.domp = (TP_DOMAIN **) malloc (type_list_size);
      if (list_id_p->type_list.domp == NULL)
	{
	  free_and_init (list_id_p);
	  return NULL;
	}

      memcpy (list_id_p->type_list.domp, type_list_p->domp, type_list_size);
    }

  /* build sort_list */
  if (QFILE_IS_FLAG_SET (flag, QFILE_FLAG_DISTINCT))
    {
      len = list_id_p->type_list.type_cnt;
      if (len > 0)
	{
	  dest_sort_list_p = qfile_allocate_sort_list (len);
	  if (dest_sort_list_p == NULL)
	    {
	      free_and_init (list_id_p->type_list.domp);
	      free_and_init (list_id_p);
	      return NULL;
	    }

	  for (i = 0, list_id_p->sort_list = dest_sort_list_p; i < len;
	       i++, dest_sort_list_p = dest_sort_list_p->next)
	    {
	      dest_sort_list_p->s_order = S_ASC;
	      dest_sort_list_p->s_nulls = S_NULLS_FIRST;
	      dest_sort_list_p->pos_descr.pos_no = i;
	    }
	}
    }
  else if (sort_list_p != NULL)
    {
      len = qfile_get_sort_list_size (sort_list_p);
      if (len > 0)
	{
	  dest_sort_list_p = qfile_allocate_sort_list (len);
	  if (dest_sort_list_p == NULL)
	    {
	      free_and_init (list_id_p->type_list.domp);
	      free_and_init (list_id_p);
	      return NULL;
	    }

	  for (src_sort_list_p = sort_list_p, list_id_p->sort_list =
	       dest_sort_list_p; src_sort_list_p;
	       src_sort_list_p = src_sort_list_p->next, dest_sort_list_p =
	       dest_sort_list_p->next)
	    {
	      dest_sort_list_p->s_order = src_sort_list_p->s_order;
	      dest_sort_list_p->s_nulls = src_sort_list_p->s_nulls;
	      dest_sort_list_p->pos_descr.pos_no =
		src_sort_list_p->pos_descr.pos_no;
	    }
	}
    }
  else
    {
      /* no DISTINCT and no source SORT_LIST supplied */
      list_id_p->sort_list = NULL;
    }

  return list_id_p;
}

/*
 * qfile_close_list () -
 *   return: none
 *   list_id(in/out): List file identifier
 *
 * Note: The specified list file is closed and memory buffer for the
 *       list file is freed.
 */
void
qfile_close_list (THREAD_ENTRY * thread_p, QFILE_LIST_ID * list_id_p)
{
  if (list_id_p)
    {
      if (list_id_p->last_pgptr != NULL)
	{
	  QFILE_PUT_NEXT_VPID_NULL (list_id_p->last_pgptr);

	  qmgr_free_old_page_and_init (thread_p, list_id_p->last_pgptr,
				       list_id_p->tfile_vfid);
	}
    }
}

/*
 * qfile_reopen_list_as_append_mode () -
 *   thread_p(in) :
 *   list_id_p(in):
 *
 * Note:
 */
int
qfile_reopen_list_as_append_mode (THREAD_ENTRY * thread_p,
				  QFILE_LIST_ID * list_id_p)
{
  PAGE_PTR last_page_ptr;
  QMGR_TEMP_FILE *temp_file_p;

  if (list_id_p->tfile_vfid == NULL)
    {
      /* Invalid list_id_p. list_id_p might be cleared or not be opened.
       * list_id_p must have valid QMGR_TEMP_FILE to reopen.
       */
      assert_release (0);
      return ER_FAILED;
    }

  if (VPID_ISNULL (&list_id_p->first_vpid))
    {
      assert_release (VPID_ISNULL (&list_id_p->last_vpid));
      assert_release (list_id_p->last_pgptr == NULL);

      return NO_ERROR;
    }

  if (list_id_p->last_pgptr != NULL)
    {
      return NO_ERROR;
    }

  temp_file_p = list_id_p->tfile_vfid;

  if (temp_file_p->membuf && list_id_p->last_vpid.volid == NULL_VOLID)
    {
      /* The last page is in the membuf */
      assert_release (temp_file_p->membuf_last >=
		      list_id_p->last_vpid.pageid);
      /* The page of last record in the membuf */
      last_page_ptr = temp_file_p->membuf[list_id_p->last_vpid.pageid];
    }
  else
    {
      assert_release (!VPID_ISNULL (&list_id_p->last_vpid));
      last_page_ptr = pgbuf_fix (thread_p, &list_id_p->last_vpid, OLD_PAGE,
				 PGBUF_LATCH_WRITE,
				 PGBUF_UNCONDITIONAL_LATCH,
				 MNT_STATS_DATA_PAGE_FETCHES_QRESULT);
      if (last_page_ptr == NULL)
	{
	  return ER_FAILED;
	}
    }

  list_id_p->last_pgptr = last_page_ptr;

  return NO_ERROR;
}

static bool
qfile_is_first_tuple (QFILE_LIST_ID * list_id_p)
{
  return VPID_ISNULL (&list_id_p->first_vpid);
}

static bool
qfile_is_last_page_full (QFILE_LIST_ID * list_id_p, int tuple_length,
			 bool is_ovf_page)
{
  bool result;

  result = tuple_length + list_id_p->last_offset > DB_PAGESIZE;

  if (result && !is_ovf_page)
    {
      result = list_id_p->last_offset > QFILE_PAGE_HEADER_SIZE;
    }

  return result;
}

static void
qfile_set_dirty_page (THREAD_ENTRY * thread_p, PAGE_PTR page_p, int free_page,
		      QMGR_TEMP_FILE * vfid_p)
{
  LOG_DATA_ADDR addr;

  LOG_ADDR_SET (&addr, NULL, page_p, -1);
  qmgr_set_dirty_page (thread_p, page_p, free_page, &addr, vfid_p);
}

static PAGE_PTR
qfile_allocate_new_page (THREAD_ENTRY * thread_p, QFILE_LIST_ID * list_id_p,
			 PAGE_PTR page_p, bool is_ovf_page)
{
  PAGE_PTR new_page_p;
  VPID new_vpid;

  if (qmgr_interrupt_query (thread_p, list_id_p->query_id) == true)
    {
      return NULL;
    }

  new_page_p = qmgr_get_new_page (thread_p, &new_vpid, list_id_p->tfile_vfid);
  if (new_page_p == NULL)
    {
      return NULL;
    }

  QFILE_PUT_TUPLE_COUNT (new_page_p, 0);
  QFILE_PUT_PREV_VPID (new_page_p, &list_id_p->last_vpid);

  /*
   * For streaming query support, set next_vpid differently
   */
  if (is_ovf_page)
    {
      QFILE_PUT_NEXT_VPID_NULL (new_page_p);
    }
  else
    {
      LS_PUT_NEXT_VPID_NULL_ASYNC (new_page_p);
    }

  QFILE_PUT_LAST_TUPLE_OFFSET (new_page_p, QFILE_PAGE_HEADER_SIZE);
  QFILE_PUT_OVERFLOW_VPID_NULL (new_page_p);

  if (page_p)
    {
      QFILE_PUT_NEXT_VPID (page_p, &new_vpid);
    }

  list_id_p->page_cnt++;

  if (page_p)
    {
      qfile_set_dirty_page (thread_p, page_p, FREE, list_id_p->tfile_vfid);
    }
  else
    {
      /* first list file tuple */
      QFILE_COPY_VPID (&list_id_p->first_vpid, &new_vpid);
    }
  QFILE_COPY_VPID (&list_id_p->last_vpid, &new_vpid);
  list_id_p->last_pgptr = new_page_p;
  list_id_p->last_offset = QFILE_PAGE_HEADER_SIZE;

  return new_page_p;
}

static PAGE_PTR
qfile_allocate_new_ovf_page (THREAD_ENTRY * thread_p,
			     QFILE_LIST_ID * list_id_p, PAGE_PTR page_p,
			     PAGE_PTR prev_page_p, int tuple_length,
			     int offset, int *tuple_page_size_p)
{
  PAGE_PTR new_page_p;
  VPID new_vpid;

  *tuple_page_size_p = MIN (tuple_length - offset, qfile_Max_tuple_page_size);

  new_page_p = qmgr_get_new_page (thread_p, &new_vpid, list_id_p->tfile_vfid);
  if (new_page_p == NULL)
    {
      return NULL;
    }

  list_id_p->page_cnt++;

  QFILE_PUT_NEXT_VPID_NULL (new_page_p);
  QFILE_PUT_TUPLE_COUNT (new_page_p, QFILE_OVERFLOW_TUPLE_COUNT_FLAG);
  QFILE_PUT_OVERFLOW_TUPLE_PAGE_SIZE (new_page_p, *tuple_page_size_p);
  QFILE_PUT_OVERFLOW_VPID_NULL (new_page_p);

  /*
   * connect the previous page to this page and free,
   * if it is not the first page
   */
  QFILE_PUT_OVERFLOW_VPID (prev_page_p, &new_vpid);
  if (prev_page_p != page_p)
    {
      qfile_set_dirty_page (thread_p, prev_page_p, FREE,
			    list_id_p->tfile_vfid);
    }

  return new_page_p;
}

static int
qfile_allocate_new_page_if_need (THREAD_ENTRY * thread_p,
				 QFILE_LIST_ID * list_id_p, PAGE_PTR * page_p,
				 int tuple_length, bool is_ovf_page)
{
  PAGE_PTR new_page_p;

  if (qfile_is_first_tuple (list_id_p)
      || qfile_is_last_page_full (list_id_p, tuple_length, is_ovf_page))
    {
      new_page_p = qfile_allocate_new_page (thread_p, list_id_p, *page_p,
					    is_ovf_page);
      if (new_page_p == NULL)
	{
	  return ER_FAILED;
	}

      *page_p = new_page_p;
    }

  QFILE_PUT_TUPLE_COUNT (*page_p, QFILE_GET_TUPLE_COUNT (*page_p) + 1);
  QFILE_PUT_LAST_TUPLE_OFFSET (*page_p, list_id_p->last_offset);

  return NO_ERROR;
}

static void
qfile_add_tuple_to_list_id (QFILE_LIST_ID * list_id_p, PAGE_PTR page_p,
			    int tuple_length, int offset)
{
  QFILE_PUT_PREV_TUPLE_LENGTH (page_p, list_id_p->lasttpl_len);

  list_id_p->tuple_cnt++;
  list_id_p->lasttpl_len = tuple_length;
  list_id_p->last_offset += offset;
}

/*
 * qfile_add_tuple_to_list () - The given tuple is added to the end of the list file
 *   return: int (NO_ERROR or ER_FAILED)
 *   list_id(in): List File Identifier
 * 	 tpl(in): Tuple to be added
 *
 */
int
qfile_add_tuple_to_list (THREAD_ENTRY * thread_p, QFILE_LIST_ID * list_id_p,
			 QFILE_TUPLE tuple)
{
  PAGE_PTR cur_page_p, new_page_p, prev_page_p;
  int tuple_length;
  char *page_p, *tuple_p;
  int offset, tuple_page_size;

  if (list_id_p == NULL)
    {
      return ER_FAILED;
    }

  QFILE_CHECK_LIST_FILE_IS_CLOSED (list_id_p);

  cur_page_p = list_id_p->last_pgptr;
  tuple_length = QFILE_GET_TUPLE_LENGTH (tuple);

  if (qfile_allocate_new_page_if_need (thread_p, list_id_p, &cur_page_p,
				       tuple_length, false) != NO_ERROR)
    {
      return ER_FAILED;
    }

  page_p = (char *) cur_page_p + list_id_p->last_offset;
  tuple_page_size = MIN (tuple_length, qfile_Max_tuple_page_size);
  memcpy (page_p, tuple, tuple_page_size);

  offset = ((tuple_length + list_id_p->last_offset) > DB_PAGESIZE ?
	    DB_PAGESIZE : tuple_length);
  qfile_add_tuple_to_list_id (list_id_p, page_p, tuple_length, offset);

  prev_page_p = cur_page_p;
  for (offset = tuple_page_size, tuple_p = (char *) tuple + offset;
       offset < tuple_length;
       offset += tuple_page_size, tuple_p += tuple_page_size)
    {
      new_page_p = qfile_allocate_new_ovf_page (thread_p, list_id_p,
						cur_page_p, prev_page_p,
						tuple_length, offset,
						&tuple_page_size);
      if (new_page_p == NULL)
	{
	  if (prev_page_p != cur_page_p)
	    {
	      qfile_set_dirty_page (thread_p, prev_page_p, FREE,
				    list_id_p->tfile_vfid);
	    }
	  return ER_FAILED;
	}

      memcpy ((char *) new_page_p + QFILE_PAGE_HEADER_SIZE, tuple_p,
	      tuple_page_size);

      prev_page_p = new_page_p;
    }

  if (prev_page_p != cur_page_p)
    {
      QFILE_PUT_OVERFLOW_VPID_NULL (prev_page_p);
      qfile_set_dirty_page (thread_p, prev_page_p, FREE,
			    list_id_p->tfile_vfid);
    }

  qfile_set_dirty_page (thread_p, cur_page_p, DONT_FREE,
			list_id_p->tfile_vfid);

  return NO_ERROR;
}

static int
qfile_save_single_bound_item_tuple (QFILE_TUPLE_DESCRIPTOR * tuple_descr_p,
				    char *tuple_p, char *page_p,
				    int tuple_length)
{
  int align;

  align = DB_ALIGN (tuple_descr_p->item_size, MAX_ALIGNMENT);

  QFILE_PUT_TUPLE_VALUE_FLAG (tuple_p, V_BOUND);
  QFILE_PUT_TUPLE_VALUE_LENGTH (tuple_p, align);

  tuple_p += QFILE_TUPLE_VALUE_HEADER_SIZE;
  memcpy (tuple_p, tuple_descr_p->item, tuple_descr_p->item_size);
#if !defined(NDEBUG)
  /* suppress valgrind UMW error */
  memset (tuple_p + tuple_descr_p->item_size, 0,
	  align - tuple_descr_p->item_size);
#endif

  QFILE_PUT_TUPLE_LENGTH (page_p, tuple_length);

  return NO_ERROR;
}

static int
qfile_save_normal_tuple (QFILE_TUPLE_DESCRIPTOR * tuple_descr_p,
			 char *tuple_p, char *page_p, int tuple_length)
{
  int i, tuple_value_size;

  for (i = 0; i < tuple_descr_p->f_cnt; i++)
    {
      if (qdata_copy_db_value_to_tuple_value
	  (tuple_descr_p->f_valp[i], tuple_p, &tuple_value_size) != NO_ERROR)
	{
	  return ER_FAILED;
	}
      tuple_p += tuple_value_size;
    }

  QFILE_PUT_TUPLE_LENGTH (page_p, tuple_length);
  return NO_ERROR;
}

static int
qfile_save_sort_key_tuple (QFILE_TUPLE_DESCRIPTOR * tuple_descr_p,
			   char *tuple_p, char *page_p, int tuple_length)
{
  SORTKEY_INFO *key_info_p;
  SORT_REC *sort_rec_p;
  int i, c, nkeys, len;
  char *src_p;

  key_info_p = (SORTKEY_INFO *) (tuple_descr_p->sortkey_info);
  nkeys = key_info_p->nkeys;
  sort_rec_p = (SORT_REC *) (tuple_descr_p->sort_rec);

  for (i = 0; i < nkeys; i++)
    {
      c = key_info_p->key[i].permuted_col;

      if (sort_rec_p->s.offset[c] == 0)
	{
	  QFILE_PUT_TUPLE_VALUE_FLAG (tuple_p, V_UNBOUND);
	  QFILE_PUT_TUPLE_VALUE_LENGTH (tuple_p, 0);
	}
      else
	{
	  src_p =
	    (char *) sort_rec_p + sort_rec_p->s.offset[c] -
	    QFILE_TUPLE_VALUE_HEADER_SIZE;
	  len = QFILE_GET_TUPLE_VALUE_LENGTH (src_p);
	  memcpy (tuple_p, src_p, len + QFILE_TUPLE_VALUE_HEADER_SIZE);
	  tuple_p += len;
	}

      tuple_p += QFILE_TUPLE_VALUE_HEADER_SIZE;
    }

  QFILE_PUT_TUPLE_LENGTH (page_p, tuple_length);
  return NO_ERROR;
}

int
qfile_save_tuple (QFILE_TUPLE_DESCRIPTOR * tuple_descr_p,
		  QFILE_TUPLE_TYPE tuple_type, char *page_p,
		  int *tuple_length_p)
{
  char *tuple_p;

  tuple_p = (char *) page_p + QFILE_TUPLE_LENGTH_SIZE;

  switch (tuple_type)
    {
    case T_SINGLE_BOUND_ITEM:
      (void) qfile_save_single_bound_item_tuple (tuple_descr_p, tuple_p,
						 page_p, *tuple_length_p);
      break;

    case T_NORMAL:
      if (qfile_save_normal_tuple (tuple_descr_p, tuple_p, page_p,
				   *tuple_length_p) != NO_ERROR)
	{
	  return ER_FAILED;
	}
      break;

    case T_SORTKEY:
      (void) qfile_save_sort_key_tuple (tuple_descr_p, tuple_p, page_p,
					*tuple_length_p);
      break;

    default:
      return ER_FAILED;
    }

  return NO_ERROR;
}

/*
 * qfile_generate_tuple_into_list () -
 *   return: int (NO_ERROR or ER_FAILED)
 *   list_id(in/out): List File Identifier
 *   tpl_type(in): tuple descriptor type
 * 		   - single bound field tuple or multi field tuple
 *
 */
int
qfile_generate_tuple_into_list (THREAD_ENTRY * thread_p,
				QFILE_LIST_ID * list_id_p,
				QFILE_TUPLE_TYPE tuple_type)
{
  QFILE_TUPLE_DESCRIPTOR *tuple_descr_p;
  PAGE_PTR cur_page_p;
  int tuple_length, offset;
  char *page_p;

  if (list_id_p == NULL)
    {
      return ER_FAILED;
    }

  QFILE_CHECK_LIST_FILE_IS_CLOSED (list_id_p);

  cur_page_p = list_id_p->last_pgptr;
  tuple_descr_p = &(list_id_p->tpl_descr);
  tuple_length = tuple_descr_p->tpl_size;

  if (qfile_allocate_new_page_if_need (thread_p, list_id_p, &cur_page_p,
				       tuple_length, false) != NO_ERROR)
    {
      return ER_FAILED;
    }

  page_p = (char *) cur_page_p + list_id_p->last_offset;
  if (qfile_save_tuple (tuple_descr_p, tuple_type, page_p,
			&tuple_length) != NO_ERROR)
    {
      return ER_FAILED;
    }

  offset = ((tuple_length + list_id_p->last_offset) > DB_PAGESIZE ?
	    DB_PAGESIZE : tuple_length);
  qfile_add_tuple_to_list_id (list_id_p, page_p, tuple_length, offset);

  qfile_set_dirty_page (thread_p, cur_page_p, DONT_FREE,
			list_id_p->tfile_vfid);
  return NO_ERROR;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * qfile_fast_intint_tuple_to_list () - generate a two integer value tuple into
 *                                      a listfile
 *   return: int (NO_ERROR or ER_FAILED)
 *   list_id(in/out): List File Identifier
 *   v1(in): first int value
 *   v2(in): second int value
 *
 * NOTE: This function is meant to skip usual validation od DB_VALUES and
 * disk size computation in order to generate the tuple as fast as possible.
 * Also, it must write tuples identical to tuples generated by
 * qfile_generate_tuple_into_list via the built tuple descriptor. Generated
 * tuples must be readable and scanable via usual qfile routines.
 */
int
qfile_fast_intint_tuple_to_list (THREAD_ENTRY * thread_p,
				 QFILE_LIST_ID * list_id_p, int v1, int v2)
{
  PAGE_PTR cur_page_p;
  int tuple_length, tuple_value_length, tuple_value_size, offset;
  char *page_p, *tuple_p;

  if (list_id_p == NULL)
    {
      return ER_FAILED;
    }

  QFILE_CHECK_LIST_FILE_IS_CLOSED (list_id_p);

  /* compute sizes */
  tuple_value_size = DB_ALIGN (tp_Integer.disksize, MAX_ALIGNMENT);
  tuple_value_length = QFILE_TUPLE_VALUE_HEADER_SIZE + tuple_value_size;
  tuple_length = QFILE_TUPLE_LENGTH_SIZE + tuple_value_length * 2;

  /* fetch page or alloc if necessary */
  cur_page_p = list_id_p->last_pgptr;
  if (qfile_allocate_new_page_if_need (thread_p, list_id_p, &cur_page_p,
				       tuple_length, false) != NO_ERROR)
    {
      return ER_FAILED;
    }
  page_p = (char *) cur_page_p + list_id_p->last_offset;
  tuple_p = page_p + QFILE_TUPLE_LENGTH_SIZE;

  /* write the two not-null integers */
  QFILE_PUT_TUPLE_LENGTH (page_p, tuple_length);

  QFILE_PUT_TUPLE_VALUE_FLAG (tuple_p, V_BOUND);
  QFILE_PUT_TUPLE_VALUE_LENGTH (tuple_p, tuple_value_size);
  OR_PUT_INT (tuple_p + QFILE_TUPLE_VALUE_HEADER_SIZE, v1);
  tuple_p += tuple_value_length;

  QFILE_PUT_TUPLE_VALUE_FLAG (tuple_p, V_BOUND);
  QFILE_PUT_TUPLE_VALUE_LENGTH (tuple_p, tuple_value_size);
  OR_PUT_INT (tuple_p + QFILE_TUPLE_VALUE_HEADER_SIZE, v2);

  /* list_id maintainance stuff */
  offset = ((tuple_length + list_id_p->last_offset) > DB_PAGESIZE ?
	    DB_PAGESIZE : tuple_length);
  qfile_add_tuple_to_list_id (list_id_p, page_p, tuple_length, offset);

  qfile_set_dirty_page (thread_p, cur_page_p, DONT_FREE,
			list_id_p->tfile_vfid);
  return NO_ERROR;
}

/*
 * qfile_fast_intval_tuple_to_list () - generate a two value tuple into a file
 *   return: int (NO_ERROR, error code or positive overflow tuple size)
 *   list_id(in/out): List File Identifier
 *   v1(in): integer value
 *   v2(in): generic value
 *
 * NOTE: This function is meant to partially skip usual validation of DB_VALUES
 * and disk size computation in order to generate the tuple as fast as
 * possible. Also, it must write tuples identical to tuples generated by
 * qfile_generate_tuple_into_list via the built tuple descriptor. Generated
 * tuples must be readable and scanable via usual qfile routines.
 */
int
qfile_fast_intval_tuple_to_list (THREAD_ENTRY * thread_p,
				 QFILE_LIST_ID * list_id_p,
				 int v1, DB_VALUE * v2)
{
  PAGE_PTR cur_page_p;
  int tuple_length, tuple_int_value_size, tuple_int_value_length;
  int tuple_value_size, tuple_value_length, offset;
  char *page_p, *tuple_p;

  if (list_id_p == NULL)
    {
      return ER_FAILED;
    }

  QFILE_CHECK_LIST_FILE_IS_CLOSED (list_id_p);

  /* compute sizes */
  tuple_int_value_size = DB_ALIGN (tp_Integer.disksize, MAX_ALIGNMENT);
  tuple_int_value_length =
    QFILE_TUPLE_VALUE_HEADER_SIZE + tuple_int_value_size;
  tuple_value_size =
    DB_ALIGN (pr_data_writeval_disk_size (v2), MAX_ALIGNMENT);
  tuple_value_length = QFILE_TUPLE_VALUE_HEADER_SIZE + tuple_value_size;
  tuple_length =
    QFILE_TUPLE_LENGTH_SIZE + tuple_int_value_length + tuple_value_length;

  /* register tuple size and see if we can write it or not */
  list_id_p->tpl_descr.tpl_size = tuple_length;
  if (tuple_length > QFILE_MAX_TUPLE_SIZE_IN_PAGE)
    {
      /* can't write it here */
      return tuple_length;
    }

  /* fetch page or alloc if necessary */
  cur_page_p = list_id_p->last_pgptr;
  if (qfile_allocate_new_page_if_need (thread_p, list_id_p, &cur_page_p,
				       tuple_length, false) != NO_ERROR)
    {
      return ER_FAILED;
    }
  page_p = (char *) cur_page_p + list_id_p->last_offset;
  tuple_p = page_p + QFILE_TUPLE_LENGTH_SIZE;

  /* write the two not-null integers */
  QFILE_PUT_TUPLE_LENGTH (page_p, tuple_length);

  QFILE_PUT_TUPLE_VALUE_FLAG (tuple_p, V_BOUND);
  QFILE_PUT_TUPLE_VALUE_LENGTH (tuple_p, tuple_int_value_size);
  OR_PUT_INT (tuple_p + QFILE_TUPLE_VALUE_HEADER_SIZE, v1);
  tuple_p += tuple_int_value_length;

  if (DB_IS_NULL (v2))
    {
      QFILE_PUT_TUPLE_VALUE_FLAG (tuple_p, V_UNBOUND);
      QFILE_PUT_TUPLE_VALUE_LENGTH (tuple_p, 0);
    }
  else
    {
      DB_TYPE dbval_type = DB_VALUE_DOMAIN_TYPE (v2);
      PR_TYPE *pr_type = PR_TYPE_FROM_ID (dbval_type);
      OR_BUF buf;

      QFILE_PUT_TUPLE_VALUE_FLAG (tuple_p, V_BOUND);
      QFILE_PUT_TUPLE_VALUE_LENGTH (tuple_p, tuple_value_size);

      OR_BUF_INIT (buf, tuple_p + QFILE_TUPLE_VALUE_HEADER_SIZE,
		   tuple_value_size);
      if (pr_type == NULL
	  || (*(pr_type->data_writeval)) (&buf, v2) != NO_ERROR)
	{
	  return ER_FAILED;
	}
    }

  /* list_id maintainance stuff */
  offset = ((tuple_length + list_id_p->last_offset) > DB_PAGESIZE ?
	    DB_PAGESIZE : tuple_length);
  qfile_add_tuple_to_list_id (list_id_p, page_p, tuple_length, offset);

  qfile_set_dirty_page (thread_p, cur_page_p, DONT_FREE,
			list_id_p->tfile_vfid);
  return NO_ERROR;
}

/*
 * qfile_fast_val_tuple_to_list () - generate a one value tuple into
 *                                   a file
 *   return: int (NO_ERROR, error code or positive overflow tuple size)
 *   list_id(in/out): List File Identifier
 *   val(in): generic value
 *
 * NOTE: This function is meant to partially skip usual validation of DB_VALUES
 * and disk size computation in order to generate the tuple as fast as
 * possible. Also, it must write tuples identical to tuples generated by
 * qfile_generate_tuple_into_list via the built tuple descriptor. Generated
 * tuples must be readable and scanable via usual qfile routines.
 */
int
qfile_fast_val_tuple_to_list (THREAD_ENTRY * thread_p,
			      QFILE_LIST_ID * list_id_p, DB_VALUE * val)
{
  PAGE_PTR cur_page_p;
  int tuple_length;
  int tuple_value_size, tuple_value_length, offset;
  char *page_p, *tuple_p;

  if (list_id_p == NULL)
    {
      return ER_FAILED;
    }

  QFILE_CHECK_LIST_FILE_IS_CLOSED (list_id_p);

  tuple_value_size =
    DB_ALIGN (pr_data_writeval_disk_size (val), MAX_ALIGNMENT);
  tuple_value_length = QFILE_TUPLE_VALUE_HEADER_SIZE + tuple_value_size;
  tuple_length = QFILE_TUPLE_LENGTH_SIZE + tuple_value_length;

  /* register tuple size and see if we can write it or not */
  list_id_p->tpl_descr.tpl_size = tuple_length;
  if (tuple_length > QFILE_MAX_TUPLE_SIZE_IN_PAGE)
    {
      /* can't write it here */
      return tuple_length;
    }

  /* fetch page or alloc if necessary */
  cur_page_p = list_id_p->last_pgptr;
  if (qfile_allocate_new_page_if_need (thread_p, list_id_p, &cur_page_p,
				       tuple_length, false) != NO_ERROR)
    {
      return ER_FAILED;
    }
  page_p = (char *) cur_page_p + list_id_p->last_offset;
  tuple_p = page_p + QFILE_TUPLE_LENGTH_SIZE;

  /* write the two not-null integers */
  QFILE_PUT_TUPLE_LENGTH (page_p, tuple_length);

  if (DB_IS_NULL (val))
    {
      QFILE_PUT_TUPLE_VALUE_FLAG (tuple_p, V_UNBOUND);
      QFILE_PUT_TUPLE_VALUE_LENGTH (tuple_p, 0);
    }
  else
    {
      DB_TYPE dbval_type = DB_VALUE_DOMAIN_TYPE (val);
      PR_TYPE *pr_type = PR_TYPE_FROM_ID (dbval_type);
      OR_BUF buf;

      QFILE_PUT_TUPLE_VALUE_FLAG (tuple_p, V_BOUND);
      QFILE_PUT_TUPLE_VALUE_LENGTH (tuple_p, tuple_value_size);

      OR_BUF_INIT (buf, tuple_p + QFILE_TUPLE_VALUE_HEADER_SIZE,
		   tuple_value_size);
      if (pr_type == NULL
	  || (*(pr_type->data_writeval)) (&buf, val) != NO_ERROR)
	{
	  return ER_FAILED;
	}
    }

  /* list_id maintainance stuff */
  offset = ((tuple_length + list_id_p->last_offset) > DB_PAGESIZE ?
	    DB_PAGESIZE : tuple_length);
  qfile_add_tuple_to_list_id (list_id_p, page_p, tuple_length, offset);

  qfile_set_dirty_page (thread_p, cur_page_p, DONT_FREE,
			list_id_p->tfile_vfid);
  return NO_ERROR;
}
#endif

/*
 * qfile_add_overflow_tuple_to_list () -
 *   return: int (NO_ERROR or ER_FAILED)
 *   list_id(in/out): List File Identifier
 *   ovfl_tpl_pg(in): First page of the overflow tuple to be added
 *   input_list_id_p(in):
 *
 * Note: The indicated overflow tuple is added to the end of the list
 *              file. The given page contains the initial portion of the
 *              tuple and the rest of the tuple is formed from following
 *              overflow pages.
 *
 * Note: This routine is a specific routine of qfile_add_tuple_to_list used by list file
 *       sorting mechanism.
 */
int
qfile_add_overflow_tuple_to_list (THREAD_ENTRY * thread_p,
				  QFILE_LIST_ID * list_id_p,
				  PAGE_PTR ovf_tuple_page_p,
				  QFILE_LIST_ID * input_list_id_p)
{
  PAGE_PTR cur_page_p, new_page_p, prev_page_p, ovf_page_p;
  int tuple_length;
  char *page_p;
  int offset, tuple_page_size;
  QFILE_TUPLE tuple;
  VPID ovf_vpid;

  QFILE_CHECK_LIST_FILE_IS_CLOSED (list_id_p);

  tuple = (char *) ovf_tuple_page_p + QFILE_PAGE_HEADER_SIZE;
  cur_page_p = list_id_p->last_pgptr;
  tuple_length = QFILE_GET_TUPLE_LENGTH (tuple);

  if (qfile_allocate_new_page_if_need (thread_p, list_id_p, &cur_page_p,
				       tuple_length, true) != NO_ERROR)
    {
      return ER_FAILED;
    }

  page_p = (char *) cur_page_p + list_id_p->last_offset;
  tuple_page_size = MIN (tuple_length, qfile_Max_tuple_page_size);
  memcpy (page_p, tuple, tuple_page_size);

  qfile_add_tuple_to_list_id (list_id_p, page_p, tuple_length, tuple_length);

  prev_page_p = cur_page_p;
  QFILE_GET_OVERFLOW_VPID (&ovf_vpid, ovf_tuple_page_p);

  for (offset = tuple_page_size; offset < tuple_length;
       offset += tuple_page_size)
    {
      ovf_page_p = qmgr_get_old_page (thread_p, &ovf_vpid,
				      input_list_id_p->tfile_vfid);
      if (ovf_page_p == NULL)
	{
	  return ER_FAILED;
	}

      QFILE_GET_OVERFLOW_VPID (&ovf_vpid, ovf_page_p);

      new_page_p = qfile_allocate_new_ovf_page (thread_p, list_id_p,
						cur_page_p, prev_page_p,
						tuple_length, offset,
						&tuple_page_size);
      if (new_page_p == NULL)
	{
	  if (prev_page_p != cur_page_p)
	    {
	      qfile_set_dirty_page (thread_p, prev_page_p, FREE,
				    list_id_p->tfile_vfid);
	    }
	  return ER_FAILED;
	}

      memcpy ((char *) new_page_p + QFILE_PAGE_HEADER_SIZE,
	      (char *) ovf_page_p + QFILE_PAGE_HEADER_SIZE, tuple_page_size);

      qmgr_free_old_page_and_init (thread_p, ovf_page_p,
				   input_list_id_p->tfile_vfid);
      prev_page_p = new_page_p;
    }

  if (prev_page_p != cur_page_p)
    {
      QFILE_PUT_OVERFLOW_VPID_NULL (prev_page_p);
      qfile_set_dirty_page (thread_p, prev_page_p, FREE,
			    list_id_p->tfile_vfid);
    }

  qfile_set_dirty_page (thread_p, cur_page_p, DONT_FREE,
			list_id_p->tfile_vfid);
  return NO_ERROR;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * qfile_get_first_page () -
 *   return: int (NO_ERROR or ER_FAILED)
 *   list_id(in): List File Identifier
 */
int
qfile_get_first_page (THREAD_ENTRY * thread_p, QFILE_LIST_ID * list_id_p)
{
  PAGE_PTR new_page_p;
  VPID new_vpid;

  if (list_id_p->tfile_vfid == NULL)
    {
      list_id_p->tfile_vfid = qmgr_create_new_temp_file (thread_p,
							 list_id_p->query_id,
							 TEMP_FILE_MEMBUF_NORMAL);
      if (list_id_p->tfile_vfid == NULL)
	{
	  return ER_FAILED;
	}
    }

  new_page_p = qmgr_get_new_page (thread_p, &new_vpid, list_id_p->tfile_vfid);
  if (new_page_p == NULL)
    {
      return ER_FAILED;
    }

  list_id_p->page_cnt++;

  QFILE_PUT_TUPLE_COUNT (new_page_p, 0);
  QFILE_PUT_PREV_VPID (new_page_p, &list_id_p->last_vpid);
  LS_PUT_NEXT_VPID_NULL_ASYNC (new_page_p);

  QFILE_COPY_VPID (&list_id_p->first_vpid, &new_vpid);
  QFILE_COPY_VPID (&list_id_p->last_vpid, &new_vpid);

  list_id_p->last_pgptr = new_page_p;
  list_id_p->last_offset = QFILE_PAGE_HEADER_SIZE;
  QFILE_PUT_OVERFLOW_VPID_NULL (new_page_p);

  list_id_p->lasttpl_len = 0;
  list_id_p->last_offset += ((0 + list_id_p->last_offset > DB_PAGESIZE)
			     ? DB_PAGESIZE : 0);

  qfile_set_dirty_page (thread_p, new_page_p, DONT_FREE,
			list_id_p->tfile_vfid);
  return NO_ERROR;
}
#endif

/*
 * qfile_destroy_list () -
 *   return: int
 *   list_id(in): List File Identifier
 *
 * Note: All the pages of the list file are deallocated from the query
 *              file, the memory areas for the list file identifier are freed
 *              and the number of pages deallocated is returned. This routine
 *              is basically called for temporarily created list files.
 */
void
qfile_destroy_list (THREAD_ENTRY * thread_p, QFILE_LIST_ID * list_id_p)
{
  if (list_id_p)
    {
      if (list_id_p->tfile_vfid)
	{
	  qmgr_free_list_temp_file (thread_p, list_id_p->query_id,
				    list_id_p->tfile_vfid);
	}
      else
	{
	  /* because qmgr_free_list_temp_file() destroy only FILE_TMP file */
	  if (!VFID_ISNULL (&list_id_p->temp_vfid))
	    {
	      file_destroy (thread_p, &list_id_p->temp_vfid);
	    }
	}

      qfile_clear_list_id (list_id_p);
    }
}

#if defined (ENABLE_UNUSED_FUNCTION)
#if defined(SERVER_MODE)
/*
 * qfile_get_list_page_with_waiting
 *   return:
 *   arg(in):
 */
static int
qfile_get_list_page_with_waiting (THREAD_ENTRY * thread_p,
				  QMGR_WAIT_ARGS * arg_p)
{
  VPID next_vpid;
  PAGE_PTR page_p;

  /* check error */
  if (qmgr_get_query_error_with_id (thread_p, arg_p->query_id) < 0)
    {
      return true;
    }

  page_p = qmgr_get_old_page (thread_p, &arg_p->vpid, arg_p->tfile_vfidp);
  if (page_p == NULL)
    {
      return true;
    }
  QFILE_GET_NEXT_VPID (&next_vpid, page_p);
  QFILE_COPY_VPID (&(arg_p->next_vpid), &next_vpid);
  qmgr_free_old_page_and_init (thread_p, page_p, arg_p->tfile_vfidp);

  /* if the next page is not ready yet, return 0 */
  return (next_vpid.pageid != NULL_PAGEID_ASYNC);
}
#endif /* SERVER_MODE */
#endif

/*
 * xqfile_get_list_file_page () -
 *   return: NO_ERROR or ER_ code
 *   query_id(in):
 *   volid(in): List file page volume identifier
 *   pageid(in): List file page identifier
 *   page_bufp(out): Buffer to contain list file page content
 *   page_sizep(out):
 *
 * Note: This routine is basically called by the C/S communication
 *              routines to fetch and copy the indicated list file page to
 *              the buffer area. The area pointed by the buffer must have
 *              been allocated by the caller and should be big enough to
 *              store a list file page.
 */
int
xqfile_get_list_file_page (THREAD_ENTRY * thread_p, QUERY_ID query_id,
			   VOLID vol_id, PAGEID page_id, char *page_buf_p,
			   int *page_size_p)
{
  QMGR_QUERY_ENTRY *query_entry_p = NULL;
  QFILE_LIST_ID *list_id_p;
  QMGR_TEMP_FILE *tfile_vfid_p;
  VPID vpid, next_vpid;
  PAGE_PTR page_p;
  int one_page_size = DB_PAGESIZE;
  int tran_index;
#if defined(SERVER_MODE)
  int error = NO_ERROR;
  int rv;
#endif /* SERVER_MODE */

  assert (page_buf_p != NULL);

  *page_size_p = 0;

  VPID_SET (&vpid, vol_id, page_id);
  tran_index = logtb_get_current_tran_index (thread_p);

  if (query_id == NULL_QUERY_ID)
    {
      assert (false);
      return ER_QPROC_UNKNOWN_QUERYID;
    }
  else if (query_id >= SHRT_MAX)
    {
      tfile_vfid_p = (QMGR_TEMP_FILE *) query_id;
      goto get_page;
    }
  else
    {
      query_entry_p = qmgr_get_query_entry (thread_p, query_id, tran_index);
      if (query_entry_p == NULL)
	{
	  return ER_QPROC_UNKNOWN_QUERYID;
	}
    }

#if defined(SERVER_MODE)
  error = qmgr_get_query_error_with_entry (query_entry_p);
  if (error < 0)
    {
      if (error == ER_LK_UNILATERALLY_ABORTED)
	{
	  tran_server_unilaterally_abort_tran (thread_p);
	}
      return error;
    }

  if (query_entry_p->list_id == NULL || page_id == NULL_PAGEID_ASYNC)
    {
      int sleep_msec = 0;

      /*
       * async query does not yet make the output list file;
       * wait for the async query to end and to make the output list file
       */
      /* TODO: replace fixed-constants */
      while (query_entry_p->query_mode != QUERY_COMPLETED
	     || query_entry_p->list_id == NULL)
	{
	  sleep_msec += 5;

	  if (sleep_msec >= 2000)
	    {
	      sleep_msec = 5;
	    }
	  thread_sleep (sleep_msec);

	  query_entry_p = qmgr_get_query_entry (thread_p, query_id,
						tran_index);
	  if (query_entry_p == NULL)
	    {
	      /*
	       * if async query got an error or interrupted and its query
	       * entry has been cleaned, then 'query_entryp == NULL'
	       */
	      return ER_QPROC_UNKNOWN_QUERYID;
	    }
	  if (query_entry_p->query_mode == QUERY_COMPLETED)
	    {
	      break;
	    }
	}

      error = qmgr_get_query_error_with_entry (query_entry_p);
      if (error < 0)
	{
	  if (error == ER_LK_UNILATERALLY_ABORTED)
	    {
	      tran_server_unilaterally_abort_tran (thread_p);
	    }
	  return error;
	}

      if (query_entry_p->list_id == NULL)
	{
	  *page_size_p = 0;
	  return NO_ERROR;
	}

      vol_id = query_entry_p->list_id->first_vpid.volid;
      page_id = query_entry_p->list_id->first_vpid.pageid;

      /* no result case */
      if (vol_id == NULL_VOLID && page_id == NULL_PAGEID)
	{
	  *page_size_p = 0;
	  return NO_ERROR;
	}

      VPID_SET (&vpid, vol_id, page_id);
    }

  rv = pthread_mutex_lock (&query_entry_p->lock);
  list_id_p = query_entry_p->list_id;
  tfile_vfid_p = list_id_p->tfile_vfid;
  pthread_mutex_unlock (&query_entry_p->lock);

  rv = pthread_mutex_lock (&query_entry_p->lock);
  list_id_p = query_entry_p->list_id;
  tfile_vfid_p = (list_id_p) ? list_id_p->tfile_vfid : NULL;
  pthread_mutex_unlock (&query_entry_p->lock);
#else /* not SERVER_MODE */
  list_id_p = query_entry_p->list_id;
  tfile_vfid_p = list_id_p->tfile_vfid;
#endif /* not SERVER_MODE */

get_page:
  /* append pages until a network page is full */
  while ((*page_size_p + DB_PAGESIZE) <= IO_MAX_PAGE_SIZE)
    {
      page_p = qmgr_get_old_page (thread_p, &vpid, tfile_vfid_p);
      if (page_p == NULL)
	{
	  return er_errid ();
	}

      /* find next page to append */
      QFILE_GET_OVERFLOW_VPID (&next_vpid, page_p);
      if (next_vpid.pageid == NULL_PAGEID)
	{
	  QFILE_GET_NEXT_VPID (&next_vpid, page_p);
	}

      /* current page is not ready yet, so stop appending */
      if (next_vpid.pageid == NULL_PAGEID_ASYNC)
	{
	  qmgr_free_old_page_and_init (thread_p, page_p, tfile_vfid_p);
	  break;
	}
      if (QFILE_GET_TUPLE_COUNT (page_p) == QFILE_OVERFLOW_TUPLE_COUNT_FLAG
	  || QFILE_GET_OVERFLOW_PAGE_ID (page_p) != NULL_PAGEID)
	{
	  one_page_size = DB_PAGESIZE;
	}
      else
	{
	  one_page_size = QFILE_GET_LAST_TUPLE_OFFSET (page_p)
	    + QFILE_GET_TUPLE_LENGTH (page_p +
				      QFILE_GET_LAST_TUPLE_OFFSET (page_p));
	  if (one_page_size < QFILE_PAGE_HEADER_SIZE)
	    {
	      one_page_size = QFILE_PAGE_HEADER_SIZE;
	    }
	  if (one_page_size > DB_PAGESIZE)
	    {
	      one_page_size = DB_PAGESIZE;
	    }
	}

      memcpy ((page_buf_p + *page_size_p), page_p, one_page_size);
      qmgr_free_old_page_and_init (thread_p, page_p, tfile_vfid_p);

      *page_size_p += DB_PAGESIZE;

      /* next page to append does not exists, stop appending */
      if (next_vpid.pageid == NULL_PAGEID)
	{
	  break;
	}

      vpid = next_vpid;
    }

  *page_size_p += one_page_size - DB_PAGESIZE;

  return NO_ERROR;
}

/*
 * qfile_add_item_to_list () -
 *   return: int (NO_ERROR or ER_FAILED)
 *   item(in): Value in disk representation form
 *   item_size(in): Size of the value
 *   list_id(in): List File Identifier
 *
 * Note: The given item is added to the end of the given list file.
 *       The list file must be of a single column.
 */
int
qfile_add_item_to_list (THREAD_ENTRY * thread_p, char *item_p, int item_size,
			QFILE_LIST_ID * list_id_p)
{
  QFILE_TUPLE tuple;
  int tuple_length, align;
  char *tuple_p;

  tuple_length =
    QFILE_TUPLE_LENGTH_SIZE + QFILE_TUPLE_VALUE_HEADER_SIZE + item_size;

  align = DB_ALIGN (item_size, MAX_ALIGNMENT) - item_size;
  tuple_length += align;

  if (tuple_length < QFILE_MAX_TUPLE_SIZE_IN_PAGE)
    {
      /* SMALL_TUPLE */

      list_id_p->tpl_descr.item = item_p;
      list_id_p->tpl_descr.item_size = item_size;
      list_id_p->tpl_descr.tpl_size = tuple_length;

      if (qfile_generate_tuple_into_list (thread_p, list_id_p,
					  T_SINGLE_BOUND_ITEM) != NO_ERROR)
	{
	  return ER_FAILED;
	}
    }
  else
    {
      /* BIG_TUPLE */

      tuple = (QFILE_TUPLE) malloc (tuple_length);
      if (tuple == NULL)
	{
	  return ER_FAILED;
	}

      QFILE_PUT_TUPLE_LENGTH (tuple, tuple_length);
      tuple_p = (char *) tuple + QFILE_TUPLE_LENGTH_SIZE;
      QFILE_PUT_TUPLE_VALUE_FLAG (tuple_p, V_BOUND);
      QFILE_PUT_TUPLE_VALUE_LENGTH (tuple_p, item_size + align);
      tuple_p += QFILE_TUPLE_VALUE_HEADER_SIZE;
      memcpy (tuple_p, item_p, item_size);
#if !defined(NDEBUG)
      /* suppress valgrind UMW error */
      memset (tuple_p + item_size, 0, align);
#endif

      if (qfile_add_tuple_to_list (thread_p, list_id_p, tuple) != NO_ERROR)
	{
	  free_and_init (tuple);
	  return ER_FAILED;
	}

      free_and_init (tuple);
    }

  return NO_ERROR;
}

/*
 * qfile_compare_tuple_helper () -
 *   return:
 *   lhs(in):
 *   rhs(in):
 *   types(in):
 */
static int
qfile_compare_tuple_helper (QFILE_TUPLE lhs, QFILE_TUPLE rhs,
			    QFILE_TUPLE_VALUE_TYPE_LIST * types, int *cmp)
{
  char *lhs_tuple_p, *rhs_tuple_p;
  int i, result;

  lhs_tuple_p = (char *) lhs + QFILE_TUPLE_LENGTH_SIZE;
  rhs_tuple_p = (char *) rhs + QFILE_TUPLE_LENGTH_SIZE;

  for (i = 0; i < types->type_cnt; i++)
    {
      result = qfile_compare_tuple_values (lhs_tuple_p, rhs_tuple_p,
					   types->domp[i], cmp);
      if (result != NO_ERROR)
	{
	  return result;
	}

      if (*cmp != 0)
	{
	  return NO_ERROR;
	}

      lhs_tuple_p +=
	QFILE_TUPLE_VALUE_HEADER_SIZE +
	QFILE_GET_TUPLE_VALUE_LENGTH (lhs_tuple_p);
      rhs_tuple_p +=
	QFILE_TUPLE_VALUE_HEADER_SIZE +
	QFILE_GET_TUPLE_VALUE_LENGTH (rhs_tuple_p);
    }

  *cmp = 0;
  return NO_ERROR;
}

/*
 * qfile_advance_single () -
 *   return:
 *   next_scan(in):
 *   next_tpl(in):
 *   last_scan(in):
 *   last_tpl(in):
 *   types(in):
 */
static SCAN_CODE
qfile_advance_single (THREAD_ENTRY * thread_p,
		      QFILE_LIST_SCAN_ID * next_scan_p,
		      QFILE_TUPLE_RECORD * next_tuple_p,
		      UNUSED_ARG QFILE_LIST_SCAN_ID * last_scan_p,
		      UNUSED_ARG QFILE_TUPLE_RECORD * last_tuple_p,
		      UNUSED_ARG QFILE_TUPLE_VALUE_TYPE_LIST * types)
{
  if (next_scan_p == NULL)
    {
      return S_END;
    }

  return qfile_scan_list_next (thread_p, next_scan_p, next_tuple_p, PEEK);
}

/*
 * qfile_advance_group () -
 *   return:
 *   next_scan(in/out):
 *   next_tpl(out):
 *   last_scan(in/out):
 *   last_tpl(out):
 *   types(in):
 */
static SCAN_CODE
qfile_advance_group (THREAD_ENTRY * thread_p,
		     QFILE_LIST_SCAN_ID * next_scan_p,
		     QFILE_TUPLE_RECORD * next_tuple_p,
		     QFILE_LIST_SCAN_ID * last_scan_p,
		     QFILE_TUPLE_RECORD * last_tuple_p,
		     QFILE_TUPLE_VALUE_TYPE_LIST * types)
{
  SCAN_CODE status;
  int error_code, cmp;

  if (next_scan_p == NULL)
    {
      return S_END;
    }

  status = S_SUCCESS;

  switch (last_scan_p->position)
    {
    case S_BEFORE:
      status = qfile_scan_list_next (thread_p, next_scan_p, next_tuple_p,
				     PEEK);
      break;

    case S_ON:
      do
	{
	  status = qfile_scan_list_next (thread_p, next_scan_p, next_tuple_p,
					 PEEK);
	  if (status != S_SUCCESS)
	    {
	      break;
	    }

	  error_code = qfile_compare_tuple_helper (last_tuple_p->tpl,
						   next_tuple_p->tpl,
						   types, &cmp);
	}
      while (error_code == NO_ERROR && cmp == 0);
      break;

    case S_AFTER:
    default:
      status = S_END;
      break;
    }

  if (status == S_SUCCESS)
    {
      QFILE_TUPLE_POSITION next_pos;

      qfile_save_current_scan_tuple_position (next_scan_p, &next_pos);
      status = qfile_jump_scan_tuple_position (thread_p, last_scan_p,
					       &next_pos, last_tuple_p, PEEK);
    }

  return status;
}

/*
 * qfile_add_one_tuple () -
 *   return:
 *   dst(in/out):
 *   lhs(in):
 *   rhs(in):
 */
static int
qfile_add_one_tuple (THREAD_ENTRY * thread_p, QFILE_LIST_ID * dest_list_p,
		     QFILE_TUPLE lhs, UNUSED_ARG QFILE_TUPLE rhs)
{
  return qfile_add_tuple_to_list (thread_p, dest_list_p, lhs);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * qfile_add_two_tuple () -
 *   return:
 *   dst(in):
 *   lhs(in):
 *   rhs(in):
 */
static int
qfile_add_two_tuple (THREAD_ENTRY * thread_p, QFILE_LIST_ID * dest_list_p,
		     QFILE_TUPLE lhs, QFILE_TUPLE rhs)
{
  int error;

  error = qfile_add_tuple_to_list (thread_p, dest_list_p, lhs);
  if (error == NO_ERROR)
    {
      error = qfile_add_tuple_to_list (thread_p, dest_list_p, rhs);
    }

  return error;
}
#endif

static int
qfile_advance (THREAD_ENTRY * thread_p, ADVANCE_FUCTION advance_func,
	       QFILE_TUPLE_RECORD * side_p, QFILE_TUPLE_RECORD * last_side_p,
	       QFILE_LIST_SCAN_ID * scan_p, QFILE_LIST_SCAN_ID * last_scan_p,
	       QFILE_LIST_ID * side_file_p, int *have_side_p)
{
  SCAN_CODE scan_result;

  scan_result = (*advance_func) (thread_p, scan_p, side_p, last_scan_p,
				 last_side_p, &side_file_p->type_list);
  switch (scan_result)
    {
    case S_SUCCESS:
      *have_side_p = 1;
      break;
    case S_END:
      *have_side_p = 0;
      break;
    default:
      return ER_FAILED;
    }

  return NO_ERROR;
}

/*
 * qfile_combine_two_list () -
 *   return: QFILE_LIST_ID *, or NULL
 *   lhs_file(in): pointer to a QFILE_LIST_ID for one of the input files
 *   rhs_file(in): pointer to a QFILE_LIST_ID for the other input file, or NULL
 *   flag(in): {QFILE_FLAG_UNION, QFILE_FLAG_DIFFERENCE, QFILE_FLAG_INTERSECT,
 *             QFILE_FLAG_ALL, QFILE_FLAG_DISTINCT}
 *             the kind of combination desired (union, diff, or intersect) and
 *             whether to do 'all' or 'distinct'
 *
 */
QFILE_LIST_ID *
qfile_combine_two_list (THREAD_ENTRY * thread_p, QFILE_LIST_ID * lhs_file_p,
			QFILE_LIST_ID * rhs_file_p, int flag)
{
  QFILE_LIST_ID *dest_list_id_p = NULL;
  QFILE_LIST_SCAN_ID lhs_scan_id, rhs_scan_id, last_lhs_scan_id,
    last_rhs_scan_id;
  QFILE_LIST_SCAN_ID *lhs_scan_p = NULL, *rhs_scan_p = NULL;
  QFILE_LIST_SCAN_ID *last_lhs_scan_p = NULL, *last_rhs_scan_p = NULL;
  int have_lhs = 0, have_rhs = 0, cmp;
  QFILE_TUPLE_RECORD lhs = { NULL, 0 };
  QFILE_TUPLE_RECORD rhs = { NULL, 0 };
  QFILE_TUPLE_RECORD last_lhs = { NULL, 0 };
  QFILE_TUPLE_RECORD last_rhs = { NULL, 0 };
  QUERY_OPTIONS distinct_or_all;

  ADVANCE_FUCTION advance_func;
  int (*act_left_func) (THREAD_ENTRY * thread_p, QFILE_LIST_ID *,
			QFILE_TUPLE);
  int (*act_right_func) (THREAD_ENTRY * thread_p, QFILE_LIST_ID *,
			 QFILE_TUPLE);
  int (*act_both_func) (THREAD_ENTRY * thread_p, QFILE_LIST_ID *, QFILE_TUPLE,
			QFILE_TUPLE);

  advance_func = NULL;
  act_left_func = NULL;
  act_right_func = NULL;
  act_both_func = NULL;

  if (QFILE_IS_FLAG_SET_BOTH (flag, QFILE_FLAG_UNION, QFILE_FLAG_ALL))
    {
      return qfile_union_list (thread_p, lhs_file_p, rhs_file_p, flag);
    }

  if (QFILE_IS_FLAG_SET (flag, QFILE_FLAG_DISTINCT))
    {
      distinct_or_all = Q_DISTINCT;
    }
  else
    {
      distinct_or_all = Q_ALL;
    }

  lhs_file_p =
    qfile_sort_list (thread_p, lhs_file_p, NULL, distinct_or_all, true);
  if (lhs_file_p == NULL)
    {
      goto error;
    }

  if (qfile_open_list_scan (lhs_file_p, &lhs_scan_id) != NO_ERROR)
    {
      goto error;
    }
  lhs_scan_p = &lhs_scan_id;

  if (rhs_file_p)
    {
      rhs_file_p = qfile_sort_list (thread_p, rhs_file_p, NULL,
				    distinct_or_all, true);
      if (rhs_file_p == NULL)
	{
	  goto error;
	}

      if (qfile_open_list_scan (rhs_file_p, &rhs_scan_id) != NO_ERROR)
	{
	  goto error;
	}

      rhs_scan_p = &rhs_scan_id;
    }

  dest_list_id_p = qfile_open_list (thread_p, &lhs_file_p->type_list, NULL,
				    lhs_file_p->query_id, flag);
  if (dest_list_id_p == NULL)
    {
      goto error;
    }

  if (rhs_file_p
      && qfile_unify_types (dest_list_id_p, rhs_file_p) != NO_ERROR)
    {
      goto error;
    }

  if (QFILE_IS_FLAG_SET (flag, QFILE_FLAG_INTERSECT))
    {
      act_left_func = NULL;
      act_right_func = NULL;
      act_both_func = qfile_add_one_tuple;
    }
  else if (QFILE_IS_FLAG_SET (flag, QFILE_FLAG_DIFFERENCE))
    {
      act_left_func = qfile_add_tuple_to_list;
      act_right_func = NULL;
      act_both_func = NULL;
    }
  else
    {
      /* QFILE_FLAG_UNION */
      act_left_func = qfile_add_tuple_to_list;
      act_right_func = qfile_add_tuple_to_list;
      act_both_func = qfile_add_one_tuple;
    }

  if (QFILE_IS_FLAG_SET (flag, QFILE_FLAG_DISTINCT))
    {
      advance_func = qfile_advance_group;
      if (qfile_open_list_scan (lhs_file_p, &last_lhs_scan_id) != NO_ERROR)
	{
	  goto error;
	}

      last_lhs_scan_p = &last_lhs_scan_id;
      if (rhs_file_p)
	{
	  if (qfile_open_list_scan (rhs_file_p, &last_rhs_scan_id) !=
	      NO_ERROR)
	    {
	      goto error;
	    }
	  last_rhs_scan_p = &last_rhs_scan_id;
	}
    }
  else
    {
      /* QFILE_FLAG_ALL */
      advance_func = qfile_advance_single;
      assert (!QFILE_IS_FLAG_SET (flag, QFILE_FLAG_UNION));
    }

  while (1)
    {
      if (!have_lhs)
	{
	  if (qfile_advance (thread_p, advance_func, &lhs, &last_lhs,
			     lhs_scan_p, last_lhs_scan_p, lhs_file_p,
			     &have_lhs) != NO_ERROR)
	    {
	      goto error;
	    }
	}

      if (!have_rhs)
	{
	  if (qfile_advance (thread_p, advance_func, &rhs, &last_rhs,
			     rhs_scan_p, last_rhs_scan_p, rhs_file_p,
			     &have_rhs) != NO_ERROR)
	    {
	      goto error;
	    }
	}

      if (!have_lhs || !have_rhs)
	{
	  break;
	}

      if (qfile_compare_tuple_helper (lhs.tpl, rhs.tpl,
				      &lhs_file_p->type_list,
				      &cmp) != NO_ERROR)
	{
	  goto error;
	}

      if (cmp < 0)
	{
	  if (act_left_func
	      && act_left_func (thread_p, dest_list_id_p,
				lhs.tpl) != NO_ERROR)
	    {
	      goto error;
	    }

	  have_lhs = 0;
	}
      else if (cmp == 0)
	{
	  if (act_both_func
	      && act_both_func (thread_p, dest_list_id_p, lhs.tpl,
				rhs.tpl) != NO_ERROR)
	    {
	      goto error;
	    }

	  have_lhs = 0;
	  have_rhs = 0;
	}
      else
	{
	  if (act_right_func
	      && act_right_func (thread_p, dest_list_id_p,
				 rhs.tpl) != NO_ERROR)
	    {
	      goto error;
	    }
	  have_rhs = 0;
	}
    }

  while (have_lhs)
    {
      if (act_left_func
	  && act_left_func (thread_p, dest_list_id_p, lhs.tpl) != NO_ERROR)
	{
	  goto error;
	}

      if (qfile_advance (thread_p, advance_func, &lhs, &last_lhs, lhs_scan_p,
			 last_lhs_scan_p, lhs_file_p, &have_lhs) != NO_ERROR)
	{
	  goto error;
	}
    }

  while (have_rhs)
    {
      if (act_right_func
	  && act_right_func (thread_p, dest_list_id_p, rhs.tpl) != NO_ERROR)
	{
	  goto error;
	}

      if (qfile_advance (thread_p, advance_func, &rhs, &last_rhs, rhs_scan_p,
			 last_rhs_scan_p, rhs_file_p, &have_rhs) != NO_ERROR)
	{
	  goto error;
	}
    }

success:
  if (lhs_scan_p)
    {
      qfile_close_scan (thread_p, lhs_scan_p);
    }
  if (rhs_scan_p)
    {
      qfile_close_scan (thread_p, rhs_scan_p);
    }
  if (last_lhs_scan_p)
    {
      qfile_close_scan (thread_p, last_lhs_scan_p);
    }
  if (last_rhs_scan_p)
    {
      qfile_close_scan (thread_p, last_rhs_scan_p);
    }
  if (lhs_file_p)
    {
      qfile_close_list (thread_p, lhs_file_p);
    }
  if (rhs_file_p)
    {
      qfile_close_list (thread_p, rhs_file_p);
    }
  if (dest_list_id_p)
    {
      qfile_close_list (thread_p, dest_list_id_p);
    }

  return dest_list_id_p;

error:
  if (dest_list_id_p)
    {
      qfile_close_and_free_list_file (thread_p, dest_list_id_p);
      dest_list_id_p = NULL;
    }
  goto success;
}

static int
qfile_copy_tuple (THREAD_ENTRY * thread_p, QFILE_LIST_ID * to_list_id_p,
		  QFILE_LIST_ID * from_list_id_p)
{
  QFILE_LIST_SCAN_ID scan_id;
  QFILE_TUPLE_RECORD tuple_record = { NULL, 0 };
  SCAN_CODE qp_scan;

  /* scan through the first list file and add the tuples to the result
   * list file.
   */
  if (qfile_open_list_scan (from_list_id_p, &scan_id) != NO_ERROR)
    {
      return ER_FAILED;
    }

  while (true)
    {
      qp_scan = qfile_scan_list_next (thread_p, &scan_id, &tuple_record,
				      PEEK);
      if (qp_scan != S_SUCCESS)
	{
	  break;
	}

      if (qfile_add_tuple_to_list (thread_p, to_list_id_p, tuple_record.tpl)
	  != NO_ERROR)
	{
	  qfile_close_scan (thread_p, &scan_id);
	  return ER_FAILED;
	}
    }

  qfile_close_scan (thread_p, &scan_id);

  if (qp_scan != S_END)
    {
      return ER_FAILED;
    }

  return NO_ERROR;
}

static void
qfile_close_and_free_list_file (THREAD_ENTRY * thread_p,
				QFILE_LIST_ID * list_id)
{
  qfile_close_list (thread_p, list_id);
  QFILE_FREE_AND_INIT_LIST_ID (list_id);
}

/*
 * qfile_union_list () -
 * 	 return: IST_ID *, or NULL
 *   list_id1(in): First list file identifier
 * 	 list_id2(in): Second list file identifier
 * 	 flag(in):
 *
 * Note: This routine takes the union of two list files by getting the
 *              tuples of the both list files and generates a new list file.
 *              The source list files are not affected.
 */
static QFILE_LIST_ID *
qfile_union_list (THREAD_ENTRY * thread_p, QFILE_LIST_ID * list_id1_p,
		  QFILE_LIST_ID * list_id2_p, UNUSED_ARG int flag)
{
  QFILE_LIST_ID *result_list_id_p, *base, *tail;

  base = list_id1_p;
  tail = list_id2_p;

  result_list_id_p = qfile_clone_list_id (base, false);
  if (result_list_id_p == NULL)
    {
      return NULL;
    }

  if (tail != NULL)
    {
      if (qfile_reopen_list_as_append_mode (thread_p,
					    result_list_id_p) != NO_ERROR)
	{
	  goto error;
	}

      if (qfile_unify_types (result_list_id_p, tail) != NO_ERROR)
	{
	  goto error;
	}

      if (qfile_copy_tuple (thread_p, result_list_id_p, tail) != NO_ERROR)
	{
	  goto error;
	}

      qfile_close_list (thread_p, result_list_id_p);
    }

  /* clear base list_id to prevent double free of tfile_vfid */
  qfile_clear_list_id (base);

  return result_list_id_p;

error:
  qfile_close_list (thread_p, result_list_id_p);
  qfile_free_list_id (result_list_id_p);
  return NULL;
}

/*
 * qfile_reallocate_tuple () - reallocates a tuple to the desired size.
 *              If it cant, it sets an error and returns 0
 *   return: nt 1 succes, 0 failure
 *   tplrec(in): tuple descriptor
 * 	 tpl_size(in): desired size
 * 	 file(in):
 *   line(in):
 *
 */
int
qfile_reallocate_tuple (QFILE_TUPLE_RECORD * tuple_record_p, int tuple_size)
{
  QFILE_TUPLE tuple;

  if (tuple_record_p->size == 0)
    {
      tuple_record_p->tpl = (QFILE_TUPLE) malloc (tuple_size);
    }
  else
    {
      /*
       * Don't leak the original tuple if we get a malloc failure!
       */
      tuple = (QFILE_TUPLE) realloc (tuple_record_p->tpl, tuple_size);
      if (tuple == NULL)
	{
	  free_and_init (tuple_record_p->tpl);
	}
      tuple_record_p->tpl = tuple;
    }

  if (tuple_record_p->tpl == NULL)
    {
      return ER_FAILED;
    }

  tuple_record_p->size = tuple_size;

  return NO_ERROR;
}

#if defined (RYE_DEBUG)
/*
 * qfile_print_list () - Dump the content of the list file to the standard output
 *   return: none
 *   list_id(in): List File Identifier
 */
void
qfile_print_list (THREAD_ENTRY * thread_p, QFILE_LIST_ID * list_id_p)
{
  QFILE_LIST_SCAN_ID scan_id;
  QFILE_TUPLE_RECORD tuple_record = { NULL, 0 };

  if (!list_id_p || list_id_p->type_list.type_cnt < 0)
    {
      fprintf (stdout, "\n <invalid tuple list> ");
      return;
    }
  if (list_id_p->type_list.type_cnt == 0)
    {
      fprintf (stdout, "\n <empty tuple list> ");
      return;
    }

  if (qfile_open_list_scan (list_id_p, &scan_id) != NO_ERROR)
    {
      return;
    }

  while (qfile_scan_list_next (thread_p, &scan_id, &tuple_record, PEEK) ==
	 S_SUCCESS)
    {
      qfile_print_tuple (&list_id_p->type_list, tuple_record.tpl);
    }

  qfile_close_scan (thread_p, &scan_id);
}
#endif

/*
 * Sorting Related Routines
 */

/* qfile_make_sort_key () -
 *   return:
 *   info(in):
 *   key(in):
 *   input_scan(in):
 *   tplrec(in):
 */
SORT_STATUS
qfile_make_sort_key (THREAD_ENTRY * thread_p, SORTKEY_INFO * key_info_p,
		     RECDES * key_record_p, QFILE_LIST_SCAN_ID * input_scan_p,
		     QFILE_TUPLE_RECORD * tuple_record_p)
{
  int i, nkeys, length;
  SORT_REC *sort_record_p;
  char *data;
  SCAN_CODE scan_status;
  char *field_data;
  int field_length, offset;
  SORT_STATUS status;

  scan_status =
    qfile_scan_list_next (thread_p, input_scan_p, tuple_record_p, PEEK);
  if (scan_status != S_SUCCESS)
    {
      return ((scan_status == S_END)
	      ? SORT_NOMORE_RECS : SORT_ERROR_OCCURRED);
    }

  nkeys = key_info_p->nkeys;
  sort_record_p = (SORT_REC *) key_record_p->data;
  sort_record_p->next = NULL;

  if (key_info_p->use_original)
    {
      /* P_sort_key */

      /* get sort_key body start position, align data to 8 bytes boundary */
      data = &(sort_record_p->s.original.body[0]);
      data = PTR_ALIGN (data, MAX_ALIGNMENT);

      length = CAST_BUFLEN (data - key_record_p->data);	/* i.e, 12 */

      /* STEP 1: build header(tuple_ID) */
      if (length <= key_record_p->area_size)
	{
	  sort_record_p->s.original.pageid = input_scan_p->curr_vpid.pageid;
	  sort_record_p->s.original.volid = input_scan_p->curr_vpid.volid;
	  sort_record_p->s.original.offset = input_scan_p->curr_offset;
	}

      /* STEP 2: build body */
      for (i = 0; i < nkeys; i++)
	{
	  /* Position ourselves at the next field, and find out its length */
	  QFILE_GET_TUPLE_VALUE_HEADER_POSITION (tuple_record_p->tpl,
						 key_info_p->key[i].col,
						 field_data);
	  field_length =
	    ((QFILE_GET_TUPLE_VALUE_FLAG (field_data) ==
	      V_BOUND) ? QFILE_GET_TUPLE_VALUE_LENGTH (field_data) : 0);

	  length += QFILE_TUPLE_VALUE_HEADER_SIZE + field_length;

	  if (length <= key_record_p->area_size)
	    {
	      memcpy (data, field_data,
		      QFILE_TUPLE_VALUE_HEADER_SIZE + field_length);
	    }

	  /*
	   * Always pretend that we copied the data, even if we didn't.
	   * That will allow us to find out how big the record really needs
	   * to be.
	   */
	  data += QFILE_TUPLE_VALUE_HEADER_SIZE + field_length;
	}
    }
  else
    {
      /* A_sort_key */

      /* get sort_key body start position, align data to 8 bytes boundary */
      data = (char *) &sort_record_p->s.offset[nkeys];
      data = PTR_ALIGN (data, MAX_ALIGNMENT);

      length = CAST_BUFLEN (data - key_record_p->data);	/* i.e, 4 + 4 * (n - 1) */

      /* STEP 1: build header(offset_MAP) - go on with STEP 2 */

      /* STEP 2: build body */

      for (i = 0; i < nkeys; i++)
	{
	  /* Position ourselves at the next field, and find out its length */
	  QFILE_GET_TUPLE_VALUE_HEADER_POSITION (tuple_record_p->tpl,
						 key_info_p->key[i].col,
						 field_data);
	  field_length =
	    ((QFILE_GET_TUPLE_VALUE_FLAG (field_data) ==
	      V_BOUND) ? QFILE_GET_TUPLE_VALUE_LENGTH (field_data) : 0);

	  if (field_length)
	    {
	      /* non-NULL value */

	      offset = CAST_BUFLEN (data - key_record_p->data
				    + QFILE_TUPLE_VALUE_HEADER_SIZE);
	      length = offset + field_length;

	      if (length <= key_record_p->area_size)
		{
		  sort_record_p->s.offset[i] = offset;
		  memcpy (data, field_data,
			  QFILE_TUPLE_VALUE_HEADER_SIZE + field_length);
		}
	      /*
	       * Always pretend that we copied the data, even if we didn't.
	       * That will allow us to find out how big the record really
	       * needs to be.
	       */
	      data += QFILE_TUPLE_VALUE_HEADER_SIZE + field_length;
	    }
	  else
	    {
	      /* do not copy NULL-value field */

	      if (length <= key_record_p->area_size)
		{
		  sort_record_p->s.offset[i] = 0;
		}
	    }
	}
    }

  key_record_p->length = CAST_BUFLEN (data - key_record_p->data);

  if (key_record_p->length <= key_record_p->area_size)
    {
      status = SORT_SUCCESS;
    }
  else
    {
      scan_status = qfile_scan_prev (thread_p, input_scan_p);
      status = ((scan_status == S_ERROR)
		? SORT_ERROR_OCCURRED : SORT_REC_DOESNT_FIT);
    }

  return status;
}

/* qfile_generate_sort_tuple () -
 *   return:
 *   info(in):
 *   sort_rec(in):
 *   output_recdes(out):
 */
QFILE_TUPLE
qfile_generate_sort_tuple (SORTKEY_INFO * key_info_p,
			   SORT_REC * sort_record_p, RECDES * output_recdes_p)
{
  int nkeys, size, i;
  char *tuple_p, *field_p;
  char *p;
  int c;
  char *src;
  int len;

  nkeys = key_info_p->nkeys;
  size = QFILE_TUPLE_LENGTH_SIZE;

  for (i = 0; i < nkeys; i++)
    {
      size += QFILE_TUPLE_VALUE_HEADER_SIZE;
      if (sort_record_p->s.offset[i] != 0)
	{
	  p =
	    (char *) sort_record_p + sort_record_p->s.offset[i] -
	    QFILE_TUPLE_VALUE_HEADER_SIZE;
	  size += QFILE_GET_TUPLE_VALUE_LENGTH (p);
	}
    }

  if (output_recdes_p->area_size < size)
    {
      if (output_recdes_p->area_size == 0)
	{
	  tuple_p = (char *) malloc (size);
	}
      else
	{
	  tuple_p = (char *) realloc (output_recdes_p->data, size);
	}

      if (tuple_p == NULL)
	{
	  return NULL;
	}

      output_recdes_p->data = tuple_p;
      output_recdes_p->area_size = size;
    }

  tuple_p = output_recdes_p->data;
  field_p = tuple_p + QFILE_TUPLE_LENGTH_SIZE;

  for (i = 0; i < nkeys; i++)
    {
      c = key_info_p->key[i].permuted_col;

      if (sort_record_p->s.offset[c] == 0)
	{
	  QFILE_PUT_TUPLE_VALUE_FLAG (field_p, V_UNBOUND);
	  QFILE_PUT_TUPLE_VALUE_LENGTH (field_p, 0);
	}
      else
	{
	  src =
	    (char *) sort_record_p + sort_record_p->s.offset[c] -
	    QFILE_TUPLE_VALUE_HEADER_SIZE;
	  len = QFILE_GET_TUPLE_VALUE_LENGTH (src);
	  memcpy (field_p, src, len + QFILE_TUPLE_VALUE_HEADER_SIZE);
	  field_p += len;
	}

      field_p += QFILE_TUPLE_VALUE_HEADER_SIZE;
    }

  QFILE_PUT_TUPLE_LENGTH (tuple_p, field_p - tuple_p);
  return tuple_p;
}

/*
 * qfile_get_next_sort_item	() -
 *   return: SORT_STATUS
 *   recdes(in): Temporary record descriptor
 *   arg(in): Scan identifier
 *
 * Note: This routine is called by the sorting module to get the next
 * input item to sort. The scan identifier opened on the list file
 * is used to get the next list file tuple and return it to the sorting module
 * within the record descriptor. If the record descriptor area is not big
 * enough to hold the tuple, it is indicated to the sorting module and
 * scan position remains unchanged. The routine is supposed to be called again
 * with a bigger record descriptor. If there are no more tuples left
 * in the list file, or if an error occurs, the sorting module is informed with
 * necessary return codes.
 */
static SORT_STATUS
qfile_get_next_sort_item (THREAD_ENTRY * thread_p, RECDES * recdes_p,
			  void *arg)
{
  SORT_INFO *sort_info_p;
  QFILE_SORT_SCAN_ID *scan_id_p;

  sort_info_p = (SORT_INFO *) arg;
  scan_id_p = sort_info_p->s_id;

  return qfile_make_sort_key (thread_p, &sort_info_p->key_info,
			      recdes_p, scan_id_p->s_id, &scan_id_p->tplrec);
}

/*
 * qfile_put_next_sort_item () -
 *   return: SORT_STATUS
 *   recdes(in): Temporary record descriptor
 *   arg(in): Scan identifier
 *
 * Note: This routine is called by the sorting module to output	the next sorted
 * item.
 *
 * We have two versions of the put_next function:
 * ls_sort_put_next_long and ls_sort_put_next_short.  The long version
 * is building sort keys from records that hold more fields than sort keys.
 * It optimizes by simply keeping a pointer to the original record, and
 * when the sort keys are delivered back from the sort module, it uses
 * the pointer to retrieve the original record with all of the fields and
 * delivers that record to the output listfile.
 *
 * The short version is used when we we're sorting on all of the fields of
 * the record.  In that case there's no point in remembering the original record,
 * and we save the space occupied by the pointer. We also avoid the relatively
 * random traversal of the input file when rendering the output file,
 * since we can reconstruct the input records without actually consulting
 * the input file.
 */
static int
qfile_put_next_sort_item (THREAD_ENTRY * thread_p, const RECDES * recdes_p,
			  void *arg)
{
  SORT_INFO *sort_info_p;
  SORT_REC *key_p;
  int error;
  QFILE_TUPLE_DESCRIPTOR *tuple_descr_p;
  int nkeys, i;
  char *p;
  PAGE_PTR page_p;
  VPID vpid;
  QFILE_LIST_ID *list_id_p;
  char *data;

  error = NO_ERROR;
  sort_info_p = (SORT_INFO *) arg;

  for (key_p = (SORT_REC *) recdes_p->data; key_p && error == NO_ERROR;
       key_p = key_p->next)
    {
      if (sort_info_p->key_info.use_original)
	{
	  /* P_sort_key */

	  list_id_p = &(sort_info_p->s_id->s_id->list_id);

	  vpid.pageid = key_p->s.original.pageid;
	  vpid.volid = key_p->s.original.volid;

#if 0				/* SortCache */
	  /* check if page is already fixed */
	  if (VPID_EQ (&(sort_info_p->fixed_vpid), &vpid))
	    {
	      /* use cached page pointer */
	      page_p = sort_info_p->fixed_page;
	    }
	  else
	    {
	      /* free currently fixed page */
	      if (sort_info_p->fixed_page != NULL)
		{
		  qmgr_free_old_page_and_init (thread_p,
					       sort_info_p->fixed_page,
					       list_id_p->tfile_vfid);
		}

	      /* fix page and cache fixed vpid */
	      page_p =
		qmgr_get_old_page (thread_p, &vpid, list_id_p->tfile_vfid);
	      if (page_p == NULL)
		{
		  return er_errid ();
		}

	      /* cache page pointer */
	      sort_info_p->fixed_vpid = vpid;
	      sort_info_p->fixed_page = page_p;
	    }
#else /* not SortCache */
	  page_p = qmgr_get_old_page (thread_p, &vpid, list_id_p->tfile_vfid);
	  if (page_p == NULL)
	    {
	      return er_errid ();
	    }
#endif /* not SortCache */

	  QFILE_GET_OVERFLOW_VPID (&vpid, page_p);
	  if (vpid.pageid == NULL_PAGEID || vpid.pageid == NULL_PAGEID_ASYNC)
	    {
	      /*
	       * This is the normal case of a non-overflow tuple.  We can use
	       * the page image directly, since we know that the tuple resides
	       * entirely on that page.
	       */
	      data = page_p + key_p->s.original.offset;
	      error =
		qfile_add_tuple_to_list (thread_p, sort_info_p->output_file,
					 data);
	    }
	  else
	    {
	      /*
	       * Rats; this tuple requires overflow pages.  We need to copy
	       * all of the pages from the input file to the output file.
	       */
	      error =
		qfile_add_overflow_tuple_to_list (thread_p,
						  sort_info_p->output_file,
						  page_p, list_id_p);
	    }
#if 1				/* not SortCache */
	  qmgr_free_old_page_and_init (thread_p, page_p,
				       list_id_p->tfile_vfid);
#endif /* not SortCache */
	}
      else
	{
	  /* A_sort_key */

	  nkeys = sort_info_p->key_info.nkeys;	/* get sort_key field number */

	  /* generate tuple descriptor */
	  tuple_descr_p = &(sort_info_p->output_file->tpl_descr);

	  /* determine how big a tuple we'll need */
	  tuple_descr_p->tpl_size =
	    QFILE_TUPLE_LENGTH_SIZE + (QFILE_TUPLE_VALUE_HEADER_SIZE * nkeys);
	  for (i = 0; i < nkeys; i++)
	    {
	      if (key_p->s.offset[i] != 0)
		{
		  /*
		   * Remember, the offset[] value points to the start of the
		   * value's *data* (i.e., after the valflag/vallen nonsense),
		   * and is measured from the start of the sort_rec.
		   */
		  p =
		    (char *) key_p + key_p->s.offset[i] -
		    QFILE_TUPLE_VALUE_HEADER_SIZE;
		  tuple_descr_p->tpl_size += QFILE_GET_TUPLE_VALUE_LENGTH (p);
		}
	    }

	  if (tuple_descr_p->tpl_size < QFILE_MAX_TUPLE_SIZE_IN_PAGE)
	    {
	      /* SMALL QFILE_TUPLE */

	      /* set tuple descriptor */
	      tuple_descr_p->sortkey_info = (void *) (&sort_info_p->key_info);
	      tuple_descr_p->sort_rec = (void *) key_p;

	      /* generate sort_key driven tuple into list file page */
	      error =
		qfile_generate_tuple_into_list (thread_p,
						sort_info_p->output_file,
						T_SORTKEY);
	    }
	  else
	    {
	      /* BIG QFILE_TUPLE */

	      /*
	       * We didn't record the original vpid, and we should just
	       * reconstruct the original record from this sort key (rather
	       * than pressure the page buffer pool by reading in the original
	       * page to get the original tuple).
	       */
	      if (qfile_generate_sort_tuple (&sort_info_p->key_info,
					     key_p,
					     &sort_info_p->output_recdes) ==
		  NULL)
		{
		  error = ER_FAILED;
		}
	      else
		{
		  error =
		    qfile_add_tuple_to_list (thread_p,
					     sort_info_p->output_file,
					     sort_info_p->output_recdes.data);
		}
	    }
	}
    }

  return (error == NO_ERROR) ? NO_ERROR : er_errid ();
}

/*
 * qfile_compare_partial_sort_record () -
 *   return: -1, 0, or 1, strcmp-style
 *   pk0(in): Pointer to pointer to first sort record
 *   pk1(in): Pointer to pointer to second sort record
 *   arg(in): Pointer to sort info
 *
 * Note: These routines are used for relative comparisons of two sort
 *       records during sorting.
 */
int
qfile_compare_partial_sort_record (const void *pk0, const void *pk1,
				   void *arg)
{
  SORTKEY_INFO *key_info_p;
  SORT_REC *k0, *k1;
  int i, n;
  int o0, o1;
  int order;
  char *d0, *d1;
  char *fp0, *fp1;		/* sort_key field pointer */

  assert (pk0 != NULL);
  assert (pk1 != NULL);

  key_info_p = (SORTKEY_INFO *) arg;
  n = key_info_p->nkeys;
  order = 0;

  k0 = *(SORT_REC **) pk0;
  k1 = *(SORT_REC **) pk1;

  assert (k0 != NULL);
  assert (k1 != NULL);

  /* get body start position of k0, k1 */
  fp0 = &(k0->s.original.body[0]);
  fp0 = PTR_ALIGN (fp0, MAX_ALIGNMENT);

  fp1 = &(k1->s.original.body[0]);
  fp1 = PTR_ALIGN (fp1, MAX_ALIGNMENT);

  for (i = 0; i < n; i++)
    {
      if (QFILE_GET_TUPLE_VALUE_FLAG (fp0) == V_BOUND)
	{
	  o0 = 1;
	}
      else
	{
	  o0 = 0;		/* NULL */
	}

      if (QFILE_GET_TUPLE_VALUE_FLAG (fp1) == V_BOUND)
	{
	  o1 = 1;
	}
      else
	{
	  o1 = 0;		/* NULL */
	}

      if (o0 && o1)
	{
	  d0 = fp0 + QFILE_TUPLE_VALUE_HEADER_LENGTH;
	  d1 = fp1 + QFILE_TUPLE_VALUE_HEADER_LENGTH;

	  assert (key_info_p->key[i].col_dom != NULL);
	  assert (TP_DOMAIN_TYPE (key_info_p->key[i].col_dom) !=
		  DB_TYPE_VARIABLE);

	  order = (*key_info_p->key[i].sort_f) (d0, d1,
						key_info_p->key[i].col_dom,
						0, 1);

	  order = key_info_p->key[i].is_desc ? -order : order;
	}
      else
	{
	  order = qfile_compare_with_null_value (o0, o1, key_info_p->key[i]);
	}

      if (order != 0)
	{
	  break;
	}

      fp0 +=
	QFILE_TUPLE_VALUE_HEADER_LENGTH + QFILE_GET_TUPLE_VALUE_LENGTH (fp0);
      fp1 +=
	QFILE_TUPLE_VALUE_HEADER_LENGTH + QFILE_GET_TUPLE_VALUE_LENGTH (fp1);
    }

  return order;
}

int
qfile_compare_all_sort_record (const void *pk0, const void *pk1, void *arg)
{
  SORTKEY_INFO *key_info_p;
  SORT_REC *k0, *k1;
  int i, n;
  int order;
  int o0, o1;
  char *d0, *d1;

  assert (pk0 != NULL);
  assert (pk1 != NULL);

  key_info_p = (SORTKEY_INFO *) arg;
  n = key_info_p->nkeys;
  order = 0;

  k0 = *(SORT_REC **) pk0;
  k1 = *(SORT_REC **) pk1;

  assert (k0 != NULL);
  assert (k1 != NULL);

  for (i = 0; i < n; i++)
    {
      o0 = k0->s.offset[i];
      o1 = k1->s.offset[i];

      if (o0 && o1)
	{
	  d0 = (char *) k0 + o0;
	  d1 = (char *) k1 + o1;

	  assert (key_info_p->key[i].col_dom != NULL);
	  assert (TP_DOMAIN_TYPE (key_info_p->key[i].col_dom) !=
		  DB_TYPE_VARIABLE);

	  order = (*key_info_p->key[i].sort_f) (d0, d1,
						key_info_p->key[i].col_dom,
						0, 1);
	  order = key_info_p->key[i].is_desc ? -order : order;
	}
      else
	{
	  order = qfile_compare_with_null_value (o0, o1, key_info_p->key[i]);
	}

      if (order != 0)
	{
	  break;
	}
    }

  return order;
}

/*
 * qfile_compare_with_null_value () -
 *   return: -1, 0, or 1, strcmp-style
 *   o0(in): The first value
 *   o1(in): The second value
 *   key_info(in): Sub-key info
 *
 * Note: These routines are internally used for relative comparisons
 *       of two values which include NULL value.
 */
static int
qfile_compare_with_null_value (int o0, int o1, SUBKEY_INFO key_info)
{
  /* At least one of the values sholud be NULL */
  assert (o0 == 0 || o1 == 0);

  if (o0 == 0 && o1 == 0)
    {
      /* both are unbound */
      return 0;
    }
  else if (o0 == 0)
    {
      /* NULL compare_op !NULL */
      assert (o1 != 0);
      if (key_info.is_nulls_first)
	{
	  return -1;
	}
      else
	{
	  return 1;
	}
    }
  else
    {
      /* !NULL compare_op NULL */
      assert (o1 == 0);
      if (key_info.is_nulls_first)
	{
	  return 1;
	}
      else
	{
	  return -1;
	}
    }
}

/* qfile_get_estimated_pages_for_sorting () -
 *   return:
 *   listid(in):
 *   info(in):
 *
 * Note: Make an estimate of input page count to be passed to the sorting
 *       module.  We want this to be an upper bound, because the sort
 *       package already has a limit on sort buffer size, and sorting
 *       proceeds faster with larger in-memory use.
 */
int
qfile_get_estimated_pages_for_sorting (QFILE_LIST_ID * list_id_p,
				       SORTKEY_INFO * key_info_p)
{
  int prorated_pages, sort_key_size, sort_key_overhead;

  prorated_pages = (int) list_id_p->page_cnt;
  if (key_info_p->use_original == 1)
    {
      /* P_sort_key */

      /*
       * Every Part sort key record will have one int of overhead
       * per field in the key (for the offset vector).
       */
      sort_key_size = (int) offsetof (SORT_REC, s.original.body[0]);
      sort_key_overhead =
	(int) ceil (((double) (list_id_p->tuple_cnt * sort_key_size)) /
		    DB_PAGESIZE);
    }
  else
    {
      /* A_sort_key */

      /*
       * Every Part sort key record will have one int of overhead
       * per field in the key (for the offset vector).
       */
      sort_key_size = (int) offsetof (SORT_REC, s.offset[0]) +
	sizeof (((SORT_REC *) 0)->s.offset[0]) * key_info_p->nkeys;
      sort_key_overhead =
	(int) ceil (((double) (list_id_p->tuple_cnt * sort_key_size)) /
		    DB_PAGESIZE);
    }

  return prorated_pages + sort_key_overhead;
}

/* qfile_initialize_sort_key_info () -
 *   return:
 *   info(in):
 *   list(in):
 *   types(in):
 */
SORTKEY_INFO *
qfile_initialize_sort_key_info (SORTKEY_INFO * key_info_p, SORT_LIST * list_p,
				QFILE_TUPLE_VALUE_TYPE_LIST * types)
{
  int i, n;
  SUBKEY_INFO *subkey;

  if (types == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
      return NULL;
    }

  if (list_p)
    {
      n = qfile_get_sort_list_size (list_p);
    }
  else
    {
      n = types->type_cnt;
    }

  key_info_p->nkeys = n;
  key_info_p->use_original = (n != types->type_cnt);
  key_info_p->error = NO_ERROR;

  if (n <= (int) DIM (key_info_p->default_keys))
    {
      key_info_p->key = key_info_p->default_keys;
    }
  else
    {
      key_info_p->key = (SUBKEY_INFO *) malloc (n * sizeof (SUBKEY_INFO));
      if (key_info_p->key == NULL)
	{
	  return NULL;
	}
    }

  if (list_p)
    {
      SORT_LIST *p;
      for (i = 0, p = list_p; p; i++, p = p->next)
	{
	  assert_release (p->pos_descr.pos_no >= 0);

	  subkey = &key_info_p->key[i];
	  subkey->col = p->pos_descr.pos_no;
	  if (key_info_p->use_original)
	    {
	      key_info_p->key[i].permuted_col = i;
	    }
	  else
	    {
	      key_info_p->key[subkey->col].permuted_col = i;
	    }

	  assert (subkey->col < types->type_cnt);
	  assert (types->domp[subkey->col] != NULL);
	  subkey->col_dom = types->domp[subkey->col];
	  subkey->sort_f = subkey->col_dom->type->data_cmpdisk;

	  subkey->is_desc = (p->s_order == S_ASC) ? 0 : 1;
	  subkey->is_nulls_first = (p->s_nulls == S_NULLS_LAST) ? 0 : 1;
	}
    }
  else
    {
      for (i = 0; i < n; i++)
	{
	  SUBKEY_INFO *subkey;

	  subkey = &key_info_p->key[i];
	  subkey->col = i;
	  subkey->permuted_col = i;

	  assert (subkey->col < types->type_cnt);
	  assert (types->domp[subkey->col] != NULL);
	  subkey->col_dom = types->domp[subkey->col];

	  subkey->sort_f = subkey->col_dom->type->data_cmpdisk;

	  subkey->is_desc = 0;
	  subkey->is_nulls_first = 1;
	}
    }

  return key_info_p;
}

/* qfile_clear_sort_key_info () -
 *   return:
 *   info(in):
 */
void
qfile_clear_sort_key_info (SORTKEY_INFO * key_info_p)
{
  if (!key_info_p)
    {
      return;
    }

  if (key_info_p->key && key_info_p->key != key_info_p->default_keys)
    {
      free_and_init (key_info_p->key);
    }

  key_info_p->key = NULL;
  key_info_p->nkeys = 0;
}

/* qfile_initialize_sort_info () -
 *   return:
 *   info(in):
 *   listid(in):
 *   sort_list(in):
 */
static SORT_INFO *
qfile_initialize_sort_info (SORT_INFO * sort_info_p,
			    QFILE_LIST_ID * list_id_p,
			    SORT_LIST * sort_list_p)
{
  sort_info_p->key_info.key = NULL;
#if 0				/* SortCache */
  VPID_SET_NULL (&(sort_info_p->fixed_vpid));
  sort_info_p->fixed_page = NULL;
#endif /* SortCache */
  sort_info_p->output_recdes.data = NULL;
  sort_info_p->output_recdes.area_size = 0;
  if (qfile_initialize_sort_key_info (&sort_info_p->key_info, sort_list_p,
				      &list_id_p->type_list) == NULL)
    {
      return NULL;
    }

  return sort_info_p;
}

/* qfile_clear_sort_info () -
 *   return: none
 *   info(in): Pointer to info block to be initialized
 *
 * Note: Free all internal structures in the given SORT_INFO block.
 */
static void
qfile_clear_sort_info (SORT_INFO * sort_info_p)
{
#if 0				/* SortCache */
  QFILE_LIST_ID *list_idp;

  list_idp = &(sort_info_p->s_id->s_id->list_id);

  if (sort_info_p->fixed_page != NULL)
    {
      qmgr_free_old_page_and_init (thread_p, sort_info_p->fixed_page,
				   list_idp->tfile_vfid);
    }
#endif /* SortCache */

  qfile_clear_sort_key_info (&sort_info_p->key_info);

  if (sort_info_p->output_recdes.data)
    {
      free_and_init (sort_info_p->output_recdes.data);
    }

  sort_info_p->output_recdes.data = NULL;
  sort_info_p->output_recdes.area_size = 0;
}

/*
 * qfile_sort_list_with_func () -
 *   return: QFILE_LIST_ID *, or NULL
 *   list_id(in): Source list file identifier
 *   sort_list(in): List of comparison items
 *   option(in):
 *   ls_flag(in):
 *   get_fn(in):
 *   put_fn(in):
 *   cmp_fn(in):
 *   extra_arg(in):
 *   limit(in):
 *   do_close(in):
 */
QFILE_LIST_ID *
qfile_sort_list_with_func (THREAD_ENTRY * thread_p, QFILE_LIST_ID * list_id_p,
			   SORT_LIST * sort_list_p, QUERY_OPTIONS option,
			   int flag, SORT_GET_FUNC * get_func,
			   SORT_PUT_FUNC * put_func, SORT_CMP_FUNC * cmp_func,
			   void *extra_arg, int limit, bool do_close)
{
  QFILE_LIST_ID *srlist_id;
  QFILE_LIST_SCAN_ID t_scan_id;
  QFILE_SORT_SCAN_ID s_scan_id;
  SORT_INFO info;
  int sort_result, estimated_pages;
  SORT_DUP_OPTION dup_option;

  srlist_id = qfile_open_list (thread_p, &list_id_p->type_list, sort_list_p,
			       list_id_p->query_id, flag);
  if (srlist_id == NULL)
    {
      return NULL;
    }

  /* open a scan on the unsorted list file */
  if (qfile_open_list_scan (list_id_p, &t_scan_id) != NO_ERROR)
    {
      qfile_close_and_free_list_file (thread_p, srlist_id);
      return NULL;
    }

  if (qfile_initialize_sort_info (&info, list_id_p, sort_list_p) == NULL)
    {
      qfile_close_scan (thread_p, &t_scan_id);
      qfile_close_and_free_list_file (thread_p, srlist_id);
      return NULL;
    }

  info.s_id = &s_scan_id;
  info.output_file = srlist_id;
  info.extra_arg = extra_arg;

  if (get_func == NULL)
    {
      get_func = &qfile_get_next_sort_item;
    }

  if (put_func == NULL)
    {
      put_func = &qfile_put_next_sort_item;
    }

  if (cmp_func == NULL)
    {
      if (info.key_info.use_original == 1)
	{
	  cmp_func = &qfile_compare_partial_sort_record;
	}
      else
	{
	  cmp_func = &qfile_compare_all_sort_record;
	}
    }

  s_scan_id.s_id = &t_scan_id;
  s_scan_id.tplrec.size = 0;
  s_scan_id.tplrec.tpl = (char *) NULL;

  estimated_pages = qfile_get_estimated_pages_for_sorting (list_id_p,
							   &info.key_info);
  dup_option = ((option == Q_DISTINCT) ? SORT_ELIM_DUP : SORT_DUP);

  sort_result =
    sort_listfile (thread_p, NULL_VOLID, estimated_pages, get_func, &info,
		   put_func, &info, cmp_func, &info.key_info, dup_option,
		   limit);

  if (sort_result < 0)
    {
#if 0				/* SortCache */
      qfile_clear_sort_info (&info);
      qfile_close_scan (&t_scan_id);
#else /* not SortCache */
      qfile_close_scan (thread_p, &t_scan_id);
      qfile_clear_sort_info (&info);
#endif /* not SortCache */
      qfile_close_list (thread_p, list_id_p);
      qfile_destroy_list (thread_p, list_id_p);
      qfile_close_and_free_list_file (thread_p, srlist_id);
      return NULL;
    }

  if (do_close)
    {
      qfile_close_list (thread_p, srlist_id);
    }

#if 0				/* SortCache */
  qfile_clear_sort_info (&info);
  qfile_close_scan (&t_scan_id);
#else /* not SortCache */
  qfile_close_scan (thread_p, &t_scan_id);
  qfile_clear_sort_info (&info);
#endif /* not SortCache */

  qfile_close_list (thread_p, list_id_p);
  qfile_destroy_list (thread_p, list_id_p);
  qfile_copy_list_id (list_id_p, srlist_id, true);
  QFILE_FREE_AND_INIT_LIST_ID (srlist_id);

  return list_id_p;
}

/*
 * qfile_sort_list () -
 *   return: QFILE_LIST_ID *, or NULL
 *   list_id(in): Source list file identifier
 *   sort_list(in): List of comparison items
 *   option(in):
 *   do_close(in);
 *
 * Note: This routine sorts the specified list file tuples according
 *       to the list of comparison items and generates a sorted list
 *       file. The source list file is not affected by the routine.
 */
QFILE_LIST_ID *
qfile_sort_list (THREAD_ENTRY * thread_p, QFILE_LIST_ID * list_id_p,
		 SORT_LIST * sort_list_p, QUERY_OPTIONS option, bool do_close)
{
  int ls_flag;

  if (sort_list_p
      && qfile_is_sort_list_covered (list_id_p->sort_list,
				     sort_list_p) == true)
    {
      /* no need to sort here */
      return list_id_p;
    }

  ls_flag = (option == Q_DISTINCT) ? QFILE_FLAG_DISTINCT : QFILE_FLAG_ALL;

  return qfile_sort_list_with_func (thread_p, list_id_p, sort_list_p, option,
				    ls_flag, NULL, NULL, NULL, NULL,
				    NO_SORT_LIMIT, do_close);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * qfile_copy_list_pages () -
 *   return:
 *   old_first_vpidp(in)        :
 *   old_tfile_vfidp(in)        :
 *   new_first_vpidp(in)        :
 *   new_last_vpidp(in) :
 *   new_tfile_vfidp(in)        :
 */
static int
qfile_copy_list_pages (THREAD_ENTRY * thread_p, VPID * old_first_vpid_p,
		       QMGR_TEMP_FILE * old_tfile_vfid_p,
		       VPID * new_first_vpid_p, VPID * new_last_vpid_p,
		       QMGR_TEMP_FILE * new_tfile_vfid_p)
{
  PAGE_PTR old_page_p, old_ovfl_page_p, prev_page_p, new_page_p,
    new_ovfl_page_p;
  VPID old_next_vpid, old_ovfl_vpid, prev_vpid, new_ovfl_vpid;

  old_page_p = qmgr_get_old_page (thread_p, old_first_vpid_p,
				  old_tfile_vfid_p);
  if (old_page_p == NULL)
    {
      return ER_FAILED;
    }

  new_page_p = qmgr_get_new_page (thread_p, new_first_vpid_p,
				  new_tfile_vfid_p);
  if (new_page_p == NULL)
    {
      qmgr_free_old_page_and_init (thread_p, old_page_p, old_tfile_vfid_p);
      return ER_FAILED;
    }

  *new_last_vpid_p = *new_first_vpid_p;

  while (true)
    {
      (void) memcpy (new_page_p, old_page_p, DB_PAGESIZE);

      QFILE_GET_OVERFLOW_VPID (&old_ovfl_vpid, old_page_p);
      prev_page_p = new_page_p;
      new_ovfl_page_p = NULL;

      while (!VPID_ISNULL (&old_ovfl_vpid))
	{
	  old_ovfl_page_p = qmgr_get_old_page (thread_p, &old_ovfl_vpid,
					       old_tfile_vfid_p);
	  if (old_ovfl_page_p == NULL)
	    {
	      qmgr_free_old_page_and_init (thread_p, old_page_p,
					   old_tfile_vfid_p);
	      qfile_set_dirty_page (thread_p, new_page_p, FREE,
				    new_tfile_vfid_p);
	      return ER_FAILED;
	    }

	  new_ovfl_page_p = qmgr_get_new_page (thread_p, &new_ovfl_vpid,
					       new_tfile_vfid_p);

	  if (new_ovfl_page_p == NULL)
	    {
	      qmgr_free_old_page_and_init (thread_p, old_ovfl_page_p,
					   old_tfile_vfid_p);
	      qmgr_free_old_page_and_init (thread_p, old_page_p,
					   old_tfile_vfid_p);
	      qfile_set_dirty_page (thread_p, new_page_p, FREE,
				    new_tfile_vfid_p);
	      return ER_FAILED;
	    }

	  (void) memcpy (new_ovfl_page_p, old_ovfl_page_p, DB_PAGESIZE);

	  QFILE_GET_OVERFLOW_VPID (&old_ovfl_vpid, old_ovfl_page_p);
	  qmgr_free_old_page_and_init (thread_p, old_ovfl_page_p,
				       old_tfile_vfid_p);

	  QFILE_PUT_OVERFLOW_VPID (prev_page_p, &new_ovfl_vpid);
	  qfile_set_dirty_page (thread_p, prev_page_p, FREE,
				new_tfile_vfid_p);

	  prev_page_p = new_ovfl_page_p;
	}

      if (new_ovfl_page_p)
	{
	  qfile_set_dirty_page (thread_p, new_ovfl_page_p, FREE,
				new_tfile_vfid_p);
	}

      QFILE_GET_NEXT_VPID (&old_next_vpid, old_page_p);
      qmgr_free_old_page_and_init (thread_p, old_page_p, old_tfile_vfid_p);

      if (VPID_ISNULL (&old_next_vpid))
	{
	  qfile_set_dirty_page (thread_p, new_page_p, FREE, new_tfile_vfid_p);
	  new_page_p = NULL;
	  break;
	}

      old_page_p = qmgr_get_old_page (thread_p, &old_next_vpid,
				      old_tfile_vfid_p);
      prev_page_p = new_page_p;
      prev_vpid = *new_last_vpid_p;

      if (old_page_p == NULL)
	{
	  qfile_set_dirty_page (thread_p, prev_page_p, FREE,
				new_tfile_vfid_p);
	  return ER_FAILED;
	}

      new_page_p = qmgr_get_new_page (thread_p, new_last_vpid_p,
				      new_tfile_vfid_p);
      if (new_page_p == NULL)
	{
	  qmgr_free_old_page_and_init (thread_p, old_page_p,
				       old_tfile_vfid_p);
	  qfile_set_dirty_page (thread_p, prev_page_p, FREE,
				new_tfile_vfid_p);
	  return ER_FAILED;
	}

      QFILE_PUT_PREV_VPID (new_page_p, &prev_vpid);
      QFILE_PUT_NEXT_VPID (prev_page_p, new_last_vpid_p);

      qfile_set_dirty_page (thread_p, prev_page_p, FREE, new_tfile_vfid_p);
    }

  if (new_page_p)
    {
      qfile_set_dirty_page (thread_p, prev_page_p, FREE, new_tfile_vfid_p);
    }

  return NO_ERROR;
}
#endif

/*
 * List File Scan Routines
 */

/*
 * qfile_get_tuple () -
 *   return:
 *   first_page(in):
 *   tuplep(in):
 *   tplrec(in):
 *   list_idp(in):
 */
int
qfile_get_tuple (THREAD_ENTRY * thread_p, PAGE_PTR first_page_p,
		 QFILE_TUPLE tuple, QFILE_TUPLE_RECORD * tuple_record_p,
		 QFILE_LIST_ID * list_id_p)
{
  VPID ovfl_vpid;
  char *tuple_p;
  int offset;
  int tuple_length, tuple_page_size;
  int max_tuple_page_size;
  PAGE_PTR page_p;

  page_p = first_page_p;
  tuple_length = QFILE_GET_TUPLE_LENGTH (tuple);

  if (tuple_record_p->size < tuple_length)
    {
      if (qfile_reallocate_tuple (tuple_record_p, tuple_length) != NO_ERROR)
	{
	  return ER_FAILED;
	}
    }

  tuple_p = (char *) tuple_record_p->tpl;

  if (QFILE_GET_OVERFLOW_PAGE_ID (page_p) == NULL_PAGEID)
    {
      /* tuple is inside the page */
      memcpy (tuple_p, tuple, tuple_length);
      return NO_ERROR;
    }
  else
    {
      /* tuple has overflow pages */
      offset = 0;
      max_tuple_page_size = qfile_Max_tuple_page_size;

      do
	{
	  QFILE_GET_OVERFLOW_VPID (&ovfl_vpid, page_p);
	  tuple_page_size = MIN (tuple_length - offset, max_tuple_page_size);

	  memcpy (tuple_p, (char *) page_p + QFILE_PAGE_HEADER_SIZE,
		  tuple_page_size);

	  tuple_p += tuple_page_size;
	  offset += tuple_page_size;

	  if (page_p != first_page_p)
	    {
	      qmgr_free_old_page_and_init (thread_p, page_p,
					   list_id_p->tfile_vfid);
	    }

	  if (ovfl_vpid.pageid != NULL_PAGEID)
	    {
	      page_p = qmgr_get_old_page (thread_p, &ovfl_vpid,
					  list_id_p->tfile_vfid);
	      if (page_p == NULL)
		{
		  return ER_FAILED;
		}
	    }
	}
      while (ovfl_vpid.pageid != NULL_PAGEID);
    }

  return NO_ERROR;
}

/*
 * qfile_get_tuple_from_current_list () -
 *   return: int (NO_ERROR or ER_FAILED)
 *   s_id(in): Scan identifier
 *   tplrec(in): Tuple record descriptor
 *
 * Note: Fetch the current tuple of the given scan identifier into the
 *       tuple descriptor.
 */
static int
qfile_get_tuple_from_current_list (THREAD_ENTRY * thread_p,
				   QFILE_LIST_SCAN_ID * scan_id_p,
				   QFILE_TUPLE_RECORD * tuple_record_p)
{
  PAGE_PTR page_p;
  QFILE_TUPLE tuple;

  page_p = scan_id_p->curr_pgptr;
  tuple = (char *) page_p + scan_id_p->curr_offset;

  return qfile_get_tuple (thread_p, page_p, tuple, tuple_record_p,
			  &scan_id_p->list_id);
}

/*
 * qfile_scan_next  () -
 *   return: SCAN_CODE (S_SUCCESS, S_END, S_ERROR)
 *   s_id(in/out): Scan identifier
 *
 * Note: The scan is moved to the next scan item. If there are no more
 *       scan items, S_END is returned.  If an error occurs,
 *       S_ERROR is returned.
 *
 * Note: The scan identifier must be of type LIST FILE scan identifier.
 */
static SCAN_CODE
qfile_scan_next (THREAD_ENTRY * thread_p, QFILE_LIST_SCAN_ID * scan_id_p)
{
  PAGE_PTR page_p, next_page_p;
  VPID next_vpid;

  if (scan_id_p->position == S_BEFORE)
    {
      if (scan_id_p->list_id.tuple_cnt > 0)
	{
	  page_p =
	    qmgr_get_old_page (thread_p, &scan_id_p->list_id.first_vpid,
			       scan_id_p->list_id.tfile_vfid);
	  if (page_p == NULL)
	    {
	      return S_ERROR;
	    }

	  QFILE_COPY_VPID (&scan_id_p->curr_vpid,
			   &scan_id_p->list_id.first_vpid);
	  scan_id_p->curr_pgptr = page_p;
	  scan_id_p->curr_offset = QFILE_PAGE_HEADER_SIZE;
	  scan_id_p->curr_tpl =
	    (char *) scan_id_p->curr_pgptr + QFILE_PAGE_HEADER_SIZE;
	  scan_id_p->curr_tplno = 0;
	  scan_id_p->position = S_ON;
	  return S_SUCCESS;
	}
      else
	{
	  return S_END;
	}
    }
  else if (scan_id_p->position == S_ON)
    {
      if (scan_id_p->curr_tplno <
	  QFILE_GET_TUPLE_COUNT (scan_id_p->curr_pgptr) - 1)
	{
	  scan_id_p->curr_offset +=
	    QFILE_GET_TUPLE_LENGTH (scan_id_p->curr_tpl);
	  scan_id_p->curr_tpl += QFILE_GET_TUPLE_LENGTH (scan_id_p->curr_tpl);
	  scan_id_p->curr_tplno++;
	  return S_SUCCESS;
	}
      else if ((QFILE_GET_NEXT_PAGE_ID (scan_id_p->curr_pgptr) != NULL_PAGEID)
	       && (QFILE_GET_NEXT_PAGE_ID (scan_id_p->curr_pgptr) !=
		   NULL_PAGEID_ASYNC))
	{
	  QFILE_GET_NEXT_VPID (&next_vpid, scan_id_p->curr_pgptr);
	  next_page_p =
	    qmgr_get_old_page (thread_p, &next_vpid,
			       scan_id_p->list_id.tfile_vfid);
	  if (next_page_p == NULL)
	    {
	      return S_ERROR;
	    }

	  qmgr_free_old_page_and_init (thread_p, scan_id_p->curr_pgptr,
				       scan_id_p->list_id.tfile_vfid);
	  QFILE_COPY_VPID (&scan_id_p->curr_vpid, &next_vpid);
	  scan_id_p->curr_pgptr = next_page_p;
	  scan_id_p->curr_tplno = 0;
	  scan_id_p->curr_offset = QFILE_PAGE_HEADER_SIZE;
	  scan_id_p->curr_tpl =
	    (char *) scan_id_p->curr_pgptr + QFILE_PAGE_HEADER_SIZE;
	  return S_SUCCESS;
	}
      else
	{
	  scan_id_p->position = S_AFTER;

	  if (!scan_id_p->keep_page_on_finish)
	    {
	      scan_id_p->curr_vpid.pageid = NULL_PAGEID;
	      qmgr_free_old_page_and_init (thread_p, scan_id_p->curr_pgptr,
					   scan_id_p->list_id.tfile_vfid);
	    }

	  return S_END;
	}
    }
  else if (scan_id_p->position == S_AFTER)
    {
      return S_END;
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_UNKNOWN_CRSPOS, 0);
      return S_ERROR;
    }
}

/*
 * qfile_scan_prev () -
 *   return: SCAN_CODE (S_SUCCESS, S_END, S_ERROR)
 *   s_id(in/out): Scan identifier
 *
 * Note: The scan is moved to previous scan item. If there are no more
 *       scan items, S_END is returned.  If an error occurs,
 *       S_ERROR is returned.
 */
static SCAN_CODE
qfile_scan_prev (THREAD_ENTRY * thread_p, QFILE_LIST_SCAN_ID * scan_id_p)
{
  PAGE_PTR page_p, prev_page_p;
  VPID prev_vpid;

  if (scan_id_p->position == S_BEFORE)
    {
      return S_END;
    }
  else if (scan_id_p->position == S_ON)
    {
      if (scan_id_p->curr_tplno > 0)
	{
	  scan_id_p->curr_offset -=
	    QFILE_GET_PREV_TUPLE_LENGTH (scan_id_p->curr_tpl);
	  scan_id_p->curr_tpl -=
	    QFILE_GET_PREV_TUPLE_LENGTH (scan_id_p->curr_tpl);
	  scan_id_p->curr_tplno--;
	  return S_SUCCESS;
	}
      else if (QFILE_GET_PREV_PAGE_ID (scan_id_p->curr_pgptr) != NULL_PAGEID)
	{
	  QFILE_GET_PREV_VPID (&prev_vpid, scan_id_p->curr_pgptr);
	  prev_page_p =
	    qmgr_get_old_page (thread_p, &prev_vpid,
			       scan_id_p->list_id.tfile_vfid);
	  if (prev_page_p == NULL)
	    {
	      return S_ERROR;
	    }

	  qmgr_free_old_page_and_init (thread_p, scan_id_p->curr_pgptr,
				       scan_id_p->list_id.tfile_vfid);
	  QFILE_COPY_VPID (&scan_id_p->curr_vpid, &prev_vpid);
	  scan_id_p->curr_pgptr = prev_page_p;
	  scan_id_p->curr_tplno = QFILE_GET_TUPLE_COUNT (prev_page_p) - 1;
	  scan_id_p->curr_offset = QFILE_GET_LAST_TUPLE_OFFSET (prev_page_p);
	  scan_id_p->curr_tpl =
	    (char *) scan_id_p->curr_pgptr + scan_id_p->curr_offset;
	  return S_SUCCESS;
	}
      else
	{
	  scan_id_p->position = S_BEFORE;
	  scan_id_p->curr_vpid.pageid = NULL_PAGEID;
	  qmgr_free_old_page_and_init (thread_p, scan_id_p->curr_pgptr,
				       scan_id_p->list_id.tfile_vfid);
	  return S_END;
	}
    }
  else if (scan_id_p->position == S_AFTER)
    {

      if (VPID_ISNULL (&scan_id_p->list_id.first_vpid))
	{
	  return S_END;
	}
      page_p = qmgr_get_old_page (thread_p, &scan_id_p->list_id.last_vpid,
				  scan_id_p->list_id.tfile_vfid);
      if (page_p == NULL)
	{
	  return S_ERROR;
	}

      scan_id_p->position = S_ON;
      QFILE_COPY_VPID (&scan_id_p->curr_vpid, &scan_id_p->list_id.last_vpid);
      scan_id_p->curr_pgptr = page_p;
      scan_id_p->curr_tplno = QFILE_GET_TUPLE_COUNT (page_p) - 1;
      scan_id_p->curr_offset = QFILE_GET_LAST_TUPLE_OFFSET (page_p);
      scan_id_p->curr_tpl =
	(char *) scan_id_p->curr_pgptr + scan_id_p->curr_offset;
      return S_SUCCESS;
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_UNKNOWN_CRSPOS, 0);
      return S_ERROR;
    }
}

/*
 * qfile_save_current_scan_tuple_position () -
 *   return:
 *   s_id(in): Scan identifier
 *   ls_tplpos(in/out): Set to contain current scan tuple position
 *
 * Note: Save current scan tuple position information.
 */
void
qfile_save_current_scan_tuple_position (QFILE_LIST_SCAN_ID * scan_id_p,
					QFILE_TUPLE_POSITION *
					tuple_position_p)
{
  tuple_position_p->status = scan_id_p->status;
  tuple_position_p->position = scan_id_p->position;
  tuple_position_p->vpid.pageid = scan_id_p->curr_vpid.pageid;
  tuple_position_p->vpid.volid = scan_id_p->curr_vpid.volid;
  tuple_position_p->offset = scan_id_p->curr_offset;
  tuple_position_p->tpl = scan_id_p->curr_tpl;
  tuple_position_p->tplno = scan_id_p->curr_tplno;
}

static SCAN_CODE
qfile_retrieve_tuple (THREAD_ENTRY * thread_p, QFILE_LIST_SCAN_ID * scan_id_p,
		      QFILE_TUPLE_RECORD * tuple_record_p, int peek)
{
  int tuple_size;

  if (QFILE_GET_OVERFLOW_PAGE_ID (scan_id_p->curr_pgptr) == NULL_PAGEID)
    {
      if (peek)
	{
	  tuple_record_p->tpl = scan_id_p->curr_tpl;
	}
      else
	{
	  tuple_size = QFILE_GET_TUPLE_LENGTH (scan_id_p->curr_tpl);
	  if (tuple_record_p->size < tuple_size)
	    {
	      if (qfile_reallocate_tuple (tuple_record_p, tuple_size) !=
		  NO_ERROR)
		{
		  return S_ERROR;
		}
	    }
	  memcpy (tuple_record_p->tpl, scan_id_p->curr_tpl, tuple_size);
	}
    }
  else
    {
      /* tuple has overflow pages */
      if (peek)
	{
	  if (qfile_get_tuple_from_current_list (thread_p, scan_id_p,
						 &scan_id_p->tplrec)
	      != NO_ERROR)
	    {
	      return S_ERROR;
	    }
	  tuple_record_p->tpl = scan_id_p->tplrec.tpl;
	}
      else
	{
	  if (qfile_get_tuple_from_current_list (thread_p, scan_id_p,
						 tuple_record_p) != NO_ERROR)
	    {
	      return S_ERROR;
	    }
	}
    }

  return S_SUCCESS;
}

/*
 * qfile_jump_scan_tuple_position() -
 *   return: SCAN_CODE (S_SUCCESS, S_END, S_ERROR)
 *   s_id(in/out): Scan identifier
 *   ls_tplpos(in): Scan tuple position
 *   tplrec(in/out): Tuple record descriptor
 *   peek(in): Peek or Copy Tuple
 *
 * Note: Jump to the given list file scan position and fetch the tuple
 *       to the given tuple descriptor, either by peeking or by
 *       copying depending on the value of the peek parameter.
 *
 * Note: Saved scan can only be on "ON" or "AFTER" positions.
 */
SCAN_CODE
qfile_jump_scan_tuple_position (THREAD_ENTRY * thread_p,
				QFILE_LIST_SCAN_ID * scan_id_p,
				QFILE_TUPLE_POSITION * tuple_position_p,
				QFILE_TUPLE_RECORD * tuple_record_p, int peek)
{
  PAGE_PTR page_p;

  if (tuple_position_p->position == S_ON)
    {
      if (scan_id_p->position == S_ON)
	{
	  if (scan_id_p->curr_vpid.pageid != tuple_position_p->vpid.pageid
	      || scan_id_p->curr_vpid.volid != tuple_position_p->vpid.volid)
	    {
	      page_p = qmgr_get_old_page (thread_p, &tuple_position_p->vpid,
					  scan_id_p->list_id.tfile_vfid);
	      if (page_p == NULL)
		{
		  return S_ERROR;
		}

	      qmgr_free_old_page_and_init (thread_p, scan_id_p->curr_pgptr,
					   scan_id_p->list_id.tfile_vfid);

	      QFILE_COPY_VPID (&scan_id_p->curr_vpid,
			       &tuple_position_p->vpid);
	      scan_id_p->curr_pgptr = page_p;
	    }
	}
      else
	{
	  page_p = qmgr_get_old_page (thread_p, &tuple_position_p->vpid,
				      scan_id_p->list_id.tfile_vfid);
	  if (page_p == NULL)
	    {
	      return S_ERROR;
	    }

	  QFILE_COPY_VPID (&scan_id_p->curr_vpid, &tuple_position_p->vpid);
	  scan_id_p->curr_pgptr = page_p;
	}
    }
  else
    {
      if (scan_id_p->position == S_ON)
	{
	  qmgr_free_old_page_and_init (thread_p, scan_id_p->curr_pgptr,
				       scan_id_p->list_id.tfile_vfid);
	}
    }

  scan_id_p->status = tuple_position_p->status;
  scan_id_p->position = tuple_position_p->position;
  scan_id_p->curr_offset = tuple_position_p->offset;
  scan_id_p->curr_tpl =
    (char *) scan_id_p->curr_pgptr + scan_id_p->curr_offset;
  scan_id_p->curr_tplno = tuple_position_p->tplno;

  if (scan_id_p->position == S_ON)
    {
      return qfile_retrieve_tuple (thread_p, scan_id_p, tuple_record_p, peek);
    }
  else if (scan_id_p->position == S_BEFORE || scan_id_p->position == S_AFTER)
    {
      return S_END;
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_UNKNOWN_CRSPOS, 0);
      return S_ERROR;
    }
}

/*
 * qfile_start_scan_fix () -
 *   return: int (NO_ERROR or ER_FAILED)
 *   s_id(in/out): Scan identifier
 *
 * Note: Start a scan operation which will keep the accessed list file
 *       pages fixed in the buffer pool. The routine starts the scan
 *       operation either from the beginning or from the last point
 *       scan fix ended.
 */
int
qfile_start_scan_fix (THREAD_ENTRY * thread_p, QFILE_LIST_SCAN_ID * scan_id_p)
{
  if (scan_id_p->position == S_ON && !scan_id_p->curr_pgptr)
    {
      scan_id_p->curr_pgptr =
	qmgr_get_old_page (thread_p, &scan_id_p->curr_vpid,
			   scan_id_p->list_id.tfile_vfid);
      if (scan_id_p->curr_pgptr == NULL)
	{
	  return ER_FAILED;
	}

      scan_id_p->curr_tpl =
	(char *) scan_id_p->curr_pgptr + scan_id_p->curr_offset;
    }
  else
    {
      scan_id_p->status = S_STARTED;
      scan_id_p->position = S_BEFORE;
    }

  return NO_ERROR;
}

/*
 * qfile_open_list_scan () -
 *   return: int (NO_ERROR or ER_FAILED)
 *   list_id(in): List identifier
 *   s_id(in/out): Scan identifier
 *
 * Note: A scan identifier is created to scan through the given list of tuples.
 */
int
qfile_open_list_scan (QFILE_LIST_ID * list_id_p,
		      QFILE_LIST_SCAN_ID * scan_id_p)
{
  scan_id_p->status = S_OPENED;
  scan_id_p->position = S_BEFORE;
  scan_id_p->keep_page_on_finish = 0;
  scan_id_p->curr_vpid.pageid = NULL_PAGEID;
  scan_id_p->curr_vpid.volid = NULL_VOLID;
  QFILE_CLEAR_LIST_ID (&scan_id_p->list_id);

  if (qfile_copy_list_id (&scan_id_p->list_id, list_id_p, true) != NO_ERROR)
    {
      return ER_FAILED;
    }

  scan_id_p->tplrec.size = 0;
  scan_id_p->tplrec.tpl = NULL;

  return NO_ERROR;
}

/*
 * qfile_scan_list () -
 *   return: SCAN_CODE (S_SUCCESS, S_END, S_ERROR)
 *   s_id(in): Scan identifier
 *   tplrec(in/out): Tuple record descriptor
 *   peek(in): Peek or Copy Tuple
 *
 * Note: The regular LIST FILE scan is moved to the next scan tuple and
 *       the tuple is fetched to the tuple record descriptor. If there
 *       are no more scan items(tuples), S_END is returned. If
 *       an error occurs, S_ERROR is returned. If peek is true,
 *       the tplrec->tpl pointer is directly set to point to scan list
 *       file page, otherwise the tuple content is copied to the
 *       tplrec tuple area. If the area inside the tplrec is not enough
 *       it is reallocated by this routine.
 *
 * Note1: The pointer set by a PEEK operation is valid until another scan
 *        operation on the list file or until scan is closed.
 *
 * Note2: When the PEEK is specified, the area pointed by the tuple must not
 *        be modified by the caller.
 */
static SCAN_CODE
qfile_scan_list (THREAD_ENTRY * thread_p, QFILE_LIST_SCAN_ID * scan_id_p,
		 SCAN_CODE (*scan_func) (THREAD_ENTRY * thread_p,
					 QFILE_LIST_SCAN_ID *),
		 QFILE_TUPLE_RECORD * tuple_record_p, int peek)
{
  SCAN_CODE qp_scan;

  qp_scan = (*scan_func) (thread_p, scan_id_p);
  if (qp_scan == S_SUCCESS)
    {
      qp_scan = qfile_retrieve_tuple (thread_p, scan_id_p, tuple_record_p,
				      peek);
    }

  return qp_scan;
}

/*
 * qfile_scan_list_next () -
 *   return: SCAN_CODE (S_SUCCESS, S_END, S_ERROR)
 *   s_id(in): Scan identifier
 *   tplrec(in/out): Tuple record descriptor
 *   peek(in): Peek or Copy Tuple
 */
SCAN_CODE
qfile_scan_list_next (THREAD_ENTRY * thread_p, QFILE_LIST_SCAN_ID * scan_id_p,
		      QFILE_TUPLE_RECORD * tuple_record_p, int peek)
{
  return qfile_scan_list (thread_p, scan_id_p, qfile_scan_next,
			  tuple_record_p, peek);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * qfile_scan_list_prev () -
 *   return: SCAN_CODE (S_SUCCESS, S_END, S_ERROR)
 *   s_id(in): Scan identifier
 *   tplrec(in/out): Tuple record descriptor
 *   peek(in): Peek or Copy Tuple
 */
SCAN_CODE
qfile_scan_list_prev (THREAD_ENTRY * thread_p, QFILE_LIST_SCAN_ID * scan_id_p,
		      QFILE_TUPLE_RECORD * tuple_record_p, int peek)
{
  return qfile_scan_list (thread_p, scan_id_p, qfile_scan_prev,
			  tuple_record_p, peek);
}
#endif

/*
 * qfile_end_scan_fix () -
 *   return:
 *   s_id(in/out)   : Scan identifier
 *
 * Note: End a scan fix operation by freeing the current scan page pointer.
 */
void
qfile_end_scan_fix (THREAD_ENTRY * thread_p, QFILE_LIST_SCAN_ID * scan_id_p)
{
  if (scan_id_p->position == S_ON && scan_id_p->curr_pgptr)
    {
      qmgr_free_old_page_and_init (thread_p, scan_id_p->curr_pgptr,
				   scan_id_p->list_id.tfile_vfid);
    }
  else
    {
      scan_id_p->status = S_ENDED;
      scan_id_p->position = S_AFTER;
    }
}

/*
 * qfile_close_scan () -
 *   return:
 *   s_id(in)   : Scan identifier
 *
 * Note: The scan identifier is closed and allocated areas and page
 *       buffers are freed.
 */
void
qfile_close_scan (THREAD_ENTRY * thread_p, QFILE_LIST_SCAN_ID * scan_id_p)
{
  if (scan_id_p->status == S_CLOSED)
    {
      return;
    }

  if ((scan_id_p->position == S_ON
       || (scan_id_p->position == S_AFTER && scan_id_p->keep_page_on_finish))
      && scan_id_p->curr_pgptr)
    {
      qmgr_free_old_page_and_init (thread_p, scan_id_p->curr_pgptr,
				   scan_id_p->list_id.tfile_vfid);
    }

  if (scan_id_p->tplrec.tpl != NULL)
    {
      free_and_init (scan_id_p->tplrec.tpl);
      scan_id_p->tplrec.size = 0;
    }

  qfile_clear_list_id (&scan_id_p->list_id);

  scan_id_p->status = S_CLOSED;
}

/*
 * qfile_has_next_page() - returns whether the page has the next page or not.
 *   If false, that means the page is the last page of the list file.
 *  return: true/false
 *  page_p(in):
 */
bool
qfile_has_next_page (PAGE_PTR page_p)
{
  return (QFILE_GET_NEXT_PAGE_ID (page_p) != NULL_PAGEID
	  && QFILE_GET_NEXT_PAGE_ID (page_p) != NULL_PAGEID_ASYNC);
}
