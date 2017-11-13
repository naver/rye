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
 * btree.c - B+-Tree mananger
 */

#ident "$Id$"

#include "config.h"

#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include "btree_load.h"
#include "storage_common.h"
#include "error_manager.h"
#include "page_buffer.h"
#include "file_io.h"
#include "file_manager.h"
#include "slotted_page.h"
#include "oid.h"
#include "log_manager.h"
#include "memory_alloc.h"
#include "overflow_file.h"
#include "xserver_interface.h"
#include "btree.h"
#include "scan_manager.h"
#include "thread.h"
#include "heap_file.h"
#include "object_primitive.h"
#include "object_print.h"
#include "list_file.h"
#include "fetch.h"
#include "connection_defs.h"
#include "locator_sr.h"
#include "network_interface_sr.h"
#include "utility.h"

#include "fault_injection.h"

/* this must be the last header file included!!! */
#include "dbval.h"

#define BTREE_NODE_MAX_SPLIT_SIZE(page_ptr) \
  (db_page_size() - spage_header_size() - spage_get_space_for_record((page_ptr), HEADER))

#define BTREE_IS_ROOT_PAGE(btid, vpid) \
  ((btid)->vfid.volid == (vpid)->volid && (btid)->root_pageid == (vpid)->pageid)

static int btree_get_prefix (THREAD_ENTRY * thread_p, BTID_INT * btid,
			     const DB_IDXKEY * key1,
			     const DB_IDXKEY * key2, DB_IDXKEY * prefix_key);

static int btree_insert_into_leaf (THREAD_ENTRY * thread_p, BTID_INT * btid,
				   PAGE_PTR page_ptr, const DB_IDXKEY * key);
static int btree_find_split_point (THREAD_ENTRY * thread_p, BTID_INT * btid,
				   PAGE_PTR page_ptr,
				   const DB_IDXKEY * key,
				   INT16 * mid_slot,
				   DB_IDXKEY * mid_key, bool * clear_midkey);
static int btree_split_find_pivot (int total, int ent_size,
				   BTREE_NODE_SPLIT_INFO * split_info);

static int btree_split_node (THREAD_ENTRY * thread_p, BTID_INT * btid,
			     PAGE_PTR P, PAGE_PTR Q, PAGE_PTR R,
			     VPID * P_vpid, VPID * Q_vpid, VPID * R_vpid,
			     INT16 p_slot_id, const DB_IDXKEY * key,
			     VPID * child_vpid);
static int btree_split_root (THREAD_ENTRY * thread_p, BTID_INT * btid,
			     PAGE_PTR P, PAGE_PTR Q, PAGE_PTR R,
			     VPID * P_page_vpid, VPID * Q_page_vpid,
			     VPID * R_page_vpid, const DB_IDXKEY * key,
			     VPID * child_vpid);
static int btree_insert_new_key (THREAD_ENTRY * thread_p,
				 BTID_INT * btid, PAGE_PTR leaf_page,
				 const DB_IDXKEY * key, INT16 slot_id);

/*
 * btree_insert_new_key () -
 *   return:
 *   btid(in): B+tree index identifier
 *   leaf_page(in): Leaf page pointer to which the key is to be inserted
 *   key(in): Key to be inserted
 *   oid(in): Object identifier to be inserted together with the key
 *   slot_id(in):
 */
static int
btree_insert_new_key (THREAD_ENTRY * thread_p, BTID_INT * btid,
		      PAGE_PTR leaf_page, const DB_IDXKEY * key,
		      INT16 slot_id)
{
  int ret = NO_ERROR;
  int max_free;
#if !defined(NDEBUG)
  int key_len;
#endif
  RECDES rec = RECDES_INITIALIZER;
  char rec_buf[IO_MAX_PAGE_SIZE + BTREE_MAX_ALIGN];
  char rv_key[OR_OID_SIZE + OR_BTID_ALIGNED_SIZE + BTREE_MAX_KEYLEN +
	      BTREE_MAX_ALIGN];
  int rv_key_len;
  BTREE_NODE_HEADER node_header;
  LOG_DATA_ADDR addr = LOG_ADDR_INITIALIZER;

  rec.type = REC_HOME;
  rec.area_size = DB_PAGESIZE;
  rec.data = PTR_ALIGN (rec_buf, BTREE_MAX_ALIGN);


  max_free = spage_max_space_for_new_record (thread_p, leaf_page);
#if !defined(NDEBUG)
  key_len = btree_get_key_length (key);	/* TODO - */

  assert (BTREE_IS_VALID_KEY_LEN (key_len));
#endif

  /* put a LOGICAL log to undo the insertion of <key, oid> pair
   * to the B+tree index. This will be a call to delete this pair
   * from the index. Put this logical log here, because now we know
   * that the <key, oid> pair to be inserted is not already in the index.
   */
  ret = btree_rv_save_keyval (btid, key, rv_key, &rv_key_len);
  if (ret != NO_ERROR)
    {
      return ret;
    }

  /* form a new leaf record */
  ret = btree_write_record (thread_p, btid, NULL, key, BTREE_LEAF_NODE, &rec);
  if (ret != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (rec.length > max_free)
    {
      assert (false);
      ret = ER_BTREE_NO_SPACE;

      GOTO_EXIT_ON_ERROR;
    }

  /* save the inserted record for redo purposes,
   * in the case of redo, the record will be inserted
   */
  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, leaf_page, slot_id);
  log_append_undoredo_data (thread_p, RVBT_KEYVAL_INSERT, &addr,
			    rv_key_len, rec.length, rv_key, rec.data);

  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  /* insert the new record */
  if (spage_insert_at (thread_p, leaf_page, slot_id, &rec) != SP_SUCCESS)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* update the page header */
  if (btree_read_node_header (leaf_page, &node_header) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  assert_release (node_header.key_cnt >= 0);

  node_header.key_cnt++;

  assert_release (node_header.key_cnt >= 1);
  assert (node_header.node_level == 1);
  BTREE_CHECK_KEY_CNT (leaf_page, node_header.node_level,
		       node_header.key_cnt);

  assert (node_header.split_info.pivot >= 0);
  assert (node_header.key_cnt > 0);
  btree_split_next_pivot (&node_header.split_info,
			  (float) slot_id / node_header.key_cnt,
			  node_header.key_cnt);

  if (btree_write_node_header (leaf_page, &node_header) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* log the new record insertion and update to the header record for
   * undo/redo purposes.  This can be after the insert/update since we
   * still have the page pinned.
   */
  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  pgbuf_set_dirty (thread_p, leaf_page, DONT_FREE);

end:

  return ret;

exit_on_error:

  ret = (ret == NO_ERROR
	 && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;

  goto end;
}

/*
 * btree_insert_into_leaf () -
 *   return: NO_ERROR
 *   btid(in): B+tree index identifier
 *   page_ptr(in): Leaf page pointer to which the key is to be inserted
 *   key(in): Key to be inserted
 *
 * Note: Insert the given < key, oid > pair into the leaf page
 * specified. If the key is a new one, it assumes that there is
 * enough space in the page to make insertion, otherwise an
 * error condition is raised. If the key is an existing one,
 * inserting "oid" may necessitate the use of overflow pages.
 *
 * LOGGING Note: When the btree is new, splits and merges will
 * not be committed, but will be attached.  If the transaction
 * is rolled back, the merge and split actions will be rolled
 * back as well.  The undo (and redo) logging for splits and
 * merges are page based (physical) logs, thus the rest of the
 * logs for the undo session must be page based as well.  When
 * the btree is old, splits and merges are committed and all
 * the rest of the logging must be logical (non page based)
 * since pages may change as splits and merges are performed.
 *
 * LOGGING Note2: We adopts a new concept of log, that is a combined log of
 * logical undo and physical redo log, for performance reasons.
 * For key insert, this will be written only when the btree is old and
 * the given key is not an overflow-key.
 * However each undo log and redo log will be written as it is in the rest of
 * the cases(need future work).
 */
static int
btree_insert_into_leaf (THREAD_ENTRY * thread_p,
			BTID_INT * btid, PAGE_PTR page_ptr,
			const DB_IDXKEY * key)
{
  int error = NO_ERROR;
  bool key_found;
  INT16 slot_id;
  int max_diff_column_index;
  OID oid;
  OR_INDEX *indexp = NULL;
#if defined(SERVER_MODE)
  int tran_index;
  LOG_TDES *tdes;
#endif /* SERVER_MODE */

  assert (btid != NULL);
  assert (key->size > 1);

  assert (btid->classrepr != NULL);
  assert (btid->classrepr_cache_idx != -1);
  assert (btid->indx_id != -1);

  indexp = &(btid->classrepr->indexes[btid->indx_id]);

  assert (key->size == indexp->n_atts + 1);

  slot_id = NULL_SLOTID;
  key_found = btree_search_leaf_page (thread_p, btid, page_ptr, key,
				      &slot_id, &max_diff_column_index);
  if (slot_id == NULL_SLOTID)
    {
      error = er_errid ();
      GOTO_EXIT_ON_ERROR;
    }

  if (key_found)
    {
#if defined(SERVER_MODE)
      tran_index = logtb_get_current_tran_index (thread_p);
      tdes = LOG_FIND_TDES (tran_index);
      assert (tdes != NULL);

      /* check iff is from add index */
      if (tdes != NULL && tdes->type == TRAN_TYPE_DDL)
	{
	  assert (INDEX_IS_IN_PROGRESS (indexp));
	  goto end;
	}
#endif /* SERVER_MODE */

      if (log_is_in_crash_recovery ())
	{
	  goto end;
	}

      assert (false);
      btree_get_oid_from_key (thread_p, btid, key, &oid);
      error = ER_BTREE_DUPLICATE_OID;
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, error, 3,
	      oid.volid, oid.pageid, oid.slotid);

      GOTO_EXIT_ON_ERROR;
    }

  if (INDEX_IS_UNIQUE (indexp)
      && max_diff_column_index == key->size - 1 && !btree_key_is_null (key))
    {
      btree_get_oid_from_key (thread_p, btid, key, &oid);
      BTREE_SET_UNIQUE_VIOLATION_ERROR (thread_p, key, &oid, btid->sys_btid);
      error = ER_BTREE_UNIQUE_FAILED;

      GOTO_EXIT_ON_ERROR;
    }

  /* key does not exist */
  error = btree_insert_new_key (thread_p, btid, page_ptr, key, slot_id);

end:
  assert (error == NO_ERROR || error == ER_BTREE_NO_SPACE);
  return error;

exit_on_error:
  assert (error != NO_ERROR);
  if (error == NO_ERROR)
    {
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
    }

  return error;
}


/*
 * btree_get_prefix () -
 *   return: db_value containing the prefix key.  This must be
 *           cleared when it is done being used.
 *   thread_p(in):
 *   btid(in):
 *   key1(in) : Left side of compare.
 *   key2(in) : Right side of compare.
 *
 * Note: This function finds the prefix (the separator) of two strings.
 * Currently this is only done for one of the six string types,
 * but with multi-column indexes and uniques coming, we may want
 * to do prefix keys for sequences as well.
 *
 * The purpose of this routine is to find a prefix that is
 * greater than or equal to the first key but strictly less
 * than the second key.  This routine assumes that the second
 * key is strictly greater than the first key.
 */
static int
btree_get_prefix (UNUSED_ARG THREAD_ENTRY * thread_p, BTID_INT * btid,
		  const DB_IDXKEY * key1, const DB_IDXKEY * key2,
		  DB_IDXKEY * prefix_key)
{
  int c = DB_UNK;
  int i, d;
  OR_INDEX *indexp = NULL;
  bool dom_is_desc = false, next_dom_is_desc = false;

  /* Assertions */
  assert (btid != NULL);
  assert (btid->classrepr != NULL);
  assert (btid->classrepr_cache_idx != -1);
  assert (btid->indx_id != -1);

  assert (key1 != NULL);
  assert (key2 != NULL);
  assert (!DB_IDXKEY_IS_NULL (key1));
  assert (!DB_IDXKEY_IS_NULL (key2));

  assert (prefix_key != NULL);

  assert (key1->size == key2->size);

  d = 0;
  c = pr_idxkey_compare (key1, key2, -1, &d);
  assert (c != DB_EQ);

  indexp = &(btid->classrepr->indexes[btid->indx_id]);

  if (c == DB_LT || c == DB_GT)
    {
      assert (d < indexp->n_atts + 1);

      if (d < indexp->n_atts)
	{
	  if (indexp->asc_desc[d])
	    {
	      dom_is_desc = true;
	    }

	  if (d + 1 < indexp->n_atts)
	    {
	      if (indexp->asc_desc[d + 1])
		{
		  next_dom_is_desc = true;
		}
	    }
	}

      if (dom_is_desc)
	{
	  c = ((c == DB_GT) ? DB_LT : (c == DB_LT) ? DB_GT : c);
	}
    }

  assert (c == DB_LT);
  if (c != DB_LT)
    {
      return er_errid () == NO_ERROR ? ER_FAILED : er_errid ();
    }

  if (d == key1->size - 1
      || (DB_IS_NULL (&(key1->vals[d + 1]))
	  || DB_IS_NULL (&(key2->vals[d + 1]))))
    {
      /* not found prefix separator: give up */
#if 1				/* TODO - */
      db_idxkey_clone (key1, prefix_key);
#endif
    }
  else
    {
      assert (d < key1->size - 1);
      assert (d < key2->size - 1);

      prefix_key->size = key1->size;
      assert (prefix_key->size > d);

      for (i = 0; i <= d; i++)
	{
	  if (!next_dom_is_desc)
	    {
	      pr_clone_value (&(key2->vals[i]), &(prefix_key->vals[i]));
	    }
	  else
	    {
	      pr_clone_value (&(key1->vals[i]), &(prefix_key->vals[i]));
	    }
	}

      for (; i < prefix_key->size; i++)
	{
	  DB_MAKE_NULL (&(prefix_key->vals[i]));
	}
    }

#if !defined(NDEBUG)
  /* key1 <= prefix_key */
  c = btree_compare_key (thread_p, btid, key1, prefix_key, NULL);
  assert (c == DB_LT || c == DB_EQ);

  /* prefix_key < key2 */
  c = btree_compare_key (thread_p, btid, prefix_key, key2, NULL);
  assert (c == DB_LT);
#endif

  return NO_ERROR;
}

/*
 * btree_find_split_point () -
 *   return: the key or key separator (prefix) to be moved to the
 *           parent page, or NULL_KEY. The length of the returned
 *           key, or prefix, is set in mid_keylen. The parameter
 *           mid_slot is set to the record number of the split point record.
 *   btid(in):
 *   page_ptr(in): Pointer to the page
 *   key(in): Key to be inserted to the index
 *   mid_slot(out): Set to contain the record number for the split point slot
 *   mid_key(out):
 *   clear_midkey(out):
 *
 * Note: Finds the split point of the given page by considering the
 * length of the existing records and the length of the key.
 * For a leaf page split operation, if there are n keys in the
 * page, then mid_slot can be set to :
 *
 *              0 : all the records in the page are to be moved to the newly
 *                  allocated page, key is to be inserted into the original
 *                  page. Mid_key is between key and the first record key.
 *
 *              n : all the records will be kept in the original page. Key is
 *                  to be inserted to the newly allocated page. Mid_key is
 *                  between the last record key and the key.
 *      otherwise : slot point is in the range 1 to n-1, inclusive. The page
 *                  is to be split into half.
 *
 * Note: the returned db_value should be cleared and FREED by the caller.
 */
static int
btree_find_split_point (THREAD_ENTRY * thread_p,
			BTID_INT * btid, PAGE_PTR page_ptr,
			const DB_IDXKEY * key,
			INT16 * mid_slot, DB_IDXKEY * mid_key,
			bool * clear_midkey)
{
  int error = NO_ERROR;
  RECDES rec = RECDES_INITIALIZER;
  int node_type;
  INT16 slot_id;
  int ent_size;
  int key_cnt, key_len;
  int tot_rec, sum, max_split_size;
  int n, i, mid_size;
  bool m_clear_key = false, n_clear_key = false;
  DB_IDXKEY next_key, prefix_key;
  bool found;
  BTREE_NODE_HEADER node_header;
  NON_LEAF_REC nleaf_pnt;
  BTREE_NODE_SPLIT_INFO split_info;

  assert (DB_IDXKEY_IS_NULL (mid_key));

  assert (clear_midkey != NULL);
  assert (*clear_midkey == false);

  DB_IDXKEY_MAKE_NULL (&next_key);

  DB_IDXKEY_MAKE_NULL (&prefix_key);

  /* get the page header */
  error = btree_read_node_header (page_ptr, &node_header);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (node_header.node_level > 1)
    {
      node_type = BTREE_NON_LEAF_NODE;
    }
  else
    {
      node_type = BTREE_LEAF_NODE;
    }
  key_cnt = node_header.key_cnt;
  BTREE_CHECK_KEY_CNT (page_ptr, node_header.node_level, node_header.key_cnt);

  n = spage_number_of_records (page_ptr) - 1;	/* last record position */
  split_info = node_header.split_info;

  if (key_cnt <= 3)
    {
      assert (false);

      er_log_debug (ARG_FILE_LINE,
		    "btree_find_split_point: node key count underflow: %d",
		    key_cnt);
      GOTO_EXIT_ON_ERROR;
    }

  key_len = btree_get_key_length (key);
  assert (BTREE_IS_VALID_KEY_LEN (key_len));

  /* find the slot position of the key if it is to be located in the page */
  if (node_type == BTREE_LEAF_NODE)
    {
      found = btree_search_leaf_page (thread_p, btid, page_ptr,
				      key, &slot_id, NULL);
      assert (found == false);
      if (slot_id == NULL_SLOTID || found == true)	/* leaf search failed */
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }
  else
    {
      slot_id = NULL_SLOTID;
    }

  /* first find out the size of the data on the page, don't count the
   * header record.
   */
  for (i = 1, tot_rec = 0; i <= n; i++)
    {
      tot_rec += spage_get_space_for_record (page_ptr, i);
    }
  max_split_size = BTREE_NODE_MAX_SPLIT_SIZE (page_ptr);

#if 1
  if (node_type == BTREE_LEAF_NODE)
    {
      ent_size = LEAFENTSZ (key_len);
      tot_rec += ent_size;

      mid_size = btree_split_find_pivot (tot_rec, ent_size, &split_info);
    }
  else
    {
      mid_size = btree_split_find_pivot (tot_rec, ONE_K, &split_info);
    }

  if (mid_size > max_split_size)
    {
      assert (false);
      GOTO_EXIT_ON_ERROR;
    }

  for (i = 1, sum = 0; sum < mid_size && i <= n; i++)
    {
      sum += spage_get_space_for_record (page_ptr, i);
    }
#else
  if (node_type == BTREE_LEAF_NODE)
    {				/* take key length into consideration */
      ent_size = LEAFENTSZ (key_len);
      tot_rec += ent_size;

      mid_size = MIN (max_split_size - ent_size,
		      btree_split_find_pivot (tot_rec, ent_size,
					      &split_info));
      for (i = 1, sum = 0; i < slot_id && sum < mid_size; i++)
	{
	  sum += spage_get_space_for_record (page_ptr, i);
	}

      if (sum < mid_size)
	{
	  /* new key insert into left node */
	  sum += ent_size;

	  for (; sum < mid_size && i <= n; i++)
	    {
	      int len = spage_get_space_for_record (page_ptr, i);
	      if (sum + len >= mid_size)
		{
		  break;
		}

	      sum += len;
	    }
	}
      else
	{
	  while (sum < ent_size)
	    {
	      if (i == slot_id)
		{
		  /* new key insert into left node */
		  sum += ent_size;
		}
	      else
		{
		  sum += spage_get_space_for_record (page_ptr, i);
		  i++;
		}
	    }
	}
    }
  else
    {				/* consider only the length of the records in the page */
      mid_size = btree_split_find_pivot (tot_rec, ONE_K, &split_info);
      for (i = 1, sum = 0;
	   sum < mid_size && sum < max_split_size && i <= n; i++)
	{
	  sum += spage_get_space_for_record (page_ptr, i);
	}
    }
#endif

  *mid_slot = i - 1;

  /* We used to have a check here to make sure that the key could be
   * inserted into one of the pages after the split operation.  It must
   * always be the case that the key can be inserted into one of the
   * pages after split because keys can be no larger than
   * ??
   * and the determination of the splitpoint above
   * should always guarantee that both pages have at least that much
   * free (usually closer to half the page, certainly more than 2 *
   * ??.
   */

  if (*mid_slot == n)
    {
      (*mid_slot)--;
    }
  if (*mid_slot == 0)
    {
      (*mid_slot)++;
    }

  if (node_type == BTREE_LEAF_NODE && (*mid_slot) == (slot_id - 1))
    {
      /* the new key is the split key */
      db_idxkey_clone (key, mid_key);

      m_clear_key = false;
    }
  else
    {
      /* the split key is one of the keys on the page */
      if (spage_get_record (page_ptr, *mid_slot, &rec, PEEK) != S_SUCCESS)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      /* we copy the key here because rec lives on the stack and mid_key
       * is returned from this routine.
       */
      if (node_type == BTREE_LEAF_NODE)
	{
	  error = btree_read_record (thread_p, btid, &rec, mid_key,
				     NULL, node_type, &m_clear_key,
				     COPY_KEY_VALUE);
	}
      else
	{
	  error = btree_read_record (thread_p, btid, &rec, mid_key,
				     (void *) &nleaf_pnt, node_type,
				     &m_clear_key, COPY_KEY_VALUE);
	}

      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  /* for separator, should use middle key as it is.
   * should not use the prefix
   */
  if (node_type == BTREE_NON_LEAF_NODE)
    {
      *clear_midkey = true;	/* we must always clear prefix mid key */

      goto done;		/* OK */
    }

  assert (node_type == BTREE_LEAF_NODE);

  /* The determination of the prefix key is dependent on the next key */

  if (spage_get_record (page_ptr, (*mid_slot) + 1, &rec, PEEK) != S_SUCCESS)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* we copy the key here because rec lives on the stack and mid_key
   * is returned from this routine.
   */
  error = btree_read_record (thread_p, btid, &rec, &next_key, NULL,
			     BTREE_LEAF_NODE, &n_clear_key, COPY_KEY_VALUE);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* now that we have the mid key and the next key, we can determine the
   * prefix key.
   */

  assert (btree_get_key_length (mid_key) <= BTREE_MAX_KEYLEN);
  assert (btree_get_key_length (&next_key) <= BTREE_MAX_KEYLEN);

  error = btree_get_prefix (thread_p, btid, mid_key, &next_key, &prefix_key);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  assert (!DB_IDXKEY_IS_NULL (&prefix_key));
  assert (error == NO_ERROR);

  /* replace the mid_key with the prefix_key */
  db_idxkey_clear (mid_key);

  *mid_key = prefix_key;

  *clear_midkey = true;		/* we must always clear prefix mid key */

done:

  btree_clear_key_value (&n_clear_key, &next_key);

  return error;

exit_on_error:

  assert (DB_IDXKEY_IS_NULL (&prefix_key));

  btree_clear_key_value (&m_clear_key, mid_key);

  if (error == NO_ERROR)
    {
      error = er_errid ();
      if (error == NO_ERROR)
	{
	  error = ER_FAILED;
	}
    }

  goto done;
}

/*
 * btree_split_find_pivot () -
 *   return:
 *   total(in):
 *   ent_size(in):
 *   split_info(in):
 */
static int
btree_split_find_pivot (int total, int ent_size,
			BTREE_NODE_SPLIT_INFO * split_info)
{
  int split_point;

  if (split_info->pivot == 0
      || (split_info->pivot > BTREE_SPLIT_LOWER_BOUND
	  && split_info->pivot < BTREE_SPLIT_UPPER_BOUND))
    {
      split_point = CEIL_PTVDIV (total, 2);
    }
  else
    {
      split_point = total * MAX (MIN (split_info->pivot,
				      BTREE_SPLIT_MAX_PIVOT),
				 BTREE_SPLIT_MIN_PIVOT);
    }

  split_point = MIN (split_point, (total - ent_size));
  split_point = MAX (split_point, ent_size);

  return split_point;
}

/*
 * btree_split_node () -
 *   return: NO_ERROR
 *           child_vpid is set to page identifier for the child page to be
 *           followed, Q or R, or the page identifier of a newly allocated
 *           page to insert the key, or NULL_PAGEID. The parameter key is
 *           set to the middle key that will be put into the parent page P.
 *   btid(in): The index identifier
 *   P(in): Page pointer for the parent page of page Q
 *   Q(in): Page pointer for the page to be split
 *   R(in): Page pointer for the newly allocated page
 *   next_page(in):
 *   P_vpid(in): Page identifier for page Q
 *   Q_vpid(in): Page identifier for page Q
 *   R_vpid(in): Page identifier for page R
 *   p_slot_id(in): The slot of parent page P which points to page Q
 *   key(out): Set to contain the middle key of the split operation
 *   child_vpid(out): Set to the child page identifier
 *
 * Note: Page Q is split into two pages: Q and R. The second half of
 * of the page Q is move to page R. The middle key of of the
 * split operation is moved to parent page P. Depending on the
 * split point, the whole page Q may be moved to page R, or the
 * whole page content may be kept in page Q. If the key can not
 * fit into one of the pages after the split, a new page is
 * allocated for the key and its page identifier is returned.
 * The headers of all pages are updated, accordingly.
 */
static int
btree_split_node (THREAD_ENTRY * thread_p, BTID_INT * btid, PAGE_PTR P,
		  PAGE_PTR Q, PAGE_PTR R, VPID * P_vpid,
		  VPID * Q_vpid, VPID * R_vpid, INT16 p_slot_id,
		  const DB_IDXKEY * key, VPID * child_vpid)
{
  short Q_node_type;
  INT16 mid_slot_id;
#if !defined(NDEBUG)
  int nrecs;
#endif
  int key_cnt, leftcnt, rightcnt, right;
  RECDES rec = RECDES_INITIALIZER, trec = RECDES_INITIALIZER;
  NON_LEAF_REC nleaf_rec, nleaf_ptr;
  BTREE_NODE_HEADER qheader, rheader;
  BTREE_NODE_HEADER pheader;
  VPID next_vpid, page_vpid;
  int i, c;
  bool clear_midkey = false;
  DB_IDXKEY mid_key;
//  int q_moved;
  RECSET_HEADER recset_header;	/* for recovery purposes */
  char *recset_data, *datap;	/* for recovery purposes */
  int recset_length;		/* for recovery purposes */
  int ret = NO_ERROR;
  char rec_buf[IO_MAX_PAGE_SIZE + BTREE_MAX_ALIGN];
  char recset_data_buf[IO_MAX_PAGE_SIZE + BTREE_MAX_ALIGN];
  LOG_DATA_ADDR addr = LOG_ADDR_INITIALIZER;
  UINT64 perf_start;

  PERF_MON_GET_CURRENT_TIME (perf_start);

  DB_IDXKEY_MAKE_NULL (&mid_key);

  recset_data = NULL;
  rec.data = NULL;

  /* initialize child page identifier */
  VPID_SET_NULL (child_vpid);

  assert (P != NULL);
  assert (Q != NULL);
  assert (R != NULL);
  assert (!VPID_ISNULL (P_vpid) && !VPID_ISNULL (Q_vpid)
	  && !VPID_ISNULL (R_vpid));

  /* initializations */
  rec.area_size = DB_PAGESIZE;
  rec.data = PTR_ALIGN (rec_buf, BTREE_MAX_ALIGN);
  rec.type = REC_HOME;

#if !defined(NDEBUG)
  nrecs = spage_number_of_records (Q);	/* get the key count of page Q */
#endif

  ret = btree_read_node_header (Q, &qheader);
  if (ret != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  Q_node_type =
    qheader.node_level > 1 ? BTREE_NON_LEAF_NODE : BTREE_LEAF_NODE;

  key_cnt = qheader.key_cnt;

  assert_release (key_cnt >= 0);
  assert ((Q_node_type == BTREE_NON_LEAF_NODE && key_cnt + 2 == nrecs)
	  || (Q_node_type == BTREE_LEAF_NODE && key_cnt + 1 == nrecs));

  if (key_cnt <= 0)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* find the middle record of the page Q  and find the number of
   * keys after split in pages Q and R, respectively
   */
  ret = btree_find_split_point (thread_p, btid, Q, key, &mid_slot_id,
				&mid_key, &clear_midkey);
  if (ret != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (Q_node_type == BTREE_LEAF_NODE)
    {
      leftcnt = mid_slot_id;
      rightcnt = key_cnt - leftcnt;
      assert (leftcnt + rightcnt == key_cnt);
    }
  else
    {
      leftcnt = mid_slot_id - 1;
      rightcnt = key_cnt - leftcnt - 1;
      assert (leftcnt + 1 + rightcnt == key_cnt);
    }

//  q_moved = (mid_slot_id == 0) ? 1 : 0;

  /* log the old header record for undo purposes */
  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, Q, HEADER);
  log_append_undo_data (thread_p, RVBT_NDRECORD_UPD, &addr,
			sizeof (qheader), &qheader);

  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  qheader.key_cnt = leftcnt;
  assert_release (leftcnt >= 0);
  VPID_COPY (&next_vpid, &qheader.next_vpid);

  if (Q_node_type == BTREE_LEAF_NODE)
    {
      VPID_COPY (&qheader.next_vpid, R_vpid);
    }
  else
    {
      VPID_SET_NULL (&qheader.next_vpid);
    }

  qheader.split_info.index = 1;

  ret = btree_write_node_header (Q, &qheader);
  if (ret != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* log the new header record for redo purposes */
  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, Q, HEADER);
  log_append_redo_data (thread_p, RVBT_NDRECORD_UPD, &addr,
			sizeof (qheader), &qheader);

  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  btree_init_node_header (thread_p, &rheader);
  rheader.key_cnt = rightcnt;
  rheader.next_vpid = next_vpid;
  if (Q_node_type == BTREE_LEAF_NODE)
    {
      rheader.prev_vpid = *Q_vpid;
    }
  else
    {
      VPID_SET_NULL (&rheader.prev_vpid);
    }
  rheader.split_info = qheader.split_info;
  rheader.node_level = qheader.node_level;
  ret = btree_insert_node_header (thread_p, R, &rheader);
  if (ret != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* log the new header record for redo purposes, there is no need
     to undo the change to the header record, since the page will be
     deallocated on further undo operations. */
  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, R, HEADER);
  log_append_redo_data (thread_p, RVBT_NDHEADER_INS, &addr,
			sizeof (rheader), &rheader);

  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  /* move second half of page Q to page R */
  right = (Q_node_type == BTREE_LEAF_NODE) ? rightcnt : (rightcnt + 1);

  /* for recovery purposes */
  recset_data = PTR_ALIGN (recset_data_buf, BTREE_MAX_ALIGN);

  /* read the before image of second half of page Q for undo logging */
  ret = btree_rv_util_save_page_records (Q, mid_slot_id + 1, right,
					 mid_slot_id + 1, recset_data,
					 &recset_length);
  if (ret != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* move the second half of page Q to page R */
  for (i = 1; i <= right; i++)
    {
      if (spage_get_record (Q, mid_slot_id + 1, &trec, PEEK) != S_SUCCESS)
	{
	  ret = ER_FAILED;
	  break;
	}
      if (spage_insert_at (thread_p, R, i, &trec) != SP_SUCCESS)
	{
	  ret = ER_FAILED;
	  break;
	}
      if (spage_delete (thread_p, Q, mid_slot_id + 1) != mid_slot_id + 1)
	{
	  ret = ER_FAILED;
	  break;
	}
    }

  if (ret != NO_ERROR)
    {
      if (i > 1)
	{
	  ret = btree_rv_util_save_page_records (R, 1, i - 1, 1,
						 recset_data, &recset_length);
	  if (ret == NO_ERROR)
	    {
	      LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, Q, -1);
	      log_append_undo_data (thread_p, RVBT_DEL_PGRECORDS, &addr,
				    recset_length, recset_data);
	    }
	}
      GOTO_EXIT_ON_ERROR;
    }

  /* for delete redo logging of page Q */
  recset_header.rec_cnt = right;
  recset_header.first_slotid = mid_slot_id + 1;

  /* undo/redo logging for page Q */
  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, Q, -1);
  log_append_undoredo_data (thread_p, RVBT_DEL_PGRECORDS,
			    &addr, recset_length,
			    sizeof (RECSET_HEADER), recset_data,
			    &recset_header);

  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  /* Log the second half of page Q for redo purposes on Page R,
     the records on the second half of page Q will be inserted to page R */
  datap = recset_data;
  ((RECSET_HEADER *) datap)->first_slotid = 1;
  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, R, -1);
  log_append_redo_data (thread_p, RVBT_INS_PGRECORDS, &addr,
			recset_length, recset_data);

  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  /* update parent page P */
  if (spage_get_record (P, p_slot_id, &rec, COPY) != S_SUCCESS)
    {
      ret = er_errid ();

      GOTO_EXIT_ON_ERROR;
    }

  /* log the old node record for undo purposes */
  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, P, p_slot_id);
  log_append_undo_data (thread_p, RVBT_NDRECORD_UPD, &addr,
			rec.length, rec.data);

  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  btree_read_fixed_portion_of_non_leaf_record (&rec, &nleaf_ptr);
  nleaf_ptr.pnt = *R_vpid;
  btree_write_fixed_portion_of_non_leaf_record (&rec, &nleaf_ptr);
  if (spage_update (thread_p, P, p_slot_id, &rec) != SP_SUCCESS)
    {
      ret = er_errid ();

      GOTO_EXIT_ON_ERROR;
    }

  /* log the new node record for redo purposes */
  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, P, p_slot_id);
  log_append_redo_data (thread_p, RVBT_NDRECORD_UPD, &addr,
			rec.length, rec.data);

  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  /* update the parent page P to keep the middle key and to point to
   * pages Q and R.  Remember that this mid key will be on a non leaf page
   * regardless of whether we are splitting a leaf or non leaf page.
   */
  nleaf_rec.pnt = *Q_vpid;

  ret = btree_write_record (thread_p, btid, &nleaf_rec, &mid_key,
			    BTREE_NON_LEAF_NODE, &rec);
  if (ret != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* log the inserted record for both undo and redo purposes,
   * in the case of undo, the inserted record at p_slot_id will be deleted,
   * in the case of redo, the record will be inserted at p_slot_id
   */
  if (spage_insert_at (thread_p, P, p_slot_id, &rec) != SP_SUCCESS)
    {
      assert (false);
      ret = er_errid ();

      GOTO_EXIT_ON_ERROR;
    }

  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, P, p_slot_id);
  log_append_undoredo_data (thread_p, RVBT_NDRECORD_INS, &addr,
			    sizeof (p_slot_id), rec.length, &p_slot_id,
			    rec.data);

  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  if (BTREE_IS_ROOT_PAGE (btid->sys_btid, P_vpid))
    {
      ret = btree_read_node_header (P, &pheader);
      if (ret != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      /* log the old header record for undo purposes */
      LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, P, HEADER);
      log_append_undo_data (thread_p, RVBT_NDRECORD_UPD, &addr,
			    sizeof (pheader), &pheader);
    }
  else
    {
      ret = btree_read_node_header (P, &pheader);
      if (ret != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      /* log the old header record for undo purposes */
      LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, P, HEADER);
      log_append_undo_data (thread_p, RVBT_NDRECORD_UPD, &addr,
			    sizeof (pheader), &pheader);
    }

  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  assert_release (pheader.key_cnt >= 0);
  pheader.key_cnt++;
  assert_release (pheader.key_cnt >= 1);
  assert (pheader.node_level > 1);
  BTREE_CHECK_KEY_CNT (P, pheader.node_level, pheader.key_cnt);
  assert_release (pheader.split_info.pivot >= 0);
  assert_release (pheader.key_cnt > 0);
  btree_split_next_pivot (&pheader.split_info,
			  (float) p_slot_id / pheader.key_cnt,
			  pheader.key_cnt);

  if (BTREE_IS_ROOT_PAGE (btid->sys_btid, P_vpid))
    {
      ret = btree_write_node_header (P, &pheader);
      if (ret != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      /* log the new header record for redo purposes */
      LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, P, HEADER);
      log_append_redo_data (thread_p, RVBT_NDRECORD_UPD,
			    &addr, sizeof (pheader), &pheader);
    }
  else
    {
      ret = btree_write_node_header (P, &pheader);
      if (ret != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      /* log the new header record for redo purposes */
      LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, P, HEADER);
      log_append_redo_data (thread_p, RVBT_NDRECORD_UPD,
			    &addr, sizeof (pheader), &pheader);
    }

  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  /* find the child page to be followed */

  c = btree_compare_key (thread_p, btid, key, &mid_key, NULL);
  if (c == DB_UNK)
    {
      assert (false);
      GOTO_EXIT_ON_ERROR;
    }

  if (c <= 0)
    {
      page_vpid = *Q_vpid;
    }
  else
    {
      page_vpid = *R_vpid;
    }

#if !defined(NDEBUG)
  {
    int key_len, max_free;
    OR_INDEX *indexp = NULL;
    DB_IDXKEY left_max, right_min;

    DB_IDXKEY_MAKE_NULL (&left_max);
    DB_IDXKEY_MAKE_NULL (&right_min);

    assert (btid->classrepr != NULL);
    assert (btid->classrepr_cache_idx != -1);
    assert (btid->indx_id != -1);

    indexp = &(btid->classrepr->indexes[btid->indx_id]);

    /* get max key from Q_vpid */
    ret = btree_find_min_or_max_key (thread_p, &(btid->cls_oid),
				     btid->sys_btid, Q_vpid, &left_max,
				     indexp->asc_desc[0] ? true : false);
    if (ret != NO_ERROR)
      {
	assert (false);
	GOTO_EXIT_ON_ERROR;
      }

    /* check iff narrow right fence */
    ret = btree_fence_check_key (thread_p, btid, &left_max, &mid_key, true);
    if (ret != NO_ERROR)
      {
	GOTO_EXIT_ON_ERROR;
      }

    /* get min key from R_vpid */
    ret = btree_find_min_or_max_key (thread_p, &(btid->cls_oid),
				     btid->sys_btid,
				     R_vpid, &right_min,
				     indexp->asc_desc[0] ? false : true);
    if (ret != NO_ERROR)
      {
	assert (false);
	GOTO_EXIT_ON_ERROR;
      }

    /* check iff narrow left fence */
    ret = btree_fence_check_key (thread_p, btid, &mid_key, &right_min, false);
    if (ret != NO_ERROR)
      {
	GOTO_EXIT_ON_ERROR;
      }

    if (c <= 0)
      {
	max_free = spage_max_space_for_new_record (thread_p, Q);
      }
    else
      {
	max_free = spage_max_space_for_new_record (thread_p, R);
      }

    if (Q_node_type == BTREE_NON_LEAF_NODE)
      {
	key_len = ONE_K;	/* guess */
      }
    else
      {
	key_len = btree_get_key_length (key);	/* TODO - */
      }

    assert (max_free > key_len);
  }
#endif

  ret = FI_TEST_ARG_INT (thread_p, FI_TEST_BTREE_MANAGER_SPLIT_ERROR1, 0, 0);
  if (ret != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  btree_clear_key_value (&clear_midkey, &mid_key);

  pgbuf_set_dirty (thread_p, P, DONT_FREE);
  pgbuf_set_dirty (thread_p, Q, DONT_FREE);
  pgbuf_set_dirty (thread_p, R, DONT_FREE);

  /* set child page pointer */
  *child_vpid = page_vpid;

  mnt_stats_counter_with_time (thread_p, MNT_STATS_BTREE_SPLITS, 1,
			       perf_start);

  return ret;

exit_on_error:

  btree_clear_key_value (&clear_midkey, &mid_key);

  if (ret == NO_ERROR)
    {
      assert (false);
      ret = ER_GENERIC_ERROR;
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1,
	      "");
    }

  return ret;
}

/*
 * btree_split_root () -
 *   return: NO_ERROR
 *           child_vpid parameter is set to the child page to be followed
 *           after the split operation, or the page identifier of a newly
 *           allocated page for future key insertion, or NULL_PAGEID.
 *           The parameter key is set to the middle key of the split operation.
 *   btid(in): B+tree index identifier
 *   P(in): Page pointer for the root to be split
 *   Q(in): Page pointer for the newly allocated page
 *   R(in): Page pointer for the newly allocated page
 *   P_page_vpid(in): Page identifier for root page P
 *   Q_page_vpid(in): Page identifier for page Q
 *   R_page_vpid(in): Page identifier for page R
 *   key(out): Set to contain the middle key of the split operation
 *   child_vpid(out): Set to the child page identifier
 *
 * Note: The root page P is split into two pages: Q and R. In order
 * not to change the actual root page, the first half of the page
 * is moved to page Q and the second half is moved to page R.
 * Depending on the split point found, the whole root page may be
 * moved to Q, or R, leaving the other one empty for future  key
 * insertion. If the key cannot fit into either Q or R after the
 * split, a new page is allocated and its page identifier is
 * returned. Two new records are formed within root page to point
 * to pages Q and R. The headers of all pages are updated.
 */
static int
btree_split_root (THREAD_ENTRY * thread_p, BTID_INT * btid, PAGE_PTR P,
		  PAGE_PTR Q, PAGE_PTR R, UNUSED_ARG VPID * P_vpid,
		  VPID * Q_vpid, VPID * R_vpid,
		  const DB_IDXKEY * key, VPID * child_vpid)
{
  INT16 mid_slot_id;
  int nrecs, key_cnt, leftcnt, rightcnt, right, left;
  RECDES rec = RECDES_INITIALIZER, peek_rec = RECDES_INITIALIZER;
  NON_LEAF_REC nleaf_rec;
  BTREE_NODE_HEADER qheader, rheader;
  BTREE_NODE_HEADER root_header;
  int i, c;
  bool clear_midkey = false;
  DB_IDXKEY mid_key;
  VPID page_vpid;
//  int q_moved, r_moved;
  char *recset_data;		/* for recovery purposes */
  RECSET_HEADER recset_header;	/* for recovery purposes */
  int recset_length;		/* for recovery purposes */
  int sp_success;
  PGLENGTH log_addr_offset;
  int ret = NO_ERROR;
  char rec_buf[IO_MAX_PAGE_SIZE + BTREE_MAX_ALIGN];
  char recset_data_buf[IO_MAX_PAGE_SIZE + BTREE_MAX_ALIGN];
  BTREE_NODE_SPLIT_INFO split_info;
  LOG_DATA_ADDR addr = LOG_ADDR_INITIALIZER;
  UINT64 perf_start;

  PERF_MON_GET_CURRENT_TIME (perf_start);

  DB_IDXKEY_MAKE_NULL (&mid_key);

  recset_data = NULL;
  rec.data = NULL;

  /* initialize child page identifier */
  VPID_SET_NULL (child_vpid);

  assert (P != NULL);
  assert (Q != NULL);
  assert (R != NULL);
  assert (!VPID_ISNULL (P_vpid));
  assert (!VPID_ISNULL (Q_vpid));
  assert (!VPID_ISNULL (R_vpid));

  /* initializations */
  rec.area_size = DB_PAGESIZE;
  rec.data = PTR_ALIGN (rec_buf, BTREE_MAX_ALIGN);
  rec.type = REC_HOME;

  /* log the whole root page P for undo purposes. */
  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, P, -1);
  log_append_undo_data (thread_p, RVBT_COPYPAGE, &addr, DB_PAGESIZE, P);

  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  nrecs = spage_number_of_records (P);

  /* get the number of keys in the root page P */
  if (btree_read_node_header (P, &root_header) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  assert (root_header.node_level > 1);

  /* root is always a non leaf node, the number of keys is actually one greater
   */
  key_cnt = root_header.key_cnt;
  assert_release (key_cnt > 0);
  assert (key_cnt + 2 == nrecs);
  if (key_cnt <= 0)
    {
      GOTO_EXIT_ON_ERROR;
    }

  split_info = root_header.split_info;
  split_info.index = 1;

  /* find the middle record of the root page and find the number of
   * keys in pages Q and R, respectively
   */
  if (btree_find_split_point
      (thread_p, btid, P, key, &mid_slot_id, &mid_key,
       &clear_midkey) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  leftcnt = mid_slot_id - 1;
  rightcnt = key_cnt - leftcnt - 1;
  assert (leftcnt + 1 + rightcnt == key_cnt);

#if 0
  q_moved = (mid_slot_id == (nrecs - 1)) ? 1 : 0;
  r_moved = (mid_slot_id == 0) ? 1 : 0;
#endif

  /* update root page P header */
  root_header.key_cnt = 1;

  root_header.split_info.pivot = BTREE_SPLIT_DEFAULT_PIVOT;
  root_header.split_info.index = 1;
  root_header.node_level++;
  assert (root_header.node_level > 2);

  if (btree_write_node_header (P, &root_header) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* log the new header record for redo purposes */
  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, P, HEADER);
  log_append_redo_data (thread_p, RVBT_NDRECORD_UPD, &addr,
			sizeof (root_header), &root_header);

  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  /* update page Q header */
  btree_init_node_header (thread_p, &qheader);
  qheader.key_cnt = leftcnt;
  qheader.split_info = split_info;
  qheader.node_level = root_header.node_level - 1;
  assert (qheader.node_level > 1);

  ret = btree_insert_node_header (thread_p, Q, &qheader);
  if (ret != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* log the new header record for redo purposes */
  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, Q, HEADER);
  log_append_redo_data (thread_p, RVBT_NDHEADER_INS, &addr,
			sizeof (qheader), &qheader);

  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  /* update page R header */
  btree_init_node_header (thread_p, &rheader);
  rheader.key_cnt = rightcnt;
  VPID_SET_NULL (&rheader.next_vpid);
  rheader.prev_vpid = *Q_vpid;
  rheader.split_info = split_info;
  rheader.node_level = root_header.node_level - 1;
  assert (rheader.node_level > 1);

  ret = btree_insert_node_header (thread_p, R, &rheader);
  if (ret != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* log the new header record for redo purposes */
  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, R, HEADER);
  log_append_redo_data (thread_p, RVBT_NDHEADER_INS, &addr,
			sizeof (rheader), &rheader);

  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  /* move the second half of root page P to page R */
  right = rightcnt + 1;
  for (i = 1; i <= right; i++)
    {
      if (spage_get_record (P, mid_slot_id + 1, &peek_rec, PEEK) != S_SUCCESS)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      sp_success = spage_insert_at (thread_p, R, i, &peek_rec);
      if (sp_success != SP_SUCCESS)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      if (spage_delete (thread_p, P, mid_slot_id + 1) != mid_slot_id + 1)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  /* for recovery purposes */
  recset_data = PTR_ALIGN (recset_data_buf, BTREE_MAX_ALIGN);

  /* Log page R records for redo purposes */
  ret = btree_rv_util_save_page_records (R, 1, right, 1, recset_data,
					 &recset_length);
  if (ret != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, R, -1);
  log_append_redo_data (thread_p, RVBT_INS_PGRECORDS, &addr,
			recset_length, recset_data);

  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  /* move the first half of root page P to page Q */
  left = leftcnt + 1;
  for (i = 1; i <= left; i++)
    {
      if (spage_get_record (P, 1, &peek_rec, PEEK) != S_SUCCESS)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      sp_success = spage_insert_at (thread_p, Q, i, &peek_rec);
      if (sp_success != SP_SUCCESS)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      if (spage_delete (thread_p, P, 1) != 1)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  /* Log page Q records for redo purposes */
  ret = btree_rv_util_save_page_records (Q, 1, left, 1, recset_data,
					 &recset_length);
  if (ret != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, Q, -1);
  log_append_redo_data (thread_p, RVBT_INS_PGRECORDS, &addr,
			recset_length, recset_data);

  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  /* Log deletion of all page P records (except the header!!)
   * for redo purposes
   */
  recset_header.rec_cnt = nrecs - 1;
  recset_header.first_slotid = 1;
  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, P, -1);
  log_append_redo_data (thread_p, RVBT_DEL_PGRECORDS, &addr,
			sizeof (RECSET_HEADER), &recset_header);

  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  /* update the root page P to keep the middle key and to point to
   * page Q and R.  Remember that this mid key will be on a non leaf page
   * regardless of whether we are splitting a leaf or non leaf page.
   */
  nleaf_rec.pnt = *Q_vpid;

  ret =
    btree_write_record (thread_p, btid, &nleaf_rec, &mid_key,
			BTREE_NON_LEAF_NODE, &rec);
  if (ret != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (spage_insert_at (thread_p, P, 1, &rec) != SP_SUCCESS)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* log the inserted record for undo/redo purposes */
  log_addr_offset = 1;
  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, P, log_addr_offset);
  log_append_undoredo_data (thread_p, RVBT_NDRECORD_INS, &addr,
			    sizeof (log_addr_offset), rec.length,
			    &log_addr_offset, rec.data);

  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  nleaf_rec.pnt = *R_vpid;

  ret =
    btree_write_record (thread_p, btid, &nleaf_rec, &mid_key,
			BTREE_NON_LEAF_NODE, &rec);
  if (ret != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (spage_insert_at (thread_p, P, 2, &rec) != SP_SUCCESS)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* log the inserted record for undo/redo purposes */
  log_addr_offset = 2;
  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, P, log_addr_offset);
  log_append_undoredo_data (thread_p, RVBT_NDRECORD_INS, &addr,
			    sizeof (log_addr_offset), rec.length,
			    &log_addr_offset, rec.data);

  FI_TEST (thread_p, FI_TEST_BTREE_MANAGER_RANDOM_EXIT, 0);

  /* find the child page to be followed */

  c = btree_compare_key (thread_p, btid, key, &mid_key, NULL);
  if (c == DB_UNK)
    {
#if 1
      assert (false);
#endif
      GOTO_EXIT_ON_ERROR;
    }

  if (c <= 0)
    {
      page_vpid = *Q_vpid;
    }
  else
    {
      page_vpid = *R_vpid;
    }

#if !defined(NDEBUG)
  {
    OR_INDEX *indexp = NULL;
    DB_IDXKEY left_max, right_min;

    DB_IDXKEY_MAKE_NULL (&left_max);
    DB_IDXKEY_MAKE_NULL (&right_min);

    assert (btid->classrepr != NULL);
    assert (btid->classrepr_cache_idx != -1);
    assert (btid->indx_id != -1);

    indexp = &(btid->classrepr->indexes[btid->indx_id]);

    /* get max key from Q_vpid */
    if (btree_find_min_or_max_key (thread_p, &(btid->cls_oid),
				   btid->sys_btid,
				   Q_vpid, &left_max,
				   indexp->asc_desc[0] ? true : false) !=
	NO_ERROR)
      {
	assert (false);
	GOTO_EXIT_ON_ERROR;
      }

    /* check iff narrow right fence */
    ret = btree_fence_check_key (thread_p, btid, &left_max, &mid_key, true);
    if (ret != NO_ERROR)
      {
	GOTO_EXIT_ON_ERROR;
      }

    /* get min key from R_vpid */
    if (btree_find_min_or_max_key (thread_p, &(btid->cls_oid),
				   btid->sys_btid,
				   R_vpid, &right_min,
				   indexp->asc_desc[0] ? false : true) !=
	NO_ERROR)
      {
	assert (false);
	GOTO_EXIT_ON_ERROR;
      }

    /* check iff narrow left fence */
    ret = btree_fence_check_key (thread_p, btid, &mid_key, &right_min, false);
    if (ret != NO_ERROR)
      {
	GOTO_EXIT_ON_ERROR;
      }
  }
#endif
  ret = FI_TEST_ARG_INT (thread_p, FI_TEST_BTREE_MANAGER_ROOT_SPLIT_ERROR1,
			 0, 0);
  if (ret != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  btree_clear_key_value (&clear_midkey, &mid_key);

  pgbuf_set_dirty (thread_p, P, DONT_FREE);
  pgbuf_set_dirty (thread_p, Q, DONT_FREE);
  pgbuf_set_dirty (thread_p, R, DONT_FREE);

  /* set child page identifier */
  *child_vpid = page_vpid;

exit_on_end:

  mnt_stats_counter_with_time (thread_p, MNT_STATS_BTREE_SPLITS, 1,
			       perf_start);

  return ret;

exit_on_error:

  btree_clear_key_value (&clear_midkey, &mid_key);

  if (ret == NO_ERROR)
    {
      assert (false);
      ret = ER_GENERIC_ERROR;
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1,
	      "");
    }

  goto exit_on_end;
}

/*
 * btree_insert () -
 *   return: (the key to be inserted or NULL)
 *   btid(in): B+tree index identifier
 *   key(in): Key to be inserted
 *
 */
DB_IDXKEY *
btree_insert (THREAD_ENTRY * thread_p, BTID_INT * btid, DB_IDXKEY * key)
{
  VPID P_vpid, Q_vpid, R_vpid, child_vpid;
  PAGE_PTR P = NULL, Q = NULL, R = NULL, next_page = NULL;
  BTREE_NODE_HEADER pheader, qheader;
  int key_len;
  INT16 p_slot_id;
  int top_op_active = 0;
  int max_free, new_max_free;
#if 1
  PAGEID_STRUCT pageid_struct;	/* for recovery purposes */
  LOG_DATA_ADDR addr = LOG_ADDR_INITIALIZER;
#endif
  int ret_val;
  short node_type;
  INT16 key_cnt;
  VPID next_vpid;
#if defined(SERVER_MODE)
  bool old_check_interrupt;
#endif /* SERVER_MODE */
  short split_level, root_level;
  int P_req_mode, Q_req_mode;
  UINT64 perf_start;

  assert (btid->sys_btid != NULL);
  assert (!OID_ISNULL (&(btid->cls_oid)));
  assert (btid->classrepr != NULL);
#if !defined(NDEBUG)
  assert (DB_IDXKEY_IS_NULL (&(btid->left_fence)));
  assert (DB_IDXKEY_IS_NULL (&(btid->right_fence)));
#endif

  PERF_MON_GET_CURRENT_TIME (perf_start);

  assert (key != NULL);
  if (key == NULL)
    {
      return key;
    }

  assert (file_is_new_file (thread_p, &(btid->sys_btid->vfid)) ==
	  FILE_OLD_FILE);

  assert (!BTREE_INVALID_INDEX_ID (btid->sys_btid));

  FI_RESET (thread_p, FI_TEST_BTREE_MANAGER_ALLOC_PAGE_ERROR1);
  FI_RESET (thread_p, FI_TEST_BTREE_MANAGER_ALLOC_PAGE_ERROR2);
  FI_RESET (thread_p, FI_TEST_BTREE_MANAGER_ALLOC_PAGE_ERROR3);

  key_len = btree_get_key_length (key);	/* TODO - */
  if (!BTREE_IS_VALID_KEY_LEN (key_len))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_BTREE_MAX_KEYLEN, 2,
	      key_len, BTREE_MAX_KEYLEN);

      return NULL;
    }

#if defined(SERVER_MODE)
  old_check_interrupt = thread_set_check_interrupt (thread_p, false);
#endif /* SERVER_MODE */

  /* init */
  P_req_mode = PGBUF_LATCH_READ;
  split_level = 1;

start_point:

#if !defined(NDEBUG)
  (void) db_idxkey_clear (&(btid->left_fence));
  (void) db_idxkey_clear (&(btid->right_fence));
#endif

  P_vpid.volid = btid->sys_btid->vfid.volid;	/* read the root page */
  P_vpid.pageid = btid->sys_btid->root_pageid;

#if 0				/* dbg - print */
  if (split_level > 1)		/* is retry */
    {
      fprintf (stdout, "%c", P_req_mode == PGBUF_LATCH_WRITE ? 'X' : 'r');
    }
#endif

  P = btree_pgbuf_fix (thread_p, &(btid->sys_btid->vfid), &P_vpid,
		       P_req_mode, PGBUF_UNCONDITIONAL_LATCH);
  if (P == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }
  BTREE_STATS_ADD_WAIT_TIME (PAGE_BTREE_ROOT);

  /* free space in the root node */
  max_free = spage_max_space_for_new_record (thread_p, P);

  /* read the header record */
  if (btree_read_node_header (P, &pheader) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  key_cnt = pheader.key_cnt;
  assert (key_cnt >= 0);

  root_level = pheader.node_level;
  assert (root_level > 1);
  node_type = BTREE_NON_LEAF_NODE;

  if (P_req_mode == PGBUF_LATCH_WRITE)
    {
      split_level = root_level;	/* reset */
    }

  new_max_free = max_free - ONE_K;	/* guess */

  /* So, root is always a non leaf node,
   * the number of keys is actually one greater
   */
  key_cnt += 1;

  BTREE_CHECK_KEY_CNT (P, pheader.node_level, pheader.key_cnt);

  /* there is a need to split the root, only if there is not enough space
   * for a new entry and either there are more than one record.
   *
   * in no case should a split happen if the node is currently empty
   * (key_cnt == 1).
   */
  if (P_req_mode == PGBUF_LATCH_WRITE && key_cnt > 1 && new_max_free < 0)
    {
      assert (node_type == BTREE_NON_LEAF_NODE);

      /* start system top operation */
      log_start_system_op (thread_p);
      top_op_active = 1;

      FI_SET (thread_p, FI_TEST_BTREE_MANAGER_ALLOC_PAGE_ERROR2, 1);

      /* get two new pages */
      Q = btree_get_new_page (thread_p, btid, &Q_vpid, &P_vpid);
      if (Q == NULL)
	{
	  GOTO_EXIT_ON_ERROR;
	}

#if 1				/* TODO:[happy:remove "#if 0" when postpone op. bug fixed] */

      /* log the newly allocated pageid for deallocation for undo purposes */
      pageid_struct.vpid = Q_vpid;
      pageid_struct.vfid.fileid = btid->sys_btid->vfid.fileid;
      pageid_struct.vfid.volid = btid->sys_btid->vfid.volid;
      LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, NULL, -1);
      log_append_undo_data (thread_p, RVBT_NEW_PGALLOC, &addr,
			    sizeof (PAGEID_STRUCT), &pageid_struct);
#endif

      FI_SET (thread_p, FI_TEST_BTREE_MANAGER_ALLOC_PAGE_ERROR3, 1);

      R = btree_get_new_page (thread_p, btid, &R_vpid, &P_vpid);
      if (R == NULL)
	{
	  GOTO_EXIT_ON_ERROR;
	}

#if 1				/* TODO:[happy:remove "#if 0" when postpone op. bug fixed] */

      /* log the newly allocated pageid for deallocation for undo purposes */
      pageid_struct.vpid = R_vpid;
      assert (pageid_struct.vfid.fileid == btid->sys_btid->vfid.fileid);
      assert (pageid_struct.vfid.volid == btid->sys_btid->vfid.volid);

      LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, NULL, -1);
      log_append_undo_data (thread_p, RVBT_NEW_PGALLOC, &addr,
			    sizeof (PAGEID_STRUCT), &pageid_struct);
#endif

      /* split the root P into two pages Q and R */
      if (btree_split_root (thread_p, btid, P, Q, R, &P_vpid, &Q_vpid,
			    &R_vpid, key, &child_vpid) != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

#if !defined(NDEBUG)
      (void) spage_check_num_slots (thread_p, P);
      (void) spage_check_num_slots (thread_p, Q);
      (void) spage_check_num_slots (thread_p, R);
#endif

      log_end_system_op (thread_p, LOG_RESULT_TOPOP_COMMIT);
      top_op_active = 0;

      if (VPID_EQ (&child_vpid, &Q_vpid))
	{
	  /* child page to be followed is page Q */
	  pgbuf_unfix_and_init (thread_p, R);
	}
      else if (VPID_EQ (&child_vpid, &R_vpid))
	{
	  /* child page to be followed is page R */
	  pgbuf_unfix_and_init (thread_p, Q);

	  Q = R;
	  R = NULL;
	  Q_vpid = R_vpid;
	}
      else
	{
	  assert (false);	/* is error ? */

	  pgbuf_unfix_and_init (thread_p, R);
	  pgbuf_unfix_and_init (thread_p, Q);
	  pgbuf_unfix_and_init (thread_p, P);

	  GOTO_EXIT_ON_ERROR;
	}

      /* release parent page P, and repeat the same operations from child
       * page Q on
       */
      pgbuf_unfix_and_init (thread_p, P);
      P = Q;
      Q = NULL;
      P_vpid = Q_vpid;
    }

  /* get the header record */
  if (btree_read_node_header (P, &pheader) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  assert (pheader.node_level > 1);

  node_type = BTREE_NON_LEAF_NODE;
  key_cnt = pheader.key_cnt;
  assert (key_cnt >= 0);
  BTREE_CHECK_KEY_CNT (P, pheader.node_level, pheader.key_cnt);
  VPID_COPY (&next_vpid, &pheader.next_vpid);

  while (node_type == BTREE_NON_LEAF_NODE)
    {
      assert (VPID_ISNULL (&pheader.next_vpid));

      /* find and get the child page to be followed */
      if (btree_search_nonleaf_page (thread_p, btid, P, key,
				     &p_slot_id, &Q_vpid) != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      if (split_level == pheader.node_level - 1)
	{
	  Q_req_mode = PGBUF_LATCH_WRITE;
	}
      else
	{
	  Q_req_mode = P_req_mode;
	}

      assert (Q_req_mode == PGBUF_LATCH_READ
	      || split_level >= pheader.node_level - 1);
      assert (P_req_mode == PGBUF_LATCH_READ
	      || Q_req_mode == PGBUF_LATCH_WRITE);

      Q = btree_pgbuf_fix (thread_p, &(btid->sys_btid->vfid), &Q_vpid,
			   Q_req_mode, PGBUF_UNCONDITIONAL_LATCH);
      if (Q == NULL)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      if (pheader.node_level > 2)
	{
	  /* Q is non leaf node */
	  BTREE_STATS_ADD_WAIT_TIME (PAGE_BTREE_NON_LEAF);
	}
      else
	{
	  /* Q is leaf node */
	  assert (Q_req_mode == PGBUF_LATCH_WRITE);
	  BTREE_STATS_ADD_WAIT_TIME (PAGE_BTREE_LEAF);
	}

      max_free = spage_max_space_for_new_record (thread_p, Q);

      /* read the header record */
      if (btree_read_node_header (Q, &qheader) != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      assert (pheader.node_level - 1 == qheader.node_level);
      node_type =
	qheader.node_level > 1 ? BTREE_NON_LEAF_NODE : BTREE_LEAF_NODE;
      key_cnt = qheader.key_cnt;
      assert (key_cnt >= 0);
      /* if Q is a non leaf node, the number of keys is actually one greater */
      key_cnt = (node_type == BTREE_LEAF_NODE) ? key_cnt : key_cnt + 1;
      BTREE_CHECK_KEY_CNT (Q, qheader.node_level, qheader.key_cnt);

      if (node_type == BTREE_NON_LEAF_NODE)
	{
	  new_max_free = max_free - ONE_K;	/* guess */
	}
      else
	{
	  new_max_free = max_free - (key_len + BTREE_MAX_ALIGN);
	}

      assert ((split_level < pheader.node_level - 1
	       && P_req_mode == PGBUF_LATCH_READ
	       && Q_req_mode == PGBUF_LATCH_READ)
	      || (split_level == pheader.node_level - 1
		  && P_req_mode == PGBUF_LATCH_READ
		  && Q_req_mode == PGBUF_LATCH_WRITE)
	      || (split_level > pheader.node_level - 1
		  && P_req_mode == PGBUF_LATCH_WRITE
		  && Q_req_mode == PGBUF_LATCH_WRITE));

#if 1
      if (split_level == pheader.node_level - 1
	  && key_cnt > 0 && new_max_free < 0)
	{
	  pgbuf_unfix_and_init (thread_p, Q);
	  pgbuf_unfix_and_init (thread_p, P);

	  assert (P_req_mode == PGBUF_LATCH_READ);
	  assert (Q_req_mode == PGBUF_LATCH_WRITE);

#if 0				/* dbg - print */
	  if (split_level == 1)
	    {
	      fprintf (stdout, "I.%d", root_level);	/* first retry */
	    }
#endif

#if 1				/* TODO - PRM */
	  split_level++;
	  assert (split_level == pheader.node_level);
#else
	  split_level = root_level;
#endif

	  assert (split_level <= root_level);
	  if (split_level >= root_level)
	    {
	      P_req_mode = PGBUF_LATCH_WRITE;
	    }
	  else
	    {
	      P_req_mode = PGBUF_LATCH_READ;
	    }
	  goto start_point;
	}
#endif

      /* there is a need to split Q, only if there is not enough space
       * for a new entry and either there are more than one record.
       *
       * in no case should a split happen if the node is currently empty
       * (key_cnt == 0).
       */
      if (P_req_mode == PGBUF_LATCH_WRITE && Q_req_mode == PGBUF_LATCH_WRITE
	  && key_cnt > 0 && new_max_free < 0)
	{
	  assert (split_level >= pheader.node_level);

	  /* start system top operation */
	  log_start_system_op (thread_p);
	  top_op_active = 1;

	  FI_SET (thread_p, FI_TEST_BTREE_MANAGER_ALLOC_PAGE_ERROR1, 1);

	  /* split the page Q into two pages Q and R, and update parent page P */
	  R = btree_get_new_page (thread_p, btid, &R_vpid, &Q_vpid);
	  if (R == NULL)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

#if 1				/* TODO:[happy:remove "#if 0" when postpone op. bug fixed] */
	  /* Log the newly allocated pageid for deallocation for undo purposes */
	  pageid_struct.vpid = R_vpid;
	  pageid_struct.vfid.fileid = btid->sys_btid->vfid.fileid;
	  pageid_struct.vfid.volid = btid->sys_btid->vfid.volid;
	  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, NULL, -1);
	  log_append_undo_data (thread_p, RVBT_NEW_PGALLOC, &addr,
				sizeof (PAGEID_STRUCT), &pageid_struct);
#endif

	  if (btree_split_node (thread_p, btid, P, Q, R,
				&P_vpid, &Q_vpid, &R_vpid, p_slot_id,
				key, &child_vpid) != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

#if !defined(NDEBUG)
	  (void) spage_check_num_slots (thread_p, P);
	  (void) spage_check_num_slots (thread_p, Q);
	  (void) spage_check_num_slots (thread_p, R);
#endif

	  next_page = NULL;
	  if (node_type == BTREE_LEAF_NODE)
	    {
	      assert (next_page == NULL);

	      next_page = btree_get_next_page (thread_p, btid, R);
	      if (next_page != NULL)
		{
		  if (btree_set_vpid_previous_vpid (thread_p, btid,
						    next_page,
						    &R_vpid) != NO_ERROR)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }
		}
	    }

	  log_end_system_op (thread_p, LOG_RESULT_TOPOP_COMMIT);
	  top_op_active = 0;

	  if (VPID_EQ (&child_vpid, &Q_vpid))
	    {
	      /* child page to be followed is Q */
	      pgbuf_unfix_and_init (thread_p, R);
	    }
	  else if (VPID_EQ (&child_vpid, &R_vpid))
	    {
	      /* child page to be followed is R */
	      pgbuf_unfix_and_init (thread_p, Q);

	      Q = R;
	      R = NULL;
	      Q_vpid = R_vpid;
	    }
	  else
	    {
	      assert (false);	/* is error ? */

	      pgbuf_unfix_and_init (thread_p, Q);
	      pgbuf_unfix_and_init (thread_p, R);
	      pgbuf_unfix_and_init (thread_p, P);

	      if (next_page)
		{
		  pgbuf_unfix_and_init (thread_p, next_page);
		}

	      GOTO_EXIT_ON_ERROR;
	    }

	  if (next_page)
	    {
	      pgbuf_unfix_and_init (thread_p, next_page);
	    }
	}

      /* release parent page P, and repeat the same operations from child
       * page Q on
       */
      pgbuf_unfix_and_init (thread_p, P);
      P = Q;
      Q = NULL;
      P_vpid = Q_vpid;

      /* node_type must be recalculated */
      if (btree_read_node_header (P, &pheader) != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      node_type =
	pheader.node_level > 1 ? BTREE_NON_LEAF_NODE : BTREE_LEAF_NODE;
      key_cnt = pheader.key_cnt;
      assert_release (key_cnt >= 0);
      BTREE_CHECK_KEY_CNT (P, pheader.node_level, pheader.key_cnt);
      VPID_COPY (&next_vpid, &pheader.next_vpid);

      if (node_type == BTREE_NON_LEAF_NODE)
	{
	  P_req_mode = Q_req_mode;
	}
    }				/* while */

  assert (pheader.node_level == 1);

  /* a leaf page is reached, make the actual insertion in this page.
   * Because of the specific top-down splitting algorithm, there will be
   * no need to go up to parent pages, and it will always be possible to
   * make the insertion in this leaf page.
   */
  ret_val = btree_insert_into_leaf (thread_p, btid, P, key);
  assert (ret_val != ER_BTREE_NO_SPACE);
  if (ret_val != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

#if !defined(NDEBUG)
  (void) spage_check_num_slots (thread_p, P);
#endif

  assert (top_op_active == 0);
  assert (Q == NULL);
  assert (R == NULL);

  pgbuf_unfix_and_init (thread_p, P);

#if !defined(NDEBUG)
  (void) db_idxkey_clear (&(btid->left_fence));
  (void) db_idxkey_clear (&(btid->right_fence));
#endif

#if 0				/* dbg - print */
  if (split_level > 1)
    {
      fprintf (stdout, "\n");
    }
#endif

#if defined(SERVER_MODE)
  (void) thread_set_check_interrupt (thread_p, old_check_interrupt);
#endif /* SERVER_MODE */

  mnt_stats_counter_with_time (thread_p, MNT_STATS_BTREE_INSERTS, 1,
			       perf_start);

  return key;

exit_on_error:

  /* do not unfix P, Q, R before topop rollback */
  if (top_op_active)
    {
      log_end_system_op (thread_p, LOG_RESULT_TOPOP_ABORT);
    }

  if (P)
    {
      pgbuf_unfix_and_init (thread_p, P);
    }
  if (Q)
    {
      pgbuf_unfix_and_init (thread_p, Q);
    }
  if (R)
    {
      pgbuf_unfix_and_init (thread_p, R);
    }
  if (next_page)
    {
      pgbuf_unfix_and_init (thread_p, next_page);
    }

#if !defined(NDEBUG)
  (void) db_idxkey_clear (&(btid->left_fence));
  (void) db_idxkey_clear (&(btid->right_fence));
#endif

#if defined(SERVER_MODE)
  (void) thread_set_check_interrupt (thread_p, old_check_interrupt);
#endif /* SERVER_MODE */

  return NULL;
}

#if 1				/* TODO - do not delete me */
DB_IDXKEY *
btree_insert_old (THREAD_ENTRY * thread_p, BTID_INT * btid, DB_IDXKEY * key)
{
  VPID P_vpid, Q_vpid, R_vpid, child_vpid;
  PAGE_PTR P = NULL, Q = NULL, R = NULL, next_page = NULL;
  BTREE_NODE_HEADER pheader, qheader;
  int key_len;
  INT16 p_slot_id;
  int top_op_active = 0;
  int max_free, new_max_free;
#if 1
  PAGEID_STRUCT pageid_struct;	/* for recovery purposes */
  LOG_DATA_ADDR addr = LOG_ADDR_INITIALIZER;
#endif
  int ret_val;
  short node_type;
  INT16 key_cnt;
  VPID next_vpid;
#if defined(SERVER_MODE)
  bool old_check_interrupt;
#endif /* SERVER_MODE */
  int retry_btree_no_space = 0;
  int non_leaf_request_mode = PGBUF_LATCH_READ;
  UINT64 perf_start;

  assert (btid->sys_btid != NULL);
  assert (!OID_ISNULL (&(btid->cls_oid)));
  assert (btid->classrepr != NULL);
#if !defined(NDEBUG)
  assert (DB_IDXKEY_IS_NULL (&(btid->left_fence)));
  assert (DB_IDXKEY_IS_NULL (&(btid->right_fence)));
#endif

  PERF_MON_GET_CURRENT_TIME (perf_start);

  assert (key != NULL);
  if (key == NULL)
    {
      return key;
    }

  assert (file_is_new_file (thread_p, &(btid->sys_btid->vfid)) ==
	  FILE_OLD_FILE);

  assert (!BTREE_INVALID_INDEX_ID (btid->sys_btid));

  FI_RESET (thread_p, FI_TEST_BTREE_MANAGER_ALLOC_PAGE_ERROR1);
  FI_RESET (thread_p, FI_TEST_BTREE_MANAGER_ALLOC_PAGE_ERROR2);
  FI_RESET (thread_p, FI_TEST_BTREE_MANAGER_ALLOC_PAGE_ERROR3);

  key_len = btree_get_key_length (key);	/* TODO - */
  if (!BTREE_IS_VALID_KEY_LEN (key_len))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_BTREE_MAX_KEYLEN, 2,
	      key_len, BTREE_MAX_KEYLEN);

      return NULL;
    }

#if defined(SERVER_MODE)
  old_check_interrupt = thread_set_check_interrupt (thread_p, false);
#endif /* SERVER_MODE */

start_point:

#if !defined(NDEBUG)
  (void) db_idxkey_clear (&(btid->left_fence));
  (void) db_idxkey_clear (&(btid->right_fence));
#endif

  P_vpid.volid = btid->sys_btid->vfid.volid;	/* read the root page */
  P_vpid.pageid = btid->sys_btid->root_pageid;
  P =
    btree_pgbuf_fix (thread_p, &(btid->sys_btid->vfid), &P_vpid,
		     non_leaf_request_mode, PGBUF_UNCONDITIONAL_LATCH);
  if (P == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }
  BTREE_STATS_ADD_WAIT_TIME (PAGE_BTREE_ROOT);

  /* free space in the root node */
  max_free = spage_max_space_for_new_record (thread_p, P);

  /* read the header record */
  if (btree_read_node_header (P, &pheader) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  key_cnt = pheader.key_cnt;
  assert_release (key_cnt >= 0);
  assert_release (pheader.node_level > 1);
  node_type = BTREE_NON_LEAF_NODE;

  new_max_free = max_free - ONE_K;	/* guess */

  /* So, root is always a non leaf node,
   * the number of keys is actually one greater
   */
  key_cnt += 1;

  BTREE_CHECK_KEY_CNT (P, pheader.node_level, pheader.key_cnt);

  /* there is a need to split the root, only if there is not enough space
   * for a new entry and either there are more than one record.
   *
   * in no case should a split happen if the node is currently empty
   * (key_cnt == 1).
   */
  if (non_leaf_request_mode == PGBUF_LATCH_WRITE
      && key_cnt > 1 && new_max_free < 0)
    {
      assert (node_type == BTREE_NON_LEAF_NODE);

      /* start system top operation */
      log_start_system_op (thread_p);
      top_op_active = 1;

      FI_SET (thread_p, FI_TEST_BTREE_MANAGER_ALLOC_PAGE_ERROR2, 1);

      /* get two new pages */
      Q = btree_get_new_page (thread_p, btid, &Q_vpid, &P_vpid);
      if (Q == NULL)
	{
	  GOTO_EXIT_ON_ERROR;
	}

#if 1				/* TODO:[happy:remove "#if 0" when postpone op. bug fixed] */

      /* log the newly allocated pageid for deallocation for undo purposes */
      pageid_struct.vpid = Q_vpid;
      pageid_struct.vfid.fileid = btid->sys_btid->vfid.fileid;
      pageid_struct.vfid.volid = btid->sys_btid->vfid.volid;
      LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, NULL, -1);
      log_append_undo_data (thread_p, RVBT_NEW_PGALLOC, &addr,
			    sizeof (PAGEID_STRUCT), &pageid_struct);
#endif

      FI_SET (thread_p, FI_TEST_BTREE_MANAGER_ALLOC_PAGE_ERROR3, 1);

      R = btree_get_new_page (thread_p, btid, &R_vpid, &P_vpid);
      if (R == NULL)
	{
	  GOTO_EXIT_ON_ERROR;
	}

#if 1				/* TODO:[happy:remove "#if 0" when postpone op. bug fixed] */

      /* log the newly allocated pageid for deallocation for undo purposes */
      pageid_struct.vpid = R_vpid;
      assert (pageid_struct.vfid.fileid == btid->sys_btid->vfid.fileid);
      assert (pageid_struct.vfid.volid == btid->sys_btid->vfid.volid);

      LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, NULL, -1);
      log_append_undo_data (thread_p, RVBT_NEW_PGALLOC, &addr,
			    sizeof (PAGEID_STRUCT), &pageid_struct);
#endif

      /* split the root P into two pages Q and R */
      if (btree_split_root (thread_p, btid, P, Q, R, &P_vpid, &Q_vpid,
			    &R_vpid, key, &child_vpid) != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

#if !defined(NDEBUG)
      (void) spage_check_num_slots (thread_p, P);
      (void) spage_check_num_slots (thread_p, Q);
      (void) spage_check_num_slots (thread_p, R);
#endif

      log_end_system_op (thread_p, LOG_RESULT_TOPOP_COMMIT);
      top_op_active = 0;

      if (VPID_EQ (&child_vpid, &Q_vpid))
	{
	  /* child page to be followed is page Q */
	  pgbuf_unfix_and_init (thread_p, R);
	}
      else if (VPID_EQ (&child_vpid, &R_vpid))
	{
	  /* child page to be followed is page R */
	  pgbuf_unfix_and_init (thread_p, Q);

	  Q = R;
	  R = NULL;
	  Q_vpid = R_vpid;
	}
      else
	{
	  assert (false);	/* is error ? */

	  pgbuf_unfix_and_init (thread_p, R);
	  pgbuf_unfix_and_init (thread_p, Q);
	  pgbuf_unfix_and_init (thread_p, P);

	  GOTO_EXIT_ON_ERROR;
	}

      /* release parent page P, and repeat the same operations from child
       * page Q on
       */
      pgbuf_unfix_and_init (thread_p, P);
      P = Q;
      Q = NULL;
      P_vpid = Q_vpid;
    }

  /* get the header record */
  if (btree_read_node_header (P, &pheader) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  assert (pheader.node_level > 1);

  node_type = BTREE_NON_LEAF_NODE;
  key_cnt = pheader.key_cnt;
  assert_release (key_cnt >= 0);
  BTREE_CHECK_KEY_CNT (P, pheader.node_level, pheader.key_cnt);
  VPID_COPY (&next_vpid, &pheader.next_vpid);

  while (node_type == BTREE_NON_LEAF_NODE)
    {
      assert (VPID_ISNULL (&pheader.next_vpid));

      /* find and get the child page to be followed */
      if (btree_search_nonleaf_page
	  (thread_p, btid, P, key, &p_slot_id, &Q_vpid) != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      if (pheader.node_level > 2)
	{
	  /* Q is non leaf node */
	  Q =
	    btree_pgbuf_fix (thread_p, &(btid->sys_btid->vfid), &Q_vpid,
			     non_leaf_request_mode,
			     PGBUF_UNCONDITIONAL_LATCH);
	  BTREE_STATS_ADD_WAIT_TIME (PAGE_BTREE_NON_LEAF);
	}
      else
	{
	  /* Q is leaf node */
	  Q =
	    btree_pgbuf_fix (thread_p, &(btid->sys_btid->vfid), &Q_vpid,
			     PGBUF_LATCH_WRITE, PGBUF_UNCONDITIONAL_LATCH);
	  BTREE_STATS_ADD_WAIT_TIME (PAGE_BTREE_LEAF);
	}
      if (Q == NULL)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      max_free = spage_max_space_for_new_record (thread_p, Q);

      /* read the header record */
      if (btree_read_node_header (Q, &qheader) != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      assert (pheader.node_level - 1 == qheader.node_level);
      node_type =
	qheader.node_level > 1 ? BTREE_NON_LEAF_NODE : BTREE_LEAF_NODE;
      key_cnt = qheader.key_cnt;
      assert_release (key_cnt >= 0);
      /* if Q is a non leaf node, the number of keys is actually one greater */
      key_cnt = (node_type == BTREE_LEAF_NODE) ? key_cnt : key_cnt + 1;
      BTREE_CHECK_KEY_CNT (Q, qheader.node_level, qheader.key_cnt);

      if (node_type == BTREE_NON_LEAF_NODE)
	{
	  new_max_free = max_free - ONE_K;	/* guess */
	}
      else
	{
	  new_max_free = max_free - (key_len + BTREE_MAX_ALIGN);
	}

      /* there is a need to split Q, only if there is not enough space
       * for a new entry and either there are more than one record.
       *
       * in no case should a split happen if the node is currently empty
       * (key_cnt == 0).
       */
      if (non_leaf_request_mode == PGBUF_LATCH_WRITE
	  && key_cnt > 0 && new_max_free < 0)
	{
	  /* start system top operation */
	  log_start_system_op (thread_p);
	  top_op_active = 1;

	  FI_SET (thread_p, FI_TEST_BTREE_MANAGER_ALLOC_PAGE_ERROR1, 1);

	  /* split the page Q into two pages Q and R, and update parent page P */
	  R = btree_get_new_page (thread_p, btid, &R_vpid, &Q_vpid);
	  if (R == NULL)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

#if 1				/* TODO:[happy:remove "#if 0" when postpone op. bug fixed] */
	  /* Log the newly allocated pageid for deallocation for undo purposes */
	  pageid_struct.vpid = R_vpid;
	  pageid_struct.vfid.fileid = btid->sys_btid->vfid.fileid;
	  pageid_struct.vfid.volid = btid->sys_btid->vfid.volid;
	  LOG_ADDR_SET (&addr, &btid->sys_btid->vfid, NULL, -1);
	  log_append_undo_data (thread_p, RVBT_NEW_PGALLOC, &addr,
				sizeof (PAGEID_STRUCT), &pageid_struct);
#endif

	  if (btree_split_node (thread_p, btid, P, Q, R,
				&P_vpid, &Q_vpid, &R_vpid, p_slot_id,
				key, &child_vpid) != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

#if !defined(NDEBUG)
	  (void) spage_check_num_slots (thread_p, P);
	  (void) spage_check_num_slots (thread_p, Q);
	  (void) spage_check_num_slots (thread_p, R);
#endif

	  next_page = NULL;
	  if (node_type == BTREE_LEAF_NODE)
	    {
	      assert (next_page == NULL);

	      next_page = btree_get_next_page (thread_p, btid, R);
	      if (next_page != NULL)
		{
		  if (btree_set_vpid_previous_vpid (thread_p, btid,
						    next_page,
						    &R_vpid) != NO_ERROR)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }
		}
	    }

	  log_end_system_op (thread_p, LOG_RESULT_TOPOP_COMMIT);
	  top_op_active = 0;

	  if (VPID_EQ (&child_vpid, &Q_vpid))
	    {
	      /* child page to be followed is Q */
	      pgbuf_unfix_and_init (thread_p, R);
	    }
	  else if (VPID_EQ (&child_vpid, &R_vpid))
	    {
	      /* child page to be followed is R */
	      pgbuf_unfix_and_init (thread_p, Q);

	      Q = R;
	      R = NULL;
	      Q_vpid = R_vpid;
	    }
	  else
	    {
	      assert (false);	/* is error ? */

	      pgbuf_unfix_and_init (thread_p, Q);
	      pgbuf_unfix_and_init (thread_p, R);
	      pgbuf_unfix_and_init (thread_p, P);

	      if (next_page)
		{
		  pgbuf_unfix_and_init (thread_p, next_page);
		}

	      GOTO_EXIT_ON_ERROR;
	    }

	  if (next_page)
	    {
	      pgbuf_unfix_and_init (thread_p, next_page);
	    }
	}

      /* release parent page P, and repeat the same operations from child
       * page Q on
       */
      pgbuf_unfix_and_init (thread_p, P);
      P = Q;
      Q = NULL;
      P_vpid = Q_vpid;

      /* node_type must be recalculated */
      if (btree_read_node_header (P, &pheader) != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
      node_type =
	pheader.node_level > 1 ? BTREE_NON_LEAF_NODE : BTREE_LEAF_NODE;
      key_cnt = pheader.key_cnt;
      assert_release (key_cnt >= 0);
      BTREE_CHECK_KEY_CNT (P, pheader.node_level, pheader.key_cnt);
      VPID_COPY (&next_vpid, &pheader.next_vpid);
    }				/* while */

  assert (pheader.node_level == 1);

  /* a leaf page is reached, make the actual insertion in this page.
   * Because of the specific top-down splitting algorithm, there will be
   * no need to go up to parent pages, and it will always be possible to
   * make the insertion in this leaf page.
   */
  ret_val = btree_insert_into_leaf (thread_p, btid, P, key);

  if (ret_val != NO_ERROR)
    {
      if (ret_val == ER_BTREE_NO_SPACE)
	{
	  if (retry_btree_no_space < 1)
	    {
	      assert (non_leaf_request_mode == PGBUF_LATCH_READ);

	      /* ER_BTREE_NO_SPACE can be made by split node algorithm
	       * In this case, release resource and retry it one time.
	       */
	      assert (top_op_active == 0);
	      assert (Q == NULL);
	      assert (R == NULL);

	      pgbuf_unfix_and_init (thread_p, P);

	      non_leaf_request_mode = PGBUF_LATCH_WRITE;
	      retry_btree_no_space++;
	      goto start_point;
	    }
	  else
	    {
	      assert (non_leaf_request_mode == PGBUF_LATCH_WRITE);
	      assert (false);	/* give up */

	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_BTREE_NO_SPACE, 2,
		      btree_get_key_length (key), "");
	    }
	}

      GOTO_EXIT_ON_ERROR;
    }

#if !defined(NDEBUG)
  (void) spage_check_num_slots (thread_p, P);
#endif

  assert (top_op_active == 0);
  assert (Q == NULL);
  assert (R == NULL);

  pgbuf_unfix_and_init (thread_p, P);

#if !defined(NDEBUG)
  (void) db_idxkey_clear (&(btid->left_fence));
  (void) db_idxkey_clear (&(btid->right_fence));
#endif

#if defined(SERVER_MODE)
  (void) thread_set_check_interrupt (thread_p, old_check_interrupt);
#endif /* SERVER_MODE */

  mnt_stats_counter_with_time (thread_p, MNT_STATS_BTREE_INSERTS, 1,
			       perf_start);

  return key;

exit_on_error:
  /* do not unfix P, Q, R before topop rollback */
  if (top_op_active)
    {
      log_end_system_op (thread_p, LOG_RESULT_TOPOP_ABORT);
    }

  if (P)
    {
      pgbuf_unfix_and_init (thread_p, P);
    }
  if (Q)
    {
      pgbuf_unfix_and_init (thread_p, Q);
    }
  if (R)
    {
      pgbuf_unfix_and_init (thread_p, R);
    }
  if (next_page)
    {
      pgbuf_unfix_and_init (thread_p, next_page);
    }

#if !defined(NDEBUG)
  (void) db_idxkey_clear (&(btid->left_fence));
  (void) db_idxkey_clear (&(btid->right_fence));
#endif

#if defined(SERVER_MODE)
  (void) thread_set_check_interrupt (thread_p, old_check_interrupt);
#endif /* SERVER_MODE */

  return NULL;
}
#endif

/*
 * btree_rv_leafrec_redo_insert_key () -
 *   return: int
 *   recv(in): Recovery structure
 *
 * Note: Recover a leaf record key insertion for redo purposes
 */
int
btree_rv_leafrec_redo_insert_key (THREAD_ENTRY * thread_p, LOG_RCV * recv)
{
  BTREE_NODE_HEADER node_header;
  RECDES rec = RECDES_INITIALIZER;
  INT16 slotid;
  int sp_success;

  /* insert the new record */

  slotid = recv->offset;

  rec.type = REC_HOME;
  rec.area_size = rec.length = recv->length;
  rec.data = (char *) recv->data;

  sp_success = spage_insert_at (thread_p, recv->pgptr, slotid, &rec);
  if (sp_success != SP_SUCCESS)
    {
      if (sp_success != SP_ERROR)
	{
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_GENERIC_ERROR, 1, "");
	}
      assert (false);
      goto error;
    }

  /*
   * update node header of btree
   */

  if (btree_read_node_header (recv->pgptr, &node_header) != NO_ERROR)
    {
      assert (false);
      goto error;
    }

  /* update key_cnt */
  assert_release (node_header.key_cnt >= 0);
  node_header.key_cnt++;
  assert_release (node_header.key_cnt >= 1);

  assert (node_header.node_level == 1);
  BTREE_CHECK_KEY_CNT (recv->pgptr, node_header.node_level,
		       node_header.key_cnt);

  /* update split_info */
  assert (node_header.split_info.pivot >= 0);
  assert (node_header.key_cnt > 0);
  btree_split_next_pivot (&node_header.split_info,
			  (float) slotid / node_header.key_cnt,
			  node_header.key_cnt);

  if (btree_write_node_header (recv->pgptr, &node_header) != NO_ERROR)
    {
      assert (false);
      goto error;
    }

  pgbuf_set_dirty (thread_p, recv->pgptr, DONT_FREE);

  return NO_ERROR;

error:

  return er_errid ();
}

/*
 * btree_get_next_page () -
 *   return:
 *
 *   btid(in):
 *   page_p(in):
 */
PAGE_PTR
btree_get_next_page (THREAD_ENTRY * thread_p, BTID_INT * btid,
		     PAGE_PTR page_p)
{
  PAGE_PTR next_page = NULL;
  BTREE_NODE_HEADER node_header;

  if (page_p == NULL)
    {
      assert (page_p != NULL);
      return NULL;
    }

  if (btree_read_node_header (page_p, &node_header) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (VPID_ISNULL (&node_header.next_vpid))
    {
      return NULL;		/* is end-of-leaf */
    }

  next_page =
    btree_pgbuf_fix (thread_p, &(btid->sys_btid->vfid),
		     &node_header.next_vpid, PGBUF_LATCH_WRITE,
		     PGBUF_UNCONDITIONAL_LATCH);
  if (next_page == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }
  BTREE_STATS_ADD_WAIT_TIME (PAGE_BTREE_LEAF);

  return next_page;

exit_on_error:

  if (next_page)
    {
      pgbuf_unfix_and_init (thread_p, next_page);
    }
  return NULL;
}
