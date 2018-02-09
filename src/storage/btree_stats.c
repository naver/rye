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

/* B+tree statistical information environment */
typedef struct btree_stats_env BTREE_STATS_ENV;
struct btree_stats_env
{
  BTREE_SCAN btree_scan;	/* BTS */
  BTREE_STATS *stat_info;
  DB_IDXKEY pkey;
};

static int btree_get_stats_idxkey (THREAD_ENTRY * thread_p,
				   BTREE_STATS_ENV * env,
				   const DB_IDXKEY * key);
static int btree_get_stats_key (THREAD_ENTRY * thread_p,
				BTREE_STATS_ENV * env);
static int btree_get_stats_with_AR_sampling (THREAD_ENTRY * thread_p,
					     BTREE_STATS_ENV * env);
static int btree_get_stats_with_fullscan (THREAD_ENTRY * thread_p,
					  BTREE_STATS_ENV * env);

static PAGE_PTR btree_find_AR_sampling_leaf (THREAD_ENTRY * thread_p,
					     BTID * btid, VPID * pg_vpid,
					     BTREE_STATS * stat_info_p,
					     bool * found_p);
/*
 * btree_get_stats_idxkey () -
 *   return: NO_ERROR
 *   thread_p(in);
 *   env(in/out): Structure to store and return the statistical information
 *   key(in);
 */
static int
btree_get_stats_idxkey (UNUSED_ARG THREAD_ENTRY * thread_p,
			BTREE_STATS_ENV * env, const DB_IDXKEY * key)
{
  int rc = DB_UNK;
//  BTREE_SCAN *BTS;
  int i, k;
  const DB_VALUE *elem;
  int ret = NO_ERROR;

  assert (env != NULL);
  assert (key != NULL);
  assert (!DB_IDXKEY_IS_NULL (key));

//  BTS = &(env->btree_scan);

  assert (env->pkey.size == key->size - 1);

  for (i = 0; i < env->pkey.size; i++)
    {
      /* extract the element of the key */
      elem = &(key->vals[i]);

      rc = tp_value_compare (&(env->pkey.vals[i]), elem, 0, 1, NULL);
      assert (rc != DB_UNK);

      if (rc != DB_EQ)
	{
	  /* found different value */

	  env->stat_info->keys++;

	  env->stat_info->pkeys[i]++;
	  pr_clear_value (&(env->pkey.vals[i]));	/* clear saved */
	  pr_clone_value (elem, &(env->pkey.vals[i]));	/* save */

	  /* propagate to the following partial key-values */
	  for (k = i + 1; k < env->pkey.size; k++)
	    {
	      elem = &(key->vals[k]);

	      env->stat_info->pkeys[k]++;
	      pr_clear_value (&(env->pkey.vals[k]));	/* clear saved */
	      pr_clone_value (elem, &(env->pkey.vals[k]));	/* save */
	    }

	  break;
	}
    }

  assert (ret == NO_ERROR);

  return ret;

#if 0
exit_on_error:

  return (ret == NO_ERROR
	  && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;
#endif
}

/*
 * btree_get_stats_key () -
 *   return: NO_ERROR
 *   thread_p(in);
 *   env(in/out): Structure to store and return the statistical information
 */
static int
btree_get_stats_key (THREAD_ENTRY * thread_p, BTREE_STATS_ENV * env)
{
  BTREE_SCAN *BTS;
  RECDES rec = RECDES_INITIALIZER;
  DB_IDXKEY key_value;
  bool clear_key = false;
  int ret = NO_ERROR;

  assert (env != NULL);

  DB_IDXKEY_MAKE_NULL (&key_value);

  if (env->pkey.size <= 0)
    {
      assert (false);
      ;				/* do not request pkeys info; go ahead */
    }
  else
    {
      BTS = &(env->btree_scan);

      if (spage_get_record (BTS->C_page, BTS->slot_id, &rec, PEEK) !=
	  S_SUCCESS)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      /* read key-value */

      assert (clear_key == false);

      ret = btree_read_record (thread_p, &BTS->btid_int, &rec,
			       &key_value, NULL,
			       BTREE_LEAF_NODE, &clear_key, PEEK_KEY_VALUE);
      if (ret != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      if (DB_IDXKEY_IS_NULL (&key_value))
	{
	  assert (false);	/* is impossible */
	  GOTO_EXIT_ON_ERROR;
	}

      /* get pkeys info */
      ret = btree_get_stats_idxkey (thread_p, env, &key_value);
      if (ret != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      btree_clear_key_value (&clear_key, &key_value);
    }

end:

  btree_clear_key_value (&clear_key, &key_value);

  return ret;

exit_on_error:

  ret = (ret == NO_ERROR
	 && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;

  goto end;
}

/*
 * btree_get_stats_with_AR_sampling () - Do Acceptance/Rejection Sampling
 *   return: NO_ERROR
 *   env(in/out): Structure to store and return the statistical information
 */
static int
btree_get_stats_with_AR_sampling (THREAD_ENTRY * thread_p,
				  BTREE_STATS_ENV * env)
{
  BTREE_STATS *stat_info;
  BTREE_SCAN *BTS;
  int n, i;
  bool found;
  int key_cnt;
  BTREE_NODE_HEADER node_header;
  INT64 max_free_space;
  int exp_ratio;
  int ret = NO_ERROR;

  assert (env != NULL);
  assert (env->stat_info != NULL);

  stat_info = env->stat_info;
  BTS = &(env->btree_scan);
  BTS->use_desc_index = 0;	/* init */

  for (n = 0; n < STATS_SAMPLING_THRESHOLD; n++)
    {
      if (stat_info->leafs >= STATS_SAMPLING_LEAFS_MAX)
	{
	  break;		/* found all samples */
	}

      BTS->C_page = btree_find_AR_sampling_leaf (thread_p,
						 BTS->btid_int.sys_btid,
						 &BTS->C_vpid,
						 stat_info, &found);
      if (BTS->C_page == NULL)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      /* found sampling leaf page */
      if (found)
	{
	  /* get header information (key_cnt) */
	  ret = btree_read_node_header (NULL, BTS->C_page, &node_header);
	  if (ret != NO_ERROR)
	    {
	      assert_release (false);
	      GOTO_EXIT_ON_ERROR;
	    }
	  assert (node_header.node_level == 1);

	  key_cnt = node_header.key_cnt;
	  assert_release (key_cnt >= 0);

	  if (key_cnt > 0)
	    {
	      BTS->slot_id = 1;

	      assert_release (BTS->slot_id <= key_cnt);

	      for (i = 0; i < key_cnt; i++)
		{
		  ret = btree_get_stats_key (thread_p, env);
		  if (ret != NO_ERROR)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }

		  /* get the next index record */
		  ret = btree_find_next_record (thread_p,
						BTS, BTS->use_desc_index,
						NULL);
		  if (ret != NO_ERROR)
		    {
		      GOTO_EXIT_ON_ERROR;
		    }
		}
	    }
	}

      if (BTS->P_page != NULL)
	{
	  pgbuf_unfix_and_init (thread_p, BTS->P_page);
	}

      if (BTS->C_page != NULL)
	{
	  pgbuf_unfix_and_init (thread_p, BTS->C_page);
	}
    }				/* for (n = 0; ... ) */

  /* apply distributed expansion */
  if (stat_info->leafs > 0)
    {
      exp_ratio = stat_info->pages / stat_info->leafs;

      stat_info->leafs *= exp_ratio;
      if (stat_info->leafs < 0)
	{
	  stat_info->leafs = INT_MAX;
	}

      stat_info->keys *= exp_ratio;
      if (stat_info->keys < 0)
	{			/* multiply-overflow defense */
	  stat_info->keys = DB_BIGINT_MAX;
	}

      for (i = 0; i < env->pkey.size; i++)
	{
	  stat_info->pkeys[i] *= exp_ratio;
	  if (stat_info->pkeys[i] < 0)
	    {			/* multiply-overflow defense */
	      stat_info->pkeys[i] = INT_MAX;
	    }
	}

      stat_info->tot_free_space *= exp_ratio;
      if (stat_info->tot_free_space < 0)
	{			/* multiply-overflow defense */
	  stat_info->tot_free_space = DB_BIGINT_MAX;
	}
      max_free_space =
	(double) stat_info->leafs * DB_PAGESIZE * BTREE_SPLIT_MAX_PIVOT;
      if (max_free_space < 0)
	{			/* multiply-overflow defense */
	  max_free_space = DB_BIGINT_MAX;
	}

      stat_info->tot_free_space =
	MIN (stat_info->tot_free_space, max_free_space);
    }

end:

  if (BTS->P_page != NULL)
    {
      pgbuf_unfix_and_init (thread_p, BTS->P_page);
    }

  if (BTS->C_page != NULL)
    {
      pgbuf_unfix_and_init (thread_p, BTS->C_page);
    }

  return ret;

exit_on_error:

  ret = (ret == NO_ERROR
	 && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;

  goto end;
}

/*
 * btree_get_stats_with_fullscan () - Do Full Scan
 *   return: NO_ERROR
 *   env(in/out): Structure to store and return the statistical information
 */
static int
btree_get_stats_with_fullscan (THREAD_ENTRY * thread_p, BTREE_STATS_ENV * env)
{
  BTREE_STATS *stat_info;
  BTREE_SCAN *BTS;
  VPID root_vpid;
  int ret = NO_ERROR;

  assert (env != NULL);
  assert (env->stat_info != NULL);

  stat_info = env->stat_info;
  BTS = &(env->btree_scan);
  BTS->use_desc_index = 0;	/* get the left-most leaf page */

  assert (BTS->btid_int.sys_btid != NULL);
  root_vpid.volid = BTS->btid_int.sys_btid->vfid.volid;
  root_vpid.pageid = BTS->btid_int.sys_btid->root_pageid;

  ret = btree_find_lower_bound_leaf (thread_p, &root_vpid, BTS, stat_info);
  if (ret != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  while (!BTREE_END_OF_SCAN (BTS))
    {
      ret = btree_get_stats_key (thread_p, env);
      if (ret != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      /* get the next index record */
      ret =
	btree_find_next_record (thread_p, BTS, BTS->use_desc_index,
				stat_info);
      if (ret != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

end:

  if (BTS->P_page != NULL)
    {
      pgbuf_unfix_and_init (thread_p, BTS->P_page);
    }

  if (BTS->C_page != NULL)
    {
      pgbuf_unfix_and_init (thread_p, BTS->C_page);
    }

  return ret;

exit_on_error:

  ret = (ret == NO_ERROR
	 && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;

  goto end;
}

/*
 * btree_get_stats () - Get Statistical Information about the B+tree index
 *   return: NO_ERROR
 *   class_oid(in):
 *   stat_info(in/out): Structure to store and
 *                        return the statistical information
 *   with_fullscan(in): true iff WITH FULLSCAN
 *
 * Note: Computes and returns statistical information about B+tree
 * which consist of the number of leaf pages, total number of
 * pages, number of keys and the height of the tree.
 */
int
btree_get_stats (THREAD_ENTRY * thread_p, OID * class_oid,
		 BTREE_STATS * stat_info, bool with_fullscan)
{
  int npages;
  BTREE_STATS_ENV stat_env, *env;
  VPID root_vpid;
  PAGE_PTR root_page_ptr = NULL;
  BTREE_NODE_HEADER root_header;
  BTREE_SCAN *BTS = NULL;
#if !defined(NDEBUG)
  OR_INDEX *indexp = NULL;
#endif
  int i;
  int ret = NO_ERROR;

  assert_release (stat_info != NULL);
  assert_release (!BTID_IS_NULL (&stat_info->btid));

  npages =
    file_get_numpages (thread_p, &(stat_info->btid.vfid),
		       &(stat_info->num_table_vpids),
		       &(stat_info->num_user_pages_mrkdelete),
		       &(stat_info->num_allocsets));
  if (npages < 0)
    {
      npages = INT_MAX;
    }
  assert_release (npages >= 1);
  assert (stat_info->num_table_vpids >= 1);
  assert (stat_info->num_user_pages_mrkdelete >= 0);
  assert (stat_info->num_allocsets >= 1);

#if 0				/* TODO - do not delete me; btree merge is disabled */
  /* For the optimization of the sampling,
   * if the btree file has currently the same pages as we gathered
   * statistics, we guess the btree file has not been modified;
   * So, we take current stats as it is
   */
  if (!with_fullscan)
    {
      /* check if the stats has been gathered */
      if (stat_info->keys > 0)
	{
	  /* guess the stats has not been modified */
	  if (npages == stat_info->pages)
	    {
	      return NO_ERROR;
	    }
	}
    }
#endif

  /* set environment variable */
  env = &stat_env;
  env->stat_info = stat_info;

  BTS = &(env->btree_scan);

  /* index scan info */
  BTREE_INIT_SCAN (BTS);

  /* construct BTID_INT structure */
  BTREE_INIT_BTID_INT (&(BTS->btid_int));

  BTS->btid_int.sys_btid = &(stat_info->btid);

  assert (stat_info->pkeys_size > 0);
  assert (stat_info->pkeys_size <= BTREE_STATS_PKEYS_NUM);
  env->pkey.size = stat_info->pkeys_size;

  assert (env->pkey.size <= BTREE_STATS_PKEYS_NUM);
  for (i = 0; i < env->pkey.size; i++)
    {
      DB_MAKE_NULL (&(env->pkey.vals[i]));
    }

#if 1				/* TODO - */
  root_vpid.pageid = stat_info->btid.root_pageid;	/* read root page */
  root_vpid.volid = stat_info->btid.vfid.volid;

  root_page_ptr =
    btree_pgbuf_fix (thread_p, &(stat_info->btid.vfid), &root_vpid,
		     PGBUF_LATCH_READ, PGBUF_UNCONDITIONAL_LATCH,
		     PAGE_BTREE_ROOT);
  if (root_page_ptr == NULL)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (btree_read_node_header (NULL, root_page_ptr, &root_header) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  pgbuf_unfix_and_init (thread_p, root_page_ptr);
#endif

  /* get class representation of the index */
  COPY_OID (&(BTS->btid_int.cls_oid), class_oid);
  BTS->btid_int.classrepr =
    heap_classrepr_get (thread_p, &(BTS->btid_int.cls_oid), NULL_REPRID,
			&(BTS->btid_int.classrepr_cache_idx), true);
  if (BTS->btid_int.classrepr == NULL)
    {
      assert (false);
      GOTO_EXIT_ON_ERROR;
    }

  /* get the index ID which corresponds to the BTID */
  BTS->btid_int.indx_id =
    heap_classrepr_find_index_id (BTS->btid_int.classrepr,
				  BTS->btid_int.sys_btid);
  if (BTS->btid_int.indx_id < 0)
    {
      assert (false);
      GOTO_EXIT_ON_ERROR;
    }

#if !defined(NDEBUG)
  indexp = &(BTS->btid_int.classrepr->indexes[BTS->btid_int.indx_id]);
#endif

  /* initialize environment stat_info structure */
  stat_info->pages = npages;
  stat_info->leafs = 0;
  stat_info->height = root_header.node_level;
  stat_info->keys = 0;

  for (i = 0; i < env->pkey.size; i++)
    {
      stat_info->pkeys[i] = 0;	/* clear old stats */
    }

  stat_info->tot_free_space = 0;

  /* exclude rightmost OID type */

#if !defined(NDEBUG)
  assert (env->pkey.size == indexp->n_atts);
#endif

  if (with_fullscan || npages <= STATS_SAMPLING_THRESHOLD)
    {
      /* do fullscan at small table */
      ret = btree_get_stats_with_fullscan (thread_p, env);
    }
  else
    {
      ret = btree_get_stats_with_AR_sampling (thread_p, env);
    }

  if (ret != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* check for emptiness */
  for (i = 0; i < env->pkey.size; i++)
    {
      assert_release (stat_info->keys >= stat_info->pkeys[i]);

      if (stat_info->keys <= 0)
	{
	  /* is empty */
	  assert_release (stat_info->pkeys[i] == 0);
	  stat_info->pkeys[i] = 0;
	}
      else
	{
	  stat_info->pkeys[i] = MAX (stat_info->pkeys[i], 1);
	}
    }

  /* check for leaf pages */
  stat_info->leafs = MAX (1, stat_info->leafs);
  stat_info->leafs = MIN (stat_info->leafs, npages - (stat_info->height - 1));

  assert_release (stat_info->pages >= 1);
  assert_release (stat_info->leafs >= 1);
  assert_release (stat_info->height >= 1);
  assert_release (stat_info->keys >= 0);
  assert (stat_info->tot_free_space >= 0);
  assert (stat_info->leafs == 0
	  || stat_info->tot_free_space <
	  (INT64) stat_info->leafs * DB_PAGESIZE);
  assert (stat_info->num_table_vpids >= 1);
  assert (stat_info->num_user_pages_mrkdelete >= 0);
  assert (stat_info->num_allocsets >= 1);

end:

  if (root_page_ptr)
    {
      pgbuf_unfix_and_init (thread_p, root_page_ptr);
    }

#if 1				/* TODO */
  if (BTS->btid_int.classrepr)
    {
      assert (BTS->btid_int.classrepr_cache_idx != -1);
      assert (BTS->btid_int.indx_id != -1);

      (void) heap_classrepr_free (BTS->btid_int.classrepr,
				  &(BTS->btid_int.classrepr_cache_idx));
      assert (BTS->btid_int.classrepr_cache_idx == -1);

      BTS->btid_int.classrepr = NULL;
//          BTS->btid_int.classrepr_cache_idx = -1;
//          BTS->btid_int.indx_id = -1;
    }
#endif

  /* clear partial key-values */
  db_idxkey_clear (&(env->pkey));

  return ret;

exit_on_error:

  ret = (ret == NO_ERROR
	 && (ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;

  goto end;
}

/*
 * btree_find_AR_sampling_leaf () -
 *   return: page pointer
 *   btid(in):
 *   pg_vpid(in):
 *   stat_info_p(in):
 *   found_p(out):
 *
 * Note: Find the page identifier via the Acceptance/Rejection Sampling
 *       leaf page of the B+tree index.
 * Note: Random Sampling from Databases
 *       (Chapter 3. Random Sampling from B+ Trees)
 */
static PAGE_PTR
btree_find_AR_sampling_leaf (THREAD_ENTRY * thread_p, BTID * btid,
			     VPID * pg_vpid, BTREE_STATS * stat_info_p,
			     bool * found_p)
{
  PAGE_PTR P_page = NULL, C_page = NULL;
  VPID P_vpid, C_vpid;
  BTREE_NODE_HEADER node_header;
  int slot_id;
  short node_type;
  NON_LEAF_REC nleaf;
  RECDES rec = RECDES_INITIALIZER;
  int est_page_size, free_space;
  int key_cnt = 0;
  int depth = 0;
  double prob = 1.0;		/* Acceptance probability */
  int retry = 0;

  assert (stat_info_p != NULL);
  assert (found_p != NULL);

  *found_p = false;		/* init */

  VPID_SET_NULL (pg_vpid);

  /* read the root page */
  P_vpid.volid = btid->vfid.volid;
  P_vpid.pageid = btid->root_pageid;
  P_page =
    btree_pgbuf_fix (thread_p, &(btid->vfid), &P_vpid,
		     PGBUF_LATCH_READ, PGBUF_UNCONDITIONAL_LATCH,
		     PAGE_BTREE_ROOT);
  if (P_page == NULL)
    {
      goto error;
    }

  if (btree_read_node_header (NULL, P_page, &node_header) != NO_ERROR)
    {
      goto error;
    }
  assert (node_header.node_level > 1);

  node_type = BTREE_NON_LEAF_NODE;
  key_cnt = node_header.key_cnt;

  est_page_size = (int) (DB_PAGESIZE - (spage_header_size () +
					NODE_HEADER_SIZE +
					spage_slot_size ()));
  assert (est_page_size > 0);

  while (node_type == BTREE_NON_LEAF_NODE)
    {
      depth++;

      /* get the randomized child page to follow */

      if (key_cnt < 0 || spage_number_of_records (P_page) <= 1)
	{			/* node record underflow */
	  er_log_debug (ARG_FILE_LINE, "btree_find_AR_sampling_leaf:"
			" node key count underflow: %d. Operation Ignored.",
			key_cnt);
	  goto error;
	}

      /* When key_cnt == 0 and the page has a child link,
       * this means there actually exists a key.
       * In this case, follow the slot whose slotid is 1.
       */
      slot_id = (int) (drand48 () * key_cnt);
      slot_id = MAX (slot_id, 1);

      if (spage_get_record (P_page, slot_id, &rec, PEEK) != S_SUCCESS)
	{
	  goto error;
	}

      btree_read_fixed_portion_of_non_leaf_record (&rec, &nleaf);
      C_vpid = nleaf.pnt;
      C_page =
	btree_pgbuf_fix (thread_p, &(btid->vfid), &C_vpid,
			 PGBUF_LATCH_READ, PGBUF_UNCONDITIONAL_LATCH,
			 PAGE_BTREE);
      if (C_page == NULL)
	{
	  goto error;
	}

      pgbuf_unfix_and_init (thread_p, P_page);

      if (btree_read_node_header (NULL, C_page, &node_header) != NO_ERROR)
	{
	  goto error;
	}

      node_type = BTREE_GET_NODE_TYPE (node_header.node_level);
      key_cnt = node_header.key_cnt;

      /* update Acceptance probability */

      free_space = spage_max_space_for_new_record (thread_p, C_page);
      assert (est_page_size > free_space);

      prob *=
	(((double) est_page_size) - free_space) / ((double) est_page_size);

      P_page = C_page;
      C_page = NULL;
      P_vpid = C_vpid;
    }

  assert (node_header.node_level == 1);

  if (key_cnt != 0)
    {
      goto end;			/* OK */
    }

again:

  if (retry++ > 10)
    {
      goto end;			/* give up */
    }

  /* fix the next leaf page and set slot_id and oid_pos if it exists. */
  VPID_COPY (&C_vpid, &node_header.next_vpid);
  if (!VPID_ISNULL (&C_vpid))
    {
      C_page = btree_pgbuf_fix (thread_p, &(btid->vfid), &C_vpid,
				PGBUF_LATCH_READ, PGBUF_UNCONDITIONAL_LATCH,
				PAGE_BTREE);
      if (C_page == NULL)
	{
	  goto error;
	}

      /* unfix the previous leaf page if it is fixed. */
      if (P_page != NULL)
	{
	  pgbuf_unfix_and_init (thread_p, P_page);
	  /* do not clear bts->P_vpid for UNCONDITIONAL lock request handling */
	}

      /* check if the current leaf page has valid slots */
      if (C_page != NULL)
	{
	  if (btree_read_node_header (NULL, C_page, &node_header) != NO_ERROR)
	    {
	      goto error;
	    }

	  key_cnt = node_header.key_cnt;

	  if (key_cnt <= 0)
	    {			/* empty page */
	      /* keep empty leaf page's stat */
	      stat_info_p->leafs++;

	      free_space = spage_max_space_for_new_record (thread_p, C_page);
	      stat_info_p->tot_free_space += free_space;

	      P_page = C_page;
	      C_page = NULL;
	      goto again;
	    }

	  P_vpid = C_vpid;
	  P_page = C_page;
	}
    }

  /* NOTE that we do NOT release the page latch on P here */
end:

  assert (P_page != NULL);

  /* keep leaf page's stat */
  stat_info_p->leafs++;

  free_space = spage_max_space_for_new_record (thread_p, P_page);
  stat_info_p->tot_free_space += free_space;

  *pg_vpid = P_vpid;

  assert_release (stat_info_p->height == depth + 1);

  /* do Acceptance/Rejection sampling */
  if (drand48 () < prob)
    {
      /* Acceptance */
      *found_p = true;
    }
  else
    {
      /* Rejection */
      assert (*found_p == false);
    }

  return P_page;

error:

  if (P_page)
    {
      pgbuf_unfix_and_init (thread_p, P_page);
    }
  if (C_page)
    {
      pgbuf_unfix_and_init (thread_p, C_page);
    }

  return NULL;
}
