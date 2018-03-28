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
 * statistics_sr.c - statistics manager (server)
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <string.h>
#include <math.h>

#include "xserver_interface.h"
#include "memory_alloc.h"
#include "statistics_sr.h"
#include "object_representation.h"
#include "error_manager.h"
#include "storage_common.h"
#include "system_catalog.h"
#include "btree.h"
#include "extendible_hash.h"
#include "heap_file.h"
#include "db.h"

#define SQUARE(n) ((n)*(n))

/* Used by the "stats_update_all_statistics" routine to create the list of all
   classes from the extendible hashing directory used by the catalog manager. */
typedef struct class_id_list CLASS_ID_LIST;
struct class_id_list
{
  OID class_id;
  CLASS_ID_LIST *next;
};

static void stats_free_class_list (CLASS_ID_LIST * clsid_list);
static int stats_get_class_list (THREAD_ENTRY * thread_p, void *key,
				 void *val, void *args);

/*
 * xstats_update_statistics () -  Updates the statistics for the objects
 *                                of a given class
 *   return:
 *   class_id(in): Identifier of the class
 *   update_stats(in): true iff UPDATE
 *   with_fullscan(in): true iff WITH FULLSCAN
 *
 * Note: It first retrieves the whole catalog information about this class,
 *       including all possible forms of disk representations for the instance
 *       objects. Then, it performs a complete pass on the heap file of the
 *       class, reading in all of the instance objects one by one and
 *       calculating the ranges of numeric attribute values (ie. min. & max.
 *       values for each numeric attribute).
 *
 *       During this pass on the heap file, these values are maintained
 *       separately for objects with the same representation. Each minimum and
 *       maximum value is initialized when the first instance of the class
 *       with the corresponding representation is encountered. These values are
 *       continually updated as attribute values exceeding the known range are
 *       encountered. At the end of this pass, these individual ranges for
 *       each representation are uniformed in the last (the current)
 *       representation, building the global range values for the attributes
 *       of the class. Then, the btree statistical information is obtained for
 *       each attribute that is indexed and stored in this final representation
 *       structure. Finally, a new timestamp is obtained for these class
 *       statistics and they are stored to disk within the catalog structure
 *       for the last class representation.
 */
int
xstats_update_statistics (THREAD_ENTRY * thread_p, OID * class_id_p,
			  bool update_stats, bool with_fullscan)
{
  CLS_INFO *cls_info_p = NULL;
  REPR_ID repr_id;
  DISK_REPR *disk_repr_p = NULL;
  DISK_ATTR *disk_attr_p = NULL;
  BTREE_STATS *btree_stats_p = NULL;
  int npages;
  INT64 estimated_nobjs;
  char *class_name = NULL;
  char *index_name = NULL;
  CIRP_CT_INDEX_STATS index_stats;
  int i, j;
  int error_code = NO_ERROR;

  cls_info_p = catalog_get_class_info (thread_p, class_id_p);
  if (cls_info_p == NULL)
    {
      goto error;
    }

  class_name = heap_get_class_name (thread_p, class_id_p);
  assert (class_name != NULL);

  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
	  ER_LOG_STARTED_TO_UPDATE_STATISTICS, 4,
	  class_name ? class_name : "*UNKNOWN-TABLE*", class_id_p->volid,
	  class_id_p->pageid, class_id_p->slotid);

  /* if class information was not obtained */
  if (cls_info_p->hfid.vfid.fileid < 0 || cls_info_p->hfid.vfid.volid < 0)
    {
      /* The class does not have a heap file (i.e. it has no instances);
         so no statistics can be obtained for this class;
         just set to 0 and return. */

      cls_info_p->tot_pages = 0;
      cls_info_p->tot_objects = 0;

      error_code = catalog_add_class_info (thread_p, class_id_p, cls_info_p);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}

      goto end;
    }

  error_code =
    catalog_get_last_representation_id (thread_p, class_id_p, &repr_id);
  if (error_code != NO_ERROR)
    {
      goto error;
    }

  disk_repr_p = catalog_get_representation (thread_p, class_id_p, repr_id);
  if (disk_repr_p == NULL)
    {
      goto error;
    }

  npages = 0;
  estimated_nobjs = 0;

  /* do not use estimated npages, get correct info */
  npages =
    file_get_numpages (thread_p, &(cls_info_p->hfid.vfid), NULL, NULL, NULL);
  cls_info_p->tot_pages = MAX (npages, 0);

  error_code =
    heap_estimate_num_objects (thread_p, &(cls_info_p->hfid),
			       &estimated_nobjs);
  if (error_code != NO_ERROR)
    {
      assert (cls_info_p->tot_objects >= 0);
      goto error;
    }

  cls_info_p->tot_objects = estimated_nobjs;

  /* update the index statistics for each attribute */

  for (i = 0; i < disk_repr_p->n_fixed + disk_repr_p->n_variable; i++)
    {
      if (i < disk_repr_p->n_fixed)
	{
	  disk_attr_p = disk_repr_p->fixed + i;
	}
      else
	{
	  disk_attr_p = disk_repr_p->variable + (i - disk_repr_p->n_fixed);
	}

      for (j = 0, btree_stats_p = disk_attr_p->bt_stats;
	   j < disk_attr_p->n_btstats; j++, btree_stats_p++)
	{
	  assert_release (!BTID_IS_NULL (&btree_stats_p->btid));
	  assert_release (btree_stats_p->pkeys_size > 0);
	  assert_release (btree_stats_p->pkeys_size <= BTREE_STATS_PKEYS_NUM);

	  error_code =
	    btree_get_stats (thread_p, class_id_p, btree_stats_p,
			     with_fullscan);
	  if (error_code != NO_ERROR)
	    {
	      goto error;
	    }

	  assert (btree_stats_p->keys >= 0);
	  assert (btree_stats_p->leafs >= 1);

	  cls_info_p->tot_objects =
	    MAX (btree_stats_p->keys, cls_info_p->tot_objects);

	  assert (index_name == NULL);
	  if (heap_get_indexname_of_btid
	      (thread_p, class_id_p, &btree_stats_p->btid,
	       &index_name) != NO_ERROR)
	    {
	      index_name = NULL;
	    }

	  /* update db_index_stats catalog table
	   */
	  assert (class_name != NULL);
	  assert (index_name != NULL);
	  if (class_name != NULL && index_name != NULL)
	    {
	      assert (strlen (class_name) < SM_MAX_IDENTIFIER_LENGTH);
	      assert (strlen (index_name) < SM_MAX_IDENTIFIER_LENGTH);

	      index_stats.table_name = class_name;
	      index_stats.index_name = index_name;
	      index_stats.pages = btree_stats_p->pages;
	      index_stats.leafs = btree_stats_p->leafs;
	      index_stats.height = btree_stats_p->height;
	      index_stats.keys = btree_stats_p->keys;
	      index_stats.leaf_space_free = btree_stats_p->tot_free_space;

	      index_stats.leaf_pct_free =
		((double) btree_stats_p->tot_free_space /
		 ((double) btree_stats_p->leafs * DB_PAGESIZE)) * 100;

	      assert (btree_stats_p->num_table_vpids >= 1);
	      assert (btree_stats_p->num_user_pages_mrkdelete >= 0);
	      assert (btree_stats_p->num_allocsets >= 1);
	      index_stats.num_table_vpids = btree_stats_p->num_table_vpids;
	      index_stats.num_user_pages_mrkdelete =
		btree_stats_p->num_user_pages_mrkdelete;
	      index_stats.num_allocsets = btree_stats_p->num_allocsets;

	      error_code = qexec_update_index_stats (thread_p, &index_stats);
	      if (error_code != NO_ERROR)
		{
		  goto error;
		}
	    }

	  if (index_name)
	    {
	      free_and_init (index_name);
	    }
	}			/* for (j = 0; ...) */
    }				/* for (i = 0; ...) */

  /* replace the current disk representation structure/information in the
     catalog with the newly computed statistics */
  if (update_stats)
    {
      error_code =
	catalog_add_representation (thread_p, class_id_p, repr_id,
				    disk_repr_p);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}

      cls_info_p->time_stamp = stats_get_time_stamp ();	/* current system time */

      error_code = catalog_add_class_info (thread_p, class_id_p, cls_info_p);
      if (error_code != NO_ERROR)
	{
	  goto error;
	}

      /* remove XASL cache entries which is relevant with that class */
      if (!OID_IS_ROOTOID (class_id_p))
	{
	  error_code =
	    qexec_remove_xasl_cache_ent_by_class (thread_p, class_id_p, 1);
	  if (error_code != NO_ERROR)
	    {
	      er_log_debug (ARG_FILE_LINE,
			    "xstats_update_statistics:"
			    " qexec_remove_xasl_cache_ent_by_class"
			    " failed for class { %d %d %d }\n",
			    class_id_p->pageid, class_id_p->slotid,
			    class_id_p->volid);
	    }
	}
    }

end:
  if (disk_repr_p)
    {
      catalog_free_representation (disk_repr_p);
    }

  if (cls_info_p)
    {
      catalog_free_class_info (cls_info_p);
    }

  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
	  ER_LOG_FINISHED_TO_UPDATE_STATISTICS, 5,
	  class_name ? class_name : "*UNKNOWN-TABLE*",
	  class_id_p->volid, class_id_p->pageid, class_id_p->slotid,
	  error_code);

  if (index_name)
    {
      free_and_init (index_name);
    }
  if (class_name)
    {
      free_and_init (class_name);
    }

  return error_code;

error:

  if (error_code == NO_ERROR && (error_code = er_errid ()) == NO_ERROR)
    {
      error_code = ER_FAILED;
    }
  goto end;
}

/*
 * xstats_update_all_statistics () - Updates the statistics
 *                                   for all the classes of the database
 *   return:
 *   update_stats(in): true iff UPDATE
 *   with_fullscan(in): true iff WITH FULLSCAN
 *
 * Note: It performs this by getting the list of all classes existing in the
 *       database and their OID's from the catalog's class collection
 *       (maintained in an extendible hashing structure) and calling the
 *       "xstats_update_statistics" function for each one of the elements
 *       of this list one by one.
 */
int
xstats_update_all_statistics (THREAD_ENTRY * thread_p, bool update_stats,
			      bool with_fullscan)
{
  int error;
  OID class_id;
  CLASS_ID_LIST *class_id_list_p = NULL, *class_id_item_p;

  ehash_map (thread_p, &catalog_Id.xhid, stats_get_class_list,
	     (void *) &class_id_list_p);

  for (class_id_item_p = class_id_list_p; class_id_item_p->next != NULL;
       class_id_item_p = class_id_item_p->next)
    {
      class_id.volid = class_id_item_p->class_id.volid;
      class_id.pageid = class_id_item_p->class_id.pageid;
      class_id.slotid = class_id_item_p->class_id.slotid;

      error =
	xstats_update_statistics (thread_p, &class_id, update_stats,
				  with_fullscan);
      if (error != NO_ERROR)
	{
	  stats_free_class_list (class_id_list_p);
	  class_id_list_p = NULL;

	  return error;
	}
    }

  stats_free_class_list (class_id_list_p);
  class_id_list_p = NULL;

  return NO_ERROR;
}

/*
 * xstats_get_statistics_from_server () - Retrieves the class statistics
 *   return: buffer contaning class statistics, or NULL on error
 *   class_id(in): Identifier of the class
 *   time_stamp(in):
 *   length(in): Length of the buffer
 *
 * Note: This function retrieves the statistics for the given class from the
 *       catalog manager and stores them into a buffer. Note that since the
 *       statistics are kept on the current (last) representation structure of
 *       the catalog, only this structure is retrieved. Note further that
 *       since the statistics are used only on the client side they are not
 *       put into a structure here on the server side (not even temporarily),
 *       but stored into a buffer area to be transmitted to the client side.
 */
char *
xstats_get_statistics_from_server (THREAD_ENTRY * thread_p, OID * class_id_p,
				   unsigned int time_stamp, int *length_p)
{
  CLS_INFO *cls_info_p;
  REPR_ID repr_id;
  DISK_REPR *disk_repr_p;
  DISK_ATTR *disk_attr_p;
  BTREE_STATS *btree_stats_p;
  int npages;
  int num_table_vpids, num_user_pages_mrkdelete, num_allocsets;
  INT64 estimated_nobjs, max_index_keys;
  int i, j, k, size, n_attrs, tot_n_btstats, tot_key_info_size;
  char *ptr, *start_p;

  *length_p = -1;

  cls_info_p = catalog_get_class_info (thread_p, class_id_p);
  if (cls_info_p == NULL)
    {
      return NULL;
    }

  if (time_stamp > 0 && time_stamp >= cls_info_p->time_stamp)
    {
      catalog_free_class_info (cls_info_p);
      *length_p = 0;
      return NULL;
    }

  if (catalog_get_last_representation_id (thread_p, class_id_p, &repr_id) !=
      NO_ERROR)
    {
      catalog_free_class_info (cls_info_p);
      return NULL;
    }

  disk_repr_p = catalog_get_representation (thread_p, class_id_p, repr_id);
  if (disk_repr_p == NULL)
    {
      catalog_free_class_info (cls_info_p);
      return NULL;
    }

  n_attrs = disk_repr_p->n_fixed + disk_repr_p->n_variable;

  tot_n_btstats = tot_key_info_size = 0;
  for (i = 0; i < n_attrs; i++)
    {
      if (i < disk_repr_p->n_fixed)
	{
	  disk_attr_p = disk_repr_p->fixed + i;
	}
      else
	{
	  disk_attr_p = disk_repr_p->variable + (i - disk_repr_p->n_fixed);
	}

      tot_n_btstats += disk_attr_p->n_btstats;
      for (j = 0, btree_stats_p = disk_attr_p->bt_stats;
	   j < disk_attr_p->n_btstats; j++, btree_stats_p++)
	{
	  assert (btree_stats_p->pkeys_size <= BTREE_STATS_PKEYS_NUM);
	  tot_key_info_size += OR_INT_SIZE;	/* pkeys_size */
	  tot_key_info_size += (btree_stats_p->pkeys_size * OR_INT_SIZE);	/* pkeys[] */
	}
    }

  size = (OR_INT_SIZE		/* time_stamp of CLS_INFO */
	  + OR_BIGINT_ALIGNED_SIZE	/* tot_objects of CLS_INFO */
	  + OR_INT_SIZE		/* tot_pages of CLS_INFO */
	  + OR_INT_SIZE		/* n_attrs from DISK_REPR */
	  + OR_INT_SIZE		/* num_table_vpids of heap fhdr */
	  + OR_INT_SIZE		/* num_user_pages_mrkdelete of heap fhdr */
	  + OR_INT_SIZE		/* num_allocsets of heap fhdr */
	  + (OR_INT_SIZE	/* id of DISK_ATTR */
	     + OR_INT_SIZE	/* type of DISK_ATTR */
	     + OR_INT_SIZE	/* n_btstats of DISK_ATTR */
	  ) * n_attrs);		/* number of attributes */

  size += ((OR_BTID_ALIGNED_SIZE	/* btid of BTREE_STATS */
	    + OR_INT_SIZE	/* leafs of BTREE_STATS */
	    + OR_INT_SIZE	/* pages of BTREE_STATS */
	    + OR_INT_SIZE	/* height of BTREE_STATS */
	    + OR_BIGINT_ALIGNED_SIZE	/* keys of BTREE_STATS */
	    + OR_BIGINT_ALIGNED_SIZE	/* tot_free_space of BTREE_STATS */
	    + OR_INT_SIZE	/* num_table_vpids of index fhdr */
	    + OR_INT_SIZE	/* num_user_pages_mrkdelete of index fhdr */
	    + OR_INT_SIZE	/* num_allocsets of index fhdr */
	   ) * tot_n_btstats);	/* total number of indexes */

  size += tot_key_info_size;	/* pkeys_size, pkeys[] of BTREE_STATS */

  size += OR_BIGINT_ALIGNED_SIZE;	/* max_index_keys */

  start_p = (char *) malloc (size);
  if (start_p == NULL)
    {
      catalog_free_representation (disk_repr_p);
      catalog_free_class_info (cls_info_p);

      return NULL;
    }

  memset (start_p, 0, size);

  ptr = or_pack_int (start_p, cls_info_p->time_stamp);

  npages = 0;
  estimated_nobjs = max_index_keys = 0;
  num_table_vpids = num_user_pages_mrkdelete = num_allocsets = 0;

  assert (cls_info_p->tot_objects >= 0);
  assert (cls_info_p->tot_pages >= 0);

  if (HFID_IS_NULL (&cls_info_p->hfid))
    {
      /* The class does not have a heap file (i.e. it has no instances);
       * so no statistics can be obtained for this class
       */
      ptr = PTR_ALIGN (ptr, MAX_ALIGNMENT);
      ptr = or_pack_int64 (ptr, cls_info_p->tot_objects);	/* #objects */

      ptr = or_pack_int (ptr, cls_info_p->tot_pages);	/* #pages */

      ptr = or_pack_int (ptr, 0);	/* num_table_vpids */
      ptr = or_pack_int (ptr, 0);	/* num_user_pages_mrkdelete */
      ptr = or_pack_int (ptr, 0);	/* num_allocsets */
    }
  else
    {
      /* use estimates from the heap since it is likely that its estimates
       * are more accurate than the ones gathered at update statistics time
       */
      if (heap_estimate_num_objects
	  (thread_p, &(cls_info_p->hfid), &estimated_nobjs) != NO_ERROR)
	{
	  catalog_free_representation (disk_repr_p);
	  catalog_free_class_info (cls_info_p);
	  free_and_init (start_p);

	  return NULL;
	}

      /* heuristic is that big nobjs is better than small */
      estimated_nobjs = MAX (estimated_nobjs, cls_info_p->tot_objects);

      ptr = PTR_ALIGN (ptr, MAX_ALIGNMENT);
      ptr = or_pack_int64 (ptr, estimated_nobjs);	/* #objects */

      /* do not use estimated npages, get correct info */
      assert (!VFID_ISNULL (&cls_info_p->hfid.vfid));
      npages =
	file_get_numpages (thread_p, &cls_info_p->hfid.vfid,
			   &num_table_vpids, &num_user_pages_mrkdelete,
			   &num_allocsets);
      if (npages < 0)
	{
	  /* cannot get #pages from the heap, use ones from the catalog */
	  npages = cls_info_p->tot_pages;
	}

      ptr = or_pack_int (ptr, npages);	/* #pages */

      assert (num_table_vpids >= 0);
      assert (num_user_pages_mrkdelete >= 0);
      assert (num_allocsets >= 0);
      ptr = or_pack_int (ptr, num_table_vpids);	/* num_table_vpids */
      ptr = or_pack_int (ptr, num_user_pages_mrkdelete);	/* num_user_pages_mrkdelete */
      ptr = or_pack_int (ptr, num_allocsets);	/* num_allocsets */
    }

  ptr = or_pack_int (ptr, n_attrs);

  /* put the statistics information of each attribute to the buffer */
  for (i = 0; i < n_attrs; i++)
    {
      if (i < disk_repr_p->n_fixed)
	{
	  disk_attr_p = disk_repr_p->fixed + i;
	}
      else
	{
	  disk_attr_p = disk_repr_p->variable + (i - disk_repr_p->n_fixed);
	}

      ptr = or_pack_int (ptr, disk_attr_p->id);

      ptr = or_pack_int (ptr, disk_attr_p->type);

      ptr = or_pack_int (ptr, disk_attr_p->n_btstats);

      for (j = 0, btree_stats_p = disk_attr_p->bt_stats;
	   j < disk_attr_p->n_btstats; j++, btree_stats_p++)
	{
#if !defined(NDEBUG)
	  {
	    VPID root_page;

	    root_page.volid = btree_stats_p->btid.vfid.volid;
	    root_page.pageid = btree_stats_p->btid.root_pageid;

	    assert (pgbuf_is_valid_page
		    (thread_p, &root_page, false, NULL, NULL) == DISK_VALID);
	  }
#endif
	  ptr = or_pack_btid (ptr, &btree_stats_p->btid);

	  /* defense for not gathered statistics */
	  btree_stats_p->leafs = MAX (1, btree_stats_p->leafs);
	  btree_stats_p->pages = MAX (1, btree_stats_p->pages);
	  btree_stats_p->height = MAX (1, btree_stats_p->height);

	  /* If the btree file has currently more pages than when we gathered
	     statistics, assume that all growth happen at the leaf level. If
	     the btree is smaller, we use the gathered statistics since the
	     btree may have an external file (unknown at this level) to keep
	     overflow keys. */
	  npages =
	    file_get_numpages (thread_p, &btree_stats_p->btid.vfid,
			       &btree_stats_p->num_table_vpids,
			       &btree_stats_p->num_user_pages_mrkdelete,
			       &btree_stats_p->num_allocsets);

	  assert (btree_stats_p->num_table_vpids >= 0);
	  assert (btree_stats_p->num_user_pages_mrkdelete >= 0);
	  assert (btree_stats_p->num_allocsets >= 0);

	  if (npages > btree_stats_p->pages)
	    {
	      ptr =
		or_pack_int (ptr,
			     (btree_stats_p->leafs +
			      (npages - btree_stats_p->pages)));

	      ptr = or_pack_int (ptr, npages);
	    }
	  else
	    {
	      ptr = or_pack_int (ptr, btree_stats_p->leafs);

	      ptr = or_pack_int (ptr, btree_stats_p->pages);
	    }

	  ptr = or_pack_int (ptr, btree_stats_p->height);

	  /* check and handle with estimation,
	   * since pkeys[] is not gathered before update stats
	   */
	  if (estimated_nobjs > 0)
	    {
	      /* is non-empty index */
	      btree_stats_p->keys = MAX (btree_stats_p->keys, 1);

	      /* If the estimated objects from heap manager is greater than
	       * the estimate when the statistics were gathered, assume that
	       * the difference is in distinct keys.
	       */
	      if (cls_info_p->tot_objects > 0
		  && estimated_nobjs > cls_info_p->tot_objects)
		{
		  btree_stats_p->keys +=
		    (estimated_nobjs - cls_info_p->tot_objects);
		}
	    }

	  ptr = PTR_ALIGN (ptr, MAX_ALIGNMENT);
	  ptr = or_pack_int64 (ptr, btree_stats_p->keys);

	  assert_release (btree_stats_p->leafs >= 1);
	  assert_release (btree_stats_p->pages >= 1);
	  assert_release (btree_stats_p->height >= 1);
	  assert_release (btree_stats_p->keys >= 0);

	  assert (btree_stats_p->pkeys_size > 0);
	  assert (btree_stats_p->pkeys_size <= BTREE_STATS_PKEYS_NUM);
	  ptr = or_pack_int (ptr, btree_stats_p->pkeys_size);

	  for (k = 0; k < btree_stats_p->pkeys_size; k++)
	    {
	      /* check and handle with estimation,
	       * since pkeys[] is not gathered before update stats
	       */
	      if (estimated_nobjs > 0)
		{
		  /* is non-empty index */
		  btree_stats_p->pkeys[k] = MAX (btree_stats_p->pkeys[k], 1);
		}

	      ptr = or_pack_int (ptr, btree_stats_p->pkeys[k]);
	    }

	  assert (btree_stats_p->tot_free_space >= 0);
	  assert (btree_stats_p->leafs == 0
		  || btree_stats_p->tot_free_space <
		  btree_stats_p->leafs * DB_PAGESIZE);
	  ptr = PTR_ALIGN (ptr, MAX_ALIGNMENT);
	  ptr = or_pack_int64 (ptr, btree_stats_p->tot_free_space);

	  assert (btree_stats_p->num_table_vpids >= 1);
	  assert (btree_stats_p->num_user_pages_mrkdelete >= 0);
	  assert (btree_stats_p->num_allocsets >= 1);
	  ptr = or_pack_int (ptr, btree_stats_p->num_table_vpids);
	  ptr = or_pack_int (ptr, btree_stats_p->num_user_pages_mrkdelete);
	  ptr = or_pack_int (ptr, btree_stats_p->num_allocsets);

	  /* collect maximum index keys info */
	  max_index_keys = MAX (btree_stats_p->keys, max_index_keys);
	}			/* for (j = 0, ...) */
    }

  ptr = PTR_ALIGN (ptr, MAX_ALIGNMENT);
  ptr = or_pack_int64 (ptr, max_index_keys);

  catalog_free_representation (disk_repr_p);
  catalog_free_class_info (cls_info_p);

  *length_p = CAST_STRLEN (ptr - start_p);

  return start_p;
}

/*
 * qst_get_class_list () - Build the list of OIDs of classes
 *   return: NO_ERROR or error code
 *   key(in): next class OID to be added to the list
 *   val(in): data value associated with the class id on the ext. hash entry
 *   args(in): class list being built
 *
 * Note: This function builds the next node of the class id list. It is passed
 *       to the ehash_map function to be called once on each item kept on the
 *       extendible hashing structure used by the catalog manager.
 */
static int
stats_get_class_list (UNUSED_ARG THREAD_ENTRY * thread_p,
		      UNUSED_ARG void *key, UNUSED_ARG void *val, void *args)
{
  CLASS_ID_LIST *class_id_item_p, **p;

  p = (CLASS_ID_LIST **) args;
  class_id_item_p = (CLASS_ID_LIST *) malloc (sizeof (CLASS_ID_LIST));
  if (class_id_item_p == NULL)
    {
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  class_id_item_p->class_id.volid = ((OID *) key)->volid;
  class_id_item_p->class_id.pageid = ((OID *) key)->pageid;
  class_id_item_p->class_id.slotid = ((OID *) key)->slotid;
  class_id_item_p->next = *p;

  *p = class_id_item_p;

  return NO_ERROR;
}

/*
 * qst_free_class_list () - Frees the dynamic memory area used by the passed
 *                          linked list
 *   return: void
 *   class_id_list(in): list to be freed
 */
static void
stats_free_class_list (CLASS_ID_LIST * class_id_list_p)
{
  CLASS_ID_LIST *p, *next_p;

  if (class_id_list_p == NULL)
    {
      return;
    }

  for (p = class_id_list_p; p; p = next_p)
    {
      next_p = p->next;
      free_and_init (p);
    }

}


/*
 * stats_get_time_stamp () - returns the current system time
 *   return: current system time
 */
unsigned int
stats_get_time_stamp (void)
{
  time_t tloc;

  return (unsigned int) time (&tloc);
}


#if defined(RYE_DEBUG)
/*
 * stats_dump_class_statistics () - Dumps the given statistics about a class
 *   return:
 *   class_stats(in): statistics to be printed
 *   fpp(in):
 */
void
stats_dump_class_statistics (CLASS_STATS * class_stats, FILE * fpp)
{
  int i, j, k;
  const char *prefix = "";
  time_t tloc;

  if (class_stats == NULL)
    {
      return;
    }

  fprintf (fpp, "\nCLASS STATISTICS\n");
  fprintf (fpp, "****************\n");
  tloc = (time_t) class_stats->time_stamp;
  fprintf (fpp, " Timestamp: %s", ctime (&tloc));
  fprintf (fpp, " Total Pages in Class Heap: %d\n",
	   class_stats->heap_num_pages);
  fprintf (fpp, " Total Objects: %d\n", class_stats->heap_num_objects);
  fprintf (fpp, " Number of attributes: %d\n", class_stats->n_attrs);

  for (i = 0; i < class_stats->n_attrs; i++)
    {
      fprintf (fpp, "\n Attribute :\n");
      fprintf (fpp, "    id: %d\n", class_stats->attr_stats[i].id);
      fprintf (fpp, "    Type: %s\n",
	       qdump_data_type_string (class_stats->attr_stats[i].type));

#if 0
      switch (class_stats->attr_stats[i].type)
	{
	case DB_TYPE_INTEGER:
	  fprintf (fpp, "DB_TYPE_INTEGER \n");
	  break;

	case DB_TYPE_BIGINT:
	  fprintf (fpp, "DB_TYPE_BIGINT \n");
	  break;

	case DB_TYPE_DOUBLE:
	  fprintf (fpp, "DB_TYPE_DOUBLE \n");
	  break;

	case DB_TYPE_VARCHAR:
	  fprintf (fpp, "DB_TYPE_VARCHAR \n");
	  break;

	case DB_TYPE_OBJECT:
	  fprintf (fpp, "DB_TYPE_OBJECT \n");
	  break;

	case DB_TYPE_SEQUENCE:
	  fprintf (fpp, "DB_TYPE_SEQUENCE \n");
	  break;

	case DB_TYPE_TIME:
	  fprintf (fpp, "DB_TYPE_TIME \n");
	  break;

	case DB_TYPE_DATETIME:
	  fprintf (fpp, "DB_TYPE_DATETIME \n");
	  break;

	case DB_TYPE_DATE:
	  fprintf (fpp, "DB_TYPE_DATE \n");
	  break;

	case DB_TYPE_VARIABLE:
	  fprintf (fpp, "DB_TYPE_VARIABLE  \n");
	  break;

	case DB_TYPE_SUB:
	  fprintf (fpp, "DB_TYPE_SUB \n");
	  break;

	case DB_TYPE_NULL:
	  fprintf (fpp, "DB_TYPE_NULL \n");
	  break;

	case DB_TYPE_NUMERIC:
	  fprintf (fpp, "DB_TYPE_NUMERIC \n");
	  break;

	case DB_TYPE_VARBIT:
	  fprintf (fpp, "DB_TYPE_VARBIT \n");
	  break;

	default:
	  break;
	}
#endif

      fprintf (fpp, "    BTree statistics:\n");

      for (j = 0; j < class_stats->attr_stats[i].n_btstats; j++)
	{
	  BTREE_STATS *bt_statsp = &class_stats->attr_stats[i].bt_stats[j];
	  fprintf (fpp, "        BTID: { %d , %d }\n",
		   bt_statsp->btid.vfid.volid, bt_statsp->btid.vfid.fileid);
	  fprintf (fpp, "        Cardinality: %d (", bt_statsp->keys);

	  prefix = "";
	  assert (bt_statsp->pkeys_size <= BTREE_STATS_PKEYS_NUM);
	  for (k = 0; k < bt_statsp->pkeys_size; k++)
	    {
	      fprintf (fpp, "%s%d", prefix, bt_statsp->pkeys[k]);
	      prefix = ",";
	    }

	  fprintf (fpp, ") ,");
	  fprintf (fpp, " Total Pages: %d , Leaf Pages: %d ,"
		   " Height: %d , Free: %f%%\n",
		   bt_statsp->pages, bt_statsp->leafs, bt_statsp->height,
		   ((double) bt_stats_p->tot_free_space /
		    ((double) bt_stats_p->leafs * DB_PAGESIZE)) * 100);
	}
      fprintf (fpp, "\n");
    }

  fprintf (fpp, "\n\n");
}
#endif /* RYE_DEBUG */
