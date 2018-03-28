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
 * statistics_cl.c - statistics manager (client)
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>

#include "object_representation.h"
#include "statistics.h"
#include "object_primitive.h"
#include "memory_alloc.h"
#include "work_space.h"
#include "schema_manager.h"
#include "network_interface_cl.h"
#include "db_date.h"

static CLASS_STATS *stats_client_unpack_statistics (char *buffer);

/*
 * stats_get_statistics () - Get class statistics
 *   return:
 *   classoid(in): OID of the class
 *   timestamp(in):
 *
 * Note: This function provides an easier interface for the client for
 *       obtaining statistics to the client side by taking care of the
 *       communication details . (Note that the upper levels shouldn't have to
 *       worry about the communication buffer.)
 */
CLASS_STATS *
stats_get_statistics (OID * class_oid_p, unsigned int time_stamp)
{
  CLASS_STATS *stats_p = NULL;
  char *buffer_p;
  int length;

  buffer_p = stats_get_statistics_from_server (class_oid_p, time_stamp, &length);
  if (buffer_p)
    {
      stats_p = stats_client_unpack_statistics (buffer_p);
      free_and_init (buffer_p);
    }

  return stats_p;
}

/*
 * qst_client_unpack_statistics () - Unpack the buffer containing statistics
 *   return: CLASS_STATS or NULL in case of error
 *   ptr(in): buffer containing the class statistics
 *
 * Note: This function unpacks the statistics on the buffer received from the
 *       server side, and builds a CLASS_STATS structure on the work space
 *       area. This structure is returned to the caller.
 */
static CLASS_STATS *
stats_client_unpack_statistics (char *ptr)
{
  CLASS_STATS *class_stats_p;
  ATTR_STATS *attr_stats_p;
  BTREE_STATS *btree_stats_p;
  int tmp;
  INT64 max_index_keys;
  int i, j, k;

  if (ptr == NULL)
    {
      return NULL;
    }

  class_stats_p = (CLASS_STATS *) db_ws_alloc (sizeof (CLASS_STATS));
  if (class_stats_p == NULL)
    {
      return NULL;
    }

  ptr = or_unpack_int (ptr, &tmp);
  class_stats_p->time_stamp = (unsigned int) tmp;

  ptr = PTR_ALIGN (ptr, MAX_ALIGNMENT);
  ptr = or_unpack_int64 (ptr, &class_stats_p->heap_num_objects);
  if (class_stats_p->heap_num_objects < 0)
    {
      assert (false);
      class_stats_p->heap_num_objects = 0;
    }

  ptr = or_unpack_int (ptr, &class_stats_p->heap_num_pages);
  if (class_stats_p->heap_num_pages < 0)
    {
      assert (false);
      class_stats_p->heap_num_pages = 0;
    }

  /* to get the doubtful statistics to be updated, need to clear timestamp */
  if (class_stats_p->heap_num_objects == 0 || class_stats_p->heap_num_pages == 0)
    {
      class_stats_p->time_stamp = 0;
    }

  ptr = or_unpack_int (ptr, &class_stats_p->num_table_vpids);
  ptr = or_unpack_int (ptr, &class_stats_p->num_user_pages_mrkdelete);
  ptr = or_unpack_int (ptr, &class_stats_p->num_allocsets);
  assert (class_stats_p->num_table_vpids >= 0);
  assert (class_stats_p->num_user_pages_mrkdelete >= 0);
  assert (class_stats_p->num_allocsets >= 0);

  ptr = or_unpack_int (ptr, &class_stats_p->n_attrs);
  if (class_stats_p->n_attrs == 0)
    {
      db_ws_free (class_stats_p);
      return NULL;
    }

  class_stats_p->attr_stats = (ATTR_STATS *) db_ws_alloc (class_stats_p->n_attrs * sizeof (ATTR_STATS));
  if (class_stats_p->attr_stats == NULL)
    {
      db_ws_free (class_stats_p);
      return NULL;
    }

  for (i = 0, attr_stats_p = class_stats_p->attr_stats; i < class_stats_p->n_attrs; i++, attr_stats_p++)
    {
      ptr = or_unpack_int (ptr, &attr_stats_p->id);

      ptr = or_unpack_int (ptr, &tmp);
      attr_stats_p->type = (DB_TYPE) tmp;

      memset (&(attr_stats_p->unused_min_value), 0, sizeof (DB_DATA));
      memset (&(attr_stats_p->unused_max_value), 0, sizeof (DB_DATA));

      ptr = or_unpack_int (ptr, &attr_stats_p->n_btstats);
      if (attr_stats_p->n_btstats <= 0)
        {
          attr_stats_p->bt_stats = NULL;
          continue;
        }

      attr_stats_p->bt_stats = (BTREE_STATS *) db_ws_alloc (attr_stats_p->n_btstats * sizeof (BTREE_STATS));
      if (attr_stats_p->bt_stats == NULL)
        {
          stats_free_statistics (class_stats_p);
          return NULL;
        }

      for (j = 0, btree_stats_p = attr_stats_p->bt_stats; j < attr_stats_p->n_btstats; j++, btree_stats_p++)
        {
          ptr = or_unpack_btid (ptr, &btree_stats_p->btid);

          ptr = or_unpack_int (ptr, &btree_stats_p->leafs);

          ptr = or_unpack_int (ptr, &btree_stats_p->pages);

          ptr = or_unpack_int (ptr, &btree_stats_p->height);

          ptr = PTR_ALIGN (ptr, MAX_ALIGNMENT);
          ptr = or_unpack_int64 (ptr, &btree_stats_p->keys);

          ptr = or_unpack_int (ptr, &btree_stats_p->pkeys_size);
          assert (btree_stats_p->pkeys_size > 0);
          assert (btree_stats_p->pkeys_size <= BTREE_STATS_PKEYS_NUM);

          /* safe code - cut-off to stats */
          if (btree_stats_p->pkeys_size > BTREE_STATS_PKEYS_NUM)
            {
              assert (false);   /* is impossible */
              btree_stats_p->pkeys_size = BTREE_STATS_PKEYS_NUM;
            }

          for (k = 0; k < BTREE_STATS_PKEYS_NUM; k++)
            {
              btree_stats_p->pkeys[k] = 0;      /* init */
            }

          for (k = 0; k < btree_stats_p->pkeys_size; k++)
            {
              ptr = or_unpack_int (ptr, &(btree_stats_p->pkeys[k]));
            }

          ptr = PTR_ALIGN (ptr, MAX_ALIGNMENT);
          ptr = or_unpack_int64 (ptr, &btree_stats_p->tot_free_space);
          assert (btree_stats_p->tot_free_space >= 0);
          assert (btree_stats_p->leafs == 0
                  || btree_stats_p->tot_free_space < (INT64) btree_stats_p->leafs * DB_PAGESIZE);

          ptr = or_unpack_int (ptr, &btree_stats_p->num_table_vpids);
          ptr = or_unpack_int (ptr, &btree_stats_p->num_user_pages_mrkdelete);
          ptr = or_unpack_int (ptr, &btree_stats_p->num_allocsets);
          assert (btree_stats_p->num_table_vpids >= 0);
          assert (btree_stats_p->num_user_pages_mrkdelete >= 0);
          assert (btree_stats_p->num_allocsets >= 0);
        }
    }

  /* correct estimated num_objects with index keys */
  ptr = PTR_ALIGN (ptr, MAX_ALIGNMENT);
  ptr = or_unpack_int64 (ptr, &max_index_keys);
  class_stats_p->heap_num_objects = MAX (max_index_keys, class_stats_p->heap_num_objects);

  return class_stats_p;
}

/*
 * stats_free_statistics () - Frees the given CLASS_STAT structure
 *   return: void
 *   class_statsp(in): class statistics to be freed
 */
void
stats_free_statistics (CLASS_STATS * class_statsp)
{
  ATTR_STATS *attr_statsp;
  int i;

  if (class_statsp)
    {
      if (class_statsp->attr_stats)
        {
          for (i = 0, attr_statsp = class_statsp->attr_stats; i < class_statsp->n_attrs; i++, attr_statsp++)
            {
              if (attr_statsp->bt_stats)
                {
                  db_ws_free (attr_statsp->bt_stats);
                  attr_statsp->bt_stats = NULL;
                }
            }
          db_ws_free (class_statsp->attr_stats);
          class_statsp->attr_stats = NULL;
        }

      db_ws_free (class_statsp);
    }
}

/*
 * stats_dump () - Dumps the given statistics about a class
 *   return:
 *   classname(in): The name of class to be printed
 *   fp(in):
 */
void
stats_dump (const char *class_name_p, FILE * file_p)
{
  MOP class_mop;
  CLASS_STATS *class_stats_p;
  ATTR_STATS *attr_stats_p;
  BTREE_STATS *bt_stats_p;
  SM_CLASS *smclass_p;
  int i, j, k, n_found;
  const char *name_p;
  const char *prefix_p = "";
  time_t tloc;

  class_mop = sm_find_class (class_name_p);
  if (class_mop == NULL)
    {
      return;
    }

  smclass_p = sm_get_class_with_statistics (class_mop);
  if (smclass_p == NULL)
    {
      return;
    }

  class_stats_p = smclass_p->stats;
  if (class_stats_p == NULL)
    {
      return;
    }

  tloc = (time_t) class_stats_p->time_stamp;

  fprintf (file_p, "\nTABLE STATISTICS\n");
  fprintf (file_p, "****************\n");
  fprintf (file_p, " Table name: %s\n", class_name_p);
  fprintf (file_p, " Timestamp: %s", ctime (&tloc));
  fprintf (file_p, " Total pages in table heap: %d\n", class_stats_p->heap_num_pages);
  fprintf (file_p, " Total rows: %ld\n", class_stats_p->heap_num_objects);
  fprintf (file_p, " Number of columns: %d\n", class_stats_p->n_attrs);
  fprintf (file_p,
           " num_table_vpids: %d , num_user_pages_mrkdelete: %d , num_allocsets: %d\n",
           class_stats_p->num_table_vpids, class_stats_p->num_user_pages_mrkdelete, class_stats_p->num_allocsets);
  fprintf (file_p, "\n");

  n_found = 0;
  for (i = 0; i < class_stats_p->n_attrs; i++)
    {
      for (j = 0; j < class_stats_p->n_attrs; j++)
        {
          attr_stats_p = &(class_stats_p->attr_stats[j]);
          if (attr_stats_p->id == i)
            {
              n_found++;        /* found */
              break;
            }
        }

      if (j >= class_stats_p->n_attrs)
        {
          /* not found */
          assert (false);
          continue;
        }

      assert (attr_stats_p != NULL);

      name_p = sm_get_att_name (class_mop, attr_stats_p->id);
      fprintf (file_p, " Column: %s\n", (name_p ? name_p : "not found"));
      fprintf (file_p, "    id: %d\n", attr_stats_p->id);
      fprintf (file_p, "    Type: %s\n", qdump_data_type_string (attr_stats_p->type));

      if (attr_stats_p->n_btstats > 0)
        {
          fprintf (file_p, "    B+tree statistics:\n");

          for (j = 0; j < attr_stats_p->n_btstats; j++)
            {
              bt_stats_p = &(attr_stats_p->bt_stats[j]);

              fprintf (file_p, "        BTID: { %d , %d }\n",
                       bt_stats_p->btid.vfid.volid, bt_stats_p->btid.vfid.fileid);
              fprintf (file_p, "        Cardinality: %ld (", bt_stats_p->keys);

              prefix_p = "";
              assert (bt_stats_p->pkeys_size > 0);
              assert (bt_stats_p->pkeys_size <= BTREE_STATS_PKEYS_NUM);
              for (k = 0; k < bt_stats_p->pkeys_size; k++)
                {
                  fprintf (file_p, "%s%d", prefix_p, bt_stats_p->pkeys[k]);
                  prefix_p = ",";
                }
              fprintf (file_p, ") ,");
              fprintf (file_p, " Total pages: %d , Leaf pages: %d ,"
                       " Height: %d , Free: %0.1f%%\n",
                       bt_stats_p->pages, bt_stats_p->leafs,
                       bt_stats_p->height,
                       ((double) bt_stats_p->tot_free_space / ((double) bt_stats_p->leafs * DB_PAGESIZE)) * 100);

              fprintf (file_p,
                       "        num_table_vpids: %d , num_user_pages_mrkdelete: %d , num_allocsets: %d\n",
                       bt_stats_p->num_table_vpids, bt_stats_p->num_user_pages_mrkdelete, bt_stats_p->num_allocsets);
            }
        }
      fprintf (file_p, "\n");
    }

  assert (n_found == class_stats_p->n_attrs);

  fprintf (file_p, "\n\n");
}
