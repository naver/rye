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

#include "log_impl.h"
#include "memory_hash.h"
#include "cas_cci_internal.h"
#include "rbl_conf.h"
#include "rbl_error_log.h"
#include "rbl_sync_log.h"
#include "rbl_sync_query.h"

typedef struct tran_query_item TRAN_QUERY_ITEM;
struct tran_query_item
{
  TRANID tran_id;
  char *query;
  TRAN_QUERY_ITEM *next;
};

typedef struct tran_query_list TRAN_QUERY_LIST;
struct tran_query_list
{
  TRAN_QUERY_ITEM *first;
  TRAN_QUERY_ITEM *last;
};

static MHT_TABLE *ht_Tran_queries;

unsigned int
rbl_tranid_hash (const void *key, const unsigned int ht_size)
{
  RBL_ASSERT (key != NULL);

  return *((const TRANID *) key) % ht_size;
}

int
rbl_compare_tranid_are_equal (const void *key1, const void *key2)
{
  return *((const TRANID *) key1) == *((const TRANID *) key2);
}

int
rbl_sync_query_init (void)
{
  int error = NO_ERROR;

  ht_Tran_queries = mht_create ("Tran Queries", 1024, rbl_tranid_hash, rbl_compare_tranid_are_equal);
  if (ht_Tran_queries == NULL)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_OUT_OF_MEMORY, 1024);
      error = RBL_OUT_OF_MEMORY;
    }

  return error;
}

int
rbl_tran_list_add (TRANID tran_id, char *query)
{
  TRAN_QUERY_LIST *list;
  TRAN_QUERY_ITEM *item;

  item = (TRAN_QUERY_ITEM *) malloc (sizeof (TRAN_QUERY_ITEM));
  if (item == NULL)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_OUT_OF_MEMORY, sizeof (TRAN_QUERY_ITEM));
      return RBL_OUT_OF_MEMORY;
    }

  item->tran_id = tran_id;
  item->query = query;
  item->next = NULL;

  list = (TRAN_QUERY_LIST *) mht_get (ht_Tran_queries, &tran_id);
  if (list == NULL)
    {
      list = (TRAN_QUERY_LIST *) malloc (sizeof (TRAN_QUERY_LIST));
      if (list == NULL)
        {
          free (item);
          RBL_ERROR (ARG_FILE_LINE, RBL_OUT_OF_MEMORY, sizeof (TRAN_QUERY_LIST));
          return RBL_OUT_OF_MEMORY;
        }

      if (mht_put (ht_Tran_queries, &item->tran_id, list) == NULL)
        {
          free (item);
          free (list);
          return ER_FAILED;
        }

      list->first = item;
      list->last = item;
    }
  else
    {
      assert (list->last != NULL);
      list->last->next = item;
      list->last = item;
    }

  return NO_ERROR;
}

static int
rbl_tran_list_free (UNUSED_ARG const void *key, void *data, UNUSED_ARG void *args)
{
  TRAN_QUERY_LIST *list;
  TRAN_QUERY_ITEM *item, *next;

  list = (TRAN_QUERY_LIST *) data;

  item = list->first;
  while (item != NULL)
    {
      free (item->query);

      next = item->next;
      free (item);
      item = next;
    }

  free (list);

  return NO_ERROR;
}

int
rbl_sync_execute_query (RBL_SYNC_CONTEXT * ctx, TRANID tran_id, int gid)
{
  CCI_CONN *conn;
  CCI_STMT stmt;
  TRAN_QUERY_LIST *list;
  TRAN_QUERY_ITEM *item;
  int error;

  list = (TRAN_QUERY_LIST *) mht_get (ht_Tran_queries, &tran_id);
  if (list == NULL)
    {
      return NO_ERROR;
    }

  conn = rbl_conf_get_destdb_conn (RBL_SYNC);
  RBL_ASSERT (conn != NULL);
  cci_set_autocommit (conn, CCI_AUTOCOMMIT_TRUE);

  for (item = list->first; item != NULL; item = item->next)
    {
      if (cci_prepare (conn, &stmt, item->query, CCI_PREPARE_FROM_MIGRATOR) < 0)
        {
          RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR, conn->err_buf.err_code, conn->err_buf.err_msg);
          goto error_exit;
        }

      error = cci_execute_with_gid (&stmt, 0, 0, -gid);
      if (error < 0)
        {
          if (stmt.err_buf.err_code == ER_BTREE_UNIQUE_FAILED)
            {
              ctx->num_synced_collision++;
            }
          else
            {
              RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR, stmt.err_buf.err_code, stmt.err_buf.err_msg);
              goto error_exit;
            }
        }
      else
        {
          ctx->num_synced_rows++;
        }

      cci_close_req_handle (&stmt);
    }

  mht_rem (ht_Tran_queries, &tran_id, rbl_tran_list_free, NULL);
  cci_set_autocommit (conn, CCI_AUTOCOMMIT_FALSE);

  return NO_ERROR;

error_exit:

  cci_close_req_handle (&stmt);
  cci_end_tran (conn, CCI_TRAN_ROLLBACK);
  cci_set_autocommit (conn, CCI_AUTOCOMMIT_FALSE);

  return ER_FAILED;
}

void
rbl_clear_tran_list (TRANID tran_id)
{
  TRAN_QUERY_LIST *list;

  list = (TRAN_QUERY_LIST *) mht_get (ht_Tran_queries, &tran_id);
  if (list == NULL)
    {
      return;
    }

  mht_rem (ht_Tran_queries, &tran_id, rbl_tran_list_free, NULL);
}
