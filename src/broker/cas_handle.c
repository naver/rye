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
 * cas_handle.c -
 */

#ident "$Id$"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <unistd.h>

#include "dbi.h"
#include "cas_execute.h"

#include "cas.h"
#include "cas_common.h"
#include "cas_handle.h"
#include "cas_log.h"

#define SRV_HANDLE_ALLOC_SIZE		256

static void srv_handle_content_free (T_SRV_HANDLE * srv_handle);

static T_SRV_HANDLE **srv_handle_table = NULL;
static int max_srv_handle = 0;
static int max_handle_id = 0;
static int current_handle_count = 0;

int
hm_new_srv_handle (T_SRV_HANDLE ** new_handle, unsigned int seq_num)
{
  int i;
  int new_max_srv_handle;
  int new_handle_id = 0;
  T_SRV_HANDLE **new_srv_handle_table = NULL;
  T_SRV_HANDLE *srv_handle;

  if (current_handle_count >= shm_Appl->max_prepared_stmt_count)
    {
      return ERROR_INFO_SET (CAS_ER_MAX_PREPARED_STMT_COUNT_EXCEEDED, CAS_ERROR_INDICATOR);
    }

  for (i = 0; i < max_srv_handle; i++)
    {
      if (srv_handle_table[i] == NULL)
        {
          *new_handle = srv_handle_table[i];
          new_handle_id = i + 1;
          break;
        }
    }

  if (new_handle_id == 0)
    {
      new_max_srv_handle = max_srv_handle + SRV_HANDLE_ALLOC_SIZE;
      new_srv_handle_table = (T_SRV_HANDLE **)
        RYE_REALLOC (srv_handle_table, sizeof (T_SRV_HANDLE *) * new_max_srv_handle);
      if (new_srv_handle_table == NULL)
        {
          return ERROR_INFO_SET (CAS_ER_NO_MORE_MEMORY, CAS_ERROR_INDICATOR);
        }

      new_handle_id = max_srv_handle + 1;
      memset (new_srv_handle_table + max_srv_handle, 0, sizeof (T_SRV_HANDLE *) * SRV_HANDLE_ALLOC_SIZE);
      max_srv_handle = new_max_srv_handle;
      srv_handle_table = new_srv_handle_table;
    }

  srv_handle = (T_SRV_HANDLE *) RYE_MALLOC (sizeof (T_SRV_HANDLE));
  if (srv_handle == NULL)
    {
      return ERROR_INFO_SET (CAS_ER_NO_MORE_MEMORY, CAS_ERROR_INDICATOR);
    }
  memset (srv_handle, 0, sizeof (T_SRV_HANDLE));
  srv_handle->id = new_handle_id;
  srv_handle->query_seq_num = seq_num;
  srv_handle->use_plan_cache = false;
  srv_handle->is_holdable = false;
  srv_handle->is_from_current_transaction = true;
  srv_handle->is_pooled = as_Info->cur_statement_pooling;

  *new_handle = srv_handle;
  srv_handle_table[new_handle_id - 1] = srv_handle;
  if (new_handle_id > max_handle_id)
    {
      max_handle_id = new_handle_id;
    }

  current_handle_count++;

  return new_handle_id;
}

T_SRV_HANDLE *
hm_find_srv_handle (int h_id)
{
  if (h_id <= 0 || h_id > max_srv_handle)
    {
      return NULL;
    }

  return (srv_handle_table[h_id - 1]);
}

void
hm_srv_handle_free (int h_id)
{
  T_SRV_HANDLE *srv_handle;

  if (h_id <= 0 || h_id > max_srv_handle)
    {
      return;
    }

  srv_handle = srv_handle_table[h_id - 1];
  if (srv_handle == NULL)
    {
      return;
    }

  srv_handle_content_free (srv_handle);

  RYE_FREE_MEM (srv_handle);
  srv_handle_table[h_id - 1] = NULL;
  current_handle_count--;
}

void
hm_srv_handle_free_all (bool free_holdable)
{
  T_SRV_HANDLE *srv_handle;
  int i;
  int new_max_handle_id = 0;

  for (i = 0; i < max_handle_id; i++)
    {
      srv_handle = srv_handle_table[i];
      if (srv_handle == NULL)
        {
          continue;
        }

      if (srv_handle->is_holdable && !free_holdable)
        {
          new_max_handle_id = i;
          continue;
        }

      srv_handle_content_free (srv_handle);
      RYE_FREE_MEM (srv_handle);
      srv_handle_table[i] = NULL;
      current_handle_count--;
    }

  max_handle_id = new_max_handle_id;
  if (free_holdable)
    {
      current_handle_count = 0;
      as_Info->num_holdable_results = 0;
    }
}

void
hm_srv_handle_qresult_end_all (bool end_holdable)
{
  T_SRV_HANDLE *srv_handle;
  int i;

  for (i = 0; i < max_handle_id; i++)
    {
      srv_handle = srv_handle_table[i];
      if (srv_handle == NULL)
        {
          continue;
        }

      if (srv_handle->is_holdable && !end_holdable)
        {
          /* do not close holdable results */
          srv_handle->is_from_current_transaction = false;
          continue;
        }

      if (srv_handle->is_holdable && !srv_handle->is_from_current_transaction)
        {
          /* end only holdable handles from the current transaction */
          continue;
        }

      if (srv_handle->schema_type < 0
          || srv_handle->schema_type == CCI_SCH_TABLE
          || srv_handle->schema_type == CCI_SCH_COLUMN
          || srv_handle->schema_type == CCI_SCH_QUERY_SPEC || srv_handle->schema_type == CCI_SCH_PRIMARY_KEY)
        {
          hm_qresult_end (srv_handle, FALSE);
        }
    }
}

void
hm_qresult_clear (T_QUERY_RESULT * q_result)
{
  memset (q_result, 0, sizeof (T_QUERY_RESULT));
}

void
hm_qresult_end (T_SRV_HANDLE * srv_handle, char free_flag)
{
  T_QUERY_RESULT *q_result;

  q_result = srv_handle->q_result;
  if (q_result)
    {
      if (q_result->copied != TRUE && q_result->result)
        {
          ux_free_result (q_result->result);

          if (q_result->is_holdable == true)
            {
              q_result->is_holdable = false;
              as_Info->num_holdable_results--;
            }
        }
      q_result->result = NULL;
      q_result->tuple_count = 0;

      if (free_flag == TRUE)
        {
          RYE_FREE_MEM (q_result);
        }
    }

  if (free_flag == TRUE)
    {
      srv_handle->q_result = NULL;
    }
}

void
hm_session_free (T_SRV_HANDLE * srv_handle)
{
  if (srv_handle->session)
    {
      db_close_session ((DB_SESSION *) (srv_handle->session));
    }
  srv_handle->session = NULL;
}

static void
srv_handle_content_free (T_SRV_HANDLE * srv_handle)
{
  RYE_FREE_MEM (srv_handle->sql_stmt);

  if (srv_handle->schema_type < 0
      || srv_handle->schema_type == CCI_SCH_TABLE
      || srv_handle->schema_type == CCI_SCH_COLUMN
      || srv_handle->schema_type == CCI_SCH_QUERY_SPEC || srv_handle->schema_type == CCI_SCH_PRIMARY_KEY)
    {
      hm_qresult_end (srv_handle, TRUE);
      hm_session_free (srv_handle);
    }
  else if (srv_handle->schema_type == CCI_SCH_TABLE_PRIVILEGE || srv_handle->schema_type == CCI_SCH_COLUMN_PRIVILEGE)
    {
      RYE_FREE_MEM (srv_handle->session);
    }
}
