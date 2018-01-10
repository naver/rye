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
 * repl_common.c -
 */

#ident "$Id$"

#include <fcntl.h>
#include <errno.h>

#include "db.h"
#include "config.h"
#include "error_code.h"
#include "file_io.h"
#include "log_compress.h"
#include "memory_alloc.h"
#include "db_query.h"
#include "language_support.h"
#include "authenticate.h"
#include "connection_support.h"
#include "repl_common.h"
#include "repl_log.h"
#include "repl.h"
#include "heartbeat.h"

#include "transform.h"

#include "cas_cci_internal.h"

#if defined(CS_MODE) || defined(SERVER_MODE)
static bool repl_Agent_need_restart = false;
static bool repl_Agent_need_shutdown = false;

/*
 * rp_signal_handler ()
 */
void
rp_signal_handler (UNUSED_ARG int signo)
{
  repl_Agent_need_shutdown = true;
}

/*
 * rp_clear_need_restart ()
 */
void
rp_clear_need_restart (void)
{
  repl_Agent_need_restart = false;
}

/*
 * rp_set_agent_flag ()
 */
void
rp_set_agent_need_restart (const char *file_name, int line)
{
  er_log_debug (ARG_FILE_LINE,
		"FILE(%s,%d),repl_Agent_need_restart(%d) repl_Agent_need_shutdown(%d), "
		"hb_Proc_shutdown(%d)",
		file_name, line, repl_Agent_need_restart,
		repl_Agent_need_shutdown, hb_Proc_shutdown);

  repl_Agent_need_restart = true;
}

/*
 * rp_set_agent_flag ()
 */
void
rp_set_agent_need_shutdown (const char *file_name, int line)
{
  er_log_debug (ARG_FILE_LINE,
		"FILE(%s,%d),repl_Agent_need_restart(%d) repl_Agent_need_shutdown(%d), "
		"hb_Proc_shutdown(%d)",
		file_name, line, repl_Agent_need_restart,
		repl_Agent_need_shutdown, hb_Proc_shutdown);

  repl_Agent_need_shutdown = true;
}

/*
 * rp_need_restart ()
 */
bool
rp_need_restart (void)
{
  return (repl_Agent_need_restart == true || repl_Agent_need_shutdown == true
	  || hb_Proc_shutdown == true);
}

/*
 * rp_need_shutdown -
 *   return: bool
 *
 */
bool
rp_need_shutdown (const char *file_name, int line)
{
  er_log_debug (ARG_FILE_LINE,
		"FILE(%s,%d),repl_Agent_need_restart(%d) repl_Agent_need_shutdown(%d), "
		"hb_Proc_shutdown(%d)",
		file_name, line, repl_Agent_need_restart,
		repl_Agent_need_shutdown, hb_Proc_shutdown);

  return (repl_Agent_need_shutdown == true || hb_Proc_shutdown == true);
}

/*
 * cirp_new_repl_item_data()-
 *    return: error code
 *
 *    repl_item(out):
 *    lsa(in):
 */
int
rp_new_repl_item_data (CIRP_REPL_ITEM ** repl_item, const LOG_LSA * lsa)
{
  CIRP_REPL_ITEM *item = NULL;
  RP_DATA_ITEM *data = NULL;
  int error = NO_ERROR;

  if (repl_item == NULL || lsa == NULL)
    {
      assert (false);
      REPL_SET_GENERIC_ERROR (error, "Invalid argument");
      return error;
    }
  *repl_item = NULL;

  item = malloc (DB_SIZEOF (CIRP_REPL_ITEM));
  if (item == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      error, 1, DB_SIZEOF (CIRP_REPL_ITEM));
      return error;
    }

  item->item_type = RP_ITEM_TYPE_DATA;
  item->next = NULL;

  data = &item->info.data;

  data->rcv_index = -1;
  data->groupid = NULL_GROUPID;
  data->class_name = NULL;
  DB_IDXKEY_MAKE_NULL (&data->key);

  LSA_COPY (&data->lsa, lsa);
  LSA_SET_NULL (&data->target_lsa);

  data->recdes = NULL;

  *repl_item = item;

  assert (error == NO_ERROR);
  return NO_ERROR;
}

/*
 * cirp_new_repl_item_ddl()-
 *    return: error code
 *
 *    repl_item(out):
 *    lsa(in):
 */
int
rp_new_repl_item_ddl (CIRP_REPL_ITEM ** repl_item, const LOG_LSA * lsa)
{
  CIRP_REPL_ITEM *item;
  RP_DDL_ITEM *ddl;
  int error = NO_ERROR;

  if (repl_item == NULL || lsa == NULL)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "Invalid argument");
      return error;
    }
  *repl_item = NULL;

  item = malloc (DB_SIZEOF (CIRP_REPL_ITEM));
  if (item == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      error, 1, DB_SIZEOF (CIRP_REPL_ITEM));
      return error;
    }

  item->item_type = RP_ITEM_TYPE_DDL;
  item->next = NULL;


  ddl = &item->info.ddl;

  ddl->stmt_type = RYE_STMT_UNKNOWN;
  ddl->ddl_type = REPL_BLOCKED_DDL;
  ddl->db_user = NULL;
  ddl->query = NULL;
  LSA_COPY (&ddl->lsa, lsa);

  *repl_item = item;

  assert (error == NO_ERROR);
  return NO_ERROR;
}

/*
 * rp_new_repl_catalog_item()-
 *    return: error code
 *
 *    repl_item(out):
 *    lsa(in):
 */
int
rp_new_repl_catalog_item (CIRP_REPL_ITEM ** repl_item, const LOG_LSA * lsa)
{
  CIRP_REPL_ITEM *item;
  RP_CATALOG_ITEM *catalog;
  int error = NO_ERROR;

  if (repl_item == NULL || lsa == NULL)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "Invalid argument");
      return error;
    }
  *repl_item = NULL;

  item = malloc (DB_SIZEOF (CIRP_REPL_ITEM));
  if (item == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      error, 1, DB_SIZEOF (CIRP_REPL_ITEM));
      return error;
    }

  item->item_type = RP_ITEM_TYPE_CATALOG;
  item->next = NULL;

  catalog = &item->info.catalog;

  catalog->copyarea_op = -1;
  catalog->class_name = NULL;
  DB_IDXKEY_MAKE_NULL (&catalog->key);
  catalog->recdes = NULL;

  LSA_COPY (&catalog->lsa, lsa);

  *repl_item = item;

  assert (error == NO_ERROR);
  return NO_ERROR;
}

/*
 * cirp_free_repl_item()-
 *    return:
 *
 *    item(in/out):
 */
void
cirp_free_repl_item (CIRP_REPL_ITEM * item)
{
  assert (item != NULL);

  switch (item->item_type)
    {
    case RP_ITEM_TYPE_DATA:
      {
	RP_DATA_ITEM *data;

	data = &item->info.data;
	if (data->class_name != NULL)
	  {
	    free_and_init (data->class_name);
	    db_idxkey_clear (&data->key);
	  }
      }
      break;
    case RP_ITEM_TYPE_DDL:
      {
	RP_DDL_ITEM *ddl;

	ddl = &item->info.ddl;

	if (ddl->db_user != NULL)
	  {
	    free_and_init (ddl->db_user);
	  }
	if (ddl->query != NULL)
	  {
	    free_and_init (ddl->query);
	  }
      }
      break;
    case RP_ITEM_TYPE_CATALOG:
      {
	RP_CATALOG_ITEM *catalog;

	catalog = &item->info.catalog;

	if (catalog->class_name != NULL)
	  {
	    free_and_init (catalog->class_name);
	    db_idxkey_clear (&catalog->key);
	  }
      }
      break;

    }

  free_and_init (item);

  return;
}
#endif /* CS_MODE || SERVER_MODE */

/*
 * rp_is_valid_repl_item ()-
 *   return:
 *
 *   item(in):
 */
bool
rp_is_valid_repl_item (CIRP_REPL_ITEM * item)
{
  RP_DATA_ITEM *data;
  RP_DDL_ITEM *ddl;
  RP_CATALOG_ITEM *catalog;

  while (item != NULL)
    {
      switch (item->item_type)
	{
	case RP_ITEM_TYPE_DATA:
	  data = &item->info.data;
	  if (data->class_name == NULL || LSA_ISNULL (&data->lsa)
	      || cci_db_idxkey_is_null (&data->key))
	    {
	      return false;
	    }

	  if (data->rcv_index != RVREPL_DATA_INSERT
	      && data->rcv_index != RVREPL_DATA_UPDATE
	      && data->rcv_index != RVREPL_DATA_DELETE)
	    {
	      return false;
	    }
	  break;
	case RP_ITEM_TYPE_DDL:
	  ddl = &item->info.ddl;
	  if (ddl->query == NULL || ddl->db_user == NULL
	      || LSA_ISNULL (&ddl->lsa))
	    {
	      return false;
	    }
	  break;
	case RP_ITEM_TYPE_CATALOG:
	  catalog = &item->info.catalog;

	  if (catalog->class_name == NULL
	      || cci_db_idxkey_is_null (&catalog->key))
	    {
	      return false;
	    }

	  if (catalog->copyarea_op != LC_FLUSH_HA_CATALOG_ANALYZER_UPDATE
	      && catalog->copyarea_op != LC_FLUSH_HA_CATALOG_APPLIER_UPDATE)
	    {
	      return false;
	    }

	  if (catalog->copyarea_op == LC_FLUSH_HA_CATALOG_ANALYZER_UPDATE
	      && strncasecmp (catalog->class_name,
			      CT_LOG_ANALYZER_NAME,
			      strlen (CT_LOG_ANALYZER_NAME) != 0))
	    {
	      return false;
	    }
	  if (catalog->copyarea_op == LC_FLUSH_HA_CATALOG_APPLIER_UPDATE
	      && strncasecmp (catalog->class_name,
			      CT_LOG_APPLIER_NAME,
			      strlen (CT_LOG_APPLIER_NAME) != 0))
	    {
	      return false;
	    }
	}
      item = item->next;
    }

  return true;
}

/*
 * rp_make_repl_host_key () -
 *
 *   return:
 *   dbval(out):
 *   node_info(in):
 */
int
rp_make_repl_host_key (DB_VALUE * dbval, const PRM_NODE_INFO * node_info)
{
  char *host_key_str;

  host_key_str = (char *) malloc (MAX_NODE_INFO_STR_LEN);
  if (host_key_str == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, MAX_NODE_INFO_STR_LEN);
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }
  prm_node_info_to_str (host_key_str, MAX_NODE_INFO_STR_LEN, node_info);
  db_make_string (dbval, host_key_str);
  dbval->need_clear = true;

  return NO_ERROR;
}

int
rp_host_str_to_node_info (PRM_NODE_INFO * node_info, const char *host_str)
{
  PRM_NODE_LIST node_list;

  memset (&node_list, 0, sizeof (node_list));

  if (host_str != NULL &&
      prm_split_node_str (&node_list, host_str, false) == NO_ERROR &&
      node_list.num_nodes >= 1)
    {
      *node_info = node_list.nodes[0];
      return NO_ERROR;
    }
  else
    {
      PRM_NODE_INFO tmp_node_info = prm_get_null_node_info ();
      *node_info = tmp_node_info;
      return ER_FAILED;
    }
}
