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
#include "heartbeat.h"


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
 *    return: repl_item
 *
 *    lsa(in):
 *    target_lsa(in):
 */
CIRP_REPL_ITEM *
cirp_new_repl_item_data (const LOG_LSA * lsa, const LOG_LSA * target_lsa)
{
  CIRP_REPL_ITEM *item;
  RP_DATA_ITEM *data;

  item = malloc (DB_SIZEOF (CIRP_REPL_ITEM));
  if (item == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, DB_SIZEOF (CIRP_REPL_ITEM));
      return NULL;
    }

  item->item_type = RP_ITEM_TYPE_DATA;
  item->next = NULL;

  data = &item->info.data;

  data->rcv_index = -1;
  data->groupid = NULL_GROUPID;
  data->class_name = NULL;
  DB_IDXKEY_MAKE_NULL (&data->key);

  LSA_COPY (&data->lsa, lsa);
  LSA_COPY (&data->target_lsa, target_lsa);

  data->recdes = NULL;

  return item;
}

/*
 * cirp_new_repl_item_ddl()-
 *    return: repl_item
 *
 *    lsa(in):
 */
CIRP_REPL_ITEM *
cirp_new_repl_item_ddl (const LOG_LSA * lsa)
{
  CIRP_REPL_ITEM *item;
  RP_DDL_ITEM *ddl;


  item = malloc (DB_SIZEOF (CIRP_REPL_ITEM));
  if (item == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, DB_SIZEOF (CIRP_REPL_ITEM));
      return NULL;
    }

  item->item_type = RP_ITEM_TYPE_DDL;
  item->next = NULL;


  ddl = &item->info.ddl;

  ddl->stmt_type = RYE_STMT_UNKNOWN;
  ddl->ddl_type = REPL_BLOCKED_DDL;
  ddl->db_user = NULL;
  ddl->query = NULL;
  LSA_COPY (&ddl->lsa, lsa);

  return item;
}

/*
 * cirp_new_repl_catalog_item()-
 *    return: repl_item
 *
 *    lsa(in):
 *    target_lsa(in):
 */
CIRP_REPL_ITEM *
cirp_new_repl_catalog_item (const LOG_LSA * lsa)
{
  CIRP_REPL_ITEM *item;
  RP_CATALOG_ITEM *catalog;

  item = malloc (DB_SIZEOF (CIRP_REPL_ITEM));
  if (item == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, DB_SIZEOF (CIRP_REPL_ITEM));
      return NULL;
    }

  item->item_type = RP_ITEM_TYPE_CATALOG;
  item->next = NULL;

  catalog = &item->info.catalog;

  catalog->copyarea_op = -1;
  catalog->class_name = NULL;
  DB_IDXKEY_MAKE_NULL (&catalog->key);
  catalog->recdes = NULL;

  LSA_COPY (&catalog->lsa, lsa);

  return item;
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
