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
 * rbl_conf.c -
 */

#ident "$Id$"

#include <stdio.h>
#include <stdlib.h>

#include "db.h"
#include "dbi.h"
#include "porting.h"
#include "error_manager.h"
#include "connection_defs.h"
#include "boot_cl.h"
#include "transform.h"

#include "cas_cci_internal.h"
#include "rbl_error_log.h"
#include "rbl_conf.h"

static CCI_SHARD_NODE_INFO *node_Info;

static CCI_NODE_INFO **src_Node_info;
static int num_Src_node_info;
static int src_Master_index = -1;
static int src_Connected_index = -1;

static CCI_NODE_INFO **dest_Node_info;
static int num_Dest_node_info;
static int dest_Master_index = -1;

static CCI_CONN mgmt_Conn;
static CCI_CONN srcdb_Conn;
static CCI_CONN destdb_Conn[2];

static char prog_Name[64];

CCI_CONN *
rbl_conf_get_mgmt_conn (void)
{
  return &mgmt_Conn;
}

CCI_CONN *
rbl_conf_get_srcdb_conn (void)
{
  return &srcdb_Conn;
}

CCI_CONN *
rbl_conf_get_destdb_conn (int index)
{
  RBL_ASSERT (index == RBL_COPY || index == RBL_SYNC);
  return &destdb_Conn[index];
}

static HA_STATE
rbl_conf_connect_db (const char *db, const char *host)
{
  int error;
  char dbname[MAXHOSTNAMELEN + MAX_DBNAME_SIZE + 2];

  sprintf (dbname, "%s@%s", db, host);
  boot_clear_host_connected ();

  error = db_restart (prog_Name, TRUE, dbname);
  if (error != NO_ERROR)
    {
      RBL_DEBUG (ARG_FILE_LINE, "%s", db_error_string (3));
      return HA_STATE_NA;
    }

  return db_get_server_state ();
}

static int
rbl_conf_find_master_node_index (CCI_NODE_INFO ** node_info, int num_node)
{
  int i;
  HA_STATE server_state;

  AU_DISABLE_PASSWORDS ();
  db_set_client_type (BOOT_CLIENT_RBL_MIGRATOR);
  db_login ("DBA", NULL);

  for (i = 0; i < num_node; i++)
    {
      server_state = rbl_conf_connect_db (node_info[i]->dbname,
					  node_info[i]->hostname);
      if (server_state == HA_STATE_MASTER)
	{
	  db_shutdown ();
	  return i;
	}

      db_shutdown ();
    }

  return -1;
}

static int
rbl_conf_connect_srcdb (int mode)
{
  int i;
  HA_STATE server_state;

  RBL_ASSERT (mode == RBL_MASTER || mode == RBL_SLAVE);

  src_Master_index = rbl_conf_find_master_node_index (src_Node_info,
						      num_Src_node_info);
  if (src_Master_index < 0)
    {
      RBL_ASSERT (0);
      return ER_FAILED;
    }

  AU_DISABLE_PASSWORDS ();
  db_set_client_type (BOOT_CLIENT_RBL_MIGRATOR);
  db_login ("DBA", NULL);

  if (mode == RBL_MASTER)
    {
      server_state =
	rbl_conf_connect_db (src_Node_info[src_Master_index]->dbname,
			     src_Node_info[src_Master_index]->hostname);
      RBL_ASSERT (server_state == HA_STATE_MASTER);

      if (server_state == HA_STATE_MASTER)
	{
	  src_Connected_index = src_Master_index;
	  return NO_ERROR;
	}
    }
  else
    {
      RBL_ASSERT (mode == RBL_SLAVE);
      for (i = 0; i < num_Src_node_info; i++)
	{
	  if (i == src_Master_index)
	    {
	      continue;
	    }

	  server_state = rbl_conf_connect_db (src_Node_info[i]->dbname,
					      "localhost");
	  if (server_state == HA_STATE_SLAVE)
	    {
	      src_Connected_index = i;
	      return NO_ERROR;
	    }

	  db_shutdown ();
	}
    }

  return ER_FAILED;
}

static CCI_NODE_INFO **
rbl_conf_find_nodeinfo (int nodeid, int *num_node_info)
{
  int i, num_found = 0;
  CCI_NODE_INFO **nodes;

  for (i = 0; i < node_Info->node_count; i++)
    {
      if (node_Info->node_info[i].nodeid == nodeid)
	{
	  num_found++;
	}
    }

  nodes = (CCI_NODE_INFO **) malloc (sizeof (CCI_NODE_INFO *) * num_found);
  if (nodes == NULL)
    {
      return NULL;
    }

  num_found = 0;
  for (i = 0; i < node_Info->node_count; i++)
    {
      if (node_Info->node_info[i].nodeid == nodeid)
	{
	  nodes[num_found++] = &node_Info->node_info[i];
	}
    }

  *num_node_info = num_found;
  return nodes;
}

static CCI_NODE_INFO *
rbl_conf_get_src_node (int index)
{
  if (index < num_Src_node_info)
    {
      return src_Node_info[index];
    }

  return NULL;
}

static CCI_NODE_INFO *
rbl_conf_get_dest_node (int index)
{
  if (index < num_Dest_node_info)
    {
      return dest_Node_info[index];
    }

  return NULL;
}

static int
rbl_conf_make_connection (const char *host, int port, const char *dbname,
			  CCI_CONN * conn, bool dbname_with_host,
			  bool to_slave, bool ignore_dba_password,
			  bool is_local_con)
{
  char url[1024];
  const char *rwmode = "rw";
  char *pw = NULL;
  const char *url_property;

  if (is_local_con)
    {
      url_property = "connectionType=local";
    }
  else
    {
      url_property = "connectionType=global";
    }

  if (to_slave == true)
    {
      rwmode = "so";
    }

  if (dbname_with_host == true)
    {
      sprintf (url, "cci:rye://%s:%d/%s@%s/%s?%s",
	       host, port, dbname, host, rwmode, url_property);
    }
  else
    {
      sprintf (url, "cci:rye://%s:%d/%s/%s?%s",
	       host, port, dbname, rwmode, url_property);
    }

  if (ignore_dba_password == false)
    {
      pw = getenv ("PASSWD");
    }

  RBL_DEBUG (ARG_FILE_LINE, "url= %s, pass= %s\n", url, (pw ? pw : ""));

  if (cci_connect (conn, url, "dba", (pw ? pw : "")) < 0)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		 conn->err_buf.err_code, conn->err_buf.err_msg);
      return ER_FAILED;
    }

  cci_set_autocommit (conn, CCI_AUTOCOMMIT_FALSE);

  return NO_ERROR;
}

int
rbl_conf_init (const char *mgmt_host, int mgmt_port, const char *mgmt_dbname,
	       int src_node_id, int dest_node_id, int group_id,
	       const char *dest_host, int dest_port, const char *dest_dbname,
	       int src_node_ha_staus, bool copy_schema)
{
  int i, error;
  CCI_SHARD_GROUPID_INFO *groupid_info;
  CCI_NODE_INFO *src_node, *dest_node;

  sprintf (prog_Name, "%s_%d", PROG_NAME, group_id);

  error = rbl_conf_make_connection (mgmt_host, mgmt_port, mgmt_dbname,
				    &mgmt_Conn, false, false, false, false);
  if (error != NO_ERROR)
    {
      return error;
    }

  error = cci_shard_get_info (&mgmt_Conn, &node_Info, &groupid_info);
  if (error != NO_ERROR)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		 mgmt_Conn.err_buf.err_code, mgmt_Conn.err_buf.err_msg);
      return ER_FAILED;
    }

  /* for test */
#if 0
  for (i = 0; i < node_Info->node_count; i++)
    {
      RBL_DEBUG (ARG_FILE_LINE, "Node info:\n%d %s %s %d\n",
		 node_Info->node_info[i].nodeid,
		 node_Info->node_info[i].dbname,
		 node_Info->node_info[i].hostname,
		 node_Info->node_info[i].port);
    }

  for (i = 1; i <= groupid_info->groupid_count; i++)
    {
      RBL_DEBUG (ARG_FILE_LINE, "Group Id info:\n%3d %d\n", i + 1,
		 groupid_info->nodeid_table[i]);
    }
#endif
  cci_shard_group_info_free (groupid_info);

  src_Node_info = rbl_conf_find_nodeinfo (src_node_id, &num_Src_node_info);
  if (src_Node_info == NULL)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_NODE_NOT_FOUND, src_node_id);
      return ER_FAILED;
    }

  if (copy_schema == false)
    {
      dest_Node_info =
	rbl_conf_find_nodeinfo (dest_node_id, &num_Dest_node_info);
      if (dest_Node_info == NULL)
	{
	  RBL_ERROR (ARG_FILE_LINE, RBL_NODE_NOT_FOUND, dest_node_id);
	  return ER_FAILED;
	}

      dest_Master_index =
	rbl_conf_find_master_node_index (dest_Node_info, num_Dest_node_info);
      if (dest_Master_index < 0)
	{
	  return ER_FAILED;
	}
    }

  /* source db login */
  if (rbl_conf_connect_srcdb (src_node_ha_staus) != NO_ERROR)
    {
      return ER_FAILED;
    }

  RBL_ASSERT (src_Connected_index > -1);
  src_node = rbl_conf_get_src_node (src_Connected_index);
  if (src_node == NULL)
    {
      return ER_FAILED;
    }

  error = rbl_conf_make_connection (src_node->hostname, src_node->port,
				    src_node->dbname, &srcdb_Conn, true,
				    src_node_ha_staus == RBL_SLAVE, false,
				    true);
  if (error != NO_ERROR)
    {
      return error;
    }

  if (copy_schema == true)
    {
      for (i = 0; i < 2; i++)
	{
	  error = rbl_conf_make_connection (dest_host, dest_port, dest_dbname,
					    &destdb_Conn[i], false, false,
					    true, true);
	  if (error != NO_ERROR)
	    {
	      return error;
	    }
	}
    }
  else
    {
      RBL_ASSERT (dest_Master_index > -1);
      dest_node = rbl_conf_get_dest_node (dest_Master_index);
      if (dest_node == NULL)
	{
	  return ER_FAILED;
	}

      for (i = 0; i < 2; i++)
	{
	  error =
	    rbl_conf_make_connection (dest_node->hostname, dest_node->port,
				      dest_node->dbname, &destdb_Conn[i],
				      false, false, false, true);
	  if (error != NO_ERROR)
	    {
	      return error;
	    }
	}
    }

  return NO_ERROR;
}

static int
rbl_conf_update_groupid_bitmap (RBL_COPY_CONTEXT * ctx, CCI_CONN * conn,
				bool on_off, bool do_commit)
{
  int error;

  error = cci_update_db_group_id (conn, ctx->sync_ctx->migrator_id,
				  ctx->gid, 0 /* master */ , on_off);
  if (error != NO_ERROR)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		 conn->err_buf.err_code, conn->err_buf.err_msg);
      cci_end_tran (conn, CCI_TRAN_ROLLBACK);
      return error;
    }

  if (do_commit == true)
    {
      cci_end_tran (conn, CCI_TRAN_COMMIT);
    }

  return NO_ERROR;
}

int
rbl_conf_update_src_groupid (RBL_COPY_CONTEXT * ctx, bool on_off,
			     bool do_commit)
{
  CCI_CONN *conn, tmp_conn;
  CCI_NODE_INFO *node;
  int error;

  if (ctx->run_slave == true)
    {
      node = src_Node_info[src_Master_index];
      error = rbl_conf_make_connection (node->hostname, node->port,
					node->dbname, &tmp_conn, true,
					false, false, true);
      if (error != NO_ERROR)
	{
	  return error;
	}

      conn = &tmp_conn;
    }
  else
    {
      conn = ctx->src_conn;
    }

  error = rbl_conf_update_groupid_bitmap (ctx, conn, on_off, do_commit);

  if (ctx->run_slave == true)
    {
      cci_disconnect (&tmp_conn);
    }

  return error;
}

int
rbl_conf_update_dest_groupid (RBL_COPY_CONTEXT * ctx)
{
  return rbl_conf_update_groupid_bitmap (ctx, ctx->dest_conn, true, true);
}

static INT64
rbl_conf_get_repl_delay (CCI_CONN * conn)
{
  char sql[512];
  CCI_STMT stmt;
  int ind, error;
  INT64 max_delay = 0;

  sprintf (sql, "SELECT max(repl_delay) FROM [%s]", CT_LOG_APPLIER_NAME);

  if (cci_prepare (conn, &stmt, sql, CCI_PREPARE_FROM_MIGRATOR) < 0)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		 conn->err_buf.err_code, conn->err_buf.err_msg);
      return -1;
    }

  if (cci_execute (&stmt, 0, 0) < 0)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		 stmt.err_buf.err_code, stmt.err_buf.err_msg);
      goto error_exit;
    }

  error = cci_fetch_next (&stmt);
  if (error < 0)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		 stmt.err_buf.err_code, stmt.err_buf.err_msg);
      goto error_exit;
    }

  max_delay = cci_get_bigint (&stmt, 1, &ind);
  if (stmt.err_buf.err_code != CCI_ER_NO_ERROR)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		 stmt.err_buf.err_code, stmt.err_buf.err_msg);
      goto error_exit;
    }

  cci_close_req_handle (&stmt);
  cci_end_tran (conn, CCI_TRAN_COMMIT);

  assert (max_delay >= 0);
  return max_delay;

error_exit:

  cci_close_req_handle (&stmt);
  cci_end_tran (conn, CCI_TRAN_ROLLBACK);

  return -1;
}

int
rbl_conf_check_repl_delay (CCI_CONN * conn)
{
  INT64 delay;
  int max_delay_msec;
  int retry = 0;

  max_delay_msec = prm_get_integer_value (PRM_ID_MIGRATOR_MAX_REPL_DELAY);

  while (true)
    {
      delay = rbl_conf_get_repl_delay (conn);
      if (delay < 0)
	{
	  return ER_FAILED;
	}

      RBL_DEBUG (ARG_FILE_LINE, "Replication delay: %ld, %d\n",
		 delay, max_delay_msec);

      if (delay <= max_delay_msec)
	{
	  break;
	}

      if (++retry >= 100)
	{
	  RBL_ERROR_MSG (ARG_FILE_LINE,
			 "Give up migration due to replication delay : %d (ms)\n",
			 delay);
	  return ER_FAILED;
	}

      THREAD_SLEEP (1000);
    }

  return NO_ERROR;
}

int
rbl_conf_insert_gid_removed_info_srcdb (int group_id, bool run_slave)
{
  CCI_CONN *conn, tmp_conn;
  CCI_NODE_INFO *node;
  int error = NO_ERROR;

  if (run_slave == true)
    {
      node = src_Node_info[src_Master_index];
      error = rbl_conf_make_connection (node->hostname, node->port,
					node->dbname, &tmp_conn, true, false,
					false, true);
      if (error != NO_ERROR)
	{
	  return error;
	}
      conn = &tmp_conn;
    }
  else
    {
      conn = rbl_conf_get_srcdb_conn ();
    }

  error = cci_insert_gid_removed_info (conn, group_id);
  if (error != NO_ERROR)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR,
		 conn->err_buf.err_code, conn->err_buf.err_msg);
    }

  if (conn == &tmp_conn)
    {
      cci_end_tran (conn,
		    error == NO_ERROR ? CCI_TRAN_COMMIT : CCI_TRAN_ROLLBACK);
      cci_disconnect (conn);
    }

  return error;
}

void
rbl_conf_final (void)
{
  cci_disconnect (&mgmt_Conn);
  cci_disconnect (&srcdb_Conn);
  cci_disconnect (&destdb_Conn[0]);
  cci_disconnect (&destdb_Conn[1]);

  cci_shard_node_info_free (node_Info);

  db_commit_transaction ();
  (void) db_shutdown ();
}
