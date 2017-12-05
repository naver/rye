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
 * rye_master_shm.c -
 */

#ident "$Id$"

#include <sys/types.h>
#include <sys/time.h>
#include <errno.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <assert.h>

#include "rye_master_shm.h"
#include "error_manager.h"
#include "system_parameter.h"
#include "tcp.h"

static RYE_SHM_MASTER *rye_Master_shm = NULL;

static RYE_SHM_MASTER *rye_master_shm_attach (void);
static void rye_master_shm_detach (RYE_SHM_MASTER * shm_p);

/***********************************************************
 * master use function
 ***********************************************************/

/*
 * master_shm_initialize -
 *
 * return:
 * Note:
 */
int
master_shm_initialize (void)
{
  RYE_SHM_MASTER *shm_p = NULL;
  int error = NO_ERROR;
  int mid;
  char *str_shm_key;
  int shm_key;

  str_shm_key = prm_get_string_value (PRM_ID_RYE_SHM_KEY);
  parse_int (&shm_key, str_shm_key, 16);

  if (shm_key < 0 || rye_Master_shm != NULL)
    {
      assert (false);
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "invalid shm key");
      return error;
    }

  rye_shm_destroy (shm_key);

  mid = shmget (shm_key, 0, 0644);
  if (mid != -1)
    {
      assert (false);
      (void) rye_shm_destroy (shm_key);
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "invalid shm");
      return error;
    }

  shm_p = (RYE_SHM_MASTER *) rye_shm_create (shm_key,
					     sizeof (RYE_SHM_MASTER),
					     RYE_SHM_TYPE_MASTER);
  if (shm_p == NULL)
    {
      error = ER_GENERIC_ERROR;
      assert (error != NO_ERROR);

      return error;
    }

  error = rye_shm_mutex_init (&shm_p->lock);
  if (error != NO_ERROR)
    {
      return error;
    }

  shm_p->shm_header.status = RYE_SHM_VALID;
  rye_Master_shm = shm_p;

  return NO_ERROR;
}

/*
 * master_shm_final()-
 *   return: none
 *
 */
int
master_shm_final (void)
{
  if (rye_Master_shm == NULL)
    {
      return ER_FAILED;
    }

  rye_master_shm_detach (rye_Master_shm);
  rye_Master_shm = NULL;

  return NO_ERROR;
}

/*
 * master_shm_set_node_state ()-
 *   return: error code
 */
int
master_shm_set_node_state (const PRM_NODE_INFO * node_info,
			   unsigned short node_state)
{
  int i;

  if (rye_Master_shm == NULL)
    {
      assert (false);
      return ER_FAILED;
    }

  for (i = 0; i < rye_Master_shm->num_ha_nodes; i++)
    {
      if (prm_is_same_node (&rye_Master_shm->ha_nodes[i].node_info,
			    node_info) == true)
	{
	  rye_Master_shm->ha_nodes[i].node_state = node_state;
	  break;
	}
    }

  return NO_ERROR;
}

/*
 * master_shm_set_node_version ()-
 *   return: error code
 */
int
master_shm_set_node_version (const PRM_NODE_INFO * node_info,
			     const RYE_VERSION * node_version)
{
  int i;

  if (rye_Master_shm == NULL)
    {
      assert (false);
      return ER_FAILED;
    }

  for (i = 0; i < rye_Master_shm->num_ha_nodes; i++)
    {
      if (prm_is_same_node (&rye_Master_shm->ha_nodes[i].node_info,
			    node_info) == true)
	{
	  rye_Master_shm->ha_nodes[i].ha_node_version = *node_version;
	  break;
	}
    }

  return NO_ERROR;
}

/*
 * master_shm_reset_hb_nodes()-
 *   return: error code
 *
 *   nodes(in):
 *   num_nodes(in):
 */
int
master_shm_reset_hb_nodes (RYE_SHM_HA_NODE * nodes, int num_nodes)
{
  if (rye_Master_shm == NULL)
    {
      assert (0);
      return ER_FAILED;
    }

  memcpy (rye_Master_shm->ha_nodes, nodes,
	  sizeof (RYE_SHM_HA_NODE) * num_nodes);
  rye_Master_shm->num_ha_nodes = num_nodes;
  rye_Master_shm->ha_node_reset_time = time (NULL);

  return NO_ERROR;
}

/*
 * master_shm_get_server_state ()
 *   return: error code
 *
 *   pid(in):
 *   state(in):
 */
int
master_shm_get_server_state (const char *dbname)
{
  int server_state = HA_STATE_NA;
  int num_db_servers;
  int i;

  if (rye_Master_shm == NULL)
    {
      assert (0);
      return ER_FAILED;
    }

  num_db_servers = MIN (rye_Master_shm->num_db_servers, SHM_MAX_DB_SERVERS);
  for (i = 0; i < num_db_servers; i++)
    {
      if (strncasecmp (rye_Master_shm->db_server_info[i].dbname,
		       dbname, SHM_DBNAME_SIZE) == 0)
	{
	  server_state = rye_Master_shm->db_server_info[i].server_state;
	  break;
	}
    }

  return server_state;
}

/*
 * master_shm_update_server_state ()
 *   return: error code
 *
 *   pid(in):
 *   state(in):
 */
int
master_shm_update_server_state (int pid, unsigned char server_state)
{
  int num_db_servers;
  int i;

  if (rye_Master_shm == NULL)
    {
      assert (0);
      return ER_FAILED;
    }

  num_db_servers = MIN (rye_Master_shm->num_db_servers, SHM_MAX_DB_SERVERS);
  for (i = 0; i < num_db_servers; i++)
    {
      if (rye_Master_shm->db_server_info[i].pid == pid)
	{
	  rye_Master_shm->db_server_info[i].server_state = server_state;
	  break;
	}
    }

  return NO_ERROR;
}


/*
 * master_shm_get_shard_mgmt_info () -
 *    return: NO_ERROR or ER_FAILED
 */
int
master_shm_get_shard_mgmt_info (const char *local_dbname,
				char *global_dbname,
				short *nodeid,
				PRM_NODE_INFO * shard_mgmt_node_info)
{
  int i, rv;
  int error = ER_FAILED;

  if (rye_Master_shm == NULL)
    {
      assert (0);
      return ER_FAILED;
    }

  rv = pthread_mutex_lock (&rye_Master_shm->lock);
  if (rv == EOWNERDEAD)
    {
      pthread_mutex_unlock (&rye_Master_shm->lock);
      return ER_FAILED;
    }

  for (i = 0; i < RYE_SHD_MGMT_TABLE_SIZE; i++)
    {
      if (rye_Master_shm->shd_mgmt_table[i].local_dbname == '\0')
	{
	  break;
	}
      else
	if (strcmp
	    (local_dbname,
	     rye_Master_shm->shd_mgmt_table[i].local_dbname) == 0)
	{
	  /* choose most recently updated shard mgmt info */
	  int j, recent = 0;
	  RYE_SHD_MGMT_TABLE *shd_mgmt_entry;

	  shd_mgmt_entry = &rye_Master_shm->shd_mgmt_table[i];

	  for (j = 0; j < RYE_SHD_MGMT_INFO_MAX_COUNT; j++)
	    {
	      if (shd_mgmt_entry->shd_mgmt_info[j].node_info.port == 0)
		{
		  break;
		}
	      else if (shd_mgmt_entry->shd_mgmt_info[j].sync_time >
		       shd_mgmt_entry->shd_mgmt_info[recent].sync_time)
		{
		  recent = j;
		}
	    }

	  if (shd_mgmt_entry->shd_mgmt_info[recent].node_info.port != 0)
	    {
	      strncpy (global_dbname, shd_mgmt_entry->global_dbname,
		       RYE_SHD_MGMT_TABLE_DBNAME_SIZE);
	      *nodeid = shd_mgmt_entry->nodeid;
	      *shard_mgmt_node_info =
		shd_mgmt_entry->shd_mgmt_info[recent].node_info;
	      error = NO_ERROR;
	    }

	  break;
	}
    }

  pthread_mutex_unlock (&rye_Master_shm->lock);

  return error;
}

/***********************************************************
 * non-master use functions
 ***********************************************************/

/*
 * rye_master_shm_attach -
 *
 * return:
 * Note:
 */
static RYE_SHM_MASTER *
rye_master_shm_attach ()
{
  RYE_SHM_MASTER *shm_p;
  int rv;
  char *str_shm_key;
  int shm_key;

  str_shm_key = prm_get_string_value (PRM_ID_RYE_SHM_KEY);
  if (str_shm_key == NULL)
    {
      return NULL;
    }
  parse_int (&shm_key, str_shm_key, 16);

  shm_p = (RYE_SHM_MASTER *) rye_shm_attach (shm_key, RYE_SHM_TYPE_MASTER,
					     false);
  if (shm_p == NULL)
    {
      return NULL;
    }

  rv = pthread_mutex_lock (&shm_p->lock);
  if (rv == EOWNERDEAD)
    {
      pthread_mutex_unlock (&shm_p->lock);

      rye_shm_detach (shm_p);
      return NULL;
    }
  else if (rv == ENOTRECOVERABLE)
    {
      /* mutex is not recovered by ???? */
      THREAD_SLEEP (3 * 1000);
      rv = pthread_mutex_lock (&shm_p->lock);
      if (rv != 0)
	{
	  rye_shm_detach (shm_p);
	  return NULL;
	}
    }
  pthread_mutex_unlock (&shm_p->lock);

  return shm_p;
}

/*
 * rye_master_shm_detach() -
 *   return:
 *
 *   shm_p(in/out):
 */
static void
rye_master_shm_detach (RYE_SHM_MASTER * shm_p)
{
  assert (shm_p != NULL);
  if (shm_p == NULL)
    {
      return;
    }

  (void) rye_shm_detach (shm_p);

  return;
}

/*
 * rye_master_shm_get_new_server_shm_key () -
 *   return: error code or shm key index
 *
 *   dbname(in):
 *   server_pid(in):
 */
int
rye_master_shm_get_new_server_shm_key (const char *dbname, int server_pid)
{
  RYE_SHM_MASTER *master_shm = NULL;
  RYE_SHM_DB_SERVER_INFO *shm_server_info_p = NULL;
  int rv;
  int i, num_db_servers;
  int res_index = -1;
  int svr_shm_key = -1;

  assert (dbname != NULL);

  master_shm = rye_master_shm_attach ();
  if (master_shm == NULL)
    {
      return ER_FAILED;
    }

  rv = pthread_mutex_lock (&master_shm->lock);
  if (rv == EOWNERDEAD)
    {
      pthread_mutex_unlock (&master_shm->lock);

      rye_master_shm_detach (master_shm);

      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_HA_LA_SHARED_MEM_RESET, 0);
      return -1;
    }

  assert (master_shm->num_db_servers <= SHM_MAX_DB_SERVERS);
  res_index = -1;
  num_db_servers = MIN (master_shm->num_db_servers, SHM_MAX_DB_SERVERS);
  for (i = 0; i < num_db_servers; i++)
    {
      if (strcmp (master_shm->db_server_info[i].dbname, dbname) == 0)
	{
	  res_index = i;
	  break;
	}
    }

  if (res_index >= 0)
    {
      shm_server_info_p = &(master_shm->db_server_info[res_index]);

      svr_shm_key = shm_server_info_p->shm_key;

      rye_shm_destroy (svr_shm_key);
    }
  else
    {
      char *str_shm_key;
      int master_shm_key;

      if (master_shm->num_db_servers >= SHM_MAX_DB_SERVERS)
	{
	  assert (false);

	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR,
		  1, "too many databases in master shm");
	  return -1;
	}

      res_index = master_shm->num_db_servers;

      shm_server_info_p = &(master_shm->db_server_info[res_index]);

      strncpy (shm_server_info_p->dbname, dbname, SHM_DBNAME_SIZE);
      shm_server_info_p->dbname[SHM_DBNAME_SIZE - 1] = '\0';
      shm_server_info_p->shm_key = 0;

      str_shm_key = prm_get_string_value (PRM_ID_RYE_SHM_KEY);
      parse_int (&master_shm_key, str_shm_key, 16);

      svr_shm_key = (master_shm_key + DEFUALT_SERVER_SHM_KEY_BASE +
		     res_index);

      master_shm->num_db_servers++;
    }

  shm_server_info_p->pid = server_pid;
  shm_server_info_p->server_state = HA_STATE_UNKNOWN;

  pthread_mutex_unlock (&master_shm->lock);

  rye_master_shm_detach (master_shm);

  return svr_shm_key;
}

/*
 * rye_master_shm_get_ha_nodes()-
 *   return: error code
 *
 *   nodes(in/out):
 *   num_nodes(out):
 *   max_nodes(in):
 */
int
rye_master_shm_get_ha_nodes (RYE_SHM_HA_NODE * nodes, int *num_nodes,
			     int max_nodes)
{
  RYE_SHM_MASTER *master_shm;
  int i;

  if (nodes == NULL || num_nodes == NULL)
    {
      assert (false);
      return ER_FAILED;
    }
  *num_nodes = 0;

  master_shm = rye_master_shm_attach ();
  if (master_shm == NULL)
    {
      return ER_FAILED;
    }
  if (master_shm->num_ha_nodes > max_nodes)
    {
      assert (false);
      return ER_FAILED;
    }

  for (i = 0; i < master_shm->num_ha_nodes; i++)
    {
      nodes[i] = master_shm->ha_nodes[i];
    }

  *num_nodes = master_shm->num_ha_nodes;

  rye_master_shm_detach (master_shm);

  return NO_ERROR;
}

/*
 * rye_master_shm_get_node_state()-
 *   return: error code
 *
 *   node_state(out):
 *   host(in):
 */
int
rye_master_shm_get_node_state (HA_STATE * node_state, const char *host_ip)
{
  RYE_SHM_MASTER *master_shm;
  int i;
  PRM_NODE_INFO node_info;

  if (node_state == NULL || host_ip == NULL)
    {
      assert (false);
      return ER_FAILED;
    }

  node_info.ip = hostname_to_ip (host_ip);
  node_info.port = prm_get_local_port_id ();	/* TODO: cgkang */

  *node_state = HA_STATE_NA;

  master_shm = rye_master_shm_attach ();
  if (master_shm == NULL)
    {
      return ER_FAILED;
    }

  for (i = 0; i < master_shm->num_ha_nodes; i++)
    {
      if (prm_is_same_node (&master_shm->ha_nodes[i].node_info,
			    &node_info) == true)
	{
	  *node_state = master_shm->ha_nodes[i].node_state;
	}
    }

  rye_master_shm_detach (master_shm);

  return NO_ERROR;
}

static void
dump_shard_mgmt_info (FILE * outfp, RYE_SHM_MASTER * rye_shm)
{
  int i, j;

  fprintf (outfp, "shard mgmt:\n");

  for (i = 0; i < RYE_SHD_MGMT_TABLE_SIZE; i++)
    {
      if (rye_shm->shd_mgmt_table[i].local_dbname[0] == '\0')
	{
	  break;
	}

      fprintf (outfp, "\t%s     nodeid:%d of %s\n",
	       rye_shm->shd_mgmt_table[i].local_dbname,
	       rye_shm->shd_mgmt_table[i].nodeid,
	       rye_shm->shd_mgmt_table[i].global_dbname);

      for (j = 0; j < RYE_SHD_MGMT_INFO_MAX_COUNT; j++)
	{
	  struct timeval time_val;
	  char time_array[256];
	  char node_str[MAX_NODE_INFO_STR_LEN];

	  if (rye_shm->shd_mgmt_table[i].shd_mgmt_info[j].node_info.port == 0)
	    {
	      break;
	    }

	  time_val.tv_sec =
	    rye_shm->shd_mgmt_table[i].shd_mgmt_info[j].sync_time;
	  time_val.tv_usec = 0;

	  (void) er_datetime (&time_val, time_array, sizeof (time_array));

	  prm_node_info_to_str (node_str, sizeof (node_str),
				&rye_shm->shd_mgmt_table[i].shd_mgmt_info[j].
				node_info);
	  fprintf (outfp, "\t\t%s %s\n", node_str, time_array);
	}
    }
}

/*
 * rye_master_shm_dump () -
 *    return: NO_ERROR or error code
 */
int
rye_master_shm_dump (FILE * outfp)
{
  RYE_SHM_MASTER *shm_master = NULL;
  int i;
  int num_shm_keys;
  int num_hb_nodes;

  shm_master = rye_master_shm_attach ();
  if (shm_master == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR,
	      1, "shm open error:shm key info");

      return ER_GENERIC_ERROR;
    }

  fprintf (outfp, "shm_key:%x\n", shm_master->shm_header.shm_key);

  assert (shm_master->num_db_servers <= SHM_MAX_DB_SERVERS);
  num_shm_keys = MIN (shm_master->num_db_servers, SHM_MAX_DB_SERVERS);
  for (i = 0; i < num_shm_keys; i++)
    {
      fprintf (outfp, "\tIndex:%-3d, type:%s, dbname:%s key:%08x, state:%d\n",
	       i, rye_shm_type_to_string (RYE_SHM_TYPE_SERVER),
	       shm_master->db_server_info[i].dbname,
	       shm_master->db_server_info[i].shm_key,
	       shm_master->db_server_info[i].server_state);
    }

  num_hb_nodes = MIN (shm_master->num_ha_nodes, SHM_MAX_HA_NODE_LIST);
  fprintf (outfp, "ha_nodes: %d\n", num_hb_nodes);
  for (i = 0; i < num_hb_nodes; i++)
    {
      char host[256];
      css_ip_to_str (host, sizeof (host),
		     shm_master->ha_nodes[i].node_info.ip);
      fprintf (outfp, "\t%s:node_state=%d priority=%d\n",
	       host, shm_master->ha_nodes[i].node_state,
	       shm_master->ha_nodes[i].priority);
    }

  dump_shard_mgmt_info (outfp, shm_master);

  fflush (outfp);

  if (shm_master != NULL)
    {
      rye_master_shm_detach (shm_master);
    }

  return NO_ERROR;
}

/*
 * set_shard_mgmt_info () -
 */
static void
set_shard_mgmt_info (RYE_SHD_MGMT_TABLE * shd_mgmt_table_entry,
		     const char *local_dbname,
		     const char *global_dbname,
		     short nodeid, const PRM_NODE_INFO * shard_mgmt_node_info)
{
  int i;
  int set_index = 0;

  if (shd_mgmt_table_entry->nodeid != nodeid ||
      strcmp (shd_mgmt_table_entry->global_dbname, global_dbname) != 0)
    {
      memset (shd_mgmt_table_entry, 0, sizeof (RYE_SHD_MGMT_TABLE));

      strncpy (shd_mgmt_table_entry->global_dbname, global_dbname,
	       RYE_SHD_MGMT_TABLE_DBNAME_SIZE - 1);
      shd_mgmt_table_entry->nodeid = nodeid;
    }

  if (shd_mgmt_table_entry->local_dbname[0] == '\0')
    {
      strncpy (shd_mgmt_table_entry->local_dbname, local_dbname,
	       RYE_SHD_MGMT_TABLE_DBNAME_SIZE - 1);
    }

  for (i = 0; i < RYE_SHD_MGMT_INFO_MAX_COUNT; i++)
    {
      if (shd_mgmt_table_entry->shd_mgmt_info[i].node_info.port == 0)
	{
	  /* shard mgmt info not found. */
	  set_index = i;
	  break;
	}
      else
	if (prm_is_same_node
	    (&shd_mgmt_table_entry->shd_mgmt_info[i].node_info,
	     shard_mgmt_node_info) == true)
	{
	  /* shard mgmt info exists. */
	  set_index = i;
	  break;
	}
      else
	{
	  /* if there is no room to save current shard mgmt info,
	     oldest shard mgmt info will be removed */
	  if (shd_mgmt_table_entry->shd_mgmt_info[i].sync_time <
	      shd_mgmt_table_entry->shd_mgmt_info[set_index].sync_time)
	    {
	      set_index = i;
	    }
	}

    }

  shd_mgmt_table_entry->shd_mgmt_info[set_index].node_info =
    *shard_mgmt_node_info;
  shd_mgmt_table_entry->shd_mgmt_info[set_index].sync_time = time (NULL);
}

/*
 * rye_master_shm_add_shard_mgmt_info () -
 *    return: NO_ERROR or ER_FAILED
 */
int
rye_master_shm_add_shard_mgmt_info (const char *local_dbname,
				    const char *global_dbname,
				    short nodeid,
				    const PRM_NODE_INFO *
				    shard_mgmt_node_info)
{
  int i, rv;
  RYE_SHM_MASTER *rye_shm;
  int error = ER_FAILED;

  rye_shm = rye_master_shm_attach ();
  if (rye_shm == NULL)
    {
      return ER_FAILED;;
    }

  rv = pthread_mutex_lock (&rye_shm->lock);
  if (rv == EOWNERDEAD)
    {
      pthread_mutex_unlock (&rye_shm->lock);
      rye_master_shm_detach (rye_shm);
      return ER_FAILED;
    }

  for (i = 0; i < RYE_SHD_MGMT_TABLE_SIZE; i++)
    {
      if (rye_shm->shd_mgmt_table[i].local_dbname[0] == '\0' ||
	  strcmp (local_dbname, rye_shm->shd_mgmt_table[i].local_dbname) == 0)
	{
	  set_shard_mgmt_info (&rye_shm->shd_mgmt_table[i], local_dbname,
			       global_dbname, nodeid, shard_mgmt_node_info);
	  error = NO_ERROR;
	  break;
	}
    }

  pthread_mutex_unlock (&rye_shm->lock);

  rye_master_shm_detach (rye_shm);

  return error;
}

int
rye_master_shm_get_server_state (const char *dbname, HA_STATE * server_state)
{
  RYE_SHM_MASTER *shm_master;
  int num_db_servers;
  int i;

  if (dbname == NULL || server_state == NULL)
    {
      assert (false);
      return ER_FAILED;
    }

  *server_state = HA_STATE_UNKNOWN;

  shm_master = rye_master_shm_attach ();
  if (shm_master == NULL)
    {
      return ER_FAILED;;
    }

  num_db_servers = MIN (shm_master->num_db_servers, SHM_MAX_DB_SERVERS);
  for (i = 0; i < num_db_servers; i++)
    {
      if (strcmp (dbname, shm_master->db_server_info[i].dbname) == 0)
	{
	  *server_state = shm_master->db_server_info[i].server_state;
	}
    }

  rye_master_shm_detach (shm_master);
  return NO_ERROR;
}

int
rye_master_shm_set_server_state (const char *dbname, int server_state)
{
  RYE_SHM_MASTER *shm_master;
  int num_db_servers;
  int i;

  shm_master = rye_master_shm_attach ();
  if (shm_master == NULL)
    {
      return ER_FAILED;;
    }

  num_db_servers = MIN (shm_master->num_db_servers, SHM_MAX_DB_SERVERS);
  for (i = 0; i < num_db_servers; i++)
    {
      if (strcmp (dbname, shm_master->db_server_info[i].dbname) == 0)
	{
	  shm_master->db_server_info[i].server_state = server_state;
	}
    }

  rye_master_shm_detach (shm_master);
  return NO_ERROR;
}

/*
 * rye_master_shm_get_server_shm_key ()-
 *   retrun: -1 or shm key
 *
 *   dbname(in):
 */
int
rye_master_shm_get_server_shm_key (const char *dbname)
{
  RYE_SHM_MASTER *shm_master;
  int server_shm_key = -1;
  int i;
  int num_db_servers;

  shm_master = rye_master_shm_attach ();
  if (shm_master == NULL)
    {
      return -1;
    }

  assert (shm_master->num_db_servers <= SHM_MAX_DB_SERVERS);
  num_db_servers = MIN (shm_master->num_db_servers, SHM_MAX_DB_SERVERS);
  for (i = 0; i < num_db_servers; i++)
    {
      if (strcmp (shm_master->db_server_info[i].dbname, dbname) == 0)
	{
	  server_shm_key = shm_master->db_server_info[i].shm_key;
	  break;
	}
    }

  rye_master_shm_detach (shm_master);

  return server_shm_key;
}

/*
 * rye_master_shm_set_server_shm_key ()-
 *    return: -1 or  key index
 *
 *    dbname(in):
 *    server_shm_key(in):
 */
int
rye_master_shm_set_server_shm_key (const char *dbname, int server_shm_key)
{
  RYE_SHM_MASTER *shm_master;
  int i;
  int num_db_servers;

  shm_master = rye_master_shm_attach ();
  if (shm_master == NULL)
    {
      return -1;
    }

  assert (shm_master->num_db_servers <= SHM_MAX_DB_SERVERS);
  num_db_servers = MIN (shm_master->num_db_servers, SHM_MAX_DB_SERVERS);
  for (i = 0; i < num_db_servers; i++)
    {
      if (strcmp (shm_master->db_server_info[i].dbname, dbname) == 0)
	{
	  shm_master->db_server_info[i].shm_key = server_shm_key;
	  break;
	}
    }

  rye_master_shm_detach (shm_master);

  if (i == num_db_servers)
    {
      return -1;
    }

  return i;
}

/*
 * rye_master_shm_get_node_reset_time()-
 *   return: error code
 *
 *   node_reset_time(out):
 */
int
rye_master_shm_get_node_reset_time (INT64 * node_reset_time)
{
  RYE_SHM_MASTER *master_shm;

  if (node_reset_time == NULL)
    {
      assert (false);
      return ER_FAILED;
    }

  *node_reset_time = -1;
  master_shm = rye_master_shm_attach ();
  if (master_shm == NULL)
    {
      return ER_FAILED;
    }

  *node_reset_time = master_shm->ha_node_reset_time;

  rye_master_shm_detach (master_shm);

  return NO_ERROR;
}
