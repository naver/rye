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
 * rye_server_shm.c -
 */

#ident "$Id$"

#include <sys/types.h>
#include <sys/time.h>
#include <errno.h>
#include <sys/ipc.h>
#include <sys/shm.h>
#include <assert.h>

#include "rye_shm.h"
#include "rye_master_shm.h"
#include "rye_server_shm.h"
#include "error_manager.h"
#include "system_parameter.h"

static RYE_SERVER_SHM *rye_Server_shm = NULL;

static RYE_SERVER_SHM *rye_server_shm_attach (const char *dbname, bool is_monitoring);
static void rye_server_shm_detach (RYE_SERVER_SHM * shm_p);


/***********************************************************
 * server functions
 ***********************************************************/

/*
 * svr_shm_initialize -
 */
int
svr_shm_initialize (const char *dbname)
{
  int error = NO_ERROR;
  RYE_SERVER_SHM *shm_server = NULL;
  int svr_shm_key, shm_size;

#if !defined(SERVER_MODE)
  return NO_ERROR;
#endif

  svr_shm_key = rye_master_shm_get_new_shm_key (dbname, RYE_SHM_TYPE_SERVER);
  if (svr_shm_key < 0)
    {
      return ER_FAILED;
    }

  if (rye_shm_check_shm (svr_shm_key, RYE_SHM_TYPE_SERVER, false) == RYE_SHM_TYPE_SERVER)
    {
      rye_shm_destroy (svr_shm_key);
    }

  shm_size = sizeof (RYE_SERVER_SHM);
  shm_server = rye_shm_create (svr_shm_key, shm_size, RYE_SHM_TYPE_SERVER);
  if (shm_server == NULL)
    {
      return ER_FAILED;
    }

  shm_server->shard_info.groupid_bitmap_size = -1;
  shm_server->shard_info.nodeid = -1;
  shm_server->shard_info.num_groupid = 0;

  /* init ha_info */
  LSA_SET_NULL (&shm_server->ha_info.eof_lsa);
  shm_server->ha_info.num_repl = 0;

  shm_server->shm_header.status = RYE_SHM_VALID;
  shm_server->start_time = time (NULL);
  shm_server->server_state = HA_STATE_UNKNOWN;

  rye_Server_shm = shm_server;

  return error;
}

int
svr_shm_get_start_time ()
{
#if defined(SERVER_MODE)
  if (rye_Server_shm == NULL)
    {
      assert (0);
      return 0;
    }
  else
    {
      return rye_Server_shm->start_time;
    }
#else /* SERVER_MODE */
  return 0;
#endif /* SERVER_MODE */
}

#if 0
/*
 * svr_shm_clear_stats - Clear recorded server statistics for the current
 *                          transaction index
 *   return: none
 *   item(in):
 */
void
svr_shm_clear_stats (int tran_index, MNT_SERVER_ITEM item)
{
  assert (tran_index >= 0);

  if (rye_Server_shm == NULL || tran_index < 0 || tran_index >= rye_Server_shm->ntrans)
    {
      assert (false);
    }
  else
    {
      rye_Server_shm->tran_info[tran_index].stats.values[item] = 0;
      rye_Server_shm->tran_info[tran_index].stats.acc_time[item] = 0;
    }
}
#endif

/*
 * svr_shm_set_eof ()-
 *
 *   eof(in):
 */
void
svr_shm_set_eof (LOG_LSA * eof)
{
  assert (eof != NULL);

  if (rye_Server_shm == NULL || eof == NULL)
    {
      return;
    }

  LSA_COPY (&rye_Server_shm->ha_info.eof_lsa, eof);

  return;
}

/*
 * svr_shm_get_nodeid ()-
 *
 *   eof(in):
 */
short
svr_shm_get_nodeid (void)
{
  if (rye_Server_shm == NULL)
    {
      return -1;
    }

  if (rye_Server_shm->shard_info.groupid_bitmap_size <= 0)
    {
      assert (rye_Server_shm->shard_info.groupid_bitmap_size == -1);
      return -1;
    }

  assert (rye_Server_shm->shard_info.nodeid > 0);
  return rye_Server_shm->shard_info.nodeid;
}

/*
 * svr_shm_is_group_own ()
 *   return:
 *
 *   gid(in):
 */
bool
svr_shm_is_group_own (int gid)
{
  char *bitmap;

  if (rye_Server_shm == NULL)
    {
      return ER_FAILED;
    }

  if (rye_Server_shm->shard_info.groupid_bitmap_size <= 0)
    {
      assert (rye_Server_shm->shard_info.groupid_bitmap_size == -1);

      /* give up */
      return false;
    }

  if (rye_Server_shm->shard_info.num_groupid <= 0
      || gid < GLOBAL_GROUPID || gid > rye_Server_shm->shard_info.num_groupid)
    {
      assert (false);
      return false;
    }

  bitmap = rye_Server_shm->shard_info.groupid_bitmap;
  assert (!SHARD_GET_GROUP_BIT (bitmap, GLOBAL_GROUPID));

  if (SHARD_GET_GROUP_BIT (bitmap, gid))
    {
      return true;
    }

  return false;
}

/*
 * svr_shm_enable_group_bit ()
 *   return: error code
 *
 *   gid(in):
 */
int
svr_shm_enable_group_bit (int gid)
{
  char *bitmap;

  if (rye_Server_shm == NULL)
    {
      assert (false);
      return ER_FAILED;
    }

  if (rye_Server_shm->shard_info.groupid_bitmap_size <= 0)
    {
      assert (false);
      return ER_FAILED;
    }

  if (gid < GLOBAL_GROUPID || gid > rye_Server_shm->shard_info.num_groupid)
    {
      assert (false);
      return ER_FAILED;
    }

  if (gid > GLOBAL_GROUPID)
    {
      bitmap = rye_Server_shm->shard_info.groupid_bitmap;

      SHARD_ENABLE_GROUP_BIT (bitmap, gid);
    }

  return NO_ERROR;
}

/*
 * svr_shm_clear_group_bit ()
 *   return: error code
 *
 *   gid(in):
 */
int
svr_shm_clear_group_bit (int gid)
{
  char *bitmap;

  if (rye_Server_shm == NULL)
    {
      assert (false);
      return ER_FAILED;
    }

  if (rye_Server_shm->shard_info.groupid_bitmap_size <= 0)
    {
      assert (false);
      return ER_FAILED;
    }

  if (gid < GLOBAL_GROUPID || gid > rye_Server_shm->shard_info.num_groupid)
    {
      assert (false);
      return ER_FAILED;
    }

  if (gid > GLOBAL_GROUPID)
    {
      bitmap = rye_Server_shm->shard_info.groupid_bitmap;

      SHARD_CLEAR_GROUP_BIT (bitmap, gid);
    }

  return NO_ERROR;
}

/*
 * svr_shm_set_server_state ()
 *   return: NO_ERROR
 *
 *   server_state(in):
 */
int
svr_shm_set_server_state (HA_STATE server_state)
{
  if (rye_Server_shm == NULL)
    {
      assert (false);
      return ER_FAILED;
    }

  if (rye_Server_shm->server_state != server_state)
    {
      rye_Server_shm->server_state = server_state;
    }

  return NO_ERROR;
}

/*
 * svr_shm_get_server_state ()
 *   return: ha state
 *
 *   server_state(in):
 */
HA_STATE
svr_shm_get_server_state (void)
{
  if (rye_Server_shm == NULL)
    {
      assert (false);
      return HA_STATE_NA;
    }

  return rye_Server_shm->server_state;
}

/*
 * svr_shm_check_repl_done ()
 *   return:
 *
 */
bool
svr_shm_check_repl_done (void)
{
  HA_STATE server_state;
  bool found = false;
  int i;

  if (rye_Server_shm == NULL)
    {
      assert (false);
      return false;
    }

  for (i = 0; i < rye_Server_shm->ha_info.num_repl; i++)
    {
      if (rye_Server_shm->ha_info.repl_info[i].is_local_host == false
          && rye_Server_shm->ha_info.repl_info[i].state != HA_APPLY_STATE_DONE)
        {
          found = true;
          break;
        }
    }

  if (found == false)
    {
      server_state = svr_shm_get_server_state ();
      if (server_state == HA_STATE_TO_BE_MASTER || server_state == HA_STATE_MASTER)
        {
          return true;
        }
    }

  return false;
}

/*
 * svr_shm_sync_node_info_to_repl ()
 *   return: error code
 *
 */
int
svr_shm_sync_node_info_to_repl (void)
{
  RYE_SHM_HA_NODE nodes[SHM_MAX_HA_NODE_LIST];
  SERVER_SHM_REPL_INFO new_repl_info[SHM_MAX_REPL_COUNT];
  SERVER_SHM_REPL_INFO *repl_info;
  int num_nodes;
  int i, j;
  int error = NO_ERROR;
  bool found;
  int new_repl_count;

  if (rye_Server_shm == NULL)
    {
      assert (false);
      return ER_FAILED;
    }

  error = rye_master_shm_get_ha_nodes (nodes, &num_nodes, SHM_MAX_HA_NODE_LIST);
  if (error != NO_ERROR)
    {
      return error;
    }
  if (num_nodes <= 0)
    {
      assert (false);
      return ER_FAILED;
    }

  /* copy existed node info */
  new_repl_count = 0;
  for (i = 0; i < rye_Server_shm->ha_info.num_repl; i++)
    {
      repl_info = &rye_Server_shm->ha_info.repl_info[i];
      found = false;
      for (j = 0; j < num_nodes; j++)
        {
          if (prm_is_same_node (&nodes[j].node_info, &repl_info->node_info) == true)
            {
              found = true;
              break;
            }
        }
      if (found == true)
        {
          new_repl_info[new_repl_count++] = *repl_info;
        }
    }

  /* append added node info */
  for (i = 0; i < num_nodes; i++)
    {
      found = false;
      for (j = 0; j < rye_Server_shm->ha_info.num_repl; j++)
        {
          repl_info = &rye_Server_shm->ha_info.repl_info[j];
          if (prm_is_same_node (&nodes[i].node_info, &repl_info->node_info) == true)
            {
              found = true;
              break;
            }
        }

      if (found == false)
        {
          new_repl_info[new_repl_count].node_info = nodes[i].node_info;
          new_repl_info[new_repl_count].state = HA_APPLY_STATE_UNREGISTERED;
          new_repl_info[new_repl_count].is_local_host = nodes[i].is_localhost;

          new_repl_count++;
        }
    }

  rye_Server_shm->ha_info.num_repl = new_repl_count;
  for (i = 0; i < new_repl_count; i++)
    {
      rye_Server_shm->ha_info.repl_info[i] = new_repl_info[i];
    }

  return NO_ERROR;
}

/*
 * svr_shm_set_repl_info
 */
int
svr_shm_set_repl_info (const PRM_NODE_INFO * node_info, HA_APPLY_STATE state)
{
  SERVER_SHM_REPL_INFO *found_repl_info = NULL, *repl_info;
  int error = NO_ERROR;
  int i;

  if (rye_Server_shm == NULL)
    {
      assert (false);
      return ER_FAILED;
    }

  assert (state >= HA_APPLY_STATE_UNREGISTERED && state <= HA_APPLY_STATE_ERROR);

  error = svr_shm_sync_node_info_to_repl ();
  if (error != NO_ERROR)
    {
      return error;
    }

  found_repl_info = NULL;
  for (i = 0; i < rye_Server_shm->ha_info.num_repl; i++)
    {
      repl_info = &rye_Server_shm->ha_info.repl_info[i];
      if (prm_is_same_node (&repl_info->node_info, node_info) == true)
        {
          found_repl_info = repl_info;
          break;
        }
    }

  if (found_repl_info == NULL)
    {
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
      return error;
    }

  if (found_repl_info->state != state)
    {
      found_repl_info->state = state;
    }

  return NO_ERROR;
}

/***********************************************************
 * client functions
 ***********************************************************/

/*
 *
 */
static RYE_SERVER_SHM *
rye_server_shm_attach (const char *dbname, bool is_monitoring)
{
  RYE_SERVER_SHM *shm_p;
  int svr_shm_key;

  assert (rye_Server_shm == NULL);

  svr_shm_key = rye_master_shm_get_shm_key (dbname, RYE_SHM_TYPE_SERVER);
  shm_p = rye_shm_attach (svr_shm_key, RYE_SHM_TYPE_SERVER, is_monitoring);

  return shm_p;
}

/*
 * rye_server_shm_detach() -
 *   return:
 *
 *   shm_p(in/out):
 */
static void
rye_server_shm_detach (RYE_SERVER_SHM * shm_p)
{
  assert (rye_Server_shm == NULL);

  if (shm_p == NULL)
    {
      assert (false);
      return;
    }

  (void) rye_shm_detach (shm_p);

  return;
}

/*
 * rye_server_shm_set_groupid_bitmap ()-
 *
 *   bitmap(in):
 *   dbname(in):
 */
int
rye_server_shm_set_groupid_bitmap (SERVER_SHM_SHARD_INFO * shard_info, const char *dbname)
{
  RYE_SERVER_SHM *shm_p;

  assert (rye_Server_shm == NULL);

  if (dbname == NULL || shard_info == NULL)
    {
      assert (false);
      return ER_FAILED;
    }

  shm_p = rye_server_shm_attach (dbname, false);
  if (shm_p == NULL)
    {
      return ER_FAILED;
    }

  if (shard_info->groupid_bitmap_size <= 0
      || shard_info->groupid_bitmap_size >= SHARD_MAX_BITMAP_SIZE
      || shard_info->nodeid < 0 || shard_info->num_groupid <= 0 || shard_info->num_groupid > GROUPID_MAX)
    {
      assert (false);
      return ER_FAILED;
    }

  memcpy (shm_p->shard_info.groupid_bitmap, shard_info->groupid_bitmap, shard_info->groupid_bitmap_size);
  shm_p->shard_info.nodeid = shard_info->nodeid;
  shm_p->shard_info.num_groupid = shard_info->num_groupid;

  shm_p->shard_info.groupid_bitmap_size = shard_info->groupid_bitmap_size;

  rye_server_shm_detach (shm_p);

  return NO_ERROR;
}

/*
 * rye_server_shm_set_state ()-
 *
 *   dbname(in):
 *   server_state(in):
 */
int
rye_server_shm_set_state (const char *dbname, HA_STATE server_state)
{
  RYE_SERVER_SHM *shm_p;

  assert (rye_Server_shm == NULL);

  if (dbname == NULL)
    {
      assert (false);
      return ER_FAILED;
    }

  shm_p = rye_server_shm_attach (dbname, false);
  if (shm_p == NULL)
    {
      return ER_FAILED;
    }

  shm_p->server_state = server_state;

  rye_server_shm_detach (shm_p);

  return NO_ERROR;
}

/*
 * rye_server_shm_get_state ()-
 *   return: error code
 *
 *   server_state(out):
 *   dbname(in):
 */
int
rye_server_shm_get_state (HA_STATE * server_state, const char *dbname)
{
  RYE_SERVER_SHM *shm_p;

  assert (rye_Server_shm == NULL);

  if (dbname == NULL || server_state == NULL)
    {
      assert (false);
      return ER_FAILED;
    }
  *server_state = HA_STATE_UNKNOWN;

  shm_p = rye_server_shm_attach (dbname, true);
  if (shm_p == NULL)
    {
      return ER_FAILED;
    }

  *server_state = shm_p->server_state;

  rye_server_shm_detach (shm_p);

  return NO_ERROR;
}

/*
 * rye_server_shm_get_eof_lsa ()-
 *   return: error code
 *
 *   eof_lsa(out):
 *   dbname(in):
 */
int
rye_server_shm_get_eof_lsa (LOG_LSA * eof_lsa, const char *dbname)
{
  RYE_SERVER_SHM *shm_p;

  assert (rye_Server_shm == NULL);

  if (dbname == NULL || eof_lsa == NULL)
    {
      assert (false);
      return ER_FAILED;
    }

  shm_p = rye_server_shm_attach (dbname, true);
  if (shm_p == NULL)
    {
      return ER_FAILED;
    }

  LSA_COPY (eof_lsa, &shm_p->ha_info.eof_lsa);

  rye_server_shm_detach (shm_p);

  return NO_ERROR;
}

/*
 * rye_server_shm_get_nodeid ()-
 *   return: error code
 *
 *   nodeid(out):
 *   dbname(in):
 */
int
rye_server_shm_get_nodeid (short *nodeid, const char *dbname)
{
  RYE_SERVER_SHM *shm_p;

  assert (rye_Server_shm == NULL);

  if (dbname == NULL || nodeid == NULL)
    {
      assert (false);
      return ER_FAILED;
    }

  shm_p = rye_server_shm_attach (dbname, true);
  if (shm_p == NULL)
    {
      return ER_FAILED;
    }

  *nodeid = shm_p->shard_info.nodeid;

  rye_server_shm_detach (shm_p);

  return NO_ERROR;
}

/*
 * rye_server_shm_dump ()-
 *   return: error code
 *
 *   out_fp(out):
 *   shm_key(in):
 */
int
rye_server_shm_dump (FILE * out_fp, int shm_key)
{
  RYE_SERVER_SHM *shm_p;
  int i, index;
  char buf[256];

  assert (rye_Server_shm == NULL);

  if (out_fp == NULL)
    {
      assert (false);
      return ER_FAILED;
    }

  shm_p = rye_shm_attach (shm_key, RYE_SHM_TYPE_SERVER, true);
  if (shm_p == NULL)
    {
      return ER_FAILED;
    }

  /* shard info */
  fprintf (out_fp, "\t\t node_id:%d, num_groupid:%d bitmap_size:%d\n",
           shm_p->shard_info.nodeid, shm_p->shard_info.num_groupid, shm_p->shard_info.groupid_bitmap_size);

  /* ha info */
  fprintf (out_fp, "\t\t state:%s, eof:%ld,%d, num_repl:%d\n",
           HA_STATE_NAME (shm_p->server_state),
           (long) shm_p->ha_info.eof_lsa.pageid, shm_p->ha_info.eof_lsa.offset, shm_p->ha_info.num_repl);

  /* repl info */
  index = 0;
  for (i = 0; i < shm_p->ha_info.num_repl; i++)
    {
      if (shm_p->ha_info.repl_info[i].is_local_host == true)
        {
          continue;
        }
      prm_node_info_to_str (buf, sizeof (buf), &shm_p->ha_info.repl_info[i].node_info);

      fprintf (out_fp, "\t\t\t Repl:%d, node:%s, state:%s\n",
               index++, buf, HA_APPLY_STATE_NAME (shm_p->ha_info.repl_info[i].state));
    }

  rye_server_shm_detach (shm_p);

  return NO_ERROR;
}
