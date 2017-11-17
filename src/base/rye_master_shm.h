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
 * rye_master_shm.h -
 */

#ifndef _RYE_MASTER_SHM_H_
#define _RYE_MASTER_SHM_H_

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <sys/types.h>
#include <pthread.h>

#include "porting.h"
#include "rye_shm.h"

typedef struct _rye_shm_master RYE_SHM_MASTER;
struct _rye_shm_master
{
  RYE_SHM_HEADER shm_header;

  pthread_mutex_t lock;

  RYE_SHD_MGMT_TABLE shd_mgmt_table[RYE_SHD_MGMT_TABLE_SIZE];

  INT64 ha_node_reset_time;
  int num_ha_nodes;
  RYE_SHM_HA_NODE ha_nodes[SHM_MAX_HA_NODE_LIST];

  int num_db_servers;
  RYE_SHM_DB_SERVER_INFO db_server_info[SHM_MAX_DB_SERVERS];
};

/* master use function */
extern int master_shm_initialize (void);
extern int master_shm_final (void);
extern int master_shm_set_node_state (const char *host_name,
				      unsigned short node_state);
extern int master_shm_set_node_version (const char *host_name,
					const RYE_VERSION * node_version);
extern int master_shm_reset_hb_nodes (RYE_SHM_HA_NODE * nodes, int num_nodes);

extern int master_shm_get_server_state (const char *dbname);
extern int master_shm_update_server_state (int pid,
					   unsigned char server_state);

extern int master_shm_get_shard_mgmt_info (const char *local_dbname,
					   char *global_dbname,
					   short *nodeid,
					   unsigned char *shard_mgmt_ip,
					   int *shard_mgmt_port);

/* non-master use functions */
extern int rye_master_shm_get_new_server_shm_key (const char *dbname,
						  int server_pid);
extern int rye_master_shm_get_server_shm_key (const char *dbname);
extern int rye_master_shm_set_server_shm_key (const char *dbname,
					      int server_shm_key);

extern int rye_master_shm_get_server_state (const char *dbname,
					    HA_STATE * server_state);
extern int rye_master_shm_set_server_state (const char *dbname,
					    int server_state);

extern int rye_master_shm_get_node_reset_time (INT64 * node_reset_time);

extern int rye_master_shm_get_ha_nodes (RYE_SHM_HA_NODE * nodes,
					int *num_nodes, int max_nodes);
extern int rye_master_shm_get_node_state (HA_STATE * node_state,
					  const char *host);

extern int rye_master_shm_add_shard_mgmt_info (const char *local_dbname,
					       const char *global_dbname,
					       short nodeid,
					       const unsigned char
					       *shard_mgmt_ip,
					       int shard_mgmt_port);

extern int rye_master_shm_dump (FILE * outfp);

#endif /* _RYE_MASTER_SHM_H_ */
