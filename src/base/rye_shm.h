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
 * rye_shm.h -
 */

#ifndef _RYE_SHM_H_
#define _RYE_SHM_H_

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <sys/types.h>
#include <pthread.h>

#include "porting.h"

#include "connection_defs.h"


#define SHM_MAX_HA_NODE_LIST		(PRM_MAX_HA_NODE_LIST)
#define SHM_MAX_REPL_COUNT              SHM_MAX_HA_NODE_LIST
#define SHM_MAX_DB_SERVERS              (10)
#define MAX_NUM_SHM		        (SHM_MAX_DB_SERVERS * 3)

#define DEFAULT_RYE_SHM_KEY		((getuid() << 16) | 0x00001000)
#define DEFUALT_SERVER_SHM_KEY_BASE		(0x00000100)
#define DEFUALT_MONITOR_SERVER_SHM_KEY_BASE	(0x00000200)
#define DEFUALT_BROKER_SHM_KEY_BASE		(0x00000300)

#define RYE_SHM_MAGIC_STR_SIZE		(8)
#define RYE_SHM_MAGIC_STR		"RYE SHM"
#define RYE_SHM_MAGIC_NUMBER            (MAJOR_VERSION * 1000000 + MINOR_VERSION * 10000 + PATCH_VERSION)

#define RYE_SHD_MGMT_TABLE_SIZE		SHM_MAX_DB_SERVERS
#define RYE_SHD_MGMT_TABLE_DBNAME_SIZE	(32)
#define RYE_SHD_MGMT_INFO_MAX_COUNT	(4)

#define SHM_DBNAME_SIZE			(32)
#define SHM_NAME_SIZE                   (64)

typedef enum _rye_shm_status RYE_SHM_STATUS;
enum _rye_shm_status
{
  RYE_SHM_UNKNOWN,
  RYE_SHM_CREATED,
  RYE_SHM_INVALID,
  RYE_SHM_VALID,
  RYE_SHM_MARK_DELETED
};

#define RYE_SHM_STATUS_NAME(status)                                              \
  ((status) == RYE_SHM_UNKNOWN ? "RYE_SHM_UNKNOWN" :                             \
   (status) == RYE_SHM_CREATED ? "RYE_SHM_CREATED" :                             \
   (status) == RYE_SHM_INVALID ? "RYE_SHM_INVALID" :                             \
   (status) == RYE_SHM_MARK_DELETED ? "RYE_SHM_MARK_DELETED" : "UNKNOWN STATUS")

typedef enum _rye_shm_type RYE_SHM_TYPE;
enum _rye_shm_type
{
  RYE_SHM_TYPE_UNKNOWN,
  RYE_SHM_TYPE_MASTER,
  RYE_SHM_TYPE_SERVER,
  RYE_SHM_TYPE_BROKER_GLOBAL,
  RYE_SHM_TYPE_BROKER_LOCAL,
  RYE_SHM_TYPE_MONITOR_SERVER,
  RYE_SHM_TYPE_MAX = RYE_SHM_TYPE_MONITOR_SERVER
};

#define RYE_SHM_TYPE_NAME(type)                                                        \
  ((type) == RYE_SHM_TYPE_UNKNOWN ? "RYE_SHM_TYPE_UNKNOWN" :                           \
   (type) == RYE_SHM_TYPE_MASTER ? "RYE_SHM_TYPE_MASTER" :                             \
   (type) == RYE_SHM_TYPE_SERVER ? "RYE_SHM_TYPE_SERVER" :                             \
   (type) == RYE_SHM_TYPE_BROKER_GLOBAL ? "RYE_SHM_TYPE_BROKER_GLOBAL" :               \
   (type) == RYE_SHM_TYPE_BROKER_LOCAL ? "RYE_SHM_TYPE_BROKER_LOCAL" :                 \
   (type) == RYE_SHM_TYPE_MONITOR_SERVER ? "RYE_SHM_TYPE_MONITOR_SERVER" : "UNKNOWN TYPE")


typedef struct _rye_shd_mgmt_table RYE_SHD_MGMT_TABLE;
struct _rye_shd_mgmt_table
{
  char local_dbname[RYE_SHD_MGMT_TABLE_DBNAME_SIZE];
  char global_dbname[RYE_SHD_MGMT_TABLE_DBNAME_SIZE];
  short nodeid;
  struct
  {
    PRM_NODE_INFO node_info;
    int sync_time;
  } shd_mgmt_info[RYE_SHD_MGMT_INFO_MAX_COUNT];
};

typedef struct _rye_shm_header RYE_SHM_HEADER;
struct _rye_shm_header
{
  char magic_string[RYE_SHM_MAGIC_STR_SIZE];
  int magic_number;
  RYE_SHM_TYPE shm_type;
  RYE_SHM_STATUS status;
  int shm_key;
  RYE_VERSION shm_version;
};

typedef struct _rye_shm_ha_node RYE_SHM_HA_NODE;
struct _rye_shm_ha_node
{
  RYE_VERSION ha_node_version;
  bool is_localhost;
  PRM_NODE_INFO node_info;
  HA_STATE node_state;
  unsigned short priority;
};

typedef struct _rye_shm_info RYE_SHM_INFO;
struct _rye_shm_info
{
  char name[SHM_NAME_SIZE];
  int shm_key;
  RYE_SHM_TYPE type;
};

extern void *rye_shm_create (int shm_key, int size, RYE_SHM_TYPE shm_type);
extern RYE_SHM_TYPE rye_shm_check_shm (int shm_key, RYE_SHM_TYPE shm_type,
				       bool check_status);
extern bool rye_shm_is_used_key (int shm_key);
extern void *rye_shm_attach (int shm_key, RYE_SHM_TYPE shm_type,
			     bool is_monitoring);
extern int rye_shm_detach (void *shm_p);
extern int rye_shm_destroy (int shm_key);
extern int rye_shm_mutex_init (pthread_mutex_t * mutex);
#if defined(RYE_SHM_UNUSED_FUNCTION)
extern int rye_shm_cond_init (pthread_cond_t * cond);
#endif




extern int rye_shm_destroy_all_server_shm (void);



#endif /* _RYE_LOG_SHM_H_ */
