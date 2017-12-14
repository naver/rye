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
 * rye_server_shm.h -
 */

#ifndef _RYE_SERVER_SHM_H_
#define _RYE_SERVER_SHM_H_

#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <sys/types.h>
#include <pthread.h>

#include "porting.h"
#include "rye_shm.h"
#include "perf_monitor.h"
#include "storage_common.h"

#include "repl.h"

/* GROUP ID BITMAP */

#define SHARD_MULTI_BOUND_BIT_MASK(element) (1 << ((int)(element) & 7))

#define SHARD_MULTI_GET_BOUND_BIT_BYTE(bitptr, element)       \
        ((char *)(bitptr) + ((int)(element) >> 3))

#define SHARD_GET_GROUP_BIT(bitptr, element)                  \
        ((*SHARD_MULTI_GET_BOUND_BIT_BYTE(bitptr, element)) & \
         SHARD_MULTI_BOUND_BIT_MASK(element))

#define SHARD_ENABLE_GROUP_BIT(bitptr, element)               \
        *SHARD_MULTI_GET_BOUND_BIT_BYTE(bitptr, element) =    \
          *SHARD_MULTI_GET_BOUND_BIT_BYTE(bitptr, element) |  \
          SHARD_MULTI_BOUND_BIT_MASK(element)

#define SHARD_CLEAR_GROUP_BIT(bitptr, element)                \
        *SHARD_MULTI_GET_BOUND_BIT_BYTE(bitptr, element) =    \
          *SHARD_MULTI_GET_BOUND_BIT_BYTE(bitptr, element) &  \
          ~SHARD_MULTI_BOUND_BIT_MASK(element)


/* max groupid is 1000000 */
#define SHARD_MAX_BITMAP_SIZE (130*ONE_K)

typedef struct _server_shm_tran_info SERVER_SHM_TRAN_INFO;
struct _server_shm_tran_info
{
  MNT_SERVER_EXEC_STATS stats;
};

typedef struct _server_shm_groupid_bitmap SERVER_SHM_SHARD_INFO;
struct _server_shm_groupid_bitmap
{
  short nodeid;
  int num_groupid;
  int groupid_bitmap_size;
  char groupid_bitmap[SHARD_MAX_BITMAP_SIZE];	/* shard groupid bitmap */
};

typedef struct _server_shm_repl_info SERVER_SHM_REPL_INFO;
struct _server_shm_repl_info
{
  char host_ip[HOST_IP_SIZE];
  bool is_local_host;
  HA_APPLY_STATE state;
};

typedef struct _server_shm_ha_info SERVER_SHM_HA_INFO;
struct _server_shm_ha_info
{
  LOG_LSA eof_lsa;

  int num_repl;
  SERVER_SHM_REPL_INFO repl_info[SHM_MAX_REPL_COUNT];
};

typedef struct _rye_server_shm RYE_SERVER_SHM;
struct _rye_server_shm
{
  RYE_SHM_HEADER shm_header;

  int start_time;
  HA_STATE server_state;

  SERVER_SHM_SHARD_INFO shard_info;
  SERVER_SHM_HA_INFO ha_info;

  char dbname[SHM_DBNAME_SIZE];
  int ntrans;
  int num_stats_values;

  MNT_SERVER_EXEC_STATS global_stats;
  SERVER_SHM_TRAN_INFO tran_info[1];
};

/* server functions */
extern int svr_shm_initialize (const char *dbname, int max_ntrans);
extern int svr_shm_get_start_time (void);
#if 0
extern void svr_shm_clear_stats (int tran_index, MNT_SERVER_ITEM item);
#endif
extern void svr_shm_copy_stats (int tran_index,
				MNT_SERVER_EXEC_STATS * to_stats);
extern void svr_shm_copy_global_stats (MNT_SERVER_EXEC_STATS * to_stats);
extern void svr_shm_stats_counter_with_time (int tran_index,
					     MNT_SERVER_ITEM item,
					     INT64 value, UINT64 exec_time);
extern void svr_shm_stats_gauge (int tran_index, MNT_SERVER_ITEM item,
				 INT64 value);
extern INT64 svr_shm_get_stats_with_time (int tran_index,
					  MNT_SERVER_ITEM item,
					  UINT64 * acc_time);
#if 0
extern INT64 svr_shm_get_stats (int tran_index, MNT_SERVER_ITEM item);
#endif
extern void svr_shm_set_eof (LOG_LSA * eof);
extern short svr_shm_get_nodeid (void);
extern bool svr_shm_is_group_own (int gid);
extern int svr_shm_enable_group_bit (int gid);
extern int svr_shm_clear_group_bit (int gid);
extern int svr_shm_set_server_state (HA_STATE server_state);
extern HA_STATE svr_shm_get_server_state (void);
extern bool svr_shm_check_repl_done (void);
extern int svr_shm_sync_node_info_to_repl (void);
extern int svr_shm_set_repl_info (const char *host_ip, HA_APPLY_STATE state);


/* client functions */
extern int rye_server_shm_set_groupid_bitmap (SERVER_SHM_SHARD_INFO *
					      shard_info, const char *dbname);
extern int rye_server_shm_set_state (const char *dbname,
				     HA_STATE server_state);
extern int rye_server_shm_get_state (HA_STATE * server_state,
				     const char *dbname);
extern int rye_server_shm_get_global_stats (MNT_SERVER_EXEC_STATS *
					    global_stats, const char *dbname);
extern int rye_server_shm_get_eof_lsa (LOG_LSA * eof_lsa, const char *dbname);
extern int rye_server_shm_get_nodeid (short *nodeid, const char *dbname);

#endif /* _RYE_SERVER_SHM_H_ */
