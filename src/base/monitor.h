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
 * monitor.h - Monitor execution statistics
 */

#ifndef MONITOR_H_
#define MONITOR_H_

#ident "$Id$"

#include <stdio.h>
#include <time.h>
#include <sys/time.h>

#include "porting.h"
#include "rye_shm.h"

/* EXPORTED GLOBAL DEFINITIONS */

typedef enum
{
  MNT_DUMP_TYPE_NORMAL,
  MNT_DUMP_TYPE_CSV_DATA,
  MNT_DUMP_TYPE_CSV_HEADER
} MONITOR_DUMP_TYPE;

typedef enum
{
  MONITOR_TYPE_UNKNOWN,
  MONITOR_TYPE_SERVER,
  MONITOR_TYPE_REPL,
} MONITOR_TYPE;

typedef enum
{
  MONITOR_STATS_VALUE_COUNTER,
  MONITOR_STATS_VALUE_COUNTER_WITH_TIME,
  MONITOR_STATS_VALUE_GAUGE,
  MONITOR_STATS_VALUE_EVENT
} MONITOR_STATS_VALUE_TYPE;

typedef struct
{
  const char *name;
  MONITOR_STATS_VALUE_TYPE value_type;
} MONITOR_STATS_INFO;

typedef struct
{
  INT64 value;
  UINT64 acc_time;
} MONITOR_STATS;

typedef struct
{
  MONITOR_TYPE monitor_type;
  int num_stats;

  MONITOR_STATS_INFO info[1];
} MONITOR_STATS_META;

typedef struct
{
  RYE_SHM_HEADER shm_header;

  char name[SHM_NAME_SIZE];

  MONITOR_TYPE monitor_type;
  int num_stats;
  int num_monitors;

  MONITOR_STATS stats[1];
} RYE_MONITOR_SHM;

typedef struct
{
  int shm_key;

  MONITOR_STATS_META *meta;

  RYE_MONITOR_SHM *monitor_shm;
} MONITOR_INFO;


#define MONITOR_SUFFIX "_mnt"


#if defined(X86)
#define MONITOR_GET_CURRENT_TIME(VAR)                                   \
        do {                                                            \
          unsigned int lo, hi;                                          \
          __asm__ __volatile__ ("rdtsc":"=a" (lo), "=d" (hi));          \
          (VAR) = ((UINT64) lo) | (((UINT64) hi) << 32);                \
        } while (0)
#else
#define MONITOR_GET_CURRENT_TIME(VAR)                                   \
        do {                                                            \
          (VAR) = 0;                                                    \
        } while (0)
#endif

#define IS_CUMMULATIVE_VALUE(TYPE)      \
        ((TYPE) == MONITOR_STATS_VALUE_COUNTER || (TYPE) == MONITOR_STATS_VALUE_COUNTER_WITH_TIME)
#define IS_COLLECTING_TIME(TYPE)        \
        ((TYPE) == MONITOR_STATS_VALUE_COUNTER_WITH_TIME)


/* repl monitor items */
typedef enum
{
  MNT_RP_EOF_PAGEID,
  MNT_RP_RECEIVED_GAP,
  MNT_RP_RECEIVED_PAGEID,
  MNT_RP_FLUSHED_GAP,
  MNT_RP_FLUSHED_PAGEID,

  MNT_RP_CURRENT_GAP,
  MNT_RP_CURRENT_PAGEID,
  MNT_RP_REQUIRED_GAP,
  MNT_RP_REQUIRED_PAGEID,
  MNT_RP_APPLIED_TIME,
  MNT_RP_DELAY,
  MNT_RP_QUEUE_FULL,

  MNT_RP_INSERT,
  MNT_RP_UPDATE,
  MNT_RP_DELETE,
  MNT_RP_DDL,
  MNT_RP_COMMIT,
  MNT_RP_FAIL,

  MNT_SIZE_OF_REPL_EXEC_STATS
} MNT_REPL_ITEM;

extern void monitor_make_name (char *monitor_name, const char *name);
extern void monitor_make_repl_name (char *monitor_name, const char *db_name);

/******************************************************************
 * MONITOR COLLECTOR
 ******************************************************************/
extern int monitor_create_collector (const char *name, int num_monitors,
				     MONITOR_TYPE monitor_type);
extern void monitor_final_collector (void);
extern void monitor_stats_counter (int mnt_id, int item, INT64 value);
extern void monitor_stats_counter_with_time (int mnt_id, int item,
					     INT64 value, UINT64 start_time);
extern void monitor_stats_gauge (int mnt_id, int item, INT64 value);
extern INT64 monitor_get_stats_with_time (UINT64 * acc_time, int mnt_id,
					  int item);
extern INT64 monitor_get_stats (int mnt_id, int item);


/******************************************************************
 * MONITOR VIEWER
 ******************************************************************/

extern MONITOR_INFO *monitor_create_viewer_from_name (const char *name);
extern MONITOR_INFO *monitor_create_viewer_from_key (int shm_key);
extern void monitor_final_viewer (MONITOR_INFO * monitor_info);
extern const char *monitor_get_name (MONITOR_INFO * monitor);
extern bool monitor_stats_is_cumulative (MONITOR_INFO * monitor_info,
					 int item);
extern bool monitor_stats_is_collecting_time (MONITOR_INFO * monitor_info,
					      int item);
extern int monitor_copy_stats (MONITOR_INFO * monitor,
			       MONITOR_STATS * to_stats, int mnt_id);
extern int monitor_copy_global_stats (MONITOR_INFO * monitor,
				      MONITOR_STATS * to_stats);
extern void monitor_dump_stats_to_buffer (MONITOR_INFO * monitor,
					  char *buffer, int buf_size,
					  MONITOR_STATS * stats,
					  MONITOR_DUMP_TYPE dump_type,
					  const char *substr);
extern void monitor_dump_stats (FILE * stream, MONITOR_INFO * monitor,
				MONITOR_STATS * cur_stats,
				MONITOR_STATS * old_stats, int cumulative,
				MONITOR_DUMP_TYPE dump_type,
				const char *substr);
extern int monitor_diff_stats (MONITOR_INFO * monitor,
			       MONITOR_STATS * diff_stats,
			       MONITOR_STATS * new_stats,
			       MONITOR_STATS * old_stats);

extern int monitor_open_viewer_data (MONITOR_INFO * monitor);
extern int monitor_close_viewer_data (MONITOR_INFO * monitor);
#endif /* MONITOR_H_ */
