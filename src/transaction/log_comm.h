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

#ifndef _LOG_COMM_H_
#define _LOG_COMM_H_

#ident "$Id$"

#include <stdio.h>
#include "storage_common.h"
#include "dbdef.h"
#include "object_representation.h"
#include "query_list.h"
#include "boot.h"

#define LOG_USERNAME_MAX        (DB_MAX_USER_LENGTH + 1)

#define TRAN_LOCK_INFINITE_WAIT (-1)

#define LOG_ONELOG_PACKED_SIZE (OR_INT_SIZE * 3)

#define LOG_MANYLOGS_PTR_IN_LOGAREA(log_areaptr)                         \
  ((struct manylogs *) ((char *)log_areaptr->mem + log_areaptr->length - \
                                  DB_SIZEOF(struct manylogs)))

#define TIME_SIZE_OF_DUMP_LOG_INFO 32

/*
 * STATES OF TRANSACTIONS
 */
typedef enum
{
  TRAN_RECOVERY,                /* State of a system
                                 *  transaction which is
                                 *  used for recovery
                                 *  purposes. For example
                                 *  , set lock for
                                 *  damaged pages.
                                 */
  TRAN_ACTIVE,                  /* Active transaction */
  TRAN_UNACTIVE_COMMITTED,      /* Transaction is in
                                   the commit process
                                   or has been comitted
                                 */
  TRAN_UNACTIVE_WILL_COMMIT,    /* Transaction will be
                                   committed
                                 */
  TRAN_UNACTIVE_COMMITTED_WITH_POSTPONE,        /* Transaction has been
                                                   committed, but it is
                                                   still executing
                                                   postpone operations
                                                 */
  TRAN_UNACTIVE_TOPOPE_COMMITTED_WITH_POSTPONE, /* In the process of
                                                   executing postpone
                                                   top system operations
                                                 */
  TRAN_UNACTIVE_ABORTED,        /* Transaction is in the
                                   abort process or has
                                   been aborted
                                 */
  TRAN_UNACTIVE_UNILATERALLY_ABORTED,   /* Transaction was
                                           active a the time of
                                           a system crash. The
                                           transaction is
                                           unilaterally aborted
                                           by the system
                                         */
  TRAN_UNACTIVE_UNKNOWN         /* Unknown state. */
} TRAN_STATE;

typedef enum
{
  TRAN_TYPE_UNKNOWN,
  TRAN_TYPE_DML,
  TRAN_TYPE_READ_ONLY,
  TRAN_TYPE_DDL
} TRAN_TYPE;

/*
 * RESULT OF NESTED TOP OPERATION
 */

typedef enum
{
  LOG_RESULT_TOPOP_COMMIT,
  LOG_RESULT_TOPOP_ABORT,
  LOG_RESULT_TOPOP_ATTACH_TO_OUTER
} LOG_RESULT_TOPOP;

/* name used by the internal modules */
typedef DB_TRAN_ISOLATION TRAN_ISOLATION;
typedef int LOG_RCVCLIENT_INDEX;        /* Index to recovery functions */

typedef struct log_copy LOG_COPY;
struct log_copy
{                               /* Copy area for flushing and fetching */
  char *mem;                    /* Pointer to location of chunk of area */
  int length;                   /* The size of the area                 */
};

struct onelog
{                               /* One data record to recovery */
  LOG_RCVCLIENT_INDEX rcvindex; /* Recovery function to apply */
  int length;                   /* Length of the data         */
  int offset;                   /* Offset to data             */
};

struct manylogs
{
  struct onelog onelog;
  int num_logs;                 /* How many log recovery records are described */
};

typedef struct tran_query_exec_info TRAN_QUERY_EXEC_INFO;
struct tran_query_exec_info
{
  char *wait_for_tran_index_string;
  float query_time;
  float tran_time;
  char *query_stmt;
  char *sql_id;
  XASL_ID xasl_id;
};

extern const char *log_state_string (TRAN_STATE state);
extern const char *log_isolation_string (TRAN_ISOLATION isolation);
#if defined (ENABLE_UNUSED_FUNCTION)
extern LOG_COPY *log_alloc_client_copy_area (int length);
extern void log_free_client_copy_area (LOG_COPY * copy_area);
extern char *log_pack_descriptors (int num_records, LOG_COPY * log_area, char *descriptors);
extern char *log_unpack_descriptors (int num_records, LOG_COPY * log_area, char *descriptors);
extern int
log_copy_area_send (LOG_COPY * log_area, char **contents_ptr,
                    int *contents_length, char **descriptors_ptr, int *descriptors_length);
extern LOG_COPY *log_copy_area_malloc_recv (int num_records,
                                            char **packed_descriptors,
                                            int packed_descriptors_length, char **contents_ptr, int contents_length);
#endif
extern int log_dump_log_info (const char *logname_info, bool also_stdout, const char *fmt, ...);
extern bool log_does_allow_replication (void);

#endif /* _LOG_COMM_H_ */
