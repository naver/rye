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
 * repl_log.h - the header file of replication module
 *
 */

#ifndef _REPL_LOG_H_
#define _REPL_LOG_H_

#ident "$Id$"

#include "config.h"

#include "system_parameter.h"
#include "oid.h"
#include "log_impl.h"
#include "memory_alloc.h"
#include "page_buffer.h"
#include "error_manager.h"
#include "thread.h"

typedef enum
{
  REPL_INFO_TYPE_SCHEMA,
  REPL_INFO_TYPE_STMT_NORMAL,
} REPL_INFO_TYPE;

enum
{
  REPL_UNKNOWN_DDL,
  REPL_NON_BLOCKED_DDL,
  REPL_BLOCKED_DDL
};

typedef struct repl_info REPL_INFO;
struct repl_info
{
  int repl_info_type;
  char *info;
};

typedef struct repl_info_schema REPL_INFO_SCHEMA;
struct repl_info_schema
{
  int statement_type;
  int online_ddl_type;
  const char *name;
  const char *ddl;
  const char *db_user;
};

/*
 * STATES OF TRANSACTIONS
 */

#if defined(SERVER_MODE) || defined(SA_MODE)
/* for replication, declare replication log dump function */
extern void log_repl_data_dump (FILE * out_fp, UNUSED_ARG int length, void *data);
extern void log_repl_schema_dump (FILE * out_fp, UNUSED_ARG int length, void *data);
extern void repl_log_send (void);
extern int repl_add_update_lsa (THREAD_ENTRY * thread_p, const OID * inst_oid);
extern int repl_log_insert (THREAD_ENTRY * thread_p, const OID * class_oid,
                            const OID * inst_oid, LOG_RECTYPE log_type, LOG_RCVINDEX rcvindex, DB_IDXKEY * key);
extern int repl_log_insert_schema (THREAD_ENTRY * thread_p, REPL_INFO_SCHEMA * repl_schema);
extern int repl_log_abort_after_lsa (LOG_TDES * tdes, LOG_LSA * start_lsa);
#endif /* SERVER_MODE || SA_MODE */

#endif /* _REPL_LOG_H_ */
