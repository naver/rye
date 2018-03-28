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
 * critical_section.h - Definitions for critical section interface.
 */

#ifndef _CRITICAL_SECTION_H_
#define _CRITICAL_SECTION_H_

#ident "$Id$"

#include <pthread.h>

#include "thread.h"

enum
{ INF_WAIT = -1,                /* INFINITE WAIT */
  NOT_WAIT = 0                  /* NO WAIT */
};

/*
 * These are the user defined lock definitions. When adding more locks, also
 * add initialization entries in critical_section.c
 */
/* csect sub-info */
typedef enum
{
  CSECT_ER_LOG_FILE = 0,        /* Latch for error msg log file */
  CSECT_ER_MSG_CACHE,           /* Latch for error msg cache */
  CSECT_WFG,                    /* Latch for wait-for-graph */
  CSECT_LOG,                    /* Latch for log manager */
  CSECT_LOG_BUFFER,             /* Latch for log page buffer */
  CSECT_LOG_ARCHIVE,            /* Latch for log archives */
  CSECT_LOCATOR_SR_CLASSNAME_TABLE,     /* Latch for temp classname to classOID entries */
  CSECT_FILE_NEWFILE,           /* Latch related to new file table */
  CSECT_QPROC_QUERY_TABLE,      /* Latch for query manager table */
#if defined (ENABLE_UNUSED_FUNCTION)
  CSECT_QPROC_QFILE_PGCNT,      /* Latch for query file page count */
  CSECT_QPROC_LIST_CACHE,       /* Latch for query result(list file) cache (mht) */
#endif
  CSECT_BOOT_SR_DBPARM,         /* Latch for accessing System Database parameters.
                                 * Used during vol creation */
  CSECT_DISK_REFRESH_GOODVOL,   /* Latch for refreshing good volume cache */
  CSECT_CNV_FMT_LEXER,          /* Latch for value/string format translation lexer */
#if defined (ENABLE_UNUSED_FUNCTION)
  CSECT_TRAN_TABLE,             /* Latch for transaction table */
#endif
  CSECT_CT_OID_TABLE,
#if defined (ENABLE_UNUSED_FUNCTION)
  CSECT_LOG_FLUSH,              /* for 2 flushing (by LFT, by normal thread) */
#endif
  CSECT_HA_SERVER_STATE,        /* Latch for HA server mode change */
  CSECT_SESSION_STATE,          /* Latch for session state table */
  CSECT_ACL,                    /* Latch for accessible IP list table */
  CSECT_EVENT_LOG_FILE,         /* Latch for event log file */
  CSECT_ACCESS_STATUS,          /* Latch for user access status */
  CSECT_TEMPFILE_CACHE,         /* Latch for temp file cache */
  CSECT_CSS_ACTIVE_CONN,        /* Latch for active css active conn */
  CSECT_CSS_FREE_CONN,          /* Latch for free css free conn */
  CSECT_UNKNOWN,
  CSECT_LAST = CSECT_UNKNOWN    /* refer MNT_STATS_CSECT_WAITS_UNKNOWN */
} CSECT_TYPE;

typedef struct css_critical_section
{
  CSECT_TYPE cs_index;
  const char *name;
  pthread_mutex_t lock;         /* read/write monitor lock */
  int rwlock;                   /* >0 = # readers, <0 = writer, 0 = none */
  unsigned int waiting_writers; /* # of waiting writers */
  pthread_cond_t readers_ok;    /* start waiting readers */
  THREAD_ENTRY *waiting_writers_queue;  /* queue of waiting writers */
  THREAD_ENTRY *waiting_promoters_queue;        /* queue of waiting promoters */
  pthread_t owner;              /* CS owner writer */
  int tran_index;               /* transaction id acquiring CS, is debug info */
#if 1                           /* TODO - */
  struct timeval max_wait;
#endif
} CSS_CRITICAL_SECTION;

#define CSS_CRITICAL_SECTION_INITIALIZER \
  { 0, NULL, PTHREAD_MUTEX_INITIALIZER, 0, 0, PTHREAD_COND_INITIALIZER, \
    NULL, NULL, (pthread_t) 0, -1, 0, 0, { 0, 0 }, { 0, 0 } }

extern int csect_initialize (void);
extern void cs_clear_tran_index (int tran_index);
extern int csect_finalize (void);

extern int csect_enter (THREAD_ENTRY * thread_p, int cs_index, int wait_secs);
extern int csect_enter_as_reader (THREAD_ENTRY * thread_p, int cs_index, int wait_secs);
extern int csect_demote (THREAD_ENTRY * thread_p, int cs_index, int wait_secs);
extern int csect_promote (THREAD_ENTRY * thread_p, int cs_index, int wait_secs);
extern int csect_exit (int cs_index);

extern int csect_initialize_critical_section (CSS_CRITICAL_SECTION * cs_ptr);
extern int csect_finalize_critical_section (CSS_CRITICAL_SECTION * cs_ptr);
extern int csect_enter_critical_section (THREAD_ENTRY * thread_p, CSS_CRITICAL_SECTION * cs_ptr, int wait_secs);
extern int csect_enter_critical_section_as_reader (THREAD_ENTRY * thread_p,
                                                   CSS_CRITICAL_SECTION * cs_ptr, int wait_secs);
extern int csect_demote_critical_section (THREAD_ENTRY * thread_p, CSS_CRITICAL_SECTION * cs_ptr, int wait_secs);
extern int csect_promote_critical_section (THREAD_ENTRY * thread_p, CSS_CRITICAL_SECTION * cs_ptr, int wait_secs);
extern int csect_exit_critical_section (CSS_CRITICAL_SECTION * cs_ptr);

extern int csect_check_own (THREAD_ENTRY * thread_p, int cs_index);
extern int csect_check_own_critical_section (THREAD_ENTRY * thread_p, CSS_CRITICAL_SECTION * cs_ptr);

extern void csect_dump_statistics (FILE * fp);
extern const char *csect_get_cs_name (int cs_index);

#if !defined(SERVER_MODE)
#define csect_initialize_critical_section(a)
#define csect_finalize_critical_section(a)
#define csect_enter(a, b, c) NO_ERROR
#define csect_enter_as_reader(a, b, c) NO_ERROR
#define csect_exit(a)
#define csect_enter_critical_section(a,b,c)
#define csect_enter_critical_section_as_reader(a, b, c)
#define csect_exit_critical_section(a)
#define csect_check_own_critical_section(a, b)
#define csect_check_own(a, b) 1
#endif /* !SERVER_MODE */

#endif /* _CRITICAL_SECTION_H_ */
