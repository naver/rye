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
 * repl.h - the header file of repl module
 *
 */

#ifndef _REPL_H_
#define _REPL_H_

#ident "$Id$"

#include "repl_page_buffer.h"

#include "ds_queue.h"

#include "cas_cci.h"

#define CIRP_TRAN_Q_SIZE        (1024)
#define HOST_IP_SIZE            20

typedef enum _cirp_agent_status CIRP_AGENT_STATUS;
enum _cirp_agent_status
{
  CIRP_AGENT_INIT,
  CIRP_AGENT_BUSY,
  CIRP_AGENT_WAIT,
  CIRP_AGENT_DEAD
};

typedef struct _cirp_stats CIRP_STATS;
struct _cirp_stats
{
  unsigned long insert;
  unsigned long update;
  unsigned long delete;
  unsigned long schema;
  unsigned long fail;
  unsigned long commit;
};

typedef enum _cirp_thread_type CIRP_THREAD_TYPE;
enum _cirp_thread_type
{
  CIRP_THREAD_WRITER,
  CIRP_THREAD_FLUSHER,
  CIRP_THREAD_ANALYZER,
  CIRP_THREAD_APPLIER,
  CIRP_THREAD_HEALTH_CHEKER,
  CIRP_THREAD_COPY_ARCHIVE
};

typedef struct
{
  char *log_path;
  char *db_name;
  int mode;
} REPL_ARGUMENT;

typedef struct cirp_thread_entry CIRP_THREAD_ENTRY;
struct cirp_thread_entry
{
  pthread_t tid;
  pthread_mutex_t th_lock;
  CIRP_THREAD_TYPE th_type;
  int applier_index;

  const REPL_ARGUMENT *arg;
};

typedef struct cirp_ct_log_applier CIRP_CT_LOG_APPLIER;
struct cirp_ct_log_applier
{
  char host_ip[HOST_IP_SIZE];
  int id;

  LOG_LSA committed_lsa;	/* last committed commit log lsa */
  INT64 master_last_commit_time;	/* Time in Milli seconds */
  INT64 repl_delay;

  INT64 insert_count;
  INT64 update_count;
  INT64 delete_count;
  INT64 schema_count;
  INT64 commit_count;
  INT64 fail_count;
};

typedef struct cirp_ct_log_analyzer CIRP_CT_LOG_ANALYZER;
struct cirp_ct_log_analyzer
{
  char host_ip[HOST_IP_SIZE];

  LOG_LSA current_lsa;
  LOG_LSA required_lsa;

  INT64 start_time;		/* analyzer start time, Time in Milli seconds */
  INT64 source_applied_time;	/* Time in Milli seconds */
  INT64 creation_time;		/* Time in Milli seconds */

  INT64 queue_full;
};

typedef struct cirp_ct_log_writer CIRP_CT_LOG_WRITER;
struct cirp_ct_log_writer
{
  char host_ip[HOST_IP_SIZE];

  INT64 last_flushed_pageid;
  INT64 last_received_time;	/* Time in Milli seconds */
  LOG_LSA eof_lsa;		/* eof lsa of active log header */
};

typedef struct cirp_writer_info CIRP_WRITER_INFO;
struct cirp_writer_info
{
  pthread_mutex_t lock;

  LOG_PAGE *hdr_page;

  int reader_count;
  bool is_archiving;

  CIRP_AGENT_STATUS writer_status;
  CIRP_AGENT_STATUS copier_status;

  CIRP_CT_LOG_WRITER ct;
};

typedef struct _cirp_q_item CIRP_Q_ITEM;
struct _cirp_q_item
{
  TRANID trid;
  LOG_LSA tran_start_lsa;
  LOG_LSA committed_lsa;
  LOG_LSA repl_start_lsa;
};

typedef struct _cirp_tran_q CIRP_TRAN_Q;
struct _cirp_tran_q
{
  int num_item;
  int front;
  int rear;

  /*
   * boundary pointing a first item that
   * must not be overwritten by push operation.
   */
  int boundary;

  CIRP_Q_ITEM log_item[CIRP_TRAN_Q_SIZE];
};

typedef struct __rp_applied_time_node RP_APPLIED_TIME_NODE;
struct __rp_applied_time_node
{
  LOG_LSA applied_lsa;
  INT64 applied_time;
};

typedef struct _cirp_analyzer_info CIRP_ANALYZER_INFO;
struct _cirp_analyzer_info
{
  pthread_mutex_t lock;

  CIRP_AGENT_STATUS status;

  HA_APPLY_STATE apply_state;

  MHT_TABLE *tran_table;

  /* master info */
  bool last_is_end_of_record;
  bool is_end_of_record;
  int last_node_state;
  int last_ha_file_status;	/* FIXME-notout: check initial value */
  bool is_role_changed;
  RQueue *q_applied_time;

  /* file lock */
  int log_path_lockf_vdes;
  int db_lockf_vdes;

  CIRP_BUF_MGR buf_mgr;
  CCI_CONN conn;

  CIRP_CT_LOG_ANALYZER ct;
};

typedef struct _cirp_applier_info CIRP_APPLIER_INFO;
struct _cirp_applier_info
{
  CIRP_AGENT_STATUS status;
  CIRP_AGENT_STATUS analyzer_status;

  pthread_mutex_t lock;
  pthread_cond_t cond;

  CCI_CONN conn;
  CIRP_CT_LOG_APPLIER ct;

  int num_uncommitted_tran;
  int num_unflushed;
  CIRP_REPL_ITEM *head;
  CIRP_REPL_ITEM *tail;

  /* not cumulative, transaction commit will reset it */
  CIRP_STATS stats;

  /* transaction queue */
  CIRP_TRAN_Q logq;

  /* log page buffer */
  CIRP_BUF_MGR buf_mgr;
};

typedef struct _cirp_repl_info CIRP_REPL_INFO;
struct _cirp_repl_info
{
  time_t start_time;
  INT64 max_mem_size;
  INT64 start_vsize;
  char *broker_key;
  int broker_port;
  int pid;

  CIRP_AGENT_STATUS health_check_status;

  CIRP_ANALYZER_INFO analyzer_info;
  CIRP_WRITER_INFO writer_info;

  int num_applier;
  CIRP_APPLIER_INFO applier_info[1];
};

#define RP_SET_AGENT_NEED_RESTART() (rp_set_agent_need_restart (ARG_FILE_LINE))
#define RP_SET_AGENT_NEED_SHUTDOWN() (rp_set_agent_need_shutdown (ARG_FILE_LINE))

#define REPL_NEED_SHUTDOWN()                                        \
  (rp_need_shutdown (ARG_FILE_LINE) == true || rp_dead_agent_exists () == true)

#define REPL_SET_GENERIC_ERROR(error, ...)                                     \
      do                                                                       \
        {                                                                      \
          char __error_msg[ER_MSG_SIZE];                                       \
          snprintf (__error_msg, sizeof (__error_msg), __VA_ARGS__);           \
          (error) = ER_GENERIC_ERROR;                                          \
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, (error), 1, __error_msg);  \
        }                                                                      \
      while (0)


extern CIRP_REPL_INFO *Repl_Info;

extern int cirp_connect_copylogdb (const char *db_name, bool retry);
extern int cirp_connect_agents (const char *db_name);
extern int rp_disconnect_agents (void);
extern int cirp_get_repl_info_from_catalog (CIRP_ANALYZER_INFO * analyzer);

extern bool rp_dead_agent_exists (void);

extern int rp_start_all_applier (void);
extern int rp_end_all_applier (void);
extern bool rp_check_appliers_status (CIRP_AGENT_STATUS status);
extern void _rp_log_debug (const char *file_name, const int line_no,
			   const char *fmt, ...);

#define REPL_MEM_CHECK_DEBUG
#if defined(REPL_MEM_CHECK_DEBUG) || !defined (NDEBUG)
#define rp_log_debug(...) _rp_log_debug(ARG_FILE_LINE, __VA_ARGS__)
#else
#define rp_log_debug(...)
#endif


#endif /* _REPL_H_ */
