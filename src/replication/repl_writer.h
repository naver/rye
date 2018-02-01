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
 * repl_writer.h -
 */

#ifndef _REPL_WRITER_H_
#define _REPL_WRITER_H_

#ident "$Id$"


#include <stdio.h>

#include "ds_queue.h"

typedef struct logwr_context LOGWR_CONTEXT;
struct logwr_context
{
  int rc;
  int last_error;
  bool shutdown;
};

typedef enum cirpwr_action CIRPWR_ACTION;
enum cirpwr_action
{
  CIRPWR_ACTION_NONE = 0x00,
  CIRPWR_ACTION_ARCHIVING = 0x01,
  CIRPWR_ACTION_FORCE_FLUSH = 0x02,
};

#define HB_RECV_Q_MAX_COUNT 250

typedef struct cirpwr_queue_node RECV_Q_NODE;
struct cirpwr_queue_node
{
  int server_status;

  int area_length;
  int length;
  INT64 fpageid;
  int num_page;
  char *data;
};

typedef struct cirp_logwr_global CIRP_LOGWR_GLOBAL;
struct cirp_logwr_global
{
  COPY_LOG_HEADER ha_info;
  LOG_PAGE *loghdr_pgptr;

  char db_name[PATH_MAX];
  PRM_NODE_INFO host_info;
  char log_path[PATH_MAX];
  char loginf_path[PATH_MAX];
  char active_name[PATH_MAX];
  int append_vdes;

  LOG_PAGEID last_received_pageid;
  int last_received_file_status;
  pthread_mutex_t recv_q_lock;
  pthread_cond_t recv_q_cond;
  RQueue *recv_log_queue;
  RQueue *free_list;

  LOG_ZIP *unzip_area;

  char *logpg_area;
  int logpg_area_size;
  int logpg_fill_size;

  LOG_PAGE **toflush;
  int max_toflush;
  int num_toflush;

  CIRPWR_ACTION action;

  LOG_PAGEID last_arv_lpageid;

  /* background log archiving info */
  BACKGROUND_ARCHIVING_INFO bg_archive_info;
  char bg_archive_name[PATH_MAX];
};

extern int cirpwr_create_active_log (CCI_CONN * conn);
extern int cirpwr_init_copy_log_info (void);

extern int cirp_init_writer (CIRP_WRITER_INFO * writer);
extern int cirp_final_writer (CIRP_WRITER_INFO * writer);
extern int cirpwr_initialize (const char *db_name, const char *log_path);
extern int cirpwr_read_active_log_info (void);
extern void cirpwr_finalize (void);

extern int cirpwr_write_log_pages (void);

extern void *log_copier_main (void *arg);
extern void *log_writer_main (void *arg);

extern bool rpwr_recv_queue_is_empty (void);
extern CIRP_AGENT_STATUS cirpwr_get_copier_status (CIRP_WRITER_INFO *
						   writer_info);
extern CIRP_AGENT_STATUS cirpwr_get_writer_status (CIRP_WRITER_INFO *
						   writer_info);

#endif /* _REPL_WRITER_H_ */
