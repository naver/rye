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
 * rbl_sync_log.h -
 */

#ifndef RBL_SYNC_LOG_H_
#define RBL_SYNC_LOG_H_

#ident "$Id$"

#include <time.h>

#include "log_impl.h"
#include "log_compress.h"

typedef struct rbl_sync_context RBL_SYNC_CONTEXT;
struct rbl_sync_context
{
  int migrator_id;
  int gid;

  LOG_ZIP *unzip_area;

  char *logpg_area;
  int logpg_area_size;
  int logpg_fill_size;
  LOG_PAGEID last_recv_pageid;

  LOG_ZIP *undo_unzip;
  LOG_ZIP *redo_unzip;

  LOG_PAGE **log_pages;
  int max_log_pages;
  int num_log_pages;

  LOG_LSA final_lsa;
  LOG_PAGEID cur_pageid;
  int cur_page_index;
  LOG_PAGE *cur_page;
  PGLENGTH cur_offset;

  bool shutdown;

  int num_synced_rows;
  int num_synced_collision;
  LOG_LSA synced_lsa;
  LOG_LSA server_lsa;

  time_t start_time;
  int total_log_pages;
  int delay;
};

extern int rbl_sync_log_init (RBL_SYNC_CONTEXT * ctx, int gid);
extern void rbl_sync_log_final (RBL_SYNC_CONTEXT * ctx);
extern int rbl_sync_log (RBL_SYNC_CONTEXT * ctx);
extern int rbl_sync_check_delay (RBL_SYNC_CONTEXT * ctx);
#endif /* RBL_SYNC_LOG_H_ */
