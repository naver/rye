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
 * rbl_move_group.h -
 */

#ifndef RBL_MOVE_GROUP_H_
#define RBL_MOVE_GROUP_H_

#ident "$Id$"

#include "cas_cci_internal.h"
#include "rbl_sync_log.h"

typedef struct rbl_copy_context RBL_COPY_CONTEXT;
struct rbl_copy_context
{
  int gid;
  int num_skeys;

  CCI_CONN *src_conn;
  CCI_CONN *dest_conn;

  RBL_SYNC_CONTEXT *sync_ctx;

  int last_error;
  bool interrupt;
  bool was_gid_updated;
  bool run_slave;

  int num_copied_keys;
  int num_copied_rows;
  int num_copied_collision;

  struct timeval start_time;
};

extern int rbl_init_copy_context (RBL_COPY_CONTEXT * ctx, int gid,
				  bool run_slave);
extern int rbl_copy_group_data (RBL_COPY_CONTEXT * ctx);
#endif /* RBL_MOVE_GROUP_H_ */
