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
 * rbl_conf.h -
 */

#ifndef RBL_CONF_H_
#define RBL_CONF_H_

#ident "$Id$"

#include "cas_cci_internal.h"
#include "rbl_move_group.h"

#define MAX_DBNAME_SIZE (64)
#define PROG_NAME  "rye_migrator"

#define RBL_MASTER     0
#define RBL_SLAVE      1

#define RBL_COPY       0
#define RBL_SYNC       1

extern int rbl_conf_init (const char *mgmt_host, int mgmt_port,
			  const char *mgmt_dbname,
			  int src_node_id, int dest_node_id, int group_id,
			  const char *dest_host, int dest_port,
			  const char *dest_dbname, int src_node_ha_staus,
			  bool copy_schema);
extern void rbl_conf_final (void);
extern CCI_CONN *rbl_conf_get_mgmt_conn (void);
extern CCI_CONN *rbl_conf_get_srcdb_conn (void);
extern CCI_CONN *rbl_conf_get_destdb_conn (int index);
extern int rbl_conf_update_src_groupid (RBL_COPY_CONTEXT * ctx, bool on_off,
					bool do_commit);
extern int rbl_conf_update_dest_groupid (RBL_COPY_CONTEXT * ctx);
extern int rbl_conf_check_repl_delay (CCI_CONN * conn);
extern int rbl_conf_insert_gid_removed_info_srcdb (int group_id,
						   bool run_slave);
#endif /* RBL_CONF_H_ */
