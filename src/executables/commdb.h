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
 *      commdb.h:
 */

#ifndef _COMMDB_H_
#define _COMMDB_H_

#ident "$Id$"

extern int commdb_master_shutdown (CSS_CONN_ENTRY ** local_conn, int minutes);
extern int commdb_get_server_status (CSS_CONN_ENTRY ** local_conn);
extern int commdb_is_registered_procs (CSS_CONN_ENTRY ** local_conn,
				       bool * success_fail);
extern int commdb_deact_stop_all (CSS_CONN_ENTRY * conn,
				  bool deact_immediately);
extern int commdb_deact_confirm_stop_all (CSS_CONN_ENTRY * conn,
					  bool * success_fail);
extern int commdb_deactivate_heartbeat (CSS_CONN_ENTRY * conn);
extern int commdb_deact_confirm_no_server (CSS_CONN_ENTRY * conn,
					   bool * success_fail);
extern int commdb_activate_heartbeat (CSS_CONN_ENTRY ** local_conn);
extern int commdb_ha_node_info_query (CSS_CONN_ENTRY ** local_conn,
				      bool verbose_yn);
extern int commdb_ha_process_info_query (CSS_CONN_ENTRY ** local_conn,
					 bool verbose_yn);
extern int commdb_ha_ping_host_info_query (CSS_CONN_ENTRY ** local_conn);
extern int commdb_ha_admin_info_query (CSS_CONN_ENTRY ** local_conn);
extern int commdb_reconfig_heartbeat (CSS_CONN_ENTRY ** local_conn);
extern int commdb_changemode (CSS_CONN_ENTRY ** local_conn,
			      HA_STATE req_node_state, bool force);

#endif /* _COMMDB_H_ */
