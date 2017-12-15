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
 * connection_sr.h - Client/Server Connection List Management
 */

#ifndef _CONNECTION_SR_H_
#define _CONNECTION_SR_H_

#ident "$Id$"

#include <pthread.h>

#include "porting.h"
#include "thread.h"
#include "connection_defs.h"
#include "connection_support.h"
#include "error_manager.h"
#include "critical_section.h"
#include "thread.h"

#define IP_BYTE_COUNT 5

typedef struct ip_info IP_INFO;
struct ip_info
{
  unsigned char *address_list;
  int num_list;
};

extern CSS_CONN_ENTRY *css_Conn_array;
extern CSS_CONN_ENTRY *css_Active_conn_anchor;
extern CSS_CONN_ENTRY *css_Listen_conn;

extern int css_Num_access_user;

extern int (*css_Connect_handler) (CSS_CONN_ENTRY *);

extern int css_initialize_conn (CSS_CONN_ENTRY * conn, SOCKET fd);
extern void css_shutdown_conn (CSS_CONN_ENTRY * conn);
extern int css_init_conn_list (void);
extern void css_final_conn_list (void);

extern CSS_CONN_ENTRY *css_make_conn (SOCKET fd);
extern void css_insert_into_active_conn_list (CSS_CONN_ENTRY * conn);

extern int css_increment_num_conn (BOOT_CLIENT_TYPE client_type);
extern void css_decrement_num_conn (BOOT_CLIENT_TYPE client_type);

extern void css_free_conn (CSS_CONN_ENTRY * conn);
extern void css_print_conn_entry_info (CSS_CONN_ENTRY * p);
extern void css_print_conn_list (void);
extern CSS_CONN_ENTRY *css_find_conn_by_tran_index (int tran_index);
extern int css_get_session_ids_for_active_connections (SESSION_ID ** ids,
						       int *count);
extern void css_shutdown_conn_by_tran_index (int tran_index);

extern int css_send_abort_request (CSS_CONN_ENTRY * conn,
				   unsigned short request_id);
extern int css_return_queued_data (CSS_CONN_ENTRY * conn,
				   unsigned int req_id, char **buffer,
				   int *buffer_size, int timeout);

extern unsigned int css_return_eid_from_conn (CSS_CONN_ENTRY * conn,
					      unsigned short rid);

extern unsigned short css_get_request_id (CSS_CONN_ENTRY * conn);
extern int css_set_accessible_ip_info (void);
extern int css_free_accessible_ip_info (void);
extern int css_free_ip_info (IP_INFO * ip_info);
extern int css_read_ip_info (IP_INFO ** out_ip_info, char *filename);
extern int css_check_ip (IP_INFO * ip_info, unsigned char *address);

extern void css_set_user_access_status (const char *db_user,
					const char *host,
					const char *program_name);
#if defined (ENABLE_UNUSED_FUNCTION)
extern void css_get_user_access_status (int num_user,
					LAST_ACCESS_STATUS **
					access_status_array);
#endif
extern void css_free_user_access_status (void);
extern int css_recv_data_packet_from_client (CSS_NET_PACKET ** recv_packet,
					     CSS_CONN_ENTRY * conn, int rid,
					     int timeout, int num_buffers,
					     ...);
extern int css_find_dupliated_conn (int conn_idx);

extern int css_common_connect_sr (CSS_CONN_ENTRY * conn,
				  unsigned short *rid,
				  const PRM_NODE_INFO * node_info,
				  int connect_type,
				  const char *server_name,
				  int server_name_length);

#endif /* _CONNECTION_SR_H_ */
