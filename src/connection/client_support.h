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
 * client_support.h -
 */

#ifndef _CLIENT_SUPPORT_H_
#define _CLIENT_SUPPORT_H_

#ident "$Id$"

#include "connection_defs.h"

extern int css_Errno;
extern CSS_MAP_ENTRY *css_Client_anchor;

extern int css_client_init (const char *server_name, const char *host_name);
extern int css_send_error_to_server (char *host, unsigned int eid,
				     char *buffer, int buffer_size);
extern int css_send_data_to_server_v (char *host, unsigned short rid,
				      int num_buffers, va_list args);

extern void css_terminate (bool server_error);

extern unsigned int css_send_request_to_server_v (char *host, int request,
						  int num_buffers,
						  va_list args);
extern int css_recv_data_from_server (CSS_NET_PACKET ** recv_packet,
				      CSS_CONN_ENTRY * conn, int rid,
				      int timeout, int num_buffers, ...);
extern int css_recv_data_from_server_v (CSS_NET_PACKET ** recv_packet,
					CSS_CONN_ENTRY * conn, int rid,
					int timeout,
					int num_buffers, va_list args);
extern int css_recv_error_from_server (CSS_CONN_ENTRY * conn, int rid,
				       char **error_area, int *error_length,
				       int timeout);

#endif /* _CLIENT_SUPPORT_H_ */
