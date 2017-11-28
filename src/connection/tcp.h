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
 * tcp.h -
 */

#ifndef _TCP_H_
#define _TCP_H_

#ident "$Id$"

#include "config.h"
#include <sys/socket.h>
#include <netinet/in.h>

#define LOCALHOST	"127.0.0.1"

extern void css_get_master_domain_path (char *path_buf, int buf_len);
extern void css_get_server_domain_path (char *path_buf, int buf_len,
					const char *dbname);

extern int css_tcp_master_open (SOCKET * sockfd);
extern bool css_tcp_setup_server_datagram (char *pathname, SOCKET * sockfd);
extern bool css_tcp_master_datagram (char *pathname, SOCKET * sockfd);
extern SOCKET css_master_accept (SOCKET sockfd);
extern SOCKET css_open_new_socket_from_master (SOCKET fd,
					       unsigned short *rid);
extern bool css_transfer_fd (SOCKET server_fd, SOCKET client_fd,
			     unsigned short rid);
extern void css_shutdown_socket (SOCKET fd);
extern SOCKET css_server_accept (SOCKET sockfd);
extern int css_get_max_socket_fds (void);

extern int css_tcp_client_open (const char *host, int connect_type,
				const char *dbname, int timeout);
extern int css_ping (SOCKET sd, struct sockaddr_in *sa_send, int timeout);
extern bool css_peer_alive (SOCKET sd, int timeout);
extern in_addr_t css_host_ip_addr (void);

extern int css_get_peer_name (SOCKET sockfd, char *hostname, size_t len);
#endif /* _TCP_H_ */
