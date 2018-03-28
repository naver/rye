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
 * connection_support.h -
 */

#ifndef _CONNECTION_SUPPORT_H_
#define _CONNECTION_SUPPORT_H_

#ident "$Id$"

#include "connection_defs.h"
#include "heartbeat.h"

typedef void (*CSS_SERVER_TIMEOUT_FN) (void);
/* check server alive */
typedef bool (*CSS_CHECK_SERVER_ALIVE_FN) (const char *, const PRM_NODE_INFO *);
typedef bool (*CSS_CHECK_CLIENT_ALIVE_FN) (void);
extern CSS_CHECK_SERVER_ALIVE_FN css_check_server_alive_fn;

extern int css_recv_command_packet (CSS_CONN_ENTRY * conn, CSS_NET_PACKET ** recv_packet);

extern int css_send_data_packet (CSS_CONN_ENTRY * conn, unsigned short rid, int num_buffers, ...);
extern int css_send_data_packet_v (CSS_CONN_ENTRY * conn, unsigned short rid, int num_buffers, va_list args);
#if defined(SERVER_MODE)
extern int css_send_abort_packet (CSS_CONN_ENTRY * conn, unsigned short rid);
#endif
extern int css_send_close_packet (CSS_CONN_ENTRY * conn, unsigned short rid);
extern int css_send_error_packet (CSS_CONN_ENTRY * conn, unsigned short rid, const char *buffer, int buffer_size);
extern int css_send_command_packet (CSS_CONN_ENTRY * conn, int request,
                                    unsigned short *request_id, int num_buffers, ...);
extern int css_send_command_packet_v (CSS_CONN_ENTRY * conn, int request,
                                      unsigned short *request_id, int num_buffers, va_list data_args);
extern int css_net_packet_recv (CSS_NET_PACKET ** recv_packet,
                                CSS_CONN_ENTRY * conn, int timeout, int num_buffers, ...);
extern int css_net_packet_recv_v (CSS_NET_PACKET ** recv_packet,
                                  CSS_CONN_ENTRY * conn, int timeout, int num_buffers, va_list args);
extern void css_net_packet_free (CSS_NET_PACKET * net_packet);
extern char *css_net_packet_get_buffer (CSS_NET_PACKET * net_packet, int index, int expected_size, bool reset_ptr);
extern int css_net_packet_get_recv_size (CSS_NET_PACKET * net_packet, int index);


extern const char *css_ha_mode_string (HA_MODE mode);
extern const char *css_ha_filestat_string (LOG_HA_FILESTAT ha_file_state);


#if !defined (SERVER_MODE)
extern void css_register_server_timeout_fn (CSS_SERVER_TIMEOUT_FN callback_fn);
extern void css_register_check_server_alive_fn (CSS_CHECK_SERVER_ALIVE_FN callback_fn);
extern void css_register_check_client_alive_fn (CSS_CHECK_CLIENT_ALIVE_FN callback_fn);
#endif /* !SERVER_MODE */

extern int css_send_magic (CSS_CONN_ENTRY * conn);
extern int css_check_magic (CSS_CONN_ENTRY * conn);

extern bool css_is_client_ro_tran (THREAD_ENTRY * thread_p);

extern CSS_CONN_ENTRY *css_register_to_master (HB_PROC_TYPE type, const char *server_name, const char *log_path);

#endif /* _CONNECTION_SUPPORT_H_ */
