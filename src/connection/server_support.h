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
 * server_support.h -
 */

#ifndef _SERVER_SUPPORT_H_
#define _SERVER_SUPPORT_H_

#ident "$Id$"

#include "connection_defs.h"
#include "connection_sr.h"
#include "thread.h"
#include "porting.h"

typedef struct css_job_entry CSS_JOB_ENTRY;
struct css_job_entry
{
  CSS_CONN_ENTRY *conn_entry;	/* conn entry from which we read request */
  CSS_THREAD_FN func;		/* request handling function */
  CSS_THREAD_ARG arg;		/* handling function argument */
  struct css_job_entry *next;
};

#define CSS_JOB_ENTRY_SET(JOB_ENTRY, CONN, FUNC, ARG)	\
	do {						\
	  (JOB_ENTRY).conn_entry = CONN;		\
	  (JOB_ENTRY).func = FUNC;			\
	  (JOB_ENTRY).arg = ARG;			\
	} while (0)
#define CSS_JOB_ENTRY_GET(JOB_ENTRY, CONN, FUNC, ARG)	\
	do {						\
	  CONN = (JOB_ENTRY).conn_entry;		\
	  FUNC = (JOB_ENTRY).func;			\
	  ARG = (JOB_ENTRY).arg;			\
	} while (0)


extern void css_block_all_active_conn (unsigned short stop_phase);
extern void css_wakeup_all_jobq_waiters (void);

extern void *css_oob_handler_thread (void *arg);
extern void *css_master_thread (void);

extern int css_send_reply_to_client (CSS_CONN_ENTRY * conn, unsigned int eid,
				     int num_buffers, ...);
extern unsigned int css_send_abort_to_client (CSS_CONN_ENTRY * conn,
					      unsigned int eid);
extern char *css_pack_server_name (const char *server_name, int *name_length);
extern int css_init (char *server_name, int server_name_length,
		     int connection_id);
extern const char *css_add_client_version_string (THREAD_ENTRY * thread_p,
						  const char *version_string);
extern void css_end_server_request (CSS_CONN_ENTRY * conn);
extern bool css_is_shutdown_timeout_expired (void);

extern bool css_is_ha_repl_delayed (void);
extern void css_set_ha_repl_delayed (void);
extern void css_unset_ha_repl_delayed (void);
extern int css_check_ha_server_state_for_client (THREAD_ENTRY * thread_p,
						 int whence);
extern int css_change_ha_server_state (THREAD_ENTRY * thread_p,
				       HA_STATE req_server_state, bool force);
extern int css_notify_ha_apply_state (THREAD_ENTRY * thread_p,
				      const char *host_ip,
				      HA_APPLY_STATE state);
extern THREAD_RET_T THREAD_CALLING_CONVENTION
css_connection_handler_thread (void *arg_p);

extern void css_epoll_set_check (CSS_CONN_ENTRY * conn, bool check_conn);
extern int css_epoll_del_conn (CSS_CONN_ENTRY * conn);
extern int css_epoll_add_conn (CSS_CONN_ENTRY * conn);

extern int css_incr_num_run_thread (JOB_QUEUE_TYPE q_type);
extern int css_decr_num_run_thread (JOB_QUEUE_TYPE q_type);

extern int css_init_job_queue (void);
extern void css_final_job_queue (void);
extern int css_get_new_job (JOB_QUEUE_TYPE q_type,
			    CSS_JOB_ENTRY * ret_job_entry);
extern int css_add_to_job_queue (JOB_QUEUE_TYPE q_type,
				 CSS_JOB_ENTRY * job_entry);
extern int css_internal_request_handler (THREAD_ENTRY * thread_p,
					 CSS_THREAD_ARG arg);
#if 0
extern void css_job_queue_check (FILE * out_fp);
#endif

#endif /* _SERVER_SUPPORT_H_ */
