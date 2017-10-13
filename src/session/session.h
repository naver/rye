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
 * session.h - Session state api
 */
#ifndef _SESSION_H_
#define _SESSION_H_

#include "dbtype.h"
#include "thread.h"
#include "query_list.h"
#include "query_manager.h"

extern bool sessions_is_states_table_initialized (void);
extern int session_states_init (THREAD_ENTRY * thread_p);
extern void session_states_finalize (THREAD_ENTRY * thread_p);
extern int session_state_create (THREAD_ENTRY * thread_p, SESSION_KEY * key);
extern int session_state_destroy (THREAD_ENTRY * thread_p,
				  const SESSION_KEY * key);
extern int session_check_session (THREAD_ENTRY * thread_p,
				  const SESSION_KEY * key);
extern int session_set_session_key (THREAD_ENTRY * thread_p,
				    const SESSION_KEY * key);
extern int session_remove_expired_sessions (struct timeval *timeout);
#if !defined(SERVER_MODE)
extern int session_get_session_id (THREAD_ENTRY * thread_p,
				   SESSION_KEY * key);
#endif
#if defined (ENABLE_UNUSED_FUNCTION)
extern int login_user (THREAD_ENTRY * thread_p, const char *username);
#endif
extern void session_states_dump (THREAD_ENTRY * thread_p);
extern void session_store_query_entry_info (THREAD_ENTRY * thread_p,
					    QMGR_QUERY_ENTRY * qentry_p);
extern int session_load_query_entry_info (THREAD_ENTRY * thread_p,
					  QMGR_QUERY_ENTRY * qentry_p);
extern int session_remove_query_entry_info (THREAD_ENTRY * thread_p,
					    const QUERY_ID query_id);
extern int session_clear_query_entry_info (THREAD_ENTRY * thread_p,
					   const QUERY_ID query_id);
extern int session_get_trace_stats (THREAD_ENTRY * thread_p,
				    DB_VALUE * result);
extern int session_set_trace_stats (THREAD_ENTRY * thread_p, char *scan_stats,
				    int format);
extern int session_clear_trace_stats (THREAD_ENTRY * thread_p);
#endif /* _SESSION_H_ */
