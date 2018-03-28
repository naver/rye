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
 * session_sr.c - Session management on the server
 */

#include "xserver_interface.h"
#include "session.h"

/*
 *  xsession_create_new () - create a new session
 *  return		: error code
 *  session_id (in/out) : session id
 * Note: this function allocates a new session id and creates a session for
 * it
 */
int
xsession_create_new (THREAD_ENTRY * thread_p, SESSION_KEY * key)
{
  assert (key != NULL);

  return session_state_create (thread_p, key);
}

/*
 *  xsession_check_session  () - validates the session with session_id
 *  return	    : error code
 *  session_id (in) : session id
 * Note: this function checks if the session with session_id is still active
 * and updates the last access timeout for it
 */
int
xsession_check_session (THREAD_ENTRY * thread_p, const SESSION_KEY * key)
{
  return session_check_session (thread_p, key);
}

/*
 *  xsession_set_session_key () -
 *  return          : error code
 *  session_key (in) : session identifier
 */
int
xsession_set_session_key (THREAD_ENTRY * thread_p, const SESSION_KEY * key)
{
  return session_set_session_key (thread_p, key);
}

/*
 *  xsession_end_session () - end the session with session_id
 *  return	    : error code
 *  session_id (in) : session id
 *  thread_p (in)
 */
int
xsession_end_session (THREAD_ENTRY * thread_p, const SESSION_KEY * key)
{
  return session_state_destroy (thread_p, key);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * xlogin_user () - login user
 * return : error code or NO_ERROR
 * thread_p (in)  :
 * username (in)  : name of the prepared statement
 */
int
xlogin_user (THREAD_ENTRY * thread_p, const char *username)
{
  return login_user (thread_p, username);
}
#endif

/*
 * xsession_store_query_entry_info () - create a query entry
 * return : void
 * thread_p (in) :
 * qentry_p (in) : query entry
 */
void
xsession_store_query_entry_info (THREAD_ENTRY * thread_p, QMGR_QUERY_ENTRY * qentry_p)
{
  session_store_query_entry_info (thread_p, qentry_p);
}

/*
 * xsession_load_query_entry_info () - search for a query entry
 * return : error code or NO_ERROR
 * thread_p (in) :
 * qentry_p (in/out) : query entry
 */
int
xsession_load_query_entry_info (THREAD_ENTRY * thread_p, QMGR_QUERY_ENTRY * qentry_p)
{
  return session_load_query_entry_info (thread_p, qentry_p);
}

/*
 * xsession_remove_query_entry_info () - remove a query entry from the
 *					 holdable queries list
 * return : error code or NO_ERROR
 * thread_p (in) : active thread
 * query_id (in) : query id
 */
int
xsession_remove_query_entry_info (THREAD_ENTRY * thread_p, const QUERY_ID query_id)
{
  return session_remove_query_entry_info (thread_p, query_id);
}

/*
 * xsession_remove_query_entry_info () - remove a query entry from the
 *					 holdable queries list but do not
 *					 close the associated list files
 * return : error code or NO_ERROR
 * thread_p (in) : active thread
 * query_id (in) : query id
 */
int
xsession_clear_query_entry_info (THREAD_ENTRY * thread_p, const QUERY_ID query_id)
{
  return session_clear_query_entry_info (thread_p, query_id);
}
