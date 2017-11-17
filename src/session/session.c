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
 * session.c - session state internal API
 */

#ident "$Id$"


#include <assert.h>

#include <sys/time.h>
#include <sys/resource.h>
#include "jansson.h"

#include "porting.h"
#include "critical_section.h"
#include "memory_hash.h"
#include "error_manager.h"
#include "system_parameter.h"
#include "db.h"
#include "query_executor.h"
#include "session.h"
#include "environment_variable.h"
#if defined(SERVER_MODE)
#include "connection_sr.h"
#endif
#include "xserver_interface.h"

/* this must be the last header file included!!! */
#include "dbval.h"

#define SESSIONS_HASH_SIZE 1000

typedef struct active_sessions
{
  MHT_TABLE *sessions_table;
  SESSION_ID last_sesson_id;
  int num_holdable_cursors;
} ACTIVE_SESSIONS;

typedef struct session_timeout_info SESSION_TIMEOUT_INFO;
struct session_timeout_info
{
  SESSION_ID *session_ids;
  int count;
  struct timeval *timeout;
};

typedef struct session_query_entry SESSION_QUERY_ENTRY;
struct session_query_entry
{
  QUERY_ID query_id;		/* unique query identifier */
  QFILE_LIST_ID *list_id;	/* result list file identifier */
  QMGR_TEMP_FILE *qe_temp_vfid;	/* temp files */
  int qe_num_temp;		/* number of temp files allocated */
  int total_count;		/* total number of file pages allocated
				 * for the entire query */
  QUERY_FLAG query_flag;

  SESSION_QUERY_ENTRY *next;
};

typedef struct session_state
{
  SESSION_ID session_id;
  SESSION_QUERY_ENTRY *queries;
  struct timeval session_timeout;
  SOCKET related_socket;
  char *trace_stats;
  int trace_format;
  bool mark_deleted;
} SESSION_STATE;

/* the active sessions storage */
static ACTIVE_SESSIONS sessions = { NULL, 0, 0 };

static unsigned int sessions_hash (const void *key,
				   unsigned int hash_table_size);

static int session_free_session (const void *key, void *data, void *args);

#if defined (ENABLE_UNUSED_FUNCTION)
static int session_print_active_sessions (const void *key, void *data,
					  void *args);
#endif
static int session_check_expired (const void *key, void *data, void *args);

static int session_dump_session (const void *key, void *data, void *args);

static SESSION_QUERY_ENTRY *qentry_to_sentry (QMGR_QUERY_ENTRY * qentry_p);
static int session_preserve_temporary_files (THREAD_ENTRY * thread_p,
					     SESSION_QUERY_ENTRY * q_entry);
static void sentry_to_qentry (const SESSION_QUERY_ENTRY * sentry_p,
			      QMGR_QUERY_ENTRY * qentry_p);
static void session_free_sentry_data (THREAD_ENTRY * thread_p,
				      SESSION_QUERY_ENTRY * sentry_p);
static void session_set_conn_entry_data (THREAD_ENTRY * thread_p,
					 SESSION_STATE * session_p);
static SESSION_STATE *session_get_session_state (THREAD_ENTRY * thread_p);

/*
 * session_hash () - hashing function for the session hash
 *   return: int
 *   key(in): Session key
 *   htsize(in): Memory Hash Table Size
 *
 * Note: Generate a hash number for the given key for the given hash table
 *	 size.
 */
static unsigned int
sessions_hash (const void *key, unsigned int hash_table_size)
{
  const unsigned int *session_id = (const unsigned int *) key;

  return ((*session_id) % hash_table_size);
}

/*
 * sessions_compare () - Compare two session keys
 *   return: int (true or false)
 *   key_left  (in) : First session key
 *   key_right (in) : Second session key
 */
static int
sessions_compare (const void *key_left, const void *key_right)
{
  const unsigned int *key1, *key2;

  key1 = (SESSION_ID *) key_left;
  key2 = (SESSION_ID *) key_right;

  return (*key1) == (*key2);
}

/*
 * sessions_is_states_table_initialized () - check to see if session states
 *					     memory area is initialized
 *   return: true if initialized, false otherwise
 *
 * Note: this function should only be called after entering the critical
 * section used by the session state module
 */
bool
sessions_is_states_table_initialized (void)
{
  return (sessions.sessions_table != NULL);
}

/*
 * session_states_init () - Initialize session states area
 *   return: NO_ERROR or error code
 *
 * Note: Creates and initializes a main memory hash table that will be
 * used by session states operations. This routine should only be
 * called once during server boot.
 */
int
session_states_init (UNUSED_ARG THREAD_ENTRY * thread_p)
{
  sessions.last_sesson_id = 0;
  sessions.num_holdable_cursors = 0;

  er_log_debug (ARG_FILE_LINE, "creating session states table\n");

  if (csect_enter (thread_p, CSECT_SESSION_STATE, INF_WAIT) != NO_ERROR)
    {
      return ER_FAILED;
    }

  if (sessions_is_states_table_initialized ())
    {
      csect_exit (CSECT_SESSION_STATE);
      return NO_ERROR;
    }

  sessions.sessions_table = mht_create ("Sessions_State_Table",
					SESSIONS_HASH_SIZE, sessions_hash,
					sessions_compare);
  if (sessions.sessions_table == NULL)
    {
      csect_exit (CSECT_SESSION_STATE);
      return ER_FAILED;
    }

  csect_exit (CSECT_SESSION_STATE);

  return NO_ERROR;
}

/*
 * session_states_finalize () - cleanup the session states information
 *   return: NO_ERROR or error code
 *   thread_p (in) : the thread executing this function
 *
 * Note: This function deletes the session states global storage area.
 *	 This function should be called only during server shutdown
 */
void
session_states_finalize (THREAD_ENTRY * thread_p)
{
  const char *env_value;

  env_value = envvar_get ("DUMP_SESSION");
  if (env_value != NULL)
    {
      session_states_dump (thread_p);
    }

#if 0
  er_log_debug (ARG_FILE_LINE, "deleting session state table\n");
#endif

  if (csect_enter (thread_p, CSECT_SESSION_STATE, INF_WAIT) != NO_ERROR)
    {
      return;
    }

  if (sessions.sessions_table != NULL)
    {
      (void) mht_map (sessions.sessions_table, session_free_session, NULL);
      mht_destroy (sessions.sessions_table);
      sessions.sessions_table = NULL;
    }

  csect_exit (CSECT_SESSION_STATE);
}

/*
 * session_state_create () - Create a sessions state with the specified id
 *   return: NO_ERROR or error code
 *   session_id (in) : the session id
 *
 * Note: This function creates and adds a sessions state object to the
 *       sessions state memory hash. This function should be called when a
 *	 session starts.
 */
int
session_state_create (THREAD_ENTRY * thread_p, SESSION_KEY * key)
{
  static bool overflow = false;
  SESSION_STATE *session_p = NULL;
  SESSION_ID *session_key = NULL;

  session_p = (SESSION_STATE *) malloc (sizeof (SESSION_STATE));
  if (session_p == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, sizeof (SESSION_STATE));
      goto error_return;
    }

  session_p->session_id = DB_EMPTY_SESSION;
  session_p->queries = NULL;
  session_p->related_socket = INVALID_SOCKET;
  session_p->trace_stats = NULL;
  session_p->trace_format = QUERY_TRACE_TEXT;
  session_p->mark_deleted = false;

  /* initialize the timeout */
  if (gettimeofday (&(session_p->session_timeout), NULL) != 0)
    {
      goto error_return;
    }

  session_key = (SESSION_ID *) malloc (sizeof (SESSION_ID));
  if (session_key == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, sizeof (SESSION_ID));
      goto error_return;
    }

  /* add this session_p state to the session_p states table */
  if (csect_enter (thread_p, CSECT_SESSION_STATE, INF_WAIT) != NO_ERROR)
    {
      goto error_return;
    }

  if (sessions.last_sesson_id >= UINT_MAX - 1)
    {
      /* we really should do something else here */
      sessions.last_sesson_id = 0;
      overflow = true;
    }

  if (overflow)
    {
      do
	{
	  sessions.last_sesson_id++;
	  key->id = sessions.last_sesson_id;
	}
      while (mht_get (sessions.sessions_table, &key->id) != NULL);
    }
  else
    {
      sessions.last_sesson_id++;
      key->id = sessions.last_sesson_id;
    }

  session_p->session_id = key->id;
  *session_key = key->id;

  er_log_debug (ARG_FILE_LINE, "adding session with id %u\n", key->id);

  (void) mht_put (sessions.sessions_table, session_key, session_p);

#if 0
  if (prm_get_bool_value (PRM_ID_ER_LOG_DEBUG) == true)
    {
      er_log_debug (ARG_FILE_LINE, "printing active sessions\n");
      mht_map (sessions.sessions_table, session_print_active_sessions, NULL);
      er_log_debug (ARG_FILE_LINE, "finished printing active sessions\n");
    }
#endif

  /* need to do this under the critical section because we cannot rely on the
   * session_p pointer after we left it */
  session_set_conn_entry_data (thread_p, session_p);
  csect_exit (CSECT_SESSION_STATE);

  return NO_ERROR;

error_return:
  if (session_p != NULL)
    {
      free_and_init (session_p);
    }

  if (session_key != NULL)
    {
      free_and_init (session_key);
    }

  return ER_FAILED;
}

/*
 * session_free_session () - Free the memory associated with a session state
 *   return  : NO_ERROR or error code
 *   key(in) : the key from the MHT_TABLE for this session
 *   data(in): session state data
 *   args(in): not used
 *
 * Note: This function is used with the MHT_TABLE routines to free an entry in
 * the table
 */
static int
session_free_session (const void *key, void *data, UNUSED_ARG void *args)
{
  SESSION_STATE *session = (SESSION_STATE *) data;
  SESSION_ID *sess_key = (SESSION_ID *) key;
  THREAD_ENTRY *thread_p = thread_get_thread_entry_info ();
  SESSION_QUERY_ENTRY *qcurent = NULL, *qnext = NULL;
  int cnt = 0;

  if (session == NULL)
    {
      if (sess_key != NULL)
	{
	  free_and_init (sess_key);
	}
      return NO_ERROR;
    }

#if 0
  er_log_debug (ARG_FILE_LINE, "session_free_session %u\n",
		session->session_id);
#endif

  /* free session key */
  if (sess_key != NULL)
    {
      free_and_init (sess_key);
    }

  /* free holdable queries */
  qcurent = session->queries;
  while (qcurent)
    {
      qnext = qcurent->next;
      qcurent->next = NULL;
      session_free_sentry_data (thread_p, qcurent);
      free_and_init (qcurent);
      qcurent = qnext;
      cnt++;
    }

#if 0
  er_log_debug (ARG_FILE_LINE,
		"session_free_session closed %d queries for %d\n", cnt,
		session->session_id);
#endif

  if (session->trace_stats != NULL)
    {
      free_and_init (session->trace_stats);
    }

  free_and_init (session);

  return NO_ERROR;
}

/*
 * session_state_destroy () - close a session state
 *   return	    : NO_ERROR or error code
 *   session_id(in) : the identifier for the session
 */
int
session_state_destroy (UNUSED_ARG THREAD_ENTRY * thread_p,
		       const SESSION_KEY * key)
{
  SESSION_STATE *session_p;
  int error = NO_ERROR;

  er_log_debug (ARG_FILE_LINE, "removing session %u", key->id);

  if (csect_enter_as_reader (thread_p, CSECT_SESSION_STATE, INF_WAIT) !=
      NO_ERROR)
    {
      return ER_FAILED;
    }

  session_p = (SESSION_STATE *) mht_get (sessions.sessions_table, &key->id);
  if (session_p == NULL || session_p->related_socket != key->fd)
    {
      csect_exit (CSECT_SESSION_STATE);
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_SES_SESSION_EXPIRED, 0);
      return ER_SES_SESSION_EXPIRED;
    }

#if defined(SERVER_MODE)
  /* This entry will be removed by session_control thread */
  session_p->mark_deleted = true;

  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  if (thread_p != NULL && thread_p->conn_entry != NULL)
    {
      thread_p->conn_entry->session_id = DB_EMPTY_SESSION;

      if (thread_p->conn_entry->session_p != NULL)
	{
	  thread_p->conn_entry->session_p = NULL;
	}
    }
#else
  error = mht_rem (sessions.sessions_table, &key->id,
		   session_free_session, NULL);
#endif

  csect_exit (CSECT_SESSION_STATE);

  return error;
}

#if defined (ENABLE_UNUSED_FUNCTION)
static int
session_print_active_sessions (const void *key, void *data, void *args)
{
  SESSION_STATE *session_p = (SESSION_STATE *) data;

  er_log_debug (ARG_FILE_LINE, "session %u", session_p->session_id);

  return NO_ERROR;
}
#endif

/*
 * session_check_session () - check if the session state with id session_id
 *			      exists and update the timeout for it
 *   return	    : NO_ERROR or error code
 *   session_id(in) : the identifier for the session
 */
int
session_check_session (THREAD_ENTRY * thread_p, const SESSION_KEY * key)
{
  SESSION_STATE *session_p = NULL;
  int error = NO_ERROR;

  er_log_debug (ARG_FILE_LINE, "updating timeout for session_id %u\n",
		key->id);

  if (csect_enter (thread_p, CSECT_SESSION_STATE, INF_WAIT) != NO_ERROR)
    {
      return ER_FAILED;
    }

  session_p = (SESSION_STATE *) mht_get (sessions.sessions_table, &key->id);
  if (session_p == NULL)
    {
      csect_exit (CSECT_SESSION_STATE);
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_SES_SESSION_EXPIRED, 0);
      return ER_SES_SESSION_EXPIRED;
    }

  /* update the timeout */
  if (gettimeofday (&(session_p->session_timeout), NULL) != 0)
    {
      csect_exit (CSECT_SESSION_STATE);
      return ER_FAILED;
    }
  /* need to do this under the critical section because we cannot rely on the
   * session_p pointer after we left it */
  session_set_conn_entry_data (thread_p, session_p);
  csect_exit (CSECT_SESSION_STATE);
  return error;
}

/*
 * session_set_session_key () -
 *   return          : NO_ERROR or error code
 *   session_key(in) : the identifier for the session
 */
int
session_set_session_key (UNUSED_ARG THREAD_ENTRY * thread_p,
			 const SESSION_KEY * key)
{
  SESSION_STATE *session_p = NULL;
  int error = NO_ERROR;

  er_log_debug (ARG_FILE_LINE, "set related socket for session_id %u\n",
		key->id);

  if (csect_enter (thread_p, CSECT_SESSION_STATE, INF_WAIT) != NO_ERROR)
    {
      return ER_FAILED;
    }

  session_p = (SESSION_STATE *) mht_get (sessions.sessions_table, &key->id);
  if (session_p == NULL)
    {
      csect_exit (CSECT_SESSION_STATE);
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_SES_SESSION_EXPIRED, 0);
      return ER_SES_SESSION_EXPIRED;
    }

  session_p->related_socket = key->fd;

  csect_exit (CSECT_SESSION_STATE);

  return error;
}

/*
 * session_remove_expired_sessions () - remove expired sessions
 *   return      : NO_ERROR or error code
 *   timeout(in) :
 */
int
session_remove_expired_sessions (struct timeval *timeout)
{
  int err = NO_ERROR;
  SESSION_TIMEOUT_INFO timeout_info;
  timeout_info.count = -1;
  timeout_info.session_ids = NULL;
  timeout_info.timeout = timeout;

  if (csect_enter (NULL, CSECT_SESSION_STATE, INF_WAIT) != NO_ERROR)
    {
      return ER_FAILED;
    }

  err = mht_map (sessions.sessions_table, session_check_expired,
		 &timeout_info);

  csect_exit (CSECT_SESSION_STATE);

  if (timeout_info.session_ids != NULL)
    {
      assert (timeout_info.count > 0);
      free_and_init (timeout_info.session_ids);
    }

  return err;
}

/*
 * session_check_expired  () -
 *             remove an expired (timeout or mark deleted) session.
 *
 *   return  : NO_ERROR or error code
 *   key(in) : session id
 *   data(in): session state data
 *   args(in): timeout
 */
static int
session_check_expired (UNUSED_ARG const void *key, void *data, void *args)
{
  int err = NO_ERROR;
#if defined(SERVER_MODE)
  int i = 0;
#endif
  SESSION_STATE *session_p = (SESSION_STATE *) data;
  SESSION_TIMEOUT_INFO *timeout_info = (SESSION_TIMEOUT_INFO *) args;

  if (session_p->mark_deleted
      || (timeout_info->timeout->tv_sec - session_p->session_timeout.tv_sec
	  >= prm_get_integer_value (PRM_ID_SESSION_STATE_TIMEOUT)))
    {
#if defined(SERVER_MODE)
      /* first see if we still have an active connection */
      if (timeout_info->count == -1)
	{
	  /* we need to get the active connection list */
	  err =
	    css_get_session_ids_for_active_connections (&timeout_info->
							session_ids,
							&timeout_info->count);
	  if (err != NO_ERROR)
	    {
	      return err;
	    }
	}

      for (i = 0; i < timeout_info->count; i++)
	{
	  if (timeout_info->session_ids[i] == session_p->session_id)
	    {
	      /* also update timeout */
	      if (gettimeofday (&(session_p->session_timeout), NULL) != 0)
		{
		  err = ER_FAILED;
		}
	      if (session_p->mark_deleted)
		{
		  /* It is busy, unset mark_deleted */
		  session_p->mark_deleted = false;
		}
	      return err;
	    }
	}
#endif
      /* remove this session: timeout expired and it doesn't have an active
       * connection. */
      if (session_p->mark_deleted == false)
	{
	  er_log_debug (ARG_FILE_LINE, "timeout expired for session %u\n",
			session_p->session_id);
	}

      err = mht_rem (sessions.sessions_table, &(session_p->session_id),
		     session_free_session, NULL);

      if (err != NO_ERROR)
	{
	  return err;
	}
    }
  else
    {
      er_log_debug (ARG_FILE_LINE, "timeout ok for session %u\n",
		    session_p->session_id);
    }

  return err;
}

#if !defined(SERVER_MODE)
/*
 * session_get_session_id  () - get the session id associated with a thread
 *   return  : NO_ERROR or error code
 *   thread_p  (in) : thread for which to get the session id
 *   session_id(out): session_id
 */
int
session_get_session_id (UNUSED_ARG THREAD_ENTRY * thread_p, SESSION_KEY * key)
{
  assert (key != NULL);

  key->id = db_Session_id;
  key->fd = INVALID_SOCKET;

  return NO_ERROR;
}
#endif

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * login_user () - login user
 * return	  : error code
 * thread_p	  : worker thread
 * username(in)	  : name of the user
 */
int
login_user (THREAD_ENTRY * thread_p, const char *username)
{
  LOG_TDES *tdes = NULL;

  tdes = logtb_get_current_tdes (thread_p);
  if (tdes != NULL)
    {
      strncpy (tdes->client.db_user, username, DB_MAX_USER_LENGTH);
    }

  return NO_ERROR;
}
#endif

/*
 * session_states_dump () - dump the session states information
 *   return: void
 *   thread_p (in) : the thread executing this function
 */
void
session_states_dump (UNUSED_ARG THREAD_ENTRY * thread_p)
{
  if (csect_enter (thread_p, CSECT_SESSION_STATE, INF_WAIT) != NO_ERROR)
    {
      return;
    }

  if (sessions.sessions_table != NULL)
    {
      fprintf (stdout, "\nSESSION COUNT = %d\n",
	       mht_count (sessions.sessions_table));
      (void) mht_map (sessions.sessions_table, session_dump_session, NULL);
      fflush (stdout);
    }

  csect_exit (CSECT_SESSION_STATE);
}

/*
 * session_dump_session () - dump a session state
 *   return  : NO_ERROR
 *   key(in) : the key from the MHT_TABLE for this session
 *   data(in): session state data
 *   args(in): not used
 */
static int
session_dump_session (UNUSED_ARG const void *key, void *data,
		      UNUSED_ARG void *args)
{
  SESSION_STATE *session = (SESSION_STATE *) data;

  fprintf (stdout, "SESSION ID = %d\n", session->session_id);

  fprintf (stdout, "\n");

  return NO_ERROR;
}

/*
 * qentry_to_sentry () - create a session query entry from a query manager
 *			 entry
 * return : session query entry or NULL
 * qentry_p (in) : query manager query entry
 */
static SESSION_QUERY_ENTRY *
qentry_to_sentry (QMGR_QUERY_ENTRY * qentry_p)
{
  SESSION_QUERY_ENTRY *sqentry_p = NULL;
  assert (qentry_p != NULL);
  sqentry_p = (SESSION_QUERY_ENTRY *) malloc (sizeof (SESSION_QUERY_ENTRY));
  if (sqentry_p == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, sizeof (SESSION_QUERY_ENTRY));
      return NULL;
    }
  sqentry_p->query_id = qentry_p->query_id;
  sqentry_p->list_id = qentry_p->list_id;
  sqentry_p->qe_temp_vfid = qentry_p->qe_temp_vfid;

  qentry_p->list_id = NULL;
  qentry_p->qe_temp_vfid = NULL;

  sqentry_p->qe_num_temp = qentry_p->qe_num_temp;
  sqentry_p->total_count = qentry_p->total_count;
  sqentry_p->query_flag = qentry_p->query_flag;

  sqentry_p->next = NULL;

  return sqentry_p;
}

/*
 * session_preserve_temporary_files () - remove list files used by qentry_p
 *					 from the file manager so that it
 *					 doesn't delete them at transaction
 *					 end
 * return : error code or NO_ERROR
 * thread_p (in) :
 * qentry_p (in) :  query entry
 */
static int
session_preserve_temporary_files (THREAD_ENTRY * thread_p,
				  SESSION_QUERY_ENTRY * qentry_p)
{
  QMGR_TEMP_FILE *tfile_vfid_p = NULL, *temp = NULL;

  if (qentry_p == NULL)
    {
      assert (false);
      return NO_ERROR;
    }
  if (qentry_p->list_id == NULL)
    {
      return NO_ERROR;
    }
  if (qentry_p->list_id->page_cnt == 0)
    {
      /* make sure temp_file is not cyclic */
      if (qentry_p->qe_temp_vfid)
	{
	  qentry_p->qe_temp_vfid->prev->next = NULL;
	}
      return NO_ERROR;
    }
  if (qentry_p->qe_temp_vfid)
    {
      tfile_vfid_p = qentry_p->qe_temp_vfid;
      tfile_vfid_p->prev->next = NULL;
      while (tfile_vfid_p)
	{
	  file_preserve_temporary (thread_p, &tfile_vfid_p->temp_vfid);
	  temp = tfile_vfid_p;
	  tfile_vfid_p = tfile_vfid_p->next;
	}
    }
  return NO_ERROR;
}

/*
 * sentry_to_qentry () - create a query manager entry from a session query
 *			 entry
 * return : void
 * sentry_p (in)     : session query entry
 * qentry_p (in/out) : query manager query entry
 */
static void
sentry_to_qentry (const SESSION_QUERY_ENTRY * sentry_p,
		  QMGR_QUERY_ENTRY * qentry_p)
{
  qentry_p->query_id = sentry_p->query_id;
  qentry_p->list_id = sentry_p->list_id;
  qentry_p->qe_temp_vfid = sentry_p->qe_temp_vfid;

  qentry_p->qe_num_temp = sentry_p->qe_num_temp;
  qentry_p->total_count = sentry_p->total_count;
  qentry_p->query_mode = QUERY_COMPLETED;
  qentry_p->query_flag = sentry_p->query_flag;
  qentry_p->save_vpid.pageid = NULL_PAGEID;
  qentry_p->save_vpid.volid = NULL_VOLID;
  XASL_ID_SET_NULL (&qentry_p->xasl_id);
  qentry_p->xasl_ent = NULL;
  qentry_p->er_msg = NULL;
  qentry_p->is_holdable = true;
}

/*
 * session_store_query_entry_info () - create a query entry
 * return : void
 * thread_p (in) :
 * qentry_p (in) : query entry
 */
void
session_store_query_entry_info (THREAD_ENTRY * thread_p,
				QMGR_QUERY_ENTRY * qentry_p)
{
  SESSION_STATE *state_p = NULL;
  SESSION_QUERY_ENTRY *sqentry_p = NULL, *current = NULL;

  assert (qentry_p != NULL);

  state_p = session_get_session_state (thread_p);
  if (state_p == NULL)
    {
      return;
    }

  /* iterate over queries so we don't add the same query twice */
  current = state_p->queries;
  while (current != NULL)
    {
      if (current->query_id == qentry_p->query_id)
	{
	  /* we don't need to add it again, just set list_id to null
	     so that the query manager does not drop it */
	  qentry_p->list_id = NULL;
	  qentry_p->qe_temp_vfid = NULL;
	  return;
	}
      current = current->next;
    }

  /* We didn't find it. Create an entry and add it to the list */
  sqentry_p = qentry_to_sentry (qentry_p);
  if (sqentry_p == NULL)
    {
      return;
    }
  session_preserve_temporary_files (thread_p, sqentry_p);

  if (state_p->queries == NULL)
    {
      state_p->queries = sqentry_p;
    }
  else
    {
      sqentry_p->next = state_p->queries;
      state_p->queries = sqentry_p;
    }

  mnt_stats_gauge (thread_p, MNT_STATS_QUERY_HOLDABLE_CURSORS,
		   ++sessions.num_holdable_cursors);
}

/*
 * session_free_sentry_data () - close list files associated with a query
 *				 entry
 * return : void
 * thread_p (in) :
 * sentry_p (in) :
 */
static void
session_free_sentry_data (THREAD_ENTRY * thread_p,
			  SESSION_QUERY_ENTRY * sentry_p)
{
  if (sentry_p == NULL)
    {
      return;
    }

  if (sentry_p->list_id != NULL)
    {
      qfile_close_list (thread_p, sentry_p->list_id);
      qfile_free_list_id (sentry_p->list_id);
    }

  if (sentry_p->qe_temp_vfid != NULL)
    {
      qmgr_free_temp_file_list (thread_p, sentry_p->qe_temp_vfid,
				sentry_p->query_id, false);
    }

  mnt_stats_gauge (thread_p, MNT_STATS_QUERY_HOLDABLE_CURSORS,
		   --sessions.num_holdable_cursors);
}

/*
 * session_load_query_entry_info () - search for a query entry
 * return : error code or NO_ERROR
 * thread_p (in) :
 * qentry_p (in/out) : query entry
 */
int
session_load_query_entry_info (THREAD_ENTRY * thread_p,
			       QMGR_QUERY_ENTRY * qentry_p)
{
  SESSION_STATE *state_p = NULL;
  SESSION_QUERY_ENTRY *sentry_p = NULL;

  state_p = session_get_session_state (thread_p);
  if (state_p == NULL)
    {
      return ER_FAILED;
    }

  sentry_p = state_p->queries;
  while (sentry_p != NULL)
    {
      if (sentry_p->query_id == qentry_p->query_id)
	{
	  sentry_to_qentry (sentry_p, qentry_p);
	  return NO_ERROR;
	}
      sentry_p = sentry_p->next;
    }
  return ER_FAILED;
}

/*
 * session_remove_query_entry_info () - remove a query entry from the holdable
 *					queries list
 * return : error code or NO_ERROR
 * thread_p (in) : active thread
 * query_id (in) : query id
 */
int
session_remove_query_entry_info (THREAD_ENTRY * thread_p,
				 const QUERY_ID query_id)
{
  SESSION_STATE *state_p = NULL;
  SESSION_QUERY_ENTRY *sentry_p = NULL, *prev = NULL;
  state_p = session_get_session_state (thread_p);
  if (state_p == NULL)
    {
      return ER_FAILED;
    }

  sentry_p = state_p->queries;
  while (sentry_p != NULL)
    {
      if (sentry_p->query_id == query_id)
	{
	  /* remove sentry_p from the queries list */
	  if (prev == NULL)
	    {
	      state_p->queries = sentry_p->next;
	    }
	  else
	    {
	      prev->next = sentry_p->next;
	    }
	  session_free_sentry_data (thread_p, sentry_p);

	  free_and_init (sentry_p);
	  break;
	}
      prev = sentry_p;
      sentry_p = sentry_p->next;
    }

  return NO_ERROR;
}

/*
 * session_remove_query_entry_info () - remove a query entry from the holdable
 *					queries list but do not close the
 *					associated list files
 * return : error code or NO_ERROR
 * thread_p (in) : active thread
 * query_id (in) : query id
 */
int
session_clear_query_entry_info (THREAD_ENTRY * thread_p,
				const QUERY_ID query_id)
{
  SESSION_STATE *state_p = NULL;
  SESSION_QUERY_ENTRY *sentry_p = NULL, *prev = NULL;

  state_p = session_get_session_state (thread_p);
  if (state_p == NULL)
    {
      return ER_FAILED;
    }

  sentry_p = state_p->queries;
  while (sentry_p != NULL)
    {
      if (sentry_p->query_id == query_id)
	{
	  /* remove sentry_p from the queries list */
	  if (prev == NULL)
	    {
	      state_p->queries = sentry_p->next;
	    }
	  else
	    {
	      prev->next = sentry_p->next;
	    }

	  free_and_init (sentry_p);
	  mnt_stats_gauge (thread_p, MNT_STATS_QUERY_HOLDABLE_CURSORS,
			   --sessions.num_holdable_cursors);

	  break;
	}
      prev = sentry_p;
      sentry_p = sentry_p->next;
    }

  return NO_ERROR;
}

/*
 * session_set_conn_entry_data () - set references to session state objects
 *				    into the connection entry associated
 *				    with this thread
 * return : void
 * thread_p (in) : current thread
 * session_p (in) : session state object
 */
static void
session_set_conn_entry_data (UNUSED_ARG THREAD_ENTRY * thread_p,
			     UNUSED_ARG SESSION_STATE * session_p)
{
#if defined(SERVER_MODE)
  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
      if (thread_p == NULL)
	{
	  return;
	}
    }

  if (thread_p->conn_entry != NULL)
    {
      /* If we have a connection entry associated with this thread, setup
       * session data for this conn_entry
       */
      thread_p->conn_entry->session_p = session_p;
    }
#endif
}

/*
 * session_get_session_state () - get the session state object
 * return : session state object or NULL in case of error
 * thread_p (in) : thread for which to get the session
 */
static SESSION_STATE *
session_get_session_state (THREAD_ENTRY * thread_p)
{
#if defined(SERVER_MODE)
  /* The session state object cached in the conn_entry object associated with
   * every server request. Instead of accessing the session states hash
   * through a critical section, we can just return the hashed value. */
  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  if (thread_p != NULL && thread_p->conn_entry != NULL
      && thread_p->conn_entry->session_p != NULL)
    {
      return thread_p->conn_entry->session_p;
    }

  /* any request for this object should find it cached in the connection
   * entry */
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_SES_SESSION_EXPIRED, 0);

  return NULL;
#else
  SESSION_KEY key;
  int error = NO_ERROR;
  SESSION_STATE *state_p = NULL;

  error = session_get_session_id (thread_p, &key);
  if (error != NO_ERROR)
    {
      return NULL;
    }

  csect_enter_as_reader (thread_p, CSECT_SESSION_STATE, INF_WAIT);

  if (!sessions_is_states_table_initialized ())
    {
      csect_exit (CSECT_SESSION_STATE);
      return NULL;
    }

  state_p = mht_get (sessions.sessions_table, &key.id);
  csect_exit (CSECT_SESSION_STATE);

  if (state_p == NULL || state_p->related_socket != key.fd)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_SES_SESSION_EXPIRED, 0);
      return NULL;
    }

  return state_p;
#endif
}

/*
 * session_get_trace_stats () - return query trace result
 *   return  : query trace
 *   result(out) :
 */
int
session_get_trace_stats (THREAD_ENTRY * thread_p, DB_VALUE * result)
{
  SESSION_STATE *state_p = NULL;
  char *trace_str = NULL;
  size_t sizeloc;
  FILE *fp;
  json_t *xasl, *stats;
  DB_VALUE temp_result;

  state_p = session_get_session_state (thread_p);
  if (state_p == NULL)
    {
      return ER_FAILED;
    }

  if (state_p->trace_stats == NULL)
    {
      DB_MAKE_NULL (result);
      return NO_ERROR;
    }

  if (state_p->trace_format == QUERY_TRACE_TEXT)
    {
      fp = port_open_memstream (&trace_str, &sizeloc);
      if (fp)
	{
	  if (state_p->trace_stats != NULL)
	    {
	      fprintf (fp, "\nTrace Statistics:\n%s", state_p->trace_stats);
	    }

	  port_close_memstream (fp, &trace_str, &sizeloc);
	}
    }
  else if (state_p->trace_format == QUERY_TRACE_JSON)
    {
      stats = json_object ();

      if (state_p->trace_stats != NULL)
	{
	  xasl = json_loads (state_p->trace_stats, 0, NULL);
	  if (xasl != NULL)
	    {
	      json_object_set_new (stats, "Trace Statistics", xasl);
	    }
	}

      trace_str = json_dumps (stats, JSON_INDENT (2) | JSON_PRESERVE_ORDER);

      json_object_clear (stats);
      json_decref (stats);
    }

  if (trace_str != NULL)
    {
      DB_MAKE_STRING (&temp_result, trace_str);
      pr_clone_value (&temp_result, result);
      free_and_init (trace_str);
    }
  else
    {
      DB_MAKE_NULL (result);
    }

  thread_set_clear_trace (thread_p, true);

  return NO_ERROR;
}

/*
 * session_set_trace_stats () - save query trace result to session
 *   return  :
 *   stats(in) :
 *   format(in) :
 */
int
session_set_trace_stats (THREAD_ENTRY * thread_p, char *stats, int format)
{
  SESSION_STATE *state_p = NULL;

  state_p = session_get_session_state (thread_p);
  if (state_p == NULL)
    {
      return ER_FAILED;
    }

  if (state_p->trace_stats != NULL)
    {
      free_and_init (state_p->trace_stats);
    }

  state_p->trace_stats = stats;
  state_p->trace_format = format;

  return NO_ERROR;
}

/*
 * session_clear_trace_stats () - clear query trace result from session
 *   return  :
 *   stats(in) :
 *   format(in) :
 */
int
session_clear_trace_stats (THREAD_ENTRY * thread_p)
{
  SESSION_STATE *state_p = NULL;

  assert (thread_need_clear_trace (thread_p) == true);

  state_p = session_get_session_state (thread_p);
  if (state_p == NULL)
    {
      return ER_FAILED;
    }

  if (state_p->trace_stats != NULL)
    {
      free_and_init (state_p->trace_stats);
    }

  thread_set_clear_trace (thread_p, false);

  return NO_ERROR;
}
