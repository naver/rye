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
 * connection_sr.c - Client/Server connection list management
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>

#include <sys/time.h>
#include <sys/ioctl.h>
#include <sys/uio.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include <unistd.h>

#include "porting.h"
#include "error_manager.h"
#include "connection_globals.h"
#include "memory_alloc.h"
#include "environment_variable.h"
#include "system_parameter.h"
#include "thread.h"
#include "critical_section.h"
#include "log_manager.h"
#include "object_representation.h"
#include "connection_error.h"
#include "log_impl.h"
#include "tcp.h"
#include "connection_sr.h"

#ifdef PACKET_TRACE
#define TRACE(string, arg)					\
	do {							\
		er_log_debug(ARG_FILE_LINE, string, arg);	\
	}							\
	while(0);
#else /* PACKET_TRACE */
#define TRACE(string, arg)
#endif /* PACKET_TRACE */

#define NUM_MASTER_CHANNEL 1

static const int CSS_MAX_CLIENT_ID = INT_MAX - 1;

static int css_Client_id = 0;
static pthread_mutex_t css_Client_id_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t css_Conn_rule_lock = PTHREAD_MUTEX_INITIALIZER;
static CSS_CONN_ENTRY *css_Free_conn_anchor = NULL;
static int css_Num_free_conn = 0;
static int css_Num_max_conn = 101;      /* default max_clients + 1 for conn with master */

CSS_CONN_ENTRY *css_Conn_array = NULL;
CSS_CONN_ENTRY *css_Active_conn_anchor = NULL;
static int css_Num_active_conn = 0;

static LAST_ACCESS_STATUS *css_Access_status_anchor = NULL;
int css_Num_access_user = 0;

CSS_CONN_ENTRY *css_Listen_conn = NULL;

static int css_get_next_client_id (void);

static void css_dealloc_conn (CSS_CONN_ENTRY * conn);

static unsigned int css_make_eid (unsigned short entry_id, unsigned short rid);

static int css_increment_num_conn_internal (CSS_CONN_RULE_INFO * conn_rule_info);
static void css_decrement_num_conn_internal (CSS_CONN_RULE_INFO * conn_rule_info);

/*
 * get_next_client_id() -
 *   return: client id
 */
static int
css_get_next_client_id (void)
{
  static bool overflow = false;
  int next_client_id, rv, i;
  bool retry;

  rv = pthread_mutex_lock (&css_Client_id_lock);
  if (rv != 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_CSS_PTHREAD_MUTEX_LOCK, 0);
      return ER_FAILED;
    }

  do
    {
      css_Client_id++;
      if (css_Client_id == CSS_MAX_CLIENT_ID)
        {
          css_Client_id = 1;
          overflow = true;
        }

      retry = false;
      for (i = 0; overflow && i < css_Num_max_conn; i++)
        {
          if (css_Conn_array[i].client_id == css_Client_id)
            {
              retry = true;
              break;
            }
        }
    }
  while (retry);

  next_client_id = css_Client_id;

  rv = pthread_mutex_unlock (&css_Client_id_lock);
  if (rv != 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_CSS_PTHREAD_MUTEX_UNLOCK, 0);
      return ER_FAILED;
    }

  return next_client_id;
}

/*
 * css_initialize_conn() - initialize connection entry
 *   return: void
 *   conn(in):
 *   fd(in):
 */
int
css_initialize_conn (CSS_CONN_ENTRY * conn, SOCKET fd)
{
  int client_id;

  conn->fd = fd;
  conn->request_id = 0;
  conn->status = CONN_OPEN;
  conn->tran_index = -1;
  client_id = css_get_next_client_id ();
  if (client_id < 0)
    {
      return ER_CSS_CONN_INIT;
    }
  conn->client_id = client_id;
  conn->is_server_in_tran = false;
  conn->conn_reset_on_commit = false;
  conn->is_client_ro_tran = false;
  conn->stop_talk = false;
  conn->ignore_repl_delay = false;
  conn->stop_phase = THREAD_STOP_WORKERS_EXCEPT_LOGWR;
  conn->session_id = DB_EMPTY_SESSION;
  conn->server_shard_nodeid = 0;
#if defined(SERVER_MODE)
  conn->session_p = NULL;
  conn->client_type = BOOT_CLIENT_UNKNOWN;
  conn->con_close_handler_activated = false;
  conn->epoll_check_err = false;
#endif
  memset (&conn->peer_version, 0, sizeof (RYE_VERSION));

  return NO_ERROR;
}

/*
 * css_shutdown_conn() - close connection entry
 *   return: void
 *   conn(in):
 */
void
css_shutdown_conn (CSS_CONN_ENTRY * conn)
{
  if (!IS_INVALID_SOCKET (conn->fd))
    {
      /* if this is the PC, it also shuts down Winsock */
      css_shutdown_socket (conn->fd);
      conn->fd = INVALID_SOCKET;
    }

  if (conn->status == CONN_OPEN || conn->status == CONN_CLOSING)
    {
      conn->status = CONN_CLOSED;
      conn->stop_talk = false;
      conn->stop_phase = THREAD_STOP_WORKERS_EXCEPT_LOGWR;
    }

#if defined(SERVER_MODE)
  conn->session_p = NULL;
#endif
}

/*
 * css_init_conn_list() - initialize connection list
 *   return: NO_ERROR if success, or error code
 */
int
css_init_conn_list (void)
{
  int i, err;
  CSS_CONN_ENTRY *conn;

  css_init_conn_rules ();

  css_Num_max_conn = css_get_max_conn () + NUM_MASTER_CHANNEL;

  if (css_Conn_array != NULL)
    {
      return NO_ERROR;
    }

  /*
   * allocate NUM_MASTER_CHANNEL + the total number of
   *  conn entries
   */
  css_Conn_array = (CSS_CONN_ENTRY *) malloc (sizeof (CSS_CONN_ENTRY) * (css_Num_max_conn));
  if (css_Conn_array == NULL)
    {
      return ER_CSS_CONN_INIT;
    }

  /* initialize all CSS_CONN_ENTRY */
  for (i = 0; i < css_Num_max_conn; i++)
    {
      conn = &css_Conn_array[i];
      conn->idx = i;
      err = css_initialize_conn (conn, -1);
      if (err != NO_ERROR)
        {
          er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_CSS_CONN_INIT, 0);
          return ER_CSS_CONN_INIT;
        }
      if (err != NO_ERROR)
        {
          er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_CSS_CONN_INIT, 0);
          return ER_CSS_CONN_INIT;
        }

      pthread_mutex_init (&conn->conn_mutex, NULL);

      if (i < css_Num_max_conn - 1)
        {
          conn->next = &css_Conn_array[i + 1];
        }
      else
        {
          conn->next = NULL;
        }
    }

  /* initialize active conn list, used for stopping all threads */
  css_Active_conn_anchor = NULL;
  css_Free_conn_anchor = &css_Conn_array[0];
  css_Num_free_conn = css_Num_max_conn;

  return NO_ERROR;
}

/*
 * css_final_conn_list() - free connection list
 *   return: void
 */
void
css_final_conn_list (void)
{
  CSS_CONN_ENTRY *conn, *next;
  int i;

  if (css_Active_conn_anchor != NULL)
    {
      for (conn = css_Active_conn_anchor; conn != NULL; conn = next)
        {
          next = conn->next;
          css_shutdown_conn (conn);
          css_dealloc_conn (conn);

          css_Num_active_conn--;
          assert (css_Num_active_conn >= 0);
        }

      css_Active_conn_anchor = NULL;
    }

  assert (css_Num_active_conn == 0);
  assert (css_Active_conn_anchor == NULL);

  for (i = 0; i < css_Num_max_conn; i++)
    {
      conn = &css_Conn_array[i];
    }

  free_and_init (css_Conn_array);
}

/*
 * css_make_conn() - make new connection entry, but not insert into active
 *                   conn list
 *   return: new connection entry
 *   fd(in): socket discriptor
 */
CSS_CONN_ENTRY *
css_make_conn (SOCKET fd)
{
  CSS_CONN_ENTRY *conn = NULL;

  if (csect_enter (NULL, CSECT_CSS_FREE_CONN, INF_WAIT) != NO_ERROR)
    {
      assert (false);
      return NULL;
    }

  if (css_Free_conn_anchor != NULL)
    {
      conn = css_Free_conn_anchor;
      css_Free_conn_anchor = css_Free_conn_anchor->next;
      conn->next = NULL;

      css_Num_free_conn--;
      assert (css_Num_free_conn >= 0);
    }

  csect_exit (CSECT_CSS_FREE_CONN);

  if (conn != NULL)
    {
      if (css_initialize_conn (conn, fd) != NO_ERROR)
        {
          er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_CSS_CONN_INIT, 0);
          return NULL;
        }
    }

  return conn;
}

/*
 * css_insert_into_active_conn_list() - insert/remove into/from active conn
 *                                      list. this operation must be called
 *                                      after/before css_free_conn etc.
 *   return: void
 *   conn(in): connection entry will be inserted
 */
void
css_insert_into_active_conn_list (CSS_CONN_ENTRY * conn)
{
  if (csect_enter (NULL, CSECT_CSS_ACTIVE_CONN, INF_WAIT) != NO_ERROR)
    {
      assert (false);
      return;
    }


  conn->next = css_Active_conn_anchor;
  css_Active_conn_anchor = conn;

  css_Num_active_conn++;

  assert (css_Num_active_conn > 0);
  assert (css_Num_active_conn <= css_Num_max_conn);

  csect_exit (CSECT_CSS_ACTIVE_CONN);
}

/*
 * css_dealloc_conn() - free connection entry
 *   return: void
 *   conn(in): connection entry will be free
 */
static void
css_dealloc_conn (CSS_CONN_ENTRY * conn)
{
  if (csect_enter (NULL, CSECT_CSS_FREE_CONN, INF_WAIT) != NO_ERROR)
    {
      assert (false);
      return;
    }

  conn->next = css_Free_conn_anchor;
  css_Free_conn_anchor = conn;

  css_Num_free_conn++;
  assert (css_Num_free_conn > 0 && css_Num_free_conn <= css_Num_max_conn);

  csect_exit (CSECT_CSS_FREE_CONN);
}

/*
 * css_increment_num_conn_internal() - increments conn counter
 *   based on client type
 *   return: error code
 *   client_type(in): a type of a client trying
 *   to release the connection
 */
static int
css_increment_num_conn_internal (CSS_CONN_RULE_INFO * conn_rule_info)
{
  int error = NO_ERROR;

  switch (conn_rule_info->rule)
    {
    case CR_NORMAL_ONLY:
      if (conn_rule_info->num_curr_conn == conn_rule_info->max_num_conn)
        {
          error = ER_CSS_CLIENTS_EXCEEDED;
        }
      else
        {
          conn_rule_info->num_curr_conn++;
        }
      break;
    case CR_NORMAL_FIRST:
      /* tries to use a normal conn first */
      if (css_increment_num_conn_internal (&css_Conn_rules[CSS_CR_NORMAL_ONLY_IDX]) != NO_ERROR)
        {
          /* if normal conns are all occupied, uses a reserved conn */
          if (conn_rule_info->num_curr_conn == conn_rule_info->max_num_conn)
            {
              error = ER_CSS_CLIENTS_EXCEEDED;
            }
          else
            {
              conn_rule_info->num_curr_conn++;
              assert (conn_rule_info->num_curr_conn <= conn_rule_info->max_num_conn);
            }
        }
      break;
    case CR_RESERVED_FIRST:
      /* tries to use a reserved conn first */
      if (conn_rule_info->num_curr_conn < conn_rule_info->max_num_conn)
        {
          conn_rule_info->num_curr_conn++;
        }
      else                      /* uses a normal conn if no reserved conn is available */
        {
          if (css_increment_num_conn_internal (&css_Conn_rules[CSS_CR_NORMAL_ONLY_IDX]) != NO_ERROR)
            {
              error = ER_CSS_CLIENTS_EXCEEDED;
            }
          else
            {
              /* also increments its own conn counter */
              conn_rule_info->num_curr_conn++;
              assert (conn_rule_info->num_curr_conn <=
                      (css_Conn_rules[CSS_CR_NORMAL_ONLY_IDX].max_num_conn + conn_rule_info->max_num_conn));
            }
        }
      break;
    default:
      assert (false);
      break;
    }

  return error;
}

/*
 * css_decrement_num_conn_internal() - decrements conn counter
 *   based on client type
 *   return:
 *   client_type(in): a type of a client trying
 *   to release the connection
 */
static void
css_decrement_num_conn_internal (CSS_CONN_RULE_INFO * conn_rule_info)
{
  int i;

  switch (conn_rule_info->rule)
    {
    case CR_NORMAL_ONLY:
      /* When a normal client decrements the counter, it should
       * first check that other normal-first-reserved-last clients
       * need to take the released connection first.
       */
      for (i = 1; i < css_Conn_rules_size; i++)
        {
          if (css_Conn_rules[i].rule == CR_NORMAL_FIRST && css_Conn_rules[i].num_curr_conn > 0)
            {
              css_Conn_rules[i].num_curr_conn--;

              return;
            }
        }
      conn_rule_info->num_curr_conn--;
      break;

    case CR_NORMAL_FIRST:
      /* decrements reserved conn counter first if exists */
      if (conn_rule_info->num_curr_conn > 0)
        {
          conn_rule_info->num_curr_conn--;
        }
      else                      /* decrements normal conn counter if no reserved conn is in use */
        {
          css_decrement_num_conn_internal (&css_Conn_rules[CSS_CR_NORMAL_ONLY_IDX]);
        }
      break;

    case CR_RESERVED_FIRST:
      /* decrements normal conn counter if exists */
      if (conn_rule_info->num_curr_conn > conn_rule_info->max_num_conn)
        {
          css_decrement_num_conn_internal (&css_Conn_rules[CSS_CR_NORMAL_ONLY_IDX]);
        }
      /* also decrements its own conn counter */
      conn_rule_info->num_curr_conn--;
      break;

    default:
      assert (false);
      break;
    }

  assert (conn_rule_info->num_curr_conn >= 0);

  return;
}

/*
 * css_increment_num_conn() - increment a connection counter
 * and check if a client can take its connection
 *   return: error code
 *   client_type(in): a type of a client trying
 *   to take the connection
 */
int
css_increment_num_conn (BOOT_CLIENT_TYPE client_type)
{
  int i;
  int error = NO_ERROR;

  for (i = 0; i < css_Conn_rules_size; i++)
    {
      if (css_Conn_rules[i].check_client_type_fn (client_type))
        {
          pthread_mutex_lock (&css_Conn_rule_lock);
          error = css_increment_num_conn_internal (&css_Conn_rules[i]);
          pthread_mutex_unlock (&css_Conn_rule_lock);
          break;
        }
    }

  return error;
}

/*
 * css_decrement_num_conn() - decrement a connection counter
 *   return:
 *   client_type(in): a type of a client trying
 *   to release the connection
 */
void
css_decrement_num_conn (BOOT_CLIENT_TYPE client_type)
{
  int i;

  if (client_type == BOOT_CLIENT_UNKNOWN)
    {
      return;
    }

  for (i = 0; i < css_Conn_rules_size; i++)
    {
      if (css_Conn_rules[i].check_client_type_fn (client_type))
        {
          pthread_mutex_lock (&css_Conn_rule_lock);
          css_decrement_num_conn_internal (&css_Conn_rules[i]);
          pthread_mutex_unlock (&css_Conn_rule_lock);
          break;
        }
    }

  return;
}

/*
 * css_free_conn() - destroy all connection related structures, and free conn
 *                   entry, delete from css_Active_conn_anchor list
 *   return: void
 *   conn(in): connection entry will be free
 */
void
css_free_conn (CSS_CONN_ENTRY * conn)
{
  CSS_CONN_ENTRY *p, *prev = NULL, *next;

  if (csect_enter (NULL, CSECT_CSS_ACTIVE_CONN, INF_WAIT) != NO_ERROR)
    {
      assert (false);
      return;
    }

  /* find and remove from active conn list */
  for (p = css_Active_conn_anchor; p != NULL; p = next)
    {
      next = p->next;

      if (p == conn)
        {
          if (prev == NULL)
            {
              css_Active_conn_anchor = next;
            }
          else
            {
              prev->next = next;
            }

          css_Num_active_conn--;
          assert (css_Num_active_conn >= 0 && css_Num_active_conn < css_Num_max_conn);

          break;
        }

      prev = p;
    }

  css_shutdown_conn (conn);
  css_dealloc_conn (conn);
  css_decrement_num_conn (conn->client_type);

  csect_exit (CSECT_CSS_ACTIVE_CONN);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * css_print_conn_entry_info() - print connection entry information to stderr
 *   return: void
 *   conn(in): connection entry
 */
void
css_print_conn_entry_info (CSS_CONN_ENTRY * conn)
{
  fprintf (stderr,
           "CONN_ENTRY: %p, next(%p), idx(%d),fd(%d),request_id(%d),transaction_id(%d),client_id(%d)\n",
           conn, conn->next, conn->idx, conn->fd, conn->request_id, conn->tran_index, conn->client_id);
}

/*
 * css_print_conn_list() - print active connection list to stderr
 *   return: void
 */
void
css_print_conn_list (void)
{
  CSS_CONN_ENTRY *conn, *next;
  int i;

  if (css_Active_conn_anchor != NULL)
    {
      if (csect_enter_as_reader (NULL, CSECT_CSS_ACTIVE_CONN, INF_WAIT) != NO_ERROR)
        {
          assert (false);
          return;
        }

      fprintf (stderr, "active conn list (%d)\n", css_Num_active_conn);

      for (conn = css_Active_conn_anchor, i = 0; conn != NULL; conn = next, i++)
        {
          next = conn->next;
          css_print_conn_entry_info (conn);
        }

      assert (i == css_Num_active_conn);

      csect_exit (CSECT_CSS_ACTIVE_CONN);
    }
}
#endif

/*
 * css_common_connect_sr() - actually try to make a connection to a server.
 *   return: CSS_ERROR
 */
int
css_common_connect_sr (CSS_CONN_ENTRY * conn, unsigned short *rid,
                       const PRM_NODE_INFO * node_info, int connect_type, const char *packed_name, int packed_name_len)
{
  SOCKET fd;
  int css_error = NO_ERRORS;
  int timeout = prm_get_integer_value (PRM_ID_TCP_CONNECTION_TIMEOUT) * 1000;

  fd = css_tcp_client_open (node_info, connect_type, NULL, timeout);
  if (IS_INVALID_SOCKET (fd))
    {
      char hostname[256];
      prm_node_info_to_str (hostname, sizeof (hostname), node_info);
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ERR_CSS_TCP_CANNOT_CONNECT_TO_MASTER, 1, hostname);
      css_error = REQUEST_REFUSED;
    }
  else
    {
      conn->fd = fd;

      css_error = css_send_magic (conn);

      if (css_error == NO_ERRORS)
        {
          css_error = css_send_command_packet (conn, connect_type, rid, 1, packed_name, packed_name_len);
        }
    }

  return css_error;
}

/*
 * css_find_conn_by_tran_index() - find connection entry having given
 *                                 transaction id
 *   return: connection entry if find, or NULL
 *   tran_index(in): transaction id
 */
CSS_CONN_ENTRY *
css_find_conn_by_tran_index (int tran_index)
{
  CSS_CONN_ENTRY *conn = NULL, *next;

  if (css_Active_conn_anchor != NULL)
    {
      if (csect_enter_as_reader (NULL, CSECT_CSS_ACTIVE_CONN, INF_WAIT) != NO_ERROR)
        {
          assert (false);
          return NULL;
        }

      for (conn = css_Active_conn_anchor; conn != NULL; conn = next)
        {
          next = conn->next;
          if (conn->tran_index == tran_index)
            {
              break;
            }
        }

      csect_exit (CSECT_CSS_ACTIVE_CONN);
    }

  return conn;
}

/*
 * css_get_session_ids_for_active_connections () - get active session ids
 * return : error code or NO_ERROR
 * session_ids (out)  : holder for session ids
 * count (out)	      : number of session ids
 */
int
css_get_session_ids_for_active_connections (SESSION_ID ** session_ids, int *count)
{
  CSS_CONN_ENTRY *conn = NULL, *next = NULL;
  SESSION_ID *sessions_p = NULL;
  int error = NO_ERROR, i = 0;

  assert (count != NULL);
  if (count == NULL)
    {
      error = ER_FAILED;
      goto error_return;
    }

  if (css_Active_conn_anchor == NULL)
    {
      *session_ids = NULL;
      *count = 0;
      return NO_ERROR;
    }

  if (csect_enter_as_reader (NULL, CSECT_CSS_ACTIVE_CONN, INF_WAIT) != NO_ERROR)
    {
      assert (false);
      error = ER_FAILED;
      goto error_return;
    }

  *count = css_Num_active_conn;
  sessions_p = (SESSION_ID *) malloc (css_Num_active_conn * sizeof (SESSION_ID));

  if (sessions_p == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, css_Num_active_conn * sizeof (SESSION_ID));
      error = ER_FAILED;
      csect_exit (CSECT_CSS_ACTIVE_CONN);
      goto error_return;
    }

  for (conn = css_Active_conn_anchor; conn != NULL; conn = next)
    {
      next = conn->next;
      sessions_p[i] = conn->session_id;
      i++;
    }

  csect_exit (CSECT_CSS_ACTIVE_CONN);
  *session_ids = sessions_p;
  return error;

error_return:
  if (sessions_p != NULL)
    {
      free_and_init (sessions_p);
    }

  *session_ids = NULL;

  if (count != NULL)
    {
      *count = 0;
    }

  return error;
}

/*
 * css_shutdown_conn_by_tran_index() - shutdown connection having given
 *                                     transaction id
 *   return: void
 *   tran_index(in): transaction id
 */
void
css_shutdown_conn_by_tran_index (int tran_index)
{
  CSS_CONN_ENTRY *conn = NULL;

  if (css_Active_conn_anchor != NULL)
    {
      if (csect_enter (NULL, CSECT_CSS_ACTIVE_CONN, INF_WAIT) != NO_ERROR)
        {
          assert (false);
          return;
        }

      for (conn = css_Active_conn_anchor; conn != NULL; conn = conn->next)
        {
          if (conn->tran_index == tran_index)
            {
              if (conn->status == CONN_OPEN)
                {
                  conn->status = CONN_CLOSING;
                }
              break;
            }
        }

      csect_exit (CSECT_CSS_ACTIVE_CONN);
    }
}

/*
 * css_get_request_id() - return the next valid request id
 *   return: request id
 *   conn(in): connection entry
 */
unsigned short
css_get_request_id (CSS_CONN_ENTRY * conn)
{
  conn->request_id++;
  if (conn->request_id == 0)
    {
      conn->request_id++;
    }

  return conn->request_id;
}

/*
 * css_send_abort_request() - abort an outstanding request.
 *   return:  0 if success, or error code
 *   conn(in): connection entry
 *   request_id(in): request id
 *
 * Note: Once this is issued, any queued data buffers for this command will be
 *       released.
 */
int
css_send_abort_request (CSS_CONN_ENTRY * conn, unsigned short request_id)
{
  int rc;

  if (!conn || conn->status != CONN_OPEN)
    {
      return (CONNECTION_CLOSED);
    }

  rc = css_send_abort_packet (conn, request_id);

  return rc;
}

/*
 * css_return_eid_from_conn() - get enquiry id from connection entry
 *   return: enquiry id
 *   conn(in): connection entry
 *   rid(in): request id
 */
unsigned int
css_return_eid_from_conn (CSS_CONN_ENTRY * conn, unsigned short rid)
{
  return css_make_eid ((unsigned short) conn->idx, rid);
}

/*
 * css_make_eid() - make enquiry id
 *   return: enquiry id
 *   entry_id(in): connection entry id
 *   rid(in): request id
 */
static unsigned int
css_make_eid (unsigned short entry_id, unsigned short rid)
{
  int top;

  top = entry_id;
  return ((top << 16) | rid);
}

/*
 * css_set_user_access_status() - set user access status information
 *   return: void
 *   db_user(in):
 *   host(in):
 *   program_name(in):
 */
void
css_set_user_access_status (const char *db_user, const char *host, const char *program_name)
{
  LAST_ACCESS_STATUS *access = NULL;

  assert (db_user != NULL);
  assert (host != NULL);
  assert (program_name != NULL);

  csect_enter (NULL, CSECT_ACCESS_STATUS, INF_WAIT);

  for (access = css_Access_status_anchor; access != NULL; access = access->next)
    {
      if (strcmp (access->db_user, db_user) == 0)
        {
          break;
        }
    }

  if (access == NULL)
    {
      access = (LAST_ACCESS_STATUS *) malloc (sizeof (LAST_ACCESS_STATUS));
      if (access == NULL)
        {
          /* if memory allocation fail, just ignore and return */
          csect_exit (CSECT_ACCESS_STATUS);
          return;
        }
      css_Num_access_user++;

      memset (access, 0, sizeof (LAST_ACCESS_STATUS));

      access->next = css_Access_status_anchor;
      css_Access_status_anchor = access;

      strncpy (access->db_user, db_user, sizeof (access->db_user) - 1);
    }

  csect_exit (CSECT_ACCESS_STATUS);

  access->time = time (NULL);
  strncpy (access->host, host, sizeof (access->host) - 1);
  strncpy (access->program_name, program_name, sizeof (access->program_name) - 1);

  return;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * css_get_user_access_status() - get user access status informations
 *   return: void
 *   num_user(in):
 *   access_status_array(out):
 */
void
css_get_user_access_status (int num_user, LAST_ACCESS_STATUS ** access_status_array)
{
  int i = 0;
  LAST_ACCESS_STATUS *access = NULL;

  csect_enter_as_reader (NULL, CSECT_ACCESS_STATUS, INF_WAIT);

  for (access = css_Access_status_anchor; (access != NULL && i < num_user); access = access->next, i++)
    {
      access_status_array[i] = access;
    }

  csect_exit (CSECT_ACCESS_STATUS);

  return;
}
#endif

/*
 * css_free_user_access_status() - free all user access status information
 *   return: void
 */
void
css_free_user_access_status (void)
{
  LAST_ACCESS_STATUS *access = NULL;

  csect_enter (NULL, CSECT_ACCESS_STATUS, INF_WAIT);

  while (css_Access_status_anchor != NULL)
    {
      access = css_Access_status_anchor;
      css_Access_status_anchor = access->next;

      free_and_init (access);
    }

  csect_exit (CSECT_ACCESS_STATUS);

  css_Num_access_user = 0;

  return;
}

int
css_recv_data_packet_from_client (CSS_NET_PACKET ** recv_packet,
                                  CSS_CONN_ENTRY * conn, int rid, int timeout, int num_buffers, ...)
{
  CSS_NET_PACKET *tmp_recv_packet = NULL;
  int css_error;
  va_list args;

  if (recv_packet)
    {
      *recv_packet = NULL;
    }

  va_start (args, num_buffers);

  css_error = css_net_packet_recv_v (&tmp_recv_packet, conn, timeout, num_buffers, args);

  va_end (args);

  if (css_error != NO_ERRORS)
    {
      return css_error;
    }

  if (tmp_recv_packet->header.request_id != rid ||
      (tmp_recv_packet->header.packet_type != DATA_TYPE && tmp_recv_packet->header.packet_type != ERROR_TYPE))
    {
      assert (false);           /* TODO: NEED TEST */
      css_net_packet_free (tmp_recv_packet);
      return WRONG_PACKET_TYPE;
    }

  if (recv_packet)
    {
      *recv_packet = tmp_recv_packet;
    }
  else
    {
      css_net_packet_free (tmp_recv_packet);
    }

  return css_error;
}

#if defined (ENABLE_UNUSED_FUNCTION)
int
css_find_dupliated_conn (int conn_idx)
{
  CSS_CONN_ENTRY *conn;
  int i, tran_index;

  tran_index = css_Conn_array[conn_idx].tran_index;
  for (i = 0; i < css_Num_max_conn; i++)
    {
      conn = &css_Conn_array[i];
      if (i != conn_idx && !IS_INVALID_SOCKET (conn->fd) && conn->status == CONN_OPEN && conn->tran_index == tran_index)
        {
          return i;
        }
    }

  return -1;
}
#endif
