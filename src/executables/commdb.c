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
 * commdb.c - commdb main
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <stdarg.h>
#include <assert.h>
#ifdef HAVE_GETOPT_H
#include <getopt.h>
#else
#include "getopt.h"
#endif

#include <sys/types.h>
#include <netinet/in.h>
#include <unistd.h>

#include <netdb.h>

#include "connection_defs.h"
#include "connection_cl.h"
#include "error_manager.h"
#include "language_support.h"
#include "porting.h"
#include "heartbeat.h"
#include "master_util.h"
#include "message_catalog.h"
#include "utility.h"
#include "util_support.h"
#include "porting.h"
#include "client_support.h"
#include "commdb.h"
#include "object_representation.h"

static CSS_CONN_ENTRY *make_local_master_connection (CSS_CONN_ENTRY **
						     local_conn);


/*
 * request_for_string_value()
 */
static int
request_for_string_value (char **buffer, CSS_CONN_ENTRY * conn, int command)
{
  int area_size;
  char *area = NULL;
  int css_error = NO_ERRORS;


  if (buffer == NULL)
    {
      assert (false);
      return ERROR_ON_READ;
    }
  *buffer = NULL;

  css_error = css_send_request_to_master (conn, command, -1, 0, 1,
					  &area, &area_size);
  if (css_error != NO_ERRORS || area == NULL || area_size <= 0)
    {
      assert (css_error != NO_ERRORS);

      *buffer = NULL;
      return css_error;
    }

  or_unpack_string_alloc (area, buffer);

  free_and_init (area);

  return NO_ERRORS;
}

/*
 * request_for_int_value() - request for integer result value
 */
static int
request_for_int_value (int *int_value, CSS_CONN_ENTRY * conn, int command)
{
  char *area = NULL;
  int area_size;
  int css_error = NO_ERRORS;

  if (int_value == NULL)
    {
      assert (false);
      return ERROR_ON_READ;
    }
  *int_value = 0;

  css_error = css_send_request_to_master (conn, command, -1, 0, 1,
					  &area, &area_size);
  if (css_error != NO_ERRORS || area == NULL || area_size <= 0)
    {
      assert (css_error != NO_ERRORS);

      return css_error;
    }

  or_unpack_int (area, int_value);

  free_and_init (area);

  return NO_ERRORS;
}

/*
 * commdb_get_server_status() - print or get process status
 *   return: none
 *   conn(in): connection info
 */
int
commdb_get_server_status (CSS_CONN_ENTRY ** local_conn)
{
  int server_count, requests_serviced;
  char *buffer1 = NULL, *buffer2 = NULL;
  CSS_CONN_ENTRY *conn;

  conn = make_local_master_connection (local_conn);

  request_for_int_value (&requests_serviced, conn, MASTER_GET_REQUEST_COUNT);
  request_for_int_value (&server_count, conn, MASTER_GET_SERVER_COUNT);
  request_for_string_value (&buffer1, conn, MASTER_GET_START_TIME);
  request_for_string_value (&buffer2, conn, MASTER_GET_SERVER_LIST);

  if (server_count > 0 && buffer2 != NULL)
    {
      printf (msgcat_message (MSGCAT_CATALOG_UTILS, MSGCAT_UTIL_SET_COMMDB,
			      COMMDB_STRING4), buffer2);
    }

  free_and_init (buffer1);
  free_and_init (buffer2);

  return NO_ERROR;
}

/*
 * commdb_master_shutdown() - send request to shut down master
 *   return: none
 *   conn(in): connection info
 *   minutes(in): shutdown timeout in minutes
 */
int
commdb_master_shutdown (CSS_CONN_ENTRY ** local_conn, int minutes)
{
  OR_ALIGNED_BUF (OR_INT_SIZE) a_request;
  char *request;
  CSS_CONN_ENTRY *conn;
  int css_error = NO_ERRORS;

  conn = make_local_master_connection (local_conn);
  request = OR_ALIGNED_BUF_START (a_request);

  or_pack_int (request, minutes);
  css_error = css_send_request_to_master (conn, MASTER_START_SHUTDOWN, -1,
					  1, 0,
					  request,
					  OR_ALIGNED_BUF_SIZE (a_request));
  if (css_error != NO_ERRORS)
    {
      return ER_FAILED;
    }

  return NO_ERROR;
}

/*
 * commdb_ha_node_info_query() - process heartbeat node list
 *   return:  none
 *   conn(in): connection info
 *   verbose_yn(in):
 */
int
commdb_ha_node_info_query (CSS_CONN_ENTRY ** local_conn, bool verbose_yn)
{
  CSS_CONN_ENTRY *conn;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_request;
  char *request;
  char *reply;
  int reply_size;
  const char *msg;
  int css_error = NO_ERRORS;

  conn = make_local_master_connection (local_conn);
  request = OR_ALIGNED_BUF_START (a_request);

  or_pack_int (request, (int) verbose_yn);
  css_error = css_send_request_to_master (conn, MASTER_GET_HA_NODE_LIST, -1,
					  1, 1, request,
					  OR_ALIGNED_BUF_SIZE (a_request),
					  &reply, &reply_size);
  if (css_error != NO_ERRORS || reply == NULL || reply_size <= 0)
    {
      assert (css_error != NO_ERRORS);

      return ER_FAILED;
    }

  or_unpack_string_nocopy (reply, &msg);
  printf ("\n%s\n", msg);

  free_and_init (reply);

  return NO_ERROR;
}

/*
 commdb_ha_process_info_query() - process heartbeat process list
 *   return:  none
 *   conn(in): connection info
 *   verbose_yn(in):
 */
int
commdb_ha_process_info_query (CSS_CONN_ENTRY ** local_conn, bool verbose_yn)
{
  CSS_CONN_ENTRY *conn;
  OR_ALIGNED_BUF (OR_INT_SIZE) a_request;
  char *request;
  char *reply;
  int reply_size;
  const char *msg;
  int css_error = NO_ERRORS;

  conn = make_local_master_connection (local_conn);
  request = OR_ALIGNED_BUF_START (a_request);

  or_pack_int (request, (int) verbose_yn);
  css_error = css_send_request_to_master (conn, MASTER_GET_HA_PROCESS_LIST,
					  -1, 1, 1, request,
					  OR_ALIGNED_BUF_SIZE (a_request),
					  &reply, &reply_size);
  if (css_error != NO_ERRORS || reply == NULL || reply_size <= 0)
    {
      assert (css_error != NO_ERRORS);

      return ER_FAILED;
    }

  or_unpack_string_nocopy (reply, &msg);
  printf ("\n%s\n", msg);

  free_and_init (reply);

  return NO_ERROR;
}

/*
 * commdb_ha_ping_host_info_query() - process heartbeat ping hosts list
 *   return:  none
 *   conn(in): connection info
 */
int
commdb_ha_ping_host_info_query (CSS_CONN_ENTRY ** local_conn)
{
  char *reply_buffer = NULL;
  CSS_CONN_ENTRY *conn = make_local_master_connection (local_conn);

  request_for_string_value (&reply_buffer, conn,
			    MASTER_GET_HA_PING_HOST_INFO);

  if (reply_buffer != NULL)
    {
      printf ("\n%s\n", reply_buffer);
      free_and_init (reply_buffer);
    }

  return NO_ERROR;
}

/*
 * commdb_ha_admin_info_query() - request administrative info
 *   return:  none
 *   conn(in): connection info
 */
int
commdb_ha_admin_info_query (CSS_CONN_ENTRY ** local_conn)
{
  char *reply_buffer = NULL;
  CSS_CONN_ENTRY *conn;

  conn = make_local_master_connection (local_conn);

  request_for_string_value (&reply_buffer, conn, MASTER_GET_HA_ADMIN_INFO);

  if (reply_buffer != NULL)
    {
      printf ("\n%s\n", reply_buffer);
      free_and_init (reply_buffer);
    }

  return NO_ERROR;
}

/*
 * commdb_is_registered_procs () - check registerd server and repl agent
 *   return:  none
 *   local_conn(in): connection info
 */
int
commdb_is_registered_procs (CSS_CONN_ENTRY ** local_conn, bool * success_fail)
{
  char *reply_buffer = NULL;
  CSS_CONN_ENTRY *conn;

  *success_fail = false;

  conn = make_local_master_connection (local_conn);

  request_for_string_value (&reply_buffer, conn,
			    MASTER_IS_REGISTERED_HA_PROCS);

  if (reply_buffer == NULL)
    {
      *success_fail = false;
      return ER_FAILED;
    }

  if (strcmp (reply_buffer, HA_REQUEST_SUCCESS) == 0)
    {
      *success_fail = true;
    }
  else
    {
      *success_fail = false;
    }
  free_and_init (reply_buffer);

  return NO_ERROR;
}

/*
 * commdb_reconfig_heartbeat() - reconfigure heartbeat node
 *   return:  error code
 *   conn(in): connection info
 */
int
commdb_reconfig_heartbeat (CSS_CONN_ENTRY ** local_conn)
{
  CSS_CONN_ENTRY *conn;
  char *reply;
  int reply_size;
  int css_error = NO_ERRORS, error = NO_ERROR;

  conn = make_local_master_connection (local_conn);

  css_error = css_send_request_to_master (conn,
					  MASTER_RECONFIG_HEARTBEAT,
					  -1, 0, 1, &reply, &reply_size);
  if (css_error != NO_ERRORS || reply == NULL || reply_size <= 0)
    {
      assert (css_error != NO_ERRORS);

      return ER_FAILED;
    }

  or_unpack_int (reply, &error);

  free_and_init (reply);

  return error;
}

/*
 * commdb_changemode() - change heartbeat node
 *   return:  error code
 *   conn(in): connection info
 */
int
commdb_changemode (CSS_CONN_ENTRY ** local_conn, HA_STATE req_node_state,
		   bool force)
{
  CSS_CONN_ENTRY *conn;
  OR_ALIGNED_BUF (OR_INT_SIZE + OR_INT_SIZE) a_request;
  char *request, *ptr;
  char *reply;
  int reply_size;
  int error = NO_ERROR, css_error = NO_ERRORS;

  conn = make_local_master_connection (local_conn);

  request = OR_ALIGNED_BUF_START (a_request);

  ptr = or_pack_int (request, (int) req_node_state);
  ptr = or_pack_int (ptr, (int) force);

  css_error = css_send_request_to_master (conn, MASTER_CHANGEMODE,
					  -1, 1, 1, request,
					  OR_ALIGNED_BUF_SIZE (a_request),
					  &reply, &reply_size);
  if (css_error != NO_ERRORS || reply == NULL || reply_size <= 0)
    {
      assert (css_error != NO_ERRORS);

      return ER_FAILED;
    }

  or_unpack_int (reply, &error);

  free_and_init (reply);

  return error;
}

/*
 * commdb_deactivate_heartbeat() - deactivate heartbeat
 *   return:  none
 *   conn(in): connection info
 */
int
commdb_deactivate_heartbeat (CSS_CONN_ENTRY * conn)
{
  char *reply;
  int reply_size;
  const char *msg = NULL;
  char *ptr;
  int error = NO_ERROR, css_error = NO_ERRORS;

  css_error = css_send_request_to_master (conn, MASTER_DEACTIVATE_HEARTBEAT,
					  -1, 0, 1, &reply, &reply_size);
  if (css_error != NO_ERRORS || reply == NULL || reply_size <= 0)
    {
      assert (css_error != NO_ERRORS);

      return ER_FAILED;
    }

  ptr = or_unpack_int (reply, &error);
  ptr = or_unpack_string_nocopy (ptr, &msg);

  if (msg != NULL)
    {
      printf ("\n%s\n", msg);
    }

  free_and_init (reply);

  return error;
}

/*
 * commdb_deact_confirm_no_server()-
 *   return: error code
 *
 *   conn(in):
 *   success_fail(out):
 */
int
commdb_deact_confirm_no_server (CSS_CONN_ENTRY * conn, bool * success_fail)
{
  char *reply;
  int reply_size;
  int css_error = NO_ERRORS;
  int int_value;

  *success_fail = false;

  css_error = css_send_request_to_master (conn,
					  MASTER_DEACT_CONFIRM_NO_SERVER,
					  -1, 0, 1, &reply, &reply_size);
  if (css_error != NO_ERRORS || reply == NULL || reply_size <= 0)
    {
      assert (css_error != NO_ERRORS);

      return ER_FAILED;
    }

  or_unpack_int (reply, &int_value);

  *success_fail = (bool) int_value;

  free_and_init (reply);


  return NO_ERROR;
}

/*
 * commdb_deact_confirm_stop_all ()-
 *   return: error code
 *
 *   conn(in):
 *   success_fail(out):
 */
int
commdb_deact_confirm_stop_all (CSS_CONN_ENTRY * conn, bool * success_fail)
{
  char *reply;
  int reply_size;
  int int_value;
  int css_error = NO_ERRORS;

  *success_fail = false;

  css_error = css_send_request_to_master (conn, MASTER_DEACT_CONFIRM_STOP_ALL,
					  -1, 0, 1, &reply, &reply_size);
  if (css_error != NO_ERRORS || reply == NULL || reply_size <= 0)
    {
      assert (css_error != NO_ERRORS);

      return ER_FAILED;
    }

  or_unpack_int (reply, &int_value);

  *success_fail = (bool) int_value;

  free_and_init (reply);

  return NO_ERROR;
}

int
commdb_deact_stop_all (CSS_CONN_ENTRY * conn, bool deact_immediately)
{
  OR_ALIGNED_BUF (OR_INT_SIZE) a_request;
  char *request;
  char *reply;
  int reply_size;
  int error = NO_ERROR, css_error = NO_ERRORS;

  request = OR_ALIGNED_BUF_START (a_request);

  or_pack_int (request, (int) deact_immediately);

  css_error = css_send_request_to_master (conn, MASTER_DEACT_STOP_ALL,
					  -1, 1, 1, request,
					  OR_ALIGNED_BUF_SIZE (a_request),
					  &reply, &reply_size);
  if (css_error != NO_ERRORS || reply == NULL || reply_size <= 0)
    {
      assert (css_error != NO_ERRORS);

      return ER_FAILED;
    }

  or_unpack_int (reply, &error);

  free_and_init (reply);

  return error;
}

/*
 * commdb_activate_heartbeat() - activate heartbeat
 *   return:  none
 *   conn(in): connection info
 */
int
commdb_activate_heartbeat (CSS_CONN_ENTRY ** local_conn)
{
  int error = NO_ERROR;
  char *area = NULL;
  int area_size = 0;
  CSS_CONN_ENTRY *conn;

  conn = make_local_master_connection (local_conn);

  error = css_send_request_to_master (conn, MASTER_ACTIVATE_HEARTBEAT, -1,
				      0, 1, &area, &area_size);
  if (error != NO_ERRORS || area == NULL || area_size <= 0)
    {
      assert (error != NO_ERRORS);

      return ER_FAILED;
    }

  or_unpack_int (area, &error);

  free_and_init (area);

  return error;
}

static CSS_CONN_ENTRY *
make_local_master_connection (CSS_CONN_ENTRY ** local_conn)
{
  if (*local_conn == NULL)
    {
      PRM_NODE_INFO node_info = prm_get_myself_node_info ();
      unsigned short rid;
      *local_conn = css_connect_to_master_for_info (&node_info, &rid);
    }

  return *local_conn;
}
