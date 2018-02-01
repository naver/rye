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
 * heartbeat.c - heartbeat resource process common
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/types.h>

#include <fcntl.h>
#include <sys/time.h>
#include <sys/ioctl.h>
#include <sys/uio.h>
#include <sys/socket.h>
#include <netinet/in.h>

#include <unistd.h>

#include "environment_variable.h"
#include "porting.h"
#include "log_impl.h"
#include "system_parameter.h"
#include "error_manager.h"
#include "connection_defs.h"
#include "connection_support.h"
#include "connection_cl.h"
#include "tcp.h"
#include "heartbeat.h"
#include "master_heartbeat.h"
#include "utility.h"

#if defined(CS_MODE)
static THREAD_RET_T THREAD_CALLING_CONVENTION hb_thread_master_reader (void
								       *arg);
static CSS_CONN_ENTRY *hb_connect_to_master (const char *server_name,
					     const char *log_path,
					     HB_PROC_TYPE type);
static int hb_create_master_reader (void);
static const char *hb_type_to_str (HB_PROC_TYPE type);

static int hb_init_hbp_register (HBP_PROC_REGISTER * hbp_register,
				 HB_PROC_TYPE type, char *exec_path,
				 const char *argv[]);
#endif
static int hb_process_master_request_info (CSS_CONN_ENTRY * conn);



static CSS_CONN_ENTRY *hb_Conn = NULL;
static char hb_Exec_path[PATH_MAX];
static char **hb_Argv;

bool hb_Proc_shutdown = false;

SOCKET hb_Pipe_to_master = INVALID_SOCKET;

/*
 * hb_process_type_string () -
 *   return: process type string
 *
 *   ptype(in):
 */
const char *
hb_process_type_string (int ptype)
{
  switch (ptype)
    {
    case HB_PTYPE_SERVER:
      return HB_PTYPE_SERVER_STR;
    case HB_PTYPE_REPLICATION:
      return HB_PTYPE_REPLICATION_STR;
    }
  return "invalid";
}

/*
 * hb_set_exec_path () -
 *   return: none
 *
 *   exec_path(in):
 */
void
hb_set_exec_path (const char *prog_name)
{
  envvar_bindir_file (hb_Exec_path, sizeof (hb_Exec_path), prog_name);
}

/*
 * hb_set_argv () -
 *   return: none
 *
 *   argv(in):
 */
void
hb_set_argv (char **argv)
{
  hb_Argv = argv;
}


/*
 * css_send_heartbeat_request () -
 *   return:
 *
 *   conn(in):
 *   command(in):
 */
int
css_send_heartbeat_request (CSS_CONN_ENTRY * conn, int command,
			    int num_buffers, ...)
{
  if (conn && !IS_INVALID_SOCKET (conn->fd))
    {
      va_list args;
      int css_error;

      va_start (args, num_buffers);

      css_error = css_send_command_packet_v (conn, command, NULL,
					     num_buffers, args);
      va_end (args);

      return (css_error);
    }

  return CONNECTION_CLOSED;
}

/*
 * css_receive_heartbeat_request () -
 *   return:
 *
 *   conn(in):
 *   command(in):
 */
int
css_receive_heartbeat_request (CSS_CONN_ENTRY * conn,
			       CSS_NET_PACKET ** recv_packet)
{
  if (conn && !IS_INVALID_SOCKET (conn->fd))
    {
      CSS_NET_PACKET *tmp_recv_packet = NULL;
      int css_error;

      css_error = css_net_packet_recv (&tmp_recv_packet, conn, -1, 0);
      if (css_error != NO_ERRORS)
	{
	  return css_error;
	}

      if (tmp_recv_packet->header.packet_type == COMMAND_TYPE)
	{
	  if (recv_packet)
	    {
	      *recv_packet = tmp_recv_packet;
	      tmp_recv_packet = NULL;
	    }
	}
      else
	{
	  css_error = WRONG_PACKET_TYPE;
	}

      css_net_packet_free (tmp_recv_packet);
      return css_error;
    }
  return CONNECTION_CLOSED;
}

#if defined(CS_MODE)
/*
* hb_thread_master_reader () -
*   return: none
*
*   arg(in):
*/
static THREAD_RET_T THREAD_CALLING_CONVENTION
hb_thread_master_reader (UNUSED_ARG void *arg)
{
  int error;
  ER_MSG_INFO *er_msg;

  er_msg = malloc (sizeof (ER_MSG_INFO));
  error = er_set_msg_info (er_msg);
  if (error != NO_ERROR)
    {
      assert (false);
      return (THREAD_RET_T) NULL;
    }

  error = hb_process_master_request ();
  if (error != NO_ERROR)
    {
      hb_process_term ();

      /* wait 1 sec */
      THREAD_SLEEP (1000);

      /* is it ok? */
      os_send_signal (getpid (), SIGTERM);
    }

  return (THREAD_RET_T) 0;
}
#endif

/*
 * hb_init_hbp_register ()-
 *   return: error code
 *
 *   hbp_register(out):
 *   type(in):
 *   exec_path(in):
 *   argv(in):
 */
static int
hb_init_hbp_register (HBP_PROC_REGISTER * hbp_register, HB_PROC_TYPE type,
		      char *exec_path, const char *argv[])
{
  int i;
  int buf_size, len;

  memset ((void *) hbp_register, 0, sizeof (HBP_PROC_REGISTER));

  hbp_register->pid = -1;
  hbp_register->type = type;

  strncpy (hbp_register->exec_path, exec_path,
	   sizeof (hbp_register->exec_path) - 1);

  buf_size = sizeof (hbp_register->args);
  len = 0;
  for (i = 0; argv[i] != NULL && i < HB_MAX_NUM_PROC_ARGV; i++)
    {
      strncpy ((char *) hbp_register->argv[i], argv[i],
	       (HB_MAX_SZ_PROC_ARGV - 1));

      len = str_append (hbp_register->args, len, argv[i], buf_size - len);
      len = str_append (hbp_register->args, len, " ", buf_size - len);
    }
  if (len < 0)
    {
      return ER_FAILED;
    }

  return NO_ERROR;
}

/*
 * hb_make_hbp_register ()-
 *   return: error code
 *
 *   hbp_register(out):
 *   type(in):
 *   exec_path(in):
 *   argv(in):
 */
int
hb_make_hbp_register (HBP_PROC_REGISTER * hbp_register,
		      const HA_CONF * ha_conf, HB_PROC_TYPE proc_type,
		      HB_PROC_COMMAND command_type, const char *db_name,
		      const PRM_NODE_INFO * host_info)
{
  char log_path[PATH_MAX], db_host[PATH_MAX], exec_path[PATH_MAX];
  int i;
  const char *util_name = NULL;
  const char *args[] = { NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL };

  if (proc_type == HB_PTYPE_REPLICATION)
    {
      char host_str[MAX_NODE_INFO_STR_LEN];
      assert (host_info != NULL);
      prm_node_info_to_str (host_str, sizeof (host_str), host_info);
      ha_make_log_path (log_path, sizeof (log_path),
			ha_conf->node_conf[0].copy_log_base,
			db_name, host_str);
      ha_concat_db_and_host (db_host, sizeof (db_host), db_name, host_str);
    }

  i = 0;
  switch (proc_type)
    {
    case HB_PTYPE_SERVER:
      util_name = UTIL_SERVER_NAME;
      switch (command_type)
	{
	case HB_PCMD_START:
	  {
	    /* CMD: rye_server [db_name] */
	    args[i++] = util_name;
	    args[i++] = db_name;
	  }
	  break;
	case HB_PCMD_STOP:
	  break;
	default:
	  assert (false);
	  util_name = NULL;
	  break;
	}
      break;
    case HB_PTYPE_REPLICATION:
      util_name = UTIL_REPL_NAME;
      switch (command_type)
	{
	case HB_PCMD_START:
	  {
	    /* CMD: rye_server [db_name] */
	    args[i++] = util_name;
	    args[i++] = "--log-path";
	    args[i++] = log_path;
	    args[i++] = db_host;

	    if (rye_mkdir (log_path, 0755) != true)
	      {
		util_name = NULL;
	      }
	  }
	  break;
	case HB_PCMD_STOP:
	  break;
	default:
	  assert (false);
	  util_name = NULL;
	  break;
	}
      break;
    default:
      assert (false);
      util_name = NULL;
      break;
    }
  if (util_name == NULL || i >= (int) DIM (args))
    {
      assert (false);
      return ER_FAILED;
    }

  /* save executable path */
  (void) envvar_bindir_file (exec_path, sizeof (exec_path) - 1, util_name);

  return hb_init_hbp_register (hbp_register, proc_type, exec_path, args);
}

/*
* hb_make_set_hbp_register () -
*   return:
*
*   type(in):
*/
static HBP_PROC_REGISTER *
hb_make_set_hbp_register (HB_PROC_TYPE type)
{
  HBP_PROC_REGISTER *hbp_register;

  hbp_register = (HBP_PROC_REGISTER *) malloc (sizeof (HBP_PROC_REGISTER));
  if (NULL == hbp_register)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      sizeof (HBP_PROC_REGISTER));
      return NULL;
    }

  if (hb_init_hbp_register (hbp_register, type, hb_Exec_path,
			    (const char **) hb_Argv) != NO_ERROR)
    {
      free_and_init (hbp_register);
    }

  return (hbp_register);
}

/*
* hb_register_to_master () -
*   return: NO_ERROR or ER_FAILED
*
*   conn(in):
*   type(in):
*/
int
hb_register_to_master (CSS_CONN_ENTRY * conn, HB_PROC_TYPE type)
{
  int error;
  HBP_PROC_REGISTER *hbp_register = NULL;
  char *proc_id = NULL;

  if (NULL == conn)
    {
      er_log_debug (ARG_FILE_LINE, "invalid conn. (conn:NULL).\n");
      return (ER_FAILED);
    }

  hbp_register = hb_make_set_hbp_register (type);
  if (NULL == hbp_register)
    {
      er_log_debug (ARG_FILE_LINE, "hbp_register failed. \n");
      return (ER_FAILED);
    }
  proc_id = getenv (HB_PROC_ID_STR);
  if (proc_id == NULL)
    {
      er_log_debug (ARG_FILE_LINE, "not defined env(%s). \n", HB_PROC_ID_STR);
      free_and_init (hbp_register);
      return (ER_FAILED);
    }
  parse_int (&hbp_register->pid, proc_id, 10);

  er_log_debug (ARG_FILE_LINE, "hbp_register send. \n");
  error = css_send_heartbeat_request (conn, MASTER_REGISTER_HA_PROCESS, 1,
				      (const char *) hbp_register,
				      sizeof (*hbp_register));
  if (error != NO_ERRORS)
    {
      goto error_return;
    }

  er_log_debug (ARG_FILE_LINE, "hbp_register success. \n");
  free_and_init (hbp_register);
  return (NO_ERROR);

error_return:
  free_and_init (hbp_register);
  er_log_debug (ARG_FILE_LINE, "hbp_register fail. \n");
  return (ER_FAILED);
}

/*
* hb_process_master_request_info () -
*   return: NO_ERROR or ER_FAILED
*
*   conn(in):
*/
static int
hb_process_master_request_info (CSS_CONN_ENTRY * conn)
{
  if (NULL == conn)
    {
      er_log_debug (ARG_FILE_LINE, "invalid conn. (conn:NULL).\n");
      return (ER_FAILED);
    }

  if (css_receive_heartbeat_request (conn, NULL) == NO_ERRORS)
    {
      /* Ignore request, just check connection is alive or not */
      return (NO_ERROR);
    }

  return (ER_FAILED);
}

#if !defined(SERVER_MODE)
static const char *
hb_type_to_str (HB_PROC_TYPE type)
{
  if (type == HB_PTYPE_REPLICATION)
    {
      return "replication";
    }
  else
    {
      return "";
    }
}
#endif

/*
* hb_process_to_master () -
*   return: NO_ERROR or ER_FAILED
*
*   argv(in):
*/
int
hb_process_master_request (void)
{
  int error;
  int r, status = 0;
  struct pollfd po[1] = { {0, 0, 0} };

  if (NULL == hb_Conn)
    {
      er_log_debug (ARG_FILE_LINE, "hb_Conn did not allocated yet. \n");
      return (ER_FAILED);
    }

  while (false == hb_Proc_shutdown)
    {
      po[0].fd = hb_Conn->fd;
      po[0].events = POLLIN;
      r = poll (po, 1,
		(prm_get_integer_value (PRM_ID_TCP_CONNECTION_TIMEOUT) *
		 1000));

      switch (r)
	{
	case 0:
	  break;
	case -1:
	  if (!IS_INVALID_SOCKET (hb_Conn->fd)
	      && fcntl (hb_Conn->fd, F_GETFL, status) < 0)
	    hb_Proc_shutdown = true;
	  break;
	default:
	  error = hb_process_master_request_info (hb_Conn);
	  if (NO_ERROR != error)
	    {
	      hb_Proc_shutdown = true;
	    }
	  break;
	}
    }

  return (ER_FAILED);
}

#if defined(CS_MODE)
/*
 * hb_connect_to_master() - connect to the master server
 *   return: conn
 *   server_name(in): server name
 *   log_path(in): log path
 */
static CSS_CONN_ENTRY *
hb_connect_to_master (const char *server_name, const char *log_path,
		      HB_PROC_TYPE type)
{
  CSS_CONN_ENTRY *conn;

  conn = css_register_to_master (type, server_name, log_path);
  if (conn == NULL)
    {
      return NULL;
    }

  hb_Pipe_to_master = conn->fd;
  return conn;
}

/*
* hb_create_master_reader () -
*   return: NO_ERROR or ER_FAILED
*
*   conn(in):
*/
static int
hb_create_master_reader (void)
{
  int rv;
  pthread_attr_t thread_attr;
#if defined(_POSIX_THREAD_ATTR_STACKSIZE)
  size_t ts_size;
#endif
  pthread_t master_reader_th;

  rv = pthread_attr_init (&thread_attr);
  if (rv != 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_CSS_PTHREAD_ATTR_INIT, 0);
      return ER_CSS_PTHREAD_ATTR_INIT;
    }

  rv = pthread_attr_setdetachstate (&thread_attr, PTHREAD_CREATE_DETACHED);
  if (rv != 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_CSS_PTHREAD_ATTR_SETDETACHSTATE, 0);
      return ER_CSS_PTHREAD_ATTR_SETDETACHSTATE;
    }

  rv = pthread_attr_setscope (&thread_attr, PTHREAD_SCOPE_SYSTEM);
  if (rv != 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_CSS_PTHREAD_ATTR_SETSCOPE, 0);
      return ER_CSS_PTHREAD_ATTR_SETSCOPE;
    }

#if defined(_POSIX_THREAD_ATTR_STACKSIZE)
  rv = pthread_attr_getstacksize (&thread_attr, &ts_size);
  if (ts_size != (size_t) prm_get_bigint_value (PRM_ID_THREAD_STACKSIZE))
    {
      rv =
	pthread_attr_setstacksize (&thread_attr,
				   prm_get_bigint_value
				   (PRM_ID_THREAD_STACKSIZE));
      if (rv != 0)
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_CSS_PTHREAD_ATTR_SETSTACKSIZE, 0);
	  return ER_CSS_PTHREAD_ATTR_SETSTACKSIZE;
	}
    }
#endif /* _POSIX_THREAD_ATTR_STACKSIZE */

  rv =
    pthread_create (&master_reader_th, &thread_attr, hb_thread_master_reader,
		    (void *) NULL);
  if (rv != 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_CSS_PTHREAD_CREATE, 0);
      return ER_CSS_PTHREAD_CREATE;
    }

  return (NO_ERROR);
}
#endif

/*
* hb_process_init () -
*   return: NO_ERROR or ER_FAILED
*
*   server_name(in):
*   log_path(in):
*/
int
hb_process_init (UNUSED_ARG const char *server_name,
		 UNUSED_ARG const char *log_path,
		 UNUSED_ARG HB_PROC_TYPE type)
{
#if defined(CS_MODE)
  int error;
  static bool is_first = true;

  if (is_first == false)
    {
      return (NO_ERROR);
    }

  er_log_debug (ARG_FILE_LINE, "hb_process_init. (type:%s). \n",
		hb_type_to_str (type));

  if (hb_Exec_path[0] == '\0' || *(hb_Argv) == 0)
    {
      er_log_debug (ARG_FILE_LINE, "hb_Exec_path or hb_Argv is not set. \n");
      return (ER_FAILED);
    }

  hb_Conn = hb_connect_to_master (server_name, log_path, type);

  /* wait 1 sec */
  sleep (1);

  error = hb_register_to_master (hb_Conn, type);
  if (NO_ERROR != error)
    {
      er_log_debug (ARG_FILE_LINE, "hb_register_to_master failed. \n");
      return (error);
    }

  error = hb_create_master_reader ();
  if (NO_ERROR != error)
    {
      er_log_debug (ARG_FILE_LINE, "hb_create_master_reader failed. \n");
      return (error);
    }

  is_first = false;

  return (NO_ERROR);
#else
  return (ER_FAILED);
#endif
}


/*
* hb_process_term () -
*   return: none
*
*   type(in):
*/
void
hb_process_term (void)
{
  if (hb_Conn)
    {
      css_shutdown_conn (hb_Conn);
      hb_Conn = NULL;
    }
  hb_Proc_shutdown = true;
}
