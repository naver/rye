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
 * util_service.c - a front end of service utilities
 */

#ident "$Id$"

#include "config.h"

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <fcntl.h>
#include <assert.h>
#include <sys/wait.h>

#include "porting.h"
#include "utility.h"
#include "error_code.h"
#include "util_support.h"
#include "system_parameter.h"
#include "connection_cl.h"
#include "util_func.h"
#include "environment_variable.h"
#include "release_string.h"
#include "dynamic_array.h"
#include "heartbeat.h"
#include "rye_master_shm.h"
#include "language_support.h"
#include "broker_admin_pub.h"
#include "commdb.h"

#define IS_ARG_UTIL(t) \
  ((ARG_UTIL_SERVICE <= (t) && (t) < ARG_CMD_HELP) ? true : false)

#define IS_ARG_CMD(t) \
  ((ARG_CMD_HELP <= (t) && (t) < ARG_END) ? true : false)

#define DOES_UTIL_NEED_CONF(t) \
  (!_DOES_UTIL_NEED_NOT_CONF(t))

#define _DOES_UTIL_NEED_NOT_CONF(t) \
  ((t) == ARG_UTIL_BROKER)

typedef enum
{
  ALL_SERVICES_RUNNING,
  ALL_SERVICES_STOPPED
} UTIL_ALL_SERVICES_STATUS;

typedef struct
{
  ARG_TYPE arg_type;
  const char *arg_name;
  int arg_mask;
} UTIL_ARG_MAP_T;

typedef struct
{
  int property_index;
  char *property_value;
} UTIL_SERVICE_PROPERTY_T;

#define ARG_NAME_UTIL_SERVICE           "service"
#define ARG_NAME_UTIL_BROKER            "broker"
#define ARG_NAME_UTIL_HEARTBEAT         "heartbeat"
#define ARG_NAME_UTIL_HB_SHORT          "hb"
#define ARG_NAME_CMD_START              "start"
#define ARG_NAME_CMD_STOP               "stop"
#define ARG_NAME_CMD_RESTART            "restart"
#define ARG_NAME_CMD_STATUS             "status"
#define ARG_NAME_CMD_LIST               "list"
#define ARG_NAME_CMD_RELOAD             "reload"
#define ARG_NAME_CMD_CHANGEMODE         "changemode"
#define ARG_NAME_CMD_ON                 "on"
#define ARG_NAME_CMD_OFF                "off"
#define ARG_NAME_CMD_ACL                "acl"
#define ARG_NAME_CMD_RESET              "reset"
#define ARG_NAME_CMD_INFO               "info"
#define ARG_NAME_CMD_TEST               "test"

static UTIL_ARG_MAP_T us_Arg_map[] = {
  {ARG_UTIL_SERVICE, ARG_NAME_UTIL_SERVICE, MASK_SERVICE},
  {ARG_UTIL_BROKER, ARG_NAME_UTIL_BROKER, MASK_BROKER},
  {ARG_UTIL_HEARTBEAT, ARG_NAME_UTIL_HEARTBEAT, MASK_HEARTBEAT},
  {ARG_UTIL_HEARTBEAT, ARG_NAME_UTIL_HB_SHORT, MASK_HEARTBEAT},
  {ARG_CMD_HELP, "--help", MASK_ALL},
  {ARG_CMD_VERSION, "--version", MASK_ALL},
  {ARG_UTIL_ADMIN, UTIL_OPTION_CREATEDB, MASK_ADMIN},
  {ARG_UTIL_ADMIN, UTIL_OPTION_DELETEDB, MASK_ADMIN},
  {ARG_UTIL_ADMIN, UTIL_OPTION_BACKUPDB, MASK_ADMIN},
  {ARG_UTIL_ADMIN, UTIL_OPTION_RESTOREDB, MASK_ADMIN},
  {ARG_UTIL_ADMIN, UTIL_OPTION_ADDVOLDB, MASK_ADMIN},
  {ARG_UTIL_ADMIN, UTIL_OPTION_SPACEDB, MASK_ADMIN},
  {ARG_UTIL_ADMIN, UTIL_OPTION_LOCKDB, MASK_ADMIN},
  {ARG_UTIL_ADMIN, UTIL_OPTION_KILLTRAN, MASK_ADMIN},
  {ARG_UTIL_ADMIN, UTIL_OPTION_DIAGDB, MASK_ADMIN},
  {ARG_UTIL_ADMIN, UTIL_OPTION_PLANDUMP, MASK_ADMIN},
  {ARG_UTIL_ADMIN, UTIL_OPTION_PARAMDUMP, MASK_ADMIN},
  {ARG_UTIL_ADMIN, UTIL_OPTION_STATDUMP, MASK_ADMIN},
  {ARG_UTIL_ADMIN, UTIL_OPTION_TRANLIST, MASK_ADMIN},
  {ARG_CMD_START, ARG_NAME_CMD_START, MASK_ALL},
  {ARG_CMD_STOP, ARG_NAME_CMD_STOP, MASK_ALL},
  {ARG_CMD_RESTART, ARG_NAME_CMD_RESTART,
   MASK_SERVICE | MASK_BROKER},
  {ARG_CMD_STATUS, ARG_NAME_CMD_STATUS, MASK_ALL},
  {ARG_CMD_LIST, ARG_NAME_CMD_LIST, MASK_HEARTBEAT},
  {ARG_CMD_RELOAD, ARG_NAME_CMD_RELOAD, MASK_HEARTBEAT},
  {ARG_CMD_CHANGEMODE, ARG_NAME_CMD_CHANGEMODE, MASK_HEARTBEAT},
  {ARG_CMD_ON, ARG_NAME_CMD_ON, MASK_BROKER},
  {ARG_CMD_OFF, ARG_NAME_CMD_OFF, MASK_BROKER},
  {ARG_CMD_ACCESS_CONTROL, ARG_NAME_CMD_ACL,
   MASK_SERVER | MASK_BROKER},
  {ARG_CMD_RESET, ARG_NAME_CMD_RESET, MASK_BROKER},
  {ARG_CMD_INFO, ARG_NAME_CMD_INFO, MASK_BROKER},
  {ARG_CMD_TEST, ARG_NAME_CMD_TEST, MASK_BROKER},
  {ARG_END, "", MASK_ADMIN}
};

CSS_CONN_ENTRY *master_Conn = NULL;
static char *exec_Name;

static int util_get_service_option_mask (ARG_TYPE util_type);
static int util_get_command_option_mask (ARG_TYPE command_type);
static void util_service_usage (int util_type);
static void util_service_version (void);
static int load_properties (void);
static ARG_TYPE parse_arg (UTIL_ARG_MAP_T * option, const char *arg);
static int process_service (int command_type);
static int process_server (int command_type, int argc, char **argv,
			   bool show_usage);
static int process_broker (int command_type, int argc, char **argv);
static int process_heartbeat (int command_type, int argc, char **argv);
static int process_heartbeat_start (HA_CONF * ha_conf, int argc, char **argv);
static int process_heartbeat_stop (HA_CONF * ha_conf, int argc, char **argv);
static int process_heartbeat_status (int argc, char **argv);
static int process_heartbeat_reload (int argc, char **argv);
static int process_heartbeat_changemode (UNUSED_ARG int argc,
					 UNUSED_ARG char **argv);
static int proc_execute (const char *file, char *args[],
			 bool wait_child, bool close_output,
			 bool close_err, int *pid);
static int process_master (int command_type);
static void print_message (FILE * output, int message_id, ...);
static void print_result (const char *util_name, int status,
			  int command_type);
static const char *command_string (int command_type);

static bool are_all_services_running (unsigned int sleep_time);
static bool are_all_services_stopped (unsigned int sleep_time);
static bool check_all_services_status (unsigned int sleep_time,
				       UTIL_ALL_SERVICES_STATUS
				       expected_status);


static int us_hb_deactivate (const PRM_NODE_INFO * node_info,
			     bool immediate_stop);
static int us_hb_activate (void);
static int us_hb_reload (void);
static int us_hb_changemode (HA_STATE req_node_state, bool force);

#define US_HB_DEREG_WAIT_TIME_IN_SEC	100

static const char *
command_string (int command_type)
{
  const char *command;

  switch (command_type)
    {
    case ARG_CMD_START:
      command = PRINT_CMD_START;
      break;
    case ARG_CMD_STATUS:
      command = PRINT_CMD_STATUS;
      break;
    case ARG_CMD_LIST:
      command = PRINT_CMD_LIST;
      break;
    case ARG_CMD_RELOAD:
      command = PRINT_CMD_RELOAD;
      break;
    case ARG_CMD_ACCESS_CONTROL:
      command = PRINT_CMD_ACL;
      break;
    case ARG_CMD_TEST:
      command = PRINT_CMD_TEST;
      break;
    case ARG_CMD_STOP:
    default:
      command = PRINT_CMD_STOP;
      break;
    }

  return command;
}

static void
print_result (const char *util_name, int status, int command_type)
{
  const char *result;

  if (status != NO_ERROR)
    {
      result = PRINT_RESULT_FAIL;
    }
  else
    {
      result = PRINT_RESULT_SUCCESS;
    }

  print_message (stdout, MSGCAT_UTIL_GENERIC_RESULT, util_name,
		 command_string (command_type), result);
}

/*
 * print_message() -
 *
 * return:
 *
 */
static void
print_message (FILE * output, int message_id, ...)
{
  va_list arg_list;
  const char *format;

  format = utility_get_generic_message (message_id);
  va_start (arg_list, message_id);
  vfprintf (output, format, arg_list);
  va_end (arg_list);
}

/*
 * process_admin() - process admin utility
 *
 * return:
 *
 */
static int
process_admin (int argc, char **argv)
{
  int status;
  bool exec_cs_mode = true;

  status = util_admin (argc, argv, &exec_cs_mode);
  if (status != NO_ERROR && exec_cs_mode == false)
    {
      status = proc_execute (UTIL_ADMIN_SA_NAME, argv, true,
			     false, false, NULL);
    }

  return status;
}

/*
 * util_get_service_option_mask () -
 *
 */
static int
util_get_service_option_mask (ARG_TYPE util_type)
{
  int i;

  assert (IS_ARG_UTIL (util_type));
  assert (util_type != ARG_UTIL_ADMIN);
  assert (util_type != ARG_UNKNOWN);
  assert (util_type < ARG_CMD_HELP);

  for (i = 0; us_Arg_map[i].arg_type != ARG_END; i++)
    {
      if (us_Arg_map[i].arg_type == util_type)
	{
	  return us_Arg_map[i].arg_mask;
	}
    }
  return 0;			/* NULL mask */
}

/*
 * util_get_command_option_mask () -
 *
 */
static int
util_get_command_option_mask (ARG_TYPE command_type)
{
  int i;

  assert (IS_ARG_CMD (command_type));
  assert (command_type >= ARG_CMD_HELP);
  assert (command_type < ARG_END);

  for (i = 0; us_Arg_map[i].arg_type != ARG_END; i++)
    {
      if (us_Arg_map[i].arg_type == command_type)
	{
	  return us_Arg_map[i].arg_mask;
	}
    }
  return 0;			/* NULL mask */
}

/*
 * main() - a service utility's entry point
 *
 * return:
 *
 * NOTE:
 */
int
main (int argc, char *argv[])
{
  ARG_TYPE util_type, command_type;
  int status;
  pid_t pid = getpid ();
  char env_buf[16];

  sprintf (env_buf, "%d", pid);
  envvar_set (UTIL_PID_ENVVAR_NAME, env_buf);

  exec_Name = basename (argv[0]);

  if (argc == 2)
    {
      if (parse_arg (us_Arg_map, argv[1]) == ARG_CMD_VERSION)
	{
	  util_service_version ();
	  return EXIT_SUCCESS;
	}
    }

  /* validate the number of arguments to avoid klocwork's error message */
  if (argc < 2 || argc > 1024)
    {
      util_type = -1;
      goto usage;
    }

  util_type = parse_arg (us_Arg_map, argv[1]);
  if (!IS_ARG_UTIL (util_type))
    {
      util_type = parse_arg (us_Arg_map, argv[2]);
      if (!IS_ARG_UTIL (util_type))
	{
	  print_message (stderr, MSGCAT_UTIL_GENERIC_SERVICE_INVALID_NAME,
			 argv[1]);
	  goto error;
	}
    }

  if (os_set_signal_handler (SIGPIPE, SIG_IGN) == SIG_ERR)
    {
      goto error;
    }

  (void) er_init (prm_get_string_value (PRM_ID_ER_LOG_FILE),
		  prm_get_integer_value (PRM_ID_ER_EXIT_ASK));

  if (lang_init () != NO_ERROR)
    {
      assert (false);
      goto error;
    }

  if (utility_initialize () != NO_ERROR)
    {
      assert (false);
      goto error;
    }

  if (load_properties () != NO_ERROR)
    {
      if (DOES_UTIL_NEED_CONF (util_type))
	{
	  print_message (stderr, MSGCAT_UTIL_GENERIC_SERVICE_PROPERTY_FAIL);

	  util_log_write_command (argc, argv);
	  util_log_write_errid (MSGCAT_UTIL_GENERIC_SERVICE_PROPERTY_FAIL);

	  return EXIT_FAILURE;
	}
    }

  if (util_type == ARG_UTIL_ADMIN)
    {
      util_log_write_command (argc, argv);
      status = process_admin (argc, argv);
      util_log_write_result (status);

      return status;
    }

  if (util_type == ARG_CMD_HELP)
    {
      util_type = -1;
      goto usage;
    }

  if (argc < 3)
    {
      goto usage;
    }

  command_type = parse_arg (us_Arg_map, (char *) argv[2]);
  if (!IS_ARG_CMD (command_type))
    {
      command_type = parse_arg (us_Arg_map, (char *) argv[1]);
      if (!IS_ARG_CMD (command_type))
	{
	  print_message (stderr, MSGCAT_UTIL_GENERIC_SERVICE_INVALID_CMD,
			 argv[2]);
	  goto error;
	}
    }
  else
    {
      int util_mask = util_get_service_option_mask (util_type);
      int command_mask = util_get_command_option_mask (command_type);

      if ((util_mask & command_mask) == 0)
	{
	  print_message (stderr, MSGCAT_UTIL_GENERIC_SERVICE_INVALID_CMD,
			 argv[2]);
	  goto error;
	}
    }

  util_log_write_command (argc, argv);

  switch (util_type)
    {
    case ARG_UTIL_SERVICE:
      status = process_service (command_type);
      break;
    case ARG_UTIL_BROKER:
      status = process_broker (command_type, argc - 3, &argv[3]);
      break;
    case ARG_UTIL_HEARTBEAT:
      status = process_heartbeat (command_type, argc - 3, &argv[3]);
      break;
    default:
      goto usage;
    }

  util_log_write_result (status);

  if (status == ER_ARG_OUT_OF_RANGE)
    {
      goto usage;
    }

  css_free_conn (master_Conn);
  return ((status == NO_ERROR) ? EXIT_SUCCESS : EXIT_FAILURE);

usage:
  util_service_usage (util_type);
error:
  css_free_conn (master_Conn);
  return EXIT_FAILURE;
}

/*
 * util_service_usage - display an usage of this utility
 *
 * return:
 *
 * NOTE:
 */
static void
util_service_usage (int util_type)
{
  if (util_type < 0)
    {
      util_type = 0;
    }
  else
    {
      util_type++;
    }

  print_message (stdout, MSGCAT_UTIL_GENERIC_RYE_USAGE + util_type,
		 PRODUCT_STRING, exec_Name, exec_Name, exec_Name);
}

/*
 * util_service_version - display a version of this utility
 *
 * return:
 *
 * NOTE:
 */
static void
util_service_version ()
{
  char buf[REL_MAX_VERSION_LENGTH];

  rel_copy_release_string (buf, REL_MAX_VERSION_LENGTH);
  print_message (stdout, MSGCAT_UTIL_GENERIC_VERSION, exec_Name, buf);
}


static int
proc_execute (const char *file, char *args[], bool wait_child,
	      bool close_output, bool close_err, int *out_pid)
{
  pid_t pid, tmp;
  char executable_path[PATH_MAX];

  if (out_pid)
    {
      *out_pid = 0;
    }

  (void) envvar_bindir_file (executable_path, PATH_MAX, file);

  /* do not process SIGCHLD, a child process will be defunct */
  if (wait_child)
    {
      signal (SIGCHLD, SIG_DFL);
    }
  else
    {
      signal (SIGCHLD, SIG_IGN);
    }

  pid = fork ();
  if (pid == -1)
    {
      perror ("fork");
      return ER_GENERIC_ERROR;
    }
  else if (pid == 0)
    {
      /* a child process handle SIGCHLD to SIG_DFL */
      signal (SIGCHLD, SIG_DFL);
      if (close_output)
	{
	  fclose (stdout);
	}

      if (close_err)
	{
	  fclose (stderr);
	}

      if (execv (executable_path, args) == -1)
	{
	  perror ("execv");
	  return ER_GENERIC_ERROR;
	}
    }
  else
    {
      int status = 0;

      /* sleep (0); */
      if (wait_child)
	{
	  do
	    {
	      tmp = waitpid (-1, &status, 0);
	      if (tmp == -1)
		{
		  perror ("waitpid");
		  return ER_GENERIC_ERROR;
		}
	    }
	  while (tmp != pid);
	}
      else
	{
	  /*sleep (3); */
	  if (out_pid)
	    {
	      *out_pid = pid;
	    }
	  return NO_ERROR;
	}

      if (WIFEXITED (status))
	{
	  return WEXITSTATUS (status);
	}
    }
  return ER_GENERIC_ERROR;
}

/*
 * process_master -
 *
 * return:
 *
 *      command_type(in):
 */
static int
process_master (int command_type)
{
  int status = NO_ERROR;

  switch (command_type)
    {
    case ARG_CMD_START:
      {
	print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_2S,
		       PRINT_MASTER_NAME, PRINT_CMD_START);
	if (!css_does_master_exist ())
	  {
	    char argv0[] = UTIL_MASTER_NAME;
	    char *args[] = { argv0, NULL };
	    status = proc_execute (UTIL_MASTER_NAME, args, false, false,
				   false, NULL);
	    /* The master process needs a few seconds to bind port */
	    sleep (3);
	    status = css_does_master_exist ()? NO_ERROR : ER_GENERIC_ERROR;
	    print_result (PRINT_MASTER_NAME, status, command_type);
	  }
	else
	  {
	    status = ER_GENERIC_ERROR;
	    print_message (stdout, MSGCAT_UTIL_GENERIC_ALREADY_RUNNING_1S,
			   PRINT_MASTER_NAME);
	    util_log_write_errid (MSGCAT_UTIL_GENERIC_ALREADY_RUNNING_1S,
				  PRINT_MASTER_NAME);
	  }
      }
      break;
    case ARG_CMD_STOP:
      print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_2S,
		     PRINT_MASTER_NAME, PRINT_CMD_STOP);
      if (css_does_master_exist ())
	{
	  while (css_does_master_exist ())
	    {
	      if (commdb_master_shutdown (&master_Conn, 0) != NO_ERROR)
		{
		  break;
		}
	      THREAD_SLEEP (1000);
	    }
	  css_free_conn (master_Conn);
	  master_Conn = NULL;

	  print_result (PRINT_MASTER_NAME, NO_ERROR, command_type);
	}
      else
	{
	  status = ER_GENERIC_ERROR;
	  print_message (stdout, MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
			 PRINT_MASTER_NAME);
	  util_log_write_errid (MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
				PRINT_MASTER_NAME);
	}
      break;
    }
  return status;
}

/*
 * are_all_services_running - are all of services running
 *
 * return:
 *
 *      sleep_time(in):
 *
 * NOTE:
 */
static bool
are_all_services_running (unsigned int sleep_time)
{
  return check_all_services_status (sleep_time, ALL_SERVICES_RUNNING);
}

/*
 * are_all_services_stopped - are all of services stopped
 *
 * return:
 *
 *      sleep_time(in):
 *
 * NOTE:
 */
static bool
are_all_services_stopped (unsigned int sleep_time)
{
  return check_all_services_status (sleep_time, ALL_SERVICES_STOPPED);
}


/*
 * check_all_services_status - check all service status and compare with
			      expected_status, if not meet return false.
 *
 * return:
 *
 *      sleep_time(in):
 *      expected_status(in);
 * NOTE:
 */
static bool
check_all_services_status (UNUSED_ARG unsigned int sleep_time,
			   UTIL_ALL_SERVICES_STATUS expected_status)
{
  bool ret;
  bool broker_running;

  /* check whether rye_master is running */
  ret = css_does_master_exist ();
  if ((expected_status == ALL_SERVICES_RUNNING && !ret)
      || (expected_status == ALL_SERVICES_STOPPED && ret))
    {
      return false;
    }

  /* check whether rye_broker is running */
  if (broker_is_running (&broker_running) < 0)
    {
      return false;
    }

  if ((expected_status == ALL_SERVICES_RUNNING && broker_running == false) ||
      (expected_status == ALL_SERVICES_STOPPED && broker_running == true))
    {
      return false;
    }

  return true;
}


/*
 * process_service -
 *
 * return: error code
 *
 *      command_type(in):
 *
 * NOTE:
 */
static int
process_service (int command_type)
{
  int error = NO_ERROR;

  switch (command_type)
    {
    case ARG_CMD_START:
      if (!are_all_services_running (0))
	{
	  (void) process_master (command_type);

	  (void) process_broker (command_type, 0, NULL);
	  (void) process_heartbeat (command_type, 0, NULL);

	  error = are_all_services_running (0) ? NO_ERROR : ER_GENERIC_ERROR;
	}
      else
	{
	  print_message (stdout, MSGCAT_UTIL_GENERIC_ALREADY_RUNNING_1S,
			 PRINT_SERVICE_NAME);
	  util_log_write_errid (MSGCAT_UTIL_GENERIC_ALREADY_RUNNING_1S,
				PRINT_SERVICE_NAME);
	  error = ER_GENERIC_ERROR;
	}
      break;
    case ARG_CMD_STOP:
      if (!are_all_services_stopped (0))
	{
	  (void) process_broker (command_type, 0, NULL);
	  (void) process_heartbeat (command_type, 0, NULL);
	  (void) process_master (command_type);

	  error = are_all_services_stopped (0) ? NO_ERROR : ER_GENERIC_ERROR;
	}
      else
	{
	  print_message (stdout, MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
			 PRINT_SERVICE_NAME);
	  util_log_write_errid (MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
				PRINT_SERVICE_NAME);
	  error = ER_GENERIC_ERROR;
	}
      break;
    case ARG_CMD_RESTART:
      error = process_service (ARG_CMD_STOP);
      error = process_service (ARG_CMD_START);
      break;
    case ARG_CMD_STATUS:
      print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_2S,
		     PRINT_MASTER_NAME, PRINT_CMD_STATUS);
      if (css_does_master_exist ())
	{
	  print_message (stdout, MSGCAT_UTIL_GENERIC_ALREADY_RUNNING_1S,
			 PRINT_MASTER_NAME);
	}
      else
	{
	  print_message (stdout, MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
			 PRINT_MASTER_NAME);
	}

      {
	char arg0[] = "-b";
	char *args[] = { arg0 };

	(void) process_server (command_type, 0, NULL, false);
	(void) process_broker (command_type, 1, args);
	(void) process_heartbeat (command_type, 0, NULL);

	print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_2S,
		       PRINT_RYE_SHM_NAME, "");
	sysprm_load_and_init (NULL);
	rye_master_shm_dump (stdout);
      }
      break;
    default:
      error = ER_ARG_OUT_OF_RANGE;
    }

  return error;
}

/*
 * process_server -
 *
 * return:
 *
 *      command_type(in):
 *      argc(in):
 *      argv(in) :
 *      show_usage(in):
 *
 * NOTE:
 */
static int
process_server (int command_type, int argc, char **argv, bool show_usage)
{
  char buf[4096];
  int status = NO_ERROR;

  memset (buf, '\0', sizeof (buf));

  /* A string is copyed because strtok_r() modify an original string. */
  if (argc != 0)
    {
      strncpy (buf, argv[0], sizeof (buf) - 1);
    }

  if (command_type != ARG_CMD_STATUS && strlen (buf) == 0)
    {
      if (show_usage)
	{
	  util_log_write_errid (MSGCAT_UTIL_GENERIC_INVALID_CMD);
	}
      return ER_ARG_OUT_OF_RANGE;
    }

  switch (command_type)
    {
    case ARG_CMD_STATUS:
      print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_2S,
		     PRINT_SERVER_NAME, PRINT_CMD_STATUS);
      if (css_does_master_exist ())
	{
	  status = commdb_get_server_status (&master_Conn);
	}
      else
	{
	  print_message (stdout, MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
			 PRINT_MASTER_NAME);
	}
      break;
    case ARG_CMD_ACCESS_CONTROL:
      {
	if (argc != 2)
	  {
	    status = ER_ARG_OUT_OF_RANGE;
	    if (show_usage)
	      {
		util_log_write_errid (MSGCAT_UTIL_GENERIC_INVALID_CMD);
	      }
	    break;
	  }

	if (strcasecmp (argv[0], "reload") == 0)
	  {
	    char argv0[] = UTIL_RYE_NAME;
	    char argv1[] = UTIL_OPTION_ACLDB;
	    char argv2[] = ACLDB_RELOAD;
	    char *args[] = { argv0, argv1, argv2, argv[1], NULL };

	    status = util_admin (DIM (args) - 1, args, NULL);
	    print_result (PRINT_SERVER_NAME, status, command_type);
	  }
	else if (strcasecmp (argv[0], "status") == 0)
	  {
	    char argv0[] = UTIL_RYE_NAME;
	    char argv1[] = UTIL_OPTION_ACLDB;
	    char *args[] = { argv0, argv1, argv[1], NULL };

	    status = util_admin (DIM (args) - 1, args, NULL);
	  }
	else
	  {
	    status = ER_ARG_OUT_OF_RANGE;
	    if (show_usage)
	      {
		util_log_write_errid (MSGCAT_UTIL_GENERIC_INVALID_CMD);
	      }

	    break;
	  }
      }

      break;
    default:
      status = ER_ARG_OUT_OF_RANGE;
      break;
    }

  return status;
}

/*
 * process_broker -
 *
 * return:
 *
 *      command_type(in):
 *      name(in):
 *
 */
static int
process_broker (int command_type, int argc, char **argv)
{
  int status = NO_ERROR;
  bool broker_running;
  char program_name[] = PRINT_BROKER_NAME;
  char **argv_alloced = NULL;
  int i;

  if (broker_is_running (&broker_running) < 0)
    {
      status = ER_GENERIC_ERROR;
      print_result (PRINT_BROKER_NAME, ER_GENERIC_ERROR, command_type);
      return status;
    }

  argv_alloced = (char **) malloc (sizeof (char *) * (argc + 2));
  if (argv_alloced == NULL)
    {
      status = ER_GENERIC_ERROR;
      print_result (PRINT_BROKER_NAME, ER_GENERIC_ERROR, command_type);
      return status;
    }

  argv_alloced[0] = program_name;
  for (i = 0; i < argc; i++)
    {
      argv_alloced[i + 1] = argv[i];
    }
  argv_alloced[argc + 1] = NULL;

  argc++;
  argv = argv_alloced;

  switch (command_type)
    {
    case ARG_CMD_START:
      print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_2S,
		     PRINT_BROKER_NAME, PRINT_CMD_START);
      if (broker_running)
	{
	  status = ER_GENERIC_ERROR;
	  print_message (stdout, MSGCAT_UTIL_GENERIC_ALREADY_RUNNING_1S,
			 PRINT_BROKER_NAME);
	  util_log_write_errid (MSGCAT_UTIL_GENERIC_ALREADY_RUNNING_1S,
				PRINT_BROKER_NAME);
	}
      else
	{
	  status = broker_admin (ARG_CMD_START, argc, argv);

	  print_result (PRINT_BROKER_NAME, status, command_type);
	}
      break;
    case ARG_CMD_STOP:
      print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_2S,
		     PRINT_BROKER_NAME, PRINT_CMD_STOP);
      if (broker_running)
	{
	  status = broker_admin (ARG_CMD_STOP, argc, argv);

	  print_result (PRINT_BROKER_NAME, status, command_type);
	}
      else
	{
	  status = ER_GENERIC_ERROR;
	  print_message (stdout, MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
			 PRINT_BROKER_NAME);
	  util_log_write_errid (MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
				PRINT_BROKER_NAME);
	}
      break;
    case ARG_CMD_RESTART:
      status = process_broker (ARG_CMD_STOP, 0, NULL);
      status = process_broker (ARG_CMD_START, 0, NULL);
      break;
    case ARG_CMD_STATUS:
      {
	print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_2S,
		       PRINT_BROKER_NAME, PRINT_CMD_STATUS);
	if (broker_running)
	  {
	    status = broker_monitor (argc, argv);
	  }
	else
	  {
	    print_message (stdout, MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
			   PRINT_BROKER_NAME);
	  }
      }
      break;
    case ARG_CMD_ON:
      {
	if (argc <= 1)
	  {
	    status = ER_GENERIC_ERROR;
	    print_message (stdout, MSGCAT_UTIL_GENERIC_MISS_ARGUMENT);
	    util_log_write_errid (MSGCAT_UTIL_GENERIC_MISS_ARGUMENT);
	    break;
	  }

	status = broker_admin (ARG_CMD_ON, argc, argv);
      }
      break;
    case ARG_CMD_OFF:
      {
	if (argc <= 1)
	  {
	    status = ER_GENERIC_ERROR;
	    print_message (stdout, MSGCAT_UTIL_GENERIC_MISS_ARGUMENT);
	    util_log_write_errid (MSGCAT_UTIL_GENERIC_MISS_ARGUMENT);
	    break;
	  }

	status = broker_admin (ARG_CMD_OFF, argc, argv);
      }
      break;
    case ARG_CMD_ACCESS_CONTROL:
      {
	if (argc <= 1)
	  {
	    status = ER_ARG_OUT_OF_RANGE;
	    util_service_usage (ARG_UTIL_BROKER);
	    util_log_write_errid (MSGCAT_UTIL_GENERIC_INVALID_CMD);
	    break;
	  }

	status = broker_admin (ARG_CMD_ACCESS_CONTROL, argc, argv);

	print_result (PRINT_BROKER_NAME, status, command_type);
	break;
      }
    case ARG_CMD_RESET:
      {
	if (argc <= 1)
	  {
	    status = ER_GENERIC_ERROR;
	    print_message (stdout, MSGCAT_UTIL_GENERIC_MISS_ARGUMENT);
	    util_log_write_errid (MSGCAT_UTIL_GENERIC_MISS_ARGUMENT);
	    break;
	  }

	status = broker_admin (ARG_CMD_RESET, argc, argv);
      }
      break;

    case ARG_CMD_INFO:
      {
	status = broker_admin (ARG_CMD_INFO, argc, argv);
      }
      break;

    case ARG_CMD_TEST:
      {
	print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_2S,
		       PRINT_BROKER_NAME, PRINT_CMD_TEST);
	if (broker_running)
	  {
	    status = broker_tester (argc, argv);
	  }
	else
	  {
	    print_message (stdout, MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
			   PRINT_BROKER_NAME);
	  }
      }
      break;

    default:
      status = ER_ARG_OUT_OF_RANGE;
      break;
    }

  free_and_init (argv_alloced);
  return status;
}

/*
 * us_hb_activate - activate Rye heartbeat
 *    return:
 *
 */
static int
us_hb_activate (void)
{
  int error = NO_ERROR;
  bool success;

  /*
   * activate messages
   * 1. ACTIVATE_HEARTBEAT->IS_REGISTERED_HA_PROCS
   */

  if (!css_does_master_exist ())
    {
      error = ER_GENERIC_ERROR;
      print_message (stdout, MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
		     PRINT_MASTER_NAME);
      util_log_write_errid (MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
			    PRINT_MASTER_NAME);

      return error;
    }

  error = commdb_activate_heartbeat (&master_Conn);
  if (error != NO_ERROR)
    {
      return error;
    }

  /* wait until all processes are registered */
  while (true)
    {
      error = commdb_is_registered_procs (&master_Conn, &success);
      if (error != NO_ERROR)
	{
	  return error;
	}

      if (success)
	{
	  break;
	}
      else
	{
	  sleep (1);
	}
    }

  return NO_ERROR;
}

/*
 * us_hb_reload - reload Rye heartbeat
 *    return:
 *
 */
static int
us_hb_reload (void)
{
  int error = NO_ERROR;
  bool success;
  INT64 old_ha_node_reset_time, new_ha_node_reset_time;

  /*
   * activate messages
   * 1. RECOFIG_HEARTBEAT->IS_REGISTERED_HA_PROCS
   */

  if (!css_does_master_exist ())
    {
      error = ER_GENERIC_ERROR;
      print_message (stdout, MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
		     PRINT_MASTER_NAME);
      util_log_write_errid (MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
			    PRINT_MASTER_NAME);

      return error;
    }

  error = rye_master_shm_get_node_reset_time (&old_ha_node_reset_time);
  if (error != NO_ERROR)
    {
      return error;
    }

  error = commdb_reconfig_heartbeat (&master_Conn);
  if (error != NO_ERROR)
    {
      return error;
    }

  /* wait until reconfig job is started.  */
  do
    {
      THREAD_SLEEP (1000);
      rye_master_shm_get_node_reset_time (&new_ha_node_reset_time);
    }
  while (old_ha_node_reset_time == new_ha_node_reset_time);


  /* wait until all processes are registered */
  while (true)
    {
      error = commdb_is_registered_procs (&master_Conn, &success);
      if (error != NO_ERROR)
	{
	  return error;
	}

      if (success)
	{
	  break;
	}
      else
	{
	  sleep (1);
	}
    }

  return NO_ERROR;
}

/*
 * us_hb_changemode - change Rye heartbeat node
 *    return:
 *
 */
static int
us_hb_changemode (HA_STATE req_node_state, bool force)
{
  int error = NO_ERROR;

  /*
   * activate messages
   * 1. RECOFIG_HEARTBEAT->IS_REGISTERED_HA_PROCS
   */

  if (!css_does_master_exist ())
    {
      error = ER_GENERIC_ERROR;
      print_message (stdout, MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
		     PRINT_MASTER_NAME);
      util_log_write_errid (MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
			    PRINT_MASTER_NAME);

      return error;
    }

  error = commdb_changemode (&master_Conn, req_node_state, force);
  if (error != NO_ERROR)
    {
      return error;
    }

  return NO_ERROR;
}

/*
 * us_hb_deactivate - deactivate Rye heartbeat
 *    return:
 *
 *    hostname(in): target hostname
 *    immediate_stop(in):
 */
static int
us_hb_deactivate (const PRM_NODE_INFO * node_info, bool immediate_stop)
{
  CSS_CONN_ENTRY *tmp_master_conn = NULL;
  unsigned short rid;
  int status = NO_ERROR;
  bool success;
  PRM_NODE_INFO myself_node_info = prm_get_myself_node_info ();

  if (node_info == NULL)
    {
      node_info = &myself_node_info;
    }

  /*
   * deactivate messages
   * 1. DEACT_STOP_ALL->DEACT_CONFIRM_STOP_ALL
   * 2. DEACTIVATE_HEARTBEAT->DEACT_CONFIRM_NO_SERVER
   */

  tmp_master_conn = css_connect_to_master_for_info (node_info, &rid);
  if (tmp_master_conn == NULL)
    {
      return ER_FAILED;
    }

  /* stop all HA processes including rye_server */
  status = commdb_deact_stop_all (tmp_master_conn, immediate_stop);
  if (status != NO_ERROR)
    {
      css_free_conn (tmp_master_conn);
      return status;
    }

  /* wait until all processes are shutdown */
  while (true)
    {
      status = commdb_deact_confirm_stop_all (tmp_master_conn, &success);
      if (status != NO_ERROR)
	{
	  css_free_conn (tmp_master_conn);
	  return status;
	}

      if (success)
	{
	  break;
	}
      else
	{
	  sleep (1);
	}
    }

  /* start deactivation */
  status = commdb_deactivate_heartbeat (tmp_master_conn);
  if (status != NO_ERROR)
    {
      css_free_conn (tmp_master_conn);
      return status;
    }

  /* wait until no rye_server processes are running */
  while (true)
    {
      status = commdb_deact_confirm_no_server (tmp_master_conn, &success);
      if (status != NO_ERROR)
	{
	  css_free_conn (tmp_master_conn);
	  return status;
	}

      if (success)
	{
	  break;
	}
      else
	{
	  sleep (1);
	}
    }

  css_free_conn (tmp_master_conn);
  return NO_ERROR;
}

/*
 * process_heartbeat_start -
 *
 * return:
 *
 *      ha_conf(in):
 *      argc(in):
 *      argv(in):
 *
 */
static int
process_heartbeat_start (UNUSED_ARG HA_CONF * ha_conf, UNUSED_ARG int argc,
			 UNUSED_ARG char **argv)
{
  int error = NO_ERROR;

  print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_2S,
		 PRINT_HEARTBEAT_NAME, PRINT_CMD_START);

  error = us_hb_activate ();

  print_result (PRINT_HEARTBEAT_NAME, error, ARG_CMD_START);
  return error;
}

/*
 * process_heartbeat_stop -
 *
 * return:
 *
 *      ha_conf(in):
 *      argc(in):
 *      argv(in):
 *
 */
static int
process_heartbeat_stop (UNUSED_ARG HA_CONF * ha_conf, UNUSED_ARG int argc,
			UNUSED_ARG char **argv)
{
  int status = NO_ERROR;
  int hb_argc;
  int opt, opt_idx = 0;
  char opt_str[64];
  char hostname[MAXHOSTNAMELEN] = "";
  char **hb_args = NULL;
  char hb_arg0[] = PRINT_HEARTBEAT_NAME " " PRINT_CMD_STOP;
  bool immediate_stop = false;
  PRM_NODE_INFO node_info = prm_get_myself_node_info ();

  struct option hb_stop_opts[] = {
    {HB_STOP_HB_DEACT_IMMEDIATELY_L, 0, 0, HB_STOP_HB_DEACT_IMMEDIATELY_S},
    {HB_STOP_HOST_L, 1, 0, HB_STOP_HOST_S},
    {0, 0, 0, 0}
  };

  print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_2S,
		 PRINT_HEARTBEAT_NAME, PRINT_CMD_STOP);

  /* prog name + user given args */
  hb_argc = 1 + argc;

  /* +1 for null termination */
  hb_args = (char **) malloc ((hb_argc + 1) * sizeof (char *));
  if (hb_args == NULL)
    {
      status = ER_GENERIC_ERROR;
      print_message (stderr, MSGCAT_UTIL_GENERIC_NO_MEM);
      util_log_write_errid (MSGCAT_UTIL_GENERIC_NO_MEM);
      goto ret;
    }

  memset (hb_args, 0, (hb_argc + 1) * sizeof (char *));

  hb_args[0] = hb_arg0;
  memcpy (&hb_args[1], argv, argc * sizeof (char *));

  utility_make_getopt_optstring (hb_stop_opts, opt_str);
  while ((opt = getopt_long (hb_argc, hb_args, opt_str, hb_stop_opts,
			     &opt_idx)) != -1)
    {
      switch (opt)
	{
	case HB_STOP_HOST_S:
	  /* TODO fix me. hostname to PRM_NODE_INFO - cgkang */
	  strncpy (hostname, optarg, sizeof (hostname) - 1);
	  break;
	case HB_STOP_HB_DEACT_IMMEDIATELY_S:
	  immediate_stop = true;
	  break;
	default:
	  status = ER_GENERIC_ERROR;
	  util_log_write_errid (MSGCAT_UTIL_GENERIC_INVALID_ARGUMENT);
	  break;
	}
    }

  if (status == NO_ERROR && hb_argc > optind)
    {
      /* -h, -i options do not take a non-option argument */
      if (hostname[0] != '\0' || immediate_stop == true)
	{
	  status = ER_GENERIC_ERROR;
	  print_message (stderr, MSGCAT_UTIL_GENERIC_ARGS_OVER,
			 hb_args[optind]);
	  util_log_write_errid (MSGCAT_UTIL_GENERIC_ARGS_OVER,
				hb_args[optind]);
	}
      else if (hb_argc - optind > 1)
	{
	  status = ER_GENERIC_ERROR;
	  print_message (stderr, MSGCAT_UTIL_GENERIC_ARGS_OVER,
			 hb_args[optind + 1]);
	  util_log_write_errid (MSGCAT_UTIL_GENERIC_ARGS_OVER,
				hb_args[optind + 1]);
	}
    }

  if (hb_args != NULL)
    {
      free_and_init (hb_args);
    }

  if (status != NO_ERROR)
    {
      goto ret;
    }

  status = us_hb_deactivate (&node_info, immediate_stop);

  if (status == NO_ERROR)
    {
      /* wait for rye_master to clean up its internal resources */
      THREAD_SLEEP (HB_STOP_WAITING_TIME);
    }

ret:
  print_result (PRINT_HEARTBEAT_NAME, status, ARG_CMD_STOP);
  return status;
}

/*
 * process_heartbeat_status -
 *
 * return:
 *
 *      argc(in):
 *      argv(in):
 *
 */
static int
process_heartbeat_status (int argc, char **argv)
{
  int status = NO_ERROR;

  print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_2S,
		 PRINT_HEARTBEAT_NAME, PRINT_CMD_STATUS);

  if (css_does_master_exist ())
    {
      bool verbose = false;

      if (argc == 1 && strcmp (argv[0], "-v") == 0)
	{
	  verbose = true;
	}
      else if (argc > 0)
	{
	  print_message (stdout, MSGCAT_UTIL_GENERIC_INVALID_ARGUMENT);
	  util_log_write_errid (MSGCAT_UTIL_GENERIC_INVALID_ARGUMENT);
	  return ER_GENERIC_ERROR;
	}

      status = commdb_ha_node_info_query (&master_Conn, verbose);
      if (status != NO_ERROR)
	{
	  return status;
	}

      status = commdb_ha_process_info_query (&master_Conn, verbose);
      if (status != NO_ERROR)
	{
	  return status;
	}

      status = commdb_ha_ping_host_info_query (&master_Conn);
      if (status != NO_ERROR)
	{
	  return status;
	}

      if (verbose == true)
	{
	  status = commdb_ha_admin_info_query (&master_Conn);
	}
    }
  else
    {
      status = ER_GENERIC_ERROR;
      print_message (stdout, MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S,
		     PRINT_MASTER_NAME);
    }

  return status;
}

/*
 * process_heartbeat_reload -
 *
 * return:
 *
 *      argc(in):
 *      argv(in):
 *
 */
static int
process_heartbeat_reload (UNUSED_ARG int argc, UNUSED_ARG char **argv)
{
  int error = NO_ERROR;

  print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_2S,
		 PRINT_HEARTBEAT_NAME, PRINT_CMD_RELOAD);

  error = us_hb_reload ();

  print_result (PRINT_HEARTBEAT_NAME, error, ARG_CMD_RELOAD);
  return error;
}

/*
 * process_heartbeat_changemode -
 *
 * return:
 *
 *      argc(in):
 *      argv(in):
 *
 */
static int
process_heartbeat_changemode (int argc, char **argv)
{
  int error = NO_ERROR;
  HA_STATE req_node_state;
  bool force;

  print_message (stdout, MSGCAT_UTIL_GENERIC_START_STOP_2S,
		 PRINT_HEARTBEAT_NAME, PRINT_CMD_CHANGEMODE);

  if (argc != 1)
    {
      error = ER_GENERIC_ERROR;
      util_log_write_errid (MSGCAT_UTIL_GENERIC_INVALID_ARGUMENT);
      return error;
    }

  if (strcasecmp (argv[0], "slave") == 0)
    {
      req_node_state = HA_STATE_SLAVE;
      force = false;
    }
  else if (strcasecmp (argv[0], "force") == 0)
    {
      req_node_state = HA_STATE_UNKNOWN;
      force = true;
    }
  else
    {
      error = ER_GENERIC_ERROR;
      util_log_write_errid (MSGCAT_UTIL_GENERIC_INVALID_ARGUMENT);
      return error;
    }

  error = us_hb_changemode (req_node_state, force);

  print_result (PRINT_HEARTBEAT_NAME, error, ARG_CMD_CHANGEMODE);
  return error;
}

/*
 * process_heartbeat -
 *
 * return:
 *
 *      command_type(in):
 *      argc(in):
 *      argv(in):
 *
 */
static int
process_heartbeat (int command_type, int argc, char **argv)
{
  int status = NO_ERROR;
  HA_CONF ha_conf;
  bool broker_running;

  memset ((void *) &ha_conf, 0, sizeof (HA_CONF));
  status = util_make_ha_conf (&ha_conf);
  if (status != NO_ERROR)
    {
      print_message (stderr, MSGCAT_UTIL_GENERIC_SERVICE_PROPERTY_FAIL);
      util_log_write_errid (MSGCAT_UTIL_GENERIC_SERVICE_PROPERTY_FAIL);
      print_result (PRINT_HEARTBEAT_NAME, ER_FAILED, command_type);
      goto ret;			/* is ERROR */
    }

  switch (command_type)
    {
    case ARG_CMD_START:
      if (!css_does_master_exist ())
	{
	  status = process_master (command_type);
	  if (status != NO_ERROR)
	    {
	      break;		/* escape switch */
	    }
	}
      if (broker_is_running (&broker_running) < 0)
	{
	  status = ER_GENERIC_ERROR;
	  print_result (PRINT_BROKER_NAME, ER_GENERIC_ERROR, command_type);
	  break;
	}
      if (broker_running == false)
	{
	  (void) process_broker (command_type, 0, NULL);
	}
      status = process_heartbeat_start (&ha_conf, argc, argv);
      break;
    case ARG_CMD_STOP:
      status = process_heartbeat_stop (&ha_conf, argc, argv);
      break;
    case ARG_CMD_STATUS:
    case ARG_CMD_LIST:
      status = process_heartbeat_status (argc, argv);
      break;
    case ARG_CMD_RELOAD:
      status = process_heartbeat_reload (argc, argv);
      break;
    case ARG_CMD_CHANGEMODE:
      status = process_heartbeat_changemode (argc, argv);
      break;
    default:
      util_service_usage (ARG_UTIL_HEARTBEAT);
      status = ER_ARG_OUT_OF_RANGE;
      break;
    }

ret:
  util_free_ha_conf (&ha_conf);
  return status;
}

/*
 * parse_arg -
 *
 * return:
 *
 *      option(in):
 *      arg(in):
 *
 */
static ARG_TYPE
parse_arg (UTIL_ARG_MAP_T * option, const char *arg)
{
  int i;

  if (arg == NULL || arg[0] == 0)
    {
      return ARG_UNKNOWN;
    }
  for (i = 0; option[i].arg_type != ARG_END; i++)
    {
      if (strcasecmp (option[i].arg_name, arg) == 0)
	{
	  return option[i].arg_type;
	}
    }
  return ARG_UNKNOWN;
}

/*
 * load_properties -
 *
 * return:
 *
 * NOTE:
 */
static int
load_properties (void)
{
  return sysprm_load_and_init (NULL);
}
