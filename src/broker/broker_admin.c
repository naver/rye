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
 * broker_admin.c -
 */

#ident "$Id$"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <assert.h>

#include "cas_common.h"
#include "broker_admin_pub.h"
#include "broker_config.h"
#include "broker_shm.h"

#include "broker_util.h"
#include "util_func.h"
#include "error_manager.h"
#include "language_support.h"
#include "utility.h"
#include "dbi.h"

#define	TRUE			1
#define	FALSE			0

static void
admin_log_write (const char *log_file, const char *msg)
{
  FILE *fp;
  char time_array[256];

  fp = fopen (log_file, "a");
  if (fp != NULL)
    {
      (void) er_datetime (NULL, time_array, sizeof (time_array));
      fprintf (fp, "%s %s\n", time_array, msg);
      fclose (fp);
    }
  else
    {
      printf ("cannot open admin log file [%s]\n", log_file);
    }
}

int
broker_admin (int command_type, int argc, char **argv)
{
  T_BROKER_INFO br_info[MAX_BROKER_NUM];
  char admin_log_file[BROKER_PATH_MAX];
  int num_broker, shm_key_br_gl;
  int err = 0;
  char msg_buf[256];

  argc--;
  argv++;

  if (argc >= 1 && strcmp (argv[0], "--version") == 0)
    {
      printf ("VERSION %s\n", makestring (BUILD_NUMBER));
      return 0;
    }

  err = broker_config_read (br_info, &num_broker, &shm_key_br_gl,
			    admin_log_file, 1);
  if (err < 0)
    {
      util_log_write_errstr ("broker config read error.\n");
      return -1;
    }

#if 0
  if (admin_get_host_ip ())
    {
      PRINT_AND_LOG_ERR_MSG ("%s\n", admin_Err_msg);
      return -1;
    }
#endif
  admin_Err_msg[0] = '\0';

  admin_init_env ();

  if (command_type == ARG_CMD_START)
    {
      /* change the working directory to $RYE/bin */
      ut_cd_work_dir ();

      if (argc >= 1 && strncmp (argv[0], "local_mgmt_port=", 16) == 0)
	{
	  int local_mgmt_port = 0;
	  const char *port_str_value;

	  port_str_value = argv[0] + 16;
	  parse_int (&local_mgmt_port, port_str_value, 0);
	  if (local_mgmt_port > 0)
	    {
	      T_BROKER_INFO *br_local_mgmt;
	      br_local_mgmt = ut_find_broker (br_info, num_broker,
					      BR_LOCAL_MGMT_NAME, LOCAL_MGMT);
	      if (br_local_mgmt == NULL)
		{
		  assert (false);
		}
	      else
		{
		  err = db_update_persist_conf_file ("broker",
						     BROKER_SECTION_NAME,
						     "broker_port",
						     port_str_value);
		  if (err != NO_ERROR)
		    {
		      PRINT_AND_LOG_ERR_MSG ("Cannot update conf file\n");
		      return -1;
		    }

		  br_local_mgmt->port = local_mgmt_port;
		}
	    }
	}

      if (rye_shm_is_used_key (shm_key_br_gl) == false)
	{
	  if (admin_start_cmd (br_info, num_broker, shm_key_br_gl) < 0)
	    {
	      PRINT_AND_LOG_ERR_MSG ("%s\n", admin_Err_msg);
	      return -1;
	    }
	  else
	    {
	      admin_log_write (admin_log_file, "start");
	    }
	}
      else
	{
	  PRINT_AND_LOG_ERR_MSG ("Error: RYE Broker is already running "
				 "with shared memory key '%x'.\n",
				 shm_key_br_gl);
	}
    }
  else if (command_type == ARG_CMD_STOP)
    {
      if (admin_stop_cmd (shm_key_br_gl))
	{
	  PRINT_AND_LOG_ERR_MSG ("%s\n", admin_Err_msg);
	  return -1;
	}
      else
	{
	  admin_log_write (admin_log_file, "stop");
	}
    }
  else if (command_type == ARG_CMD_ON)
    {
      /* change the working directory to $RYE/bin */
      ut_cd_work_dir ();

      if (argc < 1)
	{
	  PRINT_AND_LOG_ERR_MSG ("%s on <broker-name>\n", PRINT_BROKER_NAME);
	  return -1;
	}
      if (admin_on_cmd (shm_key_br_gl, argv[0]) < 0)
	{
	  PRINT_AND_LOG_ERR_MSG ("%s\n", admin_Err_msg);
	  return -1;
	}
      else
	{
	  sprintf (msg_buf, "%s on", argv[0]);
	  admin_log_write (admin_log_file, msg_buf);
	}
    }
  else if (command_type == ARG_CMD_OFF)
    {
      if (argc < 1)
	{
	  PRINT_AND_LOG_ERR_MSG ("%s on <broker-name>\n", PRINT_BROKER_NAME);
	  return -1;
	}
      if (admin_off_cmd (shm_key_br_gl, argv[0]) < 0)
	{
	  PRINT_AND_LOG_ERR_MSG ("%s\n", admin_Err_msg);
	  return -1;
	}
      else
	{
	  sprintf (msg_buf, "%s off", argv[0]);
	  admin_log_write (admin_log_file, msg_buf);
	}
    }
  else if (command_type == ARG_CMD_RESET)
    {
      if (argc < 1)
	{
	  PRINT_AND_LOG_ERR_MSG ("%s reset <broker-name>\n",
				 PRINT_BROKER_NAME);
	  return -1;
	}
      if (admin_reset_cmd (shm_key_br_gl, argv[0]) < 0)
	{
	  PRINT_AND_LOG_ERR_MSG ("%s\n", admin_Err_msg);
	  return -1;
	}
      else
	{
	  sprintf (msg_buf, "%s reset", argv[0]);
	  admin_log_write (admin_log_file, msg_buf);
	}
    }
  else if (command_type == ARG_CMD_INFO)
    {
      if (admin_info_cmd (shm_key_br_gl) < 0)
	{
	  PRINT_AND_LOG_ERR_MSG ("%s\n", admin_Err_msg);
	  return -1;
	}
      else
	{
	  admin_log_write (admin_log_file, "info");
	}
    }
  else if (command_type == ARG_CMD_ACCESS_CONTROL)
    {
      int err_code;

      if (argc < 1)
	{
	  goto acl_cmd_usage;
	}

      if (strcasecmp (argv[0], "reload") == 0)
	{
	  char *new_acl_file = (argc > 1 ? argv[1] : NULL);
	  err_code = admin_acl_reload_cmd (shm_key_br_gl, new_acl_file);
	}
      else if (strcasecmp (argv[0], "status") == 0)
	{
	  char *br_name = (argc > 1 ? argv[1] : NULL);
	  err_code = admin_acl_status_cmd (shm_key_br_gl, br_name);
	}
      else if (strcasecmp (argv[0], "test") == 0)
	{
	  if (argc < 2)
	    {
	      goto acl_cmd_usage;
	    }

	  err_code = admin_acl_test_cmd (argc - 1, argv + 1);
	}
      else
	{
	  goto acl_cmd_usage;
	}
      if (err_code < 0)
	{
	  PRINT_AND_LOG_ERR_MSG ("%s\n", admin_Err_msg);
	  return -1;
	}
    }
  else
    {
      goto usage;
    }

  return 0;

acl_cmd_usage:
  fprintf (stderr, "usage: %s acl reload [new-acl-file]\n"
	   "       %s acl status [broker-name]\n"
	   "       %s acl test test-acl-file \n",
	   PRINT_BROKER_NAME, PRINT_BROKER_NAME, PRINT_BROKER_NAME);
  return -1;
usage:
  fprintf (stderr, "%s (start | stop | add | drop | restart \
	    | on | off | reset | info | acl )\n", PRINT_BROKER_NAME);
  return -1;
}

int
broker_is_running (bool * running)
{
  int err;
  int shm_key_br_gl;

  err = broker_config_read (NULL, NULL, &shm_key_br_gl, NULL, 0);
  if (err < 0)
    {
      return -1;
    }

  *running = rye_shm_is_used_key (shm_key_br_gl);
  return 0;
}

int
broker_get_local_mgmt_info (char **broker_key, int *port)
{
  int error;
  int shm_key_br_gl;

  error = broker_config_read (NULL, NULL, &shm_key_br_gl, NULL, 0);
  if (error < 0)
    {
      return -1;
    }

  error = admin_get_broker_key_and_portid (broker_key, port, shm_key_br_gl,
					   BR_LOCAL_MGMT_NAME, LOCAL_MGMT);
  if (error < 0)
    {
      return -1;
    }

  return 0;
}
