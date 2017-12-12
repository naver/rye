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
 * broker_admin_pub.c -
 */

#ident "$Id$"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <signal.h>
#include <time.h>
#include <errno.h>
#include <fcntl.h>
#include <assert.h>

#include <unistd.h>
#include <sys/time.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <math.h>

#if defined(LINUX)
#include <sys/resource.h>
#endif /* LINUX */

#include "porting.h"
#include "cas_common.h"
#include "broker_shm.h"
#include "broker_util.h"
#include "broker_env_def.h"
#include "broker_admin_pub.h"
#include "broker_acl.h"
#include "chartype.h"
#include "error_manager.h"
#include "cas_error.h"
#include "environment_variable.h"

#include "dbdef.h"

#define ADMIN_ERR_MSG_SIZE	1024

#define MAKE_VERSION(MAJOR, MINOR)	(((MAJOR) << 8) | (MINOR))

#define MEMBER_SIZE(TYPE, MEMBER) ((int) sizeof(((TYPE *)0)->MEMBER))
#define NUM_OF_DIGITS(NUMBER) (int)log10(NUMBER) + 1

static int br_activate (T_BROKER_INFO * br_info, int shm_key_br_gl,
			T_SHM_BROKER * shm_br, int br_index);
static int br_inactivate (T_BROKER_INFO *);
static void as_activate (T_SHM_BROKER * shm_br, T_BROKER_INFO * br_info,
			 T_SHM_APPL_SERVER * shm_as_p,
			 T_APPL_SERVER_INFO * as_info_p, int as_idex,
			 char **env, int env_num);
static void as_inactivate (T_APPL_SERVER_INFO * as_info_p, char *broker_name);

static void free_env (char **env, int env_num);
static char **make_env (char *env_file, int *env_num);

static void get_br_init_err_msg (char *msg_buf, int buf_size, int err_code,
				 int os_err_code);
static int admin_acl_test_cmd_internal (const char *acl_filename, int argc,
					char **argv, FILE * out_fp);

char admin_Err_msg[ADMIN_ERR_MSG_SIZE];
#define SET_ADMIN_ERR_MSG(...)		\
	snprintf (admin_Err_msg, sizeof(admin_Err_msg), __VA_ARGS__)

static void
create_log_dir (const char *br_name, char broker_type)
{
  char path[BROKER_PATH_MAX];

  if (broker_type == NORMAL_BROKER)
    {
      envvar_ryelog_broker_sqllog_file (path, sizeof (path), br_name, NULL);
      rye_mkdir (path, 0755);
      envvar_ryelog_broker_slowlog_file (path, sizeof (path), br_name, NULL);
      rye_mkdir (path, 0755);
      envvar_ryelog_broker_errorlog_file (path, sizeof (path), br_name, NULL);
      rye_mkdir (path, 0755);
    }
  else
    {
      envvar_ryelog_broker_file (path, sizeof (path), br_name, NULL);
      rye_mkdir (path, 0755);
    }
}

int
admin_start_cmd (T_BROKER_INFO * br_info, int br_num, int shm_key_br_gl)
{
  int i;
  int res = 0;
  char path[BROKER_PATH_MAX];
  T_SHM_BROKER *shm_br;
  T_SHM_APPL_SERVER *shm_as_p = NULL;
  T_BROKER_TYPE start_order[] = { NORMAL_BROKER, LOCAL_MGMT, SHARD_MGMT };
  unsigned int order_idx;
  bool *broker_started;
  char br_acl_file[BROKER_PATH_MAX];
  BROKER_ACL_CONF br_acl_conf;
  bool has_shard_mgmt = false;

  if (br_num <= 0)
    {
      SET_ADMIN_ERR_MSG ("Cannot start RYE Broker. (number of broker is 0)");
      return -1;
    }

  broker_started = alloca (sizeof (bool) * br_num);
  if (broker_started == NULL)
    {
      return -1;
    }
  memset (broker_started, 0, sizeof (bool) * br_num);

  chdir ("..");
  rye_mkdir (envvar_vardir_file (path, sizeof (path), ""), 0755);
  rye_mkdir (envvar_tmpdir_file (path, sizeof (path), ""), 0755);
  rye_mkdir (envvar_as_pid_dir_file (path, sizeof (path), ""), 0755);
  rye_mkdir (envvar_socket_file (path, sizeof (path), ""), 0755);


  for (i = 0; i < br_num; i++)
    {
      /*prevent the broker from hanging due to an excessively long path
       * socket path length = sock_path[broker_name].[as_index]
       */
      if (strlen (path) + strlen (br_info[i].name) + 1 +
	  NUM_OF_DIGITS (br_info[i].appl_server_max_num) >
	  MEMBER_SIZE (struct sockaddr_un, sun_path) - 1)
	{
	  SET_ADMIN_ERR_MSG ("The socket path is too long (>%d): %s",
			     MEMBER_SIZE (struct sockaddr_un, sun_path),
			     path);
	  return -1;
	}

      create_log_dir (br_info[i].name, br_info[i].broker_type);

      if (br_info[i].broker_type == SHARD_MGMT)
	{
	  has_shard_mgmt = true;
	}
    }
  chdir (envvar_bindir_file (path, BROKER_PATH_MAX, ""));

  /* create master shared memory */
  shm_br = br_shm_init_shm_broker (shm_key_br_gl, br_info, br_num);

  if (shm_br == NULL)
    {
      SET_ADMIN_ERR_MSG ("failed to initialize broker shared memory");
      return -1;
    }

  envvar_broker_acl_file (br_acl_file, sizeof (br_acl_file));
  if (br_acl_read_config_file (&br_acl_conf, br_acl_file, !has_shard_mgmt,
			       admin_Err_msg) < 0)
    {
      res = -1;
      goto end;
    }

  for (order_idx = 0; res == 0 && order_idx < DIM (start_order); order_idx++)
    {
      for (i = 0; i < br_num; i++)
	{
	  shm_as_p = NULL;

	  if (start_order[order_idx] !=
	      (T_BROKER_TYPE) br_info[i].broker_type)
	    {
	      continue;
	    }
	  if (br_info[i].service_flag != ON)
	    {
	      continue;
	    }

	  shm_br->br_info[i].appl_server_shm_key = shm_key_br_gl + i + 1;

	  shm_as_p =
	    br_shm_init_shm_as (&(shm_br->br_info[i]), shm_key_br_gl);
	  if (shm_as_p == NULL)
	    {
	      SET_ADMIN_ERR_MSG
		("%s: failed to initialize appl server shared memory.",
		 br_info[i].name);

	      res = -1;
	      break;
	    }

	  broker_started[i] = true;

	  if (br_acl_init_shm (shm_as_p, &shm_br->br_info[i],
			       shm_br, admin_Err_msg, &br_acl_conf) < 0)
	    {
	      res = -1;
	      break;
	    }

	  res = br_activate (&(shm_br->br_info[i]), shm_key_br_gl, shm_br, i);

	  if (res < 0)
	    {
	      break;
	    }

	  if (shm_as_p)
	    {
	      rye_shm_detach (shm_as_p);
	    }

	  res = 0;
	}
    }

end:
  if (res < 0)
    {
      char err_msg_backup[ADMIN_ERR_MSG_SIZE];
      memcpy (err_msg_backup, admin_Err_msg, ADMIN_ERR_MSG_SIZE);

      for (i = 0; i < br_num; i++)
	{
	  if (broker_started[i])
	    {
	      br_inactivate (&(shm_br->br_info[i]));
	      rye_shm_destroy ((shm_br->br_info[i].appl_server_shm_key));
	    }
	}

      memcpy (admin_Err_msg, err_msg_backup, ADMIN_ERR_MSG_SIZE);
    }
  if (shm_br)
    {
      rye_shm_detach (shm_br);
    }

  if (res < 0)
    {
      rye_shm_destroy (shm_key_br_gl);

      if (shm_as_p)
	{
	  rye_shm_detach (shm_as_p);
	}
    }

  return res;
}

int
admin_stop_cmd (int shm_key_br_gl)
{
  T_SHM_BROKER *shm_br;
  int i, res;
  char err_msg_backup[ADMIN_ERR_MSG_SIZE];

  shm_br = rye_shm_attach (shm_key_br_gl, RYE_SHM_TYPE_BROKER_GLOBAL, false);
  if (shm_br == NULL)
    {
      SET_ADMIN_ERR_MSG ("Cannot open shared memory (shmkey:%x)",
			 shm_key_br_gl);
      return -1;
    }

  if (shm_br->owner_uid != getuid ())
    {
      SET_ADMIN_ERR_MSG ("Cannot stop RYE Broker. (Not owner)\n");
      return -1;
    }

  memcpy (err_msg_backup, admin_Err_msg, ADMIN_ERR_MSG_SIZE);

  for (i = 0; i < MAX_BROKER_NUM && i < shm_br->num_broker; i++)
    {
      if (shm_br->br_info[i].service_flag == ON)
	{
	  res = br_inactivate (&(shm_br->br_info[i]));
	  if (res < 0)
	    {
	      SET_ADMIN_ERR_MSG ("Cannot inactivate broker [%s]",
				 shm_br->br_info[i].name);
	      rye_shm_detach (shm_br);
	      return -1;
	    }
	  else
	    {
	      rye_shm_destroy ((shm_br->br_info[i].appl_server_shm_key));
	    }
	}
    }

  memcpy (admin_Err_msg, err_msg_backup, ADMIN_ERR_MSG_SIZE);

  memset (shm_br->broker_key, 0, sizeof (shm_br->broker_key));

  rye_shm_destroy (shm_key_br_gl);

  return 0;
}

int
admin_on_cmd (int shm_key_br_gl, const char *broker_name)
{
  int i, res = 0;
  T_SHM_BROKER *shm_br;
  T_SHM_APPL_SERVER *shm_as_p = NULL;
  char br_acl_file[BROKER_PATH_MAX];
  BROKER_ACL_CONF br_acl_conf;

  shm_br = rye_shm_attach (shm_key_br_gl, RYE_SHM_TYPE_BROKER_GLOBAL, false);
  if (shm_br == NULL)
    {
      SET_ADMIN_ERR_MSG ("Cannot open shared memory (shmkey:%x)",
			 shm_key_br_gl);
      return -1;
    }

  envvar_broker_acl_file (br_acl_file, sizeof (br_acl_file));
  if (br_acl_read_config_file
      (&br_acl_conf, br_acl_file, false, admin_Err_msg) < 0)
    {
      rye_shm_detach (shm_br);
      return -1;
    }

  for (i = 0; i < shm_br->num_broker; i++)
    {
      if (strcmp (shm_br->br_info[i].name, broker_name) == 0)
	{
	  if (shm_br->br_info[i].service_flag == ON)
	    {
	      SET_ADMIN_ERR_MSG ("Broker[%s] is already running",
				 broker_name);
	      rye_shm_detach (shm_br);
	      return -1;
	    }
	  else
	    {
	      shm_as_p = br_shm_init_shm_as (&(shm_br->br_info[i]),
					     shm_key_br_gl);
	      if (shm_as_p == NULL)
		{
		  SET_ADMIN_ERR_MSG ("%s: cannot create shared memory",
				     broker_name);

		  res = -1;
		  break;
		}

	      if (br_acl_init_shm (shm_as_p, &shm_br->br_info[i],
				   shm_br, admin_Err_msg, &br_acl_conf) < 0)
		{
		  res = -1;
		  break;
		}

	      res = br_activate (&(shm_br->br_info[i]), shm_key_br_gl, shm_br,
				 i);
	    }
	  break;
	}
    }

  if (res < 0)
    {
      char err_msg_backup[ADMIN_ERR_MSG_SIZE];

      /* if shm_as_p == NULL then, it is expected that failed creating shared memory */
      if (shm_as_p == NULL)
	{
	  rye_shm_detach (shm_br);

	  return -1;
	}

      memcpy (err_msg_backup, admin_Err_msg, ADMIN_ERR_MSG_SIZE);

      br_inactivate (&(shm_br->br_info[i]));

      rye_shm_destroy ((shm_br->br_info[i].appl_server_shm_key));

      memcpy (admin_Err_msg, err_msg_backup, ADMIN_ERR_MSG_SIZE);
    }

  if (i >= shm_br->num_broker)
    {
      SET_ADMIN_ERR_MSG ("Cannot find broker [%s]", broker_name);

      res = -1;
    }

  rye_shm_detach (shm_br);
  if (shm_as_p)
    {
      rye_shm_detach (shm_as_p);
    }

  return res;
}

int
admin_off_cmd (int shm_key_br_gl, const char *broker_name)
{
  int i, res;
  T_SHM_BROKER *shm_br;

  shm_br = rye_shm_attach (shm_key_br_gl, RYE_SHM_TYPE_BROKER_GLOBAL, false);
  if (shm_br == NULL)
    {
      SET_ADMIN_ERR_MSG ("Cannot open shared memory (shmkey:%x)",
			 shm_key_br_gl);
      return -1;
    }

  if (shm_br->owner_uid != getuid ())
    {
      SET_ADMIN_ERR_MSG ("Cannot stop broker. (Not owner)");
      return -1;
    }

  for (i = 0; i < shm_br->num_broker; i++)
    {
      if (strcmp (shm_br->br_info[i].name, broker_name) == 0)
	{
	  if (shm_br->br_info[i].service_flag == OFF)
	    {
	      SET_ADMIN_ERR_MSG ("Broker[%s] is not running", broker_name);
	      rye_shm_detach (shm_br);
	      return -1;
	    }
	  else
	    {
	      res = br_inactivate (&(shm_br->br_info[i]));
	      if (res < 0)
		{
		  SET_ADMIN_ERR_MSG ("Cannot inactivate broker [%s]",
				     broker_name);
		  rye_shm_detach (shm_br);
		  return -1;
		}
	      else
		{
		  rye_shm_destroy ((shm_br->br_info[i].appl_server_shm_key));
		}
	    }
	  break;
	}
    }

  if (i >= shm_br->num_broker)
    {
      SET_ADMIN_ERR_MSG ("Cannot find broker [%s]", broker_name);
      rye_shm_detach (shm_br);
      return -1;
    }

  rye_shm_detach (shm_br);
  return 0;
}

int
admin_reset_cmd (int shm_key_br_gl, const char *broker_name)
{
  int i, br_index;
  T_SHM_BROKER *shm_br;
  T_SHM_APPL_SERVER *shm_as_p;
  int limit_appl_index;

  shm_br = rye_shm_attach (shm_key_br_gl, RYE_SHM_TYPE_BROKER_GLOBAL, false);
  if (shm_br == NULL)
    {
      SET_ADMIN_ERR_MSG ("Cannot open shared memory (shmkey:%x)",
			 shm_key_br_gl);
      return -1;
    }

  br_index = -1;
  for (i = 0; i < shm_br->num_broker; i++)
    {
      if (strcmp (shm_br->br_info[i].name, broker_name) == 0)
	{
	  if (shm_br->br_info[i].service_flag == OFF)
	    {
	      SET_ADMIN_ERR_MSG ("Broker[%s] is not running", broker_name);
	      rye_shm_detach (shm_br);
	      return -1;
	    }
	  else
	    {
	      br_index = i;
	    }
	  break;
	}
    }

  if (br_index < 0)
    {
      SET_ADMIN_ERR_MSG ("Cannot find broker [%s]", broker_name);
      rye_shm_detach (shm_br);
      return -1;
    }

  shm_as_p = rye_shm_attach (shm_br->br_info[br_index].appl_server_shm_key,
			     RYE_SHM_TYPE_BROKER_LOCAL, false);
  if (shm_as_p == NULL)
    {
      SET_ADMIN_ERR_MSG ("Cannot open shared memory (shmkey:%x)",
			 shm_br->br_info[br_index].appl_server_shm_key);
      rye_shm_detach (shm_br);
      return -1;
    }
  assert (shm_as_p->num_appl_server <= APPL_SERVER_NUM_LIMIT);

  limit_appl_index = shm_br->br_info[br_index].appl_server_max_num;

  if (limit_appl_index > APPL_SERVER_NUM_LIMIT)
    {
      limit_appl_index = APPL_SERVER_NUM_LIMIT;
    }

  for (i = 0; i < limit_appl_index; i++)
    {
      shm_as_p->info.as_info[i].reset_flag = TRUE;
    }

  rye_shm_detach (shm_as_p);
  rye_shm_detach (shm_br);
  return 0;
}

int
admin_info_cmd (int shm_key_br_gl)
{
  T_SHM_BROKER *shm_br;

  shm_br = rye_shm_attach (shm_key_br_gl, RYE_SHM_TYPE_BROKER_GLOBAL, true);
  if (shm_br == NULL)
    {
      SET_ADMIN_ERR_MSG ("Cannot open shared memory (shmkey:%x)",
			 shm_key_br_gl);
      return -1;
    }

  broker_config_dump (stdout, shm_br->br_info, shm_br->num_broker,
		      shm_key_br_gl);

  rye_shm_detach (shm_br);
  return 0;
}

int
admin_conf_change (int shm_key_br_gl, const char *br_name,
		   const char *conf_name, const char *conf_value,
		   int as_number)
{
  int i, br_index;
  T_SHM_BROKER *shm_br = NULL;
  T_SHM_APPL_SERVER *shm_as_p = NULL;
  T_BROKER_INFO *br_info_p = NULL;

  shm_br = rye_shm_attach (shm_key_br_gl, RYE_SHM_TYPE_BROKER_GLOBAL, false);
  if (shm_br == NULL)
    {
      SET_ADMIN_ERR_MSG ("Broker is not started.");
      goto set_conf_error;
    }

  br_index = -1;
  for (i = 0; i < shm_br->num_broker; i++)
    {
      if (strcasecmp (br_name, shm_br->br_info[i].name) == 0)
	{
	  br_index = i;
	  break;
	}
    }
  if (br_index < 0)
    {
      SET_ADMIN_ERR_MSG ("Cannot find Broker [%s]", br_name);
      goto set_conf_error;
    }

  if (shm_br->br_info[br_index].service_flag == OFF)
    {
      SET_ADMIN_ERR_MSG ("Broker [%s] is not running.", br_name);
      goto set_conf_error;
    }

  br_info_p = &(shm_br->br_info[br_index]);
  shm_as_p = rye_shm_attach (br_info_p->appl_server_shm_key,
			     RYE_SHM_TYPE_BROKER_LOCAL, false);

  if (shm_as_p == NULL)
    {
      SET_ADMIN_ERR_MSG ("Cannot open shared memory (shmkey:%x)",
			 br_info_p->appl_server_shm_key);
      goto set_conf_error;
    }
  assert (shm_as_p->num_appl_server <= APPL_SERVER_NUM_LIMIT);

  if (strcasecmp (conf_name, "SQL_LOG") == 0)
    {
      char sql_log_mode;

      sql_log_mode = conf_get_value_sql_log_mode (conf_value);
      if (sql_log_mode < 0)
	{
	  SET_ADMIN_ERR_MSG ("invalid value : %s", conf_value);
	  goto set_conf_error;
	}

      if (as_number <= 0)
	{
	  shm_br->br_info[br_index].sql_log_mode = sql_log_mode;
	  shm_as_p->sql_log_mode = sql_log_mode;
	  for (i = 0; i < shm_br->br_info[br_index].appl_server_max_num; i++)
	    {
	      shm_as_p->info.as_info[i].cur_sql_log_mode = sql_log_mode;
	      shm_as_p->info.as_info[i].cas_log_reset = CAS_LOG_RESET_REOPEN;
	    }
	}
      else
	{
	  if (as_number > shm_as_p->num_appl_server)
	    {
	      SET_ADMIN_ERR_MSG ("Invalid cas number : %d", as_number);
	      goto set_conf_error;
	    }
	  shm_as_p->info.as_info[as_number - 1].cur_sql_log_mode =
	    sql_log_mode;
	  shm_as_p->info.as_info[as_number - 1].cas_log_reset =
	    CAS_LOG_RESET_REOPEN;
	}
    }
  else if (strcasecmp (conf_name, "BROKER_LOG") == 0)
    {
      char broker_log_mode;

      broker_log_mode = conf_get_value_broker_log_mode (conf_value);
      if (broker_log_mode < 0)
	{
	  SET_ADMIN_ERR_MSG ("invalid value : %s", conf_value);
	  goto set_conf_error;
	}

      shm_br->br_info[br_index].broker_log_mode = broker_log_mode;
      shm_as_p->broker_log_mode = broker_log_mode;

      shm_as_p->broker_log_reset = 1;
    }
  else if (strcasecmp (conf_name, "SLOW_LOG") == 0)
    {
      char slow_log_mode;

      slow_log_mode = conf_get_value_table_on_off (conf_value);
      if (slow_log_mode < 0)
	{
	  SET_ADMIN_ERR_MSG ("invalid value : %s", conf_value);
	  goto set_conf_error;
	}

      if (as_number <= 0)
	{
	  shm_br->br_info[br_index].slow_log_mode = slow_log_mode;
	  shm_as_p->slow_log_mode = slow_log_mode;
	  for (i = 0; i < shm_br->br_info[br_index].appl_server_max_num; i++)
	    {
	      shm_as_p->info.as_info[i].cur_slow_log_mode = slow_log_mode;
	      shm_as_p->info.as_info[i].cas_slow_log_reset =
		CAS_LOG_RESET_REOPEN;
	    }
	}
      else
	{
	  if (as_number > shm_as_p->num_appl_server)
	    {
	      SET_ADMIN_ERR_MSG ("Invalid cas number : %d", as_number);
	      goto set_conf_error;
	    }
	  shm_as_p->info.as_info[as_number - 1].cur_slow_log_mode =
	    slow_log_mode;
	  shm_as_p->info.as_info[as_number - 1].cas_slow_log_reset =
	    CAS_LOG_RESET_REOPEN;
	}
    }
  else if (strcasecmp (conf_name, "ACCESS_MODE") == 0)
    {
      char access_mode = conf_get_value_access_mode (conf_value);

      if (access_mode == -1)
	{
	  SET_ADMIN_ERR_MSG ("invalid value : %s", conf_value);
	  goto set_conf_error;
	}
      if (br_info_p->access_mode == access_mode)
	{
	  SET_ADMIN_ERR_MSG ("same as previous value : %s", conf_value);
	  goto set_conf_error;
	}
      br_info_p->access_mode = access_mode;
      shm_as_p->access_mode = access_mode;

      for (i = 0;
	   i < shm_as_p->num_appl_server && i < APPL_SERVER_NUM_LIMIT; i++)
	{
	  shm_as_p->info.as_info[i].reset_flag = TRUE;
	}
    }
  else if (strcasecmp (conf_name, "CONNECT_ORDER_RANDOM") == 0)
    {
      char connect_order_random;

      connect_order_random = conf_get_value_table_on_off (conf_value);
      if (connect_order_random < 0)
	{
	  SET_ADMIN_ERR_MSG ("invalid value : %s", conf_value);
	  goto set_conf_error;
	}

      if (br_info_p->connect_order_random == connect_order_random)
	{
	  SET_ADMIN_ERR_MSG ("same as previous value : %s", conf_value);
	  goto set_conf_error;
	}

      br_info_p->connect_order_random = connect_order_random;
      shm_as_p->connect_order_random = connect_order_random;

      for (i = 0;
	   i < shm_as_p->num_appl_server && i < APPL_SERVER_NUM_LIMIT; i++)
	{
	  shm_as_p->info.as_info[i].reset_flag = TRUE;
	}
    }
  else if (strcasecmp (conf_name, "MAX_NUM_DELAYED_HOSTS_LOOKUP") == 0)
    {
      int max_num_delayed_hosts_lookup = 0, result;

      result = parse_int (&max_num_delayed_hosts_lookup, conf_value, 10);

      if (result != 0
	  || max_num_delayed_hosts_lookup <
	  DEFAULT_MAX_NUM_DELAYED_HOSTS_LOOKUP)
	{
	  SET_ADMIN_ERR_MSG ("invalid value : %s", conf_value);
	  goto set_conf_error;
	}

      if (br_info_p->max_num_delayed_hosts_lookup ==
	  max_num_delayed_hosts_lookup)
	{
	  SET_ADMIN_ERR_MSG ("same as previous value : %s", conf_value);
	  goto set_conf_error;
	}

      br_info_p->max_num_delayed_hosts_lookup = max_num_delayed_hosts_lookup;
      shm_as_p->max_num_delayed_hosts_lookup = max_num_delayed_hosts_lookup;
    }
  else if (strcasecmp (conf_name, "RECONNECT_TIME") == 0)
    {
      int rctime;

      rctime = (int) ut_time_string_to_sec (conf_value, "sec");
      if (rctime < 0)
	{
	  SET_ADMIN_ERR_MSG ("invalid value : %s", conf_value);
	  goto set_conf_error;
	}

      br_info_p->cas_rctime = rctime;
      shm_as_p->cas_rctime = rctime;
    }
  else if (strcasecmp (conf_name, "SQL_LOG_MAX_SIZE") == 0)
    {
      int sql_log_max_size;

      sql_log_max_size = (int) ut_size_string_to_kbyte (conf_value, "K");

      if (sql_log_max_size <= 0)
	{
	  SET_ADMIN_ERR_MSG ("invalid value : %s", conf_value);
	  goto set_conf_error;
	}
      else if (sql_log_max_size > MAX_SQL_LOG_MAX_SIZE)
	{
	  SET_ADMIN_ERR_MSG ("value is out of range : %s", conf_value);
	  goto set_conf_error;
	}
      br_info_p->sql_log_max_size = sql_log_max_size;
      shm_as_p->sql_log_max_size = sql_log_max_size;
    }
  else if (strcasecmp (conf_name, "BROKER_LOG_MAX_SIZE") == 0)
    {
      int broker_log_max_size;

      broker_log_max_size = (int) ut_size_string_to_kbyte (conf_value, "K");

      if (broker_log_max_size <= 0)
	{
	  SET_ADMIN_ERR_MSG ("invalid value : %s", conf_value);
	  goto set_conf_error;
	}
      else if (broker_log_max_size > MAX_BROKER_LOG_MAX_SIZE)
	{
	  SET_ADMIN_ERR_MSG ("value is out of range : %s", conf_value);
	  goto set_conf_error;
	}
      br_info_p->broker_log_max_size = broker_log_max_size;
      shm_as_p->broker_log_max_size = broker_log_max_size;
    }
  else if (strcasecmp (conf_name, "LONG_QUERY_TIME") == 0)
    {
      float long_query_time;

      long_query_time = (float) ut_time_string_to_sec (conf_value, "sec");

      if (long_query_time < 0)
	{
	  SET_ADMIN_ERR_MSG ("invalid value : %s", conf_value);
	  goto set_conf_error;
	}
      else if (long_query_time > LONG_QUERY_TIME_LIMIT)
	{
	  SET_ADMIN_ERR_MSG ("value is out of range : %s", conf_value);
	  goto set_conf_error;
	}

      br_info_p->long_query_time = (int) (long_query_time * 1000.0);
      shm_as_p->long_query_time = (int) (long_query_time * 1000.0);
    }
  else if (strcasecmp (conf_name, "LONG_TRANSACTION_TIME") == 0)
    {
      float long_transaction_time;

      long_transaction_time =
	(float) ut_time_string_to_sec (conf_value, "sec");

      if (long_transaction_time < 0)
	{
	  SET_ADMIN_ERR_MSG ("invalid value : %s", conf_value);
	  goto set_conf_error;
	}
      else if (long_transaction_time > LONG_TRANSACTION_TIME_LIMIT)
	{
	  SET_ADMIN_ERR_MSG ("value is out of range : %s", conf_value);
	  goto set_conf_error;
	}

      br_info_p->long_transaction_time =
	(int) (long_transaction_time * 1000.0);
      shm_as_p->long_transaction_time =
	(int) (long_transaction_time * 1000.0);
    }
  else if (strcasecmp (conf_name, "APPL_SERVER_MAX_SIZE") == 0)
    {
      int max_size;

      max_size = (int) ut_size_string_to_kbyte (conf_value, "M");

      if (max_size < 0)
	{
	  SET_ADMIN_ERR_MSG ("invalid value : %s", conf_value);
	  goto set_conf_error;
	}

      if (max_size > 0
	  && max_size > (shm_br->br_info[br_index].appl_server_hard_limit))
	{
	  SET_ADMIN_ERR_MSG
	    ("CONFIGURATION WARNING - the APPL_SERVER_MAX_SIZE (%dM)"
	     " is greater than the APPL_SERVER_MAX_SIZE_HARD_LIMIT (%dM)",
	     max_size / ONE_K, shm_as_p->appl_server_hard_limit / ONE_K);
	}

      br_info_p->appl_server_max_size = max_size;
      shm_as_p->appl_server_max_size = max_size;
    }
  else if (strcasecmp (conf_name, "APPL_SERVER_MAX_SIZE_HARD_LIMIT") == 0)
    {
      int hard_limit;
      int max_hard_limit;

      hard_limit = (int) ut_size_string_to_kbyte (conf_value, "M");

      max_hard_limit = INT_MAX;
      if (hard_limit <= 0)
	{
	  SET_ADMIN_ERR_MSG
	    ("APPL_SERVER_MAX_SIZE_HARD_LIMIT must be between 1 and %d",
	     max_hard_limit / ONE_K);
	  goto set_conf_error;
	}

      if (hard_limit < shm_br->br_info[br_index].appl_server_max_size)
	{
	  SET_ADMIN_ERR_MSG
	    ("CONFIGURATION WARNING - the APPL_SERVER_MAX_SIZE_HARD_LIMIT (%dM) "
	     "is smaller than the APPL_SERVER_MAX_SIZE (%dM)",
	     hard_limit / ONE_K, shm_as_p->appl_server_max_size / ONE_K);
	}

      br_info_p->appl_server_hard_limit = hard_limit;
      shm_as_p->appl_server_hard_limit = hard_limit;
    }
  else if (strcasecmp (conf_name, "LOG_BACKUP") == 0)
    {
      int log_backup;

      log_backup = conf_get_value_table_on_off (conf_value);
      if (log_backup < 0)
	{
	  SET_ADMIN_ERR_MSG ("invalid value : %s", conf_value);
	  goto set_conf_error;
	}
      br_info_p->log_backup = log_backup;
    }
  else if (strcasecmp (conf_name, "TIME_TO_KILL") == 0)
    {
      int time_to_kill;

      time_to_kill = (int) ut_time_string_to_sec (conf_value, "sec");
      if (time_to_kill <= 0)
	{
	  SET_ADMIN_ERR_MSG ("invalid value : %s", conf_value);
	  goto set_conf_error;
	}
      br_info_p->time_to_kill = time_to_kill;
    }
  else if (strcasecmp (conf_name, "ACCESS_LOG") == 0)
    {
      int access_log_flag;

      access_log_flag = conf_get_value_table_on_off (conf_value);
      if (access_log_flag < 0)
	{
	  SET_ADMIN_ERR_MSG ("invalid value : %s", conf_value);
	  goto set_conf_error;
	}
      br_info_p->access_log = access_log_flag;
      shm_as_p->access_log = access_log_flag;
    }
  else if (strcasecmp (conf_name, "ACCESS_LOG_MAX_SIZE") == 0)
    {
      int size;

      /*
       * Use "KB" as unit, because MAX_ACCESS_LOG_MAX_SIZE uses this unit.
       * the range of the config value should be verified to avoid the invalid setting.
       */
      size = (int) ut_size_string_to_kbyte (conf_value, "K");

      if (size < 0)
	{
	  SET_ADMIN_ERR_MSG ("invalid value : %s", conf_value);
	  goto set_conf_error;
	}
      else if (size > MAX_ACCESS_LOG_MAX_SIZE)
	{
	  SET_ADMIN_ERR_MSG ("value is out of range : %s", conf_value);
	  goto set_conf_error;
	}


      br_info_p->access_log_max_size = size;
      shm_as_p->access_log_max_size = size;
    }
  else if (strcasecmp (conf_name, "KEEP_CONNECTION") == 0)
    {
      int keep_con;

      keep_con = conf_get_value_keep_con (conf_value);
      if (keep_con < 0)
	{
	  SET_ADMIN_ERR_MSG ("invalid value : %s", conf_value);
	  goto set_conf_error;
	}
      br_info_p->keep_connection = keep_con;
      shm_as_p->keep_connection = keep_con;
    }
  else if (strcasecmp (conf_name, "STATEMENT_POOLING") == 0)
    {
      int val;

      val = conf_get_value_table_on_off (conf_value);
      if (val < 0)
	{
	  SET_ADMIN_ERR_MSG ("invalid value : %s", conf_value);
	  goto set_conf_error;
	}
      br_info_p->statement_pooling = val;
      shm_as_p->statement_pooling = val;
    }
  else if (strcasecmp (conf_name, "MAX_QUERY_TIMEOUT") == 0)
    {
      int val;

      val = (int) ut_time_string_to_sec (conf_value, "sec");

      if (val < 0)
	{
	  SET_ADMIN_ERR_MSG ("invalid value: %s", conf_value);
	  goto set_conf_error;
	}
      else if (val > MAX_QUERY_TIMEOUT_LIMIT)
	{
	  SET_ADMIN_ERR_MSG ("value is out of range : %s", conf_value);
	  goto set_conf_error;
	}
      br_info_p->query_timeout = val;
      shm_as_p->query_timeout = val;
    }
  else if (strcasecmp (conf_name, "PREFERRED_HOSTS") == 0)
    {
      PRM_NODE_LIST node_list;

      if (prm_split_node_str (&node_list, conf_value, false) != NO_ERROR)
	{
	  SET_ADMIN_ERR_MSG ("invalid value: %s", conf_value);
	  goto set_conf_error;
	}

      br_info_p->preferred_hosts = node_list;
      shm_as_p->preferred_hosts = node_list;
    }
  else if (strcasecmp (conf_name, "MAX_PREPARED_STMT_COUNT") == 0)
    {
      int err_code = 0;
      int stmt_cnt = 0;

      err_code = parse_int (&stmt_cnt, conf_value, 10);
      if (err_code < 0)
	{
	  SET_ADMIN_ERR_MSG ("invalid value : %s", conf_value);
	  goto set_conf_error;
	}

      if (stmt_cnt < 1)
	{
	  SET_ADMIN_ERR_MSG ("value is out of range : %s", conf_value);
	  goto set_conf_error;
	}

      if (br_info_p->max_prepared_stmt_count == stmt_cnt)
	{
	  SET_ADMIN_ERR_MSG ("same as previous value : %s", conf_value);
	  goto set_conf_error;
	}

      if (br_info_p->max_prepared_stmt_count > stmt_cnt)
	{
	  SET_ADMIN_ERR_MSG
	    ("cannot be decreased below the previous value '%d' : %s",
	     br_info_p->max_prepared_stmt_count, conf_value);
	  goto set_conf_error;
	}

      br_info_p->max_prepared_stmt_count = stmt_cnt;
      shm_as_p->max_prepared_stmt_count = stmt_cnt;
    }
  else if (strcasecmp (conf_name, "SESSION_TIMEOUT") == 0)
    {
      int session_timeout = 0;

      session_timeout = (int) ut_time_string_to_sec (conf_value, "sec");
      if (session_timeout < 0)
	{
	  SET_ADMIN_ERR_MSG ("invalid value : %s", conf_value);
	  goto set_conf_error;
	}

      if (br_info_p->session_timeout == session_timeout)
	{
	  SET_ADMIN_ERR_MSG ("same as previous value : %s", conf_value);
	  goto set_conf_error;
	}

      br_info_p->session_timeout = session_timeout;
      shm_as_p->session_timeout = session_timeout;
    }
  else if (strcasecmp (conf_name, "ENABLE_MONITOR_SERVER") == 0)
    {
      int monitor_server_flag;

      monitor_server_flag = conf_get_value_table_on_off (conf_value);
      if (monitor_server_flag < 0)
	{
	  SET_ADMIN_ERR_MSG ("invalid value : %s", conf_value);
	  goto set_conf_error;
	}
      br_info_p->monitor_server_flag = monitor_server_flag;
      shm_as_p->monitor_server_flag = monitor_server_flag;
    }
  else if (strcasecmp (conf_name, "SHARD_MGMT_NUM_MIGRATOR") == 0)
    {
      int num_migrator = 0;

      if (parse_int (&num_migrator, conf_value, 10) < 0 || num_migrator <= 0)
	{
	  SET_ADMIN_ERR_MSG ("invalid value : %s", conf_value);
	  goto set_conf_error;
	}

      for (i = 0; i < shm_br->num_broker; i++)
	{
	  if (shm_br->br_info[i].broker_type == SHARD_MGMT)
	    {
	      shm_br->br_info[i].shard_mgmt_num_migrator = num_migrator;
	    }
	}
    }
  else
    {
      SET_ADMIN_ERR_MSG ("unknown keyword %s", conf_name);
      goto set_conf_error;
    }

  if (shm_as_p)
    {
      rye_shm_detach (shm_as_p);
    }
  if (shm_br)
    {
      rye_shm_detach (shm_br);
    }
  return 0;

set_conf_error:
  if (shm_as_p)
    {
      rye_shm_detach (shm_as_p);
    }
  if (shm_br)
    {
      rye_shm_detach (shm_br);
    }
  return -1;
}

void
admin_init_env ()
{
  int i, j;
  char *p;
  const char *clt_envs[] = { NULL };
  static char dummy_env[] = "DUMMY_ENV=DUMMY";

  for (i = 0; environ[i] != NULL; i++)
    {
      p = strchr (environ[i], '=');
      if (p == NULL)
	{
	  environ[i] = dummy_env;
	  continue;
	}
      for (j = 0; clt_envs[j] != NULL; j++)
	{
	  if (strncmp (environ[i], clt_envs[j], strlen (clt_envs[j])) == 0)
	    {
	      environ[i] = dummy_env;
	      break;
	    }
	}
    }
}

int
admin_acl_status_cmd (int shm_key_br_gl, const char *broker_name)
{
  int i, j;
  int br_index;
  T_SHM_BROKER *shm_br;
  T_SHM_APPL_SERVER *shm_appl;

  shm_br = rye_shm_attach (shm_key_br_gl, RYE_SHM_TYPE_BROKER_GLOBAL, true);

  if (shm_br == NULL)
    {
      SET_ADMIN_ERR_MSG ("Cannot open shared memory (shmkey:%x)",
			 shm_key_br_gl);
      return -1;
    }

  br_index = -1;
  if (broker_name != NULL)
    {
      for (i = 0; i < shm_br->num_broker; i++)
	{
	  if (strcmp (shm_br->br_info[i].name, broker_name) == 0)
	    {
	      br_index = i;
	      break;
	    }
	}
      if (br_index == -1)
	{
	  SET_ADMIN_ERR_MSG ("Cannot find broker [%s]\n", broker_name);
	  rye_shm_detach (shm_br);
	  return -1;
	}
    }

  for (i = 0; i < shm_br->num_broker; i++)
    {
      if (shm_br->br_info[i].service_flag == OFF ||
	  shm_br->br_info[i].broker_type != NORMAL_BROKER ||
	  strcmp (shm_br->br_info[i].name, BR_SHARD_MGMT_NAME) == 0 ||
	  (br_index >= 0 && br_index != i))
	{
	  continue;
	}

      shm_appl = rye_shm_attach (shm_br->br_info[i].appl_server_shm_key,
				 RYE_SHM_TYPE_BROKER_LOCAL, true);
      if (shm_appl == NULL)
	{
	  SET_ADMIN_ERR_MSG ("Cannot open shared memory (shmkey:%x)",
			     shm_br->br_info[i].appl_server_shm_key);
	  rye_shm_detach (shm_br);
	  return -1;
	}

      fprintf (stdout, "[%%%s]\n", shm_appl->broker_name);

      for (j = 0; j < shm_appl->num_acl_info; j++)
	{
	  br_acl_dump (stdout, &shm_appl->acl_info[j]);
	}
      fprintf (stdout, "\n");
      rye_shm_detach (shm_appl);
    }

  rye_shm_detach (shm_br);
  return 0;
}

static int
cp_acl_file (const char *new_acl_file)
{
  char tmp_acl_file[BROKER_PATH_MAX] = "";
  char acl_file[BROKER_PATH_MAX] = "";
  char bak_acl_file[BROKER_PATH_MAX] = "";
  char tmp_filename[BROKER_PATH_MAX];
  int fd_src, fd_dest;

  /* make filenames */
  sprintf (tmp_filename, "%s.tmp.%d", BROKER_ACL_FILE, getpid ());
  if (envvar_confdir_file (tmp_acl_file, sizeof (tmp_acl_file),
			   tmp_filename) == NULL)
    {
      return -1;
    }
  if (envvar_confdir_file (bak_acl_file, sizeof (tmp_acl_file),
			   BROKER_ACL_FILE ".bak") == NULL)
    {
      return -1;
    }
  envvar_broker_acl_file (acl_file, sizeof (acl_file));

  /* cpoy file */
  fd_src = open (new_acl_file, O_RDONLY);
  if (fd_src < 0)
    {
      return -1;
    }
  fd_dest = open (tmp_acl_file, O_CREAT | O_WRONLY | O_TRUNC, 0666);
  if (fd_dest < 0)
    {
      close (fd_src);
      return -1;
    }

  while (true)
    {
      char buf[1024];
      int n;

      n = read (fd_src, buf, sizeof (buf));
      if (n < 0)
	{
	  close (fd_src);
	  close (fd_dest);
	  unlink (tmp_acl_file);
	  return -1;
	}
      else if (n == 0)
	{
	  break;
	}
      if (write (fd_dest, buf, n) <= 0)
	{
	  close (fd_src);
	  close (fd_dest);
	  unlink (tmp_acl_file);
	  return -1;
	}
    }
  close (fd_src);
  close (fd_dest);

  /* test acl file */
  if (admin_acl_test_cmd_internal (tmp_acl_file, 0, NULL, NULL) < 0)
    {
      unlink (tmp_acl_file);
      return -1;
    }

  /* rename files */
  unlink (bak_acl_file);
  if (rename (acl_file, bak_acl_file) < 0)
    {
      unlink (tmp_acl_file);
      return -1;
    }
  if (rename (tmp_acl_file, acl_file) < 0)
    {
      unlink (tmp_acl_file);
      rename (bak_acl_file, acl_file);
    }

  return 0;
}

int
admin_acl_reload_cmd (int shm_key_br_gl, const char *new_acl_file)
{
  T_SHM_BROKER *shm_br;
  T_SHM_APPL_SERVER *shm_appl;
  char br_acl_file[BROKER_PATH_MAX];
  BROKER_ACL_CONF br_acl_conf;
  int i;

  envvar_broker_acl_file (br_acl_file, sizeof (br_acl_file));

  if (new_acl_file != NULL)
    {
      if (cp_acl_file (new_acl_file) < 0)
	{
	  SET_ADMIN_ERR_MSG ("Cannot make acl file ");
	  return -1;
	}
    }

  shm_br = rye_shm_attach (shm_key_br_gl, RYE_SHM_TYPE_BROKER_GLOBAL, false);

  if (shm_br == NULL)
    {
      SET_ADMIN_ERR_MSG ("Cannot open shared memory (shmkey:%x)",
			 shm_key_br_gl);
      return -1;
    }

  if (br_acl_read_config_file (&br_acl_conf, br_acl_file, false,
			       admin_Err_msg) < 0)
    {
      goto error;
    }

  for (i = 0; i < shm_br->num_broker; i++)
    {
      int err;

      if (shm_br->br_info[i].service_flag == OFF)
	{
	  continue;
	}

      shm_appl = rye_shm_attach (shm_br->br_info[i].appl_server_shm_key,
				 RYE_SHM_TYPE_BROKER_LOCAL, false);
      if (shm_appl == NULL)
	{
	  SET_ADMIN_ERR_MSG ("Cannot open shared memory (shmkey:%x)",
			     shm_br->br_info[i].appl_server_shm_key);
	  goto error;
	}

      err = br_acl_set_shm (shm_appl, NULL, &br_acl_conf,
			    shm_br->br_info[i].name, admin_Err_msg);

      rye_shm_detach (shm_appl);

      if (err < 0)
	{
	  goto error;
	}
    }

  rye_shm_detach (shm_br);
  return 0;

error:
  rye_shm_detach (shm_br);
  return -1;
}

int
admin_acl_test_cmd (int argc, char **argv)
{
  const char *acl_filename;

  assert (argc >= 1);

  acl_filename = argv[0];

  return admin_acl_test_cmd_internal (acl_filename, argc - 1, argv + 1,
				      stdout);
}

static int
admin_acl_test_cmd_internal (const char *acl_filename, int argc, char **argv,
			     FILE * out_fp)
{
  BROKER_ACL_CONF br_acl_conf;
  int i;
  int num_broker, shm_key_br_gl;
  T_BROKER_INFO br_info[MAX_BROKER_NUM];
  int num_acl_broker = 0;
  char **acl_broker = NULL;
  BR_ACL_INFO **acl_info = NULL;
  int *num_acl_info = NULL;
  int err_code = -1;

  if (broker_config_read (br_info, &num_broker, &shm_key_br_gl, NULL, 1) < 0)
    {
      SET_ADMIN_ERR_MSG ("broker config read error\n");
      goto end;
    }

  for (i = 0; i < num_broker; i++)
    {
      if (br_info[i].broker_type == NORMAL_BROKER &&
	  br_info[i].name[0] != '_')
	{
	  num_acl_broker++;
	}
    }

  acl_broker = malloc (sizeof (char *) * num_acl_broker);
  acl_info = malloc (sizeof (BR_ACL_INFO *) * num_acl_broker);
  num_acl_info = malloc (sizeof (int) * num_acl_broker);

  num_acl_broker = 0;

  if (acl_broker == NULL || acl_info == NULL || num_acl_info == NULL)
    {
      goto end;
    }

  if (br_acl_read_config_file (&br_acl_conf, acl_filename,
			       false, admin_Err_msg) < 0)
    {
      goto end;
    }

  for (i = 0; i < num_broker; i++)
    {
      if (br_info[i].broker_type == NORMAL_BROKER &&
	  br_info[i].name[0] != '_')
	{
	  acl_broker[num_acl_broker] = br_info[i].name;
	  acl_info[num_acl_broker] = malloc (sizeof (BR_ACL_INFO) *
					     ACL_MAX_ITEM_COUNT);
	  if (acl_info[num_acl_broker] == NULL)
	    {
	      goto end;
	    }

	  num_acl_info[num_acl_broker] =
	    br_acl_set_shm (NULL, acl_info[num_acl_broker], &br_acl_conf,
			    br_info[i].name, admin_Err_msg);
	  if (num_acl_info[num_acl_broker] < 0)
	    {
	      goto end;
	    }

	  num_acl_broker++;
	}
    }

  if (out_fp != NULL)
    {
      for (i = 0; i < argc; i++)
	{
	  int cur_acl_info = -1;
	  int j;
	  const char *res;

	  char *save = NULL;
	  const char *broker_name;
	  const char *dbname;
	  const char *dbuser;
	  char *ip_str;
	  ACL_IP_INFO ip_info;

	  fprintf (out_fp, "%s -> ", argv[i]);

	  broker_name = strtok_r (argv[i], ":", &save);
	  dbname = strtok_r (NULL, ":", &save);
	  dbuser = strtok_r (NULL, ":", &save);
	  ip_str = strtok_r (NULL, ":", &save);
	  if (broker_name == NULL || dbname == NULL || dbuser == NULL ||
	      ip_str == NULL)
	    {
	      fprintf (out_fp, "ERROR: argument format\n");
	      continue;
	    }

	  for (j = 0; j < num_acl_broker; j++)
	    {
	      if (strcmp (broker_name, acl_broker[j]) == 0)
		{
		  cur_acl_info = j;
		  break;
		}
	    }

	  if (cur_acl_info < 0)
	    {
	      res = "ERROR:broker not found";
	    }
	  else
	    {
	      memset (&ip_info, 0, sizeof (ACL_IP_INFO));
	      if (br_acl_conf_read_ip_addr (&ip_info, ip_str, NULL, NULL) < 0)
		{
		  res = "ERROR:INVALID IP";
		}
	      else
		{

		  if (br_acl_check_right (NULL, acl_info[cur_acl_info],
					  num_acl_info[cur_acl_info],
					  dbname, dbuser,
					  ip_info.ip_addr) < 0)
		    {
		      res = "FAIL";
		    }
		  else
		    {
		      res = "SUCCESS";
		    }
		}
	    }

	  fprintf (out_fp, "%s\n", res);
	}

      fprintf (out_fp, "\n");
      for (i = 0; i < num_acl_broker; i++)
	{
	  BR_ACL_INFO *cur_acl_info;
	  int j;

	  fprintf (out_fp, "[%%%s]\n", acl_broker[i]);

	  cur_acl_info = acl_info[i];
	  for (j = 0; j < num_acl_info[i]; j++)
	    {
	      br_acl_dump (out_fp, &cur_acl_info[j]);
	    }
	  fprintf (out_fp, "\n");
	}
    }

  err_code = 0;

end:

  if (acl_info != NULL)
    {
      for (i = 0; i < num_acl_broker; i++)
	{
	  RYE_FREE_MEM (acl_info[i]);
	}
    }
  RYE_FREE_MEM (acl_broker);
  RYE_FREE_MEM (acl_info);
  RYE_FREE_MEM (num_acl_info);

  return err_code;
}

int
admin_get_broker_key_and_portid (char **broker_key, int *port,
				 int shm_key_br_gl, const char *broker_name,
				 char broker_type)
{
  T_SHM_BROKER *shm_br = NULL;
  T_BROKER_INFO *br_info_p = NULL;

  assert (broker_key != NULL && port != NULL);

  *broker_key = NULL;
  *port = -1;

  shm_br = rye_shm_attach (shm_key_br_gl, RYE_SHM_TYPE_BROKER_GLOBAL, true);
  if (shm_br == NULL)
    {
      goto error;
    }

  *broker_key = strdup (shm_br->broker_key);
  if (*broker_key == NULL)
    {
      goto error;
    }

  br_info_p = ut_find_broker (shm_br->br_info, shm_br->num_broker,
			      broker_name, broker_type);
  if (br_info_p == NULL)
    {
      goto error;
    }

  *port = br_info_p->port;

  rye_shm_detach (shm_br);

  return 0;

error:
  RYE_FREE_MEM (*broker_key);
  *port = -1;

  if (shm_br != NULL)
    {
      rye_shm_detach (shm_br);
    }

  return -1;
}

static int
br_activate (T_BROKER_INFO * br_info, int shm_key_br_gl,
	     T_SHM_BROKER * shm_br, int br_index)
{
  int pid, i, res = 0;
  T_SHM_APPL_SERVER *shm_appl = NULL;
  char **env = NULL;
  int env_num = 0;
  char master_shm_key_str[32];
  const char *broker_exe_name;
  int broker_check_loop_count = 30;
  char br_index_env_str[32];
  char broker_port_name[BROKER_PATH_MAX] = "";

  br_info->br_init_err.err_code = 1;	/* will be changed to 0 (success) or err code */
  br_info->br_init_err.os_err_code = 0;
  br_info->num_busy_count = 0;
  br_info->reject_client_count = 0;
  br_info->connect_fail_count = 0;
  br_info->cancel_req_count = 0;
  br_info->ping_req_count = 0;

  shm_appl = rye_shm_attach (br_info->appl_server_shm_key,
			     RYE_SHM_TYPE_BROKER_LOCAL, false);
  if (shm_appl == NULL)
    {
      SET_ADMIN_ERR_MSG ("%s: cannot open shared memory", br_info->name);
      res = -1;
      goto end;
    }
  assert (shm_appl->num_appl_server <= APPL_SERVER_NUM_LIMIT);

  if (ut_get_broker_port_name (broker_port_name,
			       sizeof (broker_port_name), br_info) == 0)
    {
      unlink (broker_port_name);
    }

  env = make_env (br_info->source_env, &env_num);

  signal (SIGCHLD, SIG_IGN);

  if ((pid = fork ()) < 0)
    {
      SET_ADMIN_ERR_MSG ("fork error");
      res = -1;
      goto end;
    }

  br_info->ready_to_service = false;

  if (pid == 0)
    {
      signal (SIGCHLD, SIG_DFL);
      char argv0[PATH_MAX];

      br_info->broker_pid = getpid ();

      if (env != NULL)
	{
	  for (i = 0; i < env_num; i++)
	    putenv (env[i]);
	}

      sprintf (master_shm_key_str, "%s=%d", MASTER_SHM_KEY_ENV_STR,
	       shm_key_br_gl);
      putenv (master_shm_key_str);

      sprintf (br_index_env_str, "%s=%d", BROKER_INDEX_ENV_STR, br_index);
      putenv (br_index_env_str);

      broker_exe_name = NAME_CAS_BROKER;
      ut_make_broker_process_name (argv0, sizeof (argv0), br_info);

      rye_shm_detach (shm_appl);
      rye_shm_detach (shm_br);

      if (execle (broker_exe_name, argv0, NULL, environ) < 0)
	{
	  br_info->broker_pid = 0;
	  perror (broker_exe_name);
	  exit (1);
	}
      exit (0);
    }


  THREAD_SLEEP (200);

  for (i = 0; i < shm_appl->num_appl_server && i < APPL_SERVER_NUM_LIMIT; i++)
    {
      as_activate (shm_br, br_info, shm_appl, &shm_appl->info.as_info[i], i,
		   env, env_num);
    }
  for (; i < br_info->appl_server_max_num; i++)
    {
      CON_STATUS_LOCK_INIT (&(shm_appl->info.as_info[i]));
    }

  br_info->ready_to_service = true;
  br_info->service_flag = ON;

  for (i = 0; i < broker_check_loop_count; i++)
    {
      if (br_info->br_init_err.err_code > 0)
	{
	  THREAD_SLEEP (100);
	}
      else
	{
	  if (br_info->br_init_err.err_code < 0)
	    {
	      char br_init_err_msg[1024];
	      get_br_init_err_msg (br_init_err_msg, sizeof (br_init_err_msg),
				   br_info->br_init_err.err_code,
				   br_info->br_init_err.os_err_code);
	      SET_ADMIN_ERR_MSG ("%s: %s", br_info->name, br_init_err_msg);
	      res = -1;
	    }
	  break;
	}
    }

  if (i == broker_check_loop_count)
    {
      SET_ADMIN_ERR_MSG ("%s: unknown error", br_info->name);
      res = -1;
    }

end:
  if (shm_appl)
    {
      rye_shm_detach (shm_appl);
    }
  if (env)
    {
      free_env (env, env_num);
    }

  return res;
}

static int
br_inactivate (T_BROKER_INFO * br_info)
{
  int res = 0;
  int as_index;
  T_SHM_APPL_SERVER *shm_appl = NULL;

  if (br_info->broker_pid)
    {
      ut_kill_broker_process (br_info);

      br_info->broker_pid = 0;

      if (br_info->broker_type == NORMAL_BROKER)
	{
	  THREAD_SLEEP (1000);
	}
    }

  shm_appl = rye_shm_attach (br_info->appl_server_shm_key,
			     RYE_SHM_TYPE_BROKER_LOCAL, false);

  if (shm_appl == NULL)
    {
      SET_ADMIN_ERR_MSG ("Cannot open shared memory (shmkey:%x)",
			 br_info->appl_server_shm_key);
      res = -1;
      goto end;
    }

  for (as_index = 0; as_index < br_info->appl_server_max_num; as_index++)
    {
      as_inactivate (&shm_appl->info.as_info[as_index], br_info->name);
    }

  br_info->num_busy_count = 0;
  br_info->service_flag = OFF;

  br_sem_destroy (&shm_appl->acl_sem);

end:
  if (shm_appl)
    {
      rye_shm_detach (shm_appl);
    }

  return res;
}

static void
as_activate (T_SHM_BROKER * shm_br, T_BROKER_INFO * br_info,
	     T_SHM_APPL_SERVER * shm_appl, T_APPL_SERVER_INFO * as_info,
	     int as_index, char **env, int env_num)
{
  int pid;
  char appl_server_shm_key_env_str[32];
  char appl_name[APPL_SERVER_NAME_MAX_SIZE];
  int i;
  char as_id_env_str[32];

  char port_name[BROKER_PATH_MAX];

  ut_get_as_port_name (port_name, br_info->name, as_index, BROKER_PATH_MAX);
  unlink (port_name);

  /* mutex variable initialize */
  as_info->mutex_flag[SHM_MUTEX_BROKER] = FALSE;
  as_info->mutex_flag[SHM_MUTEX_ADMIN] = FALSE;
  as_info->mutex_turn = SHM_MUTEX_BROKER;
  CON_STATUS_LOCK_INIT (as_info);

  as_info->num_request = 0;
  as_info->uts_status = UTS_STATUS_START;
  as_info->reset_flag = FALSE;
  as_info->cur_sql_log_mode = shm_appl->sql_log_mode;
  as_info->cur_slow_log_mode = shm_appl->slow_log_mode;

  as_info->cas_clt_ip_addr = 0;
  as_info->cas_clt_port = 0;
  as_info->client_version[0] = '\0';

  as_info->service_ready_flag = FALSE;

  pid = fork ();
  if (pid < 0)
    {
      perror ("fork");
    }

  if (pid == 0)
    {
      char argv0[PATH_MAX];

      if (env != NULL)
	{
	  for (i = 0; i < env_num; i++)
	    putenv (env[i]);
	}

      sprintf (appl_server_shm_key_env_str, "%s=%d",
	       APPL_SERVER_SHM_KEY_STR, br_info->appl_server_shm_key);
      putenv (appl_server_shm_key_env_str);

      snprintf (as_id_env_str, sizeof (as_id_env_str), "%s=%d", AS_ID_ENV_STR,
		as_index);
      putenv (as_id_env_str);

      strcpy (appl_name, shm_appl->appl_server_name);


      ut_make_cas_process_name (argv0, sizeof (argv0), br_info->name,
				as_index);

      rye_shm_detach (shm_appl);
      rye_shm_detach (shm_br);

      if (execle (appl_name, argv0, NULL, environ) < 0)
	{
	  perror (appl_name);
	}
      exit (0);
    }

  (void) ut_is_appl_server_ready (pid, &as_info->service_ready_flag);

  as_info->pid = pid;
  as_info->last_access_time = time (NULL);
  as_info->transaction_start_time = (time_t) 0;
  as_info->psize_time = time (NULL);
  as_info->psize = os_get_mem_size (as_info->pid, MEM_VSIZE);
  if (as_info->psize > 1)
    {
      as_info->psize = as_info->psize / ONE_K;
    }

  as_info->uts_status = UTS_STATUS_IDLE;

  as_info->service_flag = SERVICE_ON;
}

static void
as_inactivate (T_APPL_SERVER_INFO * as_info_p, char *broker_name)
{
  if (as_info_p->pid <= 0)
    {
      return;
    }

  ut_kill_as_process (as_info_p->pid, broker_name, as_info_p->as_id);

  as_info_p->pid = 0;
  as_info_p->service_flag = SERVICE_OFF;
  as_info_p->service_ready_flag = FALSE;

  /* initialize con / uts status */
  as_info_p->uts_status = UTS_STATUS_IDLE;
  as_info_p->con_status = CON_STATUS_CLOSE;

  CON_STATUS_LOCK_DESTROY (as_info_p);

  return;
}

static void
free_env (char **env, int env_num)
{
  int i;

  if (env == NULL)
    {
      return;
    }

  for (i = 0; i < env_num; i++)
    {
      RYE_FREE_MEM (env[i]);
    }
  RYE_FREE_MEM (env);
}

static char **
make_env (char *env_file, int *env_num)
{
  char **env = NULL, **tmp_env = NULL;
  int num, read_num;
  FILE *env_fp;
  char read_buf[BUFSIZ], col1[128], col2[128];

  *env_num = 0;

  if (env_file[0] == '\0')
    return NULL;

  env_fp = fopen (env_file, "r");
  if (env_fp == NULL)
    return NULL;

  num = 0;

  while (fgets (read_buf, BUFSIZ, env_fp) != NULL)
    {
      if (read_buf[0] == '#')
	continue;
      read_num = sscanf (read_buf, "%127s%127s", col1, col2);
      if (read_num != 2)
	continue;

      if (env == NULL)
	{
	  env = (char **) malloc (sizeof (char *));
	  if (env == NULL)
	    {
	      break;
	    }
	}
      else
	{
	  tmp_env = (char **) realloc (env, sizeof (char *) * (num + 1));
	  if (tmp_env == NULL)
	    {
	      break;
	    }
	  env = tmp_env;
	}


      env[num] = (char *) malloc (strlen (col1) + strlen (col2) + 2);
      if (env[num] == NULL)
	{
	  for (num--; num >= 0; num--)
	    RYE_FREE_MEM (env[num]);
	  RYE_FREE_MEM (env);
	  env = NULL;
	  break;
	}

      sprintf (env[num], "%s=%s", col1, col2);
      num++;
    }

  fclose (env_fp);

  *env_num = num;
  return env;
}

static void
get_br_init_err_msg (char *msg_buf, int buf_size, int err_code,
		     int os_err_code)
{
  switch (err_code)
    {
    case BR_ER_INIT_NO_MORE_MEMORY:
      snprintf (msg_buf, buf_size, "error (NO_MORE_MEMORY)");
      break;
    case BR_ER_INIT_CANT_CREATE_SOCKET:
      snprintf (msg_buf, buf_size, "error (CANT_CREATE_SOCKET)");
      break;
    case BR_ER_INIT_CANT_BIND:
      snprintf (msg_buf, buf_size, "error (CANT_BIND)");
      break;
    case BR_ER_INIT_SHM_OPEN:
      snprintf (msg_buf, buf_size, "error (SHM_OPEN)");
      break;
    case BR_ER_INIT_SHARD_MGMT_INIT_FAIL:
      snprintf (msg_buf, buf_size, "error (SHARD_MGMT_INIT_FAIL)");
      break;
    case BR_ER_INIT_LOCAL_MGMT_INIT_FAIL:
      snprintf (msg_buf, buf_size, "error (LOCAL_MGMT_INIT_FAIL)");
      break;
    default:
      snprintf (msg_buf, buf_size, "error (%d)", err_code);
      break;
    }

  if (os_err_code != 0)
    {
      char *p;
      p = strerror (os_err_code);
      if (p != NULL)
	{
	  snprintf (msg_buf, buf_size, "%s (OS err %d, %s)", msg_buf,
		    os_err_code, p);
	}
    }
}
