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
 * broker_util.c -
 */

#ident "$Id$"

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <signal.h>
#include <assert.h>
#include <ctype.h>

#include <unistd.h>
#include <sys/time.h>
#include <sys/timeb.h>
#include <netinet/tcp.h>

#include "porting.h"
#include "cas_common.h"
#include "broker_env_def.h"
#include "broker_util.h"
#include "environment_variable.h"
#include "porting.h"

int
ut_kill_process (int pid)
{
  int i;

  if (pid > 0)
    {
      for (i = 0; i < 10; i++)
	{
	  if (kill (pid, SIGTERM) < 0)
	    {
	      return 0;
	    }
	  THREAD_SLEEP (30);
	  if (kill (pid, 0) < 0)
	    {
	      break;
	    }
	}
      if (i >= 10)
	{
	  kill (pid, SIGKILL);
	}
    }

  return 0;
}

void
ut_kill_broker_process (const T_BROKER_INFO * br_info)
{
  char tmp[BROKER_PATH_MAX];

  ut_kill_process (br_info->broker_pid);

  if (ut_get_broker_port_name (tmp, BROKER_PATH_MAX, br_info) == 0)
    {
      unlink (tmp);
    }
}

int
ut_kill_as_process (int pid, char *broker_name, int as_index)
{
  ut_kill_process (pid);

  if (broker_name != NULL)
    {
      char tmp[BROKER_PATH_MAX];

      ut_get_as_port_name (tmp, broker_name, as_index, BROKER_PATH_MAX);

      unlink (tmp);

      ut_get_as_pid_name (tmp, broker_name, as_index, BROKER_PATH_MAX);

      unlink (tmp);

      return 0;
    }
  return -1;
}

int
ut_set_keepalive (int sock)
{
  int optval, optlen;

  optlen = sizeof (optval);
  optval = 1;			/* true for SO_KEEPALIVE */
  if (setsockopt (sock, SOL_SOCKET, SO_KEEPALIVE, &optval, optlen) < 0)
    {
      assert (0);
    }

  return 0;
}

void
ut_cd_work_dir (void)
{
  char path[BROKER_PATH_MAX];

  chdir (envvar_bindir_file (path, BROKER_PATH_MAX, ""));
}

void
ut_cd_root_dir (void)
{
  chdir (envvar_root ());
}

void
as_pid_file_create (char *br_name, int as_index)
{
  FILE *fp;
  char as_pid_file_name[BROKER_PATH_MAX];

  ut_get_as_pid_name (as_pid_file_name, br_name, as_index, BROKER_PATH_MAX);

  fp = fopen (as_pid_file_name, "w");
  if (fp)
    {
      fprintf (fp, "%d\n", (int) getpid ());
      fclose (fp);
    }
}

float
ut_get_avg_from_array (int array[], int size)
{
  int i, total = 0;
  for (i = 0; i < size; i++)
    {
      total += array[i];
    }

  return (float) total / size;
}

bool
ut_is_appl_server_ready (int pid, char *ready_flag)
{
  unsigned int i;

  for (i = 0; i < SERVICE_READY_WAIT_COUNT; i++)
    {
      if (*ready_flag == true)
	{
	  return true;
	}
      else
	{
	  if (kill (pid, 0) == 0)
	    {
	      THREAD_SLEEP (10);
	      continue;
	    }
	  else
	    {
	      return false;
	    }
	}
    }

  return false;
}

int
ut_get_broker_port_name (char *port_name, size_t len,
			 const T_BROKER_INFO * br_info)
{
  char filename[BROKER_PATH_MAX] = "";

  if (br_info->broker_type == NORMAL_BROKER && br_info->name[0] != '\0')
    {
      snprintf (filename, sizeof (filename), "rye_broker.%s", br_info->name);
    }
  else if (br_info->broker_type == SHARD_MGMT &&
	   br_info->shard_global_dbname[0] != '\0')
    {
      snprintf (filename, sizeof (filename), "shard_mgmt.%s",
		br_info->shard_global_dbname);
    }

  if (filename[0] == '\0')
    {
      return -1;
    }

  envvar_socket_file (port_name, len, filename);
  return 0;
}

void
ut_get_as_port_name (char *port_name, const char *broker_name,
		     int as_id, int len)
{
  char filename[BROKER_PATH_MAX];
  snprintf (filename, sizeof (filename), "%s.%d", broker_name, as_id + 1);
  envvar_socket_file (port_name, len, filename);
}

double
ut_size_string_to_kbyte (const char *size_str, const char *default_unit)
{
  double val;
  char *end = NULL;
  const char *unit = NULL;

  if (size_str == NULL || default_unit == NULL)
    {
      assert (false);
      return -1.0;
    }

  val = strtod (size_str, &end);
  if (end == size_str)
    {
      return -1.0;
    }

  if (isalpha (*end))
    {
      unit = end;
    }
  else
    {
      unit = default_unit;
    }

  if (strcasecmp (unit, "b") == 0)
    {
      /* byte */
      val = val / ONE_K;
    }
  else if ((strcasecmp (unit, "k") == 0) || (strcasecmp (unit, "kb") == 0))
    {
      /* kilo */
    }
  else if ((strcasecmp (unit, "m") == 0) || (strcasecmp (unit, "mb") == 0))
    {
      /* mega */
      val = val * ONE_K;
    }
  else if ((strcasecmp (unit, "g") == 0) || (strcasecmp (unit, "gb") == 0))
    {
      /* giga */
      val = val * ONE_M;
    }
  else
    {
      return -1.0;
    }

  if (val > INT_MAX)		/* spec */
    {
      return -1.0;
    }

  return val;
}

double
ut_time_string_to_sec (const char *time_str, const char *default_unit)
{
  double val;
  char *end;
  const char *unit;

  if (time_str == NULL || default_unit == NULL)
    {
      assert (false);
      return -1.0;
    }

  val = strtod (time_str, &end);
  if (end == time_str)
    {
      return -1.0;
    }

  if (isalpha (*end))
    {
      unit = end;
    }
  else
    {
      unit = default_unit;
    }

  if ((strcasecmp (unit, "ms") == 0) || (strcasecmp (unit, "msec") == 0))
    {
      /* millisecond */
      val = val / ONE_SEC;
    }
  else if ((strcasecmp (unit, "s") == 0) || (strcasecmp (unit, "sec") == 0))
    {
      /* second */
    }
  else if (strcasecmp (unit, "min") == 0)
    {
      /* minute */
      val = val * ONE_MIN / ONE_SEC;
    }
  else if (strcasecmp (unit, "h") == 0)
    {
      /* hours */
      val = val * ONE_HOUR / ONE_SEC;
    }
  else
    {
      return -1.0;
    }

  if (val > INT_MAX)		/* spec */
    {
      return -1.0;
    }

  return val;
}

void
ut_get_as_pid_name (char *pid_name, char *br_name, int as_index, int len)
{
  char filename[BROKER_PATH_MAX];
  snprintf (filename, sizeof (filename), "%s_%d.pid", br_name, as_index + 1);
  envvar_as_pid_dir_file (pid_name, len, filename);
}

T_BROKER_INFO *
ut_find_broker (T_BROKER_INFO * br_info, int num_brs, const char *brname,
		char broker_type)
{
  int i;

  if (brname == NULL)
    {
      assert (0);
      return NULL;
    }

  for (i = 0; i < num_brs; i++)
    {
      if (strcmp (brname, br_info[i].name) == 0 &&
	  br_info[i].broker_type == broker_type)
	{
	  return (&br_info[i]);
	}
    }

  return NULL;
}

T_BROKER_INFO *
ut_find_shard_mgmt (T_BROKER_INFO * br_info, int num_brs, const char *dbname)
{
  int i;

  if (dbname == NULL)
    {
      assert (0);
      return NULL;
    }

  for (i = 0; i < num_brs; i++)
    {
      if (br_info[i].broker_type == SHARD_MGMT &&
	  strcmp (dbname, br_info[i].shard_global_dbname) == 0)
	{
	  return (&br_info[i]);
	}
    }

  return NULL;
}

void
ut_make_broker_process_name (char *buf, size_t size,
			     const T_BROKER_INFO * br_info)
{
  char tmp_proc_name[PATH_MAX];
  if (br_info->broker_type == SHARD_MGMT)
    {
      snprintf (tmp_proc_name, sizeof (tmp_proc_name), "%s_shard_mgmt_%s",
		NAME_CAS_BROKER, br_info->shard_global_dbname);
    }
  else if (br_info->broker_type == SHARD_MGMT)
    {
      snprintf (tmp_proc_name, sizeof (tmp_proc_name), "%s_local_mgmt",
		NAME_CAS_BROKER);
    }
  else
    {
      snprintf (tmp_proc_name, sizeof (tmp_proc_name), "%s_%s",
		NAME_CAS_BROKER, br_info->name);
    }

  envvar_process_name (buf, size, tmp_proc_name);
}

void
ut_make_cas_process_name (char *buf, size_t size, const char *broker_name,
			  int as_index)
{
  char tmp_proc_name[PATH_MAX];
  snprintf (tmp_proc_name, sizeof (tmp_proc_name), "%s_%s_%d",
	    APPL_SERVER_CAS_NAME, broker_name, as_index + 1);

  envvar_process_name (buf, size, tmp_proc_name);
}
