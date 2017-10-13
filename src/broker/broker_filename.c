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
 * broker_filename.c -
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>

#include "system_parameter.h"
#include "broker_filename.h"
#include "environment_variable.h"

#define NUM_RYE_FILE		MAX_RYE_FILE
static T_RYE_FILE_INFO rye_file[NUM_RYE_FILE] = {
  {FID_RYE_BROKER_CONF, ""},
  {FID_CAS_TMP_DIR, ""},
  {FID_VAR_DIR, ""},
  {FID_SOCK_DIR, ""},
  {FID_AS_PID_DIR, ""},
  {FID_LOG_DIR, ""},
  {FID_SQL_LOG_DIR, ""},
  {FID_ERR_LOG_DIR, ""},
  {FID_ACCESS_CONTROL_FILE, ""},
  {FID_SLOW_LOG_DIR, ""}
};

void
rye_file_reset ()
{
  int i;

  for (i = 0; i < NUM_RYE_FILE; i++)
    {
      rye_file[i].file_name[0] = '\0';
    }
}

void
set_rye_file (T_RYE_FILE_ID fid, const char *value, const char *br_name)
{
  if (value == NULL)
    {
      return;
    }

  if (br_name == NULL)
    {
      br_name = "";
    }

  switch (fid)
    {
    case FID_LOG_DIR:
      if (IS_ABS_PATH (value))
	{
	  snprintf (rye_file[FID_LOG_DIR].file_name, BROKER_PATH_MAX,
		    "%s/%s/", value, br_name);
	}
      else
	{
	  snprintf (rye_file[FID_LOG_DIR].file_name, BROKER_PATH_MAX,
		    "%s/%s/%s/", envvar_root (), value, br_name);
	}

      snprintf (rye_file[FID_SQL_LOG_DIR].file_name, BROKER_PATH_MAX,
		"%s%s/", rye_file[FID_LOG_DIR].file_name, SQL_LOG_DIR);
      snprintf (rye_file[FID_SLOW_LOG_DIR].file_name, BROKER_PATH_MAX,
		"%s%s/", rye_file[FID_LOG_DIR].file_name, SLOW_LOG_DIR);
      snprintf (rye_file[FID_ERR_LOG_DIR].file_name, BROKER_PATH_MAX,
		"%s%s/", rye_file[FID_LOG_DIR].file_name, ERR_LOG_DIR);
      break;

    case FID_CAS_TMP_DIR:
    case FID_VAR_DIR:
    case FID_SOCK_DIR:
    case FID_AS_PID_DIR:
      if (IS_ABS_PATH (value))
	{
	  snprintf (rye_file[FID_LOG_DIR].file_name, BROKER_PATH_MAX,
		    "%s/", value);
	}
      else
	{
	  snprintf (rye_file[FID_LOG_DIR].file_name, BROKER_PATH_MAX,
		    "%s/%s/", envvar_root (), value);
	}
      break;

    case FID_RYE_BROKER_CONF:
      if (IS_ABS_PATH (value))
	{
	  snprintf (rye_file[fid].file_name, BROKER_PATH_MAX, value);
	}
      else
	{
	  snprintf (rye_file[fid].file_name, BROKER_PATH_MAX, "%s/%s",
		    envvar_root (), value);
	}
      break;
    default:
      assert (0);
    }
}

char *
get_rye_file (T_RYE_FILE_ID fid, char *buf, size_t len)
{
  assert (fid == rye_file[fid].fid);

  buf[0] = '\0';

  if (strlen (rye_file[fid].file_name) > 0)
    {
      snprintf (buf, len, "%s", rye_file[fid].file_name);
      return buf;
    }

  switch (fid)
    {
    case FID_RYE_BROKER_CONF:
      (void) envvar_confdir_file (buf, len, sysprm_auto_conf_file_name);
      break;
    case FID_CAS_TMP_DIR:
      envvar_tmpdir_file (buf, len, "");
      break;
    case FID_VAR_DIR:
      envvar_vardir_file (buf, len, "");
      break;
    case FID_SOCK_DIR:
      envvar_vardir_file (buf, len, "RYE_SOCK/");
      break;
    case FID_AS_PID_DIR:
      envvar_vardir_file (buf, len, "as_pid/");
      break;
    case FID_LOG_DIR:
      envvar_ryelogdir_file (buf, len, DEFAULT_LOG_DIR);
      break;
    case FID_SQL_LOG_DIR:
      envvar_ryelogdir_file (buf, len, DEFAULT_LOG_DIR SQL_LOG_DIR);
      break;
    case FID_SLOW_LOG_DIR:
      envvar_ryelogdir_file (buf, len, DEFAULT_LOG_DIR SLOW_LOG_DIR);
      break;
    case FID_ERR_LOG_DIR:
      envvar_ryelogdir_file (buf, len, DEFAULT_LOG_DIR ERR_LOG_DIR);
      break;
    case FID_ACCESS_CONTROL_FILE:
      (void) envvar_confdir_file (buf, len, BROKER_ACL_FILE);
      break;
    default:
      break;
    }

  snprintf (rye_file[fid].file_name, BROKER_PATH_MAX, "%s", buf);
  return buf;
}
