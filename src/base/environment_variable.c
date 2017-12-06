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
 * environment_variable.c : Functions for manipulating the environment variable
 */

#ident "$Id$"

#include "config.h"

#include <stdlib.h>
#include <errno.h>
#include <ctype.h>
#include <assert.h>

#include "porting.h"
#include "error_code.h"
#include "databases_file.h"
#include "environment_variable.h"

#define RYE_CONF_FILE	"rye-auto.conf"

/* available root directory symbols; NULL terminated array */
static const char envvar_Prefix_name[] = "RYE";
static const char *envvar_Prefix = NULL;
static const char *envvar_Root = NULL;

#define _ENVVAR_MAX_LENGTH      255

typedef enum
{
  ENV_INVALID_DIR,
  ENV_DONT_EXISTS_ROOT,
  ENV_MUST_ABS_PATH,
  ENV_TOO_LONG
} ENV_ERR_MSG;

static const char *env_msg[] = {
  "The directory in $%s is invalid. (%s)\n",
  "The root directory environment variable $%s is not set.\n",
  "The $%s should be an absolute path. (%s)\n",
  "The $%s is too long. (%s)\n"
};

/*
 * envvar_prefix - find a recognized prefix symbol
 *   return: prefix symbol
 */
static const char *
envvar_prefix (void)
{
  if (envvar_Prefix == NULL)
    {
      envvar_Root = getenv (envvar_Prefix_name);
      if (envvar_Root != NULL)
	{
	  if (access (envvar_Root, F_OK) != 0)
	    {
	      fprintf (stderr, env_msg[ENV_INVALID_DIR],
		       envvar_Prefix_name, envvar_Root);
	      fflush (stderr);
	      exit (1);
	    }

	  envvar_Prefix = envvar_Prefix_name;
	}
      else
	{
	  fprintf (stderr, env_msg[ENV_DONT_EXISTS_ROOT], envvar_Prefix_name);
	  fflush (stderr);
	  exit (1);
	}
    }

  return envvar_Prefix;
}

/*
 * envvar_root - get value of the root directory environment variable
 *   return: root directory
 */
const char *
envvar_root (void)
{
  if (envvar_Root == NULL)
    {
      envvar_prefix ();
    }

  return envvar_Root;
}

/*
 * envvar_name - add the prefix symbol to an environment variable name
 *   return: prefixed name
 *   buf(out): string buffer to store the prefixed name
 *   size(out): size of the buffer
 *   name(in): an environment variable name
 */
static const char *
envvar_name (char *buf, size_t size, const char *name)
{
  snprintf (buf, size, "%s_%s", envvar_prefix (), name);
  return buf;
}

/*
 * envvar_get - get value of an prefixed environment variable
 *   return: a string containing the value for the specified environment
 *           variable
 *   name(in): environment variable name without prefix
 *
 * Note: the prefix symbol will be added to the name
 */
const char *
envvar_get (const char *name)
{
  char buf[_ENVVAR_MAX_LENGTH];

  return getenv (envvar_name (buf, _ENVVAR_MAX_LENGTH, name));
}

/*
 * enclosing_method - change value of an prefixed environment variable
 *   return: error code
 *   name(in): environment variable name without prefix
 *   val(in): the value to be set to the environment variable
 *
 * Note: the prefix symbol will be added to the name
 */
int
envvar_set (const char *name, const char *val)
{
  char buf[_ENVVAR_MAX_LENGTH];
  int ret;

  envvar_name (buf, _ENVVAR_MAX_LENGTH, name);
  ret = setenv (buf, val, 1);
  if (ret != 0)
    {
      return ER_FAILED;
    }

  return NO_ERROR;
}

char *
envvar_bindir_file (char *path, size_t size, const char *filename)
{
  assert (filename != NULL);
  snprintf (path, size, "%s/bin/%s", envvar_root (), filename);
  return path;
}

char *
envvar_localedir_file (char *path, size_t size, const char *langpath,
		       const char *filename)
{
  assert (filename != NULL);
  snprintf (path, size, "%s/msg/%s/%s", envvar_root (), langpath, filename);
  return path;
}

char *
envvar_confdir_file (char *path, size_t size, const char *filename)
{
  const char *env_name;

  assert (filename != NULL);

  env_name = envvar_get (DATABASES_ENVNAME);
  if (env_name == NULL || strlen (env_name) == 0)
    {
      return NULL;
    }

  snprintf (path, size, "%s%c%s", env_name, PATH_SEPARATOR, filename);
  path[size - 1] = '\0';

  return path;
}

static char *
envvar_confdir_file_with_dir (char *path, size_t size,
			      const char *dirname, const char *filename)
{
  char dirpath[PATH_MAX];

  assert (dirname != NULL);
  assert (filename != NULL);

  envvar_confdir_file (dirpath, PATH_MAX, dirname);

  snprintf (path, size, "%s/%s", dirpath, filename);

  return path;
}

char *
envvar_rye_conf_file (char *path, size_t size)
{
  return envvar_confdir_file (path, size, RYE_CONF_FILE);
}

char *
envvar_db_dir (char *path, size_t size, const char *db_name)
{
  return envvar_confdir_file (path, size, db_name);
}

char *
envvar_db_log_dir (char *path, size_t size, const char *db_name)
{
  return envvar_confdir_file_with_dir (path, size, db_name, "log");
}

char *
envvar_vardir_file (char *path, size_t size, const char *filename)
{
  return envvar_confdir_file_with_dir (path, size, "var", filename);
}

char *
envvar_socket_file (char *path, size_t size, const char *filename)
{
  char sock_dir[PATH_MAX];

  assert (filename != NULL);

  envvar_vardir_file (sock_dir, PATH_MAX, "RYE_SOCK");
  snprintf (path, size, "%s/%s", sock_dir, filename);
  return path;
}

char *
envvar_as_pid_dir_file (char *path, size_t size, const char *filename)
{
  char as_pid_dir[PATH_MAX];

  assert (filename != NULL);

  envvar_vardir_file (as_pid_dir, PATH_MAX, "as_pid");
  snprintf (path, size, "%s/%s", as_pid_dir, filename);
  return path;
}

char *
envvar_tmpdir_file (char *path, size_t size, const char *filename)
{
  return envvar_confdir_file_with_dir (path, size, "tmp", filename);
}

char *
envvar_ryelogdir_file (char *path, size_t size, const char *filename)
{
  return envvar_confdir_file_with_dir (path, size, "ryelog", filename);
}

static char *
envvar_ryelog_broker_subdir_file (char *path, size_t size, const char *dir1,
				  const char *dir2, const char *filename)
{
  char broker_log_dir[PATH_MAX];
  int n;

  envvar_ryelogdir_file (broker_log_dir, sizeof (broker_log_dir), "broker");

  n = snprintf (path, size, "%s/%s", broker_log_dir, dir1);
  if (dir2 != NULL)
    {
      n += snprintf (path + n, size - n, "/%s", dir2);
    }
  if (filename != NULL)
    {
      snprintf (path + n, size - n, "/%s", filename);
    }
  return path;
}

char *
envvar_ryelog_broker_file (char *path, size_t size, const char *br_name,
			   const char *filename)
{
  envvar_ryelog_broker_subdir_file (path, size, br_name, NULL, filename);
}

char *
envvar_ryelog_broker_sqllog_file (char *path, size_t size,
				  const char *br_name, const char *filename)
{
  envvar_ryelog_broker_subdir_file (path, size, br_name, "sql_log", filename);
}

char *
envvar_ryelog_broker_slowlog_file (char *path, size_t size,
				   const char *br_name, const char *filename)
{
  envvar_ryelog_broker_subdir_file (path, size, br_name, "slow_log",
				    filename);
}

char *
envvar_ryelog_broker_errorlog_file (char *path, size_t size,
				    const char *br_name, const char *filename)
{
  envvar_ryelog_broker_subdir_file (path, size, br_name, "error_log",
				    filename);
}

char *
envvar_broker_acl_file (char *path, size_t size)
{
  return envvar_confdir_file (path, size, BROKER_ACL_FILE);
}
