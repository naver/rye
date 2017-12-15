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
 * util_common.c - utility common functions
 */

#ident "$Id$"

#include <ctype.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "config.h"
#include "porting.h"
#include "utility.h"
#include "message_catalog.h"
#include "error_code.h"
#include "tcp.h"

#include "mprec.h"
#include "system_parameter.h"
#include "databases_file.h"
#include "util_func.h"
#include "ini_parser.h"
#include "environment_variable.h"
#include "heartbeat.h"
#include "log_impl.h"

typedef enum
{
  EXISTING_DATABASE,
  NEW_DATABASE
} DATABASE_NAME;

static int utility_get_option_index (UTIL_ARG_MAP * arg_map, int arg_ch);
static int check_database_name_local (const char *name,
				      int existing_or_new_db);
static char **util_split_ha_db (const char *str);
static int util_get_ha_parameters (PRM_NODE_LIST * ha_node_list,
				   const char **ha_db_list_p,
				   const char **ha_copy_log_base_p,
				   int *ha_max_log_applier_p);

/*
 * utility_initialize() - initialize Rye library
 *   return: 0 if success, otherwise -1
 */
int
utility_initialize ()
{
  if (msgcat_init () != NO_ERROR)
    {
      PRINT_AND_LOG_ERR_MSG ("Unable to access system message catalog.\n");
      return ER_BO_CANNOT_ACCESS_MESSAGE_CATALOG;
    }

  return NO_ERROR;
}

/*
 * utility_get_generic_message() - get a string of the generic-utility from the catalog
 *   return: message string
 *   message_index(in): an index of the message string
 */
const char *
utility_get_generic_message (int message_index)
{
  return (msgcat_message (MSGCAT_CATALOG_UTILS,
			  MSGCAT_UTIL_SET_GENERIC, message_index));
}

/*
 * check_database_name() - check validation of the name of a database for existing db
 *   return: error code
 *   name(in): the name of a database
 */
int
check_database_name (const char *name)
{
  return check_database_name_local (name, EXISTING_DATABASE);
}

/*
 * check_database_name() - check validation of the name of a database for new db
 *   return: error code
 *   name(in): the name of a database
 */
int
check_new_database_name (const char *name)
{
  return check_database_name_local (name, NEW_DATABASE);
}

/*
 * check_database_name_local() - check validation of the name of a database
 *   return: error code
 *   name(in): the name of a database
 *   existing_or_new_db(in): whether db is existing or new one
 */
static int
check_database_name_local (const char *name, int existing_or_new_db)
{
  int status = NO_ERROR;
  int i = 0;

  if (name[0] == '#')
    {
      status = ER_GENERIC_ERROR;
    }
  else
    {
      for (i = 0; name[i] != 0; i++)
	{
	  if (isspace (name[i]) || name[i] == '/' || name[i] == '\\'
	      || !isprint (name[i])
	      || (existing_or_new_db == NEW_DATABASE && name[i] == '@'))
	    {
	      status = ER_GENERIC_ERROR;
	      break;
	    }
	}
    }

  if (status == ER_GENERIC_ERROR)
    {
      const char *message =
	utility_get_generic_message (MSGCAT_UTIL_GENERIC_BAD_DATABASE_NAME);
      if (message != NULL)
	{
	  PRINT_AND_LOG_ERR_MSG (message, name[i], name);
	}
    }
  return status;
}

/*
 * check_volume_name() - check validation of the name of a volume
 *   return: error code
 *   name(in): the name of a volume
 */
int
check_volume_name (const char *name)
{
  int status = NO_ERROR;
  int i = 0;

#if 1
  assert (name == NULL);
#endif

  if (name == NULL)
    {
      return NO_ERROR;
    }

#if 1
  assert (false);
#endif

  if (name[0] == '#')
    {
      status = ER_GENERIC_ERROR;
    }
  else
    {
      for (i = 0; name[i] != 0; i++)
	{
	  if (isspace (name[i]) || name[i] == '/' || name[i] == '\\'
	      || !isprint (name[i]))
	    {
	      status = ER_GENERIC_ERROR;
	      break;
	    }
	}
    }

  if (status == ER_GENERIC_ERROR)
    {
      const char *message =
	utility_get_generic_message (MSGCAT_UTIL_GENERIC_BAD_VOLUME_NAME);
      if (message != NULL)
	{
	  PRINT_AND_LOG_ERR_MSG (message, name[i], name);
	}
    }
  return status;
}

/*
 * utility_get_option_index() - search an option in the map of arguments
 *   return: an index of a founded option
 *   arg_map(in): the map of arguments
 *   arg_ch(in): the value of an argument
 */
static int
utility_get_option_index (UTIL_ARG_MAP * arg_map, int arg_ch)
{
  int i;

  for (i = 0; arg_map[i].arg_ch; i++)
    {
      if (arg_map[i].arg_ch == arg_ch)
	{
	  return i;
	}
    }
  return -1;
}

/*
 * utility_get_option_int_value() - search an option in the map of arguments
 *      and return that value
 *   return: the value of a searched argument
 *   arg_map(in): the map of arguments
 *   arg_ch(in): the value of an argument
 */
int
utility_get_option_int_value (UTIL_ARG_MAP * arg_map, int arg_ch)
{
  int index = utility_get_option_index (arg_map, arg_ch);
  if (index != -1)
    {
      return arg_map[index].arg_value.i;
    }
  return 0;
}

/*
 * get_option_bool_value() - search an option in the map of arguments
 *      and return that value
 *   return: the value of a searched argument
 *   arg_map(in): the map of arguments
 *   arg_ch(in): the value of an argument
 */
bool
utility_get_option_bool_value (UTIL_ARG_MAP * arg_map, int arg_ch)
{
  int index = utility_get_option_index (arg_map, arg_ch);
  if (index != -1)
    {
      if (arg_map[index].arg_value.i == 1)
	{
	  return true;
	}
    }
  return false;
}

/*
 * get_option_string_value() - search an option in the map of arguments
 *      and return that value
 *   return: the value of a searched argument
 *   arg_map(in): the map of arguments
 *   arg_ch(in): the value of an argument
 */
const char *
utility_get_option_string_value (UTIL_ARG_MAP * arg_map, int arg_ch,
				 int index)
{
  int arg_index = utility_get_option_index (arg_map, arg_ch);
  if (arg_index != -1)
    {
      if (arg_ch == OPTION_STRING_TABLE)
	{
	  if (index < arg_map[arg_index].value_info.num_strings)
	    {
	      return (((const char *const *) arg_map[arg_index].arg_value.
		       p)[index]);
	    }
	}
      else
	{
	  return ((const char *) arg_map[arg_index].arg_value.p);
	}
    }
  return NULL;
}

/*
 * utility_get_option_bigint_value() - search an option in the map of arguments
 *      and return that value
 *   return: the value of a searched argument
 *   arg_map(in): the map of arguments
 *   arg_ch(in): the value of an argument
 */
INT64
utility_get_option_bigint_value (UTIL_ARG_MAP * arg_map, int arg_ch)
{
  int index = utility_get_option_index (arg_map, arg_ch);
  if (index != -1)
    {
      return arg_map[index].arg_value.l;
    }
  return 0;
}

int
utility_get_option_string_table_size (UTIL_ARG_MAP * arg_map)
{
  int arg_index = utility_get_option_index (arg_map, OPTION_STRING_TABLE);
  if (arg_index != -1)
    {
      return arg_map[arg_index].value_info.num_strings;
    }
  return 0;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * fopen_ex - open a file for variable architecture
 *    return: FILE *
 *    filename(in): path to the file to open
 *    type(in): open type
 */
FILE *
fopen_ex (const char *filename, const char *type)
{
#if defined(I386)
  return fopen64 (filename, type);
#else
  return fopen (filename, type);
#endif
}
#endif

/*
 * utility_keyword_search
 */
int
utility_keyword_search (UTIL_KEYWORD * keywords, int *keyval_p,
			const char **keystr_p)
{
  UTIL_KEYWORD *keyp;

  if (*keyval_p >= 0 && *keystr_p == NULL)
    {
      /* get keyword string from keyword value */
      for (keyp = keywords; keyp->keyval >= 0; keyp++)
	{
	  if (*keyval_p == keyp->keyval)
	    {
	      *keystr_p = keyp->keystr;
	      return NO_ERROR;
	    }
	}
    }
  else if (*keyval_p < 0 && *keystr_p != NULL)
    {
      /* get keyword value from keyword string */
      for (keyp = keywords; keyp->keystr != NULL; keyp++)
	{
	  if (!strcasecmp (*keystr_p, keyp->keystr))
	    {
	      *keyval_p = keyp->keyval;
	      return NO_ERROR;
	    }
	}
    }
  return ER_FAILED;
}

/*
 * utility_localtime - transform date and time to broken-down time
 *    return: 0 if success, otherwise -1
 *    ts(in): pointer of time_t data value
 *    result(out): pointer of struct tm which will store the broken-down time
 */
int
utility_localtime (const time_t * ts, struct tm *result)
{
  struct tm *tm_p, tm_val;

  if (result == NULL)
    {
      return -1;
    }

  tm_p = localtime_r (ts, &tm_val);
  if (tm_p == NULL)
    {
      memset (result, 0, sizeof (struct tm));
      return -1;
    }

  memcpy (result, tm_p, sizeof (struct tm));
  return 0;
}

static char **
util_split_ha_db (const char *str)
{
  return util_split_string (str, " ,:");
}

static int
util_get_ha_parameters (PRM_NODE_LIST * ha_node_list,
			const char **ha_db_list_p,
			const char **ha_copy_log_base_p,
			int *ha_max_log_applier_p)
{
  int error = NO_ERROR;

  *(ha_db_list_p) = prm_get_string_value (PRM_ID_HA_DB_LIST);
  if (*(ha_db_list_p) == NULL || **(ha_db_list_p) == '\0')
    {
      const char *message =
	utility_get_generic_message (MSGCAT_UTIL_GENERIC_INVALID_PARAMETER);
      fprintf (stderr, message, prm_get_name (PRM_ID_HA_DB_LIST), "");
      return ER_GENERIC_ERROR;
    }

  prm_get_ha_node_list (ha_node_list);

  *(ha_max_log_applier_p) = prm_get_integer_value (PRM_ID_HA_MAX_LOG_APPLIER);

  *(ha_copy_log_base_p) = envvar_get (DATABASES_ENVNAME);
  if (*(ha_copy_log_base_p) == NULL)
    {
      *(ha_copy_log_base_p) = ".";
    }

  return error;
}

/*
 * util_free_ha_conf -
 *
 * return:
 *
 * NOTE:
 */
void
util_free_ha_conf (HA_CONF * ha_conf)
{
  int i;
  HA_NODE_CONF *nc;

  for (i = 0, nc = ha_conf->node_conf; i < ha_conf->num_node_conf; i++)
    {
      if (nc[i].copy_log_base)
	{
	  free_and_init (nc[i].copy_log_base);
	}
    }
  free_and_init (ha_conf->node_conf);
  ha_conf->num_node_conf = 0;
  ha_conf->node_conf = NULL;

  if (ha_conf->db_names)
    {
      util_free_string_array (ha_conf->db_names);
      ha_conf->db_names = NULL;
    }

  return;
}

/*
 * util_make_ha_conf -
 *
 * return:
 *
 * NOTE:
 */
int
util_make_ha_conf (HA_CONF * ha_conf)
{
  int error = NO_ERROR;
  int i;
  const char *ha_db_list_p = NULL;
  const char *ha_copy_log_base_p;
  int ha_max_log_applier;
  PRM_NODE_LIST ha_node_list;

  error = util_get_ha_parameters (&ha_node_list, &ha_db_list_p,
				  &ha_copy_log_base_p, &ha_max_log_applier);
  if (error != NO_ERROR)
    {
      return error;
    }

  ha_conf->db_names = util_split_ha_db (ha_db_list_p);
  if (ha_conf->db_names == NULL)
    {
      const char *message =
	utility_get_generic_message (MSGCAT_UTIL_GENERIC_NO_MEM);
      fprintf (stderr, message);

      error = ER_GENERIC_ERROR;
      goto ret;
    }

  ha_conf->node_conf =
    (HA_NODE_CONF *) malloc (sizeof (HA_NODE_CONF) * ha_node_list.num_nodes);
  if (ha_conf->node_conf == NULL)
    {
      const char *message =
	utility_get_generic_message (MSGCAT_UTIL_GENERIC_NO_MEM);
      fprintf (stderr, message);

      error = ER_GENERIC_ERROR;
      goto ret;
    }
  memset ((void *) ha_conf->node_conf, 0,
	  sizeof (HA_NODE_CONF) * ha_node_list.num_nodes);
  ha_conf->num_node_conf = ha_node_list.num_nodes;
  ha_conf->max_log_applier = ha_max_log_applier;

  for (i = 0; i < ha_node_list.num_nodes; i++)
    {
      ha_conf->node_conf[i].node = ha_node_list.nodes[i];
      ha_conf->node_conf[i].copy_log_base = strdup (ha_copy_log_base_p);

      if (ha_conf->node_conf[i].copy_log_base == NULL)
	{
	  const char *message =
	    utility_get_generic_message (MSGCAT_UTIL_GENERIC_NO_MEM);
	  fprintf (stderr, message);

	  error = ER_GENERIC_ERROR;
	  goto ret;
	}
    }

ret:
  if (error != NO_ERROR)
    {
      util_free_ha_conf (ha_conf);
    }

  return error;
}

#if defined(NDEBUG)
/*
 * util_redirect_stdout_to_null - redirect stdout/stderr to /dev/null
 *
 * return:
 *
 */
void
util_redirect_stdout_to_null (void)
{
  const char *null_dev = "/dev/null";
  int fd;

  fd = open (null_dev, O_WRONLY);
  if (fd != -1)
    {
      close (1);
      close (2);
      dup2 (fd, 1);
      dup2 (fd, 2);
      close (fd);
    }
}
#endif

/*
 * util_size_to_byte -
 *
 * return:
 *
 */
static int
util_size_to_byte (double *pre, const char *post)
{
  if (strcasecmp (post, "b") == 0)
    {
      /* bytes */
    }
  else if ((strcasecmp (post, "k") == 0) || (strcasecmp (post, "kb") == 0))
    {
      /* kilo */
      *pre = *pre * ONE_K;
    }
  else if ((strcasecmp (post, "m") == 0) || (strcasecmp (post, "mb") == 0))
    {
      /* mega */
      *pre = *pre * ONE_M;
    }
  else if ((strcasecmp (post, "g") == 0) || (strcasecmp (post, "gb") == 0))
    {
      /* giga */
      *pre = *pre * ONE_G;
    }
  else if ((strcasecmp (post, "t") == 0) || (strcasecmp (post, "tb") == 0))
    {
      /* tera */
      *pre = *pre * ONE_T;
    }
  else if ((strcasecmp (post, "p") == 0) || (strcasecmp (post, "pb") == 0))
    {
      /* peta */
      *pre = *pre * ONE_P;
    }
  else
    {
      return ER_FAILED;
    }

  return NO_ERROR;
}

/*
 * util_byte_to_size_string -
 *
 * return:
 *
 */
int
util_byte_to_size_string (char *buf, size_t len, INT64 size_num)
{
  char num_str[100];
  const char *ss = "BKMGTP";
  double v = (double) size_num;
  int pow = 0;
  int i, decpt, sign, num_len;
  char *rve;

  if (buf == NULL)
    {
      return ER_FAILED;
    }

  buf[0] = '\0';

  while (pow < 6 && v >= ONE_K)
    {
      pow++;
      v /= ONE_K;
    }

  _dtoa (v, 3, 1, &decpt, &sign, &rve, num_str, 0);
  num_str[99] = '\0';
  num_len = strlen (num_str);

  if ((int) len < decpt + 4)
    {
      return ER_FAILED;
    }

  for (i = 0; i <= decpt + 1; i++)
    {
      if (i == decpt)
	{
	  buf[i] = '.';
	}
      else if (i == decpt + 1)
	{
	  if (num_len > 0 && num_len > decpt)
	    {
	      buf[i] = num_str[num_len - 1];
	    }
	  else
	    {
	      buf[i] = '0';
	    }
	  buf[i + 1] = ss[pow];
	  buf[i + 2] = '\0';
	}
      else
	{
	  if (num_len < decpt && i >= num_len)
	    {
	      buf[i] = '0';
	    }
	  else
	    {
	      buf[i] = num_str[i];
	    }
	}
    }

  return NO_ERROR;
}

/*
 * util_size_string_to_byte -
 *
 * return:
 *
 */
int
util_size_string_to_byte (INT64 * size_num, const char *size_str)
{
  double val;
  const char *default_unit = "B";
  char *end;
  const char *size_unit;

  if (size_str == NULL || size_num == NULL)
    {
      return ER_FAILED;
    }
  *size_num = 0;

  val = strtod (size_str, &end);
  if (end == size_str)
    {
      return ER_FAILED;
    }

  if (val < 0)
    {
      return ER_FAILED;
    }

  if (*end != '\0')
    {
      size_unit = end;
    }
  else
    {
      size_unit = default_unit;
    }

  if (util_size_to_byte (&val, size_unit) != NO_ERROR)
    {
      return ER_FAILED;
    }

  *size_num = (INT64) val;
  return NO_ERROR;
}

/*
 * util_time_to_byte -
 *
 * return:
 *
 */
static int
util_time_to_msec (double *pre, const char *post)
{
  if ((strcasecmp (post, "ms") == 0) || (strcasecmp (post, "msec") == 0))
    {
      /* millisecond */
    }
  else if ((strcasecmp (post, "s") == 0) || (strcasecmp (post, "sec") == 0))
    {
      /* second */
      *pre = *pre * ONE_SEC;
    }
  else if (strcasecmp (post, "min") == 0)
    {
      /* minute */
      *pre = *pre * ONE_MIN;
    }
  else if (strcasecmp (post, "h") == 0)
    {
      /* hours */
      *pre = *pre * ONE_HOUR;
    }
  else
    {
      return ER_FAILED;
    }

  return NO_ERROR;
}

/*
 * util_msec_to_time_string -
 *
 * return:
 *
 */
int
util_msec_to_time_string (char *buf, size_t len, INT64 msec_num)
{
  INT64 v = msec_num;
  INT64 sec, msec;
  int error = 0;

  if (buf == NULL)
    {
      assert (false);
      return ER_FAILED;
    }
  buf[0] = '\0';

  sec = v / ONE_SEC;

  if (sec > 0)
    {
      msec = v % ONE_SEC;
      error = snprintf (buf, len, "%lld.%03lldsec",
			(long long) sec, (long long) msec);
    }
  else if (v < 0)
    {
      error = snprintf (buf, len, "%lld", (long long) v);
    }
  else
    {
      error = snprintf (buf, len, "%lldmsec", (long long) v);
    }

  if (error < 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_GENERIC_ERROR, 1, "invalid buffer length");
      return ER_GENERIC_ERROR;
    }

  return NO_ERROR;
}

/*
 * util_time_string_to_msec -
 *   return:
 *
 *   msec_num(out):
 *   time_str(in):
 */
int
util_time_string_to_msec (INT64 * msec_num, const char *time_str)
{
  double val;
  const char *default_unit = "ms";
  char *end;
  const char *time_unit;

  if (time_str == NULL || msec_num == NULL)
    {
      return ER_FAILED;
    }
  *msec_num = 0;

  val = strtod (time_str, &end);
  if (end == time_str)
    {
      return ER_FAILED;
    }

  if (val < 0)
    {
      *msec_num = (INT64) val;
      return NO_ERROR;
    }

  if (*end != '\0')
    {
      time_unit = end;
    }
  else
    {
      time_unit = default_unit;
    }

  if (util_time_to_msec (&val, time_unit) != NO_ERROR)
    {
      return ER_FAILED;
    }

  *msec_num = (INT64) val;
  return NO_ERROR;
}

/*
 * util_print_deprecated -
 *
 * return:
 *
 */
void
util_print_deprecated (const char *option)
{
  int cat = MSGCAT_CATALOG_UTILS;
  int set = MSGCAT_UTIL_SET_GENERIC;
  int msg = MSGCAT_UTIL_GENERIC_DEPRECATED;
  const char *fmt = msgcat_message (cat, set, msg);
  if (fmt == NULL)
    {
      fprintf (stderr, "error: msgcat_message");
    }
  else
    {
      fprintf (stderr, fmt, option);
    }
}
