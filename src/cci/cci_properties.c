/*
 * Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 *
 * - Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * - Neither the name of the <ORGANIZATION> nor the names of its contributors
 *   may be used to endorse or promote products derived from this software without
 *   specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */


/*
 * cci_properties.c -
 */

#ident "$Id$"

/*
 * IMPORTED SYSTEM HEADER FILES
 */
#include "config.h"
#include <string.h>
#include <stdlib.h>
#include <sys/time.h>

/*
 * OTHER IMPORTED HEADER FILES
 */
#include "porting.h"
#include "cci_handle_mng.h"
#include "cci_log.h"
#include "cci_util.h"

/*
 * PRIVATE DEFINITIONS
 */
typedef enum
{
  BOOL_PROPERTY,
  INT_PROPERTY,
  STRING_PROPERTY,
  CON_TYPE_PROPERTY
} T_TYPE_PROPERTY;

typedef struct
{
  const char *name;
  T_TYPE_PROPERTY type;
  void *data;
} T_URL_PROPERTY;

/*
 * PRIVATE FUNCTION PROTOTYPES
 */
static int cci_url_parse_properties (T_URL_PROPERTY props[], int len, const char *properties);
static int cci_url_set_properties (T_URL_PROPERTY props[], int len, const char *name, const char *value);
static int cci_url_set_value (T_URL_PROPERTY * property, const char *value);
static int cci_url_get_int (const char *str, int *value);
static int cci_url_get_bool (const char *str, bool * value);
static void cci_shuffle_althosts (T_ALTER_HOST * handle);

/*
 * INTERFACE VARIABLES
 */

/*
 * PUBLIC VARIABLES
 */

/*
 * PRIVATE VARIABLES
 */

/*
 * IMPLEMENTATION OF INTERFACE FUNCTIONS
 */
static int
cci_url_get_bool (const char *str, bool * value)
{
  static const char *accepts[] = {
    "true", "false", "on", "off", "yes", "no"
  };
  int i, dim;

  if (value == NULL)
    {
      return CCI_ER_INVALID_URL;
    }

  dim = DIM (accepts);
  for (i = 0; i < dim; i++)
    {
      if (strcasecmp (accepts[i], str) == 0)
        {
          *value = (i % 2) == 0;
          return CCI_ER_NO_ERROR;
        }
    }

  return CCI_ER_INVALID_URL;
}

static int
cci_url_get_int (const char *str, int *value)
{
  int v;
  char *end;

  if (value == NULL)
    {
      return CCI_ER_INVALID_URL;
    }

  v = strtol (str, &end, 10);
  if (end != NULL && end[0] != '\0')
    {
      return CCI_ER_INVALID_URL;
    }

  *value = v;
  return CCI_ER_NO_ERROR;
}

static int
cci_url_set_value (T_URL_PROPERTY * property, const char *value)
{
  int error = CCI_ER_NO_ERROR;

  switch (property->type)
    {
    case BOOL_PROPERTY:
      {
        bool v;
        error = cci_url_get_bool (value, &v);
        if (error == CCI_ER_NO_ERROR)
          {
            *((char *) property->data) = v;
          }
        break;
      }
    case INT_PROPERTY:
      {
        int v;
        error = cci_url_get_int (value, &v);
        if (error == CCI_ER_NO_ERROR)
          {
            *((int *) property->data) = v;
          }
        break;
      }
    case STRING_PROPERTY:
      {
        *((char **) property->data) = strdup (value);
        if (*((char **) property->data) == NULL)
          {
            return CCI_ER_NO_MORE_MEMORY;
          }
        break;
      }
    case CON_TYPE_PROPERTY:
      {
        if (strcasecmp (value, "local") == 0)
          {
            *((T_CON_TYPE *) property->data) = CON_TYPE_LOCAL;
          }
        else
          {
            *((T_CON_TYPE *) property->data) = CON_TYPE_GLOBAL;
          }
        break;
      }
    default:
      return CCI_ER_INVALID_URL;
    }

  return error;
}

static int
cci_url_set_properties (T_URL_PROPERTY props[], int len, const char *name, const char *value)
{
  int i, error = CCI_ER_NO_ERROR;

  if (name == NULL || value == NULL)
    {
      return CCI_ER_INVALID_URL;
    }

  for (i = 0; i < len && error == CCI_ER_NO_ERROR; i++)
    {
      if (strcasecmp (name, props[i].name) == 0)
        {
          error = cci_url_set_value (&props[i], value);
          return error;
        }
    }

  if (i == len)
    {
      return CCI_ER_INVALID_URL;
    }

  return error;
}

static int
cci_url_parse_properties (T_URL_PROPERTY props[], int len, const char *properties)
{
  char *token, *save_url = NULL;
  int error = CCI_ER_NO_ERROR;
  char *buffer;

  if (props == NULL)
    {
      return CCI_ER_INVALID_URL;
    }

  buffer = strdup (properties);
  if (buffer == NULL)
    {
      return CCI_ER_NO_MORE_MEMORY;
    }

  token = strtok_r (buffer, "?&", &save_url);
  while (token != NULL && error == CCI_ER_NO_ERROR)
    {
      char *name, *value, *save_property = NULL;
      name = strtok_r (token, "=", &save_property);
      value = strtok_r (NULL, "=", &save_property);
      error = cci_url_set_properties (props, len, name, value);

      token = strtok_r (NULL, "&", &save_url);
    }

  free (buffer);

  return error;
}

int
cci_url_get_althosts (T_ALTER_HOST ** ret_alter_host, const char *server_list, char is_load_balance_mode)
{
  T_HOST_INFO hosts[ALTER_HOST_MAX_SIZE];
  T_ALTER_HOST *alter_host;
  int num_alter_hosts;
  char *data;

  *ret_alter_host = NULL;

  data = strdup (server_list);
  if (data == NULL)
    {
      return CCI_ER_NO_MORE_MEMORY;
    }
  else
    {
      int error = CCI_ER_NO_ERROR;
      char *token, *save_data = NULL, *end;

      for (num_alter_hosts = 0;; num_alter_hosts++)
        {
          char *host, *port_str, *save_alter = NULL;
          int port;

          if (num_alter_hosts >= ALTER_HOST_MAX_SIZE)
            {
              free (data);
              return CCI_ER_INVALID_URL;
            }

          token = strtok_r ((num_alter_hosts == 0 ? data : NULL), ",", &save_data);
          if (token == NULL)
            {
              break;
            }

          host = strtok_r (token, ":", &save_alter);
          if (host == NULL)
            {
              free (data);
              return CCI_ER_INVALID_URL;
            }

          port_str = strtok_r (NULL, ":", &save_alter);
          if (port_str == NULL)
            {
              free (data);
              return CCI_ER_INVALID_URL;
            }
          port = strtol (port_str, &end, 10);
          if (port <= 0 || (end != NULL && end[0] != '\0'))
            {
              free (data);
              return CCI_ER_INVALID_URL;
            }

          error = ut_set_host_info (&hosts[num_alter_hosts], host, port);
          if (error < 0)
            {
              free (data);
              return error;
            }
        }
    }

  free (data);

  if (num_alter_hosts <= 0)
    {
      return CCI_ER_INVALID_URL;
    }

  alter_host = (T_ALTER_HOST *) malloc (sizeof (T_ALTER_HOST) + sizeof (T_HOST_INFO) * num_alter_hosts);
  if (alter_host == NULL)
    {
      return CCI_ER_NO_MORE_MEMORY;
    }

  alter_host->count = num_alter_hosts;
  alter_host->cur_id = 0;
  memcpy (alter_host->host_info, hosts, sizeof (T_HOST_INFO) * num_alter_hosts);

  if (is_load_balance_mode)
    {
      cci_shuffle_althosts (alter_host);
    }

  *ret_alter_host = alter_host;

  return CCI_ER_NO_ERROR;
}

static void
cci_shuffle_althosts (T_ALTER_HOST * alter_host)
{
  struct timeval t;
  int i, j;
  double r;
  struct drand48_data buf;
  T_HOST_INFO temp_host;

  gettimeofday (&t, NULL);

  srand48_r (t.tv_usec, &buf);

  /* Fisher-Yates shuffle */
  for (i = alter_host->count - 1; i > 0; i--)
    {
      drand48_r (&buf, &r);
      j = (int) ((i + 1) * r);

      temp_host = alter_host->host_info[j];
      alter_host->host_info[j] = alter_host->host_info[i];
      alter_host->host_info[i] = temp_host;
    }
}

void
con_property_free (T_CON_PROPERTY * con_property)
{
  FREE_MEM (con_property->log_base);
  FREE_MEM (con_property->log_filename);
}

static void
con_property_init (T_CON_PROPERTY * con_property)
{
  memset (con_property, 0, sizeof (T_CON_PROPERTY));

  con_property->load_balance = false;
  con_property->rc_time = 600;
  con_property->login_timeout = CCI_LOGIN_TIMEOUT_DEFAULT;
  con_property->query_timeout = 0;
  con_property->disconnect_on_query_timeout = false;
  con_property->log_filename = NULL;
  con_property->log_on_exception = false;
  con_property->log_slow_queries = false;
  con_property->slow_query_threshold_millis = 60000;
  con_property->log_trace_api = false;
  con_property->log_trace_network = false;
  con_property->error_on_server_restart = false;
  con_property->con_type = CON_TYPE_GLOBAL;
}

int
cci_url_get_properties (T_CON_PROPERTY * url_property, const char *properties)
{
  T_URL_PROPERTY props[] = {
    {"loadBalance", BOOL_PROPERTY, &url_property->load_balance},
    {"rcTime", INT_PROPERTY, &url_property->rc_time},
    {"loginTimeout", INT_PROPERTY, &url_property->login_timeout},
    {"queryTimeout", INT_PROPERTY, &url_property->query_timeout},
    {"disconnectOnQueryTimeout", BOOL_PROPERTY,
     &url_property->disconnect_on_query_timeout},
    {"logFile", STRING_PROPERTY, &url_property->log_filename},
    {"logOnException", BOOL_PROPERTY, &url_property->log_on_exception},
    {"logSlowQueries", BOOL_PROPERTY, &url_property->log_slow_queries},
    {"slowQueryThresholdMillis", INT_PROPERTY,
     &url_property->slow_query_threshold_millis},
    {"logTraceApi", BOOL_PROPERTY, &url_property->log_trace_api},
    {"logTraceNetwork", BOOL_PROPERTY, &url_property->log_trace_network},
    {"logBaseDir", STRING_PROPERTY, &url_property->log_base},
    /* for backward compatibility */
    {"login_timeout", INT_PROPERTY, &url_property->login_timeout},
    {"query_timeout", INT_PROPERTY, &url_property->query_timeout},
    {"disconnect_on_query_timeout", BOOL_PROPERTY,
     &url_property->disconnect_on_query_timeout},
    {"error_on_server_restart", BOOL_PROPERTY,
     &url_property->error_on_server_restart},
    {"connectionType", CON_TYPE_PROPERTY,
     &url_property->con_type}
  };
  int error = CCI_ER_NO_ERROR;

  con_property_init (url_property);

  if (properties == NULL)
    {
      return CCI_ER_NO_ERROR;
    }

  error = cci_url_parse_properties (props, DIM (props), properties);
  if (error != CCI_ER_NO_ERROR)
    {
      con_property_free (url_property);
      return error;
    }

  if (url_property->rc_time < MONITORING_INTERVAL)
    {
      url_property->rc_time = MONITORING_INTERVAL;
    }

  return CCI_ER_NO_ERROR;
}
