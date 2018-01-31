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
 * cci_util.c -
 */

#ident "$Id$"

/************************************************************************
 * IMPORTED SYSTEM HEADER FILES						*
 ************************************************************************/
#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <sys/types.h>
#include <regex38a.h>

/************************************************************************
 * OTHER IMPORTED HEADER FILES						*
 ************************************************************************/

#include "cci_common.h"
#include "cas_cci.h"
#include "cci_util.h"
#include "cci_handle_mng.h"

/************************************************************************
 * PRIVATE DEFINITIONS							*
 ************************************************************************/

/************************************************************************
 * PRIVATE TYPE DEFINITIONS						*
 ************************************************************************/

/************************************************************************
 * PRIVATE FUNCTION PROTOTYPES						*
 ************************************************************************/

static char is_float_str (const char *str);
static void *cci_reg_malloc (void *dummy, size_t s);
static void *cci_reg_realloc (void *dummy, void *p, size_t s);
static void cci_reg_free (void *dummy, void *p);

/************************************************************************
 * INTERFACE VARIABLES							*
 ************************************************************************/

/************************************************************************
 * PUBLIC VARIABLES							*
 ************************************************************************/

/************************************************************************
 * PRIVATE VARIABLES							*
 ************************************************************************/

/************************************************************************
 * IMPLEMENTATION OF INTERFACE FUNCTIONS 				*
 ************************************************************************/

/************************************************************************
 * IMPLEMENTATION OF PUBLIC FUNCTIONS	 				*
 ************************************************************************/

char *
ut_make_url (char *buf, int bufsize,
	     const char *server_list, const char *dbname,
	     const char *dbuser, const char *dbpasswd,
	     const char *port_name, const char *property)
{
  if (dbuser == NULL)
    {
      dbuser = "";
    }
  if (dbpasswd == NULL)
    {
      dbpasswd = "";
    }
  if (property == NULL)
    {
      property = "";
    }

  snprintf (buf, bufsize, "%s%s/%s:%s:%s/%s?%s",
	    RYE_URL_HEADER, server_list, dbname, dbuser, dbpasswd,
	    port_name, property);

  return buf;
}

int
ut_str_to_bigint (const char *str, INT64 * value)
{
  char *end_p;
  INT64 bi_val;

  if (str_to_int64 (&bi_val, &end_p, str, 10) == 0)
    {
      if (*end_p == 0 || *end_p == '.' || isspace ((int) *end_p))
	{
	  *value = bi_val;
	  return 0;
	}
    }

  return (CCI_ER_TYPE_CONVERSION);
}

int
ut_str_to_int (const char *str, int *value)
{
  char *end_p;
  int i_val;

  i_val = strtol (str, &end_p, 10);
  if (*end_p == 0 || *end_p == '.' || isspace ((int) *end_p))
    {
      *value = i_val;
      return 0;
    }

  return (CCI_ER_TYPE_CONVERSION);
}

int
ut_str_to_double (const char *str, double *value)
{
  if (is_float_str (str))
    {
      sscanf (str, "%lf", value);
      return 0;
    }

  return (CCI_ER_TYPE_CONVERSION);
}

void
ut_int_to_str (INT64 value, char *str, int size)
{
  snprintf (str, size, "%lld", (long long) value);
}

void
ut_double_to_str (double value, char *str, int size)
{
  snprintf (str, size, "%.16f", value);
}

void
ut_datetime_to_str (T_CCI_DATETIME * value, T_CCI_TYPE type, char *str,
		    int size)
{
  if (type == CCI_TYPE_DATE)
    {
      snprintf (str, size, "%04d-%02d-%02d", value->yr, value->mon,
		value->day);
    }
  else if (type == CCI_TYPE_TIME)
    {
      snprintf (str, size, "%02d:%02d:%02d", value->hh, value->mm, value->ss);
    }
  else
    {				/* type == CCI_TYPE_DATETIME */
      snprintf (str, size, "%04d-%02d-%02d %02d:%02d:%02d.%03d",
		value->yr, value->mon, value->day,
		value->hh, value->mm, value->ss, value->ms);
    }
}

void
ut_bit_to_str (char *bit_str, int bit_size, char *str, int str_size)
{
  char ch, c;
  int i;

  for (i = 0; i < bit_size; i++)
    {
      if (2 * i + 1 >= str_size)
	{
	  break;
	}

      ch = bit_str[i];

      c = (ch >> 4) & 0x0f;
      if (c <= 9)
	{
	  str[2 * i] = c + '0';
	}
      else
	{
	  str[2 * i] = c - 10 + 'A';
	}

      c = ch & 0x0f;
      if (c <= 9)
	{
	  str[2 * i + 1] = c + '0';
	}
      else
	{
	  str[2 * i + 1] = c - 10 + 'A';
	}
    }
  str[2 * i] = 0;
}

int
ut_host_str_to_addr (const char *ip_str, unsigned char *ip_addr)
{
  in_addr_t in_addr;

  in_addr = hostname_to_ip (ip_str);
  if (in_addr == INADDR_NONE)
    {
      return CCI_ER_HOSTNAME;
    }

  memcpy ((void *) ip_addr, (void *) &in_addr, sizeof (in_addr));

  return CCI_ER_NO_ERROR;
}

int
ut_set_host_info (T_HOST_INFO * host_info, const char *hostname, int port)
{
  int error;

  error = ut_host_str_to_addr (hostname, host_info->ip_addr);
  if (error < 0)
    {
      return error;
    }

  host_info->port = port;

  return CCI_ER_NO_ERROR;
}

char *
ut_host_info_to_str (char *buf, const T_HOST_INFO * host_info)
{
  sprintf (buf, "%d.%d.%d.%d:%d",
	   host_info->ip_addr[0], host_info->ip_addr[1],
	   host_info->ip_addr[2], host_info->ip_addr[3], host_info->port);
  return buf;
}

char *
ut_cur_host_info_to_str (char *buf, const T_CON_HANDLE * con_handle)
{
  int cur_id = con_handle->alter_hosts->cur_id;

  return ut_host_info_to_str (buf,
			      &con_handle->alter_hosts->host_info[cur_id]);
}

void
ut_tolower (char *str)
{
  char *p;

  if (str == NULL)
    return;

  for (p = str; *p; p++)
    {
      if (*p >= 'A' && *p <= 'Z')
	*p = *p - 'A' + 'a';
    }
}

/************************************************************************
 * IMPLEMENTATION OF PRIVATE FUNCTIONS	 				*
 ************************************************************************/

static char
is_float_str (const char *str)
{
  const char *p;
  char ch;
  int state = 0;

  for (p = str; *p && state >= 0; p++)
    {
      ch = *p;
      switch (state)
	{
	case 0:
	  if (ch == '+' || ch == '-')
	    state = 1;
	  else if (ch == '.')
	    state = 3;
	  else if (ch >= '0' && ch <= '9')
	    state = 2;
	  else
	    state = -1;
	  break;
	case 1:
	  if (ch >= '0' && ch <= '9')
	    state = 2;
	  else
	    state = -1;
	  break;
	case 2:
	  if (ch == '.')
	    state = 3;
	  else if (ch == 'e' || ch == 'E')
	    state = 4;
	  else if (ch >= '0' && ch <= '9')
	    state = 2;
	  else
	    state = -1;
	  break;
	case 3:
	  if (ch >= '0' && ch <= '9')
	    state = 5;
	  else
	    state = -1;
	  break;
	case 4:
	  if (ch == '+' || ch == '-' || (ch >= '0' && ch <= '9'))
	    state = 6;
	  else
	    state = -1;
	  break;
	case 5:
	  if (ch == 'e' || ch == 'E')
	    state = 4;
	  else if (ch >= '0' && ch <= '9')
	    state = 5;
	  else
	    state = -1;
	  break;
	case 6:
	  if (ch >= '0' && ch <= '9')
	    state = 6;
	  else
	    state = -1;
	default:
	  break;
	}
    }

  if (state == 2 || state == 5 || state == 6)
    return 1;

  return 0;
}

static void *
cci_reg_malloc (UNUSED_ARG void *dummy, size_t s)
{
  return cci_malloc (s);
}

static void *
cci_reg_realloc (UNUSED_ARG void *dummy, void *p, size_t s)
{
  return cci_realloc (p, s);
}

static void
cci_reg_free (UNUSED_ARG void *dummy, void *p)
{
  cci_free (p);
}

int
cci_url_match (const char *src, char *token[])
{
  static int match_idx[] = { 1, 3, 4, 5, 6, 7, -1 };

  unsigned i, len;
  int error;
  cub_regex_t regex;
  cub_regmatch_t match[100];

  char b[256];

  cub_regset_malloc (cci_reg_malloc);
  cub_regset_realloc (cci_reg_realloc);
  cub_regset_free (cci_reg_free);

  error = cub_regcomp (&regex, RYE_URL_PATTERN,
		       CUB_REG_EXTENDED | CUB_REG_ICASE);
  if (error != CUB_REG_OKAY)
    {
      /* should not reach on this */
      cub_regerror (error, &regex, b, 256);
      fprintf (stderr, "cub_regcomp : %s\n", b);
      cub_regfree (&regex);
      return CCI_ER_INVALID_URL;	/* pattern compilation error */
    }

  len = strlen (src);
  error = cub_regexec (&regex, src, len, 100, match, 0);
  if (error == CUB_REG_NOMATCH)
    {
      cub_regfree (&regex);
      return CCI_ER_INVALID_URL;	/* invalid url */
    }
  if (error != CUB_REG_OKAY)
    {
      /* should not reach on this */
      cub_regerror (error, &regex, b, 256);
      fprintf (stderr, "cub_regcomp : %s\n", b);
      cub_regfree (&regex);
      return CCI_ER_INVALID_URL;	/* regexec error */
    }

  if (match[0].rm_eo - match[0].rm_so != len)
    {
      cub_regfree (&regex);
      return CCI_ER_INVALID_URL;	/* invalid url */
    }

  for (i = 0; match_idx[i] != -1; i++)
    {
      token[i] = NULL;
    }

  error = CCI_ER_NO_ERROR;
  for (i = 0; match_idx[i] != -1; i++)
    {
      if (match[match_idx[i]].rm_so == -1)
	{
	  continue;
	}

      const char *t = src + match[match_idx[i]].rm_so;
      size_t n = match[match_idx[i]].rm_eo - match[match_idx[i]].rm_so;
      token[i] = MALLOC (n + 1);
      if (token[i] == NULL)
	{
	  error = CCI_ER_NO_MORE_MEMORY;	/* out of memory */
	  break;
	}
      strncpy (token[i], t, n);
      token[i][n] = '\0';
    }

  if (error != CCI_ER_NO_ERROR)
    {
      /* free allocated memory when error was CCI_ER_NO_MORE_MEMORY */
      for (i = 0; match_idx[i] != -1 && match[match_idx[i]].rm_so != -1; i++)
	{
	  FREE_MEM (token[i]);
	}
    }

  cub_regfree (&regex);
  return error;
}

long
ut_timeval_diff_msec (struct timeval *start, struct timeval *end)
{
  long diff_msec;
  assert (start != NULL && end != NULL);

  diff_msec = (end->tv_sec - start->tv_sec) * 1000;
  diff_msec += ((end->tv_usec - start->tv_usec) / 1000);

  return diff_msec;
}
