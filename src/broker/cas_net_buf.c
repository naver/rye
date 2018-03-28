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
 * cas_net_buf.c -
 */

#ident "$Id$"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

#include <arpa/inet.h>

#include "cas.h"
#include "cas_common.h"
#include "cas_net_buf.h"
#include "cas_util.h"
#include "dbi.h"
#include "error_code.h"

static int net_buf_realloc (T_NET_BUF * net_buf, int size);

void
net_buf_init (T_NET_BUF * net_buf)
{
  net_buf->data = NULL;
  net_buf->alloc_size = 0;
  net_buf->data_size = 0;
  net_buf->err_code = 0;
}

void
net_buf_clear (T_NET_BUF * net_buf)
{
  net_buf->data_size = 0;
  net_buf->err_code = 0;
}

void
net_buf_destroy (T_NET_BUF * net_buf)
{
  RYE_FREE_MEM (net_buf->data);
  net_buf->alloc_size = 0;
  net_buf_clear (net_buf);
}

int
net_buf_cp_byte (T_NET_BUF * net_buf, char ch)
{
  if (NET_BUF_FREE_SIZE (net_buf) < NET_SIZE_BYTE && net_buf_realloc (net_buf, NET_SIZE_BYTE) < 0)
    {
      return CAS_ER_NO_MORE_MEMORY;
    }

  *(NET_BUF_CURR_PTR (net_buf)) = ch;   /* do not call memcpy(); simply assign */
  net_buf->data_size += NET_SIZE_BYTE;
  return 0;
}

int
net_buf_cp_str (T_NET_BUF * net_buf, const char *buf, int size)
{
  if (size <= 0)
    return 0;

  if (NET_BUF_FREE_SIZE (net_buf) < size && net_buf_realloc (net_buf, size) < 0)
    {
      return CAS_ER_NO_MORE_MEMORY;
    }

  memcpy (NET_BUF_CURR_PTR (net_buf), buf, size);
  net_buf->data_size += size;
  return 0;
}

int
net_buf_cp_int (T_NET_BUF * net_buf, int value, int *begin_offset)
{
  if (NET_BUF_FREE_SIZE (net_buf) < NET_SIZE_INT && net_buf_realloc (net_buf, NET_SIZE_INT) < 0)
    {
      if (begin_offset)
        {
          *begin_offset = -1;
        }
      return CAS_ER_NO_MORE_MEMORY;
    }

  value = htonl (value);
  memcpy (NET_BUF_CURR_PTR (net_buf), &value, NET_SIZE_INT);

  if (begin_offset)
    {
      *begin_offset = net_buf->data_size;
    }

  net_buf->data_size += NET_SIZE_INT;
  return 0;
}

void
net_buf_overwrite_int (T_NET_BUF * net_buf, int offset, int value)
{
  if (net_buf->data == NULL || offset < 0)
    {
      return;
    }
  value = htonl (value);
  memcpy (net_buf->data + NET_BUF_HEADER_SIZE + offset, &value, NET_SIZE_INT);
}

int
net_buf_cp_bigint (T_NET_BUF * net_buf, DB_BIGINT value, int *begin_offset)
{
  if (NET_BUF_FREE_SIZE (net_buf) < NET_SIZE_BIGINT && net_buf_realloc (net_buf, NET_SIZE_BIGINT) < 0)
    {
      if (begin_offset)
        {
          *begin_offset = -1;
        }
      return CAS_ER_NO_MORE_MEMORY;
    }

  value = net_htoni64 (value);
  memcpy (NET_BUF_CURR_PTR (net_buf), &value, NET_SIZE_BIGINT);

  if (begin_offset)
    {
      *begin_offset = net_buf->data_size;
    }

  net_buf->data_size += NET_SIZE_BIGINT;
  return 0;
}

int
net_buf_cp_double (T_NET_BUF * net_buf, double value)
{
  if (NET_BUF_FREE_SIZE (net_buf) < NET_SIZE_DOUBLE && net_buf_realloc (net_buf, NET_SIZE_DOUBLE) < 0)
    {
      return CAS_ER_NO_MORE_MEMORY;
    }
  value = net_htond (value);
  memcpy (NET_BUF_CURR_PTR (net_buf), &value, NET_SIZE_DOUBLE);
  net_buf->data_size += NET_SIZE_DOUBLE;

  return 0;
}

int
net_buf_cp_short (T_NET_BUF * net_buf, short value)
{
  if (NET_BUF_FREE_SIZE (net_buf) < NET_SIZE_SHORT && net_buf_realloc (net_buf, NET_SIZE_SHORT) < 0)
    {
      return CAS_ER_NO_MORE_MEMORY;
    }
  value = htons (value);
  memcpy (NET_BUF_CURR_PTR (net_buf), &value, NET_SIZE_SHORT);
  net_buf->data_size += NET_SIZE_SHORT;

  return 0;
}

void
net_buf_error_msg_set (T_NET_BUF * net_buf, int err_indicator, int err_code, char *err_str)
{
  size_t err_str_len = 0;

  assert (err_code < 0);

  net_buf_clear (net_buf);

  net_buf_cp_byte (net_buf, ERROR_RESPONSE);

  net_buf_cp_int (net_buf, err_indicator, NULL);

  net_buf_cp_int (net_buf, err_code, NULL);

  err_str_len = (err_str == NULL ? 0 : strlen (err_str) + 1);
  net_buf_cp_int (net_buf, err_str_len, NULL);
  net_buf_cp_str (net_buf, err_str, err_str_len);
}

#ifndef BYTE_ORDER_BIG_ENDIAN
INT64
net_htoni64 (INT64 from)
{
  INT64 to;
  char *p, *q;

  p = (char *) &from;
  q = (char *) &to;

  q[0] = p[7];
  q[1] = p[6];
  q[2] = p[5];
  q[3] = p[4];
  q[4] = p[3];
  q[5] = p[2];
  q[6] = p[1];
  q[7] = p[0];

  return to;
}

float
net_htonf (float from)
{
  float to;
  char *p, *q;

  p = (char *) &from;
  q = (char *) &to;

  q[0] = p[3];
  q[1] = p[2];
  q[2] = p[1];
  q[3] = p[0];

  return to;
}

double
net_htond (double from)
{
  double to;
  char *p, *q;

  p = (char *) &from;
  q = (char *) &to;

  q[0] = p[7];
  q[1] = p[6];
  q[2] = p[5];
  q[3] = p[4];
  q[4] = p[3];
  q[5] = p[2];
  q[6] = p[1];
  q[7] = p[0];

  return to;
}
#endif /* !BYTE_ORDER_BIG_ENDIAN */

void
net_buf_column_info_set (T_NET_BUF * net_buf, char ut, short scale, int prec, const char *name)
{
  net_buf_cp_byte (net_buf, ut);
  net_buf_cp_short (net_buf, scale);
  net_buf_cp_int (net_buf, prec, NULL);
  if (name == NULL)
    {
      net_buf_cp_int (net_buf, 1, NULL);
      net_buf_cp_byte (net_buf, '\0');
    }
  else
    {
      char *tmp_str;

      RYE_ALLOC_COPY_STR (tmp_str, name);
      if (tmp_str == NULL)
        {
          net_buf_cp_int (net_buf, 1, NULL);
          net_buf_cp_byte (net_buf, '\0');
        }
      else
        {
          trim (tmp_str);
          net_buf_cp_int (net_buf, strlen (tmp_str) + 1, NULL);
          net_buf_cp_str (net_buf, tmp_str, strlen (tmp_str) + 1);
          RYE_FREE_MEM (tmp_str);
        }
    }
}

static int
net_buf_realloc (T_NET_BUF * net_buf, int size)
{
  if (NET_BUF_FREE_SIZE (net_buf) < size)
    {
      int extra, new_alloc_size;

      /* realloc unit is 64 Kbyte */
      extra = (size + NET_BUF_EXTRA_SIZE - 1) / NET_BUF_EXTRA_SIZE;
      new_alloc_size = net_buf->alloc_size + extra * NET_BUF_EXTRA_SIZE;
      net_buf->data = (char *) RYE_REALLOC (net_buf->data, new_alloc_size);
      if (net_buf->data == NULL)
        {
          net_buf->alloc_size = 0;
          net_buf->err_code = CAS_ER_NO_MORE_MEMORY;
          return -1;
        }

      net_buf->alloc_size = new_alloc_size;
    }

  return 0;
}

void
net_arg_get_size (int *size, void *arg)
{
  int tmp_i;

  memcpy (&tmp_i, arg, NET_SIZE_INT);
  *size = ntohl (tmp_i);
}

void
net_arg_get_bigint (DB_BIGINT * value, void *arg)
{
  DB_BIGINT tmp_i;
  char *cur_p = (char *) arg + NET_SIZE_INT;

  memcpy (&tmp_i, cur_p, NET_SIZE_BIGINT);
  *value = ntohi64 (tmp_i);
  cur_p += NET_SIZE_BIGINT;
}

void
net_arg_get_char (char *value, void *arg)
{
  char *cur_p = (char *) arg + NET_SIZE_INT;

  *value = *cur_p;
}

void
net_arg_get_int (int *value, void *arg)
{
  int tmp_i;
  char *cur_p = (char *) arg + NET_SIZE_INT;

  memcpy (&tmp_i, cur_p, NET_SIZE_INT);
  *value = ntohl (tmp_i);
  cur_p += NET_SIZE_INT;
}

void
net_arg_get_short (short *value, void *arg)
{
  short tmp_s;
  char *cur_p = (char *) arg + NET_SIZE_INT;

  memcpy (&tmp_s, cur_p, NET_SIZE_SHORT);
  *value = ntohs (tmp_s);
  cur_p += NET_SIZE_SHORT;
}

void
net_arg_get_double (double *value, void *arg)
{
  double tmp_d;
  char *cur_p = (char *) arg + NET_SIZE_INT;

  memcpy (&tmp_d, cur_p, NET_SIZE_DOUBLE);
  *value = net_ntohd (tmp_d);
  cur_p += NET_SIZE_DOUBLE;
}

void
net_arg_get_str (char **value, int *size, void *arg)
{
  int tmp_i;
  char *cur_p = (char *) arg;

  memcpy (&tmp_i, cur_p, NET_SIZE_INT);
  *size = ntohl (tmp_i);
  cur_p += NET_SIZE_INT;
  if (*size <= 0)
    {
      *value = NULL;
      *size = 0;
    }
  else
    {
      *value = cur_p;
    }
}

void
net_arg_get_date (short *year, short *mon, short *day, void *arg)
{
  short tmp_s;
  char *cur_p = (char *) arg + NET_SIZE_INT;

  memcpy (&tmp_s, cur_p, NET_SIZE_SHORT);
  *year = ntohs (tmp_s);
  cur_p += NET_SIZE_SHORT;
  memcpy (&tmp_s, cur_p, NET_SIZE_SHORT);
  *mon = ntohs (tmp_s);
  cur_p += NET_SIZE_SHORT;
  memcpy (&tmp_s, cur_p, NET_SIZE_SHORT);
  *day = ntohs (tmp_s);
  cur_p += NET_SIZE_SHORT;
}

void
net_arg_get_time (short *hh, short *mm, short *ss, void *arg)
{
  short tmp_s;
  char *cur_p = (char *) arg + NET_SIZE_INT + NET_SIZE_SHORT * 3;

  memcpy (&tmp_s, cur_p, NET_SIZE_SHORT);
  *hh = ntohs (tmp_s);
  cur_p += NET_SIZE_SHORT;
  memcpy (&tmp_s, cur_p, NET_SIZE_SHORT);
  *mm = ntohs (tmp_s);
  cur_p += NET_SIZE_SHORT;
  memcpy (&tmp_s, cur_p, NET_SIZE_SHORT);
  *ss = ntohs (tmp_s);
  cur_p += NET_SIZE_SHORT;
}

void
net_arg_get_datetime (short *yr, short *mon, short *day, short *hh, short *mm, short *ss, short *ms, void *arg)
{
  short tmp_s;
  char *cur_p = (char *) arg + NET_SIZE_INT;

  memcpy (&tmp_s, cur_p, NET_SIZE_SHORT);
  *yr = ntohs (tmp_s);
  cur_p += NET_SIZE_SHORT;
  memcpy (&tmp_s, cur_p, NET_SIZE_SHORT);
  *mon = ntohs (tmp_s);
  cur_p += NET_SIZE_SHORT;
  memcpy (&tmp_s, cur_p, NET_SIZE_SHORT);
  *day = ntohs (tmp_s);
  cur_p += NET_SIZE_SHORT;
  memcpy (&tmp_s, cur_p, NET_SIZE_SHORT);
  *hh = ntohs (tmp_s);
  cur_p += NET_SIZE_SHORT;
  memcpy (&tmp_s, cur_p, NET_SIZE_SHORT);
  *mm = ntohs (tmp_s);
  cur_p += NET_SIZE_SHORT;
  memcpy (&tmp_s, cur_p, NET_SIZE_SHORT);
  *ss = ntohs (tmp_s);
  cur_p += NET_SIZE_SHORT;
  memcpy (&tmp_s, cur_p, NET_SIZE_SHORT);
  *ms = ntohs (tmp_s);
  cur_p += NET_SIZE_SHORT;
}
