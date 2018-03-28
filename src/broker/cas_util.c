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
 * cas_util.c -
 */

#ident "$Id$"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include <sys/time.h>
#include <assert.h>

#include "cas_common.h"
#include "cas_util.h"
#include "cas_net_buf.h"

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

void
ut_toupper (char *str)
{
  char *p;

  if (str == NULL)
    return;

  for (p = str; *p; p++)
    {
      if (*p >= 'a' && *p <= 'z')
        *p = *p - 'a' + 'A';
    }
}

void
ut_timeval_diff (struct timeval *start, struct timeval *end, int *res_sec, int *res_msec)
{
  int sec, msec;

  assert (start != NULL);
  assert (end != NULL);
  assert (res_sec != NULL);
  assert (res_msec != NULL);

  sec = end->tv_sec - start->tv_sec;
  msec = (end->tv_usec / 1000) - (start->tv_usec / 1000);
  if (msec < 0)
    {
      msec += 1000;
      sec--;
    }
  *res_sec = sec;
  *res_msec = msec;
}

int
ut_check_timeout (struct timeval *start_time, struct timeval *end_time, int timeout_msec, int *res_sec, int *res_msec)
{
  struct timeval cur_time;
  int diff_msec;

  assert (start_time != NULL);
  assert (res_sec != NULL);
  assert (res_msec != NULL);

  if (end_time == NULL)
    {
      end_time = &cur_time;
      gettimeofday (end_time, NULL);
    }
  ut_timeval_diff (start_time, end_time, res_sec, res_msec);

  if (timeout_msec > 0)
    {
      diff_msec = *res_sec * 1000 + *res_msec;
    }
  else
    {
      diff_msec = -1;
    }

  return (diff_msec >= timeout_msec) ? diff_msec : -1;
}
