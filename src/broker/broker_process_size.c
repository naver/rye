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
 * broker_process_size.c - Get process size
 *	return values
 *		> 1 	success
 *		1 	cannot get process information
 *		<= 0	no such process
 */

#ident "$Id$"

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

#include <unistd.h>

#include <ctype.h>
#include <fcntl.h>
#include <string.h>
#include <sys/procfs.h>

#include "cas_common.h"
#include "broker_process_size.h"


#define GETSIZE_PATH            "getsize"

static char *skip_token (char *p);

int
getsize (int pid)
{
  char buf[4096];
  char *p;
  int fd;
  int read_len, i;
  INT64 psize;

  if (pid <= 0)
    {
      return -1;
    }

  sprintf (buf, "/proc/%d/stat", pid);
  fd = open (buf, O_RDONLY);
  if (fd < 0)
    {
      return -1;
    }

  read_len = read (fd, buf, sizeof (buf) - 1);
  close (fd);

  if (read_len < 0 || read_len >= (int) sizeof (buf))
    {
      return 1;
    }
  buf[read_len] = '\0';

  p = strchr (buf, ')');
  p++;
  for (i = 0; i < 20; i++)
    {
      p = skip_token (p);
    }

  psize = atoll (p);
  return (int) (psize / 1024);
}

static char *
skip_token (char *p)
{
  while (isspace (*p))
    p++;
  while (*p && !isspace (*p))
    p++;
  return p;
}
