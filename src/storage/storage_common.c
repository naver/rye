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
 * storage_common.c - Definitions and data types of disk related stuffs
 *                    such as pages, file structures, and so on.
 */

#ident "$Id$"

#include <stdlib.h>
#include <assert.h>

#include "config.h"

#include "storage_common.h"
#include "memory_alloc.h"
#include "error_manager.h"
#include "system_parameter.h"
#include "environment_variable.h"
#include "file_io.h"
#include "db_date.h"


/* RESERVED_SIZE_IN_PAGE should be aligned */
#define RESERVED_SIZE_IN_PAGE   sizeof(FILEIO_PAGE_RESERVED)

#define IS_POWER_OF_2(x)        (((x) & ((x)-1)) == 0)

static PGLENGTH db_Io_page_size = IO_DEFAULT_PAGE_SIZE;
static PGLENGTH db_Log_page_size = IO_DEFAULT_PAGE_SIZE;
static PGLENGTH db_User_page_size =
  IO_DEFAULT_PAGE_SIZE - RESERVED_SIZE_IN_PAGE;

static PGLENGTH find_valid_page_size (PGLENGTH page_size);

/*
 * db_page_size(): returns the user page size
 *
 *   returns: user page size
 */
PGLENGTH
db_page_size (void)
{
  return db_User_page_size;
}

/*
 * db_io_page_size(): returns the IO page size
 *
 *   returns: IO page size
 */
PGLENGTH
db_io_page_size (void)
{
  return db_Io_page_size;
}

/*
 * db_log_page_size(): returns the log page size
 *
 *   returns: log page size
 */
PGLENGTH
db_log_page_size (void)
{
  return db_Log_page_size;
}

/*
 * db_set_page_size(): set the page size of system.
 *
 *   returns: NO_ERROR if page size is set by given size, otherwise ER_FAILED
 *   io_page_size(IN): the IO page size
 *   log_page_size(IN): the LOG page size
 *
 * Note: Set the database page size to the given size. The given size
 *       must be power of 2, greater than or equal to 1K, and smaller
 *       than or equal to 16K.
 */
int
db_set_page_size (PGLENGTH io_page_size, PGLENGTH log_page_size)
{
  assert (io_page_size >= IO_MIN_PAGE_SIZE
	  && log_page_size >= IO_MIN_PAGE_SIZE);

  if (io_page_size < IO_MIN_PAGE_SIZE || log_page_size < IO_MIN_PAGE_SIZE)
    {
      return ER_FAILED;
    }

  db_Io_page_size = find_valid_page_size (io_page_size);
  db_User_page_size = db_Io_page_size - RESERVED_SIZE_IN_PAGE;
  db_Log_page_size = find_valid_page_size (log_page_size);

  if (db_Io_page_size != io_page_size || db_Log_page_size != log_page_size)
    {
      return ER_FAILED;
    }

  return NO_ERROR;
}

/*
 * db_network_page_size(): find the network pagesize
 *
 *   returns: network pagesize
 *
 * Note: Find the best network pagesize for C/S communications for
 *       given transaction/client.
 */
PGLENGTH
db_network_page_size (void)
{
  return db_Io_page_size;
}

/*
 * find_valid_page_size(): find the valid page size of system
 *
 *   returns: page_size
 *   page_size(IN): the page size
 *
 * Note: Find the database pagesize with the given size, where the given size
 *       must be power of 2, greater than or equal to 1K, and smaller than or
 *       equal to 16K.
 */
static PGLENGTH
find_valid_page_size (PGLENGTH page_size)
{
  PGLENGTH power2_page_size = page_size;

  if (power2_page_size < IO_MIN_PAGE_SIZE)
    {
      power2_page_size = IO_MIN_PAGE_SIZE;
    }
  else if (power2_page_size > IO_MAX_PAGE_SIZE)
    {
      power2_page_size = IO_MAX_PAGE_SIZE;
    }
  else
    {
      if (!IS_POWER_OF_2 (power2_page_size))
	{
	  /*
	   * Not a power of 2 or page size is too small
	   *
	   * Round the number to a power of two. Find smaller number that it is
	   * a power of two, and then shift to get larger number.
	   */
	  while (!IS_POWER_OF_2 (power2_page_size))
	    {
	      if (power2_page_size < IO_MIN_PAGE_SIZE)
		{
		  power2_page_size = IO_MIN_PAGE_SIZE;
		  break;
		}
	      else
		{
		  /* Turn off some bits but the left most one */
		  power2_page_size =
		    power2_page_size & (power2_page_size - 1);
		}
	    }

	  power2_page_size <<= 1;

	  if (power2_page_size < IO_MIN_PAGE_SIZE)
	    {
	      power2_page_size = IO_MIN_PAGE_SIZE;
	    }
	  else if (power2_page_size > IO_MAX_PAGE_SIZE)
	    {
	      power2_page_size = IO_MAX_PAGE_SIZE;
	    }

	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_DTSR_BAD_PAGESIZE, 2,
		  page_size, power2_page_size);
	}
    }

  return power2_page_size;
}


int
recdes_allocate_data_area (RECDES * rec, int size)
{
  char *data;

  data = (char *) malloc (size);
  if (data == NULL)
    {
      return ER_FAILED;
    }

  rec->data = data;
  rec->area_size = size;

  return NO_ERROR;
}

void
recdes_free_data_area (RECDES * rec)
{
  free_and_init (rec->data);
}

void
recdes_set_data_area (RECDES * rec, char *data, int size)
{
  rec->data = data;
  rec->area_size = size;
}

#if defined (ENABLE_UNUSED_FUNCTION)
char *
lsa_to_string (char *buf, int buf_size, LOG_LSA * lsa)
{
  snprintf (buf, buf_size, "(%lld|%d)", (long long int) lsa->pageid,
	    lsa->offset);
  buf[buf_size - 1] = 0;
  return buf;
}
#endif /* ENABLE_UNUSED_FUNCTION */

INT64
lsa_to_int64 (LOG_LSA lsa)
{
  INT64 value;

  value = lsa.pageid << 15 | lsa.offset;

  return value;
}

LOG_LSA
int64_to_lsa (INT64 value)
{
  LOG_LSA lsa;

  lsa.offset = value & 0x0000000000007FFF;
  lsa.pageid = (value & 0xFFFFFFFFFFFF8000) >> 15;

  return lsa;
}

#if defined(ENABLE_UNUSED_FUNCTION)
char *
oid_to_string (char *buf, int buf_size, OID * oid)
{
  snprintf (buf, buf_size, "(%d|%d|%d)", oid->volid, oid->pageid,
	    oid->slotid);
  buf[buf_size - 1] = 0;
  return buf;
}

char *
vpid_to_string (char *buf, int buf_size, VPID * vpid)
{
  snprintf (buf, buf_size, "(%d|%d)", vpid->volid, vpid->pageid);
  buf[buf_size - 1] = 0;
  return buf;
}

char *
vfid_to_string (char *buf, int buf_size, VFID * vfid)
{
  snprintf (buf, buf_size, "(%d|%d)", vfid->volid, vfid->fileid);
  buf[buf_size - 1] = 0;
  return buf;
}

char *
hfid_to_string (char *buf, int buf_size, HFID * hfid)
{
  snprintf (buf, buf_size, "(%d|%d|%d)", hfid->vfid.volid,
	    hfid->vfid.fileid, hfid->hpgid);
  buf[buf_size - 1] = 0;
  return buf;
}
#endif /* ENABLE_UNUSED_FUNCTION */

char *
btid_to_string (char *buf, int buf_size, BTID * btid)
{
  snprintf (buf, buf_size, "%d|%d|%d", btid->vfid.volid,
	    btid->vfid.fileid, btid->root_pageid);
  buf[buf_size - 1] = 0;
  return buf;
}

BTID
string_to_btid (const char *buffer)
{
  char *ptr;
  int result = 0;
  int val;
  BTID btid;

  if (buffer == NULL)
    {
      goto exit_on_error;
    }

  result = str_to_int32 (&val, &ptr, buffer, 10);
  btid.vfid.volid = val;
  if (result != 0)
    {
      goto exit_on_error;
    }
  buffer = ptr + 1;

  result = str_to_int32 (&val, &ptr, buffer, 10);
  btid.vfid.fileid = val;
  if (result != 0)
    {
      goto exit_on_error;
    }
  buffer = ptr + 1;

  result = str_to_int32 (&val, &ptr, buffer, 10);
  btid.root_pageid = val;
  if (result != 0)
    {
      goto exit_on_error;
    }

  return btid;

exit_on_error:
  BTID_SET_NULL (&btid);

  return btid;
}
