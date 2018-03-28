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
 * cas_error.c -
 */

#ident "$Id$"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <unistd.h>
#include <fcntl.h>
#include <sys/time.h>

#include "dbi.h"
#include "cas.h"
#include "cas_common.h"
#include "cas_execute.h"
#include "cas_network.h"
#include "cas_util.h"
#include "cas_log.h"

static bool server_aborted = false;

void
err_msg_set (T_NET_BUF * net_buf, UNUSED_ARG const char *file, UNUSED_ARG int line)
{
  if ((err_Info.err_indicator != CAS_ERROR_INDICATOR) && (err_Info.err_indicator != DBMS_ERROR_INDICATOR))
    {
      er_log_debug (ARG_FILE_LINE, "invalid internal error info : file %s line %d", file, line);
      return;
    }

  if (net_buf != NULL)
    {
      net_buf_error_msg_set (net_buf, err_Info.err_indicator, err_Info.err_number, err_Info.err_string);
      er_log_debug (ARG_FILE_LINE, "err_msg_set: err_code %d file %s line %d", err_Info.err_number, file, line);
    }
  if (err_Info.err_indicator == CAS_ERROR_INDICATOR)
    {
      return;
    }

  if ((net_buf == NULL) && (err_Info.err_number == ER_TM_SERVER_DOWN_UNILATERALLY_ABORTED))
    {
      set_server_aborted (true);
    }

  switch (err_Info.err_number)
    {
    case ER_TM_SERVER_DOWN_UNILATERALLY_ABORTED:
    case ER_NET_SERVER_CRASHED:
    case ER_OBJ_NO_CONNECT:
    case ER_BO_CONNECT_FAILED:
      /*case -581: *//* ER_DB_NO_MODIFICATIONS */
      as_Info->reset_flag = TRUE;
      er_log_debug (ARG_FILE_LINE, "db_err_msg_set: set reset_flag");
      break;
    }
}

int
error_info_set (int err_number, int err_indicator, const char *file, int line)
{
  return error_info_set_with_msg (err_number, err_indicator, NULL, false, file, line);
}

int
error_info_set_force (int err_number, int err_indicator, const char *file, int line)
{
  return error_info_set_with_msg (err_number, err_indicator, NULL, true, file, line);
}

int
error_info_set_with_msg (int err_number, int err_indicator, const char *err_msg, bool force, const char *file, int line)
{
  const char *tmp_err_msg;

  assert (err_number < 0);

  if ((!force) && (err_Info.err_indicator != ERROR_INDICATOR_UNSET))
    {
      er_log_debug (ARG_FILE_LINE, "ERROR_INFO_SET reset error info : err_code %d", err_Info.err_number);
      return err_Info.err_indicator;
    }

  if ((err_indicator == DBMS_ERROR_INDICATOR) && (err_number == -1))    /* might be connection error */
    {
      err_Info.err_number = er_errid ();
    }
  else
    {
      err_Info.err_number = err_number;
    }

  err_Info.err_indicator = err_indicator;
  strncpy (err_Info.err_file, file, ERR_FILE_LENGTH - 1);
  err_Info.err_string[ERR_FILE_LENGTH - 1] = 0;
  err_Info.err_line = line;

  if ((err_indicator == CAS_ERROR_INDICATOR) && (err_msg == NULL))
    return err_indicator;

  if (err_msg)
    {
      strncpy (err_Info.err_string, err_msg, ERR_MSG_LENGTH - 1);
    }
  else if (err_indicator == DBMS_ERROR_INDICATOR)
    {
      tmp_err_msg = db_error_string (1);
      strncpy (err_Info.err_string, tmp_err_msg, ERR_MSG_LENGTH - 1);
    }
  err_Info.err_string[ERR_MSG_LENGTH - 1] = 0;

  return err_indicator;
}

void
error_info_clear (void)
{
  err_Info.err_indicator = ERROR_INDICATOR_UNSET;
  err_Info.err_number = CAS_NO_ERROR;
  memset (err_Info.err_string, 0x00, ERR_MSG_LENGTH);
  memset (err_Info.err_file, 0x00, ERR_FILE_LENGTH);
  err_Info.err_line = 0;
}


void
set_server_aborted (bool is_aborted)
{
  server_aborted = is_aborted;
}

bool
is_server_aborted (void)
{
  return server_aborted;
}
