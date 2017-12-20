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
 * rbl_error_log.c -
 */

#ident "$Id$"

#include <stdio.h>
#include <time.h>
#include <pthread.h>
#include <sys/time.h>

#include "porting.h"
#include "error_manager.h"
#include "language_support.h"
#include "environment_variable.h"
#include "rbl_error_log.h"

const char *rbl_Err_msg[] = {
  /* NO_ERROR */
  "",
  /* RBL_CCI_ERROR */
  "CCI Error: code = %d, message = %s\n",
  /* RBL_NODE_NOT_FOUND */
  "Node not found: id = %d\n",
  /* RBL_OUT_OF_MEMORY */
  "Out of memory: unable to allocate %lld memory bytes.\n",
  /* RBL_LOG_PAGE_ERROR */
  "Failed to get log page: page id = %lld, server error= %d\n",
  /* RBL_LOG_DECOMPRESS_FAIL */
  ""
};

static FILE *log_Fp;
static pthread_mutex_t log_Mutex;
static const char *rbl_Severity_msg[] = { "DEBUG", "NOTICE", "ERROR" };
static int max_Err_severity = 0;
static char log_File_path[PATH_MAX];

void
rbl_error_log (int severity, const char *file_name, const int line_no,
	       const char *fmt, ...)
{
  va_list ap;
  char time_array[256];
  int r;

  r = pthread_mutex_lock (&log_Mutex);
  if (r != NO_ERROR)
    {
      return;
    }

  va_start (ap, fmt);

  r = er_datetime (NULL, time_array, sizeof (time_array));
  if (r < 0)
    {
      assert (false);
      va_end (ap);
      pthread_mutex_unlock (&log_Mutex);
      return;
    }

  fprintf (log_Fp, "\nTime: %s - %s - File: %s, Line: %d\n", time_array,
	   rbl_Severity_msg[severity], file_name, line_no);
  vfprintf (log_Fp, fmt, ap);
  fflush (log_Fp);

  va_end (ap);

  if (severity >= RBL_ERROR_SEVERITY)
    {
      va_start (ap, fmt);
      fprintf (stderr, "\nTime: %s - %s - File: %s, Line: %d\n", time_array,
	       rbl_Severity_msg[severity], file_name, line_no);
      vfprintf (stderr, fmt, ap);
      fflush (stderr);
      va_end (ap);
    }

  if (max_Err_severity < severity)
    {
      max_Err_severity = severity;
    }

  pthread_mutex_unlock (&log_Mutex);
}

void
rbl_error_log_init (const char *prefix, char *dbname, int id)
{
  char filename[32];

  sprintf (filename, "%s_%s_%d.err", prefix, dbname, id);
  envvar_ryelogdir_file (log_File_path, PATH_MAX, filename);

  log_Fp = fopen (log_File_path, "w");
  if (log_Fp == NULL)
    {
      log_Fp = stderr;
    }

  (void) pthread_mutex_init (&log_Mutex, NULL);

#if 1				/* TODO - #1074 er Mgr */
  (void) er_init (prm_get_string_value (PRM_ID_ER_LOG_FILE),
		  prm_get_integer_value (PRM_ID_ER_EXIT_ASK));
#endif

  (void) lang_init ();
}

void
rbl_error_log_final (bool remove_log_file)
{
  if (log_Fp != stderr)
    {
      fclose (log_Fp);

      if (remove_log_file == true && max_Err_severity < RBL_ERROR_SEVERITY)
	{
	  unlink (log_File_path);
	}
    }

  pthread_mutex_destroy (&log_Mutex);
}
