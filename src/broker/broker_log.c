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
 * cas_log.c -
 */

#ident "$Id$"

#include <stdio.h>
#include <stdarg.h>
#include <assert.h>
#include <sys/time.h>
#include <pthread.h>

#include "porting.h"
#include "cas_common.h"
#include "broker_config.h"
#include "broker_shm.h"

#include "broker_filename.h"
#include "broker_util.h"
#include "broker_log.h"
#include "error_manager.h"

static void br_log_open (char *br_name, int port);
static void br_log_close (void);
static char *make_broker_log_filename (char *buf, size_t buf_size,
				       const char *br_name, int port);
static void br_log_write_internal (T_BROKER_LOG_SEVERITY severity,
				   struct timeval *logtime,
				   const unsigned char *clt_ip,
				   const char *fmt, va_list ap);
static const char *br_log_severity_str (T_BROKER_LOG_SEVERITY severity);
static void br_log_end (void);

static T_SHM_APPL_SERVER *shm_appl;
static FILE *br_log_fp = NULL;
static char br_log_file[BROKER_PATH_MAX];
static time_t br_log_write_time = 0;
static int cur_broker_log_mode;
static pthread_mutex_t br_log_lock;

void
br_log_init (T_SHM_APPL_SERVER * shm_p)
{
  shm_appl = shm_p;
  cur_broker_log_mode = shm_appl->broker_log_mode;

  pthread_mutex_init (&br_log_lock, NULL);
}

void
br_log_check ()
{
  pthread_mutex_lock (&br_log_lock);

  set_rye_file (FID_LOG_DIR, shm_appl->log_dir, shm_appl->broker_name);

  if (br_log_fp != NULL)
    {
      if (access (br_log_file, F_OK) < 0)
	{
	  br_log_close ();
	}
    }

  if (shm_appl->broker_log_reset)
    {
      br_log_close ();
      cur_broker_log_mode = shm_appl->broker_log_mode;
      shm_appl->broker_log_reset = 0;
    }

  pthread_mutex_unlock (&br_log_lock);
}

void
br_log_write (T_BROKER_LOG_SEVERITY severity, const unsigned char *clt_ip,
	      const char *fmt, ...)
{
  struct timeval logtime;
  va_list ap;

  if ((int) severity > cur_broker_log_mode)
    {
      return;
    }

  pthread_mutex_lock (&br_log_lock);

  gettimeofday (&logtime, NULL);

  br_log_write_time = logtime.tv_sec;

  va_start (ap, fmt);
  br_log_write_internal (severity, &logtime, clt_ip, fmt, ap);
  va_end (ap);

  br_log_write_time = 0;

  pthread_mutex_unlock (&br_log_lock);
}

int
br_log_hang_time ()
{
  time_t last_log_write_time = br_log_write_time;

  if (last_log_write_time > 0)
    {
      return (time (NULL) - last_log_write_time);
    }
  return 0;
}

static void
br_log_write_internal (T_BROKER_LOG_SEVERITY severity,
		       UNUSED_ARG struct timeval *logtime,
		       const unsigned char *clt_ip,
		       const char *fmt, va_list ap)
{
  char time_str[256];
  char clt_ip_str[64];

  if (br_log_fp == NULL && cur_broker_log_mode != BROKER_LOG_MODE_OFF)
    {
      br_log_open (shm_appl->broker_name, shm_appl->broker_port);
    }

  if (br_log_fp != NULL)
    {
      if (clt_ip == NULL)
	{
	  clt_ip_str[0] = '\0';
	}
      else
	{
	  int n;
	  n = sprintf (clt_ip_str, " CLIENT=");
	  ut_get_ipv4_string (clt_ip_str + n, sizeof (clt_ip_str) - n,
			      clt_ip);
	}

      (void) er_datetime (NULL, time_str, sizeof (time_str));
      fprintf (br_log_fp, "%s: %s %s\n",
	       br_log_severity_str (severity), time_str, clt_ip_str);

      vfprintf (br_log_fp, fmt, ap);

      fputc ('\n', br_log_fp);
      fputc ('\n', br_log_fp);

      br_log_end ();
    }
}

static void
br_log_end ()
{
  long file_pos;
  char backupfile[BROKER_PATH_MAX];

  if (br_log_fp != NULL)
    {
      fflush (br_log_fp);

      file_pos = ftell (br_log_fp);
      if (file_pos / 1000 > shm_appl->broker_log_max_size)
	{
	  br_log_close ();

	  snprintf (backupfile, BROKER_PATH_MAX, "%s.bak", br_log_file);
	  unlink (backupfile);
	  if (rename (br_log_file, backupfile) < 0)
	    {
	      assert (0);
	    }
	}

      //TODO: reset
    }
}

static const char *
br_log_severity_str (T_BROKER_LOG_SEVERITY severity)
{
  switch (severity)
    {
    case BROKER_LOG_ERROR:
      return "ERROR";
    case BROKER_LOG_NOTICE:
      return "NOTICE";
    default:
      assert (0);
      return "";
    }
}

static void
br_log_open (char *br_name, int port)
{
  if (br_log_fp != NULL)
    {
      br_log_close ();
    }

  if (cur_broker_log_mode != BROKER_LOG_MODE_OFF)
    {
      make_broker_log_filename (br_log_file, BROKER_PATH_MAX, br_name, port);

      br_log_fp = fopen (br_log_file, "r+");
      if (br_log_fp == NULL)
	{
	  br_log_fp = fopen (br_log_file, "w");
	}
      else
	{
	  fseek (br_log_fp, 0, SEEK_END);
	}
    }
  else
    {
      br_log_fp = NULL;
    }
}

static void
br_log_close ()
{
  if (br_log_fp != NULL)
    {
      fclose (br_log_fp);
      br_log_fp = NULL;
    }
}

static char *
make_broker_log_filename (char *buf, size_t buf_size, const char *br_name,
			  int port)
{
  char dirname[BROKER_PATH_MAX];

  get_rye_file (FID_LOG_DIR, dirname, BROKER_PATH_MAX);
  snprintf (buf, buf_size, "%s%s.%d.log", dirname, br_name, port);
  return buf;
}
