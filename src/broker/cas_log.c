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
#include <stdlib.h>
#include <time.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/time.h>
#include <assert.h>

#include "porting.h"
#include "cas_common.h"
#include "cas_log.h"
#include "cas_util.h"
#include "broker_config.h"

#include "broker_env_def.h"
#include "broker_filename.h"
#include "broker_util.h"
#include "dbi.h"
#include "broker_shm.h"

#define SQL_LOG_BUFFER_SIZE 163840
#define ACCESS_LOG_IS_DENIED_TYPE(T)  ((T)==ACL_REJECTED)

#define CAS_LOG_IS_CLOSED(CAS_LOG_HANDLE)	\
	((CAS_LOG_HANDLE)->log_fp == NULL)

#define IS_CAS_LOG_WRITE_MODE(CAS_LOG_TYPE)			\
	((CAS_LOG_TYPE == CAS_LOG_SQL_LOG &&			\
	  as_info->cur_sql_log_mode != SQL_LOG_MODE_NONE) ||	\
	 (CAS_LOG_TYPE == CAS_LOG_SLOW_LOG &&			\
	  as_info->cur_slow_log_mode != SLOW_LOG_MODE_OFF))

#define IS_CAS_LOG_ACCESS_LOG(CAS_LOG_TYPE)			\
	(CAS_LOG_TYPE == CAS_LOG_ACCESS_LOG ||			\
	 CAS_LOG_TYPE == CAS_LOG_DENIED_ACCESS_LOG)

typedef struct
{
  char log_filepath[BROKER_PATH_MAX];
  char *log_buffer;
  T_CAS_LOG_TYPE cas_log_type;
  int capacity;
  int cur_pos;
  FILE *log_fp;
} T_CAS_LOG_HANDLE;

typedef enum
{
  CAS_LOG_BACKUP_BAK,
  CAS_LOG_BACKUP_DATE
} T_CAS_LOG_BACKUP_TYPE;

static void cas_log_write_v (T_CAS_LOG_TYPE cas_log_type, int flag,
			     struct timeval *log_time,
			     unsigned int seq_num, const char *fmt,
			     va_list ap);
static T_CAS_LOG_HANDLE *get_cas_log_handle (T_CAS_LOG_TYPE cas_log_type);
static void cas_log_open (T_CAS_LOG_HANDLE * log_handle);
static void cas_log_close (T_CAS_LOG_HANDLE * log_handle);

static void cas_log_handle_init (T_CAS_LOG_HANDLE * log_handle,
				 T_CAS_LOG_TYPE cas_log_type, int capacity);
static void cas_log_handle_vprint (T_CAS_LOG_HANDLE * log_handle,
				   const char *format, va_list ap);

static void cas_sql_log_query_cancel (void);
static void make_cas_log_filename (T_CAS_LOG_TYPE cas_log_type,
				   char *filename_buf, size_t buf_size);
static void cas_log_backup (T_CAS_LOG_HANDLE * log_handle,
			    T_CAS_LOG_BACKUP_TYPE cas_log_backup_type);

static void cas_log_handle_reset (T_CAS_LOG_HANDLE * log_handle,
				  FILE * log_fp);

bool sql_log_Notice_mode_flush = false;

static T_APPL_SERVER_INFO *as_info;
static T_SHM_APPL_SERVER *shm_appl;
static int cas_id;
static INT64 query_Cancel_time = 0;

static T_CAS_LOG_HANDLE cas_Log_handle_arr[CAS_LOG_TYPE_MAX + 1];

void
cas_log_init (T_SHM_APPL_SERVER * shm_p, T_APPL_SERVER_INFO * as_info_p,
	      int id)
{
  int i;

  shm_appl = shm_p;
  as_info = as_info_p;
  cas_id = id;

  for (i = 0; i <= CAS_LOG_TYPE_MAX; i++)
    {
      int capacity;

      if (i == CAS_LOG_SQL_LOG || i == CAS_LOG_SLOW_LOG)
	{
	  capacity = 160 * ONE_K;
	}
      else
	{
	  capacity = 8 * ONE_K;
	}

      cas_log_handle_init (&cas_Log_handle_arr[i], i, capacity);
    }
}

void
cas_log_close_all ()
{
  cas_log_close (get_cas_log_handle (CAS_LOG_SQL_LOG));
  cas_log_close (get_cas_log_handle (CAS_LOG_SLOW_LOG));
}

void
cas_sql_log_reset ()
{
  if (as_info->cas_log_reset)
    {
      T_CAS_LOG_HANDLE *log_handle = get_cas_log_handle (CAS_LOG_SQL_LOG);
      cas_log_close (log_handle);

      if ((as_info->cas_log_reset & CAS_LOG_RESET_REMOVE) != 0)
	{
	  unlink (log_handle->log_filepath);
	}

      cas_log_open (log_handle);

      as_info->cas_log_reset = 0;
    }
}

void
cas_slow_log_reset ()
{
  if (as_info->cas_slow_log_reset)
    {
      T_CAS_LOG_HANDLE *log_handle = get_cas_log_handle (CAS_LOG_SLOW_LOG);

      cas_log_close (log_handle);

      if ((as_info->cas_slow_log_reset & CAS_LOG_RESET_REMOVE) != 0)
	{
	  unlink (log_handle->log_filepath);
	}

      cas_log_open (log_handle);

      as_info->cas_slow_log_reset = 0;
    }
}

static T_CAS_LOG_HANDLE *
get_cas_log_handle (T_CAS_LOG_TYPE cas_log_type)
{
  if (cas_log_type <= CAS_LOG_TYPE_MAX)
    {
      return &cas_Log_handle_arr[cas_log_type];
    }
  else
    {
      assert (0);
      return &cas_Log_handle_arr[CAS_LOG_SQL_LOG];
    }
}

static void
cas_log_handle_init (T_CAS_LOG_HANDLE * log_handle,
		     T_CAS_LOG_TYPE cas_log_type, int capacity)
{
  memset (log_handle, 0, sizeof (T_CAS_LOG_HANDLE));

  log_handle->log_buffer = malloc (capacity);
  if (log_handle->log_buffer != NULL)
    {
      log_handle->capacity = capacity;
    }

  log_handle->cas_log_type = cas_log_type;
}

static void
cas_log_handle_reset (T_CAS_LOG_HANDLE * log_handle, FILE * log_fp)
{
  log_handle->cur_pos = 0;
  log_handle->log_fp = log_fp;
}

static void
cas_log_handle_check_flush (T_CAS_LOG_HANDLE * log_handle, bool force_flush,
			    int add_size)
{
  if (!CAS_LOG_IS_CLOSED (log_handle))
    {
      if (log_handle->cur_pos + add_size >= log_handle->capacity)
	{
	  sql_log_Notice_mode_flush = true;
	  force_flush = true;
	}

      if (force_flush)
	{
	  fwrite (log_handle->log_buffer, log_handle->cur_pos, 1,
		  log_handle->log_fp);
	  cas_log_handle_reset (log_handle, log_handle->log_fp);
	}
    }
}

static void
cas_log_handle_putc (T_CAS_LOG_HANDLE * log_handle, char c)
{
  cas_log_handle_check_flush (log_handle, false, 1);

  log_handle->log_buffer[log_handle->cur_pos] = c;
  log_handle->cur_pos += 1;
}

static void
cas_log_handle_puts (T_CAS_LOG_HANDLE * log_handle, const char *ptr, int size)
{
  assert (!CAS_LOG_IS_CLOSED (log_handle));

  cas_log_handle_check_flush (log_handle, false, size);

  if (size >= log_handle->capacity)
    {
      fwrite (ptr, size, 1, log_handle->log_fp);
    }
  else
    {
      memcpy (log_handle->log_buffer + log_handle->cur_pos, ptr, size);
      log_handle->cur_pos += size;
    }
}

static void
cas_log_handle_print (T_CAS_LOG_HANDLE * log_handle, const char *format, ...)
{
  va_list ap;

  assert (!CAS_LOG_IS_CLOSED (log_handle));

  va_start (ap, format);
  cas_log_handle_vprint (log_handle, format, ap);
  va_end (ap);
}

static void
cas_log_handle_vprint (T_CAS_LOG_HANDLE * log_handle, const char *format,
		       va_list ap)
{
  va_list ap_copy;
  int len;

  assert (!CAS_LOG_IS_CLOSED (log_handle));

  va_copy (ap_copy, ap);

  len = vsnprintf (log_handle->log_buffer + log_handle->cur_pos,
		   log_handle->capacity - log_handle->cur_pos, format, ap);

  if (len < log_handle->capacity - log_handle->cur_pos)
    {
      log_handle->cur_pos += len;
    }
  else
    {
      cas_log_handle_check_flush (log_handle, true, 0);

      if (len >= log_handle->capacity)
	{
	  vfprintf (log_handle->log_fp, format, ap_copy);
	}
      else
	{
	  len = vsnprintf (log_handle->log_buffer, log_handle->capacity,
			   format, ap_copy);
	  log_handle->cur_pos = len;
	}
    }

  va_end (ap_copy);
}

static long
cas_log_file_size (T_CAS_LOG_HANDLE * log_handle)
{
  if (CAS_LOG_IS_CLOSED (log_handle))
    {
      return -1;
    }
  else
    {
      return ftell (log_handle->log_fp);
    }
}

static void
make_cas_log_filename (T_CAS_LOG_TYPE cas_log_type, char *filename_buf,
		       size_t buf_size)
{
  char dirname[BROKER_PATH_MAX];

  assert (filename_buf != NULL);

  if (cas_log_type == CAS_LOG_SQL_LOG)
    {
      if (as_info->cas_log_reset == CAS_LOG_RESET_REOPEN)
	{
	  set_rye_file (FID_LOG_DIR, shm_appl->log_dir,
			shm_appl->broker_name);
	}

      get_rye_file (FID_SQL_LOG_DIR, dirname, BROKER_PATH_MAX);
      snprintf (filename_buf, buf_size, "%s%s_%d.sql.log", dirname,
		shm_appl->broker_name, cas_id);
    }
  else if (cas_log_type == CAS_LOG_SLOW_LOG)
    {
      if (as_info->cas_slow_log_reset == CAS_LOG_RESET_REOPEN)
	{
	  set_rye_file (FID_LOG_DIR, shm_appl->log_dir,
			shm_appl->broker_name);
	}

      get_rye_file (FID_SLOW_LOG_DIR, dirname, BROKER_PATH_MAX);
      snprintf (filename_buf, buf_size, "%s%s_%d.slow.log", dirname,
		shm_appl->broker_name, cas_id);
    }
  else if (IS_CAS_LOG_ACCESS_LOG (cas_log_type))
    {
      const char *denied_file_postfix;

      get_rye_file (FID_LOG_DIR, dirname, BROKER_PATH_MAX);

      if (cas_log_type == CAS_LOG_DENIED_ACCESS_LOG)
	{
	  denied_file_postfix = ACCESS_LOG_DENIED_FILENAME_POSTFIX;
	}
      else
	{
	  denied_file_postfix = "";
	}

      snprintf (filename_buf, buf_size, "%s%s%s%s",
		dirname, shm_appl->broker_name,
		ACCESS_LOG_FILENAME_POSTFIX, denied_file_postfix);
    }
  else
    {
      assert (0);
      filename_buf[0] = '\0';
    }
}

static void
cas_log_open (T_CAS_LOG_HANDLE * log_handle)
{
  if (log_handle->capacity > 0 && CAS_LOG_IS_CLOSED (log_handle) &&
      (IS_CAS_LOG_WRITE_MODE (log_handle->cas_log_type) ||
       IS_CAS_LOG_ACCESS_LOG (log_handle->cas_log_type)))
    {
      FILE *log_fp;

      make_cas_log_filename (log_handle->cas_log_type,
			     log_handle->log_filepath, BROKER_PATH_MAX);

      /* note: in "a+" mode, output is always appended */
      log_fp = fopen (log_handle->log_filepath, "a");
      if (log_fp != NULL)
	{
	  setbuf (log_fp, NULL);
	}

      cas_log_handle_reset (log_handle, log_fp);
    }
}

static void
cas_log_close (T_CAS_LOG_HANDLE * log_handle)
{
  if (!CAS_LOG_IS_CLOSED (log_handle))
    {
      fclose (log_handle->log_fp);
    }

  cas_log_handle_reset (log_handle, NULL);
}

static void
cas_log_backup (T_CAS_LOG_HANDLE * log_handle,
		T_CAS_LOG_BACKUP_TYPE cas_log_backup_type)
{
  char backup_filepath[BROKER_PATH_MAX];
  char *filepath;

  assert (log_handle->log_filepath[0] != '\0');
  filepath = log_handle->log_filepath;

  if (cas_log_backup_type == CAS_LOG_BACKUP_BAK)
    {
      snprintf (backup_filepath, BROKER_PATH_MAX, "%s.bak", filepath);
    }
  else if (cas_log_backup_type == CAS_LOG_BACKUP_DATE)
    {
      struct tm ct;
      time_t cur_time = time (NULL);

      if (localtime_r (&cur_time, &ct) == NULL)
	{
	  return;
	}

      snprintf (backup_filepath, BROKER_PATH_MAX,
		"%s.%04d%02d%02d%02d%02d%02d", filepath,
		ct.tm_year + 1900, ct.tm_mon + 1, ct.tm_mday,
		ct.tm_hour, ct.tm_min, ct.tm_sec);
    }
  else
    {
      assert (0);
      return;
    }

  unlink (backup_filepath);
  rename (filepath, backup_filepath);
}

void
cas_sql_log_end (bool flush, int run_time_sec, int run_time_msec)
{
  T_CAS_LOG_HANDLE *log_handle = get_cas_log_handle (CAS_LOG_SQL_LOG);

  if (!CAS_LOG_IS_CLOSED (log_handle))
    {
      if (flush)
	{
	  long file_size;

	  if (run_time_sec >= 0 || run_time_msec >= 0)
	    {
	      cas_sql_log_write (0, "*** elapsed time %d.%03d\n",
				 run_time_sec, run_time_msec);
	    }

	  cas_log_handle_check_flush (log_handle, true, 0);

	  file_size = cas_log_file_size (log_handle);

	  if ((file_size / 1000) > shm_appl->sql_log_max_size)
	    {
	      cas_log_close (log_handle);
	      cas_log_backup (log_handle, CAS_LOG_BACKUP_BAK);
	      cas_log_open (log_handle);
	    }
	}
      else
	{
	  cas_log_handle_reset (log_handle, log_handle->log_fp);
	}
    }
}

void
cas_log_write (T_CAS_LOG_TYPE cas_log_type, int flag,
	       struct timeval *log_time, unsigned int seq_num,
	       const char *fmt, ...)
{
  va_list ap;

  va_start (ap, fmt);
  cas_log_write_v (cas_log_type, flag, log_time, seq_num, fmt, ap);
  va_end (ap);
}

static void
cas_log_write_v (T_CAS_LOG_TYPE cas_log_type, int flag,
		 struct timeval *log_time, unsigned int seq_num,
		 const char *fmt, va_list ap)
{
  T_CAS_LOG_HANDLE *log_handle = get_cas_log_handle (cas_log_type);

  cas_log_open (log_handle);

  if (cas_log_type == CAS_LOG_SQL_LOG)
    {
      cas_sql_log_query_cancel ();
    }

  if (!CAS_LOG_IS_CLOSED (log_handle))
    {
      char timebuf[256];
      int n;

      n = er_datetime (log_time, timebuf, sizeof (timebuf));
      assert (n > 0);

      if (flag & CAS_LOG_FLAG_PRINT_HEADER)
	{
	  cas_log_handle_puts (log_handle, timebuf, n);
	  cas_log_handle_print (log_handle, " (%u) ", seq_num);
	}

      cas_log_handle_vprint (log_handle, fmt, ap);

      if (flag & (CAS_LOG_FLAG_PRINT_NL | CAS_LOG_FLAG_LOG_END))
	{
	  cas_log_handle_putc (log_handle, '\n');
	}

      if (cas_log_type == CAS_LOG_SQL_LOG &&
	  as_info->cur_sql_log_mode == SQL_LOG_MODE_ALL)
	{
	  cas_log_handle_check_flush (log_handle, true, 0);
	}

      if (flag & CAS_LOG_FLAG_LOG_END)
	{
	  if (cas_log_type == CAS_LOG_SQL_LOG)
	    {
	      cas_sql_log_end (true, -1, -1);
	    }
	  else if (cas_log_type == CAS_LOG_SLOW_LOG)
	    {
	      cas_slow_log_end ();
	    }
	  else if (IS_CAS_LOG_ACCESS_LOG (cas_log_type))
	    {
	      long file_size;

	      cas_log_handle_check_flush (log_handle, true, 0);

	      file_size = cas_log_file_size (log_handle);
	      cas_log_close (log_handle);

	      if ((file_size / ONE_K) > shm_appl->access_log_max_size)
		{
		  cas_log_backup (log_handle, CAS_LOG_BACKUP_DATE);
		}
	    }
	}
    }
}

void
cas_sql_log_set_query_cancel_time (INT64 cancel_time)
{
  query_Cancel_time = cancel_time;
}

static void
cas_sql_log_query_cancel ()
{
  if (query_Cancel_time > 0)
    {
      struct timeval tv;
      char ip_str[16];

      tv.tv_sec = query_Cancel_time / 1000;
      tv.tv_usec = (query_Cancel_time % 1000) * 1000;

      ut_get_ipv4_string (ip_str, sizeof (ip_str), as_info->cas_clt_ip);

      query_Cancel_time = 0;

      cas_log_write (CAS_LOG_SQL_LOG,
		     CAS_LOG_FLAG_PRINT_HEADER | CAS_LOG_FLAG_PRINT_NL,
		     &tv, 0, "query_cancel client ip %s port %u",
		     ip_str, as_info->cas_clt_port);
    }
}

void
cas_log_write_string (T_CAS_LOG_TYPE cas_log_type, char *value,
		      int size, bool print_nl)
{
  T_CAS_LOG_HANDLE *log_handle = get_cas_log_handle (cas_log_type);

  cas_log_open (log_handle);

  if (!CAS_LOG_IS_CLOSED (log_handle))
    {
      cas_log_handle_puts (log_handle, value, size);

      if (print_nl)
	{
	  cas_log_handle_putc (log_handle, '\n');
	}
    }
}

int
cas_access_log (struct timeval *start_time, int as_index, int client_ip_addr,
		char *dbname, char *dbuser, ACCESS_LOG_TYPE log_type)
{
  char clt_ip_str[16];
  T_CAS_LOG_TYPE cas_log_type;
  char timebuf[256];
  const char *access_type_str;

  er_datetime (start_time, timebuf, sizeof (timebuf));

  if (ACCESS_LOG_IS_DENIED_TYPE (log_type))
    {
      cas_log_type = CAS_LOG_DENIED_ACCESS_LOG;
      access_type_str = "REJECT";
    }
  else
    {
      cas_log_type = CAS_LOG_ACCESS_LOG;
      access_type_str = (log_type == NEW_CONNECTION ? "NEW" : "OLD");
    }

  ut_get_ipv4_string (clt_ip_str, sizeof (clt_ip_str),
		      (unsigned char *) (&client_ip_addr));

  cas_log_write (cas_log_type,
		 CAS_LOG_FLAG_PRINT_HEADER | CAS_LOG_FLAG_LOG_END,
		 start_time, db_get_session_id (),
		 "%d %s %s %s %s",
		 as_index + 1, clt_ip_str, dbname, dbuser, access_type_str);

  return 0;
}

void
cas_slow_log_end ()
{
  T_CAS_LOG_HANDLE *log_handle = get_cas_log_handle (CAS_LOG_SLOW_LOG);

  if (!CAS_LOG_IS_CLOSED (log_handle))
    {
      long cur_file_pos;

      cur_file_pos = cas_log_file_size (log_handle);

      cas_log_handle_putc (log_handle, '\n');
      cas_log_handle_check_flush (log_handle, true, 0);

      if ((cur_file_pos / 1000) > shm_appl->sql_log_max_size)
	{
	  cas_log_close (log_handle);
	  cas_log_backup (log_handle, CAS_LOG_BACKUP_BAK);
	  cas_log_open (log_handle);
	}
    }
}
