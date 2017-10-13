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
 * file_io_lock.c - input/output lock module (at server)
 */

#ident "$Id$"

#include "config.h"

#include <stdlib.h>
#include <stddef.h>
#include <stdio.h>
#include <string.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <limits.h>
#include <sys/stat.h>
#include <assert.h>

#include <unistd.h>
#include <sys/time.h>
#include <sys/param.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/vfs.h>

#if defined(USE_AIO)
#include <aio.h>
#endif /* USE_AIO */

#include "porting.h"

#include "chartype.h"
#include "file_io.h"
#include "file_io_lock.h"
#include "storage_common.h"
#include "memory_alloc.h"
#include "error_manager.h"
#include "critical_section.h"
#include "system_parameter.h"
#include "databases_file.h"
#include "message_catalog.h"
#include "util_func.h"
#include "perf_monitor.h"
#include "environment_variable.h"
#include "page_buffer.h"
#include "connection_error.h"
#include "release_string.h"
#include "xserver_interface.h"
#include "log_manager.h"
#include "perf_monitor.h"
#if defined(SERVER_MODE)
#include "server_support.h"
#endif

#if defined(SERVER_MODE)
#include "connection_error.h"
#include "network_interface_sr.h"
#endif /* SERVER_MODE */

#include "intl_support.h"


#ifdef L_cuserid
#define FILEIO_USER_NAME_SIZE L_cuserid
#else /* L_cuserid */
#define FILEIO_USER_NAME_SIZE 9
#endif /* L_cuserid */

#define GETPID()  getpid()

#define FILEIO_MAX_WAIT_DBTXT              300
#define FILEIO_VOLLOCK_SUFFIX        "__lock"

#define fileio_lock_file_read(fd, offset, whence, len) \
  fileio_lock_region(fd, F_SETLK, F_RDLCK, offset, whence, len)
#define fileio_lock_file_readw(fd, offset, whence, len) \
  fileio_lock_region(fd, F_SETLKW, F_RDLCK, offset, whence, len)
#define fileio_lock_file_write(fd, offset, whence, len) \
  fileio_lock_region(fd, F_SETLK, F_WRLCK, offset, whence, len)
#define fileio_lock_file_writew(fd, offset, whence, len) \
  fileio_lock_region(fd, F_SETLKW, F_WRLCK, offset, whence, len)
#define fileio_unlock_file(fd, offset, whence, len) \
  fileio_lock_region(fd, F_SETLK, F_UNLCK, offset, whence, len)

static void fileio_make_volume_lock_name (char *vol_lockname,
					  const char *vol_fullname);
static bool fileio_is_terminated_process (int pid);
static int fileio_lock_region (int fd, int cmd, int type, off_t offset,
			       int whence, off_t len);

/*
 * fileio_is_terminated_process () -
 *   return:
 *   pid(in):
 */
static bool
fileio_is_terminated_process (int pid)
{
  if (kill (pid, 0) == -1)
    {
      return true;
    }
  else
    {
      return false;
    }
}

/*
 * fileio_lock () - LOCKF A DATABASE VOLUME
 *   return:
 *   db_fullname(in): Name of the database where the volume belongs
 *   vlabel(in): Volume label
 *   vdes(in): Volume descriptor
 *   dowait(in): true when it is ok to wait for the lock (databases.txt)
 *
 */
FILEIO_LOCKF_TYPE
fileio_lock (const char *db_full_name_p, const char *vol_label_p,
	     int vol_fd, bool dowait)
{
  FILE *fp;
  char name_info_lock[PATH_MAX];
  char host[MAXHOSTNAMELEN];
  char host2[MAXHOSTNAMELEN];
  char user[FILEIO_USER_NAME_SIZE];
  char login_name[FILEIO_USER_NAME_SIZE];
  INT64 lock_time = 0;
  long long tmp_lock_time;
  int pid;
  bool retry = true;
  int lockf_errno;
  FILEIO_LOCKF_TYPE result = FILEIO_LOCKF;
  int total_num_loops = 0;
  int num_loops = 0;
  int max_num_loops;
  char io_timeval[CTIME_MAX], format_string[32];

  if (prm_get_bool_value (PRM_ID_IO_LOCKF_ENABLE) != true)
    {
      return FILEIO_LOCKF;
    }

#if defined(RYE_DEBUG)
  struct stat stbuf;

  /*
   * Make sure that advisory locks are used. An advisory lock is desired
   * since we are observing a voluntarily locking scheme.
   * Mandatory locks are know to be dangerous. If a runaway or otherwise
   * out-of-control process should hold a mandatory lock on the database
   * and fail to release that lock,  the entire database system could hang
   */
  if (fstat (vol_fd, &stbuf) != -1)
    {
      if ((stbuf.st_mode & S_ISGID) != 0
	  && (stbuf.st_mode & S_IRWXG) != S_IXGRP)
	{
	  er_log_debug (ARG_FILE_LINE,
			"A mandatory lock will be set on file = %s",
			vol_label_p);
	}
    }
#endif /* RYE_DEBUG */

  if (vol_label_p == NULL)
    {
      vol_label_p = "";
    }

  max_num_loops = FILEIO_MAX_WAIT_DBTXT;
  fileio_make_volume_lock_name (name_info_lock, vol_label_p);

  /*
   * NOTE: The lockby auxiliary file is created only after we have acquired
   *       the lock. This is important to avoid a possible synchronization
   *       problem with this secundary technique
   */

  sprintf (format_string, "%%%ds %%d %%%ds %%lld", FILEIO_USER_NAME_SIZE - 1,
	   MAXHOSTNAMELEN - 1);

again:
  while (retry == true && fileio_lock_file_write (vol_fd, 0, SEEK_SET, 0) < 0)
    {
      if (errno == EINTR)
	{
	  /* Retry if the an interruption was signed */
	  retry = true;
	  continue;
	}
      lockf_errno = errno;
      retry = false;

      /* Volume seems to be mounted by someone else. Find out who has it. */
      fp = fopen (name_info_lock, "r");
      if (fp == NULL)
	{

	  (void) sleep (3);
	  num_loops += 3;
	  total_num_loops += 3;
	  fp = fopen (name_info_lock, "r");
	  if (fp == NULL && num_loops <= 3)
	    {
	      /*
	       * Note that we try to check for the lock only one more time,
	       * unless we have been waiting for a while
	       * (Case of dowait == false,
	       * note that num_loops is set to 0 when waiting for a lock).
	       */
	      retry = true;
	      continue;
	    }
	}

      if (fp == NULL || fscanf (fp, format_string, user, &pid, host,
				&tmp_lock_time) != 4)
	{
	  strcpy (user, "???");
	  strcpy (host, "???");
	  pid = 0;
	  lock_time = 0;
	}
      else
	{
	  lock_time = tmp_lock_time;
	}
      /* Make sure that the process holding the lock is not a
       * run away process. A run away process is one
       * of the following:
       *
       * 1) If the lockby file exist and the following is true:
       *    same user, same host, and lockby process does not exist any
       *    longer
       */
      if (fp == NULL)
	{
	  /* It is no more true that if the lockby file does not exist, then it
	   * is the run away process. When the user cannot get the file lock,
	   * it means that the another process who owns the database exists.
	   */
	  fileio_ctime (&lock_time, io_timeval);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IO_MOUNT_LOCKED, 6,
		  vol_label_p, db_full_name_p, user, pid, host,
		  (lock_time == 0) ? "???" : io_timeval);
	  return FILEIO_NOT_LOCKF;
	}
      else
	{
	  (void) fclose (fp);
	  *host2 = '\0';
	  cuserid ((char *) login_name);

	  login_name[FILEIO_USER_NAME_SIZE - 1] = '\0';

	  if (!(strcmp (user, login_name) == 0
		&& GETHOSTNAME (host2, MAXHOSTNAMELEN) == 0
		&& strcmp (host, host2) == 0
		&& fileio_is_terminated_process (pid) != 0 && errno == ESRCH))
	    {
	      if (dowait != false)
		{
		  /*
		   * NOBODY USES dowait EXPECT DATABASE.TXT
		   *
		   * It would be nice if we could use a wait function to wait on a
		   * process that is not a child process.
		   * Wait until the process is gone if we are in the same machine,
		   * otherwise, continue looping.
		   */
		  while (fileio_is_volume_exist (name_info_lock) == true
			 && num_loops < 60 && total_num_loops < max_num_loops)
		    {
		      if (strcmp (host, host2) == 0
			  && fileio_is_terminated_process (pid) != 0)
			{
			  break;
			}

		      (void) sleep (3);
		      num_loops += 3;
		      total_num_loops += 3;
		    }

		  if (total_num_loops < max_num_loops)
		    {
		      retry = true;
		      num_loops = 0;
		      goto again;
		    }
		}

	      /* not a run away process */
	      fileio_ctime (&lock_time, io_timeval);
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_IO_MOUNT_LOCKED, 6, vol_label_p, db_full_name_p,
		      user, pid, host, (lock_time == 0) ? "???" : io_timeval);
	      return FILEIO_NOT_LOCKF;
	    }
	}
#if defined(RYE_DEBUG)
      er_log_debug (ARG_FILE_LINE, "io_lock: WARNING ignoring a run away"
		    " lock on volume = %s\n. locked daemon may not be"
		    " working right.\n UNIX error = %s",
		    vol_label_p, strerror (lockf_errno));
#endif /* RYE_DEBUG */
    }

  /* Create the information lock file and write the information about the
     lock */
  fp = fopen (name_info_lock, "w");
  if (fp != NULL)
    {
      if (GETHOSTNAME (host, MAXHOSTNAMELEN) != 0)
	{
	  strcpy (host, "???");
	}

      if (getuserid (login_name, FILEIO_USER_NAME_SIZE) == NULL)
	{
	  strcpy (login_name, "???");
	}

      (void) fprintf (fp, "%s %d %s %ld", login_name, (int) GETPID (),
		      host, time (NULL));
      (void) fclose (fp);
    }
  else
    {
      /* Unable to create the lockf file. */
      if (result == FILEIO_LOCKF)
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_IO_MOUNT_FAIL, 1, name_info_lock);
	  fileio_unlock (vol_label_p, vol_fd, result);
	  result = FILEIO_NOT_LOCKF;
	}
    }

  return result;
}

/*
 * fileio_lock_la_log_path () - LOCKF A applylogdb logpath lock
 *   return:
 *   db_fullname(in): Name of the database where the volume belongs
 *   lock_path(in): Lock file path
 *   vol_fd(in): Volume descriptor
 *
 */
FILEIO_LOCKF_TYPE
fileio_lock_la_log_path (const char *db_full_name_p, const char *lock_path_p,
			 int vol_fd)
{
  FILE *fp;
  char host[MAXHOSTNAMELEN];
  char user[FILEIO_USER_NAME_SIZE];
  char login_name[FILEIO_USER_NAME_SIZE];
  INT64 lock_time;
  long long tmp_lock_time;
  int pid;
  bool retry = true;
  int lockf_errno;
  FILEIO_LOCKF_TYPE result = FILEIO_LOCKF;
  int num_loops = 0;
  char io_timeval[64], format_string[32];
  int new_fd;

#if defined(RYE_DEBUG)
  struct stat stbuf;

  /*
   * Make sure that advisory locks are used. An advisory lock is desired
   * since we are observing a voluntarily locking scheme.
   * Mandatory locks are know to be dangerous. If a runaway or otherwise
   * out-of-control process should hold a mandatory lock on the database
   * and fail to release that lock,  the entire database system could hang
   */
  if (fstat (vol_fd, &stbuf) != -1)
    {
      if ((stbuf.st_mode & S_ISGID) != 0
	  && (stbuf.st_mode & S_IRWXG) != S_IXGRP)
	{
	  er_log_debug (ARG_FILE_LINE,
			"A mandatory lock will be set on file = %s",
			vol_label_p);
	}
    }
#endif /* RYE_DEBUG */

  if (lock_path_p == NULL)
    {
      lock_path_p = "";
    }

  /*
   * NOTE: The lockby auxiliary file is created only after we have acquired
   *       the lock. This is important to avoid a possible synchronization
   *       problem with this secundary technique
   */
  sprintf (format_string, "%%%ds %%d %%%ds %%lld",
	   FILEIO_USER_NAME_SIZE - 1, MAXHOSTNAMELEN - 1);

  while (retry == true && fileio_lock_file_write (vol_fd, 0, SEEK_SET, 0) < 0)
    {
      if (errno == EINTR)
	{
	  /* Retry if the an interruption was signed */
	  retry = true;
	  continue;
	}
      lockf_errno = errno;
      retry = false;

      /* Volume seems to be mounted by someone else. Find out who has it. */
      fp = fopen (lock_path_p, "r");
      if (fp == NULL)
	{
	  (void) sleep (3);
	  num_loops += 3;
	  fp = fopen (lock_path_p, "r");
	  if (fp == NULL && num_loops <= 3)
	    {
	      retry = true;
	      continue;
	    }
	}

      if (fp == NULL
	  || fscanf (fp, format_string, user, &pid,
		     host, &tmp_lock_time) != 4)
	{
	  strcpy (user, "???");
	  strcpy (host, "???");
	  pid = 0;
	  lock_time = 0;
	}
      else
	{
	  lock_time = tmp_lock_time;
	}

      if (fp != NULL)
	{
	  (void) fclose (fp);
	}
#if defined(RYE_DEBUG)
      er_log_debug (ARG_FILE_LINE, "io_lock: WARNING ignoring a run away"
		    " lock on volume = %s\n. locked daemon may not be"
		    " working right.\n UNIX error = %s",
		    lock_path_p, strerror (lockf_errno));
#endif /* RYE_DEBUG */

      memset (io_timeval, 0, sizeof (io_timeval));
      fileio_ctime (&lock_time, io_timeval);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IO_MOUNT_LOCKED, 6,
	      lock_path_p, db_full_name_p, user, pid, host,
	      (lock_time == 0) ? "???" : io_timeval);
      return FILEIO_NOT_LOCKF;
    }

  /* Create the information lock file and write the information about the
     lock */
  new_fd = dup (vol_fd);
  if (new_fd != -1)
    {
      fp = fdopen (new_fd, "w+");
    }
  else
    {
      fp = NULL;		/* error */
    }

  if (fp != NULL)
    {
      lseek (new_fd, (off_t) 0, SEEK_SET);

      if (GETHOSTNAME (host, MAXHOSTNAMELEN) != 0)
	{
	  strcpy (host, "???");
	}

      if (getuserid (login_name, FILEIO_USER_NAME_SIZE) == NULL)
	{
	  strcpy (login_name, "???");
	}

      (void) fprintf (fp, "%s %d %s %ld",
		      login_name, (int) GETPID (), host, time (NULL));
      fflush (fp);

      (void) fclose (fp);
    }
  else
    {
      /* Unable to create the lockf file. */
      assert (result == FILEIO_LOCKF);
      if (result == FILEIO_LOCKF)
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_IO_MOUNT_FAIL, 1, lock_path_p);
	  result = FILEIO_NOT_LOCKF;
	}
    }

  return result;
}

/*
 * fileio_lock_la_dbname () - LOCKF A applylogdb database lock
 *   return:
 *
 *   lockf_vdes(in): lock file descriptor
 *   db_name(in): database name
 *   log_path(in): log file path
 *
 */
FILEIO_LOCKF_TYPE
fileio_lock_la_dbname (int *lockf_vdes, char *db_name, char *log_path)
{
  int error = NO_ERROR;
  int fd = NULL_VOLDES;
  int pid;
  int r;
  FILEIO_LOCKF_TYPE result = FILEIO_LOCKF;
  FILE *fp = NULL;
  char lock_dir[PATH_MAX], lock_path[PATH_MAX];
  char tmp_db_name[DB_MAX_IDENTIFIER_LENGTH], tmp_log_path[PATH_MAX];
  char format_string[PATH_MAX];
  const char *base_p = NULL;

  base_p = prm_get_string_value (PRM_ID_HA_COPY_LOG_BASE);
  if (base_p == NULL || *base_p == '\0')
    {
      base_p = envvar_get (DATABASES_ENVNAME);
      if (base_p == NULL)
	{
	  base_p = ".";
	}
    }
  snprintf (lock_dir, sizeof (lock_dir), "%s/APPLYLOGDB", base_p);
  snprintf (lock_path, sizeof (lock_path), "%s/%s", lock_dir, db_name);

  if (access (lock_dir, F_OK) < 0)
    {
      if (mkdir (lock_dir, 0777) < 0)
	{
	  er_log_debug (ARG_FILE_LINE, "unable to create dir (%s)", lock_dir);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_BO_DIRECTORY_DOESNOT_EXIST, 1, lock_dir);
	  result = FILEIO_NOT_LOCKF;
	  goto error_return;
	}
    }

  snprintf (format_string, sizeof (format_string), "%%d %%%ds %%%ds",
	    DB_MAX_IDENTIFIER_LENGTH - 1, PATH_MAX - 1);

  fd = fileio_open (lock_path, O_RDWR | O_CREAT, 0644);
  if (fd == NULL_VOLDES)
    {
      er_log_debug (ARG_FILE_LINE, "unable to open lock_file (%s)",
		    lock_path);
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IO_MOUNT_FAIL,
			   1, lock_path);

      result = FILEIO_NOT_LOCKF;
      goto error_return;
    }

  fp = fopen (lock_path, "r");
  if (fp)
    {
      fseek (fp, (off_t) 0, SEEK_SET);

      r = fscanf (fp, format_string, &pid, tmp_db_name, tmp_log_path);
      if (r == 3)
	{
	  assert_release (strcmp (db_name, tmp_db_name) == 0);

	  if (strcmp (db_name, tmp_db_name)
	      || strcmp (log_path, tmp_log_path))
	    {
	      er_log_debug (ARG_FILE_LINE, "db_name(%s,%s), log_path(%s,%s)",
			    db_name, tmp_db_name, log_path, tmp_log_path);

	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_IO_MOUNT_LOCKED, 6, lock_path, db_name, "-", pid,
		      "-", "-");

	      fclose (fp);

	      result = FILEIO_NOT_LOCKF;
	      goto error_return;
	    }
	}

      fclose (fp);
    }
  else
    {
      er_log_debug (ARG_FILE_LINE, "unable to open lock_file (%s)",
		    lock_path);
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IO_MOUNT_FAIL,
			   1, lock_path);

      result = FILEIO_NOT_LOCKF;
      goto error_return;
    }

  if (fileio_lock_file_write (fd, 0, SEEK_SET, 0) < 0)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IO_MOUNT_LOCKED, 6,
	      lock_path, db_name, "-", 0, "-", "-");

      result = FILEIO_NOT_LOCKF;
      goto error_return;
    }

  fp = fopen (lock_path, "w+");
  if (fp)
    {
      fseek (fp, (off_t) 0, SEEK_SET);

      pid = getpid ();
      fprintf (fp, "%-10d %s %s", pid, db_name, log_path);
      fflush (fp);
      fclose (fp);
    }
  else
    {
      error = fileio_release_lock (fd);
      assert_release (error == NO_ERROR);

      er_log_debug (ARG_FILE_LINE, "unable to open lock_file (%s)",
		    lock_path);
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IO_MOUNT_FAIL,
			   1, lock_path);

      result = FILEIO_NOT_LOCKF;
      goto error_return;
    }

  (*lockf_vdes) = fd;

  return result;

error_return:

  if (fd != NULL_VOLDES)
    {
      fileio_close (fd);
      fd = NULL_VOLDES;
    }

  (*lockf_vdes) = fd;

  return result;
}

/*
 * fileio_unlock_la_dbname () - UNLOCKF A applylogdb database lock
 *   return:
 *
 *   lockf_vdes(in): lock file descriptor
 *   db_name(in): database name
 *   clear_owner(in): clear lock owner
 *
 */
FILEIO_LOCKF_TYPE
fileio_unlock_la_dbname (int *lockf_vdes, char *db_name, bool clear_owner)
{
  int result;
  int error;
  off_t end_offset;
  FILE *fp = NULL;
  char lock_dir[PATH_MAX], lock_path[PATH_MAX];
  const char *base_p = NULL;

  if ((*lockf_vdes) == NULL_VOLDES)
    {
      assert (false);
      return FILEIO_NOT_LOCKF;
    }

  base_p = prm_get_string_value (PRM_ID_HA_COPY_LOG_BASE);
  if (base_p == NULL || *base_p == '\0')
    {
      base_p = envvar_get (DATABASES_ENVNAME);
      if (base_p == NULL)
	{
	  base_p = ".";
	}
    }
  snprintf (lock_dir, sizeof (lock_dir), "%s/APPLYLOGDB", base_p);
  snprintf (lock_path, sizeof (lock_path), "%s/%s", lock_dir, db_name);

  if (access (lock_dir, F_OK) < 0)
    {
      er_log_debug (ARG_FILE_LINE, "lock directory does not exist (%s)",
		    lock_dir);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_BO_DIRECTORY_DOESNOT_EXIST,
	      1, lock_dir);
      return FILEIO_NOT_LOCKF;
    }

  if (clear_owner)
    {
      fp = fopen (lock_path, "w+");
      if (fp == NULL)
	{
	  er_log_debug (ARG_FILE_LINE, "unable to open lock_file (%s)",
			lock_path);
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_IO_MOUNT_FAIL, 1, lock_path);

	  return FILEIO_LOCKF;
	}

      fseek (fp, (off_t) 0, SEEK_END);
      end_offset = ftell (fp);
      fseek (fp, (off_t) 0, SEEK_SET);

      if (end_offset > 0)
	{
	  fprintf (fp, "%*s", (int) end_offset, " ");
	}
      fflush (fp);
      fclose (fp);
    }

  error = fileio_release_lock ((*lockf_vdes));
  if (error == NO_ERROR)
    {
      result = FILEIO_NOT_LOCKF;
    }
  else
    {
      assert (error == NO_ERROR);
      result = FILEIO_LOCKF;
    }

  if (result == FILEIO_NOT_LOCKF)
    {
      fileio_close ((*lockf_vdes));
      (*lockf_vdes) = NULL_VOLDES;
    }

  return result;
}

static void
fileio_check_lockby_file (char *name_info_lock_p)
{
  /*
   * Either we did not acquire the lock through flock or seek has failed.
   * Use secondary technique for verification.
   * Make sure that current process has the lock, that is, check if
   * these are the consecuences of a run away process. If the lockby file
   * indicates that current process has the lock, remove the lockby file
   * to indicate that the process does not have the lock any longer.
   */
  FILE *fp;
  int pid;
  char login_name[FILEIO_USER_NAME_SIZE];
  char user[FILEIO_USER_NAME_SIZE];
  char host[MAXHOSTNAMELEN];
  char host2[MAXHOSTNAMELEN];
  char format_string[32];

  fp = fopen (name_info_lock_p, "r");
  if (fp != NULL)
    {
      sprintf (format_string, "%%%ds %%d %%%ds", FILEIO_USER_NAME_SIZE - 1,
	       MAXHOSTNAMELEN - 1);
      if (fscanf (fp, format_string, user, &pid, host) != 3)
	{
	  strcpy (user, "???");
	  strcpy (host, "???");
	  pid = 0;
	}
      (void) fclose (fp);

      /* Check for same process, same user, same host */
      getuserid (login_name, FILEIO_USER_NAME_SIZE);

      if (pid == GETPID () && strcmp (user, login_name) == 0
	  && GETHOSTNAME (host2, MAXHOSTNAMELEN) == 0
	  && strcmp (host, host2) == 0)
	{
	  (void) remove (name_info_lock_p);
	}
    }
}

/*
 * fileio_unlock () - UNLOCK A DATABASE VOLUME
 *   return: void
 *   vlabel(in): Volume label
 *   vdes(in): Volume descriptor
 *   lockf_type(in): Type of lock
 *
 * Note: The volume associated with the given name is unlocked and the
 *       lock information file is removed.
 *       If the Unix system complains that the volume is not locked by
 *       the requested process, the information lock file is consulted
 *       to verify for run a way process. If the requested process is
 *       identical to the one recorded in the information lock, the
 *       function returns without any error, otherwise, an error is set
 *       and an error condition is returned.
 */
void
fileio_unlock (const char *vol_label_p, int vol_fd,
	       FILEIO_LOCKF_TYPE lockf_type)
{
  char name_info_lock[PATH_MAX];

  assert (lockf_type == FILEIO_LOCKF);

  if (prm_get_bool_value (PRM_ID_IO_LOCKF_ENABLE) == true)
    {
      if (vol_label_p == NULL)
	{
	  vol_label_p = "";
	}

      strcpy (name_info_lock, vol_label_p);
      fileio_make_volume_lock_name (name_info_lock, vol_label_p);

      /*
       * We must remove the lockby file before we call flock to unlock the file.
       * Otherwise, we may remove the file when is locked by another process
       * Case of preemption and another process acquiring the lock.
       */

      if (lockf_type != FILEIO_LOCKF)
	{
	  assert (lockf_type == FILEIO_NOT_LOCKF);
	  fileio_check_lockby_file (name_info_lock);
	}
      else
	{
	  (void) remove (name_info_lock);
	  fileio_unlock_file (vol_fd, 0, SEEK_SET, 0);
	}
    }
}

/*
 * fileio_get_lock () -
 *   return:
 *   fd(in):
 *   vol_label(in):
 */
int
fileio_get_lock (int fd, const char *vol_label_p)
{
  int error = NO_ERROR;

  if (fileio_lock_file_read (fd, 0, SEEK_SET, 0) < 0)
    {
      error = ER_IO_GET_LOCK_FAIL;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   error, 2, vol_label_p, fd);
    }

  return error;
}

/*
 * fileio_get_lock_retry () -
 *   return:
 *   fd(in):
 *   vol_label(in):
 */
int
fileio_get_lock_retry (int fd, const char *vol_label_p)
{
  int error = NO_ERROR;
  bool retry = true;
  int num_loops = 0;
  int max_num_loops;

  max_num_loops = FILEIO_MAX_WAIT_DBTXT;

again:
  while (retry == true && fileio_lock_file_read (fd, 0, SEEK_SET, 0) < 0)
    {
      if (errno == EINTR)
	{
	  /* Retry if the an interruption was signed */
	  retry = true;
	  continue;
	}

      retry = false;

      (void) sleep (3);
      num_loops += 3;

      if (num_loops < max_num_loops)
	{
	  retry = true;
	  goto again;
	}
    }

  if (retry == false)
    {
      error = ER_IO_GET_LOCK_FAIL;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   error, 2, vol_label_p, fd);
    }

  return error;
}

/*
 * fileio_release_lock () -
 *   return:
 *   fd(in):
 */
int
fileio_release_lock (int fd)
{
  int error = NO_ERROR;

  if (fileio_unlock_file (fd, 0, SEEK_SET, 0) < 0)
    {
      error = ER_IO_RELEASE_LOCK_FAIL;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, fd);
    }

  return error;
}

/*
 * fileio_make_volume_lock_name () - Build the name of volumes
 *   return: void
 *   vol_lockname(out):
 *   vol_fullname(in):
 *
 * Note: The caller must have enough space to store the name of the volume
 *       that is constructed(sprintf). It is recommended to have at least
 *       DB_MAX_PATH_LENGTH length.
 */
static void
fileio_make_volume_lock_name (char *vol_lock_name_p,
			      const char *vol_full_name_p)
{
  sprintf (vol_lock_name_p, "%s%s", vol_full_name_p, FILEIO_VOLLOCK_SUFFIX);
}

/*
 * fileio_lock_region () -
 *   return:
 *   fd(in):
 *   cmd(in):
 *   type(in):
 *   offset(in):
 *   whence(in):
 *   len(in):
 */
static int
fileio_lock_region (int fd, int cmd, int type, off_t offset,
		    int whence, off_t len)
{
  struct flock lock;

  assert (fd != NULL_VOLDES);
  assert (offset == 0);
  assert (whence == SEEK_SET);
  assert (len == 0);

  lock.l_type = type;		/* F_RDLOCK, F_WRLOCK, F_UNLOCK */
  lock.l_start = offset;	/* byte offset, relative to l_whence */
  lock.l_whence = whence;	/* SEEK_SET, SEEK_CUR, SEEK_END */
  lock.l_len = len;		/* #bytes (O means to EOF) */

  return fcntl (fd, cmd, &lock);
}
