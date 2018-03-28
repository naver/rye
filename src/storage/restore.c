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
 * restore.c - restore module
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
#include <assert.h>
#include <unistd.h>
#include <sys/time.h>

#include "porting.h"
#include "storage_common.h"
#include "file_io.h"
#include "memory_alloc.h"
#include "error_manager.h"
#include "critical_section.h"
#include "system_parameter.h"
#include "databases_file.h"
#include "message_catalog.h"
#include "util_func.h"
#include "environment_variable.h"
#include "page_buffer.h"
#include "connection_error.h"
#include "release_string.h"
#include "xserver_interface.h"
#include "log_manager.h"
#if defined(SERVER_MODE)
#include "server_support.h"
#endif

#if defined(SERVER_MODE)
#include "connection_error.h"
#include "network_interface_sr.h"
#endif /* SERVER_MODE */

#include "intl_support.h"
#include "backup.h"
#include "restore.h"
#include "boot_sr.h"

static BK_BACKUP_SESSION *bk_start_restore (THREAD_ENTRY * thread_p,
					    const char *db_fullname,
					    char *backup_source,
					    INT64 match_dbcreation,
					    PGLENGTH * db_iopagesize,
					    RYE_VERSION * bkup_version,
					    BK_BACKUP_SESSION * session,
					    bool authenticate,
					    INT64 match_bkupcreation,
					    const char
					    *restore_verbose_file_path);
static int bk_finish_restore (THREAD_ENTRY * thread_p,
			      BK_BACKUP_SESSION * session);
static void bk_abort_restore (THREAD_ENTRY * thread_p,
			      BK_BACKUP_SESSION * session);
static int bk_get_next_restore_file (THREAD_ENTRY * thread_p,
				     BK_BACKUP_SESSION * session,
				     char *filename, VOLID * volid);
static int bk_restore_volume (THREAD_ENTRY * thread_p,
			      BK_BACKUP_SESSION * session, char *to_vlabel,
			      char *verbose_to_vlabel, char *prev_vlabel);
static int bk_skip_restore_volume (THREAD_ENTRY * thread_p,
				   BK_BACKUP_SESSION * session);

static BK_BACKUP_SESSION
  * bk_initialize_restore (THREAD_ENTRY * thread_p,
			   const char *db_fullname, char *backup_src,
			   BK_BACKUP_SESSION * session,
			   const char *restore_verbose_file_path);
static int bk_read_restore (THREAD_ENTRY * thread_p,
			    BK_BACKUP_SESSION * session, int toread_nbytes);
static void *bk_write_restore (THREAD_ENTRY * thread_p, int vdes,
			       void *io_pgptr, VOLID volid, PAGEID pageid);
static int bk_read_restore_header (BK_BACKUP_SESSION * session);

static BK_BACKUP_SESSION
  * bk_continue_restore (THREAD_ENTRY * thread_p, const char *db_fullname,
			 INT64 db_creation,
			 BK_BACKUP_SESSION * session, bool first_time,
			 bool authenticate, INT64 match_bkupcreation);
static int bk_fill_hole_during_restore (THREAD_ENTRY * thread_p,
					int *next_pageid, int stop_pageid,
					BK_BACKUP_SESSION * session);
static int bk_decompress_restore_volume (THREAD_ENTRY * thread_p,
					 BK_BACKUP_SESSION * session,
					 int nbytes);
static int logpb_check_stop_at_time (BK_BACKUP_SESSION * session,
				     time_t stop_at, time_t backup_time);

/*
 * bk_initialize_restore () - Initialize the restore session structure with the given
 *                      information
 *   return: session or NULL
 *   db_fullname(in): Name of the database to backup
 *   backup_src(in): Name of backup device (file or directory)
 *   session(in/out): The session array
 *   restore_verbose_file_path(in):
 *   newvolpath(in): restore the database and log volumes to the path
 *                   specified in the database-loc-file
 *
 * Note: Note that the user may choose a new location for the volume, so the
 *       contents of the backup source path may be set as a side effect.
 */
static BK_BACKUP_SESSION *
bk_initialize_restore (UNUSED_ARG THREAD_ENTRY * thread_p,
		       const char *db_full_name_p,
		       char *backup_source_p, BK_BACKUP_SESSION * session_p,
		       const char *restore_verbose_file_path)
{
  char orig_name[PATH_MAX];

  STRNCPY (orig_name, backup_source_p, PATH_MAX);
  /* First, make sure the volume given exists and we can access it. */
  while (!fileio_is_volume_exist (backup_source_p))
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IO_MOUNT_FAIL,
			   1, backup_source_p);
      fprintf (stdout, "%s\n", er_msg ());
      return NULL;
    }

  if (bk_init_backup_buffer (session_p, db_full_name_p, backup_source_p, 0) !=
      NO_ERROR)
    {
      return NULL;
    }

  if (bk_init_backup_vol_buffer (session_p, 0, 0) != NO_ERROR)
    {
      return NULL;
    }

  if (restore_verbose_file_path && *restore_verbose_file_path)
    {
      session_p->verbose_fp = fopen (restore_verbose_file_path, "w");
      if (session_p->verbose_fp == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_IO_CANNOT_OPEN_VERBOSE_FILE, 1,
		  restore_verbose_file_path);

	  return NULL;
	}

      setbuf (session_p->verbose_fp, NULL);
    }
  else
    {
      session_p->verbose_fp = NULL;
    }

  return session_p;
}

/*
 * bk_abort_restore () - The restore session is aborted
 *   return: void
 *   session(in/out): The session array
 */
static void
bk_abort_restore (UNUSED_ARG THREAD_ENTRY * thread_p,
		  BK_BACKUP_SESSION * session_p)
{
  bk_abort_backup_client (session_p, false);
  bk_abort_backup_server (session_p);
}

/*
 * bk_read_restore () - The number of bytes to read from the backup destination
 *   return:
 *   session(in/out): The session array
 *   toread_nbytes(in): Number of bytes to read
 *
 * Note: Now handles reads which span volumes, as well as reading incomplete
 *       blocks at the end of one volume.  See bk_flush_backup for details
 *       about how the final block is repeated at the start of the new volumes.
 */
static int
bk_read_restore (UNUSED_ARG THREAD_ENTRY * thread_p,
		 BK_BACKUP_SESSION * session_p, int to_read_nbytes)
{
  ssize_t nbytes;
  char *buffer_p;
  bool is_end_of_backup = false;
  UNUSED_VAR bool is_need_next_vol = false;

  /* Read until you acumulate the desired number of bytes (a database page)
     or the EOF mark is reached. */
  buffer_p = (char *) session_p->dbfile.area;
  while (to_read_nbytes > 0 && is_end_of_backup == false)
    {
      if (session_p->bkup.count <= 0)
	{
	  /*
	   * Read and buffer another backup page from the backup volume.
	   * Note that a backup page is not necessarily the same size as the
	   * database page.
	   */
#if 0
	restart_newvol:
#endif
	  is_need_next_vol = false;
	  session_p->bkup.ptr = session_p->bkup.buffer;
	  session_p->bkup.count = session_p->bkup.iosize;
	  while (session_p->bkup.count > 0)
	    {
	      /* Read a backup I/O page. */
	      nbytes =
		read (session_p->bkup.vdes, session_p->bkup.ptr,
		      session_p->bkup.count);
	      if (nbytes <= 0)
		{
		  switch (errno)
		    {
		    case EINTR:
		    case EAGAIN:
		      continue;
		    default:
		      er_set_with_oserror (ER_ERROR_SEVERITY,
					   ARG_FILE_LINE,
					   ER_IO_READ, 2,
					   CEIL_PTVDIV
					   (session_p->bkup.voltotalio,
					    IO_PAGESIZE),
					   session_p->bkup.vlabel);
		      return ER_FAILED;
		    }
		}
	      else
		{
		  /* Increase the amount of read bytes */
		  session_p->bkup.ptr += nbytes;
		  session_p->bkup.count -= nbytes;
		  session_p->bkup.voltotalio += nbytes;
		}
	    }

	  /* Increase the buffered information */
	  session_p->bkup.ptr = session_p->bkup.buffer;
	  session_p->bkup.count =
	    session_p->bkup.iosize - session_p->bkup.count;
	}

      /* Now copy the desired bytes */
      nbytes = session_p->bkup.count;
      if (nbytes > to_read_nbytes)
	{
	  nbytes = to_read_nbytes;
	}
      memcpy (buffer_p, session_p->bkup.ptr, nbytes);
      session_p->bkup.count -= nbytes;
      to_read_nbytes -= nbytes;
      session_p->bkup.ptr += nbytes;
      buffer_p += nbytes;
    }

  if (to_read_nbytes > 0 && !is_end_of_backup)
    {
      return ER_FAILED;
    }
  else
    {
      return NO_ERROR;
    }
}

/*
 * bk_read_restore_header () - READ A BACKUP VOLUME HEADER
 *   return:
 *   session(in/out): The session array
 *
 * Note: This routine should be the first read from a backup volume or device.
 *       It reads the backup volume header that was written with the
 *       bk_write_backup_header routine.  The header was written with a
 *       specific buffer block size to be more compatible with tape devices so
 *       we can read it in without knowing how the rest of the data was
 *       buffered. Note this also means that backup volume headers are not the
 *       same size as BK_BACKUP_PAGE anymore.
 */
static int
bk_read_restore_header (BK_BACKUP_SESSION * session_p)
{
  int to_read_nbytes;
  int nbytes;
  char *buffer_p;
  BK_BACKUP_HEADER *backup_header_p;

  backup_header_p = session_p->bkuphdr;
  to_read_nbytes = BK_BACKUP_HEADER_IO_SIZE;
  buffer_p = (char *) backup_header_p;
  while (to_read_nbytes > 0)
    {
      nbytes = read (session_p->bkup.vdes, buffer_p, to_read_nbytes);
      if (nbytes == -1)
	{
	  if (errno == EINTR)
	    {
	      continue;
	    }
	  else
	    {
	      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
				   ER_IO_READ, 2,
				   CEIL_PTVDIV (session_p->bkup.voltotalio,
						IO_PAGESIZE),
				   session_p->bkup.vlabel);
	      return ER_FAILED;
	    }
	}
      else if (nbytes == 0)
	{
	  /* EOF should not happen when reading the header. */
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_IO_READ, 2,
			       CEIL_PTVDIV (session_p->bkup.voltotalio,
					    IO_PAGESIZE),
			       session_p->bkup.vlabel);
	  return ER_FAILED;
	}
      to_read_nbytes -= nbytes;
      session_p->bkup.voltotalio += nbytes;
      buffer_p += nbytes;
    }

  if (to_read_nbytes > 0)
    {
      return ER_FAILED;
    }
  else
    {
      return NO_ERROR;
    }
}

/*
 * bk_start_restore () - Start a restore session
 *   return: session or NULL
 *   db_fullname(in): Name of the database to backup
 *   backup_source(in): Name of backup destination device (file or directory)
 *   match_dbcreation(out): Creation of data base of backup
 *   db_iopagesize(out): Database size of database in backup
 *   bkdb_version(out): Disk compatibility of database in backup
 *   session(in/out): The session array
 *   authenticate(in): true when validation of new bkup volume header needed
 *   match_bkupcreation(in): explicit timestamp to match in new backup volume
 *   restore_verbose_file_path(in):
 *   newvolpath(in): restore the database and log volumes to the path
 *                   specified in the database-loc-file
 */
static BK_BACKUP_SESSION *
bk_start_restore (THREAD_ENTRY * thread_p,
		  const char *db_full_name_p,
		  char *backup_source_p,
		  INT64 match_db_creation_time,
		  PGLENGTH * db_io_page_size_p,
		  RYE_VERSION * bkdb_version,
		  BK_BACKUP_SESSION * session_p,
		  bool is_authenticate, INT64 match_backup_creation_time,
		  const char *restore_verbose_file_path)
{
  BK_BACKUP_SESSION *temp_session_p;

  /* Initialize the session array and open the backup source device. */
  if (bk_initialize_restore
      (thread_p, boot_db_name (), backup_source_p, session_p,
       restore_verbose_file_path) == NULL)
    {
      return NULL;
    }

  temp_session_p = bk_continue_restore (thread_p, db_full_name_p,
					match_db_creation_time, session_p,
					true, is_authenticate,
					match_backup_creation_time);
  if (temp_session_p != NULL)
    {
      *db_io_page_size_p = session_p->bkuphdr->db_iopagesize;
      *bkdb_version = session_p->bkuphdr->bk_db_version;
    }

  return (temp_session_p);
}

/*
 * bk_continue_restore () - CONTINUE A RESTORE SESSION
 *   return: session or NULL
 *   db_fullname(in): Name of the database to backup
 *   db_creation(in): Creation of time data base
 *   session(in/out): The session array
 *   first_time(in): true the first time called during restore
 *   authenticate(in): true when validation of new bkup volume header needed
 *   match_bkupcreation(in): Creation time of backup
 *
 * Note: Called when a new backup volume is needed, this routine locates and
 *       opens the desired backup volume for this database. Also authenticates
 *       that it has the right timestamp, unit_num etc.
 *       The match_dbcreation parameter specifies an explicit bkup timestamp
 *       that must be matched. This is useful when one level is restored and
 *       the next is required. A zero for this variable will ignore the test.
 */
static BK_BACKUP_SESSION *
bk_continue_restore (UNUSED_ARG THREAD_ENTRY * thread_p,
		     const char *db_full_name_p,
		     INT64 db_creation_time,
		     BK_BACKUP_SESSION * session_p,
		     bool is_first_time, bool is_authenticate,
		     INT64 match_backup_creation_time)
{
  BK_BACKUP_HEADER *backup_header_p;
  int unit_num = BK_INITIAL_BACKUP_UNITS;
  PAGEID expect_page_id;
  bool is_need_retry;
  UNUSED_VAR bool is_original_header = true;
  struct stat stbuf;
  const char *db_nopath_name_p;
  char copy_name[PATH_MAX];
  char orig_name[PATH_MAX];
  int exists;
  int search_loop_count = 0;
  char io_timeval[CTIME_MAX];

  memset (io_timeval, 0, sizeof (io_timeval));

  /* Note that for the first volume to be restored, bkuphdr must have
     been initialized with sensible defaults for these variables. */
  do
    {
      is_need_retry = false;
      /* Have to locate and open the desired volume */
      while (session_p->bkup.vdes == NULL_VOLDES)
	{

	  /* If the name chosen is a actually a directory, then append
	     correct backup volume name here. */
	  exists = stat (session_p->bkup.vlabel, &stbuf) != -1;
	  if (exists && S_ISDIR (stbuf.st_mode))
	    {
	      db_nopath_name_p = fileio_get_base_file_name (db_full_name_p);
	      strcpy (copy_name, session_p->bkup.vlabel);
	      bk_make_backup_name (session_p->bkup.name, db_nopath_name_p,
				   copy_name, unit_num);
	      session_p->bkup.vlabel = session_p->bkup.name;
	    }

	  if (search_loop_count == 0)
	    {
	      STRNCPY (orig_name, session_p->bkup.vlabel, PATH_MAX);
	    }

	  session_p->bkup.vdes =
	    fileio_open (session_p->bkup.vlabel, O_RDONLY, 0);
	  if (session_p->bkup.vdes == NULL_VOLDES)
	    {
	      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
				   ER_IO_MOUNT_FAIL, 1,
				   session_p->bkup.vlabel);
	      fprintf (stdout, "%s\n", er_msg ());
	      return NULL;
	    }
	  search_loop_count++;
	}

      /* Read description of the backup file. */
      if (bk_read_restore_header (session_p) != NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IO_NOT_A_BACKUP, 1,
		  session_p->bkup.vlabel);
	  is_need_retry = true;
	  goto retry_newvol;
	}

      backup_header_p = session_p->bkuphdr;

      /* Always check for a valid magic number, regardless of whether
         we need to check other authentications. */
      if (strcmp (backup_header_p->bk_magic, RYE_MAGIC_DATABASE_BACKUP) != 0)
	{
	  if (is_first_time)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IO_NOT_A_BACKUP, 1,
		      session_p->bkup.vlabel);
	      return NULL;
	    }
	  else
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_BK_MAGIC_MISMATCH,
		      1, session_p->bkup.vlabel);
	    }
	}

      /* Should check the release version before we do anything */
      if (is_first_time
	  && rel_is_log_compatible (&backup_header_p->bk_db_version) != true)
	{
	  char bkdb_release[REL_MAX_VERSION_LENGTH];
	  /*
	   * First time this database is restarted using the current version of
	   * Rye. Recovery should be done using the old version of the
	   * system
	   */
	  rel_version_to_string (&backup_header_p->bk_db_version,
				 bkdb_release, sizeof (bkdb_release));
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LOG_RECOVER_ON_OLD_RELEASE, 2, bkdb_release,
		  rel_version_string ());
	  return NULL;
	}

      if (is_authenticate)
	{
	  /* Test the timestamp of when the backup was taken. */
	  if (match_backup_creation_time != 0
	      && difftime ((time_t) match_backup_creation_time,
			   (time_t) backup_header_p->start_time))
	    {
	      char save_time1[64];

	      fileio_ctime (&match_backup_creation_time, io_timeval);
	      strcpy (save_time1, io_timeval);

	      fileio_ctime (&backup_header_p->start_time, io_timeval);
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_BK_BACKUP_TIME_MISMATCH, 3, session_p->bkup.vlabel,
		      save_time1, io_timeval);
	    }

	  /* Should this one be treated as fatal? */
	  expect_page_id =
	    (is_first_time) ? BK_BACKUP_START_PAGE_ID :
	    BK_BACKUP_VOL_CONT_PAGE_ID;
	  if (backup_header_p->iopageid != expect_page_id)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_BK_MAGIC_MISMATCH,
		      1, session_p->bkup.vlabel);
	    }

	  /* NOTE: This could mess with restoring to a new location */
	  if (strcmp (backup_header_p->db_name, boot_db_name ()) != 0
	      || (db_creation_time > 0
		  && difftime ((time_t) db_creation_time,
			       (time_t) backup_header_p->db_creation)))
	    {
	      if (is_first_time)
		{
		  char save_time1[64];
		  char save_time2[64];

		  fileio_ctime (&backup_header_p->db_creation, io_timeval);
		  strcpy (save_time1, io_timeval);

		  fileio_ctime (&db_creation_time, io_timeval);
		  strcpy (save_time2, io_timeval);

		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_IO_NOT_A_BACKUP_OF_GIVEN_DATABASE, 5,
			  session_p->bkup.vlabel, backup_header_p->db_name,
			  save_time1, db_full_name_p, save_time2);
		  return NULL;
		}
	      else
		{
		  fileio_ctime (&backup_header_p->db_creation, io_timeval);
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_BK_DB_MISMATCH, 3, session_p->bkup.vlabel,
			  backup_header_p->db_name, io_timeval);
		}
	    }
	}
      /* Passed all tests above */
      break;
    retry_newvol:
      is_original_header = false;
      /* close it, in case it was opened previously */
      if (session_p->bkup.vdes != NULL_VOLDES)
	{
	  fileio_close (session_p->bkup.vdes);
	  session_p->bkup.vdes = NULL_VOLDES;
	}

      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_LOG_CANNOT_ACCESS_BACKUP, 1, session_p->bkup.vlabel);
      return NULL;
    }
  while (is_need_retry);
  backup_header_p = session_p->bkuphdr;
  /*
   * If we read an archive header and notice that the buffer size
   * was different than our current bkup.iosize then we will have
   * to REALLOC the io areas set up in _init.  Same for the
   * when the database IO pagesize changes.
   */
  if (backup_header_p->bkup_iosize > session_p->bkup.iosize)
    {
      session_p->bkup.buffer =
	(char *) realloc (session_p->bkup.buffer,
			  backup_header_p->bkup_iosize);
      if (session_p->bkup.buffer == NULL)
	{
	  return NULL;
	}
      session_p->bkup.ptr = session_p->bkup.buffer;	/* reinit in case it moved */
    }
  /* Always use the saved size from the backup to restore with */
  session_p->bkup.iosize = backup_header_p->bkup_iosize;
  /* backuped page is bigger than the current DB pagesize.
     must resize read buffer */
  if (is_first_time)
    {
      if (backup_header_p->db_iopagesize > IO_PAGESIZE)
	{
	  int io_pagesize, size;
	  io_pagesize = backup_header_p->db_iopagesize;
	  io_pagesize *= FILEIO_FULL_LEVEL_EXP;

	  size =
	    MAX (io_pagesize + BK_BACKUP_PAGE_OVERHEAD,
		 BK_VOL_HEADER_IN_BACKUP_PAGE_SIZE);
	  free_and_init (session_p->dbfile.area);
	  session_p->dbfile.area = (BK_BACKUP_PAGE *) malloc (size);
	  if (session_p->dbfile.area == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, size);
	      return NULL;
	    }
	}
    }

  return session_p;
}

/*
 * bk_finish_restore () - Finish the restore session
 *   return:
 *   session(in/out): The session array
 */
static int
bk_finish_restore (THREAD_ENTRY * thread_p, BK_BACKUP_SESSION * session_p)
{
  int success;

  success = fileio_synchronize_all (thread_p, false);
  bk_abort_restore (thread_p, session_p);

  return success;
}

/*
 * bk_list_restore () - List description of current backup source
 *   return: session or NULL
 *   db_fullname(in): Name of the database to backup
 *   backup_source(out): Name of backup source device (file or directory)
 *   newvolpath(in): restore the database and log volumes to the path
 *                   specified in the database-loc-file
 */
int
bk_list_restore (THREAD_ENTRY * thread_p, const char *db_full_name_p,
		 char *backup_source_p)
{
  BK_BACKUP_SESSION backup_session;
  BK_BACKUP_SESSION *session_p = &backup_session;
  BK_BACKUP_HEADER *backup_header_p;
  BK_VOL_HEADER_IN_BACKUP *file_header_p;
  PGLENGTH db_iopagesize;
  RYE_VERSION bkup_version;
  int nbytes;
  INT64 db_creation_time = 0;
  char file_name[PATH_MAX];
  time_t tmp_time;
  char time_val[CTIME_MAX];
  char bkup_db_release[REL_MAX_VERSION_LENGTH];
  char db_host_str[MAX_NODE_INFO_STR_LEN];

  if (bk_start_restore (thread_p, db_full_name_p, backup_source_p,
			db_creation_time, &db_iopagesize, &bkup_version,
			session_p, false, 0, NULL) == NULL)
    {
      /* Cannot access backup file.. Restore from backup is cancelled */
      if (er_errid () == ER_GENERIC_ERROR)
	{
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LOG_CANNOT_ACCESS_BACKUP, 1, backup_source_p);
	}
      return ER_FAILED;
    }

  /* First backup header was just read */
  backup_header_p = session_p->bkuphdr;
  /* this check is probably redundant */
  if (backup_header_p->iopageid != BK_BACKUP_START_PAGE_ID
      && backup_header_p->iopageid != BK_BACKUP_VOL_CONT_PAGE_ID)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IO_NOT_A_BACKUP, 1,
	      session_p->bkup.vlabel);
      goto error;
    }

  /* Show the backup volume header information. */
  fprintf (stdout,
	   msgcat_message (MSGCAT_CATALOG_RYE, MSGCAT_SET_IO,
			   MSGCAT_FILEIO_BKUP_HDR));

  tmp_time = (time_t) backup_header_p->db_creation;
  (void) ctime_r (&tmp_time, time_val);
  fprintf (stdout,
	   msgcat_message (MSGCAT_CATALOG_RYE, MSGCAT_SET_IO,
			   MSGCAT_FILEIO_BKUP_HDR_DBINFO),
	   backup_header_p->db_name, time_val,
	   backup_header_p->db_iopagesize);
  fprintf (stdout,
	   msgcat_message (MSGCAT_CATALOG_RYE, MSGCAT_SET_IO,
			   MSGCAT_FILEIO_BKUP_HDR_LEVEL), 0,
	   bk_get_backup_level_string (), (long long int) -1, -1,
	   backup_header_p->chkpt_lsa.pageid,
	   backup_header_p->chkpt_lsa.offset);

  tmp_time = (time_t) backup_header_p->start_time;
  (void) ctime_r (&tmp_time, time_val);
  fprintf (stdout,
	   msgcat_message (MSGCAT_CATALOG_RYE, MSGCAT_SET_IO,
			   MSGCAT_FILEIO_BKUP_HDR_TIME), time_val, 1);
  rel_version_to_string (&backup_header_p->bk_db_version, bkup_db_release,
			 sizeof (bkup_db_release));
  fprintf (stdout,
	   msgcat_message (MSGCAT_CATALOG_RYE, MSGCAT_SET_IO,
			   MSGCAT_FILEIO_BKUP_HDR_RELEASES), bkup_db_release,
	   backup_header_p->bk_db_version.major);
  fprintf (stdout,
	   msgcat_message (MSGCAT_CATALOG_RYE, MSGCAT_SET_IO,
			   MSGCAT_FILEIO_BKUP_HDR_BKUP_PAGESIZE),
	   backup_header_p->bkpagesize);
  fprintf (stdout,
	   msgcat_message (MSGCAT_CATALOG_RYE, MSGCAT_SET_IO,
			   MSGCAT_FILEIO_BKUP_HDR_ZIP_INFO),
	   backup_header_p->zip_method,
	   bk_get_zip_method_string (backup_header_p->zip_method),
	   backup_header_p->zip_level,
	   bk_get_zip_level_string (backup_header_p->zip_level));
  fprintf (stdout,
	   msgcat_message (MSGCAT_CATALOG_RYE, MSGCAT_SET_IO,
			   MSGCAT_FILEIO_BKUP_HDR_INC_ACTIVELOG), "YES");

  assert (!LSA_ISNULL (&(backup_header_p->backuptime_lsa)));
  assert (backup_header_p->end_time > 0);

  prm_node_info_to_str (db_host_str, sizeof (db_host_str),
			&backup_header_p->db_host_info);
  fprintf (stdout,
	   msgcat_message (MSGCAT_CATALOG_RYE, MSGCAT_SET_IO,
			   MSGCAT_FILEIO_BKUP_HDR_HOST_INFO), db_host_str,
	   backup_header_p->server_state,
	   backup_header_p->backuptime_lsa.pageid,
	   backup_header_p->backuptime_lsa.offset,
	   backup_header_p->make_slave ? "YES" : "NO");

  fprintf (stdout, "\n");

  if (backup_header_p->make_slave == true)
    {
      return bk_finish_restore (thread_p, session_p);
    }

  /* Start reading information of every database volumes/files of the
     database which is in backup. */
  file_header_p =
    (BK_VOL_HEADER_IN_BACKUP *) (&session_p->dbfile.area->iopage);
  while (true)
    {
      nbytes = BK_VOL_HEADER_IN_BACKUP_PAGE_SIZE;
      if (bk_read_restore (thread_p, session_p, nbytes) != NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IO_RESTORE_READ_ERROR,
		  1, 1);
	  goto error;
	}

      if (BK_GET_BACKUP_PAGE_ID (session_p->dbfile.area) ==
	  BK_BACKUP_END_PAGE_ID)
	{
	  break;
	}

      if (BK_GET_BACKUP_PAGE_ID (session_p->dbfile.area) !=
	  BK_BACKUP_FILE_START_PAGE_ID)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_IO_BKUP_DATABASE_VOLUME_OR_FILE_EXPECTED, 0);
	  goto error;
	}

      fprintf (stdout,
	       msgcat_message (MSGCAT_CATALOG_RYE, MSGCAT_SET_IO,
			       MSGCAT_FILEIO_BKUP_FILE),
	       file_header_p->vlabel, file_header_p->volid,
	       file_header_p->nbytes, CEIL_PTVDIV (file_header_p->nbytes,
						   IO_PAGESIZE));
      session_p->dbfile.volid = file_header_p->volid;
      session_p->dbfile.nbytes = file_header_p->nbytes;
      STRNCPY (file_name, file_header_p->vlabel, PATH_MAX);
      session_p->dbfile.vlabel = file_name;
      /* Read all file pages until the end of the file */
      if (bk_skip_restore_volume (thread_p, session_p) != NO_ERROR)
	{
	  goto error;
	}
    }

  fprintf (stdout, "\n");
  return bk_finish_restore (thread_p, session_p);
error:
  bk_abort_restore (thread_p, session_p);
  return ER_FAILED;
}

/*
 * bk_get_backup_volume () - Get backup volume
 *   return: session or NULL
 *   db_fullname(in): Name of the database to backup
 *   logpath(in): Directory where the log volumes reside
 *   user_backuppath(in): Backup path that user specified
 *   from_volbackup (out) : Name of the backup volume
 *
 */
int
bk_get_backup_volume (UNUSED_ARG THREAD_ENTRY * thread_p,
		      UNUSED_ARG const char *db_fullname,
		      const char *user_backuppath, char *from_volbackup)
{

  assert (user_backuppath != NULL);

  strncpy (from_volbackup, user_backuppath, PATH_MAX - 1);

  return NO_ERROR;
}


/*
 * bk_get_next_restore_file () - Find information of next file to restore
 *   return: -1 A failure, 0 No more files to restore (End of BACKUP),
 *           1 There is a file to restore
 *   session(in/out): The session array
 *   filename(out): the name of next file to restore
 *   volid(out): Identifier of the database volume/file to restore
 *   vol_nbytes(out): Nbytes of the database volume/file to restore
 */
static int
bk_get_next_restore_file (THREAD_ENTRY * thread_p,
			  BK_BACKUP_SESSION * session_p, char *file_name_p,
			  VOLID * vol_id_p)
{
  BK_VOL_HEADER_IN_BACKUP *file_header_p;
  int nbytes;

  /* Read the next database volume and/or file to restore. */
  file_header_p =
    (BK_VOL_HEADER_IN_BACKUP *) (&session_p->dbfile.area->iopage);
  nbytes = BK_VOL_HEADER_IN_BACKUP_PAGE_SIZE;
  if (bk_read_restore (thread_p, session_p, nbytes) != NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IO_RESTORE_READ_ERROR, 1,
	      1);
      return -1;
    }

  if (BK_GET_BACKUP_PAGE_ID (session_p->dbfile.area) == BK_BACKUP_END_PAGE_ID)
    {
      return 0;
    }

  if (BK_GET_BACKUP_PAGE_ID (session_p->dbfile.area) !=
      BK_BACKUP_FILE_START_PAGE_ID)
    {
      return -1;
    }

  session_p->dbfile.volid = file_header_p->volid;
  session_p->dbfile.nbytes = file_header_p->nbytes;

  strncpy (file_name_p, file_header_p->vlabel, PATH_MAX);

  *vol_id_p = session_p->dbfile.volid;
  return 1;
}

/*
 * bk_fill_hole_during_restore () - Fill in a hole found in the backup during
 *                           a restore
 *   return:
 *   next_pageid(out):
 *   stop_pageid(in):
 *   session(in/out): The session array
 *
 * Note: A hole is likely only for 2 reasons. After the system pages in
 *       permament temp volumes, or at the end of a volume if we stop backing
 *       up unallocated pages.
 */
static int
bk_fill_hole_during_restore (THREAD_ENTRY * thread_p, int *next_page_id_p,
			     int stop_page_id, BK_BACKUP_SESSION * session_p)
{
  FILEIO_PAGE *io_pgptr = NULL;

  io_pgptr = fileio_alloc_io_page (thread_p);
  if (io_pgptr == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      IO_PAGESIZE);
      return ER_FAILED;
    }

  while (*next_page_id_p < stop_page_id)
    {
      /*
       * We did not back up a page since it was deallocated, or there
       * is a hole of some kind that must be filled in with correctly
       * formatted pages.
       */

      if (bk_write_restore (thread_p, session_p->dbfile.vdes,
			    io_pgptr, session_p->dbfile.volid,
			    *next_page_id_p) == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IO_RESTORE_READ_ERROR,
		  1, 1);
	  return ER_FAILED;
	}
      *next_page_id_p += 1;
    }

  if (io_pgptr != NULL)
    {
      free_and_init (io_pgptr);
    }

  return NO_ERROR;
}

/*
 * bk_decompress_restore_volume () - The number of bytes to decompress/read
 *                                        from the backup destination
 *   return:
 *   session(in/out): The session array
 *   nbytes(in): Number of bytes to read
 */
static int
bk_decompress_restore_volume (THREAD_ENTRY * thread_p,
			      BK_BACKUP_SESSION * session_p, int nbytes)
{
  int error = NO_ERROR;
  BK_THREAD_INFO *thread_info_p;
  BK_QUEUE *queue_p;
  BK_BACKUP_HEADER *backup_header_p;
  BK_BACKUP_PAGE *save_area_p;
  BK_NODE *node;

  assert (nbytes >= 0);

  thread_info_p = &session_p->read_thread_info;
  queue_p = &thread_info_p->io_queue;
  backup_header_p = session_p->bkuphdr;
  node = NULL;

  switch (backup_header_p->zip_method)
    {
    case BK_ZIP_NONE_METHOD:
      if (bk_read_restore (thread_p, session_p, nbytes) != NO_ERROR)
	{
	  error = ER_IO_RESTORE_READ_ERROR;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, 1);
	  goto exit_on_error;
	}
      break;

    case BK_ZIP_LZO1X_METHOD:
      {
	int rv;
	/* alloc queue node */
	node = bk_allocate_node (queue_p, backup_header_p);
	if (node == NULL)
	  {
	    goto exit_on_error;
	  }

	save_area_p = session_p->dbfile.area;	/* save link */
	session_p->dbfile.area = (BK_BACKUP_PAGE *) node->zip_page;

	rv = bk_read_restore (thread_p, session_p, sizeof (lzo_uint));
	session_p->dbfile.area = save_area_p;	/* restore link */
	if (rv != NO_ERROR)
	  {
	    error = ER_IO_RESTORE_READ_ERROR;
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, 1);
	    goto exit_on_error;
	  }

	/* sanity check of the size values */
	if (node->zip_page->buf_len > (size_t) nbytes
	    || node->zip_page->buf_len == 0)
	  {
	    error = ER_IO_LZO_COMPRESS_FAIL;	/* may be compress fail */
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 4,
		    backup_header_p->zip_method,
		    bk_get_zip_method_string
		    (backup_header_p->zip_method), backup_header_p->zip_level,
		    bk_get_zip_level_string (backup_header_p->zip_level));
#if defined(RYE_DEBUG)
	    fprintf (stdout,
		     "bk_decompress_restore_volume: "
		     "block size error - data corrupted\n");
#endif /* RYE_DEBUG */
	    goto exit_on_error;
	  }
	else if (node->zip_page->buf_len < (size_t) nbytes)
	  {
	    /* read compressed block data */
	    lzo_uint unzip_len;

	    save_area_p = session_p->dbfile.area;	/* save link */
	    session_p->dbfile.area = (BK_BACKUP_PAGE *) node->zip_page->buf;

	    rv =
	      bk_read_restore (thread_p, session_p, node->zip_page->buf_len);
	    session_p->dbfile.area = save_area_p;	/* restore link */
	    if (rv != NO_ERROR)
	      {
		error = ER_IO_RESTORE_READ_ERROR;
		er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, 1);
		goto exit_on_error;
	      }

	    /* decompress - use safe decompressor as data might be corrupted
	       during a file transfer */
	    unzip_len = nbytes;
	    rv = lzo1x_decompress_safe (node->zip_page->buf,
					node->zip_page->buf_len,
					(lzo_bytep) session_p->dbfile.area,
					&unzip_len, NULL);
	    if (rv != LZO_E_OK || unzip_len != (size_t) nbytes)
	      {
		error = ER_IO_LZO_DECOMPRESS_FAIL;
		er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
#if defined(RYE_DEBUG)
		fprintf (stdout,
			 "bk_decompress_restore_volume: "
			 "compressed data violation\n");
#endif /* RYE_DEBUG */
		goto exit_on_error;
	      }
	  }
	else
	  {
	    /* no compressed block */
	    rv =
	      bk_read_restore (thread_p, session_p, node->zip_page->buf_len);
	    if (rv != NO_ERROR)
	      {
		error = ER_IO_RESTORE_READ_ERROR;
		er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, 1);
		goto exit_on_error;
	      }
	  }

      }
      break;

    default:
      error = ER_IO_RESTORE_READ_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, 1);
      goto exit_on_error;
    }

exit_on_end:

  /* free node */
  if (node)
    {
      (void) bk_free_node (queue_p, node);
    }

  return error;
exit_on_error:

  if (error == NO_ERROR)
    {
      error = ER_FAILED;
    }
  goto exit_on_end;
}

/*
 * bk_restore_volume () - Restore a volume/file of given database
 *   return:
 *   session_p(in/out):  The session array
 *   to_vlabel_p(in): Restore the next file using this name
 *   verbose_to_vlabel_p(in): Printable volume name
 *   prev_vlabel_p(in): Previous restored file name
 */
static int
bk_restore_volume (THREAD_ENTRY * thread_p,
		   BK_BACKUP_SESSION * session_p,
		   char *to_vol_label_p, char *verbose_to_vol_label_p,
		   UNUSED_ARG char *prev_vol_label_p)
{
  int next_page_id = 0;
  INT64 total_nbytes = 0;
  int nbytes;
  int from_npages, npages;
  int check_ratio = 0, check_npages = 0;
  BK_BACKUP_HEADER *backup_header_p = session_p->bkuphdr;
  int unit;
  int i;
  char *buffer_p;

  npages = (int) CEIL_PTVDIV (session_p->dbfile.nbytes, IO_PAGESIZE);
  session_p->dbfile.vlabel = to_vol_label_p;
  nbytes = (int) MIN (backup_header_p->bkpagesize, session_p->dbfile.nbytes);
  unit = nbytes / IO_PAGESIZE;
  if (nbytes % IO_PAGESIZE)
    {
      unit++;
    }

#if defined(RYE_DEBUG)
  if (io_Bkuptrace_debug > 0)
    {
      fprintf (stdout,
	       msgcat_message (MSGCAT_CATALOG_RYE, MSGCAT_SET_IO,
			       MSGCAT_FILEIO_BKUP_FILE),
	       session_p->dbfile.vlabel, session_p->dbfile.volid,
	       session_p->dbfile.nbytes,
	       CEIL_PTVDIV (session_p->dbfile.nbytes, IO_PAGESIZE));
      fprintf (stdout, "\n");
    }
#endif /* RYE_DEBUG */

  if (session_p->verbose_fp)
    {
      fprintf (session_p->verbose_fp, " %-28s | %10d | ",
	       fileio_get_base_file_name (verbose_to_vol_label_p), npages);
      check_ratio = 1;
      check_npages = (int) (((float) npages / 25.0) * check_ratio);
    }

  /*
   * Reformatting the volume guarantees no pollution from old contents.
   * Note that for incremental restores, one can only reformat the volume
   * once ... the first time that volume is replaced.  This is needed
   * because we are applying the restoration in reverse time order.
   */
  if (!fileio_is_volume_exist (session_p->dbfile.vlabel))
    {
      session_p->dbfile.vdes = fileio_format (thread_p, NULL,
					      session_p->dbfile.vlabel,
					      session_p->dbfile.volid, npages,
					      false, false, false,
					      IO_PAGESIZE, 0, false);
    }
  else
    {
      session_p->dbfile.vdes =
	fileio_mount (thread_p, NULL, session_p->dbfile.vlabel,
		      session_p->dbfile.volid, false, false);
    }

  if (session_p->dbfile.vdes == NULL_VOLDES)
    {
      goto error;
    }

  /* Read all file pages until the end of the volume/file. */
  from_npages =
    (int) CEIL_PTVDIV (session_p->dbfile.nbytes, backup_header_p->bkpagesize);
  nbytes = BK_RESTORE_DBVOLS_IO_PAGE_SIZE (session_p);

  while (true)
    {
      if (bk_decompress_restore_volume (thread_p, session_p, nbytes) !=
	  NO_ERROR)
	{
	  goto error;
	}

      if (BK_GET_BACKUP_PAGE_ID (session_p->dbfile.area) ==
	  BK_BACKUP_FILE_END_PAGE_ID)
	{
	  /*
	   * End of File marker in backup, but may not be true end of file being
	   * restored so we have to continue filling in pages until the
	   * restored volume is finished.
	   */
	  if (next_page_id < npages)
	    {
	      if (bk_fill_hole_during_restore
		  (thread_p, &next_page_id, npages, session_p) != NO_ERROR)
		{
		  goto error;
		}
	    }
	  break;
	}

      if (BK_GET_BACKUP_PAGE_ID (session_p->dbfile.area) > from_npages)
	{
	  /* Too many pages for this volume according to the file header */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_IO_RESTORE_PAGEID_OUTOF_BOUNDS, 4, 1,
		  BK_GET_BACKUP_PAGE_ID (session_p->dbfile.area), from_npages,
		  session_p->dbfile.volid);
	  goto error;
	}

#if defined(RYE_DEBUG)
      fprintf (stdout, "bk_restore_volume: %d\t%d,\t%d\n",
	       ((BK_BACKUP_PAGE *) (session_p->dbfile.area))->iopageid,
	       *(PAGEID *) (((char *) (session_p->dbfile.area)) +
			    offsetof (BK_BACKUP_PAGE,
				      iopage) + backup_header_p->bkpagesize),
	       backup_header_p->bkpagesize);
#endif

      /* Check for holes and fill them (only for full backup level) */
      if (next_page_id < BK_GET_BACKUP_PAGE_ID (session_p->dbfile.area))
	{
	  if (bk_fill_hole_during_restore (thread_p, &next_page_id,
					   session_p->dbfile.area->iopageid,
					   session_p) != NO_ERROR)
	    {
	      goto error;
	    }
	}

      buffer_p = (char *) &session_p->dbfile.area->iopage;
      for (i = 0; i < unit && next_page_id < npages; i++)
	{
	  if (bk_write_restore (thread_p, session_p->dbfile.vdes,
				buffer_p + i * IO_PAGESIZE,
				session_p->dbfile.volid,
				next_page_id) == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_IO_RESTORE_READ_ERROR, 1, 1);
	      goto error;
	    }

	  next_page_id += 1;
	  total_nbytes += IO_PAGESIZE;
	  if (session_p->verbose_fp && npages >= 25
	      && next_page_id >= check_npages)
	    {
	      fprintf (session_p->verbose_fp, "#");
	      check_ratio++;
	      check_npages = (int) (((float) npages / 25.0) * check_ratio);
	    }
	}
    }

  if (total_nbytes > session_p->dbfile.nbytes
      && session_p->dbfile.volid < LOG_DBFIRST_VOLID)
    {
      (void) ftruncate (session_p->dbfile.vdes, session_p->dbfile.nbytes);
    }

#if defined(RYE_DEBUG)
  if (io_Bkuptrace_debug >= 2 && cache_p)
    {
      mht_dump (stdout, cache_p->ht, 1, logpb_print_hash_entry, NULL);
      (void) fprintf (stdout, "\n\n");
    }
#endif /* RYE_DEBUG */

  fileio_dismount (thread_p, session_p->dbfile.vdes);
  session_p->dbfile.vdes = NULL_VOLDES;
  session_p->dbfile.volid = NULL_VOLID;
  session_p->dbfile.vlabel = NULL;

  if (session_p->verbose_fp)
    {
      if (next_page_id < 25)
	{
	  fprintf (session_p->verbose_fp,
		   "######################### | done\n");
	}
      else
	{
	  while (check_ratio <= 25)
	    {
	      fprintf (session_p->verbose_fp, "#");
	      check_ratio++;
	    }
	  fprintf (session_p->verbose_fp, " | done\n");
	}
    }

  return NO_ERROR;

error:
  if (session_p->dbfile.vdes != NULL_VOLDES)
    {
      fileio_dismount (thread_p, session_p->dbfile.vdes);
    }

  session_p->dbfile.vdes = NULL_VOLDES;
  session_p->dbfile.volid = NULL_VOLID;
  session_p->dbfile.vlabel = NULL;

  return ER_FAILED;
}

/*
 * bk_write_restore () - Write the content of the page described by pageid
 *                       to disk
 *   return: o_pgptr on success, NULL on failure
 *   vdes(in): Volume descriptor
 *   io_pgptr(in): In-memory address where the current content of page resides
 *   volid(in):
 *   pageid(in): Page identifier
 *
 * Note: The contents of the page stored on io_pgptr buffer which is
 *       IO_PAGESIZE long are sent to disk using fileio_write. The restore pageid
 *       cache is updated.
 */
static void *
bk_write_restore (THREAD_ENTRY * thread_p,
		  UNUSED_ARG int vol_fd, void *io_page_p,
		  UNUSED_ARG VOLID vol_id, PAGEID page_id)
{
  if (fileio_write (thread_p, vol_fd, io_page_p, page_id, IO_PAGESIZE) ==
      NULL)
    {
      return NULL;
    }

  return io_page_p;
}

/*
 * bk_skip_restore_volume () - Skip over the next db volume from the backup
 *                             during a restore
 *   return:
 *   session(in/out): The session array
 *
 * Note: Basically have to read all of the pages until we get to the end of
 *       the current backup file.  It is necessary to "fast forward" to the
 *       next backup meta-data.
 */
static int
bk_skip_restore_volume (THREAD_ENTRY * thread_p,
			BK_BACKUP_SESSION * session_p)
{
  int nbytes;
#if defined(RYE_DEBUG)
  BK_BACKUP_HEADER *backup_header_p = session_p->bkuphdr;
#endif

  /* Read all file pages until the end of the volume/file. */
  nbytes = BK_RESTORE_DBVOLS_IO_PAGE_SIZE (session_p);
  while (true)
    {
      if (bk_decompress_restore_volume (thread_p, session_p, nbytes) !=
	  NO_ERROR)
	{
	  goto error;
	}

      if (BK_GET_BACKUP_PAGE_ID (session_p->dbfile.area) ==
	  BK_BACKUP_FILE_END_PAGE_ID)
	{
	  /* End of FILE */
	  break;
	}

#if defined(RYE_DEBUG)
      fprintf (stdout, "bk_skip_restore_volume: %d\t%d,\t%d\n",
	       ((BK_BACKUP_PAGE *) (session_p->dbfile.area))->iopageid,
	       *(PAGEID *) (((char *) (session_p->dbfile.area)) +
			    offsetof (BK_BACKUP_PAGE,
				      iopage) + backup_header_p->bkpagesize),
	       backup_header_p->bkpagesize);
#endif
    }

  session_p->dbfile.vdes = NULL_VOLDES;
  session_p->dbfile.volid = NULL_VOLID;
  session_p->dbfile.vlabel = NULL;

  return NO_ERROR;

error:

  session_p->dbfile.vdes = NULL_VOLDES;
  session_p->dbfile.volid = NULL_VOLID;
  session_p->dbfile.vlabel = NULL;

  return ER_FAILED;
}

/*
 * logpb_check_stop_at_time - Check if the stopat time is valid
 *
 * return: NO_ERROR if valid, ER_FAILED otherwise
 *
 *   session(in):
 *   stop_at(in):
 *   backup_time(in):
 */
static int
logpb_check_stop_at_time (UNUSED_ARG BK_BACKUP_SESSION * session,
			  time_t stop_at, time_t backup_time)
{
  char ctime_buf1[CTIME_MAX], ctime_buf2[CTIME_MAX];

  if (stop_at < backup_time)
    {
      ctime_r (&stop_at, ctime_buf1);
      ctime_r (&backup_time, ctime_buf2);

      ctime_buf1[MAX (strlen (ctime_buf1) - 1, 0)] = '\0';	/* strip '\n' */
      ctime_buf2[MAX (strlen (ctime_buf2) - 1, 0)] = '\0';

      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_RYE,
				       MSGCAT_SET_LOG,
				       MSGCAT_LOG_UPTODATE_ERROR), ctime_buf1,
	       ctime_buf2);

      return ER_FAILED;
    }

  return NO_ERROR;
}

/*
 * bk_restore - Restore volume from its backup
 *
 * return: NO_ERROR if all OK, ER status otherwise
 *
 *   db_fullname(in): Full name of the database
 *   logpath(in): Directory where the log volumes reside
 *   prefix_logname(in):
 *   newall_path(in):
 *   ask_forpath(in):
 *   r_args(in):
 *
 * NOTE:Restore a database from its backup files. This function is run
 *              without recovery. The logs must be applied to the restored
 *              volume to finish the full restore process. This is not done
 *              here since the database may have other volumes to restore.
 *              We must lock the active log during the restore to keep other
 *              utilities from trying to use the db files being restored.
 *              This routine leaves the database locked, as there will
 *              probably be a restart following the restore.
 *
 *     This function must be run offline.
 *
 *     Note this function is incomplete.. How to change location of files
 *     during restore... The main problem here is the VOLINFO which has to
 *     be recreated without updating the header of the volumes.
 */
int
bk_restore (THREAD_ENTRY * thread_p, const char *db_fullname,
	    const char *logpath, const char *prefix_logname,
	    BO_RESTART_ARG * r_args)
{
  BK_BACKUP_SESSION session_storage;
  BK_BACKUP_SESSION *session = NULL;
  char to_volname[PATH_MAX];	/* Name of a volume (TO)      */
  char verbose_to_volname[PATH_MAX];	/* Printable name of a volume (TO) */
  char prev_volname[PATH_MAX];	/* Name of a prev volume (TO) */
  char from_volbackup[PATH_MAX];	/* Name of the backup volume
					 * (FROM)
					 */
  VOLID to_volid;
  FILE *backup_volinfo_fp = NULL;	/* Pointer to backup
					 * information/directory file
					 */
  int another_vol;
  INT64 db_creation;
  INT64 bkup_match_time = 0;
  PGLENGTH db_iopagesize;
  PGLENGTH log_page_size;
  RYE_VERSION db_version;
  PGLENGTH bkdb_iopagesize;
  RYE_VERSION bkdb_version;
  bool error_expected = false;
  bool restore_in_progress = false;	/* true if any vols restored */
  int lgat_vdes = NULL_VOLDES;
  time_t restore_start_time, restore_end_time;
  char time_val[CTIME_MAX];
  int loop_cnt = 0;
  char lgat_tmpname[PATH_MAX];	/* active log temp name */
  struct stat stat_buf;
  int error_code = NO_ERROR, success = NO_ERROR;
  bool printtoc;
  INT64 backup_time;
  REL_COMPATIBILITY compat;
  int dummy;

  memset (&session_storage, 0, sizeof (BK_BACKUP_SESSION));
  memset (verbose_to_volname, 0, PATH_MAX);
  memset (lgat_tmpname, 0, PATH_MAX);

  LOG_CS_ENTER (thread_p);

  if (logpb_find_header_parameters (thread_p, db_fullname, logpath,
				    prefix_logname, &db_iopagesize,
				    &log_page_size, &db_creation, &db_version,
				    &dummy) == -1)
    {
      db_iopagesize = IO_PAGESIZE;
      log_page_size = LOG_PAGESIZE;
      db_creation = 0;
      db_version = rel_cur_version ();
    }

  /*
   * Must lock the database if possible. Would be nice to have a way
   * to lock the db somehow even when the lgat file does not exist.
   */
  if (fileio_is_volume_exist (log_Name_active))
    {
      lgat_vdes =
	fileio_mount (thread_p, db_fullname, log_Name_active,
		      LOG_DBLOG_ACTIVE_VOLID, true, false);
      if (lgat_vdes == NULL_VOLDES)
	{
	  error_code = ER_FAILED;
	  LOG_CS_EXIT ();
	  goto error;
	}
    }

  error_code =
    bk_get_backup_volume (thread_p, db_fullname, r_args->backuppath,
			  from_volbackup);
  if (error_code != NO_ERROR)
    {
      if (error_code == ER_LOG_CANNOT_ACCESS_BACKUP)
	{
	  error_expected = true;
	}
      LOG_CS_EXIT ();
      goto error;
    }

  from_volbackup[sizeof (from_volbackup) - 1] = '\0';

  printtoc = (r_args->printtoc) ? false : true;
  if (bk_start_restore (thread_p, db_fullname, from_volbackup,
			db_creation, &bkdb_iopagesize,
			&bkdb_version, &session_storage, printtoc,
			bkup_match_time, r_args->verbose_file) == NULL)
    {
      /* Cannot access backup file.. Restore from backup is cancelled */
      if (er_errid () == ER_GENERIC_ERROR)
	{
	  er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_LOG_CANNOT_ACCESS_BACKUP, 1, from_volbackup);
	}
      error_code = ER_LOG_CANNOT_ACCESS_BACKUP;
      LOG_CS_EXIT ();
      goto error;
    }

  session = &session_storage;

  assert (!LSA_ISNULL (&(session->bkuphdr->backuptime_lsa)));
  assert (session->bkuphdr->end_time > 0);

  if (r_args->restore_upto_backuptime)
    {
      assert (r_args->stopat == -1);
    }
  else if (r_args->stopat > 0)
    {
      if (session->bkuphdr->end_time > 0)
	{
	  backup_time = session->bkuphdr->end_time;
	}
      else
	{
	  assert (false);
	  backup_time = session->bkuphdr->start_time;
	}

      error_code =
	logpb_check_stop_at_time (session, r_args->stopat,
				  (time_t) backup_time);
      if (error_code != NO_ERROR)
	{
	  (void) bk_finish_restore (thread_p, session);

	  LOG_CS_EXIT ();
	  goto error;
	}
    }

  if (db_iopagesize != bkdb_iopagesize)
    {
      /*
       * Pagesize is incorrect. We need to undefine anything that has been
       * created with old pagesize and start again.
       * If we do not have a log, we should reset the pagesize and start the
       * restore process.
       */
      if (log_Gl.append.vdes == NULL_VOLDES)
	{
	  /*
	   * Reset the page size
	   */
	  if (db_set_page_size (bkdb_iopagesize, log_page_size) != NO_ERROR)
	    {
	      error_code = ER_FAILED;
	      LOG_CS_EXIT ();
	      goto error;
	    }

	  error_code = logtb_define_trantable_log_latch (thread_p, -1);
	  if (error_code != NO_ERROR)
	    {
	      LOG_CS_EXIT ();
	      goto error;
	    }
	}
    }

  /* Can this be moved to restore_continue? */
  /* Removed strict condition for checking disk compatibility.
   * Check it according to the predefined rules.
   */
  compat = rel_check_disk_compatible (&bkdb_version);
  if (compat != REL_COMPATIBLE)
    {
      char bkdb_release[REL_MAX_VERSION_LENGTH];
      rel_version_to_string (&bkdb_version, bkdb_release,
			     sizeof (bkdb_release));
      /* Database is incompatible with current release */
      error_code = ER_LOG_BKUP_INCOMPATIBLE;
      er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 2,
	      bkdb_release, rel_version_string ());
      LOG_CS_EXIT ();
      goto error;
    }

  if (r_args->make_slave == true)
    {
      goto end;
    }

  if (session->verbose_fp)
    {
      fprintf (session->verbose_fp, "\n[ Database(%s) Restore start ]\n\n",
	       boot_db_name ());

      restore_start_time = time (NULL);
      (void) ctime_r (&restore_start_time, time_val);
      fprintf (session->verbose_fp, "- restore start time: %s\n", time_val);
      fprintf (session->verbose_fp, " step %1d) restore using backup data\n",
	       ++loop_cnt);
      fprintf (session->verbose_fp, "\n");

      fprintf (session->verbose_fp,
	       "- restore progress status (backup data)\n");
      fprintf (session->verbose_fp,
	       " -----------------------------------------------------------------------------\n");
      fprintf (session->verbose_fp,
	       " volume name                  | # of pages |  restore progress status  | done \n");
      fprintf (session->verbose_fp,
	       " -----------------------------------------------------------------------------\n");
    }

  while (success == NO_ERROR)
    {
      another_vol =
	bk_get_next_restore_file (thread_p, session, to_volname, &to_volid);
      if (another_vol == 1)
	{
	  if (session->verbose_fp)
	    {
	      strcpy (verbose_to_volname, to_volname);
	    }

	  if (to_volid == LOG_DBLOG_ACTIVE_VOLID)
	    {
	      /* rename _lgat to _lgat_tmp name */
	      fileio_make_log_active_temp_name (lgat_tmpname, to_volname);
	      strcpy (to_volname, lgat_tmpname);
	    }

	  restore_in_progress = true;

	  success =
	    bk_restore_volume (thread_p, session, to_volname,
			       verbose_to_volname, prev_volname);
	}
      else if (another_vol == 0)
	{
	  break;
	}
      else
	{
	  success = ER_FAILED;
	  break;
	}
    }

  if (session->verbose_fp)
    {
      fprintf (session->verbose_fp,
	       " -----------------------------------------------------------------------------\n\n");
    }

end:
  /* rename logactive tmp to logactive */
  if (stat (log_Name_active, &stat_buf) != 0
      && stat (lgat_tmpname, &stat_buf) == 0)
    {
      rename (lgat_tmpname, log_Name_active);
    }

  unlink (lgat_tmpname);

  r_args->server_state = session->bkuphdr->server_state;
  r_args->db_host_info = session->bkuphdr->db_host_info;
  r_args->backuptime_lsa = session->bkuphdr->backuptime_lsa;
  r_args->db_creation = session->bkuphdr->db_creation;

  if (session != NULL)
    {
      if (session->verbose_fp)
	{
	  restore_end_time = time (NULL);
	  (void) ctime_r (&restore_end_time, time_val);
	  fprintf (session->verbose_fp, "- restore end time: %s\n", time_val);
	  fprintf (session->verbose_fp, "[ Database(%s) Restore end ]\n",
		   boot_db_name ());
	}

      error_code = bk_finish_restore (thread_p, session);
    }

  LOG_CS_EXIT ();

  if (success != NO_ERROR)
    {
      return success;
    }
  else
    {
      return error_code;
    }

  /* **** */
error:
  if (restore_in_progress)
    {
      /*
       * We have probably already restored something to their database
       * and therefore they need to be sure and try another restore until
       * they succeed.
       */
      fprintf (stdout,
	       msgcat_message (MSGCAT_CATALOG_RYE,
			       MSGCAT_SET_LOG,
			       MSGCAT_LOG_READ_ERROR_DURING_RESTORE),
	       session->bkup.name, BK_INITIAL_BACKUP_UNITS, to_volname,
	       session->dbfile.volid);

      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOG_MAYNEED_MEDIA_RECOVERY,
	      1, from_volbackup);
      error_code = ER_LOG_MAYNEED_MEDIA_RECOVERY;
    }

  if (backup_volinfo_fp != NULL)
    {
      fclose (backup_volinfo_fp);
    }

  if (session != NULL)
    {
      bk_abort_restore (thread_p, session);
    }

  if (lgat_vdes != NULL_VOLDES)
    {
      fileio_dismount (thread_p, lgat_vdes);
    }

  if (!error_expected)
    {
      logpb_fatal_error (thread_p, false, ARG_FILE_LINE, "logpb_restore");
    }

  if (lgat_tmpname[0] != '\0')
    {
      unlink (lgat_tmpname);
    }

  return error_code;
}
