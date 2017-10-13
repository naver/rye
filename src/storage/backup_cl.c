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
 * backup_cl.c - backup module (at client)
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
#include "system_parameter.h"
#include "databases_file.h"
#include "message_catalog.h"
#include "network_interface_cl.h"
#include "environment_variable.h"
#include "backup_cl.h"

#define BK_PAGE_SIZE_FULL_LEVEL (IO_PAGESIZE * FILEIO_FULL_LEVEL_EXP)
#define BK_BACKUP_DBVOLS_IO_PAGE_SIZE_FULL_LEVEL \
  (BK_PAGE_SIZE_FULL_LEVEL + BK_BACKUP_PAGE_OVERHEAD)

/* Define minimum number of pages required for a backup volume
   For now, specify at least 4 pages plus the header. */
#define BK_BACKUP_MINIMUM_NUM_PAGES_FULL_LEVEL \
  CEIL_PTVDIV((BK_BACKUP_HEADER_IO_SIZE +   \
              (BK_BACKUP_DBVOLS_IO_PAGE_SIZE_FULL_LEVEL) * 4), IO_PAGESIZE)


static int bk_create_backup_volume (const char *db_name, const char *vlabel);
static int bk_write_backup_header (BK_BACKUP_SESSION * session_p);
static int bk_write_backup_end_time_to_header (BK_BACKUP_SESSION * session_p);
static void bk_verbose_backup_info (BK_BACKUP_SESSION * session,
				    char *db_name, int num_threads);
static int bk_finish_backup_session (BK_BACKUP_SESSION * session_p);

int
bk_run_backup (char *db_name, char *db_host, const char *backup_path,
	       const char *backup_verbose_file_path, int num_threads,
	       int do_compress, int sleep_msecs,
	       int delete_unneeded_logarchives, bool force_overwrite,
	       int make_slave, HA_STATE server_state)
{
  BK_BACKUP_SESSION session;
  bool does_unformat_bk;
  int error = NO_ERROR;
  time_t db_creation_time;
  time_t backup_end_time;
  char time_val[CTIME_MAX];

  memset (&session, 0, sizeof (BK_BACKUP_SESSION));
  does_unformat_bk = false;

  /* Initialization gives us some useful information about the
   * backup location.
   */

  error = bk_init_backup_buffer (&session, db_name, backup_path, do_compress);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  if (backup_verbose_file_path && *backup_verbose_file_path)
    {
      session.verbose_fp = fopen (backup_verbose_file_path, "w");
      if (session.verbose_fp == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_IO_CANNOT_OPEN_VERBOSE_FILE, 1,
		  backup_verbose_file_path);

	  goto error_exit;
	}

      setbuf (session.verbose_fp, NULL);
    }
  else
    {
      session.verbose_fp = NULL;
    }

  session.sleep_msecs = sleep_msecs;
  session.bkuphdr->make_slave = make_slave;
  session.bkuphdr->server_state = server_state;
  strcpy (session.bkuphdr->db_host, db_host);

  /*
   * Check for existing backup volumes in this location, and warn
   * the user that they will be destroyed.
   */
  if (force_overwrite == false
      && fileio_is_volume_exist (session.bkup.vlabel))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_BO_VOLUME_EXISTS, 1, session.bkup.vlabel);

      goto error_exit;
    }

  does_unformat_bk = true;

  /* receive backup header from server */
  error = bk_prepare_backup (num_threads, do_compress,
			     sleep_msecs, make_slave, &session);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  bk_verbose_backup_info (&session, db_name, num_threads);

  /* Complete the session array initialization and create/open the backup
     destination device. */
  session.bkup.vdes = bk_create_backup_volume (db_name, session.bkup.vlabel);

  if (session.bkup.vdes == NULL_VOLDES)
    {
      goto error_exit;
    }

  /* Now write this information to the backup volume. */
  if (bk_write_backup_header (&session) != NO_ERROR)
    {
      goto error_exit;
    }

  error = bk_backup_volume (&session);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  error = bk_backup_log_volume (&session, delete_unneeded_logarchives);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  assert (!LSA_ISNULL (&(session.bkuphdr->backuptime_lsa)));
  assert (session.bkuphdr->end_time > 0);

  if (session.verbose_fp)
    {
      fprintf (session.verbose_fp,
	       "-----------------------------------------------------------------------------\n\n");

      db_creation_time = (time_t) session.bkuphdr->db_creation;
      (void) ctime_r (&db_creation_time, time_val);
      fprintf (session.verbose_fp,
	       "- HA apply info: %s %lld\n  DB Creation Time: %s  backuptime_LSA: %lld|%d\n\n",
	       db_name, (long long int) session.bkuphdr->db_creation,
	       time_val,
	       (long long int) session.bkuphdr->backuptime_lsa.pageid,
	       session.bkuphdr->backuptime_lsa.offset);

      backup_end_time = (time_t) session.bkuphdr->end_time;
      (void) ctime_r (&backup_end_time, time_val);
      fprintf (session.verbose_fp, "- backup end time: %s\n", time_val);
      fprintf (session.verbose_fp, "[ Database(%s) Full Backup end ]\n",
	       db_name);
    }

  error = bk_finish_backup_session (&session);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  return NO_ERROR;

error_exit:

  bk_abort_backup_client (&session, does_unformat_bk);

  return ER_FAILED;
}

static void
bk_verbose_backup_info (BK_BACKUP_SESSION * session, char *db_name,
			int num_threads)
{
  time_t backup_start_time;
  char time_val[CTIME_MAX];

  if (session->verbose_fp == NULL)
    {
      return;
    }

  fprintf (session->verbose_fp, "[ Database(%s) Backup start ]\n\n", db_name);

  fprintf (session->verbose_fp, "- num-threads: %d\n\n", num_threads);

  if (session->bkuphdr->zip_method == BK_ZIP_NONE_METHOD)
    {
      fprintf (session->verbose_fp, "- compression method: %s\n\n",
	       bk_get_zip_method_string (session->bkuphdr->zip_method));
    }
  else
    {
      fprintf (session->verbose_fp,
	       "- compression method: %d (%s), compression level: %d (%s)\n\n",
	       session->bkuphdr->zip_method,
	       bk_get_zip_method_string (session->bkuphdr->zip_method),
	       session->bkuphdr->zip_level,
	       bk_get_zip_level_string (session->bkuphdr->zip_level));
    }

  if (session->sleep_msecs > 0)
    {
      fprintf (session->verbose_fp,
	       "- sleep %d millisecond per 1M read.\n\n",
	       session->sleep_msecs);
    }

  backup_start_time = time (NULL);
  (void) ctime_r (&backup_start_time, time_val);
  fprintf (session->verbose_fp, "- backup start time: %s\n", time_val);
  fprintf (session->verbose_fp, "- number of permanent volumes: %d\n\n",
	   session->num_perm_vols);

  assert (LSA_ISNULL (&(session->bkuphdr->backuptime_lsa)));	/* not yet acquired */
  assert (session->bkuphdr->end_time == -1);	/* not yet acquired */

  fprintf (session->verbose_fp, "- backup progress status\n\n");
  fprintf (session->verbose_fp,
	   "-----------------------------------------------------------------------------\n");
  fprintf (session->verbose_fp,
	   " volume name                  | # of pages | backup progress status    | done \n");
  fprintf (session->verbose_fp,
	   "-----------------------------------------------------------------------------\n");
}

/*
 * bk_create_backup_volume () - CREATE A BACKUP VOLUME (INSURE ENOUGH SPACE EXISTS)
 *   return: volume descriptor identifier on success, NULL_VOLDES on failure
 *   db_fullname(in): Name of the database where the volume belongs
 *   vlabel(in): Volume label
 *   volid(in): Volume identifier
 *   dolock(in): Lock the volume from other Unix processes
 *   dosync(in): synchronize the writes on the volume ?
 *   atleast_npages(in): minimum number of pages required to be free
 *
 * Note: Tests to insure that there is at least the minimum requred amount of
 *       space on the given file system are.  Then calls fileio_create to create
 *       the volume (or file) without initializing it. This is needed for tape
 *       backups since they are not initialized at all plus saves time w/out
 *       formatting.
 *       Note: Space checking does not apply to devices, only files.
 */
static int
bk_create_backup_volume (const char *db_name, const char *vol_label_p)
{
  struct stat stbuf;
  int num_free;
  int atleast_npages = BK_BACKUP_MINIMUM_NUM_PAGES_FULL_LEVEL;

  if (stat (vol_label_p, &stbuf) != -1)
    {
      /* In WINDOWS platform, FIFO is not supported, until now.
         FIFO must be existent before backup operation is executed. */
      if (S_ISFIFO (stbuf.st_mode))
	{
	  int vdes;
	  struct timeval to = { 0, 100000 };

	  while (true)
	    {
	      vdes = fileio_open (vol_label_p, O_WRONLY | O_NONBLOCK, 0200);
	      if (vdes != NULL_VOLDES)
		{
		  break;
		}

	      if (errno == ENXIO)
		{
		  /* sleep for 100 milli-seconds : consider cs & sa mode */
		  select (0, NULL, NULL, NULL, &to);
		  continue;
		}
	      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
				   ER_IO_FORMAT_FAIL, 3, vol_label_p, -1, -1);
	      return NULL_VOLDES;
	    }
	  return vdes;
	}
      /* If there is not enough space in file-system, then do not bother
         opening backup volume */
      if (atleast_npages > 0 && S_ISREG (stbuf.st_mode))
	{
	  num_free = fileio_get_number_of_partition_free_pages (vol_label_p,
								IO_PAGESIZE);
	  if (num_free < atleast_npages)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_IO_FORMAT_OUT_OF_SPACE, 5, vol_label_p,
		      atleast_npages, ((IO_PAGESIZE / 1024) * atleast_npages),
		      num_free, ((IO_PAGESIZE / 1024) * num_free));
	      return NULL_VOLDES;
	    }
	}
    }

  return (fileio_create (NULL, db_name, vol_label_p, -1, false, false));
}

/*
 * bk_write_backup_header () - Immediately write the backup header to the
 *                             destination
 *   return:
 *   session(in/out): The session array
 *
 * Note: Note that unlike io_backup_write, we do not buffer, instead we
 *       write directly to the output destination the number of bytes
 *       in a bkuphdr.  This insures that headers all have the same
 *       physical block size so we can read them properly.  The main
 *       purpose of this routine is to write the headers in a tape
 *       friendly blocking factor such that we can be sure we can read
 *       them back in without knowing how the tape was written in the
 *       first place.
 */
static int
bk_write_backup_header (BK_BACKUP_SESSION * session_p)
{
  char *buffer_p;
  int count, nbytes;

  /* Write immediately to the backup.  We do not use fileio_write for the same
     reason io_backup_flush does not. */
  count = BK_BACKUP_HEADER_IO_SIZE;
  buffer_p = (char *) session_p->bkuphdr;
  do
    {
      nbytes = write (session_p->bkup.vdes, buffer_p, count);
      if (nbytes == -1)
	{
	  if (errno == EINTR || errno == EAGAIN)
	    {
	      continue;
	    }

	  if (errno == ENOSPC)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_IO_WRITE_OUT_OF_SPACE, 2,
		      CEIL_PTVDIV (session_p->bkup.voltotalio,
				   IO_PAGESIZE),
		      fileio_get_volume_label_by_fd (session_p->bkup.vdes,
						     PEEK));
	    }
	  else
	    {
	      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
				   ER_IO_WRITE, 2,
				   CEIL_PTVDIV (session_p->bkup.voltotalio,
						IO_PAGESIZE),
				   session_p->bkup.vlabel);
	    }
	  return ER_FAILED;
	}
      else
	{
	  count -= nbytes;
	  buffer_p += nbytes;
	}
    }
  while (count > 0);
  session_p->bkup.voltotalio += BK_BACKUP_HEADER_IO_SIZE;
  return NO_ERROR;
}

/*
 * bk_write_backup_end_time_to_header () - Write the end time of backup
 *                                             to backup volume header
 *   return: error status
 *   session(in): backup session
 */
static int
bk_write_backup_end_time_to_header (BK_BACKUP_SESSION * session_p)
{
  const char *first_bkvol_name;
  int vdes, nbytes;

  assert (!LSA_ISNULL (&(session_p->bkuphdr->backuptime_lsa)));
  assert (session_p->bkuphdr->end_time > 0);

  first_bkvol_name = session_p->bkup.name;

  if (first_bkvol_name == NULL)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_IO_MOUNT_FAIL, 1,
			   "(backup volume name is null)");
      return ER_IO_MOUNT_FAIL;
    }

  if (session_p->bkuphdr->make_slave == false
      && strncmp (first_bkvol_name, session_p->bkup.vlabel, PATH_MAX) == 0)
    {
      lseek (session_p->bkup.vdes, 0, SEEK_SET);
      bk_write_backup_header (session_p);
    }
  else
    {
      vdes = fileio_open (first_bkvol_name, O_RDWR, 0);
      if (vdes == NULL_VOLDES)
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_IO_MOUNT_FAIL, 1, first_bkvol_name);
	  return ER_IO_MOUNT_FAIL;
	}

      lseek (vdes, offsetof (BK_BACKUP_HEADER, backuptime_lsa), SEEK_SET);
      nbytes =
	write (vdes, (char *) &(session_p->bkuphdr->backuptime_lsa),
	       sizeof (LOG_LSA));
      if (nbytes != sizeof (LOG_LSA))
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_IO_WRITE, 2, 1, first_bkvol_name);
	  fileio_close (vdes);
	  return ER_IO_WRITE;
	}

      lseek (vdes, offsetof (BK_BACKUP_HEADER, end_time), SEEK_SET);
      nbytes =
	write (vdes, (char *) &(session_p->bkuphdr->end_time),
	       sizeof (INT64));
      if (nbytes != sizeof (INT64))
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_IO_WRITE, 2, 1, first_bkvol_name);
	  fileio_close (vdes);
	  return ER_IO_WRITE;
	}

      fileio_close (vdes);
    }

  return NO_ERROR;
}

/*
 * bk_finish_backup () - Finish the backup session successfully
 *   return: session or NULL
 *   session(in/out): The session array
 */
static int
bk_finish_backup_session (BK_BACKUP_SESSION * session_p)
{
  int nbytes;
  BK_BACKUP_PAGE end_page;

  /*
   * Indicate end of backup and flush any buffered data.
   * Note that only the end of backup marker is written,
   * so callers of io_restore_read must check for the appropriate
   * end of backup condition.
   */

  if (session_p->bkuphdr->make_slave == false)
    {
      end_page.iopageid = BK_BACKUP_END_PAGE_ID;
      nbytes = offsetof (BK_BACKUP_PAGE, iopage);
      memset (session_p->bkup.buffer, '\0', session_p->bkup.iosize);
      memcpy (session_p->bkup.buffer, &end_page, nbytes);

      if (bk_write_backup (session_p, session_p->bkup.iosize,
			   session_p->bkup.iosize) != NO_ERROR)
	{
	  return ER_FAILED;
	}
    }

  /*
   * Now, make sure that all the information is physically written to
   * the backup device. That is, make sure that nobody (e.g., backup
   * device controller or OS) is caching data.
   */
  if (bk_write_backup_end_time_to_header (session_p) != NO_ERROR)
    {
      return ER_FAILED;
    }

  fileio_close (session_p->bkup.vdes);

  return NO_ERROR;
}

static int
bk_decompress (BK_BACKUP_SESSION * session_p, int unzip_nbytes,
	       ssize_t * nbytes_out)
{
  int rv, error = NO_ERROR;
  BK_QUEUE io_queue, *queue_p;
  BK_NODE *node;
  char *buffer_p;
  lzo_uint unzip_len;

  *nbytes_out = 0;
  queue_p = &io_queue;
  queue_p->size = 0;
  queue_p->free_list = NULL;
  queue_p->head = NULL;
  queue_p->tail = NULL;
  buffer_p = session_p->bkup.buffer;

  assert (session_p->bkuphdr->zip_method == BK_ZIP_LZO1X_METHOD);

  node = bk_allocate_node (queue_p, session_p->bkuphdr);
  if (node == NULL)
    {
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  assert (node->zip_page != NULL);

  memcpy (node->zip_page, buffer_p, sizeof (lzo_uint));
  buffer_p += sizeof (lzo_uint);

  /* sanity check of the size values */
  if (node->zip_page->buf_len > (size_t) unzip_nbytes
      || node->zip_page->buf_len == 0)
    {
      error = ER_IO_LZO_COMPRESS_FAIL;	/* may be compress fail */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 4,
	      session_p->bkuphdr->zip_method,
	      bk_get_zip_method_string (session_p->bkuphdr->zip_method),
	      session_p->bkuphdr->zip_level,
	      bk_get_zip_level_string (session_p->bkuphdr->zip_level));
      goto end;
    }
  else if (node->zip_page->buf_len < (size_t) unzip_nbytes)
    {
      /* read compressed block data */

      memcpy (node->zip_page->buf, buffer_p, node->zip_page->buf_len);
      buffer_p += node->zip_page->buf_len;

      /* decompress - use safe decompressor as data might be corrupted
         during a file transfer */
      unzip_len = unzip_nbytes;
      rv = lzo1x_decompress_safe (node->zip_page->buf,
				  node->zip_page->buf_len,
				  (lzo_bytep) session_p->bkup.buffer,
				  &unzip_len, NULL);
      if (rv != LZO_E_OK || unzip_len != (size_t) unzip_nbytes)
	{
	  error = ER_IO_LZO_DECOMPRESS_FAIL;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	  goto end;
	}

      session_p->bkup.ptr = session_p->bkup.buffer;
      *nbytes_out = unzip_len;
    }
  else
    {
      /* no compressed block */
      session_p->bkup.ptr = buffer_p;
      *nbytes_out = node->zip_page->buf_len;
    }

end:

  if (node)
    {
      (void) bk_free_node (queue_p, node);
    }

  return error;
}

/*
 * bk_write_backup () - Write the number of indicated bytes from the server
 *                      to to the backup destination
 *   return:
 *   session(in/out): The session array
 *   towrite_nbytes(in): Number of bytes that must be written
 */
int
bk_write_backup (BK_BACKUP_SESSION * session_p, ssize_t to_write_nbytes,
		 int unzip_nbytes)
{
  char *buffer_p;
  ssize_t nbytes;
  int write_npages;
  BK_BACKUP_PAGE *p;

  if (session_p->bkuphdr->make_slave == true)
    {
      if (session_p->bkuphdr->zip_method == BK_ZIP_LZO1X_METHOD)
	{
	  if (bk_decompress (session_p, unzip_nbytes,
			     &to_write_nbytes) != NO_ERROR)
	    {
	      return ER_FAILED;
	    }

	  buffer_p = session_p->bkup.ptr;
	}
      else
	{
	  buffer_p = session_p->bkup.buffer;
	}

      p = (BK_BACKUP_PAGE *) buffer_p;
      buffer_p = (char *) &(p->iopage);
      to_write_nbytes -= BK_BACKUP_PAGE_OVERHEAD;
    }
  else
    {
      buffer_p = session_p->bkup.buffer;
    }

  do
    {
      nbytes = write (session_p->bkup.vdes, buffer_p, to_write_nbytes);
      if (nbytes <= 0)
	{
	  switch (errno)
	    {
	      /* equiv to try again */
	    case EINTR:
	    case EAGAIN:
	      continue;
	    default:
	      er_set_with_oserror (ER_ERROR_SEVERITY,
				   ARG_FILE_LINE,
				   ER_IO_WRITE, 2,
				   CEIL_PTVDIV
				   (session_p->bkup.voltotalio,
				    IO_PAGESIZE), session_p->bkup.vlabel);
	      return ER_FAILED;
	    }
	}
      else
	{
	  session_p->bkup.voltotalio += nbytes;
	  to_write_nbytes -= nbytes;
	  buffer_p += nbytes;
	}
    }
  while (to_write_nbytes > 0);

  write_npages = (int) CEIL_PTVDIV (session_p->bkup.voltotalio,
				    session_p->bkuphdr->bkpagesize);
  if (session_p->verbose_fp && session_p->dbfile.bk_npages >= 25
      && write_npages >= session_p->dbfile.check_npages)
    {
      fprintf (session_p->verbose_fp, "#");
      session_p->dbfile.check_ratio++;
      session_p->dbfile.check_npages =
	(int) (((float) session_p->dbfile.bk_npages / 25.0) *
	       session_p->dbfile.check_ratio);
    }

  return NO_ERROR;
}

void
bk_start_vol_in_backup (BK_BACKUP_SESSION * session, int vol_type)
{
  BK_BACKUP_PAGE *page;
  BK_VOL_HEADER_IN_BACKUP *header;
  int vol_npages;
  const char *vol_name;
  char path[PATH_MAX];
  char fullpath[PATH_MAX];

  page = (BK_BACKUP_PAGE *) session->bkup.buffer;
  header = (BK_VOL_HEADER_IN_BACKUP *) (&page->iopage);

  session->dbfile.volid = header->volid;
  session->dbfile.nbytes = header->nbytes;

  vol_npages = (int) CEIL_PTVDIV (session->dbfile.nbytes, IO_PAGESIZE);
  session->dbfile.bk_npages = (int) CEIL_PTVDIV (session->dbfile.nbytes,
						 session->bkuphdr->
						 bkpagesize);
  session->dbfile.check_ratio = 1;
  session->dbfile.check_npages =
    (int) (((float) session->dbfile.bk_npages / 25.0) *
	   session->dbfile.check_ratio);

  vol_name = fileio_get_base_file_name (header->vlabel);
  if (session->verbose_fp)
    {
      fprintf (session->verbose_fp, " %-28s | %10d | ", vol_name, vol_npages);
    }

  if (session->bkuphdr->make_slave == true)
    {
      if (session->bkup.vdes != NULL_VOLDES)
	{
	  fileio_close (session->bkup.vdes);
	}

      if (vol_type == 0)
	{
	  if (envvar_db_dir (path, PATH_MAX,
			     session->bkuphdr->db_name) == NULL)
	    {
	      assert (false);
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_BO_UNKNOWN_DATABASE, 1, session->bkuphdr->db_name);
	      return;
	    }
	}
      else
	{
	  if (envvar_db_log_dir (path, PATH_MAX,
				 session->bkuphdr->db_name) == NULL)
	    {
	      assert (false);
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_BO_UNKNOWN_DATABASE, 1, session->bkuphdr->db_name);
	      return;
	    }
	}

      if (rye_mkdir (path, 0755) == false)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_BO_DIRECTORY_DOESNOT_EXIST, 1, path);
	  return;
	}

      COMPOSE_FULL_NAME (fullpath, sizeof (fullpath), path, vol_name);

      session->bkup.vdes = bk_create_backup_volume (session->bkuphdr->db_name,
						    fullpath);
      if (session->bkup.vdes == NULL_VOLDES)
	{
	  assert (0);
	}
    }
}

void
bk_end_vol_in_backup (BK_BACKUP_SESSION * session)
{
  if (session->verbose_fp)
    {
      if (session->dbfile.bk_npages < 25)
	{
	  fprintf (session->verbose_fp, "######################### | done\n");
	}
      else
	{
	  while (session->dbfile.check_ratio <= 25)
	    {
	      fprintf (session->verbose_fp, "#");
	      session->dbfile.check_ratio++;
	    }
	  fprintf (session->verbose_fp, " | done\n");
	}
    }
}
