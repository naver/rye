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
 * backup.c - backup module (common)
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
#include "log_manager.h"
#include "file_io.h"
#include "memory_alloc.h"
#include "error_manager.h"
#include "system_parameter.h"
#include "databases_file.h"
#include "message_catalog.h"
#include "environment_variable.h"
#include "release_string.h"
#include "backup.h"
#include "backup_sr.h"

#define BK_SUFFIX_BACKUP         "_bk"

#if defined(SERVER_MODE)
static int bk_os_sysconf (void);
#endif
static void bk_remove_all_backup (THREAD_ENTRY * thread_p,
				  BK_BACKUP_SESSION * session_p);
#if defined(SERVER_MODE)
/*
 * bk_os_sysconf () -
 *   return:
 */
static int
bk_os_sysconf (void)
{
  long nprocs = -1;

#if defined(_SC_NPROCESSORS_ONLN)
  nprocs = sysconf (_SC_NPROCESSORS_ONLN);
#elif defined(_SC_NPROC_ONLN)
  nprocs = sysconf (_SC_NPROC_ONLN);
#elif defined(_SC_CRAY_NCPU)
  nprocs = sysconf (_SC_CRAY_NCPU);
#else
  ;				/* give up */
#endif
  return (nprocs > 1) ? (int) nprocs : 1;
}
#endif

static int
bk_initialize_backup_thread (BK_BACKUP_SESSION * session_p,
			     UNUSED_ARG int num_threads)
{
  BK_THREAD_INFO *thread_info_p;
  BK_QUEUE *queue_p;
#if defined(SERVER_MODE)
  int num_cpus;
  int rv;
#endif /* SERVER_MODE */

  thread_info_p = &session_p->read_thread_info;
  queue_p = &thread_info_p->io_queue;

#if defined(SERVER_MODE)
  rv = pthread_mutex_init (&thread_info_p->mtx, NULL);
  if (rv != 0)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_CSS_PTHREAD_MUTEX_INIT, 0);
      return ER_CSS_PTHREAD_MUTEX_INIT;
    }

  rv = pthread_cond_init (&thread_info_p->rcv, NULL);
  if (rv != 0)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_CSS_PTHREAD_COND_INIT, 0);
      return ER_CSS_PTHREAD_COND_INIT;
    }

  rv = pthread_cond_init (&thread_info_p->wcv, NULL);
  if (rv != 0)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_CSS_PTHREAD_COND_INIT, 0);
      return ER_CSS_PTHREAD_COND_INIT;
    }

  /* get the number of CPUs */
  num_cpus = bk_os_sysconf ();
  /* check for the upper bound of threads */
  if (num_threads == BK_BACKUP_NUM_THREADS_AUTO)
    {
      thread_info_p->num_threads = num_cpus;
    }
  else
    {
      thread_info_p->num_threads = MIN (num_threads, num_cpus * 2);
    }
  thread_info_p->num_threads =
    MIN (thread_info_p->num_threads, NUM_NORMAL_TRANS);
#else /* SERVER_MODE */
  thread_info_p->num_threads = 1;
#endif /* SERVER_MODE */

#if 1				/* TODO - */
  /* at here, disable multi-thread usage for fast Vol copy */
  thread_info_p->num_threads = 1;
#endif

  queue_p->size = 0;
  queue_p->head = NULL;
  queue_p->tail = NULL;
  queue_p->free_list = NULL;

  thread_info_p->initialized = true;

  return NO_ERROR;
}

/*
 * bk_init_backup_buffer () - Initialize the backup session structure with the given
 *                     information
 *   return: error code
 */
int
bk_init_backup_buffer (BK_BACKUP_SESSION * session,
		       const char *db_name, const char *backup_path,
		       int do_compress)
{
  int vol_fd;
  struct stat stbuf;
  int buf_size;

  /*
   * First assume that backup device is a regular file or a raw device.
   * Adjustments are made at a later point, if the backup_destination is
   * a directory.
   */
  strncpy (session->bkup.name, backup_path, PATH_MAX);
  session->bkup.name[PATH_MAX - 1] = '\0';
  session->bkup.vlabel = session->bkup.name;
  session->bkup.vdes = NULL_VOLDES;
  session->bkup.buffer = NULL;

  /* Now find out the type of backup_destination and the best page I/O for
     the backup. The accepted type is a directory only. */
  while (stat (backup_path, &stbuf) == -1)
    {
      /*
       * If the backup_destination does not exist, try to create it to make
       * sure that we can write at this backup destination.
       */
      vol_fd = fileio_open (backup_path, FILEIO_DISK_FORMAT_MODE,
			    FILEIO_DISK_PROTECTION_MODE);
      if (vol_fd == NULL_VOLDES)
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_IO_MOUNT_FAIL, 1, backup_path);
	  return ER_FAILED;
	}
      fileio_close (vol_fd);
      continue;
    }

  if (S_ISDIR (stbuf.st_mode))
    {
      /*
       * This is a DIRECTORY where the backup is going to be sent.
       * The name of the backup file in this directory is labeled as
       * databasename.bkLvNNN (Unix). In this case, we may destroy any previous
       * backup in this directory.
       */
      bk_make_backup_name (session->bkup.name, db_name,
			   backup_path, BK_INITIAL_BACKUP_UNITS);
    }

  buf_size = stbuf.st_blksize;
  /* User may override the default size by specifying a multiple of the
     natural block size for the device. */
  session->bkup.iosize = buf_size *
    prm_get_integer_value (PRM_ID_IO_BACKUP_NBUFFERS);

  /*
   * Initialize backup device related information.
   *
   * Make sure it is large enough to hold various headers and pages.
   * Beware that upon restore, both the backup buffer size and the
   * database io pagesize may be different.
   */

  session->bkup.buffer = (char *) malloc (session->bkup.iosize);
  if (session->bkup.buffer == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      session->bkup.iosize);

      goto error;
    }

  session->bkuphdr = (BK_BACKUP_HEADER *) malloc (BK_BACKUP_HEADER_IO_SIZE);
  if (session->bkuphdr == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      BK_BACKUP_HEADER_IO_SIZE);

      goto error;
    }

  session->bkup.ptr = session->bkup.buffer;
  session->bkup.count = 0;
  session->bkup.voltotalio = 0;
  session->bkup.alltotalio = 0;

  session->bkuphdr->bkup_iosize = session->bkup.iosize;
  session->bkuphdr->bk_hdr_version = BK_BACKUP_HEADER_VERSION;
  session->bkuphdr->start_time = 0;
  LSA_SET_NULL (&(session->bkuphdr->backuptime_lsa));
  session->bkuphdr->end_time = -1;
  if (do_compress)
    {
      session->bkuphdr->zip_method = BK_ZIP_LZO1X_METHOD;
      session->bkuphdr->zip_level = BK_ZIP_LZO1X_DEFAULT_LEVEL;
    }
  else
    {
      session->bkuphdr->zip_method = BK_ZIP_NONE_METHOD;
      session->bkuphdr->zip_level = BK_ZIP_NONE_LEVEL;
    }

  return NO_ERROR;

error:
  if (session->bkup.buffer != NULL)
    {
      free_and_init (session->bkup.buffer);
    }
  if (session->bkuphdr != NULL)
    {
      free_and_init (session->bkuphdr);
    }

  return ER_FAILED;
}

/*
 * bk_init_backup_vol_buffer () - Initialize the backup session structure with the given
 *                     information
 *   return: session or NULL
 *   db_fullname(in):  Name of the database to backup
 *   backup_destination(in): Name of backup device (file or directory)
 *   session(out): The session array
 *   verbose_file_path(in): verbose mode file path
 *   num_threads(in): number of threads
 *   sleep_msecs(in): sleep interval in msecs
 */
int
bk_init_backup_vol_buffer (BK_BACKUP_SESSION * session_p,
			   int num_threads, int sleep_msecs)
{
  int size;
  int io_page_size;

  /*
   * Initialize backup device related information.
   *
   * Make sure it is large enough to hold various headers and pages.
   * Beware that upon restore, both the backup buffer size and the
   * database io pagesize may be different.
   */
  io_page_size = IO_PAGESIZE;
  io_page_size *= FILEIO_FULL_LEVEL_EXP;

  size = MAX (io_page_size + BK_BACKUP_PAGE_OVERHEAD,
	      BK_VOL_HEADER_IN_BACKUP_PAGE_SIZE);

  session_p->dbfile.area = (BK_BACKUP_PAGE *) malloc (size);
  if (session_p->dbfile.area == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      size);

      goto error;
    }

  session_p->dbfile.vlabel = NULL;
  session_p->dbfile.volid = NULL_VOLID;
  session_p->dbfile.vdes = NULL_VOLDES;
  session_p->dbfile.nbytes = -1;
  BK_SET_BACKUP_PAGE_ID (session_p->dbfile.area, NULL_PAGEID, io_page_size);

  if (bk_initialize_backup_thread (session_p, num_threads) != NO_ERROR)
    {
      goto error;
    }

  session_p->sleep_msecs = sleep_msecs;

  return NO_ERROR;

error:
  if (session_p->dbfile.area != NULL)
    {
      free_and_init (session_p->dbfile.area);
    }

  return ER_FAILED;
}

/*
 * bk_make_backup_name () - Build the name of volumes
 *   return: void
 *   backup_name(out):
 *   nopath_volname(in):
 *   backup_path(in):
 *   unit_num(in):
 *
 * Note: The caller must have enough space to store the name of the volume
 *       that is constructed(sprintf). It is recommended to have at least
 *       DB_MAX_PATH_LENGTH length.
 */
void
bk_make_backup_name (char *backup_name_p, const char *no_path_vol_name_p,
		     const char *backup_path_p, int unit_num)
{
  int n;

  n = snprintf (backup_name_p, PATH_MAX, "%s%c%s%sv%03d",
		backup_path_p, PATH_SEPARATOR, no_path_vol_name_p,
		BK_SUFFIX_BACKUP, unit_num);
  if (n <= 0)
    {
      assert (false);
      ;				/* TODO - avoid compile error */
    }
}

/*
 * bk_get_backup_level_string () - return the string name of the backup level
 *   return: pointer to string containing name of level
 */
const char *
bk_get_backup_level_string (void)
{
  return ("FULL LEVEL");
}

/*
 * bk_get_zip_method_string () - return the string name of the compression method
 *   return: pointer to string containing name of zip_method
 *   zip_method(in): the compression method to convert
 */
const char *
bk_get_zip_method_string (BK_ZIP_METHOD zip_method)
{
  switch (zip_method)
    {
    case BK_ZIP_NONE_METHOD:
      return ("NONE");

    case BK_ZIP_LZO1X_METHOD:
      return ("LZO1X");

    default:
      return ("UNKNOWN");
    }
}

/*
 * bk_get_zip_level_string () - return the string name of the compression level
 *   return: pointer to string containing name of zip_level
 *   zip_level(in): the compression level to convert
 */
const char *
bk_get_zip_level_string (BK_ZIP_LEVEL zip_level)
{
  switch (zip_level)
    {
    case BK_ZIP_NONE_LEVEL:
      return ("NONE");

    case BK_ZIP_1_LEVEL:	/* case BK_ZIP_LZO1X_DEFAULT_LEVEL: */
      return ("ZIP LEVEL 1 - BEST SPEED");

    case BK_ZIP_9_LEVEL:	/* case BK_ZIP_LZO1X_999_LEVEL: */
      return ("ZIP LEVEL 9 - BEST REDUCTION");

    default:
      return ("UNKNOWN");
    }
}

/*
 * bk_allocate_node () -
 *   return:
 *   qp(in):
 *   backup_hdr(in):
 */
BK_NODE *
bk_allocate_node (BK_QUEUE * queue_p, BK_BACKUP_HEADER * backup_header_p)
{
  BK_NODE *node_p;
  int size;
  size_t zip_page_size, wrkmem_size;

  if (queue_p->free_list)	/* re-use already alloced nodes */
    {
      node_p = queue_p->free_list;
      queue_p->free_list = node_p->next;	/* cut-off */
      return node_p;
    }

  /* at here, need to alloc */
  node_p = (BK_NODE *) malloc (sizeof (BK_NODE));
  if (node_p == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      sizeof (BK_NODE));
      goto exit_on_error;
    }

  node_p->area = NULL;
  node_p->zip_page = NULL;
  node_p->wrkmem = NULL;
  size = backup_header_p->bkpagesize + BK_BACKUP_PAGE_OVERHEAD;
  node_p->area = (BK_BACKUP_PAGE *) malloc (size);
  if (node_p == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      size);
      goto exit_on_error;
    }

  switch (backup_header_p->zip_method)
    {
    case BK_ZIP_LZO1X_METHOD:
      zip_page_size = sizeof (lzo_uint) + size + size / 16 + 64 + 3;
      node_p->zip_page = (BK_ZIP_PAGE *) malloc (zip_page_size);
      if (node_p->zip_page == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, zip_page_size);
	  goto exit_on_error;
	}

      if (backup_header_p->zip_level == BK_ZIP_LZO1X_999_LEVEL)
	{
	  /* best reduction */
	  wrkmem_size = LZO1X_999_MEM_COMPRESS;
	}
      else
	{
	  /* best speed */
	  wrkmem_size = LZO1X_1_MEM_COMPRESS;
	}

      node_p->wrkmem = (lzo_bytep) malloc (wrkmem_size);
      if (node_p->wrkmem == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, wrkmem_size);
	  goto exit_on_error;
	}
      break;
    default:
      break;
    }

exit_on_end:

  return node_p;

exit_on_error:

  if (node_p)
    {
      if (node_p->wrkmem)
	{
	  free_and_init (node_p->wrkmem);
	}

      if (node_p->zip_page)
	{
	  free_and_init (node_p->zip_page);
	}

      if (node_p->area)
	{
	  free_and_init (node_p->area);
	}

      free_and_init (node_p);
    }

  node_p = NULL;
  goto exit_on_end;
}

/*
 * bk_free_node () -
 *   return:
 *   qp(in):
 *   node(in):
 */
BK_NODE *
bk_free_node (BK_QUEUE * queue_p, BK_NODE * node_p)
{
  if (node_p)
    {
      node_p->prev = node_p->next = NULL;
      node_p->next = queue_p->free_list;	/* add to free list */
      queue_p->free_list = node_p;
    }

  return node_p;
}

/*
 * bk_append_queue () -
 *   return:
 *   qp(in):
 *   node(in):
 */
BK_NODE *
bk_append_queue (BK_QUEUE * queue_p, BK_NODE * node_p)
{
  if (node_p)
    {
      node_p->prev = node_p->next = NULL;
      node_p->next = queue_p->tail;	/* add to tail */
      if (queue_p->tail)
	{
	  queue_p->tail->prev = node_p;
	}
      queue_p->tail = node_p;
      if (queue_p->head == NULL)
	{
	  /* the first */
	  queue_p->head = node_p;
	}

      queue_p->size++;
    }

  return node_p;
}

/*
 * bk_delete_queue_head () -
 *   return:
 *   qp(in):
 */
BK_NODE *
bk_delete_queue_head (BK_QUEUE * queue_p)
{
  BK_NODE *node;

  node = queue_p->head;
  if (node)
    {
      if (node == queue_p->tail)	/* only one node */
	{
	  queue_p->tail = NULL;
	}
      else
	{
	  node->prev->next = NULL;	/* cut-off */
	}

      queue_p->head = node->prev;
      queue_p->size--;
    }

  return node;
}

/*
 * bk_abort_backup_client () - The backup session is aborted
 *   return: void
 *   session(in/out):  The session array
 *   does_unformat_bk(in): set to TRUE to delete backup volumes we know about
 *
 * Note: The currently created backup file can be removed if desired. This
 *       routine is called in for normal cleanup as well as to handle
 *       exceptions.
 */
void
bk_abort_backup_client (BK_BACKUP_SESSION * session_p, bool does_unformat_bk)
{
  /* Remove the currently created backup */
  if (session_p->bkup.vdes != NULL_VOLDES)
    {
      fileio_dismount (NULL, session_p->bkup.vdes);
    }

  /* Destroy the current backup volumes */
  if (does_unformat_bk)
    {
      /* Remove current backup volume */
      if (fileio_is_volume_exist_and_file (session_p->bkup.vlabel))
	{
	  fileio_unformat (NULL, session_p->bkup.vlabel);
	}

      /* Remove backup volumes previous to this one */
      if (session_p->bkuphdr)
	{
	  bk_remove_all_backup (NULL, session_p);
	}
    }

  if (session_p->verbose_fp)
    {
      fclose (session_p->verbose_fp);
      session_p->verbose_fp = NULL;
    }

  /* Deallocate memory space */
  if (session_p->bkup.buffer != NULL)
    {
      free_and_init (session_p->bkup.buffer);
    }

  if (session_p->bkuphdr != NULL)
    {
      free_and_init (session_p->bkuphdr);
    }

  session_p->bkup.vdes = NULL_VOLDES;
  session_p->bkup.vlabel = NULL;
  session_p->bkup.iosize = -1;
  session_p->bkup.count = 0;
  session_p->bkup.voltotalio = 0;
  session_p->bkup.alltotalio = 0;
  session_p->bkup.buffer = session_p->bkup.ptr = NULL;
  session_p->bkuphdr = NULL;
}

/*
 * bk_abort_backup_server () - The backup session is aborted
 *   return: void
 *   session(in/out):  The session array
 *
 * Note: The currently created backup file can be removed if desired. This
 *       routine is called in for normal cleanup as well as to handle
 *       exceptions.
 */
void
bk_abort_backup_server (BK_BACKUP_SESSION * session_p)
{
  if (session_p->verbose_fp)
    {
      fclose (session_p->verbose_fp);
      session_p->verbose_fp = NULL;
    }

  if (session_p->dbfile.area != NULL)
    {
      free_and_init (session_p->dbfile.area);
    }

  if (session_p->bkuphdr != NULL)
    {
      free_and_init (session_p->bkuphdr);
    }

  session_p->dbfile.vdes = NULL_VOLDES;
  session_p->dbfile.volid = NULL_VOLID;
  session_p->dbfile.vlabel = NULL;
  session_p->dbfile.nbytes = -1;
  session_p->dbfile.area = NULL;
  session_p->bkuphdr = NULL;
}

/*
 * bk_remove_all_backup () - REMOVE ALL BACKUP VOLUMES
 *   return: void
 *
 * Note: Initialize the backup session structure with the given information.
 *       Remove backup. Cleanup backup. This routine assumes that the
 *       bkvinf cache has already been read in from the bkvinf file.
 */
static void
bk_remove_all_backup (THREAD_ENTRY * thread_p, BK_BACKUP_SESSION * session_p)
{
  const char *vol_name_p;

  vol_name_p = session_p->bkup.name;
  if (vol_name_p == NULL)
    {
      return;
    }

  if (fileio_is_volume_exist_and_file (vol_name_p))
    {
      fileio_unformat (thread_p, vol_name_p);
    }
}
