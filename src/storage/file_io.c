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
 * file_io.c - input/output module (at server)
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


/*
 * Message id in the set MSGCAT_SET_IO
 * in the message catalog MSGCAT_CATALOG_RYE (file rye.msg).
 */
#define MSGCAT_FILEIO_STARTS                        1
#define MSGCAT_FILEIO_BKUP_NEEDED                   2
#define MSGCAT_FILEIO_BKUP_HDR                      3
#define MSGCAT_FILEIO_BKUP_HDR_MAGICID              4
#define MSGCAT_FILEIO_BKUP_HDR_RELEASES             5
#define MSGCAT_FILEIO_BKUP_HDR_DBINFO               6
#define MSGCAT_FILEIO_BKUP_HDR_LEVEL                7
#define MSGCAT_FILEIO_BKUP_HDR_TIME                 8
#define MSGCAT_FILEIO_BKUP_FILE                     9
#define MSGCAT_FILEIO_REST_RELO_NEEDED              10
#define MSGCAT_FILEIO_REST_RELO_OPTIONS             11
#define MSGCAT_FILEIO_NEWLOCATION                   12
#define MSGCAT_FILEIO_INPUT_RANGE_ERROR             13
#define MSGCAT_FILEIO_INCORRECT_BKVOLUME            14
#define MSGCAT_FILEIO_LEVEL_MISMATCH                15
#define MSGCAT_FILEIO_MAGIC_MISMATCH                16
#define MSGCAT_FILEIO_DB_MISMATCH                   17
#define MSGCAT_FILEIO_UNIT_NUM_MISMATCH             18
#define MSGCAT_FILEIO_BACKUP_TIME_MISMATCH          19
#define MSGCAT_FILEIO_BACKUP_VINF_ERROR             20
#define MSGCAT_FILEIO_BACKUP_LABEL_INFO             21
#define MSGCAT_FILEIO_BKUP_HDR_LX_LSA               22
#define MSGCAT_FILEIO_RESTORE_FIND_REASON           23
#define MSGCAT_FILEIO_BKUP_FIND_REASON              24
#define MSGCAT_FILEIO_BKUP_PREV_BKVOL               25
#define MSGCAT_FILEIO_BKUP_NEXT_BKVOL               26
#define MSGCAT_FILEIO_BKUP_HDR_BKUP_PAGESIZE        27
#define MSGCAT_FILEIO_BKUP_HDR_ZIP_INFO             28
#define MSGCAT_FILEIO_BKUP_HDR_INC_ACTIVELOG        29

#ifdef L_cuserid
#define FILEIO_USER_NAME_SIZE L_cuserid
#else /* L_cuserid */
#define FILEIO_USER_NAME_SIZE 9
#endif /* L_cuserid */

#define GETPID()  getpid()

#define FILEIO_CHECK_AND_INITIALIZE_VOLUME_HEADER_CACHE(rtn) \
  do { \
    if (fileio_Vol_info_header.volinfo == NULL \
        && fileio_initialize_volume_info_cache () < 0) \
      return (rtn); \
  } while (0)

/* Definitions of some log archive and backup names */
#define FILEIO_SUFFIX_LOGACTIVE      "_lgat"
#define FILEIO_SUFFIX_LOGARCHIVE     "_lgar"
#define FILEIO_SUFFIX_TMP_LOGARCHIVE "_lgar_t"
#define FILEIO_SUFFIX_LOGINFO        "_lginf"
#define FILEIO_VOLEXT_PREFIX         "_x"
#define FILEIO_VOLTMP_PREFIX         "_t"
#define FILEIO_VOLINFO_SUFFIX        "_vinf"
#define FILEIO_MAX_SUFFIX_LENGTH     7

#define FILEIO_END_OF_FILE                (1)

#define FILEIO_MIN_FLUSH_PAGES_PER_SEC    10    /* minimum unit: #pages */
#define FILEIO_PAGE_FLUSH_RATE            0.01  /* minimum unit: 1% */

#define FILEIO_VOLINFO_INCREMENT        32

#if !defined(SERVER_MODE)
#define pthread_mutex_init(a, b)
#define pthread_mutex_destroy(a)
#define pthread_mutex_lock(a)	0
#define pthread_mutex_unlock(a)
#endif

typedef struct fileio_sys_volinfo FILEIO_SYSTEM_VOLUME_INFO;
typedef struct fileio_sys_volinfo_header FILEIO_SYSTEM_VOLUME_HEADER;
typedef struct fileio_volinfo FILEIO_VOLUME_INFO;
typedef struct fileio_volinfo_header FILEIO_VOLUME_HEADER;

/* Volume information structure for system volumes(volid < NULL_VOLID) */
struct fileio_sys_volinfo
{
  VOLID volid;
  int vdes;
  FILEIO_LOCKF_TYPE lockf_type;
  char vlabel[PATH_MAX];
  FILEIO_SYSTEM_VOLUME_INFO *next;
};

/* System volume informations are linked as a list */
struct fileio_sys_volinfo_header
{
#if defined(SERVER_MODE)
  pthread_mutex_t mutex;
#endif                          /* SERVER_MODE */
  int num_vols;
  FILEIO_SYSTEM_VOLUME_INFO anchor;
};

/* Volume information structure for perm/temp volumes */
struct fileio_volinfo
{
  VOLID volid;
  int vdes;
  FILEIO_LOCKF_TYPE lockf_type;
  char vlabel[PATH_MAX];
};

typedef union fileio_apply_function_arg
{
  int vol_id;
  int vdes;
  const char *vol_label;
} APPLY_ARG;

/* Perm/temp volume informations are stored on array.
 * Direct access by volid is possible */
struct fileio_volinfo_header
{
#if defined(SERVER_MODE)
  pthread_mutex_t mutex;
#endif                          /* SERVER_MODE */
  int max_perm_vols;            /* # of max. io_volinfo entries for perm. vol */
  int next_perm_volid;          /* # of used io_volinfo entries for perm. vol */
  int max_temp_vols;            /* # of max. io_volinfo entries for temp. vol */
  int next_temp_volid;          /* # of used io_volinfo entries for temp. vol */
  /* if volid of volume is equal to this value, */
  /* it is temp. volume                         */
  int num_volinfo_array;        /* # of io_volinfo entry chunks               */
  FILEIO_VOLUME_INFO **volinfo; /* array of pointer for io_volinfo chunks     */
};

typedef bool (*VOLINFO_APPLY_FN) (THREAD_ENTRY * thread_p, FILEIO_VOLUME_INFO * vol_info_p, APPLY_ARG * arg);
typedef bool (*SYS_VOLINFO_APPLY_FN) (THREAD_ENTRY * thread_p,
                                      FILEIO_SYSTEM_VOLUME_INFO * sys_vol_info_p, APPLY_ARG * arg);

static FILEIO_SYSTEM_VOLUME_HEADER fileio_Sys_vol_info_header = {
#if defined(SERVER_MODE)
  PTHREAD_MUTEX_INITIALIZER,
#endif /* SERVER_MODE */
  0,
  {
   NULL_VOLID, NULL_VOLDES, FILEIO_NOT_LOCKF, "",
   NULL}
};

static FILEIO_VOLUME_HEADER fileio_Vol_info_header = {
#if defined(SERVER_MODE)
  PTHREAD_MUTEX_INITIALIZER,
#endif /* SERVER_MODE */
  0, 0, 0, LOG_MAX_DBVOLID, 0, NULL
};

#if defined(SERVER_MODE)
/* Flush Control */
static int fileio_Flushed_page_count = 0;

static TOKEN_BUCKET fc_Token_bucket_s;
static TOKEN_BUCKET *fc_Token_bucket = NULL;
static int max_Flush_pages_per_sec = 0;
static FLUSH_STATS fc_Stats;
#endif

static int fileio_initialize_volume_info_cache (void);
static int fileio_max_permanent_volumes (int index, int num_permanent_volums);
static int fileio_min_temporary_volumes (int index, int num_temp_volums, int num_volinfo_array);
#if defined (ENABLE_UNUSED_FUNCTION)
static FILEIO_SYSTEM_VOLUME_INFO *fileio_traverse_system_volume (THREAD_ENTRY
                                                                 * thread_p,
                                                                 SYS_VOLINFO_APPLY_FN apply_function, APPLY_ARG * arg);
#endif
static FILEIO_VOLUME_INFO *fileio_traverse_permanent_volume (THREAD_ENTRY *
                                                             thread_p,
                                                             VOLINFO_APPLY_FN apply_function, APPLY_ARG * arg);
static FILEIO_VOLUME_INFO *fileio_traverse_temporary_volume (THREAD_ENTRY *
                                                             thread_p,
                                                             VOLINFO_APPLY_FN apply_function, APPLY_ARG * arg);
static bool fileio_dismount_volume (THREAD_ENTRY * thread_p, FILEIO_VOLUME_INFO * vol_info_p, APPLY_ARG * ignore_arg);
static bool fileio_is_volume_descriptor_equal (THREAD_ENTRY * thread_p,
                                               FILEIO_VOLUME_INFO * vol_info_p, APPLY_ARG * arg);
static FILEIO_SYSTEM_VOLUME_INFO *fileio_find_system_volume (THREAD_ENTRY *
                                                             thread_p,
                                                             SYS_VOLINFO_APPLY_FN apply_function, APPLY_ARG * arg);
static bool fileio_is_system_volume_descriptor_equal (THREAD_ENTRY * thread_p,
                                                      FILEIO_SYSTEM_VOLUME_INFO * sys_vol_info_p, APPLY_ARG * arg);
static bool fileio_is_system_volume_id_equal (THREAD_ENTRY * thread_p,
                                              FILEIO_SYSTEM_VOLUME_INFO * sys_vol_info_p, APPLY_ARG * arg);
static bool fileio_is_system_volume_label_equal (THREAD_ENTRY * thread_p,
                                                 FILEIO_SYSTEM_VOLUME_INFO * sys_vol_info_p, APPLY_ARG * arg);
#if defined (ENABLE_UNUSED_FUNCTION)
static bool fileio_synchronize_sys_volume (THREAD_ENTRY * thread_p,
                                           FILEIO_SYSTEM_VOLUME_INFO * vol_sys_info_p, APPLY_ARG * arg);
#endif
static bool fileio_synchronize_volume (THREAD_ENTRY * thread_p, FILEIO_VOLUME_INFO * vol_info_p, APPLY_ARG * arg);
#if !defined(CS_MODE)
static int fileio_cache (VOLID volid, const char *vlabel, int vdes, FILEIO_LOCKF_TYPE lockf_type);
#endif
static VOLID fileio_get_volume_id (int vdes);
static bool fileio_is_volume_label_equal (THREAD_ENTRY * thread_p, FILEIO_VOLUME_INFO * vol_info_p, APPLY_ARG * arg);
#if !defined(CS_MODE)
static int fileio_expand_permanent_volume_info (FILEIO_VOLUME_HEADER * header, int volid);
static int fileio_expand_temporary_volume_info (FILEIO_VOLUME_HEADER * header, int volid);
#endif
static int fileio_get_primitive_way_max (const char *path, long int *filename_max, long int *pathname_max);
static void fileio_compensate_flush (THREAD_ENTRY * thread_p, int fd, int npage);
#if defined(SERVER_MODE)
static int fileio_increase_flushed_page_count (int npages);
static int fileio_flush_control_get_token (THREAD_ENTRY * thread_p, int ntoken);
static int fileio_flush_control_get_desired_rate (TOKEN_BUCKET * tb);
#endif
#if defined (ENABLE_UNUSED_FUNCTION)
static int fileio_synchronize_bg_archive_volume (THREAD_ENTRY * thread_p);
#endif

#if defined(SERVER_MODE)
static int
fileio_increase_flushed_page_count (int npages)
{
  int flushed_page_count;

  flushed_page_count = ATOMIC_INC_32 (&fileio_Flushed_page_count, npages);

  return flushed_page_count;
}
#endif

static void
fileio_compensate_flush (UNUSED_ARG THREAD_ENTRY * thread_p, UNUSED_ARG int fd, UNUSED_ARG int npage)
{
#if !defined(SERVER_MODE)
  return;
#else
  int rv;
  bool need_sync;
  int flushed_page_count;

  assert (npage > 0);

  if (npage <= 0)
    {
      return;
    }

  if (thread_p == NULL)
    {
      thread_p = thread_get_thread_entry_info ();
    }

  rv = fileio_flush_control_get_token (thread_p, npage);
  if (rv != NO_ERROR)
    {
      return;
    }

  need_sync = false;

  flushed_page_count = fileio_increase_flushed_page_count (npage);
  if (flushed_page_count > (prm_get_bigint_value (PRM_ID_PB_SYNC_ON_FLUSH_SIZE) / IO_PAGESIZE))
    {
      need_sync = true;
      fileio_Flushed_page_count = 0;
    }

  if (need_sync)
    {
      fileio_synchronize_all (thread_p, false);
    }
#endif /* SERVER_MODE */
}


/*
 * fileio_flush_control_initialize():
 *
 *   returns:
 *
 * Note:
 */
int
fileio_flush_control_initialize (void)
{
#if !defined(SERVER_MODE)
  return NO_ERROR;
#else
  TOKEN_BUCKET *tb;
  int rv = NO_ERROR;

  assert (fc_Token_bucket == NULL);
  tb = &fc_Token_bucket_s;

  rv = pthread_mutex_init (&tb->token_mutex, NULL);
  if (rv != NO_ERROR)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_CSS_PTHREAD_MUTEX_INIT, 0);
      return ER_CSS_PTHREAD_MUTEX_INIT;
    }
  tb->tokens = 0;
  tb->token_consumed = 0;

  rv = pthread_cond_init (&tb->waiter_cond, NULL);
  if (rv != 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_CSS_PTHREAD_COND_INIT, 0);
      return ER_CSS_PTHREAD_COND_INIT;
    }

  fc_Stats.num_tokens = 0;
  fc_Stats.num_log_pages = 0;
  fc_Stats.num_pages = 0;

  max_Flush_pages_per_sec = (int) prm_get_bigint_value (PRM_ID_MAX_FLUSH_SIZE_PER_SECOND) / IO_PAGESIZE;

  fc_Token_bucket = tb;
  return rv;
#endif
}

/*
 * fileio_flush_control_finalize():
 *
 *   returns:
 *
 * Note:
 */
void
fileio_flush_control_finalize (void)
{
#if !defined(SERVER_MODE)
  return;
#else
  TOKEN_BUCKET *tb;

  assert (fc_Token_bucket != NULL);
  if (fc_Token_bucket == NULL)
    {
      return;
    }

  tb = fc_Token_bucket;
  fc_Token_bucket = NULL;

  (void) pthread_mutex_destroy (&tb->token_mutex);
  (void) pthread_cond_destroy (&tb->waiter_cond);
#endif
}

#if defined(SERVER_MODE)
/*
 * fileio_flush_control_get_token():
 *
 *   returns:
 *
 * Note:
 */
static int
fileio_flush_control_get_token (UNUSED_ARG THREAD_ENTRY * thread_p, UNUSED_ARG int ntoken)
{
#if !defined(SERVER_MODE)
  return NO_ERROR;
#else
  TOKEN_BUCKET *tb = fc_Token_bucket;
  int retry_count = 0;
  int nreq;
  bool log_cs_own = false;

  if (tb == NULL)
    {
      return NO_ERROR;
    }

  assert (ntoken > 0);

  if (LOG_CS_OWN (thread_p))
    {
      log_cs_own = true;
    }

  nreq = ntoken;
  while (nreq > 0 && retry_count < 10)
    {
      /* try to get a token from share tokens */
      pthread_mutex_lock (&tb->token_mutex);

      if (log_cs_own == true)
        {
          fc_Stats.num_log_pages += nreq;
        }
      else
        {
          fc_Stats.num_pages += nreq;
        }

      if (tb->tokens >= nreq)
        {
          tb->tokens -= nreq;
          tb->token_consumed += nreq;
          pthread_mutex_unlock (&tb->token_mutex);
          return NO_ERROR;
        }
      else if (tb->tokens > 0)
        {
          nreq -= tb->tokens;
          tb->token_consumed += tb->tokens;
          tb->tokens = 0;
        }

      assert (nreq > 0);

      if (log_cs_own == true)
        {
          pthread_mutex_unlock (&tb->token_mutex);
          return NO_ERROR;
        }

      /* Wait for signal */
      pthread_cond_wait (&tb->waiter_cond, &tb->token_mutex);

      pthread_mutex_unlock (&tb->token_mutex);
      retry_count++;
    }

  /* I am very very unlucky (unlikely to happen) */
  er_log_debug (ARG_FILE_LINE, "Failed to get token within %d trial (req=%d, remained=%d)", retry_count, ntoken, nreq);
  return NO_ERROR;
#endif
}
#endif

/*
 * fileio_flush_control_add_tokens():
 *
 *   returns:
 *
 * Note:
 */
int
fileio_flush_control_add_tokens (UNUSED_ARG THREAD_ENTRY * thread_p,
                                 UNUSED_ARG INT64 diff_usec, UNUSED_ARG int *token_gen, UNUSED_ARG int *token_consumed)
{
#if !defined(SERVER_MODE)
  return NO_ERROR;
#else
  TOKEN_BUCKET *tb = fc_Token_bucket;
  int gen_tokens;
  int rv = NO_ERROR;

  assert (token_gen != NULL);

  if (tb == NULL)
    {
      return NO_ERROR;
    }

  /* add remaining tokens to shared tokens */
  rv = pthread_mutex_lock (&tb->token_mutex);

  *token_consumed = tb->token_consumed;
  tb->token_consumed = 0;

  mnt_stats_counter (thread_p, MNT_STATS_ADAPTIVE_FLUSH_PAGES, fc_Stats.num_pages);
  mnt_stats_counter (thread_p, MNT_STATS_ADAPTIVE_FLUSH_LOG_PAGES, fc_Stats.num_log_pages);
  mnt_stats_counter (thread_p, MNT_STATS_ADAPTIVE_FLUSH_MAX_PAGES, fc_Stats.num_tokens);

  if (prm_get_bool_value (PRM_ID_ADAPTIVE_FLUSH_CONTROL) == true)
    {
      max_Flush_pages_per_sec += fileio_flush_control_get_desired_rate (tb);
      max_Flush_pages_per_sec = MAX (FILEIO_MIN_FLUSH_PAGES_PER_SEC, max_Flush_pages_per_sec);
    }
  else if (max_Flush_pages_per_sec != (int) prm_get_bigint_value (PRM_ID_MAX_FLUSH_SIZE_PER_SECOND) / IO_PAGESIZE)
    {
      max_Flush_pages_per_sec = (int) prm_get_bigint_value (PRM_ID_MAX_FLUSH_SIZE_PER_SECOND) / IO_PAGESIZE;
    }

  gen_tokens = (int) ((double) max_Flush_pages_per_sec / 1000000.0 * (double) diff_usec);
  gen_tokens = MAX (FILEIO_MIN_FLUSH_PAGES_PER_SEC, gen_tokens);

  *token_gen = gen_tokens;

  /* initialization statistics */
  fc_Stats.num_pages = 0;
  fc_Stats.num_log_pages = 0;
  fc_Stats.num_tokens = gen_tokens;

  tb->tokens = gen_tokens;

  /* signal to waiters */
  pthread_cond_broadcast (&tb->waiter_cond);
  pthread_mutex_unlock (&tb->token_mutex);
  return rv;

#endif
}

#if defined(SERVER_MODE)
/*
 * fileio_flush_control_get_desired_rate () -
 *
 */
static int
fileio_flush_control_get_desired_rate (TOKEN_BUCKET * tb)
{
  if (tb->tokens > 0)
    {
      return -(tb->tokens);
    }
  else
    {
      if (fc_Stats.num_log_pages == 0)
        {
          return (int) ((fc_Stats.num_tokens
                         + MAX (FILEIO_MIN_FLUSH_PAGES_PER_SEC, (fc_Stats.num_tokens * FILEIO_PAGE_FLUSH_RATE))));
        }
      else
        {
          return ((fc_Stats.num_pages + fc_Stats.num_log_pages) - fc_Stats.num_tokens);
        }
    }
}
#endif

/*
 * fileio_initialize_volume_info_cache () - Allocate/initialize
 *                                          volinfo_header.volinfo array
 *   return: 0 if success, or -1
 *
 * Note: This function is usually first called by
 *       fileio_find_volume_descriptor_with_label()(normal startup,
 *       backup/restore etc.) or fileio_mount()(database creation time)
 */
static int
fileio_initialize_volume_info_cache (void)
{
  int i, n;

  pthread_mutex_lock (&fileio_Vol_info_header.mutex);

  if (fileio_Vol_info_header.volinfo == NULL)
    {
      n = (VOLID_MAX - 1) / FILEIO_VOLINFO_INCREMENT + 1;
      fileio_Vol_info_header.volinfo = (FILEIO_VOLUME_INFO **) malloc (sizeof (FILEIO_VOLUME_INFO *) * n);
      if (fileio_Vol_info_header.volinfo == NULL)
        {
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (FILEIO_VOLUME_INFO *) * n);
          pthread_mutex_unlock (&fileio_Vol_info_header.mutex);
          return -1;
        }
      fileio_Vol_info_header.num_volinfo_array = n;

      for (i = 0; i < fileio_Vol_info_header.num_volinfo_array; i++)
        {
          fileio_Vol_info_header.volinfo[i] = NULL;
        }
    }

  pthread_mutex_unlock (&fileio_Vol_info_header.mutex);
  return 0;
}

/* TODO: check not use */
#if 0
/*
 * fileio_final_volinfo_cache () - Free volinfo_header.volinfo array
 *   return: void
 */
void
fileio_final_volinfo_cache (void)
{
  int i;
  if (fileio_Vol_info_header.volinfo != NULL)
    {
      for (i = 0; i < fileio_Vol_info_header.num_volinfo_array; i++)
        {
          free_and_init (fileio_Vol_info_header.volinfo[i]);
        }
      free_and_init (fileio_Vol_info_header.volinfo);
      fileio_Vol_info_header.num_volinfo_array = 0;
    }
}
#endif

#if !defined(CS_MODE)
static int
fileio_allocate_and_initialize_volume_info (FILEIO_VOLUME_HEADER * header_p, int idx)
{
  FILEIO_VOLUME_INFO *vol_info_p;
  int i;

  header_p->volinfo[idx] = NULL;
  vol_info_p = (FILEIO_VOLUME_INFO *) malloc (sizeof (FILEIO_VOLUME_INFO) * FILEIO_VOLINFO_INCREMENT);
  if (vol_info_p == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
              1, sizeof (FILEIO_VOLUME_INFO) * FILEIO_VOLINFO_INCREMENT);
      return ER_FAILED;
    }

  for (i = 0; i < FILEIO_VOLINFO_INCREMENT; i++)
    {
      vol_info_p[i].volid = NULL_VOLID;
      vol_info_p[i].vdes = NULL_VOLDES;
      vol_info_p[i].lockf_type = FILEIO_NOT_LOCKF;
      vol_info_p[i].vlabel[0] = '\0';
    }

  header_p->volinfo[idx] = vol_info_p;
  return NO_ERROR;
}

/*
 * fileio_expand_permanent_volume_info () - Expand io_volinfo chunks to cache
 *                                          volid volume information
 *   return: 0 if success, or -1
 *   header(in):
 *   volid(in):
 *
 * Note: Permanent volume informations are stored from volid 0 to
 *       header->max_perm_vols. If header->max_perm_vols is less than volid,
 *       allocate new io_volinfo chunk.
 */
static int
fileio_expand_permanent_volume_info (FILEIO_VOLUME_HEADER * header_p, int volid)
{
  int from_idx, to_idx;

  pthread_mutex_lock (&header_p->mutex);

  from_idx = (header_p->max_perm_vols / FILEIO_VOLINFO_INCREMENT);
  to_idx = (volid + 1) / FILEIO_VOLINFO_INCREMENT;

  /* check if to_idx chunks are used for temp volume information */
  if (to_idx >= (header_p->num_volinfo_array - 1 - header_p->max_temp_vols / FILEIO_VOLINFO_INCREMENT))
    {
      pthread_mutex_unlock (&header_p->mutex);
      return -1;
    }

  for (; from_idx <= to_idx; from_idx++)
    {
      if (fileio_allocate_and_initialize_volume_info (header_p, from_idx) != NO_ERROR)
        {
          pthread_mutex_unlock (&header_p->mutex);
          return -1;
        }

      header_p->max_perm_vols = (from_idx + 1) * FILEIO_VOLINFO_INCREMENT;
    }

  pthread_mutex_unlock (&header_p->mutex);
  return 0;
}

/*
 * fileio_expand_temporary_volume_info () - Expand io_volinfo chunks to cache
 *                                          volid volume information
 *   return: 0 if success, or -1
 *   header(in):
 *   volid(in):
 *
 * Note: Temporary volume informations are stored from volid LOG_MAX_DBVOLID to
 *       LOG_MAX_DBVOLID-header->max_temp_vols.
 *       If LOG_MAX_DBVOLID-header->max_temp_vols is greater than volid,
 *       allocate new io_volinfo chunk.
 */
static int
fileio_expand_temporary_volume_info (FILEIO_VOLUME_HEADER * header_p, int volid)
{
  int from_idx, to_idx;

  pthread_mutex_lock (&header_p->mutex);

  from_idx = header_p->num_volinfo_array - 1 - (header_p->max_temp_vols / FILEIO_VOLINFO_INCREMENT);
  to_idx = header_p->num_volinfo_array - 1 - ((LOG_MAX_DBVOLID - volid) / FILEIO_VOLINFO_INCREMENT);

  /* check if to_idx chunks are used for perm. volume information */
  if (to_idx <= (header_p->max_perm_vols - 1) / FILEIO_VOLINFO_INCREMENT)
    {
      pthread_mutex_unlock (&header_p->mutex);
      return -1;
    }

  for (; from_idx >= to_idx; from_idx--)
    {
      if (fileio_allocate_and_initialize_volume_info (header_p, from_idx) != NO_ERROR)
        {
          pthread_mutex_unlock (&header_p->mutex);
          return -1;
        }

      header_p->max_temp_vols = (header_p->num_volinfo_array - from_idx) * FILEIO_VOLINFO_INCREMENT;
    }

  pthread_mutex_unlock (&header_p->mutex);
  return 0;
}
#endif

/* TODO: recoding to use APR
 *
 * fileio_ctime() - VARIANT OF NORMAL CTIME THAT ALWAYS REMOVES THE NEWLINE
 *   return: ptr to time string returned by ctime
 *   time_t(in):
 *   buf(in):
 *
 * Note: Strips the \n off the end of the string returned by ctime.
 *       this routine is really general purpose, there may be other users
 *       of ctime.
 */
char *
fileio_ctime (INT64 * clock_p, char *buffer_p)
{
  char *p, *t;
  time_t tmp_time;

  tmp_time = (time_t) (*clock_p);
  t = ctime_r (&tmp_time, buffer_p);

  p = strchr (t, '\n');
  if (p)
    {
      *p = '\0';
    }

  return (t);
}

/*
 * fileio_initialize_pages () - Initialize the first npages of the given volume with the
 *                    content of given page
 *   return: io_pgptr on success, NULL on failure
 *   vdes(in): Volume descriptor
 *   io_pgptr(in): Initialization content of all pages
 *   npages(in): Number of pages to initialize
 *   kbytes_to_be_written_per_sec : size to add volume per sec
 */
void *
fileio_initialize_pages (THREAD_ENTRY * thread_p, int vol_fd, void *io_page_p,
                         DKNPAGES start_pageid, DKNPAGES npages,
                         size_t page_size, UNUSED_ARG int kbytes_to_be_written_per_sec)
{
  PAGEID page_id;
#if defined (SERVER_MODE)
  int count_of_page_for_a_sleep = 10;
  INT64 allowed_millis_for_a_sleep = 0; /* time which is time for  writing unit of page
                                         * and sleeping in a sleep
                                         */
  INT64 previous_elapsed_millis;        /* time which is previous time for writing unit of page\
                                         * and sleep
                                         */
  INT64 time_to_sleep;
  struct timeval tv;
  INT64 start_in_millis = 0;
  INT64 current_in_millis;
  INT64 page_count_per_sec;
#endif

#if defined (SERVER_MODE)
  if (kbytes_to_be_written_per_sec > 0)
    {
      page_count_per_sec = kbytes_to_be_written_per_sec / (IO_PAGESIZE / ONE_K);

      if (page_count_per_sec < count_of_page_for_a_sleep)
        {
          page_count_per_sec = count_of_page_for_a_sleep;
        }
      allowed_millis_for_a_sleep = count_of_page_for_a_sleep * 1000 / page_count_per_sec;

      gettimeofday (&tv, NULL);
      start_in_millis = (tv.tv_sec * 1000LL) + (tv.tv_usec / 1000LL);
    }
#endif

  for (page_id = start_pageid; page_id < npages + start_pageid; page_id++)
    {
#if !defined(CS_MODE)
      /* check for interrupts from user (i.e. Ctrl-C) */
      if ((page_id % FILEIO_CHECK_FOR_INTERRUPT_INTERVAL) == 0)
        {
          if (pgbuf_is_log_check_for_interrupts (thread_p) == true)
            {
              return NULL;
            }
        }
#endif /* !CS_MODE */

#if !defined(NDEBUG)
      /* skip volume header page to find abnormal update */
      if (page_id == 0)
        {
          continue;
        }
#endif

      if (fileio_write (thread_p, vol_fd, io_page_p, page_id, page_size) == NULL)
        {
          return NULL;
        }

#if defined (SERVER_MODE)
      if (kbytes_to_be_written_per_sec > 0 && (page_id + 1) % count_of_page_for_a_sleep == 0)
        {
          gettimeofday (&tv, NULL);
          current_in_millis = (tv.tv_sec * 1000LL) + (tv.tv_usec / 1000LL);

          previous_elapsed_millis = current_in_millis - start_in_millis;

          /* calculate time to sleep through subtracting */
          time_to_sleep = allowed_millis_for_a_sleep - previous_elapsed_millis;
          if (time_to_sleep > 0)
            {
              thread_sleep (time_to_sleep);
            }
          gettimeofday (&tv, NULL);
          start_in_millis = (tv.tv_sec * 1000LL) + (tv.tv_usec / 1000LL);
        }
#endif
    }

  return io_page_p;
}

PAGE_TYPE
fileio_get_page_ptype (UNUSED_ARG THREAD_ENTRY * thread_p, FILEIO_PAGE_RESERVED * prv_p)
{
  PAGE_TYPE ptype;

  assert (prv_p != NULL);

  ptype = (PAGE_TYPE) (prv_p->ptype);

  return ptype;
}

PAGE_TYPE
fileio_set_page_ptype (THREAD_ENTRY * thread_p, FILEIO_PAGE_RESERVED * prv_p, PAGE_TYPE ptype)
{
  PAGE_TYPE old_ptype;

  assert (prv_p != NULL);
//  assert (ptype >= PAGE_UNKNOWN);
  assert (ptype < PAGE_LAST);

  old_ptype = fileio_get_page_ptype (thread_p, prv_p);

  prv_p->ptype = (unsigned char) ptype;

  return old_ptype;
}

/*
 * fileio_initialize_res () -
 *   return:
 */
void
fileio_initialize_res (THREAD_ENTRY * thread_p, FILEIO_PAGE_RESERVED * prv_p)
{
  LSA_SET_NULL (&(prv_p->lsa));
  prv_p->pageid = NULL_PAGEID;
  prv_p->volid = NULL_VOLID;

  (void) fileio_set_page_ptype (thread_p, prv_p, PAGE_UNKNOWN);
  prv_p->pflag_reserve_1 = '\0';
  prv_p->p_reserve_2 = 0;
  prv_p->p_reserve_3 = 0;
}

/*
 * fileio_alloc_io_page () - get clean I/O page
 */
FILEIO_PAGE *
fileio_alloc_io_page (THREAD_ENTRY * thread_p)
{
  FILEIO_PAGE *malloc_io_pgptr = NULL;

  malloc_io_pgptr = (FILEIO_PAGE *) malloc (IO_PAGESIZE);
  if (malloc_io_pgptr == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, IO_PAGESIZE);
      return NULL;
    }

  (void) fileio_initialize_res (thread_p, &(malloc_io_pgptr->prv));
  MEM_REGION_INIT (&malloc_io_pgptr->page[0], DB_PAGESIZE);

  return malloc_io_pgptr;
}

/*
 * fileio_open () - Same as Unix open, but with retry during interrupts
 *   return: volume descriptor identifier on success, NULL_VOLDES on failure
 *   vol_label_p(in): Volume label
 *   flags(in): open the volume as specified by the flags
 *   mode(in): used when the volume is created
 */
int
fileio_open (const char *vol_label_p, int flags, int mode)
{
  int vol_fd;

  do
    {
      vol_fd = open (vol_label_p, flags, mode);
    }
  while (vol_fd == NULL_VOLDES && errno == EINTR);

#if defined(SERVER_MODE)
  if (vol_fd > NULL_VOLDES)
    {
      int high_vol_fd;
      int range = MAX_NTRANS + 10;

      /* move fd to the over max_clients range */
      high_vol_fd = fcntl (vol_fd, F_DUPFD, range);
      if (high_vol_fd != -1)
        {
          close (vol_fd);
          vol_fd = high_vol_fd;
        }
    }
#endif

  return vol_fd;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * fileio_set_permission () -
 *   return:
 *   vlabel(in):
 */
int
fileio_set_permission (const char *vol_label_p)
{
  int mode;
  struct stat buf;
  int error = NO_ERROR;

  if (stat (vol_label_p, &buf) < 0)
    {
      error = ER_IO_CANNOT_GET_PERMISSION;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, vol_label_p);
      return error;
    }

  /* get currently set mode */
  mode = buf.st_mode;
  /* remove group execute permission from mode */
  mode &= ~(S_IEXEC >> 3);
  /* set 'set group id bit' in mode */
  mode |= S_ISGID;

  if (chmod (vol_label_p, mode) < 0)
    {
      error = ER_IO_CANNOT_CHANGE_PERMISSION;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, vol_label_p);
      return error;
    }

  return error;
}
#endif

/*
 * fileio_close () - Close the volume associated with the given volume descriptor
 *   return: void
 *   vdes(in): Volume descriptor
 */
void
fileio_close (int vol_fd)
{
  assert (vol_fd != NULL_VOLDES);

  if (close (vol_fd) != 0)
    {
      er_set_with_oserror (ER_WARNING_SEVERITY, ARG_FILE_LINE,
                           ER_IO_DISMOUNT_FAIL, 1, fileio_get_volume_label_by_fd (vol_fd, PEEK));
    }
}

/*
 * fileio_create () - Create the volume (or file) without initializing it
 *   return: volume descriptor identifier on success, NULL_VOLDES on failure
 *   db_fullname(in): Name of the database where the volume belongs
 *   vlabel(in): Volume label
 *   volid(in): Volume identifier
 *   dolock(in): Lock the volume from other Unix processes
 *   dosync(in): synchronize the writes on the volume ?
 */
int
fileio_create (THREAD_ENTRY * thread_p, const char *db_full_name_p,
               const char *vol_label_p, UNUSED_ARG VOLID vol_id, bool is_do_lock, bool is_do_sync)
{
  int tmp_vol_desc = NULL_VOLDES;
  int vol_fd;
  FILEIO_LOCKF_TYPE lockf_type = FILEIO_NOT_LOCKF;
  int o_sync;

#if !defined(CS_MODE)
  /* Make sure that the volume is not already mounted.
     if it is, dismount the volume. */
  vol_fd = fileio_find_volume_descriptor_with_label (vol_label_p);
  if (vol_fd != NULL_VOLDES)
    {
      fileio_dismount (thread_p, vol_fd);
    }
#endif /* !CS_MODE */

  o_sync = (is_do_sync != false) ? O_SYNC : 0;

  /* If the file exist make sure that nobody else is using it, before it is
     truncated */
  if (is_do_lock != false)
    {
      tmp_vol_desc = fileio_open (vol_label_p, O_RDWR | o_sync, 0);
      if (tmp_vol_desc != NULL_VOLDES)
        {
          /* The volume (file) already exist. Make sure that nobody is using it
             before the old one is destroyed */
          lockf_type = fileio_lock (db_full_name_p, vol_label_p, tmp_vol_desc, false);
          if (lockf_type == FILEIO_NOT_LOCKF)
            {
              /* Volume seems to be mounted by someone else */
              fileio_close (tmp_vol_desc);
              return NULL_VOLDES;
            }
        }
    }

  vol_fd = fileio_open (vol_label_p, FILEIO_DISK_FORMAT_MODE | o_sync, FILEIO_DISK_PROTECTION_MODE);
  if (vol_fd == NULL_VOLDES)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IO_FORMAT_FAIL, 3, vol_label_p, -1, -1);
    }

  if (tmp_vol_desc != NULL_VOLDES)
    {
      if (lockf_type != FILEIO_NOT_LOCKF)
        {
          fileio_unlock (vol_label_p, tmp_vol_desc, lockf_type);
        }
      fileio_close (tmp_vol_desc);
    }

  mnt_stats_counter (thread_p, MNT_STATS_FILE_CREATES, 1);

  if (vol_fd != NULL_VOLDES)
    {
      if (is_do_lock == true)
        {
          lockf_type = fileio_lock (db_full_name_p, vol_label_p, vol_fd, false);

          if (lockf_type == FILEIO_NOT_LOCKF)
            {
              /* This should not happen, the volume seems to be mounted by
                 someone else */
              fileio_dismount (thread_p, vol_fd);
              fileio_unformat (thread_p, vol_label_p);
              vol_fd = NULL_VOLDES;

              return vol_fd;
            }
        }

#if !defined(CS_MODE)
      if (fileio_cache (vol_id, vol_label_p, vol_fd, lockf_type) != vol_fd)
        {
          /* This should not happen, the volume seems to be mounted by
             someone else */
          fileio_dismount (thread_p, vol_fd);
          fileio_unformat (thread_p, vol_label_p);
          vol_fd = NULL_VOLDES;

          return vol_fd;
        }
#endif /* !CS_MODE */
    }

  return vol_fd;
}

/*
 * fileio_format () - Format a volume of npages and mount the volume
 *   return: volume descriptor identifier on success, NULL_VOLDES on failure
 *   db_fullname(in): Name of the database where the volume belongs
 *   vlabel(in): Volume label
 *   volid(in): Volume identifier
 *   npages(in): Number of pages
 *   sweep_clean(in): Clean the newly formatted volume
 *   dolock(in): Lock the volume from other Unix processes
 *   dosync(in): synchronize the writes on the volume ?
 *   kbytes_to_be_written_per_sec : size to add volume per sec
 *
 * Note: If sweep_clean is true, every page is initialized with recovery
 *       information. In addition a volume can be optionally locked.
 *       For example, the active log volume is locked to prevent
 *       several server processes from accessing the same database.
 */
int
fileio_format (THREAD_ENTRY * thread_p, const char *db_full_name_p,
               const char *vol_label_p, VOLID vol_id, DKNPAGES npages,
               bool is_sweep_clean, bool is_do_lock, bool is_do_sync,
               size_t page_size, int kbytes_to_be_written_per_sec, bool reuse_file)
{
  int vol_fd;
  FILEIO_PAGE *io_page_p;
  off_t offset;
  DKNPAGES max_npages;
  struct stat buf;
  bool is_raw_device = false;

  /* Check for bad number of pages...and overflow */
  if (npages <= 0)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IO_FORMAT_BAD_NPAGES, 2, vol_label_p, npages);
      return NULL_VOLDES;
    }

  if (fileio_is_volume_exist (vol_label_p) == true && reuse_file == false)
    {
      /* The volume that we are trying to create already exist.
         Remove it and try again */
      if (lstat (vol_label_p, &buf) != 0)
        {
          er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IO_MOUNT_FAIL, 1, vol_label_p);
        }

      if (!S_ISLNK (buf.st_mode))
        {
          fileio_unformat (thread_p, vol_label_p);
        }
      else
        {
          if (stat (vol_label_p, &buf) != 0)
            {
              er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IO_MOUNT_FAIL, 1, vol_label_p);
            }

          is_raw_device = S_ISCHR (buf.st_mode);
        }
    }

  er_log_debug (ARG_FILE_LINE, "fileio_format: start, is_raw_device = %d\n", is_raw_device);

  if (is_raw_device)
    {
      max_npages = (DKNPAGES) VOL_MAX_NPAGES (page_size);
    }
  else
    {
      max_npages = fileio_get_number_of_partition_free_pages (vol_label_p, page_size);
    }

  offset = FILEIO_GET_FILE_SIZE (page_size, npages - 1);

  /*
   * Make sure that there is enough pages on the given partition before we
   * create and initialize the volume.
   * We should also check for overflow condition.
   */
  if (npages > max_npages || (offset < npages && npages > 1))
    {
      if (offset < npages)
        {
          /* Overflow */
          offset = FILEIO_GET_FILE_SIZE (page_size, VOL_MAX_NPAGES (page_size));
        }

      if (max_npages >= 0)
        {
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IO_FORMAT_OUT_OF_SPACE,
                  5, vol_label_p, npages, (offset / 1024), max_npages,
                  FILEIO_GET_FILE_SIZE (page_size / 1024, max_npages));
        }
      else
        {
          /* There was an error in fileio_get_number_of_partition_free_pages */
          ;
        }

      return NULL_VOLDES;
    }

  io_page_p = fileio_alloc_io_page (thread_p);
  if (io_page_p == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, page_size);
      return NULL_VOLDES;
    }

  vol_fd = fileio_create (thread_p, db_full_name_p, vol_label_p, vol_id, is_do_lock, is_do_sync);
  if (vol_fd != NULL_VOLDES)
    {
      /* initialize the pages of the volume */

      if (!((fileio_write (thread_p, vol_fd, io_page_p, npages - 1,
                           page_size) == io_page_p)
            && (is_sweep_clean == false
                || fileio_initialize_pages (thread_p, vol_fd,
                                            io_page_p, 0, npages,
                                            page_size, kbytes_to_be_written_per_sec) == io_page_p)))
        {
          /* It is likely that we run of space. The partition where the volume
             was created has been used since we checked above. */

          max_npages = fileio_get_number_of_partition_free_pages (vol_label_p, page_size);

          fileio_dismount (thread_p, vol_fd);
          fileio_unformat (thread_p, vol_label_p);
          free_and_init (io_page_p);
          if (er_errid () != ER_INTERRUPTED)
            {
              er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
                      ER_IO_FORMAT_OUT_OF_SPACE, 5, vol_label_p, npages,
                      (offset / 1024), max_npages, ((page_size / 1024) * max_npages));
            }
          vol_fd = NULL_VOLDES;
          return vol_fd;
        }

    }
  else
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_BO_CANNOT_CREATE_VOL, 2, vol_label_p, db_full_name_p);
    }

  er_log_debug (ARG_FILE_LINE, "fileio_format: end\n");

  free_and_init (io_page_p);
  return vol_fd;
}

/*
 * fileio_expand () -  Expand a volume with the given number of data pages
 *   return: npages
 *   volid(in): Volume identifier
 *   npages_toadd(in): Number of pages to add
 *
 * Note: Pages are not sweep_clean/initialized if they are part of
 *       temporary volumes.
 *
 *       NOTE: No checking for temporary volumes is performed by this function.
 *
 *	 NOTE: On WINDOWS && SERVER MODE io_mutex lock must be obtained before
 *	  calling lseek. Otherwise, expanding can interfere with fileio_read
 *	  and fileio_write calls. This caused corruptions in the temporary
 *	  file, random pages being written at the end of file instead of being
 *	  written at their designated places.
 */
DKNPAGES
fileio_expand (THREAD_ENTRY * thread_p, VOLID vol_id, DKNPAGES npages_toadd, DISK_VOLPURPOSE purpose)
{
  int vol_fd;
  const char *vol_label_p;
  off_t start_offset, last_offset;
  DKNPAGES max_npages;

  vol_fd = fileio_get_volume_descriptor (vol_id);
  vol_label_p = fileio_get_volume_label (vol_id, PEEK);

  if (vol_fd == NULL_VOLDES || vol_label_p == NULL)
    {
      return -1;
    }

  /* Check for bad number of pages and overflow */
  if (npages_toadd <= 0)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IO_FORMAT_BAD_NPAGES, 2, vol_label_p, npages_toadd);
      return -1;
    }

  er_log_debug (ARG_FILE_LINE, "fileio_expand: start, npages_toadd = %d\n", npages_toadd);

  max_npages = fileio_get_number_of_partition_free_pages (vol_label_p, IO_PAGESIZE);

  /* Find the offset to the end of the file, then add the given number of
     pages */
  start_offset = lseek (vol_fd, 0, SEEK_END);
  last_offset = start_offset + FILEIO_GET_FILE_SIZE (IO_PAGESIZE, npages_toadd - 1);

  /*
   * Make sure that there is enough pages on the given partition before we
   * create and initialize the volume.
   * We should also check for overflow condition.
   */
  if (npages_toadd > max_npages || last_offset < npages_toadd)
    {
      if (last_offset < npages_toadd)
        {
          /* Overflow */
          last_offset = FILEIO_GET_FILE_SIZE (IO_PAGESIZE, VOL_MAX_NPAGES (IO_PAGESIZE));
        }
      if (npages_toadd > max_npages && max_npages >= 0)
        {
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IO_EXPAND_OUT_OF_SPACE,
                  5, vol_label_p, npages_toadd, last_offset / 1024,
                  max_npages, FILEIO_GET_FILE_SIZE (IO_PAGESIZE / 1024, max_npages));
        }
      else
        {
          /* There was an error in fileio_get_number_of_partition_free_pages */
        }
      return -1;
    }

  if (purpose != DISK_TEMPVOL_TEMP_PURPOSE)
    {
      FILEIO_PAGE *io_page_p;
      DKNPAGES start_pageid, last_pageid;

      io_page_p = fileio_alloc_io_page (thread_p);
      if (io_page_p == NULL)
        {
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, IO_PAGESIZE);
          return -1;
        }

      start_pageid = (DKNPAGES) (start_offset / IO_PAGESIZE);
      last_pageid = (DKNPAGES) (last_offset / IO_PAGESIZE);

      /* support generic volume only */
      assert_release (purpose == DISK_PERMVOL_GENERIC_PURPOSE);

      if (fileio_initialize_pages (thread_p, vol_fd,
                                   io_page_p, start_pageid, last_pageid - start_pageid + 1, IO_PAGESIZE, -1) == NULL)
        {
          npages_toadd = -1;
        }

      free_and_init (io_page_p);
    }

  if (npages_toadd < 0 && er_errid () != ER_INTERRUPTED)
    {
      /* It is likely that we run of space. The partition where the volume was
         created has been used since we checked above. */
      max_npages = fileio_get_number_of_partition_free_pages (vol_label_p, IO_PAGESIZE);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IO_EXPAND_OUT_OF_SPACE, 5,
              vol_label_p, npages_toadd, last_offset / 1024, max_npages,
              FILEIO_GET_FILE_SIZE (IO_PAGESIZE / 1024, max_npages));
    }

  er_log_debug (ARG_FILE_LINE, "fileio_expand: end, npages_toadd = %d\n", npages_toadd);

  return npages_toadd;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * fileio_truncate () -  TRUNCATE A TEMPORARY VOLUME
 *   return: npages
 *   volid(in):  Volume identifier
 *   npages_to_resize(in):  Number of pages to resize
 */
DKNPAGES
fileio_truncate (VOLID vol_id, DKNPAGES npages_to_resize)
{
  int vol_fd;
  const char *vol_label_p;
  off_t length;
  bool is_retry = true;

  vol_fd = fileio_get_volume_descriptor (vol_id);
  vol_label_p = fileio_get_volume_label (vol_id, PEEK);

  if (vol_fd == NULL_VOLDES || vol_label_p == NULL)
    {
      return -1;
    }

  if (npages_to_resize <= 0)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IO_FORMAT_BAD_NPAGES, 2, vol_label_p, npages_to_resize);
      return -1;
    }

  length = FILEIO_GET_FILE_SIZE (IO_PAGESIZE, npages_to_resize);
  while (is_retry == true)
    {
      is_retry = false;
      if (ftruncate (vol_fd, length))
        {
          if (errno == EINTR)
            {
              is_retry = true;
            }
          else
            {
              er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
                                   ER_IO_TRUNCATE, 2, npages_to_resize, fileio_get_volume_label_by_fd (vol_fd, PEEK));
              return -1;
            }
        }
    }
  return npages_to_resize;
}
#endif

/*
 * fileio_unformat () - DESTROY A VOLUME
 *   return: void
 *   vlabel(in): Label of volume to unformat
 *
 * Note: If the volume is mounted, it is dismounted. Then, the volume is
 *       destroyed/unformatted.
 */
void
fileio_unformat (THREAD_ENTRY * thread_p, const char *vol_label_p)
{
  fileio_unformat_and_rename (thread_p, vol_label_p, NULL);
}

/*
 * fileio_unformat_and_rename () - DESTROY A VOLUME
 *   return: void
 *   vol_label(in): Label of volume to unformat
 *   new_vlabel(in): New volume label. if NULL, volume will be deleted
 *
 * Note: If the volume is mounted, it is dismounted. Then, the volume is
 *       destroyed/unformatted.
 */
void
fileio_unformat_and_rename (UNUSED_ARG THREAD_ENTRY * thread_p, const char *vol_label_p, const char *new_label_p)
{
#if defined (EnableThreadMonitoring)
  struct timeval start_time, end_time, elapsed_time;
#endif
#if !defined(CS_MODE)
  int vol_fd;
  char vlabel_p[PATH_MAX];

  /* Dismount the volume if it is mounted */
  vol_fd = fileio_find_volume_descriptor_with_label (vol_label_p);
  if (vol_fd != NULL_VOLDES)
    {
      /* if vol_label_p is a pointer of global vinfo->vlabel,
       * It can be reset in fileio_dismount
       */
      STRNCPY (vlabel_p, vol_label_p, PATH_MAX);
      vol_label_p = vlabel_p;
      fileio_dismount (thread_p, vol_fd);
    }
#endif /* !CS_MODE */

#if defined (EnableThreadMonitoring)
  if (0 < prm_get_integer_value (PRM_ID_MNT_WAITING_THREAD))
    {
      gettimeofday (&start_time, NULL);
    }
#endif

  if (new_label_p == NULL)
    {
      (void) rye_remove_files (vol_label_p);
    }
  else
    {
      if (os_rename_file (vol_label_p, new_label_p) != NO_ERROR)
        {
          er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IO_RENAME_FAIL, 2, vol_label_p, new_label_p);
        }
    }

#if defined (EnableThreadMonitoring)
  if (0 < prm_get_integer_value (PRM_ID_MNT_WAITING_THREAD))
    {
      gettimeofday (&end_time, NULL);
      DIFF_TIMEVAL (start_time, end_time, elapsed_time);
    }

  if (MONITOR_WAITING_THREAD (elapsed_time))
    {
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
              ER_MNT_WAITING_THREAD, 2, "file remove", prm_get_integer_value (PRM_ID_MNT_WAITING_THREAD));
      er_log_debug (ARG_FILE_LINE, "fileio_unformat: %6d.%06d\n", elapsed_time.tv_sec, elapsed_time.tv_usec);
    }
#endif
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * fileio_copy_volume () - COPY A DISK
 *   return: volume descriptor identifier on success, NULL_VOLDES on failure
 *   from_vdes(in): From Volume descriptor
 *   npages(in): From Volume descriptor
 *   to_vlabel(in): To Volume label
 *   to_volid(in): Volume identifier assigned to the copy
 *   reset_rcvinfo(in): Reset recovery information?
 *
 * Note: Format a new volume with the number of given pages and copy
 *       the contents of the volume associated with from_vdes onto the
 *       new generated volume. The recovery information kept in every
 *       page may be optionally initialized.
 */
int
fileio_copy_volume (THREAD_ENTRY * thread_p, int from_vol_desc,
                    DKNPAGES npages, const char *to_vol_label_p, VOLID to_vol_id, bool is_reset_recovery_info)
{
  PAGEID page_id;
  FILEIO_PAGE *malloc_io_page_p = NULL;
  int to_vol_desc;

  /*
   * Create the to_volume. Don't initialize the volume with recovery
   * information since it generated/created when the content of the pages are
   * copied.
   */

  to_vol_desc =
    fileio_format (thread_p, NULL, to_vol_label_p, to_vol_id, npages, false, false, false, IO_PAGESIZE, 0, false);
  if (to_vol_desc == NULL_VOLDES)
    {
      return NULL_VOLDES;
    }

  /* Don't read the pages from the page buffer pool but directly from disk */
  malloc_io_page_p = (FILEIO_PAGE *) malloc (IO_PAGESIZE);
  if (malloc_io_page_p == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, IO_PAGESIZE);
      goto error;
    }

  if (is_reset_recovery_info == false)
    {
      /* Copy the volume as it is */
      for (page_id = 0; page_id < npages; page_id++)
        {
          if (fileio_read (thread_p, from_vol_desc, malloc_io_page_p, page_id,
                           IO_PAGESIZE) == NULL
              || fileio_write (thread_p, to_vol_desc, malloc_io_page_p, page_id, IO_PAGESIZE) == NULL)
            {
              goto error;
            }
        }
    }
  else
    {
      /* Reset the recovery information.
         Just like if this was a formatted volume */
      for (page_id = 0; page_id < npages; page_id++)
        {
          if (fileio_read (thread_p, from_vol_desc, malloc_io_page_p, page_id, IO_PAGESIZE) == NULL)
            {
              goto error;
            }
          else
            {
              LSA_SET_NULL (&malloc_io_page_p->prv.lsa);
              if (fileio_write (thread_p, to_vol_desc, malloc_io_page_p, page_id, IO_PAGESIZE) == NULL)
                {
                  goto error;
                }
            }
        }
    }

  if (fileio_synchronize (thread_p, to_vol_desc, to_vol_label_p) != to_vol_desc)
    {
      goto error;
    }

  free_and_init (malloc_io_page_p);
  return to_vol_desc;

error:
  fileio_dismount (thread_p, to_vol_desc);
  fileio_unformat (thread_p, to_vol_label_p);
  if (malloc_io_page_p != NULL)
    {
      free_and_init (malloc_io_page_p);
    }

  return NULL_VOLDES;
}
#endif

/*
 * fileio_reset_volume () - Reset the recovery information (LSA) of all pages of given
 *                  volume with given reset_lsa
 *   return:
 *   vdes(in): Volume descriptor
 *   vlabel(in): Volume label
 *   npages(in): Number of pages of volume to reset
 *   reset_lsa(in): The reset recovery information LSA
 */
int
fileio_reset_volume (THREAD_ENTRY * thread_p, int vol_fd, const char *vlabel, DKNPAGES npages, LOG_LSA * reset_lsa_p)
{
  PAGEID page_id;
  FILEIO_PAGE *malloc_io_page_p;
  int success = NO_ERROR;

  malloc_io_page_p = (FILEIO_PAGE *) malloc (IO_PAGESIZE);
  if (malloc_io_page_p == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, IO_PAGESIZE);
      return ER_FAILED;
    }

  for (page_id = 0; page_id < npages; page_id++)
    {
      if (fileio_read (thread_p, vol_fd, malloc_io_page_p, page_id, IO_PAGESIZE) != NULL)
        {
          LSA_COPY (&malloc_io_page_p->prv.lsa, reset_lsa_p);
          if (fileio_write (thread_p, vol_fd, malloc_io_page_p, page_id, IO_PAGESIZE) == NULL)
            {
              success = ER_FAILED;
              break;
            }
        }
      else
        {
          success = ER_FAILED;
          break;
        }
    }
  free_and_init (malloc_io_page_p);

  if (fileio_synchronize (thread_p, vol_fd, vlabel) != vol_fd)
    {
      success = ER_FAILED;
    }

  return success;
}

/*
 * fileio_mount () - Mount the volume associated with the given name and permanent
 *               identifier
 *   return: volume descriptor identifier on success, NULL_VOLDES on failure
 *   db_fullname(in): Name of the database where the volume belongs
 *   vlabel(in): Volume label
 *   volid(in): Permanent Volume identifier
 *   lockwait(in): Lock the volume from other Unix processes
 *   dosync(in): synchronize the writes on the volume ?
 */
int
fileio_mount (THREAD_ENTRY * thread_p, const char *db_full_name_p,
              const char *vol_label_p, UNUSED_ARG VOLID vol_id, int lock_wait, bool is_do_sync)
{
  int vol_fd;
  int o_sync;
  FILEIO_LOCKF_TYPE lockf_type = FILEIO_NOT_LOCKF;
  bool is_do_wait;
  struct stat stat_buf;
  time_t last_modification_time = 0;
  off_t last_size = 0;

#if !defined(CS_MODE)
  FILEIO_CHECK_AND_INITIALIZE_VOLUME_HEADER_CACHE (NULL_VOLDES);

  /* Is volume already mounted ? */
  vol_fd = fileio_find_volume_descriptor_with_label (vol_label_p);
  if (vol_fd != NULL_VOLDES)
    {
      return vol_fd;
    }
#endif /* !CS_MODE */

  o_sync = (is_do_sync != false) ? O_SYNC : 0;

  /* OPEN THE DISK VOLUME PARTITION OR FILE SIMULATED VOLUME */
start:
  vol_fd = fileio_open (vol_label_p, O_RDWR | o_sync, 0600);
  if (vol_fd == NULL_VOLDES)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IO_MOUNT_FAIL, 1, vol_label_p);
      return NULL_VOLDES;
    }

  is_do_wait = (lock_wait > 1) ? true : false;
  if (is_do_wait)
    {
      if (fstat (vol_fd, &stat_buf) != 0)
        {
          fileio_close (vol_fd);
          return NULL_VOLDES;
        }
      last_modification_time = stat_buf.st_mtime;
      last_size = stat_buf.st_size;
    }

  /* LOCK THE DISK   */
  if (lock_wait != 0)
    {
      lockf_type = fileio_lock (db_full_name_p, vol_label_p, vol_fd, is_do_wait);
      if (lockf_type == FILEIO_NOT_LOCKF)
        {
          /* Volume seems to be mounted by someone else */
          fileio_close (vol_fd);
          return NULL_VOLDES;
        }
      else if (lockf_type == FILEIO_LOCKF && is_do_wait == true)
        {
          /* may need to reopen the file */
          if (fstat (vol_fd, &stat_buf) != 0)
            {
              fileio_dismount (thread_p, vol_fd);
              return NULL_VOLDES;
            }

          if (last_modification_time != stat_buf.st_mtime || last_size != stat_buf.st_size)
            {
              /* somebody changed the file before the file lock was acquired */
              fileio_dismount (thread_p, vol_fd);
              goto start;
            }
        }
    }

#if !defined(CS_MODE)
  /* Cache mounting information */
  if (fileio_cache (vol_id, vol_label_p, vol_fd, lockf_type) != vol_fd)
    {
      fileio_dismount (thread_p, vol_fd);
      return NULL_VOLDES;
    }
#endif /* !CS_MODE */

  return vol_fd;
}

/*
 * fileio_dismount () - Dismount the volume associated with the given volume
 *                  descriptor
 *   return: void
 *   vdes(in): Volume descriptor
 */
void
fileio_dismount (THREAD_ENTRY * thread_p, int vol_fd)
{
  const char *vlabel;
  FILEIO_LOCKF_TYPE lockf_type;

  assert (vol_fd != NULL_VOLDES);

  /*
   * Make sure that all dirty pages of the volume are forced to disk. This
   * is needed since a close of a file and program exist, does not imply
   * that the dirty pages of the file (or files that the program opened) are
   * forced to disk.
   */
  vlabel = fileio_get_volume_label_by_fd (vol_fd, PEEK);

  (void) fileio_synchronize (thread_p, vol_fd, vlabel);

  lockf_type = fileio_get_lockf_type (vol_fd);
  if (lockf_type != FILEIO_NOT_LOCKF)
    {
      fileio_unlock (vlabel, vol_fd, lockf_type);
    }

  fileio_close (vol_fd);

  /* Decache volume information even during errors */
  fileio_decache (thread_p, vol_fd);
}

static int
fileio_max_permanent_volumes (int index, int num_permanent_volums)
{
  if (index < (num_permanent_volums - 1) / FILEIO_VOLINFO_INCREMENT)
    {
      return FILEIO_VOLINFO_INCREMENT - 1;
    }
  else
    {
      return (num_permanent_volums - 1) % FILEIO_VOLINFO_INCREMENT;
    }
}

static int
fileio_min_temporary_volumes (int index, int num_temp_volums, int num_volinfo_array)
{
  if (index > (num_volinfo_array - 1 - (num_temp_volums - 1) / FILEIO_VOLINFO_INCREMENT))
    {
      return 0;
    }
  else
    {
      return FILEIO_VOLINFO_INCREMENT - 1 - (num_temp_volums - 1) % FILEIO_VOLINFO_INCREMENT;
    }
}

#if defined (ENABLE_UNUSED_FUNCTION)
static FILEIO_SYSTEM_VOLUME_INFO *
fileio_traverse_system_volume (THREAD_ENTRY * thread_p, SYS_VOLINFO_APPLY_FN apply_function, APPLY_ARG * arg)
{
  FILEIO_SYSTEM_VOLUME_INFO *sys_vol_info_p;

  pthread_mutex_lock (&fileio_Sys_vol_info_header.mutex);

  for (sys_vol_info_p = &fileio_Sys_vol_info_header.anchor;
       sys_vol_info_p != NULL && sys_vol_info_p->vdes != NULL_VOLDES; sys_vol_info_p = sys_vol_info_p->next)
    {
      if ((*apply_function) (thread_p, sys_vol_info_p, arg) == true)
        {
          pthread_mutex_unlock (&fileio_Sys_vol_info_header.mutex);
          return sys_vol_info_p;
        }
    }
  pthread_mutex_unlock (&fileio_Sys_vol_info_header.mutex);

  return NULL;
}
#endif

static FILEIO_VOLUME_INFO *
fileio_traverse_permanent_volume (THREAD_ENTRY * thread_p, VOLINFO_APPLY_FN apply_function, APPLY_ARG * arg)
{
  int i, j, max_j;
  FILEIO_VOLUME_HEADER *header_p;
  FILEIO_VOLUME_INFO *vol_info_p;

  header_p = &fileio_Vol_info_header;

  for (i = 0; i <= (header_p->next_perm_volid - 1) / FILEIO_VOLINFO_INCREMENT; i++)
    {
      max_j = fileio_max_permanent_volumes (i, header_p->next_perm_volid);

      for (j = 0; j <= max_j; j++)
        {
          vol_info_p = &header_p->volinfo[i][j];
          if ((*apply_function) (thread_p, vol_info_p, arg) == true)
            {
              return vol_info_p;
            }
        }
    }

  return NULL;
}

static FILEIO_VOLUME_INFO *
fileio_traverse_temporary_volume (THREAD_ENTRY * thread_p, VOLINFO_APPLY_FN apply_function, APPLY_ARG * arg)
{
  int i, j, min_j, num_temp_vols;
  FILEIO_VOLUME_HEADER *header_p;
  FILEIO_VOLUME_INFO *vol_info_p;

  header_p = &fileio_Vol_info_header;
  num_temp_vols = LOG_MAX_DBVOLID - header_p->next_temp_volid;

  for (i = header_p->num_volinfo_array - 1;
       i > (header_p->num_volinfo_array - 1
            - (num_temp_vols + FILEIO_VOLINFO_INCREMENT - 1) / FILEIO_VOLINFO_INCREMENT); i--)
    {
      min_j = fileio_min_temporary_volumes (i, num_temp_vols, header_p->num_volinfo_array);

      for (j = FILEIO_VOLINFO_INCREMENT - 1; j >= min_j; j--)
        {
          vol_info_p = &header_p->volinfo[i][j];
          if ((*apply_function) (thread_p, vol_info_p, arg) == true)
            {
              return vol_info_p;
            }
        }
    }

  return NULL;
}

static bool
fileio_dismount_volume (THREAD_ENTRY * thread_p, FILEIO_VOLUME_INFO * vol_info_p, UNUSED_ARG APPLY_ARG * ignore_arg)
{
  if (vol_info_p->vdes != NULL_VOLDES)
    {
      (void) fileio_synchronize (thread_p, vol_info_p->vdes, vol_info_p->vlabel);

      if (vol_info_p->lockf_type != FILEIO_NOT_LOCKF)
        {
          fileio_unlock (vol_info_p->vlabel, vol_info_p->vdes, vol_info_p->lockf_type);
        }

      fileio_close (vol_info_p->vdes);
    }

  return false;
}

/*
 * fileio_dismount_all () - Dismount all mounted volumes
 *   return: void
 */
void
fileio_dismount_all (THREAD_ENTRY * thread_p)
{
  FILEIO_SYSTEM_VOLUME_HEADER *sys_header_p;
  FILEIO_SYSTEM_VOLUME_INFO *sys_vol_info_p, *tmp_sys_vol_info_p;
  FILEIO_VOLUME_HEADER *vol_header_p;
  int i, num_perm_vols, num_temp_vols;
  APPLY_ARG ignore_arg = { 0 };

  /* First, traverse sys volumes */
  sys_header_p = &fileio_Sys_vol_info_header;
  pthread_mutex_lock (&sys_header_p->mutex);

  for (sys_vol_info_p = &sys_header_p->anchor; sys_vol_info_p != NULL;)
    {
      if (sys_vol_info_p->vdes != NULL_VOLDES)
        {
          (void) fileio_synchronize (thread_p, sys_vol_info_p->vdes, sys_vol_info_p->vlabel);

          if (sys_vol_info_p->lockf_type != FILEIO_NOT_LOCKF)
            {
              fileio_unlock (sys_vol_info_p->vlabel, sys_vol_info_p->vdes, sys_vol_info_p->lockf_type);
            }

          fileio_close (sys_vol_info_p->vdes);
        }

      tmp_sys_vol_info_p = sys_vol_info_p;
      sys_vol_info_p = sys_vol_info_p->next;
      if (tmp_sys_vol_info_p != &sys_header_p->anchor)
        {
          free_and_init (tmp_sys_vol_info_p);
        }
    }

  pthread_mutex_unlock (&sys_header_p->mutex);

  /* Second, traverse perm/temp volumes */
  vol_header_p = &fileio_Vol_info_header;
  pthread_mutex_lock (&vol_header_p->mutex);
  num_perm_vols = vol_header_p->next_perm_volid;

  (void) fileio_traverse_permanent_volume (thread_p, fileio_dismount_volume, &ignore_arg);

  for (i = 0; i <= (num_perm_vols - 1) / FILEIO_VOLINFO_INCREMENT; i++)
    {
      if (vol_header_p->volinfo)
        {
          free_and_init (vol_header_p->volinfo[i]);
        }
    }

  vol_header_p->max_perm_vols = 0;
  vol_header_p->next_perm_volid = 0;

  num_temp_vols = LOG_MAX_DBVOLID - vol_header_p->next_temp_volid;

  (void) fileio_traverse_temporary_volume (thread_p, fileio_dismount_volume, &ignore_arg);

  for (i = vol_header_p->num_volinfo_array - 1;
       i > (vol_header_p->num_volinfo_array - 1
            - (num_temp_vols + FILEIO_VOLINFO_INCREMENT - 1) / FILEIO_VOLINFO_INCREMENT); i--)
    {
      if (vol_header_p->volinfo)
        {
          free_and_init (vol_header_p->volinfo[i]);
        }
    }

  vol_header_p->max_temp_vols = 0;
  vol_header_p->next_temp_volid = LOG_MAX_DBVOLID;

  free_and_init (vol_header_p->volinfo);

  pthread_mutex_unlock (&vol_header_p->mutex);
}

/*
 * fileio_map_mounted () - Map over the data volumes
 *   return:
 *   fun(in): Function to call on volid and args
 *   args(in): argumemts for fun
 *
 * Note : Map over all data volumes (i.e., the log volumes are skipped),
 *        by calling the given function on every volume. If the function
 *        returns false the mapping is stopped.
 */
bool
fileio_map_mounted (THREAD_ENTRY * thread_p,
                    bool (*fun) (THREAD_ENTRY * thread_p, VOLID vol_id, void *args), void *args)
{
  FILEIO_VOLUME_INFO *vol_info_p;
  FILEIO_VOLUME_HEADER *header_p;
  int i, j, max_j, min_j, num_temp_vols;

  FILEIO_CHECK_AND_INITIALIZE_VOLUME_HEADER_CACHE (false);

  header_p = &fileio_Vol_info_header;
  for (i = 0; i <= (header_p->next_perm_volid - 1) / FILEIO_VOLINFO_INCREMENT; i++)
    {
      max_j = fileio_max_permanent_volumes (i, header_p->next_perm_volid);

      for (j = 0; j <= max_j; j++)
        {
          vol_info_p = &header_p->volinfo[i][j];
          if (vol_info_p->vdes != NULL_VOLDES)
            {
              if (((*fun) (thread_p, vol_info_p->volid, args)) == false)
                {
                  return false;
                }
            }
        }
    }

  num_temp_vols = LOG_MAX_DBVOLID - header_p->next_temp_volid;
  for (i = header_p->num_volinfo_array - 1;
       i > (header_p->num_volinfo_array - 1
            - (num_temp_vols + FILEIO_VOLINFO_INCREMENT - 1) / FILEIO_VOLINFO_INCREMENT); i--)
    {
      min_j = fileio_min_temporary_volumes (i, num_temp_vols, header_p->num_volinfo_array);

      for (j = FILEIO_VOLINFO_INCREMENT - 1; j >= min_j; j--)
        {
          vol_info_p = &header_p->volinfo[i][j];
          if (vol_info_p->vdes != NULL_VOLDES)
            {
              if (((*fun) (thread_p, vol_info_p->volid, args)) == false)
                {
                  return false;
                }
            }
        }
    }

  return true;
}

static bool
fileio_is_volume_descriptor_equal (UNUSED_ARG THREAD_ENTRY * thread_p, FILEIO_VOLUME_INFO * vol_info_p, APPLY_ARG * arg)
{
  return (vol_info_p->vdes == arg->vdes);
}

static FILEIO_SYSTEM_VOLUME_INFO *
fileio_find_system_volume (THREAD_ENTRY * thread_p, SYS_VOLINFO_APPLY_FN apply_function, APPLY_ARG * arg)
{
  FILEIO_SYSTEM_VOLUME_INFO *sys_vol_info_p;

  for (sys_vol_info_p = &fileio_Sys_vol_info_header.anchor;
       sys_vol_info_p != NULL && sys_vol_info_p->vdes != NULL_VOLDES; sys_vol_info_p = sys_vol_info_p->next)
    {
      if ((*apply_function) (thread_p, sys_vol_info_p, arg) == true)
        {
          return sys_vol_info_p;
        }
    }

  return NULL;
}

static bool
fileio_is_system_volume_descriptor_equal (UNUSED_ARG THREAD_ENTRY * thread_p,
                                          FILEIO_SYSTEM_VOLUME_INFO * sys_vol_info_p, APPLY_ARG * arg)
{
  return (sys_vol_info_p->vdes == arg->vdes);
}

static bool
fileio_is_system_volume_id_equal (UNUSED_ARG THREAD_ENTRY * thread_p,
                                  FILEIO_SYSTEM_VOLUME_INFO * sys_vol_info_p, APPLY_ARG * arg)
{
  return (sys_vol_info_p->volid == arg->vol_id);
}

static bool
fileio_is_system_volume_label_equal (UNUSED_ARG THREAD_ENTRY * thread_p,
                                     FILEIO_SYSTEM_VOLUME_INFO * sys_vol_info_p, APPLY_ARG * arg)
{
  return (util_compare_filepath (sys_vol_info_p->vlabel, arg->vol_label) == 0);
}

/*
 * fileio_read () - READ A PAGE FROM DISK
 *   return:
 *   vol_fd(in): Volume descriptor
 *   io_page_p(out): Address where content of page is stored. Must be of
 *                   page_size long
 *   page_id(in): Page identifier
 *   page_size(in): Page size
 *
 * Note: Read the content of the page described by page_id onto the
 *       given io_page_p buffer. The io_page_p must be page_size long.
 */
void *
fileio_read (UNUSED_ARG THREAD_ENTRY * thread_p, int vol_fd, void *io_page_p, PAGEID page_id, size_t page_size)
{
#if defined (EnableThreadMonitoring)
  struct timeval start_time, end_time, elapsed_time;
#endif
  off_t offset = FILEIO_GET_FILE_SIZE (page_size, page_id);
  ssize_t nbytes = 0;
  bool is_retry = true;

#if defined(USE_AIO)
  struct aiocb cb;
  const struct aiocb *cblist[1];
#endif /* USE_AIO */
  UINT64 perf_start;

  PERF_MON_GET_CURRENT_TIME (perf_start);

#if defined (EnableThreadMonitoring)
  if (0 < prm_get_integer_value (PRM_ID_MNT_WAITING_THREAD))
    {
      gettimeofday (&start_time, NULL);
    }
#endif

  while (is_retry == true)
    {
      is_retry = false;

#if !defined(SERVER_MODE)
      /* Locate the desired page */
      if (lseek (vol_fd, offset, SEEK_SET) != offset)
        {
          er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
                               ER_IO_READ, 2, page_id, fileio_get_volume_label (fileio_get_volume_id (vol_fd), PEEK));
          return NULL;
        }

      /* Read the desired page */
      nbytes = read (vol_fd, io_page_p, page_size);
      if (nbytes != page_size)
#else /* SERVER_MODE */
#if defined(USE_AIO)
      bzero (&cb, sizeof (cb));
      cb.aio_fildes = vol_fd;
      cb.aio_lio_opcode = LIO_READ;
      cb.aio_buf = io_page_p;
      cb.aio_nbytes = page_size;
      cb.aio_offset = offset;
      cblist[0] = &cb;

      if (aio_read (&cb) < 0)
#else /* USE_AIO */
      nbytes = pread (vol_fd, io_page_p, page_size, offset);
      if (nbytes != (ssize_t) page_size)
#endif /* USE_AIO */
#endif /* SERVER_MODE */
        {
          if (nbytes == 0)
            {
              /* This is an end of file. We are trying to read beyond the
                 allocated disk space */
              er_set (ER_FATAL_ERROR_SEVERITY, ARG_FILE_LINE,
                      ER_PB_BAD_PAGEID, 2, page_id, fileio_get_volume_label_by_fd (vol_fd, PEEK));
              return NULL;
            }

          if (errno == EINTR)
            {
              is_retry = true;
            }
          else
            {
              er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
                                   ER_IO_READ, 2, page_id, fileio_get_volume_label_by_fd (vol_fd, PEEK));
              return NULL;
            }
        }
    }

#if defined (EnableThreadMonitoring)
  if (0 < prm_get_integer_value (PRM_ID_MNT_WAITING_THREAD))
    {
      gettimeofday (&end_time, NULL);
      DIFF_TIMEVAL (start_time, end_time, elapsed_time);
    }

  if (MONITOR_WAITING_THREAD (elapsed_time))
    {
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
              ER_MNT_WAITING_THREAD, 2, "file read", prm_get_integer_value (PRM_ID_MNT_WAITING_THREAD));
      er_log_debug (ARG_FILE_LINE, "fileio_read: %6d.%06d\n", elapsed_time.tv_sec, elapsed_time.tv_usec);
    }
#endif

#if defined(SERVER_MODE) && defined(USE_AIO)
  if (aio_suspend (cblist, 1, NULL) < 0 || aio_return (&cb) != page_size)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
                           ER_IO_READ, 2, page_id, fileio_get_volume_label_by_fd (vol_fd, PEEK));
      return NULL;
    }
#endif

  mnt_stats_counter_with_time (thread_p, MNT_STATS_FILE_IOREADS, 1, perf_start);
  return io_page_p;
}

/*
 * fileio_write () - WRITE A PAGE TO DISK
 *   return: io_page_p on success, NULL on failure
 *   vol_fd(in): Volume descriptor
 *   io_page_p(in): In-memory address where the current content of page resides
 *   page_id(in): Page identifier
 *   page_size(in): Page size
 *
 * Note:  Write the content of the page described by page_id to disk. The
 *        content of the page is stored onto io_page_p buffer which is
 *        page_size long.
 */
void *
fileio_write (THREAD_ENTRY * thread_p, int vol_fd, void *io_page_p, PAGEID page_id, size_t page_size)
{
#if defined (EnableThreadMonitoring)
  struct timeval start_time, end_time, elapsed_time;
#endif
  off_t offset = FILEIO_GET_FILE_SIZE (page_size, page_id);
  bool is_retry = true;
#if defined(USE_AIO)
  struct aiocb cb;
  const struct aiocb *cblist[1];
#endif /* USE_AIO */
  UINT64 perf_start;

  PERF_MON_GET_CURRENT_TIME (perf_start);

#if defined (EnableThreadMonitoring)
  if (0 < prm_get_integer_value (PRM_ID_MNT_WAITING_THREAD))
    {
      gettimeofday (&start_time, NULL);
    }
#endif

  while (is_retry == true)
    {
      is_retry = false;

#if !defined(SERVER_MODE)
      if (lseek (vol_fd, offset, SEEK_SET) != offset)
        {
          er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
                               ER_IO_WRITE, 2, page_id, fileio_get_volume_label (fileio_get_volume_id (vol_fd), PEEK));
          return NULL;
        }

      /* write the page */
      if (write (vol_fd, io_page_p, page_size) != page_size)
#else /* SERVER_MODE */
#if defined(USE_AIO)
      bzero (&cb, sizeof (cb));
      cb.aio_fildes = vol_fd;
      cb.aio_lio_opcode = LIO_WRITE;
      cb.aio_buf = io_page_p;
      cb.aio_nbytes = page_size;
      cb.aio_offset = offset;
      cblist[0] = &cb;

      if (aio_write (&cb) < 0)
#else /* USE_AIO */
      if (pwrite (vol_fd, io_page_p, page_size, offset) != (ssize_t) page_size)
#endif /* USE_AIO */
#endif /* SERVER_MODE */
        {
          if (errno == EINTR)
            {
              is_retry = true;
            }
          else
            {
              if (errno == ENOSPC)
                {
                  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
                          ER_IO_WRITE_OUT_OF_SPACE, 2, page_id, fileio_get_volume_label_by_fd (vol_fd, PEEK));
                }
              else
                {
                  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
                                       ER_IO_WRITE, 2, page_id, fileio_get_volume_label_by_fd (vol_fd, PEEK));
                }
              return NULL;
            }
        }
    }

#if defined (EnableThreadMonitoring)
  if (0 < prm_get_integer_value (PRM_ID_MNT_WAITING_THREAD))
    {
      gettimeofday (&end_time, NULL);
      DIFF_TIMEVAL (start_time, end_time, elapsed_time);
    }

  if (MONITOR_WAITING_THREAD (elapsed_time))
    {
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
              ER_MNT_WAITING_THREAD, 2, "file write", prm_get_integer_value (PRM_ID_MNT_WAITING_THREAD));
      er_log_debug (ARG_FILE_LINE, "fileio_write: %6d.%06d\n", elapsed_time.tv_sec, elapsed_time.tv_usec);
    }
#endif

#if defined(SERVER_MODE) && defined(USE_AIO)
  if (aio_suspend (cblist, 1, NULL) < 0 || aio_return (&cb) != page_size)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
                           ER_IO_WRITE, 2, page_id, fileio_get_volume_label_by_fd (vol_fd, PEEK));
      return NULL;
    }
#endif

  fileio_compensate_flush (thread_p, vol_fd, 1);
  mnt_stats_counter_with_time (thread_p, MNT_STATS_FILE_IOWRITES, 1, perf_start);
  return io_page_p;
}

/*
 * fileio_read_pages () -
 */
void *
fileio_read_pages (UNUSED_ARG THREAD_ENTRY * thread_p, int vol_fd,
                   char *io_pages_p, PAGEID page_id, int num_pages, size_t page_size)
{
#if defined (EnableThreadMonitoring)
  struct timeval start_time, end_time, elapsed_time;
#endif
  off_t offset;
  ssize_t nbytes;
  size_t read_bytes;
  UINT64 perf_start;

#if defined(USE_AIO)
  struct aiocb cb;
  const struct aiocb *cblist[1];
#endif /* USE_AIO */

  PERF_MON_GET_CURRENT_TIME (perf_start);

  assert (num_pages > 0);

  offset = FILEIO_GET_FILE_SIZE (page_size, page_id);
  read_bytes = ((size_t) page_size) * ((size_t) num_pages);

#if defined (EnableThreadMonitoring)
  if (0 < prm_get_integer_value (PRM_ID_MNT_WAITING_THREAD))
    {
      gettimeofday (&start_time, NULL);
    }
#endif

  while (read_bytes > 0)
    {
#if !defined(SERVER_MODE)
      /* Locate the desired page */
      if (lseek (vol_fd, offset, SEEK_SET) != offset)
        {
          er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
                               ER_IO_READ, 2, page_id, fileio_get_volume_label (fileio_get_volume_id (vol_fd), PEEK));
          return NULL;
        }

      /* Read the desired page */
      nbytes = read (vol_fd, io_pages_p, read_bytes);
#else /* SERVER_MODE */
#if defined (USE_AIO)
      bzero (&cb, sizeof (cb));
      cb.aio_fildes = vol_fd;
      cb.aio_lio_opcode = LIO_READ;
      cb.aio_buf = io_pages_p;
      cb.aio_nbytes = read_bytes;
      cb.aio_offset = offset;
      cblist[0] = &cb;

      if (aio_read (&cb) < 0 || aio_suspend (cblist, 1, NULL) < 0)
        {
          nbytes = -1;
        }
      nbytes = aio_return (&cb);
#else /* USE_AIO */
      nbytes = pread (vol_fd, io_pages_p, read_bytes, offset);
#endif /* USE_AIO */
#endif /* SERVER_MODE */
      if (nbytes <= 0)
        {
          if (nbytes == 0)
            {
              return NULL;
            }

          switch (errno)
            {
            case EINTR:
            case EAGAIN:
              continue;
            case EOVERFLOW:
              return NULL;
            default:
              {
                er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
                                     ER_IO_READ, 2, page_id, fileio_get_volume_label_by_fd (vol_fd, PEEK));
                return NULL;
              }
            }
        }

      offset += nbytes;
      io_pages_p += nbytes;
      read_bytes -= nbytes;
    }

#if defined (EnableThreadMonitoring)
  if (0 < prm_get_integer_value (PRM_ID_MNT_WAITING_THREAD))
    {
      gettimeofday (&end_time, NULL);
      DIFF_TIMEVAL (start_time, end_time, elapsed_time);
    }

  if (MONITOR_WAITING_THREAD (elapsed_time))
    {
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
              ER_MNT_WAITING_THREAD, 2, "file read", prm_get_integer_value (PRM_ID_MNT_WAITING_THREAD));
      er_log_debug (ARG_FILE_LINE, "fileio_read_pages: %6d.%06d\n", elapsed_time.tv_sec, elapsed_time.tv_usec);
    }
#endif

  mnt_stats_counter_with_time (thread_p, MNT_STATS_FILE_IOREADS, 1, perf_start);
  return io_pages_p;
}

/*
 * fileio_write_pages () -
 */
void *
fileio_write_pages (THREAD_ENTRY * thread_p, int vol_fd, char *io_pages_p,
                    PAGEID page_id, int num_pages, size_t page_size)
{
#if defined (EnableThreadMonitoring)
  struct timeval start_time, end_time, elapsed_time;
#endif
  off_t offset;
  ssize_t nbytes;
  size_t write_bytes;
  UINT64 perf_start;

#if defined(USE_AIO)
  struct aiocb cb;
  const struct aiocb *cblist[1];
#endif /* USE_AIO */

  PERF_MON_GET_CURRENT_TIME (perf_start);

  assert (num_pages > 0);

  offset = FILEIO_GET_FILE_SIZE (page_size, page_id);
  write_bytes = ((size_t) page_size) * ((size_t) num_pages);

#if defined (EnableThreadMonitoring)
  if (0 < prm_get_integer_value (PRM_ID_MNT_WAITING_THREAD))
    {
      gettimeofday (&start_time, NULL);
    }
#endif

  while (write_bytes > 0)
    {
#if !defined(SERVER_MODE)
      if (lseek (vol_fd, offset, SEEK_SET) != offset)
        {
          er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
                               ER_IO_WRITE, 2, page_id, fileio_get_volume_label (fileio_get_volume_id (vol_fd), PEEK));
          return NULL;
        }

      /* write the page */
      nbytes = write (vol_fd, io_pages_p, write_bytes);
#else /* SERVER_MODE */
#if defined (USE_AIO)
      bzero (&cb, sizeof (cb));
      cb.aio_fildes = vol_fd;
      cb.aio_lio_opcode = LIO_WRITE;
      cb.aio_buf = io_pages_p;
      cb.aio_nbytes = write_bytes;
      cb.aio_offset = offset;
      cblist[0] = &cb;

      if (aio_write (&cb) < 0 || aio_suspend (cblist, 1, NULL) < 0)
        {
          nbytes = -1;
        }
      nbytes = aio_return (&cb);
#else /* USE_AIO */
      nbytes = pwrite (vol_fd, io_pages_p, write_bytes, offset);
#endif /* USE_AIO */
#endif /* SERVER_MODE */
      if (nbytes <= 0)
        {
          if (nbytes == 0)
            {
              return NULL;
            }

          switch (errno)
            {
            case EINTR:
            case EAGAIN:
              continue;
            case EOVERFLOW:
              return NULL;
            default:
              {
                er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
                                     ER_IO_WRITE, 2, page_id, fileio_get_volume_label_by_fd (vol_fd, PEEK));
                return NULL;
              }
            }
        }

      offset += nbytes;
      io_pages_p += nbytes;
      write_bytes -= nbytes;
    }

#if defined (EnableThreadMonitoring)
  if (0 < prm_get_integer_value (PRM_ID_MNT_WAITING_THREAD))
    {
      gettimeofday (&end_time, NULL);
      DIFF_TIMEVAL (start_time, end_time, elapsed_time);
    }

  if (MONITOR_WAITING_THREAD (elapsed_time))
    {
      er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
              ER_MNT_WAITING_THREAD, 2, "file write", prm_get_integer_value (PRM_ID_MNT_WAITING_THREAD));
      er_log_debug (ARG_FILE_LINE, "fileio_write_pages: %6d.%06d\n", elapsed_time.tv_sec, elapsed_time.tv_usec);
    }
#endif

  fileio_compensate_flush (thread_p, vol_fd, num_pages);
  mnt_stats_counter_with_time (thread_p, MNT_STATS_FILE_IOWRITES, num_pages, perf_start);
  return io_pages_p;
}

/*
 * fileio_writev () - WRITE A SET OF CONTIGUOUS PAGES TO DISK
 *   return: io_pgptr on success, NULL on failure
 *   vol_fd(in): Volume descriptor
 *   arrayof_io_pgptr(in): An array address to address where the current
 *                         content of pages reside
 *   start_page_id(in): Page identifier of first page
 *   npages(in): Number of consecutive pages
 *   page_size(in): Page size
 *
 * Note: Write the content of the consecutive pages described by
 *       start_pageid to disk. The content of the pages are address
 *       by the io_pgptr array. Each io_pgptr buffer is page size
 *       long.
 *
 *            io_pgptr[0]  -->> start_pageid
 *            io_pgptr[1]  -->> start_pageid + 1
 *                        ...
 *            io_pgptr[npages - 1] -->> start_pageid + npages - 1
 */
void *
fileio_writev (THREAD_ENTRY * thread_p, int vol_fd, void **io_page_array,
               PAGEID start_page_id, DKNPAGES npages, size_t page_size)
{
  int i;

  for (i = 0; i < npages; i++)
    {
      if (fileio_write (thread_p, vol_fd, io_page_array[i], start_page_id + i, page_size) == NULL)
        {
          return NULL;
        }
    }

  return io_page_array[0];
}

/*
 * fileio_synchronize () - Synchronize a database volume's state with that on disk
 *   return: vdes or NULL_VOLDES
 *   vol_fd(in): Volume descriptor
 *   vlabel(in): Volume label
 */
int
fileio_synchronize (UNUSED_ARG THREAD_ENTRY * thread_p, int vol_fd, const char *vlabel)
{
  int ret;
#if defined (EnableThreadMonitoring)
  struct timeval start_time, end_time, elapsed_time;
#endif
#if defined (SERVER_MODE)
  static pthread_mutex_t inc_cnt_mutex = PTHREAD_MUTEX_INITIALIZER;
#endif
#if defined (SERVER_MODE)
  static int inc_cnt = 0;
#endif
#if defined(USE_AIO)
  struct aiocb cb;
#endif /* USE_AIO */

#if defined (SERVER_MODE)
  if (prm_get_integer_value (PRM_ID_SUPPRESS_FSYNC) > 0)
    {
      pthread_mutex_lock (&inc_cnt_mutex);

      if (++inc_cnt >= prm_get_integer_value (PRM_ID_SUPPRESS_FSYNC))
        {
          inc_cnt = 0;
        }
      else
        {
          pthread_mutex_unlock (&inc_cnt_mutex);

          return vol_fd;
        }

      pthread_mutex_unlock (&inc_cnt_mutex);
    }
#endif

#if defined (EnableThreadMonitoring)
  if (0 < prm_get_integer_value (PRM_ID_MNT_WAITING_THREAD))
    {
      gettimeofday (&start_time, NULL);
    }
#endif

#if defined(SERVER_MODE) && defined(USE_AIO)
  bzero (&cb, sizeof (cb));
  cb.aio_fildes = vol_fd;
  ret = aio_fsync (O_SYNC, &cb);
#else /* USE_AIO */
  ret = fsync (vol_fd);
#endif /* USE_AIO */

#if defined (EnableThreadMonitoring)
  if (0 < prm_get_integer_value (PRM_ID_MNT_WAITING_THREAD))
    {
      gettimeofday (&end_time, NULL);
      DIFF_TIMEVAL (start_time, end_time, elapsed_time);
    }
#endif

  if (ret != 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IO_SYNC, 1, (vlabel ? vlabel : "Unknown"));
      return NULL_VOLDES;
    }
  else
    {
#if defined (EnableThreadMonitoring)
      if (MONITOR_WAITING_THREAD (elapsed_time))
        {
          er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE,
                  ER_MNT_WAITING_THREAD, 2, "file sync", prm_get_integer_value (PRM_ID_MNT_WAITING_THREAD));
          er_log_debug (ARG_FILE_LINE, "fileio_synchronize: %6d.%06d\n", elapsed_time.tv_sec, elapsed_time.tv_usec);
        }
#endif

#if defined(SERVER_MODE) && defined(USE_AIO)
      while (aio_error (&cb) == EINPROGRESS)
        ;
      if (aio_return (&cb) != 0)
        {
          er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IO_SYNC, 1, (vlabel ? vlabel : "Unknown"));
          return NULL_VOLDES;
        }
#endif

      mnt_stats_counter (thread_p, MNT_STATS_FILE_IOSYNCHES, 1);
      return vol_fd;
    }
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * fileio_synchronize_bg_archive_volume () -
 *   return:
 */
static int
fileio_synchronize_bg_archive_volume (THREAD_ENTRY * thread_p)
{
  APPLY_ARG arg = { 0 };

  arg.vol_id = LOG_DBLOG_BG_ARCHIVE_VOLID;
  (void) fileio_traverse_system_volume (thread_p, fileio_synchronize_sys_volume, &arg);
  return NO_ERROR;
}

/*
 * fileio_synchronize_sys_volume () -
 *   return:
 *   vol_info_p(in):
 */
static bool
fileio_synchronize_sys_volume (THREAD_ENTRY * thread_p, FILEIO_SYSTEM_VOLUME_INFO * sys_vol_info_p, APPLY_ARG * arg)
{
  bool found = false;

  if (sys_vol_info_p->vdes != NULL_VOLDES)
    {
      /* sync when match is found or
       * arg.vol_id is given as NULL_VOLID for all sys volumes.
       */
      if (arg->vol_id == NULL_VOLID)
        {
          /* fall through */
          ;
        }
      else if (sys_vol_info_p->volid == arg->vol_id)
        {
          found = true;
        }
      else
        {
          /* irrelevant volume */
          return false;
        }

      fileio_synchronize (thread_p, sys_vol_info_p->vdes, sys_vol_info_p->vlabel);
    }

  return found;
}
#endif

/*
 * fileio_synchronize_volume () -
 *   return:
 *   vol_info_p(in):
 */
static bool
fileio_synchronize_volume (THREAD_ENTRY * thread_p, FILEIO_VOLUME_INFO * vol_info_p, APPLY_ARG * arg)
{
  bool found = false;

  if (vol_info_p->vdes != NULL_VOLDES)
    {
      /* sync when match is found or
       * arg.vol_id is given as NULL_VOLID for all sys volumes.
       */
      if (arg->vol_id == NULL_VOLID)
        {
          /* fall through */
          ;
        }
      else if (vol_info_p->volid == arg->vol_id)
        {
          found = true;
        }
      else
        {
          /* irrelevant volume */
          return false;
        }

      fileio_synchronize (thread_p, vol_info_p->vdes, vol_info_p->vlabel);
    }

  return found;
}

/*
 * fileio_synchronize_all () - Synchronize all database volumes with disk
 *   return:
 *   include_log(in):
 */
int
fileio_synchronize_all (THREAD_ENTRY * thread_p, bool is_include)
{
  int success = NO_ERROR;
  APPLY_ARG arg = { 0 };

  assert (is_include == false);

  arg.vol_id = NULL_VOLID;

  er_stack_push ();

#if defined (ENABLE_UNUSED_FUNCTION)
  if (is_include)
    {
      (void) fileio_traverse_system_volume (thread_p, fileio_synchronize_sys_volume, &arg);
    }
#endif

  (void) fileio_traverse_permanent_volume (thread_p, fileio_synchronize_volume, &arg);

  if (er_errid () == ER_IO_SYNC)
    {
      success = ER_FAILED;
    }

  er_stack_pop ();

  return success;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * fileio_read_user_area () - READ A PORTION OF THE USER AREA OF THE GIVEN PAGE
 *   return: area on success, NULL on failure
 *   vdes(in): Volume descriptor
 *   pageid(in): Page identifier
 *   start_offset(in): Start offset of interested content in page
 *   nbytes(in): Length of the content of page to copy
 *   area(out):
 *
 * Note: Copy a portion of the content of the user area of the page described
 *       by pageid onto the given area. The area must be big enough to hold
 *       the needed content
 */
void *
fileio_read_user_area (THREAD_ENTRY * thread_p, int vol_fd, PAGEID page_id,
                       off_t start_offset, size_t nbytes, void *area_p)
{
  off_t offset;
  bool is_retry = true;
  FILEIO_PAGE *io_page_p;
  UINT64 perf_start;

  PERF_MON_GET_CURRENT_TIME (perf_start);

  io_page_p = (FILEIO_PAGE *) malloc (IO_PAGESIZE);
  if (io_page_p == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, IO_PAGESIZE);
      return NULL;
    }

  /* Find the offset intop the user area on the desired page */
  offset = FILEIO_GET_FILE_SIZE (IO_PAGESIZE, page_id);

  while (is_retry == true)
    {
      is_retry = false;

#if !defined(SERVER_MODE)
      /* Locate the desired page */
      if (lseek (vol_fd, offset, SEEK_SET) != offset)
        {
          if (io_page_p != NULL)
            {
              free_and_init (io_page_p);
            }

          er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
                               ER_IO_READ, 2, page_id, fileio_get_volume_label (fileio_get_volume_id (vol_fd), PEEK));
          return NULL;
        }

      /* Read the desired page */
      if (read (vol_fd, io_page_p, IO_PAGESIZE) != IO_PAGESIZE)
#else /* SERVER_MODE */
      if (pread (vol_fd, io_page_p, IO_PAGESIZE, offset) != IO_PAGESIZE)
#endif /* SERVER_MODE */
        {
          if (errno == EINTR)
            {
              is_retry = true;
            }
          else
            {
              if (io_page_p != NULL)
                {
                  free_and_init (io_page_p);
                }

              er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
                                   ER_IO_READ, 2, page_id, fileio_get_volume_label_by_fd (vol_fd, PEEK));
              return NULL;
            }
        }
    }

  memcpy (area_p, io_page_p->page + start_offset, nbytes);

  if (io_page_p != NULL)
    {
      free_and_init (io_page_p);
    }

  mnt_stats_counter_with_time (thread_p, MNT_STATS_FILE_IOREADS, 1, perf_start);
  return area_p;
}

/*
 * fileio_write_user_area () - READ A PORTION OF THE USER AREA OF THE GIVEN PAGE
 *   return: area on success, NULL on failure
 *   vdes(in): Volume descriptor
 *   pageid(in): Page identifier
 *   start_offset(in): Start offset of interested content in page
 *   nbytes(in): Length of the content of page to copy
 *   area(out):
 *
 * Note: Copy a portion of the content of the user area of the page described
 *       by pageid onto the given area. The area must be big enough to hold
 *       the needed content
 */
void *
fileio_write_user_area (THREAD_ENTRY * thread_p, int vol_fd, PAGEID page_id,
                        off_t start_offset, int nbytes, void *area_p)
{
  off_t offset;
  bool is_retry = true;
  FILEIO_PAGE *io_page_p = NULL;
  void *write_p;
  struct stat stat_buf;
  UINT64 perf_start;

  PERF_MON_GET_CURRENT_TIME (perf_start);

  if (fstat (vol_fd, &stat_buf) != 0)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
                           ER_IO_WRITE, 2, page_id, fileio_get_volume_label_by_fd (vol_fd, PEEK));
      return NULL;
    }

  if (S_ISREG (stat_buf.st_mode))       /* regular file */
    {
      /* Find the offset intop the user area on the desired page */
      offset = (FILEIO_GET_FILE_SIZE (IO_PAGESIZE, page_id) + offsetof (FILEIO_PAGE, page));

      /* Add the starting offset */
      offset += start_offset;

      write_p = area_p;

    }
  else if (S_ISCHR (stat_buf.st_mode))  /* Raw device */
    {
      offset = FILEIO_GET_FILE_SIZE (IO_PAGESIZE, page_id);
      if (nbytes != DB_PAGESIZE)
        {
          er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
                               ER_IO_WRITE, 2, page_id, fileio_get_volume_label_by_fd (vol_fd, PEEK));
          return NULL;

        }

      io_page_p = (FILEIO_PAGE *) malloc (IO_PAGESIZE);
      if (io_page_p == NULL)
        {
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, IO_PAGESIZE);
          return NULL;
        }

      LSA_SET_NULL (&io_page_p->prv.lsa);
      memcpy (io_page_p->page, area_p, nbytes);

      write_p = (void *) io_page_p;
      nbytes = IO_PAGESIZE;

    }
  else
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
                           ER_IO_WRITE, 2, page_id, fileio_get_volume_label_by_fd (vol_fd, PEEK));
      return NULL;
    }

  while (is_retry == true)
    {
      is_retry = false;

#if !defined(SERVER_MODE)
      /* Locate the desired page */
      if (lseek (vol_fd, offset, SEEK_SET) != offset)
        {
          if (io_page_p != NULL)
            {
              free_and_init (io_page_p);
            }

          er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
                               ER_IO_WRITE, 2, page_id, fileio_get_volume_label (fileio_get_volume_id (vol_fd), PEEK));
          return NULL;
        }

      /* Write desired portion to page */
      if (write (vol_fd, write_p, nbytes) != nbytes)
#else /* SERVER_MODE */
      if (pwrite (vol_fd, write_p, nbytes, offset) != nbytes)
#endif /* SERVER_MODE */
        {
          if (errno == EINTR)
            {
              is_retry = true;
            }
          else
            {
              if (errno == ENOSPC)
                {
                  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
                          ER_IO_WRITE_OUT_OF_SPACE, 2, page_id, fileio_get_volume_label_by_fd (vol_fd, PEEK));
                }
              else
                {
                  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
                                       ER_IO_WRITE, 2, page_id, fileio_get_volume_label_by_fd (vol_fd, PEEK));
                }

              if (io_page_p != NULL)
                {
                  free_and_init (io_page_p);
                }

              return NULL;
            }
        }
    }

  if (io_page_p != NULL)
    {
      free_and_init (io_page_p);
    }

  fileio_compensate_flush (thread_p, vol_fd, 1);
  mnt_stats_counter_with_time (thread_p, MNT_STATS_FILE_IOWRITES, 1, perf_start);
  return area_p;
}
#endif

/*
 * fileio_get_number_of_volume_pages () - Find the size of the volume in number of pages
 *   return: Num pages
 *   vol_fd(in): Volume descriptor
 */
DKNPAGES
fileio_get_number_of_volume_pages (int vol_fd, size_t page_size)
{
  off_t offset;

  offset = lseek (vol_fd, 0L, SEEK_END);
  return (DKNPAGES) (offset / page_size);

}

/*
 * fileio_get_number_of_partition_free_pages () - Find the number of free pages in the given
 *                               OS disk partition
 *   return: number of free pages
 *   path(in): Path to disk partition
 *
 * Note: The number of pages is in the size of the database system not
 *       the size of the OS system.
 */
int
fileio_get_number_of_partition_free_pages (const char *path_p, size_t page_size)
{
  char dir[PATH_MAX];
  int vol_fd;
  INT64 npages = -1;
  struct statfs buf;

  assert (path_p != NULL);

  rye_dirname_r (path_p, dir, PATH_MAX);
  if (rye_mkdir (dir, 0777) != true)
    {
      assert (false);
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IO_MOUNT_FAIL, 1, path_p);
      assert (npages == -1);
      return npages;
    }

  if (statfs (path_p, &buf) == -1)
    {
      if (errno == ENOENT
          && ((vol_fd = fileio_open (path_p, FILEIO_DISK_FORMAT_MODE, FILEIO_DISK_PROTECTION_MODE)) != NULL_VOLDES))
        {
          /* The given file did not exist. We create it for temporary
             consumption then it is removed */
          npages = fileio_get_number_of_partition_free_pages (path_p, page_size);
          /* Close the file and remove it */
          fileio_close (vol_fd);
          (void) rye_remove_files (path_p);
        }
      else
        {
          assert (false);
          er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IO_MOUNT_FAIL, 1, path_p);
          assert (npages == -1);
        }
    }
  else
    {
      npages = (buf.f_bavail / page_size) * ((off_t) buf.f_bsize);

      if (npages < 0 || npages > INT_MAX)
        {
          npages = INT_MAX;
        }
    }

  return (int) npages;
}

/*
 * fileio_rename () - Rename the volume from "old_vlabel" to "new_vlabel"
 *   return: new_vlabel or NULL in case of error
 *   volid(in): Volume Identifier
 *   old_vlabel(in): Old volume label
 *   new_vlabel(in): New volume label
 */
const char *
fileio_rename (UNUSED_ARG VOLID vol_id, const char *old_label_p, const char *new_label_p)
{
#if defined(RYE_DEBUG)
  if (fileio_get_volume_descriptor (vol_id) != NULL_VOLDES)
    {
      er_log_debug (ARG_FILE_LINE,
                    "fileio_rename: SYSTEM ERROR..The volume %s must" " be dismounted to rename a volume...");
      return NULL;
    }
#endif /* RYE_DEBUG */

  if (os_rename_file (old_label_p, new_label_p) != NO_ERROR)
    {
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IO_RENAME_FAIL, 2, old_label_p, new_label_p);
      return NULL;
    }
  return new_label_p;
}

/*
 * fileio_is_volume_exist () - Find if a volume exist
 *   return: true/false
 *   vlabel(in): Volume label
 */
bool
fileio_is_volume_exist (const char *vol_label_p)
{
  int vol_fd;

#if !defined(CS_MODE)
  /* Is volume already mounted ? */
  vol_fd = fileio_find_volume_descriptor_with_label (vol_label_p);
  if (vol_fd != NULL_VOLDES)
    {
      return true;
    }
#endif /* !CS_MODE */

  /* Check the existance of the file by opening the file */
  vol_fd = fileio_open (vol_label_p, O_RDONLY, 0);
  if (vol_fd == NULL_VOLDES)
    {
      if (errno == ENOENT)
        {
          return false;
        }
    }
  else
    {
      fileio_close (vol_fd);
    }

  return true;
}

/*
 * fileio_is_volume_exist_and_file () - Find if a volume exist and is a regular file
 *   return: true/false
 *   vlabel(in): Volume label
 *
 * Note:  This is to differentiate between directories, raw devices, and files.
 */
bool
fileio_is_volume_exist_and_file (const char *vol_label_p)
{
  int vol_fd;
  struct stat stbuf;

  /* Is volume already mounted ? */
  vol_fd = fileio_find_volume_descriptor_with_label (vol_label_p);
  if (vol_fd != NULL_VOLDES)
    {
      return true;
    }

  if (stat (vol_label_p, &stbuf) != -1 && S_ISREG (stbuf.st_mode))
    {
      return true;
    }

  return false;
}

static char *
fileio_check_file_exist (char *name_p, char *new_guess_path_p, int check_size, int *max_name_size_p)
{
  char *tmp_name_p;
  int vol_fd = NULL_VOLDES;

  tmp_name_p = name_p - (*max_name_size_p - check_size + 1);
  *tmp_name_p = '\0';

  vol_fd = fileio_open (new_guess_path_p, O_RDONLY, 0);
  if (vol_fd == NULL_VOLDES)
    {
      vol_fd = fileio_open (new_guess_path_p, FILEIO_DISK_FORMAT_MODE, FILEIO_DISK_PROTECTION_MODE);
      if (vol_fd == NULL_VOLDES && errno == ENAMETOOLONG)
        {
          *max_name_size_p = check_size + 1;
          name_p = tmp_name_p;
        }
      else
        {
          if (vol_fd != NULL_VOLDES)
            {
              fileio_close (vol_fd);
              (void) rye_remove_files (new_guess_path_p);
            }
          *tmp_name_p = 'x';
        }
    }
  else
    {
      *tmp_name_p = 'x';
      if (vol_fd != NULL_VOLDES)
        {
          fileio_close (vol_fd);
        }
    }

  return name_p;
}

static char *
fileio_check_file_is_same (char *name_p, char *new_guess_path_p, int check_size, int *max_name_size_p, struct stat *buf)
{
  char *tmp_name_p;

  tmp_name_p = name_p - (*max_name_size_p - check_size + 1);
  *tmp_name_p = '\0';

  if (stat (new_guess_path_p, &buf[1]) == 0 && buf[0].st_ino == buf[1].st_ino)
    {
      *max_name_size_p = check_size + 1;
      name_p = tmp_name_p;
    }
  else
    {
      *tmp_name_p = 'x';
    }

  return name_p;
}

/*
 * fileio_get_primitive_way_max () - Find the longest names of files and
 *                                          path names that can be given for
 *                                          the given file system (path) in a
 *                                          primitive way
 *   return: filename max
 *   path(in): Path to directory or file name
 *   filename_max(out): the longest name that could be given
 *   pathname_max(out): the longest path that could be given
 *
 * Note: This function should only be used when the values cannot be
 *       determine using pathconf.
 */
static int
fileio_get_primitive_way_max (const char *path_p, long int *file_name_max_p, long int *path_name_max_p)
{
  static char last_guess_path[PATH_MAX] = { '\0' };
  static int max_name_size = -1;
  char new_guess_path[PATH_MAX];
  char *name_p;
  int check256, check14;
  bool is_remove = false;
  int vol_fd = NULL_VOLDES;
  struct stat buf[2];
  int i;
  int success;

  *file_name_max_p = NAME_MAX;
  *path_name_max_p = PATH_MAX;

  if (*file_name_max_p > *path_name_max_p)
    {
      *file_name_max_p = *path_name_max_p;
    }

  /* Verify the above compilation guesses */

  strncpy (new_guess_path, path_p, PATH_MAX);
  new_guess_path[PATH_MAX - 1] = '\0';
  name_p = strrchr (new_guess_path, '/');

  if (name_p != NULL)
    {
      *++name_p = '\0';
    }
  else
    {
      name_p = new_guess_path;
    }

  if (max_name_size != -1 && strcmp (last_guess_path, new_guess_path) == 0)
    {
      return *file_name_max_p = max_name_size;
    }

  for (max_name_size = 1, i = (int) strlen (new_guess_path) + 1;
       max_name_size < *file_name_max_p && i < *path_name_max_p; max_name_size++, i++)
    {
      *name_p++ = 'x';
    }

  *name_p++ = '\0';

  /* Start from the back until you find a file which is different.
     The assumption is that the files do not exist. */

  check256 = 1;
  check14 = 1;
  while (max_name_size > 1)
    {
      vol_fd = fileio_open (new_guess_path, O_RDONLY, 0);
      if (vol_fd != NULL_VOLDES)
        {
          /* The file already exist */
          is_remove = false;
          break;
        }
      else
        {
          /* The file did not exist.
             Create the file and at the end remove the file */
          is_remove = true;
          vol_fd = fileio_open (new_guess_path, FILEIO_DISK_FORMAT_MODE, FILEIO_DISK_PROTECTION_MODE);
          if (vol_fd != NULL_VOLDES)
            {
              break;
            }

          if (errno != ENAMETOOLONG)
            {
              goto error;
            }

          /*
           * Name truncation is not allowed. Most Unix systems accept
           * filename of 256 or 14.
           * Assume one of this for now
           */
          if (max_name_size > 257 && check256 == 1)
            {
              check256 = 0;
              name_p = fileio_check_file_exist (name_p, new_guess_path, 256, &max_name_size);
            }
          else if (max_name_size > 15 && check14 == 1)
            {
              check14 = 0;
              name_p = fileio_check_file_exist (name_p, new_guess_path, 14, &max_name_size);
            }
          *name_p-- = '\0';
          max_name_size--;
        }
    }

  STRNCPY (last_guess_path, new_guess_path, PATH_MAX);

  if (vol_fd != NULL_VOLDES)
    {
      fileio_close (vol_fd);
      if (stat (new_guess_path, &buf[0]) == -1)
        {
          goto error;
        }
    }
  else
    {
      goto error;
    }

  /*
   * Most Unix system are either 256 or 14. Do a quick check to see if 15
   * is the same than current value. If it is, set maxname to 15 and decrement
   * name.
   */

  check256 = 1;
  check14 = 1;
  for (; max_name_size > 1; max_name_size--)
    {
      *name_p-- = '\0';
      if ((success = stat (new_guess_path, &buf[1])) == 0 && buf[0].st_ino == buf[1].st_ino)
        {
          /*
           * Same file. Most Unix system allow either 256 or 14 for filenames.
           * Perform a quick check to see if we can speed up the checking
           * process
           */

          if (max_name_size > 257 && check256 == 1)
            {
              check256 = 0;
              name_p = fileio_check_file_is_same (name_p, new_guess_path, 256, &max_name_size, buf);
              /* Check if the name with 257 is the same. If it is advance the
                 to 256 */
            }
          else if (max_name_size > 15 && check14 == 1)
            {
              check14 = 0;
              name_p = fileio_check_file_is_same (name_p, new_guess_path, 14, &max_name_size, buf);
            }
        }
      else
        {
          if (success == 0)
            {
              continue;
            }
          else if (errno == ENOENT)
            {
              /* The file did not exist or the file is different. Therefore,
                 previous maxname is  the maximum name */
              max_name_size++;
              break;
            }

          goto error;
        }
    }

  /* The length has been found */
  if (is_remove == true)
    {
      (void) rye_remove_files (last_guess_path);
    }

  name_p = strrchr (last_guess_path, '/');
  if (name_p != NULL)
    {
      *++name_p = '\0';
    }

  /* Plus 2 since we start with zero and we need to include null character */
  max_name_size = max_name_size + 2;

  return *file_name_max_p = max_name_size;

error:
  if (is_remove == true)
    {
      (void) rye_remove_files (last_guess_path);
    }

  max_name_size = -1;
  *path_name_max_p = -1;
  *file_name_max_p = -1;

  return -1;
}

/*
 * fileio_get_max_name () - Find the longest names of files and path
 *                                 names that can be given for the given file
 *                                 system (path)
 *   return: filename max
 *   path(in): Path to directory or file name
 *   filename_max(out): the longest name that could be given
 *   pathname_max(out): the longest path that could be given
 *
 * Note: The main goal of this function is to respect the limits that
 *       the database system is using at compile time (e.g., variables
 *       defined with PATH_MAX) and at run time. For example, if
 *       the constant FILENAME_MAX cannot be used to detect long names
 *       since this value at compilation time may be different from the
 *       value at execution time (e.g., at other installation). If we
 *       use the compiled value, it may be possible that we will be
 *       removing a file when a new one is created when truncation of
 *       filenames is allowed at the running file system.
 *       In addition, it is possible that such limits may differ across
 *       file systems, device boundaries. For example, Unix System V
 *       uses a maximum of 14 characters for file names, and Unix BSD
 *       uses 255. On this implementations, we are forced to use 14
 *       characters.
 *       The functions returns the minimum of the compilation and run
 *       time for both filename and pathname.
 */
int
fileio_get_max_name (const char *given_path_p, long int *file_name_max_p, long int *path_name_max_p)
{
  char new_path[PATH_MAX];
  const char *path_p;
  char *name_p;
  struct stat stbuf;

  /* Errno need to be reset to find out if the values are not handle */
  errno = 0;
  path_p = given_path_p;

  *file_name_max_p = pathconf ((char *) path_p, _PC_NAME_MAX);
  *path_name_max_p = pathconf ((char *) path_p, _PC_PATH_MAX);

  if ((*file_name_max_p < 0 || *path_name_max_p < 0) && (errno == ENOENT || errno == EINVAL))
    {
      /*
       * The above values may not be accepted for that path. The path may be
       * a file instead of a directory, try it with the directory since some
       * implementations cannot answer the above question when the path is a
       * file
       */

      if (stat (path_p, &stbuf) != -1 && ((stbuf.st_mode & S_IFMT) != S_IFDIR))
        {
          /* Try it with the directory instead */
          strncpy (new_path, given_path_p, PATH_MAX);
          new_path[PATH_MAX - 1] = '\0';
          name_p = strrchr (new_path, '/');
          if (name_p != NULL)
            {
              *name_p = '\0';
            }
          path_p = new_path;

          *file_name_max_p = pathconf ((char *) path_p, _PC_NAME_MAX);
          *path_name_max_p = pathconf ((char *) path_p, _PC_PATH_MAX);

          path_p = given_path_p;
        }
    }

  if (*file_name_max_p < 0 || *path_name_max_p < 0)
    {
      /* If errno is zero, the values are indeterminate */
      (void) fileio_get_primitive_way_max (path_p, file_name_max_p, path_name_max_p);
    }

  /* Make sure that we do not overpass compilation structures */
  if (*file_name_max_p < 0 || *file_name_max_p > NAME_MAX)
    {
      *file_name_max_p = NAME_MAX;
    }

  if (*path_name_max_p < 0 || *path_name_max_p > PATH_MAX)
    {
      *path_name_max_p = PATH_MAX;
    }

  return *file_name_max_p;
}

/*
 * fileio_get_base_file_name () - Find start of basename in given filename
 *   return: basename
 *   fullname(in): Fullname of file
 */
const char *
fileio_get_base_file_name (const char *full_name_p)
{
  const char *no_path_name_p;

  no_path_name_p = strrchr (full_name_p, PATH_SEPARATOR);
  if (no_path_name_p == NULL)
    {
      no_path_name_p = full_name_p;
    }
  else
    {
      no_path_name_p++;         /* Skip to the name */
    }

  return no_path_name_p;
}

/*
 * fileio_get_directory_path () - Find directory path of given file. That is copy all but the
 *                basename of filename
 *   return: path
 *   path(out): The path of the file
 *   fullname(in): Fullname of file
 */
char *
fileio_get_directory_path (char *path_p, const char *full_name_p)
{
  const char *base_p;
  size_t path_size;

  base_p = fileio_get_base_file_name (full_name_p);

  assert (base_p >= full_name_p);

  if (base_p == full_name_p)
    {
#if 1                           /* TODO - trace */
      assert (false);           /* should be impossible */
#else
      /* Same pointer, the file does not contain a path/directory portion. Use
         the current directory */
      if (getcwd (path_p, PATH_MAX) == NULL)
#endif
        {
          er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_BO_CWD_FAIL, 0);
          *path_p = '\0';
        }
    }
  else
    {
      path_size = (size_t) (base_p - full_name_p - 1);
      if (path_size > PATH_MAX)
        {
          path_size = PATH_MAX;
        }
      memcpy (path_p, full_name_p, path_size);
      path_p[path_size] = '\0';
    }

  return path_p;
}

/*
 * fileio_get_volume_max_suffix () -
 *   return:
 */
int
fileio_get_volume_max_suffix (void)
{
  return FILEIO_MAX_SUFFIX_LENGTH;
}

/*
 * fileio_make_volume_info_name () - Build the name of volumes
 *   return: void
 *   volinfo_name(out):
 *   db_fullname(in):
 *
 * Note: The caller must have enough space to store the name of the volume
 *       that is constructed(sprintf). It is recommended to have at least
 *       DB_MAX_PATH_LENGTH length.
 */
void
fileio_make_volume_info_name (char *vol_info_name_p, const char *db_full_name_p)
{
  sprintf (vol_info_name_p, "%s%s", db_full_name_p, FILEIO_VOLINFO_SUFFIX);
}

/*
 * fileio_make_volume_ext_name () - Build the name of volumes
 *   return: void
 *   volext_fullname(out):
 *   ext_path(in):
 *   ext_name(in):
 *   volid(in):
 *
 * Note: The caller must have enough space to store the name of the volume
 *       that is constructed(sprintf). It is recommended to have at least
 *       DB_MAX_PATH_LENGTH length.
 */
void
fileio_make_volume_ext_name (char *vol_ext_full_name_p, const char *ext_path_p, const char *ext_name_p, VOLID vol_id)
{
  assert (vol_id >= 0);

  if (vol_id == 0)
    {
      sprintf (vol_ext_full_name_p, "%s%s%s", ext_path_p, FILEIO_PATH_SEPARATOR (ext_path_p), ext_name_p);
    }
  else
    {
      sprintf (vol_ext_full_name_p, "%s%s%s%s%03d",
               ext_path_p, FILEIO_PATH_SEPARATOR (ext_path_p), ext_name_p, FILEIO_VOLEXT_PREFIX, vol_id);
    }
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * fileio_make_volume_ext_given_name () - Build the name of volumes
 *   return: void
 *   volext_fullname(out):
 *   ext_path(in):
 *   ext_name(in):
 *
 * Note: The caller must have enough space to store the name of the volume
 *       that is constructed(sprintf). It is recommended to have at least
 *       DB_MAX_PATH_LENGTH length.
 */
void
fileio_make_volume_ext_given_name (char *vol_ext_full_name_p, const char *ext_path_p, const char *ext_name_p)
{
  sprintf (vol_ext_full_name_p, "%s%s%s", ext_path_p, FILEIO_PATH_SEPARATOR (ext_path_p), ext_name_p);
}
#endif

/*
 * fileio_make_volume_temp_name () - Build the name of volumes
 *   return: void
 *   voltmp_fullname(out):
 *   tmp_path(in):
 *   tmp_name(in):
 *   volid(in):
 *
 * Note: The caller must have enough space to store the name of the volume
 *       that is constructed(sprintf). It is recommended to have at least
 *       DB_MAX_PATH_LENGTH length.
 */
void
fileio_make_volume_temp_name (char *vol_tmp_full_name_p, const char *tmp_path_p, const char *tmp_name_p, VOLID vol_id)
{
  sprintf (vol_tmp_full_name_p, "%s%c%s%s%03d", tmp_path_p, PATH_SEPARATOR, tmp_name_p, FILEIO_VOLTMP_PREFIX, vol_id);
}

/*
 * fileio_make_log_active_name () - Build the name of volumes
 *   return: void
 *   logactive_name(out):
 *   log_path(in):
 *   dbname(in):
 *
 * Note: The caller must have enough space to store the name of the volume
 *       that is constructed(sprintf). It is recommended to have at least
 *       DB_MAX_PATH_LENGTH length.
 */
void
fileio_make_log_active_name (char *log_active_name_p, const char *log_path_p, const char *db_name_p)
{
  sprintf (log_active_name_p, "%s%s%s%s",
           log_path_p, FILEIO_PATH_SEPARATOR (log_path_p), db_name_p, FILEIO_SUFFIX_LOGACTIVE);
}

/*
 * fileio_make_log_active_temp_name () - Build the name of volumes
 *   return: void
 *   logactive_name(out):
 *   active_name(in):
 *
 * Note: The caller must have enough space to store the name of the volume
 *       that is constructed(sprintf). It is recommended to have at least
 *       DB_MAX_PATH_LENGTH length.
 */
void
fileio_make_log_active_temp_name (char *log_active_name_p, const char *active_name_p)
{
  sprintf (log_active_name_p, "%s_tmp", active_name_p);
}

/*
 * fileio_make_log_archive_name () - Build the name of volumes
 *   return: void
 *   logarchive_name(out):
 *   log_path(in):
 *   dbname(in):
 *   arvnum(in):
 *
 * Note: The caller must have enough space to store the name of the volume
 *       that is constructed(sprintf). It is recommended to have at least
 *       DB_MAX_PATH_LENGTH length.
 */
void
fileio_make_log_archive_name (char *log_archive_name_p,
                              const char *log_path_p, const char *db_name_p, int archive_number)
{
  sprintf (log_archive_name_p, "%s%s%s%s%03d",
           log_path_p, FILEIO_PATH_SEPARATOR (log_path_p), db_name_p, FILEIO_SUFFIX_LOGARCHIVE, archive_number);
}

/*
 * fileio_make_removed_log_archive_name () - Build the name of removed volumes
 *   return: void
 *   logarchive_name(out):
 *   log_path(in):
 *   dbname(in):
 *
 * Note: The caller must have enough space to store the name of the volume
 *       that is constructed(sprintf). It is recommended to have at least
 *       DB_MAX_PATH_LENGTH length.
 */
void
fileio_make_removed_log_archive_name (char *log_archive_name_p, const char *log_path_p, const char *db_name_p)
{
  sprintf (log_archive_name_p, "%s%s%s%s.removed",
           log_path_p, FILEIO_PATH_SEPARATOR (log_path_p), db_name_p, FILEIO_SUFFIX_LOGARCHIVE);
}

/*
 * fileio_make_log_archive_temp_name () -
 *   return: void
 *   logarchive_name_p(out):
 *   log_path_p(in):
 *   db_name_p(in):
 *
 * Note:
 */
void
fileio_make_log_archive_temp_name (char *log_archive_temp_name_p, const char *log_path_p, const char *db_name_p)
{
  const char *fmt_string_p;

  fmt_string_p = "%s%s%s%s";

  snprintf (log_archive_temp_name_p, PATH_MAX - 1, fmt_string_p,
            log_path_p, FILEIO_PATH_SEPARATOR (log_path_p), db_name_p, FILEIO_SUFFIX_TMP_LOGARCHIVE);
}

/*
 * fileio_make_log_info_name () - Build the name of volumes
 *   return: void
 *   loginfo_name(out):
 *   log_path(in):
 *   dbname(in):
 *
 * Note: The caller must have enough space to store the name of the volume
 *       that is constructed(sprintf). It is recommended to have at least
 *       DB_MAX_PATH_LENGTH length.
 */
void
fileio_make_log_info_name (char *log_info_name_p, const char *log_path_p, const char *db_name_p)
{
  sprintf (log_info_name_p, "%s%s%s%s",
           log_path_p, FILEIO_PATH_SEPARATOR (log_path_p), db_name_p, FILEIO_SUFFIX_LOGINFO);
}

#if !defined(CS_MODE)
/*
 * fileio_cache () - Cache information related to a mounted volume
 *   return: vdes on success, NULL_VOLDES on failure
 *   volid(in): Permanent volume identifier
 *   vlabel(in): Name/label of the volume
 *   vdes(in): I/O volume descriptor
 *   lockf_type(in): Type of lock
 */
static int
fileio_cache (VOLID vol_id, const char *vol_label_p, int vol_fd, FILEIO_LOCKF_TYPE lockf_type)
{
  bool is_permanent_volume;
  FILEIO_VOLUME_INFO *vol_info_p;
  FILEIO_SYSTEM_VOLUME_INFO *sys_vol_info_p;
  int i, j;

  FILEIO_CHECK_AND_INITIALIZE_VOLUME_HEADER_CACHE (NULL_VOLDES);

  if (vol_id > NULL_VOLID)
    {
      /* perm volume */
      if (vol_id < fileio_Vol_info_header.next_temp_volid)
        {
          i = vol_id / FILEIO_VOLINFO_INCREMENT;
          j = vol_id % FILEIO_VOLINFO_INCREMENT;
          if (vol_id >= fileio_Vol_info_header.max_perm_vols
              && fileio_expand_permanent_volume_info (&fileio_Vol_info_header, vol_id) < 0)
            {
              return NULL_VOLDES;
            }
          is_permanent_volume = true;
        }
      else
        {
          /* volid is the next temp volume id */
          i = (fileio_Vol_info_header.num_volinfo_array - 1 - (LOG_MAX_DBVOLID - vol_id) / FILEIO_VOLINFO_INCREMENT);
          j = (FILEIO_VOLINFO_INCREMENT - 1 - (LOG_MAX_DBVOLID - vol_id) % FILEIO_VOLINFO_INCREMENT);
          if (((LOG_MAX_DBVOLID - vol_id) >=
               fileio_Vol_info_header.max_temp_vols)
              && fileio_expand_temporary_volume_info (&fileio_Vol_info_header, vol_id) < 0)
            {
              return NULL_VOLDES;
            }
          is_permanent_volume = false;
        }

      vol_info_p = &fileio_Vol_info_header.volinfo[i][j];
      vol_info_p->volid = vol_id;
      vol_info_p->vdes = vol_fd;
      vol_info_p->lockf_type = lockf_type;
      strncpy (vol_info_p->vlabel, vol_label_p, PATH_MAX);
      /* modify next volume id */
      pthread_mutex_lock (&fileio_Vol_info_header.mutex);
      if (is_permanent_volume)
        {
          if (fileio_Vol_info_header.next_perm_volid <= vol_id)
            {
              fileio_Vol_info_header.next_perm_volid = vol_id + 1;
            }
        }
      else
        {
          if (fileio_Vol_info_header.next_temp_volid >= vol_id)
            {
              fileio_Vol_info_header.next_temp_volid = vol_id - 1;
            }
        }
      pthread_mutex_unlock (&fileio_Vol_info_header.mutex);
    }
  else
    {
      /* system volume */
      pthread_mutex_lock (&fileio_Sys_vol_info_header.mutex);
      if (fileio_Sys_vol_info_header.anchor.vdes != NULL_VOLDES)
        {
          sys_vol_info_p = (FILEIO_SYSTEM_VOLUME_INFO *) malloc (sizeof (FILEIO_SYSTEM_VOLUME_INFO));
          if (sys_vol_info_p == NULL)
            {
              er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
                      ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (FILEIO_SYSTEM_VOLUME_INFO));
              vol_fd = NULL_VOLDES;
            }
          else
            {
              sys_vol_info_p->volid = vol_id;
              sys_vol_info_p->vdes = vol_fd;
              sys_vol_info_p->lockf_type = lockf_type;
              STRNCPY (sys_vol_info_p->vlabel, vol_label_p, PATH_MAX);
              sys_vol_info_p->next = fileio_Sys_vol_info_header.anchor.next;
              fileio_Sys_vol_info_header.anchor.next = sys_vol_info_p;
              fileio_Sys_vol_info_header.num_vols++;
            }
        }
      else
        {
          sys_vol_info_p = &fileio_Sys_vol_info_header.anchor;
          sys_vol_info_p->volid = vol_id;
          sys_vol_info_p->vdes = vol_fd;
          sys_vol_info_p->lockf_type = lockf_type;
          sys_vol_info_p->next = NULL;
          STRNCPY (sys_vol_info_p->vlabel, vol_label_p, PATH_MAX);
          fileio_Sys_vol_info_header.num_vols++;
        }

      pthread_mutex_unlock (&fileio_Sys_vol_info_header.mutex);
    }

  return vol_fd;
}
#endif

/*
 * fileio_decache () - Decache volume information. Used when the volume is
 *                 dismounted
 *   return: void
 *   vdes(in): I/O Volume descriptor
 */
void
fileio_decache (THREAD_ENTRY * thread_p, int vol_fd)
{
  FILEIO_SYSTEM_VOLUME_INFO *sys_vol_info_p, *prev_sys_vol_info_p;
  FILEIO_VOLUME_INFO *vol_info_p;
  int vol_id;
  APPLY_ARG arg = { 0 };

  pthread_mutex_lock (&fileio_Sys_vol_info_header.mutex);
  /* sys volume ? */
  for ((sys_vol_info_p =
        &fileio_Sys_vol_info_header.anchor, prev_sys_vol_info_p =
        NULL);
       (sys_vol_info_p != NULL
        && sys_vol_info_p->vdes != NULL_VOLDES);
       prev_sys_vol_info_p = sys_vol_info_p, sys_vol_info_p = sys_vol_info_p->next)
    {
      if (sys_vol_info_p->vdes == vol_fd)
        {
          if (prev_sys_vol_info_p == NULL)
            {
              if (fileio_Sys_vol_info_header.anchor.next != NULL)
                {
                  /* copy next volinfo to anchor. */
                  sys_vol_info_p = fileio_Sys_vol_info_header.anchor.next;
                  fileio_Sys_vol_info_header.anchor.volid = sys_vol_info_p->volid;
                  fileio_Sys_vol_info_header.anchor.vdes = sys_vol_info_p->vdes;
                  fileio_Sys_vol_info_header.anchor.lockf_type = sys_vol_info_p->lockf_type;
                  strncpy (fileio_Sys_vol_info_header.anchor.vlabel, sys_vol_info_p->vlabel, PATH_MAX);
                  fileio_Sys_vol_info_header.anchor.next = sys_vol_info_p->next;
                  free_and_init (sys_vol_info_p);
                }
              else
                {
                  fileio_Sys_vol_info_header.anchor.volid = NULL_VOLID;
                  fileio_Sys_vol_info_header.anchor.vdes = NULL_VOLDES;
                  fileio_Sys_vol_info_header.anchor.lockf_type = FILEIO_NOT_LOCKF;
                  fileio_Sys_vol_info_header.anchor.vlabel[0] = '\0';
                  fileio_Sys_vol_info_header.anchor.next = NULL;
                }
            }
          else
            {
              prev_sys_vol_info_p->next = sys_vol_info_p->next;
              free_and_init (sys_vol_info_p);
            }
          fileio_Sys_vol_info_header.num_vols--;
          pthread_mutex_unlock (&fileio_Sys_vol_info_header.mutex);
          return;
        }
    }
  pthread_mutex_unlock (&fileio_Sys_vol_info_header.mutex);
  arg.vdes = vol_fd;
  vol_info_p = fileio_traverse_permanent_volume (thread_p, fileio_is_volume_descriptor_equal, &arg);
  if (vol_info_p)
    {
      vol_id = vol_info_p->volid;
      vol_info_p->volid = NULL_VOLID;
      vol_info_p->vdes = NULL_VOLDES;
      vol_info_p->lockf_type = FILEIO_NOT_LOCKF;
      vol_info_p->vlabel[0] = '\0';

      /* update next_perm_volid, if needed */
      pthread_mutex_lock (&fileio_Vol_info_header.mutex);
      if (fileio_Vol_info_header.next_perm_volid == vol_id + 1)
        {
          fileio_Vol_info_header.next_perm_volid = vol_id;
        }

      pthread_mutex_unlock (&fileio_Vol_info_header.mutex);
      return;
    }

  arg.vdes = vol_fd;
  vol_info_p = fileio_traverse_temporary_volume (thread_p, fileio_is_volume_descriptor_equal, &arg);
  if (vol_info_p)
    {
      vol_id = vol_info_p->volid;
      vol_info_p->volid = NULL_VOLID;
      vol_info_p->vdes = NULL_VOLDES;
      vol_info_p->lockf_type = FILEIO_NOT_LOCKF;
      vol_info_p->vlabel[0] = '\0';

      /* update next_perm_volid, if needed */
      pthread_mutex_lock (&fileio_Vol_info_header.mutex);
      if (fileio_Vol_info_header.next_perm_volid == vol_id - 1)
        {
          fileio_Vol_info_header.next_perm_volid = vol_id;
        }

      pthread_mutex_unlock (&fileio_Vol_info_header.mutex);
      return;
    }
}

/*
 * fileio_get_volume_label ()
 *  - Find the name of a mounted volume given its permanent volume identifier
 *   return: Volume label
 *   volid(in): Permanent volume identifier
 */
char *
fileio_get_volume_label (VOLID vol_id, bool is_peek)
{
  FILEIO_VOLUME_INFO *vol_info_p;
  FILEIO_SYSTEM_VOLUME_INFO *sys_vol_info_p;
  char *vol_label_p = NULL;
  int i, j;
  APPLY_ARG arg = { 0 };

  FILEIO_CHECK_AND_INITIALIZE_VOLUME_HEADER_CACHE (NULL);
  if (vol_id > NULL_VOLID)
    {
      /* perm volume */
      if (vol_id < fileio_Vol_info_header.next_temp_volid)
        {
          if (vol_id >= fileio_Vol_info_header.max_perm_vols)
            {
              return NULL;
            }

          i = vol_id / FILEIO_VOLINFO_INCREMENT;
          j = vol_id % FILEIO_VOLINFO_INCREMENT;
        }
      else
        {
          /* volid is the next temp volume id */
          if ((LOG_MAX_DBVOLID - vol_id) >= fileio_Vol_info_header.max_temp_vols)
            {
              return NULL;
            }

          i = fileio_Vol_info_header.num_volinfo_array - 1 - (LOG_MAX_DBVOLID - vol_id) / FILEIO_VOLINFO_INCREMENT;
          j = FILEIO_VOLINFO_INCREMENT - 1 - (LOG_MAX_DBVOLID - vol_id) % FILEIO_VOLINFO_INCREMENT;
        }
      vol_info_p = &fileio_Vol_info_header.volinfo[i][j];
      vol_label_p = (char *) vol_info_p->vlabel;
    }
  else
    {
      /* system volume */
      pthread_mutex_lock (&fileio_Sys_vol_info_header.mutex);
      arg.vol_id = vol_id;
      sys_vol_info_p = fileio_find_system_volume (NULL, fileio_is_system_volume_id_equal, &arg);
      if (sys_vol_info_p)
        {
          vol_label_p = (char *) sys_vol_info_p->vlabel;
        }

      pthread_mutex_unlock (&fileio_Sys_vol_info_header.mutex);
    }

  if (!is_peek)
    {
      char *ret = NULL;

      if (vol_label_p != NULL)
        {
          ret = strdup (vol_label_p);

          if (ret == NULL)
            {
              er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, strlen (vol_label_p));
            }
        }

      return ret;
    }

  return vol_label_p;
}

/*
 * fileio_get_volume_label_by_fd ()
 *   - Find the name of a mounted volume given its file descriptor
 *   return: Volume label
 *   vol_fd(in): volume descriptor
 */
char *
fileio_get_volume_label_by_fd (int vol_fd, bool is_peek)
{
  return fileio_get_volume_label (fileio_get_volume_id (vol_fd), is_peek);
}


/*
 * fileio_get_volume_id () - Find volume identifier of a mounted volume given its
 *               descriptor
 *   return: The volume identifier
 *   vdes(in): I/O volume descriptor
 */
static VOLID
fileio_get_volume_id (int vol_fd)
{
  FILEIO_VOLUME_INFO *vol_info_p;
  FILEIO_SYSTEM_VOLUME_INFO *sys_vol_info_p;
  VOLID vol_id = NULL_VOLID;
  APPLY_ARG arg = { 0 };

  FILEIO_CHECK_AND_INITIALIZE_VOLUME_HEADER_CACHE (NULL_VOLID);
  /* sys volume ? */
  pthread_mutex_lock (&fileio_Sys_vol_info_header.mutex);
  arg.vdes = vol_fd;
  sys_vol_info_p = fileio_find_system_volume (NULL, fileio_is_system_volume_descriptor_equal, &arg);
  if (sys_vol_info_p)
    {
      vol_id = sys_vol_info_p->volid;
      pthread_mutex_unlock (&fileio_Sys_vol_info_header.mutex);
      return vol_id;
    }

  pthread_mutex_unlock (&fileio_Sys_vol_info_header.mutex);

  arg.vdes = vol_fd;
  vol_info_p = fileio_traverse_permanent_volume (NULL, fileio_is_volume_descriptor_equal, &arg);
  if (vol_info_p)
    {
      return vol_info_p->volid;
    }

  arg.vdes = vol_fd;
  vol_info_p = fileio_traverse_temporary_volume (NULL, fileio_is_volume_descriptor_equal, &arg);
  if (vol_info_p)
    {
      return vol_info_p->volid;
    }

  return vol_id;
}

static bool
fileio_is_volume_label_equal (UNUSED_ARG THREAD_ENTRY * thread_p, FILEIO_VOLUME_INFO * vol_info_p, APPLY_ARG * arg)
{
  return (util_compare_filepath (vol_info_p->vlabel, arg->vol_label) == 0);
}

/*
 * fileio_find_volume_id_with_label () - Find the volume identifier given the volume label of
 *                      a mounted volume
 *   return: The volume identifier
 *   vlabel(in): Volume Name/label
 */
VOLID
fileio_find_volume_id_with_label (THREAD_ENTRY * thread_p, const char *vol_label_p)
{
  FILEIO_VOLUME_INFO *vol_info_p;
  FILEIO_SYSTEM_VOLUME_INFO *sys_vol_info_p;
  VOLID vol_id = NULL_VOLID;
  APPLY_ARG arg = { 0 };

  FILEIO_CHECK_AND_INITIALIZE_VOLUME_HEADER_CACHE (NULL_VOLID);
  pthread_mutex_lock (&fileio_Sys_vol_info_header.mutex);
  arg.vol_label = vol_label_p;
  sys_vol_info_p = fileio_find_system_volume (thread_p, fileio_is_system_volume_label_equal, &arg);
  if (sys_vol_info_p)
    {
      vol_id = sys_vol_info_p->volid;
      pthread_mutex_unlock (&fileio_Sys_vol_info_header.mutex);
      return vol_id;
    }

  pthread_mutex_unlock (&fileio_Sys_vol_info_header.mutex);

  arg.vol_label = vol_label_p;
  vol_info_p = fileio_traverse_permanent_volume (thread_p, fileio_is_volume_label_equal, &arg);
  if (vol_info_p)
    {
      return vol_info_p->volid;
    }

  arg.vol_label = vol_label_p;
  vol_info_p = fileio_traverse_temporary_volume (thread_p, fileio_is_volume_label_equal, &arg);
  if (vol_info_p)
    {
      return vol_info_p->volid;
    }

  return vol_id;
}

/*
 * fileio_get_volume_descriptor () - Find the volume descriptor given the volume permanent
 *              identifier
 *   return: I/O volume descriptor
 *   volid(in): Permanent volume identifier
 */
int
fileio_get_volume_descriptor (VOLID vol_id)
{
  FILEIO_VOLUME_INFO *vol_info_p;
  FILEIO_SYSTEM_VOLUME_INFO *sys_vol_info_p;
  int vol_fd = NULL_VOLDES;
  int i, j;
  APPLY_ARG arg = { 0 };

  FILEIO_CHECK_AND_INITIALIZE_VOLUME_HEADER_CACHE (NULL_VOLDES);
  if (vol_id > NULL_VOLID)
    {
      /* perm volume */
      if (vol_id < fileio_Vol_info_header.next_temp_volid)
        {
          if (vol_id >= fileio_Vol_info_header.max_perm_vols)
            {
              return NULL_VOLDES;
            }
          i = vol_id / FILEIO_VOLINFO_INCREMENT;
          j = vol_id % FILEIO_VOLINFO_INCREMENT;
        }
      else
        {
          /* volid is the next temp volume id */
          if ((LOG_MAX_DBVOLID - vol_id) >= fileio_Vol_info_header.max_temp_vols)
            {
              return NULL_VOLDES;
            }
          i = fileio_Vol_info_header.num_volinfo_array - 1 - (LOG_MAX_DBVOLID - vol_id) / FILEIO_VOLINFO_INCREMENT;
          j = FILEIO_VOLINFO_INCREMENT - 1 - (LOG_MAX_DBVOLID - vol_id) % FILEIO_VOLINFO_INCREMENT;
        }
      vol_info_p = &fileio_Vol_info_header.volinfo[i][j];
      vol_fd = vol_info_p->vdes;
    }
  else
    {
      pthread_mutex_lock (&fileio_Sys_vol_info_header.mutex);
      arg.vol_id = vol_id;
      sys_vol_info_p = fileio_find_system_volume (NULL, fileio_is_system_volume_id_equal, &arg);
      if (sys_vol_info_p)
        {
          vol_fd = sys_vol_info_p->vdes;
        }

      pthread_mutex_unlock (&fileio_Sys_vol_info_header.mutex);
    }

  return vol_fd;
}

/*
 * fileio_find_volume_descriptor_with_label () - Find the volume descriptor given the volume label/name
 *   return: Volume Name/label
 *   vlabel(in): I/O volume descriptor
 */
int
fileio_find_volume_descriptor_with_label (const char *vol_label_p)
{
  FILEIO_VOLUME_INFO *vol_info_p;
  FILEIO_SYSTEM_VOLUME_INFO *sys_vol_info_p;
  int vol_fd = NULL_VOLDES;
  APPLY_ARG arg = { 0 };

  FILEIO_CHECK_AND_INITIALIZE_VOLUME_HEADER_CACHE (NULL_VOLDES);
  pthread_mutex_lock (&fileio_Sys_vol_info_header.mutex);
  arg.vol_label = vol_label_p;
  sys_vol_info_p = fileio_find_system_volume (NULL, fileio_is_system_volume_label_equal, &arg);
  if (sys_vol_info_p)
    {
      vol_fd = sys_vol_info_p->vdes;
      pthread_mutex_unlock (&fileio_Sys_vol_info_header.mutex);
      return vol_fd;
    }

  pthread_mutex_unlock (&fileio_Sys_vol_info_header.mutex);

  arg.vol_label = vol_label_p;
  vol_info_p = fileio_traverse_permanent_volume (NULL, fileio_is_volume_label_equal, &arg);
  if (vol_info_p)
    {
      return vol_info_p->vdes;
    }

  arg.vol_label = vol_label_p;
  vol_info_p = fileio_traverse_temporary_volume (NULL, fileio_is_volume_label_equal, &arg);
  if (vol_info_p)
    {
      return vol_info_p->vdes;
    }

  return vol_fd;
}

/*
 * fileio_get_lockf_type () - Find the lock type applied to a mounted volume
 *   return: lockf_type
 *   vdes(in): I/O volume descriptor
 */
FILEIO_LOCKF_TYPE
fileio_get_lockf_type (int vol_fd)
{
  FILEIO_VOLUME_INFO *vol_info_p;
  FILEIO_SYSTEM_VOLUME_INFO *sys_vol_info_p;
  FILEIO_LOCKF_TYPE lockf_type = FILEIO_NOT_LOCKF;
  APPLY_ARG arg = { 0 };

  FILEIO_CHECK_AND_INITIALIZE_VOLUME_HEADER_CACHE (FILEIO_NOT_LOCKF);
  pthread_mutex_lock (&fileio_Sys_vol_info_header.mutex);
  arg.vdes = vol_fd;
  sys_vol_info_p = fileio_find_system_volume (NULL, fileio_is_system_volume_descriptor_equal, &arg);
  if (sys_vol_info_p)
    {
      lockf_type = sys_vol_info_p->lockf_type;
      pthread_mutex_unlock (&fileio_Sys_vol_info_header.mutex);
      return lockf_type;
    }
  pthread_mutex_unlock (&fileio_Sys_vol_info_header.mutex);

  arg.vdes = vol_fd;
  vol_info_p = fileio_traverse_permanent_volume (NULL, fileio_is_volume_descriptor_equal, &arg);
  if (vol_info_p)
    {
      return vol_info_p->lockf_type;
    }

  arg.vdes = vol_fd;
  vol_info_p = fileio_traverse_temporary_volume (NULL, fileio_is_volume_descriptor_equal, &arg);
  if (vol_info_p)
    {
      return vol_info_p->lockf_type;
    }

  return lockf_type;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * fileio_symlink () -
 *   return:
 *   src(in):
 *   dest(in):
 *   overwrite(in):
 */
int
fileio_symlink (const char *src_p, const char *dest_p, int overwrite)
{
  if (overwrite && fileio_is_volume_exist (dest_p))
    {
      unlink (dest_p);
    }

  if (symlink (src_p, dest_p) == -1)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_BO_CANNOT_CREATE_LINK, 2, src_p, dest_p);
      return ER_FAILED;
    }

  return NO_ERROR;
}
#endif
