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
 * file_io.h - I/O module at server
 *
 */

#ifndef _FILE_IO_H_
#define _FILE_IO_H_

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <time.h>

#include "porting.h"
#include "storage_common.h"
#include "release_string.h"
#include "dbtype.h"
#include "memory_hash.h"
#include "lzoconf.h"
#include "lzo1x.h"
#include "thread.h"
#include "file_io_lock.h"

#define NULL_VOLDES   (-1)      /* Value of a null (invalid) vol descriptor */

/* Note: this value must be at least as large as PATH_MAX */
#define FILEIO_MAX_USER_RESPONSE_SIZE 2000

#define FILEIO_CHECK_FOR_INTERRUPT_INTERVAL       100
#define FILEIO_DISK_FORMAT_MODE            (O_RDWR | O_CREAT)
#define FILEIO_DISK_PROTECTION_MODE        0600
#define FILEIO_MAX_WAIT_DBTXT              300
#define FILEIO_FULL_LEVEL_EXP              32

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
#define MSGCAT_FILEIO_BKUP_HDR_HOST_INFO            30

#define STR_PATH_SEPARATOR "/"

#define PEEK           true     /* Peek volume label pointer */
#define ALLOC_COPY  false       /* alloc and copy volume label */

/* If the last character of path string is PATH_SEPARATOR, don't append PATH_SEPARATOR */
#define FILEIO_PATH_SEPARATOR(path) \
  (path[strlen(path) - 1] == PATH_SEPARATOR ? "" : STR_PATH_SEPARATOR)

#define FILEIO_GET_FILE_SIZE(pagesize, npages)  \
  (((off_t)(pagesize)) * ((off_t)(npages)))


typedef enum
{
  FILEIO_PROMPT_UNKNOWN,
  FILEIO_PROMPT_RANGE_TYPE,
  FILEIO_PROMPT_BOOLEAN_TYPE,
  FILEIO_PROMPT_STRING_TYPE,
  FILEIO_PROMPT_RANGE_WITH_SECONDARY_STRING_TYPE,
  FILEIO_PROMPT_DISPLAY_ONLY
} FILEIO_REMOTE_PROMPT_TYPE;

typedef enum
{
  FILEIO_ERROR_INTERRUPT,       /* error/interrupt */
  FILEIO_READ,                  /* access device for read */
  FILEIO_WRITE                  /* access device for write */
} FILEIO_TYPE;

/* Reserved area of FILEIO_PAGE */
typedef struct fileio_page_reserved FILEIO_PAGE_RESERVED;
struct fileio_page_reserved
{
  LOG_LSA lsa;                  /* Log Sequence number of page, Page recovery
                                   stuff */
  INT32 pageid;                 /* Page identifier */
  INT16 volid;                  /* Volume identifier where the page reside */
  unsigned char ptype;          /* Page type */
  unsigned char pflag_reserve_1;        /* unused - Reserved field */
  INT64 p_reserve_2;            /* unused - Reserved field */
  INT64 p_reserve_3;            /* unused - Reserved field */
};

/* The FILEIO_PAGE */
typedef struct fileio_page FILEIO_PAGE;
struct fileio_page
{
  FILEIO_PAGE_RESERVED prv;     /* System page area. Reserved */
  char page[1];                 /* The user page area               */
};

typedef struct token_bucket TOKEN_BUCKET;
struct token_bucket
{
  pthread_mutex_t token_mutex;
  int tokens;                   /* shared tokens between all lines */
  int token_consumed;

  pthread_cond_t waiter_cond;
};

typedef struct flush_stats FLUSH_STATS;
struct flush_stats
{
  unsigned int num_log_pages;
  unsigned int num_pages;
  unsigned int num_tokens;
};

extern int fileio_open (const char *vlabel, int flags, int mode);
extern void fileio_close (int vdes);
extern int fileio_format (THREAD_ENTRY * thread_p, const char *db_fullname,
                          const char *vlabel, VOLID volid, DKNPAGES npages,
                          bool sweep_clean, bool dolock, bool dosync,
                          size_t page_size, int kbytes_to_be_written_per_sec, bool reuse_file);
extern DKNPAGES fileio_expand (THREAD_ENTRY * threda_p, VOLID volid, DKNPAGES npages_toadd, DISK_VOLPURPOSE purpose);
extern void *fileio_initialize_pages (THREAD_ENTRY * thread_p, int vdes,
                                      void *io_pgptr, DKNPAGES start_pageid,
                                      DKNPAGES npages, size_t page_size, int kbytes_to_be_written_per_sec);
extern PAGE_TYPE fileio_get_page_ptype (UNUSED_ARG THREAD_ENTRY * thread_p, FILEIO_PAGE_RESERVED * prv_p);
extern PAGE_TYPE fileio_set_page_ptype (THREAD_ENTRY * thread_p, FILEIO_PAGE_RESERVED * prv_p, PAGE_TYPE ptype);
extern FILEIO_PAGE *fileio_alloc_io_page (THREAD_ENTRY * thread_p);
#if defined (ENABLE_UNUSED_FUNCTION)
extern DKNPAGES fileio_truncate (VOLID volid, DKNPAGES npages_to_resize);
#endif
extern void fileio_unformat (THREAD_ENTRY * thread_p, const char *vlabel);
extern void fileio_unformat_and_rename (THREAD_ENTRY * thread_p, const char *vlabel, const char *new_vlabel);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int fileio_copy_volume (THREAD_ENTRY * thread_p, int from_vdes,
                               DKNPAGES npages, const char *to_vlabel, VOLID to_volid, bool reset_recvinfo);
#endif
extern int fileio_reset_volume (THREAD_ENTRY * thread_p, int vdes,
                                const char *vlabel, DKNPAGES npages, LOG_LSA * reset_lsa);
extern int fileio_mount (THREAD_ENTRY * thread_p, const char *db_fullname,
                         const char *vlabel, VOLID volid, int lockwait, bool dosync);
extern void fileio_dismount (THREAD_ENTRY * thread_p, int vdes);
extern void fileio_dismount_all (THREAD_ENTRY * thread_p);
extern void *fileio_read (THREAD_ENTRY * thread_p, int vol_fd, void *io_page_p, PAGEID page_id, size_t page_size);
extern void *fileio_write (THREAD_ENTRY * thread_p, int vol_fd, void *io_page_p, PAGEID page_id, size_t page_size);
extern void *fileio_read_pages (THREAD_ENTRY * thread_p, int vol_fd,
                                char *io_pages_p, PAGEID page_id, int num_pages, size_t page_size);
extern void *fileio_write_pages (THREAD_ENTRY * thread_p, int vol_fd,
                                 char *io_pages_p, PAGEID page_id, int num_pages, size_t page_size);
extern void *fileio_writev (THREAD_ENTRY * thread_p, int vdes,
                            void **arrayof_io_pgptr, PAGEID start_pageid, DKNPAGES npages, size_t page_size);
extern int fileio_synchronize (THREAD_ENTRY * thread_p, int vdes, const char *vlabel);
extern int fileio_synchronize_all (THREAD_ENTRY * thread_p, bool include_log);
#if defined (ENABLE_UNUSED_FUNCTION)
extern void *fileio_read_user_area (THREAD_ENTRY * thread_p, int vdes,
                                    PAGEID pageid, off_t start_offset, size_t nbytes, void *area);
extern void *fileio_write_user_area (THREAD_ENTRY * thread_p, int vdes,
                                     PAGEID pageid, off_t start_offset, int nbytes, void *area);
#endif
extern bool fileio_is_volume_exist_and_file (const char *vlabel);
extern DKNPAGES fileio_get_number_of_volume_pages (int vdes, size_t page_size);
extern char *fileio_get_volume_label (VOLID volid, bool is_peek);
extern char *fileio_get_volume_label_by_fd (int vol_fd, bool is_peek);
extern VOLID fileio_find_volume_id_with_label (THREAD_ENTRY * thread_p, const char *vlabel);
extern int fileio_get_volume_descriptor (VOLID volid);
extern bool fileio_map_mounted (THREAD_ENTRY * thread_p,
                                bool (*fun) (THREAD_ENTRY * thread_p, VOLID volid, void *args), void *args);
extern int fileio_get_number_of_partition_free_pages (const char *path, size_t page_size);
extern const char *fileio_rename (VOLID volid, const char *old_vlabel, const char *new_vlabel);
extern bool fileio_is_volume_exist (const char *vlabel);
extern int fileio_find_volume_descriptor_with_label (const char *vol_label_p);
extern int fileio_get_max_name (const char *path, long int *filename_max, long int *pathname_max);
extern const char *fileio_get_base_file_name (const char *fullname);
extern char *fileio_get_directory_path (char *path, const char *fullname);
extern int fileio_get_volume_max_suffix (void);
extern void fileio_make_volume_info_name (char *volinfo_name, const char *db_fullname);
extern void fileio_make_volume_ext_name (char *volext_fullname,
                                         const char *ext_path, const char *ext_name, VOLID volid);
#if defined (ENABLE_UNUSED_FUNCTION)
extern void fileio_make_volume_ext_given_name (char *volext_fullname, const char *ext_path, const char *ext_name);
#endif
extern void fileio_make_volume_temp_name (char *voltmp_fullname,
                                          const char *tmp_path, const char *tmp_name, VOLID volid);
extern void fileio_make_log_active_name (char *logactive_name, const char *log_path, const char *dbname);
extern void fileio_make_log_active_temp_name (char *logactive_tmpname, const char *active_name);
extern void fileio_make_log_archive_name (char *logarchive_name, const char *log_path, const char *dbname, int arvnum);
extern void fileio_make_removed_log_archive_name (char *logarchive_name, const char *log_path, const char *dbname);
extern void fileio_make_log_archive_temp_name (char *log_archive_temp_name_p,
                                               const char *log_path_p, const char *db_name_p);
extern void fileio_make_log_info_name (char *loginfo_name, const char *log_path, const char *dbname);

#if 0
extern FILEIO_LOCKF_TYPE fileio_lock_la_log_path (const char *db_fullname, const char *lock_path, int vdes);
extern FILEIO_LOCKF_TYPE fileio_lock_la_dbname (int *lockf_vdes, char *db_name, char *log_path);
extern FILEIO_LOCKF_TYPE fileio_unlock_la_dbname (int *lockf_vdes, char *db_name, bool clear_owner);
#endif
#if defined (ENABLE_UNUSED_FUNCTION)
extern int fileio_symlink (const char *src, const char *dest, int overwrite);
extern int fileio_set_permission (const char *vlabel);
#endif

/* flush control related */
extern int fileio_flush_control_initialize (void);
extern void fileio_flush_control_finalize (void);

/* flush token management */
extern int fileio_flush_control_add_tokens (THREAD_ENTRY * thread_p,
                                            INT64 diff_usec, int *token_gen, int *token_consumed);
extern char *fileio_ctime (INT64 * clock_p, char *buffer_p);
extern void fileio_decache (THREAD_ENTRY * thread_p, int vdes);
extern FILEIO_LOCKF_TYPE fileio_get_lockf_type (int vdes);
extern int fileio_create (THREAD_ENTRY * thread_p, const char *db_fullname,
                          const char *vlabel, VOLID volid, bool dolock, bool dosync);
extern void fileio_initialize_res (THREAD_ENTRY * thread_p, FILEIO_PAGE_RESERVED * prv_p);

#endif /* _FILE_IO_H_ */
