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
 * backup.h: backup module (common)
 */

#ifndef BACKUP_H_
#define BACKUP_H_

#include "config.h"

#include <stdio.h>
#include <time.h>

#include "porting.h"
#include "system_parameter.h"
#include "storage_common.h"
#include "release_string.h"
#include "dbtype.h"
#include "memory_hash.h"
#include "lzoconf.h"
#include "lzo1x.h"
#include "thread.h"
#include "file_io.h"
#include "connection_defs.h"

#define BK_INITIAL_BACKUP_UNITS    0
#define BK_BACKUP_NUM_THREADS_AUTO    0
#define BK_BACKUP_SLEEP_MSECS_AUTO    0

/*
 * Define a fixed size for backup and restore input/output of the volume
 * headers.  For most modern devices multiples of 512 or 1024 are needed.
 * A size of the header data is computed in compile.
 */
#define GET_NEXT_1K_SIZE(s)             (((((s) - 1) / 1024) + 1) * 1024)
#define BK_BACKUP_HEADER_IO_SIZE    GET_NEXT_1K_SIZE(sizeof(BK_BACKUP_HEADER))

#define BK_BACKUP_HEADER_VERSION       1

#define BK_BACKUP_PAGE_OVERHEAD (offsetof(BK_BACKUP_PAGE, iopage))

#define BK_VOL_HEADER_IN_BACKUP_PAGE_SIZE  \
  (sizeof(BK_VOL_HEADER_IN_BACKUP) + offsetof(BK_BACKUP_PAGE, iopage))

/* Get the backup page primary pageid */
#define BK_GET_BACKUP_PAGE_ID(area)  (((BK_BACKUP_PAGE *)(area))->iopageid)

/* Set the backup page pageid(s) */
#define BK_SET_BACKUP_PAGE_ID(area, pageid, psize) \
  ((BK_BACKUP_PAGE *)(area))->iopageid = pageid;

#define BK_RESTORE_DBVOLS_IO_PAGE_SIZE(sess)  \
  ((sess)->bkuphdr->bkpagesize + BK_BACKUP_PAGE_OVERHEAD)

/* Some specifications of page identifiers of backup */
#define BK_BACKUP_START_PAGE_ID      (-2)
#define BK_BACKUP_END_PAGE_ID        (-3)
#define BK_BACKUP_FILE_START_PAGE_ID (-4)
#define BK_BACKUP_FILE_END_PAGE_ID   (-5)
#define BK_BACKUP_VOL_CONT_PAGE_ID   (-6)

typedef enum
{
  BK_ZIP_NONE_METHOD,           /* None  */
  BK_ZIP_LZO1X_METHOD           /* LZO1X */
} BK_ZIP_METHOD;

typedef enum
{
  BK_ZIP_NONE_LEVEL = 0,        /* None */
  BK_ZIP_1_LEVEL = 1,           /* best speed */
  BK_ZIP_9_LEVEL = 9,           /* best compression */
  BK_ZIP_LZO1X_999_LEVEL = BK_ZIP_9_LEVEL,
  BK_ZIP_LZO1X_DEFAULT_LEVEL = BK_ZIP_1_LEVEL,
} BK_ZIP_LEVEL;

typedef enum
{
  BK_PACKET_VOL_START,
  BK_PACKET_DATA,
  BK_PACKET_VOL_END,
  BK_PACKET_VOLS_BACKUP_END,
  BK_PACKET_LOGS_BACKUP_END
} BK_PACKET_TYPE;

/* BK_PACKET_VOL_START:         int + int
 * BK_PACKET_DATA:              int + int
 * BK_PACKET_VOL_END:           int + int
 * BK_PACKET_VOLS_BACKUP_END:   int + int
 * BK_PACKET_LOGS_BACKUP_END:   int + int [+ lsa + int64]
 */
#define BK_PACKET_HDR_SIZE   ((OR_INT_SIZE * 2) + OR_LOG_LSA_ALIGNED_SIZE + OR_BIGINT_ALIGNED_SIZE)

typedef struct bk_backup_page BK_BACKUP_PAGE;
struct bk_backup_page
{
  PAGEID iopageid;              /* Identifier of page to buffer */
  FILEIO_PAGE iopage;           /* The content of the page */
};

/* Backup header */

typedef struct bk_backup_header BK_BACKUP_HEADER;
struct bk_backup_header
{
  PAGEID iopageid;              /* Must be the same as start of an BK_BACKUP_PAGE
                                   NOTE: a union would be better. */
  char bk_magic[RYE_MAGIC_MAX_LENGTH];  /* Magic value for file/magic
                                           Unix utility */
  RYE_VERSION bk_db_version;    /* rye version for compatibility check */
  int bk_hdr_version;           /* For future compatibility checking */
  INT64 db_creation;            /* Database creation time */
  INT64 start_time;             /* Time of backup start */
  INT64 end_time;               /* Time of backup end */
  char db_name[PATH_MAX];       /* Fullname of backed up database.
                                   Really more than one byte */
  PRM_NODE_INFO db_host_info;   /* host info */
  PGLENGTH db_iopagesize;       /* Size of database pages */
  LOG_LSA chkpt_lsa;            /* LSA for next incremental backup */
  LOG_LSA backuptime_lsa;       /* for HA apply */

  int bkup_iosize;              /* Buffered io size when backup was taken */
  int bkpagesize;               /* size of backup page */
  BK_ZIP_METHOD zip_method;     /* compression method  */
  BK_ZIP_LEVEL zip_level;       /* compression level   */
  int make_slave;
  HA_STATE server_state;
};

/* Shouldn't this structure should use int and such? */
typedef struct bk_backup_buffer BK_BACKUP_BUFFER;
struct bk_backup_buffer
{
  int vdes;                     /* Open descriptor of backup device */
  const char *vlabel;           /* Pointer to current backup device name */
  char name[PATH_MAX];          /* Name of the current backup volume */

  int iosize;                   /* Optimal I/O pagesize for backup device */
  int count;                    /* Number of current buffered bytes */
  INT64 voltotalio;             /* Total number of bytes that have been
                                   either read or written (current volume) */
  INT64 alltotalio;             /* total for all volumes */
  char *buffer;                 /* Pointer to the buffer */
  char *ptr;                    /* Pointer to the first buffered byte when
                                 * reading and pointer to the next byte to
                                 * buffer when writing
                                 */
};

typedef struct bk_dbvol_buffer BK_DBVOL_BUFFER;
struct bk_dbvol_buffer
{
  int vdes;                     /* Open file descriptor of device name for
                                   writing purposes */
  VOLID volid;                  /* Identifier of volume to backup/restore */
  INT64 nbytes;                 /* Number of bytes of file */
  const char *vlabel;           /* Pointer to file name to backup */
  BK_BACKUP_PAGE *area;         /* Area to read/write the page */

  /* for verbose backup progress */
  int bk_npages;
  int check_ratio;
  int check_npages;
};

typedef struct bk_zip_page BK_ZIP_PAGE;
struct bk_zip_page
{
  lzo_uint buf_len;             /* compressed block size */
  lzo_byte buf[1];              /* data block */
};

typedef struct bk_node BK_NODE;
struct bk_node
{
  struct bk_node *prev;
  struct bk_node *next;
  int pageid;
  bool writeable;
  ssize_t nread;
  BK_BACKUP_PAGE *area;         /* Area to read/write the page */
  BK_ZIP_PAGE *zip_page;        /* Area to compress/decompress the page */
  lzo_bytep wrkmem;
};

typedef struct bk_queue BK_QUEUE;
struct bk_queue
{
  int size;
  BK_NODE *head;
  BK_NODE *tail;
  BK_NODE *free_list;
};

typedef struct bk_thread_info BK_THREAD_INFO;
struct bk_thread_info
{
#if defined(SERVER_MODE)
  pthread_mutex_t mtx;
  pthread_cond_t rcv;           /* condition variable of read_thread */
  pthread_cond_t wcv;           /* condition variable of write_thread */
#endif                          /* SERVER_MODE */

  int tran_index;

  int num_threads;              /* number of read threads plus one write thread */
  int act_r_threads;            /* number of activated read threads */
  int end_r_threads;            /* number of ended read threads */

  int pageid;
  int from_npages;

  FILEIO_TYPE io_type;
  int errid;

  bool initialized;

  BK_QUEUE io_queue;
};

typedef struct bk_backup_session BK_BACKUP_SESSION;
struct bk_backup_session
{
  BK_BACKUP_HEADER *bkuphdr;    /* pointer to header information */
  BK_BACKUP_BUFFER bkup;        /* Buffering area for backup volume */
  BK_DBVOL_BUFFER dbfile;       /* Buffer area for database files */
  BK_THREAD_INFO read_thread_info;      /* read-threads info */
  FILE *verbose_fp;             /* Backupdb/Restoredb status msg */
  int sleep_msecs;              /* sleep internval in msecs */

  int num_perm_vols;
  int first_arv_needed;
  LOG_PAGEID saved_run_nxchkpt_atpageid;

  int send_buf_size;
  int rid;
};

typedef struct bk_vol_header_in_backup BK_VOL_HEADER_IN_BACKUP;

/* A FILE/VOLUME HEADER IN BACKUP */
struct bk_vol_header_in_backup
{
  INT64 nbytes;
  VOLID volid;
  short dummy1;                 /* Dummy field for 8byte align */
  int dummy2;                   /* Dummy field for 8byte align */
  char vlabel[PATH_MAX];
};

extern int bk_init_backup_buffer (BK_BACKUP_SESSION * session,
                                  const char *db_name, const char *backup_path, int do_compress);
extern int bk_init_backup_vol_buffer (BK_BACKUP_SESSION * session_p, int num_threads, int sleep_msecs);
extern void bk_make_backup_name (char *backup_name, const char *nopath_volname, const char *backup_path, int unit_num);
extern const char *bk_get_backup_level_string (void);
extern const char *bk_get_zip_method_string (BK_ZIP_METHOD zip_method);
extern const char *bk_get_zip_level_string (BK_ZIP_LEVEL zip_level);
extern BK_NODE *bk_allocate_node (BK_QUEUE * qp, BK_BACKUP_HEADER * backup_hdr);
extern BK_NODE *bk_free_node (BK_QUEUE * qp, BK_NODE * node);
extern BK_NODE *bk_append_queue (BK_QUEUE * queue_p, BK_NODE * node_p);
extern BK_NODE *bk_delete_queue_head (BK_QUEUE * qp);
extern void bk_abort_backup_client (BK_BACKUP_SESSION * session_p, bool does_unformat_bk);
extern void bk_abort_backup_server (BK_BACKUP_SESSION * session_p);
#endif /* BACKUP_H_ */
