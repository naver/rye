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
 * storage_common.h - Definitions and data types of disk related stuffs
 *          such as pages, file structures, and so on.
 */

#ifndef _STORAGE_COMMON_H_
#define _STORAGE_COMMON_H_

#ident "$Id$"

#include "config.h"

#include <limits.h>
#include <time.h>
#include <stdio.h>

#include "porting.h"

#include "dbdef.h"
#include "dbtype.h"

/* LIMITS AND NULL VALUES ON DISK RELATED DATATYPES */

#define NULL_PAGEID (-1)	/* Value of an invalid page identifier */
#define NULL_SLOTID (-1)	/* Value of an invalid slot identifier */
#define NULL_VOLID  (-1)	/* Value of an invalid volume identifier */
#define NULL_GROUPID (DB_INT32_MIN)	/* Value of an invalid shard group identifier */
#define GLOBAL_GROUPID (0)	/* Value of a no-shard group identifier */

#define NULL_SECTID (-1)	/* Value of an invalid sector identifier */
#define NULL_OFFSET (-1)	/* Value of an invalid offset */
#define NULL_FILEID (-1)	/* Value of an invalid file identifier */


#define VOLID_MAX       SHRT_MAX
#define PAGEID_MAX      INT_MAX
/* max groupid is 1000000, +1 is global groupid */
#define GROUPID_MAX     1000001
#define PGLENGTH_MAX    SHRT_MAX
#define VOL_MAX_NPAGES(page_size) \
  ((sizeof(off_t) == 4) ? (INT_MAX / (page_size)) : INT_MAX)

#define LOGPAGEID_MAX   0x7fffffffffffLL	/* 6 bytes length */

/* Compose the full name of a database */

#define COMPOSE_FULL_NAME(buf, buf_size, path, name) \
  do { \
    int len = strlen(path); \
    if (len > 0 && path[len - 1] != PATH_SEPARATOR) { \
      snprintf(buf, buf_size - 1, "%s%c%s", path, PATH_SEPARATOR, name); \
    } else { \
      snprintf(buf, buf_size - 1, "%s%s", path, name); \
    } \
  } while (0)

/* Type definitions related to disk information	*/

typedef INT32 PAGEID;		/* Data page identifier */
typedef INT64 LOG_PAGEID;	/* Log page identifier */
typedef PAGEID LOG_PHY_PAGEID;	/* physical log page identifier */

typedef INT16 VOLID;		/* Volume identifier */
typedef PAGEID DKNPAGES;	/* Number of disk pages */

typedef INT16 PGSLOTID;		/* Page slot identifier */
typedef PGSLOTID PGNSLOTS;	/* Number of slots on a page */
typedef INT16 PGLENGTH;		/* Page length */

typedef PAGEID FILEID;		/* File identifier */
typedef INT32 LOLENGTH;		/* Length for a large object */

typedef INT32 GROUPID;		/* Shard group identifier */

#define PAGEID_EQ(p1, p2) ((p1) == (p2))
#define VOLID_EQ(v1, v2) ((v1) == (v2))


/* Log address structure */

typedef struct log_lsa LOG_LSA;	/* Log address identifier */
struct log_lsa
{
  INT64 pageid:49;		/* Log page identifier : 49 bits length */
  INT64 offset:15;		/* Offset in page : 15 bits length */
};

#define LSA_COPY(lsa_ptr1, lsa_ptr2) *(lsa_ptr1) = *(lsa_ptr2)
#define LSA_SET_NULL(lsa_ptr)\
  do {									      \
    (lsa_ptr)->pageid = NULL_PAGEID;                                          \
    (lsa_ptr)->offset = NULL_OFFSET;                                          \
  } while(0)

#define LSA_SET_INIT_NONTEMP(lsa_ptr) LSA_SET_NULL(lsa_ptr)
#define LSA_SET_INIT_TEMP(lsa_ptr)\
  do {									      \
    (lsa_ptr)->pageid = NULL_PAGEID - 1;                                      \
    (lsa_ptr)->offset = NULL_OFFSET - 1;                                      \
  } while(0)

#define LSA_ISNULL(lsa_ptr) ((lsa_ptr)->pageid == NULL_PAGEID)
#define LSA_IS_INIT_NONTEMP(lsa_ptr) LSA_ISNULL(lsa_ptr)
#define LSA_IS_INIT_TEMP(lsa_ptr) (((lsa_ptr)->pageid == NULL_PAGEID - 1) &&  \
				  ((lsa_ptr)->offset == NULL_OFFSET - 1))

#define LSA_LT(lsa_ptr1, lsa_ptr2)                                            \
  ((lsa_ptr1) != (lsa_ptr2) &&                                                \
   ((lsa_ptr1)->pageid < (lsa_ptr2)->pageid ||                                \
    ((lsa_ptr1)->pageid == (lsa_ptr2)->pageid &&                              \
     (lsa_ptr1)->offset < (lsa_ptr2)->offset)))                               \

#define LSA_EQ(lsa_ptr1, lsa_ptr2)                                            \
  ((lsa_ptr1) == (lsa_ptr2) ||                                                \
    ((lsa_ptr1)->pageid == (lsa_ptr2)->pageid &&                              \
     (lsa_ptr1)->offset == (lsa_ptr2)->offset))

#define LSA_LE(lsa_ptr1, lsa_ptr2) (!LSA_LT(lsa_ptr2, lsa_ptr1))
#define LSA_GT(lsa_ptr1, lsa_ptr2) LSA_LT(lsa_ptr2, lsa_ptr1)
#define LSA_GE(lsa_ptr1, lsa_ptr2) LSA_LE(lsa_ptr2, lsa_ptr1)

/* BOTH IO_PAGESIZE AND DB_PAGESIZE MUST BE MULTIPLE OF sizeof(int) */

#if 1				/* TODO - */
#define IO_DEFAULT_PAGE_SIZE    (16 * ONE_K)
#else
#define IO_DEFAULT_PAGE_SIZE    (4 * ONE_K)
#endif
#define IO_MIN_PAGE_SIZE        IO_DEFAULT_PAGE_SIZE
#define IO_MAX_PAGE_SIZE        IO_DEFAULT_PAGE_SIZE

#define LOG_PAGESIZE            (db_log_page_size())
#define IO_PAGESIZE             (db_io_page_size())
#define DB_PAGESIZE             (db_page_size())
#define DB_MAX_PATH_LENGTH      PATH_MAX

/* BTREE definitions */
#define STAT_INFO_SIZE (sizeof( BTREE_STAT_INFO))
#define SPLIT_INFO_SIZE (sizeof(BTREE_NODE_SPLIT_INFO))

#define DISK_VFID_SIZE (OR_INT_SIZE + OR_SHORT_SIZE)
#define DISK_VPID_SIZE (OR_INT_SIZE + OR_SHORT_SIZE)

/* offset values to access fields */
#define BTREE_NODE_TYPE_SIZE            OR_SHORT_SIZE
#define BTREE_NODE_KEY_CNT_SIZE         OR_SHORT_SIZE
#define BTREE_NODE_MAX_KEY_LEN_SIZE     OR_SHORT_SIZE
#define BTREE_NODE_NEXT_VPID_SIZE       DISK_VPID_SIZE	/* SHORT + INT */
#define BTREE_NODE_PREV_VPID_SIZE       DISK_VPID_SIZE	/* SHORT + INT */
#define BTREE_NODE_PADDING_SIZE         OR_SHORT_SIZE
#define BTREE_NODE_SPLIT_INFO_SIZE      SPLIT_INFO_SIZE

#define BTREE_NUM_OIDS_SIZE             OR_INT_SIZE
#define BTREE_NUM_NULLS_SIZE            OR_INT_SIZE
#define BTREE_NUM_KEYS_SIZE             OR_INT_SIZE
#if 0
#define BTREE_UNIQUE_SIZE               OR_INT_SIZE
#define BTREE_REVERSE_SIZE              OR_INT_SIZE
#define BTREE_REV_LEVEL_SIZE            OR_INT_SIZE
#define BTREE_OVFID_SIZE                DISK_VFID_SIZE	/* INT + SHORT */
#define BTREE_RESERVED_SIZE             OR_SHORT_SIZE	/* currently, unused */
#endif

#define BTREE_NODE_TYPE_OFFSET          (0)

#define BTREE_NODE_KEY_CNT_OFFSET \
  (BTREE_NODE_TYPE_OFFSET + BTREE_NODE_TYPE_SIZE)

#define BTREE_NODE_MAX_KEY_LEN_OFFSET \
  (BTREE_NODE_KEY_CNT_OFFSET + BTREE_NODE_KEY_CNT_SIZE)

#define BTREE_NODE_NEXT_VPID_OFFSET \
  (BTREE_NODE_MAX_KEY_LEN_OFFSET + BTREE_NODE_MAX_KEY_LEN_SIZE)

#define BTREE_NODE_PREV_VPID_OFFSET \
  (BTREE_NODE_NEXT_VPID_OFFSET + BTREE_NODE_NEXT_VPID_SIZE)

#define BTREE_NODE_PADDING_OFFSET \
  (BTREE_NODE_PREV_VPID_OFFSET + BTREE_NODE_PREV_VPID_SIZE)

#define BTREE_NODE_SPLIT_INFO_OFFSET \
  (BTREE_NODE_PADDING_OFFSET + BTREE_NODE_PADDING_SIZE)

#define BTREE_NUM_OIDS_OFFSET \
  (BTREE_NODE_SPLIT_INFO_OFFSET + BTREE_NODE_SPLIT_INFO_SIZE)

#define BTREE_NUM_NULLS_OFFSET \
  (BTREE_NUM_OIDS_OFFSET + BTREE_NUM_OIDS_SIZE)

#define BTREE_NUM_KEYS_OFFSET \
  (BTREE_NUM_NULLS_OFFSET + BTREE_NUM_NULLS_SIZE)

#if 0
#define BTREE_TOPCLASS_OID_OFFSET \
  (BTREE_NUM_KEYS_OFFSET + BTREE_NUM_KEYS_SIZE)

#define BTREE_UNIQUE_OFFSET \
  (BTREE_TOPCLASS_OID_OFFSET + BTREE_TOPCLASS_OID_SIZE)

#define BTREE_REVERSE_RESERVED_OFFSET \
  (BTREE_UNIQUE_OFFSET + BTREE_UNIQUE_SIZE)

#define BTREE_REV_LEVEL_OFFSET \
  (BTREE_REVERSE_RESERVED_OFFSET + BTREE_REVERSE_SIZE)

#define BTREE_OVFID_OFFSET \
  (BTREE_REV_LEVEL_OFFSET + BTREE_REV_LEVEL_SIZE)

#define BTREE_RESERVED_OFFSET \
  (BTREE_OVFID_OFFSET + BTREE_OVFID_SIZE)

#define BTREE_KEY_TYPE_OFFSET \
  (BTREE_RESERVED_OFFSET + BTREE_RESERVED_SIZE)

#define ROOT_HEADER_FIXED_SIZE BTREE_KEY_TYPE_OFFSET
#endif

typedef struct btree_node_split_info BTREE_NODE_SPLIT_INFO;
struct btree_node_split_info
{
  float pivot;			/* pivot = split_slot_id / num_keys */
  int index;			/* number of key insert after node split */
};

typedef char *PAGE_PTR;		/* Pointer to a page */

/* TODO - PAGE_TYPE is used for debugging */
/* fetches sub-info */
typedef enum
{
  PAGE_UNKNOWN = 0,		/* 0 used for initialized page            */
  PAGE_FILE_HEADER,		/* 1 file header page                     */
  PAGE_FILE_TAB,		/* 2 file allocset table page             */
  PAGE_HEAP_HEADER,		/* 3 heap header page               */
  PAGE_HEAP,			/* 4 heap page                            */
  PAGE_VOLHEADER,		/* 5 volume header page                   */
  PAGE_VOLBITMAP,		/* 6 volume bitmap page                   */
  PAGE_XASL,			/* 7 xasl stream page                     */
  PAGE_QRESULT,			/* 8 query result page                    */
  PAGE_EHASH,			/* 9 ehash bucket/dir page                */
  PAGE_OVERFLOW,		/* 10 overflow page                        */
  PAGE_AREA,			/* 11 area page                            */
  PAGE_CATALOG,			/* 12 catalog page                         */
  PAGE_BTREE_ROOT,		/* 13 b+tree index root page               */
  PAGE_BTREE,			/* 14 b+tree index page                    */

  PAGE_LAST = PAGE_BTREE
} PAGE_TYPE;

#define ISCAN_OID_BUFFER_SIZE \
  ((prm_get_bigint_value (PRM_ID_BT_OID_BUFFER_SIZE) / OR_OID_SIZE) * OR_OID_SIZE)

/* TYPE DEFINITIONS RELATED TO KEY AND VALUES */

typedef enum			/* range search option */
{
  NA_NA,			/* v1 and v2 are N/A, so that no range is defined */
  GE_LE,			/* v1 <= key <= v2 */
  GE_LT,			/* v1 <= key < v2 */
  GT_LE,			/* v1 < key <= v2 */
  GT_LT,			/* v1 < key < v2 */
  GE_INF,			/* v1 <= key (<= the end) */
  GT_INF,			/* v1 < key (<= the end) */
  INF_LE,			/* (the beginning <=) key <= v2 */
  INF_LT,			/* (the beginning <=) key < v2 */
  INF_INF,			/* the beginning <= key <= the end */
  EQ_NA,			/* key = v1, v2 is N/A */

  /* following options are reserved for the future use */
  LE_GE,			/* key <= v1 || key >= v2 or NOT (v1 < key < v2) */
  LE_GT,			/* key <= v1 || key > v2 or NOT (v1 < key <= v2) */
  LT_GE,			/* key < v1 || key >= v2 or NOT (v1 <= key < v2) */
  LT_GT,			/* key < v1 || key > v2 or NOT (v1 <= key <= v2) */
  NEQ_NA			/* key != v1 */
} RANGE;

/* File structure identifiers */

typedef struct hfid HFID;	/* FILE HEAP IDENTIFIER */
struct hfid
{
  VFID vfid;			/* Volume and file identifier */
  INT32 hpgid;			/* First page identifier (the header page) */
};
#define NULL_HFID_INITIALIZER    {{NULL_FILEID, NULL_VOLID}, NULL_PAGEID}

typedef struct btid BTID;	/* B+tree identifier */
struct btid
{
  VFID vfid;			/* B+tree index volume identifier */
  INT32 root_pageid;		/* Root page identifier */
};
#define NULL_BTID_INITIALIZER { NULL_VFID_INITIALIZER, NULL_PAGEID }

typedef struct ehid EHID;	/* EXTENDIBLE HASHING IDENTIFIER */
struct ehid
{
  VFID vfid;			/* Volume and Directory file identifier */
  INT32 pageid;			/* The first (root) page of the directory */
};

typedef struct recdes RECDES;	/* RECORD DESCRIPTOR */
struct recdes
{
  int area_size;		/* Length of the allocated area. It includes
				   only the data field. The value is negative
				   if data is inside buffer. For example,
				   peeking in a slotted page. */
  int length;			/* Length of the data. Does not include the
				   length and type fields */
  INT16 type;			/* Type of record */
  char *data;			/* The data */
};

typedef struct lorecdes LORECDES;	/* Work area descriptor */
struct lorecdes
{
  LOLENGTH length;		/* The length of data in the area */
  LOLENGTH area_size;		/* The size of the area */
  char *data;			/* Pointer to the beginning of the area */
};

#define RECDES_INITIALIZER { 0, -1, REC_UNKNOWN, NULL }

#define HFID_SET_NULL(hfid) \
  do { \
    (hfid)->vfid.fileid = NULL_FILEID; \
    (hfid)->hpgid = NULL_PAGEID; \
  } while(0)

#define HFID_COPY(hfid_ptr1, hfid_ptr2) *(hfid_ptr1) = *(hfid_ptr2)

#define HFID_IS_NULL(hfid)  (((hfid)->vfid.fileid == NULL_FILEID) ? 1 : 0)

#define BTID_SET_NULL(btid) \
  do { \
    (btid)->vfid.fileid = NULL_FILEID; \
    (btid)->vfid.volid = NULL_VOLID; \
    (btid)->root_pageid = NULL_PAGEID; \
  } while(0)

#define BTID_COPY(btid_ptr1, btid_ptr2) *(btid_ptr1) = *(btid_ptr2)

#define BTID_IS_NULL(btid)  (((btid)->vfid.fileid == NULL_FILEID) ? 1 : 0)

#define BTID_IS_EQUAL(b1,b2) \
  (((b1)->vfid.fileid == (b2)->vfid.fileid) \
   && ((b1)->vfid.volid == (b2)->vfid.volid) \
   && ((b1)->root_pageid == (b2)->root_pageid))

#define EHID_SET_NULL(ehid) \
  do { \
    (ehid)->vfid.fileid = NULL_FILEID; \
    (ehid)->pageid = NULL_PAGEID; \
  } while(0)

#define EHID_IS_NULL(ehid)  (((ehid)->vfid.fileid == NULL_FILEID) ? 1 : 0)

#define LOID_SET_NULL(loid) \
  do { \
    (loid)->vpid.pageid = NULL_PAGEID; \
    (loid)->vfid.fileid = NULL_FILEID; \
  } while (0)

#define LOID_IS_NULL(loid)  (((loid)->vpid.pageid == NULL_PAGEID) ? 1 : 0)

#define DISK_VOLPURPOSE DB_VOLPURPOSE

#define RECDES_INIT(recdes)             \
  do                                    \
    {                                   \
      (recdes)->area_size = 0;          \
      (recdes)->length = 0;             \
      (recdes)->type = REC_UNKNOWN;     \
      (recdes)->data = NULL;            \
    }                                   \
  while (0)

/* Types ans defines of transaction managment */

typedef int TRANID;		/* Transaction identifier      */

#define NULL_TRANID     (-1)
#define NULL_TRAN_INDEX (-1)

typedef enum
{
  /* Don't change the initialization since they reflect the elements of
     lock_Conv and lock_Comp */
  NA_LOCK = 0,			/* N/A lock */
  NULL_LOCK = 1,		/* NULL lock */
  S_LOCK = 2,			/* Shared lock */
  U_LOCK = 3,			/* Update lock */
  X_LOCK = 4,			/* Exclusive lock */
} LOCK;

extern LOCK lock_Conv[5][5];

#define LOCK_TO_LOCKMODE_STRING(lock) 			\
  (((lock) ==NULL_LOCK) ? "NULL_LOCK" :			\
   ((lock) ==   S_LOCK) ? "   S_LOCK" :			\
   ((lock) ==   U_LOCK) ? "   U_LOCK" :			\
   ((lock) ==   X_LOCK) ? "   X_LOCK" : "UNKNOWN")

/* CLASSNAME TO OID RETURN VALUES */

typedef enum
{
  LC_CLASSNAME_EXIST,
  LC_CLASSNAME_RESERVED,
  LC_CLASSNAME_DELETED,
  LC_CLASSNAME_RESERVED_RENAME,
  LC_CLASSNAME_DELETED_RENAME,
  LC_CLASSNAME_ERROR
} LC_FIND_CLASSNAME;

#define LC_EXIST              1
#define LC_DOESNOT_EXIST      2
#define LC_ERROR              3

/* Enumeration type for the result of ehash_search function */
typedef enum
{
  EH_KEY_FOUND,
  EH_KEY_NOTFOUND,
  EH_ERROR_OCCURRED
} EH_SEARCH;

typedef enum
{
  BTREE_KEY_FOUND,
  BTREE_KEY_NOTFOUND,
  BTREE_ERROR_OCCURRED
} BTREE_SEARCH;

/* TYPEDEFS FOR BACKUP/RESTORE */

/* structure for passing arguments into boot_restart_server et. al. */
typedef struct bo_restart_arg BO_RESTART_ARG;
struct bo_restart_arg
{
  bool printtoc;		/* True to show backup's table of contents */
  time_t stopat;		/* the recovery stop time if restarting from
				   backup */
  const char *backuppath;	/* Pathname override for location of backup
				   volumes */
  const char *verbose_file;	/* restoredb verbose msg file */
  bool restore_upto_backuptime;
  bool make_slave;

  int server_state;
  char db_host[MAXHOSTNAMELEN];
  LOG_LSA backuptime_lsa;	/* for HA apply */
  INT64 db_creation;		/* Database creation time */
};

/* Magic default values */
#define RYE_MAGIC_MAX_LENGTH                 25
#define RYE_MAGIC_PREFIX		     "RYE/"
#define RYE_MAGIC_DATABASE_VOLUME            "RYE/Volume"
#define RYE_MAGIC_LOG_ACTIVE                 "RYE/LogActive"
#define RYE_MAGIC_LOG_ARCHIVE                "RYE/LogArchive"
#define RYE_MAGIC_LOG_INFO                   "RYE/LogInfo"
#define RYE_MAGIC_DATABASE_BACKUP            "RYE/Backup"

/*
 * Typedefs related to the scan data structures
 */

typedef enum
{
  S_OPENED = 1,
  S_STARTED,
  S_ENDED,
  S_CLOSED
} SCAN_STATUS;

typedef enum
{
  S_BEFORE = 1,
  S_ON,
  S_AFTER
} SCAN_POSITION;

typedef enum
{
  S_ERROR = -1,
  S_END = 0,
  S_SUCCESS = 1,
  S_DOESNT_FIT,			/* only for slotted page */
  S_DOESNT_EXIST		/* only for slotted page */
} SCAN_CODE;

typedef enum
{
  S_SELECT,
  S_DELETE,
  S_UPDATE
} SCAN_OPERATION_TYPE;

#define IS_WRITE_EXCLUSIVE_LOCK(lock) ((lock) == X_LOCK)

extern INT16 db_page_size (void);
extern INT16 db_io_page_size (void);
extern INT16 db_log_page_size (void);
extern int db_set_page_size (INT16 io_page_size, INT16 log_page_size);
extern INT16 db_network_page_size (void);

extern int recdes_allocate_data_area (RECDES * rec, int size);
extern void recdes_free_data_area (RECDES * rec);
extern void recdes_set_data_area (RECDES * rec, char *data, int size);

extern INT64 lsa_to_int64 (LOG_LSA lsa);
extern LOG_LSA int64_to_lsa (INT64 value);
#if defined (ENABLE_UNUSED_FUNCTION)
extern char *lsa_to_string (char *buf, int buf_size, LOG_LSA * lsa);
extern char *oid_to_string (char *buf, int buf_size, OID * oid);
extern char *vpid_to_string (char *buf, int buf_size, VPID * vpid);
extern char *vfid_to_string (char *buf, int buf_size, VFID * vfid);
extern char *hfid_to_string (char *buf, int buf_size, HFID * hfid);
#endif /* ENABLE_UNUSED_FUNCTION */
extern char *btid_to_string (char *buf, int buf_size, BTID * btid);
extern BTID string_to_btid (const char *buffer);

extern const char *page_type_to_string (PAGE_TYPE ptype);
#endif /* _STORAGE_COMMON_H_ */
