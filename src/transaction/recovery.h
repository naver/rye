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
 * recovery.h: recovery functions (at server)
 */

#ifndef _RECOVERY_H_
#define _RECOVERY_H_

#ident "$Id$"

#include <stdio.h>

#include "log_comm.h"
#include "error_manager.h"
#include "thread.h"

typedef enum
{
  /*
     RULE *********************************************

     NEW ENTRIES SHOULD BE ADDED AT THE BOTTOM OF THE FILE TO AVOID FULL
     RECOMPILATIONS (e.g., the file can be utimed) and to AVOID OLD DATABASES
     TO BE RECOVERED UNDER OLD FILE
   */
  RVDK_NEWVOL = 0,
  RVDK_FORMAT = 1,
  RVDK_INITMAP = 2,
  RVDK_VHDR_SCALLOC = 3,
  RVDK_VHDR_PGALLOC = 4,
  RVDK_IDALLOC = 5,
  RVDK_IDDEALLOC_WITH_VOLHEADER = 6,
  /* Never use this recovery index anymore. Only for backward compatibility */
  RVDK_MAGIC = 7,
  /* Never use this recovery index anymore. Only for backward compatibility */
  RVDK_CHANGE_CREATION = 8,
  /* Never use this recovery index anymore. Only for backward compatibility */
  RVDK_RESET_BOOT_HFID = 9,
  RVDK_LINK_PERM_VOLEXT = 10,

  RVFL_CREATE_TMPFILE = 11, /* not used */
  RVFL_FTAB_CHAIN = 12,
  RVFL_IDSTABLE = 13,
  /* Never use this recovery index anymore. Only for backward compatibility */
  RVFL_MARKED_DELETED = 14,
  RVFL_ALLOCSET_SECT = 15,
  RVFL_ALLOCSET_PAGETB_ADDRESS = 16,
  RVFL_ALLOCSET_NEW = 17,
  RVFL_ALLOCSET_LINK = 18,
  RVFL_ALLOCSET_ADD_PAGES = 19,
  RVFL_ALLOCSET_DELETE_PAGES = 20,
  RVFL_ALLOCSET_SECT_SHIFT = 21,
  RVFL_ALLOCSET_COPY = 22,
  RVFL_FHDR = 23,
  RVFL_FHDR_ADD_LAST_ALLOCSET = 24,
  RVFL_FHDR_REMOVE_LAST_ALLOCSET = 25,
  RVFL_FHDR_CHANGE_LAST_ALLOCSET = 26,
  RVFL_FHDR_ADD_PAGES = 27,
  RVFL_FHDR_MARK_DELETED_PAGES = 28,
  RVFL_FHDR_DELETE_PAGES = 29,
  RVFL_FHDR_FTB_EXPANSION = 30,
  RVFL_FILEDESC = 31,
  RVFL_DES_FIRSTREST_NEXTVPID = 32,
  RVFL_DES_NREST_NEXTVPID = 33,
  RVFL_TRACKER_REGISTER = 34,
  RVFL_LOGICAL_NOOP = 35,

  RVHF_CREATE = 36,
  RVHF_NEWHDR = 37,
  RVHF_NEWPAGE = 38,
  RVHF_STATS = 39,
  RVHF_CHAIN = 40,
  RVHF_INSERT = 41,
  RVHF_DELETE = 42,
  RVHF_DELETE_NEWHOME = 43,
  RVHF_UPDATE = 44,
  RVHF_UPDATE_TYPE = 45,

  RVOVF_NEWPAGE_LOGICAL_UNDO = 46,
  RVOVF_NEWPAGE_INSERT = 47,
  RVOVF_NEWPAGE_LINK = 48,
  RVOVF_PAGE_UPDATE = 49,
  RVOVF_CHANGE_LINK = 50,

  RVEH_REPLACE = 51,
  RVEH_INSERT = 52,
  RVEH_DELETE = 53,
  RVEH_INIT_DIR = 54,
  RVEH_INIT_NEW_DIR_PAGE = 55,
  RVEH_INIT_BUCKET = 56,
  RVEH_CONNECT_BUCKET = 57,
  RVEH_INC_COUNTER = 58,

  RVBT_NDHEADER_INS = 59,
  RVBT_NDRECORD_UPD = 60,
  RVBT_NDRECORD_INS = 61,
  RVBT_NDRECORD_DEL = 62,
  RVBT_DUMMY_1 = 63,
  RVBT_DEL_PGRECORDS = 64,
  RVBT_GET_NEWROOT = 65,
  RVBT_GET_NEWPAGE = 66,

  RVBT_NEW_PGALLOC = 67,
  RVBT_KEYVAL_INSERT = 68,
  RVBT_KEYVAL_DELETE = 69,
  RVBT_COPYPAGE = 70,
  RVBT_NOOP = 71,
  RVBT_INS_PGRECORDS = 72,
  RVBT_CREATE_INDEX = 73,

  RVCT_NEWPAGE = 74,
  RVCT_INSERT = 75,
  RVCT_DELETE = 76,
  RVCT_UPDATE = 77,
  RVCT_NEW_OVFPAGE_LOGICAL_UNDO = 78,

  RVLOG_OUTSIDE_LOGICAL_REDO_NOOP = 79,

  RVREPL_DATA_INSERT = 80,
  RVREPL_DATA_UPDATE = 81,
  RVREPL_DATA_DELETE = 82,
  RVREPL_SCHEMA = 83,

  RVDK_IDDEALLOC_BITMAP_ONLY = 84,
  RVDK_IDDEALLOC_VHDR_ONLY = 85,

  RVDK_INIT_PAGES = 86,

  /* This is the last entry. */
  RCV_INDEX_END,

  RV_NOT_DEFINED = 999
} LOG_RCVINDEX;

/*
 * RECOVERY STRUCTURE SEEN BY RECOVERY FUNCTIONS
 */
typedef struct log_rcv LOG_RCV;
struct log_rcv
{				/* Recovery information */
  PAGE_PTR pgptr;		/* Page to recover. Page should not be free by recovery
				   functions, however it should be set dirty whenever is
				   needed
				 */
  PGLENGTH offset;		/* Offset/slot of data in the above page to recover */
  int length;			/* Length of data */
  const char *data;		/* Replacement data. Pointer becomes invalid once the
				   recovery of the data is finished
				 */
};

/*
 * STRUCTURE ENTRY OF RECOVERY FUNCTIONS
 */

struct rvfun
{
  LOG_RCVINDEX recv_index;	/* For verification   */
  const char *recv_string;
  int (*undofun) (THREAD_ENTRY * thread_p, LOG_RCV * logrcv);
  int (*redofun) (THREAD_ENTRY * thread_p, LOG_RCV * logrcv);
  void (*dump_undofun) (FILE * fp, int length, void *data);
  void (*dump_redofun) (FILE * fp, int length, void *data);
};

extern struct rvfun RV_fun[];

extern const char *rv_rcvindex_string (LOG_RCVINDEX rcvindex);
extern int rv_init_rvfuns (void);

#define RCV_IS_LOGICAL_LOG(vpid, idx)                       \
          ( (((vpid)->volid == NULL_VOLID)                  \
             || ((vpid)->pageid == NULL_PAGEID)            \
             || ((idx) == RVBT_KEYVAL_INSERT) \
             || ((idx) == RVBT_KEYVAL_DELETE) ) ? true : false )

#define RCV_IS_NEWPG_LOG(idx)                       	\
          ( (((idx) == RVBT_GET_NEWROOT)		\
             || ((idx) == RVBT_GET_NEWPAGE)		\
             || ((idx) == RVCT_NEWPAGE)			\
             || ((idx) == RVDK_FORMAT)			\
             || ((idx) == RVDK_INITMAP)			\
             || ((idx) == RVEH_INIT_DIR)		\
             || ((idx) == RVEH_INIT_NEW_DIR_PAGE)	\
             || ((idx) == RVEH_INIT_BUCKET)		\
             || ((idx) == RVFL_FHDR)			\
             || ((idx) == RVFL_FTAB_CHAIN)		\
             || ((idx) == RVHF_NEWHDR)			\
             || ((idx) == RVHF_NEWPAGE)                 \
             || ((idx) == RVOVF_NEWPAGE_INSERT)		\
             || ((idx) == RVOVF_PAGE_UPDATE) ) ? true : false )

#endif /* _RECOVERY_H_ */
