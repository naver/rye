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
 * utility.h : Message constant definitions used by the utility
 *
 */

#ifndef _UTILITY_H_
#define _UTILITY_H_

#include <config.h>
#include <stdio.h>
#include "util_func.h"

/*
 * UTILITY MESSAGE SETS
 */

/*
 * Message set id in the message catalog MSGCAT_CATALOG_UTILS.
 * These define the $set numbers within the catalog file of the message
 * for each utility.
 */
typedef enum
{
  MSGCAT_UTIL_SET_GENERIC = 1,
  MSGCAT_UTIL_SET_BACKUPDB = 2,
  MSGCAT_UTIL_SET_CREATEDB = 4,
  MSGCAT_UTIL_SET_DELETEDB = 6,
  MSGCAT_UTIL_SET_MASTER = 9,
  MSGCAT_UTIL_SET_RESTOREDB = 11,
  MSGCAT_UTIL_SET_COMMDB = 15,
  MSGCAT_UTIL_SET_ADDVOLDB = 17,
  MSGCAT_UTIL_SET_SPACEDB = 19,
  MSGCAT_UTIL_SET_MIGDB = 23,
  MSGCAT_UTIL_SET_DIAGDB = 24,
  MSGCAT_UTIL_SET_LOCKDB = 25,
  MSGCAT_UTIL_SET_KILLTRAN = 26,
  MSGCAT_UTIL_SET_PLANDUMP = 37,
  MSGCAT_UTIL_SET_PARAMDUMP = 38,
  MSGCAT_UTIL_SET_COPYLOGDB = 40,
  MSGCAT_UTIL_SET_APPLYLOGDB = 41,
  MSGCAT_UTIL_SET_STATDUMP = 43,
  MSGCAT_UTIL_SET_APPLYINFO = 44,
  MSGCAT_UTIL_SET_ACLDB = 45,
  MSGCAT_UTIL_SET_TRANLIST = 46,
  MSGCAT_UTIL_SET_ANALYZELOGDB = 47
} MSGCAT_UTIL_SET;

/* Message id in the set MSGCAT_UTIL_SET_GENERIC */
typedef enum
{
  MSGCAT_UTIL_GENERIC_BAD_DATABASE_NAME = 1,
  MSGCAT_UTIL_GENERIC_BAD_OUTPUT_FILE = 2,
  MSGCAT_UTIL_GENERIC_BAD_VOLUME_NAME = 6,
  MSGCAT_UTIL_GENERIC_VERSION = 9,
  MSGCAT_UTIL_GENERIC_ADMIN_USAGE = 10,
  MSGCAT_UTIL_GENERIC_SERVICE_INVALID_NAME = 12,
  MSGCAT_UTIL_GENERIC_SERVICE_INVALID_CMD = 13,
  MSGCAT_UTIL_GENERIC_SERVICE_PROPERTY_FAIL = 14,
  MSGCAT_UTIL_GENERIC_START_STOP_3S = 15,
  MSGCAT_UTIL_GENERIC_START_STOP_2S = 16,
  MSGCAT_UTIL_GENERIC_NOT_RUNNING_2S = 17,
  MSGCAT_UTIL_GENERIC_NOT_RUNNING_1S = 18,
  MSGCAT_UTIL_GENERIC_ALREADY_RUNNING_2S = 19,
  MSGCAT_UTIL_GENERIC_ALREADY_RUNNING_1S = 20,
  MSGCAT_UTIL_GENERIC_RESULT = 21,
  MSGCAT_UTIL_GENERIC_MISS_ARGUMENT = 22,
  MSGCAT_UTIL_GENERIC_RYE_USAGE = 23,
  MSGCAT_UTIL_GENERIC_ARGS_OVER = 31,
#if 0				/* unused */
  MSGCAT_UTIL_GENERIC_MISS_DBNAME = 32,
#endif
  MSGCAT_UTIL_GENERIC_DEPRECATED = 33,
  MSGCAT_UTIL_GENERIC_INVALID_PARAMETER = 34,
  MSGCAT_UTIL_GENERIC_NO_MEM = 35,
  MSGCAT_UTIL_GENERIC_NOT_HA_MODE = 36,
  MSGCAT_UTIL_GENERIC_HA_MODE = 37,
  MSGCAT_UTIL_GENERIC_HA_MODE_NOT_LISTED_HA_DB = 38,
  MSGCAT_UTIL_GENERIC_HA_MODE_NOT_LISTED_HA_NODE = 39,
  MSGCAT_UTIL_GENERIC_INVALID_CMD = 40,
  MSGCAT_UTIL_GENERIC_MANAGER_NOT_INSTALLED = 41,
  MSGCAT_UTIL_GENERIC_INVALID_ARGUMENT = 42,
  MSGCAT_UTIL_GENERIC_FILEOPEN_ERROR = 43
} MSGCAT_UTIL_GENERIC_MSG;

/* Message id in the set MSGCAT_UTIL_SET_DELETEDB */
typedef enum
{
  DELETEDB_MSG_USAGE = 60
} MSGCAT_DELETEDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_BACKUPDB */
typedef enum
{
  BACKUPDB_INVALID_THREAD_NUM_OPT = 30,
  BACKUPDB_INVALID_PATH = 31,
  BACKUPDB_NOT_FOUND_HOST = 32,
  BACKUPDB_MSG_USAGE = 60
} MSGCAT_BACKUPDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_COMMDB */
typedef enum
{
  COMMDB_STRING1 = 21,
  COMMDB_STRING2 = 22,
  COMMDB_STRING3 = 23,
  COMMDB_STRING4 = 24,
  COMMDB_STRING5 = 25,
  COMMDB_STRING6 = 26,
  COMMDB_STRING7 = 27,
  COMMDB_STRING8 = 28,
  COMMDB_STRING9 = 29,
  COMMDB_STRING10 = 30,
  COMMDB_STRING11 = 31,
  COMMDB_STRING12 = 32,
  COMMDB_STRING13 = 33,
  COMMDB_STRING14 = 34,
  COMMDB_INVALID_IMMEDIATELY_OPTION = 39
} MSGCAT_COMMDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_CREATEDB */
typedef enum
{
  CREATEDB_MSG_MISSING_USER = 41,
  CREATEDB_MSG_UNKNOWN_CMD = 42,
  CREATEDB_MSG_BAD_OUTPUT = 43,
  CREATEDB_MSG_CREATING = 45,
  CREATEDB_MSG_FAILURE = 46,
  CREATEDB_MSG_BAD_USERFILE = 47,
  CREATEDB_MSG_BAD_RANGE = 48,
  CREATEDB_MSG_INVALID_SIZE = 49,
  CREATEDB_MSG_USAGE = 60
} MSGCAT_CREATEDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_MASTER */
typedef enum
{
  MASTER_MSG_DUPLICATE = 11,
  MASTER_MSG_STARTING = 12,
  MASTER_MSG_EXITING = 13,
  MASTER_MSG_NO_PARAMETERS = 15,
  MASTER_MSG_PROCESS_ERROR = 16,
  MASTER_MSG_SERVER_STATUS = 17,
  MASTER_MSG_SERVER_NOTIFIED = 18,
  MASTER_MSG_SERVER_NOT_FOUND = 19,
  MASTER_MSG_GOING_DOWN = 20,
  MASTER_MSG_FAILOVER_FINISHED = 21
} MSGCAT_MASTER_MSG;

/* Message id in the set MSGCAT_UTIL_SET_RESTOREDB */
typedef enum
{
  RESTOREDB_MSG_BAD_DATE = 19,
  RESTOREDB_MSG_FAILURE = 20,
  RESTOREDB_MSG_NO_BACKUP_FILE = 21,
  RESTOREDB_MSG_USAGE = 60
} MSGCAT_RESTOREDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_ADDVOLDB */
typedef enum
{
  ADDVOLDB_MSG_BAD_NPAGES = 20,
  ADDVOLDB_MSG_BAD_PURPOSE = 21,
  ADDVOLDB_INVALID_MAX_WRITESIZE_IN_SEC = 22,
  ADDVOLDB_MSG_USAGE = 60
} MSGCAT_ADDVOLDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_SPACEDB */
typedef enum
{
  SPACEDB_OUTPUT_TITLE = 15,
  SPACEDB_OUTPUT_FORMAT = 16,
  SPACEDB_MSG_BAD_OUTPUT = 17,
  SPACEDB_OUTPUT_TITLE_TMP_VOL = 18,
  SPACEDB_OUTPUT_TITLE_PAGE = 19,
  SPACEDB_OUTPUT_TITLE_SIZE = 20,
  SPACEDB_OUTPUT_UNDERLINE = 21,

  SPACEDB_OUTPUT_SUMMARIZED_TITLE = 30,
  SPACEDB_OUTPUT_SUMMARIZED_TITLE_PAGE = 31,
  SPACEDB_OUTPUT_SUMMARIZED_TITLE_SIZE = 32,
  SPACEDB_OUTPUT_SUMMARIZED_FORMAT = 33,
  SPACEDB_OUTPUT_SUMMARIZED_UNDERLINE = 34,

  SPACEDB_OUTPUT_PURPOSE_TITLE = SPACEDB_OUTPUT_TITLE,
  SPACEDB_OUTPUT_PURPOSE_TITLE_PAGE = 40,
  SPACEDB_OUTPUT_PURPOSE_TITLE_SIZE = 41,
  SPACEDB_OUTPUT_PURPOSE_FORMAT = 42,
  SPACEDB_OUTPUT_PURPOSE_UNDERLINE = 43,

  SPACEDB_OUTPUT_SUMMARIZED_PURPOSE_TITLE = SPACEDB_OUTPUT_SUMMARIZED_TITLE,
  SPACEDB_OUTPUT_SUMMARIZED_PURPOSE_TITLE_PAGE = 50,
  SPACEDB_OUTPUT_SUMMARIZED_PURPOSE_TITLE_SIZE = 51,
  SPACEDB_OUTPUT_SUMMARIZED_PURPOSE_FORMAT = 52,
  SPACEDB_OUTPUT_SUMMARIZED_PURPOSE_UNDERLINE = 53,
  SPACEDB_MSG_USAGE = 60
} MSGCAT_SPACEDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_DIAGDB */
typedef enum
{
  DIAGDB_MSG_BAD_OUTPUT = 15,
  DIAGDB_MSG_USAGE = 60
} MSGCAT_DIAGDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_LOCKDB */
typedef enum
{
  LOCKDB_MSG_BAD_OUTPUT = 15,
  LOCKDB_MSG_NOT_IN_STANDALONE = 59,
  LOCKDB_MSG_USAGE = 60
} MSGCAT_LOCKDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_TRANLIST */
typedef enum
{
  TRANLIST_MSG_USER_PASSWORD = 20,
  TRANLIST_MSG_SUMMARY_HEADER = 22,
  TRANLIST_MSG_SUMMARY_UNDERSCORE = 23,
  TRANLIST_MSG_SUMMARY_ENTRY = 24,
  TRANLIST_MSG_NONE_TABLE_ENTRIES = 25,
  TRANLIST_MSG_NOT_DBA_USER = 26,
  TRANLIST_MSG_INVALID_SORT_KEY = 27,
  TRANLIST_MSG_QUERY_INFO_HEADER = 32,
  TRANLIST_MSG_QUERY_INFO_ENTRY = 33,
  TRANLIST_MSG_QUERY_INFO_UNDERSCORE = 34,
  TRANLIST_MSG_FULL_INFO_HEADER = 42,
  TRANLIST_MSG_FULL_INFO_ENTRY = 43,
  TRANLIST_MSG_FULL_INFO_UNDERSCORE = 44,
  TRANLIST_MSG_TRAN_INDEX = 45,
  TRANLIST_MSG_SQL_ID = 46,
  TRANLIST_MSG_NOT_IN_STANDALONE = 59,
  TRANLIST_MSG_USAGE = 60
} MSGCAT_TRANLIST_MSG;

/* Message id in the set MSGCAT_UTIL_SET_KILLTRAN */
typedef enum
{
  KILLTRAN_MSG_MANY_ARGS = 20,
  KILLTRAN_MSG_DBA_PASSWORD = 21,
  KILLTRAN_MSG_NO_MATCHES = 26,
  KILLTRAN_MSG_READY_TO_KILL = 27,
  KILLTRAN_MSG_VERIFY = 28,
  KILLTRAN_MSG_KILLING = 29,
  KILLTRAN_MSG_KILL_FAILED = 30,
  KILLTRAN_MSG_KILL_TIMEOUT = 31,
  KILLTRAN_MSG_INVALID_TRANINDEX = 32,
  KILLTRAN_MSG_NOT_IN_STANDALONE = 59,
  KILLTRAN_MSG_USAGE = 60
} MSGCAT_KILLTRAN_MSG;

/* Message id in the set MSGCAT_UTIL_SET_PLANDUMP */
typedef enum
{
  PLANDUMP_MSG_BAD_OUTPUT = 15,
  PLANDUMP_MSG_NOT_IN_STANDALONE = 59,
  PLANDUMP_MSG_USAGE = 60
} MSGCAT_PLANDUMP_MSG;

/* Message id in the set MSGCAT_UTIL_SET_MIGDB */
typedef enum
{
  MIGDB_MSG_TEMPORARY_CLASS_OID = 1,
  MIGDB_MSG_CANT_PRINT_ELO = 2,
  MIGDB_MSG_CANT_ACCESS_LO = 3,
  MIGDB_MSG_CANT_OPEN_LO_FILE = 4,
  MIGDB_MSG_READ_ERROR = 5,
  MIGDB_MSG_WRITE_ERROR = 6,
  MIGDB_MSG_CANT_OPEN_ELO = 7,
  MIGDB_MSG_FH_HASH_FILENAME = 9,
  MIGDB_MSG_FH_NAME = 10,
  MIGDB_MSG_FH_SIZE = 11,
  MIGDB_MSG_FH_PAGE_SIZE = 12,
  MIGDB_MSG_FH_DATA_SIZE = 13,
  MIGDB_MSG_FH_ENTRY_SIZE = 14,
  MIGDB_MSG_FH_ENTRIES_PER_PAGE = 15,
  MIGDB_MSG_FH_CACHED_PAGES = 16,
  MIGDB_MSG_FH_NUM_ENTRIES = 17,
  MIGDB_MSG_FH_NUM_COLLISIONS = 18,
  MIGDB_MSG_FH_HASH_FILENAME2 = 19,
  MIGDB_MSG_FH_NEXT_OVERFLOW_ENTRY = 20,
  MIGDB_MSG_FH_KEY_TYPE = 21,
  MIGDB_MSG_FH_PAGE_HEADERS = 22,
  MIGDB_MSG_FH_LAST_PAGE_HEADER = 23,
  MIGDB_MSG_FH_FREE_PAGE_HEADER = 24,
  MIGDB_MSG_FH_PAGE_BITMAP = 25,
  MIGDB_MSG_FH_PAGE_BITMAP_SIZE = 26
} MSGCAT_MIGDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_PARAMDUMP */
typedef enum
{
  PARAMDUMP_MSG_BAD_OUTPUT = 11,
  PARAMDUMP_MSG_CLIENT_PARAMETER = 21,
  PARAMDUMP_MSG_SERVER_PARAMETER = 22,
  PARAMDUMP_MSG_STANDALONE_PARAMETER = 23,
  PARAMDUMP_MSG_PERSIST_PARAMETER = 24,
  PARAMDUMP_MSG_USAGE = 60
} MSGCAT_PARAMDUMP_MSG;

/* Message id in the set MSGCAT_UTIL_SET_COPYLOGDB */
typedef enum
{
  COPYLOGDB_MSG_BAD_MODE = 11,
  COPYLOGDB_MSG_DBA_PASSWORD = 21,
  COPYLOGDB_MSG_NOT_HA_MODE = 22,
  COPYLOGDB_MSG_HA_NOT_SUPPORT = 58,
  COPYLOGDB_MSG_NOT_IN_STANDALONE = 59,
  COPYLOGDB_MSG_USAGE = 60
} MSGCAT_COPYLOGDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_APPLYLOGDB */
typedef enum
{
  APPLYLOGDB_MSG_DBA_PASSWORD = 21,
  APPLYLOGDB_MSG_NOT_HA_MODE = 22,
  APPLYLOGDB_MSG_HA_NOT_SUPPORT = 58,
  APPLYLOGDB_MSG_NOT_IN_STANDALONE = 59,
  APPLYLOGDB_MSG_USAGE = 60
} MSGCAT_APPLYLOGDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_STATMDUMP */
typedef enum
{
  STATDUMP_MSG_BAD_OUTPUT = 11,
  STATDUMP_MSG_SHM_OPEN = 12,
  STATDUMP_MSG_NOT_IN_STANDALONE = 59,
  STATDUMP_MSG_USAGE = 60
} MSGCAT_STATDUMP_MSG;

/* Message id in the set MSGCAT_UTIL_SET_APPLYINFO */
typedef enum
{
  APPLYINFO_MSG_DBA_PASSWORD = 21,
  APPLYINFO_MSG_NOT_HA_MODE = 22,
  APPLYINFO_MSG_HA_NOT_SUPPORT = 58,
  APPLYINFO_MSG_NOT_IN_STANDALONE = 59,
  APPLYINFO_MSG_USAGE = 60
} MSGCAT_APPLYINFO_MSG;

/* Message id in the set MSGCAT_UTIL_SET_ACLDB */
typedef enum
{
  ACLDB_MSG_NOT_IN_STANDALONE = 59,
  ACLDB_MSG_USAGE = 60
} MSGCAT_ACLDB_MSG;

/* Message id in the set MSGCAT_UTIL_SET_ANALYZELOGDB */
typedef enum
{
  ANALYZELOGDB_MSG_DBA_PASSWORD = 21,
  ANALYZELOGDB_MSG_NOT_HA_MODE = 22,
  ANALYZELOGDB_MSG_HA_NOT_SUPPORT = 58,
  ANALYZELOGDB_MSG_NOT_IN_STANDALONE = 59,
  ANALYZELOGDB_MSG_USAGE = 60
} MSGCAT_ANALYZELOGDB_MSG;

typedef void *DSO_HANDLE;

typedef enum
{
  CREATEDB,
  DELETEDB,
  BACKUPDB,
  RESTOREDB,
  ADDVOLDB,
  SPACEDB,
  LOCKDB,
  KILLTRAN,
  DIAGDB,
  PLANDUMP,
  PARAMDUMP,
  STATDUMP,
  COPYLOGDB,
  APPLYLOGDB,
  APPLYINFO,
  ACLDB,
  TRANLIST,
  ANALYZELOGDB
} UTIL_INDEX;

typedef enum
{
  SA_ONLY,
  CS_ONLY,
  SA_CS
} UTIL_MODE;

typedef enum
{
  ARG_INTEGER,
  ARG_STRING,
  ARG_BOOLEAN,
  ARG_BIGINT
} UTIL_ARG_TYPE;

typedef struct option GETOPT_LONG;

typedef struct
{
  int arg_ch;
  union
  {
    int value_type;		/* if arg_ch is not OPTION_STRING_TABLE */
    int num_strings;		/* if arg_ch is OPTION_STRING_TABLE */
  } value_info;
  union
  {
    int i;
    INT64 l;
    const void *p;
  } arg_value;
} UTIL_ARG_MAP;

typedef struct
{
  int utility_index;
  int utility_type;
  int need_args_num;
  const char *utility_name;
  const char *function_name;
  GETOPT_LONG *getopt_long;
  UTIL_ARG_MAP *arg_map;
} UTIL_MAP;

typedef struct _node_config
{
  char *node_name;
  char *copy_log_base;
} HA_NODE_CONF;

typedef struct _ha_config
{
  char **db_names;

  int num_node_conf;
  int max_log_applier;

  HA_NODE_CONF *node_conf;
} HA_CONF;

#define OPTION_STRING_TABLE                     10000

#define UTIL_EXE_EXT            ""

#define UTIL_MASTER_NAME        "rye_master" UTIL_EXE_EXT
#define UTIL_SERVER_NAME        "rye_server" UTIL_EXE_EXT
#define UTIL_ADMIN_SA_NAME      "rye_admin_sa" UTIL_EXE_EXT
#define UTIL_RSQL_NAME          "rsql" UTIL_EXE_EXT
#define UTIL_RYE_REL_NAME       "rye_rel" UTIL_EXE_EXT
#define UTIL_RYE_NAME           "rye" UTIL_EXE_EXT
#define UTIL_REPL_NAME          "rye_repl" UTIL_EXE_EXT

#define PROPERTY_ON             "on"
#define PROPERTY_OFF            "off"

#define PRINT_SERVICE_NAME	"rye service"
#define PRINT_MASTER_NAME       "rye master"
#define PRINT_SERVER_NAME       "rye server"
#define PRINT_BROKER_NAME       "rye broker"
#define PRINT_HEARTBEAT_NAME    "rye heartbeat"
#define PRINT_HA_PROCS_NAME     "HA processes"
#define PRINT_RYE_SHM_NAME      "rye shm"

#define PRINT_CMD_SERVICE       "service"
#define PRINT_CMD_BROKER        "broker"
#define PRINT_CMD_SERVER        "server"
#define PRINT_CMD_START         "start"
#define PRINT_CMD_STOP          "stop"
#define PRINT_CMD_STATUS        "status"
#define PRINT_CMD_LIST          "list"
#define PRINT_CMD_RELOAD        "reload"
#define PRINT_CMD_CHANGEMODE    "changemode"
#define PRINT_CMD_ACL           "acl"
#define PRINT_CMD_GETID         "getid"
#define PRINT_CMD_TEST          "test"

#define PRINT_RESULT_SUCCESS    "success"
#define PRINT_RESULT_FAIL       "fail"

#define CHECK_SERVER            "Server"
#define CHECK_HA_SERVER         "HA-Server"

#define ACLDB_RELOAD            "-r"

#define MASK_ALL                0xFF
#define MASK_SERVICE            0x01
#define MASK_SERVER             0x02
#define MASK_BROKER             0x04
#define MASK_MANAGER            0x08
#define MASK_ADMIN              0x20
#define MASK_HEARTBEAT          0x40

/* utility option list */
#define UTIL_OPTION_CREATEDB                    "createdb"
#define UTIL_OPTION_DELETEDB                    "deletedb"
#define UTIL_OPTION_BACKUPDB                    "backupdb"
#define UTIL_OPTION_RESTOREDB                   "restoredb"
#define UTIL_OPTION_ADDVOLDB                    "addvoldb"
#define UTIL_OPTION_SPACEDB                     "spacedb"
#define UTIL_OPTION_LOCKDB                      "lockdb"
#define UTIL_OPTION_TRANLIST                    "tranlist"
#define UTIL_OPTION_KILLTRAN                    "killtran"
#define UTIL_OPTION_DIAGDB                      "diagdb"
#define UTIL_OPTION_PLANDUMP                    "plandump"
#define UTIL_OPTION_PARAMDUMP                   "paramdump"
#define UTIL_OPTION_STATDUMP                    "statdump"
#define UTIL_OPTION_ACLDB			"acldb"

/* createdb option list */
#define CREATE_REPLACE_S                        'r'
#define CREATE_REPLACE_L                        "replace"
#define CREATE_RSQL_INITIALIZATION_FILE_S       10109
#define CREATE_RSQL_INITIALIZATION_FILE_L       "rsql-initialization-file"
#define CREATE_OUTPUT_FILE_S                    'o'
#define CREATE_OUTPUT_FILE_L                    "output-file"
#define CREATE_VERBOSE_S                        'v'
#define CREATE_VERBOSE_L                        "verbose"
#define CREATE_DB_VOLUME_SIZE_S                 10115
#define CREATE_DB_VOLUME_SIZE_L                 "db-volume-size"
#define CREATE_LOG_VOLUME_SIZE_S                10116
#define CREATE_LOG_VOLUME_SIZE_L                "log-volume-size"

/* deletedb option list */
#define DELETE_OUTPUT_FILE_S                    'o'
#define DELETE_OUTPUT_FILE_L                    "output-file"
#define DELETE_DELETE_BACKUP_S                  'd'
#define DELETE_DELETE_BACKUP_L                  "delete-backup"

/* backupdb option list */
#define BACKUP_DESTINATION_PATH_S               'D'
#define BACKUP_DESTINATION_PATH_L		"destination-path"
#define BACKUP_REMOVE_ARCHIVE_S                 'r'
#define BACKUP_REMOVE_ARCHIVE_L                 "remove-archive"
#define BACKUP_OUTPUT_FILE_S                    'o'
#define BACKUP_OUTPUT_FILE_L                    "output-file"
#define BACKUP_THREAD_COUNT_S                   't'
#define BACKUP_THREAD_COUNT_L                   "thread-count"
#define BACKUP_COMPRESS_S                       'z'
#define BACKUP_COMPRESS_L                       "compress"
#define BACKUP_SLEEP_MSECS_S                    10600
#define BACKUP_SLEEP_MSECS_L                    "sleep-msecs"
#define BACKUP_FORCE_OVERWRITE_S                'f'
#define BACKUP_FORCE_OVERWRITE_L                "force-overwrite"
#define BACKUP_MAKE_SLAVE_S                     'm'
#define BACKUP_MAKE_SLAVE_L                     "make-slave"
#define BACKUP_CONNECT_MODE_S                   'c'
#define BACKUP_CONNECT_MODE_L                   "connect-mode"

/* restoredb option list */
#define RESTORE_UP_TO_DATE_S                    'd'
#define RESTORE_UP_TO_DATE_L                    "up-to-date"
#define RESTORE_LIST_S                          10601
#define RESTORE_LIST_L                          "list"
#define RESTORE_BACKUP_FILE_PATH_S              'B'
#define RESTORE_BACKUP_FILE_PATH_L              "backup-file-path"
#define RESTORE_PARTIAL_RECOVERY_S              'p'
#define RESTORE_PARTIAL_RECOVERY_L              "partial-recovery"
#define RESTORE_OUTPUT_FILE_S                   'o'
#define RESTORE_OUTPUT_FILE_L                   "output-file"
#define RESTORE_MAKE_SLAVE_S                    'm'
#define RESTORE_MAKE_SLAVE_L                    "make-slave"

/* addvoldb option list */
#define ADDVOL_PURPOSE_S                        'p'
#define ADDVOL_PURPOSE_L                        "purpose"
#define ADDVOL_SA_MODE_S                        'S'
#define ADDVOL_SA_MODE_L                        "SA-mode"
#define ADDVOL_CS_MODE_S                        'C'
#define ADDVOL_CS_MODE_L                        "CS-mode"
#define ADDVOL_VOLUME_SIZE_S                    10706
#define ADDVOL_VOLUME_SIZE_L                    "db-volume-size"
#define ADDVOL_MAX_WRITESIZE_IN_SEC_S           10707
#define ADDVOL_MAX_WRITESIZE_IN_SEC_L           "max-writesize-in-sec"

/* spacedb option list */
#define SPACE_OUTPUT_FILE_S                     'o'
#define SPACE_OUTPUT_FILE_L                     "output-file"
#define SPACE_SA_MODE_S                         'S'
#define SPACE_SA_MODE_L                         "SA-mode"
#define SPACE_CS_MODE_S                         'C'
#define SPACE_CS_MODE_L                         "CS-mode"
#define SPACE_SIZE_UNIT_S                       10803
#define SPACE_SIZE_UNIT_L                       "size-unit"
#define SPACE_SUMMARIZE_S                       's'
#define SPACE_SUMMARIZE_L                       "summarize"
#define SPACE_PURPOSE_S                         'p'
#define SPACE_PURPOSE_L                         "purpose"

/* lockdb option list */
#define LOCK_OUTPUT_FILE_S                      'o'
#define LOCK_OUTPUT_FILE_L                      "output-file"

/* diagdb option list */
#define DIAG_DUMP_TYPE_S                        'd'
#define DIAG_DUMP_TYPE_L                        "dump-type"
#define DIAG_OUTPUT_FILE_S                     'o'
#define DIAG_OUTPUT_FILE_L                     "output-file"
#define DIAG_DUMP_RECORDS_S                     11201
#define DIAG_DUMP_RECORDS_L                     "dump-records"
#define DIAG_START_LOG_PAGEID_S                 11202
#define DIAG_START_LOG_PAGEID_L                 "start-pageid"
#define DIAG_NUM_LOG_PAGES_S                    11203
#define DIAG_NUM_LOG_PAGES_L                    "num-pages"

/* plandump option list */
#define PLANDUMP_DROP_S			        'd'
#define PLANDUMP_DROP_L                         "drop"
#define PLANDUMP_OUTPUT_FILE_S		        'o'
#define PLANDUMP_OUTPUT_FILE_L                  "output-file"

/* tranlist option list */
#define TRANLIST_USER_S                         'u'
#define TRANLIST_USER_L                         "user"
#define TRANLIST_PASSWORD_S                     'p'
#define TRANLIST_PASSWORD_L                     "password"
#define TRANLIST_SUMMARY_S                      's'
#define TRANLIST_SUMMARY_L                      "summary"
#define TRANLIST_SORT_KEY_S                     'k'
#define TRANLIST_SORT_KEY_L                     "sort-key"
#define TRANLIST_REVERSE_S                      'r'
#define TRANLIST_REVERSE_L                      "reverse"

/* killtran option list */
#define KILLTRAN_KILL_TRANSACTION_INDEX_S       'i'
#define KILLTRAN_KILL_TRANSACTION_INDEX_L       "kill-transaction-index"
#define KILLTRAN_KILL_USER_NAME_S               11701
#define KILLTRAN_KILL_USER_NAME_L               "kill-user-name"
#define KILLTRAN_KILL_HOST_NAME_S               11702
#define KILLTRAN_KILL_HOST_NAME_L               "kill-host-name"
#define KILLTRAN_KILL_PROGRAM_NAME_S            11703
#define KILLTRAN_KILL_PROGRAM_NAME_L            "kill-program-name"
#define KILLTRAN_KILL_SQL_ID_S                  11704
#define KILLTRAN_KILL_SQL_ID_L                  "kill-sql-id"
#define KILLTRAN_DBA_PASSWORD_S                 'p'
#define KILLTRAN_DBA_PASSWORD_L                 "dba-password"
#define KILLTRAN_DISPLAY_INFORMATION_S          'd'
#define KILLTRAN_DISPLAY_INFORMATION_L          "display-information"
#define KILLTRAN_DISPLAY_CLIENT_INFO_S          11705
#define KILLTRAN_DISPLAY_CLIENT_INFO_L          "display-client-information"
#define KILLTRAN_DISPLAY_QUERY_INFO_S           'q'
#define KILLTRAN_DISPLAY_QUERY_INFO_L           "query-exec-info"
#define KILLTRAN_FORCE_S                        'f'
#define KILLTRAN_FORCE_L                        "force"

/* rsql option list */
#define RSQL_SA_MODE_S                          'S'
#define RSQL_SA_MODE_L                          "SA-mode"
#define RSQL_CS_MODE_S                          'C'
#define RSQL_CS_MODE_L                          "CS-mode"
#define RSQL_USER_S                             'u'
#define RSQL_USER_L                             "user"
#define RSQL_PASSWORD_S                         'p'
#define RSQL_PASSWORD_L                         "password"
#define RSQL_ERROR_CONTINUE_S                   'e'
#define RSQL_ERROR_CONTINUE_L                   "error-continue"
#define RSQL_INPUT_FILE_S                       'i'
#define RSQL_INPUT_FILE_L                       "input-file"
#define RSQL_OUTPUT_FILE_S                      'o'
#define RSQL_OUTPUT_FILE_L                      "output-file"
#define RSQL_SINGLE_LINE_S                      's'
#define RSQL_SINGLE_LINE_L                      "single-line"
#define RSQL_COMMAND_S                          'c'
#define RSQL_COMMAND_L                          "command"
#define RSQL_LINE_OUTPUT_S                      'l'
#define RSQL_LINE_OUTPUT_L                      "line-output"
#define RSQL_NO_AUTO_COMMIT_S                   12010
#define RSQL_NO_AUTO_COMMIT_L                   "no-auto-commit"
#define RSQL_NO_PAGER_S                         12011
#define RSQL_NO_PAGER_L                         "no-pager"
#define RSQL_NO_SINGLE_LINE_S                   12013
#define RSQL_NO_SINGLE_LINE_L                   "no-single-line"
#define RSQL_STRING_WIDTH_S                     12014
#define RSQL_STRING_WIDTH_L                     "string-width"
#define RSQL_WRITE_ON_STANDBY_S                 12015
#define RSQL_WRITE_ON_STANDBY_L                 "write-on-standby"
#define RSQL_TIME_OFF_S                         12016
#define RSQL_TIME_OFF_L                         "time-off"

#define HB_STOP_HB_DEACT_IMMEDIATELY_S          'i'
#define HB_STOP_HB_DEACT_IMMEDIATELY_L          "immediately"
#define HB_STOP_HOST_S                          'h'
#define HB_STOP_HOST_L                          "host"

#define HB_CHANGEMODE_MASTER_L                  'master'
#define HB_CHANGEMODE_MASTER_S                  14000
#define HB_CHANGEMODE_SLAVE_L                   'slave'
#define HB_CHANGEMODE_SLAVE_S                   14001
#define HB_CHANGEMODE_FORCE_L                   'force'
#define HB_CHANGEMODE_FORCE_S                   14002

/* paramdump option list */
#define PARAMDUMP_OUTPUT_FILE_S                 'o'
#define PARAMDUMP_OUTPUT_FILE_L                 "output-file"
#define PARAMDUMP_BOTH_S                        'b'
#define PARAMDUMP_BOTH_L                        "both"
#define PARAMDUMP_SA_MODE_S                     'S'
#define PARAMDUMP_SA_MODE_L                     "SA-mode"
#define PARAMDUMP_CS_MODE_S                     'C'
#define PARAMDUMP_CS_MODE_L                     "CS-mode"

/* statdump option list */
#define STATDUMP_OUTPUT_FILE_S                  'o'
#define STATDUMP_OUTPUT_FILE_L                  "output-file"
#define STATDUMP_INTERVAL_S                     'i'
#define STATDUMP_INTERVAL_L                     "interval"
#define STATDUMP_CUMULATIVE_S                   'c'
#define STATDUMP_CUMULATIVE_L                   "cumulative"
#define STATDUMP_SUBSTR_S			's'
#define STATDUMP_SUBSTR_L			"substr"

/* acl option list */
#define ACLDB_RELOAD_S                          'r'
#define ACLDB_RELOAD_L				"reload"

#define VERSION_S                               20000
#define VERSION_L                               "version"

/* analyzelog option list */
#define ANALYZELOG_LOG_PATH_S                     'L'
#define ANALYZELOG_LOG_PATH_L                     "log-path"
#define ANALYZELOG_MAX_MEM_SIZE_S		  21001
#define ANALYZELOG_MAX_MEM_SIZE_L		  "max-mem-size"
#define ANALYZELOG_PLUGIN_S                       'p'
#define ANALYZELOG_PLUGIN_L                       "plugin"
#define ANALYZELOG_SOURCE_DB_S                    's'
#define ANALYZELOG_SOURCE_DB_L                    "source-db-name"

/* replication option list */
#define REPL_LOG_PATH_S                         'L'
#define REPL_LOG_PATH_L                         "log-path"


#define LIB_UTIL_CS_NAME                "libryecs.so"
#define LIB_UTIL_SA_NAME                "libryesa.so"

#define UTILITY_GENERIC_MSG_FUNC_NAME	"utility_get_generic_message"
#define UTILITY_INIT_FUNC_NAME	        "utility_initialize"
typedef int (*UTILITY_INIT_FUNC) (void);

typedef enum
{
  ARG_UNKNOWN,
  ARG_UTIL_SERVICE = 1,		/* MSGCAT_UTIL_GENERIC_RYE_USAGE+ 25 */
  ARG_UTIL_BROKER = 2,		/* MSGCAT_UTIL_GENERIC_RYE_USAGE+ 26 */
  ARG_UTIL_HEARTBEAT = 3,	/* MSGCAT_UTIL_GENERIC_RYE_USAGE+ 27 */
  ARG_UTIL_ADMIN = 4,		/* MSGCAT_UTIL_GENERIC_RYE_USAGE+ 28 */
  ARG_CMD_HELP,
  ARG_CMD_VERSION,
  ARG_CMD_START,
  ARG_CMD_STOP,
  ARG_CMD_RESTART,
  ARG_CMD_STATUS,
  ARG_CMD_LIST,
  ARG_CMD_RELOAD,
  ARG_CMD_CHANGEMODE,
  ARG_CMD_ON,
  ARG_CMD_OFF,
  ARG_CMD_ACCESS_CONTROL,
  ARG_CMD_RESET,
  ARG_CMD_INFO,
  ARG_CMD_TEST,
  ARG_END
} ARG_TYPE;

/* extern functions */
extern int utility_initialize (void);
extern const char *utility_get_generic_message (int message_index);
extern int check_database_name (const char *name);
extern int check_new_database_name (const char *name);
extern int check_volume_name (const char *name);
extern int utility_get_option_int_value (UTIL_ARG_MAP * arg_map, int arg_ch);
extern bool utility_get_option_bool_value (UTIL_ARG_MAP * arg_map,
					   int arg_ch);
extern const char *utility_get_option_string_value (UTIL_ARG_MAP * arg_map,
						    int arg_ch, int index);
extern INT64 utility_get_option_bigint_value (UTIL_ARG_MAP * arg_map,
					      int arg_ch);
extern int utility_get_option_string_table_size (UTIL_ARG_MAP * arg_map);

extern bool util_is_localhost (char *host);

extern void util_free_ha_conf (HA_CONF * ha_conf);
extern int util_make_ha_conf (HA_CONF * ha_conf);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int util_get_num_of_ha_nodes (const char *node_list);
#endif
extern char **util_split_ha_node (const char *str);
#if defined(NDEBUG)
extern void util_redirect_stdout_to_null (void);
#endif
extern int util_byte_to_size_string (char *buf, size_t len, INT64 size_num);
extern int util_size_string_to_byte (INT64 * size_num, const char *size_str);
extern int util_msec_to_time_string (char *buf, size_t len, INT64 msec_num);
extern int util_time_string_to_msec (INT64 * msec_num, const char *time_str);
extern void util_print_deprecated (const char *option);

typedef struct
{
  int keyval;
  const char *keystr;
} UTIL_KEYWORD;

extern int utility_keyword_value (UTIL_KEYWORD * keywords,
				  int *keyval_p, char **keystr_p);
extern int utility_keyword_search (UTIL_KEYWORD * keywords, int *keyval_p,
				   const char **keystr_p);

extern int utility_localtime (const time_t * ts, struct tm *result);

/* admin utility main functions */
typedef struct
{
  UTIL_ARG_MAP *arg_map;
  const char *command_name;
  char *argv0;
  char **argv;
  bool valid_arg;
} UTIL_FUNCTION_ARG;
typedef int (*UTILITY_FUNCTION) (UTIL_FUNCTION_ARG *);

extern int util_admin (int argc, char *argv[], bool * exec_mode);

extern int backupdb (UTIL_FUNCTION_ARG * arg_map);
extern int addvoldb (UTIL_FUNCTION_ARG * arg_map);
extern int spacedb (UTIL_FUNCTION_ARG * arg_map);
extern int lockdb (UTIL_FUNCTION_ARG * arg_map);
extern int tranlist (UTIL_FUNCTION_ARG * arg_map);
extern int killtran (UTIL_FUNCTION_ARG * arg_map);
extern int plandump (UTIL_FUNCTION_ARG * arg_map);
extern int createdb (UTIL_FUNCTION_ARG * arg_map);
extern int deletedb (UTIL_FUNCTION_ARG * arg_map);
extern int restoredb (UTIL_FUNCTION_ARG * arg_map);
extern int diagdb (UTIL_FUNCTION_ARG * arg_map);
extern int paramdump (UTIL_FUNCTION_ARG * arg_map);
extern int statdump (UTIL_FUNCTION_ARG * arg_map);
extern int acldb (UTIL_FUNCTION_ARG * arg_map);

#endif /* _UTILITY_H_ */
