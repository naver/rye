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
 * util_admin.c - a front end of admin utilities
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "config.h"
#ifdef HAVE_GETOPT_H
#include <getopt.h>
#else
#include "getopt.h"
#endif
#include "utility.h"
#include "error_code.h"
#include "util_support.h"
#include "backup_cl.h"

static UTIL_ARG_MAP ua_Create_Option_Map[] = {
  {OPTION_STRING_TABLE, {0}, {0}},
#if defined (ENABLE_UNUSED_FUNCTION)
  {CREATE_PAGES_S, {ARG_INTEGER}, {-1}},
  {CREATE_COMMENT_S, {ARG_STRING}, {0}},
  {CREATE_FILE_PATH_S, {ARG_STRING}, {0}},
  {CREATE_LOG_PATH_S, {ARG_STRING}, {0}},
#endif
  {CREATE_REPLACE_S, {ARG_BOOLEAN}, {0}},
#if defined (ENABLE_UNUSED_FUNCTION)
  {CREATE_MORE_VOLUME_FILE_S, {ARG_STRING}, {0}},
  {CREATE_USER_DEFINITION_FILE_S, {ARG_STRING}, {0}},
#endif
  {CREATE_RSQL_INITIALIZATION_FILE_S, {ARG_STRING}, {0}},
  {CREATE_OUTPUT_FILE_S, {ARG_STRING}, {0}},
  {CREATE_VERBOSE_S, {ARG_BOOLEAN}, {0}},
#if defined (ENABLE_UNUSED_FUNCTION)
  {CREATE_LOG_PAGE_COUNT_S, {ARG_INTEGER}, {-1}},
#endif
  {CREATE_DB_VOLUME_SIZE_S, {ARG_STRING}, {0}},
  {CREATE_LOG_VOLUME_SIZE_S, {ARG_STRING}, {0}},
  {0, {0}, {0}}
};

static GETOPT_LONG ua_Create_Option[] = {
#if defined (ENABLE_UNUSED_FUNCTION)
  {CREATE_PAGES_L, 1, 0, CREATE_PAGES_S},
  {CREATE_COMMENT_L, 1, 0, CREATE_COMMENT_S},
  {CREATE_FILE_PATH_L, 1, 0, CREATE_FILE_PATH_S},
  {CREATE_LOG_PATH_L, 1, 0, CREATE_LOG_PATH_S},
#endif
  {CREATE_REPLACE_L, 0, 0, CREATE_REPLACE_S},
#if defined (ENABLE_UNUSED_FUNCTION)
  {CREATE_MORE_VOLUME_FILE_L, 1, 0, CREATE_MORE_VOLUME_FILE_S},
  {CREATE_USER_DEFINITION_FILE_L, 1, 0, CREATE_USER_DEFINITION_FILE_S},
#endif
  {CREATE_RSQL_INITIALIZATION_FILE_L, 1, 0,
   CREATE_RSQL_INITIALIZATION_FILE_S},
  {CREATE_OUTPUT_FILE_L, 1, 0, CREATE_OUTPUT_FILE_S},
  {CREATE_VERBOSE_L, 0, 0, CREATE_VERBOSE_S},
#if defined (ENABLE_UNUSED_FUNCTION)
  {CREATE_CHARSET_L, 1, 0, CREATE_CHARSET_S},
  {CREATE_LOG_PAGE_COUNT_L, 1, 0, CREATE_LOG_PAGE_COUNT_S},
#endif
  {CREATE_DB_VOLUME_SIZE_L, 1, 0, CREATE_DB_VOLUME_SIZE_S},
  {CREATE_LOG_VOLUME_SIZE_L, 1, 0, CREATE_LOG_VOLUME_SIZE_S},
  {0, 0, 0, 0}
};

static UTIL_ARG_MAP ua_Delete_Option_Map[] = {
  {OPTION_STRING_TABLE, {0}, {0}},
  {DELETE_OUTPUT_FILE_S, {ARG_STRING}, {0}},
  {DELETE_DELETE_BACKUP_S, {ARG_BOOLEAN}, {0}},
  {0, {0}, {0}}
};

static GETOPT_LONG ua_Delete_Option[] = {
  {DELETE_OUTPUT_FILE_L, 1, 0, DELETE_OUTPUT_FILE_S},
  {DELETE_DELETE_BACKUP_L, 0, 0, DELETE_DELETE_BACKUP_S},
  {0, 0, 0, 0}
};

static UTIL_ARG_MAP ua_Backup_Option_Map[] = {
  {OPTION_STRING_TABLE, {0}, {0}},
  {BACKUP_DESTINATION_PATH_S, {ARG_STRING}, {0}},
  {BACKUP_OUTPUT_FILE_S, {ARG_STRING}, {0}},
  {BACKUP_REMOVE_ARCHIVE_S, {ARG_BOOLEAN}, {0}},
  {BACKUP_THREAD_COUNT_S, {ARG_INTEGER}, {BK_BACKUP_NUM_THREADS_AUTO}},
  {BACKUP_COMPRESS_S, {ARG_BOOLEAN}, {0}},
  {BACKUP_SLEEP_MSECS_S, {ARG_INTEGER}, {BK_BACKUP_SLEEP_MSECS_AUTO}},
  {BACKUP_FORCE_OVERWRITE_S, {ARG_BOOLEAN}, {0}},
  {BACKUP_MAKE_SLAVE_S, {ARG_BOOLEAN}, {0}},
  {BACKUP_CONNECT_MODE_S, {ARG_STRING}, {0}},
  {0, {0}, {0}}
};

static GETOPT_LONG ua_Backup_Option[] = {
  {BACKUP_DESTINATION_PATH_L, 1, 0, BACKUP_DESTINATION_PATH_S},
  {BACKUP_REMOVE_ARCHIVE_L, 0, 0, BACKUP_REMOVE_ARCHIVE_S},
  {BACKUP_OUTPUT_FILE_L, 1, 0, BACKUP_OUTPUT_FILE_S},
  {BACKUP_THREAD_COUNT_L, 1, 0, BACKUP_THREAD_COUNT_S},
  {BACKUP_COMPRESS_L, 0, 0, BACKUP_COMPRESS_S},
  {BACKUP_SLEEP_MSECS_L, 1, 0, BACKUP_SLEEP_MSECS_S},
  {BACKUP_FORCE_OVERWRITE_L, 0, 0, BACKUP_FORCE_OVERWRITE_S},
  {BACKUP_MAKE_SLAVE_L, 0, 0, BACKUP_MAKE_SLAVE_S},
  {BACKUP_CONNECT_MODE_L, 1, 0, BACKUP_CONNECT_MODE_S},
  {0, 0, 0, 0}
};

static UTIL_ARG_MAP ua_Restore_Option_Map[] = {
  {OPTION_STRING_TABLE, {0}, {0}},
  {RESTORE_UP_TO_DATE_S, {ARG_STRING}, {0}},
  {RESTORE_LIST_S, {ARG_BOOLEAN}, {0}},
  {RESTORE_BACKUP_FILE_PATH_S, {ARG_STRING}, {0}},
#if defined (ENABLE_UNUSED_FUNCTION)
  {RESTORE_LEVEL_S, {ARG_INTEGER}, {0}},
#endif
  {RESTORE_PARTIAL_RECOVERY_S, {ARG_BOOLEAN}, {0}},
  {RESTORE_OUTPUT_FILE_S, {ARG_STRING}, {0}},
  {RESTORE_MAKE_SLAVE_S, {ARG_BOOLEAN}, {0}},
  {0, {0}, {0}}
};

static GETOPT_LONG ua_Restore_Option[] = {
  {RESTORE_UP_TO_DATE_L, 1, 0, RESTORE_UP_TO_DATE_S},
  {RESTORE_LIST_L, 0, 0, RESTORE_LIST_S},
  {RESTORE_BACKUP_FILE_PATH_L, 1, 0, RESTORE_BACKUP_FILE_PATH_S},
#if defined (ENABLE_UNUSED_FUNCTION)
  {RESTORE_LEVEL_L, 1, 0, RESTORE_LEVEL_S},
#endif
  {RESTORE_PARTIAL_RECOVERY_L, 0, 0, RESTORE_PARTIAL_RECOVERY_S},
  {RESTORE_OUTPUT_FILE_L, 1, 0, RESTORE_OUTPUT_FILE_S},
  {RESTORE_MAKE_SLAVE_L, 0, 0, RESTORE_MAKE_SLAVE_S},
  {0, 0, 0, 0}
};

static UTIL_ARG_MAP ua_Addvol_Option_Map[] = {
  {OPTION_STRING_TABLE, {0}, {0}},
#if defined (ENABLE_UNUSED_FUNCTION)
  {ADDVOL_VOLUME_NAME_S, {ARG_STRING}, {0}},
#endif
  {ADDVOL_VOLUME_SIZE_S, {ARG_STRING}, {0}},
#if defined (ENABLE_UNUSED_FUNCTION)
  {ADDVOL_FILE_PATH_S, {ARG_STRING}, {0}},
  {ADDVOL_COMMENT_S, {ARG_STRING}, {0}},
#endif
  {ADDVOL_PURPOSE_S, {ARG_STRING}, {.p = "generic"}},
  {ADDVOL_SA_MODE_S, {ARG_BOOLEAN}, {0}},
  {ADDVOL_CS_MODE_S, {ARG_BOOLEAN}, {0}},
  {ADDVOL_MAX_WRITESIZE_IN_SEC_S, {ARG_STRING}, {0}},
  {0, {0}, {0}}
};

static GETOPT_LONG ua_Addvol_Option[] = {
#if defined (ENABLE_UNUSED_FUNCTION)
  {ADDVOL_VOLUME_NAME_L, 1, 0, ADDVOL_VOLUME_NAME_S},
#endif
  {ADDVOL_VOLUME_SIZE_L, 1, 0, ADDVOL_VOLUME_SIZE_S},
#if defined (ENABLE_UNUSED_FUNCTION)
  {ADDVOL_FILE_PATH_L, 1, 0, ADDVOL_FILE_PATH_S},
  {ADDVOL_COMMENT_L, 1, 0, ADDVOL_COMMENT_S},
#endif
  {ADDVOL_PURPOSE_L, 1, 0, ADDVOL_PURPOSE_S},
  {ADDVOL_SA_MODE_L, 0, 0, ADDVOL_SA_MODE_S},
  {ADDVOL_CS_MODE_L, 0, 0, ADDVOL_CS_MODE_S},
  {ADDVOL_MAX_WRITESIZE_IN_SEC_L, 1, 0, ADDVOL_MAX_WRITESIZE_IN_SEC_S},
  {0, 0, 0, 0}
};

static UTIL_ARG_MAP ua_Space_Option_Map[] = {
  {OPTION_STRING_TABLE, {0}, {0}},
  {SPACE_OUTPUT_FILE_S, {ARG_STRING}, {0}},
  {SPACE_SA_MODE_S, {ARG_BOOLEAN}, {0}},
  {SPACE_CS_MODE_S, {ARG_BOOLEAN}, {0}},
  {SPACE_SIZE_UNIT_S, {ARG_STRING}, {.p = "h"}},
  {SPACE_SUMMARIZE_S, {ARG_BOOLEAN}, {0}},
  {SPACE_PURPOSE_S, {ARG_BOOLEAN}, {0}},
  {0, {0}, {0}}
};

static GETOPT_LONG ua_Space_Option[] = {
  {SPACE_OUTPUT_FILE_L, 1, 0, SPACE_OUTPUT_FILE_S},
  {SPACE_SA_MODE_L, 0, 0, SPACE_SA_MODE_S},
  {SPACE_CS_MODE_L, 0, 0, SPACE_CS_MODE_S},
  {SPACE_SIZE_UNIT_L, 1, 0, SPACE_SIZE_UNIT_S},
  {SPACE_SUMMARIZE_L, 0, 0, SPACE_SUMMARIZE_S},
  {SPACE_PURPOSE_L, 0, 0, SPACE_PURPOSE_S},
  {0, 0, 0, 0}
};

static UTIL_ARG_MAP ua_Lock_Option_Map[] = {
  {OPTION_STRING_TABLE, {0}, {0}},
  {LOCK_OUTPUT_FILE_S, {ARG_STRING}, {0}},
  {0, {0}, {0}}
};

static GETOPT_LONG ua_Lock_Option[] = {
  {LOCK_OUTPUT_FILE_L, 1, 0, LOCK_OUTPUT_FILE_S},
  {0, 0, 0, 0}
};

static UTIL_ARG_MAP ua_Diag_Option_Map[] = {
  {OPTION_STRING_TABLE, {0}, {0}},
  {DIAG_DUMP_TYPE_S, {ARG_INTEGER}, {-1}},
  {DIAG_OUTPUT_FILE_S, {ARG_STRING}, {0}},
  {DIAG_DUMP_RECORDS_S, {ARG_BOOLEAN}, {0}},
  {DIAG_START_LOG_PAGEID_S, {ARG_BIGINT}, {0}},
  {DIAG_NUM_LOG_PAGES_S, {ARG_INTEGER}, {-1}},
  {0, {0}, {0}}
};

static GETOPT_LONG ua_Diag_Option[] = {
  {DIAG_DUMP_TYPE_L, 1, 0, DIAG_DUMP_TYPE_S},
  {DIAG_OUTPUT_FILE_L, 1, 0, DIAG_OUTPUT_FILE_S},
  {DIAG_DUMP_RECORDS_L, 0, 0, DIAG_DUMP_RECORDS_S},
  {DIAG_START_LOG_PAGEID_L, 1, 0, DIAG_START_LOG_PAGEID_S},
  {DIAG_NUM_LOG_PAGES_L, 1, 0, DIAG_NUM_LOG_PAGES_S},
  {0, 0, 0, 0}
};

static UTIL_ARG_MAP ua_Plandump_Option_Map[] = {
  {OPTION_STRING_TABLE, {0}, {0}},
  {PLANDUMP_DROP_S, {ARG_BOOLEAN}, {0}},
  {PLANDUMP_OUTPUT_FILE_S, {ARG_STRING}, {0}},
  {0, {0}, {0}}
};

static GETOPT_LONG ua_Plandump_Option[] = {
  {PLANDUMP_DROP_L, 0, 0, PLANDUMP_DROP_S},
  {PLANDUMP_OUTPUT_FILE_L, 1, 0, PLANDUMP_OUTPUT_FILE_S},
  {0, 0, 0, 0}
};

static UTIL_ARG_MAP ua_Killtran_Option_Map[] = {
  {OPTION_STRING_TABLE, {0}, {0}},
  {KILLTRAN_KILL_TRANSACTION_INDEX_S, {ARG_STRING}, {0}},
  {KILLTRAN_KILL_USER_NAME_S, {ARG_STRING}, {.p = ""}},
  {KILLTRAN_KILL_HOST_NAME_S, {ARG_STRING}, {.p = ""}},
  {KILLTRAN_KILL_PROGRAM_NAME_S, {ARG_STRING}, {.p = ""}},
  {KILLTRAN_KILL_SQL_ID_S, {ARG_STRING}, {0}},
  {KILLTRAN_DBA_PASSWORD_S, {ARG_STRING}, {.p = ""}},
  {KILLTRAN_DISPLAY_INFORMATION_S, {ARG_BOOLEAN}, {0}},
  {KILLTRAN_DISPLAY_CLIENT_INFO_S, {ARG_BOOLEAN}, {0}},
  {KILLTRAN_DISPLAY_QUERY_INFO_S, {ARG_BOOLEAN}, {0}},
  {KILLTRAN_FORCE_S, {ARG_BOOLEAN}, {0}},
  {0, {0}, {0}}
};

static GETOPT_LONG ua_Killtran_Option[] = {
  {KILLTRAN_KILL_TRANSACTION_INDEX_L, 1, 0,
   KILLTRAN_KILL_TRANSACTION_INDEX_S},
  {KILLTRAN_KILL_USER_NAME_L, 1, 0, KILLTRAN_KILL_USER_NAME_S},
  {KILLTRAN_KILL_HOST_NAME_L, 1, 0, KILLTRAN_KILL_HOST_NAME_S},
  {KILLTRAN_KILL_PROGRAM_NAME_L, 1, 0, KILLTRAN_KILL_PROGRAM_NAME_S},
  {KILLTRAN_KILL_SQL_ID_L, 1, 0, KILLTRAN_KILL_SQL_ID_S},
  {KILLTRAN_DBA_PASSWORD_L, 1, 0, KILLTRAN_DBA_PASSWORD_S},
  {KILLTRAN_DISPLAY_INFORMATION_L, 0, 0, KILLTRAN_DISPLAY_INFORMATION_S},
  {KILLTRAN_DISPLAY_CLIENT_INFO_L, 0, 0, KILLTRAN_DISPLAY_CLIENT_INFO_S},
  {KILLTRAN_DISPLAY_QUERY_INFO_L, 0, 0, KILLTRAN_DISPLAY_QUERY_INFO_S},
  {KILLTRAN_FORCE_L, 0, 0, KILLTRAN_FORCE_S},
  {0, 0, 0, 0}
};

static UTIL_ARG_MAP ua_Tranlist_Option_Map[] = {
  {OPTION_STRING_TABLE, {0}, {0}},
  {TRANLIST_USER_S, {ARG_STRING}, {0}},
  {TRANLIST_PASSWORD_S, {ARG_STRING}, {0}},
  {TRANLIST_SUMMARY_S, {ARG_BOOLEAN}, {0}},
  {TRANLIST_SORT_KEY_S, {ARG_INTEGER}, {0}},
  {TRANLIST_REVERSE_S, {ARG_BOOLEAN}, {0}},
  {0, {0}, {0}}
};

static GETOPT_LONG ua_Tranlist_Option[] = {
  {TRANLIST_USER_L, 1, 0, TRANLIST_USER_S},
  {TRANLIST_PASSWORD_L, 1, 0, TRANLIST_PASSWORD_S},
  {TRANLIST_SUMMARY_L, 0, 0, TRANLIST_SUMMARY_S},
  {TRANLIST_SORT_KEY_L, 1, 0, TRANLIST_SORT_KEY_S},
  {TRANLIST_REVERSE_L, 0, 0, TRANLIST_REVERSE_S},
  {0, 0, 0, 0}
};

static UTIL_ARG_MAP ua_Paramdump_Option_Map[] = {
  {OPTION_STRING_TABLE, {ARG_INTEGER}, {0}},
  {PARAMDUMP_OUTPUT_FILE_S, {ARG_STRING}, {0}},
  {PARAMDUMP_BOTH_S, {ARG_BOOLEAN}, {0}},
  {PARAMDUMP_SA_MODE_S, {ARG_BOOLEAN}, {0}},
  {PARAMDUMP_CS_MODE_S, {ARG_BOOLEAN}, {0}},
  {0, {0}, {0}}
};

static GETOPT_LONG ua_Paramdump_Option[] = {
  {PARAMDUMP_OUTPUT_FILE_L, 1, 0, PARAMDUMP_OUTPUT_FILE_S},
  {PARAMDUMP_BOTH_L, 0, 0, PARAMDUMP_BOTH_S},
  {PARAMDUMP_SA_MODE_L, 0, 0, PARAMDUMP_SA_MODE_S},
  {PARAMDUMP_CS_MODE_L, 0, 0, PARAMDUMP_CS_MODE_S},
  {0, 0, 0, 0}
};

static UTIL_ARG_MAP ua_Statdump_Option_Map[] = {
  {OPTION_STRING_TABLE, {ARG_INTEGER}, {0}},
  {STATDUMP_OUTPUT_FILE_S, {ARG_STRING}, {0}},
  {STATDUMP_INTERVAL_S, {ARG_INTEGER}, {0}},
  {STATDUMP_CUMULATIVE_S, {ARG_BOOLEAN}, {0}},
  {STATDUMP_SUBSTR_S, {ARG_STRING}, {0}},
  {STATDUMP_OUTPUT_TYPE_S, {ARG_STRING}, {0}},
  {0, {0}, {0}}
};

static GETOPT_LONG ua_Statdump_Option[] = {
  {STATDUMP_OUTPUT_FILE_L, 1, 0, STATDUMP_OUTPUT_FILE_S},
  {STATDUMP_INTERVAL_L, 1, 0, STATDUMP_INTERVAL_S},
  {STATDUMP_CUMULATIVE_L, 0, 0, STATDUMP_CUMULATIVE_S},
  {STATDUMP_SUBSTR_L, 1, 0, STATDUMP_SUBSTR_S},
  {STATDUMP_OUTPUT_TYPE_L, 1, 0, STATDUMP_OUTPUT_TYPE_S},
  {0, 0, 0, 0}
};

static UTIL_MAP ua_Utility_Map[] = {
  {CREATEDB, SA_ONLY, 2, UTIL_OPTION_CREATEDB, "createdb",
   ua_Create_Option, ua_Create_Option_Map},
  {DELETEDB, SA_ONLY, 1, UTIL_OPTION_DELETEDB, "deletedb",
   ua_Delete_Option, ua_Delete_Option_Map},
  {BACKUPDB, SA_CS, 1, UTIL_OPTION_BACKUPDB, "backupdb",
   ua_Backup_Option, ua_Backup_Option_Map},
  {RESTOREDB, SA_ONLY, 1, UTIL_OPTION_RESTOREDB, "restoredb",
   ua_Restore_Option, ua_Restore_Option_Map},
  {ADDVOLDB, SA_CS, 2, UTIL_OPTION_ADDVOLDB, "addvoldb",
   ua_Addvol_Option, ua_Addvol_Option_Map},
  {SPACEDB, SA_CS, 1, UTIL_OPTION_SPACEDB, "spacedb",
   ua_Space_Option, ua_Space_Option_Map},
  {LOCKDB, CS_ONLY, 1, UTIL_OPTION_LOCKDB, "lockdb",
   ua_Lock_Option, ua_Lock_Option_Map},
  {KILLTRAN, CS_ONLY, 1, UTIL_OPTION_KILLTRAN, "killtran",
   ua_Killtran_Option, ua_Killtran_Option_Map},
  {DIAGDB, SA_ONLY, 1, UTIL_OPTION_DIAGDB, "diagdb",
   ua_Diag_Option, ua_Diag_Option_Map},
  {PLANDUMP, CS_ONLY, 1, UTIL_OPTION_PLANDUMP, "plandump",
   ua_Plandump_Option, ua_Plandump_Option_Map},
  {PARAMDUMP, SA_CS, 1, UTIL_OPTION_PARAMDUMP, "paramdump",
   ua_Paramdump_Option, ua_Paramdump_Option_Map},
  {STATDUMP, CS_ONLY, 1, UTIL_OPTION_STATDUMP, "statdump",
   ua_Statdump_Option, ua_Statdump_Option_Map},
  {TRANLIST, CS_ONLY, 1, UTIL_OPTION_TRANLIST, "tranlist",
   ua_Tranlist_Option, ua_Tranlist_Option_Map},
  {-1, -1, 0, 0, 0, 0, 0}
};

static bool is_util_cs_mode (int utility_index);
static int util_get_utility_index (int *utility_index,
				   const char *utility_name);
static void print_admin_usage (char *argv0);
static void print_admin_version (char *argv0);

/*
 * print_admin_usage - display an usage of this utility
 *
 * return:
 *
 * NOTE:
 */
static void
print_admin_usage (char *argv0)
{
  const char *exec_name;
  exec_name = basename (argv0);
  fprintf (stderr,
	   utility_get_generic_message (MSGCAT_UTIL_GENERIC_ADMIN_USAGE),
	   PRODUCT_STRING, exec_name, exec_name, exec_name);
}

/*
 * print_admin_version - display a version of this utility
 *
 * return:
 *
 * NOTE:
 */
static void
print_admin_version (char *argv0)
{
  const char *exec_name;

  exec_name = basename (argv0);
  fprintf (stderr, utility_get_generic_message (MSGCAT_UTIL_GENERIC_VERSION),
	   exec_name, PRODUCT_STRING);
}

/*
 * main() - a administrator utility's entry point
 *
 * return: EXIT_SUCCESS/EXIT_FAILURE
 *
 * NOTE:
 */
int
util_admin (int argc, char *argv[], bool * is_cs_mode)
{
  int status;
  DSO_HANDLE symbol_handle;
  UTILITY_FUNCTION loaded_function;
  int utility_index;
  bool is_valid_arg = true;
  bool util_exec_mode;

  if (argc > 1 && strcmp (argv[1], "--version") == 0)
    {
      print_admin_version (argv[0]);
      return NO_ERROR;
    }

  if (argc < 2 ||
      util_get_utility_index (&utility_index, argv[1]) != NO_ERROR)
    {
      goto print_usage;
    }

  if (util_parse_argument (&ua_Utility_Map[utility_index], argc - 1, &argv[1])
      != NO_ERROR)
    {
      is_valid_arg = false;
      argc = 2;
    }

  util_exec_mode = is_util_cs_mode (utility_index);
  if (is_cs_mode != NULL && *is_cs_mode != util_exec_mode)
    {
      *is_cs_mode = util_exec_mode;
      return ER_FAILED;
    }


  status = utility_load_symbol (NULL, &symbol_handle,
				ua_Utility_Map[utility_index].function_name);
  if (status == NO_ERROR)
    {
      UTIL_FUNCTION_ARG util_func_arg;
      util_func_arg.arg_map = ua_Utility_Map[utility_index].arg_map;
      util_func_arg.command_name = ua_Utility_Map[utility_index].utility_name;
      util_func_arg.argv0 = argv[0];
      util_func_arg.argv = argv;
      util_func_arg.valid_arg = is_valid_arg;
      loaded_function = (UTILITY_FUNCTION) symbol_handle;
      status = (*loaded_function) (&util_func_arg);
    }
  else
    {
      utility_load_print_error (stderr);
      goto error_exit;
    }

  return status;

print_usage:
  print_admin_usage (argv[0]);

error_exit:
  return ER_FAILED;
}

/*
 * is_util_cs_mode - get utility mode
 *
 * return:
 *
 * NOTE:
 */
static bool
is_util_cs_mode (int utility_index)
{
  int utility_type = ua_Utility_Map[utility_index].utility_type;
  UTIL_ARG_MAP *arg_map = ua_Utility_Map[utility_index].arg_map;

  switch (utility_type)
    {
    case SA_ONLY:
      return false;
    case CS_ONLY:
      return true;
    case SA_CS:
      {
	int i;
	for (i = 0; arg_map[i].arg_ch; i++)
	  {
	    int key = arg_map[i].arg_ch;
	    if ((key == 'C') && arg_map[i].arg_value.p != NULL)
	      {
		return true;
	      }
	    if ((key == 'S') && arg_map[i].arg_value.p != NULL)
	      {
		return false;
	      }
	  }
      }
    }
  return true;
}

/*
 * util_get_utility_index - get an index of the utility by the name
 *
 * return: utility index
 */
static int
util_get_utility_index (int *utility_index, const char *utility_name)
{
  int i;
  for (i = 0, *utility_index = -1; ua_Utility_Map[i].utility_index != -1; i++)
    {
      if (strcasecmp (ua_Utility_Map[i].utility_name, utility_name) == 0)
	{
	  *utility_index = ua_Utility_Map[i].utility_index;
	  break;
	}
    }

  return *utility_index == -1 ? ER_GENERIC_ERROR : NO_ERROR;
}
