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
 * util_cs.c : Implementations of utilities that operate in both
 *             client/server and standalone modes.
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <signal.h>
#include <errno.h>
#include <arpa/inet.h>

#include "utility.h"
#include "error_manager.h"
#include "message_catalog.h"
#include "system_parameter.h"
#include "environment_variable.h"
#include "databases_file.h"
#include "boot_cl.h"
#include "boot_sr.h"
#include "db.h"
#include "authenticate.h"
#include "server_interface.h"
#include "object_representation.h"
#include "transaction_cl.h"
#include "porting.h"
#include "network_interface_cl.h"
#include "connection_defs.h"
#include "repl_common.h"
#include "schema_manager.h"
#include "locator_cl.h"
#include "dynamic_array.h"
#include "util_func.h"
#include "heartbeat.h"
#include "connection_support.h"
#include "backup_cl.h"
#include "monitor.h"

#define PASSBUF_SIZE 12
#define SPACEDB_NUM_VOL_PURPOSE 5
#define MAX_KILLTRAN_INDEX_LIST_NUM  64
#define MAX_DBNAME_SIZE (64)

typedef enum
{
  SPACEDB_SIZE_UNIT_PAGE = 0,
  SPACEDB_SIZE_UNIT_MBYTES,
  SPACEDB_SIZE_UNIT_GBYTES,
  SPACEDB_SIZE_UNIT_TBYTES,
  SPACEDB_SIZE_UNIT_HUMAN_READABLE
} T_SPACEDB_SIZE_UNIT;

typedef enum
{
  TRANDUMP_SUMMARY,
  TRANDUMP_QUERY_INFO,
  TRANDUMP_FULL_INFO,
  TRANDUMP_CLIENT_INFO
} TRANDUMP_LEVEL;

typedef enum
{
  SORT_COLUMN_TYPE_INT,
  SORT_COLUMN_TYPE_FLOAT,
  SORT_COLUMN_TYPE_STR,
} SORT_COLUMN_TYPE;

typedef enum
{
  BACKUPDB_SLAVE_MASTER,
  BACKUPDB_MASTER_SLAVE,
  BACKUPDB_SLAVE_ONLY,
  BACKUPDB_MASTER_ONLY
} BACKUPDB_CONNECT_ORDER;

#if defined (CS_MODE)
static int tranlist_Sort_column = 0;
static bool tranlist_Sort_desc = false;
#endif

static bool is_Sigint_caught = false;
#if defined (CS_MODE)
static void intr_handler (int sig_no);
#endif

static void backupdb_sig_interrupt_handler (int sig_no);
static bool check_client_alive (void);
static int spacedb_get_size_str (char *buf, UINT64 num_pages,
				 T_SPACEDB_SIZE_UNIT size_unit);
#if defined (CS_MODE)
static int print_tran_entry (const ONE_TRAN_INFO * tran_info,
			     TRANDUMP_LEVEL dump_level);
static int tranlist_cmp_f (const void *p1, const void *p2);
#endif

static HA_STATE
connect_db (const char *db, const PRM_NODE_INFO * node_info)
{
  int error;
  char dbname[MAX_NODE_INFO_STR_LEN + MAX_DBNAME_SIZE + 2];
  char host_str[MAX_NODE_INFO_STR_LEN];

  prm_node_info_to_str (host_str, sizeof (host_str), node_info);

  sprintf (dbname, "%s@%s", db, host_str);
  boot_clear_host_connected ();

  error = db_restart ("backupdb", TRUE, dbname);
  if (error != NO_ERROR)
    {
      return HA_STATE_NA;
    }

  return db_get_server_state ();
}

static int
find_connect_host_index (const char *dbname, const PRM_NODE_LIST * node_list,
			 HA_STATE expect_server_state)
{
  int i;
  HA_STATE server_state;

  AU_DISABLE_PASSWORDS ();
  db_set_client_type (BOOT_CLIENT_READ_ONLY_ADMIN_UTILITY);
  db_login ("DBA", NULL);

  for (i = 0; i < node_list->num_nodes; i++)
    {
      server_state = connect_db (dbname, &node_list->nodes[i]);
      if (server_state == expect_server_state)
	{
	  db_shutdown ();
	  return i;
	}

      db_shutdown ();
    }

  return -1;
}

static int
find_connect_server (char *db_name, PRM_NODE_INFO * db_host,
		     const char *database_name,
		     BACKUPDB_CONNECT_ORDER c_order)
{
  char *ptr;
  int idx = -1;
  PRM_NODE_LIST node_list;

  memset (&node_list, 0, sizeof (node_list));

  strcpy (db_name, database_name);
  ptr = strstr (db_name, "@");
  if (ptr == NULL)
    {
      prm_get_ha_node_list (&node_list);
    }
  else
    {
      prm_split_node_str (&node_list, ptr + 1, false);
      *ptr = '\0';
    }

  if (c_order == BACKUPDB_SLAVE_MASTER)
    {
      idx = find_connect_host_index (db_name, &node_list, HA_STATE_SLAVE);
      if (idx < 0)
	{
	  idx =
	    find_connect_host_index (db_name, &node_list, HA_STATE_MASTER);
	}
    }
  else if (c_order == BACKUPDB_MASTER_SLAVE)
    {
      idx = find_connect_host_index (db_name, &node_list, HA_STATE_MASTER);
      if (idx < 0)
	{
	  idx = find_connect_host_index (db_name, &node_list, HA_STATE_SLAVE);
	}
    }
  else if (c_order == BACKUPDB_SLAVE_ONLY)
    {
      idx = find_connect_host_index (db_name, &node_list, HA_STATE_SLAVE);
    }
  else
    {
      assert (c_order == BACKUPDB_MASTER_ONLY);
      idx = find_connect_host_index (db_name, &node_list, HA_STATE_MASTER);
    }

  if (idx >= 0)
    {
      *db_host = node_list.nodes[idx];
    }
  else
    {
      PRINT_AND_LOG_ERR_MSG (msgcat_message (MSGCAT_CATALOG_UTILS,
					     MSGCAT_UTIL_SET_BACKUPDB,
					     BACKUPDB_NOT_FOUND_HOST));
      return -1;
    }

  return 0;
}

/*
 * backupdb() - backupdb main routine
 *   return: EXIT_SUCCESS/EXIT_FAILURE
 */
int
backupdb (UTIL_FUNCTION_ARG * arg)
{
  UTIL_ARG_MAP *arg_map = arg->arg_map;
  char er_msg_file[PATH_MAX];
  const char *database_name;
  const char *backup_path = NULL;
  bool remove_log_archives = false;
  const char *backup_verbose_file = NULL;
  const char *connect_mode = NULL;
  int backup_num_threads;
  bool compress_flag;
  bool force_overwrite = false;
  bool make_slave = false;
  int sleep_msecs;
  struct stat st_buf;
  char real_pathbuf[PATH_MAX];
  char verbose_file_realpath[PATH_MAX];
  char backup_path_buf[PATH_MAX];
  HA_STATE server_state;
  char db_name[MAX_DBNAME_SIZE];
  char db_host_str[MAX_NODE_INFO_STR_LEN];
  PRM_NODE_INFO db_host_info;
  char db_fullname[MAXHOSTNAMELEN + MAX_DBNAME_SIZE];
  BACKUPDB_CONNECT_ORDER c_order = BACKUPDB_SLAVE_MASTER;

  if (utility_get_option_string_table_size (arg_map) != 1)
    {
      goto print_backup_usage;
    }

  database_name = utility_get_option_string_value (arg_map,
						   OPTION_STRING_TABLE, 0);
  if (database_name == NULL)
    {
      goto print_backup_usage;
    }

  backup_path = utility_get_option_string_value (arg_map,
						 BACKUP_DESTINATION_PATH_S,
						 0);
  remove_log_archives = utility_get_option_bool_value (arg_map,
						       BACKUP_REMOVE_ARCHIVE_S);
  backup_verbose_file = utility_get_option_string_value (arg_map,
							 BACKUP_OUTPUT_FILE_S,
							 0);
  backup_num_threads = utility_get_option_int_value (arg_map,
						     BACKUP_THREAD_COUNT_S);
  compress_flag = utility_get_option_bool_value (arg_map, BACKUP_COMPRESS_S);
  sleep_msecs = utility_get_option_int_value (arg_map, BACKUP_SLEEP_MSECS_S);
  force_overwrite =
    utility_get_option_bool_value (arg_map, BACKUP_FORCE_OVERWRITE_S);
  make_slave = utility_get_option_bool_value (arg_map, BACKUP_MAKE_SLAVE_S);
  connect_mode = utility_get_option_string_value (arg_map,
						  BACKUP_CONNECT_MODE_S, 0);

  /* TODO : Remove multi-thread support. */
  if (backup_num_threads < BK_BACKUP_NUM_THREADS_AUTO)
    {
      goto print_backup_usage;
    }

  if (sleep_msecs < BK_BACKUP_SLEEP_MSECS_AUTO)
    {
      goto print_backup_usage;
    }

  /* error message log file */
  snprintf (er_msg_file, sizeof (er_msg_file) - 1,
	    "%s_%s.err", database_name, arg->command_name);
  er_init (er_msg_file, ER_EXIT_DEFAULT);

  if (lang_init () != NO_ERROR)
    {
      goto error_exit;
    }

  /* extra validation */
  if (check_database_name (database_name))
    {
      goto error_exit;
    }

  if (backup_path == NULL)
    {
      backup_path = getcwd (backup_path_buf, PATH_MAX);
      if (backup_path == NULL)
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_BO_CWD_FAIL, 0);
	  PRINT_AND_LOG_ERR_MSG ("%s\n", db_error_string (3));
	  goto error_exit;
	}
    }
  else
    {
      memset (real_pathbuf, 0, sizeof (real_pathbuf));
      if (realpath (backup_path, real_pathbuf) != NULL)
	{
	  backup_path = real_pathbuf;
	}

      if (stat (backup_path, &st_buf) != 0 || !S_ISDIR (st_buf.st_mode))
	{
	  PRINT_AND_LOG_ERR_MSG (msgcat_message (MSGCAT_CATALOG_UTILS,
						 MSGCAT_UTIL_SET_BACKUPDB,
						 BACKUPDB_INVALID_PATH));
	  goto error_exit;
	}
    }

  if (sysprm_load_and_init (database_name) != NO_ERROR)
    {
      util_log_write_errid (MSGCAT_UTIL_GENERIC_SERVICE_PROPERTY_FAIL);
      goto error_exit;
    }

  if (connect_mode != NULL)
    {
      if (strcasecmp (connect_mode, "sm") == 0)
	{
	  c_order = BACKUPDB_SLAVE_MASTER;
	}
      else if (strcasecmp (connect_mode, "ms") == 0)
	{
	  c_order = BACKUPDB_MASTER_SLAVE;
	}
      else if (strcasecmp (connect_mode, "s") == 0)
	{
	  c_order = BACKUPDB_SLAVE_ONLY;
	}
      else if (strcasecmp (connect_mode, "m") == 0)
	{
	  c_order = BACKUPDB_MASTER_ONLY;
	}
      else
	{
	  goto print_backup_usage;
	}
    }

  if (find_connect_server (db_name, &db_host_info, database_name,
			   c_order) < 0)
    {
      goto error_exit;
    }

  prm_node_info_to_str (db_host_str, sizeof (db_host_str), &db_host_info);
  sprintf (db_fullname, "%s@%s", db_name, db_host_str);

  AU_DISABLE_PASSWORDS ();
  db_set_client_type (BOOT_CLIENT_READ_ONLY_ADMIN_UTILITY);
  db_login ("DBA", NULL);

  if (db_restart (arg->command_name, TRUE, db_fullname) == NO_ERROR)
    {
      /* some other utilities may need interrupt handler too */
      if (os_set_signal_handler (SIGINT,
				 backupdb_sig_interrupt_handler) == SIG_ERR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
	  PRINT_AND_LOG_ERR_MSG ("%s\n", db_error_string (3));
	  db_shutdown ();
	  goto error_exit;
	}

      if (backup_verbose_file
	  && *backup_verbose_file && *backup_verbose_file != '/')
	{
	  char dirname[PATH_MAX];

	  /* resolve relative path */
	  if (getcwd (dirname, PATH_MAX) != NULL)
	    {
	      snprintf (verbose_file_realpath, PATH_MAX - 1, "%s/%s", dirname,
			backup_verbose_file);
	      backup_verbose_file = verbose_file_realpath;
	    }
	}

      is_Sigint_caught = false;
      css_register_check_client_alive_fn (check_client_alive);
      server_state = db_get_server_state ();

      if (bk_run_backup (db_name, &db_host_info, backup_path,
			 backup_verbose_file, backup_num_threads,
			 compress_flag, sleep_msecs,
			 remove_log_archives, force_overwrite,
			 make_slave, server_state) == NO_ERROR)
	{
	  if (db_commit_transaction () != NO_ERROR)
	    {
	      PRINT_AND_LOG_ERR_MSG ("%s\n", db_error_string (3));
	    }
	}
      else
	{
	  PRINT_AND_LOG_ERR_MSG ("%s\n", db_error_string (3));
	  db_shutdown ();
	  goto error_exit;
	}

      db_shutdown ();
    }
  else
    {
      PRINT_AND_LOG_ERR_MSG ("%s\n", db_error_string (3));
      goto error_exit;
    }

  return EXIT_SUCCESS;

print_backup_usage:
  fprintf (stderr, msgcat_message (MSGCAT_CATALOG_UTILS,
				   MSGCAT_UTIL_SET_BACKUPDB,
				   BACKUPDB_MSG_USAGE),
	   basename (arg->argv0));
  util_log_write_errid (MSGCAT_UTIL_GENERIC_INVALID_ARGUMENT);

error_exit:
  return EXIT_FAILURE;
}

/*
 * addvoldb() - addvoldb main routine
 *   return: EXIT_SUCCESS/EXIT_FAILURE
 */
int
addvoldb (UTIL_FUNCTION_ARG * arg)
{
  UTIL_ARG_MAP *arg_map = arg->arg_map;
  char er_msg_file[PATH_MAX];
  const char *database_name;
  INT64 volext_size;
  INT64 volext_max_writesize;
  const char *volext_string_purpose = NULL;
  const char *volext_npages_string = NULL;
  const char *volext_size_str = NULL;
  const char *volext_max_writesize_in_sec_str = NULL;
#if defined (ENABLE_UNUSED_FUNCTION)
  char real_volext_path_buf[PATH_MAX];
#endif
  bool sa_mode;
  DBDEF_VOL_EXT_INFO ext_info;

  ext_info.overwrite = false;
  ext_info.max_npages = 0;
  if (utility_get_option_string_table_size (arg_map) < 1)
    {
      goto print_addvol_usage;
    }

  database_name = utility_get_option_string_value (arg_map,
						   OPTION_STRING_TABLE, 0);
  if (database_name == NULL)
    {
      goto print_addvol_usage;
    }

  volext_npages_string = utility_get_option_string_value (arg_map,
							  OPTION_STRING_TABLE,
							  1);
  if (volext_npages_string)
    {
      util_print_deprecated ("number-of-pages");
      ext_info.max_npages = atoi (volext_npages_string);
    }

  volext_size = 0;
  volext_size_str = utility_get_option_string_value (arg_map,
						     ADDVOL_VOLUME_SIZE_S, 0);
  if (volext_size_str)
    {
      if (util_size_string_to_byte (&volext_size,
				    volext_size_str) != NO_ERROR)
	{
	  goto print_addvol_usage;
	}
    }

  volext_max_writesize_in_sec_str =
    utility_get_option_string_value (arg_map, ADDVOL_MAX_WRITESIZE_IN_SEC_S,
				     0);
  if (volext_max_writesize_in_sec_str)
    {
      if (util_size_string_to_byte (&volext_max_writesize,
				    volext_max_writesize_in_sec_str) !=
	  NO_ERROR)
	{
	  goto print_addvol_usage;
	}
      ext_info.max_writesize_in_sec = volext_max_writesize / ONE_K;
    }
  else
    {
      ext_info.max_writesize_in_sec = 0;
    }

#if defined (ENABLE_UNUSED_FUNCTION)
  ext_info.name = utility_get_option_string_value (arg_map,
						   ADDVOL_VOLUME_NAME_S, 0);
  ext_info.path = utility_get_option_string_value (arg_map,
						   ADDVOL_FILE_PATH_S, 0);
  if (ext_info.path != NULL)
    {
      memset (real_volext_path_buf, 0, sizeof (real_volext_path_buf));
      if (realpath (ext_info.path, real_volext_path_buf) != NULL)
	{
	  ext_info.path = real_volext_path_buf;
	}
    }
#endif
  ext_info.fullname = NULL;
  assert (ext_info.fullname == NULL);

#if defined (ENABLE_UNUSED_FUNCTION)
  ext_info.comments = utility_get_option_string_value (arg_map,
						       ADDVOL_COMMENT_S, 0);
#endif

  volext_string_purpose = utility_get_option_string_value (arg_map,
							   ADDVOL_PURPOSE_S,
							   0);
  if (volext_string_purpose == NULL)
    {
      volext_string_purpose = "generic";
    }

  ext_info.purpose = DISK_PERMVOL_GENERIC_PURPOSE;

  if (strcasecmp (volext_string_purpose, "data") == 0)
    {
      ext_info.purpose = DISK_PERMVOL_DATA_PURPOSE;
    }
  else if (strcasecmp (volext_string_purpose, "index") == 0)
    {
      ext_info.purpose = DISK_PERMVOL_INDEX_PURPOSE;
    }
  else if (strcasecmp (volext_string_purpose, "temp") == 0)
    {
      ext_info.purpose = DISK_PERMVOL_TEMP_PURPOSE;
    }
  else if (strcasecmp (volext_string_purpose, "generic") == 0)
    {
      ext_info.purpose = DISK_PERMVOL_GENERIC_PURPOSE;
    }
  else
    {
      PRINT_AND_LOG_ERR_MSG (msgcat_message (MSGCAT_CATALOG_UTILS,
					     MSGCAT_UTIL_SET_ADDVOLDB,
					     ADDVOLDB_MSG_BAD_PURPOSE),
			     volext_string_purpose);

      goto error_exit;
    }

  sa_mode = utility_get_option_bool_value (arg_map, ADDVOL_SA_MODE_S);
  if (sa_mode && ext_info.max_writesize_in_sec > 0)
    {
      ext_info.max_writesize_in_sec = 0;
      fprintf (stderr, msgcat_message (MSGCAT_CATALOG_UTILS,
				       MSGCAT_UTIL_SET_ADDVOLDB,
				       ADDVOLDB_INVALID_MAX_WRITESIZE_IN_SEC));
    }

  /* extra validation */
#if 1
  assert (ext_info.fullname == NULL);
#endif

  if (check_database_name (database_name)
      || check_volume_name (ext_info.fullname))
    {
      goto error_exit;
    }

  /* error message log file */
  snprintf (er_msg_file, sizeof (er_msg_file) - 1,
	    "%s_%s.err", database_name, arg->command_name);
  er_init (er_msg_file, ER_EXIT_DEFAULT);

  if (lang_init () != NO_ERROR)
    {
      goto error_exit;
    }

  /* tuning system parameters */
  sysprm_set_force (prm_get_name (PRM_ID_PAGE_BUFFER_SIZE), "50M");

  AU_DISABLE_PASSWORDS ();
  db_set_client_type (BOOT_CLIENT_READ_WRITE_ADMIN_UTILITY);
  db_login ("DBA", NULL);

  if (db_restart (arg->command_name, TRUE, database_name) == NO_ERROR)
    {
      if (volext_size == 0)
	{
	  volext_size = prm_get_bigint_value (PRM_ID_DB_VOLUME_SIZE);
	}

      if (ext_info.max_npages == 0)
	{
	  ext_info.max_npages = (int) (volext_size / IO_PAGESIZE);
	}

      if (ext_info.max_npages <= 0)
	{
	  PRINT_AND_LOG_ERR_MSG (msgcat_message (MSGCAT_CATALOG_UTILS,
						 MSGCAT_UTIL_SET_ADDVOLDB,
						 ADDVOLDB_MSG_BAD_NPAGES),
				 ext_info.max_npages);
	  db_shutdown ();
	  goto error_exit;
	}

      if (db_add_volume (&ext_info) == NO_ERROR)
	{
	  db_commit_transaction ();
	}
      else
	{
	  PRINT_AND_LOG_ERR_MSG ("%s\n", db_error_string (3));
	  db_shutdown ();
	  goto error_exit;
	}
      db_shutdown ();
    }
  else
    {
      PRINT_AND_LOG_ERR_MSG ("%s\n", db_error_string (3));
      goto error_exit;
    }

  return EXIT_SUCCESS;

print_addvol_usage:
  fprintf (stderr, msgcat_message (MSGCAT_CATALOG_UTILS,
				   MSGCAT_UTIL_SET_ADDVOLDB,
				   ADDVOLDB_MSG_USAGE),
	   basename (arg->argv0));
  util_log_write_errid (MSGCAT_UTIL_GENERIC_INVALID_ARGUMENT);
error_exit:
  return EXIT_FAILURE;
}

#define VOL_PURPOSE_STRING(VOL_PURPOSE)		\
	    ((VOL_PURPOSE == DISK_PERMVOL_DATA_PURPOSE) ? "DATA"	\
	    : (VOL_PURPOSE == DISK_PERMVOL_INDEX_PURPOSE) ? "INDEX"	\
	    : (VOL_PURPOSE == DISK_PERMVOL_GENERIC_PURPOSE) ? "GENERIC"	\
	    : (VOL_PURPOSE == DISK_TEMPVOL_TEMP_PURPOSE) ? "TEMP TEMP" \
	    : "TEMP")


/*
 * spacedb() - spacedb main routine
 *   return: EXIT_SUCCESS/EXIT_FAILURE
 */
int
spacedb (UTIL_FUNCTION_ARG * arg)
{
  UTIL_ARG_MAP *arg_map = arg->arg_map;
  char er_msg_file[PATH_MAX];
  const char *database_name;
  const char *output_file = NULL;
  int i;
  const char *size_unit;
  DB_VOLPURPOSE vol_purpose;
  T_SPACEDB_SIZE_UNIT size_unit_type;
  char vol_label[PATH_MAX];

  UINT64 db_ntotal_pages, db_nfree_pages;
  UINT64 db_ndata_pages, db_nindex_pages, db_ntemp_pages;

  UINT64 db_summarize_ntotal_pages[SPACEDB_NUM_VOL_PURPOSE];
  UINT64 db_summarize_nfree_pages[SPACEDB_NUM_VOL_PURPOSE];
  UINT64 db_summarize_ndata_pages[SPACEDB_NUM_VOL_PURPOSE];
  UINT64 db_summarize_nindex_pages[SPACEDB_NUM_VOL_PURPOSE];
  UINT64 db_summarize_ntemp_pages[SPACEDB_NUM_VOL_PURPOSE];
  int db_summarize_nvols[SPACEDB_NUM_VOL_PURPOSE];

  bool summarize, purpose;
  FILE *outfp = NULL;
  int nvols, nbest;
  VOLID temp_volid;
  char num_total_str[64], num_free_str[64], num_used_str[64];
  char num_data_used_str[64];
  char num_index_used_str[64];
  char num_temp_used_str[64];
  char io_size_str[64], log_size_str[64];
  VOL_SPACE_INFO space_info;
  MSGCAT_SPACEDB_MSG title_format, output_format, size_title_format,
    underline;

  if (utility_get_option_string_table_size (arg_map) != 1)
    {
      goto print_space_usage;
    }

  database_name = utility_get_option_string_value (arg_map,
						   OPTION_STRING_TABLE, 0);
  if (database_name == NULL)
    {
      goto print_space_usage;
    }

  output_file = utility_get_option_string_value (arg_map,
						 SPACE_OUTPUT_FILE_S, 0);
  size_unit = utility_get_option_string_value (arg_map, SPACE_SIZE_UNIT_S, 0);
  summarize = utility_get_option_bool_value (arg_map, SPACE_SUMMARIZE_S);
  purpose = utility_get_option_bool_value (arg_map, SPACE_PURPOSE_S);

  size_unit_type = SPACEDB_SIZE_UNIT_HUMAN_READABLE;

  if (size_unit != NULL)
    {
      if (strcasecmp (size_unit, "page") == 0)
	{
	  size_unit_type = SPACEDB_SIZE_UNIT_PAGE;
	}
      else if (strcasecmp (size_unit, "m") == 0)
	{
	  size_unit_type = SPACEDB_SIZE_UNIT_MBYTES;
	}
      else if (strcasecmp (size_unit, "g") == 0)
	{
	  size_unit_type = SPACEDB_SIZE_UNIT_GBYTES;
	}
      else if (strcasecmp (size_unit, "t") == 0)
	{
	  size_unit_type = SPACEDB_SIZE_UNIT_TBYTES;
	}
      else if (strcasecmp (size_unit, "h") != 0)
	{
	  /* invalid option string */
	  goto print_space_usage;
	}
    }

  if (output_file == NULL)
    {
      outfp = stdout;
    }
  else
    {
      outfp = fopen (output_file, "w");
      if (outfp == NULL)
	{
	  PRINT_AND_LOG_ERR_MSG (msgcat_message (MSGCAT_CATALOG_UTILS,
						 MSGCAT_UTIL_SET_SPACEDB,
						 SPACEDB_MSG_BAD_OUTPUT),
				 output_file);
	  goto error_exit;
	}
    }

  if (check_database_name (database_name))
    {
      goto error_exit;
    }

  /* error message log file */
  snprintf (er_msg_file, sizeof (er_msg_file) - 1,
	    "%s_%s.err", database_name, arg->command_name);
  er_init (er_msg_file, ER_EXIT_DEFAULT);

  if (lang_init () != NO_ERROR)
    {
      goto error_exit;
    }

  /* tuning system parameters */
  sysprm_set_force (prm_get_name (PRM_ID_PAGE_BUFFER_SIZE), "50M");

  /* should have little copyright herald message ? */
  AU_DISABLE_PASSWORDS ();
  db_set_client_type (BOOT_CLIENT_READ_ONLY_ADMIN_UTILITY);
  db_login ("DBA", NULL);

  if (db_restart (arg->command_name, TRUE, database_name) != NO_ERROR)
    {
      PRINT_AND_LOG_ERR_MSG ("%s\n", db_error_string (3));
      goto error_exit;
    }

  nvols = db_num_volumes ();

  db_ntotal_pages = db_nfree_pages = 0;
  db_ndata_pages = db_nindex_pages = db_ntemp_pages = 0;

  util_byte_to_size_string (io_size_str, 64, IO_PAGESIZE);
  util_byte_to_size_string (log_size_str, 64, LOG_PAGESIZE);

  if (summarize && purpose)
    {
      title_format = SPACEDB_OUTPUT_SUMMARIZED_PURPOSE_TITLE;
      size_title_format =
	(size_unit_type == SPACEDB_SIZE_UNIT_PAGE) ?
	SPACEDB_OUTPUT_SUMMARIZED_PURPOSE_TITLE_PAGE :
	SPACEDB_OUTPUT_SUMMARIZED_PURPOSE_TITLE_SIZE;
      output_format = SPACEDB_OUTPUT_SUMMARIZED_PURPOSE_FORMAT;
      underline = SPACEDB_OUTPUT_SUMMARIZED_PURPOSE_UNDERLINE;
    }
  else if (summarize && !purpose)
    {
      title_format = SPACEDB_OUTPUT_SUMMARIZED_TITLE;
      size_title_format =
	(size_unit_type == SPACEDB_SIZE_UNIT_PAGE) ?
	SPACEDB_OUTPUT_SUMMARIZED_TITLE_PAGE :
	SPACEDB_OUTPUT_SUMMARIZED_TITLE_SIZE;
      output_format = SPACEDB_OUTPUT_SUMMARIZED_FORMAT;
      underline = SPACEDB_OUTPUT_SUMMARIZED_UNDERLINE;
    }
  else if (!summarize && purpose)
    {
      title_format = SPACEDB_OUTPUT_PURPOSE_TITLE;
      size_title_format =
	(size_unit_type == SPACEDB_SIZE_UNIT_PAGE) ?
	SPACEDB_OUTPUT_PURPOSE_TITLE_PAGE : SPACEDB_OUTPUT_PURPOSE_TITLE_SIZE;
      output_format = SPACEDB_OUTPUT_PURPOSE_FORMAT;
      underline = SPACEDB_OUTPUT_PURPOSE_UNDERLINE;
    }
  else				/* !summarize && !purpose */
    {
      title_format = SPACEDB_OUTPUT_TITLE;
      size_title_format =
	(size_unit_type == SPACEDB_SIZE_UNIT_PAGE) ?
	SPACEDB_OUTPUT_TITLE_PAGE : SPACEDB_OUTPUT_TITLE_SIZE;
      output_format = SPACEDB_OUTPUT_FORMAT;
      underline = SPACEDB_OUTPUT_UNDERLINE;
    }

  if (summarize)
    {
      for (i = 0; i < SPACEDB_NUM_VOL_PURPOSE; i++)
	{
	  db_summarize_ntotal_pages[i] = 0;
	  db_summarize_nfree_pages[i] = 0;
	  db_summarize_nvols[i] = 0;

	  if (purpose)
	    {
	      db_summarize_ndata_pages[i] = 0;
	      db_summarize_nindex_pages[i] = 0;
	      db_summarize_ntemp_pages[i] = 0;
	    }
	}
    }

  fprintf (outfp, msgcat_message (MSGCAT_CATALOG_UTILS,
				  MSGCAT_UTIL_SET_SPACEDB,
				  title_format),
	   database_name, io_size_str, log_size_str);

  fprintf (outfp, msgcat_message (MSGCAT_CATALOG_UTILS,
				  MSGCAT_UTIL_SET_SPACEDB,
				  size_title_format));

  for (i = 0; i < nvols; i++)
    {
      if (disk_get_purpose_and_space_info (i, &vol_purpose,
					   &space_info) != NULL_VOLID)
	{
	  if (summarize)
	    {
	      if (vol_purpose < DISK_UNKNOWN_PURPOSE)
		{
		  db_summarize_ntotal_pages[vol_purpose]
		    += space_info.total_pages;
		  db_summarize_nfree_pages[vol_purpose]
		    += space_info.free_pages;
		  db_summarize_nvols[vol_purpose]++;

		  if (purpose)
		    {
		      db_summarize_ndata_pages[vol_purpose] +=
			space_info.used_data_npages;
		      db_summarize_nindex_pages[vol_purpose] +=
			space_info.used_index_npages;
		      db_summarize_ntemp_pages[vol_purpose] +=
			space_info.used_temp_npages;
		    }
		}
	    }
	  else
	    {
	      db_ntotal_pages += space_info.total_pages;
	      db_nfree_pages += space_info.free_pages;

	      if (db_vol_label (i, vol_label) == NULL)
		{
		  strcpy (vol_label, " ");
		}

	      spacedb_get_size_str (num_total_str,
				    (UINT64) space_info.total_pages,
				    size_unit_type);
	      spacedb_get_size_str (num_free_str,
				    (UINT64) space_info.free_pages,
				    size_unit_type);

	      if (purpose)
		{
		  db_ndata_pages += space_info.used_data_npages;
		  db_nindex_pages += space_info.used_index_npages;
		  db_ntemp_pages += space_info.used_temp_npages;

		  spacedb_get_size_str (num_data_used_str,
					(UINT64) space_info.used_data_npages,
					size_unit_type);
		  spacedb_get_size_str (num_index_used_str,
					(UINT64) space_info.used_index_npages,
					size_unit_type);
		  spacedb_get_size_str (num_temp_used_str,
					(UINT64) space_info.used_temp_npages,
					size_unit_type);

		  fprintf (outfp, msgcat_message (MSGCAT_CATALOG_UTILS,
						  MSGCAT_UTIL_SET_SPACEDB,
						  output_format),
			   i, VOL_PURPOSE_STRING (vol_purpose),
			   num_total_str, num_free_str, num_data_used_str,
			   num_index_used_str, num_temp_used_str, vol_label);
		}
	      else
		{
		  fprintf (outfp,
			   msgcat_message (MSGCAT_CATALOG_UTILS,
					   MSGCAT_UTIL_SET_SPACEDB,
					   output_format),
			   i, VOL_PURPOSE_STRING (vol_purpose),
			   num_total_str, num_free_str, vol_label);
		}
	    }
	}
      else
	{
	  PRINT_AND_LOG_ERR_MSG ("%s\n", db_error_string (3));
	  db_shutdown ();
	  goto error_exit;
	}
    }

  /* get bestspace info
   */
  nbest = db_num_bestspace_entries ();
  if (nbest > 0)
    {
      vol_purpose = DISK_PERMVOL_GENERIC_PURPOSE;

      space_info.max_pages = 0;
      space_info.total_pages = 0;
      space_info.free_pages = nbest / 2;
      space_info.used_data_npages = 0;
      space_info.used_index_npages = 0;
      space_info.used_temp_npages = 0;

      if (summarize)
	{
	  if (vol_purpose < DISK_UNKNOWN_PURPOSE)
	    {
	      db_summarize_ntotal_pages[vol_purpose] +=
		space_info.total_pages;
	      db_summarize_nfree_pages[vol_purpose] += space_info.free_pages;
	      //                  db_summarize_nvols[vol_purpose]++;

	      if (purpose)
		{
		  db_summarize_ndata_pages[vol_purpose]
		    += space_info.used_data_npages;
		  db_summarize_nindex_pages[vol_purpose]
		    += space_info.used_index_npages;
		  db_summarize_ntemp_pages[vol_purpose]
		    += space_info.used_temp_npages;
		}
	    }
	}
      else
	{
	  db_ntotal_pages += space_info.total_pages;
	  db_nfree_pages += space_info.free_pages;

#if 1				/* TODO - */
	  //              if (db_vol_label (i, vol_label) == NULL)
	  {
	    strcpy (vol_label, "bestspace");
	  }
#endif

	  spacedb_get_size_str (num_total_str,
				(UINT64) space_info.total_pages,
				size_unit_type);
	  spacedb_get_size_str (num_free_str, (UINT64) space_info.free_pages,
				size_unit_type);

	  if (purpose)
	    {
	      db_ndata_pages += space_info.used_data_npages;
	      db_nindex_pages += space_info.used_index_npages;
	      db_ntemp_pages += space_info.used_temp_npages;

	      spacedb_get_size_str (num_data_used_str,
				    (UINT64) space_info.used_data_npages,
				    size_unit_type);
	      spacedb_get_size_str (num_index_used_str,
				    (UINT64) space_info.used_index_npages,
				    size_unit_type);
	      spacedb_get_size_str (num_temp_used_str,
				    (UINT64) space_info.used_temp_npages,
				    size_unit_type);

	      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_UTILS,
					      MSGCAT_UTIL_SET_SPACEDB,
					      output_format), i,
		       VOL_PURPOSE_STRING (vol_purpose), num_total_str,
		       num_free_str, num_data_used_str, num_index_used_str,
		       num_temp_used_str, vol_label);
	    }
	  else
	    {
	      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_UTILS,
					      MSGCAT_UTIL_SET_SPACEDB,
					      output_format), i,
		       VOL_PURPOSE_STRING (vol_purpose), num_total_str,
		       num_free_str, vol_label);
	    }
	}
    }

  if (nvols > 1 && !summarize)
    {
      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_UTILS,
				      MSGCAT_UTIL_SET_SPACEDB, underline));

      spacedb_get_size_str (num_total_str, db_ntotal_pages, size_unit_type);
      spacedb_get_size_str (num_free_str, db_nfree_pages, size_unit_type);

      if (purpose)
	{
	  spacedb_get_size_str (num_data_used_str, db_ndata_pages,
				size_unit_type);
	  spacedb_get_size_str (num_index_used_str, db_nindex_pages,
				size_unit_type);
	  spacedb_get_size_str (num_temp_used_str, db_ntemp_pages,
				size_unit_type);

	  fprintf (outfp, msgcat_message (MSGCAT_CATALOG_UTILS,
					  MSGCAT_UTIL_SET_SPACEDB,
					  output_format), nvols, " ",
		   num_total_str, num_free_str, num_data_used_str,
		   num_index_used_str, num_temp_used_str, " ");
	}
      else
	{
	  fprintf (outfp, msgcat_message (MSGCAT_CATALOG_UTILS,
					  MSGCAT_UTIL_SET_SPACEDB,
					  output_format), nvols, " ",
		   num_total_str, num_free_str, " ");
	}
    }

  /* Find info on temp volumes */
  nvols = boot_find_number_temp_volumes ();
  temp_volid = boot_find_last_temp ();

  if (!summarize)
    {
      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_UTILS,
				      MSGCAT_UTIL_SET_SPACEDB,
				      SPACEDB_OUTPUT_TITLE_TMP_VOL),
	       database_name, io_size_str);

      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_UTILS,
				      MSGCAT_UTIL_SET_SPACEDB,
				      size_title_format));
      db_ntotal_pages = db_nfree_pages = 0;
      db_ndata_pages = db_nindex_pages = db_ntemp_pages = 0;
    }

  for (i = 0; i < nvols; i++)
    {
      if (disk_get_purpose_and_space_info ((temp_volid + i), &vol_purpose,
					   &space_info) != NULL_VOLID)
	{
	  assert (space_info.used_data_npages == 0
		  && space_info.used_index_npages == 0);

	  if (summarize)
	    {
	      if (vol_purpose < DISK_UNKNOWN_PURPOSE)
		{
		  db_summarize_ntotal_pages[vol_purpose]
		    += space_info.total_pages;
		  db_summarize_nfree_pages[vol_purpose]
		    += space_info.free_pages;
		  db_summarize_nvols[vol_purpose]++;

		  if (purpose)
		    {
		      db_summarize_ntemp_pages[vol_purpose]
			+= space_info.used_temp_npages;
		    }
		}
	    }
	  else
	    {
	      db_ntotal_pages += space_info.total_pages;
	      db_nfree_pages += space_info.free_pages;

	      if (db_vol_label ((temp_volid + i), vol_label) == NULL)
		{
		  strcpy (vol_label, " ");
		}

	      spacedb_get_size_str (num_total_str,
				    (UINT64) space_info.total_pages,
				    size_unit_type);
	      spacedb_get_size_str (num_free_str,
				    (UINT64) space_info.free_pages,
				    size_unit_type);

	      if (purpose)
		{
		  db_ntemp_pages += space_info.used_temp_npages;

		  spacedb_get_size_str (num_data_used_str,
					(UINT64) 0, size_unit_type);
		  spacedb_get_size_str (num_index_used_str,
					(UINT64) 0, size_unit_type);
		  spacedb_get_size_str (num_temp_used_str,
					(UINT64) space_info.used_temp_npages,
					size_unit_type);

		  fprintf (outfp, msgcat_message (MSGCAT_CATALOG_UTILS,
						  MSGCAT_UTIL_SET_SPACEDB,
						  output_format),
			   (temp_volid + i),
			   VOL_PURPOSE_STRING (DISK_TEMPVOL_TEMP_PURPOSE),
			   num_total_str, num_free_str, num_data_used_str,
			   num_index_used_str, num_temp_used_str, vol_label);
		}
	      else
		{
		  fprintf (outfp, msgcat_message (MSGCAT_CATALOG_UTILS,
						  MSGCAT_UTIL_SET_SPACEDB,
						  output_format),
			   (temp_volid + i),
			   VOL_PURPOSE_STRING (DISK_TEMPVOL_TEMP_PURPOSE),
			   num_total_str, num_free_str, vol_label);
		}
	    }
	}
      else
	{
	  PRINT_AND_LOG_ERR_MSG ("%s\n", db_error_string (3));
	  db_shutdown ();
	  goto error_exit;
	}
    }

  if (!summarize)
    {
      if (nvols > 1)
	{
	  fprintf (outfp, msgcat_message (MSGCAT_CATALOG_UTILS,
					  MSGCAT_UTIL_SET_SPACEDB,
					  underline));

	  spacedb_get_size_str (num_total_str, db_ntotal_pages,
				size_unit_type);
	  spacedb_get_size_str (num_free_str, db_nfree_pages, size_unit_type);

	  if (purpose)
	    {
	      spacedb_get_size_str (num_temp_used_str, db_ntemp_pages,
				    size_unit_type);

	      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_UTILS,
					      MSGCAT_UTIL_SET_SPACEDB,
					      output_format), nvols, " ",
		       num_total_str, num_free_str, num_data_used_str,
		       num_index_used_str, num_temp_used_str, " ");
	    }
	  else
	    {
	      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_UTILS,
					      MSGCAT_UTIL_SET_SPACEDB,
					      output_format), nvols, " ",
		       num_total_str, num_free_str, " ");
	    }
	}

    }
  else
    {
      int total_volume_count = 0;

      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_UTILS,
				      MSGCAT_UTIL_SET_SPACEDB, underline));

      for (i = 0; i < SPACEDB_NUM_VOL_PURPOSE; i++)
	{
	  spacedb_get_size_str (num_total_str, db_summarize_ntotal_pages[i],
				size_unit_type);

	  spacedb_get_size_str (num_used_str,
				db_summarize_ntotal_pages[i] -
				db_summarize_nfree_pages[i], size_unit_type);

	  spacedb_get_size_str (num_free_str, db_summarize_nfree_pages[i],
				size_unit_type);

	  db_ntotal_pages += db_summarize_ntotal_pages[i];
	  db_nfree_pages += db_summarize_nfree_pages[i];
	  total_volume_count += db_summarize_nvols[i];

	  if (purpose)
	    {
	      db_ndata_pages += db_summarize_ndata_pages[i];
	      db_nindex_pages += db_summarize_nindex_pages[i];
	      db_ntemp_pages += db_summarize_ntemp_pages[i];

	      spacedb_get_size_str (num_data_used_str,
				    db_summarize_ndata_pages[i],
				    size_unit_type);
	      spacedb_get_size_str (num_index_used_str,
				    db_summarize_nindex_pages[i],
				    size_unit_type);
	      spacedb_get_size_str (num_temp_used_str,
				    db_summarize_ntemp_pages[i],
				    size_unit_type);

	      fprintf (outfp,
		       msgcat_message (MSGCAT_CATALOG_UTILS,
				       MSGCAT_UTIL_SET_SPACEDB,
				       output_format),
		       VOL_PURPOSE_STRING (i), num_total_str, num_used_str,
		       num_free_str, num_data_used_str, num_index_used_str,
		       num_temp_used_str, db_summarize_nvols[i]);
	    }
	  else
	    {
	      fprintf (outfp,
		       msgcat_message (MSGCAT_CATALOG_UTILS,
				       MSGCAT_UTIL_SET_SPACEDB,
				       output_format),
		       VOL_PURPOSE_STRING (i), num_total_str, num_used_str,
		       num_free_str, db_summarize_nvols[i]);
	    }
	}

      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_UTILS,
				      MSGCAT_UTIL_SET_SPACEDB, underline));

      spacedb_get_size_str (num_total_str, db_ntotal_pages, size_unit_type);
      spacedb_get_size_str (num_used_str, db_ntotal_pages - db_nfree_pages,
			    size_unit_type);
      spacedb_get_size_str (num_free_str, db_nfree_pages, size_unit_type);

      if (purpose)
	{
	  spacedb_get_size_str (num_data_used_str,
				db_ndata_pages, size_unit_type);
	  spacedb_get_size_str (num_index_used_str,
				db_nindex_pages, size_unit_type);
	  spacedb_get_size_str (num_temp_used_str,
				db_ntemp_pages, size_unit_type);

	  fprintf (outfp,
		   msgcat_message (MSGCAT_CATALOG_UTILS,
				   MSGCAT_UTIL_SET_SPACEDB,
				   output_format),
		   "TOTAL", num_total_str, num_used_str,
		   num_free_str, num_data_used_str,
		   num_index_used_str, num_temp_used_str, total_volume_count);
	}
      else
	{
	  fprintf (outfp,
		   msgcat_message (MSGCAT_CATALOG_UTILS,
				   MSGCAT_UTIL_SET_SPACEDB,
				   output_format),
		   "TOTAL", num_total_str, num_used_str, num_free_str,
		   total_volume_count);
	}
    }

  db_shutdown ();
  if (outfp != stdout)
    {
      fclose (outfp);
    }

  return EXIT_SUCCESS;

print_space_usage:
  fprintf (stderr, msgcat_message (MSGCAT_CATALOG_UTILS,
				   MSGCAT_UTIL_SET_SPACEDB,
				   SPACEDB_MSG_USAGE), basename (arg->argv0));
  util_log_write_errid (MSGCAT_UTIL_GENERIC_INVALID_ARGUMENT);
error_exit:
  if (outfp != stdout && outfp != NULL)
    {
      fclose (outfp);
    }
  return EXIT_FAILURE;
}

/*
 * lockdb() - lockdb main routine
 *   return: EXIT_SUCCESS/EXIT_FAILURE
 */
int
lockdb (UTIL_FUNCTION_ARG * arg)
{
#if defined (CS_MODE)
  UTIL_ARG_MAP *arg_map = arg->arg_map;
  char er_msg_file[PATH_MAX];
  const char *database_name;
  const char *output_file = NULL;
  FILE *outfp = NULL;

  if (utility_get_option_string_table_size (arg_map) != 1)
    {
      goto print_lock_usage;
    }

  database_name = utility_get_option_string_value (arg_map,
						   OPTION_STRING_TABLE, 0);
  if (database_name == NULL)
    {
      goto print_lock_usage;
    }

  output_file = utility_get_option_string_value (arg_map, LOCK_OUTPUT_FILE_S,
						 0);
  if (output_file == NULL)
    {
      outfp = stdout;
    }
  else
    {
      outfp = fopen (output_file, "w");
      if (outfp == NULL)
	{
	  PRINT_AND_LOG_ERR_MSG (msgcat_message (MSGCAT_CATALOG_UTILS,
						 MSGCAT_UTIL_SET_LOCKDB,
						 LOCKDB_MSG_BAD_OUTPUT),
				 output_file);
	  goto error_exit;
	}
    }

  if (check_database_name (database_name))
    {
      goto error_exit;
    }

  /* error message log file */
  snprintf (er_msg_file, sizeof (er_msg_file) - 1,
	    "%s_%s.err", database_name, arg->command_name);
  er_init (er_msg_file, ER_EXIT_DEFAULT);

  if (lang_init () != NO_ERROR)
    {
      goto error_exit;
    }

  /* should have little copyright herald message ? */
  AU_DISABLE_PASSWORDS ();
  db_set_client_type (BOOT_CLIENT_READ_ONLY_ADMIN_UTILITY);
  db_login ("DBA", NULL);

  if (db_restart (arg->command_name, TRUE, database_name) != NO_ERROR)
    {
      PRINT_AND_LOG_ERR_MSG ("%s\n", db_error_string (3));
      goto error_exit;
    }

#if 0				/* unused */
  db_set_isolation (TRAN_COMMIT_CLASS_UNCOMMIT_INSTANCE);
#endif

  lock_dump (outfp);
  db_shutdown ();

  if (outfp != stdout)
    {
      fclose (outfp);
    }

  return EXIT_SUCCESS;

print_lock_usage:
  fprintf (stderr, msgcat_message (MSGCAT_CATALOG_UTILS,
				   MSGCAT_UTIL_SET_LOCKDB, LOCKDB_MSG_USAGE),
	   basename (arg->argv0));
  util_log_write_errid (MSGCAT_UTIL_GENERIC_INVALID_ARGUMENT);

error_exit:
  if (outfp != stdout && outfp != NULL)
    {
      fclose (outfp);
    }
  return EXIT_FAILURE;
#else /* CS_MODE */
  fprintf (stderr, msgcat_message (MSGCAT_CATALOG_UTILS,
				   MSGCAT_UTIL_SET_LOCKDB,
				   LOCKDB_MSG_NOT_IN_STANDALONE),
	   basename (arg->argv0));
  return EXIT_FAILURE;
#endif /* !CS_MODE */
}

#if defined (CS_MODE)
/*
 * isvalid_transaction() - test if transaction is valid
 *   return: non-zero if valid transaction
 *   tran(in)
 */
static int
isvalid_transaction (const ONE_TRAN_INFO * tran)
{
  int valid;

  valid = (tran != NULL && tran->tran_index != -1
	   && tran->tran_index != tm_Tran_index);

  return valid;
}
#endif

#if defined (CS_MODE)
/*
 * doesmatch_transaction() - test if matching transaction
 *   return: non-zero if the information matches this transaction
 *   tran(in)
 *   tran_index_list(in)
 *   index_list_size(in)
 *   user_name(in)
 *   hostname(in)
 *   progname(in)
 */
static bool
doesmatch_transaction (const ONE_TRAN_INFO * tran, int *tran_index_list,
		       int index_list_size,
		       const char *username, const char *hostname,
		       const char *progname, const char *sql_id)
{
  int i;

  if (isvalid_transaction (tran))
    {
      if ((username != NULL && strcmp (tran->login_name, username) == 0)
	  || (hostname != NULL && strcmp (tran->host_name, hostname) == 0)
	  || (progname != NULL && strcmp (tran->program_name, progname) == 0)
	  || (sql_id != NULL && tran->query_exec_info.sql_id != NULL
	      && strcmp (tran->query_exec_info.sql_id, sql_id) == 0))
	{
	  return true;
	}

      for (i = 0; i < index_list_size; i++)
	{
	  if (tran->tran_index == tran_index_list[i])
	    {
	      return true;
	    }
	}
    }
  return false;
}
#endif

#if defined (CS_MODE)
/*
 * dump_trantb() - Displays information about all the currently
 *                 active transactions
 *   return: none
 *   info(in) :
 *   dump_level(in) :
 */
static void
dump_trantb (TRANS_INFO * info, TRANDUMP_LEVEL dump_level)
{
  int i;
  int num_valid = 0;
  MSGCAT_TRANLIST_MSG header = TRANLIST_MSG_SUMMARY_HEADER;
  MSGCAT_TRANLIST_MSG underscore = TRANLIST_MSG_SUMMARY_UNDERSCORE;

  if (dump_level == TRANDUMP_FULL_INFO)
    {
      header = TRANLIST_MSG_FULL_INFO_HEADER;
      underscore = TRANLIST_MSG_FULL_INFO_UNDERSCORE;
    }
  else if (dump_level == TRANDUMP_QUERY_INFO)
    {
      header = TRANLIST_MSG_QUERY_INFO_HEADER;
      underscore = TRANLIST_MSG_QUERY_INFO_UNDERSCORE;
    }

  if (info != NULL && info->num_trans > 0)
    {
      /*
       * remember that we have to print the messages one at a time, mts_
       * reuses the message buffer on each call.
       */
      for (i = 0; i < info->num_trans; i++)
	{
	  /*
	   * Display transactions in transaction table that seems to be valid
	   */
	  if (isvalid_transaction (&info->tran[i]))
	    {
	      if (num_valid == 0)
		{
		  /* Dump table header */
		  fprintf (stdout, msgcat_message (MSGCAT_CATALOG_UTILS,
						   MSGCAT_UTIL_SET_TRANLIST,
						   header));
		  fprintf (stdout, msgcat_message (MSGCAT_CATALOG_UTILS,
						   MSGCAT_UTIL_SET_TRANLIST,
						   underscore));
		}

	      num_valid++;
	      print_tran_entry (&info->tran[i], dump_level);
	    }
	}
    }

  if (num_valid > 0)
    {
      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_UTILS,
				       MSGCAT_UTIL_SET_TRANLIST, underscore));
    }
  else
    {
      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_UTILS,
				       MSGCAT_UTIL_SET_TRANLIST,
				       TRANLIST_MSG_NONE_TABLE_ENTRIES));
    }

  if (info != NULL
      && (dump_level == TRANDUMP_QUERY_INFO
	  || dump_level == TRANDUMP_FULL_INFO))
    {
      int j;

      fprintf (stdout, "\n");
      /* print query string info */
      for (i = 0; i < info->num_trans; i++)
	{
	  if (isvalid_transaction (&info->tran[i])
	      && !XASL_ID_IS_NULL (&info->tran[i].query_exec_info.xasl_id))
	    {
	      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_UTILS,
					       MSGCAT_UTIL_SET_TRANLIST,
					       TRANLIST_MSG_SQL_ID),
		       info->tran[i].query_exec_info.sql_id);
	      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_UTILS,
					       MSGCAT_UTIL_SET_TRANLIST,
					       TRANLIST_MSG_TRAN_INDEX),
		       info->tran[i].tran_index);

	      for (j = i + 1; j < info->num_trans; j++)
		{
		  if (isvalid_transaction (&info->tran[j])
		      && XASL_ID_EQ (&info->tran[i].query_exec_info.xasl_id,
				     &info->tran[j].query_exec_info.xasl_id))
		    {
		      /* same query */
		      fprintf (stdout, ", %d", info->tran[j].tran_index);
		      /* reset xasl to skip in next search */
		      XASL_ID_SET_NULL (&info->tran[j].query_exec_info.
					xasl_id);
		    }
		}
	      fprintf (stdout, "\n");

	      /* print query statement */
	      fprintf (stdout, "%s\n\n",
		       info->tran[i].query_exec_info.query_stmt);
	    }
	}
    }
}
#endif

#if defined (CS_MODE)
/*
 * kill_transactions() - kill transaction(s)
 *   return: number of killed transactions
 *   info(in/out)
 *   tran_index(in)
 *   username(in)
 *   hostname(in)
 *   progname(in)
 *   verify(in)
 *
 * Note: Kill one or several transactions identified only one of the
 *       above parameters. If the verification flag is set, the user is
 *       prompted for verification wheheter or not to kill the
 *       transaction(s).
 *
 *       if tran_index_list != NULL && != ""
 *         kill transactions in the tran_index comma list.
 *       else if username != NULL && != ""
 *         kill all transactions associated with given user.
 *       else if hostname != NULL && != ""
 *         kill all transactions associated with given host.
 *       else if progname != NULL && != ""
 *         kill all transactions associated with given program.
 *       else
 *         error.
 *
 *    If verify is set, the transactions are only killed after prompting
 *    for verification.
 */
static int
kill_transactions (TRANS_INFO * info, int *tran_index_list, int list_size,
		   const char *username, const char *hostname,
		   const char *progname, const char *sql_id, bool verify)
{
  int i, ok;
  int nkills = 0, nfailures = 0;
  int ch;
  MSGCAT_TRANLIST_MSG header = TRANLIST_MSG_SUMMARY_HEADER;
  MSGCAT_TRANLIST_MSG underscore = TRANLIST_MSG_SUMMARY_UNDERSCORE;
  TRANDUMP_LEVEL dump_level = TRANDUMP_SUMMARY;

  if (sql_id != NULL)
    {
      /* print --query-exec-info table format */
      header = TRANLIST_MSG_QUERY_INFO_HEADER;
      underscore = TRANLIST_MSG_QUERY_INFO_UNDERSCORE;
      dump_level = TRANDUMP_QUERY_INFO;
    }

  /* see if we have anything do do */
  for (i = 0; i < info->num_trans; i++)
    {
      if (doesmatch_transaction (&info->tran[i], tran_index_list, list_size,
				 username, hostname, progname, sql_id))
	{
	  break;
	}
    }

  if (i >= info->num_trans)
    {
      /*
       * There is not matches
       */
      PRINT_AND_LOG_ERR_MSG (msgcat_message (MSGCAT_CATALOG_UTILS,
					     MSGCAT_UTIL_SET_KILLTRAN,
					     KILLTRAN_MSG_NO_MATCHES));
    }
  else
    {
      if (!verify)
	{
	  ok = 1;
	}
      else
	{
	  ok = 0;
	  /*
	   * display the transactin identifiers that we are about to kill
	   */
	  fprintf (stdout, msgcat_message (MSGCAT_CATALOG_UTILS,
					   MSGCAT_UTIL_SET_KILLTRAN,
					   KILLTRAN_MSG_READY_TO_KILL));

	  fprintf (stdout, msgcat_message (MSGCAT_CATALOG_UTILS,
					   MSGCAT_UTIL_SET_TRANLIST, header));
	  fprintf (stdout, msgcat_message (MSGCAT_CATALOG_UTILS,
					   MSGCAT_UTIL_SET_TRANLIST,
					   underscore));

	  for (i = 0; i < info->num_trans; i++)
	    {
	      if (doesmatch_transaction (&info->tran[i], tran_index_list,
					 list_size, username, hostname,
					 progname, sql_id))
		{
		  print_tran_entry (&info->tran[i], dump_level);
		}
	    }
	  fprintf (stdout, msgcat_message (MSGCAT_CATALOG_UTILS,
					   MSGCAT_UTIL_SET_TRANLIST,
					   underscore));

	  fprintf (stdout, msgcat_message (MSGCAT_CATALOG_UTILS,
					   MSGCAT_UTIL_SET_KILLTRAN,
					   KILLTRAN_MSG_VERIFY));
	  fflush (stdout);

	  ch = getc (stdin);
	  if (ch == 'Y' || ch == 'y')
	    {
	      ok = 1;
	    }
	}

      if (ok)
	{
	  for (i = 0; i < info->num_trans; i++)
	    {
	      if (doesmatch_transaction (&info->tran[i], tran_index_list,
					 list_size, username, hostname,
					 progname, sql_id))
		{
		  fprintf (stdout, msgcat_message (MSGCAT_CATALOG_UTILS,
						   MSGCAT_UTIL_SET_KILLTRAN,
						   KILLTRAN_MSG_KILLING),
			   info->tran[i].tran_index);
		  if (thread_kill_tran_index (info->tran[i].tran_index,
					      info->tran[i].db_user,
					      info->tran[i].host_name,
					      info->tran[i].process_id) ==
		      NO_ERROR)
		    {
		      info->tran[i].tran_index = -1;	/* Gone */
		      nkills++;
		    }
		  else
		    {
		      /*
		       * Fail to kill the transaction
		       */
		      if (nfailures == 0)
			{
			  fprintf (stdout,
				   msgcat_message (MSGCAT_CATALOG_UTILS,
						   MSGCAT_UTIL_SET_KILLTRAN,
						   KILLTRAN_MSG_KILL_FAILED));
			  fprintf (stdout,
				   msgcat_message (MSGCAT_CATALOG_UTILS,
						   MSGCAT_UTIL_SET_TRANLIST,
						   header));
			  fprintf (stdout,
				   msgcat_message (MSGCAT_CATALOG_UTILS,
						   MSGCAT_UTIL_SET_TRANLIST,
						   underscore));
			}

		      print_tran_entry (&info->tran[i], dump_level);

		      if (er_errid () != NO_ERROR)
			{
			  PRINT_AND_LOG_ERR_MSG ("%s\n", db_error_string (3));
			}
		      else	/* probably it is the case of timeout */
			{
			  PRINT_AND_LOG_ERR_MSG (msgcat_message
						 (MSGCAT_CATALOG_UTILS,
						  MSGCAT_UTIL_SET_KILLTRAN,
						  KILLTRAN_MSG_KILL_TIMEOUT));
			}
		      nfailures++;
		    }
		}
	    }

	  if (nfailures > 0)
	    {
	      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_UTILS,
					       MSGCAT_UTIL_SET_TRANLIST,
					       underscore));
	    }
	}
    }

  return nkills;
}
#endif

#if defined (CS_MODE)
/*
 * print_tran_entry()
 *   return: NO_ERROR
 *   tran_info(in) :
 *   include_query_info(in) :
 */
static int
print_tran_entry (const ONE_TRAN_INFO * tran_info, TRANDUMP_LEVEL dump_level)
{
  char *buf = NULL;
  char query_buf[32];

  if (tran_info == NULL)
    {
      assert_release (0);
      return ER_FAILED;
    }

  assert_release (dump_level <= TRANDUMP_FULL_INFO);

  if (dump_level == TRANDUMP_FULL_INFO || dump_level == TRANDUMP_QUERY_INFO)
    {
      buf = tran_info->query_exec_info.wait_for_tran_index_string;

      if (tran_info->query_exec_info.query_stmt != NULL)
	{
	  /* print 31 string */
	  strncpy (query_buf, tran_info->query_exec_info.query_stmt, 32);
	  query_buf[31] = '\0';
	}
    }

  if (dump_level == TRANDUMP_FULL_INFO)
    {
      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_UTILS,
				       MSGCAT_UTIL_SET_TRANLIST,
				       TRANLIST_MSG_FULL_INFO_ENTRY),
	       tran_info->tran_index,
	       tran_get_tranlist_state_name (tran_info->state),
	       tran_info->db_user, tran_info->host_name,
	       tran_info->process_id, tran_info->program_name,
	       tran_info->query_exec_info.query_time,
	       tran_info->query_exec_info.tran_time,
	       (buf == NULL ? "-1" : buf),
	       ((tran_info->query_exec_info.sql_id) ? tran_info->
		query_exec_info.sql_id : "*** empty ***"),
	       ((tran_info->query_exec_info.query_stmt) ? query_buf : " "));
    }
  else if (dump_level == TRANDUMP_QUERY_INFO)
    {
      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_UTILS,
				       MSGCAT_UTIL_SET_TRANLIST,
				       TRANLIST_MSG_QUERY_INFO_ENTRY),
	       tran_info->tran_index,
	       tran_get_tranlist_state_name (tran_info->state),
	       tran_info->process_id, tran_info->program_name,
	       tran_info->query_exec_info.query_time,
	       tran_info->query_exec_info.tran_time,
	       (buf == NULL ? "-1" : buf),
	       ((tran_info->query_exec_info.sql_id) ? tran_info->
		query_exec_info.sql_id : "*** empty ***"),
	       ((tran_info->query_exec_info.query_stmt) ? query_buf : " "));
    }
  else
    {
      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_UTILS,
				       MSGCAT_UTIL_SET_TRANLIST,
				       TRANLIST_MSG_SUMMARY_ENTRY),
	       tran_info->tran_index,
	       tran_get_tranlist_state_name (tran_info->state),
	       tran_info->db_user, tran_info->host_name,
	       tran_info->process_id, tran_info->program_name);
    }

  return NO_ERROR;
}
#endif


/*
 * tranlist() -
 *   return: EXIT_SUCCESS/EXIT_FAILURE
 */
int
tranlist (UTIL_FUNCTION_ARG * arg)
{
#if defined (CS_MODE)
  UTIL_ARG_MAP *arg_map = arg->arg_map;
  char er_msg_file[PATH_MAX];
  const char *database_name;
  const char *username;
  const char *password;
  char *passbuf = NULL;
  TRANS_INFO *info = NULL;
  int error;
  bool is_summary, include_query_info;
  TRANDUMP_LEVEL dump_level = TRANDUMP_FULL_INFO;

  if (utility_get_option_string_table_size (arg_map) != 1)
    {
      goto print_tranlist_usage;
    }

  database_name = utility_get_option_string_value (arg_map,
						   OPTION_STRING_TABLE, 0);
  if (database_name == NULL)
    {
      goto print_tranlist_usage;
    }

  username = utility_get_option_string_value (arg_map, TRANLIST_USER_S, 0);
  password =
    utility_get_option_string_value (arg_map, TRANLIST_PASSWORD_S, 0);
  is_summary = utility_get_option_bool_value (arg_map, TRANLIST_SUMMARY_S);
  tranlist_Sort_column =
    utility_get_option_int_value (arg_map, TRANLIST_SORT_KEY_S);
  tranlist_Sort_desc =
    utility_get_option_bool_value (arg_map, TRANLIST_REVERSE_S);

  if (username == NULL)
    {
      /* default : DBA user */
      username = "DBA";
    }

  if (check_database_name (database_name) != NO_ERROR)
    {
      goto error_exit;
    }

  if (tranlist_Sort_column > 10 || tranlist_Sort_column < 0
      || (is_summary && tranlist_Sort_column > 5))
    {
      PRINT_AND_LOG_ERR_MSG (msgcat_message (MSGCAT_CATALOG_UTILS,
					     MSGCAT_UTIL_SET_TRANLIST,
					     TRANLIST_MSG_INVALID_SORT_KEY),
			     tranlist_Sort_column);
      goto error_exit;
    }

  /* error message log file */
  snprintf (er_msg_file, sizeof (er_msg_file) - 1,
	    "%s_%s.err", database_name, arg->command_name);
  er_init (er_msg_file, ER_EXIT_DEFAULT);

  if (lang_init () != NO_ERROR)
    {
      goto error_exit;
    }

  error = db_restart_ex (arg->command_name, database_name, username, password,
			 BOOT_CLIENT_READ_ONLY_ADMIN_UTILITY);
  if (error != NO_ERROR)
    {
      char msg_buf[64];

      if (error == ER_AU_INVALID_PASSWORD && password == NULL)
	{
	  /*
	   * prompt for a valid password and try again, need a reusable
	   * password prompter so we can use getpass() on platforms that
	   * support it.
	   */
	  snprintf (msg_buf, 64, msgcat_message (MSGCAT_CATALOG_UTILS,
						 MSGCAT_UTIL_SET_TRANLIST,
						 TRANLIST_MSG_USER_PASSWORD),
		    username);

	  passbuf = getpass (msg_buf);

	  if (passbuf[0] == '\0')
	    {
	      passbuf = (char *) NULL;
	    }
	  password = passbuf;

	  error = db_restart_ex (arg->command_name, database_name,
				 username, password,
				 BOOT_CLIENT_READ_ONLY_ADMIN_UTILITY);
	}

      if (error != NO_ERROR)
	{
	  PRINT_AND_LOG_ERR_MSG ("%s\n", db_error_string (3));
	  goto error_exit;
	}
    }

  if (!au_is_dba_group_member (au_get_user ()))
    {
      PRINT_AND_LOG_ERR_MSG (msgcat_message (MSGCAT_CATALOG_UTILS,
					     MSGCAT_UTIL_SET_TRANLIST,
					     TRANLIST_MSG_NOT_DBA_USER),
			     username);
      db_shutdown ();
      goto error_exit;
    }

  /*
   * Get the current state of transaction table information. All the
   * transaction kills are going to be based on this information. The
   * transaction information may be changed back in the server if there
   * are new transactions starting and finishing. We need to do this way
   * since verification is required at this level, and we cannot freeze the
   * state of the server ()transaction table).
   */
  include_query_info = !is_summary;

  info = logtb_get_trans_info (include_query_info);
  if (info == NULL)
    {
      util_log_write_errstr ("%s\n", db_error_string (3));
      db_shutdown ();
      goto error_exit;
    }

  if (is_summary)
    {
      dump_level = TRANDUMP_SUMMARY;
    }

  if (tranlist_Sort_column > 0 || tranlist_Sort_desc == true)
    {
      qsort ((void *) info->tran, info->num_trans,
	     sizeof (ONE_TRAN_INFO), tranlist_cmp_f);
    }

  (void) dump_trantb (info, dump_level);

  if (info)
    {
      logtb_free_trans_info (info);
    }

  (void) db_shutdown ();
  return EXIT_SUCCESS;

print_tranlist_usage:
  fprintf (stderr, msgcat_message (MSGCAT_CATALOG_UTILS,
				   MSGCAT_UTIL_SET_TRANLIST,
				   TRANLIST_MSG_USAGE),
	   basename (arg->argv0));
  util_log_write_errid (MSGCAT_UTIL_GENERIC_INVALID_ARGUMENT);

error_exit:
  return EXIT_FAILURE;
#else /* CS_MODE */
  PRINT_AND_LOG_ERR_MSG (msgcat_message (MSGCAT_CATALOG_UTILS,
					 MSGCAT_UTIL_SET_TRANLIST,
					 TRANLIST_MSG_NOT_IN_STANDALONE),
			 basename (arg->argv0));
  return EXIT_FAILURE;
#endif /* !CS_MODE */
}

/*
 * killtran() - killtran main routine
 *   return: EXIT_SUCCESS/EXIT_FAILURE
 */
int
killtran (UTIL_FUNCTION_ARG * arg)
{
#if defined (CS_MODE)
  UTIL_ARG_MAP *arg_map = arg->arg_map;
  char er_msg_file[PATH_MAX];
  const char *database_name;
  const char *kill_tran_index;
  const char *kill_progname;
  const char *kill_user;
  const char *kill_host;
  const char *kill_sql_id;
  const char *dba_password;
  bool dump_trantab_flag;
  bool dump_client_info_flag;
  bool force = true;
  int isbatch;
  char *passbuf = NULL;
  TRANS_INFO *info = NULL;
  int error;
  bool include_query_exec_info;
  int tran_index_list[MAX_KILLTRAN_INDEX_LIST_NUM];
  int list_size = 0;
  int value;
  char delimiter = ',';
  const char *ptr;
  char *tmp;

  if (utility_get_option_string_table_size (arg_map) != 1)
    {
      goto print_killtran_usage;
    }

  database_name = utility_get_option_string_value (arg_map,
						   OPTION_STRING_TABLE, 0);
  if (database_name == NULL)
    {
      goto print_killtran_usage;
    }

  kill_tran_index =
    utility_get_option_string_value (arg_map,
				     KILLTRAN_KILL_TRANSACTION_INDEX_S, 0);
  kill_user =
    utility_get_option_string_value (arg_map, KILLTRAN_KILL_USER_NAME_S, 0);
  kill_host =
    utility_get_option_string_value (arg_map, KILLTRAN_KILL_HOST_NAME_S, 0);
  kill_progname =
    utility_get_option_string_value (arg_map, KILLTRAN_KILL_PROGRAM_NAME_S,
				     0);
  kill_sql_id =
    utility_get_option_string_value (arg_map, KILLTRAN_KILL_SQL_ID_S, 0);

  force = utility_get_option_bool_value (arg_map, KILLTRAN_FORCE_S);
  dba_password = utility_get_option_string_value (arg_map,
						  KILLTRAN_DBA_PASSWORD_S, 0);
  dump_trantab_flag =
    utility_get_option_bool_value (arg_map, KILLTRAN_DISPLAY_INFORMATION_S);
  dump_client_info_flag =
    utility_get_option_bool_value (arg_map, KILLTRAN_DISPLAY_CLIENT_INFO_S);

  include_query_exec_info =
    utility_get_option_bool_value (arg_map, KILLTRAN_DISPLAY_QUERY_INFO_S);

  if (check_database_name (database_name))
    {
      goto error_exit;
    }

  isbatch = 0;
  if (kill_tran_index != NULL && strlen (kill_tran_index) != 0)
    {
      isbatch++;
    }
  if (kill_user != NULL && strlen (kill_user) != 0)
    {
      isbatch++;
    }
  if (kill_host != NULL && strlen (kill_host) != 0)
    {
      isbatch++;
    }
  if (kill_progname != NULL && strlen (kill_progname) != 0)
    {
      isbatch++;
    }
  if (kill_sql_id != NULL && strlen (kill_sql_id) != 0)
    {
      isbatch++;
    }

  if (isbatch > 1)
    {
      PRINT_AND_LOG_ERR_MSG (msgcat_message (MSGCAT_CATALOG_UTILS,
					     MSGCAT_UTIL_SET_KILLTRAN,
					     KILLTRAN_MSG_MANY_ARGS));
      goto error_exit;
    }

  /* error message log file */
  snprintf (er_msg_file, sizeof (er_msg_file) - 1,
	    "%s_%s.err", database_name, arg->command_name);
  er_init (er_msg_file, ER_EXIT_DEFAULT);

  if (lang_init () != NO_ERROR)
    {
      goto error_exit;
    }

  db_set_client_type (BOOT_CLIENT_READ_WRITE_ADMIN_UTILITY);

  if (db_login ("DBA", dba_password) != NO_ERROR)
    {
      PRINT_AND_LOG_ERR_MSG ("%s\n", db_error_string (3));
      goto error_exit;
    }

  /* first try to restart with the password given (possibly none) */
  error = db_restart (arg->command_name, TRUE, database_name);
  if (error)
    {
      if (error == ER_AU_INVALID_PASSWORD
	  && (dba_password == NULL || strlen (dba_password) == 0))
	{
	  /*
	   * prompt for a valid password and try again, need a reusable
	   * password prompter so we can use getpass() on platforms that
	   * support it.
	   */

	  /* get password interactively if interactive mode */
	  passbuf = getpass (msgcat_message (MSGCAT_CATALOG_UTILS,
					     MSGCAT_UTIL_SET_KILLTRAN,
					     KILLTRAN_MSG_DBA_PASSWORD));
	  if (passbuf[0] == '\0')	/* to fit into db_login protocol */
	    {
	      passbuf = (char *) NULL;
	    }
	  dba_password = passbuf;
	  if (db_login ("DBA", dba_password) != NO_ERROR)
	    {
	      PRINT_AND_LOG_ERR_MSG ("%s\n", db_error_string (3));
	      goto error_exit;
	    }
	  else
	    {
	      error = db_restart (arg->command_name, TRUE, database_name);
	    }
	}

      if (error)
	{
	  PRINT_AND_LOG_ERR_MSG ("%s\n", db_error_string (3));
	  goto error_exit;
	}
    }

  /*
   * Get the current state of transaction table information. All the
   * transaction kills are going to be based on this information. The
   * transaction information may be changed back in the server if there
   * are new transactions starting and finishing. We need to do this way
   * since verification is required at this level, and we cannot freeze the
   * state of the server ()transaction table).
   */

  info = logtb_get_trans_info (include_query_exec_info || kill_sql_id);
  if (info == NULL)
    {
      util_log_write_errstr ("%s\n", db_error_string (3));
      db_shutdown ();
      goto error_exit;
    }

  if (dump_trantab_flag || dump_client_info_flag || include_query_exec_info
      || ((kill_tran_index == NULL || strlen (kill_tran_index) == 0)
	  && (kill_user == NULL || strlen (kill_user) == 0)
	  && (kill_host == NULL || strlen (kill_host) == 0)
	  && (kill_progname == NULL || strlen (kill_progname) == 0)
	  && (kill_sql_id == NULL || strlen (kill_sql_id) == 0)))
    {
      TRANDUMP_LEVEL dump_level;

      if (include_query_exec_info == true)
	{
	  dump_level = TRANDUMP_QUERY_INFO;
	}
      else
	{
	  dump_level = TRANDUMP_SUMMARY;
	}
      dump_trantb (info, dump_level);
    }
  else
    {
      if (kill_tran_index != NULL && strlen (kill_tran_index) > 0)
	{
	  int result;

	  ptr = kill_tran_index;

	  tmp = strchr (ptr, delimiter);
	  while (*ptr != '\0' && tmp)
	    {
	      if (list_size >= MAX_KILLTRAN_INDEX_LIST_NUM)
		{
		  break;
		}

	      *tmp = '\0';
	      result = parse_int (&value, ptr, 10);
	      if (result != 0 || value <= 0)
		{
		  PRINT_AND_LOG_ERR_MSG (msgcat_message (MSGCAT_CATALOG_UTILS,
							 MSGCAT_UTIL_SET_KILLTRAN,
							 KILLTRAN_MSG_INVALID_TRANINDEX),
					 ptr);

		  if (info)
		    {
		      logtb_free_trans_info (info);
		    }
		  db_shutdown ();
		  goto error_exit;
		}

	      tran_index_list[list_size++] = value;
	      ptr = tmp + 1;
	      tmp = strchr (ptr, delimiter);
	    }

	  result = parse_int (&value, ptr, 10);
	  if (result != 0 || value <= 0)
	    {
	      PRINT_AND_LOG_ERR_MSG (msgcat_message (MSGCAT_CATALOG_UTILS,
						     MSGCAT_UTIL_SET_KILLTRAN,
						     KILLTRAN_MSG_INVALID_TRANINDEX),
				     ptr);

	      if (info)
		{
		  logtb_free_trans_info (info);
		}
	      db_shutdown ();
	      goto error_exit;
	    }

	  if (list_size < MAX_KILLTRAN_INDEX_LIST_NUM)
	    {
	      tran_index_list[list_size++] = value;
	    }
	}

      /* some piece of transaction identifier was entered, try to use it */
      if (kill_transactions (info, tran_index_list,
			     list_size, kill_user, kill_host,
			     kill_progname, kill_sql_id, !force) <= 0)
	{
	  if (info)
	    {
	      logtb_free_trans_info (info);
	    }
	  db_shutdown ();
	  goto error_exit;
	}
    }

  if (info)
    {
      logtb_free_trans_info (info);
    }

  (void) db_shutdown ();
  return EXIT_SUCCESS;

print_killtran_usage:
  fprintf (stderr, msgcat_message (MSGCAT_CATALOG_UTILS,
				   MSGCAT_UTIL_SET_KILLTRAN,
				   KILLTRAN_MSG_USAGE),
	   basename (arg->argv0));
  util_log_write_errid (MSGCAT_UTIL_GENERIC_INVALID_ARGUMENT);

error_exit:
  return EXIT_FAILURE;
#else /* CS_MODE */
  PRINT_AND_LOG_ERR_MSG (msgcat_message (MSGCAT_CATALOG_UTILS,
					 MSGCAT_UTIL_SET_KILLTRAN,
					 KILLTRAN_MSG_NOT_IN_STANDALONE),
			 basename (arg->argv0));
  return EXIT_FAILURE;
#endif /* !CS_MODE */
}

/*
 * plandump() - plandump main routine
 *   return: EXIT_SUCCESS/EXIT_FAILURE
 */
int
plandump (UTIL_FUNCTION_ARG * arg)
{
#if defined (CS_MODE)
  UTIL_ARG_MAP *arg_map = arg->arg_map;
  char er_msg_file[PATH_MAX];
  const char *database_name;
  const char *output_file = NULL;
  bool drop_flag = false;
  FILE *outfp = NULL;

  database_name = utility_get_option_string_value (arg_map,
						   OPTION_STRING_TABLE, 0);
  if (database_name == NULL)
    {
      goto print_plandump_usage;
    }

  drop_flag = utility_get_option_bool_value (arg_map, PLANDUMP_DROP_S);
  output_file = utility_get_option_string_value (arg_map,
						 PLANDUMP_OUTPUT_FILE_S, 0);

  if (utility_get_option_string_table_size (arg_map) != 1)
    {
      goto print_plandump_usage;
    }

  if (output_file == NULL)
    {
      outfp = stdout;
    }
  else
    {
      outfp = fopen (output_file, "w");
      if (outfp == NULL)
	{
	  PRINT_AND_LOG_ERR_MSG (msgcat_message (MSGCAT_CATALOG_UTILS,
						 MSGCAT_UTIL_SET_PLANDUMP,
						 PLANDUMP_MSG_BAD_OUTPUT),
				 output_file);
	  goto error_exit;
	}
    }

  if (check_database_name (database_name))
    {
      goto error_exit;
    }

  /* error message log file */
  snprintf (er_msg_file, sizeof (er_msg_file) - 1,
	    "%s_%s.err", database_name, arg->command_name);
  er_init (er_msg_file, ER_EXIT_DEFAULT);

  if (lang_init () != NO_ERROR)
    {
      goto error_exit;
    }

  /* should have little copyright herald message ? */
  AU_DISABLE_PASSWORDS ();
  db_set_client_type (BOOT_CLIENT_READ_ONLY_ADMIN_UTILITY);
  db_login ("DBA", NULL);

  if (db_restart (arg->command_name, TRUE, database_name) != NO_ERROR)
    {
      PRINT_AND_LOG_ERR_MSG ("%s\n", db_error_string (3));
      goto error_exit;
    }

  qmgr_dump_query_plans (outfp);
  if (drop_flag)
    {
      if (qmgr_drop_all_query_plans () != NO_ERROR)
	{
	  PRINT_AND_LOG_ERR_MSG ("%s\n", db_error_string (3));
	  db_shutdown ();
	  goto error_exit;
	}
    }
  db_shutdown ();

  if (outfp != stdout)
    {
      fclose (outfp);
    }

  return EXIT_SUCCESS;

print_plandump_usage:
  fprintf (stderr, msgcat_message (MSGCAT_CATALOG_UTILS,
				   MSGCAT_UTIL_SET_PLANDUMP,
				   PLANDUMP_MSG_USAGE),
	   basename (arg->argv0));
  util_log_write_errid (MSGCAT_UTIL_GENERIC_INVALID_ARGUMENT);

error_exit:
  if (outfp != stdout && outfp != NULL)
    {
      fclose (outfp);
    }
  return EXIT_FAILURE;
#else /* CS_MODE */
  fprintf (stderr, msgcat_message (MSGCAT_CATALOG_UTILS,
				   MSGCAT_UTIL_SET_PLANDUMP,
				   PLANDUMP_MSG_NOT_IN_STANDALONE),
	   basename (arg->argv0));
  return EXIT_FAILURE;
#endif /* !CS_MODE */
}

/*
 * paramdump() - paramdump main routine
 *   return: EXIT_SUCCESS/EXIT_FAILURE
 */
int
paramdump (UTIL_FUNCTION_ARG * arg)
{
  UTIL_ARG_MAP *arg_map = arg->arg_map;
  char er_msg_file[PATH_MAX];
  const char *database_name;
  const char *output_file = NULL;
#if defined (CS_MODE)
  bool both_flag = false;
#endif
  FILE *outfp = NULL;

  if (utility_get_option_string_table_size (arg_map) != 1)
    {
      goto print_dumpparam_usage;
    }

  database_name = utility_get_option_string_value (arg_map,
						   OPTION_STRING_TABLE, 0);
  if (database_name == NULL)
    {
      goto print_dumpparam_usage;
    }

  output_file = utility_get_option_string_value (arg_map,
						 PARAMDUMP_OUTPUT_FILE_S, 0);
#if defined (CS_MODE)
  both_flag = utility_get_option_bool_value (arg_map, PARAMDUMP_BOTH_S);
#endif

  if (output_file == NULL)
    {
      outfp = stdout;
    }
  else
    {
      outfp = fopen (output_file, "w");
      if (outfp == NULL)
	{
	  PRINT_AND_LOG_ERR_MSG (msgcat_message (MSGCAT_CATALOG_UTILS,
						 MSGCAT_UTIL_SET_PARAMDUMP,
						 PARAMDUMP_MSG_BAD_OUTPUT),
				 output_file);
	  goto error_exit;
	}
    }

  if (check_database_name (database_name))
    {
      goto error_exit;
    }

  /* error message log file */
  snprintf (er_msg_file, sizeof (er_msg_file) - 1,
	    "%s_%s.err", database_name, arg->command_name);
  er_init (er_msg_file, ER_EXIT_DEFAULT);

  if (lang_init () != NO_ERROR)
    {
      goto error_exit;
    }

#if defined (CS_MODE)
  /* should have little copyright herald message ? */
  AU_DISABLE_PASSWORDS ();
  db_set_client_type (BOOT_CLIENT_READ_ONLY_ADMIN_UTILITY);
  db_login ("DBA", NULL);

  if (db_restart (arg->command_name, TRUE, database_name) != NO_ERROR)
    {
      PRINT_AND_LOG_ERR_MSG ("%s\n", db_error_string (3));
      goto error_exit;
    }

  if (both_flag)
    {
      fprintf (outfp, msgcat_message (MSGCAT_CATALOG_UTILS,
				      MSGCAT_UTIL_SET_PARAMDUMP,
				      PARAMDUMP_MSG_CLIENT_PARAMETER));
      sysprm_dump_parameters (outfp);
    }

  fprintf (outfp, msgcat_message (MSGCAT_CATALOG_UTILS,
				  MSGCAT_UTIL_SET_PARAMDUMP,
				  PARAMDUMP_MSG_SERVER_PARAMETER),
	   database_name);
  sysprm_dump_server_parameters (outfp);

  db_shutdown ();
#else /* CS_MODE */
  fprintf (outfp, msgcat_message (MSGCAT_CATALOG_UTILS,
				  MSGCAT_UTIL_SET_PARAMDUMP,
				  PARAMDUMP_MSG_STANDALONE_PARAMETER));
  if (sysprm_load_and_init (database_name) == NO_ERROR)
    {
      sysprm_dump_parameters (outfp);
    }
#endif /* !CS_MODE */

  fprintf (outfp, msgcat_message (MSGCAT_CATALOG_UTILS,
				  MSGCAT_UTIL_SET_PARAMDUMP,
				  PARAMDUMP_MSG_PERSIST_PARAMETER));
  (void) db_dump_persist_conf_file (outfp, NULL, NULL);
#if 0				/* dbg */
  (void) db_dump_persist_conf_file (outfp, "server", NULL);
  (void) db_dump_persist_conf_file (outfp, NULL, "@rdb");
  (void) db_dump_persist_conf_file (outfp, "broker", "common");
#endif

  if (outfp != stdout)
    {
      fclose (outfp);
    }

  return EXIT_SUCCESS;

print_dumpparam_usage:
  fprintf (stderr, msgcat_message (MSGCAT_CATALOG_UTILS,
				   MSGCAT_UTIL_SET_PARAMDUMP,
				   PARAMDUMP_MSG_USAGE),
	   basename (arg->argv0));
  util_log_write_errid (MSGCAT_UTIL_GENERIC_INVALID_ARGUMENT);

error_exit:
  if (outfp != stdout && outfp != NULL)
    {
      fclose (outfp);
    }
  return EXIT_FAILURE;
}

/*
 * statdump() - statdump main routine
 *   return: EXIT_SUCCESS/EXIT_FAILURE
 */
int
statdump (UTIL_FUNCTION_ARG * arg)
{
#if defined (CS_MODE)
  UTIL_ARG_MAP *arg_map = arg->arg_map;
  char er_msg_file[PATH_MAX];
  const char *database_name;
  const char *output_file = NULL;
  const char *output_type = NULL;
  int interval;
  bool cumulative;
  const char *substr;
  FILE *outfp = NULL;
  char local_db_name[ONE_K], name[ONE_K];
  char hostname[MAX_NODE_INFO_STR_LEN];
  char *ptr;
  bool is_header_printed;

  char monitor_name[ONE_K];

  MONITOR_DUMP_TYPE dump_type = MNT_DUMP_TYPE_NORMAL;

  MONITOR_INFO *server_monitor = NULL;
  MONITOR_STATS server_cur_stats[MNT_SIZE_OF_SERVER_EXEC_STATS];
  MONITOR_STATS server_old_stats[MNT_SIZE_OF_SERVER_EXEC_STATS];

  MONITOR_INFO *repl_monitor[PRM_MAX_HA_NODE_LIST];
  PRM_NODE_LIST node_list = PRM_NODE_LIST_INITIALIZER;
  MONITOR_STATS *repl_cur_stats[PRM_MAX_HA_NODE_LIST];
  MONITOR_STATS *repl_old_stats[PRM_MAX_HA_NODE_LIST];
  int repl_stats_size;

  int i;

  if (utility_get_option_string_table_size (arg_map) != 1)
    {
      goto print_statdump_usage;
    }

  database_name = utility_get_option_string_value (arg_map,
						   OPTION_STRING_TABLE, 0);
  if (database_name == NULL)
    {
      goto print_statdump_usage;
    }

  output_file = utility_get_option_string_value (arg_map,
						 STATDUMP_OUTPUT_FILE_S, 0);
  output_type = utility_get_option_string_value (arg_map,
						 STATDUMP_OUTPUT_TYPE_S, 0);
  interval = utility_get_option_int_value (arg_map, STATDUMP_INTERVAL_S);
  if (interval < 0)
    {
      goto print_statdump_usage;
    }
  cumulative = utility_get_option_bool_value (arg_map, STATDUMP_CUMULATIVE_S);
  substr = utility_get_option_string_value (arg_map, STATDUMP_SUBSTR_S, 0);

  if (interval == 0)
    {
      cumulative = true;
    }

  if (output_file == NULL)
    {
      outfp = stdout;
    }
  else
    {
      outfp = fopen (output_file, "w");
      if (outfp == NULL)
	{
	  PRINT_AND_LOG_ERR_MSG (msgcat_message (MSGCAT_CATALOG_UTILS,
						 MSGCAT_UTIL_SET_STATDUMP,
						 STATDUMP_MSG_BAD_OUTPUT),
				 output_file);
	  goto error_exit;
	}
    }

  if (output_type == NULL)
    {
      dump_type = MNT_DUMP_TYPE_NORMAL;
    }
  else if (strcasecmp (output_type, "csv") == 0)
    {
      dump_type = MNT_DUMP_TYPE_CSV_DATA;
    }

  if (check_database_name (database_name) != NO_ERROR)
    {
      goto error_exit;
    }

  snprintf (local_db_name, sizeof (local_db_name), "%s", database_name);
  ptr = strchr (local_db_name, '@');
  if (ptr != NULL)
    {
      if (strcmp (ptr + 1, "localhost") != 0)
	{
	  PRINT_AND_LOG_ERR_MSG ("%s\n", "Invalid host name");
	  goto error_exit;
	}
      *ptr = '\0';
    }

  /* error message log file */
  snprintf (er_msg_file, sizeof (er_msg_file) - 1,
	    "%s_%s.err", local_db_name, arg->command_name);
  er_init (er_msg_file, ER_EXIT_DEFAULT);

  if (lang_init () != NO_ERROR)
    {
      goto error_exit;
    }

  sysprm_load_and_init (NULL);


  monitor_make_name (monitor_name, local_db_name);
  server_monitor = monitor_create_viewer_from_name (monitor_name);
  if (server_monitor == NULL)
    {
      PRINT_AND_LOG_ERR_MSG ("Not found shm name(%s) of monitor\n",
			     monitor_name);

      goto error_exit;
    }
  memset (server_cur_stats, 0, sizeof (server_cur_stats));
  memset (server_old_stats, 0, sizeof (server_old_stats));

  prm_get_ha_node_list (&node_list);

  for (i = 0; i < node_list.num_nodes; i++)
    {
      repl_monitor[i] = NULL;
      repl_old_stats[i] = NULL;
      repl_cur_stats[i] = NULL;

      if (prm_is_myself_node_info (&node_list.nodes[i]))
	{
	  continue;
	}

      prm_node_info_to_str (hostname, sizeof (hostname), &node_list.nodes[i]);
      /* db_name@host_ip:port_id */
      snprintf (name, sizeof (name), "%s@%s", local_db_name, hostname);

      monitor_make_name (monitor_name, name);
      repl_monitor[i] = monitor_create_viewer_from_name (monitor_name);
      if (repl_monitor[i] == NULL)
	{
	  PRINT_AND_LOG_ERR_MSG ("Not found shm name(%s) of monitor\n",
				 monitor_name);
	  goto error_exit;
	}

      repl_stats_size =
	sizeof (MONITOR_STATS) * repl_monitor[i]->meta->num_stats;
      repl_cur_stats[i] = (MONITOR_STATS *) malloc (repl_stats_size);
      if (repl_cur_stats[i] == NULL)
	{
	  PRINT_AND_LOG_ERR_MSG ("Out of virtual memory\n");
	  goto error_exit;
	}
      memset (repl_cur_stats[i], 0, repl_stats_size);

      repl_old_stats[i] = (MONITOR_STATS *) malloc (repl_stats_size);
      if (repl_old_stats[i] == NULL)
	{
	  PRINT_AND_LOG_ERR_MSG ("Out of virtual memory\n");
	  goto error_exit;
	}
      memset (repl_old_stats[i], 0, repl_stats_size);
    }

  if (interval > 0)
    {
      is_Sigint_caught = false;
      os_set_signal_handler (SIGINT, intr_handler);
    }

  is_header_printed = false;
  do
    {
      if (dump_type == MNT_DUMP_TYPE_CSV_DATA && is_header_printed == false)
	{
	  monitor_dump_stats (outfp, server_monitor, NULL, NULL, cumulative,
			      MNT_DUMP_TYPE_CSV_HEADER, substr);

	  for (i = 0; i < node_list.num_nodes; i++)
	    {
	      if (repl_monitor[i] == NULL)
		{
		  continue;
		}
	      monitor_dump_stats (outfp, repl_monitor[i], NULL, NULL,
				  cumulative, MNT_DUMP_TYPE_CSV_HEADER,
				  substr);
	    }
	  is_header_printed = true;
	}

      /* server statdump */
      if (monitor_copy_global_stats (server_monitor,
				     server_cur_stats) != NO_ERROR)
	{
	  PRINT_AND_LOG_ERR_MSG ("%s\n", db_error_string (3));
	  continue;
	}
      mnt_calc_hit_ratio (server_cur_stats, server_monitor->meta->num_stats);
      monitor_dump_stats (outfp, server_monitor, server_cur_stats,
			  server_old_stats, cumulative, dump_type, substr);
      memcpy (server_old_stats, server_cur_stats, sizeof (server_cur_stats));
      monitor_close_viewer_data (server_monitor);

      /* repl statdump */
      for (i = 0; i < node_list.num_nodes; i++)
	{
	  if (repl_monitor[i] == NULL)
	    {
	      continue;
	    }

	  if (monitor_copy_global_stats (repl_monitor[i],
					 repl_cur_stats[i]) != NO_ERROR)
	    {
	      PRINT_AND_LOG_ERR_MSG ("%s\n", db_error_string (3));
	      continue;
	    }
	}

      for (i = 0; i < node_list.num_nodes; i++)
	{
	  if (repl_monitor[i] == NULL)
	    {
	      continue;
	    }
	  monitor_dump_stats (outfp, repl_monitor[i],
			      repl_cur_stats[i], repl_old_stats[i],
			      cumulative, dump_type, substr);
	  memcpy (repl_old_stats[i], repl_cur_stats[i],
		  sizeof (MONITOR_STATS) * MNT_SIZE_OF_REPL_EXEC_STATS);

	  monitor_close_viewer_data (repl_monitor[i]);
	}

      fprintf (outfp, "\n");
      fflush (outfp);
      sleep (interval);
    }
  while (interval > 0 && !is_Sigint_caught);

  if (local_db_name == NULL)
    {
      histo_stop ();
      db_shutdown ();
    }

  if (outfp != stdout)
    {
      fclose (outfp);
    }

  return EXIT_SUCCESS;

print_statdump_usage:
  fprintf (stderr, msgcat_message (MSGCAT_CATALOG_UTILS,
				   MSGCAT_UTIL_SET_STATDUMP,
				   STATDUMP_MSG_USAGE),
	   basename (arg->argv0));
  util_log_write_errid (MSGCAT_UTIL_GENERIC_INVALID_ARGUMENT);

error_exit:
  if (outfp != stdout && outfp != NULL)
    {
      fclose (outfp);
    }
  if (server_monitor != NULL)
    {
      monitor_final_viewer (server_monitor);
    }
  for (i = 0; i < node_list.num_nodes; i++)
    {
      if (repl_monitor[i] != NULL)
	{
	  monitor_final_viewer (repl_monitor[i]);
	}
      if (repl_old_stats[i] != NULL)
	{
	  free_and_init (repl_old_stats[i]);
	}
      if (repl_cur_stats[i] != NULL)
	{
	  free_and_init (repl_cur_stats[i]);
	}
    }
  return EXIT_FAILURE;
#else /* CS_MODE */
  PRINT_AND_LOG_ERR_MSG (msgcat_message (MSGCAT_CATALOG_UTILS,
					 MSGCAT_UTIL_SET_STATDUMP,
					 STATDUMP_MSG_NOT_IN_STANDALONE),
			 basename (arg->argv0));

  return EXIT_FAILURE;
#endif /* !CS_MODE */
}

#if defined (ENABLE_UNUSED_FUNCTION)
/* check ha_mode is turned on in the server */
static int
check_server_ha_mode (void)
{
  char prm_buf[LINE_MAX], *prm_val;

  strcpy (prm_buf, prm_get_name (PRM_ID_HA_MODE));
  if (db_get_system_parameters (prm_buf, LINE_MAX - 1) != NO_ERROR)
    {
      return ER_FAILED;
    }
  prm_val = strchr (prm_buf, '=');
  if (prm_val == NULL)
    {
      return ER_FAILED;
    }
  if (strcmp (prm_val + 1, "y") != 0)
    {
      return ER_FAILED;
    }
  return NO_ERROR;
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * sig_interrupt() -
 *   return: none
 *   sig_no(in)
 */
static void
backupdb_sig_interrupt_handler (UNUSED_ARG int sig_no)
{
  is_Sigint_caught = true;
  db_set_interrupt (1);
}

static bool
check_client_alive ()
{
  if (is_Sigint_caught == true)
    return false;

  return true;
}

static int
spacedb_get_size_str (char *buf, UINT64 num_pages,
		      T_SPACEDB_SIZE_UNIT size_unit)
{
  int pgsize, i;
  double size;

  assert (buf);

  if (size_unit == SPACEDB_SIZE_UNIT_PAGE)
    {
      sprintf (buf, "%11llu", (long long unsigned int) num_pages);
    }
  else
    {
      pgsize = IO_PAGESIZE / ONE_K;
      size = pgsize * ((double) num_pages);

      if (size_unit == SPACEDB_SIZE_UNIT_HUMAN_READABLE)
	{
	  for (i = SPACEDB_SIZE_UNIT_MBYTES;
	       i <= SPACEDB_SIZE_UNIT_TBYTES; i++)
	    {
	      size /= ONE_K;

	      if (size < ONE_K)
		{
		  break;
		}
	    }
	}
      else
	{
	  i = size_unit;
	  for (; size_unit > SPACEDB_SIZE_UNIT_PAGE; size_unit--)
	    {
	      size /= ONE_K;
	    }
	}

      sprintf (buf, "%9.1f %c", size,
	       (i == SPACEDB_SIZE_UNIT_MBYTES) ? 'M' :
	       (i == SPACEDB_SIZE_UNIT_GBYTES) ? 'G' : 'T');
    }

  return NO_ERROR;
}

#if defined (CS_MODE)
/*
 * intr_handler() - Interrupt handler for utility
 *   return: none
 *   sig_no(in)
 */
static void
intr_handler (UNUSED_ARG int sig_no)
{
  is_Sigint_caught = true;
}
#endif

#if defined (CS_MODE)
/*
 * tranlist_cmp_f() - qsort compare function used in tranlist().
 *   return:
 */
static int
tranlist_cmp_f (const void *p1, const void *p2)
{
  int ret;
  SORT_COLUMN_TYPE column_type;
  const ONE_TRAN_INFO *info1, *info2;
  const char *str_key1 = NULL, *str_key2 = NULL;
  double number_key1 = 0, number_key2 = 0;

  info1 = (const ONE_TRAN_INFO *) p1;
  info2 = (const ONE_TRAN_INFO *) p2;

  switch (tranlist_Sort_column)
    {
    case 0:
    case 1:
      number_key1 = info1->tran_index;
      number_key2 = info2->tran_index;
      column_type = SORT_COLUMN_TYPE_INT;
      break;
    case 2:
      str_key1 = info1->db_user;
      str_key2 = info2->db_user;
      column_type = SORT_COLUMN_TYPE_STR;
      break;
    case 3:
      str_key1 = info1->host_name;
      str_key2 = info2->host_name;
      column_type = SORT_COLUMN_TYPE_STR;
      break;
    case 4:
      number_key1 = info1->process_id;
      number_key2 = info2->process_id;
      column_type = SORT_COLUMN_TYPE_INT;
      break;
    case 5:
      str_key1 = info1->program_name;
      str_key2 = info2->program_name;
      column_type = SORT_COLUMN_TYPE_STR;
      break;
    case 6:
      number_key1 = info1->query_exec_info.query_time;
      number_key2 = info2->query_exec_info.query_time;
      column_type = SORT_COLUMN_TYPE_FLOAT;
      break;
    case 7:
      number_key1 = info1->query_exec_info.tran_time;
      number_key2 = info2->query_exec_info.tran_time;
      column_type = SORT_COLUMN_TYPE_FLOAT;
      break;
    case 8:
      str_key1 = info1->query_exec_info.wait_for_tran_index_string;
      str_key2 = info2->query_exec_info.wait_for_tran_index_string;
      column_type = SORT_COLUMN_TYPE_STR;
      break;
    case 9:
      str_key1 = info1->query_exec_info.sql_id;
      str_key2 = info2->query_exec_info.sql_id;
      column_type = SORT_COLUMN_TYPE_STR;
      break;
    case 10:
      str_key1 = info1->query_exec_info.query_stmt;
      str_key2 = info2->query_exec_info.query_stmt;
      column_type = SORT_COLUMN_TYPE_STR;
      break;
    default:
      assert (0);
      return 0;
    }

  switch (column_type)
    {
    case SORT_COLUMN_TYPE_INT:
    case SORT_COLUMN_TYPE_FLOAT:
      {
	if (number_key1 == number_key2)
	  {
	    ret = 0;
	  }
	else if (number_key1 > number_key2)
	  {
	    ret = 1;
	  }
	else
	  {
	    ret = -1;
	  }
      }
      break;
    case SORT_COLUMN_TYPE_STR:
      {
	if (str_key1 == NULL && str_key2 == NULL)
	  {
	    ret = 0;
	  }
	else if (str_key1 == NULL && str_key2 != NULL)
	  {
	    ret = -1;
	  }
	else if (str_key1 != NULL && str_key2 == NULL)
	  {
	    ret = 1;
	  }
	else
	  {
	    ret = strcmp (str_key1, str_key2);
	  }
      }
      break;
    default:
      assert (0);
      ret = 0;
    }

  if (tranlist_Sort_desc == true)
    {
      ret *= (-1);
    }

  return ret;
}
#endif
