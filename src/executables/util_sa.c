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
 * util_sa.c - Implementation for utilities that operate in standalone mode.
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <dlfcn.h>

#include "porting.h"
#include "chartype.h"
#include "error_manager.h"
#include "message_catalog.h"
#include "databases_file.h"
#include "environment_variable.h"
#include "system_parameter.h"
#include "boot_sr.h"
#include "db.h"
#include "authenticate.h"
#include "schema_manager.h"
#include "heap_file.h"
#include "btree.h"
#include "extendible_hash.h"
#include "locator_sr.h"
#include "log_impl.h"
#include "xserver_interface.h"
#include "utility.h"
#include "transform.h"
#include "rsql.h"
#include "locator_cl.h"
#include "network_interface_cl.h"
#include "boot_cl.h"
#include "restore.h"

#include "repl.h"

#define MAX_LINE_LEN            4096

#define COMMENT_CHAR            '-'
#define COMMAND_USER            "user"
#define COMMAND_GROUP           "group"


#define BO_DB_FULLNAME          (bo_Dbfullname)

static char bo_Dbfullname[PATH_MAX];

extern bool catcls_Enable;
extern int log_default_input_for_archive_log_location;

extern int catcls_compile_catalog_classes (THREAD_ENTRY * thread_p);

static int parse_up_to_date (const char *up_to_date, struct tm *time_date);
static int print_backup_info (const char *database_name,
			      BO_RESTART_ARG * restart_arg);

static void
make_valid_page_size (int *v)
{
  int pow_size;

  assert (*v == IO_DEFAULT_PAGE_SIZE);
  assert (*v == IO_PAGESIZE);

  if (*v < IO_MIN_PAGE_SIZE)
    {
      assert (false);
      *v = IO_MIN_PAGE_SIZE;
      return;
    }

  if (*v > IO_MAX_PAGE_SIZE)
    {
      assert (false);
      *v = IO_MAX_PAGE_SIZE;
      return;
    }

  pow_size = IO_MIN_PAGE_SIZE;
  if ((*v & (*v - 1)) != 0)
    {
      while (pow_size < *v)
	{
	  pow_size *= 2;
	}
      *v = pow_size;
    }

  assert (*v == IO_DEFAULT_PAGE_SIZE);
  assert (*v == IO_PAGESIZE);
}

/*
 * createdb() - createdb main routine
 *   return: EXIT_SUCCESS/EXIT_FAILURE
 */
int
createdb (UTIL_FUNCTION_ARG * arg)
{
  UTIL_ARG_MAP *arg_map = arg->arg_map;
  char er_msg_file[PATH_MAX];
  int status;
  FILE *output_file = NULL;
//  FILE *user_define_file = NULL;

  const char *output_file_name;
  const char *program_name;
  const char *database_name;
  const char *host_name = NULL;
  bool overwrite;
  bool verbose;
  const char *init_file_name;
  const char *volume_spec_file_name = NULL;
//  const char *user_define_file_name = NULL;

  int db_volume_pages;
  int db_page_size;
  INT64 db_volume_size;
  const INT64 db_volume_size_default = 512ULL * ONE_M;	/* 512M */
  int log_volume_pages;
  INT64 log_volume_size;
  const INT64 log_volume_size_default = 256ULL * ONE_M;	/* 256M */
  const INT64 log_volume_size_lower = 20ULL * ONE_M;	/* 20M */
  const INT64 log_volume_size_upper = 512ULL * ONE_M;	/* 512M */
  const char *db_volume_str;
  const char *log_volume_str;

  char required_size[16];

  database_name = utility_get_option_string_value (arg_map,
						   OPTION_STRING_TABLE, 0);
  if (database_name == 0 || database_name[0] == 0)
    {
      goto print_create_usage;
    }

  if (sysprm_load_and_init (database_name) != NO_ERROR)
    {
      util_log_write_errid (MSGCAT_UTIL_GENERIC_SERVICE_PROPERTY_FAIL);
      goto error_exit;
    }

  output_file_name = utility_get_option_string_value (arg_map,
						      CREATE_OUTPUT_FILE_S,
						      0);
  program_name = arg->command_name;
#if defined (ENABLE_UNUSED_FUNCTION)
  volume_path = utility_get_option_string_value (arg_map,
						 CREATE_FILE_PATH_S, 0);
  log_path = utility_get_option_string_value (arg_map, CREATE_LOG_PATH_S, 0);
#endif

  overwrite = utility_get_option_bool_value (arg_map, CREATE_REPLACE_S);
  verbose = utility_get_option_bool_value (arg_map, CREATE_VERBOSE_S);
#if defined (ENABLE_UNUSED_FUNCTION)
  comment = utility_get_option_string_value (arg_map, CREATE_COMMENT_S, 0);
#endif
  init_file_name =
    utility_get_option_string_value (arg_map,
				     CREATE_RSQL_INITIALIZATION_FILE_S, 0);
#if defined (ENABLE_UNUSED_FUNCTION)
  volume_spec_file_name =
    utility_get_option_string_value (arg_map, CREATE_MORE_VOLUME_FILE_S, 0);
  user_define_file_name =
    utility_get_option_string_value (arg_map, CREATE_USER_DEFINITION_FILE_S,
				     0);
#endif
  assert (volume_spec_file_name == NULL);
//  assert (user_define_file_name == NULL);

  db_page_size = IO_PAGESIZE;

  make_valid_page_size (&db_page_size);

  db_volume_str = utility_get_option_string_value (arg_map,
						   CREATE_DB_VOLUME_SIZE_S,
						   0);
  if (db_volume_str == NULL)
    {
      db_volume_size = db_volume_size_default;	/* 512M */
    }
  else
    {
      if (util_size_string_to_byte (&db_volume_size,
				    db_volume_str) != NO_ERROR)
	{
	  PRINT_AND_LOG_ERR_MSG (msgcat_message (MSGCAT_CATALOG_UTILS,
						 MSGCAT_UTIL_SET_CREATEDB,
						 CREATEDB_MSG_INVALID_SIZE),
				 CREATE_DB_VOLUME_SIZE_L, db_volume_str);
	  goto error_exit;
	}
    }

#if defined (ENABLE_UNUSED_FUNCTION)
  db_volume_pages = utility_get_option_int_value (arg_map, CREATE_PAGES_S);
  if (db_volume_pages != -1)
    {
      util_print_deprecated ("--" CREATE_PAGES_L);
    }
  else
#endif
    {
      db_volume_pages = db_volume_size / db_page_size;
    }

//  db_volume_size = (UINT64) db_volume_pages *(UINT64) db_page_size;

  log_volume_str = utility_get_option_string_value (arg_map,
						    CREATE_LOG_VOLUME_SIZE_S,
						    0);
  if (log_volume_str == NULL)
    {
      log_volume_size = log_volume_size_default;	/* 256M */
    }
  else
    {
      if (util_size_string_to_byte (&log_volume_size,
				    log_volume_str) != NO_ERROR)
	{
	  PRINT_AND_LOG_ERR_MSG (msgcat_message (MSGCAT_CATALOG_UTILS,
						 MSGCAT_UTIL_SET_CREATEDB,
						 CREATEDB_MSG_INVALID_SIZE),
				 CREATE_LOG_VOLUME_SIZE_L, log_volume_str);
	  goto error_exit;
	}
    }

#if defined (ENABLE_UNUSED_FUNCTION)
  log_volume_pages = utility_get_option_int_value (arg_map,
						   CREATE_LOG_PAGE_COUNT_S);
  if (log_volume_pages != -1)
    {
      util_print_deprecated ("--" CREATE_LOG_PAGE_COUNT_L);
    }
  else
#endif
    {
      log_volume_pages = log_volume_size / db_page_size;
    }

  if (check_new_database_name (database_name))
    {
      goto error_exit;
    }

  if (output_file_name == 0 || output_file_name[0] == 0)
    {
      output_file = stdout;
    }
  else
    {
      output_file = fopen (output_file_name, "w");
    }

  if (output_file == NULL)
    {
      PRINT_AND_LOG_ERR_MSG (msgcat_message (MSGCAT_CATALOG_UTILS,
					     MSGCAT_UTIL_SET_CREATEDB,
					     CREATEDB_MSG_BAD_OUTPUT),
			     output_file_name);
      goto error_exit;
    }

  if (sysprm_check_range (prm_get_name (PRM_ID_DB_VOLUME_SIZE),
			  &db_volume_size) != NO_ERROR)
    {
      INT64 min, max;
      char min_buf[64], max_buf[64], vol_buf[64];

      if (sysprm_get_range (prm_get_name (PRM_ID_DB_VOLUME_SIZE), &min, &max)
	  != NO_ERROR)
	{
	  goto error_exit;
	}
      util_byte_to_size_string (min_buf, 64, min);
      util_byte_to_size_string (max_buf, 64, max);
      if (db_volume_str != NULL)
	{
	  int len;
	  len = strlen (db_volume_str);
	  if (char_isdigit (db_volume_str[len - 1]))
	    {
	      snprintf (vol_buf, 64, "%sB", db_volume_str);
	    }
	  else
	    {
	      snprintf (vol_buf, 64, "%s", db_volume_str);
	    }
	}
      else
	{
	  util_byte_to_size_string (vol_buf, 64, db_volume_size);
	}
      fprintf (stderr, msgcat_message (MSGCAT_CATALOG_UTILS,
				       MSGCAT_UTIL_SET_CREATEDB,
				       CREATEDB_MSG_FAILURE));

      PRINT_AND_LOG_ERR_MSG (msgcat_message (MSGCAT_CATALOG_UTILS,
					     MSGCAT_UTIL_SET_CREATEDB,
					     CREATEDB_MSG_BAD_RANGE),
			     prm_get_name (PRM_ID_DB_VOLUME_SIZE), vol_buf,
			     min_buf, max_buf);
      goto error_exit;
    }

  if (!(log_volume_size_lower <= log_volume_size
	&& log_volume_size <= log_volume_size_upper))
    {
      char min_buf[64], max_buf[64], vol_buf[64];

      util_byte_to_size_string (min_buf, 64, log_volume_size_lower);
      util_byte_to_size_string (max_buf, 64, log_volume_size_upper);
      if (log_volume_str != NULL)
	{
	  int len;
	  len = strlen (log_volume_str);
	  if (char_isdigit (log_volume_str[len - 1]))
	    {
	      snprintf (vol_buf, 64, "%sB", log_volume_str);
	    }
	  else
	    {
	      snprintf (vol_buf, 64, "%s", log_volume_str);
	    }
	}
      else
	{
	  util_byte_to_size_string (vol_buf, 64, log_volume_size);
	}
      fprintf (stderr, msgcat_message (MSGCAT_CATALOG_UTILS,
				       MSGCAT_UTIL_SET_CREATEDB,
				       CREATEDB_MSG_FAILURE));
      PRINT_AND_LOG_ERR_MSG (msgcat_message (MSGCAT_CATALOG_UTILS,
					     MSGCAT_UTIL_SET_CREATEDB,
					     CREATEDB_MSG_BAD_RANGE),
			     "log_volume_size", vol_buf, min_buf, max_buf);

      goto error_exit;
    }

//  assert (user_define_file_name == NULL);

#if defined (ENABLE_UNUSED_FUNCTION)
  if (user_define_file_name != NULL)
    {
      user_define_file = fopen (user_define_file_name, "r");
      if (user_define_file == NULL)
	{
	  PRINT_AND_LOG_ERR_MSG (msgcat_message (MSGCAT_CATALOG_UTILS,
						 MSGCAT_UTIL_SET_CREATEDB,
						 CREATEDB_MSG_BAD_USERFILE),
				 user_define_file_name);
	  goto error_exit;
	}
    }
#endif

  util_byte_to_size_string (er_msg_file, sizeof (er_msg_file),
			    db_volume_size);
  /* total amount of disk space of database is
   * db volume size + log_volume_size + temp_log_volume_size */
  util_byte_to_size_string (required_size, sizeof (required_size),
			    db_volume_size + (log_volume_size * 2));
  fprintf (output_file,
	   msgcat_message (MSGCAT_CATALOG_UTILS, MSGCAT_UTIL_SET_CREATEDB,
			   CREATEDB_MSG_CREATING),
	   er_msg_file, "UTF-8", required_size);

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
  db_set_client_type (BOOT_CLIENT_CREATEDB);

  db_login ("DBA", NULL);

#if 1				/* TODO - */
  assert (host_name == NULL);
#endif
  status = db_init (program_name, true, database_name,
		    host_name, overwrite,
		    volume_spec_file_name, db_volume_pages, db_page_size,
		    log_volume_pages, db_page_size);

  if (status != NO_ERROR)
    {
      fprintf (stderr, msgcat_message (MSGCAT_CATALOG_UTILS,
				       MSGCAT_UTIL_SET_CREATEDB,
				       CREATEDB_MSG_FAILURE));
      PRINT_AND_LOG_ERR_MSG ("%s\n", db_error_string (3));
      goto error_exit;
    }

  sm_mark_system_classes ();

  (void) lang_db_put_charset ();
  if (verbose)
    {
#if 0
      au_dump_to_file (output_file);
#endif
    }
  if (!tf_Metaclass_class.n_variable)
    {
      tf_compile_meta_classes ();
    }
  if ((catcls_Enable != true)
      && (catcls_compile_catalog_classes (NULL) != NO_ERROR))
    {
      assert (false);
      util_log_write_errstr ("%s\n", db_error_string (3));
      db_shutdown ();
      goto error_exit;
    }
  if (catcls_Enable == true)
    {
      if (sm_force_write_all_classes () != NO_ERROR)
	{
	  util_log_write_errstr ("%s\n", db_error_string (3));
	  db_shutdown ();
	  goto error_exit;
	}
    }
  if (sm_update_all_catalog_statistics (true /* update_stats */ ,
					STATS_WITH_FULLSCAN) != NO_ERROR)
    {
      util_log_write_errstr ("%s\n", db_error_string (3));
      db_shutdown ();
      goto error_exit;
    }

  db_commit_transaction ();

//  assert (user_define_file == NULL);

#if defined (ENABLE_UNUSED_FUNCTION)
  if (user_define_file != NULL)
    {
      if (parse_user_define_file (user_define_file, output_file) != NO_ERROR)
	{
	  PRINT_AND_LOG_ERR_MSG (msgcat_message (MSGCAT_CATALOG_UTILS,
						 MSGCAT_UTIL_SET_CREATEDB,
						 CREATEDB_MSG_BAD_USERFILE),
				 user_define_file_name);
	  db_shutdown ();
	  goto error_exit;
	}
      fclose (user_define_file);
    }
#endif

  db_commit_transaction ();
  db_shutdown ();

  if (output_file != stdout)
    {
      fclose (output_file);
    }

  if (init_file_name != NULL)
    {
      RSQL_ARGUMENT rsql_arg;

      memset (&rsql_arg, 0, sizeof (RSQL_ARGUMENT));
      rsql_arg.auto_commit = true;
      rsql_arg.db_name = database_name;
      rsql_arg.in_file_name = init_file_name;
      rsql (arg->command_name, &rsql_arg);
    }

  return EXIT_SUCCESS;

print_create_usage:
  fprintf (stderr, msgcat_message (MSGCAT_CATALOG_UTILS,
				   MSGCAT_UTIL_SET_CREATEDB,
				   CREATEDB_MSG_USAGE),
	   basename (arg->argv0));
  util_log_write_errid (MSGCAT_UTIL_GENERIC_INVALID_ARGUMENT);

error_exit:
  if (output_file != stdout && output_file != NULL)
    {
      fclose (output_file);
    }

#if defined (ENABLE_UNUSED_FUNCTION)
  if (user_define_file != NULL)
    {
      fclose (user_define_file);
    }
#endif

  return EXIT_FAILURE;
}

/*
 * deletedb() - deletedb main routine
 *   return: EXIT_SUCCESS/EXIT_FAILURE
 */
int
deletedb (UTIL_FUNCTION_ARG * arg)
{
  UTIL_ARG_MAP *arg_map = arg->arg_map;
  char er_msg_file[PATH_MAX];
  FILE *output_file = NULL;
  const char *output_file_name;
  const char *database_name;
  bool force_delete;

  database_name = utility_get_option_string_value (arg_map,
						   OPTION_STRING_TABLE, 0);
  if (database_name == NULL)
    {
      goto print_delete_usage;
    }

  output_file_name = utility_get_option_string_value (arg_map,
						      DELETE_OUTPUT_FILE_S,
						      0);
  force_delete = utility_get_option_bool_value (arg_map,
						DELETE_DELETE_BACKUP_S);

  if (utility_get_option_string_table_size (arg_map) != 1)
    {
      goto print_delete_usage;
    }

  /* error message log file */
  snprintf (er_msg_file, sizeof (er_msg_file) - 1,
	    "%s_%s.err", database_name, arg->command_name);
  er_init (er_msg_file, ER_EXIT_DEFAULT);

  if (lang_init () != NO_ERROR)
    {
      goto error_exit;
    }

  if (check_database_name (database_name))
    {
      goto error_exit;
    }

  if (output_file_name == NULL)
    {
      output_file = stdout;
    }
  else
    {
      output_file = fopen (output_file_name, "w");
    }

  if (output_file == NULL)
    {
      PRINT_AND_LOG_ERR_MSG (msgcat_message (MSGCAT_CATALOG_UTILS,
					     MSGCAT_UTIL_SET_GENERIC,
					     MSGCAT_UTIL_GENERIC_BAD_OUTPUT_FILE),
			     output_file_name);

      goto error_exit;
    }

  /* tuning system parameters */
  sysprm_set_force (prm_get_name (PRM_ID_PAGE_BUFFER_SIZE), "50M");

  AU_DISABLE_PASSWORDS ();
  db_set_client_type (BOOT_CLIENT_READ_WRITE_ADMIN_UTILITY);
  db_login ("DBA", NULL);
  if (boot_delete (database_name, force_delete) != NO_ERROR)
    {
      PRINT_AND_LOG_ERR_MSG ("%s\n", db_error_string (3));
      goto error_exit;
    }

  er_stack_clearall ();
  er_clear ();
  if (output_file != stdout)
    {
      fclose (output_file);
    }
  return EXIT_SUCCESS;

print_delete_usage:
  fprintf (stderr, msgcat_message (MSGCAT_CATALOG_UTILS,
				   MSGCAT_UTIL_SET_DELETEDB,
				   DELETEDB_MSG_USAGE),
	   basename (arg->argv0));
  util_log_write_errid (MSGCAT_UTIL_GENERIC_INVALID_ARGUMENT);

error_exit:
  er_stack_clearall ();
  er_clear ();
  if (output_file != stdout && output_file != NULL)
    {
      fclose (output_file);
    }
  return EXIT_FAILURE;
}

static int
parse_up_to_date (const char *date_string, struct tm *time_data)
{
  int status;
  int date_index;
  char *save_ptr, *token;
  const char *delim = "-:";
  char *copy_date_string;

  copy_date_string = strdup (date_string);
  if (copy_date_string == NULL)
    {
      return ER_GENERIC_ERROR;
    }

  status = NO_ERROR;
  date_index = 0;
  token = strtok_r (copy_date_string, delim, &save_ptr);
  while (status == NO_ERROR && token != NULL)
    {
      switch (date_index)
	{
	case 0:		/* year */
	  time_data->tm_year = atoi (token) - 1900;
	  if (time_data->tm_year < 0)
	    {
	      status = ER_GENERIC_ERROR;
	    }
	  break;
	case 1:		/* month */
	  time_data->tm_mon = atoi (token) - 1;
	  if (time_data->tm_mon < 0 || time_data->tm_mon > 11)
	    {
	      status = ER_GENERIC_ERROR;
	    }
	  break;
	case 2:		/* day */
	  time_data->tm_mday = atoi (token);
	  if (time_data->tm_mday < 1 || time_data->tm_mday > 31)
	    {
	      status = ER_GENERIC_ERROR;
	    }
	  break;
	case 3:		/* hour */
	  time_data->tm_hour = atoi (token);
	  if (time_data->tm_hour < 0 || time_data->tm_hour > 23)
	    {
	      status = ER_GENERIC_ERROR;
	    }
	  break;
	case 4:		/* minute */
	  time_data->tm_min = atoi (token);
	  if (time_data->tm_min < 0 || time_data->tm_min > 59)
	    {
	      status = ER_GENERIC_ERROR;
	    }
	  break;
	case 5:		/* second */
	  time_data->tm_sec = atoi (token);
	  if (time_data->tm_sec < 0 || time_data->tm_sec > 59)
	    {
	      status = ER_GENERIC_ERROR;
	    }
	  break;
	default:
	  status = ER_GENERIC_ERROR;
	  break;
	}

      date_index++;
      token = strtok_r (NULL, delim, &save_ptr);
    }

  free (copy_date_string);

  return date_index != 6 ? ER_GENERIC_ERROR : status;
}

static int
print_backup_info (const char *database_name, BO_RESTART_ARG * restart_arg)
{
  char pathname[PATH_MAX];
  char from_volbackup[PATH_MAX];
  int error_code = NO_ERROR;

  if (envvar_db_dir (pathname, PATH_MAX, database_name) == NULL)
    {
      assert (false);
      error_code = ER_BO_UNKNOWN_DATABASE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1, database_name);
      goto exit;
    }

  COMPOSE_FULL_NAME (BO_DB_FULLNAME, sizeof (BO_DB_FULLNAME),
		     pathname, database_name);

  error_code = bk_get_backup_volume (NULL, BO_DB_FULLNAME,
				     restart_arg->backuppath, from_volbackup);
  if (error_code != NO_ERROR)
    {
      goto exit;
    }

  from_volbackup[sizeof (from_volbackup) - 1] = '\0';
  error_code = bk_list_restore (NULL, BO_DB_FULLNAME, from_volbackup);
exit:

  return error_code;
}

/*
 * restoredb() - restoredb main routine
 *   return: EXIT_SUCCESS/EXIT_FAILURE
 */
int
restoredb (UTIL_FUNCTION_ARG * arg)
{
  UTIL_ARG_MAP *arg_map = arg->arg_map;
  char er_msg_file[PATH_MAX];
  int status, error_code;
  struct tm time_data;
  const char *up_to_date;
  const char *database_name;
  bool partial_recovery;
  BO_RESTART_ARG restart_arg;

  database_name = utility_get_option_string_value (arg_map,
						   OPTION_STRING_TABLE, 0);
  up_to_date = utility_get_option_string_value (arg_map,
						RESTORE_UP_TO_DATE_S, 0);
  partial_recovery = utility_get_option_bool_value (arg_map,
						    RESTORE_PARTIAL_RECOVERY_S);
  restart_arg.printtoc = utility_get_option_bool_value (arg_map,
							RESTORE_LIST_S);
  restart_arg.stopat = -1;
  restart_arg.backuppath =
    utility_get_option_string_value (arg_map, RESTORE_BACKUP_FILE_PATH_S, 0);
  restart_arg.verbose_file =
    utility_get_option_string_value (arg_map, RESTORE_OUTPUT_FILE_S, 0);
  restart_arg.restore_upto_backuptime = false;
  restart_arg.make_slave = utility_get_option_bool_value (arg_map,
							  RESTORE_MAKE_SLAVE_S);

  if (utility_get_option_string_table_size (arg_map) != 1)
    {
      goto print_restore_usage;
    }

  if (restart_arg.backuppath == NULL)
    {
      PRINT_AND_LOG_ERR_MSG (msgcat_message (MSGCAT_CATALOG_UTILS,
					     MSGCAT_UTIL_SET_RESTOREDB,
					     RESTOREDB_MSG_NO_BACKUP_FILE));
      goto error_exit;
    }

  if (restart_arg.make_slave == true)
    {
      if (up_to_date != NULL && strlen (up_to_date) > 0)
	{
	  if (strcasecmp (up_to_date, "backuptime") == 0)
	    {
	      ;			/* nop */
	    }
	  else
	    {
	      goto print_restore_usage;
	    }
	}

      restart_arg.restore_upto_backuptime = true;
    }
  else if (up_to_date != NULL && strlen (up_to_date) > 0)
    {
      if (strcasecmp (up_to_date, "backuptime") == 0)
	{
	  restart_arg.restore_upto_backuptime = true;
	}
      else
	{
	  status = parse_up_to_date (up_to_date, &time_data);
	  restart_arg.stopat = mktime (&time_data);
	  if (status != NO_ERROR || restart_arg.stopat < 0)
	    {
	      PRINT_AND_LOG_ERR_MSG (msgcat_message (MSGCAT_CATALOG_UTILS,
						     MSGCAT_UTIL_SET_RESTOREDB,
						     RESTOREDB_MSG_BAD_DATE));
	      goto error_exit;
	    }
	}
    }
  else
    {
      restart_arg.stopat = time (NULL);
    }

  assert ((restart_arg.restore_upto_backuptime == false
	   && restart_arg.stopat > 0) || (restart_arg.stopat == -1));

  /* error message log file */
  snprintf (er_msg_file, sizeof (er_msg_file) - 1,
	    "%s_%s.err", database_name, arg->command_name);
  er_init (er_msg_file, ER_EXIT_DEFAULT);

  if (lang_init () != NO_ERROR)
    {
      goto error_exit;
    }

  if (restart_arg.printtoc)
    {
      error_code = print_backup_info (database_name, &restart_arg);
      if (error_code != NO_ERROR)
	{
	  PRINT_AND_LOG_ERR_MSG ("%s\n", db_error_string (3));
	  fprintf (stderr, msgcat_message (MSGCAT_CATALOG_UTILS,
					   MSGCAT_UTIL_SET_RESTOREDB,
					   RESTOREDB_MSG_FAILURE));
	  goto error_exit;
	}

      return EXIT_SUCCESS;
    }

  AU_DISABLE_PASSWORDS ();
  db_set_client_type (BOOT_CLIENT_READ_WRITE_ADMIN_UTILITY);
  db_login ("DBA", NULL);
  if (partial_recovery == true)
    {
      log_default_input_for_archive_log_location = 1;
    }
  status = boot_restart_from_backup (true, database_name, &restart_arg);
  if (status == NULL_TRAN_INDEX)
    {
      PRINT_AND_LOG_ERR_MSG ("%s\n", db_error_string (3));
      fprintf (stderr, msgcat_message (MSGCAT_CATALOG_UTILS,
				       MSGCAT_UTIL_SET_RESTOREDB,
				       RESTOREDB_MSG_FAILURE));
      goto error_exit;
    }

  assert (!LSA_ISNULL (&(restart_arg.backuptime_lsa)));

  if (restart_arg.make_slave == true
      && restart_arg.server_state == HA_STATE_MASTER)
    {
      CIRP_CT_LOG_ANALYZER analyzer_info;
      DB_IDXKEY key;

      memset (&analyzer_info, 0, sizeof (CIRP_CT_LOG_ANALYZER));

#if 1				/* TODO - fix me; convert host_name to host_ip */
      strncpy (analyzer_info.host_ip, restart_arg.db_host, HOST_IP_SIZE - 1);
#endif
      analyzer_info.host_ip[HOST_IP_SIZE - 1] = '\0';
      analyzer_info.current_lsa = restart_arg.backuptime_lsa;
      analyzer_info.required_lsa = restart_arg.backuptime_lsa;
      analyzer_info.creation_time = restart_arg.db_creation * 1000;

      /* make pkey idxkey */
      DB_IDXKEY_MAKE_NULL (&key);
      key.size = 1;
      db_make_string (&key.vals[0], analyzer_info.host_ip);
      error_code = qexec_upsert_analyzer_info (NULL, &key, &analyzer_info);
      if (error_code != NO_ERROR)
	{
	  db_idxkey_clear (&key);

	  goto error_exit;
	}
      db_idxkey_clear (&key);

      if (tran_server_commit () != TRAN_UNACTIVE_COMMITTED)
	{
	  error_code = er_errid ();
	  goto error_exit;
	}
    }

  boot_shutdown_server ();

  return EXIT_SUCCESS;

print_restore_usage:
  fprintf (stderr, msgcat_message (MSGCAT_CATALOG_UTILS,
				   MSGCAT_UTIL_SET_RESTOREDB,
				   RESTOREDB_MSG_USAGE),
	   basename (arg->argv0));
  util_log_write_errid (MSGCAT_UTIL_GENERIC_INVALID_ARGUMENT);

error_exit:
  return EXIT_FAILURE;
}

typedef enum
{
  DIAGDUMP_ALL = -1,
  DIAGDUMP_FILE_TABLES = 1,
  DIAGDUMP_FILE_CAPACITIES,
  DIAGDUMP_HEAP_CAPACITIES,
  DIAGDUMP_INDEX_CAPACITIES,
  DIAGDUMP_CLASSNAMES,
  DIAGDUMP_DISK_BITMAPS,
  DIAGDUMP_CATALOG,
  DIAGDUMP_LOG,
  DIAGDUMP_HEAP,
  DIAGDUMP_END_OF_OPTION
} DIAGDUMP_TYPE;

/*
 * diagdb() - diagdb main routine
 *   return: EXIT_SUCCESS/EXIT_FAILURE
 */
int
diagdb (UTIL_FUNCTION_ARG * arg)
{
  UTIL_ARG_MAP *arg_map = arg->arg_map;
  char er_msg_file[PATH_MAX];
  const char *db_name;
  DIAGDUMP_TYPE diag;
  const char *output_file = NULL;
  FILE *outfp = NULL;

  db_name = utility_get_option_string_value (arg_map, OPTION_STRING_TABLE, 0);
  if (db_name == NULL)
    {
      goto print_diag_usage;
    }

  diag = utility_get_option_int_value (arg_map, DIAG_DUMP_TYPE_S);

  if (diag != DIAGDUMP_LOG
      && utility_get_option_string_table_size (arg_map) != 1)
    {
      goto print_diag_usage;
    }

  output_file = utility_get_option_string_value (arg_map,
						 DIAG_OUTPUT_FILE_S, 0);
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
						 MSGCAT_UTIL_SET_DIAGDB,
						 DIAGDB_MSG_BAD_OUTPUT),
				 output_file);
	  goto error_exit;
	}
    }

  if (check_database_name (db_name))
    {
      goto error_exit;
    }

  /* error message log file */
  snprintf (er_msg_file, sizeof (er_msg_file) - 1,
	    "%s_%s.err", db_name, arg->command_name);
  er_init (er_msg_file, ER_EXIT_DEFAULT);

  if (lang_init () != NO_ERROR)
    {
      goto error_exit;
    }

  AU_DISABLE_PASSWORDS ();
  db_set_client_type (BOOT_CLIENT_READ_ONLY_ADMIN_UTILITY);
  db_login ("DBA", NULL);

  if (db_restart (arg->command_name, TRUE, db_name) != NO_ERROR)
    {
      PRINT_AND_LOG_ERR_MSG ("%s\n", db_error_string (3));
      goto error_exit;
    }

  if (diag < DIAGDUMP_ALL || diag >= DIAGDUMP_END_OF_OPTION)
    {
      goto print_diag_usage;
    }

  if (diag == DIAGDUMP_ALL || diag == DIAGDUMP_FILE_TABLES)
    {
      /* this dumps the allocated file stats */
      fprintf (outfp, "\n*** DUMP OF FILE STATISTICS ***\n");
      file_tracker_dump (NULL, outfp);
    }

  if (diag == DIAGDUMP_ALL || diag == DIAGDUMP_FILE_CAPACITIES)
    {
      /* this dumps the allocated file stats */
      fprintf (outfp, "\n*** DUMP OF FILE DESCRIPTIONS ***\n");
      file_dump_all_capacities (NULL, outfp);
    }

  if (diag == DIAGDUMP_ALL || diag == DIAGDUMP_HEAP_CAPACITIES)
    {
      /* this dumps lower level info about capacity of all heaps */
      fprintf (outfp, "\n*** DUMP CAPACITY OF ALL HEAPS ***\n");
      heap_dump_all_capacities (NULL, outfp);
    }

  if (diag == DIAGDUMP_ALL || diag == DIAGDUMP_INDEX_CAPACITIES)
    {
      /* this dumps lower level info about capacity of
       * all indices */
      fprintf (outfp, "\n*** DUMP CAPACITY OF ALL INDICES ***\n");
      btree_dump_capacity_all (NULL, outfp);
    }

  if (diag == DIAGDUMP_ALL || diag == DIAGDUMP_CLASSNAMES)
    {
      /* this dumps the known classnames */
      fprintf (outfp, "\n*** DUMP CLASSNAMES ***\n");
      locator_dump_class_names (NULL, outfp);
    }

  if (diag == DIAGDUMP_ALL || diag == DIAGDUMP_DISK_BITMAPS)
    {
      /* this dumps lower level info about the disk */
      fprintf (outfp, "\n*** DUMP OF DISK STATISTICS ***\n");
      disk_dump_all (NULL, outfp);
    }

  if (diag == DIAGDUMP_ALL || diag == DIAGDUMP_CATALOG)
    {
      /* this dumps the content of catalog */
      fprintf (outfp, "\n*** DUMP OF CATALOG ***\n");
      catalog_dump (NULL, outfp, 1);
    }

  if (diag == DIAGDUMP_ALL || diag == DIAGDUMP_LOG)
    {
      /* this dumps the content of log */
      LOG_PAGEID start_logpageid;
      DKNPAGES dump_npages;

      start_logpageid = utility_get_option_bigint_value (arg_map,
							 DIAG_START_LOG_PAGEID_S);
      dump_npages = utility_get_option_int_value (arg_map,
						  DIAG_NUM_LOG_PAGES_S);

      fprintf (outfp, "\n*** DUMP OF LOG ***\n");
      xlog_dump (NULL, outfp, 1, start_logpageid, dump_npages, -1);
    }

  if (diag == DIAGDUMP_ALL || diag == DIAGDUMP_HEAP)
    {
      bool dump_records;
      /* this dumps the contents of all heaps */
      dump_records =
	utility_get_option_bool_value (arg_map, DIAG_DUMP_RECORDS_S);
      fprintf (outfp, "\n*** DUMP OF ALL HEAPS ***\n");
      heap_dump_all (NULL, outfp, dump_records);
    }

  db_shutdown ();

  return EXIT_SUCCESS;

print_diag_usage:
  fprintf (stderr, msgcat_message (MSGCAT_CATALOG_UTILS,
				   MSGCAT_UTIL_SET_DIAGDB, DIAGDB_MSG_USAGE),
	   basename (arg->argv0));
  util_log_write_errid (MSGCAT_UTIL_GENERIC_INVALID_ARGUMENT);

error_exit:
  return EXIT_FAILURE;
}
