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
 * repl.c -
 */

#ident "$Id$"

#include <stdio.h>
#include <getopt.h>
#include <unistd.h>
#include <pthread.h>
#include <sys/time.h>
#include <sys/shm.h>
#include <fcntl.h>

#include "db.h"
#include "dbi.h"
#include "porting.h"
#include "transform.h"

#include "utility.h"
#include "environment_variable.h"
#include "heartbeat.h"
#include "file_io_lock.h"
#include "repl_common.h"
#include "broker_admin_pub.h"
#include "broker_config.h"
#include "broker_shm.h"
#include "connection_support.h"

#include "cas_cci_internal.h"
#include "cci_util.h"

#include "repl.h"
#include "repl_analyzer.h"
#include "repl_applier.h"
#include "repl_writer.h"


CIRP_REPL_INFO *Repl_Info = NULL;

extern CIRP_LOGWR_GLOBAL cirpwr_Gl;

static int cirp_init_repl_info (const char *database_name,
				const char *log_path, int num_applier);
static int cirp_final_repl_info (void);
static int cirp_get_cci_connection (CCI_CONN * conn, const char *db_name);

static int cirp_connect_to_master (const char *db_name, const char *log_path,
				   char **argv);

static void cirp_init_repl_arg (REPL_ARGUMENT * repl_arg);
static void cirp_free_repl_arg (REPL_ARGUMENT * repl_arg);

static int cirp_init_thread_entry (CIRP_THREAD_ENTRY * th_entry,
				   const REPL_ARGUMENT * arg,
				   CIRP_THREAD_TYPE type, int index);
static int cirp_create_thread (CIRP_THREAD_ENTRY * th_entry,
			       void *(*start_routine) (void *));
static bool check_master_alive (void);
static void *health_check_main (void *arg);


/*
 * print_usage_and_exit() -
 *
 *   return:
 */
static void
print_usage_and_exit (void)
{
  fprintf (stdout, "usage: %s [OPTIONS] db_name\n", UTIL_REPL_NAME);
  fprintf (stdout, "  options: \n");
  fprintf (stdout, "\t --%s=PATH\n", REPL_LOG_PATH_L);
}

int
main (int argc, char *argv[])
{
  char er_msg_file[PATH_MAX];
  int error = NO_ERROR;
  int num_applier = 0;
  int mem_size;
  int i;
  CIRP_THREAD_ENTRY writer_entry, flusher_entry;
  CIRP_THREAD_ENTRY analyzer_entry, health_entry;
  CIRP_THREAD_ENTRY *applier_entries = NULL;

  REPL_ARGUMENT repl_arg;
  int option_index;
  int option_key;

  GETOPT_LONG repl_option[] = {
    {REPL_LOG_PATH_L, required_argument, NULL, REPL_LOG_PATH_S},
    {0, 0, 0, 0}
  };

  argv[0] = (char *) UTIL_REPL_NAME;

  /* init client functions */
  cci_set_client_functions (or_pack_db_idxkey, db_idxkey_is_null,
			    or_db_idxkey_size, db_get_string);

  /* init */
  cirp_init_repl_arg (&repl_arg);

  while (true)
    {
      option_index = 0;
      option_key = getopt_long (argc, argv, "", repl_option, &option_index);
      if (option_key == -1)
	{
	  break;
	}

      switch (option_key)
	{
	case REPL_LOG_PATH_S:
	  RYE_FREE_MEM (repl_arg.log_path);
	  repl_arg.log_path = strdup (optarg);
	  break;
	default:
	  break;
	}
    }

  if (argc - optind == 1)
    {
      /* argv[optind] == db_name@host_name */
      repl_arg.db_name = strdup (argv[optind]);
    }

  if (repl_arg.db_name == NULL || repl_arg.log_path == NULL)
    {
      print_usage_and_exit ();

      cirp_free_repl_arg (&repl_arg);

      return EXIT_FAILURE;
    }

  /* signal processing */
  (void) os_set_signal_handler (SIGSTOP, rp_signal_handler);
  (void) os_set_signal_handler (SIGTERM, rp_signal_handler);
  (void) os_set_signal_handler (SIGPIPE, SIG_IGN);

  /* init client */
  error = msgcat_init ();
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = sysprm_load_and_init (repl_arg.db_name);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  snprintf (er_msg_file, sizeof (er_msg_file) - 1,
	    "%s_%s.err", repl_arg.db_name, argv[0]);
  error = er_init (er_msg_file, ER_EXIT_DEFAULT);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = lang_init ();
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  css_register_check_client_alive_fn (check_master_alive);

  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_NOTIFY_MESSAGE, 1,
	  "Replication Started");

  error = cirp_connect_to_master (repl_arg.db_name, repl_arg.log_path, argv);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  num_applier = prm_get_integer_value (PRM_ID_HA_MAX_LOG_APPLIER);

  error = cirp_init_repl_info (repl_arg.db_name, repl_arg.log_path,
			       num_applier);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = broker_get_local_mgmt_info (&Repl_Info->broker_key,
				      &Repl_Info->broker_port);
  if (error < 0)
    {
      REPL_SET_GENERIC_ERROR (error, " ");

      GOTO_EXIT_ON_ERROR;
    }

  error = cirpwr_initialize (repl_arg.db_name,
			     repl_arg.log_path, repl_arg.mode);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = cirp_init_thread_entry (&health_entry, &repl_arg,
				  CIRP_THREAD_HEALTH_CHEKER, -1);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  error = cirp_create_thread (&health_entry, health_check_main);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (!fileio_is_volume_exist (cirpwr_Gl.active_name))
    {
      CCI_CONN conn;

      /* connect remote host */
      error = cirp_connect_copylogdb (repl_arg.db_name, true);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      /* connect local host */
      error = cirp_get_cci_connection (&conn, repl_arg.db_name);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      error = cirpwr_create_active_log (&conn);
      if (error != NO_ERROR)
	{
	  cci_disconnect (&conn);
	  GOTO_EXIT_ON_ERROR;
	}
      cci_disconnect (&conn);
    }

  error = cirpwr_init_copy_log_info ();
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = cirp_init_thread_entry (&writer_entry, &repl_arg,
				  CIRP_THREAD_WRITER, -1);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  error = cirp_create_thread (&writer_entry, log_copier_main);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = cirp_init_thread_entry (&flusher_entry, &repl_arg,
				  CIRP_THREAD_FLUSHER, -1);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  error = cirp_create_thread (&flusher_entry, log_writer_main);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = cirp_init_thread_entry (&analyzer_entry, &repl_arg,
				  CIRP_THREAD_ANALYZER, -1);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  error = cirp_create_thread (&analyzer_entry, analyzer_main);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  mem_size = num_applier * sizeof (CIRP_THREAD_ENTRY);
  applier_entries = (CIRP_THREAD_ENTRY *) RYE_MALLOC (mem_size);
  if (applier_entries == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, mem_size);

      GOTO_EXIT_ON_ERROR;
    }

  for (i = 0; i < num_applier; i++)
    {
      error = cirp_init_thread_entry (&applier_entries[i], &repl_arg,
				      CIRP_THREAD_APPLIER, i);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      error = cirp_create_thread (&applier_entries[i], applier_main);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  Repl_Info->pid = getpid ();
  Repl_Info->start_vsize = os_get_mem_size (Repl_Info->pid, MEM_RSS);
  Repl_Info->max_mem_size = Repl_Info->start_vsize + ONE_G;

  pthread_join (analyzer_entry.tid, NULL);
  rp_set_agent_flag (REPL_AGENT_NEED_SHUTDOWN);

  error = rp_end_all_applier ();
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  for (i = 0; i < num_applier; i++)
    {
      pthread_join (applier_entries[i].tid, NULL);
    }
  pthread_join (writer_entry.tid, NULL);
  pthread_join (flusher_entry.tid, NULL);

  RYE_FREE_MEM (applier_entries);

  rp_disconnect_agents ();

  cirp_final_repl_info ();

  cirp_free_repl_arg (&repl_arg);

  cirpwr_finalize ();

  assert (error == NO_ERROR);
  return EXIT_SUCCESS;

exit_on_error:
  assert (error != NO_ERROR);

  rp_set_agent_flag (REPL_AGENT_NEED_SHUTDOWN);

  rp_disconnect_agents ();

  cirp_final_repl_info ();

  RYE_FREE_MEM (applier_entries);

  cirp_free_repl_arg (&repl_arg);

  cirpwr_finalize ();

  return EXIT_FAILURE;
}

/*
 * cirp_init_repl_arg()
 *   return:
 *
 *   repl_arg(out):
 */
static void
cirp_init_repl_arg (REPL_ARGUMENT * repl_arg)
{
  repl_arg->log_path = NULL;
  repl_arg->db_name = NULL;

  repl_arg->mode = LOGWR_MODE_SYNC;

  return;
}

/*
 * cirp_free_repl_arg()-
 *   return:
 *
 *   repl_arg(in/out):
 */
static void
cirp_free_repl_arg (REPL_ARGUMENT * repl_arg)
{
  RYE_FREE_MEM (repl_arg->log_path);
  RYE_FREE_MEM (repl_arg->db_name);

  repl_arg->mode = -1;

  return;
}

/*
 * cirp_create_thread ()-
 *   return: error code
 *
 *   th_entry(in/out):
 *   start_routine(in):
 */
static int
cirp_create_thread (CIRP_THREAD_ENTRY * th_entry,
		    void *(*start_routine) (void *))
{
  int error = NO_ERROR;

  error = pthread_mutex_lock (&th_entry->th_lock);
  if (error != 0)
    {
      error = ER_CSS_PTHREAD_MUTEX_LOCK;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      return error;
    }

  error = pthread_create (&th_entry->tid, NULL, start_routine, th_entry);
  if (error != 0)
    {
      error = ER_CSS_PTHREAD_CREATE;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      pthread_mutex_unlock (&th_entry->th_lock);
      return error;
    }

  error = pthread_mutex_unlock (&th_entry->th_lock);
  if (error != 0)
    {
      error = ER_CSS_PTHREAD_MUTEX_UNLOCK;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      return error;
    }

  return NO_ERROR;
}


/*
 * cirp_init_thread_entry () -
 *    return: error code
 *
 *    th_entry(out):
 *    arg(in):
 *    type(in)
 *    index(in):
 */
static int
cirp_init_thread_entry (CIRP_THREAD_ENTRY * th_entry,
			const REPL_ARGUMENT * arg, CIRP_THREAD_TYPE type,
			int index)
{
  int error = NO_ERROR;
  if (pthread_mutex_init (&th_entry->th_lock, NULL) < 0)
    {
      error = ER_CSS_PTHREAD_MUTEX_INIT;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

      return error;
    }

  th_entry->arg = arg;
  th_entry->th_type = type;
  th_entry->applier_index = index;

  return NO_ERROR;
}

/*
 * rp_check_appliers_status()-
 *   return: error code
 *
 *   status(in):
 */
bool
rp_check_appliers_status (CIRP_AGENT_STATUS status)
{
  CIRP_APPLIER_INFO *applier;
  int i;
  bool check_status = true;
  int error = NO_ERROR;

  for (i = 0; i < Repl_Info->num_applier && check_status == true; i++)
    {
      applier = &Repl_Info->applier_info[i];

      error = pthread_mutex_lock (&applier->lock);
      if (error != NO_ERROR)
	{
	  error = ER_CSS_PTHREAD_MUTEX_LOCK;
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

	  rp_set_agent_flag (REPL_AGENT_NEED_SHUTDOWN);

	  return error;
	}
      if (applier->status != status)
	{
	  check_status = false;
	}
      pthread_mutex_unlock (&applier->lock);
    }

  return check_status;
}

/*
 * rp_start_all_applier()-
 *   return: error code
 */
int
rp_start_all_applier (void)
{
  CIRP_APPLIER_INFO *applier;
  int i, error = NO_ERROR;

  for (i = 0; i < Repl_Info->num_applier; i++)
    {
      applier = &Repl_Info->applier_info[i];
      error = pthread_mutex_lock (&applier->lock);
      if (error != NO_ERROR)
	{
	  error = ER_CSS_PTHREAD_MUTEX_LOCK;
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

	  rp_set_agent_flag (REPL_AGENT_NEED_SHUTDOWN);

	  return error;
	}
      applier->status = CIRP_AGENT_BUSY;

      pthread_mutex_unlock (&applier->lock);
    }

  return NO_ERROR;
}

/*
 * rp_end_all_applier()-
 *   return: error code
 */
int
rp_end_all_applier (void)
{
  CIRP_APPLIER_INFO *applier;
  int i, error = NO_ERROR;

  for (i = 0; i < Repl_Info->num_applier; i++)
    {
      applier = &Repl_Info->applier_info[i];
      error = pthread_mutex_lock (&applier->lock);
      if (error != NO_ERROR)
	{
	  error = ER_CSS_PTHREAD_MUTEX_LOCK;
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

	  return error;
	}
      applier->status = CIRP_AGENT_DEAD;

      pthread_mutex_unlock (&applier->lock);
    }

  return NO_ERROR;
}

/*
 * cirp_init_repl_info()-
 *   return: error code
 *
 *   db_name(in):
 *   log_path(in):
 *   num_applier(in):
 */
static int
cirp_init_repl_info (const char *db_name, const char *log_path,
		     int num_applier)
{
  int error = NO_ERROR;
  int i;
  int size;

  if (Repl_Info != NULL)
    {
      assert (false);
      cirp_final_repl_info ();
    }

  size = (sizeof (CIRP_REPL_INFO) + num_applier * sizeof (CIRP_APPLIER_INFO));
  Repl_Info = (CIRP_REPL_INFO *) RYE_MALLOC (size);
  if (Repl_Info == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, size);

      GOTO_EXIT_ON_ERROR;
    }
  memset (Repl_Info, 0, size);

  Repl_Info->start_time = time (NULL);
  Repl_Info->max_mem_size = -1;
  Repl_Info->start_vsize = -1;
  Repl_Info->pid = -1;

  error = cirp_init_writer (&Repl_Info->writer_info);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  Repl_Info->num_applier = num_applier;

  /* init analyzer */
  error = cirp_init_analyzer (&Repl_Info->analyzer_info, db_name, log_path);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  /* init appliers */
  for (i = 0; i < num_applier; i++)
    {
      error = cirp_init_applier (&Repl_Info->applier_info[i],
				 db_name, log_path);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);
      REPL_SET_GENERIC_ERROR (error, "Invalid error code");
    }
  if (Repl_Info != NULL)
    {
      cirp_final_repl_info ();
    }

  return error;
}

/*
 * cirp_final_repl_info()-
 *    return: NO_ERROR
 */
static int
cirp_final_repl_info (void)
{
  int i;

  if (Repl_Info == NULL)
    {
      return NO_ERROR;
    }

  cirp_final_writer (&Repl_Info->writer_info);
  cirp_final_analyzer (&Repl_Info->analyzer_info);

  for (i = 0; i < Repl_Info->num_applier; i++)
    {
      cirp_final_applier (&Repl_Info->applier_info[i]);
    }

  RYE_FREE_MEM (Repl_Info->broker_key);

  RYE_FREE_MEM (Repl_Info);

  return NO_ERROR;
}

/*
 * cirp_get_cci_connection() -
 *   return: error code
 *
 *   conn(out):
 *   db_name(in):
 */
static int
cirp_get_cci_connection (CCI_CONN * conn, const char *db_name)
{
  char url[ONE_K];
  char local_db_name[ONE_K];
  char *p;
  int error = NO_ERROR;
  char err_msg[ER_MSG_SIZE];

  strncpy (local_db_name, db_name, sizeof (local_db_name));
  local_db_name[sizeof (local_db_name) - 1] = '\0';
  p = strchr (local_db_name, '@');
  if (p != NULL)
    {
      *p = '\0';
    }

  snprintf (url, sizeof (url),
	    "cci:rye://localhost:%d/%s/repl?error_on_server_restart=yes",
	    Repl_Info->broker_port, local_db_name);

  error = cci_connect (conn, url, "dba", Repl_Info->broker_key);
  if (error != 0)
    {
      error = ER_HA_REPL_AGENT_ERROR;

      snprintf (err_msg, sizeof (err_msg), "URL:%s, ERR:%s",
		url, conn->err_buf.err_msg);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, err_msg);

      GOTO_EXIT_ON_ERROR;
    }
  error = cci_set_autocommit (conn, CCI_AUTOCOMMIT_TRUE);
  if (error != 0)
    {
      error = ER_HA_REPL_AGENT_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
	      conn->err_buf.err_msg);

      GOTO_EXIT_ON_ERROR;
    }
  error = cci_set_login_timeout (conn, -1);
  if (error != 0)
    {
      error = ER_HA_REPL_AGENT_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
	      conn->err_buf.err_msg);

      GOTO_EXIT_ON_ERROR;
    }

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  assert (error != NO_ERROR);

  cci_disconnect (conn);
  return error;
}

/*
 * cirp_connect_agents()
 *   return: error code
 *
 *   db_name(in):
 */
int
cirp_connect_agents (const char *db_name)
{
  CIRP_ANALYZER_INFO *analyzer = NULL;
  CIRP_APPLIER_INFO *applier = NULL;
  int error = NO_ERROR;
  int i;

  if (Repl_Info->broker_key != NULL)
    {
      RYE_FREE_MEM (Repl_Info->broker_key);
    }
  error = broker_get_local_mgmt_info (&Repl_Info->broker_key,
				      &Repl_Info->broker_port);
  if (error < 0)
    {
      REPL_SET_GENERIC_ERROR (error, " ");

      GOTO_EXIT_ON_ERROR;
    }

  analyzer = &Repl_Info->analyzer_info;
  error = cirp_get_cci_connection (&analyzer->conn, db_name);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  for (i = 0; i < Repl_Info->num_applier
       && REPL_NEED_SHUTDOWN () == false; i++)
    {
      applier = &Repl_Info->applier_info[i];
      error = cirp_get_cci_connection (&applier->conn, db_name);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  assert (error != NO_ERROR);

  rp_disconnect_agents ();
  return error;
}

/*
 * rp_disconnect_agents()-
 *   return: NO_ERROR
 */
int
rp_disconnect_agents (void)
{
  CIRP_ANALYZER_INFO *analyzer = NULL;
  CIRP_APPLIER_INFO *applier = NULL;
  int i;

  if (Repl_Info == NULL)
    {
      return NO_ERROR;
    }

  analyzer = &Repl_Info->analyzer_info;

  cci_disconnect (&analyzer->conn);
  cirp_clear_analyzer (analyzer);

  for (i = 0; i < Repl_Info->num_applier; i++)
    {
      applier = &Repl_Info->applier_info[i];
      cci_disconnect (&applier->conn);
      cirp_clear_applier (applier);
    }

  return NO_ERROR;
}

/*
 * cirp_connect_to_master ()-
 *   return: error code
 *
 *   db_name(in):
 *   log_path(in):
 *   binary_name(in):
 *   argv(in):
 */
static int
cirp_connect_to_master (const char *db_name, const char *log_path,
			char **argv)
{
  int error = NO_ERROR;
  char executable_path[PATH_MAX];

  error = check_database_name (db_name);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

#if defined(NDEBUG)
  util_redirect_stdout_to_null ();
#endif

  /* save executable path */
  (void) envvar_bindir_file (executable_path, PATH_MAX, UTIL_REPL_NAME);

  hb_set_exec_path (executable_path);
  hb_set_argv (argv);

  /* initialize system parameters */
  error = sysprm_load_and_init (db_name);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = hb_process_init (db_name, log_path, HB_PTYPE_REPLICATION);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  assert (error != NO_ERROR);

  return error;
}

/*
 * cirp_connect_copylogdb()-
 *   return: error code
 *
 *   db_name(in):
 *   retry(in):
 */
int
cirp_connect_copylogdb (const char *db_name, bool retry)
{
  int error = NO_ERROR;

reconnect:

  AU_DISABLE_PASSWORDS ();
  db_set_client_type (BOOT_CLIENT_LOG_COPIER);
  error = db_login ("DBA", NULL);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = db_restart (UTIL_REPL_NAME, TRUE, db_name);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (db_get_connect_status () != DB_CONNECTION_STATUS_CONNECTED)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, " ");
      GOTO_EXIT_ON_ERROR;
    }

  assert (error == NO_ERROR);

  return error;

exit_on_error:
  assert (error != NO_ERROR);

  db_shutdown ();

  if (retry == true && rp_need_restart () == false)
    {
      THREAD_SLEEP (100);

      goto reconnect;
    }

  if (error == NO_ERROR
      || db_get_connect_status () == DB_CONNECTION_STATUS_CONNECTED)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "Invalid error code");
    }

  return error;
}

/*
 * health_check_main ()
 */
static void *
health_check_main (void *arg)
{
  int error = NO_ERROR;
  ER_MSG_INFO *th_er_msg_info;
  CIRP_THREAD_ENTRY *th_entry = NULL;
  char err_msg[ER_MSG_SIZE];
  int wakeup_interval = 1000;	/* 1sec */

  th_entry = (CIRP_THREAD_ENTRY *) arg;

  th_er_msg_info = malloc (sizeof (ER_MSG_INFO));
  error = er_set_msg_info (th_er_msg_info);
  if (error != NO_ERROR)
    {
      rp_set_agent_flag (REPL_AGENT_NEED_SHUTDOWN);

      free_and_init (th_er_msg_info);
      return NULL;
    }

  /* wait until thread_create finish */
  error = pthread_mutex_lock (&th_entry->th_lock);
  pthread_mutex_unlock (&th_entry->th_lock);

  assert (th_entry->th_type == CIRP_THREAD_HEALTH_CHEKER);

  while (REPL_NEED_SHUTDOWN () == false)
    {
      THREAD_SLEEP (wakeup_interval);

      if (cirp_check_mem_size () != NO_ERROR)
	{
	  rp_set_agent_flag (REPL_AGENT_NEED_SHUTDOWN);
	}
    }

  snprintf (err_msg, sizeof (err_msg), "Health Checker Exit");
  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_NOTIFY_MESSAGE, 1,
	  err_msg);

  free_and_init (th_er_msg_info);

  return NULL;
}

/*
 * cirp_check_mem_size()
 *   return: error code
 */
int
cirp_check_mem_size (void)
{
  int error = NO_ERROR;
  INT64 vsize;

  if (Repl_Info->start_vsize == -1 || Repl_Info->max_mem_size == -1)
    {
      return NO_ERROR;
    }

  vsize = os_get_mem_size (Repl_Info->pid, MEM_RSS);
  if (vsize > Repl_Info->max_mem_size)
    {
      error = ER_HA_LA_EXCEED_MAX_MEM_SIZE;
    }

  return error;
}

/*
 * cirp_get_repl_info_from_catalog() - get last applied info
 *   return: NO_ERROR or error code
 *
 * Note:
 */
int
cirp_get_repl_info_from_catalog (CIRP_ANALYZER_INFO * analyzer)
{
  int error = NO_ERROR;
  struct timeval t;
  INT64 current_time_in_msec;
  CIRP_WRITER_INFO *writer = NULL;
  LOG_HEADER *log_hdr = NULL;
  char *host_ip = NULL;

  writer = &Repl_Info->writer_info;

  error = cirp_logpb_act_log_fetch_hdr (&analyzer->buf_mgr);
  if (error != NO_ERROR)
    {
      return error;
    }
  log_hdr = analyzer->buf_mgr.act_log.log_hdr;

  host_ip = analyzer->buf_mgr.host_name;

  gettimeofday (&t, NULL);
  current_time_in_msec = timeval_to_msec (&t);

  /* get analyzer info */
  error = rpct_get_log_analyzer (&analyzer->conn, &analyzer->ct, host_ip);
  if (error != NO_ERROR)
    {
      assert (error != CCI_ER_NO_MORE_DATA);

      return error;
    }

  /* check analyzer info */
  if (LSA_ISNULL (&analyzer->ct.required_lsa))
    {
      REPL_SET_GENERIC_ERROR (error, "required_lsa in %s cannot be NULL",
			      CT_LOG_ANALYZER_NAME);
      return error;
    }

  if (log_hdr->db_creation != (analyzer->ct.creation_time / 1000))
    {
      REPL_SET_GENERIC_ERROR (error, "db creation time is different.");
      return error;
    }
  analyzer->ct.start_time = current_time_in_msec;

  /* init writer info */
  error = rpct_init_writer_info (&analyzer->conn, &writer->ct, host_ip,
				 &log_hdr->eof_lsa, current_time_in_msec);
  if (error != NO_ERROR)
    {
      return error;
    }

  /* init applier info */
  error = rpct_init_applier_info (&analyzer->conn, host_ip,
				  current_time_in_msec);
  if (error != NO_ERROR)
    {
      return error;
    }

  return error;
}

/*
 * rp_dead_agent_exists ()-
 *    return:
 */
bool
rp_dead_agent_exists (void)
{
  int i;
  CIRP_AGENT_STATUS status = CIRP_AGENT_INIT;

  status = cirp_get_analyzer_status (&Repl_Info->analyzer_info);
  if (status == CIRP_AGENT_DEAD)
    {
      return true;
    }

  status = cirpwr_get_status (&Repl_Info->writer_info);
  if (status == CIRP_AGENT_DEAD)
    {
      return true;
    }

  for (i = 0; i < Repl_Info->num_applier; i++)
    {
      status = cirp_get_applier_status (&Repl_Info->applier_info[i]);
      if (status == CIRP_AGENT_DEAD)
	{
	  return true;
	}
    }

  return false;
}

/*
 *
 */
static bool
check_master_alive (void)
{
  return (hb_Proc_shutdown == false);
}
