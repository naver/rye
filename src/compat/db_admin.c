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
 * db_admin.c - Rye Application Program Interface.
 *      Functions related to database creation and administration.
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>
#include <ctype.h>

#include "porting.h"
#include "system_parameter.h"
#include "storage_common.h"
#include "environment_variable.h"
#include "db.h"
#include "class_object.h"
#include "object_print.h"
#include "server_interface.h"
#include "boot_cl.h"
#include "locator_cl.h"
#include "schema_manager.h"
#include "schema_template.h"
#include "object_accessor.h"
#include "set_object.h"
#include "parser.h"
#include "memory_alloc.h"
#include "execute_schema.h"
#include "execute_statement.h"
#include "network_interface_cl.h"
#include "connection_support.h"
#if !defined(CS_MODE)
#include "session.h"
#endif

#include "dbval.h"		/* this must be the last header file included!!! */

void (*prev_sigfpe_handler) (int) = SIG_DFL;

/* Some like to assume that the db_ layer is able to recognize that a
 database has not been successfully restarted.  For now, check every
 time.  We'll want another functional layer for esql that doesn't
 do all this checking.
 The macros for testing this variable were moved to db.h so the query
 interface functions can use them as well. */

char db_Database_name[DB_MAX_IDENTIFIER_LENGTH + 1];
char db_Program_name[PATH_MAX];

static PRM_NODE_LIST db_Preferred_hosts = PRM_NODE_LIST_INITIALIZER;
static bool db_Connect_order_random = true;
static int db_Max_num_delayed_hosts_lookup = -1;
static int db_Delayed_hosts_count = 0;
static int db_Reconnect_reason = 0;
static bool db_Ignore_repl_delay = false;

static bool db_is_Initialized = false;

static int fetch_set_internal (DB_SET * set, int quit_on_error);
void sigfpe_handler (int sig);

/*
 * db_init() - This will create a database file and associated log files and
 *    install the authorization objects and other required system objects.
 *    This is kept only for temporary compatibility.  The creation of
 *    databases will ultimately be done only by a specially written utility
 *    function that will ensure all of the various configuration options are
 *    applied.
 *
 * return           : Error Indicator.
 * program(in)      : the program name from argv[0]
 * print_version(in): a flag enabling an initial "herald" message
 * dbname(in)	    : the name of the database (server name)
 * host_name(in)    :
 * overwrite(in)    :
 * addmore_vols_file(in):
 * npages(in)       : the initial page allocation
 * desired_pagesize(in):
 * log_npages(in):
 * desired_log_page_size(in):
 * lang_charset(in): string for language and charset (ko_KR.utf8)
 *
 */

int
db_init (const char *program, UNUSED_ARG int print_version,
	 const char *dbname, const char *host_name,
	 const bool overwrite,
	 const char *addmore_vols_file, int npages, int desired_pagesize,
	 int log_npages, int desired_log_page_size)
{
#if defined (RYE_DEBUG)
  int value;
  const char *env_value;
  char more_vol_info_temp_file[L_tmpnam];
  const char *more_vol_info_file = NULL;
#endif
  int error = NO_ERROR;
  BOOT_CLIENT_CREDENTIAL client_credential;
  BOOT_DB_PATH_INFO db_path_info;

#if 1				/* TODO - */
  assert (host_name == NULL);
#endif

  db_Connect_status = DB_CONNECTION_STATUS_CONNECTED;

#if defined (RYE_DEBUG)
  if (addmore_vols_file == NULL)
    {
      /* Added for debugging of multivols using old test programs/scripts
         What to do with volumes. */
      env_value = envvar_get ("BOSR_SPLIT_INIT_VOLUME");
      if (env_value != NULL)
	{
	  value = atoi (env_value);
	}
      else
	{
	  value = 0;
	}

      if (value != 0)
	{
	  FILE *more_vols_fp;
	  DKNPAGES db_npages;

	  db_npages = npages / 4;

	  if (tmpnam (more_vol_info_temp_file) != NULL
	      && (more_vols_fp =
		  fopen (more_vol_info_temp_file, "w")) != NULL)
	    {
	      fprintf (more_vols_fp, "%s %s %s %d", "PURPOSE", "DATA",
		       "NPAGES", db_npages);
	      fprintf (more_vols_fp, "%s %s %s %d", "PURPOSE", "INDEX",
		       "NPAGES", db_npages);
	      fprintf (more_vols_fp, "%s %s %s %d", "PURPOSE", "TEMP",
		       "NPAGES", db_npages);
	      fclose (more_vols_fp);

	      if ((db_npages * 4) != npages)
		{
		  npages = npages - (db_npages * 4);
		}
	      else
		{
		  npages = db_npages;
		}

	      addmore_vols_file = more_vol_info_file =
		more_vol_info_temp_file;
	    }
	}
    }
#endif /* RYE_DEBUG */

  if (desired_pagesize > 0)
    {
      if (desired_pagesize < IO_MIN_PAGE_SIZE)
	{
	  desired_pagesize = IO_MIN_PAGE_SIZE;
	}
      else if (desired_pagesize > IO_MAX_PAGE_SIZE)
	{
	  desired_pagesize = IO_MAX_PAGE_SIZE;
	}
    }
  else
    {
      desired_pagesize = IO_DEFAULT_PAGE_SIZE;
    }

  if (desired_log_page_size > 0)
    {
      if (desired_log_page_size < IO_MIN_PAGE_SIZE)
	{
	  desired_log_page_size = IO_MIN_PAGE_SIZE;
	}
      else if (desired_log_page_size > IO_MAX_PAGE_SIZE)
	{
	  desired_log_page_size = IO_MAX_PAGE_SIZE;
	}
    }
  else
    {
      desired_log_page_size = desired_pagesize;
    }

  client_credential.client_type = BOOT_CLIENT_READ_WRITE_ADMIN_UTILITY;
  client_credential.client_info = NULL;
  client_credential.db_name = dbname;
  client_credential.db_user = NULL;
  client_credential.db_password = NULL;
  client_credential.program_name = program;
  client_credential.login_name = NULL;
  client_credential.host_name = NULL;
  client_credential.process_id = -1;

  db_path_info.db_host = host_name;

#if 1				/* TODO - */
  assert (db_path_info.db_host == NULL);
#endif

  error = boot_initialize_client (&client_credential, &db_path_info,
				  (bool) overwrite, addmore_vols_file,
				  npages, (PGLENGTH) desired_pagesize,
				  log_npages,
				  (PGLENGTH) desired_log_page_size);

#if defined (RYE_DEBUG)
  if (more_vol_info_file != NULL)
    {
      remove (more_vol_info_file);
    }
#endif

  if (error != NO_ERROR)
    {
      db_Connect_status = DB_CONNECTION_STATUS_NOT_CONNECTED;
    }
  else
    {
      db_Connect_status = DB_CONNECTION_STATUS_CONNECTED;
      /* should be part of boot_initialize_client when we figure out what this does */
    }

  return (error);
}

/*
 * db_add_volume() - Add a volume extension to the database. The addition of
 *       the volume is a system operation that will be either aborted in case
 *       of failure or committed in case of success, independently on the
 *       destiny of the current transaction. The volume becomes immediately
 *       available to other transactions.
 *
 *    return : Error code
 *    ext_info : volume info
 *
 */
int
db_add_volume (DBDEF_VOL_EXT_INFO * ext_info)
{
  VOLID volid;
  int error = NO_ERROR;

  CHECK_CONNECT_ERROR ();

  assert (ext_info != NULL);

  if (Au_dba_user != NULL && !au_is_dba_group_member (Au_user))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_AU_DBA_ONLY, 1,
	      "db_add_volume");
      return er_errid ();
    }

  ext_info->overwrite = false;
  volid = boot_add_volume_extension (ext_info);
  if (volid == NULL_VOLID)
    {
      error = er_errid ();
    }

  return error;
}

/*
 * db_num_volumes() - Find the number of permanent volumes in the database.
 *
 * return : the number of permanent volumes in database.
 */
int
db_num_volumes (void)
{
  int retval;

  CHECK_CONNECT_ZERO ();

  retval = boot_find_number_permanent_volumes ();

  return ((int) retval);
}

/*
 * db_num_bestspace_entries() -
 *
 * return :
 */
int
db_num_bestspace_entries (void)
{
  int retval;

  CHECK_CONNECT_ZERO ();

  retval = boot_find_number_bestspace_entries ();

  return ((int) retval);
}

/*
 * db_vol_label() - Find the name of the volume associated with given volid.
 *   return : vol_fullname or NULL in case of failure.
 *   volid(in)       : Permanent volume identifier.
 *                     If NULL_VOLID is given, the total information of all
 *                     volumes is requested.
 *   vol_fullname(out): Address where the name of the volume is placed. The size
 *                     must be at least PATH_MAX (SET AS A SIDE EFFECT)
 */
char *
db_vol_label (int volid, char *vol_fullname)
{
  char *retval;

  CHECK_CONNECT_ZERO_TYPE (char *);

  retval = disk_get_fullname (db_Database_name, (VOLID) volid, vol_fullname);

  return (retval);
}

/*
 * db_get_database_name() - Returns a C string containing the name of
 *    the active database.
 * return : name of the currently active database.
 *
 * note : The string is copied and must be freed by the db_string_free()
 *        function when it is no longer required.
 */
char *
db_get_database_name (void)
{
  char *name = NULL;

  CHECK_CONNECT_NULL ();

  if (strlen (db_Database_name))
    {
      name = ws_copy_string ((const char *) db_Database_name);
    }

  return (name);
}

/*
 * db_get_database_version() - Returns a C string containing the version of
 *    the active database server.
 * return : release version of the currently active server.
 *
 * note : The string is allocated and must be freed with the db_string_free()
 *        function when it is no longer required.
 */
char *
db_get_database_version (void)
{
  char *name = NULL;
  name = ws_copy_string (rel_version_string ());
  return name;
}

#if !defined(SERVER_MODE)
int
db_get_client_type (void)
{
  return db_Client_type;
}
#endif

void
db_set_client_type (int client_type)
{
  if (client_type > BOOT_CLIENT_TYPE_MAX || client_type < BOOT_CLIENT_DEFAULT)
    {
      db_Client_type = BOOT_CLIENT_DEFAULT;
    }
  else
    {
      db_Client_type = client_type;
    }
}

void
db_set_preferred_hosts (const PRM_NODE_LIST * node_list)
{
  if (node_list)
    {
      db_Preferred_hosts = *node_list;
    }
}

void
db_set_connect_order_random (bool connect_order_random)
{
  db_Connect_order_random = connect_order_random;
}

void
db_set_max_num_delayed_hosts_lookup (int max_num_delayed_hosts_lookup)
{
  db_Max_num_delayed_hosts_lookup = max_num_delayed_hosts_lookup;
}

int
db_get_max_num_delayed_hosts_lookup (void)
{
  return db_Max_num_delayed_hosts_lookup;
}

void
db_set_reconnect_reason (int reason)
{
  db_Reconnect_reason |= reason;

  if (reason & DB_RC_HA_REPL_DELAY)
    {
      db_Delayed_hosts_count++;
    }
}

void
db_unset_reconnect_reason (int reason)
{
  db_Reconnect_reason &= ~reason;
}

int
db_get_delayed_hosts_count (void)
{
  return db_Delayed_hosts_count;
}

void
db_clear_delayed_hosts_count (void)
{
  db_Delayed_hosts_count = 0;
}

void
db_clear_reconnect_reason (void)
{
  db_Reconnect_reason = 0;
  db_Delayed_hosts_count = 0;
}

bool
db_get_need_reconnect (void)
{
  return (db_Reconnect_reason != 0);
}

void
db_set_ignore_repl_delay (void)
{
  db_Ignore_repl_delay = true;
}

void
db_clear_ignore_repl_delay (void)
{
  db_Ignore_repl_delay = false;
}

bool
db_get_ignore_repl_delay (void)
{
  return db_Ignore_repl_delay;
}

/*
 * db_initialize ()-
 *    retrun: error code
 */
int
db_initialize (void)
{
  int error = NO_ERROR;

  if (db_is_Initialized == true)
    {
      return NO_ERROR;
    }

  error = sysprm_load_and_init (NULL);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  error = msgcat_init ();
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  error = er_init (NULL, ER_EXIT_DEFAULT);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  error = lang_init ();
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  db_is_Initialized = true;

  return NO_ERROR;

exit_on_error:
  tp_final ();
  sysprm_final ();
  msgcat_final ();
  lang_final ();

  return error;
}

/*
 * db_finalize ()
 *   return: NO_ERROR
 *
 */
int
db_finalize (void)
{
  tp_final ();

  sysprm_final ();

  msgcat_final ();

  lang_final ();
#if defined (ENABLE_UNUSED_FUNCTION)
  /* adj_arrays & lex buffers in the cnv formatting library. */
  cnv_cleanup ();
#endif

  db_is_Initialized = false;

  return NO_ERROR;
}

/*
 * DATABASE ACCESS
 */

/*
 * db_login() -
 * return      : Error code.
 * name(in)    : user name
 * password(in): optional password
 *
 */
int
db_login (const char *name, const char *password)
{
  int retval;

  retval = au_login (name, password, false);

  return (retval);
}

/*
 * sigfpe_handler() - The function is registered with the system to handle
 *       the SIGFPE signal. It will call the user function if one was set when
 *       the database was started.
 * return : void
 * sig    : signal number.
 */
void
sigfpe_handler (int sig)
{
  void (*prev_sig) (int);
  /* If the user had a SIGFPE handler, call it */
  if ((prev_sigfpe_handler != SIG_IGN) &&
#if defined(SIG_ERR)
      (prev_sigfpe_handler != SIG_ERR) &&
#endif
#if defined(SIG_HOLD)
      (prev_sigfpe_handler != SIG_HOLD) &&
#endif
      (prev_sigfpe_handler != SIG_DFL))
    {
      (*prev_sigfpe_handler) (sig);
    }
  /* If using reliable signals, the previous handler is this routine
   * because it's been reestablished.  In that case, don't change
   * the value of the user's handler.
   */
  prev_sig = os_set_signal_handler (SIGFPE, sigfpe_handler);
  if (prev_sig != sigfpe_handler)
    {
      prev_sigfpe_handler = prev_sig;
    }
}

/*
 * db_clear_host_connected() -
 */
void
db_clear_host_connected (void)
{
  boot_clear_host_connected ();
}

/*
 * db_restart() - This is the primary interface function for opening a
 *    database. The database must have already been created using the
 *    system defined generator tool.
 *
 * return           : error code.
 * program(in)      : the program name from argv[0]
 * print_version(in): flag to enable printing of an initial herald message
 * volume(in)       : the name of the database (server)
 *
 */
int
db_restart (const char *program, UNUSED_ARG int print_version,
	    const char *volume)
{
  int error = NO_ERROR;
  BOOT_CLIENT_CREDENTIAL client_credential;

  assert (lang_check_initialized () == true);

  if (program == NULL || volume == NULL)
    {
      error = ER_OBJ_INVALID_ARGUMENTS;
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 0);

      return error;
    }

  STRNCPY (db_Program_name, program, PATH_MAX);
  db_Database_name[0] = '\0';

  /* authorization will need to access the database and call some db_
     functions so assume connection will be ok until after boot_restart_client
     returns */
  db_Connect_status = DB_CONNECTION_STATUS_CONNECTED;

  client_credential.client_type = (BOOT_CLIENT_TYPE) db_Client_type;
  client_credential.client_info = NULL;
  client_credential.db_name = volume;
  client_credential.db_user = NULL;
  client_credential.db_password = NULL;
  client_credential.program_name = program;
  client_credential.login_name = NULL;
  client_credential.host_name = NULL;
  client_credential.process_id = -1;
  client_credential.preferred_nodes = db_Preferred_hosts;
  client_credential.connect_order_random = db_Connect_order_random;

  error = boot_restart_client (&client_credential);
  if (error != NO_ERROR)
    {
      db_Connect_status = DB_CONNECTION_STATUS_NOT_CONNECTED;
    }
  else
    {
      db_Connect_status = DB_CONNECTION_STATUS_CONNECTED;
      strncpy (db_Database_name, volume, DB_MAX_IDENTIFIER_LENGTH);
      prev_sigfpe_handler = os_set_signal_handler (SIGFPE, sigfpe_handler);
    }

  return error;
}

/*
 * db_restart_ex() - extended db_restart()
 *
 * returns : error code.
 *
 *   program(in) : the program name from argv[0]
 *   db_name(in) : the name of the database (server)
 *   db_user(in) : the database user name
 *   db_password(in) : the password
 *   client_type(in) : BOOT_CLIENT_TYPE_XXX in boot.h
 */
int
db_restart_ex (const char *program, const char *db_name, const char *db_user,
	       const char *db_password, int client_type)
{
  int retval;

  retval = au_login (db_user, db_password, false);
  if (retval != NO_ERROR)
    {
      return retval;
    }

  db_set_client_type (client_type);
#if !defined(CS_MODE)
  /* if we're in SERVER_MODE, this is the only place where we can initialize
     the sessions state module */
  switch (client_type)
    {
    case BOOT_CLIENT_ADMIN_RSQL:
    case BOOT_CLIENT_RSQL:
      session_states_init (NULL);
    default:
      break;
    }
#endif

  return db_restart (program, false, db_name);
}

/*
 * db_shutdown() - This closes a database that was previously restarted.
 * return : error code.
 *
 * note: This will ABORT the current transaction.
 */
int
db_shutdown (void)
{
  int error = NO_ERROR;

  error = boot_shutdown_client ();

  db_Database_name[0] = '\0';
  db_Connect_status = DB_CONNECTION_STATUS_NOT_CONNECTED;
  db_Program_name[0] = '\0';
  (void) os_set_signal_handler (SIGFPE, prev_sigfpe_handler);

  db_enable_modification ();

  db_free_execution_plan ();

  return error;
}

int
db_ping_server (UNUSED_ARG int client_val, UNUSED_ARG int *server_val)
{
  int error = NO_ERROR;

  CHECK_CONNECT_ERROR ();
#if defined (CS_MODE)
  error = net_client_ping_server (client_val, server_val, 5000);
#endif /* CS_MODE */
  return error;
}

/*
 * db_end_session - end current session
 *   return: error code
 *
 * NOTE: This function ends the session identified by 'db_Session_id'
 */
int
db_end_session (void)
{
  int retval;

  CHECK_CONNECT_ERROR ();

  retval = csession_end_session (db_get_session_id ());

  return (retval);
}

/*
 *  TRANSACTION MANAGEMENT
 */

/*
 * db_commit_transaction() - Commits the current transaction.
 *    You must call this function if you want changes to be made permanent.
 * return : error code.
 *
 * note : If you call db_shutdown without calling this function,
 *    the transaction will be aborted and the changes lost.
 */
int
db_commit_transaction (void)
{
  int retval;

  CHECK_CONNECT_ERROR ();

  /* API does not support RETAIN LOCK */
  retval = tran_commit ();

  return (retval);
}

/*
 * db_abort_transaction() - Abort the current transaction.
 *    This will throw away all changes that have been made since the last call
 *    to db_commit_transaction.
 *    Currently this will invoke a garbage collection because its a
 *    convenient place to test this and is probably what we want anyway.
 *
 * return : error code.
 */
int
db_abort_transaction (void)
{
  int error;

  CHECK_CONNECT_ERROR ();

  error = tran_abort ();

  return (error);
}

/*
 * db_commit_is_needed() - This function can be used to test to see if there
 *    are any dirty objects in the workspace that have not been flushed OR
 *    if there are any objects on the server that have been flushed but have
 *    not been committed during the current transaction.  This could be used
 *    to display a warning message or prompt window in interface utilities
 *    that gives the user a last chance to commit a transaction before
 *    exiting the process.
 *
 * return : non-zero if there objects need to be committed
 *
 */
int
db_commit_is_needed (void)
{
  int retval;

  CHECK_CONNECT_FALSE ();

  retval = (tran_has_updated ())? 1 : 0;

  return (retval);
}

int
db_savepoint_transaction_internal (const char *savepoint_name)
{
  int retval;

  retval = tran_savepoint_internal (savepoint_name, USER_SAVEPOINT);

  return (retval);
}

/*
 * db_savepoint_transaction() - see the note below.
 *
 * returns/side-effects: error code.
 * savepoint_name(in)  : Name of the savepoint
 *
 * note: A savepoint is established for the current transaction, so
 *       that future transaction operations can be rolled back to this
 *       established savepoint. This operation is called a partial
 *       abort (rollback). That is, all database actions affected by
 *       the transaction after the savepoint are "undone", and all
 *       effects of the transaction preceding the savepoint remain. The
 *       transaction can then continue executing other database
 *       statements. It is permissible to abort to the same savepoint
 *       repeatedly within the same transaction.
 *       If the same savepoint name is used in multiple savepoint
 *       declarations within the same transaction, then only the latest
 *       savepoint with that name is available for aborts and the
 *       others are forgotten.
 *       There is no limit on the number of savepoints that a
 *       transaction can have.
 */
int
db_savepoint_transaction (const char *savepoint_name)
{
  int retval;

  CHECK_CONNECT_ERROR ();

  retval = db_savepoint_transaction_internal (savepoint_name);

  return (retval);
}

int
db_abort_to_savepoint_internal (const char *savepoint_name)
{
  int error;

  if (savepoint_name == NULL)
    {
      return db_abort_transaction ();
    }

  error = tran_abort_upto_user_savepoint (savepoint_name);

  return (error);
}

/*
 * db_abort_to_savepoint() - All the effects of the current transaction
 *     after the given savepoint are undone and all effects of the transaction
 *     preceding the given savepoint remain. After the partial abort the
 *     transaction can continue its normal execution as if the
 *     statements that were undone were never executed.
 *
 * return            : error code
 * savepoint_name(in): Name of the savepoint or NULL
 *
 * note: If savepoint_name is NULL, the transaction is aborted.
 */
int
db_abort_to_savepoint (const char *savepoint_name)
{
  int error;

  CHECK_CONNECT_ERROR ();

  error = db_abort_to_savepoint_internal (savepoint_name);

  return (error);
}

/*
 * db_set_interrupt: Set or clear a database interruption flags.
 * return : void
 * set(in): Set or clear an interruption
 *
 */
void
db_set_interrupt (int set)
{
  CHECK_CONNECT_VOID ();
  locator_set_sig_interrupt (set);
}

/*
 * db_set_suppress_repl_on_transaction : Suppress writing replication logs during
 *                                       setting the flag on the transaction
 *
 * return : void
 * set(in): Set or clear the flag
 *
 */
int
db_set_suppress_repl_on_transaction (int set)
{
  CHECK_CONNECT_ERROR ();
  return log_set_suppress_repl_on_transaction (set);
}

/*
 * db_checkpoint: Set or clear a database interruption flags.
 * return : void
 * set(in): Set or clear an interruption
 *
 */
void
db_checkpoint (void)
{
  CHECK_CONNECT_VOID ();
  log_checkpoint ();
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_set_lock_timeout() - This sets a timeout on the amount of time to spend
 *    waiting to aquire a lock on an object.  Normally the system will wait
 *    forever for a lock to be granted.  If you enable lock timeouts, you must
 *    be prepared to handle lock failure errors at the return of any function
 *    that performs an operation on a DB_OBJECT. A timeout value of zero
 *    indicates that there is no timeout and the system will return immediately
 *    if the lock cannot be granted.  A positive integer indicates the maximum
 *    number of seconds to wait. A value of -1 indicates an infinite timeout
 *    where the system will wait forever to aquire the lock.  Infinite timeout
 *    is the default behavior.
 *
 * return      : the old timeout value.
 * seconds(in) : the new timeout value
 *
 */
int
db_set_lock_timeout (int seconds)
{
  int retval;

  CHECK_CONNECT_MINUSONE ();

  if (seconds > 0)
    {
      retval = tran_reset_wait_times (seconds * 1000);
    }
  else
    {
      retval = tran_reset_wait_times (seconds);
    }


  return (retval);
}

/*
 * db_set_isolation() - Set the isolation level for present and future client
 *     transactions to the given isolation level. It is recommended to set the
 *     isolation level at the beginning of the client transaction. If the
 *     isolation level is set in the middle of the client transaction, some
 *     resources/locks acquired by the current transactions may be released at
 *     this point according to the new isolation level. We say that the
 *     transaction will see the given isolation level from this point on.
 *     However, we should not call the transaction as one of that isolation
 *     level.
 *     For example, if a transaction with TRAN_COMMIT_CLASS_UNCOMMIT_INSTANCE
 *     is change to TRAN_REP_CLASS_REP_INSTANCE, we cannot say that the
 *     transaction has run with the last level of isolation...just that a part
 *     of the transaction was run with that.
 *
 * return        : error code.
 * isolation(in) : new Isolation level.
 */
int
db_set_isolation (DB_TRAN_ISOLATION isolation)
{
  int retval;

  CHECK_CONNECT_MINUSONE ();

  retval = tran_reset_isolation (isolation);

  return retval;
}
#endif

/*
 * db_get_tran_settings() - Retrieve transaction settings.
 * return : none
 * lock_wait(out)      : Transaction lock wait assigned to client transaction
 *                       (Set as a side effect)
 * tran_isolation(out) : Transaction isolation assigned to client transactions
 *                       (Set as a side effect)
 */
void
db_get_tran_settings (int *lock_wait, DB_TRAN_ISOLATION * tran_isolation)
{
  int lock_timeout_in_msecs = -1;

  CHECK_CONNECT_VOID ();
  /* API does not support ASYNC WORKSPACE */
  tran_get_tran_settings (&lock_timeout_in_msecs, tran_isolation);
  if (lock_timeout_in_msecs > 0)
    {
      *lock_wait = lock_timeout_in_msecs / 1000;
    }
  else
    {
      *lock_wait = lock_timeout_in_msecs;
    }
}

/*
 *  AUTHORIZATION
 */

/*
 * db_find_user() - Returns the database object for a named user if that user
 *                  has been defined to the authorization system.
 * return  : user object
 * name(in): user name
 *
 */

DB_OBJECT *
db_find_user (const char *name)
{
  DB_OBJECT *retval;

  CHECK_CONNECT_NULL ();
  CHECK_1ARG_NULL (name);

  retval = au_find_user (name);
  return (retval);
}

/*
 * db_add_user() - This will add a new user to the database.  Only the DBA can
 *       add users. If the user already exists, its object pointer will be
 *       returned and the exists flag will be set to non-zero. The exists
 *       pointer can be NULL if the caller isn't interested in this value.
 *
 * return     : new user object
 * name(in)   : user name
 * exists(out): pointer to flag, set if user already exists
 *
 */
DB_OBJECT *
db_add_user (const char *name, int *exists)
{
  DB_OBJECT *retval;

  CHECK_CONNECT_NULL ();
  CHECK_1ARG_NULL (name);
  CHECK_MODIFICATION_NULL ();

  retval = au_add_user (name, exists);

  return (retval);
}

/*
 * db_drop_user() - This will remove a user from the database.  Only the DBA
 *    can remove user objects.  You should call this rather than db_drop so
 *    that the internal system tables are updated correctly.
 * return  : error code.
 * user(in): user object pointer
 *
 */
int
db_drop_user (DB_OBJECT * user)
{
  int retval;

  CHECK_CONNECT_ERROR ();
  CHECK_1ARG_ERROR (user);
  CHECK_MODIFICATION_ERROR ();

  retval = au_drop_user (user);

  return (retval);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_drop_member() - removes a member from a user/group.  The removed member
 *    loses all privilidges that were inherted directely or indirectely from
 *    the group.
 *
 * return : error code
 * user(in/out)  : user/group that needs member removed
 * member(in/out): member to remove
 *
 */

int
db_drop_member (DB_OBJECT * user, DB_OBJECT * member)
{
  int retval;

  CHECK_CONNECT_ERROR ();
  CHECK_2ARGS_ERROR (user, member);
  CHECK_MODIFICATION_ERROR ();

  retval = au_drop_member (user, member);

  return (retval);
}
#endif

/*
 * db_grant() -This is the basic mechanism for passing permissions to other
 *    users.  The authorization type is one of the numeric values defined
 *    by the DB_AUTH enumeration.  If more than one authorization is to
 *    be granted, the values in DB_AUTH can be combined using the C bitwise
 *    "or" operator |.  Errors are likely if the currently logged in user
 *    was not the owner of the class and was not given the grant_option for
 *    the desired authorization types.
 * return  : error code
 * user(in)         : a user object
 * class(in)        : a class object
 * auth(in)         : an authorization type
 * grant_option(in) : true if the grant option is to be added
 *
 */
int
db_grant (MOP user, MOP class_, AU_TYPE auth, int grant_option)
{
  int retval;

  CHECK_CONNECT_ERROR ();
  CHECK_2ARGS_ERROR (user, class_);
  CHECK_MODIFICATION_ERROR ();

  retval = au_grant (user, class_, auth, (bool) grant_option);

  return retval;
}

/*
 * db_revoke() - This is the basic mechanism for revoking previously granted
 *    authorizations.  A prior authorization must have been made.
 * returns  : error code
 * user(in) : a user object
 * class_mop(in): a class object
 * auth(in) : the authorization type(s) to revoke
 *
 */
int
db_revoke (MOP user, MOP class_mop, AU_TYPE auth)
{
  int retval;

  CHECK_CONNECT_ERROR ();
  CHECK_2ARGS_ERROR (user, class_mop);
  CHECK_MODIFICATION_ERROR ();

  retval = au_revoke (user, class_mop, auth);

  return retval;
}

/*
 * db_check_authorization() - This will check to see if a particular
 *    authorization is available for a class.  An error will be returned
 *    if the authorization was not granted.
 * return  : error status
 * op(in)  : class or instance object
 * auth(in): authorization type
 *
 */
int
db_check_authorization (MOP op, DB_AUTH auth)
{
  SM_CLASS *class_;
  int retval;

  CHECK_CONNECT_ERROR ();
  CHECK_1ARG_ERROR (op);

  /* should try to get a write lock on the class if the authorization
     type is AU_ALTER ? */

  retval = (au_fetch_class (op, &class_, S_LOCK, auth));

  return (retval);
}

/*
 * db_get_owner() - returns the user object that owns the class.
 * return   : owner object
 * class(in): class object
 *
 */
DB_OBJECT *
db_get_owner (DB_OBJECT * class_obj)
{
  DB_OBJECT *retval;

  CHECK_CONNECT_NULL ();
  CHECK_1ARG_NULL (class_obj);

  retval = au_get_class_owner (class_obj);
  return (retval);
}

/*
 * db_get_user_name() - This returns the name of the user that is currently
 *    logged in. It simply makes a copy of the authorization name buffer.
 *    The returned string must later be freed with db_string_free.
 *
 * return : name of current user
 */
char *
db_get_user_name (void)
{
  char *name;

  CHECK_CONNECT_NULL ();

  /* Kludge, twiddle the constness of this thing.  It probably
     doesn't need to be const anyway, its just a copy of the
     attribute value. */
  name = au_user_name ();

  return (name);
}

/*
 * db_get_user_and_host_name() - This returns the name of the user that is
 *    currently logged in and the host name. Format for return value is
 *    user_name@host_name
 *
 * return : user and host name
 */
char *
db_get_user_and_host_name (void)
{
  char *user = NULL;
  char *username = NULL;
  char hostname[MAXHOSTNAMELEN];
  int len;

  if (GETHOSTNAME (hostname, MAXHOSTNAMELEN) != 0)
    {
      return NULL;
    }

  username = db_get_user_name ();
  if (!username)
    {
      return NULL;
    }

  len = strlen (hostname) + strlen (username) + 2;
  user = (char *) malloc (len);
  if (!user)
    {
      db_string_free (username);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
	      1, len);
      return 0;
    }

  strcpy (user, username);
  strcat (user, "@");
  strcat (user, hostname);
  db_string_free (username);

  return user;
}

/*
 * db_get_user() - This returns the user object of the current user. If no user
 *    has been logged in, it returns NULL. No error is set if NULL is returned,
 *    it simply means that there is no active user.
 * return : name of current user
 */
DB_OBJECT *
db_get_user (void)
{
  return Au_user;
}

/*
 * ERROR INTERFACE
 */

#if defined (ENABLE_UNUSED_FUNCTION)
const char *
db_error_string_test (int level)
{
  const char *retval;

  retval = db_error_string (level);
  return (retval);
}
#endif

/*
 * db_error_string() - This is used to get a string describing the LAST error
 *    that was detected in the database system.
 *    Whenever a db_ function returns an negative error code or when they
 *    return results such as NULL pointers that indicate error, the error will
 *    have been stored in a global structure that can be examined to find out
 *    more about the error.
 *    The string returned will be overwritten when the next error is detected
 *    so it may be necessary to copy it to a private area if you need to keep
 *    it for some lenght of time.  The level parameter controls the amount of
 *    information to be included in the message, level 1 is for short messages,
 *    level 3 is for longer error messages.  Not all error conditions
 *    (few in fact) have level 3 descriptions, all have level 1 descriptions.
 *    If you ask for level 3 and there is no description present, you will be
 *    returned the description at the next highest level.
 * return    : string containing description of the error
 * level(in) : level of description
 */
const char *
db_error_string (UNUSED_ARG int level)
{
  /* this can be called when the database is not started */
  return er_msg ();
}

#if defined (ENABLE_UNUSED_FUNCTION)
int
db_error_code_test (void)
{
  int retval;

  retval = db_error_code ();
  return (retval);
}
#endif

/*
 * db_error_code() - This is used to get an integer code identifying the LAST
 *    error that was detected by the database.  See the description under
 *    db_error_string for more information on how error descriptions are
 *    maintained.  Normally, an application would use db_error_string
 *    to display error messages to the user.  It may be useful in some
 *    cases to let an application examine the error code and conditionalize
 *    execution to handle a particular event.  In these cases, this function
 *    can be used to get the error code.
 * return : a error code constant
 */
int
db_error_code (void)
{
  int retval;

  /* can be called when the database is not started */
  retval = ((int) er_errid ());
  return (retval);
}

/*
 *
 * db_register_error_loghandler () - This function registers user supplied
 * error log handler function (db_error_log_handler_t type) which is called
 * whenever DBMS error message is to be logged.
 *
 * return : previously registered error log handler function (may be NULL)
 * f (in) : user supplied error log handler function
 *
 */

db_error_log_handler_t
db_register_error_log_handler (db_error_log_handler_t f)
{
  return (db_error_log_handler_t) er_register_log_handler ((er_log_handler_t)
							   f);
}

/*
 *  CLUSTER FETCH FUNCTIONS
 */

/*
 * fetch_set_internal() -
 *    This is used to fetch all of the objects contained in a set
 *    in a single call to the server.  It is a convenience function that
 *    behaves similar to db_fetch_array().
 *    If quit_on_error is zero, an attempt will be made to fetch all of the
 *    objects in the array.  If one of the objects could not be fetched with
 *    the indicated lock, it will be ignored.  In this case, an error code will
 *    be returned only if there was another system error such as unilateral
 *    abort due to a deadlock detection.
 *    If quit_on_error is non-zero, the operation will stop the first time a
 *    lock cannot be obtained on any object.  The lock error will then be
 *    returned by this function.
 * return : error code
 * set(in/out) : a set object
 * purpose(in) : fetch purpose
 * quit_on_error(in): non-zero if operation quits after first error
 */
static int
fetch_set_internal (DB_SET * set, int quit_on_error)
{
  int error = NO_ERROR;
  DB_VALUE value;
  MOBJ obj;
  int max, cnt, i;
  DB_OBJECT **mops;

  CHECK_CONNECT_ERROR ();
  CHECK_1ARG_ERROR (set);

  max = set_size (set);
  if (max)
    {
      mops = (DB_OBJECT **) malloc ((max + 1) * sizeof (DB_OBJECT *));
      if (mops == NULL)
	{
	  return (er_errid ());
	}
      cnt = 0;

      for (i = 0; i < max && error == NO_ERROR; i++)
	{
	  error = set_get_element (set, i, &value);
	  if (error == NO_ERROR)
	    {
	      if (DB_VALUE_TYPE (&value) == DB_TYPE_OBJECT
		  && DB_GET_OBJECT (&value) != NULL)
		{
		  mops[cnt] = DB_GET_OBJECT (&value);
		  cnt++;
		}
	      db_value_clear (&value);
	    }
	}
      mops[cnt] = NULL;
      if (error == NO_ERROR && cnt)
	{
	  obj = locator_fetch_set (cnt, mops, S_LOCK, quit_on_error);
	  if (obj == NULL)
	    {
	      error = er_errid ();
	    }
	}

      for (i = 0; i < max; i++)
	mops[i] = NULL;

      free_and_init (mops);
    }

  return (error);
}

/*
 * db_fetch_set() - see the function fetch_set_internal()
 * return : error code
 * set(in/out) : a set object
 * purpose(in) : fetch purpose
 * quit_on_error(in): non-zero if operation quits after first error
 */
int
db_fetch_set (DB_SET * set, int quit_on_error)
{
  int retval;

  retval = (fetch_set_internal (set, quit_on_error));
  return (retval);
}

/*
 *  MISC UTILITIES
 */

/*
 * db_string_free() - Used to free strings that have been returned from any of
 *    the db_ functions.
 * return    : void
 * string(in): a string that was allocated within the workspace
 *
 */
void
db_string_free (char *string)
{
  /* don't check connection here, we always allow things to be freed */
  if (string != NULL)
    {
      db_ws_free (string);
    }
}

/*
 * db_objlist_free() - free an object list that was returned by one of the
 *    other db_ functions.
 * returns/side-effects: none
 * list(in): an object list
 */
void
db_objlist_free (DB_OBJLIST * list)
{
  /* don't check connection here, we always allow things to be freed */
  /* the list must have been allocated with the ml_ext functions */
  if (list != NULL)
    {
      ml_ext_free (list);
    }
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_identifier() - This function returns the permanent object identifier of
 *    the given object.
 * return : Pointer to object identifier
 * obj(in): Object
 */
DB_IDENTIFIER *
db_identifier (DB_OBJECT * obj)
{
  return ws_identifier (obj);
}

/*
 * db_object() - This function returns the object (MOP) associated with given
 *    permanent object identifier.
 * return : Pointer to Object
 * oid(in): Permanent Object Identifier
 */
DB_OBJECT *
db_object (DB_IDENTIFIER * oid)
{
  DB_OBJECT *retval;

  retval = ws_mop (oid, NULL);

  return (retval);
}
#endif

/*
 * db_update_persist_conf_file () - set new value into conf file
 *    return    : error code
 */
int
db_update_persist_conf_file (const char *proc_name, const char *sect_name,
			     const char *key, const char *value)
{
  assert (proc_name != NULL);
  assert (sect_name != NULL);
  assert (key != NULL);
  assert (value != NULL);

  if (proc_name == NULL || sect_name == NULL || key == NULL || value == NULL)
    {
      assert (false);
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR,
			   1, "db_update_persist_conf_file:");
      return ER_GENERIC_ERROR;
    }

  if (intl_mbs_casecmp ("DEFAULT", value) == 0)
    {
      return db_delete_key_persist_conf_file (proc_name, sect_name, key);
    }

  return sysprm_change_persist_conf_file (proc_name, sect_name, key, value);
}

/*
 * db_delete_proc_persist_conf_file () - delete proc from conf file
 *    return    : error code
 */
int
db_delete_proc_persist_conf_file (const char *proc_name)
{
  assert (proc_name != NULL);

  if (proc_name == NULL)
    {
      assert (false);
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR,
			   1, "db_delete_proc_persist_conf_file:");
      return ER_GENERIC_ERROR;
    }

  return sysprm_change_persist_conf_file (proc_name, NULL, NULL, NULL);
}

/*
 * db_delete_sect_persist_conf_file () - delete proc.sect from conf file
 *    return    : error code
 */
int
db_delete_sect_persist_conf_file (const char *proc_name,
				  const char *sect_name)
{
  assert (proc_name != NULL);
  assert (sect_name != NULL);

  if (proc_name == NULL || sect_name == NULL)
    {
      assert (false);
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR,
			   1, "db_delete_sect_persist_conf_file:");
      return ER_GENERIC_ERROR;
    }

  return sysprm_change_persist_conf_file (proc_name, sect_name, NULL, NULL);
}

/*
 * db_delete_key_persist_conf_file () - delete proc.sect.key from conf file
 *    return    : error code
 */
int
db_delete_key_persist_conf_file (const char *proc_name, const char *sect_name,
				 const char *key)
{
  assert (proc_name != NULL);
  assert (sect_name != NULL);
  assert (key != NULL);

  if (proc_name == NULL || sect_name == NULL || key == NULL)
    {
      assert (false);
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR,
			   1, "db_delete_key_persist_conf_file:");
      return ER_GENERIC_ERROR;
    }

  return sysprm_change_persist_conf_file (proc_name, sect_name, key, NULL);
}

/*
 * db_read_server_persist_conf_file () - get values from conf file
 *    return    : error code
 */
int
db_read_server_persist_conf_file (const char *sect_name, const bool reload)
{
  assert (sect_name != NULL);

  if (sect_name == NULL)
    {
      assert (false);
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR,
			   1, "db_read_server_persist_conf_file:");
      return ER_GENERIC_ERROR;
    }

  return prm_read_and_parse_server_persist_conf_file (sect_name, reload);
}

/*
 * db_read_broker_persist_conf_file () - get values from conf file
 *    return    : error code
 */
int
db_read_broker_persist_conf_file (INI_TABLE * ini)
{
  assert (ini != NULL);

  if (ini == NULL)
    {
      assert (false);
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR,
			   1, "db_read_broker_persist_conf_file:");
      return ER_GENERIC_ERROR;
    }

  return prm_read_and_parse_broker_persist_conf_file (ini);
}

/*
 * db_dump_persist_conf_file () -
 *    return    : error code
 */
int
db_dump_persist_conf_file (FILE * fp, const char *proc_name,
			   const char *sect_name)
{
  return sysprm_dump_persist_conf_file (fp, proc_name, sect_name);
}

/*
 * db_set_system_parameters () - set new values to system parameters
 *    return    : error code
 *
 *    prm_names(out): a list of system parameters to get with next
 *                 format: "param1; param2; ...".
 *    len(in): max length of prm_names
 *    data (in) : string with new parameter values defined as:
 *	          "param1=new_val1; param2=new_val2; ..."
 *    persist(in):
 */
int
db_set_system_parameters (char *prm_names, int len, const char *data,
			  const bool persist)
{
  int rc;
  int error = NO_ERROR;
  SYSPRM_ASSIGN_VALUE *assignments = NULL;

  /* validate changes */
  rc = sysprm_validate_change_parameters (data, persist, true, &assignments);
  /* If a server parameter is changed, user must belong to DBA group */
  if (rc == PRM_ERR_NOT_FOR_CLIENT && Au_dba_user != NULL
      && !au_is_dba_group_member (Au_user))
    {
      /* user is not authorized to do the changes */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_AU_DBA_ONLY, 1,
	      "db_set_system_parameters");
      error = ER_AU_DBA_ONLY;
      goto cleanup;
    }
  if (rc == PRM_ERR_NOT_FOR_CLIENT || rc == PRM_ERR_NOT_FOR_CLIENT_NO_AUTH)
    {
      /* set system parameters on server */
      rc = sysprm_change_server_parameters (assignments);
    }

  /* set them on client too */
  error = sysprm_change_parameter_values (assignments, true, true);
  if (error < 0)
    {
      if (er_errid () == NO_ERROR)
	{
	  error = ER_GENERIC_ERROR;
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
			       "sysprm_change_parameter_values:");
	}
      goto cleanup;
    }

  error = sysprm_print_assign_names (prm_names, len, assignments);
  if (error < 0)
    {
      assert (false);
      error = ER_GENERIC_ERROR;
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
			   "sysprm_print_assign_names:");
      goto cleanup;
    }

  /* convert SYSPRM_ERR to error code */
  error = sysprm_set_error (rc, data);

cleanup:
  /* clean up */
  sysprm_free_assign_values (&assignments);
  return error;
}

/*
 * db_get_system_parameters () - get system parameters values.
 *
 * return	 : error code.
 * data (in/out) : data (in) is a list of system parameters to get with next
 *		   format: "param1; param2; ...".
 *		   data (out) is a list of system parameters and their values
 *		   with the format: "param1=val1;param2=val2..."
 * len (in)	 : maximum allowed size of output.
 */
int
db_get_system_parameters (char *data, int len)
{
  int rc;
  int error = NO_ERROR;
  SYSPRM_ASSIGN_VALUE *prm_values = NULL;

  er_clear ();

  /* parse data and obtain the parameters required to print */
  rc = sysprm_obtain_parameters (data, &prm_values);
  if (rc == PRM_ERR_NOT_FOR_CLIENT)
    {
      /* obtain parameter values from server */
      rc = sysprm_obtain_server_parameters (&prm_values);
    }

  /* convert SYSPRM_ERR to error code */
  error = sysprm_set_error (rc, data);

  /* print parameter values in data */
  memset (data, 0, len);
  (void) sysprm_print_assign_values (prm_values, data, len);

  /* clean up */
  sysprm_free_assign_values (&prm_values);
  return error;
}

/*
 * db_get_system_parameter_value () - get system parameters value.
 *
 * return        : error code.
 *
 * value(out)    : value string of system parameter
 * max_len (in)  : maximum allowed size of value.
 * param_name(in): name of system parameter
 */
int
db_get_system_parameter_value (char *value, int max_len,
			       const char *param_name)
{
  int error = NO_ERROR;
  char buffer[512];
  char *p;

  strncpy (buffer, param_name, sizeof (buffer));
  buffer[sizeof (buffer) - 1] = 0;

  error = db_get_system_parameters (buffer, sizeof (buffer));
  if (error != NO_ERROR)
    {
      return error;
    }

  p = strchr (buffer, '=');
  if (p == NULL)
    {
      error = sysprm_set_error (PRM_ERR_UNKNOWN_PARAM, param_name);

      return error;
    }
  strncpy (value, p + 1, max_len);

  assert (error == NO_ERROR);
  return error;
}

int
db_get_server_start_time (void)
{
#if defined(CS_MODE)
  return boot_get_server_start_time ();
#else
  return 0;
#endif
}

void
db_set_server_session_key (const char *key)
{
  boot_set_server_session_key (key);
}

char *
db_get_server_session_key (void)
{
  return boot_get_server_session_key ();
}

/*
 * db_get_session_id () - get current session id
 * return : session id
 */
SESSION_ID
db_get_session_id (void)
{
  return db_Session_id;
}

/*
 * db_set_session_id () - set current session id
 * return : void
 * session_id (in): session id
 */
void
db_set_session_id (const SESSION_ID session_id)
{
  db_Session_id = session_id;
}

/*
 * db_find_or_create_session - check if current session is still active
 *                               if not, create a new session
 * return error code or NO_ERROR
 * db_user(in)  :
 * program_name(in)  :
 * Note: This function will check if the current session is active and will
 *	 create a new one if needed and save user access status in server
 */
int
db_find_or_create_session (const char *db_user, const char *program_name)
{
  int err = NO_ERROR;
  SESSION_ID sess_id = db_get_session_id ();
  char *server_session_key;
  const char *host_name = boot_get_host_name ();

  server_session_key = db_get_server_session_key ();
  /* server_session_key is in/out parameter, it is replaced new key */
  err =
    csession_find_or_create_session (&sess_id,
				     server_session_key, db_user, host_name,
				     program_name);
  if (err != NO_ERROR)
    {
      db_set_session_id (DB_EMPTY_SESSION);
      return err;
    }

  db_set_session_id (sess_id);

  return NO_ERROR;
}

void
db_set_client_ro_tran (UNUSED_ARG bool mode)
{
#if defined (CS_MODE)
  net_client_set_ro_tran (mode);
#endif
}

bool
db_is_server_in_tran ()
{
#if defined (CS_MODE)
  return net_client_is_server_in_transaction ();
#else
  return true;
#endif
}

short
db_server_shard_nodeid ()
{
#if defined (CS_MODE)
  return net_client_server_shard_nodeid ();
#else
  return 0;
#endif
}

int
db_update_group_id (UNUSED_ARG int migrator_id, UNUSED_ARG int group_id,
		    UNUSED_ARG int target, UNUSED_ARG int on_off)
{
#if defined (CS_MODE)
  return logtb_update_group_id (migrator_id, group_id, target, on_off);
#else
  return 0;
#endif
}

int
db_block_globl_dml (UNUSED_ARG int start_or_end)
{
#if defined (CS_MODE)
  return logtb_block_globl_dml (start_or_end);
#else
  return 0;
#endif
}

int
db_get_server_state ()
{
  return boot_get_server_state ();
}

unsigned int
db_get_server_addr ()
{
  unsigned int server_addr = INADDR_NONE;

#if defined(CS_MODE)
  in_addr_t in_addr = net_client_get_server_addr ();
  assert (sizeof (server_addr) == sizeof (in_addr));
  memcpy (&server_addr, &in_addr, sizeof (in_addr));
#endif

  return server_addr;
}
