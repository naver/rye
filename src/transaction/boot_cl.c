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
 * boot_cl.c - Boot management in the client
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <string.h>
#include <sys/time.h>

#include <stdio.h>
#include <unistd.h>


#include <assert.h>

#include "porting.h"
#include "util_func.h"
#include "boot_cl.h"
#include "memory_alloc.h"
#include "storage_common.h"
#include "oid.h"
#include "error_manager.h"
#include "work_space.h"
#include "schema_manager.h"
#include "authenticate.h"
#include "db.h"
#include "transaction_cl.h"
#include "log_comm.h"
#include "server_interface.h"
#include "release_string.h"
#include "system_parameter.h"
#include "locator_cl.h"
#include "databases_file.h"
#include "db_query.h"
#include "language_support.h"
#include "message_catalog.h"
#include "parser.h"
#include "set_object.h"
#include "cnv.h"
#include "environment_variable.h"
#include "locator.h"
#include "transform.h"
#include "client_support.h"

#if defined(CS_MODE)
#include "network.h"
#include "connection_cl.h"
#endif /* CS_MODE */
#include "network_interface_cl.h"

/* TODO : Move .h */
#if defined(SA_MODE)
extern bool catcls_Enable;
extern int catcls_compile_catalog_classes (THREAD_ENTRY * thread_p);
#endif /* SA_MODE */

#define BOOT_FORMAT_MAX_LENGTH 500

/* for optional capability check */
#define BOOT_NO_OPT_CAP                 0
#define BOOT_CHECK_HA_DELAY_CAP         NET_CAP_HA_REPL_DELAY

typedef int (*DEF_FUNCTION) ();
typedef int (*DEF_CLASS_FUNCTION) (MOP);

typedef struct catcls_function CATCLS_FUNCTION;
struct catcls_function
{
  const char *name;
  const DEF_FUNCTION function;
};

static BOOT_SERVER_CREDENTIAL boot_Server_credential = {
  /* db_full_name */ NULL, /* host_name */ NULL,
  /* process_id */ -1,
  /* root_class_oid */ NULL_OID_INITIALIZER,
  /* root_class_hfid */ {{NULL_FILEID, NULL_VOLID}, NULL_PAGEID},
  /* data page_size */ -1, /* log page_size */ -1,
  /* server_session_key */ {0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF},
  INTL_CODESET_NONE,
  NULL,
  0,
  NULL
};

static const char *boot_Client_no_user_string = "(nouser)";
static const char *boot_Client_id_unknown_string = "(unknown)";

static char boot_Client_id_buffer[L_cuserid + 1];
static char boot_Db_host_buf[MAXHOSTNAMELEN + 1];

/* Volume assigned for new files/objects (e.g., heap files) */
VOLID boot_User_volid = 0;
#if defined(CS_MODE)
/* Server host connected */
static PRM_NODE_INFO boot_Host_connected = PRM_NULL_NODE_INFO;
#endif /* CS_MODE */
char boot_Host_name[MAXHOSTNAMELEN] = "";
RYE_VERSION boot_Peer_version = RYE_CUR_VERSION;

static char boot_Volume_label[PATH_MAX] = " ";
static bool boot_Is_client_all_final = true;
static bool boot_Set_client_at_exit = false;
static int boot_Process_id = -1;

static int boot_client (int tran_index, int lock_wait);
#if 0
static void boot_shutdown_client_at_exit (void);
#endif
#if defined(CS_MODE)
static int boot_cli_init_css (const char *dbname,
			      const PRM_NODE_LIST * node_list,
			      int client_type,
			      bool check_capabilities, int opt_cap,
			      bool discriminative, bool is_preferred_host);
#endif /* CS_MODE */
static int boot_define_class (MOP class_mop);
static int boot_define_attribute (MOP class_mop);
static int boot_define_domain (MOP class_mop);
static int boot_define_query_spec (MOP class_mop);
static int boot_define_index (MOP class_mop);
static int boot_define_index_key (MOP class_mop);
static int boot_add_data_type (MOP class_mop);
static int boot_define_data_type (MOP class_mop);
static int boot_define_collations (MOP class_mop);
static int catcls_class_install (void);
static int boot_define_catalog_table (void);
#if defined(CS_MODE)
static int boot_check_locales (BOOT_CLIENT_CREDENTIAL * cli_cred);
#endif /* CS_MODE */
/*
 * boot_client () -
 *
 * return :
 *
 *   tran_index(in) : transaction index
 *   lock_wait(in) :
 *
 * Note: macros that find if the rye client is restarted
 */
static int
boot_client (int tran_index, int lock_wait)
{
  tran_cache_tran_settings (tran_index, lock_wait);

  if (boot_Set_client_at_exit)
    {
      return NO_ERROR;
    }

  boot_Set_client_at_exit = true;
  boot_Process_id = getpid ();
#if 0
  atexit (boot_shutdown_client_at_exit);
#endif

  return NO_ERROR;
}

/*
 * boot_initialize_client () -
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   cli_cred(in): Contains database access information such as :
 *                          database name, user name and password, client type
 *   db_path_info(in) : Directory where the database is created. It allows you
 *                      to specify the exact pathname of a directory in which
  *                     to create the new database.
 *   db_overwrite(in) : Wheater to overwrite the database if it already exist.
 *   file_addmore_vols(in): More volumes are created during the initialization
 *                      process.
 *   npages(in)       : Total number of pages to allocate for the database.
 *   db_desired_pagesize(in): Desired pagesize for the new database.
 *                      The given size must be power of 2 and greater or
 *                      equal than 512.
 *   log_npages(in)   : Number of log pages. If log_npages <=0, default value
 *                      of system parameter is used.
 *   db_desired_log_page_size(in):
 *   lang_charset(in): language and charset to set on DB
 *
 * Note:
 *              The first step of any Rye application is to initialize a
 *              database. A database is composed of data volumes (or Unix file
 *              system files), database backup files, and log files. A data
 *              volume contains information on attributes, classes, indexes,
 *              and objects created in the database. A database backup is a
 *              fuzzy snapshot of the entire database. The backup is fuzzy
 *              since it can be taken online when other transactions are
 *              updating the database. The logs contain records that reflect
 *              changes to the database. The log and backup files are used by
 *              the system to recover committed and uncommitted transactions
 *              in the event of system and media crashes. Logs are also used
 *              to support user-initiated rollbacks. This function also
 *              initializes the database with built-in Rye classes.
 *
 *              The rest of this function is identical to the restart. The
 *              transaction for the current client session is automatically
 *              started.
 */
int
boot_initialize_client (BOOT_CLIENT_CREDENTIAL * cli_cred,
			BOOT_DB_PATH_INFO * db_path_info,
			bool db_overwrite, const char *file_addmore_vols,
			DKNPAGES npages, PGLENGTH db_desired_pagesize,
			DKNPAGES log_npages,
			PGLENGTH db_desired_log_page_size)
{
  OID rootclass_oid;		/* Oid of root class */
  HFID rootclass_hfid;		/* Heap for classes */
  int tran_index = NULL_TRAN_INDEX;	/* Assigned transaction index */
  int tran_lock_wait_msecs;	/* Default lock waiting */
  unsigned int length;
  int error_code = NO_ERROR;
#if defined (CS_MODE)
  char format[BOOT_FORMAT_MAX_LENGTH];
#endif
  char *db_user_alloced = NULL;
  char pathname[PATH_MAX];
  PRM_NODE_LIST node_list;

  assert (cli_cred != NULL);
  assert (db_path_info != NULL);

  tran_index = NULL_TRAN_INDEX;
  /* If the client is restarted, shutdown the client */
  if (BOOT_IS_CLIENT_RESTARTED ())
    {
      (void) boot_shutdown_client ();
    }

  if (!boot_Is_client_all_final)
    {
      boot_client_all_finalize ();
    }

  /*
   * initialize language parameters  */
  if (lang_init () != NO_ERROR)
    {
      if (er_errid () == NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOC_INIT, 1,
		  "Failed to initialize language module");
	}
      error_code = ER_LOC_INIT;
      goto error_exit;
    }

  if (lang_set_charset_lang () != NO_ERROR)
    {
      error_code = ER_LOC_INIT;
      goto error_exit;
    }

  /* database name must be specified */
  if (cli_cred->db_name == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_BO_UNKNOWN_DATABASE, 1,
	      "(null)");
      error_code = ER_BO_UNKNOWN_DATABASE;
      goto error_exit;
    }

  /* open the system message catalog, before prm_ ?  */
  if (msgcat_init () != NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_BO_CANNOT_ACCESS_MESSAGE_CATALOG, 0);
      error_code = ER_BO_CANNOT_ACCESS_MESSAGE_CATALOG;
      goto error_exit;
    }

  /* initialize system parameters */
  if (sysprm_load_and_init_client (cli_cred->db_name) != NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_BO_CANT_LOAD_SYSPRM, 0);
      error_code = ER_BO_CANT_LOAD_SYSPRM;
      goto error_exit;
    }

  /* initialize the "areas" memory manager */
  locator_initialize_areas ();

  (void) db_set_page_size (db_desired_pagesize, db_desired_log_page_size);

  if (envvar_db_dir (pathname, PATH_MAX, cli_cred->db_name) == NULL)
    {
      assert (false);
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_BO_CWD_FAIL, 0);
      error_code = ER_BO_CWD_FAIL;
      goto error_exit;
    }

  /* make sure that the full path for the database is not too long */
  length = strlen (cli_cred->db_name) + strlen (pathname) + 2;
  if (length > (unsigned) PATH_MAX)
    {
      /* db_path + db_name is too long */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_BO_FULL_DATABASE_NAME_IS_TOO_LONG, 3, pathname,
	      cli_cred->db_name, length, PATH_MAX);

      error_code = ER_BO_FULL_DATABASE_NAME_IS_TOO_LONG;
      goto error_exit;
    }

  /* If a host was not given, assume the current host */
  assert (db_path_info->db_host == NULL);
  if (db_path_info->db_host == NULL)
    {
#if 0				/* use Unix-domain socket for localhost */
      if (GETHOSTNAME (db_host_buf, MAXHOSTNAMELEN) != 0)
	{
	  er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			       ER_BO_UNABLE_TO_FIND_HOSTNAME, 0);
	  error_code = ER_BO_UNABLE_TO_FIND_HOSTNAME;
	  goto error_exit;
	}
      db_host_buf[MAXHOSTNAMELEN] = '\0';
#else
      strcpy (boot_Db_host_buf, "localhost");
#endif
      db_path_info->db_host = boot_Db_host_buf;
    }

  prm_split_node_str (&node_list, db_path_info->db_host, false);

  /* Get the absolute path name */
  COMPOSE_FULL_NAME (boot_Volume_label, sizeof (boot_Volume_label),
		     pathname, cli_cred->db_name);

  er_clear ();

  /* Get the user name */
  if (cli_cred->db_user == NULL)
    {
      char *user_name = au_user_name_dup ();
      int upper_case_name_size;
      char *upper_case_name;

      if (user_name != NULL)
	{
	  upper_case_name_size = strlen (user_name);
	  upper_case_name = (char *) malloc (upper_case_name_size + 1);
	  if (upper_case_name == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, upper_case_name_size + 1);
	    }
	  else
	    {
	      intl_identifier_upper (user_name, upper_case_name);
	      cli_cred->db_user = upper_case_name;
	      db_user_alloced = upper_case_name;
	    }
	  free_and_init (user_name);
	}
      upper_case_name = NULL;

      if (cli_cred->db_user == NULL)
	{
	  cli_cred->db_user = boot_Client_no_user_string;
	}
    }
  /* Get the login name, host, and process identifier */
  if (cli_cred->login_name == NULL)
    {
      if (getuserid (boot_Client_id_buffer, L_cuserid) != (char *) NULL)
	{
	  cli_cred->login_name = boot_Client_id_buffer;
	}
      else
	{
	  cli_cred->login_name = boot_Client_id_unknown_string;
	}
    }

  if (cli_cred->host_name == NULL)
    {
      cli_cred->host_name = boot_get_host_name ();
    }

#if defined(CS_MODE)
  /* Initialize the communication subsystem */
  error_code = boot_cli_init_css (cli_cred->db_name, &node_list,
				  cli_cred->client_type, false,
				  BOOT_NO_OPT_CAP, false, false);
  if (error_code != NO_ERROR)
    {
      goto error_exit;
    }
#endif /* CS_MODE */

  boot_User_volid = 0;
  tran_lock_wait_msecs = prm_get_bigint_value (PRM_ID_LK_TIMEOUT);

  /* this must be done before the init_server because recovery steps
   * may need domains.
   */
  tp_init ();

  if (tran_lock_wait_msecs > 0)
    {
      tran_lock_wait_msecs = tran_lock_wait_msecs * 1000;
    }
  /* Initialize the disk and the server part */
  tran_index = boot_initialize_server (cli_cred, db_path_info,
				       db_overwrite, file_addmore_vols,
				       npages, db_desired_pagesize,
				       log_npages, db_desired_log_page_size,
				       &rootclass_oid, &rootclass_hfid,
				       tran_lock_wait_msecs);
  if (db_user_alloced != NULL)
    {
      assert (cli_cred->db_user != NULL);
      assert (cli_cred->db_user != boot_Client_no_user_string);
      free_and_init (db_user_alloced);
      cli_cred->db_user = NULL;
    }

  if (tran_index == NULL_TRAN_INDEX)
    {
      error_code = er_errid ();
      if (error_code == NO_ERROR)
	{
	  error_code = ER_GENERIC_ERROR;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1, "");
	}
      goto error_exit;
    }

  oid_set_root (&rootclass_oid);
  OID_INIT_TEMPID ();

  error_code = ws_init ();

  if (error_code != NO_ERROR)
    {
      goto error_exit;
    }

  /* Create system classes such as the root and authorization classes */
  sm_create_root (&rootclass_oid, &rootclass_hfid);
  au_init ();

  /* Create authorization classes and enable authorization */
  error_code = au_install ();
  if (error_code != NO_ERROR)
    {
      goto error_exit;
    }

  error_code = au_start ();
  if (error_code != NO_ERROR)
    {
      goto error_exit;
    }

  error_code = catcls_class_install ();
  if (error_code != NO_ERROR)
    {
      goto error_exit;
    }

  /*
   * mark all classes created during the initialization as "system"
   * classes,
   */
  sm_mark_system_classes ();
  error_code = tran_commit ();
  if (error_code != NO_ERROR)
    {
      goto error_exit;
    }

  boot_client (tran_index, tran_lock_wait_msecs);
#if defined (CS_MODE)
  /* print version string */
  strncpy (format, msgcat_message (MSGCAT_CATALOG_RYE,
				   MSGCAT_SET_GENERAL,
				   MSGCAT_GENERAL_DATABASE_INIT),
	   BOOT_FORMAT_MAX_LENGTH);
  (void) fprintf (stdout, format, rel_package_string ());
#endif /* CS_MODE */

  assert (error_code == NO_ERROR);
  return error_code;

error_exit:

  if (error_code == NO_ERROR)
    {
      assert (error_code != NO_ERROR);

      error_code = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1, "");
    }

  if (tran_index != NULL_TRAN_INDEX)
    {
      (void) boot_shutdown_client ();
    }

  if (db_user_alloced != NULL)
    {
      assert (cli_cred->db_user != NULL);
      assert (cli_cred->db_user != boot_Client_no_user_string);
      free_and_init (db_user_alloced);
      cli_cred->db_user = NULL;
    }

  return error_code;
}

/*
 * boot_restart_client () - restart client
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   cli_cred(in) : Information required to start as client, such as:
 *                           database name, user name and password, client
 *                           type.
 *
 * Note:
 *              An application must restart the database system with the
 *              desired database (the database must have already been created)
 *              before the application start invoking the Rye functional
 *              interface. This function restarts the Rye client. It also
 *              initializes all client modules for the execution of the client
 *              interface. A transaction for the current client session is
 *              automatically started.
 *
 *              It is very important that the application check for success
 *              of this function before calling any other Rye function.
 */

int
boot_restart_client (BOOT_CLIENT_CREDENTIAL * cli_cred)
{
  int tran_index;
  int tran_lock_wait_msecs;
  TRAN_STATE transtate;
  int error_code = NO_ERROR;
  char *ptr;
#if defined(CS_MODE)
  int i, optional_cap;
  bool check_capabilities;
  bool skip_preferred_hosts = false;
  bool skip_db_info = false;
  char connected_node_str[MAX_NODE_INFO_STR_LEN];
  PRM_NODE_LIST node_list;
#endif /* CS_MODE */
  char *db_user_alloced = NULL;
  char db_name[DB_MAX_IDENTIFIER_LENGTH];

  assert (cli_cred != NULL);

  /* If the client is restarted, shutdown the client */
  if (BOOT_IS_CLIENT_RESTARTED ())
    {
      (void) boot_shutdown_client ();
    }

  if (!boot_Is_client_all_final)
    {
      boot_client_all_finalize ();
    }

  /* initialize language parameters */
  if (lang_init () != NO_ERROR)
    {
      if (er_errid () == NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_LOC_INIT, 1,
		  "Failed to initialize language module");
	}
      return ER_LOC_INIT;
    }

  /* database name must be specified */
  if (cli_cred->db_name == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_BO_UNKNOWN_DATABASE, 1,
	      "(null)");
      return ER_BO_UNKNOWN_DATABASE;
    }

  /* open the system message catalog, before prm_ ?  */
  if (msgcat_init () != NO_ERROR)
    {
      error_code = ER_BO_CANNOT_ACCESS_MESSAGE_CATALOG;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 0);
      goto error;
    }

  /* initialize system parameters */
  if (sysprm_load_and_init_client (cli_cred->db_name) != NO_ERROR)
    {
      error_code = ER_BO_CANT_LOAD_SYSPRM;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 0);
      goto error;
    }

  /* initialize the "areas" memory manager, requires prm_ */
  locator_initialize_areas ();

  ptr = (char *) strstr (cli_cred->db_name, "@");
  if (ptr == NULL)
    {
      strncpy (db_name, cli_cred->db_name, sizeof (db_name));
      db_name[sizeof (db_name) - 1] = '\0';

      if (cli_cred->client_type == BOOT_CLIENT_REPL_BROKER)
	{
#if defined(CS_MODE)
	  prm_split_node_str (&node_list, NULL, true);
#else /* CS_MODE */
	  error_code = ER_NOT_IN_STANDALONE;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code,
		  1, cli_cred->db_name);
	  goto error;
#endif /* !CS_MODE */
	}
      else
	{
#if defined(CS_MODE)
	  cfg_get_hosts_from_shm (&node_list,
				  cli_cred->client_type,
				  cli_cred->connect_order_random);
	  if (node_list.num_nodes <= 0)
	    {
	      prm_get_ha_node_list (&node_list);
	      if (node_list.num_nodes <= 0)
		{
		  prm_split_node_str (&node_list, NULL, true);
		}
	    }

	  if (node_list.num_nodes <= 0 ||
	      (node_list.num_nodes > 1 &&
	       (BOOT_ADMIN_CLIENT_TYPE (cli_cred->client_type) ||
		BOOT_RSQL_CLIENT_TYPE (cli_cred->client_type) ||
		BOOT_LOG_REPLICATOR_TYPE (cli_cred->client_type))))
	    {
	      error_code = ER_NET_NO_EXPLICIT_SERVER_HOST;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 0);
	      goto error;
	    }
#endif /* CS_MODE */
	}
    }
  else
    {
      /* db_name@host_name */
#if defined(CS_MODE)
      *ptr = '\0';		/* screen 'db@host' */

      prm_split_node_str (&node_list, ptr + 1, false);

      strncpy (db_name, cli_cred->db_name, sizeof (db_name));
      db_name[sizeof (db_name) - 1] = '\0';

      *ptr = (char) '@';
#else /* CS_MODE */
      error_code = ER_NOT_IN_STANDALONE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code,
	      1, cli_cred->db_name);
      goto error;
#endif /* !CS_MODE */
    }

  er_clear ();

  /* Get the user name */
  if (cli_cred->db_user == NULL)
    {
      char *user_name = au_user_name_dup ();

      if (user_name != NULL)
	{
	  /* user name is upper-cased in server using server's charset */
	  cli_cred->db_user = user_name;
	  db_user_alloced = user_name;
	}

      if (cli_cred->db_user == NULL)
	{
	  cli_cred->db_user = boot_Client_no_user_string;
	}
      else if (cli_cred->db_user[0] == '\0')
	{
	  free_and_init (db_user_alloced);
	  cli_cred->db_user = AU_PUBLIC_USER_NAME;
	}
    }
  /* Get the login name, host, and process identifier */
  if (cli_cred->login_name == NULL)
    {
      if (getuserid (boot_Client_id_buffer, L_cuserid) != (char *) NULL)
	{
	  cli_cred->login_name = boot_Client_id_buffer;
	}
      else
	{
	  cli_cred->login_name = boot_Client_id_unknown_string;
	}
    }
  if (cli_cred->host_name == NULL)
    {
      cli_cred->host_name = boot_get_host_name ();
    }
  cli_cred->process_id = getpid ();

  /* read only mode? */
  if (BOOT_READ_ONLY_CLIENT_TYPE (cli_cred->client_type))
    {
      db_disable_modification ();
    }

#if defined(CS_MODE)
  /* Initialize the communication subsystem */
  db_clear_reconnect_reason ();
  db_clear_ignore_repl_delay ();

  for (i = 0; i < 2; i++)
    {
      if (BOOT_IS_PREFERRED_HOSTS_SET (cli_cred)
	  && skip_preferred_hosts == false)
	{
	  check_capabilities = true;

	  if (i == 0)		/* first */
	    {
	      optional_cap = BOOT_CHECK_HA_DELAY_CAP;
	    }
	  else			/* second */
	    {
	      if (!BOOT_REPLICA_ONLY_BROKER_CLIENT_TYPE
		  (cli_cred->client_type)
		  && BOOT_NORMAL_CLIENT_TYPE (cli_cred->client_type))
		{
		  check_capabilities = false;
		}
	      db_set_ignore_repl_delay ();

	      optional_cap = BOOT_NO_OPT_CAP;
	    }

	  boot_Host_connected.ip = INADDR_NONE;

	  db_unset_reconnect_reason (DB_RC_NON_PREFERRED_HOSTS);

	  /* connect to preferred hosts in a sequential order even though
	   * a user sets CONNECT_ORDER to RANDOM
	   */
	  error_code = boot_cli_init_css (db_name, &cli_cred->preferred_nodes,
					  cli_cred->client_type,
					  check_capabilities,
					  optional_cap, false, true);

	  if (error_code != NO_ERROR)
	    {
	      db_set_reconnect_reason (DB_RC_NON_PREFERRED_HOSTS);

	      if (error_code == ER_NET_SERVER_HAND_SHAKE)
		{
		  er_log_debug (ARG_FILE_LINE, "boot_restart_client: "
				"boot_cli_init_css () ER_NET_SERVER_HAND_SHAKE\n");

		  boot_Host_connected.ip = INADDR_NONE;
		}
	      else
		{
		  skip_preferred_hosts = true;
		}
	    }
	}

      if (skip_db_info == true)
	{
	  continue;
	}

      if (BOOT_IS_PREFERRED_HOSTS_SET (cli_cred) && error_code == NO_ERROR)
	{
	  /* connected to any preferred hosts successfully */
	  break;
	}
      else
	if (BOOT_REPLICA_ONLY_BROKER_CLIENT_TYPE (cli_cred->client_type) ||
	    cli_cred->client_type == BOOT_CLIENT_SLAVE_ONLY_BROKER)

	{
	  check_capabilities = true;
	  if (i == 0)		/* first */
	    {
	      optional_cap = BOOT_CHECK_HA_DELAY_CAP;
	    }
	  else			/* second */
	    {
	      db_set_ignore_repl_delay ();

	      optional_cap = BOOT_NO_OPT_CAP;
	    }

	  error_code = boot_cli_init_css (db_name, &node_list,
					  cli_cred->client_type,
					  check_capabilities,
					  optional_cap, false, false);
	}
      else if (BOOT_RSQL_CLIENT_TYPE (cli_cred->client_type))
	{
	  assert (!BOOT_IS_PREFERRED_HOSTS_SET (cli_cred));

	  check_capabilities = false;
	  optional_cap = BOOT_NO_OPT_CAP;

	  error_code = boot_cli_init_css (db_name, &node_list,
					  cli_cred->client_type,
					  check_capabilities, optional_cap,
					  false, false);
	  break;		/* dont retry */
	}
      else if (BOOT_NORMAL_CLIENT_TYPE (cli_cred->client_type))
	{
	  if (i == 0)		/* first */
	    {
	      check_capabilities = true;
	      optional_cap = BOOT_CHECK_HA_DELAY_CAP;
	    }
	  else			/* second */
	    {
	      db_set_ignore_repl_delay ();

	      check_capabilities = false;
	      optional_cap = BOOT_NO_OPT_CAP;
	    }

	  error_code = boot_cli_init_css (db_name, &node_list,
					  cli_cred->client_type,
					  check_capabilities, optional_cap,
					  false, false);

	}
      else
	{
	  assert (!BOOT_IS_PREFERRED_HOSTS_SET (cli_cred));

	  check_capabilities = false;
	  optional_cap = BOOT_NO_OPT_CAP;
	  error_code = boot_cli_init_css (db_name, &node_list,
					  cli_cred->client_type,
					  check_capabilities, optional_cap,
					  false, false);
	  break;		/* dont retry */
	}

      if (error_code == NO_ERROR)
	{
	  break;
	}
      else if (error_code == ER_NET_SERVER_HAND_SHAKE)
	{
	  er_log_debug (ARG_FILE_LINE, "boot_restart_client: "
			"boot_cli_init_css () ER_NET_SERVER_HAND_SHAKE\n");
	}
      else
	{
	  skip_db_info = true;
	}
    }

  if (error_code != NO_ERROR)
    {
      er_log_debug (ARG_FILE_LINE, "boot_restart_client: "
		    "boot_cli_init_css () error %d\n", error_code);
      goto error;
    }
  prm_node_info_to_str (connected_node_str, sizeof (connected_node_str),
			&boot_Host_connected);
  er_set (ER_NOTIFICATION_SEVERITY, ARG_FILE_LINE, ER_BO_CONNECTED_TO, 4,
	  cli_cred->program_name, cli_cred->process_id,
	  cli_cred->db_name, connected_node_str);

  /* tune some client parameters with the value from the server */
  sysprm_tune_client_parameters ();
#endif /* CS_MODE */

  /* this must be done before the register_client because recovery steps
   * may need domains.
   */
  tp_init ();
  error_code = ws_init ();
  if (error_code != NO_ERROR)
    {
      goto error;
    }

  /*
   * At this moment, we should use the default wait timeout,
   * since the client fetches objects during the restart process.
   * This values are reset at a later point, once the client has been fully
   * restarted.
   */

  tran_lock_wait_msecs = TRAN_LOCK_INFINITE_WAIT;

  er_log_debug (ARG_FILE_LINE, "boot_restart_client: "
		"register client { type %d db %s user %s password %s "
		"program %s login %s host %s pid %d }\n",
		cli_cred->client_type,
		cli_cred->db_name, cli_cred->db_user,
		cli_cred->db_password ==
		NULL ? "(null)" : cli_cred->db_password,
		cli_cred->program_name,
		cli_cred->login_name,
		cli_cred->host_name, cli_cred->process_id);

  tran_index = boot_register_client (cli_cred,
				     tran_lock_wait_msecs,
				     &transtate, &boot_Server_credential);

  if (tran_index == NULL_TRAN_INDEX)
    {
      error_code = er_errid ();
      goto error;
    }

#if defined(CS_MODE)
  if (lang_set_charset_lang () != NO_ERROR)
    {
      error_code = er_errid ();
      goto error;
    }

  /* Reset the pagesize according to server.. */
  if (db_set_page_size (boot_Server_credential.page_size,
			boot_Server_credential.log_page_size) != NO_ERROR)
    {
      error_code = er_errid ();
      goto error;
    }
#endif /* CS_MODE */

#if defined (ENABLE_UNUSED_FUNCTION)
  sysprm_init_intl_param ();
#endif

  /* Initialize client modules for execution */
  boot_client (tran_index, tran_lock_wait_msecs);

  oid_set_root (&boot_Server_credential.root_class_oid);
  OID_INIT_TEMPID ();

  sm_init (&boot_Server_credential.root_class_oid,
	   &boot_Server_credential.root_class_hfid);
  au_init ();			/* initialize authorization globals */

  /* start authorization and make sure the logged in user has access */
  error_code = au_start ();
  if (error_code != NO_ERROR)
    {
      goto error;
    }

  (void) db_find_or_create_session (cli_cred->db_user,
				    cli_cred->program_name);
  /* free the thing get from au_user_name_dup() */
  if (db_user_alloced != NULL)
    {
      assert (cli_cred->db_user != NULL);
      assert (cli_cred->db_user != boot_Client_no_user_string);
      assert (cli_cred->db_user != AU_PUBLIC_USER_NAME);
      free_and_init (db_user_alloced);
      cli_cred->db_user = NULL;
    }

#if defined(CS_MODE)
  error_code = boot_check_locales (cli_cred);
  if (error_code != NO_ERROR)
    {
      goto error;
    }
#endif /* CS_MODE */

  /* If the client has any loose ends from the recovery manager, do them */

  if (transtate != TRAN_ACTIVE)
    {
      error_code = er_errid ();
      goto error;
    }
  /* Does not care if was committed/aborted .. */
  (void) tran_commit ();

  /*
   * If there is a need to change the lock wait,
   * do it at this moment
   */
  tran_lock_wait_msecs = prm_get_bigint_value (PRM_ID_LK_TIMEOUT);
  if (tran_lock_wait_msecs >= 0)
    {
      (void) tran_reset_wait_times (tran_lock_wait_msecs * 1000);
    }

  return error_code;

error:

  /* free the thing get from au_user_name_dup() */
  if (db_user_alloced != NULL)
    {
      assert (cli_cred->db_user != NULL);
      assert (cli_cred->db_user != boot_Client_no_user_string);
      assert (cli_cred->db_user != AU_PUBLIC_USER_NAME);
      free_and_init (db_user_alloced);
      cli_cred->db_user = NULL;
    }

  /* Protect against falsely returning NO_ERROR to caller */
  if (error_code == NO_ERROR)
    {
      error_code = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1, "");
    }

  if (BOOT_IS_CLIENT_RESTARTED ())
    {
      er_log_debug (ARG_FILE_LINE, "boot_shutdown_client: "
		    "unregister client { tran %d }\n", tm_Tran_index);
      boot_shutdown_client ();
    }
  else
    {
      /* msgcat_final (); */
#if 0
      lang_final ();
#endif

#if 0
      sysprm_final ();
#endif
    }

  return error_code;
}

/*
 * boot_shutdown_client () - shutdown client
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 * Note:
 *              This function should be called before the Rye
 *              application is finished. This function will notify the
 *              recovery manager that the application has finished and will
 *              terminate all client modules (e.g., allocation of memory is
 *              deallocated).If there are active transactions, they are either
 *              committed or aborted according to the commit_on_shutdown
 *              system parameter.
 */

int
boot_shutdown_client (void)
{
  if (BOOT_IS_CLIENT_RESTARTED ())
    {
      /*
       * wait for other server request to finish.
       * if db_shutdown() is called by signal handler or atexit handler,
       * the server request may be running.
       */
      tran_wait_server_active_trans ();

      /*
       * Either Abort or commit the current transaction depending upon the value
       * of the commit_on_shutdown system parameter.
       */
      if (tran_is_active_and_has_updated ())
	{
	  if (prm_get_bool_value (PRM_ID_COMMIT_ON_SHUTDOWN) != false)
	    {
	      (void) tran_commit ();
	    }
	  else
	    {
	      (void) tran_abort ();
	    }
	}

      /*
       * Make sure that we are still up. For example, if the server died, we do
       * not need to call the following stuff any longer.
       */

      if (BOOT_IS_CLIENT_RESTARTED ())
	{
	  (void) boot_unregister_client (tm_Tran_index);
#if defined(CS_MODE)
	  (void) net_client_final ();
#endif /* !CS_MODE */
	}

      boot_client_all_finalize ();
    }

  return NO_ERROR;
}

#if 0
/*
 * boot_shutdown_client_at_exit () - make sure that the client is shutdown at exit
 *
 * return : nothing
 *
 * Note:
 *       This function is called when the invoked program terminates
 *       normally. This function make sure that the client is shutdown
 *       in a nice way.
 */
static void
boot_shutdown_client_at_exit (void)
{
  if (BOOT_IS_CLIENT_RESTARTED () && boot_Process_id == getpid ())
    {
      /* Avoid infinite looping if someone calls exit during shutdown */
      boot_Process_id++;
      (void) boot_shutdown_client (true);
    }
}
#endif

#if !defined(SERVER_MODE)
/*
 * boot_donot_shutdown_client_at_exit: do not shutdown client at exist.
 *
 * return : nothing
 *
 * This function must be called when the system needs to exit
 *  without shutting down the client (e.g., in case of fatal
 *  failure).
 */
void
boot_donot_shutdown_client_at_exit (void)
{
  if (BOOT_IS_CLIENT_RESTARTED () && boot_Process_id == getpid ())
    {
      boot_Process_id++;
    }
}
#endif

/*
 * boot_server_die_or_changed: shutdown client when the server is dead
 *
 * return : nothing
 *
 * Note: The server has been terminated for circumstances beyond the client
 *       control. All active client transactions have been unilaterally
 *       aborted as a consequence of the termination of server.
 */
void
boot_server_die_or_changed (void)
{
  /*
   * If the client is restarted, abort the active transaction in the client and
   * terminate the client modules
   */
  if (BOOT_IS_CLIENT_RESTARTED ())
    {
      (void) tran_abort_only_client (true);
      boot_client (NULL_TRAN_INDEX, TM_TRAN_WAIT_MSECS ());
      boot_Is_client_all_final = false;
#if defined(CS_MODE)
      css_terminate (true);
#endif /* !CS_MODE */
      er_log_debug (ARG_FILE_LINE,
		    "boot_server_die_or_changed() terminated\n");
    }
}

/*
 * boot_client_all_finalize () - terminate every single client
 *
 * return : nothing
 *
 * Note: Terminate every single module of the client. This function is called
 *       during the shutdown of the client.
 */
void
boot_client_all_finalize (void)
{
  if (BOOT_IS_CLIENT_RESTARTED () || boot_Is_client_all_final == false)
    {
#if defined(CS_MODE)
      if (boot_Server_credential.alloc_buffer)
	{
	  free_and_init (boot_Server_credential.alloc_buffer);
	}
#endif
      tran_free_savepoint_list ();
      set_final ();
      parser_final ();
      au_final ();
      sm_final ();
      ws_final ();
      tp_final ();

      locator_free_areas ();
#if 0
      sysprm_final ();
#endif

#if defined(ENABLE_UNUSED_FUNCTION)
      msgcat_final ();
#endif
      er_stack_clearall ();
      er_clear ();
#if 0
      lang_final ();
#endif

#if defined (ENABLE_UNUSED_FUNCTION)
      /* adj_arrays & lex buffers in the cnv formatting library. */
      cnv_cleanup ();
#endif

      memset (&boot_Server_credential, 0, sizeof (boot_Server_credential));
      memset (boot_Server_credential.server_session_key, 0xFF,
	      SERVER_SESSION_KEY_SIZE);

      boot_client (NULL_TRAN_INDEX, TRAN_LOCK_INFINITE_WAIT);
      boot_Is_client_all_final = true;
    }

}

#if defined(CS_MODE)
/*
 * boot_cli_init_css () - Attempts to connect to hosts
 *                                          in list
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   db(in) : host information
 *   opt_cap(in): optional capability
 *   discriminative(in): deprecated
 *
 * Note: This function will try an initialize the communications with the hosts
 *       until success or the end of list is reached.
 */
static int
boot_cli_init_css (const char *dbname, const PRM_NODE_LIST * arg_node_list,
		   int client_type, bool check_capabilities, int opt_cap,
		   UNUSED_ARG bool discriminative, bool is_preferred_host)
{
  int error = ER_NET_NO_SERVER_HOST;
  int n;
  char node_list_str[MAX_NODE_INFO_STR_LEN * PRM_MAX_HA_NODE_LIST];
  bool cap_error = false, boot_host_connected_exist = false;
  int max_num_delayed_hosts_lookup;
  PRM_NODE_LIST node_list;

  if (arg_node_list == NULL)
    {
      assert (0);
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
      return error;
    }

  assert (arg_node_list->num_nodes > 0);

  max_num_delayed_hosts_lookup = db_get_max_num_delayed_hosts_lookup ();
  if (is_preferred_host == false
      && max_num_delayed_hosts_lookup == 0
      && (opt_cap & BOOT_CHECK_HA_DELAY_CAP))
    {
      /* if max_num_delayed_hosts_lookup is zero, move on to 2nd try */
      return ER_NET_SERVER_HAND_SHAKE;
    }

  /* try the connected host first */
  if (boot_Host_connected.ip == INADDR_NONE)
    {
      node_list = *arg_node_list;
    }
  else
    {
      int cpsize;
      boot_host_connected_exist = true;
      node_list.nodes[0] = boot_Host_connected;
      cpsize = MIN (arg_node_list->num_nodes, PRM_MAX_HA_NODE_LIST - 1);
      node_list.num_nodes = cpsize + 1;
      memcpy (&node_list.nodes[1], arg_node_list->nodes,
	      cpsize * sizeof (arg_node_list->nodes[0]));
    }

  db_clear_delayed_hosts_count ();

  for (n = 0; n < node_list.num_nodes; n++)
    {
      char host_str[MAX_NODE_INFO_STR_LEN];

      if (css_check_server_alive_fn != NULL)
	{
	  if (css_check_server_alive_fn (dbname,
					 &node_list.nodes[n]) == false)
	    {
	      continue;
	    }
	}

      prm_node_info_to_str (host_str, sizeof (host_str), &node_list.nodes[n]);

      error = net_client_init (dbname, &node_list.nodes[n]);
      if (error == NO_ERROR)
	{
	  RYE_VERSION server_version;

	  boot_Host_connected = node_list.nodes[n];

	  /* ping to validate availability and to check compatibility */
	  er_clear ();
	  error = net_client_ping_server_with_handshake (client_type,
							 check_capabilities,
							 opt_cap,
							 &server_version);
	  if (error == NO_ERROR)
	    {
	      boot_Peer_version = server_version;
	    }
	  else
	    {
	      css_terminate (false);
	    }
	}

      /* connect error to the db at the host */
      switch (error)
	{
	case NO_ERROR:
	  return NO_ERROR;

	case ER_NET_SERVER_HAND_SHAKE:
	case ER_NET_HS_UNKNOWN_SERVER_REL:
	  cap_error = true;
	case ER_NET_DIFFERENT_RELEASE:
	case ER_NET_NO_SERVER_HOST:
	case ER_NET_CANT_CONNECT_SERVER:
	case ER_NET_NO_MASTER:
	case ERR_CSS_TCP_CANNOT_CONNECT_TO_MASTER:
	case ERR_CSS_TCP_CONNECT_TIMEDOUT:
	case ERR_CSS_ERROR_FROM_SERVER:
	case ER_CSS_CLIENTS_EXCEEDED:
	  {
	    er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		    ER_BO_CONNECT_FAILED, 2, dbname, host_str);
	  }
	  break;
	default:
	  /* ?? */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_BO_CONNECT_FAILED,
		  2, dbname, host_str);
	}

      if (error == ER_NET_SERVER_HAND_SHAKE
	  && is_preferred_host == false
	  && (opt_cap & BOOT_CHECK_HA_DELAY_CAP)
	  && max_num_delayed_hosts_lookup > 0)
	{
	  /* do not count delayed boot_Host_connected */
	  if (boot_host_connected_exist == true && n == 0)
	    {
	      db_clear_delayed_hosts_count ();
	    }

	  if (db_get_delayed_hosts_count () >= max_num_delayed_hosts_lookup)
	    {
	      break;
	    }
	}
    }				/* for (tn) */

  /* failed to connect all hosts; write an error message */
  node_list_str[0] = '\0';
  for (n = 0; n < node_list.num_nodes; n++)
    {
      char host_str[MAX_NODE_INFO_STR_LEN];
      prm_node_info_to_str (host_str, sizeof (host_str), &node_list.nodes[n]);
      if (n != 0)
	{
	  strcat (node_list_str, ":");
	}
      strncat (node_list_str, host_str, MAX_NODE_INFO_STR_LEN);
    }
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_BO_CONNECT_FAILED, 2,
	  dbname, node_list_str);

  if (check_capabilities == true && cap_error == true)
    {
      /*
       * There'a a live host which has cause handshake error,
       * so adjust the return value
       */
      error = ER_NET_SERVER_HAND_SHAKE;
    }

  return (error);
}
#endif /* CS_MODE */

/*
 * boot_define_class :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   class(IN) :
 */
static int
boot_define_class (MOP class_mop)
{
  SM_TEMPLATE *def;
  char domain_string[32];
  int error_code = NO_ERROR;
  const char *index_col_names[2] = { "table_name", NULL };

  def = smt_edit_class_mop_with_lock (class_mop, X_LOCK);
  if (def == NULL)
    {
      return er_errid ();
    }

  error_code = smt_add_attribute (def, "table_of", "object", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "table_name", "varchar(255)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "table_type", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "is_system_table", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "owner", AU_USER_CLASS_NAME, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "owner_name", "varchar(255)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "num_col", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "collation_id", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  sprintf (domain_string, "sequence of %s", CT_COLUMN_NAME);

  error_code = smt_add_attribute (def, "cols", domain_string, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  sprintf (domain_string, "sequence of %s", CT_QUERYSPEC_NAME);

  error_code = smt_add_attribute (def, "query_specs", domain_string, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  sprintf (domain_string, "sequence of %s", CT_INDEX_NAME);

  error_code = smt_add_attribute (def, "indexes", domain_string, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_finish_class (def, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  /* add index */
  error_code = db_add_constraint (class_mop, DB_CONSTRAINT_INDEX, NULL,
				  index_col_names);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  if (locator_has_heap (class_mop) == NULL)
    {
      return er_errid ();
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_attribute :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   class(IN) :
 */
static int
boot_define_attribute (MOP class_mop)
{
  SM_TEMPLATE *def;
  char domain_string[32];
  int error_code = NO_ERROR;
  const char *index_col_names[3] = { "table_name", "col_name", NULL };

  def = smt_edit_class_mop_with_lock (class_mop, X_LOCK);
  if (def == NULL)
    {
      return er_errid ();
    }

  error_code = smt_add_attribute (def, "table_of", CT_TABLE_NAME, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "table_name", "varchar(255)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "col_name", "varchar(255)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "def_order", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "data_type", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "default_value", "varchar(255)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  sprintf (domain_string, "sequence of %s", CT_DOMAIN_NAME);

  error_code = smt_add_attribute (def, "domains", domain_string, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "is_nullable", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "is_shard_key", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_finish_class (def, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  /* add index */
  error_code = db_add_constraint (class_mop, DB_CONSTRAINT_INDEX, NULL,
				  index_col_names);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  if (locator_has_heap (class_mop) == NULL)
    {
      return er_errid ();
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_domain :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   class(IN) :
 *
 * Note:
 *
 */
static int
boot_define_domain (MOP class_mop)
{
  SM_TEMPLATE *def;
  char domain_string[32];
  int error_code = NO_ERROR;
  const char *index_col_names[3] = { "table_name", "col_name", NULL };

  def = smt_edit_class_mop_with_lock (class_mop, X_LOCK);
  if (def == NULL)
    {
      return er_errid ();
    }

  error_code = smt_add_attribute (def, "object_of", "object", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "table_name", "varchar(255)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "col_name", "varchar(255)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "data_type", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "prec", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "scale", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "table_of", CT_TABLE_NAME, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code =
    smt_add_attribute (def, "domain_table_name", "varchar(255)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "code_set", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "collation_id", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  sprintf (domain_string, "sequence of %s", CT_DOMAIN_NAME);

  error_code = smt_add_attribute (def, "set_domains", domain_string, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_finish_class (def, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  /* add index */
  error_code = db_add_constraint (class_mop, DB_CONSTRAINT_INDEX, NULL,
				  index_col_names);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  if (locator_has_heap (class_mop) == NULL)
    {
      return er_errid ();
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_query_spec :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   class(IN) :
 */
static int
boot_define_query_spec (MOP class_mop)
{
  SM_TEMPLATE *def;
  int error_code = NO_ERROR;
  const char *index_col_names[2] = { "table_name", NULL };

  def = smt_edit_class_mop_with_lock (class_mop, X_LOCK);
  if (def == NULL)
    {
      return er_errid ();
    }

  error_code = smt_add_attribute (def, "table_of", CT_TABLE_NAME, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "table_name", "varchar(256)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "spec", "varchar(4096)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_finish_class (def, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  /* add index */
  error_code = db_add_constraint (class_mop, DB_CONSTRAINT_INDEX, NULL,
				  index_col_names);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  if (locator_has_heap (class_mop) == NULL)
    {
      return er_errid ();
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_index :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   class(IN) :
 */
static int
boot_define_index (MOP class_mop)
{
  SM_TEMPLATE *def;
  char domain_string[32];
  int error_code = NO_ERROR;
  const char *index_col_names[3] = { "table_name", "index_name", NULL };

  def = smt_edit_class_mop_with_lock (class_mop, X_LOCK);
  if (def == NULL)
    {
      return er_errid ();
    }

  error_code = smt_add_attribute (def, "table_of", CT_TABLE_NAME, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "table_name", "varchar(255)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "index_name", "varchar(255)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "is_unique", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "key_count", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  sprintf (domain_string, "sequence of %s", CT_INDEXKEY_NAME);

  error_code = smt_add_attribute (def, "key_cols", domain_string, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "is_primary_key", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "status", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_finish_class (def, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  /* add index */
  error_code = db_add_constraint (class_mop, DB_CONSTRAINT_INDEX, NULL,
				  index_col_names);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  if (locator_has_heap (class_mop) == NULL)
    {
      return er_errid ();
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_index_key :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   class(IN) :
 */
static int
boot_define_index_key (MOP class_mop)
{
  int error_code = NO_ERROR;
  SM_TEMPLATE *def;
  const char *index_col_names[3] = { "table_name", "index_name", NULL };

  def = smt_edit_class_mop_with_lock (class_mop, X_LOCK);
  if (def == NULL)
    {
      return er_errid ();
    }

  error_code = smt_add_attribute (def, "index_of", CT_INDEX_NAME, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "table_name", "varchar(255)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "index_name", "varchar(255)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "key_col_name", "varchar(255)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "key_order", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "asc_desc", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_finish_class (def, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  /* add index */
  error_code = db_add_constraint (class_mop, DB_CONSTRAINT_INDEX, NULL,
				  index_col_names);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  if (locator_has_heap (class_mop) == NULL)
    {
      return error_code;
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_add_data_type :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   class(IN) :
 *
 * Note:
 *
 */
static int
boot_add_data_type (MOP class_mop)
{
  DB_OBJECT *obj;
  DB_VALUE val;
  int i;

  const char *names[DB_TYPE_LAST] = {
    "INTEGER" /* = 1 */ , NULL /* FLOAT */ , "DOUBLE", "VARCHAR", "OBJECT",
    NULL /* SET */ , NULL /* MULTISET */ , "SEQUENCE", NULL /* ELO */ ,
    "TIME", NULL /* TIMESTAMP */ , "DATE",
    NULL /* MONETARY */ , NULL /* VARIABLE */ , NULL /* SUB */ ,
    NULL /* POINTER */ , NULL /* ERROR */ , NULL /* SHORT */ ,
    NULL /* VOBJ */ ,
    NULL /* OID */ , NULL /* VALUE */ ,
    "NUMERIC", NULL /* BINARY */ , "VARBINARY",
    NULL /* CHAR */ , NULL /* NCHAR */ , NULL /* VARNCHAR */ ,
    NULL /* RESULTSET */ , NULL /* IDXKEY */ ,
    NULL /* TABLE */ ,
    "BIGINT", "DATETIME"
  };

  for (i = 0; i < DB_TYPE_LAST; i++)
    {
      if (names[i] != NULL)
	{
	  obj = db_create_internal (class_mop);
	  if (obj == NULL)
	    {
	      return er_errid ();
	    }

	  DB_MAKE_INTEGER (&val, i + 1);
	  db_put_internal (obj, "type_id", &val);

	  DB_MAKE_VARCHAR (&val, 9, (char *) names[i], strlen (names[i]),
			   LANG_SYS_COLLATION);
	  db_put_internal (obj, "type_name", &val);
	}
    }

  return NO_ERROR;
}

/*
 * boot_define_data_type :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   class(IN) :
 */
static int
boot_define_data_type (MOP class_mop)
{
  SM_TEMPLATE *def;
  int error_code = NO_ERROR;

  def = smt_edit_class_mop_with_lock (class_mop, X_LOCK);
  if (def == NULL)
    {
      return er_errid ();
    }

  error_code = smt_add_attribute (def, "type_id", "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, "type_name", "varchar(9)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_finish_class (def, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  if (locator_has_heap (class_mop) == NULL)
    {
      return er_errid ();
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = boot_add_data_type (class_mop);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_add_collations :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   class(IN) :
 *
 * Note:
 *
 */
int
boot_add_collations (MOP class_mop)
{
  int i;
//  int count_collations;
  int found_coll = 0;

//  count_collations = lang_collation_count ();

  for (i = 0; i < LANG_MAX_COLLATIONS; i++)
    {
      LANG_COLLATION *lang_coll = lang_get_collation (i);
      DB_OBJECT *obj;
      DB_VALUE val;

      assert (lang_coll != NULL);

      if (i > LANG_COLL_UTF8_EN_CI
	  && lang_coll->coll.coll_id == LANG_COLL_UTF8_EN_CI)
	{
	  continue;
	}
      found_coll++;

      obj = db_create_internal (class_mop);
      if (obj == NULL)
	{
	  return er_errid ();
	}

      assert (lang_coll->coll.coll_id == i);

      DB_MAKE_INTEGER (&val, i);
      db_put_internal (obj, CT_DBCOLL_COLL_ID_COLUMN, &val);

      DB_MAKE_VARCHAR (&val, 32, lang_coll->coll.coll_name,
		       strlen (lang_coll->coll.coll_name),
		       LANG_SYS_COLLATION);
      db_put_internal (obj, CT_DBCOLL_COLL_NAME_COLUMN, &val);

      DB_MAKE_INTEGER (&val, (int) (lang_coll->codeset));
      db_put_internal (obj, CT_DBCOLL_CHARSET_ID_COLUMN, &val);

      DB_MAKE_INTEGER (&val, lang_coll->built_in);
      db_put_internal (obj, CT_DBCOLL_BUILT_IN_COLUMN, &val);

      DB_MAKE_INTEGER (&val, lang_coll->coll.uca_opt.sett_expansions ? 1 : 0);
      db_put_internal (obj, CT_DBCOLL_EXPANSIONS_COLUMN, &val);

      DB_MAKE_INTEGER (&val, lang_coll->coll.count_contr);
      db_put_internal (obj, CT_DBCOLL_CONTRACTIONS_COLUMN, &val);

      DB_MAKE_INTEGER (&val, (int) (lang_coll->coll.uca_opt.sett_strength));
      db_put_internal (obj, CT_DBCOLL_UCA_STRENGTH, &val);

      assert (strlen (lang_coll->coll.checksum) == 32);
      DB_MAKE_VARCHAR (&val, 32, lang_coll->coll.checksum, 32,
		       LANG_SYS_COLLATION);
      db_put_internal (obj, CT_DBCOLL_CHECKSUM_COLUMN, &val);
    }

  assert (found_coll == lang_collation_count ());

  return NO_ERROR;
}

/*
 * boot_define_collations :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 *   class(IN) :
 */
static int
boot_define_collations (MOP class_mop)
{
  SM_TEMPLATE *def;
  int error_code = NO_ERROR;

  def = smt_edit_class_mop_with_lock (class_mop, X_LOCK);
  if (def == NULL)
    {
      return er_errid ();
    }

  error_code = smt_add_attribute (def, CT_DBCOLL_COLL_ID_COLUMN, "integer",
				  NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, CT_DBCOLL_COLL_NAME_COLUMN,
				  "varchar(32)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, CT_DBCOLL_CHARSET_ID_COLUMN, "integer",
				  NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, CT_DBCOLL_BUILT_IN_COLUMN, "integer",
				  NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, CT_DBCOLL_EXPANSIONS_COLUMN, "integer",
				  NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, CT_DBCOLL_CONTRACTIONS_COLUMN,
				  "integer", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, CT_DBCOLL_UCA_STRENGTH, "integer",
				  NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_add_attribute (def, CT_DBCOLL_CHECKSUM_COLUMN,
				  "varchar(32)", NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = smt_finish_class (def, NULL);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  if (locator_has_heap (class_mop) == NULL)
    {
      return er_errid ();
    }

  error_code = au_change_owner (class_mop, Au_dba_user);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  error_code = boot_add_collations (class_mop);
  if (error_code != NO_ERROR)
    {
      return error_code;
    }

  return NO_ERROR;
}

/*
 * boot_define_catalog_table () -
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 *
 */
static int
boot_define_catalog_table (void)
{
  CATCLS_TABLE *ct_list[] = {
    &table_IndexStats,
    &table_LogWriter,
    &table_LogAnalyzer,
    &table_LogApplier,
    &table_ShardGidSkey,
    &table_ShardGidRemoved
  };

  CATCLS_TABLE *table;
  SM_TEMPLATE *def;
  MOP class_mop;
  int i, j;
  int save;
  int num_tables;
  int error_code = NO_ERROR;

  num_tables = sizeof (ct_list) / sizeof (ct_list[0]);

  AU_DISABLE (save);

  for (i = 0; i < num_tables; i++)
    {
      table = ct_list[i];

      class_mop = db_create_class (table->name);
      if (class_mop == NULL)
	{
	  error_code = er_errid ();
	  goto end;
	}

      def = smt_edit_class_mop_with_lock (class_mop, X_LOCK);
      if (def == NULL)
	{
	  error_code = er_errid ();
	  goto end;
	}

      /* add columns */
      for (j = 0; j < table->num_columns; j++)
	{
	  error_code = smt_add_attribute (def, table->columns[j].name,
					  table->columns[j].type, NULL);
	  if (error_code != NO_ERROR)
	    {
	      goto end;
	    }
	}

      error_code = smt_finish_class (def, NULL);
      if (error_code != NO_ERROR)
	{
	  goto end;
	}

      /* add constraints */
      for (j = 0; j < table->num_constraints; j++)
	{
	  error_code = db_add_constraint (class_mop,
					  table->constraint[j].type,
					  table->constraint[j].name,
					  table->constraint[j].atts);
	  if (error_code != NO_ERROR)
	    {
	      goto end;
	    }
	}

      if (locator_has_heap (class_mop) == NULL)
	{
	  error_code = er_errid ();
	  goto end;
	}

      error_code = au_change_owner (class_mop, Au_dba_user);
      if (error_code != NO_ERROR)
	{
	  goto end;
	}
    }

end:
  AU_ENABLE (save);

  return error_code;
}

/*
 * catcls_class_install :
 *
 * returns : NO_ERROR if all OK, ER_ status otherwise
 */
static int
catcls_class_install (void)
{
  CATCLS_FUNCTION clist[] = {
    {CT_TABLE_NAME, (DEF_FUNCTION) boot_define_class},
    {CT_COLUMN_NAME, (DEF_FUNCTION) boot_define_attribute},
    {CT_DOMAIN_NAME, (DEF_FUNCTION) boot_define_domain},
    {CT_QUERYSPEC_NAME, (DEF_FUNCTION) boot_define_query_spec},
    {CT_INDEX_NAME, (DEF_FUNCTION) boot_define_index},
    {CT_INDEXKEY_NAME, (DEF_FUNCTION) boot_define_index_key},
    {CT_DATATYPE_NAME, (DEF_FUNCTION) boot_define_data_type},
    {CT_COLLATION_NAME, (DEF_FUNCTION) boot_define_collations}
  };

  MOP class_mop[sizeof (clist) / sizeof (clist[0])];
  int i, save;
  int error_code = NO_ERROR;
  int num_classes = sizeof (clist) / sizeof (clist[0]);

  AU_DISABLE (save);

  for (i = 0; i < num_classes; i++)
    {
      class_mop[i] = db_create_class (clist[i].name);
      if (class_mop[i] == NULL)
	{
	  error_code = er_errid ();
	  goto end;
	}
    }

  for (i = 0; i < num_classes; i++)
    {
      error_code = ((DEF_CLASS_FUNCTION) (clist[i].function)) (class_mop[i]);
      if (error_code != NO_ERROR)
	{
	  error_code = er_errid ();
	  goto end;
	}
    }

  error_code = boot_define_catalog_table ();

end:
  AU_ENABLE (save);

  return error_code;
}

PRM_NODE_INFO
boot_get_host_connected (void)
{
#if defined(CS_MODE)
  return boot_Host_connected;
#else
  return prm_get_myself_node_info ();
#endif
}

#if defined(CS_MODE)
int
boot_get_server_start_time (void)
{
  return boot_Server_credential.server_start_time;
}
#endif /* CS_MODE */

/*
 * boot_clear_host_connected () -
 */
void
boot_clear_host_connected (void)
{
#if defined(CS_MODE)
  memset (&boot_Host_connected, 0, sizeof (boot_Host_connected));
  boot_Host_connected.ip = INADDR_NONE;
#endif
}

char *
boot_get_host_name (void)
{
  if (boot_Host_name[0] == '\0')
    {
      if (GETHOSTNAME (boot_Host_name, MAXHOSTNAMELEN) != 0)
	{
	  strcpy (boot_Host_name, boot_Client_id_unknown_string);
	}
      boot_Host_name[MAXHOSTNAMELEN - 1] = '\0';	/* bullet proof */
    }

  return boot_Host_name;
}

#if defined(CS_MODE)
/*
 * boot_check_locales () - checks that client locales are compatible with
 *                         server locales
 *
 *  return : error code
 *
 */
static int
boot_check_locales (BOOT_CLIENT_CREDENTIAL * cli_cred)
{
  int error_code = NO_ERROR;
  LANG_COLL_COMPAT *server_collations = NULL;
  LANG_LOCALE_COMPAT *server_locales = NULL;
  int server_coll_cnt, server_locales_cnt;
  char cli_text[PATH_MAX];
  char srv_text[DB_MAX_IDENTIFIER_LENGTH + 10];

  error_code = boot_get_server_locales (&server_collations, &server_locales,
					&server_coll_cnt,
					&server_locales_cnt);
  if (error_code != NO_ERROR)
    {
      goto exit;
    }

  (void) basename_r (cli_cred->program_name, cli_text, sizeof (cli_text));
  snprintf (srv_text, sizeof (srv_text) - 1, "server '%s'",
	    cli_cred->db_name);

  error_code = lang_check_coll_compat (server_collations, server_coll_cnt,
				       cli_text, srv_text);
  if (error_code != NO_ERROR)
    {
      goto exit;
    }

  error_code = lang_check_locale_compat (server_locales, server_locales_cnt,
					 cli_text, srv_text);

exit:
  if (server_collations != NULL)
    {
      free_and_init (server_collations);
    }
  if (server_locales != NULL)
    {
      free_and_init (server_locales);
    }

  return error_code;
}
#endif /* CS_MODE */

/*
 * boot_get_server_session_key () -
 */
char *
boot_get_server_session_key (void)
{
  return boot_Server_credential.server_session_key;
}

/*
 * boot_set_server_session_key () -
 */
void
boot_set_server_session_key (const char *key)
{
  memcpy (boot_Server_credential.server_session_key, key,
	  SERVER_SESSION_KEY_SIZE);
}
