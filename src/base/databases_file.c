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
 * databases_file.c - Parsing the database directory file
 *
 */

#ident "$Id$"

#include "config.h"

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <sys/types.h>
#include <sys/stat.h>

#include <unistd.h>
#include <assert.h>
#include <sys/time.h>

#include "porting.h"

#include "chartype.h"
#include "error_manager.h"
#include "databases_file.h"
#include "boot.h"
#include "memory_alloc.h"
#include "environment_variable.h"
#include "system_parameter.h"
#include "file_io.h"
#include "rye_master_shm.h"
#include "master_heartbeat.h"

/* conservative upper bound of a line in databases.txt */
#define CFG_MAX_LINE 4096

/*
 * DB_INFO
 *
 * Note: This is a descriptor structure for databases in the currently
 *    accessible directory file.
 */
typedef struct database_info DB_INFO;

struct database_info
{
  char *name;
  DB_INFO *next;
};

static char CFG_HOST_SEPARATOR = ':';

static int cfg_read_directory (int vdes, DB_INFO ** info_p);
static void cfg_write_directory (int vdes, const DB_INFO * databases);

static void cfg_free_directory (DB_INFO * databases);
static DB_INFO *cfg_new_db (const char *name);
static DB_INFO *cfg_find_db_list (DB_INFO * dir, const char *name);
static DB_INFO *cfg_add_db (DB_INFO ** dir, const char *name);
static bool cfg_delete_db (DB_INFO ** dir_info_p, const char *name);

static char *cfg_create_host_list (const char *primary_host_name,
				   bool append_local_host, int *cnt);


static char *cfg_next_char (char *str_p);
static char *cfg_next_line (char *str_p);
static char *cfg_pop_linetoken (char *str_p, char **token_p);
static int cfg_get_directory_filename (char *buffer, size_t size);

static const char *cfg_pop_host (const char *host_list, char *buffer,
				 int *length);
static bool cfg_host_exists (char *host_list, char *hostname, int num_items);

/* PARSING UTILITIES */
/*
 * cfg_next_char() - Advances the given pointer until a non-whitespace character
 *               or the end of the string are encountered.
 *    return: char *
 *    str_p(in): buffer pointer
 */
static char *
cfg_next_char (char *str_p)
{
  char *p;

  p = str_p;
  while (char_isspace ((int) *p) && *p != '\0')
    {
      p++;
    }

  return (p);
}

/*
 * cfg_next_line()
 *    return: char *
 *    str_p(in): buffer pointer
 */
static char *
cfg_next_line (char *str_p)
{
  char *p;

  p = str_p;
  while (!char_iseol ((int) *p) && *p != '\0')
    {
      p++;
    }
  while (char_iseol ((int) *p) && *p != '\0')
    {
      p++;
    }

  return (p);
}

/*
 * cfg_pop_linetoken()
 *    return: char *
 *    str_p(in):
 *    token_p(in/out):
 */
static char *
cfg_pop_linetoken (char *str_p, char **token_p)
{
  char *p, *end, *token = NULL;
  int length;

  if (str_p == NULL || char_iseol ((int) *str_p))
    {
      *token_p = NULL;
      return str_p;
    }
  token = NULL;
  p = str_p;
  while (char_isspace ((int) *p) && !char_iseol ((int) *p) && *p != '\0')
    {
      p++;
    }
  end = p;

  while (!char_isspace ((int) *end) && *end != '\0')
    {
      end++;
    }

  length = (int) (end - p);
  if (length > 0)
    {
      token = (char *) malloc (length + 1);
      if (token != NULL)
	{
	  strncpy (token, p, length);
	  token[length] = '\0';
	}
    }

  *token_p = token;
  return (end);
}

/*
 * cfg_get_directory_filename() - Finds the full pathname of the database
 *                                directory file.
 *    return: non-zero iff success
 *    buffer(in): character buffer to hold the full path name
 *    sizes(in): buffer size
 */
static int
cfg_get_directory_filename (char *buffer, size_t size)
{
  int status;

  status = 0;

  if (envvar_confdir_file (buffer, size, DATABASES_FILENAME))
    {
      status = 1;		/* success */
    }

  return status;
}

/*
 * cfg_maycreate_get_directory_filename()
 *    return: char *
 *    buffer(in):
 *    size(in): buffer size
 */
char *
cfg_maycreate_get_directory_filename (char *buffer, size_t size)
{
  FILE *file_p = NULL;

  if (cfg_get_directory_filename (buffer, size))
    {
      file_p = fopen (buffer, "a+");
    }

  if (file_p == NULL)
    {
#if !defined(CS_MODE)
      er_set_with_oserror (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			   ER_CFG_NO_WRITE_ACCESS, 1, buffer);
#else /* !CS_MODE */
      er_set_with_oserror (ER_WARNING_SEVERITY, ARG_FILE_LINE,
			   ER_CFG_NO_WRITE_ACCESS, 1, buffer);
#endif /* !CS_MODE */
      return NULL;
    }
  fclose (file_p);
  return buffer;
}

/*
 * cfg_read_directory() - This reads the database directory file and returns
 *                           a list of descriptors
 *    return: error code
 *    vdes(in): file descriptor
 *    info_p(out): pointer to returned list of descriptors
 *
 *    Note: However it does not open/close the file, the file lock is
 *          preserved.
 */
static int
cfg_read_directory (int vdes, DB_INFO ** info_p)
{
  char *line = NULL;
  DB_INFO *databases, *last, *db;
  char *str = NULL;
  struct stat stat_buffer;
  int error_code = ER_FAILED;

  assert (vdes != NULL_VOLDES);

  databases = last = NULL;

  if (lseek (vdes, 0L, SEEK_SET) == 0L)
    {
      fstat (vdes, &stat_buffer);
      line = (char *) malloc (stat_buffer.st_size + 1);
      if (line == NULL)
	{
	  *info_p = NULL;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, stat_buffer.st_size + 1);
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}
      read (vdes, line, stat_buffer.st_size);
      line[stat_buffer.st_size] = '\0';
      str = cfg_next_char (line);
      while (*str != '\0')
	{
	  if (*str != '#')
	    {
	      if ((db = (DB_INFO *) malloc (sizeof (DB_INFO))) == NULL)
		{
		  if (databases != NULL)
		    {
		      cfg_free_directory (databases);
		    }
		  *info_p = NULL;
		  free_and_init (line);

		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (DB_INFO));
		  return ER_OUT_OF_VIRTUAL_MEMORY;
		}

	      db->next = NULL;
	      str = cfg_pop_linetoken (str, &db->name);

	      if (databases == NULL)
		{
		  databases = db;
		}
	      else
		{
		  last->next = db;
		}
	      last = db;
	      if (db->name == NULL)

		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_CFG_INVALID_DATABASES, 1, DATABASES_FILENAME);
		  if (databases != NULL)
		    {
		      cfg_free_directory (databases);
		    }
		  *info_p = NULL;
		  free_and_init (line);
		  return ER_CFG_INVALID_DATABASES;
		}
	    }
	  str = cfg_next_line (str);
	  str = cfg_next_char (str);
	}
      error_code = NO_ERROR;
      free_and_init (line);
    }
  *info_p = databases;
  return (error_code);
}

/*
 * cfg_write_directory() - This writes a list of database descriptors
 *                            to the accessible config file.
 *                            only the first host, (primary host),
 *                            is written to file.
 *    return: none
 *    vdes(in): file descriptor
 *    databases(in): list of database descriptors
 *
 * Note : However, cfg_write_directory() has a potential problem that
 *        the file lock is released. The file lock acquired through io_mount()
 *        is released when cfg_write_directory() opens and closes the file.
 */
static void
cfg_write_directory (int vdes, const DB_INFO * databases)
{
  char line[LINE_MAX], *s;
  const DB_INFO *db_info_p;
  int n;
  sigset_t new_mask, old_mask;

  sigfillset (&new_mask);
  sigdelset (&new_mask, SIGINT);
  sigdelset (&new_mask, SIGQUIT);
  sigdelset (&new_mask, SIGTERM);
  sigdelset (&new_mask, SIGHUP);
  sigdelset (&new_mask, SIGABRT);
  sigprocmask (SIG_SETMASK, &new_mask, &old_mask);

  lseek (vdes, 0L, SEEK_SET);
  n = sprintf (line, "#db-name\n");
  write (vdes, line, n);
  for (db_info_p = databases; db_info_p != NULL; db_info_p = db_info_p->next)
    {
      s = line;
      s += sprintf (s, "%s\n", db_info_p->name);

      n = (int) (s - line);
      write (vdes, line, n);
    }

  ftruncate (vdes, lseek (vdes, 0L, SEEK_CUR));

  sigprocmask (SIG_SETMASK, &old_mask, NULL);
}

/*
 * cfg_free_directory() - Frees a list of database descriptors.
 *    return: none
 *    databases(in): list of databases
 */
static void
cfg_free_directory (DB_INFO * databases)
{
  DB_INFO *db_info_p, *next_info_p;

  for (db_info_p = databases, next_info_p = NULL; db_info_p != NULL;
       db_info_p = next_info_p)
    {

      next_info_p = db_info_p->next;

      if (db_info_p->name != NULL)
	{
	  free_and_init (db_info_p->name);
	}

      free_and_init (db_info_p);
    }
}

/*
 * cfg_new_db() - creates a new DB_INFO structure. If the hosts array sent
 *                in is NULL, an array with the local host as primary host
 *                is created.
 *    return: new database descriptor
 *    name(in): database name
 */
static DB_INFO *
cfg_new_db (const char *name)
{
  DB_INFO *db_info_p;
  int error = NO_ERROR;

  db_info_p = (DB_INFO *) malloc (DB_SIZEOF (DB_INFO));
  if (db_info_p == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, DB_SIZEOF (DB_INFO));
      GOTO_EXIT_ON_ERROR;
    }

  db_info_p->name = strdup (name);
  if (db_info_p->name == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, strlen (name));
      GOTO_EXIT_ON_ERROR;
    }

  db_info_p->next = NULL;

  assert (error == NO_ERROR);
  return (db_info_p);

exit_on_error:
  assert (error != NO_ERROR);

  if (db_info_p != NULL)
    {
      if (db_info_p->name != NULL)
	{
	  free_and_init (db_info_p->name);
	}

      free_and_init (db_info_p);
    }

  return NULL;
}

/*
 * cfg_find_db_list()
 *    return: database descriptor
 *    dir(in): descriptor list
 *    name(in): database name
 */
static DB_INFO *
cfg_find_db_list (DB_INFO * db_info_list_p, const char *name)
{
  DB_INFO *db_info_p, *found_info_p;

  found_info_p = NULL;
  for (db_info_p = db_info_list_p; db_info_p != NULL && found_info_p == NULL;
       db_info_p = db_info_p->next)
    {
      if (strcmp (db_info_p->name, name) == 0)
	{
	  found_info_p = db_info_p;
	}
    }

  return (found_info_p);
}

/*
 * cfg_add_db() - Creates a new hosts array and DB_INFO structure and pops
 *                the structure into the dir linked-list.
 *    return: new database descriptor
 *    dir(in/out): pointer to directory list
 *    name(in): database name
 */
static DB_INFO *
cfg_add_db (DB_INFO ** dir, const char *name)
{
  DB_INFO *db_info_p;

  db_info_p = cfg_new_db (name);

  if (db_info_p != NULL)
    {
      db_info_p->next = *dir;
      *dir = db_info_p;
    }

  return (db_info_p);
}

/*
 * cfg_delete_db() - Deletes a database entry from a directory list.
 *    return: if success is return true, otherwise return false
 *    dir_info_p(in): pointer to directory list
 *    name(in): database name
 */
static bool
cfg_delete_db (DB_INFO ** dir_info_p, const char *name)
{
  DB_INFO *db_info_p, *prev_info_p, *found_info_p;
  int success = false;

  for (db_info_p = *dir_info_p, found_info_p = NULL, prev_info_p = NULL;
       db_info_p != NULL && found_info_p == NULL; db_info_p = db_info_p->next)
    {
      if (strcmp (db_info_p->name, name) == 0)
	{
	  found_info_p = db_info_p;
	}
      else
	{
	  prev_info_p = db_info_p;
	}
    }
  if (found_info_p != NULL)
    {
      if (prev_info_p == NULL)
	{
	  *dir_info_p = found_info_p->next;
	}
      else
	{
	  prev_info_p->next = found_info_p->next;
	}
      found_info_p->next = NULL;
      cfg_free_directory (found_info_p);
      success = true;
    }
  return (success);
}

/*
 * cfg_get_hosts_from_prm() -
 */
char **
cfg_get_hosts_from_prm (int *count)
{
  const char *ha_node_list;
  const char *str;

  ha_node_list = prm_get_string_value (PRM_ID_HA_NODE_LIST);
  if (ha_node_list != NULL)
    {
      str = strchr (ha_node_list, '@');
      ha_node_list = (str) ? str + 1 : NULL;
    }

  return cfg_get_hosts (ha_node_list, count, false);
}

struct shm_ha_node_info
{
  RYE_SHM_HA_NODE *shm_ha_node;
  int con_order;
  int priority;
};

static int
ha_node_info_cmp (const void *data1, const void *data2)
{
  const struct shm_ha_node_info *node1 = data1;
  const struct shm_ha_node_info *node2 = data2;

  if (node1->con_order != node2->con_order)
    {
      return (node1->con_order - node2->con_order);
    }

  if (node1->shm_ha_node->is_localhost)
    {
      return -1;
    }
  if (node2->shm_ha_node->is_localhost)
    {
      return 1;
    }

  return (node1->priority - node2->priority);
}

/*
 * cfg_get_hosts_from_shm() -
 */
char **
cfg_get_hosts_from_shm (int *count, BOOT_CLIENT_TYPE client_type,
			bool connect_order_random)
{
  int num_nodes;
  struct shm_ha_node_info ha_nodes_info[SHM_MAX_HA_NODE_LIST];
  RYE_SHM_HA_NODE nodes[SHM_MAX_HA_NODE_LIST];

  int con_order_master;
  int con_order_slave;
  int con_order_replica;
  int con_order_unknown;

  struct drand48_data buf;
  struct timeval t;
  double r;
  int i;

  /* set connection order */
  if (client_type == BOOT_CLIENT_READ_ONLY_BROKER ||
      client_type == BOOT_CLIENT_SLAVE_ONLY_BROKER)
    {
      con_order_slave = 1;
      con_order_replica = 2;
      con_order_master = 3;
      con_order_unknown = 4;
    }
  else if (client_type == BOOT_CLIENT_RW_BROKER_REPLICA_ONLY ||
	   client_type == BOOT_CLIENT_RO_BROKER_REPLICA_ONLY ||
	   client_type == BOOT_CLIENT_SO_BROKER_REPLICA_ONLY)
    {
      con_order_replica = 1;
      con_order_slave = 2;
      con_order_master = 3;
      con_order_unknown = 4;
    }
  else
    {
      con_order_master = 1;
      con_order_slave = 2;
      con_order_replica = 3;
      con_order_unknown = 4;
    }

  *count = 0;

  if (rye_master_shm_get_ha_nodes (nodes, &num_nodes,
				   SHM_MAX_HA_NODE_LIST) != NO_ERROR)
    {
      return NULL;
    }
  if (num_nodes == 0)
    {
      return NULL;
    }

  assert (num_nodes > 0);

  gettimeofday (&t, NULL);
  srand48_r (t.tv_usec, &buf);

  for (i = 0; i < num_nodes; i++)
    {
      if (nodes[i].is_localhost)
	{
	  strcpy (nodes[i].host_name, "localhost");
	}

      ha_nodes_info[i].shm_ha_node = &nodes[i];

      /* set con order */
      switch (nodes[i].node_state)
	{
	case HA_STATE_SLAVE:
	case HA_STATE_TO_BE_SLAVE:
	  ha_nodes_info[i].con_order = con_order_slave;
	  break;
	case HA_STATE_TO_BE_MASTER:
	case HA_STATE_MASTER:
	  ha_nodes_info[i].con_order = con_order_master;
	  break;
	case HA_STATE_REPLICA:
	  ha_nodes_info[i].con_order = con_order_replica;
	  break;
	case HA_STATE_UNKNOWN:
	default:
	  ha_nodes_info[i].con_order = con_order_unknown;
	  break;
	}

      /* set priority */
      if (connect_order_random)
	{
	  drand48_r (&buf, &r);
	  ha_nodes_info[i].priority = (int) (r * 0x0fffffff);
	}
      else
	{
	  ha_nodes_info[i].priority = nodes[i].priority;
	}
    }

  qsort (ha_nodes_info, num_nodes, sizeof (struct shm_ha_node_info),
	 ha_node_info_cmp);


  /* return same memory structure with cfg_get_hosts */
  do
    {
      char *hosts_data;
      char **host_array;
      int hosts_data_len;
      int i;

      hosts_data_len = 0;
      for (i = 0; i < num_nodes; i++)
	{
	  hosts_data_len +=
	    (strlen (ha_nodes_info[i].shm_ha_node->host_name) + 1);
	}

      if (hosts_data_len == 0)
	{
	  assert (0);
	  return NULL;
	}

      hosts_data = malloc (hosts_data_len + 1);
      host_array = malloc (sizeof (char *) * num_nodes);
      if (hosts_data == NULL || host_array == NULL)
	{
	  free_and_init (hosts_data);
	  free_and_init (host_array);
	  return NULL;
	}

      for (i = 0; i < num_nodes; i++)
	{
	  host_array[i] = hosts_data;
	  strcpy (hosts_data, ha_nodes_info[i].shm_ha_node->host_name);
	  hosts_data +=
	    (strlen (ha_nodes_info[i].shm_ha_node->host_name) + 1);
	}

      *count = num_nodes;
      return host_array;
    }
  while (0);
}

/*
 * cfg_get_hosts() - assigns value count, to the number of hosts in array.
 *                   cfg_free_hosts should be called to free up memory used
 *                   by the array
 *    return: pointer to an array containing the host list.
 *    prim_host(in): primary hostname for database.
 *    count(in): count will contain the number of host found after processing
 *    include_local_host(in): boolean indicating if the local host name should
 *                            be prepended to the list.
 */
char **
cfg_get_hosts (const char *prim_host, int *count, bool include_local_host)
{
  /* pointers to array of hosts, to return */
  char **host_array;
  char *hosts_data;
  int i;

  *count = 0;

  /*
   * get a clean host list, i.e., null fields and duplicate hosts removed.
   * prim_host will be prepended to the list, and the local host will
   * will be appended if include_local_host is true.
   */
  hosts_data = cfg_create_host_list (prim_host, include_local_host, count);
  if (*count == 0 || hosts_data == NULL)
    {
      return NULL;
    }

  /* create a list of pointers to point to the hosts in hosts_data */
  host_array = (char **) calloc (*count + 1, sizeof (char **));
  if (host_array == NULL)
    {
      free_and_init (hosts_data);
      return NULL;
    }
  for (i = 0; i < *count; i++)
    {
      host_array[i] = hosts_data;
      hosts_data = strchr (hosts_data, CFG_HOST_SEPARATOR);
      if (hosts_data == NULL)
	{
	  break;
	}

      *hosts_data++ = '\0';
    }

  return host_array;
}

/*
 * cfg_free_hosts() - free_and_init's host_array and *host_array if not NULL
 *    return: none
 *    host_array(in): array of pointers to buffer containing hostnames.
 */
void
cfg_free_hosts (char **host_array)
{
  if (host_array != NULL)
    {
      if (*host_array != NULL)
	{
	  free_and_init (*host_array);
	}
      free_and_init (host_array);
    }
}

/*
 * cfg_pop_host() - pointer to next character in string
 *    return: returns pointer to next character in string
 *    host_list(in): String containing list of hosts
 *    buffer(in): Buffer to pop in hostname.
 *    length(out): Returns the length of the hostname, popped.
 *             -1 indicates that the hostname was too long > MAXHOSTLEN,
 *             and buffer is empty
 *
 *    Note : Sending in a NULL buffer will mean the function will assign length
 *           to the length of the next host in the list only.
 */
static const char *
cfg_pop_host (const char *host_list, char *buffer, int *length)
{
  int current_host_length = 0;
  const char *start, *host;

  host = host_list;

  if (buffer != NULL)
    {
      *buffer = '\0';
    }

  /* Ignore initial spaces/field separators in list */

  while (((char_isspace (*host))
	  || (*host == CFG_HOST_SEPARATOR)) && (*host != '\0'))
    {
      ++host;
    }

  /* Read in next host, and make a note of its length */

  start = host;
  current_host_length = 0;

  while ((*host != CFG_HOST_SEPARATOR)
	 && (!char_isspace (*host)) && (*host != '\0'))
    {
      host++;
      current_host_length++;
    }

  /*
   * Increment count if we have a valid hostname, and we have reached,
   * a field separator, a space or end of line.
   * Copy host into buffer supplied.
   */
  if (((*host == CFG_HOST_SEPARATOR) || (char_isspace (*host))
       || (*host == '\0')) && (current_host_length != 0))
    {
      /* Note buffer is empty if length of host is greater than MAXHOSTNAMELEN) */
      if ((buffer != NULL) && (current_host_length <= MAXHOSTNAMELEN))
	{
	  strncpy (buffer, start, current_host_length);
	  *(buffer + current_host_length) = '\0';
	}
    }

  if (current_host_length >= MAXHOSTNAMELEN)
    {
      *length = (-1);
    }
  else
    {
      *length = current_host_length;
    }
  return (host);
}

/*
 * cfg_host_exists() - Traverses the host_list to locate hostname.
 *    return: true if item exists.
 *    host_list(in): Pointer to array holding host names
 *    hostname(in): host name to search for.
 *    num_items(in): The number of items currently in the list
 */
static bool
cfg_host_exists (char *host_list, char *hostname, int num_items)
{
  char *current_host;
  char *next_sep;
  int i = 0, len, hostname_len;

  hostname_len = strlen (hostname);

  current_host = host_list;
  while ((current_host != NULL) && (i < num_items))
    {
      next_sep = strchr (current_host, CFG_HOST_SEPARATOR);
      if (next_sep == NULL)
	{
	  if (strcmp (current_host, hostname) == 0)
	    {
	      return true;
	    }
	  else
	    {
	      return false;
	    }
	}
      else
	{
	  len = next_sep - current_host;

	  if (len == hostname_len
	      && strncmp (current_host, hostname, len) == 0)
	    {
	      return true;
	    }
	}

      i++;
      current_host = next_sep + 1;
    }
  return false;
}				/* cfg_host_exists() */

/*
 * cfg_create_host_lsit()
 *    return: returns a pointer to a copy of the array holding hostnames
 *    primary_host_name(in): String containing primary host name.
 *    include_local_host(in): Flag indicating if the local hostname should be
 *                       included in the list.
 *    count(out): Pointer to integer which will be assigned the number of hosts
 *             in list.
 *
 *    Note : Null or empty hostnames are ignored, and duplicates are not
 *           included in the list.
 */
static char *
cfg_create_host_list (const char *primary_host_name, bool include_local_host,
		      int *count)
{
  int host_list_length, host_length, host_count;
  const char *str_ptr;
  char *full_host_list, *host_ptr;
  char local_host[MAXHOSTNAMELEN + 1];

  assert (count != NULL);

  host_list_length = 0;
  /* include local host to list if required */
  *local_host = '\0';
  if (include_local_host)
    {
#if 0				/* use Unix-domain socket for localhost */
      if (GETHOSTNAME (local_host, MAXHOSTNAMELEN) == 0)
	{
	  local_host[MAXHOSTNAMELEN] = '\0';
	  host_list_length += strlen (local_host) + 1;
	}
#else
      strcpy (local_host, "localhost");
      host_list_length += strlen (local_host) + 1;
#endif
    }
  /* check the given primary hosts list */
  if (primary_host_name != NULL && *primary_host_name != '\0')
    {
      host_list_length += strlen (primary_host_name) + 1;
    }

  /*
   * concatenate host lists with separator
   * count the number of hosts in the list
   * ignore null and space
   * removing duplicates
   */
  if (host_list_length == 0)
    {
      return NULL;
    }
  full_host_list = (char *) malloc (host_list_length + 1);
  if (full_host_list == NULL)
    {
      return NULL;
    }
  host_count = 0;
  host_ptr = full_host_list;
  *host_ptr = '\0';
  /* add the given primary hosts to the list */
  if (primary_host_name != NULL && *primary_host_name != '\0')
    {
      str_ptr = primary_host_name;
      while (*str_ptr != '\0')
	{
	  str_ptr = cfg_pop_host (str_ptr, host_ptr, &host_length);
	  if (host_length > 0)
	    {
	      if (!cfg_host_exists (full_host_list, host_ptr, host_count))
		{
		  host_count++;
		  host_ptr += host_length;
		  *host_ptr++ = CFG_HOST_SEPARATOR;
		}
	      *host_ptr = '\0';
	    }
	}
    }

  /* append local host if exists */
  if (*local_host != '\0')
    {
      if (!cfg_host_exists (full_host_list, local_host, host_count))
	{
	  strcpy (host_ptr, local_host);
	  host_ptr += strlen (local_host);
	  host_count++;
	}
    }

  /* remove last separator */
  host_ptr--;
  if (*host_ptr == CFG_HOST_SEPARATOR)
    {
      *host_ptr = '\0';
    }

  /* return host list and counter */
  if (host_count != 0)
    {
      *count = host_count;
      return full_host_list;
    }

  /* no valid host name */
  free_and_init (full_host_list);
  return NULL;
}

/*
 * cfg_database_exists()
 */
int
cfg_database_exists (bool * exists, int vdes, const char *dbname)
{
  int error;
  DB_INFO *dir = NULL;

  assert (vdes != NULL_VOLDES);

  error = cfg_read_directory (vdes, &dir);
  if (error != NO_ERROR)
    {
      return error;
    }

  if (dir && cfg_find_db_list (dir, dbname) != NULL)
    {
      *exists = true;
    }
  else
    {
      *exists = false;
    }

  cfg_free_directory (dir);

  return NO_ERROR;
}

/*
 * cfg_database_add()
 */
int
cfg_database_add (int vdes, const char *dbname)
{
  int error;
  DB_INFO *dir = NULL;
  DB_INFO *db;

  assert (vdes != NULL_VOLDES);

  error = cfg_read_directory (vdes, &dir);
  if (error != NO_ERROR)
    {
      return error;
    }

  db = cfg_find_db_list (dir, dbname);
  if (db == NULL)
    {
      db = cfg_add_db (&dir, dbname);
      if (db != NULL)
	{
	  cfg_write_directory (vdes, dir);
	}
    }

  cfg_free_directory (dir);

  return NO_ERROR;
}

/*
 * cfg_database_delete()
 */
int
cfg_database_delete (int vdes, const char *dbname)
{
  int error;
  DB_INFO *dir = NULL;
  DB_INFO *db;

  assert (vdes != NULL_VOLDES);

  error = cfg_read_directory (vdes, &dir);
  if (error != NO_ERROR)
    {
      return error;
    }

  db = cfg_find_db_list (dir, dbname);
  if (db && cfg_delete_db (&dir, dbname))
    {
      cfg_write_directory (vdes, dir);
    }

  cfg_free_directory (dir);

  return NO_ERROR;
}
