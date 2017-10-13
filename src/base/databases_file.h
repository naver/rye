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
 * databases_file.h - Configuration file parser
 *
 */

#ifndef _DATABASES_FILE_H_
#define _DATABASES_FILE_H_

#ident "$Id$"

#include "boot.h"

/* Name of the environment variable pointing to database file */
#define DATABASES_ENVNAME "DATABASES"

/* name of the database file */
#define DATABASES_FILENAME "databases.txt"

extern char *cfg_maycreate_get_directory_filename (char *buffer, size_t size);
extern char **cfg_get_hosts (const char *prim_host, int *count,
			     bool include_local_host);
extern char **cfg_get_hosts_from_prm (int *count);
extern char **cfg_get_hosts_from_shm (int *count,
				      BOOT_CLIENT_TYPE client_type,
				      bool connect_order_random);

extern void cfg_free_hosts (char **host_array);
extern int cfg_database_exists (bool * exists, int vdes, const char *dbname);
extern int cfg_database_add (int vdes, const char *dbname);
extern int cfg_database_delete (int vdes, const char *dbname);

#endif /* _DATABASES_FILE_H_ */
