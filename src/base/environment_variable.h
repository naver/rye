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
 * environment_variable.h : Functions for manipulating the environment variable
 *
 */

#ifndef _ENVIRONMENT_VARIABLE_H_
#define _ENVIRONMENT_VARIABLE_H_

#ident "$Id$"

#define RYE_CONF_FILE   	"rye-auto.conf"
#define BROKER_ACL_FILE		"rye_broker.acl"

#define APPL_SERVER_CAS_NAME            "rye_cas"
#define NAME_CAS_BROKER                 "rye_broker"

#define BROKER_PATH_MAX             (PATH_MAX)

#define ACCESS_LOG_FILENAME_POSTFIX             ".access"
#define ACCESS_LOG_DENIED_FILENAME_POSTFIX      ".denied"

extern const char *envvar_root (void);
extern const char *envvar_get (const char *);
extern int envvar_set (const char *, const char *);

extern char *envvar_bindir_file (char *path, size_t size,
				 const char *filename);
extern char *envvar_localedir_file (char *path, size_t size,
				    const char *langpath,
				    const char *filename);
extern char *envvar_confdir_file (char *path, size_t size,
				  const char *filename);
extern char *envvar_rye_conf_file (char *path, size_t size);

extern char *envvar_db_dir (char *path, size_t size, const char *db_name);
extern char *envvar_db_log_dir (char *path, size_t size, const char *db_name);
extern char *envvar_vardir_file (char *path, size_t size,
				 const char *filename);
extern char *envvar_tmpdir_file (char *path, size_t size,
				 const char *filename);
extern char *envvar_socket_file (char *path, size_t size,
				 const char *filename);
extern char *envvar_as_pid_dir_file (char *path, size_t size,
				     const char *filename);

extern void envvar_ryelogdir_file (char *path, size_t size,
				   const char *filename);
extern void envvar_ryelog_broker_file (char *path, size_t size,
				       const char *br_name,
				       const char *filename);
extern void envvar_ryelog_broker_sqllog_file (char *path, size_t size,
					      const char *br_name,
					      const char *filename);
extern void envvar_ryelog_broker_slowlog_file (char *path, size_t size,
					       const char *br_name,
					       const char *filename);
extern void envvar_ryelog_broker_errorlog_file (char *path, size_t size,
						const char *br_name,
						const char *filename);
extern void envvar_broker_acl_file (char *path, size_t size);
extern void envvar_process_name (char *buf, size_t size,
				 const char *base_name);

#endif /* _ENVIRONMENT_VARIABLE_H_ */
