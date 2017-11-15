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
 * boot.h - Boot management common header
 */

#ifndef _BOOT_H_
#define _BOOT_H_

#include "porting.h"
#include "storage_common.h"

typedef enum boot_client_type BOOT_CLIENT_TYPE;
enum boot_client_type
{
  BOOT_CLIENT_UNKNOWN = -1,
  BOOT_CLIENT_SYSTEM_INTERNAL = 0,
  BOOT_CLIENT_DEFAULT = 1,
  BOOT_CLIENT_RSQL = 2,
  BOOT_CLIENT_READ_ONLY_RSQL = 3,
  BOOT_CLIENT_READ_WRITE_BROKER = 4,
  BOOT_CLIENT_READ_ONLY_BROKER = 5,
  BOOT_CLIENT_SLAVE_ONLY_BROKER = 6,
  BOOT_CLIENT_READ_WRITE_ADMIN_UTILITY = 7,
  BOOT_CLIENT_READ_ONLY_ADMIN_UTILITY = 8,
  BOOT_CLIENT_ADMIN_RSQL = 9,
  BOOT_CLIENT_LOG_COPIER = 10,
  BOOT_CLIENT_RW_BROKER_REPLICA_ONLY = 12,
  BOOT_CLIENT_RO_BROKER_REPLICA_ONLY = 13,
  BOOT_CLIENT_SO_BROKER_REPLICA_ONLY = 14,
  BOOT_CLIENT_ADMIN_RSQL_WOS = 15,	/* admin rsql that can write on slave */
  BOOT_CLIENT_RBL_MIGRATOR = 17,
  BOOT_CLIENT_REPL_BROKER = 18,
  BOOT_CLIENT_CREATEDB = 19,
  BOOT_CLIENT_TYPE_MAX = BOOT_CLIENT_CREATEDB
};

#define BOOT_CLIENT_TYPE_STRING(type)                                          \
  ((type) == BOOT_CLIENT_SYSTEM_INTERNAL ? "system" :                          \
   (type) == BOOT_CLIENT_DEFAULT ? "default" :                                 \
   (type) == BOOT_CLIENT_RSQL ? "rsql" :                                       \
   (type) == BOOT_CLIENT_READ_ONLY_RSQL ? "read_only_rsql" :                   \
   (type) == BOOT_CLIENT_READ_WRITE_BROKER ? "read_write_broker" :             \
   (type) == BOOT_CLIENT_READ_ONLY_BROKER ? "read_only_broker" :               \
   (type) == BOOT_CLIENT_SLAVE_ONLY_BROKER ? "slave_only_broker":              \
   (type) == BOOT_CLIENT_READ_WRITE_ADMIN_UTILITY ? "read_write_admin":        \
   (type) == BOOT_CLIENT_READ_ONLY_ADMIN_UTILITY ? "read_only_admin":          \
   (type) == BOOT_CLIENT_ADMIN_RSQL ? "admin_rsql":                            \
   (type) == BOOT_CLIENT_LOG_COPIER ? "log_copier":                            \
   (type) == BOOT_CLIENT_RW_BROKER_REPLICA_ONLY ? "read_write_replica_only_broker":\
   (type) == BOOT_CLIENT_RO_BROKER_REPLICA_ONLY ? "read_replica_only_broker" : \
   (type) == BOOT_CLIENT_SO_BROKER_REPLICA_ONLY ? "slave_replica_only_broker": \
   (type) == BOOT_CLIENT_ADMIN_RSQL_WOS ? "admin_rsql_write_on_slave":         \
   (type) == BOOT_CLIENT_RBL_MIGRATOR ? "rbl_migrator":                        \
   (type) == BOOT_CLIENT_REPL_BROKER ? "replication_broker":                   \
   (type) == BOOT_CLIENT_CREATEDB ? "createdb": "unknown")

#define BOOT_WRITE_NORMAL_CLIENT_TYPE(client_type) \
        ((client_type) == BOOT_CLIENT_DEFAULT \
         || (client_type) == BOOT_CLIENT_RSQL \
         || (client_type) == BOOT_CLIENT_READ_WRITE_BROKER \
         || (client_type) == BOOT_CLIENT_RW_BROKER_REPLICA_ONLY \
         || (client_type) == BOOT_CLIENT_RBL_MIGRATOR)

#define BOOT_NORMAL_CLIENT_TYPE(client_type) \
        ((client_type) == BOOT_CLIENT_DEFAULT \
         || (client_type) == BOOT_CLIENT_RSQL \
         || (client_type) == BOOT_CLIENT_READ_ONLY_RSQL \
         || (client_type) == BOOT_CLIENT_READ_WRITE_BROKER \
         || (client_type) == BOOT_CLIENT_READ_ONLY_BROKER \
         || (client_type) == BOOT_CLIENT_RW_BROKER_REPLICA_ONLY \
         || (client_type) == BOOT_CLIENT_RO_BROKER_REPLICA_ONLY \
         || (client_type) == BOOT_CLIENT_RBL_MIGRATOR)

#define BOOT_READ_ONLY_CLIENT_TYPE(client_type) \
        ((client_type) == BOOT_CLIENT_READ_ONLY_RSQL \
         || (client_type) == BOOT_CLIENT_READ_ONLY_BROKER \
         || (client_type) == BOOT_CLIENT_SLAVE_ONLY_BROKER \
         || (client_type) == BOOT_CLIENT_READ_ONLY_ADMIN_UTILITY \
         || (client_type) == BOOT_CLIENT_RO_BROKER_REPLICA_ONLY \
         || (client_type) == BOOT_CLIENT_SO_BROKER_REPLICA_ONLY)

#define BOOT_ADMIN_CLIENT_TYPE(client_type) \
        ((client_type) == BOOT_CLIENT_READ_WRITE_ADMIN_UTILITY \
         || (client_type) == BOOT_CLIENT_READ_ONLY_ADMIN_UTILITY \
         || (client_type) == BOOT_CLIENT_ADMIN_RSQL \
         || (client_type) == BOOT_CLIENT_ADMIN_RSQL_WOS)

#define BOOT_LOG_REPLICATOR_TYPE(client_type) \
        ((client_type) == BOOT_CLIENT_LOG_COPIER \
         || (client_type) == BOOT_CLIENT_REPL_BROKER)

#define BOOT_RSQL_CLIENT_TYPE(client_type) \
        ((client_type) == BOOT_CLIENT_RSQL \
        || (client_type) == BOOT_CLIENT_READ_ONLY_RSQL \
        || (client_type) == BOOT_CLIENT_ADMIN_RSQL \
        || (client_type) == BOOT_CLIENT_ADMIN_RSQL_WOS)

#define BOOT_BROKER_AND_DEFAULT_CLIENT_TYPE(client_type) \
        ((client_type) == BOOT_CLIENT_DEFAULT \
         || (client_type) == BOOT_CLIENT_READ_WRITE_BROKER \
         || (client_type) == BOOT_CLIENT_READ_ONLY_BROKER \
         || (client_type) == BOOT_CLIENT_SLAVE_ONLY_BROKER \
         || (client_type) == BOOT_CLIENT_RBL_MIGRATOR \
         || BOOT_REPLICA_ONLY_BROKER_CLIENT_TYPE(client_type))

#define BOOT_REPLICA_ONLY_BROKER_CLIENT_TYPE(client_type) \
    ((client_type) == BOOT_CLIENT_RW_BROKER_REPLICA_ONLY \
        || (client_type) == BOOT_CLIENT_RO_BROKER_REPLICA_ONLY \
        || (client_type) == BOOT_CLIENT_SO_BROKER_REPLICA_ONLY)

#define BOOT_WRITE_ON_STANDY_CLIENT_TYPE(client_type) \
  ((client_type) == BOOT_CLIENT_REPL_BROKER \
      || (client_type) == BOOT_CLIENT_RW_BROKER_REPLICA_ONLY \
      || (client_type) == BOOT_CLIENT_ADMIN_RSQL_WOS)

/*
 * BOOT_IS_ALLOWED_CLIENT_TYPE_IN_MT_MODE()
 * ((broker_default type || (remote && rsql or broker_default type)) ? 0 : 1)
 */
#define BOOT_IS_ALLOWED_CLIENT_TYPE_IN_MT_MODE(host1, host2, client_type) \
        ((BOOT_BROKER_AND_DEFAULT_CLIENT_TYPE(client_type) || \
          ((host1 != NULL && strcmp (host1, host2)) && \
           (BOOT_RSQL_CLIENT_TYPE(client_type) \
            || BOOT_BROKER_AND_DEFAULT_CLIENT_TYPE(client_type)))) ? 0 : 1)

#define BOOT_IS_PREFERRED_HOSTS_SET(credential) \
        ((credential)->preferred_hosts != NULL \
        && (credential)->preferred_hosts[0] != '\0')

typedef struct boot_client_credential BOOT_CLIENT_CREDENTIAL;
struct boot_client_credential
{
  BOOT_CLIENT_TYPE client_type;
  const char *client_info;	/* DB_MAX_IDENTIFIER_LENGTH */
  const char *db_name;		/* DB_MAX_IDENTIFIER_LENGTH */
  const char *db_user;		/* DB_MAX_USER_LENGTH */
  const char *db_password;	/* DB_MAX_PASSWORD_LENGTH */
  const char *program_name;	/* PATH_MAX */
  const char *login_name;	/* L_cuserid */
  const char *host_name;	/* MAXHOSTNAMELEN */
  const char *preferred_hosts;	/* LINE_MAX */
  bool connect_order_random;
  int process_id;
};

typedef struct boot_db_path_info BOOT_DB_PATH_INFO;
struct boot_db_path_info
{
  const char *db_host;
};

typedef struct boot_server_credential BOOT_SERVER_CREDENTIAL;
struct boot_server_credential
{
  const char *db_full_name;	/* PATH_MAX */
  const char *host_name;	/* MAXHOSTNAMELEN */
  int process_id;
  OID root_class_oid;
  HFID root_class_hfid;
  PGLENGTH page_size;
  PGLENGTH log_page_size;
  char server_session_key[SERVER_SESSION_KEY_SIZE];
  int db_charset;
  const char *db_lang;
  int server_start_time;
  char *alloc_buffer;
};

extern char boot_Host_name[MAXHOSTNAMELEN];

#endif /* _BOOT_H_ */
