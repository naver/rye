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
 * connection_defs.h - all the #define, the structure defs and the typedefs
 *          for the client/server implementation
 */

#ifndef _CONNECTION_DEFS_H_
#define _CONNECTION_DEFS_H_

#ident "$Id$"

#include <stdio.h>
#include <poll.h>
#if defined(SERVER_MODE)
#include <pthread.h>
#endif /* SERVER_MODE */

#include "porting.h"
#include "release_string.h"
#include "memory_alloc.h"
#include "error_manager.h"
#if defined(SERVER_MODE)
#include "critical_section.h"
#endif
#include "thread.h"
#include "boot.h"

/*
 * These are the types of top-level commands sent to the master server
 * from the client when initiating a connection. They distinguish the
 * difference between an information connection and a user connection.
 */
typedef enum
{
  SVR_CONNECT_TYPE_MASTER_INFO,	/* send master request to the master server */
  SVR_CONNECT_TYPE_TO_SERVER,	/* get data from the database server */
  SVR_CONNECT_TYPE_TRANSFER_CONN,	/* get data from the database server */
  SVR_CONNECT_TYPE_MASTER_HB_PROC	/* let new server attach */
} SVR_CONNECT_TYPE;

/*
 * These are the responses from the master to a server
 * when it is trying to connect and register itself.
 */
enum css_master_response
{
  SERVER_ALREADY_EXISTS,
  SERVER_REQUEST_ACCEPTED,
};

/*
 * These are the types of requests sent by the information client,
 * rye_server, rye_repl to the master.
 */
typedef enum _css_master_request CSS_MASTER_REQUEST;
enum _css_master_request
{
  MASTER_GET_START_TIME,
  MASTER_GET_SERVER_COUNT,
  MASTER_GET_REQUEST_COUNT,
  MASTER_GET_SERVER_LIST,
  MASTER_GET_HA_PING_HOST_INFO,	/* HA: get ping hosts info */
  MASTER_GET_HA_NODE_LIST,	/* HA: get ha node list */
  MASTER_GET_HA_PROCESS_LIST,	/* HA: get ha process list */
  MASTER_GET_HA_ADMIN_INFO,	/* HA: get administrative info */
  MASTER_IS_REGISTERED_HA_PROCS,	/* HA: check registered ha process */
  MASTER_GET_SERVER_STATE,	/* broker: get the server state */

  MASTER_START_SHUTDOWN,
  MASTER_ACTIVATE_HEARTBEAT,	/* HA: activate */
  MASTER_REGISTER_HA_PROCESS,
  MASTER_DEACT_STOP_ALL,	/* HA: prepare for deactivation */
  MASTER_DEACT_CONFIRM_STOP_ALL,	/* HA: confirm preparation for deactiavtion */
  MASTER_DEACTIVATE_HEARTBEAT,	/* HA: deactivate */
  MASTER_DEACT_CONFIRM_NO_SERVER,	/* HA: confirm the completion of deactivation */
  MASTER_RECONFIG_HEARTBEAT,	/* HA: reconfigure ha node */

  MASTER_CHANGEMODE,
  MASTER_CHANGE_SERVER_STATE,
  MASTER_REQUEST_END
};

/*
 * These are the types of requests sent by rye_master to the rye_server.
 */
typedef enum _css_master_to_server_request CSS_MASTER_TO_SERVER_REQUEST;
enum _css_master_to_server_request
{
  SERVER_START_SHUTDOWN,
  SERVER_CHANGE_HA_MODE
};

/*
 * These are the status codes for the connection structure which represent
 * the state of the connection.
 */
enum css_conn_status
{
  CONN_OPEN = 1,
  CONN_CLOSED = 2,
  CONN_CLOSING = 3
};

/*
 * These are the types of fds in the socket queue.
 */
enum
{
  READ_WRITE = 0,
  READ_ONLY = 1,
  WRITE_ONLY = 2
};

/*
 * These are the types of "packets" that can be sent over the comm interface.
 */
enum css_packet_type
{
  COMMAND_TYPE = 1,
  DATA_TYPE = 2,
  ABORT_TYPE = 3,
  CLOSE_TYPE = 4,
  ERROR_TYPE = 5
};

/*
 * These are the status conditions that can be returned when a client
 * is trying to get a connection.
 */
enum css_status
{
  SERVER_CONNECTED = 0,
  SERVER_CLIENTS_EXCEEDED = 1,
  SERVER_INACCESSIBLE_IP = 2
};

/*
 * These are the error values returned by the client and server interfaces
 */
enum css_error_code
{
  NO_ERRORS = 1,
  CONNECTION_CLOSED = 2,
  REQUEST_REFUSED = 3,
  ERROR_ON_READ = 4,
  ERROR_ON_WRITE = 5,
  RECORD_TRUNCATED = 6,
  ERROR_WHEN_READING_SIZE = 7,
  READ_LENGTH_MISMATCH = 8,
  ERROR_ON_COMMAND_READ = 9,
  NO_DATA_AVAILABLE = 10,
  WRONG_PACKET_TYPE = 11,
  SERVER_WAS_NOT_FOUND = 12,
  SERVER_ABORTED = 13,
  INTERRUPTED_READ = 14,
  CANT_ALLOC_BUFFER = 15,
  OS_ERROR = 16,
  TIMEDOUT_ON_QUEUE = 17,
  NOT_COMPATIBLE_VERSION = 18
};

/*
 * Server's request_handler status codes.
 * Assigned to error_p in current socket queue entry.
 */
enum css_status_code
{
  CSS_NO_ERRORS = 0,
  CSS_UNPLANNED_SHUTDOWN = 1,
  CSS_PLANNED_SHUTDOWN = 2
};

/*
 * There are the modes to check peer-alive.
 */
enum css_check_peer_alive
{
  CSS_CHECK_PEER_ALIVE_NONE,
  CSS_CHECK_PEER_ALIVE_SERVER_ONLY,
  CSS_CHECK_PEER_ALIVE_CLIENT_ONLY,
  CSS_CHECK_PEER_ALIVE_BOTH
};
#define CHECK_CLIENT_IS_ALIVE() \
  (prm_get_integer_value (PRM_ID_CHECK_PEER_ALIVE) == CSS_CHECK_PEER_ALIVE_BOTH \
  || prm_get_integer_value (PRM_ID_CHECK_PEER_ALIVE) == CSS_CHECK_PEER_ALIVE_SERVER_ONLY)
#define CHECK_SERVER_IS_ALIVE() \
  (prm_get_integer_value (PRM_ID_CHECK_PEER_ALIVE) == CSS_CHECK_PEER_ALIVE_BOTH \
  || prm_get_integer_value (PRM_ID_CHECK_PEER_ALIVE) == CSS_CHECK_PEER_ALIVE_CLIENT_ONLY)

#define HB_PROC_ID_STR         "HB_PROC_ID"

#define NULL_CLIENT_ID         -1

/*
 * HA mode
 */
typedef enum ha_mode HA_MODE;
enum ha_mode
{
  HA_MODE_OFF = 0,		/* unused */
  HA_MODE_FAIL_OVER = 1,	/* unused */
  HA_MODE_FAIL_BACK = 2,
  HA_MODE_LAZY_BACK = 3,	/* not implemented yet */
  HA_MODE_ROLE_CHANGE = 4,
  HA_MODE_REPLICA = 5
};
#define HA_MODE_OFF_STR		"off"
#define HA_MODE_FAIL_OVER_STR	"fail-over"
#define HA_MODE_FAIL_BACK_STR	"fail-back"
#define HA_MODE_LAZY_BACK_STR	"lazy-back"
#define HA_MODE_ROLE_CHANGE_STR	"role-change"
#define HA_MODE_REPLICA_STR     "replica"
#define HA_MODE_ON_STR          "on"

typedef enum _ha_state HA_STATE;
enum _ha_state
{
  HA_STATE_NA = -1,		/* N/A */
  HA_STATE_UNKNOWN = 0,		/* initial state */
  HA_STATE_MASTER = 1,
  HA_STATE_TO_BE_MASTER = 2,
  HA_STATE_SLAVE = 3,
  HA_STATE_TO_BE_SLAVE = 4,
  HA_STATE_REPLICA = 5,		/* replica mode */
  HA_STATE_DEAD = 6,		/* virtual state; not exists */
  HA_STATE_MAX
};

#define HA_STATE_STR_SZ                  (13)

#define HA_STATE_NAME(ha_stat)                           \
  ((ha_stat) == HA_STATE_NA ? "na" :                     \
   (ha_stat) == HA_STATE_UNKNOWN ? "unknown" :           \
   (ha_stat) == HA_STATE_MASTER ? "master" :             \
   (ha_stat) == HA_STATE_TO_BE_MASTER ? "to-be-master" : \
   (ha_stat) == HA_STATE_SLAVE ? "slave" :               \
   (ha_stat) == HA_STATE_TO_BE_SLAVE ? "to-be-slave" :   \
   (ha_stat) == HA_STATE_REPLICA ? "replica" :           \
   (ha_stat) == HA_STATE_DEAD ? "dead" : "invalid")


#define HA_STATE_NAME(ha_stat)                           \
  ((ha_stat) == HA_STATE_NA ? "na" :                     \
   (ha_stat) == HA_STATE_UNKNOWN ? "unknown" :           \
   (ha_stat) == HA_STATE_MASTER ? "master" :             \
   (ha_stat) == HA_STATE_TO_BE_MASTER ? "to-be-master" : \
   (ha_stat) == HA_STATE_SLAVE ? "slave" :               \
   (ha_stat) == HA_STATE_TO_BE_SLAVE ? "to-be-slave" :   \
   (ha_stat) == HA_STATE_REPLICA ? "replica" :           \
   (ha_stat) == HA_STATE_DEAD ? "dead" : "invalid")


/*
 * HA log applier state
 */
typedef enum ha_apply_state HA_APPLY_STATE;
enum ha_apply_state
{
  HA_APPLY_STATE_NA = -1,
  HA_APPLY_STATE_UNREGISTERED = 0,
  HA_APPLY_STATE_RECOVERING = 1,
  HA_APPLY_STATE_WORKING = 2,
  HA_APPLY_STATE_DONE = 3,
  HA_APPLY_STATE_ERROR = 4
};

#define HA_APPLY_STATE_NAME(apply_stat)                           \
  ((apply_stat) == HA_APPLY_STATE_NA ? "na" :                     \
   (apply_stat) == HA_APPLY_STATE_UNREGISTERED ? "unregistered" : \
   (apply_stat) == HA_APPLY_STATE_RECOVERING ? "recovering" :     \
   (apply_stat) == HA_APPLY_STATE_WORKING ? "working" :           \
   (apply_stat) == HA_APPLY_STATE_DONE ? "done" :                 \
   (apply_stat) == HA_APPLY_STATE_ERROR ? "error" : "invalid")

typedef enum log_ha_filestat LOG_HA_FILESTAT;
enum log_ha_filestat
{
  LOG_HA_FILESTAT_CLEAR = 0,
  LOG_HA_FILESTAT_ARCHIVED = 1,
  LOG_HA_FILESTAT_SYNCHRONIZED = 2
};

#define LOG_HA_FILESTAT_NAME(fstat)                                   \
  ((fstat) == LOG_HA_FILESTAT_CLEAR ? "clear" :                       \
   (fstat) == LOG_HA_FILESTAT_ARCHIVED ? "archived" :                 \
   (fstat) == LOG_HA_FILESTAT_SYNCHRONIZED ? "synchronized" : "invalid")

#define HA_DELAY_ERR_CORRECTION             1000

#define HA_REQUEST_SUCCESS      "1\0"
#define HA_REQUEST_FAILURE      "0\0"
#define HA_REQUEST_RESULT_SIZE  2

#define HA_EMPTY_BUFFER         "\0"
#define HA_EMPTY_BUFFER_SIZE    1

/*
 * This constant defines the maximum size of a msg from the master to the
 * server.  Every msg between the master and the server will transmit this
 * many bytes.  A constant msg size is necessary since the protocol does
 * not pre-send the msg length to the server before sending the actual msg.
 */
#define MASTER_TO_SRV_MSG_SIZE 1024

#ifdef PRINTING
#define TPRINTF(error_string, arg) \
  do \
    { \
      fprintf (stderr, error_string, (arg)); \
      fflush (stderr); \
    } \
  while (0)

#define TPRINTF2(error_string, arg1, arg2) \
  do \
    { \
      fprintf (stderr, error_string, (arg1), (arg2)); \
      fflush (stderr); \
    } \
  while (0)
#else /* PRINTING */
#define TPRINTF(error_string, arg)
#define TPRINTF2(error_string, arg1, arg2)
#endif /* PRINTING */

/* TODO: 64Bit porting */
#define HIGH16BITS(X) (((X) >> 16) & 0xffffL)
#define LOW16BITS(X)  ((X) & 0xffffL)
#define DEFAULT_HEADER_DATA {0,0,0,0,0,0,0,0,0}

#define CSS_RID_FROM_EID(eid)           ((unsigned short) LOW16BITS(eid))
#define CSS_ENTRYID_FROM_EID(eid)       ((unsigned short) HIGH16BITS(eid))


/*
 * This is the format of the header for each command packet that is sent
 * across the network.
 */
#define CSS_NET_PACKET_MAX_BUFFERS	5

typedef struct packet_header NET_HEADER;
struct packet_header
{
  char packet_type;
  char is_server_in_tran;
  char reset_on_commit;
  char is_client_ro_tran;
  short server_shard_nodeid;
  short function_code;
  int tran_index;
  int request_id;
  int num_buffers;
  int buffer_sizes[CSS_NET_PACKET_MAX_BUFFERS];
};

typedef struct
{
  NET_HEADER header;
  struct
  {
    const char *data_ptr;
  } buffer[CSS_NET_PACKET_MAX_BUFFERS];
} CSS_NET_PACKET;

/*
 * This data structure is the interface between the client and the
 * communication software to identify the data connection.
 */

typedef struct css_conn_entry CSS_CONN_ENTRY;
struct css_conn_entry
{
  RYE_VERSION peer_version;
  SOCKET fd;
  unsigned short request_id;
  int status;			/* CONN_OPEN, CONN_CLOSED, CONN_CLOSING = 3 */
  int tran_index;
  int client_id;
  bool is_server_in_tran;	/* worker threads's tran status. */
  bool conn_reset_on_commit;	/* set reset_on_commit when commit/abort */
  bool is_client_ro_tran;	/* is client read-only tran? */
  short server_shard_nodeid;	/* server's nodeid */

#if defined(SERVER_MODE)
  int idx;			/* connection index */
  BOOT_CLIENT_TYPE client_type;
  pthread_mutex_t conn_mutex;
  bool stop_talk;		/* block and stop this connection */
  bool ignore_repl_delay;	/* don't do reset_on_commit by the delay
				 * of replication */
  unsigned short stop_phase;
  bool con_close_handler_activated;
  int epoll_info_index;

  struct session_state *session_p;	/* session object for current request */
  int epoll_check_time;
  bool epoll_check_err;
#endif

  SESSION_ID session_id;
  CSS_CONN_ENTRY *next;
};

/*
 * This is the mapping entry from a host/key to/from the entry id.
 */
typedef struct css_mapping_entry CSS_MAP_ENTRY;
struct css_mapping_entry
{
  char *key;			/* host name (or some such) */
  CSS_CONN_ENTRY *conn;		/* the connection */
#if !defined(SERVER_MODE)
  CSS_MAP_ENTRY *next;
#endif
  unsigned short id;		/* host id to help identify the connection */
};

/*
 * This data structure is the information of user access status written
 * when client login server.
 */
typedef struct last_access_status LAST_ACCESS_STATUS;
struct last_access_status
{
  char db_user[DB_MAX_USER_LENGTH];
  time_t time;
  char host[MAXHOSTNAMELEN];
  char program_name[32];
  LAST_ACCESS_STATUS *next;
};

#endif /* _CONNECTION_DEFS_H_ */
