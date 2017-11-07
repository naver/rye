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
 * cas_protocol.h -
 */

#ifndef _CAS_PROTOCOL_H_
#define _CAS_PROTOCOL_H_

#ifdef __cplusplus
extern "C"
{
#endif

#ident "$Id$"

#define BRREQ_MSG_SIZE				20
#define BRREQ_MSG_MAGIC_LEN			4
#define BRREQ_MSG_MAGIC_STR			"RYE\001"
#define BRREQ_OP_CODE_MSG_MAX_SIZE		0x7fff	/* short int */

/* db_name used by client's broker health checker */
#define HEALTH_CHECK_DUMMY_DB "___health_check_dummy_db___"

#define SHARD_MGMT_DB_USER	"shard_management"

#define ERROR_RESPONSE 		0
#define SUCCESS_RESPONSE 	1

#define EXEC_QUERY_ERROR		0
#define EXEC_QUERY_SUCCESS		1

#define BRREQ_NET_MSG_SIZE(VAR)		\
		((VAR)->op_code_msg_size + BRREQ_MSG_SIZE)

#define BROKER_RESPONSE_MAX_ADDITIONAL_MSG 	5
#define BROKER_RESPONSE_MSG_SIZE		\
	      (12 + sizeof(int) * BROKER_RESPONSE_MAX_ADDITIONAL_MSG)
  /* protocol(2),  result_code(4), additional_mesaage_size */

#define SRV_CON_DBNAME_SIZE			32
#define SRV_CON_DBUSER_SIZE			32
#define SRV_CON_DBPASSWD_SIZE			32
#define SRV_CON_URL_SIZE			512
#define SRV_CON_DBSESS_ID_SIZE			20
#define SRV_CON_VER_STR_MAX_SIZE		20

#define MSG_HEADER_MSG_SIZE		((int) sizeof(int))
#define MSG_HEADER_SIZE		(CAS_STATUS_INFO_SIZE +  MSG_HEADER_MSG_SIZE)

#define CAS_STATUS_INFO_SIZE			(16)
#define CAS_STATUS_INFO_RESERVED_DEFAULT	(0)

#define DRIVER_SESSION_SIZE			20

#define EXEC_NOT_CONTAIN_FETCH_RESULT		0x00
#define EXEC_CONTAIN_FETCH_RESULT		0x01

/* next_result flag */
#define CCI_CLOSE_CURRENT_RESULT        0
#define CCI_KEEP_CURRENT_RESULT         1

#define DRIVER_SHARD_INFO_SYNC_INTERVAL_SEC	10

  typedef struct
  {
    short ver_major;
    short ver_minor;
    short ver_patch;
    short ver_build;
  } T_BROKER_RYE_VERSION;

  typedef struct
  {
    T_BROKER_RYE_VERSION clt_version;
    char clt_type;
    unsigned char op_code;
    short op_code_msg_size;
    char *op_code_msg;
    char msg_buffer[BRREQ_MSG_SIZE];
  } T_BROKER_REQUEST_MSG;

  typedef struct
  {
    T_BROKER_RYE_VERSION svr_version;
    int result_code;
    int additional_message_size[BROKER_RESPONSE_MAX_ADDITIONAL_MSG];
  } T_BROKER_RESPONSE;

  typedef struct
  {
    int msg_buffer_size;
    char msg_buffer[sizeof (int) + sizeof (T_BROKER_RESPONSE)];
  }
  T_BROKER_RESPONSE_NET_MSG;

  typedef enum
  {
    BRREQ_OP_CODE_CAS_CONNECT = 1,
    BRREQ_OP_CODE_PING = 2,
    BRREQ_OP_CODE_QUERY_CANCEL = 3,

    BRREQ_OP_CODE_MIN = BRREQ_OP_CODE_CAS_CONNECT,
    BRREQ_OP_CODE_MAX = BRREQ_OP_CODE_QUERY_CANCEL,

    BRREQ_OP_CODE_SYNC_SHARD_MGMT_INFO = 16,
    BRREQ_OP_CODE_LAUNCH_PROCESS = 17,
    BRREQ_OP_CODE_GET_SHARD_MGMT_INFO = 18,
    BRREQ_OP_CODE_NUM_SHARD_VERSION_INFO = 19,
    BRREQ_OP_CODE_READ_RYE_FILE = 20,
    BRREQ_OP_CODE_WRITE_RYE_CONF = 21,
    BRREQ_OP_CODE_UPDATE_CONF = 22,
    BRREQ_OP_CODE_DELETE_CONF = 23,
    BRREQ_OP_CODE_GET_CONF = 24,
    BRREQ_OP_CODE_BR_ACL_RELOAD = 25,

    BRREQ_OP_CODE_LOCAL_MMGT_MIN = BRREQ_OP_CODE_SYNC_SHARD_MGMT_INFO,
    BRREQ_OP_CODE_LOCAL_MGMT_MAX = BRREQ_OP_CODE_BR_ACL_RELOAD,

    /* shard mgmt op codes */
    BRREQ_OP_CODE_GET_SHARD_INFO = 64,
    BRREQ_OP_CODE_INIT = 65,
    BRREQ_OP_CODE_ADD_NODE = 66,
    BRREQ_OP_CODE_DROP_NODE = 67,
    BRREQ_OP_CODE_MIGRATION_START = 68,
    BRREQ_OP_CODE_MIGRATION_END = 69,
    BRREQ_OP_CODE_DDL_START = 70,
    BRREQ_OP_CODE_DDL_END = 71,
    BRREQ_OP_CODE_REBALANCE_REQ = 72,
    BRREQ_OP_CODE_REBALANCE_JOB_COUNT = 73,
    BRREQ_OP_CODE_GC_START = 74,
    BRREQ_OP_CODE_GC_END = 75,

    BRREQ_OP_CODE_SHARD_MGMT_MIN = BRREQ_OP_CODE_GET_SHARD_INFO,
    BRREQ_OP_CODE_SHARD_MGMT_MAX = BRREQ_OP_CODE_GC_END
  } T_BRREQ_OP_CODE;

  typedef enum
  {
    READ_RYE_FILE_RYE_CONF = 1,
    READ_RYE_FILE_BR_ACL = 2
  } T_READ_RYE_FILE;

  typedef enum
  {
    MGMT_LAUNCH_PROCESS_MIGRATOR = 1,
    MGMT_LAUNCH_PROCESS_RYE_COMMAND = 2,
    MGMT_LAUNCH_PROCESS_ID_MIN = MGMT_LAUNCH_PROCESS_MIGRATOR,
    MGMT_LAUNCH_PROCESS_ID_MAX = MGMT_LAUNCH_PROCESS_RYE_COMMAND
  } T_MGMT_LAUNCH_PROCESS_ID;

  typedef enum
  {
    MGMT_REQ_ARG_INT = 1,
    MGMT_REQ_ARG_INT64 = 2,
    MGMT_REQ_ARG_STR = 3,
    MGMT_REQ_ARG_STR_ARRAY = 4,
    MGMT_REQ_ARG_INT_ARRAY = 5,
    MGMT_REQ_ARG_END = 6
  } T_MGMT_REQ_ARG_TYPE;

  typedef enum
  {
    MGMT_LAUNCH_ERROR_EXEC_FAIL = -101,
    MGMT_LAUNCH_ERROR_ABNORMALLY_TERMINATED = -102
  } MGMT_LAUNCH_ERROR;

#define BR_MGMT_REQ_LAST_ARG_VALUE	0x12345678

  typedef enum
  {
    BRREQ_REBALANCE_TYPE_REBALANCE = 0,
    BRREQ_REBALANCE_TYPE_EMPTY_NODE = 1
  } BRREQ_REBALANCE_TYPE;

  typedef enum
  {
    MGMT_REBALANCE_JOB_COUNT_TYPE_REMAIN = 0,
    MGMT_REBALANCE_JOB_COUNT_TYPE_COMPLETE = 1,
    MGMT_REBALANCE_JOB_COUNT_TYPE_FAILED = 2
  } T_MGMT_REBALANCE_JOB_COUNT_TYPE;

  typedef enum
  {
    CAS_STATUS_INACTIVE = 0,
    CAS_STATUS_ACTIVE = 1
  } T_CAS_STATUS_TYPE;

  typedef enum
  {
    CAS_STATUS_INFO_IDX_STATUS = 0,
    CAS_STATUS_INFO_IDX_SERVER_NODEID = 1,
    CAS_STATUS_INFO_IDX_SHARD_INFO_VER = 3,
    CAS_STATUS_INFO_IDX_RESERVED_11 = 11,
    CAS_STATUS_INFO_IDX_RESERVED_12 = 12,
    CAS_STATUS_INFO_IDX_RESERVED_13 = 13,
    CAS_STATUS_INFO_IDX_RESERVED_14 = 14,
    CAS_STATUS_INFO_IDX_RESERVED_15 = 15
  } T_CAS_STATUS_INFO_IDX;

  typedef enum
  {
    CAS_HOLDABLE_RESULT_NOT_SUPPORT = 0,
    CAS_HOLDABLE_RESULT_SUPPORT = 1
  } T_CAS_HOLDABLE_RESULT;

  typedef enum
  {
    CAS_STATEMENT_POOLING_OFF = 0,
    CAS_STATEMENT_POOLING_ON = 1
  } T_CAS_STATEMENT_POOLING;

  typedef enum
  {
    CCI_DEFAULT_AUTOCOMMIT_OFF = 0,
    CCI_DEFAULT_AUTOCOMMIT_ON = 1
  } T_CCI_DEFAULT_AUTOCOMMIT;

  typedef enum
  {
    CAS_DBMS_RYE = 1,
  } T_T_DBMS_TYPE;

  typedef enum
  {
    CAS_CLIENT_JDBC = 1,
    CAS_CLIENT_CCI = 2
  } T_CAS_CLIENT_TYPE;

  typedef struct
  {
    int msg_size;
    char *db_name;
    char *db_user;
    char *db_passwd;
    char *url;
    char *client_version;
    char *db_session_id;
    char msg_buffer[1];
  } T_DB_CONNECT_MSG;

  typedef enum
  {
    CAS_FC_END_TRAN = 1,
    CAS_FC_PREPARE = 2,
    CAS_FC_EXECUTE = 3,
    CAS_FC_GET_DB_PARAMETER = 4,
    CAS_FC_CLOSE_REQ_HANDLE = 5,
    CAS_FC_FETCH = 6,
    CAS_FC_SCHEMA_INFO = 7,
    CAS_FC_GET_DB_VERSION = 8,
    CAS_FC_GET_CLASS_NUM_OBJS = 9,
    CAS_FC_EXECUTE_BATCH = 10,
    CAS_FC_GET_QUERY_PLAN = 11,
    CAS_FC_CON_CLOSE = 12,
    CAS_FC_CHECK_CAS = 13,
    CAS_FC_CURSOR_CLOSE = 14,
    CAS_FC_CHANGE_DBUSER = 15,
    CAS_FC_UPDATE_GROUP_ID = 16,
    CAS_FC_INSERT_GID_REMOVED_INFO = 17,
    CAS_FC_DELETE_GID_REMOVED_INFO = 18,
    CAS_FC_DELETE_GID_SKEY_INFO = 19,
    CAS_FC_BLOCK_GLOBAL_DML = 20,
    CAS_FC_SERVER_MODE = 21,
    CAS_FC_SEND_REPL_DATA = 22,
    CAS_FC_NOTIFY_HA_AGENT_STATE = 23,

    CAS_FC_MAX,
  } T_CAS_FUNC_CODE;

  typedef enum
  {
    CAS_CHANGE_MODE_UNKNOWN = 0,
    CAS_CHANGE_MODE_AUTO = 1,
    CAS_CHANGE_MODE_KEEP = 2,
    CAS_CHANGE_MODE_DEFAULT = CAS_CHANGE_MODE_AUTO
  } T_CAS_CHANGE_MODE;

  typedef enum
  {
    CAS_CURSOR_STATUS_OPEN = 0,
    CAS_CURSOR_STATUS_CLOSED
  } T_CAS_CURSOR_STATUS;

  typedef enum
  {
    BR_RES_SHARD_INFO_ALL = 0,
    BR_RES_SHARD_INFO_CHANGED_ONLY = 1
  } T_BR_RES_SHARD_INFO;

  typedef enum
  {
    HA_STATE_FOR_DRIVER_UNKNOWN = 0,
    HA_STATE_FOR_DRIVER_MASTER,
    HA_STATE_FOR_DRIVER_TO_BE_MASTER,
    HA_STATE_FOR_DRIVER_SLAVE,
    HA_STATE_FOR_DRIVER_TO_BE_SLAVE,
    HA_STATE_FOR_DRIVER_REPLICA
  } HA_STATE_FOR_DRIVER;

  extern UINT64 br_msg_protocol_version (const T_BROKER_RYE_VERSION * ver);
  extern T_BROKER_REQUEST_MSG *brreq_msg_alloc (int opcode_msg_size);
  extern T_BROKER_REQUEST_MSG *brreq_msg_clone (const T_BROKER_REQUEST_MSG *
						org_msg);

  extern void brreq_msg_free (T_BROKER_REQUEST_MSG * ptr);
  extern int brreq_msg_unpack (T_BROKER_REQUEST_MSG * srv_con_msg);
  extern char *brreq_msg_pack (T_BROKER_REQUEST_MSG * srv_con_msg,
			       char clt_type, char op_code,
			       int op_code_msg_size);
  extern void brres_msg_pack (T_BROKER_RESPONSE_NET_MSG * res_msg,
			      int result_code, int num_additional_msg,
			      const int *addtional_msg_size);
  extern int brres_msg_unpack (T_BROKER_RESPONSE * res,
			       const char *msg_buffer, int msg_size);
  extern void cas_status_info_init (char *info_ptr);

  extern int brreq_msg_normal_broker_opcode_msg_size (const char *port_name,
						      int add_size);
  extern char *brreq_msg_pack_port_name (char *ptr, const char *port_name);
  extern const char *brreq_msg_unpack_port_name (const T_BROKER_REQUEST_MSG *
						 brreq_msg,
						 const char **ret_msg_ptr,
						 int *ret_msg_remain);
  extern char *br_msg_pack_int (char *ptr, int value);
  extern char *br_msg_pack_short (char *ptr, short value);
  extern char *br_msg_pack_str (char *ptr, const char *str, int size);
  extern char *br_msg_pack_char (char *ptr, char value);


#ifdef __cplusplus
}
#endif

#endif				/* _CAS_PROTOCOL_H_ */
