/*
 * Copyright (C) 2008 Search Solution Corporation. All rights reserved by Search Solution.
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 *
 * - Redistributions of source code must retain the above copyright notice,
 *   this list of conditions and the following disclaimer.
 *
 * - Redistributions in binary form must reproduce the above copyright notice,
 *   this list of conditions and the following disclaimer in the documentation
 *   and/or other materials provided with the distribution.
 *
 * - Neither the name of the <ORGANIZATION> nor the names of its contributors
 *   may be used to endorse or promote products derived from this software without
 *   specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA,
 * OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */


/*
 * cci_handle_mng.h -
 */

#ifndef	_CCI_HANDLE_MNG_H_
#define	_CCI_HANDLE_MNG_H_

#ifdef __cplusplus
extern "C"
{
#endif

#ident "$Id$"

#ifdef CAS
#error include error
#endif

#include "config.h"

/************************************************************************
 * IMPORTED SYSTEM HEADER FILES						*
 ************************************************************************/

/************************************************************************
 * OTHER IMPORTED HEADER FILES						*
 ************************************************************************/

#include "release_string.h"
#include "cci_common.h"
#include "cas_cci.h"
#include "cas_protocol.h"

/************************************************************************
 * PUBLIC DEFINITIONS							*
 ************************************************************************/

#define QUERY_RESULT_FREE(REQ_HANDLE)		\
	do {					\
	  qe_query_result_free((REQ_HANDLE)->num_query_res, (REQ_HANDLE)->qr); \
	  (REQ_HANDLE)->cur_fetch_tuple_index = 0; \
	  (REQ_HANDLE)->num_query_res = 0;	\
	  (REQ_HANDLE)->num_tuple = 0;		\
	  (REQ_HANDLE)->qr = NULL;		\
	} while (0)

#define ALTER_HOST_MAX_SIZE                     256
#define DEFERRED_CLOSE_HANDLE_ALLOC_SIZE        256
#define MONITORING_INTERVAL		    	60

#define REACHABLE       true
#define UNREACHABLE     false

#define COL_INFO_FREE(COL_INFO, NUM_COL_INFO)			\
	do {							\
	  if (COL_INFO)						\
	    {							\
	      int i;						\
	      for (i = 0; i < NUM_COL_INFO; i++)		\
		{						\
		  FREE_MEM (COL_INFO[i].col_name);		\
		  FREE_MEM (COL_INFO[i].real_attr);		\
		  FREE_MEM (COL_INFO[i].class_name);		\
		  FREE_MEM (COL_INFO[i].default_value);		\
		}						\
	      FREE_MEM (COL_INFO);				\
	    }							\
	} while (0)

/************************************************************************
 * PUBLIC TYPE DEFINITIONS						*
 ************************************************************************/

  typedef enum
  {
    CCI_CON_STATUS_OUT_TRAN = 0,
    CCI_CON_STATUS_IN_TRAN = 1
  } T_CCI_CON_STATUS;

  typedef enum
  {
    HANDLE_PREPARE,
    HANDLE_SCHEMA_INFO,
  } T_HANDLE_TYPE;

  typedef struct
  {
    int tuple_index;
    char **column_ptr;
  } T_TUPLE_VALUE;

  typedef struct
  {
    T_CCI_TYPE a_type;
    int size;			/* bind_param : value size
				   bind_param_array : a_type of value */
    const void *value;
    int *null_ind;
    void *alloc_buffer;
  } T_BIND_VALUE;

  typedef struct
  {
    int size;
    void *data;
  } T_VALUE_BUF;

  typedef struct
  {
    CCI_HANDLE_ID req_handle_id;
    char prepare_flag;
    char execute_flag;
    char handle_type;
    char *sql_text;
    int max_row;
    int server_handle_id;
    int num_tuple;
    T_CCI_STMT_TYPE stmt_type;
    int num_bind;
    T_BIND_VALUE *bind_value;
    T_CCI_COL_INFO *col_info;
    int num_col_info;
    int bind_array_size;
    int fetch_size;
    void *net_res;
    int cursor_pos;
    int fetched_tuple_begin;
    int fetched_tuple_end;
    int cur_fetch_tuple_index;
    T_TUPLE_VALUE *tuple_value;
    T_VALUE_BUF conv_value_buffer;
    T_CCI_QUERY_RESULT *qr;
    int num_query_res;
    int valid;
    int query_timeout;
    int is_closed;
    int is_from_current_transaction;
    /* char is_fetch_completed; * used only cas4oracle */
    char cas_cursor_status;
    void *prev;
    void *next;
  } T_REQ_HANDLE;

  typedef struct
  {
    unsigned char ip_addr[4];
    int port;
  } T_HOST_INFO;

  typedef struct
  {
    int count;
    int cur_id;
    T_HOST_INFO host_info[1];
  } T_ALTER_HOST;

#define CON_CAS_ID(c)					\
	((c)->cas_connect_info.cas_id)
#define CON_CAS_PID(c)					\
	((c)->cas_connect_info.cas_pid)
#define CON_CAS_PROTO_VERSION(c)			\
	br_msg_protocol_version(&((c)->cas_connect_info.svr_version))
#define CON_SESSION_ID(c)				\
	((c)->cas_connect_info.session_id)
#define CON_CAS_STATUS_INFO(c)				\
	((c)->cas_connect_info.status_info)

#define CON_CAS_DBMS(c)					\
	((c)->cas_connect_info.dbms)
#define IS_CAS_HOLDABLE_RESULT(c)			\
	((c)->cas_connect_info.holdable_result == 	\
	 CAS_HOLDABLE_RESULT_SUPPORT)
#define IS_CAS_STATEMENT_POOLING(c)			\
	((c)->cas_connect_info.statement_pooling == 	\
	 CAS_STATEMENT_POOLING_ON)
#define SET_AUTOCOMMIT_FROM_CASINFO(c)			\
  (c)->autocommit_mode = \
  ((c)->cas_connect_info.cci_default_autocommit == CCI_DEFAULT_AUTOCOMMIT_ON ? \
          CCI_AUTOCOMMIT_TRUE : CCI_AUTOCOMMIT_FALSE)

#define IS_CON_TYPE_LOCAL(CON_HANDLE)		\
	((CON_HANDLE)->con_property.con_type == CON_TYPE_LOCAL)
#define IS_CON_TYPE_GLOBAL(CON_HANDLE)	\
	((CON_HANDLE)->con_property.con_type == CON_TYPE_GLOBAL)

  typedef struct
  {
    int cas_id;
    int cas_pid;
    RYE_VERSION svr_version;
    char dbms;
    char holdable_result;
    char statement_pooling;
    char cci_default_autocommit;
    char session_id[DRIVER_SESSION_SIZE];
    char status_info[CAS_STATUS_INFO_SIZE];
    int server_start_time;
  } T_CAS_CONNECT_INFO;

  typedef enum
  {
    CON_TYPE_GLOBAL,
    CON_TYPE_LOCAL
  } T_CON_TYPE;

  typedef struct
  {
    char load_balance;
    int rc_time;		/* failback try duration */
    int login_timeout;
    int query_timeout;
    char disconnect_on_query_timeout;
    char log_on_exception;
    char log_slow_queries;
    char error_on_server_restart;
    int slow_query_threshold_millis;
    char log_trace_api;
    char log_trace_network;
    char *log_base;
    char *log_filename;
    T_CON_TYPE con_type;
  } T_CON_PROPERTY;

  typedef struct
  {
    short server_shard_nodeid;
    CCI_HANDLE_ID con_handle_id;
    char used;
    char is_retry;
    char con_status;
    CCI_AUTOCOMMIT_MODE autocommit_mode;
    char *server_list;
    char *db_name;
    char *db_user;
    char *db_passwd;
    char *port_name;
    char url_for_logging[SRV_CON_URL_SIZE];
    SOCKET sock_fd;
    int max_req_handle;
    T_REQ_HANDLE **req_handle_table;
    int req_handle_count;
    int open_prepared_statement_count;
    T_CAS_CONNECT_INFO cas_connect_info;
    CCI_MHT_TABLE *stmt_pool;

    /* The connection properties are not supported by the URL */
    T_CCI_TRAN_ISOLATION isolation_level;
    int lock_timeout;
    char *charset;

    /* connection properties */
    T_ALTER_HOST *alter_hosts;
    char force_failback;
    int last_failure_time;

    /* to check timeout */
    struct timeval start_time;	/* function start time to check timeout */
    int current_timeout;	/* login_timeout or query_timeout */
    int deferred_max_close_handle_count;
    int *deferred_close_handle_list;
    int deferred_close_handle_count;
    void *con_logger;
    int is_holdable;
    int64_t req_id_seq_next;
    int lock;

    SOCKET shard_admin_sock_fd;

    T_CON_PROPERTY con_property;

    T_CCI_ERROR err_buf;

  } T_CON_HANDLE;

/************************************************************************
 * PUBLIC FUNCTION PROTOTYPES						*
 ************************************************************************/

  extern char *con_port_name_cache_put (const char *port_name);
  extern bool con_is_unreachable_host (const T_CON_HANDLE * con_handle,
				       int host_id);
  extern void con_unreachable_host_add (const T_CON_HANDLE * con_handle,
					int host_id);

  extern void hm_con_handle_table_init (void);
  extern T_CON_HANDLE *hm_con_handle_alloc (T_ALTER_HOST * alter_hosts,
					    const char *server_list,
					    const char *port_name,
					    const char *db_name,
					    const char *db_user,
					    const char *db_passwd,
					    const char *property_str,
					    const T_CON_PROPERTY *
					    con_property);
  extern int hm_req_handle_alloc (T_CON_HANDLE * connection,
				  T_REQ_HANDLE ** statement);
  extern void hm_req_handle_free (T_CON_HANDLE * con_handle,
				  T_REQ_HANDLE * req_handle);
  extern void hm_req_handle_free_all (T_CON_HANDLE * con_handle);
  extern void hm_req_handle_free_all_unholdable (T_CON_HANDLE * con_handle);
  extern void hm_req_handle_close_all_resultsets (T_CON_HANDLE * con_handle);
  extern void hm_req_handle_close_all_unholdable_resultsets (T_CON_HANDLE *
							     con_handle);
  extern int hm_con_handle_free (T_CON_HANDLE * connection);

  extern T_CCI_ERROR_CODE hm_get_connection (CCI_HANDLE_ID * conn_handle_id,
					     T_CON_HANDLE ** connection,
					     bool force);
  extern T_CCI_ERROR_CODE hm_get_statement (CCI_STMT * stmt,
					    T_CON_HANDLE ** connection,
					    T_REQ_HANDLE ** statement);
  extern void hm_req_handle_fetch_buf_free (T_REQ_HANDLE * req_handle);
  extern int hm_conv_value_buf_alloc (T_VALUE_BUF * val_buf, int size);

  extern void req_handle_col_info_free (T_REQ_HANDLE * req_handle);
  extern void hm_conv_value_buf_clear (T_VALUE_BUF * val_buf);
  extern void req_handle_content_free (T_REQ_HANDLE * req_handle, int reuse);
  extern int req_close_query_result (T_REQ_HANDLE * req_handle);
  extern void hm_invalidate_all_req_handle (T_CON_HANDLE * con_handle);

  extern void hm_set_con_handle_holdable (T_CON_HANDLE * con_handle,
					  int holdable);
  extern int hm_get_con_handle_holdable (T_CON_HANDLE * con_handle);
  extern int hm_get_req_handle_holdable (T_CON_HANDLE * con_handle,
					 T_REQ_HANDLE * req_handle);

  extern void hm_check_rc_time (T_CON_HANDLE * con_handle);
  extern void hm_create_health_check_th (void);

  extern void hm_force_close_connection (T_CON_HANDLE * con_handle);

  extern void hm_set_conn_handle_id (CCI_CONN * conn,
				     const T_CON_HANDLE * con_handle);
  extern void hm_set_stmt_handle_id (CCI_STMT * stmt,
				     const T_CON_HANDLE * con_handle,
				     const T_REQ_HANDLE * req_handle);

  extern void hm_set_handle_id (CCI_HANDLE_ID * handle_id, int id,
				int64_t id_seq);

/************************************************************************
 * PUBLIC VARIABLES							*
 ************************************************************************/

#ifdef __cplusplus
}
#endif

#endif				/* _CCI_HANDLE_MNG_H_ */
