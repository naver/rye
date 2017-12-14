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
 * cci_query_execute.h -
 */

#ifndef	_CCI_QUERY_EXECUTE_H_
#define	_CCI_QUERY_EXECUTE_H_

#ident "$Id$"

/************************************************************************
 * IMPORTED SYSTEM HEADER FILES						*
 ************************************************************************/

/************************************************************************
 * IMPORTED OTHER HEADER FILES						*
 ************************************************************************/

#include "cas_cci.h"
#include "cci_handle_mng.h"

#include "repl_common.h"

/************************************************************************
 * EXPORTED DEFINITIONS							*
 ************************************************************************/

#define NET_STR_TO_INT64(INT64_VALUE, PTR)                              \
        do {                                                            \
          INT64           macro_var_tmp_value;                          \
          memcpy((char*) &macro_var_tmp_value, PTR, NET_SIZE_INT64);    \
          macro_var_tmp_value = ntohi64(macro_var_tmp_value);           \
          INT64_VALUE = macro_var_tmp_value;                            \
        } while (0)

#define NET_STR_TO_BIGINT(BIGINT_VALUE, PTR)  \
           NET_STR_TO_INT64(BIGINT_VALUE, PTR)

#define NET_STR_TO_INT(INT_VALUE, PTR)		                        \
	do {					                        \
	  int		macro_var_tmp_value;		                \
	  memcpy((char*) &macro_var_tmp_value, PTR, NET_SIZE_INT);      \
	  macro_var_tmp_value = ntohl(macro_var_tmp_value);		\
	  INT_VALUE = macro_var_tmp_value;		                \
	} while (0)

#define NET_STR_TO_SHORT(SHORT_VALUE, PTR)	                        \
	do {					                        \
	  short		macro_var_tmp_value;		                \
	  memcpy((char*) &macro_var_tmp_value, PTR, NET_SIZE_SHORT);    \
	  macro_var_tmp_value = ntohs(macro_var_tmp_value);		\
	  SHORT_VALUE = macro_var_tmp_value;		                \
	} while (0)

#define NET_STR_TO_BYTE(BYTE_VALUE, PTR)                                \
        do {                                                            \
          (BYTE_VALUE) = *(char*)PTR;                                   \
        } while (0)

#define NET_STR_TO_DOUBLE(DOUBLE_VALUE, PTR)	                        \
	do {					                        \
	  double	macro_var_tmp_value;		                \
	  memcpy((char*) &macro_var_tmp_value, PTR, NET_SIZE_DOUBLE);       \
	  macro_var_tmp_value = ntohd(macro_var_tmp_value);             \
	  DOUBLE_VALUE = macro_var_tmp_value;		                \
	} while (0)

#define NET_STR_TO_DATE(DATE_VAL, PTR)		                        \
	do {					                        \
	  short		macro_var_yr, macro_var_mon, macro_var_day;	\
	  int           pos = 0;                                        \
	  NET_STR_TO_SHORT(macro_var_yr, (PTR) + pos);		        \
	  pos += NET_SIZE_SHORT;                                            \
	  NET_STR_TO_SHORT(macro_var_mon, (PTR) + pos);	                \
	  pos += NET_SIZE_SHORT;                                            \
          NET_STR_TO_SHORT(macro_var_day, (PTR) + pos);	                \
	  (DATE_VAL).yr = macro_var_yr;			                \
	  (DATE_VAL).mon = macro_var_mon;			        \
	  (DATE_VAL).day = macro_var_day;			        \
	} while (0)

#define NET_STR_TO_TIME(TIME_VAL, PTR)		                        \
	do {					                        \
	  short	macro_var_hh, macro_var_mm, macro_var_ss;	        \
          int           pos = 0;                                        \
	  NET_STR_TO_SHORT(macro_var_hh, (PTR) + pos);                  \
          pos += NET_SIZE_SHORT;                                            \
	  NET_STR_TO_SHORT(macro_var_mm, (PTR) + pos);                  \
          pos += NET_SIZE_SHORT;                                            \
	  NET_STR_TO_SHORT(macro_var_ss, (PTR) + pos);                  \
	  (TIME_VAL).hh = macro_var_hh;			                \
	  (TIME_VAL).mm = macro_var_mm;			                \
	  (TIME_VAL).ss = macro_var_ss;			                \
	} while (0)

#define NET_STR_TO_MTIME(TIME_VAL, PTR)                                 \
        do {                                                            \
          short macro_var_hh, macro_var_mm, macro_var_ss, macro_var_ms; \
          int           pos = 0;                                        \
          NET_STR_TO_SHORT(macro_var_hh, (PTR) + pos);                  \
          pos += NET_SIZE_SHORT;                                            \
          NET_STR_TO_SHORT(macro_var_mm, (PTR) + pos);                  \
          pos += NET_SIZE_SHORT;                                            \
          NET_STR_TO_SHORT(macro_var_ss, (PTR) + pos);                  \
          pos += NET_SIZE_SHORT;                                            \
          NET_STR_TO_SHORT(macro_var_ms, (PTR) + pos);                  \
          (TIME_VAL).hh = macro_var_hh;                                 \
          (TIME_VAL).mm = macro_var_mm;                                 \
          (TIME_VAL).ss = macro_var_ss;                                 \
          (TIME_VAL).ms = macro_var_ms;                                 \
        } while (0)

#define NET_STR_TO_DATETIME(TS_VAL, PTR)                \
        do {                                            \
          NET_STR_TO_DATE((TS_VAL), (PTR));             \
          NET_STR_TO_MTIME((TS_VAL), (PTR) + NET_SIZE_DATE);\
        } while (0)

#define ADD_ARG_INT(BUF, VALUE)			\
	do {					\
	  net_buf_cp_int((BUF), NET_SIZE_INT);	\
	  net_buf_cp_int((BUF), (VALUE));	\
	} while (0)

#define ADD_ARG_INT64(BUF, VALUE)               \
        do {                                    \
          net_buf_cp_int((BUF), NET_SIZE_INT64);    \
          net_buf_cp_bigint((BUF), (VALUE));    \
        } while (0)

#define ADD_ARG_BIGINT(BUF, VALUE)  ADD_ARG_INT64(BUF, VALUE)


#define ADD_ARG_STR(BUF, STR, SIZE)		 \
 	ADD_ARG_BYTES(BUF, STR, SIZE);

#define ADD_ARG_BIND_STR(BUF, STR, SIZE) \
	do { \
	  net_buf_cp_int (BUF, SIZE); \
	  net_buf_cp_str (BUF, (const char*) STR, SIZE - 1); \
	  net_buf_cp_byte (BUF, '\0'); \
	} while (0)

#define ADD_ARG_BYTES(BUF, STR, SIZE)		\
	do {					\
	  net_buf_cp_int(BUF, SIZE);		\
	  net_buf_cp_str(BUF, (const char*) STR, SIZE);	\
	} while (0)

#define ADD_ARG_DOUBLE(BUF, VALUE)		\
	do {					\
	  net_buf_cp_int(BUF, NET_SIZE_DOUBLE);	\
	  net_buf_cp_double(BUF, VALUE);	\
	} while (0)

#define ADD_ARG_DATETIME(BUF, VALUE_P)		\
	do {					\
	  const T_CCI_DATETIME *macro_var_date_p = (const T_CCI_DATETIME*) (VALUE_P);  \
	  net_buf_cp_int(BUF, NET_SIZE_DATETIME);	\
	  net_buf_cp_short(BUF, macro_var_date_p->yr);	\
	  net_buf_cp_short(BUF, macro_var_date_p->mon);	\
	  net_buf_cp_short(BUF, macro_var_date_p->day);	\
	  net_buf_cp_short(BUF, macro_var_date_p->hh);	\
	  net_buf_cp_short(BUF, macro_var_date_p->mm);	\
	  net_buf_cp_short(BUF, macro_var_date_p->ss);	\
          net_buf_cp_short(BUF, macro_var_date_p->ms);  \
	} while (0)

#define ADD_ARG_CACHE_TIME(BUF, SEC, USEC)	\
	do {					\
	  net_buf_cp_int(BUF, NET_SIZE_INT*2);	\
	  net_buf_cp_int(BUF, SEC);		\
	  net_buf_cp_int(BUF, USEC);		\
	} while (0)

#define TUPLE_VALUE_FREE(PTR, NUM_TUPLE, NUM_COL)			\
	do {								\
	  tuple_value_free(PTR, NUM_TUPLE, NUM_COL);			\
	  PTR = NULL;							\
	} while (0)

/************************************************************************
 * EXPORTED TYPE DEFINITIONS						*
 ************************************************************************/

/************************************************************************
 * EXPORTED FUNCTION PROTOTYPES						*
 ************************************************************************/

extern int qe_con_close (T_CON_HANDLE * con_handle);
extern int qe_prepare (T_REQ_HANDLE * req_handle,
		       T_CON_HANDLE * con_handle,
		       const char *sql_stmt, char flag, int reuse);
extern void qe_bind_value_free (T_REQ_HANDLE * req_handle);
extern int qe_bind_param (T_REQ_HANDLE * req_handle, int index,
			  T_CCI_TYPE a_type, const void *value, int length,
			  char flag);
extern int qe_execute (T_REQ_HANDLE * req_handle, T_CON_HANDLE * con_handle,
		       char flag, int max_col_size, int group_id);
extern int qe_end_tran (T_CON_HANDLE * con_handle, char type);
extern int qe_get_db_parameter (T_CON_HANDLE * con_handle,
				T_CCI_DB_PARAM param_name, void *value);
extern int qe_close_query_result (T_REQ_HANDLE * req_handle,
				  T_CON_HANDLE * con_handle);
extern int qe_close_req_handle (T_REQ_HANDLE * req_handle,
				T_CON_HANDLE * con_handle);
extern void qe_close_req_handle_all (T_CON_HANDLE * con_handle);
extern int qe_cursor (T_REQ_HANDLE * req_handle, int offset, char origin);
extern int qe_fetch (T_REQ_HANDLE * req_handle, T_CON_HANDLE * con_handle,
		     int result_set_index);
extern int qe_get_data (T_REQ_HANDLE * req_handle,
			int col_no, int a_type, void *value, int *indicator);
extern int qe_schema_info (T_REQ_HANDLE * req_handle,
			   T_CON_HANDLE * con_handle,
			   int type, char *arg1, char *arg2, int flag);
extern int qe_get_db_version (T_CON_HANDLE * con_handle,
			      char *out_buf, int buf_size);

extern int qe_execute_batch (T_REQ_HANDLE * req_handle,
			     T_CON_HANDLE * con_handle,
			     T_CCI_QUERY_RESULT ** qr);
extern void qe_query_result_free (int num_q, T_CCI_QUERY_RESULT * qr);
extern int qe_query_result_copy (T_REQ_HANDLE * req_handle,
				 T_CCI_QUERY_RESULT ** res_qr);

extern int qe_get_query_plan (T_CON_HANDLE * con_handle, const char *sql,
			      char **out_buf);

extern void tuple_value_free (T_TUPLE_VALUE * tuple_value,
			      int num_tuple, int num_cols);
extern int qe_update_db_group_id (T_CON_HANDLE * con_handle, int migrator_id,
				  int group_id, int target, bool on_off);

extern int qe_insert_gid_removed_info (T_CON_HANDLE * con_handle,
				       int group_id);
extern int qe_delete_gid_removed_info (T_CON_HANDLE * con_handle,
				       int group_id);
extern int qe_delete_gid_skey_info (T_CON_HANDLE * con_handle, int group_id);
extern int qe_block_global_dml (T_CON_HANDLE * con_handle, bool start_or_end);
extern int qe_get_server_mode (T_CON_HANDLE * con_handle, int *mode,
			       unsigned int *master_addr);
extern int qe_send_repl_data (T_CON_HANDLE * con_handle,
			      CIRP_REPL_ITEM * head, int num_items);
extern int qe_notify_ha_agent_state (T_CON_HANDLE * con_handle,
				     in_addr_t ip, int port, int state);
extern int qe_change_dbuser (T_CON_HANDLE * con_handle, const char *user,
			     const char *passwd);

/************************************************************************
 * EXPORTED VARIABLES							*
 ************************************************************************/
#endif /* _CCI_QUERY_EXECUTE_H_ */
