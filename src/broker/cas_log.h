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
 * cas_log.h -
 */

#ifndef	_CAS_LOG_H_
#define	_CAS_LOG_H_

#ident "$Id$"

#include "broker_shm.h"

#define CAS_LOG_FLAG_NO_FLAG            0
#define CAS_LOG_FLAG_PRINT_HEADER       1
#define CAS_LOG_FLAG_PRINT_NL           2
#define CAS_LOG_FLAG_LOG_END            4

typedef enum
{
  NEW_CONNECTION,
  CLIENT_CHANGED,
  ACL_REJECTED
} ACCESS_LOG_TYPE;

typedef enum
{
  CAS_LOG_SQL_LOG = 0,
  CAS_LOG_SLOW_LOG,
  CAS_LOG_ACCESS_LOG,
  CAS_LOG_DENIED_ACCESS_LOG,
  CAS_LOG_TYPE_MAX = CAS_LOG_DENIED_ACCESS_LOG
} T_CAS_LOG_TYPE;

extern void cas_log_init (T_SHM_APPL_SERVER * shm_p, T_APPL_SERVER_INFO * as_info_p, int id);
extern void cas_log_close_all (void);

extern int cas_access_log (struct timeval *start_time, int as_index,
                           in_addr_t client_ip, char *dbname, char *dbuser, ACCESS_LOG_TYPE log_type);
extern void cas_log_write (T_CAS_LOG_TYPE cas_log_type, int flag,
                           struct timeval *log_time, unsigned int seq_num, const char *fmt, ...);
extern void cas_log_write_string (T_CAS_LOG_TYPE cas_log_type, char *value, int size, bool print_nl);

extern void cas_sql_log_end (bool flush, int run_time_sec, int run_time_msec);
extern void cas_sql_log_reset (void);
extern void cas_sql_log_set_query_cancel_time (INT64 cancel_time);

#define cas_sql_log_write(SEQ_NUM, ...)					\
  cas_log_write (CAS_LOG_SQL_LOG,					\
		 CAS_LOG_FLAG_PRINT_HEADER | CAS_LOG_FLAG_PRINT_NL,	\
		 NULL, SEQ_NUM, __VA_ARGS__)

#define cas_sql_log_write_with_ts(LOG_TIME, SEQ_NUM, ...)		\
  cas_log_write (CAS_LOG_SQL_LOG,					\
		 CAS_LOG_FLAG_PRINT_HEADER | CAS_LOG_FLAG_PRINT_NL,	\
		 LOG_TIME, SEQ_NUM, __VA_ARGS__)

#define cas_sql_log_write_nonl(SEQ_NUM, ...)				\
  cas_log_write (CAS_LOG_SQL_LOG,					\
  		 CAS_LOG_FLAG_PRINT_HEADER,				\
		 NULL, SEQ_NUM, __VA_ARGS__)

#define cas_sql_log_write_and_end(SEQ_NUM, ...)				\
  cas_log_write (CAS_LOG_SQL_LOG,					\
		 CAS_LOG_FLAG_PRINT_HEADER | CAS_LOG_FLAG_LOG_END,	\
		 NULL, SEQ_NUM, __VA_ARGS__)

#define cas_log_write2(CAS_LOG_TYPE, ...)					\
  cas_log_write (CAS_LOG_TYPE,					\
  		 CAS_LOG_FLAG_NO_FLAG,					\
		 NULL, 0, __VA_ARGS__)

extern void cas_slow_log_reset (void);
extern void cas_slow_log_end (void);
#define cas_slow_log_write(LOG_TIME, SEQ_NUM, ...)			\
  cas_log_write (CAS_LOG_SLOW_LOG,					\
		 CAS_LOG_FLAG_PRINT_HEADER | CAS_LOG_FLAG_PRINT_NL,	\
		 LOG_TIME, SEQ_NUM, __VA_ARGS__)

#define cas_slow_log_write_nonl(LOG_TIME, SEQ_NUM, ...)			\
  cas_log_write (CAS_LOG_SLOW_LOG, CAS_LOG_FLAG_PRINT_HEADER,		\
		 LOG_TIME, SEQ_NUM, __VA_ARGS__)

#define cas_slow_log_write_and_end(LOG_TIME, SEQ_NUM, ...)		\
  cas_log_write (CAS_LOG_SLOW_LOG,					\
		 CAS_LOG_FLAG_PRINT_HEADER | CAS_LOG_FLAG_LOG_END,	\
		 LOG_TIME, SEQ_NUM, __VA_ARGS__)

extern bool sql_log_Notice_mode_flush;

#endif /* _CAS_LOG_H_ */
