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
 * cas.h -
 */

#ifndef	_CAS_H_
#define	_CAS_H_

#ident "$Id$"

#ifndef CAS
#define CAS
#endif

#include "broker_shm.h"
#include "cas_protocol.h"
#include "cas_cci.h"
#include "cas_common.h"

#define ERROR_INDICATOR_UNSET	0
#define CAS_ERROR_INDICATOR	-1
#define DBMS_ERROR_INDICATOR	-2
#define CAS_NO_ERROR		0
#define ERR_MSG_LENGTH		1024
#define ERR_FILE_LENGTH		256
#define MAX_SHARD_INFO_LENGTH   30

#define MAX_HA_DBINFO_LENGTH    (SRV_CON_DBNAME_SIZE + MAX_CONN_INFO_LENGTH)

enum tran_auto_commit
{
  TRAN_NOT_AUTOCOMMIT = 0,
  TRAN_AUTOCOMMIT = 1,
  TRAN_AUTOROLLBACK = 2
};

typedef struct t_req_info T_REQ_INFO;
struct t_req_info
{
  T_BROKER_RYE_VERSION clt_version;
  enum tran_auto_commit need_auto_commit;
  char need_rollback;
};

typedef struct t_error_info T_ERROR_INFO;
struct t_error_info
{
  int err_indicator;
  int err_number;
  char err_string[ERR_MSG_LENGTH];
  char err_file[ERR_FILE_LENGTH];
  int err_line;
};

extern int restart_is_needed (void);

extern const char *program_Name;

extern int shm_As_index;
extern T_SHM_APPL_SERVER *shm_Appl;
extern T_APPL_SERVER_INFO *as_Info;

extern T_SHM_BROKER *shm_Br_master;
extern bool repl_Agent_connected;

extern struct timeval tran_Start_time;
extern struct timeval query_Start_time;
extern char query_Cancel_flag;

extern int cas_Info_size;

extern T_ERROR_INFO err_Info;

extern SOCKET new_Req_sock_fd;

extern int query_seq_num_next_value (void);
extern int query_seq_num_current_value (void);

extern void set_hang_check_time (void);
extern void unset_hang_check_time (void);

extern bool check_server_alive (const char *db_name, const char *db_host);

extern void cas_set_db_connect_status (int status);
extern int cas_get_db_connect_status (void);

extern void query_cancel_enable_sig_handler (void);
extern void query_cancel_disable_sig_handler (void);
extern int64_t cas_shard_info_version (void);

#endif /* _CAS_H_ */
