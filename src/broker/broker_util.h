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
 * broker_util.h -
 */

#ifndef	_BROKER_UTIL_H_
#define	_BROKER_UTIL_H_

#ident "$Id$"

#include <time.h>

#include "cas_common.h"
#include "porting.h"
#include "broker_config.h"

#define	SERVICE_READY_WAIT_COUNT	6000

#define MAKE_FILEPATH(dest,src,dest_len) \
  do { \
      char _buf[BROKER_PATH_MAX]; \
      if ((src) == NULL || (src)[0] == 0) { \
	  (dest)[0] = 0; \
      } else if (realpath ((src), _buf) != NULL) { \
	  strncpy ((dest), _buf, (dest_len)); \
	  (dest)[(dest_len) - 1] = 0; \
      } else { \
	  strncpy ((dest), (src), (dest_len)); \
	  (dest)[(dest_len) - 1] = 0; \
      } \
  } while (0)

extern int ut_kill_process (int pid);
extern void ut_kill_broker_process (const T_BROKER_INFO * br_info);
extern int ut_kill_as_process (int pid, char *br_name, int as_id);

extern void ut_cd_work_dir (void);
extern void ut_cd_root_dir (void);

extern int ut_set_keepalive (int sock);

extern void as_pid_file_create (char *br_name, int as_index);

extern void ut_get_as_pid_name (char *pid_name, char *br_name, int as_index,
				int len);

extern char *ut_get_ipv4_string (char *ip_str, int len, in_addr_t ip_addr);
extern float ut_get_avg_from_array (int array[], int size);
extern bool ut_is_appl_server_ready (int pid, char *ready_flag);
extern int ut_get_broker_port_name (char *port_name, size_t len,
				    const T_BROKER_INFO * br_info);
extern void ut_get_as_port_name (char *port_name, const char *broker_name,
				 int as_id, int len);

extern double ut_size_string_to_kbyte (const char *size_str,
				       const char *default_unit);
extern double ut_time_string_to_sec (const char *time_str,
				     const char *default_unit);
extern T_BROKER_INFO *ut_find_broker (T_BROKER_INFO * br_info, int num_brs,
				      const char *brname, char broker_type);
extern T_BROKER_INFO *ut_find_shard_mgmt (T_BROKER_INFO * br_info,
					  int num_brs, const char *dbname);
#endif /* _BROKER_UTIL_H_ */
