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
 * heartbeat.h -
 */

#ifndef _HEARTBEAT_H_
#define _HEARTBEAT_H_

#ident "$Id$"

#include "connection_defs.h"
#include "utility.h"

/* heartbeat */
#define HB_DEFAULT_HA_PORT_ID                           (59901)
#define HB_DEFAULT_APPLY_MAX_MEM_SIZE                   (500)

#define HB_DEFAULT_CHECK_VALID_PING_SERVER_INTERVAL           (1*ONE_HOUR)
#define HB_TEMP_CHECK_VALID_PING_SERVER_INTERVAL              (5*ONE_MIN)
#define HB_MIN_DIFF_CHECK_DISK_FAILURE_INTERVAL         (10*ONE_SEC)


#define HB_JOB_TIMER_IMMEDIATELY                        (0)
#define HB_JOB_TIMER_WAIT_A_SECOND                      (1*ONE_SEC)
#define HB_JOB_TIMER_WAIT_500_MILLISECOND               (5*100)
#define HB_JOB_TIMER_WAIT_100_MILLISECOND               (1*100)
#define HB_DISK_FAILURE_CHECK_TIMER                     (1*100)

#define HB_STOP_WAITING_TIME			        (1*ONE_SEC)

/* heartbeat resource process type */
typedef enum hb_proc_type HB_PROC_TYPE;
enum hb_proc_type
{
  HB_PTYPE_SERVER = 0,
  HB_PTYPE_REPLICATION = 1,
  HB_PTYPE_MAX
};

typedef enum hb_proc_command HB_PROC_COMMAND;
enum hb_proc_command
{
  HB_PCMD_START = 0,
  HB_PCMD_STOP = 1,
  HB_PCMD_MAX
};

#define HB_PTYPE_SERVER_STR             "rye_server"
#define HB_PTYPE_REPLICATION_STR        "rye_repl"
#define HB_PTYPE_STR_SZ                 (16)

enum HBP_CLUSTER_MESSAGE
{
  HBP_CLUSTER_HEARTBEAT = 0,
  HBP_CLUSTER_MSG_MAX
};

#define HB_MAX_GROUP_ID_LEN		(64)
#define HB_MAX_SZ_PROC_EXEC_PATH        (128)
#define HB_MAX_NUM_PROC_ARGV            (16)
#define HB_MAX_SZ_PROC_ARGV             (64)
#define HB_MAX_SZ_PROC_ARGS             (HB_MAX_NUM_PROC_ARGV*HB_MAX_SZ_PROC_ARGV)


/*
 * heartbeat cluster message header and body
 */

/* heartbeat net header */
typedef struct hbp_header HBP_HEADER;
struct hbp_header
{
  unsigned char type;
  char reserved:7;
  char r:1;			/* is request? */
  unsigned short len;
  unsigned int seq;
  char group_id[HB_MAX_GROUP_ID_LEN];
  char orig_host_name[MAXHOSTNAMELEN];
  char dest_host_name[MAXHOSTNAMELEN];
};


/*
 * heartbeat resource message body
 */

/* process register */
typedef struct hbp_proc_register HBP_PROC_REGISTER;
struct hbp_proc_register
{
  int pid;
  HB_PROC_TYPE type;
  char exec_path[HB_MAX_SZ_PROC_EXEC_PATH];
  char args[HB_MAX_SZ_PROC_ARGS];
  char argv[HB_MAX_NUM_PROC_ARGV][HB_MAX_SZ_PROC_ARGV];
};


/*
 * externs
 */
extern const char *hb_process_type_string (int ptype);
extern void hb_set_exec_path (char *exec_path);
extern void hb_set_argv (char **argv);
extern int css_send_heartbeat_request (CSS_CONN_ENTRY * conn, int command,
				       int num_buffers, ...);
extern int css_receive_heartbeat_request (CSS_CONN_ENTRY * conn,
					  CSS_NET_PACKET ** recv_packet);
extern int hb_process_master_request (void);
extern int hb_register_to_master (CSS_CONN_ENTRY * conn, HB_PROC_TYPE type);
extern int hb_process_init (const char *server_name, const char *log_path,
			    HB_PROC_TYPE type);
extern void hb_process_term (void);
extern int hb_make_hbp_register (HBP_PROC_REGISTER * hbp_register,
				 const HA_CONF * ha_conf,
				 HB_PROC_TYPE proc_type,
				 HB_PROC_COMMAND command_type,
				 const char *db_name, const char *host_ip);

extern bool hb_Proc_shutdown;


#endif /* _HEARTBEAT_H_ */
