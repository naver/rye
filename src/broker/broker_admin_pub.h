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
 * broker_admin_pub.h -
 */

#ifndef	_BROKER_ADMIN_PUB_H_
#define	_BROKER_ADMIN_PUB_H_

#ident "$Id$"

#include "broker_config.h"
#include "environment_variable.h"

extern int broker_admin (int command_type, int argc, char **argv);
extern int broker_monitor (int argc, char **argv);
extern int broker_tester (int argc, char **argv);
extern int broker_is_running (bool * running);
extern int broker_get_local_mgmt_info (char **broker_key, int *port);

extern int admin_start_cmd (T_BROKER_INFO *, int, int);
extern int admin_stop_cmd (int);
extern int admin_on_cmd (int, const char *);
extern int admin_off_cmd (int, const char *);
extern int admin_reset_cmd (int, const char *);
extern int admin_info_cmd (int);
extern int admin_conf_change (int, const char *, const char *, const char *, int);
extern int admin_acl_status_cmd (int shm_key_br_gl, const char *broker_name);
extern int admin_acl_reload_cmd (int shm_key_br_gl, const char *new_acl_file);
extern int admin_acl_test_cmd (int argc, char **argv);
extern int admin_get_broker_key_and_portid (char **broker_key, int *port,
                                            int shm_key_br_gl, const char *broker_name, char broker_type);


void admin_init_env (void);

extern char admin_Err_msg[];

#endif /* _BROKER_ADMIN_PUB_H_ */
