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
 * broker_acl.h -
 */

#ifndef	_BROKER_ACL_H_
#define	_BROKER_ACL_H_

#ident "$Id$"

#include "broker_shm.h"

#define MAX_IP_SECT_NAME		64
#define MAX_NUM_IP_SECT_CONF		256
#define MAX_NUM_ACL_GROUP_CONF		256

typedef struct
{
  char ip_sect_name[MAX_IP_SECT_NAME];
  int num_acl_ip_info;
  ACL_IP_INFO acl_ip_info[ACL_MAX_IP_COUNT];
} BR_ACL_IP_SECTION;

typedef struct
{
  char broker_name[BROKER_NAME_LEN];
  char dbname[ACL_MAX_DBNAME_LENGTH];
  char dbuser[ACL_MAX_DBUSER_LENGTH];
  char ip_sect_name[MAX_IP_SECT_NAME];
} BR_ACL_GROUP;

typedef struct
{
  int num_acl_group;
  BR_ACL_GROUP acl_group[MAX_NUM_ACL_GROUP_CONF];
  int num_ip_sect;
  BR_ACL_IP_SECTION ip_sect[MAX_NUM_IP_SECT_CONF];
} BROKER_ACL_CONF;

extern int br_acl_init_shm (T_SHM_APPL_SERVER * shm_as_cp,
			    T_BROKER_INFO * br_info_p,
			    T_SHM_BROKER * shm_br, char *admin_err_msg,
			    const BROKER_ACL_CONF * br_acl_conf);
extern int br_acl_set_shm (T_SHM_APPL_SERVER * shm_appl,
			   BR_ACL_INFO * ret_acl_info,
			   const BROKER_ACL_CONF * br_acl_conf,
			   const char *broker_name, char *admin_err_msg);
extern int br_acl_read_config_file (BROKER_ACL_CONF * br_acl_conf,
				    const char *filename,
				    bool make_default_acl_file,
				    char *admin_err_msg);

extern int br_acl_check_right (T_SHM_APPL_SERVER * shm_as_p,
			       BR_ACL_INFO * acl_info, int num_acl_info,
			       const char *dbname, const char *dbuser,
			       unsigned char *address);
extern void br_acl_dump (FILE * fp, BR_ACL_INFO * acl_list);
extern int br_acl_conf_read_ip_addr (ACL_IP_INFO * ip_info, char *linebuf,
				     char *admin_err_msg,
				     const char *err_line_info);

#endif /* _BROKER_ACL_H_ */
