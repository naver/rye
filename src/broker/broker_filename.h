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
 * broker_filename.h -
 */

#ifndef _BROKER_FILENAME_H_
#define _BROKER_FILENAME_H_

#ident "$Id$"

#include "porting.h"

#define APPL_SERVER_CAS_NAME            "rye_cas"
#define NAME_CAS_BROKER			"rye_broker"

#define BROKER_PATH_MAX             (PATH_MAX)

/* default values */
#define DEFAULT_LOG_DIR			"broker/"
#define SQL_LOG_DIR			"sql_log/"
#define SLOW_LOG_DIR			"slow_log/"
#define ERR_LOG_DIR			"error_log/"

#define BROKER_ACL_FILE			"rye_broker.acl"

#define ACCESS_LOG_FILENAME_POSTFIX		".access"
#define ACCESS_LOG_DENIED_FILENAME_POSTFIX	".denied"

typedef enum t_rye_file_id T_RYE_FILE_ID;
enum t_rye_file_id
{
  FID_RYE_BROKER_CONF = 0,
  FID_CAS_TMP_DIR,
  FID_VAR_DIR,
  FID_SOCK_DIR,
  FID_AS_PID_DIR,
  FID_LOG_DIR,
  FID_SQL_LOG_DIR,
  FID_ERR_LOG_DIR,
  FID_ACCESS_CONTROL_FILE,
  FID_SLOW_LOG_DIR,
  MAX_RYE_FILE
};

typedef struct t_rye_file_info T_RYE_FILE_INFO;
struct t_rye_file_info
{
  T_RYE_FILE_ID fid;
  char file_name[BROKER_PATH_MAX];
};

extern void rye_file_reset (void);

extern void set_rye_file (T_RYE_FILE_ID fid, const char *value,
			  const char *br_name);
extern char *get_rye_file (T_RYE_FILE_ID fid, char *buf, size_t len);

#endif /* _BROKER_FILENAME_H_ */
