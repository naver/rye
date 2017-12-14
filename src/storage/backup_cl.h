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
 * backup_cl.h: backup module (at client)
 */

#ifndef BACKUP_CL_H_
#define BACKUP_CL_H_

#include "config.h"

#include <stdio.h>
#include <time.h>

#include "porting.h"
#include "storage_common.h"
#include "release_string.h"
#include "dbtype.h"
#include "memory_hash.h"
#include "lzoconf.h"
#include "lzo1x.h"
#include "thread.h"
#include "file_io.h"
#include "connection_defs.h"
#include "backup.h"

extern int bk_run_backup (char *db_name, const PRM_NODE_INFO * db_host_info,
			  const char *backup_path,
			  const char *backup_verbose_file_path,
			  int num_threads, int do_compress,
			  int sleep_msecs,
			  int delete_unneeded_logarchives,
			  bool force_overwrite, int make_slave,
			  HA_STATE server_state);
extern int bk_write_backup (BK_BACKUP_SESSION * session_p,
			    ssize_t to_write_nbytes, int unzip_nbytes);
extern void bk_start_vol_in_backup (BK_BACKUP_SESSION * session,
				    int vol_type);
extern void bk_end_vol_in_backup (BK_BACKUP_SESSION * session);
#endif /* BACKUP_CL_H_ */
