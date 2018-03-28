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
 * restore.h: restore module
 */

#ifndef RESTORE_H_
#define RESTORE_H_

#include "config.h"

#include <stdio.h>
#include <time.h>

#include "porting.h"
#include "storage_common.h"
#include "dbtype.h"
#include "thread.h"
#include "file_io.h"

extern int bk_get_backup_volume (THREAD_ENTRY * thread_p,
                                 const char *db_fullname, const char *user_backuppath, char *from_volbackup);
extern int bk_list_restore (THREAD_ENTRY * thread_p, const char *db_full_name_p, char *backup_source_p);
extern int bk_restore (THREAD_ENTRY * thread_p, const char *db_fullname,
                       const char *logpath, const char *prefix_logname, BO_RESTART_ARG * r_args);
#endif /* RESTORE_H_ */
