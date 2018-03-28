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
 * backup_sr.h: backup module (at server)
 */

#ifndef BACKUP_SR_H_
#define BACKUP_SR_H_

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
#include "backup.h"

extern int xbk_prepare_backup (THREAD_ENTRY * thread_p, int num_threads,
			       int do_compress, int sleep_msecs,
			       int make_slave, BK_BACKUP_SESSION * session);
extern int xbk_backup_volume (THREAD_ENTRY * thread_p, int rid, int buf_size);
extern int xbk_backup_log_volume (THREAD_ENTRY * thread_p, int rid,
				  int buf_size,
				  int delete_unneeded_logarchives);

#endif /* BACKUP_SR_H_ */
