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
 * file_io.h - I/O lock module at server
 *
 */

#ifndef _FILE_IO_LOCK_H_
#define _FILE_IO_LOCK_H_

#ident "$Id$"

typedef enum
{
  FILEIO_LOCKF,
//  FILEIO_RUN_AWAY_LOCKF,
  FILEIO_NOT_LOCKF
} FILEIO_LOCKF_TYPE;

extern FILEIO_LOCKF_TYPE fileio_lock_la_log_path (const char *db_fullname,
						  const char *lock_path,
						  int vdes);
extern FILEIO_LOCKF_TYPE fileio_lock_la_dbname (int *lockf_vdes,
						char *db_name,
						char *log_path);
extern FILEIO_LOCKF_TYPE fileio_unlock_la_dbname (int *lockf_vdes,
						  char *db_name,
						  bool clear_owner);
extern int fileio_get_lock (int fd, const char *vol_label_p);
extern int fileio_get_lock_retry (int fd, const char *vol_label_p);

extern FILEIO_LOCKF_TYPE fileio_lock (const char *db_fullname,
				      const char *vlabel, int vdes,
				      bool dowait);
extern void fileio_unlock (const char *vol_label_p, int vol_fd,
			   FILEIO_LOCKF_TYPE lockf_type);
extern int fileio_release_lock (int fd);
#endif /* _FILE_IO_LOCK_H_ */
