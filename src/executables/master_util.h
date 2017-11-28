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
 * master_util.h : common module for commdb and master
 *
 */

#ifndef _MASTER_UTIL_H_
#define _MASTER_UTIL_H_

#ident "$Id$"

#include "thread.h"
#include "connection_defs.h"
#include "rye_shm.h"

typedef struct socket_queue_entry SOCKET_QUEUE_ENTRY;
struct socket_queue_entry
{
  SOCKET fd;
  int fd_type;
  int db_error;
  int queue_p;
  int error_p;
  int pid;
  char *name;
  char *env_var;
  CSS_CONN_ENTRY *conn_ptr;
  struct socket_queue_entry *next;
};

#endif /* _MASTER_UTIL_H_ */
