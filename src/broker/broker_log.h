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
 * cas_log.h -
 */

#ifndef	_BROKER_LOG_H_
#define	_BROKER_LOG_H_

#ident "$Id$"

#include "broker_shm.h"

extern void br_log_init (T_SHM_APPL_SERVER * shm_p);
extern void br_log_write (T_BROKER_LOG_SEVERITY severity,
			  in_addr_t clt_ip, const char *fmt, ...);
extern void br_log_check (void);
extern int br_log_hang_time (void);

#endif /* _BROKER_LOG_H_ */
