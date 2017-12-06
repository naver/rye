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
 * repl_log_writer_sr.h - DECLARATIONS FOR LOG WRITER (AT CLIENT & SERVER)
 */

#ifndef _REPL_LOG_WRITER_SR_HEADER_
#define _REPL_LOG_WRITER_SR_HEADER_

#ident "$Id$"

#include <stdio.h>
#include "log_impl.h"

extern int xlogwr_get_log_pages (THREAD_ENTRY * thread_p,
				 LOG_PAGEID first_pageid,
				 bool compressed_protocol);
extern LOG_PAGEID logwr_get_min_copied_fpageid (void);
extern LOGWR_ENTRY *logwr_find_copy_completed_entry (LOGWR_INFO *
						     writer_info);
extern LOGWR_ENTRY *logwr_find_entry_status (LOGWR_INFO * writer_info,
					     LOGWR_STATUS status);

extern int xmigrator_get_log_pages (THREAD_ENTRY * thread_p,
				    LOG_PAGEID first_pageid,
				    bool compressed_protocol);

#endif /* _REPL_LOG_WRITER_SR_HEADER_ */
