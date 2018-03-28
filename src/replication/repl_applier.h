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
 * repl_applier.h - the header file of replication module
 *
 */

#ifndef _REPL_APPLIER_H_
#define _REPL_APPLIER_H_

#ident "$Id$"

#include "config.h"

#include "repl_page_buffer.h"
#include "repl_catalog.h"
#include "repl_queue.h"
#include "cas_cci.h"

extern void *applier_main (void *arg);
extern int cirp_init_applier (CIRP_APPLIER_INFO * applier, const char *database_name, const char *log_path);
extern int cirp_clear_applier (CIRP_APPLIER_INFO * applier);
extern int cirp_final_applier (CIRP_APPLIER_INFO * applier);
extern int cirp_appl_get_committed_lsa (CIRP_APPLIER_INFO * applier, LOG_LSA * committed_lsa);
CIRP_AGENT_STATUS cirp_get_applier_status (CIRP_APPLIER_INFO * applier);

#endif /* _REPL_APPLIER_H_ */
