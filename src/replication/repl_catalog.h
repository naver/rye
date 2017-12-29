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

#ifndef _CCI_REPL_CATALOG_H_
#define _CCI_REPL_CATALOG_H_

#ident "$Id$"

#include <unistd.h>

#include "dbtype.h"
#include "log_impl.h"

#include "repl.h"

#include "cas_cci.h"


extern int rpct_get_log_analyzer (CCI_CONN * conn,
				  CIRP_CT_LOG_ANALYZER * ct_data,
				  const PRM_NODE_INFO * host_info);
extern int rpct_insert_log_analyzer (CCI_CONN * conn,
				     CIRP_CT_LOG_ANALYZER * ct_data);
extern int rpct_update_log_analyzer (CCI_CONN * conn,
				     CIRP_CT_LOG_ANALYZER * ct_data);
extern int rpct_analyzer_to_catalog_item (RP_CATALOG_ITEM * catalog,
					  RECDES * recdes,
					  CIRP_CT_LOG_ANALYZER * ct_data);

extern int rpct_init_applier_info (CCI_CONN * conn,
				   const PRM_NODE_INFO * host_ip);
extern int rpct_insert_log_applier (CCI_CONN * conn,
				    CIRP_CT_LOG_APPLIER * ct_data);
extern int rpct_update_log_applier (CCI_CONN * conn,
				    CIRP_CT_LOG_APPLIER * ct_data);
extern int rpct_applier_to_catalog_item (RP_CATALOG_ITEM * catalog,
					 RECDES * recdes,
					 CIRP_CT_LOG_APPLIER * ct_data);


extern int cirp_execute_query (CCI_CONN * conn, CCI_STMT * stmt, char *query);


#endif /* _CCI_REPL_CATALOG_H_ */
