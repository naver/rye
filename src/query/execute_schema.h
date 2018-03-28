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
 * execute_schema.h - This file contains do_class and do_partition extern prototypes.
 */

#ifndef _EXECUTE_SCHEMA_H_
#define _EXECUTE_SCHEMA_H_

#ident "$Id$"

#include "dbi.h"
#include "query_executor.h"
#include "schema_manager.h"

#if 0                           /* not used; do not delete me */
#define PARTITION_CATALOG_CLASS "_db_partition"
#define PARTITION_ATT_CLASSOF "class_of"
#define PARTITION_ATT_PNAME "pname"
#define PARTITION_ATT_PTYPE "ptype"
#define PARTITION_ATT_PEXPR "pexpr"
#define PARTITION_ATT_PVALUES "pvalues"
#endif

extern int do_add_queries (PARSER_CONTEXT * parser, DB_CTMPL * ctemplate, const PT_NODE * queries);
extern int do_add_attributes (PARSER_CONTEXT * parser,
                              DB_CTMPL * ctemplate, PT_NODE * atts, PT_NODE * constraints, PT_NODE * shard_key);
extern int do_add_constraints (DB_CTMPL * ctemplate, PT_NODE * constraints);
extern int do_create_entity (DB_SESSION * session, PT_NODE * node);
#if defined (ENABLE_UNUSED_FUNCTION)    /* TODO - delete me */
extern int do_check_rows_for_null (MOP class_mop, const char *att_name, bool * has_nulls);
#endif

#endif /* _EXECUTE_SCHEMA_H_ */
