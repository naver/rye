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
 * execute_statement.h -
 */

#ifndef _EXECUTE_STATEMENT_H_
#define _EXECUTE_STATEMENT_H_

#ident "$Id$"

#include "dbi.h"
#include "parser.h"

extern int do_alter (PARSER_CONTEXT * parser, PT_NODE * statement);

extern int do_alter_index (PARSER_CONTEXT * parser, const PT_NODE * statement);
extern int do_create_index (PARSER_CONTEXT * parser, const PT_NODE * statement);
extern int do_drop_index (PARSER_CONTEXT * parser, const PT_NODE * statement);

extern int do_commit (PARSER_CONTEXT * parser, PT_NODE * statement);
extern int do_get_optimization_param (PARSER_CONTEXT * parser, PT_NODE * statement);
extern int do_get_xaction (PARSER_CONTEXT * parser, PT_NODE * statement);
extern int do_rollback (PARSER_CONTEXT * parser, PT_NODE * statement);
extern int do_savepoint (PARSER_CONTEXT * parser, PT_NODE * statement);
extern int do_set_optimization_param (PARSER_CONTEXT * parser, PT_NODE * statement);
extern int do_set_sys_params (PARSER_CONTEXT * parser, PT_NODE * statement);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int do_set_xaction (PARSER_CONTEXT * parser, PT_NODE * statement);
#endif

extern int do_prepare_delete (DB_SESSION * session, PT_NODE * statement, PT_NODE * parent);
extern int do_execute_delete (DB_SESSION * session, PT_NODE * statement);

extern int do_drop (PARSER_CONTEXT * parser, PT_NODE * statement);

extern int do_grant (const PARSER_CONTEXT * parser, const PT_NODE * statement);
extern int do_revoke (const PARSER_CONTEXT * parser, const PT_NODE * statement);
extern int do_create_user (const PARSER_CONTEXT * parser, const PT_NODE * statement);
extern int do_drop_user (const PARSER_CONTEXT * parser, const PT_NODE * statement);
extern int do_alter_user (const PARSER_CONTEXT * parser, const PT_NODE * statement);

extern int do_prepare_insert (DB_SESSION * session, PT_NODE * statement);
extern int do_execute_insert (DB_SESSION * session, PT_NODE * statement);

extern int do_rename (const PARSER_CONTEXT * parser, const PT_NODE * statement);

extern int do_prepare_select (DB_SESSION * session, PT_NODE * statement);
extern int do_execute_select (DB_SESSION * session, PT_NODE * statement);

extern int do_prepare_update (DB_SESSION * session, PT_NODE * statement);
extern int do_execute_update (DB_SESSION * session, PT_NODE * statement);

extern int do_update_stats (PARSER_CONTEXT * parser, PT_NODE * statement);

extern int do_replicate_schema (PARSER_CONTEXT * parser, PT_NODE * statement);

extern int do_prepare_statement (DB_SESSION * session, PT_NODE * statement);
extern int do_execute_statement (DB_SESSION * session, PT_NODE * statement);
#if defined (ENABLE_UNUSED_FUNCTION)
extern int do_evaluate_default_expr (PARSER_CONTEXT * parser, PT_NODE * class_name);
#endif
extern bool is_schema_repl_log_statment (const PT_NODE * node);

extern int do_set_query_trace (PARSER_CONTEXT * parser, PT_NODE * statement);

extern int check_for_default_expr (PARSER_CONTEXT * parser,
                                   PT_NODE * specified_attrs, PT_NODE ** default_expr_attrs, DB_OBJECT * class_obj);
#endif /* _EXECUTE_STATEMENT_H_ */
