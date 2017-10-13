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
 * view_transform.h - view (virtual class) transformation
 */

#ifndef _VIEW_TRANSFORM_H_
#define _VIEW_TRANSFORM_H_

#ident "$Id$"

#include "parser.h"
#include "schema_manager.h"

extern PT_NODE *mq_bump_correlation_level (PARSER_CONTEXT * parser,
					   PT_NODE * node, int increment,
					   int match);

extern PT_NODE *mq_translate (PARSER_CONTEXT * parser, PT_NODE * node);

extern PT_NODE *mq_make_derived_spec (PARSER_CONTEXT * parser, PT_NODE * node,
				      PT_NODE * subquery, int *idx,
				      PT_NODE ** spec_ptr,
				      PT_NODE ** attr_list_ptr);

extern PT_NODE *mq_get_references (PARSER_CONTEXT * parser,
				   PT_NODE * statement, PT_NODE * spec);
extern PT_NODE *mq_get_references_helper (PARSER_CONTEXT * parser,
					  PT_NODE * statement,
					  PT_NODE * spec,
					  bool get_spec_referenced_attr);
extern PT_NODE *mq_reset_ids (PARSER_CONTEXT * parser,
			      PT_NODE * statement, PT_NODE * spec);
extern PT_NODE *mq_clear_ids (PARSER_CONTEXT * parser, PT_NODE * node,
			      PT_NODE * spec);

extern PT_NODE *mq_set_references (PARSER_CONTEXT * parser,
				   PT_NODE * statement, PT_NODE * spec);

extern bool mq_is_outer_join_spec (PARSER_CONTEXT * parser, PT_NODE * spec);

extern PT_NODE *mq_reset_ids_in_statement (PARSER_CONTEXT * parser,
					   PT_NODE * statement);

extern PT_NODE *mq_rewrite_aggregate_as_derived (PARSER_CONTEXT * parser,
						 PT_NODE * agg_sel);

#endif /* _VIEW_TRANSFORM_H_ */
