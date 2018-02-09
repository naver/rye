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
 * query_result.c - Helper functions to allocate, initialize query result
 *                  descriptor for select expressions.
 */

#ident "$Id$"

#include "config.h"

#include "misc_string.h"
#include "error_manager.h"
#include "parser.h"
#include "parser_message.h"
#include "server_interface.h"
#include "db_query.h"
#include "xasl_support.h"
#include "object_accessor.h"
#include "schema_manager.h"
#include "memory_alloc.h"
#include "execute_statement.h"
#include "xasl_generation.h"
#include "object_primitive.h"
#include "db.h"
#include "network_interface_cl.h"


static int pt_arity_of_query_type (const DB_QUERY_TYPE * qt);
static char *pt_get_attr_name (PARSER_CONTEXT * parser, PT_NODE * node);
static void pt_set_domain_class (SM_DOMAIN * dom, const PT_NODE * nam,
				 const DB_OBJECT * virt);
static void pt_set_domain_class_list (SM_DOMAIN * dom, const PT_NODE * nam,
				      const DB_OBJECT * virt);
static SM_DOMAIN *pt_get_src_domain (PARSER_CONTEXT * parser,
				     const PT_NODE * s,
				     const PT_NODE * specs);
static DB_QUERY_TYPE *pt_get_node_title (PARSER_CONTEXT * parser,
					 const PT_NODE * col,
					 const PT_NODE * from_list);
static PT_NODE *pt_get_from_list (const PARSER_CONTEXT * parser,
				  const PT_NODE * query);

/*
 * pt_arity_of_query_type () -  return arity (number of columns) of
 *                             a given DB_QUERY_TYPE
 *   return:  the arity (number of columns) of qt
 *   qt(in): a DB_QUERY_TYPE handle
 */

static int
pt_arity_of_query_type (const DB_QUERY_TYPE * qt)
{
  int cnt = 0;

  while (qt)
    {
      cnt++;
      qt = qt->next;
    }

  return cnt;
}

/*
 * pt_get_attr_name () - return the attribute name of a select_list item or NULL
 *   return: the attribute name of s if s is a path expression,
 *          NULL otherwise
 *   parser(in):
 *   node(in): an expression representing a select_list item
 */
static char *
pt_get_attr_name (PARSER_CONTEXT * parser, PT_NODE * node)
{
  const char *name;
  char *res = NULL;
  unsigned int save_custom = parser->custom_print;

  node = pt_get_end_path_node (node);
  if (node == NULL)
    {
      return NULL;
    }
  if (node->node_type != PT_NAME)
    {
      return NULL;
    }

  parser->custom_print = (parser->custom_print | PT_SUPPRESS_RESOLVED);

  name = parser_print_tree (parser, node);
  parser->custom_print = save_custom;

  if (name)
    {
      res = strdup (name);
    }

  return res;
}

/*
 * pt_set_domain_class() -  set SM_DOMAIN's class field
 *   return:  none
 *   dom(out): an SM_DOMAIN
 *   nam(in): an entity name
 *   virt(in):
 */
static void
pt_set_domain_class (SM_DOMAIN * dom, const PT_NODE * nam,
		     const DB_OBJECT * virt)
{
  if (!dom || !nam || nam->node_type != PT_NAME)
    {
      return;
    }

  dom->type = PR_TYPE_FROM_ID (DB_TYPE_OBJECT);
  if (virt != NULL)
    {
      dom->class_mop = (DB_OBJECT *) virt;
    }
  else
    {
      if (nam->info.name.db_object != NULL)
	{
	  dom->class_mop = nam->info.name.db_object;
	  COPY_OID (&dom->class_oid, &(dom->class_mop->ws_oid));
	}
      else
	{
	  dom->class_mop = sm_find_class (nam->info.name.original);
	  if (dom->class_mop != NULL)
	    {
	      COPY_OID (&dom->class_oid, &(dom->class_mop->ws_oid));
	    }
	}
    }
}

/*
 * pt_set_domain_class_list() -  set SM_DOMAIN's class fields
 *   return:  none
 *   dom(out): an SM_DOMAIN anchor node
 *   nam(in): an entity name list
 *   virt(in):
 */
static void
pt_set_domain_class_list (SM_DOMAIN * dom, const PT_NODE * nam,
			  const DB_OBJECT * virt)
{
  SM_DOMAIN *tail = dom;

  while (nam && nam->node_type == PT_NAME && dom)
    {
      pt_set_domain_class (dom, nam, virt);
      nam = nam->next;

      if (!nam || nam->node_type != PT_NAME)
	{
	  break;
	}

      dom = regu_domain_db_alloc ();
      tail->next = dom;
      tail = dom;
    }
}

/*
 * pt_get_src_domain() -  compute & return the source domain of an expression
 *   return:  source domain of the given expression
 *   parser(in): the parser context
 *   s(in): an expression representing a select_list item
 *   specs(in): the list of specs to which s was resolved
 */
static SM_DOMAIN *
pt_get_src_domain (PARSER_CONTEXT * parser, const PT_NODE * s,
		   const PT_NODE * specs)
{
  SM_DOMAIN *result;
  PT_NODE *spec, *entity_names, *leaf = (PT_NODE *) s;
  UINTPTR spec_id;

  result = regu_domain_db_alloc ();
  if (result == NULL)
    {
      return result;
    }

  /* if s is not a path expression then its source domain is DB_TYPE_NULL */
  result->type = PR_TYPE_FROM_ID (DB_TYPE_NULL);

  /* make leaf point to the last leaf name node */
  if (s->node_type == PT_DOT_)
    {
      leaf = s->info.dot.arg2;
    }

  /* s's source domain is the domain of leaf's resolvent(s) */
  if (leaf->node_type == PT_NAME
      && (spec_id = leaf->info.name.spec_id)
      && (spec = pt_find_entity (parser, specs, spec_id))
      && (entity_names = spec->info.spec.flat_entity_list))
    {
      pt_set_domain_class_list (result, entity_names,
				entity_names->info.name.virt_object);
    }

  return result;
}

/*
 * pt_report_to_ersys () - report query compilation error by
 *                         setting global ER state
 *   return:
 *   parser(in): handle to parser used to process the query
 *   error_type(in): syntax, semantic, or execution
 */
void
pt_report_to_ersys (const PARSER_CONTEXT * parser,
		    const PT_ERROR_TYPE error_type)
{
  PT_NODE *error_node;
  int err;
  char buf[ONE_K];

  error_node = parser->error_msgs;
  if (error_node && error_node->node_type == PT_ZZ_ERROR_MSG)
    {
      err = er_errid ();
      if (!ER_IS_LOCK_TIMEOUT_ERROR (err) && !ER_IS_SERVER_DOWN_ERROR (err))
	{
	  switch (error_type)
	    {
	    case PT_SYNTAX:
	      er_set (ER_SYNTAX_ERROR_SEVERITY, ARG_FILE_LINE, ER_PT_SYNTAX,
		      2, error_node->info.error_msg.error_message, "");
	      break;
	    case PT_SEMANTIC:
	      er_set (ER_SYNTAX_ERROR_SEVERITY, ARG_FILE_LINE, ER_PT_SEMANTIC,
		      2, error_node->info.error_msg.error_message, "");
	      break;
	    case PT_EXECUTION:
	      assert (false);	/* TODO - trace */
	      er_set (ER_SYNTAX_ERROR_SEVERITY, ARG_FILE_LINE, ER_PT_EXECUTE,
		      2, error_node->info.error_msg.error_message, "");
	      break;
	    default:
	      assert (false);	/* TODO - trace */
	      er_set (ER_SYNTAX_ERROR_SEVERITY, ARG_FILE_LINE, ER_PT_SEMANTIC,
		      2, error_node->info.error_msg.error_message, "");
	      break;
	    }
	}
      return;
    }

  assert (false);		/* TODO - trace */

  /* a system error reporting error messages */
  sprintf (buf, "Internal error- reporting %s error.",
	   (error_type == PT_SYNTAX ? "syntax" :
	    (error_type == PT_SEMANTIC ? "semantic" : "execution")));

  er_set (ER_SYNTAX_ERROR_SEVERITY, ARG_FILE_LINE, ER_PT_EXECUTE, 2, buf, "");
}

/*
 * pt_report_to_ersys_with_statement () - report query compilation error by
 *                                        setting global ER state
 *   return:
 *   parser(in): handle to parser used to process the query
 *   error_type(in): syntax, semantic, or execution
 *   statement(in): statement tree
 */
void
pt_report_to_ersys_with_statement (PARSER_CONTEXT * parser,
				   const PT_ERROR_TYPE error_type,
				   PT_NODE * statement)
{
  PT_NODE *error_node;
  char buf[1000];
  char *stmt_string = NULL;
  int err;

  if (parser == NULL)
    {
      return;
    }

  error_node = parser->error_msgs;
  if (statement)
    {
      PT_NODE_PRINT_TO_ALIAS (parser, statement,
			      PT_CONVERT_RANGE | PT_SHORT_PRINT);
      stmt_string = (char *) statement->alias_print;
    }
  if (stmt_string == NULL)
    {
      stmt_string = (char *) "";
    }

  if (error_node && error_node->node_type == PT_ZZ_ERROR_MSG)
    {
      err = er_errid ();
      if (!ER_IS_LOCK_TIMEOUT_ERROR (err) && !ER_IS_SERVER_DOWN_ERROR (err))
	{
	  switch (error_type)
	    {
	    case PT_SYNTAX:
	      er_set (ER_SYNTAX_ERROR_SEVERITY, ARG_FILE_LINE, ER_PT_SYNTAX,
		      2, error_node->info.error_msg.error_message,
		      stmt_string);
	      break;
	    case PT_SEMANTIC:
	      er_set (ER_SYNTAX_ERROR_SEVERITY, ARG_FILE_LINE, ER_PT_SEMANTIC,
		      2, error_node->info.error_msg.error_message,
		      stmt_string);
	      break;
	    case PT_EXECUTION:
	      er_set (ER_SYNTAX_ERROR_SEVERITY, ARG_FILE_LINE, ER_PT_EXECUTE,
		      2, error_node->info.error_msg.error_message,
		      stmt_string);
	      break;
	    default:
	      assert (false);	/* TODO - trace */
	      er_set (ER_SYNTAX_ERROR_SEVERITY, ARG_FILE_LINE, ER_PT_SEMANTIC,
		      2, error_node->info.error_msg.error_message,
		      stmt_string);
	      break;
	    }
	}
      return;
    }

  assert (false);		/* TODO - trace */

  /* a system error reporting error messages */
  sprintf (buf, "Internal error- reporting %s error.",
	   (error_type == PT_SYNTAX ? "syntax" :
	    (error_type == PT_SEMANTIC ? "semantic" : "execution")));

  er_set (ER_SYNTAX_ERROR_SEVERITY, ARG_FILE_LINE,
	  ER_PT_EXECUTE, 2, buf, stmt_string);
}				/* pt_report_to_ersys_with_statement() */

/*
 * pt_get_select_list () - PT_NODE *, a pointer to query's select list
 *	                   NULL if query is a 'SELECT *' query
 *   return:
 *   parser(in): handle to parser used to process & derive query
 *   query(out): abstract syntax tree form of a SELECT expression
 */
PT_NODE *
pt_get_select_list (PARSER_CONTEXT * parser, PT_NODE * query)
{
  PT_NODE *list = NULL, *attr;

  if (query == NULL || !PT_IS_QUERY (query))
    {
      return NULL;
    }

  if (query->node_type == PT_SELECT)
    {
      list = query->info.query.q.select.list;
      if (list == NULL)
	{
	  return NULL;
	}

      /* return the first row of PT_NODE_LIST */
      if (list->node_type == PT_NODE_LIST)
	{
	  return list->info.node_list.list;
	}

      if (list->node_type == PT_VALUE && list->type_enum == PT_TYPE_STAR)
	{
	  return NULL;
	}

      for (attr = list; attr; attr = attr->next)
	{
	  if (attr->node_type == PT_NAME && attr->type_enum == PT_TYPE_STAR)
	    {
	      /* found "class_name.*" */
	      return NULL;
	    }
	}
    }
  else
    {
      assert (query->node_type == PT_DIFFERENCE
	      || query->node_type == PT_INTERSECTION
	      || query->node_type == PT_UNION);

      list = pt_get_select_list (parser, query->info.query.q.union_.arg1);
    }

  return list;
}

/*
 * pt_get_from_list () - returns a pointer to query's from list
 *   return:
 *   parser(in): handle to parser used to process & derive query
 *   query(in): abstract syntax tree form of a SELECT expression
 */
static PT_NODE *
pt_get_from_list (const PARSER_CONTEXT * parser, const PT_NODE * query)
{
  if (query == NULL)
    {
      return NULL;
    }

  switch (query->node_type)
    {
    default:
      return NULL;

    case PT_SELECT:
      return query->info.query.q.select.from;

    case PT_DIFFERENCE:
    case PT_INTERSECTION:
    case PT_UNION:
      return pt_get_from_list (parser, query->info.query.q.union_.arg1);
    }
}

/*
 * pt_get_titles() - creates, initializes, returns DB_QUERY_TYPE describing the
 *   		     output columns titles of the given query
 *   return:  DB_QUERY_TYPE*, a descriptor of query's output columns
 *   parser(in/out): handle to parser used to process & derive query
 *   query(in): abstract syntax tree form of a SELECT expression
 */

DB_QUERY_TYPE *
pt_get_titles (PARSER_CONTEXT * parser, PT_NODE * query)
{
  DB_QUERY_TYPE *q, *t, *tail;
  PT_NODE *s, *f;

  s = pt_get_select_list (parser, query);
  if (pt_length_of_select_list (s, EXCLUDE_HIDDEN_COLUMNS) <= 0)
    {
      return NULL;
    }
  f = pt_get_from_list (parser, query);

  for (q = NULL, tail = NULL; s; s = s->next)
    {
      if (s->is_hidden_column)
	{
	  continue;
	}
      else
	{
	  parser->custom_print |= PT_SUPPRESS_CHARSET_PRINT;
	  t = pt_get_node_title (parser, s, f);
	  parser->custom_print &= ~PT_SUPPRESS_CHARSET_PRINT;

	  if (t == NULL)
	    {
	      db_free_query_format (q);
	      return NULL;
	    }

	  if (q == NULL)
	    {
	      q = t;
	    }
	  else
	    {
	      tail->next = t;
	    }

	  t->next = NULL;
	  tail = t;
	}
    }

  return q;
}

/*
 * pt_get_node_title() -  allocate and initialize a query_type node.
 *   return:  a fully initialized query type node
 *   parser(in/out): handle to parser used to process & derive query
 *   col(in): column to create the query type node from.
 *   from_list(in):
 *
 */

static DB_QUERY_TYPE *
pt_get_node_title (PARSER_CONTEXT * parser, const PT_NODE * col,
		   const PT_NODE * from_list)
{
  DB_QUERY_TYPE *q = NULL;
  char *name;
  unsigned int save_custom;
  PT_NODE *node, *spec, *range_var;

  save_custom = parser->custom_print;
  parser->custom_print |= PT_SUPPRESS_QUOTES;

  q = db_alloc_query_format (1);
  if (q == NULL)
    {
      goto error;
    }

  if (pt_resolved (col))
    {
      parser->custom_print |= PT_SUPPRESS_RESOLVED;
    }

  name = pt_print_alias (parser, col);

  if (col->alias_print == NULL)
    {
      switch (col->node_type)
	{
	case PT_NAME:
	  break;

	case PT_DOT_:
	  /* traverse left node */
	  node = (PT_NODE *) col;
	  while (node && node->node_type == PT_DOT_)
	    {
	      node = node->info.dot.arg1;
	    }

	  if (node && node->node_type == PT_NAME)
	    {
	      if (pt_resolved (col))
		{
		  ;
		}
	      else if (node->info.name.meta_class == PT_NORMAL)
		{
		  /* check for classname */
		  for (spec = (PT_NODE *) from_list; spec; spec = spec->next)
		    {
		      /* get spec's range variable
		       * if range variable for spec is used, use range_var.
		       * otherwise, use entity_name
		       */
		      range_var = spec->info.spec.range_var ?
			spec->info.spec.range_var :
			spec->info.spec.entity_name;

		      if (pt_check_path_eq (parser, range_var, node) == 0)
			{
			  if (name)
			    {
			      char *save_name = name;

			      /* strip off classname.* */
			      name = strchr (name, '.');
			      if (name == NULL || name[0] == '\0')
				{
				  name = save_name;
				}
			      else
				{
				  name++;
				}
			      break;
			    }

			}
		    }
		}
	    }
	  break;
	default:
	  break;
	}
    }

  if (name)
    {
      q->name = strdup (name);
      if (q->name == NULL)
	{
	  goto error;
	}
    }

  q->attr_name = pt_get_attr_name (parser, (PT_NODE *) col);

  parser->custom_print = save_custom;

  return q;

error:
  parser->custom_print = save_custom;
  if (q)
    {
      db_free_query_format (q);
    }

  return NULL;
}				/* pt_get_node_title */


/*
 * pt_fillin_type_size() -  set the db_type&size fields of a DB_QUERY_TYPE list
 *   return:  list, a fully initialized descriptor of query's output columns
 *   parser(in): handle to parser used to process & derive query
 *   query(in): abstract syntax tree form of a SELECT expression
 *   list(in/out): a partially initialized DB_QUERY_TYPE list
 */

DB_QUERY_TYPE *
pt_fillin_type_size (PARSER_CONTEXT * parser, PT_NODE * query,
		     DB_QUERY_TYPE * list)
{
  DB_QUERY_TYPE *t;
  PT_NODE *s, *from_list;
  const char *spec_name;
  PT_NODE *node, *spec;
  UINTPTR spec_id;
#if 0				/* TODO - TYPE; need more consideration */
  SM_DOMAIN *dom;
#endif

  s = pt_get_select_list (parser, query);
  if (s == NULL || list == NULL)
    {
      return list;
    }

  from_list = pt_get_from_list (parser, query);
  /* from_list is allowed to be NULL for supporting SELECT without references
     to tables */

  if (pt_length_of_select_list (s, EXCLUDE_HIDDEN_COLUMNS) !=
      pt_arity_of_query_type (list))
    {
      PT_INTERNAL_ERROR (parser, "query result");
      return list;
    }

  for (t = list; s && t; s = s->next, t = t->next)
    {
#if 0				/* TODO - TYPE; need more consideration */
      dom = NULL;
#else
      t->domain = tp_domain_resolve_default (DB_TYPE_VARIABLE);
      t->src_domain = pt_get_src_domain (parser, s, from_list);
#endif

      spec_name = NULL;
      /* if it is attribute, find spec name */
      if (pt_is_attr (s))
	{
	  node = s;
	  while (node->node_type == PT_DOT_)
	    {
	      node = node->info.dot.arg1;	/* root node for path expression */
	    }

	  if (node->node_type == PT_NAME
	      && (spec_id = node->info.name.spec_id)
	      && (spec = pt_find_entity (parser, from_list, spec_id)))
	    {
	      if (spec->info.spec.range_var)
		{
		  spec_name = spec->info.spec.range_var->info.name.original;
		}
	    }

#if 0				/* TODO - TYPE; need more consideration */
	  /* TYPE */
	  dom = pt_node_to_db_domain (parser, node, spec_name);
	  dom = tp_domain_cache (dom);
#endif
	}

#if 0				/* TODO - TYPE; need more consideration */
      t->domain = dom;
      if (t->domain == NULL)
	{
	  t->domain = tp_domain_resolve_default (DB_TYPE_VARIABLE);
	}

      t->src_domain = pt_get_src_domain (parser, s, from_list);
#endif

      t->spec_name = (spec_name) ? strdup (spec_name) : NULL;
    }

  return list;
}

/*
 * pt_new_query_result_descriptor() - allocates, initializes, returns a new
 *      query result descriptor and opens a cursor for the query's results
 *   return:  DB_QUERY_RESULT* with an open cursor
 *   parser(in): handle to parser used to process & derive query
 *   query(out): abstract syntax tree (AST) form of a query statement
 */

DB_QUERY_RESULT *
pt_new_query_result_descriptor (PARSER_CONTEXT * parser, PT_NODE * query)
{
  int degree;
  DB_QUERY_RESULT *r = NULL;
  QFILE_LIST_ID *list_id;
  bool failure = false;

  if (query == NULL)
    {
      return NULL;
    }

  switch (query->node_type)
    {
    default:
      assert (false);		/* TODO - trace */
      return NULL;
      break;

    case PT_INSERT:
      assert (false);		/* TODO - trace */
      degree = 1;
      break;

    case PT_DIFFERENCE:
    case PT_INTERSECTION:
    case PT_UNION:
    case PT_SELECT:
#if 1
      assert (query->info.query.oids_included == DB_NO_OIDS);
#endif
      degree = 0;
      degree = pt_length_of_select_list (pt_get_select_list (parser, query),
					 EXCLUDE_HIDDEN_COLUMNS);
      break;
    }

  r = db_alloc_query_result (T_SELECT, degree);
  if (r == NULL)
    {
      return NULL;
    }

  db_init_query_result (r, T_SELECT);
  r->type = T_SELECT;
  r->col_cnt = degree;

  r->res.s.query_id = parser->query_id;
  r->res.s.stmt_type = RYE_STMT_SELECT;

  /* the following is for clean up when the query fails */
  memset (&r->res.s.cursor_id.list_id, 0, sizeof (QFILE_LIST_ID));
  r->res.s.cursor_id.query_id = parser->query_id;
  r->res.s.cursor_id.buffer = NULL;
  r->res.s.cursor_id.tuple_record.tpl = NULL;
  r->res.s.holdable = parser->is_holdable;

  list_id = (QFILE_LIST_ID *) query->etc;
  r->type_cnt = degree;
  if (list_id)
    {
      failure = !cursor_open (&r->res.s.cursor_id, list_id);
      /* free result, which was copied by open cursor operation! */
      regu_free_listid (list_id);
    }
  else
    {
      QFILE_LIST_ID empty_list_id;
      QFILE_CLEAR_LIST_ID (&empty_list_id);
      failure = !cursor_open (&r->res.s.cursor_id, &empty_list_id);
    }

  if (failure)
    {
      db_free_query_result (r);
      r = NULL;
    }

  return r;
}

/*
 * pt_free_query_etc_area () -
 *   return: none
 *   session(in):
 *   query(in): abstract syntax tree (AST) form of a query statement
 */
void
pt_free_query_etc_area (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * query)
{
  if (query && query->etc && pt_node_to_stmt_type (query) == RYE_STMT_SELECT)
    {
      assert (false);		/* TODO - trace */
      regu_free_listid ((QFILE_LIST_ID *) query->etc);
    }
}


/*
 * pt_end_query() -
 *   return:
 *   parser(in): parser context
 *   query_id_self(in):
 */
void
pt_end_query (PARSER_CONTEXT * parser, QUERY_ID query_id_self,
	      PT_NODE * statement)
{
  bool notify_server = true;

  assert (parser != NULL);
  assert (query_id_self != 0);
  assert (query_id_self == NULL_QUERY_ID || query_id_self > 0);

  if (statement && statement->is_server_query_ended)
    {
      notify_server = false;
      statement->is_server_query_ended = 0;
    }

  if (parser->query_id > 0)
    {
      if ((notify_server) && (er_errid () != ER_LK_UNILATERALLY_ABORTED))
	{
	  qmgr_end_query (parser->query_id);
	}
    }
  else
    {
      assert (parser->query_id == 0);
    }

  parser->query_id = query_id_self;
}

/*
 * db_get_attribute () -
 *   return:
 *   obj(in):
 *   name(in):
 */
DB_ATTRIBUTE *
db_get_attribute_force (DB_OBJECT * obj, const char *name)
{
  SM_CLASS *class_;
  SM_ATTRIBUTE *att;

  att = NULL;
  if (au_fetch_class_force (obj, &class_, S_LOCK) == NO_ERROR)
    {
      att = classobj_find_attribute (class_->attributes, name);
      if (att == NULL)
	{
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		  ER_OBJ_INVALID_ATTRIBUTE, 1, name);
	}
    }

  return ((DB_ATTRIBUTE *) att);
}

/*
 * db_get_attributes_force () -
 *   return:
 *   obj(in):
 */
DB_ATTRIBUTE *
db_get_attributes_force (DB_OBJECT * obj)
{
  SM_CLASS *class_;
  SM_ATTRIBUTE *atts;

  atts = NULL;
  if (au_fetch_class_force ((DB_OBJECT *) obj, &class_, S_LOCK) == NO_ERROR)
    {
      atts = class_->ordered_attributes;
      if (atts == NULL)
	er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_NO_COMPONENTS, 0);
    }
  return ((DB_ATTRIBUTE *) atts);
}


/*
 * pt_find_users_class () -
 *   return: class object if found
 *   parser(in):
 *   name(in/out):
 */
DB_OBJECT *
pt_find_users_class (PARSER_CONTEXT * parser, PT_NODE * name)
{
  DB_OBJECT *object;

  object = sm_find_class (name->info.name.original);

  if (!object)
    {
      PT_ERRORmf (parser, name, MSGCAT_SET_PARSER_SEMANTIC,
		  MSGCAT_SEMANTIC_CLASS_DOES_NOT_EXIST,
		  name->info.name.original);
    }
  name->info.name.db_object = object;

  pt_check_user_owns_class (parser, name);

  return object;
}
