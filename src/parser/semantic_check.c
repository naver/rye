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
 * semantic_check.c - semantic checking functions
 */

#ident "$Id$"

#include "config.h"


#include <assert.h>
#include "error_manager.h"
#include "parser.h"
#include "parser_message.h"
#include "semantic_check.h"
#include "memory_alloc.h"
#include "execute_schema.h"
#include "set_object.h"
#include "schema_manager.h"
#include "release_string.h"
#include "dbi.h"
#include "xasl_generation.h"
#include "view_transform.h"

/* this must be the last header file included!!! */
#include "dbval.h"

typedef enum
{ PT_CAST_VALID, PT_CAST_INVALID, PT_CAST_UNSUPPORTED } PT_CAST_VAL;

typedef enum
{ PT_UNION_COMP = 1, PT_UNION_INCOMP = 0,
  PT_UNION_INCOMP_CANNOT_FIX = -1, PT_UNION_ERROR = -2
} PT_UNION_COMPATIBLE;

#if defined (ENABLE_UNUSED_FUNCTION)
typedef struct seman_compatible_info
{
  int idx;
  PT_TYPE_ENUM type_enum;
  int prec;
  int scale;
} SEMAN_COMPATIBLE_INFO;
#endif

typedef enum
{
  RANGE_MIN = 0,
  RANGE_MAX = 1
} RANGE_MIN_MAX_ENUM;

typedef struct db_value_plist
{
  struct db_value_plist *next;
  DB_VALUE *val;
} DB_VALUE_PLIST;

#if defined (ENABLE_UNUSED_FUNCTION)
static PT_NODE *pt_derive_attribute (PARSER_CONTEXT * parser, PT_NODE * c);
static PT_NODE *pt_get_attributes (PARSER_CONTEXT * parser, const DB_OBJECT * c);
#endif
static PT_MISC_TYPE pt_get_class_type (PARSER_CONTEXT * parser, const DB_OBJECT * cls);
static int pt_number_of_attributes (PARSER_CONTEXT * parser, PT_NODE * stmt, PT_NODE ** attrs);
#if defined (ENABLE_UNUSED_FUNCTION)
static int pt_objects_assignable (PARSER_CONTEXT * parser, const PT_NODE * d_class_dt, const PT_NODE * s_class);
static int pt_class_compatible (PARSER_CONTEXT * parser, const PT_NODE * class1, const PT_NODE * class2);
static bool pt_vclass_compatible (PARSER_CONTEXT * parser, const PT_NODE * att, const PT_NODE * qcol);
static int pt_type_assignable (PARSER_CONTEXT * parser, const PT_NODE * d_type, const PT_NODE * s_type);
static int pt_collection_compatible (PARSER_CONTEXT * parser, const PT_NODE * col1, const PT_NODE * col2);
#endif
static PT_UNION_COMPATIBLE pt_union_compatible (PARSER_CONTEXT * parser, PT_NODE * item1, PT_NODE * item2);
#if defined (ENABLE_UNUSED_FUNCTION)
static void pt_get_compatible_info_from_node (const PT_NODE * att, SEMAN_COMPATIBLE_INFO * cinfo);
#endif
static PT_NODE *pt_check_data_default (PARSER_CONTEXT * parser, PT_NODE * data_default_list);
static PT_NODE *pt_find_default_expression (PARSER_CONTEXT * parser, PT_NODE * tree, void *arg, int *continue_walk);
static PT_NODE *pt_find_aggregate_function (PARSER_CONTEXT * parser, PT_NODE * tree, void *arg, int *continue_walk);
static void pt_check_attribute_domain (PARSER_CONTEXT * parser,
                                       PT_NODE * attr_defs, PT_MISC_TYPE class_type, const char *self, PT_NODE * stmt);
static void pt_check_alter (PARSER_CONTEXT * parser, PT_NODE * alter);
static const char *attribute_name (PARSER_CONTEXT * parser, PT_NODE * att);

#if defined (ENABLE_UNUSED_FUNCTION)
static bool pt_attr_refers_to_self (PARSER_CONTEXT * parser, PT_NODE * attr, const char *self);
#endif
static bool pt_is_compatible_type (const PT_TYPE_ENUM arg1_type, const PT_TYPE_ENUM arg2_type);
static PT_UNION_COMPATIBLE
pt_check_vclass_attr_qspec_compatible (PARSER_CONTEXT * parser, PT_NODE * attr, PT_NODE * col);
static PT_NODE *pt_check_vclass_query_spec (PARSER_CONTEXT * parser,
                                            PT_NODE * qry, PT_NODE * attrs,
                                            const char *self, const bool do_semantic_check);
static PT_NODE *pt_type_cast_vclass_query_spec (PARSER_CONTEXT * parser, PT_NODE * qry, PT_NODE * attrs);
static PT_NODE *pt_check_default_vclass_query_spec (PARSER_CONTEXT * parser, PT_NODE * qry, PT_NODE * attrs);
static void pt_check_create_view (PARSER_CONTEXT * parser, PT_NODE * stmt);
static void pt_check_create_entity (PARSER_CONTEXT * parser, PT_NODE * node);
static void pt_check_create_user (PARSER_CONTEXT * parser, PT_NODE * node);
static void pt_check_create_index (PARSER_CONTEXT * parser, PT_NODE * node);
static void pt_check_drop (PARSER_CONTEXT * parser, PT_NODE * node);
static void pt_check_grant_revoke (PARSER_CONTEXT * parser, PT_NODE * node);
static PT_NODE *pt_check_single_valued_node (PARSER_CONTEXT * parser, PT_NODE * node, void *arg, int *continue_walk);
static PT_NODE *pt_check_single_valued_node_post (PARSER_CONTEXT * parser,
                                                  PT_NODE * node, void *arg, int *continue_walk);
static PT_NODE *pt_semantic_check_local (PARSER_CONTEXT * parser, PT_NODE * node, void *arg, int *continue_walk);
static PT_NODE *pt_check_with_info (PARSER_CONTEXT * parser, PT_NODE * node, SEMANTIC_CHK_INFO * info);
static DB_OBJECT *pt_find_class (PARSER_CONTEXT * parser, PT_NODE * p);
static void pt_check_unique_attr (PARSER_CONTEXT * parser,
                                  const char *entity_name, PT_NODE * att, PT_NODE_TYPE att_type);
static void pt_check_assignments (PARSER_CONTEXT * parser, PT_NODE * stmt);
static PT_NODE *pt_coerce_insert_values (PARSER_CONTEXT * parser, PT_NODE * stmt);
#if defined (ENABLE_UNUSED_FUNCTION)
static void pt_check_xaction_list (PARSER_CONTEXT * parser, PT_NODE * node);
static PT_NODE *pt_count_iso_nodes (PARSER_CONTEXT * parser, PT_NODE * node, void *arg, int *continue_walk);
static PT_NODE *pt_count_time_nodes (PARSER_CONTEXT * parser, PT_NODE * node, void *arg, int *continue_walk);
static PT_NODE *pt_check_isolation_lvl (PARSER_CONTEXT * parser, PT_NODE * node, void *arg, int *continue_walk);
#endif
static PT_NODE *pt_check_constraint (PARSER_CONTEXT * parser, const PT_NODE * create, const PT_NODE * constraint);
static PT_NODE *pt_check_constraints (PARSER_CONTEXT * parser, const PT_NODE * create);
static DB_OBJECT *pt_check_user_exists (PARSER_CONTEXT * parser, PT_NODE * cls_ref);
#if defined (ENABLE_UNUSED_FUNCTION)
static int pt_collection_assignable (PARSER_CONTEXT * parser, const PT_NODE * d_col, const PT_NODE * s_col);
#endif
static int pt_assignment_class_compatible (PARSER_CONTEXT * parser, PT_NODE * lhs, PT_NODE * rhs);
static int pt_check_defaultf (PARSER_CONTEXT * parser, PT_NODE * node);
static PT_NODE *pt_check_vclass_union_spec (PARSER_CONTEXT * parser, PT_NODE * qry, PT_NODE * attrds);
static int pt_check_group_concat_order_by (PARSER_CONTEXT * parser, PT_NODE * func);
static PT_NODE *pt_check_sub_insert (PARSER_CONTEXT * parser, PT_NODE * node, void *void_arg, int *continue_walk);
static PT_NODE *pt_get_assignments (PT_NODE * node);
static void pt_check_shard_key (PARSER_CONTEXT * parser, PT_NODE * node);


/*
 * pt_check_compatible_node_for_orderby ()
 */
bool
pt_check_compatible_node_for_orderby (PARSER_CONTEXT * parser, PT_NODE * order, PT_NODE * column)
{
  PT_NODE *arg1, *cast_type;
  PT_TYPE_ENUM type1, type2;

  if (order == NULL || column == NULL || order->node_type != PT_EXPR || order->info.expr.op != PT_CAST)
    {
      return false;
    }

  arg1 = order->info.expr.arg1;
  if (arg1->node_type != column->node_type)
    {
      return false;
    }

  if (arg1->node_type != PT_NAME && arg1->node_type != PT_DOT_)
    {
      return false;
    }

  if (pt_check_path_eq (parser, arg1, column) != 0)
    {
      return false;
    }

  cast_type = order->info.expr.cast_type;
  assert (cast_type != NULL);

  type1 = arg1->type_enum;
  type2 = cast_type->type_enum;

  if (PT_IS_NUMERIC_TYPE (type1) && PT_IS_NUMERIC_TYPE (type2))
    {
      return true;
    }

  /* Only string type :
   * Do not consider 'CAST (enum_col as VARCHAR)' equal to 'enum_col' */
  if (PT_IS_STRING_TYPE (type1) && PT_IS_STRING_TYPE (type2))
    {
      int c1, c2;

      c1 = arg1->data_type->info.data_type.collation_id;
      c2 = cast_type->info.data_type.collation_id;
      return c1 == c2;
    }

  if (PT_IS_DATE_TIME_TYPE (type1) && PT_IS_DATE_TIME_TYPE (type2))
    {
      if ((type1 == PT_TYPE_TIME && type2 != PT_TYPE_TIME) || (type1 != PT_TYPE_TIME && type2 == PT_TYPE_TIME))
        {
          return false;
        }
      return true;
    }

  return false;
}

/*
 * pt_check_cast_op () - Checks to see if the cast operator is well-formed
 *   return: none
 *   parser(in):
 *   node(in): the node to check
 */
bool
pt_check_cast_op (PARSER_CONTEXT * parser, PT_NODE * node)
{
  PT_NODE *arg1;
  PT_TYPE_ENUM cast_type = PT_TYPE_NONE, arg_type;
  PT_CAST_VAL cast_is_valid = PT_CAST_VALID;

  if (node == NULL || node->node_type != PT_EXPR || node->info.expr.op != PT_CAST)
    {
      /* this should not happen, but don't crash and burn if it does */
      assert (false);
      return false;
    }

  /* get cast type */
  if (node->info.expr.cast_type != NULL)
    {
      cast_type = node->info.expr.cast_type->type_enum;
    }
  else
    {
      if (!pt_has_error (parser))
        {
          PT_INTERNAL_ERROR (parser, "null cast type");
        }
      return false;
    }

  /* get argument */
  arg1 = node->info.expr.arg1;
  if (arg1 == NULL)
    {
      /* a parse error might have occurred lower in the parse tree of arg1;
         don't register another error unless no error has been set */

      if (!pt_has_error (parser))
        {
          PT_ERRORmf2 (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
                       MSGCAT_SEMANTIC_CANT_COERCE_TO, "(null)", pt_show_type_enum (cast_type));
        }
      return false;
    }

  /* CAST (arg_type AS cast_type) */
  if (arg1->node_type == PT_EXPR && arg1->info.expr.op == PT_CAST)
    {
      /* arg1 is a cast, so arg1.type_enum is not yet set; pull type from
         expression's cast type */
      arg_type = arg1->info.expr.cast_type->type_enum;
    }
  else
    {
      /* arg1 is not a cast */
      arg_type = arg1->type_enum;
    }

  switch (arg_type)
    {
    case PT_TYPE_INTEGER:
    case PT_TYPE_BIGINT:
    case PT_TYPE_DOUBLE:
    case PT_TYPE_NUMERIC:
      switch (cast_type)
        {
        case PT_TYPE_VARBIT:
        case PT_TYPE_DATE:
          /* allow numeric to TIME and TIMESTAMP conversions */
        case PT_TYPE_DATETIME:
        case PT_TYPE_SEQUENCE:
        case PT_TYPE_OBJECT:
          cast_is_valid = PT_CAST_INVALID;
          break;
        default:
          break;
        }
      break;
    case PT_TYPE_DATE:
      switch (cast_type)
        {
        case PT_TYPE_INTEGER:
        case PT_TYPE_BIGINT:
        case PT_TYPE_DOUBLE:
        case PT_TYPE_NUMERIC:
        case PT_TYPE_VARBIT:
        case PT_TYPE_TIME:
        case PT_TYPE_SEQUENCE:
        case PT_TYPE_OBJECT:
          cast_is_valid = PT_CAST_INVALID;
          break;
        default:
          break;
        }
      break;
    case PT_TYPE_TIME:
      switch (cast_type)
        {
        case PT_TYPE_INTEGER:
        case PT_TYPE_BIGINT:
        case PT_TYPE_DOUBLE:
        case PT_TYPE_NUMERIC:
        case PT_TYPE_VARBIT:
        case PT_TYPE_DATE:
        case PT_TYPE_SEQUENCE:
        case PT_TYPE_OBJECT:
          cast_is_valid = PT_CAST_INVALID;
          break;
        case PT_TYPE_DATETIME:
          cast_is_valid = PT_CAST_UNSUPPORTED;
          break;
        default:
          break;
        }
      break;
    case PT_TYPE_DATETIME:
      switch (cast_type)
        {
        case PT_TYPE_INTEGER:
        case PT_TYPE_DOUBLE:
        case PT_TYPE_NUMERIC:
        case PT_TYPE_VARBIT:
        case PT_TYPE_SEQUENCE:
        case PT_TYPE_OBJECT:
          cast_is_valid = PT_CAST_INVALID;
          break;
        default:
          break;
        }
      break;
    case PT_TYPE_VARCHAR:
      switch (cast_type)
        {
        case PT_TYPE_SEQUENCE:
          cast_is_valid = PT_CAST_UNSUPPORTED;
          break;
        case PT_TYPE_OBJECT:
          cast_is_valid = PT_CAST_INVALID;
          break;
        default:
          break;
        }
      break;
    case PT_TYPE_VARBIT:
      switch (cast_type)
        {
        case PT_TYPE_INTEGER:
        case PT_TYPE_BIGINT:
        case PT_TYPE_DOUBLE:
        case PT_TYPE_NUMERIC:
        case PT_TYPE_DATE:
        case PT_TYPE_TIME:
        case PT_TYPE_DATETIME:
        case PT_TYPE_SEQUENCE:
        case PT_TYPE_OBJECT:
          cast_is_valid = PT_CAST_INVALID;
          break;
        default:
          break;
        }
      break;
    case PT_TYPE_OBJECT:
      cast_is_valid = PT_CAST_UNSUPPORTED;
      break;
    case PT_TYPE_SEQUENCE:
      switch (cast_type)
        {
        case PT_TYPE_INTEGER:
        case PT_TYPE_BIGINT:
        case PT_TYPE_DOUBLE:
        case PT_TYPE_NUMERIC:
        case PT_TYPE_VARBIT:
        case PT_TYPE_DATE:
        case PT_TYPE_TIME:
        case PT_TYPE_DATETIME:
        case PT_TYPE_OBJECT:
          cast_is_valid = PT_CAST_INVALID;
          break;
        case PT_TYPE_VARCHAR:
          cast_is_valid = PT_CAST_UNSUPPORTED;
          break;
        default:
          break;
        }
      break;

    default:
      break;
    }

  switch (cast_is_valid)
    {
    case PT_CAST_VALID:
      break;
    case PT_CAST_INVALID:
      PT_ERRORmf2 (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
                   MSGCAT_SEMANTIC_CANT_COERCE_TO,
                   pt_short_print (parser, node->info.expr.arg1), pt_show_type_enum (cast_type));
      break;
    case PT_CAST_UNSUPPORTED:
      PT_ERRORmf2 (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
                   MSGCAT_SEMANTIC_COERCE_UNSUPPORTED,
                   pt_short_print (parser, node->info.expr.arg1), pt_show_type_enum (cast_type));
      break;
    }

  return (cast_is_valid == PT_CAST_VALID) ? true : false;
}

/*
 * pt_check_user_exists () -  given 'user.class', check that 'user' exists
 *   return:  db_user instance if user exists, NULL otherwise.
 *   parser(in): the parser context used to derive cls_ref
 *   cls_ref(in): a PT_NAME node
 *
 * Note :
 *   this routine is needed only in the context of checking create stmts,
 *   ie, in checking 'create vclass usr.cls ...'.
 *   Otherwise, pt_check_user_owns_class should be used.
 */
static DB_OBJECT *
pt_check_user_exists (PARSER_CONTEXT * parser, PT_NODE * cls_ref)
{
  const char *usr;
  DB_OBJECT *result;

  assert (parser != NULL);

  if (!cls_ref || cls_ref->node_type != PT_NAME || (usr = cls_ref->info.name.resolved) == NULL || usr[0] == '\0')
    {
      return NULL;
    }

  result = db_find_user (usr);
  if (!result)
    {
      PT_ERRORmf (parser, cls_ref, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_USER_IS_NOT_IN_DB, usr);
    }

  return result;
}

/*
 * pt_check_user_owns_class () - given user.class, check that user owns class
 *   return:  db_user instance if 'user' exists & owns 'class', NULL otherwise
 *   parser(in): the parser context used to derive cls_ref
 *   cls_ref(in): a PT_NAME node
 */
DB_OBJECT *
pt_check_user_owns_class (PARSER_CONTEXT * parser, PT_NODE * cls_ref)
{
  DB_OBJECT *result, *cls, *owner;

  if ((result = pt_check_user_exists (parser, cls_ref)) == NULL || (cls = cls_ref->info.name.db_object) == NULL)
    {
      return NULL;
    }

  owner = db_get_owner (cls);
  result = (owner == result ? result : NULL);
  if (!result)
    {
      PT_ERRORmf2 (parser, cls_ref, MSGCAT_SET_PARSER_SEMANTIC,
                   MSGCAT_SEMANTIC_USER_DOESNT_OWN_CLS, cls_ref->info.name.resolved, cls_ref->info.name.original);
    }

  return result;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * pt_derive_attribute () - derive a new ATTR_DEF node from a query_spec
 *                          column
 *   return:  a new ATTR_DEF node derived from c if all OK, NULL otherwise.
 *   parser(in): the parser context to use for creating the ATTR_DEF node
 *   c(in): a query_spec column
 */
static PT_NODE *
pt_derive_attribute (PARSER_CONTEXT * parser, PT_NODE * c)
{
  PT_NODE *attr = NULL;
  PT_NODE *cname = NULL;

  assert (parser != NULL);

  if (c == NULL)
    {
      return NULL;
    }

  if (c->alias_print != NULL)
    {
      cname = pt_name (parser, c->alias_print);
    }
  else if (c->node_type == PT_NAME)
    {
      if (c->type_enum == PT_TYPE_OBJECT
          && c->info.name.meta_class == PT_OID_ATTR
          && (c->info.name.original == NULL || strlen (c->info.name.original) == 0))
        {
          cname = pt_name (parser, c->info.name.resolved);
        }
      else
        {
          cname = pt_name (parser, c->info.name.original);
        }
    }
  else
    {
      return NULL;
    }

  if (cname == NULL)
    {
      return NULL;
    }

  attr = parser_new_node (parser, PT_ATTR_DEF);
  if (attr == NULL)
    {
      return NULL;
    }

  attr->data_type = NULL;
  attr->info.attr_def.attr_name = cname;
  attr->info.attr_def.attr_type = PT_NORMAL;

  return attr;
}

/*
 * pt_get_attributes () - get & return the attribute list of
 *                        a {class|vclass|view}
 *   return:  c's attribute list if successful, NULL otherwise.
 *   parser(in): the parser context to use for creating the list
 *   c(in): a {class|vclass|view} object
 */
/* TODO modify the function so that we can distinguish between a class having
 *      no attributes and an execution error.
 */
static PT_NODE *
pt_get_attributes (PARSER_CONTEXT * parser, const DB_OBJECT * c)
{
  DB_ATTRIBUTE *attributes;
  const char *class_name;
  PT_NODE *i_attr, *name, *typ, *types, *list = NULL;
  DB_OBJECT *cls;
  DB_DOMAIN *dom;

  assert (parser != NULL);

  if (!c || !(class_name = sm_class_name ((DB_OBJECT *) c)))
    {
      return list;
    }

  attributes = db_get_attributes ((DB_OBJECT *) c);
  while (attributes)
    {
      /* create a new attribute node */
      i_attr = parser_new_node (parser, PT_ATTR_DEF);
      if (i_attr == NULL)
        {
          PT_INTERNAL_ERROR (parser, "allocate new node");
          return NULL;
        }

      /* its name is class_name.attribute_name */
      i_attr->info.attr_def.attr_name = name = pt_name (parser, db_attribute_name (attributes));
      name->info.name.resolved = pt_append_string (parser, NULL, class_name);
      name->info.name.meta_class = PT_NORMAL;

      /* set its data type */
      i_attr->type_enum = (PT_TYPE_ENUM) pt_db_to_type_enum (db_attribute_type (attributes));
      switch (i_attr->type_enum)
        {
        case PT_TYPE_OBJECT:
          cls = db_domain_class (db_attribute_domain (attributes));
          if (cls)
            {
              name = pt_name (parser, sm_class_name (cls));
              name->info.name.meta_class = PT_CLASS;
              name->info.name.db_object = cls;
              name->info.name.spec_id = (UINTPTR) name;
              i_attr->data_type = typ = parser_new_node (parser, PT_DATA_TYPE);
              if (typ)
                {
                  typ->info.data_type.entity = name;
                }
            }
          break;

        case PT_TYPE_SEQUENCE:
          types = NULL;
          dom = db_domain_set (db_attribute_domain (attributes));
          while (dom)
            {
              typ = pt_domain_to_data_type (parser, dom);
              if (typ)
                {
                  typ->next = types;
                }
              types = typ;
              dom = db_domain_next (dom);
            }
          i_attr->data_type = types;
          break;

        default:
          dom = attributes->sma_domain;
          assert (TP_DOMAIN_TYPE (dom) != DB_TYPE_VARIABLE);

          typ = pt_domain_to_data_type (parser, dom);
          i_attr->data_type = typ;
          break;
        }

      list = parser_append_node (i_attr, list);

      /* advance to next attribute */
      attributes = db_attribute_next (attributes);
    }
  return list;
}
#endif

/*
 * pt_get_class_type () - return a class instance's type
 *   return:  PT_CLASS, PT_VCLASS, or PT_MISC_DUMMY
 *   cls(in): a class instance
 */
static PT_MISC_TYPE
pt_get_class_type (UNUSED_ARG PARSER_CONTEXT * parser, const DB_OBJECT * cls)
{
  if (!cls)
    {
      return PT_MISC_DUMMY;
    }

  if (db_is_vclass ((DB_OBJECT *) cls))
    {
      return PT_VCLASS;
    }

  if (db_is_class ((DB_OBJECT *) cls))
    {
      return PT_CLASS;
    }

  return PT_MISC_DUMMY;
}

/*
 * pt_number_of_attributes () - determine the number of attributes
 *      of the new class to be created by a create_vclass statement,
 *	or the number of attributes of the new definition of a view
 *	in the case of "ALTER VIEW xxx AS SELECT ...".
 *   return:  number of attributes of the new class to be created by stmt
 *   parser(in): the parser context used to derive stmt
 *   stmt(in): a create_vclass statement
 *   attrs(out): the attributes of the new class to be created by stmt
 */
static int
pt_number_of_attributes (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * stmt, PT_NODE ** attrs)
{
  int count = 0;

  if (stmt == NULL || attrs == NULL)
    {
      return count;
    }

  assert (stmt->node_type == PT_CREATE_ENTITY);
  if (stmt->node_type != PT_CREATE_ENTITY)
    {
      return count;
    }

  *attrs = stmt->info.create_entity.attr_def_list;
  count = pt_length_of_list (*attrs);

  return count;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * pt_objects_assignable () - determine if src is assignable to data_type dest
 *   return:  1 iff src is assignable to dest, 0 otherwise
 *   parser(in): the parser context
 *   d_class_dt(in): data_type of target attribute
 *   s_class(in): source PT_NODE
 */
static int
pt_objects_assignable (PARSER_CONTEXT * parser, const PT_NODE * d_class_dt, const PT_NODE * s_class)
{
  PT_NODE *s_class_type, *d_class_dt_type = NULL;

  if (!s_class || s_class->type_enum != PT_TYPE_OBJECT)
    {
      return 0;
    }

  if (!d_class_dt || (d_class_dt->node_type == PT_DATA_TYPE && !(d_class_dt_type = d_class_dt->info.data_type.entity)))
    {
      /* a wildcard destination object matches any other object type */
      return 1;
    }

  else if ((d_class_dt && d_class_dt->node_type != PT_DATA_TYPE)
           || !s_class->data_type || s_class->data_type->node_type != PT_DATA_TYPE)
    {
      /* weed out structural errors as failures */
      return 0;
    }
  else
    {
      /* s_class is assignable to d_class_dt
       * if s_class is a subclass of d_class_dt
       * this is what it should be:
       *   return pt_is_subset_of(parser, s_class_type, d_class_dt_type);
       * but d_class_dt->info.data_type.entity does not have ALL the
       * subclasses of the type, ie, if d_class_dt's type is "glo", it
       * shows only "glo" instead of:
       * "glo, audio, etc." so we do this instead:
       */
      if (!(s_class_type = s_class->data_type->info.data_type.entity))
        {
          return 1;             /* general object type */
        }
      else
        {
          return (s_class_type->info.name.db_object == d_class_dt_type->info.name.db_object);
        }
    }
}

/*
 * pt_class_assignable () - determine if s_class is assignable to d_class_dt
 *   return:  1 if s_class is assignable to d_class
 *   parser(in): the parser context
 *   d_class_dt(in): a PT_DATA_TYPE node whose type_enum is PT_TYPE_OBJECT
 *   s_class(in): a PT_NODE whose type_enum is PT_TYPE_OBJECT
 */
int
pt_class_assignable (PARSER_CONTEXT * parser, const PT_NODE * d_class_dt, const PT_NODE * s_class)
{

  /* a wildcard destination object accepts any other object type */
  if (!d_class_dt || (d_class_dt->node_type == PT_DATA_TYPE && !d_class_dt->info.data_type.entity))
    {
      return 1;
    }

  /* weed out structural errors as failures */
  if (!s_class || (d_class_dt && d_class_dt->node_type != PT_DATA_TYPE))
    {
      return 0;
    }

  /* NULL is assignable to any class type */
  if (PT_IS_NULL_NODE (s_class))
    {
      return 1;
    }

  /* make sure we are dealing only with object types */
  if (s_class->type_enum != PT_TYPE_OBJECT)
    {
      return 0;
    }

  return (pt_objects_assignable (parser, d_class_dt, s_class));
}

/*
 * pt_class_compatible () - determine if two classes have compatible domains
 *   return:  1 if class1 and class2 have compatible domains
 *   parser(in): the parser context
 *   class1(in): a PT_NODE whose type_enum is PT_TYPE_OBJECT
 *   class2(in): a PT_NODE whose type_enum is PT_TYPE_OBJECT
 */
static int
pt_class_compatible (PARSER_CONTEXT * parser, const PT_NODE * class1, const PT_NODE * class2)
{
  if (!class1 || class1->type_enum != PT_TYPE_OBJECT || !class2 || class2->type_enum != PT_TYPE_OBJECT)
    {
      return 0;
    }

  return pt_class_assignable (parser, class1->data_type, class2);
}

/*
 * pt_vclass_compatible () - determine if att is vclass compatible with qcol
 *   return:  true if att is vclass compatible with qcol
 *   parser(in): the parser context
 *   att(in): PT_DATA_TYPE node of a vclass attribute def
 *   qcol(in): a query spec column
 */
static bool
pt_vclass_compatible (PARSER_CONTEXT * parser, const PT_NODE * att, const PT_NODE * qcol)
{
  PT_NODE *entity, *qcol_entity, *qcol_typ;
  DB_OBJECT *vcls = NULL;
  const char *clsnam = NULL, *qcol_typnam = NULL, *spec, *qs_clsnam;
  DB_QUERY_SPEC *specs;

  /* a wildcard object accepts any other object type
   * but is not vclass_compatible with any other object */
  if (!att || (att->node_type == PT_DATA_TYPE && !att->info.data_type.entity))
    {
      return false;
    }

  /* weed out structural errors as failures */
  if (!qcol
      || (att && att->node_type != PT_DATA_TYPE)
      || (entity = att->info.data_type.entity) == NULL
      || entity->node_type != PT_NAME
      || ((vcls = entity->info.name.db_object) == NULL && (clsnam = entity->info.name.original) == NULL))
    {
      return false;
    }

  /* make sure we are dealing only with object types
   * that can be union vclass_compatible with vcls. */
  if (qcol->type_enum != PT_TYPE_OBJECT
      || (qcol_typ = qcol->data_type) == NULL
      || qcol_typ->node_type != PT_DATA_TYPE
      || (qcol_entity = qcol_typ->info.data_type.entity) == NULL
      || qcol_entity->node_type != PT_NAME || (qcol_typnam = qcol_entity->info.name.original) == NULL)
    {
      return false;
    }

  /* make sure we have the vclass */
  if (!vcls)
    {
      vcls = sm_find_class (clsnam);
    }
  if (!vcls)
    {
      return false;
    }

  /* return true iff att is a vclass & qcol is in att's query_spec list */
  for (specs = db_get_query_specs (vcls);
       specs && (spec = db_query_spec_string (specs)); specs = db_query_spec_next (specs))
    {
      qs_clsnam = pt_get_proxy_spec_name (spec);
      if (qs_clsnam && intl_identifier_casecmp (qs_clsnam, qcol_typnam) == 0)
        {
          return true;          /* att is vclass_compatible with qcol */
        }
    }

  return false;                 /* att is not vclass_compatible with qcol */
}

/*
 * pt_type_assignable () - determine if s_type is assignable to d_type
 *   return:  1 if s_type is assignable to d_type
 *   parser(in): the parser context
 *   d_type(in): a PT_DATA_TYPE node whose type_enum is PT_TYPE_OBJECT
 *   s_type(in): a PT_DATA_TYPE node whose type_enum is PT_TYPE_OBJECT
 */
static int
pt_type_assignable (PARSER_CONTEXT * parser, const PT_NODE * d_type, const PT_NODE * s_type)
{
  PT_NODE *src_type, *dest_type = NULL;

  /* a wildcard destination object accepts any other object type */
  if (!d_type || (d_type->node_type == PT_DATA_TYPE && !d_type->info.data_type.entity))
    {
      return 1;
    }

  /* weed out structural errors as failures */
  if (!s_type || (d_type && d_type->node_type != PT_DATA_TYPE))
    {
      return 0;
    }

  /* make sure we are dealing only with object types */
  if (s_type->type_enum != PT_TYPE_OBJECT)
    {
      return 0;
    }

  dest_type = d_type->info.data_type.entity;
  src_type = s_type->info.data_type.entity;
  if (!dest_type || !src_type)
    {
      return 0;
    }

  /* If the destination isn't resolved, resolve it. */
  if (!dest_type->info.name.db_object)
    {
      dest_type->info.name.db_object = sm_find_class (dest_type->info.name.original);
      dest_type->info.name.meta_class = PT_CLASS;
    }

  return (src_type->info.name.db_object == dest_type->info.name.db_object);
}

/*
 * pt_collection_assignable () - determine if s_col is assignable to d_col
 *   return:  1 if s_col is assignable to d_col
 *   parser(in): the parser context
 *   d_col(in): a PT_NODE whose type_enum is a PT_IS_COLLECTION_TYPE
 *   s_col(in): a PT_NODE whose type_enum is a PT_IS_COLLECTION_TYPE
 */
static int
pt_collection_assignable (PARSER_CONTEXT * parser, const PT_NODE * d_col, const PT_NODE * s_col)
{
  int assignable = 1;           /* innocent until proven guilty */

  if (!d_col || !s_col || !PT_IS_COLLECTION_TYPE (d_col->type_enum))
    {
      return 0;
    }

  /* NULL is assignable to any class type */
  if (PT_IS_NULL_NODE (s_col))
    {
      return 1;
    }

  /* make sure we are dealing only with collection types */
  if (!PT_IS_COLLECTION_TYPE (s_col->type_enum))
    {
      return 0;
    }

  /* can't assign multiset to a sequence */
  if (PT_IS_COLLECTION_TYPE (d_col->type_enum))
    {
      assignable = 0;
    }
  else if (!d_col->data_type)
    {
      /* the wildcard set (set of anything) can be assigned a set of
       * any type. */
      assignable = 1;
    }
  else if (!s_col->data_type)
    {
      /* in this case, we have a wild card set being assigned to a
       * non-wildcard set. */
      assignable = 0;
    }
  else
    {
      /* Check to see that every type in the source collection is in the
       * destination collection.  That is, the source types must be a
       * subset of the destination types.  There is no coercion allowed.
       */
      PT_NODE *st, *dt;
      int found;

      for (st = s_col->data_type; st != NULL; st = st->next)
        {
          found = 0;
          for (dt = d_col->data_type; dt != NULL; dt = dt->next)
            {
              if (st->type_enum == dt->type_enum)
                {
                  if ((st->type_enum != PT_TYPE_OBJECT) || (pt_type_assignable (parser, dt, st)))
                    {
                      found = 1;
                      break;
                    }
                }
            }

          if (!found)
            {
              assignable = 0;
              break;
            }
        }
    }

  return assignable;
}

/*
 * pt_collection_compatible () - determine if two collections
 *                               have compatible domains
 *   return:  1 if c1 and c2 have compatible domains
 *   parser(in): the parser context
 *   col1(in): a PT_NODE whose type_enum is PT_TYPE_OBJECT
 *   col2(in): a PT_NODE whose type_enum is PT_TYPE_OBJECT
 */
static int
pt_collection_compatible (PARSER_CONTEXT * parser, const PT_NODE * col1, const PT_NODE * col2)
{
  if (!col1 || !PT_IS_COLLECTION_TYPE (col1->type_enum) || !col2 || !PT_IS_COLLECTION_TYPE (col2->type_enum))
    {
      return 0;
    }

  return pt_collection_assignable (parser, col1, col2);
}
#endif

/*
 * pt_union_compatible () - determine if two select_list items are
 *                          union compatible
 *   return:  1 if item1 and item2 are union compatible.
 *   parser(in): the parser context
 *   item1(in): an element of a select_list or attribute_list
 *   item2(in): an element of a select_list or attribute_list
 *
 * Note :
 *   return 1 if:
 *   - item1 or  item2 is "NA", or
 *   - item1 and item2 have identical types, or
 *   - item1 is a literal that can be coerced to item2's type, or
 *   - item2 is a literal that can be coerced to item1's type.
 */

static PT_UNION_COMPATIBLE
pt_union_compatible (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * item1, PT_NODE * item2)
{
  PT_TYPE_ENUM typ1, typ2, common_type;
#if 0
  PT_NODE *data_type;
#endif

  typ1 = item1->type_enum;
  typ2 = item2->type_enum;

  if (typ1 == typ2 && typ1 != PT_TYPE_OBJECT && !PT_IS_COLLECTION_TYPE (typ1))
    {
      if (typ1 == PT_TYPE_NONE) /* is not compatible with anything */
        {
          return PT_UNION_INCOMP_CANNOT_FIX;
        }

      if (typ1 == PT_TYPE_MAYBE)
        {
          /* assume hostvars are compatible */
          return PT_UNION_COMP;
        }

      return PT_UNION_COMP;
    }

  if (PT_IS_NULL_NODE (item2))
    {
      /* NA is compatible with any type except PT_TYPE_NONE */
      return ((typ1 != PT_TYPE_NONE) ? PT_UNION_COMP : PT_UNION_INCOMP_CANNOT_FIX);
    }

  if (PT_IS_NULL_NODE (item1))
    {
      /* NA is compatible with any type except PT_TYPE_NONE */
      return ((typ2 != PT_TYPE_NONE) ? PT_UNION_COMP : PT_UNION_INCOMP_CANNOT_FIX);
    }

#if 1                           /* TODO - */
  if (typ1 != PT_TYPE_MAYBE && typ2 == PT_TYPE_MAYBE)
    {
      return PT_UNION_COMP;
    }
#endif

  common_type = typ1;

  if (common_type == PT_TYPE_NONE)      /* not union compatible */
    {
      return PT_UNION_INCOMP_CANNOT_FIX;
    }

  if (item1->node_type == PT_VALUE || item2->node_type == PT_VALUE)
    {
#if 0
      data_type = NULL;
      if (common_type == PT_TYPE_NUMERIC)
        {
          SEMAN_COMPATIBLE_INFO ci1, ci2;

          pt_get_compatible_info_from_node (item1, &ci1);
          pt_get_compatible_info_from_node (item2, &ci2);

          data_type = parser_new_node (parser, PT_DATA_TYPE);
          if (data_type == NULL)
            {
              return ER_OUT_OF_VIRTUAL_MEMORY;
            }

          data_type->info.data_type.precision =
            MAX ((ci1.prec - ci1.scale), (ci2.prec - ci2.scale)) + MAX (ci1.scale, ci2.scale);
          data_type->info.data_type.dec_scale = MAX (ci1.scale, ci2.scale);

          if (data_type->info.data_type.precision > DB_MAX_NUMERIC_PRECISION)
            {
#if 1                           /* TODO - */
              assert (false);   /* is not permit */
#endif
              data_type->info.data_type.dec_scale -= (data_type->info.data_type.precision - DB_MAX_NUMERIC_PRECISION);
              if (data_type->info.data_type.dec_scale < 0)
                {
                  data_type->info.data_type.dec_scale = 0;
                }
              data_type->info.data_type.precision = DB_MAX_NUMERIC_PRECISION;
            }
        }
#endif

      if (item1->type_enum == common_type && item2->type_enum == common_type)
        {
          return PT_UNION_COMP;
        }
    }
  else if (common_type == PT_TYPE_OBJECT || PT_IS_COLLECTION_TYPE (common_type))
    {
#if 1                           /* TODO - */
      assert (false);           /* not permit */
#endif
    }

  return PT_UNION_INCOMP;       /* not union compatible */
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * pt_get_compatible_info_from_node () -
 *   return:
 *   att(in):
 *   cinfo(out):
 */
static void
pt_get_compatible_info_from_node (const PT_NODE * att, SEMAN_COMPATIBLE_INFO * cinfo)
{
  assert (cinfo != NULL);

  cinfo->prec = cinfo->scale = 0;

  cinfo->type_enum = att->type_enum;

  switch (att->type_enum)
    {
    case PT_TYPE_INTEGER:
      cinfo->prec = 10;
      cinfo->scale = 0;
      break;
    case PT_TYPE_BIGINT:
      cinfo->prec = 19;
      cinfo->scale = 0;
      break;
    case PT_TYPE_NUMERIC:
      cinfo->prec = (att->data_type) ? att->data_type->info.data_type.precision : 0;
      cinfo->scale = (att->data_type) ? att->data_type->info.data_type.dec_scale : 0;
      break;
    case PT_TYPE_VARCHAR:
      cinfo->prec = (att->data_type) ? att->data_type->info.data_type.precision : 0;
      cinfo->scale = 0;
      break;
    default:
      break;
    }

}
#endif

/*
 * pt_check_data_default () - checks data_default for semantic errors
 *
 * result	    	 : modified data_default
 * parser(in)	    	 : parser context
 * data_default_list(in) : data default node
 */
static PT_NODE *
pt_check_data_default (PARSER_CONTEXT * parser, PT_NODE * data_default_list)
{
  PT_NODE *result;
  PT_NODE *default_value;
  PT_NODE *save_next;
  PT_NODE *node_ptr;
  PT_NODE *data_default;
  PT_NODE *prev;

  if (pt_has_error (parser))
    {
      /* do nothing */
      return data_default_list;
    }

  if (data_default_list == NULL || data_default_list->node_type != PT_DATA_DEFAULT)
    {
      /* do nothing */
      return data_default_list;
    }

  prev = NULL;
  for (data_default = data_default_list; data_default; data_default = data_default->next)
    {
      save_next = data_default->next;
      data_default->next = NULL;

      default_value = data_default->info.data_default.default_value;

      if (PT_IS_QUERY (default_value))
        {
          PT_NODE *subquery_list = NULL;
          default_value = pt_compile (parser, default_value);
          if (default_value == NULL || pt_has_error (parser))
            {
              /* compilation error */
              goto end;
            }
          subquery_list = pt_get_subquery_list (default_value);
          if (subquery_list && subquery_list->next)
            {
              /* cannot allow more than one column */
              char *str = pt_short_print (parser, default_value);
              PT_ERRORmf (parser, default_value, MSGCAT_SET_PARSER_SEMANTIC,
                          MSGCAT_SEMANTIC_NOT_SINGLE_VALUE, (str != NULL ? str : "\0"));
              goto end;
            }
          /* skip other checks */
          goto end;
        }

      result = pt_semantic_type (parser, data_default, NULL);
      if (result != NULL)
        {
          /* change data_default */
          if (prev)
            {
              prev->next = result;
            }
          else
            {
              data_default_list = result;
            }
          data_default = result;
        }
      else
        {
          /* an error has occurred, skip other checks */
          goto end;
        }

      node_ptr = NULL;
      (void) parser_walk_tree (parser, default_value, pt_find_default_expression, &node_ptr, NULL, NULL);
      if (node_ptr != NULL && node_ptr != default_value)
        {
          /* nested default expressions are not supported */
          PT_ERRORmf (parser, node_ptr, MSGCAT_SET_PARSER_SEMANTIC,
                      MSGCAT_SEMANTIC_DEFAULT_NESTED_EXPR_NOT_ALLOWED, pt_show_binopcode (node_ptr->info.expr.op));
          goto end;
        }

      node_ptr = NULL;
      (void) parser_walk_tree (parser, default_value, pt_find_aggregate_function, &node_ptr, NULL, NULL);
      if (node_ptr != NULL)
        {
          PT_ERRORmf (parser, node_ptr, MSGCAT_SET_PARSER_SEMANTIC,
                      MSGCAT_SEMANTIC_DEFAULT_EXPR_NOT_ALLOWED,
                      pt_show_function (node_ptr->info.function.function_type));
          goto end;
        }
    end:
      data_default->next = save_next;
      prev = data_default;
    }

  return data_default_list;
}

/*
 * pt_find_default_expression () - find a default expression
 *
 * result	  :
 * parser(in)	  :
 * tree(in)	  :
 * arg(in)	  : will point to default expression if any is found
 * continue_walk  :
 */
static PT_NODE *
pt_find_default_expression (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * tree, void *arg, int *continue_walk)
{
  PT_NODE **default_expr = (PT_NODE **) arg;

  if (tree == NULL || !PT_IS_EXPR_NODE (tree))
    {
      *continue_walk = PT_STOP_WALK;
    }

  switch (tree->info.expr.op)
    {
    case PT_SYS_DATE:
    case PT_SYS_DATETIME:
    case PT_USER:
    case PT_CURRENT_USER:
    case PT_UNIX_TIMESTAMP:
      *default_expr = tree;
      *continue_walk = PT_STOP_WALK;
      break;

    default:
      break;
    }

  return tree;
}

/*
 * pt_find_aggregate_function () - check if current expression contains an
 *				    aggregate function
 *
 * result	  :
 * parser(in)	  :
 * tree(in)	  :
 * arg(in)	  : will point to an aggregate function if any is found
 * continue_walk  :
 */
static PT_NODE *
pt_find_aggregate_function (PARSER_CONTEXT * parser, PT_NODE * tree, void *arg, int *continue_walk)
{
  PT_NODE **agg_function = (PT_NODE **) arg;

  if (tree == NULL || (!PT_IS_EXPR_NODE (tree) && !PT_IS_FUNCTION (tree)))
    {
      *continue_walk = PT_STOP_WALK;
    }

  if (PT_IS_FUNCTION (tree) && pt_is_aggregate_function (parser, tree))
    {
      *agg_function = tree;
      *continue_walk = PT_STOP_WALK;
    }

  return tree;
}

/*
 * pt_check_attribute_domain () - enforce composition hierarchy restrictions
 *      on a given list of attribute type definitions
 *   return:  none
 *   parser(in): the parser context
 *   attr_defs(in): a list of PT_ATTR_DEF nodes
 *   class_type(in): class, vclass, or proxy
 *   self(in): name of new class (for create case) or NULL (for alter case)
 *   stmt(in): current statement
 *
 * Note :
 * - enforce the composition hierarchy rules:
 *     1. enforce the (temporary?) restriction that no proxy may have an
 *        attribute whose type is heterogeneous set/multiset/sequence of
 *        some object and something else (homogeneous sets/sequences are OK)
 *     2. no attribute may have a domain of set(vclass), multiset(vclass)
 *        or sequence(vclass).
 *     3. an attribute of a class may NOT have a domain of a vclass or a proxy
 *        but may still have a domain of another class
 *     4. an attribute of a vclass may have a domain of a vclass or class
 *     5. an attribute of a proxy may have a domain of another proxy but not
 *        a class or vclass.
 *     6. an attribute cannot have a reusable OID class (a non-referable
 *        class) as a domain, neither directly nor as the domain of a set
 * - 'create class c (a c)' is not an error but a feature.
 */

static void
pt_check_attribute_domain (PARSER_CONTEXT * parser, PT_NODE * attr_defs,
                           PT_MISC_TYPE class_type, const char *self, UNUSED_ARG PT_NODE * stmt)
{
  PT_NODE *def, *att, *dtyp, *sdtyp;
  DB_OBJECT *cls;
  const char *att_nam, *typ_nam, *styp_nam;

  for (def = attr_defs; def != NULL && def->node_type == PT_ATTR_DEF; def = def->next)
    {
      att = def->info.attr_def.attr_name;
      att_nam = att->info.name.original;

      /* we don't allow sets/multisets/sequences of vclasses or reusable OID
         classes */
      if (PT_IS_COLLECTION_TYPE (def->type_enum))
        {
          for (dtyp = def->data_type; dtyp != NULL; dtyp = dtyp->next)
            {
              if ((dtyp->type_enum == PT_TYPE_OBJECT)
                  && (sdtyp = dtyp->info.data_type.entity)
                  && (sdtyp->node_type == PT_NAME) && (styp_nam = sdtyp->info.name.original))
                {
                  cls = sm_find_class (styp_nam);
                  if (cls != NULL)
                    {
                      if (db_is_vclass (cls))
                        {
                          PT_ERRORm (parser, att, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_WANT_NO_VOBJ_IN_SETS);
                          break;
                        }
                      else
                        {
                          PT_ERRORmf (parser, att, MSGCAT_SET_PARSER_SEMANTIC,
                                      MSGCAT_SEMANTIC_NON_REFERABLE_VIOLATION, styp_nam);
                          break;
                        }
                    }
                  else if (self != NULL && intl_identifier_casecmp (self, styp_nam) == 0)
                    {
                      PT_ERRORmf (parser, att, MSGCAT_SET_PARSER_SEMANTIC,
                                  MSGCAT_SEMANTIC_NON_REFERABLE_VIOLATION, styp_nam);
                      break;
                    }
                }
            }
        }

      if (def->type_enum == PT_TYPE_OBJECT
          && def->data_type
          && def->data_type->node_type == PT_DATA_TYPE
          && (dtyp = def->data_type->info.data_type.entity) != NULL
          && dtyp->node_type == PT_NAME && (typ_nam = dtyp->info.name.original) != NULL)
        {
          /* typ_nam must be a class in the database */
          cls = sm_find_class (typ_nam);
          if (!cls)
            {
              if (self != NULL && intl_identifier_casecmp (self, typ_nam) == 0)
                {
                  PT_ERRORmf (parser, att, MSGCAT_SET_PARSER_SEMANTIC,
                              MSGCAT_SEMANTIC_NON_REFERABLE_VIOLATION, typ_nam);
                }
              else
                {
                  PT_ERRORmf (parser, att, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_IS_NOT_DEFINED, typ_nam);
                }
            }
          else
            {
              /* if dtyp is 'user.class' then check that 'user' owns 'class' */
              dtyp->info.name.db_object = cls;
              pt_check_user_owns_class (parser, dtyp);

              PT_ERRORmf (parser, att, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_NON_REFERABLE_VIOLATION, typ_nam);

              switch (class_type)
                {
                case PT_CLASS:
                  /* an attribute of a class must be of type class */
                  if (db_is_vclass (cls))
                    {
                      PT_ERRORmf (parser, att, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_CAN_NOT_BE_VCLASS, att_nam);
                    }
                  break;
                case PT_VCLASS:
                  /* an attribute of a vclass must be of type vclass or class */
                  break;
                default:
                  break;
                }
            }
        }
    }                           /* for (def = attr_defs; ...) */

}

/*
 * pt_check_alter () -  semantic check an alter statement
 *   return:  none
 *   parser(in): the parser context used to derive the statement
 *   alter(in): an alter statement
 */
static void
pt_check_alter (PARSER_CONTEXT * parser, PT_NODE * alter)
{
  DB_OBJECT *db, *super;
  PT_ALTER_CODE code;
  PT_MISC_TYPE type;
  PT_NODE *name, *att, *attr;
  const char *cls_nam, *att_nam;
  DB_ATTRIBUTE *db_att;
  PT_NODE *cnstr;

  /* look up the class */
  name = alter->info.alter.entity_name;
  cls_nam = name->info.name.original;

  db = pt_find_class (parser, name);
  if (!db)
    {
      PT_ERRORmf (parser,
                  alter->info.alter.entity_name,
                  MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_CLASS_DOES_NOT_EXIST, cls_nam);
      return;
    }

  /* attach object */
  name->info.name.db_object = db;
  pt_check_user_owns_class (parser, name);

  /* check that class type is what it's supposed to be */
  if (alter->info.alter.entity_type == PT_MISC_DUMMY)
    {
      alter->info.alter.entity_type = pt_get_class_type (parser, db);
    }
  else
    {
      type = alter->info.alter.entity_type;
      if ((type == PT_CLASS && !db_is_class (db)) || (type == PT_VCLASS && !db_is_vclass (db)))
        {
          PT_ERRORmf2 (parser, alter,
                       MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_IS_NOT_A, cls_nam, pt_show_misc_type (type));
          return;
        }
    }

  type = alter->info.alter.entity_type;

  code = alter->info.alter.code;
  switch (code)
    {
    case PT_ADD_ATTR_MTHD:
      pt_check_attribute_domain (parser, alter->info.alter.alter_clause.attr_mthd.attr_def_list, type, NULL, alter);
      for (attr = alter->info.alter.alter_clause.attr_mthd.attr_def_list; attr; attr = attr->next)
        {
          attr->info.attr_def.data_default = pt_check_data_default (parser, attr->info.attr_def.data_default);
        }

      /* don't allow add new PK constraint */
      for (cnstr = alter->info.alter.constraint_list; cnstr != NULL; cnstr = cnstr->next)
        {
          if (cnstr->info.constraint.type == PT_CONSTRAIN_PRIMARY_KEY)
            {
              PT_ERRORm (parser, cnstr, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_ALTER_ADD_PRIMARY_KEY);
              break;
            }
        }

      break;

    case PT_CHANGE_ATTR:
      {
        PT_NODE *const att_def = alter->info.alter.alter_clause.attr_mthd.attr_def_list;

        if (att_def->next != NULL || att_def->node_type != PT_ATTR_DEF)
          {
            assert (false);
            break;
          }

        if (alter->info.alter.entity_type != PT_CLASS)
          {
            PT_ERRORm (parser, alter, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_ALTER_CHANGE_ONLY_TABLE);
            break;
          }

        pt_check_attribute_domain (parser, att_def, type, NULL, alter);
        for (attr = alter->info.alter.alter_clause.attr_mthd.attr_def_list; attr; attr = attr->next)
          {
            attr->info.attr_def.data_default = pt_check_data_default (parser, attr->info.attr_def.data_default);
          }

        /* don't allow add new PK constraint */
        for (cnstr = alter->info.alter.constraint_list; cnstr != NULL; cnstr = cnstr->next)
          {
            if (cnstr->info.constraint.type == PT_CONSTRAIN_PRIMARY_KEY)
              {
                PT_ERRORm (parser, cnstr, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_ALTER_ADD_PRIMARY_KEY);
                break;
              }
          }
      }
      break;

    case PT_RENAME_ATTR_MTHD:
      break;

    case PT_DROP_ATTR_MTHD:
      for (att = alter->info.alter.alter_clause.attr_mthd.attr_mthd_name_list;
           att != NULL && att->node_type == PT_NAME; att = att->next)
        {
          att_nam = att->info.name.original;
          db_att = db_get_attribute_force (db, att_nam);
          if (db_att)
            {
              /* an inherited attribute can not be dropped by the heir */
              super = (DB_OBJECT *) db_attribute_class (db_att);
              if (super != db)
                {
                  PT_ERRORmf2 (parser, att,
                               MSGCAT_SET_PARSER_SEMANTIC,
                               MSGCAT_SEMANTIC_HEIR_CANT_CHANGE_IT, att_nam, sm_class_name (super));
                }
            }
          else
            {
              PT_ERRORmf2 (parser, att,
                           MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_NOT_METHOD_OR_ATTR, att_nam, cls_nam);
            }
        }
      break;

    default:
      break;
    }
}

/*
 * attribute_name () -  return the name of this attribute
 *   return:  printable name of att
 *   parser(in): the parser context
 *   att(in): an attribute
 */
static const char *
attribute_name (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * att)
{
  if (!att)
    {
      return NULL;
    }

  if (att->node_type == PT_ATTR_DEF)
    {
      att = att->info.attr_def.attr_name;
    }

  if (att->node_type != PT_NAME)
    {
      return NULL;
    }

  return att->info.name.original;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * pt_attr_refers_to_self () - is this a self referencing attribute?
 *   return:  1 if attr refers to self
 *   parser(in): the parser context
 *   attr(in): an attribute
 *   self(in): name of vclass being created/altered
 */
static bool
pt_attr_refers_to_self (PARSER_CONTEXT * parser, PT_NODE * attr, const char *self)
{
  PT_NODE *type;

  if (!attr
      || attr->type_enum != PT_TYPE_OBJECT || !attr->data_type || attr->data_type->node_type != PT_DATA_TYPE || !self)
    {
      return false;
    }

  for (type = attr->data_type->info.data_type.entity; type && type->node_type == PT_NAME; type = type->next)
    {
      /* self is a string because in the create case,
       * self does not exist yet */
      if (!intl_identifier_casecmp (self, type->info.name.original))
        {
          return true;
        }
    }

  return false;
}
#endif

/*
 * pt_is_compatible_type () -
 *   return:  true on compatible type
 *   arg1_type(in):
 *   arg2_type(in):
 */
static bool
pt_is_compatible_type (const PT_TYPE_ENUM arg1_type, const PT_TYPE_ENUM arg2_type)
{
  bool is_compatible = false;

  assert (arg1_type != PT_TYPE_MAYBE);

  if (arg1_type == arg2_type)
    {
      is_compatible = true;
    }
#if 1                           /* TODO - */
  else if (arg1_type != PT_TYPE_MAYBE && arg2_type == PT_TYPE_MAYBE)
    {
      is_compatible = true;
    }
#endif
  else
    {
      switch (arg1_type)
        {
        case PT_TYPE_INTEGER:
        case PT_TYPE_BIGINT:
        case PT_TYPE_DOUBLE:
        case PT_TYPE_NUMERIC:
          switch (arg2_type)
            {
            case PT_TYPE_INTEGER:
            case PT_TYPE_BIGINT:
            case PT_TYPE_DOUBLE:
            case PT_TYPE_NUMERIC:
            case PT_TYPE_LOGICAL:      /* logical is compatible with these types */
              is_compatible = true;
              break;
            default:
              break;
            }
          break;

        default:
          break;
        }
    }

  return is_compatible;
}

/*
 * pt_check_vclass_attr_qspec_compatible () -
 *   return:
 *   parser(in):
 *   attr(in):
 *   col(in):
 */
static PT_UNION_COMPATIBLE
pt_check_vclass_attr_qspec_compatible (PARSER_CONTEXT * parser, PT_NODE * attr, PT_NODE * col)
{
  PT_UNION_COMPATIBLE c;

  assert (attr->type_enum != PT_TYPE_MAYBE);

  c = pt_union_compatible (parser, attr, col);

  if (c == PT_UNION_INCOMP && pt_is_compatible_type (attr->type_enum, col->type_enum))
    {
      c = PT_UNION_COMP;
    }

  return c;
}

/*
 * pt_check_vclass_union_spec () -
 *   return:
 *   parser(in):
 *   qry(in):
 *   attrs(in):
 */
static PT_NODE *
pt_check_vclass_union_spec (PARSER_CONTEXT * parser, PT_NODE * qry, PT_NODE * attrds)
{
  PT_NODE *attrd = NULL;
  PT_NODE *attrs1 = NULL;
  PT_NODE *attrs2 = NULL;
  int cnt1, cnt2;
  PT_NODE *att1 = NULL;
  PT_NODE *att2 = NULL;
  PT_NODE *result_stmt = NULL;

  /* parser assures us that it's a query but better make sure */
  if (!pt_is_query (qry))
    {
      return NULL;
    }

  if (!(qry->node_type == PT_UNION || qry->node_type == PT_DIFFERENCE || qry->node_type == PT_INTERSECTION))
    {
      assert (qry->node_type == PT_SELECT);
      return qry;
    }

  result_stmt = pt_check_vclass_union_spec (parser, qry->info.query.q.union_.arg1, attrds);
  if (pt_has_error (parser) || result_stmt == NULL)
    {
      return NULL;
    }
  result_stmt = pt_check_vclass_union_spec (parser, qry->info.query.q.union_.arg2, attrds);
  if (pt_has_error (parser) || result_stmt == NULL)
    {
      return NULL;
    }

  attrs1 = pt_get_select_list (parser, qry->info.query.q.union_.arg1);
  if (attrs1 == NULL)
    {
      return NULL;
    }
  attrs2 = pt_get_select_list (parser, qry->info.query.q.union_.arg2);
  if (attrs2 == NULL)
    {
      return NULL;
    }

  cnt1 = pt_length_of_select_list (attrs1, EXCLUDE_HIDDEN_COLUMNS);
  cnt2 = pt_length_of_select_list (attrs2, EXCLUDE_HIDDEN_COLUMNS);
  if (cnt1 != cnt2)
    {
      PT_ERRORmf2 (parser, attrs1, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_ARITY_MISMATCH, cnt1, cnt2);
      return NULL;
    }

  for (attrd = attrds, att1 = attrs1, att2 = attrs2; attrd != NULL;
       attrd = attrd->next, att1 = att1->next, att2 = att2->next)
    {
      assert (att1 != NULL);
      assert (att2 != NULL);

      /* we have a vclass attribute def context,
       * so do union vclass compatibility checks where applicable
       */
      if (attrd->type_enum == PT_TYPE_OBJECT)
        {
          return NULL;          /* not permit */
        }
    }

  assert (att1 == NULL);
  assert (att2 == NULL);

  return qry;
}

/*
 * pt_check_cyclic_reference_in_view_spec () -
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in):
 *   continue_walk(in):
 */
PT_NODE *
pt_check_cyclic_reference_in_view_spec (PARSER_CONTEXT * parser, PT_NODE * node, void *arg, int *continue_walk)
{
  const char *spec_name = NULL;
  PT_NODE *entity_name = NULL;
  DB_OBJECT *class_object;
  DB_QUERY_SPEC *db_query_spec;
  PT_NODE *result;
  PT_NODE *query_spec;
  const char *query_spec_string;
  const char *self = (const char *) arg;
  PARSER_CONTEXT *query_cache;

  if (node == NULL)
    {
      return node;
    }

  switch (node->node_type)
    {
    case PT_SPEC:

      entity_name = node->info.spec.entity_name;
      if (entity_name == NULL)
        {
          return node;
        }
      if (entity_name->node_type != PT_NAME)
        {
          return node;
        }

      spec_name = pt_get_name (entity_name);
      if (pt_str_compare (spec_name, self, CASE_INSENSITIVE) == 0)
        {
          PT_ERRORmf (parser, node, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_CYCLIC_REFERENCE_VIEW_SPEC, self);
          *continue_walk = PT_STOP_WALK;
          return node;
        }

      class_object = entity_name->info.name.db_object;
      if (!db_is_vclass (class_object))
        {
          return node;
        }

      query_cache = parser_create_parser ();
      if (query_cache == NULL)
        {
          return node;
        }

      db_query_spec = db_get_query_specs (class_object);
      while (db_query_spec)
        {
          query_spec_string = db_query_spec_string (db_query_spec);
          result = parser_parse_string_use_sys_charset (query_cache, query_spec_string);

          if (result != NULL)
            {
              query_spec = result;
              parser_walk_tree (query_cache, query_spec, pt_check_cyclic_reference_in_view_spec, arg, NULL, NULL);
            }

          if (pt_has_error (query_cache))
            {
              PT_ERROR (parser, node, query_cache->error_msgs->info.error_msg.error_message);

              *continue_walk = PT_STOP_WALK;
              break;
            }

          db_query_spec = db_query_spec_next (db_query_spec);
        }
      parser_free_parser (query_cache);
      break;

    default:
      break;
    }

  return node;
}

/*
 * pt_check_vclass_query_spec () -  do semantic checks on a vclass query spec
 *   return:
 *   parser(in): the parser context used to derive the qry
 *   qry(in): a vclass query specification
 *   attrs(in): the attributes of the vclass
 *   self(in): name of vclass being created/altered
 *
 * Note :
 * check that query_spec:
 * - count(attrs) == count(columns)
 * - corresponding attribute and query_spec column match type-wise
 */

static PT_NODE *
pt_check_vclass_query_spec (PARSER_CONTEXT * parser, PT_NODE * qry,
                            PT_NODE * attrs, const char *self, const bool do_semantic_check)
{
  PT_NODE *columns, *col, *attr;
  int col_count, attr_count;

  assert (do_semantic_check == false);

  if (!pt_is_query (qry))
    {
      return NULL;
    }

  if (do_semantic_check)
    {
      qry->do_not_replace_orderby = 1;
      qry = pt_semantic_check (parser, qry);
      if (pt_has_error (parser) || qry == NULL)
        {
          return NULL;
        }
    }

  (void) parser_walk_tree (parser, qry, pt_check_cyclic_reference_in_view_spec, (void *) self, NULL, NULL);
  if (pt_has_error (parser))
    {
      return NULL;
    }

  qry = pt_check_vclass_union_spec (parser, qry, attrs);
  if (pt_has_error (parser) || qry == NULL)
    {
      return NULL;
    }

  /* count(attrs) == count(query spec columns) */
  columns = pt_get_select_list (parser, qry);
  col_count = pt_length_of_select_list (columns, EXCLUDE_HIDDEN_COLUMNS);
  attr_count = pt_length_of_list (attrs);
  if (attr_count != col_count)
    {
      PT_ERRORmf2 (parser, qry, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_ATT_CNT_NE_COL_CNT, attr_count, col_count);
      return NULL;
    }

  /* foreach normal attribute and query_spec column do */
  for (attr = attrs, col = columns; attr != NULL && col != NULL; attr = attr->next, col = col->next)
    {
      if (col->node_type == PT_HOST_VAR)
        {
          PT_ERRORm (parser, col, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_HOSTVAR_NOT_ALLOWED_ON_QUERY_SPEC);
        }
      else if (attr->type_enum == PT_TYPE_NONE
               || attr->type_enum == PT_TYPE_OBJECT || PT_IS_COLLECTION_TYPE (attr->type_enum))
        {
#if 1                           /* TODO - */
          assert (false);       /* not permit */
#endif

          PT_ERRORmf2 (parser, col,
                       MSGCAT_SET_PARSER_SEMANTIC,
                       MSGCAT_SEMANTIC_ATT_INCOMPATIBLE_COL,
                       attribute_name (parser, attr), pt_short_print (parser, col));
        }
      else if (pt_check_vclass_attr_qspec_compatible (parser, attr, col) != PT_UNION_COMP)
        {
          PT_ERRORmf2 (parser, col,
                       MSGCAT_SET_PARSER_SEMANTIC,
                       MSGCAT_SEMANTIC_ATT_INCOMPATIBLE_COL,
                       attribute_name (parser, attr), pt_short_print (parser, col));
        }

    }

  return qry;
}

/*
 * pt_type_cast_vclass_query_spec_column () -
 *   return:  current or new column
 *   parser(in): the parser context used to derive the qry
 *   attr(in): the attributes of the vclass
 *   col(in): the query_spec column of the vclass
 */

PT_NODE *
pt_type_cast_vclass_query_spec_column (PARSER_CONTEXT * parser, PT_NODE * attr, PT_NODE * col)
{
  PT_UNION_COMPATIBLE c;
  PT_NODE *new_col, *new_dt, *next_col;

  /* guarantees PT_TYPE_OBJECT and SET types are fully compatible. */
  if (attr->type_enum == PT_TYPE_OBJECT || PT_IS_COLLECTION_TYPE (attr->type_enum))
    {
      return col;
    }

  c = pt_union_compatible (parser, attr, col);
  if (((c == PT_UNION_COMP)
       && (attr->type_enum == col->type_enum
           && PT_IS_PARAMETERIZED_TYPE (attr->type_enum)
           && attr->data_type
           && col->data_type)
       && ((attr->data_type->info.data_type.precision !=
            col->data_type->info.data_type.precision)
           || (attr->data_type->info.data_type.dec_scale !=
               col->data_type->info.data_type.dec_scale))) || (c == PT_UNION_INCOMP))
    {
      /* rewrite */
      next_col = col->next;
      col->next = NULL;
      new_col = new_dt = NULL;

      new_col = parser_new_node (parser, PT_EXPR);
      if (new_col == NULL)
        {
          PT_ERRORm (parser, col, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_OUT_OF_MEMORY);
          return col;           /* give up */
        }

      new_dt = parser_new_node (parser, PT_DATA_TYPE);
      if (new_dt == NULL)
        {
          PT_ERRORm (parser, col, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_OUT_OF_MEMORY);
          parser_free_tree (parser, new_col);
          return col;           /* give up */
        }

      /* move alias */
      new_col->line_number = col->line_number;
      new_col->column_number = col->column_number;
      new_col->alias_print = col->alias_print;
      col->alias_print = NULL;
      new_dt->type_enum = attr->type_enum;
      if (attr->data_type)
        {
          new_dt->info.data_type.precision = attr->data_type->info.data_type.precision;
          new_dt->info.data_type.dec_scale = attr->data_type->info.data_type.dec_scale;
          new_dt->info.data_type.collation_id = attr->data_type->info.data_type.collation_id;
          assert (new_dt->info.data_type.collation_id >= 0);
        }
      new_col->type_enum = new_dt->type_enum;
      new_col->info.expr.op = PT_CAST;
      new_col->info.expr.cast_type = new_dt;
      new_col->info.expr.arg1 = col;
      new_col->next = next_col;
      new_col->data_type = parser_copy_tree_list (parser, new_dt);
      col = new_col;
    }

  return col;
}

/*
 * pt_type_cast_vclass_query_spec () -
 *   return:
 *   parser(in):
 *   qry(in):
 *   attrs(in):
 */
static PT_NODE *
pt_type_cast_vclass_query_spec (PARSER_CONTEXT * parser, PT_NODE * qry, PT_NODE * attrs)
{
  PT_NODE *columns, *col, *attr;
  PT_NODE *new_col, *prev_col;

  /* parser assures us that it's a query but better make sure */
  if (!pt_is_query (qry))
    {
      return NULL;
    }

  if (qry->node_type != PT_SELECT)
    {
      if (!pt_type_cast_vclass_query_spec (parser,
                                           qry->info.query.q.union_.arg1,
                                           attrs)
          || pt_has_error (parser)
          || (!pt_type_cast_vclass_query_spec (parser, qry->info.query.q.union_.arg2, attrs)) || pt_has_error (parser))
        {
          return NULL;
        }

      return qry;               /* already done */
    }

  assert (qry->node_type == PT_SELECT);

  columns = pt_get_select_list (parser, qry);

  /* foreach normal attribute and query_spec column do */
  attr = attrs;
  col = columns;
  prev_col = NULL;

  while (attr && col)
    {
      new_col = pt_type_cast_vclass_query_spec_column (parser, attr, col);
      if (new_col != col)
        {
          if (prev_col == NULL)
            {
              qry->info.query.q.select.list = new_col;
              qry->type_enum = new_col->type_enum;
              if (qry->data_type)
                {
                  parser_free_tree (parser, qry->data_type);
                }
              qry->data_type = parser_copy_tree_list (parser, new_col->data_type);
            }
          else
            {
              prev_col->next = new_col;
            }

          col = new_col;
        }

      /* save previous link */
      prev_col = col;
      /* advance to next attribute and column */
      attr = attr->next;
      col = col->next;
    }

  return qry;
}

/*
 * pt_check_default_vclass_query_spec () -
 *   return: new attrs node including default values
 *   parser(in):
 *   qry(in):
 *   attrs(in):
 *
 * For all those view attributes that don't have implicit default values,
 * copy the default values from the original table
 *
 * NOTE: there are two ways attrs is constructed at this point:
 *  - stmt: create view (attr_list) as select... and the attrs will be created
 * directly from the statement
 *  - stmt: create view as select... and the attrs will be created from the
 * query's select list
 * In both cases, each attribute in attrs will correspond to the column in the
 * select list at the same index
 */
static PT_NODE *
pt_check_default_vclass_query_spec (PARSER_CONTEXT * parser, PT_NODE * qry, PT_NODE * attrs)
{
  PT_NODE *attr, *col;
  PT_NODE *columns = pt_get_select_list (parser, qry);
  PT_NODE *default_data = NULL;
  PT_NODE *default_value = NULL;
  PT_NODE *spec, *entity_name;
  DB_OBJECT *obj;
  DB_ATTRIBUTE *col_attr;

  /* Import default value from referenced table for those attributes in the
   * the view that have no default value.
   */
  for (attr = attrs, col = columns; attr && col; attr = attr->next, col = col->next)
    {
      if (!attr->info.attr_def.data_default)
        {
          if (col->node_type == PT_NAME)
            {
              /* found matching column */
              if (col->info.name.spec_id == 0)
                {
                  continue;
                }
              spec = (PT_NODE *) col->info.name.spec_id;
              entity_name = spec->info.spec.entity_name;
              if (entity_name == NULL || !PT_IS_NAME_NODE (entity_name))
                {
                  continue;
                }
              obj = entity_name->info.name.db_object;
              if (!obj)
                {
                  continue;
                }
              col_attr = db_get_attribute_force (obj, col->info.name.original);
              if (!col_attr)
                {
                  continue;
                }

              if (DB_IS_NULL (&col_attr->default_value.value)
                  && (col_attr->default_value.default_expr == DB_DEFAULT_NONE))
                {
                  /* don't create any default node if default value is null
                   * unless default expression type is not DB_DEFAULT_NONE
                   */
                  continue;
                }

              if (col_attr->default_value.default_expr == DB_DEFAULT_NONE)
                {
                  default_value = pt_dbval_to_value (parser, &col_attr->default_value.value);
                  if (!default_value)
                    {
                      PT_ERRORm (parser, qry, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_OUT_OF_MEMORY);
                      goto error;
                    }

                  default_data = parser_new_node (parser, PT_DATA_DEFAULT);
                  if (!default_data)
                    {
                      parser_free_tree (parser, default_value);
                      PT_ERRORm (parser, qry, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_OUT_OF_MEMORY);
                      goto error;
                    }
                  default_data->info.data_default.default_value = default_value;
                  default_data->info.data_default.default_expr = DB_DEFAULT_NONE;
                }
              else
                {
                  default_value = parser_new_node (parser, PT_EXPR);
                  if (default_value)
                    {
                      default_value->info.expr.op =
                        pt_op_type_from_default_expr_type (col_attr->default_value.default_expr);
                    }
                  else
                    {
                      PT_ERRORm (parser, qry, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_OUT_OF_MEMORY);
                      goto error;
                    }

                  default_data = parser_new_node (parser, PT_DATA_DEFAULT);
                  if (!default_data)
                    {
                      parser_free_tree (parser, default_value);
                      PT_ERRORm (parser, qry, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_OUT_OF_MEMORY);
                      goto error;
                    }
                  default_data->info.data_default.default_value = default_value;
                  default_data->info.data_default.default_expr = col_attr->default_value.default_expr;
                }
              attr->info.attr_def.data_default = default_data;
            }
        }
    }

  return attrs;

error:
  parser_free_tree (parser, attrs);
  return NULL;
}

/*
 * pt_check_create_view () - do semantic checks on a create vclass statement
 *   or an "ALTER VIEW AS SELECT" statement
 *
 *   return:  none
 *   parser(in): the parser context used to derive the statement
 *   stmt(in): a create vclass statement
 *
 * Note :
 * This function is also called when doing "ALTER VIEW xxx AS SELECT ...",
 * which is a simplified case, since it does not support class inheritance.
 *
 * check that
 * - stmt's query_specs are union compatible with each other
 * - if no attributes are given, derive them from the query_spec columns
 * - if an attribute has no data type then derive it from its
 *   matching query_spec column
 * - corresponding attribute and query_spec column must match type-wise
 * - count(attributes) == count(query_spec columns)
 * - we allow order by clauses in the queries
 */

static void
pt_check_create_view (PARSER_CONTEXT * parser, PT_NODE * stmt)
{
  PT_NODE *all_attrs = NULL;
  PT_NODE *result_stmt = NULL;
  PT_NODE **qry_specs_ptr = NULL;
  PT_NODE *crt_qry = NULL;
  PT_NODE **prev_qry_link_ptr = NULL;
  UNUSED_VAR PT_NODE **attr_def_list_ptr = NULL;
  PT_NODE *prev_qry;
  const char *name = NULL;
  int attr_count = 0;

  assert (parser != NULL);

  if (stmt == NULL || stmt->node_type != PT_CREATE_ENTITY)
    {
      return;
    }

  assert (stmt->node_type == PT_CREATE_ENTITY);

  if (stmt->info.create_entity.entity_type != PT_VCLASS || stmt->info.create_entity.entity_name == NULL)
    {
      return;
    }

  name = stmt->info.create_entity.entity_name->info.name.original;
  qry_specs_ptr = &stmt->info.create_entity.as_query_list;
  attr_def_list_ptr = &stmt->info.create_entity.attr_def_list;

  if (*qry_specs_ptr == NULL || pt_has_error (parser))
    {
      return;
    }

  assert ((*qry_specs_ptr)->next == NULL);

  prev_qry = NULL;
  for (crt_qry = *qry_specs_ptr, prev_qry_link_ptr = qry_specs_ptr;
       crt_qry != NULL; prev_qry_link_ptr = &crt_qry->next, crt_qry = crt_qry->next)
    {
      PT_NODE *const save_next = crt_qry->next;

      crt_qry->next = NULL;

      /* TODO This seems to flag too many queries as view specs because
       * it also traverses the tree to subqueries. It might need a
       * pre_function that returns PT_STOP_WALK for subqueries.
       */
      result_stmt = parser_walk_tree (parser, crt_qry, pt_set_is_view_spec, NULL, NULL, NULL);
      if (result_stmt == NULL)
        {
          assert (false);
          PT_ERRORm (parser, stmt, MSGCAT_SET_ERROR, -(ER_GENERIC_ERROR));
          return;
        }
      crt_qry = result_stmt;

      crt_qry->do_not_replace_orderby = 1;
      result_stmt = pt_semantic_check (parser, crt_qry);
      if (pt_has_error (parser))
        {
          if (prev_qry)
            {
              prev_qry->next = save_next;
            }
          crt_qry = NULL;
          (*prev_qry_link_ptr) = crt_qry;
          return;
        }

      if (result_stmt == NULL)
        {
          assert (false);
          PT_ERRORm (parser, stmt, MSGCAT_SET_ERROR, -(ER_GENERIC_ERROR));
          return;
        }

      crt_qry = result_stmt;

      crt_qry->next = save_next;
      (*prev_qry_link_ptr) = crt_qry;
      prev_qry = crt_qry;
    }

  attr_count = pt_number_of_attributes (parser, stmt, &all_attrs);
#if 1
  if (attr_count <= 0)
    {
      assert (false);
      PT_ERRORm (parser, stmt, MSGCAT_SET_ERROR, -(ER_GENERIC_ERROR));
      return;
    }
#else
  /* if no attributes are given, try to
     derive them from the query_spec columns. */
  if (attr_count <= 0)
    {
      PT_NODE *crt_attr = NULL;
      PT_NODE *const qspec_attr = pt_get_select_list (parser, *qry_specs_ptr);

      assert (attr_count == 0);
      assert (*attr_def_list_ptr == NULL);

      for (crt_attr = qspec_attr; crt_attr != NULL; crt_attr = crt_attr->next)
        {
          PT_NODE *s_attr = NULL;

          if (crt_attr->alias_print)
            {
              s_attr = crt_attr;
            }
          else
            {
              /* allow attributes to be derived only from path expressions. */
              s_attr = pt_get_end_path_node (crt_attr);
              if (s_attr->node_type != PT_NAME)
                {
                  s_attr = NULL;
                }
            }

          if (s_attr == NULL)
            {
              PT_ERRORmf (parser, stmt,
                          MSGCAT_SET_PARSER_SEMANTIC,
                          MSGCAT_SEMANTIC_MISSING_ATTR_NAME, pt_short_print (parser, crt_attr));
              return;
            }
          else if (s_attr->node_type == PT_HOST_VAR)
            {
              PT_ERRORm (parser, s_attr, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_HOSTVAR_NOT_ALLOWED_ON_QUERY_SPEC);
              return;
            }
          else if (PT_IS_NULL_NODE (s_attr))
            {
              PT_ERRORm (parser, s_attr, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_NULL_NOT_ALLOWED_ON_QUERY_SPEC);
              return;
            }

          derived_attr = pt_derive_attribute (parser, s_attr);
          if (derived_attr == NULL)
            {
              PT_ERRORm (parser, stmt, MSGCAT_SET_ERROR, -(ER_GENERIC_ERROR));
              return;
            }

          *attr_def_list_ptr = parser_append_node (derived_attr, *attr_def_list_ptr);
        }

      attr_count = pt_number_of_attributes (parser, stmt, &all_attrs);
    }
#endif

  assert (attr_count > 0);

  /* do other checks on query specs */
  for (crt_qry = *qry_specs_ptr, prev_qry_link_ptr = qry_specs_ptr;
       crt_qry != NULL; prev_qry_link_ptr = &crt_qry->next, crt_qry = crt_qry->next)
    {
      PT_NODE *const save_next = crt_qry->next;

      crt_qry->next = NULL;

      result_stmt = pt_check_vclass_query_spec (parser, crt_qry, all_attrs, name, false);
      if (pt_has_error (parser))
        {
          return;
        }
      if (result_stmt == NULL)
        {
          PT_ERRORm (parser, stmt, MSGCAT_SET_ERROR, -(ER_GENERIC_ERROR));
          return;
        }
      crt_qry = result_stmt;

      all_attrs = pt_check_default_vclass_query_spec (parser, crt_qry, all_attrs);
      if (pt_has_error (parser))
        {
          return;
        }
      if (all_attrs == NULL)
        {
          PT_ERRORm (parser, stmt, MSGCAT_SET_ERROR, -(ER_GENERIC_ERROR));
          return;
        }

      result_stmt = pt_type_cast_vclass_query_spec (parser, crt_qry, all_attrs);
      if (pt_has_error (parser))
        {
          return;
        }
      if (result_stmt == NULL)
        {
          assert (false);
          PT_ERRORm (parser, stmt, MSGCAT_SET_ERROR, -(ER_GENERIC_ERROR));
          return;
        }
      crt_qry = result_stmt;

      crt_qry->next = save_next;
      (*prev_qry_link_ptr) = crt_qry;
    }
}

/*
 * pt_check_create_user () - semantic check a create user statement
 * return	: none
 * parser(in)	: the parser context
 * node(in)	: create user node
 */
static void
pt_check_create_user (PARSER_CONTEXT * parser, PT_NODE * node)
{
  PT_NODE *user_name;
  const char *name;
  int name_upper_size;

  if (!node)
    {
      return;
    }
  if (node->node_type != PT_CREATE_USER)
    {
      return;
    }

  user_name = node->info.create_user.user_name;
  if (user_name->node_type != PT_NAME)
    {
      return;
    }

  name = user_name->info.name.original;
  if (name == NULL)
    {
      return;
    }
  name_upper_size = strlen (name);
  if (name_upper_size >= DB_MAX_USER_LENGTH)
    {
      PT_ERRORm (parser, user_name, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_USER_NAME_TOO_LONG);
    }
}

/*
 * pt_check_create_entity () - semantic check a create class/vclass
 *   return:  none
 *   parser(in): the parser context used to derive the statement
 *   node(in): a create statement
 */

static void
pt_check_create_entity (PARSER_CONTEXT * parser, PT_NODE * node)
{
  PT_NODE *qry_specs, *name, *create_like;
  PT_NODE *all_attrs, *attr;
  PT_MISC_TYPE entity_type;
  DB_OBJECT *db_obj, *existing_entity;

  entity_type = node->info.create_entity.entity_type;
  assert (entity_type == PT_CLASS || node->info.create_entity.is_shard == 0);

  if (entity_type != PT_CLASS && entity_type != PT_VCLASS)
    {
      /* control should never reach here if tree is well-formed */
      assert (false);
      return;
    }

  /* check name doesn't already exist as a class */
  name = node->info.create_entity.entity_name;
  existing_entity = pt_find_class (parser, name);
  if (existing_entity != NULL)
    {
      if (!(entity_type == PT_VCLASS && node->info.create_entity.or_replace == 1 && db_is_vclass (existing_entity)))
        {
          PT_ERRORmf (parser, name, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_CLASS_EXISTS, name->info.name.original);
          return;
        }
    }

  pt_check_user_exists (parser, name);

  /* check uniqueness of non-inherited attribute names */
  all_attrs = node->info.create_entity.attr_def_list;
  pt_check_unique_attr (parser, name->info.name.original, all_attrs, PT_ATTR_DEF);

  /* enforce composition hierarchy restrictions on attr type defs */
  pt_check_attribute_domain (parser, all_attrs, entity_type, name->info.name.original, node);

  if (entity_type == PT_VCLASS)
    {
      /* The grammar restricts table options to CREATE VIEW */
      assert (node->info.create_entity.is_shard == 0);
    }

  qry_specs = node->info.create_entity.as_query_list;
  if (node->info.create_entity.entity_type == PT_CLASS)
    {
      /* simple CLASSes must not have any query specs */
      if (qry_specs)
        {
          PT_ERRORm (parser, qry_specs, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_CLASS_NO_QUERY_SPEC);
          return;
        }
    }
  else                          /* must be a CREATE VCLASS statement */
    {
      pt_check_create_view (parser, node);
    }

  /* check that all constraints look valid */
  if (!pt_has_error (parser))
    {
      (void) pt_check_constraints (parser, node);
    }

  create_like = node->info.create_entity.create_like;
  if (create_like != NULL)
    {
      assert (entity_type == PT_CLASS);

      db_obj = pt_find_class (parser, create_like);
      if (db_obj == NULL)
        {
          PT_ERRORmf (parser, create_like, MSGCAT_SET_PARSER_SEMANTIC,
                      MSGCAT_SEMANTIC_CLASS_DOES_NOT_EXIST, create_like->info.name.original);
          return;
        }
      else
        {
          create_like->info.name.db_object = db_obj;
          pt_check_user_owns_class (parser, create_like);
          if (!db_is_class (db_obj))
            {
              PT_ERRORmf2 (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
                           MSGCAT_SEMANTIC_IS_NOT_A, create_like->info.name.original, pt_show_misc_type (PT_CLASS));
              return;
            }
        }
    }

  for (attr = node->info.create_entity.attr_def_list; attr; attr = attr->next)
    {
      attr->info.attr_def.data_default = pt_check_data_default (parser, attr->info.attr_def.data_default);
    }

  pt_check_shard_key (parser, node);
}

static void
pt_check_shard_key (PARSER_CONTEXT * parser, PT_NODE * node)
{
  PT_NODE *tbl_opt, *tbl_opt_shard_by, *shard_key;
  PT_NODE *cnstr;
  PT_NODE *pk_first_col;
  PT_NODE *attr;

  tbl_opt_shard_by = shard_key = pk_first_col = NULL;   /* init */

  if (node->info.create_entity.is_shard == 0)
    {
      /* is not shard table */
      return;
    }

  assert (node->info.create_entity.entity_type == PT_CLASS);

  if (node->info.create_entity.create_like != NULL)
    {
      /* is shard table like */
      return;
    }

  for (tbl_opt = node->info.create_entity.table_option_list; tbl_opt != NULL; tbl_opt = tbl_opt->next)
    {
      assert (tbl_opt->node_type == PT_TABLE_OPTION);
      assert (tbl_opt->info.table_option.option == PT_TABLE_OPTION_SHARD_KEY);

      if (tbl_opt_shard_by != NULL)
        {
          PT_ERRORmf (parser, node,
                      MSGCAT_SET_PARSER_SEMANTIC,
                      MSGCAT_SEMANTIC_DUPLICATE_TABLE_OPTION, parser_print_tree (parser, tbl_opt));
          return;
        }
      else
        {
          tbl_opt_shard_by = tbl_opt;
        }
    }

  assert (tbl_opt_shard_by != NULL);
  if (tbl_opt_shard_by == NULL)
    {
      /* is impossible */
      PT_ERRORf (parser, node, "check syntax at %s, expecting 'SHARD BY' expression.", pt_short_print (parser, node));
      return;
    }

  shard_key = tbl_opt_shard_by->info.table_option.val;

  assert (shard_key != NULL);
  assert (shard_key->node_type == PT_NAME);
  if (shard_key == NULL || shard_key->node_type != PT_NAME)
    {
      /* is impossible */
      PT_ERRORf (parser, node, "check syntax at %s, expecting 'SHARD BY' expression.", pt_short_print (parser, node));
      return;
    }

  /* get PK */
  for (cnstr = node->info.create_entity.constraint_list; cnstr != NULL; cnstr = cnstr->next)
    {
      if (cnstr->info.constraint.type == PT_CONSTRAIN_PRIMARY_KEY)
        {
          break;
        }
    }

  if (cnstr == NULL)
    {
      /* not permit shard table without PK */
      PT_ERRORf (parser, node,
                 "check syntax at %s, expecting 'PRIMARY KEY' expression.", pt_short_print (parser, node));
      return;
    }

  /* the first column of pk constraint should be shard key column */
  pk_first_col = cnstr->info.constraint.un.primary_key.attrs;

  assert (pk_first_col != NULL);
  assert (pk_first_col->node_type == PT_NAME);
  if (pk_first_col == NULL || pk_first_col->node_type != PT_NAME)
    {
      /* is impossible */
      PT_ERRORf (parser, node,
                 "check syntax at %s, expecting 'PRIMARY KEY' expression.", pt_short_print (parser, node));
      return;
    }

  /* check attribute name in shard by definition */
  if (intl_identifier_casecmp (shard_key->info.name.original, pk_first_col->info.name.original) != 0)
    {
      PT_ERRORmf (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
                  MSGCAT_SEMANTIC_WRONG_SHARDKEY_POSITION_IN_PK, shard_key->info.name.original);
      return;
    }

  for (attr = node->info.create_entity.attr_def_list; attr != NULL; attr = attr->next)
    {
      /* check attribute name in attr definition */
      if (intl_identifier_casecmp
          (attr->info.attr_def.attr_name->info.name.original, shard_key->info.name.original) == 0)
        {
          /* check attribute type, shard key type should be string type */
          if (attr->type_enum != PT_TYPE_VARCHAR)
            {
              PT_ERRORmf (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
                          MSGCAT_SEMANTIC_WRONG_SHARDKEY_TYPE, shard_key->info.name.original);
            }

          return;
        }
    }

  PT_ERRORmf2 (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
               MSGCAT_SEMANTIC_NOT_ATTRIBUTE_OF,
               shard_key->info.name.original, node->info.create_entity.entity_name->info.name.original);
}

/*
 * pt_check_create_index () - semantic check a create index
 *   return:  none
 *   parser(in): the parser context used to derive the statement
 *   node(in): a create index statement
 */

static void
pt_check_create_index (PARSER_CONTEXT * parser, PT_NODE * node)
{
  PT_NODE *name, *col, *col_expr;
  DB_OBJECT *db_obj;

  /* check that there trying to create an index on a class */
  name = node->info.index.indexed_class->info.spec.entity_name;
  db_obj = sm_find_class (name->info.name.original);

  if (db_obj == NULL)
    {
      PT_ERRORmf (parser, name, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_IS_NOT_A_CLASS, name->info.name.original);
      return;
    }
  else
    {
      /* make sure it's not a virtual class */
      if (db_is_class (db_obj) == 0)
        {
          PT_ERRORm (parser, name, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_NO_INDEX_ON_VCLASS);
          return;
        }

      /* Check if the columns are valid. We only allow expressions and
       * attribute names. The actual expressions will be validated later,
       * we're only interested in the node type
       */
      for (col = node->info.index.column_names; col != NULL; col = col->next)
        {
          if (col->node_type != PT_SORT_SPEC)
            {
              assert_release (col->node_type == PT_SORT_SPEC);
              return;
            }
          col_expr = col->info.sort_spec.expr;
          if (col_expr == NULL)
            {
              continue;
            }
          if (col_expr->node_type == PT_NAME)
            {
              /* make sure this is not a parameter */
              if (col_expr->info.name.meta_class != PT_NORMAL)
                {
                  PT_ERRORmf (parser, col_expr, MSGCAT_SET_PARSER_SEMANTIC,
                              MSGCAT_SEMANTIC_INVALID_INDEX_COLUMN, pt_short_print (parser, col_expr));
                  return;
                }
            }
          else if (col_expr->node_type != PT_EXPR)
            {
              PT_ERRORmf (parser, col_expr, MSGCAT_SET_PARSER_SEMANTIC,
                          MSGCAT_SEMANTIC_INVALID_INDEX_COLUMN, pt_short_print (parser, col_expr));
              return;
            }
        }
      /* make sure we don't mix up index types */

      if (pt_has_error (parser))
        {
          return;
        }

      name->info.name.db_object = db_obj;

      /* check that there is only one column to index on */
      pt_check_unique_attr (parser, NULL, node->info.index.column_names, PT_SORT_SPEC);
      if (pt_has_error (parser))
        {
          return;
        }
      pt_check_user_owns_class (parser, name);
      if (pt_has_error (parser))
        {
          return;
        }

    }

}

/*
 * pt_check_drop () - do semantic checks on the drop statement
 *   return:  none
 *   parser(in): the parser context used to derive the statement
 *   node(in): a statement
 */
static void
pt_check_drop (PARSER_CONTEXT * parser, PT_NODE * node)
{
  PT_NODE *temp;
  PT_NODE *name;
  PT_NODE *chk_parent = NULL;
  DB_OBJECT *db_obj;
  DB_ATTRIBUTE *attributes;

  if (node->info.drop.if_exists)
    {
      PT_NODE *prev_node, *free_node, *tmp1, *tmp2;
      prev_node = free_node = node->info.drop.spec_list;

      while ((free_node != NULL) && (free_node->node_type == PT_SPEC))
        {
          const char *cls_name;
          /* check if class name exists. if not, we remove the corresponding
             node from spec_list.
           */
          if ((name = free_node->info.spec.entity_name) != NULL
              && name->node_type == PT_NAME
              && (cls_name = name->info.name.original) != NULL && (db_obj = sm_find_class (cls_name)) == NULL)
            {
              if (free_node == node->info.drop.spec_list)
                {
                  node->info.drop.spec_list = node->info.drop.spec_list->next;
                  prev_node = free_node->next;
                  parser_free_node (parser, free_node);
                  free_node = node->info.drop.spec_list;
                }
              else
                {
                  prev_node->next = free_node->next;
                  parser_free_node (parser, free_node);
                  free_node = prev_node->next;
                }
            }
          else
            {
              prev_node = free_node;
              free_node = free_node->next;
            }
        }
      /* For each class, we check if it has previously been placed in
         spec_list. We also check if every class has a superclass marked
         with PT_ALL previously placed in spec_list. If any of the two cases
         above occurs, we mark for deletion the corresponding node in
         spec_list. Marking is done by setting the entity_name as NULL.
       */

      if (node->info.drop.spec_list && (node->info.drop.spec_list)->next)
        {
          tmp1 = (node->info.drop.spec_list)->next;
          while ((tmp1 != NULL) && (tmp1->node_type == PT_SPEC))
            {
              tmp2 = node->info.drop.spec_list;
              while ((tmp2 != NULL) && (tmp2 != tmp1) && (tmp2->node_type == PT_SPEC))
                {
                  DB_OBJECT *db_obj1, *db_obj2;
                  PT_NODE *name1, *name2;
                  const char *cls_name1, *cls_name2;

                  name1 = tmp1->info.spec.entity_name;
                  name2 = tmp2->info.spec.entity_name;
                  if (name1 && name2)
                    {
                      cls_name1 = name1->info.name.original;
                      cls_name2 = name2->info.name.original;

                      db_obj1 = sm_find_class (cls_name1);
                      db_obj2 = sm_find_class (cls_name2);

                      if ((db_obj1 == db_obj2))
                        {
                          parser_free_node (parser, name1);
                          tmp1->info.spec.entity_name = NULL;
                          break;
                        }
                    }

                  tmp2 = tmp2->next;
                }

              tmp1 = tmp1->next;
            }
        }

      /* now we remove the nodes with entity_name NULL */

      prev_node = free_node = node->info.drop.spec_list;

      while ((free_node != NULL) && (free_node->node_type == PT_SPEC))
        {

          if ((name = free_node->info.spec.entity_name) == NULL)
            {
              if (free_node == node->info.drop.spec_list)
                {
                  node->info.drop.spec_list = node->info.drop.spec_list->next;
                  prev_node = free_node->next;
                  parser_free_node (parser, free_node);
                  free_node = node->info.drop.spec_list;
                }
              else
                {
                  prev_node->next = free_node->next;
                  parser_free_node (parser, free_node);
                  free_node = prev_node->next;
                }
            }
          else
            {
              prev_node = free_node;
              free_node = free_node->next;
            }
        }

    }

  /* Replace each Entity Spec with an Equivalent flat list */
  parser_walk_tree (parser, node, pt_flat_spec_pre, &chk_parent, pt_continue_walk, NULL);

  if (node->info.drop.entity_type != PT_MISC_DUMMY)
    {
      const char *cls_nam;
      PT_MISC_TYPE typ = node->info.drop.entity_type;

      /* verify declared class type is correct */
      for (temp = node->info.drop.spec_list; temp && temp->node_type == PT_SPEC; temp = temp->next)
        {
          if ((name = temp->info.spec.entity_name) != NULL
              && name->node_type == PT_NAME
              && (cls_nam = name->info.name.original) != NULL && (db_obj = sm_find_class (cls_nam)) != NULL)
            {
              name->info.name.db_object = db_obj;
              pt_check_user_owns_class (parser, name);
              if ((typ == PT_CLASS && !db_is_class (db_obj)) || (typ == PT_VCLASS && !db_is_vclass (db_obj)))
                {
                  PT_ERRORmf2 (parser, node,
                               MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_IS_NOT_A, cls_nam, pt_show_misc_type (typ));
                }
            }
        }
    }

  /* for the classes to drop, check if a TEXT attr is defined on the class,
     and if defined, drop the reference class for the attr */
  for (temp = node->info.drop.spec_list; temp && temp->node_type == PT_SPEC; temp = temp->next)
    {
      const char *cls_nam;

      if ((name = temp->info.spec.entity_name) != NULL
          && name->node_type == PT_NAME
          && (cls_nam = name->info.name.original) != NULL && (db_obj = sm_find_class (cls_nam)) != NULL)
        {
          attributes = db_get_attributes_force (db_obj);
          while (attributes)
            {
              attributes = db_attribute_next (attributes);
            }
        }
    }
}

/*
 * pt_check_grant_revoke () - do semantic checks on the grant statement
 *   return:  none
 *   parser(in): the parser context used to derive the statement
 *   node(in): a statement
 */

static void
pt_check_grant_revoke (PARSER_CONTEXT * parser, PT_NODE * node)
{
  PT_NODE *user;
  PT_NODE *chk_parent = NULL;
  const char *username;

  /* Replace each Entity Spec with an Equivalent flat list */
  parser_walk_tree (parser, node, pt_flat_spec_pre, &chk_parent, pt_continue_walk, NULL);

  /* make sure the grantees/revokees exist */
  for ((user = (node->node_type == PT_GRANT ?
                node->info.grant.user_list : node->info.revoke.user_list)); user; user = user->next)
    {
      if (user->node_type == PT_NAME && (username = user->info.name.original) && !db_find_user (username))
        {
          PT_ERRORmf (parser, user, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_USER_IS_NOT_IN_DB, username);
        }
    }
}

/*
 * pt_check_single_valued_node () - looks for names outside an aggregate
 *      which are not in group by list. If it finds one, raises an error
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in/out):
 *   continue_walk(in/out):
 */
static PT_NODE *
pt_check_single_valued_node (PARSER_CONTEXT * parser, PT_NODE * node, void *arg, int *continue_walk)
{
  PT_AGG_CHECK_INFO *info = (PT_AGG_CHECK_INFO *) arg;
  PT_NODE *arg2, *group, *expr;
  char *node_str;

  *continue_walk = PT_CONTINUE_WALK;

  if (pt_is_aggregate_function (parser, node))
    {
      *continue_walk = PT_LIST_WALK;
    }
  else if (node->node_type == PT_SELECT)
    {
      /* Can not increment level for list portion of walk.
       * Since those queries are not sub-queries of this query.
       * Consequently, we recurse separately for the list leading
       * from a query.  Can't just call pt_to_uncorr_subquery_list()
       * directly since it needs to do a leaf walk and we want to do a full
       * walk on the next list.
       */
      if (node->next)
        {
          (void) parser_walk_tree (parser, node->next,
                                   pt_check_single_valued_node, info, pt_check_single_valued_node_post, info);
        }

      *continue_walk = PT_LEAF_WALK;

      /* increase query depth as we dive into subqueries */
      info->depth++;
    }
  else
    {
      switch (node->node_type)
        {
        case PT_NAME:
          *continue_walk = PT_LIST_WALK;

          if (pt_find_spec (parser, info->from, node) && pt_find_attribute (parser, node, info->group_by) < 0)
            {
              PT_ERRORmf (parser, node,
                          MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_NOT_SINGLE_VALUED, pt_short_print (parser, node));
            }
          break;

        case PT_DOT_:
          *continue_walk = PT_LIST_WALK;

          if ((arg2 = node->info.dot.arg2)
              && pt_find_spec (parser, info->from, arg2) && (pt_find_attribute (parser, arg2, info->group_by) < 0))
            {
              PT_ERRORmf (parser, node,
                          MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_NOT_SINGLE_VALUED, pt_short_print (parser, node));
            }
          break;

        case PT_VALUE:
          /* watch out for parameters of type object--don't walk their
             data_type list */
          *continue_walk = PT_LIST_WALK;
          break;

        case PT_EXPR:
          if (node->info.expr.op == PT_INST_NUM || node->info.expr.op == PT_ROWNUM)
            {
              if (info->depth == 0)
                {               /* not in subqueries */
                  PT_ERRORmf (parser, node,
                              MSGCAT_SET_PARSER_SEMANTIC,
                              MSGCAT_SEMANTIC_NOT_SINGLE_VALUED, pt_short_print (parser, node));
                }
            }
          else
            {
              unsigned int save_custom;

              save_custom = parser->custom_print;       /* save */

              parser->custom_print |= PT_CONVERT_RANGE;
              node_str = parser_print_tree (parser, node);
              parser->custom_print = save_custom;       /* restore */

              for (group = info->group_by; group; group = group->next)
                {
                  expr = group->info.sort_spec.expr;
                  if (expr->node_type == PT_EXPR)
                    {
                      if (expr->alias_print == NULL)
                        {
                          /* print expr to alias_print */
                          PT_NODE_PRINT_TO_ALIAS (parser, expr, PT_CONVERT_RANGE);
                        }

                      if (pt_str_compare (node_str, expr->alias_print, CASE_INSENSITIVE) == 0)
                        {
                          /* find matched expression */
                          *continue_walk = PT_LIST_WALK;
                          break;
                        }
                    }
                }               /* for (group = info->group_by; ...) */

            }
          break;

        default:
          break;
        }
    }

  return node;
}

/*
 * pt_check_single_valued_node_post () -
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in/out):
 *   continue_walk(in/out):
 */
static PT_NODE *
pt_check_single_valued_node_post (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * node, void *arg, int *continue_walk)
{
  PT_AGG_CHECK_INFO *info = (PT_AGG_CHECK_INFO *) arg;

  *continue_walk = PT_CONTINUE_WALK;

  if (node->node_type == PT_SELECT)
    {
      info->depth--;            /* decrease query depth */
    }

  return node;
}

/*
 * pt_semantic_check_local () - checks semantics on a particular statement
 *   return:
 *   parser(in):
 *   node(in):
 *   arg(in):
 *   continue_walk(in):
 */
static PT_NODE *
pt_semantic_check_local (PARSER_CONTEXT * parser, PT_NODE * node, void *arg, UNUSED_ARG int *continue_walk)
{
  SEMANTIC_CHK_INFO *info = (SEMANTIC_CHK_INFO *) arg;
  PT_NODE *next, *top_node = info->top_node;
  PT_NODE *orig = node;
  PT_NODE *t_node;
  PT_NODE *entity;
  PT_ASSIGNMENTS_HELPER ea;

  assert (parser != NULL);

  if (node == NULL)
    {
      return NULL;
    }

  next = node->next;
  node->next = NULL;

  switch (node->node_type)
    {
      /* Every type of node that can appear at the highest level should be
       * listed here, unless no semantic check is required. */
    case PT_DELETE:
      entity = NULL;

      /* Make sure that none of the classes that are subject for delete is a
       * derived table */
      t_node = node->info.delete_.target_classes;

      if (t_node == NULL)
        {
          /* this is not a multi-table delete; check all specs for derived
             tables */
          entity = node->info.delete_.spec;

          while (entity)
            {
              assert (entity->node_type == PT_SPEC);

              if (entity->info.spec.derived_table != NULL)
                {
                  PT_ERRORm (parser, node, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_DELETE_DERIVED_TABLE);
                  break;
                }
            }
        }
      else
        {
          assert (t_node != NULL);
          assert (t_node->next == NULL);

          /* multi-table delete */
          while (t_node)
            {
              entity = pt_find_spec_in_statement (parser, node, t_node);

              if (entity == NULL)
                {
                  PT_ERRORmf (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
                              MSGCAT_SEMANTIC_RESOLUTION_FAILED,
                              t_node->node_type == PT_NAME ? t_node->info.name.original : "unknown");
                  break;
                }

              if (entity->info.spec.derived_table != NULL)
                {
                  PT_ERRORm (parser, node, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_DELETE_DERIVED_TABLE);
                  break;
                }

              t_node = t_node->next;
            }
        }

      node = pt_semantic_type (parser, node, info);
      break;

    case PT_INSERT:
      if (node->info.insert.odku_assignments != NULL)
        {
          node = pt_check_odku_assignments (parser, node);
          if (node == NULL || pt_has_error (parser))
            {
              break;
            }
        }
      /* semantic check value clause for SELECT and INSERT subclauses */
      if (node)
        {
          node = pt_semantic_type (parser, node, info);
          if (node == NULL || pt_has_error (parser))
            {
              break;
            }
        }

      /* try to coerce insert_values into types indicated
       * by insert_attributes */
      if (node)
        {
          pt_coerce_insert_values (parser, node);
        }

      if (node == NULL || pt_has_error (parser))
        {
          break;
        }

      if (node->info.insert.value_clauses->info.node_list.list_type != PT_IS_SUBQUERY)
        {
          /* Search and check sub-inserts in value list */
          (void) parser_walk_tree (parser, node->info.insert.value_clauses, pt_check_sub_insert, NULL, NULL, NULL);
        }
      if (pt_has_error (parser))
        {
          break;
        }

      if (top_node && top_node->node_type != PT_INSERT)
        {
          PT_ERRORm (parser, node, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_INS_EXPR_IN_INSERT);
        }
      break;

    case PT_FUNCTION:
      assert (node->info.function.function_type != PT_GENERIC);

      if (node->info.function.function_type == PT_GROUP_CONCAT)
        {
          if (pt_check_group_concat_order_by (parser, node) != NO_ERROR)
            {
              break;
            }
        }
      break;

    case PT_UNION:
    case PT_INTERSECTION:
    case PT_DIFFERENCE:
      /* semantic check {union|intersection|difference} operands */
      if (pt_has_error (parser))
        {
          break;
        }

      /* check the orderby clause if present (all 3 nodes have SAME
       * structure) */
      if (pt_check_order_by (parser, node) != NO_ERROR)
        {
          break;                /* error */
        }

      node = pt_semantic_type (parser, node, info);
      break;

    case PT_SELECT:
      if (node->info.query.single_tuple == 1)
        {
          if (pt_length_of_select_list (node->info.query.q.select.list, EXCLUDE_HIDDEN_COLUMNS) != 1)
            {
              /* illegal multi-column subquery */
              PT_ERRORm (parser, node, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_NOT_SINGLE_COL);
            }
        }

      if (pt_has_aggregate (parser, node))
        {
          PT_AGG_CHECK_INFO info;
          PT_NODE *r;
          QFILE_TUPLE_VALUE_POSITION pos;
          PT_NODE *referred_node;
          int max_position;

          /* STEP 1: init agg info */
          info.from = node->info.query.q.select.from;
          info.depth = 0;
          info.group_by = node->info.query.q.select.group_by;

          max_position = pt_length_of_select_list (node->info.query.q.select.list, EXCLUDE_HIDDEN_COLUMNS);

          for (t_node = info.group_by; t_node; t_node = t_node->next)
            {
              r = t_node->info.sort_spec.expr;
              if (r == NULL)
                {
                  continue;
                }
              /*
               * If a position is specified on group by clause,
               * we should check its range.
               */
              if (r->node_type == PT_VALUE && r->alias_print == NULL)
                {
                  if (r->type_enum != PT_TYPE_INTEGER)
                    {
                      PT_ERRORm (parser, r, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_SORT_SPEC_WANT_NUM);
                      continue;
                    }
                  else if (r->info.value.data_value.i == 0 || r->info.value.data_value.i > max_position)
                    {
                      PT_ERRORmf (parser, r,
                                  MSGCAT_SET_PARSER_SEMANTIC,
                                  MSGCAT_SEMANTIC_SORT_SPEC_RANGE_ERR, r->info.value.data_value.i);
                      continue;
                    }
                }
              else if (r->node_type == PT_HOST_VAR)
                {
                  PT_ERRORmf (parser, r, MSGCAT_SET_PARSER_SEMANTIC,
                              MSGCAT_SEMANTIC_NO_GROUPBY_ALLOWED, pt_short_print (parser, r));
                  continue;
                }

              /* check for after group by position */
              pt_to_pos_descr (parser, &pos, r, node, &referred_node);
              if (pos.pos_no > 0)
                {
                  /* set after group by position num, domain info */
                  t_node->info.sort_spec.pos_descr = pos;
                }
              /*
               * If there is a node referred by the position,
               * we should rewrite the position to real name or expression
               * regardless of pos.pos_no.
               */
              if (referred_node != NULL)
                {
                  t_node->info.sort_spec.expr = parser_copy_tree (parser, referred_node);
                  parser_free_node (parser, r);
                }
            }

          /* STEP 2: check that grouped things are single valued */
          (void) parser_walk_tree (parser,
                                   node->info.query.q.select.list,
                                   pt_check_single_valued_node, &info, pt_check_single_valued_node_post, &info);
          (void) parser_walk_tree (parser,
                                   node->info.query.q.select.having,
                                   pt_check_single_valued_node, &info, pt_check_single_valued_node_post, &info);
        }

      /* check the order by */
      if (pt_check_order_by (parser, node) != NO_ERROR)
        {
          break;                /* error */
        }

      if (node->info.query.q.select.group_by != NULL && node->info.query.q.select.group_by->with_rollup)
        {
          bool has_gbynum = false;

          /* we do not allow GROUP BY ... WITH ROLLUP and GROUPBY_NUM () */
          (void) parser_walk_tree (parser, node->info.query.q.select.having,
                                   pt_check_groupbynum_pre, NULL, pt_check_groupbynum_post, (void *) &has_gbynum);

          if (has_gbynum)
            {
              PT_ERRORm (parser, node, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_CANNOT_USE_GROUPBYNUM_WITH_ROLLUP);
            }
        }

      node = pt_semantic_type (parser, node, info);
      break;

    case PT_UPDATE:
      if (pt_has_aggregate (parser, node))
        {
          PT_ERRORm (parser, node, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_UPDATE_NO_AGGREGATE);
        }

      pt_check_assignments (parser, node);
      pt_no_double_updates (parser, node);

      /* cannot update derived tables */
      pt_init_assignments_helper (parser, &ea, node->info.update.assignment);
      while ((t_node = pt_get_next_assignment (&ea)) != NULL)
        {
          entity = pt_find_spec_in_statement (parser, node, t_node);

          if (entity == NULL)
            {
              PT_ERRORmf (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
                          MSGCAT_SEMANTIC_RESOLUTION_FAILED,
                          t_node->node_type == PT_NAME ? t_node->info.name.original : "unknown");
              break;
            }

          if (entity->info.spec.derived_table != NULL)
            {
              PT_ERRORm (parser, node, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_UPDATE_DERIVED_TABLE);
              break;
            }
        }

      node = pt_semantic_type (parser, node, info);

      if (node != NULL && node->info.update.order_by != NULL)
        {
          PT_NODE *order;
          for (order = node->info.update.order_by; order != NULL; order = order->next)
            {
              PT_NODE *r = order->info.sort_spec.expr;
              if (r != NULL && r->node_type == PT_VALUE)
                {
                  PT_ERRORmf (parser, r, MSGCAT_SET_PARSER_SEMANTIC,
                              MSGCAT_SEMANTIC_NO_ORDERBY_ALLOWED, pt_short_print (parser, r));
                  break;
                }
            }
        }
      break;

    case PT_EXPR:
      if (node->info.expr.op == PT_CAST)
        {
          (void) pt_check_cast_op (parser, node);
        }

      /* check instnum compatibility */
      if (pt_is_instnum (node) && PT_EXPR_INFO_IS_FLAGED (node, PT_EXPR_INFO_INSTNUM_NC))
        {
          PT_ERRORmf2 (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
                       MSGCAT_SEMANTIC_INSTNUM_COMPATIBILITY_ERR, "INST_NUM() or ROWNUM", "INST_NUM() or ROWNUM");
        }

      /* check default function */
      if (node->info.expr.op == PT_DEFAULTF)
        {
          pt_check_defaultf (parser, node);
        }

      break;

    case PT_SPEC:
      {
        PT_NODE *derived_table, *a, *b, *select_list;
        int attr_cnt, col_cnt, i, j;

        /* check ambiguity in as_attr_list of derived-query */
        if (node->info.spec.derived_table_type == PT_IS_SUBQUERY && (derived_table = node->info.spec.derived_table))
          {
            a = node->info.spec.as_attr_list;
            for (; a && !pt_has_error (parser); a = a->next)
              {
                for (b = a->next; b && !pt_has_error (parser); b = b->next)
                  {
                    if (a->node_type == PT_NAME
                        && b->node_type == PT_NAME
                        && !pt_str_compare (a->info.name.original, b->info.name.original, CASE_INSENSITIVE))
                      {
                        PT_ERRORmf (parser, b, MSGCAT_SET_PARSER_SEMANTIC,
                                    MSGCAT_SEMANTIC_AMBIGUOUS_REF_TO, b->info.name.original);
                      }
                  }
              }

            /* check hidden column of subquery-derived table */
            if (!pt_has_error (parser)
                && derived_table->node_type == PT_SELECT
                && derived_table->info.query.order_by && (select_list = pt_get_select_list (parser, derived_table)))
              {
                attr_cnt = pt_length_of_list (node->info.spec.as_attr_list);
                col_cnt = pt_length_of_select_list (select_list, INCLUDE_HIDDEN_COLUMNS);
                if (col_cnt - attr_cnt > 0)
                  {
                    /* make hidden column attrs */
                    for (i = attr_cnt, j = attr_cnt; i < col_cnt; i++)
                      {
                        t_node = pt_name (parser, mq_generate_name (parser, "ha", &j));
                        node->info.spec.as_attr_list = parser_append_node (t_node, node->info.spec.as_attr_list);
                      }
                  }
              }
          }
      }
      break;

    default:                   /* other node types */
      break;
    }

  /* Select Aliasing
   * semantic checking of select aliasing, check if it is zero-length string,
   * i.e. ""
   * Only appropriate PT_NODE to be check will have 'alias' field as not NULL
   * pointer because the initialized value of 'alias' is NULL pointer.
   * So it is safe to do semantic checking of aliasing out of the scope of
   * above 'switch' statement and without considering type of the PT_NODE.
   */
  if (node && node->alias_print && *(node->alias_print) == '\0')
    {
      PT_ERRORmf (parser, node, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_IS_NOT_DEFINED, "\"\"");
    }

  /* restore list link, if any */
  if (node)
    {
      node->next = next;
    }

  if (pt_has_error (parser))
    {
      if (node)
        {
          pt_register_orphan (parser, node);
        }
      else
        {
          pt_register_orphan (parser, orig);
        }
      return NULL;
    }
  else
    {
      return node;
    }
}


/*
 * pt_check_with_info () -  do name resolution & semantic checks on this tree
 *   return:  statement if no errors, NULL otherwise
 *   parser(in): the parser context
 *   node(in): a parsed sql statement that needs to be checked.
 *   info(in): NULL or info->attrdefs is a vclass' attribute defs list
 */

static PT_NODE *
pt_check_with_info (PARSER_CONTEXT * parser, PT_NODE * node, SEMANTIC_CHK_INFO * info)
{
  PT_NODE *next;
  SEMANTIC_CHK_INFO sc_info = { NULL, false };
  SEMANTIC_CHK_INFO *sc_info_ptr = info;

  assert (parser != NULL);

  if (!node)
    {
      return NULL;
    }

  /* If it is an internally created statement, set its host variable
   * info again to search host variables at parent parser */
  SET_HOST_VARIABLES_IF_INTERNAL_STATEMENT (parser);

  if (sc_info_ptr == NULL)
    {
      sc_info_ptr = &sc_info;
    }

  sc_info_ptr->top_node = node;
  next = node->next;
  node->next = NULL;

  switch (node->node_type)
    {
    case PT_UPDATE:
    case PT_HOST_VAR:
    case PT_EXPR:
    case PT_NAME:
    case PT_VALUE:
    case PT_FUNCTION:

    case PT_DELETE:
    case PT_INSERT:
    case PT_UNION:
    case PT_INTERSECTION:
    case PT_DIFFERENCE:
    case PT_SELECT:
      sc_info_ptr->system_class = false;
      node = pt_resolve_names (parser, node, sc_info_ptr);

      if (!pt_has_error (parser))
        {
          if (sc_info_ptr->system_class && PT_IS_QUERY (node))
            {
              /* do not cache the result if a system class is involved
                 in the query */
              node->info.query.reexecute = 1;
              node->info.query.do_cache = 0;
              node->info.query.do_not_cache = 1;
            }

          /* remove unnecessary variable */
          node = parser_walk_tree (parser, node, NULL, NULL, pt_semantic_check_local, sc_info_ptr);
        }

      break;

    case PT_CREATE_INDEX:
    case PT_ALTER_INDEX:
    case PT_DROP_INDEX:
      if (parser->host_var_count)
        {
          PT_ERRORm (parser, node, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_HOSTVAR_IN_DDL);
        }
      else
        {
          sc_info_ptr->system_class = false;
          node = pt_resolve_names (parser, node, sc_info_ptr);
          if (!pt_has_error (parser) && node->node_type == PT_CREATE_INDEX)
            {
              pt_check_create_index (parser, node);
            }

          if (!pt_has_error (parser) && node->node_type == PT_ALTER_INDEX)
            {
              DB_OBJECT *db_obj = NULL;
              PT_NODE *name = NULL;

              if (node->info.index.indexed_class)
                {
                  name = node->info.index.indexed_class->info.spec.entity_name;
                  db_obj = sm_find_class (name->info.name.original);

                  if (db_obj == NULL)
                    {
                      PT_ERRORmf (parser, name,
                                  MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_IS_NOT_A_CLASS, name->info.name.original);
                    }
                }
            }

          if (!pt_has_error (parser))
            {
              node = pt_semantic_type (parser, node, info);
            }

          if (node && !pt_has_error (parser))
            {
              node = parser_walk_tree (parser, node, NULL, NULL, pt_semantic_check_local, sc_info_ptr);
            }
        }
      break;

    case PT_SAVEPOINT:
      break;
    case PT_ROLLBACK_WORK:
      break;
    case PT_AUTH_CMD:
      break;                    /* see GRANT/REVOKE  */

    case PT_DROP:
      pt_check_drop (parser, node);
      break;

    case PT_GRANT:
    case PT_REVOKE:
      pt_check_grant_revoke (parser, node);
      break;

    case PT_ALTER:
    case PT_ALTER_USER:
    case PT_CREATE_ENTITY:
    case PT_CREATE_USER:
    case PT_DROP_USER:
    case PT_RENAME:
    case PT_UPDATE_STATS:
      switch (node->node_type)
        {
        case PT_ALTER:
          pt_check_alter (parser, node);

          if (node->info.alter.code == PT_ADD_ATTR_MTHD)
            {
              if (parser->host_var_count)
                {
                  PT_ERRORm (parser, node, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_HOSTVAR_IN_DDL);
                }
            }
          break;

        case PT_CREATE_ENTITY:
          pt_check_create_entity (parser, node);
          break;

        case PT_CREATE_USER:
          pt_check_create_user (parser, node);
          break;

        default:
          break;
        }
      break;

    default:
      break;
    }

  /* restore list link, if any */
  if (node)
    {
      node->next = next;
    }

  RESET_HOST_VARIABLES_IF_INTERNAL_STATEMENT (parser);
  if (pt_has_error (parser))
    {
      pt_register_orphan (parser, node);
      return NULL;
    }
  else
    {
      return node;
    }
}

/*
 * pt_semantic_check () -
 *   return: PT_NODE *(modified) if no errors, else NULL if errors
 *   parser(in):
 *   node(in): statement a parsed sql statement that needs to be checked
 */

PT_NODE *
pt_semantic_check (PARSER_CONTEXT * parser, PT_NODE * node)
{
  return pt_check_with_info (parser, node, NULL);
}

/*
 * pt_find_class () -
 *   return: DB_OBJECT * for the class whose name is in p,
 *           NULL if not a class name
 *   parser(in):
 *   p(in): a PT_NAME node
 *
 * Note :
 * Finds CLASS VCLASS VIEW only
 */
static DB_OBJECT *
pt_find_class (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * p)
{
  if (!p)
    return 0;

  if (p->node_type != PT_NAME)
    return 0;

  return sm_find_class (p->info.name.original);
}


/*
 * pt_check_unique_attr () - check that there are no duplicate attr
 *                           in given list
 *   return: none
 *   parser(in): the parser context
 *   entity_name(in): class name or index name
 *   att(in): an attribute definition list
 *   att_type(in): an attribute definition type list
 */
static void
pt_check_unique_attr (PARSER_CONTEXT * parser, const char *entity_name, PT_NODE * att, PT_NODE_TYPE att_type)
{
  PT_NODE *p, *q, *p_nam, *q_nam;

  assert (parser != NULL);
  if (!att)
    {
      return;
    }

  for (p = att; p; p = p->next)
    {
      if (p->node_type != att_type)
        {
          continue;             /* give up */
        }

      p_nam = NULL;             /* init */
      if (att_type == PT_ATTR_DEF)
        {
          p_nam = p->info.attr_def.attr_name;
        }
      else if (att_type == PT_SORT_SPEC)
        {
          p_nam = p->info.sort_spec.expr;
        }

      if (p_nam == NULL || p_nam->node_type != PT_NAME)
        {
          continue;             /* give up */
        }

      for (q = p->next; q; q = q->next)
        {
          if (q->node_type != att_type)
            {
              continue;         /* give up */
            }

          q_nam = NULL;         /* init */
          if (att_type == PT_ATTR_DEF)
            {
              q_nam = q->info.attr_def.attr_name;
            }
          else if (att_type == PT_SORT_SPEC)
            {
              q_nam = q->info.sort_spec.expr;
            }

          if (q_nam == NULL || q_nam->node_type != PT_NAME)
            {
              continue;         /* give up */
            }

          if (att_type == PT_ATTR_DEF)
            {
              if (p->info.attr_def.attr_type != q->info.attr_def.attr_type)
                {
                  continue;     /* OK */
                }
            }

          if (!pt_str_compare (p_nam->info.name.original, q_nam->info.name.original, CASE_INSENSITIVE))
            {
              if (att_type == PT_ATTR_DEF)      /* is class entity */
                {
                  PT_ERRORmf2 (parser, q_nam,
                               MSGCAT_SET_PARSER_SEMANTIC,
                               MSGCAT_SEMANTIC_CLASS_ATTR_DUPLICATED, q_nam->info.name.original, entity_name);
                }
              else              /* is index entity */
                {
                  PT_ERRORmf (parser, q_nam,
                              MSGCAT_SET_PARSER_SEMANTIC,
                              MSGCAT_SEMANTIC_INDEX_ATTR_DUPLICATED, q_nam->info.name.original);
                }
            }
        }
    }
}

/*
 * pt_assignment_class_compatible () - Make sure that the rhs is a valid
 *                                     candidate for assignment into the lhs
 *   return: error_code
 *   parser(in): handle to context used to parse the insert statement
 *   lhs(in): the AST form of an attribute from the namelist part of an insert
 *   rhs(in): the AST form of an expression from the values part of an insert
 */
static int
pt_assignment_class_compatible (PARSER_CONTEXT * parser, PT_NODE * lhs, PT_NODE * rhs)
{
  assert (parser != NULL);
  assert (lhs != NULL);
  assert (lhs->node_type == PT_NAME);
  assert (lhs->type_enum != PT_TYPE_NONE);
  assert (rhs != NULL);

  if (lhs->node_type == PT_NAME)
    {
      if (lhs->type_enum == PT_TYPE_OBJECT)
        {
#if 1                           /* TODO - */
          assert (false);       /* not permit */
#endif

          /* incompatible object domains */
          PT_ERRORmf (parser, rhs,
                      MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_INCOMP_TYPE_ON_ATTR, lhs->info.name.original);
          return ER_FAILED;
        }

      if (PT_NAME_INFO_IS_FLAGED (lhs, PT_NAME_FOR_SHARD_KEY))
        {
          PT_ERRORmf (parser, rhs,
                      MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_CANT_UPDATE_SHARDKEY, lhs->info.name.original);
          return ER_FAILED;

        }
    }

  return NO_ERROR;
}

/*
 * pt_check_assignments () - assert that the lhs of the set clause are
 *      all pt_name nodes.
 *      This will guarantee that there are no complex path expressions.
 *      Also asserts that the right hand side is assignment compatible.
 *   return:  none
 *   parser(in): the parser context
 *   stmt(in): an insert/update statement
 */

static void
pt_check_assignments (PARSER_CONTEXT * parser, PT_NODE * stmt)
{
  PT_NODE *a, *next, *lhs, *rhs, *list, *rhs_list;
  PT_NODE *assignment_list;

  assert (parser != NULL);

  assignment_list = pt_get_assignments (stmt);
  if (assignment_list == NULL)
    {
      return;
    }

  for (a = assignment_list; a; a = next)
    {
      next = a->next;           /* save next link */
      if (a->node_type == PT_EXPR
          && a->info.expr.op == PT_ASSIGN && (lhs = a->info.expr.arg1) != NULL && (rhs = a->info.expr.arg2) != NULL)
        {
          if (lhs->node_type == PT_NAME)
            {
              /* multi-column update with subquery
               * CASE1: always-false subquery is already converted NULL.
               *        so, change NULL into NULL paren-expr
               *        (a)    = NULL    ->    a = NULL
               *        (a, b) = NULL    ->    a = NULL, b = NULL
               * CASE2: (a, b) = subquery
               */

              if (pt_is_query (rhs))
                {
                  /* check select list length */
                  list = lhs;
                  rhs_list = pt_get_select_list (parser, rhs);
                  assert (rhs_list != NULL);
                  if (pt_length_of_list (list) != pt_length_of_select_list (rhs_list, EXCLUDE_HIDDEN_COLUMNS))
                    {
                      /* e.g., a = (select 1, 2 from ...) */
                      PT_ERRORm (parser, lhs, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_ILLEGAL_RHS);
                    }
                  else
                    {
                      for (; list && rhs_list; list = list->next, rhs_list = rhs_list->next)
                        {
                          if (rhs_list->is_hidden_column)
                            {
                              /* skip hidden column */
                              continue;
                            }

                          (void) pt_assignment_class_compatible (parser, list, rhs_list);
                        }
                      assert (list == NULL);
                      assert (rhs_list == NULL);
                    }
                }
              else
                {
                  /* Not a query, just check if assignment is possible. */
                  (void) pt_assignment_class_compatible (parser, lhs, rhs);
                }
            }
          else
            {
              PT_ERRORm (parser, lhs, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_ILLEGAL_LHS);
            }
        }
      else
        {
          /* malformed assignment list */
          PT_INTERNAL_ERROR (parser, "semantic");
        }
    }
}

/*
 * pt_node_double_insert_assignments () - Check if an attribute is assigned
 *					  more than once.
 *
 * return      : Void.
 * parser (in) : Parser context.
 * stmt (in)   : Insert statement.
 */
void
pt_no_double_insert_assignments (PARSER_CONTEXT * parser, PT_NODE * stmt)
{
  PT_NODE *attr = NULL, *spec = NULL;
  PT_NODE *entity_name;

  if (stmt == NULL || stmt->node_type != PT_INSERT)
    {
      return;
    }

  spec = stmt->info.insert.spec;
  entity_name = spec->info.spec.entity_name;
  if (entity_name == NULL)
    {
      assert (false);
      PT_ERROR (parser, stmt,
                "The parse tree of the insert statement is incorrect." " entity_name of spec must be set.");
      return;
    }

  if (entity_name->info.name.original == NULL)
    {
      assert (false);
      PT_ERROR (parser, entity_name, er_msg ());
      return;
    }

  /* check for duplicate assignments */
  for (attr = pt_attrs_part (stmt); attr != NULL; attr = attr->next)
    {
      PT_NODE *attr2;
      for (attr2 = attr->next; attr2 != NULL; attr2 = attr2->next)
        {
          if (pt_name_equal (parser, attr, attr2))
            {
              PT_ERRORmf2 (parser, attr2, MSGCAT_SET_PARSER_SEMANTIC,
                           MSGCAT_SEMANTIC_GT_1_ASSIGNMENT_TO,
                           entity_name->info.name.original, attr2->info.name.original);
              return;
            }
        }
    }
}

/*
 * pt_no_double_updates () - assert that there are no multiple assignments to
 *      the same attribute in the given update statement
 *   return:  none
 *   parser(in): the parser context
 *   stmt(in): an update statement
 */

void
pt_no_double_updates (PARSER_CONTEXT * parser, PT_NODE * stmt)
{
  PT_NODE *a, *b, *att_a, *att_b;
  PT_NODE *assignment_list;

  assert (parser != NULL);

  assignment_list = pt_get_assignments (stmt);
  if (assignment_list == NULL)
    {
      return;
    }

  for (a = assignment_list; a; a = a->next)
    {
      if (!(a->node_type == PT_EXPR && a->info.expr.op == PT_ASSIGN && (att_a = a->info.expr.arg1)))
        {
          goto exit_on_error;
        }

      for (; att_a; att_a = att_a->next)
        {
          if (att_a->node_type != PT_NAME || att_a->info.name.original == NULL)
            {
              goto exit_on_error;
            }

          /* first, check current node */
          for (att_b = att_a->next; att_b; att_b = att_b->next)
            {
              if (att_b->node_type != PT_NAME || att_b->info.name.original == NULL)
                {
                  goto exit_on_error;
                }
              /* for multi-table we must check name and spec id */
              if (!pt_str_compare (att_a->info.name.original,
                                   att_b->info.name.original,
                                   CASE_INSENSITIVE) && att_a->info.name.spec_id == att_b->info.name.spec_id)
                {
                  PT_ERRORmf2 (parser, att_a,
                               MSGCAT_SET_PARSER_SEMANTIC,
                               MSGCAT_SEMANTIC_GT_1_ASSIGNMENT_TO,
                               att_a->info.name.resolved, att_a->info.name.original);
                  return;
                }
            }

          /* then, check the following node */
          for (b = a->next; b; b = b->next)
            {
              if (!(b->node_type == PT_EXPR && b->info.expr.op == PT_ASSIGN && (att_b = b->info.expr.arg1)))
                {
                  goto exit_on_error;
                }

              for (; att_b; att_b = att_b->next)
                {
                  if (att_b->node_type != PT_NAME || att_b->info.name.original == NULL)
                    {
                      goto exit_on_error;
                    }
                  /* for multi-table we must check name and spec id */
                  if (!pt_str_compare (att_a->info.name.original,
                                       att_b->info.name.original,
                                       CASE_INSENSITIVE) && att_a->info.name.spec_id == att_b->info.name.spec_id)
                    {
                      PT_ERRORmf2 (parser, att_a,
                                   MSGCAT_SET_PARSER_SEMANTIC,
                                   MSGCAT_SEMANTIC_GT_1_ASSIGNMENT_TO,
                                   att_a->info.name.resolved, att_a->info.name.original);
                      return;
                    }
                }
            }
        }
    }

  return;

exit_on_error:
  /* malformed assignment list */
  PT_INTERNAL_ERROR (parser, "semantic");
  return;
}

/*
 * pt_invert () -
 *   return:
 *   parser(in):
 *   name_expr(in): an expression from a select list
 *   result(out): written in terms of the same single variable or path-expr
 *
 * Note :
 * Given an expression p that involves only:
 *   + - / * ( ) constants and a single variable (which occurs only once).
 *
 * Find the functional inverse of the expression.
 * [ f and g are functional inverses if f(g(x)) == x ]
 *
 *       function       inverse
 *       --------       --------
 *          -x              -x
 *          4*x            x/4
 *          4*x+10         (x-10)/4
 *          6+x            x-6
 *
 * Can't invert:  x+y;  x+x;  x*x; constants ; count(*);  f(x) ;
 */
PT_NODE *
pt_invert (PARSER_CONTEXT * parser, PT_NODE * name_expr, PT_NODE * result)
{
  int result_isnull = 0;
  PT_NODE *tmp;
  PT_NODE *msgs;
  SEMANTIC_CHK_INFO sc_info = { NULL, false };

  assert (parser != NULL);
  msgs = parser->error_msgs;

  /* find the variable and return if none */
  if (pt_find_var (name_expr, &tmp) != 1 || tmp == NULL)
    {
      return NULL;
    }

  /* walk through the expression, inverting as you go */
  while (name_expr)
    {
      /* Got a path expression, you're done. ( result = path expr ) */
      if (name_expr->node_type == PT_NAME)
        {
          break;
        }

      /* not an expression? then can't do it */
      if (name_expr->node_type != PT_EXPR)
        {
          result = 0;
          break;
        }

      /* the inverse of any expression involving NULL is NULL */
      result_isnull = PT_IS_NULL_NODE (result);
      switch (name_expr->info.expr.op)
        {
        case PT_UNARY_MINUS:
          /* ( result =  -expr ) <=>  ( -result = expr ) */
          name_expr = name_expr->info.expr.arg1;
          if (!result_isnull)
            {
              tmp = parser_new_node (parser, PT_EXPR);
              if (tmp == NULL)
                {
                  PT_INTERNAL_ERROR (parser, "allocate new node");
                  return NULL;
                }

              tmp->info.expr.op = PT_UNARY_MINUS;
              tmp->info.expr.arg1 = result;
              if (tmp->info.expr.arg1->node_type == PT_EXPR)
                {
                  tmp->info.expr.arg1->info.expr.paren_type = 1;
                }
              result = tmp;
            }
          break;

        case PT_PLUS:
          /* ( result = A + B ) <=>  ( result - A = B ) */
          if (pt_find_var (name_expr->info.expr.arg1, 0))
            {
              if (result_isnull)
                {
                  /* no need to invert result because
                   * result already has a null */
                  name_expr = name_expr->info.expr.arg1;
                }
              else
                {
                  tmp = parser_new_node (parser, PT_EXPR);
                  if (tmp == NULL)
                    {
                      PT_INTERNAL_ERROR (parser, "allocate new node");
                      return NULL;
                    }

                  tmp->info.expr.op = PT_MINUS;
                  tmp->info.expr.arg1 = result;
                  tmp->info.expr.arg2 = parser_copy_tree (parser, name_expr->info.expr.arg2);

                  if (tmp->info.expr.arg2 == NULL)
                    {
                      PT_INTERNAL_ERROR (parser, "parser_copy_tree");
                      return NULL;
                    }

                  if (tmp->info.expr.arg1->node_type == PT_EXPR)
                    {
                      tmp->info.expr.arg1->info.expr.paren_type = 1;
                    }
                  if (tmp->info.expr.arg2->node_type == PT_EXPR)
                    {
                      tmp->info.expr.arg2->info.expr.paren_type = 1;
                    }
                  name_expr = name_expr->info.expr.arg1;
                  result = tmp;
                }
              break;
            }

          if (pt_find_var (name_expr->info.expr.arg2, 0))
            {
              if (result_isnull)
                {
                  /* no need to invert result because
                   * result already has a null */
                  name_expr = name_expr->info.expr.arg2;
                }
              else
                {
                  tmp = parser_new_node (parser, PT_EXPR);
                  if (tmp == NULL)
                    {
                      PT_INTERNAL_ERROR (parser, "allocate new node");
                      return NULL;
                    }

                  tmp->info.expr.op = PT_MINUS;
                  tmp->info.expr.arg1 = result;
                  tmp->info.expr.arg2 = parser_copy_tree (parser, name_expr->info.expr.arg1);
                  if (tmp->info.expr.arg2 == NULL)
                    {
                      PT_INTERNAL_ERROR (parser, "parser_copy_tree");
                      return NULL;
                    }

                  if (tmp->info.expr.arg1->node_type == PT_EXPR)
                    {
                      tmp->info.expr.arg1->info.expr.paren_type = 1;
                    }
                  if (tmp->info.expr.arg2->node_type == PT_EXPR)
                    {
                      tmp->info.expr.arg2->info.expr.paren_type = 1;
                    }
                  name_expr = name_expr->info.expr.arg2;
                  result = tmp;
                }
              break;
            }

          return NULL;

        case PT_MINUS:
          /* ( result = A-B ) <=>  ( result+B = A )
             ( result = A-B ) <=>  ( A-result = B ) */
          if (pt_find_var (name_expr->info.expr.arg1, 0))
            {
              if (result_isnull)
                {
                  /* no need to invert result because
                   * result already has a null */
                  name_expr = name_expr->info.expr.arg1;
                }
              else
                {
                  tmp = parser_new_node (parser, PT_EXPR);
                  if (tmp == NULL)
                    {
                      PT_INTERNAL_ERROR (parser, "allocate new node");
                      return NULL;
                    }

                  tmp->info.expr.op = PT_PLUS;
                  tmp->info.expr.arg1 = result;
                  tmp->info.expr.arg2 = parser_copy_tree (parser, name_expr->info.expr.arg2);
                  if (tmp->info.expr.arg2 == NULL)
                    {
                      PT_INTERNAL_ERROR (parser, "parser_copy_tree");
                      return NULL;
                    }

                  if (tmp->info.expr.arg1->node_type == PT_EXPR)
                    {
                      tmp->info.expr.arg1->info.expr.paren_type = 1;
                    }
                  if (tmp->info.expr.arg2->node_type == PT_EXPR)
                    {
                      tmp->info.expr.arg2->info.expr.paren_type = 1;
                    }
                  name_expr = name_expr->info.expr.arg1;
                  result = tmp;
                }
              break;
            }

          if (pt_find_var (name_expr->info.expr.arg2, 0))
            {
              if (result_isnull)
                {
                  /* no need to invert result because
                   * result already has a null */
                  name_expr = name_expr->info.expr.arg2;
                }
              else
                {
                  tmp = parser_new_node (parser, PT_EXPR);
                  if (tmp == NULL)
                    {
                      PT_INTERNAL_ERROR (parser, "allocate new node");
                      return NULL;
                    }

                  tmp->info.expr.op = PT_MINUS;
                  tmp->info.expr.arg2 = result;
                  tmp->info.expr.arg1 = parser_copy_tree (parser, name_expr->info.expr.arg1);
                  if (tmp->info.expr.arg1 == NULL)
                    {
                      PT_INTERNAL_ERROR (parser, "parser_copy_tree");
                      return NULL;
                    }

                  if (tmp->info.expr.arg1->node_type == PT_EXPR)
                    {
                      tmp->info.expr.arg1->info.expr.paren_type = 1;
                    }
                  if (tmp->info.expr.arg2->node_type == PT_EXPR)
                    {
                      tmp->info.expr.arg2->info.expr.paren_type = 1;
                    }
                  name_expr = name_expr->info.expr.arg2;
                  result = tmp;
                }
              break;
            }

          return NULL;

        case PT_DIVIDE:
          /* ( result = A/B ) <=>  ( result*B = A )
             ( result = A/B ) <=>  ( A/result = B ) */
          if (pt_find_var (name_expr->info.expr.arg1, 0))
            {
              if (result_isnull)
                {
                  /* no need to invert result because
                   * result already has a null */
                  name_expr = name_expr->info.expr.arg1;
                }
              else
                {
                  tmp = parser_new_node (parser, PT_EXPR);
                  if (tmp == NULL)
                    {
                      PT_INTERNAL_ERROR (parser, "allocate new node");
                      return NULL;
                    }

                  tmp->info.expr.op = PT_TIMES;
                  tmp->info.expr.arg1 = result;
                  tmp->info.expr.arg2 = parser_copy_tree (parser, name_expr->info.expr.arg2);
                  if (tmp->info.expr.arg2 == NULL)
                    {
                      PT_INTERNAL_ERROR (parser, "parser_copy_tree");
                      return NULL;
                    }

                  if (tmp->info.expr.arg1->node_type == PT_EXPR)
                    {
                      tmp->info.expr.arg1->info.expr.paren_type = 1;
                    }
                  if (tmp->info.expr.arg2->node_type == PT_EXPR)
                    {
                      tmp->info.expr.arg2->info.expr.paren_type = 1;
                    }
                  name_expr = name_expr->info.expr.arg1;
                  result = tmp;
                }
              break;
            }

          if (pt_find_var (name_expr->info.expr.arg2, 0))
            {
              if (result_isnull)
                {
                  /* no need to invert result because
                   * result already has a null */
                  name_expr = name_expr->info.expr.arg2;
                }
              else
                {
                  tmp = parser_new_node (parser, PT_EXPR);
                  if (tmp == NULL)
                    {
                      PT_INTERNAL_ERROR (parser, "allocate new node");
                      return NULL;
                    }

                  tmp->info.expr.op = PT_DIVIDE;
                  tmp->info.expr.arg2 = result;
                  tmp->info.expr.arg1 = parser_copy_tree (parser, name_expr->info.expr.arg1);
                  if (tmp->info.expr.arg1 == NULL)
                    {
                      PT_INTERNAL_ERROR (parser, "parser_copy_tree");
                      return NULL;
                    }

                  if (tmp->info.expr.arg1->node_type == PT_EXPR)
                    {
                      tmp->info.expr.arg1->info.expr.paren_type = 1;
                    }
                  if (tmp->info.expr.arg2->node_type == PT_EXPR)
                    {
                      tmp->info.expr.arg2->info.expr.paren_type = 1;
                    }
                  name_expr = name_expr->info.expr.arg2;
                  result = tmp;
                }
              break;
            }

          return NULL;

        case PT_TIMES:
          /* ( result = A*B ) <=>  ( result/A = B ) */
          if (pt_find_var (name_expr->info.expr.arg1, 0))
            {
              if (result_isnull)
                {
                  /* no need to invert result because
                   * result already has a null */
                  name_expr = name_expr->info.expr.arg1;
                }
              else
                {
                  tmp = parser_new_node (parser, PT_EXPR);
                  if (tmp == NULL)
                    {
                      PT_INTERNAL_ERROR (parser, "allocate new node");
                      return NULL;
                    }

                  tmp->info.expr.op = PT_DIVIDE;
                  tmp->info.expr.arg1 = result;
                  tmp->info.expr.arg2 = parser_copy_tree (parser, name_expr->info.expr.arg2);
                  if (tmp->info.expr.arg2 == NULL)
                    {
                      PT_INTERNAL_ERROR (parser, "parser_copy_tree");
                      return NULL;
                    }

                  if (tmp->info.expr.arg1->node_type == PT_EXPR)
                    {
                      tmp->info.expr.arg1->info.expr.paren_type = 1;
                    }
                  if (tmp->info.expr.arg2->node_type == PT_EXPR)
                    {
                      tmp->info.expr.arg2->info.expr.paren_type = 1;
                    }
                  name_expr = name_expr->info.expr.arg1;
                  result = tmp;
                }
              break;
            }

          if (pt_find_var (name_expr->info.expr.arg2, 0))
            {
              if (result_isnull)
                {
                  /* no need to invert result because
                   * result already has a null */
                  name_expr = name_expr->info.expr.arg2;
                }
              else
                {
                  tmp = parser_new_node (parser, PT_EXPR);
                  if (tmp == NULL)
                    {
                      PT_INTERNAL_ERROR (parser, "allocate new node");
                      return NULL;
                    }

                  tmp->info.expr.op = PT_DIVIDE;
                  tmp->info.expr.arg1 = result;
                  tmp->info.expr.arg2 = parser_copy_tree (parser, name_expr->info.expr.arg1);
                  if (tmp->info.expr.arg2 == NULL)
                    {
                      PT_INTERNAL_ERROR (parser, "parser_copy_tree");
                      return NULL;
                    }

                  if (tmp->info.expr.arg1->node_type == PT_EXPR)
                    {
                      tmp->info.expr.arg1->info.expr.paren_type = 1;
                    }
                  if (tmp->info.expr.arg2->node_type == PT_EXPR)
                    {
                      tmp->info.expr.arg2->info.expr.paren_type = 1;
                    }
                  name_expr = name_expr->info.expr.arg2;
                  result = tmp;
                }
              break;
            }

          return NULL;

        case PT_CAST:
          /* special case */
          name_expr = name_expr->info.expr.arg1;
          break;

        default:
          return NULL;
        }
    }

  /* set type of expression */
  if (!result_isnull)
    {
      sc_info.top_node = name_expr;
      result = pt_semantic_type (parser, result, &sc_info);
    }

  if (result)
    {
      /* return name and resulting expression */
      result->next = parser_copy_tree (parser, name_expr);
    }

  if (pt_has_error (parser))
    {
      /* if we got an error just indicate not-invertible, end return with
       * previous error state. */
      parser->error_msgs = msgs;
      return NULL;
    }

  return result;
}

/*
 * pt_find_var () - Explores an expression looking for a path expr.
 *                  Count these and return the count
 *   return: number of path (PT_NAME node) expressions in the tree
 *   p(in): an parse tree representing the syntactic
 *   result(out): for returning a result expression pointer
 */

int
pt_find_var (PT_NODE * p, PT_NODE ** result)
{
  if (!p)
    return 0;

  /* got a name expression */
  if (p->node_type == PT_NAME || (p->node_type == PT_DOT_))
    {
      if (result)
        {
          *result = p;
        }

      return 1;
    }

  /* if an expr (binary op) count both paths */
  if (p->node_type == PT_EXPR)
    {
      return (pt_find_var (p->info.expr.arg1, result) + pt_find_var (p->info.expr.arg2, result));
    }

  return 0;
}

/*
 * pt_remove_from_list () -
 *   return: PT_NODE* to the list without "node" in it
 *   parser(in):
 *   node(in/out):
 *   list(in/out):
 */
PT_NODE *
pt_remove_from_list (PARSER_CONTEXT * parser, PT_NODE * node, PT_NODE * list)
{
  PT_NODE *temp;

  if (!list)
    return list;

  if (node == list)
    {
      temp = node->next;
      node->next = NULL;
      parser_free_tree (parser, node);
      return temp;
    }

  temp = list;
  while (temp && temp->next != node)
    {
      temp = temp->next;
    }

  if (temp)
    {
      temp->next = node->next;
      node->next = NULL;
      parser_free_tree (parser, node);
    }

  return list;
}

/*
 * pt_find_order_value_in_list () - checking an ORDER_BY list for a node with
 *                                  the same value as sort_spec
 *   return: PT_NODE* the found match or NULL
 *   parser(in):
 *   sort_value(in):
 *   order_list(in):
 */

PT_NODE *
pt_find_order_value_in_list (PARSER_CONTEXT * parser, const PT_NODE * sort_value, const PT_NODE * order_list)
{
  PT_NODE *match = NULL;

  match = (PT_NODE *) order_list;

  while (match && sort_value && match->info.sort_spec.expr)
    {
      if (sort_value->node_type == PT_VALUE
          && match->info.sort_spec.expr->node_type == PT_VALUE
          && (match->info.sort_spec.expr->info.value.data_value.i == sort_value->info.value.data_value.i))
        {
          break;
        }
      else if (sort_value->node_type == PT_NAME
               && match->info.sort_spec.expr->node_type == PT_NAME
               && (pt_check_path_eq (parser, sort_value, match->info.sort_spec.expr) == 0))
        {
          /* when create/alter view, columns which are not in select list
           * will not be replaced as value type.
           */
          break;
        }
      else
        {
          match = match->next;
        }
    }

  return match;
}

/*
 * pt_check_order_by () - checking an ORDER_BY clause
 *   return:
 *   parser(in):
 *   query(in): query node has ORDER BY
 *
 * Note :
 * If it is an INTEGER, make sure it does not exceed the number of items
 * in the select list.
 * If it is a path expression, match it with an item in the select list and
 * replace it with the corresponding INTEGER.
 * IF not match, add hidden_column to select_list.
 * For the order-by clause of a UNION/INTERSECTION type query,
 * simply check that the items are ALL INTEGERS and it does not
 * exceed the number of items in the select list..
 */

int
pt_check_order_by (PARSER_CONTEXT * parser, PT_NODE * query)
{
  PT_NODE *select_list, *order_by, *col, *r, *temp, *order, *match;
  int n, i, select_list_len;
  bool ordbynum_flag;
  char *r_str = NULL;
  int error;
  bool skip_orderby_num = false;

  /* initinalize local variables */
  error = NO_ERROR;
  select_list = order_by = NULL;

  /* get select_list */
  switch (query->node_type)
    {
    case PT_SELECT:
      select_list = query->info.query.q.select.list;
      break;

    case PT_UNION:
    case PT_INTERSECTION:
    case PT_DIFFERENCE:
      {
        PT_NODE *arg1, *arg2;

        /* traverse through nested union */
        temp = query;
        while (1)
          {
            arg1 = temp->info.query.q.union_.arg1;
            arg2 = temp->info.query.q.union_.arg2;

            if (PT_IS_QUERY (arg1))
              {
                if (arg1->node_type == PT_SELECT)
                  {             /* found, exit loop */
                    select_list = arg1->info.query.q.select.list;
                    break;
                  }
                else
                  {
                    temp = arg1;        /* continue */
                  }
              }
            else
              {
                /* should not get here, that is an error! */
                error = MSGCAT_SEMANTIC_UNION_INCOMPATIBLE;
                PT_ERRORmf2 (parser, arg1,
                             MSGCAT_SET_PARSER_SEMANTIC,
                             error, pt_short_print (parser, arg1), pt_short_print (parser, arg2));
                break;
              }
          }
      }
      break;

    default:
      break;
    }

  /* not query statement or error occurs */
  if (select_list == NULL)
    {
      return error;
    }

  if (query->node_type == PT_SELECT && pt_is_single_tuple (parser, query))
    {
      /*
       * This case means "select count(*) from athlete order by code"
       * we will remove order by clause to avoid error message
       * but, "select count(*) from athlete order by 2" should make out of range err
       */
      if (query->info.query.order_by != NULL)
        {
          PT_NODE head;
          PT_NODE *last = &head;
          PT_NODE *order_by = query->info.query.order_by;

          last->next = NULL;
          while (order_by != NULL)
            {
              PT_NODE *next = order_by->next;
              order_by->next = NULL;

              if (order_by->info.sort_spec.expr->node_type == PT_NAME)
                {
                  parser_free_node (parser, order_by);
                  skip_orderby_num = true;
                }
              else
                {
                  last->next = order_by;
                  last = order_by;
                }

              order_by = next;
            }

          query->info.query.order_by = head.next;
        }

      /*
       * This case means "select count(*) from athlete limit 1"
       * This limit clause should be evaluated after "select count(*) from athlete"
       * So we will change it as subquery.
       */
      if (query->info.query.limit != NULL)
        {
          SEMANTIC_CHK_INFO sc_info = { NULL, false };
          PT_NODE *limit = query->info.query.limit;
          query->info.query.limit = NULL;

          /* rewrite as derived table */
          query = mq_rewrite_aggregate_as_derived (parser, query);
          if (query == NULL || pt_has_error (parser))
            {
              return ER_FAILED;
            }

          /* clear spec_ids of names referring derived table and re-run
           * name resolving; their types are not resolved and, since we're on
           * the post stage of the tree walk, this will not happen naturally;
           */
          query->info.query.q.select.list =
            mq_clear_ids (parser, query->info.query.q.select.list, query->info.query.q.select.from);

          /* re-resolve names */
          sc_info.top_node = query;
          query = pt_resolve_names (parser, query, &sc_info);
          if (pt_has_error (parser) || query == NULL)
            {
              error = er_errid ();
              if (error == NO_ERROR)
                {
                  error = ER_FAILED;    /* defense code */
                }
              return error;
            }

          query->info.query.limit = limit;
        }
    }

  /* get ORDER BY clause */
  order_by = query->info.query.order_by;
  if (order_by == NULL)
    {
      if (query->node_type == PT_SELECT)
        {
          /* need to check select_list */
          goto check_select_list;
        }

      /* union has not ORDER BY */
      return error;
    }

  /* save original length of select_list */
  select_list_len = pt_length_of_select_list (select_list, EXCLUDE_HIDDEN_COLUMNS);
  for (order = order_by; order; order = order->next)
    {
      /* get the EXPR */
      r = order->info.sort_spec.expr;
      if (r == NULL)
        {                       /* impossible case */
          continue;
        }

      /* if a good integer, done */
      if (r->node_type == PT_VALUE)
        {
          if (r->type_enum == PT_TYPE_INTEGER)
            {
              n = r->info.value.data_value.i;
              /* check size of the integer */
              if (n > select_list_len || n < 1)
                {
                  error = MSGCAT_SEMANTIC_SORT_SPEC_RANGE_ERR;
                  PT_ERRORmf (parser, r, MSGCAT_SET_PARSER_SEMANTIC, error, n);
                  /* go ahead */
                }
              else
                {
                  /* the following invalid query cause error in here
                   * SELECT orderby_num() FROM t ORDER BY 1; */
                  for (col = select_list, i = 1; i < n; i++)
                    {
                      col = col->next;
                    }

                  if (col->node_type == PT_EXPR && col->info.expr.op == PT_ORDERBY_NUM)
                    {
                      error = MSGCAT_SEMANTIC_SORT_SPEC_NAN_PATH;
                      PT_ERRORmf (parser, col, MSGCAT_SET_PARSER_SEMANTIC, error, "ORDERBY_NUM()");
                      /* go ahead */
                    }
                }
            }
          else
            {
              error = MSGCAT_SEMANTIC_SORT_SPEC_WANT_NUM;
              PT_ERRORm (parser, r, MSGCAT_SET_PARSER_SEMANTIC, error);
              /* go ahead */
            }
        }
      else if (r->node_type == PT_HOST_VAR)
        {
          PT_ERRORmf (parser, r, MSGCAT_SET_PARSER_SEMANTIC,
                      MSGCAT_SEMANTIC_NO_ORDERBY_ALLOWED, pt_short_print (parser, r));
          error = MSGCAT_SEMANTIC_NO_ORDERBY_ALLOWED;
        }
      else
        {
          /* not an integer value.
             Try to match with something in the select list. */

          n = 1;                /* a counter for position in select_list */
          if (r->node_type != PT_NAME && r->node_type != PT_DOT_)
            {
              r_str = parser_print_tree (parser, r);
            }

          for (col = select_list; col; col = col->next)
            {
              /* if match, break; */
              if (r->node_type == col->node_type)
                {
                  if (r->node_type == PT_NAME || r->node_type == PT_DOT_)
                    {
                      if (pt_check_path_eq (parser, r, col) == 0)
                        {
                          break;        /* match */
                        }
                    }
                  else
                    {
                      if (pt_str_compare (r_str, parser_print_tree (parser, col), CASE_INSENSITIVE) == 0)
                        {
                          break;        /* match */
                        }
                    }
                }
              else if (pt_check_compatible_node_for_orderby (parser, r, col))
                {
                  break;
                }
              n++;
            }

          /* if end of list, no match create a hidden column node
             and append to select_list */
          if (col == NULL)
            {
              if (query->node_type != PT_SELECT)
                {
                  error = MSGCAT_SEMANTIC_ORDERBY_IS_NOT_INT;
                  PT_ERRORm (parser, r, MSGCAT_SET_PARSER_SEMANTIC, error);
                }
              else if (query->info.query.all_distinct == PT_DISTINCT)
                {
                  error = MSGCAT_SEMANTIC_INVALID_ORDERBY_WITH_DISTINCT;
                  PT_ERRORm (parser, r, MSGCAT_SET_PARSER_SEMANTIC, error);
                }
              else
                {
                  /* when check order by clause in create/alter view, do not
                   * change order_by and select_list. The order by clause
                   * will be replaced in mq_translate_subqueries() again.
                   */
                  if (query->do_not_replace_orderby)
                    {
                      continue;
                    }

                  col = parser_copy_tree (parser, r);
                  if (col == NULL)
                    {
                      error = MSGCAT_SEMANTIC_OUT_OF_MEMORY;
                      PT_ERRORm (parser, r, MSGCAT_SET_PARSER_SEMANTIC, error);
                      return error;     /* give up */
                    }
                  else
                    {
                      /* mark as a hidden column */
                      col->is_hidden_column = 1;
                      parser_append_node (col, select_list);
                    }
                }
            }

          /* we got a match=n, Create a value node and replace expr with it */
          temp = parser_new_node (parser, PT_VALUE);
          if (temp == NULL)
            {
              error = MSGCAT_SEMANTIC_OUT_OF_MEMORY;
              PT_ERRORm (parser, r, MSGCAT_SET_PARSER_SEMANTIC, error);
            }
          else
            {
              temp->type_enum = PT_TYPE_INTEGER;
              temp->info.value.data_value.i = n;
              pt_value_to_db (parser, temp);
              parser_free_tree (parser, r);
              order->info.sort_spec.expr = temp;
            }
        }

      if (error != NO_ERROR)
        {                       /* something wrong */
          continue;             /* go ahead */
        }

      /* set order by position num */
      order->info.sort_spec.pos_descr.pos_no = n;

    }                           /* for (order = order_by; ...) */

  /* now check for duplicate entries.
   *  - If they match on ascending/descending, remove the second.
   *  - If they do not, generate an error. */
  for (order = order_by; order; order = order->next)
    {
      while ((match = pt_find_order_value_in_list (parser, order->info.sort_spec.expr, order->next)))
        {
          if ((order->info.sort_spec.asc_or_desc !=
               match->info.sort_spec.asc_or_desc) || (pt_to_null_ordering (order) != pt_to_null_ordering (match)))
            {
              error = MSGCAT_SEMANTIC_SORT_DIR_CONFLICT;
              PT_ERRORmf (parser, match, MSGCAT_SET_PARSER_SEMANTIC, error, pt_short_print (parser, match));
              break;
            }
          else
            {
              order->next = pt_remove_from_list (parser, match, order->next);
            }
        }
    }

  if (error != NO_ERROR)
    {                           /* give up */
      return error;
    }

check_select_list:

  /* orderby_num() in select list restriction check */
  for (col = select_list; col; col = col->next)
    {
      if (PT_IS_QUERY_NODE_TYPE (col->node_type))
        {
          /* skip orderby_num() expression in subqueries */
          continue;
        }

      if (col->node_type == PT_EXPR && col->info.expr.op == PT_ORDERBY_NUM)
        {
          if (!order_by && !skip_orderby_num)
            {
              /* the following invalid query cause error in here;
               *   SELECT orderby_num() FROM t; */
              error = MSGCAT_SEMANTIC_SORT_SPEC_NOT_EXIST;
              PT_ERRORmf (parser, col, MSGCAT_SET_PARSER_SEMANTIC, error, "ORDERBY_NUM()");
              break;
            }
        }
      else
        {
          /* the following query cause error in here;
           *   SELECT orderby_num()+1      FROM t;
           *   SELECT orderby_num()+1, a   FROM t ORDER BY 2;
           *   SELECT {orderby_num()}      FROM t;
           *   SELECT {orderby_num()+1}, a FROM t ORDER BY 2;
           */
          ordbynum_flag = false;
          (void) parser_walk_leaves (parser, col,
                                     pt_check_orderbynum_pre, NULL, pt_check_orderbynum_post, &ordbynum_flag);
          if (ordbynum_flag)
            {
              error = MSGCAT_SEMANTIC_ORDERBYNUM_SELECT_LIST_ERR;
              PT_ERRORm (parser, col, MSGCAT_SET_PARSER_SEMANTIC, error);
              break;
            }
        }
    }

  return error;
}

/*
 * pt_check_path_eq () - determine if two path expressions are the same
 *   return: 0 if two path expressions are the same, else non-zero.
 *   parser(in):
 *   p(in):
 *   q(in):
 */
int
pt_check_path_eq (PARSER_CONTEXT * parser, const PT_NODE * p, const PT_NODE * q)
{
  PT_NODE_TYPE n;

  if (p == NULL && q == NULL)
    {
      return 0;
    }

  if (p == NULL || q == NULL)
    {
      return 1;
    }

  /* check node types are same */
  if (p->node_type != q->node_type)
    {
      return 1;
    }

  n = p->node_type;
  switch (n)
    {
      /* if a name, the original and resolved fields must match */
    case PT_NAME:
      if (pt_str_compare (p->info.name.original, q->info.name.original, CASE_INSENSITIVE))
        {
          return 1;
        }
      if (pt_str_compare (p->info.name.resolved, q->info.name.resolved, CASE_INSENSITIVE))
        {
          return 1;
        }
      if (p->info.name.spec_id != q->info.name.spec_id)
        {
          return 1;
        }
      break;

      /* EXPR must be X.Y.Z. */
    case PT_DOT_:
      if (pt_check_path_eq (parser, p->info.dot.arg1, q->info.dot.arg1))
        {
          return 1;
        }

      /* A recursive call on arg2 should work, except that we have not
       * yet recognised common sub-path expressions
       * However, it is also sufficient and true that the left
       * path be strictly equal and arg2's names match.
       * That even allows us to use this very function to
       * implement recognition of common path expressions.
       */
      if (p->info.dot.arg2 == NULL || q->info.dot.arg2 == NULL)
        {
          return 1;
        }

      if (p->info.dot.arg2->node_type != PT_NAME || q->info.dot.arg2->node_type != PT_NAME)
        {
          return 1;
        }

      if (pt_str_compare (p->info.dot.arg2->info.name.original, q->info.dot.arg2->info.name.original, CASE_INSENSITIVE))
        {
          return 1;
        }

      break;

    default:
      PT_ERRORmf (parser, p, MSGCAT_SET_PARSER_SEMANTIC,
                  MSGCAT_SEMANTIC_SORT_SPEC_NAN_PATH, pt_short_print (parser, p));
      return 1;
    }

  return 0;
}

/*
 * pt_coerce_insert_values () - try to coerce the insert values to the types
 *  	                        indicated by the insert attributes
 *   return:
 *   parser(in): handle to context used to parse the insert statement
 *   stmt(in): the AST form of an insert statement
 */
static PT_NODE *
pt_coerce_insert_values (PARSER_CONTEXT * parser, PT_NODE * stmt)
{
  PT_NODE *v = NULL, *crt_list = NULL;
  int a_cnt = 0, v_cnt = 0;
  PT_NODE *attr_list = NULL;
  PT_NODE *value_clauses = NULL;

  /* preconditions are not met */
  if (stmt->node_type != PT_INSERT)
    {
      return NULL;
    }

  attr_list = pt_attrs_part (stmt);
  value_clauses = stmt->info.insert.value_clauses;

  a_cnt = pt_length_of_list (attr_list);

  for (crt_list = value_clauses; crt_list != NULL; crt_list = crt_list->next)
    {
      if (crt_list->info.node_list.list_type == PT_IS_DEFAULT_VALUE)
        {
          v = NULL;
        }
      else if (crt_list->info.node_list.list_type == PT_IS_SUBQUERY)
        {
          /* this sort of nods at union queries */
          v = pt_get_select_list (parser, crt_list->info.node_list.list);
          v_cnt = pt_length_of_select_list (v, EXCLUDE_HIDDEN_COLUMNS);
          if (a_cnt != v_cnt)
            {
              PT_ERRORmf2 (parser, stmt, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_ATT_CNT_COL_CNT_NE, a_cnt, v_cnt);
            }
          else
            {
              continue;
            }
        }
      else
        {
          v = crt_list->info.node_list.list;
          v_cnt = pt_length_of_list (v);
          if (a_cnt != v_cnt)
            {
              PT_ERRORmf2 (parser, stmt, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_ATT_CNT_VAL_CNT_NE, a_cnt, v_cnt);
            }
        }
    }

  return stmt;
}

/*
 * pt_check_sub_insert () - Checks if sub-inserts are semantically correct
 *
 * return	      : Unchanged node argument.
 * parser (in)	      : Parser context.
 * node (in)	      : Parse tree node.
 * void_arg (in)      : Unused argument.
 * continue_walk (in) : Continue walk.
 */
static PT_NODE *
pt_check_sub_insert (PARSER_CONTEXT * parser, PT_NODE * node, UNUSED_ARG void *void_arg, int *continue_walk)
{
  PT_NODE *entity_name = NULL, *value_clauses = NULL;

  if (*continue_walk == PT_STOP_WALK)
    {
      return node;
    }

  switch (node->node_type)
    {
    case PT_INSERT:
      assert (false);
      /* continue to checks */
      *continue_walk = PT_LIST_WALK;
      break;
    case PT_SELECT:
    case PT_INTERSECTION:
    case PT_DIFFERENCE:
    case PT_UNION:
      /* stop advancing into this node */
      *continue_walk = PT_LIST_WALK;
      return node;
    default:
      /* do nothing */
      *continue_walk = PT_CONTINUE_WALK;
      return node;
    }

  /* Check current insert node */
  value_clauses = node->info.insert.value_clauses;
  if (value_clauses->next)
    {
      /* Only one row is allowed for sub-inserts */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_DO_INSERT_TOO_MANY, 0);
      if (!pt_has_error (parser))
        {
          PT_ERRORc (parser, node, db_error_string (3));
        }
      *continue_walk = PT_STOP_WALK;
      return node;
    }
  entity_name = node->info.insert.spec->info.spec.entity_name;
  if (entity_name == NULL || entity_name->info.name.db_object == NULL)
    {
      PT_INTERNAL_ERROR (parser, "Unresolved insert spec");
      *continue_walk = PT_STOP_WALK;
      return node;
    }

  /* Inserting OID is not allowed */
  PT_ERRORmf (parser, node, MSGCAT_SET_PARSER_SEMANTIC,
              MSGCAT_SEMANTIC_NON_REFERABLE_VIOLATION, entity_name->info.name.original);
  *continue_walk = PT_STOP_WALK;

  return node;
}

/*
 * pt_count_input_markers () - If the node is a input host variable marker,
 *      compare its index+1 against *num_ptr and record the bigger of
 *      the two into *num_ptr
 *   return:
 *   parser(in):
 *   node(in): the node to check
 *   arg(in/out):
 *   continue_walk(in):
 */

PT_NODE *
pt_count_input_markers (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * node, void *arg, UNUSED_ARG int *continue_walk)
{
  int *num_markers;

  num_markers = (int *) arg;

  if (pt_is_input_hostvar (node))
    {
      if (*num_markers < node->info.host_var.index + 1)
        {
          *num_markers = node->info.host_var.index + 1;
        }
    }

  return node;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * pt_validate_query_spec () - check if a query_spec is compatible with a
 * 			       given {vclass} object
 *   return: an error code if checking found an error, NO_ERROR otherwise
 *   parser(in): handle to context used to parse the query_specs
 *   s(in): a query_spec in parse_tree form
 *   c(in): a vclass object
 */

int
pt_validate_query_spec (PARSER_CONTEXT * parser, PT_NODE * s, DB_OBJECT * c)
{
  PT_NODE *attrs = NULL;
  int error_code = NO_ERROR;

  assert (parser != NULL && s != NULL && c != NULL);

  /* a syntax error for query_spec */
  if (pt_has_error (parser))
    {
      pt_report_to_ersys (parser, PT_SYNTAX);
      error_code = er_errid ();
      goto error_exit;
    }

  if (!db_is_vclass (c))
    {
      error_code = ER_OBJ_INVALID_ARGUMENTS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 0);
      goto error_exit;
    }

  s = parser_walk_tree (parser, s, pt_set_is_view_spec, NULL, NULL, NULL);
  assert (s != NULL);

  attrs = pt_get_attributes (parser, c);

  /* apply semantic checks to query_spec */
  s = pt_check_vclass_query_spec (parser, s, attrs, sm_class_name (c), true);
  if (pt_has_error (parser))
    {
      pt_report_to_ersys (parser, PT_SEMANTIC);
      error_code = er_errid ();
      goto error_exit;
    }
  if (s == NULL)
    {
      error_code = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1, "");
      goto error_exit;
    }

  s = pt_type_cast_vclass_query_spec (parser, s, attrs);
  if (pt_has_error (parser))
    {
      pt_report_to_ersys (parser, PT_SEMANTIC);
      error_code = er_errid ();
      goto error_exit;
    }
  if (s == NULL)
    {
      error_code = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1, "");
      goto error_exit;
    }

  return error_code;

error_exit:
  return error_code;
}

/*
 * pt_check_xaction_list () - Checks to see if there is more than one
 *      isolation level clause or more than one timeout value clause
 *   return:
 *   parser(in):
 *   node(in): the node to check
 */

static void
pt_check_xaction_list (PARSER_CONTEXT * parser, PT_NODE * node)
{
  int num_iso_nodes = 0;
  int num_time_nodes = 0;

  (void) parser_walk_tree (parser, node, pt_count_iso_nodes, &num_iso_nodes, NULL, NULL);

  (void) parser_walk_tree (parser, node, pt_count_time_nodes, &num_time_nodes, NULL, NULL);

  if (num_iso_nodes > 1)
    {
      PT_ERRORm (parser, node, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_GT_1_ISOLATION_LVL);
    }

  if (num_time_nodes > 1)
    {
      PT_ERRORm (parser, node, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_GT_1_TIMEOUT_CLAUSES);
    }
}

/*
 * pt_count_iso_nodes () - returns node unchanged, count by reference
 *   return:
 *   parser(in):
 *   node(in): the node to check
 *   arg(in/out): count of isolation level nodes
 *   continue_walk(in):
 */

static PT_NODE *
pt_count_iso_nodes (PARSER_CONTEXT * parser, PT_NODE * node, void *arg, int *continue_walk)
{
  int *cnt = (int *) arg;

  if (node->node_type == PT_ISOLATION_LVL)
    {
      (*cnt)++;
    }

  return node;
}

/*
 * pt_count_time_nodes () - returns node timeouted, count by reference
 *   return:
 *   parser(in):
 *   node(in): the node to check
 *   arg(in/out): count of timeout nodes
 *   continue_walk(in):
 */
static PT_NODE *
pt_count_time_nodes (PARSER_CONTEXT * parser, PT_NODE * node, void *arg, int *continue_walk)
{
  int *cnt = (int *) arg;

  if (node->node_type == PT_TIMEOUT)
    {
      (*cnt)++;
    }

  return node;
}

/*
 * pt_check_isolation_lvl () - checks isolation level node
 *   return:
 *   parser(in):
 *   node(in/out): the node to check
 *   arg(in):
 *   continue_walk(in):
 *
 * Note :
 * checks
 * 1) if isolation levels for schema & instances are compatible.
 * 2) if isolation number entered, check to see if it is valid.
 */

static PT_NODE *
pt_check_isolation_lvl (PARSER_CONTEXT * parser, PT_NODE * node, void *arg, int *continue_walk)
{
  DB_TRAN_ISOLATION cur_lvl;
  int dummy;

  if (node->node_type == PT_ISOLATION_LVL)
    {
      if (node->info.isolation_lvl.level != NULL)
        {
          /* assume correct type, value checking will be done at run-time */
          return node;
        }

      /* check to make sure an isolation level has been given */
      if ((node->info.isolation_lvl.schema == PT_NO_ISOLATION_LEVEL)
          && (node->info.isolation_lvl.instances == PT_NO_ISOLATION_LEVEL))
        {
          PT_ERRORm (parser, node, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_NO_ISOLATION_LVL_MSG);
        }

      /* get the current isolation level in case user is defaulting either
       * the schema or the instances level. */
      (void) db_get_tran_settings (&dummy, &cur_lvl);

      if (node->info.isolation_lvl.schema == PT_NO_ISOLATION_LEVEL)
        {
          switch (cur_lvl)
            {
#if 0                           /* unused */
            case TRAN_UNKNOWN_ISOLATION:
              /* in this case, the user is specifying only the instance
               * level when there was not a previous isolation level
               * set.  Default the schema isolation level to the instance
               * isolation level.
               */
              node->info.isolation_lvl.schema = node->info.isolation_lvl.instances;
              break;

            case TRAN_COMMIT_CLASS_UNCOMMIT_INSTANCE:
            case TRAN_COMMIT_CLASS_COMMIT_INSTANCE:
              node->info.isolation_lvl.schema = PT_READ_COMMITTED;
              break;
#endif

            case TRAN_DEFAULT_ISOLATION:
#if 0                           /* unused */
            case TRAN_REP_CLASS_COMMIT_INSTANCE:
            case TRAN_REP_CLASS_REP_INSTANCE:
#endif
              node->info.isolation_lvl.schema = PT_REPEATABLE_READ;
              break;

#if 0                           /* unused */
            case TRAN_SERIALIZABLE:
              node->info.isolation_lvl.schema = PT_SERIALIZABLE;
              break;
#endif
            default:
              assert (false);
              break;
            }
        }

      if (node->info.isolation_lvl.instances == PT_NO_ISOLATION_LEVEL)
        {
          switch (cur_lvl)
            {
#if 0                           /* unused */
            case TRAN_UNKNOWN_ISOLATION:
              /* in this case, the user is specifying only the schema
               * level when there was not a previous isolation level
               * set.  Default the instances isolation level to the schema
               * isolation level.
               */
              node->info.isolation_lvl.instances = node->info.isolation_lvl.schema;
              break;

            case TRAN_COMMIT_CLASS_UNCOMMIT_INSTANCE:
#endif
            case TRAN_DEFAULT_ISOLATION:
              node->info.isolation_lvl.instances = PT_READ_UNCOMMITTED;
              break;

#if 0                           /* unused */
            case TRAN_REP_CLASS_COMMIT_INSTANCE:
            case TRAN_COMMIT_CLASS_COMMIT_INSTANCE:
              node->info.isolation_lvl.instances = PT_READ_COMMITTED;
              break;

            case TRAN_REP_CLASS_REP_INSTANCE:
              node->info.isolation_lvl.instances = PT_REPEATABLE_READ;
              break;

            case TRAN_SERIALIZABLE:
              node->info.isolation_lvl.instances = PT_SERIALIZABLE;
              break;
#endif

            default:
              assert (false);
              break;
            }
        }

      /* coercing/correcting of incompatible level
       *  happens in do_set_xaction() */
    }

  return node;
}

/*
 * pt_find_attr_def () - Finds the PT_NODE in attr_def_list with the same
 *  			 original_name as the given name
 *   return:  db_user instance if user exists, NULL otherwise.
 *   attr_def_list(in): the list of attr_def's in a CREATE_ENTITY node
 *   name(in): a PT_NAME node
 */

PT_NODE *
pt_find_attr_def (const PT_NODE * attr_def_list, const PT_NODE * name)
{
  PT_NODE *p;

  for (p = (PT_NODE *) attr_def_list; p; p = p->next)
    {
      if (intl_identifier_casecmp (p->info.attr_def.attr_name->info.name.original, name->info.name.original) == 0)
        {
          break;
        }
    }

  return p;
}

/*
 * pt_find_cnstr_def () - Finds the PT_NODE in cnstr_def_list with the same
 *                        original_name as the given name
 *   return:  attribute instance iff attribute exists, NULL otherwise.
 *   cnstr_def_list(in): the list of constraint elements
 *   name(in): a PT_NAME node
 */
PT_NODE *
pt_find_cnstr_def (const PT_NODE * cnstr_def_list, const PT_NODE * name)
{
  PT_NODE *p;

  for (p = (PT_NODE *) cnstr_def_list; p; p = p->next)
    {
      if (intl_identifier_casecmp (p->info.name.original, name->info.name.original) == 0)
        {
          break;
        }
    }

  return p;
}
#endif

/*
 * pt_check_constraint () - Checks the given constraint appears to be valid
 *   return:  the constraint node
 *   parser(in): the current parser context
 *   create(in): a CREATE_ENTITY node
 *   constraint(in): a CONSTRAINT node, assumed to have come from the
 *                   constraint_list of "create"
 *
 * Note :
 *    Right now single-column UNIQUE and NOT NULL constraints are the only
 *    ones that are understood.  All others are ignored (with a warning).
 *    Unfortunately, this can't do the whole job because at this point we know
 *    nothing about inherited attributes, etc.  For example, someone could be
 *    trying to add a UNIQUE constraint to an inherited attribute, but we won't
 *    be able to handle it because we'll be unable to resolve the name.  Under
 *    the current architecture the template stuff will need to be extended to
 *    understand constraints.
 */

static PT_NODE *
pt_check_constraint (PARSER_CONTEXT * parser, const PT_NODE * create, const PT_NODE * constraint)
{
  switch (constraint->info.constraint.type)
    {
    case PT_CONSTRAIN_UNKNOWN:
      goto warning;

    case PT_CONSTRAIN_NULL:
    case PT_CONSTRAIN_NOT_NULL:
    case PT_CONSTRAIN_UNIQUE:
    case PT_CONSTRAIN_INDEX:
      break;

    case PT_CONSTRAIN_PRIMARY_KEY:
    case PT_CONSTRAIN_CHECK:
      if (create->info.create_entity.entity_type != PT_CLASS)
        {
          goto error;
        }
      else
        {
          goto warning;
        }

    default:
      assert (false);
      break;
    }

  return (PT_NODE *) constraint;

warning:
  PT_WARNINGmf (parser, constraint, MSGCAT_SET_PARSER_SEMANTIC,
                MSGCAT_SEMANTIC_UNIMPLEMENTED_CONSTRAINT, parser_print_tree (parser, constraint));
  return (PT_NODE *) constraint;

error:
  PT_ERRORm (parser, constraint, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_WANT_NO_CONSTRAINTS);
  return NULL;
}

/*
 * pt_check_constraints () - Checks all of the constraints given in
 *                           this CREATE_ENTITY node
 *   return:  the CREATE_ENTITY node
 *   parser(in): the current parser context
 *   create(in): a CREATE_ENTITY node
 */
static PT_NODE *
pt_check_constraints (PARSER_CONTEXT * parser, const PT_NODE * create)
{
  PT_NODE *constraint;

  for (constraint = create->info.create_entity.constraint_list; constraint; constraint = constraint->next)
    {
      (void) pt_check_constraint (parser, create, constraint);
    }

  return (PT_NODE *) create;
}

/*
 * pt_check_defaultf () - Checks to see if default function is well-formed
 *   return: none
 *   parser(in):
 *   node(in): the node to check
 */
static int
pt_check_defaultf (PARSER_CONTEXT * parser, PT_NODE * node)
{
  PT_NODE *arg;

  assert (node != NULL && node->node_type == PT_EXPR && node->info.expr.op == PT_DEFAULTF);
  if (node == NULL || node->node_type != PT_EXPR || node->info.expr.op != PT_DEFAULTF)
    {
      PT_INTERNAL_ERROR (parser, "bad node type");
      return ER_FAILED;
    }

  arg = node->info.expr.arg1;

  /* OIDs don't have default value */
  if (arg == NULL || arg->node_type != PT_NAME || arg->info.name.meta_class == PT_OID_ATTR)
    {
      PT_ERRORm (parser, node, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_DEFAULT_JUST_COLUMN_NAME);
      return ER_FAILED;
    }

  /* Argument of DEFAULT function should be given. So, PT_NAME_INFO_FILL_DEFAULT
   * bit might be always set when expression node was created. The following
   * assertion and defensive code will be used to handle unexpected situation.
   */
  assert (PT_NAME_INFO_IS_FLAGED (arg, PT_NAME_INFO_FILL_DEFAULT) && arg->info.name.default_value != NULL);
  if (!PT_NAME_INFO_IS_FLAGED (arg, PT_NAME_INFO_FILL_DEFAULT) || arg->info.name.default_value == NULL)
    {
      PT_INTERNAL_ERROR (parser, "bad DEFAULTF node");
      return ER_FAILED;
    }

  /* In case of no default value defined on an attribute:
   * DEFAULT function returns NULL when the attribute given as argument
   * has UNIQUE or no constraint, but it returns a semantic error for
   * PRIMARY KEY or NOT NULL constraint.
   */
  if (arg->info.name.resolved && arg->info.name.original)
    {
      DB_ATTRIBUTE *db_att = NULL;
      db_att = db_get_attribute_by_name (arg->info.name.resolved, arg->info.name.original);

      if (db_att)
        {
          if ((db_attribute_is_primary_key (db_att) ||
               db_attribute_is_non_null (db_att)) &&
              arg->info.name.default_value->node_type == PT_VALUE &&
              DB_IS_NULL (&(arg->info.name.default_value->info.value.db_value)))
            {
              er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_ATTRIBUTE_CANT_BE_NULL, 1, arg->info.name.original);
              PT_ERRORc (parser, arg, er_msg ());
              return ER_FAILED;
            }
        }
    }
  return NO_ERROR;
}


/*
 * pt_check_group_concat_order_by () - checks an ORDER_BY clause of a
 *			      GROUP_CONCAT aggregate function;
 *			      if the expression or identifier from
 *			      ORDER BY clause matches an argument of function,
 *			      the ORDER BY item is converted into associated
 *			      number.
 *   return:
 *   parser(in):
 *   query(in): query node has ORDER BY
 *
 *
 *  Note :
 *
 * Only one order by item is supported :
 *    - if it is an INTEGER, make sure it does not exceed the number of items
 *	in the argument list.
 *    - if it is a path expression, match it with an argument in the
 *	function's argument list and replace the node with a PT_VALUE
 *	with corresponding number.
 *    - if it doesn't match, an error is issued.
 */
static int
pt_check_group_concat_order_by (PARSER_CONTEXT * parser, PT_NODE * func)
{
  PT_NODE *arg_list = NULL;
  PT_NODE *order_by = NULL;
  PT_NODE *arg = NULL;
  PT_NODE *temp, *order;
  int n, i, arg_list_len;
  int error = NO_ERROR;
  PT_NODE *group_concat_sep_node_save = NULL;

  assert (func->info.function.function_type == PT_GROUP_CONCAT);

  /* get ORDER BY clause */
  order_by = func->info.function.order_by;
  if (order_by == NULL)
    {
      goto error_exit;
    }

  arg_list = func->info.function.arg_list;
  if (arg_list == NULL)
    {
      PT_ERRORmf (parser, func, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_INVALID_INTERNAL_FUNCTION, "GROUP_CONCAT");
      goto error_exit;
    }

  /* remove separator from list of arguments */
  group_concat_sep_node_save = func->info.function.arg_list->next;
  func->info.function.arg_list->next = NULL;

  /* save original length of select_list */
  arg_list_len = pt_length_of_list (arg_list);
  for (order = order_by; order != NULL; order = order->next)
    {
      /* get the EXPR */
      PT_NODE *r = order->info.sort_spec.expr;
      if (r == NULL)
        {                       /* impossible case */
          continue;
        }

      /* if a good integer, done */
      if (r->node_type == PT_VALUE)
        {
          if (r->type_enum == PT_TYPE_INTEGER)
            {
              n = r->info.value.data_value.i;
              /* check size of the integer */
              if (n > arg_list_len || n < 1)
                {
                  error = MSGCAT_SEMANTIC_SORT_SPEC_RANGE_ERR;
                  PT_ERRORmf (parser, r, MSGCAT_SET_PARSER_SEMANTIC, error, n);
                  /* go ahead */
                }
              else
                {
                  /* goto associated argument: */
                  for (arg = arg_list, i = 1; i < n; i++)
                    {
                      arg = arg->next;
                    }
                }
            }
          else
            {
              error = MSGCAT_SEMANTIC_SORT_SPEC_WANT_NUM;
              PT_ERRORm (parser, r, MSGCAT_SET_PARSER_SEMANTIC, error);
              /* go ahead */
            }
        }
      else
        {
          char *r_str = NULL;
          /* not an integer value.
             Try to match with something in the select list. */

          n = 1;                /* a counter for position in select_list */
          if (r->node_type != PT_NAME && r->node_type != PT_DOT_)
            {
              r_str = parser_print_tree (parser, r);
            }

          for (arg = arg_list; arg != NULL; arg = arg->next)
            {
              /* if match, break; */
              if (r->node_type == arg->node_type)
                {
                  if (r->node_type == PT_NAME || r->node_type == PT_DOT_)
                    {
                      if (pt_check_path_eq (parser, r, arg) == 0)
                        {
                          break;        /* match */
                        }
                    }
                  else
                    {
                      if (pt_str_compare (r_str, parser_print_tree (parser, arg), CASE_INSENSITIVE) == 0)
                        {
                          break;        /* match */
                        }
                    }
                }
              n++;
            }

          /* if end of list -> error : currently aggregate functions don't
           * support other order by expression than arguments*/
          if (arg == NULL)
            {
              error = MSGCAT_SEMANTIC_GROUP_CONCAT_ORDERBY_SAME_EXPR;
              PT_ERRORm (parser, r, MSGCAT_SET_PARSER_SEMANTIC, error);
              /* go ahead */
            }
          else
            {
              /* we got a match=n,
               * Create a value node and replace expr with it*/
              temp = parser_new_node (parser, PT_VALUE);
              if (temp == NULL)
                {
                  error = MSGCAT_SEMANTIC_OUT_OF_MEMORY;
                  PT_ERRORm (parser, r, MSGCAT_SET_PARSER_SEMANTIC, error);
                }
              else
                {
                  temp->type_enum = PT_TYPE_INTEGER;
                  temp->info.value.data_value.i = n;
                  pt_value_to_db (parser, temp);
                  parser_free_tree (parser, r);
                  order->info.sort_spec.expr = temp;
                }
            }
        }

      if (error != NO_ERROR)
        {                       /* something wrong, exit */
          goto error_exit;
        }

      /* at this point <n> contains the sorting position : either specified in
       * statement or computed,
       * and <arg> is the corresponding function argument */
      assert (arg != NULL);
      assert (n > 0 && n <= arg_list_len);

      /* set order by position num */
      order->info.sort_spec.pos_descr.pos_no = n;
    }

  assert (func->info.function.order_by->next == NULL);

error_exit:
  if (group_concat_sep_node_save != NULL)
    {
      func->info.function.arg_list->next = group_concat_sep_node_save;
    }

  return error;
}

/*
 * pt_get_assignments () - get assignment list for INSERT/UPDATE/MERGE
 *			   statements
 * return   : assignment list or NULL
 * node (in): statement node
 */
static PT_NODE *
pt_get_assignments (PT_NODE * node)
{
  if (node == NULL)
    {
      return NULL;
    }

  switch (node->node_type)
    {
    case PT_UPDATE:
      return node->info.update.assignment;
    case PT_INSERT:
      return node->info.insert.odku_assignments;
    default:
      return NULL;
    }
}

/*
 * pt_check_odku_assignments () - check ON DUPLICATE KEY assignments of an
 *				  INSERT statement
 * return : node
 * parser (in) : parser context
 * insert (in) : insert statement
 *
 * Note: this function performs the following validations:
 *    - there are no double assignments
 *    - assignments are only performed to columns belonging to the insert spec
 *    - only instance attributes are updated
 */
PT_NODE *
pt_check_odku_assignments (PARSER_CONTEXT * parser, PT_NODE * insert)
{
  PT_NODE *assignment, *spec, *lhs;
  if (insert == NULL || insert->node_type != PT_INSERT)
    {
      return insert;
    }

  if (insert->info.insert.odku_assignments == NULL)
    {
      return insert;
    }

  pt_no_double_updates (parser, insert);
  if (pt_has_error (parser))
    {
      return NULL;
    }
  pt_check_assignments (parser, insert);
  if (pt_has_error (parser))
    {
      return NULL;
    }

  spec = insert->info.insert.spec;
  for (assignment = insert->info.insert.odku_assignments; assignment != NULL; assignment = assignment->next)
    {
      if (assignment->node_type != PT_EXPR || assignment->info.expr.op != PT_ASSIGN)
        {
          assert (false);
          PT_INTERNAL_ERROR (parser, "semantic");
          return NULL;
        }
      lhs = assignment->info.expr.arg1;
      if (lhs == NULL || lhs->node_type != PT_NAME)
        {
          assert (false);
          PT_INTERNAL_ERROR (parser, "semantic");
          return NULL;
        }
      if (lhs->info.name.spec_id != spec->info.spec.id)
        {
          PT_ERRORm (parser, lhs, MSGCAT_SET_PARSER_SEMANTIC, MSGCAT_SEMANTIC_ILLEGAL_LHS);
          return NULL;
        }
    }
  return insert;
}
