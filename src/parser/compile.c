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
 * compile.c - compile parse tree into executable form
 */

#ident "$Id$"

#include "config.h"

#include <assert.h>

#include "dbi.h"
#include "parser.h"
#include "semantic_check.h"
#include "locator_cl.h"
#include "memory_alloc.h"
#include "schema_manager.h"
#include "parser_message.h"
#include "view_transform.h"
#include "intl_support.h"
#include "server_interface.h"
#include "network_interface_cl.h"
#include "execute_statement.h"

/* this must be the last header file included!!! */
#include "dbval.h"

/* structure used for parser_walk_tree in pt_class_pre_fetch */
typedef struct pt_class_locks PT_CLASS_LOCKS;
struct pt_class_locks
{
  int num_classes;
  int allocated_count;
  LOCK lock_type;
  char **classes;
  LOCK *locks;
};

enum pt_order_by_adjustment
{
  PT_ADD_ONE,
  PT_TIMES_TWO
};

static PT_NODE *pt_add_oid_to_select_list (PARSER_CONTEXT * parser, PT_NODE * statement);
static PT_NODE *pt_count_entities (PARSER_CONTEXT * parser, PT_NODE * node, void *arg, int *continue_walk);
static int pt_add_lock_class (PARSER_CONTEXT * parser, PT_CLASS_LOCKS * lcks, PT_NODE * spec);
static PT_NODE *pt_find_lck_classes (PARSER_CONTEXT * parser, PT_NODE * node, void *arg, int *continue_walk);
static int pt_in_lck_array (PT_CLASS_LOCKS * lcks, const char *str);

/*
 * pt_spec_to_oid_attr () - Generate an oid attribute from a resolved spec.
 * 			    Can be called any time after name resolution.
 *   return:  a PT_NAME node, or a NULL
 *   parser(in): the parser context used to derive stmt
 *   spec(in/out): an entity spec. requires spec has been resolved
 */

PT_NODE *
pt_spec_to_oid_attr (PARSER_CONTEXT * parser, PT_NODE * spec)
{
  PT_NODE *oid = NULL;
  PT_NODE *flat;
  PT_NODE *range;

  if (spec->info.spec.range_var == NULL)
    {
      return NULL;
    }

  flat = spec->info.spec.flat_entity_list;
  range = spec->info.spec.range_var;

  if (spec->info.spec.derived_table && spec->info.spec.flat_entity_list && spec->info.spec.as_attr_list)
    {
      /* this spec should have come from a vclass that was rewritten as a
         derived table; pull ROWOID from as_attr_list */
      return parser_copy_tree (parser, spec->info.spec.as_attr_list);
    }

  /* just generate an oid name, if that is what is asked for
   */
  oid = pt_name (parser, "");
  if (oid == NULL)
    {
      return NULL;
    }

  oid->info.name.resolved = range->info.name.original;
  oid->info.name.meta_class = PT_OID_ATTR;
  oid->info.name.spec_id = spec->info.spec.id;
  oid->type_enum = PT_TYPE_OBJECT;
  oid->data_type = parser_new_node (parser, PT_DATA_TYPE);
  if (oid->data_type == NULL)
    {
      return NULL;
    }

  oid->data_type->type_enum = PT_TYPE_OBJECT;
  oid->data_type->info.data_type.entity = parser_copy_tree_list (parser, flat);

  PT_NAME_INFO_SET_FLAG (oid, PT_NAME_INFO_GENERATED_OID);

  return oid;
}


/*
 * pt_add_oid_to_select_list () - augment a statement's select_list
 *                                to select the oid
 *   return:  none
 *   parser(in): the parser context used to derive statement
 *   statement(in/out): a SELECT/UNION/DIFFERENCE/INTERSECTION statement
 */

static PT_NODE *
pt_add_oid_to_select_list (PARSER_CONTEXT * parser, PT_NODE * statement)
{
  PT_NODE *oid, *from;

  if (!statement)
    {
      return NULL;
    }

  if (PT_IS_QUERY_NODE_TYPE (statement->node_type))
    {
      PT_NODE *p;
#if !defined(NDEBUG)
      PT_NODE *ord;
#endif

      /*
       * It would be nice to make this adjustment more automatic by
       * actually counting the number of "invisible" columns and keeping a
       * running adjustment bias, but right now there doesn't seem to be a
       * way of distinguishing invisible columns from ordinary ones, short
       * of just knowing which style of adjustment is being made (single
       * oid or multiple oids).  If a new style is added, this code will
       * need to be extended.
       */
      p = statement->info.query.order_by;
      while (p)
        {
#if !defined(NDEBUG)
          ord = p->info.sort_spec.expr;
          assert (ord->node_type == PT_VALUE);
#endif

          /* adjust value */
          p->info.sort_spec.pos_descr.pos_no += 1;
          p->info.sort_spec.expr->info.value.data_value.i += 1;
          p->info.sort_spec.expr->info.value.db_value.data.i += 1;

          /* not needed any more */
          p->info.sort_spec.expr->info.value.text = NULL;

          p = p->next;

        }
    }

  if (statement->node_type == PT_SELECT)
    {
      statement->info.query.oids_included = DB_ROW_OIDS;
      from = statement->info.query.q.select.from;
      if (from && from->node_type == PT_SPEC)
        {
          oid = pt_spec_to_oid_attr (parser, from);
          if (oid)
            {
              /* prepend oid to the statement's select_list */
              oid->next = statement->info.query.q.select.list;
              statement->info.query.q.select.list = oid;
            }
        }
    }
  else if (statement->node_type == PT_UNION
           || statement->node_type == PT_INTERSECTION || statement->node_type == PT_DIFFERENCE)
    {

      statement->info.query.oids_included = DB_ROW_OIDS;
      statement->info.query.q.union_.arg1 = pt_add_oid_to_select_list (parser, statement->info.query.q.union_.arg1);
      statement->info.query.q.union_.arg2 = pt_add_oid_to_select_list (parser, statement->info.query.q.union_.arg2);
    }

  return statement;
}

/*
 * pt_add_row_oid_name () - augment a statement's select_list to
 * 			    select the row oid
 *   return:  none
 *   parser(in): the parser context used to derive statement
 *   statement(in/out): a SELECT/UNION/DIFFERENCE/INTERSECTION statement
 */

PT_NODE *
pt_add_row_oid_name (PARSER_CONTEXT * parser, PT_NODE * statement)
{
  return pt_add_oid_to_select_list (parser, statement);
}

/*
 * pt_compile () - Semantic check and convert parse tree into executable form
 *   return:
 *   parser(in):
 *   statement(in/out):
 */
PT_NODE *
pt_compile (PARSER_CONTEXT * parser, PT_NODE * volatile statement)
{
  PT_NODE *next;

  PT_SET_JMP_ENV (parser);

  if (statement)
    {
      next = statement->next;
      statement->next = NULL;

      statement = pt_semantic_check (parser, statement);

      /* restore link */
      if (statement)
        {
          statement->next = next;
        }
    }

  PT_CLEAR_JMP_ENV (parser);

  return statement;
}


/*
 * pt_class_pre_fetch () - minimize potential deadlocks by prefetching
 *      the classes the statement will need in the correct lock mode
 *   return:
 *   parser(in):
 *   statement(in):
 *
 * Note :
 * This routine will not avoid deadlock altogether because classes which
 * are implicitly accessed via path expressions are not know at this time.
 * We are assured however that these implicit classes will be read only
 * classes and will not need lock escalation.  These implicit classes
 * will be locked during semantic analysis.
 *
 * This routine will only prefetch for the following statement types:
 * UPDATE, DELETE, INSERT, SELECT, UNION, DIFFERENCE, INTERSECTION.
 *
 * There are two types of errors:
 * 1) lock timeout.  In this case, we set an error and return.
 * 2) unknown class.  In this case, the user has a semantic error.
 *    We need to continue with semantic analysis so that the proper
 *    error msg will be returned.  Thus WE DO NOT set an error for this case.
 */

PT_NODE *
pt_class_pre_fetch (PARSER_CONTEXT * parser, PT_NODE * statement)
{
  PT_CLASS_LOCKS lcks;
  lcks.classes = NULL;
  lcks.locks = NULL;

  /* we don't pre fetch for non query statements */
  if (statement == NULL)
    {
      return NULL;
    }

  switch (statement->node_type)
    {
    case PT_DELETE:
    case PT_INSERT:
    case PT_UPDATE:
    case PT_SELECT:
    case PT_UNION:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
      break;
    default:
      return statement;
    }

  lcks.num_classes = 0;

  /* pt_count_entities() will give us too large a count if a class is
   * mentioned more than once, but this will not hurt us. */
  (void) parser_walk_tree (parser, statement, pt_count_entities, &lcks.num_classes, NULL, NULL);

  if (lcks.num_classes == 0)
    {
      return statement;         /* caught in semantic check */
    }

  /* allocate the arrays */
  lcks.allocated_count = lcks.num_classes;
  lcks.classes = (char **) malloc ((lcks.num_classes + 1) * sizeof (char *));
  if (lcks.classes == NULL)
    {
      PT_ERRORmf (parser, statement, MSGCAT_SET_PARSER_RUNTIME,
                  MSGCAT_RUNTIME_OUT_OF_MEMORY, lcks.num_classes * sizeof (char *));
      goto cleanup;
    }

  lcks.locks = (LOCK *) malloc (lcks.num_classes * sizeof (LOCK));
  if (lcks.locks == NULL)
    {
      PT_ERRORmf (parser, statement, MSGCAT_SET_PARSER_RUNTIME,
                  MSGCAT_RUNTIME_OUT_OF_MEMORY, lcks.num_classes * sizeof (LOCK));
      goto cleanup;
    }

  memset (lcks.classes, 0, (lcks.num_classes + 1) * sizeof (char *));

  /* reset so parser_walk_tree can step through arrays */
  lcks.num_classes = 0;

  (void) parser_walk_tree (parser, statement, pt_find_lck_classes, &lcks, NULL, NULL);

  if (!pt_has_error (parser)
      && locator_lockhint_classes (lcks.num_classes,
                                   (const char **) lcks.classes, lcks.locks, true) != LC_CLASSNAME_EXIST)
    {
      PT_ERRORc (parser, statement, db_error_string (3));
    }

  /* free already assigned parser->lcks_classes if exist */
  parser_free_lcks_classes (parser);

  /* parser->lcks_classes will be freed at parser_free_parser() */
  parser->lcks_classes = lcks.classes;
  parser->num_lcks_classes = lcks.num_classes;
  lcks.classes = NULL;

cleanup:
  if (lcks.classes)
    {
      free_and_init (lcks.classes);
    }

  if (lcks.locks)
    {
      free_and_init (lcks.locks);
    }
  return statement;
}

/*
 * pt_count_entities () - If the node is an entity spec, bump counter
 *   return:
 *   parser(in):
 *   node(in): the node to check, leave node unchanged
 *   arg(out): count of entities
 *   continue_walk(in):
 */
static PT_NODE *
pt_count_entities (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * node, void *arg, UNUSED_ARG int *continue_walk)
{
  int *cnt = (int *) arg;

  if (node->node_type == PT_SPEC)
    {
      (*cnt)++;
    }

  return node;
}

/*
 * pt_add_lock_class () - add class locks in the prefetch structure
 *   return : error code or NO_ERROR
 *   parser (in)    : parser context
 *   lcks (in/out)  : pointer to PT_CLASS_LOCKS structure
 *   spec (in)	    : spec to add in locks list.
 */
int
pt_add_lock_class (PARSER_CONTEXT * parser, PT_CLASS_LOCKS * lcks, PT_NODE * spec)
{
  int len = 0;

  if (lcks->num_classes >= lcks->allocated_count)
    {
      /* Need to allocate more space in the locks array. Do not free locks
       * array if memory allocation fails, it will be freed by the caller
       * of this function */
      void *ptr = NULL;
      size_t new_size = lcks->allocated_count + 1;

      /* expand classes */
      ptr = realloc (lcks->classes, new_size * sizeof (char *));
      if (ptr == NULL)
        {
          PT_ERRORmf (parser, spec, MSGCAT_SET_PARSER_RUNTIME,
                      MSGCAT_RUNTIME_OUT_OF_MEMORY, new_size * sizeof (char *));
          return ER_FAILED;
        }
      lcks->classes = (char **) ptr;

      /* expand locks */
      ptr = realloc (lcks->locks, new_size * sizeof (LOCK));
      if (ptr == NULL)
        {
          PT_ERRORmf (parser, spec, MSGCAT_SET_PARSER_RUNTIME, MSGCAT_RUNTIME_OUT_OF_MEMORY, new_size * sizeof (LOCK));
          return ER_FAILED;
        }
      lcks->locks = (LOCK *) ptr;

      lcks->allocated_count++;
    }

  /* need to lowercase the class name so that the lock manager
   * can find it. */
  len = strlen (spec->info.spec.entity_name->info.name.original);
  /* parser->lcks_classes[n] will be freed at parser_free_parser() */
  lcks->classes[lcks->num_classes] = (char *) calloc (1, len + 1);
  if (lcks->classes[lcks->num_classes] == NULL)
    {
      PT_ERRORmf (parser, spec, MSGCAT_SET_PARSER_RUNTIME, MSGCAT_RUNTIME_OUT_OF_MEMORY, len + 1);
      return MSGCAT_RUNTIME_OUT_OF_MEMORY;
    }

  sm_downcase_name (spec->info.spec.entity_name->info.name.original, lcks->classes[lcks->num_classes], len + 1);

  lcks->locks[lcks->num_classes] = lcks->lock_type;
  lcks->num_classes++;

  return NO_ERROR;
}

/*
 * pt_find_lck_classes () - identifies classes and adds an unique entry in the
 *                          prefetch structure with the SCH-S lock mode
 *   return:
 *   parser(in):
 *   node(in): the node to check, returns node unchanged
 *   arg(in/out): pointer to PT_CLASS_LOCKS structure
 *   continue_walk(in):
 */
static PT_NODE *
pt_find_lck_classes (PARSER_CONTEXT * parser, PT_NODE * node, void *arg, int *continue_walk)
{
  PT_CLASS_LOCKS *lcks = (PT_CLASS_LOCKS *) arg;

  /* if its not an entity, there's nothing left to do */
  if (node->node_type != PT_SPEC)
    {
      /* if its not an entity, there's nothing left to do */
      return node;
    }

  /* check if this is a parenthesized entity list */
  if (node->info.spec.entity_name == NULL || node->info.spec.entity_name->node_type != PT_NAME)
    {
      return node;
    }

  lcks->lock_type = S_LOCK;

  /* only add to the array, if not there already in this lock mode. */
  if (!pt_in_lck_array (lcks, node->info.spec.entity_name->info.name.original))
    {
      if (pt_add_lock_class (parser, lcks, node) != NO_ERROR)
        {
          *continue_walk = PT_STOP_WALK;
          return node;
        }
    }

  return node;
}

/*
 * pt_in_lck_array () -
 *   return: true if string found in array with given lockmode, false otherwise
 *   lcks(in):
 *   str(in):
 */
static int
pt_in_lck_array (PT_CLASS_LOCKS * lcks, const char *str)
{
  int i;

  for (i = 0; i < lcks->num_classes; i++)
    {
      if (intl_identifier_casecmp (str, lcks->classes[i]) == 0 && lcks->locks[i] == lcks->lock_type)
        {
          return true;
        }
    }

  return false;                 /* not found */
}

/*
 * pt_name_occurs_in_from_list() - counts the number of times a name
 * appears as an exposed name in a list of entity_spec's
 *   return:
 *   parser(in):
 *   name(in):
 *   from_list(in):
 */
int
pt_name_occurs_in_from_list (UNUSED_ARG PARSER_CONTEXT * parser, const char *name, PT_NODE * from_list)
{
  PT_NODE *spec;
  int i = 0;

  if (!name || !from_list)
    {
      return i;
    }

  for (spec = from_list; spec != NULL; spec = spec->next)
    {
      if (spec->info.spec.range_var
          && spec->info.spec.range_var->info.name.original
          && (intl_identifier_casecmp (name, spec->info.spec.range_var->info.name.original) == 0))
        {
          i++;
        }
    }

  return i;
}
