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
 * parse_evaluate.c - Helper functions to interprets tree and
 * 	              returns its result as a DB_VALUE
 */

#ident "$Id$"

#include "config.h"

#include <assert.h>

#include "porting.h"
#include "error_manager.h"
#include "parser.h"
#include "cursor.h"
#include "memory_alloc.h"
#include "memory_hash.h"
#include "parser_message.h"
#include "execute_statement.h"
#include "object_domain.h"
#include "object_template.h"
#include "work_space.h"
#include "server_interface.h"
#include "arithmetic.h"
#include "parser_support.h"
#include "view_transform.h"
#include "network_interface_cl.h"
#include "xasl_support.h"
#include "transform.h"

/* this must be the last header file included!!! */
#include "dbval.h"

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * pt_get_one_tuple_from_list_id () -  return 1st tuple of a given list_id
 *   return:  1 if all OK, 0 otherwise
 *   parser(in): the parser context
 *   tree(in): a select/union/difference/intersection
 *   vals(out): an array of DB_VALUEs
 *   cnt(in): number of columns in the requested tuple
 */
int
pt_get_one_tuple_from_list_id (PARSER_CONTEXT * parser, PT_NODE * tree, DB_VALUE * vals, int cnt)
{
  QFILE_LIST_ID *list_id;
  CURSOR_ID cursor_id;
  int result = 0;
  PT_NODE *select_list;

  assert (parser != NULL);

  if (!tree
      || !vals || !(list_id = (QFILE_LIST_ID *) (tree->etc)) || !(select_list = pt_get_select_list (parser, tree)))
    {
      return result;
    }

#if 1                           /* TODO - */
  if (tree->info.query.oids_included)
    {
      assert (false);
      return result;
    }
#endif

  if (cursor_open (&cursor_id, list_id))
    {
      /* succesfully opened a cursor */
      cursor_id.query_id = parser->query_id;

      if (cursor_next_tuple (&cursor_id) != DB_CURSOR_SUCCESS
          || cursor_get_tuple_value_list (&cursor_id, cnt, vals) != NO_ERROR)
        {
          /*
           * This isn't really an error condition, especially when we are in an
           * esql context.  Just say that we didn't succeed, which should be
           * enough to keep upper levels from trying to do anything with the
           * result, but don't report an error.
           */
          result = 0;
        }
      else if (cursor_next_tuple (&cursor_id) == DB_CURSOR_SUCCESS)
        {
          char query_prefix[65], *p;

          p = parser_print_tree (parser, tree);
          if (p == NULL)
            {
              query_prefix[0] = '\0';
            }
          else
            {
              strncpy (query_prefix, p, 60);
              if (query_prefix[59])
                {
                  query_prefix[60] = '\0';
                  strncat (query_prefix, "...", 59);
                }
            }

          PT_ERRORmf (parser, select_list, MSGCAT_SET_PARSER_RUNTIME, MSGCAT_RUNTIME_YIELDS_GT_ONE_ROW, query_prefix);
        }
      else
        {
          result = 1;           /* all OK */
        }

      cursor_close (&cursor_id);
    }

  return result;
}
#endif

/*
 * pt_final () - deallocate all resources used by the parser
 *   return: none
 */
void
parser_final (void)
{
  pt_final_packing_buf ();
}

/*
 * pt_evaluate_def_val () - interprets tree & returns its result as a DB_VALUE
 *   return:  error_code
 *   parser(in): handle to the parser used to process & derive tree
 *   tree(in): an abstract syntax tree form of a Rye insert_value
 *   db_values(out): array of newly set DB_VALUEs if successful, untouched
 *		    otherwise
 */

int
pt_evaluate_def_val (PARSER_CONTEXT * parser, PT_NODE * tree, DB_VALUE * db_values)
{
  int error = NO_ERROR;
  DB_VALUE *val, opd1;

  assert (parser != NULL);

  if (tree == NULL || db_values == NULL || pt_has_error (parser))
    {
      return NO_ERROR;
    }

  if (tree->next != NULL || tree->or_next != NULL)
    {
      assert (false);           /* not permit */
      goto exit_on_error;
    }

  if (tree->node_type == PT_VALUE)
    {
      val = pt_value_to_db (parser, tree);
      if (val == NULL)
        {
          assert (false);
          goto exit_on_error;
        }

      (void) db_value_clone (val, db_values);
    }
  else if (tree->node_type == PT_EXPR)
    {
      switch (tree->info.expr.op)
        {
#if 1                           /* TODO - */
        case PT_UNIX_TIMESTAMP:

          if (tree->info.expr.arg1 != NULL)
            {
              goto exit_on_error;       /* not permit */
              break;
            }

          error = db_unix_timestamp (NULL, db_values);
          if (error < 0)
            {
              PT_ERRORc (parser, tree, er_msg ());
              goto exit_on_error;
            }

          break;
#endif

        case PT_SYS_DATETIME:
          {
            DB_DATETIME *tmp_datetime;

            db_value_domain_init (db_values, DB_TYPE_DATETIME, DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
            tmp_datetime = db_get_datetime (&parser->sys_datetime);

            db_make_datetime (db_values, tmp_datetime);
          }
          break;

#if 1                           /* TODO - */
        case PT_SYS_DATE:
          {
            DB_DATETIME *tmp_datetime;

            db_value_domain_init (db_values, DB_TYPE_DATE, DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);

            tmp_datetime = db_get_datetime (&parser->sys_datetime);

            db_value_put_encoded_date (db_values, &tmp_datetime->date);
          }
          break;

        case PT_CURRENT_USER:
          {
            char *username = au_user_name ();

            error = db_make_string (db_values, username);
            if (error < 0)
              {
                db_string_free (username);
                PT_ERRORc (parser, tree, er_msg ());
                goto exit_on_error;
              }

            db_values->need_clear = true;
          }
          break;

        case PT_USER:
          {
            char *user = NULL;

            user = db_get_user_and_host_name ();

            error = db_make_string (db_values, user);
            if (error < 0)
              {
                free_and_init (user);
                PT_ERRORc (parser, tree, er_msg ());
                goto exit_on_error;
              }

            db_values->need_clear = true;
          }
          break;
#endif

        case PT_UNARY_MINUS:
          assert (tree->info.expr.arg1 != NULL);
          assert (tree->info.expr.arg2 == NULL);
          assert (tree->info.expr.arg3 == NULL);

          /* evaluate operands */
          db_make_null (&opd1);
          error = pt_evaluate_def_val (parser, tree->info.expr.arg1, &opd1);
          if (error != NO_ERROR)
            {
              goto exit_on_error;
            }

          error = qdata_unary_minus_dbval (db_values, &opd1);
          if (error != NO_ERROR)
            {
              goto exit_on_error;
            }

          db_value_clear (&opd1);
          break;

        default:
          error = ER_FAILED;
          break;
        }
    }
  else
    {
      goto exit_on_error;
    }

  if (error != NO_ERROR)
    {
      goto exit_on_error;
    }

end:

  return error;

exit_on_error:

  if (!pt_has_error (parser))
    {
      PT_ERRORmf (parser, tree, MSGCAT_SET_PARSER_RUNTIME,
                  MSGCAT_RUNTIME__CAN_NOT_EVALUATE, pt_short_print (parser, tree));
    }

  if (error == NO_ERROR)
    {
      error = ER_FAILED;
    }

  goto end;
}
