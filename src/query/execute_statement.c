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
 * execute_statement.c - functions to do execute
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>             /* for getpid() */
#include <libgen.h>             /* for dirname, basename() */
#include <sys/time.h>           /* for struct timeval */
#include <string.h>
#include <limits.h>
#include <errno.h>
#include <stdarg.h>
#include <ctype.h>


#include "error_manager.h"
#include "db.h"
#include "dbi.h"
#include "dbdef.h"
#include "dbtype.h"
#include "parser.h"
#include "porting.h"
#include "schema_manager.h"
#include "transform.h"
#include "parser_message.h"
#include "system_parameter.h"
#include "execute_statement.h"

#include "semantic_check.h"
#include "execute_schema.h"
#include "server_interface.h"
#include "transaction_cl.h"
#include "object_print.h"
#include "optimizer.h"
#include "memory_alloc.h"
#include "object_domain.h"
#include "release_string.h"
#include "object_accessor.h"
#include "locator_cl.h"
#include "authenticate.h"
#include "xasl_generation.h"
#include "xasl_support.h"
#include "query_opfunc.h"
#include "environment_variable.h"
#include "set_object.h"
#include "intl_support.h"
#include "repl_log.h"
#include "view_transform.h"
#include "network_interface_cl.h"
#include "arithmetic.h"

/* this must be the last header file included!!! */
#include "dbval.h"

typedef int (PT_DO_FUNC) (DB_SESSION *, PT_NODE *);

/*
 * Function Group:
 * Do create/alter/drop serial statement
 *
 */

static void do_set_trace_to_query_flag (QUERY_FLAG * query_flag);

static int do_check_delete (DB_SESSION * session, PT_NODE * statement, PT_DO_FUNC * do_func);
static int do_check_update (DB_SESSION * session, PT_NODE * statement, PT_DO_FUNC * do_func);
static PT_NODE *do_check_names_for_insert_values_pre (PARSER_CONTEXT *
                                                      parser, PT_NODE * node, void *arg, int *continue_walk);
static int do_prepare_insert_internal (DB_SESSION * session, PT_NODE * statement);
static void init_compile_context (PARSER_CONTEXT * parser);
static int parser_set_shard_key (DB_SESSION * session, PT_NODE * statement);


/*
 * is_schema_repl_log_statment()
 *   return: true if it's a schema replications log statement
 *           otherwise false
 *   node(in):
 */
bool
is_schema_repl_log_statment (const PT_NODE * node)
{
  /* All DDLs will be replicated via schema replication */
  if (pt_is_ddl_statement (node))
    {
      return true;
    }

  return false;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * do_evaluate_default_expr() - evaluates the default expressions, if any, for
 *				the attributes of a given class
 *   return: Error code
 *   parser(in):
 *   class_name(in):
 */
int
do_evaluate_default_expr (PARSER_CONTEXT * parser, PT_NODE * class_name)
{
  SM_ATTRIBUTE *att;
  SM_CLASS *smclass;
  int error;
  TP_DOMAIN_STATUS status;
  char *user_name;
  DB_DATETIME *datetime;

  assert (class_name->node_type == PT_NAME);

  error = au_fetch_class_force (class_name->info.name.db_object, &smclass, S_LOCK);
  if (error != NO_ERROR)
    {
      return error;
    }

  for (att = smclass->attributes; att != NULL; att = att->next)
    {
      assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

      if (att->default_value.default_expr != DB_DEFAULT_NONE)
        {
          switch (att->default_value.default_expr)
            {
            case DB_DEFAULT_SYSDATE:
              if (DB_IS_NULL (&parser->sys_datetime))
                {
                  db_make_null (&att->default_value.value);
                }
              else
                {
                  datetime = DB_GET_DATETIME (&parser->sys_datetime);
                  error = db_value_put_encoded_date (&att->default_value.value, &datetime->date);
                }
              break;
            case DB_DEFAULT_SYSDATETIME:
              error = pr_clone_value (&parser->sys_datetime, &att->default_value.value);
              break;
            case DB_DEFAULT_UNIX_TIMESTAMP:
              error = db_unix_timestamp (&parser->sys_datetime, &att->default_value.value);
              break;
            case DB_DEFAULT_USER:
              user_name = db_get_user_and_host_name ();
              error = db_make_string (&att->default_value.value, user_name);
              att->default_value.value.need_clear = true;
              break;
            case DB_DEFAULT_CURR_USER:
              user_name = db_get_user_name ();
              error = DB_MAKE_STRING (&att->default_value.value, user_name);
              att->default_value.value.need_clear = true;
              break;
            default:
              break;
            }

          if (error != NO_ERROR)
            {
              return error;
            }
          /* make sure the default value can be used for this attribute */
          status = tp_value_coerce (&att->default_value.value, &att->default_value.value, att->sma_domain);
          if (status != DOMAIN_COMPATIBLE)
            {
              er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
                      pr_type_name (DB_VALUE_TYPE
                                    (&att->default_value.value)), pr_type_name (TP_DOMAIN_TYPE (att->sma_domain)));
              return ER_FAILED;
            }
        }
    }

  return NO_ERROR;
}
#endif

/*
 * Function Group:
 * Entry functions to do execute
 *
 */

#define ER_PT_UNKNOWN_STATEMENT ER_GENERIC_ERROR

/*
 * do_prepare_statement() - Prepare a given statement for execution
 *   return: Error code
 *   session(in) : contains the SQL query that has been compiled
 *   statement(in): Parse tree of a statement
 *
 * Note:
 * 	PREPARE includes query optimization and plan generation (XASL) for the SQL
 * 	statement. EXECUTE means requesting the server to execute the given XASL.
 *
 * 	Some type of statement is not necessary or not able to do PREPARE stage.
 * 	They can or must be EXECUTEd directly without PREPARE. For those types of
 * 	statements, this function will return NO_ERROR.
 */
int
do_prepare_statement (DB_SESSION * session, PT_NODE * statement)
{
  int err = NO_ERROR;
  PARSER_CONTEXT *parser;

  assert (session != NULL);
  assert (session->parser != NULL);

  parser = session->parser;

  init_compile_context (parser);

  switch (statement->node_type)
    {
    case PT_DELETE:
      err = do_prepare_delete (session, statement, NULL);
      break;
    case PT_INSERT:
      err = do_prepare_insert (session, statement);
      break;
    case PT_UPDATE:
      err = do_prepare_update (session, statement);
      break;
    case PT_SELECT:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
    case PT_UNION:
      err = do_prepare_select (session, statement);
      break;
    default:
      /* there are no actions for other types of statements */
      break;
    }

  return ((err == ER_FAILED && (err = er_errid ()) == NO_ERROR) ? ER_GENERIC_ERROR : err);
}                               /* do_prepare_statement() */

/*
 * do_execute_statement() - Execute a prepared statement
 *   return: Error code
 *   session(in) : contains the SQL query that has been compiled
 *   statement(in): Parse tree of a statement
 *
 * Note:
 * 	The statement should be PREPAREd before to EXECUTE. But, some type of
 * 	statement will be EXECUTEd directly without PREPARE stage because we can
 * 	decide the fact that they should be executed using query plan (XASL)
 * 	at the time of execution stage.
 */
int
do_execute_statement (DB_SESSION * session, PT_NODE * statement)
{
  int err = NO_ERROR;
  PARSER_CONTEXT *parser;
  bool need_schema_replication = false;
  int suppress_repl_error;

  assert (session != NULL);
  assert (session->parser != NULL);

  parser = session->parser;

  assert (parser->query_id == NULL_QUERY_ID);

  /* If it is an internally created statement,
     set its host variable info again to search host variables at parent parser */
  SET_HOST_VARIABLES_IF_INTERNAL_STATEMENT (parser);

  /* for the subset of nodes which represent top level statements,
     process them; for any other node, return an error */

  /* disable data replication log for schema replication log types in HA mode */
  if (is_schema_repl_log_statment (statement))
    {
      need_schema_replication = true;

      /* since we are going to suppress writing replication logs,
       * we need to flush all dirty objects to server not to lose them
       */
      err = locator_all_flush ();
      if (err != NO_ERROR)
        {
          goto end;
        }

      suppress_repl_error = db_set_suppress_repl_on_transaction (true);
      if (suppress_repl_error != NO_ERROR)
        {
          goto end;
        }
    }

  switch (statement->node_type)
    {
    case PT_CREATE_ENTITY:
      err = do_create_entity (session, statement);
      break;
    case PT_CREATE_INDEX:
      err = do_create_index (parser, statement);
      break;
    case PT_CREATE_USER:
      err = do_create_user (parser, statement);
      break;
    case PT_ALTER:
      err = do_alter (parser, statement);
      break;
    case PT_ALTER_INDEX:
      err = do_alter_index (parser, statement);
      break;
    case PT_ALTER_USER:
      err = do_alter_user (parser, statement);
      break;
    case PT_DROP:
      err = do_drop (parser, statement);
      break;
    case PT_DROP_INDEX:
      err = do_drop_index (parser, statement);
      break;
    case PT_DROP_USER:
      err = do_drop_user (parser, statement);
      break;
    case PT_RENAME:
      err = do_rename (parser, statement);
      break;
    case PT_GRANT:
      err = do_grant (parser, statement);
      break;
    case PT_REVOKE:
      err = do_revoke (parser, statement);
      break;
    case PT_GET_XACTION:
      err = do_get_xaction (parser, statement);
      break;
    case PT_SAVEPOINT:
      err = do_savepoint (parser, statement);
      break;
    case PT_COMMIT_WORK:
      err = do_commit (parser, statement);
      break;
    case PT_ROLLBACK_WORK:
      err = do_rollback (parser, statement);
      break;
    case PT_DELETE:
      err = do_check_delete (session, statement, do_execute_delete);
      break;
    case PT_INSERT:
      err = do_execute_insert (session, statement);
      break;
    case PT_UPDATE:
      err = do_check_update (session, statement, do_execute_update);
      break;
    case PT_SELECT:
    case PT_DIFFERENCE:
    case PT_INTERSECTION:
    case PT_UNION:
      err = do_execute_select (session, statement);
      break;
    case PT_UPDATE_STATS:
      err = do_update_stats (parser, statement);
      break;
    case PT_GET_OPT_LVL:
      err = do_get_optimization_param (parser, statement);
      break;
    case PT_SET_OPT_LVL:
      err = do_set_optimization_param (parser, statement);
      break;
    case PT_SET_SYS_PARAMS:
      err = do_set_sys_params (parser, statement);
      break;
    case PT_QUERY_TRACE:
      err = do_set_query_trace (parser, statement);
      break;
    default:
      err = ER_PT_UNKNOWN_STATEMENT;
      er_set (ER_ERROR_SEVERITY, __FILE__, statement->line_number, err, 1, statement->node_type);
      break;
    }

  /* enable data replication log */
  if (need_schema_replication)
    {
      /* before enable data replication log
       * we have to flush all dirty objects to server not to write
       * redundant data replication logs for DDLs */
      if (err == NO_ERROR)
        {
          err = locator_all_flush ();
        }

      suppress_repl_error = db_set_suppress_repl_on_transaction (false);
    }

  /* write schema replication log */
  if (err == NO_ERROR && need_schema_replication && suppress_repl_error == NO_ERROR)
    {
      err = do_replicate_schema (parser, statement);
    }

end:
  /* There may be parse tree fragments that were collected during the
     execution of the statement that should be freed now. */
  pt_free_orphans (parser);

  /* During query execution,
     if current transaction was rollbacked by the system,
     abort transaction on client side also. */
  if (err == ER_LK_UNILATERALLY_ABORTED)
    {
      (void) tran_abort_only_client (false);
    }

  RESET_HOST_VARIABLES_IF_INTERNAL_STATEMENT (parser);

  return ((err == ER_FAILED && (err = er_errid ()) == NO_ERROR) ? ER_GENERIC_ERROR : err);
}

/*
 * Function Group:
 * Parse tree to update statistics translation.
 *
 */


/*
 * do_update_stats() - Updates the statistics of a list of classes
 *		       or ALL classes
 *   return: Error code
 *   parser(in): Parser context
 *   statement(in/out): Parse tree of a update statistics statement
 */
int
do_update_stats (PARSER_CONTEXT * parser, PT_NODE * statement)
{
  PT_NODE *cls = NULL;
  int error = NO_ERROR;
  DB_OBJECT *obj;
  bool update_stats, with_fullscan;

  CHECK_MODIFICATION_ERROR ();

  assert (statement != NULL);
  assert (statement->node_type == PT_UPDATE_STATS);
  assert (statement->info.update_stats.update_stats == 0 || statement->info.update_stats.update_stats == 1);
  assert (statement->info.update_stats.with_fullscan == 0 || statement->info.update_stats.with_fullscan == 1);

  update_stats = statement->info.update_stats.update_stats ? true : false;
  with_fullscan = statement->info.update_stats.with_fullscan ? true : false;

  if (statement->info.update_stats.all_classes > 0)
    {
      error = sm_update_all_statistics (update_stats, with_fullscan);
    }
  else if (statement->info.update_stats.all_classes < 0)
    {
      error = sm_update_all_catalog_statistics (update_stats, with_fullscan);
    }
  else
    {
      for (cls = statement->info.update_stats.class_list; cls != NULL && error == NO_ERROR; cls = cls->next)
        {
          obj = sm_find_class (cls->info.name.original);
          if (obj)
            {
              cls->info.name.db_object = obj;
              pt_check_user_owns_class (parser, cls);
            }
          else
            {
              assert (er_errid () != NO_ERROR);
              return er_errid ();
            }

          error = sm_update_statistics (obj, update_stats, with_fullscan);
        }                       /* for (cls = ...) */
    }

  return error;
}


/*
 * Function Group:
 * DO functions for transaction management
 *
 */

#if defined (ENABLE_UNUSED_FUNCTION)
static int map_iso_levels (PARSER_CONTEXT * parser, PT_NODE * statement,
                           DB_TRAN_ISOLATION * tran_isolation, PT_NODE * node);
static int set_iso_level (PARSER_CONTEXT * parser,
                          DB_TRAN_ISOLATION * tran_isolation, PT_NODE * statement, const DB_VALUE * level);
static int check_timeout_value (PARSER_CONTEXT * parser, PT_NODE * statement, DB_VALUE * val);
#endif
static char *get_savepoint_name_from_db_value (DB_VALUE * val);


/*
 * do_commit() - Commit a transaction
 *   return: Error code
 *   parser(in): Parser context
 *   statement(in): Parse tree of a commit statement
 *
 * Note:
 */
int
do_commit (UNUSED_ARG PARSER_CONTEXT * parser, UNUSED_ARG PT_NODE * statement)
{
  return tran_commit ();
}

/*
 * do_rollback() - Rollbacks a transaction
 *   return: Error code
 *   parser(in): Parser context
 *   statement(in): Parse tree of a rollback statement (for regularity)
 *
 * Note: If a savepoint name is given, the transaction is rolled back to
 *   the savepoint, otherwise the entire transaction is rolled back.
 */
int
do_rollback (PARSER_CONTEXT * parser, PT_NODE * statement)
{
  int error = NO_ERROR;
  const char *save_name;
  PT_NODE *name;
  DB_VALUE *val = NULL;

  name = statement->info.rollback_work.save_name;
  if (name == NULL)
    {
      error = tran_abort ();
    }
  else
    {
      if (name->node_type == PT_NAME)
        {
          save_name = name->info.name.original;
          error = db_abort_to_savepoint_internal (save_name);
        }
      else
        {
#if 1                           /* TODO - */
          if (!PT_IS_CONST (name))
            {
              return ER_OBJ_INVALID_ARGUMENTS;
            }
#endif
          val = pt_value_to_db (parser, name);
          if (pt_has_error (parser))
            {
              return ER_GENERIC_ERROR;
            }
          save_name = get_savepoint_name_from_db_value (val);
          if (!save_name)
            {
              return er_errid ();
            }
          error = db_abort_to_savepoint_internal (save_name);
        }
    }

  return error;
}

/*
 * do_savepoint() - Creates a transaction savepoint
 *   return: Error code if savepoint fails
 *   parser(in): Parser context of a savepoint statement
 *   statement(in): Parse tree of a rollback statement (for regularity)
 *
 * Note: If a savepoint name is given, the savepoint is created
 *   with that name, if no savepoint name is given, we generate a unique one.
 */
int
do_savepoint (PARSER_CONTEXT * parser, PT_NODE * statement)
{
  int error = NO_ERROR;
  const char *save_name;
  PT_NODE *name;
  DB_VALUE *val = NULL;

  name = statement->info.savepoint.save_name;
  if (name == NULL)
    {
      PT_INTERNAL_ERROR (parser, "transactions");
    }
  else
    {
      if (name->node_type == PT_NAME)
        {
          save_name = name->info.name.original;
          error = db_savepoint_transaction_internal (save_name);
        }
      else
        {
#if 1                           /* TODO - */
          if (!PT_IS_CONST (name))
            {
              return ER_OBJ_INVALID_ARGUMENTS;
            }
#endif
          val = pt_value_to_db (parser, name);
          if (pt_has_error (parser))
            {
              return ER_GENERIC_ERROR;
            }
          save_name = get_savepoint_name_from_db_value (val);
          if (!save_name)
            {
              return er_errid ();
            }
          error = db_savepoint_transaction_internal (save_name);
        }
    }

  return error;
}

/*
 * do_get_xaction() - Gets the isolation level and/or timeout value for
 *      	      a transaction
 *   return: Error code if it fails
 *   parser(in): Parser context
 *   statement(in/out): Parse tree of a get transaction statement
 *
 * Note:
 */
int
do_get_xaction (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * statement)
{
  int lock_timeout_in_msecs = 0;
  DB_TRAN_ISOLATION tran_isolation = TRAN_UNKNOWN_ISOLATION;
  int tran_num;
  DB_VALUE *ins_val;
  int error = NO_ERROR;

  (void) tran_get_tran_settings (&lock_timeout_in_msecs, &tran_isolation);

  /* create a DB_VALUE to hold the result */
  ins_val = db_value_create ();
  if (ins_val == NULL)
    {
      return er_errid ();
    }

  db_make_int (ins_val, 0);

  switch (statement->info.get_xaction.option)
    {
    case PT_ISOLATION_LEVEL:
      tran_num = (int) tran_isolation;
      db_make_int (ins_val, tran_num);
      break;

    case PT_LOCK_TIMEOUT:
      if (lock_timeout_in_msecs > 0)
        {
          db_make_double (ins_val, (double) lock_timeout_in_msecs / 1000);
        }
      else
        {
          db_make_double (ins_val, (double) lock_timeout_in_msecs);
        }
      break;

    default:
      break;
    }

  statement->etc = (void *) ins_val;

  return error;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * do_set_xaction() - Sets the isolation level and/or timeout value for
 *      	      a transaction
 *   return: Error code if it fails
 *   parser(in): Parser context
 *   statement(in): Parse tree of a set transaction statement
 *
 * Note:
 */
int
do_set_xaction (PARSER_CONTEXT * parser, PT_NODE * statement)
{
  DB_TRAN_ISOLATION tran_isolation;
  DB_VALUE *val = NULL;
  PT_NODE *mode = statement->info.set_xaction.xaction_modes;
  int error = NO_ERROR;
  double wait_secs;

  while ((error == NO_ERROR) && (mode != NULL))
    {
      switch (mode->node_type)
        {
        case PT_ISOLATION_LVL:
          if (mode->info.isolation_lvl.level == NULL)
            {
              /* map schema/instance pair to level */
              error = map_iso_levels (parser, statement, &tran_isolation, mode);
            }
          else
            {
#if 1                           /* TODO - */
              if (!PT_IS_CONST (mode->info.isolation_lvl.level))
                {
                  return ER_OBJ_INVALID_ARGUMENTS;
                }
#endif
              val = pt_value_to_db (parser, mode->info.isolation_lvl.level);

              if (pt_has_error (parser))
                {
                  return ER_GENERIC_ERROR;
                }

#if 1                           /* TODO - need to cast as integer */
              if (DB_VALUE_TYPE (val) != DB_TYPE_INTEGER)
                {
                  assert (false);
                  ;             /* under construction */
                }
#endif

              error = set_iso_level (parser, &tran_isolation, statement, val);
            }

          if (error == NO_ERROR)
            {
              error = tran_reset_isolation (tran_isolation);
            }
          break;
        case PT_TIMEOUT:
#if 1                           /* TODO - */
          if (!PT_IS_CONST (mode->info.timeout.val))
            {
              return ER_OBJ_INVALID_ARGUMENTS;
            }
#endif
          val = pt_value_to_db (parser, mode->info.timeout.val);
          if (pt_has_error (parser))
            {
              return ER_GENERIC_ERROR;
            }

          if (check_timeout_value (parser, statement, val) != NO_ERROR)
            {
              return ER_GENERIC_ERROR;
            }
          else
            {
              wait_secs = DB_GET_DOUBLE (val);
              if (wait_secs > 0)
                {
                  wait_secs *= 1000;
                }
              (void) tran_reset_wait_times ((int) wait_secs);
            }
          break;
        default:
          return ER_GENERIC_ERROR;
        }

      mode = mode->next;
    }

  return error;
}
#endif

/*
 * do_get_optimization_level() - Determine the current optimization and
 *				 return it through the statement parameter.
 *   return: Error code if it fails
 *   parser(in): Parser context
 *   statement(in/out): Parse tree of a get transaction statement
 *
 * Note:
 */
int
do_get_optimization_param (PARSER_CONTEXT * parser, PT_NODE * statement)
{
  DB_VALUE *val = NULL;
  int error = NO_ERROR;

  val = db_value_create ();
  if (val == NULL)
    {
      return er_errid ();
    }

  switch (statement->info.get_opt_lvl.option)
    {
    case PT_OPT_LVL:
      {
        int i;

        qo_get_optimization_param (&i, QO_PARAM_LEVEL);
        db_make_int (val, i);
        break;
      }
    case PT_OPT_COST:
      {
        DB_VALUE *plan = NULL;
        char cost[2];

#if 1                           /* TODO - */
        if (!PT_IS_CONST (statement->info.get_opt_lvl.args))
          {
            db_value_free (val);
            return ER_OBJ_INVALID_ARGUMENTS;
          }
#endif

        plan = pt_value_to_db (parser, statement->info.get_opt_lvl.args);
        if (pt_has_error (parser))
          {
            db_value_free (val);
            return ER_OBJ_INVALID_ARGUMENTS;
          }

        qo_get_optimization_param (cost, QO_PARAM_COST, DB_GET_STRING (plan));
        db_make_string_copy (val, cost);
      }
    default:
      /*
       * Default ok; nothing else can get in here.
       */
      break;
    }

  statement->etc = (void *) val;

  return error;
}

/*
 * do_set_optimization_param() - Set the optimization level to the indicated
 *				 value and return the old value through the
 *				 statement paramter.
 *   return: Error code if it fails
 *   parser(in): Parser context
 *   statement(in): Parse tree of a set transaction statement
 *
 * Note:
 */
int
do_set_optimization_param (PARSER_CONTEXT * parser, PT_NODE * statement)
{
  PT_NODE *p1, *p2;
  DB_VALUE *val1 = NULL, *val2 = NULL;
  char *plan, *cost;

  p1 = statement->info.set_opt_lvl.val;

  if (p1 == NULL)
    {
      er_set (ER_ERROR_SEVERITY, __FILE__, __LINE__, ER_OBJ_INVALID_ARGUMENTS, 0);
      return ER_OBJ_INVALID_ARGUMENTS;
    }

#if 1                           /* TODO - */
  if (!PT_IS_CONST (p1))
    {
      return ER_OBJ_INVALID_ARGUMENTS;
    }
#endif
  val1 = pt_value_to_db (parser, p1);
  if (pt_has_error (parser))
    {
      return NO_ERROR;
    }

  switch (statement->info.set_opt_lvl.option)
    {
    case PT_OPT_LVL:
      if (DB_VALUE_TYPE (val1) == DB_TYPE_INTEGER)
        {
          qo_set_optimization_param (NULL, QO_PARAM_LEVEL, (int) DB_GET_INTEGER (val1));
        }
      else
        {
          er_set (ER_ERROR_SEVERITY, __FILE__, __LINE__, ER_OBJ_INVALID_ARGUMENTS, 0);
          return ER_OBJ_INVALID_ARGUMENTS;
        }
      break;
    case PT_OPT_COST:
      plan = DB_GET_STRING (val1);
      p2 = p1->next;
#if 1                           /* TODO - */
      if (!PT_IS_CONST (p2))
        {
          return ER_OBJ_INVALID_ARGUMENTS;
        }
#endif
      val2 = pt_value_to_db (parser, p2);
      if (pt_has_error (parser))
        {
          return ER_OBJ_INVALID_ARGUMENTS;
        }
      switch (DB_VALUE_TYPE (val2))
        {
        case DB_TYPE_INTEGER:
          qo_set_optimization_param (NULL, QO_PARAM_COST, plan, DB_GET_INT (val2));
          break;
        case DB_TYPE_VARCHAR:
          cost = DB_PULL_STRING (val2);
          qo_set_optimization_param (NULL, QO_PARAM_COST, plan, (int) cost[0]);
          break;
        default:
          er_set (ER_ERROR_SEVERITY, __FILE__, __LINE__, ER_OBJ_INVALID_ARGUMENTS, 0);
          return ER_OBJ_INVALID_ARGUMENTS;
        }
      break;
    default:
      /*
       * Default ok; no other options available.
       */
      break;
    }

  return NO_ERROR;
}

/*
 * do_set_sys_params() - Set the system parameters defined in 'rye-auto.conf'.
 *   return: Error code if it fails
 *   parser(in): Parser context
 *   statement(in): Parse tree of a set transaction statement
 *
 * Note:
 */
int
do_set_sys_params (PARSER_CONTEXT * parser, PT_NODE * statement)
{
  PT_NODE *val;
  DB_VALUE *db_val = NULL;
  bool persist = false;
  int error = NO_ERROR;

  val = statement->info.set_sys_params.val;
  if (val == NULL)
    {
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  if (statement->info.set_sys_params.persist == PT_PERSIST_ON)
    {
      persist = true;
    }

  while (val && error == NO_ERROR)
    {
#if 1                           /* TODO - */
      if (!PT_IS_CONST (val))
        {
          return ER_OBJ_INVALID_ARGUMENTS;
        }
#endif
      db_val = pt_value_to_db (parser, val);
      if (pt_has_error (parser))
        {
          error = ER_GENERIC_ERROR;
        }
      else
        {
          error = db_set_system_parameters (NULL, 0, DB_GET_STRING (db_val), persist);
        }

      val = val->next;
    }

  return error;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * map_iso_levels() - Maps the schema/instance isolation level to the
 *      	      DB_TRAN_ISOLATION enumerated type.
 *   return: Error code if it fails
 *   parser(in): Parser context
 *   tran_isolation(out):
 *   node(in): Parse tree of a set transaction statement
 *
 * Note: Initializes isolation_levels array
 */
static int
map_iso_levels (PARSER_CONTEXT * parser, PT_NODE * statement, DB_TRAN_ISOLATION * tran_isolation, PT_NODE * node)
{
  PT_MISC_TYPE instances = node->info.isolation_lvl.instances;
  PT_MISC_TYPE schema = node->info.isolation_lvl.schema;

  switch (schema)
    {
    case PT_SERIALIZABLE:
      PT_ERRORmf2 (parser, statement, MSGCAT_SET_PARSER_RUNTIME,
                   MSGCAT_RUNTIME_XACT_INVALID_ISO_LVL_MSG, pt_show_misc_type (schema), pt_show_misc_type (instances));
      return ER_GENERIC_ERROR;
    case PT_REPEATABLE_READ:
      if (instances == PT_READ_UNCOMMITTED)
        {
          *tran_isolation = TRAN_DEFAULT_ISOLATION;
        }
      else
        {
          PT_ERRORmf2 (parser, statement, MSGCAT_SET_PARSER_RUNTIME,
                       MSGCAT_RUNTIME_XACT_INVALID_ISO_LVL_MSG,
                       pt_show_misc_type (schema), pt_show_misc_type (instances));
          return ER_GENERIC_ERROR;
        }
      break;
    case PT_READ_COMMITTED:
      PT_ERRORmf2 (parser, statement, MSGCAT_SET_PARSER_RUNTIME,
                   MSGCAT_RUNTIME_XACT_INVALID_ISO_LVL_MSG, pt_show_misc_type (schema), pt_show_misc_type (instances));
      return ER_GENERIC_ERROR;
    case PT_READ_UNCOMMITTED:
      PT_ERRORmf2 (parser, statement, MSGCAT_SET_PARSER_RUNTIME,
                   MSGCAT_RUNTIME_XACT_INVALID_ISO_LVL_MSG, pt_show_misc_type (schema), pt_show_misc_type (instances));
      return ER_GENERIC_ERROR;
    default:
      return ER_GENERIC_ERROR;
    }

  return NO_ERROR;
}

/*
 * set_iso_level() -
 *   return: Error code if it fails
 *   parser(in): Parser context
 *   tran_isolation(out): Isolation level set as a side effect
 *   statement(in): Parse tree of a set transaction statement
 *   level(in):
 *
 * Note: Translates the user entered isolation level (1,2,3,4,5) into
 *       the enumerated type.
 */
static int
set_iso_level (PARSER_CONTEXT * parser, DB_TRAN_ISOLATION * tran_isolation, PT_NODE * statement, const DB_VALUE * level)
{
  int error = NO_ERROR;
  int isolvl = DB_GET_INTEGER (level);

  /* translate to the enumerated type */
  switch (isolvl)
    {
#if 0                           /* unused */
    case 1:
      *tran_isolation = TRAN_COMMIT_CLASS_UNCOMMIT_INSTANCE;
      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_RYE,
                                       MSGCAT_SET_PARSER_RUNTIME, MSGCAT_RUNTIME_ISO_LVL_SET_TO_MSG));
      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_RYE,
                                       MSGCAT_SET_PARSER_RUNTIME, MSGCAT_RUNTIME_READCOM_S_READUNC_I));
      break;
    case 2:
      *tran_isolation = TRAN_COMMIT_CLASS_COMMIT_INSTANCE;
      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_RYE,
                                       MSGCAT_SET_PARSER_RUNTIME, MSGCAT_RUNTIME_ISO_LVL_SET_TO_MSG));
      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_RYE,
                                       MSGCAT_SET_PARSER_RUNTIME, MSGCAT_RUNTIME_READCOM_S_READCOM_I));
      break;
#endif
    case 3:
      *tran_isolation = TRAN_DEFAULT_ISOLATION;
      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_RYE,
                                       MSGCAT_SET_PARSER_RUNTIME, MSGCAT_RUNTIME_ISO_LVL_SET_TO_MSG));
      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_RYE,
                                       MSGCAT_SET_PARSER_RUNTIME, MSGCAT_RUNTIME_REPREAD_S_READUNC_I));
      break;
#if 0                           /* unused */
    case 4:
      *tran_isolation = TRAN_REP_CLASS_COMMIT_INSTANCE;
      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_RYE,
                                       MSGCAT_SET_PARSER_RUNTIME, MSGCAT_RUNTIME_ISO_LVL_SET_TO_MSG));
      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_RYE,
                                       MSGCAT_SET_PARSER_RUNTIME, MSGCAT_RUNTIME_REPREAD_S_READCOM_I));
      break;
    case 5:
      *tran_isolation = TRAN_REP_CLASS_REP_INSTANCE;
      fprintf (stdout,
               msgcat_message (MSGCAT_CATALOG_RYE, MSGCAT_SET_PARSER_RUNTIME, MSGCAT_RUNTIME_ISO_LVL_SET_TO_MSG));
      fprintf (stdout,
               msgcat_message (MSGCAT_CATALOG_RYE, MSGCAT_SET_PARSER_RUNTIME, MSGCAT_RUNTIME_REPREAD_S_REPREAD_I));
      break;
    case 6:
      *tran_isolation = TRAN_SERIALIZABLE;
      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_RYE,
                                       MSGCAT_SET_PARSER_RUNTIME, MSGCAT_RUNTIME_ISO_LVL_SET_TO_MSG));
      fprintf (stdout, msgcat_message (MSGCAT_CATALOG_RYE,
                                       MSGCAT_SET_PARSER_RUNTIME, MSGCAT_RUNTIME_SERIAL_S_SERIAL_I));
      break;
#endif
    case 0:
      /* fall through */
    default:
      PT_ERRORm (parser, statement, MSGCAT_SET_PARSER_RUNTIME, MSGCAT_RUNTIME_XACT_ISO_LVL_MSG);
      error = ER_GENERIC_ERROR;
    }

  return error;
}

/*
 * check_timeout_value() -
 *   return: Error code if it fails
 *   parser(in): Parser context
 *   statement(in):
 *   val(in): DB_VALUE with the value to set
 *
 * Note: Checks the user entered isolation level. Valid values are:
 *                    -1 : Infinite
 *                     0 : Don't wait
 *                    >0 : Wait this number of seconds
 */
static int
check_timeout_value (PARSER_CONTEXT * parser, PT_NODE * statement, DB_VALUE * val)
{
  double timeout;

  if (tp_value_coerce (val, val, &tp_Double_domain) == DOMAIN_COMPATIBLE)
    {
      timeout = DB_GET_DOUBLE (val);
      if ((timeout == -1) || (timeout >= 0))
        {
          return NO_ERROR;
        }
    }
  PT_ERRORm (parser, statement, MSGCAT_SET_PARSER_RUNTIME, MSGCAT_RUNTIME_TIMEOUT_VALUE_MSG);
  return ER_GENERIC_ERROR;
}
#endif

/*
 * get_savepoint_name_from_db_value() -
 *   return: a NULL if the value doesn't properly describe the name
 *           of a savepoint.
 *   val(in):
 *
 * Note: Mutates the contents of val to hold a NULL terminated string
 *       holding a valid savepoint name.  If the value is already of
 *       type string, a NULL termination will be assumed since the
 *       name came from a parse tree.
 */
static char *
get_savepoint_name_from_db_value (DB_VALUE * val)
{
  if (DB_VALUE_TYPE (val) != DB_TYPE_VARCHAR)
    {
      if (tp_value_coerce (val, val, tp_domain_resolve_default (DB_TYPE_VARCHAR)) != DOMAIN_COMPATIBLE)
        {
          return (char *) NULL;
        }
    }

  return db_get_string (val);
}

/*
 * PARSE TREE MACROS
 *
 * arguments:
 *	statement: parser node
 *
 * returns/side-effects: non-zero
 *
 * description:
 *    These are used as shorthand for parse tree access.
 *    Given a statement node, they test for certain characteristics
 *    and return a boolean.
 */

/*
 * do_check_delete () -
 *   return: Error code
 *   session(in) : contains the SQL query that has been compiled
 *   statement(in): Parse tree of a statement
 *   do_func(in): Function to do
 *
 * Note: The function checks multi-node delete.
 */
static int
do_check_delete (DB_SESSION * session, PT_NODE * statement, PT_DO_FUNC * do_func)
{
  UNUSED_VAR PARSER_CONTEXT *parser;
  int affected_count, error = 0;

  assert (session != NULL);
  assert (session->parser != NULL);

  parser = session->parser;

  if (prm_get_bool_value (PRM_ID_BLOCK_NOWHERE_STATEMENT) && statement->info.delete_.search_cond == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_BLOCK_NOWHERE_STMT, 0);
      return ER_BLOCK_NOWHERE_STMT;
    }

  error = do_func (session, statement);

  /* if the statement that contains joins with conditions deletes no record then
   * we skip the deletion in the subsequent classes beacuse the original join
   * would have deleted no record */
  if (error <= NO_ERROR)
    {
      return error;
    }

  affected_count = error;

  return affected_count;
}

/*
 * do_check_update () -
 *   return: Error code
 *   session(in) : contains the SQL query that has been compiled
 *   statement(in): Parse tree of a statement
 */
static int
do_check_update (DB_SESSION * session, PT_NODE * statement, PT_DO_FUNC * do_func)
{
  int err;
  UNUSED_VAR PARSER_CONTEXT *parser;

  assert (session != NULL);
  assert (session->parser != NULL);

  parser = session->parser;

  if (prm_get_bool_value (PRM_ID_BLOCK_NOWHERE_STATEMENT) && statement->info.update.search_cond == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_BLOCK_NOWHERE_STMT, 0);
      return ER_BLOCK_NOWHERE_STMT;
    }

  err = do_func (session, statement);

  return err;
}

/*
 * Function Group:
 * DO functions for update statements
 *
 */

typedef enum
{ NORMAL_UPDATE, UPDATE_OBJECT, ON_DUPLICATE_KEY_UPDATE } UPDATE_TYPE;

#define DB_VALUE_STACK_MAX 40

#if defined (ENABLE_UNUSED_FUNCTION)
static void unlink_list (PT_NODE * list);
#endif

static int update_check_for_constraints (PARSER_CONTEXT * parser,
                                         int *has_unique, PT_NODE ** not_nulls, const PT_NODE * statement);
static int is_server_update_allowed (PARSER_CONTEXT * parser,
                                     PT_NODE ** non_null_attrs,
                                     int *has_uniques, int *const server_allowed, const PT_NODE * statement);
#if defined (ENABLE_UNUSED_FUNCTION)
static int has_unique_constraint (DB_OBJECT * mop);
#endif

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * unlink_list - Unlinks next pointer shortcut of lhs, rhs assignments
 *   return: None
 *   list(in): Node list to cut
 *
 * Note:
 */
static void
unlink_list (PT_NODE * list)
{
  PT_NODE *next;

  while (list)
    {
      next = list->next;
      list->next = NULL;
      list = next;
    }
}
#endif

/*
 * init_compile_context() -
 *
 *   parser (in/out):
 *
 */
static void
init_compile_context (PARSER_CONTEXT * parser)
{
  memset (&parser->context, 0x00, sizeof (COMPILE_CONTEXT));
}

/*
 * init_xasl_stream() - init XASL_STREAM
 *
 *   stream (in): initialized parameter
 *
 */
static void
init_xasl_stream (XASL_STREAM * stream)
{
  memset (stream, 0x00, sizeof (XASL_STREAM));
}

/*
 * update_check_for_constraints - Determine whether attributes of the target
 *				  classes have UNIQUE and/or NOT NULL
 *				  constraints, and return a list of NOT NULL
 *				  attributes if exist
 *   return: Error code
 *   parser(in): Parser context
 *   has_unique(out): Indicator representing there is UNIQUE constraint, 1 or 0
 *   not_nulls(out): A list of pointers to NOT NULL attributes, or NULL
 *   statement(in):  Parse tree of an UPDATE or MERGE statement
 *
 * Note:
 */
static int
update_check_for_constraints (PARSER_CONTEXT * parser, int *has_unique, PT_NODE ** not_nulls, const PT_NODE * statement)
{
  int error = NO_ERROR;
  PT_NODE *lhs = NULL, *att = NULL, *pointer = NULL, *spec = NULL;
  PT_NODE *assignment;
  DB_OBJECT *class_obj = NULL;

  assignment = statement->info.update.assignment;

  *has_unique = 0;
  *not_nulls = NULL;

  for (; assignment; assignment = assignment->next)
    {
      lhs = assignment->info.expr.arg1;
      if (lhs->node_type == PT_NAME)
        {
          att = lhs;
        }
      else
        {
          /* bullet proofing, should not get here */
#if defined(RYE_DEBUG)
          fprintf (stdout, "system error detected in %s, line %d.\n", __FILE__, __LINE__);
#endif
          error = ER_GENERIC_ERROR;
          goto exit_on_error;
        }

      for (; att; att = att->next)
        {
          if (att->node_type != PT_NAME)
            {
              /* bullet proofing, should not get here */
#if defined(RYE_DEBUG)
              fprintf (stdout, "system error detected in %s, line %d.\n", __FILE__, __LINE__);
#endif
              error = ER_GENERIC_ERROR;
              goto exit_on_error;
            }

          spec = pt_find_spec_in_statement (parser, statement, att);
          if (spec == NULL || (class_obj = spec->info.spec.flat_entity_list->info.name.db_object) == NULL)
            {
              error = ER_GENERIC_ERROR;
              goto exit_on_error;
            }

          if (*has_unique == 0 && sm_att_unique_constrained (class_obj, att->info.name.original))
            {
              *has_unique = 1;
              spec->info.spec.flag |= PT_SPEC_FLAG_HAS_UNIQUE;
            }
          if (sm_att_constrained (class_obj, att->info.name.original, SM_ATTFLAG_NON_NULL))
            {
              pointer = pt_point (parser, att);
              if (pointer == NULL)
                {
                  PT_ERRORm (parser, att, MSGCAT_SET_PARSER_RUNTIME, MSGCAT_RUNTIME_RESOURCES_EXHAUSTED);
                  error = MSGCAT_RUNTIME_RESOURCES_EXHAUSTED;
                  goto exit_on_error;
                }
              *not_nulls = parser_append_node (pointer, *not_nulls);
            }
        }                       /* for ( ; attr; ...) */
    }                           /* for ( ; assignment; ...) */

  return NO_ERROR;

exit_on_error:
  if (*not_nulls)
    {
      parser_free_tree (parser, *not_nulls);
      *not_nulls = NULL;
    }
  return error;
}

/*
 * is_server_update_allowed() - Checks to see if a server-side update is
 *                              allowed
 *   return: NO_ERROR or error code on failure
 *   parser(in): Parser context
 *   non_null_attrs(in/out): Parse tree for attributes with the NOT NULL
 *                           constraint
 *   has_uniques(in/out): whether unique indexes are affected by the update
 *   server_allowed(in/out): whether the update can be executed on the server
 *   statement(in): Parse tree of an update statement
 */
static int
is_server_update_allowed (PARSER_CONTEXT * parser, PT_NODE ** non_null_attrs,
                          int *has_uniques, int *const server_allowed, const PT_NODE * statement)
{
  int error = NO_ERROR;
  int is_virt = 0;
  PT_NODE *spec = NULL;
  int save_au;

  assert (non_null_attrs != NULL);
  assert (*non_null_attrs == NULL);
  assert (has_uniques != NULL);
  assert (server_allowed != NULL);

  *has_uniques = 0;
  *server_allowed = 0;

  AU_DISABLE (save_au);

  /* check if at least one spec that will be updated is virtual */
  for (spec = statement->info.update.spec; spec && !is_virt; spec = spec->next)
    {
      if (!(spec->info.spec.flag & PT_SPEC_FLAG_UPDATE))
        {
          spec = spec->next;
          continue;
        }

      is_virt = (spec->info.spec.flat_entity_list->info.name.virt_object != NULL);
    }

  error = update_check_for_constraints (parser, has_uniques, non_null_attrs, statement);
  if (error < NO_ERROR)
    {
      goto error_exit;
    }

  /* Check to see if the update can be done on the server */
  *server_allowed = (!is_virt);
  assert (*server_allowed == 1);

  AU_ENABLE (save_au);

  return error;

error_exit:
  if (non_null_attrs != NULL && *non_null_attrs != NULL)
    {
      parser_free_tree (parser, *non_null_attrs);
      *non_null_attrs = NULL;
    }
  AU_ENABLE (save_au);

  return error;
}

/*
 * do_prepare_update() - Prepare the UPDATE statement
 *   return: Error code
 *   session(in) : contains the SQL query that has been compiled
 *   statement(in/out): Parse tree of a update statement
 *
 * Note:
 */
int
do_prepare_update (DB_SESSION * session, PT_NODE * statement)
{
  int err;
  PARSER_CONTEXT *parser;
  PT_NODE *flat, *not_nulls, *spec = NULL;
  int has_unique, au_save, has_virt = 0;
  bool server_update;

  assert (session != NULL);
  assert (session->parser != NULL);

  parser = session->parser;

  if (parser == NULL || statement == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  assert (statement == NULL || statement->next == NULL);

  for (err = NO_ERROR; statement && (err >= NO_ERROR); statement = statement->next)
    {
      COMPILE_CONTEXT *contextp;
      XASL_STREAM stream;

      contextp = &parser->context;

      init_xasl_stream (&stream);

      contextp->sql_user_text = statement->sql_user_text;
      contextp->sql_user_text_len = statement->sql_user_text_len;

      /* if already prepared */
      if (statement->xasl_id)
        {
          continue;             /* continue to next UPDATE statement */
        }

      AU_SAVE_AND_DISABLE (au_save);    /* TODO - calls au_fetch_class() */

      /* check if at least one spec to be updated is virtual */
      for (spec = statement->info.update.spec; spec && err == NO_ERROR; spec = spec->next)
        {
          if (spec->info.spec.flag & PT_SPEC_FLAG_UPDATE)
            {
              flat = spec->info.spec.flat_entity_list;
              assert (flat->next == NULL);

              if (!has_virt)
                {
                  has_virt = (flat->info.name.virt_object != NULL);
                }
            }
        }

      AU_RESTORE (au_save);

      if (err != NO_ERROR)
        {
          PT_INTERNAL_ERROR (parser, "update");
          break;                /* stop while loop if error */
        }

      /* make shard key info */
      err = parser_set_shard_key (session, statement);
      if (err != NO_ERROR)
        {
          pt_record_error (parser, statement->line_number, statement->column_number, er_msg (), NULL);
          break;
        }

      /* check if the target class has UNIQUE constraint and
         get attributes that has NOT NULL constraint */
      err = update_check_for_constraints (parser, &has_unique, &not_nulls, statement);
      if (err < NO_ERROR)
        {
          PT_INTERNAL_ERROR (parser, "update");
          break;                /* stop while loop if error */
        }

      statement->info.update.has_unique = (bool) has_unique;

      /* determine whether it can be server-side or OID list update */
      server_update = (!has_virt);
      if (server_update == false)
        {
          assert (false);
          PT_INTERNAL_ERROR (parser, "update");
          err = er_errid ();
          break;                /* stop while loop if error */
        }

      /*
       * Server-side update case: (by requesting server to execute XASL)
       *  build UPDATE_PROC XASL
       */

      /* make query string */
      parser->print_type_ambiguity = 0;
      PT_NODE_PRINT_TO_ALIAS (parser, statement, (PT_CONVERT_RANGE | PT_PRINT_QUOTES));
      contextp->sql_hash_text = statement->alias_print;
      if (parser->print_type_ambiguity)
        {
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_PREPARE_TOO_LONG_BAD_VALUE, 0);
          return ER_PREPARE_TOO_LONG_BAD_VALUE;
        }

      stream.xasl_id = NULL;

      /*
       * Server-side update case: (by requesting server to execute XASL)
       *  build UPDATE_PROC XASL
       */

      /* look up server's XASL cache for this query string
         and get XASL file id (XASL_ID) returned if found */
      if (statement->recompile == 0)
        {
          err = prepare_query (contextp, &stream);

          if (err != NO_ERROR)
            {
              err = er_errid ();
            }
        }
      else
        {
          err = qmgr_drop_query_plan (contextp->sql_hash_text, ws_identifier (db_get_user ()), NULL);
        }

      if (stream.xasl_id == NULL && err == NO_ERROR)
        {
          /* cache not found;
             make XASL from the parse tree including query optimization
             and plan generation */

          /* mark the beginning of another level of xasl packing */
          pt_enter_packing_buf ();

          /* this prevents authorization checking during generating XASL */
          AU_SAVE_AND_DISABLE (au_save);

          /* pt_to_update_xasl() will build XASL tree from parse tree */
          contextp->xasl = pt_to_update_xasl (session, statement, &not_nulls);
          AU_RESTORE (au_save);

          if (contextp->xasl && (err >= NO_ERROR))
            {
              /* convert the created XASL tree to the byte stream for
                 transmission to the server */
              err = xts_map_xasl_to_stream (contextp->xasl, &stream);
              if (err != NO_ERROR)
                {
                  PT_ERRORm (parser, statement, MSGCAT_SET_PARSER_RUNTIME, MSGCAT_RUNTIME_RESOURCES_EXHAUSTED);
                }
            }
          else
            {
              err = er_errid ();
              pt_record_error (parser, statement->line_number, statement->column_number, er_msg (), NULL);
            }

          /* request the server to prepare the query;
             give XASL stream generated from the parse tree
             and get XASL file id returned */
          if (stream.xasl_stream && (err >= NO_ERROR))
            {
              err = prepare_query (contextp, &stream);

              if (err != NO_ERROR)
                {
                  err = er_errid ();
                }
            }

          /* mark the end of another level of xasl packing */
          pt_exit_packing_buf ();

          /* As a result of query preparation of the server,
             the XASL cache for this query will be created or updated. */

          /* free 'stream' that is allocated inside of
             xts_map_xasl_to_stream() */
          if (stream.xasl_stream)
            {
              free_and_init (stream.xasl_stream);
            }
          statement->use_plan_cache = 0;
        }
      else
        {                       /* if (!xasl_id) */
          spec = statement->info.update.spec;
          while (spec && err == NO_ERROR)
            {
              flat = spec->info.spec.flat_entity_list;

              while (flat)
                {
                  assert (flat->next == NULL);
                  err = locator_flush_class (flat->info.name.db_object);
                  if (err != NO_ERROR)
                    {
                      stream.xasl_id = NULL;
                      break;
                    }
                  flat = flat->next;
                }
              spec = spec->next;
            }
          if (err == NO_ERROR)
            {
              statement->use_plan_cache = 1;
            }
          else
            {
              statement->use_plan_cache = 0;
            }
        }

      /* save the XASL_ID that is allocated and returned by
         prepare_query() into 'statement->xasl_id'
         to be used by do_execute_update() */
      statement->xasl_id = stream.xasl_id;

      if (not_nulls)
        {
          parser_free_tree (parser, not_nulls);
        }
    }

  return err;
}

/*
 * do_execute_update() - Execute the prepared UPDATE statement
 *   return: Error code
 *   session(in) : contains the SQL query that has been compiled
 *   statement(in): Parse tree of a update statement
 *
 * Note:
 */
int
do_execute_update (DB_SESSION * session, PT_NODE * statement)
{
  int err, result = 0;
  PARSER_CONTEXT *parser;
  QFILE_LIST_ID *list_id;
  int au_save;
  QUERY_ID query_id_self;

  assert (session != NULL);
  assert (session->parser != NULL);

  parser = session->parser;

  assert (parser->query_id == NULL_QUERY_ID);
  query_id_self = parser->query_id;

  CHECK_MODIFICATION_ERROR ();

  assert (statement == NULL || statement->next == NULL);
  for (err = NO_ERROR, result = 0; statement && (err >= NO_ERROR); statement = statement->next)
    {
      /* check if it is not necessary to execute this statement,
         e.g. false where or not prepared correctly */
      if (statement->xasl_id == NULL)
        {
          assert (false);       /* TODO - trace */
          statement->etc = NULL;
          err = NO_ERROR;
          continue;             /* continue to next UPDATE statement */
        }

      /* Request that the server executes the stored XASL, which is
         the execution plan of the prepared query, with the host variables
         given by users as parameter values for the query.
         As a result, query id and result file id (QFILE_LIST_ID) will be
         returned.
         do_prepare_update() has saved the XASL file id (XASL_ID) in
         'statement->xasl_id' */

      QUERY_FLAG query_flag = NOT_FROM_RESULT_CACHE | RESULT_CACHE_INHIBITED;

      if (prm_get_bool_value (PRM_ID_QUERY_TRACE) == true && parser->query_trace == true)
        {
          do_set_trace_to_query_flag (&query_flag);
        }

      AU_SAVE_AND_ENABLE (au_save);     /* this insures authorization
                                           checking for method */
      assert (parser->query_id == NULL_QUERY_ID);
      list_id = NULL;

      err = execute_query (session, statement, &list_id, query_flag);
      AU_RESTORE (au_save);
      if (err != NO_ERROR)
        {
          break;                /* stop while loop if error */
        }

      /* free returned QFILE_LIST_ID */
      if (list_id)
        {
          if (err >= NO_ERROR)
            {
              err = list_id->tuple_cnt; /* as a result */
            }
          regu_free_listid (list_id);
        }

      /* end the query; reset query_id and call qmgr_end_query() */
      pt_end_query (parser, query_id_self, statement);

      /* accumulate intermediate results */
      if (err >= NO_ERROR)
        {
          result += err;
        }

      if ((err < NO_ERROR) && er_errid () != NO_ERROR)
        {
          pt_record_error (parser, statement->line_number, statement->column_number, er_msg (), NULL);
        }
    }

  return (err < NO_ERROR) ? err : result;
}


/*
 * Function Group:
 * DO functions for delete statements
 *
 */

#if defined (ENABLE_UNUSED_FUNCTION)
/* used to generate unique savepoint names */
static int delete_savepoint_number = 0;

/*
 * has_unique_constraint() - Check if class has an unique constraint
 *   return: 1 if the class has an unique constraint, otherwise 0
 *   mop(in/out): Class object to be checked
 */
static int
has_unique_constraint (DB_OBJECT * mop)
{
  DB_CONSTRAINT *constraint_list, *c;
  SM_CONSTRAINT_TYPE ctype;

  if (mop == NULL)
    {
      return 0;
    }

  constraint_list = db_get_constraints (mop);
  for (c = constraint_list; c; c = c->next)
    {
      ctype = c->type;
      if (SM_IS_CONSTRAINT_UNIQUE_FAMILY (ctype))
        {
          return 1;
        }
    }

  return 0;
}
#endif

/*
 * do_prepare_delete() - Prepare the DELETE statement
 *   return: Error code
 *   session(in) : contains the SQL query that has been compiled
 *   statement(in/out): Delete statement
 *   parent(in): Parent statement if using multi-delete list
 */
int
do_prepare_delete (DB_SESSION * session, PT_NODE * statement, UNUSED_ARG PT_NODE * parent)
{
  int err;
  PARSER_CONTEXT *parser;
  PT_NODE *flat;
//  DB_OBJECT *class_obj;
  int au_save;
  bool has_virt_object;
  PT_NODE *node = NULL;
  PT_NODE *save_stmt;

  assert (session != NULL);
  assert (session->parser != NULL);

  parser = session->parser;

  if (parser == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  assert (statement == NULL || statement->next == NULL);

  save_stmt = statement;

  for (err = NO_ERROR; statement && (err >= NO_ERROR); statement = statement->next)
    {
      COMPILE_CONTEXT *contextp;
      XASL_STREAM stream;

      contextp = &parser->context;

      init_xasl_stream (&stream);

      contextp->sql_user_text = statement->sql_user_text;
      contextp->sql_user_text_len = statement->sql_user_text_len;

      assert (statement->info.delete_.spec != NULL);

      /* if already prepared */
      if (statement->xasl_id)
        {
          continue;             /* continue to next DELETE statement */
        }

      AU_SAVE_AND_DISABLE (au_save);    /* TODO - calls au_fetch_class() */

      has_virt_object = false;
      node = (PT_NODE *) statement->info.delete_.spec;
      while (node && err == NO_ERROR)
        {
          if (node->info.spec.flag & PT_SPEC_FLAG_DELETE)
            {
              flat = node->info.spec.flat_entity_list;
              assert (flat->next == NULL);

              if (flat)
                {
                  if (flat->info.name.virt_object)
                    {
                      has_virt_object = true;
                    }
//                class_obj = flat->info.name.db_object;
                }
              else
                {
//                class_obj = NULL;
                }
            }

          node = node->next;
        }

      AU_RESTORE (au_save);

      if (err != NO_ERROR)
        {
          PT_INTERNAL_ERROR (parser, "delete");
          break;                /* stop while loop if error */
        }

      /* make shard key info */
      err = parser_set_shard_key (session, statement);
      if (err != NO_ERROR)
        {
          pt_record_error (parser, statement->line_number, statement->column_number, er_msg (), NULL);
          break;
        }

      if (has_virt_object == true)
        {
          assert (false);
          PT_INTERNAL_ERROR (parser, "delete");
          err = er_errid ();
          break;                /* stop while loop if error */
        }

      stream.xasl_id = NULL;

      /* Server-side deletion case: (by requesting server to execute XASL)
         build DELETE_PROC XASL */

      /* make query string */
      parser->print_type_ambiguity = 0;
      PT_NODE_PRINT_TO_ALIAS (parser, statement, (PT_CONVERT_RANGE | PT_PRINT_QUOTES));
      contextp->sql_hash_text = statement->alias_print;
      if (parser->print_type_ambiguity)
        {
          err = ER_PREPARE_TOO_LONG_BAD_VALUE;
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, err, 0);
          break;
        }

      /* look up server's XASL cache for this query string
         and get XASL file id (XASL_ID) returned if found */
      if (statement->recompile == 0)
        {
          err = prepare_query (contextp, &stream);
          if (err != NO_ERROR)
            {
              err = er_errid ();
            }
        }
      else
        {
          err = qmgr_drop_query_plan (contextp->sql_hash_text, ws_identifier (db_get_user ()), NULL);
        }

      if (stream.xasl_id == NULL && err == NO_ERROR)
        {
          /* cache not found;
             make XASL from the parse tree including query optimization
             and plan generation */

          /* mark the beginning of another level of xasl packing */
          pt_enter_packing_buf ();

          /* this prevents authorization checking during generating XASL */
          AU_SAVE_AND_DISABLE (au_save);

          /* pt_to_delete_xasl() will build XASL tree from parse tree */
          contextp->xasl = pt_to_delete_xasl (session, statement);
          AU_RESTORE (au_save);

          if (contextp->xasl && (err >= NO_ERROR))
            {
              /* convert the created XASL tree to the byte stream for
                 transmission to the server */
              err = xts_map_xasl_to_stream (contextp->xasl, &stream);
              if (err != NO_ERROR)
                {
                  PT_ERRORm (parser, statement, MSGCAT_SET_PARSER_RUNTIME, MSGCAT_RUNTIME_RESOURCES_EXHAUSTED);
                }
            }
          else
            {
              err = er_errid ();
              pt_record_error (parser, statement->line_number, statement->column_number, er_msg (), NULL);
            }

          /* request the server to prepare the query;
             give XASL stream generated from the parse tree
             and get XASL file id returned */
          if (stream.xasl_stream && (err >= NO_ERROR))
            {
              err = prepare_query (contextp, &stream);
              if (err != NO_ERROR)
                {
                  err = er_errid ();
                }
            }

          /* mark the end of another level of xasl packing */
          pt_exit_packing_buf ();

          /* As a result of query preparation of the server,
             the XASL cache for this query will be created or updated. */

          /* free 'stream' that is allocated inside of
             xts_map_xasl_to_stream() */
          if (stream.xasl_stream)
            {
              free_and_init (stream.xasl_stream);
            }
          statement->use_plan_cache = 0;
        }
      else
        {
          if (err == NO_ERROR)
            {
              statement->use_plan_cache = 1;
            }
          else
            {
              statement->use_plan_cache = 0;
            }
        }

      /* save the XASL_ID that is allocated and returned by
         prepare_query() into 'statement->xasl_id'
         to be used by do_execute_delete() */
      statement->xasl_id = stream.xasl_id;
    }

  /* if something failed, clear all statement->xasl_id */
  if (err != NO_ERROR)
    {
      for (node = save_stmt; node != statement; node = node->next)
        {
          pt_free_statement_xasl_id (node);
        }
    }

  return err;
}

/*
 * do_execute_delete() - Execute the prepared DELETE statement
 *   return: Tuple count if success, otherwise an error code
 *   session(in) : contains the SQL query that has been compiled
 *   statement(in): Delete statement
 */
int
do_execute_delete (DB_SESSION * session, PT_NODE * statement)
{
  int err, result = 0;
  PARSER_CONTEXT *parser;
  PT_NODE *flat, *node;
  DB_OBJECT *class_obj;
  QFILE_LIST_ID *list_id;
  int au_save;
  QUERY_FLAG query_flag;
  QUERY_ID query_id_self;

  assert (session != NULL);
  assert (session->parser != NULL);

  parser = session->parser;

  assert (parser->query_id == NULL_QUERY_ID);
  query_id_self = parser->query_id;

  CHECK_MODIFICATION_ERROR ();

  assert (statement == NULL || statement->next == NULL);
  for (err = NO_ERROR, result = 0; statement && (err >= NO_ERROR); statement = statement->next)
    {
      /* check if it is not necessary to execute this statement,
         e.g. false where or not prepared correctly */
      if (statement->xasl_id == NULL)
        {
          assert (false);       /* TODO - trace */
          statement->etc = NULL;
          err = NO_ERROR;
          continue;             /* continue to next DELETE statement */
        }

      /* Request that the server executes the stored XASL, which is
         the execution plan of the prepared query, with the host variables
         given by users as parameter values for the query.
         As a result, query id and result file id (QFILE_LIST_ID) will be
         returned.
         do_prepare_delete() has saved the XASL file id (XASL_ID) in
         'statement->xasl_id' */
      query_flag = NOT_FROM_RESULT_CACHE | RESULT_CACHE_INHIBITED;

      if (prm_get_bool_value (PRM_ID_QUERY_TRACE) == true && parser->query_trace == true)
        {
          do_set_trace_to_query_flag (&query_flag);
        }

      AU_SAVE_AND_ENABLE (au_save);     /* this insures authorization
                                           checking for method */
      assert (parser->query_id == NULL_QUERY_ID);
      list_id = NULL;
      err = execute_query (session, statement, &list_id, query_flag);

      AU_RESTORE (au_save);

      if ((err >= NO_ERROR) && list_id)
        {
          err = list_id->tuple_cnt;
        }

      /* free returned QFILE_LIST_ID */
      if (list_id)
        {
          if (list_id->tuple_cnt > 0)
            {
              int err2 = NO_ERROR;

              node = statement->info.delete_.spec;

              while (node && err2 >= NO_ERROR)
                {
                  if (node->info.spec.flag & PT_SPEC_FLAG_DELETE)
                    {
                      flat = node->info.spec.flat_entity_list;
                      assert (flat->next == NULL);

                      class_obj = (flat) ? flat->info.name.db_object : NULL;

#if 1                           /* TODO - trace */
                      err2 = sm_flush_and_decache_objects (class_obj, DONT_DECACHE);
#else
                      err2 = sm_flush_and_decache_objects (class_obj, DECACHE);
#endif
                    }

                  node = node->next;
                }
              if (err2 != NO_ERROR)
                {
                  err = err2;
                }
            }
          regu_free_listid (list_id);
        }

      /* end the query; reset query_id and call qmgr_end_query() */
      pt_end_query (parser, query_id_self, statement);

      /* accumulate intermediate results */
      if (err >= NO_ERROR)
        {
          result += err;
        }

    }

  return (err < NO_ERROR) ? err : result;
}

/*
 * Function Group:
 * DO functions for insert statements
 *
 */

#if defined(ENABLE_UNUSED_FUNCTION)
typedef enum
{
  INSERT_SELECT = 1,
  INSERT_VALUES = 2,
  /*
   * NOT USED ANY MORE.
   * prm_insert_mode_upper is still left as 31 for backward compatibility.
   *
   */
  INSERT_DEFAULT = 4,
  INSERT_REPLACE = 8,
  INSERT_ON_DUP_KEY_UPDATE = 16
} SERVER_PREFERENCE;
#endif /* ENABLE_UNUSED_FUNCTION */

typedef struct odku_tuple_value_arg ODKU_TUPLE_VALUE_ARG;
struct odku_tuple_value_arg
{
  PT_NODE *insert_stmt;         /* insert statement */
  CURSOR_ID *cursor_p;          /* select cursor id */
};

#if defined (ENABLE_UNUSED_FUNCTION)
/* used to generate unique savepoint names */
static int insert_savepoint_number = 0;
#endif

static int check_for_cons (PARSER_CONTEXT * parser,
                           int *has_unique,
                           PT_NODE ** non_null_attrs, const PT_NODE * attr_list, DB_OBJECT * class_obj);
static int is_server_insert_allowed (PARSER_CONTEXT * parser, int *const server_allowed, PT_NODE * statement);
static int is_attr_not_in_insert_list (const PARSER_CONTEXT * parser, PT_NODE * name_list, const char *name);
static int check_missing_non_null_attrs (const PARSER_CONTEXT * parser,
                                         const PT_NODE * spec, PT_NODE * attr_list, const bool has_default_values_list);
static PT_NODE *do_create_odku_stmt (PARSER_CONTEXT * parser, PT_NODE * insert);
static PT_NODE *do_check_insert_server_allowed (PARSER_CONTEXT * parser, PT_NODE * node, void *arg, int *continue_walk);
static int find_shard_key_node (DB_SESSION * session, PT_NODE * statement, PT_NODE * node);

/*
 * do_prepare_insert_internal () - Prepares insert statement for server
 *				   execution.
 *
 * return	  : Error code.
 *   session(in) : contains the SQL query that has been compiled
 * statement (in) : Parse tree node for insert statement.
 */
static int
do_prepare_insert_internal (DB_SESSION * session, PT_NODE * statement)
{
  int error = NO_ERROR;
  PARSER_CONTEXT *parser;

  COMPILE_CONTEXT *contextp;
  XASL_STREAM stream;

  assert (session != NULL);
  assert (session->parser != NULL);

  parser = session->parser;

  contextp = &parser->context;

  init_xasl_stream (&stream);

  if (!parser || !statement || statement->node_type != PT_INSERT)
    {
      assert (false);
      return ER_GENERIC_ERROR;
    }

  /* make shard key info */
  error = parser_set_shard_key (session, statement);
  if (error != NO_ERROR)
    {
      pt_record_error (parser, statement->line_number, statement->column_number, er_msg (), NULL);
      return error;
    }

  contextp->sql_user_text = statement->sql_user_text;
  contextp->sql_user_text_len = statement->sql_user_text_len;

  /* make query string */
  parser->print_type_ambiguity = 0;
  PT_NODE_PRINT_TO_ALIAS (parser, statement, (PT_CONVERT_RANGE | PT_PRINT_QUOTES));
  contextp->sql_hash_text = statement->alias_print;
  if (parser->print_type_ambiguity)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_PREPARE_TOO_LONG_BAD_VALUE, 0);
      return ER_PREPARE_TOO_LONG_BAD_VALUE;
    }

  /* look up server's XASL cache for this query string
     and get XASL file id (XASL_ID) returned if found */
  if (statement->recompile == 0)
    {
      error = prepare_query (contextp, &stream);
      if (error != NO_ERROR)
        {
          error = er_errid ();
        }
    }
  else
    {
      error = qmgr_drop_query_plan (contextp->sql_hash_text, ws_identifier (db_get_user ()), NULL);
    }

  if (stream.xasl_id == NULL && error == NO_ERROR)
    {
      /* mark the beginning of another level of xasl packing */
      pt_enter_packing_buf ();
      contextp->xasl = pt_to_insert_xasl (session, parser, statement);

      if (contextp->xasl)
        {
          assert (contextp->xasl->dptr_list == NULL);

          if (error == NO_ERROR)
            {
              error = xts_map_xasl_to_stream (contextp->xasl, &stream);
              if (error != NO_ERROR)
                {
                  PT_ERRORm (parser, statement, MSGCAT_SET_PARSER_RUNTIME, MSGCAT_RUNTIME_RESOURCES_EXHAUSTED);
                }
            }
        }
      else
        {
          error = er_errid ();
        }

      if (stream.xasl_stream && (error >= NO_ERROR))
        {
          error = prepare_query (contextp, &stream);
          if (error != NO_ERROR)
            {
              error = er_errid ();
            }
        }

      /* mark the end of another level of xasl packing */
      pt_exit_packing_buf ();

      /* As a result of query preparation of the server,
         the XASL cache for this query will be created or updated. */

      /* free 'stream' that is allocated inside of xts_map_xasl_to_stream() */
      if (stream.xasl_stream)
        {
          free_and_init (stream.xasl_stream);
        }

      statement->use_plan_cache = 0;
    }
  else
    {
      if (error == NO_ERROR)
        {
          statement->use_plan_cache = 1;
        }
      else
        {
          statement->use_plan_cache = 0;
        }
    }

  /* save the XASL_ID that is allocated and returned by
     prepare_query() into 'statement->xasl_id'
     to be used by do_execute_update() */
  statement->xasl_id = stream.xasl_id;

  return error;
}

/*
 * check_for_default_expr() - Builds a list of attributes that have a default
 *			      expression and are not found in the specified
 *			      attributes list
 *   return: Error code
 *   parser(in/out): Parser context
 *   specified_attrs(in): the list of attributes that are not to be considered
 *   default_expr_attrs(out):
 *   class_obj(in):
 */
int
check_for_default_expr (PARSER_CONTEXT * parser, PT_NODE * specified_attrs,
                        PT_NODE ** default_expr_attrs, DB_OBJECT * class_obj)
{
  SM_CLASS *cls;
  SM_ATTRIBUTE *att;
  int error = NO_ERROR;
  PT_NODE *new_ = NULL, *node = NULL;

  assert (default_expr_attrs != NULL);
  if (default_expr_attrs == NULL)
    {
      return ER_FAILED;
    }

  error = au_fetch_class_force (class_obj, &cls, S_LOCK);
  if (error != NO_ERROR)
    {
      return error;
    }
  for (att = cls->attributes; att != NULL; att = (SM_ATTRIBUTE *) att->next)
    {
      /* skip if a value has already been specified for this attribute */
      for (node = specified_attrs; node != NULL; node = node->next)
        {
          if (!pt_str_compare (pt_get_name (node), att->name, CASE_INSENSITIVE))
            {
              break;
            }
        }
      if (node != NULL)
        {
          continue;
        }

      /* add attribute to default_expr_attrs list */
      new_ = parser_new_node (parser, PT_NAME);
      if (new_ == NULL)
        {
          PT_INTERNAL_ERROR (parser, "allocate new node");
          return ER_FAILED;
        }
      new_->info.name.original = att->name;
      if (*default_expr_attrs != NULL)
        {
          new_->next = *default_expr_attrs;
          *default_expr_attrs = new_;
        }
      else
        {
          *default_expr_attrs = new_;
        }
    }
  return NO_ERROR;
}

/*
 * check_for_cons() - Determines whether an attribute has not null or unique
 *		      constraints.
 *
 *   return: Error code
 *   parser(in): Parser context
 *   has_unique(in/out):
 *   non_null_attrs(in/out): all the "NOT NULL" attributes
 *   attr_list(in): Parse tree of an insert statement attribute list
 *   class_obj(in): Class object

 */
static int
check_for_cons (PARSER_CONTEXT * parser, int *has_unique,
                PT_NODE ** non_null_attrs, const PT_NODE * attr_list, DB_OBJECT * class_obj)
{
  PT_NODE *pointer;

  assert (non_null_attrs != NULL);
  if (*non_null_attrs != NULL)
    {
      /* non_null_attrs already checked */
      return NO_ERROR;
    }
  *has_unique = 0;

  while (attr_list)
    {
      if (attr_list->node_type != PT_NAME)
        {
          /* bullet proofing, should not get here */
          return ER_GENERIC_ERROR;
        }

      if (*has_unique == 0 && sm_att_unique_constrained (class_obj, attr_list->info.name.original))
        {
          *has_unique = 1;
        }

      if (sm_att_constrained (class_obj, attr_list->info.name.original, SM_ATTFLAG_NON_NULL))
        {
          pointer = pt_point (parser, attr_list);
          if (pointer == NULL)
            {
              PT_ERRORm (parser, attr_list, MSGCAT_SET_PARSER_RUNTIME, MSGCAT_RUNTIME_RESOURCES_EXHAUSTED);

              if (*non_null_attrs)
                {
                  parser_free_tree (parser, *non_null_attrs);
                }
              *non_null_attrs = NULL;

              return MSGCAT_RUNTIME_RESOURCES_EXHAUSTED;
            }
          *non_null_attrs = parser_append_node (pointer, *non_null_attrs);
        }

      attr_list = attr_list->next;
    }

  return NO_ERROR;
}

/*
 * is_server_insert_allowed () - Checks to see if a server-side insert is
 *                               allowed
 *
 * return	  : Error code.
 * parser (in)	  : Parser context.
 * server_allowed(in/out): whether the insert can be executed on the server
 * statement (in) : Parse tree node for insert statement.
 */
static int
is_server_insert_allowed (PARSER_CONTEXT * parser, int *const server_allowed, PT_NODE * statement)
{
  int error = NO_ERROR;
  PT_NODE *attrs = NULL, *attr = NULL;
  PT_NODE *value_clauses = NULL, *class_ = NULL;
  /* set lock timeout hint if specified */
  int save_au;

  assert (server_allowed != NULL);
  *server_allowed = 0;

  if (statement == NULL || statement->node_type != PT_INSERT)
    {
      assert (false);
      return ER_FAILED;
    }

  AU_DISABLE (save_au);

  class_ = statement->info.insert.spec->info.spec.flat_entity_list;
  assert (class_->next == NULL);

  value_clauses = statement->info.insert.value_clauses;
  attrs = pt_attrs_part (statement);
  attr = attrs;
  while (attr)
    {
      if (attr->node_type != PT_NAME)
        {
          /* this can occur when inserting into a view with default values.
           * The name list may not be inverted, and may contain expressions,
           * such as (x+2).
           */
          PT_INTERNAL_ERROR (parser, "insert");
          goto end;
        }
      if (attr->info.name.meta_class != PT_NORMAL)
        {
          /* We found a shared attribute, bail out */
          assert (false);
          PT_INTERNAL_ERROR (parser, "insert");
          goto end;
        }
      attr = attr->next;
    }

  error = check_for_cons (parser, &statement->info.insert.has_uniques,
                          &statement->info.insert.non_null_attrs, attrs, class_->info.name.db_object);
  if (error != NO_ERROR)
    {
      goto end;
    }

  if (value_clauses->info.node_list.list_type == PT_IS_SUBQUERY)
    {
      ;                         /* go ahead */
    }
  else if (value_clauses->info.node_list.list_type == PT_IS_VALUE)
    {
      int all_allowed = 1;      /* init as guess */

      (void) parser_walk_tree (parser, value_clauses, do_check_insert_server_allowed, &all_allowed, NULL, NULL);
      if (pt_has_error (parser) || !all_allowed)
        {
          PT_INTERNAL_ERROR (parser, "insert");
          error = ER_FAILED;
          goto end;
        }
    }
  else
    {
      assert (false);
      PT_INTERNAL_ERROR (parser, "insert");
      error = ER_FAILED;
      goto end;
    }

  /* Even if unique indexes are defined on the class,
   * the operation could be performed on server.
   */
  *server_allowed = 1;

end:

  if (error == NO_ERROR)
    {
      if (pt_has_error (parser))
        {
          error = ER_FAILED;
        }
    }

  AU_ENABLE (save_au);

  return error;
}

/*
 * do_check_insert_server_allowed () - Parser walk function that checks all
 *				       sub-inserts are allowed on server.
 *
 * return	      : Unchanged node argument.
 * parser (in)	      : Parser context.
 * node (in)	      : Parse tree node.
 * arg (out)	      : int * argument that stores server_allowed.
 * continue_walk (in) : Continue walk.
 */
static PT_NODE *
do_check_insert_server_allowed (PARSER_CONTEXT * parser, PT_NODE * node, void *arg, int *continue_walk)
{
  int *server_allowed = (int *) arg;
#if 0
  int error = NO_ERROR;
#endif

  if (node == NULL || node->node_type != PT_INSERT)
    {
      /* stop check */
      return node;
    }

#if 1
  assert (node != NULL);
  assert (node->node_type == PT_INSERT);

  /* this can occur when inserting is nested,
   * such as
   * INSERT ... VALUES (..., INSERT ... VALUES (...))
   */
  PT_INTERNAL_ERROR (parser, "insert");

//  assert (false); /* TODO - enable me after remove object type */

  *server_allowed = 0;
  *continue_walk = PT_STOP_WALK;
#else
  *server_allowed = 1;          /* init as guess */
  error = is_server_insert_allowed (parser, server_allowed, node);
  if (error != NO_ERROR || !(*server_allowed))
    {
      *server_allowed = 0;
      *continue_walk = PT_STOP_WALK;
    }
#endif

  return node;
}

/*
 * do_create_odku_stmt () - create an UPDATE statement for ON DUPLICATE KEY
 *			    UPDATE node
 * return : update node or NULL
 * parser (in) : parser context
 * insert (in) : INSERT statement node
 *
 * Note: this function alters the flag set on the SPEC node of the INSERT
 *   statement. Callers should backup the value of the flag and restore it
 *   when they're finished with the UPDATE statement.
 */
static PT_NODE *
do_create_odku_stmt (PARSER_CONTEXT * parser, PT_NODE * insert)
{
  PT_NODE *update = NULL;

  if (insert == NULL || insert->node_type != PT_INSERT)
    {
      assert (false);
      PT_INTERNAL_ERROR (parser, "invalid arguments");
      return NULL;
    }

  insert->info.insert.spec->info.spec.flag |= PT_SPEC_FLAG_UPDATE;

  update = parser_new_node (parser, PT_UPDATE);
  if (update == NULL)
    {
      PT_INTERNAL_ERROR (parser, "allocate new node");
      goto error_return;
    }
  update->info.update.assignment = insert->info.insert.odku_assignments;
  update->info.update.spec = insert->info.insert.spec;

  return update;

error_return:
  if (update != NULL)
    {
      update->info.update.assignment = NULL;
      update->info.update.spec = NULL;
      parser_free_tree (parser, update);
    }

  return NULL;
}

/*
 * is_attr_not_in_insert_list() - Returns 1 if the name is not on the name_list,
 *              0 otherwise. name_list is assumed to be a list of PT_NAME nodes.
 *   return: Error code
 *   param1(out): Short description of the param1
 *   param2(in/out): Short description of the param2
 *   param2(in): Short description of the param3
 *
 * Note: If you feel the need
 */
static int
is_attr_not_in_insert_list (UNUSED_ARG const PARSER_CONTEXT * parser, PT_NODE * name_list, const char *name)
{
  PT_NODE *tmp;
  int not_on_list = 1;

  for (tmp = name_list; tmp != NULL; tmp = tmp->next)
    {
      if (intl_identifier_casecmp (tmp->info.name.original, name) == 0)
        {
          not_on_list = 0;
          break;
        }
    }

  return not_on_list;

}                               /* is_attr_not_in_insert_list */

/*
 * check_missing_non_null_attrs() - Check to see that all attributes of
 *              the class that have a NOT NULL constraint AND have no default
 *              value are present in the inserts assign list.
 *   return: Error code
 *   parser(in):
 *   spec(in):
 *   attr_list(in):
 *   has_default_values_list(in): whether this statement is used to insert
 *                                default values
 */
static int
check_missing_non_null_attrs (const PARSER_CONTEXT * parser,
                              const PT_NODE * spec, PT_NODE * attr_list, const bool has_default_values_list)
{
  DB_ATTRIBUTE *attr;
  DB_OBJECT *class_;
  int error = NO_ERROR;
  int save_au;

  if (!spec || !spec->info.spec.entity_name || !(class_ = spec->info.spec.entity_name->info.name.db_object))
    {
      return ER_GENERIC_ERROR;
    }

  AU_DISABLE (save_au);
  attr = db_get_attributes (class_);
  while (attr)
    {
      if (db_attribute_is_non_null (attr)
          && db_value_is_null (db_attribute_default (attr))
          && attr->default_value.default_expr == DB_DEFAULT_NONE
          && (is_attr_not_in_insert_list (parser, attr_list, db_attribute_name (attr)) || has_default_values_list))
        {
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_MISSING_NON_NULL_ASSIGN, 1, db_attribute_name (attr));
          error = ER_OBJ_MISSING_NON_NULL_ASSIGN;
        }
      attr = db_attribute_next (attr);
    }
  AU_ENABLE (save_au);

  return error;
}

/*
 * check_all_shard_query() - test if the statement is all-shard query
 *
 * e.g)
 * for the predicate 'a < ?"
 * SELECT FOR UPDATE, DELETE, UPDATE query will return error.
 * SELECT query will execute as all-shard query.
 */
static int
check_all_shard_query (DB_SESSION * session, UNUSED_ARG PT_NODE * stmt, int error)
{
  UNUSED_VAR PARSER_CONTEXT *parser;

  assert (session != NULL);
  assert (session->parser != NULL);

  parser = session->parser;

  assert (error != NO_ERROR);

  if (session->shardkey_required)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      return error;
    }
  else
    {
      session->num_shardkeys = 0;
      free_and_init (session->shardkeys);
      return NO_ERROR;
    }
}

/*
 * node_to_shardkey_info () - make SHARDKEY_INFO from PT_NODE
 *
 * return : NO_ERROR - make shard key info successfully
 * 	    error_code - error or all-shard query.
 *		    do not need to check the other terms
 */
static int
node_to_shardkey_info (DB_SESSION * session, PT_NODE * stmt, PT_NODE * att, PT_NODE * expr)
{
  int error = NO_ERROR;
  UNUSED_VAR PARSER_CONTEXT *parser;

  assert (session != NULL);
  assert (session->parser != NULL);

  parser = session->parser;

  if (expr->node_type != PT_VALUE && expr->node_type != PT_HOST_VAR && expr->node_type != PT_NAME)
    {
      return check_all_shard_query (session, stmt, ER_SHARD_KEY_MUST_VALUE_OR_HOSTVAR);
    }

  if (expr->node_type == PT_NAME)
    {
      if (att->type_enum != expr->type_enum)
        {
          error = ER_SHARD_CANT_ASSIGN_TWO_SHARD_KEY_A_STMT;
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
          return error;
        }

      return NO_ERROR;
    }

  /* node type is PT_VALUE or PT_HOST_VAR */

  if (session->shardkey_exhausted)
    {
      assert (session->num_shardkeys == 0);

      return NO_ERROR;          /* give up */
    }

  if (session->shardkeys == NULL)
    {
      assert (session->num_shardkeys == 0);
      session->shardkeys = (SHARDKEY_INFO *) malloc (sizeof (SHARDKEY_INFO));
    }
  else
    {
      assert (session->num_shardkeys > 0);

      if (session->num_shardkeys >= DB_MAX_SHARDKEY_INFO_COUNT)
        {
          /* too many shardkeys
           * SELECT FOR UPDATE, DELETE, UPDATE query will return error.
           * SELECT query will execute as all-shard query.
           */
          session->shardkey_exhausted = true;

          return check_all_shard_query (session, stmt, ER_SHARD_CANT_ASSIGN_TWO_SHARD_KEY_A_STMT);
        }

      session->shardkeys =
        (SHARDKEY_INFO *) realloc (session->shardkeys, sizeof (SHARDKEY_INFO) * (session->num_shardkeys + 1));
    }

  if (session->shardkeys == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, sizeof (SHARDKEY_INFO) * (session->num_shardkeys + 1));
      return error;
    }

  session->shardkeys[session->num_shardkeys].value = expr;
  session->num_shardkeys++;

  assert (error == NO_ERROR);

  return NO_ERROR;
}

static int
find_shard_key_node (DB_SESSION * session, PT_NODE * statement, PT_NODE * node)
{
  int error = NO_ERROR;
  UNUSED_VAR PARSER_CONTEXT *parser;
  PT_NODE *att, *expr;
  int coll_id;
  PT_NODE *elem;

  assert (session != NULL);
  assert (session->parser != NULL);

  parser = session->parser;

  if (node->or_next != NULL)
    {
      /* 'a = 1 or b = 2'; dnf_list is not shard node
       */
      return NO_ERROR;
    }

  if (node->node_type != PT_EXPR)
    {
      return NO_ERROR;
    }

  att = node->info.expr.arg1;
  expr = node->info.expr.arg2;

  if (!(att->node_type == PT_NAME && PT_NAME_INFO_IS_FLAGED (att, PT_NAME_FOR_SHARD_KEY)))
    {
      /* is not shard column */
      return NO_ERROR;
    }

  assert (statement != NULL);
  assert (att != NULL);
  if (statement == NULL || att == NULL || expr == NULL)
    {
      return NO_ERROR;
    }

  /* save shard key column collation_id */
  if (pt_get_collation_info (att, &coll_id) == true)
    {
      assert (coll_id >= 0);
      if (session->shardkey_coll_id == LANG_COERCIBLE_COLL)
        {
          session->shardkey_coll_id = coll_id;  /* very the first time */
        }

      if (session->shardkey_coll_id != coll_id)
        {
          error = ER_SHARD_INCOMPATIBLE_COLLATIONS;
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
          return error;
        }
    }
  else
    {
      assert (false);
      error = ER_SHARD_INCOMPATIBLE_COLLATIONS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      return error;
    }

  switch (node->info.expr.op)
    {
    case PT_RANGE:
      for (elem = expr; error == NO_ERROR && elem != NULL; elem = elem->or_next)
        {
          if (elem->info.expr.op == PT_BETWEEN_EQ_NA)
            {
              error = node_to_shardkey_info (session, statement, att, elem->info.expr.arg1);
            }
          else
            {
              error = check_all_shard_query (session, statement, ER_SHARD_CANT_ASSIGN_TWO_SHARD_KEY_A_STMT);
              break;            /* immediately stop */
            }
        }

      PT_EXPR_INFO_SET_FLAG (node, PT_EXPR_INFO_SHARD_KEY);
      break;

    case PT_EQ:
      error = node_to_shardkey_info (session, statement, att, expr);

      PT_EXPR_INFO_SET_FLAG (node, PT_EXPR_INFO_SHARD_KEY);
      break;

    default:
      error = check_all_shard_query (session, statement, ER_SHARD_CANT_ASSIGN_TWO_SHARD_KEY_A_STMT);
      break;
    }

  return error;
}

static int
parser_set_shard_key (DB_SESSION * session, PT_NODE * statement)
{
  UNUSED_VAR PARSER_CONTEXT *parser;
  PT_NODE *att = NULL, *expr = NULL;
  PT_NODE *spec;
  DB_OBJECT *table;
  int error = NO_ERROR;
  bool shardkey_required = true;
  PT_NODE *where = NULL;

  assert (session != NULL);
  assert (session->parser != NULL);

  parser = session->parser;

  assert (statement != NULL);
  assert (statement->next == NULL);

  switch (statement->node_type)
    {
    case PT_INSERT:
      spec = statement->info.insert.spec;
      break;

    case PT_UPDATE:
      spec = statement->info.update.spec;
      break;

    case PT_DELETE:
      spec = statement->info.delete_.spec;
      break;

    case PT_SELECT:
      spec = statement->info.query.q.select.from;
      if (spec == NULL)
        {
          return NO_ERROR;
        }
      if (!PT_SELECT_INFO_IS_FLAGED (statement, PT_SELECT_INFO_FOR_UPDATE))
        {
          shardkey_required = false;
        }
      break;

    default:
      return NO_ERROR;
    }

  assert (spec != NULL);

  for (; spec != NULL; spec = spec->next)
    {
      if (spec->info.spec.flat_entity_list != NULL)
        {
          assert (spec->info.spec.flat_entity_list->next == NULL);

          table = spec->info.spec.flat_entity_list->info.name.db_object;
          if (sm_is_shard_table (table))
            {
              break;            /* found shard table */
            }
        }
    }

  if (spec == NULL)
    {
      return NO_ERROR;          /* not found shard table */
    }

  /* found shard table; need to set shardkey info
   */

  if (shardkey_required == true)
    {
      session->shardkey_required = true;
    }

  assert (session->shardkeys == NULL);
  assert (session->num_shardkeys == 0);

  switch (statement->node_type)
    {
    case PT_INSERT:
      {
        PT_NODE *crt_list;
        PT_NODE_LIST_INFO *node_list;
        int i, k, s;
        int num_shardkeys = 0;
        int coll_id;

        crt_list = statement->info.insert.value_clauses;

        if (crt_list->info.node_list.list_type == PT_IS_SUBQUERY)
          {
            assert (crt_list->next == NULL);

            er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_SHARD_UNSUPPORT_INSERT_SELECT_STMT, 0);
            return ER_SHARD_UNSUPPORT_INSERT_SELECT_STMT;
          }

        assert (crt_list->info.node_list.list_type == PT_IS_VALUE);

        /* check iff shard column exists */
        for (att = pt_attrs_part (statement), k = 0; att != NULL; att = att->next, k++)
          {
            if (PT_NAME_INFO_IS_FLAGED (att, PT_NAME_FOR_SHARD_KEY))
              {
                break;          /* found shard column */
              }
          }

        if (att != NULL)
          {
            num_shardkeys = pt_length_of_list (crt_list);
            assert (num_shardkeys > 0);

            if (num_shardkeys >= DB_MAX_SHARDKEY_INFO_COUNT)
              {
                /* too many shardkeys
                 * INSERT ... VALUES query will return error.
                 */
                session->shardkey_exhausted = true;

                error = check_all_shard_query (session, statement, ER_SHARD_CANT_ASSIGN_TWO_SHARD_KEY_A_STMT);
              }
            else
              {
                /* save shard key column collation_id */
                if (pt_get_collation_info (att, &coll_id) == true)
                  {
                    assert (coll_id >= 0);
                    assert (session->shardkey_coll_id == LANG_COERCIBLE_COLL);
                    session->shardkey_coll_id = coll_id;        /* very the first time */
                  }
                else
                  {
                    assert (false);
                    error = ER_SHARD_INCOMPATIBLE_COLLATIONS;
                    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
                    return error;
                  }

                session->shardkeys = (SHARDKEY_INFO *) malloc (sizeof (SHARDKEY_INFO) * num_shardkeys);
                if (session->shardkeys == NULL)
                  {
                    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (PT_NODE *));
                    return ER_OUT_OF_VIRTUAL_MEMORY;
                  }

                /* retrive all value list */
                for (s = 0; s < num_shardkeys && crt_list != NULL; s++, crt_list = crt_list->next)
                  {
                    node_list = &crt_list->info.node_list;
                    for (i = 0, expr = node_list->list; i < k && expr != NULL; i++, expr = expr->next)
                      {
                        ;
                      }

                    /* get k-th expr */
                    if (i != k || expr == NULL || (expr->node_type != PT_VALUE && expr->node_type != PT_HOST_VAR))
                      {
                        er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_SHARD_KEY_MUST_VALUE_OR_HOSTVAR, 0);
                        return ER_SHARD_KEY_MUST_VALUE_OR_HOSTVAR;
                      }

                    session->shardkeys[s].value = expr;
                  }

                assert (s == num_shardkeys);
                assert (crt_list == NULL);
                session->num_shardkeys = num_shardkeys;
              }
          }
      }
      assert (where == NULL);
      break;

    case PT_UPDATE:
      where = statement->info.update.search_cond;
      break;

    case PT_DELETE:
      where = statement->info.delete_.search_cond;
      break;

    case PT_SELECT:
      where = statement->info.query.q.select.where;
      break;
    default:
      break;
    }

  if (where != NULL)
    {
      PT_NODE *node = NULL;

      for (node = where; error == NO_ERROR && node != NULL; node = node->next)
        {
          error = find_shard_key_node (session, statement, node);
        }
    }

  if (session->shardkey_exhausted)
    {
      assert (session->num_shardkeys == 0);
    }

  if (error == NO_ERROR)
    {
      if (session->num_shardkeys == 0)
        {
          error = check_all_shard_query (session, statement, ER_SHARD_NO_SHARD_KEY);
        }
    }

  return error;
}

/*
 * do_prepare_insert () - Prepare the INSERT statement
 *   return: Error code
 *   session(in) : contains the SQL query that has been compiled
 *   statement(in):
 */
int
do_prepare_insert (DB_SESSION * session, PT_NODE * statement)
{
  int error = NO_ERROR;
  PARSER_CONTEXT *parser;
#if !defined(NDEBUG)
  PT_NODE *class_;
#endif
  PT_NODE *update = NULL;
  PT_NODE *values = NULL;
  int upd_has_uniques = 0;
  bool has_default_values_list = false;
  int server_allowed = 0;

  assert (session != NULL);
  assert (session->parser != NULL);

  parser = session->parser;

  if (statement == NULL ||
      statement->node_type != PT_INSERT ||
      statement->info.insert.spec == NULL || statement->info.insert.spec->info.spec.flat_entity_list == NULL)
    {
      assert (false);
      return ER_GENERIC_ERROR;
    }

#if 1                           /* TODO - trace */
  assert (statement->etc == NULL);
#endif

  statement->etc = NULL;

#if !defined(NDEBUG)
  class_ = statement->info.insert.spec->info.spec.flat_entity_list;
  assert (class_->next == NULL);
#endif

  values = statement->info.insert.value_clauses;

  /* prevent multi statements */
  if (pt_length_of_list (statement) > 1)
    {
      return NO_ERROR;
    }

  /* check non null attrs */
  if (values->info.node_list.list_type == PT_IS_DEFAULT_VALUE)
    {
      has_default_values_list = true;
    }

  error = check_missing_non_null_attrs (parser, statement->info.insert.spec,
                                        pt_attrs_part (statement), has_default_values_list);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = is_server_insert_allowed (parser, &server_allowed, statement);
  if (error != NO_ERROR || !server_allowed)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (statement->info.insert.odku_assignments != NULL)
    {
      /* Test if server UPDATE is allowed */
      update = do_create_odku_stmt (parser, statement);
      if (update == NULL)
        {
          error = er_errid ();
          GOTO_EXIT_ON_ERROR;
        }

      error = is_server_update_allowed (parser,
                                        &statement->info.insert.odku_non_null_attrs, &upd_has_uniques,
                                        &server_allowed, update);
      if (error != NO_ERROR || !server_allowed)
        {
          GOTO_EXIT_ON_ERROR;
        }
    }

  error = do_prepare_insert_internal (session, statement);

  return error;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "invalid error code");
    }

  return error;
}

/*
 * do_execute_insert () - Execute the prepared INSERT statement
 *   return: Error code
 *   session(in) : contains the SQL query that has been compiled
 *   statement(in):
 */
int
do_execute_insert (DB_SESSION * session, PT_NODE * statement)
{
  int err;
  PARSER_CONTEXT *parser;
#if !defined(NDEBUG)
  PT_NODE *flat;
#endif
//  DB_OBJECT *class_obj;
  QFILE_LIST_ID *list_id;
  QUERY_FLAG query_flag;
  QUERY_ID query_id_self;

  assert (session != NULL);
  assert (session->parser != NULL);

  parser = session->parser;

  assert (parser->query_id == NULL_QUERY_ID);
  query_id_self = parser->query_id;

  CHECK_MODIFICATION_ERROR ();

#if 1                           /* TODO - trace */
  assert (statement->etc == NULL);
#endif

  if (statement->xasl_id == NULL)
    {
      assert (false);           /* TODO - trace */
      /* check if it is not necessary to execute this statement */
      if (qo_need_skip_execution ())
        {
          statement->etc = NULL;
          return NO_ERROR;
        }

      assert (false);
      err = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, err, 1, "xasl_id is NULL.");
      return err;
    }

#if !defined(NDEBUG)
  flat = statement->info.insert.spec->info.spec.flat_entity_list;
  assert (flat->next == NULL);
#endif
//  class_obj = (flat) ? flat->info.name.db_object : NULL;

  query_flag = NOT_FROM_RESULT_CACHE | RESULT_CACHE_INHIBITED;

  if (prm_get_bool_value (PRM_ID_QUERY_TRACE) == true && parser->query_trace == true)
    {
      do_set_trace_to_query_flag (&query_flag);
    }

  assert (parser->query_id == NULL_QUERY_ID);
  list_id = NULL;

  err = execute_query (session, statement, &list_id, query_flag);

  /* free returned QFILE_LIST_ID */
  if (list_id)
    {
      /* set as result */
      err = list_id->tuple_cnt;
      regu_free_listid (list_id);
    }

  /* end the query; reset query_id and call qmgr_end_query() */
  pt_end_query (parser, query_id_self, statement);

#if 0
  if ((err < NO_ERROR) && er_errid () != NO_ERROR)
    {
      pt_record_error (parser, parser->statement_number,
                       statement->line_number, statement->column_number, er_msg (), NULL);
    }
#endif

  return err;
}

/*
 * do_prepare_select() - Prepare the SELECT statement including optimization and
 *                       plan generation, and creating XASL as the result
 *   return: Error code
 *   session(in) : contains the SQL query that has been compiled
 *   statement(in/out): A statement to do
 *
 * Note:
 */
int
do_prepare_select (DB_SESSION * session, PT_NODE * statement)
{
  int err = NO_ERROR;
  PARSER_CONTEXT *parser;
  int au_save;

  COMPILE_CONTEXT *contextp;
  XASL_STREAM stream;

  assert (session != NULL);
  assert (session->parser != NULL);

  parser = session->parser;

  contextp = &parser->context;

  init_xasl_stream (&stream);

  if (parser == NULL || statement == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  /* if already prepared */
  if (statement->xasl_id)
    {
      return NO_ERROR;
    }

  /* make shard key info */
  err = parser_set_shard_key (session, statement);
  if (err != NO_ERROR)
    {
      pt_record_error (parser, statement->line_number, statement->column_number, er_msg (), NULL);
      return err;
    }

  contextp->sql_user_text = statement->sql_user_text;
  contextp->sql_user_text_len = statement->sql_user_text_len;

  /* make query string */
  parser->print_type_ambiguity = 0;
  PT_NODE_PRINT_TO_ALIAS (parser, statement, (PT_CONVERT_RANGE | PT_PRINT_QUOTES
#if 0
                                              | PT_PRINT_DIFFERENT_SYSTEM_PARAMETERS
#endif
                          ));
  contextp->sql_hash_text = statement->alias_print;
  if (parser->print_type_ambiguity)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_PREPARE_TOO_LONG_BAD_VALUE, 0);
      return ER_PREPARE_TOO_LONG_BAD_VALUE;
    }

  /* look up server's XASL cache for this query string
     and get XASL file id (XASL_ID) returned if found */
  if (statement->recompile == 0)
    {
      XASL_NODE_HEADER xasl_header;
      stream.xasl_header = &xasl_header;

      err = prepare_query (contextp, &stream);
      if (err != NO_ERROR)
        {
          err = er_errid ();
        }
      else if (stream.xasl_id != NULL)
        {
          /* check xasl header */
          if (pt_recompile_for_limit_optimizations (parser, statement, stream.xasl_header->xasl_flag))
            {
              err = qmgr_drop_query_plan (contextp->sql_hash_text, ws_identifier (db_get_user ()), NULL);
              stream.xasl_id = NULL;
            }
        }
    }
  else
    {
      err = qmgr_drop_query_plan (contextp->sql_hash_text, ws_identifier (db_get_user ()), NULL);
    }

  if (stream.xasl_id == NULL && err == NO_ERROR)
    {
      /* cache not found;
         make XASL from the parse tree including query optimization
         and plan generation */

      /* mark the beginning of another level of xasl packing */
      pt_enter_packing_buf ();

      AU_SAVE_AND_DISABLE (au_save);    /* this prevents authorization
                                           checking during generating XASL */
      /* parser_generate_xasl() will build XASL tree from parse tree */
      contextp->xasl = parser_generate_xasl (parser, statement);
      AU_RESTORE (au_save);

      if (contextp->xasl && (err == NO_ERROR) && !pt_has_error (parser))
        {
          /* convert the created XASL tree to the byte stream for transmission
             to the server */
          err = xts_map_xasl_to_stream (contextp->xasl, &stream);
          if (err != NO_ERROR)
            {
              PT_ERRORm (parser, statement, MSGCAT_SET_PARSER_RUNTIME, MSGCAT_RUNTIME_RESOURCES_EXHAUSTED);
            }
        }
      else
        {
          err = er_errid ();
          if (err == NO_ERROR)
            {
              err = ER_FAILED;
            }
        }

      /* request the server to prepare the query;
         give XASL stream generated from the parse tree
         and get XASL file id returned */
      if (stream.xasl_stream && (err == NO_ERROR))
        {
          err = prepare_query (contextp, &stream);
          if (err != NO_ERROR)
            {
              err = er_errid ();
            }
        }

      /* mark the end of another level of xasl packing */
      pt_exit_packing_buf ();

      /* As a result of query preparation of the server,
         the XASL cache for this query will be created or updated. */

      /* free 'stream' that is allocated inside of xts_map_xasl_to_stream() */
      if (stream.xasl_stream)
        {
          free_and_init (stream.xasl_stream);
        }
      statement->use_plan_cache = 0;

    }
  else
    {
      if (err == NO_ERROR)
        {
          statement->use_plan_cache = 1;
        }
      else
        {
          statement->use_plan_cache = 0;
        }
    }

  /* save the XASL_ID that is allocated and returned by prepare_query()
     into 'statement->xasl_id' to be used by do_execute_select() */
  statement->xasl_id = stream.xasl_id;

  return err;
}                               /* do_prepare_select() */

/*
 * do_execute_select() - Execute the prepared SELECT statement
 *   return: Error code
 *   session(in) : contains the SQL query that has been compiled
 *   statement(in/out): A statement to do
 *
 * Note:
 */
int
do_execute_select (DB_SESSION * session, PT_NODE * statement)
{
  int err;
  PARSER_CONTEXT *parser;
  QFILE_LIST_ID *list_id;
  QUERY_FLAG query_flag;
  int au_save;
  bool query_trace = false;

  assert (session != NULL);
  assert (session->parser != NULL);

  parser = session->parser;

  assert (parser->query_id == NULL_QUERY_ID);

  /* check if it is not necessary to execute this statement,
     e.g. false where or not prepared correctly */
  if (statement->xasl_id == NULL)
    {
      assert (false);           /* TODO - trace */
      statement->etc = NULL;
      return NO_ERROR;
    }

  if (prm_get_bool_value (PRM_ID_QUERY_TRACE) == true && parser->query_trace == true)
    {
      query_trace = true;
    }

  /* adjust query flag */
  query_flag = EXEC_INIT_FLAG;

  if (statement->si_datetime == 1)
    {
      statement->info.query.reexecute = 1;
      statement->info.query.do_not_cache = 1;
    }

  if (statement->info.query.reexecute == 1)
    {
      query_flag |= NOT_FROM_RESULT_CACHE;
    }

  if (statement->info.query.do_cache == 1)
    {
      query_flag |= RESULT_CACHE_REQUIRED;
    }

  if (statement->info.query.do_not_cache == 1)
    {
      query_flag |= RESULT_CACHE_INHIBITED;
    }
  if (parser->is_holdable)
    {
      query_flag |= RESULT_HOLDABLE;
    }

  if (query_trace == true)
    {
      do_set_trace_to_query_flag (&query_flag);
    }

#if 1                           /* TODO - trace */
  assert (!ws_has_updated ());
#else
  /* flush necessary objects before execute */
  if (ws_has_updated ())
    {
      (void) parser_walk_tree (parser, statement, pt_flush_classes, NULL, NULL, NULL);
    }
#endif

  /* Request that the server executes the stored XASL, which is the execution
     plan of the prepared query, with the host variables given by users as
     parameter values for the query.
     As a result, query id and result file id (QFILE_LIST_ID) will be returned.
     do_prepare_select() has saved the XASL file id (XASL_ID) in
     'statement->xasl_id' */

  AU_SAVE_AND_ENABLE (au_save); /* this insures authorization
                                   checking for method */

  assert (parser->query_id == NULL_QUERY_ID);
  list_id = NULL;

  err = execute_query (session, statement, &list_id, query_flag);

  AU_RESTORE (au_save);

  /* save the returned QFILE_LIST_ID into 'statement->etc' */
  statement->etc = (void *) list_id;

  if (err < NO_ERROR)
    {
      return er_errid ();
    }

  return err;
}





/*
 * Function Group:
 * DO Functions for replication management
 *
 */


/*
 * do_replicate_schema() -
 *   return: Error code
 *   parser(in): Parser context
 *   statement(in): The parse tree of a DDL statement
 *
 * Note:
 */
int
do_replicate_schema (PARSER_CONTEXT * parser, PT_NODE * statement)
{
  int error = NO_ERROR;
  REPL_INFO repl_info;
  REPL_INFO_SCHEMA repl_schema;
  PARSER_VARCHAR *name = NULL;
  static const char *unknown_schema_name = "-";
  unsigned int save_custom;
  MOP mop;
  LOCK lock;

  if (log_does_allow_replication () == false)
    {
      return NO_ERROR;
    }

  if (!is_schema_repl_log_statment (statement))
    {
      assert (false);
      return NO_ERROR;
    }

  /* refer pt_is_ddl_statement ()
   */
  switch (statement->node_type)
    {
    case PT_CREATE_ENTITY:
      name = pt_print_bytes (parser, statement->info.create_entity.entity_name);
      repl_schema.statement_type = RYE_STMT_CREATE_CLASS;
      break;

    case PT_ALTER:
      name = pt_print_bytes (parser, statement->info.alter.entity_name);
      repl_schema.statement_type = RYE_STMT_ALTER_CLASS;
      break;

    case PT_RENAME:
      name = pt_print_bytes (parser, statement->info.rename.old_name);
      repl_schema.statement_type = RYE_STMT_RENAME_CLASS;
      break;

    case PT_DROP:
      /* No replication log will be written
       * when there's no applicable table for "drop if exists"
       */
      if (statement->info.drop.if_exists && statement->info.drop.spec_list == NULL)
        {
          return NO_ERROR;
        }
      repl_schema.statement_type = RYE_STMT_DROP_CLASS;
      break;

    case PT_CREATE_INDEX:
      save_custom = parser->custom_print;
      parser->custom_print |= PT_SUPPRESS_RESOLVED;
      name = pt_print_bytes (parser, statement->info.index.indexed_class);
      parser->custom_print = save_custom;

      repl_schema.statement_type = RYE_STMT_CREATE_INDEX;
      break;

    case PT_ALTER_INDEX:
      name = pt_print_bytes (parser, statement->info.index.indexed_class);
      repl_schema.statement_type = RYE_STMT_ALTER_INDEX;
      break;

    case PT_DROP_INDEX:
      name = pt_print_bytes (parser, statement->info.index.indexed_class);
      repl_schema.statement_type = RYE_STMT_DROP_INDEX;
      break;

    case PT_CREATE_USER:
      repl_schema.statement_type = RYE_STMT_CREATE_USER;
      break;

    case PT_ALTER_USER:
      repl_schema.statement_type = RYE_STMT_ALTER_USER;
      break;

    case PT_DROP_USER:
      repl_schema.statement_type = RYE_STMT_DROP_USER;
      break;

    case PT_GRANT:
      repl_schema.statement_type = RYE_STMT_GRANT;
      break;

    case PT_REVOKE:
      repl_schema.statement_type = RYE_STMT_REVOKE;
      break;

    case PT_UPDATE_STATS:
      name = pt_print_bytes (parser, statement->info.update_stats.class_list);
      repl_schema.statement_type = RYE_STMT_UPDATE_STATS;
      break;

    default:
      assert (false);           /* is impossible */
      break;
    }

  repl_info.repl_info_type = REPL_INFO_TYPE_SCHEMA;
  if (name == NULL)
    {
      repl_schema.name = unknown_schema_name;
      repl_schema.online_ddl_type = REPL_BLOCKED_DDL;
    }
  else
    {
      repl_schema.name = (const char *) pt_get_varchar_bytes (name);

      repl_schema.online_ddl_type = REPL_BLOCKED_DDL;
      mop = ws_find_class (repl_schema.name);
      if (mop != NULL)
        {
          lock = ws_get_lock (mop);
          if (lock != X_LOCK)
            {
              repl_schema.online_ddl_type = REPL_NON_BLOCKED_DDL;
            }
        }
    }

  repl_schema.ddl = statement->sql_user_text;

  repl_schema.db_user = db_get_user_name ();

  assert_release (repl_schema.db_user != NULL);

  repl_info.info = (char *) &repl_schema;

  error = locator_flush_replication_info (&repl_info);

  db_string_free ((char *) repl_schema.db_user);

  return error;
}


/*
 * Function Group:
 * Implements the DO statement.
 *
 */

/*
 * insert_check_names_in_value_clauses () - Check names in insert VALUE clause.
 *
 * return		 : void.
 * parser (in)		 : Parser context.
 * insert_statement (in) : Insert statement.
 *
 */
void
insert_check_names_in_value_clauses (PARSER_CONTEXT * parser, PT_NODE * insert_statement)
{
  PT_NODE *attr_list = NULL, *value_clauses = NULL, *value_list = NULL;
  PT_NODE *value = NULL, *value_tmp = NULL, *save_next = NULL, *prev = NULL;

  if (insert_statement == NULL || insert_statement->node_type != PT_INSERT)
    {
      return;
    }

  attr_list = pt_attrs_part (insert_statement);
  value_clauses = insert_statement->info.insert.value_clauses;
  if (attr_list == NULL || value_clauses == NULL)
    {
      return;
    }

  for (value_list = value_clauses; value_list != NULL; value_list = value_list->next)
    {
      if (value_list->info.node_list.list_type != PT_IS_VALUE)
        {
          continue;
        }

      prev = NULL;
      for (value = value_list->info.node_list.list; value != NULL; value = save_next)
        {
          save_next = value->next;
          if (PT_IS_VALUE_NODE (value) || PT_IS_HOSTVAR (value))
            {
              prev = value;
              continue;
            }
          value->next = NULL;

          value = parser_walk_tree (parser, value, do_check_names_for_insert_values_pre, NULL, NULL, NULL);
          if (!pt_has_error (parser))
            {
              value_tmp = pt_semantic_type (parser, value, NULL);
              if (value_tmp == NULL)
                {
                  /* In this case, pt_has_error (parser) is true,
                   * we need recovery the link list firstly, then return. */
                  ;
                }
              else
                {
                  value = value_tmp;
                }
            }
          value->next = save_next;
          if (prev == NULL)
            {
              value_list->info.node_list.list = value;
            }
          else
            {
              prev->next = value;
            }
          if (pt_has_error (parser))
            {
              return;
            }
          prev = value;
        }
    }
}

/*
 * do_check_names_for_insert_values_pre () - Used by parser_walk_tree to
 *					       check names in insert values
 *
 * return	      : node or replaced name.
 * parser (in)	      : parser context.
 * node (in)	      : node in parse tree.
 * arg (in)	      : NULL.
 * continue_walk (in) : continue walk.
 */
static PT_NODE *
do_check_names_for_insert_values_pre (PARSER_CONTEXT * parser, PT_NODE * node, UNUSED_ARG void *arg, int *continue_walk)
{
  if (node == NULL || *continue_walk == PT_STOP_WALK || pt_has_error (parser))
    {
      return node;
    }

  switch (node->node_type)
    {
    case PT_NAME:
      *continue_walk = PT_LIST_WALK;

      /* column not allowed here */
      PT_ERRORmf (parser, node, MSGCAT_SET_PARSER_RUNTIME,
                  MSGCAT_RUNTIME__CAN_NOT_EVALUATE, pt_short_print (parser, node));
      break;
    case PT_EXPR:
      /* continue walk if current node is expression except default */
      if (node->info.expr.op == PT_DEFAULTF)
        {
          assert (node->info.expr.arg1 != NULL);
          assert (node->info.expr.arg1->node_type == PT_NAME);
          assert (node->info.expr.arg1->info.name.default_value != NULL);

          *continue_walk = PT_LIST_WALK;
        }
      else
        {
          *continue_walk = PT_CONTINUE_WALK;
        }
      break;
    case PT_FUNCTION:
      /* continue walk if current node is function */
      *continue_walk = PT_CONTINUE_WALK;
      break;

      /* stop advancing in the tree if node is not a name, expression or
       * function
       */

    case PT_VALUE:
      *continue_walk = PT_LIST_WALK;
      break;

    default:
      *continue_walk = PT_LIST_WALK;
      break;
    }

  return node;
}

/*
 * do_set_query_trace() - Set query trace
 *   return: NO_ERROR
 *   parser(in): Parser context
 *   statement(in): Parse tree of a set statement
 *
 */
int
do_set_query_trace (UNUSED_ARG PARSER_CONTEXT * parser, UNUSED_ARG PT_NODE * statement)
{
#if defined(SA_MODE)
  return NO_ERROR;
#else
  if (statement->info.trace.on_off == PT_TRACE_ON)
    {
      prm_set_bool_value (PRM_ID_QUERY_TRACE, true);

      if (statement->info.trace.format == PT_TRACE_FORMAT_TEXT)
        {
          prm_set_integer_value (PRM_ID_QUERY_TRACE_FORMAT, QUERY_TRACE_TEXT);
        }
      else if (statement->info.trace.format == PT_TRACE_FORMAT_JSON)
        {
          prm_set_integer_value (PRM_ID_QUERY_TRACE_FORMAT, QUERY_TRACE_JSON);
        }
    }
  else
    {
      prm_set_bool_value (PRM_ID_QUERY_TRACE, false);
    }

  return NO_ERROR;
#endif /* SA_MODE */
}

/*
 * do_set_trace_to_query_flag() -
 *   return: void
 *   query_flag(in):
 */
static void
do_set_trace_to_query_flag (QUERY_FLAG * query_flag)
{
  int trace_format;

  trace_format = prm_get_integer_value (PRM_ID_QUERY_TRACE_FORMAT);

  if (trace_format == QUERY_TRACE_TEXT)
    {
      *query_flag |= XASL_TRACE_TEXT;
    }
  else if (trace_format == QUERY_TRACE_JSON)
    {
      *query_flag |= XASL_TRACE_JSON;
    }
}
