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
 * query.c - Query processor main interface
 */

#ident "$Id$"

#include "config.h"

#include <ctype.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>

#include "error_manager.h"
#include "work_space.h"
#include "object_representation.h"
#include "db.h"
#include "schema_manager.h"
#include "xasl_support.h"
#include "server_interface.h"
#include "optimizer.h"
#include "network_interface_cl.h"
#include "transaction_cl.h"

static int evaluate_shard_key (PARSER_CONTEXT * parser, PT_NODE * node,
			       int coll_id, DB_VALUE * val);

/*
 * prepare_query () - Prepares a query for later (and repetitive)
 *                         execution
 *   return		 : Error code
 *   context (in)	 : query string; used for hash key of the XASL cache
 *   stream (in/out)	 : XASL stream, size, xasl_id & xasl_header;
 *                         set to NULL if you want to look up the XASL cache
 *
 *   NOTE: If stream->xasl_header is not NULL, also XASL node header will be
 *	   requested from server.
 */
int
prepare_query (COMPILE_CONTEXT * context, XASL_STREAM * stream)
{
  int ret = NO_ERROR;

  assert (context->sql_hash_text);

  /* if QO_PARAM_LEVEL indicate no execution, just return */
  if (qo_need_skip_execution ())
    {
      return NO_ERROR;
    }

  /* allocate XASL_ID, the caller is responsible to free this */
  stream->xasl_id = (XASL_ID *) malloc (sizeof (XASL_ID));
  if (stream->xasl_id == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_OUT_OF_VIRTUAL_MEMORY, 1, sizeof (XASL_ID));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  /* send XASL stream to the server and get XASL_ID */
  if (qmgr_prepare_query (context, stream,
			  ws_identifier (db_get_user ())) == NULL)
    {
      free_and_init (stream->xasl_id);
      return ((ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;
    }

  /* if the query is not found in the cache */
  if (stream->xasl_stream == NULL && stream->xasl_id &&
      XASL_ID_IS_NULL (stream->xasl_id))
    {
      free_and_init (stream->xasl_id);
    }

  assert (ret == NO_ERROR);

  return ret;
}

static int
evaluate_shard_key (PARSER_CONTEXT * parser, PT_NODE * node, int coll_id,
		    DB_VALUE * val)
{
  DB_VALUE *tmp_val = NULL;

  DB_MAKE_NULL (val);

  if (node->node_type != PT_VALUE && node->node_type != PT_HOST_VAR)
    {
      assert (false);		/* is impossible */
      goto exit_on_error;
    }

  tmp_val = pt_value_to_db (parser, node);
  if (tmp_val != NULL)
    {
      TP_DOMAIN *dom;
      TP_DOMAIN_STATUS dom_status;

      (void) db_value_clone (tmp_val, val);

#if 1				/* TODO - do not delete me; need for rsql */
      if (!TP_IS_CHAR_TYPE (DB_VALUE_DOMAIN_TYPE (val)))
	{
	  dom = db_type_to_db_domain (DB_TYPE_VARCHAR);
	  if (dom == NULL)
	    {
	      assert (false);	/* is impossible */
	      goto exit_on_error;
	    }

	  /* the domains don't match, must attempt coercion */
	  dom_status = tp_value_coerce (val, val, dom);
	  if (dom_status != DOMAIN_COMPATIBLE)
	    {
	      assert (false);	/* is impossible */
	      (void) tp_domain_status_er_set (dom_status, ARG_FILE_LINE,
					      val, dom);
	      assert (er_errid () != NO_ERROR);

	      goto exit_on_error;
	    }
	}
#endif

      db_string_put_cs_and_collation (val, coll_id);
    }

  assert (TP_IS_CHAR_TYPE (DB_VALUE_DOMAIN_TYPE (val)));

  return NO_ERROR;

exit_on_error:

  DB_MAKE_NULL (val);

  return ER_FAILED;
}

/*
 * execute_query () - Execute a prepared query
 *   return: Error code
 *   session(in) : contains the SQL query that has been compiled
 *   xasl_id(in)        : XASL file id that was a result of prepare_query()
 *   statement(in)     :
 *   list_idp(out)      : query result file id (QFILE_LIST_ID)
 *   flag(in)   : flag to determine if this is an asynchronous query
 */
int
execute_query (DB_SESSION * session, PT_NODE * statement,
	       QFILE_LIST_ID ** list_idp, QUERY_FLAG flag)
{
  int ret = NO_ERROR;
  PARSER_CONTEXT *parser;
  DB_VALUE shard_key_val, *shard_key = NULL;
  QUERY_EXECUTE_STATUS_FLAG qe_status_flag = 0;
  int i;

  assert (session != NULL);
  assert (session->parser != NULL);

  parser = session->parser;

  /*init */

  *list_idp = NULL;

  shard_key = &shard_key_val;
  DB_MAKE_NULL (shard_key);

  assert (statement != NULL);
  assert (statement->next == NULL);

  /* if QO_PARAM_LEVEL indicate no execution, just return */
  if (qo_need_skip_execution ())
    {
      return NO_ERROR;
    }

  /* defense code; check iff is DDL */
  if (pt_is_ddl_statement (statement))
    {
      assert (false);		/* currently, is impossible */
      ret = ER_GENERIC_ERROR;	/* TODO - */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ret, 1,
	      "execute_query(): Not permit DDL statement");

      db_value_clear (shard_key);

      return ret;
    }

  assert (!pt_is_ddl_statement (statement));

  if (session->num_shardkeys > 0)
    {
      /* is 1-shard SQL for shard table */
      assert (session->groupid != NULL_GROUPID);
      assert (session->shardkey_exhausted == false);

      /* defense code */
      if (session->shardkeys == NULL)
	{
	  assert (false);	/* is impossible */
	  db_value_clear (shard_key);

	  ret = ER_SHARD_NO_SHARD_KEY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ret, 0);

	  return ret;
	}

      if (evaluate_shard_key (parser, session->shardkeys[0].value,
			      session->shardkey_coll_id,
			      shard_key) != NO_ERROR)
	{
	  db_value_clear (shard_key);

	  ret = ER_SHARD_NO_SHARD_KEY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ret, 0);

	  return ret;		/* error */
	}

      i = 1;

#if 1				/* TODO - do not delete me; need for rsql */
      if (session->num_shardkeys > 1)
	{
	  DB_VALUE prev_key_val;

	  DB_MAKE_NULL (&prev_key_val);

	  for (; i < session->num_shardkeys; i++)
	    {
	      db_value_clear (&prev_key_val);
	      db_value_clone (shard_key, &prev_key_val);
	      db_value_clear (shard_key);

	      if (evaluate_shard_key (parser, session->shardkeys[i].value,
				      session->shardkey_coll_id,
				      shard_key) != NO_ERROR)
		{
		  break;	/* error */
		}

	      if (db_value_compare (shard_key, &prev_key_val) != DB_EQ)
		{
		  break;	/* error */
		}
	    }

	  db_value_clear (&prev_key_val);
	}
#endif

      /* check iff has only one shardkey */
      if (i == session->num_shardkeys)
	{
	  /* found only one shard key */

	  if (shard_key == NULL || DB_IS_NULL (shard_key))
	    {
	      db_value_clear (shard_key);

	      ret = ER_SHARD_NO_SHARD_KEY;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ret, 0);

	      return ret;
	    }
	}
      else
	{
	  /* found another shard keys; is error or all-shard SQL */

	  db_value_clear (shard_key);

	  if (session->shardkey_required)
	    {
	      /* is DML */
	      ret = ER_SHARD_CANT_ASSIGN_TWO_SHARD_KEY_A_STMT;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ret, 0);

	      return ret;
	    }
	  else
	    {
	      /* is not DML */
	      assert (DB_IS_NULL (shard_key));
	      shard_key = NULL;
	    }
	}
    }
  else
    {
      assert (session->shardkey_required == false);

      assert (DB_IS_NULL (shard_key));
      shard_key = NULL;
    }

  if (parser->is_autocommit)
    {
      flag |= AUTO_COMMIT_MODE;
      parser->is_autocommit = false;
    }
  if (session->from_migrator)
    {
      flag |= REQUEST_FROM_MIGRATOR;
    }

  *list_idp =
    qmgr_execute_query (statement->xasl_id, &parser->query_id,
			parser->host_var_count,
			parser->host_variables, flag,
			tran_get_query_timeout (),
			shard_key != NULL ? session->groupid : GLOBAL_GROUPID,
			shard_key, &qe_status_flag);

  db_value_clear (shard_key);

  if (IS_SERVER_QUERY_ENDED (qe_status_flag))
    {
      statement->is_server_query_ended = 1;
    }

  if (*list_idp == NULL)
    {
      return ((ret = er_errid ()) == NO_ERROR) ? ER_FAILED : ret;
    }

  assert (ret == NO_ERROR);

  return ret;
}
