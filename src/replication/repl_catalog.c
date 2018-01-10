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
 * repl_catalog.c -
 */

#ident "$Id$"

#include "db.h"
#include "transform.h"
#include "repl_common.h"
#include "repl_catalog.h"
#include "cci_util.h"
#include "cas_cci_internal.h"
#include "schema_manager.h"
#include "repl_page_buffer.h"
#include "repl_applier.h"
#include "system_parameter.h"
#include "query_executor.h"

#define REPL_CT_QRY_BUF_SIZE 	(ONE_K * 2)	/* 2048 */

/* common */
static int cirp_make_select_query_with_pk (char *query_buf, size_t buf_size,
					   CATCLS_TABLE * table,
					   int num_pk_array,
					   const char **pk_array);
static int cirp_make_count_all_query_with_cond (char *query_buf,
						size_t buf_size,
						const char *table_name,
						const char *cond);

/* analyzer */
static int cirp_get_analyzer_from_result (CIRP_CT_LOG_ANALYZER * ct_data,
					  CCI_CONN * conn, char *query);

/* applier */
static int rpct_get_num_log_applier (CCI_CONN * conn, int *num_appliers,
				     const PRM_NODE_INFO * host_info);
static int rpct_get_log_applier (CCI_CONN * conn,
				 CIRP_CT_LOG_APPLIER * ct_data,
				 const PRM_NODE_INFO * host_info,
				 int applier_id);
static int cirp_get_applier_from_result (CIRP_CT_LOG_APPLIER * applier,
					 CCI_CONN * conn, char *query);


/*
 * rpct_get_log_analyzer () -
 *   return:
 *
 *   conn(in):
 *   analyzer(out):
 *   host_info(in):
 */
int
rpct_get_log_analyzer (CCI_CONN * conn, CIRP_CT_LOG_ANALYZER * ct_data,
		       const PRM_NODE_INFO * host_info)
{
  int error = NO_ERROR;
  char query_buf[REPL_CT_QRY_BUF_SIZE];
  int len;
  CATCLS_TABLE *table;
  const char *pk_array[1];
  char host_key_str[MAX_NODE_INFO_STR_LEN];

  table = &table_LogAnalyzer;

  prm_node_info_to_str (host_key_str, sizeof (host_key_str), host_info);
  pk_array[0] = host_key_str;
  len = cirp_make_select_query_with_pk (query_buf, sizeof (query_buf), table,
					1, pk_array);
  if (len <= 0)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "query error");

      GOTO_EXIT_ON_ERROR;
    }

  error = cirp_get_analyzer_from_result (ct_data, conn, query_buf);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  return error;

exit_on_error:

  return error;
}

/*
 * rpct_insert_log_analyzer() -
 *   return: error code
 *
 *   conn(in):
 *   ct_data(in):
 */
int
rpct_insert_log_analyzer (CCI_CONN * conn, CIRP_CT_LOG_ANALYZER * ct_data)
{
  CIRP_CT_LOG_ANALYZER tmp_rec;
  CIRP_REPL_ITEM *item = NULL;
  int error = NO_ERROR;
  LOG_LSA null_lsa;
  RECDES recdes;

  LSA_SET_NULL (&null_lsa);

  error = rp_new_repl_catalog_item (&item, &null_lsa);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  recdes.area_size = sizeof (CIRP_CT_LOG_ANALYZER);
  recdes.data = (char *) (&tmp_rec);
  error = rpct_analyzer_to_catalog_item (&item->info.catalog, &recdes,
					 ct_data);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  item->info.catalog.copyarea_op = LC_FLUSH_HA_CATALOG_ANALYZER_UPDATE;

  assert (rp_is_valid_repl_item (item));
  error = cci_send_repl_data (conn, item, 1);
  if (error < 0)
    {
      REPL_SET_GENERIC_ERROR (error, "cci error(%d), msg:%s",
			      conn->err_buf.err_code, conn->err_buf.err_msg);

      GOTO_EXIT_ON_ERROR;
    }

  cirp_free_repl_item (item);
  item = NULL;

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "Invalid error code");
    }

  if (item != NULL)
    {
      cirp_free_repl_item (item);
      item = NULL;
    }

  return error;
}

/*
 * rpct_update_log_analyzer () -
 *   returns  : error code, if execution failed
 *              number of affected objects, if a success
 *
 *   conn(in):
 *   log_writer(in):
 */
int
rpct_update_log_analyzer (CCI_CONN * conn, CIRP_CT_LOG_ANALYZER * ct_data)
{
  CIRP_CT_LOG_ANALYZER tmp_rec;
  CIRP_REPL_ITEM *item = NULL;
  int error = NO_ERROR;
  LOG_LSA null_lsa;
  RECDES recdes;

  LSA_SET_NULL (&null_lsa);

  error = rp_new_repl_catalog_item (&item, &null_lsa);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  recdes.area_size = sizeof (CIRP_CT_LOG_ANALYZER);
  recdes.data = (char *) (&tmp_rec);
  error = rpct_analyzer_to_catalog_item (&item->info.catalog, &recdes,
					 ct_data);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  item->info.catalog.copyarea_op = LC_FLUSH_HA_CATALOG_ANALYZER_UPDATE;

  assert (rp_is_valid_repl_item (item));
  error = cci_send_repl_data (conn, item, 1);
  if (error < 0)
    {
      REPL_SET_GENERIC_ERROR (error, "cci error(%d), msg:%s",
			      conn->err_buf.err_code, conn->err_buf.err_msg);

      GOTO_EXIT_ON_ERROR;
    }

  cirp_free_repl_item (item);
  item = NULL;

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "Invalid error code");
    }

  if (item != NULL)
    {
      cirp_free_repl_item (item);
      item = NULL;
    }

  return error;
}

/*
 * rpct_init_applier_info() -
 *   return: error code
 *
 *   conn(in):
 *   host_info(in):
 */
int
rpct_init_applier_info (CCI_CONN * conn, const PRM_NODE_INFO * host_info)
{
  CIRP_APPLIER_INFO *applier = NULL;
  int num_appliers;
  int i;
  int error = NO_ERROR;

  error = cci_set_autocommit (conn, CCI_AUTOCOMMIT_FALSE);
  if (error != CCI_ER_NO_ERROR)
    {
      REPL_SET_GENERIC_ERROR (error, conn->err_buf.err_msg);
      GOTO_EXIT_ON_ERROR;
    }

  error = rpct_get_num_log_applier (conn, &num_appliers, host_info);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (num_appliers != 0 && Repl_Info->num_applier != num_appliers)
    {
      REPL_SET_GENERIC_ERROR (error, "invalid max_num_appliers");

      GOTO_EXIT_ON_ERROR;
    }

  if (num_appliers == 0)
    {
      /* insert new log appliers info */
      for (i = 0; i < Repl_Info->num_applier; i++)
	{
	  applier = &Repl_Info->applier_info[i];

	  applier->ct.host_info = *host_info;
	  applier->ct.id = i + 1;

	  LSA_SET_NULL (&applier->ct.committed_lsa);

	  error = rpct_insert_log_applier (conn, &applier->ct);
	  if (error != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }

	  er_log_debug (ARG_FILE_LINE, "insert applier info(id:%d)",
			applier->ct.id);
	}
    }
  else
    {
      if (num_appliers != Repl_Info->num_applier)
	{
	  REPL_SET_GENERIC_ERROR (error, "invalid max_num_appliers");

	  GOTO_EXIT_ON_ERROR;
	}
      /* get new log appliers info */
      for (i = 0; i < Repl_Info->num_applier; i++)
	{
	  applier = &Repl_Info->applier_info[i];

	  error = rpct_get_log_applier (conn, &applier->ct, host_info, i + 1);
	  if (error != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}
    }

  error = cci_end_tran (conn, CCI_TRAN_COMMIT);
  if (error != CCI_ER_NO_ERROR)
    {
      REPL_SET_GENERIC_ERROR (error, conn->err_buf.err_msg);

      GOTO_EXIT_ON_ERROR;
    }

  cci_set_autocommit (conn, CCI_AUTOCOMMIT_TRUE);

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  assert (error != NO_ERROR);

  cci_end_tran (conn, CCI_TRAN_ROLLBACK);
  cci_set_autocommit (conn, CCI_AUTOCOMMIT_TRUE);

  return error;
}

/*
 * rpct_get_log_applier () -
 *   return:
 *
 *   conn(in):
 *   ct_data(out):
 *   host_info(in):
 *   applier_id(in):
 */
static int
rpct_get_log_applier (CCI_CONN * conn,
		      CIRP_CT_LOG_APPLIER * ct_data,
		      const PRM_NODE_INFO * host_info, int applier_id)
{
  int error = NO_ERROR;
  char query_buf[REPL_CT_QRY_BUF_SIZE];
  int len;
  CATCLS_TABLE *table;
  const char *pk_array[2];
  char id_buf[32];
  char host_key_str[MAX_NODE_INFO_STR_LEN];

  table = &table_LogApplier;

  prm_node_info_to_str (host_key_str, sizeof (host_key_str), host_info);
  pk_array[0] = host_key_str;
  snprintf (id_buf, sizeof (id_buf), "%d", applier_id);
  pk_array[1] = id_buf;
  len = cirp_make_select_query_with_pk (query_buf, sizeof (query_buf),
					table, 2, pk_array);
  if (len <= 0)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "query error");

      return error;
    }

  error = cirp_get_applier_from_result (ct_data, conn, query_buf);
  if (error < 0)
    {
      GOTO_EXIT_ON_ERROR;
    }

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  assert (error != NO_ERROR);

  return error;
}

/*
 * rpct_get_num_log_applier () -
 *   return:
 *
 *   conn(in):
 *   num_appliers(out):
 *   host_info(in):
 */
static int
rpct_get_num_log_applier (CCI_CONN * conn, int *num_appliers,
			  const PRM_NODE_INFO * host_info)
{
  int error = NO_ERROR;
  char query_buf[REPL_CT_QRY_BUF_SIZE];
  char cond[REPL_CT_QRY_BUF_SIZE];
  int len;
  CATCLS_TABLE *table;
  CCI_STMT stmt;
  int ind;
  int n;
  char host_key_str[MAX_NODE_INFO_STR_LEN];

  table = &table_LogApplier;

  /* init out parameter */
  *num_appliers = -1;

  assert (strcasecmp (table->columns[0].name, "host_ip") == 0);

  prm_node_info_to_str (host_key_str, sizeof (host_key_str), host_info);
  snprintf (cond, sizeof (cond), "%s='%s'",
	    table->columns[0].name, host_key_str);
  len = cirp_make_count_all_query_with_cond (query_buf, sizeof (query_buf),
					     table->name, cond);
  if (len <= 0)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "query error");

      return error;
    }

  error = cirp_execute_query (conn, &stmt, query_buf);
  if (error < 0)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = cci_fetch_next (&stmt);
  if (error < 0)
    {
      REPL_SET_GENERIC_ERROR (error, stmt.err_buf.err_msg);

      GOTO_EXIT_ON_ERROR;
    }

  n = cci_get_int (&stmt, 1, &ind);
  if (stmt.err_buf.err_code != CCI_ER_NO_ERROR)
    {
      REPL_SET_GENERIC_ERROR (error, stmt.err_buf.err_msg);
      GOTO_EXIT_ON_ERROR;
    }

  assert (cci_fetch_next (&stmt) == CCI_ER_NO_MORE_DATA);

  cci_close_req_handle (&stmt);

  *num_appliers = n;

  assert (error == NO_ERROR);
  return NO_ERROR;

exit_on_error:
  assert (error != NO_ERROR);

  cci_close_req_handle (&stmt);

  return error;
}

/*
 * rpct_insert_log_applier() -
 *   return: error code
 *
 *   conn(in):
 *   ct_data(in):
 */
int
rpct_insert_log_applier (CCI_CONN * conn, CIRP_CT_LOG_APPLIER * ct_data)
{
  CIRP_CT_LOG_APPLIER tmp_rec;
  CIRP_REPL_ITEM *item = NULL;
  int error = NO_ERROR;
  LOG_LSA null_lsa;
  RECDES recdes;

  LSA_SET_NULL (&null_lsa);

  error = rp_new_repl_catalog_item (&item, &null_lsa);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  recdes.area_size = sizeof (CIRP_CT_LOG_APPLIER);
  recdes.data = (char *) (&tmp_rec);
  error = rpct_applier_to_catalog_item (&item->info.catalog, &recdes,
					ct_data);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  item->info.catalog.copyarea_op = LC_FLUSH_HA_CATALOG_APPLIER_UPDATE;

  assert (rp_is_valid_repl_item (item));
  error = cci_send_repl_data (conn, item, 1);
  if (error < 0)
    {
      REPL_SET_GENERIC_ERROR (error, "cci error(%d), msg:%s",
			      conn->err_buf.err_code, conn->err_buf.err_msg);

      GOTO_EXIT_ON_ERROR;
    }

  cirp_free_repl_item (item);
  item = NULL;

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "Invalid error code");
    }

  if (item != NULL)
    {
      cirp_free_repl_item (item);
      item = NULL;
    }

  return error;
}

/*
 * rpct_update_log_applier () -
 *   returns  : error code, if execution failed
 *              number of affected objects, if a success
 *
 *   conn(in):
 *   ct_data(in):
 */
int
rpct_update_log_applier (CCI_CONN * conn, CIRP_CT_LOG_APPLIER * ct_data)
{
  CIRP_CT_LOG_APPLIER tmp_rec;
  CIRP_REPL_ITEM *item = NULL;
  int error = NO_ERROR;
  LOG_LSA null_lsa;
  RECDES recdes;

  LSA_SET_NULL (&null_lsa);

  error = rp_new_repl_catalog_item (&item, &null_lsa);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  recdes.area_size = sizeof (CIRP_CT_LOG_APPLIER);
  recdes.data = (char *) (&tmp_rec);
  error = rpct_applier_to_catalog_item (&item->info.catalog, &recdes,
					ct_data);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  item->info.catalog.copyarea_op = LC_FLUSH_HA_CATALOG_APPLIER_UPDATE;

  assert (rp_is_valid_repl_item (item));
  error = cci_send_repl_data (conn, item, 1);
  if (error < 0)
    {
      REPL_SET_GENERIC_ERROR (error, "cci error(%d), msg:%s",
			      conn->err_buf.err_code, conn->err_buf.err_msg);

      GOTO_EXIT_ON_ERROR;
    }

  cirp_free_repl_item (item);
  item = NULL;

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (false);

      REPL_SET_GENERIC_ERROR (error, "Invalid error code");
    }

  if (item != NULL)
    {
      cirp_free_repl_item (item);
      item = NULL;
    }

  return error;
}

/*
 * cirp_make_select_query_with_pk ()
 *   return: query length
 *
 *   query_buf(out):
 *   buf_size(in):
 *   table(in):
 */
static int
cirp_make_select_query_with_pk (char *query_buf, size_t buf_size,
				CATCLS_TABLE * table, int num_pk_array,
				const char **pk_array)
{
  char condition[ONE_K];
  int i;
  size_t len;
  CATCLS_CONSTRAINT *pk = NULL;

  /* ex) SELECT a, b FROM foo WHERE PK=?; */

  len = snprintf (query_buf, buf_size, "SELECT ");
  for (i = 0; i < table->num_columns; i++)
    {
      if (i != 0)
	{
	  len = str_append (query_buf, len, ", ", buf_size - len);
	}
      len = str_append (query_buf, len, table->columns[i].name,
			buf_size - len);
    }

  for (i = 0; i < table->num_constraints; i++)
    {
      if (table->constraint[i].type == DB_CONSTRAINT_PRIMARY_KEY)
	{
	  pk = &table->constraint[i];
	  break;
	}
    }

  if (pk == NULL)
    {
      int error = NO_ERROR;

      assert (false);

      REPL_SET_GENERIC_ERROR (error, "not found primary key in %s",
			      table->name);

      return -1;
    }

  snprintf (condition, sizeof (condition), " FROM %s WHERE", table->name);
  len = str_append (query_buf, len, condition, buf_size - len);

#if !defined(NDEBUG)
  {
    for (i = 0; pk->atts[i] != NULL && i < MAX_INDEX_KEY_LIST_NUM + 1; i++);
    assert (i == num_pk_array);
  }
#endif

  for (i = 0; i < num_pk_array; i++)
    {
      if (i > MAX_INDEX_KEY_LIST_NUM)
	{
	  return -1;
	}

      if (i != 0)
	{
	  len = str_append (query_buf, len, " AND ", buf_size - len);
	}
      snprintf (condition, sizeof (condition), " %s = '%s'",
		pk->atts[i], pk_array[i]);
      len = str_append (query_buf, len, condition, buf_size - len);
    }

  len = str_append (query_buf, len, "; ", buf_size - len);

  return len;
}

/*
 * cirp_make_count_all_query_with_cond ()
 *   return: query length
 *
 *   query_buf(out):
 *   buf_size(in):
 *   table_name(in):
 *   cond(in):
 */
static int
cirp_make_count_all_query_with_cond (char *query_buf, size_t buf_size,
				     const char *table_name, const char *cond)
{
  int len;

  len = snprintf (query_buf, buf_size, "SELECT COUNT(*) FROM %s WHERE %s;",
		  table_name, cond);

  return len;
}

/*
 * cirp_execute_query () -
 *   return: number of records or error code
 *
 *   conn(in):
 *   stmt(out):
 *   query(in):
 */
int
cirp_execute_query (CCI_CONN * conn, CCI_STMT * stmt, char *query)
{
  int error = NO_ERROR;

  error = cci_prepare (conn, stmt, query, 0);
  if (error < 0)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = cci_execute (stmt, 0, 0);
  if (error < 0)
    {
      GOTO_EXIT_ON_ERROR;
    }

  return error;

exit_on_error:
  if (error == CCI_ER_DBMS)
    {
      error = stmt->err_buf.err_code;
    }

  return error;
}

/*
 * cirp_get_analyzer_from_result()-
 *    return: error code
 *
 *    log_analyzer(out):
 *    conn(in):
 *    query(in):
 */
static int
cirp_get_analyzer_from_result (CIRP_CT_LOG_ANALYZER * applier,
			       CCI_CONN * conn, char *query)
{
  int index;
  int ind;
  char *str_value;
  INT64 bi;
  CCI_STMT stmt;
  bool init_stmt = false;
  int error = NO_ERROR;

  error = cci_prepare (conn, &stmt, query, 0);
  if (error < 0)
    {
      GOTO_EXIT_ON_ERROR;
    }
  init_stmt = true;

  error = cci_execute (&stmt, 0, 0);
  if (error < 0)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = cci_fetch_next (&stmt);
  if (error < 0)
    {
      GOTO_EXIT_ON_ERROR;
    }

  index = 1;

  str_value = cci_get_string (&stmt, index, &ind);
  if (stmt.err_buf.err_code < 0)
    {
      GOTO_EXIT_ON_ERROR;
    }
  assert (ind >= 0 && str_value != NULL);
  if (rp_host_str_to_node_info (&applier->host_info, str_value) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  index++;

  bi = cci_get_bigint (&stmt, index, &ind);
  if (stmt.err_buf.err_code < 0)
    {
      GOTO_EXIT_ON_ERROR;
    }
  assert (ind >= 0);
  applier->required_lsa = int64_to_lsa (bi);
  index++;

  applier->source_applied_time = cci_get_bigint (&stmt, index, &ind);
  if (stmt.err_buf.err_code < 0)
    {
      GOTO_EXIT_ON_ERROR;
    }
  assert (ind >= 0);
  index++;

  applier->creation_time = cci_get_bigint (&stmt, index, &ind);
  if (stmt.err_buf.err_code < 0)
    {
      GOTO_EXIT_ON_ERROR;
    }
  assert (ind >= 0);
  index++;

  assert (cci_fetch_next (&stmt) == CCI_ER_NO_MORE_DATA);

  error = cci_close_req_handle (&stmt);
  if (error < 0)
    {
      GOTO_EXIT_ON_ERROR;
    }
  init_stmt = false;

  return NO_ERROR;

exit_on_error:
  if (error != CCI_ER_NO_MORE_DATA)
    {
      REPL_SET_GENERIC_ERROR (error, stmt.err_buf.err_msg);
    }

  if (init_stmt == true)
    {
      cci_close_req_handle (&stmt);
      init_stmt = false;
    }

  return error;
}

/*
 * cirp_get_applier_from_result()-
 *    return: error code
 *
 *    log_analyzer(out):
 *    conn(in):
 *    query(in):
 */
static int
cirp_get_applier_from_result (CIRP_CT_LOG_APPLIER * applier,
			      CCI_CONN * conn, char *query)
{
  int index;
  int ind;
  char *str_value;
  INT64 bi;
  CCI_STMT stmt;
  bool init_stmt = false;
  int error = NO_ERROR;

  error = cci_prepare (conn, &stmt, query, 0);
  if (error < 0)
    {
      GOTO_EXIT_ON_ERROR;
    }
  init_stmt = true;

  error = cci_execute (&stmt, 0, 0);
  if (error < 0)
    {
      GOTO_EXIT_ON_ERROR;
    }

  error = cci_fetch_next (&stmt);
  if (error < 0)
    {
      GOTO_EXIT_ON_ERROR;
    }

  index = 1;

  str_value = cci_get_string (&stmt, index, &ind);
  if (stmt.err_buf.err_code < 0)
    {
      GOTO_EXIT_ON_ERROR;
    }
  assert (ind >= 0 && str_value != NULL);
  if (rp_host_str_to_node_info (&applier->host_info, str_value) != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  index++;

  applier->id = cci_get_int (&stmt, index, &ind);
  if (stmt.err_buf.err_code < 0)
    {
      GOTO_EXIT_ON_ERROR;
    }
  assert (ind >= 0);
  index++;

  bi = cci_get_bigint (&stmt, index, &ind);
  if (stmt.err_buf.err_code < 0)
    {
      GOTO_EXIT_ON_ERROR;
    }
  assert (ind >= 0);
  applier->committed_lsa = int64_to_lsa (bi);
  index++;

  assert (cci_fetch_next (&stmt) == CCI_ER_NO_MORE_DATA);

  error = cci_close_req_handle (&stmt);
  if (error < 0)
    {
      GOTO_EXIT_ON_ERROR;
    }
  init_stmt = false;

  return NO_ERROR;

exit_on_error:
  REPL_SET_GENERIC_ERROR (error, stmt.err_buf.err_msg);

  if (init_stmt == true)
    {
      cci_close_req_handle (&stmt);
      init_stmt = false;
    }

  return error;
}

/*
 * rpct_applier_to_catalog_item ()-
 *   retrun: error code
 *
 *   catalog(out):
 *   recdes(in):
 *   ct_data(in):
 */
int
rpct_applier_to_catalog_item (RP_CATALOG_ITEM * catalog,
			      RECDES * recdes, CIRP_CT_LOG_APPLIER * ct_data)
{
  int error = NO_ERROR;

  if (recdes->area_size < (int) sizeof (CIRP_CT_LOG_APPLIER))
    {
      REPL_SET_GENERIC_ERROR (error, "Invalid RECDES");

      GOTO_EXIT_ON_ERROR;
    }

  catalog->class_name = strdup (CT_LOG_APPLIER_NAME);
  if (catalog->class_name == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
	      strlen (CT_LOG_APPLIER_NAME));

      GOTO_EXIT_ON_ERROR;
    }

  /* make pkey idxkey */
  catalog->key.size = 2;
  error = rp_make_repl_host_key (&catalog->key.vals[0], &ct_data->host_info);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  DB_MAKE_INT (&catalog->key.vals[1], ct_data->id);

  recdes->length = sizeof (CIRP_CT_LOG_APPLIER);
  memcpy (recdes->data, (char *) ct_data, recdes->length);
  recdes->type = REC_HOME;

  catalog->recdes = recdes;

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  assert (error != NO_ERROR);

  return error;
}

/*
 * rpct_analyzer_to_catalog_item ()-
 *   retrun: error code
 *
 *   catalog(out):
 *   recdes(in):
 *   ct_data(in):
 */
int
rpct_analyzer_to_catalog_item (RP_CATALOG_ITEM * catalog,
			       RECDES * recdes,
			       CIRP_CT_LOG_ANALYZER * ct_data)
{
  int error = NO_ERROR;

  if (recdes->area_size < (int) sizeof (CIRP_CT_LOG_ANALYZER))
    {
      REPL_SET_GENERIC_ERROR (error, "Invalid RECDES");

      GOTO_EXIT_ON_ERROR;
    }

  catalog->class_name = strdup (CT_LOG_ANALYZER_NAME);
  if (catalog->class_name == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
	      strlen (CT_LOG_ANALYZER_NAME));

      GOTO_EXIT_ON_ERROR;
    }

  /* make pkey idxkey */
  catalog->key.size = 1;
  error = rp_make_repl_host_key (&catalog->key.vals[0], &ct_data->host_info);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  recdes->length = sizeof (CIRP_CT_LOG_ANALYZER);
  memcpy (recdes->data, (char *) ct_data, recdes->length);
  recdes->type = REC_HOME;

  catalog->recdes = recdes;

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  assert (error != NO_ERROR);

  return error;
}
