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
 * rbl_copy_schema.c -
 *
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "db.h"
#include "dbi.h"
#include "authenticate.h"
#include "transform.h"
#include "object_accessor.h"
#include "schema_manager.h"
#include "parser.h"
#include "class_object.h"
#include "cas_cci_internal.h"
#include "rbl_conf.h"
#include "rbl_copy_schema.h"
#include "rbl_error_log.h"
#include "dbval.h"

#define GRANT_STR(fp, op, sql) \
  if (op == 1) { \
    if (add_comma) { fprintf (fp, ","); } \
    fprintf (fp, sql); add_comma = true; \
  }

#define GET_INT_VAL(qr, index, dbval, var) \
  do { \
    error = db_query_get_tuple_value (qr, index, dbval); \
    if (error != NO_ERROR) { \
      goto error_exit; \
    } \
    var = db_get_int (dbval); \
  } while (0)


static int
rbl_execute_ddl (CCI_CONN * conn, char *ddl_sql)
{
  CCI_STMT stmt;

  RBL_NOTICE (ARG_FILE_LINE, "Execute DDL:\n%s\n", ddl_sql);

  if (cci_prepare (conn, &stmt, ddl_sql, CCI_PREPARE_FROM_MIGRATOR) < 0)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR, conn->err_buf.err_code, conn->err_buf.err_msg);
      return RBL_CCI_ERROR;
    }

  if (cci_execute (&stmt, 0, 0) < 0)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR, stmt.err_buf.err_code, stmt.err_buf.err_msg);
      cci_close_req_handle (&stmt);
      return RBL_CCI_ERROR;
    }

  cci_close_req_handle (&stmt);

  return NO_ERROR;
}

/*
 * rbl_datetype_def_sql -
 *
 */
static void
rbl_datetype_def_sql (DB_DOMAIN * domain, FILE * fp)
{
  DB_TYPE type;
  PR_TYPE *prtype;
  int precision;
  int has_collation;

  type = TP_DOMAIN_TYPE (domain);
  prtype = PR_TYPE_FROM_ID (type);
  if (prtype == NULL)
    {
      return;
    }

  has_collation = 0;
  (void) fprintf (fp, "%s", prtype->name);

  switch (type)
    {
    case DB_TYPE_VARCHAR:
      has_collation = 1;
      /* fall through */
    case DB_TYPE_VARBIT:
      precision = db_domain_precision (domain);
      fprintf (fp, "(%d)", precision == TP_FLOATING_PRECISION_VALUE ? DB_MAX_STRING_LENGTH : precision);
      break;

    case DB_TYPE_NUMERIC:
      fprintf (fp, "(%d,%d)", db_domain_precision (domain), db_domain_scale (domain));
      break;

    default:
      break;
    }

  if (has_collation)
    {
      (void) fprintf (fp, " COLLATE %s", lang_get_collation_name (domain->collation_id));
    }
}

/*
 * rbl_column_def_sql -
 *
 */
static void
rbl_column_def_sql (DB_ATTRIBUTE * att, FILE * fp)
{
  DB_VALUE *default_value;
  const char *name;

  name = db_attribute_name (att);

  fprintf (fp, " [%s] ", name);

  rbl_datetype_def_sql (db_attribute_domain (att), fp);

  default_value = db_attribute_default (att);
  if ((default_value != NULL && !DB_IS_NULL (default_value)) || att->default_value.default_expr != DB_DEFAULT_NONE)
    {
      fprintf (fp, " DEFAULT ");

      switch (att->default_value.default_expr)
        {
        case DB_DEFAULT_SYSDATE:
          fprintf (fp, "SYS_DATE");
          break;
        case DB_DEFAULT_SYSDATETIME:
          fprintf (fp, "SYS_DATETIME");
          break;
        case DB_DEFAULT_UNIX_TIMESTAMP:
          fprintf (fp, "UNIX_TIMESTAMP()");
          break;
        case DB_DEFAULT_USER:
          fprintf (fp, "USER()");
          break;
        case DB_DEFAULT_CURR_USER:
          fprintf (fp, "CURRENT_USER");
          break;
        default:
          db_value_fprint (fp, default_value);
          break;
        }
    }

  if (db_attribute_is_non_null (att))
    {
      fprintf (fp, " NOT NULL");
    }
}

/*
 * rbl_index_def_sql -
 *
 */
static void
rbl_index_def_sql (MOP class_, FILE * fp)
{
  DB_CONSTRAINT *constraint;
  DB_ATTRIBUTE **atts, **att;
  int num_printed = 0;
  const char *name;
  const int *asc_desc;

  for (constraint = db_get_constraints (class_); constraint != NULL; constraint = db_constraint_next (constraint))
    {
      if (db_constraint_type (constraint) == DB_CONSTRAINT_NOT_NULL)
        {
          continue;
        }

      atts = db_constraint_attributes (constraint);

      if (num_printed > 0)
        {
          (void) fprintf (fp, ",\n");
        }

      if (constraint->type == SM_CONSTRAINT_PRIMARY_KEY)
        {
          (void) fprintf (fp, " CONSTRAINT [%s] PRIMARY KEY(", constraint->name);
        }
      else if (constraint->type == SM_CONSTRAINT_UNIQUE)
        {
          (void) fprintf (fp, " CONSTRAINT [%s] UNIQUE(", constraint->name);
        }
      else if (constraint->type == SM_CONSTRAINT_INDEX)
        {
          (void) fprintf (fp, " INDEX [%s] (", constraint->name);
        }

      asc_desc = db_constraint_asc_desc (constraint);

      for (att = atts; *att != NULL; att++)
        {
          name = db_attribute_name (*att);
          if (att != atts)
            {
              fprintf (fp, ", ");
            }

          fprintf (fp, "[%s]", name);

          if (asc_desc)
            {
              if (*asc_desc == 1)
                {
                  fprintf (fp, "%s", " DESC");
                }
              asc_desc++;
            }
        }
      (void) fprintf (fp, ")");

      ++num_printed;
    }
}

/*
 * rbl_query_specs_def_sql -
 *
 */
static void
rbl_query_specs_def_sql (MOP cls_mop, FILE * fp)
{
  DB_QUERY_SPEC *spec;

  spec = db_get_query_specs (cls_mop);
  RBL_ASSERT (spec != NULL);

  fprintf (fp, " AS %s\n", db_query_spec_string (spec));
}

/*
 * rbl_table_def_sql -
 *
 */
static int
rbl_table_def_sql (MOP cls_mop, CCI_CONN * conn, bool is_view)
{
  const char *name;
  SM_CLASS *class_ = NULL;
  DB_ATTRIBUTE *att, *shard_key_att;
  FILE *fp;
  size_t sizeloc;
  char *ptr;
  int error, c;

  fp = port_open_memstream (&ptr, &sizeloc);
  if (fp == NULL)
    {
      return NO_ERROR;
    }

  name = sm_class_name (cls_mop);
  if (is_view)
    {
      fprintf (fp, "CREATE VIEW [%s] (", name);
    }
  else
    {
      fprintf (fp, "CREATE %s TABLE [%s] (", sm_is_shard_table (cls_mop) ? "SHARD" : "GLOBAL", name);
    }

  if (au_fetch_class_force (cls_mop, &class_, S_LOCK) != NO_ERROR)
    {
      return ER_FAILED;
    }

  for (att = class_->ordered_attributes, c = 0; att != NULL; att = att->order_link, c++)
    {
      rbl_column_def_sql (att, fp);
      if (!is_view || c < class_->att_count - 1)
        {
          fprintf (fp, ",\n");
        }
    }

  rbl_index_def_sql (cls_mop, fp);

  fprintf (fp, ")");

  if (is_view)
    {
      rbl_query_specs_def_sql (cls_mop, fp);
    }
  else
    {
      if (sm_is_shard_table (cls_mop))
        {
          shard_key_att = classobj_find_shard_key_column (class_);

          fprintf (fp, " SHARD BY [%s]", shard_key_att->name);
        }
    }

  port_close_memstream (fp, &ptr, &sizeloc);
  RBL_ASSERT (ptr != NULL);
  error = rbl_execute_ddl (conn, ptr);
  free (ptr);

  return error;
}

/*
 * rbl_table_owner_sql -
 *
 */
static int
rbl_table_owner_sql (MOP class_, CCI_CONN * conn, bool is_view)
{
  const char *classname;
  MOP owner;
  DB_VALUE value;
  char sql[256];

  classname = sm_class_name (class_);
  if (classname == NULL)
    {
      RBL_ASSERT (0);
      return er_errid ();
    }

  owner = au_get_class_owner (class_);
  if (owner == NULL)
    {
      RBL_ASSERT (0);
      return er_errid ();
    }

  db_make_null (&value);
  if (obj_get (owner, "name", &value) != NO_ERROR)
    {
      RBL_ASSERT (0);
      return er_errid ();
    }

  RBL_ASSERT (DB_VALUE_TYPE (&value) == DB_TYPE_VARCHAR);
  RBL_ASSERT (DB_GET_STRING (&value) != NULL);

  if (is_view)
    {
      sprintf (sql, "ALTER VIEW [%s] OWNER TO %s", classname, DB_GET_STRING (&value));
    }
  else
    {
      sprintf (sql, "ALTER TABLE [%s] OWNER TO %s", classname, DB_GET_STRING (&value));
    }

  db_value_clear (&value);

  return rbl_execute_ddl (conn, sql);
}

static int
rbl_user_def_sql (CCI_CONN * conn)
{
  int error;
  DB_QUERY_RESULT *query_result = NULL;
  DB_QUERY_ERROR query_error;
  DB_VALUE name_val, pass_val;
  char buf[256];
  char *user_name;
  const char *password = "";

  DB_MAKE_NULL (&name_val);
  DB_MAKE_NULL (&pass_val);
  error = NO_ERROR;

  sprintf (buf, "SELECT name, password FROM [%s];", au_get_user_class_name ());

  error = db_execute (buf, &query_result, &query_error);
  if (error < 0)
    {
      goto error_exit;
    }

  while (db_query_next_tuple (query_result) == DB_CURSOR_SUCCESS)
    {
      error = db_query_get_tuple_value (query_result, 0, &name_val);
      if (error != NO_ERROR)
        {
          goto error_exit;
        }

      RBL_ASSERT (!DB_IS_NULL (&name_val));
      user_name = db_get_string (&name_val);
      if (user_name == NULL)
        {
          RBL_ASSERT (false);
          goto error_exit;
        }

      if (!strcasecmp (user_name, "SHARD_MANAGEMENT"))
        {
          db_value_clear (&name_val);
          continue;
        }

      error = db_query_get_tuple_value (query_result, 1, &pass_val);
      if (error != NO_ERROR)
        {
          goto error_exit;
        }

      if (!DB_IS_NULL (&pass_val))
        {
          password = db_get_string (&pass_val);
        }

      if (!strcasecmp (user_name, "DBA") || !strcasecmp (user_name, "PUBLIC"))
        {
          if (!DB_IS_NULL (&pass_val))
            {
              sprintf (buf, "ALTER USER [%s] PASSWORD '%s' ENCRYPT", user_name, password);
            }
          else
            {
              db_value_clear (&name_val);
              db_value_clear (&pass_val);
              continue;
            }
        }
      else
        {
          if (DB_IS_NULL (&pass_val))
            {
              sprintf (buf, "CREATE USER [%s]", user_name);
            }
          else
            {
              sprintf (buf, "CREATE USER [%s] PASSWORD '%s' ENCRYPT", user_name, password);
            }
        }

      error = rbl_execute_ddl (conn, buf);
      if (error != NO_ERROR)
        {
          goto error_exit;
        }

      db_value_clear (&name_val);
      db_value_clear (&pass_val);
    }

  db_query_end (query_result);

  return NO_ERROR;

error_exit:

  db_value_clear (&name_val);
  db_value_clear (&pass_val);
  db_query_end (query_result);

  return error;
}

static int
rbl_grant_def_sql (MOP cls_mop, CCI_CONN * conn)
{
  const char *classname;
  char buf[265];
  int error;
  DB_QUERY_RESULT *query_result = NULL;
  DB_QUERY_ERROR query_error;
  DB_VALUE user_val, grant_val;
  char *user_name;
  int sel, ins, upd, del, alt;
  bool add_comma = false;
  FILE *fp;
  size_t sizeloc;
  char *ptr;

  classname = sm_class_name (cls_mop);
  if (classname == NULL)
    {
      RBL_ASSERT (0);
      return ER_FAILED;
    }

  DB_MAKE_NULL (&user_val);
  error = NO_ERROR;

  sprintf (buf, "SELECT * FROM [%s] WHERE table_name = '%s';", CT_AUTH_NAME, classname);

  error = db_execute (buf, &query_result, &query_error);
  if (error < 0)
    {
      goto error_exit;
    }

  while (db_query_next_tuple (query_result) == DB_CURSOR_SUCCESS)
    {
      error = db_query_get_tuple_value (query_result, 0, &user_val);
      if (error != NO_ERROR)
        {
          goto error_exit;
        }

      RBL_ASSERT (!DB_IS_NULL (&user_val));
      user_name = db_get_string (&user_val);

      GET_INT_VAL (query_result, 2, &grant_val, sel);
      GET_INT_VAL (query_result, 3, &grant_val, ins);
      GET_INT_VAL (query_result, 4, &grant_val, upd);
      GET_INT_VAL (query_result, 5, &grant_val, del);
      GET_INT_VAL (query_result, 6, &grant_val, alt);

      add_comma = false;

      fp = port_open_memstream (&ptr, &sizeloc);
      if (fp == NULL)
        {
          goto error_exit;
        }

      fprintf (fp, "GRANT");
      GRANT_STR (fp, sel, " SELECT");
      GRANT_STR (fp, ins, " INSERT");
      GRANT_STR (fp, upd, " UPDATE");
      GRANT_STR (fp, del, " DELETE");
      GRANT_STR (fp, alt, " ALTER");
      fprintf (fp, " ON [%s] TO [%s]", classname, user_name);

      port_close_memstream (fp, &ptr, &sizeloc);
      RBL_ASSERT (ptr != NULL);
      error = rbl_execute_ddl (conn, ptr);
      free (ptr);

      if (error != NO_ERROR)
        {
          goto error_exit;
        }

      db_value_clear (&user_val);
    }

  db_query_end (query_result);
  return NO_ERROR;

error_exit:

  db_value_clear (&user_val);
  db_query_end (query_result);
  return error;
}

static bool
rbl_is_shard_mgmt_table (MOP cls_mop)
{
  const char *t_name;

  t_name = sm_class_name (cls_mop);

  if (strcasecmp (t_name, "shard_db") == 0
      || strcasecmp (t_name, "shard_node") == 0
      || strcasecmp (t_name, "shard_groupid") == 0 || strcasecmp (t_name, "shard_migration") == 0)
    {
      return true;
    }

  return false;
}

/*
 * rbl_copy_schema -
 */
int
rbl_copy_schema (void)
{
  DB_OBJLIST *classes, *t;
  CCI_CONN *conn;
  int error = NO_ERROR;

  conn = rbl_conf_get_destdb_conn (RBL_COPY);
  RBL_ASSERT (conn != NULL);

  error = rbl_user_def_sql (conn);
  if (error != NO_ERROR)
    {
      return error;
    }

  classes = sm_fetch_all_classes (S_LOCK);
  if (classes == NULL)
    {
      return er_errid ();
    }

  /* table */
  for (t = classes; t != NULL; t = t->next)
    {
      if (sm_is_system_table (t->op) || db_is_vclass (t->op) || rbl_is_shard_mgmt_table (t->op))
        {
          continue;
        }

      error = rbl_table_def_sql (t->op, conn, false);
      if (error != NO_ERROR)
        {
          goto error_exit;
        }

      error = rbl_table_owner_sql (t->op, conn, false);
      if (error != NO_ERROR)
        {
          goto error_exit;
        }

      error = rbl_grant_def_sql (t->op, conn);
      if (error != NO_ERROR)
        {
          goto error_exit;
        }
    }

  /* view */
  for (t = classes; t != NULL; t = t->next)
    {
      if (sm_is_system_table (t->op) || !db_is_vclass (t->op))
        {
          continue;
        }

      error = rbl_table_def_sql (t->op, conn, true);
      if (error != NO_ERROR)
        {
          goto error_exit;
        }

      error = rbl_table_owner_sql (t->op, conn, true);
      if (error != NO_ERROR)
        {
          goto error_exit;
        }

      error = rbl_grant_def_sql (t->op, conn);
      if (error != NO_ERROR)
        {
          goto error_exit;
        }
    }

  error = cci_block_global_dml (conn, true);
  if (error < 0)
    {
      RBL_ERROR (ARG_FILE_LINE, RBL_CCI_ERROR, conn->err_buf.err_code, conn->err_buf.err_msg);
      goto error_exit;
    }
  cci_end_tran (conn, CCI_TRAN_COMMIT);

error_exit:

  db_objlist_free (classes);
  return error;
}
