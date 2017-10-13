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
 * shard_catalog.c -
 */

#ident "$Id$"

#include "db.h"
#include "transform.h"
#include "schema_manager.h"
#include "authenticate.h"
#include "shard_catalog.h"

#define SHARD_CT_QUERY_BUF_SIZE		(2048)
#define SHARD_CT_GID_SKEY_IN_VALUE_CNT   2

static int
shard_update_query_execute_with_values (const char *sql, int arg_count,
					DB_VALUE * vals);

#if defined (ENABLE_UNUSED_FUNCTION)
int
shard_get_ct_shard_gid_skey_info (const int gid, const char *skey,
				  SHARD_CT_SHARD_GID_SKEY_INFO *
				  shard_gid_skey_info)
{
#define SHARD_CT_IN_VALUE_CNT	2
#define SHARD_CT_OUT_VALUE_CNT	2
  int error = NO_ERROR;
  int in_value_idx, out_value_idx;
  int i, col_cnt;
  int result_cnt;

  DB_VALUE in_value[SHARD_CT_IN_VALUE_CNT];
  DB_VALUE out_value[SHARD_CT_OUT_VALUE_CNT];

  char query_buf[SHARD_CT_QUERY_BUF_SIZE];
  DB_QUERY_ERROR query_error;
  DB_QUERY_RESULT *result = NULL;

  if (sm_find_class (CT_SHARD_GID_SKEY_INFO_NAME) == NULL)
    {
      return er_errid ();
    }

  snprintf (query_buf, sizeof (query_buf), "SELECT "	/* SELECT */
	    " gid, "		/* 1 */
	    " skey "		/* 2 */
	    " FROM %s "
	    " WHERE gid = ? and skey = ? ; ", CT_SHARD_GID_SKEY_INFO_NAME);

  in_value_idx = 0;
  db_make_int (&in_value[in_value_idx++], gid);
  db_make_varchar (&in_value[in_value_idx++], SHARD_SKEY_LENGTH,
		   (char *) skey, strlen (skey), LANG_SYS_COLLATION);

  assert_release (in_value_idx == SHARD_CT_IN_VALUE_CNT);

  result_cnt = db_execute_with_values (query_buf, &result, &query_error,
				       in_value_idx, &in_value[0]);

  if (result_cnt > 0)
    {
      int pos;
      char *skey_str;
      int skey_len;
      pos = db_query_first_tuple (result);

      switch (pos)
	{
	case DB_CURSOR_SUCCESS:
	  col_cnt = db_query_column_count (result);
	  assert_release (col_cnt == SHARD_CT_OUT_VALUE_CNT);

	  error =
	    db_query_get_tuple_valuelist (result, SHARD_CT_OUT_VALUE_CNT,
					  out_value);

	  if (error != NO_ERROR)
	    {
	      break;
	    }

	  out_value_idx = 0;

	  shard_gid_skey_info->gid =
	    DB_GET_INTEGER (&out_value[out_value_idx++]);
	  skey_str = DB_GET_STRING (&out_value[out_value_idx++]);

	  skey_len = (skey_str != NULL) ? strlen (skey_str) : 0;
	  strncpy (shard_gid_skey_info->skey, skey_str,
		   MIN (skey_len, SHARD_SKEY_LENGTH));
	  shard_gid_skey_info->skey[MIN (skey_len, SHARD_SKEY_LENGTH - 1)] =
	    '\0';


	  assert_release (out_value_idx == SHARD_CT_OUT_VALUE_CNT);

	  for (i = 0; i < SHARD_CT_OUT_VALUE_CNT; i++)
	    {
	      db_value_clear (&out_value[i]);
	    }
	  break;

	case DB_CURSOR_END:
	default:
	  error = ER_FAILED;
	  break;
	}
    }
  else if (result_cnt == 0)
    {
      /* maybe, should be return NO DATA */
      error = NO_DATA;
    }
  else
    {
      error = ER_FAILED;
    }

  db_query_end (result);

  for (i = 0; i < in_value_idx; i++)
    {
      db_value_clear (&in_value[i]);
    }

  return error;
}
#endif

int
shard_delete_ct_shard_gid_skey_info_with_gid (const int gid)
{
#define SHARD_CT_GID_IN_VALUE_CNT 1
  int in_value_idx = 0;
  int error = NO_ERROR;
  int i;
  DB_VALUE in_value[SHARD_CT_GID_IN_VALUE_CNT];
  char query_buf[SHARD_CT_QUERY_BUF_SIZE];

  snprintf (query_buf, sizeof (query_buf), "DELETE FROM %s "	/* DELETE */
	    " WHERE gid = ?; ", CT_SHARD_GID_SKEY_INFO_NAME);


  db_make_int (&in_value[in_value_idx++], gid);
  assert_release (in_value_idx == SHARD_CT_GID_IN_VALUE_CNT);

  error =
    shard_update_query_execute_with_values (query_buf, in_value_idx,
					    &in_value[0]);


  for (i = 0; i < in_value_idx; i++)
    {
      db_value_clear (&in_value[i]);
    }

  if (error < 0)
    {
      return error;
    }

  return error;
}

#if defined (ENABLE_UNUSED_FUNCTION)
int
shard_insert_ct_shard_gid_skey_info (SHARD_CT_SHARD_GID_SKEY_INFO *
				     shard_gid_skey_info)
{
  int i;
  int error = NO_ERROR;
  int in_value_idx = 0;
  DB_VALUE in_value[SHARD_CT_GID_SKEY_IN_VALUE_CNT];
  char query_buf[SHARD_CT_QUERY_BUF_SIZE];


  snprintf (query_buf, sizeof (query_buf), "INSERT INTO %s "	/* INSERT */
	    "( gid, "		/* 1 */
	    " skey ) "		/* 2 */
	    " VALUES ( ?, "	/* 1. gid */
	    " ? "		/* 2. skey */
	    " ) ;", CT_SHARD_GID_SKEY_INFO_NAME);

  /* 1. gid */
  db_make_int (&in_value[in_value_idx++], shard_gid_skey_info->gid);
  db_make_varchar (&in_value[in_value_idx++], SHARD_SKEY_LENGTH,
		   shard_gid_skey_info->skey,
		   strlen (shard_gid_skey_info->skey), LANG_SYS_COLLATION);

  assert_release (in_value_idx == SHARD_CT_GID_SKEY_IN_VALUE_CNT);

  error =
    shard_update_query_execute_with_values (query_buf, in_value_idx,
					    &in_value[0]);
  for (i = 0; i < in_value_idx; i++)
    {
      db_value_clear (&in_value[i]);
    }

  if (error < 0)
    {
      return error;
    }

  return error;
}

int
shard_get_ct_shard_gid_removed_info (const int gid,
				     SHARD_CT_SHARD_GID_REMOVED_INFO *
				     shard_gid_removed_info)
{
#define SHARD_CT_GID_IN_VALUE_CNT	1
#define SHARD_CT_GID_RMDT_OUT_VALUE_CNT	2
  int error = NO_ERROR;
  int in_value_idx, out_value_idx;
  int i, col_cnt;
  int result_cnt;

  DB_VALUE in_value[SHARD_CT_GID_IN_VALUE_CNT];
  DB_VALUE out_value[SHARD_CT_GID_RMDT_OUT_VALUE_CNT];

  char query_buf[SHARD_CT_QUERY_BUF_SIZE];
  DB_QUERY_ERROR query_error;
  DB_QUERY_RESULT *result = NULL;

  if (sm_find_class (CT_SHARD_GID_REMOVED_INFO_NAME) == NULL)
    {
      return er_errid ();
    }

  snprintf (query_buf, sizeof (query_buf), "SELECT "	/* SELECT */
	    " gid, "		/* 1 */
	    " rem_dt "		/* 2 */
	    " FROM %s " " WHERE gid = ? ; ", CT_SHARD_GID_REMOVED_INFO_NAME);

  in_value_idx = 0;
  db_make_int (&in_value[in_value_idx++], gid);


  assert_release (in_value_idx == SHARD_CT_GID_IN_VALUE_CNT);

  result_cnt = db_execute_with_values (query_buf, &result, &query_error,
				       in_value_idx, &in_value[0]);


  if (result_cnt > 0)
    {
      int pos;
      DB_DATETIME *db_time;

      pos = db_query_first_tuple (result);

      switch (pos)
	{
	case DB_CURSOR_SUCCESS:
	  col_cnt = db_query_column_count (result);
	  assert_release (col_cnt == SHARD_CT_GID_RMDT_OUT_VALUE_CNT);

	  error =
	    db_query_get_tuple_valuelist (result,
					  SHARD_CT_GID_RMDT_OUT_VALUE_CNT,
					  out_value);

	  if (error != NO_ERROR)
	    {
	      break;
	    }

	  out_value_idx = 0;

	  shard_gid_removed_info->gid =
	    DB_GET_INTEGER (&out_value[out_value_idx++]);
	  db_time = DB_GET_DATETIME (&out_value[out_value_idx++]);
	  shard_gid_removed_info->rem_dt.date = db_time->date;
	  shard_gid_removed_info->rem_dt.time = db_time->time;

	  assert_release (out_value_idx == SHARD_CT_GID_RMDT_OUT_VALUE_CNT);

	  for (i = 0; i < SHARD_CT_GID_RMDT_OUT_VALUE_CNT; i++)
	    {
	      db_value_clear (&out_value[i]);
	    }
	  break;

	case DB_CURSOR_END:
	default:
	  error = ER_FAILED;
	  break;
	}
    }
  else if (result_cnt == 0)
    {
      /* maybe, should be return NO DATA */
      error = NO_DATA;
    }
  else
    {
      error = ER_FAILED;
    }

  db_query_end (result);

  for (i = 0; i < in_value_idx; i++)
    {
      db_value_clear (&in_value[i]);
    }

  return error;
}
#endif

int
shard_delete_ct_shard_gid_removed_info_with_gid (const int gid)
{
  int in_value_idx = 0;
  int error = NO_ERROR;
  int i;
  DB_VALUE in_value[SHARD_CT_GID_IN_VALUE_CNT];
  char query_buf[SHARD_CT_QUERY_BUF_SIZE];

  snprintf (query_buf, sizeof (query_buf), "DELETE FROM %s "	/* DELETE */
	    " WHERE gid = ?; ", CT_SHARD_GID_REMOVED_INFO_NAME);

  db_make_int (&in_value[in_value_idx++], gid);
  assert_release (in_value_idx == SHARD_CT_GID_IN_VALUE_CNT);

  error =
    shard_update_query_execute_with_values (query_buf, in_value_idx,
					    &in_value[0]);


  for (i = 0; i < in_value_idx; i++)
    {
      db_value_clear (&in_value[i]);
    }

  if (error < 0)
    {
      return error;
    }

  return error;
}


int
shard_insert_ct_shard_gid_removed_info (const int gid)
{
#define SHARD_CT_GID_REMOVED_IN_VALUE_CNT	 1

  int i;
  int error = NO_ERROR;
  int in_value_idx = 0;
  DB_VALUE in_value[SHARD_CT_GID_SKEY_IN_VALUE_CNT];
  char query_buf[SHARD_CT_QUERY_BUF_SIZE];


  snprintf (query_buf, sizeof (query_buf), "INSERT INTO %s "	/* INSERT */
	    "( gid, "		/* 1 */
	    " rem_dt ) "	/* 2 */
	    " VALUES ( ?, "	/* 1. gid */
	    " SYS_DATETIME "	/* 2. skey */
	    " ) ;", CT_SHARD_GID_REMOVED_INFO_NAME);

  /* 1. gid */
  db_make_int (&in_value[in_value_idx++], gid);

  assert_release (in_value_idx == SHARD_CT_GID_REMOVED_IN_VALUE_CNT);

  error =
    shard_update_query_execute_with_values (query_buf, in_value_idx,
					    &in_value[0]);
  for (i = 0; i < in_value_idx; i++)
    {
      db_value_clear (&in_value[i]);
    }

  if (error < 0)
    {
      return error;
    }

  return error;
}

static int
shard_update_query_execute_with_values (const char *sql, int arg_count,
					DB_VALUE * vals)
{
  int error = NO_ERROR, au_save;
  DB_QUERY_RESULT *result;
  DB_QUERY_ERROR query_error;

  AU_DISABLE (au_save);

  error = db_execute_with_values (sql, &result, &query_error,
				  arg_count, vals);
  if (error >= 0)
    {
      error = db_query_end (result);
    }

  AU_RESTORE (au_save);

  return error;
}
