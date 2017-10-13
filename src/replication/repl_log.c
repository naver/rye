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
 * repl_log.c -
 */

#ident "$Id$"

#include <stdio.h>
#include <assert.h>
#include "repl_log.h"
#include "object_primitive.h"
#include "object_print.h"
#include "heap_file.h"

/*
 * EXTERN TO ALL SERVER RECOVERY FUNCTION CODED SOMEWHERE ELSE
 */

#define 	REPL_LOG_IS_NOT_EXISTS(tran_index)                            \
               (log_Gl.trantable.all_tdes[(tran_index)]->num_repl_records == 0)
#define 	REPL_LOG_IS_FULL(tran_index)                                  \
               (log_Gl.trantable.all_tdes[(tran_index)]->num_repl_records     \
                 == log_Gl.trantable.all_tdes[(tran_index)]->cur_repl_record+1)

static const int REPL_LOG_INFO_ALLOC_SIZE = 100;

#if defined(SERVER_MODE) || defined(SA_MODE)
static int repl_log_info_alloc (LOG_TDES * tdes, int arr_size,
				bool need_realloc);
#endif /* SERVER_MODE || SA_MODE */

#if defined(SERVER_MODE) || defined(SA_MODE)

/*
 * log_repl_data_dump - dump the "DATA INSERT/UPDATE/DELETE" replication log
 *
 * return:
 *
 *   length(in): length of the data
 *   data(in): log data
 */
void
log_repl_data_dump (FILE * out_fp, UNUSED_ARG int length, void *data)
{
  char *ptr;
  const char *class_name;
  int group_id;
  DB_IDXKEY key;
  char buf[ONE_K];

  DB_IDXKEY_MAKE_NULL (&key);

  ptr = data;
  ptr = or_unpack_int (ptr, &group_id);
  assert (ptr != NULL);
  ptr = or_unpack_string_nocopy (ptr, &class_name);
  assert (ptr != NULL);
  ptr = or_unpack_db_idxkey (ptr, &key);
  assert (ptr != NULL);

  help_sprint_idxkey (&key, buf, sizeof (buf) - 1);
  fprintf (out_fp, "G[%d] C[%s] K[%s]\n", group_id, class_name, buf);

  db_idxkey_clear (&key);
}

/*
 * log_repl_data_dump - dump the "SCHEMA" replication log
 *
 * return:
 *
 *   length(in): length of the data
 *   data(in): log data
 */
void
log_repl_schema_dump (FILE * out_fp, UNUSED_ARG int length, void *data)
{
  char *ptr;
  int statement_type, ddl_type;
  const char *class_name;
  const char *sql;

  ptr = data;
  ptr = or_unpack_int (ptr, &statement_type);
  ptr = or_unpack_int (ptr, &ddl_type);
  ptr = or_unpack_string_nocopy (ptr, &class_name);
  ptr = or_unpack_string_nocopy (ptr, &sql);

  fprintf (out_fp, "DDL [%d] C[%s] S[%s]\n", ddl_type, class_name, sql);
}

/*
 * repl_log_info_alloc - log info area allocation
 *
 * return: Error Code
 *
 *   tdes(in): transaction descriptor
 *   arr_size(in): array size to be allocated
 *   need_realloc(in): 0-> initial allocation (malloc), otherwise "realloc"
 *
 * NOTE:This function allocates the memory for the log info area of the
 *       target transaction. It is called when the transaction tries to to
 *   insert/update/delete operation. If the transaction do read operation
 *   only, no memory is allocated.
 *
 *   The allocation size is defined by a constant - REPL_LOG_INFO_ALLOC_SIZE
 *   We need to set the size for the user request ?
 */
static int
repl_log_info_alloc (LOG_TDES * tdes, int arr_size, bool need_realloc)
{
  int i = 0, k;
  int error = NO_ERROR;

  if (need_realloc == false)
    {
      i = arr_size * DB_SIZEOF (LOG_REPL_RECORD);
      tdes->repl_records = (LOG_REPL_RECORD *) malloc (i);
      if (tdes->repl_records == NULL)
	{
	  error = ER_REPL_ERROR;
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_REPL_ERROR, 1,
		  "can't allocate memory");
	  return error;
	}
      tdes->num_repl_records = arr_size;
      k = 0;
    }
  else
    {
      i = tdes->num_repl_records + arr_size;
      tdes->repl_records = (LOG_REPL_RECORD *)
	realloc (tdes->repl_records, i * DB_SIZEOF (LOG_REPL_RECORD));
      if (tdes->repl_records == NULL)
	{
	  error = ER_REPL_ERROR;
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_REPL_ERROR, 1,
		  "can't allocate memory");
	  return error;
	}
      k = tdes->num_repl_records;
      tdes->num_repl_records = i;
    }

  for (i = k; i < tdes->num_repl_records; i++)
    {
      tdes->repl_records[i].repl_data = NULL;
    }

  return error;
}

/*
 * repl_add_update_lsa - update the LSA of the target transaction lo
 *
 * return: NO_ERROR or error code
 *
 *   inst_oid(in): OID of the instance
 *
 * NOTE:For update operation, the server does "Heap stuff" after "Index Stuff".
 *     In order to reduce the cost of replication log, we generates a
 *     replication log at the point of indexing  processing step.
 *     (During index processing, the primary key value is fetched ..)
 *     After Heap operation, we have to set the target LSA into the
 *     replication log.
 *
 *     For update case, this function is called by locator_update_force().
 *     In the case of insert/delete cases, when the replication log info. is
 *     generated, the server already has the target transaction log(HEAP_INSERT
 *     or HEAP_DELETE).
 *     But, for the udpate case, the server doesn't has the target log when
 *     it generates the replication log. So, the server has to find out the
 *     location of replication record and match with the target transaction
 *     log after heap_update(). This is done by locator_update_force().
 */
int
repl_add_update_lsa (THREAD_ENTRY * thread_p, const OID * inst_oid)
{
  int tran_index;
  LOG_TDES *tdes;
  LOG_REPL_RECORD *repl_rec;
  int i;
  bool find = false;
  int error = NO_ERROR;

  tran_index = logtb_get_current_tran_index (thread_p);

  tdes = LOG_FIND_TDES (tran_index);
  if (tdes == NULL)
    {
      assert (false);
      return ER_FAILED;
    }

  /* If suppress_replication flag is set, do not write replication log. */
  if (tdes->suppress_replication != 0)
    {
      return NO_ERROR;
    }

  for (i = tdes->cur_repl_record - 1; i >= 0; i--)
    {
      repl_rec = (LOG_REPL_RECORD *) (&(tdes->repl_records[i]));
      if (OID_EQ (&repl_rec->inst_oid, inst_oid)
	  && !LSA_ISNULL (&tdes->repl_update_lsa))
	{
	  assert (repl_rec->rcvindex == RVREPL_DATA_UPDATE);
	  if (repl_rec->rcvindex == RVREPL_DATA_UPDATE)
	    {
	      LSA_COPY (&repl_rec->lsa, &tdes->repl_update_lsa);
	      LSA_SET_NULL (&tdes->repl_update_lsa);
	      LSA_SET_NULL (&tdes->repl_insert_lsa);
	      find = true;
	      break;
	    }
	}
    }

  if (find == false)
    {
      er_log_debug (ARG_FILE_LINE, "can't find out the UPDATE LSA");
    }

  return error;
}

/*
 * repl_log_insert - insert a replication info to the transaction descriptor
 *
 * return: NO_ERROR or error code
 *
 *   class_oid(in): OID of the class
 *   inst_oid(in): OID of the instance
 *   log_type(in): log type (DATA or SCHEMA)
 *   rcvindex(in): recovery index (INSERT or DELETE or UPDATE)
 *   key(in): Primary Key value
 *
 * NOTE:insert a replication log info to the transaction descriptor (tdes)
 */
int
repl_log_insert (THREAD_ENTRY * thread_p, const OID * class_oid,
		 const OID * inst_oid, LOG_RECTYPE log_type,
		 LOG_RCVINDEX rcvindex, DB_IDXKEY * key)
{
  int tran_index;
  LOG_TDES *tdes;
  LOG_REPL_RECORD *repl_rec;
  char *class_name;
  char *ptr;
  int error = NO_ERROR;
  int str_tot_len, idxkey_len, strlen;

  assert (key != NULL);

  tran_index = logtb_get_current_tran_index (thread_p);
  tdes = LOG_FIND_TDES (tran_index);
  if (tdes == NULL)
    {
      assert (false);
      return ER_FAILED;
    }

  /* If suppress_replication flag is set, do not write replication log. */
  if (tdes->suppress_replication != 0)
    {
      return NO_ERROR;
    }

  /* check the replication log array status, if we need to alloc? */
  if (REPL_LOG_IS_NOT_EXISTS (tran_index)
      && ((error = repl_log_info_alloc (tdes, REPL_LOG_INFO_ALLOC_SIZE,
					false)) != NO_ERROR))
    {
      return error;
    }
  /* the replication log array is full? re-alloc? */
  else if (REPL_LOG_IS_FULL (tran_index)
	   && (error = repl_log_info_alloc (tdes, REPL_LOG_INFO_ALLOC_SIZE,
					    true)) != NO_ERROR)
    {
      return error;
    }

  repl_rec = (LOG_REPL_RECORD *) (&tdes->repl_records[tdes->cur_repl_record]);
  repl_rec->repl_type = log_type;

  repl_rec->rcvindex = rcvindex;
  COPY_OID (&repl_rec->inst_oid, inst_oid);

  /* make the common info for the data replication */
  if (log_type == LOG_REPLICATION_DATA)
    {
      class_name = heap_get_class_name (thread_p, class_oid);
      if (class_name == NULL)
	{
	  error = er_errid ();
	  if (error == NO_ERROR)
	    {
	      error = ER_REPL_ERROR;
	      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_REPL_ERROR, 1,
		      "can't get class_name");
	    }
	  return error;
	}

      str_tot_len = or_packed_string_length (class_name, &strlen);
      idxkey_len = OR_IDXKEY_ALIGNED_SIZE (key);
      repl_rec->length = (OR_INT_SIZE	/* group id */
			  + str_tot_len + idxkey_len);

      ptr = (char *) malloc (repl_rec->length);
      if (ptr == NULL)
	{
	  assert (false);
	  free_and_init (class_name);
	  error = ER_REPL_ERROR;
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_REPL_ERROR, 1,
		  "can't allocate memory");
	  return error;
	}

#if !defined(NDEBUG)
      /* suppress valgrind UMW error */
      memset (ptr, 0, repl_rec->length);
#endif

      repl_rec->repl_data = ptr;

      ptr = or_pack_int (ptr, inst_oid->groupid);
      ptr = or_pack_string_with_length (ptr, class_name, strlen);
      if (ptr == NULL)
	{
	  assert (false);
	  free_and_init (class_name);
	  error = ER_REPL_ERROR;
	  return error;
	}
      ptr = or_pack_db_idxkey (ptr, key);
      if (ptr == NULL)
	{
	  assert (false);
	  free_and_init (class_name);
	  error = ER_REPL_ERROR;
	  return error;
	}

      free_and_init (class_name);
    }
  else
    {
      repl_rec->repl_data = NULL;
      repl_rec->length = 0;
    }
  repl_rec->must_flush = LOG_REPL_COMMIT_NEED_FLUSH;

  switch (rcvindex)
    {
    case RVREPL_DATA_INSERT:
      if (!LSA_ISNULL (&tdes->repl_insert_lsa))
	{
	  LSA_COPY (&repl_rec->lsa, &tdes->repl_insert_lsa);
	  LSA_SET_NULL (&tdes->repl_insert_lsa);
	  LSA_SET_NULL (&tdes->repl_update_lsa);
	}
      break;
    case RVREPL_DATA_UPDATE:
      /*
       * for the update case, this function is called before the heap
       * file update, so we don't need to LSA for update log here.
       */
      if (LSA_ISNULL (&tdes->last_lsa))
	{
	  LSA_COPY (&repl_rec->lsa, &log_Gl.prior_info.prior_lsa);
	}
      else
	{
	  LSA_COPY (&repl_rec->lsa, &tdes->last_lsa);
	}
      break;
    case RVREPL_DATA_DELETE:
      /*
       * for the delete case, we don't need to find out the target
       * LSA. Delete is operation is possible without "After Image"
       */
      if (LSA_ISNULL (&tdes->last_lsa))
	{
	  LSA_COPY (&repl_rec->lsa, &log_Gl.prior_info.prior_lsa);
	}
      else
	{
	  LSA_COPY (&repl_rec->lsa, &tdes->last_lsa);
	}
      break;
    default:
      break;
    }
  tdes->cur_repl_record++;

  return error;
}

/*
 * repl_log_insert_schema - insert a replication info(schema) to the
 *                          transaction descriptor
 *
 * return: NO_ERROR or error code
 *
 *   repl_schema(in):
 *
 * NOTE:insert a replication log info(schema) to the transaction
 *      descriptor (tdes)
 */
int
repl_log_insert_schema (THREAD_ENTRY * thread_p,
			REPL_INFO_SCHEMA * repl_schema)
{
  int tran_index;
  LOG_TDES *tdes;
  LOG_REPL_RECORD *repl_rec;
  char *ptr;
  int error = NO_ERROR, strlen1, strlen2, strlen3;

  tran_index = logtb_get_current_tran_index (thread_p);
  tdes = LOG_FIND_TDES (tran_index);
  if (tdes == NULL)
    {
      assert (false);
      return ER_FAILED;
    }

  /* If suppress_replication flag is set, do not write replication log. */
  if (tdes->suppress_replication != 0)
    {
      return NO_ERROR;
    }

  /* check the replication log array status, if we need to alloc? */
  if (REPL_LOG_IS_NOT_EXISTS (tran_index)
      && ((error = repl_log_info_alloc (tdes, REPL_LOG_INFO_ALLOC_SIZE,
					false)) != NO_ERROR))
    {
      return error;
    }
  /* the replication log array is full? re-alloc? */
  else if (REPL_LOG_IS_FULL (tran_index)
	   && (error = repl_log_info_alloc (tdes, REPL_LOG_INFO_ALLOC_SIZE,
					    true)) != NO_ERROR)
    {
      return error;
    }

  repl_rec = (LOG_REPL_RECORD *) & tdes->repl_records[tdes->cur_repl_record];
  repl_rec->repl_type = LOG_REPLICATION_SCHEMA;
  repl_rec->rcvindex = RVREPL_SCHEMA;
  repl_rec->must_flush = LOG_REPL_COMMIT_NEED_FLUSH;
  OID_SET_NULL (&repl_rec->inst_oid);

  /* make the common info for the schema replication */
  repl_rec->length = OR_INT_SIZE	/* REPL_INFO_SCHEMA.statement_type */
    + OR_INT_SIZE		/* REPL_INFO_SCHEMA.online_ddl_type */
    + or_packed_string_length (repl_schema->name, &strlen1)
    + or_packed_string_length (repl_schema->ddl, &strlen2)
    + or_packed_string_length (repl_schema->db_user, &strlen3);
  repl_rec->repl_data = (char *) malloc (repl_rec->length);
  if (repl_rec->repl_data == NULL)
    {
      error = ER_REPL_ERROR;
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_REPL_ERROR, 1,
	      "can't allocate memory");
      return error;
    }
  ptr = repl_rec->repl_data;
  ptr = or_pack_int (ptr, repl_schema->statement_type);
  ptr = or_pack_int (ptr, repl_schema->online_ddl_type);
  ptr = or_pack_string_with_length (ptr, repl_schema->name, strlen1);
  ptr = or_pack_string_with_length (ptr, repl_schema->ddl, strlen2);
  ptr = or_pack_string_with_length (ptr, repl_schema->db_user, strlen3);

  er_log_debug (ARG_FILE_LINE, "repl_log_insert_schema:"
		" repl_schema { type %d, ddl_type %d, name %s, ddl %s, user %s }\n",
		repl_schema->statement_type, repl_schema->online_ddl_type,
		repl_schema->name, repl_schema->ddl, repl_schema->db_user);
  LSA_COPY (&repl_rec->lsa, &tdes->last_lsa);

  tdes->cur_repl_record++;

  return error;
}

/*
 * repl_log_abort_after_lsa -
 *
 * return:
 *
 *   tdes (in) :
 *   start_lsa (in) :
 *
 */
int
repl_log_abort_after_lsa (LOG_TDES * tdes, LOG_LSA * start_lsa)
{
  LOG_REPL_RECORD *repl_rec_arr;
  int i;

  repl_rec_arr = tdes->repl_records;
  for (i = 0; i < tdes->cur_repl_record; i++)
    {
      if (LSA_GT (&repl_rec_arr[i].lsa, start_lsa))
	{
	  repl_rec_arr[i].must_flush = LOG_REPL_DONT_NEED_FLUSH;
	}
    }

  return NO_ERROR;
}

#endif /* SERVER_MODE || SA_MODE */
