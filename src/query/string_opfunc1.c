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
 * string_opfunc.c - Routines that manipulate arbitrary strings
 */

#ident "$Id$"

/* This includes bit strings, character strings, and national character strings
 */

#include "config.h"

#include <assert.h>
#include <string.h>
#include <errno.h>
#include <math.h>
#include <sys/timeb.h>

#include "chartype.h"
#include "system_parameter.h"
#include "intl_support.h"
#include "error_code.h"
#include "db.h"
#include "memory_alloc.h"
#include "language_support.h"
#include "query_evaluator.h"
#if defined(SERVER_MODE)
#include "thread.h"
#endif

#include "misc_string.h"
#include "md5.h"
#include "porting.h"
#include "crypt_opfunc.h"
#include "base64.h"

/* this must be the last header file included!!! */
#include "dbval.h"

#define QSTR_STRING_MAX_SIZE_BYTES (32 * 1024 * 1024)	/* 32M */

#define QSTR_VALUE_PRECISION(value)                                       \
            ((DB_VALUE_PRECISION(value) == TP_FLOATING_PRECISION_VALUE)  \
                     ?      DB_GET_STRING_LENGTH(value)       :          \
                            DB_VALUE_PRECISION(value))

#define QSTR_MAX_PRECISION(str_type)                                         \
            (QSTR_IS_CHAR(str_type)          ?	DB_MAX_VARCHAR_PRECISION :  \
	                                        DB_MAX_VARBIT_PRECISION)

#define STACK_SIZE        100

#define LOB_CHUNK_SIZE	(128 * 1024)
#define REGEX_MAX_ERROR_MSG_SIZE  100

static int qstr_trim (MISC_OPERAND tr_operand,
		      const unsigned char *trim,
		      int trim_length,
		      int trim_size,
		      const unsigned char *src_ptr,
		      DB_TYPE src_type,
		      int src_length,
		      int src_size,
		      unsigned char **res,
		      DB_TYPE * res_type, int *res_length, int *res_size);
static void trim_leading (const unsigned char *trim_charset_ptr,
			  int trim_charset_size,
			  const unsigned char *src_ptr, DB_TYPE src_type,
			  int src_length, int src_size,
			  unsigned char **lead_trimmed_ptr,
			  int *lead_trimmed_length, int *lead_trimmed_size);
static void trim_trailing (const unsigned char *trim_charset_ptr,
			   int trim_charset_size,
			   const unsigned char *src_ptr, DB_TYPE src_type,
			   int src_length, int src_size,
			   int *trail_trimmed_length,
			   int *trail_trimmed_size);
static int qstr_pad (MISC_OPERAND pad_operand, int pad_length,
		     const unsigned char *pad_charset_ptr,
		     int pad_charset_length, int pad_charset_size,
		     const unsigned char *src_ptr, DB_TYPE src_type,
		     int src_length, int src_size, unsigned char **result,
		     DB_TYPE * result_type, int *result_length,
		     int *result_size);
static int qstr_eval_like (const char *tar, int tar_length, const char *expr,
			   int expr_length, const char *escape, int coll_id);
#if defined(ENABLE_UNUSED_FUNCTION)
static int kor_cmp (unsigned char *src, unsigned char *dest, int size);
#endif
static int qstr_replace (unsigned char *src_buf,
			 int src_len,
			 int src_size,
			 int coll_id,
			 unsigned char *srch_str_buf,
			 int srch_str_size,
			 unsigned char *repl_str_buf,
			 int repl_str_size,
			 unsigned char **result_buf,
			 int *result_len, int *result_size);
static int qstr_translate (unsigned char *src_ptr,
			   DB_TYPE src_type,
			   int src_size,
			   unsigned char *from_str_ptr,
			   int from_str_size,
			   unsigned char *to_str_ptr,
			   int to_str_size,
			   unsigned char **result_ptr,
			   DB_TYPE * result_type,
			   int *result_len, int *result_size);
#if defined (ENABLE_UNUSED_FUNCTION)
static bool is_string (const DB_VALUE * s);
#endif /* ENABLE_UNUSED_FUNCTION */
#if defined (ENABLE_UNUSED_FUNCTION)
static bool is_integer (const DB_VALUE * i);
#endif
static int qstr_grow_string (DB_VALUE * src_string,
			     DB_VALUE * result, int new_size);
static int qstr_concatenate (const unsigned char *s1,
			     int s1_length,
			     int s1_precision,
			     DB_TYPE s1_type,
			     const unsigned char *s2,
			     int s2_length,
			     int s2_precision,
			     DB_TYPE s2_type,
			     unsigned char **result,
			     int *result_length,
			     int *result_size,
			     DB_TYPE * result_type,
			     DB_DATA_STATUS * data_status);
static bool varchar_truncated (const unsigned char *s,
			       DB_TYPE s_type, int s_length, int used_chars);
#if defined (ENABLE_UNUSED_FUNCTION)
static bool varbit_truncated (const unsigned char *s,
			      int s_length, int used_bits);
#endif
static void bit_ncat (unsigned char *r, int offset, const unsigned char *s,
		      int n);
#if defined (ENABLE_UNUSED_FUNCTION)
static int bstring_fls (const char *s, int n);
#endif
static int qstr_bit_coerce (const unsigned char *src,
			    int src_length,
			    int src_precision,
			    DB_TYPE src_type,
			    unsigned char **dest,
			    int *dest_length,
			    int dest_precision,
			    DB_TYPE dest_type, DB_DATA_STATUS * data_status);
static int qstr_coerce (const unsigned char *src, int src_length,
			int src_precision, DB_TYPE src_type,
			INTL_CODESET src_codeset, INTL_CODESET dest_codeset,
			unsigned char **dest, int *dest_length,
			int *dest_size, int dest_precision,
			DB_TYPE dest_type, DB_DATA_STATUS * data_status);
static int qstr_position (const char *sub_string, const int sub_size,
			  const int sub_length,
			  const char *src_string, const char *src_end,
			  const char *src_string_bound,
			  int src_length, int coll_id,
			  bool is_forward_search, int *position);
#if defined (ENABLE_UNUSED_FUNCTION)
static int shift_left (unsigned char *bit_string, int bit_string_size);
#endif
static int qstr_substring (const unsigned char *src,
			   int src_length,
			   int start,
			   int length,
			   unsigned char **r, int *r_length, int *r_size);
#if defined (ENABLE_UNUSED_FUNCTION)
static void left_nshift (const unsigned char *bit_string, int bit_string_size,
			 int shift_amount, unsigned char *r, int r_size);
static int qstr_ffs (int v);
#endif
static int hextoi (char hex_char);
static void set_time_argument (struct tm *dest, int year, int month, int day,
			       int hour, int min, int sec);
static long calc_unix_timestamp (struct tm *time_argument);
#if defined (ENABLE_UNUSED_FUNCTION)
static int parse_for_next_int (char **ch, char *output);
#endif

/*
 *  Public Functions for Strings - Bit and Character
 */

/*
 * db_string_compare () -
 *
 * Arguments:
 *                string1: Left side of compare.
 *                string2: Right side of compare
 *                 result: Integer result of comparison.
 *            data_status: Status of errors.
 *
 * Returns: int
 *
 * Errors:
 *      ER_OBJ_INVALID_ARGUMENTS   :
 *        <string1> or <string2> are not character strings.
 *
 *    ER_QSTR_INCOMPATIBLE_CODE_SETS:
 *        <string1> and <string2> have differing character code sets.
 *
 */

int
db_string_compare (const DB_VALUE * string1, const DB_VALUE * string2,
		   DB_VALUE * result)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_string1, tmp_string2;
  int cmp_result = 0;

  /* Assert that DB_VALUE structures have been allocated. */
  assert (string1 != (DB_VALUE *) NULL);
  assert (string2 != (DB_VALUE *) NULL);
  assert (result != (DB_VALUE *) NULL);
  assert (string1 != result);
  assert (string2 != result);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (2, &tmp_string1, &tmp_string2);

  /* arg check null *********************************************************
   */
  ;				/* go ahead */

  /* arg cast type **********************************************************
   */
  if (!DB_IS_NULL (string1))
    {
      string1 =
	db_value_cast_arg (string1, &tmp_string1, DB_TYPE_VARCHAR,
			   &error_status);
    }
  if (!DB_IS_NULL (string2))
    {
      string2 =
	db_value_cast_arg (string2, &tmp_string2, DB_TYPE_VARCHAR,
			   &error_status);
    }

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  /* Categorize the two input parameters and check for errors.
     Verify that the parameters are both character strings.
     Verify that the input strings belong to compatible categories. */
  assert (qstr_get_category (string1) == QSTR_VARCHAR);
  assert (qstr_get_category (string2) == QSTR_VARCHAR);

  /* A string which is NULL (not the same as a NULL string) is
     ordered less than a string which is not NULL.  Two strings
     which are NULL are ordered equivalently.
     If both strings are not NULL, then the strings themselves
     are compared. */
  if (DB_IS_NULL (string1) && !DB_IS_NULL (string2))
    {
      cmp_result = -1;
    }
  else if (!DB_IS_NULL (string1) && DB_IS_NULL (string2))
    {
      cmp_result = 1;
    }
  else if (DB_IS_NULL (string1) && DB_IS_NULL (string2))
    {
      cmp_result = 0;
    }
  else
    {
      int coll_id;

      LANG_RT_COMMON_COLL (DB_GET_STRING_COLLATION (string1),
			   DB_GET_STRING_COLLATION (string2), coll_id);

      if (coll_id == -1)
	{
	  error_status = ER_QSTR_INCOMPATIBLE_COLLATIONS;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto exit_on_error;
	}

      cmp_result =
	QSTR_COMPARE (coll_id,
		      (unsigned char *) DB_PULL_STRING (string1),
		      (int) DB_GET_STRING_SIZE (string1),
		      (unsigned char *) DB_PULL_STRING (string2),
		      (int) DB_GET_STRING_SIZE (string2));
    }

  if (cmp_result < 0)
    {
      cmp_result = -1;
    }
  else if (cmp_result > 0)
    {
      cmp_result = 1;
    }
  DB_MAKE_INTEGER (result, cmp_result);

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

#if 0
done:
#endif
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_string1, &tmp_string2);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (result);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_string1, &tmp_string2);

  return error_status;
}

/*
 * db_string_unique_prefix () -
 *
 * Arguments:
 *                string1: (IN) Left side of compare.
 *                string2: (IN) Right side of compare.
 *                is_desc: (IN)
 *                 result: (OUT) string such that >= string1, and < string2.
 *
 * Returns: int
 *
 * Errors:
 *    (TBD)
 *
 * Note:
 *    The purpose of this routine is to find a prefix that is greater
 *    than or equal to the first string but strictly less than the second
 *    string.
 *
 *    This routine assumes:
 *       a) The second string is strictly greater than the first
 *           (according to the ANSI SQL string comparison rules).
 *       b) The two strings are both of the same 'type', although one may be
 *           'fixed' and the other may be 'varying'.
 *       c) No padding is done.
 *
 * Assert:
 *
 *    1. string1 != (DB_VALUE *)NULL
 *    2. string2 != (DB_VALUE *)NULL
 *    3. result  != (DB_VALUE *)NULL
 *
 */
int
db_string_unique_prefix (const DB_VALUE * db_string1,
			 const DB_VALUE * db_string2,
			 const bool is_desc, DB_VALUE * db_result)
{
  DB_TYPE result_type = (DB_TYPE) 0;
  int error_status = NO_ERROR;
  int precision;
  DB_VALUE tmp_result;
  int c;

  /* Assertions */
  assert (db_string1 != (DB_VALUE *) NULL);
  assert (db_string2 != (DB_VALUE *) NULL);
  assert (db_result != (DB_VALUE *) NULL);

  error_status = db_string_compare (db_string1, db_string2, &tmp_result);
  if ((error_status != NO_ERROR) ||
      ((c = DB_GET_INTEGER (&tmp_result)) &&
       ((!is_desc && c > 0) || (is_desc && c < 0))))
    {
      DB_MAKE_NULL (db_result);
#if defined(RYE_DEBUG)
      if (error_status == ER_OBJ_INVALID_ARGUMENTS)
	{
	  printf ("db_string_unique_prefix(): non-string type: %s and %s\n",
		  pr_type_name (DB_VALUE_DOMAIN_TYPE (db_string1)),
		  pr_type_name (DB_VALUE_DOMAIN_TYPE (db_string2)));
	}
      if (error_status == ER_QSTR_INCOMPATIBLE_CODE_SETS)
	{
	  printf
	    ("db_string_unique_prefix(): incompatible types: %s and %s\n",
	     pr_type_name (DB_VALUE_DOMAIN_TYPE (db_string1)),
	     pr_type_name (DB_VALUE_DOMAIN_TYPE (db_string2)));
	}
      if (DB_GET_INTEGER (&tmp_result) > 0)
	{
	  printf
	    ("db_string_unique_prefix(): string1 %s, greater than string2 %s\n",
	     DB_GET_STRING (db_string1), DB_GET_STRING (db_string2));
	}
#endif
      return ER_GENERIC_ERROR;
    }

  precision = DB_VALUE_PRECISION (db_string1);
  /* Determine the result type */
  result_type = DB_VALUE_DOMAIN_TYPE (db_string1);
  if (QSTR_IS_CHAR (result_type))
    {
      result_type = DB_TYPE_VARCHAR;
    }
  else
    {
      DB_MAKE_NULL (db_result);
#if defined(RYE_DEBUG)
      printf ("db_string_unique_prefix(): non-string type: %s and %s\n",
	      pr_type_name (DB_VALUE_DOMAIN_TYPE (db_string1)),
	      pr_type_name (DB_VALUE_DOMAIN_TYPE (db_string2)));
#endif
      return ER_GENERIC_ERROR;
    }

  /* A string which is NULL (not the same as a NULL string) is
     ordered less than a string which is not NULL.  Since string2 is
     assumed to be strictly > string1, string2 can never be NULL. */
  if (DB_IS_NULL (db_string1))
    {
      db_value_domain_init (db_result, result_type, precision, 0);
    }

  /* Find the first byte where the 2 strings differ.  Set the result
     accordingly. */
  else
    {
      int size1, size2, result_size, pad_size = 0;
      unsigned char *string1, *string2, *key = NULL, pad[2], *t;
      char *result;
      int collation_id;

      string1 = (unsigned char *) DB_GET_STRING (db_string1);
      size1 = (int) DB_GET_STRING_SIZE (db_string1);
      string2 = (unsigned char *) DB_GET_STRING (db_string2);
      size2 = (int) DB_GET_STRING_SIZE (db_string2);
      collation_id = DB_GET_STRING_COLLATION (db_string1);

      assert (collation_id == DB_GET_STRING_COLLATION (db_string2));

      if (string1 == NULL || string2 == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_INVALID_PARAMETER, 0);
	  return ER_QPROC_INVALID_PARAMETER;
	}

      if (collation_id == LANG_COERCIBLE_COLL)
	{
	  collation_id = LANG_SYS_COLLATION;
	}

      intl_pad_char (pad, &pad_size);

#if 0
    trim_again:
#endif
      /* We need to implicitly trim both strings since we don't want padding
         for the result (its of varying type) and since padding can mask the
         logical end of both of the strings.  Trimming depends on codeset. */
      if (pad_size == 1)
	{
	  for (t = string1 + (size1 - 1); t >= string1 && *t == pad[0];
	       t--, size1--)
	    {
	      ;
	    }
	  for (t = string2 + (size2 - 1); t >= string2 && *t == pad[0];
	       t--, size2--)
	    {
	      ;
	    }
	}
      else
	{
	  assert (pad_size == 2);

	  for (t = string1 + (size1 - 2); t >= string1 && *t == pad[0]
	       && *(t + 1) == pad[1]; t--, t--, size1--, size1--)
	    {
	      ;
	    }

	  for (t = string2 + (size2 - 2); t >= string2 && *t == pad[0]
	       && *(t + 1) == pad[1]; t--, t--, size2--, size2--)
	    {
	      ;
	    }
	}

      error_status = QSTR_SPLIT_KEY (collation_id, is_desc,
				     string1, size1, string2, size2, &key,
				     &result_size);
      if (error_status == NO_ERROR)
	{
	  assert (key != NULL);

	  result = (char *) malloc (result_size + 1);
	  if (result)
	    {
	      if (result_size)
		{
		  (void) memcpy (result, key, result_size);
		}

	      result[result_size] = 0;

	      db_value_domain_init (db_result, result_type, precision, 0);
	      error_status = db_make_db_char (db_result, collation_id,
					      result, result_size);
	      db_result->need_clear = true;
	    }
	  else
	    {
	      /* will already be set by memory mgr */
	      error_status = er_errid ();
	    }
	}
    }

#if !defined(NDEBUG)
  if (error_status == NO_ERROR)
    {
      int err_status2 = NO_ERROR;
      int c1 = 1, c2 = -1;

      err_status2 = db_string_compare (db_string1, db_result, &tmp_result);
      if (err_status2 == NO_ERROR)
	{
	  c1 = DB_GET_INTEGER (&tmp_result);
	}
      err_status2 = db_string_compare (db_result, db_string2, &tmp_result);
      if (err_status2 == NO_ERROR)
	{
	  c2 = DB_GET_INTEGER (&tmp_result);
	}

      if (!is_desc)
	{
	  assert (c1 <= 0 && c2 <= 0);
	}
      else
	{
	  assert (c1 >= 0 && c2 >= 0);
	}
    }
#endif

  return (error_status);
}

/*
 * db_string_concatenate () -
 *
 * Arguments:
 *          string1: Left string to concatenate.
 *          string2: Right string to concatenate.
 *           result: Result of concatenation of both strings.
 *      data_status: DB_DATA_STATUS which indicates if truncation occurred.
 *
 * Returns: int
 *
 * Errors:
 *      ER_OBJ_INVALID_ARGUMENTS     :
 *          <string1> or <string2> not string types.
 *
 *      ER_QSTR_INCOMPATIBLE_CODE_SETS:
 *          <string1> or <string2> have different character code sets
 *          or are not all bit strings.
 *
 */

int
db_string_concatenate (const DB_VALUE * string1,
		       const DB_VALUE * string2,
		       DB_VALUE * result, DB_DATA_STATUS * data_status)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_string1, tmp_string2;
  QSTR_CATEGORY string1_code_set, string2_code_set;
  unsigned char *r;
  int r_length, r_size;
  DB_TYPE r_type;
  int result_domain_length;
  int common_coll;

  /*
   *  Initialize status value
   */
  *data_status = DATA_STATUS_OK;

  /*
   *  Assert that DB_VALUE structures have been allocated.
   */
  assert (string1 != (DB_VALUE *) NULL);
  assert (string2 != (DB_VALUE *) NULL);
  assert (result != (DB_VALUE *) NULL);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (2, &tmp_string1, &tmp_string2);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (2, string1, string2))
    {
      db_value_domain_init (result, DB_TYPE_VARCHAR,
			    DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  string1 = db_value_cast_arg (string1, &tmp_string1, DB_TYPE_VARCHAR,
			       &error_status);
  string2 = db_value_cast_arg (string2, &tmp_string2, DB_TYPE_VARCHAR,
			       &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  /*
   *  Categorize the parameters into respective code sets.
   *  Verify that the parameters are both character strings.
   *  Verify that the input strings belong to compatible code sets.
   */

  string1_code_set = qstr_get_category (string1);
  string2_code_set = qstr_get_category (string2);

  if (string1_code_set != string2_code_set)
    {
      error_status = ER_QSTR_INCOMPATIBLE_CODE_SETS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  LANG_RT_COMMON_COLL (DB_GET_STRING_COLLATION (string1),
		       DB_GET_STRING_COLLATION (string2), common_coll);
  if (common_coll == -1)
    {
      error_status = ER_QSTR_INCOMPATIBLE_COLLATIONS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  error_status =
    qstr_concatenate ((unsigned char *) DB_PULL_STRING (string1),
		      (int) DB_GET_STRING_LENGTH (string1),
		      (int) QSTR_VALUE_PRECISION (string1),
		      DB_VALUE_DOMAIN_TYPE (string1),
		      (unsigned char *) DB_PULL_STRING (string2),
		      (int) DB_GET_STRING_LENGTH (string2),
		      (int) QSTR_VALUE_PRECISION (string2),
		      DB_VALUE_DOMAIN_TYPE (string2),
		      &r, &r_length, &r_size, &r_type, data_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  if (r != NULL)
    {
      if ((DB_VALUE_PRECISION (string1) ==
	   TP_FLOATING_PRECISION_VALUE) ||
	  (DB_VALUE_PRECISION (string2) == TP_FLOATING_PRECISION_VALUE))
	{
	  result_domain_length = TP_FLOATING_PRECISION_VALUE;
	}
      else
	{
	  result_domain_length = MIN (QSTR_MAX_PRECISION (r_type),
				      DB_VALUE_PRECISION (string1) +
				      DB_VALUE_PRECISION (string2));
	}

      qstr_make_typed_string (r_type,
			      result,
			      result_domain_length,
			      (char *) r, r_size, common_coll);
      r[r_size] = 0;
      result->need_clear = true;
    }

  if (error_status != NO_ERROR)
    {
      pr_clear_value (result);
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_string1, &tmp_string2);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (result);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_string1, &tmp_string2);

  return error_status;
}

/*
 * db_string_chr () - take character of db_value
 *   return: NO_ERROR, or ER_code
 *   result(OUT)   : resultant db_value node
 *   dbval1(IN) : first db_value node
 *   dbval2(IN) : charset name to use
 */

int
db_string_chr (DB_VALUE * result, DB_VALUE * dbval1)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_dbval1;
  DB_TYPE arg_type;
  DB_BIGINT temp_bigint = 0;
  unsigned int temp_arg = 0, uint_arg = 0;
  int itmp = 0;

  DB_BIGINT bi = 0;

  double dtmp = 0;

  char *num_as_bytes = NULL;
  char *invalid_pos = NULL;
  int num_byte_count = 0;
  int i, collation = -1;

  assert (result != NULL);
  assert (dbval1 != NULL);
  assert (result != dbval1);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (1, &tmp_dbval1);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (1, dbval1))
    {
      DB_MAKE_NULL (result);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  arg_type = DB_VALUE_DOMAIN_TYPE (dbval1);

  if (!TP_IS_NUMERIC_TYPE (arg_type))
    {
      dbval1 = db_value_cast_arg (dbval1, &tmp_dbval1, DB_TYPE_BIGINT,
				  &error_status);
    }

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  arg_type = DB_VALUE_DOMAIN_TYPE (dbval1);

  /* Get value according to DB_TYPE */
  switch (arg_type)
    {
    case DB_TYPE_INTEGER:
      itmp = DB_GET_INTEGER (dbval1);
      break;
    case DB_TYPE_BIGINT:
      bi = DB_GET_BIGINT (dbval1);
      break;
    case DB_TYPE_DOUBLE:
      dtmp = DB_GET_DOUBLE (dbval1);
      break;
    case DB_TYPE_NUMERIC:
      numeric_coerce_num_to_double (db_locate_numeric (dbval1),
				    DB_VALUE_SCALE (dbval1), &dtmp);
      break;
    default:
      assert (false);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
      error_status = ER_GENERIC_ERROR;
      goto exit_on_error;
    }				/* switch */

  /* bi, dtmp and itmp have the default value set to 0, so temp_bigint will
   * hold the numeric representation of the first argument, regardless of
   * its type. */
  temp_bigint = bi + (DB_BIGINT) round (fmod (dtmp, 0x100000000)) + itmp;

  if (temp_bigint >= 0)
    {
      temp_arg = DB_UINT32_MAX & temp_bigint;
    }
  else
    {
      temp_arg = DB_UINT32_MAX & (-temp_bigint);
      temp_arg = DB_UINT32_MAX - temp_arg + 1;
    }
  uint_arg = temp_arg;

  if (temp_arg == 0)
    {
      num_byte_count = 1;
    }
  else
    {
      while (temp_arg > 0)
	{
	  num_byte_count++;
	  temp_arg >>= 8;
	}
    }

  num_as_bytes = (char *) malloc ((1 + num_byte_count) * sizeof (char));
  if (num_as_bytes == NULL)
    {
      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
      goto exit_on_error;
    }
  temp_arg = uint_arg;

  for (i = num_byte_count - 1; i >= 0; i--)
    {
      num_as_bytes[i] = (char) (temp_arg & 0xFF);
      temp_arg >>= 8;
    }
  num_as_bytes[num_byte_count] = '\0';

  if (intl_check_utf8 ((const unsigned char *) num_as_bytes,
		       num_byte_count, &invalid_pos) != 0)
    {
      free_and_init (num_as_bytes);

      error_status = ER_OBJ_INVALID_ARGUMENTS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  collation = LANG_GET_DEFAULT_COLLATION;
  db_make_varchar (result, DB_DEFAULT_PRECISION, num_as_bytes, num_byte_count,
		   collation);
  result->need_clear = true;

  if (error_status != NO_ERROR)
    {
      pr_clear_value (result);
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_dbval1);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (result);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_dbval1);

  return error_status;
}

/*
 * db_string_instr () -
 *
 * Arguments:
 *      sub_string: String fragment to search for within <src_string>.
 *      src_string: String to be searched.
 *          result: Character or bit position of the first <sub_string>
 *                  occurance.
 *
 * Returns: int
 *
 * Errors:
 *      ER_OBJ_INVALID_ARGUMENTS     :
 *         <sub_string> or <src_string> are not a character strings.
 *      ER_QSTR_INCOMPATIBLE_CODE_SETS:
 *         <sub_string> and <src_string> have different character
 *         code sets, or are not both bit strings.
 *
 */

int
db_string_instr (const DB_VALUE * src_string,
		 const DB_VALUE * sub_string,
		 const DB_VALUE * start_pos, DB_VALUE * result)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_src_string, tmp_sub_string, tmp_start_pos;

  int position = -1;
  int src_str_len;
  int sub_str_len;
  int offset;
  char *search_from, *src_buf, *sub_str;
  int coll_id;
  int sub_str_size;
  int from_byte_offset;
  int src_size;

  /*
   *  Assert that DB_VALUE structures have been allocated.
   */
  assert (src_string != (DB_VALUE *) NULL);
  assert (sub_string != (DB_VALUE *) NULL);
  assert (start_pos != (DB_VALUE *) NULL);
  assert (result != (DB_VALUE *) NULL);
  assert (src_string != result);
  assert (sub_string != result);
  assert (start_pos != result);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (3, &tmp_src_string, &tmp_sub_string, &tmp_start_pos);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (3, src_string, sub_string, start_pos))
    {
      assert (position == -1);
      DB_MAKE_NULL (result);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  src_string =
    db_value_cast_arg (src_string, &tmp_src_string, DB_TYPE_VARCHAR,
		       &error_status);
  sub_string =
    db_value_cast_arg (sub_string, &tmp_sub_string, DB_TYPE_VARCHAR,
		       &error_status);
  start_pos =
    db_value_cast_arg (start_pos, &tmp_start_pos, DB_TYPE_INTEGER,
		       &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  /*
   *  Categorize the parameters into respective code sets.
   *  Verify that the parameters are both character strings.
   *  Verify that the input strings belong to compatible code sets.
   */
  assert (qstr_get_category (src_string) == QSTR_VARCHAR);
  assert (qstr_get_category (sub_string) == QSTR_VARCHAR);

  position = 0;
  offset = DB_GET_INT (start_pos);
  sub_str_size = DB_GET_STRING_SIZE (sub_string);
  src_size = DB_GET_STRING_SIZE (src_string);

  LANG_RT_COMMON_COLL (DB_GET_STRING_COLLATION (src_string),
		       DB_GET_STRING_COLLATION (sub_string), coll_id);
  if (coll_id == -1)
    {
      error_status = ER_QSTR_INCOMPATIBLE_COLLATIONS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  src_str_len = DB_GET_STRING_LENGTH (src_string);
  sub_str_len = DB_GET_STRING_LENGTH (sub_string);

  src_buf = DB_PULL_STRING (src_string);
  if (src_size < 0)
    {
      src_size = strlen (src_buf);
    }

  sub_str = DB_PULL_STRING (sub_string);
  if (sub_str_size < 0)
    {
      sub_str_size = strlen (sub_str);
    }

  if (offset > 0)
    {
      offset--;
      if (offset + sub_str_len > src_str_len)
	{			/* out of bound */
	  position = 0;
	}
      else
	{
	  search_from = src_buf;

	  intl_char_size ((unsigned char *) search_from, offset,
			  &from_byte_offset);
	  search_from += from_byte_offset;

	  intl_char_count ((unsigned char *) search_from,
			   src_size - from_byte_offset, &src_str_len);

	  /* forward search */
	  error_status =
	    qstr_position (sub_str, sub_str_size, sub_str_len,
			   search_from, src_buf + src_size,
			   src_buf + src_size, src_str_len,
			   coll_id, true, &position);
	  position += (position != 0) ? offset : 0;
	}
    }
  else if (offset < 0)
    {
      if (src_str_len + offset + 1 < sub_str_len)
	{
	  position = 0;
	}
      else
	{
	  int real_offset = src_str_len + offset - (sub_str_len - 1);

	  search_from = src_buf;

	  intl_char_size ((unsigned char *) search_from, real_offset,
			  &from_byte_offset);

	  search_from += from_byte_offset;

	  /* backward search */
	  error_status =
	    qstr_position (sub_str, sub_str_size, sub_str_len,
			   search_from, src_buf + src_size, src_buf,
			   src_str_len + offset + 1, coll_id,
			   false, &position);
	  if (position != 0)
	    {
	      position = src_str_len - (-offset - 1)
		- (position - 1) - (sub_str_len - 1);
	    }
	}
    }
  else
    {
      /* offset == 0 */
      position = 0;
    }

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  if (position >= 0)
    {
      DB_MAKE_INTEGER (result, position);
    }

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_src_string, &tmp_sub_string, &tmp_start_pos);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (result);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_src_string, &tmp_sub_string, &tmp_start_pos);

  return error_status;
}

/*
 * db_string_space () -
 *  returns a VARCHAR string consisting of a number of space characters equals
 *  to the given argument
 *
 * Arguments:
 *	count: number of space characters in the returned string
 *
 * Returns: int
 *
 * Errors:
 *     ER_OBJ_INVALID_ARGUMENTS: count is not a discrete numeric type (integer)
 *			    ....  ...
 */

int
db_string_space (DB_VALUE const *count, DB_VALUE * result)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_count;
  int len = 0;
  char *space_string_p = NULL;

  assert (count != (DB_VALUE *) NULL);
  assert (result != (DB_VALUE *) NULL);
  assert (count != result);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (1, &tmp_count);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (1, count))
    {
      DB_MAKE_NULL (result);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  count =
    db_value_cast_arg (count, &tmp_count, DB_TYPE_INTEGER, &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  assert (DB_VALUE_DOMAIN_TYPE (count) == DB_TYPE_INTEGER);

  len = DB_GET_INTEGER (count);

  if (len < 0)
    {
      len = 0;
    }

  if (len > QSTR_STRING_MAX_SIZE_BYTES)
    {
      error_status = ER_QPROC_STRING_SIZE_TOO_BIG;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      error_status, 2, len, QSTR_STRING_MAX_SIZE_BYTES);
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  space_string_p = (char *) malloc (len + 1);
  if (space_string_p == NULL)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  if (len > 64)
    {
      /* if string is longer than 64 chars use memset to
       * initialize it */
      memset (space_string_p, ' ', len);
    }
  else
    {
      int i = 0;

      while (i < len)
	{
	  space_string_p[i++] = ' ';
	}
    }
  space_string_p[len] = '\0';

  qstr_make_typed_string (DB_TYPE_VARCHAR, result, len,
			  space_string_p, len, LANG_COERCIBLE_COLL);
  result->need_clear = true;

  if (error_status != NO_ERROR)
    {
      pr_clear_value (result);
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_count);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (result);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_count);

  return error_status;
}

/*
 * db_string_position () -
 *
 * Arguments:
 *      sub_string: String fragment to search for within <src_string>.
 *      src_string: String to be searched.
 *          result: Character or bit position of the first <sub_string>
 *                  occurance.
 *
 * Returns: int
 *
 * Errors:
 *      ER_OBJ_INVALID_ARGUMENTS     :
 *         <sub_string> or <src_string> are not a character strings.
 *      ER_QSTR_INCOMPATIBLE_CODE_SETS:
 *         <sub_string> and <src_string> have different character
 *         code sets, or are not both bit strings.
 *
 */

int
db_string_position (const DB_VALUE * sub_string,
		    const DB_VALUE * src_string, DB_VALUE * result)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_sub_string, tmp_src_string;

  int position = -1;

  char *src_str;
  int src_size;
  char *sub_str;
  int sub_size;
  int coll_id;

  /*
   *  Assert that DB_VALUE structures have been allocated.
   */
  assert (sub_string != (DB_VALUE *) NULL);
  assert (src_string != (DB_VALUE *) NULL);
  assert (result != (DB_VALUE *) NULL);
  assert (sub_string != result);
  assert (src_string != result);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (2, &tmp_sub_string, &tmp_src_string);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (2, sub_string, src_string))
    {
      assert (position == -1);
      DB_MAKE_NULL (result);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  sub_string =
    db_value_cast_arg (sub_string, &tmp_sub_string, DB_TYPE_VARCHAR,
		       &error_status);
  src_string =
    db_value_cast_arg (src_string, &tmp_src_string, DB_TYPE_VARCHAR,
		       &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  assert (DB_VALUE_DOMAIN_TYPE (sub_string) == DB_TYPE_VARCHAR);
  assert (DB_VALUE_DOMAIN_TYPE (src_string) == DB_TYPE_VARCHAR);

  /*
   *  Categorize the parameters into respective code sets.
   *  Verify that the parameters are both character strings.
   *  Verify that the input strings belong to compatible code sets.
   */
  assert (qstr_get_category (sub_string) == QSTR_VARCHAR);
  assert (qstr_get_category (src_string) == QSTR_VARCHAR);

  src_str = DB_PULL_STRING (src_string);
  src_size = DB_GET_STRING_SIZE (src_string);
  sub_str = DB_PULL_STRING (sub_string);
  sub_size = DB_GET_STRING_SIZE (sub_string);

  LANG_RT_COMMON_COLL (DB_GET_STRING_COLLATION (src_string),
		       DB_GET_STRING_COLLATION (sub_string), coll_id);
  if (coll_id == -1)
    {
      error_status = ER_QSTR_INCOMPATIBLE_COLLATIONS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  if (src_size < 0)
    {
      src_size = strlen (src_str);
    }

  if (sub_size < 0)
    {
      sub_size = strlen (sub_str);
    }

  error_status =
    qstr_position (sub_str, sub_size,
		   DB_GET_STRING_LENGTH (sub_string),
		   src_str, src_str + src_size, src_str + src_size,
		   DB_GET_STRING_LENGTH (src_string),
		   coll_id, true, &position);

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  if (position >= 0)
    {
      DB_MAKE_INTEGER (result, position);
    }

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_sub_string, &tmp_src_string);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (result);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_sub_string, &tmp_src_string);

  return error_status;
}

/*
 * db_string_substring
 *
 * Arguments:
 *             src_string: String from which extraction will occur.
 *              start_pos: Character position to begin extraction from.
 *      extraction_length: Number of characters to extract (Optional).
 *             sub_string: Extracted subtring is returned here.
 *
 * Returns: int
 *
 * Errors:
 *      ER_OBJ_INVALID_ARGUMENTS     :
 *         <src_string> is not a string type,
 *         <start_pos> or <extraction_length>  is not an integer type
 *      ER_QSTR_INCOMPATIBLE_CODE_SETS:
 *         <src_string> have different character
 *         code sets or are not both bit strings.
 *
 */

int
db_string_substring (const MISC_OPERAND substr_operand,
		     const DB_VALUE * src_string,
		     const DB_VALUE * start_position,
		     const DB_VALUE * extraction_length, DB_VALUE * result)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_src_string, tmp_start_position, tmp_extraction_length;
  unsigned char *sub;
  int sub_length;
  int extract_nchars = -1;

  int sub_size;
  unsigned char *string;
  int start_offset;
  int string_len;

  /*
   *  Assert that DB_VALUE structures have been allocated.
   */
  assert (src_string != (DB_VALUE *) NULL);
  assert (start_position != (DB_VALUE *) NULL);
  assert (result != (DB_VALUE *) NULL);
  assert (src_string != result);
  assert (start_position != result);
  assert (extraction_length != result);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (3, &tmp_src_string, &tmp_start_position,
		      &tmp_extraction_length);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (2, src_string, start_position))
    {
      DB_MAKE_NULL (result);
      goto done;
    }
  if (extraction_length != NULL && DB_IS_NULL (extraction_length))
    {
      DB_MAKE_NULL (result);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  src_string =
    db_value_cast_arg (src_string, &tmp_src_string, DB_TYPE_VARCHAR,
		       &error_status);
  start_position =
    db_value_cast_arg (start_position, &tmp_start_position, DB_TYPE_INTEGER,
		       &error_status);
  if (extraction_length != NULL)
    {
      extraction_length =
	db_value_cast_arg (extraction_length, &tmp_extraction_length,
			   DB_TYPE_INTEGER, &error_status);
    }

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  /* Initialize the memory manager of the substring */
  sub_size = 0;

  string = (unsigned char *) DB_PULL_STRING (src_string);
  start_offset = DB_GET_INTEGER (start_position);
  string_len = DB_GET_STRING_LENGTH (src_string);

  if (extraction_length != NULL)
    {
      extract_nchars = DB_GET_INTEGER (extraction_length);
    }
  else
    {
      extract_nchars = string_len;
    }

  if (substr_operand == SUBSTR)
    {
#if 1				/* TODO - */
      if (extract_nchars < 0 || string_len < ABS (start_offset))
	{
	  goto done;
	}
#endif

      if (start_offset < 0)
	{
	  int byte_pos;
	  (void) intl_char_size (string,
				 string_len + start_offset, &byte_pos);
	  string += byte_pos;
	  string_len = -start_offset;
	}
    }

  error_status = qstr_substring (string, string_len, start_offset,
				 extract_nchars, &sub, &sub_length,
				 &sub_size);
  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

  if (sub != NULL)
    {
      qstr_make_typed_string (DB_TYPE_VARCHAR, result,
			      DB_VALUE_PRECISION (src_string),
			      (char *) sub, sub_size,
			      DB_GET_STRING_COLLATION (src_string));
      sub[sub_size] = 0;
      result->need_clear = true;
    }

  if (error_status != NO_ERROR)
    {
      pr_clear_value (result);
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_src_string, &tmp_start_position,
			&tmp_extraction_length);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (result);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_src_string, &tmp_start_position,
			&tmp_extraction_length);

  return error_status;
}

/*
 * db_string_repeat
 *
 * Arguments:
 *             src_string: String which repeats itself.
 *		    count: Number of repetions.
 *		   result: string containing the repeated original.
 *
 * Returns: int
 *
 * Errors:
 *      ER_OBJ_INVALID_ARGUMENTS     :
 *         <src_string> is not a string type,
 *         <start_pos> or <extraction_length>  is not an integer type
 *
 */

int
db_string_repeat (const DB_VALUE * src_string,
		  const DB_VALUE * count, DB_VALUE * result)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_src_string, tmp_count;
  int src_length, count_i = 0, src_size = 0;

  /*
   *  Assert that DB_VALUE structures have been allocated.
   */
  assert (src_string != (DB_VALUE *) NULL);
  assert (count != (DB_VALUE *) NULL);
  assert (result != (DB_VALUE *) NULL);
  assert (src_string != result);
  assert (count != result);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (2, &tmp_src_string, &tmp_count);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (2, src_string, count))
    {
      DB_MAKE_NULL (result);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  src_string =
    db_value_cast_arg (src_string, &tmp_src_string, DB_TYPE_VARCHAR,
		       &error_status);
  count =
    db_value_cast_arg (count, &tmp_count, DB_TYPE_INTEGER, &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  src_length = (int) DB_GET_STRING_LENGTH (src_string);
  src_size = DB_GET_STRING_SIZE (src_string);

  count_i = DB_GET_INTEGER (count);

  if (src_size < 0)
    {
      intl_char_size ((unsigned char *) DB_PULL_STRING (result), src_length,
		      &src_size);
    }

  if (count_i <= 0 || src_length <= 0)
    {
      error_status =
	db_string_make_empty_typed_string (NULL, result, DB_TYPE_VARCHAR,
					   src_length,
					   DB_GET_STRING_COLLATION
					   (src_string));
      if (error_status != NO_ERROR)
	{
	  goto exit_on_error;
	}
    }
  else
    {
      DB_VALUE dummy;
      unsigned char *res_ptr, *src_ptr;
      int expected_size;

      /* init dummy */
      DB_MAKE_NULL (&dummy);
      /* create an empy string for result */

      error_status =
	db_string_make_empty_typed_string (NULL, &dummy, DB_TYPE_VARCHAR,
					   src_length * count_i,
					   DB_GET_STRING_COLLATION
					   (src_string));
      if (error_status != NO_ERROR)
	{
	  goto exit_on_error;
	}

      expected_size = src_size * count_i;
      error_status = qstr_grow_string (&dummy, result, expected_size);
      if (error_status != NO_ERROR)
	{
	  pr_clear_value (&dummy);
	  return error_status;
	}

      pr_clear_value (&dummy);

      /* qstr_grow_string may return DB_NULL if size too big */
      if (DB_IS_NULL (result))
	{
	  goto done;
	}

      res_ptr = (unsigned char *) DB_PULL_STRING (result);
      src_ptr = (unsigned char *) DB_PULL_STRING (src_string);

      while (count_i--)
	{
	  memcpy (res_ptr, src_ptr, src_size);
	  res_ptr += src_size;
	}

      /* update size of string */
      qstr_make_typed_string (DB_TYPE_VARCHAR,
			      result,
			      DB_VALUE_PRECISION (result),
			      DB_PULL_STRING (result),
			      (const int) expected_size,
			      DB_GET_STRING_COLLATION (src_string));
      result->need_clear = true;
    }

  if (error_status != NO_ERROR)
    {
      pr_clear_value (result);
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_src_string, &tmp_count);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (result);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_src_string, &tmp_count);

  return error_status;
}

/*
 * db_string_substring_index - returns the substring from a string before
 *			       count occurences of delimeter
 *
 * Arguments:
 *             src_string: String to search in.
 *	     delim_string: String delimiter
 *		    count: Number of occurences.
 *		   result: string containing reminder.
 *
 * Returns: int
 *
 * Errors:
 *      ER_OBJ_INVALID_ARGUMENTS     :
 *         <str_string> or <delim_string> is not a string type,
 *         <count> is not an integer type
 *	ER_QSTR_INCOMPATIBLE_CODE_SETS:
 *	   <str_string> or <delim_string> are not compatible
 *
 */

int
db_string_substring_index (DB_VALUE * src_string,
			   DB_VALUE * delim_string,
			   const DB_VALUE * count, DB_VALUE * result)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_src_string, tmp_delim_string, tmp_count;
  int count_i = 0;
  INTL_CODESET src_cs, delim_cs;
  int src_coll, delim_coll;

  /*
   *  Assert that DB_VALUE structures have been allocated.
   */
  assert (src_string != (DB_VALUE *) NULL);
  assert (delim_string != (DB_VALUE *) NULL);
  assert (count != (DB_VALUE *) NULL);
  assert (result != (DB_VALUE *) NULL);
  assert (src_string != result);
  assert (delim_string != result);
  assert (count != result);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (3, &tmp_src_string, &tmp_delim_string, &tmp_count);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (3, src_string, delim_string, count))
    {
      DB_MAKE_NULL (result);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  src_string =
    db_value_cast_arg (src_string, &tmp_src_string, DB_TYPE_VARCHAR,
		       &error_status);
  delim_string =
    db_value_cast_arg (delim_string, &tmp_delim_string, DB_TYPE_VARCHAR,
		       &error_status);
  count =
    db_value_cast_arg (count, &tmp_count, DB_TYPE_INTEGER, &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  count_i = DB_GET_INT (count);

  /*
   *  Categorize the parameters into respective code sets.
   *  Verify that the parameters are both character strings.
   *  Verify that the input strings belong to compatible code sets.
   */
  if (qstr_get_category (src_string) != qstr_get_category (delim_string))
    {
      error_status = ER_QSTR_INCOMPATIBLE_CODE_SETS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  src_cs = INTL_CODESET_UTF8;
  src_coll = DB_IS_NULL (src_string) ? LANG_SYS_COLLATION :
    DB_GET_STRING_COLLATION (src_string);

  delim_cs = INTL_CODESET_UTF8;
  delim_coll = DB_IS_NULL (delim_string) ? LANG_SYS_COLLATION :
    DB_GET_STRING_COLLATION (delim_string);

  if (count_i == 0)
    {
      /* return an empty string */
      error_status =
	db_string_make_empty_typed_string (NULL, result, DB_TYPE_VARCHAR,
					   TP_FLOATING_PRECISION_VALUE,
					   src_coll);
    }
  else
    {
      DB_VALUE offset_val, interm_pos;
      int offset = 1, initial_count = 0;
      bool count_from_start;
      const int src_length = DB_GET_STRING_LENGTH (src_string);
      const int delim_length = DB_GET_STRING_LENGTH (delim_string);

      DB_MAKE_NULL (&interm_pos);
      initial_count = count_i;
      count_from_start = (count_i > 0) ? true : false;
      count_i = abs (count_i);

      assert (src_cs == delim_cs);

      if (count_from_start)
	{
	  while (count_i > 0)
	    {
	      DB_MAKE_INTEGER (&offset_val, offset);
	      error_status =
		db_string_instr (src_string, delim_string, &offset_val,
				 &interm_pos);
	      if (error_status != NO_ERROR)
		{
		  goto exit_on_error;
		}

	      offset = DB_GET_INT (&interm_pos);
	      if (offset != 0)
		{
		  offset += delim_length;
		  DB_MAKE_INTEGER (&offset_val, offset);
		}
	      else
		{
		  break;
		}
	      count_i--;
	    }

	}
      else
	{
	  while (count_i > 0)
	    {
	      /* search from end */
	      DB_MAKE_INTEGER (&offset_val, -offset);
	      error_status =
		db_string_instr (src_string, delim_string, &offset_val,
				 &interm_pos);
	      if (error_status != NO_ERROR)
		{
		  goto exit_on_error;
		}

	      offset = DB_GET_INT (&interm_pos);
	      if (offset != 0)
		{
		  /* adjust offset to indicate position relative to end */
		  offset = src_length - offset + 2;
		  DB_MAKE_INTEGER (&offset_val, offset);
		}
	      else
		{
		  break;
		}
	      count_i--;
	    }
	}

      assert (count_i >= 0);

      if (count_i == 0)
	{
	  /* found count occurences , return the string */
	  DB_VALUE start_val, len_val;
	  int start_pos = 1, end_pos = 0;

	  if (count_from_start)
	    {
	      start_pos = 1;
	      end_pos = offset - delim_length - 1;
	    }
	  else
	    {
	      start_pos = src_length - offset + 2 + delim_length;
	      end_pos = src_length;
	    }

	  if (start_pos > end_pos || start_pos < 1 || end_pos > src_length)
	    {
	      /* empty string */
	      error_status =
		db_string_make_empty_typed_string (NULL, result,
						   DB_TYPE_VARCHAR,
						   TP_FLOATING_PRECISION_VALUE,
						   src_coll);
	    }
	  else
	    {
	      DB_MAKE_INTEGER (&start_val, start_pos);
	      DB_MAKE_INTEGER (&len_val, end_pos - start_pos + 1);

	      error_status = db_string_substring (SUBSTRING, src_string,
						  &start_val, &len_val,
						  result);

	      result->need_clear = true;
	      if (error_status != NO_ERROR)
		{
		  pr_clear_value (result);
		  goto exit_on_error;
		}
	    }
	}
      else
	{
	  assert (count_i > 0);
	  /* not found at all or not enough number of occurences */
	  /* return the entire source string */

	  error_status = pr_clone_value (src_string, result);
	  if (error_status != NO_ERROR)
	    {
	      goto exit_on_error;
	    }
	}
    }

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_src_string, &tmp_delim_string, &tmp_count);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (result);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_src_string, &tmp_delim_string, &tmp_count);

  return error_status;
}

/*
 * db_string_shaone - sha1 encrypt function
 *   return: If success, return 0.
 *   src(in): source string
 *	 result(out): the encrypted data.
 * Note:
 */
int
db_string_sha_one (DB_VALUE const *src, DB_VALUE * result)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_src;
  char *result_strp = NULL;
  int result_len = 0;

  assert (src != (DB_VALUE *) NULL);
  assert (result != (DB_VALUE *) NULL);
  assert (src != result);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (1, &tmp_src);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (1, src))
    {
      DB_MAKE_NULL (result);	/* SH1(NULL) returns NULL */
      goto done;
    }

  /* arg cast type **********************************************************
   */
  src = db_value_cast_arg (src, &tmp_src, DB_TYPE_VARCHAR, &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  error_status =
    sha_one (NULL, DB_PULL_STRING (src), DB_GET_STRING_SIZE (src),
	     &result_strp, &result_len);
  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

  qstr_make_typed_string (DB_TYPE_VARCHAR, result, result_len,
			  result_strp, result_len,
			  DB_GET_STRING_COLLATION (src));
  result->need_clear = true;

  if (error_status != NO_ERROR)
    {
      pr_clear_value (result);
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_src);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (result);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_src);

  return error_status;
}

/*
 * db_string_shatwo - sha2 encrypt function
 *   return: If success, return 0.
 *   src(in): source string
 *	 hash_len(in): the hash length
 *	 result(out): the encrypted data.
 * Note:
 */
int
db_string_sha_two (DB_VALUE const *src, DB_VALUE const *hash_len,
		   DB_VALUE * result)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_src, tmp_hash_len;
  char *result_strp = NULL;
  int result_len = 0;
  int len = 0;

  assert (src != (DB_VALUE *) NULL);
  assert (hash_len != (DB_VALUE *) NULL);
  assert (result != (DB_VALUE *) NULL);
  assert (src != result);
  assert (hash_len != result);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (2, &tmp_src, &tmp_hash_len);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (2, src, hash_len))
    {
      DB_MAKE_NULL (result);	/* sha2(NULL, ...) or sha2(..., NULL) returns NULL */
      goto done;
    }

  /* arg cast type **********************************************************
   */
  src = db_value_cast_arg (src, &tmp_src, DB_TYPE_VARCHAR, &error_status);
  hash_len =
    db_value_cast_arg (hash_len, &tmp_hash_len, DB_TYPE_INTEGER,
		       &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  len = DB_GET_INT (hash_len);

  error_status =
    sha_two (NULL, DB_PULL_STRING (src), DB_GET_STRING_LENGTH (src), len,
	     &result_strp, &result_len);
  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

  /* It means that the hash_len is wrong. */
  if (result_strp == NULL)
    {
      DB_MAKE_NULL (result);
      goto done;
    }

  qstr_make_typed_string (DB_TYPE_VARCHAR, result, result_len,
			  result_strp, result_len,
			  DB_GET_STRING_COLLATION (src));
  result->need_clear = true;

  if (error_status != NO_ERROR)
    {
      pr_clear_value (result);
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_src, &tmp_hash_len);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (result);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_src, &tmp_hash_len);

  return error_status;
}

/*
 * MD5('str')
 * Arguments
 *	val: string to compute the MD5 (message digest) for
 *	result: DB_VALUE to receive the computed MD5 from the val argument
 */
int
db_string_md5 (DB_VALUE const *val, DB_VALUE * result)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_val;

  /* MD5 hash string buffer */
  char hashString[32 + 1] = { '\0' };

  DB_VALUE hash_string;

  assert (val != (DB_VALUE *) NULL);
  assert (result != (DB_VALUE *) NULL);
  assert (val != result);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (1, &tmp_val);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (1, val))
    {
      DB_MAKE_NULL (result);	/* MD5(NULL) returns NULL */
      goto done;
    }

  /* arg cast type **********************************************************
   */
  val = db_value_cast_arg (val, &tmp_val, DB_TYPE_VARCHAR, &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  assert (DB_VALUE_DOMAIN_TYPE (val) == DB_TYPE_VARCHAR);

  md5_buffer (DB_PULL_STRING (val), DB_GET_STRING_LENGTH (val), hashString);

  md5_hash_to_hex (hashString, hashString);

  /* dump result as hex string */
  qstr_make_typed_string (DB_TYPE_VARCHAR, &hash_string, 32,
			  hashString, 32, DB_GET_STRING_COLLATION (val));
  hash_string.need_clear = false;
  pr_clone_value (&hash_string, result);

  if (error_status != NO_ERROR)
    {
      pr_clear_value (result);
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_val);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (result);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_val);

  return error_status;
}

/*
 * db_string_insert_substring - insert a substring into a string replacing
 *				"length" characters starting at "position"
 *
 * Arguments:
 *             src_string: string to insert into. Its value will not be
 *                         modified as the output is the "result" parameter
 *		 position: starting position
 *		   length: number of character to replace
 *	       sub_string: string to be inserted
 *		   result: string containing result.
 *
 * Returns: int
 *
 * Errors:
 *      ER_OBJ_INVALID_ARGUMENTS     :
 *         <str_string> or <delim_string> is not a string type,
 *         <count> is not an integer type
 *	ER_QSTR_INCOMPATIBLE_CODE_SETS:
 *	   <str_string> or <delim_string> are not compatible
 *
 */

int
db_string_insert_substring (DB_VALUE * src_string,
			    const DB_VALUE * position,
			    const DB_VALUE * length,
			    DB_VALUE * sub_string, DB_VALUE * result)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_src_string, tmp_sub_string, tmp_position, tmp_length;

  int position_i = 0, length_i = 0;
  DB_VALUE string1, string2;
  int src_length = 0;
  int result_size = 0;
  DB_VALUE partial_result;
  INTL_CODESET src_cs, substr_cs;
  int src_coll, substr_coll;

  /*
   *  Assert that DB_VALUE structures have been allocated.
   */
  assert (src_string != (DB_VALUE *) NULL);
  assert (sub_string != (DB_VALUE *) NULL);
  assert (position != (DB_VALUE *) NULL);
  assert (length != (DB_VALUE *) NULL);
  assert (result != (DB_VALUE *) NULL);

  /*
   *  Initialize values
   */
  DB_MAKE_NULL (&string1);
  DB_MAKE_NULL (&string2);
  DB_MAKE_NULL (&partial_result);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (4, &tmp_src_string, &tmp_sub_string, &tmp_position,
		      &tmp_length);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (4, src_string, sub_string, position, length))
    {
      DB_MAKE_NULL (result);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  src_string =
    db_value_cast_arg (src_string, &tmp_src_string, DB_TYPE_VARCHAR,
		       &error_status);
  sub_string =
    db_value_cast_arg (sub_string, &tmp_sub_string, DB_TYPE_VARCHAR,
		       &error_status);
  position =
    db_value_cast_arg (position, &tmp_position, DB_TYPE_INTEGER,
		       &error_status);
  length =
    db_value_cast_arg (length, &tmp_length, DB_TYPE_INTEGER, &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  /*
   *  Categorize the parameters into respective code sets.
   *  Verify that the parameters are both character strings.
   *  Verify that the input strings belong to compatible code sets.
   */
  if (qstr_get_category (src_string) != qstr_get_category (sub_string))
    {
      error_status = ER_QSTR_INCOMPATIBLE_CODE_SETS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  src_cs = INTL_CODESET_UTF8;
  src_coll = DB_IS_NULL (src_string) ? LANG_SYS_COLLATION :
    DB_GET_STRING_COLLATION (src_string);

  substr_cs = INTL_CODESET_UTF8;
  substr_coll = DB_IS_NULL (sub_string) ? LANG_SYS_COLLATION :
    DB_GET_STRING_COLLATION (sub_string);

  position_i = DB_GET_INT (position);
  length_i = DB_GET_INT (length);

  src_length = DB_GET_STRING_LENGTH (src_string);

  if (position_i <= 0 || position_i > src_length + 1)
    {
      /* return the source string */
      error_status = pr_clone_value ((DB_VALUE *) src_string, result);
      result_size = DB_GET_STRING_SIZE (src_string);
    }
  else
    {
      DB_DATA_STATUS data_status;
      /*  result = string1 + substring + string2 */

      /* string1 = left(string,position) */
      error_status =
	db_string_make_empty_typed_string (NULL, &string1, DB_TYPE_VARCHAR,
					   TP_FLOATING_PRECISION_VALUE,
					   src_coll);
      if (error_status != NO_ERROR)
	{
	  goto exit_on_error;
	}

      if (position_i > 1)
	{
	  DB_VALUE start_val, len_val;

	  DB_MAKE_INTEGER (&start_val, 1);
	  DB_MAKE_INTEGER (&len_val, position_i - 1);
	  pr_clear_value (&string1);

	  error_status = db_string_substring (SUBSTRING, src_string,
					      &start_val, &len_val, &string1);
	  if (error_status != NO_ERROR)
	    {
	      goto exit_on_error;
	    }
	}

      /* string2 = susbtring(string,position+len) */
      error_status =
	db_string_make_empty_typed_string (NULL, &string2, DB_TYPE_VARCHAR,
					   TP_FLOATING_PRECISION_VALUE,
					   src_coll);

      if (error_status != NO_ERROR)
	{
	  goto exit_on_error;
	}

      /* get string2 if the conditions are fullfilled :
       * 1. length_i >= 0 - compatibility with MySql
       *                  (if len is negative, no remainder is concatenated)
       * 2. (position_i + length_i) <= src_length
       *                  - check the start boundary for substring
       */
      if ((length_i >= 0) && ((position_i + length_i) <= src_length))
	{
	  DB_VALUE start_val, len_val;

	  DB_MAKE_INTEGER (&start_val, position_i + length_i);
	  DB_MAKE_INTEGER (&len_val,
			   src_length - (position_i + length_i) + 1);
	  pr_clear_value (&string2);

	  error_status = db_string_substring (SUBSTRING, src_string,
					      &start_val, &len_val, &string2);
	  if (error_status != NO_ERROR)
	    {
	      goto exit_on_error;
	    }
	}

      /* partial_result = concat(string1,substring) */
      error_status =
	db_string_concatenate (&string1, sub_string, &partial_result,
			       &data_status);
      if (error_status != NO_ERROR)
	{
	  goto exit_on_error;
	}
      if (data_status != DATA_STATUS_OK)
	{
	  /* This should never happen as the partial_result is a VAR[N]CHAR */
	  assert (false);
	  error_status = ER_FAILED;
	  goto exit_on_error;
	}

      /* result = concat(partial_result,string2) */
      error_status = db_string_concatenate (&partial_result, &string2, result,
					    &data_status);
      if (error_status != NO_ERROR)
	{
	  goto exit_on_error;
	}

      if (data_status != DATA_STATUS_OK)
	{
	  /* This should never happen as the result is a VAR[N]CHAR */
	  assert (false);
	  error_status = ER_FAILED;
	  goto exit_on_error;
	}

      result_size = DB_GET_STRING_SIZE (result);
    }

  result->need_clear = true;

  if (error_status != NO_ERROR)
    {
      pr_clear_value (result);
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  pr_clear_value (&string1);
  pr_clear_value (&string2);
  pr_clear_value (&partial_result);

  db_value_clear_nargs (4, &tmp_src_string, &tmp_sub_string, &tmp_position,
			&tmp_length);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (result);

  /* arg clear tmp **********************************************************
   */
  pr_clear_value (&string1);
  pr_clear_value (&string2);
  pr_clear_value (&partial_result);

  db_value_clear_nargs (4, &tmp_src_string, &tmp_sub_string, &tmp_position,
			&tmp_length);

  return error_status;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
    ELT(index, arg1, arg2, arg3, ...)

    Clones into result the argument with the index given by the first
    argument.

    Returns: NO_ERROR or an error code
*/
int
db_string_elt (DB_VALUE * result, DB_VALUE * arg[], int const num_args)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_arg;
  int index = 0;

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (1, &tmp_arg);

  /* arg check null *********************************************************
   */
  if (num_args <= 0)
    {
      DB_MAKE_NULL (result);
      goto done;
    }

  if (DB_IS_NULL_NARGS (1, arg[0]))
    {
      DB_MAKE_NULL (result);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  arg[0] =
    db_value_cast_arg (arg[0], &tmp_arg, DB_TYPE_INTEGER, &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  assert (DB_VALUE_DOMAIN_TYPE (arg[0]) == DB_TYPE_INTEGER);

  index = DB_GET_INT (arg[0]);

  if (index > 0 && index < num_args)
    {
      pr_clone_value (arg[index], result);
    }
  else
    {
      DB_MAKE_NULL (result);
    }

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_arg);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (result);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_arg);

  return error_status;
}

/*
 * db_string_byte_length
 *
 * Arguments:
 *          string: (IN)  Input string of which the byte count is desired.
 *      byte_count: (OUT) The number of bytes in string.
 *
 * Returns: int
 *
 * Errors:
 *      ER_OBJ_INVALID_ARGUMENTS:
 *          <string> is not a string type
 *
 * Note:
 *   This function returns the number of bytes in <string>.
 *
 *   If the NULL flag is set for <string>, then the NULL flag
 *   for the <byte_count> is set.
 *
 * Assert:
 *   1. string     != (DB_VALUE *) NULL
 *   2. byte_count != (DB_VALUE *) NULL
 *
 */

int
db_string_byte_length (const DB_VALUE * string, DB_VALUE * byte_count)
{
  int error_status = NO_ERROR;
  DB_TYPE str_type;

  /*
   *  Assert that DB_VALUE structures have been allocated.
   */
  assert (string != (DB_VALUE *) NULL);
  assert (byte_count != (DB_VALUE *) NULL);

  /*
   *  Verify that the input string is a valid character
   *  string.  Bit strings are not allowed.
   *
   *  If the input string is a NULL, then set
   *  the output null flag.
   *
   *  Otherwise, calculte the byte size.
   */

  str_type = DB_VALUE_DOMAIN_TYPE (string);
  if (!QSTR_IS_CHAR_OR_BIT (str_type))
    {
      error_status = ER_OBJ_INVALID_ARGUMENTS;
    }
  else if (DB_IS_NULL (string))
    {
      db_value_domain_init (byte_count, DB_TYPE_INTEGER, DB_DEFAULT_PRECISION,
			    DB_DEFAULT_SCALE);
    }
  else
    {
      DB_MAKE_INTEGER (byte_count, DB_GET_STRING_SIZE (string));
    }

  return error_status;
}

/*
 * db_string_bit_length () -
 *
 * Arguments:
 *          string: Inpute string of which the bit length is desired.
 *  bit_count(out): Bit count of string.
 *
 * Returns: int
 *
 * Errors:
 *      ER_OBJ_INVALID_ARGUMENTS:
 *          <string> is not a string type
 *
 * Note:
 *   This function returns the number of bits in <string>.
 *
 *   If the NULL flag is set for <string>, then the NULL flag
 *   for the <bit_count> is set.
 *
 * Assert:
 *   1. string    != (DB_VALUE *) NULL
 *   2. bit_count != (DB_VALUE *) NULL
 *
 */
int
db_string_bit_length (const DB_VALUE * string, DB_VALUE * bit_count)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_string;

  /*
   *  Assert that DB_VALUE structures have been allocated.
   */
  assert (string != (DB_VALUE *) NULL);
  assert (bit_count != (DB_VALUE *) NULL);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (1, &tmp_string);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (1, string))
    {
      db_value_domain_init (bit_count, DB_TYPE_INTEGER,
			    DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  string =
    db_value_cast_arg (string, &tmp_string, DB_TYPE_VARCHAR, &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  assert (DB_VALUE_DOMAIN_TYPE (string) == DB_TYPE_VARCHAR);
  assert (qstr_get_category (string) == QSTR_VARCHAR);

  /*
   *  Verify that the input string is a valid character string.
   *  Bit strings are not allowed.
   *
   *  If the input string is a NULL, then set the output null flag.
   *
   *  If the input parameter is valid, then extract the byte length
   *  of the string.
   */

  DB_MAKE_INTEGER (bit_count, (DB_GET_STRING_SIZE (string) * BITS_IN_BYTE));

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_string);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (bit_count);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_string);

  return error_status;
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * db_string_char_length () -
 *
 * Arguments:
 *          string: String for which the number of characters is desired.
 *      char_count: Number of characters in string.
 *
 * Returns: int
 *
 * Errors:
 *      ER_OBJ_INVALID_ARGUMENTS:
 *          <string> is not a character string
 *
 * Note:
 *   This function returns the number of characters in <string>.
 *
 *   If the NULL flag is set for <string>, then the NULL flag
 *   for the <char_count> is set.
 *
 * Assert:
 *   1. string     != (DB_VALUE *) NULL
 *   2. char_count != (DB_VALUE *) NULL
 *
 */

int
db_string_char_length (const DB_VALUE * string, DB_VALUE * char_count)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_string;

  /*
   *  Assert that DB_VALUE structures have been allocated.
   */
  assert (string != (DB_VALUE *) NULL);
  assert (char_count != (DB_VALUE *) NULL);
  assert (string != char_count);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (1, &tmp_string);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (1, string))
    {
      db_value_domain_init (char_count, DB_TYPE_INTEGER,
			    DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  string =
    db_value_cast_arg (string, &tmp_string, DB_TYPE_VARCHAR, &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  assert (DB_VALUE_DOMAIN_TYPE (string) == DB_TYPE_VARCHAR);
  assert (qstr_get_category (string) == QSTR_VARCHAR);

  /*
   *  Verify that the input string is a valid character
   *  string.  Bit strings are not allowed.
   *
   *  If the input string is a NULL, then set the output null flag.
   *
   *  If the input parameter is valid, then extract the character
   *  length of the string.
   */

  DB_MAKE_INTEGER (char_count, DB_GET_STRING_LENGTH (string));

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_string);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (char_count);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_string);

  return error_status;
}

/*
 * db_string_lower () -
 *
 * Arguments:
 *            string: Input string that will be converted to lower case.
 *      lower_string: Output converted string.
 *
 * Returns: int
 *
 * Errors:
 *      ER_OBJ_INVALID_ARGUMENTS     :
 *         <string> is not a character string.
 *
 * Note:
 *   This function returns a string with all uppercase ASCII
 *   and LATIN alphabetic characters converted to lowercase.
 *
 *   If the NULL flag is set for <string>, then the NULL flag
 *   for the <lower_string> is set.
 *
 *   The <lower_string> value structure will be cloned from <string>.
 *   <lower_string> should be cleared with pr_clone_value() if it has
 *   already been initialized or DB_MAKE_NULL if it has not been
 *   previously used by the system.
 *
 * Assert:
 *
 *   1. string       != (DB_VALUE *) NULL
 *   2. lower_string != (DB_VALUE *) NULL
 *
 */

int
db_string_lower (const DB_VALUE * string, DB_VALUE * lower_string)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_string;

  unsigned char *lower_str;
  int lower_size;
  int src_length;
  const ALPHABET_DATA *alphabet;

  int lower_length;

  /*
   *  Assert that DB_VALUE structures have been allocated.
   */
  assert (string != (DB_VALUE *) NULL);
  assert (lower_string != (DB_VALUE *) NULL);
  assert (string != lower_string);

  /*
   *  Categorize the two input parameters and check for errors.
   *    Verify that the parameters are both character strings.
   */

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (1, &tmp_string);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (1, string))
    {
      DB_MAKE_NULL (lower_string);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  string =
    db_value_cast_arg (string, &tmp_string, DB_TYPE_VARCHAR, &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  assert (DB_VALUE_DOMAIN_TYPE (string) == DB_TYPE_VARCHAR);

  alphabet = lang_user_alphabet_w_coll (DB_GET_STRING_COLLATION (string));

  src_length = DB_GET_STRING_LENGTH (string);
  lower_size =
    intl_lower_string_size (alphabet,
			    (unsigned char *) DB_PULL_STRING (string),
			    DB_GET_STRING_SIZE (string), src_length);

  lower_str = (unsigned char *) malloc (lower_size + 1);
  if (lower_str == NULL)
    {
      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
      goto exit_on_error;
    }

  lower_length = TP_FLOATING_PRECISION_VALUE;

  intl_lower_string (alphabet,
		     (unsigned char *) DB_PULL_STRING (string),
		     lower_str, src_length);
  lower_str[lower_size] = 0;

  if (DB_VALUE_PRECISION (string) != TP_FLOATING_PRECISION_VALUE)
    {
      intl_char_count (lower_str, lower_size, &lower_length);
    }
  qstr_make_typed_string (DB_TYPE_VARCHAR, lower_string, lower_length,
			  (char *) lower_str, lower_size,
			  DB_GET_STRING_COLLATION (string));
  lower_string->need_clear = true;


  if (error_status != NO_ERROR)
    {
      pr_clear_value (lower_string);
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_string);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (lower_string);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_string);

  return error_status;
}

/*
 * db_string_upper () -
 *
 * Arguments:
 *            string: Input string that will be converted to upper case.
 *      lower_string: Output converted string.
 *
 * Returns: int
 *
 * Errors:
 *      ER_OBJ_INVALID_ARGUMENTS     :
 *         <string> is not a character string.
 *
 * Note:
 *
 *   This function returns a string with all lowercase ASCII
 *   and LATIN alphabetic characters converted to uppercase.
 *
 *   If the NULL flag is set for <string>, then the NULL flag
 *   for the <upper_string> is set.
 *
 *   The <upper_string> value structure will be cloned from <string>.
 *   <upper_string> should be cleared with pr_clone_value() if it has
 *   already been initialized or DB_MAKE_NULL if it has not been
 *   previously used by the system.
 *
 * Assert:
 *
 *   1. string       != (DB_VALUE *) NULL
 *   2. upper_string != (DB_VALUE *) NULL
 *
 */

int
db_string_upper (const DB_VALUE * string, DB_VALUE * upper_string)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_string;

  unsigned char *upper_str;
  int upper_size, src_length;
  const ALPHABET_DATA *alphabet;

  int upper_length;

  /*
   *  Assert that DB_VALUE structures have been allocated.
   */
  assert (string != (DB_VALUE *) NULL);
  assert (upper_string != (DB_VALUE *) NULL);
  assert (string != upper_string);

  /*
   *  Categorize the two input parameters and check for errors.
   *    Verify that the parameters are both character strings.
   */

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (1, &tmp_string);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (1, string))
    {
      DB_MAKE_NULL (upper_string);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  string =
    db_value_cast_arg (string, &tmp_string, DB_TYPE_VARCHAR, &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  assert (DB_VALUE_DOMAIN_TYPE (string) == DB_TYPE_VARCHAR);

  alphabet = lang_user_alphabet_w_coll (DB_GET_STRING_COLLATION (string));

  src_length = DB_GET_STRING_LENGTH (string);
  upper_size =
    intl_upper_string_size (alphabet,
			    (unsigned char *) DB_PULL_STRING (string),
			    DB_GET_STRING_SIZE (string), src_length);

  upper_str = (unsigned char *) malloc (upper_size + 1);
  if (upper_str == NULL)
    {
      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
      goto exit_on_error;
    }

  upper_length = TP_FLOATING_PRECISION_VALUE;

  intl_upper_string (alphabet,
		     (unsigned char *) DB_PULL_STRING (string),
		     upper_str, src_length);

  upper_str[upper_size] = 0;
  if (DB_VALUE_PRECISION (string) != TP_FLOATING_PRECISION_VALUE)
    {
      intl_char_count (upper_str, upper_size, &upper_length);
    }
  qstr_make_typed_string (DB_TYPE_VARCHAR, upper_string, upper_length,
			  (char *) upper_str, upper_size,
			  DB_GET_STRING_COLLATION (string));
  upper_string->need_clear = true;

  if (error_status != NO_ERROR)
    {
      pr_clear_value (upper_string);
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_string);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (upper_string);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_string);

  return error_status;
}

/*
 * db_string_trim () -
 *
 * Arguments:
 *        trim_operand: Specifies whether the character to be trimmed is
 *                      removed from the beginning, ending or both ends
 *                      of the string.
 *        trim_charset: (Optional) The characters to be removed.
 *          src_string: String to remove trim character from.
 *      trimmed_string: Resultant trimmed string.
 *
 * Returns: int
 *
 * Errors:
 *      ER_OBJ_INVALID_ARGUMENTS     : <trim_char> or <src_string> are
 *                                     not character strings.
 *      ER_QSTR_INVALID_TRIM_OPERAND  : <trim_char> has char length > 1.
 *      ER_QSTR_INCOMPATIBLE_CODE_SETS: <trim_char>, <src_string> and
 *                                     <trimmed_string> have different
 *                                     character code sets.
 *
 */

int
db_string_trim (const MISC_OPERAND tr_operand,
		const DB_VALUE * trim_charset,
		const DB_VALUE * src_string, DB_VALUE * trimmed_string)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_src_string, tmp_trim_charset;

  bool trim_charset_is_null = false;

  unsigned char *result;
  int result_length, result_size = 0, result_domain_length;
  DB_TYPE result_type = DB_TYPE_NULL;

  unsigned char *trim_charset_ptr = NULL;
  int trim_charset_length = 0;
  int trim_charset_size = 0;

  /*
   * Assert DB_VALUE structures have been allocated
   */

  assert (trim_charset != (DB_VALUE *) NULL);
  assert (src_string != (DB_VALUE *) NULL);
  assert (trimmed_string != (DB_VALUE *) NULL);
  assert (trim_charset != trimmed_string);
  assert (src_string != trimmed_string);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (2, &tmp_src_string, &tmp_trim_charset);

  /* arg check null *********************************************************
   */
  if (DB_VALUE_DOMAIN_TYPE (trim_charset) == DB_TYPE_NULL)
    {
      trim_charset_is_null = true;
    }

  if (DB_IS_NULL (src_string)
      || (!trim_charset_is_null && DB_IS_NULL (trim_charset)))
    {
      db_value_domain_init (trimmed_string, DB_TYPE_VARCHAR,
			    DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  src_string =
    db_value_cast_arg (src_string, &tmp_src_string, DB_TYPE_VARCHAR,
		       &error_status);
  trim_charset =
    db_value_cast_arg (trim_charset, &tmp_trim_charset, DB_TYPE_VARCHAR,
		       &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  assert (DB_VALUE_DOMAIN_TYPE (src_string) == DB_TYPE_VARCHAR);

  if (!trim_charset_is_null)
    {
      if (qstr_get_category (src_string) != qstr_get_category (trim_charset))
	{
	  error_status = ER_QSTR_INCOMPATIBLE_CODE_SETS;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto exit_on_error;
	}

      trim_charset_ptr = (unsigned char *) DB_PULL_STRING (trim_charset);
      trim_charset_length = DB_GET_STRING_LENGTH (trim_charset);
      trim_charset_size = DB_GET_STRING_SIZE (trim_charset);
    }

  error_status = qstr_trim (tr_operand,
			    trim_charset_ptr,
			    trim_charset_length,
			    trim_charset_size,
			    (unsigned char *) DB_PULL_STRING (src_string),
			    DB_VALUE_DOMAIN_TYPE (src_string),
			    DB_GET_STRING_LENGTH (src_string),
			    DB_GET_STRING_SIZE (src_string),
			    &result,
			    &result_type, &result_length, &result_size);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  if (result != NULL)
    {
      result_domain_length = MIN (QSTR_MAX_PRECISION (result_type),
				  DB_VALUE_PRECISION (src_string));
      qstr_make_typed_string (result_type,
			      trimmed_string,
			      result_domain_length,
			      (char *) result, result_size,
			      DB_GET_STRING_COLLATION (src_string));
      result[result_size] = 0;
      trimmed_string->need_clear = true;
    }

  if (error_status != NO_ERROR)
    {
      pr_clear_value (trimmed_string);
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_src_string, &tmp_trim_charset);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (trimmed_string);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_src_string, &tmp_trim_charset);

  return error_status;
}

/* qstr_trim () -
*/
static int
qstr_trim (MISC_OPERAND trim_operand,
	   const unsigned char *trim_charset,
	   int trim_charset_length,
	   int trim_charset_size,
	   const unsigned char *src_ptr,
	   DB_TYPE src_type,
	   int src_length,
	   int src_size,
	   unsigned char **result,
	   DB_TYPE * result_type, int *result_length, int *result_size)
{
  unsigned char pad_char[2], *lead_trimmed_ptr, *trail_trimmed_ptr;
  int lead_trimmed_length, trail_trimmed_length;
  int lead_trimmed_size, trail_trimmed_size, pad_char_size = 0;
  int error_status = NO_ERROR;

  /* default case */
  intl_pad_char (pad_char, &pad_char_size);
  if (trim_charset_length == 0)
    {
      trim_charset = pad_char;
      trim_charset_length = 1;
      trim_charset_size = pad_char_size;
    }

  /* trim from front */
  lead_trimmed_ptr = (unsigned char *) src_ptr;
  lead_trimmed_length = src_length;
  lead_trimmed_size = src_size;

  if (trim_operand == LEADING || trim_operand == BOTH)
    {
      trim_leading (trim_charset, trim_charset_size,
		    src_ptr, src_type, src_length, src_size,
		    &lead_trimmed_ptr,
		    &lead_trimmed_length, &lead_trimmed_size);
    }

  trail_trimmed_ptr = lead_trimmed_ptr;
  trail_trimmed_length = lead_trimmed_length;
  trail_trimmed_size = lead_trimmed_size;

  if (trim_operand == TRAILING || trim_operand == BOTH)
    {
      trim_trailing (trim_charset, trim_charset_size,
		     lead_trimmed_ptr,
		     src_type,
		     lead_trimmed_length,
		     lead_trimmed_size,
		     &trail_trimmed_length, &trail_trimmed_size);
    }

  /* setup result */
  *result = (unsigned char *) malloc ((size_t) trail_trimmed_size + 1);
  if (*result == NULL)
    {
      error_status = er_errid ();
      return error_status;
    }

  (void) memcpy ((char *) (*result), (char *) trail_trimmed_ptr,
		 trail_trimmed_size);
  (*result)[trail_trimmed_size] = '\0';

  *result_type = DB_TYPE_VARCHAR;
  *result_length = trail_trimmed_length;
  *result_size = trail_trimmed_size;

  return error_status;
}

/*
 * trim_leading () -
 *
 * Arguments:
 *       trim_charset_ptr: (in)  Single character trim string.
 *      trim_charset_size: (in)  Size of trim string.
 *         src_string_ptr: (in)  Source string to be trimmed.
 *      src_string_length: (in)  Length of source string.
 *       lead_trimmed_ptr: (out) Pointer to start of trimmed string.
 *    lead_trimmed_length: (out) Length of trimmed string.
 *
 * Returns: nothing
 *
 * Errors:
 *
 * Note:
 *     Remove trim character from the front of the source string.  No
 *     characters are actually removed.  Instead, the function returns
 *     a pointer to the beginning of the source string after the trim
 *     characters and the resultant length of the string.
 *
 */
static void
trim_leading (const unsigned char *trim_charset_ptr,
	      int trim_charset_size,
	      const unsigned char *src_ptr,
	      UNUSED_ARG DB_TYPE src_type,
	      int src_length,
	      int src_size,
	      unsigned char **lead_trimmed_ptr,
	      int *lead_trimmed_length, int *lead_trimmed_size)
{
  int cur_src_char_size, cur_trim_char_size;
  unsigned char *cur_src_char_ptr, *cur_trim_char_ptr;

  int cmp_flag = 0;

  *lead_trimmed_ptr = (unsigned char *) src_ptr;
  *lead_trimmed_length = src_length;
  *lead_trimmed_size = src_size;

  /* iterate for source string */
  for (cur_src_char_ptr = (unsigned char *) src_ptr;
       cur_src_char_ptr < src_ptr + src_size;)
    {
      for (cur_trim_char_ptr = (unsigned char *) trim_charset_ptr;
	   cur_src_char_ptr < (src_ptr + src_size)
	   && (cur_trim_char_ptr < trim_charset_ptr + trim_charset_size);)
	{
	  intl_char_size (cur_src_char_ptr, 1, &cur_src_char_size);
	  intl_char_size (cur_trim_char_ptr, 1, &cur_trim_char_size);

	  if (cur_src_char_size != cur_trim_char_size)
	    {
	      return;
	    }

	  cmp_flag =
	    memcmp ((char *) cur_src_char_ptr, (char *) cur_trim_char_ptr,
		    cur_trim_char_size);
	  if (cmp_flag != 0)
	    {
	      return;
	    }

	  cur_src_char_ptr += cur_src_char_size;
	  cur_trim_char_ptr += cur_trim_char_size;
	}

      if (cur_trim_char_ptr >= trim_charset_ptr + trim_charset_size)
	{			/* all string matched */
	  *lead_trimmed_length -= trim_charset_size;
	  *lead_trimmed_size -= trim_charset_size;
	  *lead_trimmed_ptr += trim_charset_size;
	}
    }
}

/*
 * trim_trailing () -
 *
 * Arguments:
 *       trim_charset_ptr: (in)  Single character trim string.
 *      trim_charset_size: (in)  Size of trim string.
 *                src_ptr: (in)  Source string to be trimmed.
 *             src_length: (in)  Length of source string.
 *   trail_trimmed_length: (out) Length of trimmed string.
 *
 * Returns: nothing
 *
 * Errors:
 *
 * Note:
 *     Remove trim character from the end of the source string.  No
 *     characters are actually removed.  Instead, the function returns
 *     a pointer to the beginning of the source string after the trim
 *     characters and the resultant length of the string.
 *
 */
static void
trim_trailing (const unsigned char *trim_charset_ptr,
	       int trim_charset_size,
	       const unsigned char *src_ptr,
	       UNUSED_ARG DB_TYPE src_type,
	       int src_length,
	       int src_size,
	       int *trail_trimmed_length, int *trail_trimmed_size)
{
  int prev_src_char_size, prev_trim_char_size;
  unsigned char *cur_src_char_ptr, *cur_trim_char_ptr;
  unsigned char *prev_src_char_ptr, *prev_trim_char_ptr;
  int cmp_flag = 0;

  *trail_trimmed_length = src_length;
  *trail_trimmed_size = src_size;

  /* iterate for source string */
  for (cur_src_char_ptr = (unsigned char *) src_ptr + src_size;
       cur_src_char_ptr > src_ptr;)
    {
      for (cur_trim_char_ptr =
	   (unsigned char *) trim_charset_ptr + trim_charset_size;
	   cur_trim_char_ptr > trim_charset_ptr
	   && cur_src_char_ptr > src_ptr;)
	{
	  /* get previous letter */
	  prev_src_char_ptr =
	    intl_prev_char (cur_src_char_ptr, src_ptr, &prev_src_char_size);
	  prev_trim_char_ptr =
	    intl_prev_char (cur_trim_char_ptr, trim_charset_ptr,
			    &prev_trim_char_size);

	  if (prev_trim_char_size != prev_src_char_size)
	    {
	      return;
	    }

	  cmp_flag = memcmp ((char *) prev_src_char_ptr,
			     (char *) prev_trim_char_ptr,
			     prev_trim_char_size);
	  if (cmp_flag != 0)
	    {
	      return;
	    }

	  cur_src_char_ptr -= prev_src_char_size;
	  cur_trim_char_ptr -= prev_trim_char_size;
	}

      if (cur_trim_char_ptr <= trim_charset_ptr)
	{
	  *trail_trimmed_length -= trim_charset_size;
	  *trail_trimmed_size -= trim_charset_size;
	}
    }
}

/*
 * db_string_pad () -
 *
 * Arguments:
 *      pad_operand: (in)  Left or Right padding?
 *       src_string: (in)  Source string to be padded.
 *       pad_length: (in)  Length of padded string
 *      pad_charset: (in)  Padding char set
 *    padded_string: (out) Padded string
 *
 * Returns: nothing
 */
int
db_string_pad (const MISC_OPERAND pad_operand, const DB_VALUE * src_string,
	       const DB_VALUE * pad_length, const DB_VALUE * pad_charset,
	       DB_VALUE * padded_string)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_src_string, tmp_pad_length, tmp_pad_charset;
  int total_length;
  unsigned char *result;
  int result_length = 0, result_size = 0;
  DB_TYPE result_type;

  assert (src_string != (DB_VALUE *) NULL);
  assert (padded_string != (DB_VALUE *) NULL);
  assert (src_string != padded_string);
  assert (pad_length != padded_string);
  assert (pad_charset != padded_string);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (3, &tmp_src_string, &tmp_pad_length, &tmp_pad_charset);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (3, src_string, pad_length, pad_charset))
    {
      db_value_domain_init (padded_string, DB_TYPE_VARCHAR,
			    DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  src_string =
    db_value_cast_arg (src_string, &tmp_src_string, DB_TYPE_VARCHAR,
		       &error_status);
  pad_length =
    db_value_cast_arg (pad_length, &tmp_pad_length, DB_TYPE_INTEGER,
		       &error_status);
  pad_charset =
    db_value_cast_arg (pad_charset, &tmp_pad_charset, DB_TYPE_VARCHAR,
		       &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  total_length = DB_GET_INTEGER (pad_length);

#if 1				/* TODO - */
  if (total_length <= 0)
    {
      /*error_status = ER_QPROC_INVALID_PARAMETER; */

      db_value_domain_init (padded_string, DB_TYPE_VARCHAR,
			    DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);

      goto done;
    }
#endif

  if (qstr_get_category (src_string) != qstr_get_category (pad_charset))
    {
      error_status = ER_QSTR_INCOMPATIBLE_CODE_SETS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  error_status = qstr_pad (pad_operand, total_length,
			   (unsigned char *) DB_PULL_STRING (pad_charset),
			   DB_GET_STRING_LENGTH (pad_charset),
			   DB_GET_STRING_SIZE (pad_charset),
			   (unsigned char *) DB_PULL_STRING (src_string),
			   DB_VALUE_DOMAIN_TYPE (src_string),
			   DB_GET_STRING_LENGTH (src_string),
			   DB_GET_STRING_SIZE (src_string),
			   &result,
			   &result_type, &result_length, &result_size);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  if (result != NULL)
    {
      qstr_make_typed_string (result_type, padded_string, result_length,
			      (char *) result, result_size,
			      DB_GET_STRING_COLLATION (src_string));

      result[result_size] = 0;
      padded_string->need_clear = true;
    }

  if (error_status != NO_ERROR)
    {
      pr_clear_value (padded_string);
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_src_string, &tmp_pad_length,
			&tmp_pad_charset);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (padded_string);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_src_string, &tmp_pad_length,
			&tmp_pad_charset);

  return error_status;
}

/*
 * qstr_pad () -
 */
static int
qstr_pad (MISC_OPERAND pad_operand,
	  int pad_length,
	  const unsigned char *pad_charset_ptr,
	  int pad_charset_length,
	  int pad_charset_size,
	  const unsigned char *src_ptr,
	  UNUSED_ARG DB_TYPE src_type,
	  int src_length,
	  int src_size,
	  unsigned char **result,
	  DB_TYPE * result_type, int *result_length, int *result_size)
{
  unsigned char def_pad_char[2];
  unsigned char *cur_pad_char_ptr;
  int def_pad_char_size = 0;	/* default padding char */
  int truncate_size, pad_size, alloc_size, cnt;
  int length_to_be_padded;	/* length that will be really padded */
  int remain_length_to_be_padded;	/* remained length that will be padded */
  int pad_full_size = 0;
  int pad_reminder_size = 0;
  int error_status = NO_ERROR;

  intl_pad_char (def_pad_char, &def_pad_char_size);

  if (pad_charset_length == 0)
    {
      pad_charset_ptr = def_pad_char;
      pad_charset_length = 1;
      pad_charset_size = def_pad_char_size;
    }

  assert (pad_charset_length > 0);

  if (src_length >= pad_length)
    {
      alloc_size = src_size;
    }
  else
    {
      pad_full_size = ((pad_length - src_length) / pad_charset_length)
	* pad_charset_size;
      intl_char_size ((unsigned char *) pad_charset_ptr,
		      (pad_length - src_length) % pad_charset_length,
		      &pad_reminder_size);
      alloc_size = src_size + pad_full_size + pad_reminder_size;
    }

  *result_type = DB_TYPE_VARCHAR;

  *result = (unsigned char *) malloc ((size_t) alloc_size + 1);
  if (*result == NULL)
    {
      error_status = er_errid ();
      return error_status;
    }

  /*
   * now start padding
   */

  /* if source length is greater than pad_length */
  if (src_length >= pad_length)
    {
      truncate_size = 0;	/* SIZE to be cut */
      intl_char_size ((unsigned char *) src_ptr, pad_length, &truncate_size);
      memcpy ((char *) (*result), (char *) src_ptr, truncate_size);

      *result_length = pad_length;
      *result_size = truncate_size;

      return error_status;
    }

  /*
   * Get real length to be paded
   * if source length is greater than pad_length
   */

  length_to_be_padded = pad_length - src_length;

  /* pad heading first */

  cnt = 0;			/* how many times copy pad_char_set */
  pad_size = 0;			/* SIZE of padded char */
  remain_length_to_be_padded = 0;

  for (; cnt < (length_to_be_padded / pad_charset_length); cnt++)
    {
      (void) memcpy ((char *) (*result) + pad_charset_size * cnt,
		     (char *) pad_charset_ptr, pad_charset_size);
    }
  pad_size = pad_charset_size * cnt;
  remain_length_to_be_padded = (pad_length - src_length) % pad_charset_length;

  if (remain_length_to_be_padded != 0)
    {
      int remain_size_to_be_padded = 0;

      assert (remain_length_to_be_padded > 0);

      cur_pad_char_ptr = (unsigned char *) pad_charset_ptr;

      intl_char_size (cur_pad_char_ptr, remain_length_to_be_padded,
		      &remain_size_to_be_padded);
      (void) memcpy ((char *) (*result) + pad_size,
		     (char *) cur_pad_char_ptr, remain_size_to_be_padded);
      cur_pad_char_ptr += remain_size_to_be_padded;
      pad_size += remain_size_to_be_padded;
    }

  memcpy ((char *) (*result) + pad_size, src_ptr, src_size);

  if (pad_operand == TRAILING)
    {				/* switch source and padded string */
      memmove ((char *) (*result) + src_size, (char *) (*result), pad_size);
      memcpy ((char *) (*result), src_ptr, src_size);
    }

  pad_size += src_size;

  *result_length = pad_length;
  *result_size = pad_size;

  return error_status;
}

/*
 * db_string_like () -
 *
 * Arguments:
 *             src_string:  (IN) Source string.
 *                pattern:  (IN) Pattern string which can contain % and _
 *                               characters.
 *               esc_char:  (IN) Optional escape character.
 *                 result: (OUT) Integer result.
 *
 * Returns: int
 *
 * Errors:
 *      ER_OBJ_INVALID_ARGUMENTS:
 *          <src_string>, <pattern>, or <esc_char> (if it's not NULL)
 *          is not a character string.
 *
 *      ER_QSTR_INCOMPATIBLE_CODE_SETS:
 *          <src_string>, <pattern>, and <esc_char> (if it's not NULL)
 *          have different character code sets.
 *
 *      ER_QSTR_INVALID_ESCAPE_SEQUENCE:
 *          An illegal pattern is specified.
 *
 *      ER_QSTR_INVALID_ESCAPE_CHARACTER:
 *          If <esc_char> is not NULL and the length of E is > 1.
 *
 */
/* TODO ER_QSTR_INVALID_ESCAPE_CHARACTER is not checked for, although it
        probably should be (the escape sequence string should contain a single
	character)
*/

int
db_string_like (const DB_VALUE * src_string,
		const DB_VALUE * pattern,
		const DB_VALUE * esc_char, int *result)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_src_string, tmp_pattern, tmp_esc_char;
  QSTR_CATEGORY src_category = QSTR_UNKNOWN;
  QSTR_CATEGORY pattern_category = QSTR_UNKNOWN;
  char *src_char_string_p = NULL;
  char *pattern_char_string_p = NULL;
  char const *esc_char_p = NULL;
  int src_length = 0, pattern_length = 0;
  int coll_id;

  /*
   *  Assert that DB_VALUE structures have been allocated.
   */
  assert (src_string != NULL);
  assert (pattern != NULL);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (3, &tmp_src_string, &tmp_pattern, &tmp_esc_char);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL (src_string))
    {
      *result = V_UNKNOWN;
      goto done;
    }

  if (DB_IS_NULL (pattern))
    {
      *result = V_FALSE;
      goto done;
    }

  /* arg cast type **********************************************************
   */
  src_string =
    db_value_cast_arg (src_string, &tmp_src_string, DB_TYPE_VARCHAR,
		       &error_status);
  pattern =
    db_value_cast_arg (pattern, &tmp_pattern, DB_TYPE_VARCHAR, &error_status);
  if (esc_char != NULL)
    {
      esc_char =
	db_value_cast_arg (esc_char, &tmp_esc_char, DB_TYPE_VARCHAR,
			   &error_status);
    }

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  src_category = qstr_get_category (src_string);
  pattern_category = qstr_get_category (pattern);

  if (src_category != pattern_category)
    {
      error_status = ER_QSTR_INCOMPATIBLE_CODE_SETS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  LANG_RT_COMMON_COLL (DB_GET_STRING_COLLATION (src_string),
		       DB_GET_STRING_COLLATION (pattern), coll_id);
  if (coll_id == -1)
    {
      error_status = ER_QSTR_INCOMPATIBLE_COLLATIONS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  if (esc_char != NULL)
    {
      if (DB_IS_NULL (esc_char))
	{
	  /* The implicit escape character ('\\') is used if
	     (a LIKE b ESCAPE NULL) is given in the syntax */
	  esc_char_p = "\\";
	}
      else
	{
	  QSTR_CATEGORY esc_category = qstr_get_category (esc_char);
	  int esc_char_len, esc_char_size;

	  if (src_category != esc_category)
	    {
	      error_status = ER_QSTR_INCOMPATIBLE_CODE_SETS;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }

	  esc_char_p = DB_PULL_STRING (esc_char);
	  esc_char_size = DB_GET_STRING_SIZE (esc_char);

	  intl_char_count ((unsigned char *) esc_char_p,
			   esc_char_size, &esc_char_len);

	  assert (esc_char_p != NULL);
	  if (esc_char_len != 1)
	    {
	      error_status = ER_QSTR_INVALID_ESCAPE_SEQUENCE;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	}
    }

  src_char_string_p = DB_PULL_STRING (src_string);
  src_length = DB_GET_STRING_SIZE (src_string);

  pattern_char_string_p = DB_PULL_STRING (pattern);
  pattern_length = DB_GET_STRING_SIZE (pattern);

  *result = qstr_eval_like (src_char_string_p, src_length,
			    pattern_char_string_p, pattern_length,
			    esc_char_p, coll_id);

  if (*result == V_ERROR)
    {
      error_status = ER_QSTR_INVALID_ESCAPE_SEQUENCE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);
  assert (*result != V_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_src_string, &tmp_pattern, &tmp_esc_char);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  *result = V_ERROR;

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_src_string, &tmp_pattern, &tmp_esc_char);

  return error_status;
}

static void *
db_rlike_malloc (UNUSED_ARG void *dummy, size_t s)
{
  return malloc (s);
}

static void *
db_rlike_realloc (UNUSED_ARG void *dummy, void *p, size_t s)
{
  return realloc (p, s);
}

static void
db_rlike_free (UNUSED_ARG void *dummy, void *p)
{
  free (p);
}

/*
 * db_string_rlike () - check for match between string and regex
 *
 * Arguments:
 *             src_string:     (IN) Source string.
 *                pattern:     (IN) Regular expression.
 *	   case_sensitive:     (IN) Perform case sensitive matching when 1
 *	       comp_regex: (IN/OUT) Compiled regex object
 *	     comp_pattern: (IN/OUT) Compiled regex pattern
 *                 result:    (OUT) Integer result.
 *
 * Returns: int
 *
 * Errors:
 *      ER_OBJ_INVALID_ARGUMENTS:
 *          <src_string>, <pattern> (if it's not NULL)
 *          is not a character string.
 *
 *      ER_QSTR_INCOMPATIBLE_CODE_SETS:
 *          <src_string>, <pattern> (if it's not NULL)
 *          have different character code sets.
 *
 *      ER_QSTR_INVALID_ESCAPE_SEQUENCE:
 *          An illegal pattern is specified.
 *
 */

int
db_string_rlike (const DB_VALUE * src_string, const DB_VALUE * pattern,
		 const DB_VALUE * case_sensitive, cub_regex_t ** comp_regex,
		 char **comp_pattern, int *result)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_src_string, tmp_pattern, tmp_case_sensitive;
  const char *src_char_string_p = NULL;
  const char *pattern_char_string_p = NULL;
  bool is_case_sensitive = false;
  int src_length = 0, pattern_length = 0;

  char rx_err_buf[REGEX_MAX_ERROR_MSG_SIZE] = { '\0' };
  int rx_err = CUB_REG_OKAY;
  int rx_err_len = 0;
  char *rx_compiled_pattern = NULL;
  cub_regex_t *rx_compiled_regex = NULL;

  /* check for allocated DB values */
  assert (src_string != NULL);
  assert (pattern != NULL);
  assert (case_sensitive != NULL);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (3, &tmp_src_string, &tmp_pattern, &tmp_case_sensitive);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (2, src_string, pattern))
    {
      *result = V_UNKNOWN;
      goto cleanup;
    }

  if (DB_IS_NULL (case_sensitive))
    {
      error_status = ER_QPROC_INVALID_PARAMETER;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto cleanup;
    }

  /* arg cast type **********************************************************
   */
  src_string =
    db_value_cast_arg (src_string, &tmp_src_string, DB_TYPE_VARCHAR,
		       &error_status);
  pattern =
    db_value_cast_arg (pattern, &tmp_pattern, DB_TYPE_VARCHAR, &error_status);
  case_sensitive =
    db_value_cast_arg (case_sensitive, &tmp_case_sensitive, DB_TYPE_INTEGER,
		       &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto cleanup;
    }

  /* start main body ********************************************************
   */

  /* get compiled pattern */
  if (comp_pattern != NULL)
    {
      rx_compiled_pattern = *comp_pattern;
    }

  /* if regex object was specified, use local regex */
  if (comp_regex != NULL)
    {
      rx_compiled_regex = *comp_regex;
    }

  /* type checking */
  if (qstr_get_category (src_string) != qstr_get_category (pattern))
    {
      error_status = ER_QSTR_INCOMPATIBLE_CODE_SETS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto cleanup;
    }

  src_char_string_p = DB_PULL_STRING (src_string);
  src_length = DB_GET_STRING_SIZE (src_string);

  pattern_char_string_p = DB_PULL_STRING (pattern);
  pattern_length = DB_GET_STRING_SIZE (pattern);

  /* initialize regex library memory allocator */
  cub_regset_malloc ((CUB_REG_MALLOC) db_rlike_malloc);
  cub_regset_realloc ((CUB_REG_REALLOC) db_rlike_realloc);
  cub_regset_free ((CUB_REG_FREE) db_rlike_free);

  /* extract case sensitivity */
  is_case_sensitive = (case_sensitive->data.i != 0);

  /* check for re-compile */
  if (rx_compiled_pattern == NULL || rx_compiled_regex == NULL
      || pattern_length != strlen (rx_compiled_pattern)
      || strncmp (rx_compiled_pattern, pattern_char_string_p,
		  pattern_length) != 0)
    {
      /* regex must be re-compiled if regex object is not specified, pattern is
         not specified or compiled pattern does not match current pattern */

      /* update compiled pattern */
      if (rx_compiled_pattern != NULL)
	{
	  /* free old memory */
	  free_and_init (rx_compiled_pattern);
	}

      /* allocate new memory */
      rx_compiled_pattern = (char *) malloc (pattern_length + 1);

      if (rx_compiled_pattern == NULL)
	{
	  /* out of memory */
	  error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	  goto cleanup;
	}

      /* copy string */
      memcpy (rx_compiled_pattern, pattern_char_string_p, pattern_length);
      rx_compiled_pattern[pattern_length] = '\0';

      /* update compiled regex */
      if (rx_compiled_regex != NULL)
	{
	  /* free previously allocated memory */
	  cub_regfree (rx_compiled_regex);
	  free_and_init (rx_compiled_regex);
	}

      /* allocate memory for new regex object */
      rx_compiled_regex = (cub_regex_t *) malloc (sizeof (cub_regex_t));

      if (rx_compiled_regex == NULL)
	{
	  /* out of memory */
	  error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	  goto cleanup;
	}

      /* compile regex */
      rx_err = cub_regcomp (rx_compiled_regex, rx_compiled_pattern,
			    CUB_REG_EXTENDED | CUB_REG_NOSUB
			    | (is_case_sensitive ? 0 : CUB_REG_ICASE));

      if (rx_err != CUB_REG_OKAY)
	{
	  /* regex compilation error */
	  rx_err_len = cub_regerror (rx_err, rx_compiled_regex, rx_err_buf,
				     REGEX_MAX_ERROR_MSG_SIZE);

	  error_status = ER_REGEX_COMPILE_ERROR;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 1,
		  rx_err_buf);
	  goto cleanup;
	}
    }

  /* match against pattern; regexec returns zero on match */
  rx_err =
    cub_regexec (rx_compiled_regex, src_char_string_p, src_length, 0, NULL,
		 0);
  switch (rx_err)
    {
    case CUB_REG_OKAY:
      *result = V_TRUE;
      break;

    case CUB_REG_NOMATCH:
      *result = V_FALSE;
      break;

    default:
      rx_err_len = cub_regerror (rx_err, rx_compiled_regex, rx_err_buf,
				 REGEX_MAX_ERROR_MSG_SIZE);
      error_status = ER_REGEX_EXEC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 1, rx_err_buf);
      break;
    }

cleanup:

  if ((comp_regex == NULL || error_status != NO_ERROR)
      && rx_compiled_regex != NULL)
    {
      /* free memory if (using local regex) or (error occurred) */
      cub_regfree (rx_compiled_regex);
      free_and_init (rx_compiled_regex);
    }

  if ((comp_pattern == NULL || error_status != NO_ERROR)
      && rx_compiled_pattern != NULL)
    {
      /* free memory if (using local pattern) or (error occurred) */
      free_and_init (rx_compiled_pattern);
    }

  if (comp_regex != NULL)
    {
      /* pass compiled regex object out */
      *comp_regex = rx_compiled_regex;
    }

  if (comp_pattern != NULL)
    {
      /* pass compiled pattern out */
      *comp_pattern = rx_compiled_pattern;
    }

  if (error_status != NO_ERROR)
    {
      if (er_errid () == NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
		  0);
	}
      *result = V_ERROR;
    }

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_src_string, &tmp_pattern,
			&tmp_case_sensitive);

  return error_status;
}

/*
 * db_string_limit_size_string () - limits the size of a string. It limits
 *				    the size of value, but in case of fixed
 *				    length values, it limits also the domain
 *				    precision.
 *
 * Arguments:
 *              src: (IN)  String variable.
 *	     result: (OUT) Variable with new size
 *	   new_size: (IN)  New size for the string (in bytes).
 *	spare_bytes: (OUT) the number of bytes that could fit from last
 *		      truncated character
 *
 * Returns:
 *
 * Errors:
 *	ER_OBJ_INVALID_ARGUMENTS:
 *		  <src_string> is not CHAR, NCHAR, VARCHAR, VARNCHAR, VARBINARY
 *
 * Note : result variable must already be created
 *	  operates directly on memory buffer
 *	  if the new size is greater than the source, it clones the input
 *	  The truncation of domain size in case of fixed domain argument
 *	  is needed in context of GROUP_CONCAT, when the result needs to be
 *	  truncated.
 *	  The full-char adjusting code in this function is specific to
 *	  GROUP_CONCAT.
 */
int
db_string_limit_size_string (DB_VALUE * src_string, DB_VALUE * result,
			     const int new_size, int *spare_bytes)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_src_string;
  int result_size = 0, src_size = 0, src_domain_precision = 0;
  unsigned char *r;
  int char_count = 0, adj_char_size = 0;

  assert (src_string != (DB_VALUE *) NULL);
  assert (result != (DB_VALUE *) NULL);
  assert (new_size >= 0);
  assert (spare_bytes != NULL);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (1, &tmp_src_string);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (1, src_string))
    {
#if 1				/* TODO - */
      error_status = ER_OBJ_INVALID_ARGUMENTS;
      goto exit_on_error;
#endif
    }

  if (new_size < 0)
    {
      error_status = ER_OBJ_INVALID_ARGUMENTS;
      goto exit_on_error;
    }

  /* arg cast type **********************************************************
   */
  src_string =
    db_value_cast_arg (src_string, &tmp_src_string, DB_TYPE_VARCHAR,
		       &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  src_size = DB_GET_STRING_SIZE (src_string);
  src_domain_precision = DB_VALUE_PRECISION (src_string);

  *spare_bytes = 0;

  if (src_size <= 0 || new_size >= src_size)
    {
      assert (error_status == NO_ERROR);
      error_status = pr_clone_value (src_string, result);
      if (error_status != NO_ERROR)
	{
	  goto exit_on_error;
	}

      goto done;
    }

  result_size = new_size;

  /* Adjust size to a full character.
   */
  intl_char_count ((unsigned char *) DB_PULL_STRING (src_string), result_size,
		   &char_count);
  intl_char_size ((unsigned char *) DB_PULL_STRING (src_string), char_count,
		  &adj_char_size);

  assert (adj_char_size <= result_size);

  /* Allocate storage for the result string */
  r = (unsigned char *) malloc ((size_t) result_size + 1);
  if (r == NULL)
    {
      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
      goto exit_on_error;
    }

  memset (r, 0, (size_t) result_size + 1);

  if (adj_char_size > 0)
    {
      memcpy ((char *) r, (char *) DB_PULL_STRING (src_string),
	      adj_char_size);
    }

  qstr_make_typed_string (DB_TYPE_VARCHAR,
			  result,
			  src_domain_precision, (char *) r, adj_char_size,
			  DB_GET_STRING_COLLATION (src_string));
  result->need_clear = true;

  *spare_bytes = result_size - adj_char_size;

  if (error_status != NO_ERROR)
    {
      pr_clear_value (result);
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_src_string);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (result);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_src_string);

  return error_status;
}

/*
 * db_string_fix_string_size () - fixes the size of a string according to its
 *				  content (NULL terminator)
 *
 * Arguments:
 *            src: (IN/OUT)  String variable.
 *
 * Returns:
 *
 * Errors:
 *	ER_OBJ_INVALID_ARGUMENTS:
 *		  <src_string> is not CHAR, NCHAR, VARCHAR, VARNCHAR
 *
 * Note : Used in context of GROUP_CONCAT. It is complementary to
 *	  'db_string_limit_size_string' function
 */
int
db_string_fix_string_size (DB_VALUE * src_string)
{
  int val_size = 0;
  int string_size = 0;
  int error_status = NO_ERROR;
  DB_TYPE src_type;
  bool save_need_clear;

  assert (src_string != (DB_VALUE *) NULL);

  src_type = DB_VALUE_DOMAIN_TYPE (src_string);
  save_need_clear = src_string->need_clear;

  if (!QSTR_IS_CHAR (src_type))
    {
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  val_size = DB_GET_STRING_SIZE (src_string);
  /* this is a system generated string; it must have the null terminator */
  string_size = strlen (DB_PULL_STRING (src_string));
  assert (val_size >= string_size);

  qstr_make_typed_string (src_type, src_string,
			  DB_VALUE_PRECISION (src_string),
			  DB_PULL_STRING (src_string), string_size,
			  DB_GET_STRING_COLLATION (src_string));
  src_string->need_clear = save_need_clear;

  return error_status;
}

/*
 * qstr_eval_like () -
 */
/* TODO this function should be modified to not rely on the special value 1
        in the situation of no escape character. With the current
	implementation it will incorrectly process strings containing
	character 1.
*/
static int
qstr_eval_like (const char *tar, int tar_length,
		const char *expr, int expr_length,
		const char *escape, int coll_id)
{
  const int IN_CHECK = 0;
  const int IN_PERCENT = 1;

  int status = IN_CHECK;
  unsigned char *tarstack[STACK_SIZE], *exprstack[STACK_SIZE];
  int stackp = -1;

  unsigned char *tar_ptr, *end_tar;
  unsigned char *expr_ptr, *end_expr;
  bool escape_is_match_one =
    ((escape != NULL) && *escape == LIKE_WILDCARD_MATCH_ONE);
  bool escape_is_match_many =
    ((escape != NULL) && *escape == LIKE_WILDCARD_MATCH_MANY);
  unsigned char pad_char[2];


  LANG_COLLATION *current_collation;

  int pad_char_size;

  current_collation = lang_get_collation (coll_id);
  intl_pad_char (pad_char, &pad_char_size);

  tar_ptr = (unsigned char *) tar;
  expr_ptr = (unsigned char *) expr;
  end_tar = (unsigned char *) (tar + tar_length);
  end_expr = (unsigned char *) (expr + expr_length);

  while (1)
    {
      int dummy = 1;

      if (status == IN_CHECK)
	{
	  bool go_back = true;
	  if (expr_ptr == end_expr)
	    {
	      go_back = false;

	      while (tar_ptr < end_tar && *tar_ptr == ' ')
		{
		  tar_ptr++;
		}

	      if (tar_ptr == end_tar)
		{
		  return V_TRUE;
		}
	      else
		{
		  if (stackp >= 0 && stackp < STACK_SIZE)
		    {
		      tar_ptr = tarstack[stackp];
		      INTL_NEXT_CHAR (tar_ptr, tar_ptr, &dummy);
		      expr_ptr = exprstack[stackp--];
		    }
		  else
		    {
		      return V_FALSE;
		    }
		}
	    }
	  else if (!escape_is_match_many && expr_ptr < end_expr
		   && *expr_ptr == LIKE_WILDCARD_MATCH_MANY)
	    {
	      go_back = false;
	      status = IN_PERCENT;
	      while ((expr_ptr + 1 < end_expr)
		     && *(expr_ptr + 1) == LIKE_WILDCARD_MATCH_MANY)
		{
		  expr_ptr++;
		}
	    }
	  else if (tar_ptr < end_tar && expr_ptr < end_expr)
	    {
	      if (!escape_is_match_one
		  && *expr_ptr == LIKE_WILDCARD_MATCH_ONE)
		{
		  INTL_NEXT_CHAR (tar_ptr, tar_ptr, &dummy);
		  expr_ptr++;
		  go_back = false;
		}
	      else
		{
		  unsigned char *expr_seq_end = expr_ptr;
		  int cmp;
		  int tar_matched_size;
		  unsigned char *match_escape = NULL;
		  bool inescape = false;
		  bool has_last_escape = false;

		  /* build sequence to check (until wildcard) */
		  do
		    {
		      if (!inescape &&
			  (((!escape_is_match_many &&
			     *expr_seq_end == LIKE_WILDCARD_MATCH_MANY)
			    || (!escape_is_match_one &&
				*expr_seq_end == LIKE_WILDCARD_MATCH_ONE))))
			{
			  break;
			}

		      /* set escape for match: if remains NULL, we don't check
		       * for escape in matching function */
		      if (!inescape && escape != NULL
			  && intl_cmp_char (expr_seq_end,
					    (unsigned char *) escape,
					    &dummy) == 0)
			{
			  /* last escape character is not considered escape,
			   * but normal character */
			  if (expr_seq_end + 1 >= end_expr)
			    {
			      has_last_escape = true;
			      inescape = false;
			    }
			  else
			    {
			      inescape = true;
			      match_escape = (unsigned char *) escape;
			    }
			}
		      else
			{
			  inescape = false;
			}
		      INTL_NEXT_CHAR (expr_seq_end, expr_seq_end, &dummy);
		    }
		  while (expr_seq_end < end_expr);

		  assert (end_tar - tar_ptr > 0);
		  assert (expr_seq_end - expr_ptr > 0);

		  /* match using collation */
		  cmp =
		    current_collation->strmatch (current_collation, true,
						 tar_ptr, end_tar - tar_ptr,
						 expr_ptr,
						 expr_seq_end - expr_ptr,
						 match_escape,
						 has_last_escape,
						 &tar_matched_size);

		  if (cmp == 0)
		    {
		      tar_ptr += tar_matched_size;
		      expr_ptr = expr_seq_end;
		      go_back = false;
		    }

		  assert (tar_ptr <= end_tar);
		  assert (expr_ptr <= end_expr);
		}
	    }

	  if (go_back)
	    {
	      if (stackp >= 0 && stackp < STACK_SIZE)
		{
		  tar_ptr = tarstack[stackp];
		  INTL_NEXT_CHAR (tar_ptr, tar_ptr, &dummy);
		  expr_ptr = exprstack[stackp--];
		}
	      else if (stackp > STACK_SIZE)
		{
		  return V_ERROR;
		}
	      else
		{
		  return V_FALSE;
		}
	    }
	}
      else
	{
	  unsigned char *next_expr_ptr;
	  INTL_NEXT_CHAR (next_expr_ptr, expr_ptr, &dummy);

	  assert (status == IN_PERCENT);
	  if ((next_expr_ptr < end_expr)
	      && (!escape_is_match_one || escape == NULL)
	      && *next_expr_ptr == LIKE_WILDCARD_MATCH_ONE)
	    {
	      if (stackp >= STACK_SIZE - 1)
		{
		  return V_ERROR;
		}
	      tarstack[++stackp] = tar_ptr;
	      exprstack[stackp] = expr_ptr;
	      expr_ptr = next_expr_ptr;
	      INTL_NEXT_CHAR (next_expr_ptr, expr_ptr, &dummy);

	      if (stackp > STACK_SIZE)
		{
		  return V_ERROR;
		}
	      status = IN_CHECK;
	      continue;
	    }

	  if (next_expr_ptr == end_expr)
	    {
	      return V_TRUE;
	    }

	  if (tar_ptr < end_tar && next_expr_ptr < end_expr)
	    {
	      unsigned char *expr_seq_end = next_expr_ptr;
	      int cmp;
	      int tar_matched_size;
	      unsigned char *match_escape = NULL;
	      bool inescape = false;
	      bool has_last_escape = false;

	      /* build sequence to check (until wildcard) */
	      do
		{
		  if (!inescape &&
		      (((!escape_is_match_many &&
			 *expr_seq_end == LIKE_WILDCARD_MATCH_MANY)
			|| (!escape_is_match_one &&
			    *expr_seq_end == LIKE_WILDCARD_MATCH_ONE))))
		    {
		      break;
		    }

		  /* set escape for match: if remains NULL, we don't check
		   * for escape in matching function */
		  if (!inescape && escape != NULL
		      && intl_cmp_char (expr_seq_end,
					(unsigned char *) escape,
					&dummy) == 0)
		    {
		      /* last escape character is not considered escape,
		       * but normal character */
		      if (expr_seq_end + 1 >= end_expr)
			{
			  has_last_escape = true;
			  inescape = false;
			}
		      else
			{
			  inescape = true;
			  match_escape = (unsigned char *) escape;
			}
		    }
		  else
		    {
		      inescape = false;
		    }

		  INTL_NEXT_CHAR (expr_seq_end, expr_seq_end, &dummy);
		}
	      while (expr_seq_end < end_expr);

	      assert (end_tar - tar_ptr > 0);
	      assert (expr_seq_end - next_expr_ptr > 0);

	      do
		{
		  /* match using collation */
		  cmp =
		    current_collation->strmatch (current_collation, true,
						 tar_ptr, end_tar - tar_ptr,
						 next_expr_ptr,
						 expr_seq_end - next_expr_ptr,
						 match_escape,
						 has_last_escape,
						 &tar_matched_size);

		  if (cmp == 0)
		    {
		      if (stackp >= STACK_SIZE - 1)
			{
			  return V_ERROR;
			}
		      tarstack[++stackp] = tar_ptr;
		      tar_ptr += tar_matched_size;

		      exprstack[stackp] = expr_ptr;
		      expr_ptr = expr_seq_end;

		      if (stackp > STACK_SIZE)
			{
			  return V_ERROR;
			}
		      status = IN_CHECK;
		      break;
		    }
		  else
		    {
		      /* check starting from next char */
		      INTL_NEXT_CHAR (tar_ptr, tar_ptr, &dummy);
		    }
		}
	      while (tar_ptr < end_tar);
	    }
	}

      if (tar_ptr == end_tar)
	{
	  while (expr_ptr < end_expr && *expr_ptr == LIKE_WILDCARD_MATCH_MANY)
	    {
	      expr_ptr++;
	    }

	  if (expr_ptr == end_expr)
	    {
	      return V_TRUE;
	    }
	  else
	    {
	      return V_FALSE;
	    }
	}
      else if (tar_ptr > end_tar)
	{
	  return V_FALSE;
	}
    }
}

/*
 * db_string_replace () -
 */
int
db_string_replace (const DB_VALUE * src_string, const DB_VALUE * srch_string,
		   const DB_VALUE * repl_string, DB_VALUE * replaced_string)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_src_string, tmp_srch_string, tmp_repl_string;
  QSTR_CATEGORY src_category = QSTR_UNKNOWN;
  QSTR_CATEGORY srch_category = QSTR_UNKNOWN;
  QSTR_CATEGORY repl_category = QSTR_UNKNOWN;

  unsigned char *result_ptr = NULL;
  int result_length = 0, result_size = 0;
  int coll_id, coll_id_tmp;

  assert (src_string != (DB_VALUE *) NULL);
  assert (replaced_string != (DB_VALUE *) NULL);
  assert (src_string != replaced_string);
  assert (srch_string != replaced_string);
  assert (repl_string != replaced_string);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (3, &tmp_src_string, &tmp_srch_string, &tmp_repl_string);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (3, src_string, srch_string, repl_string))
    {
      error_status =
	db_value_domain_init (replaced_string, DB_TYPE_VARCHAR,
			      DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
      if (error_status != NO_ERROR)
	{
	  goto exit_on_error;
	}

      goto done;
    }

  /* arg cast type **********************************************************
   */
  src_string =
    db_value_cast_arg (src_string, &tmp_src_string, DB_TYPE_VARCHAR,
		       &error_status);
  srch_string =
    db_value_cast_arg (srch_string, &tmp_srch_string, DB_TYPE_VARCHAR,
		       &error_status);
  repl_string =
    db_value_cast_arg (repl_string, &tmp_repl_string, DB_TYPE_VARCHAR,
		       &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  src_category = qstr_get_category (src_string);
  srch_category = qstr_get_category (srch_string);
  repl_category = qstr_get_category (repl_string);

  if (!(src_category == srch_category && srch_category == repl_category))
    {
      error_status = ER_QSTR_INCOMPATIBLE_CODE_SETS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  LANG_RT_COMMON_COLL (DB_GET_STRING_COLLATION (src_string),
		       DB_GET_STRING_COLLATION (srch_string), coll_id_tmp);
  if (coll_id_tmp == -1)
    {
      error_status = ER_QSTR_INCOMPATIBLE_COLLATIONS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  LANG_RT_COMMON_COLL (coll_id_tmp,
		       DB_GET_STRING_COLLATION (repl_string), coll_id);

  if (coll_id == -1)
    {
      error_status = ER_QSTR_INCOMPATIBLE_COLLATIONS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  error_status = qstr_replace ((unsigned char *) DB_PULL_STRING (src_string),
			       DB_GET_STRING_LENGTH (src_string),
			       DB_GET_STRING_SIZE (src_string),
			       coll_id,
			       (unsigned char *) DB_PULL_STRING (srch_string),
			       DB_GET_STRING_SIZE (srch_string),
			       (unsigned char *) DB_PULL_STRING (repl_string),
			       DB_GET_STRING_SIZE (repl_string),
			       &result_ptr, &result_length, &result_size);

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

  if (result_ptr != NULL)
    {
      if (result_length == 0)
	{
	  qstr_make_typed_string (DB_TYPE_VARCHAR,
				  replaced_string,
				  (DB_GET_STRING_LENGTH (src_string) == 0) ?
				  1 : DB_GET_STRING_LENGTH (src_string),
				  (char *) result_ptr, result_size, coll_id);
	}
      else
	{
	  qstr_make_typed_string (DB_TYPE_VARCHAR,
				  replaced_string,
				  result_length,
				  (char *) result_ptr, result_size, coll_id);
	}
      result_ptr[result_size] = 0;
      replaced_string->need_clear = true;
    }

  if (error_status != NO_ERROR)
    {
      pr_clear_value (replaced_string);
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_src_string, &tmp_srch_string,
			&tmp_repl_string);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (replaced_string);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_src_string, &tmp_srch_string,
			&tmp_repl_string);

  return error_status;
}

/* qstr_replace () -
 */
static int
qstr_replace (unsigned char *src_buf, int src_len, int src_size, int coll_id,
	      unsigned char *srch_str_buf, int srch_str_size,
	      unsigned char *repl_str_buf, int repl_str_size,
	      unsigned char **result_buf, int *result_len, int *result_size)
{
#define REPL_POS_ARRAY_EXTENT 32

  int error_status = NO_ERROR;
  int char_size, i;
  unsigned char *matched_ptr, *matched_ptr_end, *target;
  int *repl_pos_array = NULL, *tmp_array = NULL;
  int repl_pos_array_size;
  int repl_pos_array_cnt;
  unsigned char *src_ptr;
  int repl_str_len;

  assert (result_buf != NULL);

  *result_buf = NULL;

  /*
   * if search string is NULL or is longer than source string
   * copy source string as a result
   */
  if (srch_str_buf == NULL || src_size < srch_str_size)
    {
      *result_buf = (unsigned char *) malloc ((size_t) src_size + 1);
      if (*result_buf == NULL)
	{
	  error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 1,
		  src_size);
	  goto exit;
	}

      (void) memcpy ((char *) (*result_buf), (char *) src_buf, src_size);
      *result_len = src_len;
      *result_size = src_size;
      goto exit;
    }

  if (repl_str_buf == NULL)
    {
      repl_str_buf = (unsigned char *) "";
    }

  repl_pos_array_size = REPL_POS_ARRAY_EXTENT;
  repl_pos_array = (int *) malloc (2 * sizeof (int) * repl_pos_array_size);
  if (repl_pos_array == NULL)
    {
      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 1,
	      2 * sizeof (int) * repl_pos_array_size);
      goto exit;
    }

  intl_char_count (repl_str_buf, repl_str_size, &repl_str_len);

  repl_pos_array_cnt = 0;
  for (*result_size = 0, *result_len = 0, src_ptr = src_buf;
       src_size > 0 && srch_str_size > 0 && src_ptr < src_buf + src_size;)
    {
      int matched_size;

      if (QSTR_MATCH (coll_id, src_ptr, src_buf + src_size - src_ptr,
		      srch_str_buf, srch_str_size, NULL, false,
		      &matched_size) == 0)
	{
	  /* store byte position and size of matched string */
	  if (repl_pos_array_cnt >= repl_pos_array_size)
	    {
	      repl_pos_array_size += REPL_POS_ARRAY_EXTENT;
	      tmp_array = (int *) realloc (repl_pos_array,
					   2 * sizeof (int)
					   * repl_pos_array_size);
	      if (tmp_array == NULL)
		{
		  error_status = ER_OUT_OF_VIRTUAL_MEMORY;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 1,
			  2 * sizeof (int) * repl_pos_array_size);
		  goto exit;
		}
	      repl_pos_array = tmp_array;
	    }
	  repl_pos_array[repl_pos_array_cnt * 2] = src_ptr - src_buf;
	  repl_pos_array[repl_pos_array_cnt * 2 + 1] = matched_size;
	  src_ptr += matched_size;
	  repl_pos_array_cnt++;
	  *result_size += repl_str_size;
	  *result_len += repl_str_len;
	}
      else
	{
	  INTL_NEXT_CHAR (src_ptr, src_ptr, &char_size);
	  *result_size += char_size;
	  *result_len += 1;
	}
    }

  if (repl_pos_array_cnt == 0)
    {
      *result_size = src_size;
    }

  *result_buf = (unsigned char *) malloc ((size_t) (*result_size) + 1);
  if (*result_buf == NULL)
    {
      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 1,
	      *result_size + 1);
      goto exit;
    }

  matched_ptr = matched_ptr_end = src_buf;
  target = *result_buf;
  for (i = 0; i < repl_pos_array_cnt; i++)
    {
      /* first, copy non matched original string preceeding matched part */
      matched_ptr = src_buf + repl_pos_array[2 * i];
      if ((matched_ptr - matched_ptr_end) > 0)
	{
	  (void) memcpy (target, matched_ptr_end,
			 matched_ptr - matched_ptr_end);
	  target += matched_ptr - matched_ptr_end;
	}

      /* second, copy replacing string */
      (void) memcpy (target, repl_str_buf, repl_str_size);
      target += repl_str_size;
      matched_ptr_end = matched_ptr + repl_pos_array[2 * i + 1];
    }

  /* append any trailing string (after last matched part) */
  if (matched_ptr_end < src_buf + src_size)
    {
      (void) memcpy (target, matched_ptr_end,
		     src_buf + src_size - matched_ptr_end);
      target += src_buf + src_size - matched_ptr_end;
    }

  assert (target - *result_buf == *result_size);

exit:
  if (repl_pos_array != NULL)
    {
      free_and_init (repl_pos_array);
    }

  return error_status;

#undef REPL_POS_ARRAY_EXTENT
}

/*
 * db_string_translate () -
 */
int
db_string_translate (const DB_VALUE * src_string,
		     const DB_VALUE * from_string, const DB_VALUE * to_string,
		     DB_VALUE * transed_string)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_src_string, tmp_from_string, tmp_to_string;
  QSTR_CATEGORY src_category = QSTR_UNKNOWN;
  QSTR_CATEGORY from_category = QSTR_UNKNOWN;
  QSTR_CATEGORY to_category = QSTR_UNKNOWN;

  unsigned char *result_ptr = NULL;
  int result_length = 0, result_size = 0;
  DB_TYPE result_type = DB_TYPE_NULL;
  int coll_id, coll_id_tmp;

  assert (src_string != (DB_VALUE *) NULL);
  assert (transed_string != (DB_VALUE *) NULL);
  assert (src_string != transed_string);
  assert (from_string != transed_string);
  assert (to_string != transed_string);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (3, &tmp_src_string, &tmp_from_string, &tmp_to_string);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (3, src_string, from_string, to_string))
    {
      error_status =
	db_value_domain_init (transed_string, DB_TYPE_VARCHAR,
			      DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
      if (error_status != NO_ERROR)
	{
	  goto exit_on_error;
	}

      goto done;
    }

  /* arg cast type **********************************************************
   */
  src_string =
    db_value_cast_arg (src_string, &tmp_src_string, DB_TYPE_VARCHAR,
		       &error_status);
  from_string =
    db_value_cast_arg (from_string, &tmp_from_string, DB_TYPE_VARCHAR,
		       &error_status);
  to_string =
    db_value_cast_arg (to_string, &tmp_to_string, DB_TYPE_VARCHAR,
		       &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  src_category = qstr_get_category (src_string);
  from_category = qstr_get_category (from_string);
  to_category = qstr_get_category (to_string);

  if (!(src_category == from_category && from_category == to_category))
    {
      error_status = ER_QSTR_INCOMPATIBLE_CODE_SETS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  LANG_RT_COMMON_COLL (DB_GET_STRING_COLLATION (src_string),
		       DB_GET_STRING_COLLATION (from_string), coll_id_tmp);
  if (coll_id_tmp == -1)
    {
      error_status = ER_QSTR_INCOMPATIBLE_COLLATIONS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  LANG_RT_COMMON_COLL (coll_id_tmp,
		       DB_GET_STRING_COLLATION (to_string), coll_id);

  if (coll_id == -1)
    {
      error_status = ER_QSTR_INCOMPATIBLE_COLLATIONS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  error_status =
    qstr_translate ((unsigned char *) DB_PULL_STRING (src_string),
		    DB_VALUE_DOMAIN_TYPE (src_string),
		    DB_GET_STRING_SIZE (src_string),
		    (unsigned char *) DB_PULL_STRING (from_string),
		    DB_GET_STRING_SIZE (from_string),
		    (unsigned char *) DB_PULL_STRING (to_string),
		    DB_GET_STRING_SIZE (to_string),
		    &result_ptr, &result_type, &result_length, &result_size);

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

  if (result_ptr != NULL)
    {
      if (result_length == 0)
	{
	  qstr_make_typed_string (result_type,
				  transed_string,
				  (DB_GET_STRING_LENGTH (src_string) == 0) ?
				  1 : DB_GET_STRING_LENGTH (src_string),
				  (char *) result_ptr, result_size, coll_id);
	}
      else
	{
	  qstr_make_typed_string (result_type,
				  transed_string,
				  result_length,
				  (char *) result_ptr, result_size, coll_id);
	}
      result_ptr[result_size] = 0;
      transed_string->need_clear = true;
    }

  if (error_status != NO_ERROR)
    {
      pr_clear_value (transed_string);
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_src_string, &tmp_from_string, &tmp_to_string);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (transed_string);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_src_string, &tmp_from_string, &tmp_to_string);

  return error_status;
}

/*
 * qstr_translate () -
 */
static int
qstr_translate (unsigned char *src_ptr, UNUSED_ARG DB_TYPE src_type,
		int src_size, unsigned char *from_str_ptr, int from_str_size,
		unsigned char *to_str_ptr, int to_str_size,
		unsigned char **result_ptr, DB_TYPE * result_type,
		int *result_len, int *result_size)
{
  int error_status = NO_ERROR;
  int j, offset, offset1, offset2;
  int from_char_loc, to_char_cnt, to_char_loc;
  unsigned char *srcp, *fromp, *target = NULL;
  int matched = 0, phase = 0;

  if ((from_str_ptr == NULL && to_str_ptr != NULL))
    {
      error_status = ER_QPROC_INVALID_PARAMETER;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  if (to_str_ptr == NULL)
    {
      to_str_ptr = (unsigned char *) "";
    }

  /* check from, to string */
  to_char_cnt = 0;
  for (j = 0; j < to_str_size;)
    {
      intl_char_size (to_str_ptr + j, 1, &offset2);
      j += offset2;
      to_char_cnt++;
    }

  /* calculate total length */
  *result_size = 0;
  phase = 0;

loop:
  srcp = src_ptr;
  for (srcp = src_ptr; srcp < src_ptr + src_size;)
    {
      intl_char_size (srcp, 1, &offset);

      matched = 0;
      from_char_loc = 0;
      for (fromp = from_str_ptr;
	   fromp != NULL && fromp < from_str_ptr + from_str_size;
	   from_char_loc++)
	{
	  intl_char_size (fromp, 1, &offset1);

	  /* if source and from char are matched, translate */
	  if ((offset == offset1) && (memcmp (srcp, fromp, offset) == 0))
	    {
	      matched = 1;
	      to_char_loc = 0;
	      for (j = 0; j < to_str_size;)
		{
		  intl_char_size (to_str_ptr + j, 1, &offset2);

		  if (to_char_loc == from_char_loc)
		    {		/* if matched char exist, replace */
		      if (phase == 0)
			{
			  *result_size += offset2;
			}
		      else
			{
			  memcpy (target, to_str_ptr + j, offset2);
			  target += offset2;
			}
		      break;
		    }
		  j += offset2;
		  to_char_loc++;
		}
	      break;
	    }
	  fromp += offset1;
	}
      if (!matched)
	{			/* preserve source char */
	  if (phase == 0)
	    {
	      *result_size += offset;
	    }
	  else
	    {
	      memcpy (target, srcp, offset);
	      target += offset;
	    }
	}
      srcp += offset;
    }

  if (phase == 1)
    {
      return error_status;
    }

  /* evaluate result string length */
  *result_type = DB_TYPE_VARCHAR;
  *result_ptr = (unsigned char *) malloc ((size_t) * result_size + 1);
  if (*result_ptr == NULL)
    {
      error_status = er_errid ();
      return error_status;
    }
  if (phase == 0)
    {
      phase = 1;
      target = *result_ptr;
      *result_len = *result_size;
      goto loop;
    }

  return error_status;
}

/*
 * db_bit_string_coerce () -
 *
 * Arguments:
 *        src_string:  (In) Source string
 *       dest_string: (Out) Coerced string
 *       data_status: (Out) Data status
 *
 * Returns: int
 *
 * Errors:
 *   ER_OBJ_INVALID_ARGUMENTS
 *      <src_string> is not a bit string
 *   ER_QSTR_INCOMPATIBLE_CODE_SETS
 *      <dest_domain> is not a compatible domain type
 *
 * Note:
 *
 *   This function coerces a bit string from one domain to another.
 *   A new DB_VALUE is created making use of the memory manager and
 *   domain information stored in <dest_value>, and coercing the
 *   data portion of <src_string>.
 *
 *   If any loss of data due to truncation occurs, <data_status>
 *   is set to DATA_STATUS_TRUNCATED.
 *
 *   The destination container should have the memory manager, precision
 *   and domain type initialized.
 *
 * Assert:
 *
 *   1. src_string  != (DB_VALUE *) NULL
 *   2. dest_value  != (DB_VALUE *) NULL
 *   3. data_status != (DB_DATA_STATUS *) NULL
 *
 */

int
db_bit_string_coerce (const DB_VALUE * src_string,
		      DB_VALUE * dest_string, DB_DATA_STATUS * data_status)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_src_string, tmp_dest_string;
  DB_TYPE src_type, dest_type;
  unsigned char *dest;
  int dest_prec;
  int dest_length;

  /* Assert that DB_VALUE structures have been allocated. */
  assert (src_string != (DB_VALUE *) NULL);
  assert (dest_string != (DB_VALUE *) NULL);
  assert (data_status != (DB_DATA_STATUS *) NULL);

  /* Initialize status value */
  *data_status = DATA_STATUS_OK;

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (2, &tmp_src_string, &tmp_dest_string);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (1, src_string))
    {
      db_value_domain_init (dest_string, DB_VALUE_DOMAIN_TYPE (dest_string),
			    DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
      goto done;
    }

  /* arg cast type **********************************************************
   */

  /* src, dest should be varbit.
   * refer tp_value_cast_internal () to keep out infinite call-stack loop
   */
  src_type = DB_VALUE_DOMAIN_TYPE (src_string);
  dest_type = DB_VALUE_DOMAIN_TYPE (dest_string);

  if (!QSTR_IS_VARBIT (src_type) || !QSTR_IS_VARBIT (dest_type))
    {
      error_status = ER_OBJ_INVALID_ARGUMENTS;
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  if (qstr_get_category (src_string) != qstr_get_category (dest_string))
    {
      error_status = ER_QSTR_INCOMPATIBLE_CODE_SETS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  /* Categorize the two input parameters and check for errors.
     Verify that the parameters are both character strings. */

  if (DB_VALUE_PRECISION (dest_string) == TP_FLOATING_PRECISION_VALUE)
    {
      dest_prec = DB_GET_STRING_LENGTH (src_string);
    }
  else
    {
      dest_prec = DB_VALUE_PRECISION (dest_string);
    }

  error_status =
    qstr_bit_coerce ((unsigned char *) DB_PULL_STRING (src_string),
		     DB_GET_STRING_LENGTH (src_string),
		     QSTR_VALUE_PRECISION (src_string),
		     DB_TYPE_VARBIT, &dest, &dest_length, dest_prec,
		     DB_TYPE_VARBIT, data_status);

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

  qstr_make_typed_string (DB_TYPE_VARBIT,
			  dest_string,
			  DB_VALUE_PRECISION (dest_string),
			  (char *) dest, dest_length, 0);
  dest_string->need_clear = true;

  if (error_status != NO_ERROR)
    {
      pr_clear_value (dest_string);
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_src_string, &tmp_dest_string);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (dest_string);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_src_string, &tmp_dest_string);

  return error_status;
}

/*
 * db_char_string_coerce () -
 *
 * Arguments:
 *        src_string:  (In) Source string
 *       dest_string: (Out) Coerced string
 *       data_status: (Out) Data status
 *
 * Returns: int
 *
 * Errors:
 *   ER_OBJ_INVALID_ARGUMENTS
 *      <src_string> and <dest_string> are not both char strings
 *   ER_QSTR_INCOMPATIBLE_CODE_SETS
 *      <dest_domain> is not a compatible domain type
 *
 * Note:
 *
 *   This function coerces a char string from one domain to
 *   another.  A new DB_VALUE is created making use of the
 *   memory manager and domain information stored in
 *   <dest_value>, and coercing the data portion of
 *   <src_string>.
 *
 *   If any loss of data due to truncation occurs, <data_status>
 *   is set to DATA_STATUS_TRUNCATED.
 *
 * Assert:
 *
 *   1. src_string  != (DB_VALUE *) NULL
 *   2. dest_value  != (DB_VALUE *) NULL
 *   3. data_status != (DB_DATA_STATUS *) NULL
 *
 */

int
db_char_string_coerce (const DB_VALUE * src_string,
		       DB_VALUE * dest_string, DB_DATA_STATUS * data_status)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_src_string, tmp_dest_string;

  unsigned char *dest;
  int dest_prec;
  int dest_length;
  int dest_size;
  INTL_CODESET src_codeset;
  INTL_CODESET dest_codeset;

  /* Assert that DB_VALUE structures have been allocated. */
  assert (src_string != (DB_VALUE *) NULL);
  assert (dest_string != (DB_VALUE *) NULL);
  assert (data_status != (DB_DATA_STATUS *) NULL);

  /* Initialize status value */
  *data_status = DATA_STATUS_OK;

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (2, &tmp_src_string, &tmp_dest_string);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (1, src_string))
    {
      db_value_domain_init (dest_string, DB_VALUE_DOMAIN_TYPE (dest_string),
			    DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
      goto done;
    }

  /* arg cast type **********************************************************
   */

  /* src, dest should be varchar.
   * refer tp_value_cast_internal () to keep out infinite call-stack loop
   */
  if (!is_char_string (src_string) || !is_char_string (dest_string))
    {
      error_status = ER_OBJ_INVALID_ARGUMENTS;
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  if (qstr_get_category (src_string) != qstr_get_category (dest_string))
    {
      error_status = ER_QSTR_INCOMPATIBLE_CODE_SETS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  src_codeset = INTL_CODESET_UTF8;
  dest_codeset = INTL_CODESET_UTF8;

  /* Initialize the memory manager of the destination */
  if (DB_VALUE_PRECISION (dest_string) == TP_FLOATING_PRECISION_VALUE)
    {
      dest_prec = DB_GET_STRING_LENGTH (src_string);
    }
  else
    {
      dest_prec = DB_VALUE_PRECISION (dest_string);
    }

  error_status =
    qstr_coerce ((unsigned char *) DB_PULL_STRING (src_string),
		 DB_GET_STRING_LENGTH (src_string),
		 QSTR_VALUE_PRECISION (src_string),
		 DB_VALUE_DOMAIN_TYPE (src_string),
		 src_codeset, dest_codeset,
		 &dest, &dest_length, &dest_size, dest_prec,
		 DB_VALUE_DOMAIN_TYPE (dest_string), data_status);

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

  if (dest != NULL)
    {
      qstr_make_typed_string (DB_VALUE_DOMAIN_TYPE (dest_string),
			      dest_string,
			      DB_VALUE_PRECISION (dest_string),
			      (char *) dest, dest_size,
			      DB_GET_STRING_COLLATION (dest_string));
      dest[dest_size] = 0;
      dest_string->need_clear = true;
    }

  if (error_status != NO_ERROR)
    {
      pr_clear_value (dest_string);
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_src_string, &tmp_dest_string);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (dest_string);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_src_string, &tmp_dest_string);

  return error_status;
}

/*
 * db_string_make_empty_typed_string() -
 *
 * Arguments:
 *	 thread_p   : (In) thread context (may be NULL)
 *       db_val	    : (In/Out) value to make
 *       db_type    : (In) Type of string (char,nchar,bit)
 *       precision  : (In)
 *       collation_id  : (In)
 *
 * Returns: int
 *
 * Errors:
 *   ER_OBJ_INVALID_ARGUMENTS
 *      <type> is not one of (char,nchar,bit)
 *   ER_OUT_OF_VIRTUAL_MEMORY
 *      out of memory
 *
 */

int
db_string_make_empty_typed_string (UNUSED_ARG THREAD_ENTRY * thread_p,
				   DB_VALUE * db_val, const DB_TYPE db_type,
				   int precision, int collation_id)
{
  int status = NO_ERROR;
  char *buf = NULL;

  /* handle bad cases */
  assert (db_val != NULL);
  assert (precision >= DB_DEFAULT_PRECISION);

  if (db_type != DB_TYPE_VARBIT && db_type != DB_TYPE_VARCHAR)
    {
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  if (db_val == NULL)
    {
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  if (DB_IS_NULL (db_val))
    {
      db_value_domain_init (db_val, db_type, precision, 0);
    }

  precision = ((precision < DB_DEFAULT_PRECISION)
	       ? DB_DEFAULT_PRECISION : precision);

  /* create an empty string DB VALUE */
  buf = (char *) malloc (1);
  if (buf == NULL)
    {
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }
  *buf = '\0';

  qstr_make_typed_string (db_type, db_val, precision, buf, 0, collation_id);
  db_val->need_clear = true;

  return status;
}

/*
 * db_find_string_in_in_set () - find the position of a string token in
 *				 a string containing comma separated tokens
 * return : error code or NO_ERROR
 * needle (in)	: the token to look for
 * stack (in)	: the set of tokens
 * result (in/out) : will hold the position of the token
 */
int
db_find_string_in_in_set (const DB_VALUE * needle, const DB_VALUE * stack,
			  DB_VALUE * result)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_needle, tmp_stack;
  int position = 1;
  int stack_size = 0, needle_size = 0;
  const char *stack_str = NULL;
  const char *needle_str = NULL;
  int cmp, coll_id, matched_stack_size;
  const char *stack_ptr, *elem_start;

  assert (needle != result);
  assert (stack != result);

  /*
   *  Categorize the parameters into respective code sets.
   *  Verify that the parameters are both character strings.
   *  Verify that the input strings belong to compatible code sets.
   */

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (2, &tmp_needle, &tmp_stack);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (2, needle, stack))
    {
      DB_MAKE_NULL (result);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  needle =
    db_value_cast_arg (needle, &tmp_needle, DB_TYPE_VARCHAR, &error_status);
  stack =
    db_value_cast_arg (stack, &tmp_stack, DB_TYPE_VARCHAR, &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  if (qstr_get_category (needle) != qstr_get_category (stack))
    {
      error_status = ER_QSTR_INCOMPATIBLE_CODE_SETS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  LANG_RT_COMMON_COLL (DB_GET_STRING_COLLATION (stack),
		       DB_GET_STRING_COLLATION (needle), coll_id);
  if (coll_id == -1)
    {
      error_status = ER_QSTR_INCOMPATIBLE_COLLATIONS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  stack_str = DB_PULL_STRING (stack);
  stack_size = DB_GET_STRING_SIZE (stack);
  needle_str = DB_PULL_STRING (needle);
  needle_size = DB_GET_STRING_SIZE (needle);

  if (stack_size == 0 && needle_size == 0)
    {
      /* if both are empty string, no match */
      DB_MAKE_INTEGER (result, 0);
      goto done;
    }

  elem_start = stack_ptr = stack_str;

  for (;;)
    {
      if (*stack_ptr == ',' || stack_ptr >= stack_str + stack_size)
	{
	  assert (stack_ptr <= stack_str + stack_size);

	  if (stack_ptr == elem_start)
	    {
	      if (needle_size == 0)
		{
		  DB_MAKE_INT (result, position);
		  goto done;
		}
	    }
	  else
	    {
	      assert (stack_ptr > elem_start);
	      /* check using collation */
	      if (needle_size > 0)
		{
		  cmp = QSTR_MATCH (coll_id, elem_start,
				    stack_ptr - elem_start,
				    needle_str, needle_size,
				    false, false, &matched_stack_size);
		  if (cmp == 0
		      && matched_stack_size == stack_ptr - elem_start)
		    {
		      DB_MAKE_INT (result, position);
		      goto done;
		    }
		}
	    }

	  if (stack_ptr >= stack_str + stack_size)
	    {
	      break;
	    }

	  position++;
	  elem_start = ++stack_ptr;
	}
      else
	{
	  stack_ptr++;
	}
    }

  /* if we didn't find it in the loop above, then there is no match */
  DB_MAKE_INTEGER (result, 0);

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_needle, &tmp_stack);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (result);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_needle, &tmp_stack);

  return error_status;
}

/*
 * db_bigint_to_binary_string () - compute the string representation of a
 *				   binary a value
 * return : error code or NO_ERROR
 * src_bigint (in)  : the binary value
 * result (out)	    : the string representation of the binary value
 */
int
db_bigint_to_binary_string (const DB_VALUE * src_bigint, DB_VALUE * result)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_src_bigint;
  int i = 0;
  DB_BIGINT bigint_val = 0;
  int digits_count = 0;
  char *binary_form = NULL;

  assert (src_bigint != result);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (1, &tmp_src_bigint);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (1, src_bigint))
    {
      DB_MAKE_NULL (result);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  src_bigint =
    db_value_cast_arg (src_bigint, &tmp_src_bigint, DB_TYPE_BIGINT,
		       &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  bigint_val = DB_GET_BIGINT (src_bigint);

  /* count the number of digits in bigint_val */
  if (bigint_val < (DB_BIGINT) 0)
    {
      /* MSB is the sign bit */
      digits_count = sizeof (DB_BIGINT) * 8;
    }
  else if (bigint_val == 0)
    {
      digits_count = 1;
    }
  else
    {
      i = 0;
      /* positive numbers have at most 8 * sizeof(DB_BIGINT) - 1 digits */
      while ((DB_BIGINT) 1 << i <= bigint_val
	     && i < (int) sizeof (DB_BIGINT) * 8 - 1)
	{
	  i++;
	}
      digits_count = i;
    }

  binary_form = (char *) malloc (digits_count + 1);
  if (binary_form == NULL)
    {
      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
      goto exit_on_error;
    }

  memset (binary_form, 0, digits_count + 1);

  for (i = 0; i < digits_count; i++)
    {
      binary_form[digits_count - i - 1] =
	(((DB_BIGINT) 1 << i) & bigint_val) ? '1' : '0';
    }

  DB_MAKE_VARCHAR (result, digits_count, binary_form, digits_count,
		   LANG_COERCIBLE_COLL);
  result->need_clear = true;

  if (error_status != NO_ERROR)
    {
      pr_clear_value (result);
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_src_bigint);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  if (binary_form != NULL)
    {
      free_and_init (binary_form);
    }

  DB_MAKE_NULL (result);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_src_bigint);

  return error_status;
}

/*
 * qstr_pad_string () -
 *
 * Arguments:
 *            s: (IN OUT) Pointer to input string.
 *       length: (IN)     Size of input string.
 *
 * Returns: unsigned char
 *
 * Errors:
 *
 * Note:
 *     This is a convenience function which will copy pad characters into
 *     the input string.  It is assumed that the pad character will consist
 *     of one or two bytes (this is currently true).
 *
 *     The address immediately after the padded string is returned.  Thus,
 *     If a NULL terminated string was desired, then a call could be made:
 *
 *         ptr = qstr_pad_string();
 *         *ptr = '\0';
 *
 */

unsigned char *
qstr_pad_string (unsigned char *s, int length)
{
  unsigned char pad[2];
  int i, j, pad_size = 0;

  if (length == 0)
    {
      return s;
    }

  assert (length > 0);

  intl_pad_char (pad, &pad_size);

  if (pad_size == 1)
    {
      (void) memset ((char *) s, (int) pad[0], length);
      s = s + length;
    }
  else
    {
      for (i = 0; i < length; i++)
	{
	  for (j = 0; j < pad_size; j++)
	    {
	      *(s++) = pad[j];
	    }
	}
    }

  return s;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * qstr_bin_to_hex () -
 *
 * arguments:
 *        dest: Pointer to destination hex buffer area
 *   dest_size: Size of destination buffer area in bytes
 *         src: Pointer to source binary buffer area
 *    src_size: Size of source buffer area in bytes
 *
 * returns/side-effects: int
 *    The number of converted source bytes is returned.  This value will
 *    equal src_size if (dest_size >= 2*src_size) and less otherwise.
 *
 * description:
 *    Convert the binary data in the source buffer to ASCII hex characters
 *    in the destination buffer.  The destination buffer should be at
 *    least 2 * src_size.  If not, as much of the source string is processed
 *    as possible.  The number of ASCII Hex characters in dest will
 *    equal two times the returned value.
 *
 */

int
qstr_bin_to_hex (char *dest, int dest_size, const char *src, int src_size)
{
  int i, copy_size;

  if (dest_size >= (2 * src_size))
    {
      copy_size = src_size;
    }
  else
    {
      copy_size = dest_size / 2;
    }

  for (i = 0; i < copy_size; i++)
    {
      sprintf (&(dest[2 * i]), "%02x", (unsigned char) (src[i]));
    }

  return copy_size;
}
#endif

/*
 * qstr_hex_to_bin () -
 *
 * arguments:
 *        dest: Pointer to destination hex buffer area
 *   dest_size: Size of destination buffer area in bytes
 *         src: Pointer to source binary buffer area
 *    src_size: Size of source buffer area in bytes
 *
 * returns/side-effects: int
 *    The number of converted hex characters is returned.
 *
 * description:
 *    Convert the string of hex characters to decimal values.  For each two
 *    characters, one unsigned character value is produced.  If the number
 *    of characters is odd, then the second nibble of the last byte will
 *    be 0 padded.  If the destination buffer is not large enough to hold
 *    the converted data, as much data is converted as possible.
 *
 */

int
qstr_hex_to_bin (char *dest, int dest_size, char *src, int src_size)
{
  int i, copy_size, src_index, required_size;

  required_size = (src_size + 1) / 2;

  if (dest_size >= required_size)
    {
      copy_size = required_size;
    }
  else
    {
      copy_size = dest_size;
    }

  src_index = 0;
  for (i = 0; i < copy_size; i++)
    {
      int hex_digit;

      hex_digit = hextoi (src[src_index++]);
      if (hex_digit < 0)
	{
	  return -1;
	}
      else
	{
	  dest[i] = hex_digit << 4;
	  if (src_index < src_size)
	    {
	      hex_digit = hextoi (src[src_index++]);
	      if (hex_digit < 0)
		{
		  return -1;
		}
	      else
		{
		  dest[i] += hex_digit;
		}
	    }
	}
    }

  return src_index;
}

/*
 * qstr_bit_to_bin () -
 *
 * arguments:
 *        dest: Pointer to destination buffer area
 *   dest_size: Size of destination buffer area in bytes
 *         src: Pointer to source binary buffer area
 *    src_size: Size of source buffer area in bytes
 *
 * returns/side-effects: int
 *    The number of converted binary characters is returned.
 *
 * description:
 *    Convert the string of '0's and '1's to decimal values.  For each 8
 *    characters, one unsigned character value is produced.  If the number
 *    of characters is not a multiple of 8, the result will assume trailing
 *    0 padding.  If the destination buffer is not large enough to hold
 *    the converted data, as much data is converted as possible.
 *
 */

int
qstr_bit_to_bin (char *dest, int dest_size, char *src, int src_size)
{
  int dest_byte, copy_size, src_index, required_size;

  required_size = (src_size + 7) / 8;

  if (dest_size >= required_size)
    {
      copy_size = required_size;
    }
  else
    {
      copy_size = dest_size;
    }

  src_index = 0;
  for (dest_byte = 0; dest_byte < copy_size; dest_byte++)
    {
      int bit_count;

      dest[dest_byte] = 0;
      for (bit_count = 0; bit_count < 8; bit_count++)
	{
	  dest[dest_byte] = dest[dest_byte] << 1;
	  if (src_index < src_size)
	    {
	      if (src[src_index] == '1')
		{
		  dest[dest_byte]++;
		}
	      else if (src[src_index] != '0')
		{
		  return -1;	/* Illegal digit */
		}
	      src_index++;
	    }
	}
    }

  return src_index;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * qstr_bit_to_hex_coerce () -
 *
 * arguments:
 *      buffer: Pointer to destination buffer area
 * buffer_size: Size of destination buffer area (in bytes, *including* null
 *              terminator)
 *         src: Pointer to source buffer area
 *  src_length: Length of source buffer area in bits
 *    pad_flag: TRUE if the buffer should be padded and FALSE otherwise
 *   copy_size: Number of bytes transfered from the src string to the dst
 *              buffer
 *  truncation: pointer to a int field.  *outlen will equal 0 if no
 *              truncation occurred and will equal the size of the dst buffer
 *              in bytes needed to avoid truncation (not including the
 *              terminating NULL), otherwise.
 *
 * returns/side-effects: void
 *
 * description:
 *    Transfers at most buffer_size bytes to the region pointed at by dst.
 *    If  pad_flag is TRUE, strings shorter than buffer_size will be
 *    blank-padded out to buffer_size-1 bytes.  All strings will be
 *    null-terminated.  If truncation is necessary (i.e., if buffer_size is
 *    less than or equal to src_length), *truncation is set to src_length;
 *    if truncation is is not necessary, *truncation is set to 0.
 *
 */

void
qstr_bit_to_hex_coerce (char *buffer,
			int buffer_size,
			const char *src,
			int src_length,
			int pad_flag, int *copy_size, int *truncation)
{
  int src_size = BITS_TO_BYTES (src_length);

  if (src == NULL)
    {
      buffer[0] = '\0';
      return;
    }

  if (buffer_size > (2 * src_size))
    {
      /*
       * No truncation; copy the data and blank pad if necessary.
       */
      qstr_bin_to_hex (buffer, buffer_size, src, src_size);
/*
	for (i=0; i<src_size; i++)
	    sprintf(&(buffer[2*i]), "%02x", (unsigned char)(src[i]));
*/
      if (pad_flag == true)
	{
	  memset (&(buffer[2 * src_size]), '0',
		  (buffer_size - (2 * src_size)));
	  *copy_size = buffer_size - 1;
	}
      else
	{
	  *copy_size = 2 * src_size;
	}
      buffer[*copy_size] = '\0';
      *truncation = 0;
    }
  else
    {
      /*
       * Truncation is necessary; put as many bytes as possible into
       * the receiving buffer and null-terminate it (i.e., it receives
       * at most dstsize-1 bytes).  If there is not outlen indicator by
       * which we can indicate truncation, this is an error.
       *
       */
      if (buffer_size % 2)
	{
	  src_size = buffer_size / 2;
	}
      else
	{
	  src_size = (buffer_size - 1) / 2;
	}

      qstr_bin_to_hex (buffer, buffer_size, src, src_size);
/*
	for (i=0; i<src_size; i++)
	    sprintf(&(buffer[2*i]), "%02x", (unsigned char)(src[i]));
*/
      *copy_size = 2 * src_size;
      buffer[*copy_size] = '\0';

      *truncation = src_size;
    }
}
#endif

/*
 * db_get_string_length
 *
 * Arguments:
 *        value: Value  container
 *
 * Returns: int
 *
 * Errors:
 *
 * Note:
 *     Returns the character length of the string in the container.
 *
 */

int
db_get_string_length (const DB_VALUE * value)
{
  DB_TYPE db_type;
  DB_C_VARCHAR str;
  int size;
  int length = 0;

  if (value == NULL)
    {
      return length;		/* do nothing */
    }

  db_type = DB_VALUE_DOMAIN_TYPE (value);

  if (!QSTR_IS_CHAR_OR_BIT (db_type))
    {
#if 0				/* TODO - */
      assert (false);
#endif
      return length;		/* give up */
    }

  str = value->data.ch.buf;
  length = size = value->data.ch.size;

  if (value->domain.general_info.type != DB_TYPE_VARBIT)
    {
      intl_char_count ((unsigned char *) str, size, &length);
    }

  return length;
}

/*
 * qstr_make_typed_string () -
 *
 * Arguments:
 *      db_type: value type for the result.
 *        value: Value container for the result.
 *    precision: Length of the string precision.
 *          src: Pointer to string.
 *       s_unit: Size of the string.
 * collation_id: collation
 *
 * Returns: void
 *
 * Errors:
 *
 * Note:
 *     Make a value container from the string of the given domain.
 *     This is a convenience function which allows for all string
 *     types given the proper domain type.
 *
 */

void
qstr_make_typed_string (const DB_TYPE db_type, DB_VALUE * value,
			const int precision, const DB_C_VARCHAR src,
			const int s_unit, const int collation_id)
{
  switch (db_type)
    {
    case DB_TYPE_VARCHAR:
      DB_MAKE_VARCHAR (value, precision, src, s_unit, collation_id);
      break;

    case DB_TYPE_VARBIT:
      DB_MAKE_VARBIT (value, precision, src, s_unit);
      break;

    default:
      assert (false);
      DB_MAKE_NULL (value);
      break;
    }
}

/*
 *  Private Functions
 */

/*
 * qstr_get_category
 *
 * Arguments:
 *      s: DB_VALUE representation of a string.
 *
 * Returns: QSTR_CATEGORY
 *
 * Errors:
 *
 * Note:
 *   Returns the character code set of the string "s."  The character code
 *   set of strings is:
 *
 *       QSTR_VARCHAR, QSTR_VARBIT
 *
 *   as defined in type QSTR_CATEGORY.  A value of QSTR_UNKNOWN is defined
 *   if the string does not fit into one of these categories.  This should
 *   never happen if is_string() returns TRUE.
 *
 */

QSTR_CATEGORY
qstr_get_category (const DB_VALUE * s)
{
  QSTR_CATEGORY code_set;

  switch (DB_VALUE_DOMAIN_TYPE (s))
    {

    case DB_TYPE_VARCHAR:
      code_set = QSTR_VARCHAR;
      break;

    case DB_TYPE_VARBIT:
      code_set = QSTR_VARBIT;
      break;

    default:
      code_set = QSTR_UNKNOWN;
      break;
    }

  return code_set;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * is_string () -
 *
 * Arguments:
 *      s: (IN) DB_VALUE variable.
 *
 * Returns: bool
 *
 * Errors:
 *
 * Note:
 *   Verifies that the value is a string.  Returns TRUE if the
 *   domain type is one of:
 *
 *       DB_TYPE_VARCHAR
 *       DB_TYPE_VARBIT
 *
 *   Returns FALSE otherwise.
 *
 *   This function supports the older type DB_TYPE_VARCHAR which
 *   has been replaced by DB_TYPE_VARCHAR.
 *
 */

static bool
is_string (const DB_VALUE * s)
{
  DB_TYPE domain_type = DB_VALUE_DOMAIN_TYPE (s);

  return QSTR_IS_CHAR_OR_BIT (domain_type);
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * is_char_string () -
 *
 * Arguments:
 *      s: DB_VALUE variable.
 *
 * Returns: bool
 *
 * Errors:
 *
 * Note:
 *   Verifies that the value is a character string.  Returns TRUE if the
 *   value is of domain type is one of:
 *
 *       DB_TYPE_VARCHAR
 *
 *   Returns FALSE otherwise.
 *
 *   This function supports the older type DB_TYPE_VARCHAR which
 *   has been replaced by DB_TYPE_VARCHAR.
 *
 */

bool
is_char_string (const DB_VALUE * s)
{
  DB_TYPE domain_type = DB_VALUE_DOMAIN_TYPE (s);

  return (QSTR_IS_CHAR (domain_type));
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * is_integer () -
 *
 * Arguments:
 *      i: (IN) DB_VALUE variable.
 *
 * Returns: bool
 *
 * Errors:
 *
 * Note:
 *   Verifies that the value is an integer.  Returns TRUE if the
 *   value is of domain type is one of:
 *
 *       DB_TYPE_INTEGER
 *
 *   Returns FALSE otherwise.
 *
 */

static bool
is_integer (const DB_VALUE * i)
{
  return (DB_VALUE_DOMAIN_TYPE (i) == DB_TYPE_INTEGER);
}
#endif

/*
 * is_number () -
 *
 * Arguments:
 *      n: (IN) DB_VALUE variable.
 *
 * Returns: bool
 *
 * Errors:
 *
 * Note:
 *   Verifies that the value is an number.  Returns TRUE if the
 *   value is of domain type is one of:
 *
 *       DB_TYPE_NUMERIC
 *       DB_TYPE_INTEGER
 *       DB_TYPE_DOUBLE
 *
 *   Returns FALSE otherwise.
 *
 */

bool
is_number (const DB_VALUE * n)
{
  DB_TYPE domain_type = DB_VALUE_DOMAIN_TYPE (n);

  return TP_IS_NUMERIC_TYPE (domain_type);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * qstr_compare () - compare two character strings of DB_TYPE_VARCHAR(tp_String)
 *
 * Arguments:
 *      string1: 1st character string
 *        size1: size of 1st string
 *      string2: 2nd character string
 *        size2: size of 2nd string
 *
 * Returns:
 *   Greater than 0 if string1 > string2
 *   Equal to 0     if string1 = string2
 *   Less than 0    if string1 < string2
 *
 * Errors:
 *
 * Note:
 *   This function is similar to strcmp(3) or bcmp(3). It is designed to
 *   follow SQL_TEXT character set collation. Padding character(space ' ') is
 *   the smallest character in the set. (e.g.) "ab z" < "ab\t1"
 *
 */

int
qstr_compare (const unsigned char *string1, int size1,
	      const unsigned char *string2, int size2)
{
  int n, i, cmp;
  unsigned char c1, c2;

#define PAD ' '			/* str_pad_char(INTL_CODESET_ISO88591, pad, &pad_size) */
#define SPACE PAD		/* smallest character in the collation sequence */
#define ZERO '\0'		/* space is treated as zero */

  n = size1 < size2 ? size1 : size2;
  for (i = 0, cmp = 0; i < n && cmp == 0; i++)
    {
      c1 = *string1++;
      if (c1 == SPACE)
	{
	  c1 = ZERO;
	}
      c2 = *string2++;
      if (c2 == SPACE)
	{
	  c2 = ZERO;
	}
      cmp = c1 - c2;
    }
  if (cmp != 0)
    {
      return cmp;
    }
  if (size1 == size2)
    {
      return cmp;
    }

  c1 = c2 = ZERO;
  if (size1 < size2)
    {
      n = size2 - size1;
      for (i = 0; i < n && cmp == 0; i++)
	{
	  c2 = *string2++;
	  if (c2 == PAD)
	    {
	      c2 = ZERO;
	    }
	  cmp = c1 - c2;
	}
    }
  else
    {
      n = size1 - size2;
      for (i = 0; i < n && cmp == 0; i++)
	{
	  c1 = *string1++;
	  if (c1 == PAD)
	    {
	      c1 = ZERO;
	    }
	  cmp = c1 - c2;
	}
    }
  return cmp;

#undef PAD
#undef SPACE
#undef ZERO
}				/* qstr_compare() */

/*
 * char_compare () - compare two character strings of DB_TYPE_VARCHAR(tp_VarChar)
 *
 * Arguments:
 *      string1: 1st character string
 *        size1: size of 1st string
 *      string2: 2nd character string
 *        size2: size of 2nd string
 *
 * Returns:
 *   Greater than 0 if string1 > string2
 *   Equal to 0     if string1 = string2
 *   Less than 0    if string1 < string2
 *
 * Errors:
 *
 * Note:
 *   This function is identical to qstr_compare().
 *
 */

int
char_compare (const unsigned char *string1, int size1,
	      const unsigned char *string2, int size2)
{
  int n, i, cmp;
  unsigned char c1, c2;

  assert (size1 >= 0 && size2 >= 0);

#define PAD ' '			/* str_pad_char(INTL_CODESET_ISO88591, pad, &pad_size) */
#define SPACE PAD		/* smallest character in the collation sequence */
#define ZERO '\0'		/* space is treated as zero */

  n = size1 < size2 ? size1 : size2;
  for (i = 0, cmp = 0; i < n && cmp == 0; i++)
    {
      c1 = *string1++;
      if (c1 == SPACE)
	{
	  c1 = ZERO;
	}
      c2 = *string2++;
      if (c2 == SPACE)
	{
	  c2 = ZERO;
	}
      cmp = c1 - c2;
    }
  if (cmp != 0)
    {
      return cmp;
    }
  if (size1 == size2)
    {
      return cmp;
    }

  c1 = c2 = ZERO;
  if (size1 < size2)
    {
      n = size2 - size1;
      for (i = 0; i < n && cmp == 0; i++)
	{
	  c2 = *string2++;
	  if (c2 == PAD)
	    {
	      c2 = ZERO;
	    }
	  cmp = c1 - c2;
	}
    }
  else
    {
      n = size1 - size2;
      for (i = 0; i < n && cmp == 0; i++)
	{
	  c1 = *string1++;
	  if (c1 == PAD)
	    {
	      c1 = ZERO;
	    }
	  cmp = c1 - c2;
	}
    }
  return cmp;

#undef PAD
#undef SPACE
#undef ZERO
}				/* char_compare() */

#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * varbit_compare () - compare two bit strings of DB_TYPE_VARBIT(tp_VarBit)
 *
 * Arguments:
 *      string1: 1st bit string
 *        size1: size of 1st string
 *      string2: 2nd bit string
 *        size2: size of 2nd string
 *
 * Returns:
 *   Greater than 0 if string1 > string2
 *   Equal to 0     if string1 = string2
 *   Less than 0    if string1 < string2
 *
 * Errors:
 *
 * Note:
 *   This function is identical to qstr_compare().
 *
 */

int
varbit_compare (const unsigned char *string1, int size1,
		const unsigned char *string2, int size2)
{
  int n, i, cmp;

#define PAD '\0'		/* str_pad_char(INTL_CODESET_RAW_BITS, pad, &pad_size) */
  n = size1 < size2 ? size1 : size2;
  for (i = 0, cmp = 0; i < n && cmp == 0; i++)
    {
      cmp = (*string1++ - *string2++);
    }
  if (cmp != 0)
    {
      return cmp;
    }
  cmp = size1 - size2;
  return cmp;
#undef PAD
}				/* varbit_compare() */


/*
 * qstr_grow_string () - grows the memory buffer of string value
 *
 * Arguments:
 *            src: (IN)  String variable.
 *         result: (IN/OUT) value with new size
 *       new_size: (IN)  New size to be reserved for the string (in bytes).
 *
 * Returns:
 *
 * Errors:
 *	ER_OBJ_INVALID_ARGUMENTS:
 *		  <src_string> is not CHAR, NCHAR, VARCHAR or VARNCHAR
 *
 * Note : src buffer is not freed, caller should be aware of this;
 *	  Result DB_VALUE must already be created.
 *	  It doesn't operate on BIT strings;
 */

static int
qstr_grow_string (DB_VALUE * src_string, DB_VALUE * result, int new_size)
{
  int error_status = NO_ERROR;
  int result_size = 0, src_length = 0, result_domain_length = 0, src_size = 0;
  unsigned char *r = NULL;
  DB_TYPE src_type, result_type;

  assert (src_string != (DB_VALUE *) NULL);
  assert (result != (DB_VALUE *) NULL);

  src_type = DB_VALUE_DOMAIN_TYPE (src_string);
  src_length = (int) DB_GET_STRING_LENGTH (src_string);
  result_domain_length = DB_VALUE_PRECISION (src_string);

  if (!QSTR_IS_CHAR (src_type) || DB_IS_NULL (src_string))
    {
      return ER_OBJ_INVALID_ARGUMENTS;
    }
  else
    {
      result_type = DB_TYPE_VARCHAR;
    }

  result_size = src_length * INTL_CODESET_MULT;

  src_size = DB_GET_STRING_SIZE (src_string);

  assert (new_size >= result_size);
  assert (new_size >= src_size);

  result_size = MAX (result_size, new_size);
  result_size = MAX (result_size, src_size);

  if (result_size > QSTR_STRING_MAX_SIZE_BYTES)
    {
      error_status = ER_QPROC_STRING_SIZE_TOO_BIG;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      error_status, 2, result_size, QSTR_STRING_MAX_SIZE_BYTES);
      assert (er_errid () != NO_ERROR);

      DB_MAKE_NULL (result);

      return error_status;
    }

  /* Allocate storage for the result string */
  r = (unsigned char *) malloc ((size_t) result_size + 1);
  if (r == NULL)
    {
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }
  memset (r, 0, (size_t) result_size + 1);

  if (src_size > 0)
    {
      memcpy ((char *) r, (char *) DB_PULL_STRING (src_string), src_size);
    }
  qstr_make_typed_string (result_type,
			  result,
			  result_domain_length,
			  (char *) r, (int) MIN (result_size, src_size),
			  DB_GET_STRING_COLLATION (src_string));

  result->need_clear = true;

  assert (error_status == NO_ERROR);

  return error_status;
}

/*
 * qstr_concatenate () -
 *
 * Arguments:
 *             s1: (IN)  First string pointer.
 *      s1_length: (IN)  Character length of <s1>.
 *   s1_precision: (IN)  Max character length of <s1>.
 *        s1_type: (IN)  Domain type of <s1>.
 *             s2: (IN)  Second string pointer.
 *      s2_length: (IN)  Character length of <s2>.
 *   s2_precision: (IN)  Max character length of <s2>.
 *        s2_type: (IN)  Domain type of <s2>.
 *         result: (OUT) Concatenated string pointer.
 *  result_length: (OUT) Character length of <result>.
 *    result_size: (OUT) Byte size of <result>.
 *    result_type: (OUT) Domain type of <result>
 *
 * Returns:
 *
 * Errors:
 *
 */

static int
qstr_concatenate (const unsigned char *s1,
		  int s1_length,
		  UNUSED_ARG int s1_precision,
		  DB_TYPE s1_type,
		  const unsigned char *s2,
		  int s2_length,
		  UNUSED_ARG int s2_precision,
		  DB_TYPE s2_type,
		  unsigned char **result,
		  int *result_length,
		  int *result_size,
		  DB_TYPE * result_type, DB_DATA_STATUS * data_status)
{
  int copy_length, copy_size;
  int pad1_length, pad2_length;
  int length_left, cat_length, cat_size;
  int s1_logical_length, s2_logical_length;
  int s1_size, s2_size;
  unsigned char *cat_ptr;
  int error_status = NO_ERROR;

  *data_status = DATA_STATUS_OK;

  s1_logical_length = s1_length;

  s2_logical_length = s2_length;

  /*
   *  The result length will be the sum of the lengths of
   *  the two source strings.  If this is greater than the
   *  maximum length of a variable length string, then the
   *  result length is adjusted appropriately.  This does
   *  not necessarily indicate a truncation condition.
   */
  *result_length = MIN ((s1_logical_length + s2_logical_length),
			QSTR_MAX_PRECISION (s1_type));

  *result_type = DB_TYPE_VARCHAR;

  intl_char_size ((unsigned char *) s1, s1_logical_length, &s1_size);
  intl_char_size ((unsigned char *) s2, s2_logical_length, &s2_size);

  if (s1_size == 0)
    {
      s1_size = s1_logical_length;
    }
  if (s2_size == 0)
    {
      s2_size = s2_logical_length;
    }

  *result_size = s1_size + s2_size;

  if (*result_size > QSTR_STRING_MAX_SIZE_BYTES)
    {
      goto size_error;
    }

  /*  Allocate the result string */
  *result = (unsigned char *) malloc ((size_t) * result_size + 1);
  if (*result == NULL)
    {
      goto mem_error;
    }

  /*
   *  Calculate the number of characters from string1 that can
   *  be copied to the result.  If we cannot copy the entire
   *  string and if the portion of the string which was not
   *  copied contained anything but pad characters, then raise
   *  a truncation exception.
   */
  copy_length = s1_length;
  if (copy_length > *result_length)
    {
      copy_length = *result_length;

      if (varchar_truncated ((unsigned char *) s1,
			     s1_type, s1_length, copy_length))
	{
	  *data_status = DATA_STATUS_TRUNCATED;
	}
    }
  intl_char_size ((unsigned char *) s1, copy_length, &copy_size);

  pad1_length = MIN (s1_logical_length, *result_length) - copy_length;
  length_left = *result_length - copy_length - pad1_length;

  /*
   *  Processess string2 as we did for string1.
   */
  cat_length = s2_length;
  if (cat_length > (*result_length - copy_length))
    {
      cat_length = *result_length - copy_length;

      if (varchar_truncated ((unsigned char *) s2,
			     s2_type, s2_length, cat_length))
	{
	  *data_status = DATA_STATUS_TRUNCATED;
	}
    }
  intl_char_size ((unsigned char *) s2, cat_length, &cat_size);

  pad2_length = length_left - cat_length;

  /*
   *  Actually perform the copy operations.
   */
  memcpy ((char *) *result, (char *) s1, copy_size);
  cat_ptr = qstr_pad_string ((unsigned char *) &((*result)[copy_size]),
			     pad1_length);

  memcpy ((char *) cat_ptr, (char *) s2, cat_size);
  (void) qstr_pad_string ((unsigned char *) &cat_ptr[cat_size], pad2_length);

  intl_char_size (*result, *result_length, result_size);

  return error_status;

size_error:
  error_status = ER_QPROC_STRING_SIZE_TOO_BIG;
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	  error_status, 2, *result_size, QSTR_STRING_MAX_SIZE_BYTES);
  return error_status;
  /*
   * Error handler
   */
mem_error:
  error_status = er_errid ();
  return error_status;
}

/*
 * varchar_truncated () -
 *
 * Arguments:
 *            s:  (IN) Pointer to input string.
 *     s_length:  (IN) Length of input string.
 *   used_chars:  (IN) Number of characters which were used by caller.
 *                     0 <= <used_chars> <= <s_length>
 * Returns: bool
 *
 * Errors:
 *
 * Note:
 *     This is a convenience function which is used by the concatenation
 *     function to determine if a variable length string has been
 *     truncated.  When concatenating variable length strings, the string
 *     is not considered truncated if only pad characters were omitted.
 *
 *     This function accepts a string <s>, its length <s_length>, and
 *     a count of characters <used_chars>.  If the remaining characters
 *     are all pad characters, then the function returns true value.
 *     A False value is returned otherwise.
 *
 */

static bool
varchar_truncated (const unsigned char *s,
		   DB_TYPE s_type, int s_length, int used_chars)
{
  unsigned char pad[2];
  int pad_size = 0, trim_length, trim_size;
  int s_size;

  bool truncated = false;

  intl_pad_char (pad, &pad_size);
  intl_char_size ((unsigned char *) s, s_length, &s_size);

  trim_trailing (pad, pad_size,
		 s, s_type, s_length, s_size, &trim_length, &trim_size);

  if (trim_length > used_chars)
    {
      truncated = true;
    }

  return truncated;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * varbit_truncated () -
 *
 * Arguments:
 *            s:  (IN) Pointer to input string.
 *     s_length:  (IN) Length of input string.
 *    used_bits:  (IN) Number of characters which were used by caller.
 *                     0 <= <used_chars> <= <s_length>
 *
 * Returns:
 *
 * Errors:
 *
 * Note:
 *     This is a convenience function which is used by the concatenation
 *     function to determine if a variable length string has been
 *     truncated.  When concatenating variable length strings, the bit
 *     string is not considered truncated if only 0's were omitted.
 *
 *     This function accepts a string <s>, its length <s_length>, and
 *     a count of characters <used_chars>.  If the remaining characters
 *     are all 0's, then the function returns true value.  A False value
 *     is returned otherwise.
 *
 */

static bool
varbit_truncated (const unsigned char *s, int s_length, int used_bits)
{
  int last_set_bit;
  bool truncated = false;


  last_set_bit = bstring_fls ((char *) s, BITS_TO_BYTES (s_length));

  if (last_set_bit > used_bits)
    {
      truncated = true;
    }

  return truncated;
}
#endif

/*
 * bit_ncat () -
 *
 * Arguments:
 *            r: Pointer to bit string 1
 *       offset: Number of bits in string1
 *            s: Pointer to bit string 2
 *            n: Number of bits in string 2
 *
 * Returns: void
 *
 * Errors:
 *
 * Note:
 *   Shift the bits of <s> onto the end of <r>.  This is a helper
 *   function to str_bit_concatenate.  This function shifts
 *   (concatenates) exactly the number of bits specified into the result
 *   buffer which must be preallocated to the correct size.
 *
 */

static void
bit_ncat (unsigned char *r, int offset, const unsigned char *s, int n)
{
  int i, copy_size, cat_size, total_size;
  unsigned int remainder, shift_amount;
  unsigned short tmp_shifted;
  unsigned char mask;

  copy_size = BITS_TO_BYTES (offset);
  cat_size = BITS_TO_BYTES (n);
  total_size = BITS_TO_BYTES (offset + n);

  remainder = offset % BITS_IN_BYTE;

  if (remainder == 0)
    {
      memcpy ((char *) &r[copy_size], (char *) s, cat_size);
    }
  else
    {
      int start_byte = copy_size - 1;

      shift_amount = BITS_IN_BYTE - remainder;
      mask = 0xff << shift_amount;

      /*
       *  tmp_shifted is loaded with a byte from the source
       *  string and shifted into poition.  The upper byte is
       *  used for the current destination location, while the
       *  lower byte is used by the next destination location.
       */
      for (i = start_byte; i < total_size; i++)
	{
	  tmp_shifted = (unsigned short) (s[i - start_byte]);
	  tmp_shifted = tmp_shifted << shift_amount;
	  r[i] = (r[i] & mask) | (tmp_shifted >> BITS_IN_BYTE);

	  if (i < (total_size - 1))
	    {
	      r[i + 1] =
		(unsigned char) (tmp_shifted & (unsigned short) 0xff);
	    }
	}
    }

  /*  Mask out the unused bits */
  mask = 0xff << (BITS_IN_BYTE - ((offset + n) % BITS_IN_BYTE));
  if (mask != 0)
    {
      r[total_size - 1] &= mask;
    }
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * bstring_fls () -
 *
 * Arguments:
 *            s: Pointer to source bit string
 *            n: Number of bits in string1
 *
 * Returns: int
 *
 * Errors:
 *
 * Note:
 *   Find the last set bit in the bit string.  The bits are numbered left
 *   to right starting at 1.  A value of 0 indicates that no set bits were
 *   found in the string.
 *
 */

static int
bstring_fls (const char *s, int n)
{
  int byte_num, bit_num, inter_bit_num;


  /*
   *  We are looking for the first non-zero byte (starting at the end).
   */
  byte_num = n - 1;
  while ((byte_num >= 0) && ((int) (s[byte_num]) == 0))
    {
      byte_num--;
    }

  /*
   *  If byte_num is < 0, then the string is all 0's.
   *  Othersize, byte_num is the index for the first byte which has
   *  some bits set (from the end).
   */
  if (byte_num < 0)
    {
      bit_num = 0;
    }
  else
    {
      inter_bit_num = (int) qstr_ffs ((int) (s[byte_num]));
      bit_num =
	(byte_num * BITS_IN_BYTE) + (BITS_IN_BYTE - inter_bit_num + 1);
    }

  return bit_num;
}
#endif

/*
 * qstr_bit_coerce () -
 *
 * Arguments:
 *        src_string:  (In) Source string
 *       dest_string: (Out) Coerced string
 *
 * Returns: DB_DATA_STATUS
 *
 * Errors:
 *
 * Note:
 *   This is a helper function which performs the actual coercion for
 *   bit strings.  It is called from db_bit_string_coerce().
 *
 *   If any loss of data due to truncation occurs DATA_STATUS_TRUNCATED
 *   is returned.
 *
 */

static int
qstr_bit_coerce (const unsigned char *src,
		 int src_length,
		 UNUSED_ARG int src_precision,
		 UNUSED_ARG DB_TYPE src_type,
		 unsigned char **dest,
		 int *dest_length,
		 int dest_precision,
		 UNUSED_ARG DB_TYPE dest_type, DB_DATA_STATUS * data_status)
{
  int src_padded_length, copy_size, dest_size, copy_length;
  int error_status = NO_ERROR;

  *data_status = DATA_STATUS_OK;

  src_padded_length = src_length;

  assert (dest_precision > 0);
  dest_precision *= BITS_IN_BYTE;

  /*
   *  If there is not enough precision in the destination string,
   *  then some bits will be omited from the source string.
   */
  if (src_padded_length > dest_precision)
    {
      src_padded_length = dest_precision;
      *data_status = DATA_STATUS_TRUNCATED;
    }

  copy_length = MIN (src_length, src_padded_length);
  copy_size = BITS_TO_BYTES (copy_length);

  /*
   *  For variable-length destination strings...
   *    Allocate enough for a fully padded source string, copy
   *    the source string and pad the rest.
   */
  *dest_length = MIN (src_padded_length, dest_precision);
  dest_size = BITS_TO_BYTES (*dest_length);

  *dest = (unsigned char *) malloc (dest_size + 1);
  if (*dest == NULL)
    {
      error_status = er_errid ();
    }
  else
    {
      bit_ncat (*dest, 0, src, copy_length);
      (void) memset ((char *) &((*dest)[copy_size]),
		     (int) 0, (dest_size - copy_size));
    }

  return error_status;
}

/*
 * qstr_coerce () -
 *
 * Arguments:
 *        src_string:  (In) Source string
 *       dest_string: (Out) Coerced string
 *
 * Returns: DB_DATA_STATUS
 *
 * Errors:
 *
 * Note:
 *   This is a helper function which performs the actual coercion for
 *   character strings.  It is called from db_char_string_coerce().
 *
 *   If any loss of data due to truncation occurs DATA_STATUS_TRUNCATED
 *   is returned.
 *
 */

static int
qstr_coerce (const unsigned char *src,
	     int src_length,
	     UNUSED_ARG int src_precision,
	     DB_TYPE src_type,
	     UNUSED_ARG INTL_CODESET src_codeset,
	     UNUSED_ARG INTL_CODESET dest_codeset,
	     unsigned char **dest,
	     int *dest_length,
	     int *dest_size,
	     int dest_precision,
	     UNUSED_ARG DB_TYPE dest_type, DB_DATA_STATUS * data_status)
{
  int src_padded_length, copy_length, copy_size;
  int alloc_size;
  char *end_of_string;
  int error_status = NO_ERROR;

  *data_status = DATA_STATUS_OK;
  *dest_size = 0;

  src_padded_length = src_length;

  /*
   *  Some characters will be truncated if there is not enough
   *  precision in the destination string.  If any of the
   *  truncated characters are non-pad characters, a truncation
   *  exception is raised.
   */
  if (src_padded_length > dest_precision)
    {
      src_padded_length = dest_precision;
      if ((src_length > src_padded_length) &&
	  (varchar_truncated (src, src_type, src_length, src_padded_length)))
	{
	  *data_status = DATA_STATUS_TRUNCATED;
	}
    }

  copy_length = MIN (src_length, src_padded_length);

  /*
   *  For variable-length destination strings...
   *    Allocate enough for a fully padded source string, copy
   *    the source string and pad the rest.
   */
  *dest_length = src_padded_length;

  /* copy_length = number of characters, count the bytes according to
   * source codeset */
  intl_char_size ((unsigned char *) src, copy_length, &copy_size);

  alloc_size = INTL_CODESET_MULT * (*dest_length);

  /* fix allocation size enough to fit copy size plus pad size */
  {
    unsigned char pad[2];
    int pad_size = 0;

    intl_pad_char (pad, &pad_size);
    alloc_size =
      MAX (alloc_size, copy_size + (*dest_length - copy_length) * pad_size);
  }

  if (!alloc_size)
    {
      alloc_size = 1;
    }

  *dest = (unsigned char *) malloc (alloc_size + 1);
  if (*dest == NULL)
    {
      error_status = er_errid ();
    }
  else
    {
      assert (copy_size >= 0);
      if (copy_size == 0)
	{
	  assert (alloc_size > 0);
	  **dest = '\0';
	}
      else
	{
	  if (copy_size > alloc_size)
	    {
	      copy_size = alloc_size;
	      *data_status = DATA_STATUS_TRUNCATED;
	    }
	  (void) memcpy ((char *) *dest, (char *) src, (int) copy_size);
	}

      end_of_string = (char *) qstr_pad_string ((unsigned char *)
						&((*dest)[copy_size]),
						(*dest_length - copy_length));
      *dest_size = CAST_STRLEN (end_of_string - (char *) (*dest));

      assert (*dest_size <= alloc_size);
    }

  return error_status;
}

/*
 * qstr_position () -
 *
 * Arguments:
 *        sub_string: String fragment to search for within <src_string>.
 *        sub_length: Number of characters in sub_string.
 *        src_string: String to be searched.
 *  src_string_bound: Bound of string buffer:
 *		      end of string buffer, if 'is_forward_search == true'
 *		      start of string buffer, if 'is_forward_search == false'
 *        src_length: Number of characters in src_string.
 * is_forward_search: forward search or backward search.
 *
 * Returns: int
 *
 * Errors:
 *
 * Note:
 *     This function accepts a source string <src_sring> and a string
 *     string fragment <sub_string> and returns the character position
 *     corresponding to the first occurance of <sub_string> within
 *     <src_string>.
 *
 *     This function works with National character strings.
 *
 */

static int
qstr_position (const char *sub_string, const int sub_size,
	       const int sub_length,
	       const char *src_string, const char *src_end,
	       const char *src_string_bound,
	       int src_length, int coll_id,
	       bool is_forward_search, int *position)
{
  int error_status = NO_ERROR;
  int dummy;

  *position = 0;

  if (sub_length == 0)
    {
      *position = 1;
    }
  else
    {
      int i, num_searches, current_position, result;
      unsigned char *ptr;
      int char_size;
      LANG_COLLATION *lc;

      lc = lang_get_collation (coll_id);
      assert (lc != NULL);

      /*
       *  Since the entire sub-string must be matched, a reduced
       *  number of compares <num_searches> are needed.  A collation-based
       *  comparison will be used.
       */
      if (lc->coll.uca_exp_num > 1 || lc->coll.count_contr > 0)
	{
	  /* characters may not match one-by-one */
	  num_searches = src_length;
	}
      else
	{
	  num_searches = src_length - sub_length + 1;
	  if (sub_length > src_length)
	    {
	      *position = 0;
	      return error_status;
	    }
	}

      /*
       *  Starting at the first position of the string, match the
       *  sub-string to the source string.  If a match is not found,
       *  then increment into the source string by one character and
       *  try again.  This is repeated until a match is found, or
       *  there are no more comparisons to be made.
       */
      ptr = (unsigned char *) src_string;
      current_position = 0;
      result = 1;

      for (i = 0; i < num_searches; i++)
	{
	  result = QSTR_MATCH (coll_id, ptr, (unsigned char *) src_end - ptr,
			       (unsigned char *) sub_string, sub_size,
			       NULL, false, &dummy);
	  current_position++;
	  if (result == 0)
	    {
	      break;
	    }

	  if (is_forward_search)
	    {
	      if (ptr >= (unsigned char *) src_string_bound)
		{
		  break;
		}

	      INTL_NEXT_CHAR (ptr, (unsigned char *) ptr, &char_size);
	    }
	  else
	    {
	      /* backward */
	      if (ptr > (unsigned char *) src_string_bound)
		{
		  ptr = intl_prev_char ((unsigned char *) ptr,
					(const unsigned char *)
					src_string_bound, &char_size);
		}
	      else
		{
		  break;
		}
	    }
	}

      /*
       *  Return the position of the match, if found.
       */
      if (result == 0)
	{
	  *position = current_position;
	}
    }

  return error_status;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * shift_left () -
 *
 * Arguments:
 *             bit_string: Byte array representing a bit string.
 *        bit_string_size: Number of bytes in the array.
 *
 * Returns: int
 *
 * Errors:
 *
 * Note:
 *     Shift the bit string left one bit.  The left most bit is shifted out
 *     and returned.  A 0 is inserted into the rightmost bit position.
 *     The entire array is shifted regardless of the number of significant
 *     bits in the array.
 *
 */

static int
shift_left (unsigned char *bit_string, int bit_string_size)
{
  int i, highest_bit;


  highest_bit = ((bit_string[0] & 0x80) != 0);
  bit_string[0] = bit_string[0] << 1;

  for (i = 1; i < bit_string_size; i++)
    {
      if (bit_string[i] & 0x80)
	{
	  bit_string[i - 1] |= 0x01;
	}

      bit_string[i] = bit_string[i] << 1;
    }

  return highest_bit;
}
#endif

/*
 * qstr_substring () -
 *
 * Arguments:
 *             src_string: Source string.
 *         start_position: Starting character position of sub-string.
 *      extraction_length: Length of sub-string.
 *             sub_string: Returned sub-string.
 *
 * Returns: void
 *
 * Errors:
 *
 * Note:
 *     Extract the sub-string from the source string.  The sub-string is
 *     specified by a starting position and length.
 *
 *     This functions works on character and national character strings.
 *
 */

static int
qstr_substring (const unsigned char *src,
		int src_length,
		int start,
		int length, unsigned char **r, int *r_length, int *r_size)
{
  int error_status = NO_ERROR;
  const unsigned char *sub;
  int src_size, leading_bytes;
  *r_size = 0;

  /* Get the size of the source string. */
  intl_char_size ((unsigned char *) src, src_length, &src_size);

  /*
   * Perform some error chaecking.
   * If the starting position is < 1, then set it to 1.
   * If the starting position is after the end of the source string,
   * then set the sub-string length to 0.
   * If the sub-string length will extend beyond the end of the source string,
   * then shorten the sub-string length to fit.
   */
  if (start < 1)
    {
      start = 1;
    }

  if (start > src_length)
    {
      start = 1;
      length = 0;
    }

  if ((length < 0) || ((start + length - 1) > src_length))
    {
      length = src_length - start + 1;
    }

  *r_length = length;

  /*
   *  Get a pointer to the start of the sub-string and the
   *  size of the sub-string.
   *
   *  Compute the starting byte of the sub-string.
   *  Compute the length of the sub-string in bytes.
   */
  intl_char_size ((unsigned char *) src, (start - 1), &leading_bytes);
  sub = &(src[leading_bytes]);
  intl_char_size ((unsigned char *) sub, *r_length, r_size);

  *r = (unsigned char *) malloc ((size_t) ((*r_size) + 1));
  if (*r == NULL)
    {
      error_status = er_errid ();
    }
  else
    {
      (void) memcpy (*r, sub, *r_size);
    }

  return error_status;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * left_nshift () -
 *
 * Arguments:
 *             bit_string: Byte array containing the bit string.
 *        bit_string_size: Size of the bit array in bytes.
 *           shift_amount: Number of bit positions to shift by.
 *                             range: 0 <= shift_amount
 *                      r: Pointer to result buffer where the shifted bit
 *                         array will be stored.
 *                 r_size: Size of the result array in bytes.
 *
 * Returns: void
 *
 * Errors:
 *
 * Note:
 *     Shift the bit string left <shift_amount> bits.  The left most bits
 *     are shifted out.  0's are inserted into the rightmost bit positions.
 *     The entire array is shifted regardless of the number of significant
 *     bits in the array.
 *
 */
static void
left_nshift (const unsigned char *bit_string,
	     int bit_string_size,
	     int shift_amount, unsigned char *r, int r_size)
{
  int i, shift_bytes, shift_bits, adj_bit_string_size;
  const unsigned char *ptr;

  shift_bytes = shift_amount / BITS_IN_BYTE;
  shift_bits = shift_amount % BITS_IN_BYTE;
  ptr = &(bit_string[shift_bytes]);

  adj_bit_string_size = bit_string_size - shift_bytes;

  for (i = 0; i < r_size; i++)
    {
      if (i < (adj_bit_string_size - 1))
	{
	  r[i] = ((ptr[i] << shift_bits) |
		  (ptr[i + 1] >> (BITS_IN_BYTE - shift_bits)));
	}
      else if (i == (adj_bit_string_size - 1))
	{
	  r[i] = (ptr[i] << shift_bits);
	}
      else
	{
	  r[i] = 0;
	}
    }
}

/*
 *  The version below handles multibyte character sets by promoting all
 *  characters to two bytes each.  Unfortunately, the current implementation
 *  of the regular expression package has some limitations with characters
 *  that are not char sized.  The above version works with char sized
 *  sets only and therefore will not work with national character sets.
 */

/*
 * qstr_ffs () -
 *   Returns: int
 *   v: (IN) Source string.
 *
 *
 * Errors:
 *
 * Note:
 *     Finds the first bit set in the passed argument and returns
 *     the index of that bit.  Bits are numbered starting at 1
 *     from the right.  A return value of 0 indicates that the value
 *     passed is zero.
 *
 */
static int
qstr_ffs (int v)
{
  int nbits;

  int i = 0;
  int position = 0;
  unsigned int uv = (unsigned int) v;

  nbits = sizeof (int) * 8;

  if (uv != 0)
    {
      while ((i < nbits) && (position == 0))
	{
	  if (uv & 0x01)
	    {
	      position = i + 1;
	    }

	  i++;
	  uv >>= 1;
	}
    }

  return position;
}
#endif

/*
 * hextoi () -
 *
 * Arguments:
 *             hex_char: (IN) Character containing ASCII hex character
 *
 * Returns: int
 *
 * Errors:
 *
 * Note:
 *     Returns the decimal value associated with the ASCII hex character.
 *     Will return a -1 if hex_char is not a hexadecimal ASCII character.
 *
 */

static int
hextoi (char hex_char)
{
  if ((hex_char >= '0') && (hex_char <= '9'))
    {
      return (hex_char - '0');
    }
  else if ((hex_char >= 'A') && (hex_char <= 'F'))
    {
      return (hex_char - 'A' + 10);
    }
  else if ((hex_char >= 'a') && (hex_char <= 'f'))
    {
      return (hex_char - 'a' + 10);
    }
  else
    {
      return (-1);
    }
}

/*
 * set_time_argument() - construct struct tm
 *   return:
 *   dest(out):
 *   year(in):
 *   month(in):
 *   day(in):
 *   hour(in):
 *   min(in):
 *   sec(in):
 */
static void
set_time_argument (struct tm *dest, int year, int month, int day,
		   int hour, int min, int sec)
{
  if (year >= 1900)
    {
      dest->tm_year = year - 1900;
    }
  else
    {
      dest->tm_year = -1;
    }
  dest->tm_mon = month - 1;
  dest->tm_mday = day;
  dest->tm_hour = hour;
  dest->tm_min = min;
  dest->tm_sec = sec;
  dest->tm_isdst = -1;
}

/*
 * calc_unix_timestamp() - calculates UNIX timestamp
 *   return:
 *   time_argument(in):
 */
static long
calc_unix_timestamp (struct tm *time_argument)
{
  time_t result;

  if (time_argument != NULL)
    {
      /* validation for tm fields in order to cover for mktime conversion's
         like 40th of Sept equals 10th of Oct */
      if (time_argument->tm_year < 0 || time_argument->tm_year > 9999
	  || time_argument->tm_mon < 0 || time_argument->tm_mon > 11
	  || time_argument->tm_mday < 1 || time_argument->tm_mday > 31
	  || time_argument->tm_hour < 0 || time_argument->tm_hour > 23
	  || time_argument->tm_min < 0 || time_argument->tm_min > 59
	  || time_argument->tm_sec < 0 || time_argument->tm_sec > 59)
	{
	  return -1L;
	}
      result = mktime (time_argument);

#if 0 /* DO NOT DELETE ME - not convert to UTC, respect current timezone */
      /* convert to UTC epoch time */
      if (strcmp (time_argument->tm_zone, "UTC") != 0)
        {
          result += time_argument->tm_gmtoff;
        }
#endif
    }
  else
    {
      result = time (NULL);
    }

  if (result < (time_t) 0)
    {
      return -1L;
    }
  return (long) result;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * parse_for_next_int () -
 *
 * Arguments:
 *         ch: char position from which we start parsing
 *	   output: integer read
 *
 * Returns: -1 if error, 0 if success
 *
 * Note:
 *  parses a string for integers while skipping non-alpha delimitators
 */
static int
parse_for_next_int (char **ch, char *output)
{
  int i;
  /* we need in fact only 6 (upper bound for the integers we want) */
  char buf[16];

  i = 0;
  memset (buf, 0, sizeof (buf));

  /* trailing zeroes - accept only 2 (for year 00 which is short for 2000) */
  while (**ch == '0')
    {
      if (i < 2)
	{
	  buf[i++] = **ch;
	}
      (*ch)++;
    }

  while (i < 6 && char_isdigit (**ch) && **ch != 0)
    {
      buf[i++] = **ch;
      (*ch)++;
    }
  if (i > 6)
    {
      return -1;
    }
  strcpy (output, buf);

  /* skip all delimitators */
  while (**ch != 0 && !char_isalpha (**ch) && !char_isdigit (**ch))
    {
      (*ch)++;
    }
  return 0;
}
#endif
/*
 * db_unix_timestamp () -
 *
 * Arguments:
 *         src_date: datetime from which we calculate timestamp
 *
 * Returns: int
 *
 * Errors:
 *
 * Note:
 * Returns a Unix timestamp (seconds since '1970-01-01 00:00:00' UTC)
 */
int
db_unix_timestamp (const DB_VALUE * src_date, DB_VALUE * result_timestamp)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_src_date;
  int val = 0;
  int month = 0, day = 0, year = 0;
  int second = 0, minute = 0, hour = 0, ms = 0;
  struct tm time_argument;

  assert (src_date != result_timestamp);

  /* check iff empty arg ; refer pt_evaluate_def_val () */
  if (src_date == NULL)
    {
      time_t now = 0;

      now = time (NULL);
      if (now < (time_t) 0)
	{
	  assert (false);	/* something wrong */
	  goto exit_on_error;
	}

      DB_MAKE_INT (result_timestamp, now);

      return NO_ERROR;
    }

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (1, &tmp_src_date);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (1, src_date))
    {
      DB_MAKE_NULL (result_timestamp);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  src_date =
    db_value_cast_arg (src_date, &tmp_src_date, DB_TYPE_DATETIME,
		       &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  assert (DB_VALUE_DOMAIN_TYPE (src_date) == DB_TYPE_DATETIME);

  /* The supported datetime range is '1970-01-01 00:00:01'
   * UTC to '2038-01-19 03:14:07' UTC */

  error_status = db_datetime_decode (DB_GET_DATETIME (src_date),
				     &month, &day, &year, &hour,
				     &minute, &second, &ms);
  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

  if (year == 0 && month == 0 && day == 0
      && hour == 0 && minute == 0 && second == 0 && ms == 0)
    {
      /* This function should return 0 if the date is zero date */
      DB_MAKE_INT (result_timestamp, 0);
      goto done;
    }

  if (year < 1970 || year > 2038)
    {
      error_status = ER_OBJ_INVALID_ARGUMENTS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  set_time_argument (&time_argument, year, month, day, hour, minute, second);
  val = (int) calc_unix_timestamp (&time_argument);
  if (val < 0)
    {
      error_status = ER_OBJ_INVALID_ARGUMENTS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  DB_MAKE_INT (result_timestamp, val);

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_src_date);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (result_timestamp);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_src_date);

  return error_status;
}
