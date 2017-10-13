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
 * arithmetic.c - arithmetic functions
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <float.h>
#include <math.h>

#include "set_object.h"
#include "arithmetic.h"
#include "error_manager.h"
#include "memory_alloc.h"
#include "object_representation.h"
#include "object_domain.h"
#include "numeric_opfunc.h"
#include "db.h"
#include "query_opfunc.h"

/* this must be the last header file included!!! */
#include "dbval.h"

static int db_mod_int (DB_VALUE * value, DB_VALUE * value1,
		       DB_VALUE * value2);
static int db_mod_bigint (DB_VALUE * value, DB_VALUE * value1,
			  DB_VALUE * value2);
static int db_mod_double (DB_VALUE * value, DB_VALUE * value1,
			  DB_VALUE * value2);
static int db_mod_string (DB_VALUE * value, DB_VALUE * value1,
			  DB_VALUE * value2);
static int db_mod_numeric (DB_VALUE * value, DB_VALUE * value1,
			   DB_VALUE * value2);
static int round_double (double num, double y, double *result);
static double truncate_double (double num, double integer);
static DB_BIGINT truncate_bigint (DB_BIGINT num, DB_BIGINT integer);
static int get_number_dbval_as_double (double *d, const DB_VALUE * value);
#if defined (ENABLE_UNUSED_FUNCTION)
static int get_number_dbval_as_long_double (long double *ld,
					    const DB_VALUE * value);
#endif
static int db_width_bucket_calculate_numeric (double *result,
					      const DB_VALUE * value1,
					      const DB_VALUE * value2,
					      const DB_VALUE * value3,
					      const DB_VALUE * value4);

static double qdata_coerce_numeric_to_double (DB_VALUE * numeric_val_p);

static int qdata_add_int_to_dbval (DB_VALUE * int_val_p,
				   DB_VALUE * dbval_p, DB_VALUE * result_p);
static int qdata_add_bigint_to_dbval (DB_VALUE * bigint_val_p,
				      DB_VALUE * dbval_p,
				      DB_VALUE * result_p);
static int qdata_add_double_to_dbval (DB_VALUE * double_val_p,
				      DB_VALUE * dbval_p,
				      DB_VALUE * result_p);
static int qdata_add_numeric_to_dbval (DB_VALUE * numeric_val_p,
				       DB_VALUE * dbval_p,
				       DB_VALUE * result_p);
#if defined (ENABLE_UNUSED_FUNCTION)
static int qdata_add_sequence_to_dbval (DB_VALUE * seq_val_p,
					DB_VALUE * dbval_p,
					DB_VALUE * result_p,
					TP_DOMAIN * domain_p);
#endif

static int qdata_add_int (int i1, int i2, DB_VALUE * result_p);
static int qdata_add_bigint (DB_BIGINT i1, DB_BIGINT i2, DB_VALUE * result_p);
static int qdata_add_double (double d1, double d2, DB_VALUE * result_p);
static int qdata_add_numeric (DB_VALUE * numeric_val_p, DB_VALUE * dbval_p,
			      DB_VALUE * result_p);

static int qdata_subtract_int_to_dbval (DB_VALUE * int_val_p,
					DB_VALUE * dbval_p,
					DB_VALUE * result_p);
static int qdata_subtract_bigint_to_dbval (DB_VALUE * bigint_val_p,
					   DB_VALUE * dbval_p,
					   DB_VALUE * result_p);
static int qdata_subtract_double_to_dbval (DB_VALUE * double_val_p,
					   DB_VALUE * dbval_p,
					   DB_VALUE * result_p);
static int qdata_subtract_numeric_to_dbval (DB_VALUE * numeric_val_p,
					    DB_VALUE * dbval_p,
					    DB_VALUE * result_p);
#if defined (ENABLE_UNUSED_FUNCTION)
static int qdata_subtract_sequence_to_dbval (DB_VALUE * seq_val_p,
					     DB_VALUE * dbval_p,
					     DB_VALUE * result_p,
					     TP_DOMAIN * domain_p);
#endif
static int qdata_subtract_int (int i1, int i2, DB_VALUE * result_p);
static int qdata_subtract_bigint (DB_BIGINT i1, DB_BIGINT i2,
				  DB_VALUE * result_p);
static int qdata_subtract_double (double d1, double d2, DB_VALUE * result_p);

static int qdata_multiply_int_to_dbval (DB_VALUE * int_val_p,
					DB_VALUE * dbval_p,
					DB_VALUE * result_p);
static int qdata_multiply_bigint_to_dbval (DB_VALUE * bigint_val_p,
					   DB_VALUE * dbval_p,
					   DB_VALUE * result_p);
static int qdata_multiply_double_to_dbval (DB_VALUE * double_val_p,
					   DB_VALUE * dbval_p,
					   DB_VALUE * result_p);
static int qdata_multiply_numeric_to_dbval (DB_VALUE * numeric_val_p,
					    DB_VALUE * dbval_p,
					    DB_VALUE * result_p);
static int qdata_multiply_int (DB_VALUE * int_val_p, int i2,
			       DB_VALUE * result_p);
static int qdata_multiply_bigint (DB_VALUE * bigint_val_p, DB_BIGINT bi2,
				  DB_VALUE * result_p);
static int qdata_multiply_double (double d1, double d2, DB_VALUE * result_p);
static int qdata_multiply_numeric (DB_VALUE * numeric_val_p,
				   DB_VALUE * dbval, DB_VALUE * result_p);

static bool qdata_is_divided_zero (DB_VALUE * dbval_p);
static int qdata_divide_int_to_dbval (DB_VALUE * int_val_p,
				      DB_VALUE * dbval_p,
				      DB_VALUE * result_p);
static int qdata_divide_bigint_to_dbval (DB_VALUE * bigint_val_p,
					 DB_VALUE * dbval_p,
					 DB_VALUE * result_p);
static int qdata_divide_double_to_dbval (DB_VALUE * double_val_p,
					 DB_VALUE * dbval_p,
					 DB_VALUE * result_p);
static int qdata_divide_numeric_to_dbval (DB_VALUE * numeric_val_p,
					  DB_VALUE * dbval_p,
					  DB_VALUE * result_p);
static int qdata_divide_int (int i1, int i2, DB_VALUE * result_p);
static int qdata_divide_bigint (DB_BIGINT bi1, DB_BIGINT bi2,
				DB_VALUE * result_p);
static int qdata_divide_double (double d1, double d2,
				DB_VALUE * result_p, bool is_check_overflow);

/*
 * db_floor_dbval () - take floor of db_value
 *   return: NO_ERROR
 *   result(out) : resultant db_value
 *   value(in)   : input db_value
 */
int
db_floor_dbval (DB_VALUE * result, DB_VALUE * value)
{
  DB_TYPE res_type;
  double dtmp;
  int er_status = NO_ERROR;
  DB_VALUE cast_value;

  assert (result != value);

  res_type = DB_VALUE_DOMAIN_TYPE (value);
  if (res_type == DB_TYPE_NULL || DB_IS_NULL (value))
    {
      DB_MAKE_NULL (result);
      return er_status;
    }

  switch (res_type)
    {
    case DB_TYPE_INTEGER:
      DB_MAKE_INT (result, DB_GET_INTEGER (value));
      break;
    case DB_TYPE_BIGINT:
      DB_MAKE_BIGINT (result, DB_GET_BIGINT (value));
      break;
    case DB_TYPE_VARCHAR:
      DB_MAKE_NULL (&cast_value);
      er_status = tp_value_str_cast_to_number (value, &cast_value, &res_type);
      if (er_status != NO_ERROR)
	{
	  return er_status;
	}

      assert (res_type == DB_TYPE_DOUBLE);

      value = &cast_value;

      /* fall through */

    case DB_TYPE_DOUBLE:
      dtmp = floor (DB_GET_DOUBLE (value));
      DB_MAKE_DOUBLE (result, (double) dtmp);
      break;
    case DB_TYPE_NUMERIC:
      {
	int p = DB_VALUE_PRECISION (value), s = DB_VALUE_SCALE (value);

	if (s)
	  {
	    unsigned char num[DB_NUMERIC_BUF_SIZE];
	    char num_str[DB_MAX_NUMERIC_PRECISION * 4 + 2] = { '\0' };
	    char *num_str_p;
	    int num_str_len;
	    bool decrement = false;

	    num_str_p = num_str + 1;
	    numeric_coerce_num_to_dec_str (DB_PULL_NUMERIC (value),
					   num_str_p);
	    num_str_len = strlen (num_str_p);

	    num_str_p += num_str_len - s;

	    while (*num_str_p)
	      {
		if (*num_str_p != '0')
		  {
		    *num_str_p = '0';
		    decrement = true;
		  }

		num_str_p++;
	      }

	    if (decrement && num_str[1] == '-')
	      {
		/* To decrement a negative value, the absolute value (the
		 * digits) actually has to be incremented. */

		char *num_str_digits = num_str + num_str_len - p;
		bool carry = true;

		num_str_p = num_str + num_str_len - s;
		while (*num_str_p == '9')
		  {
		    *num_str_p-- = '0';
		  }

		if (*num_str_p == '-')
		  {
		    num_str[0] = '-';
		    *num_str_p = '1';
		  }
		else
		  {
		    (*num_str_p)++;
		    carry = false;
		  }

		if (carry || num_str_p <= num_str_digits)
		  {
		    if (p < DB_MAX_NUMERIC_PRECISION)
		      {
			p++;
		      }
		    else
		      {
			s--;
			num_str[num_str_len] = '\0';
		      }
		  }

		if (num_str[0])
		  {
		    num_str_p = num_str;
		  }
		else
		  {
		    num_str_p = num_str + 1;
		  }

		numeric_coerce_dec_str_to_num (num_str_p, num);
		er_status = DB_MAKE_NUMERIC (result, num, p, s);
	      }
	    else
	      {
		/* given numeric is positive or already rounded */
		numeric_coerce_dec_str_to_num (num_str + 1, num);
		er_status = DB_MAKE_NUMERIC (result, num, p, s);
	      }
	  }
	else
	  {
	    /* given numeric number is already of integral type */
	    er_status =
	      DB_MAKE_NUMERIC (result, DB_PULL_NUMERIC (value), p, 0);
	  }

	break;
      }
    default:
      er_status = ER_TP_CANT_COERCE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, er_status, 2,
	      pr_type_name (res_type), pr_type_name (DB_TYPE_DOUBLE));
      break;
    }

  return er_status;
}

/*
 * db_ceil_dbval () - take ceil of db_value
 *   return: NO_ERROR
 *   result(out) : resultant db_value
 *   value(in)   : input db_value
 */
int
db_ceil_dbval (DB_VALUE * result, DB_VALUE * value)
{
  DB_TYPE res_type;
  double dtmp;
  int er_status = NO_ERROR;
  DB_VALUE cast_value;

  assert (result != value);

  res_type = DB_VALUE_DOMAIN_TYPE (value);
  if (res_type == DB_TYPE_NULL || DB_IS_NULL (value))
    {
      DB_MAKE_NULL (result);
      return er_status;
    }

  switch (res_type)
    {
    case DB_TYPE_INTEGER:
      DB_MAKE_INT (result, DB_GET_INTEGER (value));
      break;
    case DB_TYPE_BIGINT:
      DB_MAKE_BIGINT (result, DB_GET_BIGINT (value));
      break;
    case DB_TYPE_VARCHAR:
      DB_MAKE_NULL (&cast_value);
      er_status = tp_value_str_cast_to_number (value, &cast_value, &res_type);
      if (er_status != NO_ERROR)
	{
	  return er_status;
	}

      assert (res_type == DB_TYPE_DOUBLE);

      value = &cast_value;

      /* fall through */

    case DB_TYPE_DOUBLE:
      dtmp = ceil (DB_GET_DOUBLE (value));
      DB_MAKE_DOUBLE (result, (double) dtmp);
      break;
    case DB_TYPE_NUMERIC:
      {
	int s = DB_VALUE_SCALE (value), p = DB_VALUE_PRECISION (value);

	if (s)
	  {
	    char num_str[DB_MAX_NUMERIC_PRECISION * 4 + 2] = { '\0' };
	    char *num_str_p;
	    int num_str_len = 0;
	    bool increment = false;

	    num_str_p = num_str + 1;
	    numeric_coerce_num_to_dec_str (db_locate_numeric (value),
					   num_str_p);
	    if (num_str_p[0] == '-')
	      {
		num_str_p++;
	      }

	    num_str_len = strlen (num_str_p);
	    num_str_p += num_str_len - s;

	    while (*num_str_p)
	      {
		if (*num_str_p != '0')
		  {
		    increment = true;
		    *num_str_p = '0';
		  }

		num_str_p++;
	      }

	    if (increment)
	      {
		unsigned char num[DB_NUMERIC_BUF_SIZE];
		if (num_str[1] == '-')
		  {
		    /* CEIL(-3.1) is -3.0, as opposed to CEIL(+3.1) which is 4 */
		    numeric_coerce_dec_str_to_num (num_str + 1, num);
		    er_status = DB_MAKE_NUMERIC (result, num, p, s);
		  }
		else
		  {
		    bool carry = true;
		    char *num_str_digits = num_str + 1 + num_str_len - p;

		    /* position num_str_p one digit in front of the decimal point */
		    num_str_p = num_str;
		    num_str_p += num_str_len - s;

		    while (*num_str_p == '9')
		      {
			*num_str_p-- = '0';
		      }

		    if (*num_str_p)
		      {
			(*num_str_p)++;
			carry = false;
		      }

		    if (carry || num_str_p < num_str_digits)
		      {
			if (carry)
			  {
			    *num_str_p = '1';
			  }

			if (p < DB_MAX_NUMERIC_PRECISION)
			  {
			    p++;
			  }
			else
			  {
			    num_str[num_str_len] = '\0';
			    s--;
			  }
		      }
		    else
		      {
			num_str_p = num_str + 1;
		      }

		    numeric_coerce_dec_str_to_num (num_str_p, num);
		    er_status = DB_MAKE_NUMERIC (result, num, p, s);
		  }
	      }
	    else
	      {
		/* the given numeric value is already an integer */
		er_status =
		  DB_MAKE_NUMERIC (result, db_locate_numeric (value), p, s);
	      }
	  }
	else
	  {
	    /* the given numeric value has a scale of 0 */
	    er_status =
	      DB_MAKE_NUMERIC (result, db_locate_numeric (value), p, 0);
	  }

	break;
      }
    default:
      er_status = ER_TP_CANT_COERCE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, er_status, 2,
	      pr_type_name (res_type), pr_type_name (DB_TYPE_DOUBLE));
      break;
    }

  return er_status;
}

/*
 * db_sign_dbval - take sign of db_value
 *   return: NO_ERROR
 *   result(out) : resultant db_value
 *   value(in)   : input db_value
 */
int
db_sign_dbval (DB_VALUE * result, DB_VALUE * value)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_value;
  DB_TYPE res_type;
  int itmp;
  DB_BIGINT bitmp;
  double dtmp;

  assert (result != value);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (1, &tmp_value);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (1, value))
    {
      DB_MAKE_NULL (result);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  res_type = DB_VALUE_DOMAIN_TYPE (value);

  if (!TP_IS_NUMERIC_TYPE (res_type))
    {
      value =
	db_value_cast_arg (value, &tmp_value, DB_TYPE_DOUBLE, &error_status);
    }

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  res_type = DB_VALUE_DOMAIN_TYPE (value);

  switch (res_type)
    {
    case DB_TYPE_INTEGER:
      itmp = DB_GET_INTEGER (value);
      if (itmp == 0)
	{
	  DB_MAKE_INT (result, 0);
	}
      else if (itmp < 0)
	{
	  DB_MAKE_INT (result, -1);
	}
      else
	{
	  DB_MAKE_INT (result, 1);
	}
      break;
    case DB_TYPE_BIGINT:
      bitmp = DB_GET_BIGINT (value);
      if (bitmp == 0)
	{
	  DB_MAKE_INT (result, 0);
	}
      else if (bitmp < 0)
	{
	  DB_MAKE_INT (result, -1);
	}
      else
	{
	  DB_MAKE_INT (result, 1);
	}
      break;
    case DB_TYPE_DOUBLE:
      dtmp = DB_GET_DOUBLE (value);
      if (dtmp == 0)
	{
	  DB_MAKE_INT (result, 0);
	}
      else if (dtmp < 0)
	{
	  DB_MAKE_INT (result, -1);
	}
      else
	{
	  DB_MAKE_INT (result, 1);
	}
      break;
    case DB_TYPE_NUMERIC:
      numeric_coerce_num_to_double (db_locate_numeric (value),
				    DB_VALUE_SCALE (value), &dtmp);
      if (dtmp == 0)
	{
	  DB_MAKE_INT (result, 0);
	}
      else if (dtmp < 0)
	{
	  DB_MAKE_INT (result, -1);
	}
      else
	{
	  DB_MAKE_INT (result, 1);
	}
      break;
    default:
      assert (false);
      error_status = ER_TP_CANT_COERCE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
	      pr_type_name (res_type), pr_type_name (DB_TYPE_DOUBLE));
      goto exit_on_error;
      break;
    }

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_value);

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
  db_value_clear_nargs (1, &tmp_value);

  return error_status;
}

/*
 * db_abs_dbval () - take absolute value of db_value
 *   return: NO_ERROR
 *   result(out) : resultant db_value
 *   value(in)   : input db_value
 */
int
db_abs_dbval (DB_VALUE * result, DB_VALUE * value)
{
  DB_TYPE res_type;
  int itmp;
  DB_BIGINT bitmp;
  double dtmp;
  int er_status = NO_ERROR;
  DB_VALUE cast_value;

  assert (result != value);

  res_type = DB_VALUE_DOMAIN_TYPE (value);
  if (res_type == DB_TYPE_NULL || DB_IS_NULL (value))
    {
      DB_MAKE_NULL (result);
      return er_status;
    }

  switch (res_type)
    {
    case DB_TYPE_INTEGER:
      itmp = DB_GET_INTEGER (value);
      itmp = abs (itmp);
      DB_MAKE_INT (result, itmp);
      break;
    case DB_TYPE_BIGINT:
      bitmp = DB_GET_BIGINT (value);
      bitmp = llabs (bitmp);
      DB_MAKE_BIGINT (result, bitmp);
      break;

    case DB_TYPE_VARCHAR:
      DB_MAKE_NULL (&cast_value);
      er_status = tp_value_str_cast_to_number (value, &cast_value, &res_type);
      if (er_status != NO_ERROR)
	{
	  return er_status;
	}

      assert (res_type == DB_TYPE_DOUBLE);

      value = &cast_value;

      /* fall through */

    case DB_TYPE_DOUBLE:
      dtmp = DB_GET_DOUBLE (value);
      dtmp = fabs (dtmp);
      DB_MAKE_DOUBLE (result, (double) dtmp);
      break;
    case DB_TYPE_NUMERIC:
      {
	unsigned char num[DB_NUMERIC_BUF_SIZE];

	numeric_db_value_abs (db_locate_numeric (value), num);
	er_status = DB_MAKE_NUMERIC (result, num,
				     DB_VALUE_PRECISION (value),
				     DB_VALUE_SCALE (value));
	break;
      }
    default:
      er_status = ER_TP_CANT_COERCE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, er_status, 2,
	      pr_type_name (res_type), pr_type_name (DB_TYPE_DOUBLE));
      break;
    }

  return er_status;
}

/*
 * db_exp_dbval () - take exponential value of db_value
 *   return: NO_ERROR
 *   result(out) : resultant db_value
 *   value(in)   : input db_value
 */
int
db_exp_dbval (DB_VALUE * result, DB_VALUE * value)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_value;
  DB_TYPE type;
  int i;
  double d;
  double dtmp;
  DB_BIGINT bi;

  assert (result != value);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (1, &tmp_value);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (1, value))
    {
      DB_MAKE_NULL (result);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  type = DB_VALUE_DOMAIN_TYPE (value);

  if (!TP_IS_NUMERIC_TYPE (type))
    {
      value =
	db_value_cast_arg (value, &tmp_value, DB_TYPE_DOUBLE, &error_status);
    }

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  type = DB_VALUE_DOMAIN_TYPE (value);

  switch (type)
    {
    case DB_TYPE_INTEGER:
      i = DB_GET_INTEGER (value);
      dtmp = exp ((double) i);
      break;
    case DB_TYPE_BIGINT:
      bi = DB_GET_BIGINT (value);
      dtmp = exp ((double) bi);
      break;
    case DB_TYPE_DOUBLE:
      d = DB_GET_DOUBLE (value);
      dtmp = exp (d);
      break;
    case DB_TYPE_NUMERIC:
      numeric_coerce_num_to_double (db_locate_numeric (value),
				    DB_VALUE_SCALE (value), &d);
      dtmp = exp (d);
      break;
    default:
      assert (false);
      error_status = ER_TP_CANT_COERCE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
	      pr_type_name (type), pr_type_name (DB_TYPE_DOUBLE));
      goto exit_on_error;
      break;
    }

  if (OR_CHECK_DOUBLE_OVERFLOW (dtmp))
    {
      error_status = ER_QPROC_OVERFLOW_EXP;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  DB_MAKE_DOUBLE (result, dtmp);

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_value);

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
  db_value_clear_nargs (1, &tmp_value);

  return error_status;
}

/*
 * db_sqrt_dbval () - take sqrt value of db_value
 *   return: NO_ERROR
 *   result(out): resultant db_value
 *   value(in) : input db_value
 */
int
db_sqrt_dbval (DB_VALUE * result, DB_VALUE * value)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_value;
  DB_TYPE type;
  int i;
  double d;
  double dtmp;
  DB_BIGINT bi;

  assert (result != value);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (1, &tmp_value);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (1, value))
    {
      DB_MAKE_NULL (result);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  type = DB_VALUE_DOMAIN_TYPE (value);

  if (!TP_IS_NUMERIC_TYPE (type))
    {
      value =
	db_value_cast_arg (value, &tmp_value, DB_TYPE_DOUBLE, &error_status);
    }

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  type = DB_VALUE_DOMAIN_TYPE (value);

  switch (type)
    {
    case DB_TYPE_INTEGER:
      i = DB_GET_INTEGER (value);
      if (i < 0)
	{
	  error_status = ER_QPROC_FUNCTION_ARG_ERROR;
	  goto exit_on_error;
	}
      dtmp = sqrt ((double) i);
      break;
    case DB_TYPE_BIGINT:
      bi = DB_GET_BIGINT (value);
      if (bi < 0)
	{
	  error_status = ER_QPROC_FUNCTION_ARG_ERROR;
	  goto exit_on_error;
	}
      dtmp = sqrt ((double) bi);
      break;
    case DB_TYPE_DOUBLE:
      d = DB_GET_DOUBLE (value);
      if (d < 0)
	{
	  error_status = ER_QPROC_FUNCTION_ARG_ERROR;
	  goto exit_on_error;
	}
      dtmp = sqrt (d);
      break;
    case DB_TYPE_NUMERIC:
      numeric_coerce_num_to_double (db_locate_numeric (value),
				    DB_VALUE_SCALE (value), &d);
      if (d < 0)
	{
	  error_status = ER_QPROC_FUNCTION_ARG_ERROR;
	  goto exit_on_error;
	}
      dtmp = sqrt (d);
      break;
    default:
      assert (false);
      error_status = ER_TP_CANT_COERCE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
	      pr_type_name (type), pr_type_name (DB_TYPE_DOUBLE));
      goto exit_on_error;
      break;
    }

  DB_MAKE_DOUBLE (result, dtmp);

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_value);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (error_status == ER_QPROC_FUNCTION_ARG_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_FUNCTION_ARG_ERROR,
	      1, "sqrt()");
    }
  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (result);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_value);

  return error_status;
}

/*
 * db_power_dbval () - take power value of db_value
 *   return: NO_ERROR, ER_FAILED
 *   result(out) : resultant db_value
 *   value1(in)  : first db_value
 *   value2(in)  : second db_value
 */
int
db_power_dbval (DB_VALUE * result, DB_VALUE * value1, DB_VALUE * value2)
{
  double d1, d2;
  double dtmp;
  int error = NO_ERROR;

  assert (result != value1);
  assert (result != value2);

  if (DB_IS_NULL (value1) || DB_IS_NULL (value2))
    {
      DB_MAKE_NULL (result);
      return NO_ERROR;
    }

  error = get_number_dbval_as_double (&d1, value1);
  if (error != NO_ERROR)
    {
      goto pow_error;
    }
  error = get_number_dbval_as_double (&d2, value2);
  if (error != NO_ERROR)
    {
      goto pow_error;
    }

  if (d1 < 0 && d2 != ceil (d2))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_POWER_ERROR, 0);
      goto pow_error;
    }

  dtmp = pow (d1, d2);
  if (OR_CHECK_DOUBLE_OVERFLOW (dtmp))
    {
      goto pow_overflow;
    }

  DB_MAKE_DOUBLE (result, dtmp);

  return NO_ERROR;

pow_overflow:
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_POWER, 0);
  return ER_FAILED;

pow_error:
  return ER_FAILED;
}

/*
 * db_mod_int () - take mod value of value1(int) with value2
 *   return: NO_ERROR, ER_FAILED
 *   result(out) : resultant db_value
 *   value1(in)  : int db_value
 *   value2(in)  : second db_value
 */
static int
db_mod_int (DB_VALUE * result, DB_VALUE * value1, DB_VALUE * value2)
{
#if !defined(NDEBUG)
  DB_TYPE type1;
#endif
  DB_TYPE type2;
  int i1, i2;
  double d2;
  DB_BIGINT bi2;
  double dtmp;
  unsigned char num[DB_NUMERIC_BUF_SIZE];
  int p, s;
  int er_status = NO_ERROR;
  DB_VALUE cast_value2;

  assert (result != NULL && value1 != NULL && value2 != NULL);

  DB_MAKE_NULL (&cast_value2);

#if !defined(NDEBUG)
  type1 = DB_VALUE_DOMAIN_TYPE (value1);
  assert (type1 == DB_TYPE_INTEGER);
#endif

  i1 = DB_GET_INTEGER (value1);

  type2 = DB_VALUE_DOMAIN_TYPE (value2);
  switch (type2)
    {
    case DB_TYPE_INTEGER:
      i2 = DB_GET_INTEGER (value2);
      if (i2 == 0)
	{
	  DB_MAKE_INT (result, i1);
	}
      else
	{
	  DB_MAKE_INT (result, (int) (i1 % i2));
	}
      break;
    case DB_TYPE_BIGINT:
      bi2 = DB_GET_BIGINT (value2);
      if (bi2 == 0)
	{
	  DB_MAKE_BIGINT (result, i1);
	}
      else
	{
	  DB_MAKE_BIGINT (result, (DB_BIGINT) (i1 % bi2));
	}
      break;
    case DB_TYPE_VARCHAR:
      er_status = tp_value_str_cast_to_number (value2, &cast_value2, &type2);
      if (er_status != NO_ERROR)
	{
	  goto exit;
	}

      assert (type2 == DB_TYPE_DOUBLE);

      value2 = &cast_value2;

      /* fall through */

    case DB_TYPE_DOUBLE:
      d2 = DB_GET_DOUBLE (value2);
      if (d2 == 0)
	{
	  DB_MAKE_DOUBLE (result, i1);
	}
      else
	{
	  DB_MAKE_DOUBLE (result, (double) fmod (i1, d2));
	}
      break;
    case DB_TYPE_NUMERIC:
      numeric_coerce_num_to_double (db_locate_numeric (value2),
				    DB_VALUE_SCALE (value2), &d2);
      if (d2 == 0)
	{
	  er_status = qdata_coerce_dbval_to_numeric (value1, result);
	}
      else
	{
	  dtmp = fmod (i1, d2);
	  (void) numeric_internal_double_to_num (dtmp,
						 DB_VALUE_SCALE (value2),
						 num, &p, &s);
	  er_status = DB_MAKE_NUMERIC (result, num, p, s);
	}
      break;
    default:
      er_status = ER_TP_CANT_COERCE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, er_status, 2,
	      pr_type_name (type2), pr_type_name (DB_TYPE_DOUBLE));
      goto exit;
    }

exit:
  return er_status;
}

/*
 * db_mod_bigint () - take mod value of value1(bigint) with value2
 *   return: NO_ERROR, ER_FAILED
 *   result(out) : resultant db_value
 *   value1(in)  : bigint db_value
 *   value2(in)  : second db_value
 */
static int
db_mod_bigint (DB_VALUE * result, DB_VALUE * value1, DB_VALUE * value2)
{
#if !defined(NDEBUG)
  DB_TYPE type1;
#endif
  DB_TYPE type2;
  int i2;
  double d2;
  DB_BIGINT bi1, bi2;
  double dtmp;
  unsigned char num[DB_NUMERIC_BUF_SIZE];
  int p, s;
  int er_status = NO_ERROR;
  DB_VALUE cast_value2;

  assert (result != NULL);
  assert (value1 != NULL);
  assert (value2 != NULL);

  DB_MAKE_NULL (&cast_value2);

#if !defined(NDEBUG)
  type1 = DB_VALUE_DOMAIN_TYPE (value1);
  assert (type1 == DB_TYPE_BIGINT);
#endif

  bi1 = DB_GET_BIGINT (value1);

  type2 = DB_VALUE_DOMAIN_TYPE (value2);
  switch (type2)
    {
    case DB_TYPE_INTEGER:
      i2 = DB_GET_INTEGER (value2);
      if (i2 == 0)
	{
	  DB_MAKE_BIGINT (result, bi1);
	}
      else
	{
	  DB_MAKE_BIGINT (result, (DB_BIGINT) (bi1 % i2));
	}
      break;
    case DB_TYPE_BIGINT:
      bi2 = DB_GET_BIGINT (value2);
      if (bi2 == 0)
	{
	  DB_MAKE_BIGINT (result, bi1);
	}
      else
	{
	  DB_MAKE_BIGINT (result, (DB_BIGINT) (bi1 % bi2));
	}
      break;
    case DB_TYPE_VARCHAR:
      er_status = tp_value_str_cast_to_number (value2, &cast_value2, &type2);
      if (er_status != NO_ERROR)
	{
	  goto exit;
	}

      assert (type2 == DB_TYPE_DOUBLE);

      value2 = &cast_value2;

      /* fall through */

    case DB_TYPE_DOUBLE:
      d2 = DB_GET_DOUBLE (value2);
      if (d2 == 0)
	{
	  DB_MAKE_DOUBLE (result, (double) bi1);
	}
      else
	{
	  DB_MAKE_DOUBLE (result, (double) fmod ((double) bi1, d2));
	}
      break;
    case DB_TYPE_NUMERIC:
      numeric_coerce_num_to_double (db_locate_numeric (value2),
				    DB_VALUE_SCALE (value2), &d2);
      if (d2 == 0)
	{
	  er_status = qdata_coerce_dbval_to_numeric (value1, result);
	}
      else
	{
	  dtmp = fmod ((double) bi1, d2);
	  (void) numeric_internal_double_to_num (dtmp,
						 DB_VALUE_SCALE (value2),
						 num, &p, &s);
	  er_status = DB_MAKE_NUMERIC (result, num, p, s);
	}
      break;
    default:
      er_status = ER_TP_CANT_COERCE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, er_status, 2,
	      pr_type_name (type2), pr_type_name (DB_TYPE_DOUBLE));
      goto exit;
    }

exit:
  return er_status;
}

/*
 * db_mod_double () - take mod value of value1(double) with value2
 *   return: NO_ERROR, ER_FAILED
 *   result(out) : resultant db_value
 *   value1(in)  : double db_value
 *   value2(in)  : second db_value
 */
static int
db_mod_double (DB_VALUE * result, DB_VALUE * value1, DB_VALUE * value2)
{
#if !defined(NDEBUG)
  DB_TYPE type1;
#endif
  DB_TYPE type2;
  int i2;
  double d1, d2;
  DB_BIGINT bi2;
  int er_status = NO_ERROR;
  DB_VALUE cast_value2;

  assert (result != NULL && value1 != NULL && value2 != NULL);

  DB_MAKE_NULL (&cast_value2);

#if !defined(NDEBUG)
  type1 = DB_VALUE_DOMAIN_TYPE (value1);
  assert (type1 == DB_TYPE_DOUBLE);
#endif

  d1 = DB_GET_DOUBLE (value1);

  type2 = DB_VALUE_DOMAIN_TYPE (value2);
  switch (type2)
    {
    case DB_TYPE_INTEGER:
      i2 = DB_GET_INTEGER (value2);
      if (i2 == 0)
	{
	  DB_MAKE_DOUBLE (result, d1);
	}
      else
	{
	  DB_MAKE_DOUBLE (result, (double) fmod (d1, i2));
	}
      break;
    case DB_TYPE_BIGINT:
      bi2 = DB_GET_BIGINT (value2);
      if (bi2 == 0)
	{
	  DB_MAKE_DOUBLE (result, d1);
	}
      else
	{
	  DB_MAKE_DOUBLE (result, (double) fmod (d1, (double) bi2));
	}
      break;
    case DB_TYPE_VARCHAR:
      er_status = tp_value_str_cast_to_number (value2, &cast_value2, &type2);
      if (er_status != NO_ERROR)
	{
	  goto exit;
	}

      assert (type2 == DB_TYPE_DOUBLE);

      value2 = &cast_value2;

      /* fall through */

    case DB_TYPE_DOUBLE:
      d2 = DB_GET_DOUBLE (value2);
      if (d2 == 0)
	{
	  DB_MAKE_DOUBLE (result, d1);
	}
      else
	{
	  DB_MAKE_DOUBLE (result, (double) fmod (d1, d2));
	}
      break;
    case DB_TYPE_NUMERIC:
      numeric_coerce_num_to_double (db_locate_numeric (value2),
				    DB_VALUE_SCALE (value2), &d2);
      if (d2 == 0)
	{
	  DB_MAKE_DOUBLE (result, d1);
	}
      else
	{
	  DB_MAKE_DOUBLE (result, (double) fmod (d1, d2));
	}
      break;
    default:
      er_status = ER_TP_CANT_COERCE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, er_status, 2,
	      pr_type_name (type2), pr_type_name (DB_TYPE_DOUBLE));
      goto exit;
    }

exit:
  return er_status;
}

/*
 * db_mod_string () - take mod value of value1(string) with value2
 *   return: NO_ERROR, ER_FAILED
 *   result(out) : resultant db_value
 *   value1(in)  : string db_value
 *   value2(in)  : second db_value
 */
static int
db_mod_string (DB_VALUE * result, DB_VALUE * value1, DB_VALUE * value2)
{
  DB_TYPE type1;
  int er_status = NO_ERROR;
  DB_VALUE cast_value1;

  assert (result != NULL && value1 != NULL && value2 != NULL);

  DB_MAKE_NULL (&cast_value1);

#if !defined(NDEBUG)
  type1 = DB_VALUE_DOMAIN_TYPE (value1);
  assert (TP_IS_CHAR_TYPE (type1));
#endif

  er_status = tp_value_str_cast_to_number (value1, &cast_value1, &type1);
  if (er_status != NO_ERROR)
    {
      return er_status;
    }

  assert (type1 == DB_TYPE_DOUBLE);

  value1 = &cast_value1;

  return db_mod_double (result, value1, value2);
}

/*
 * db_mod_numeric () - take mod value of value1(numeric) with value2
 *   return: NO_ERROR, ER_FAILED
 *   result(out) : resultant db_value
 *   value1(in)  : numeric db_value
 *   value2(in)  : second db_value
 */
static int
db_mod_numeric (DB_VALUE * result, DB_VALUE * value1, DB_VALUE * value2)
{
#if !defined(NDEBUG)
  DB_TYPE type1;
#endif
  DB_TYPE type2;
  int i2;
  double d1, d2;
  DB_BIGINT bi2;
  double dtmp;
  unsigned char num[DB_NUMERIC_BUF_SIZE];
  int p, s;
  int er_status = NO_ERROR;
  DB_VALUE cast_value2;

  assert (result != NULL && value1 != NULL && value2 != NULL);

  DB_MAKE_NULL (&cast_value2);

#if !defined(NDEBUG)
  type1 = DB_VALUE_DOMAIN_TYPE (value1);
  assert (type1 == DB_TYPE_NUMERIC);
#endif

  numeric_coerce_num_to_double (db_locate_numeric (value1),
				DB_VALUE_SCALE (value1), &d1);

  type2 = DB_VALUE_DOMAIN_TYPE (value2);
  switch (type2)
    {
    case DB_TYPE_INTEGER:
      i2 = DB_GET_INTEGER (value2);
      if (i2 == 0)
	{
	  er_status = qdata_coerce_dbval_to_numeric (value1, result);
	}
      else
	{
	  dtmp = fmod (d1, i2);
	  (void) numeric_internal_double_to_num (dtmp,
						 DB_VALUE_SCALE (value1),
						 num, &p, &s);
	  er_status = DB_MAKE_NUMERIC (result, num, p, s);
	}
      break;
    case DB_TYPE_BIGINT:
      bi2 = DB_GET_BIGINT (value2);
      if (bi2 == 0)
	{
	  er_status = qdata_coerce_dbval_to_numeric (value1, result);
	}
      else
	{
	  dtmp = fmod (d1, (double) bi2);
	  (void) numeric_internal_double_to_num (dtmp,
						 DB_VALUE_SCALE (value1),
						 num, &p, &s);
	  er_status = DB_MAKE_NUMERIC (result, num, p, s);
	}
      break;
    case DB_TYPE_VARCHAR:
      er_status = tp_value_str_cast_to_number (value2, &cast_value2, &type2);
      if (er_status != NO_ERROR)
	{
	  goto exit;
	}

      assert (type2 == DB_TYPE_DOUBLE);

      value2 = &cast_value2;

      /* fall through */

    case DB_TYPE_DOUBLE:
      d2 = DB_GET_DOUBLE (value2);
      if (d2 == 0)
	{
	  DB_MAKE_DOUBLE (result, d1);
	}
      else
	{
	  DB_MAKE_DOUBLE (result, (double) fmod (d1, d2));
	}
      break;
    case DB_TYPE_NUMERIC:
      numeric_coerce_num_to_double (db_locate_numeric (value2),
				    DB_VALUE_SCALE (value2), &d2);
      if (d2 == 0)
	{
	  er_status = qdata_coerce_dbval_to_numeric (value1, result);
	}
      else
	{
	  dtmp = fmod (d1, d2);
	  (void) numeric_internal_double_to_num (dtmp,
						 MAX (DB_VALUE_SCALE
						      (value1),
						      DB_VALUE_SCALE
						      (value2)), num, &p, &s);
	  er_status = DB_MAKE_NUMERIC (result, num, p, s);
	}
      break;
    default:
      er_status = ER_TP_CANT_COERCE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, er_status, 2,
	      pr_type_name (type2), pr_type_name (DB_TYPE_DOUBLE));
      goto exit;
    }

exit:
  return er_status;
}

/*
 * db_mod_dbval () - take mod value of db_value
 *   return: NO_ERROR, ER_FAILED
 *   result(out) : resultant db_value
 *   value1(in)  : first db_value
 *   value2(in)  : second db_value
 */
int
db_mod_dbval (DB_VALUE * result, DB_VALUE * value1, DB_VALUE * value2)
{
  DB_TYPE type1;

  assert (result != value1);
  assert (result != value2);

  if (DB_IS_NULL (value1) || DB_IS_NULL (value2))
    {
      return NO_ERROR;
    }

  type1 = DB_VALUE_DOMAIN_TYPE (value1);
  switch (type1)
    {
    case DB_TYPE_INTEGER:
      return db_mod_int (result, value1, value2);

    case DB_TYPE_BIGINT:
      return db_mod_bigint (result, value1, value2);

    case DB_TYPE_VARCHAR:
      return db_mod_string (result, value1, value2);

    case DB_TYPE_DOUBLE:
      return db_mod_double (result, value1, value2);

    case DB_TYPE_NUMERIC:
      return db_mod_numeric (result, value1, value2);

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
	      pr_type_name (type1), pr_type_name (DB_TYPE_DOUBLE));
      return ER_TP_CANT_COERCE;
    }
}

/*
 * round_double ()
 *   return: error code
 *   num(in)    :
 *   y(in):
 *   result(out): num rounded to y places to the right of the decimal point
 */
static int
round_double (double num, double y, double *result)
{
  int error_status = NO_ERROR;
  /*
   * Under high optimization level, some optimizers (e.g, gcc -O3 on linux)
   * generates a wrong result without "volatile".
   */
  volatile double scale_down, num_scale_up;

  if (num == 0)
    {
      *result = 0;
      return NO_ERROR;
    }

  scale_down = pow (10, -y);
  num_scale_up = num / scale_down;
  if (!FINITE (num_scale_up))
    {
      error_status = ER_IT_DATA_OVERFLOW;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 1,
	      tp_Double_domain.type->name);
    }
  else
    {
      if (num_scale_up > 0)
	{
	  *result = floor (num_scale_up + 0.5) * scale_down;
	}
      else
	{
	  *result = ceil (num_scale_up - 0.5) * scale_down;
	}
    }

  return error_status;
}

/*
 * db_round_dbval () - returns value1 rounded to value2 places right of
 *                     the decimal point
 *   return: NO_ERROR, ER_FAILED
 *   result(out): resultant db_value
 *   value1(in) : first db_value
 *   value2(in) : second db_value
 */
int
db_round_dbval (DB_VALUE * result, DB_VALUE * value1, DB_VALUE * value2)
{
  int er_status = NO_ERROR;
  DB_TYPE type1, type2;
  int i1;
  double d1, d2 = 0.0;
  DB_BIGINT bi1, bi2, bi_tmp;
  double dtmp;
  unsigned char num[DB_NUMERIC_BUF_SIZE];
  char num_string[(2 * DB_MAX_NUMERIC_PRECISION) + 4];
  char *ptr, *end;
  int need_round = 0;
  int p, s;
  DB_VALUE cast_value, cast_format;
  DB_DOMAIN *domain;

  assert (result != value1);
  assert (result != value2);

  DB_MAKE_NULL (&cast_value);
  DB_MAKE_NULL (&cast_format);

  if (DB_IS_NULL (value1) || DB_IS_NULL (value2))
    {
      DB_MAKE_NULL (result);
      return NO_ERROR;
    }

  type1 = DB_VALUE_DOMAIN_TYPE (value1);
  type2 = DB_VALUE_DOMAIN_TYPE (value2);

  /* cast value1 to double */
  if (!TP_IS_NUMERIC_TYPE (type1))
    {
      type1 = DB_TYPE_UNKNOWN;
      /* try type double */
      domain = tp_domain_resolve_default (DB_TYPE_DOUBLE);
      if (tp_value_coerce (value1, &cast_value, domain) != DOMAIN_COMPATIBLE)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_TP_CANT_COERCE, 2,
		  pr_type_name (DB_VALUE_DOMAIN_TYPE (value1)),
		  pr_type_name (DB_TYPE_DOUBLE));
	  return ER_TP_CANT_COERCE;
	}
      type1 = DB_TYPE_DOUBLE;
      value1 = &cast_value;
    }

  /* cast value2 to integer */
  if (type2 != DB_TYPE_INTEGER)
    {
      type2 = DB_TYPE_UNKNOWN;
      /* try type int */
      domain = tp_domain_resolve_default (DB_TYPE_INTEGER);
      if (tp_value_coerce (value2, &cast_format, domain) != DOMAIN_COMPATIBLE)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_TP_CANT_COERCE, 2,
		  pr_type_name (DB_VALUE_DOMAIN_TYPE (value2)),
		  pr_type_name (DB_TYPE_INTEGER));
	  return ER_TP_CANT_COERCE;
	}
      type2 = DB_TYPE_INTEGER;
      value2 = &cast_format;
    }

  /* get value2 */
  d2 = (double) DB_GET_INTEGER (value2);

  /* round double */
  switch (type1)
    {
    case DB_TYPE_INTEGER:
      i1 = DB_GET_INTEGER (value1);
      er_status = round_double (i1, d2, &dtmp);
      if (er_status != NO_ERROR)
	{
	  return er_status;
	}
      DB_MAKE_INT (result, (int) dtmp);
      break;

    case DB_TYPE_BIGINT:
      bi1 = DB_GET_BIGINT (value1);
      er_status = round_double ((double) bi1, d2, &dtmp);
      if (er_status != NO_ERROR)
	{
	  return er_status;
	}
      bi_tmp = (DB_BIGINT) dtmp;
      DB_MAKE_BIGINT (result, bi_tmp);
      break;

    case DB_TYPE_DOUBLE:
      d1 = DB_GET_DOUBLE (value1);
      er_status = round_double (d1, d2, &dtmp);
      if (er_status != NO_ERROR)
	{
	  return er_status;
	}
      DB_MAKE_DOUBLE (result, (double) dtmp);
      break;

    case DB_TYPE_NUMERIC:
      memset (num_string, 0, sizeof (num_string));
      numeric_coerce_num_to_dec_str (db_locate_numeric (value1), num_string);
      p = DB_VALUE_PRECISION (value1);
      s = DB_VALUE_SCALE (value1);
      end = num_string + strlen (num_string);

      bi2 = DB_GET_INTEGER (value2);
      ptr = end - s + bi2;

      if (end < ptr)
	{			/* no need to round, return as it is */
	  *result = *value1;
	  break;
	}
      else if (ptr < num_string)
	{			/* return zero */
	  memset (num_string, 0, sizeof (num_string));
	}
      else
	{
	  if (*ptr >= '5')
	    {
	      need_round = 1;
	    }
	  while (ptr < end)
	    {
	      *ptr++ = '0';
	    }
	  if (need_round)
	    {
	      /* round up */
	      int done = 0;

	      for (ptr = end - s + bi2 - 1; ptr >= num_string && !done; ptr--)
		{
		  if (*ptr == '9')
		    {
		      *ptr = '0';
		    }
		  else
		    {
		      *ptr += 1;
		      done = 1;
		    }
		}

	      for (ptr = num_string; ptr < end; ptr++)
		{
		  if ('1' <= *ptr && *ptr <= '9')
		    {
		      if (strlen (ptr) > DB_MAX_NUMERIC_PRECISION)
			{
			  /* overflow happened during round up */
			  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
				  ER_NUM_OVERFLOW, 0);
			  return ER_NUM_OVERFLOW;
			}
		      break;
		    }
		}
	    }
	}

      numeric_coerce_dec_str_to_num (num_string, num);
      er_status = DB_MAKE_NUMERIC (result, num, p, s);
      break;

    default:
      assert (false);		/* is impossible */
      er_status = ER_TP_CANT_COERCE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, er_status, 2,
	      pr_type_name (type1), pr_type_name (DB_TYPE_DOUBLE));
      break;
    }

  return er_status;
}

/*
 * db_log_dbval () -
 *   return: NO_ERROR, ER_FAILED
 *   result(out): resultant db_value
 *   value1(in) : first db_value
 *   value2(in) : second db_value
 */
int
db_log_dbval (DB_VALUE * result, DB_VALUE * value1, DB_VALUE * value2)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_value1, tmp_value2;
  DB_TYPE type1, type2;
  int i1, i2;
  double d1, d2;
  DB_BIGINT bi1, bi2;
  double dtmp = 0.0;

  assert (result != value1);
  assert (result != value2);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (2, &tmp_value1, &tmp_value2);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (2, value1, value2))
    {
      DB_MAKE_NULL (result);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  type1 = DB_VALUE_DOMAIN_TYPE (value1);
  if (!TP_IS_NUMERIC_TYPE (type1))
    {
      value1 =
	db_value_cast_arg (value1, &tmp_value1, DB_TYPE_DOUBLE,
			   &error_status);
    }

  type2 = DB_VALUE_DOMAIN_TYPE (value2);
  if (!TP_IS_NUMERIC_TYPE (type2))
    {
      value2 =
	db_value_cast_arg (value2, &tmp_value2, DB_TYPE_DOUBLE,
			   &error_status);
    }

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  type1 = DB_VALUE_DOMAIN_TYPE (value1);
  type2 = DB_VALUE_DOMAIN_TYPE (value2);

  switch (type1)
    {
    case DB_TYPE_BIGINT:
      bi1 = DB_GET_BIGINT (value1);
      if (bi1 <= 1)
	{
	  error_status = ER_QPROC_FUNCTION_ARG_ERROR;
	  goto exit_on_error;
	}

      switch (type2)
	{
	case DB_TYPE_INTEGER:
	  i2 = DB_GET_INTEGER (value2);
	  if (i2 <= 0)
	    {
	      error_status = ER_QPROC_FUNCTION_ARG_ERROR;
	      goto exit_on_error;
	    }
	  dtmp = log10 ((double) i2) / log10 ((double) bi1);
	  break;
	case DB_TYPE_BIGINT:
	  bi2 = DB_GET_BIGINT (value2);
	  if (bi2 <= 0)
	    {
	      error_status = ER_QPROC_FUNCTION_ARG_ERROR;
	      goto exit_on_error;
	    }
	  dtmp = log10 ((double) bi2) / log10 ((double) bi1);
	  break;
	case DB_TYPE_DOUBLE:
	  d2 = DB_GET_DOUBLE (value2);
	  if (d2 <= 0)
	    {
	      error_status = ER_QPROC_FUNCTION_ARG_ERROR;
	      goto exit_on_error;
	    }
	  dtmp = log10 ((double) d2) / log10 ((double) bi1);
	  break;
	case DB_TYPE_NUMERIC:
	  numeric_coerce_num_to_double (db_locate_numeric (value2),
					DB_VALUE_SCALE (value2), &d2);
	  if (d2 <= 0)
	    {
	      error_status = ER_QPROC_FUNCTION_ARG_ERROR;
	      goto exit_on_error;
	    }
	  dtmp = log10 ((double) d2) / log10 ((double) bi1);
	  break;
	default:
	  assert (false);
	  error_status = ER_TP_CANT_COERCE;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
		  pr_type_name (type2), pr_type_name (DB_TYPE_DOUBLE));
	  goto exit_on_error;
	  break;
	}
      break;

    case DB_TYPE_INTEGER:
      i1 = DB_GET_INTEGER (value1);
      if (i1 <= 1)
	{
	  error_status = ER_QPROC_FUNCTION_ARG_ERROR;
	  goto exit_on_error;
	}

      switch (type2)
	{
	case DB_TYPE_INTEGER:
	  i2 = DB_GET_INTEGER (value2);
	  if (i2 <= 0)
	    {
	      error_status = ER_QPROC_FUNCTION_ARG_ERROR;
	      goto exit_on_error;
	    }
	  dtmp = log10 ((double) i2) / log10 ((double) i1);
	  break;
	case DB_TYPE_BIGINT:
	  bi2 = DB_GET_BIGINT (value2);
	  if (bi2 <= 0)
	    {
	      error_status = ER_QPROC_FUNCTION_ARG_ERROR;
	      goto exit_on_error;
	    }
	  dtmp = log10 ((double) bi2) / log10 ((double) i1);
	  break;
	case DB_TYPE_DOUBLE:
	  d2 = DB_GET_DOUBLE (value2);
	  if (d2 <= 0)
	    {
	      error_status = ER_QPROC_FUNCTION_ARG_ERROR;
	      goto exit_on_error;
	    }
	  dtmp = log10 (d2) / log10 ((double) i1);
	  break;
	case DB_TYPE_NUMERIC:
	  numeric_coerce_num_to_double (db_locate_numeric (value2),
					DB_VALUE_SCALE (value2), &d2);
	  if (d2 <= 0)
	    {
	      error_status = ER_QPROC_FUNCTION_ARG_ERROR;
	      goto exit_on_error;
	    }
	  dtmp = log10 (d2) / log10 ((double) i1);
	  break;
	default:
	  assert (false);
	  error_status = ER_TP_CANT_COERCE;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
		  pr_type_name (type2), pr_type_name (DB_TYPE_DOUBLE));
	  goto exit_on_error;
	  break;
	}
      break;

    case DB_TYPE_DOUBLE:
      d1 = DB_GET_DOUBLE (value1);
      if (d1 <= 1)
	{
	  error_status = ER_QPROC_FUNCTION_ARG_ERROR;
	  goto exit_on_error;
	}

      switch (type2)
	{
	case DB_TYPE_INTEGER:
	  i2 = DB_GET_INTEGER (value2);
	  if (i2 <= 0)
	    {
	      error_status = ER_QPROC_FUNCTION_ARG_ERROR;
	      goto exit_on_error;
	    }
	  dtmp = log10 ((double) i2) / log10 (d1);
	  break;
	case DB_TYPE_BIGINT:
	  bi2 = DB_GET_BIGINT (value2);
	  if (bi2 <= 0)
	    {
	      error_status = ER_QPROC_FUNCTION_ARG_ERROR;
	      goto exit_on_error;
	    }
	  dtmp = log10 ((double) bi2) / log10 (d1);
	  break;
	case DB_TYPE_DOUBLE:
	  d2 = DB_GET_DOUBLE (value2);
	  if (d2 <= 0)
	    {
	      error_status = ER_QPROC_FUNCTION_ARG_ERROR;
	      goto exit_on_error;
	    }
	  dtmp = log10 (d2) / log10 (d1);
	  break;
	case DB_TYPE_NUMERIC:
	  numeric_coerce_num_to_double (db_locate_numeric (value2),
					DB_VALUE_SCALE (value2), &d2);
	  if (d2 <= 0)
	    {
	      error_status = ER_QPROC_FUNCTION_ARG_ERROR;
	      goto exit_on_error;
	    }
	  dtmp = log10 (d2) / log10 (d1);
	  break;
	default:
	  assert (false);
	  error_status = ER_TP_CANT_COERCE;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
		  pr_type_name (type2), pr_type_name (DB_TYPE_DOUBLE));
	  goto exit_on_error;
	  break;
	}
      break;

    case DB_TYPE_NUMERIC:
      numeric_coerce_num_to_double (db_locate_numeric (value1),
				    DB_VALUE_SCALE (value1), &d1);
      if (d1 <= 1)
	{
	  error_status = ER_QPROC_FUNCTION_ARG_ERROR;
	  goto exit_on_error;
	}

      switch (type2)
	{
	case DB_TYPE_INTEGER:
	  i2 = DB_GET_INTEGER (value2);
	  if (i2 <= 0)
	    {
	      error_status = ER_QPROC_FUNCTION_ARG_ERROR;
	      goto exit_on_error;
	    }
	  dtmp = log10 ((double) i2) / log10 (d1);
	  break;
	case DB_TYPE_BIGINT:
	  bi2 = DB_GET_BIGINT (value2);
	  if (bi2 <= 0)
	    {
	      error_status = ER_QPROC_FUNCTION_ARG_ERROR;
	      goto exit_on_error;
	    }
	  dtmp = log10 ((double) bi2) / log10 (d1);
	  break;
	case DB_TYPE_DOUBLE:
	  d2 = DB_GET_DOUBLE (value2);
	  if (d2 <= 0)
	    {
	      error_status = ER_QPROC_FUNCTION_ARG_ERROR;
	      goto exit_on_error;
	    }
	  dtmp = log10 (d2) / log10 (d1);
	  break;
	case DB_TYPE_NUMERIC:
	  numeric_coerce_num_to_double (db_locate_numeric (value2),
					DB_VALUE_SCALE (value2), &d2);
	  if (d2 <= 0)
	    {
	      error_status = ER_QPROC_FUNCTION_ARG_ERROR;
	      goto exit_on_error;
	    }
	  dtmp = log10 (d2) / log10 (d1);
	  break;
	default:
	  assert (false);
	  error_status = ER_TP_CANT_COERCE;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
		  pr_type_name (type2), pr_type_name (DB_TYPE_DOUBLE));
	  goto exit_on_error;
	  break;
	}
      break;

    default:
      assert (false);
      error_status = ER_TP_CANT_COERCE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
	      pr_type_name (type1), pr_type_name (DB_TYPE_DOUBLE));
      goto exit_on_error;
      break;
    }

  DB_MAKE_DOUBLE (result, dtmp);

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_value1, &tmp_value2);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (error_status == ER_QPROC_FUNCTION_ARG_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_FUNCTION_ARG_ERROR,
	      1, "log()");
    }
  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (result);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_value1, &tmp_value2);

  return error_status;
}

/*
 * truncate_double ()
 *   return: num truncated to integer places
 *   num(in)    :
 *   integer(in):
 */
static double
truncate_double (double num, double integer)
{
  /*
   * Under high optimization level, some optimizers (e.g, gcc -O3 on linux)
   * generates a wrong result without "volatile".
   */
  double scale_up, num_scale_up, result;

  if (num == 0)
    {
      return num;
    }

  scale_up = pow (10, integer);
  num_scale_up = num * scale_up;
  if (num > 0)
    {
      result = floor (num_scale_up);
    }
  else
    {
      result = ceil (num_scale_up);
    }

  if (num_scale_up == result)	/* no need to calculate, return as it is */
    {
      result = num;		/* to avoid possible truncation */
    }
  else
    {
      result = result / scale_up;
    }

  return result;
}

/*
 * truncate_bigint ()
 *   return: num truncated to integer places
 *   num(in)    :
 *   integer(in):
 */
static DB_BIGINT
truncate_bigint (DB_BIGINT num, DB_BIGINT integer)
{
  if (num == 0 || integer >= 0)
    {
      return num;
    }

  integer = (DB_BIGINT) pow (10, (double) -integer);
  num -= num % integer;

  return num;
}

/*
 * db_trunc_dbval () - return dbval1 truncated to dbval2 decimal places
 *
 * Used to truncate number
 *            trunc(PT_GENERIC_TYPE_NUMBER, PT_TYPE_INTEGER)
 *
 *   return: NO_ERROR, ER_FAILED
 *   result(out): resultant db_value
 *   value1(in) : first db_value
 *   value2(in) : second db_value
 */
int
db_trunc_dbval (DB_VALUE * result, DB_VALUE * value1, DB_VALUE * value2)
{
  DB_TYPE type1, type2;
  DB_BIGINT bi2;
  double dtmp;
  DB_VALUE cast_value, cast_format;
  int er_status = NO_ERROR;
  DB_DOMAIN *domain;
  TP_DOMAIN_STATUS cast_status;

  assert (result != value1);
  assert (result != value2);

  DB_MAKE_NULL (&cast_value);
  DB_MAKE_NULL (&cast_format);

  if (DB_IS_NULL (value1) || DB_IS_NULL (value2))
    {
      DB_MAKE_NULL (result);
      return NO_ERROR;
    }

  type1 = DB_VALUE_DOMAIN_TYPE (value1);
  type2 = DB_VALUE_DOMAIN_TYPE (value2);
  if (type2 != DB_TYPE_INTEGER
      && type2 != DB_TYPE_BIGINT && type2 != DB_TYPE_NUMERIC
      && type2 != DB_TYPE_DOUBLE && !QSTR_IS_CHAR (type2))
    {
      er_status = ER_TP_CANT_COERCE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, er_status, 2,
	      pr_type_name (type2), pr_type_name (DB_TYPE_DOUBLE));

      goto end;
    }

  /* convert value1 to double when it's a string */
  switch (type1)
    {
    case DB_TYPE_INTEGER:
    case DB_TYPE_BIGINT:
    case DB_TYPE_NUMERIC:
    case DB_TYPE_DOUBLE:
      break;
    default:			/* convert to double */
      type1 = DB_TYPE_UNKNOWN;

      /* try type double */
      DB_MAKE_NULL (&cast_value);
      domain = tp_domain_resolve_default (DB_TYPE_DOUBLE);
      cast_status = tp_value_coerce (value1, &cast_value, domain);
      if (cast_status == DOMAIN_COMPATIBLE)
	{
	  type1 = DB_TYPE_DOUBLE;
	  value1 = &cast_value;
	}

      /* convert fail */
      if (type1 == DB_TYPE_UNKNOWN)
	{
	  er_status = ER_TP_CANT_COERCE;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, er_status, 2,
		  pr_type_name (type1), pr_type_name (DB_TYPE_DOUBLE));

	  goto end;
	}
    }

  /* translate default fmt */
  if (pr_is_string_type (type2)
      && strcasecmp (DB_PULL_STRING (value2), "default") == 0)
    {
      if (type1 == DB_TYPE_DATE || type1 == DB_TYPE_DATETIME)
	{
	  DB_MAKE_STRING (&cast_format, "dd");
	}
      else
	{
	  DB_MAKE_INT (&cast_format, 0);
	  type2 = DB_TYPE_INTEGER;
	}

      value2 = &cast_format;
    }

  if (type2 == DB_TYPE_INTEGER)
    {
      bi2 = DB_GET_INTEGER (value2);
    }
  else if (type2 == DB_TYPE_BIGINT)
    {
      bi2 = DB_GET_BIGINT (value2);
    }
  else if (type1 != DB_TYPE_DATE && type1 != DB_TYPE_DATETIME)
    {
      domain = tp_domain_resolve_default (DB_TYPE_BIGINT);
      cast_status = tp_value_coerce (value2, &cast_format, domain);
      if (cast_status != DOMAIN_COMPATIBLE)
	{
	  er_status = ER_QSTR_INVALID_FORMAT;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, er_status, 0);

	  goto end;
	}

      bi2 = DB_GET_BIGINT (&cast_format);
    }
  else
    {
      bi2 = 0;			/* to make compiler be silent */
    }

  switch (type1)
    {
    case DB_TYPE_INTEGER:
      {
	int i1;

	i1 = DB_GET_INTEGER (value1);
	dtmp = truncate_double (i1, (double) bi2);
	DB_MAKE_INT (result, (int) dtmp);
      }
      break;
    case DB_TYPE_BIGINT:
      {
	DB_BIGINT bi1;

	bi1 = DB_GET_BIGINT (value1);
	bi1 = truncate_bigint (bi1, bi2);
	DB_MAKE_BIGINT (result, bi1);
      }
      break;

    case DB_TYPE_DOUBLE:
      {
	double d1;

	d1 = DB_GET_DOUBLE (value1);
	dtmp = truncate_double (d1, (double) bi2);
	DB_MAKE_DOUBLE (result, (double) dtmp);
      }
      break;
    case DB_TYPE_NUMERIC:
      {
	unsigned char num[DB_NUMERIC_BUF_SIZE];
	char num_string[(2 * DB_MAX_NUMERIC_PRECISION) + 4];
	char *ptr, *end;
	int p, s;

	memset (num_string, 0, sizeof (num_string));
	numeric_coerce_num_to_dec_str (db_locate_numeric (value1),
				       num_string);
	p = DB_VALUE_PRECISION (value1);
	s = DB_VALUE_SCALE (value1);
	end = num_string + strlen (num_string);
	ptr = end - s + bi2;

	if (end < ptr)
	  {
	    /* no need to round, return as it is */
	    *result = *value1;
	    break;
	  }
	else if (ptr < num_string)
	  {
	    /* return zero */
	    memset (num_string, 0, sizeof (num_string));
	  }
	else
	  {
	    while (ptr < end)
	      {
		*ptr++ = '0';
	      }
	  }
	numeric_coerce_dec_str_to_num (num_string, num);
	er_status = DB_MAKE_NUMERIC (result, num, p, s);
      }
      break;
    default:
      er_status = ER_TP_CANT_COERCE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, er_status, 2,
	      pr_type_name (type1), pr_type_name (DB_TYPE_DOUBLE));
      goto end;
    }

end:
  pr_clear_value (&cast_value);
  pr_clear_value (&cast_format);

  return er_status;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * db_random_dbval () - take random integer
 *   return: NO_ERROR
 *   result(out) : resultant db_value
 */
int
db_random_dbval (DB_VALUE * result)
{
  DB_MAKE_INTEGER (result, lrand48 ());

  return NO_ERROR;
}

/*
 * db_drandom_dbval () - take random double
 *   return: NO_ERROR
 *   result(out) : resultant db_value
 */
int
db_drandom_dbval (DB_VALUE * result)
{
  DB_MAKE_DOUBLE (result, drand48 ());

  return NO_ERROR;
}
#endif

/*
 * get_number_dbval_as_double () -
 *   return: NO_ERROR/error code
 *   d(out) : double
 *   value(in) : input db_value
 */
static int
get_number_dbval_as_double (double *d, const DB_VALUE * value)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_value;
  DB_TYPE value_type;
  int i;
  double dtmp;
  DB_BIGINT bi;

  assert (value != NULL);
  assert (!DB_IS_NULL (value));	/* should not reach here */

  value_type = DB_VALUE_DOMAIN_TYPE (value);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (1, &tmp_value);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (1, value))
    {
      assert (false);
      goto exit_on_error;	/* should not reach here */
    }

  /* arg cast type **********************************************************
   */
  if (!TP_IS_NUMERIC_TYPE (value_type))
    {
      value =
	db_value_cast_arg (value, &tmp_value, DB_TYPE_DOUBLE, &error_status);
    }

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  value_type = DB_VALUE_DOMAIN_TYPE (value);

  switch (value_type)
    {
    case DB_TYPE_INTEGER:
      i = DB_GET_INTEGER (value);
      dtmp = (double) i;
      break;
    case DB_TYPE_BIGINT:
      bi = DB_GET_BIGINT (value);
      dtmp = (double) bi;
      break;
    case DB_TYPE_DOUBLE:
      dtmp = DB_GET_DOUBLE (value);
      break;
    case DB_TYPE_NUMERIC:
      numeric_coerce_num_to_double ((DB_C_NUMERIC) db_locate_numeric (value),
				    DB_VALUE_SCALE (value), &dtmp);
      break;
    default:
      assert (false);
      error_status = ER_TP_CANT_COERCE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
	      pr_type_name (value_type), pr_type_name (DB_TYPE_DOUBLE));
      goto exit_on_error;
      break;
    }

  *d = dtmp;

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
  db_value_clear_nargs (1, &tmp_value);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_value);

  return error_status;
}

/*
 * db_cos_dbval () - computes cosine value of db_value
 *   return: NO_ERROR
 *   result(out): resultant db_value
 *   value(in) : input db_value
 */
int
db_cos_dbval (DB_VALUE * result, DB_VALUE * value)
{
  DB_TYPE type;
  int err;
  double dtmp;

  assert (result != value);

  type = DB_VALUE_DOMAIN_TYPE (value);
  if (type == DB_TYPE_NULL || DB_IS_NULL (value))
    {
      DB_MAKE_NULL (result);
      return NO_ERROR;
    }

  err = get_number_dbval_as_double (&dtmp, value);
  if (err != NO_ERROR)
    {
      return err;
    }

  dtmp = cos (dtmp);

  DB_MAKE_DOUBLE (result, dtmp);
  return NO_ERROR;
}

/*
 * db_sin_dbval () - computes sine value of db_value
 *   return: NO_ERROR
 *   result(out): resultant db_value
 *   value(in) : input db_value
 */
int
db_sin_dbval (DB_VALUE * result, DB_VALUE * value)
{
  DB_TYPE type;
  int err;
  double dtmp;

  assert (result != value);

  type = DB_VALUE_DOMAIN_TYPE (value);
  if (type == DB_TYPE_NULL || DB_IS_NULL (value))
    {
      DB_MAKE_NULL (result);
      return NO_ERROR;
    }

  err = get_number_dbval_as_double (&dtmp, value);
  if (err != NO_ERROR)
    {
      return err;
    }

  dtmp = sin (dtmp);

  DB_MAKE_DOUBLE (result, dtmp);
  return NO_ERROR;
}

/*
 * db_tan_dbval () - computes tangent value of db_value
 *   return: NO_ERROR
 *   result(out): resultant db_value
 *   value(in) : input db_value
 */
int
db_tan_dbval (DB_VALUE * result, DB_VALUE * value)
{
  DB_TYPE type;
  int err;
  double dtmp;

  assert (result != value);

  type = DB_VALUE_DOMAIN_TYPE (value);
  if (type == DB_TYPE_NULL || DB_IS_NULL (value))
    {
      DB_MAKE_NULL (result);
      return NO_ERROR;
    }

  err = get_number_dbval_as_double (&dtmp, value);
  if (err != NO_ERROR)
    {
      return err;
    }

  dtmp = tan (dtmp);

  DB_MAKE_DOUBLE (result, dtmp);
  return NO_ERROR;
}

/*
 * db_cot_dbval () - computes cotangent value of db_value
 *   return: NO_ERROR
 *   result(out): resultant db_value
 *   value(in) : input db_value
 */
int
db_cot_dbval (DB_VALUE * result, DB_VALUE * value)
{
  DB_TYPE type;
  int err;
  double dtmp;

  assert (result != value);

  type = DB_VALUE_DOMAIN_TYPE (value);
  if (type == DB_TYPE_NULL || DB_IS_NULL (value))
    {
      DB_MAKE_NULL (result);
      return NO_ERROR;
    }

  err = get_number_dbval_as_double (&dtmp, value);
  if (err != NO_ERROR)
    {
      return err;
    }

  if (dtmp == 0)
    {
      DB_MAKE_NULL (result);
    }
  else
    {
      dtmp = 1 / tan (dtmp);
      DB_MAKE_DOUBLE (result, dtmp);
    }

  return NO_ERROR;
}

/*
 * db_acos_dbval () - computes arc cosine value of db_value
 *   return: NO_ERROR
 *   result(out): resultant db_value
 *   value(in) : input db_value
 */
int
db_acos_dbval (DB_VALUE * result, DB_VALUE * value)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_value;
  DB_TYPE type;
  double dtmp;

  assert (result != value);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (1, &tmp_value);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (1, value))
    {
      DB_MAKE_NULL (result);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  type = DB_VALUE_DOMAIN_TYPE (value);

  if (!TP_IS_NUMERIC_TYPE (type))
    {
      value =
	db_value_cast_arg (value, &tmp_value, DB_TYPE_DOUBLE, &error_status);
    }

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  error_status = get_number_dbval_as_double (&dtmp, value);
  if (error_status != NO_ERROR)
    {
      return error_status;
    }

  if (dtmp < -1 || dtmp > 1)
    {
      error_status = ER_QPROC_FUNCTION_ARG_ERROR;
      goto exit_on_error;
    }

  dtmp = acos (dtmp);

  DB_MAKE_DOUBLE (result, dtmp);

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_value);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (error_status == ER_QPROC_FUNCTION_ARG_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_FUNCTION_ARG_ERROR,
	      1, "acos()");
    }
  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (result);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_value);

  return error_status;
}

/*
 * db_asin_dbval () - computes arc sine value of db_value
 *   return: NO_ERROR
 *   result(out): resultant db_value
 *   value(in) : input db_value
 */
int
db_asin_dbval (DB_VALUE * result, DB_VALUE * value)
{
  DB_TYPE type;
  int err;
  double dtmp;

  assert (result != value);

  type = DB_VALUE_DOMAIN_TYPE (value);
  if (type == DB_TYPE_NULL || DB_IS_NULL (value))
    {
      DB_MAKE_NULL (result);
      return NO_ERROR;
    }

  err = get_number_dbval_as_double (&dtmp, value);
  if (err != NO_ERROR)
    {
      return err;
    }

  if (dtmp < -1 || dtmp > 1)
    {
      goto error;
    }

  dtmp = asin (dtmp);

  DB_MAKE_DOUBLE (result, dtmp);
  return NO_ERROR;

error:
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_FUNCTION_ARG_ERROR,
	  1, "asin()");
  return ER_QPROC_FUNCTION_ARG_ERROR;
}

/*
 * db_atan_dbval () - computes arc tangent value of value2 / value
 *   return: NO_ERROR
 *   result(out): resultant db_value
 *   value(in) : input db_value
 */
int
db_atan_dbval (DB_VALUE * result, DB_VALUE * value)
{
  DB_TYPE type;
  int err;
  double dtmp;

  assert (result != value);

  type = DB_VALUE_DOMAIN_TYPE (value);
  if (type == DB_TYPE_NULL || DB_IS_NULL (value))
    {
      DB_MAKE_NULL (result);
      return NO_ERROR;
    }

  err = get_number_dbval_as_double (&dtmp, value);
  if (err != NO_ERROR)
    {
      return err;
    }

  dtmp = atan (dtmp);

  DB_MAKE_DOUBLE (result, dtmp);
  return NO_ERROR;
}

/*
 * db_atan2_dbval () - computes arc tangent value of value2 / value
 *   return: NO_ERROR
 *   result(out): resultant db_value
 *   value(in) : input db_value
 *   value2(in) : second input db_value
 *  OBS: this should have been done like db_power_dbval, i.e. switch in switch
 *	  but this yields in very much code so we prefered to get all values
 *	  separated and then convert all to double. Then just one call of atan2.
 */
int
db_atan2_dbval (DB_VALUE * result, DB_VALUE * value, DB_VALUE * value2)
{
  DB_TYPE type, type2;
  int err;
  double d, d2, dtmp;

  assert (result != value);
  assert (result != value2);

  /* arg1 */
  type = DB_VALUE_DOMAIN_TYPE (value);
  if (type == DB_TYPE_NULL || DB_IS_NULL (value))
    {
      DB_MAKE_NULL (result);
      return NO_ERROR;
    }

  err = get_number_dbval_as_double (&d, value);
  if (err != NO_ERROR)
    {
      return err;
    }

  /* arg2 */
  type2 = DB_VALUE_DOMAIN_TYPE (value2);
  if (type2 == DB_TYPE_NULL || DB_IS_NULL (value2))
    {
      DB_MAKE_NULL (result);
      return NO_ERROR;
    }

  err = get_number_dbval_as_double (&d2, value2);
  if (err != NO_ERROR)
    {
      return err;
    }

  /* function call, all is double type */
  dtmp = atan2 (d, d2);

  DB_MAKE_DOUBLE (result, dtmp);
  return NO_ERROR;
}

/*
 * db_degrees_dbval () - computes radians from value in degrees
 *   return: NO_ERROR
 *   result(out): resultant db_value
 *   value(in) : input db_value
 */
int
db_degrees_dbval (DB_VALUE * result, DB_VALUE * value)
{
  DB_TYPE type;
  int err;
  double dtmp;

  assert (result != value);

  type = DB_VALUE_DOMAIN_TYPE (value);
  if (type == DB_TYPE_NULL || DB_IS_NULL (value))
    {
      DB_MAKE_NULL (result);
      return NO_ERROR;
    }

  err = get_number_dbval_as_double (&dtmp, value);
  if (err != NO_ERROR)
    {
      return err;
    }

  dtmp = dtmp * (double) 57.295779513082320876798154814105;	/* 180 / PI */
  DB_MAKE_DOUBLE (result, dtmp);

  return NO_ERROR;
}

/*
 * db_radians_dbval () - converts degrees in value to radians
 *   return: NO_ERROR
 *   result(out): resultant db_value
 *   value(in) : input db_value
 */
int
db_radians_dbval (DB_VALUE * result, DB_VALUE * value)
{
  DB_TYPE type;
  int err;
  double dtmp;

  assert (result != value);

  type = DB_VALUE_DOMAIN_TYPE (value);
  if (type == DB_TYPE_NULL || DB_IS_NULL (value))
    {
      DB_MAKE_NULL (result);
      return NO_ERROR;
    }

  err = get_number_dbval_as_double (&dtmp, value);
  if (err != NO_ERROR)
    {
      return err;
    }

  dtmp = dtmp * (double) 0.017453292519943295769236907684886;	/* PI / 180 */
  DB_MAKE_DOUBLE (result, dtmp);

  return NO_ERROR;
}

/*
 * db_log_generic_dbval () - computes log of db_value in base
 *   return: NO_ERROR
 *   result(out): resultant db_value
 *   value(in) : input db_value
 */
int
db_log_generic_dbval (DB_VALUE * result, DB_VALUE * value, long b)
{
  DB_TYPE type;
  int err;
  double dtmp;
  double base = ((b == -1) ? (2.7182818284590452353) : (double) b);

  assert (result != value);

  type = DB_VALUE_DOMAIN_TYPE (value);
  if (type == DB_TYPE_NULL || DB_IS_NULL (value))
    {
      DB_MAKE_NULL (result);
      return NO_ERROR;
    }

  err = get_number_dbval_as_double (&dtmp, value);
  if (err != NO_ERROR)
    {
      return err;
    }

  if (dtmp > 0)
    {
      dtmp = log10 (dtmp) / log10 (base);
      DB_MAKE_DOUBLE (result, dtmp);
    }
  else
    {
      const char *log_func;

      switch (b)
	{
	case -1:
	  log_func = "ln()";
	  break;
	case 2:
	  log_func = "log2()";
	  break;
	case 10:
	  log_func = "log10()";
	  break;
	default:
	  assert (0);
	  log_func = "unknown";
	  break;
	}

      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_FUNCTION_ARG_ERROR,
	      1, log_func);
      return ER_QPROC_FUNCTION_ARG_ERROR;
    }

  return NO_ERROR;
}

/*
 * db_bit_count_dbval () - bit count of db_value
 *   return:
 *   result(out): resultant db_value
 *   value(in) : input db_value
 */
int
db_bit_count_dbval (DB_VALUE * result, DB_VALUE * value)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_value;
  DB_TYPE type;
  int i, c = 0;
  double d;
  DB_BIGINT bi;
  DB_VALUE *tmpval_p;

  assert (result != value);

  if (value == NULL)
    {
      return ER_FAILED;
    }

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (1, &tmp_value);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (1, value))
    {
      DB_MAKE_NULL (result);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  type = DB_VALUE_DOMAIN_TYPE (value);

  if (!TP_IS_NUMERIC_TYPE (type))
    {
      value =
	db_value_cast_arg (value, &tmp_value, DB_TYPE_DOUBLE, &error_status);
    }

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  tmpval_p = value;
  type = DB_VALUE_DOMAIN_TYPE (value);

  switch (type)
    {
    case DB_TYPE_INTEGER:
      i = DB_GET_INTEGER (value);
      for (c = 0; i; c++)
	{
	  i &= i - 1;
	}
      break;

    case DB_TYPE_BIGINT:
      bi = DB_GET_BIGINT (value);
      for (c = 0; bi; c++)
	{
	  bi &= bi - 1;
	}
      break;

    case DB_TYPE_DOUBLE:
      d = DB_GET_DOUBLE (tmpval_p);
      if (d < 0)
	{
	  bi = (DB_BIGINT) (d - 0.5f);
	}
      else
	{
	  bi = (DB_BIGINT) (d + 0.5f);
	}
      for (c = 0; bi; c++)
	{
	  bi &= bi - 1;
	}
      break;

    case DB_TYPE_NUMERIC:
      numeric_coerce_num_to_double ((DB_C_NUMERIC) db_locate_numeric (value),
				    DB_VALUE_SCALE (value), &d);
      if (d < 0)
	{
	  bi = (DB_BIGINT) (d - 0.5f);
	}
      else
	{
	  bi = (DB_BIGINT) (d + 0.5f);
	}
      for (c = 0; bi; c++)
	{
	  bi &= bi - 1;
	}
      break;

    default:
      assert (false);
      error_status = ER_TP_CANT_COERCE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
	      pr_type_name (type), pr_type_name (DB_TYPE_DOUBLE));
      goto exit_on_error;
      break;
    }

  DB_MAKE_INT (result, c);

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_value);

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
  db_value_clear_nargs (1, &tmp_value);

  return error_status;
}

/*
 * db_typeof_dbval() -
 *   return:
 *   result(out):
 *   value(in) : input db_value
 */
int
db_typeof_dbval (DB_VALUE * result, DB_VALUE * value)
{
  DB_TYPE type;
  const char *type_name;
  char *buf;

  assert (result != value);

  type = DB_VALUE_TYPE (value);
  type_name = pr_type_name (type);
  if (type_name == NULL)
    {
      db_make_null (result);
      return NO_ERROR;
    }

  switch (type)
    {
    case DB_TYPE_VARCHAR:
    case DB_TYPE_VARBIT:
    case DB_TYPE_NUMERIC:
      buf = (char *) malloc (128);
      if (buf == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, 128);
	  return ER_OUT_OF_VIRTUAL_MEMORY;
	}

      if (type == DB_TYPE_NUMERIC)
	{
	  snprintf (buf, 128, "%s (%u, %u)", type_name,
		    value->domain.numeric_info.precision,
		    value->domain.numeric_info.scale);
	}
      else
	{
	  snprintf (buf, 128, "%s (%d)", type_name,
		    value->domain.char_info.length);
	}

      db_make_string (result, buf);
      result->need_clear = true;
      break;

    default:
      db_make_string (result, type_name);
    }

  return NO_ERROR;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * get_number_dbval_as_long_double () -
 *   return:
 *   long double(out):
 *   value(in) :
 */
static int
get_number_dbval_as_long_double (long double *ld, const DB_VALUE * value)
{
  DB_TYPE value_type;
  int i;
  long double dtmp;
  DB_BIGINT bi;
  char num_string[2 * DB_MAX_NUMERIC_PRECISION + 2];
  char *tail_ptr = NULL;

  value_type = DB_VALUE_DOMAIN_TYPE (value);

  switch (value_type)
    {
    case DB_TYPE_INTEGER:
      i = DB_GET_INTEGER (value);
      dtmp = (long double) i;
      break;

    case DB_TYPE_BIGINT:
      bi = DB_GET_BIGINT (value);
      dtmp = (long double) bi;
      break;

    case DB_TYPE_DOUBLE:
      dtmp = (long double) DB_GET_DOUBLE (value);
      break;

    case DB_TYPE_NUMERIC:
      numeric_coerce_num_to_dec_str ((DB_C_NUMERIC) db_locate_numeric (value),
				     num_string);
#ifdef _ISOC99_SOURCE
      dtmp =
	strtold (num_string, &tail_ptr) / powl (10.0, DB_VALUE_SCALE (value));
#else
      dtmp = atof (num_string) / pow (10.0, DB_VALUE_SCALE (value));
#endif
      break;

    default:
      assert (false);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
	      pr_type_name (value_type), pr_type_name (DB_TYPE_DOUBLE));
      return ER_TP_CANT_COERCE;
    }

  *ld = dtmp;
  return NO_ERROR;
}
#endif

/*
 * db_width_bucket_calculate_numeric() -
 *   return:
 *   result(out):
 *   value1-4(in) : input db_value
 */
static int
db_width_bucket_calculate_numeric (double *result,
				   const DB_VALUE * value1,
				   const DB_VALUE * value2,
				   const DB_VALUE * value3,
				   const DB_VALUE * value4)
{
  int er_status = NO_ERROR, c;
  DB_VALUE cmp_result;
  DB_VALUE n1, n2, n3, n4;
  double res = 0.0;

  assert (value1 != NULL && value2 != NULL
	  && value3 != NULL && value4 != NULL && result != NULL);

  assert (DB_VALUE_TYPE (value1) == DB_TYPE_NUMERIC
	  && DB_VALUE_TYPE (value2) == DB_TYPE_NUMERIC
	  && DB_VALUE_TYPE (value3) == DB_TYPE_NUMERIC
	  && DB_VALUE_TYPE (value4) == DB_TYPE_NUMERIC);

  DB_MAKE_NULL (&cmp_result);
  DB_MAKE_NULL (&n1);
  DB_MAKE_NULL (&n2);
  DB_MAKE_NULL (&n3);
  DB_MAKE_NULL (&n4);

  er_status = numeric_db_value_compare (value2, value3, &cmp_result);
  if (er_status != NO_ERROR)
    {
      return er_status;
    }

  c = DB_GET_INTEGER (&cmp_result);
  if (c == 0 || c == -1)
    {
      /* value2 <= value3 */

      er_status = numeric_db_value_compare (value1, value2, &cmp_result);
      if (er_status != NO_ERROR)
	{
	  return er_status;
	}

      if (DB_GET_INTEGER (&cmp_result) < 0)
	{
	  res = 0.0;
	}
      else
	{
	  er_status = numeric_db_value_compare (value3, value1, &cmp_result);
	  if (er_status != NO_ERROR)
	    {
	      return er_status;
	    }

	  if (DB_GET_INTEGER (&cmp_result) < 1)
	    {
	      numeric_coerce_num_to_double (DB_GET_NUMERIC (value4),
					    DB_VALUE_SCALE (value4), &res);
	      res += 1.0;
	    }
	  else
	    {
	      /* floor ((v1-v2)/((v3-v2)/v4)) + 1 */
	      er_status = numeric_db_value_sub (value1, value2, &n1);
	      if (er_status != NO_ERROR)
		{
		  return er_status;
		}

	      er_status = numeric_db_value_sub (value3, value2, &n2);
	      if (er_status != NO_ERROR)
		{
		  return er_status;
		}

	      er_status = numeric_db_value_div (&n2, value4, &n3);
	      if (er_status != NO_ERROR)
		{
		  return er_status;
		}

	      er_status = numeric_db_value_div (&n1, &n3, &n4);
	      if (er_status != NO_ERROR)
		{
		  return er_status;
		}

	      numeric_coerce_num_to_double (DB_GET_NUMERIC (&n4),
					    DB_VALUE_SCALE (&n4), &res);
	      if (OR_CHECK_DOUBLE_OVERFLOW (res))
		{
		  return ER_QPROC_OVERFLOW_COERCION;
		}

	      res = floor (res) + 1.0;
	    }
	}
    }
  else
    {
      /* value2 > value3 */
      assert (c == 1);

      er_status = numeric_db_value_compare (value2, value1, &cmp_result);
      if (er_status != NO_ERROR)
	{
	  return er_status;
	}

      if (DB_GET_INTEGER (&cmp_result) < 0)
	{
	  res = 0.0;
	}
      else
	{
	  er_status = numeric_db_value_compare (value2, value3, &cmp_result);
	  if (er_status != NO_ERROR)
	    {
	      return er_status;
	    }

	  if (DB_GET_INTEGER (&cmp_result) < 1)
	    {
	      numeric_coerce_num_to_double (DB_GET_NUMERIC (value4),
					    DB_VALUE_SCALE (value4), &res);
	      res += 1.0;
	    }
	  else
	    {
	      /* floor ((v2-v1)/((v2-v3)/v4)) + 1 */
	      er_status = numeric_db_value_sub (value2, value1, &n1);
	      if (er_status != NO_ERROR)
		{
		  return er_status;
		}

	      er_status = numeric_db_value_sub (value2, value3, &n2);
	      if (er_status != NO_ERROR)
		{
		  return er_status;
		}

	      er_status = numeric_db_value_div (&n2, value4, &n3);
	      if (er_status != NO_ERROR)
		{
		  return er_status;
		}

	      er_status = numeric_db_value_div (&n1, &n3, &n4);
	      if (er_status != NO_ERROR)
		{
		  return er_status;
		}

	      numeric_coerce_num_to_double (DB_GET_NUMERIC (&n4),
					    DB_VALUE_SCALE (&n4), &res);
	      if (OR_CHECK_DOUBLE_OVERFLOW (res))
		{
		  return ER_QPROC_OVERFLOW_COERCION;
		}

	      res = floor (res) + 1.0;
	    }
	}
    }

  if (OR_CHECK_DOUBLE_OVERFLOW (res))
    {
      return ER_QPROC_OVERFLOW_HAPPENED;
    }

  *result = res;
  return NO_ERROR;
}

/*
 * db_width_bucket() -
 *   return:
 *   result(out):
 *   value1-4(in) : input db_value
 */
int
db_width_bucket (DB_VALUE * result, const DB_VALUE * value1,
		 const DB_VALUE * value2, const DB_VALUE * value3,
		 const DB_VALUE * value4)
{
#define RETURN_ERROR(err) \
  do \
    { \
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, (err), 0); \
      return (err); \
    } \
  while (0)

#define RETURN_ERROR_WITH_TWO_ARGS(err, arg1, arg2) \
  do \
    { \
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, (err), 2, (arg1), (arg2)); \
      return (err); \
    } \
  while (0)

  double d1, d2, d3, d4, d_ret;
  double tmp_d1 = 0.0, tmp_d2 = 0.0, tmp_d3 = 0.0, tmp_d4 = 0.0;
  DB_TYPE type, cast_type;
  DB_VALUE cast_value1, cast_value2, cast_value3, cast_value4;
  TP_DOMAIN *cast_domain = NULL;
  TP_DOMAIN_STATUS cast_status;
  bool is_deal_with_numeric = false;
  int er_status = NO_ERROR;

  assert (result != NULL);
  assert (value1 != NULL);
  assert (value2 != NULL);
  assert (value3 != NULL);
  assert (value4 != NULL);
  assert (result != value1);
  assert (result != value2);
  assert (result != value3);
  assert (result != value4);

  DB_MAKE_NULL (&cast_value1);
  DB_MAKE_NULL (&cast_value2);
  DB_MAKE_NULL (&cast_value3);
  DB_MAKE_NULL (&cast_value4);

  if (DB_VALUE_TYPE (value1) == DB_TYPE_NULL
      || DB_VALUE_TYPE (value2) == DB_TYPE_NULL
      || DB_VALUE_TYPE (value3) == DB_TYPE_NULL
      || DB_VALUE_TYPE (value4) == DB_TYPE_NULL)
    {
      DB_MAKE_NULL (result);
      return NO_ERROR;
    }

  if (DB_VALUE_TYPE (value4) == DB_TYPE_DOUBLE)
    {
      d4 = DB_GET_DOUBLE (value4);
    }
  else
    {
      DB_VALUE tmpval;

      DB_MAKE_NULL (&tmpval);

      if (tp_value_coerce (value4, &tmpval, &tp_Double_domain) !=
	  DOMAIN_COMPATIBLE)
	{
	  er_status = ER_TP_CANT_COERCE;
	  RETURN_ERROR_WITH_TWO_ARGS (er_status,
				      pr_type_name (DB_VALUE_TYPE (value4)),
				      pr_type_name (DB_TYPE_DOUBLE));
	}

      d4 = DB_GET_DOUBLE (&tmpval);
    }

  if (d4 < 1 || d4 >= DB_INT32_MAX)
    {
      RETURN_ERROR (ER_PROC_WIDTH_BUCKET_COUNT);
    }

  d4 = (int) floor (d4);

  /* find the common type of value1, value2 and value3
   * and cast them to the common type
   */
  cast_type = DB_TYPE_UNKNOWN;

  type = DB_VALUE_DOMAIN_TYPE (value1);
  switch (type)
    {
    case DB_TYPE_VARCHAR:
      /* try double */
      cast_domain = tp_domain_resolve_default (DB_TYPE_DOUBLE);
      cast_status = tp_value_coerce (value1, &cast_value1, cast_domain);
      if (cast_status == DOMAIN_COMPATIBLE)
	{
	  cast_type = DB_TYPE_DOUBLE;
	}
      else
	{
	  /* try datetime
	   * date, is compatible with datetime
	   */
	  cast_domain = tp_domain_resolve_default (DB_TYPE_DATETIME);
	  cast_status = tp_value_coerce (value1, &cast_value1, cast_domain);
	  if (cast_status == DOMAIN_COMPATIBLE)
	    {
	      cast_type = DB_TYPE_DATETIME;
	    }
	  else
	    {
	      /* try time */
	      cast_domain = tp_domain_resolve_default (DB_TYPE_TIME);
	      cast_status = tp_value_coerce (value1, &cast_value1,
					     cast_domain);
	      if (cast_status == DOMAIN_COMPATIBLE)
		{
		  cast_type = DB_TYPE_TIME;
		}
	      else
		{
		  RETURN_ERROR_WITH_TWO_ARGS (ER_TP_CANT_COERCE,
					      pr_type_name (type),
					      pr_type_name (DB_TYPE_TIME));
		}
	    }
	}

      value1 = &cast_value1;
      break;

    case DB_TYPE_DATE:
    case DB_TYPE_DATETIME:
    case DB_TYPE_TIME:
      cast_type = type;
      break;

    default:
      break;
    }

  if (cast_type != DB_TYPE_UNKNOWN)
    {
      /* coerce value2 with the type of value1 */
      if (cast_type != DB_VALUE_DOMAIN_TYPE (value2))
	{
	  cast_domain = tp_domain_resolve_default (cast_type);
	  cast_status = tp_value_coerce (value2, &cast_value2, cast_domain);
	  if (cast_status != DOMAIN_COMPATIBLE)
	    {
	      RETURN_ERROR_WITH_TWO_ARGS (ER_TP_CANT_COERCE,
					  pr_type_name (type),
					  pr_type_name (cast_type));
	    }

	  value2 = &cast_value2;
	}

      /* coerce value3 with the type of value1 */
      if (cast_type != DB_VALUE_DOMAIN_TYPE (value3))
	{
	  cast_domain = tp_domain_resolve_default (cast_type);
	  cast_status = tp_value_coerce (value3, &cast_value3, cast_domain);
	  if (cast_status != DOMAIN_COMPATIBLE)
	    {
	      RETURN_ERROR_WITH_TWO_ARGS (ER_TP_CANT_COERCE,
					  pr_type_name (type),
					  pr_type_name (cast_type));
	    }

	  value3 = &cast_value3;
	}
    }

  /* the type of value1 is fixed */
  type = DB_VALUE_DOMAIN_TYPE (value1);
  switch (type)
    {
    case DB_TYPE_DATE:
#if 1				/* TODO - */
      if (DB_VALUE_DOMAIN_TYPE (value2) != DB_TYPE_DATE)
	{
	  RETURN_ERROR_WITH_TWO_ARGS (ER_TP_CANT_COERCE,
				      pr_type_name (DB_VALUE_DOMAIN_TYPE
						    (value2)),
				      pr_type_name (DB_TYPE_DATE));
	}
      if (DB_VALUE_DOMAIN_TYPE (value3) != DB_TYPE_DATE)
	{
	  RETURN_ERROR_WITH_TWO_ARGS (ER_TP_CANT_COERCE,
				      pr_type_name (DB_VALUE_DOMAIN_TYPE
						    (value3)),
				      pr_type_name (DB_TYPE_DATE));
	}
#endif
      d1 = (double) *DB_GET_DATE (value1);
      d2 = (double) *DB_GET_DATE (value2);
      d3 = (double) *DB_GET_DATE (value3);
      break;

    case DB_TYPE_DATETIME:
#if 1				/* TODO - */
      if (DB_VALUE_DOMAIN_TYPE (value2) != DB_TYPE_DATETIME)
	{
	  RETURN_ERROR_WITH_TWO_ARGS (ER_TP_CANT_COERCE,
				      pr_type_name (DB_VALUE_DOMAIN_TYPE
						    (value2)),
				      pr_type_name (DB_TYPE_DATETIME));
	}
      if (DB_VALUE_DOMAIN_TYPE (value3) != DB_TYPE_DATETIME)
	{
	  RETURN_ERROR_WITH_TWO_ARGS (ER_TP_CANT_COERCE,
				      pr_type_name (DB_VALUE_DOMAIN_TYPE
						    (value3)),
				      pr_type_name (DB_TYPE_DATETIME));
	}
#endif
      /* double can hold datetime type */
      d1 = ((double) DB_GET_DATETIME (value1)->date)
	* MILLISECONDS_OF_ONE_DAY + DB_GET_DATETIME (value1)->time;
      d2 = ((double) DB_GET_DATETIME (value2)->date)
	* MILLISECONDS_OF_ONE_DAY + DB_GET_DATETIME (value2)->time;
      d3 = ((double) DB_GET_DATETIME (value3)->date)
	* MILLISECONDS_OF_ONE_DAY + DB_GET_DATETIME (value3)->time;
      break;

    case DB_TYPE_TIME:
#if 1				/* TODO - */
      if (DB_VALUE_DOMAIN_TYPE (value2) != DB_TYPE_TIME)
	{
	  RETURN_ERROR_WITH_TWO_ARGS (ER_TP_CANT_COERCE,
				      pr_type_name (DB_VALUE_DOMAIN_TYPE
						    (value2)),
				      pr_type_name (DB_TYPE_TIME));
	}
      if (DB_VALUE_DOMAIN_TYPE (value3) != DB_TYPE_TIME)
	{
	  RETURN_ERROR_WITH_TWO_ARGS (ER_TP_CANT_COERCE,
				      pr_type_name (DB_VALUE_DOMAIN_TYPE
						    (value3)),
				      pr_type_name (DB_TYPE_TIME));
	}
#endif
      d1 = (double) *DB_GET_TIME (value1);
      d2 = (double) *DB_GET_TIME (value2);
      d3 = (double) *DB_GET_TIME (value3);
      break;

    case DB_TYPE_INTEGER:
    case DB_TYPE_DOUBLE:
      if (get_number_dbval_as_double (&d1, value1) != NO_ERROR)
	{
	  RETURN_ERROR_WITH_TWO_ARGS (ER_TP_CANT_COERCE,
				      pr_type_name (DB_VALUE_DOMAIN_TYPE
						    (value1)),
				      pr_type_name (DB_TYPE_DOUBLE));
	}
      if (get_number_dbval_as_double (&d2, value2) != NO_ERROR)
	{
	  RETURN_ERROR_WITH_TWO_ARGS (ER_TP_CANT_COERCE,
				      pr_type_name (DB_VALUE_DOMAIN_TYPE
						    (value2)),
				      pr_type_name (DB_TYPE_DOUBLE));
	}
      if (get_number_dbval_as_double (&d3, value3) != NO_ERROR)
	{
	  RETURN_ERROR_WITH_TWO_ARGS (ER_TP_CANT_COERCE,
				      pr_type_name (DB_VALUE_DOMAIN_TYPE
						    (value3)),
				      pr_type_name (DB_TYPE_DOUBLE));
	}
      break;

    case DB_TYPE_BIGINT:
    case DB_TYPE_NUMERIC:
      d1 = d2 = d3 = 0;		/* to make compiler be silent */

      /* gcc fully support long double (80 or 128bits)
       * if long double is not fully supported, do calculation with numeric
       */
      cast_domain =
	tp_domain_construct (DB_TYPE_NUMERIC, NULL, DB_MAX_NUMERIC_PRECISION,
			     DB_MAX_NUMERIC_PRECISION / 2, NULL);
      if (cast_domain == NULL)
	{
	  RETURN_ERROR (er_errid ());
	}

      cast_domain = tp_domain_cache (cast_domain);

      if (type == DB_TYPE_BIGINT)
	{
	  /* cast bigint to numeric
	   * Compiler doesn't support long double (80 or 128bits), so we use
	   * numeric instead.
	   * If a high precision lib is introduced or long double is full
	   * supported, remove this part and use the lib or long double
	   * to calculate.
	   */
	  /* convert value1 */
	  cast_status = tp_value_coerce (value1, &cast_value1, cast_domain);
	  if (cast_status != DOMAIN_COMPATIBLE)
	    {
	      RETURN_ERROR_WITH_TWO_ARGS (ER_TP_CANT_COERCE_OVERFLOW,
					  pr_type_name (DB_VALUE_TYPE
							(value1)),
					  pr_type_name (TP_DOMAIN_TYPE
							(cast_domain)));
	    }

	  value1 = &cast_value1;
	}

      /* cast value2, value3, value4 to numeric to make the calculation */
      if (DB_VALUE_DOMAIN_TYPE (value2) != DB_TYPE_NUMERIC)
	{
	  cast_status = tp_value_coerce (value2, &cast_value2, cast_domain);
	  if (cast_status != DOMAIN_COMPATIBLE)
	    {
	      RETURN_ERROR_WITH_TWO_ARGS (ER_TP_CANT_COERCE_OVERFLOW,
					  pr_type_name (DB_VALUE_TYPE
							(value2)),
					  pr_type_name (TP_DOMAIN_TYPE
							(cast_domain)));
	    }

	  value2 = &cast_value2;
	}

      if (DB_VALUE_DOMAIN_TYPE (value3) != DB_TYPE_NUMERIC)
	{
	  cast_status = tp_value_coerce (value3, &cast_value3, cast_domain);
	  if (cast_status != DOMAIN_COMPATIBLE)
	    {
	      RETURN_ERROR_WITH_TWO_ARGS (ER_TP_CANT_COERCE_OVERFLOW,
					  pr_type_name (DB_VALUE_TYPE
							(value3)),
					  pr_type_name (TP_DOMAIN_TYPE
							(cast_domain)));
	    }

	  value3 = &cast_value3;
	}

      DB_MAKE_INT (&cast_value4, ((int) d4));

      cast_domain =
	tp_domain_construct (DB_TYPE_NUMERIC, NULL, DB_INTEGER_PRECISION, 0,
			     NULL);
      if (cast_domain == NULL)
	{
	  RETURN_ERROR (er_errid ());
	}

      cast_domain = tp_domain_cache (cast_domain);

      cast_status = tp_value_coerce (&cast_value4, &cast_value4, cast_domain);
      if (cast_status != DOMAIN_COMPATIBLE)
	{
	  RETURN_ERROR (ER_QPROC_OVERFLOW_HAPPENED);
	}

      value4 = &cast_value4;

      is_deal_with_numeric = true;

      break;

    default:
      RETURN_ERROR (ER_OBJ_INVALID_ARGUMENTS);
    }

  if (is_deal_with_numeric)
    {
      er_status = db_width_bucket_calculate_numeric (&d_ret, value1, value2,
						     value3, value4);
      if (er_status != NO_ERROR)
	{
	  RETURN_ERROR (er_status);
	}
    }
  else
    {
      if (d2 <= d3)
	{
	  if (d1 < d2)
	    {
	      d_ret = 0.0;
	    }
	  else if (d3 <= d1)
	    {
	      d_ret = d4 + 1.0;
	    }
	  else
	    {
	      /* d_ret = floor ((d1 - d2) / ((d3 - d2) / d4)) + 1.0 */
	      tmp_d1 = d1 - d2;
	      tmp_d2 = d3 - d2;
	      tmp_d3 = tmp_d2 / d4;
	      tmp_d4 = tmp_d1 / tmp_d3;
	      d_ret = floor (tmp_d4) + 1.0;
	    }
	}
      else
	{
	  if (d2 < d1)
	    {
	      d_ret = 0.0;
	    }
	  else if (d1 <= d3)
	    {
	      d_ret = d4 + 1.0;
	    }
	  else
	    {
	      /* d_ret = floor ((d2 - d1) / ((d2 - d3) / d4)) + 1.0 */
	      tmp_d1 = d2 - d1;
	      tmp_d2 = d2 - d3;
	      tmp_d3 = tmp_d2 / d4;
	      tmp_d4 = tmp_d1 / tmp_d3;
	      d_ret = floor (tmp_d4) + 1.0;
	    }
	}
    }

  /* check overflow */
  if (OR_CHECK_DOUBLE_OVERFLOW (tmp_d1) || OR_CHECK_DOUBLE_OVERFLOW (tmp_d2))
    {
      RETURN_ERROR (ER_QPROC_OVERFLOW_SUBTRACTION);
    }
  else if (OR_CHECK_DOUBLE_OVERFLOW (tmp_d3)
	   || OR_CHECK_DOUBLE_OVERFLOW (tmp_d4))
    {
      RETURN_ERROR (ER_QPROC_OVERFLOW_DIVISION);
    }
  else if (OR_CHECK_INT_OVERFLOW (d_ret))
    {
      RETURN_ERROR (ER_QPROC_OVERFLOW_HAPPENED);
    }

  DB_MAKE_INT (result, ((int) d_ret));

  return er_status;

#undef RETURN_ERROR
#undef RETURN_ERROR_WITH_TWO_ARGS
}

/*
 * qdata_add_dbval () -
 *   return: NO_ERROR, or ER_code
 *   dbval1(in) : First db_value node
 *   dbval2(in) : Second db_value node
 *   res(out)   : Resultant db_value node
 *
 */
int
qdata_add_dbval (DB_VALUE * dbval1_p, DB_VALUE * dbval2_p,
		 DB_VALUE * result_p)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_dbval1, tmp_dbval2;
  DB_TYPE type1, type2;
#if 0
  TP_DOMAIN *domain_p;
#endif

  assert (dbval1_p != result_p);
  assert (dbval2_p != result_p);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (2, &tmp_dbval1, &tmp_dbval2);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (2, dbval1_p, dbval2_p))
    {
      goto done;
    }

  /* arg cast type **********************************************************
   */
  type1 = DB_VALUE_DOMAIN_TYPE (dbval1_p);
  type2 = DB_VALUE_DOMAIN_TYPE (dbval2_p);

  if (TP_IS_SET_TYPE (type1))
    {
      ;				/* TODO - for catalog access */
    }
  else if (!TP_IS_NUMERIC_TYPE (type1))
    {
      dbval1_p =
	db_value_cast_arg (dbval1_p, &tmp_dbval1, DB_TYPE_DOUBLE,
			   &error_status);
    }

  if (TP_IS_SET_TYPE (type2))
    {
      ;				/* TODO - for catalog access */
    }
  else if (!TP_IS_NUMERIC_TYPE (type2))
    {
      dbval2_p =
	db_value_cast_arg (dbval2_p, &tmp_dbval2, DB_TYPE_DOUBLE,
			   &error_status);
    }

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  type1 = DB_VALUE_DOMAIN_TYPE (dbval1_p);
  type2 = DB_VALUE_DOMAIN_TYPE (dbval2_p);

  switch (type1)
    {
    case DB_TYPE_INTEGER:
      error_status = qdata_add_int_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_BIGINT:
      error_status = qdata_add_bigint_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_DOUBLE:
      error_status = qdata_add_double_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_NUMERIC:
      error_status =
	qdata_add_numeric_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_SEQUENCE:
#if 0				/* TODO - for catalog access */
      if (!TP_IS_SET_TYPE (type2))
	{
	  error_status = ER_TP_CANT_COERCE;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
		  pr_type_name (type2), pr_type_name (type1));
	  goto exit_on_error;
	}

      if (type1 == type2)
	{
	  /* partial resolve : set only basic domain; full domain will be
	   * resolved in 'fetch', based on the result's value*/
	  domain_p = tp_domain_resolve_default (type1);
	}
      else
	{
	  domain_p = tp_domain_resolve_default (DB_TYPE_SEQUENCE);
	}

      error_status =
	qdata_add_sequence_to_dbval (dbval1_p, dbval2_p, result_p, domain_p);
#else
      error_status = ER_TP_CANT_COERCE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
	      pr_type_name (type1), pr_type_name (DB_TYPE_DOUBLE));
#endif
      break;

    default:
      assert (false);
      error_status = ER_TP_CANT_COERCE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
	      pr_type_name (type1), pr_type_name (DB_TYPE_DOUBLE));
      break;
    }

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_dbval1, &tmp_dbval2);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
	      pr_type_name (type1), pr_type_name (type2));
    }

  DB_MAKE_NULL (result_p);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_dbval1, &tmp_dbval2);

  return error_status;
}

static int
qdata_add_int_to_dbval (DB_VALUE * int_val_p, DB_VALUE * dbval_p,
			DB_VALUE * result_p)
{
  int i;
  DB_TYPE type;

  i = DB_GET_INT (int_val_p);
  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_INTEGER:
      return qdata_add_int (i, DB_GET_INT (dbval_p), result_p);

    case DB_TYPE_BIGINT:
      return qdata_add_bigint (i, DB_GET_BIGINT (dbval_p), result_p);

    case DB_TYPE_DOUBLE:
      return qdata_add_double (i, DB_GET_DOUBLE (dbval_p), result_p);

    case DB_TYPE_NUMERIC:
      return qdata_add_numeric (dbval_p, int_val_p, result_p);
      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
	      pr_type_name (type), pr_type_name (DB_TYPE_DOUBLE));
      return ER_TP_CANT_COERCE;
      break;
    }

  return NO_ERROR;
}

static int
qdata_add_bigint_to_dbval (DB_VALUE * bigint_val_p, DB_VALUE * dbval_p,
			   DB_VALUE * result_p)
{
  DB_BIGINT bi;
  DB_TYPE type;

  bi = DB_GET_BIGINT (bigint_val_p);
  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_INTEGER:
      return qdata_add_bigint (bi, DB_GET_INT (dbval_p), result_p);

    case DB_TYPE_BIGINT:
      return qdata_add_bigint (bi, DB_GET_BIGINT (dbval_p), result_p);

    case DB_TYPE_DOUBLE:
      return qdata_add_double ((double) bi, DB_GET_DOUBLE (dbval_p),
			       result_p);

    case DB_TYPE_NUMERIC:
      return qdata_add_numeric (dbval_p, bigint_val_p, result_p);
      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
	      pr_type_name (type), pr_type_name (DB_TYPE_DOUBLE));
      return ER_TP_CANT_COERCE;
      break;
    }

  return NO_ERROR;
}

static int
qdata_add_double_to_dbval (DB_VALUE * double_val_p, DB_VALUE * dbval_p,
			   DB_VALUE * result_p)
{
  double d1;
  DB_TYPE type;

  d1 = DB_GET_DOUBLE (double_val_p);
  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_INTEGER:
      return qdata_add_double (d1, DB_GET_INT (dbval_p), result_p);

    case DB_TYPE_BIGINT:
      return qdata_add_double (d1, (double) DB_GET_BIGINT (dbval_p),
			       result_p);

    case DB_TYPE_DOUBLE:
      return qdata_add_double (d1, DB_GET_DOUBLE (dbval_p), result_p);

    case DB_TYPE_NUMERIC:
      return qdata_add_double (d1, qdata_coerce_numeric_to_double (dbval_p),
			       result_p);

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
	      pr_type_name (type), pr_type_name (DB_TYPE_DOUBLE));
      return ER_TP_CANT_COERCE;
      break;
    }

  return NO_ERROR;
}

static int
qdata_add_numeric_to_dbval (DB_VALUE * numeric_val_p, DB_VALUE * dbval_p,
			    DB_VALUE * result_p)
{
  DB_TYPE type;

  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_INTEGER:
    case DB_TYPE_BIGINT:
      return qdata_add_numeric (numeric_val_p, dbval_p, result_p);

    case DB_TYPE_NUMERIC:
      if (numeric_db_value_add (numeric_val_p, dbval_p, result_p) != NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_OVERFLOW_HAPPENED, 0);
	  return ER_QPROC_OVERFLOW_HAPPENED;
	}
      break;

    case DB_TYPE_DOUBLE:
      return
	qdata_add_double (qdata_coerce_numeric_to_double (numeric_val_p),
			  DB_GET_DOUBLE (dbval_p), result_p);

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
	      pr_type_name (type), pr_type_name (DB_TYPE_DOUBLE));
      return ER_TP_CANT_COERCE;
      break;
    }

  return NO_ERROR;
}

#if defined (ENABLE_UNUSED_FUNCTION)
static int
qdata_add_sequence_to_dbval (DB_VALUE * seq_val_p, DB_VALUE * dbval_p,
			     DB_VALUE * result_p, TP_DOMAIN * domain_p)
{
  DB_SET *set_tmp;
  DB_SEQ *seq_tmp, *seq_tmp1;
  DB_VALUE dbval_tmp;
  int i, card, card1;
#if !defined(NDEBUG)
  DB_TYPE type1, type2;
#endif

#if !defined(NDEBUG)
  type1 = DB_VALUE_DOMAIN_TYPE (seq_val_p);
  type2 = DB_VALUE_DOMAIN_TYPE (dbval_p);

  assert (TP_IS_SET_TYPE (type1));
  assert (TP_IS_SET_TYPE (type2));
#endif

  if (domain_p == NULL)
    {
      return ER_FAILED;
    }

  DB_MAKE_NULL (&dbval_tmp);

  if (TP_DOMAIN_TYPE (domain_p) == DB_TYPE_SEQUENCE)
    {
      if (tp_value_coerce (seq_val_p, result_p, domain_p) !=
	  DOMAIN_COMPATIBLE)
	{
	  return ER_FAILED;
	}

      seq_tmp = DB_GET_SEQUENCE (dbval_p);
      card = db_seq_size (seq_tmp);
      seq_tmp1 = DB_GET_SEQUENCE (result_p);
      card1 = db_seq_size (seq_tmp1);

      for (i = 0; i < card; i++)
	{
	  if (db_seq_get (seq_tmp, i, &dbval_tmp) != NO_ERROR)
	    {
	      return ER_FAILED;
	    }

	  if (db_seq_put (seq_tmp1, card1 + i, &dbval_tmp) != NO_ERROR)
	    {
	      pr_clear_value (&dbval_tmp);
	      return ER_FAILED;
	    }

	  pr_clear_value (&dbval_tmp);
	}
    }
  else
    {
      /* set or multiset */
      if (set_union (DB_GET_SET (seq_val_p), DB_GET_SET (dbval_p),
		     &set_tmp, domain_p) < 0)
	{
	  return ER_FAILED;
	}

      pr_clear_value (result_p);
      set_make_collection (result_p, set_tmp);
    }

  return NO_ERROR;
}
#endif

/*
 * ARITHMETIC EXPRESSION EVALUATION ROUTINES
 */

static int
qdata_add_int (int i1, int i2, DB_VALUE * result_p)
{
  int result;

  result = i1 + i2;

  if (OR_CHECK_ADD_OVERFLOW (i1, i2, result))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_HAPPENED,
	      0);
      return ER_QPROC_OVERFLOW_HAPPENED;
    }

  DB_MAKE_INT (result_p, result);
  return NO_ERROR;
}

static int
qdata_add_bigint (DB_BIGINT bi1, DB_BIGINT bi2, DB_VALUE * result_p)
{
  DB_BIGINT result;

  result = bi1 + bi2;

  if (OR_CHECK_ADD_OVERFLOW (bi1, bi2, result))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_HAPPENED,
	      0);
      return ER_QPROC_OVERFLOW_HAPPENED;
    }

  DB_MAKE_BIGINT (result_p, result);
  return NO_ERROR;
}

static int
qdata_add_double (double d1, double d2, DB_VALUE * result_p)
{
  double result;

  result = d1 + d2;

  if (OR_CHECK_DOUBLE_OVERFLOW (result))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_HAPPENED,
	      0);
      return ER_QPROC_OVERFLOW_HAPPENED;
    }

  DB_MAKE_DOUBLE (result_p, result);
  return NO_ERROR;
}

static double
qdata_coerce_numeric_to_double (DB_VALUE * numeric_val_p)
{
  DB_VALUE dbval_tmp;
  DB_DATA_STATUS data_stat;

  db_value_domain_init (&dbval_tmp, DB_TYPE_DOUBLE,
			DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
  (void) numeric_db_value_coerce_from_num (numeric_val_p, &dbval_tmp,
					   &data_stat);

  return DB_GET_DOUBLE (&dbval_tmp);
}

int
qdata_coerce_dbval_to_numeric (DB_VALUE * dbval_p, DB_VALUE * result_p)
{
  DB_TYPE type;
  DB_DATA_STATUS data_stat;

  type = DB_VALUE_DOMAIN_TYPE (dbval_p);
  if (type == DB_TYPE_NUMERIC)
    {
      db_value_domain_init (result_p, DB_TYPE_NUMERIC,
			    DB_VALUE_PRECISION (dbval_p),
			    DB_VALUE_SCALE (dbval_p));
    }
  else
    {
      db_value_domain_init (result_p, DB_TYPE_NUMERIC,
			    DB_DEFAULT_PRECISION, DB_DEFAULT_SCALE);
    }

  return numeric_db_value_coerce_to_num (dbval_p, result_p, &data_stat);
}

static int
qdata_add_numeric (DB_VALUE * numeric_val_p, DB_VALUE * dbval_p,
		   DB_VALUE * result_p)
{
  DB_VALUE dbval_tmp;

  if (qdata_coerce_dbval_to_numeric (dbval_p, &dbval_tmp) != NO_ERROR
      || numeric_db_value_add (&dbval_tmp, numeric_val_p,
			       result_p) != NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_HAPPENED,
	      0);
      return ER_QPROC_OVERFLOW_HAPPENED;
    }

  return NO_ERROR;
}

/*
 * qdata_subtract_dbval () -
 *   return: NO_ERROR, or ER_code
 *   dbval1(in) : First db_value node
 *   dbval2(in) : Second db_value node
 *   res(out)   : Resultant db_value node
 *
 * Note: Subtract dbval2 value from dbval1 value.
 * Overflow checks are only done when both operand maximums have
 * overlapping precision/scale.  That is,
 *     short - integer -> overflow is checked
 *     float - double  -> overflow is not checked.  Maximum float
 *                        value does not overlap maximum double
 *                        precision/scale.
 *                        MAX_FLT - MAX_DBL = -MAX_DBL
 */
int
qdata_subtract_dbval (DB_VALUE * dbval1_p, DB_VALUE * dbval2_p,
		      DB_VALUE * result_p)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_dbval1, tmp_dbval2;
  DB_TYPE type1, type2;
#if 0
  TP_DOMAIN *domain_p;
#endif

  assert (dbval1_p != result_p);
  assert (dbval2_p != result_p);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (2, &tmp_dbval1, &tmp_dbval2);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (2, dbval1_p, dbval2_p))
    {
      goto done;
    }

  /* arg cast type **********************************************************
   */
  type1 = DB_VALUE_DOMAIN_TYPE (dbval1_p);
  type2 = DB_VALUE_DOMAIN_TYPE (dbval2_p);

  if (TP_IS_SET_TYPE (type1))
    {
      ;				/* TODO - for catalog access */
    }
  else if (!TP_IS_NUMERIC_TYPE (type1))
    {
      dbval1_p =
	db_value_cast_arg (dbval1_p, &tmp_dbval1, DB_TYPE_DOUBLE,
			   &error_status);
    }

  if (TP_IS_SET_TYPE (type2))
    {
      ;				/* TODO - for catalog access */
    }
  else if (!TP_IS_NUMERIC_TYPE (type2))
    {
      dbval2_p =
	db_value_cast_arg (dbval2_p, &tmp_dbval2, DB_TYPE_DOUBLE,
			   &error_status);
    }

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  type1 = DB_VALUE_DOMAIN_TYPE (dbval1_p);
  type2 = DB_VALUE_DOMAIN_TYPE (dbval2_p);

  switch (type1)
    {
    case DB_TYPE_BIGINT:
      error_status =
	qdata_subtract_bigint_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_INTEGER:
      error_status =
	qdata_subtract_int_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_DOUBLE:
      error_status =
	qdata_subtract_double_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_NUMERIC:
      error_status =
	qdata_subtract_numeric_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_SEQUENCE:
#if 0				/* TODO - for catalog access */
      if (!TP_IS_SET_TYPE (type2))
	{
	  error_status = ER_TP_CANT_COERCE;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
		  pr_type_name (type2), pr_type_name (type1));
	  goto exit_on_error;
	}

      domain_p = tp_domain_resolve_default (DB_TYPE_SEQUENCE);

      error_status =
	qdata_subtract_sequence_to_dbval (dbval1_p, dbval2_p, result_p,
					  domain_p);
#else
      error_status = ER_TP_CANT_COERCE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
	      pr_type_name (type1), pr_type_name (DB_TYPE_DOUBLE));
#endif
      break;

    default:
      assert (false);
      error_status = ER_TP_CANT_COERCE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
	      pr_type_name (type1), pr_type_name (DB_TYPE_DOUBLE));
      break;
    }

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_dbval1, &tmp_dbval2);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
	      pr_type_name (type1), pr_type_name (type2));
    }

  DB_MAKE_NULL (result_p);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_dbval1, &tmp_dbval2);

  return error_status;
}

static int
qdata_subtract_int_to_dbval (DB_VALUE * int_val_p, DB_VALUE * dbval_p,
			     DB_VALUE * result_p)
{
  int i;
  DB_TYPE type;
  DB_VALUE dbval_tmp;

  i = DB_GET_INT (int_val_p);
  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_INTEGER:
      return qdata_subtract_int (i, DB_GET_INT (dbval_p), result_p);

    case DB_TYPE_BIGINT:
      return qdata_subtract_bigint (i, DB_GET_BIGINT (dbval_p), result_p);

    case DB_TYPE_DOUBLE:
      return qdata_subtract_double (i, DB_GET_DOUBLE (dbval_p), result_p);

    case DB_TYPE_NUMERIC:
      if (qdata_coerce_dbval_to_numeric (int_val_p, &dbval_tmp) != NO_ERROR
	  || numeric_db_value_sub (&dbval_tmp, dbval_p, result_p) != NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_OVERFLOW_SUBTRACTION, 0);
	  return ER_FAILED;
	}
      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
	      pr_type_name (type), pr_type_name (DB_TYPE_DOUBLE));
      return ER_TP_CANT_COERCE;
      break;
    }

  return NO_ERROR;
}

static int
qdata_subtract_bigint_to_dbval (DB_VALUE * bigint_val_p, DB_VALUE * dbval_p,
				DB_VALUE * result_p)
{
  DB_BIGINT bi;
  DB_TYPE type;
  DB_VALUE dbval_tmp;

  bi = DB_GET_BIGINT (bigint_val_p);
  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_INTEGER:
      return qdata_subtract_bigint (bi, DB_GET_INT (dbval_p), result_p);

    case DB_TYPE_BIGINT:
      return qdata_subtract_bigint (bi, DB_GET_BIGINT (dbval_p), result_p);

    case DB_TYPE_DOUBLE:
      return qdata_subtract_double ((double) bi, DB_GET_DOUBLE (dbval_p),
				    result_p);

    case DB_TYPE_NUMERIC:
      if (qdata_coerce_dbval_to_numeric (bigint_val_p, &dbval_tmp) != NO_ERROR
	  || numeric_db_value_sub (&dbval_tmp, dbval_p, result_p) != NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_OVERFLOW_SUBTRACTION, 0);
	  return ER_FAILED;
	}
      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
	      pr_type_name (type), pr_type_name (DB_TYPE_DOUBLE));
      return ER_TP_CANT_COERCE;
      break;
    }

  return NO_ERROR;
}

static int
qdata_subtract_double_to_dbval (DB_VALUE * double_val_p, DB_VALUE * dbval_p,
				DB_VALUE * result_p)
{
  double d;
  DB_TYPE type;

  d = DB_GET_DOUBLE (double_val_p);
  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_INTEGER:
      return qdata_subtract_double (d, DB_GET_INT (dbval_p), result_p);

    case DB_TYPE_BIGINT:
      return qdata_subtract_double (d, (double) DB_GET_BIGINT (dbval_p),
				    result_p);

    case DB_TYPE_DOUBLE:
      return qdata_subtract_double (d, DB_GET_DOUBLE (dbval_p), result_p);

    case DB_TYPE_NUMERIC:
      return qdata_subtract_double (d,
				    qdata_coerce_numeric_to_double (dbval_p),
				    result_p);

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
	      pr_type_name (type), pr_type_name (DB_TYPE_DOUBLE));
      return ER_TP_CANT_COERCE;
      break;
    }

  return NO_ERROR;
}

static int
qdata_subtract_numeric_to_dbval (DB_VALUE * numeric_val_p,
				 DB_VALUE * dbval_p, DB_VALUE * result_p)
{
  DB_TYPE type;
  DB_VALUE dbval_tmp;

  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_INTEGER:
    case DB_TYPE_BIGINT:
      if (qdata_coerce_dbval_to_numeric (dbval_p, &dbval_tmp) != NO_ERROR
	  || numeric_db_value_sub (numeric_val_p, &dbval_tmp,
				   result_p) != NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_OVERFLOW_SUBTRACTION, 0);
	  return ER_FAILED;
	}
      break;

    case DB_TYPE_NUMERIC:
      if (numeric_db_value_sub (numeric_val_p, dbval_p, result_p) != NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_OVERFLOW_SUBTRACTION, 0);
	  return ER_FAILED;
	}
      break;

    case DB_TYPE_DOUBLE:
      return
	qdata_subtract_double (qdata_coerce_numeric_to_double
			       (numeric_val_p), DB_GET_DOUBLE (dbval_p),
			       result_p);
      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
	      pr_type_name (type), pr_type_name (DB_TYPE_DOUBLE));
      return ER_TP_CANT_COERCE;
      break;
    }

  return NO_ERROR;
}

#if defined (ENABLE_UNUSED_FUNCTION)
static int
qdata_subtract_sequence_to_dbval (DB_VALUE * seq_val_p, DB_VALUE * dbval_p,
				  DB_VALUE * result_p, TP_DOMAIN * domain_p)
{
  DB_SET *set_tmp;
#if !defined(NDEBUG)
  DB_TYPE type1, type2;
#endif

#if !defined(NDEBUG)
  type1 = DB_VALUE_DOMAIN_TYPE (seq_val_p);
  type2 = DB_VALUE_DOMAIN_TYPE (dbval_p);

  assert (TP_IS_SET_TYPE (type1));
  assert (TP_IS_SET_TYPE (type2));
#endif

  if (domain_p == NULL)
    {
      return ER_FAILED;
    }

  if (set_difference (DB_GET_SET (seq_val_p), DB_GET_SET (dbval_p),
		      &set_tmp, domain_p) < 0)
    {
      return ER_FAILED;
    }

  set_make_collection (result_p, set_tmp);
  return NO_ERROR;
}
#endif

static int
qdata_subtract_int (int i1, int i2, DB_VALUE * result_p)
{
  int itmp;

  itmp = i1 - i2;

  if (OR_CHECK_SUB_UNDERFLOW (i1, i2, itmp))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_SUBTRACTION,
	      0);
      return ER_FAILED;
    }

  DB_MAKE_INT (result_p, itmp);
  return NO_ERROR;
}

static int
qdata_subtract_bigint (DB_BIGINT bi1, DB_BIGINT bi2, DB_VALUE * result_p)
{
  DB_BIGINT bitmp;

  bitmp = bi1 - bi2;

  if (OR_CHECK_SUB_UNDERFLOW (bi1, bi2, bitmp))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_SUBTRACTION,
	      0);
      return ER_FAILED;
    }

  DB_MAKE_BIGINT (result_p, bitmp);
  return NO_ERROR;
}

static int
qdata_subtract_double (double d1, double d2, DB_VALUE * result_p)
{
  double dtmp;

  dtmp = d1 - d2;

  if (OR_CHECK_DOUBLE_OVERFLOW (dtmp))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_SUBTRACTION,
	      0);
      return ER_FAILED;
    }

  DB_MAKE_DOUBLE (result_p, dtmp);
  return NO_ERROR;
}

/*
 * qdata_multiply_dbval () -
 *   return: NO_ERROR, or ER_code
 *   dbval1(in) : First db_value node
 *   dbval2(in) : Second db_value node
 *   res(out)   : Resultant db_value node
 *
 * Note: Multiply two db_values.
 */
int
qdata_multiply_dbval (DB_VALUE * dbval1_p, DB_VALUE * dbval2_p,
		      DB_VALUE * result_p)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_dbval1, tmp_dbval2;
  DB_TYPE type1, type2;

  assert (dbval1_p != result_p);
  assert (dbval2_p != result_p);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (2, &tmp_dbval1, &tmp_dbval2);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (2, dbval1_p, dbval2_p))
    {
      goto done;
    }

  /* arg cast type **********************************************************
   */
  type1 = DB_VALUE_DOMAIN_TYPE (dbval1_p);
  type2 = DB_VALUE_DOMAIN_TYPE (dbval2_p);

  if (!TP_IS_NUMERIC_TYPE (type1))
    {
      dbval1_p =
	db_value_cast_arg (dbval1_p, &tmp_dbval1, DB_TYPE_DOUBLE,
			   &error_status);
    }

  if (!TP_IS_NUMERIC_TYPE (type2))
    {
      dbval2_p =
	db_value_cast_arg (dbval2_p, &tmp_dbval2, DB_TYPE_DOUBLE,
			   &error_status);
    }

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  type1 = DB_VALUE_DOMAIN_TYPE (dbval1_p);
  type2 = DB_VALUE_DOMAIN_TYPE (dbval2_p);

  switch (type1)
    {
    case DB_TYPE_INTEGER:
      error_status =
	qdata_multiply_int_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_BIGINT:
      error_status =
	qdata_multiply_bigint_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_DOUBLE:
      error_status =
	qdata_multiply_double_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_NUMERIC:
      error_status =
	qdata_multiply_numeric_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    default:
      assert (false);
      error_status = ER_TP_CANT_COERCE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
	      pr_type_name (type1), pr_type_name (DB_TYPE_DOUBLE));
      goto exit_on_error;
      break;
    }

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_dbval1, &tmp_dbval2);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
	      pr_type_name (type1), pr_type_name (type2));
    }

  DB_MAKE_NULL (result_p);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_dbval1, &tmp_dbval2);

  return error_status;
}

static int
qdata_multiply_int_to_dbval (DB_VALUE * int_val_p, DB_VALUE * dbval_p,
			     DB_VALUE * result_p)
{
  DB_TYPE type2;

  type2 = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type2)
    {
    case DB_TYPE_INTEGER:
      return qdata_multiply_int (int_val_p, DB_GET_INT (dbval_p), result_p);

    case DB_TYPE_BIGINT:
      return qdata_multiply_bigint (dbval_p, DB_GET_INT (int_val_p),
				    result_p);

    case DB_TYPE_DOUBLE:
      return qdata_multiply_double (DB_GET_DOUBLE (dbval_p),
				    DB_GET_INT (int_val_p), result_p);

    case DB_TYPE_NUMERIC:
      return qdata_multiply_numeric (dbval_p, int_val_p, result_p);

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
	      pr_type_name (type2), pr_type_name (DB_TYPE_DOUBLE));
      return ER_TP_CANT_COERCE;
      break;
    }

  return NO_ERROR;
}

static int
qdata_multiply_bigint_to_dbval (DB_VALUE * bigint_val_p, DB_VALUE * dbval_p,
				DB_VALUE * result_p)
{
  DB_TYPE type2;

  type2 = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type2)
    {
    case DB_TYPE_INTEGER:
      return qdata_multiply_bigint (bigint_val_p, DB_GET_INT (dbval_p),
				    result_p);

    case DB_TYPE_BIGINT:
      return qdata_multiply_bigint (bigint_val_p, DB_GET_BIGINT (dbval_p),
				    result_p);

    case DB_TYPE_DOUBLE:
      return qdata_multiply_double (DB_GET_DOUBLE (dbval_p),
				    (double) DB_GET_BIGINT (bigint_val_p),
				    result_p);

    case DB_TYPE_NUMERIC:
      return qdata_multiply_numeric (dbval_p, bigint_val_p, result_p);

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
	      pr_type_name (type2), pr_type_name (DB_TYPE_DOUBLE));
      return ER_TP_CANT_COERCE;
      break;
    }

  return NO_ERROR;
}

static int
qdata_multiply_double_to_dbval (DB_VALUE * double_val_p, DB_VALUE * dbval_p,
				DB_VALUE * result_p)
{
  double d;
  DB_TYPE type2;

  d = DB_GET_DOUBLE (double_val_p);
  type2 = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type2)

    {
    case DB_TYPE_INTEGER:
      return qdata_multiply_double (d, DB_GET_INT (dbval_p), result_p);

    case DB_TYPE_BIGINT:
      return qdata_multiply_double (d, (double) DB_GET_BIGINT (dbval_p),
				    result_p);

    case DB_TYPE_DOUBLE:
      return qdata_multiply_double (d, DB_GET_DOUBLE (dbval_p), result_p);

    case DB_TYPE_NUMERIC:
      return qdata_multiply_double (d,
				    qdata_coerce_numeric_to_double (dbval_p),
				    result_p);

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
	      pr_type_name (type2), pr_type_name (DB_TYPE_DOUBLE));
      return ER_TP_CANT_COERCE;
      break;
    }

  return NO_ERROR;
}

static int
qdata_multiply_numeric_to_dbval (DB_VALUE * numeric_val_p,
				 DB_VALUE * dbval_p, DB_VALUE * result_p)
{
  DB_TYPE type2;

  type2 = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type2)
    {
    case DB_TYPE_INTEGER:
    case DB_TYPE_BIGINT:
      return qdata_multiply_numeric (numeric_val_p, dbval_p, result_p);

    case DB_TYPE_NUMERIC:
      if (numeric_db_value_mul (numeric_val_p, dbval_p, result_p) != NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_OVERFLOW_MULTIPLICATION, 0);
	  return ER_FAILED;
	}
      break;

    case DB_TYPE_DOUBLE:
      return
	qdata_multiply_double (qdata_coerce_numeric_to_double
			       (numeric_val_p), DB_GET_DOUBLE (dbval_p),
			       result_p);

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
	      pr_type_name (type2), pr_type_name (DB_TYPE_DOUBLE));
      return ER_TP_CANT_COERCE;
      break;
    }

  return NO_ERROR;
}

static int
qdata_multiply_int (DB_VALUE * int_val_p, int i2, DB_VALUE * result_p)
{
  int i1, itmp;

  i1 = DB_GET_INT (int_val_p);
  itmp = i1 * i2;

  if (OR_CHECK_MULT_OVERFLOW (i1, i2, itmp))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_QPROC_OVERFLOW_MULTIPLICATION, 0);
      return ER_FAILED;
    }

  DB_MAKE_INT (result_p, itmp);
  return NO_ERROR;
}

static int
qdata_multiply_bigint (DB_VALUE * bigint_val_p, DB_BIGINT bi2,
		       DB_VALUE * result_p)
{
  DB_BIGINT bi1, bitmp;

  bi1 = DB_GET_BIGINT (bigint_val_p);
  bitmp = bi1 * bi2;

  if (OR_CHECK_MULT_OVERFLOW (bi1, bi2, bitmp))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_QPROC_OVERFLOW_MULTIPLICATION, 0);
      return ER_FAILED;
    }

  DB_MAKE_BIGINT (result_p, bitmp);
  return NO_ERROR;
}

static int
qdata_multiply_double (double d1, double d2, DB_VALUE * result_p)
{
  double dtmp;

  dtmp = d1 * d2;

  if (OR_CHECK_DOUBLE_OVERFLOW (dtmp))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_QPROC_OVERFLOW_MULTIPLICATION, 0);
      return ER_FAILED;
    }

  DB_MAKE_DOUBLE (result_p, dtmp);
  return NO_ERROR;
}

static int
qdata_multiply_numeric (DB_VALUE * numeric_val_p, DB_VALUE * dbval,
			DB_VALUE * result_p)
{
  DB_VALUE dbval_tmp;

  if (qdata_coerce_dbval_to_numeric (dbval, &dbval_tmp) != NO_ERROR
      || numeric_db_value_mul (numeric_val_p, &dbval_tmp,
			       result_p) != NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_QPROC_OVERFLOW_MULTIPLICATION, 0);
      return ER_FAILED;
    }

  return NO_ERROR;
}

/*
 * qdata_divide_dbval () -
 *   return: NO_ERROR, or ER_code
 *   dbval1(in) : First db_value node
 *   dbval2(in) : Second db_value node
 *   res(out)   : Resultant db_value node
 *
 * Note: Divide dbval1 by dbval2
 * Overflow checks are only done when the right operand may be
 * smaller than one.  That is,
 *     short / integer -> overflow is not checked.  Result will
 *                        always be smaller than the numerand.
 *     float / short   -> overflow is not checked.  Minimum float
 *                        representation (e-38) overflows to zero
 *                        which we want.
 *     Because of zero divide checks, most of the others will not
 *     overflow but is still being checked in case we are on a
 *     platform where DBL_EPSILON approaches the value of FLT_MIN.
 */
int
qdata_divide_dbval (DB_VALUE * dbval1_p, DB_VALUE * dbval2_p,
		    DB_VALUE * result_p)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_dbval1, tmp_dbval2;
  DB_TYPE type1, type2;

  assert (dbval1_p != result_p);
  assert (dbval2_p != result_p);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (2, &tmp_dbval1, &tmp_dbval2);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (2, dbval1_p, dbval2_p))
    {
      goto done;
    }

  /* arg cast type **********************************************************
   */
  type1 = DB_VALUE_DOMAIN_TYPE (dbval1_p);
  type2 = DB_VALUE_DOMAIN_TYPE (dbval2_p);

  if (!TP_IS_NUMERIC_TYPE (type1))
    {
      dbval1_p =
	db_value_cast_arg (dbval1_p, &tmp_dbval1, DB_TYPE_DOUBLE,
			   &error_status);
    }

  if (!TP_IS_NUMERIC_TYPE (type2))
    {
      dbval2_p =
	db_value_cast_arg (dbval2_p, &tmp_dbval2, DB_TYPE_DOUBLE,
			   &error_status);
    }

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  type1 = DB_VALUE_DOMAIN_TYPE (dbval1_p);
  type2 = DB_VALUE_DOMAIN_TYPE (dbval2_p);

  if (qdata_is_divided_zero (dbval2_p))
    {
      error_status = ER_QPROC_ZERO_DIVIDE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  switch (type1)
    {
    case DB_TYPE_INTEGER:
      error_status = qdata_divide_int_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_BIGINT:
      error_status =
	qdata_divide_bigint_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_DOUBLE:
      error_status =
	qdata_divide_double_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    case DB_TYPE_NUMERIC:
      error_status =
	qdata_divide_numeric_to_dbval (dbval1_p, dbval2_p, result_p);
      break;

    default:
      assert (false);
      error_status = ER_TP_CANT_COERCE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
	      pr_type_name (type1), pr_type_name (DB_TYPE_DOUBLE));
      goto exit_on_error;
      break;
    }

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_dbval1, &tmp_dbval2);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
	      pr_type_name (type1), pr_type_name (type2));
    }

  DB_MAKE_NULL (result_p);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_dbval1, &tmp_dbval2);

  return error_status;
}

static bool
qdata_is_divided_zero (DB_VALUE * dbval_p)
{
  DB_TYPE type;

  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_INTEGER:
      return DB_GET_INT (dbval_p) == 0;

    case DB_TYPE_BIGINT:
      return DB_GET_BIGINT (dbval_p) == 0;

    case DB_TYPE_DOUBLE:
      return fabs (DB_GET_DOUBLE (dbval_p)) <= DBL_EPSILON;

    case DB_TYPE_NUMERIC:
      return numeric_db_value_is_zero (dbval_p);

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
	      pr_type_name (type), pr_type_name (DB_TYPE_DOUBLE));
      break;
    }

  return false;
}

static int
qdata_divide_int_to_dbval (DB_VALUE * int_val_p, DB_VALUE * dbval_p,
			   DB_VALUE * result_p)
{
  int i;
  DB_TYPE type2;
  DB_VALUE dbval_tmp;

  i = DB_GET_INT (int_val_p);
  type2 = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type2)
    {
    case DB_TYPE_INTEGER:
      return qdata_divide_int (i, DB_GET_INT (dbval_p), result_p);

    case DB_TYPE_BIGINT:
      return qdata_divide_bigint (i, DB_GET_BIGINT (dbval_p), result_p);

    case DB_TYPE_DOUBLE:
      return qdata_divide_double (i, DB_GET_DOUBLE (dbval_p), result_p, true);

    case DB_TYPE_NUMERIC:
      if (qdata_coerce_dbval_to_numeric (int_val_p, &dbval_tmp) != NO_ERROR
	  || numeric_db_value_div (&dbval_tmp, dbval_p, result_p) != NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_OVERFLOW_DIVISION, 0);
	  return ER_FAILED;
	}
      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
	      pr_type_name (type2), pr_type_name (DB_TYPE_DOUBLE));
      return ER_TP_CANT_COERCE;
      break;
    }

  return NO_ERROR;
}

static int
qdata_divide_bigint_to_dbval (DB_VALUE * bigint_val_p, DB_VALUE * dbval_p,
			      DB_VALUE * result_p)
{
  DB_BIGINT bi;
  DB_TYPE type2;
  DB_VALUE dbval_tmp;

  bi = DB_GET_BIGINT (bigint_val_p);
  type2 = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type2)
    {
    case DB_TYPE_INTEGER:
      return qdata_divide_bigint (bi, DB_GET_INT (dbval_p), result_p);

    case DB_TYPE_BIGINT:
      return qdata_divide_bigint (bi, DB_GET_BIGINT (dbval_p), result_p);

    case DB_TYPE_DOUBLE:
      return qdata_divide_double ((double) bi, DB_GET_DOUBLE (dbval_p),
				  result_p, true);

    case DB_TYPE_NUMERIC:
      if (qdata_coerce_dbval_to_numeric (bigint_val_p, &dbval_tmp) != NO_ERROR
	  || numeric_db_value_div (&dbval_tmp, dbval_p, result_p) != NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_OVERFLOW_DIVISION, 0);
	  return ER_FAILED;
	}
      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
	      pr_type_name (type2), pr_type_name (DB_TYPE_DOUBLE));
      return ER_TP_CANT_COERCE;
      break;
    }

  return NO_ERROR;
}

static int
qdata_divide_double_to_dbval (DB_VALUE * double_val_p, DB_VALUE * dbval_p,
			      DB_VALUE * result_p)
{
  double d;
  DB_TYPE type2;

  d = DB_GET_DOUBLE (double_val_p);
  type2 = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type2)
    {
    case DB_TYPE_INTEGER:
      return qdata_divide_double (d, DB_GET_INT (dbval_p), result_p, false);

    case DB_TYPE_BIGINT:
      return qdata_divide_double (d, (double) DB_GET_BIGINT (dbval_p),
				  result_p, false);

    case DB_TYPE_DOUBLE:
      return qdata_divide_double (d, DB_GET_DOUBLE (dbval_p), result_p, true);

    case DB_TYPE_NUMERIC:
      return qdata_divide_double (d,
				  qdata_coerce_numeric_to_double (dbval_p),
				  result_p, false);

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
	      pr_type_name (type2), pr_type_name (DB_TYPE_DOUBLE));
      return ER_TP_CANT_COERCE;
      break;
    }

  return NO_ERROR;
}

static int
qdata_divide_numeric_to_dbval (DB_VALUE * numeric_val_p, DB_VALUE * dbval_p,
			       DB_VALUE * result_p)
{
  DB_TYPE type2;
  DB_VALUE dbval_tmp;

  type2 = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type2)
    {
    case DB_TYPE_INTEGER:
    case DB_TYPE_BIGINT:
      if (qdata_coerce_dbval_to_numeric (dbval_p, &dbval_tmp) != NO_ERROR
	  || numeric_db_value_div (numeric_val_p, &dbval_tmp,
				   result_p) != NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_OVERFLOW_DIVISION, 0);
	  return ER_FAILED;
	}
      break;

    case DB_TYPE_NUMERIC:
      if (numeric_db_value_div (numeric_val_p, dbval_p, result_p) != NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_QPROC_OVERFLOW_DIVISION, 0);
	  return ER_FAILED;
	}
      break;

    case DB_TYPE_DOUBLE:
      return
	qdata_divide_double (qdata_coerce_numeric_to_double (numeric_val_p),
			     DB_GET_DOUBLE (dbval_p), result_p, true);

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
	      pr_type_name (type2), pr_type_name (DB_TYPE_DOUBLE));
      return ER_TP_CANT_COERCE;
      break;
    }

  return NO_ERROR;
}

static int
qdata_divide_int (int i1, int i2, DB_VALUE * result_p)
{
  int itmp;

  itmp = i1 / i2;
  DB_MAKE_INT (result_p, itmp);

  return NO_ERROR;
}

static int
qdata_divide_bigint (DB_BIGINT bi1, DB_BIGINT bi2, DB_VALUE * result_p)
{
  DB_BIGINT bitmp;

  bitmp = bi1 / bi2;
  DB_MAKE_BIGINT (result_p, bitmp);

  return NO_ERROR;
}

static int
qdata_divide_double (double d1, double d2, DB_VALUE * result_p,
		     bool is_check_overflow)
{
  double dtmp;

  dtmp = d1 / d2;

  if (is_check_overflow && OR_CHECK_DOUBLE_OVERFLOW (dtmp))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_OVERFLOW_DIVISION,
	      0);
      return ER_FAILED;
    }

  DB_MAKE_DOUBLE (result_p, dtmp);
  return NO_ERROR;
}

/*
 * qdata_bit_not_dbval () - bitwise not
 *   return: NO_ERROR, or ER_code
 *   dbval_p(in) : db_value node
 *   result_p(out) : resultant db_value node
 *
 */
int
qdata_bit_not_dbval (DB_VALUE * dbval_p, DB_VALUE * result_p)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_dbval;
  DB_TYPE type;

  assert (dbval_p != result_p);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (1, &tmp_dbval);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (1, dbval_p))
    {
      DB_MAKE_NULL (result_p);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  type = DB_VALUE_DOMAIN_TYPE (dbval_p);
  if (!(type == DB_TYPE_INTEGER || type == DB_TYPE_BIGINT))
    {
      dbval_p =
	db_value_cast_arg (dbval_p, &tmp_dbval, DB_TYPE_BIGINT,
			   &error_status);
    }

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (type)
    {
    case DB_TYPE_INTEGER:
      db_make_bigint (result_p, ~((INT64) DB_GET_INTEGER (dbval_p)));
      break;

    case DB_TYPE_BIGINT:
      db_make_bigint (result_p, ~DB_GET_BIGINT (dbval_p));
      break;

    default:
      assert (false);
      error_status = ER_TP_CANT_COERCE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
	      pr_type_name (type), pr_type_name (DB_TYPE_BIGINT));
      goto exit_on_error;
      break;
    }

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_dbval);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (result_p);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_dbval);

  return error_status;
}

/*
 * qdata_bit_and_dbval () - bitwise and
 *   return: NO_ERROR, or ER_code
 *   dbval1_p(in) : first db_value node
 *   dbval2_p(in) : second db_value node
 *   result_p(out) : resultant db_value node
 *
 */
int
qdata_bit_and_dbval (DB_VALUE * dbval1_p, DB_VALUE * dbval2_p,
		     DB_VALUE * result_p)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_dbval1, tmp_dbval2;
  DB_TYPE dbval_type, type[2];
  DB_BIGINT bi[2];
  DB_VALUE *dbval[2];
  int i;

  assert (dbval1_p != result_p);
  assert (dbval2_p != result_p);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (2, &tmp_dbval1, &tmp_dbval2);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (2, dbval1_p, dbval2_p))
    {
      DB_MAKE_NULL (result_p);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  dbval_type = DB_VALUE_DOMAIN_TYPE (dbval1_p);
  if (!(dbval_type == DB_TYPE_INTEGER || dbval_type == DB_TYPE_BIGINT))
    {
      dbval1_p =
	db_value_cast_arg (dbval1_p, &tmp_dbval1, DB_TYPE_BIGINT,
			   &error_status);
    }

  dbval_type = DB_VALUE_DOMAIN_TYPE (dbval2_p);
  if (!(dbval_type == DB_TYPE_INTEGER || dbval_type == DB_TYPE_BIGINT))
    {
      dbval2_p =
	db_value_cast_arg (dbval2_p, &tmp_dbval2, DB_TYPE_BIGINT,
			   &error_status);
    }

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  type[0] = DB_VALUE_DOMAIN_TYPE (dbval1_p);
  type[1] = DB_VALUE_DOMAIN_TYPE (dbval2_p);

  dbval[0] = dbval1_p;
  dbval[1] = dbval2_p;

  for (i = 0; i < 2; i++)
    {
      switch (type[i])
	{
	case DB_TYPE_INTEGER:
	  bi[i] = (DB_BIGINT) DB_GET_INTEGER (dbval[i]);
	  break;

	case DB_TYPE_BIGINT:
	  bi[i] = DB_GET_BIGINT (dbval[i]);
	  break;

	default:
	  assert (false);
	  error_status = ER_TP_CANT_COERCE;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
		  pr_type_name (type[i]), pr_type_name (DB_TYPE_BIGINT));
	  goto exit_on_error;
	  break;
	}
    }

  if (type[0] != DB_TYPE_NULL && type[1] != DB_TYPE_NULL)
    {
      db_make_bigint (result_p, bi[0] & bi[1]);
    }

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_dbval1, &tmp_dbval2);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (result_p);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_dbval1, &tmp_dbval2);

  return error_status;
}

/*
 * qdata_bit_or_dbval () - bitwise or
 *   return: NO_ERROR, or ER_code
 *   dbval1_p(in) : first db_value node
 *   dbval2_p(in) : second db_value node
 *   result_p(out) : resultant db_value node
 *
 */
int
qdata_bit_or_dbval (DB_VALUE * dbval1_p, DB_VALUE * dbval2_p,
		    DB_VALUE * result_p)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_dbval1, tmp_dbval2;
  DB_TYPE dbval_type, type[2];
  DB_BIGINT bi[2];
  DB_VALUE *dbval[2];
  int i;

  assert (dbval1_p != result_p);
  assert (dbval2_p != result_p);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (2, &tmp_dbval1, &tmp_dbval2);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (2, dbval1_p, dbval2_p))
    {
      DB_MAKE_NULL (result_p);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  dbval_type = DB_VALUE_DOMAIN_TYPE (dbval1_p);
  if (!(dbval_type == DB_TYPE_INTEGER || dbval_type == DB_TYPE_BIGINT))
    {
      dbval1_p =
	db_value_cast_arg (dbval1_p, &tmp_dbval1, DB_TYPE_BIGINT,
			   &error_status);
    }

  dbval_type = DB_VALUE_DOMAIN_TYPE (dbval2_p);
  if (!(dbval_type == DB_TYPE_INTEGER || dbval_type == DB_TYPE_BIGINT))
    {
      dbval2_p =
	db_value_cast_arg (dbval2_p, &tmp_dbval2, DB_TYPE_BIGINT,
			   &error_status);
    }

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  type[0] = DB_VALUE_DOMAIN_TYPE (dbval1_p);
  type[1] = DB_VALUE_DOMAIN_TYPE (dbval2_p);

  dbval[0] = dbval1_p;
  dbval[1] = dbval2_p;

  for (i = 0; i < 2; i++)
    {
      switch (type[i])
	{
	case DB_TYPE_INTEGER:
	  bi[i] = (DB_BIGINT) DB_GET_INTEGER (dbval[i]);
	  break;

	case DB_TYPE_BIGINT:
	  bi[i] = DB_GET_BIGINT (dbval[i]);
	  break;

	default:
	  assert (false);
	  error_status = ER_TP_CANT_COERCE;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
		  pr_type_name (type[i]), pr_type_name (DB_TYPE_BIGINT));
	  goto exit_on_error;
	  break;
	}
    }

  if (type[0] != DB_TYPE_NULL && type[1] != DB_TYPE_NULL)
    {
      db_make_bigint (result_p, bi[0] | bi[1]);
    }

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_dbval1, &tmp_dbval2);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (result_p);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_dbval1, &tmp_dbval2);

  return error_status;
}

/*
 * qdata_bit_xor_dbval () - bitwise xor
 *   return: NO_ERROR, or ER_code
 *   dbval1_p(in) : first db_value node
 *   dbval2_p(in) : second db_value node
 *   result_p(out) : resultant db_value node
 *
 */
int
qdata_bit_xor_dbval (DB_VALUE * dbval1_p, DB_VALUE * dbval2_p,
		     DB_VALUE * result_p)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_dbval1, tmp_dbval2;
  DB_TYPE dbval_type, type[2];
  DB_BIGINT bi[2];
  DB_VALUE *dbval[2];
  int i;

  assert (dbval1_p != result_p);
  assert (dbval2_p != result_p);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (2, &tmp_dbval1, &tmp_dbval2);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (2, dbval1_p, dbval2_p))
    {
      DB_MAKE_NULL (result_p);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  dbval_type = DB_VALUE_DOMAIN_TYPE (dbval1_p);
  if (!(dbval_type == DB_TYPE_INTEGER || dbval_type == DB_TYPE_BIGINT))
    {
      dbval1_p =
	db_value_cast_arg (dbval1_p, &tmp_dbval1, DB_TYPE_BIGINT,
			   &error_status);
    }

  dbval_type = DB_VALUE_DOMAIN_TYPE (dbval2_p);
  if (!(dbval_type == DB_TYPE_INTEGER || dbval_type == DB_TYPE_BIGINT))
    {
      dbval2_p =
	db_value_cast_arg (dbval2_p, &tmp_dbval2, DB_TYPE_BIGINT,
			   &error_status);
    }

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  type[0] = DB_VALUE_DOMAIN_TYPE (dbval1_p);
  type[1] = DB_VALUE_DOMAIN_TYPE (dbval2_p);

  dbval[0] = dbval1_p;
  dbval[1] = dbval2_p;

  for (i = 0; i < 2; i++)
    {
      switch (type[i])
	{
	case DB_TYPE_INTEGER:
	  bi[i] = (DB_BIGINT) DB_GET_INTEGER (dbval[i]);
	  break;

	case DB_TYPE_BIGINT:
	  bi[i] = DB_GET_BIGINT (dbval[i]);
	  break;

	default:
	  assert (false);
	  error_status = ER_TP_CANT_COERCE;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
		  pr_type_name (type[i]), pr_type_name (DB_TYPE_BIGINT));
	  goto exit_on_error;
	  break;
	}
    }

  if (type[0] != DB_TYPE_NULL && type[1] != DB_TYPE_NULL)
    {
      db_make_bigint (result_p, bi[0] ^ bi[1]);
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_dbval1, &tmp_dbval2);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (result_p);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_dbval1, &tmp_dbval2);

  return error_status;
}

/*
 * qdata_bit_shift_dbval () - bitshift
 *   return: NO_ERROR, or ER_code
 *   dbval1_p(in) : first db_value node
 *   dbval2_p(in) : second db_value node
 *   result_p(out) : resultant db_value node
 *
 */
int
qdata_bit_shift_dbval (DB_VALUE * dbval1_p, DB_VALUE * dbval2_p,
		       OPERATOR_TYPE op, DB_VALUE * result_p)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_dbval1, tmp_dbval2;
  DB_TYPE dbval_type, type[2];
  DB_BIGINT bi[2];
  DB_VALUE *dbval[2];
  int i;

  assert (dbval1_p != result_p);
  assert (dbval2_p != result_p);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (2, &tmp_dbval1, &tmp_dbval2);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (2, dbval1_p, dbval2_p))
    {
      DB_MAKE_NULL (result_p);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  dbval_type = DB_VALUE_DOMAIN_TYPE (dbval1_p);
  if (!(dbval_type == DB_TYPE_INTEGER || dbval_type == DB_TYPE_BIGINT))
    {
      dbval1_p =
	db_value_cast_arg (dbval1_p, &tmp_dbval1, DB_TYPE_BIGINT,
			   &error_status);
    }

  dbval_type = DB_VALUE_DOMAIN_TYPE (dbval2_p);
  if (!(dbval_type == DB_TYPE_INTEGER || dbval_type == DB_TYPE_BIGINT))
    {
      dbval2_p =
	db_value_cast_arg (dbval2_p, &tmp_dbval2, DB_TYPE_BIGINT,
			   &error_status);
    }

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  type[0] = DB_VALUE_DOMAIN_TYPE (dbval1_p);
  type[1] = DB_VALUE_DOMAIN_TYPE (dbval2_p);

  dbval[0] = dbval1_p;
  dbval[1] = dbval2_p;

  for (i = 0; i < 2; i++)
    {
      switch (type[i])
	{
	case DB_TYPE_INTEGER:
	  bi[i] = (DB_BIGINT) DB_GET_INTEGER (dbval[i]);
	  break;

	case DB_TYPE_BIGINT:
	  bi[i] = DB_GET_BIGINT (dbval[i]);
	  break;

	default:
	  assert (false);
	  error_status = ER_TP_CANT_COERCE;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
		  pr_type_name (type[i]), pr_type_name (DB_TYPE_BIGINT));
	  goto exit_on_error;
	  break;
	}
    }

  if (type[0] != DB_TYPE_NULL && type[1] != DB_TYPE_NULL)
    {
      if (bi[1] < (int) (sizeof (DB_BIGINT) * 8) && bi[1] >= 0)
	{
	  if (op == T_BITSHIFT_LEFT)
	    {
	      db_make_bigint (result_p, ((UINT64) bi[0]) << ((UINT64) bi[1]));
	    }
	  else
	    {
	      db_make_bigint (result_p, ((UINT64) bi[0]) >> ((UINT64) bi[1]));
	    }
	}
      else
	{
	  db_make_bigint (result_p, 0);
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
  db_value_clear_nargs (2, &tmp_dbval1, &tmp_dbval2);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (result_p);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_dbval1, &tmp_dbval2);

  return error_status;
}

/*
 * qdata_divmod_dbval () - DIV operator
 *   return: NO_ERROR, or ER_code
 *   dbval1_p(in) : first db_value node
 *   dbval2_p(in) : second db_value node
 *   result_p(out) : resultant db_value node
 *
 */
int
qdata_divmod_dbval (DB_VALUE * dbval1_p, DB_VALUE * dbval2_p,
		    OPERATOR_TYPE op, DB_VALUE * result_p)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_dbval1, tmp_dbval2;
  DB_TYPE dbval_type, type[2];
  DB_BIGINT bi[2];
  DB_VALUE *dbval[2];
  int i;

  assert (dbval1_p != result_p);
  assert (dbval2_p != result_p);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (2, &tmp_dbval1, &tmp_dbval2);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (2, dbval1_p, dbval2_p))
    {
      DB_MAKE_NULL (result_p);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  dbval_type = DB_VALUE_DOMAIN_TYPE (dbval1_p);
  if (!(dbval_type == DB_TYPE_INTEGER || dbval_type == DB_TYPE_BIGINT))
    {
      dbval1_p =
	db_value_cast_arg (dbval1_p, &tmp_dbval1, DB_TYPE_BIGINT,
			   &error_status);
    }

  dbval_type = DB_VALUE_DOMAIN_TYPE (dbval2_p);
  if (!(dbval_type == DB_TYPE_INTEGER || dbval_type == DB_TYPE_BIGINT))
    {
      dbval2_p =
	db_value_cast_arg (dbval2_p, &tmp_dbval2, DB_TYPE_BIGINT,
			   &error_status);
    }

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  type[0] = DB_VALUE_DOMAIN_TYPE (dbval1_p);
  type[1] = DB_VALUE_DOMAIN_TYPE (dbval2_p);

  dbval[0] = dbval1_p;
  dbval[1] = dbval2_p;

  for (i = 0; i < 2; i++)
    {
      switch (type[i])
	{
	case DB_TYPE_INTEGER:
	  bi[i] = (DB_BIGINT) DB_GET_INTEGER (dbval[i]);
	  break;

	case DB_TYPE_BIGINT:
	  bi[i] = DB_GET_BIGINT (dbval[i]);
	  break;

	default:
	  assert (false);
	  error_status = ER_TP_CANT_COERCE;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
		  pr_type_name (type[i]), pr_type_name (DB_TYPE_BIGINT));
	  goto exit_on_error;
	  break;
	}
    }

  if (type[0] != DB_TYPE_NULL && type[1] != DB_TYPE_NULL)
    {
      if (bi[1] == 0)
	{
	  error_status = ER_QPROC_ZERO_DIVIDE;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto exit_on_error;
	}

      assert (op == T_INTDIV);
      if (op == T_INTDIV)
	{
	  if (type[0] == DB_TYPE_INTEGER)
	    {
	      if (OR_CHECK_INT_DIV_OVERFLOW (bi[0], bi[1]))
		{
		  error_status = ER_QPROC_OVERFLOW_HAPPENED;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  goto exit_on_error;
		}
	      db_make_int (result_p, (INT32) (bi[0] / bi[1]));
	    }
	  else
	    {
	      assert (type[0] == DB_TYPE_BIGINT);
	      if (OR_CHECK_BIGINT_DIV_OVERFLOW (bi[0], bi[1]))
		{
		  error_status = ER_QPROC_OVERFLOW_HAPPENED;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  goto exit_on_error;
		}
	      db_make_bigint (result_p, bi[0] / bi[1]);
	    }
	}
#if 0
      else
	{
	  if (type[0] == DB_TYPE_INTEGER)
	    {
	      db_make_int (result_p, (INT32) (bi[0] % bi[1]));
	    }
	  else
	    {
	      assert (type[0] == DB_TYPE_BIGINT);
	      db_make_bigint (result_p, bi[0] % bi[1]);
	    }
	}
#endif
    }

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_dbval1, &tmp_dbval2);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (result_p);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_dbval1, &tmp_dbval2);

  return error_status;
}

/*
 * qdata_unary_minus_dbval () -
 *   return: NO_ERROR, or ER_code
 *   res(out)   : Resultant db_value node
 *   dbval1(in) : First db_value node
 *
 * Note: Take unary minus of db_value.
 */
int
qdata_unary_minus_dbval (DB_VALUE * result_p, DB_VALUE * dbval_p)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_dbval;
  DB_TYPE res_type;
  int itmp;
  DB_BIGINT bitmp;
  DB_VALUE cast_value;

  assert (result_p != dbval_p);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (1, &tmp_dbval);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (1, dbval_p))
    {
      goto done;
    }

  /* arg cast type **********************************************************
   */
  res_type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  if (!TP_IS_NUMERIC_TYPE (res_type))
    {
      dbval_p =
	db_value_cast_arg (dbval_p, &tmp_dbval, DB_TYPE_DOUBLE,
			   &error_status);
    }

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  res_type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (res_type)
    {
    case DB_TYPE_INTEGER:
      itmp = DB_GET_INT (dbval_p);
      if (itmp == INT_MIN)
	{
	  error_status = ER_QPROC_OVERFLOW_UMINUS;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto exit_on_error;
	}
      DB_MAKE_INT (result_p, (-1) * itmp);
      break;

    case DB_TYPE_BIGINT:
      bitmp = DB_GET_BIGINT (dbval_p);
      if (bitmp == DB_BIGINT_MIN)
	{
	  error_status = ER_QPROC_OVERFLOW_UMINUS;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto exit_on_error;
	}
      DB_MAKE_BIGINT (result_p, (-1) * bitmp);
      break;

    case DB_TYPE_VARCHAR:
      error_status = tp_value_str_cast_to_number (dbval_p, &cast_value,
						  &res_type);
      if (error_status != NO_ERROR)
	{
	  goto exit_on_error;
	}

      assert (res_type == DB_TYPE_DOUBLE);

      dbval_p = &cast_value;

      /* fall through */

    case DB_TYPE_DOUBLE:
      DB_MAKE_DOUBLE (result_p, (-1) * DB_GET_DOUBLE (dbval_p));
      break;

    case DB_TYPE_NUMERIC:
      error_status = DB_MAKE_NUMERIC (result_p,
				      DB_GET_NUMERIC (dbval_p),
				      DB_VALUE_PRECISION (dbval_p),
				      DB_VALUE_SCALE (dbval_p));
      if (error_status != NO_ERROR)
	{
	  goto exit_on_error;
	}
      error_status = numeric_db_value_negate (result_p);
      if (error_status != NO_ERROR)
	{
	  goto exit_on_error;
	}
      break;

    default:
      assert (false);
      error_status = ER_TP_CANT_COERCE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
	      pr_type_name (res_type), pr_type_name (DB_TYPE_DOUBLE));
      goto exit_on_error;
      break;
    }

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_dbval);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
	      pr_type_name (res_type), pr_type_name (DB_TYPE_DOUBLE));
    }

  DB_MAKE_NULL (result_p);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_dbval);

  return error_status;
}

/*
 * qdata_extract_dbval () -
 *   return: NO_ERROR, or ER_code
 *   extr_operand(in)   : Specifies datetime field to be extracted
 *   dbval(in)  : Extract source db_value node
 *   res(out)   : Resultant db_value node
 *
 * Note: Extract a datetime field from db_value.
 */
int
qdata_extract_dbval (const MISC_OPERAND extr_operand,
		     DB_VALUE * dbval_p, DB_VALUE * result_p)
{
  DB_TYPE dbval_type;
  DB_DATE date;
  DB_TIME time;
  DB_DATETIME *datetime;
  int extvar[NUM_MISC_OPERANDS];

  assert (dbval_p != result_p);

  if (DB_IS_NULL (dbval_p))
    {
      return NO_ERROR;
    }

  dbval_type = DB_VALUE_DOMAIN_TYPE (dbval_p);

  switch (dbval_type)
    {
    case DB_TYPE_TIME:
      time = *DB_GET_TIME (dbval_p);
      db_time_decode (&time, &extvar[HOUR], &extvar[MINUTE], &extvar[SECOND]);

      switch (extr_operand)
	{
	case HOUR:
	case MINUTE:
	case SECOND:
	  ;			/* OK */
	  break;

	default:
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OBJ_INVALID_ARGUMENTS, 0);
	  return ER_FAILED;
	}

      break;

    case DB_TYPE_DATE:
      date = *DB_GET_DATE (dbval_p);
      db_date_decode (&date, &extvar[MONTH], &extvar[DAY], &extvar[YEAR]);

      switch (extr_operand)
	{
	case YEAR:
	case MONTH:
	case DAY:
	  ;			/* OK */
	  break;

	default:
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OBJ_INVALID_ARGUMENTS, 0);
	  return ER_FAILED;
	}

      break;

    case DB_TYPE_DATETIME:
      datetime = DB_GET_DATETIME (dbval_p);
      db_datetime_decode (datetime, &extvar[MONTH], &extvar[DAY],
			  &extvar[YEAR], &extvar[HOUR], &extvar[MINUTE],
			  &extvar[SECOND], &extvar[MILLISECOND]);

      switch (extr_operand)
	{
	case YEAR:
	case MONTH:
	case DAY:
	case HOUR:
	case MINUTE:
	case SECOND:
	case MILLISECOND:
	  ;			/* OK */
	  break;

	default:
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OBJ_INVALID_ARGUMENTS, 0);
	  return ER_FAILED;
	}

      break;

    case DB_TYPE_VARCHAR:
      {
	DB_DATETIME datetime_s;
	char *str_date = DB_PULL_STRING (dbval_p);
	int str_date_len = DB_GET_STRING_SIZE (dbval_p);

	switch (extr_operand)
	  {
	  case YEAR:
	  case MONTH:
	  case DAY:
	    if (db_string_to_date_ex (str_date, str_date_len, &date)
		== NO_ERROR)
	      {
		db_date_decode (&date, &extvar[MONTH], &extvar[DAY],
				&extvar[YEAR]);
		break;
	      }
	    if (db_string_to_datetime_ex (str_date, str_date_len, &datetime_s)
		== NO_ERROR)
	      {
		db_datetime_decode (&datetime_s, &extvar[MONTH],
				    &extvar[DAY], &extvar[YEAR],
				    &extvar[HOUR], &extvar[MINUTE],
				    &extvar[SECOND], &extvar[MILLISECOND]);
		break;
	      }
	    /* no date/time can be extracted from string, error */
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
		    pr_type_name (DB_TYPE_VARCHAR),
		    pr_type_name (DB_TYPE_DATETIME));
	    return ER_FAILED;

	  case HOUR:
	  case MINUTE:
	  case SECOND:
	    if (db_string_to_time_ex (str_date, str_date_len, &time)
		== NO_ERROR)
	      {
		db_time_decode (&time, &extvar[HOUR], &extvar[MINUTE],
				&extvar[SECOND]);
		break;
	      }
	    /* fall through */
	  case MILLISECOND:
	    if (db_string_to_datetime_ex (str_date, str_date_len, &datetime_s)
		== NO_ERROR)
	      {
		db_datetime_decode (&datetime_s, &extvar[MONTH], &extvar[DAY],
				    &extvar[YEAR], &extvar[HOUR],
				    &extvar[MINUTE],
				    &extvar[SECOND], &extvar[MILLISECOND]);
		break;
	      }
	    /* no date/time can be extracted from string, error */
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_TP_CANT_COERCE, 2,
		    pr_type_name (DB_TYPE_VARCHAR),
		    pr_type_name (DB_TYPE_DATETIME));
	    return ER_FAILED;

	  default:
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		    ER_OBJ_INVALID_ARGUMENTS, 0);
	    return ER_FAILED;
	  }
      }
      break;

    default:
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
      return ER_FAILED;
    }

  DB_MAKE_INT (result_p, extvar[extr_operand]);

  return NO_ERROR;
}

/*
 * qdata_strcat_dbval () -
 *   return:
 *   dbval1_p(in) :
 *   dbval2_p(in) :
 *   result_p(in)    :
 */
int
qdata_strcat_dbval (DB_VALUE * dbval1_p, DB_VALUE * dbval2_p,
		    DB_VALUE * result_p)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_dbval1, tmp_dbval2;

  assert (dbval1_p != result_p);
  assert (dbval2_p != result_p);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (2, &tmp_dbval1, &tmp_dbval2);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (2, dbval1_p, dbval2_p))
    {
      goto done;
    }

  dbval1_p =
    db_value_cast_arg (dbval1_p, &tmp_dbval1, DB_TYPE_VARCHAR, &error_status);
  dbval2_p =
    db_value_cast_arg (dbval2_p, &tmp_dbval2, DB_TYPE_VARCHAR, &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  assert (DB_VALUE_DOMAIN_TYPE (dbval1_p) == DB_TYPE_VARCHAR);
  assert (DB_VALUE_DOMAIN_TYPE (dbval2_p) == DB_TYPE_VARCHAR);

  error_status = qdata_add_chars_to_dbval (dbval1_p, dbval2_p, result_p);

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);


  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_dbval1, &tmp_dbval2);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (result_p);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_dbval1, &tmp_dbval2);

  return error_status;
}

int
qdata_add_chars_to_dbval (DB_VALUE * dbval1_p, DB_VALUE * dbval2_p,
			  DB_VALUE * result_p)
{
  DB_DATA_STATUS data_stat;

  if ((db_string_concatenate (dbval1_p, dbval2_p, result_p,
			      &data_stat) != NO_ERROR)
      || (data_stat != DATA_STATUS_OK))
    {
      return ER_FAILED;
    }

  return NO_ERROR;
}
