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

/* AM/PM position references */
enum
{ am_NAME = 0, pm_NAME, Am_NAME, Pm_NAME, AM_NAME, PM_NAME,
  a_m_NAME, p_m_NAME, A_m_NAME, P_m_NAME, A_M_NAME, P_M_NAME
};

/*
 * Number format
 */
typedef enum
{
  N_END = -2,			/*format string end */
  N_INVALID = -1,		/* invalid format */
  N_FORMAT,
  N_SPACE,
  N_TEXT
} NUMBER_FORMAT;

typedef enum
{
  SDT_DAY = 0,
  SDT_MONTH,
  SDT_DAY_SHORT,
  SDT_MONTH_SHORT,
  SDT_AM_PM
} STRING_DATE_TOKEN;

#define WHITE_CHARS             " \t\n"

#define QSTR_DATE_LENGTH 10	/* yyyy-mm-dd */
#define QSTR_TIME_LENGTH 8	/* hh:mm:ss */
#if defined (ENABLE_UNUSED_FUNCTION)
#define QSTR_TIME_STAMPLENGTH 22
#endif
#define QSTR_DATETIME_LENGTH 23	/* yyyy-mm-dd hh:mm:ss.sss */
/* multiplier ratio for TO_CHAR function : estimate result len/size based on
 * format string len/size : maximum multiplier is given by:
 * - format element : DAY (3)
 * - result :Wednesday (9) */
#define QSTR_TO_CHAR_LEN_MULTIPLIER_RATIO LOC_PARSE_FRMT_TO_TOKEN_MULT

#define MAX_TOKEN_SIZE 16000

static int date_to_char (const DB_VALUE * src_value,
			 const DB_VALUE * format_str,
			 const DB_VALUE * date_lang, DB_VALUE * result_str);
static int number_to_char (const DB_VALUE * src_value,
			   const DB_VALUE * format_str,
			   const DB_VALUE * number_lang,
			   DB_VALUE * result_str);
static int make_number_to_char (const INTL_LANG lang, char *num_string,
				char *format_str, int *length,
				char **result_str);
static int make_scientific_notation (char *src_string, int cipher);
static int roundoff (const INTL_LANG lang, char *src_string, int flag,
		     int *cipher, char *format);
static int scientific_to_decimal_string (const INTL_LANG lang,
					 char *src_string,
					 char **scientific_str);
static int to_number_next_state (const int previous_state,
				 const int input_char,
				 const INTL_LANG number_lang_id);
static int make_number (char *src, char *last_src,
			char *token, int *token_length, DB_VALUE * r,
			const int precision, const int scale,
			const INTL_LANG number_lang_id);
static int get_number_token (const INTL_LANG lang, char *fsp, int *length,
			     char *last_position, char **next_fsp);
static int get_next_format (char *sp, DB_TYPE str_type, int *format_length,
			    char **next_pos);
static int get_cur_year (void);
#if defined (ENABLE_UNUSED_FUNCTION)
static int get_cur_month (void);
#endif
/* utility functions */
static int add_and_normalize_date_time (int *years,
					int *months,
					int *days,
					int *hours,
					int *minutes,
					int *seconds,
					int *milliseconds,
					DB_BIGINT y,
					DB_BIGINT m,
					DB_BIGINT d,
					DB_BIGINT h,
					DB_BIGINT mi,
					DB_BIGINT s, DB_BIGINT ms);
static int sub_and_normalize_date_time (int *years,
					int *months,
					int *days,
					int *hours,
					int *minutes,
					int *seconds,
					int *milliseconds,
					DB_BIGINT y,
					DB_BIGINT m,
					DB_BIGINT d,
					DB_BIGINT h,
					DB_BIGINT mi,
					DB_BIGINT s, DB_BIGINT ms);
static int db_check_or_create_null_term_string (const DB_VALUE * str_val,
						char *pre_alloc_buf,
						int pre_alloc_buf_size,
						bool ignore_prec_spaces,
						bool ignore_trail_spaces,
						char **str_out,
						bool * do_alloc);

/* reads cnt digits until non-digit char reached,
 * returns nr of characters traversed
 */
static int parse_digits (char *s, int *nr, int cnt);
#if defined (ENABLE_UNUSED_FUNCTION)
static int parse_time_string (const char *timestr, int timestr_size,
			      int *sign, int *h, int *m, int *s, int *ms);
#endif
static int get_string_date_token_id (const STRING_DATE_TOKEN token_type,
				     const INTL_LANG intl_lang_id,
				     const char *cs,
				     int *token_id, int *token_size);
static int print_string_date_token (const STRING_DATE_TOKEN token_type,
				    const INTL_LANG intl_lang_id,
				    int token_id, int case_mode,
				    char *buffer, int *token_size);

#define WHITESPACE(c) ((c) == ' ' || (c) == '\t' || (c) == '\r' || (c) == '\n')

/* concatenate a char to s */
#define STRCHCAT(s, c) \
  {\
    char __cch__[2];\
    __cch__[0] = c;__cch__[1] = 0; strcat(s, __cch__);\
  }

#define SKIP_SPACES(ch, end) 	do {\
	while (ch != end && char_isspace(*(ch))) (ch)++; \
}while(0)

/*
 * db_time_format ()
 *
 * Arguments:
 *         time_value: time from which we get the informations
 *         format: format specifiers string
 *	   result: output string
 *
 * Returns: int
 *
 * Errors:
 *
 * Note:
 *     This is used like the DATE_FORMAT() function, but the format
 *  string may contain format specifiers only for hours, minutes, seconds, and
 *  milliseconds. Other specifiers produce a NULL value or 0.
 */
int
db_time_format (const DB_VALUE * time_value, const DB_VALUE * format,
		const DB_VALUE * date_lang, DB_VALUE * result)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_time_value, tmp_format, tmp_date_lang;
  DB_TIME *t_p;
  DB_DATETIME *dt_p;
  DB_TYPE res_type;
  char *res = NULL, *res2 = NULL, *format_s = NULL;
  char *strend;
  int format_s_len;
  int len;
  int h, mi, s, ms, year, month, day;
  char format_specifiers[256][64];
  char och = -1, ch;
  INTL_LANG date_lang_id;
  const LANG_LOCALE_DATA *lld;
  bool dummy;
  int res_collation;

  assert (time_value != result);
  assert (format != result);
  assert (date_lang != result);

  if (result == NULL)
    {
      assert (false);		/* is impossible */
      return ER_FAILED;
    }

  h = mi = s = ms = 0;
  memset (format_specifiers, 0, sizeof (format_specifiers));

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (3, &tmp_time_value, &tmp_format, &tmp_date_lang);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (2, time_value, format))
    {
      DB_MAKE_NULL (result);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  res_type = DB_VALUE_DOMAIN_TYPE (time_value);

  if (!(res_type == DB_TYPE_DATETIME || res_type == DB_TYPE_TIME))
    {
      time_value =
	db_value_cast_arg (time_value, &tmp_time_value, DB_TYPE_TIME,
			   &error_status);
    }
  format =
    db_value_cast_arg (format, &tmp_format, DB_TYPE_VARCHAR, &error_status);
  date_lang =
    db_value_cast_arg (date_lang, &tmp_date_lang, DB_TYPE_INTEGER,
		       &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  assert (DB_VALUE_TYPE (date_lang) == DB_TYPE_INTEGER);
  date_lang_id = lang_get_lang_id_from_flag (DB_GET_INT (date_lang), &dummy,
					     &dummy);

  res_collation = DB_GET_STRING_COLLATION (format);

  lld = lang_get_specific_locale (date_lang_id);
  if (lld == NULL)
    {
      error_status = ER_LANG_CODESET_NOT_AVAILABLE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
	      lang_get_lang_name_from_id (date_lang_id),
	      lang_charset_name (INTL_CODESET_UTF8));
      goto exit_on_error;
    }

  res_type = DB_VALUE_DOMAIN_TYPE (time_value);

  /* 1. Get date values */
  switch (res_type)
    {
    case DB_TYPE_DATETIME:
      dt_p = DB_GET_DATETIME (time_value);
      db_datetime_decode (dt_p, &month, &day, &year, &h, &mi, &s, &ms);
      break;

    case DB_TYPE_TIME:
      t_p = DB_GET_TIME (time_value);
      db_time_decode (t_p, &h, &mi, &s);
      break;

    default:
      assert (false);
      error_status = ER_TP_CANT_COERCE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
	      pr_type_name (res_type), pr_type_name (DB_TYPE_DATETIME));
      goto exit_on_error;
    }

  /* 2. Compute the value for each format specifier */
  if (mi < 0)
    {
      mi = -mi;
    }
  if (s < 0)
    {
      s = -s;
    }
  if (ms < 0)
    {
      ms = -ms;
    }

  /* %f       Milliseconds (000..999) */
  sprintf (format_specifiers['f'], "%03d", ms);

  /* %H       Hour (00..23) */
  if (h < 0)
    {
      sprintf (format_specifiers['H'], "-%02d", -h);
    }
  else
    {
      sprintf (format_specifiers['H'], "%02d", h);
    }
  if (h < 0)
    {
      h = -h;
    }

  /* %h       Hour (01..12) */
  sprintf (format_specifiers['h'], "%02d", (h % 12 == 0) ? 12 : (h % 12));

  /* %I       Hour (01..12) */
  sprintf (format_specifiers['I'], "%02d", (h % 12 == 0) ? 12 : (h % 12));

  /* %i       Minutes, numeric (00..59) */
  sprintf (format_specifiers['i'], "%02d", mi);

  /* %k       Hour (0..23) */
  sprintf (format_specifiers['k'], "%d", h);

  /* %l       Hour (1..12) */
  sprintf (format_specifiers['l'], "%d", (h % 12 == 0) ? 12 : (h % 12));

  /* %p       AM or PM */
  STRNCPY (format_specifiers['p'],
	   (h > 11) ? lld->am_pm[PM_NAME] : lld->am_pm[AM_NAME], 64);

  /* %r       Time, 12-hour (hh:mm:ss followed by AM or PM) */
  sprintf (format_specifiers['r'], "%02d:%02d:%02d %s",
	   (h % 12 == 0) ? 12 : (h % 12), mi, s,
	   (h > 11) ? lld->am_pm[PM_NAME] : lld->am_pm[AM_NAME]);

  /* %S       Seconds (00..59) */
  sprintf (format_specifiers['S'], "%02d", s);

  /* %s       Seconds (00..59) */
  sprintf (format_specifiers['s'], "%02d", s);

  /* %T       Time, 24-hour (hh:mm:ss) */
  sprintf (format_specifiers['T'], "%02d:%02d:%02d", h, mi, s);

  /* 3. Generate the output according to the format and the values */
  assert (DB_VALUE_DOMAIN_TYPE (format) == DB_TYPE_VARCHAR);
  format_s = DB_PULL_STRING (format);
  format_s_len = DB_GET_STRING_SIZE (format);

  len = 1024;
  res = (char *) malloc (len);
  if (res == NULL)
    {
      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
      goto exit_on_error;
    }

  memset (res, 0, len);

  ch = *format_s;
  strend = format_s + format_s_len;

  while (format_s < strend)
    {
      format_s++;
      och = ch;
      ch = *format_s;

      if (och == '%' /* && (res[strlen(res) - 1] != '%') */ )
	{
	  if (ch == '%')
	    {
	      STRCHCAT (res, '%');

	      /* jump a character */
	      format_s++;
	      och = ch;
	      ch = *format_s;

	      continue;
	    }
	  /* parse the character */
	  if (strlen (format_specifiers[(unsigned char) ch]) == 0)
	    {
	      /* append the character itself */
	      STRCHCAT (res, ch);
	    }
	  else
	    {
	      strcat (res, format_specifiers[(unsigned char) ch]);
	    }

	  /* jump a character */
	  format_s++;
	  och = ch;
	  ch = *format_s;
	}
      else
	{
	  STRCHCAT (res, och);
	}

      /* chance of overflow ? */
      /* assume we can't add at a time mode than 16 chars */
      if (strlen (res) + 16 > len)
	{
	  /* realloc - copy temporary in res2 */
	  res2 = (char *) malloc (len);
	  if (res2 == NULL)
	    {
	      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	      goto exit_on_error;
	    }

	  memset (res2, 0, len);
	  strcpy (res2, res);
	  free_and_init (res);

	  len += 1024;
	  res = (char *) malloc (len);
	  if (res == NULL)
	    {
	      free_and_init (res2);
	      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	      goto exit_on_error;
	    }

	  memset (res, 0, len);
	  strcpy (res, res2);
	  free_and_init (res2);
	}
    }
  /* finished string */

  /* 4. */

  DB_MAKE_STRING (result, res);
  if (result != NULL && TP_IS_CHAR_TYPE (DB_VALUE_DOMAIN_TYPE (result)))
    {
      db_string_put_cs_and_collation (result, res_collation);
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
  db_value_clear_nargs (3, &tmp_time_value, &tmp_format, &tmp_date_lang);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  if (res != NULL)
    {
      free_and_init (res);
    }

  DB_MAKE_NULL (result);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_time_value, &tmp_format, &tmp_date_lang);

  return error_status;
}

/*
 * db_to_char () -
 */
int
db_to_char (const DB_VALUE * src_value,
	    const DB_VALUE * format_or_length,
	    const DB_VALUE * lang_str, DB_VALUE * result_str)
{
  int error_status = NO_ERROR;
  DB_TYPE type;

  assert (src_value != result_str);
  assert (format_or_length != result_str);
  assert (lang_str != result_str);

  type = DB_VALUE_DOMAIN_TYPE (src_value);
  if (type == DB_TYPE_NULL || is_number (src_value))
    {
      return number_to_char (src_value, format_or_length, lang_str,
			     result_str);
    }
  else if (TP_IS_DATE_OR_TIME_TYPE (type))
    {
      return date_to_char (src_value, format_or_length, lang_str, result_str);
    }
  else if (TP_IS_CHAR_TYPE (type))
    {
      error_status = pr_clone_value (src_value, result_str);

      return error_status;
    }
  else
    {
      error_status = ER_OBJ_INVALID_ARGUMENTS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);

      return error_status;
    }
}

#define MAX_STRING_DATE_TOKEN_LEN  LOC_DATA_MONTH_WIDE_SIZE
const char *Month_name_UTF8[][12] = {
  {"January", "February", "March", "April",
   "May", "June", "July", "August", "September", "October",
   "November", "December"},	/* US */
  {"1\xec\x9b\x94",
   "2\xec\x9b\x94",
   "3\xec\x9b\x94",
   "4\xec\x9b\x94",
   "5\xec\x9b\x94",
   "6\xec\x9b\x94",
   "7\xec\x9b\x94",
   "8\xec\x9b\x94",
   "9\xec\x9b\x94",
   "10\xec\x9b\x94",
   "11\xec\x9b\x94",
   "12\xec\x9b\x94"}		/* KR */
};

const char Month_name_parse_order[][12] = {
  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
};

const char *Day_name_UTF8[][7] = {
  {"Sunday", "Monday", "Tuesday", "Wednesday",
   "Thursday", "Friday", "Saturday"},	/* US */
  {"\xec\x9d\xbc\xec\x9a\x94\xec\x9d\xbc",
   "\xec\x9b\x94\xec\x9a\x94\xec\x9d\xbc",
   "\xed\x99\x94\xec\x9a\x94\xec\x9d\xbc",
   "\xec\x88\x98\xec\x9a\x94\xec\x9d\xbc",
   "\xeb\xaa\xa9\xec\x9a\x94\xec\x9d\xbc",
   "\xea\xb8\x88\xec\x9a\x94\xec\x9d\xbc",
   "\xed\x86\xa0\xec\x9a\x94\xec\x9d\xbc"}	/* KR */
};

const char Day_name_parse_order[][7] = {
  {0, 1, 2, 3, 4, 5, 6},
  {1, 0, 2, 3, 4, 6, 5}
};

const char *Short_Month_name_UTF8[][12] = {
  {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"},	/* US */
  {"1\xec\x9b\x94",
   "2\xec\x9b\x94",
   "3\xec\x9b\x94",
   "4\xec\x9b\x94",
   "5\xec\x9b\x94",
   "6\xec\x9b\x94",
   "7\xec\x9b\x94",
   "8\xec\x9b\x94",
   "9\xec\x9b\x94",
   "10\xec\x9b\x94",
   "11\xec\x9b\x94",
   "12\xec\x9b\x94"}		/* KR */
};

const char Short_Month_name_parse_order[][12] = {
  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
};

const char *Short_Day_name_UTF8[][7] = {
  {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"},	/* US */
  {"\xec\x9d\xbc",
   "\xec\x9b\x94",
   "\xed\x99\x94",
   "\xec\x88\x98",
   "\xeb\xaa\xa9",
   "\xea\xb8\x88",
   "\xed\x86\xa0"}		/* KR */
};

const char Short_Day_name_parse_order[][7] = {
  {0, 1, 2, 3, 4, 5, 6},
  {0, 1, 2, 3, 4, 5, 6}
};

#define AM_NAME_KR_UTF8 "\xec\x98\xa4\xec\xa0\x84"
#define PM_NAME_KR_UTF8 "\xec\x98\xa4\xed\x9b\x84"

const char AM_PM_parse_order[][12] = {
  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
  {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
};

const char *Am_Pm_name_UTF8[][12] = {
  {"am", "pm", "Am", "Pm", "AM", "PM",
   "a.m.", "p.m.", "A.m.", "P.m.", "A.M.", "P.M."},	/* US */
  {AM_NAME_KR_UTF8, PM_NAME_KR_UTF8, AM_NAME_KR_UTF8,
   PM_NAME_KR_UTF8, AM_NAME_KR_UTF8, PM_NAME_KR_UTF8,
   AM_NAME_KR_UTF8, PM_NAME_KR_UTF8, AM_NAME_KR_UTF8,
   PM_NAME_KR_UTF8, AM_NAME_KR_UTF8, PM_NAME_KR_UTF8}
  /* KR */
};

/*
 * db_to_date () -
 */
int
db_to_date (const DB_VALUE * src_str,
	    const DB_VALUE * format_str,
	    const DB_VALUE * date_lang, DB_VALUE * result_date)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_src_str, tmp_format_str, tmp_date_lang;
  char *cur_format_str_ptr, *next_format_str_ptr;
  char *cs;			/*current source string pointer */
  char *last_src, *last_format;

  int cur_format;

  int cur_format_size;

  int month = 0, day = 0, year = 0, day_of_the_week = 0, week = -1;
  int monthcount = 0, daycount = 0, yearcount = 0, day_of_the_weekcount = 0;

  int i;
  bool no_user_format;
  INTL_LANG date_lang_id;
  char stack_buf_str[64], stack_buf_format[64];
  char *initial_buf_str = NULL, *initial_buf_format = NULL;
  bool do_free_buf_str = false, do_free_buf_format = false;
  DB_VALUE default_format;
  bool has_user_format = false;
  bool dummy;

  assert (src_str != (DB_VALUE *) NULL);
  assert (date_lang != (DB_VALUE *) NULL);
  assert (result_date != (DB_VALUE *) NULL);
  assert (src_str != result_date);
  assert (format_str != result_date);
  assert (date_lang != result_date);

  DB_MAKE_NULL (&default_format);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (3, &tmp_src_str, &tmp_format_str, &tmp_date_lang);

  if (DB_IS_NULL (src_str))
    {
      DB_MAKE_NULL (result_date);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  src_str =
    db_value_cast_arg (src_str, &tmp_src_str, DB_TYPE_VARCHAR, &error_status);
  date_lang =
    db_value_cast_arg (date_lang, &tmp_date_lang, DB_TYPE_INTEGER,
		       &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  assert (DB_VALUE_TYPE (date_lang) == DB_TYPE_INTEGER);
  date_lang_id = lang_get_lang_id_from_flag (DB_GET_INT (date_lang),
					     &has_user_format, &dummy);

  if (DB_GET_STRING_SIZE (src_str) == 0)
    {
      error_status = ER_QSTR_EMPTY_STRING;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  if (DB_GET_STRING_SIZE (src_str) > MAX_TOKEN_SIZE)
    {
      error_status = ER_QSTR_SRC_TOO_LONG;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  if (lang_get_specific_locale (date_lang_id) == NULL)
    {
      error_status = ER_LANG_CODESET_NOT_AVAILABLE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
	      lang_get_lang_name_from_id (date_lang_id),
	      lang_charset_name (INTL_CODESET_UTF8));
      goto exit_on_error;
    }

  error_status =
    db_check_or_create_null_term_string (src_str, stack_buf_str,
					 sizeof (stack_buf_str),
					 true, true,
					 &initial_buf_str, &do_free_buf_str);

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

  cs = initial_buf_str;
  last_src = cs + strlen (cs);

  last_src = (char *) intl_backskip_spaces (cs, last_src - 1);
  last_src = last_src + 1;

  no_user_format = (format_str == NULL) || (!has_user_format);

  if (no_user_format)
    {
      DB_DATE date_tmp;
      const char *default_format_str;

      /* try default Rye format first */
      if (NO_ERROR == db_string_to_date_ex ((char *) cs,
					    last_src - cs, &date_tmp))
	{
	  DB_MAKE_ENCODED_DATE (result_date, &date_tmp);
	  goto done;
	}

      /* error parsing Rye default format, try the locale format, if any */
      default_format_str =
	lang_date_format_parse (date_lang_id, DB_TYPE_DATE);
      if (default_format_str == NULL)
	{
	  error_status = ER_DATE_CONVERSION;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto exit_on_error;
	}

      DB_MAKE_VARCHAR (&default_format, strlen (default_format_str),
		       (char *) default_format_str,
		       strlen (default_format_str),
		       LANG_GET_DEFAULT_COLLATION);
      format_str = &default_format;
    }

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL (format_str))
    {
      DB_MAKE_NULL (result_date);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  format_str =
    db_value_cast_arg (format_str, &tmp_format_str, DB_TYPE_VARCHAR,
		       &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  if (DB_GET_STRING_SIZE (format_str) > MAX_TOKEN_SIZE)
    {
      error_status = ER_QSTR_FORMAT_TOO_LONG;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  if (DB_GET_STRING_SIZE (format_str) == 0)
    {
      error_status = ER_QSTR_EMPTY_STRING;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  error_status =
    db_check_or_create_null_term_string (format_str, stack_buf_format,
					 sizeof (stack_buf_format),
					 true, true,
					 &initial_buf_format,
					 &do_free_buf_format);
  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

  cur_format_str_ptr = initial_buf_format;
  last_format = cur_format_str_ptr + strlen (cur_format_str_ptr);

  /* Skip space, tab, CR     */
  while (cs < last_src && strchr (WHITE_CHARS, *cs))
    {
      cs++;
    }

  /* Skip space, tab, CR     */
  while (cur_format_str_ptr < last_format &&
	 strchr (WHITE_CHARS, *cur_format_str_ptr))
    {
      cur_format_str_ptr++;
    }

  while (cs < last_src)
    {
      int token_size, cmp, cs_byte_size;
      int k;

      cur_format = get_next_format (cur_format_str_ptr,
				    DB_TYPE_DATE, &cur_format_size,
				    &next_format_str_ptr);
      switch (cur_format)
	{
	case DT_YYYY:
	  if (yearcount != 0)
	    {
	      error_status = ER_QSTR_FORMAT_DUPLICATION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  else
	    {
	      yearcount++;
	    }

	  k = parse_digits (cs, &year, 4);
	  if (k <= 0)
	    {
	      error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }

	  cs += k;
	  break;

	case DT_YY:
	  if (yearcount != 0)
	    {
	      error_status = ER_QSTR_FORMAT_DUPLICATION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  else
	    {
	      yearcount++;
	    }

	  k = parse_digits (cs, &year, 2);
	  if (k <= 0)
	    {
	      error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }

	  cs += k;

	  i = get_cur_year ();
	  if (i == -1)
	    {
	      error_status = ER_SYSTEM_DATE;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }

	  year += (i / 100) * 100;
	  break;

	case DT_MM:
	  if (monthcount != 0)
	    {
	      error_status = ER_QSTR_FORMAT_DUPLICATION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  else
	    {
	      monthcount++;
	    }

	  k = parse_digits (cs, &month, 2);
	  if (k <= 0)
	    {
	      error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }

	  cs += k;

	  if (month < 1 || month > 12)
	    {
	      error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  break;

	case DT_MONTH:
	  if (monthcount != 0)
	    {
	      error_status = ER_QSTR_FORMAT_DUPLICATION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  else
	    {
	      monthcount++;
	    }

	  error_status = get_string_date_token_id (SDT_MONTH,
						   date_lang_id, cs,
						   &month, &token_size);
	  if (error_status != NO_ERROR)
	    {
	      goto exit_on_error;
	    }

	  cs += token_size;

	  if (month == 0)
	    {
	      error_status = ER_DATE_CONVERSION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  break;

	case DT_MON:
	  if (monthcount != 0)
	    {
	      error_status = ER_QSTR_FORMAT_DUPLICATION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  else
	    {
	      monthcount++;
	    }

	  month = 0;


	  error_status = get_string_date_token_id (SDT_MONTH_SHORT,
						   date_lang_id, cs,
						   &month, &token_size);
	  if (error_status != NO_ERROR)
	    {
	      goto exit_on_error;
	    }
	  cs += token_size;


	  if (month == 0)
	    {
	      error_status = ER_DATE_CONVERSION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  break;

	case DT_DD:
	  if (daycount != 0)
	    {
	      error_status = ER_QSTR_FORMAT_DUPLICATION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  else
	    {
	      daycount++;
	    }

	  k = parse_digits (cs, &day, 2);
	  if (k <= 0)
	    {
	      error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }

	  cs += k;

	  if (day < 0 || day > 31)
	    {
	      error_status = ER_DATE_CONVERSION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  break;

	case DT_TEXT:
	  cmp =
	    intl_case_match_tok (date_lang_id,
				 (unsigned char *)
				 (cur_format_str_ptr + 1),
				 (unsigned char *) cs,
				 cur_format_size - 2, strlen (cs),
				 &cs_byte_size);

	  if (cmp != 0)
	    {
	      error_status = ER_QSTR_INVALID_FORMAT;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }

	  cs += cs_byte_size;
	  break;

	case DT_PUNCTUATION:
	  if (strncasecmp ((const char *) cur_format_str_ptr,
			   (const char *) cs, cur_format_size) != 0)
	    {
	      error_status = ER_QSTR_INVALID_FORMAT;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }

	  cs += cur_format_size;
	  break;

	case DT_CC:
	case DT_Q:
	  error_status = ER_QSTR_INVALID_FORMAT;
	  /* Does it need error message? */
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto exit_on_error;

	case DT_DAY:
	  if (day_of_the_weekcount != 0)
	    {
	      error_status = ER_QSTR_FORMAT_DUPLICATION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  else
	    {
	      day_of_the_weekcount++;
	    }

	  error_status = get_string_date_token_id (SDT_DAY, date_lang_id,
						   cs, &day_of_the_week,
						   &token_size);
	  if (error_status != NO_ERROR)
	    {
	      goto exit_on_error;
	    }

	  cs += token_size;

	  if (day_of_the_week == 0)
	    {
	      error_status = ER_DATE_CONVERSION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  break;

	case DT_DY:
	  if (day_of_the_weekcount != 0)
	    {
	      error_status = ER_QSTR_FORMAT_DUPLICATION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  else
	    {
	      day_of_the_weekcount++;
	    }

	  error_status =
	    get_string_date_token_id (SDT_DAY_SHORT, date_lang_id, cs,
				      &day_of_the_week, &token_size);
	  if (error_status != NO_ERROR)
	    {
	      goto exit_on_error;
	    }

	  cs += token_size;

	  if (day_of_the_week == 0)
	    {
	      error_status = ER_DATE_CONVERSION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  break;

	case DT_D:
	  if (day_of_the_weekcount != 0)
	    {
	      error_status = ER_QSTR_FORMAT_DUPLICATION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  else
	    {
	      day_of_the_weekcount++;
	    }

	  k = parse_digits (cs, &day_of_the_week, 1);
	  if (k <= 0)
	    {
	      error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }

	  cs += k;

	  if (day_of_the_week < 1 || day_of_the_week > 7)
	    {
	      error_status = ER_DATE_CONVERSION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  break;

	case DT_INVALID:
	case DT_NORMAL:
	  error_status = ER_QSTR_INVALID_FORMAT;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto exit_on_error;
	}

      /* Skip space, tab, CR     */
      while (cs < last_src && strchr (WHITE_CHARS, *cs))
	{
	  cs++;
	}

      cur_format_str_ptr = next_format_str_ptr;

      /* Skip space, tab, CR     */
      while (cur_format_str_ptr < last_format &&
	     strchr (WHITE_CHARS, *cur_format_str_ptr))
	{
	  cur_format_str_ptr++;
	}

      if (last_format == next_format_str_ptr)
	{
	  while (cs < last_src && strchr (WHITE_CHARS, *cs))
	    {
	      cs++;
	    }

	  if (cs != last_src)
	    {
	      error_status = ER_QSTR_INVALID_FORMAT;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  break;
	}
    }


  /* Both format and src should end at same time     */
  if (cs != last_src || cur_format_str_ptr != last_format)
    {
      error_status = ER_QSTR_INVALID_FORMAT;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  /**************            Check DATE        ****************/
#if 1				/* TODO - trace; do not delete me */
  if (yearcount == 0 || monthcount == 0 || daycount == 0)
    {
      error_status = ER_QSTR_INVALID_FORMAT;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }
#else
  year = (yearcount == 0) ? get_cur_year () : year;
  month = (monthcount == 0) ? get_cur_month () : month;
  day = (daycount == 0) ? 1 : day;
#endif
  week = (day_of_the_weekcount == 0) ? -1 : day_of_the_week - 1;

  if (week != -1 && week != db_get_day_of_week (year, month, day))
    {
      error_status = ER_DATE_CONVERSION;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  DB_MAKE_DATE (result_date, month, day, year);

  if (*(DB_GET_DATE (result_date)) == 0)
    {
      error_status = ER_DATE_CONVERSION;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
    }

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  if (do_free_buf_str)
    {
      free_and_init (initial_buf_str);
    }
  if (do_free_buf_format)
    {
      free_and_init (initial_buf_format);
    }

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_src_str, &tmp_format_str, &tmp_date_lang);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  if (do_free_buf_str)
    {
      free_and_init (initial_buf_str);
    }
  if (do_free_buf_format)
    {
      free_and_init (initial_buf_format);
    }

  DB_MAKE_NULL (result_date);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_src_str, &tmp_format_str, &tmp_date_lang);

  return error_status;
}

/*
 * db_to_time () -
 */
int
db_to_time (const DB_VALUE * src_str,
	    const DB_VALUE * format_str,
	    const DB_VALUE * date_lang, DB_VALUE * result_time)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_src_str, tmp_format_str, tmp_date_lang;
  char *cur_format_str_ptr, *next_format_str_ptr;
  char *cs;			/*current source string pointer */
  char *last_format, *last_src;

  int cur_format;

  int cur_format_size;

  int second = 0, minute = 0, hour = 0;
  int time_count = 0;
  int mil_time_count = 0;
  int am = false;
  int pm = false;

  bool no_user_format;
  INTL_LANG date_lang_id;
  char stack_buf_str[64], stack_buf_format[64];
  char *initial_buf_str = NULL, *initial_buf_format = NULL;
  bool do_free_buf_str = false, do_free_buf_format = false;
  DB_VALUE default_format;
  bool has_user_format = false;
  bool dummy;

  assert (src_str != (DB_VALUE *) NULL);
  assert (date_lang != (DB_VALUE *) NULL);
  assert (result_time != (DB_VALUE *) NULL);
  assert (src_str != result_time);
  assert (format_str != result_time);
  assert (date_lang != result_time);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (3, &tmp_src_str, &tmp_format_str, &tmp_date_lang);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL (src_str))
    {
      DB_MAKE_NULL (result_time);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  src_str =
    db_value_cast_arg (src_str, &tmp_src_str, DB_TYPE_VARCHAR, &error_status);
  date_lang =
    db_value_cast_arg (date_lang, &tmp_date_lang, DB_TYPE_INTEGER,
		       &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  assert (DB_VALUE_TYPE (date_lang) == DB_TYPE_INTEGER);
  date_lang_id = lang_get_lang_id_from_flag (DB_GET_INT (date_lang),
					     &has_user_format, &dummy);

  if (DB_GET_STRING_SIZE (src_str) == 0)
    {
      error_status = ER_QSTR_EMPTY_STRING;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  if (DB_GET_STRING_SIZE (src_str) > MAX_TOKEN_SIZE)
    {
      error_status = ER_QSTR_SRC_TOO_LONG;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  if (lang_get_specific_locale (date_lang_id) == NULL)
    {
      error_status = ER_LANG_CODESET_NOT_AVAILABLE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
	      lang_get_lang_name_from_id (date_lang_id),
	      lang_charset_name (INTL_CODESET_UTF8));
      goto exit_on_error;
    }

  error_status =
    db_check_or_create_null_term_string (src_str, stack_buf_str,
					 sizeof (stack_buf_str),
					 true, true,
					 &initial_buf_str, &do_free_buf_str);
  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

  cs = initial_buf_str;
  last_src = cs + strlen (cs);

  last_src = (char *) intl_backskip_spaces (cs, last_src - 1);
  last_src = last_src + 1;

  no_user_format = (format_str == NULL) || (!has_user_format);

  if (no_user_format)
    {
      DB_TIME time_tmp;
      const char *default_format_str;

      /* try default Rye format first */
      if (NO_ERROR == db_string_to_time_ex ((const char *) cs,
					    last_src - cs, &time_tmp))
	{
	  DB_MAKE_ENCODED_TIME (result_time, &time_tmp);
	  goto done;
	}

      /* error parsing Rye default format, try the locale format, if any */
      default_format_str =
	lang_date_format_parse (date_lang_id, DB_TYPE_TIME);
      if (default_format_str == NULL)
	{
	  error_status = ER_TIME_CONVERSION;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto exit_on_error;
	}
      DB_MAKE_VARCHAR (&default_format, strlen (default_format_str),
		       (char *) default_format_str,
		       strlen (default_format_str),
		       LANG_GET_DEFAULT_COLLATION);
      format_str = &default_format;
    }

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL (format_str))
    {
      DB_MAKE_NULL (result_time);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  format_str =
    db_value_cast_arg (format_str, &tmp_format_str, DB_TYPE_VARCHAR,
		       &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  if (DB_GET_STRING_SIZE (format_str) > MAX_TOKEN_SIZE)
    {
      error_status = ER_QSTR_FORMAT_TOO_LONG;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  if (DB_GET_STRING_SIZE (format_str) == 0)
    {
      error_status = ER_QSTR_EMPTY_STRING;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  error_status =
    db_check_or_create_null_term_string (format_str, stack_buf_format,
					 sizeof (stack_buf_format),
					 true, true,
					 &initial_buf_format,
					 &do_free_buf_format);
  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

  cur_format_str_ptr = initial_buf_format;
  last_format = cur_format_str_ptr + strlen (cur_format_str_ptr);

  /* Skip space, tab, CR     */
  while (cs < last_src && strchr (WHITE_CHARS, *cs))
    {
      cs++;
    }

  /* Skip space, tab, CR     */
  while (cur_format_str_ptr < last_format &&
	 strchr (WHITE_CHARS, *cur_format_str_ptr))
    {
      cur_format_str_ptr++;
    }

  while (cs < last_src)
    {
      int cmp, cs_byte_size, token_size;
      int am_pm_id;
      int k;

      cur_format = get_next_format (cur_format_str_ptr,
				    DB_TYPE_TIME, &cur_format_size,
				    &next_format_str_ptr);
      switch (cur_format)
	{
	case DT_AM:
	case DT_A_M:
	case DT_PM:
	case DT_P_M:
	  if (mil_time_count != 0)
	    {
	      error_status = ER_QSTR_FORMAT_DUPLICATION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  else
	    {
	      mil_time_count++;
	    }

	  error_status =
	    get_string_date_token_id (SDT_AM_PM, date_lang_id, cs,
				      &am_pm_id, &token_size);
	  if (error_status != NO_ERROR)
	    {
	      goto exit_on_error;
	    }

	  if (am_pm_id > 0)
	    {
	      if (am_pm_id % 2)
		{
		  am = true;
		}
	      else
		{
		  pm = true;
		}
	    }
	  else
	    {
	      error_status = ER_QSTR_INVALID_FORMAT;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }

	  cs += token_size;
	  break;

	case DT_HH:
	case DT_HH12:
	  if (time_count != 0)
	    {
	      error_status = ER_QSTR_FORMAT_DUPLICATION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  else
	    {
	      time_count++;
	    }

	  k = parse_digits (cs, &hour, 2);
	  if (k <= 0)
	    {
	      error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  cs += k;

	  if (hour < 1 || hour > 12)
	    {
	      error_status = ER_DATE_CONVERSION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  break;

	case DT_HH24:
	  if (time_count != 0)
	    {
	      error_status = ER_QSTR_FORMAT_DUPLICATION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  else
	    {
	      time_count++;
	    }

	  if (mil_time_count != 0)
	    {
	      error_status = ER_QSTR_FORMAT_DUPLICATION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  else
	    {
	      mil_time_count++;
	    }

	  k = parse_digits (cs, &hour, 2);
	  if (k <= 0)
	    {
	      error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  cs += k;

	  if (hour < 0 || hour > 23)
	    {
	      error_status = ER_TIME_CONVERSION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  break;

	case DT_MI:
	  k = parse_digits (cs, &minute, 2);
	  if (k <= 0)
	    {
	      error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  cs += k;

	  if (minute < 0 || minute > 59)
	    {
	      error_status = ER_TIME_CONVERSION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  break;

	case DT_SS:
	  k = parse_digits (cs, &second, 2);
	  if (k <= 0)
	    {
	      error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  cs += k;

	  if (second < 0 || second > 59)
	    {
	      error_status = ER_TIME_CONVERSION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  break;

	case DT_TEXT:
	  cmp =
	    intl_case_match_tok (date_lang_id,
				 (unsigned char *)
				 (cur_format_str_ptr + 1),
				 (unsigned char *) cs,
				 cur_format_size - 2, strlen (cs),
				 &cs_byte_size);

	  if (cmp != 0)
	    {
	      error_status = ER_TIME_CONVERSION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }

	  cs += cs_byte_size;
	  break;

	case DT_PUNCTUATION:
	  if (strncasecmp ((const char *) cur_format_str_ptr,
			   (const char *) cs, cur_format_size) != 0)
	    {
	      error_status = ER_TIME_CONVERSION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }

	  cs += cur_format_size;
	  break;

	case DT_INVALID:
	  error_status = ER_TIME_CONVERSION;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto exit_on_error;
	}

      /* Skip space, tab, CR     */
      while (cs < last_src && strchr (WHITE_CHARS, *cs))
	{
	  cs++;
	}

      cur_format_str_ptr = next_format_str_ptr;

      /* Skip space, tab, CR     */
      while (cur_format_str_ptr < last_format &&
	     strchr (WHITE_CHARS, *cur_format_str_ptr))
	{
	  cur_format_str_ptr++;
	}

      if (last_format == next_format_str_ptr)
	{
	  while (cs < last_src && strchr (WHITE_CHARS, *cs))
	    {
	      cs++;
	    }

	  if (cs != last_src)
	    {
	      error_status = ER_QSTR_INVALID_FORMAT;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  break;
	}
    }

  /* Both format and src should end at same time     */
  if (cs != last_src || cur_format_str_ptr != last_format)
    {
      error_status = ER_TIME_CONVERSION;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  if (am == true && pm == false && hour <= 12)
    {				/* If A.M.    */
      hour = (hour == 12) ? 0 : hour;
    }
  else if (am == false && pm == true && hour <= 12)
    {				/* If P.M.    */
      hour = (hour == 12) ? hour : hour + 12;
    }
  else if (am == false && pm == false)
    {				/* If military time    */
      ;
    }
  else
    {
      error_status = ER_TIME_CONVERSION;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  DB_MAKE_TIME (result_time, hour, minute, second);

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  if (do_free_buf_str)
    {
      free_and_init (initial_buf_str);
    }

  if (do_free_buf_format)
    {
      free_and_init (initial_buf_format);
    }

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_src_str, &tmp_format_str, &tmp_date_lang);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  if (do_free_buf_str)
    {
      free_and_init (initial_buf_str);
    }

  if (do_free_buf_format)
    {
      free_and_init (initial_buf_format);
    }

  DB_MAKE_NULL (result_time);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_src_str, &tmp_format_str, &tmp_date_lang);

  return error_status;
}

/*
 * db_to_datetime () -
 */
int
db_to_datetime (const DB_VALUE * src_str, const DB_VALUE * format_str,
		const DB_VALUE * date_lang, DB_VALUE * result_datetime)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_src_str, tmp_format_str, tmp_date_lang;
  DB_DATETIME tmp_datetime;

  char *cur_format_str_ptr, *next_format_str_ptr;
  char *cs;			/*current source string pointer */
  char *last_format, *last_src;

  int cur_format_size;
  int cur_format;

  int month = 0, day = 0, year = 0, day_of_the_week = 0, week = -1;
  int monthcount = 0, daycount = 0, yearcount = 0, day_of_the_weekcount = 0;

  double fraction;
  int millisecond = 0, second = 0, minute = 0, hour = 0;
  int time_count = 0;
  int mil_time_count = 0;
  int am = false;
  int pm = false;

  int i;
  bool no_user_format;
  INTL_LANG date_lang_id;
  char stack_buf_str[64], stack_buf_format[64];
  char *initial_buf_str = NULL, *initial_buf_format = NULL;
  bool do_free_buf_str = false, do_free_buf_format = false;
  DB_VALUE default_format;
  bool has_user_format = false;
  bool dummy;

  assert (src_str != (DB_VALUE *) NULL);
  assert (result_datetime != (DB_VALUE *) NULL);
  assert (src_str != result_datetime);
  assert (format_str != result_datetime);
  assert (date_lang != result_datetime);

  DB_MAKE_NULL (&default_format);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (3, &tmp_src_str, &tmp_format_str, &tmp_date_lang);

  if (DB_IS_NULL (src_str))
    {
      DB_MAKE_NULL (result_datetime);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  src_str =
    db_value_cast_arg (src_str, &tmp_src_str, DB_TYPE_VARCHAR, &error_status);
  date_lang =
    db_value_cast_arg (date_lang, &tmp_date_lang, DB_TYPE_INTEGER,
		       &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  assert (DB_VALUE_TYPE (date_lang) == DB_TYPE_INTEGER);
  date_lang_id = lang_get_lang_id_from_flag (DB_GET_INT (date_lang),
					     &has_user_format, &dummy);

  if (DB_GET_STRING_SIZE (src_str) == 0)
    {
      error_status = ER_QSTR_EMPTY_STRING;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  if (DB_GET_STRING_SIZE (src_str) > MAX_TOKEN_SIZE)
    {
      error_status = ER_QSTR_SRC_TOO_LONG;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  if (lang_get_specific_locale (date_lang_id) == NULL)
    {
      error_status = ER_LANG_CODESET_NOT_AVAILABLE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
	      lang_get_lang_name_from_id (date_lang_id),
	      lang_charset_name (INTL_CODESET_UTF8));
      goto exit_on_error;
    }

  error_status =
    db_check_or_create_null_term_string (src_str, stack_buf_str,
					 sizeof (stack_buf_str),
					 true, true,
					 &initial_buf_str, &do_free_buf_str);
  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

  cs = initial_buf_str;
  last_src = cs + strlen (cs);

  last_src = (char *) intl_backskip_spaces (cs, last_src - 1);
  last_src = last_src + 1;

  no_user_format = (format_str == NULL) || (!has_user_format);

  if (no_user_format)
    {
      DB_DATETIME datetime_tmp;
      const char *default_format_str;

      /* try default Rye format first */
      if (db_string_to_datetime_ex ((const char *) cs,
				    last_src - cs, &datetime_tmp) == NO_ERROR)
	{
	  DB_MAKE_DATETIME (result_datetime, &datetime_tmp);
	  goto done;
	}

      default_format_str = lang_date_format_parse (date_lang_id,
						   DB_TYPE_DATETIME);
      if (default_format_str == NULL)
	{
	  error_status = ER_DATETIME_CONVERSION;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto exit_on_error;
	}

      DB_MAKE_VARCHAR (&default_format, strlen (default_format_str),
		       (char *) default_format_str,
		       strlen (default_format_str),
		       LANG_GET_DEFAULT_COLLATION);
      format_str = &default_format;
    }

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL (format_str))
    {
      DB_MAKE_NULL (result_datetime);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  format_str =
    db_value_cast_arg (format_str, &tmp_format_str, DB_TYPE_VARCHAR,
		       &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  if (DB_GET_STRING_SIZE (format_str) > MAX_TOKEN_SIZE)
    {
      error_status = ER_QSTR_FORMAT_TOO_LONG;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  if (DB_GET_STRING_SIZE (format_str) == 0)
    {
      error_status = ER_QSTR_EMPTY_STRING;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  error_status =
    db_check_or_create_null_term_string (format_str, stack_buf_format,
					 sizeof (stack_buf_format),
					 true, true,
					 &initial_buf_format,
					 &do_free_buf_format);
  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

  cur_format_str_ptr = initial_buf_format;
  last_format = cur_format_str_ptr + strlen (cur_format_str_ptr);

  /* Skip space, tab, CR     */
  while (cs < last_src && strchr (WHITE_CHARS, *cs))
    {
      cs++;
    }

  /* Skip space, tab, CR     */
  while (cur_format_str_ptr < last_format &&
	 strchr (WHITE_CHARS, *cur_format_str_ptr))
    {
      cur_format_str_ptr++;
    }

  while (cs < last_src)
    {
      int token_size, cmp, cs_byte_size;
      int am_pm_id;
      int k;

      cur_format = get_next_format (cur_format_str_ptr,
				    DB_TYPE_DATETIME, &cur_format_size,
				    &next_format_str_ptr);
      switch (cur_format)
	{
	case DT_YYYY:
	  if (yearcount != 0)
	    {
	      error_status = ER_QSTR_FORMAT_DUPLICATION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  else
	    {
	      yearcount++;
	    }

	  k = parse_digits (cs, &year, 4);
	  if (k <= 0)
	    {
	      error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  cs += k;
	  break;

	case DT_YY:
	  if (yearcount != 0)
	    {
	      error_status = ER_QSTR_FORMAT_DUPLICATION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  else
	    {
	      yearcount++;
	    }

	  k = parse_digits (cs, &year, 2);
	  if (k <= 0)
	    {
	      error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  cs += k;

	  i = get_cur_year ();
	  if (i == -1)
	    {
	      error_status = ER_SYSTEM_DATE;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }

	  year += (i / 100) * 100;
	  break;

	case DT_MM:
	  if (monthcount != 0)
	    {
	      error_status = ER_QSTR_FORMAT_DUPLICATION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  else
	    {
	      monthcount++;
	    }

	  k = parse_digits (cs, &month, 2);
	  if (k <= 0)
	    {
	      error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  cs += k;

	  if (month < 1 || month > 12)
	    {
	      error_status = ER_DATE_CONVERSION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  break;

	case DT_MONTH:
	  if (monthcount != 0)
	    {
	      error_status = ER_QSTR_FORMAT_DUPLICATION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  else
	    {
	      monthcount++;
	    }

	  error_status = get_string_date_token_id (SDT_MONTH,
						   date_lang_id, cs,
						   &month, &token_size);
	  if (error_status != NO_ERROR)
	    {
	      goto exit_on_error;
	    }

	  cs += token_size;

	  if (month == 0)
	    {
	      error_status = ER_DATE_CONVERSION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  break;

	case DT_MON:
	  if (monthcount != 0)
	    {
	      error_status = ER_QSTR_FORMAT_DUPLICATION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  else
	    {
	      monthcount++;
	    }

	  month = 0;

	  error_status = get_string_date_token_id (SDT_MONTH_SHORT,
						   date_lang_id, cs,
						   &month, &token_size);
	  if (error_status != NO_ERROR)
	    {
	      goto exit_on_error;
	    }

	  cs += token_size;

	  if (month == 0)
	    {
	      error_status = ER_DATE_CONVERSION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  break;

	case DT_DD:
	  if (daycount != 0)
	    {
	      error_status = ER_QSTR_FORMAT_DUPLICATION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  else
	    {
	      daycount++;
	    }

	  k = parse_digits (cs, &day, 2);
	  if (k <= 0)
	    {
	      error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  cs += k;

	  if (day < 0 || day > 31)
	    {
	      error_status = ER_DATE_CONVERSION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  break;

	case DT_AM:
	case DT_A_M:
	case DT_PM:
	case DT_P_M:
	  if (mil_time_count != 0)
	    {
	      error_status = ER_QSTR_FORMAT_DUPLICATION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  else
	    {
	      mil_time_count++;
	    }

	  error_status =
	    get_string_date_token_id (SDT_AM_PM, date_lang_id, cs,
				      &am_pm_id, &token_size);
	  if (error_status != NO_ERROR)
	    {
	      goto exit_on_error;
	    }

	  if (am_pm_id > 0)
	    {
	      if (am_pm_id % 2)
		{
		  am = true;
		}
	      else
		{
		  pm = true;
		}
	    }
	  else
	    {
	      error_status = ER_QSTR_INVALID_FORMAT;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }

	  cs += token_size;
	  break;

	case DT_H:
	  if (time_count != 0)
	    {
	      error_status = ER_QSTR_FORMAT_DUPLICATION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  else
	    {
	      time_count++;
	    }

	  k = parse_digits (cs, &hour, 1);
	  if (k <= 0)
	    {
	      error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  cs += k;
	  if (hour < 1 || hour > 12)
	    {
	      error_status = ER_TIME_CONVERSION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  break;

	case DT_HH:
	case DT_HH12:
	  if (time_count != 0)
	    {
	      error_status = ER_QSTR_FORMAT_DUPLICATION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  else
	    {
	      time_count++;
	    }

	  k = parse_digits (cs, &hour, 2);
	  if (k <= 0)
	    {
	      error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  cs += k;

	  if (hour < 1 || hour > 12)
	    {
	      error_status = ER_TIME_CONVERSION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  break;

	case DT_HH24:
	  if (time_count != 0)
	    {
	      error_status = ER_QSTR_FORMAT_DUPLICATION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  else
	    {
	      time_count++;
	    }

	  if (mil_time_count != 0)
	    {
	      error_status = ER_QSTR_FORMAT_DUPLICATION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  else
	    {
	      mil_time_count++;
	    }

	  k = parse_digits (cs, &hour, 2);
	  if (k <= 0)
	    {
	      error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  cs += k;

	  if (hour < 0 || hour > 23)
	    {
	      error_status = ER_TIME_CONVERSION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  break;

	case DT_MI:
	  k = parse_digits (cs, &minute, 2);
	  if (k <= 0)
	    {
	      error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  cs += k;

	  if (minute < 0 || minute > 59)
	    {
	      error_status = ER_TIME_CONVERSION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  break;

	case DT_SS:
	  k = parse_digits (cs, &second, 2);
	  if (k <= 0)
	    {
	      error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  cs += k;

	  if (second < 0 || second > 59)
	    {
	      error_status = ER_TIME_CONVERSION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  break;

	case DT_MS:
	  if (!char_isdigit (*cs))
	    {
	      error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }

	  for (i = 0, fraction = 100; char_isdigit (*cs); cs++, i++)
	    {
	      millisecond += (int) ((*cs - '0') * fraction + 0.5);
	      fraction /= 10;
	    }

	  if (millisecond < 0 || millisecond > 999)
	    {
	      error_status = ER_TIME_CONVERSION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  break;

	case DT_TEXT:
	  cmp =
	    intl_case_match_tok (date_lang_id,
				 (unsigned char *)
				 (cur_format_str_ptr + 1),
				 (unsigned char *) cs,
				 cur_format_size - 2, strlen (cs),
				 &cs_byte_size);

	  if (cmp != 0)
	    {
	      error_status = ER_QSTR_INVALID_FORMAT;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }

	  cs += cs_byte_size;
	  break;

	case DT_PUNCTUATION:
	  if (strncasecmp ((const char *) (void *) cur_format_str_ptr,
			   (const char *) cs, cur_format_size) != 0)
	    {
	      error_status = ER_QSTR_INVALID_FORMAT;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }

	  cs += cur_format_size;
	  break;

	case DT_CC:
	case DT_Q:
	  error_status = ER_QSTR_INVALID_FORMAT;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto exit_on_error;

	case DT_DAY:
	  if (day_of_the_weekcount != 0)
	    {
	      error_status = ER_QSTR_FORMAT_DUPLICATION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  else
	    {
	      day_of_the_weekcount++;
	    }

	  error_status = get_string_date_token_id (SDT_DAY,
						   date_lang_id, cs,
						   &day_of_the_week,
						   &token_size);
	  if (error_status != NO_ERROR)
	    {
	      goto exit_on_error;
	    }

	  cs += token_size;

	  if (day_of_the_week == 0)
	    {
	      error_status = ER_DATE_CONVERSION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  break;

	case DT_DY:
	  if (day_of_the_weekcount != 0)
	    {
	      error_status = ER_QSTR_FORMAT_DUPLICATION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  else
	    {
	      day_of_the_weekcount++;
	    }

	  error_status = get_string_date_token_id (SDT_DAY_SHORT,
						   date_lang_id, cs,
						   &day_of_the_week,
						   &token_size);
	  if (error_status != NO_ERROR)
	    {
	      goto exit_on_error;
	    }

	  cs += token_size;

	  if (day_of_the_week == 0)
	    {
	      error_status = ER_DATE_CONVERSION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  break;

	case DT_D:
	  if (day_of_the_weekcount != 0)
	    {
	      error_status = ER_QSTR_FORMAT_DUPLICATION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  else
	    {
	      day_of_the_weekcount++;
	    }

	  k = parse_digits (cs, &day_of_the_week, 1);
	  if (k <= 0)
	    {
	      error_status = ER_QSTR_MISMATCHING_ARGUMENTS;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  cs += k;

	  if (day_of_the_week < 1 || day_of_the_week > 7)
	    {
	      error_status = ER_DATE_CONVERSION;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  break;

	case DT_NORMAL:
	case DT_INVALID:
	  error_status = ER_QSTR_INVALID_FORMAT;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto exit_on_error;
	}

      while (cs < last_src && strchr (WHITE_CHARS, *cs))
	{
	  cs++;
	}

      cur_format_str_ptr = next_format_str_ptr;

      /* Skip space, tab, CR     */
      while (cur_format_str_ptr < last_format &&
	     strchr (WHITE_CHARS, *cur_format_str_ptr))
	{
	  cur_format_str_ptr++;
	}

      if (last_format == next_format_str_ptr)
	{
	  while (cs < last_src && strchr (WHITE_CHARS, *cs))
	    {
	      cs++;
	    }

	  if (cs != last_src)
	    {
	      error_status = ER_QSTR_INVALID_FORMAT;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  break;
	}
    }

  /* Both format and src should end at same time     */
  if (cs != last_src || cur_format_str_ptr != last_format)
    {
      error_status = ER_QSTR_INVALID_FORMAT;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  /**************            Check DATE        ****************/
#if 1				/* TODO - trace; do not delete me */
  if (yearcount == 0 || monthcount == 0 || daycount == 0)
    {
      error_status = ER_QSTR_INVALID_FORMAT;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }
#else
  year = (yearcount == 0) ? get_cur_year () : year;
  month = (monthcount == 0) ? get_cur_month () : month;
  day = (daycount == 0) ? 1 : day;
#endif
  week = (day_of_the_weekcount == 0) ? -1 : day_of_the_week - 1;

  if (week != -1 && week != db_get_day_of_week (year, month, day))
    {
      error_status = ER_DATE_CONVERSION;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  /**************            Check TIME        ****************/
  if (am == true && pm == false && hour <= 12)
    {				/* If A.M.    */
      hour = (hour == 12) ? 0 : hour;
    }
  else if (am == false && pm == true && hour <= 12)
    {				/* If P.M.    */
      hour = (hour == 12) ? hour : hour + 12;
    }
  else if (am == false && pm == false)
    {				/* If military time    */
      ;
    }
  else
    {
      error_status = ER_DATE_CONVERSION;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  /*************         Make DATETIME        *****************/
  error_status = db_datetime_encode (&tmp_datetime, month, day, year, hour,
				     minute, second, millisecond);
  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

  if (DB_MAKE_DATETIME (result_datetime, &tmp_datetime) != NO_ERROR)
    {
      error_status = ER_DATE_CONVERSION;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  if (do_free_buf_str)
    {
      free_and_init (initial_buf_str);
    }
  if (do_free_buf_format)
    {
      free_and_init (initial_buf_format);
    }

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_src_str, &tmp_format_str, &tmp_date_lang);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  if (do_free_buf_str)
    {
      free_and_init (initial_buf_str);
    }

  if (do_free_buf_format)
    {
      free_and_init (initial_buf_format);
    }

  DB_MAKE_NULL (result_datetime);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_src_str, &tmp_format_str, &tmp_date_lang);

  return error_status;
}

/*
 * adjust_precision () - Change representation of 'data' as of
 *    'precision' and 'scale'.
 *                       When data has invalid format, just return
 * return : DOMAIN_INCOMPATIBLE, DOMAIN_OVERFLOW, NO_ERROR
 *
 *  Note : This function is not localized in relation to fractional and digit
 *	   grouping symbols. It assumes the default symbols ('.' for fraction
 *	   symbol and ',' for digit grouping symbol)
 */
static int
adjust_precision (char *data, int precision, int scale)
{
  char tmp_data[DB_MAX_NUMERIC_PRECISION * 2 + 1];
  int scale_counter = 0;
  int i = 0;
  int before_dec_point = 0;
  int after_dec_point = 0;
  int space_started = false;

  if (data == NULL || precision < 0 || precision > DB_MAX_NUMERIC_PRECISION
      || scale < 0 || scale > DB_MAX_NUMERIC_PRECISION)
    {
      return DOMAIN_INCOMPATIBLE;
    }

  if (*data == '-')
    {
      tmp_data[0] = '-';
      i++;
    }
  else if (*data == '+')
    {
      i++;
    }

  for (; i < DB_MAX_NUMERIC_PRECISION && *(data + i) != '\0'
       && *(data + i) != '.'; i++)
    {
      if (char_isdigit (*(data + i)))
	{
	  tmp_data[i] = *(data + i);
	  before_dec_point++;
	}
      else if (char_isspace (*(data + i)))
	{
	  space_started = true;
	  break;
	}
      else
	{
	  return DOMAIN_INCOMPATIBLE;
	}
    }

  if (space_started == true)
    {
      int j = i;
      while (char_isspace (*(data + j)))
	{
	  j++;
	}

      if (*(data + j) != '\0')
	{
	  return DOMAIN_INCOMPATIBLE;
	}
    }

  if (*(data + i) == '.')
    {
      tmp_data[i] = '.';
      i++;
      while (*(data + i) != '\0' && scale_counter < scale)
	{
	  if (char_isdigit (*(data + i)))
	    {
	      tmp_data[i] = *(data + i);
	      after_dec_point++;
	    }
	  else if (char_isspace (*(data + i)))
	    {
	      space_started = true;
	      break;
	    }
	  else
	    {
	      return DOMAIN_INCOMPATIBLE;
	    }
	  scale_counter++;
	  i++;
	}

      if (space_started == true)
	{
	  int j = i;
	  while (char_isspace (*(data + j)))
	    {
	      j++;
	    }

	  if (*(data + j) != '\0')
	    {
	      return DOMAIN_INCOMPATIBLE;
	    }
	}

      while (scale_counter < scale)
	{
	  tmp_data[i] = '0';
	  scale_counter++;
	  i++;
	}

    }
  else if (*(data + i) == '\0')
    {
      tmp_data[i] = '.';
      i++;
      while (scale_counter < scale)
	{
	  tmp_data[i] = '0';
	  scale_counter++;
	  i++;
	}

    }
  else
    {
      return DOMAIN_COMPATIBLE;
    }

  if (before_dec_point + after_dec_point > DB_MAX_NUMERIC_PRECISION
      || after_dec_point > DB_DEFAULT_NUMERIC_PRECISION
      || before_dec_point > precision - scale)
    {
      return DOMAIN_OVERFLOW;
    }

  tmp_data[i] = '\0';
  strcpy (data, tmp_data);
  return NO_ERROR;
}

/*
 * db_to_number () -
 *
 *  Note :  This function is localized in relation to fractional and digit
 *	    grouping symbols.
 */
int
db_to_number (const DB_VALUE * src_str, const DB_VALUE * format_str,
	      const DB_VALUE * number_lang, DB_VALUE * result_num)
{
  /* default precision and scale is (38, 0) */
  /* it is more profitable that the definition of this value is located in
     some header file */
  const char *dflt_format_str = "99999999999999999999999999999999999999";

  int error_status = NO_ERROR;
  DB_VALUE tmp_src_str, tmp_format_str, tmp_number_lang;
  char *cs;			/* current source string pointer        */
  char *last_cs;
  char *format_str_ptr;
  char *last_format;
  char *next_fsp;		/* next format string pointer   */
  int token_length;
  int count_format = 0;
  int cur_format;

  int precision = 0;		/* retain precision of format_str */
  int scale = 0;
  int loopvar, met_decptr = 0;
  int use_default_precision = 0;

  char *first_cs_for_error, *first_format_str_for_error;

  char stack_buf_str[64], stack_buf_format[64];
  char *initial_buf_str = NULL, *initial_buf_format = NULL;
  bool do_free_buf_str = false, do_free_buf_format = false;
  char digit_grouping_symbol;
  char fraction_symbol;
  bool has_user_format;
  bool dummy;
  int number_lang_id;

  assert (src_str != (DB_VALUE *) NULL);
  assert (format_str != NULL);
  assert (number_lang != NULL);
  assert (result_num != (DB_VALUE *) NULL);
  assert (src_str != result_num);
  assert (format_str != result_num);
  assert (number_lang != result_num);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (3, &tmp_src_str, &tmp_format_str, &tmp_number_lang);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL (src_str))
    {
      DB_MAKE_NULL (result_num);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  src_str =
    db_value_cast_arg (src_str, &tmp_src_str, DB_TYPE_VARCHAR, &error_status);
  number_lang =
    db_value_cast_arg (number_lang, &tmp_number_lang, DB_TYPE_INTEGER,
		       &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  if (DB_GET_STRING_SIZE (src_str) == 0)
    {
      error_status = ER_QSTR_EMPTY_STRING;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  if (DB_GET_STRING_SIZE (src_str) > MAX_TOKEN_SIZE)
    {
      error_status = ER_QSTR_SRC_TOO_LONG;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  assert (DB_VALUE_TYPE (number_lang) == DB_TYPE_INTEGER);
  number_lang_id = lang_get_lang_id_from_flag (DB_GET_INT (number_lang),
					       &has_user_format, &dummy);
  digit_grouping_symbol = lang_digit_grouping_symbol (number_lang_id);
  fraction_symbol = lang_digit_fractional_symbol (number_lang_id);

  error_status =
    db_check_or_create_null_term_string (src_str, stack_buf_str,
					 sizeof (stack_buf_str),
					 true, false,
					 &initial_buf_str, &do_free_buf_str);
  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

  cs = initial_buf_str;
  last_cs = cs + strlen (cs);

  /* If there is no format */
  if (!has_user_format)
    {
      format_str_ptr = (char *) dflt_format_str;
      last_format = format_str_ptr + strlen (dflt_format_str);
    }
  else				/* format_str != NULL */
    {
      /*      Format string type checking     */

      /* arg cast type *******************************************************
       */
      format_str =
	db_value_cast_arg (format_str, &tmp_format_str, DB_TYPE_VARCHAR,
			   &error_status);

      if (error_status != NO_ERROR)
	{
	  assert (er_errid () != NO_ERROR);
	  goto exit_on_error;
	}

      if (DB_GET_STRING_SIZE (format_str) > MAX_TOKEN_SIZE)
	{
	  error_status = ER_QSTR_FORMAT_TOO_LONG;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto exit_on_error;
	}

      error_status =
	db_check_or_create_null_term_string (format_str, stack_buf_format,
					     sizeof (stack_buf_format),
					     true, false,
					     &initial_buf_format,
					     &do_free_buf_format);
      if (error_status != NO_ERROR)
	{
	  goto exit_on_error;
	}
      format_str_ptr = initial_buf_format;
      last_format = format_str_ptr + strlen (format_str_ptr);

      if (DB_GET_STRING_SIZE (format_str) == 0)
	{
	  error_status = ER_QSTR_EMPTY_STRING;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto exit_on_error;
	}
    }

  last_cs = (char *) intl_backskip_spaces (cs, last_cs - 1);
  last_cs = last_cs + 1;

  /* Skip space, tab, CR  */
  while (cs < last_cs && strchr (WHITE_CHARS, *cs))
    {
      cs++;
    }
  while (format_str_ptr < last_format
	 && strchr (WHITE_CHARS, *format_str_ptr))
    {
      format_str_ptr++;
    }
  first_cs_for_error = cs;
  first_format_str_for_error = format_str_ptr;

  /* get precision and scale of format_str */
  for (loopvar = 0; format_str_ptr + loopvar < last_format; loopvar++)
    {
      switch (*(format_str_ptr + loopvar))
	{
	case '9':
	case '0':
	  precision++;
	  if (met_decptr > 0)
	    {
	      scale++;
	    }
	  break;

	case 'c':
	case 'C':
	case 's':
	case 'S':
	  break;

	default:
	  if (*(format_str_ptr + loopvar) == digit_grouping_symbol)
	    {
	      break;
	    }
	  else if (*(format_str_ptr + loopvar) == fraction_symbol)
	    {
	      met_decptr++;
	      break;
	    }
	  precision = 0;
	  scale = 0;
	  use_default_precision = 1;
	}

      if (precision + scale > DB_MAX_NUMERIC_PRECISION)
	{
	  error_status = ER_NUM_OVERFLOW;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto exit_on_error;
	}

      if (use_default_precision == 1)
	{
	  /* scientific notation */
	  precision = DB_MAX_NUMERIC_PRECISION;
	  scale = DB_DEFAULT_NUMERIC_PRECISION;
	  break;
	}
    }

  /* Skip space, tab, CR  */
  while (cs < last_cs)
    {
      cur_format =
	get_number_token (number_lang_id, format_str_ptr, &token_length,
			  last_format, &next_fsp);
      switch (cur_format)
	{
	case N_FORMAT:
	  if (count_format != 0)
	    {
	      error_status = ER_QSTR_INVALID_FORMAT;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }

	  error_status = make_number (cs, last_cs,
				      format_str_ptr, &token_length,
				      result_num, precision,
				      scale, number_lang_id);
	  if (error_status == NO_ERROR)
	    {
	      count_format++;
	      cs += token_length;
	    }
	  else if (error_status == ER_NUM_OVERFLOW)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  else
	    {
	      goto format_mismatch;
	    }

	  break;

	case N_SPACE:
	  if (!strchr (WHITE_CHARS, *cs))
	    {
	      goto format_mismatch;
	    }

	  while (cs < last_cs && strchr (WHITE_CHARS, *cs))
	    {
	      cs++;
	    }
	  break;

	case N_TEXT:
	  if (strncasecmp ((format_str_ptr + 1), cs, token_length - 2) != 0)
	    {
	      goto format_mismatch;
	    }
	  cs += token_length - 2;
	  break;

	case N_INVALID:
	  error_status = ER_QSTR_INVALID_FORMAT;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto exit_on_error;

	case N_END:
	  /* Skip space, tab, CR  */
	  while (cs < last_cs && strchr (WHITE_CHARS, *cs))
	    {
	      cs++;
	    }

	  if (cs != last_cs)
	    {
	      goto format_mismatch;
	    }
	  break;
	}

      while (cs < last_cs && strchr (WHITE_CHARS, *cs))
	{
	  cs++;
	}

      format_str_ptr = next_fsp;

      /* Skip space, tab, CR  */
      while (format_str_ptr < last_format &&
	     strchr (WHITE_CHARS, *format_str_ptr))
	{
	  format_str_ptr++;
	}
    }

  /* Both format and src should end at same time  */
  if (cs != last_cs || format_str_ptr != last_format)
    {
      goto format_mismatch;
    }

  result_num->domain.numeric_info.precision = precision;
  result_num->domain.numeric_info.scale = scale;

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);


  if (do_free_buf_str)
    {
      free_and_init (initial_buf_str);
    }

  if (do_free_buf_format)
    {
      free_and_init (initial_buf_format);
    }

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_src_str, &tmp_format_str, &tmp_number_lang);

  return error_status;

format_mismatch:
  while (strchr (WHITE_CHARS, *(last_cs - 1)))
    {
      last_cs--;
    }
  *last_cs = '\0';

  error_status = ER_QSTR_TONUM_FORMAT_MISMATCH;
  if (first_format_str_for_error == dflt_format_str)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
	      first_cs_for_error, "default");
    }
  else
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
	      first_cs_for_error, first_format_str_for_error);
    }

exit_on_error:
  assert (error_status != NO_ERROR);

  if (do_free_buf_str)
    {
      free_and_init (initial_buf_str);
    }

  if (do_free_buf_format)
    {
      free_and_init (initial_buf_format);
    }

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  DB_MAKE_NULL (result_num);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_src_str, &tmp_format_str, &tmp_number_lang);

  return error_status;
}

/*
 * date_to_char () -
 */
static int
date_to_char (const DB_VALUE * src_value,
	      const DB_VALUE * format_str,
	      const DB_VALUE * date_lang, DB_VALUE * result_str)
{
  int error_status = NO_ERROR;
  DB_TYPE src_type;
  char *cur_format_str_ptr, *next_format_str_ptr;
  char *last_format_str_ptr;

  int cur_format_size;
  int cur_format;

  char *result_buf = NULL;
  int result_len = 0;
  int result_size = 0;

  int month = 0, day = 0, year = 0;
  int second = 0, minute = 0, hour = 0, millisecond = 0;

  int i;

  unsigned int tmp_int;

  bool no_user_format;
  INTL_LANG date_lang_id;

  char stack_buf_format[64];
  char *initial_buf_format = NULL;
  bool do_free_buf_format = false;
  const int collation_id = LANG_COERCIBLE_COLL;
  bool has_user_format = false;
  bool dummy;

  assert (src_value != (DB_VALUE *) NULL);
  assert (result_str != (DB_VALUE *) NULL);
  assert (date_lang != (DB_VALUE *) NULL);

  if (DB_IS_NULL (src_value))
    {
      DB_MAKE_NULL (result_str);
      return error_status;
    }

  src_type = DB_VALUE_DOMAIN_TYPE (src_value);

  if (src_type != DB_TYPE_DATE && src_type != DB_TYPE_TIME
      && src_type != DB_TYPE_DATETIME)
    {
      error_status = ER_TP_CANT_COERCE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
	      pr_type_name (src_type), pr_type_name (DB_TYPE_DATETIME));
      return error_status;
    }

  if (date_lang == NULL || DB_IS_NULL (date_lang))
    {
      error_status = ER_OBJ_INVALID_ARGUMENTS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  assert (DB_VALUE_TYPE (date_lang) == DB_TYPE_INTEGER);
  date_lang_id = lang_get_lang_id_from_flag (DB_GET_INT (date_lang),
					     &has_user_format, &dummy);

  no_user_format = (format_str == NULL) || (!has_user_format);

  if (no_user_format)
    {
      int retval = 0;
      switch (src_type)
	{
	case DB_TYPE_DATE:
	  result_buf = (char *) malloc (QSTR_DATE_LENGTH + 1);
	  if (result_buf == NULL)
	    {
	      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	      return error_status;
	    }
	  result_len = QSTR_DATE_LENGTH;
	  retval = db_date_to_string (result_buf,
				      QSTR_DATE_LENGTH + 1,
				      DB_GET_DATE (src_value));
	  break;

	case DB_TYPE_TIME:
	  result_buf = (char *) malloc (QSTR_TIME_LENGTH + 1);
	  if (result_buf == NULL)
	    {
	      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	      return error_status;
	    }
	  result_len = QSTR_TIME_LENGTH;
	  retval = db_time_to_string (result_buf,
				      QSTR_TIME_LENGTH + 1,
				      DB_GET_TIME (src_value));
	  break;

	case DB_TYPE_DATETIME:
	  result_buf = (char *) malloc (QSTR_DATETIME_LENGTH + 1);
	  if (result_buf == NULL)
	    {
	      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	      return error_status;
	    }
	  result_len = QSTR_DATETIME_LENGTH;
	  retval = db_datetime_to_string (result_buf,
					  QSTR_DATETIME_LENGTH + 1,
					  DB_GET_DATETIME (src_value));
	  break;

	default:
	  break;
	}

      if (retval == 0)
	{
	  error_status = ER_OBJ_INVALID_ARGUMENTS;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  free_and_init (result_buf);
	  return error_status;
	}

      DB_MAKE_VARCHAR (result_str, result_len, result_buf, result_len,
		       collation_id);
    }
  else
    {
      assert (!DB_IS_NULL (date_lang));

      if (DB_IS_NULL (format_str))
	{
	  DB_MAKE_NULL (result_str);
	  goto exit;
	}

      /* compute allocation size : trade-off exact size (and small mem usage)
       * vs speed */
      result_len = (DB_GET_STRING_LENGTH (format_str)
		    * QSTR_TO_CHAR_LEN_MULTIPLIER_RATIO);
      result_size = result_len * INTL_CODESET_MULT;
      if (result_size > MAX_TOKEN_SIZE)
	{
	  error_status = ER_QSTR_FORMAT_TOO_LONG;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  return error_status;
	}

      if (DB_GET_STRING_SIZE (format_str) == 0)
	{
	  error_status = ER_QSTR_EMPTY_STRING;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto exit;
	}

      error_status =
	db_check_or_create_null_term_string (format_str, stack_buf_format,
					     sizeof (stack_buf_format),
					     true, false,
					     &initial_buf_format,
					     &do_free_buf_format);

      if (error_status != NO_ERROR)
	{
	  goto exit;
	}
      cur_format_str_ptr = initial_buf_format;
      last_format_str_ptr = cur_format_str_ptr + strlen (cur_format_str_ptr);

      switch (src_type)
	{
	case DB_TYPE_DATE:
	  db_date_decode (DB_GET_DATE (src_value), &month, &day, &year);
	  break;
	case DB_TYPE_TIME:
	  db_time_decode (DB_GET_TIME (src_value), &hour, &minute, &second);
	  break;
	case DB_TYPE_DATETIME:
	  db_datetime_decode (DB_GET_DATETIME (src_value), &month, &day,
			      &year, &hour, &minute, &second, &millisecond);
	  break;
	default:
	  break;
	}

      result_buf = (char *) malloc (result_size + 1);
      if (result_buf == NULL)
	{
	  error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	  goto exit;
	}

#if !defined(NDEBUG)
      /* suppress valgrind UMW error */
      memset (result_buf, 0, result_size + 1);
#endif

      i = 0;
      cur_format = DT_NORMAL;

      while (i < result_size)
	{
	  int token_case_mode;
	  int token_size;

	  cur_format = get_next_format (cur_format_str_ptr,
					src_type, &cur_format_size,
					&next_format_str_ptr);
	  switch (cur_format)
	    {
	    case DT_CC:
	      if (month == 0 && day == 0 && year == 0)
		{
		  goto zerodate_exit;
		}

	      tmp_int = (year / 100) + 1;
	      sprintf (&result_buf[i], "%02d\n", tmp_int);
	      i += 2;
	      break;

	    case DT_YYYY:
	      sprintf (&result_buf[i], "%04d\n", year);
	      i += 4;
	      break;

	    case DT_YY:
	      tmp_int = year - (year / 100) * 100;
	      sprintf (&result_buf[i], "%02d\n", tmp_int);
	      i += 2;
	      break;

	    case DT_MM:
	      sprintf (&result_buf[i], "%02d\n", month);
	      i += 2;
	      break;

	    case DT_MONTH:
	    case DT_MON:
	      if (month == 0 && day == 0 && year == 0)
		{
		  goto zerodate_exit;
		}

	      if (*cur_format_str_ptr == 'm')
		{
		  token_case_mode = 1;
		}
	      else if (*(cur_format_str_ptr + 1) == 'O')
		{
		  token_case_mode = 2;
		}
	      else
		{
		  token_case_mode = 0;
		}

	      if (cur_format == DT_MONTH)
		{
		  error_status =
		    print_string_date_token (SDT_MONTH, date_lang_id,
					     month - 1, token_case_mode,
					     &result_buf[i], &token_size);
		}
	      else		/* cur_format == DT_MON */
		{
		  error_status =
		    print_string_date_token (SDT_MONTH_SHORT, date_lang_id,
					     month - 1,
					     token_case_mode, &result_buf[i],
					     &token_size);
		}

	      if (error_status != NO_ERROR)
		{
		  free_and_init (result_buf);
		  goto exit;
		}

	      i += token_size;
	      break;

	    case DT_Q:
	      if (month == 0 && day == 0 && year == 0)
		{
		  goto zerodate_exit;
		}

	      result_buf[i] = '1' + ((month - 1) / 3);
	      i++;
	      break;

	    case DT_DD:
	      sprintf (&result_buf[i], "%02d\n", day);
	      i += 2;
	      break;

	    case DT_DAY:
	    case DT_DY:
	      if (month == 0 && day == 0 && year == 0)
		{
		  goto zerodate_exit;
		}

	      tmp_int = get_day (month, day, year);

	      if (*cur_format_str_ptr == 'd')
		{
		  token_case_mode = 1;
		}
	      else if (*(cur_format_str_ptr + 1) == 'A')	/* "DAY" */
		{
		  token_case_mode = 2;
		}
	      else if (*(cur_format_str_ptr + 1) == 'Y')	/* "DY" */
		{
		  token_case_mode = 2;
		}
	      else
		{
		  token_case_mode = 0;
		}

	      if (cur_format == DT_DAY)
		{
		  error_status =
		    print_string_date_token (SDT_DAY, date_lang_id,
					     tmp_int, token_case_mode,
					     &result_buf[i], &token_size);
		}
	      else		/* cur_format == DT_DY */
		{
		  error_status =
		    print_string_date_token (SDT_DAY_SHORT, date_lang_id,
					     tmp_int,
					     token_case_mode, &result_buf[i],
					     &token_size);
		}

	      if (error_status != NO_ERROR)
		{
		  free_and_init (result_buf);
		  goto exit;
		}

	      i += token_size;
	      break;

	    case DT_D:
	      if (month == 0 && day == 0 && year == 0)
		{
		  goto zerodate_exit;
		}

	      tmp_int = get_day (month, day, year);
	      result_buf[i] = '0' + tmp_int + 1;	/* sun=1 */
	      i += 1;
	      break;

	    case DT_AM:
	    case DT_PM:
	      {
		int am_pm_id = -1;
		int am_pm_len = 0;

		if (0 <= hour && hour <= 11)
		  {
		    if (*cur_format_str_ptr == 'a'
			|| *cur_format_str_ptr == 'p')
		      {
			am_pm_id = (int) am_NAME;
		      }
		    else if (*(cur_format_str_ptr + 1) == 'm')
		      {
			am_pm_id = (int) Am_NAME;
		      }
		    else
		      {
			am_pm_id = (int) AM_NAME;
		      }
		  }
		else if (12 <= hour && hour <= 23)
		  {
		    if (*cur_format_str_ptr == 'p'
			|| *cur_format_str_ptr == 'a')
		      {
			am_pm_id = (int) pm_NAME;
		      }
		    else if (*(cur_format_str_ptr + 1) == 'm')
		      {
			am_pm_id = (int) Pm_NAME;
		      }
		    else
		      {
			am_pm_id = (int) PM_NAME;
		      }
		  }
		else
		  {
		    error_status = ER_QSTR_INVALID_FORMAT;
		    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status,
			    0);
		    free_and_init (result_buf);
		    goto exit;
		  }

		assert (am_pm_id >= (int) am_NAME &&
			am_pm_id <= (int) P_M_NAME);

		error_status =
		  print_string_date_token (SDT_AM_PM, date_lang_id,
					   am_pm_id, 0, &result_buf[i],
					   &am_pm_len);

		if (error_status != NO_ERROR)
		  {
		    free_and_init (result_buf);
		    goto exit;
		  }

		i += am_pm_len;
	      }
	      break;

	    case DT_A_M:
	    case DT_P_M:
	      {
		int am_pm_id = -1;
		int am_pm_len = 0;

		if (0 <= hour && hour <= 11)
		  {
		    if (*cur_format_str_ptr == 'a'
			|| *cur_format_str_ptr == 'p')
		      {
			am_pm_id = (int) a_m_NAME;
		      }
		    else if (*(cur_format_str_ptr + 2) == 'm')
		      {
			am_pm_id = (int) A_m_NAME;
		      }
		    else
		      {
			am_pm_id = (int) A_M_NAME;
		      }
		  }
		else if (12 <= hour && hour <= 23)
		  {
		    if (*cur_format_str_ptr == 'p'
			|| *cur_format_str_ptr == 'a')
		      {
			am_pm_id = (int) p_m_NAME;
		      }
		    else if (*(cur_format_str_ptr + 2) == 'm')
		      {
			am_pm_id = (int) P_m_NAME;
		      }
		    else
		      {
			am_pm_id = (int) P_M_NAME;
		      }
		  }
		else
		  {
		    error_status = ER_QSTR_INVALID_FORMAT;
		    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status,
			    0);
		    free_and_init (result_buf);
		    goto exit;
		  }

		assert (am_pm_id >= (int) am_NAME &&
			am_pm_id <= (int) P_M_NAME);

		error_status =
		  print_string_date_token (SDT_AM_PM, date_lang_id,
					   am_pm_id, 0, &result_buf[i],
					   &am_pm_len);

		if (error_status != NO_ERROR)
		  {
		    free_and_init (result_buf);
		    goto exit;
		  }

		i += am_pm_len;
	      }
	      break;

	    case DT_HH:
	    case DT_HH12:
	      tmp_int = hour % 12;
	      if (tmp_int == 0)
		{
		  tmp_int = 12;
		}
	      sprintf (&result_buf[i], "%02d\n", tmp_int);
	      i += 2;
	      break;

	    case DT_HH24:
	      sprintf (&result_buf[i], "%02d\n", hour);
	      i += 2;
	      break;

	    case DT_MI:
	      sprintf (&result_buf[i], "%02d\n", minute);
	      i += 2;
	      break;

	    case DT_SS:
	      sprintf (&result_buf[i], "%02d\n", second);
	      i += 2;
	      break;

	    case DT_MS:
	      sprintf (&result_buf[i], "%03d\n", millisecond);
	      i += 3;
	      break;

	    case DT_INVALID:
	      error_status = ER_QSTR_INVALID_FORMAT;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      free_and_init (result_buf);
	      goto exit;

	    case DT_NORMAL:
	      memcpy (&result_buf[i], cur_format_str_ptr, cur_format_size);
	      i += cur_format_size;
	      break;

	    case DT_TEXT:
	      memcpy (&result_buf[i], cur_format_str_ptr + 1,
		      cur_format_size - 2);
	      i += cur_format_size - 2;
	      break;

	    case DT_PUNCTUATION:
	      memcpy (&result_buf[i], cur_format_str_ptr, cur_format_size);
	      i += cur_format_size;
	      break;

	    default:
	      error_status = ER_QSTR_INVALID_FORMAT;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      free_and_init (result_buf);
	      goto exit;
	      break;
	    }

	  cur_format_str_ptr = next_format_str_ptr;
	  if (next_format_str_ptr == last_format_str_ptr)
	    {
	      break;
	    }
	}

      assert (i < result_size);
      DB_MAKE_VARCHAR (result_str, result_len, result_buf, i, collation_id);
    }

  result_str->need_clear = true;

exit:
  if (do_free_buf_format)
    {
      free_and_init (initial_buf_format);
    }
  return error_status;

zerodate_exit:
  if (result_buf != NULL)
    {
      free_and_init (result_buf);
    }
  DB_MAKE_NULL (result_str);
  goto exit;
}

/*
 * number_to_char () -
 *
 *  Note :  This function is localized in relation to fractional and digit
 *	    grouping symbols.
 */
static int
number_to_char (const DB_VALUE * src_value,
		const DB_VALUE * format_str,
		const DB_VALUE * number_lang, DB_VALUE * result_str)
{
  int error_status = NO_ERROR;
  char tmp_str[64];
  char *tmp_buf;

  char *cs = NULL;		/* current source string pointer     */
  char *format_str_ptr, *last_format;
  char *next_fsp;		/* next format string pointer    */
  int token_length = 0;
  int cur_format;
  char *res_string, *res_ptr;
  int i, j;
  char stack_buf_format[64];
  char *initial_buf_format = NULL;
  bool do_free_buf_format = false;
  INTL_LANG number_lang_id;
  char fraction_symbol;
  char digit_grouping_symbol;
  bool has_user_format = false;
  bool dummy;
  const int collation_id = LANG_COERCIBLE_COLL;

  assert (src_value != (DB_VALUE *) NULL);
  assert (result_str != (DB_VALUE *) NULL);
  assert (number_lang != (DB_VALUE *) NULL);

  if (number_lang == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
	      0);
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  /* now return null */
  if (DB_IS_NULL (src_value))
    {
      DB_MAKE_NULL (result_str);
      return error_status;
    }

  number_lang_id = lang_get_lang_id_from_flag (DB_GET_INT (number_lang),
					       &has_user_format, &dummy);
  fraction_symbol = lang_digit_fractional_symbol (number_lang_id);
  digit_grouping_symbol = lang_digit_grouping_symbol (number_lang_id);

  switch (DB_VALUE_TYPE (src_value))
    {
    case DB_TYPE_NUMERIC:
      tmp_buf = numeric_db_value_print ((DB_VALUE *) src_value);
      cs = (char *) malloc (strlen (tmp_buf) + 1);
      if (cs == NULL)
	{
	  error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	  return error_status;
	}
      assert (number_lang_id == INTL_LANG_ENGLISH);
      strcpy (cs, tmp_buf);
      break;

    case DB_TYPE_INTEGER:
      sprintf (tmp_str, "%d", DB_GET_INTEGER (src_value));
      cs = (char *) malloc (strlen (tmp_str) + 1);
      if (cs == NULL)
	{
	  error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	  return error_status;
	}
      strcpy (cs, tmp_str);
      break;

    case DB_TYPE_BIGINT:
      sprintf (tmp_str, "%lld", (long long) DB_GET_BIGINT (src_value));
      cs = (char *) malloc (strlen (tmp_str) + 1);
      if (cs == NULL)
	{
	  error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	  return error_status;
	}
      strcpy (cs, tmp_str);
      break;

    case DB_TYPE_DOUBLE:
      sprintf (tmp_str, "%.15e", DB_GET_DOUBLE (src_value));
      assert (number_lang_id == INTL_LANG_ENGLISH);
      if (scientific_to_decimal_string (number_lang_id, tmp_str, &cs) !=
	  NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OBJ_INVALID_ARGUMENTS, 0);
	  return ER_OBJ_INVALID_ARGUMENTS;
	}
      break;

    default:
      error_status = ER_OBJ_INVALID_ARGUMENTS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      return error_status;
    }

  /*        Remove    'trailing zero' source string    */
  for (i = 0; i < strlen (cs); i++)
    {
      if (cs[i] == fraction_symbol)
	{
	  i = strlen (cs);
	  i--;
	  while (cs[i] == '0')
	    {
	      i--;
	    }
	  if (cs[i] == fraction_symbol)
	    {
	      cs[i] = '\0';
	    }
	  else
	    {
	      i++;
	      cs[i] = '\0';
	    }
	  break;
	}
    }

  if (format_str == NULL || !has_user_format)
    {
      /*    Caution: VARCHAR's Size        */
      DB_MAKE_VARCHAR (result_str, (ssize_t) strlen (cs), cs, strlen (cs),
		       collation_id);
      result_str->need_clear = true;
      return error_status;
    }
  else
    {
      if (DB_IS_NULL (format_str))
	{
	  free_and_init (cs);
	  DB_MAKE_NULL (result_str);
	  return error_status;
	}

      /*    Format string type checking     */
      if (is_char_string (format_str))
	{
	  if (DB_GET_STRING_SIZE (format_str) > MAX_TOKEN_SIZE)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      free_and_init (cs);
	      return error_status;
	    }

	  error_status =
	    db_check_or_create_null_term_string (format_str, stack_buf_format,
						 sizeof (stack_buf_format),
						 true, false,
						 &initial_buf_format,
						 &do_free_buf_format);

	  if (error_status != NO_ERROR)
	    {
	      free_and_init (cs);
	      goto exit;
	    }
	  format_str_ptr = initial_buf_format;
	  last_format = format_str_ptr + strlen (format_str_ptr);
	}
      else
	{
	  error_status = ER_TP_CANT_COERCE;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
		  pr_type_name (DB_VALUE_DOMAIN_TYPE (format_str)),
		  pr_type_name (DB_TYPE_VARCHAR));
	  free_and_init (cs);
	  return error_status;
	}

      if (DB_GET_STRING_SIZE (format_str) == 0)
	{
	  error_status = ER_QSTR_EMPTY_STRING;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  free_and_init (cs);
	  goto exit;
	}

      /*    Memory allocation for result                            */
      /*    size is bigger two times than strlen(format_str_ptr)    */
      /*        because of format 'C'(currency)                        */
      /*        'C' can be  expanded accoding to CODE_SET            */
      /*        +1 implies minus -                                     */
      res_string = (char *) malloc (strlen (format_str_ptr) * 2 + 1);
      if (res_string == NULL)
	{
	  free_and_init (cs);
	  error_status = er_errid ();
	  goto exit;
	}

      res_ptr = res_string;

      /* Skip space, tab, CR     */
      while (strchr (WHITE_CHARS, *cs))
	{
	  cs++;
	}

      while (format_str_ptr != last_format)
	{
	  cur_format = get_number_token (number_lang_id, format_str_ptr,
					 &token_length, last_format,
					 &next_fsp);
	  switch (cur_format)
	    {
	    case N_FORMAT:
	      if (make_number_to_char (number_lang_id, cs, format_str_ptr,
				       &token_length, &res_ptr) != NO_ERROR)
		{
		  error_status = ER_QSTR_INVALID_FORMAT;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  free_and_init (cs);
		  free_and_init (res_string);
		  goto exit;
		}
	      /*    Remove space character between sign,curerency and number */
	      i = 0;
	      j = 0;
	      while (i < token_length)
		{
		  if (res_ptr[i] == '+' || res_ptr[i] == '-')
		    {
		      i += 1;
		    }
		  else if (res_ptr[i] == ' ')
		    {
		      while (res_ptr[i + j] == ' ')
			{
			  j++;
			}
		      while (i > 0)
			{
			  i--;
			  res_ptr[i + j] = res_ptr[i];
			  res_ptr[i] = ' ';
			}
		      break;
		    }
		  else
		    {
		      break;
		    }
		}
	      res_ptr += token_length;
	      break;
	    case N_SPACE:
	      strncpy (res_ptr, format_str_ptr, token_length);
	      res_ptr += token_length;
	      break;
	    case N_TEXT:
	      strncpy (res_ptr, (format_str_ptr + 1), token_length - 2);
	      res_ptr += token_length - 2;
	      break;
	    case N_INVALID:
	      error_status = ER_QSTR_INVALID_FORMAT;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      free_and_init (cs);
	      free_and_init (res_string);
	      goto exit;
	    case N_END:
	      *res_ptr = '\0';
	      break;
	    }

	  format_str_ptr = next_fsp;
	}

      *res_ptr = '\0';
    }

  /* Both format and src should end at same time     */
  if (format_str_ptr != last_format)
    {
      error_status = ER_QSTR_INVALID_FORMAT;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      free_and_init (cs);
      free_and_init (res_string);

      goto exit;
    }

  DB_MAKE_VARCHAR (result_str, (ssize_t) strlen (res_string), res_string,
		   strlen (res_string), collation_id);
  result_str->need_clear = true;
  free_and_init (cs);

exit:
  if (do_free_buf_format)
    {
      free_and_init (initial_buf_format);
    }
  return error_status;
}

/*
 * make_number_to_char () -
 *
 *  Note :  This function is localized in relation to fractional and digit
 *	    grouping symbols.
 */
static int
make_number_to_char (const INTL_LANG lang, char *num_string,
		     char *format_str, int *length, char **result_str)
{
  int flag_sign = 1;
  int leadingzero = false;
  char *res_str = *result_str;
  char *num, *format, *res;
  char *init_format = format_str;

  char format_end_char = init_format[*length];
  const char fraction_symbol = lang_digit_fractional_symbol (lang);
  const char digit_grouping_symbol = lang_digit_grouping_symbol (lang);

  init_format[*length] = '\0';

  /* code for patch..     emm..   */
  if (strlen (format_str) == 5 && !strncasecmp (format_str, "seeee", 5))
    {
      return ER_FAILED;
    }
  else if (strlen (format_str) == 5 && !strncasecmp (format_str, "ceeee", 5))
    {
      return ER_FAILED;
    }
  else if (strlen (format_str) == 6 && !strncasecmp (format_str, "sceeee", 6))
    {
      return ER_FAILED;
    }

  /*              Check minus                     */
  if (*num_string == '-')
    {
      *res_str = '-';
      num_string++;
      res_str++;
      flag_sign = -1;
    }

  /*              Check sign                      */
  if (char_tolower (*format_str) == 's')
    {
      if (flag_sign == 1)
	{
	  *res_str = '+';
	  res_str++;
	}
      format_str++;
    }

  if (*format_str == '\0')
    {
      init_format[*length] = format_end_char;
      /* patch for format: '9999 s'   */
      *res_str = '\0';

      *length = strlen (*result_str);
      return NO_ERROR;
    }

  if (*format_str == '\0')
    {
      init_format[*length] = format_end_char;
      /* patch for format: '9999 s'   */
      *res_str = '\0';
      *length = strlen (*result_str);

      return NO_ERROR;
    }

  /* So far, format:'s','c' are settled   */
  if (*length > 4 && !strncasecmp (&init_format[*length - 4], "eeee", 4))
    {
      int cipher = 0;

      num = num_string;
      format = format_str;

      if (*num == '0')
	{
	  num++;
	  if (*num == '\0')
	    {
	      while (*format == '0' || *format == '9' ||
		     *format == digit_grouping_symbol)
		{
		  format++;
		}

	      if (*format == fraction_symbol)
		{
		  *res_str = '0';
		  res_str++;

		  format++;

		  *res_str = fraction_symbol;
		  res_str++;

		  while (1)
		    {
		      if (*format == '0' || *format == '9')
			{
			  *res_str = '0';
			  res_str++;
			  format++;
			}
		      else if (char_tolower (*format) == 'e')
			{
			  *res_str = '\0';
			  init_format[*length] = format_end_char;
			  make_scientific_notation (*result_str, cipher);
			  *length = strlen (*result_str);

			  return NO_ERROR;
			}
		      else
			{
			  return ER_FAILED;
			}
		    }
		}
	      else if (*format == 'e')
		{
		  *res_str = '0';
		  res_str++;
		  *res_str = '\0';
		  init_format[*length] = format_end_char;
		  make_scientific_notation (*result_str, cipher);
		  *length = strlen (*result_str);

		  return NO_ERROR;
		}
	      else
		{
		  return ER_FAILED;
		}
	    }
	  else if (*num == fraction_symbol)
	    {
	      num++;
	      while (1)
		{
		  if (*num == '0')
		    {
		      cipher--;
		      num++;
		    }
		  else if (char_isdigit (*num))
		    {
		      cipher--;
		      break;
		    }
		  else if (char_tolower (*num) == 'e')
		    {
		      break;
		    }
		  else if (*num == '\0')
		    {
		      return ER_FAILED;
		    }
		  else
		    {
		      return ER_FAILED;
		    }
		}
	    }
	  else
	    {
	      return ER_FAILED;
	    }
	}
      else
	{
	  while (1)
	    {
	      if (char_isdigit (*num))
		{
		  cipher++;
		  num++;
		}
	      else if (*num == fraction_symbol || *num == '\0')
		{
		  cipher--;
		  break;
		}
	      else
		{
		  return ER_FAILED;
		}
	    }
	}

      while (*format == '0' || *format == '9' ||
	     *format == digit_grouping_symbol)
	{
	  format++;
	}

      if (*format != fraction_symbol && char_tolower (*format) != 'e')
	{
	  return ER_FAILED;
	}

      num = num_string;
      res = res_str;

      while (1)
	{
	  if ('0' < *num && *num <= '9')
	    {
	      *res = *num;
	      res++;
	      num++;
	      break;
	    }
	  else
	    {
	      num++;
	    }
	}

      if (char_tolower (*format) == 'e')
	{
	  *res = '\0';
	  if (*num == fraction_symbol)
	    {
	      num++;
	      if (char_isdigit (*num) && *num - '0' > 4)
		{
		  roundoff (lang, *result_str, 1, &cipher, (char *) NULL);
		}
	    }
	  else if (char_isdigit (*num))
	    {
	      if (char_isdigit (*num) && *num - '0' > 4)
		{
		  roundoff (lang, *result_str, 1, &cipher, (char *) NULL);
		}
	    }
	  else if (*num == '\0')
	    {
	      /* do nothing */
	    }
	  else
	    {
	      return ER_FAILED;
	    }

	  /*      emm     */
	  init_format[*length] = format_end_char;
	  make_scientific_notation (*result_str, cipher);
	  *length = strlen (*result_str);

	  return NO_ERROR;
	}
      else
	{
	  *res = *format;
	  res++;
	  format++;
	}

      while (1)
	{
	  if (*format == '0' || *format == '9')
	    {
	      if (*num == fraction_symbol)
		{
		  num++;
		  *res = *num;
		}
	      else if (*num == '\0')
		{
		  while (*format == '0' || *format == '9')
		    {
		      *res = '0';
		      format++;
		      res++;
		    }

		  if (char_tolower (*format) != 'e')
		    {
		      return ER_FAILED;
		    }

		  *res = '\0';
		  init_format[*length] = format_end_char;
		  make_scientific_notation (*result_str, cipher);
		  *length = strlen (*result_str);

		  return NO_ERROR;
		}
	      else
		{
		  *res = *num;
		}

	      format++;
	      res++;
	      num++;
	    }
	  else if (char_tolower (*format) == 'e')
	    {
	      if (strlen (format) > 4)
		{
		  return ER_FAILED;
		}

	      if (*num == '\0')
		{
		  *res = '\0';
		  init_format[*length] = format_end_char;
		  make_scientific_notation (*result_str, cipher);
		  *length = strlen (*result_str);

		  return NO_ERROR;
		}
	      else
		{
		  *res = '\0';
		  /*      patch                   */
		  if (*num == fraction_symbol && *(num + 1) - '0' > 4)
		    {
		      roundoff (lang, *result_str, 1, &cipher, (char *) NULL);
		    }
		  if (*num - '0' > 4)
		    {
		      roundoff (lang, *result_str, 1, &cipher, (char *) NULL);
		    }
		  /*      emm     */
		  init_format[*length] = format_end_char;
		  make_scientific_notation (*result_str, cipher);
		  *length = strlen (*result_str);

		  return NO_ERROR;
		}
	    }
	  else
	    {
	      return ER_FAILED;
	    }
	}
    }
  /* So far, format:scientific notation are settled       */

  /*              Check leading zero              */
  if (*format_str == '0')
    {
      leadingzero = true;
    }

  num = num_string;
  format = format_str;

  /*      Scan unitl '.' or '\0' of both num or format    */
  while (char_isdigit (*num))
    {
      num++;
    }

  while (*format == '0' || *format == '9' || *format == digit_grouping_symbol)
    {
      format++;
    }

  if (*format != fraction_symbol && *format != '\0')
    {
      return ER_FAILED;
    }

  /* '.' or '\0' is copied into middle or last position of res_string */
  *(res_str + (format - format_str)) = *format;
  res = res_str + (format - format_str);

  /*      num: .xxx       format: .xxx    */
  if (format == format_str && num == num_string)
    {
      ;
    }
  /*      num: .xxx       format: xxx.xxx */
  else if (format != format_str && num == num_string)
    {
      if (leadingzero == true)
	{
	  while (format != format_str)
	    {
	      format--;

	      if (*format == '9' || *format == '0')
		{
		  *(res_str + (format - format_str)) = '0';
		}
	      else if (*format == digit_grouping_symbol)
		{
		  *(res_str + (format - format_str)) = digit_grouping_symbol;
		}
	      else
		{
		  return ER_FAILED;
		}
	    }
	}
      else
	{
	  while (format != format_str)
	    {
	      format--;
	      *(res_str + (format - format_str)) = ' ';
	    }
	}
    }
  /*      num: xxx.xxx    format: .xxx    */
  else if (format == format_str && num != num_string)
    {
      while (num != num_string)
	{
	  num--;
	  if (*num != '0')
	    {
	      /*      Make num be different from num_string   */
	      num = num_string + 1;
	      break;
	    }
	}
    }
  /*      num: xxx.xxx    format: xxx.xxx */
  else
    {
      format--;
      num--;
      /*      if      size of format string is 1              */
      if (format == format_str)
	{
	  *res_str = *num;
	}
      else
	{
	  while (format != format_str)
	    {
	      if (*format == digit_grouping_symbol)
		{
		  *(res_str + (format - format_str)) = *format;
		}
	      else if ((*format == '9' || *format == '0')
		       && num != num_string)
		{
		  *(res_str + (format - format_str)) = *num;
		  num--;
		}
	      else
		{
		  *(res_str + (format - format_str)) = *num;
		  if (leadingzero == true)
		    {
		      while (format != format_str)
			{
			  format--;
			  if (*format == '9' || *format == '0')
			    {
			      *(res_str + (format - format_str)) = '0';
			    }
			  else if (*format == digit_grouping_symbol)
			    {
			      *(res_str + (format - format_str)) =
				digit_grouping_symbol;
			    }
			  else
			    {
			      return ER_FAILED;
			    }
			}
		    }
		  else
		    {
		      while (format != format_str)
			{
			  format--;
			  *(res_str + (format - format_str)) = ' ';
			}
		    }
		  break;
		}
	      format--;
	      if (format == format_str && num == num_string)
		{
		  *(res_str + (format - format_str)) = *num;
		}
	    }
	}
    }

  if (num != num_string)
    {
      int i;

      i = strlen (init_format) - 1;
      while (init_format != &init_format[i])
	{
	  if (init_format[i] == fraction_symbol)
	    {
	      break;
	    }
	  else if (init_format[i] != '0' && init_format[i] != '9' &&
		   init_format[i] != 's' && init_format[i] != 'c' &&
		   init_format[i] != digit_grouping_symbol)
	    {
	      return ER_FAILED;
	    }
	  else
	    {
	      i--;
	    }
	}

      i = 0;
      while (i < *length)
	{
	  (*result_str)[i] = '#';
	  i++;
	}

      (*result_str)[*length] = '\0';
      init_format[*length] = format_end_char;

      return NO_ERROR;
    }
  /* So far, Left side of decimal point is settled        */

  while (char_isdigit (*num))
    {
      num++;
    }

  while (*format == '0' || *format == '9' || *format == digit_grouping_symbol)
    {
      format++;
    }

  if (*format != fraction_symbol && *format != '\0')
    {
      return ER_FAILED;
    }

  if (*format == fraction_symbol && *num == fraction_symbol)
    {
      res++;
      format++;
      num++;

      while (*format != '\0')
	{
	  if ((*format == '9' || *format == '0') && *num != '\0')
	    {
	      *res = *num;
	      num++;
	      res++;
	    }
	  else
	    {
	      while (*format != '\0')
		{
		  if (*format == '9' || *format == '0')
		    {
		      *res = '0';
		    }
		  else
		    {
		      return ER_FAILED;
		    }

		  format++;
		  res++;
		}

	      *res = '\0';
	      break;
	    }

	  format++;
	}

      *res = '\0';
      if (*num != '\0')
	{
	  /* rounding     */
	  if (*num - '0' > 4)
	    {
	      if (roundoff (lang, *result_str, 0, (int *) NULL, format_str)
		  != NO_ERROR)
		{
		  return ER_FAILED;
		}
	    }
	}
    }
  else if (*format == fraction_symbol && *num == '\0')
    {
      res++;
      format++;

      while (*format != '\0')
	{
	  if (*format == '9' || *format == '0')
	    {
	      *res = '0';
	    }
	  else
	    {
	      return ER_FAILED;
	    }

	  format++;
	  res++;
	}

      *res = '\0';
    }
  else if (*format == '\0' && *num == fraction_symbol)
    {
      if (*(num + 1) - '0' > 4)
	{
	  if (roundoff (lang, *result_str, 0, (int *) NULL, format_str) !=
	      NO_ERROR)
	    {
	      return ER_FAILED;
	    }
	}
      /*      rounding        */
    }
  else if (*format == '\0' && *num == '\0')
    {
      /* Nothing      */
    }
  else
    {
      return ER_FAILED;
    }

  init_format[*length] = format_end_char;
  *length = strlen (*result_str);

  return NO_ERROR;
}

/*
 * make_scientific_notation () -
 */
static int
make_scientific_notation (char *src_string, int cipher)
{
  int leng = strlen (src_string);

  src_string[leng] = 'E';
  leng++;

  if (cipher >= 0)
    {
      src_string[leng] = '+';
    }
  else
    {
      src_string[leng] = '-';
      cipher *= (-1);
    }

  leng++;

  if (cipher > 99)
    {
      sprintf (&src_string[leng], "%d", cipher);
    }
  else
    {
      sprintf (&src_string[leng], "%02d", cipher);
    }

  return NO_ERROR;
}

/*
 * roundoff () -
 *
 *  Note :  This function is localized in relation to fractional and digit
 *	    grouping symbols.
 */
static int
roundoff (const INTL_LANG lang, char *src_string, int flag, int *cipher,
	  char *format)
{
  int loop_state = true;
  int is_overflow = false;
  char *res = &src_string[strlen (src_string)];
  char *for_ptr = NULL;
  int i;
  const char fraction_symbol = lang_digit_fractional_symbol (lang);
  const char digit_grouping_symbol = lang_digit_grouping_symbol (lang);

  if (flag == 0)
    {
      for_ptr = &format[strlen (format)];
    }

  if (*src_string == '\0')
    {
      return ER_FAILED;
    }
  if (flag == 0 && *format == '\0')
    {
      return ER_FAILED;
    }

  res--;

  if (flag == 0)
    {
      for_ptr--;
    }

  while (loop_state)
    {
      if ('0' <= *res && *res <= '9')
	{
	  switch (*res - '0' + 1)
	    {
	    case 1:
	    case 2:
	    case 3:
	    case 4:
	    case 5:
	    case 6:
	    case 7:
	    case 8:
	    case 9:
	      *res = *res + 1;
	      loop_state = false;
	      break;

	    case 10:
	      *res = '0';
	      if (res == src_string)
		{
		  loop_state = false;
		  is_overflow = true;
		}
	      else
		{
		  res--;
		  if (flag == 0)
		    {
		      for_ptr--;
		    }
		}
	      break;
	    }
	}
      else if (*res == fraction_symbol || *res == digit_grouping_symbol)
	{
	  if (res == src_string)
	    {
	      loop_state = false;
	      is_overflow = true;
	    }
	  else
	    {
	      res--;
	      if (flag == 0)
		{
		  for_ptr--;
		}
	    }
	}
      else if (*res == ' ')
	{
	  if (flag == 0 && *for_ptr == digit_grouping_symbol)
	    {
	      *res = digit_grouping_symbol;
	      res--;
	      for_ptr--;
	    }

	  *res = '1';
	  loop_state = false;
	}
      else
	{			/* in case of sign, currency     */
	  loop_state = false;
	  is_overflow = true;
	}
    }

  if (is_overflow)
    {
      if (flag == 0)
	{			/* if decimal format    */
	  i = 0;

	  while (i < strlen (src_string))
	    {
	      src_string[i] = '#';
	      i++;
	    }

	  src_string[i] = '\0';
	}
      else
	{			/*      if scientific format    */
	  i = 0;

	  res = src_string;
	  while (!('0' <= *res && *res <= '9'))
	    {
	      res++;
	    }

	  while (i < strlen (res))
	    {
	      if (i == 0)
		{
		  res[i] = '1';
		}
	      else if (i == 1)
		{
		  res[i] = fraction_symbol;
		}
	      else
		{
		  res[i] = '0';
		}
	      i++;
	    }

	  (*cipher)++;
	  res[i] = '\0';
	}
    }

  return NO_ERROR;
}

/*
 * scientific_to_decimal_string () -
 *
 *  Note :  This function is localized in relation to fractional and digit
 *	    grouping symbols.
 */
static int
scientific_to_decimal_string (const INTL_LANG lang, char *src_string,
			      char **scientific_str)
{
#define PLUS 1
#define MINUS 0
  int src_len = strlen (src_string);
  int sign = PLUS, exponent_sign = PLUS, cipher = 0;
  char *ptr = src_string;
  char *result_str;
  int i;
  int tmp_digit;
  const char fraction_symbol = lang_digit_fractional_symbol (lang);

  while (char_isspace (*ptr))
    {
      ptr++;
    }

  if (*ptr == '+')
    {
      sign = PLUS;
      ptr++;
    }
  else if (*ptr == '-')
    {
      sign = MINUS;
      ptr++;
    }

  tmp_digit = 0;
  while (char_isdigit (*ptr))
    {
      tmp_digit = tmp_digit * 10 + (*ptr - '0');
      ptr++;
    }
  if (tmp_digit >= 10)
    {
      return ER_FAILED;
    }
  if (*ptr != fraction_symbol)
    {
      return ER_FAILED;
    }
  ptr++;
  while (char_isdigit (*ptr))
    {
      ptr++;
    }
  if (*ptr == 'e' || *ptr == 'E')
    {
      ptr++;
    }
  else
    {
      return ER_FAILED;
    }

  if (*ptr == '+')
    {
      exponent_sign = PLUS;
    }
  else if (*ptr == '-')
    {
      exponent_sign = MINUS;
    }
  else
    {
      return ER_FAILED;
    }

  ptr++;
  for (; char_isdigit (*ptr); ptr++)
    {
      cipher = cipher * 10 + (*ptr - '0');
    }
  /* So far, one pass     */
  /* Fron now, two pass   */
  while (char_isspace (*ptr))
    {
      ptr++;
    }
  if (*ptr != '\0')
    {
      return ER_FAILED;
    }
  ptr = src_string;
  while (char_isspace (*ptr))
    {
      ptr++;
    }
  *scientific_str = (char *) malloc (src_len + cipher);
  if (*scientific_str == NULL)
    {
      return ER_FAILED;
    }
  /* patch for MemoryTrash   */
  for (i = 0; i < src_len + cipher; i++)
    {
      (*scientific_str)[i] = '\0';
    }

  result_str = *scientific_str;
  if (sign == MINUS)
    {
      *result_str = '-';
      result_str++;
      ptr++;
    }
  if (exponent_sign == PLUS)
    {
      i = 0;
      while (char_isdigit (*ptr))
	{
	  *result_str = *ptr;
	  (result_str)++;
	  ptr++;
	}
      *(result_str + cipher) = fraction_symbol;
      ptr++;
      while (i < cipher || char_isdigit (*ptr))
	{
	  if (*result_str == fraction_symbol)
	    {
	      (result_str)++;
	      continue;
	    }
	  else if (char_isdigit (*ptr))
	    {
	      *result_str = *ptr;
	      ptr++;
	    }
	  else
	    {
	      *result_str = '0';
	    }
	  (result_str)++;
	  i++;
	}
    }
  else
    {
      *result_str = '0';
      result_str++;
      *result_str = fraction_symbol;
      result_str++;
      i = 0;
      while (i < cipher - 1)
	{
	  *result_str = '0';
	  result_str++;
	  i++;
	}
      while (char_isdigit (*ptr) || *ptr == fraction_symbol)
	{
	  if (*ptr == fraction_symbol)
	    {
	      ptr++;
	    }
	  *result_str = *ptr;
	  (result_str)++;
	  ptr++;
	}
    }
  *result_str = '\0';
  return NO_ERROR;
}

/*
 * to_number_next_state () -
 *
 *  Note :  This function is localized in relation to fractional and digit
 *	    grouping symbols.
 */
static int
to_number_next_state (const int previous_state, const int input_char,
		      const INTL_LANG number_lang_id)
{
  int state_table[7][7] = { {4, 5, 2, 3, -1, 6, -1},
  {4, 5, -1, 3, -1, 6, -1},
  {4, 5, -1, -1, -1, 6, -1},
  {4, 4, -1, -1, 4, 6, 7},
  {5, 5, -1, -1, 5, 6, 7},
  {6, 6, -1, -1, 6, -1, 7},
  {0, 0, 0, 0, 0, 0, 0}
  };
  int state;
  const char fraction_symbol = lang_digit_fractional_symbol (number_lang_id);
  const char digit_grouping_symbol =
    lang_digit_grouping_symbol (number_lang_id);

  if (previous_state == -1)
    {
      return -1;
    }

  switch (char_tolower (input_char))
    {
    case '0':
      state = state_table[previous_state - 1][0];
      break;
    case '9':
      state = state_table[previous_state - 1][1];
      break;
    case 's':
      state = state_table[previous_state - 1][2];
      break;
#if 1				/* TODO - */
    case 'c':
      state = state_table[previous_state - 1][3];
      break;
#endif
    default:
      if (input_char == digit_grouping_symbol)
	{
	  state = state_table[previous_state - 1][4];
	  break;
	}
      else if (input_char == fraction_symbol)
	{
	  state = state_table[previous_state - 1][5];
	  break;
	}
      state = state_table[previous_state - 1][6];
      break;
    }

  return state;
}

/*
 * to_number_next_state () -
 * Note: assume precision and scale are correct
 *	 This function is localized in relation to fractional and digit
 *	 grouping symbols.
 */
static int
make_number (char *src, char *last_src, char *token,
	     int *token_length, DB_VALUE * r, const int precision,
	     const int scale, const INTL_LANG number_lang_id)
{
  int error_status = NO_ERROR;
  int state = 1;
  int i, j, k;
  char result_str[DB_MAX_NUMERIC_PRECISION + 2];
  char *res_ptr;
  const char fraction_symbol = lang_digit_fractional_symbol (number_lang_id);
  const char digit_grouping_symbol =
    lang_digit_grouping_symbol (number_lang_id);

  result_str[0] = '\0';
  result_str[DB_MAX_NUMERIC_PRECISION] = '\0';
  result_str[DB_MAX_NUMERIC_PRECISION + 1] = '\0';
  *token_length = 0;

  while (state != 7 && src < last_src)
    {
      switch (to_number_next_state (state, *token, number_lang_id))
	{
	case 1:		/* Not reachable state  */
	  break;
	case 2:
	  if (*src == '-')
	    {
	      if (strlen (result_str) > DB_MAX_NUMERIC_PRECISION)
		{
		  assert (false);
		  return ER_QSTR_MISMATCHING_ARGUMENTS;
		}
	      strncat (result_str, src, 1);
	      src++;
	      (*token_length)++;
	      token++;
	      state = 2;
	    }
	  else if (*src == '+')
	    {
	      src++;
	      (*token_length)++;
	      token++;
	      state = 2;
	    }
	  else
	    {
	      return ER_QSTR_MISMATCHING_ARGUMENTS;
	    }
	  break;
	case 3:
#if 0				/* TODO - */
	  assert (false);
#endif
	  state = 3;
	  break;
	case 4:
	case 5:
	  if (*src == '-')
	    {
	      if (strlen (result_str) > DB_MAX_NUMERIC_PRECISION)
		{
		  assert (false);
		  return ER_QSTR_MISMATCHING_ARGUMENTS;
		}
	      strncat (result_str, src, 1);
	      src++;
	      (*token_length)++;
	    }
	  j = 0;
	  k = 0;
	  while (token[j] == '0' || token[j] == '9' ||
		 token[j] == digit_grouping_symbol)
	    {
	      j++;
	    }
	  while ((&src[k] < last_src) &&
		 (char_isdigit (src[k]) || src[k] == digit_grouping_symbol))
	    {
	      k++;
	    }
	  i = j;

	  if (k > DB_MAX_NUMERIC_PRECISION)
	    {
	      return ER_NUM_OVERFLOW;
	    }
	  if (k > 0)
	    {
	      k--;
	    }
	  j--;
	  while (k > 0 && j > 0)
	    {
	      if (token[j] == digit_grouping_symbol &&
		  src[k] != digit_grouping_symbol)
		{
		  return ER_QSTR_MISMATCHING_ARGUMENTS;
		}
	      k--;
	      j--;
	    }

	  if (k != 0)
	    {			/* format = '99' && src = '4444' */
	      return ER_QSTR_MISMATCHING_ARGUMENTS;
	    }
	  /* patch select to_number('30','9,9') from dual;                */
	  if ((src[k] == digit_grouping_symbol &&
	       token[j] != digit_grouping_symbol) ||
	      (token[j] == digit_grouping_symbol &&
	       src[k] != digit_grouping_symbol))
	    {
	      return ER_QSTR_MISMATCHING_ARGUMENTS;
	    }
	  if (j > 0)
	    {
	      j = 0;
	    }
	  while (src < last_src &&
		 (char_isdigit (*src) || *src == digit_grouping_symbol))
	    {
	      if (*src != digit_grouping_symbol)
		{
		  if (strlen (result_str) > DB_MAX_NUMERIC_PRECISION)
		    {
		      assert (false);
		      return ER_QSTR_MISMATCHING_ARGUMENTS;
		    }
		  strncat (result_str, src, 1);
		}
	      (*token_length)++;
	      src++;
	    }
	  token = token + i;
	  state = 4;
	  break;
	case 6:
	  token++;
	  if (*src == fraction_symbol)
	    {
	      if (strlen (result_str) > DB_MAX_NUMERIC_PRECISION)
		{
		  assert (false);
		  return ER_QSTR_MISMATCHING_ARGUMENTS;
		}
	      strncat (result_str, src, 1);
	      src++;
	      (*token_length)++;
	      while (src < last_src && char_isdigit (*src))
		{
		  if (*token == '0' || *token == '9')
		    {
		      if (strlen (result_str) > DB_MAX_NUMERIC_PRECISION)
			{
			  assert (false);
			  return ER_QSTR_MISMATCHING_ARGUMENTS;
			}
		      strncat (result_str, src, 1);
		      token++;
		      src++;
		      (*token_length)++;
		    }
		  else
		    {
		      return ER_QSTR_MISMATCHING_ARGUMENTS;
		    }
		}
	    }
	  while (*token == '0' || *token == '9')
	    {
	      token++;
	    }
	  state = 6;
	  break;
	case 7:
	  state = 7;
	  break;
	case -1:
	  return ER_QSTR_MISMATCHING_ARGUMENTS;
	}			/* switch       */
    }				/* while        */

  /* For Scientific notation      */
  if (strlen (token) >= 4 && strncasecmp (token, "eeee", 4) == 0 &&
      char_tolower (*src) == 'e' && (*(src + 1) == '+' || *(src + 1) == '-'))
    {
      if (strlen (result_str) - 1 > DB_MAX_NUMERIC_PRECISION)
	{
	  assert (false);
	  return ER_QSTR_MISMATCHING_ARGUMENTS;
	}
      strncat (result_str, src, 2);
      src += 2;
      (*token_length) += 2;

      while (src < last_src && char_isdigit (*src))
	{
	  if (strlen (result_str) > DB_MAX_NUMERIC_PRECISION)
	    {
	      assert (false);
	      return ER_QSTR_MISMATCHING_ARGUMENTS;
	    }
	  strncat (result_str, src, 1);
	  src += 1;
	  (*token_length) += 1;
	}

      if (scientific_to_decimal_string (number_lang_id, result_str, &res_ptr)
	  != NO_ERROR)
	{
	  return ER_QSTR_MISMATCHING_ARGUMENTS;
	  /* This line needs to be modified to reflect appropriate error */
	}

      /*
       * modify result_str to contain correct string value with respect to
       * the given precision and scale.
       */
      strncpy (result_str, res_ptr, sizeof (result_str) - 1);
      free_and_init (res_ptr);

      assert (number_lang_id == INTL_LANG_ENGLISH);

      error_status = adjust_precision (result_str, precision, scale);
      if (error_status == DOMAIN_OVERFLOW)
	{
	  return ER_NUM_OVERFLOW;
	}

      if (error_status != NO_ERROR ||
	  numeric_coerce_string_to_num (result_str, strlen (result_str),
					r) != NO_ERROR)
	{
	  /*       patch for to_number('-1.23e+03','9.99eeee')    */
	  return ER_QSTR_MISMATCHING_ARGUMENTS;
	}
      /* old comment
         error_status = DB_MAKE_NUMERIC (r,num,precision,scale);
       */
    }
  else
    {
      assert (number_lang_id == INTL_LANG_ENGLISH);
      /*
       * modify result_str to contain correct string value with respect to
       * the given precision and scale.
       */
      error_status = adjust_precision (result_str, precision, scale);
      if (error_status == DOMAIN_OVERFLOW)
	{
	  return ER_NUM_OVERFLOW;
	}

      if (error_status != NO_ERROR ||
	  numeric_coerce_string_to_num (result_str, strlen (result_str),
					r) != NO_ERROR)
	{
	  return ER_QSTR_MISMATCHING_ARGUMENTS;
	}
    }

  return error_status;
}

/*
 * get_number_token () -
 *
 *  Note :  This function is localized in relation to fractional and digit
 *	    grouping symbols.
 */
static int
get_number_token (const INTL_LANG lang, char *fsp, int *length,
		  char *last_position, char **next_fsp)
{
  const char fraction_symbol = lang_digit_fractional_symbol (lang);
  const char digit_grouping_symbol = lang_digit_grouping_symbol (lang);
  char c;

  *length = 0;

  if (fsp == last_position)
    {
      return N_END;
    }

  c = char_tolower (fsp[*length]);
  switch (c)
    {
    case 'c':
    case 's':
      if (fsp[*length + 1] == digit_grouping_symbol)
	{
	  return N_INVALID;
	}

      if ((char_tolower (fsp[*length + 1]) == 'c' ||
	   char_tolower (fsp[*length + 1]) == 's') &&
	  fsp[*length + 2] == digit_grouping_symbol)
	{
	  return N_INVALID;
	}

    case '9':
    case '0':
      while (fsp[*length] == '9' || fsp[*length] == '0' ||
	     char_tolower (fsp[*length]) == 's' ||
	     char_tolower (fsp[*length]) == 'c' ||
	     fsp[*length] == fraction_symbol ||
	     fsp[*length] == digit_grouping_symbol)
	{
	  *length += 1;
	}

      *next_fsp = &fsp[*length];
      if (strlen (*next_fsp) >= 4 && !strncasecmp (*next_fsp, "eeee", 4))
	{
	  *length += 4;
	  *next_fsp = &fsp[*length];
	}
      return N_FORMAT;

    case ' ':
    case '\t':
    case '\n':
      while (last_position != &fsp[*length]
	     && (fsp[*length] == ' ' || fsp[*length] == '\t'
		 || fsp[*length] == '\n'))
	{
	  *length += 1;
	}
      *next_fsp = &fsp[*length];
      return N_SPACE;

    case '"':
      *length += 1;
      while (fsp[*length] != '"')
	{
	  if (&fsp[*length] == last_position)
	    {
	      return N_INVALID;
	    }
	  *length += 1;
	}
      *length += 1;
      *next_fsp = &fsp[*length];
      return N_TEXT;

    default:
      if (c == fraction_symbol)
	{
	  while (fsp[*length] == '9' || fsp[*length] == '0' ||
		 char_tolower (fsp[*length]) == 's' ||
		 char_tolower (fsp[*length]) == 'c' ||
		 fsp[*length] == fraction_symbol
		 || fsp[*length] == digit_grouping_symbol)
	    {
	      *length += 1;
	    }

	  *next_fsp = &fsp[*length];
	  if (strlen (*next_fsp) >= 4 && !strncasecmp (*next_fsp, "eeee", 4))
	    {
	      *length += 4;
	      *next_fsp = &fsp[*length];
	    }
	  return N_FORMAT;
	}
      return N_INVALID;
    }
}

/*
 * get_number_format () -
 */
static int
get_next_format (char *sp, DB_TYPE str_type,
		 int *format_length, char **next_pos)
{
  /* sp : start position          */
  *format_length = 0;

  switch (char_tolower (*sp))
    {
    case 'y':
      if (str_type == DB_TYPE_TIME)
	{
	  return DT_INVALID;
	}

      if (strncasecmp (sp, "yyyy", 4) == 0)
	{
	  *format_length += 4;
	  *next_pos = sp + *format_length;
	  return DT_YYYY;
	}
      else if (strncasecmp (sp, "yy", 2) == 0)
	{
	  *format_length += 2;
	  *next_pos = sp + *format_length;
	  return DT_YY;
	}
      else
	{
	  return DT_INVALID;
	}

    case 'd':
      if (str_type == DB_TYPE_TIME)
	{
	  return DT_INVALID;
	}

      if (strncasecmp (sp, "dd", 2) == 0)
	{
	  *format_length += 2;
	  *next_pos = sp + *format_length;
	  return DT_DD;
	}
      else if (strncasecmp (sp, "dy", 2) == 0)
	{
	  *format_length += 2;
	  *next_pos = sp + *format_length;
	  return DT_DY;
	}
      else if (strncasecmp (sp, "day", 3) == 0)
	{
	  *format_length += 3;
	  *next_pos = sp + *format_length;
	  return DT_DAY;
	}
      else
	{
	  *format_length += 1;
	  *next_pos = sp + *format_length;
	  return DT_D;
	}

    case 'c':
      if (str_type == DB_TYPE_TIME)
	{
	  return DT_INVALID;
	}

      if (strncasecmp (sp, "cc", 2) == 0)
	{
	  *format_length += 2;
	  *next_pos = sp + *format_length;
	  return DT_CC;
	}
      else
	{
	  return DT_INVALID;
	}

    case 'q':
      if (str_type == DB_TYPE_TIME)
	{
	  return DT_INVALID;
	}

      *format_length += 1;
      *next_pos = sp + *format_length;
      return DT_Q;

    case 'm':
      if (str_type != DB_TYPE_TIME && strncasecmp (sp, "mm", 2) == 0)
	{
	  *format_length += 2;
	  *next_pos = sp + *format_length;
	  return DT_MM;
	}
      else if (str_type != DB_TYPE_TIME && strncasecmp (sp, "month", 5) == 0)
	{
	  *format_length += 5;
	  *next_pos = sp + *format_length;
	  return DT_MONTH;
	}
      else if (str_type != DB_TYPE_TIME && strncasecmp (sp, "mon", 3) == 0)
	{
	  *format_length += 3;
	  *next_pos = sp + *format_length;
	  return DT_MON;
	}
      else if (str_type != DB_TYPE_DATE && strncasecmp (sp, "mi", 2) == 0)
	{
	  *format_length += 2;
	  *next_pos = sp + *format_length;
	  return DT_MI;
	}
      else
	{
	  return DT_INVALID;
	}

    case 'a':
      if (str_type == DB_TYPE_DATE)
	{
	  return DT_INVALID;
	}

      if (strncasecmp (sp, "am", 2) == 0)
	{
	  *format_length += 2;
	  *next_pos = sp + *format_length;
	  return DT_AM;
	}
      else if (strncasecmp (sp, "a.m.", 4) == 0)
	{
	  *format_length += 4;
	  *next_pos = sp + *format_length;
	  return DT_A_M;
	}
      else
	{
	  return DT_INVALID;
	}

    case 'p':
      if (str_type == DB_TYPE_DATE)
	{
	  return DT_INVALID;
	}

      if (strncasecmp (sp, "pm", 2) == 0)
	{
	  *format_length += 2;
	  *next_pos = sp + *format_length;
	  return DT_PM;
	}
      else if (strncasecmp (sp, "p.m.", 4) == 0)
	{
	  *format_length += 4;
	  *next_pos = sp + *format_length;
	  return DT_P_M;
	}
      else
	{
	  return DT_INVALID;
	}

    case 'h':
      if (str_type == DB_TYPE_DATE)
	{
	  return DT_INVALID;
	}

      if (strncasecmp (sp, "hh24", 4) == 0)
	{
	  *format_length += 4;
	  *next_pos = sp + *format_length;
	  return DT_HH24;
	}
      else if (strncasecmp (sp, "hh12", 4) == 0)
	{
	  *format_length += 4;
	  *next_pos = sp + *format_length;
	  return DT_HH12;
	}
      else if (strncasecmp (sp, "hh", 2) == 0)
	{
	  *format_length += 2;
	  *next_pos = sp + *format_length;
	  return DT_HH;
	}
      else if (strncasecmp (sp, "h", 1) == 0)
	{
	  *format_length += 1;
	  *next_pos = sp + *format_length;
	  return DT_H;
	}
      else
	{
	  return DT_INVALID;
	}

    case 's':
      if (str_type == DB_TYPE_DATE)
	{
	  return DT_INVALID;
	}

      if (strncasecmp (sp, "ss", 2) == 0)
	{
	  *format_length += 2;
	  *next_pos = sp + *format_length;
	  return DT_SS;
	}
      else
	{
	  return DT_INVALID;
	}

    case 'f':
      if (str_type == DB_TYPE_DATETIME && strncasecmp (sp, "ff", 2) == 0)
	{
	  *format_length += 2;
	  *next_pos = sp + *format_length;
	  return DT_MS;
	}
      else
	{
	  return DT_INVALID;
	}

    case '"':
      *format_length += 1;
      while (sp[*format_length] != '"')
	{
	  int char_size;
	  unsigned char *ptr = (unsigned char *) sp + (*format_length);
	  if (sp[*format_length] == '\0')
	    {
	      return DT_INVALID;
	    }
	  INTL_NEXT_CHAR (ptr, ptr, &char_size);
	  *format_length += char_size;
	}
      *format_length += 1;
      *next_pos = &sp[*format_length];
      return DT_TEXT;

    case '-':
    case '/':
      /* this is not a numeric format: it is not necessary to localize point
       * and comma symbols here */
    case ',':
    case '.':
    case ';':
    case ':':
    case ' ':
    case '\t':
    case '\n':
      *format_length += 1;
      *next_pos = sp + *format_length;
      return DT_PUNCTUATION;

    default:
      return DT_INVALID;
    }
}

/*
 * get_cur_year () -
 */
static int
get_cur_year (void)
{
  time_t tloc;
  struct tm *tm, tm_val;

  if (time (&tloc) == -1)
    {
      return -1;
    }

  tm = localtime_r (&tloc, &tm_val);
  if (tm == NULL)
    {
      return -1;
    }

  return tm->tm_year + 1900;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * get_cur_month () -
 */
static int
get_cur_month (void)
{
  time_t tloc;
  struct tm *tm, tm_val;

  if (time (&tloc) == -1)
    {
      return -1;
    }

  tm = localtime_r (&tloc, &tm_val);
  if (tm == NULL)
    {
      return -1;
    }

  return tm->tm_mon + 1;
}
#endif

/*
 * get_day () -
 */
int
get_day (int month, int day, int year)
{
  return day_of_week (julian_encode (month, day, year));
}

/*
 * db_format () -
 *
 *  Note :  This function is localized in relation to fractional and digit
 *	    grouping symbols.
 */
int
db_format (const DB_VALUE * value, const DB_VALUE * decimals,
	   const DB_VALUE * number_lang, DB_VALUE * result)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_decimals, tmp_number_lang;
  DB_TYPE arg1_type, arg2_type;
  int ndec = 0, i, j;
  const char *integer_format_max =
    "99,999,999,999,999,999,999,999,999,999,999,999,999";
  char format[128];
  DB_VALUE format_val, trim_charset, formatted_val, numeric_val, trimmed_val;
  const DB_VALUE *num_dbval_p = NULL;
  char fraction_symbol;
  char digit_grouping_symbol;
  bool dummy;
  INTL_LANG number_lang_id;

  assert (value != NULL);
  assert (decimals != NULL);
  assert (number_lang != NULL);
  assert (value != result);
  assert (decimals != result);
  assert (number_lang != result);

  DB_MAKE_NULL (&formatted_val);
  DB_MAKE_NULL (&trimmed_val);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (2, &tmp_decimals, &tmp_number_lang);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (2, value, decimals))
    {
      DB_MAKE_NULL (result);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  arg2_type = DB_VALUE_DOMAIN_TYPE (decimals);

  if (!(arg2_type == DB_TYPE_INTEGER || arg2_type == DB_TYPE_BIGINT))
    {
      decimals =
	db_value_cast_arg (decimals, &tmp_decimals, DB_TYPE_BIGINT,
			   &error_status);
    }
  number_lang =
    db_value_cast_arg (number_lang, &tmp_number_lang, DB_TYPE_INTEGER,
		       &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  arg2_type = DB_VALUE_DOMAIN_TYPE (decimals);

  assert (DB_VALUE_TYPE (number_lang) == DB_TYPE_INTEGER);

  number_lang_id = lang_get_lang_id_from_flag (DB_GET_INT (number_lang),
					       &dummy, &dummy);
  fraction_symbol = lang_digit_fractional_symbol (number_lang_id);
  digit_grouping_symbol = lang_digit_grouping_symbol (number_lang_id);

  if (arg2_type == DB_TYPE_INTEGER)
    {
      ndec = DB_GET_INT (decimals);
    }
  else
    {
      DB_BIGINT bi = DB_GET_BIGINT (decimals);

      assert (arg2_type == DB_TYPE_BIGINT);

      if (bi > INT_MAX || bi < 0)
	{
	  error_status = ER_OBJ_INVALID_ARGUMENTS;
	  goto exit_on_error;
	}
      ndec = (int) bi;
    }

  if (ndec < 0)
    {
      error_status = ER_OBJ_INVALID_ARGUMENTS;
      goto exit_on_error;
    }

  /* 30 is the decimal limit for formating floating points with this function,
     in mysql */
  if (ndec > 30)
    {
      ndec = 30;
    }

  arg1_type = DB_VALUE_DOMAIN_TYPE (value);

  switch (arg1_type)
    {
    case DB_TYPE_VARCHAR:
      {
	char *c;
	int len, dot = 0;
	/* Trim first because the input string can be given like below:
	 *  - ' 1.1 ', '1.1 ', ' 1.1'
	 */
	db_make_null (&trim_charset);
	error_status =
	  db_string_trim (BOTH, &trim_charset, value, &trimmed_val);
	if (error_status != NO_ERROR)
	  {
	    goto exit_on_error;
	  }

	c = DB_GET_STRING (&trimmed_val);
	if (c == NULL)
	  {
	    error_status = ER_OBJ_INVALID_ARGUMENTS;
	    goto exit_on_error;
	  }

	len = strlen (c);

	for (i = 0; i < len; i++)
	  {
	    if (c[i] == fraction_symbol)
	      {
		dot++;
		continue;
	      }
#if 0				/* TODO - skip plus/minus sign */
	    if (!char_isdigit (c[i]))
	      {
		error_status = ER_OBJ_INVALID_ARGUMENTS;
		goto exit_on_error;
	      }
#endif
	  }

	if (dot > 1)
	  {
	    error_status = ER_OBJ_INVALID_ARGUMENTS;
	    goto exit_on_error;
	  }

	assert (number_lang_id == INTL_LANG_ENGLISH);

	error_status = numeric_coerce_string_to_num (c, len, &numeric_val);
	if (error_status != NO_ERROR)
	  {
	    pr_clear_value (&trimmed_val);
	    goto exit_on_error;
	  }

	num_dbval_p = &numeric_val;
	pr_clear_value (&trimmed_val);
      }
      break;

    case DB_TYPE_INTEGER:
    case DB_TYPE_BIGINT:
    case DB_TYPE_DOUBLE:
    case DB_TYPE_NUMERIC:
      num_dbval_p = value;
      break;

    default:
      error_status = ER_OBJ_INVALID_ARGUMENTS;
      goto exit_on_error;
      break;
    }

  /* Make format string. */
  i = snprintf (format, sizeof (format) - 1, "%s", integer_format_max);
  assert (number_lang_id == INTL_LANG_ENGLISH);
  if (ndec > 0)
    {
      format[i++] = fraction_symbol;
      for (j = 0; j < ndec; j++)
	{
	  format[i++] = '9';
	}
      format[i] = '\0';
    }

  db_make_string (&format_val, format);

  error_status = number_to_char (num_dbval_p, &format_val, number_lang,
				 &formatted_val);
  if (error_status == NO_ERROR)
    {
      /* number_to_char function returns a string with leading empty characters.
       * So, we need to remove them.
       */
      db_make_null (&trim_charset);
      error_status =
	db_string_trim (LEADING, &trim_charset, &formatted_val, result);

      pr_clear_value (&formatted_val);
    }

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  if (!DB_IS_NULL (&trimmed_val))
    {
      pr_clear_value (&trimmed_val);
    }
  if (!DB_IS_NULL (&formatted_val))
    {
      pr_clear_value (&formatted_val);
    }

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_decimals, &tmp_number_lang);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  if (!DB_IS_NULL (&trimmed_val))
    {
      pr_clear_value (&trimmed_val);
    }
  if (!DB_IS_NULL (&formatted_val))
    {
      pr_clear_value (&formatted_val);
    }

  DB_MAKE_NULL (result);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (2, &tmp_decimals, &tmp_number_lang);

  return error_status;
}

/*
 * add_and_normalize_date_time ()
 *
 * Arguments: date & time values to modify,
 *	      date & time amounts to add
 *
 * Returns: NO_ERROR/ER_FAILED
 *
 * Errors:
 *
 * Note:
 *    transforms all values in a correct interval (h: 0..23, m: 0..59, etc)
 */
static int
add_and_normalize_date_time (int *year, int *month,
			     int *day, int *hour,
			     int *minute, int *second,
			     int *millisecond, DB_BIGINT y, DB_BIGINT m,
			     DB_BIGINT d, DB_BIGINT h, DB_BIGINT mi,
			     DB_BIGINT s, DB_BIGINT ms)
{
  DB_BIGINT days[13] = { 0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
  DB_BIGINT i;
  DB_BIGINT _y, _m, _d, _h, _mi, _s, _ms;
  DB_BIGINT old_day = *day;

  _y = *year;
  _m = *month;
  _d = *day;
  _h = *hour;
  _mi = *minute;
  _s = *second;
  _ms = *millisecond;

  _y += y;
  _m += m;
  _d += d;
  _h += h;
  _mi += mi;
  _s += s;
  _ms += ms;

  /* just years and/or months case */
  if (d == 0 && h == 0 && mi == 0 && s == 0 && ms == 0 && (m > 0 || y > 0))
    {
      if (_m % 12 == 0)
	{
	  _y += (_m - 12) / 12;
	  _m = 12;
	}
      else
	{
	  _y += _m / 12;
	  _m %= 12;
	}

      days[2] = LEAP (_y) ? 29 : 28;

      if (old_day > days[_m])
	{
	  _d = days[_m];
	}

      goto set_and_return;
    }

  /* time */
  _s += _ms / 1000;
  _ms %= 1000;

  _mi += _s / 60;
  _s %= 60;

  _h += _mi / 60;
  _mi %= 60;

  _d += _h / 24;
  _h %= 24;

  /* date */
  if (_m > 12)
    {
      _y += _m / 12;
      _m %= 12;

      if (_m == 0)
	{
	  _m = 1;
	}
    }

  days[2] = LEAP (_y) ? 29 : 28;

  if (_d > days[_m])
    {
      /* rewind to 1st january */
      for (i = 1; i < _m; i++)
	{
	  _d += days[i];
	}
      _m = 1;

      /* days for years */
      while (_d >= 366)
	{
	  days[2] = LEAP (_y) ? 29 : 28;
	  _d -= (days[2] == 29) ? 366 : 365;
	  _y++;
	  if (_y > 9999)
	    {
	      goto set_and_return;
	    }
	}

      /* days within a year */
      days[2] = LEAP (_y) ? 29 : 28;
      for (_m = 1;; _m++)
	{
	  if (_d <= days[_m])
	    {
	      break;
	    }
	  _d -= days[_m];
	}
    }

  if (_m == 0)
    {
      _m = 1;
    }
  if (_d == 0)
    {
      _d = 1;
    }

set_and_return:

  if (_y >= 10000 || _y < 0)
    {
      return ER_FAILED;
    }

  *year = (int) _y;
  *month = (int) _m;
  *day = (int) _d;
  *hour = (int) _h;
  *minute = (int) _mi;
  *second = (int) _s;
  *millisecond = (int) _ms;

  return NO_ERROR;
}

/*
 * sub_and_normalize_date_time ()
 *
 * Arguments: date & time values to modify,
 *	      date & time amounts to subtract
 *
 * Returns: NO_ERROR/ER_FAILED
 *
 * Errors:
 *
 * Note:
 *    transforms all values in a correct interval (h: 0..23, m: 0..59, etc)
 */
static int
sub_and_normalize_date_time (int *year, int *month,
			     int *day, int *hour,
			     int *minute, int *second,
			     int *millisecond, DB_BIGINT y, DB_BIGINT m,
			     DB_BIGINT d, DB_BIGINT h, DB_BIGINT mi,
			     DB_BIGINT s, DB_BIGINT ms)
{
  DB_BIGINT days[13] = { 0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
  DB_BIGINT i;
  DB_BIGINT old_day = *day;
  DB_BIGINT _y, _m, _d, _h, _mi, _s, _ms;

  _y = *year;
  _m = *month;
  _d = *day;
  _h = *hour;
  _mi = *minute;
  _s = *second;
  _ms = *millisecond;

  _y -= y;
  _m -= m;
  _d -= d;
  _h -= h;
  _mi -= mi;
  _s -= s;
  _ms -= ms;

  days[2] = LEAP (_y) ? 29 : 28;

  /* time */
  _s += _ms / 1000;
  _ms %= 1000;
  if (_ms < 0)
    {
      _ms += 1000;
      _s--;
    }

  _mi += _s / 60;
  _s %= 60;
  if (_s < 0)
    {
      _s += 60;
      _mi--;
    }

  _h += _mi / 60;
  _mi %= 60;
  if (_mi < 0)
    {
      _mi += 60;
      _h--;
    }

  _d += _h / 24;
  _h %= 24;
  if (_h < 0)
    {
      _h += 24;
      _d--;
    }

  if (_d == 0)
    {
      _m--;

      if (_m == 0)
	{
	  _y--;
	  days[2] = LEAP (_y) ? 29 : 28;
	  _m = 12;
	}
      _d = days[_m];
    }

  if (_m == 0)
    {
      _y--;
      days[2] = LEAP (_y) ? 29 : 28;
      _m = 12;
    }

  /* date */
  if (_m < 0)
    {
      _y += (_m / 12);
      if (_m % 12 == 0)
	{
	  _m = 1;
	}
      else
	{
	  _m %= 12;
	  if (_m < 0)
	    {
	      _m += 12;
	      _y--;
	    }
	}
    }

  /* just years and/or months case */
  if (d == 0 && h == 0 && mi == 0 && s == 0 && ms == 0 && (m > 0 || y > 0))
    {
      if (_m <= 0)
	{
	  _y += (_m / 12);
	  if (_m % 12 == 0)
	    {
	      _m = 1;
	    }
	  else
	    {
	      _m %= 12;
	      if (_m <= 0)
		{
		  _m += 12;
		  _y--;
		}
	    }
	}

      days[2] = LEAP (_y) ? 29 : 28;

      if (old_day > days[_m])
	{
	  _d = days[_m];
	}

      goto set_and_return;
    }

  days[2] = LEAP (_y) ? 29 : 28;

  if (_d > days[_m] || _d < 0)
    {
      /* rewind to 1st january */
      for (i = 1; i < _m; i++)
	{
	  _d += days[i];
	}
      _m = 1;

      /* days for years */
      while (_d < 0)
	{
	  _y--;
	  if (_y < 0)
	    {
	      goto set_and_return;
	    }
	  days[2] = LEAP (_y) ? 29 : 28;
	  _d += (days[2] == 29) ? 366 : 365;
	}

      /* days within a year */
      days[2] = LEAP (_y) ? 29 : 28;
      for (_m = 1;; _m++)
	{
	  if (_d <= days[_m])
	    {
	      break;
	    }
	  _d -= days[_m];
	}
    }

  if (_m == 0)
    {
      _m = 1;
    }
  if (_d == 0)
    {
      _d = 1;
    }

set_and_return:

  if (_y >= 10000 || _y < 0)
    {
      return ER_FAILED;
    }

  *year = (int) _y;
  *month = (int) _m;
  *day = (int) _d;
  *hour = (int) _h;
  *minute = (int) _mi;
  *second = (int) _s;
  *millisecond = (int) _ms;

  return NO_ERROR;
}

/*
 * db_date_add_sub_interval_days ()
 *
 * Arguments:
 *         date: starting date
 *         db_days: number of days to add
 *
 * Returns: int
 *
 * Errors:
 *
 * Note:
 *    Returns date + an interval of db_days days.
 */
static int
db_date_add_sub_interval_days (DB_VALUE * result, const DB_VALUE * date,
			       const DB_VALUE * db_days, bool is_add)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_date, tmp_db_days;
  int days;
  DB_DATETIME db_datetime, *dt_p = NULL;
  DB_TIME db_time;
  DB_DATE db_date, *d_p;
  int is_dt = -1, is_d = -1, is_t = -1;
  DB_TYPE res_type;
  char *date_s = NULL, res_s[64];
  int y, m, d, h, mi, s, ms;
  int ret;
  char *res_final;

  assert (result != date);
  assert (result != db_days);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (2, &tmp_date, &tmp_db_days);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (2, date, db_days))
    {
      DB_MAKE_NULL (result);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  /* cast date *//* go ahead */

  db_days =
    db_value_cast_arg (db_days, &tmp_db_days, DB_TYPE_INTEGER, &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  /* simple case, where just a number of days is added to date */

  res_type = DB_VALUE_DOMAIN_TYPE (date);

  days = DB_GET_INT (db_days);

  switch (res_type)
    {
    case DB_TYPE_VARCHAR:
      {
	bool has_explicit_time = false;
	int str_len = DB_GET_STRING_SIZE (date);

	date_s = DB_GET_STRING (date);
	if (date_s == NULL)
	  {
	    assert (false);
	    error_status = ER_OBJ_INVALID_ARGUMENTS;
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	    goto exit_on_error;
	  }

	/* try to figure out the string format */
	if (db_date_parse_datetime_parts (date_s, str_len, &db_datetime,
					  &has_explicit_time, NULL))
	  {
	    is_dt = ER_DATETIME_CONVERSION;
	    is_d = ER_DATE_CONVERSION;
	    is_t = db_string_to_time_ex (date_s, str_len, &db_time);
	  }
	else
	  {
	    if (has_explicit_time)
	      {
		is_dt = NO_ERROR;
		is_d = ER_DATE_CONVERSION;
		is_t = ER_TIME_CONVERSION;
	      }
	    else
	      {
		db_date = db_datetime.date;
		is_dt = ER_DATETIME_CONVERSION;
		is_d = NO_ERROR;
		is_t = ER_TIME_CONVERSION;
	      }
	  }

	if (is_dt && is_d && is_t)
	  {
	    error_status = ER_OBJ_INVALID_ARGUMENTS;
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	    goto exit_on_error;
	  }

	/* add date stuff to a time -> error */
	/* in fact, disable time operations, not available on mysql */
	if (is_t == 0)
	  {
	    error_status = ER_OBJ_INVALID_ARGUMENTS;
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	    goto exit_on_error;
	  }

	dt_p = &db_datetime;
	d_p = &db_date;

	/* except just TIME business, convert all to DATETIME */
      }
      break;

    case DB_TYPE_DATE:
      is_d = 1;
      d_p = DB_GET_DATE (date);
      break;

    case DB_TYPE_DATETIME:
      is_dt = 1;
      dt_p = DB_GET_DATETIME (date);
      break;

    default:
      error_status = ER_OBJ_INVALID_ARGUMENTS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
      break;
    }

  if (is_d >= 0)
    {
      y = m = d = h = mi = s = ms = 0;
      db_date_decode (d_p, &m, &d, &y);

      if (m == 0 && d == 0 && y == 0)
	{
	  error_status = ER_DATE_CONVERSION;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto exit_on_error;
	}

      if (is_add)
	{
	  if (days > 0)
	    {
	      ret = add_and_normalize_date_time (&y, &m, &d, &h, &mi, &s, &ms,
						 0, 0, days, 0, 0, 0, 0);
	    }
	  else
	    {
	      ret = sub_and_normalize_date_time (&y, &m, &d, &h, &mi, &s, &ms,
						 0, 0, -days, 0, 0, 0, 0);
	    }
	}
      else
	{
	  if (days > 0)
	    {
	      ret = sub_and_normalize_date_time (&y, &m, &d, &h, &mi, &s, &ms,
						 0, 0, days, 0, 0, 0, 0);
	    }
	  else
	    {
	      ret = add_and_normalize_date_time (&y, &m, &d, &h, &mi, &s, &ms,
						 0, 0, -days, 0, 0, 0, 0);
	    }
	}

      /* year should always be greater than 1 and less than 9999 */
      if (ret != NO_ERROR)
	{
	  error_status = ER_OBJ_INVALID_ARGUMENTS;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto exit_on_error;
	}

      db_date_encode (&db_date, m, d, y);

      if (res_type == DB_TYPE_VARCHAR)
	{
	  db_date_to_string (res_s, 64, &db_date);

	  res_final = (char *) malloc (strlen (res_s) + 1);
	  if (res_final == NULL)
	    {
	      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	      goto exit_on_error;
	    }

	  strcpy (res_final, res_s);
	  DB_MAKE_STRING (result, res_final);
	  result->need_clear = true;

	  assert (error_status == NO_ERROR);
	}
      else
	{
	  DB_MAKE_DATE (result, m, d, y);
	}
    }
  else if (is_dt >= 0)
    {
      assert (dt_p != NULL);

      y = m = d = h = mi = s = ms = 0;
      db_datetime_decode (dt_p, &m, &d, &y, &h, &mi, &s, &ms);

      if (m == 0 && d == 0 && y == 0 && h == 0 && mi == 0 && s == 0
	  && ms == 0)
	{
	  error_status = ER_DATE_CONVERSION;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto exit_on_error;
	}

      if (is_add)
	{
	  if (days > 0)
	    {
	      ret = add_and_normalize_date_time (&y, &m, &d, &h, &mi, &s, &ms,
						 0, 0, days, 0, 0, 0, 0);
	    }
	  else
	    {
	      ret = sub_and_normalize_date_time (&y, &m, &d, &h, &mi, &s, &ms,
						 0, 0, -days, 0, 0, 0, 0);
	    }
	}
      else
	{
	  if (days > 0)
	    {
	      ret = sub_and_normalize_date_time (&y, &m, &d, &h, &mi, &s, &ms,
						 0, 0, days, 0, 0, 0, 0);
	    }
	  else
	    {
	      ret = add_and_normalize_date_time (&y, &m, &d, &h, &mi, &s, &ms,
						 0, 0, -days, 0, 0, 0, 0);
	    }
	}
      /* year should always be greater than 1 and less than 9999 */
      if (ret != NO_ERROR)
	{
	  error_status = ER_OBJ_INVALID_ARGUMENTS;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto exit_on_error;
	}

      db_datetime.date = db_datetime.time = 0;
      db_datetime_encode (&db_datetime, m, d, y, h, mi, s, ms);

      if (res_type == DB_TYPE_VARCHAR)
	{
	  db_datetime_to_string (res_s, 64, &db_datetime);

	  res_final = (char *) malloc (strlen (res_s) + 1);
	  if (res_final == NULL)
	    {
	      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	      goto exit_on_error;
	    }

	  strcpy (res_final, res_s);
	  DB_MAKE_STRING (result, res_final);
	  result->need_clear = true;

	  assert (error_status == NO_ERROR);
	}
      else
	{
	  /* datetime, date + time units, timestamp => return datetime */
	  DB_MAKE_DATETIME (result, &db_datetime);
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
  db_value_clear_nargs (2, &tmp_date, &tmp_db_days);

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
  db_value_clear_nargs (2, &tmp_date, &tmp_db_days);

  return error_status;
}

int
db_date_add_interval_days (DB_VALUE * result, const DB_VALUE * date,
			   const DB_VALUE * db_days)
{
  return db_date_add_sub_interval_days (result, date, db_days, true);
}

int
db_date_sub_interval_days (DB_VALUE * result, const DB_VALUE * date,
			   const DB_VALUE * db_days)
{
  return db_date_add_sub_interval_days (result, date, db_days, false);
}

/*
 * db_str_to_millisec () -
 *
 * Arguments:
 *         str: millisecond format
 *
 * Returns: int
 *
 * Errors:
 */
static int
db_str_to_millisec (const char *str)
{
  int digit_num, value, ret;

  if (str == NULL || str[0] == '\0')
    {
      return 0;
    }

  digit_num = strlen (str);
  if (digit_num >= 1 && str[0] == '-')
    {
      digit_num--;
      ret = sscanf (str, "%4d", &value);
    }
  else
    {
      ret = sscanf (str, "%3d", &value);
    }

  if (ret != 1)
    {
      return 0;
    }

  switch (digit_num)
    {
    case 1:
      value *= 100;
      break;

    case 2:
      value *= 10;
      break;

    default:
      break;
    }

  return value;
}

/*
 * copy_and_shift_values () -
 *
 * Arguments:
 *         shift: the offset the values are shifted
 *         n: normal number of arguments
 *	   first...: arguments
 *
 * Returns: int
 *
 * Errors:
 *
 * Note:
 *    shifts all arguments by the given value
 */
static void
copy_and_shift_values (int shift, int n, DB_BIGINT * first, ...)
{
  va_list marker;
  DB_BIGINT *curr = first;
  DB_BIGINT *v[16];		/* will contain max 5 elements */
  int i, count = 0, cnt_src = 0;

  /*
   * numeric arguments from interval expression have a delimiter read also
   * as argument so out of N arguments there are actually (N + 1)/2 numeric
   * values (ex: 1:2:3:4 or 1:2 or 1:2:3)
   */
  shift = (shift + 1) / 2;

  if (shift == n)
    {
      return;
    }

  va_start (marker, first);	/* init variable arguments */
  while (cnt_src < n)
    {
      cnt_src++;
      v[count++] = curr;
      curr = va_arg (marker, DB_BIGINT *);
    }
  va_end (marker);

  cnt_src = shift - 1;
  /* move backwards to not overwrite values */
  for (i = count - 1; i >= 0; i--)
    {
      if (cnt_src >= 0)
	{
	  /* replace */
	  *v[i] = *v[cnt_src--];
	}
      else
	{
	  /* reset */
	  *v[i] = 0;
	}
    }
}

/*
 * get_single_unit_value () -
 *   return:
 *   expr (in): input as string
 *   int_val (in) : input as integer
 */
static DB_BIGINT
get_single_unit_value (char *expr, DB_BIGINT int_val)
{
  DB_BIGINT v = 0;

  if (expr == NULL)
    {
      v = int_val;
    }
  else
    {
      sscanf (expr, "%lld", (long long *) &v);
    }

  return v;
}

/*
 * db_date_add_sub_interval_expr () -
 *
 * Arguments:
 *         date: starting date
 *         expr: string with the amounts to add
 *	   unit: unit(s) of the amounts
 *
 * Returns: int
 *
 * Errors:
 *
 * Note:
 *    Returns date + the amounts from expr
 */
static int
db_date_add_sub_interval_expr (DB_VALUE * result, const DB_VALUE * date,
			       const DB_VALUE * expr, const int unit,
			       bool is_add)
{
  int sign = 0;
  int type = 0;			/* 1 -> time, 2 -> date, 3 -> both */
  DB_TYPE res_type, expr_type;
  char *date_s = NULL, *expr_s, res_s[64], millisec_s[64];
  int error_status = NO_ERROR;
  DB_BIGINT millisec, seconds, minutes, hours;
  DB_BIGINT days, weeks, months, quarters, years;
  DB_DATETIME db_datetime, *dt_p = NULL;
  DB_TIME db_time;
  DB_DATE db_date, *d_p;
  int narg, is_dt = -1, is_d = -1, is_t = -1;
  char delim;
  DB_VALUE trimed_expr, charset;
  DB_BIGINT unit_int_val;
  double dbl;
  int y, m, d, h, mi, s, ms;
  int ret;
  char *res_final;

  assert (result != date);
  assert (result != expr);

  res_type = DB_VALUE_DOMAIN_TYPE (date);
  if (res_type == DB_TYPE_NULL || DB_IS_NULL (date))
    {
      DB_MAKE_NULL (result);
      return NO_ERROR;
    }

  expr_type = DB_VALUE_DOMAIN_TYPE (expr);
  if (expr_type == DB_TYPE_NULL || DB_IS_NULL (expr))
    {
      DB_MAKE_NULL (result);
      return NO_ERROR;
    }

  DB_MAKE_NULL (&trimed_expr);
  unit_int_val = 0;
  expr_s = NULL;

  /* 1. Prepare the input: convert expr to char */

  /*
   * expr is converted to char because it may contain a more complicated form
   * for the multiple unit formats, for example:
   * 'DAYS HOURS:MINUTES:SECONDS.MILLISECONDS'
   * For the simple unit tags, expr is integer
   */

  expr_type = DB_VALUE_DOMAIN_TYPE (expr);
  switch (expr_type)
    {
    case DB_TYPE_VARCHAR:
      DB_MAKE_NULL (&charset);
      error_status = db_string_trim (BOTH, &charset, expr, &trimed_expr);
      if (error_status != NO_ERROR)
	{
	  goto error;
	}

      /* db_string_trim builds a NULL terminated string, expr_s is NULL
       * terminated */
      expr_s = DB_GET_STRING (&trimed_expr);
      if (expr_s == NULL)
	{
	  error_status = ER_OBJ_INVALID_ARGUMENTS;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto error;
	}
      break;

    case DB_TYPE_INTEGER:
      unit_int_val = DB_GET_INTEGER (expr);
      break;

    case DB_TYPE_BIGINT:
      unit_int_val = DB_GET_BIGINT (expr);
      break;

    case DB_TYPE_DOUBLE:
      unit_int_val = (DB_BIGINT) round (DB_GET_DOUBLE (expr));
      break;

    case DB_TYPE_NUMERIC:
      numeric_coerce_num_to_double ((DB_C_NUMERIC) db_locate_numeric (expr),
				    DB_VALUE_SCALE (expr), &dbl);
      unit_int_val = (DB_BIGINT) round (dbl);
      break;

    default:
      error_status = ER_OBJ_INVALID_ARGUMENTS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto error;
    }

  /* 2. the big switch: according to unit, we parse expr and get amounts of
     ms/s/m/h/d/m/y/w/q to add or subtract */

  millisec_s[0] = '\0';
  millisec = seconds = minutes = hours = 0;
  days = weeks = months = quarters = years = 0;

  switch (unit)
    {
    case PT_MILLISECOND:
      millisec = get_single_unit_value (expr_s, unit_int_val);
      sign = (millisec >= 0);
      type |= 1;
      break;

    case PT_SECOND:
      seconds = get_single_unit_value (expr_s, unit_int_val);
      sign = (seconds >= 0);
      type |= 1;
      break;

    case PT_MINUTE:
      minutes = get_single_unit_value (expr_s, unit_int_val);
      sign = (minutes >= 0);
      type |= 1;
      break;

    case PT_HOUR:
      hours = get_single_unit_value (expr_s, unit_int_val);
      sign = (hours >= 0);
      type |= 1;
      break;

    case PT_DAY:
      days = get_single_unit_value (expr_s, unit_int_val);
      sign = (days >= 0);
      type |= 2;
      break;

    case PT_WEEK:
      weeks = get_single_unit_value (expr_s, unit_int_val);
      sign = (weeks >= 0);
      type |= 2;
      break;

    case PT_MONTH:
      months = get_single_unit_value (expr_s, unit_int_val);
      sign = (months >= 0);
      type |= 2;
      break;

    case PT_QUARTER:
      quarters = get_single_unit_value (expr_s, unit_int_val);
      sign = (quarters >= 0);
      type |= 2;
      break;

    case PT_YEAR:
      years = get_single_unit_value (expr_s, unit_int_val);
      sign = (years >= 0);
      type |= 2;
      break;

    case PT_SECOND_MILLISECOND:
      if (expr_s)
	{
	  narg = sscanf (expr_s, "%lld%c%63s", (long long *) &seconds, &delim,
			 millisec_s);
	  millisec = db_str_to_millisec (millisec_s);
	  copy_and_shift_values (narg, 2, &seconds, &millisec);
	}
      else
	{
	  millisec = unit_int_val;
	}
      sign = (seconds >= 0);
      type |= 1;
      break;

    case PT_MINUTE_MILLISECOND:
      if (expr_s)
	{
	  narg = sscanf (expr_s, "%lld%c%lld%c%63s", (long long *) &minutes,
			 &delim, (long long *) &seconds, &delim, millisec_s);
	  millisec = db_str_to_millisec (millisec_s);
	  copy_and_shift_values (narg, 3, &minutes, &seconds, &millisec);
	}
      else
	{
	  millisec = unit_int_val;
	}
      sign = (minutes >= 0);
      type |= 1;
      break;

    case PT_MINUTE_SECOND:
      if (expr_s)
	{
	  narg = sscanf (expr_s, "%lld%c%lld", (long long *) &minutes,
			 &delim, (long long *) &seconds);
	  copy_and_shift_values (narg, 2, &minutes, &seconds);
	}
      else
	{
	  seconds = unit_int_val;
	}
      sign = (minutes >= 0);
      type |= 1;
      break;

    case PT_HOUR_MILLISECOND:
      if (expr_s)
	{
	  narg =
	    sscanf (expr_s, "%lld%c%lld%c%lld%c%63s", (long long *) &hours,
		    &delim, (long long *) &minutes, &delim,
		    (long long *) &seconds, &delim, millisec_s);
	  millisec = db_str_to_millisec (millisec_s);
	  copy_and_shift_values (narg, 4, &hours, &minutes, &seconds,
				 &millisec);
	}
      else
	{
	  millisec = unit_int_val;
	}
      sign = (hours >= 0);
      type |= 1;
      break;

    case PT_HOUR_SECOND:
      if (expr_s)
	{
	  narg = sscanf (expr_s, "%lld%c%lld%c%lld", (long long *) &hours,
			 &delim, (long long *) &minutes, &delim,
			 (long long *) &seconds);
	  copy_and_shift_values (narg, 3, &hours, &minutes, &seconds);
	}
      else
	{
	  seconds = unit_int_val;
	}
      sign = (hours >= 0);
      type |= 1;
      break;

    case PT_HOUR_MINUTE:
      if (expr_s)
	{
	  narg = sscanf (expr_s, "%lld%c%lld", (long long *) &hours, &delim,
			 (long long *) &minutes);
	  copy_and_shift_values (narg, 2, &hours, &minutes);
	}
      else
	{
	  minutes = unit_int_val;
	}
      sign = (hours >= 0);
      type |= 1;
      break;

    case PT_DAY_MILLISECOND:
      if (expr_s)
	{
	  narg = sscanf (expr_s, "%lld%c%lld%c%lld%c%lld%c%63s",
			 (long long *) &days, &delim, (long long *) &hours,
			 &delim, (long long *) &minutes, &delim,
			 (long long *) &seconds, &delim, millisec_s);
	  millisec = db_str_to_millisec (millisec_s);
	  copy_and_shift_values (narg, 5, &days, &hours, &minutes, &seconds,
				 &millisec);
	}
      else
	{
	  millisec = unit_int_val;
	}
      sign = (days >= 0);
      type |= 1;
      type |= 2;
      break;

    case PT_DAY_SECOND:
      if (expr_s)
	{
	  narg =
	    sscanf (expr_s, "%lld%c%lld%c%lld%c%lld", (long long *) &days,
		    &delim, (long long *) &hours, &delim,
		    (long long *) &minutes, &delim, (long long *) &seconds);
	  copy_and_shift_values (narg, 4, &days, &hours, &minutes, &seconds);
	}
      else
	{
	  seconds = unit_int_val;
	}
      sign = (days >= 0);
      type |= 1;
      type |= 2;
      break;

    case PT_DAY_MINUTE:
      if (expr_s)
	{
	  narg =
	    sscanf (expr_s, "%lld%c%lld%c%lld", (long long *) &days, &delim,
		    (long long *) &hours, &delim, (long long *) &minutes);
	  copy_and_shift_values (narg, 3, &days, &hours, &minutes);
	}
      else
	{
	  minutes = unit_int_val;
	}
      sign = (days >= 0);
      type |= 1;
      type |= 2;
      break;

    case PT_DAY_HOUR:
      if (expr_s)
	{
	  narg = sscanf (expr_s, "%lld%c%lld", (long long *) &days, &delim,
			 (long long *) &hours);
	  copy_and_shift_values (narg, 2, &days, &hours);
	}
      else
	{
	  hours = unit_int_val;
	}
      sign = (days >= 0);
      type |= 1;
      type |= 2;
      break;

    case PT_YEAR_MONTH:
      if (expr_s)
	{
	  narg = sscanf (expr_s, "%lld%c%lld", (long long *) &years, &delim,
			 (long long *) &months);
	  copy_and_shift_values (narg, 2, &years, &months);
	}
      else
	{
	  months = unit_int_val;
	}
      sign = (years >= 0);
      type |= 2;
      break;

    default:
      error_status = ER_OBJ_INVALID_ARGUMENTS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto error;
    }

  /* we have the sign of the amounts, turn them in absolute value */
  years = ABS (years);
  months = ABS (months);
  days = ABS (days);
  weeks = ABS (weeks);
  quarters = ABS (quarters);
  hours = ABS (hours);
  minutes = ABS (minutes);
  seconds = ABS (seconds);
  millisec = ABS (millisec);

  /* convert weeks and quarters to our units */
  if (weeks != 0)
    {
      days += weeks * 7;
      weeks = 0;
    }

  if (quarters != 0)
    {
      months += 3 * quarters;
      quarters = 0;
    }

  /* 3. Convert string with date to DateTime or Time */

  switch (res_type)
    {
    case DB_TYPE_VARCHAR:
      {
	bool has_explicit_time = false;
	int str_len;

	date_s = DB_GET_STRING (date);
	if (date_s == NULL)
	  {
	    error_status = ER_DATE_CONVERSION;
	    goto error;
	  }

	str_len = DB_GET_STRING_SIZE (date);

	/* try to figure out the string format */
	if (db_date_parse_datetime_parts (date_s, str_len, &db_datetime,
					  &has_explicit_time, NULL))
	  {
	    is_dt = ER_DATETIME_CONVERSION;
	    is_d = ER_DATE_CONVERSION;
	    is_t = db_string_to_time_ex (date_s, str_len, &db_time);
	  }
	else
	  {
	    if (has_explicit_time)
	      {
		is_dt = NO_ERROR;
		is_d = ER_DATE_CONVERSION;
		is_t = ER_TIME_CONVERSION;
	      }
	    else
	      {
		db_date = db_datetime.date;
		is_dt = ER_DATETIME_CONVERSION;
		is_d = NO_ERROR;
		is_t = ER_TIME_CONVERSION;
	      }
	  }

	if (is_dt && is_d && is_t)
	  {
	    error_status = ER_DATE_CONVERSION;
	    goto error;
	  }

	/* add date stuff to a time -> error */
	/* in fact, disable time operations, not available on mysql */
	if (is_t == 0)
	  {
	    error_status = ER_OBJ_INVALID_ARGUMENTS;
	    er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	    goto error;
	  }

	dt_p = &db_datetime;
	d_p = &db_date;

	/* except just TIME business, convert all to DATETIME */
      }
      break;

    case DB_TYPE_DATE:
      is_d = 1;
      d_p = DB_GET_DATE (date);
      break;

    case DB_TYPE_DATETIME:
      is_dt = 1;
      dt_p = DB_GET_DATETIME (date);
      break;

    default:
      error_status = ER_OBJ_INVALID_ARGUMENTS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto error;
    }

  /* treat as date only if adding date units, else treat as datetime */
  if (is_d >= 0)
    {
      y = m = d = h = mi = s = ms = 0;
      db_date_decode (d_p, &m, &d, &y);

      if (m == 0 && d == 0 && y == 0)
	{
	  pr_clear_value (&trimed_expr);
	  DB_MAKE_NULL (result);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_DATE_CONVERSION, 0);

	  return ER_DATE_CONVERSION;
	}

      if (sign ^ is_add)
	{
	  ret = sub_and_normalize_date_time (&y, &m, &d, &h, &mi, &s, &ms,
					     years, months, days, hours,
					     minutes, seconds, millisec);
	}
      else
	{
	  ret = add_and_normalize_date_time (&y, &m, &d, &h, &mi, &s, &ms,
					     years, months, days, hours,
					     minutes, seconds, millisec);
	}

      /* year should always be greater than 1 and less than 9999 */
      if (ret != NO_ERROR)
	{
	  error_status = ER_OBJ_INVALID_ARGUMENTS;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto error;
	}

      if (type == 2)
	{
	  db_date_encode (&db_date, m, d, y);

	  if (m == 0 && d == 0 && y == 0)
	    {
	      pr_clear_value (&trimed_expr);
	      DB_MAKE_NULL (result);
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_DATE_CONVERSION,
		      0);

	      return ER_DATE_CONVERSION;
	    }

	  if (res_type == DB_TYPE_VARCHAR)
	    {
	      db_date_to_string (res_s, 64, &db_date);

	      res_final = (char *) malloc (strlen (res_s) + 1);
	      if (res_final == NULL)
		{
		  error_status = ER_OUT_OF_VIRTUAL_MEMORY;
		  goto error;
		}
	      strcpy (res_final, res_s);
	      DB_MAKE_STRING (result, res_final);
	      result->need_clear = true;
	    }
	  else
	    {
	      DB_MAKE_DATE (result, m, d, y);
	    }
	}
      else if (type & 1)
	{
	  db_datetime.date = db_datetime.time = 0;
	  db_datetime_encode (&db_datetime, m, d, y, h, mi, s, ms);

	  if (m == 0 && d == 0 && y == 0
	      && h == 0 && mi == 0 && s == 0 && ms == 0)
	    {
	      pr_clear_value (&trimed_expr);
	      DB_MAKE_NULL (result);
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_DATE_CONVERSION,
		      0);

	      return ER_DATE_CONVERSION;
	    }

	  if (res_type == DB_TYPE_VARCHAR)
	    {
	      db_datetime_to_string (res_s, 64, &db_datetime);

	      res_final = (char *) malloc (strlen (res_s) + 1);
	      if (res_final == NULL)
		{
		  error_status = ER_OUT_OF_VIRTUAL_MEMORY;
		  goto error;
		}
	      strcpy (res_final, res_s);
	      DB_MAKE_STRING (result, res_final);
	      result->need_clear = true;
	    }
	  else
	    {
	      DB_MAKE_DATETIME (result, &db_datetime);
	    }
	}
    }
  else if (is_dt >= 0)
    {
      assert (dt_p != NULL);

      y = m = d = h = mi = s = ms = 0;
      db_datetime_decode (dt_p, &m, &d, &y, &h, &mi, &s, &ms);

      if (m == 0 && d == 0 && y == 0 && h == 0 && mi == 0 && s == 0
	  && ms == 0)
	{
	  pr_clear_value (&trimed_expr);
	  DB_MAKE_NULL (result);
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_DATE_CONVERSION, 0);

	  return ER_DATE_CONVERSION;
	}

      if (sign ^ is_add)
	{
	  ret = sub_and_normalize_date_time (&y, &m, &d, &h, &mi, &s, &ms,
					     years, months, days, hours,
					     minutes, seconds, millisec);
	}
      else
	{
	  ret = add_and_normalize_date_time (&y, &m, &d, &h, &mi, &s, &ms,
					     years, months, days, hours,
					     minutes, seconds, millisec);
	}

      /* year should always be greater than 1 and less than 9999 */
      if (ret != NO_ERROR)
	{
	  error_status = ER_OBJ_INVALID_ARGUMENTS;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto error;
	}

      db_datetime.date = db_datetime.time = 0;
      db_datetime_encode (&db_datetime, m, d, y, h, mi, s, ms);

      if (res_type == DB_TYPE_VARCHAR)
	{
	  db_datetime_to_string (res_s, 64, &db_datetime);

	  res_final = (char *) malloc (strlen (res_s) + 1);
	  if (res_final == NULL)
	    {
	      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	      goto error;
	    }
	  strcpy (res_final, res_s);
	  DB_MAKE_STRING (result, res_final);
	  result->need_clear = true;
	}
      else
	{
	  /* datetime, date + time units, timestamp => return datetime */
	  DB_MAKE_DATETIME (result, &db_datetime);
	}
    }

error:
  pr_clear_value (&trimed_expr);
  return error_status;
}

/*
 * db_date_add_interval_expr ()
 *
 * Arguments:
 *         result(out):
 *         date(in): source date
 *         expr(in): to be added interval
 *         unit(in): unit of interval expr
 *
 * Returns: int
 *
 * Note:
 */
int
db_date_add_interval_expr (DB_VALUE * result, const DB_VALUE * date,
			   const DB_VALUE * expr, const int unit)
{
  return db_date_add_sub_interval_expr (result, date, expr, unit, true);
}

/*
 * db_date_sub_interval_expr ()
 *
 * Arguments:
 *         result(out):
 *         date(in): source date
 *         expr(in): to be substracted interval
 *         unit(in): unit of interval expr
 *
 * Returns: int
 *
 * Note:
 */
int
db_date_sub_interval_expr (DB_VALUE * result, const DB_VALUE * date,
			   const DB_VALUE * expr, const int unit)
{
  return db_date_add_sub_interval_expr (result, date, expr, unit, false);
}

/*
 * db_date_format ()
 *
 * Arguments:
 *         date_value: source date
 *         format: string with format specifiers
 *	   result: output string
 *
 * Returns: int
 *
 * Errors:
 *
 * Note:
 *    formats the date according to a specified format
 */
int
db_date_format (const DB_VALUE * date_value, const DB_VALUE * format,
		const DB_VALUE * date_lang, DB_VALUE * result)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_date_value, tmp_format, tmp_date_lang;
  DB_DATETIME *dt_p;
  DB_DATE *d_p;
  DB_TYPE res_type;
  char *res = NULL, *res2 = NULL, *format_s = NULL;
  int format_s_len;
  char *strend;
  int len;
  int y, m, d, h, mi, s, ms;
  int days[13] = { 0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
  char format_specifiers[256][64];
  int i, j;
  int dow, dow2;
  INTL_LANG date_lang_id;
  int tu, tv, tx, weeks, ld_fw, days_counter;
  char och = -1, ch;
  const LANG_LOCALE_DATA *lld;
  bool dummy;
  int res_collation;

  assert (date_lang != NULL);
  assert (date_value != result);
  assert (format != result);
  assert (date_lang != result);

  y = m = d = h = mi = s = ms = 0;
  memset (format_specifiers, 0, sizeof (format_specifiers));

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (3, &tmp_date_value, &tmp_format, &tmp_date_lang);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (2, date_value, format))
    {
      DB_MAKE_NULL (result);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  res_type = DB_VALUE_DOMAIN_TYPE (date_value);

  if (!(res_type == DB_TYPE_DATETIME || res_type == DB_TYPE_DATE))
    {
      date_value =
	db_value_cast_arg (date_value, &tmp_date_value, DB_TYPE_DATETIME,
			   &error_status);
    }
  format =
    db_value_cast_arg (format, &tmp_format, DB_TYPE_VARCHAR, &error_status);
  date_lang =
    db_value_cast_arg (date_lang, &tmp_date_lang, DB_TYPE_INTEGER,
		       &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  assert (DB_VALUE_DOMAIN_TYPE (format) == DB_TYPE_VARCHAR);
  assert (DB_VALUE_TYPE (date_lang) == DB_TYPE_INTEGER);

  date_lang_id = lang_get_lang_id_from_flag (DB_GET_INT (date_lang), &dummy,
					     &dummy);

  res_collation = DB_GET_STRING_COLLATION (format);

  lld = lang_get_specific_locale (date_lang_id);
  if (lld == NULL)
    {
      error_status = ER_LANG_CODESET_NOT_AVAILABLE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
	      lang_get_lang_name_from_id (date_lang_id),
	      lang_charset_name (INTL_CODESET_UTF8));
      goto exit_on_error;
    }

  res_type = DB_VALUE_DOMAIN_TYPE (date_value);

  /* 1. Get date values */
  switch (res_type)
    {
    case DB_TYPE_DATETIME:
      dt_p = DB_GET_DATETIME (date_value);
      db_datetime_decode (dt_p, &m, &d, &y, &h, &mi, &s, &ms);
      break;

    case DB_TYPE_DATE:
      d_p = DB_GET_DATE (date_value);
      db_date_decode (d_p, &m, &d, &y);
      break;

    default:
      assert (false);
      error_status = ER_TP_CANT_COERCE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
	      pr_type_name (res_type), pr_type_name (DB_TYPE_DATETIME));
      goto exit_on_error;
    }

  /* 2. Compute the value for each format specifier */
  days[2] += LEAP (y);
  dow = db_get_day_of_week (y, m, d);
  if (dow < 0 || dow > 6)
    {
      assert (false);
      error_status = ER_TP_CANT_COERCE;
      goto exit_on_error;
    }

  /* %a       Abbreviated weekday name (Sun..Sat) */
  strcpy (format_specifiers['a'], lld->day_short_name[dow]);

  /* %b       Abbreviated m name (Jan..Dec) */
  if (m > 0)
    {
      strcpy (format_specifiers['b'], lld->month_short_name[m - 1]);
    }

  /* %c       Month, numeric (0..12) - actually (1..12) */
  sprintf (format_specifiers['c'], "%d", m);

  /* %D       Day of the m with English suffix (0th, 1st, 2nd, 3rd,...) */
  sprintf (format_specifiers['D'], "%d", d);
  /* 11-19 are special */
  if (date_lang_id == INTL_LANG_ENGLISH)
    {
      if (d % 10 == 1 && d / 10 != 1)
	{
	  strcat (format_specifiers['D'], "st");
	}
      else if (d % 10 == 2 && d / 10 != 1)
	{
	  strcat (format_specifiers['D'], "nd");
	}
      else if (d % 10 == 3 && d / 10 != 1)
	{
	  strcat (format_specifiers['D'], "rd");
	}
      else
	{
	  strcat (format_specifiers['D'], "th");
	}
    }

  /* %d       Day of the m, numeric (00..31) */
  sprintf (format_specifiers['d'], "%02d", d);

  /* %e       Day of the m, numeric (0..31) - actually (1..31) */
  sprintf (format_specifiers['e'], "%d", d);

  /* %f       Milliseconds (000..999) */
  sprintf (format_specifiers['f'], "%03d", ms);

  /* %H       Hour (00..23) */
  sprintf (format_specifiers['H'], "%02d", h);

  /* %h       Hour (01..12) */
  sprintf (format_specifiers['h'], "%02d", (h % 12 == 0) ? 12 : (h % 12));

  /* %I       Hour (01..12) */
  sprintf (format_specifiers['I'], "%02d", (h % 12 == 0) ? 12 : (h % 12));

  /* %i       Minutes, numeric (00..59) */
  sprintf (format_specifiers['i'], "%02d", mi);

  /* %j       Day of y (001..366) */
  for (j = d, i = 1; i < m; i++)
    {
      j += days[i];
    }
  sprintf (format_specifiers['j'], "%03d", j);

  /* %k       Hour (0..23) */
  sprintf (format_specifiers['k'], "%d", h);

  /* %l       Hour (1..12) */
  sprintf (format_specifiers['l'], "%d", (h % 12 == 0) ? 12 : (h % 12));

  /* %M       Month name (January..December) */
  if (m > 0)
    {
      strcpy (format_specifiers['M'], lld->month_name[m - 1]);
    }

  /* %m       Month, numeric (00..12) */
  sprintf (format_specifiers['m'], "%02d", m);

  /* %p       AM or PM */
  strcpy (format_specifiers['p'],
	  (h > 11) ? lld->am_pm[PM_NAME] : lld->am_pm[AM_NAME]);

  /* %r       Time, 12-hour (hh:mm:ss followed by AM or PM) */
  sprintf (format_specifiers['r'], "%02d:%02d:%02d %s",
	   (h % 12 == 0) ? 12 : (h % 12), mi, s,
	   (h > 11) ? lld->am_pm[PM_NAME] : lld->am_pm[AM_NAME]);

  /* %S       Seconds (00..59) */
  sprintf (format_specifiers['S'], "%02d", s);

  /* %s       Seconds (00..59) */
  sprintf (format_specifiers['s'], "%02d", s);

  /* %T       Time, 24-hour (hh:mm:ss) */
  sprintf (format_specifiers['T'], "%02d:%02d:%02d", h, mi, s);

  /* %U       Week (00..53), where Sunday is the first d of the week */
  /* %V       Week (01..53), where Sunday is the first d of the week;
     used with %X  */
  /* %X       Year for the week where Sunday is the first day of the week,
     numeric, four digits; used with %V */

  dow2 = db_get_day_of_week (y, 1, 1);

  ld_fw = 7 - dow2;

  for (days_counter = d, i = 1; i < m; i++)
    {
      days_counter += days[i];
    }

  if (days_counter <= ld_fw)
    {
      weeks = dow2 == 0 ? 1 : 0;
    }
  else
    {
      days_counter -= (dow2 == 0) ? 0 : ld_fw;
      weeks = days_counter / 7 + ((days_counter % 7) ? 1 : 0);
    }

  tu = tv = weeks;
  tx = y;
  if (tv == 0)
    {
      dow2 = db_get_day_of_week (y - 1, 1, 1);
      days_counter = 365 + LEAP (y - 1) - (dow2 == 0 ? 0 : 7 - dow2);
      tv = days_counter / 7 + ((days_counter % 7) ? 1 : 0);
      tx = y - 1;
    }

  sprintf (format_specifiers['U'], "%02d", tu);
  sprintf (format_specifiers['V'], "%02d", tv);
  sprintf (format_specifiers['X'], "%04d", tx);

  /* %u       Week (00..53), where Monday is the first d of the week */
  /* %v       Week (01..53), where Monday is the first d of the week;
     used with %x  */
  /* %x       Year for the week, where Monday is the first day of the week,
     numeric, four digits; used with %v */

  dow2 = db_get_day_of_week (y, 1, 1);
  weeks = dow2 >= 1 && dow2 <= 4 ? 1 : 0;

  ld_fw = dow2 == 0 ? 1 : 7 - dow2 + 1;

  for (days_counter = d, i = 1; i < m; i++)
    {
      days_counter += days[i];
    }

  if (days_counter > ld_fw)
    {
      days_counter -= ld_fw;
      weeks += days_counter / 7 + ((days_counter % 7) ? 1 : 0);
    }

  tu = weeks;
  tv = weeks;
  tx = y;
  if (tv == 0)
    {
      dow2 = db_get_day_of_week (y - 1, 1, 1);
      weeks = dow2 >= 1 && dow2 <= 4 ? 1 : 0;
      ld_fw = dow2 == 0 ? 1 : 7 - dow2 + 1;
      days_counter = 365 + LEAP (y - 1) - ld_fw;
      tv = weeks + days_counter / 7 + ((days_counter % 7) ? 1 : 0);
      tx = y - 1;
    }
  else if (tv == 53)
    {
      dow2 = db_get_day_of_week (y + 1, 1, 1);
      if (dow2 >= 1 && dow2 <= 4)
	{
	  tv = 1;
	  tx = y + 1;
	}
    }

  sprintf (format_specifiers['u'], "%02d", tu);
  sprintf (format_specifiers['v'], "%02d", tv);
  sprintf (format_specifiers['x'], "%04d", tx);

  /* %W       Weekday name (Sunday..Saturday) */
  STRNCPY (format_specifiers['W'], lld->day_name[dow], 64);

  /* %w       Day of the week (0=Sunday..6=Saturday) */
  sprintf (format_specifiers['w'], "%d", dow);

  /* %Y       Year, numeric, four digits */
  sprintf (format_specifiers['Y'], "%04d", y);

  /* %y       Year, numeric (two digits) */
  sprintf (format_specifiers['y'], "%02d", y % 100);

  /* 3. Generate the output according to the format and the values */
  assert (DB_VALUE_DOMAIN_TYPE (format) == DB_TYPE_VARCHAR);
  format_s = DB_PULL_STRING (format);
  format_s_len = DB_GET_STRING_SIZE (format);

  len = 1024;
  res = (char *) malloc (len);
  if (res == NULL)
    {
      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
      goto exit_on_error;
    }

  memset (res, 0, len);

  ch = *format_s;
  strend = format_s + format_s_len;
  while (format_s < strend)
    {
      format_s++;
      och = ch;
      ch = *format_s;

      if (och == '%' /* && (res[strlen(res) - 1] != '%') */ )
	{
	  if (ch == '%')
	    {
	      STRCHCAT (res, '%');

	      /* jump a character */
	      format_s++;
	      och = ch;
	      ch = *format_s;

	      continue;
	    }
	  /* parse the character */
	  if (strlen (format_specifiers[(unsigned char) ch]) == 0)
	    {
	      /* append the character itself */
	      STRCHCAT (res, ch);
	    }
	  else
	    {
	      strcat (res, format_specifiers[(unsigned char) ch]);
	    }

	  /* jump a character */
	  format_s++;
	  och = ch;
	  ch = *format_s;
	}
      else
	{
	  STRCHCAT (res, och);
	}

      /* chance of overflow ? */
      /* assume we can't add at a time mode than 16 chars */
      if (strlen (res) + 16 > len)
	{
	  /* realloc - copy temporary in res2 */
	  res2 = (char *) malloc (len);
	  if (res2 == NULL)
	    {
	      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	      goto exit_on_error;
	    }

	  memset (res2, 0, len);
	  strcpy (res2, res);
	  free_and_init (res);

	  len += 1024;
	  res = (char *) malloc (len);
	  if (res == NULL)
	    {
	      free_and_init (res2);
	      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
	      goto exit_on_error;
	    }

	  memset (res, 0, len);
	  strcpy (res, res2);
	  free_and_init (res2);
	}
    }
  /* finished string */

  /* 4. */

  DB_MAKE_STRING (result, res);
  if (result != NULL && TP_IS_CHAR_TYPE (DB_VALUE_DOMAIN_TYPE (result)))
    {
      db_string_put_cs_and_collation (result, res_collation);
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
  db_value_clear_nargs (3, &tmp_date_value, &tmp_format, &tmp_date_lang);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  if (res != NULL)
    {
      free_and_init (res);
    }

  DB_MAKE_NULL (result);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_date_value, &tmp_format, &tmp_date_lang);

  return error_status;
}

/*
 * parse_digits ()
 *
 * Arguments:
 *         s: source string to parse
 *         nr: output number
 *	   cnt: length at which we trim the number (-1 if none)
 *
 * Returns: int - actual number of characters read
 *
 * Errors:
 *
 * Note:
 *    reads cnt digits until non-digit char reached
 */
int
parse_digits (char *s, int *nr, int cnt)
{
  int count = 0, len;
  char *ch;
  /* res[64] is safe because res has a max length of cnt, which is max 4 */
  char res[64];
  const int res_count = sizeof (res) / sizeof (char);

  ch = s;
  *nr = 0;

  memset (res, 0, sizeof (res));

  while (WHITESPACE (*ch))
    {
      ch++;
      count++;
    }

  /* do not support negative numbers because... they are not supported :) */
  while (*ch != 0 && (*ch >= '0' && *ch <= '9'))
    {
      STRCHCAT (res, *ch);

      ch++;
      count++;

      /* trim at cnt characters */
      len = strlen (res);
      if (len == cnt || len == res_count - 1)
	{
	  break;
	}
    }

  *nr = atol (res);

  return count;
}

/*
 * db_str_to_date ()
 *
 * Arguments:
 *         str: string from which we get the data
 *         format: format specifiers to match the str
 *         date_lang: id of language to use
 *
 * Returns: int
 *
 * Errors:
 *
 * Note:
 *    inverse function for date_format - compose a date/time from some format
 *    specifiers and some informations.
 */
int
db_str_to_date (const DB_VALUE * str, const DB_VALUE * format,
		const DB_VALUE * date_lang, DB_VALUE * result)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_str, tmp_format, tmp_date_lang;
  char *sstr = NULL, *format_s = NULL, *format2_s = NULL;
  int i, j, k;
  int type, len1, len2, h24 = 0, _v, _x;
  DB_TYPE res_type;
  int days[13] = { 0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
  int y, m, d, h, mi, s, ms, am /* 0 = AM, 1 = PM */ ;
  int u, U, v, V, dow, doy, w;
  char stack_buf_str[64];
  char *initial_buf_str = NULL;
  bool do_free_buf_str = false;
  INTL_LANG date_lang_id;
  bool dummy;

  assert (str != result);
  assert (format != result);
  assert (date_lang != result);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (3, &tmp_str, &tmp_format, &tmp_date_lang);

  /* arg check null *********************************************************
   */
  if (str == NULL || format == NULL || date_lang == NULL)
    {
      error_status = ER_OBJ_INVALID_ARGUMENTS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  if (DB_IS_NULL_NARGS (3, str, format, date_lang))
    {
      DB_MAKE_NULL (result);
      goto done;
    }

  /* arg cast type **********************************************************
   */
  str = db_value_cast_arg (str, &tmp_str, DB_TYPE_VARCHAR, &error_status);
  format =
    db_value_cast_arg (format, &tmp_format, DB_TYPE_VARCHAR, &error_status);
  date_lang =
    db_value_cast_arg (date_lang, &tmp_date_lang, DB_TYPE_INTEGER,
		       &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  assert (lang_charset_name (INTL_CODESET_UTF8) != NULL);
  assert (DB_VALUE_TYPE (date_lang) == DB_TYPE_INTEGER);
  date_lang_id = lang_get_lang_id_from_flag (DB_GET_INT (date_lang), &dummy,
					     &dummy);
  if (lang_get_specific_locale (date_lang_id) == NULL)
    {
      error_status = ER_LANG_CODESET_NOT_AVAILABLE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
	      lang_get_lang_name_from_id (date_lang_id),
	      lang_charset_name (INTL_CODESET_UTF8));
      goto exit_on_error;
    }

  y = m = d = V = v = U = u = -1;
  h = mi = s = ms = 0;
  dow = doy = am = -1;
  _v = _x = 0;

  error_status =
    db_check_or_create_null_term_string (str, stack_buf_str,
					 sizeof (stack_buf_str),
					 true, true,
					 &initial_buf_str, &do_free_buf_str);
  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

  sstr = initial_buf_str;

  format2_s = DB_PULL_STRING (format);
  len2 = DB_GET_STRING_SIZE (format);
  len2 = (len2 < 0) ? strlen (format2_s) : len2;

  format_s = (char *) malloc (len2 + 1);
  if (format_s == NULL)
    {
      error_status = ER_OUT_OF_VIRTUAL_MEMORY;
      goto exit_on_error;
    }

  memset (format_s, 0, sizeof (char) * (len2 + 1));

  /* delete all whitespace from format */
  for (i = 0; i < len2; i++)
    {
      if (!WHITESPACE (format2_s[i]))
	{
	  STRCHCAT (format_s, format2_s[i]);
	}
      /* '%' without format specifier */
      else if (WHITESPACE (format2_s[i]) && i > 0 && format2_s[i - 1] == '%')
	{
	  error_status = ER_OBJ_INVALID_ARGUMENTS;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	  goto exit_on_error;
	}
    }

  type = db_check_time_date_format (format_s);
  if (type == 1)
    {
      res_type = DB_TYPE_TIME;
    }
  else if (type == 2)
    {
      res_type = DB_TYPE_DATE;
    }
  else if (type == 3)
    {
      res_type = DB_TYPE_DATETIME;
    }
  else
    {
      error_status = ER_OBJ_INVALID_ARGUMENTS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  /*
   * 1. Get information according to format specifiers
   *    iterate simultaneously through each string and sscanf when
   *    it is a format specifier.
   *    If a format specifier has more than one occurence, get the last value.
   */
  do
    {
      len1 = strlen (sstr);
      len2 = strlen (format_s);

      i = j = k = 0;

      while (i < len1 && j < len2)
	{
	  while (WHITESPACE (sstr[i]))
	    {
	      i++;
	    }

	  while (WHITESPACE (format_s[j]))
	    {
	      j++;
	    }

	  if (j > 0 && format_s[j - 1] == '%')
	    {
	      int token_size;
	      int am_pm_id;

	      /* do not accept a double % */
	      if (j > 1 && format_s[j - 2] == '%')
		{
		  error_status = ER_OBJ_INVALID_ARGUMENTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
		  goto exit_on_error;
		}

	      /* we have a format specifier */
	      switch (format_s[j])
		{
		case 'a':
		  /* %a Abbreviated weekday name (Sun..Sat) */
		  error_status = get_string_date_token_id (SDT_DAY_SHORT,
							   date_lang_id,
							   sstr + i, &dow,
							   &token_size);
		  if (error_status != NO_ERROR)
		    {
		      goto exit_on_error;
		    }

		  i += token_size;

		  if (dow == 0)	/* not found - error */
		    {
		      goto exit_on_error;
		    }

		  dow = dow - 1;
		  break;

		case 'b':
		  /* %b Abbreviated month name (Jan..Dec) */
		  error_status = get_string_date_token_id (SDT_MONTH_SHORT,
							   date_lang_id,
							   sstr + i, &m,
							   &token_size);
		  if (error_status != NO_ERROR)
		    {
		      goto exit_on_error;
		    }

		  i += token_size;

		  if (m == 0)	/* not found - error */
		    {
		      goto exit_on_error;
		    }
		  break;

		case 'c':
		  /* %c Month, numeric (0..12) */
		  k = parse_digits (sstr + i, &m, 2);
		  if (k <= 0)
		    {
		      goto exit_on_error;
		    }
		  i += k;
		  break;

		case 'D':
		  /* %D Day of the month with English suffix (0th, 1st, 2nd,
		     3rd, ...) */
		  k = parse_digits (sstr + i, &d, 2);
		  if (k <= 0)
		    {
		      goto exit_on_error;
		    }
		  i += k;
		  /* need 2 necessary characters or whitespace (!) after */
		  i += 2;
		  break;

		case 'd':
		  /* %d Day of the month, numeric (00..31) */
		  k = parse_digits (sstr + i, &d, 2);
		  if (k <= 0)
		    {
		      goto exit_on_error;
		    }
		  i += k;
		  break;

		case 'e':
		  /* %e Day of the month, numeric (0..31) */
		  k = parse_digits (sstr + i, &d, 2);
		  if (k <= 0)
		    {
		      goto exit_on_error;
		    }
		  i += k;
		  break;

		case 'f':
		  /* %f Milliseconds (000..999) */
		  k = parse_digits (sstr + i, &ms, 3);
		  if (k <= 0)
		    {
		      goto exit_on_error;
		    }
		  i += k;
		  break;

		case 'H':
		  /* %H Hour (00..23) */
		  k = parse_digits (sstr + i, &h, 2);
		  if (k <= 0)
		    {
		      goto exit_on_error;
		    }
		  i += k;
		  h24 = 1;
		  break;

		case 'h':
		  /* %h Hour (01..12) */
		  k = parse_digits (sstr + i, &h, 2);
		  if (k <= 0)
		    {
		      goto exit_on_error;
		    }
		  i += k;
		  break;

		case 'I':
		  /* %I Hour (01..12) */
		  k = parse_digits (sstr + i, &h, 2);
		  if (k <= 0)
		    {
		      goto exit_on_error;
		    }
		  i += k;
		  break;

		case 'i':
		  /* %i Minutes, numeric (00..59) */
		  k = parse_digits (sstr + i, &mi, 2);
		  if (k <= 0)
		    {
		      goto exit_on_error;
		    }
		  i += k;
		  break;

		case 'j':
		  /* %j Day of year (001..366) */
		  k = parse_digits (sstr + i, &doy, 3);
		  if (k <= 0)
		    {
		      goto exit_on_error;
		    }
		  i += k;
		  break;

		case 'k':
		  /* %k Hour (0..23) */
		  k = parse_digits (sstr + i, &h, 2);
		  if (k <= 0)
		    {
		      goto exit_on_error;
		    }
		  i += k;
		  h24 = 1;
		  break;

		case 'l':
		  /* %l Hour (1..12) */
		  k = parse_digits (sstr + i, &h, 2);
		  if (k <= 0)
		    {
		      goto exit_on_error;
		    }
		  i += k;
		  break;

		case 'M':
		  /* %M Month name (January..December) */
		  error_status = get_string_date_token_id (SDT_MONTH,
							   date_lang_id,
							   sstr + i, &m,
							   &token_size);
		  if (error_status != NO_ERROR)
		    {
		      goto exit_on_error;
		    }

		  i += token_size;

		  if (m == 0)	/* not found - error */
		    {
		      goto exit_on_error;
		    }
		  break;

		case 'm':
		  /* %m Month, numeric (00..12) */
		  k = parse_digits (sstr + i, &m, 2);
		  if (k <= 0)
		    {
		      goto exit_on_error;
		    }
		  i += k;
		  break;

		case 'p':
		  /* %p AM or PM */
		  error_status = get_string_date_token_id (SDT_AM_PM,
							   date_lang_id,
							   sstr + i,
							   &am_pm_id,
							   &token_size);
		  if (error_status != NO_ERROR)
		    {
		      goto exit_on_error;
		    }

		  i += token_size;

		  if (am_pm_id > 0)
		    {
		      if (am_pm_id % 2)
			{
			  am = 0;
			}
		      else
			{
			  am = 1;
			}
		    }
		  else
		    {
		      goto exit_on_error;
		    }

		  break;

		case 'r':
		  /* %r Time, 12-hour (hh:mm:ss followed by AM or PM) */

		  k = parse_digits (sstr + i, &h, 2);
		  if (k <= 0)
		    {
		      goto exit_on_error;
		    }
		  i += k;

		  while (WHITESPACE (sstr[i]))
		    {
		      i++;
		    }

		  if (sstr[i] != ':')
		    {
		      goto exit_on_error;
		    }
		  i++;

		  k = parse_digits (sstr + i, &mi, 2);
		  if (k <= 0)
		    {
		      goto exit_on_error;
		    }
		  i += k;

		  while (WHITESPACE (sstr[i]))
		    {
		      i++;
		    }

		  if (sstr[i] != ':')
		    {
		      goto exit_on_error;
		    }
		  i++;

		  k = parse_digits (sstr + i, &s, 2);
		  if (k <= 0)
		    {
		      goto exit_on_error;
		    }
		  i += k;

		  error_status = get_string_date_token_id (SDT_AM_PM,
							   date_lang_id,
							   sstr + i,
							   &am_pm_id,
							   &token_size);
		  if (error_status != NO_ERROR)
		    {
		      goto exit_on_error;
		    }

		  i += token_size;

		  if (am_pm_id > 0)
		    {
		      if (am_pm_id % 2)
			{
			  am = 0;
			}
		      else
			{
			  am = 1;
			}
		    }
		  else
		    {
		      goto exit_on_error;
		    }

		  break;

		case 'S':
		  /* %S Seconds (00..59) */
		  k = parse_digits (sstr + i, &s, 2);
		  if (k <= 0)
		    {
		      goto exit_on_error;
		    }
		  i += k;
		  break;

		case 's':
		  /* %s Seconds (00..59) */
		  k = parse_digits (sstr + i, &s, 2);
		  if (k <= 0)
		    {
		      goto exit_on_error;
		    }
		  i += k;
		  break;

		case 'T':
		  /* %T Time, 24-hour (hh:mm:ss) */

		  k = parse_digits (sstr + i, &h, 2);
		  if (k <= 0)
		    {
		      goto exit_on_error;
		    }
		  i += k;

		  while (WHITESPACE (sstr[i]))
		    {
		      i++;
		    }

		  if (sstr[i] != ':')
		    {
		      goto exit_on_error;
		    }
		  i++;

		  k = parse_digits (sstr + i, &mi, 2);
		  if (k <= 0)
		    {
		      goto exit_on_error;
		    }

		  i += k;
		  while (WHITESPACE (sstr[i]))
		    {
		      i++;
		    }

		  if (sstr[i] != ':')
		    {
		      goto exit_on_error;
		    }
		  i++;

		  k = parse_digits (sstr + i, &s, 2);
		  if (k <= 0)
		    {
		      goto exit_on_error;
		    }
		  i += k;
		  h24 = 1;

		  break;

		case 'U':
		  /* %U Week (00..53), where Sunday is the first day
		     of the week */
		  k = parse_digits (sstr + i, &U, 2);
		  if (k <= 0)
		    {
		      goto exit_on_error;
		    }
		  i += k;
		  break;

		case 'u':
		  /* %u Week (00..53), where Monday is the first day
		     of the week */
		  k = parse_digits (sstr + i, &u, 2);
		  if (k <= 0)
		    {
		      goto exit_on_error;
		    }
		  i += k;
		  break;

		case 'V':
		  /* %V Week (01..53), where Sunday is the first day
		     of the week; used with %X  */
		  k = parse_digits (sstr + i, &V, 2);
		  if (k <= 0)
		    {
		      goto exit_on_error;
		    }
		  i += k;
		  _v = 1;
		  break;

		case 'v':
		  /* %v Week (01..53), where Monday is the first day
		     of the week; used with %x  */
		  k = parse_digits (sstr + i, &v, 2);
		  if (k <= 0)
		    {
		      goto exit_on_error;
		    }
		  i += k;
		  _v = 2;
		  break;

		case 'W':
		  /* %W Weekday name (Sunday..Saturday) */
		  error_status = get_string_date_token_id (SDT_DAY,
							   date_lang_id,
							   sstr + i, &dow,
							   &token_size);
		  if (error_status != NO_ERROR)
		    {
		      goto exit_on_error;
		    }

		  i += token_size;

		  if (dow == 0)	/* not found - error */
		    {
		      goto exit_on_error;
		    }
		  dow = dow - 1;
		  break;

		case 'w':
		  /* %w Day of the week (0=Sunday..6=Saturday) */
		  k = parse_digits (sstr + i, &dow, 1);
		  if (k <= 0)
		    {
		      goto exit_on_error;
		    }
		  i += k;
		  break;

		case 'X':
		  /* %X Year for the week where Sunday is the first day
		     of the week, numeric, four digits; used with %V  */
		  k = parse_digits (sstr + i, &y, 4);
		  if (k <= 0)
		    {
		      goto exit_on_error;
		    }
		  i += k;
		  _x = 1;
		  break;

		case 'x':
		  /* %x Year for the week, where Monday is the first day
		     of the week, numeric, four digits; used with %v  */
		  k = parse_digits (sstr + i, &y, 4);
		  if (k <= 0)
		    {
		      goto exit_on_error;
		    }
		  i += k;
		  _x = 2;
		  break;

		case 'Y':
		  /* %Y Year, numeric, four digits */
		  k = parse_digits (sstr + i, &y, 4);
		  if (k <= 0)
		    {
		      goto exit_on_error;
		    }
		  i += k;
		  break;

		case 'y':
		  /* %y Year, numeric (two digits) */
		  k = parse_digits (sstr + i, &y, 2);
		  if (k <= 0)
		    {
		      goto exit_on_error;
		    }
		  i += k;

		  /* TODO: 70 convention always available? */
		  if (y < 70)
		    {
		      y = 2000 + y;
		    }
		  else
		    {
		      y = 1900 + y;
		    }

		  break;

		default:
		  goto exit_on_error;
		  break;
		}
	    }
	  else if (sstr[i] != format_s[j] && format_s[j] != '%')
	    {
	      error_status = ER_OBJ_INVALID_ARGUMENTS;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
	      goto exit_on_error;
	    }
	  else if (format_s[j] != '%')
	    {
	      i++;
	    }

	  /* when is a format specifier do not advance in sstr
	     because we need the entire value */
	  j++;
	}
    }
  while (0);

  /* 2. Validations */
  if (am != -1)			/* 24h time format and am/pm */
    {
      if (h24 == 1 || h == 0)
	{
	  goto exit_on_error;
	}

      if (h == 12)
	{
	  h = 0;
	}
    }
  if (h24 == 0 && h > 12)
    {
      goto exit_on_error;
    }

  if (_x != _v && _x != -1)	/* accept %v only if %x and %V only if %X */
    {
      goto exit_on_error;
    }

  days[2] += LEAP (y);

  /*
   * validations are done here because they are done just on the last memorized
   * values (ie: if you supply a month 99 then a month 12 the 99 isn't validated
   * because it's overwritten by 12 which is correct).
   */

  /*
   * check only upper bounds, lower bounds will be checked later and
   * will return error
   */
  if (res_type == DB_TYPE_DATE || res_type == DB_TYPE_DATETIME)
    {
      /* replace invalid initial year with default year */
      y = (y == -1) ? 1 : y;
      /* year is validated becuase it's vital for m & d */
      if (y > 9999)
	{
	  goto exit_on_error;
	}

      if (m > 12)
	{
	  goto exit_on_error;
	}

      /* because we do not support invalid dates ... */
      if (m != -1 && d > days[m])
	{
	  goto exit_on_error;
	}

      if (u > 53)
	{
	  goto exit_on_error;
	}

      if (v > 53)
	{
	  goto exit_on_error;
	}

      if (v == 0 || u > 53)
	{
	  goto exit_on_error;
	}

      if (V == 0 || u > 53)
	{
	  goto exit_on_error;
	}

      if (doy == 0 || doy > 365 + LEAP (y))
	{
	  goto exit_on_error;
	}

      if (dow > 6)
	{
	  goto exit_on_error;
	}
    }

  if (res_type == DB_TYPE_TIME || res_type == DB_TYPE_DATETIME)
    {
      if ((am != -1 && h > 12) || (am == -1 && h > 23))
	{
	  goto exit_on_error;
	}
      if (am == 1 && h != -1)
	{
	  h += 12;
	  /* reset AM flag */
	  am = -1;
	}

      if (mi > 59)
	{
	  goto exit_on_error;
	}

      if (s > 59)
	{
	  goto exit_on_error;
	}
      /* milli does not need checking, it has all values from 0 to 999 */
    }


  /* 3. Try to compute a date according to the information from the format
     specifiers */

  if (res_type == DB_TYPE_TIME)
    {
      /* --- no job to do --- */
      goto write_results;
    }

  /* the year is fixed, compute the day and month from dow, doy, etc */
  /*
   * the day and month can be supplied specifically which suppres all other
   * informations or can be computed from dow and week or from doy
   */

  /* 3.1 - we have a valid day and month */
  if (m >= 1 && m <= 12 && d >= 1 && d <= days[m])
    {
      /* --- no job to do --- */
      goto write_results;
    }

  w = MAX (v, MAX (V, MAX (u, U)));
  /* 3.2 - we have the day of week and a week */
  if (dow != -1 && w != -1)
    {
      int dow2 = db_get_day_of_week (y, 1, 1);
      int ld_fw, save_dow, dowdiff;

      if (U == w || V == w)
	{
	  ld_fw = 7 - dow2;

	  if (w == 0)
	    {
	      dowdiff = dow - dow2;
	      d = dow2 == 0 ? 32 - (7 - dow) : dowdiff < 0 ?
		32 + dowdiff : 1 + dowdiff;
	      m = dow2 == 0 || dowdiff < 0 ? 12 : 1;
	      y = dow2 == 0 || dowdiff < 0 ? y - 1 : y;
	    }
	  else
	    {
	      d = dow2 == 0 ? 1 : ld_fw + 1;
	      m = 1;
	      if (db_add_weeks_and_days_to_date (&d, &m, &y, w - 1, dow) ==
		  ER_FAILED)
		{
		  goto exit_on_error;
		}
	    }
	}
      else if (u == w || v == w)
	{
	  ld_fw = dow2 == 0 ? 1 : 7 - dow2 + 1;
	  if (w == 0 || w == 1)
	    {
	      save_dow = dow;
	      dow = dow == 0 ? 7 : dow;
	      dow2 = dow2 == 0 ? 7 : dow2;
	      dowdiff = dow - dow2;

	      if (dow2 >= 1 && dow2 <= 4)	/* start with week 1 */
		{
		  d = w == 0 ? 32 + dowdiff - 7 :
		    dowdiff < 0 ? 32 + dowdiff : 1 + dowdiff;
		  m = w == 0 || dowdiff < 0 ? 12 : 1;
		  y = w == 0 || dowdiff < 0 ? y - 1 : y;
		}
	      else
		{
		  d = dowdiff < 0 ? (w == 0 ? 32 + dowdiff : ld_fw + dow) :
		    (w == 0 ? 1 + dowdiff : 1 + dowdiff + 7);
		  m = dowdiff < 0 && w == 0 ? 12 : 1;
		  y = dowdiff < 0 && w == 0 ? y - 1 : y;
		}
	      dow = save_dow;
	    }
	  else
	    {
	      d = ld_fw + 1;
	      m = 1;

	      if (db_add_weeks_and_days_to_date (&d, &m, &y,
						 dow2 >= 1
						 && dow2 <= 4 ? w - 2 : w - 1,
						 dow == 0 ? 6 : dow - 1) ==
		  ER_FAILED)
		{
		  goto exit_on_error;
		}
	    }
	}
      else
	{
	  goto exit_on_error;	/* should not happen */
	}
    }
  /* 3.3 - we have the day of year */
  else if (doy != -1)
    {
      for (m = 1; m <= 12 && doy > days[m]; m++)
	{
	  doy -= days[m];
	}

      d = doy;
    }

write_results:
  /* last validations before writing results - we need only complete data info */

  if (res_type == DB_TYPE_DATE || res_type == DB_TYPE_DATETIME)
    {
      /* replace invalid initial date (-1,-1,-1) with default date (1,1,1) */
      y = (y == -1) ? 1 : y;
      m = (m == -1) ? 1 : m;
      d = (d == -1) ? 1 : d;

      if (y < 0 || m < 0 || m > 12 || d < 0)
	{
	  goto exit_on_error;
	}

      if (d > days[m])
	{
	  goto exit_on_error;
	}
    }

  if (res_type == DB_TYPE_TIME || res_type == DB_TYPE_DATETIME)
    {
      if (h < 0 || mi < 0 || s < 0)
	{
	  goto exit_on_error;
	}
    }

  if (res_type == DB_TYPE_DATE)
    {
      DB_MAKE_DATE (result, m, d, y);
    }
  else if (res_type == DB_TYPE_TIME)
    {
      DB_MAKE_TIME (result, h, mi, s);
    }
  else if (res_type == DB_TYPE_DATETIME)
    {
      DB_DATETIME db_datetime;

      db_datetime_encode (&db_datetime, m, d, y, h, mi, s, ms);

      DB_MAKE_DATETIME (result, &db_datetime);
    }

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);

  if (do_free_buf_str)
    {
      free_and_init (initial_buf_str);
    }

  if (format_s)
    {
      free_and_init (format_s);
    }

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_str, &tmp_format, &tmp_date_lang);

  return error_status;

exit_on_error:

  if (error_status == NO_ERROR)
    {
      error_status = ER_DATE_CONVERSION;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
    }
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  if (do_free_buf_str)
    {
      free_and_init (initial_buf_str);
    }

  if (format_s)
    {
      free_and_init (format_s);
    }

  DB_MAKE_NULL (result);

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (3, &tmp_str, &tmp_format, &tmp_date_lang);

  return error_status;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 *  parse_time_string - parse a string given by the second argument of
 *                      timestamp function
 *  return: NO_ERROR
 *
 *  timestr(in)	    : input string
 *  timestr_size(in): input string size
 *  sign(out)	    : 0 if positive, -1 if negative
 *  h(out)	    : hours
 *  m(out)	    : minutes
 *  s(out)	    : seconds
 *  ms(out)	    : milliseconds
 */
static int
parse_time_string (const char *timestr, int timestr_size, int *sign, int *h,
		   int *m, int *s, int *ms)
{
  int args[4], num_args = 0, tmp;
  const char *ch;
  const char *dot = NULL, *end;

  assert (sign != NULL && h != NULL && m != NULL && s != NULL && ms != NULL);
  *sign = *h = *m = *s = *ms = 0;

  if (!timestr || !timestr_size)
    {
      return NO_ERROR;
    }

  ch = timestr;
  end = timestr + timestr_size;

  SKIP_SPACES (ch, end);

  if (*ch == '-')
    {
      *sign = 1;
      ch++;
    }

  /* Find dot('.') to separate milli-seconds part from whole string. */
  dot = ch;
  while (dot != end && *dot != '.')
    {
      dot++;
    }

  if (dot != end)
    {
      char ms_string[4];

      dot++;
      tmp = end - dot;
      if (tmp)
	{
	  tmp = (tmp < 3 ? tmp : 3);
	  strncpy (ms_string, dot, tmp);
	}
      ms_string[3] = '\0';

      switch (tmp)
	{
	case 0:
	  *ms = 0;
	  break;

	case 1:
	  ms_string[1] = '0';
	case 2:
	  ms_string[2] = '0';
	default:
	  *ms = atoi (ms_string);
	}
    }

  /* First ':' character means '0:'. */
  SKIP_SPACES (ch, end);
  if (ch != end && *ch == ':')
    {
      args[num_args++] = 0;
      ch++;
    }

  if (ch != end)
    {
      while (num_args < (int) (sizeof (args) / sizeof (*args))
	     && char_isdigit (*ch))
	{
	  tmp = 0;
	  do
	    {
	      /* check for overflow */
	      if (tmp >= INT_MAX / 10)
		{
		  tmp = INT_MAX;
		}
	      else
		{
		  tmp = tmp * 10 + *ch - '0';
		}
	      ch++;
	    }
	  while (ch != end && char_isdigit (*ch));

	  args[num_args++] = tmp;

	  /* Digits should be separated by ':' character.
	   * If we meet other characters, stop parsing.
	   */
	  if (ch == end || *ch != ':')
	    {
	      break;
	    }
	  ch++;
	}
    }

  switch (num_args)
    {
    case 1:
      /* Consider single value as H...HMMSS. */
      *s = args[0] % 100;
      args[0] /= 100;
      *m = args[0] % 100;
      *h = args[0] / 100;
      break;

    case 2:
      *h = args[0];
      *m = args[1];
      break;

    case 3:
      *h = args[0];
      *m = args[1];
      *s = args[2];
      break;

    case 0:
    default:
      /* do nothing */
      ;
    }
  return NO_ERROR;
}
#endif

/*
 * db_check_or_create_null_term_string () - checks if the buffer associated to
 *		      string DB_VALUE is null terminated; if it is returns it
 *                    LIKE index optimization
 *
 * returns: NO_ERROR or error code
 *
 * str_val(in): source string DB_VALUE
 * pre_alloc_buf(in): preallocated buffer to store null terminated string
 * pre_alloc_buf_size(in): size of preallocated buffer
 * ignore_prec_spaces(in): true if it should ignore preceding spaces
 *			   (used only when new buffer needs to be allocated)
 * ignore_trail_spaces(in): true if it should ignore trailing spaces
 *			   (used only when new buffer needs to be allocated)
 * str_out(out): pointer to null terminated string
 * do_alloc(out): set to true if new buffer was allocated
 *
 */
static int
db_check_or_create_null_term_string (const DB_VALUE * str_val,
				     char *pre_alloc_buf,
				     int pre_alloc_buf_size,
				     bool ignore_prec_spaces,
				     bool ignore_trail_spaces,
				     char **str_out, bool * do_alloc)
{
  char *val_buf;
  char *new_buf;
  char *val_buf_end = NULL, *val_buf_end_non_space = NULL;
  int val_size;

  assert (pre_alloc_buf != NULL);
  assert (pre_alloc_buf_size > 1);
  assert (str_out != NULL);
  assert (do_alloc != NULL);
  assert (QSTR_IS_CHAR (DB_VALUE_DOMAIN_TYPE (str_val)));

  *do_alloc = false;

  val_buf = DB_GET_STRING (str_val);
  if (val_buf == NULL)
    {
      *str_out = NULL;
      return NO_ERROR;
    }
  val_size = DB_GET_STRING_SIZE (str_val);

  /* size < 0 assumes a null terminated string */
  if (val_size < 0
      || ((val_size < DB_VALUE_PRECISION (str_val)
	   || DB_VALUE_PRECISION (str_val) == TP_FLOATING_PRECISION_VALUE)
	  && val_buf[val_size] == '\0'))
    {
      /* already null terminated , safe to use it */
      *str_out = val_buf;
      return NO_ERROR;
    }

  if (val_size < pre_alloc_buf_size)
    {
      /* use the preallocated buffer supplied to copy the content */
      strncpy (pre_alloc_buf, val_buf, val_size);
      pre_alloc_buf[val_size] = '\0';
      *str_out = pre_alloc_buf;
      return NO_ERROR;
    }

  /* trim preceding and trailing spaces */
  val_buf_end = val_buf + val_size;
  if (ignore_prec_spaces)
    {
      while (val_buf < val_buf_end &&
	     ((*val_buf) == ' ' || (*val_buf) == '\t' ||
	      (*val_buf) == '\r' || (*val_buf) == '\n'))
	{
	  val_buf++;
	}
      val_size = val_buf_end - val_buf;
      assert (val_size >= 0);
    }

  if (ignore_trail_spaces && val_size > 0)
    {
      val_buf_end_non_space = val_buf + val_size - 1;

      while (val_buf < val_buf_end_non_space &&
	     ((*val_buf_end_non_space) == ' ' ||
	      (*val_buf_end_non_space) == '\t' ||
	      (*val_buf_end_non_space) == '\r' ||
	      (*val_buf_end_non_space) == '\n'))
	{
	  val_buf_end_non_space--;
	}
      val_size = val_buf_end_non_space - val_buf + 1;
      assert (val_size >= 0);
    }

  if (val_size < pre_alloc_buf_size)
    {
      assert (ignore_prec_spaces || ignore_trail_spaces);

      /* use the preallocated buffer supplied to copy the content */
      strncpy (pre_alloc_buf, val_buf, val_size);
      pre_alloc_buf[val_size] = '\0';
      *str_out = pre_alloc_buf;
      return NO_ERROR;
    }

  new_buf = (char *) malloc (val_size + 1);
  if (new_buf == NULL)
    {
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }
  strncpy (new_buf, val_buf, val_size);
  new_buf[val_size] = '\0';
  *str_out = new_buf;
  *do_alloc = true;

  return NO_ERROR;
}

/*
 * get_string_date_token_id() - get the id of date token identifier
 *   return: NO_ERROR or error code
 *   token_type(in): string-to-date token type
 *   intl_lang_id(in):
 *   cs(in): input string to search for token (considered NULL terminated)
 *   token_id(out): id of token (if non-zero) or zero if not found;
 *		    range begins from 1 :days 1 - 7, months 1 - 12
 *   token_size(out): size in bytes ocupied by token in input string 'cs'
 */
static int
get_string_date_token_id (const STRING_DATE_TOKEN token_type,
			  const INTL_LANG intl_lang_id, const char *cs,
			  int *token_id, int *token_size)
{
  const char **p;
  int error_status = NO_ERROR;
  int search_size;
  const char *parse_order;
  int i;
  int cs_size;
  int skipped_leading_chars = 0;
  const LANG_LOCALE_DATA *lld = lang_get_specific_locale (intl_lang_id);

  assert (cs != NULL);
  assert (token_id != NULL);
  assert (token_size != NULL);

  if (lld == NULL)
    {
      error_status = ER_LANG_CODESET_NOT_AVAILABLE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
	      lang_get_lang_name_from_id (intl_lang_id),
	      lang_charset_name (INTL_CODESET_UTF8));
      return error_status;
    }

  switch (token_type)
    {
    case SDT_DAY:
      p = (const char **) lld->day_name;
      parse_order = lld->day_parse_order;
      search_size = 7;
      break;
    case SDT_DAY_SHORT:
      p = (const char **) lld->day_short_name;
      parse_order = lld->day_short_parse_order;
      search_size = 7;
      break;
    case SDT_MONTH:
      p = (const char **) lld->month_name;
      parse_order = lld->month_parse_order;
      search_size = 12;
      break;
    case SDT_MONTH_SHORT:
      p = (const char **) lld->month_short_name;
      parse_order = lld->month_short_parse_order;
      search_size = 12;
      break;
    case SDT_AM_PM:
      p = (const char **) lld->am_pm;
      parse_order = lld->am_pm_parse_order;
      search_size = 12;
      break;
    default:
      assert (false);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
      return ER_GENERIC_ERROR;
    }

  *token_id = 0;

  while (WHITESPACE (*cs))
    {
      cs++;
      skipped_leading_chars++;
    }

  cs_size = strlen (cs);

  for (i = 0; i < search_size; i++)
    {
      int cmp = 0;
      int token_index = parse_order[i];
      cmp =
	intl_case_match_tok (intl_lang_id,
			     (unsigned char *) p[token_index],
			     (unsigned char *) cs, strlen (p[token_index]),
			     cs_size, token_size);

      assert (*token_size <= cs_size);

      if (cmp == 0)
	{
	  *token_id = token_index + 1;
	  *token_size += skipped_leading_chars;
	  break;
	}
    }

  return NO_ERROR;
}

/*
 * print_string_date_token() - prints a date token to a buffer
 *   return: NO_ERROR or error code
 *   token_type(in): string-to-date token type
 *   intl_lang_id(in): locale identifier
 *   token_id(in): id of token (zero-based index)
 *		   for days: 0 - 6, months: 0 - 11
 *   case_mode(in): casing for printing token:
 *		    0 : unchanged; 1 - force lowercase; 2 - force uppercase
 *   buffer(in/out) : buffer to print to
 *   token_size(out): size in bytes of token printed
 */
static int
print_string_date_token (const STRING_DATE_TOKEN token_type,
			 const INTL_LANG intl_lang_id, int token_id,
			 int case_mode, char *buffer, int *token_size)
{
  const char *p;
  int error_status = NO_ERROR;
  int token_len;
  int token_bytes;
  int print_len = -1;
  const LANG_LOCALE_DATA *lld = lang_get_specific_locale (intl_lang_id);

  assert (buffer != NULL);
  assert (token_id >= 0);
  assert (token_size != NULL);

  if (lld == NULL)
    {
      error_status = ER_LANG_CODESET_NOT_AVAILABLE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 2,
	      lang_get_lang_name_from_id (intl_lang_id),
	      lang_charset_name (INTL_CODESET_UTF8));
      return error_status;
    }

  switch (token_type)
    {
    case SDT_DAY:
      assert (token_id < 7);
      p = lld->day_name[token_id];

      /* day names for all language use at most 9 chars */
      print_len = 9;
      break;

    case SDT_DAY_SHORT:
      assert (token_id < 7);
      p = lld->day_short_name[token_id];

      switch (intl_lang_id)
	{
	case INTL_LANG_ENGLISH:
	  print_len = 3;
	  break;
	default:
	  print_len = -1;
	  break;
	}
      break;

    case SDT_MONTH:
      assert (token_id < 12);
      p = lld->month_name[token_id];

      switch (intl_lang_id)
	{
	case INTL_LANG_ENGLISH:
	  print_len = 9;
	  break;
	default:
	  print_len = -1;
	  break;
	}

      break;

    case SDT_MONTH_SHORT:
      assert (token_id < 12);
      p = lld->month_short_name[token_id];

      /* all short names for months have 3 chars */
      print_len = 3;
      break;

    case SDT_AM_PM:
      assert (token_id < 12);
      p = lld->am_pm[token_id];

      /* AM/PM tokens are printed without padding */
      print_len = -1;
      break;

    default:
      assert (false);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
      return ER_GENERIC_ERROR;
    }

  /* determine length of token */
  token_bytes = strlen (p);
  intl_char_count ((unsigned char *) p, token_bytes, &token_len);

  if (case_mode == 2)
    {
      /* uppercase */
      intl_upper_string (&(lld->alphabet), (unsigned char *) p,
			 (unsigned char *) buffer, token_len);
      intl_char_size ((unsigned char *) buffer, token_len, token_size);
    }
  else if (case_mode == 1)
    {
      /* lowercase */
      intl_lower_string (&(lld->alphabet), (unsigned char *) p,
			 (unsigned char *) buffer, token_len);
      intl_char_size ((unsigned char *) buffer, token_len, token_size);
    }
  else
    {
      intl_char_size ((unsigned char *) p, token_len, token_size);
      memcpy (buffer, p, *token_size);
    }

  /* padding */
  if (token_len < print_len)
    {
      (void) qstr_pad_string ((unsigned char *) buffer + *token_size,
			      print_len - token_len);
      *token_size += intl_pad_size () * (print_len - token_len);
    }

  return NO_ERROR;
}

/*
 * init_builtin_calendar_names() - initializes builtin localizations for
 *				   calendar names
 *   return: void
 *   lld(in/out): locale data
 *
 */
void
init_builtin_calendar_names (LANG_LOCALE_DATA * lld)
{
  int i;

  assert (lld != NULL);

  if (lld->codeset == INTL_CODESET_UTF8)
    {
      for (i = 0; i < 7; i++)
	{
	  lld->day_short_name[i] = Short_Day_name_UTF8[lld->lang_id][i];
	  lld->day_name[i] = Day_name_UTF8[lld->lang_id][i];
	}

      for (i = 0; i < 12; i++)
	{
	  lld->month_short_name[i] = Short_Month_name_UTF8[lld->lang_id][i];
	  lld->month_name[i] = Month_name_UTF8[lld->lang_id][i];
	}

      for (i = 0; i < 12; i++)
	{
	  lld->am_pm[i] = Am_Pm_name_UTF8[lld->lang_id][i];
	}
    }

  lld->month_parse_order = Month_name_parse_order[lld->lang_id];
  lld->month_short_parse_order = Short_Month_name_parse_order[lld->lang_id];
  lld->day_parse_order = Day_name_parse_order[lld->lang_id];
  lld->day_short_parse_order = Short_Day_name_parse_order[lld->lang_id];
  lld->am_pm_parse_order = AM_PM_parse_order[lld->lang_id];
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * db_get_date_format () -
 * Returns: error number
 * format_str(in):
 * format(in/out):
 *
 */
int
db_get_date_format (const DB_VALUE * format_str, DATETIME_FORMAT * format)
{
  int error_status = NO_ERROR;
  DB_VALUE tmp_format_str;
  char *fmt_str_ptr, *next_fmt_str_ptr, *last_fmt;
  char stack_buf_format[64];
  char *initial_buf_format = NULL;
  bool do_free_buf_format = false;
  int format_size;

  assert (format_str != NULL);
  assert (format != NULL);

  /* arg init tmp ***********************************************************
   */
  DB_MAKE_NULL_NARGS (1, &tmp_format_str);

  /* arg check null *********************************************************
   */
  if (DB_IS_NULL_NARGS (1, format_str))
    {
      *format = DT_INVALID;
      goto done;
    }

  /* arg cast type **********************************************************
   */
  format_str =
    db_value_cast_arg (format_str, &tmp_format_str, DB_TYPE_VARCHAR,
		       &error_status);

  if (error_status != NO_ERROR)
    {
      assert (er_errid () != NO_ERROR);
      goto exit_on_error;
    }

  /* start main body ********************************************************
   */

  if (DB_GET_STRING_SIZE (format_str) == 0)
    {
      error_status = ER_QSTR_EMPTY_STRING;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  if (DB_GET_STRING_SIZE (format_str) > MAX_TOKEN_SIZE)
    {
      error_status = ER_QSTR_SRC_TOO_LONG;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  error_status =
    db_check_or_create_null_term_string (format_str, stack_buf_format,
					 sizeof (stack_buf_format),
					 true, true,
					 &initial_buf_format,
					 &do_free_buf_format);
  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

  fmt_str_ptr = initial_buf_format;
  last_fmt = fmt_str_ptr + strlen (fmt_str_ptr);
  /* Skip space, tab, CR     */
  while (fmt_str_ptr < last_fmt && strchr (WHITE_CHARS, *fmt_str_ptr))
    {
      fmt_str_ptr++;
    }

  next_fmt_str_ptr = NULL;
  *format =
    get_next_format (fmt_str_ptr, DB_TYPE_DATETIME,
		     &format_size, &next_fmt_str_ptr);

  if (*format == DT_INVALID
      || (next_fmt_str_ptr != NULL && *next_fmt_str_ptr != 0))
    {
      error_status = ER_QSTR_INVALID_FORMAT;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_status, 0);
      goto exit_on_error;
    }

  if (error_status != NO_ERROR)
    {
      goto exit_on_error;
    }

done:
  assert (error_status == NO_ERROR);
  assert (*format != DT_INVALID);

  if (do_free_buf_format)
    {
      free_and_init (initial_buf_format);
    }

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_format_str);

  return error_status;

exit_on_error:
  assert (error_status != NO_ERROR);

  if (er_errid () == NO_ERROR)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
    }

  *format = DT_INVALID;

  if (do_free_buf_format)
    {
      free_and_init (initial_buf_format);
    }

  /* arg clear tmp **********************************************************
   */
  db_value_clear_nargs (1, &tmp_format_str);

  return error_status;
}
#endif
