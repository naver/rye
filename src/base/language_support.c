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
 * language_support.c : Multi-language and character set support
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <langinfo.h>

#include "chartype.h"
#include "misc_string.h"
#include "language_support.h"
#include "authenticate.h"
#include "environment_variable.h"
#include "db.h"
#include "memory_hash.h"
#include "util_func.h"
#include <dlfcn.h>

/* this must be the last header file included! */
#include "dbval.h"

static INTL_LANG lang_Lang_id = INTL_LANG_ENGLISH;
static INTL_CODESET lang_Loc_charset = INTL_CODESET_UTF8;
static char lang_Loc_name[LANG_MAX_LANGNAME] = LANG_NAME_DEFAULT;
static char lang_Lang_name[LANG_MAX_LANGNAME] = LANG_NAME_DEFAULT;

/* locale data */
static LANG_LOCALE_DATA lc_English_utf8;
static LANG_LOCALE_DATA lc_Korean_utf8;
static LANG_LOCALE_DATA *lang_Loc_data = &lc_English_utf8;

static bool lang_builtin_initialized = false;
static bool lang_Initialized = false;
static bool lang_Init_w_error = false;
static bool lang_Charset_initialized = false;
static bool lang_Language_initialized = false;

typedef struct lang_defaults LANG_DEFAULTS;
struct lang_defaults
{
  const char *lang_name;
  const INTL_LANG lang;
  const INTL_CODESET codeset;
};

/* Order of language/charset pair is important: first encountered charset is
 * the default for a language */
LANG_DEFAULTS builtin_langs[] = {
  /* English - UTF-8 */
  {LANG_NAME_ENGLISH, INTL_LANG_ENGLISH, INTL_CODESET_UTF8},
  /* Korean - UTF-8 */
  {LANG_NAME_KOREAN, INTL_LANG_KOREAN, INTL_CODESET_UTF8}
};

static TEXT_CONVERSION *console_conv = NULL;
extern TEXT_CONVERSION con_iso_8859_1_conv;
extern TEXT_CONVERSION con_iso_8859_9_conv;

/* all loaded locales */
static LANG_LOCALE_DATA *lang_loaded_locales[LANG_MAX_LOADED_LOCALES] =
  { NULL };

static int lang_count_locales = 0;

/* all loaded collations */
static LANG_COLLATION *lang_collations[LANG_MAX_COLLATIONS] = { NULL };

static int lang_count_collations = 0;

#if defined(ENABLE_UNUSED_FUNCTION)
/* normalization data */
static UNICODE_NORMALIZATION *generic_unicode_norm = NULL;
#endif

static const DB_CHARSET lang_Db_charsets[] = {
#if 1
  {"utf-8", "UNICODE charset - UTF-8 encoding", " ", "_utf8",
   INTL_CODESET_UTF8, 1},
  {"", "", "", "", INTL_CODESET_NONE, 0}
#else
  {"utf-8", "UNICODE charset - UTF-8 encoding", " ", "_utf8",
   "utf8", INTL_CODESET_UTF8, 1},
  {"", "", "", "", "", INTL_CODESET_NONE, 0}
#endif
};

static void lang_init_builtin (void);
static int set_current_locale (void);
static void set_default_lang (void);
static void lang_free_locales (void);
static LANG_LOCALE_DATA *find_lang_locale_data (const char *name,
						const INTL_CODESET codeset,
						LANG_LOCALE_DATA **
						last_lang_locale);
static int register_lang_locale_data (LANG_LOCALE_DATA * lld);
static void free_lang_locale_data (LANG_LOCALE_DATA * lld);
static int register_collation (LANG_COLLATION * coll);

#if defined (ENABLE_UNUSED_FUNCTION)
static bool lang_is_codeset_allowed (const INTL_LANG intl_id,
				     const INTL_CODESET codeset);
static int lang_get_builtin_lang_id_from_name (const char *lang_name,
					       INTL_LANG * lang_id);
static INTL_CODESET lang_get_default_codeset (const INTL_LANG intl_id);
#endif

static int lang_fastcmp_byte (const LANG_COLLATION * lang_coll,
			      const unsigned char *string1,
			      const int size1,
			      const unsigned char *string2, const int size2);
static int lang_strcmp_utf8 (const LANG_COLLATION * lang_coll,
			     const unsigned char *str1, const int size1,
			     const unsigned char *str2, const int size2);
static int lang_strmatch_utf8 (const LANG_COLLATION * lang_coll,
			       bool is_match,
			       const unsigned char *str1, int size1,
			       const unsigned char *str2, int size2,
			       const unsigned char *escape,
			       const bool has_last_escape,
			       int *str1_match_size);
static unsigned int lang_get_w_first_el (const COLL_DATA * coll,
					 const unsigned char *str,
					 const int str_size,
					 unsigned char **next_char);
static COLL_CONTRACTION *lang_get_contr_for_string (const COLL_DATA *
						    coll_data,
						    const unsigned char *str,
						    const int str_size,
						    unsigned int cp);
static int lang_str_utf8_trail_zero_weights (const LANG_COLLATION * lang_coll,
					     const unsigned char *str,
					     int size);
static int lang_next_coll_char_utf8 (const LANG_COLLATION * lang_coll,
				     const unsigned char *seq, const int size,
				     unsigned char *next_seq, int *len_next);
static int lang_split_key_utf8 (const LANG_COLLATION * lang_coll,
				const bool is_desc,
				const unsigned char *str1, const int size1,
				const unsigned char *str2, const int size2,
				unsigned char **key, int *byte_size);
static unsigned int lang_mht2str_byte (const LANG_COLLATION * lang_coll,
				       const unsigned char *str,
				       const int size);
static unsigned int lang_mht2str_utf8 (const LANG_COLLATION * lang_coll,
				       const unsigned char *str,
				       const int size);
static void lang_init_coll_en_ci (LANG_COLLATION * lang_coll);
static void lang_init_coll_utf8_en_cs (LANG_COLLATION * lang_coll);
static void lang_free_collations (void);

/* built-in collations */
/* number of characters in the (extended) alphabet per language */
#define LANG_CHAR_COUNT_EN 256
#define LANG_CHAR_COUNT_TR 352

#define LANG_COLL_GENERIC_SORT_OPT \
  {TAILOR_UNDEFINED, false, false, 1, false, CONTR_IGNORE, false, \
   MATCH_CONTR_BOUND_ALLOW}
#define LANG_COLL_NO_EXP 0, NULL, NULL, NULL
#define LANG_COLL_NO_CONTR NULL, 0, 0, NULL, 0, 0

#if defined(ENABLE_UNUSED_FUNCTION)
#define LANG_NO_NORMALIZATION {NULL, 0, NULL, NULL, 0}
#endif

static unsigned int lang_weight_EN_cs[LANG_CHAR_COUNT_EN];
static unsigned int lang_next_alpha_char_EN_cs[LANG_CHAR_COUNT_EN];

static unsigned int lang_weight_EN_ci[LANG_CHAR_COUNT_EN];
static unsigned int lang_next_alpha_char_EN_ci[LANG_CHAR_COUNT_EN];

#define DEFAULT_COLL_OPTIONS {true, true}
#define CI_COLL_OPTIONS {false, false}

static LANG_COLLATION coll_utf8_binary = {
  INTL_CODESET_UTF8, 1, 0, DEFAULT_COLL_OPTIONS, NULL,
  /* collation data */
  {LANG_COLL_UTF8_BINARY, "utf8_bin",
   LANG_COLL_GENERIC_SORT_OPT,
   lang_weight_EN_cs, lang_next_alpha_char_EN_cs, LANG_CHAR_COUNT_EN,
   LANG_COLL_NO_EXP,
   LANG_COLL_NO_CONTR,
   "d16a9a3825e263f76028c1e8c3cd043d"},
  /* compare functions handles bytes, no need to handle UTF-8 chars */
  lang_fastcmp_byte,
  lang_strmatch_utf8,
  /* 'next' and 'split_point' functions must handle UTF-8 chars */
  lang_next_coll_char_utf8,
  lang_split_key_utf8,
  lang_mht2str_byte,
  NULL
};

static LANG_COLLATION coll_utf8_en_cs = {
  INTL_CODESET_UTF8, 1, 1, DEFAULT_COLL_OPTIONS, NULL,
  /* collation data */
  {LANG_COLL_UTF8_EN_CS, "utf8_en_cs",
   LANG_COLL_GENERIC_SORT_OPT,
   lang_weight_EN_cs, lang_next_alpha_char_EN_cs, LANG_CHAR_COUNT_EN,
   LANG_COLL_NO_EXP,
   LANG_COLL_NO_CONTR,
   "1bdb1b1f630edc508be37f66dfdce7b0"},
  lang_fastcmp_byte,
  lang_strmatch_utf8,
  lang_next_coll_char_utf8,
  lang_split_key_utf8,
  lang_mht2str_byte,
  lang_init_coll_utf8_en_cs
};

static LANG_COLLATION coll_utf8_en_ci = {
  INTL_CODESET_UTF8, 1, 1, DEFAULT_COLL_OPTIONS, NULL,
  /* collation data */
  {LANG_COLL_UTF8_EN_CI, "utf8_en_ci",
   LANG_COLL_GENERIC_SORT_OPT,
   lang_weight_EN_ci, lang_next_alpha_char_EN_ci, LANG_CHAR_COUNT_EN,
   LANG_COLL_NO_EXP,
   LANG_COLL_NO_CONTR,
   "3050bc8e9814b196f4bbb84759aab77c"},
  lang_fastcmp_byte,
  lang_strmatch_utf8,
  lang_next_coll_char_utf8,
  lang_split_key_utf8,
  lang_mht2str_byte,
  lang_init_coll_en_ci
};

static LANG_COLLATION coll_utf8_ko_cs = {
  INTL_CODESET_UTF8, 1, 1, DEFAULT_COLL_OPTIONS, NULL,
  /* collation data - same as en_US.utf8 */
  {LANG_COLL_UTF8_KO_CS, "utf8_ko_cs",
   LANG_COLL_GENERIC_SORT_OPT,
   lang_weight_EN_cs, lang_next_alpha_char_EN_cs, LANG_CHAR_COUNT_EN,
   LANG_COLL_NO_EXP,
   LANG_COLL_NO_CONTR,
   "422c85ede1e265a761078763d2240c81"},
  lang_strcmp_utf8,
  lang_strmatch_utf8,
  lang_next_coll_char_utf8,
  lang_split_key_utf8,
  lang_mht2str_utf8,
  lang_init_coll_utf8_en_cs
};

static LANG_COLLATION *built_in_collations[] = {
  &coll_utf8_binary,
  &coll_utf8_en_cs,
  &coll_utf8_en_ci,
  &coll_utf8_ko_cs,
};

/*
 * lang_init_builtin - Initializes the built-in available languages and sets
 *		       message catalog language according to env
 *
 *   return: error code
 *
 */
static void
lang_init_builtin (void)
{
  int i;

  if (lang_builtin_initialized)
    {
      return;
    }

  /* init all collation placeholders with ISO binary collation */
  for (i = 0; i < LANG_MAX_COLLATIONS; i++)
    {
      lang_collations[i] = &coll_utf8_en_ci;
    }

  /* built-in collations : order of registration should match colation ID */
  for (i = 0; i < (int) (sizeof (built_in_collations)
			 / sizeof (built_in_collations[0])); i++)
    {
      (void) register_collation (built_in_collations[i]);
    }

  /* register all built-in locales allowed in current charset
   * Support for multiple locales is required for switching function context
   * string - data/time , string - number conversions */

  /* built-in locales with UTF-8 codeset : should be loaded last */
  (void) register_lang_locale_data (&lc_English_utf8);
  (void) register_lang_locale_data (&lc_Korean_utf8);

  lang_builtin_initialized = true;
}

/*
 * lang_init - Initializes the multi-language module
 *
 *   return: error code
 *
 *  Note : Initializes available built-in and LDML locales.
 *	   System charset and language information is not available and is not
 *	   set here.
 */
int
lang_init (void)
{
  int error = NO_ERROR;

  if (lang_Initialized)
    {
      return (lang_Init_w_error) ? ER_LOC_INIT : NO_ERROR;
    }

  lang_init_builtin ();

  assert (!lang_Charset_initialized);
  assert (!lang_Language_initialized);

  lang_Initialized = true;

  return error;
}

/*
 * lang_init_console_txt_conv - Initializes console text conversion
 *
 */
void
lang_init_console_txt_conv (void)
{
  char *sys_id = NULL;
  char *conv_sys_ids = NULL;

  assert (lang_Initialized);
  assert (lang_Loc_data != NULL);

  if (lang_Loc_data == NULL || lang_Loc_data->txt_conv == NULL)
    {
      (void) setlocale (LC_CTYPE, "");
      return;
    }

  /* setlocale with empty string forces the current locale :
   * this is required to retrieve codepage id, but as a side-effect modifies
   * the behavior of string utility functions such as 'snprintf' to support
   * current locale charset */
  if (setlocale (LC_CTYPE, "") != NULL)
    {
      sys_id = nl_langinfo (CODESET);
      conv_sys_ids = lang_Loc_data->txt_conv->nl_lang_str;
    }

  if (sys_id != NULL && conv_sys_ids != NULL)
    {
      char *conv_sys_end = conv_sys_ids + strlen (conv_sys_ids);
      char *found_token;

      /* supported system identifiers for conversion are separated by
       * comma */
      do
	{
	  found_token = strstr (conv_sys_ids, sys_id);
	  if (found_token == NULL)
	    {
	      break;
	    }

	  if (found_token + strlen (sys_id) >= conv_sys_end
	      || *(found_token + strlen (sys_id)) == ','
	      || *(found_token + strlen (sys_id)) == ' ')
	    {
	      if (lang_Loc_data->txt_conv->init_conv_func != NULL)
		{
		  lang_Loc_data->txt_conv->init_conv_func ();
		}
	      console_conv = lang_Loc_data->txt_conv;
	      break;
	    }
	  else
	    {
	      conv_sys_ids = conv_sys_ids + strlen (sys_id);
	    }
	}
      while (conv_sys_ids < conv_sys_end);
    }
}

/*
 * set_current_locale - Initializes current locale from global variables
 *			'lang_Lang_name' and 'lang_Loc_charset';
 *			if these are invalid current locale is intialized
 *			with default locale (en_US.iso88591), and error is
 *			returned.
 *
 *  return : error code
 */
static int
set_current_locale (void)
{
  bool found = false;

  assert (lang_Loc_charset == INTL_CODESET_UTF8);
  assert (strcmp (lang_get_codeset_name (lang_Loc_charset), "utf8") == 0);

  lang_get_lang_id_from_name (lang_Lang_name, &lang_Lang_id);

  for (lang_Loc_data = lang_loaded_locales[lang_Lang_id];
       lang_Loc_data != NULL; lang_Loc_data = lang_Loc_data->next_lld)
    {
      assert (lang_Loc_data != NULL);

      if (lang_Loc_data->codeset == lang_Loc_charset
	  && strcasecmp (lang_Lang_name, lang_Loc_data->lang_name) == 0)
	{
	  found = true;
	  break;
	}
    }

  if (!found)
    {
      char err_msg[ER_MSG_SIZE];

#if 1				/* TODO - */
      assert (false);
#endif

      lang_Init_w_error = true;
      snprintf (err_msg, sizeof (err_msg) - 1,
		"Locale %s.%s was not loaded.\n"
		" %s not found in rye_locales.txt", lang_Lang_name,
		lang_get_codeset_name (lang_Loc_charset), lang_Lang_name);
      LOG_LOCALE_ERROR (err_msg, ER_LOC_INIT, false);
      set_default_lang ();
    }

  /* at this point we have locale : either the user selected or default one */
  assert (lang_Loc_data != NULL);

  /* static globals in db_date.c should also be initialized with the current
   * locale (for parsing local am/pm strings for times) */
  db_date_locale_init ();

  return lang_Init_w_error ? ER_LOC_INIT : NO_ERROR;
}

/*
 * lang_set_charset_lang - Initializes language and charset from a locale
 *			   string
 *
 *   return: NO_ERROR if success
 *
 *  Note : This function sets the following global variables according to
 *	   input:
 *	    - lang_Loc_name : resolved locale string: <lang>.<charset>
 *	    - lang_Lang_name : <lang> string part (without <charset>)
 *	    - lang_Lang_id: id of language
 *	    - lang_Loc_charset : charset id : ISO-8859-1, UTF-8 or EUC-KR
 *	    - lang_Loc_data: pointer to locale (struct) used by sistem
 */
int
lang_set_charset_lang (void)
{
  int status = NO_ERROR;

  assert (lang_Initialized);
  assert (!lang_Init_w_error);

  lang_Charset_initialized = true;
  lang_Language_initialized = true;

  lang_Loc_charset = INTL_CODESET_UTF8;

  status = set_current_locale ();
  tp_apply_sys_charset ();

  return status;
}

/*
 * set_default_lang -
 *   return:
 *
 */
static void
set_default_lang (void)
{
  assert (false);

  lang_Lang_id = INTL_LANG_ENGLISH;
  strncpy (lang_Loc_name, LANG_NAME_DEFAULT, sizeof (lang_Loc_name));
  strncpy (lang_Lang_name, LANG_NAME_DEFAULT, sizeof (lang_Lang_name));
  lang_Loc_data = &lc_English_utf8;
  lang_Loc_charset = lang_Loc_data->codeset;
}

/*
 * lang_locales_count -
 *   return: number of locales in the system
 */
int
lang_locales_count (bool check_codeset)
{
  int i;
  int count;

  if (!check_codeset)
    {
      return lang_count_locales;
    }

  count = 0;
  for (i = 0; i < lang_count_locales; i++)
    {
      LANG_LOCALE_DATA *lld = lang_loaded_locales[i];
      do
	{
	  count++;
	  lld = lld->next_lld;
	}
      while (lld != NULL);
    }

  return count;
}


/*
 * register_collation - registers a collation
 *   return: error code
 *   coll(in): collation structure
 */
static int
register_collation (LANG_COLLATION * coll)
{
  int id;
  assert (coll != NULL);
  assert (lang_count_collations < LANG_MAX_COLLATIONS);

  id = coll->coll.coll_id;

  if (id < ((coll->built_in) ? 0 : LANG_MAX_BUILTIN_COLLATIONS)
      || id >= LANG_MAX_COLLATIONS)
    {
      char err_msg[ER_MSG_SIZE];
      snprintf (err_msg, sizeof (err_msg) - 1,
		"Invalid collation numeric identifier : %d"
		" for collation '%s'. Expecting greater than %d and lower "
		"than %d.", id, coll->coll.coll_name,
		((coll->built_in) ? 0 : LANG_MAX_BUILTIN_COLLATIONS),
		LANG_MAX_COLLATIONS);
      LOG_LOCALE_ERROR (err_msg, ER_LOC_INIT, false);
      return ER_LOC_INIT;
    }

  assert (lang_collations[id] != NULL);

  if (lang_collations[id]->coll.coll_id != LANG_COLL_UTF8_EN_CI)
    {
      char err_msg[ER_MSG_SIZE];
      snprintf (err_msg, sizeof (err_msg) - 1,
		"Invalid collation numeric identifier : %d for collation '%s'"
		". This id is already used by collation '%s'",
		id, coll->coll.coll_name,
		lang_collations[id]->coll.coll_name);
      LOG_LOCALE_ERROR (err_msg, ER_LOC_INIT, false);
      return ER_LOC_INIT;
    }

  lang_collations[id] = coll;

  lang_count_collations++;

  if (coll->init_coll != NULL)
    {
      coll->init_coll (coll);
    }

  return NO_ERROR;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * lang_is_coll_name_allowed - checks if collation name is allowed
 *   return: true if allowed
 *   name(in): collation name
 */
bool
lang_is_coll_name_allowed (const char *name)
{
  int i;

  if (name == NULL || *name == '\0')
    {
      return false;
    }

  if (strchr (name, (int) ' ') || strchr (name, (int) '\t'))
    {
      return false;
    }

  for (i = 0; i < (int) (sizeof (built_in_collations)
			 / sizeof (built_in_collations[0])); i++)
    {
      if (strcasecmp (built_in_collations[i]->coll.coll_name, name) == 0)
	{
	  return false;
	}
    }

  return true;
}
#endif

/*
 * lang_get_collation - access a collation by id
 *   return: pointer to collation data or NULL
 *   coll_id(in): collation identifier
 */
LANG_COLLATION *
lang_get_collation (const int coll_id)
{
  assert (coll_id >= 0 && coll_id < LANG_MAX_COLLATIONS);

  return lang_collations[coll_id];
}


/*
 * lang_get_collation_name - return collation name
 *   return: collation name
 *   coll_id(in): collation identifier
 */
const char *
lang_get_collation_name (const int coll_id)
{
  if (coll_id < 0 || coll_id >= LANG_MAX_COLLATIONS)
    {
      return NULL;
    }

  return lang_collations[coll_id]->coll.coll_name;
}

/*
 * lang_get_collation_by_name - access a collation by name
 *   return: pointer to collation data or NULL
 *   coll_name(in): collation name
 */
LANG_COLLATION *
lang_get_collation_by_name (const char *coll_name)
{
  int i;
  assert (coll_name != NULL);

  /* check iff default value
   */
  if (intl_mbs_casecmp ("DEFAULT", coll_name) == 0)
    {
      return lang_get_collation (LANG_SYS_COLLATION);
    }

  for (i = 0; i < LANG_MAX_COLLATIONS; i++)
    {
      if (strcmp (coll_name, lang_collations[i]->coll.coll_name) == 0)
	{
	  return lang_collations[i];
	}
    }

  return NULL;
}

/*
 * lang_collation_count -
 *   return: number of collations in the system
 */
int
lang_collation_count (void)
{
  return lang_count_collations;
}

/*
 * lang_get_codeset_name - get charset string equivalent
 *   return: charset string or empty string
 *   codeset_id(in): charset/codeset id
 */
const char *
lang_get_codeset_name (int codeset_id)
{
  switch (codeset_id)
    {
    case INTL_CODESET_UTF8:
      return "utf8";
    }

  /* codeset_id is propagated downwards from the grammar, so it is either
   * INTL_CODESET_UTF8, INTL_CODESET_KSC5601_EUC or INTL_CODESET_ISO88591 */
  assert (false);

  return "";
}

/*
 * lang_user_alphabet_w_coll -
 *   return: id of default collation
 */
const ALPHABET_DATA *
lang_user_alphabet_w_coll (const int collation_id)
{
  LANG_COLLATION *lang_coll;

  lang_coll = lang_get_collation (collation_id);

  assert (lang_coll->default_lang != NULL);

  return &(lang_coll->default_lang->alphabet);
}

/*
 * find_lang_locale_data - searches a locale with a given name and codeset
 *   return: locale or NULL if the name+codeset combination was not found
 *   name(in): name of locale
 *   codeset(in): codeset to search
 *   last_locale(out): last locale whith this name or NULL if no locale was
 *		       found
 */
static LANG_LOCALE_DATA *
find_lang_locale_data (const char *name, const INTL_CODESET codeset,
		       LANG_LOCALE_DATA ** last_lang_locale)
{
  LANG_LOCALE_DATA *first_lang_locale = NULL;
  LANG_LOCALE_DATA *curr_lang_locale;
  LANG_LOCALE_DATA *found_lang_locale = NULL;
  int i;

  assert (last_lang_locale != NULL);

  for (i = 0; i < lang_count_locales; i++)
    {
      if (strcasecmp (lang_loaded_locales[i]->lang_name, name) == 0)
	{
	  first_lang_locale = lang_loaded_locales[i];
	  break;
	}
    }

  for (curr_lang_locale = first_lang_locale; curr_lang_locale != NULL;
       curr_lang_locale = curr_lang_locale->next_lld)
    {
      if (codeset == curr_lang_locale->codeset)
	{
	  found_lang_locale = curr_lang_locale;
	}

      if (curr_lang_locale->next_lld == NULL)
	{
	  *last_lang_locale = curr_lang_locale;
	  break;
	}
    }

  return found_lang_locale;
}

/*
 * register_lang_locale_data - registers a language locale data in the system
 *   return: error status
 *   lld(in): language locale data
 */
static int
register_lang_locale_data (LANG_LOCALE_DATA * lld)
{
  LANG_LOCALE_DATA *last_lang_locale = NULL;
  LANG_LOCALE_DATA *found_lang_locale = NULL;

  assert (lld != NULL);

  found_lang_locale = find_lang_locale_data (lld->lang_name, lld->codeset,
					     &last_lang_locale);

  assert (found_lang_locale == NULL);

  if (!lld->is_user_data)
    {
      /* make a copy of built-in */
      LANG_LOCALE_DATA *new_lld =
	(LANG_LOCALE_DATA *) malloc (sizeof (LANG_LOCALE_DATA));
      if (new_lld == NULL)
	{
	  LOG_LOCALE_ERROR ("memory allocation failed", ER_LOC_INIT, false);
	  return ER_LOC_INIT;
	}

      memcpy (new_lld, lld, sizeof (LANG_LOCALE_DATA));
      lld = new_lld;
    }

  if (last_lang_locale == NULL)
    {
      /* no other locales exists with the same name */
      assert (lang_count_locales < LANG_MAX_LOADED_LOCALES);
      lang_loaded_locales[lang_count_locales++] = lld;
    }
  else
    {
      last_lang_locale->next_lld = lld;
    }

  if (!(lld->is_initialized) && lld->initloc != NULL)
    {
      assert (lld->lang_id < (INTL_LANG) INTL_LANG_USER_DEF_START);
      init_builtin_calendar_names (lld);
      lld->initloc (lld);

      /* init default collation */
      if (lld->default_lang_coll != NULL
	  && lld->default_lang_coll->init_coll != NULL)
	{
	  lld->default_lang_coll->init_coll (lld->default_lang_coll);
	}
    }

  return NO_ERROR;
}

/*
 * free_lang_locale_data - Releases any resources held by a language locale
 *			   data
 *   return: none
 */
static void
free_lang_locale_data (LANG_LOCALE_DATA * lld)
{
  assert (lld != NULL);

  if (lld->next_lld != NULL)
    {
      free_lang_locale_data (lld->next_lld);
      lld->next_lld = NULL;
    }

  if (lld->is_user_data)
    {
      /* Text conversions having init_conv_func not NULL are built-in.
       * They can't be deallocated.
       */
      if (lld->txt_conv != NULL && lld->txt_conv->init_conv_func == NULL)
	{
	  free (lld->txt_conv);
	  lld->txt_conv = NULL;
	}
    }

  free (lld);
}

/*
 * lang_get_Lang_name - returns the language name according to environment
 *   return: language name string
 */
const char *
lang_get_Lang_name (void)
{
  if (!lang_Language_initialized)
    {
      assert (false);
      return NULL;
    }

  return lang_Lang_name;
}

/*
 * lang_id - Returns language id per env settings
 *   return: language identifier
 */
INTL_LANG
lang_id (void)
{
  if (!lang_Language_initialized)
    {
      assert (false);
      return -1;
    }

  assert (lang_Lang_id == INTL_LANG_ENGLISH);

  return lang_Lang_id;
}

/*
 * lang_charset - Returns language charset per env settings
 *   return: language charset
 */
INTL_CODESET
lang_charset (void)
{
  if (!lang_Charset_initialized)
    {
      assert (false);
      return INTL_CODESET_NONE;
    }
  return lang_Loc_charset;
}

/*
 * lang_final - Releases any resources held by this module
 *   return: none
 */
void
lang_final (void)
{
  lang_free_locales ();
  lang_free_collations ();

#if defined(ENABLE_UNUSED_FUNCTION)
  lang_set_generic_unicode_norm (NULL);
#endif

  lang_builtin_initialized = false;
  lang_Initialized = false;
  lang_Init_w_error = false;
  lang_Language_initialized = false;
  lang_Charset_initialized = false;
}

/*
 * lang_check_identifier - Tests an identifier for possibility
 *   return: true if the name is suitable for identifier,
 *           false otherwise.
 *   name(in): identifier name
 *   length(in): identifier name length
 */
bool
lang_check_identifier (const char *name, int length)
{
  bool ok = false;
  int i;

  if (name == NULL)
    {
      return false;
    }

  if (char_isalpha (name[0]))
    {
      ok = true;
      for (i = 0; i < length && ok; i++)
	{
	  if (!char_isalnum (name[i]) && name[i] != '_')
	    {
	      ok = false;
	    }
	}
    }

  return (ok);
}

bool
lang_check_initialized (void)
{
  return lang_Initialized;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * lang_locale - returns language locale per env settings.
 *   return: language locale data
 */
const LANG_LOCALE_DATA *
lang_locale (void)
{
  if (!lang_Charset_initialized || !lang_Language_initialized)
    {
      assert (false);
      return NULL;
    }
  return lang_Loc_data;
}
#endif

/*
 * lang_get_specific_locale - returns language locale of a specific language
 *			      and codeset
 *
 *  return: language locale data
 *  lang(in):
 *
 *  Note : if codeset is INTL_CODESET_NONE, returns the first locale it
 *	   founds with requested language id, not matter the codeset.
 */
const LANG_LOCALE_DATA *
lang_get_specific_locale (const INTL_LANG lang)
{
  if (!lang_Charset_initialized || !lang_Language_initialized)
    {
      assert (false);
      return NULL;
    }

  if ((int) lang < lang_count_locales)
    {
      LANG_LOCALE_DATA *first_lang_locale = lang_loaded_locales[lang];
      LANG_LOCALE_DATA *curr_lang_locale;

      for (curr_lang_locale = first_lang_locale; curr_lang_locale != NULL;
	   curr_lang_locale = curr_lang_locale->next_lld)
	{
	  if (curr_lang_locale->codeset == INTL_CODESET_UTF8)
	    {
	      return curr_lang_locale;
	    }
	}
    }

  return NULL;
}


/*
 * lang_get_first_locale_for_lang - returns first locale for language
 *  return: language locale data or NULL if language id is not valid
 *  lang(in):
 */
const LANG_LOCALE_DATA *
lang_get_first_locale_for_lang (const INTL_LANG lang)
{
  if (!lang_Charset_initialized || !lang_Language_initialized)
    {
      assert (false);
      return NULL;
    }

  if ((int) lang < lang_count_locales)
    {
      return lang_loaded_locales[lang];
    }

  return NULL;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * lang_get_builtin_lang_id_from_name - returns the builtin language id from a
 *					language name
 *
 *   return: 0, if language name is accepted, non-zero otherwise
 *   lang_name(in):
 *   lang_id(out): language identifier
 *
 *  Note : INTL_LANG_ENGLISH is returned if name is not a valid language name
 */
static int
lang_get_builtin_lang_id_from_name (const char *lang_name,
				    INTL_LANG * lang_id)
{
  int i;

  assert (lang_id != NULL);

  *lang_id = INTL_LANG_ENGLISH;

  for (i = 0; i < (int) (sizeof (builtin_langs) / sizeof (LANG_DEFAULTS));
       i++)
    {
      if (strncasecmp (lang_name, builtin_langs[i].lang_name,
		       strlen (builtin_langs[i].lang_name)) == 0)
	{
	  *lang_id = builtin_langs[i].lang;
	  return 0;
	}
    }

  assert (*lang_id < INTL_LANG_USER_DEF_START);

  return 1;
}
#endif

/*
 * lang_get_lang_id_from_name - returns the language id from a language name
 *
 *   return: 0, if language name is accepted, non-zero otherwise
 *   lang_name(in):
 *   lang_id(out): language identifier
 *
 *  Note : INTL_LANG_ENGLISH is returned if name is not a valid language name
 */
int
lang_get_lang_id_from_name (const char *lang_name, INTL_LANG * lang_id)
{
  int i;

  assert (lang_id != NULL);

  *lang_id = INTL_LANG_ENGLISH;

  for (i = 0; i < lang_count_locales; i++)
    {
      assert (lang_loaded_locales[i] != NULL);

      if (strcasecmp (lang_name, lang_loaded_locales[i]->lang_name) == 0)
	{
	  assert (i == (int) lang_loaded_locales[i]->lang_id);
	  *lang_id = lang_loaded_locales[i]->lang_id;
	  return 0;
	}
    }

  return 1;
}

/*
 * lang_get_lang_name_from_id - returns the language name from a language id
 *
 *   return: language name (NULL if lang_id is not valid)
 *   lang_id(in):
 *
 */
const char *
lang_get_lang_name_from_id (const INTL_LANG lang_id)
{
  if ((int) lang_id < lang_count_locales)
    {
      assert (lang_loaded_locales[lang_id] != NULL);
      return lang_loaded_locales[lang_id]->lang_name;
    }

  return NULL;
}

/*
 * lang_set_flag_from_lang - set a flag according to language string
 *
 *   return: 0 if language string OK and flag was set, non-zero otherwise
 *   lang_str(in): language string identier
 *   has_user_format(in): true if user has given a format, false otherwise
 *   has_user_lang(in): true if user has given a language, false otherwise
 *   flag(out): bit flag : bit 0 is the user flag, bits 1 - 31 are for
 *		language identification
 *		Bit 0 : if set, the language was given by user
 *		Bit 1 - 31 : INTL_LANG
 *
 *  Note : function is used in context of some date-string functions.
 *	   If lang_str cannot be solved, the language is assumed English.
 */
int
lang_set_flag_from_lang (const char *lang_str, bool has_user_format,
			 bool has_user_lang, int *flag)
{
  INTL_LANG lang = INTL_LANG_ENGLISH;
  int status = 0;

  if (lang_str != NULL)
    {
      status = lang_get_lang_id_from_name (lang_str, &lang);
      if (status != 0)
	{
	  assert (lang == INTL_LANG_ENGLISH);
	  return status;	/* error */
	}
    }

  status =
    lang_set_flag_from_lang_id (lang, has_user_format, has_user_lang, flag);
  if (status != 0)
    {
      return status;		/* error */
    }

  return 0;
}

/*
 * lang_set_flag_from_lang - set a flag according to language identifier
 *
 *   return: 0 if language string OK and flag was set, non-zero otherwise
 *   lang(in): language identier
 *   has_user_format(in): true if user has given a format, false otherwise
 *   has_user_lang(in): true if user has given a language, false otherwise
 *   flag(out): bit flag : bits 0 and 1 are user flags, bits 2 - 31 are for
 *		language identification
 *		Bit 0 : if set, the format was given by user
*		Bit 1 : if set, the language was given by user
 *		Bit 2 - 31 : INTL_LANG
 *		Consider change this flag to store the language as value
 *		instead of as bit map
 *
 *  Note : function is used in context of some date-string functions.
 */
int
lang_set_flag_from_lang_id (const INTL_LANG lang, bool has_user_format,
			    bool has_user_lang, int *flag)
{
  int lang_val = (int) lang;

  *flag = 0;

  *flag |= (has_user_format) ? 1 : 0;
  *flag |= (has_user_lang) ? 2 : 0;

  if (lang_val >= lang_count_locales)
    {
      lang_val = (int) INTL_LANG_ENGLISH;
      *flag |= lang_val << 2;
      return 1;
    }

  *flag |= lang_val << 2;

  return 0;
}

/*
 * lang_get_lang_id_from_flag - get lang id from flag
 *
 *   return: id of language, current language is returned when flag value is
 *	     invalid
 *   flag(in): bit flag : bit 0 and 1 are user flags, bits 2 - 31 are for
 *	       language identification
 *
 *  Note : function is used in context of some date-string functions.
 */
INTL_LANG
lang_get_lang_id_from_flag (const int flag, bool * has_user_format,
			    bool * has_user_lang)
{
  int lang_val;

  *has_user_format = ((flag & 0x1) == 0x1) ? true : false;
  *has_user_lang = ((flag & 0x2) == 0x2) ? true : false;

  lang_val = flag >> 2;

  if (lang_val >= 0 && lang_val < lang_count_locales)
    {
      return (INTL_LANG) lang_val;
    }

  return lang_id ();
}

/*
 * lang_date_format_parse - Returns the default format of parsing date for the
 *		      required language or NULL if a the default format is not
 *		      available
 *   lang_id (in):
 *   codeset (in):
 *   type (in): DB type for format
 *   format_codeset (in): codeset of the format found
 *
 * Note:  If a format for combination (lang_id, codeset) is not found, then
 *	  the first valid (non-NULL) format for lang_id and the codeset
 *	  are returned.
 *
 */
const char *
lang_date_format_parse (const INTL_LANG lang_id, const DB_TYPE type)
{
  const LANG_LOCALE_DATA *lld;
  const char *format = NULL;
  const char *first_valid_format = NULL;

  assert (lang_Charset_initialized && lang_Language_initialized);

  lld = lang_get_first_locale_for_lang (lang_id);

  if (lld == NULL)
    {
      return NULL;
    }

  do
    {
      switch (type)
	{
	case DB_TYPE_TIME:
	  format = lld->time_format;
	  break;
	case DB_TYPE_DATE:
	  format = lld->date_format;
	  break;
	case DB_TYPE_DATETIME:
	  format = lld->datetime_format;
	  break;
	default:
	  break;
	}

      if (lld->codeset == INTL_CODESET_UTF8)
	{
	  first_valid_format = format;
	  break;
	}

      if (first_valid_format == NULL)
	{
	  first_valid_format = format;
	}

      lld = lld->next_lld;
    }
  while (lld != NULL);

  return first_valid_format;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * lang_get_default_codeset - returns the default codeset to be used for a
 *			      given language identifier
 *   return: codeset
 *   intl_id(in):
 */
static INTL_CODESET
lang_get_default_codeset (const INTL_LANG intl_id)
{
  unsigned int i;
  INTL_CODESET codeset = INTL_CODESET_NONE;

  for (i = 0; i < sizeof (builtin_langs) / sizeof (LANG_DEFAULTS); i++)
    {
      if (intl_id == builtin_langs[i].lang)
	{
	  codeset = builtin_langs[i].codeset;
	  break;
	}
    }
  return codeset;
}

/*
 * lang_is_codeset_allowed - checks if a combination of language and codeset
 *			     is allowed
 *   return: true if combination is allowed, false otherwise
 *   intl_id(in):
 *   codeset(in):
 */
static bool
lang_is_codeset_allowed (const INTL_LANG intl_id, const INTL_CODESET codeset)
{
  unsigned int i;

  for (i = 0; i < sizeof (builtin_langs) / sizeof (LANG_DEFAULTS); i++)
    {
      if (intl_id == builtin_langs[i].lang &&
	  codeset == builtin_langs[i].codeset)
	{
	  return true;
	}
    }
  return false;
}
#endif

/*
 * lang_digit_grouping_symbol - Returns symbol used for grouping digits in
 *				numbers
 *   lang_id (in):
 */
char
lang_digit_grouping_symbol (const INTL_LANG lang_id)
{
  const LANG_LOCALE_DATA *lld = lang_get_specific_locale (lang_id);

  assert (lld != NULL);

  return lld->number_group_sym;
}

/*
 * lang_digit_fractional_symbol - Returns symbol used for fractional part of
 *				  numbers
 *   lang_id (in):
 */
char
lang_digit_fractional_symbol (const INTL_LANG lang_id)
{
  const LANG_LOCALE_DATA *lld = lang_get_specific_locale (lang_id);

  assert (lld != NULL);

  return lld->number_decimal_sym;
}

/*
 * lang_get_txt_conv - Returns the information required for console text
 *		       conversion
 */
TEXT_CONVERSION *
lang_get_txt_conv (void)
{
  return console_conv;
}

/*
 * lang_charset_name() - returns charset name
 *
 *   return:
 *   codeset(in):
 */
const char *
lang_charset_name (const INTL_CODESET codeset)
{
  int i;

  assert (codeset <= INTL_CODESET_UTF8);

  for (i = 0; lang_Db_charsets[i].charset_id != INTL_CODESET_NONE; i++)
    {
      if (codeset == lang_Db_charsets[i].charset_id)
	{
	  return lang_Db_charsets[i].charset_name;
	}
    }

  return NULL;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * lang_charset_rye_name() - returns charset name
 *
 *   return:
 *   codeset(in):
 */
const char *
lang_charset_rye_name (const INTL_CODESET codeset)
{
  int i;

  assert (codeset <= INTL_CODESET_UTF8);

  for (i = 0; lang_Db_charsets[i].charset_id != INTL_CODESET_NONE; i++)
    {
      if (codeset == lang_Db_charsets[i].charset_id)
	{
	  return lang_Db_charsets[i].charset_rye_name;
	}
    }

  return NULL;
}

/*
 * lang_get_charset_env_string -
 * buf(out):
 * buf_size(in):
 * lang_name(in):
 * codeset(in):
 * return:
 */
int
lang_get_charset_env_string (char *buf, int buf_size, const char *lang_name,
			     const INTL_CODESET codeset)
{
  if (buf == NULL)
    {
      assert_release (0);
      return ER_FAILED;
    }

  if (!strcasecmp (lang_name, "en_US"))
    {
      snprintf (buf, buf_size, "%s", lang_name);
    }
  else
    {
      snprintf (buf, buf_size, "%s.%s", lang_name,
		lang_charset_rye_name (codeset));
    }

  return NO_ERROR;
}
#endif

#if !defined (SERVER_MODE)
/* client side charset and collation */
static bool lang_Parser_use_client_charset = true;

/*
 * lang_db_put_charset - Saves the charset and language information into DB
 *   return: error code
 *
 * Note: This is called during database creation; charset and language are
 *	 initialized with DB creation parameters.
 */
int
lang_db_put_charset (void)
{
  INTL_CODESET server_codeset;
  INTL_LANG server_lang;
  DB_VALUE value;
  int au_save;

  server_codeset = lang_charset ();

  server_lang = lang_id ();

  AU_DISABLE (au_save);
  db_make_string (&value, lang_get_lang_name_from_id (server_lang));
  if (db_put_internal (Au_root, "lang", &value) != NO_ERROR)
    {
      /* Error Setting the language */
      assert (false);
    }

  db_make_int (&value, (int) server_codeset);
  if (db_put_internal (Au_root, "charset", &value) != NO_ERROR)
    {
      /* Error Setting the nchar codeset */
      assert (false);
    }
  AU_ENABLE (au_save);

  return NO_ERROR;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * lang_charset_name_to_id - Returns the INTL_CODESET of the specified charset
 *   return: NO_ERROR or error code if the specified name can't be found in
 *           the lang_Db_charsets array
 *   name(in): the name of the desired charset
 *   codeset(out): INTL_CODESET of the desired charset
 */
int
lang_charset_name_to_id (const char *name, INTL_CODESET * codeset)
{
  int i;

  /* Find the charset in the lang_Db_charsets array */
  for (i = 0; lang_Db_charsets[i].charset_id != INTL_CODESET_NONE; i++)
    {
      if (strcmp (lang_Db_charsets[i].charset_name, name) == 0)
	{
	  *codeset = lang_Db_charsets[i].charset_id;
	  return NO_ERROR;
	}
    }

  return ER_FAILED;
}
#endif

/*
 * lang_get_client_charset - Gets Client's charset
 *   return: codeset
 */
INTL_CODESET
lang_get_client_charset (void)
{
  return LANG_SYS_CODESET;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * lang_get_client_collation - Gets Client's charset
 *   return: codeset
 */
int
lang_get_client_collation (void)
{
  return LANG_SYS_COLLATION;
}
#endif

/*
 * lang_set_parser_use_client_charset - set if next parsing operation should
 *				        use client's setting of charset and
 *					collation
 */
void
lang_set_parser_use_client_charset (bool use)
{
  lang_Parser_use_client_charset = use;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * lang_get_parser_use_client_charset - checks if parser should use client's
 *					charset and collation
 *   return:
 */
bool
lang_get_parser_use_client_charset (void)
{
  return lang_Parser_use_client_charset;
}
#endif
#endif /* !SERVER_MODE */

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * lang_charset_rye_name_to_id - Returns the INTL_CODESET of the charset
 *				    with Rye name
 *   return: codeset id, INTL_CODESET_NONE if not found
 *   name(in): the name of the desired charset
 */
INTL_CODESET
lang_charset_rye_name_to_id (const char *name)
{
  if (strcasecmp (name, "utf8") == 0)
    {
      return INTL_CODESET_UTF8;
    }

  return INTL_CODESET_NONE;
}

/*
 * lang_charset_introducer() - returns introducer text to print for a charset
 *
 *   return: charset introducer or NULL if not found
 *   codeset(in):
 */
const char *
lang_charset_introducer (const INTL_CODESET codeset)
{
  int i;

  assert (codeset <= INTL_CODESET_UTF8);

  for (i = 0; lang_Db_charsets[i].charset_id != INTL_CODESET_NONE; i++)
    {
      if (codeset == lang_Db_charsets[i].charset_id)
	{
	  return lang_Db_charsets[i].introducer;
	}
    }

  return NULL;
}
#endif


/* Collation functions */

/*
 * lang_strcmp_utf8() - string compare for UTF8
 *   return:
 *   lang_coll(in) : collation data
 *   string1(in):
 *   size1(in):
 *   string2(in):
 *   size2(in):
 */
static int
lang_strcmp_utf8 (const LANG_COLLATION * lang_coll,
		  const unsigned char *str1, const int size1,
		  const unsigned char *str2, const int size2)
{
  return lang_strmatch_utf8 (lang_coll, false, str1, size1, str2, size2,
			     NULL, false, NULL);
}

/*
 * lang_strmatch_utf8() - string match and compare for UTF8 collations
 *
 *   return: negative if str1 < str2, positive if str1 > str2, zero otherwise
 *   lang_coll(in) : collation data
 *   is_match(in) : true if match, otherwise is compare
 *   str1(in):
 *   size1(in):
 *   str2(in): this is the pattern string in case of match
 *   size2(in):
 *   escape(in): pointer to escape character (multi-byte allowed)
 *		 (used in context of LIKE)
 *   has_last_escape(in): true if it should check if last character is the
 *			  escape character
 *   str1_match_size(out): size from str1 which is matched with str2
 */
static int
lang_strmatch_utf8 (const LANG_COLLATION * lang_coll, bool is_match,
		    const unsigned char *str1, int size1,
		    const unsigned char *str2, int size2,
		    const unsigned char *escape, const bool has_last_escape,
		    int *str1_match_size)
{
  const unsigned char *str1_end;
  const unsigned char *str2_end;
  const unsigned char *str1_begin;
  unsigned char *str1_next, *str2_next;
  unsigned int cp1, cp2, w_cp1, w_cp2;
  const int alpha_cnt = lang_coll->coll.w_count;
  const unsigned int *weight_ptr = lang_coll->coll.weights;

  str1_begin = str1;
  str1_end = str1 + size1;
  str2_end = str2 + size2;

  for (; str1 < str1_end && str2 < str2_end;)
    {
      assert (str1_end - str1 > 0);
      assert (str2_end - str2 > 0);

      cp1 = intl_utf8_to_cp (str1, str1_end - str1, &str1_next);
      cp2 = intl_utf8_to_cp (str2, str2_end - str2, &str2_next);

      if (is_match && escape != NULL
	  && memcmp (str2, escape, str2_next - str2) == 0)
	{
	  if (!(has_last_escape && str2_next >= str2_end))
	    {
	      str2 = str2_next;
	      cp2 = intl_utf8_to_cp (str2, str2_end - str2, &str2_next);
	    }
	}

      if (cp1 < (unsigned int) alpha_cnt)
	{
	  w_cp1 = weight_ptr[cp1];
	}
      else
	{
	  w_cp1 = cp1;
	}

      if (cp2 < (unsigned int) alpha_cnt)
	{
	  w_cp2 = weight_ptr[cp2];
	}
      else
	{
	  w_cp2 = cp2;
	}

      if (w_cp1 != w_cp2)
	{
	  return (w_cp1 < w_cp2) ? (-1) : 1;
	}

      str1 = str1_next;
      str2 = str2_next;
    }

  size1 = str1_end - str1;
  size2 = str2_end - str2;

  assert (size1 == 0 || size2 == 0);

  if (is_match)
    {
      assert (str1_match_size != NULL);
      *str1_match_size = str1 - str1_begin;
    }

  if (size1 == size2)
    {
      return 0;
    }
  else if (size2 > 0)
    {
      if (is_match)
	{
	  return -1;
	}

      if (lang_str_utf8_trail_zero_weights (lang_coll, str2, str2_end - str2)
	  != 0)
	{
	  return -1;
	}
    }
  else
    {
      assert (size1 > 0);

      if (is_match)
	{
	  return 0;
	}

      if (lang_str_utf8_trail_zero_weights (lang_coll, str1, str1_end - str1)
	  != 0)
	{
	  return 1;
	}
    }

  return 0;
}

#define ADD_TO_HASH(pseudo_key, w)	    \
  do {					    \
    unsigned int i;			    \
    pseudo_key = (pseudo_key << 4) + w;	    \
    i = pseudo_key & 0xf0000000;	    \
    if (i != 0)				    \
      {					    \
	pseudo_key ^= i >> 24;		    \
	pseudo_key ^= i;		    \
      }					    \
  } while (0)

/*
 * lang_mht2str_utf8() - computes hash 2 style for a UTF-8 string having
 *			 collation without expansions
 *
 *   return: hash value
 *   lang_coll(in) : collation data
 *   str(in):
 *   size(in):
 */
static unsigned int
lang_mht2str_utf8 (const LANG_COLLATION * lang_coll,
		   const unsigned char *str, const int size)
{
  const unsigned char *str_end;
  unsigned char *str_next;
  unsigned int cp, w;
  const COLL_DATA *coll = &(lang_coll->coll);
  const int alpha_cnt = coll->w_count;
  const unsigned int *weight_ptr = lang_coll->coll.weights;
  unsigned int pseudo_key = 0;

  str_end = str + size;

  for (; str < str_end;)
    {
      assert (str_end - str > 0);

      cp = intl_utf8_to_cp (str, str_end - str, &str_next);

      if (cp < (unsigned int) alpha_cnt)
	{
	  COLL_CONTRACTION *contr = NULL;

	  if (coll->count_contr > 0
	      && str_end - str >= coll->contr_min_size
	      && cp >= coll->cp_first_contr_offset
	      && cp < (coll->cp_first_contr_offset +
		       coll->cp_first_contr_count)
	      && ((contr =
		   lang_get_contr_for_string (coll, str,
					      str_end - str, cp)) != NULL))
	    {
	      assert (contr != NULL);

	      w = contr->wv;
	      str_next = (unsigned char *) str + contr->size;
	    }
	  else
	    {
	      w = weight_ptr[cp];
	    }
	}
      else
	{
	  w = cp;
	}

      ADD_TO_HASH (pseudo_key, w);

      str = str_next;
    }

  return pseudo_key;
}

/*
 * lang_get_w_first_el() - get the weight of the first element (character or
 *			   contraction) encountered in the string
 *
 *   return: weight value
 *   coll_data(in): collation data
 *   str(in): buffer to check for contractions
 *   str_size(in): size of buffer (bytes)
 *   next_char(out): pointer to the end of element (next character)
 *
 *   Note : This function works only on UTF-8 collations without expansions.
 *
 */
static unsigned int
lang_get_w_first_el (const COLL_DATA * coll,
		     const unsigned char *str, const int str_size,
		     unsigned char **next_char)
{
  unsigned int cp, w;
  const int alpha_cnt = coll->w_count;
  const unsigned int *weight_ptr = coll->weights;

  assert (coll->uca_exp_num == 0);
  assert (str_size > 0);
  assert (next_char != NULL);

  cp = intl_utf8_to_cp (str, str_size, next_char);
  if (cp < (unsigned int) alpha_cnt)
    {
      COLL_CONTRACTION *contr = NULL;

      if (coll->count_contr > 0
	  && str_size >= coll->contr_min_size
	  && cp >= coll->cp_first_contr_offset
	  && cp < (coll->cp_first_contr_offset
		   + coll->cp_first_contr_count)
	  && ((contr = lang_get_contr_for_string (coll, str, str_size, cp))
	      != NULL))
	{
	  assert (contr != NULL);

	  w = contr->wv;
	  *next_char = (unsigned char *) str + contr->size;
	}
      else
	{
	  w = weight_ptr[cp];
	}
    }
  else
    {
      w = cp;
    }

  return w;
}

/*
 * lang_get_contr_for_string() - checks if the string starts with a
 *				 contraction
 *
 *   return: contraction pointer or NULL if no contraction is found
 *   coll_data(in): collation data
 *   str(in): buffer to check for contractions
 *   str_size(in): size of buffer (bytes)
 *   cp(in): codepoint of first character in 'str'
 *
 */
static COLL_CONTRACTION *
lang_get_contr_for_string (const COLL_DATA * coll_data,
			   const unsigned char *str, const int str_size,
			   unsigned int cp)
{
  const int *first_contr;
  int contr_id;
  COLL_CONTRACTION *contr;
  int cmp;

  assert (coll_data != NULL);
  assert (coll_data->count_contr > 0);

  assert (str != NULL);
  assert (str_size >= coll_data->contr_min_size);

  first_contr = coll_data->cp_first_contr_array;
  assert (first_contr != NULL);
  contr_id = first_contr[cp - coll_data->cp_first_contr_offset];

  if (contr_id == -1)
    {
      return NULL;
    }

  assert (contr_id >= 0 && contr_id < coll_data->count_contr);
  contr = &(coll_data->contr_list[contr_id]);

  do
    {
      if ((int) contr->size > str_size)
	{
	  cmp = memcmp (contr->c_buf, str, str_size);
	  if (cmp == 0)
	    {
	      cmp = 1;
	    }
	}
      else
	{
	  cmp = memcmp (contr->c_buf, str, contr->size);
	}

      if (cmp >= 0)
	{
	  break;
	}

      assert (cmp < 0);

      contr++;
      contr_id++;

    }
  while (contr_id < coll_data->count_contr);

  if (cmp != 0)
    {
      contr = NULL;
    }

  return contr;
}

/*
 * lang_str_utf8_trail_zero_weights() - checks if remaining characters of an
 *					UTF-8 string have all zero weights
 *
 *   return: 0 if all remaining characters have zero weight, 1 otherwise
 *   lang_coll(in): collation data
 *   str(in):
 *   size(in):
 */
static int
lang_str_utf8_trail_zero_weights (const LANG_COLLATION * lang_coll,
				  const unsigned char *str, int size)
{
  unsigned char *str_next;
  unsigned int cp;

  while (size > 0)
    {
      cp = intl_utf8_to_cp (str, size, &str_next);

      if (cp >= (unsigned int) lang_coll->coll.w_count
	  || lang_coll->coll.weights[cp] != 0)
	{
	  return 1;
	}
      size -= str_next - str;
      str = str_next;
    }

  return 0;
}

/*
 * lang_next_coll_char_utf8() - computes the next collatable char
 *   return: size in bytes of the next collatable char
 *   lang_coll(on): collation
 *   seq(in): pointer to current char
 *   size(in): available bytes for current char
 *   next_seq(in/out): buffer to return next alphabetical char
 *   len_next(in/out): length in chars of next char (always 1 for this func)
 *
 *  Note :  It is assumed that the input buffer (cur_char) contains at least
 *	    one UTF-8 character.
 *	    The calling function should take into account cases when 'next'
 *	    character is encoded on greater byte size.
 */
static int
lang_next_coll_char_utf8 (const LANG_COLLATION * lang_coll,
			  const unsigned char *seq, const int size,
			  unsigned char *next_seq, int *len_next)
{
  unsigned int cp_alpha_char, cp_next_alpha_char;
  const int alpha_cnt = lang_coll->coll.w_count;
  const unsigned int *next_alpha_char = lang_coll->coll.next_cp;
  unsigned char *dummy = NULL;

  assert (seq != NULL);
  assert (next_seq != NULL);
  assert (len_next != NULL);
  assert (size > 0);

  cp_alpha_char = intl_utf8_to_cp (seq, size, &dummy);

  if (cp_alpha_char < (unsigned int) alpha_cnt)
    {
      cp_next_alpha_char = next_alpha_char[cp_alpha_char];
    }
  else
    {
      cp_next_alpha_char = cp_alpha_char + 1;
    }

  *len_next = 1;

  return intl_cp_to_utf8 (cp_next_alpha_char, next_seq);
}

/*
 * lang_split_key_utf8() - finds the prefix key; UTF-8 collation with
 *			   contractions but without expansions
 *
 *   return:  error status
 *   lang_coll(in):
 *   is_desc(in):
 *   str1(in):
 *   size1(in):
 *   str2(in):
 *   size2(in):
 *   key(out): key
 *   byte_size(out): size in bytes of key
 *
 *  Note : this function is used by index prefix computation (BTREE building)
 */
static int
lang_split_key_utf8 (const LANG_COLLATION * lang_coll, const bool is_desc,
		     const unsigned char *str1, const int size1,
		     const unsigned char *str2, const int size2,
		     unsigned char **key, int *byte_size)
{
  const unsigned char *str1_end, *str2_end;
  const unsigned char *str1_begin, *str2_begin;
  unsigned char *str1_next, *str2_next;
  unsigned int w1, w2;
  int key_size;
  const COLL_DATA *coll = &(lang_coll->coll);

  assert (key != NULL);
  assert (byte_size != NULL);

  str1_end = str1 + size1;
  str2_end = str2 + size2;
  str1_begin = str1;
  str2_begin = str2;

  for (; str1 < str1_end && str2 < str2_end;)
    {
      w1 = lang_get_w_first_el (coll, str1, str1_end - str1, &str1_next);
      w2 = lang_get_w_first_el (coll, str2, str2_end - str2, &str2_next);

      if (w1 == 0xffffffff || w2 == 0xffffffff)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QSTR_BAD_SRC_CODESET,
		  0);
	  return ER_QSTR_BAD_SRC_CODESET;
	}

      if (w1 != w2)
	{
	  assert ((!is_desc && w1 < w2) || (is_desc && w1 > w2));
	  break;
	}

      str1 = str1_next;
      str2 = str2_next;
    }

  if (!is_desc)
    {				/* normal index */
      bool string2_has_one_more_char = false;

      if (str1 < str1_end)
	{
	  /* check if in string2 there is one more character (or contraction)
	   * after common part */
	  if (str2 < str2_end)
	    {
	      w2 = lang_get_w_first_el (coll, str2, str2_end - str2,
					&str2_next);
	      assert (w2 != 0);
	      if (str2_next == str2_end)
		{
		  string2_has_one_more_char = true;
		}
	    }
	}

      if (str1 == str1_end || string2_has_one_more_char)
	{
	  *key = (unsigned char *) str1_begin;
	  key_size = size1;
	}
      else
	{
	  assert (str2 < str2_end);

	  *key = (unsigned char *) str2_begin;

	  /* common part plus one more character (or more, if last unit is a
	   * contraction) from string2 */
	  key_size = str2_next - str2_begin;
	  assert (key_size <= size2);
	}
    }
  else
    {				/* desc domain */
      if (str1 == str1_end)
	{
	  /* actually, this could happen only when string1 == string2 */
	  *key = (unsigned char *) str1_begin;
	  key_size = size1;
	}
      else
	{
	  assert (str1 < str1_end);
	  *key = (unsigned char *) str1_begin;

	  /* common part plus one more non-zero weight collation unit */
	  do
	    {
	      w1 = lang_get_w_first_el (coll, str1, str1_end - str1,
					&str1_next);
	      str1 = str1_next;
	    }
	  while (w1 == 0 && str1 < str1_end);

	  key_size = str1_next - str1_begin;

	  assert (key_size <= size1);
	}
    }

  *byte_size = key_size;

  return NO_ERROR;
}

/*
 * English Locale Data
 */

/* English collation */
static unsigned int lang_upper_EN[LANG_CHAR_COUNT_EN];
static unsigned int lang_lower_EN[LANG_CHAR_COUNT_EN];

/*
 * lang_init_common_en_cs () - init collation data for English case
 *			       sensitive (no matter the charset)
 *   return:
 */
static void
lang_init_common_en_cs (void)
{
  int i;
  static bool is_common_en_cs_init = false;

  if (is_common_en_cs_init)
    {
      return;
    }

  for (i = 0; i < LANG_CHAR_COUNT_EN; i++)
    {
      lang_weight_EN_cs[i] = i;
      lang_next_alpha_char_EN_cs[i] = i + 1;
    }

  lang_weight_EN_cs[32] = 0;
  lang_next_alpha_char_EN_cs[32] = 1;

  is_common_en_cs_init = true;
}

/*
 * lang_init_common_en_ci () - init collation data for English case
 *			       insensitive (no matter the charset)
 *   return:
 */
static void
lang_init_common_en_ci (void)
{
  int i;
  static bool is_common_en_ci_init = false;

  if (is_common_en_ci_init)
    {
      return;
    }

  for (i = 0; i < LANG_CHAR_COUNT_EN; i++)
    {
      lang_weight_EN_ci[i] = i;
      lang_next_alpha_char_EN_ci[i] = i + 1;
    }

  for (i = 'a'; i <= (int) 'z'; i++)
    {
      lang_weight_EN_ci[i] = i - ('a' - 'A');
      lang_next_alpha_char_EN_ci[i] = i + 1 - ('a' - 'A');
    }

  lang_next_alpha_char_EN_ci['z'] = lang_next_alpha_char_EN_ci['Z'];
  lang_next_alpha_char_EN_ci['a' - 1] = lang_next_alpha_char_EN_ci['A' - 1];

  lang_weight_EN_ci[32] = 0;
  lang_next_alpha_char_EN_ci[32] = 1;

  is_common_en_ci_init = true;
}

/*
 * lang_init_coll_utf8_en_cs () - init collation UTF8 English case sensitive
 *   return:
 */
static void
lang_init_coll_utf8_en_cs (LANG_COLLATION * lang_coll)
{
  assert (lang_coll != NULL);

  if (!(lang_coll->need_init))
    {
      return;
    }

  /* init data */
  lang_init_common_en_cs ();

  lang_coll->need_init = false;
}

/*
 * lang_init_coll_en_ci () - init collation English case insensitive; applies
 *			     to both ISO and UTF-8 charset
 *   return:
 */
static void
lang_init_coll_en_ci (LANG_COLLATION * lang_coll)
{
  assert (lang_coll != NULL);

  if (!(lang_coll->need_init))
    {
      return;
    }

  /* init data */
  lang_init_common_en_ci ();

  lang_coll->need_init = false;
}

/*
 * lang_initloc_en () - init locale data for English language
 *   return:
 */
static void
lang_initloc_en_utf8 (LANG_LOCALE_DATA * ld)
{
  int i;

  assert (ld != NULL);

  assert (ld->default_lang_coll != NULL);

  /* init alphabet */
  for (i = 0; i < LANG_CHAR_COUNT_EN; i++)
    {
      lang_upper_EN[i] = i;
      lang_lower_EN[i] = i;
    }

  for (i = (int) 'a'; i <= (int) 'z'; i++)
    {
      lang_upper_EN[i] = i - ('a' - 'A');
      lang_lower_EN[i - ('a' - 'A')] = i;
    }

  /* other initializations to follow here */
  coll_utf8_binary.default_lang = ld;
  coll_utf8_en_cs.default_lang = ld;
  coll_utf8_en_ci.default_lang = ld;

  ld->is_initialized = true;
}

/*
 * lang_fastcmp_byte () - string compare for English language in UTF-8
 *   return:
 *   lang_coll(in):
 *   string1(in):
 *   size1(in):
 *   string2(in):
 *   size2(in):
 *
 * Note: This string comparison ignores trailing white spaces.
 */
static int
lang_fastcmp_byte (const LANG_COLLATION * lang_coll,
		   const unsigned char *string1, const int size1,
		   const unsigned char *string2, const int size2)
{
  int cmp, i, size;

  size = size1 < size2 ? size1 : size2;
  for (cmp = 0, i = 0; cmp == 0 && i < size; i++)
    {
      /* compare weights of the two chars */
      cmp = lang_coll->coll.weights[*string1++] -
	lang_coll->coll.weights[*string2++];
    }
  if (cmp != 0 || size1 == size2)
    {
      return cmp;
    }

  if (size1 < size2)
    {
      size = size2 - size1;
      for (i = 0; i < size && cmp == 0; i++)
	{
	  /* ignore tailing white spaces */
	  if (lang_coll->coll.weights[*string2++])
	    {
	      return -1;
	    }
	}
    }
  else
    {
      size = size1 - size2;
      for (i = 0; i < size && cmp == 0; i++)
	{
	  /* ignore trailing white spaces */
	  if (lang_coll->coll.weights[*string1++])
	    {
	      return 1;
	    }
	}
    }

  return cmp;
}

/*
 * lang_mht2str_byte () -
 *   return:
 *   lang_coll(in):
 *   str(in):
 *   size(in):
 *
 */
static unsigned int
lang_mht2str_byte (const LANG_COLLATION * lang_coll,
		   const unsigned char *str, const int size)
{
  const unsigned char *str_end = str + size;
  unsigned int pseudo_key = 0;
  unsigned int w;

  for (; str < str_end; str++)
    {
      w = lang_coll->coll.weights[*str];
      ADD_TO_HASH (pseudo_key, w);
    }

  return pseudo_key;
}

static LANG_LOCALE_DATA lc_English_utf8 = {
  NULL,
  LANG_NAME_ENGLISH,
  INTL_LANG_ENGLISH,
  INTL_CODESET_UTF8,
  {ALPHABET_ASCII, INTL_CODESET_UTF8, LANG_CHAR_COUNT_EN, 1, lang_lower_EN, 1,
   lang_upper_EN,
   false},
  {ALPHABET_ASCII, INTL_CODESET_UTF8, LANG_CHAR_COUNT_EN, 1, lang_lower_EN, 1,
   lang_upper_EN,
   false},
  &coll_utf8_en_ci,
  &con_iso_8859_1_conv,		/* text conversion */
  false,
  NULL,				/* time, date, date-time, timestamp format */
  NULL,
  NULL,
  NULL,
  {NULL},
  {NULL},
  {NULL},
  {NULL},
  {NULL},
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  '.',
  ',',
#if defined(ENABLE_UNUSED_FUNCTION)
  LANG_NO_NORMALIZATION,
#endif
  "945bead220ece6f4d020403835308785",
  lang_initloc_en_utf8,
  false
};

/*
 * Korean Locale Data
 */

/*
 * lang_initloc_ko_utf8 () - init locale data for Korean language with UTF-8
 *			     charset
 *   return:
 */
static void
lang_initloc_ko_utf8 (LANG_LOCALE_DATA * ld)
{
  assert (ld != NULL);

  coll_utf8_ko_cs.default_lang = ld;

  ld->is_initialized = true;
}

/* built-in support of Korean in UTF-8 : date-time conversions as in English
 * collation : by codepoints
 * this needs to be overriden by user defined locale */
static LANG_LOCALE_DATA lc_Korean_utf8 = {
  NULL,
  LANG_NAME_KOREAN,
  INTL_LANG_KOREAN,
  INTL_CODESET_UTF8,
  {ALPHABET_ASCII, INTL_CODESET_UTF8, LANG_CHAR_COUNT_EN, 1, lang_lower_EN, 1,
   lang_upper_EN, false},
  {ALPHABET_ASCII, INTL_CODESET_UTF8, LANG_CHAR_COUNT_EN, 1, lang_lower_EN, 1,
   lang_upper_EN, false},
  &coll_utf8_ko_cs,		/* collation */
  NULL,				/* console text conversion */
  false,
  NULL,				/* time, date, date-time, timestamp format */
  NULL,
  NULL,
  NULL,
  {NULL},
  {NULL},
  {NULL},
  {NULL},
  {NULL},
  NULL,
  NULL,
  NULL,
  NULL,
  NULL,
  '.',
  ',',
#if defined(ENABLE_UNUSED_FUNCTION)
  LANG_NO_NORMALIZATION,
#endif
  "802cff8e10d857952241d19b50a13a27",
  lang_initloc_ko_utf8,
  false
};

/*
 * destroy_user_locales - frees the memory holding the locales already loaded
 *                        from the locale libraries (DLL/so)
 */
static void
lang_free_locales (void)
{
  int i;

  for (i = 0; i < lang_count_locales; i++)
    {
      assert (lang_loaded_locales[i] != NULL);

      free_lang_locale_data (lang_loaded_locales[i]);
      lang_loaded_locales[i] = NULL;
    }

  lang_count_locales = 0;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * lang_get_generic_unicode_norm - gets the global unicode
 *		    normalization structure
 * Returns:
 */
UNICODE_NORMALIZATION *
lang_get_generic_unicode_norm (void)
{
  return generic_unicode_norm;
}

/*
 * lang_set_generic_unicode_norm - sets the global unicode
 *		    normalization structure
 */
void
lang_set_generic_unicode_norm (UNICODE_NORMALIZATION * norm)
{
  generic_unicode_norm = norm;
}
#endif

/*
 * lang_free_collations - frees all collation data
 */
static void
lang_free_collations (void)
{
  int i;

  if (lang_count_collations <= 0)
    {
      return;
    }
  for (i = 0; i < LANG_MAX_COLLATIONS; i++)
    {
      assert (lang_collations[i] != NULL);
      if (!(lang_collations[i]->built_in))
	{
	  free (lang_collations[i]);
	}
      lang_collations[i] = NULL;
    }

  lang_count_collations = 0;
}

/*
 * lang_check_coll_compat - checks compatibility of current collations (of
 *			    running process) with a reference set of
 *			    collations
 * Returns : error code
 * coll_array(in): reference collations
 * coll_cnt(in):
 * client_text(in): text to display in message error for client (this can be
 *		    "server" when checking server vs database)
 * server_text(in): text to display in message error for server (this can be
 *		    "database" when checking server vs database)
 */
int
lang_check_coll_compat (const LANG_COLL_COMPAT * coll_array,
			const int coll_cnt, const char *client_text,
			const char *server_text)
{
  char err_msg[ER_MSG_SIZE];
  int i;
  int er_status = NO_ERROR;

  assert (coll_array != NULL);
  assert (coll_cnt > 0);
  assert (client_text != NULL);
  assert (server_text != NULL);

  if (lang_count_collations != coll_cnt)
    {
      snprintf (err_msg, sizeof (err_msg) - 1,
		"Number of collations do not match : "
		"%s has %d collations, %s has %d collations",
		client_text, lang_count_collations, server_text, coll_cnt);
      er_status = ER_LOC_INIT;
      LOG_LOCALE_ERROR (err_msg, ER_LOC_INIT, false);
      goto exit;
    }

  for (i = 0; i < coll_cnt; i++)
    {
      const LANG_COLL_COMPAT *ref_c;
      LANG_COLLATION *lc;

      ref_c = &(coll_array[i]);

      assert (ref_c->coll_id >= 0 && ref_c->coll_id < LANG_MAX_COLLATIONS);
      /* collation id is valid, check if same collation */
      lc = lang_get_collation (ref_c->coll_id);

      if (lc->coll.coll_id != ref_c->coll_id)
	{
	  snprintf (err_msg, sizeof (err_msg) - 1,
		    "Collation '%s' with id %d from %s not found with the "
		    "same id on %s", ref_c->coll_name, ref_c->coll_id,
		    server_text, client_text);
	  er_status = ER_LOC_INIT;
	  LOG_LOCALE_ERROR (err_msg, ER_LOC_INIT, false);
	  goto exit;
	}

      if (strcmp (lc->coll.coll_name, ref_c->coll_name))
	{
	  snprintf (err_msg, sizeof (err_msg) - 1,
		    "Names of collation with id %d do not match : "
		    "on %s, is '%s'; on %s, is '%s'",
		    ref_c->coll_id, client_text, ref_c->coll_name,
		    server_text, lc->coll.coll_name);
	  er_status = ER_LOC_INIT;
	  LOG_LOCALE_ERROR (err_msg, ER_LOC_INIT, false);
	  goto exit;
	}

      if (lc->codeset != ref_c->codeset)
	{
	  snprintf (err_msg, sizeof (err_msg) - 1,
		    "Codesets of collation '%s' with id %d do not match : "
		    "on %s, codeset is %d; on %s, codeset is %d",
		    ref_c->coll_name, ref_c->coll_id,
		    client_text, ref_c->codeset, server_text, lc->codeset);
	  er_status = ER_LOC_INIT;
	  LOG_LOCALE_ERROR (err_msg, ER_LOC_INIT, false);
	  goto exit;
	}

      if (strcasecmp (lc->coll.checksum, ref_c->checksum))
	{
	  snprintf (err_msg, sizeof (err_msg) - 1,
		    "Collation '%s' with id %d has changed : "
		    "on %s, checksum is '%s'; on %s, checksum is '%s'",
		    ref_c->coll_name, ref_c->coll_id,
		    client_text, ref_c->checksum,
		    server_text, lc->coll.checksum);
	  er_status = ER_LOC_INIT;
	  LOG_LOCALE_ERROR (err_msg, ER_LOC_INIT, false);
	  goto exit;
	}
    }
exit:
  return er_status;
}

/*
 * lang_check_locale_compat - checks compatibility of current locales (of
 *			      running process) with a reference set of
 *			      locales
 * Returns : error code
 * loc_array(in): reference locales
 * loc_cnt(in):
 * client_text(in): text to display in message error for client
 * server_text(in): text to display in message error for server
 */
int
lang_check_locale_compat (const LANG_LOCALE_COMPAT * loc_array,
			  const int loc_cnt, const char *client_text,
			  const char *server_text)
{
  char err_msg[ER_MSG_SIZE];
  int i, j;
  int er_status = NO_ERROR;

  assert (loc_array != NULL);
  assert (loc_cnt > 0);

  /* check that each locale from client is defined by server */
  for (i = 0; i < lang_count_locales; i++)
    {
      LANG_LOCALE_DATA *lld = lang_loaded_locales[i];
      const LANG_LOCALE_COMPAT *ref_loc = NULL;

      do
	{
	  bool ref_found = false;

	  for (j = 0; j < loc_cnt; j++)
	    {
	      ref_loc = &(loc_array[j]);

	      if (lld->codeset == ref_loc->codeset &&
		  strcasecmp (lld->lang_name, ref_loc->lang_name) == 0)
		{
		  ref_found = true;
		  break;
		}
	    }

	  if (!ref_found)
	    {
	      snprintf (err_msg, sizeof (err_msg) - 1,
			"Locale '%s' with codeset %d loaded by %s "
			"not found on %s",
			lld->lang_name, lld->codeset, client_text,
			server_text);
	      er_status = ER_LOC_INIT;
	      LOG_LOCALE_ERROR (err_msg, ER_LOC_INIT, false);
	      goto exit;
	    }

	  assert (ref_found);

	  if (strcasecmp (ref_loc->checksum, lld->checksum))
	    {
	      snprintf (err_msg, sizeof (err_msg) - 1,
			"Locale '%s' with codeset %d has changed : "
			"on %s, checksum is '%s'; on %s, checksum is '%s'",
			ref_loc->lang_name, ref_loc->codeset,
			server_text, ref_loc->checksum,
			client_text, lld->checksum);
	      er_status = ER_LOC_INIT;
	      LOG_LOCALE_ERROR (err_msg, ER_LOC_INIT, false);
	      goto exit;
	    }
	  lld = lld->next_lld;

	}
      while (lld != NULL);
    }

  /* check that each locale from server is loaded by client */
  for (j = 0; j < loc_cnt; j++)
    {
      bool loc_found = false;
      const LANG_LOCALE_COMPAT *ref_loc = NULL;
      LANG_LOCALE_DATA *lld = NULL;

      ref_loc = &(loc_array[j]);

      for (i = 0; i < lang_count_locales && !loc_found; i++)
	{
	  lld = lang_loaded_locales[i];

	  do
	    {
	      if (lld->codeset == ref_loc->codeset &&
		  strcasecmp (lld->lang_name, ref_loc->lang_name) == 0)
		{
		  loc_found = true;
		  break;
		}
	      lld = lld->next_lld;
	    }
	  while (lld != NULL);
	}

      if (!loc_found)
	{
	  snprintf (err_msg, sizeof (err_msg) - 1,
		    "Locale '%s' with codeset %d defined on %s "
		    "is not loaded by %s",
		    ref_loc->lang_name, ref_loc->codeset, server_text,
		    client_text);
	  er_status = ER_LOC_INIT;
	  LOG_LOCALE_ERROR (err_msg, ER_LOC_INIT, false);
	  goto exit;
	}

      assert (loc_found && lld != NULL);

      if (strcasecmp (ref_loc->checksum, lld->checksum))
	{
	  snprintf (err_msg, sizeof (err_msg) - 1,
		    "Locale '%s' with codeset %d has changed : "
		    "on %s, checksum is '%s'; on %s, checksum is '%s'",
		    ref_loc->lang_name, ref_loc->codeset,
		    server_text, ref_loc->checksum,
		    client_text, lld->checksum);
	  er_status = ER_LOC_INIT;
	  LOG_LOCALE_ERROR (err_msg, ER_LOC_INIT, false);
	  goto exit;
	}
    }

exit:
  return er_status;
}
