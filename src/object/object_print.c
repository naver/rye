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
 * object_print.c - Routines to print dbvalues
 */

#ident "$Id$"

#include "config.h"

#include <stdlib.h>
#include <float.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>

#include "porting.h"
#include "chartype.h"
#include "misc_string.h"

#include "error_manager.h"
#include "memory_alloc.h"
#include "class_object.h"
#include "schema_manager.h"
#include "locator_cl.h"
#include "object_accessor.h"
#include "db.h"
#include "object_print.h"
#include "set_object.h"
#include "message_catalog.h"
#include "parser.h"
#include "statistics.h"
#include "server_interface.h"
#include "execute_schema.h"
#include "class_object.h"
#include "network_interface_cl.h"

#include "dbtype.h"
#include "language_support.h"
#include "string_opfunc.h"
#include "dbval.h"		/* this must be the last header file included!!! */

#if !defined(SERVER_MODE)
/*
 * Message id in the set MSGCAT_SET_HELP
 * in the message catalog MSGCAT_CATALOG_RYE (file rye.msg).
 */
#define MSGCAT_HELP_ROOTCLASS_TITLE     (1)
#define MSGCAT_HELP_CLASS_TITLE         (2)
#define MSGCAT_HELP_ATTRIBUTES          (3)
#define MSGCAT_HELP_QUERY_SPEC          (4)
#define MSGCAT_HELP_OBJECT_TITLE        (5)
#define MSGCAT_HELP_CMD_DESCRIPTION     (6)
#define MSGCAT_HELP_CMD_STRUCTURE       (7)
#define MSGCAT_HELP_CMD_EXAMPLE         (8)
#define MSGCAT_HELP_META_CLASS_HEADER   (9)
#define MSGCAT_HELP_CLASS_HEADER        (10)
#define MSGCAT_HELP_VCLASS_HEADER       (11)
#define MSGCAT_HELP_LDB_VCLASS_HEADER   (12)
#define MSGCAT_HELP_GENERAL_TXT         (13)


#if !defined(ER_HELP_INVALID_COMMAND)
#define ER_HELP_INVALID_COMMAND ER_GENERIC_ERROR
#endif /* !ER_HELP_INVALID_COMMAND */

/* safe string free */
#define STRFREE_W(string) \
  if (string != NULL) db_string_free((char *) (string))

#define MATCH_TOKEN(string, token) \
  ((string == NULL) ? 0 : intl_mbs_casecmp(string, token) == 0)

/*
 * STRLIST
 *
 * Note :
 *    Internal structure used for maintaining lists of strings.
 *    Makes it easier to collect up strings before putting them into a
 *    fixed length array.
 *    Could be generalized into a more globally useful utility.
 *
 */

typedef struct strlist
{
  struct strlist *next;
  const char *string;
} STRLIST;

/* Constant for routines that have static file buffers */
/* this allows some overhead plus max string length of ESCAPED characters! */
const int MAX_LINE = 4096;

/* maximum lines per section */
const int MAX_LINES = 1024;

#endif /* !SERVER_MODE */
/*
 * help_Max_set_elements
 *
 * description:
 *    Variable to control the printing of runaway sets.
 *    Should be a parameter ?
 */
static int help_Max_set_elements = 20;



static PARSER_VARCHAR *describe_set (const PARSER_CONTEXT * parser,
				     PARSER_VARCHAR * buffer,
				     const DB_SET * set);
static PARSER_VARCHAR *describe_double (const PARSER_CONTEXT * parser,
					PARSER_VARCHAR * buffer,
					const double value);
static PARSER_VARCHAR *describe_bit_string (const PARSER_CONTEXT * parser,
					    PARSER_VARCHAR * buffer,
					    const DB_VALUE * value);

#if !defined(SERVER_MODE)
static void obj_print_free_strarray (char **strs);
static char *obj_print_copy_string (const char *source);
#if defined (ENABLE_UNUSED_FUNCTION)
static const char **obj_print_convert_strlist (STRLIST * str_list);
#endif
static PARSER_VARCHAR *obj_print_describe_domain (PARSER_CONTEXT * parser,
						  PARSER_VARCHAR * buffer,
						  TP_DOMAIN * domain,
						  OBJ_PRINT_TYPE prt_type);
static PARSER_VARCHAR *obj_print_identifier (PARSER_CONTEXT * parser,
					     PARSER_VARCHAR * buffer,
					     const char *identifier,
					     OBJ_PRINT_TYPE prt_type);
static char *obj_print_describe_attribute (MOP class_p,
					   PARSER_CONTEXT * parser,
					   SM_ATTRIBUTE * attribute_p,
					   OBJ_PRINT_TYPE prt_type);
static char *obj_print_describe_constraint (PARSER_CONTEXT * parser,
					    SM_CLASS * class_p,
					    SM_CLASS_CONSTRAINT *
					    constraint_p,
					    OBJ_PRINT_TYPE prt_type);
static CLASS_HELP *obj_print_make_class_help (void);
static OBJ_HELP *obj_print_make_obj_help (void);
static char *obj_print_next_token (char *ptr, char *buf);


/* This will be in one of the language directories under $RYE/msg */

static PARSER_CONTEXT *parser;

/*
 * obj_print_free_strarray() -  Most of the help functions build an array of
 *                              strings that contains the descriptions
 *                              of the object
 *      return: none
 *  strs(in) : array of strings
 *
 *  Note :
 *      This function frees the array when it is no longer necessary.
 */

static void
obj_print_free_strarray (char **strs)
{
  int i;

  if (strs == NULL)
    {
      return;
    }

  for (i = 0; strs[i] != NULL; i++)
    {
      free_and_init (strs[i]);
    }
  free_and_init (strs);
}

/*
 * obj_print_copy_string() - Copies a string, allocating space with malloc
 *      return: new string
 *  source(in) : string to copy
 *
 */

static char *
obj_print_copy_string (const char *source)
{
  char *new_str = NULL;

  if (source != NULL)
    {
      new_str = (char *) malloc (strlen (source) + 1);
      if (new_str != NULL)
	{
	  strcpy (new_str, source);
	}
    }
  return new_str;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * obj_print_convert_strlist() - This converts a string list into an array
 *                               of strings
 *      return: NULL terminated array of strings
 *  str_list(in) : string list
 *
 *  Note :
 *      Since the strings are pushed on the list in reverse order, we
 *      build the array in reverse order so the resulting array will
 *      "read" correctly.
 *
 */

static const char **
obj_print_convert_strlist (STRLIST * str_list)
{
  STRLIST *l, *next;
  const char **array;
  int count, i;

  assert (str_list != NULL);

  array = NULL;
  count = ws_list_length ((DB_LIST *) str_list);

  if (count)
    {
      array = (const char **) malloc (sizeof (char *) * (count + 1));
      if (array != NULL)
	{
	  for (i = count - 1, l = str_list, next = NULL; i >= 0;
	       i--, l = next)
	    {
	      next = l->next;
	      array[i] = l->string;
	      free_and_init (l);
	    }
	  array[count] = NULL;
	}
    }
  return array;
}
#endif

/*
 * object_print_identifier() - help function to print identifier string.
 *                             if prt_type is OBJ_PRINT_SHOW_CREATE_TABLE,
 *                             we need wrap it with "[" and "]".
 *      return: advanced buffer pointer
 *  parser(in) :
 *  buffer(in) : current buffer pointer
 *  identifier(in) : identifier string,.such as: table name.
 *  prt_type(in): the print type: rsql schema or show create table
 *
 */
static PARSER_VARCHAR *
obj_print_identifier (PARSER_CONTEXT * parser, PARSER_VARCHAR * buffer,
		      const char *identifier, OBJ_PRINT_TYPE prt_type)
{
  if (prt_type == OBJ_PRINT_RSQL_SCHEMA_COMMAND)
    {
      buffer = pt_append_nulstring (parser, buffer, identifier);
    }
  else
    {				/* prt_type == OBJ_PRINT_SHOW_CREATE_TABLE */
      buffer = pt_append_nulstring (parser, buffer, "[");
      buffer = pt_append_nulstring (parser, buffer, identifier);
      buffer = pt_append_nulstring (parser, buffer, "]");
    }

  return buffer;
}

/* CLASS COMPONENT DESCRIPTION FUNCTIONS */

/*
 * obj_print_describe_domain() - Describe the domain of an attribute
 *      return: advanced buffer pointer
 *  parser(in) :
 *  buffer(in) : current buffer pointer
 *  domain(in) : domain structure to describe
 *  prt_type(in): the print type: rsql schema or show create table
 *
 */

static PARSER_VARCHAR *
obj_print_describe_domain (PARSER_CONTEXT * parser, PARSER_VARCHAR * buffer,
			   TP_DOMAIN * domain, OBJ_PRINT_TYPE prt_type)
{
  TP_DOMAIN *temp_domain;
  char temp_buffer[27];
  char temp_buffer_numeric[50];
  int precision = 0;
  int has_collation;

  if (domain == NULL)
    {
      return buffer;
    }

  /* filter first, usually not necessary but this is visible */
  sm_filter_domain (domain);

  for (temp_domain = domain; temp_domain != NULL;
       temp_domain = temp_domain->next)
    {
      has_collation = 0;
      switch (TP_DOMAIN_TYPE (temp_domain))
	{
	case DB_TYPE_INTEGER:
	case DB_TYPE_BIGINT:
	case DB_TYPE_DOUBLE:
	case DB_TYPE_TIME:
	case DB_TYPE_DATETIME:
	case DB_TYPE_DATE:
	case DB_TYPE_SUB:
	case DB_TYPE_OID:
	case DB_TYPE_NULL:
	case DB_TYPE_VARIABLE:
	  strcpy (temp_buffer, temp_domain->type->name);
	  ustr_upper (temp_buffer);
	  buffer = pt_append_nulstring (parser, buffer, temp_buffer);
	  break;

	case DB_TYPE_OBJECT:
	  if (temp_domain->class_mop != NULL)
	    {
	      buffer =
		obj_print_identifier (parser, buffer,
				      sm_class_name (temp_domain->class_mop),
				      prt_type);
	    }
	  else
	    {
	      buffer =
		pt_append_nulstring (parser, buffer, temp_domain->type->name);
	    }
	  break;

	case DB_TYPE_VARCHAR:
	  has_collation = 1;
	  if (temp_domain->precision == TP_FLOATING_PRECISION_VALUE)
	    {
	      buffer = pt_append_nulstring (parser, buffer, "STRING");
	      break;
	    }
	  /* fall through */
	case DB_TYPE_VARBIT:
	  strcpy (temp_buffer, temp_domain->type->name);
	  ustr_upper (temp_buffer);
	  buffer = pt_append_nulstring (parser, buffer, temp_buffer);
	  if (temp_domain->precision == TP_FLOATING_PRECISION_VALUE)
	    {
	      precision = DB_MAX_STRING_LENGTH;
	    }
	  else
	    {
	      precision = temp_domain->precision;
	    }
	  sprintf (temp_buffer, "(%d)", precision);
	  buffer = pt_append_nulstring (parser, buffer, temp_buffer);
	  break;

	case DB_TYPE_NUMERIC:
	  strcpy (temp_buffer, temp_domain->type->name);
	  ustr_upper (temp_buffer);
	  buffer = pt_append_nulstring (parser, buffer, temp_buffer);
	  sprintf (temp_buffer_numeric, "(%d,%d)",
		   temp_domain->precision, temp_domain->scale);
	  buffer = pt_append_nulstring (parser, buffer, temp_buffer_numeric);
	  break;

	case DB_TYPE_SEQUENCE:
	  STRNCPY (temp_buffer, temp_domain->type->name, 27);
	  ustr_upper (temp_buffer);
	  buffer = pt_append_nulstring (parser, buffer, temp_buffer);
	  buffer = pt_append_nulstring (parser, buffer, " OF ");
	  if (temp_domain->setdomain != NULL)
	    {
	      if (temp_domain->setdomain->next != NULL
		  && prt_type == OBJ_PRINT_SHOW_CREATE_TABLE)
		{
		  buffer = pt_append_nulstring (parser, buffer, "(");
		  buffer =
		    obj_print_describe_domain (parser, buffer,
					       temp_domain->setdomain,
					       prt_type);
		  buffer = pt_append_nulstring (parser, buffer, ")");
		}
	      else
		{
		  buffer =
		    obj_print_describe_domain (parser, buffer,
					       temp_domain->setdomain,
					       prt_type);
		}
	    }
	  break;

	default:
	  break;
	}

      if (has_collation && temp_domain->collation_id != LANG_SYS_COLLATION)
	{
	  buffer = pt_append_nulstring (parser, buffer, " COLLATE ");
	  buffer =
	    pt_append_nulstring (parser, buffer,
				 lang_get_collation_name
				 (temp_domain->collation_id));
	}
      if (temp_domain->next != NULL)
	{
	  buffer = pt_append_nulstring (parser, buffer, ", ");
	}
    }
  return buffer;
}

/*
 * obj_print_describe_attribute() - Describes the definition of an attribute
 *                                  in a class
 *      return: advanced bufbuffer pointer
 *  class_p(in) : class being examined
 *  parser(in) :
 *  attribute_p(in) : attribute of the class
 *  prt_type(in): the print type: rsql schema or show create table
 *
 */

static char *
obj_print_describe_attribute (MOP class_p, PARSER_CONTEXT * parser,
			      SM_ATTRIBUTE * attribute_p,
			      OBJ_PRINT_TYPE prt_type)
{
  char *start;
  PARSER_VARCHAR *buffer;
  char line[SM_MAX_IDENTIFIER_LENGTH + 4];	/* Include room for _:_\0 */

  if (attribute_p == NULL)
    {
      return NULL;
    }

  assert (TP_DOMAIN_TYPE (attribute_p->sma_domain) != DB_TYPE_VARIABLE);

  buffer = NULL;
  if (prt_type == OBJ_PRINT_RSQL_SCHEMA_COMMAND)
    {
      sprintf (line, "%-20s ", attribute_p->name);
      buffer = pt_append_nulstring (parser, buffer, line);
    }
  else
    {				/* prt_type == OBJ_PRINT_SHOW_CREATE_TABLE */
      buffer =
	obj_print_identifier (parser, buffer, attribute_p->name, prt_type);
      buffer = pt_append_nulstring (parser, buffer, " ");
    }

  start = (char *) pt_get_varchar_bytes (buffer);
  /* could filter here but do in describe_domain */

  buffer =
    obj_print_describe_domain (parser, buffer, attribute_p->sma_domain,
			       prt_type);

  if (!DB_IS_NULL (&attribute_p->default_value.value)
      || attribute_p->default_value.default_expr != DB_DEFAULT_NONE)
    {
      buffer = pt_append_nulstring (parser, buffer, " DEFAULT ");

      switch (attribute_p->default_value.default_expr)
	{
	case DB_DEFAULT_SYSDATE:
	  buffer = pt_append_nulstring (parser, buffer, "SYS_DATE");
	  break;
	case DB_DEFAULT_SYSDATETIME:
	  buffer = pt_append_nulstring (parser, buffer, "SYS_DATETIME");
	  break;
	case DB_DEFAULT_UNIX_TIMESTAMP:
	  buffer = pt_append_nulstring (parser, buffer, "UNIX_TIMESTAMP");
	  break;
	case DB_DEFAULT_USER:
	  buffer = pt_append_nulstring (parser, buffer, "USER");
	  break;
	case DB_DEFAULT_CURR_USER:
	  buffer = pt_append_nulstring (parser, buffer, "CURRENT_USER");
	  break;
	default:
	  buffer = describe_value (parser, buffer,
				   &attribute_p->default_value.value);
	  break;
	}
    }

  if (attribute_p->flags & SM_ATTFLAG_NON_NULL)
    {
      buffer = pt_append_nulstring (parser, buffer, " NOT NULL");
    }
  if (attribute_p->class_mop != NULL && attribute_p->class_mop != class_p)
    {
      buffer = pt_append_nulstring (parser, buffer, " /* from ");
      buffer =
	obj_print_identifier (parser, buffer,
			      sm_class_name (attribute_p->class_mop),
			      prt_type);
      buffer = pt_append_nulstring (parser, buffer, " */");
    }

  /* let the higher level display routine do this */
  /*
     buffer = pt_append_nulstring(parser,buffer,"\n");
   */
  return ((char *) pt_get_varchar_bytes (buffer));
}

/*
 * obj_print_describe_constraint() - Describes the definition of an attribute
 *                                   in a class
 *      return: advanced buffer pointer
 *  parser(in) :
 *  class_p(in) : class being examined
 *  constraint_p(in) :
 *  prt_type(in): the print type: rsql schema or show create table
 *
 */

static char *
obj_print_describe_constraint (PARSER_CONTEXT * parser,
			       SM_CLASS * class_p,
			       SM_CLASS_CONSTRAINT * constraint_p,
			       OBJ_PRINT_TYPE prt_type)
{
  PARSER_VARCHAR *buffer;
  SM_ATTRIBUTE **attribute_p;
  const int *asc_desc;
  int k;

  buffer = NULL;

  if (!class_p || !constraint_p)
    {
      return NULL;
    }

  if (prt_type == OBJ_PRINT_RSQL_SCHEMA_COMMAND)
    {
      switch (constraint_p->type)
	{
	case SM_CONSTRAINT_INDEX:
	  buffer = pt_append_nulstring (parser, buffer, "INDEX ");
	  break;
	case SM_CONSTRAINT_UNIQUE:
	  buffer = pt_append_nulstring (parser, buffer, "UNIQUE ");
	  break;
	case SM_CONSTRAINT_PRIMARY_KEY:
	  buffer = pt_append_nulstring (parser, buffer, "PRIMARY KEY ");
	  break;
	default:
	  buffer = pt_append_nulstring (parser, buffer, "CONSTRAINT ");
	  break;
	}

      buffer = pt_append_nulstring (parser, buffer, constraint_p->name);
      buffer = pt_append_nulstring (parser, buffer, " ON ");
      buffer = pt_append_nulstring (parser, buffer, class_p->header.name);
      buffer = pt_append_nulstring (parser, buffer, " (");

      asc_desc = constraint_p->asc_desc;
    }
  else
    {				/* prt_type == OBJ_PRINT_SHOW_CREATE_TABLE */
      switch (constraint_p->type)
	{
	case SM_CONSTRAINT_INDEX:
	  buffer = pt_append_nulstring (parser, buffer, " INDEX ");
	  buffer =
	    obj_print_identifier (parser, buffer, constraint_p->name,
				  prt_type);
	  break;
	case SM_CONSTRAINT_UNIQUE:
	  buffer = pt_append_nulstring (parser, buffer, " CONSTRAINT ");
	  buffer =
	    obj_print_identifier (parser, buffer, constraint_p->name,
				  prt_type);
	  buffer = pt_append_nulstring (parser, buffer, " UNIQUE KEY ");
	  break;
	case SM_CONSTRAINT_PRIMARY_KEY:
	  buffer = pt_append_nulstring (parser, buffer, " CONSTRAINT ");
	  buffer =
	    obj_print_identifier (parser, buffer, constraint_p->name,
				  prt_type);
	  buffer = pt_append_nulstring (parser, buffer, " PRIMARY KEY ");
	  break;
	default:
	  assert (false);
	  break;
	}

      buffer = pt_append_nulstring (parser, buffer, " (");
      asc_desc = constraint_p->asc_desc;
    }

  for (attribute_p = constraint_p->attributes, k = 0;
       *attribute_p != NULL; attribute_p++, k++)
    {
      if (k > 0)
	{
	  buffer = pt_append_nulstring (parser, buffer, ", ");
	}
      buffer = obj_print_identifier (parser, buffer,
				     (*attribute_p)->name, prt_type);

      if (asc_desc)
	{
	  if (*asc_desc == 1)
	    {
	      buffer = pt_append_nulstring (parser, buffer, " DESC");
	    }
	  asc_desc++;
	}
    }
  buffer = pt_append_nulstring (parser, buffer, ")");

  if (constraint_p->index_status != INDEX_STATUS_COMPLETED)
    {
      buffer = pt_append_nulstring (parser, buffer, "-(INVALID)");
    }

  return ((char *) pt_get_varchar_bytes (buffer));
}				/* describe_constraint() */

/* CLASS HELP */

/*
 * obj_print_make_class_help () - Creates an empty class help structure
 *   return: class help structure
 */

static CLASS_HELP *
obj_print_make_class_help (void)
{
  CLASS_HELP *new_p;

  new_p = (CLASS_HELP *) malloc (sizeof (CLASS_HELP));
  if (new_p == NULL)
    {
      return NULL;
    }
  new_p->name = NULL;
  new_p->class_type = NULL;
  new_p->attributes = NULL;
  new_p->query_spec = NULL;
  new_p->object_id = NULL;
  new_p->shard_by = NULL;

  return new_p;
}

/*
 * obj_print_help_free_class () - Frees a class help structure that is no longer needed
 *                      The help structure should have been built
 *                      by help_class()
 *   return: none
 *   info(in): class help structure
 */

void
obj_print_help_free_class (CLASS_HELP * info)
{
  if (info != NULL)
    {
      if (info->name != NULL)
	{
	  free_and_init (info->name);
	}
      if (info->class_type != NULL)
	{
	  free_and_init (info->class_type);
	}
      if (info->object_id != NULL)
	{
	  free_and_init (info->object_id);
	}
      if (info->shard_by != NULL)
	{
	  free_and_init (info->shard_by);
	}
      obj_print_free_strarray (info->attributes);
      obj_print_free_strarray (info->query_spec);
      obj_print_free_strarray (info->constraints);
      free_and_init (info);
    }
}

/*
 * obj_print_help_class () - Constructs a class help structure containing textual
 *                 information about the class.
 *   return: class help structure
 *   op(in): class object
 *   prt_type(in): the print type: rsql schema or show create table
 */

CLASS_HELP *
obj_print_help_class (MOP op, OBJ_PRINT_TYPE prt_type)
{
  SM_CLASS *class_;
  SM_ATTRIBUTE *a;
  SM_QUERY_SPEC *p;
  CLASS_HELP *info = NULL;
  int count, i;
  char **strs = NULL;
  char *description;
  char name_buf[SM_MAX_IDENTIFIER_LENGTH + 2];

  if (parser == NULL)
    {
      parser = parser_create_parser ();
    }
  if (parser == NULL)
    {
      goto error_exit;
    }

  if (!locator_is_class (op) || locator_is_root (op))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
      goto error_exit;
    }

  else if (au_fetch_class (op, &class_, S_LOCK, AU_SELECT) == NO_ERROR)
    {
      /* make sure all the information is up to date */
      if (sm_clean_class (op, class_) != NO_ERROR)
	{
	  goto error_exit;
	}

      info = obj_print_make_class_help ();
      if (info == NULL)
	{
	  goto error_exit;
	}

      if (prt_type == OBJ_PRINT_RSQL_SCHEMA_COMMAND)
	{
	  info->name = obj_print_copy_string ((char *) class_->header.name);
	}
      else
	{			/* prt_type == OBJ_PRINT_SHOW_CREATE_TABLE */
	  snprintf (name_buf, SM_MAX_IDENTIFIER_LENGTH + 2, "[%s]",
		    class_->header.name);
	  info->name = obj_print_copy_string (name_buf);
	}

      switch (class_->class_type)
	{
	default:
	  info->class_type =
	    obj_print_copy_string (msgcat_message
				   (MSGCAT_CATALOG_RYE, MSGCAT_SET_HELP,
				    MSGCAT_HELP_META_CLASS_HEADER));
	  break;
	case SM_CLASS_CT:
	  info->class_type =
	    obj_print_copy_string (msgcat_message
				   (MSGCAT_CATALOG_RYE, MSGCAT_SET_HELP,
				    MSGCAT_HELP_CLASS_HEADER));
	  break;
	case SM_VCLASS_CT:
	  info->class_type =
	    obj_print_copy_string (msgcat_message (MSGCAT_CATALOG_RYE,
						   MSGCAT_SET_HELP,
						   MSGCAT_HELP_VCLASS_HEADER));
	  break;
	}

      if (class_->attributes != NULL)
	{
	  count = 0;
	  /* find the number own by itself */
	  for (a = class_->ordered_attributes; a != NULL; a = a->order_link)
	    {
	      if (a->class_mop == op)
		{
		  count++;
		}
	    }

	  if (count > 0)
	    {
	      strs = (char **) malloc (sizeof (char *) * (count + 1));
	      if (strs == NULL)
		{
		  goto error_exit;
		}

	      /* init */
	      for (i = 0; i < count + 1; i++)
		{
		  strs[i] = NULL;
		}
	      info->attributes = strs;

	      i = 0;
	      for (a = class_->ordered_attributes; a != NULL;
		   a = a->order_link)
		{
		  if (a->class_mop == op)
		    {
		      description =
			obj_print_describe_attribute (op, parser, a,
						      prt_type);
		      if (description == NULL)
			{
			  goto error_exit;
			}
		      strs[i] = obj_print_copy_string (description);

		      if (a->flags & SM_ATTFLAG_SHARD_KEY)
			{
			  PARSER_VARCHAR *buffer;
			  char line[SM_MAX_IDENTIFIER_LENGTH + 4];

			  buffer = NULL;

			  buffer =
			    pt_append_nulstring (parser, buffer, "SHARD BY ");
			  if (prt_type == OBJ_PRINT_RSQL_SCHEMA_COMMAND)
			    {
			      sprintf (line, "%-20s", a->name);
			      buffer =
				pt_append_nulstring (parser, buffer, line);
			    }
			  else
			    {	/* prt_type == OBJ_PRINT_SHOW_CREATE_TABLE */
			      buffer =
				obj_print_identifier (parser, buffer, a->name,
						      prt_type);
			    }

			  info->shard_by =
			    obj_print_copy_string ((const char *)
						   pt_get_varchar_bytes
						   (buffer));

			}

		      i++;
		    }
		}
	    }
	}

      if (class_->query_spec != NULL)
	{
	  count = ws_list_length ((DB_LIST *) class_->query_spec);
	  strs = (char **) malloc (sizeof (char *) * (count + 1));
	  if (strs == NULL)
	    {
	      goto error_exit;
	    }
	  i = 0;
	  for (p = class_->query_spec; p != NULL; p = p->next)
	    {
	      strs[i] = obj_print_copy_string ((char *) p->specification);
	      i++;
	    }
	  strs[i] = NULL;
	  info->query_spec = strs;
	}

      /*
       *  Process multi-column class constraints (Unique and Indexes).
       *  Single column constraints (NOT NULL) are displayed along with
       *  the attributes.
       */
      info->constraints = NULL;	/* initialize */
      if (class_->constraints != NULL)
	{
	  SM_CLASS_CONSTRAINT *c;

	  count = 0;
	  for (c = class_->constraints; c; c = c->next)
	    {
	      if (SM_IS_CONSTRAINT_INDEX_FAMILY (c->type))
		{
		  if (c->attributes[0] != NULL
		      && c->attributes[0]->class_mop == op)
		    {
		      count++;
		    }
		}
	    }

	  if (count > 0)
	    {
	      strs = (char **) malloc (sizeof (char *) * (count + 1));
	      if (strs == NULL)
		{
		  goto error_exit;
		}

	      i = 0;
	      for (c = class_->constraints; c; c = c->next)
		{
		  if (SM_IS_CONSTRAINT_INDEX_FAMILY (c->type))
		    {
		      if (c->attributes[0] != NULL
			  && c->attributes[0]->class_mop == op)
			{
			  description = obj_print_describe_constraint (parser,
								       class_,
								       c,
								       prt_type);
			  strs[i] = obj_print_copy_string (description);
			  if (strs[i] == NULL)
			    {
			      info->constraints = strs;
			      goto error_exit;
			    }
			  i++;
			}
		    }
		}
	      strs[i] = NULL;
	      info->constraints = strs;
	    }
	}

    }

  parser_free_parser (parser);
  parser = NULL;		/* Remember, it's a global! */
  return info;

error_exit:
  if (info)
    {
      obj_print_help_free_class (info);
    }
  if (parser)
    {
      parser_free_parser (parser);
      parser = NULL;		/* Remember, it's a global! */
    }

  return NULL;
}

/*
 * obj_print_help_class_name () - Creates a class help structure for the named class.
 *   return:  class help structure
 *   name(in): class name
 *
 * Note:
 *    Must free the class help structure with obj_print_help_free_class() when
 *    finished.
 */

CLASS_HELP *
obj_print_help_class_name (const char *name)
{
  CLASS_HELP *help = NULL;
  DB_OBJECT *class_;

  /* look up class in all schema's  */
  class_ = sm_find_class (name);

  if (class_ != NULL)
    {
      help = obj_print_help_class (class_, OBJ_PRINT_RSQL_SCHEMA_COMMAND);
    }

  return help;
}

/* INSTANCE HELP */

/*
 * obj_print_make_obj_help () - Create an empty instance help structure
 *   return: instance help structure
 */

static OBJ_HELP *
obj_print_make_obj_help (void)
{
  OBJ_HELP *new_p;

  new_p = (OBJ_HELP *) malloc (sizeof (OBJ_HELP));
  if (new_p != NULL)
    {
      new_p->classname = NULL;
      new_p->oid = NULL;
      new_p->attributes = NULL;
    }
  return new_p;
}

/*
 * help_free_obj () - Frees an instance help structure that was built
 *                    by help_obj()
 *   return:
 *   info(in): instance help structure
 */

void
help_free_obj (OBJ_HELP * info)
{
  if (info != NULL)
    {
      free_and_init (info->classname);
      free_and_init (info->oid);
      obj_print_free_strarray (info->attributes);
      free_and_init (info);
    }
}

/*
 * help_obj () - Builds an instance help structure containing a textual
 *               description of the instance.
 *   return: instance help structure
 *   op(in): instance object
 *
 * Note :
 *    The structure must be freed with help_free_obj() when finished.
 */

OBJ_HELP *
help_obj (MOP op)
{
  int error;
  SM_CLASS *class_;
  SM_ATTRIBUTE *attribute_p;
  char *obj;
  int i, count;
  OBJ_HELP *info = NULL;
  char **strs;
  char temp_buffer[SM_MAX_IDENTIFIER_LENGTH + 4];	/* Include room for _=_\0 */
  int pin;
  DB_VALUE value;
  PARSER_VARCHAR *buffer;

  if (parser == NULL)
    {
      parser = parser_create_parser ();
    }
  if (parser == NULL)
    {
      goto error_exit;
    }

  buffer = NULL;

  if (op == NULL || locator_is_class (op))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
      goto error_exit;
    }
  else
    {
      error = au_fetch_instance (op, &obj, S_LOCK, AU_SELECT);
      if (error == NO_ERROR)
	{
	  pin = ws_pin (op, 1);
	  error = au_fetch_class (op->class_mop, &class_, S_LOCK, AU_SELECT);
	  if (error == NO_ERROR)
	    {

	      info = obj_print_make_obj_help ();
	      if (info == NULL)
		{
		  goto error_exit;
		}
	      info->classname =
		obj_print_copy_string ((char *) class_->header.name);

	      DB_MAKE_OBJECT (&value, op);
	      buffer =
		pt_append_varchar (parser, buffer,
				   describe_data (parser, buffer, &value));
	      db_value_clear (&value);
	      DB_MAKE_NULL (&value);

	      info->oid =
		obj_print_copy_string ((char *)
				       pt_get_varchar_bytes (buffer));

	      if (class_->ordered_attributes != NULL)
		{
		  count = class_->att_count + 1;
		  strs = (char **) malloc (sizeof (char *) * count);
		  if (strs == NULL)
		    {
		      goto error_exit;
		    }
		  i = 0;
		  for (attribute_p = class_->ordered_attributes;
		       attribute_p != NULL;
		       attribute_p = attribute_p->order_link)
		    {
		      sprintf (temp_buffer, "%20s = ", attribute_p->name);
		      /*
		       * We're starting a new line here, so we don't
		       * want to append to the old buffer; pass NULL
		       * to pt_append_nulstring so that we start a new
		       * string.
		       */
		      buffer =
			pt_append_nulstring (parser, NULL, temp_buffer);
		      obj_get (op, attribute_p->name, &value);
		      buffer = describe_value (parser, buffer, &value);
		      strs[i] =
			obj_print_copy_string ((char *)
					       pt_get_varchar_bytes (buffer));
		      i++;
		    }
		  strs[i] = NULL;
		  info->attributes = strs;
		}

	      /* will we ever want to separate these lists ? */
	    }
	  (void) ws_pin (op, pin);
	}
    }
  parser_free_parser (parser);
  parser = NULL;
  return info;

error_exit:
  if (info)
    {
      help_free_obj (info);
    }
  if (parser)
    {
      parser_free_parser (parser);
      parser = NULL;
    }
  return NULL;
}

/* HELP PRINTING */
/* These functions build help structures and print them to a file. */

/*
 * help_fprint_obj () - Prints the description of a class or instance object
 *                      to the file.
 *   return: none
 *   fp(in):file pointer
 *   obj(in):class or instance to describe
 */

void
help_fprint_obj (FILE * fp, MOP obj)
{
  CLASS_HELP *cinfo;
  OBJ_HELP *oinfo;
  int i;

  if (locator_is_class (obj))
    {
      if (locator_is_root (obj))
	{
	  fprintf (fp, msgcat_message (MSGCAT_CATALOG_RYE,
				       MSGCAT_SET_HELP,
				       MSGCAT_HELP_ROOTCLASS_TITLE));
	}
      else
	{
	  cinfo = obj_print_help_class (obj, OBJ_PRINT_RSQL_SCHEMA_COMMAND);
	  if (cinfo != NULL)
	    {
	      fprintf (fp, msgcat_message (MSGCAT_CATALOG_RYE,
					   MSGCAT_SET_HELP,
					   MSGCAT_HELP_CLASS_TITLE),
		       cinfo->class_type, cinfo->name);
	      if (cinfo->attributes != NULL)
		{
		  fprintf (fp, msgcat_message (MSGCAT_CATALOG_RYE,
					       MSGCAT_SET_HELP,
					       MSGCAT_HELP_ATTRIBUTES));
		  for (i = 0; cinfo->attributes[i] != NULL; i++)
		    {
		      fprintf (fp, "  %s\n", cinfo->attributes[i]);
		    }
		}
	      if (cinfo->query_spec != NULL)
		{
		  fprintf (fp, msgcat_message (MSGCAT_CATALOG_RYE,
					       MSGCAT_SET_HELP,
					       MSGCAT_HELP_QUERY_SPEC));
		  for (i = 0; cinfo->query_spec[i] != NULL; i++)
		    {
		      fprintf (fp, "  %s\n", cinfo->query_spec[i]);
		    }
		}

	      obj_print_help_free_class (cinfo);
	    }
	}
    }
  else
    {
      oinfo = help_obj (obj);
      if (oinfo != NULL)
	{
	  fprintf (fp, msgcat_message (MSGCAT_CATALOG_RYE,
				       MSGCAT_SET_HELP,
				       MSGCAT_HELP_OBJECT_TITLE),
		   oinfo->classname);
	  if (oinfo->attributes != NULL)
	    {
	      for (i = 0; oinfo->attributes[i] != NULL; i++)
		{
		  fprintf (fp, "%s\n", oinfo->attributes[i]);
		}
	    }
	  help_free_obj (oinfo);
	}
    }
}

/* CLASS LIST HELP */

/*
 * help_class_names () - Returns an array containing the names of
 *                       all classes in the system.
 *   return: array of name strings
 *   qualifier(in):
 *
 *  Note :
 *    The array must be freed with help_free_class_names().
 */

char **
help_class_names (const char *qualifier)
{
  DB_OBJLIST *mops, *m;
  char **names, *tmp;
  const char *cname;
  int count, i, outcount;
  DB_OBJECT *requested_owner, *owner;
  char buffer[2 * DB_MAX_IDENTIFIER_LENGTH + 4];
  DB_VALUE owner_name;

  requested_owner = NULL;
  owner = NULL;
  if (qualifier && *qualifier && strcmp (qualifier, "*") != 0)
    {
      /* look up class in qualifiers' schema */
      requested_owner = db_find_user (qualifier);
      /* if this guy does not exist, it has no classes */
      if (!requested_owner)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_AU_INVALID_USER, 1, qualifier);
	  return NULL;
	}
    }

  names = NULL;
  mops = sm_fetch_all_classes (S_LOCK);

  count = ws_list_length ((DB_LIST *) mops);
  outcount = 0;
  if (count)
    {
      names = (char **) malloc (sizeof (char *) * (count + 1));
      if (names != NULL)
	{
	  for (i = 0, m = mops; i < count; i++, m = m->next)
	    {
	      owner = db_get_owner (m->op);
	      if (!requested_owner || requested_owner == owner)
		{
		  cname = sm_class_name (m->op);
		  buffer[0] = '\0';
		  if (!requested_owner
		      && obj_get (owner, "name", &owner_name) >= 0)
		    {
		      tmp = DB_GET_STRING (&owner_name);
		      if (tmp)
			{
			  snprintf (buffer, sizeof (buffer) - 1, "%s.%s",
				    tmp, cname);
			}
		      else
			{
			  snprintf (buffer, sizeof (buffer) - 1, "%s.%s",
				    "unknown_user", cname);
			}
		      db_value_clear (&owner_name);
		    }
		  else
		    {
		      snprintf (buffer, sizeof (buffer) - 1, "%s", cname);
		    }

		  names[outcount++] = obj_print_copy_string (buffer);
		}
	    }
	  names[outcount] = NULL;
	}
    }
  if (mops != NULL)
    {
      db_objlist_free (mops);
    }

  return names;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * help_base_class_names () - Returns an array containing the names of
 *                            all base classes in the system.
 *   return: array of name strings
 *  Note :
 *      A "base class" is a class that has no super classes.
 *      The array must be freed with help_free_class_names().
 */

char **
help_base_class_names (void)
{
  DB_OBJLIST *mops, *m;
  char **names;
  const char *cname;
  int count, i;

  names = NULL;
  mops = db_get_base_classes ();
  /* vector fetch as many as possible */
  (void) db_fetch_list (mops, S_LOCK, 0);

  count = ws_list_length ((DB_LIST *) mops);
  if (count)
    {
      names = (char **) malloc (sizeof (char *) * (count + 1));
      if (names != NULL)
	{
	  for (i = 0, m = mops; i < count; i++, m = m->next)
	    {
	      cname = sm_class_name (m->op);
	      names[i] = obj_print_copy_string ((char *) cname);
	    }
	  names[count] = NULL;
	}
    }
  if (mops != NULL)
    {
      db_objlist_free (mops);
    }

  return names;
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * help_free_names () - Frees an array of class names built by
 *                      help_class_names() or help_base_class_names().
 *   return: class name array
 *   names(in): class name array
 */

void
help_free_names (char **names)
{
  if (names != NULL)
    {
      obj_print_free_strarray (names);
    }
}

/*
 * backward compatibility, should be using help_free_names() for all
 * name arrays.
 */

/*
 * help_free_class_names () -
 *   return: none
 *   names(in):
 */

void
help_free_class_names (char **names)
{
  help_free_names (names);
}

/*
 * help_fprint_class_names () - Prints the names of all classes
 *                              in the system to a file.
 *   return: none
 *   fp(in): file pointer
 *   qualifier(in):
 */

void
help_fprint_class_names (FILE * fp, const char *qualifier)
{
  char **names;
  int i;

  names = help_class_names (qualifier);
  if (names != NULL)
    {
      for (i = 0; names[i] != NULL; i++)
	{
	  fprintf (fp, "%s\n", names[i]);
	}
      help_free_class_names (names);
    }
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * help_print_class_names () - Prints the names of all classes
 *                             in the system to stdout.
 *   return: none
 *   qualifier(in):
 */

void
help_print_class_names (const char *qualifier)
{
  help_fprint_class_names (stdout, qualifier);
}
#endif /* ENABLE_UNUSED_FUNCTION */


/* MISC HELP FUNCTIONS */

/*
 * help_describe_mop () - This writes a description of the MOP
 *                        to the given buffer.
 *   return:  number of characters in the description
 *   obj(in): object pointer to describe
 *   buffer(in): buffer to contain the description
 *   maxlen(in): length of the buffer
 *
 * Note :
 *    Used to get a printed representation of a MOP.
 *    This should only be used in special cases since OID's aren't
 *    supposed to be visible.
 */

int
help_describe_mop (DB_OBJECT * obj, char *buffer, int maxlen)
{
  SM_CLASS *class_;
  char oidbuffer[64];		/* three integers, better be big enough */
  int required, total;

  total = 0;
  if ((buffer != NULL) && (obj != NULL) && (maxlen > 0))
    {
      if (au_fetch_class (obj, &class_, S_LOCK, AU_SELECT) == NO_ERROR)
	{
	  sprintf (oidbuffer, "%ld.%ld.%ld",
		   (DB_C_LONG) WS_OID (obj)->volid,
		   (DB_C_LONG) WS_OID (obj)->pageid,
		   (DB_C_LONG) WS_OID (obj)->slotid);

	  required = strlen (oidbuffer) + strlen (class_->header.name) + 2;
	  if (locator_is_class (obj))
	    {
	      required++;
	      if (maxlen >= required)
		{
		  sprintf (buffer, "*%s:%s", class_->header.name, oidbuffer);
		  total = required;
		}
	    }
	  else if (maxlen >= required)
	    {
	      sprintf (buffer, "%s:%s", class_->header.name, oidbuffer);
	      total = required;
	    }
	}
    }
  return total;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * help_fprint_all_classes () - Describe all classes in the system.
 *   return: none
 *   fp(in): file pointer
 *
 * Note:
 *    This should only be used for debugging and testing.
 *    It is not intended to be used by the API.
 */

void
help_fprint_all_classes (FILE * fp)
{
  LIST_MOPS *lmops;
  int i;

  if (au_check_user () == NO_ERROR)
    {
      lmops = locator_get_all_mops (sm_Root_class_mop, DB_FETCH_QUERY_READ);
      if (lmops != NULL)
	{
	  for (i = 0; i < lmops->num; i++)
	    {
	      if (!WS_MARKED_DELETED (lmops->mops[i]))
		{
		  help_fprint_obj (fp, lmops->mops[i]);
		}
	    }
	  locator_free_list_mops (lmops);
	}
    }
}

/*
 * help_fprint_resident_instances () - Describe all resident instances of
 *                                     a class.
 *   return: none
 *   fp(in): file
 *   op(in): class object
 *
 * Note:
 *    Describe all resident instances of a class.
 *    Should only be used for testing purposes.  Not intended to be
 *    called by the API.
 */

void
help_fprint_resident_instances (FILE * fp, MOP op)
{
  MOP classmop = NULL;
  SM_CLASS *class_;
  LIST_MOPS *lmops;
  int i;

  if (locator_is_class (op, DB_FETCH_QUERY_READ))
    {
      if (!WS_MARKED_DELETED (op))
	classmop = op;
    }
  else
    {
      if (au_fetch_class (op, &class_, S_LOCK, AU_SELECT) == NO_ERROR)
	{
	  classmop = op->class_mop;
	}
    }

  if (classmop != NULL)
    {
      /* cause the mops to be loaded into the workspace */
      lmops = locator_get_all_mops (classmop, DB_FETCH_QUERY_READ);
      if (lmops != NULL)
	{
	  for (i = 0; i < lmops->num; i++)
	    {
	      if (!WS_MARKED_DELETED (lmops->mops[i]))
		{
		  help_fprint_obj (fp, lmops->mops[i]);
		}
	    }
	  locator_free_list_mops (lmops);
	}
    }
}
#endif /* ENABLE_UNUSED_FUNCTION */

/* GENERAL INFO */

/*
 * This is used to dump random information about the database
 * to the standard output device.  The information requested
 * comes in as a string that is "parsed" to determine the nature
 * of the request.  This is intended primarily as a backdoor
 * for the "info" method on the root class.  This allows us
 * to get information dumped to stdout from batch SQL/X
 * files which isn't possible currently since session commands
 * aren't allowed.
 *
 * The recognized commands are:
 *
 *   schema		display the names of all classes (;schema)
 *   schema foo		display the definition of class foo (;schema foo)
 *   workspace		dump the workspace statistics
 */

/*
 * Little tokenizing hack for help_display_info.
 */

/*
 * obj_print_next_token () -
 *   return: char *
 *   ptr(in):
 *   buffer(in):
 */

static char *
obj_print_next_token (char *ptr, char *buffer)
{
  char *p;

  p = ptr;
  while (char_isspace ((DB_C_INT) * p) && *p != '\0')
    {
      p++;
    }
  while (!char_isspace ((DB_C_INT) * p) && *p != '\0')
    {
      *buffer = *p;
      buffer++;
      p++;
    }
  *buffer = '\0';

  return p;
}

/*
 * help_print_info () -
 *   return: none
 *   command(in):
 *   fpp(in):
 */

void
help_print_info (const char *command, FILE * fpp)
{
  char buffer[128];
  char *ptr;
  DB_OBJECT *class_mop;

  if (command == NULL)
    {
      return;
    }

  ptr = obj_print_next_token ((char *) command, buffer);
  if (fpp == NULL)
    {
      fpp = stdout;
    }

  if (MATCH_TOKEN (buffer, "schema"))
    {
      ptr = obj_print_next_token (ptr, buffer);
      if (!strlen (buffer))
	{
	  help_fprint_class_names (fpp, NULL);
	}
      else
	{
	  class_mop = sm_find_class (buffer);
	  if (class_mop != NULL)
	    {
	      help_fprint_obj (fpp, class_mop);
	    }
	}
    }
  else if (MATCH_TOKEN (buffer, "workspace"))
    {
      ws_dump (fpp);
    }
  else if (MATCH_TOKEN (buffer, "lock"))
    {
      lock_dump (fpp);
    }
  else if (MATCH_TOKEN (buffer, "stats"))
    {
      ptr = obj_print_next_token (ptr, buffer);
      if (!strlen (buffer))
	{
	  fprintf (fpp, "Info stats class-name\n");
	}
      else
	{
	  stats_dump (buffer, fpp);
	}
    }
  else if (MATCH_TOKEN (buffer, "logstat"))
    {
      log_dump_stat (fpp);
    }
  else if (MATCH_TOKEN (buffer, "csstat"))
    {
      thread_dump_cs_stat (fpp);
    }
  else if (MATCH_TOKEN (buffer, "plan"))
    {
      qmgr_dump_query_plans (fpp);
    }
  else if (MATCH_TOKEN (buffer, "trantable"))
    {
      logtb_dump_trantable (fpp);
    }
  else if (MATCH_TOKEN (buffer, "serverstats"))
    {
      thread_dump_server_stat (fpp);
    }
}
#endif /* ! SERVER_MODE */

/*
 * describe_set() - Print a description of the set
 *                  as null-terminated string
 *   return: printed string (buffer pointer)
 *   parser(in) : parser context
 *   buffer(in/out) : buffer to place the printed string
 *   set(in) :
 */
static PARSER_VARCHAR *
describe_set (const PARSER_CONTEXT * parser, PARSER_VARCHAR * buffer,
	      const DB_SET * set)
{
  DB_VALUE value;
  int size, end, i;

  assert (parser != NULL && set != NULL);

  buffer = pt_append_nulstring (parser, buffer, "{");
  size = set_size ((DB_COLLECTION *) set);
  if (help_Max_set_elements == 0 || help_Max_set_elements > size)
    {
      end = size;
    }
  else
    {
      end = help_Max_set_elements;
    }

  for (i = 0; i < end; ++i)
    {
      set_get_element ((DB_COLLECTION *) set, i, &value);

      buffer = describe_value (parser, buffer, &value);

      db_value_clear (&value);
      if (i < size - 1)
	{
	  buffer = pt_append_nulstring (parser, buffer, ", ");
	}
    }
  if (i < size)
    {
      buffer = pt_append_nulstring (parser, buffer, ". . .");
    }

  buffer = pt_append_nulstring (parser, buffer, "}");
  return buffer;
}

/*
 * describe_double() - Print a description of the double value
 *                     as null-terminated string
 *   return: printed string (buffer pointer)
 *   parser(in) : parser context
 *   buffer(in/out) : buffer to place the printed string
 *   value(in) :
 */
static PARSER_VARCHAR *
describe_double (const PARSER_CONTEXT * parser, PARSER_VARCHAR * buffer,
		 const double value)
{
  char tbuf[24];

  assert (parser != NULL);

  OBJ_SPRINT_DB_DOUBLE (tbuf, value);

  if (strstr (tbuf, "Inf"))
    {
      OBJ_SPRINT_DB_DOUBLE (tbuf, (value > 0 ? DBL_MAX : -DBL_MAX));
    }

  buffer = pt_append_nulstring (parser, buffer, tbuf);

  return buffer;
}

/*
 * describe_bit_string() - Print a description of the bit value
 *                         as null-terminated string
 *   return: printed string (buffer pointer)
 *   parser(in) : parser context
 *   buffer(in/out) : buffer to place the printed string
 *   value(in) : a DB_VALUE of type DB_TYPE_VARBIT
 */
static PARSER_VARCHAR *
describe_bit_string (const PARSER_CONTEXT * parser, PARSER_VARCHAR * buffer,
		     const DB_VALUE * value)
{
  unsigned char *bstring;
  int nibble_length, nibbles, count;
  char tbuf[10];

  assert (parser != NULL && value != NULL);

  bstring = (unsigned char *) db_get_string (value);
  if (bstring == NULL)
    {
      return NULL;
    }

  nibble_length = ((db_get_string_length (value) + 3) / 4);

  for (nibbles = 0, count = 0; nibbles < nibble_length - 1;
       count++, nibbles += 2)
    {
      sprintf (tbuf, "%02x", bstring[count]);
      tbuf[2] = '\0';
      buffer = pt_append_nulstring (parser, buffer, tbuf);
    }

  /* If we don't have a full byte on the end, print the nibble. */
  if (nibbles < nibble_length)
    {
      if (parser->custom_print & PT_PAD_BYTE)
	{
	  sprintf (tbuf, "%02x", bstring[count]);
	  tbuf[2] = '\0';
	}
      else
	{
	  sprintf (tbuf, "%1x", bstring[count]);
	  tbuf[1] = '\0';
	}
      buffer = pt_append_nulstring (parser, buffer, tbuf);
    }

  return buffer;
}

/*
 * describe_data() - Describes a DB_VALUE of primitive data type
 *   return: printed string (buffer pointer)
 *   parser(in) : parser context
 *   buffer(in/out) : buffer to place the printed string
 *   value(in) :
 */
PARSER_VARCHAR *
describe_data (const PARSER_CONTEXT * parser, PARSER_VARCHAR * buffer,
	       const DB_VALUE * value)
{
  OID *oid;
#if !defined(SERVER_MODE)
  DB_OBJECT *obj;
#endif
  DB_SET *set;
  char *src, *pos, *end;
  char line[1025];
  int length;

  assert (parser != NULL);

  if (DB_IS_NULL (value))
    {
      buffer = pt_append_nulstring (parser, buffer, "NULL");
    }
  else
    {
      switch (DB_VALUE_TYPE (value))
	{
	case DB_TYPE_INTEGER:
	  sprintf (line, "%d", DB_GET_INTEGER (value));
	  buffer = pt_append_nulstring (parser, buffer, line);
	  break;

	case DB_TYPE_BIGINT:
	  sprintf (line, "%lld", (long long) db_get_bigint (value));
	  buffer = pt_append_nulstring (parser, buffer, line);
	  break;

	case DB_TYPE_DOUBLE:
	  buffer = describe_double (parser, buffer, db_get_double (value));
	  break;

	case DB_TYPE_NUMERIC:
	  buffer = pt_append_nulstring (parser, buffer,
					numeric_db_value_print ((DB_VALUE *)
								value));
	  break;

	case DB_TYPE_VARBIT:
	  buffer = describe_bit_string (parser, buffer, value);
	  break;

	case DB_TYPE_VARCHAR:
	  /* Copy string into buf providing for any embedded quotes.
	   * Strings may have embedded NULL characters and embedded
	   * quotes.  None of the supported multibyte character codesets
	   * have a conflict between a quote character and the second byte
	   * of the multibyte character.
	   */
	  src = db_get_string (value);
	  end = src + db_get_string_size (value);
	  while (src < end)
	    {
	      /* Find the position of the next quote or the end of the string,
	       * whichever comes first.  This loop is done in place of
	       * strchr in case the string has an embedded NULL.
	       */
	      for (pos = src; pos && pos < end && (*pos) != '\''; pos++)
		;

	      /* If pos < end, then a quote was found.  If so, copy the partial
	       * buffer and duplicate the quote
	       */
	      if (pos < end)
		{
		  length = CAST_STRLEN (pos - src + 1);
		  buffer = pt_append_bytes (parser, buffer, src, length);
		  buffer = pt_append_nulstring (parser, buffer, "'");
		}
	      /* If not, copy the remaining part of the buffer */
	      else
		{
		  buffer =
		    pt_append_bytes (parser, buffer, src,
				     CAST_STRLEN (end - src));
		}

	      /* advance src to just beyond the point where we left off */
	      src = pos + 1;
	    }
	  break;

	case DB_TYPE_OBJECT:
#if !defined(SERVER_MODE)
	  obj = db_get_object (value);
	  if (obj == NULL)
	    {
	      break;
	    }

	  oid = WS_OID (obj);
	  sprintf (line, "%d", (int) oid->volid);
	  buffer = pt_append_nulstring (parser, buffer, line);
	  buffer = pt_append_nulstring (parser, buffer, "|");
	  sprintf (line, "%d", (int) oid->pageid);
	  buffer = pt_append_nulstring (parser, buffer, line);
	  buffer = pt_append_nulstring (parser, buffer, "|");
	  sprintf (line, "%d", (int) oid->slotid);
	  buffer = pt_append_nulstring (parser, buffer, line);
	  break;
	  /* If we are on the server, fall thru to the oid case
	   * The value is probably nonsense, but that is safe to do.
	   * This case should simply not occur.
	   */
#endif

	case DB_TYPE_OID:
	  oid = (OID *) db_get_oid (value);
	  if (oid == NULL)
	    {
	      break;
	    }

	  sprintf (line, "%d", (int) oid->volid);
	  buffer = pt_append_nulstring (parser, buffer, line);
	  buffer = pt_append_nulstring (parser, buffer, "|");
	  sprintf (line, "%d", (int) oid->pageid);
	  buffer = pt_append_nulstring (parser, buffer, line);
	  buffer = pt_append_nulstring (parser, buffer, "|");
	  sprintf (line, "%d", (int) oid->slotid);
	  buffer = pt_append_nulstring (parser, buffer, line);
	  break;

	case DB_TYPE_SEQUENCE:
	  set = db_get_set (value);
	  if (set != NULL)
	    {
	      return describe_set (parser, buffer, set);
	    }
	  else
	    {
	      buffer = pt_append_nulstring (parser, buffer, "NULL");
	    }

	  break;

	  /*
	   * This constant is necessary to fake out the db_?_to_string()
	   * routines that are expecting a buffer length.  Since we assume
	   * that our buffer is big enough in this code, just pass something
	   * that ought to work for every case.
	   */
#define TOO_BIG_TO_MATTER       1024

	case DB_TYPE_TIME:
	  (void) db_time_to_string (line, TOO_BIG_TO_MATTER,
				    db_get_time (value));
	  buffer = pt_append_nulstring (parser, buffer, line);
	  break;

	case DB_TYPE_DATETIME:
	  (void) db_datetime_to_string (line, TOO_BIG_TO_MATTER,
					DB_GET_DATETIME (value));
	  buffer = pt_append_nulstring (parser, buffer, line);
	  break;

	case DB_TYPE_DATE:
	  (void) db_date_to_string (line, TOO_BIG_TO_MATTER,
				    db_get_date (value));
	  buffer = pt_append_nulstring (parser, buffer, line);
	  break;

	case DB_TYPE_NULL:
	  /* Can't get here because the DB_IS_NULL test covers DB_TYPE_NULL */
	  break;

	case DB_TYPE_VARIABLE:
	case DB_TYPE_SUB:
	  /* make sure line is NULL terminated, may not be necessary
	     line[0] = '\0';
	   */
	  break;

	default:
	  /* NB: THERE MUST BE NO DEFAULT CASE HERE. ALL TYPES MUST BE HANDLED! */
	  assert (false);
	  break;
	}
    }

  return buffer;
}

/*
 * describe_value() - Describes the contents of a DB_VALUE container
 *   return: printed string (buffer pointer)
 *   parser(in) : parser context
 *   buffer(in/out) : buffer to place the printed string
 *   value(in) : value to describe
 *
 * Note :
 *    Prints a SQL syntactically correct representation of the value.
 *    (assuming one exists )
 */
PARSER_VARCHAR *
describe_value (const PARSER_CONTEXT * parser, PARSER_VARCHAR * buffer,
		const DB_VALUE * value)
{
  assert (parser != NULL);

  if (DB_IS_NULL (value))
    {
      buffer = pt_append_nulstring (parser, buffer, "NULL");
    }
  else
    {
      /* add some extra info to the basic data value */
      switch (DB_VALUE_TYPE (value))
	{
	case DB_TYPE_VARCHAR:
	  buffer = pt_append_nulstring (parser, buffer, "'");
	  buffer = describe_data (parser, buffer, value);
	  buffer = pt_append_nulstring (parser, buffer, "'");
	  break;

	case DB_TYPE_DATE:
	  buffer = pt_append_nulstring (parser, buffer, "date '");
	  buffer = describe_data (parser, buffer, value);
	  buffer = pt_append_nulstring (parser, buffer, "'");
	  break;

	case DB_TYPE_TIME:
	  buffer = pt_append_nulstring (parser, buffer, "time '");
	  buffer = describe_data (parser, buffer, value);
	  buffer = pt_append_nulstring (parser, buffer, "'");
	  break;

	case DB_TYPE_DATETIME:
	  buffer = pt_append_nulstring (parser, buffer, "datetime '");
	  buffer = describe_data (parser, buffer, value);
	  buffer = pt_append_nulstring (parser, buffer, "'");
	  break;

	case DB_TYPE_VARBIT:
	  buffer = pt_append_nulstring (parser, buffer, "X'");
	  buffer = describe_data (parser, buffer, value);
	  buffer = pt_append_nulstring (parser, buffer, "'");
	  break;

	default:
	  buffer = describe_data (parser, buffer, value);
	  break;
	}
    }

  return buffer;
}

/*
 * describe_bit_string() -
 *   return: printed string (buffer pointer)
 *   parser(in) : parser context
 *   buffer(in/out) : buffer to place the printed string
 *   str(in) :
 *   str_len(in) :
 *   max_token_length(in) :
 */
PARSER_VARCHAR *
describe_string (const PARSER_CONTEXT * parser, PARSER_VARCHAR * buffer,
		 const char *str, size_t str_length, int max_token_length)
{
  const char *src, *end, *pos;
  int token_length, length;
  const char *delimiter = "'+\n '";

  src = str;
  end = src + str_length;

  /* get current buffer length */
  if (buffer == NULL)
    {
      token_length = 0;
    }
  else
    {
      token_length = buffer->length % (max_token_length + strlen (delimiter));
    }
  for (pos = src; pos < end; pos++, token_length++)
    {
      /* Process the case (*pos == '\'') first.
       * Don't break the string in the middle of internal quotes('') */
      if (*pos == '\'')
	{			/* put '\'' */
	  length = CAST_STRLEN (pos - src + 1);
	  buffer = pt_append_bytes (parser, buffer, src, length);
	  buffer = pt_append_nulstring (parser, buffer, "'");
	  token_length += 1;	/* for appended '\'' */

	  src = pos + 1;	/* advance src pointer */
	}
      else if (token_length > max_token_length)
	{			/* long string */
	  length = CAST_STRLEN (pos - src + 1);
	  buffer = pt_append_bytes (parser, buffer, src, length);
	  buffer = pt_append_nulstring (parser, buffer, delimiter);
	  token_length = 0;	/* reset token_len for the next new token */

	  src = pos + 1;	/* advance src pointer */
	}
    }

  /* dump the remainings */
  length = CAST_STRLEN (pos - src);
  buffer = pt_append_bytes (parser, buffer, src, length);

  return buffer;
}

/*
 * help_fprint_value() -  Prints a description of the contents of a DB_VALUE
 *                        to the file
 *   return: none
 *   fp(in) : FILE stream pointer
 *   value(in) : value to print
 */

void
help_fprint_value (FILE * fp, const DB_VALUE * value)
{
  PARSER_VARCHAR *buffer;
  PARSER_CONTEXT *parser;

  parser = parser_create_parser ();
  if (parser == NULL)
    {
      return;
    }

  buffer = describe_value (parser, NULL, value);
  fprintf (fp, "%.*s", (int) pt_get_varchar_length (buffer),
	   pt_get_varchar_bytes (buffer));
  parser_free_parser (parser);
}

/*
 * help_sprint_value() - This places a printed representation of the supplied
 *                       value in a buffer.
 *   return: number of characters in description
 *   value(in) : value to describe
 *   buffer(in/out) : buffer to contain description
 *   max_length(in) : maximum chars in buffer
 *
 *  NOTE:
 *   This entire module needs to be much more careful about
 *   overflowing the internal "linebuf" buffer when using long
 *   strings.
 *   If the description will fit within the buffer, the number of characters
 *   used is returned, otherwise, -1 is returned.
 */
int
help_sprint_value (const DB_VALUE * value, char *buffer, int max_length)
{
  int length;
  PARSER_VARCHAR *buf;
  PARSER_CONTEXT *parser;

  parser = parser_create_parser ();
  if (parser == NULL)
    {
      return 0;
    }

  buf = pt_append_nulstring (parser, NULL, "");
  buf = describe_value (parser, buf, value);
  length = pt_get_varchar_length (buf);
  if (length < max_length)
    {
      memcpy (buffer, (char *) pt_get_varchar_bytes (buf), length);
      buffer[length] = 0;
    }
  else
    {
      length = -length;
    }

  parser_free_parser (parser);

  return length;
}

/*
 * describe_idxkey() - Describes the contents of a DB_IDXKEY container
 *   return: printed string (buffer pointer)
 *   parser(in) : parser context
 *   buffer(in/out) : buffer to place the printed string
 *   key(in) : value to describe
 *
 * Note :
 *    Prints a SQL syntactically correct representation of the value.
 *    (assuming one exists )
 */
PARSER_VARCHAR *
describe_idxkey (const PARSER_CONTEXT * parser, PARSER_VARCHAR * buffer,
		 const DB_IDXKEY * key)
{
  int i;

  assert (parser != NULL);
  assert (key != NULL);

  if (DB_IDXKEY_IS_NULL (key))
    {
      buffer = pt_append_nulstring (parser, buffer, "NULL");
    }
  else
    {
      buffer = pt_append_nulstring (parser, buffer, "{");

      for (i = 0; i < key->size; i++)
	{
	  buffer = describe_value (parser, buffer, &(key->vals[i]));

	  if (i < key->size - 1)
	    {
	      buffer = pt_append_nulstring (parser, buffer, ", ");
	    }
	}

      buffer = pt_append_nulstring (parser, buffer, "}");
    }

  return buffer;
}

/*
 * help_fprint_idxkey() -  Prints a description of the contents of a DB_IDXKEY
 *                        to the file
 *   return: none
 *   fp(in) : FILE stream pointer
 *   key(in) : idxkey to print
 */

void
help_fprint_idxkey (FILE * fp, const DB_IDXKEY * key)
{
  PARSER_VARCHAR *buffer;
  PARSER_CONTEXT *parser;

  parser = parser_create_parser ();
  if (parser == NULL)
    {
      return;
    }

  buffer = describe_idxkey (parser, NULL, key);
  fprintf (fp, "%.*s", (int) pt_get_varchar_length (buffer),
	   pt_get_varchar_bytes (buffer));
  parser_free_parser (parser);
}

/*
 * help_sprint_idxkey() - This places a printed representation of the supplied
 *                       idxkey in a buffer.
 *   return: number of characters in description
 *   key(in) : idxkey to describe
 *   buffer(in/out) : buffer to contain description
 *   max_length(in) : maximum chars in buffer
 *
 *  NOTE:
 *   This entire module needs to be much more careful about
 *   overflowing the internal "linebuf" buffer when using long
 *   strings.
 *   If the description will fit within the buffer, the number of characters
 *   used is returned, otherwise, -1 is returned.
 */
int
help_sprint_idxkey (const DB_IDXKEY * key, char *buffer, int max_length)
{
  int length;
  PARSER_VARCHAR *buf;
  PARSER_CONTEXT *parser;

  parser = parser_create_parser ();
  if (parser == NULL)
    {
      return 0;
    }

  buf = pt_append_nulstring (parser, NULL, "");
  buf = describe_idxkey (parser, buf, key);
  length = pt_get_varchar_length (buf);
  if (length < max_length)
    {
      memcpy (buffer, pt_get_varchar_bytes (buf), length);
      buffer[length] = 0;
    }
  else
    {
      length = -length;
    }

  parser_free_parser (parser);

  return length;
}

#if defined(RYE_DEBUG)
/*
 * dbg_value() -  This is primarily for debugging
 *   return: a character string representation of the db_value
 *   value(in) : value to describe
 */

char *
dbg_value (const DB_VALUE * value)
{
  PARSER_VARCHAR *buffer;
  PARSER_CONTEXT *parser;
  char *ret;

  parser = parser_create_parser ();
  if (parser == NULL)
    {
      return 0;
    }

  buffer = pt_append_nulstring (parser, NULL, "");
  buffer = describe_value (parser, buffer, value);
  ret = (char *) pt_get_varchar_bytes (buffer);
  parser_free_parser (parser);

  return ret;
}
#endif
