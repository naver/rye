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
 * execute_schema.c
 */

#ident "$Id$"

#include "config.h"

#include <stdarg.h>
#include <ctype.h>

#include "error_manager.h"
#include "parser.h"
#include "parser_message.h"
#include "db.h"
#include "dbi.h"
#include "semantic_check.h"
#include "execute_schema.h"
#include "execute_statement.h"
#include "schema_manager.h"
#include "transaction_cl.h"
#include "system_parameter.h"
#include "semantic_check.h"
#include "xasl_generation.h"
#include "memory_alloc.h"
#include "transform.h"
#include "set_object.h"
#include "object_accessor.h"
#include "memory_hash.h"
#include "locator_cl.h"
#include "xasl_support.h"
#include "network_interface_cl.h"
#include "view_transform.h"

/* this must be the last header file included!!! */
#include "dbval.h"

#define QUERY_MAX_SIZE	1024 * 1024

typedef enum
{
  DO_INDEX_CREATE, DO_INDEX_DROP
} DO_INDEX;

typedef enum
{
  SM_ATTR_CHG_UNKNOWN = -1,
  SM_ATTR_CHG_NOT_NEEDED = 1,
  SM_ATTR_CHG_ONLY_SCHEMA = 2,
  SM_ATTR_CHG_ADD_CONSTRAINT = 3,
  SM_ATTR_UPG_CONSTRAINT = 4
} SM_ATTR_CHG_SOL;

/* The ATT_CHG_XXX enum bit flags describe the status of an attribute specific
 * property (sm_attr_properties_chg). Each property is initialized with
 * 'ATT_CHG_PROPERTY_NOT_CHECKED' value, and keeps it until is marked as
 * checked (by setting to zero) and then set corresponding bits.
 * '_OLD' and '_NEW' flags track simple presence of property in the attribute
 * existing schema and new definition, while the upper values flags are set
 * upon more cross-checkings. Some flags applies only to certain properties
 * (like '.._TYPE_..' for domain of attribute)
 * !! Values in enum should be kept in this order as some internal checks
 * rely on the order !!*/
enum
{
  /* property present in existing schema */
  ATT_CHG_PROPERTY_PRESENT_OLD = 0x1,
  /* property present in new attribute definition */
  ATT_CHG_PROPERTY_PRESENT_NEW = 0x2,
  /* present in OLD , lost in NEW */
  ATT_CHG_PROPERTY_LOST = 0x4,
  /* not present in OLD , gained in NEW */
  ATT_CHG_PROPERTY_GAINED = 0x8,
  /* property is not changed (not present in both current schema or
   * new defition or present in both but not affected in any way) */
  ATT_CHG_PROPERTY_UNCHANGED = 0x10,
  /* property is changed (i.e.: both present in old an new , but different) */
  ATT_CHG_PROPERTY_DIFF = 0x20,
  /* type : precision increase : varchar(2) -> varchar (10) */
  ATT_CHG_TYPE_PREC_INCR = 0x100,
  /* type : for COLLECTION or OBJECT (Class) type : the new SET is more
   * general (the new OBJECT is a super-class) */
  ATT_CHG_TYPE_SET_CLS_COMPAT = 0x200,
  /* type : upgrade : int -> bigint */
  ATT_CHG_TYPE_UPGRADE = 0x400,
  /* type : changed, but needs checking if new domain supports all existing
   * values , i.e : int -> char(3) */
  ATT_CHG_TYPE_NEED_ROW_CHECK = 0x800,
  /* type : pseudo-upgrade : datetime -> time: this is succesful as a cast,
   * but may fail due to unique constraint*/
  ATT_CHG_TYPE_PSEUDO_UPGRADE = 0x1000,
  /* type : not supported with existing configuration */
  ATT_CHG_TYPE_NOT_SUPPORTED_WITH_CFG = 0x2000,
  /* type : upgrade : not supported */
  ATT_CHG_TYPE_NOT_SUPPORTED = 0x4000,
  /* property was not checked
   * needs to be the highest value in enum */
  ATT_CHG_PROPERTY_NOT_CHECKED = 0x10000
};

/* Enum to access array from 'sm_attr_properties_chg' struct
 */
enum
{
  P_NAME = 0,			/* name of attribute */
  P_NOT_NULL,			/* constraint NOT NULL */
  P_DEFAULT_VALUE,		/* DEFAULT VALUE of attribute */
  P_CONSTR_CHECK,		/* constraint CHECK */
  P_DEFFERABLE,			/* DEFFERABLE */
  P_ORDER,			/* ORDERING definition */
  P_S_CONSTR_PK,		/* constraint PRIMARY KEY only on one single column : the checked attribute */
  P_M_CONSTR_PK,		/* constraint PRIMARY KEY on more columns, including checked attribute */
  P_S_CONSTR_UNI,		/* constraint UNIQUE only on one single column : the checked attribute */
  P_M_CONSTR_UNI,		/* constraint UNIQUE on more columns, including checked attribute */
  P_CONSTR_NON_UNI,		/* has a non-unique index defined on it, should apply only
				 * for existing schema (as you cannot add a non-index with
				 * ALTER CHANGE)*/
  P_TYPE,			/* type (domain) change */
  NUM_ATT_CHG_PROP
};

/* sm_attr_properties_chg :
 * structure used for checking existing attribute definition (from schema)
 * and new attribute definition
 * Array is accessed using enum values define above.
 */
typedef struct sm_attr_properties_chg SM_ATTR_PROP_CHG;
struct sm_attr_properties_chg
{
  int p[NUM_ATT_CHG_PROP];	/* 'change' property */
  SM_CONSTRAINT_INFO *constr_info;
  SM_CONSTRAINT_INFO *new_constr_info;
  int att_id;
};

static int drop_class_name (const char *name);

static int do_alter_one_clause_with_template (PARSER_CONTEXT * parser,
					      PT_NODE * alter);
static int do_alter_one_add_attr_mthd (PARSER_CONTEXT * parser,
				       PT_NODE * alter);
static int do_alter_clause_rename_entity (PARSER_CONTEXT * const parser,
					  PT_NODE * const alter);
static int do_alter_clause_drop_index (PARSER_CONTEXT * const parser,
				       PT_NODE * const alter);

static int do_rename_internal (const char *const old_name,
			       const char *const new_name);
static DB_CONSTRAINT_TYPE get_unique_index_type (const bool is_unique);
static int create_or_drop_index_helper (PARSER_CONTEXT * parser,
					const char *const constraint_name,
					const bool is_unique,
					PT_NODE * spec,
					PT_NODE * column_names,
					DB_OBJECT * const obj,
					DO_INDEX do_index);

static int validate_attribute_domain (PARSER_CONTEXT * parser,
				      PT_NODE * attribute,
				      SM_CLASS_TYPE class_type);

static int add_query_to_virtual_class (PARSER_CONTEXT * parser,
				       DB_CTMPL * ctemplate,
				       const PT_NODE * queries);
#if defined (ENABLE_UNUSED_FUNCTION)
static int do_copy_indexes (PARSER_CONTEXT * parser, MOP classmop,
			    SM_CLASS * src_class);
#endif

static int do_alter_clause_change_attribute (PARSER_CONTEXT * const parser,
					     PT_NODE * const alter);

static int do_alter_change_owner (PARSER_CONTEXT * const parser,
				  PT_NODE * const alter);

static int do_change_att_schema_only (PARSER_CONTEXT * parser,
				      DB_CTMPL * ctemplate,
				      PT_NODE * attribute,
				      PT_NODE * old_name_node,
				      PT_NODE * constraints,
				      SM_ATTR_PROP_CHG * attr_chg_prop);

static int build_attr_change_map (PARSER_CONTEXT * parser,
				  SM_CLASS * sm_class,
				  PT_NODE * attr_def,
				  PT_NODE * attr_old_name,
				  PT_NODE * constraints,
				  SM_ATTR_PROP_CHG * attr_chg_properties);

static bool is_att_property_structure_checked (const SM_ATTR_PROP_CHG *
					       attr_chg_properties);

static bool is_att_change_needed (const SM_ATTR_PROP_CHG *
				  attr_chg_properties);

static void reset_att_property_structure (SM_ATTR_PROP_CHG *
					  attr_chg_properties);

static bool is_att_prop_set (const int prop, const int value);

static int get_att_order_from_def (PT_NODE * attribute, bool * ord_first,
				   const char **ord_after_name);

static int get_att_default_from_def (PARSER_CONTEXT * parser,
				     PT_NODE * attribute,
				     DB_VALUE ** default_value);
static int get_old_property_present_of_constraints (SM_ATTR_PROP_CHG *
						    attr_chg_properties,
						    SM_CLASS_CONSTRAINT *
						    cons,
						    const char *attr_name);
static int get_new_property_present_of_constraints (SM_ATTR_PROP_CHG *
						    attr_chg_properties,
						    PT_NODE * cons,
						    const char *attr_name);

static int do_update_new_notnull_cols_without_default (PARSER_CONTEXT *
						       parser,
						       PT_NODE * alter,
						       MOP class_mop);

static bool is_attribute_primary_key (const char *class_name,
				      const char *attr_name);

static int check_change_attribute (PARSER_CONTEXT * parser,
				   SM_CLASS * sm_class, PT_NODE * attribute,
				   PT_NODE * old_name_node,
				   PT_NODE * constraints,
				   SM_ATTR_PROP_CHG * attr_chg_prop,
				   SM_ATTR_CHG_SOL * change_mode);

static int sort_constr_info_list (SM_CONSTRAINT_INFO ** source);
static int save_constraint_info_from_pt_node (SM_CONSTRAINT_INFO ** save_info,
					      const PT_NODE *
					      const pt_constr);

static const char *get_attr_name (PT_NODE * attribute);
static int do_add_attribute (PARSER_CONTEXT * parser, DB_CTMPL * ctemplate,
			     PT_NODE * atts, PT_NODE * constraints,
			     bool error_on_not_normal, PT_NODE * shard_key);
static int do_create_local (PARSER_CONTEXT * parser, DB_CTMPL * ctemplate,
			    PT_NODE * pt_node);

/*
 * Function Group :
 * DO functions for alter statement
 *
 */

/*
 * do_alter_one_clause_with_template() - Executes the operations required by a
 *                                       single ALTER clause.
 *   return: Error code
 *   parser(in): Parser context
 *   alter(in/out): Parse tree of a single clause of an ALTER statement. Not
 *                  all possible clauses are handled by this function; see the
 *                  note below and the do_alter() function.
 *
 * Note: This function handles clauses that require class template operations.
 *       It always calls dbt_edit_class(). Other ALTER clauses might have
 *       dedicated processing functions. See do_alter() for details.
 */
static int
do_alter_one_clause_with_template (PARSER_CONTEXT * parser, PT_NODE * alter)
{
  const char *entity_name;
  const char *attr_mthd_name;
  const char *new_name, *old_name;
  DB_CTMPL *ctemplate = NULL;
  DB_OBJECT *vclass;
  int error = NO_ERROR;
  DB_ATTRIBUTE *found_attr;
  DB_VALUE src_val, dest_val;
  PT_NODE *p;
  const PT_ALTER_CODE alter_code = alter->info.alter.code;

  entity_name = alter->info.alter.entity_name->info.name.original;
  if (entity_name == NULL)
    {
      ERROR1 (error, ER_UNEXPECTED,
	      "Expecting a class or virtual class name.");
      return error;
    }

  vclass = sm_find_class (entity_name);
  if (vclass == NULL)
    {
      return er_errid ();
    }

  db_make_null (&src_val);
  db_make_null (&dest_val);

  ctemplate = dbt_edit_class (vclass);
  if (ctemplate == NULL)
    {
      /* when dbt_edit_class fails (e.g. because the server unilaterally
         aborts us), we must record the associated error message into the
         parser.  Otherwise, we may get a confusing error msg of the form:
         "so_and_so is not a class". */
      pt_record_error (parser, alter->line_number, alter->column_number,
		       er_msg (), NULL);
      return er_errid ();
    }

  switch (alter_code)
    {
    case PT_DROP_ATTR_MTHD:
      p = alter->info.alter.alter_clause.attr_mthd.attr_mthd_name_list;
      for (; p && p->node_type == PT_NAME; p = p->next)
	{
	  attr_mthd_name = p->info.name.original;

	  found_attr = db_get_attribute (vclass, attr_mthd_name);
	  if (found_attr)
	    {
	      error = dbt_drop_attribute (ctemplate, attr_mthd_name);
	    }

	  if (error != NO_ERROR)
	    {
	      dbt_abort_class (ctemplate);
	      return error;
	    }
	}
      break;

    case PT_RENAME_ATTR_MTHD:
      if (alter->info.alter.alter_clause.rename.old_name)
	{
	  old_name =
	    alter->info.alter.alter_clause.rename.old_name->info.name.
	    original;
	}
      else
	{
	  old_name = NULL;
	}

      new_name =
	alter->info.alter.alter_clause.rename.new_name->info.name.original;

      switch (alter->info.alter.alter_clause.rename.element_type)
	{
	case PT_ATTRIBUTE:
	  error = dbt_rename (ctemplate, old_name, 0, new_name);
	  break;

	default:
	  /* actually, it means that a wrong thing is being
	     renamed, and is really an error condition. */
	  assert (false);
	  break;
	}
      break;

    case PT_DROP_CONSTRAINT:
      {
	SM_CLASS_CONSTRAINT *cons = NULL;
	const char *constraint_name = NULL;

	assert (alter->info.alter.constraint_list->next == NULL);
	assert (alter->info.alter.constraint_list->node_type == PT_NAME);
	constraint_name =
	  alter->info.alter.constraint_list->info.name.original;
	assert (constraint_name != NULL);
	cons = classobj_find_class_index (ctemplate->current,
					  constraint_name);

	if (cons != NULL)
	  {
	    const DB_CONSTRAINT_TYPE constraint_type =
	      db_constraint_type (cons);

	    error = dbt_drop_constraint (ctemplate, constraint_type,
					 constraint_name, NULL);
	  }
	else
	  {
	    er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
		    ER_SM_CONSTRAINT_NOT_FOUND, 1, constraint_name);
	    error = er_errid ();
	  }
      }
      break;
    case PT_CHANGE_PK:
      {
	PT_NODE *name_node = NULL;
	const char *index_name = NULL;

	name_node = alter->info.alter.alter_clause.chg_pk.index_name;
	assert (name_node->node_type == PT_NAME);
	index_name = name_node->info.name.original;

	error = dbt_change_primary_key (ctemplate, index_name);
      }
      break;

    default:
      assert (false);
      dbt_abort_class (ctemplate);
      return error;
    }

  if (error != NO_ERROR)
    {
      dbt_abort_class (ctemplate);
      return error;
    }

  vclass = dbt_finish_class (ctemplate);

  /* the dbt_finish_class() failed, the template was not freed */
  if (vclass == NULL)
    {
      error = er_errid ();
      dbt_abort_class (ctemplate);
      return error;
    }

  if (alter_code == PT_ADD_ATTR_MTHD)
    {
      error = do_update_new_notnull_cols_without_default (parser, alter,
							  vclass);
    }

  return error;
}

/*
 * do_alter_one_add_attr_mthd() -
 *
 *   return: Error code
 *
 *   parser(in): Parser context
 *   alter(in/out): Parse tree of a single clause of an ALTER statement. Not
 *                  all possible clauses are handled by this function; see the
 *                  note below and the do_alter() function.
 */
static int
do_alter_one_add_attr_mthd (PARSER_CONTEXT * parser, PT_NODE * alter)
{
  const char *entity_name;
  DB_CTMPL *ctemplate = NULL;
  DB_OBJECT *vclass;
  int error = NO_ERROR;
  DB_VALUE src_val, dest_val;
  LOCK req_lock;

  assert (alter->info.alter.code == PT_ADD_ATTR_MTHD);

  entity_name = alter->info.alter.entity_name->info.name.original;
  if (entity_name == NULL)
    {
      ERROR1 (error, ER_UNEXPECTED,
	      "Expecting a class or virtual class name.");
      return error;
    }

  vclass = sm_find_class (entity_name);
  if (vclass == NULL)
    {
      return er_errid ();
    }

  db_make_null (&src_val);
  db_make_null (&dest_val);

  req_lock = NULL_LOCK;
  if (alter->info.alter.alter_clause.attr_mthd.attr_def_list)
    {
      req_lock = X_LOCK;
    }
  else
    {
      req_lock = U_LOCK;
    }

  ctemplate = smt_edit_class_mop_with_lock (vclass, req_lock);

  if (ctemplate == NULL)
    {
      /* when dbt_edit_class fails (e.g. because the server unilaterally
         aborts us), we must record the associated error message into the
         parser.  Otherwise, we may get a confusing error msg of the form:
         "so_and_so is not a class". */
      pt_record_error (parser, alter->line_number, alter->column_number,
		       er_msg (), NULL);
      return er_errid ();
    }

  error = do_add_attributes (parser, ctemplate,
			     alter->info.alter.alter_clause.
			     attr_mthd.attr_def_list,
			     alter->info.alter.constraint_list, NULL);
  if (error != NO_ERROR)
    {
      dbt_abort_class (ctemplate);
      return error;
    }


  vclass = dbt_finish_class (ctemplate);
  if (vclass == NULL)
    {
      error = er_errid ();
      dbt_abort_class (ctemplate);
      return error;
    }

  ctemplate = smt_edit_class_mop_with_lock (vclass, req_lock);
  if (ctemplate == NULL)
    {
      error = er_errid ();
      return error;
    }

  error = do_add_constraints (ctemplate, alter->info.alter.constraint_list);
  if (error != NO_ERROR)
    {
      dbt_abort_class (ctemplate);
      return error;
    }


  if (error != NO_ERROR)
    {
      dbt_abort_class (ctemplate);
      return error;
    }

  vclass = dbt_finish_class (ctemplate);

  /* the dbt_finish_class() failed, the template was not freed */
  if (vclass == NULL)
    {
      error = er_errid ();
      dbt_abort_class (ctemplate);
      return error;
    }

  error = do_update_new_notnull_cols_without_default (parser, alter, vclass);

  return error;
}

/*
 * do_alter_clause_rename_entity() - Executes an ALTER TABLE RENAME TO clause
 *   return: Error code
 *   parser(in): Parser context
 *   alter(in/out): Parse tree of a PT_RENAME_ENTITY clause potentially
 *                  followed by the rest of the clauses in the ALTER
 *                  statement.
 * Note: The clauses following the PT_RENAME_ENTITY clause will be updated to
 *       the new name of the class.
 */
static int
do_alter_clause_rename_entity (PARSER_CONTEXT * const parser,
			       PT_NODE * const alter)
{
  int error_code = NO_ERROR;
  const char *const old_name =
    alter->info.alter.entity_name->info.name.original;
  const char *const new_name =
    alter->info.alter.alter_clause.rename.new_name->info.name.original;
  PT_NODE *tmp_clause = NULL;

  assert (alter->info.alter.code == PT_RENAME_ENTITY);

  error_code = do_rename_internal (old_name, new_name);
  if (error_code != NO_ERROR)
    {
      goto error_exit;
    }

  /* We now need to update the current name of the class for the rest of the
     ALTER clauses. */
  for (tmp_clause = alter->next; tmp_clause != NULL;
       tmp_clause = tmp_clause->next)
    {
      parser_free_tree (parser, tmp_clause->info.alter.entity_name);
      tmp_clause->info.alter.entity_name =
	parser_copy_tree (parser,
			  alter->info.alter.alter_clause.rename.new_name);
      if (tmp_clause->info.alter.entity_name == NULL)
	{
	  error_code = ER_FAILED;
	  goto error_exit;
	}
    }

  return error_code;

error_exit:
  return error_code;
}

/*
 * do_alter_clause_rename_entity() - Executes an ALTER TABLE DROP INDEX clause
 *   return: Error code
 *   parser(in): Parser context
 *   alter(in/out): Parse tree of a PT_DROP_INDEX_CLAUSE clause potentially
 *                  followed by the rest of the clauses in the ALTER
 *                  statement.
 * Note: The clauses following the PT_DROP_INDEX_CLAUSE clause are not
 *       affected in any way.
 */
static int
do_alter_clause_drop_index (PARSER_CONTEXT * const parser,
			    PT_NODE * const alter)
{
  int error_code = NO_ERROR;
  DB_OBJECT *obj = NULL;

  assert (alter->info.alter.code == PT_DROP_INDEX_CLAUSE);
  assert (alter->info.alter.constraint_list != NULL);
  assert (alter->info.alter.constraint_list->next == NULL);
  assert (alter->info.alter.constraint_list->node_type == PT_NAME);

  obj = sm_find_class (alter->info.alter.entity_name->info.name.original);
  if (obj == NULL)
    {
      error_code = er_errid ();
    }
  error_code =
    create_or_drop_index_helper (parser, alter->info.alter.constraint_list->
				 info.name.original,
				 alter->info.alter.alter_clause.index.unique,
				 NULL, NULL, obj, DO_INDEX_DROP);
  if (error_code != NO_ERROR)
    {
      goto error_exit;
    }

  return error_code;

error_exit:
  return error_code;
}


/*
 * do_alter() -
 *   return: Error code
 *   parser(in): Parser context
 *   alter(in/out): Parse tree of an alter statement
 */
int
do_alter (PARSER_CONTEXT * parser, PT_NODE * alter)
{
  int error_code = NO_ERROR;
  PT_NODE *crt_clause = NULL;
  bool do_semantic_checks = false;

  CHECK_MODIFICATION_ERROR ();

  assert (alter == NULL || alter->next == NULL);

  for (crt_clause = alter; crt_clause != NULL; crt_clause = crt_clause->next)
    {
      PT_NODE *const save_next = crt_clause->next;
      const PT_ALTER_CODE alter_code = crt_clause->info.alter.code;

      assert (save_next == NULL);

      /* The first ALTER clause has already been checked, we call the semantic
         check starting with the second clause. */
      if (do_semantic_checks)
	{
	  PT_NODE *crt_result = NULL;

	  crt_clause->next = NULL;
	  crt_result = pt_compile (parser, crt_clause);
	  crt_clause->next = save_next;
	  if (!crt_result || pt_has_error (parser))
	    {
	      pt_report_to_ersys_with_statement (parser, PT_SEMANTIC,
						 crt_clause);
	      error_code = er_errid ();
	      goto error_exit;
	    }
	  assert (crt_result == crt_clause);
	}

      switch (alter_code)
	{
	case PT_RENAME_ENTITY:
	  error_code = do_alter_clause_rename_entity (parser, crt_clause);
	  break;
	case PT_DROP_INDEX_CLAUSE:
	  error_code = do_alter_clause_drop_index (parser, crt_clause);
	  break;
	case PT_CHANGE_ATTR:
	  error_code = do_alter_clause_change_attribute (parser, crt_clause);
	  break;
	case PT_CHANGE_OWNER:
	  error_code = do_alter_change_owner (parser, crt_clause);
	  break;

	case PT_ADD_ATTR_MTHD:
	  /* This code might not correctly handle a list of ALTER clauses so
	     we keep crt_clause->next to NULL during its execution just to be
	     on the safe side. */
	  crt_clause->next = NULL;

	  error_code = do_alter_one_add_attr_mthd (parser, crt_clause);

	  crt_clause->next = save_next;
	  break;

	default:
	  /* This code might not correctly handle a list of ALTER clauses so
	     we keep crt_clause->next to NULL during its execution just to be
	     on the safe side. */
	  crt_clause->next = NULL;

	  error_code = do_alter_one_clause_with_template (parser, crt_clause);

	  crt_clause->next = save_next;
	}

      if (error_code != NO_ERROR)
	{
	  goto error_exit;
	}
      do_semantic_checks = true;
    }

  return error_code;

error_exit:
  assert (error_code != NO_ERROR);

  return error_code;
}




/*
 * Function Group :
 * DO functions for user management
 *
 */

#define IS_NAME(n)      ((n)->node_type == PT_NAME)
#define IS_STRING(n)    ((n)->node_type == PT_VALUE &&          \
                         ((n)->type_enum == PT_TYPE_VARCHAR))
#define GET_NAME(n)     ((char *) (n)->info.name.original)
#define GET_STRING(n)   ((char *) (n)->info.value.data_value.str->bytes)

/*
 * do_grant() - Grants priviledges
 *   return: Error code if grant fails
 *   parser(in): Parser context
 *   statement(in): Parse tree of a grant statement
 */
int
do_grant (UNUSED_ARG const PARSER_CONTEXT * parser, const PT_NODE * statement)
{
  int error = NO_ERROR;
  PT_NODE *user, *user_list;
  DB_OBJECT *user_obj, *class_mop;
  PT_NODE *auth_cmd_list, *auth_list, *auth;
  DB_AUTH db_auth;
  PT_NODE *spec_list, *s_list, *spec;
  PT_NODE *entity_list, *entity;
  int grant_option;

  CHECK_MODIFICATION_ERROR ();

  user_list = statement->info.grant.user_list;
  auth_cmd_list = statement->info.grant.auth_cmd_list;
  spec_list = statement->info.grant.spec_list;

  if (statement->info.grant.grant_option == PT_GRANT_OPTION)
    {
      grant_option = true;
    }
  else
    {
      grant_option = false;
    }

  for (user = user_list; user != NULL; user = user->next)
    {
      user_obj = db_find_user (user->info.name.original);
      if (user_obj == NULL)
	{
	  return er_errid ();
	}

      auth_list = auth_cmd_list;
      for (auth = auth_list; auth != NULL; auth = auth->next)
	{
	  db_auth = pt_auth_to_db_auth (auth);

	  s_list = spec_list;
	  for (spec = s_list; spec != NULL; spec = spec->next)
	    {
	      entity_list = spec->info.spec.flat_entity_list;
	      assert (entity_list->next == NULL);
	      for (entity = entity_list; entity != NULL;
		   entity = entity->next)
		{
		  class_mop = sm_find_class (entity->info.name.original);
		  if (class_mop == NULL)
		    {
		      return er_errid ();
		    }

		  error = db_grant (user_obj, class_mop, db_auth,
				    grant_option);
		  if (error != NO_ERROR)
		    {
		      return error;
		    }
		}
	    }
	}
    }

  return error;
}

/*
 * do_revoke() - Revokes priviledges
 *   return: Error code if revoke fails
 *   parser(in): Parser context
 *   statement(in): Parse tree of a revoke statement
 */
int
do_revoke (UNUSED_ARG const PARSER_CONTEXT * parser,
	   const PT_NODE * statement)
{
  int error = NO_ERROR;

  PT_NODE *user, *user_list;
  DB_OBJECT *user_obj, *class_mop;
  PT_NODE *auth_cmd_list, *auth_list, *auth;
  DB_AUTH db_auth;
  PT_NODE *spec_list, *s_list, *spec;
  PT_NODE *entity_list, *entity;

  CHECK_MODIFICATION_ERROR ();

  user_list = statement->info.revoke.user_list;
  auth_cmd_list = statement->info.revoke.auth_cmd_list;
  spec_list = statement->info.revoke.spec_list;

  for (user = user_list; user != NULL; user = user->next)
    {
      user_obj = db_find_user (user->info.name.original);
      if (user_obj == NULL)
	{
	  return er_errid ();
	}

      auth_list = auth_cmd_list;
      for (auth = auth_list; auth != NULL; auth = auth->next)
	{
	  db_auth = pt_auth_to_db_auth (auth);

	  s_list = spec_list;
	  for (spec = s_list; spec != NULL; spec = spec->next)
	    {
	      entity_list = spec->info.spec.flat_entity_list;
	      assert (entity_list->next == NULL);
	      for (entity = entity_list; entity != NULL;
		   entity = entity->next)
		{
		  class_mop = sm_find_class (entity->info.name.original);
		  if (class_mop == NULL)
		    {
		      return er_errid ();
		    }

		  error = db_revoke (user_obj, class_mop, db_auth);
		  if (error != NO_ERROR)
		    {
		      return error;
		    }
		}
	    }
	}
    }

  return error;
}

/*
 * do_create_user() - Create a user
 *   return: Error code if creation fails
 *   parser(in): Parser context
 *   statement(in): Parse tree of a create user statement
 */
int
do_create_user (const PARSER_CONTEXT * parser, const PT_NODE * statement)
{
  int error = NO_ERROR;
  DB_OBJECT *user;
  int exists;
  PT_NODE *node;
  const char *user_name, *password;

  CHECK_MODIFICATION_ERROR ();

  if (statement == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
	      0);
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  user = NULL;
  node = statement->info.create_user.user_name;
  if (node == NULL || node->node_type != PT_NAME
      || node->info.name.original == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
	      ER_AU_MISSING_OR_INVALID_USER, 0);
      return ER_AU_MISSING_OR_INVALID_USER;
    }

  user_name = node->info.name.original;

  if (parser == NULL || statement == NULL || user_name == NULL)
    {
      error = ER_AU_INVALID_USER;
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 1, "");
    }
  else
    {
      exists = 0;

      user = db_add_user (user_name, &exists);
      if (user == NULL)
	{
	  error = er_errid ();
	}
      else if (exists)
	{
	  error = ER_AU_USER_EXISTS;
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 1, user_name);
	}
      else
	{
	  node = statement->info.create_user.password;
	  password = (node && IS_STRING (node)) ? GET_STRING (node) : NULL;
	  if (error == NO_ERROR && password)
	    {
	      error = au_set_password (user, password,
				       !statement->info.create_user.
				       is_encrypted);
	    }
	}

      if (error != NO_ERROR)
	{
	  if (user && exists == 0)
	    {
	      er_stack_push ();
	      db_drop_user (user);
	      er_stack_pop ();
	    }
	}
    }

  return error;
}

/*
 * do_drop_user() - Drop the user
 *   return: Error code if dropping fails
 *   parser(in): Parser context
 *   statement(in): Parse tree of a drop user statement
 */
int
do_drop_user (const PARSER_CONTEXT * parser, const PT_NODE * statement)
{
  int error = NO_ERROR;
  DB_OBJECT *user;
  PT_NODE *node;
  const char *user_name;

  CHECK_MODIFICATION_ERROR ();

  if (statement == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
	      0);
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  node = statement->info.create_user.user_name;
  user_name = (node && IS_NAME (node)) ? GET_NAME (node) : NULL;

  if (!parser || !statement || !user_name)
    {
      error = ER_AU_INVALID_USER;
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 1, "");
    }
  else
    {
      user = db_find_user (user_name);

      if (user == NULL)
	{
	  error = er_errid ();
	}
      else
	{
	  error = db_drop_user (user);
	}
    }

  return error;
}

/*
 * do_alter_user() - Change the user's password
 *   return: Error code if alter fails
 *   parser(in): Parser context
 *   statement(in): Parse tree of an alter statement
 */
int
do_alter_user (const PARSER_CONTEXT * parser, const PT_NODE * statement)
{
  int error = NO_ERROR;
  DB_OBJECT *user;
  PT_NODE *node;
  const char *user_name, *password;

  CHECK_MODIFICATION_ERROR ();

  if (statement == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS,
	      0);
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  node = statement->info.alter_user.user_name;
  user_name = (node && IS_NAME (node)) ? GET_NAME (node) : NULL;

  if (!parser || !statement || !user_name)
    {
      error = ER_AU_INVALID_USER;
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 1, "");
    }
  else
    {
      user = db_find_user (user_name);

      if (user == NULL)
	{
	  error = er_errid ();
	}
      else
	{
	  node = statement->info.alter_user.password;
	  password = (node && IS_STRING (node)) ? GET_STRING (node) : NULL;

	  error = au_set_password (user, password,
				   !statement->info.alter_user.is_encrypted);
	}
    }

  return error;
}

/*
 * Function Group :
 * Code for dropping a Classes by Parse Tree descriptions.
 *
 */

/*
 * drop_class_name() - This static routine drops a class by name.
 *   return: Error code
 *   name(in): Class name to drop
 */
static int
drop_class_name (const char *name)
{
  DB_OBJECT *class_mop;

  class_mop = sm_find_class (name);

  if (class_mop)
    {
      return db_drop_class (class_mop);
    }
  else
    {
      /* if class is null, return the global error. */
      return er_errid ();
    }
}

/*
 * do_drop() - Drops a vclass, class
 *   return: Error code if a vclass is not deleted.
 *   parser(in): Parser context
 *   statement(in/out): Parse tree of a drop statement
 */
int
do_drop (UNUSED_ARG PARSER_CONTEXT * parser, PT_NODE * statement)
{
  int error = NO_ERROR;
  PT_NODE *entity_spec_list, *entity_spec;
  PT_NODE *entity;

  CHECK_MODIFICATION_ERROR ();

  entity_spec_list = statement->info.drop.spec_list;
  for (entity_spec = entity_spec_list; entity_spec != NULL;
       entity_spec = entity_spec->next)
    {
      assert (entity_spec->info.spec.flat_entity_list->next == NULL);
      for (entity = entity_spec->info.spec.flat_entity_list;
	   entity != NULL; entity = entity->next)
	{
	  error = drop_class_name (entity->info.name.original);
	  if (error != NO_ERROR)
	    {
	      goto error_exit;
	    }
	  error = au_drop_table (entity->info.name.original);
	  if (error != NO_ERROR)
	    {
	      goto error_exit;
	    }
	}
    }

  return error;

error_exit:
  assert (error != NO_ERROR);

  return error;
}

/*
 * do_rename() - Renames several vclasses or classes
 *   return: Error code
 *   parser(in): Parser context
 *   statement(in): Parse tree of a rename statement
 */
int
do_rename (UNUSED_ARG const PARSER_CONTEXT * parser,
	   const PT_NODE * statement)
{
  int error = NO_ERROR;
  const char *old_name;
  const char *new_name;

  CHECK_MODIFICATION_ERROR ();

  assert (statement != NULL);
  assert (statement->next == NULL);

  old_name = statement->info.rename.old_name->info.name.original;
  new_name = statement->info.rename.new_name->info.name.original;

  error = do_rename_internal (old_name, new_name);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }
  error = au_rename_table (old_name, new_name);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  return error;

error_exit:
  assert (error != NO_ERROR);

  return error;
}

static int
do_rename_internal (const char *const old_name, const char *const new_name)
{
  DB_OBJECT *old_class = NULL;

  old_class = sm_find_class (old_name);
  if (old_class == NULL)
    {
      return er_errid ();
    }

  return db_rename_class (old_class, new_name);
}

/*
 * Function Group :
 * Parse tree to index commands translation.
 *
 */

static DB_CONSTRAINT_TYPE
get_unique_index_type (const bool is_unique)
{
  if (is_unique)
    {
      return DB_CONSTRAINT_UNIQUE;
    }
  else
    {
      return DB_CONSTRAINT_INDEX;
    }
}

/*
 * create_or_drop_index_helper()
 *   return: Error code
 *   parser(in): Parser context
 *   constraint_name(in): If NULL the default constraint name is used;
 *                        column_names must be non-NULL in this case.
 *   is_unique(in):
 *   column_names(in): Can be NULL if dropping a constraint and providing the
 *                     constraint name.
 *   obj(in): Class object
 *   do_index(in) : The operation to be performed (creating or dropping)
 */
static int
create_or_drop_index_helper (UNUSED_ARG PARSER_CONTEXT * parser,
			     const char *const constraint_name,
			     const bool is_unique,
			     UNUSED_ARG PT_NODE * spec,
			     PT_NODE * column_names,
			     DB_OBJECT * const obj, DO_INDEX do_index)
{
  int error = NO_ERROR;
  int i = 0, nnames = 0;
  DB_CONSTRAINT_TYPE ctype = -1;
  const PT_NODE *c = NULL, *n = NULL;
  char **attnames = NULL;
  int *asc_desc = NULL;
  char *cname = NULL;

  nnames = pt_length_of_list (column_names);

  attnames = (char **) malloc ((nnames + 1) * sizeof (const char *));
  if (attnames == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      (nnames + 1) * sizeof (const char *));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  asc_desc = (int *) malloc ((nnames) * sizeof (int));
  if (asc_desc == NULL)
    {
      free_and_init (attnames);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      nnames * sizeof (int));
      return ER_OUT_OF_VIRTUAL_MEMORY;
    }

  for (c = column_names, i = 0; c != NULL; c = c->next, i++)
    {
      asc_desc[i] = c->info.sort_spec.asc_or_desc == PT_ASC ? 0 : 1;
      /* column name node */
      n = c->info.sort_spec.expr;
      attnames[i] = (char *) n->info.name.original;
    }
  attnames[i] = NULL;

  ctype = get_unique_index_type (is_unique);

  cname = sm_produce_constraint_name (sm_class_name (obj), ctype,
				      (const char **) attnames,
				      asc_desc, constraint_name);
  if (cname == NULL)
    {
      error = er_errid ();
    }
  else
    {
      if (do_index == DO_INDEX_CREATE)
	{
	  error = sm_add_constraint (obj, ctype, cname,
				     (const char **) attnames, asc_desc);
	}
      else
	{
	  assert (do_index == DO_INDEX_DROP);
	  error = sm_drop_constraint (obj, ctype, cname,
				      (const char **) attnames);
	}
    }

#if 0
end:
#endif

  free_and_init (attnames);
  free_and_init (asc_desc);

  if (cname != NULL)
    {
      free_and_init (cname);
    }

  return error;
}

/*
 * do_create_index() - Creates an index
 *   return: Error code if it fails
 *   parser(in): Parser context
 *   statement(in) : Parse tree of a create index statement
 */
int
do_create_index (PARSER_CONTEXT * parser, const PT_NODE * statement)
{
  PT_NODE *cls;
  DB_OBJECT *obj;
  const char *index_name = NULL;
  int error = NO_ERROR;

  CHECK_MODIFICATION_ERROR ();

  /* class should be already available */
  assert (statement->info.index.indexed_class);

  cls = statement->info.index.indexed_class->info.spec.entity_name;

  obj = sm_find_class (cls->info.name.original);
  if (obj == NULL)
    {
      return er_errid ();
    }

  index_name = statement->info.index.index_name ?
    statement->info.index.index_name->info.name.original : NULL;

  error = create_or_drop_index_helper (parser, index_name,
				       statement->info.index.unique,
				       statement->info.index.indexed_class,
				       statement->info.index.column_names,
				       obj, DO_INDEX_CREATE);
  return error;
}

/*
 * do_drop_index() - Drops an index on a class.
 *   return: Error code if it fails
 *   parser(in) : Parser context
 *   statement(in): Parse tree of a drop index statement
 */
int
do_drop_index (PARSER_CONTEXT * parser, const PT_NODE * statement)
{
  PT_NODE *cls = NULL;
  DB_OBJECT *obj = NULL;
  const char *index_name = NULL;
  int error_code = NO_ERROR;
  const char *class_name = NULL;

  CHECK_MODIFICATION_ERROR ();

  assert (statement->info.index.unique == false);
  assert (statement->info.index.index_name != NULL);

  if (statement->info.index.index_name == NULL
      || statement->info.index.indexed_class == NULL
      || statement->info.index.indexed_class->info.spec.flat_entity_list ==
      NULL)
    {
      assert (false);		/* should be impossible */
      error_code = ER_SM_INVALID_DEF_CONSTRAINT_NAME_PARAMS;
      goto error_exit;
    }

  index_name = statement->info.index.index_name->info.name.original;
  assert (index_name != NULL);

  cls = statement->info.index.indexed_class->info.spec.flat_entity_list;
  assert (cls->next == NULL);
  class_name = cls->info.name.resolved;

  obj = sm_find_class (class_name);
  if (obj == NULL)
    {
      error_code = er_errid ();
      goto error_exit;
    }

  error_code =
    create_or_drop_index_helper (parser, index_name,
				 statement->info.index.unique,
				 statement->info.index.indexed_class,
				 statement->info.index.column_names,
				 obj, DO_INDEX_DROP);
  if (error_code != NO_ERROR)
    {
      goto error_exit;
    }

  assert (error_code == NO_ERROR);

  return error_code;

error_exit:
  assert (error_code != NO_ERROR);

  return error_code;
}

/*
 * do_alter_index() - Alters an index on a class.
 *   return: Error code if it fails
 *   parser(in): Parser context
 *   statement(in): Parse tree of a alter index statement
 */
int
do_alter_index (UNUSED_ARG PARSER_CONTEXT * parser, const PT_NODE * statement)
{
  int error = NO_ERROR;
  DB_OBJECT *obj;
  PT_NODE *n, *c;
  PT_NODE *cls = NULL;
  int i, nnames;
  DB_CONSTRAINT_TYPE ctype;
  char **attnames = NULL;
  int *asc_desc = NULL;
  SM_CLASS *smcls;
  SM_CLASS_CONSTRAINT *idx;
  SM_ATTRIBUTE **attp;
  int attnames_allocated = 0;
  const char *index_name = NULL;
  const char *class_name = NULL;

  /* TODO refactor this code, the code in create_or_drop_index_helper and the
     code in do_drop_index in order to remove duplicate code */

  CHECK_MODIFICATION_ERROR ();

  assert (statement->info.index.unique == false);
  assert (statement->info.index.index_name != NULL);

  if (statement->info.index.index_name == NULL
      || statement->info.index.indexed_class == NULL
      || statement->info.index.indexed_class->info.spec.flat_entity_list ==
      NULL)
    {
      assert (false);		/* should be impossible */
      error = ER_SM_INVALID_DEF_CONSTRAINT_NAME_PARAMS;
      goto error_exit;
    }

  index_name = statement->info.index.index_name->info.name.original;
  assert (index_name != NULL);

  cls = statement->info.index.indexed_class->info.spec.flat_entity_list;
  assert (cls->next == NULL);
  class_name = cls->info.name.resolved;

  obj = sm_find_class (class_name);
  if (obj == NULL)
    {
      error = er_errid ();
      goto error_exit;
    }

  ctype = get_unique_index_type (statement->info.index.unique);

  if (statement->info.index.column_names == NULL)
    {
      /* find the attributes of the index */
      idx = NULL;

      if (au_fetch_class (obj, &smcls, S_LOCK, AU_SELECT) != NO_ERROR)
	{
	  error = er_errid ();
	  goto error_exit;
	}

      idx = classobj_find_class_index (smcls, index_name);
      if (idx == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_SM_NO_INDEX, 1, index_name);
	  error = ER_SM_NO_INDEX;
	  goto error_exit;
	}

      attp = idx->attributes;
      if (attp == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OBJ_INVALID_ATTRIBUTE, 1, "unknown");
	  error = ER_OBJ_INVALID_ATTRIBUTE;
	  goto error_exit;
	}

      nnames = 0;
      while (*attp++)
	{
	  nnames++;
	}

      attnames = (char **) malloc ((nnames + 1) * sizeof (const char *));
      if (attnames == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_OUT_OF_VIRTUAL_MEMORY, 1,
		  (nnames + 1) * sizeof (const char *));
	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  goto error_exit;
	}

      attnames_allocated = 1;

      for (i = 0, attp = idx->attributes; *attp; i++, attp++)
	{
	  attnames[i] = strdup ((*attp)->name);
	  if (attnames[i] == NULL)
	    {
	      int j;
	      for (j = 0; j < i; ++j)
		{
		  free_and_init (attnames[j]);
		}
	      free_and_init (attnames);
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1,
		      (strlen ((*attp)->name) + 1) * sizeof (char));
	      error = ER_OUT_OF_VIRTUAL_MEMORY;
	      goto error_exit;
	    }
	}
      attnames[i] = NULL;

      if (idx->asc_desc)
	{
	  asc_desc = (int *) malloc ((nnames) * sizeof (int));
	  if (asc_desc == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, nnames * sizeof (int));
	      error = ER_OUT_OF_VIRTUAL_MEMORY;
	      goto error_exit;
	    }

	  for (i = 0; i < nnames; i++)
	    {
	      asc_desc[i] = idx->asc_desc[i];
	    }
	}
    }
  else
    {
      nnames = pt_length_of_list (statement->info.index.column_names);
      attnames = (char **) malloc ((nnames + 1) * sizeof (const char *));
      if (attnames == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, (nnames + 1) * sizeof (const char *));
	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  goto error_exit;
	}

      asc_desc = (int *) malloc ((nnames) * sizeof (int));
      if (asc_desc == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, nnames * sizeof (int));
	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  goto error_exit;
	}

      for (c = statement->info.index.column_names, i = 0; c != NULL;
	   c = c->next, i++)
	{
	  asc_desc[i] = c->info.sort_spec.asc_or_desc == PT_ASC ? 0 : 1;
	  n = c->info.sort_spec.expr;	/* column name node */
	  attnames[i] = (char *) n->info.name.original;
	}
      attnames[i] = NULL;
    }

  error = sm_drop_constraint (obj, ctype, index_name,
			      (const char **) attnames);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  error =
    sm_add_constraint (obj, ctype, index_name, (const char **) attnames,
		       asc_desc);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

end:

  if (attnames_allocated)
    {
      for (i = 0; attnames && attnames[i]; i++)
	{
	  free_and_init (attnames[i]);
	}
    }

  if (attnames)
    {
      free_and_init (attnames);
    }

  if (asc_desc)
    {
      free_and_init (asc_desc);
    }

  return error;

error_exit:
  assert (error != NO_ERROR);

  error = (error == NO_ERROR && (error = er_errid ()) == NO_ERROR) ?
    ER_FAILED : error;

  goto end;
}

/*
 * validate_attribute_domain() - This checks an attribute to make sure
 *                                  that it makes sense
 *   return: Error code
 *   parser(in): Parser context
 *   attribute(in): Parse tree of an attribute
 *
 * Note: Error reporting system
 */
static int
validate_attribute_domain (PARSER_CONTEXT * parser,
			   PT_NODE * attribute,
			   UNUSED_ARG SM_CLASS_TYPE class_type)
{
  int error = NO_ERROR;

  if (attribute == NULL)
    {
      pt_record_error (parser,
		       __LINE__, 0, "system error - NULL attribute node",
		       NULL);

      return ER_PT_SEMANTIC;
    }

  if (attribute->type_enum == PT_TYPE_NONE
      || attribute->type_enum == PT_TYPE_MAYBE)
    {
      pt_record_error (parser,
		       attribute->line_number,
		       attribute->column_number,
		       "system error - attribute type not set", NULL);
    }
  else if (attribute->type_enum == PT_TYPE_OBJECT
	   || PT_IS_COLLECTION_TYPE (attribute->type_enum))
    {
      PT_ERRORmf (parser, attribute,
		  MSGCAT_SET_PARSER_SEMANTIC,
		  MSGCAT_SEMANTIC_NOT_ALLOWED_HERE,
		  pt_show_type_enum (attribute->type_enum));
    }
  else
    {
      if (attribute->data_type)
	{
	  int p = attribute->data_type->info.data_type.precision;

	  switch (attribute->type_enum)
	    {
	    case PT_TYPE_NUMERIC:
	      if (p != DB_DEFAULT_PRECISION
		  && (p <= 0 || p > DB_MAX_NUMERIC_PRECISION))
		{
		  PT_ERRORmf3 (parser, attribute,
			       MSGCAT_SET_PARSER_SEMANTIC,
			       MSGCAT_SEMANTIC_INV_PREC, p, 0,
			       DB_MAX_NUMERIC_PRECISION);
		}
	      break;

	    case PT_TYPE_VARBIT:
	      if (p != DB_DEFAULT_PRECISION
		  && (p <= 0 || p > DB_MAX_VARBIT_PRECISION))
		{
		  PT_ERRORmf3 (parser, attribute,
			       MSGCAT_SET_PARSER_SEMANTIC,
			       MSGCAT_SEMANTIC_INV_PREC, p, 0,
			       DB_MAX_VARBIT_PRECISION);
		}
	      break;

	    case PT_TYPE_VARCHAR:
	      if (p != DB_DEFAULT_PRECISION
		  && (p <= 0 || p > DB_MAX_VARCHAR_PRECISION))
		{
		  PT_ERRORmf3 (parser, attribute,
			       MSGCAT_SET_PARSER_SEMANTIC,
			       MSGCAT_SEMANTIC_INV_PREC, p, 0,
			       DB_MAX_VARCHAR_PRECISION);
		}
	      break;

	    default:
	      break;
	    }
	}
    }

  assert (error == NO_ERROR);

  if (pt_has_error (parser))
    {
      error = ER_PT_SEMANTIC;
    }

  return error;
}

static const char *
get_attr_name (PT_NODE * attribute)
{
  /* First try the derived name and then the original name. For example:
     create view a_view as select a av1, a av2, b bv from a_tbl;
   */
  return attribute->info.attr_def.attr_name->alias_print
    ? attribute->info.attr_def.attr_name->alias_print
    : attribute->info.attr_def.attr_name->info.name.original;
}

/*
 * do_add_attribute() - Adds an attribute to a class object
 *   return: Error code
 *   parser(in): Parser context
 *   ctemplate(in/out): Class template
 *   attribute(in/out): Attribute to add
 *   constraints(in/out): the constraints of the class
 *   error_on_not_normal(in): whether to flag an error on class
 *                            attributes or not
 *   shard_key(in):
 * Note : The class object is modified
 */
static int
do_add_attribute (PARSER_CONTEXT * parser, DB_CTMPL * ctemplate,
		  PT_NODE * attribute, PT_NODE * constraints,
		  bool error_on_not_normal, PT_NODE * shard_key)
{
  const char *attr_name;
  DB_VALUE stack_value;
  DB_VALUE *default_value = &stack_value;
  PT_NODE *default_info;
  int error = NO_ERROR;
  DB_DOMAIN *attr_db_domain;
  bool add_first = false;
  const char *add_after_attr = NULL;
  PT_NODE *cnstr, *pk_attr;
  DB_DEFAULT_EXPR_TYPE default_expr_type = DB_DEFAULT_NONE;
  bool is_shard_key = false;

  DB_MAKE_NULL (&stack_value);
  attr_name = get_attr_name (attribute);

  if (error_on_not_normal && attribute->info.attr_def.attr_type != PT_NORMAL)
    {
      ERROR1 (error, ER_SM_ONLY_NORMAL_ATTRIBUTES, attr_name);
      goto error_exit;
    }

  error = validate_attribute_domain (parser, attribute,
				     smt_get_class_type (ctemplate));
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  assert (default_value == &stack_value);
  error = get_att_default_from_def (parser, attribute, &default_value);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  if (default_value && DB_IS_NULL (default_value))
    {
      /* don't allow a default value of NULL for NOT NULL constrained columns */
      if (attribute->info.attr_def.constrain_not_null)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_CANNOT_HAVE_NOTNULL_DEFAULT_NULL, 1, attr_name);
	  error = ER_CANNOT_HAVE_NOTNULL_DEFAULT_NULL;
	  goto error_exit;
	}

      /* don't allow a default value of NULL in new PK constraint */
      for (cnstr = constraints; cnstr != NULL; cnstr = cnstr->next)
	{
	  if (cnstr->info.constraint.type == PT_CONSTRAIN_PRIMARY_KEY)
	    {
	      break;
	    }
	}
      if (cnstr != NULL)
	{
	  for (pk_attr = cnstr->info.constraint.un.primary_key.attrs;
	       pk_attr != NULL; pk_attr = pk_attr->next)
	    {
	      if (intl_identifier_casecmp (pk_attr->info.name.original,
					   attr_name) == 0)
		{
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  ER_CANNOT_HAVE_PK_DEFAULT_NULL, 1, attr_name);
		  error = ER_CANNOT_HAVE_PK_DEFAULT_NULL;
		  goto error_exit;
		}
	    }
	}
    }

  attr_db_domain = pt_node_to_db_domain (parser, attribute, ctemplate->name);
  if (attr_db_domain == NULL)
    {
      error = er_errid ();
      goto error_exit;
    }

  attr_db_domain = tp_domain_cache (attr_db_domain);

  error = get_att_order_from_def (attribute, &add_first, &add_after_attr);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  default_info = attribute->info.attr_def.data_default;
  if (default_info != NULL)
    {
      default_expr_type = default_info->info.data_default.default_expr;
      default_value = &stack_value;
    }

  if (shard_key != NULL)
    {
      if (strcasecmp (attr_name, shard_key->info.name.original) == 0)
	{
	  is_shard_key = true;
	}
    }

  error = smt_add_attribute_w_dflt_w_order (ctemplate, attr_name, NULL,
					    attr_db_domain, default_value,
					    add_first,
					    add_after_attr,
					    default_expr_type, is_shard_key);

  db_value_clear (&stack_value);

  /* Does the attribute belong to a NON_NULL constraint? */
  if (error == NO_ERROR)
    {
      if (attribute->info.attr_def.constrain_not_null)
	{
	  error = dbt_constrain_non_null (ctemplate, attr_name, 1);
	}
    }

  return error;

error_exit:
  db_value_clear (&stack_value);
  return error;
}

/*
 * do_add_attributes() - Adds attributes to a class object
 *   return: Error code
 *   parser(in): Parser context
 *   ctemplate(in/out): Class template
 *   atts(in/out): Attributes to add
 *   constraints(in/out): the constraints of the class
 *   shard_key(in):
 * Note : The class object is modified
 */
int
do_add_attributes (PARSER_CONTEXT * parser, DB_CTMPL * ctemplate,
		   PT_NODE * atts, PT_NODE * constraints, PT_NODE * shard_key)
{
  PT_NODE *crt_attr;
  int error = NO_ERROR;

  crt_attr = atts;

  while (crt_attr)
    {
      error =
	do_add_attribute (parser, ctemplate, crt_attr, constraints,
			  false, shard_key);
      if (error != NO_ERROR)
	{
	  return error;
	}
      crt_attr = crt_attr->next;
    }

  return error;
}

/*
 * do_add_constraints() - This extern routine adds constraints
 *			  to a class object.
 *   return: Error code
 *   ctemplate(in/out): Class template
 *   constraints(in): Constraints to add
 *
 * Note : Class object is modified
 */
int
do_add_constraints (DB_CTMPL * ctemplate, PT_NODE * constraints)
{
  int error = NO_ERROR;
  PT_NODE *cnstr;
  int max_attrs = 0;
  char **att_names = NULL;

  PT_NODE *cons_atts, *p;
  int i, n_atts;
  char *const_name = NULL;
  DB_CONSTRAINT_TYPE constraint_type = DB_CONSTRAINT_NOT_NULL;
  int *asc_desc = NULL;

  /*  Find the size of the largest UNIQUE constraint list and allocate
     a character array large enough to contain it. */
  for (cnstr = constraints; cnstr != NULL; cnstr = cnstr->next)
    {
      switch (cnstr->info.constraint.type)
	{
	case PT_CONSTRAIN_UNIQUE:
	  max_attrs = MAX (max_attrs,
			   pt_length_of_list (cnstr->info.constraint.
					      un.unique.attrs));
	  break;
	case PT_CONSTRAIN_PRIMARY_KEY:
	  max_attrs = MAX (max_attrs,
			   pt_length_of_list (cnstr->info.constraint.
					      un.primary_key.attrs));
	  break;
	case PT_CONSTRAIN_INDEX:
	  max_attrs = MAX (max_attrs,
			   pt_length_of_list (cnstr->info.constraint.
					      un.index.attrs));
	  break;
	case PT_CONSTRAIN_NOT_NULL:
	case PT_CONSTRAIN_NULL:
	case PT_CONSTRAIN_CHECK:
	  break;

	default:
	  assert (false);
	  goto constraint_error;
	  break;
	}
    }

  if (max_attrs > 0)
    {
      att_names = (char **) malloc ((max_attrs + 1) * sizeof (char *));

      if (att_names == NULL)
	{
	  error = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
		  (max_attrs + 1) * sizeof (char *));
	  goto constraint_error;
	}


      for (cnstr = constraints; cnstr != NULL; cnstr = cnstr->next)
	{
	  cons_atts = NULL;
	  switch (cnstr->info.constraint.type)
	    {
	    case PT_CONSTRAIN_PRIMARY_KEY:
	      cons_atts = cnstr->info.constraint.un.primary_key.attrs;
	      constraint_type = DB_CONSTRAINT_PRIMARY_KEY;
	      break;
	    case PT_CONSTRAIN_UNIQUE:
	      cons_atts = cnstr->info.constraint.un.unique.attrs;
	      constraint_type = DB_CONSTRAINT_UNIQUE;
	      break;
	    case PT_CONSTRAIN_INDEX:
	      cons_atts = cnstr->info.constraint.un.index.attrs;
	      constraint_type = DB_CONSTRAINT_INDEX;
	      break;
	    case PT_CONSTRAIN_NOT_NULL:
	    case PT_CONSTRAIN_NULL:
	    case PT_CONSTRAIN_CHECK:
	      cons_atts = NULL;
	      break;

	    default:
	      assert (false);
	      cons_atts = NULL;
	      break;
	    }

	  if (cons_atts != NULL)
	    {
	      n_atts = pt_length_of_list (cons_atts);
	      asc_desc = (int *) malloc (n_atts * sizeof (int));
	      if (asc_desc == NULL)
		{
		  error = ER_OUT_OF_VIRTUAL_MEMORY;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  error, 1, (n_atts * sizeof (int)));

		  goto constraint_error;
		}

	      for (i = 0, p = cons_atts; p != NULL; p = p->next, i++)
		{
		  asc_desc[i] =
		    PT_NAME_INFO_IS_FLAGED (p, PT_NAME_INFO_DESC) ? 1 : 0;
		  att_names[i] = (char *) p->info.name.original;
		}
	      att_names[i] = NULL;

	      /* Get the constraint name (if supplied) */
	      if (cnstr->info.constraint.name != NULL)
		{
		  const_name =
		    (char *) cnstr->info.constraint.name->info.name.original;
		}

	      const_name =
		sm_produce_constraint_name_tmpl (ctemplate, constraint_type,
						 (const char **) att_names,
						 asc_desc,
						 (const char *) const_name);
	      if (const_name == NULL)
		{
		  error = er_errid ();
		  free_and_init (asc_desc);

		  goto constraint_error;
		}

	      error = smt_add_constraint (ctemplate, constraint_type,
					  const_name,
					  (const char **) att_names,
					  asc_desc);

	      free_and_init (const_name);
	      free_and_init (asc_desc);
	      if (error != NO_ERROR)
		{
		  goto constraint_error;
		}
	    }
	}

      free_and_init (att_names);
    }

  return (error);

/* error handler */
constraint_error:
  if (att_names)
    {
      free_and_init (att_names);
    }
  return (error);
}

/*
 * add_query_to_virtual_class() - Adds a query to a virtual class object
 *   return: Error code
 *   parser(in): Parser context
 *   ctemplate(in/out): Class template
 *   supers(in): Queries to add
 *
 * Note : Class object is modified
 */
static int
add_query_to_virtual_class (PARSER_CONTEXT * parser,
			    DB_CTMPL * ctemplate, const PT_NODE * queries)
{
  const char *query = NULL;
  int error = NO_ERROR;
  unsigned int save_custom;

  if (queries == NULL)
    {
      assert (false);
      return NO_ERROR;
    }

  assert (queries->sql_user_text != NULL);

  save_custom = parser->custom_print;
  parser->custom_print |= PT_CHARSET_COLLATE_FULL;

  query = parser_print_tree_with_quotes (parser, queries);
  parser->custom_print = save_custom;

  error = dbt_add_query_spec (ctemplate, query);

  return error;
}

/*
 * do_add_queries() - Adds a list of queries to a virtual class object
 *   return: Error code
 *   parser(in): Parser context
 *   ctemplate(in/out): Class template
 *   queries(in): Queries to add
 *
 * Note : Class object is modified
 */
int
do_add_queries (PARSER_CONTEXT * parser, DB_CTMPL * ctemplate,
		const PT_NODE * queries)
{
  int error = NO_ERROR;

  if (queries)
    {
      assert (queries->next == NULL);	/* is impossible */

      error = add_query_to_virtual_class (parser, ctemplate, queries);
    }

  return error;
}

/*
 * do_create_local() - Creates a new class or vclass
 *   return: Error code if the class/vclass is not created
 *   parser(in): Parser context
 *   ctemplate(in/out): Class template
 *   pt_node(in): Parse tree of a create class/vclass
 */
static int
do_create_local (PARSER_CONTEXT * parser, DB_CTMPL * ctemplate,
		 PT_NODE * pt_node)
{
  int error = NO_ERROR;
  PT_NODE *cnstr;
  PT_NODE *shard_key;

  assert (ctemplate != NULL);
  assert (ctemplate->class_type == SM_CLASS_CT
	  || pt_node->info.create_entity.is_shard == 0);

  shard_key = NULL;		/* init */

  /* check iff shard table */
  if (ctemplate->class_type == SM_CLASS_CT
      && pt_node->info.create_entity.is_shard)
    {
      /* get PK */
      for (cnstr = pt_node->info.create_entity.constraint_list;
	   cnstr != NULL; cnstr = cnstr->next)
	{
	  if (cnstr->info.constraint.type == PT_CONSTRAIN_PRIMARY_KEY)
	    {
	      break;
	    }
	}
      if (cnstr == NULL)
	{
	  /* not permit shard table without PK */
	  assert (false);	/* is impossible */
	  error = ER_SM_PRIMARY_KEY_NOT_EXISTS;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  error, 1, pt_node->info.create_entity.entity_name);
	  return error;
	}

      /* the first column of pk constraint should be shard key column */
      shard_key = cnstr->info.constraint.un.primary_key.attrs;

      assert (shard_key != NULL);
      assert (shard_key->node_type == PT_NAME);
      if (shard_key == NULL || shard_key->node_type != PT_NAME)
	{
	  /* is impossible */
	  error = ER_SHARD_NO_SHARD_KEY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	  return error;
	}
    }

  error = do_add_attributes (parser, ctemplate,
			     pt_node->info.create_entity.attr_def_list,
			     pt_node->info.create_entity.constraint_list,
			     shard_key);
  if (error != NO_ERROR)
    {
      return error;
    }

  error = do_add_constraints (ctemplate,
			      pt_node->info.create_entity.constraint_list);
  if (error != NO_ERROR)
    {
      return error;
    }

  error = do_add_queries (parser, ctemplate,
			  pt_node->info.create_entity.as_query_list);
  if (error != NO_ERROR)
    {
      return error;
    }

  return error;
}

/*
 * do_create_entity() - Creates a new class/vclass
 *   return: Error code if the class/vclass is not created
 *   session(in) : contains the SQL query that has been compiled
 *   node(in/out): Parse tree of a create class/vclass
 */
int
do_create_entity (DB_SESSION * session, PT_NODE * node)
{
  int error = NO_ERROR;
  PARSER_CONTEXT *parser;
  DB_CTMPL *ctemplate = NULL;
  DB_OBJECT *class_obj = NULL;
  const char *class_name = NULL;
  const char *create_like = NULL;
  SM_CLASS *source_class = NULL;
  bool do_abort_class_on_error = false;

  CHECK_MODIFICATION_ERROR ();

  assert (session != NULL);
  assert (session->parser != NULL);

  parser = session->parser;

  class_name = node->info.create_entity.entity_name->info.name.original;

  if (node->info.create_entity.create_like != NULL)
    {
      create_like = node->info.create_entity.create_like->info.name.original;
    }

  switch (node->info.create_entity.entity_type)
    {
    case PT_CLASS:
      if (create_like)
	{
	  ctemplate = dbt_copy_class (class_name, create_like, &source_class);
	}
      else
	{
	  ctemplate = dbt_create_class (class_name);
	}
      break;

    case PT_VCLASS:
      if (node->info.create_entity.or_replace && sm_find_class (class_name))
	{
	  error = drop_class_name (class_name);
	  if (error != NO_ERROR)
	    {
	      goto error_exit;
	    }
	}

      ctemplate = dbt_create_vclass (class_name);
      break;

    default:
      error = ER_GENERIC_ERROR;	/* a system error */
      break;
    }

  if (ctemplate == NULL)
    {
      if (error == NO_ERROR)
	{
	  error = er_errid ();
	}
      goto error_exit;
    }

  do_abort_class_on_error = true;

  if (create_like != NULL)
    {
      /* Nothing left to do; shard_key has already done in dbt_copy_class() */
      assert (source_class != NULL);
    }
  else
    {
      error = do_create_local (parser, ctemplate, node);
    }

  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  class_obj = dbt_finish_class (ctemplate);

  if (class_obj == NULL)
    {
      error = er_errid ();
      goto error_exit;
    }
  do_abort_class_on_error = false;
  ctemplate = NULL;

  switch (node->info.create_entity.entity_type)
    {
    case PT_VCLASS:
      ;				/* nop */
      break;

    case PT_CLASS:
#if !defined(NDEBUG)
      if (create_like)
	{
	  assert (source_class != NULL);
	}
#endif

      if (locator_create_heap_if_needed (class_obj) == NULL)
	{
	  error = er_errid ();
	  assert (error != NO_ERROR);
	  break;
	}

      if (error == NO_ERROR)
	{
	  if (create_like)
	    {
	      int source_is_shard = 0;	/* init */

	      if (sm_isshard_table (source_class))
		{
		  assert (classobj_find_shard_key_column (source_class) !=
			  NULL);
		  source_is_shard = 1;
		}

	      if (node->info.create_entity.is_shard != source_is_shard)
		{
		  /* not match */
		  error = ER_SHARD_CREATE_TABLE_LIKE_MISMATCH;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  error, 2,
			  node->info.create_entity.
			  is_shard ? "SHARD" : "GLOBAL",
			  source_is_shard ? "SHARD" : "GLOBAL");
		  break;
		}
	    }

	  if (node->info.create_entity.is_shard)
	    {
	      error = sm_set_class_flag (class_obj,
					 SM_CLASSFLAG_SHARD_TABLE, 1);
	    }
	}

      break;

    default:
      break;
    }

  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  return error;

error_exit:
  assert (error != NO_ERROR);

  if (do_abort_class_on_error)
    {
      (void) dbt_abort_class (ctemplate);
    }

  return error;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * do_copy_indexes() - Copies all the indexes of a given class to another
 *                     class.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   parser(in): Parser context
 *   classmop(in): the class to copy the indexes to
 *   class_(in): the class to copy the indexes from
 */
static int
do_copy_indexes (PARSER_CONTEXT * parser, MOP classmop, SM_CLASS * src_class)
{
  int error = NO_ERROR;
  const char **att_names = NULL;
  SM_CLASS_CONSTRAINT *c;
  char *new_cons_name = NULL;
  SM_CONSTRAINT_INFO *index_save_info = NULL;
  DB_CONSTRAINT_TYPE constraint_type;
  int free_constraint = 0;

  assert (src_class != NULL);

  if (src_class->constraints == NULL)
    {
      return NO_ERROR;
    }

  for (c = src_class->constraints; c; c = c->next)
    {
      if (c->type != SM_CONSTRAINT_INDEX)
	{
	  /* These should have been copied already. */
	  continue;
	}

      att_names = classobj_point_at_att_names (c, NULL);
      if (att_names == NULL)
	{
	  return er_errid ();
	}

      constraint_type = db_constraint_type (c);
      new_cons_name = (char *) c->name;

      error = sm_add_constraint (classmop, constraint_type, new_cons_name,
				 att_names, c->asc_desc);

      free_and_init (att_names);

      if (new_cons_name != NULL && new_cons_name != c->name)
	{
	  free_and_init (new_cons_name);
	}

      if (free_constraint)
	{
	  sm_free_constraint_info (&index_save_info);
	}

      if (error != NO_ERROR)
	{
	  return error;
	}
    }

  return error;
}
#endif

/*
 * Function Group :
 * Code for truncating Classes by Parse Tree descriptions.
 *
 */

/*
 * do_alter_clause_change_attribute() - Executes an ALTER CHANGE or
 *				        ALTER MODIFY clause
 *   return: Error code
 *   parser(in): Parser context
 *   alter(in/out): Parse tree of a PT_RENAME_ENTITY clause potentially
 *                  followed by the rest of the clauses in the ALTER
 *                  statement.
 */
static int
do_alter_clause_change_attribute (PARSER_CONTEXT * const parser,
				  PT_NODE * const alter)
{
  int error = NO_ERROR;
  const char *entity_name = NULL;
  MOP class_obj = NULL;
  DB_CTMPL *ctemplate = NULL;
  SM_CLASS *sm_class = NULL;
  SM_ATTR_CHG_SOL change_mode = SM_ATTR_CHG_ONLY_SCHEMA;
  SM_ATTR_PROP_CHG attr_chg_prop;
  OID *usr_oid_array = NULL;
  OID class_oid;

  assert (alter->info.alter.code == PT_CHANGE_ATTR);

  OID_SET_NULL (&class_oid);
  reset_att_property_structure (&attr_chg_prop);

  entity_name = alter->info.alter.entity_name->info.name.original;
  if (entity_name == NULL)
    {
      error = ER_UNEXPECTED;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      goto exit;
    }

  class_obj = sm_find_class (entity_name);
  if (class_obj == NULL)
    {
      error = er_errid ();
      goto exit;
    }
  error = au_fetch_class (class_obj, &sm_class, S_LOCK, AU_ALTER);
  if (error != NO_ERROR)
    {
      goto exit;
    }

  /* this ALTER CHANGE syntax supports only one attribute change per
   * ALTER clause */
  assert (alter->info.alter.alter_clause.attr_mthd.attr_def_list->next ==
	  NULL);

  error = check_change_attribute (parser, sm_class,
				  alter->info.alter.alter_clause.attr_mthd.
				  attr_def_list,
				  alter->info.alter.alter_clause.attr_mthd.
				  attr_old_name,
				  alter->info.alter.constraint_list,
				  &attr_chg_prop, &change_mode);
  if (error != NO_ERROR)
    {
      goto exit;
    }

  if (change_mode == SM_ATTR_CHG_NOT_NEEDED)
    {
      /* nothing to do */
      goto exit;
    }
  if (change_mode != SM_ATTR_CHG_ONLY_SCHEMA
      && change_mode != SM_ATTR_CHG_ADD_CONSTRAINT
      && change_mode != SM_ATTR_UPG_CONSTRAINT)
    {
      assert (change_mode == SM_ATTR_CHG_NOT_NEEDED
	      || change_mode == SM_ATTR_CHG_ONLY_SCHEMA
	      || change_mode == SM_ATTR_CHG_ADD_CONSTRAINT
	      || change_mode == SM_ATTR_UPG_CONSTRAINT);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");

      goto exit;
    }

  if (change_mode == SM_ATTR_CHG_ONLY_SCHEMA
      || change_mode == SM_ATTR_UPG_CONSTRAINT)
    {
      ctemplate = smt_edit_class_mop_with_lock (class_obj, X_LOCK);
      if (ctemplate == NULL)
	{
	  /* when dbt_edit_class fails (e.g. because the server unilaterally
	     aborts us), we must record the associated error message into the
	     parser.  Otherwise, we may get a confusing error msg of the form:
	     "so_and_so is not a class". */
	  pt_record_error (parser, alter->line_number, alter->column_number,
			   er_msg (), NULL);
	  error = er_errid ();
	  goto exit;
	}

      error = do_change_att_schema_only (parser, ctemplate,
					 alter->info.alter.alter_clause.
					 attr_mthd.attr_def_list,
					 alter->info.alter.alter_clause.
					 attr_mthd.attr_old_name,
					 alter->info.alter.constraint_list,
					 &attr_chg_prop);

      if (error != NO_ERROR)
	{
	  goto exit;
	}
      COPY_OID (&class_oid, WS_OID (ctemplate->op));
      assert (!OID_ISTEMP (&class_oid));

      /* force schema update to server */
      class_obj = dbt_finish_class (ctemplate);
      if (class_obj == NULL)
	{
	  error = er_errid ();
	  goto exit;
	}

      /* set NULL, avoid 'abort_class' in case of error */
      ctemplate = NULL;
    }
  else if (change_mode == SM_ATTR_CHG_ADD_CONSTRAINT)
    {
      /* create any new constraints: */
      assert (attr_chg_prop.new_constr_info != NULL);
      SM_CONSTRAINT_INFO *ci = NULL;

      error = sort_constr_info_list (&(attr_chg_prop.new_constr_info));
      if (error != NO_ERROR)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_UNEXPECTED, 0);
	  goto exit;
	}

      /* add new constraints */
      for (ci = attr_chg_prop.new_constr_info; ci != NULL; ci = ci->next)
	{
	  assert (ci->constraint_type == DB_CONSTRAINT_UNIQUE
		  || ci->constraint_type == DB_CONSTRAINT_PRIMARY_KEY);

	  error = db_add_constraint (class_obj, ci->constraint_type, NULL,
				     (const char **) ci->att_names);

	  if (error != NO_ERROR)
	    {
	      goto exit;
	    }
	}
    }
  else
    {
      assert (false);
    }

  if (change_mode == SM_ATTR_UPG_CONSTRAINT)
    {
      assert (attr_chg_prop.att_id >= 0 && !OID_ISNULL (&class_oid));
    }

exit:

  if (ctemplate != NULL)
    {
      dbt_abort_class (ctemplate);
      ctemplate = NULL;
    }

  if (attr_chg_prop.constr_info != NULL)
    {
      sm_free_constraint_info (&(attr_chg_prop.constr_info));
    }

  if (attr_chg_prop.new_constr_info != NULL)
    {
      sm_free_constraint_info (&(attr_chg_prop.new_constr_info));
    }

  if (usr_oid_array != NULL)
    {
      free_and_init (usr_oid_array);
    }

  return error;
}

/*
 * do_alter_change_owner() - change the owner of a class/vclass
 *   return: Error code
 *   parser(in): Parser context
 *   alter(in/out): Parse tree of a PT_CHANGE_OWNER claus
 */
static int
do_alter_change_owner (UNUSED_ARG PARSER_CONTEXT * const parser,
		       PT_NODE * const alter)
{
  int error = NO_ERROR;
  DB_OBJECT *obj = NULL;
  DB_VALUE returnval, class_val, user_val;
  PT_NODE *class_, *user;

  assert (alter != NULL);

  class_ = alter->info.alter.entity_name;
  assert (class_ != NULL);

  user = alter->info.alter.alter_clause.user.user_name;
  assert (user != NULL);

  db_make_null (&returnval);

  db_make_string (&class_val, class_->info.name.original);
  db_make_string (&user_val, user->info.name.original);

  error = au_change_owner_helper (obj, &returnval, &class_val, &user_val);

  return error;
}

/*
 * do_change_att_schema_only() - Change an attribute of a class object
 *   return: Error code
 *   parser(in): Parser context
 *   ctemplate(in/out): Class template
 *   attribute(in/out): Attribute to add
 */
static int
do_change_att_schema_only (PARSER_CONTEXT * parser, DB_CTMPL * ctemplate,
			   PT_NODE * attribute, PT_NODE * old_name_node,
			   UNUSED_ARG PT_NODE * constraints,
			   SM_ATTR_PROP_CHG * attr_chg_prop)
{
  DB_DOMAIN *attr_db_domain = NULL;
  int error = NO_ERROR;
  bool change_first = false;
  const char *change_after_attr = NULL;
#if 0
  const char *old_name = NULL;
  const char *new_name = NULL;
#endif
  const char *attr_name = NULL;

  assert (attr_chg_prop != NULL);

  assert (attribute->node_type == PT_ATTR_DEF);

  assert (is_att_prop_set (attr_chg_prop->p[P_NAME],
			   ATT_CHG_PROPERTY_DIFF)
	  || is_att_prop_set (attr_chg_prop->p[P_TYPE],
			      ATT_CHG_TYPE_PREC_INCR)
	  || is_att_prop_set (attr_chg_prop->p[P_NOT_NULL],
			      ATT_CHG_PROPERTY_LOST)
	  || is_att_prop_set (attr_chg_prop->p[P_ORDER],
			      ATT_CHG_PROPERTY_GAINED));

  attr_name = get_attr_name (attribute);

  /* change name */
  if (is_att_prop_set (attr_chg_prop->p[P_NAME], ATT_CHG_PROPERTY_DIFF))
    {
      if (old_name_node == NULL)
	{
	  assert (old_name_node != NULL);

	  error = ER_GENERIC_ERROR;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");

	  goto exit;
	}

      assert (old_name_node->node_type == PT_NAME);
#if 0
      old_name = old_name_node->info.name.original;
      new_name = attr_name;
#endif

      error = smt_rename_any (ctemplate, old_name_node->info.name.original,
			      attr_name);
      if (error != NO_ERROR)
	{
	  goto exit;
	}
    }

  /* change order */
  if (is_att_prop_set (attr_chg_prop->p[P_ORDER], ATT_CHG_PROPERTY_GAINED))
    {
      error = get_att_order_from_def (attribute, &change_first,
				      &change_after_attr);
      if (error != NO_ERROR)
	{
	  goto exit;
	}

      if (change_first == true || change_after_attr != NULL)
	{
	  error =
	    smt_change_attribute_pos (ctemplate, attr_name, change_first,
				      change_after_attr);
	  if (error != NO_ERROR)
	    {
	      goto exit;
	    }
	}
    }

  /* drop not null constraint */
  if (is_att_prop_set (attr_chg_prop->p[P_NOT_NULL], ATT_CHG_PROPERTY_LOST))
    {
      error = dbt_constrain_non_null (ctemplate, attr_name, 0);
      if (error != NO_ERROR)
	{
	  goto exit;
	}
    }

  /* precision increase */
  if (is_att_prop_set (attr_chg_prop->p[P_TYPE], ATT_CHG_TYPE_PREC_INCR))
    {
      attr_db_domain = pt_node_to_db_domain (parser, attribute,
					     ctemplate->name);
      if (attr_db_domain == NULL)
	{
	  error = er_errid ();
	  if (error == NO_ERROR)
	    {
	      error = ER_GENERIC_ERROR;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
	    }

	  goto exit;
	}
      attr_db_domain = tp_domain_cache (attr_db_domain);

      error = smt_change_attribute_domain (ctemplate, attr_name,
					   attr_db_domain);
      if (error != NO_ERROR)
	{
	  goto exit;
	}
    }

exit:
  return error;
}

/*
 * build_attr_change_map() - This builds a map of changes on the attribute
 *   return: Error code
 *   parser(in): Parser context
 *   sm_class(in): direct pointer to class structure
 *   attr_def(in): New attribute definition (PT_NODE : PT_ATTR_DEF)
 *   attr_old_name(in): Old name of attribute (PT_NODE : PT_NAME)
 *   constraints(in): New constraints for class template
 *		      (PT_NODE : PT_CONSTRAINT)
 *   attr_chg_properties(out): map of attribute changes to build
 *
 */
static int
build_attr_change_map (PARSER_CONTEXT * parser,
		       SM_CLASS * sm_class,
		       PT_NODE * attr_def,
		       PT_NODE * attr_old_name,
		       PT_NODE * constraints,
		       SM_ATTR_PROP_CHG * attr_chg_properties)
{
  DB_DOMAIN *attr_db_domain = NULL;
  SM_ATTRIBUTE *att = NULL;
  const char *old_name = NULL;
  const char *new_name = NULL;
  int error = NO_ERROR;
  int i;

  if (sm_class == NULL)
    {
      assert (sm_class != NULL);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
      return error;
    }

  new_name = get_attr_name (attr_def);

  /* attribute name            */
  attr_chg_properties->p[P_NAME] = 0;
  attr_chg_properties->p[P_NAME] |= ATT_CHG_PROPERTY_PRESENT_OLD;
  if (attr_old_name != NULL)
    {
      assert (attr_old_name->node_type == PT_NAME);
      attr_chg_properties->p[P_NAME] |= ATT_CHG_PROPERTY_PRESENT_NEW;

      /* attr_name is supplied using the ATTR_DEF node and it means:
       *  for MODIFY syntax : current and unchanged node (attr_name)
       *  for CHANGE syntax : new name of the attribute  (new_name)
       */
      old_name = attr_old_name->info.name.original;
    }
  else
    {
      old_name = new_name;
    }

  /* at this point, old_name is the current name of the attribute,
   * new_name is either the desired new name or NULL, if name change is not
   * requested */

  /* get the attribute structure */
  att = classobj_find_attribute (sm_class->attributes, old_name);
  if (att == NULL)
    {
      error = ER_SM_ATTRIBUTE_NOT_FOUND;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, old_name);

      return error;
    }
  attr_chg_properties->att_id = att->id;

  assert (att != NULL);
  assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

  /* DEFAULT value             */
  attr_chg_properties->p[P_DEFAULT_VALUE] = 0;
  if (attr_def->info.attr_def.data_default != NULL)
    {
      attr_chg_properties->p[P_DEFAULT_VALUE] |= ATT_CHG_PROPERTY_PRESENT_NEW;
    }
  if (!DB_IS_NULL (&(att->default_value.original_value))
      || !DB_IS_NULL (&(att->default_value.value))
      || att->default_value.default_expr != DB_DEFAULT_NONE)
    {
      attr_chg_properties->p[P_DEFAULT_VALUE] |= ATT_CHG_PROPERTY_PRESENT_OLD;
    }

  /* DEFFERABLE : not supported, just mark as checked */
  attr_chg_properties->p[P_DEFFERABLE] = 0;

  /* constraint  CHECK : not supported, just mark as checked */
  attr_chg_properties->p[P_CONSTR_CHECK] = 0;

  /* ORDERING */
  attr_chg_properties->p[P_ORDER] = 0;
  if (attr_def->info.attr_def.ordering_info != NULL)
    {
      attr_chg_properties->p[P_ORDER] |= ATT_CHG_PROPERTY_PRESENT_NEW;
    }

  /* constraint : NOT NULL */
  attr_chg_properties->p[P_NOT_NULL] = 0;
  if (att->flags & SM_ATTFLAG_NON_NULL)
    {
      attr_chg_properties->p[P_NOT_NULL] |= ATT_CHG_PROPERTY_PRESENT_OLD;
    }

  /* constraint : pk, unique, non-unique idx */
  attr_chg_properties->p[P_S_CONSTR_PK] = 0;
  attr_chg_properties->p[P_M_CONSTR_PK] = 0;
  attr_chg_properties->p[P_S_CONSTR_UNI] = 0;
  attr_chg_properties->p[P_M_CONSTR_UNI] = 0;
  attr_chg_properties->p[P_CONSTR_NON_UNI] = 0;

  error = get_old_property_present_of_constraints (attr_chg_properties,
						   sm_class->constraints,
						   old_name);
  if (error != NO_ERROR)
    {
      return error;
    }

  error = get_new_property_present_of_constraints (attr_chg_properties,
						   constraints, new_name);
  if (error != NO_ERROR)
    {
      return error;
    }

  /* TYPE */
  attr_chg_properties->p[P_TYPE] = 0;
  attr_chg_properties->p[P_TYPE] |= ATT_CHG_PROPERTY_PRESENT_OLD;
  attr_chg_properties->p[P_TYPE] |= ATT_CHG_PROPERTY_PRESENT_NEW;


  /*****************************/
  /* consolidate properties : */
  /*****************************/
  for (i = 0; i < NUM_ATT_CHG_PROP; i++)
    {
      int *const p = &(attr_chg_properties->p[i]);

      assert (!is_att_prop_set (*p, ATT_CHG_PROPERTY_DIFF)
	      && !is_att_prop_set (*p, ATT_CHG_PROPERTY_UNCHANGED));

      if (*p & ATT_CHG_PROPERTY_PRESENT_OLD)
	{
	  if (*p & ATT_CHG_PROPERTY_PRESENT_NEW)
	    {
	      *p |= ATT_CHG_PROPERTY_UNCHANGED;
	    }
	  else
	    {
	      *p |= ATT_CHG_PROPERTY_LOST;
	    }
	}
      else
	{
	  if (*p & ATT_CHG_PROPERTY_PRESENT_NEW)
	    {
	      *p |= ATT_CHG_PROPERTY_GAINED;
	    }
	  else
	    {
	      *p |= ATT_CHG_PROPERTY_UNCHANGED;
	    }
	}
    }

  /* NAME */
  if (is_att_prop_set (attr_chg_properties->p[P_NAME],
		       ATT_CHG_PROPERTY_PRESENT_OLD)
      && is_att_prop_set (attr_chg_properties->p[P_NAME],
			  ATT_CHG_PROPERTY_PRESENT_NEW))
    {
      assert (is_att_prop_set (attr_chg_properties->p[P_NAME],
			       ATT_CHG_PROPERTY_UNCHANGED));

      if (intl_identifier_casecmp (old_name, new_name) != 0)
	{
	  attr_chg_properties->p[P_NAME] |= ATT_CHG_PROPERTY_DIFF;
	  /* remove UNCHANGED flag */
	  attr_chg_properties->p[P_NAME] &= ~ATT_CHG_PROPERTY_UNCHANGED;
	}
    }

  /* DEFAULT */
  if (is_att_prop_set (attr_chg_properties->p[P_DEFAULT_VALUE],
		       ATT_CHG_PROPERTY_PRESENT_OLD)
      && is_att_prop_set (attr_chg_properties->p[P_DEFAULT_VALUE],
			  ATT_CHG_PROPERTY_PRESENT_NEW))
    {
      DB_VALUE value, *new_default_value;
      DB_DEFAULT_EXPR_TYPE new_def_expr;

      assert (is_att_prop_set (attr_chg_properties->p[P_DEFAULT_VALUE],
			       ATT_CHG_PROPERTY_UNCHANGED));

      new_def_expr =
	attr_def->info.attr_def.data_default->info.data_default.default_expr;

      new_default_value = &value;
      DB_MAKE_NULL (new_default_value);
      error = get_att_default_from_def (parser, attr_def, &new_default_value);
      if (error != NO_ERROR)
	{
	  db_value_clear (&value);
	  return error;
	}

      assert (attr_def->info.attr_def.data_default != NULL);
      assert (attr_def->info.attr_def.data_default->node_type ==
	      PT_DATA_DEFAULT);
      assert (att->default_value.default_expr != DB_DEFAULT_NONE
	      || !DB_IS_NULL (&att->default_value.value));
      assert (new_def_expr != DB_DEFAULT_NONE
	      || !DB_IS_NULL (new_default_value));

      if (new_def_expr != att->default_value.default_expr
	  || (new_default_value != NULL
	      && !tp_value_equal (new_default_value,
				  &att->default_value.value, 1)))
	{
	  attr_chg_properties->p[P_DEFAULT_VALUE] |= ATT_CHG_PROPERTY_DIFF;
	  /* remove UNCHANGED flag */
	  attr_chg_properties->p[P_DEFAULT_VALUE] &=
	    ~ATT_CHG_PROPERTY_UNCHANGED;
	}

      db_value_clear (&value);
    }

  /* TYPE */
  attr_db_domain =
    pt_node_to_db_domain (parser, attr_def, sm_class->header.name);
  if (attr_db_domain == NULL)
    {
      return (er_errid ());
    }
  attr_db_domain = tp_domain_cache (attr_db_domain);

  if (tp_domain_match (attr_db_domain, att->sma_domain, TP_EXACT_MATCH) != 1)
    {
      assert (attr_db_domain->type != NULL);

      /* remove "UNCHANGED" flag */
      attr_chg_properties->p[P_TYPE] &= ~ATT_CHG_PROPERTY_UNCHANGED;

      if (TP_DOMAIN_TYPE (attr_db_domain) != TP_DOMAIN_TYPE (att->sma_domain))
	{
	  attr_chg_properties->p[P_TYPE] |= ATT_CHG_PROPERTY_DIFF;
	  attr_chg_properties->p[P_TYPE] |= ATT_CHG_TYPE_UPGRADE;
	}
      else
	{
	  if (TP_IS_CHAR_BIT_TYPE (TP_DOMAIN_TYPE (attr_db_domain))
	      && tp_domain_match (attr_db_domain, att->sma_domain,
				  TP_STR_MATCH))
	    {
	      attr_chg_properties->p[P_TYPE] |= ATT_CHG_TYPE_PREC_INCR;
	    }
	  else
	    {
	      attr_chg_properties->p[P_TYPE] |= ATT_CHG_TYPE_UPGRADE;
	    }
	}
    }

  return error;
}

/*
 * is_att_property_structure_checked() - Checks all properties from the
 *				    attribute change properties structure
 *
 *   return : true, if all properties are marked as checked, false otherwise
 *   attr_chg_properties(in): structure summarizing the changed properties of
 *                            attribute
 *
 */
static bool
is_att_property_structure_checked (const SM_ATTR_PROP_CHG *
				   attr_chg_properties)
{
  int i = 0;

  for (i = 0; i < NUM_ATT_CHG_PROP; i++)
    {
      if (attr_chg_properties->p[i] >= ATT_CHG_PROPERTY_NOT_CHECKED)
	{
	  return false;
	}
    }
  return true;
}

/*
 * is_att_change_needed() - Checks all properties from the attribute change
 *			    properties structure and decides if the schema
 *			    update is necessary
 *
 *   return : true, if schema change is needed, false otherwise
 *   attr_chg_properties(in): structure summarizing the changed properties of
 *			      attribute
 *
 */
static bool
is_att_change_needed (const SM_ATTR_PROP_CHG * attr_chg_properties)
{
  int i = 0;

  for (i = 0; i < NUM_ATT_CHG_PROP; i++)
    {
      if (attr_chg_properties->p[i] >= ATT_CHG_PROPERTY_DIFF)
	{
	  return true;
	}

      if (!is_att_prop_set (attr_chg_properties->p[i],
			    ATT_CHG_PROPERTY_UNCHANGED))
	{
	  return true;
	}
    }
  return false;
}

/*
 * is_att_prop_set() - Checks that the properties has the flag set
 *
 *   return : true, if property has the value flag set, false otherwise
 *   prop(in): property
 *   value(in): value
 *
 */
static bool
is_att_prop_set (const int prop, const int value)
{
  return ((prop & value) == value);
}

/*
 * reset_att_property_structure() - Resets the attribute change properties
 *				    structure, so that all properties are
 *				    marked as 'unchecked'
 *
 *   attr_chg_properties(in): structure summarizing the changed properties of
 *                            attribute
 *
 */
static void
reset_att_property_structure (SM_ATTR_PROP_CHG * attr_chg_properties)
{
  int i = 0;

  assert (sizeof (attr_chg_properties->p) / sizeof (int) == NUM_ATT_CHG_PROP);

  for (i = 0; i < NUM_ATT_CHG_PROP; i++)
    {
      attr_chg_properties->p[i] = ATT_CHG_PROPERTY_NOT_CHECKED;
    }

  attr_chg_properties->constr_info = NULL;
  attr_chg_properties->new_constr_info = NULL;
  attr_chg_properties->att_id = -1;
}

/*
 * get_att_order_from_def() - Retrieves the order properties (first,
 *			   after name) from the attribute definition node
 *
 *  return : NO_ERROR, if success; error code otherwise
 *  attribute(in): attribute definition node (PT_ATTR_DEF)
 *  ord_first(out): true if definition contains 'FIRST' specification, false
 *		    otherwise
 *  ord_after_name(out): name of column 'AFTER <col_name>'
 *
 */
static int
get_att_order_from_def (PT_NODE * attribute, bool * ord_first,
			const char **ord_after_name)
{
  PT_NODE *ordering_info = NULL;

  assert (attribute->node_type == PT_ATTR_DEF);

  ordering_info = attribute->info.attr_def.ordering_info;
  if (ordering_info != NULL)
    {
      assert (ordering_info->node_type == PT_ATTR_ORDERING);

      *ord_first = ordering_info->info.attr_ordering.first;

      if (ordering_info->info.attr_ordering.after != NULL)
	{
	  PT_NODE *const after_name = ordering_info->info.attr_ordering.after;

	  assert (after_name->node_type == PT_NAME);
	  *ord_after_name = after_name->info.name.original;
	  assert (*ord_first == false);
	}
      else
	{
	  *ord_after_name = NULL;
	  /*
	   * If we have no "AFTER name" then this must have been a "FIRST"
	   * token
	   */
	  assert (*ord_first == true);
	}
    }
  else
    {
      *ord_first = false;
      *ord_after_name = NULL;
    }

  return NO_ERROR;
}

/*
 * get_att_default_from_def() - Retrieves the default value property from the
 *				attribute definition node
 *
 *  return : NO_ERROR, if success; error code otherwise
 *  parser(in): parser context
 *  attribute(in): attribute definition node (PT_ATTR_DEF)
 *  default_value(in/out): default value; this must be initially passed as
 *			   pointer to an allocated DB_VALUE; it is returned
 *			   as NULL if a DEFAULT is not specified for the
 *			   attribute, otherwise the DEFAULT value is returned
 *			   (the initially passed value is used for storage)
 *
 */
static int
get_att_default_from_def (PARSER_CONTEXT * parser, PT_NODE * attribute,
			  DB_VALUE ** default_value)
{
  int error = NO_ERROR;

  assert (attribute->node_type == PT_ATTR_DEF);

  if (attribute->info.attr_def.data_default == NULL)
    {
      *default_value = NULL;
    }
  else
    {
      PT_NODE *def_val = NULL;
      DB_DEFAULT_EXPR_TYPE def_expr;
      PT_TYPE_ENUM desired_type = attribute->type_enum;

      def_expr = attribute->info.attr_def.data_default->info.data_default.
	default_expr;
      /* try to coerce the default value into the attribute's type */
      def_val =
	attribute->info.attr_def.data_default->info.data_default.
	default_value;

      /* check iff is not query */
      if (def_val == NULL || PT_IS_QUERY (def_val))
	{
	  PT_ERRORmf (parser, def_val, MSGCAT_SET_PARSER_SEMANTIC,
		      MSGCAT_SEMANTIC_INVALID_FIELD_DEFAULT_VALUE,
		      pt_short_print (parser, attribute));

	  return ER_IT_INCOMPATIBLE_DATATYPE;
	}

      def_val = pt_semantic_check (parser, def_val);
      if (pt_has_error (parser) || def_val == NULL)
	{
	  pt_report_to_ersys (parser, PT_SEMANTIC);
	  return er_errid ();
	}

      if (def_expr == DB_DEFAULT_NONE)
	{
	  error = pt_coerce_value (parser, def_val, def_val, desired_type,
				   attribute->data_type);
	  if (error != NO_ERROR)
	    {
	      return error;
	    }
	}
      else
	{
	  DB_VALUE src, dest;
	  TP_DOMAIN *d = NULL;

	  DB_MAKE_NULL (&src);
	  DB_MAKE_NULL (&dest);

	  def_val = pt_semantic_type (parser, def_val, NULL);
	  if (pt_has_error (parser) || def_val == NULL)
	    {
	      pt_report_to_ersys (parser, PT_SEMANTIC);
	      return er_errid ();
	    }

	  error = pt_evaluate_def_val (parser, def_val, &src);
	  if (error != NO_ERROR)
	    {
	      return error;
	    }

	  d = pt_type_enum_to_db_domain (desired_type);
	  d = tp_domain_cache (d);
	  if (tp_value_coerce (&src, &dest, d) != DOMAIN_COMPATIBLE)
	    {
	      PT_ERRORmf2 (parser, def_val, MSGCAT_SET_PARSER_SEMANTIC,
			   MSGCAT_SEMANTIC_CANT_COERCE_TO,
			   pt_short_print (parser, def_val),
			   pt_show_type_enum ((PT_TYPE_ENUM) desired_type));

	      db_value_clear (&src);
	      db_value_clear (&dest);

	      return ER_IT_INCOMPATIBLE_DATATYPE;
	    }

	  db_value_clear (&src);
	  db_value_clear (&dest);
	}

      if (def_expr == DB_DEFAULT_NONE)
	{
	  error = pt_evaluate_def_val (parser, def_val, *default_value);
	  if (error != NO_ERROR)
	    {
	      return error;
	    }
	}
      else
	{
	  *default_value = NULL;
	}

      if (pt_has_error (parser))
	{
	  pt_report_to_ersys (parser, PT_SEMANTIC);
	  return er_errid ();
	}
    }

  return error;
}

/*
 * get_old_property_present_of_constraints -
 *    return: NO_ERROR or error code
 *
 *    attr_chg_properties(in/out):
 *    cons(in):
 */
static int
get_old_property_present_of_constraints (SM_ATTR_PROP_CHG *
					 attr_chg_properties,
					 SM_CLASS_CONSTRAINT * cons,
					 const char *attr_name)
{
  SM_CLASS_CONSTRAINT *curr_cons;
  SM_ATTRIBUTE **cons_att;
  int i;
  int found_att;
  int error = NO_ERROR;

  for (curr_cons = cons; curr_cons != NULL; curr_cons = curr_cons->next)
    {
      /* check if attribute is contained in this constraint */
      found_att = -1;
      cons_att = curr_cons->attributes;
      for (i = 0; cons_att[i] != NULL; i++)
	{
	  if (cons_att[i]->name == NULL)
	    {
	      assert (false);

	      error = ER_OBJ_TEMPLATE_INTERNAL;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

	      return error;
	    }

	  if (intl_identifier_casecmp (cons_att[i]->name, attr_name) == 0)
	    {
	      found_att = i;
	    }
	}
      assert (i == curr_cons->num_atts);

      if (found_att != -1)
	{
	  switch (curr_cons->type)
	    {
	    case SM_CONSTRAINT_PRIMARY_KEY:
	      assert (curr_cons->num_atts >= 1);
	      assert (is_att_prop_set (attr_chg_properties->p[P_NOT_NULL],
				       ATT_CHG_PROPERTY_PRESENT_OLD));

	      if (curr_cons->num_atts >= 2)
		{
		  attr_chg_properties->p[P_M_CONSTR_PK] |=
		    ATT_CHG_PROPERTY_PRESENT_OLD;
		}
	      else
		{
		  attr_chg_properties->p[P_S_CONSTR_PK] |=
		    ATT_CHG_PROPERTY_PRESENT_OLD;
		}
	      attr_chg_properties->p[P_NOT_NULL] |=
		ATT_CHG_PROPERTY_PRESENT_NEW;
	      break;

	    case SM_CONSTRAINT_INDEX:
	      assert (curr_cons->num_atts >= 1);

	      attr_chg_properties->p[P_CONSTR_NON_UNI] |=
		ATT_CHG_PROPERTY_PRESENT_OLD;

	      break;
	    case SM_CONSTRAINT_UNIQUE:
	      assert (curr_cons->num_atts >= 1);
	      if (curr_cons->num_atts >= 2)
		{
		  attr_chg_properties->p[P_M_CONSTR_UNI] |=
		    ATT_CHG_PROPERTY_PRESENT_OLD;
		}
	      else
		{
		  attr_chg_properties->p[P_S_CONSTR_UNI] |=
		    ATT_CHG_PROPERTY_PRESENT_OLD;
		}

	      break;
	    case SM_CONSTRAINT_NOT_NULL:
	      assert (is_att_prop_set (P_NOT_NULL,
				       ATT_CHG_PROPERTY_PRESENT_OLD));
	      break;
	    default:
	      assert (false);

	      error = ER_GENERIC_ERROR;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");

	      return error;
	    }

	  error = sm_save_constraint_info (&(attr_chg_properties->
					     constr_info), curr_cons);
	  if (error != NO_ERROR)
	    {
	      return error;
	    }
	}
    }

  return NO_ERROR;
}

/*
 * get_new_property_present_of_constraints -
 *
 *    return: NO_ERROR or error code
 *    attr_chg_properties(in/out):
 *    cons(in):
 *    attr_name(in):
 */
static int
get_new_property_present_of_constraints (SM_ATTR_PROP_CHG *
					 attr_chg_properties, PT_NODE * cons,
					 const char *attr_name)
{
  PT_NODE *cnstr;
  PT_NODE *constr_att = NULL;
  PT_NODE *constr_att_list = NULL;
  int chg_prop_idx = NUM_ATT_CHG_PROP;
  int error = NO_ERROR;

  /* check for constraints in the new attribute definition */
  for (cnstr = cons; cnstr != NULL; cnstr = cnstr->next)
    {
      assert (cnstr->node_type == PT_CONSTRAINT);

      constr_att_list = NULL;
      switch (cnstr->info.constraint.type)
	{
	case PT_CONSTRAIN_PRIMARY_KEY:
	  constr_att_list = cnstr->info.constraint.un.primary_key.attrs;
	  chg_prop_idx = P_S_CONSTR_PK;
	  break;
	case PT_CONSTRAIN_UNIQUE:
	  constr_att_list = cnstr->info.constraint.un.unique.attrs;
	  chg_prop_idx = P_S_CONSTR_UNI;
	  break;
	case PT_CONSTRAIN_NOT_NULL:
	  constr_att_list = cnstr->info.constraint.un.not_null.attr;
	  chg_prop_idx = P_NOT_NULL;
	  break;
	case PT_CONSTRAIN_CHECK:
	  /* not supported, just mark as 'PRESENT' */
	  assert (false);
	  attr_chg_properties->p[P_CONSTR_CHECK] |=
	    ATT_CHG_PROPERTY_PRESENT_NEW;
	  continue;
	default:
	  assert (false);
	  continue;
	}

      for (constr_att = constr_att_list; constr_att != NULL;
	   constr_att = constr_att->next)
	{
	  assert (constr_att->node_type == PT_NAME);
	  if (intl_identifier_casecmp (attr_name,
				       constr_att->info.name.original) == 0)
	    {
	      assert (0 <= chg_prop_idx && chg_prop_idx < NUM_ATT_CHG_PROP);

	      if (chg_prop_idx >= NUM_ATT_CHG_PROP)
		{
		  continue;
		}

	      /* save new constraint only if it is not already present
	       * in current template*/
	      if (!is_att_prop_set (attr_chg_properties->p[chg_prop_idx],
				    ATT_CHG_PROPERTY_PRESENT_OLD))
		{
		  error = save_constraint_info_from_pt_node
		    (&(attr_chg_properties->new_constr_info), cnstr);
		  if (error != NO_ERROR)
		    {
		      return error;
		    }
		}

	      attr_chg_properties->p[chg_prop_idx] |=
		ATT_CHG_PROPERTY_PRESENT_NEW;
	      break;
	    }
	}
    }

  return NO_ERROR;
}


/*
 * is_attribute_primary_key() - Returns true if the attribute given is part
 *				of the primary key of the table.
 *
 *
 *  return : true or false
 *  class_name(in): the class name
 *  attr_name(in):  the attribute name
 *
 */
static bool
is_attribute_primary_key (const char *class_name, const char *attr_name)
{
  DB_ATTRIBUTE *db_att = NULL;

  if (class_name == NULL || attr_name == NULL)
    {
      return false;
    }

  db_att = db_get_attribute_by_name (class_name, attr_name);

  if (db_att && db_attribute_is_primary_key (db_att))
    {
      return true;
    }
  return false;
}




/*
 * do_update_new_notnull_cols_without_default()
 * Populates the newly added columns with hard-coded defaults.
 *
 * Used only on ALTER TABLE ... ADD COLUMN, and only AFTER the operation has
 * been performed (i.e. the columns have been added to the schema, even
 * though the transaction has not been committed).
 *
 * IF the clause has added columns that:
 *   1. have no default value AND
 *     2a. have the NOT NULL constraint OR
 *     2b. are part of the PRIMARY KEY
 * THEN try to fill them with a hard-coded default (zero, empty string etc.)
 *
 * This is done in MySQL compatibility mode, to ensure consistency: otherwise
 * columns with the NOT NULL constraint would have ended up being filled
 * with NULL as a default.
 *
 * NOTE: there are types (such as OBJECT) that do not have a "zero"-like
 * value, and if we encounter one of these, we block the entire operation.
 *
 *   return: Error code if operation fails or if one of the attributes to add
 *           is of type OBJECT, with NOT NULL and no default value.
 *   parser(in): Parser context
 *   alter(in):  Parse tree of the statement
 */
static int
do_update_new_notnull_cols_without_default (UNUSED_ARG PARSER_CONTEXT *
					    parser, PT_NODE * alter,
					    UNUSED_ARG MOP class_mop)
{
  int error = NO_ERROR;

  PT_NODE *attr = NULL;

  assert (alter->node_type == PT_ALTER);
  assert (alter->info.alter.code == PT_ADD_ATTR_MTHD);

  /* Look for attributes that: have NOT NULL, do not have a DEFAULT
   * and their type has a "hard" default.
   * Also look for attributes that are primary keys
   * Throw an error for types that do not have a hard default (like objects).
   */
  for (attr = alter->info.alter.alter_clause.attr_mthd.attr_def_list;
       attr; attr = attr->next)
    {
      const bool is_not_null = (attr->info.attr_def.constrain_not_null != 0);
      const bool has_default = (attr->info.attr_def.data_default != NULL);
      const bool is_pri_key =
	is_attribute_primary_key (alter->info.alter.entity_name->info.name.
				  original,
				  attr->info.attr_def.attr_name->info.name.
				  original);
      if (has_default)
	{
	  continue;
	}

      if (!is_not_null && !is_pri_key)
	{
	  continue;
	}

      ERROR1 (error, ER_SM_ATTR_NOT_NULL,
	      attr->info.attr_def.attr_name->info.name.original);
      goto end;
    }

  /* no interesting attribute found, just leave */

end:

  return error;
}

/*
 * check_change_attribute() - Checks if an attribute change attribute is
 *			      possible, in the context of the requested
 *			      change mode
 *   return: Error code
 *   parser(in): Parser context
 *   sm_class(in):
 *   attribute(in): Attribute to add
 */
static int
check_change_attribute (PARSER_CONTEXT * parser, SM_CLASS * sm_class,
			PT_NODE * attribute, PT_NODE * old_name_node,
			PT_NODE * constraints,
			SM_ATTR_PROP_CHG * attr_chg_prop,
			SM_ATTR_CHG_SOL * change_mode)
{
  int error = NO_ERROR;
  const char *attr_name = NULL;
  DB_VALUE def_value;
  DB_VALUE *ptr_def = &def_value;
  PT_NODE *cnstr;

  assert (attr_chg_prop != NULL);
  assert (change_mode != NULL);

  assert (attribute->node_type == PT_ATTR_DEF);

  *change_mode = SM_ATTR_CHG_UNKNOWN;

  DB_MAKE_NULL (&def_value);

  attr_name = get_attr_name (attribute);

  error = get_att_default_from_def (parser, attribute, &ptr_def);
  if (error != NO_ERROR)
    {
      goto exit;
    }
  /* ptr_def is either NULL or pointing to address of def_value */
  assert (ptr_def == NULL || ptr_def == &def_value);

  if (ptr_def && DB_IS_NULL (ptr_def)
      && attribute->info.attr_def.data_default->info.data_default.
      default_expr == DB_DEFAULT_NONE)
    {
      for (cnstr = constraints; cnstr != NULL; cnstr = cnstr->next)
	{
	  if (cnstr->info.constraint.type == PT_CONSTRAIN_NOT_NULL)
	    {
	      /* don't allow a default value of NULL for NOT NULL
	       * constrained columns */
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_CANNOT_HAVE_NOTNULL_DEFAULT_NULL, 1, attr_name);
	      error = ER_CANNOT_HAVE_NOTNULL_DEFAULT_NULL;
	      goto exit;
	    }
	  else if (cnstr->info.constraint.type == PT_CONSTRAIN_PRIMARY_KEY)
	    {
	      /* don't allow a default value of NULL in new PK constraint */
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_CANNOT_HAVE_PK_DEFAULT_NULL, 1, attr_name);
	      error = ER_CANNOT_HAVE_PK_DEFAULT_NULL;
	      goto exit;
	    }
	}
    }

  error = build_attr_change_map (parser, sm_class, attribute, old_name_node,
				 constraints, attr_chg_prop);
  if (error != NO_ERROR)
    {
      goto exit;
    }

  if (!is_att_change_needed (attr_chg_prop))
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE,
	      ER_ALTER_CHANGE_WARN_NO_CHANGE, 1, attr_name);
      error = NO_ERROR;
      /* just a warning : nothing to do */
      *change_mode = SM_ATTR_CHG_NOT_NEEDED;
      goto exit;
    }

  if (!is_att_property_structure_checked (attr_chg_prop))
    {
      assert (false);

      error = ER_UNEXPECTED;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      goto exit;
    }

  /* DEFAULT value spec */
  if (is_att_prop_set (attr_chg_prop->p[P_DEFAULT_VALUE],
		       ATT_CHG_PROPERTY_GAINED)
      || is_att_prop_set (attr_chg_prop->p[P_DEFAULT_VALUE],
			  ATT_CHG_PROPERTY_LOST)
      || is_att_prop_set (attr_chg_prop->p[P_DEFAULT_VALUE],
			  ATT_CHG_PROPERTY_DIFF))
    {
      error = ER_ALTER_CANNOT_CHANGE_DEFAULT_VALUE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

      goto exit;
    }

  /* TYPE */
  if (is_att_prop_set (attr_chg_prop->p[P_TYPE],
		       ATT_CHG_TYPE_UPGRADE)
      || is_att_prop_set (attr_chg_prop->p[P_TYPE], ATT_CHG_PROPERTY_DIFF))
    {
      error = ER_ALTER_CHANGE_TYPE_NOT_SUPP;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, attr_name);

      goto exit;
    }

  /* NOT NULL spec */
  if (is_att_prop_set (attr_chg_prop->p[P_NOT_NULL], ATT_CHG_PROPERTY_GAINED))
    {
      error = ER_SM_NOT_NULL_NOT_ALLOWED;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, attr_name);

      goto exit;
    }

  if (is_att_prop_set (attr_chg_prop->p[P_NAME], ATT_CHG_PROPERTY_DIFF)
      || is_att_prop_set (attr_chg_prop->p[P_NOT_NULL], ATT_CHG_PROPERTY_LOST)
      || is_att_prop_set (attr_chg_prop->p[P_ORDER], ATT_CHG_PROPERTY_GAINED))
    {
      *change_mode = SM_ATTR_CHG_ONLY_SCHEMA;
    }

  if (is_att_prop_set (attr_chg_prop->p[P_TYPE], ATT_CHG_TYPE_PREC_INCR))
    {
      *change_mode = SM_ATTR_UPG_CONSTRAINT;
    }

  if (is_att_prop_set (attr_chg_prop->p[P_S_CONSTR_PK],
		       ATT_CHG_PROPERTY_GAINED)
      || is_att_prop_set (attr_chg_prop->p[P_S_CONSTR_UNI],
			  ATT_CHG_PROPERTY_GAINED))
    {
      if (*change_mode == SM_ATTR_CHG_ONLY_SCHEMA
	  || *change_mode == SM_ATTR_UPG_CONSTRAINT)
	{
	  error = ER_ALTER_CANNOT_UPDATE_HEAP_DATA;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);

	  goto exit;
	}

      *change_mode = SM_ATTR_CHG_ADD_CONSTRAINT;
    }

  if (*change_mode == SM_ATTR_CHG_UNKNOWN)
    {
      *change_mode = SM_ATTR_CHG_NOT_NEEDED;
    }

exit:
  db_value_clear (&def_value);
  return error;
}

/*
 * sort_constr_info_list - sorts the list of constraints in the order:
 *			   - non-unique indexes
 *			   - unique indexes
 *			   - primary keys
 *   return: none
 *   source(in/out): list to sort
 */

static int
sort_constr_info_list (SM_CONSTRAINT_INFO ** orig_list)
{
  int error = NO_ERROR;
  SM_CONSTRAINT_INFO *sorted, *next, *prev, *ins, *found, *constr;
  int constr_order[5] = { 0 };

  assert (orig_list != NULL);

  if (*orig_list == NULL)
    {
      return error;
    }

  /* TODO change this to compile-time asserts when we have such a mechanism.
   */
  assert (DB_CONSTRAINT_UNIQUE == 0);

  constr_order[DB_CONSTRAINT_UNIQUE] = 2;
  constr_order[DB_CONSTRAINT_INDEX] = 0;
  constr_order[DB_CONSTRAINT_NOT_NULL] = 6;
  constr_order[DB_CONSTRAINT_PRIMARY_KEY] = 4;

  sorted = NULL;
  for (constr = *orig_list, next = NULL; constr != NULL; constr = next)
    {
      next = constr->next;

      for (ins = sorted, prev = NULL, found = NULL;
	   ins != NULL && found == NULL; ins = ins->next)
	{
	  if (constr->constraint_type > DB_CONSTRAINT_PRIMARY_KEY
	      || ins->constraint_type > DB_CONSTRAINT_PRIMARY_KEY)
	    {
	      assert (false);
	      return ER_UNEXPECTED;
	    }

	  if (constr_order[constr->constraint_type] <
	      constr_order[ins->constraint_type])
	    {
	      found = ins;
	    }
	  else
	    {
	      prev = ins;
	    }
	}

      constr->next = found;
      if (prev == NULL)
	{
	  sorted = constr;
	}
      else
	{
	  prev->next = constr;
	}
    }
  *orig_list = sorted;

  return error;
}

/*
 * save_constraint_info_from_pt_node() - Saves the information necessary to
 *	 create a constraint from a PT_CONSTRAINT_INFO node
 *
 *   return: NO_ERROR on success, non-zero for ERROR
 *   save_info(in/out): The information saved
 *   pt_constr(in): The constraint node to be saved
 *
 *  Note :this function handles only constraints for single
 *	  attributes : PT_CONSTRAIN_NOT_NULL, PT_CONSTRAIN_UNIQUE,
 *	  PT_CONSTRAIN_PRIMARY_KEY.
 *	  Foreign keys, indexes on multiple columns are not supported and also
 *	  ASC/DESC info is not supported.
 *	  It process only one node; the 'next' PT_NODE is ignored.
 */
static int
save_constraint_info_from_pt_node (SM_CONSTRAINT_INFO ** save_info,
				   const PT_NODE * const pt_constr)
{
  int error_code = NO_ERROR;
  SM_CONSTRAINT_INFO *new_constraint = NULL;
  PT_NODE *constr_att_name = NULL;

  assert (pt_constr->node_type == PT_CONSTRAINT);

  new_constraint =
    (SM_CONSTRAINT_INFO *) calloc (1, sizeof (SM_CONSTRAINT_INFO));
  if (new_constraint == NULL)
    {
      error_code = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1,
	      sizeof (SM_CONSTRAINT_INFO));
      goto error_exit;
    }

  /* set NULL, expect to generate constraint name */
  new_constraint->name = NULL;

  switch (pt_constr->info.constraint.type)
    {
    case PT_CONSTRAIN_PRIMARY_KEY:
      constr_att_name = pt_constr->info.constraint.un.primary_key.attrs;
      new_constraint->constraint_type = DB_CONSTRAINT_PRIMARY_KEY;
      break;
    case PT_CONSTRAIN_UNIQUE:
      constr_att_name = pt_constr->info.constraint.un.unique.attrs;
      new_constraint->constraint_type = DB_CONSTRAINT_UNIQUE;
      break;
    case PT_CONSTRAIN_NOT_NULL:
      constr_att_name = pt_constr->info.constraint.un.not_null.attr;
      new_constraint->constraint_type = DB_CONSTRAINT_NOT_NULL;
      break;
    default:
      assert (false);
    }

  if (constr_att_name == NULL || constr_att_name->next != NULL)
    {
      error_code = ER_UNEXPECTED;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 0);
      goto error_exit;
    }

  new_constraint->att_names = (char **) calloc (1 + 1, sizeof (char *));
  if (new_constraint->att_names == NULL)
    {
      error_code = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1,
	      (1 + 1) * sizeof (char *));
      goto error_exit;
    }

  assert (constr_att_name->info.name.original != NULL);

  new_constraint->att_names[0] = strdup (constr_att_name->info.name.original);
  if (new_constraint->att_names[0] == NULL)
    {
      error_code = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1,
	      strlen (constr_att_name->info.name.original) + 1);
      goto error_exit;
    }

  new_constraint->att_names[1] = NULL;

  assert (new_constraint->next == NULL);
  while ((*save_info) != NULL)
    {
      save_info = &((*save_info)->next);
    }
  *save_info = new_constraint;

  return error_code;

error_exit:
  if (new_constraint != NULL)
    {
      sm_free_constraint_info (&new_constraint);
    }
  return error_code;
}

#if defined (ENABLE_UNUSED_FUNCTION)	/* TODO - delete me */
/*
 * do_check_rows_for_null() - checks if a column has NULL values
 *   return: NO_ERROR or error code
 *
 *   class_mop(in): class to check
 *   att_name(in): name of column to check
 *   has_nulls(out): true if column has rows with NULL
 *
 */
int
do_check_rows_for_null (MOP class_mop, const char *att_name, bool * has_nulls)
{
  int error = NO_ERROR;
  int n = 0;
  int stmt_id = 0;
  DB_SESSION *session = NULL;
  DB_QUERY_RESULT *result = NULL;
  const char *class_name = NULL;
  char query[2 * SM_MAX_IDENTIFIER_LENGTH + 50] = { 0 };
  DB_VALUE count;

  assert (class_mop != NULL);
  assert (att_name != NULL);
  assert (has_nulls != NULL);

  *has_nulls = false;
  DB_MAKE_NULL (&count);

  class_name = sm_class_name (class_mop);
  if (class_name == NULL)
    {
      error = ER_UNEXPECTED;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      goto end;
    }

  n = snprintf (query, sizeof (query) / sizeof (char),
		"SELECT count(*) FROM [%s] WHERE [%s] IS NULL LIMIT 1",
		class_name, att_name);
  if (n < 0 || (n == sizeof (query) / sizeof (char)))
    {
      error = ER_UNEXPECTED;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      goto end;
    }

  /* RUN the query */
  session = db_open_buffer (query);
  if (session == NULL)
    {
      error = er_errid ();
      goto end;
    }

  if (db_get_errors (session) || db_statement_count (session) != 1)
    {
      error = er_errid ();
      goto end;
    }

  if (db_compile_statement (session) != NO_ERROR)
    {
      error = er_errid ();
      goto end;
    }

  error = db_execute_statement (session, &result);
  if (error < 0)
    {
      goto end;
    }

  if (result == NULL)
    {
      error = ER_UNEXPECTED;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      goto end;
    }

  error = db_query_first_tuple (result);
  if (error != NO_ERROR)
    {
      goto end;
    }

  error = db_query_set_copy_tplvalue (result, 0 /* peek */ );
  if (error != NO_ERROR)
    {
      goto end;
    }

  error = db_query_get_tuple_value (result, 0, &count);
  if (error != NO_ERROR)
    {
      goto end;
    }

  assert (!DB_IS_NULL (&count));
  assert (DB_VALUE_DOMAIN_TYPE (&count) == DB_TYPE_BIGINT);

  if (DB_GET_BIGINT (&count) > 0)
    {
      *has_nulls = true;
    }

end:
  if (result != NULL)
    {
      db_query_end (result);
    }
  if (session != NULL)
    {
      db_close_session (session);
    }
  db_value_clear (&count);

  return error;
}
#endif
