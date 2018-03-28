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
 * schema_template.c - Schema manager templates
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "error_manager.h"
#include "object_representation.h"
#include "object_domain.h"
#include "work_space.h"
#include "object_primitive.h"
#include "class_object.h"
#include "schema_manager.h"
#include "set_object.h"
#include "locator_cl.h"
#include "authenticate.h"
#include "transform_cl.h"
#include "statistics.h"
#include "db.h"
#include "release_string.h"

static int check_namespace (SM_TEMPLATE * temp, const char *name);
static int resolve_class_domain (SM_TEMPLATE * tmp, DB_DOMAIN * domain);
static int get_domain (SM_TEMPLATE * tmp, const char *domain_string, DB_DOMAIN ** domainp);
static int check_domain_class_type (SM_TEMPLATE * template_, DB_OBJECT * domain_classobj);
static SM_TEMPLATE *def_class_internal (const char *name, int class_type);
static int smt_add_constraint_to_property (SM_TEMPLATE * template_,
                                           SM_CONSTRAINT_TYPE type,
                                           const char *constraint_name, SM_ATTRIBUTE ** atts, const int *asc_desc);
static int smt_drop_constraint_from_property (SM_TEMPLATE * template_,
                                              const char *constraint_name, SM_ATTRIBUTE_FLAG constraint);
static int smt_add_attribute_to_list (SM_ATTRIBUTE ** att_list,
                                      SM_ATTRIBUTE * att, const bool add_first, const char *add_after_attribute);
static int smt_check_index_exist (SM_TEMPLATE * template_, const char *constraint_name);
static int smt_change_attribute_pos_in_list (SM_ATTRIBUTE ** att_list,
                                             SM_ATTRIBUTE * att,
                                             const bool change_first, const char *change_after_attribute);
static void
smt_set_attribute_orig_default_value (SM_ATTRIBUTE * att, DB_VALUE * new_orig_value, DB_DEFAULT_EXPR_TYPE default_expr);

/* TEMPLATE SEARCH FUNCTIONS */
/*
 * These are used to walk over the template structures and extract information
 * of interest, signaling errors if things don't look right.  These will
 * be called by the smt interface functions so we don't have to duplicate
 * a lot of the error checking code in every function.
*/

/*
 * smt_find_attribute() - Locate an instance attribute
 *    in a template. Signal an error if not found.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   template(in): schema template
 *   name(in): attribute name
 *   attp(out): returned pointer to attribute structure
 */

int
smt_find_attribute (SM_TEMPLATE * template_, const char *name, SM_ATTRIBUTE ** attp)
{
  int error = NO_ERROR;
  SM_ATTRIBUTE *att;

  if (!sm_check_name (name))
    {
      error = er_errid ();
    }
  else
    {
      att = classobj_find_attribute (template_->attributes, name);
      if (att != NULL)
        {
          *attp = att;
        }
      else
        {
          if (template_->current == NULL)
            {
              ERROR1 (error, ER_SM_ATTRIBUTE_NOT_FOUND, name);
            }
          else
            {
              /* check for mistaken references to inherited attributes and
                 give a better message */
              att = classobj_find_attribute (template_->current->attributes, name);
              if (att == NULL)
                {
                  /* wasn't inherited, give the ususal message */
                  ERROR1 (error, ER_SM_ATTRIBUTE_NOT_FOUND, name);
                }
              else
                {
                  ERROR2 (error, ER_SM_INHERITED_ATTRIBUTE, name, sm_class_name (att->class_mop));
                }
            }
        }
    }

  return error;
}

/*
 * check_namespace() - This is called when any kind of attribute is
 *    being added to a template. We check to see if there is already a component
 *    with that name and signal an appropriate error.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   temp(in): schema template
 *   name(in): attribute
 */
static int
check_namespace (SM_TEMPLATE * temp, const char *name)
{
  int error = NO_ERROR;

  if (classobj_find_attribute (temp->attributes, name) != NULL)
    {
      ERROR1 (error, ER_SM_NAME_RESERVED_BY_ATT, name);
    }

  return error;
}

/* DOMAIN DECODING */
/*
 * resolve_class_domain()
 * get_domain() - Maps a domain string into a domain structure.
 */

static int
resolve_class_domain (SM_TEMPLATE * tmp, DB_DOMAIN * domain)
{
  int error = NO_ERROR;
  DB_DOMAIN *tmp_domain;

  if (domain)
    {
      switch (TP_DOMAIN_TYPE (domain))
        {
        case DB_TYPE_SEQUENCE:
          tmp_domain = domain->setdomain;
          while (tmp_domain)
            {
              error = resolve_class_domain (tmp, tmp_domain);
              if (error != NO_ERROR)
                {
                  return error;
                }
              tmp_domain = tmp_domain->next;
            }
          break;

        case DB_TYPE_OBJECT:
          if (domain->self_ref)
            {
              domain->type = tp_Type_null;
              /* kludge, store the template as the "class" for this
                 special domain */
              domain->class_mop = (MOP) tmp;
            }
          break;

        default:
          break;
        }
    }

  return error;
}

static int
get_domain (SM_TEMPLATE * tmp, const char *domain_string, DB_DOMAIN ** domainp)
{
  int error = NO_ERROR;
  DB_DOMAIN *domain = (DB_DOMAIN *) 0;
  PR_TYPE *type;

  /* If the domain is already determined, use it */
  if (*domainp)
    {
      domain = *domainp;
    }
  else
    {
      if (domain_string == NULL || domain_string[0] == '*')
        {
          ERROR0 (error, ER_SM_INVALID_ARGUMENTS);
        }
      else
        {
          domain = pt_string_to_db_domain (domain_string, tmp->name);
        }
    }

  if (domain != NULL)
    {
      error = resolve_class_domain (tmp, domain);
      if (error != NO_ERROR)
        {
          assert (TP_DOMAIN_TYPE (domain) != DB_TYPE_VARIABLE);
          if (*domainp == NULL)
            {
              tp_domain_free (domain);
            }
          domain = NULL;
        }
    }
  else
    {
      error = er_errid ();
    }

  *domainp = domain;

  return error;
}

/*
 * check_domain_class_type() - see if a class is of the appropriate type for
 *    an attribute. Classes can only have attributes of class domains and
 *    virtual classes can have attributes of both class and vclass domains.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   template(in): class template
 *   domain_classobj(in): class to examine
 */

static int
check_domain_class_type (SM_TEMPLATE * template_, DB_OBJECT * domain_classobj)
{
  int error = NO_ERROR;
  SM_CLASS *class_;

  /* If its a class, the domain can only be "object" or another class */
  if (template_->class_type == SM_CLASS_CT)
    {
      if (domain_classobj != NULL
          && !(error = au_fetch_class_force (domain_classobj, &class_,
                                             S_LOCK)) && template_->class_type != class_->class_type)
        {
          ERROR1 (error, ER_SM_INCOMPATIBLE_DOMAIN_CLASS_TYPE, class_->header.name);
        }
    }

  return error;
}

/* SCHEMA TEMPLATE CREATION */

/*
 * def_class_internal() - Begins the definition of a new class.
 *    An empty template is created and returned.  The class name
 *    is not registed with the server at this time, that is deferred
 *    until the template is applied with smt_finish_class.
 *   return: schema template
 *   name(in): new class name
 *   class_type(in): type of class
 */

static SM_TEMPLATE *
def_class_internal (const char *name, int class_type)
{
  char realname[SM_MAX_IDENTIFIER_LENGTH];
  SM_TEMPLATE *template_ = NULL;
  PR_TYPE *type;

  if (sm_check_name (name))
    {
      type = pr_find_type (name);
      if (type != NULL)
        {
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_SM_CLASS_WITH_PRIM_NAME, 1, name);
        }
      else
        {
          sm_downcase_name (name, realname, SM_MAX_IDENTIFIER_LENGTH);
          name = realname;
          template_ = classobj_make_template (name, NULL, NULL);
          if (template_ != NULL)
            {
              template_->class_type = (SM_CLASS_TYPE) class_type;
            }
        }
    }

  return template_;
}

/*
 * smt_def_class() - Begins the definition of a normal class.
 *    See description of def_class_internal.
 *   return: template
 *   name(in): class name
 */

SM_TEMPLATE *
smt_def_class (const char *name)
{
  return (def_class_internal (name, SM_CLASS_CT));
}

/*
 * smt_edit_class_mop_with_lock () - Begins the editing of an existing class.
 *    A template is created and populated with the current definition
 *    of a class.
 *    This will get a write lock on the class as well.
 *    At this time we could also get write locks on the subclasses but
 *    if we defer this until smt_finish_class, we can be smarter about
 *    getting locks only on the affected subclasses.
 *   return: schema template
 *   op(in): class MOP
 *   lock(in):
 */

SM_TEMPLATE *
smt_edit_class_mop_with_lock (MOP op, LOCK lock)
{
  SM_TEMPLATE *template_;
  SM_CLASS *class_;

  template_ = NULL;

  /* op should be a class */
  if (!locator_is_class (op))
    {
      assert (false);

      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_OBJ_NOT_A_CLASS, 0);
    }
  else
    {
      if (au_fetch_class (op, &class_, lock, AU_ALTER) == NO_ERROR)
        {
          /* cleanup the class and flush out the run-time information prior to
             editing */
          sm_clean_class (op, class_);

          template_ = classobj_make_template (sm_class_name (op), op, class_);
        }
    }

  return template_;
}

/*
 * smt_copy_class_mop() - Duplicates an existing class for CREATE LIKE.
 *    A template is created and populated with a copy of the current definition
 *    of the given class.
 *   return: schema template
 *   op(in): class MOP of the class to duplicate
 *   class_(out): the current definition of the duplicated class is returned
 *                in order to be used for subsequent operations (such as
 *                duplicating indexes).
 */

SM_TEMPLATE *
smt_copy_class_mop (const char *name, MOP op, SM_CLASS ** class_)
{
  SM_TEMPLATE *template_ = NULL;

  assert (*class_ == NULL);

  /* op should be a class */
  if (!locator_is_class (op))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_NOT_A_CLASS, 0);
      return NULL;
    }

  if (au_fetch_class (op, class_, S_LOCK, DB_AUTH_SELECT) == NO_ERROR)
    {
      if ((*class_)->class_type != SM_CLASS_CT)
        {
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_NOT_A_CLASS, 0);
          return NULL;
        }

      template_ = classobj_make_template_like (name, *class_);
    }

  return template_;
}

/*
 * smt_copy_class() - Duplicates an existing class for CREATE LIKE.
 *    Behaves like smt_copy_class_mop except that the class is identified
 *    by name rather than with a MOP.
 *   return: schema template
 *   new_name(in): name of the class to be created
 *   existing_name(in): name of the class to be duplicated
 *   class_(out): the current definition of the duplicated class is returned
 *                in order to be used for subsequent operations (such as
 *                duplicating indexes).
 */

SM_TEMPLATE *
smt_copy_class (const char *new_name, const char *existing_name, SM_CLASS ** class_)
{
  SM_TEMPLATE *template_ = NULL;

  if (sm_check_name (existing_name) != 0)
    {
      MOP op = sm_find_class (existing_name);
      if (op != NULL)
        {
          template_ = smt_copy_class_mop (new_name, op, class_);
        }
    }

  return template_;
}

/*
 * smt_quit() - This is called to abort the creation of a schema template.
 *    If a template cannot be applied due to errors, you must either
 *    fix the template and re-apply it or use smt_quit to throw
 *    away the template and release the storage that has been allocated.
 *   return: NO_ERROR on success, non-zero for ERROR (always NO_ERROR)
 *   template(in): schema template to destroy
 */

int
smt_quit (SM_TEMPLATE * template_)
{
  int error = NO_ERROR;

  if (template_ != NULL)
    {
      classobj_free_template (template_);
    }

  return error;
}

/* TEMPLATE ATTRIBUTE FUNCTIONS */
/*
 * smt_add_attribute_w_dflt_w_order()
 *   return: NO_ERROR on success, non-zero for ERROR
 *   def(in/out):
 *   name(in): attribute name
 *   domain_string(in): domain name string
 *   domain(in): domain
 *   default_value(in):
 *   add_first(in): the attribute should be added at the beginning of the
 *                  attributes list
 *   add_after_attribute(in): the attribute should be added in the attributes
 *                            list after the attribute with the given name
 */
int
smt_add_attribute_w_dflt_w_order (DB_CTMPL * def,
                                  const char *name,
                                  const char *domain_string,
                                  DB_DOMAIN * domain,
                                  DB_VALUE * default_value,
                                  const bool add_first,
                                  const char *add_after_attribute, DB_DEFAULT_EXPR_TYPE default_expr, bool is_shard_key)
{
  int error = NO_ERROR;

  error = smt_add_attribute_any (def, name, domain_string, domain, add_first, add_after_attribute, is_shard_key);
  if (error == NO_ERROR && default_value != NULL)
    {
      error = smt_set_attribute_default (def, name, default_value, default_expr);
    }

  return error;
}

/*
 * smt_add_attribute_w_dflt()
 *   return: NO_ERROR on success, non-zero for ERROR
 *   def(in/out):
 *   name(in): attribute name
 *   domain_string(in): domain name string
 *   domain(in): domain
 *   default_value(in):
 */
int
smt_add_attribute_w_dflt (DB_CTMPL * def,
                          const char *name,
                          const char *domain_string,
                          DB_DOMAIN * domain,
                          DB_VALUE * default_value, DB_DEFAULT_EXPR_TYPE default_expr, bool is_shard_key)
{
  return smt_add_attribute_w_dflt_w_order (def, name, domain_string, domain,
                                           default_value, false, NULL, default_expr, is_shard_key);
}

/*
 * smt_add_attribute_any() - Adds an attribute to a template.
 *    The domain may be specified either with a string or a DB_DOMAIN *.
 *    If domain is not NULL, it is used.  Otherwise domain_string is used.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   template(in/out): schema template
 *   name(in): attribute name
 *   domain_string(in): domain name string
 *   domain(in): domain
 *   add_first(in): the attribute should be added at the beginning of the
 *                  attributes list
 *   add_after_attribute(in): the attribute should be added in the attributes
 *                            list after the attribute with the given name
 */

int
smt_add_attribute_any (SM_TEMPLATE * template_, const char *name,
                       const char *domain_string, DB_DOMAIN * domain,
                       const bool add_first, const char *add_after_attribute, bool is_shard_key)
{
  int error_code = NO_ERROR;
  SM_ATTRIBUTE *att = NULL;
  SM_ATTRIBUTE **att_list = NULL;
  char real_name[SM_MAX_IDENTIFIER_LENGTH] = { 0 };
  char add_after_attribute_real_name[SM_MAX_IDENTIFIER_LENGTH] = { 0 };
  DB_DOMAIN *cur_domain = NULL;

  assert (template_ != NULL);

  att_list = &template_->attributes;
  cur_domain = domain;          /* save */

  if (!sm_check_name (name))
    {
      error_code = er_errid ();
      goto error_exit;
    }

  sm_downcase_name (name, real_name, SM_MAX_IDENTIFIER_LENGTH);
  name = real_name;

  if (add_after_attribute != NULL)
    {
      sm_downcase_name (add_after_attribute, add_after_attribute_real_name, SM_MAX_IDENTIFIER_LENGTH);
      add_after_attribute = add_after_attribute_real_name;
    }

  error_code = check_namespace (template_, name);
  if (error_code != NO_ERROR)
    {
      goto error_exit;
    }

  error_code = get_domain (template_, domain_string, &domain);
  if (error_code != NO_ERROR)
    {
      goto error_exit;
    }

  if (domain == NULL)
    {
      ERROR0 (error_code, ER_SM_INVALID_ARGUMENTS);
      goto error_exit;
    }

  if (TP_DOMAIN_TYPE (domain) == DB_TYPE_OBJECT)
    {
      error_code = check_domain_class_type (template_, domain->class_mop);
      if (error_code != NO_ERROR)
        {
          goto error_exit;
        }
    }

  att = classobj_make_attribute (name, domain->type);
  if (att == NULL)
    {
      error_code = er_errid ();
      goto error_exit;
    }

  /* Flag this attribute as new so that we can initialize
     the original_value properly.  Make sure this isn't saved
     on disk ! */
  att->flags |= SM_ATTFLAG_NEW;
  if (is_shard_key == true)
    {
      att->flags |= SM_ATTFLAG_SHARD_KEY;
    }
  att->class_mop = template_->op;
  assert (TP_DOMAIN_TYPE (domain) != DB_TYPE_VARIABLE);
  att->sma_domain = domain;

  error_code = smt_add_attribute_to_list (att_list, att, add_first, add_after_attribute);
  if (error_code != NO_ERROR)
    {
      goto error_exit;
    }

  return error_code;

error_exit:
  if (att != NULL)
    {
      classobj_free_attribute (att);
      att = NULL;
    }

  if (domain != NULL)
    {
      if (cur_domain == NULL)
        {
          assert (TP_DOMAIN_TYPE (domain) != DB_TYPE_VARIABLE);
          tp_domain_free (domain);
        }
    }

  return error_code;
}

/*
 * smt_add_attribute_to_list()
 *   return: NO_ERROR on success, non-zero for ERROR
 *   att_list(in/out): the list to add to
 *   att(in): the attribute to add
 *   add_first(in): the attribute should be added at the beginning of the
 *                  attributes list
 *   add_after_attribute(in): the attribute should be added in the attributes
 *                            list after the attribute with the given name
 */

static int
smt_add_attribute_to_list (SM_ATTRIBUTE ** att_list, SM_ATTRIBUTE * att,
                           const bool add_first, const char *add_after_attribute)
{
  int error_code = NO_ERROR;
  SM_ATTRIBUTE *crt_att = NULL;

  assert (att->next == NULL);
  assert (att_list != NULL);

  if (add_first)
    {
      assert (add_after_attribute == NULL);
      *att_list = (SM_ATTRIBUTE *) WS_LIST_NCONC (att, *att_list);
      goto end;
    }

  if (add_after_attribute == NULL)
    {
      WS_LIST_APPEND (att_list, att);
      goto end;
    }

  for (crt_att = *att_list; crt_att != NULL; crt_att = crt_att->next)
    {
      if (intl_identifier_casecmp (crt_att->name, add_after_attribute) == 0)
        {
          break;
        }
    }
  if (crt_att == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_QPROC_INVALID_COLNAME, 1, add_after_attribute);
      error_code = ER_QPROC_INVALID_COLNAME;
      goto error_exit;
    }
  att->next = crt_att->next;
  crt_att->next = att;

end:
  return error_code;

error_exit:
  return error_code;
}

/*
 * smt_add_attribute() - Adds an instance attribute to a class
 *   return: NO_ERROR on success, non-zero for ERROR
 *   template(in/out): schema template
 *   name(in): attribute name
 *   domain_string(in): domain name string
 *   domain(in): domain structure
 */
/*
 * TODO Replace calls to this function with calls to smt_add_attribute_any ()
 *      and remove this function.
 */
int
smt_add_attribute (SM_TEMPLATE * template_, const char *name, const char *domain_string, DB_DOMAIN * domain)
{
  return (smt_add_attribute_any (template_, name, domain_string, domain, false, NULL, false));
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * smt_delete_set_attribute_domain() - Remove a domain entry from the domain
 *    list of an attribute whose basic type is one of the set types.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   template(in/out): schema template
 *   name(in): attribute name
 *   domain_string(in): domain name string
 *   domain(in): domain structure
 */

int
smt_delete_set_attribute_domain (SM_TEMPLATE * template_,
                                 const char *name, const char *domain_string, DB_DOMAIN * domain)
{
  int error = NO_ERROR;
  SM_ATTRIBUTE *att;

  error = smt_find_attribute (template_, name, &att);
  assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

  if (error == NO_ERROR)
    {
      if ((att->sma_domain == NULL) || !pr_is_set_type (TP_DOMAIN_TYPE (att->sma_domain)))
        {
          ERROR1 (error, ER_SM_DOMAIN_NOT_A_SET, name);
        }
      else
        {
          error = get_domain (template_, domain_string, &domain);
          if (error == NO_ERROR)
            {
              assert (domain != NULL);
              if (domain == NULL || !tp_domain_drop (&att->sma_domain->setdomain, domain))
                {
                  ERROR2 (error, ER_SM_DOMAIN_NOT_FOUND, name, (domain_string ? domain_string : "unknown"));
                }
            }
        }
    }

  return error;
}
#endif

/*
 * smt_set_attribute_default() - Assigns the default value for an attribute.
 *    Need to have domain checking and constraint checking at this
 *    level similar to that done in object.c when attribute values
 *    are being assigned.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   template(in/out): schema template
 *   name(in): attribute name
 *   proposed_value(in): default value to assign
 */

int
smt_set_attribute_default (SM_TEMPLATE * template_, const char *name,
                           DB_VALUE * proposed_value, DB_DEFAULT_EXPR_TYPE default_expr)
{
  int error = NO_ERROR;
  SM_ATTRIBUTE *att;
  DB_VALUE *value;
  TP_DOMAIN_STATUS status;
  char real_name[SM_MAX_IDENTIFIER_LENGTH] = { 0 };

  sm_downcase_name (name, real_name, SM_MAX_IDENTIFIER_LENGTH);
  name = real_name;

  error = smt_find_attribute (template_, name, &att);
  assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

  if (error == NO_ERROR)
    {
      if (proposed_value && DB_IS_NULL (proposed_value)
          && default_expr == DB_DEFAULT_NONE && (att->flags & SM_ATTFLAG_PRIMARY_KEY))
        {
          ERROR1 (error, ER_CANNOT_HAVE_PK_DEFAULT_NULL, name);
          return error;
        }

      value = proposed_value;
      status = tp_domain_check (att->sma_domain, value, TP_EXACT_MATCH);
      if (status != DOMAIN_COMPATIBLE)
        {
          /* coerce it if we can */
          value = pr_make_ext_value ();
          if (value == NULL)
            {
              error = er_errid ();
              goto end;
            }

          status = tp_value_coerce (proposed_value, value, att->sma_domain);
          /* value is freed at the bottom */
        }
      if (status != DOMAIN_COMPATIBLE)
        {
          ERROR1 (error, ER_OBJ_DOMAIN_CONFLICT, att->name);
        }
      else
        {
          /* check a subset of the integrity constraints, we can't check for
             NOT NULL or unique here */

          if (value != NULL && tp_check_value_size (att->sma_domain, value) != DOMAIN_COMPATIBLE)
            {
              /* need an error message that isn't specific to "string" types */
              ERROR2 (error, ER_OBJ_STRING_OVERFLOW, att->name, att->sma_domain->precision);
            }
          else
            {
              pr_clear_value (&att->default_value.value);
              pr_clone_value (value, &att->default_value.value);
              att->default_value.default_expr = default_expr;

              /* if there wasn't an previous original value, take this one.
               * This can only happen for new templates OR if this is a new
               * attribute that was added during this template OR if this is
               * the first time setting a default value to the attribute.
               * This should be handled by using candidates in the template
               * and storing an extra bit field in the candidate structure.
               * See the comment above sm_attribute for more information
               * about "original_value".
               */
              if (att->flags & SM_ATTFLAG_NEW)
                {
                  smt_set_attribute_orig_default_value (att, value, default_expr);
                }
            }
        }

      /* free the coerced value if any */
      if (value != proposed_value)
        {
          pr_free_ext_value (value);
        }
    }

end:
  return error;
}

/*
 * smt_set_attribute_orig_default_value() - Sets the original default value of
 *					    the attribute.
 *					    No domain checking is performed.
 *   return: void
 *   att(in): attribute
 *   new_orig_value(in): original value to set
 *
 *  Note : This function modifies the initial default value of the attribute.
 *	   The initial default value is the default value assigned when adding
 *	   the attribute. The default value of attribute may change after its
 *	   creation (or after it was added), but the initial value remains
 *	   unchanged (until attribute is dropped).
 *	   The (current) default value is stored as att->value; the initial
 *	   default value is stored as att->original_value.
 */
static void
smt_set_attribute_orig_default_value (SM_ATTRIBUTE * att, DB_VALUE * new_orig_value, DB_DEFAULT_EXPR_TYPE default_expr)
{
  assert (att != NULL);
  assert (new_orig_value != NULL);

  pr_clear_value (&att->default_value.original_value);
  pr_clone_value (new_orig_value, &att->default_value.original_value);
  att->default_value.default_expr = default_expr;
}

/*
 * smt_drop_constraint_from_property() - Drop the named constraint from the
 *                                       template property list.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   template(in/out): schema template
 *   constraint_name(in): constraint name
 *   constraint(in):
 */

static int
smt_drop_constraint_from_property (SM_TEMPLATE * template_, const char *constraint_name, SM_ATTRIBUTE_FLAG constraint)
{
  int error = NO_ERROR;
  SM_DISK_CONSTRAINT *cons, *found_cons, *prev_cons;

  if (!SM_IS_ATTFLAG_INDEX_FAMILY (constraint))
    {
      return NO_ERROR;
    }

  found_cons = prev_cons = NULL;
  for (cons = template_->disk_constraints; cons != NULL; cons = cons->next)
    {
      assert (SM_IS_CONSTRAINT_INDEX_FAMILY (cons->type));

      if (strcmp (cons->name, constraint_name) == 0)
        {
          found_cons = cons;
          break;
        }
      prev_cons = cons;
    }

  if (found_cons == NULL)
    {
      ERROR1 (error, ER_SM_CONSTRAINT_NOT_FOUND, constraint_name);

      GOTO_EXIT_ON_ERROR;
    }

  /* remove constraint of SM_TEMPLATE */
  if (prev_cons == NULL)
    {
      /* found first */
      assert (template_->disk_constraints == found_cons);

      template_->disk_constraints = found_cons->next;
    }
  else
    {
      prev_cons->next = found_cons->next;
    }
  found_cons->next = NULL;
  classobj_free_disk_constraint (found_cons);

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (error != NO_ERROR);
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
    }

  return error;
}

/*
 * smt_add_constraint_to_property() - Add the named constraint to the
 *                                    template property list
 *   return: NO_ERROR on success, non-zero for ERROR
 *   template(in/out): schema template
 *   type(in):
 *   constraint_name(in): constraint name
 *   atts(in):
 *   asc_desc(in): asc/desc info list
 *   fk_info(in):
 */

static int
smt_add_constraint_to_property (SM_TEMPLATE * template_,
                                SM_CONSTRAINT_TYPE type,
                                const char *constraint_name, SM_ATTRIBUTE ** atts, const int *asc_desc)
{
  SM_DISK_CONSTRAINT *last_cons_of_type, *new_disk_cons, *disk_cons;
  SM_DISK_CONSTRAINT_ATTRIBUTE *att, *first, *last;
  int error = NO_ERROR;
  /*
   * SM_CONSTRAINT_UNIQUE, SM_CONSTRAINT_INDEX,
   * SM_CONSTRAINT_NOT_NULL, SM_CONSTRAINT_PRIMARY_KEY
   */
  int type_order[SM_CONSTRAINT_LAST + 1] = { 2, 3, 4, 1 };
  int i;

  assert (type != SM_CONSTRAINT_NOT_NULL);

  /* init locals */
  new_disk_cons = last_cons_of_type = NULL;
  first = last = NULL;

  for (disk_cons = template_->disk_constraints; disk_cons != NULL; disk_cons = disk_cons->next)
    {
      if (strcmp (disk_cons->name, constraint_name) == 0 && disk_cons->index_status != INDEX_STATUS_IN_PROGRESS)
        {
          ERROR1 (error, ER_SM_CONSTRAINT_EXISTS, constraint_name);
          GOTO_EXIT_ON_ERROR;
        }
      if (type_order[disk_cons->type] <= type_order[type])
        {
          last_cons_of_type = disk_cons;
        }
    }

  new_disk_cons = classobj_make_disk_constraint ();
  if (new_disk_cons == NULL)
    {
      error = er_errid ();
      GOTO_EXIT_ON_ERROR;
    }

  new_disk_cons->name = ws_copy_string (constraint_name);
  if (new_disk_cons->name == NULL)
    {
      error = er_errid ();
      goto exit_on_error;
    }
  new_disk_cons->type = type;

  for (i = 0; atts[i] != NULL; i++)
    {
      att = classobj_make_disk_constraint_attribute ();
      if (att == NULL)
        {
          error = er_errid ();
          GOTO_EXIT_ON_ERROR;
        }

      if (asc_desc == NULL)
        {
          att->asc_desc = 0;
        }
      else
        {
          att->asc_desc = asc_desc[i];
        }
      att->name = ws_copy_string (atts[i]->name);
      att->att_id = atts[i]->id;

      if (first == NULL)
        {
          first = att;
          last = att;
        }
      else
        {
          assert (last != NULL);
          last->next = att;
          last = att;
        }
    }
  new_disk_cons->num_atts = i;
  new_disk_cons->disk_info_of_atts = first;

  /* append last of type */
  if (last_cons_of_type == NULL)
    {
      /* append new_disk_cons first of list */
      if (template_->disk_constraints != NULL)
        {
          new_disk_cons->next = template_->disk_constraints;
        }
      template_->disk_constraints = new_disk_cons;
    }
  else
    {
      /* append new_disk_cons after last_cons_of_type node */
      new_disk_cons->next = last_cons_of_type->next;
      last_cons_of_type->next = new_disk_cons;
    }

  assert (error == NO_ERROR);

  return error;

exit_on_error:

  if (error == NO_ERROR)
    {
      assert (error != NO_ERROR);
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
    }

  if (first != NULL)
    {
      if (new_disk_cons != NULL && new_disk_cons->disk_info_of_atts == first)
        {
          ;                     /* go ahead; free at classobj_free_disk_constraint () */
        }
      else
        {
          assert (new_disk_cons == NULL || new_disk_cons->disk_info_of_atts == NULL);
          WS_LIST_FREE (first, classobj_free_disk_constraint_attribute);
          first = NULL;
        }
    }

  if (new_disk_cons != NULL)
    {
      classobj_free_disk_constraint (new_disk_cons);
      new_disk_cons = NULL;
    }

  return error;
}

/*
 * smt_drop_constraint() - Drops the integrity constraint flags for an attribute.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   template(in/out): schema template
 *   att_names(in): array of attribute names
 *   constraint_name(in): Constraint name.
 *   constraint(in): constraint identifier
 */

int
smt_drop_constraint (SM_TEMPLATE * template_, const char **att_names,
                     const char *constraint_name, SM_ATTRIBUTE_FLAG constraint)
{
  int error = NO_ERROR;
  SM_ATTRIBUTE *not_null_attr[1], *pk_attr;
  int n_atts;

  if (!(SM_IS_ATTFLAG_INDEX_FAMILY (constraint) || constraint == SM_ATTFLAG_NON_NULL))
    {
      ERROR0 (error, ER_SM_INVALID_ARGUMENTS);
      return error;
    }

  if (constraint == SM_ATTFLAG_NON_NULL)
    {
      n_atts = 0;
      if (att_names != NULL)
        {
          while (att_names[n_atts] != NULL)
            {
              n_atts++;
            }
        }

      if (n_atts != 1)
        {
          ERROR0 (error, ER_SM_NOT_NULL_WRONG_NUM_ATTS);
          return error;
        }
    }

  error = smt_drop_constraint_from_property (template_, constraint_name, constraint);

  if (error == NO_ERROR)
    {
      if (constraint == SM_ATTFLAG_PRIMARY_KEY)
        {
          for (pk_attr = template_->attributes; pk_attr != NULL; pk_attr = pk_attr->next)
            {
              if (pk_attr->flags & SM_ATTFLAG_PRIMARY_KEY)
                {
                  pk_attr->flags &= ~SM_ATTFLAG_PRIMARY_KEY;
                  pk_attr->flags &= ~SM_ATTFLAG_NON_NULL;
                }
            }
        }
      else if (constraint == SM_ATTFLAG_NON_NULL)
        {
          error = smt_find_attribute (template_, att_names[0], &not_null_attr[0]);

          if (error == NO_ERROR)
            {
              if (not_null_attr[0]->flags & constraint)
                {
                  not_null_attr[0]->flags &= ~constraint;
                }
              else
                {
                  ERROR1 (error, ER_SM_CONSTRAINT_NOT_FOUND, "NON_NULL");
                }
            }
        }
    }

  return error;
}

/*
 * smt_check_index_exist() - Check index is duplicated.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   template(in/out): schema template
 *   constraint_name(in): Constraint name.
 */
static int
smt_check_index_exist (SM_TEMPLATE * template_, const char *constraint_name)
{
  int error = NO_ERROR;
  SM_CLASS *class_;
  SM_CLASS_CONSTRAINT *temp_cons = NULL;
  SM_CLASS_CONSTRAINT *check_cons;

  if (template_->op != NULL)
    {
      error = au_fetch_class (template_->op, &class_, S_LOCK, AU_ALTER);
      if (error != NO_ERROR)
        {
          return error;
        }

      check_cons = class_->constraints;
    }
  else
    {
      error = classobj_make_class_constraints (&check_cons, template_->attributes, template_->disk_constraints);
      if (error != NO_ERROR)
        {
          return error;
        }

      temp_cons = check_cons;
    }

  error = classobj_check_index_exist (check_cons, template_->name, constraint_name);

  if (temp_cons != NULL)
    {
      WS_LIST_FREE (temp_cons, classobj_free_class_constraint);
      temp_cons = NULL;
    }

  return error;
}

/*
 * smt_add_constraint() - Adds the integrity constraint flags for an attribute.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   template(in/out): schema template
 *   constraint_type(in): constraint type
 *   constraint_name(in): Constraint name.
 *   att_names(in): array of attribute names
 *   asc_desc(in): asc/desc info list
 */

int
smt_add_constraint (SM_TEMPLATE * template_,
                    DB_CONSTRAINT_TYPE constraint_type,
                    const char *constraint_name, const char **att_names, const int *asc_desc)
{
  int error = NO_ERROR;
  SM_ATTRIBUTE **atts = NULL;
  DB_TYPE att_type;
  int i, j, n_atts, atts_size;
  SM_ATTRIBUTE_FLAG constraint;
  SM_ATTRIBUTE **att_list = NULL;
  SM_ATTRIBUTE *crt_att = NULL;

  assert (template_ != NULL);

  error = smt_check_index_exist (template_, constraint_name);
  if (error != NO_ERROR)
    {
      goto exit_on_error;
    }

  constraint = SM_MAP_CONSTRAINT_TO_ATTFLAG (constraint_type);

  n_atts = 0;
  if (att_names != NULL)
    {
      while (att_names[n_atts] != NULL)
        {
          n_atts++;
        }
    }

  if (n_atts == 0)
    {
      ERROR0 (error, ER_OBJ_INVALID_ARGUMENTS);
      goto exit_on_error;
    }

  assert (MAX_INDEX_KEY_LIST_NUM - 1 == 16);

  if (n_atts > MAX_INDEX_KEY_LIST_NUM - 1)
    {
      error = ER_SM_MAX_INDEX_KEY_LIST_NUM;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 2, MAX_INDEX_KEY_LIST_NUM - 1, n_atts);

      goto exit_on_error;
    }

  atts_size = (n_atts + 1) * (int) sizeof (SM_ATTRIBUTE *);
  atts = (SM_ATTRIBUTE **) malloc (atts_size);
  if (atts == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1, atts_size);

      error = ER_OUT_OF_VIRTUAL_MEMORY;
      goto exit_on_error;
    }

  for (i = 0; i < n_atts; i++)
    {
      error = smt_find_attribute (template_, att_names[i], &atts[i]);
      if (error != NO_ERROR)
        {
          goto exit_on_error;
        }
    }
  atts[i] = NULL;

  /* check that there are no duplicate attr defs in given list */
  for (i = 0; i < n_atts; i++)
    {
      for (j = i + 1; j < n_atts; j++)
        {
          /* can not check attr-id, because is not yet assigned */
          if (intl_identifier_casecmp (atts[i]->name, atts[j]->name) == 0)
            {
              ERROR1 (error, ER_SM_INDEX_ATTR_DUPLICATED, atts[i]->name);

              goto exit_on_error;
            }
        }
    }

  /* Check if table has shard column then, */
  /*   the first column of pk constraint should be shard key column. */
  /*   and shard key column must be in unique constraint. */

  if (constraint_type == DB_CONSTRAINT_PRIMARY_KEY || constraint_type == DB_CONSTRAINT_UNIQUE)
    {
      att_list = &template_->attributes;
      for (crt_att = *att_list; crt_att != NULL; crt_att = crt_att->next)
        {
          if (constraint_type == DB_CONSTRAINT_PRIMARY_KEY
              && crt_att->flags & SM_ATTFLAG_SHARD_KEY && intl_identifier_casecmp (crt_att->name, atts[0]->name) != 0)
            {
              /* the first column of pk constraint should be shard key column */
              assert (false);   /* is impossible */
              ERROR1 (error, ER_SHARD_KEY_MUST_BE_FIRST_POSITION_IN_PK, crt_att->name);
              goto exit_on_error;
            }

          if (constraint_type == DB_CONSTRAINT_UNIQUE && crt_att->flags & SM_ATTFLAG_SHARD_KEY)
            {
              /* shard key column must be in unique constraint column */
              for (i = 0; i < n_atts; i++)
                {
                  if (intl_identifier_casecmp (crt_att->name, atts[i]->name) == 0)
                    {
                      assert (atts[i]->flags & SM_ATTFLAG_SHARD_KEY);
                      break;
                    }
                }

              if (i == n_atts)
                {
                  /* shard key column is not in unique constraint */
                  ERROR1 (error, ER_SHARD_KEY_MUST_BE_IN_UNIQUE, crt_att->name);
                  goto exit_on_error;
                }
            }
        }
    }

  /*
   *  Process constraint
   */

  if (SM_IS_ATTFLAG_INDEX_FAMILY (constraint))
    {
      /*
       *  Check for possible errors
       *
       *    - We do not allow index family constraints on any attribute of
       *      a virtual class.
       *    - We only allow index family constraints on indexable data types.
       */
      if (template_->class_type != SM_CLASS_CT)
        {
          assert (false);       /* is impossible */
          ERROR0 (error, SM_MAP_INDEX_ATTFLAG_TO_VCLASS_ERROR (constraint));    /* TODO: add error code */

          goto exit_on_error;
        }

      for (i = 0; i < n_atts; i++)
        {
          /* server doesn't treat DB_TYPE_OBJECT, so that convert it to
             DB_TYPE_OID */
          att_type = atts[i]->type->id;
          if (att_type == DB_TYPE_OBJECT)
            {
              att_type = DB_TYPE_OID;
            }

          if (!tp_valid_indextype (att_type))
            {
              if (SM_IS_ATTFLAG_UNIQUE_FAMILY (constraint))
                {
                  ERROR2 (error, ER_SM_INVALID_UNIQUE_TYPE,
                          atts[i]->type->name, SM_GET_CONSTRAINT_STRING (constraint_type));
                }
              else if (constraint == SM_ATTFLAG_INDEX)
                {
                  ERROR1 (error, ER_SM_INVALID_INDEX_TYPE, atts[i]->type->name);
                }
              else
                {
                  ERROR1 (error, ER_GENERIC_ERROR, "");
                }

              goto exit_on_error;
            }
        }

      /*
       * No errors were found, drop or add the constraint
       */
      /*
       *  Add the unique constraint.  The drop case was taken care
       *  of at the beginning of this function.
       */
      error = smt_add_constraint_to_property (template_,
                                              SM_MAP_INDEX_ATTFLAG_TO_CONSTRAINT
                                              (constraint), constraint_name, atts, asc_desc);
      if (error != NO_ERROR)
        {
          goto exit_on_error;
        }

      if (constraint == SM_ATTFLAG_PRIMARY_KEY)
        {
          for (i = 0; i < n_atts; i++)
            {
              atts[i]->flags |= SM_ATTFLAG_PRIMARY_KEY;
              atts[i]->flags |= SM_ATTFLAG_NON_NULL;
            }
        }
    }
  else if (constraint == SM_ATTFLAG_NON_NULL)
    {

      /*
       *  We do not support NOT NULL constraints for;
       *    - normal (not class) attributes of virtual classes
       *    - multiple attributes
       *    - class attributes without default value
       */
      if (n_atts != 1)
        {
          ERROR0 (error, ER_SM_NOT_NULL_WRONG_NUM_ATTS);
        }
      else if (template_->class_type != SM_CLASS_CT)
        {
          assert (false);       /* is impossible */
          ERROR0 (error, ER_SM_NOT_NULL_ON_VCLASS);
        }
      else
        {
          if (atts[0]->flags & constraint)
            {
              ERROR1 (error, ER_SM_CONSTRAINT_EXISTS, "NON_NULL");
            }
          else
            {
              atts[0]->flags |= constraint;
            }
        }

      if (error != NO_ERROR)
        {
          goto exit_on_error;
        }
    }
  else
    {
      /* Unknown constraint type */
      ERROR0 (error, ER_SM_INVALID_ARGUMENTS);
      goto exit_on_error;
    }

  if (atts != NULL)
    {
      free_and_init (atts);
    }

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (error != NO_ERROR);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
    }

  if (atts != NULL)
    {
      free_and_init (atts);
    }

  return error;
}

/* TEMPLATE RENAME FUNCTIONS */

/*
 * smt_rename_any() - Renames a component (attribute).
 *    This is semantically different than just dropping the component
 *    and re-adding it since the internal ID number assigned
 *    to the component must remain the same after the rename.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   template(in/out): schema template
 *   name(in): attribute name
 *   new_name(in): new name of component
 */

int
smt_rename_any (SM_TEMPLATE * template_, const char *name, const char *new_name)
{
  int error = NO_ERROR;
  char real_new_name[SM_MAX_IDENTIFIER_LENGTH];
  SM_ATTRIBUTE *att;

  if (!sm_check_name (name) || !sm_check_name (new_name))
    {
      error = er_errid ();      /* return error set by call */
      GOTO_EXIT_ON_ERROR;
    }

  sm_downcase_name (new_name, real_new_name, SM_MAX_IDENTIFIER_LENGTH);
  new_name = real_new_name;

  /* find the named component */
  error = smt_find_attribute (template_, name, &att);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }
  /* check for collisions on the new name */
  error = check_namespace (template_, new_name);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  ws_free_string (att->name);
  att->name = ws_copy_string (new_name);
  if (att->name == NULL)
    {
      error = er_errid ();
      GOTO_EXIT_ON_ERROR;
    }

  assert (error == NO_ERROR);
  return error;

exit_on_error:
  if (error == NO_ERROR)
    {
      assert (error != NO_ERROR);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
    }

  return error;
}

/* TEMPLATE DELETION FUNCTIONS */

/*
 * smt_delete_any() - This is the primary function for deletion of ID_ATTRIBUTE
 *    attributes and resolution aliases from a template.
 *    The attribute that is a member of primary key can't be deleted.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   template(in/out): schema template
 *   name(in): component name
 */

int
smt_delete_any (SM_TEMPLATE * template_, const char *name)
{
  int error = NO_ERROR;
  SM_ATTRIBUTE *att;

  error = smt_find_attribute (template_, name, &att);
  if (error == NO_ERROR)
    {
      if (att->flags & SM_ATTFLAG_PRIMARY_KEY)
        {
          ERROR1 (error, ER_SM_ATTRIBUTE_PRIMARY_KEY_MEMBER, name);
        }
      else
        {
          WS_LIST_REMOVE (&template_->attributes, att);
          classobj_free_attribute (att);
        }
    }

  return error;
}

/* TEMPLATE POPULATE FUNCTIONS */
/*
 * smt_add_query_spec() - Adds a query specification to a template.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   template(in/out): schema template
 *   specification(in): query specification
 */

int
smt_add_query_spec (SM_TEMPLATE * template_, const char *specification)
{
  int error = NO_ERROR;
  SM_QUERY_SPEC *query_spec;
  SM_CLASS_TYPE ct;

  query_spec = classobj_make_query_spec (specification);

  if (query_spec == NULL)
    {
      error = er_errid ();
    }
  else
    {
      ct = template_->class_type;
      if (ct == SM_VCLASS_CT)
        {
          WS_LIST_APPEND (&template_->query_spec, query_spec);
        }
      else
        {
          db_ws_free (query_spec);
          error = ER_SM_INVALID_CLASS;
          er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 0);
        }
    }

  return error;
}

/* VIRTUAL SCHEMA OPERATION TEMPLATE FUNCTIONS */
/*
 * smt_def_typed_class() - Begin the definition of a new virtual class.
 * Creates an empty template.  The class name is not registered at this
 * time.  It will be registered during smt_finish_class
 *   return: template
 *   name(in):
 *   ct(in):
 */

SM_TEMPLATE *
smt_def_typed_class (const char *name, SM_CLASS_TYPE ct)
{
  return def_class_internal (name, ct);
}

/*
 * smt_get_class_type() - Return the type of a class template
 *   return: class type
 *   template(in):
 */

SM_CLASS_TYPE
smt_get_class_type (SM_TEMPLATE * template_)
{
  return template_->class_type;
}

/*
 * smt_get_class_type() - Convenience function to return the type of class,
 *   that is whether, a virtual class, component class or a view
 *   return: class type
 *   class(in):
 */

SM_CLASS_TYPE
sm_get_class_type (SM_CLASS * class_)
{
  return class_->class_type;
}

/*
 * smt_change_attribute_pos() - Changes an attribute ordering of a template.
 *    The attribute ordering may be changed if either the "change_first"
 *    argument is "true" or "change_after_attribute" is non-null and contains
 *    the name of an existing attribute.
 *    If all operations are successful, the changed attribute is returned in
 *    "found_att".
 *
 *   return: NO_ERROR on success, non-zero for ERROR
 *   template(in/out): schema template
 *   name(in): attribute current name
 *   change_first(in): the attribute will become the first in the attributes
 *                     list
 *   change_after_attribute(in): the attribute will be repositioned
 *                               after the attribute with the given name
 */
int
smt_change_attribute_pos (SM_TEMPLATE * template_, const char *name,
                          const bool change_first, const char *change_after_attribute)
{
  int error_code = NO_ERROR;
  SM_ATTRIBUTE *att = NULL;
  SM_ATTRIBUTE **att_list = NULL;
  char real_name[SM_MAX_IDENTIFIER_LENGTH] = { 0 };
  char change_after_attribute_real_name[SM_MAX_IDENTIFIER_LENGTH] = { 0 };

  assert (template_ != NULL);
  assert (name != NULL);
  assert (change_first == true || change_after_attribute != NULL);

  if (template_ == NULL || name == NULL || (change_first == false && change_after_attribute == NULL))
    {
      ERROR0 (error_code, ER_SM_INVALID_ARGUMENTS);

      goto error_exit;
    }

  att_list = &template_->attributes;

  sm_downcase_name (name, real_name, SM_MAX_IDENTIFIER_LENGTH);
  name = real_name;
  if (!sm_check_name (name))
    {
      error_code = er_errid ();
      goto error_exit;
    }

  if (change_after_attribute != NULL)
    {
      sm_downcase_name (change_after_attribute, change_after_attribute_real_name, SM_MAX_IDENTIFIER_LENGTH);
      change_after_attribute = change_after_attribute_real_name;
    }

  error_code = smt_find_attribute (template_, name, &att);
  if (error_code != NO_ERROR)
    {
      goto error_exit;
    }

  assert (att != NULL);

  /* change order */
  error_code = smt_change_attribute_pos_in_list (att_list, att, change_first, change_after_attribute);

  return error_code;

error_exit:

  return error_code;
}

/*
 * smt_change_attribute_pos_in_list()
 *   return: NO_ERROR on success, non-zero for ERROR
 *   att_list(in/out): the list to add to
 *   att(in): the attribute to add
 *   add_first(in): the attribute should be added at the beginning of the
 *                  attributes list
 *   add_after_attribute(in): the attribute should be added in the attributes
 *                            list after the attribute with the given name
 */

static int
smt_change_attribute_pos_in_list (SM_ATTRIBUTE ** att_list,
                                  SM_ATTRIBUTE * att, const bool change_first, const char *change_after_attribute)
{
  int error_code = NO_ERROR;

  /* we must change the position : either to first or after another element */
  assert ((change_first && change_after_attribute == NULL) || (!change_first && change_after_attribute != NULL));

  assert (att != NULL);
  assert (att_list != NULL);

  /* first remove the attribute from list */
  if (WS_LIST_REMOVE (att_list, att) != 1)
    {
      error_code = ER_SM_ATTRIBUTE_NOT_FOUND;
      return error_code;
    }

  att->next = NULL;
  error_code = smt_add_attribute_to_list (att_list, att, change_first, change_after_attribute);
  /* error code already set */
  return error_code;
}

/*
 * smt_change_attribute_domain() - Changes an attribute domain of a template.
 *
 *   return: NO_ERROR on success, non-zero for ERROR
 *   template(in/out): schema template
 *   name(in): attribute current name
 *   new_domain(in): new attribute domain
 */
int
smt_change_attribute_domain (SM_TEMPLATE * template_, const char *name, TP_DOMAIN * new_domain)
{
  int error_code = NO_ERROR;
  SM_ATTRIBUTE *att = NULL;

  assert (template_ != NULL);
  assert (name != NULL);
  assert (new_domain != NULL);

  if (template_ == NULL || name == NULL || new_domain == NULL)
    {
      ERROR0 (error_code, ER_SM_INVALID_ARGUMENTS);

      goto error_exit;
    }

  att = classobj_find_attribute (template_->attributes, name);
  if (att == NULL)
    {
      error_code = ER_SM_ATTRIBUTE_NOT_FOUND;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1, name);

      goto error_exit;
    }

  /* change domain */
  att->sma_domain = new_domain;

  return error_code;

error_exit:

  return error_code;
}
