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
 * object_template.c - Object template module
 *
 *      This contains code for attribute access, instance creation
 *      and deletion, and misc utilitities related to instances.
 *
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <stdarg.h>

#include "db.h"
#include "dbtype.h"
#include "error_manager.h"
#include "system_parameter.h"
#include "server_interface.h"
#include "work_space.h"
#include "object_domain.h"
#include "object_primitive.h"
#include "set_object.h"
#include "class_object.h"
#include "schema_manager.h"
#include "object_accessor.h"
#include "view_transform.h"
#include "authenticate.h"
#include "locator_cl.h"
#include "parser.h"
#include "transaction_cl.h"
#include "environment_variable.h"
#include "transform.h"
#include "execute_statement.h"
#include "network_interface_cl.h"

/* Include this last; it redefines some macros! */
#include "dbval.h"

/*
 *       			GLOBAL VARIABLES
 */

/*
 *                            OBJECT MANAGER AREAS
 */

/*
 * obj_Template_traversal
 *
 *
 */

static unsigned int obj_Template_traversal = 0;
/*
 * Must make sure template savepoints have unique names to allow for concurrent
 * or nested updates.  Could be resetting this at db_restart() time.
 */
#if 0
static unsigned int template_savepoint_count = 0;
#endif


static DB_VALUE *check_att_domain (SM_ATTRIBUTE * att, DB_VALUE * proposed_value);
static int check_constraints (SM_ATTRIBUTE * att, DB_VALUE * value, unsigned force_check_not_null);
#if 0
static int quick_validate (SM_VALIDATION * valid, DB_VALUE * value);
static void cache_validation (SM_VALIDATION * valid, DB_VALUE * value);
#endif
static void begin_template_traversal (void);
static OBJ_TEMPLATE *make_template (MOP object, MOP classobj);
static int validate_template (OBJ_TEMPLATE * temp);
static OBJ_TEMPASSIGN *obt_make_assignment (OBJ_TEMPLATE * template_ptr, SM_ATTRIBUTE * att);
static void obt_free_assignment (OBJ_TEMPASSIGN * assign);
static void obt_free_template (OBJ_TEMPLATE * template_ptr);
static int populate_defaults (OBJ_TEMPLATE * template_ptr);
static MOP create_template_object (OBJ_TEMPLATE * template_ptr);
static int access_object (OBJ_TEMPLATE * template_ptr, MOP * object, MOBJ * objptr);
static int obt_convert_set_templates (SETREF * setref);
static int obt_final_check_set (SETREF * setref, int *has_uniques);

static int obt_final_check (OBJ_TEMPLATE * template_ptr, int check_non_null, int *has_uniques);
static int obt_apply_assignment (MOP op, SM_ATTRIBUTE * att, char *mem, DB_VALUE * value);
static int obt_apply_assignments (OBJ_TEMPLATE * template_ptr);

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * obt_find_attribute - locate an attribute for a template.
 *      return: error code
 *      template(in) :
 *      name(in): attribute name
 *      attp(out): returned pointer to attribute descriptor
 *
 * Note:
 *    This is a bit simpler than the others since we have the class
 *    cached in the template.
 *
 */

int
obt_find_attribute (OBJ_TEMPLATE * template_ptr, const char *name, SM_ATTRIBUTE ** attp)
{
  int error = NO_ERROR;
  SM_CLASS *class_;
  SM_ATTRIBUTE *att;

  att = NULL;

  class_ = template_ptr->class_;

  att = classobj_find_attribute (class_->attributes, name);

  if (error == NO_ERROR && att == NULL)
    {
      ERROR1 (error, ER_OBJ_INVALID_ATTRIBUTE, name);
    }

  *attp = att;
  return error;
}
#endif

/*
 *
 *                           ASSIGNMENT VALIDATION
 *
 *
 */

/*
 * check_att_domain - This checks to see if a value is within the domain of an
 *                    attribute.
 *
 *      returns: actual value container
 *      att(in): attribute name (for error messages)
 *      proposed_value(in): original value container
 *
 * Note:
 *    It calls tp_domain_check & tp_domain_coerce to do the work, this
 *    function mostly serves to inerpret the return codes and set an
 *    appropriate error condition.
 *
 */

static DB_VALUE *
check_att_domain (SM_ATTRIBUTE * att, DB_VALUE * proposed_value)
{
  TP_DOMAIN_STATUS status;
  DB_VALUE *value;

  assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

  value = proposed_value;

  /*
   * Note that we set the "exact" match flag true to disallow "tolerance"
   * matches.  Some types (such as CHAR) may appear to overflow the domain,
   * but can be truncated during the coercion process.
   */
  status = tp_domain_check (att->sma_domain, value, TP_EXACT_MATCH);

  if (status != DOMAIN_COMPATIBLE)
    {
      value = pr_make_ext_value ();
      if (value == NULL)
        {
          return NULL;
        }

      status = tp_value_coerce (proposed_value, value, att->sma_domain);
      if (status != DOMAIN_COMPATIBLE)
        {
          (void) pr_free_ext_value (value);
        }
    }

  if (status != DOMAIN_COMPATIBLE)
    {
      switch (status)
        {
        case DOMAIN_ERROR:
          /* error has already been set */
          break;
        case DOMAIN_OVERFLOW:
          (void) tp_domain_status_er_set (status, ARG_FILE_LINE, proposed_value, att->sma_domain);
          assert (er_errid () != NO_ERROR);
          break;
        case DOMAIN_INCOMPATIBLE:
        default:
          /*
           * the default case shouldn't really be encountered, might want to
           * signal a different error.  The OVERFLOW case should only
           * be returned during coercion which wasn't requested, to be safe,
           * treat these like a domain conflict.  Probably need a more generic
           * domain conflict error that uses full printed representations
           * of the entire domain.
           */
          er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_DOMAIN_CONFLICT, 1, att->name);
          break;
        }

      /* return NULL if incompatible */
      value = NULL;
    }

  return value;
}

/*
 * check_constraints - This function is used to check a proposed value
 *                     against the integrity constraints defined
 *                     for an attribute.
 *
 *      returns: error code
 *
 *      att(in): attribute descriptor
 *      value(in): value to verify
 *      force_check_not_null(in): force NOT NULL constraint check
 *
 * Note:
 *    If will return an error code if any of the constraints are violated.
 *
 */

static int
check_constraints (SM_ATTRIBUTE * att, DB_VALUE * value, unsigned force_check_not_null)
{
  int error = NO_ERROR;
  MOP mop;

  assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

  /* check NOT NULL constraint */
  if (value == NULL || DB_IS_NULL (value)
      || (att->sma_domain->type == tp_Type_object && (mop = DB_GET_OBJECT (value)) && WS_MOP_IS_NULL (mop)))
    {
      if (att->flags & SM_ATTFLAG_NON_NULL)
        {
          if (!force_check_not_null)
            {
              assert (DB_IS_NULL (value));
              assert (att->sma_domain->type != tp_Type_object);

              /* This is allowed to happen only during INSERT statements,
               * since the next serial value will be filled in at a later
               * time. For other cases, the force_check_not_null flag should
               * be set. */
            }
          else
            {
              ERROR1 (error, ER_OBJ_ATTRIBUTE_CANT_BE_NULL, att->name);
            }
        }
    }
  else
    {
      /* Check size constraints */
      if (tp_check_value_size (att->sma_domain, value) != DOMAIN_COMPATIBLE)
        {
          /* probably need an error message that isn't specific to "string" types */
          ERROR2 (error, ER_OBJ_STRING_OVERFLOW, att->name, att->sma_domain->precision);
        }
    }

  return error;
}

#if 0
/*
 * quick_validate - This function is where we try to determine as fast as
 *                  possible if a value is compatible with
 *                  a certain attribute's domain.
 *      returns: non-zero if the value is known to be valid
 *      valid(in): validation cache
 *      value(in): value to ponder
 */

static int
quick_validate (SM_VALIDATION * valid, DB_VALUE * value)
{
  int is_valid;
  DB_TYPE type;

  if (valid == NULL || value == NULL)
    {
      return 0;
    }

  is_valid = 0;
  type = DB_VALUE_TYPE (value);

  switch (type)
    {
    case DB_TYPE_OBJECT:
      {
        DB_OBJECT *obj, *class_;

        obj = db_get_object (value);
        if (obj != NULL)
          {
            class_ = sm_get_class (obj);
            if (class_ != NULL)
              {
                if (class_ == valid->last_class)
                  {
                    is_valid = 1;
                  }
                else
                  {
                    /* wasn't on the first level cache, check the list */
                    is_valid = ml_find (valid->validated_classes, class_);
                    /* if its on the list, auto select this for the next time around */
                    if (is_valid)
                      {
                        valid->last_class = class_;
                      }
                  }
              }
          }
      }
      break;

    case DB_TYPE_SEQUENCE:
      {
        DB_SET *set;
        DB_DOMAIN *domain;

        set = db_get_set (value);
        domain = set_get_domain (set);
        if (domain == valid->last_setdomain)
          {
            is_valid = 1;
          }
      }
      break;

    case DB_TYPE_VARCHAR:
      if (type == valid->last_type && DB_GET_STRING_PRECISION (value) == valid->last_precision)
        {
          is_valid = 1;
        }
      break;

    case DB_TYPE_VARBIT:
      if (type == valid->last_type && DB_GET_VARBIT_PRECISION (value) == valid->last_precision)
        {
          is_valid = 1;
        }
      break;

    case DB_TYPE_NUMERIC:
      if (type == valid->last_type
          && DB_GET_NUMERIC_PRECISION (value) == valid->last_precision
          && DB_GET_NUMERIC_SCALE (value) == valid->last_scale)
        {
          is_valid = 1;
        }
      break;

    default:
      if (type == valid->last_type)
        {
          is_valid = 1;
        }
      break;
    }

  return is_valid;
}

/*
 * cache_validation
 *      return : none
 *      valid(in): validation cache
 *      value(in): value known to be good
 *
 * Note:
 *    Caches information about the data value in the validation cache
 *    so hopefully we'll be quicker about validating values of this
 *    form if we find them again.
 *
 */

static void
cache_validation (SM_VALIDATION * valid, DB_VALUE * value)
{
  DB_TYPE type;

  if (valid == NULL || value == NULL)
    {
      return;
    }

  type = DB_VALUE_TYPE (value);
  switch (type)
    {
    case DB_TYPE_OBJECT:
      {
        DB_OBJECT *obj, *class_;

        obj = db_get_object (value);
        if (obj != NULL)
          {
            class_ = sm_get_class (obj);
            if (class_ != NULL)
              {
                valid->last_class = class_;
                /*
                 * !! note that we have to be building an external object list
                 * here so these serve as GC roots.  This is kludgey, we should
                 * be encapsulating structure rules inside cl_ where the
                 * SM_VALIDATION is allocated.
                 */
                (void) ml_ext_add (&valid->validated_classes, class_, NULL);
              }
          }
      }
      break;

    case DB_TYPE_SEQUENCE:
      {
        DB_SET *set;

        set = db_get_set (value);
        valid->last_setdomain = set_get_domain (set);
      }
      break;

    case DB_TYPE_VARCHAR:
    case DB_TYPE_VARBIT:
      valid->last_type = type;
      valid->last_precision = DB_VALUE_PRECISION (value);
      valid->last_scale = 0;
      break;

    case DB_TYPE_NUMERIC:
      valid->last_type = type;
      valid->last_precision = DB_VALUE_PRECISION (value);
      valid->last_scale = DB_VALUE_SCALE (value);
      break;

    default:
      valid->last_type = type;
      valid->last_precision = 0;
      valid->last_scale = 0;
      break;
    }
}
#endif

/*
 * obt_check_assignment - This is the main validation routine
 *                    for attribute assignment.
 *      returns: value container
 *      att(in): attribute descriptor
 *      proposed_value(in): value to assign
 *      force_check_not_null(in): force check for NOT NULL
 *                                constraints
 *
 *
 * Note:
 *    It is used both by the direct assignment function obj_set and also
 *    by object templates (which do grouped assignments).  Any other function
 *    that does attribute value assignment should also use this function
 *    or be VERY careful about the rules contained here.
 *    The check_unique flag is normally turned on only if we're building
 *    an object template because we have to check for constraint violation
 *    before allowing the rest of the template to be built.  For immediate
 *    attribute assignment (not using templates) we delay the checking for
 *    unique constraints until later (in assign_value) so we only have to
 *    do one server call instead of two.  Would be nice if templates could
 *    have a way to "batch up" their unique attribute checks.
 *    This function will return NULL if an error was detected.
 *    It will return the propsed_value pointer if the assignment is
 *    acceptable.
 *    It will return a new value container if the proposed_value wasn't
 *    acceptable but it was coerceable to a valid value.
 *    The caller must check to see if the returned value is different
 *    and if so free it with pr_free_ext_value() when done.
 *
 */

DB_VALUE *
obt_check_assignment (SM_ATTRIBUTE * att, DB_VALUE * proposed_value, unsigned force_check_not_null)
{
  DB_VALUE *value;

  /* assume this will be ok */
  value = proposed_value;

  /* for simplicity, convert this into a container with a NULL type */
  if (value == NULL)
    {
      value = pr_make_ext_value ();
    }
  else
    {
#if 1
      value = check_att_domain (att, proposed_value);
      if (value != NULL)
        {
          if (check_constraints (att, value, force_check_not_null) != NO_ERROR)
            {
              if (value != proposed_value)
                {
                  (void) pr_free_ext_value (value);
                }
              value = NULL;
            }
        }
#else
      /*
       * before we make the expensive checks, see if we've got some cached
       * validation information handy
       */
      if (!quick_validate (valid, value))
        {
          value = check_att_domain (att, proposed_value);
          if (value != NULL)
            {
              if (check_constraints (att, value, force_check_not_null) != NO_ERROR)
                {
                  if (value != proposed_value)
                    {
                      (void) pr_free_ext_value (value);
                    }
                  value = NULL;
                }
              else
                {
                  /*
                   * we're ok, if there was no coercion required, remember this for
                   * next time.
                   */
                  if (value == proposed_value)
                    {
                      cache_validation (valid, proposed_value);
                    }
                }
            }
        }
#endif
    }

  return value;
}

/*
 *
 *                         OBJECT TEMPLATE ASSIGNMENT
 *
 *
 */


/*
 * begin_template_traversal - This "allocates" the traversal counter
 *                            for a new template traversal.
 *      return : none
 *
 * Note :
 *    obj_Template_traversal is set to this value so it can
 *    be tested during traversal.
 *    This is in a function just so that the rules for skipping a traversal
 *    value of zero can be encapsulated.
 *
 */
static void
begin_template_traversal (void)
{
  /* increment the counter */
  obj_Template_traversal++;

  /* don't let it be zero */
  if (obj_Template_traversal == 0)
    {
      obj_Template_traversal++;
    }
}

/*
 * make_template - This initializes a new object template.
 *    return: new object template
 *    object(in): the object that the template is being created for
 *    classobj(in): the class of the object
 *
 */
static OBJ_TEMPLATE *
make_template (MOP object, MOP classobj)
{
  OBJ_TEMPLATE *template_ptr;
  LOCK mode;
  AU_TYPE auth;
  SM_CLASS *class_;
  MOBJ obj;
  OBJ_TEMPASSIGN **vec;

  template_ptr = NULL;          /* init */

  /* fetch & lock the class with the appropriate options */
  mode = S_LOCK;
  if (object == NULL)
    {
      auth = AU_INSERT;
    }
  else if (object != classobj)
    {
      auth = AU_UPDATE;
    }
  else
    {
      /*
       * class variable update
       * NOTE: It might be good to use AU_FETCH_WRITE here and then
       * use locator_update_class to set the dirty bit after the template
       * has been successfully applied.
       */
      mode = X_LOCK;
      auth = AU_ALTER;
    }

  if (au_fetch_class (classobj, &class_, mode, auth))
    {
      return NULL;
    }

  assert (class_->att_count > 0);

  /*
   * we only need to keep track of the base class if this is a
   * virtual class, for proxies, the instances look like usual
   */

  if (class_->class_type == SM_VCLASS_CT        /* a view, and... */
      && object != classobj /* we are not doing a meta class update */ )
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_IT_NOT_UPDATABLE_STMT, 0);
      return NULL;
    }

  /*
   * If this is an instance update, fetch & lock the instance.
   * NOTE: It might be good to use AU_FETCH_WRITE and use locator_update_instance
   * to set the dirty bit after the template has been successfully applied.
   *
   * If this is a virtual instance on a non-proxy, could be locking
   * the associated instance as well. Is this already being done ?
   */
  if (object != NULL && object != classobj)
    {
      if (au_fetch_instance (object, &obj, X_LOCK, AU_UPDATE))
        {
          return NULL;
        }

      /*
       * Could cache the object memory pointer this in the template as
       * well but that would require that it be pinned for a long
       * duration through code that we don't control.  Dangerous.
       */
    }

  template_ptr = (OBJ_TEMPLATE *) malloc (sizeof (OBJ_TEMPLATE));
  if (template_ptr != NULL)
    {
      template_ptr->object = object;
      template_ptr->classobj = classobj;

      /*
       * cache the class info directly in the template, will need
       * to remember the transaction id and chn for validation
       */
      template_ptr->class_ = class_;

      template_ptr->tran_id = tm_Tran_index;
      template_ptr->schema_id = sm_local_schema_version ();
      template_ptr->assignments = NULL;
      template_ptr->traversal = 0;
      template_ptr->traversed = 0;
      template_ptr->uniques_were_modified = 0;

      /*
       * Don't do this until we've initialized the other stuff;
       * OTMPL_NASSIGNS relies on the "class" attribute of the template.
       */

      template_ptr->nassigns = (template_ptr->class_->att_count);

      vec = NULL;
      if (template_ptr->nassigns)
        {
          int i;

          vec = (OBJ_TEMPASSIGN **) malloc (template_ptr->nassigns * sizeof (OBJ_TEMPASSIGN *));
          if (vec == NULL)
            {
              free_and_init (template_ptr);
              return NULL;
            }

          for (i = 0; i < template_ptr->nassigns; i++)
            {
              vec[i] = NULL;
            }
        }

      template_ptr->assignments = vec;
    }

  return template_ptr;
}

/*
 * validate_template - This is used to validate a template before each operation
 *      return: error code
 *      temp(in): template to validate
 *
 */

static int
validate_template (OBJ_TEMPLATE * temp)
{
  int error = NO_ERROR;

  if (temp != NULL && (temp->tran_id != tm_Tran_index || temp->schema_id != sm_local_schema_version ()))
    {
      error = ER_OBJ_INVALID_TEMPLATE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
    }

  return error;
}

/*
 * obt_make_assignment - This initializes a new assignment template.
 *    return: template assignment structure
 *    template(in):
 *    att(in):
 *
 * Note:
 *    It also adds it to a containing template.
 */

static OBJ_TEMPASSIGN *
obt_make_assignment (OBJ_TEMPLATE * template_ptr, SM_ATTRIBUTE * att)
{
  OBJ_TEMPASSIGN *assign;

  assign = (OBJ_TEMPASSIGN *) malloc (sizeof (OBJ_TEMPASSIGN));
  if (assign != NULL)
    {
      assign->obj = NULL;
      assign->variable = NULL;
      assign->att = att;
      assign->is_default = 0;

      template_ptr->assignments[att->order] = assign;
      if (classobj_has_unique_constraint (att->constraints))
        {
          template_ptr->uniques_were_modified = 1;
        }
    }

  return assign;
}

/*
 * obt_free_assignment - Work function for obt_free_template.
 *      return: none
 *      assign(in): an assignment template
 *
 * Note :
 *    Frees an attribute assignment template.  If the assigment contains
 *    an object template rather than a DB_VALUE, it will be freed by
 *    recursively calling obj_free_template.
 *
 */

static void
obt_free_assignment (OBJ_TEMPASSIGN * assign)
{
  DB_VALUE *value = NULL;
  SETREF *setref;
  int i, set_size;

  if (assign != NULL)
    {
      if (assign->variable != NULL)
        {

          DB_TYPE av_type;

          /* check for nested templates */
          av_type = DB_VALUE_TYPE (assign->variable);
          if (TP_IS_SET_TYPE (av_type) && DB_GET_SET (assign->variable) != NULL)
            {
              /* must go through and free any elements that may be template pointers */
              setref = DB_PULL_SET (assign->variable);
              if (setref->set != NULL)
                {
                  set_size = setobj_size (setref->set);
                  for (i = 0; i < set_size; i++)
                    {
                      setobj_get_element_ptr (setref->set, i, &value);
                    }
                }
            }

          (void) pr_free_ext_value (assign->variable);
        }

      free_and_init (assign);
    }
}

/*
 * obt_free_template - This frees a hierarchical object template.
 *      return: none
 *      template(in): object template
 *
 * Note :
 *    It will be called by obt_update when the template has been applied
 *    or can be called by obt_quit to abort the creation of the template.
 *    Since the template can contain circular references, must be careful and
 *    use a traversal flag in each template.
 *
 */

static void
obt_free_template (OBJ_TEMPLATE * template_ptr)
{
  OBJ_TEMPASSIGN *a;
  int i;

  if (!template_ptr->traversed)
    {
      template_ptr->traversed = 1;

      for (i = 0; i < template_ptr->nassigns; i++)
        {
          a = template_ptr->assignments[i];
          if (a == NULL)
            {
              continue;
            }

          if (a->obj != NULL)
            {
              obt_free_template (a->obj);
            }

          obt_free_assignment (a);
        }

      if (template_ptr->assignments)
        {
          free_and_init (template_ptr->assignments);
        }

      free_and_init (template_ptr);
    }
}

/*
 * populate_defaults - This populates a template with the default values
 *                      for a class.
 *      returns: error code
 *      template(in): template to fill out
 *
 * Note :
 *    This is necessary for INSERT templates.  The assignments are marked
 *    so that if an assignment is later made to the template with the
 *    same name, we don't generate an error because its ok to override
 *    a default value.
 *    If an assignment is already found with the name, it is assumed
 *    that an initial value has already been given and the default is
 *    ignored.
 *
 */

static int
populate_defaults (OBJ_TEMPLATE * template_ptr)
{
  SM_ATTRIBUTE *att;
  OBJ_TEMPASSIGN *a, *exists;
  SM_CLASS *class_;

  class_ = template_ptr->class_;

  /*
   * populate with the standard default values, ignore duplicate
   * assignments if the virtual class has already supplied
   * a value for these.
   */
  for (att = class_->attributes; att != NULL; att = att->next)
    {
      /*
       * can assume that the type is compatible and does not need
       * to be coerced
       */

      if (DB_VALUE_TYPE (&att->default_value.value) != DB_TYPE_NULL)
        {

          exists = template_ptr->assignments[att->order];

          if (exists == NULL)
            {
              a = obt_make_assignment (template_ptr, att);
              if (a == NULL)
                {
                  goto memory_error;
                }
              a->is_default = 1;
              a->variable = pr_make_ext_value ();
              if (a->variable == NULL)
                {
                  goto memory_error;
                }
              /* would be nice if we could avoid copying here */
              if (pr_clone_value (&att->default_value.value, a->variable))
                {
                  goto memory_error;
                }
            }
        }
    }

  return (NO_ERROR);

memory_error:
  /*
   * Here we couldn't allocate sufficient memory for the template and its
   * values. Probably the template should be marked as invalid and
   * the caller be forced to throw it away and start again since
   * its current state is unknown.
   */
  return er_errid ();
}

/*
 * obt_def_object - This initializes a new template for an instance of
 *                  the given class.
 *      return: new template
 *      class(in): class of the new object
 *
 * Note :
 *    This template can then be populated with assignments and given
 *    to obt_update to create the instances.
 *
 */

OBJ_TEMPLATE *
obt_def_object (MOP class_mop)
{
  OBJ_TEMPLATE *template_ptr = NULL;

  if (!locator_is_class (class_mop))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_NOT_A_CLASS, 0);
    }
  else
    {
      template_ptr = make_template (NULL, class_mop);
    }

  return template_ptr;
}

/*
 * obt_edit_object - This is used to initialize an editing template
 *                   on an existing object.
 *
 *      returns: template
 *      object(in): existing instance
 *
 */

OBJ_TEMPLATE *
obt_edit_object (MOP object)
{
  OBJ_TEMPLATE *template_ptr = NULL;

  if (locator_is_class (object))
    {
      /*
       * create a class object template, these are only allowed to
       * update class attributes
       */
      template_ptr = make_template (object, object);
    }
  else
    {
      DB_OBJECT *class_;
      /*
       * Need to make sure we have the class accessible, don't just
       * dereference obj->class. This gets a read lock early but that's ok
       * since we know we're dealing with an instance here.
       * Should be handling this inside make_template.
       */
      class_ = sm_get_class (object);
      if (class_ != NULL)
        {
          template_ptr = make_template (object, class_);
        }
    }

  return template_ptr;
}

/*
 * obt_quit - This is used to abort the creation of an object template
 *            and release all the allocated storage.
 *      return: error code
 *      template(in): template to throw away
 *
 */

int
obt_quit (OBJ_TEMPLATE * template_ptr)
{
  if (template_ptr != NULL)
    {
      obt_free_template (template_ptr);
    }

  return NO_ERROR;
}

/*
 * obt_assign - This is used to assign a value to an attribute
 *              in an object template.
 *    return: error code
 *    template(in): object template
 *    att(in):
 *    value(in): value to assign
 *
 * Note:
 *    The usual semantic checking on assignment will be performed and
 *    an error returned if the assignment would be invalid.
 *    If the base_assignment flag is zero (normal), the name/value pair
 *    must correspond to the virtual class definition and translation
 *    will be performed if this is a template on a vclass.  If the
 *    base_assignment flag is non-zero, the name/value pair are assumed
 *    to correspond to the base class and translation is not performed.
 *    If this is not a template on a virtual class, the flag has
 *    no effect.
 */

int
obt_assign (OBJ_TEMPLATE * template_ptr, SM_ATTRIBUTE * att, DB_VALUE * value)
{
  int error = NO_ERROR;
  OBJ_TEMPASSIGN *assign;
  DB_VALUE *actual;

  if ((template_ptr == NULL) || (att == NULL) || (value == NULL))
    {
      ERROR0 (error, ER_OBJ_INVALID_ARGUMENTS);
      goto error_exit;
    }

  if (validate_template (template_ptr))
    {
      goto error_exit;
    }

  /* check for duplicate assignments */
  assign = NULL;
  if (template_ptr->assignments)
    {
      assign = template_ptr->assignments[att->order];
    }

  if (assign)
    {
      ERROR1 (error, ER_OBJ_DUPLICATE_ASSIGNMENT, att->name);
      goto error_exit;
    }

  /* check assignment validity */
  actual = obt_check_assignment (att, value, 0);
  if (actual == NULL)
    {
      goto error_exit;
    }
  else
    {
      assign = obt_make_assignment (template_ptr, att);
      if (assign == NULL)
        {
          goto error_exit;
        }
    }

  if (actual != value)
    {
      if (assign->variable)
        {
          pr_free_ext_value (assign->variable);
        }
      assign->variable = actual;
    }
  else
    {
      if (assign->variable)
        {
          /*
           *
           * Clear the contents, but recycle the container.
           */
          (void) pr_clear_value (assign->variable);
        }
      else
        {
          assign->variable = pr_make_ext_value ();
          if (assign->variable == NULL)
            {
              goto error_exit;
            }
        }
      /*
       *
       * Note that this copies the set value, might not want to do this
       * when called by the interpreter under controlled conditions,
       *
       * !!! See about optimizing this so we don't do so much set copying !!!
       */
      error = pr_clone_value (value, assign->variable);
    }

  return error;

error_exit:
  return er_errid ();
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * obt_set -
 *    return: error code
 *    template(in): attname
 *    attname(in): value
 *    value(in):
 *
 * Note:
 *    This is just a shell around obt_assign that doesn't
 *    make the base_assignment flag public.
 */

int
obt_set (OBJ_TEMPLATE * template_ptr, const char *attname, DB_VALUE * value)
{
  int error = NO_ERROR;
  SM_ATTRIBUTE *att;

  if ((template_ptr == NULL) || (attname == NULL) || (value == NULL))
    {
      ERROR0 (error, ER_OBJ_INVALID_ARGUMENTS);
    }
  else
    {
      if (validate_template (template_ptr))
        {
          return er_errid ();
        }

      if (obt_find_attribute (template_ptr, attname, &att))
        {
          return er_errid ();
        }

      error = obt_assign (template_ptr, att, value, NULL);
    }

  return error;
}

/* temporary backward compatibility */
/*
 * obt_set_desc - This is similar to obt_set() except that
 *                the attribute is identified through a descriptor rather than
 *                an attribute name.
 *      return: error code
 *      template(in): object template
 *      desc(in): attribute descriptor
 *      value(in): value to assign
 *
 */

int
obt_desc_set (OBJ_TEMPLATE * template_ptr, SM_DESCRIPTOR * desc, DB_VALUE * value)
{
  int error = NO_ERROR;
  SM_CLASS *class_;
  SM_ATTRIBUTE *att;

  if ((template_ptr == NULL) || (desc == NULL) || (value == NULL))
    {
      ERROR0 (error, ER_OBJ_INVALID_ARGUMENTS);
    }
  else
    {
      if (validate_template (template_ptr))
        {
          return er_errid ();
        }

      /*
       * Note that we pass in the outer class MOP rather than an object
       * since we don't necessarily have an object at this point.
       */
      error = sm_get_descriptor_component (template_ptr->classobj, desc, 1, &class_, &att);
      if (error != NO_ERROR)
        {
          return error;
        }

      error = obt_assign (template_ptr, att, value, desc->valid);
    }

  return error;
}
#endif

/*
 * create_template_object -
 *    return: MOP of new object
 *    template(in):
 */


static MOP
create_template_object (OBJ_TEMPLATE * template_ptr)
{
  MOP mop;
  char *obj;
  SM_CLASS *class_;

  mop = NULL;

  /* must flag this condition */
  ws_class_has_object_dependencies (template_ptr->classobj);

  class_ = template_ptr->class_;

  if (class_->class_type == SM_VCLASS_CT)
    {
      assert (false);
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_SM_INVALID_ARGUMENTS, 0);
      return NULL;
    }

  /*
   * NOTE: garbage collection can occur in either the call to
   * locator_add_instance (which calls locator_add_instance).  The object
   * we're caching can't contain any object references that aren't rooted
   * elsewhere.  Currently this is the case since the object is empty
   * and will be populated later with information from the template which IS
   * a GC root.
   */
  obj = obj_alloc (class_, 0);
  if (obj != NULL)
    {
      mop = locator_add_instance (obj, template_ptr->classobj);
    }

  if (mop != NULL)
    {
      template_ptr->object = mop;
    }

  return mop;
}

/*
 * access_object - This is a preprocessing function called by
 *                 obt_apply_assignments.
 *    return: error code
 *    template(in): object template
 *    object(in):
 *    objptr(out): pointer to instance (returned)
 *
 * Note:
 *    It ensures that the object associated with the template is locked
 *    and created if necessary.
 */
static int
access_object (OBJ_TEMPLATE * template_ptr, MOP * object, MOBJ * objptr)
{
  int error = NO_ERROR;
  MOP classobj, mop;
  MOBJ obj;

  /*
   * The class and instance was already locked&fetched
   * when the template was created.
   * The class pointer was cached since they are always pinned.
   * To avoid pinning the instance through a scope we don't control,
   * they aren't pinned during make_template but rather are "fetched"
   * again and pinned during obt_apply_assignments()
   * Authorization was checked when the template was created so don't
   * do it again.
   */

  obj = NULL;

  /*
   * First, check to see if this is an INSERT template and if so, create
   * the new object.
   */
  if (template_ptr->object == NULL)
    {
      if (create_template_object (template_ptr) == NULL)
        {
          return er_errid ();
        }
    }

  /*
   * Now, fetch/lock the instance and mark the class.
   * At this point, we want to be dealing with only the base object.
   */

  classobj = template_ptr->classobj;
  mop = template_ptr->object;

  if (mop != NULL)
    {
      error = au_fetch_instance_force (mop, &obj, X_LOCK);
      if (error == NO_ERROR)
        {
          /* must call this when updating instances */
          ws_class_has_object_dependencies (classobj);
        }
    }

  if (obj == NULL)
    {
      error = er_errid ();
    }
  else
    {
      *object = mop;
      *objptr = obj;
    }

  return error;
}

/*
 * obt_convert_set_templates - Work function for obt_apply_assignments.
 *    return: error code
 *    setref(in): set pointer from a template
 *
 * Note:
 *    This will iterate through the elements of a set (or sequence) and
 *    convert any elements that are templates in to actual instances.
 *    It will recursively call obt_apply_assignments for the templates
 *    found in the set.
 */

static int
obt_convert_set_templates (SETREF * setref)
{
  int error = NO_ERROR;
  DB_VALUE *value = NULL;
  int i, set_size;
  SETOBJ *set;

  if (setref != NULL)
    {
      set = setref->set;
      if (set != NULL)
        {
          set_size = setobj_size (set);
          for (i = 0; i < set_size && error == NO_ERROR; i++)
            {
              setobj_get_element_ptr (set, i, &value);
            }
        }
    }

  return error;
}

/*
 * obt_final_check_set - This is called when a set value is encounterd in
 *                       a template that is in the final semantic checking phase.
 *    return: error code
 *    setref(in): object template that provked this call
 *    has_uniques(in):
 *
 * Note:
 *    We must go through the set and look for each element that is itself
 *    a template for a new object.
 *    When these are found, recursively call obt_final_check to make sure
 *    these templates look ok.
 */

static int
obt_final_check_set (SETREF * setref, UNUSED_ARG int *has_uniques)
{
  int error = NO_ERROR;
  DB_VALUE *value = NULL;
  SETOBJ *set;
  int i, set_size;

  if (setref != NULL)
    {
      set = setref->set;
      if (set != NULL)
        {
          set_size = setobj_size (set);
          for (i = 0; i < set_size && error == NO_ERROR; i++)
            {
              setobj_get_element_ptr (set, i, &value);
            }
        }
    }

  return error;
}

/*
 * obt_check_missing_assignments - This checks a list of attribute definitions
 *                             against a template and tries to locate missing
 *                             assignments in the template that are required
 *                             in order to process an insert template.
 *    return: error code
 *    template(in): template being processed
 *
 * Note:
 *    This includes missing initializers for attributes that are defined
 *    to be NON NULL.
 *    It also includes attributes defined with a VID flag.
 */

int
obt_check_missing_assignments (OBJ_TEMPLATE * template_ptr)
{
  int error = NO_ERROR;
  SM_CLASS *class_;
  SM_ATTRIBUTE *att;
  OBJ_TEMPASSIGN *ass;

  /* only do this if its an insert template */

  if (template_ptr->object == NULL)
    {
      /* use the base_class if this is a virtual class insert */
      class_ = template_ptr->class_;

      for (att = class_->ordered_attributes; att != NULL && error == NO_ERROR; att = att->order_link)
        {

          if ((att->flags & SM_ATTFLAG_NON_NULL)
              && DB_IS_NULL (&att->default_value.value) && att->default_value.default_expr == DB_DEFAULT_NONE)
            {
              ass = template_ptr->assignments[att->order];
              if (ass == NULL)
                {
                  if (att->flags & SM_ATTFLAG_NON_NULL)
                    {
                      ERROR1 (error, ER_OBJ_MISSING_NON_NULL_ASSIGN, att->name);
                    }
                }
            }
        }
    }

  return error;
}

/*
 * obt_final_check
 *    return: error code
 *    template(in): object template
 *    check_non_null(in):
 *    has_uniques(in):
 *
 */

static int
obt_final_check (OBJ_TEMPLATE * template_ptr, int check_non_null, int *has_uniques)
{
  int error = NO_ERROR;
  OBJ_TEMPASSIGN *a;
  int i;

  /* have we already been here ? */
  if (template_ptr->traversal == obj_Template_traversal)
    {
      return NO_ERROR;
    }
  template_ptr->traversal = obj_Template_traversal;

  if (validate_template (template_ptr))
    {
      return er_errid ();
    }

  /*
   * We locked the object when the template was created, this
   * should still be valid.  If not, it should have been detected
   * by validate_template above.
   * Could create the new instances here but wait for a later step.
   */

  /*
   * Check missing assignments on an insert template
   */
  if (template_ptr->object == NULL)
    {
      if (populate_defaults (template_ptr))
        {
          return er_errid ();
        }

      if (check_non_null && obt_check_missing_assignments (template_ptr))
        {
          return er_errid ();
        }
    }

  /* does this template have uniques? */
  if (template_ptr->uniques_were_modified)
    {
      *has_uniques = 1;
    }

  /* this template looks ok, recursively go through the sub templates */
  for (i = 0; i < template_ptr->nassigns && error == NO_ERROR; i++)
    {
      a = template_ptr->assignments[i];
      if (a == NULL)
        {
          continue;
        }
      if (a->obj != NULL)
        {
          /* the non-null flag is only used for the outermost template */
          error = obt_final_check (a->obj, 1, has_uniques);
        }
      else
        {
          DB_TYPE av_type;

          av_type = DB_VALUE_TYPE (a->variable);
          if (TP_IS_SET_TYPE (av_type))
            {
              error = obt_final_check_set (DB_GET_SET (a->variable), has_uniques);
            }
        }
    }

  /* check unique_constraints, but only if not disabled */
  /*
   * test & set interface doesn't work right now, full savepoints are instead
   * being performed in obt_update_internal.
   */

  return error;
}

/*
 * obt_apply_assignment - This is used to apply the assignments in an object
 *                        template after all of the appropriate semantic
 *                        checking has taken place.
 *    return: error code
 *    op(in): class or instance pointer
 *    att(in): attribute descriptor
 *    mem(in): instance memory pointer (instance attribute only)
 *    value(in): value to assign
 *
 * Note:
 *    This used to be a lot more complicated because the translation
 *    of virtual values to base values was deferred until this step.
 *    Now, the values are translated immediately when they are added
 *    to the template.
 */

static int
obt_apply_assignment (MOP op, SM_ATTRIBUTE * att, char *mem, DB_VALUE * value)
{
  int error = NO_ERROR;

  assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

  if (!TP_IS_SET_TYPE (TP_DOMAIN_TYPE (att->sma_domain)))
    {
      error = obj_assign_value (op, att, mem, value);
    }
  else
    {
      /* for sets, first apply any templates in the set */
      error = obt_convert_set_templates (DB_GET_SET (value));
      if (error == NO_ERROR)
        {

          /* BE VERY CAREFUL HERE, IN THE OLD VERSION THE SET WAS BEING COPIED ? */
          error = obj_assign_value (op, att, mem, value);
        }
    }

  return error;
}

/*
 * obt_apply_assignments -
 *    return: error code
 *    template(in): object template
 *
 * Note:
 *    This is used to apply the assignments in an object template after all
 *    of the appropriate semantic checking has taken place.  Technically,
 *    there shouldn't be any errors here.  If errors do occurr, they will
 *    not cause a rollback of any partially applied assignments.  The only
 *    place this is likely to happen is if there are problems updating
 *    the unique constraint table but even this would represent a serious
 *    internal error that may have other consequences as well.
 */

static int
obt_apply_assignments (OBJ_TEMPLATE * template_ptr)
{
  int error = NO_ERROR;
  OBJ_TEMPASSIGN *a;
  DB_VALUE val;
  int pin;
  SM_CLASS *class_;
  DB_OBJECT *object = NULL;
  MOBJ mobj = NULL;
  char *mem;
  int i;

  /* have we already been here ? */
  if (template_ptr->traversal == obj_Template_traversal)
    {
      return NO_ERROR;
    }
  template_ptr->traversal = obj_Template_traversal;

  /* make sure we have a good template */
  if (validate_template (template_ptr))
    {
      return er_errid ();
    }

  /* perform all operations on the base class */
  class_ = template_ptr->class_;

  pin = -1;
  error = access_object (template_ptr, &object, &mobj);
  if (error == NO_ERROR)
    {
      pin = ws_pin (object, 1);
    }

  /* Apply the assignments */
  for (i = 0; i < template_ptr->nassigns && error == NO_ERROR; i++)
    {
      a = template_ptr->assignments[i];
      if (a == NULL)
        {
          continue;
        }

      /* find memory pointer if this is an instance attribute */
      mem = NULL;
      if (mobj != NULL)
        {
          mem = (char *) mobj + a->att->offset;
        }

      if (error == NO_ERROR)
        {
          /* check for template assignment that needs to be expanded */
          if (a->obj != NULL)
            {
              /* this is a template assignment, recurse on this template */
              error = obt_apply_assignments (a->obj);
              if (error == NO_ERROR)
                {
                  DB_MAKE_OBJECT (&val, a->obj->object);
                  error = obt_apply_assignment (object, a->att, mem, &val);
                }
            }
          else
            {
              /* non-template assignment */
              error = obt_apply_assignment (object, a->att, mem, a->variable);
            }
        }
    }

  if ((error == NO_ERROR) && (object != NULL))
    {
      ws_dirty (object);
    }

  /* unpin the object */
  if (pin != -1)
    {
      (void) ws_pin (object, pin);
    }

  /*
   * check for unique constraint violations.
   * if the object has uniques and this is an insert, we must
   * flush the object to ensure that the btrees for the uniques
   * are updated correctly.
   */
  if (error == NO_ERROR)
    {
      if (template_ptr->uniques_were_modified)
        {
          if (locator_flush_class (template_ptr->classobj) != NO_ERROR)
            {
              error = er_errid ();
            }
        }
    }

  return error;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * obt_retain_after_finish
 *    return: none
 *    template(in):
 *
 */
void
obt_retain_after_finish (OBJ_TEMPLATE * template_ptr)
{
  if (template_ptr)
    {
      template_ptr->discard_on_finish = 0;
    }
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * obt_update_internal
 *    return: error code
 *    template(in): object template
 *    newobj(in): return pointer to mop of new instance
 *    check_non_null(in): set if this is an internally defined template
 *
 */

int
obt_update_internal (OBJ_TEMPLATE * template_ptr, MOP * newobj, int check_non_null)
{
  int error = NO_ERROR;
  int has_uniques = 0;

  if (template_ptr == NULL)
    {
      return NO_ERROR;
    }

  error = validate_template (template_ptr);
  if (error == NO_ERROR)
    {
      /* allocate a new traversal counter for the check pass */
      begin_template_traversal ();
      error = obt_final_check (template_ptr, check_non_null, &has_uniques);
      if (error == NO_ERROR)
        {
          /* allocate another traversal counter for the assignment pass */
          begin_template_traversal ();
          error = obt_apply_assignments (template_ptr);
          if (error == NO_ERROR)
            {
              if (newobj != NULL)
                {
                  *newobj = template_ptr->object;
                }

              obt_free_template (template_ptr);
            }
        }
    }

  return error;
}

/*
 * Don't change the external interface to allow setting the check_non_null
 * flag.
 */
/*
 * obt_update - This will take an object template and apply all of
 *              the assignments, creating new objects as necessary
 *    return: error code
 *    template(in): object template
 *    newobj(in): return pointer to mop of new instance
 *
 * Note:
 *    If the top level template is for a new object, the mop will be returned
 *    through the "newobj" parameter.
 *    Note that the template will be freed here if successful
 *    so the caller must not asusme that it can be reused.
 *    The check_non_null flag is set in the case where the template
 *    is being created in response to the obj_create().
 *    Unfortunately, as the functions are defined, there is no way
 *    to supply initial values.  If the class has attributes that are
 *    defined with the NON NULL constraint, the usual template processing
 *    refuses to create the object until the missing values
 *    are supplied.  This means that it is impossible for the "atomic"
 *    functions to make an object whose attributes
 *    have the constraint.  This is arguably the correct behavior but
 *    it hoses 4GE since it isn't currently prepared to go check for
 *    creation dependencies and use full templates instead.
 */
int
obt_update (OBJ_TEMPLATE * template_ptr, MOP * newobj)
{
  return obt_update_internal (template_ptr, newobj, 1);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * obt_populate_known_arguments - Populate default arguments of template_ptr
 *    return: error code if unsuccessful
 *
 *    template_ptr(in): temporary object
 *
 * Note :
 *    This is necessary for INSERT templates.  The assignments are marked
 *    so that if an assignment is later made to the template with the
 *    same name, we don't generate an error because its ok to override
 *    a default value.
 *    If an assignment is already found with the name, it is assumed
 *    that an initial value has already been given and the default
 *    value is ignored.
 *
 */
int
obt_populate_known_arguments (OBJ_TEMPLATE * template_ptr)
{
  if (validate_template (template_ptr))
    {
      return er_errid ();
    }

  if (populate_defaults (template_ptr) != NO_ERROR)
    {
      return er_errid ();
    }

  return NO_ERROR;
}
#endif
