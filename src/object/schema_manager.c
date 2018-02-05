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
 * schema_manager.c - "Schema" (in the SQL standard sense) implementation
 */

#ident "$Id$"

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>


#include "dbtype.h"
#include "authenticate.h"
#include "string_opfunc.h"
#include "schema_manager.h"
#include "porting.h"
#include "chartype.h"
#include "error_manager.h"
#include "work_space.h"
#include "object_primitive.h"
#include "class_object.h"
#include "message_catalog.h"
#include "memory_alloc.h"
#include "environment_variable.h"

#include "language_support.h"
#include "object_representation.h"
#include "object_domain.h"
#include "set_object.h"
#include "transform_cl.h"
#include "locator_cl.h"
#include "statistics.h"
#include "network_interface_cl.h"
#include "parser.h"
#include "storage_common.h"
#include "transform.h"
#include "system_parameter.h"
#include "object_template.h"
#include "execute_schema.h"
#include "transaction_cl.h"
#include "release_string.h"
#include "execute_statement.h"
#include "md5.h"

#include "db.h"
#include "object_accessor.h"
#include "boot_cl.h"


/*
 * SCHEMA_DEFINITION
 *
 * description:
 *    Maintains information about an SQL schema.
 */

/*
   NOTE: This is simple-minded implementation for now since we don't yet
         support CREATE SCHEMA, SET SCHEMA, and associated statements.
 */

typedef struct schema_def
{

  /* This is the default qualifier for table/view names */
  char name[DB_MAX_SCHEMA_LENGTH * INTL_UTF8_MAX_CHAR_SIZE + 4];

  /* The only user who can delete this schema. */
  /* But, note that entry level doesn't support DROP SCHEMA anyway */
  MOP owner;

  /* The next three items are currently not used at all.
     They are simply a reminder of future TODOs.
     Although entry level SQL leaves out many schema management functions,
     entry level SQL does include specification of tables, views, and grants
     as part of CREATE SCHEMA statements. */

  void *tables;			/* unused dummy                             */
  void *views;			/* unused dummy                             */
  void *grants;			/* unused dummy                             */

} SCHEMA_DEF;

/*
 * Current_schema
 *
 * description:
 *    This is the current schema.  The schema name in this structure is the
 *    default qualifier for any class/vclass names which are not
 *    explicitly qualified.
 *    This structure should only be changed with sc_set_current_schema which
 *    currently is called only from AU_SET_USER
 */

static SCHEMA_DEF Current_Schema = { {'\0'}, NULL, NULL, NULL, NULL };





#define WC_PERIOD L'.'

/* various states of a domain comparison. */
typedef enum
{

  DC_INCOMPATIBLE,
  DC_EQUAL,
  DC_LESS_SPECIFIC,
  DC_MORE_SPECIFIC
} DOMAIN_COMP;

/*
 *    Structure used internally during the flattening of a class
 *    hierarchy.  This information could be folded in with the
 *    class structure definitions but its only used once
 *    and makes it less confusing for the flattener.
 *    For each attribute in a class hierarchy, a candidate
 *    structure will be built during flattening.
 */
typedef struct sm_candidate SM_CANDIDATE;

struct sm_candidate
{
  struct sm_candidate *next;

  const char *name;
  MOP source;
  SM_ATTRIBUTE *att;		/* actual component structure */
  int order;
};

#if 0
#include <nlist.h>
#endif

/*
 *    This is the root of the currently active attribute descriptors.
 *    These are kept on a list so we can quickly invalidate them during
 *    significant but unusual events like schema changes.
 *    This list is generally short.  If it can be long, consider a
 *    doubly linked list for faster removal.
*/

#if defined (ENABLE_UNUSED_FUNCTION)
SM_DESCRIPTOR *sm_Descriptors = NULL;
#endif

/* ROOT_CLASS GLOBALS */
/* Global root class structure */
ROOT_CLASS sm_Root_class;

/* Global MOP for the root class object.  Used by the locator */
MOP sm_Root_class_mop = NULL;

/* Name of the root class */
const char *sm_Root_class_name = ROOTCLASS_NAME;

/* Heap file identifier for the root class */
HFID *sm_Root_class_hfid = &sm_Root_class.header.heap;

static unsigned int local_schema_version = 0;
static unsigned int global_schema_version = 0;

#if !defined (SERVER_MODE)
static int domain_search (MOP dclass_mop, MOP class_mop);
#endif
static int find_attribute_op (MOP op, const char *name,
			      SM_CLASS ** classp, SM_ATTRIBUTE ** attp);
#if defined (ENABLE_UNUSED_FUNCTION)
static int fetch_descriptor_class (MOP op, SM_DESCRIPTOR * desc,
				   int for_update, SM_CLASS ** class_);
#endif

static const char *template_classname (SM_TEMPLATE * template_);
#if defined (ENABLE_UNUSED_FUNCTION)
static const char *candidate_source_name (SM_TEMPLATE * template_,
					  SM_CANDIDATE * candidate);
#endif
static SM_CANDIDATE *make_candidate_from_attribute (SM_ATTRIBUTE * att,
						    MOP source);
static void free_candidates (SM_CANDIDATE * candidates);
static SM_CANDIDATE *prune_candidate (SM_CANDIDATE ** clist_pointer);
static void add_candidate (SM_CANDIDATE ** candlist, SM_ATTRIBUTE * comp,
			   int order, MOP source);
static SM_ATTRIBUTE *make_attribute_from_candidate (MOP classop,
						    SM_CANDIDATE * cand);
static SM_CANDIDATE *get_candidates (SM_TEMPLATE * def, SM_TEMPLATE * flat);
static int resolve_candidates (SM_TEMPLATE * template_,
			       SM_CANDIDATE * candidates,
			       SM_CANDIDATE ** winner_return);
static void insert_attribute (SM_ATTRIBUTE ** attlist, SM_ATTRIBUTE * att);
static int flatten_components (SM_TEMPLATE * def, SM_TEMPLATE * flat);
static int flatten_query_spec_lists (SM_TEMPLATE * def, SM_TEMPLATE * flat);
#if defined (ENABLE_UNUSED_FUNCTION)
static SM_ATTRIBUTE *find_matching_att (SM_ATTRIBUTE * list,
					SM_ATTRIBUTE * att, int idmatch);
static int retain_former_ids (SM_TEMPLATE * flat);
#endif
static int flatten_properties (SM_TEMPLATE * def, SM_TEMPLATE * flat);
static int flatten_template (SM_TEMPLATE * def, MOP deleted_class,
			     SM_TEMPLATE ** flatp);
static SM_ATTRIBUTE *order_atts_by_alignment (SM_ATTRIBUTE * atts);
static int build_storage_order (SM_CLASS * class_, SM_TEMPLATE * flat);
static void fixup_component_classes (MOP classop, SM_TEMPLATE * flat);
static void fixup_self_domain (TP_DOMAIN * domain, MOP self);
static void fixup_attribute_self_domain (SM_ATTRIBUTE * att, MOP self);
static void fixup_self_reference_domains (MOP classop, SM_TEMPLATE * flat);
static int allocate_index (MOP classop, SM_CLASS * class_,
			   SM_CLASS_CONSTRAINT * constraint);
static int deallocate_index (SM_CLASS_CONSTRAINT * cons, BTID * index);
static int allocate_disk_structures_index (MOP classop, SM_CLASS * class_,
					   SM_CLASS_CONSTRAINT * con);
static int allocate_disk_structures (MOP classop, SM_CLASS * class_,
				     int *num_allocated_index);
static int load_index_data (MOP classop, SM_CLASS * class_);
static int transfer_disk_structures (MOP classop, SM_CLASS * class_,
				     SM_TEMPLATE * flat);
static int install_new_representation (MOP classop, SM_CLASS * class_,
				       SM_TEMPLATE * flat);
static int check_catalog_space (MOP classmop, SM_CLASS * class_);
static int update_class (SM_TEMPLATE * template_, MOP * classmop);
#if defined(ENABLE_UNUSED_FUNCTION)
static int sm_exist_index (MOP classop, const char *idxname, BTID * btid);
#endif
static char *sm_default_constraint_name (const char *class_name,
					 DB_CONSTRAINT_TYPE type,
					 const char **att_names,
					 const int *asc_desc);

#if defined(ENABLE_UNUSED_FUNCTION)
static DB_OBJLIST *sm_get_all_objects (DB_OBJECT * op);
static int sm_check_index_exist (MOP classop,
				 DB_CONSTRAINT_TYPE constraint_type,
				 const char *constraint_name);

static void sm_reset_descriptors (MOP class_);
#endif

#if defined(RYE_DEBUG)
static void sm_print (MOP classmop);
#endif

#if defined(ENABLE_UNUSED_FUNCTION)
static DB_OBJLIST *sm_query_lock (MOP classop, DB_OBJLIST * exceptions,
				  int only, int update);
static DB_OBJLIST *sm_get_all_classes (int external_list);
static DB_OBJLIST *sm_get_base_classes (int external_list);
static const char *sm_get_class_name_internal (MOP op, bool return_null);
static const char *sm_get_class_name (MOP op);
static const char *sm_get_class_name_not_null (MOP op);
static const char *sc_current_schema_name (void);
static int sm_object_disk_size (MOP op);
static int sm_has_constraint (MOBJ classobj, SM_ATTRIBUTE_FLAG constraint);
static int sm_get_att_domain (MOP op, const char *name, TP_DOMAIN ** domain);
static const char *sm_type_name (DB_TYPE id);
#endif
/*
 * sc_set_current_schema()
 *      return: NO_ERROR if successful
 *              ER_FAILED if any problem extracting name from authorization
 *
 *  user(in) : MOP for authorization (user)
 *
 * Note :
 *    This function is temporary kludge to allow initial implementation
 *    of schema names.  It is to be called from just one place: AU_SET_USER.
 *    Entry level SQL specifies that a schema name is equal to the
 *    <authorization user name>, so this function simply extracts the user
 *    name from the input argument, makes it lower case, and uses that name
 *    as the schema name.
 *
 *
 */

int
sc_set_current_schema (MOP user)
{
  int error = ER_FAILED;
  char *wsp_user_name;

  Current_Schema.name[0] = '\0';
  Current_Schema.owner = user;
  wsp_user_name = au_get_user_name (user);

  if (wsp_user_name == NULL)
    {
      return error;
    }

  /* As near as I can tell, this is the most generalized  */
  /* case conversion function on our system.  If it's not */
  /* the most general, change this code accordingly.      */
  if (intl_identifier_lower (wsp_user_name, Current_Schema.name) == 0)
    {
      /* intl_identifier_lower always returns 0.      */
      /* However, someday it might return an error.            */
      error = NO_ERROR;
    }
  ws_free_string (wsp_user_name);

  /* If there's any error, it's not obvious what can be done about it here. */
  /* Probably some code needs to be fixed in the caller: AU_SET_USER        */
  return error;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * sc_current_schema_name() - Returns current schema name which is
 *                            the default qualifier for otherwise
 *                            unqualified class/vclass names
 *      return: pointer to current schema name
 *
 */

static const char *
sc_current_schema_name (void)
{
  return (const char *) &(Current_Schema.name);
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * sm_init() - Called during database restart.
 *    Setup the global variables that contain the root class OID and HFID.
 *    Also initialize the descriptor list
 *   return: none
 *   rootclass_oid(in): OID of root class
 *   rootclass_hfid(in): heap file of root class
 */

void
sm_init (OID * rootclass_oid, HFID * rootclass_hfid)
{

  sm_Root_class_mop = ws_mop (rootclass_oid, NULL);
  oid_Root_class_oid = ws_oid (sm_Root_class_mop);

  sm_Root_class.header.heap.vfid.volid = rootclass_hfid->vfid.volid;
  sm_Root_class.header.heap.vfid.fileid = rootclass_hfid->vfid.fileid;
  sm_Root_class.header.heap.hpgid = rootclass_hfid->hpgid;

  sm_Root_class_hfid = &sm_Root_class.header.heap;

#if defined (ENABLE_UNUSED_FUNCTION)
  sm_Descriptors = NULL;
#endif
}

/*
 * sm_create_root() - Called when the database is first created.
 *    Sets up the root class globals, used later when the root class
 *    is flushed to disk
 *   return: none
 *   rootclass_oid(in): OID of root class
 *   rootclass_hfid(in): heap file of root class
 */

void
sm_create_root (OID * rootclass_oid, HFID * rootclass_hfid)
{
  sm_Root_class.header.type = Meta_root;
  sm_Root_class.header.name = (char *) sm_Root_class_name;

  sm_Root_class.header.heap.vfid.volid = rootclass_hfid->vfid.volid;
  sm_Root_class.header.heap.vfid.fileid = rootclass_hfid->vfid.fileid;
  sm_Root_class.header.heap.hpgid = rootclass_hfid->hpgid;
  sm_Root_class_hfid = &sm_Root_class.header.heap;

  /* Sets up sm_Root_class_mop and Rootclass_oid */
  locator_add_root (rootclass_oid, (MOBJ) (&sm_Root_class));
}


/*
 * sm_final() - Called during the shutdown sequence
 */

void
sm_final ()
{
#if defined (ENABLE_UNUSED_FUNCTION)
  SM_DESCRIPTOR *d, *next;
#endif
  SM_CLASS *class_;
  DB_OBJLIST *cl;

#if defined (ENABLE_UNUSED_FUNCTION)
  /* If there are any remaining descriptors it represents a memory leak
     in the application. Should be displaying warning messages here !
   */

  for (d = sm_Descriptors, next = NULL; d != NULL; d = next)
    {
      next = d->next;
      sm_free_descriptor (d);
    }
#endif

  /* go through the resident class list and free anything attached
     to the class that wasn't allocated in the workspace, this is
     only the virtual_query_cache at this time */
  for (cl = ws_Resident_classes; cl != NULL; cl = cl->next)
    {
      class_ = (SM_CLASS *) cl->op->object;
      if (class_ != NULL && class_->virtual_query_cache != NULL)
	{
	  mq_free_virtual_query_cache (class_->virtual_query_cache);
	  class_->virtual_query_cache = NULL;
	}
    }
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * sm_transaction_boundary() - This is called by tm_commit() and tm_abort()
 *    to inform the schema manager that a transaction boundary has been crossed.
 *    If the commit-flag is non-zero it indicates that we've committed
 *    the transaction.
 *    We used to call sm_bump_local_schema_version directly from the tm_ functions.
 *    Now that we have more than one thing to do however, start
 *    encapsulating them in a module specific transaction boundary handler
 *    so we don't have to keep modifying transaction_cl.c
 */

void
sm_transaction_boundary (void)
{
#if defined(ENABLE_UNUSED_FUNCTION)
  /* reset any outstanding descriptor caches */
  sm_reset_descriptors (NULL);
#endif

  /* Could be resetting the transaction caches in each class too
     but the workspace is controlling that */
}
#endif

/* UTILITY FUNCTIONS */
/*
 * sm_check_name() - This is made void for ANSI compatibility.
 *      It previously insured that identifiers which were accepted could be
 *      parsed in the language interface.
 *
 *  	ANSI allows any character in an identifier. It also allows reserved
 *  	words. In order to parse identifiers with non-alpha characters
 *  	or that are reserved words, an escape syntax is defined. See the lexer
 *      tokens DELIMITED_ID_NAME, BRACKET_ID_NAME and BACKTICK_ID_NAME for
 *      details on the escaping rules.
 *   return: non-zero if name is ok
 *   name(in): name to check
 */

int
sm_check_name (const char *name)
{
  if (name == NULL || name[0] == '\0')
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_SM_INVALID_NAME, 1,
	      name);
      return 0;
    }
  else
    {
      return 1;
    }
}

/*
 * sm_downcase_name() - This is a kludge to make sure that class names are
 *    always converted to lower case in the API.
 *    This conversion is already done by the parser so we must be consistent.
 *    This is necessarily largely because the eh_ module on the server does not
 *    offer a mode for case insensitive string comparison.
 *    Is there a system function that does this? I couldn't find one
 *   return: none
 *   name(in): class name
 *   buf(out): output buffer
 *   maxlen(in): maximum buffer length
 */

void
sm_downcase_name (const char *name, char *buf, UNUSED_ARG int maxlen)
{
  int name_size;

  name_size = strlen (name);
  /* the sizes of lower and upper version of an identifier are checked when
   * entering the system */
  assert (name_size < maxlen);

  intl_identifier_lower (name, buf);
}

/* CLASS LOCATION FUNCTIONS */
/*
 * sm_get_class() - This is just a convenience function used to
 *    return a class MOP for any possible MOP.
 *    If this is a class mop return it, if this is  an object MOP,
 *    fetch the class and return its mop.
 *   return: class mop
 *   obj(in): object or class mop
 */

MOP
sm_get_class (MOP obj)
{
  MOP op = NULL;

  if (obj != NULL)
    {
      if (locator_is_class (obj))
	{
	  op = obj;
	}
      else
	{
	  if (obj->class_mop == NULL)
	    {
#if 1				/* TODO - trace */
	      assert (false);
#endif
	      /* force class load through object load */
	      (void) au_fetch_class (obj, NULL, S_LOCK, AU_SELECT);
	    }
	  op = obj->class_mop;
	}
    }

  return op;
}

/*
 * sm_fetch_all_classes() - Fetch all classes for a given purpose.
 *    Builds a list of all classes in the system.  Be careful to filter
 *    out the root class since it isn't really a class to the callers.
 *    The external_list flag is set when the object list is to be returned
 *    above the database interface layer (db_ functions) and must therefore
 *    be allocated in storage that will serve as roots to the garbage
 *    collector.
 *   return: object list of class MOPs
 *   purpose(in): Fetch purpose
 */
DB_OBJLIST *
sm_fetch_all_classes (LOCK lock)
{
  LIST_MOPS *lmops;
  DB_OBJLIST *objects, *last, *new_;
  int i;

  assert (lock == S_LOCK);

  objects = NULL;
  lmops = NULL;

  if (au_check_user () == NO_ERROR)
    {				/* make sure we have a user */
      last = NULL;
      lmops = locator_get_all_mops (sm_Root_class_mop, lock);
      /* probably should make sure we push here because the list could be long */
      if (lmops != NULL)
	{
	  for (i = 0; i < lmops->num; i++)
	    {
	      /* is it necessary to have this check ? */
	      if (!WS_MARKED_DELETED (lmops->mops[i])
		  && lmops->mops[i] != sm_Root_class_mop)
		{
		  /* should have a ext_ append function */
		  new_ = ml_ext_alloc_link ();
		  if (new_ == NULL)
		    {
		      goto memory_error;
		    }
		  new_->op = lmops->mops[i];
		  new_->next = NULL;
		  if (last != NULL)
		    {
		      last->next = new_;
		    }
		  else
		    {
		      objects = new_;
		    }
		  last = new_;
		}
	    }
	  locator_free_list_mops (lmops);
	}
    }

  return objects;

memory_error:
  if (lmops != NULL)
    {
      locator_free_list_mops (lmops);
    }

  ml_ext_free (objects);


  return NULL;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * sm_get_all_classes() -  Builds a list of all classes in the system.
 *    Be careful to filter out the root class since it isn't really a class
 *    to the callers. The external_list flag is set when the object list is
 *    to be returned above the database interface layer (db_ functions) and
 *    must therefore be allocated in storage that will serve as roots to
 *    the garbage collector.
 *    Authorization checking is not performed at this level so there may be
 *    MOPs in the list that you can't actually access.
 *   return: object list of class MOPs
 *   external_list(in): non-zero if external list links are to be used
 */

static DB_OBJLIST *
sm_get_all_classes (int external_list)
{
  /* Lock all the classes in shared mode */
  return sm_fetch_all_classes (external_list, DB_FETCH_QUERY_READ);
}				/* sm_get_all_classes */

/*
 * sm_get_base_classes() - Returns a list of classes that have no super classes
 *   return: list of class MOPs
 *   external_list(in): non-zero to create external MOP list
*/
static DB_OBJLIST *
sm_get_base_classes (int external_list)
{
  /* Lock all the classes in shared mode */
  return sm_fetch_all_base_classes (external_list, DB_FETCH_QUERY_READ);
}
#endif

/* OBJECT LOCATION */
/*
 * sm_get_all_objects() - Returns a list of all the instances that have
 *    been created for a class.
 *    This was used early on before query was available, it should not
 *    be heavily used now.  Be careful, this can potentially bring
 *    in lots of objects and overflow the workspace.
 *    This is used in the implementation of a db_ function so it must
 *    allocate an external mop list !
 *   return: list of objects
 *   op(in): class or instance object
 *   purpose(in): Fetch purpose
 */

DB_OBJLIST *
sm_fetch_all_objects (DB_OBJECT * op, LOCK lock)
{
  LIST_MOPS *lmops;
  SM_CLASS *class_;
  DB_OBJLIST *objects, *new_;
  MOP classmop;
  SM_CLASS_TYPE ct;
  int i;

  objects = NULL;
  classmop = NULL;
  lmops = NULL;

  if (op != NULL)
    {
      if (locator_is_class (op))
	{
	  classmop = op;
	}
      else
	{
	  if (op->class_mop == NULL)
	    {
#if 1				/* TODO - trace */
	      assert (false);
#endif
	      /* force load */
	      (void) au_fetch_class (op, &class_, S_LOCK, AU_SELECT);
	    }
	  classmop = op->class_mop;
	}
      if (classmop != NULL)
	{
	  class_ = (SM_CLASS *) classmop->object;
	  if (!class_)
	    {
	      (void) au_fetch_class (classmop, &class_, S_LOCK, AU_SELECT);
	    }
	  if (!class_)
	    {
	      return NULL;
	    }

	  ct = sm_get_class_type (class_);
	  if (ct == SM_CLASS_CT)
	    {
	      lmops = locator_get_all_mops (classmop, lock);
	      if (lmops != NULL)
		{
		  for (i = 0; i < lmops->num; i++)
		    {
		      /* is it necessary to have this check ? */
		      if (!WS_MARKED_DELETED (lmops->mops[i]))
			{
			  new_ = ml_ext_alloc_link ();
			  if (new_ == NULL)
			    {
			      goto memory_error;
			    }

			  new_->op = lmops->mops[i];
			  new_->next = objects;
			  objects = new_;
			}
		    }
		  locator_free_list_mops (lmops);
		}
	    }
	}
    }

  return objects;

memory_error:
  if (lmops != NULL)
    {
      locator_free_list_mops (lmops);
    }

  ml_ext_free (objects);

  return NULL;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * sm_get_all_objects() - Returns a list of all the instances that
 *    have been created for a class.
 *    This was used early on before query was available, it should not
 *    be heavily used now.  Be careful, this can potentially bring
 *    in lots of objects and overflow the workspace.
 *    This is used in the implementation of a db_ function so it must
 *    allocate an external mop list !
 *   return: list of objects
 *   op(in): class or instance object
 */

static DB_OBJLIST *
sm_get_all_objects (DB_OBJECT * op)
{
  return sm_fetch_all_objects (op, DB_FETCH_QUERY_READ);
}
#endif

/* MISC SCHEMA OPERATIONS */
/*
 * sm_rename_class() - This is used to change the name of a class if possible.
 *    It is not part of the smt_ template layer because its a fairly
 *    fundamental change that must be checked early.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   op(in/out): class mop
 *   new_name(in):
 */

int
sm_rename_class (MOP op, const char *new_name)
{
  int error;
  SM_CLASS *class_;
  const char *current, *newname;
  char realname[SM_MAX_IDENTIFIER_LENGTH];

  /* make sure this gets into the server table with no capitalization */
  sm_downcase_name (new_name, realname, SM_MAX_IDENTIFIER_LENGTH);

#if defined (ENABLE_UNUSED_FUNCTION)
  if (sm_has_text_domain (db_get_attributes (op), 1))
    {
      /* prevent to rename class */
      ERROR1 (error, ER_REGU_NOT_IMPLEMENTED, rel_version_string ());
      return error;
    }
#endif /* ENABLE_UNUSED_FUNCTION */

  if (!sm_check_name (realname))
    {
      error = er_errid ();
    }
  else if ((error = au_fetch_class (op, &class_, X_LOCK, AU_ALTER))
	   == NO_ERROR)
    {
      /*  We need to go ahead and copy the string since prepare_rename uses
       *  the address of the string in the hash table.
       */
      current = class_->header.name;
      newname = ws_copy_string (realname);
      if (newname == NULL)
	{
	  return er_errid ();
	}

      if (locator_prepare_rename_class (op, current, newname) == NULL)
	{
	  ws_free_string (newname);
	  error = er_errid ();
	}
      else
	{
	  class_->header.name = newname;
#if 1				/* TODO - remove me someday */
	  error = sm_flush_objects (op);
#endif

	  ws_free_string (current);
	}
    }

  return error;
}

/*
 * sm_mark_system_classes() - Hack used to set the "system class" flag for
 *    all currently resident classes.
 *    This is only to make it more convenient to tell the
 *    difference between Rye and user defined classes.  This is intended
 *    to be called after the appropriate Rye class initialization function.
 *    Note that authorization is disabled here because these are normally
 *    called on the authorization classes.
 */

void
sm_mark_system_classes (void)
{
  LIST_MOPS *lmops;
  int i;

  if (au_check_user () == NO_ERROR)
    {
      lmops = locator_get_all_mops (sm_Root_class_mop, S_LOCK);
      if (lmops != NULL)
	{
	  for (i = 0; i < lmops->num; i++)
	    {
	      if (!WS_MARKED_DELETED (lmops->mops[i]) && lmops->mops[i]
		  != sm_Root_class_mop)
		{
		  (void) sm_mark_system_class (lmops->mops[i], 1);
		}
	    }
	  locator_free_list_mops (lmops);
	}
    }
}

/*
 * sm_mark_system_class() - This turns on or off the system class flag.
 *   This flag is tested by the sm_is_system_table function.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop (in): class pointer
 *   on_or_off(in): state of the flag
 */

int
sm_mark_system_class (MOP classop, int on_or_off)
{
  return sm_set_class_flag (classop, SM_CLASSFLAG_SYSTEM, on_or_off);
}

/*
 * sm_set_class_flag() - This turns on or off the given flag.
 *    The flag may be tested by the sm_get_class_flag function.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop (in): class pointer
 *   flag  (in): flag to set or clear
 *   on_or_off(in): 1 to set 0 to clear
 */

int
sm_set_class_flag (MOP classop, SM_CLASS_FLAG flag, int on_or_off)
{
  SM_CLASS *class_;
  int error = NO_ERROR;

  if (classop != NULL)
    {
      error = au_fetch_class_force (classop, &class_, X_LOCK);
      if (error == NO_ERROR)
	{
	  if (on_or_off)
	    {
	      class_->flags |= flag;
	    }
	  else
	    {
	      class_->flags &= ~flag;
	    }
	}
    }

  return error;
}

/*
 * sm_is_system_table() - Tests the system class flag of a class object.
 *   return: non-zero if class is a system defined class
 *   op(in): class object
 */

int
sm_is_system_table (MOP op)
{
  return sm_get_class_flag (op, SM_CLASSFLAG_SYSTEM);
}

/*
 * sm_is_shard_table() - Tests the shard table flag of a class object.
 *   return: true if class is an shard table. otherwise, false
 *   op(in): class object
 */
bool
sm_is_shard_table (MOP op)
{
  return sm_get_class_flag (op, SM_CLASSFLAG_SHARD_TABLE);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * sm_shard_key_col_name() - Returns the name of a shard key column.
 *    Authorization is ignored for this one case.
 *   return: column name
 *   op(in): class object
 */
const char *
sm_shard_key_col_name (MOP op)
{
  SM_CLASS *class_;
  SM_ATTRIBUTE *key_attr;
  const char *name = NULL;

  if (op != NULL)
    {
      if (au_fetch_class_force (op, &class_, S_LOCK) == NO_ERROR)
	{
	  key_attr = classobj_find_shard_key_column (class_);
	  name = key_attr->name;
	  assert (name == NULL || strlen (name) >= 0);
	}
    }

  return name;
}
#endif

/*
 * sm_get_class_flag() - Tests the class flag of a class object.
 *   return: non-zero if flag set
 *   op(in): class object
 *   flag(in): flag to test
 */

int
sm_get_class_flag (MOP op, SM_CLASS_FLAG flag)
{
  SM_CLASS *class_;
  int result = 0;

  if (op != NULL && locator_is_class (op))
    {
      if (au_fetch_class_force (op, &class_, S_LOCK) == NO_ERROR)
	{
	  result = class_->flags & flag;
	}
    }

  return result;
}



/*
 * sm_force_write_all_classes()
 *   return: NO_ERROR on success, non-zero for ERROR
 */

int
sm_force_write_all_classes (void)
{
  LIST_MOPS *lmops;
  int i;

  /* get all class objects */
  lmops = locator_get_all_mops (sm_Root_class_mop, S_LOCK);
  if (lmops != NULL)
    {
      for (i = 0; i < lmops->num; i++)
	{
	  ws_dirty (lmops->mops[i]);
	}

      /* insert all class objects into the catalog classes */
      if (locator_flush_all_instances (sm_Root_class_mop, DONT_DECACHE)
	  != NO_ERROR)
	{
	  return er_errid ();
	}

      for (i = 0; i < lmops->num; i++)
	{
	  ws_dirty (lmops->mops[i]);
	}

      /* update class hierarchy values for some class objects.
       * the hierarchy makes class/class mutual references
       * so some class objects were inserted with no hierarchy values.
       */
      if (locator_flush_all_instances (sm_Root_class_mop, DONT_DECACHE)
	  != NO_ERROR)
	{
	  return er_errid ();
	}

      locator_free_list_mops (lmops);
    }

  return NO_ERROR;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * sm_destroy_representations() - This is called by the compaction utility
 *    after it has swept through the instances of a class and converted them
 *    all to the latest representation.
 *    Once this is done, the schema manager no longer needs to maintain
 *    the history of old representations. In order for this to become
 *    persistent, the transaction must be committed.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   op(in): class object
 */

int
sm_destroy_representations (MOP op)
{
  int error = NO_ERROR;
  SM_CLASS *class_;

  error = au_fetch_class_force (op, &class_, X_LOCK);
  if (error == NO_ERROR)
    {
      WS_LIST_FREE (class_->representations, classobj_free_representation);
      class_->representations = NULL;
    }

  return error;
}
#endif

/* DOMAIN MAINTENANCE FUNCTIONS */

/*
 * sm_filter_domain() - This removes any invalid domain references from a
 *    domain list.  See description of filter_domain_list for more details.
 *    If the domain list was changed, we could get a write lock on the owning
 *    class to ensure that the change is made persistent.
 *    Making the change persistent doesn't really improve much since we
 *    always have to do a filter pass when the class is fetched.
 *   return: non-zero if changes were made
 *   domain(in): domain list for attribute
 */

int
sm_filter_domain (TP_DOMAIN * domain)
{
  int changes = 0;

  if (domain != NULL)
    {
      changes = tp_domain_filter_list (domain);
      /* if changes, could get write lock on owning_class here */
    }

  return changes;
}

#if !defined (SERVER_MODE)
/*
 * domain_search() - This recursively searches through the class hierarchy
 *    to see if the "class_mop" is equal to or a subclass of "dclass_mop"
 *    in which case it is within the domain of dlcass_mop.
 *    This is essentially the same as sm_is_superclass except that it
 *    doesn't check for authorization.
 *   return: non-zero if the class was valid
 *   dclass_mop(in): domain class
 *   class_mop(in): class in question
 */

static int
domain_search (MOP dclass_mop, MOP class_mop)
{
  int ok = 0;

#if 1				/* TODO - trace */
  assert (false);
#endif

  if (dclass_mop == class_mop)
    {
      ok = 1;
    }

  return ok;
}

/*
 * sm_check_object_domain() - This checks to see if an instance is valid for
 *    a given domain. It checks to see if the instance's class is equal to or
 *    a subclass of the class in the domain.  Also handles the various NULL
 *    conditions.
 *   return: non-zero if object is within the domain
 *   domain(in): domain to examine
 *   object(in): instance
 */

int
sm_check_object_domain (TP_DOMAIN * domain, MOP object)
{
  int ok;

#if 1				/* TODO - trace */
  assert (false);
#endif

  ok = 0;
  if (domain->type == tp_Type_object)
    {
      /* check for physical and logical NULLness of the MOP, treat it
         as if it were SQL NULL which is allowed in all domains */
      if (WS_MOP_IS_NULL (object))
	{
	  ok = 1;
	}
      /* check for the wildcard object domain */
      else if (domain->class_mop == NULL)
	{
	  ok = 1;
	}
      else
	{
	  /* fetch the class if it hasn't been cached, should this be a write
	     lock ?  don't need to pin, only forcing the class fetch
	   */
	  if (object->class_mop == NULL)
	    {
	      au_fetch_instance (object, NULL, S_LOCK, AU_SELECT);
	    }

	  /* if its still NULL, assume an authorization error and go on */
	  if (object->class_mop != NULL)
	    {
	      ok = domain_search (domain->class_mop, object->class_mop);
	    }
	}
    }

  return ok;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * sm_coerce_object_domain() - This checks to see if an instance is valid for
 *    a given domain.
 *    It checks to see if the instance's class is equal to or a subclass
 *    of the class in the domain.  Also handles the various NULL
 *    conditions.
 *    If dest_object is not NULL and the object is a view on a real object,
 *    the real object will be returned.
 *   return: non-zero if object is within the domain
 *   domain(in): domain to examine
 *   object(in): instance
 *   dest_object(out): ptr to instance to coerce object to
 */

int
sm_coerce_object_domain (TP_DOMAIN * domain, MOP object, MOP * dest_object)
{
  int ok;
  SM_CLASS *class_;

  ok = 0;

  if (!dest_object)
    {
      return 0;
    }

  if (domain->type == tp_Type_object)
    {
      /* check for physical and logical NULLness of the MOP, treat it
         as if it were SQL NULL which is allowed in all domains */
      if (WS_MOP_IS_NULL (object))
	{
	  ok = 1;
	}
      /* check for the wildcard object domain */
      else if (domain->class_mop == NULL)
	{
	  ok = 1;
	}
      else
	{
	  /* fetch the class if it hasn't been cached, should this be a write lock ?
	     don't need to pin, only forcing the class fetch
	   */
	  if (object->class_mop == NULL)
	    {
	      au_fetch_instance (object, NULL, S_LOCK, AU_SELECT);
	    }

	  /* if its still NULL, assume an authorization error and go on */
	  if (object->class_mop != NULL)
	    {
	      if (domain->class_mop == object->class_mop)
		{
		  ok = 1;
		}
	      else
		{
		  if (au_fetch_class_force (object->class_mop, &class_,
					    S_LOCK) == NO_ERROR)
		    {
		      /* Coerce a view to a real class. */
		      if (class_->class_type == SM_VCLASS_CT)
			{
			  assert (false);
			  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
				  ER_SM_INVALID_ARGUMENTS, 0);
			  return 0;
			}

		      ok = domain_search (domain->class_mop,
					  object->class_mop);
		    }
		}
	    }
	}
    }

  if (ok)
    {
      *dest_object = object;
    }

  return ok;
}

/*
 * sm_check_class_domain() - see if a class is within the domain.
 *    It is similar to sm_check_object_domain except that we get
 *    a pointer directly to the class and we don't allow NULL conditions.
 *   return: non-zero if the class is within the domain
 *   domain(in): domain to examine
 *   class(in): class to look for
 */

int
sm_check_class_domain (TP_DOMAIN * domain, MOP class_)
{
  int ok = 0;

  if (domain->type == tp_Type_object && class_ != NULL)
    {
      /* check for domain class deletions and other delayed updates
         SINCE THIS IS CALLED FOR EVERY ATTRIBUTE UPDATE, WE MUST EITHER
         CACHE THIS INFORMATION OR PERFORM IT ONCE WHEN THE CLASS
         IS FETCHED */
      (void) sm_filter_domain (domain);

      /* wildcard case */
      if (domain->class_mop == NULL)
	{
	  ok = 1;
	}
      else
	{
	  /* recursively check domains for class & super classes
	     for now assume only one possible base class */
	  ok = domain_search (domain->class_mop, class_);
	}
    }

  return ok;
}
#endif /* ENABLE_UNUSED_FUNCTION */

#endif

/*
 * sm_clean_class() - used mainly before constructing a class template but
 *    it could be used in other places as well.  It will walk through the
 *    class structure and prune out any references to deleted objects
 *    in domain lists, etc. and do any other housekeeping tasks that it is
 *    convenient to delay until a major operation is performed.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classmop(in):
 *   class(in/out): class structure
 */

int
sm_clean_class (UNUSED_ARG MOP classmop, SM_CLASS * class_)
{
  int error = NO_ERROR;
  SM_ATTRIBUTE *att;

  /* we only need to do this once because once we have read locks,
     the referenced classes can't be deleted */

  for (att = class_->attributes; att != NULL; att = att->next)
    {
      assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

      sm_filter_domain (att->sma_domain);
    }

  if (!class_->post_load_cleanup)
    {
      class_->post_load_cleanup = 1;
    }

  return error;
}

/* CLASS STATISTICS FUNCTIONS */
/*
 * sm_get_class_with_statistics() - Fetches and returns the statistics information for a
 *    class from the system catalog on the server.
 *    Must make sure callers keep the class MOP visible to the garbage
 *    collector so the stat structures don't get reclaimed.
 *    Currently used only by the query optimizer.
 *   return: class object which contains statistics structure
 *   classop(in): class object
 */

SM_CLASS *
sm_get_class_with_statistics (MOP classop)
{
  SM_CLASS *class_ = NULL;

  /* only try to get statistics if we know the class has been flushed
     if it has a temporary oid, it isn't flushed and there are no statistics */

  if (classop != NULL
      && locator_is_class (classop) && !OID_ISTEMP (WS_OID (classop)))
    {
      if (au_fetch_class (classop, &class_, S_LOCK, AU_SELECT) == NO_ERROR)
	{
	  if (class_->stats == NULL)
	    {
	      /* it's first time to get the statistics of this class */
	      if (!OID_ISTEMP (WS_OID (classop)))
		{
		  /* make sure the class is flushed before asking for statistics,
		     this handles the case where an index has been added to the class
		     but the catalog & statistics do not reflect this fact until
		     the class is flushed.  We might want to flush instances
		     as well but that shouldn't affect the statistics ? */
		  if (locator_flush_class (classop) != NO_ERROR)
		    {
		      return NULL;
		    }
		  class_->stats = stats_get_statistics (WS_OID (classop), 0);
		}
	    }
	  else
	    {
	      CLASS_STATS *stats;

	      /* to get the statistics to be updated, it send timestamp
	         as uninitialized value */
	      stats = stats_get_statistics (WS_OID (classop),
					    class_->stats->time_stamp);
	      /* if newly updated statistics are fetched, replace the old one */
	      if (stats)
		{
		  stats_free_statistics (class_->stats);
		  class_->stats = stats;
		}
	    }
	}
    }

  return class_;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * sm_get_statistics_force()
 *   return: class statistics
 *   classop(in):
 */
CLASS_STATS *
sm_get_statistics_force (MOP classop)
{
  SM_CLASS *class_;
  CLASS_STATS *stats = NULL;

  if (classop != NULL
      && locator_is_class (classop) && !OID_ISTEMP (WS_OID (classop)))
    {
      if (au_fetch_class (classop, &class_, S_LOCK, AU_SELECT) == NO_ERROR)
	{
	  if (class_->stats)
	    {
	      stats_free_statistics (class_->stats);
	      class_->stats = NULL;
	    }
	  stats = class_->stats = stats_get_statistics (WS_OID (classop), 0);
	}
    }

  return stats;
}
#endif

/*
 * sm_update_statistics () - Update statistics on the server for the
 *    particular class or index. When finished, fetch the new statistics and
 *    cache them with the class.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 *   update_stats(in): true iff UPDATE
 *   with_fullscan(in): true iff WITH FULLSCAN
 *
 * NOTE: We will delay updating statistics until a transaction is committed
 *       when it is requested during other processing, such as
 *       "alter table ..." or "create index ...".
 */
int
sm_update_statistics (MOP classop, bool update_stats, bool with_fullscan)
{
  int error = NO_ERROR;
  SM_CLASS *class_ = NULL;

  assert_release (classop != NULL);

  /* only try to get statistics if we know the class has been flushed
     if it has a temporary oid, it isn't flushed and there are no statistics */

  if (classop != NULL && !OID_ISTEMP (WS_OID (classop))
      && locator_is_class (classop))
    {

      /* make sure the workspace is flushed before calculating stats */
      if (locator_flush_all_instances (classop, DONT_DECACHE) != NO_ERROR)
	{
	  return er_errid ();
	}

      error = stats_update_statistics (WS_OID (classop),
				       (update_stats ? 1 : 0),
				       (with_fullscan ? 1 : 0));
      if (error == NO_ERROR)
	{
	  /* only recache if the class itself is cached */
	  if (classop->object != NULL)
	    {			/* check cache */
	      /* why are we checking authorization here ? */
	      error = au_fetch_class_force (classop, &class_, S_LOCK);
	      if (error == NO_ERROR)
		{
		  if (class_->stats != NULL)
		    {
		      stats_free_statistics (class_->stats);
		      class_->stats = NULL;
		    }

		  /* make sure the class is flushed before acquiring stats,
		     see comments above in sm_get_class_with_statistics */
		  if (locator_flush_class (classop) != NO_ERROR)
		    {
		      return (er_errid ());
		    }

		  /* get the new ones, should do this at the same time as the
		     update operation to avoid two server calls */
		  class_->stats = stats_get_statistics (WS_OID (classop), 0);
		}
	    }
	}
    }

  return error;
}

/*
 * sm_update_all_statistics() - Update the statistics for all classes
 * 			        in the database.
 *   update_stats(in): true iff UPDATE
 *   with_fullscan(in): true iff WITH FULLSCAN
 *   return: NO_ERROR on success, non-zero for ERROR
 */

int
sm_update_all_statistics (bool update_stats, bool with_fullscan)
{
  int error = NO_ERROR;
  DB_OBJLIST *cl;

  /* make sure the workspace is flushed before calculating stats */
  if (locator_all_flush () != NO_ERROR)
    {
      return er_errid ();
    }

  error = stats_update_all_statistics ((update_stats ? 1 : 0),
				       (with_fullscan ? 1 : 0));
  if (error != NO_ERROR)
    {
      return error;
    }

  /* Need to reset the statistics cache for all resident classes */
  for (cl = ws_Resident_classes; cl != NULL; cl = cl->next)
    {
      if (!WS_ISMARK_DELETED (cl->op))
	{
	  (void) sm_update_statistics (cl->op, update_stats, with_fullscan);
	}
    }

  assert (error == NO_ERROR);

  return NO_ERROR;
}

/*
 * sm_update_all_catalog_statistics()
 *   return: NO_ERROR on success, non-zero for ERROR
 *   update_stats(in): true iff UPDATE
 *   with_fullscan(in): true iff WITH FULLSCAN
 */

int
sm_update_all_catalog_statistics (bool update_stats, bool with_fullscan)
{
  int error = NO_ERROR;
  int i;

  const char *classes[] = {
    CT_TABLE_NAME, CT_COLUMN_NAME, CT_DOMAIN_NAME,
    CT_QUERYSPEC_NAME, CT_INDEX_NAME,
    CT_INDEXKEY_NAME, CT_DATATYPE_NAME,
    CT_AUTH_NAME,
    CT_COLLATION_NAME, NULL
  };

  for (i = 0; classes[i] != NULL && error == NO_ERROR; i++)
    {
      error =
	sm_update_catalog_statistics (classes[i], update_stats,
				      with_fullscan);
    }

  assert (classes[i] == NULL);

  return error;
}

/*
 * sm_update_catalog_statistics()
 *   return: NO_ERROR on success, non-zero for ERROR
 *   class_name(in):
 *   update_stats(in): true iff UPDATE
 *   with_fullscan(in): true iff WITH FULLSCAN
 */

int
sm_update_catalog_statistics (const char *class_name, bool update_stats,
			      bool with_fullscan)
{
  int error = NO_ERROR;
  DB_OBJECT *obj;

  obj = sm_find_class (class_name);
  if (obj != NULL)
    {
      error = sm_update_statistics (obj, update_stats, with_fullscan);
    }
  else
    {
      error = er_errid ();
    }

  return error;
}

/* MISC INFORMATION FUNCTIONS */
/*
 * sm_class_name() - Returns the name of a class associated with an object.
 *    If the object is a class, its own class name is returned.
 *    If the object is an instance, the name of the instance's class
 *    is returned.
 *    Authorization is ignored for this one case.
 *   return: class name
 *   op(in): class or instance object
 */

const char *
sm_class_name (MOP op)
{
  SM_CLASS *class_;
  const char *name = NULL;

  if (op != NULL)
    {
      if (au_fetch_class_force (op, &class_, S_LOCK) == NO_ERROR)
	{
	  name = class_->header.name;
	  assert (name == NULL || strlen (name) >= 0);
	}
    }

  return name;
}

/*
 * sm_object_size_quick() - Calculate the memory size of an instance.
 *    Called only by the workspace statistics functions.
 *    Like sm_object_size but doesn't do any fetches.
 *   return: byte size of instance
 *   class(in): class structure
 *   obj(in): pointer to instance memory
 */

int
sm_object_size_quick (SM_CLASS * class_, MOBJ obj)
{
  SM_ATTRIBUTE *att;
  int size = 0;

  if (class_ != NULL && obj != NULL)
    {
      size = class_->object_size;
      for (att = class_->attributes; att != (void *) 0; att = att->next)
	{
	  if (att->type->variable_p)
	    {
	      size += pr_total_mem_size (att->type, obj + att->offset);
	    }
	}
    }

  return size;
}

#if defined(RYE_DEBUG)
/*
 * sm_dump() - Debug function to dump internal information about class objects.
 *   return: none
 *   classmop(in): class object
 */
static void
sm_print (MOP classmop)
{
  SM_CLASS *class_;

  if (au_fetch_class (classmop, &class_, S_LOCK, AU_SELECT) == NO_ERROR)
    {
      classobj_print (class_);
    }
}
#endif

/* LOCATOR SUPPORT FUNCTIONS */
/*
 * sm_classobj_name() - Given a pointer to a class object in memory,
 *    return the name. Used by the transaction locator.
 *   return: class name
 *   classobj(in): class structure
 */

const char *
sm_classobj_name (MOBJ classobj)
{
  SM_CLASS_HEADER *class_;
  const char *name = NULL;

  if (classobj != NULL)
    {
      class_ = (SM_CLASS_HEADER *) classobj;
      name = class_->name;
    }

  return name;
}

/*
 * sm_heap() - Support function for the transaction locator.
 *    This returns a pointer to the heap file identifier in a class.
 *    This will work for either classes or the root class.
 *   return: HFID of class
 *   clobj(in): pointer to class structure in memory
 */

HFID *
sm_heap (MOBJ clobj)
{
  SM_CLASS_HEADER *header;
  HFID *heap;

  header = (SM_CLASS_HEADER *) clobj;

  heap = &header->heap;

  return heap;
}

/*
 * sm_get_heap() - Return the HFID of a class given a MOP.
 *    Like sm_heap but takes a MOP.
 *   return: hfid of class
 *   classmop(in): class object
 */

HFID *
sm_get_heap (MOP classmop)
{
  SM_CLASS *class_ = NULL;
  HFID *heap;

  heap = NULL;
  if (locator_is_class (classmop))
    {
      if (au_fetch_class (classmop, &class_, S_LOCK, AU_SELECT) == NO_ERROR)
	{
	  heap = &class_->header.heap;
	}
    }

  return heap;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * sm_has_indexes() - This is used to determine if there are any indexes
 *    associated with a particular class.
 *    Currently, this is used only by the locator so
 *    that when deleted instances are flushed, we can set the appropriate
 *    flags so that the indexes on the server will be updated.  For updated
 *    objects, the "has indexes" flag is returned by tf_mem_to_disk().
 *    Since we don't transform deleted objects however, we need a different
 *    mechanism for determining whether indexes exist.  Probably we should
 *    be using this function for all cases and remove the flag from the
 *    tf_ interface.
 *    This will return an error code if the class could not be fetched for
 *    some reason.  Authorization is NOT checked here.
 *    All of the constraint information is also contained on the class
 *    property list as well as the class constraint cache.  The class
 *    constraint cache is probably easier and faster to search than
 *    scanning over each attribute.  Something that we might want to change
 *    later.
 *   return: Non-zero if there are indexes defined
 *   classmop(in): class pointer
 */

bool
sm_has_indexes (MOBJ classobj)
{
  SM_CLASS *class_;
  SM_CLASS_CONSTRAINT *con;
  bool has_indexes = false;

  class_ = (SM_CLASS *) classobj;
  for (con = class_->constraints; con != NULL; con = con->next)
    {
      if (SM_IS_CONSTRAINT_INDEX_FAMILY (con->type))
	{
	  has_indexes = true;
	  break;
	}
    }

  return has_indexes;
}

/*
 * sm_has_constraint() - This is used to determine if a constraint is
 *    associated with a particular class.
 *   return: Non-zero if there are constraints defined
 *   classobj(in): class pointer
 *   constraint(in): the constraint to look for
 */

static int
sm_has_constraint (MOBJ classobj, SM_ATTRIBUTE_FLAG constraint)
{
  SM_CLASS *class_;
  SM_ATTRIBUTE *att;
  int has_constraint = 0;

  class_ = (SM_CLASS *) classobj;
  for (att = class_->attributes; att != NULL;
       att = (SM_ATTRIBUTE *) att->header.next)
    {
      if (att->flags & constraint)
	{
	  has_constraint = 1;
	  break;
	}
    }

  return has_constraint;
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * sm_class_constraints() - Return a pointer to the class constraint cache.
 *    A NULL pointer is returned is an error occurs.
 *   return: class constraint
 *   classop(in): class pointer
 */

SM_CLASS_CONSTRAINT *
sm_class_constraints (MOP classop)
{
  int error = NO_ERROR;
  SM_CLASS *class_;
  SM_CLASS_CONSTRAINT *constraints = NULL;

  error = au_fetch_class (classop, &class_, S_LOCK, AU_SELECT);
  if (error == NO_ERROR)
    {
      constraints = class_->constraints;
    }

  return constraints;
}

/* INTERPRETER SUPPORT FUNCTIONS */
/*
 * sm_find_class() - Given a class name, return the class object.
 *    All this really does is call locator_find_class but it makes sure the
 *    search is case insensitive.
 *   return: class object
 *   name(in): class name
 */

MOP
sm_find_class (const char *name)
{
  char realname[SM_MAX_IDENTIFIER_LENGTH];

  if (name == NULL)
    {
      return NULL;
    }

  sm_downcase_name (name, realname, SM_MAX_IDENTIFIER_LENGTH);

  return (locator_find_class (realname, S_LOCK));
}

/*
 * find_attribute_op() - Given the MOP of an object and an attribute name,
 *    return a pointer to the class structure and a pointer to the
 *    attribute structure with the given name.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   op(in): class or instance MOP
 *   name(in): attribute name
 *   classp(out): return pointer to class
 *   attp(out): return pointer to attribute
 */

static int
find_attribute_op (MOP op, const char *name,
		   SM_CLASS ** classp, SM_ATTRIBUTE ** attp)
{
  int error = NO_ERROR;
  SM_CLASS *class_;
  SM_ATTRIBUTE *att;

  if (!sm_check_name (name))
    {
      error = er_errid ();
    }
  else
    {
      error = au_fetch_class (op, &class_, S_LOCK, AU_SELECT);
      if (error == NO_ERROR)
	{
	  att = classobj_find_attribute (class_->attributes, name);
	  if (att == NULL)
	    {
	      ERROR1 (error, ER_SM_ATTRIBUTE_NOT_FOUND, name);
	    }
	  else
	    {
	      *classp = class_;
	      *attp = att;
	    }
	}
    }

  return error;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * sm_get_att_domain() - Get the domain descriptor for an attribute.
 *    This should be replaced with sm_get_att_info.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   op(in): class object
 *   name(in): attribute name
 *   domain(out): returned pointer to domain
 */

static int
sm_get_att_domain (MOP op, const char *name, TP_DOMAIN ** domain)
{
  int error = NO_ERROR;
  SM_ATTRIBUTE *att;
  SM_CLASS *class_;

  if ((error = find_attribute_op (op, name, &class_, &att)) == NO_ERROR)
    {
      assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

      sm_filter_domain (att->sma_domain);
      *domain = att->sma_domain;
    }

  return error;
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * sm_get_att_name() - Get the name of an attribute with its id.
 *   return: attribute name
 *   classop(in): class object
 *   id(in): attribute ID
 */

const char *
sm_get_att_name (MOP classop, int id)
{
  const char *name = NULL;
  int error;
  SM_CLASS *class_;
  SM_ATTRIBUTE *att;

  error = au_fetch_class (classop, &class_, S_LOCK, AU_SELECT);
  if (error == NO_ERROR)
    {
      att = classobj_find_attribute_id (class_->attributes, id);
      if (att != NULL)
	{
	  name = att->name;
	}
    }

  return name;
}

/*
 * sm_att_id() - Returns the internal id number assigned to the attribute.
 *   return: attribute id number
 *   classop(in): class object
 *   name(in): attribute
 */

int
sm_att_id (MOP classop, const char *name)
{
  SM_CLASS *class_;
  SM_ATTRIBUTE *att = NULL;
  int id;

  id = -1;
  if (find_attribute_op (classop, name, &class_, &att) == NO_ERROR)
    {
      id = att->id;
    }

  return id;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * sm_att_type_id() - Return the type constant for the basic
 * 		      type of an attribute.
 *   return: type identifier
 *   classop(in): class object
 *   name(in): attribute name
 */

DB_TYPE
sm_att_type_id (MOP classop, const char *name)
{
  SM_CLASS *class_;
  SM_ATTRIBUTE *att = NULL;
  DB_TYPE type;

  type = DB_TYPE_NULL;
  if (find_attribute_op (classop, name, &class_, &att) == NO_ERROR)
    {
      type = att->type->id;
    }

  return type;
}

/*
 * sm_type_name() - Accesses the primitive type name for a type identifier.
 *    Used by the interpreter for error messages during semantic checking.
 *   return: internal primitive type name
 *   id(in): type identifier
 */

static const char *
sm_type_name (DB_TYPE id)
{
  PR_TYPE *type;

  type = PR_TYPE_FROM_ID (id);
  if (type != NULL)
    {
      return type->name;
    }

  return NULL;
}

/*
 * sm_att_class() - Returns the domain class of an attribute if its basic type
 *    is DB_TYPE_OBJECT.
 *   return: domain class of attribute
 *   classop(in): class object
 *   name(in): attribute name
 */

MOP
sm_att_class (MOP classop, const char *name)
{
  SM_CLASS *class_;
  SM_ATTRIBUTE *att = NULL;
  MOP attclass;

  attclass = NULL;
  if (find_attribute_op (classop, name, &class_, &att) == NO_ERROR)
    {
      assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

      sm_filter_domain (att->sma_domain);
      if (att->sma_domain != NULL && att->sma_domain->type == tp_Type_object)
	{
	  attclass = att->sma_domain->class_mop;
	}
    }

  return attclass;
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * sm_att_info() - Used by the interpreter and query compiler to gather
 *    misc information about an attribute.  Don't set errors
 *    if the attribute was not found, the compiler may use this to
 *    probe classes for information and will handle errors on its own.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 *   name(in): attribute name
 *   idp(out): returned attribute identifier
 *   domainp(out): returned domain structure
 */

int
sm_att_info (MOP classop, const char *name, int *idp, TP_DOMAIN ** domainp)
{
  int error = NO_ERROR;
  SM_CLASS *class_;
  SM_ATTRIBUTE *att;

  att = NULL;

  error = au_fetch_class (classop, &class_, S_LOCK, AU_SELECT);
  if (error == NO_ERROR)
    {
      att = classobj_find_attribute (class_->attributes, name);
      if (att == NULL)
	{
	  /* return error but don't call er_set */
	  error = ER_SM_ATTRIBUTE_NOT_FOUND;
	}

      if (error == NO_ERROR)
	{
	  assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

	  sm_filter_domain (att->sma_domain);
	  *idp = att->id;
	  *domainp = att->sma_domain;
	}
    }

  return error;
}

/*
 * sm_find_index()
 *   return: Pointer to B-tree ID variable.
 *   classop(in): class object
 *   att_names(in):
 *   num_atts(in):
 *   unique_index_only(in):
 *   btid(out):
 */

BTID *
sm_find_index (MOP classop, const char **att_names, int num_atts,
	       bool unique_index_only, BTID * btid)
{
  int error = NO_ERROR;
  int i;
  SM_CLASS *class_;
  SM_CLASS_CONSTRAINT *con = NULL;
  SM_ATTRIBUTE *att1, *att2;
  BTID *index = NULL;

  error = au_fetch_class (classop, &class_, S_LOCK, AU_SELECT);
  if (error != NO_ERROR)
    {
      return NULL;
    }

  for (con = class_->constraints; con != NULL; con = con->next)
    {
      if (!SM_IS_CONSTRAINT_INDEX_FAMILY (con->type))
	{
	  continue;
	}

      if (unique_index_only)
	{
	  if (!SM_IS_CONSTRAINT_UNIQUE_FAMILY (con->type))
	    {
	      continue;
	    }
	}

#if 1
      if (con->index_status == 0)
	{
	  /* in progress */
	  continue;
	}
#endif

      if (num_atts == 0)
	{
	  /* we don't care about attributes, any index is a good one */
	  break;
	}

      for (i = 0; i < num_atts; i++)
	{
	  att1 = con->attributes[i];
	  if (att1 == NULL)
	    {
	      break;
	    }

	  att2 = classobj_find_attribute (class_->attributes, att_names[i]);
	  if (att2 == NULL || att1->id != att2->id)
	    {
	      break;
	    }
	}

      if ((i == num_atts) && con->attributes[i] == NULL)
	{
	  /* found it */
	  break;
	}
    }

  if (con)
    {
#if 1
      assert (con->index_status != 0);
#endif

      BTID_COPY (btid, &con->index_btid);
      index = btid;
    }

  return index;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * sm_att_default_value() - Gets the default value of a column.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 *   name(in): attribute
 *   value(out): the default value of the specified attribute
 */

int
sm_att_default_value (MOP classop, const char *name, DB_VALUE * value,
		      DB_DEFAULT_EXPR_TYPE * function_code)
{
  SM_CLASS *class_ = NULL;
  SM_ATTRIBUTE *att = NULL;
  int error = NO_ERROR;

  assert (value != NULL);

  error = db_value_clear (value);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  error = find_attribute_op (classop, name, &class_, &att);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  error = db_value_clone (&att->default_value.value, value);
  if (error != NO_ERROR)
    {
      goto error_exit;
    }

  *function_code = att->default_value.default_expr;
  return error;

error_exit:
  return error;
}
#endif

/*
 * sm_att_constrained() - Returns whether the attribute is constained.
 *   return: whether the attribute is constrained.
 *   classop(in): class object
 *   name(in): attribute
 *   cons(in): constraint
 */

int
sm_att_constrained (MOP classop, const char *name, SM_ATTRIBUTE_FLAG cons)
{
  SM_CLASS *class_;
  SM_ATTRIBUTE *att = NULL;
  int rc;

  rc = 0;
  if (find_attribute_op (classop, name, &class_, &att) == NO_ERROR)
    {
      if (SM_IS_ATTFLAG_INDEX_FAMILY (cons))
	{
	  rc = classobj_get_cached_index_family (att->constraints,
						 SM_MAP_INDEX_ATTFLAG_TO_CONSTRAINT
						 (cons), NULL);
	}
      else
	{
	  rc = att->flags & cons;
	}
    }

  return rc;
}

/*
 * sm_att_unique_constrained() - Returns whether the attribute is UNIQUE constained.
 *   return: whether the attribute is UNIQUE constrained.
 *   classop(in): class object
 *   name(in): attribute
 */

int
sm_att_unique_constrained (MOP classop, const char *name)
{
  SM_CLASS *class_;
  SM_ATTRIBUTE *att = NULL;
  int rc;

  rc = 0;
  if (find_attribute_op (classop, name, &class_, &att) == NO_ERROR)
    {
      rc = classobj_has_unique_constraint (att->constraints);
    }

  return rc;
}

/* QUERY PROCESSOR SUPPORT FUNCTIONS */
/*
 * sm_get_class_repid() - Used by the query compiler to tag compiled
 *    queries/views with the representation ids of the involved classes.
 *    This allows it to check for class modifications at a later date and
 *    invalidate the query/view.
 *   return: current representation id if class. Returns -1 if an error ocurred
 *   classop(in): class object
 */

#if 1				/* TODO - remove me someday */
/*
 * sm_flush_objects() - Flush all the instances of a particular class
 *    to the server. Used by the query processor to ensure that all
 *    dirty objects of a class are flushed before attempting to
 *    execute the query.
 *    It is important that the class be flushed as well.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   obj(in): class or instance
 */

int
sm_flush_objects (MOP obj)
{
  return sm_flush_and_decache_objects (obj, DONT_DECACHE);
}
#endif

/*
 * sm_flush_and_decache_objects() - Flush all the instances of a particular
 *    class to the server. Optionally decache the instances of the class.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   obj(in): class or instance
 *   decache(in): whether to decache the instances of the class.
 */

int
sm_flush_and_decache_objects (MOP obj, int decache)
{
  int error = NO_ERROR;
  MOBJ mem;
  SM_CLASS *class_;

#if 1				/* TODO -trace */
  assert (decache == DONT_DECACHE);
#endif

  if (obj == NULL)
    {
      return NO_ERROR;		/* nop */
    }

  if (locator_is_class (obj))
    {
      /* always make sure the class is flushed as well */
      if (locator_flush_class (obj) != NO_ERROR)
	{
	  return er_errid ();
	}

      class_ = (SM_CLASS *) locator_fetch_class (obj, S_LOCK);
      if (class_ == NULL)
	{
	  ERROR0 (error, ER_WS_NO_CLASS_FOR_INSTANCE);
	}
      else
	{
	  switch (class_->class_type)
	    {
	    case SM_CLASS_CT:
	      if (sm_issystem (class_))
		{
		  /* if system class, flush all dirty class */
		  if (locator_flush_all_instances (sm_Root_class_mop,
						   DONT_DECACHE) != NO_ERROR)
		    {
		      error = er_errid ();
		      break;
		    }
		}

	      if (locator_flush_all_instances (obj, decache) != NO_ERROR)
		{
		  error = er_errid ();
		}
	      break;

	    case SM_VCLASS_CT:
	      break;

	    default:
	      assert (false);
	      break;
	    }
	}
    }
  else
    {
      if (obj->class_mop != NULL)
	{
	  if (locator_flush_class (obj->class_mop) != NO_ERROR)
	    {
	      return er_errid ();
	    }

	  class_ = (SM_CLASS *) locator_fetch_class (obj, S_LOCK);
	  if (class_ == NULL)
	    {
	      ERROR0 (error, ER_WS_NO_CLASS_FOR_INSTANCE);
	    }
	  else
	    {
	      switch (class_->class_type)
		{
		case SM_CLASS_CT:
		  if (locator_flush_all_instances (obj->class_mop,
						   decache) != NO_ERROR)
		    {
		      error = er_errid ();
		    }
		  break;

		case SM_VCLASS_CT:
		  break;

		default:
		  assert (false);
		  break;
		}
	    }
	}
      else
	{
#if 1				/* TODO - trace */
	  assert (false);
#endif
	  error = au_fetch_instance (obj, &mem, S_LOCK, AU_SELECT);
	  if (error == NO_ERROR)
	    {
	      /* don't need to pin here, we only wanted to check authorization */
	      if (obj->class_mop != NULL)
		{
		  if (locator_flush_class (obj->class_mop) != NO_ERROR)
		    {
		      return er_errid ();
		    }

		  class_ = (SM_CLASS *) locator_fetch_class (obj, S_LOCK);
		  if (class_ == NULL)
		    {
		      ERROR0 (error, ER_WS_NO_CLASS_FOR_INSTANCE);
		    }
		  else
		    {
		      switch (class_->class_type)
			{
			case SM_CLASS_CT:
			  if (locator_flush_all_instances (obj->class_mop,
							   decache)
			      != NO_ERROR)
			    {
			      error = er_errid ();
			    }
			  break;

			case SM_VCLASS_CT:
			  break;

			default:
			  assert (false);
			  break;
			}
		    }
		}
	      else
		{
#if 1				/* TODO - trace */
		  assert (false);
#endif
		  ERROR0 (error, ER_WS_NO_CLASS_FOR_INSTANCE);
		}
	    }
	}
    }

  return error;
}

/* WORKSPACE/GARBAGE COLLECTION SUPPORT FUNCTIONS */
/*
 * sm_issystem() - This is called by the workspace manager to see
 *    if a class is a system class.  This avoids having the ws files know about
 *    the class structure and flags.
 *   return: non-zero if class is system class
 */

int
sm_issystem (SM_CLASS * class_)
{
  return (class_->flags & SM_CLASSFLAG_SYSTEM);
}

int
sm_isshard_table (SM_CLASS * class_)
{
  return (class_->flags & SM_CLASSFLAG_SHARD_TABLE);
}

/*
 * sm_local_schema_version()
 *   return: unsigned int indicating any change in local schema as none
 */

unsigned int
sm_local_schema_version (void)
{
  return local_schema_version;
}

/*
 * sm_bump_local_schema_version()
 *
 */

void
sm_bump_local_schema_version (void)
{
  local_schema_version++;
}

/*
 * sm_global_schema_version()
 *   return: unsigned int indicating any change in global schema as none
 */

unsigned int
sm_global_schema_version (void)
{
  return global_schema_version;
}

/*
 * sm_bump_global_schema_version()
 *
 */

void
sm_bump_global_schema_version (void)
{
  global_schema_version++;
}

/*
 * sm_virtual_queries() - Frees a session for a class.
 *   return: SM_CLASS pointer, with valid virtual query cache a class db_object
 *   parent_parser(in):
 *   class_object(in):
 */

struct parser_context *
sm_virtual_queries (void *parent_parser, DB_OBJECT * class_object)
{
  SM_CLASS *cl;
  unsigned int current_schema_id;
  PARSER_CONTEXT *cache = NULL, *old_cache = NULL;
  PARSER_CONTEXT *virtual_query = NULL;

  assert (parent_parser != NULL);
  assert (class_object != NULL);

  if (au_fetch_class_force (class_object, &cl, S_LOCK) != NO_ERROR)
    {
      return NULL;
    }

  (void) ws_pin (class_object, 1);

  if (cl->virtual_query_cache != NULL
      && cl->virtual_query_cache->view_cache != NULL
      && cl->virtual_query_cache->view_cache->vquery_for_query != NULL)
    {
      (void) pt_class_pre_fetch (cl->virtual_query_cache,
				 cl->virtual_query_cache->view_cache->
				 vquery_for_query);
      if (er_has_error ())
	{
	  return NULL;
	}

      if (pt_has_error (cl->virtual_query_cache))
	{
	  mq_free_virtual_query_cache (cl->virtual_query_cache);
	  cl->virtual_query_cache = NULL;
	}
    }

  current_schema_id = sm_local_schema_version ()
    + sm_global_schema_version ();

  if (cl->virtual_query_cache != NULL
      && cl->virtual_cache_schema_id != current_schema_id)
    {
      old_cache = cl->virtual_query_cache;
      cl->virtual_query_cache = NULL;
    }

  if (cl->class_type != SM_CLASS_CT && cl->virtual_query_cache == NULL)
    {
      /* Okay, this is a bit of a kludge:  If there happens to be a
       * cyclic view definition, then the virtual_query_cache will be
       * allocated during the call to mq_virtual_queries. So, we'll
       * assign it to a temp pointer and check it again.  We need to
       * keep the old one and free the new one because the parser
       * assigned originally contains the error message.
       */
      virtual_query = mq_virtual_queries (class_object);
      if (virtual_query == NULL)
	{
	  if (old_cache)
	    {
	      cl->virtual_query_cache = old_cache;
	    }
	  return NULL;
	}

      if (old_cache)
	{
	  mq_free_virtual_query_cache (old_cache);
	}

      if (cl->virtual_query_cache)
	{
	  mq_free_virtual_query_cache (virtual_query);
	}
      else
	{
	  cl->virtual_query_cache = virtual_query;
	}

      /* need to re-evalutate current_schema_id as global_schema_version
       * was changed by the call to mq_virtual_queries() */
      current_schema_id = sm_local_schema_version ()
	+ sm_global_schema_version ();
      cl->virtual_cache_schema_id = current_schema_id;
    }

  cache = cl->virtual_query_cache;

  if (cache != NULL)
    {
      /* propagate errors */
      ((PARSER_CONTEXT *) parent_parser)->error_msgs =
	parser_append_node (parser_copy_tree_list
			    ((PARSER_CONTEXT *) parent_parser,
			     cache->error_msgs),
			    ((PARSER_CONTEXT *) parent_parser)->error_msgs);
    }

  return cache;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * sm_get_attribute_descriptor() - Find the named attribute structure
 *    in the class and return it. Lock the class with the appropriate intent.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   op(in): class or instance
 *   name(in): attribute name
 *   for_update(in): non-zero if we're intending to update the attribute
 *   desc_ptr(out): returned attribute descriptor
 */

int
sm_get_attribute_descriptor (DB_OBJECT * op, const char *name,
			     int for_update, SM_DESCRIPTOR ** desc_ptr)
{
  int error = NO_ERROR;
  SM_CLASS *class_;
  SM_ATTRIBUTE *att;
  SM_DESCRIPTOR *desc;
  MOP classmop;

  att = NULL;

  /* looking for an instance attribute */
  error = au_fetch_class (op, &class_, S_LOCK, AU_SELECT);
  if (error == NO_ERROR)
    {
      att = classobj_find_attribute (class_->attributes, name);
      if (att == NULL)
	{
	  ERROR1 (error, ER_OBJ_INVALID_ATTRIBUTE, name);
	}
    }

  if (!error && att != NULL)
    {
      /* class must have been fetched at this point */
      classmop = (locator_is_class (op)) ? op : op->class_mop;

      desc = classobj_make_descriptor (classmop, class_, att, for_update);
      if (desc == NULL)
	{
	  error = er_errid ();
	}
      else
	{
	  desc->next = sm_Descriptors;
	  sm_Descriptors = desc;
	  *desc_ptr = desc;
	}
    }

  return error;
}

/*
 * sm_free_descriptor() - Free an attribute descriptor.
 *    Remember to remove it from the global descriptor list.
 *   return: none
 *   desc(in): descriptor to free
 */

void
sm_free_descriptor (SM_DESCRIPTOR * desc)
{
  SM_DESCRIPTOR *d, *prev;

  for (d = sm_Descriptors, prev = NULL; d != desc; d = d->next)
    {
      prev = d;
    }

  /* if d == NULL, the descriptor wasn't on the global list and
     is probably a suspect pointer, ignore it */
  if (d != NULL)
    {
      if (prev == NULL)
	{
	  sm_Descriptors = d->next;
	}
      else
	{
	  prev->next = d->next;
	}

      classobj_free_descriptor (d);
    }
}

/*
 * sm_reset_descriptors() - This is called whenever a class is edited.
 *    Or when a transaction commits.
 *    We need to mark any descriptors that reference this class as
 *    being invalid since the attribute structure pointers contained
 *    in the descriptor are no longer valid.
 *   return: none
 *   class(in): class being modified
 */

void
sm_reset_descriptors (MOP class_)
{
  SM_DESCRIPTOR *d;
  SM_DESCRIPTOR_LIST *dl;

  if (class_ == NULL)
    {
      /* transaction boundary, unconditionally clear all outstanding
         descriptors */
      for (d = sm_Descriptors; d != NULL; d = d->next)
	{
	  classobj_free_desclist (d->map);
	  d->map = NULL;
	}
    }
  else
    {
      /* Schema change, clear any descriptors that reference the class.
         Note, the schema manager will call this for EVERY class in the
         hierarcy.
       */
      for (d = sm_Descriptors; d != NULL; d = d->next)
	{
	  for (dl = d->map; dl != NULL && dl->classobj != class_;
	       dl = dl->next)
	    ;

	  if (dl != NULL)
	    {
	      /* found one, free the whole list */
	      classobj_free_desclist (d->map);
	      d->map = NULL;
	    }
	}
    }
}

/*
 * fetch_descriptor_class() - Work function for sm_get_descriptor_component.
 *    If the descriptor has been cleared or if we need to fetch the
 *    class and check authorization for some reason, this function obtains
 *    the appropriate locks and checks the necessary authorization.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   op(in): object
 *   desc(in): descriptor
 *   for_update(in): non-zero if we're intending to update the attribute
 *   class(out): returned class pointer
 */

static int
fetch_descriptor_class (MOP op, SM_DESCRIPTOR * desc, int for_update,
			SM_CLASS ** class_)
{
  int error = NO_ERROR;

  if (for_update)
    {
      error = au_fetch_class (op, class_, S_LOCK, AU_UPDATE);
    }
  else
    {
      error = au_fetch_class (op, class_, S_LOCK, AU_SELECT);
    }

  return error;
}

/*
 * sm_get_descriptor_component() - This locates an attribute structure
 *    associated with the class of the supplied object and identified
 *    by the descriptor.
 *    If the attribute has already been cached in the descriptor it is
 *    returned, otherwise, we search the class for the matching component
 *    and add it to the descriptor cache.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   op(in): object
 *   desc(in): descriptor
 *   for_update(in): non-zero if we're intending to update the attribute
 *   class_ptr(out):
 *   att_ptr(out):
 */

int
sm_get_descriptor_component (MOP op, SM_DESCRIPTOR * desc,
			     int for_update,
			     SM_CLASS ** class_ptr, SM_ATTRIBUTE ** att_ptr)
{
  int error = NO_ERROR;
  SM_CLASS *class_;
  SM_ATTRIBUTE *att;
  SM_DESCRIPTOR_LIST *d, *prev, *new_;
  MOP classmop;

  /* handle common case quickly, allow either an instance MOP or
     class MOP to be used here */
  if (desc->map != NULL
      && (desc->map->classobj == op || desc->map->classobj == op->class_mop)
      && (!for_update || desc->map->write_access))
    {
      *att_ptr = desc->map->att;
      *class_ptr = desc->map->class_;
    }
  else
    {
      /* this is set when a fetch is performed, try to avoid if possible */
      class_ = NULL;

      /* get the class MOP for this thing, avoid fetching if possible */
      if (op->class_mop == NULL)
	{
	  if (fetch_descriptor_class (op, desc, for_update, &class_))
	    {
	      return er_errid ();
	    }
	}
      classmop = (IS_CLASS_MOP (op)) ? op : op->class_mop;

      /* search the descriptor map for this class */
      for (d = desc->map, prev = NULL; d != NULL && d->classobj != classmop;
	   d = d->next)
	{
	  prev = d;
	}

      if (d != NULL)
	{
	  /* found an existing one, move it to the head of the list */
	  if (prev != NULL)
	    {
	      prev->next = d->next;
	      d->next = desc->map;
	      desc->map = d;
	    }
	  /* check update authorization if we haven't done it yet */
	  if (for_update && !d->write_access)
	    {
	      if (class_ == NULL)
		{
		  if (fetch_descriptor_class (op, desc, for_update, &class_))
		    {
		      return er_errid ();
		    }
		}
	      d->write_access = 1;
	    }
	  *att_ptr = d->att;
	  *class_ptr = d->class_;
	}
      else
	{
	  /* not on the list, fetch it if we haven't already done so */
	  if (class_ == NULL)
	    {
	      if (fetch_descriptor_class (op, desc, for_update, &class_))
		{
		  return er_errid ();
		}
	    }

	  att = classobj_find_attribute (class_->attributes, desc->name);
	  if (att == NULL)
	    {
	      ERROR1 (error, ER_OBJ_INVALID_ATTRIBUTE, desc->name);
	    }
	  else
	    {
	      /* make a new descriptor and add it to the head of the list */
	      new_ = classobj_make_desclist (classmop, class_, att,
					     for_update);
	      if (new_ == NULL)
		{
		  error = er_errid ();
		}
	      else
		{
		  new_->next = desc->map;
		  desc->map = new_;
		  *att_ptr = att;
		  *class_ptr = class_;
		}
	    }
	}
    }

  return error;
}
#endif

/* NAME SEARCHERS */
/*
 * template_classname() - Shorthand function for calls to er_set.
 *    Get the class name for the class associated with a template.
 *   return: class name
 *   template(in): schema template
 */

static const char *
template_classname (SM_TEMPLATE * template_)
{
  const char *name;

  name = template_->name;
  if (name == NULL && template_->op != NULL)
    {
      name = sm_class_name (template_->op);
    }

  return name;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * candidate_source_name() - Shorthand function to determine the class name
 *    that is the source of the given candidate.
 *   return: class name
 *   template(in): template for class being edited
 *   candidate(in): candidate of interest
 */

static const char *
candidate_source_name (SM_TEMPLATE * template_, SM_CANDIDATE * candidate)
{
  const char *name = NULL;

  if (candidate->source != NULL)
    {
      name = sm_class_name (candidate->source);
    }
  else
    {
      if (template_->name != NULL)
	{
	  name = template_->name;
	}
      else if (template_->op != NULL)
	{
	  name = sm_class_name (template_->op);
	}
    }

  return name;
}
#endif

/* CANDIDATE STRUCTURE MAINTENANCE */

/*
 * make_candidate_from_attribute() - Construct a candidate structure from
 * 				     a class attribute.
 *   return: candidate structure
 *   att(in): attribute
 *   source(in): MOP of source class (immediate super class)
 */

static SM_CANDIDATE *
make_candidate_from_attribute (SM_ATTRIBUTE * att, MOP source)
{
  SM_CANDIDATE *candidate;

  candidate = (SM_CANDIDATE *) db_ws_alloc (sizeof (SM_CANDIDATE));
  if (candidate != NULL)
    {
      candidate->next = NULL;
      candidate->name = att->name;
      candidate->source = source;
      candidate->att = att;
      candidate->order = 0;
    }

  return candidate;
}

/*
 * free_candidates() - Free a list of candidates structures
 * 		       when done with schema flattening.
 *   return: none
 *   candidates(in): candidates list
 */

static void
free_candidates (SM_CANDIDATE * candidates)
{
  SM_CANDIDATE *c, *next;

  for (c = candidates, next = NULL; c != NULL; c = next)
    {
      next = c->next;
      db_ws_free (c);
    }
}

/*
 * prune_candidate() - This will remove the first candidate in the list AND
 *    all other candidates in the list that have the same name.  The list of
 *    candidates with the same name as the first candidate as returned.
 *    The source list is destructively modified to remove the pruned
 *    candidates.
 *   return: pruned candidates
 *   clist_pointer (in): source candidates list
 */

static SM_CANDIDATE *
prune_candidate (SM_CANDIDATE ** clist_pointer)
{
  SM_CANDIDATE *candidates, *head;

  candidates = NULL;
  head = *clist_pointer;
  if (head != NULL)
    {
      candidates =
	(SM_CANDIDATE *) nlist_filter ((DB_NAMELIST **) clist_pointer,
				       head->name,
				       (NLSEARCHER) SM_COMPARE_NAMES);
    }

  return candidates;
}

/*
 * add_candidate() - This adds a candidate structure for the component to
 *    the candidates list.
 *    If the component has an alias resolution in the resolution list,
 *    the candidate is marked as being aliased and an additional candidate
 *    is added to the list with the alias name.
 *   return: none
 *   candlist(in/out): pointer to candidate list head
 *   att(in): attribute to build a candidate for
 *   order(in): the definition order of this candidate
 *   source(in): the source class of the candidate
 */

static void
add_candidate (SM_CANDIDATE ** candlist, SM_ATTRIBUTE * att, int order,
	       MOP source)
{
  SM_CANDIDATE *new_;

  new_ = make_candidate_from_attribute (att, source);
  if (new_ == NULL)
    {
      return;
    }

  new_->order = order;
  new_->next = *candlist;
  *candlist = new_;
}

/*
 * make_attribute_from_candidate() - Called after candidate flattening
 *    to construct an actual class attribute for a flattened candidate.
 *   return: class attribute
 *   classop(in): class being defined
 *   cand(in): candidate structure
 */
static SM_ATTRIBUTE *
make_attribute_from_candidate (MOP classop, SM_CANDIDATE * cand)
{
  SM_ATTRIBUTE *att = NULL;

  att = classobj_copy_attribute (cand->att, NULL);
  if (att == NULL)
    {
      return NULL;
    }

  att->order = cand->order;

  /* !! ALWAYS CLEAR THIS, ITS A RUN TIME ONLY FLAG AND CAN'T
     MAKE IT TO DISK */
  att->flags &= ~SM_ATTFLAG_NEW;

  /* if this is an inherited component, clear out certain fields that
     don't get inherited automatically.
     We now allow the UNIQUE constraint to be inherited but not INDEX */

  if (cand->source != NULL && cand->source != classop)
    {
      att->id = -1;		/* must reassign this */
    }

  return att;
}

/* CANDIDATE GATHERING */
/*
 * get_candidates() - This builds a candidates list for the instance.
 *    The candidates list is the raw flattened list of all
 *    the attribute definitions.
 *    Each candidate is tagged with an order counter so that the definition
 *    order can be preserved in the resulting class.  Although attributes
 *    are included on the same candidates list, they are ordered
 *    separately.
 *   return: candidates list
 *   def(in): original template
 *   flag(in): flattened template (in progress)
 */

static SM_CANDIDATE *
get_candidates (SM_TEMPLATE * def, UNUSED_ARG SM_TEMPLATE * flat)
{
  SM_ATTRIBUTE *att;
  SM_CANDIDATE *candlist;
  int att_order;

  candlist = NULL;
  att_order = 0;

  /* add local attributes */
  for (att = def->attributes; att != NULL; att = att->next, att_order++)
    {
      add_candidate (&candlist, att, att_order, def->op);
    }

  return candlist;
}

/*
 * resolve_candidates() - This is the main function for checking component
 *    combination rules. Given a list of candidates, all of the rules for
 *    compatibility are checked and a winner is determined if there is
 *    more than one possible candidate.
 *    If any of the rules fail, an error code is returned.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   template(in): schema template
 *   candidates(in): candidates list
 *   auto_resolve(in): non-zero to enable auto resolution of conflicts
 *   winner_return(out): returned pointer to winning candidates
 */

static int
resolve_candidates (SM_TEMPLATE * template_, SM_CANDIDATE * candidates,
		    SM_CANDIDATE ** winner_return)
{
  SM_CANDIDATE *c, *local;

  local = NULL;

  for (c = candidates; c != NULL; c = c->next)
    {
      if (c->source == NULL || c->source == template_->op)
	{
	  local = c;
	}
    }

  *winner_return = local;
  return NO_ERROR;
}

/* COMPONENT FLATTENING */
/*
 * insert_attribute()
 *    This inserts an attribute into a list positioned according
 *    to the "order" field.
 *    This is intended to be used for the ordering of the flattened attribute
 *    list.  As such, we don't use the order_link field here we just use
 *    the regular next field.
 *   return: none
 *   attlist(in/out): pointer to attribte list
 *   att(in): attribute to insert
 */

static void
insert_attribute (SM_ATTRIBUTE ** attlist, SM_ATTRIBUTE * att)
{
  SM_ATTRIBUTE *a, *prev;

  prev = NULL;
  for (a = *attlist; a != NULL && a->order < att->order; a = a->next)
    {
      prev = a;
    }

  att->next = a;
  if (prev == NULL)
    {
      *attlist = att;
    }
  else
    {
      prev->next = att;
    }
}

/*
 * flatten_components() - This is used to flatten the components of a template.
 *    The components are first converted into a list of candidates.
 *    The candidates list is then checked for the rules of compatibility
 *    and conflicts are resolved.  The winning candidate for each name
 *    is then converted back to a component and added to the template
 *    on the appropriate list.
 *    NOTE: Formerly we assumed that the candidates would be pruned and
 *    resolved in order.  Although the "order" field in each candidate
 *    will be set correctly we can't assume that the resulting list we
 *    produce is also ordered.  This is important mainly because this
 *    template will be stored on the class and used in the flattening
 *    of any subclasses.  get_candidates assumes that the template
 *    lists of the super classes are ordered.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   def(in): schema template
 *   flat(out): flattened template
 */

static int
flatten_components (SM_TEMPLATE * def, SM_TEMPLATE * flat)
{
  int error = NO_ERROR;
  SM_CANDIDATE *candlist, *candidates, *winner = NULL;
  SM_ATTRIBUTE *att;

  /* get all of the possible candidates for this instance */
  candlist = get_candidates (def, flat);

  /* prune the like named candidates of the list one at a time, check
     for consistency and resolve any conflicts */

  while (error == NO_ERROR
	 && ((candidates = prune_candidate (&candlist)) != NULL))
    {
      error = resolve_candidates (flat, candidates, &winner);

      if (error == NO_ERROR)
	{
	  if (winner != NULL)
	    {
	      /* convert the candidate back to a attribute */
	      att = make_attribute_from_candidate (def->op, winner);
	      if (att == NULL)
		{
		  error = er_errid ();
		  free_candidates (candidates);
		  break;
		}

	      /* add it to the appropriate list */
	      insert_attribute (&flat->attributes, att);
	    }
	}
      free_candidates (candidates);
    }

  /* If an error occurs, the remaining candidates in candlist should be freed
   */

  if (candlist)
    {
      free_candidates (candlist);
    }

  return error;
}

/*
 * flatten_query_spec_lists() - Flatten the query_spec lists.
 *    Note that query_spec lists aren't flattened, we just use the one
 *    currently in the template.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   def(in): schema template
 *   flat(out): flattened template
 */

static int
flatten_query_spec_lists (SM_TEMPLATE * def, SM_TEMPLATE * flat)
{
  /* start by copying the local definitions to the template */
  if (def->query_spec == NULL)
    {
      flat->query_spec = NULL;
    }
  else
    {
      flat->query_spec = (SM_QUERY_SPEC *)
	WS_LIST_COPY (def->query_spec, classobj_copy_query_spec,
		      classobj_free_query_spec);
      if (flat->query_spec == NULL)
	{
	  return er_errid ();
	}
    }

  /* no need to flatten the query_spec lists */
  return NO_ERROR;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * find_matching_att() - This is a work function for retain_former_ids and
 *    others. It performs a very common attribute lookup operation.
 *    An attribute is said to match if the name, source class, and type
 *    are the same.
 *    If idmatch is selected the match is based on the id numbers only.
 *   return: matching attribute
 *   list(in): attribute list to search
 *   att(in): attribute to look for
 *   idmatch(in): flag to cause search based on id rather than name
 */

static SM_ATTRIBUTE *
find_matching_att (SM_ATTRIBUTE * list, SM_ATTRIBUTE * att, int idmatch)
{
  SM_ATTRIBUTE *a, *found;

  found = NULL;
  for (a = list; a != NULL && found == NULL; a = a->next)
    {
      if (idmatch)
	{
	  if (a->id == att->id)
	    {
	      found = a;
	    }
	}
      else
	{
	  if (SM_COMPARE_NAMES (a->name, att->name) == 0
	      && a->class_mop == att->class_mop && a->type == att->type)
	    {
	      found = a;
	    }
	}
    }

  return found;
}

/*
 * retain_former_ids() - This is a bit of a kludge because we lost the ids of
 *    the inherited attributes when the template was created.
 *    This is a problem for inherited attributes that have been renamed
 *    in the super class. Since they won't match based on name and the
 *    attribute id is -1, build_storage_order will think the inherited
 *    attribute was dropped and replaced with one of a different name.
 *    Immediately after flattening, we call this to fix the attribute
 *    id assignments for things that are the same.
 *    I think this would be a good place to copy the values of
 *    class attributes as well. We will have the same problem of
 *    name matching.
 *    When shadowing an inherited attribute, we used to think that we should
 *    retain the former attribute ID so that we don't lose access to data
 *    previously stored for that attribute. We now think that this is not
 *    the correct behavior. A shadowed attribute is a "new" attribute and
 *    it should shadow the inherited attribute along with its previously
 *    stored values.
 *   return: error code
 *   flat(in): template
 */

static int
retain_former_ids (SM_TEMPLATE * flat)
{
  SM_ATTRIBUTE *new_att, *found, *super_new, *super_old;
  SM_CLASS *sclass;

  /* Does this class have a previous representation ? */
  if (flat->current != NULL)
    {
      /* Check each new inherited attribute.  These attribute will not have
         an assigned id and their class MOPs will not match */
      for (new_att = flat->attributes; new_att != NULL;
	   new_att = new_att->next)
	{
	  /* is this a new attribute ? */
	  if (new_att->id == -1)
	    {
	      /* is it inherited ? */
	      if (new_att->class_mop != NULL
		  && new_att->class_mop != flat->op)
		{
		  assert (false);	/* is impossible */
		  /* look for a matching attribute in the existing representation */
		  found = find_matching_att (flat->current->attributes,
					     new_att, 0);
		  if (found != NULL)
		    {
		      /* re-use this attribute */
		      new_att->id = found->id;
		    }
		  else
		    {
		      /* couldn't find it, it may have been renamed in the super
		         class though */
		      if (au_fetch_class_force (new_att->class_mop, &sclass,
						S_LOCK) == NO_ERROR)
			{
			  /* search the super class' pending attribute list for
			     this name */
			  if (sclass->new_ != NULL)
			    {
			      super_new =
				find_matching_att (sclass->new_->attributes,
						   new_att, 0);
			      if (super_new != NULL)
				{
				  /*
				   * search the supers original attribute list
				   * based on the id of the new one
				   */
				  super_old =
				    find_matching_att (sclass->attributes,
						       super_new, 1);
				  if (super_old != NULL)
				    {
				      if (SM_COMPARE_NAMES
					  (super_old->name,
					   new_att->name) != 0)
					{
					  /* search our old list with the old name */
					  found =
					    find_matching_att (flat->
							       current->
							       attributes,
							       super_old, 0);
					  if (found != NULL)
					    {
					      /* found the renamed attribute, reuse id */
					      new_att->id = found->id;
					    }
					}
				    }
				}
			    }
			}
		    }
		}

/* As mentioned in the description above, we no longer think that
   it is a good idea to retain the old attribute ID when shadowing
   an inherited attribute.  Since we had thought differently before
   and might think differently again I would rather keep this part
   of the code in here as a reminder. */
#if 0
	      else
		{
		  /* Its a new local attribute.  If we're shadowing a previously
		     inherited attribute, reuse the old id so we don't lose the
		     previous value.  This is new (12/7/94), does it cause
		     unexpected problems ? */
		  /* look for one in the existing representation */
		  found =
		    classobj_find_attribute (flat->current->attributes,
					     new->header.name);
		  /* was it inherited ? */
		  if (found != NULL && found->class != new->class)
		    {
		      /* reuse the attribute id, don't have to worry about type
		         compatibility because that must have been checked during
		         flattening. */
		      new->id = found->id;
		    }
		  /* else couldn't find it, do we need to deal with the case where
		     the inherited attribute from the super class has been renamed
		     as is done above ? */
		}
#endif /* 0 */
	    }
	}
    }

  return NO_ERROR;
}
#endif

/*
 * flatten_properties() - This combines the interesting properties from the
 *    superclasses into the template property list. This is used mainly for
 *    UNIQUE constraint properties which must be inherited uniformly by
 *    the subclasses.
 *    NOTE: Things will get a lot more complicated here when we start having
 *    to deal with constraints over multiple attributes.
 *    Note that for NEW classes or constraints, the BTID will not have been
 *    allocated at this time, it is allocated in
 *    allocate_disk_structures() call after flattening has finished.
 *    This means that unique constraint info that we inherit may have a NULL
 *    BTID (fields are all -1).  That's ok for now, it will look as if it
 *    was one of our own local unique constraints.  When we get around
 *    to calling allocate_disk_structures() we must always check to see
 *    if the associated attributes were inherited and if so, go back
 *    to the super class to get its real BTID.  It is assumred that the
 *    super class will have the real BTID by this time because the call
 *    to allocate_disk_structures() has been moved to preceed the call
 *    to update_subclasses().
 *    It would be nice if we could allocate the indexes DURING flattening
 *    rather than deferring it until the end.  This would make the whole
 *    think cleaner and less prone to error.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   def(in): original class template
 *   flat(out): flattened template being built
 */

static int
flatten_properties (SM_TEMPLATE * def, SM_TEMPLATE * flat)
{
  int error = NO_ERROR;

  if (def->disk_constraints != NULL)
    {
      flat->disk_constraints =
	(SM_DISK_CONSTRAINT *) WS_LIST_COPY (def->disk_constraints,
					     classobj_copy_disk_constraint,
					     classobj_free_disk_constraint);
      if (flat->disk_constraints == NULL)
	{
	  goto structure_error;
	}
    }

  return error;

structure_error:

  /* should have a more appropriate error for this */
  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_SM_INVALID_PROPERTY, 0);

  return er_errid ();
}

/*
 * flatten_template() - Flatten a template, checking for all of the various
 *    schema rules.  Returns a flattened template that forms the basis
 *    for a new class representation if all went well.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   def(in): schema template
 *   deleted_class(in): MOP of deleted class (optional, can be NULL)
 *   flatp(out): returned pointer to flattened template
 *   auto_res(in): non-zero to enable auto resolution of conflicts
 */

static int
flatten_template (UNUSED_ARG SM_TEMPLATE * def, UNUSED_ARG MOP deleted_class,
		  SM_TEMPLATE ** flatp)
{
  int error = NO_ERROR;
  SM_TEMPLATE *flat;

  /* start with an empty template */
  flat = classobj_make_template (def->name, def->op, NULL);
  if (flat == NULL)
    {
      goto memory_error;
    }

  /* is this necessary ? */
  flat->class_type = def->class_type;

  /* remember this, CAN'T PASS THIS AS AN ARGUMENT to classobj_make_template */
  flat->current = def->current;

  /* merge query_spec lists */
  if (flatten_query_spec_lists (def, flat))
    {
      goto memory_error;
    }

  /* copy the loader commands, we should be flattening these as well ? */
  if (def->loader_commands != NULL)
    {
      flat->loader_commands = ws_copy_string (def->loader_commands);
      if (flat->loader_commands == NULL)
	{
	  goto memory_error;
	}
    }

  /* flatten each component list */
  error = flatten_components (def, flat);

  /* Flatten the properties (primarily for constraints).
   * Do this after the components have been flattened so we can see use this
   * information for selecting constraint properties.
   */
  if (flatten_properties (def, flat))
    {
      goto memory_error;
    }

#if defined (ENABLE_UNUSED_FUNCTION)
  if (error == NO_ERROR)
    {
      /* make sure these get kept */
      error = retain_former_ids (flat);
    }
#endif

  /* if errors, throw away the template and abort */
  if (error != NO_ERROR)
    {
      classobj_free_template (flat);
      flat = NULL;
    }

  *flatp = flat;
  return error;

memory_error:
  if (flat != NULL)
    {
      classobj_free_template (flat);
    }

  return er_errid ();
}

/* PREPARATION FOR NEW REPRESENTATIONS */

/*
 * order_atts_by_alignment() - Order the attributes by descending order of
 *    alignment needs.  Within the same alignment group, order the attributes
 *    by ascending order of disk size (this is mainly for the char types).
 *    In this way, if the object is too large to fit on one page, we can try to
 *    keep the smaller char types on the same page as the OID and thereby
 *    we might be able to read the attributes we need without reading
 *    the overflow page.
 *    This algorithm is simplistic but these lists are not long.
 *   return: ordered attributes.
 *   atts(in/out): attributes to be ordered
 */
static SM_ATTRIBUTE *
order_atts_by_alignment (SM_ATTRIBUTE * atts)
{
  SM_ATTRIBUTE *newatts, *found, *attr;

  newatts = NULL;

  while (atts != NULL)
    {
      for (found = atts, attr = atts; attr != NULL; attr = attr->next)
	{
	  assert (TP_DOMAIN_TYPE (attr->sma_domain) != DB_TYPE_VARIABLE);

	  /* the new attr becomes the found attr if it has larger alignment
	   * requirements or if it has the same alignment needs but has
	   * smaller disk size.
	   */
	  if ((attr->type->alignment > found->type->alignment)
	      || ((attr->type->alignment == found->type->alignment)
		  && (tp_domain_disk_size (attr->sma_domain) <
		      tp_domain_disk_size (found->sma_domain))))
	    {
	      found = attr;
	    }
	}

      /* move the one we found to the new list */
      WS_LIST_REMOVE (&atts, found);
      found->next = NULL;

      WS_LIST_APPEND (&newatts, found);
    }

  return newatts;
}

/*
 * build_storage_order() - Here we take a flattened template and reorder
 *    the attributes to be close to the ordering the class had before editing.
 *    In the process we assign attribute ids. If the current and new attribute
 *    lists turn out to be the same, we can avoid the generation of a
 *    new representation since the disk structure of the objects will
 *    be the same.  If the two attribute lists differ, non-zero is
 *    returned indicating that a new representation must be generated.
 *    At the start, the template has a list of instance
 *    attributes in the attributes list.  When this completes,
 *    the attributes list will be NULL and the attributes will have
 *    been split into two lists, ordered_attributes and empty shared_attributes.
 *    Formerly this function tried to retain ids of attributes that
 *    hadn't changed.  This is now done in retain_former_ids above.
 *    When we get here, this is the state of attribute ids in the
 *    flattened template:
 *      id != -1  : this is a local attribute (possibly renamed) that needs
 *                  to keep its former attribute id.
 *      id == -1  : this is an inherited attribute or new local attribute
 *                  if its new, assign a new id, if it's inherited, look
 *                  in the old attribute list for one that matches and reuse
 *                  the old id.  Searching the old list for matching
 *                  components could be done by make_attribute_from_candidate?
 *   return: non-zero if new representation is needed
 *   class(in): class being modified
 *   flat(out): new flattened template
 */

static int
build_storage_order (SM_CLASS * class_, SM_TEMPLATE * flat)
{
  SM_ATTRIBUTE *fixed, *variable, *current, *new_att, *found, *next, *newatts;
  int newrep;

  fixed = variable = NULL;
  newrep = 0;

  newatts = flat->attributes;
  flat->attributes = NULL;

  for (current = class_->attributes; current != NULL; current = current->next)
    {
      found = NULL;
      for (new_att = newatts; new_att != NULL && found == NULL;
	   new_att = new_att->next)
	{
	  assert (TP_DOMAIN_TYPE (new_att->sma_domain) != DB_TYPE_VARIABLE);

	  /* if the ids are the same, use it without looking at the name,
	     this is how rename works */
	  if (new_att->id != -1)
	    {
	      if (new_att->id == current->id)
		{
		  found = new_att;
		  /* ALTER CHANGE column : check if new representation is
		   * required */
		  if (!tp_domain_match
		      (current->sma_domain, new_att->sma_domain,
		       TP_EXACT_MATCH))
		    {
		      newrep = 1;
		    }
		}
	    }

	  /* this shouldn't be necessary now that we assume ids have been
	     assigned where there was one before */

	  else if ((SM_COMPARE_NAMES (current->name,
				      new_att->name) == 0)
		   && (current->class_mop == new_att->class_mop)
		   && (current->type == new_att->type))
	    {
	      found = new_att;
	    }
	}

      if (found == NULL)
	{
	  newrep = 1;		/* attribute was deleted */
	}
      else
	{
	  /* there was a match, either in name or id */
	  if (found->id == -1)
	    {
	      /* name match, reuse the old id */
	      found->id = current->id;
	    }

	  (void) WS_LIST_REMOVE (&newatts, found);
	  found->next = NULL;
	  if (found->type->variable_p)
	    {
	      WS_LIST_APPEND (&variable, found);
	    }
	  else
	    {
	      WS_LIST_APPEND (&fixed, found);
	    }
	}
    }

  /* check for new attributes */
  if (newatts != NULL)
    {
      newrep = 1;
      for (new_att = newatts, next = NULL; new_att != NULL; new_att = next)
	{
	  next = new_att->next;
	  new_att->next = NULL;
	  new_att->id = class_->att_ids++;

	  if (new_att->type->variable_p)
	    {
	      WS_LIST_APPEND (&variable, new_att);
	    }
	  else
	    {
	      WS_LIST_APPEND (&fixed, new_att);
	    }
	}
    }

  /* order the fixed attributes in descending order by alignment needs */
  if (fixed != NULL)
    {
      fixed = order_atts_by_alignment (fixed);
    }

  /* join the two lists */
  if (fixed == NULL)
    {
      newatts = variable;
    }
  else
    {
      newatts = fixed;
      for (new_att = fixed; new_att != NULL && new_att->next != NULL;
	   new_att = new_att->next)
	{
	  ;
	}
      new_att->next = variable;
    }

  /* now change the template to reflect the divided instance
     attribute lists */
  flat->instance_attributes = newatts;

  return newrep;
}

/*
 * fixup_component_classes() - Work function for install_new_representation.
 *    Now that we're certain that the template can be applied
 *    and we have a MOP for the class being edited, go through and stamp
 *    the attributes of the class with the classmop.  This
 *    makes it easier later for the browsing functions to get the origin
 *    class of attributes.  This is only a problem when the class is
 *    defined for the first time.
 *   return: none
 *   classop(in): class object
 *   flat(out): flattened template
 */

static void
fixup_component_classes (MOP classop, SM_TEMPLATE * flat)
{
  SM_ATTRIBUTE *a;

  for (a = flat->attributes; a != NULL; a = a->next)
    {
      if (a->class_mop == NULL)
	{
	  a->class_mop = classop;
	}
    }

}


/*
 * fixup_self_domain()
 * fixup_attribute_self_domain()
 * fixup_self_reference_domains() - Domains that were build for new classes
 *    that need to reference the class being build were constructed in a
 *    special way since the MOP of the class was not available at the time the
 *    domain structure was created.  Once semantic checking has been performed
 *    and the class is created, we not must go through and modify the
 *    temporary domain structures to look like real self-referencing
 *    domains.
 *    We now have a number of last minute fixup functions.  Try to bundle
 *    these into a single function sometime to avoid repeated passes
 *    over the class structures.  Not really that performance critical but
 *    nicer if this isn't spread out all over.
 *   return: none
 *   classop(in): class object
 *   flag(in/out): flattened template
 */

static void
fixup_self_domain (TP_DOMAIN * domain, MOP self)
{
  TP_DOMAIN *d;

  for (d = domain; d != NULL; d = d->next)
    {
      /* PR_TYPE is changeable only for transient domain. */
      assert (d->type != tp_Type_null || !d->is_cached);
      if (d->type == tp_Type_null && !d->is_cached)
	{
	  d->type = tp_Type_object;
	  d->class_mop = self;
	}
      fixup_self_domain (d->setdomain, self);
    }
}

static void
fixup_attribute_self_domain (SM_ATTRIBUTE * att, MOP self)
{
  /*
     Remember that attributes have a type pointer cache as well as a full
     domain.  BOTH of these need to be updated.  This is unfortunate, I
     think its time to remove the type pointer and rely on the domain
     structure only. */

  assert (TP_DOMAIN_TYPE (att->sma_domain) != DB_TYPE_VARIABLE);

  fixup_self_domain (att->sma_domain, self);
  att->sma_domain = tp_domain_cache (att->sma_domain);

  /* get the type cache as well */
  if (att->type == tp_Type_null)
    {
      att->type = tp_Type_object;
    }
}

static void
fixup_self_reference_domains (MOP classop, SM_TEMPLATE * flat)
{
  SM_ATTRIBUTE *att;

  /* should only bother with this if the class is new, can we somehow
     determine this here ? */

  for (att = flat->attributes; att != NULL; att = att->next)
    {
      fixup_attribute_self_domain (att, classop);
    }

}

/* DISK STRUCTURE ALLOCATION */

/*
   This done as a post processing pass of smt_finish_class to make sure
   that all attributes that were declared to have indexes or unique btids
   tables have the necessary disk structures allocated.

   Logically this should be done before the class is created so if any
   errors occur, we can abort the operation.  Unfortunately, doing this
   accurately requires attribute id's being assigned so it would have
   to go in install_new_representation.  After beta, restructure the
   sequence of operations in smt_finish_class and install_new_representation
   (and probably the flattener as well) so we have all the information necessary
   to generate the disk structures before the call to
   install_new_representation and before the class is created.

*/

/*
 * allocate_index() - Allocates an index on disk for an attribute of a class.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 *   class_(in): class structure
 *   constraint(in):
 */

static int
allocate_index (MOP classop, UNUSED_ARG SM_CLASS * class_,
		SM_CLASS_CONSTRAINT * constraint)
{
  int error = NO_ERROR;
  DB_TYPE *att_type = NULL;
  int i;
//  OID class_oid;
//  HFID hfid;

  assert (constraint != NULL);
  assert (constraint->num_atts > 0);

  att_type = (DB_TYPE *) malloc (constraint->num_atts * sizeof (DB_TYPE));
  if (att_type == NULL)
    {
      error = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
	      constraint->num_atts * sizeof (DB_TYPE));

      goto exit_on_error;
    }

  /* Count the attributes */
  for (i = 0; constraint->attributes[i] != NULL; i++)
    {
      /* server doesn't treat DB_TYPE_OBJECT, so that convert it to
         DB_TYPE_OID */
      att_type[i] = constraint->attributes[i]->type->id;
      if (att_type[i] == DB_TYPE_OBJECT)
	{
	  att_type[i] = DB_TYPE_OID;
	}

      if (!tp_valid_indextype (att_type[i]))
	{
	  assert (false);
	  error = ER_SM_INVALID_INDEX_TYPE;
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 1,
		  pr_type_name (att_type[i]));

	  goto exit_on_error;
	}
    }
  assert (i == constraint->num_atts);

  /* need to have macros for this !! */
  constraint->index_btid.vfid.volid = boot_User_volid;

  /* Enter the base class information into the arrays */
//  COPY_OID (&class_oid, WS_OID (classop));
//  HFID_COPY (&hfid, &class_->header.heap);

  error =
    btree_add_index (&constraint->index_btid, constraint->num_atts, att_type,
		     WS_OID (classop), constraint->attributes[0]->id);

  free_and_init (att_type);

  constraint->index_status = INDEX_STATUS_IN_PROGRESS;

  return error;

exit_on_error:

  if (att_type != NULL)
    {
      free_and_init (att_type);
    }

  if (error == NO_ERROR)
    {
      error = er_errid ();
      assert (error != NO_ERROR);
      if (error == NO_ERROR)
	{
	  error = ER_GENERIC_ERROR;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
	}
    }

  return error;
}

/*
 * deallocate_index() - Deallocate an index that was previously created for
 * 			an attribute.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   cons(in):
 *   index(in/out): index disk identifier
 */

static int
deallocate_index (SM_CLASS_CONSTRAINT * cons, BTID * index)
{
  int error = NO_ERROR;
  SM_CLASS_CONSTRAINT *con;
  int ref_count = 0;

  for (con = cons; con != NULL; con = con->next)
    {
      if (BTID_IS_EQUAL (index, &con->index_btid))
	{
	  ref_count++;
	}
    }

  if (ref_count == 1)
    {
      error = btree_delete_index (index);
    }

  return error;
}

/*
 * allocate_disk_structures_index() - Helper for index allocation
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 *   class(in): class structure
 *   con(in):constraint info
 *   recache_cls_cons(out):
 */
static int
allocate_disk_structures_index (MOP classop, SM_CLASS * class_,
				SM_CLASS_CONSTRAINT * con)
{
  int error = NO_ERROR;

  if (!SM_IS_CONSTRAINT_INDEX_FAMILY (con->type))
    {
      assert (false);
      return NO_ERROR;
    }

  if (!BTID_IS_NULL (&con->index_btid))
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
      return error;
    }

  error = allocate_index (classop, class_, con);
  if (error != NO_ERROR)
    {
      return error;
    }

  /* check for safe guard */
  if (BTID_IS_NULL (&con->index_btid))
    {
      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
      return error;		/* unknown error */
    }

  return NO_ERROR;
}

/*
 * allocate_disk_structures() - Allocate the necessary disk structures for
 *    a new or modified class. For constraints, be careful to recognize
 *    a place holder for a BTID that hasn't been allocated yet but whose
 *    definition was actually inherited from a super class. When we find these,
 *    go to the super class and use the BTID that will have by now been
 *    allocated in there rather than allocating a new one of our own.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 *   class(in): class structure
 *   num_allocated_index(out):
 */
static int
allocate_disk_structures (MOP classop, SM_CLASS * class_,
			  int *num_allocated_index)
{
  SM_CLASS_CONSTRAINT *con;
  LOCK lock_of_temp_class = NULL_LOCK;
  OID class_oid;
  HFID hfid;

  assert (classop != NULL);

  OID_SET_NULL (&class_oid);
  HFID_SET_NULL (&hfid);

  *num_allocated_index = 0;

  if (classop == NULL)
    {
      assert (classop != NULL);

      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
      return ER_GENERIC_ERROR;
    }

  if (classobj_cache_class_constraints (class_))
    {
      goto structure_error;
    }

  if (OID_ISTEMP (ws_oid (classop)))
    {
      lock_of_temp_class = ws_get_lock (classop);

      if (locator_assign_permanent_oid (classop) == NULL)
	{
	  goto structure_error;
	}
    }

  COPY_OID (&class_oid, WS_OID (classop));
  assert (!OID_ISTEMP (&class_oid));
  HFID_COPY (&hfid, &class_->header.heap);

  for (con = class_->constraints; con != NULL; con = con->next)
    {
      if (SM_IS_CONSTRAINT_INDEX_FAMILY (con->type)
	  && con->attributes[0] != NULL)
	{
	  if (!BTID_IS_NULL (&con->index_btid)
	      && con->index_status != INDEX_STATUS_COMPLETED)
	    {
	      assert (false);

	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_ALTER_EXISTS_INVALID_INDEX_IN_TABLE, 0);
	      goto structure_error;
	    }
	  else if (BTID_IS_NULL (&con->index_btid))
	    {
	      if (!HFID_IS_NULL (&hfid))
		{
		  if (ws_get_lock (classop) != U_LOCK)
		    {
		      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			      ER_ALTER_CANNOT_UPDATE_HEAP_DATA, 0);
		      goto structure_error;
		    }
		}

	      if (allocate_disk_structures_index (classop, class_, con) !=
		  NO_ERROR)
		{
		  goto structure_error;
		}
	      (*num_allocated_index)++;
	    }

	  assert (!BTID_IS_NULL (&con->index_btid));

	  /* Whether we allocated a BTID or not, always write the constraint info
	   * back out to the property list.  This is where the promotion of
	   * attribute name references to ids references happens.
	   */
	  if (classobj_put_disk_constraint (&class_->disk_constraints,
					    con) != NO_ERROR)
	    {
	      goto structure_error;
	    }
	}
    }

  /* when we're done, make sure that each attribute's cache is also updated */
  if (!classobj_cache_constraints (class_))
    {
      goto structure_error;
    }

  if (locator_update_class (classop) == NULL)
    {
      goto structure_error;
    }

  if (locator_flush_class (classop) != NO_ERROR)
    {
      goto structure_error;
    }

  if (lock_of_temp_class != NULL_LOCK)
    {
      /* classop do not have lock of permanent oid */
      ws_set_lock (classop, S_LOCK);	/* temporary set as S lock */

      /* get lock on class */
      if (locator_fetch_class (classop, lock_of_temp_class) == NULL)
	{
	  goto structure_error;
	}

      /* acquire X lock */
      assert (ws_get_lock (classop) == lock_of_temp_class);
    }

  return NO_ERROR;

structure_error:
  /* the workspace has already been damaged by this point, the caller will
   * have to recognize the error and abort the transaction.
   */
  return er_errid ();
}

/*
 * load_index_data() -
 *   return: NO_ERROR or error code
 *   classop(in): class object
 *   class(in): class structure
 */
static int
load_index_data (MOP classop, SM_CLASS * class_)
{
  SM_CLASS_CONSTRAINT *con;
  int error = NO_ERROR;
  HFID hfid;
  OID class_oid;

  if (classop == NULL)
    {
      assert (classop != NULL);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
      return error;
    }

  COPY_OID (&class_oid, WS_OID (classop));
  assert (!OID_ISTEMP (&class_oid));
  HFID_COPY (&hfid, &class_->header.heap);

  assert (HFID_IS_NULL (&hfid) || ws_get_lock (classop) == U_LOCK);

  for (con = class_->constraints; con != NULL; con = con->next)
    {
      assert (con->attributes != NULL);
      if (SM_IS_CONSTRAINT_INDEX_FAMILY (con->type)
	  && con->attributes[0] != NULL
	  && con->index_status == INDEX_STATUS_IN_PROGRESS)
	{
	  error = btree_load_data (&con->index_btid, &class_oid, &hfid);
	  if (error != NO_ERROR)
	    {
	      goto exit_on_error;
	    }

	  con->index_status = INDEX_STATUS_COMPLETED;
	  if (classobj_put_disk_constraint (&class_->disk_constraints,
					    con) != NO_ERROR)
	    {
	      GOTO_EXIT_ON_ERROR;
	    }
	}
    }

  if (locator_fetch_class (classop, U_LOCK) == NULL)
    {
      goto exit_on_error;
    }

  /* when we're done, make sure that each attribute's cache is also updated */
  if (!classobj_cache_constraints (class_))
    {
      error = er_errid ();
      goto exit_on_error;
    }

  if (locator_update_class (classop) == NULL)
    {
      error = er_errid ();

      goto exit_on_error;
    }

  error = locator_flush_class (classop);
  if (error != NO_ERROR)
    {
      goto exit_on_error;
    }

  error = sm_update_statistics (classop, true /* update_stats */ ,
				STATS_WITH_SAMPLING);
  if (error != NO_ERROR)
    {
      goto exit_on_error;
    }

  return NO_ERROR;

exit_on_error:
  /* the workspace has already been damaged by this point, the caller will
   * have to recognize the error and abort the transaction.
   */

  /* for abort transaction */
  (void) locator_fetch_class (classop, X_LOCK);

  if (error == NO_ERROR)
    {
      assert (error != NO_ERROR);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, "");
    }

  return error;
}

/*
 * transfer_disk_structures() - Work function for install_new_representation.
 *    Here we look for any attributes that are being dropped from the
 *    class and remove their associated disk structures (if any).
 *    This also moves the index ids from the existing attribute structures
 *    into the new ones.  It must do this because copying the index
 *    field is not part of the usual copying done by the cl_ functions.
 *    This is because indexes are not inherited and we
 *    must be very careful that they stay only with the class on which
 *    they were defined.
 *    This can also be called for sm_delete_class with a template of
 *    NULL in which case we just free all disk structures we find.
 *    We DO NOT allocate new index structures here, see
 *    allocate_disk_structures to see how that is done.
 *    This is where BTID's for unique & indexes get inherited.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 *   class(in): class structure
 *   flat(out): new flattened template
 */
/*
 * TODO: Think about moving the functionality of allocate_disk_structures
 *       in here, it should be possible to do that and would simplify things.
 */

static int
transfer_disk_structures (UNUSED_ARG MOP classop, SM_CLASS * class_,
			  SM_TEMPLATE * flat)
{
  int error = NO_ERROR;
  SM_DISK_CONSTRAINT *flat_disk_cons, *disk_cons, *new_disk_cons, *prev,
    *next;
  SM_DISK_CONSTRAINT_ATTRIBUTE *disk_att;
  SM_CLASS_CONSTRAINT *cons;
  SM_ATTRIBUTE *att = NULL;
  int num_pk;

  /* init locals */
  flat_disk_cons = NULL;

  /* Get the cached constraint info for the flattened template.
   * Sigh, convert the template property list to a transient constraint
   * cache so we have a prayer of dealing with it.
   */
  if (flat != NULL)
    {
      flat_disk_cons = flat->disk_constraints;

#if !defined(NDEBUG)
      for (disk_cons = flat_disk_cons; disk_cons != NULL;
	   disk_cons = disk_cons->next)
	{
	  assert (SM_IS_CONSTRAINT_INDEX_FAMILY (disk_cons->type));
	}
#endif

      assert (class_->class_type == flat->class_type);
    }

  /* loop over each old constraint */
  for (cons = class_->constraints; cons != NULL; cons = cons->next)
    {
      assert (cons->attributes != NULL);

      if (!SM_IS_CONSTRAINT_INDEX_FAMILY (cons->type))
	{
	  continue;
	}

      new_disk_cons = classobj_find_disk_constraint (flat_disk_cons,
						     cons->name);
      if (new_disk_cons == NULL)
	{
	  /* Constraint does not exist in the template */
	  if (cons->attributes[0] != NULL)
	    {
	      /* destroy the old index but only if we're the owner of
	       * it!
	       */
	      error = deallocate_index (class_->constraints,
					&cons->index_btid);
	      if (error != NO_ERROR)
		{
		  goto end;
		}

	      BTID_SET_NULL (&cons->index_btid);
	    }
	}
      else if (!BTID_IS_EQUAL (&cons->index_btid, &new_disk_cons->index_btid))
	{
	  if (BTID_IS_NULL (&(new_disk_cons->index_btid)))
	    {
	      /* Template index isn't set, transfer the old one
	       * Can this happen, it should have been transfered by now.
	       */

	      BTID_COPY (&new_disk_cons->index_btid, &cons->index_btid);
	    }
	  else
	    {
	      /* The index in the new template is not the same, I'm not entirely
	       * sure what this means or how we can get here.
	       * Possibly if we drop the unique but add it again with the same
	       * name but over different attributes.
	       */
	      if (cons->attributes[0] != NULL)
		{
		  error = deallocate_index (class_->constraints,
					    &cons->index_btid);
		  if (error != NO_ERROR)
		    {
		      goto end;
		    }
		  BTID_SET_NULL (&cons->index_btid);
		}
	    }
	}
    }

  /* Filter out any constraints that don't have associated attributes,
   * this is normally only the case for old constraints whose attributes
   * have been deleted.
   */
  prev = next = NULL;
  for (disk_cons = flat_disk_cons; disk_cons != NULL; disk_cons = next)
    {
      next = disk_cons->next;

      if (disk_cons->index_status != INDEX_STATUS_IN_PROGRESS
	  && disk_cons->disk_info_of_atts != NULL)
	{
	  for (disk_att = disk_cons->disk_info_of_atts; disk_att != NULL;
	       disk_att = disk_att->next)
	    {
	      if (disk_att->att_id >= 0)
		{
		  att = classobj_find_attribute_id (flat->instance_attributes,
						    disk_att->att_id);
		}
	      else
		{
		  att = classobj_find_attribute (flat->instance_attributes,
						 disk_att->name);
		}

	      if (att == NULL)
		{
		  break;
		}
	    }

	  if (disk_att == NULL)
	    {
	      /* found all attribute */
	      prev = disk_cons;
	      continue;
	    }
	}

      /* remove from flat_disk_cons */
      if (prev == NULL)
	{
	  /* first node */
	  flat_disk_cons = disk_cons->next;
	}
      else
	{
	  prev->next = disk_cons->next;
	}
      disk_cons->next = NULL;

      if (!BTID_IS_NULL (&disk_cons->index_btid))
	{
	  error =
	    deallocate_index (class_->constraints, &disk_cons->index_btid);
	  if (error != NO_ERROR)
	    {
	      goto end;
	    }
	  BTID_SET_NULL (&disk_cons->index_btid);
	}
      classobj_free_disk_constraint (disk_cons);
      disk_cons = NULL;
    }

  if (flat != NULL)
    {
      num_pk = 0;		/* init */
      for (disk_cons = flat_disk_cons; disk_cons != NULL;
	   disk_cons = disk_cons->next)
	{
	  if (disk_cons->type == SM_CONSTRAINT_PRIMARY_KEY)
	    {
	      if (num_pk != 0)
		{
		  error = ER_SM_PRIMARY_KEY_EXISTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  error, 2, class_->header.name, disk_cons->name);
		  goto end;
		}
	      num_pk++;
	    }
	}

      /* ignore iff createdb */
      if (db_get_client_type () != BOOT_CLIENT_CREATEDB)
	{
	  if (flat->class_type == SM_CLASS_CT && num_pk <= 0)
	    {
#if 1				/* TODO - add PK */
	      if (prm_get_bool_value (PRM_ID_TEST_MODE) == false)
#endif
		{
		  /* not permit table without PK */
		  error = ER_SM_PRIMARY_KEY_NOT_EXISTS;
		  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
			  error, 1, class_->header.name);
		  goto end;
		}
	    }
	}

      flat->disk_constraints = flat_disk_cons;
    }

end:

  return error;
}

/*
 * install_new_representation() - Final installation of a class template.
 *    It is necessary to guarantee that this is an atomic operation and
 *    the workspace will not change while this executes.
 *    Garbage collection should be disabled while this happens
 *    although we keep MOP cached in structures everywhere so it won't
 *    make a difference.
 *    This is essentially the "commit" operation of a schema modification,
 *    be VERY sure you know what you're doing if you change this
 *    code.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 *   class(in): class structure
 *   flat(out): flattened template
 */

static int
install_new_representation (MOP classop, SM_CLASS * class_,
			    SM_TEMPLATE * flat)
{
  int error = NO_ERROR;
  int needrep, newrep;

  assert (classop != NULL);

  if (classop == NULL)
    {
      return ER_FAILED;
    }

  /* now that we're ready, make sure attribute are stamped with the
     proper class mop */
  fixup_component_classes (classop, flat);

  /* go through and replace kludged "self referencing" domain with a proper
     domain containing the new class MOP */
  fixup_self_reference_domains (classop, flat);

  /* assign attribute ids and check for structural representation changes */
  needrep = build_storage_order (class_, flat);

  /* if the representation changed but there have been no objects
     created with the previous representation, don't create a new one,
     otherwise, flush all resident instances */
  newrep = 0;
  if (needrep)
    {
      /* check for error on each of the locator functions,
       * an error can happen if we run out of space during flushing.
       */
      if (!classop->no_objects)
	{
	  switch (class_->class_type)
	    {
	    case SM_CLASS_CT:
	      if (locator_flush_all_instances (classop, DECACHE) != NO_ERROR)
		{
		  return (er_errid ());
		}
	      break;

	    case SM_VCLASS_CT:
	      break;

	    default:
	      break;
	    }

	  /* note that the previous operation will flush the current class
	   * representation along with the instances and clear the dirty bit,
	   * this is unnecessary if the class was only marked dirty
	   * in preparation for the new representation.
	   * Because the dirty bit is clear however, we must turn it back on
	   * after the new representation is installed so it will be properly
	   * flushed, the next time a transaction commits or
	   * locator_flush_all_instances is called
	   */
	  if (locator_update_class (classop) == NULL)
	    {
	      return (er_errid ());
	    }

	  /* !!! I've seen some cases where objects are left cached while this
	   * flag is on which is illegal.  Not sure how this happens but leave
	   * this trap so we can track it down.  Shouldn't be necessary
	   */
	  if (ws_class_has_cached_objects (classop))
	    {
	      ERROR0 (error, ER_SM_CORRUPTED);
	      return error;
	    }

	  newrep = 1;

	  /* Set the no_objects flag so we know that if no object dependencies
	   * are introduced on this representation, we don't have to generate
	   * another one the next time the class is updated.
	   */

	  /* this used to be outside, think about why */
	  WS_SET_NO_OBJECTS (classop);
	}
      else
	{
	  newrep = 1;
	}
    }

  error = transfer_disk_structures (classop, class_, flat);
  if (error != NO_ERROR)
    {
      return error;
    }

#if defined (ENABLE_UNUSED_FUNCTION)
  /* clear any attribute descriptor caches that reference this
   * class.
   */
  sm_reset_descriptors (classop);
#endif

  /* install the template, the dirty bit must be on at this point */
  error = classobj_install_template (class_, flat, newrep);
  if (error != NO_ERROR)
    {
      return error;
    }

  /* make absolutely sure this gets marked dirty after the installation,
   * this is usually redundant but the class could get flushed
   * during memory panics so we always must make sure it gets flushed again
   */
  if (locator_update_class (classop) == NULL)
    {
      return er_errid ();
    }

  /* If the representation was incremented, invalidate any existing
   * statistics cache.  The next time statistics are requested, we'll
   * go to the server and get them based on the new catalog information.
   * This probably isn't necessary in all cases but let's be safe and
   * waste it unconditionally.
   */
  if (newrep && class_->stats != NULL)
    {
      stats_free_statistics (class_->stats);
      class_->stats = NULL;
    }

  /* formerly had classop->no_objects = 1 here, why ? */

  return error;
}

/*
 * check_catalog_space() - Checks to see if the catalog manager is able to
 *    handle another representation for this class.  There is a fixed limit
 *    on the number of representations that can be stored in the catalog
 *    for each class.  If this limit is reached, the schema operation
 *    cannot be performed until the database is compacted.
 *    Note that this needs only be called when a schema operation
 *    will actually result in the generation of a new catalog entry.
 *    Since this won't be a problem very often, its also ok just to check
 *    it up front even if the operation may not result in the generation
 *    of a new representation.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classmop(in): class pointer
 *   class(in): class structure
 */

static int
check_catalog_space (MOP classmop, SM_CLASS * class_)
{
  int error = NO_ERROR;
  int status, can_accept;

  /* if the OID is temporary, then we haven't flushed the class yet
     and it isn't necessary to check since there will be no
     existing entries in the catalog */

  if (!OID_ISTEMP (WS_OID (classmop)))
    {

      /* if the oid is permanent, we still may not have flushed the class
         because the OID could have been assigned during the transformation
         of another object that referenced this class.
         In this case, the catalog manager will return ER_CT_UNKNOWN_CLASSID
         because it will have no entries for this class oid.
       */

      status =
	catalog_is_acceptable_new_representation (WS_OID (classmop),
						  &class_->header.heap,
						  &can_accept);
      if (status != NO_ERROR)
	{
	  error = er_errid ();
	  /* ignore if if the class hasn't been flushed yet */
	  if (error == ER_CT_UNKNOWN_CLASSID)
	    {
	      /* if dirty bit isn't on in this MOP, its probably an internal error */
	      error = NO_ERROR;
	    }
	}
      else if (!can_accept)
	{
	  ERROR1 (error, ER_SM_CATALOG_SPACE, class_->header.name);
	}
    }

  return error;
}

/*
 * update_class() - Apply a schema template for a new or existing class.
 *    If there is an error in the local class
 *    because of a change in the template, none of the changes in the
 *    template will be applied.
 *    Even if there were  no errors detected during the building of the
 *    template, there still may be some outstanding errors detected
 *    during actual flattening that will cause application of the template
 *    to fail.
 *    If the returned error status is zero, the template application
 *    was successful and the template was freed and can no longer be used.
 *    If the returned error status indicates a problem locking an affected
 *    object, you either abort the template or wait and try again later.
 *    If there is another error in the template, you can either abort
 *    the template or alter the template and try again.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   template(in): schema template
 *   classmop(in): MOP of existing class (NULL if new class)
 *   auto_res(in): non-zero to enable auto-resolution of conflicts
 *   verify_oid(in): whether to verify object id
 */

/* NOTE: There were some problems when the transaction was unilaterally
 * aborted in the middle of a schema change operation.  What happens is
 * that during flattening, each class structure is given a pointer to
 * the flattened template.  If the transaction is aborted, all of the
 * dirty objects in the workspace would be flushed.  classobj_free_class()
 * would always free an associated template if one was present.  When
 * that happened, we would get back from some function, find and error,
 * but not realize that our template had been freed out from under us.
 *
 * The simplest solution to this problem is to prevent classobj_free_class()
 * from freeing templates.  This is ok in the normal case but in the
 * event of a unilateral abort, we may end up with some memory leaks as
 * the templates that had been attached to the classes will be lost.
 *
 * Fixing this to avoid this leak will be complicated and the likelihood
 * of this problem is very remote.
 */

static int
update_class (SM_TEMPLATE * template_, MOP * classmop)
{
  int error = NO_ERROR;
  SM_CLASS *class_ = NULL;
  SM_TEMPLATE *flat = NULL;
  bool do_rollback_on_error = false;
  int num_allocated_index;

  sm_bump_local_schema_version ();

  assert (template_ != NULL);

  if ((template_->op != NULL))
    {
      /* existing class, fetch it */
      assert (ws_get_lock (template_->op) == X_LOCK
	      || ws_get_lock (template_->op) == U_LOCK);

      error = au_fetch_class (template_->op, &class_,
			      template_->op->ws_lock, AU_ALTER);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}

      /* make sure the catalog manager can deal with another representation */
      error = check_catalog_space (template_->op, class_);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  /* flatten template, store the pending template in the "new" field
     of the class in case we need it to make domain comparisons */
  if (class_ != NULL)
    {
      class_->new_ = template_;
    }

  error = flatten_template (template_, NULL, &flat);
  if (error != NO_ERROR)
    {
      /* If we aborted the operation (error == ER_LK_UNILATERALLY_ABORTED)
         then the class may no longer be in the workspace.  So make sure
         that the class exists before using it.  */
      if (class_ != NULL && error != ER_LK_UNILATERALLY_ABORTED)
	{
	  class_->new_ = NULL;
	}

      GOTO_EXIT_ON_ERROR;
    }

  if (class_ != NULL)
    {
      class_->new_ = flat;
    }

  do_rollback_on_error = true;

  /* now we can assume that every class we need to touch has a write
     lock - proceed with the installation of the changes */

  /* are we creating a new class ? */
  if (class_ == NULL)
    {
      class_ = classobj_make_class (template_->name);
      if (class_ == NULL)
	{
	  error = er_errid ();

	  GOTO_EXIT_ON_ERROR;
	}

      class_->class_type = flat->class_type;
      class_->owner = au_get_user ();	/* remember the owner id */

      /* NOTE: Garbage collection can occur in the following function
         as a result of the allocation of the class MOP.  We must
         ensure that there are no object handles in the SM_CLASS structure
         at this point that don't have roots elsewhere.  Currently, this
         is the case since we are simply caching a newly created empty
         class structure which will later be populated with
         install_new_representation.  The template that holds
         the new class contents IS already a GC root.
       */
      template_->op = locator_add_class ((MOBJ) class_,
					 (char *) class_->header.name);
      if (template_->op == NULL)
	{
	  /* return locator error code */
	  error = er_errid ();

	  GOTO_EXIT_ON_ERROR;
	}
    }

  /* the next sequence of operations is extremely critical,
     if any errors are detected, we'll have to abort the current
     transaction or the database will be left in an inconsistent
     state */

  error = install_new_representation (template_->op, class_, flat);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  num_allocated_index = 0;
  error = allocate_disk_structures (template_->op, class_,
				    &num_allocated_index);
  if (error != NO_ERROR)
    {
      GOTO_EXIT_ON_ERROR;
    }

  if (num_allocated_index > 0)
    {
      do_rollback_on_error = false;
      error = load_index_data (template_->op, class_);
      if (error != NO_ERROR)
	{
	  GOTO_EXIT_ON_ERROR;
	}
    }

  if (classmop != NULL)
    {
      *classmop = template_->op;
    }

  /* we're done */
  class_->new_ = NULL;

  classobj_free_template (flat);
  classobj_free_template (template_);

  return NO_ERROR;

exit_on_error:

  if (error == NO_ERROR)
    {
      assert (error != NO_ERROR);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
    }

  if (flat != NULL)
    {
      classobj_free_template (flat);
      flat = NULL;
    }

  if (do_rollback_on_error == false)
    {
      tran_commit ();
    }

  return error;
}

/*
 * smt_finish_class() - this is called to finish a dbt template.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   template(in): schema template
 *   classmop(in): MOP of existing class (NULL if new class)
 */

int
smt_finish_class (SM_TEMPLATE * template_, MOP * classmop)
{
  return update_class (template_, classmop);
}

/*
 * sm_delete_class() - This will delete a class from the schema and
 *    delete all instances of the class from the database.  All classes that
 *    inherit from this class will be updated so that inherited components
 *    are removed.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   op(in): class object
 */

int
sm_delete_class_mop (MOP op)
{
  int error = NO_ERROR;
  SM_CLASS *class_;
  SM_TEMPLATE *template_;

  template_ = NULL;		/* init */

  if (op == NULL)
    {
      assert (false);
      return ER_FAILED;
    }

  sm_bump_local_schema_version ();

  /* op should be a class */
  if (!locator_is_class (op))
    {
      ERROR0 (error, ER_OBJ_NOT_A_CLASS);

      goto end;
    }

  error = au_fetch_class (op, &class_, X_LOCK, AU_ALTER);
  if (error != NO_ERROR)
    {
      goto end;
    }

  /* we don't really need this but some of the support routines use it */
  template_ = classobj_make_template (NULL, op, class_);
  if (template_ == NULL)
    {
      error = er_errid ();
      goto end;
    }

  /* flush all instances of this class */
  switch (class_->class_type)
    {
    case SM_CLASS_CT:
      if (locator_flush_all_instances (op, DECACHE) != NO_ERROR)
	{
	  error = er_errid ();
	}
      break;

    case SM_VCLASS_CT:
      break;

    default:
      break;
    }

  if (error != NO_ERROR)
    {
      /* we had problems flushing, this may be due to an out of space
       * condition, probably the transaction should be aborted as well
       */
      goto end;
    }

  /* OLD CODE, here we removed the class from the resident class list,
   * this causes bad problems for GC since the class will be GC'd before
   * instances have been decached. This operation has been moved below
   * with ws_remove_resident_class(). Not sure if this is position dependent.
   * If it doesn't cause any problems remove this comment.
   */
  /* ml_remove(&ws_Resident_classes, op); */

  /* free any indexes, unique btids, or other associated disk structures */
  error = transfer_disk_structures (op, class_, NULL);
  if (error != NO_ERROR)
    {
      goto end;
    }

  /* This to be maintained as long as the class is cached in the workspace,
   * dirty or not. When the deleted class is flushed, the name is removed.
   * Assuming this doesn't cause problems, remove this comment
   */
  /* ws_drop_classname((MOBJ) class); */

  /* inform the locator - this will mark the class MOP as deleted so all
   * operations that require the current class object must be done before
   * calling this function
   */

  error = locator_remove_class (op);
  if (error != NO_ERROR)
    {
      goto end;
    }

  /* mark all instance MOPs as deleted, should the locator be doing this ? */

  ws_mark_instances_deleted (op);

  /* make sure this is removed from the resident class list, this will also
   * make the class mop subject to garbage collection. This function will
   * expect that all of the instances of the class have been decached
   * by this point !
   */

  ws_remove_resident_class (op);

end:

  classobj_free_template (template_);

  return error;
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * sm_delete_class() - Delete a class by name.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   name(in): class name
 */

int
sm_delete_class (const char *name)
{
  int error = NO_ERROR;
  MOP classop;

  classop = sm_find_class (name);
  if (classop == NULL)
    {
      error = er_errid ();
    }
  else
    {
      error = sm_delete_class_mop (classop);
    }

  return error;
}
#endif /* ENABLE_UNUSED_FUNCTION */

/* INDEX FUNCTIONS */

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * These are in here bacause they share some of the internal
 * allocation/deallocation for indexes.
 * They also play games with the representation id so the
 * catalog gets updated correctly to include the new index.
*/
/*
 * sm_exist_index() - Checks to see if an index exist
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 *   idxname(in): index name
 */
static int
sm_exist_index (MOP classop, const char *idxname, BTID * btid)
{
  int error = NO_ERROR;
  SM_CLASS *class_;
  SM_CLASS_CONSTRAINT *cons;

  error = au_fetch_class (classop, &class_, S_LOCK, AU_SELECT);
  if (error == NO_ERROR)
    {
      cons = classobj_find_class_index (class_, idxname);
      if (cons)
	{
	  if (btid)
	    {
	      BTID_COPY (btid, &cons->index_btid);
	    }

	  return NO_ERROR;
	}
    }

  return ER_FAILED;
}

/*
 * sm_get_index() - Checks to see if an attribute has an index and if so,
 *    returns the BTID of the index.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class object
 *   attname(in): attribute name
 *   index(out): returned pointer to index
 */

int
sm_get_index (MOP classop, const char *attname, BTID * index)
{
  int error = NO_ERROR;
  SM_CLASS *class_;
  SM_ATTRIBUTE *att;

  /* what happens if we formerly indexed the attribute, revoked index
     authorization and now want to remove it ? */

  error = au_fetch_class (classop, &class_, S_LOCK, AU_SELECT);
  if (error == NO_ERROR)
    {
      att = classobj_find_attribute (class_->attributes, attname);
      if (att == NULL)
	{
	  ERROR1 (error, ER_SM_ATTRIBUTE_NOT_FOUND, attname);
	}
      else
	{
	  SM_CONSTRAINT *con;
	  int found = 0;

	  /*  First look for the index in the attribute constraint cache */
	  for (con = att->constraints; ((con != NULL) && !found);
	       con = con->next)
	    {
	      if (SM_IS_CONSTRAINT_INDEX_FAMILY (con->type))
		{
		  *index = con->index;
		  found = 1;
		}
	    }
	}
    }

  return error;
}
#endif

/*
 * sm_default_constraint_name() - Constructs a constraint name based upon
 *    the class and attribute names and names' asc/desc info.
 *    Returns the constraint name or NULL is an error occurred.  The string
 *    should be deallocated with free_and_init() when no longer needed.
 *    The class name is normally obtained from the Class Object.  This is
 *    not always possible, though (for instance at class creation time,
 *    there is no class object and <classop> will be NULL).  Under this
 *    condition, the class name will be taken from the Class Template
 *    <ctmpl>.
 *    The format of the default name is;
 *        X_<class>_att1_att2_... or
 *        X_<class>_att1_d_att2_...  --> _d implies that att1 order is 'desc'
 *    where X indicates the constraint type;
 *          i=INDEX,            u=UNIQUE,       pk=PRIMARY KEY,
 *          fk=FOREIGN KEY,     n=NOT NULL
 *          <class> is the class name
 *          attn is the attribute name
 *    (ex)  If we are generating a default name for
 *              create index on foo (a, b);
 *          It would look like i_foo_a_b
 *    (ex)  If we are generating a default name for
 *              create index on foo (a desc, b);
 *          It would look like i_foo_a_d_b --> use '_d' for 'desc'
 *   return: constraint name
 *   class_name(in): class name
 *   type(in): Constraint Type
 *   att_names(in): Attribute Names
 *   asc_desc(in): asc/desc info list
 */

static char *
sm_default_constraint_name (const char *class_name,
			    DB_CONSTRAINT_TYPE type,
			    const char **att_names, const int *asc_desc)
{
#define MAX_ATTR_IN_AUTO_GEN_NAME 30
  const char **ptr;
  char *name = NULL;
  int name_length = 0;
  bool do_desc;
  UNUSED_VAR int error = NO_ERROR;
  int n_attrs = 0;
  /*
   *  Construct the constraint name
   */
  if ((class_name == NULL) || (att_names == NULL))
    {
      ERROR0 (error, ER_SM_INVALID_DEF_CONSTRAINT_NAME_PARAMS);
    }
  else
    {
      const char *prefix;
      int i, k;
      int class_name_prefix_size = DB_MAX_IDENTIFIER_LENGTH;
      int att_name_prefix_size = DB_MAX_IDENTIFIER_LENGTH;
      char md5_str[32 + 1] = {
	'\0'
      };

      /* Constraint Type */
      prefix = (type == DB_CONSTRAINT_INDEX) ? "i_" :
	(type == DB_CONSTRAINT_UNIQUE) ? "u_" :
	(type == DB_CONSTRAINT_PRIMARY_KEY) ? "pk_" :
	(type == DB_CONSTRAINT_NOT_NULL) ? "n_" :
	/*          UNKNOWN TYPE            */ "x_";

      /*
       *  Count the number of characters that we'll need for the name
       */
      name_length = strlen (prefix);
      name_length += strlen (class_name);	/* class name */

      for (ptr = att_names; *ptr != NULL; ptr++)
	{
	  n_attrs++;
	}

      if (n_attrs <= 0)
	{
	  /* this should not happen */
	  assert (false);
	  ERROR0 (error, ER_SM_INVALID_DEF_CONSTRAINT_NAME_PARAMS);
	  name = NULL;
	  goto exit;
	}

      i = 0;
      for (ptr = att_names; (*ptr != NULL) && (i < n_attrs); ptr++, i++)
	{
	  int ptr_size = 0;

	  do_desc = false;	/* init */
	  if (asc_desc && asc_desc[i] == 1)
	    {
	      do_desc = true;
	    }

	  ptr_size = strlen (*ptr);
	  name_length += (1 + ptr_size);	/* separator and attr name */
	  if (do_desc)
	    {
	      name_length += 2;	/* '_d' for 'desc' */
	    }
	}			/* for (ptr = ...) */

      if (name_length >= DB_MAX_IDENTIFIER_LENGTH)
	{
	  /* constraint name will contain a descriptive prefix + prefixes of
	   * class name + prefixes of the first MAX_ATTR_IN_AUTO_GEN_NAME
	   * attributes + MD5 of the entire string of concatenated class name
	   * and attributes names */
	  char *name_all = NULL;
	  int size_class_and_attrs =
	    DB_MAX_IDENTIFIER_LENGTH - 1 - strlen (prefix) - 32 - 1;

	  name_all = (char *) malloc (name_length + 1);
	  if (name_all == NULL)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_OUT_OF_VIRTUAL_MEMORY, 1, name_length + 1);
	      goto exit;
	    }
	  strcpy (name_all, class_name);
	  for (ptr = att_names, i = 0; i < n_attrs; ptr++, i++)
	    {
	      strcat (name_all, *ptr);
	      if (asc_desc && asc_desc[i] == 1)
		{
		  strcat (name_all, "d");
		}
	    }

	  md5_buffer (name_all, strlen (name_all), md5_str);
	  md5_hash_to_hex (md5_str, md5_str);

	  free_and_init (name_all);

	  if (n_attrs > MAX_ATTR_IN_AUTO_GEN_NAME)
	    {
	      n_attrs = MAX_ATTR_IN_AUTO_GEN_NAME;
	    }

	  att_name_prefix_size = size_class_and_attrs / (n_attrs + 1);
	  class_name_prefix_size = att_name_prefix_size;

	  if (strlen (class_name) < class_name_prefix_size)
	    {
	      class_name_prefix_size = strlen (class_name);
	    }
	  else
	    {
	      char class_name_trunc[DB_MAX_IDENTIFIER_LENGTH];

	      strncpy (class_name_trunc, class_name, class_name_prefix_size);
	      class_name_trunc[class_name_prefix_size] = '\0';

	      /* make sure last character is not truncated */
	      if (intl_identifier_fix (class_name_trunc,
				       class_name_prefix_size) != NO_ERROR)
		{
		  /* this should not happen */
		  assert (false);
		  ERROR0 (error, ER_SM_INVALID_DEF_CONSTRAINT_NAME_PARAMS);
		  name = NULL;
		  goto exit;
		}
	      class_name_prefix_size = strlen (class_name_trunc);
	    }

	  /* includes '_' between attributes */
	  att_name_prefix_size =
	    ((size_class_and_attrs - class_name_prefix_size) / n_attrs) - 1;
	  name_length = DB_MAX_IDENTIFIER_LENGTH;
	}
      /*
       *  Allocate space for the name and construct it
       */
      name = (char *) malloc (name_length + 1);	/* Remember terminating NULL */
      if (name != NULL)
	{
	  /* Constraint Type */
	  strcpy (name, prefix);

	  /* Class name */
	  strncat (name, class_name, class_name_prefix_size);

	  /* separated list of attribute names */
	  k = 0;
	  i = 0;
	  /* n_attrs is already limited to MAX_ATTR_IN_AUTO_GEN_NAME here */
	  for (ptr = att_names; k < n_attrs; ptr++, i++)
	    {
	      do_desc = false;	/* init */
	      if (asc_desc && asc_desc[i] == 1)
		{
		  do_desc = true;
		}

	      strcat (name, "_");

	      if (att_name_prefix_size == DB_MAX_IDENTIFIER_LENGTH)
		{
		  (void) intl_identifier_lower (*ptr, &name[strlen (name)]);

		  /* attr is marked as 'desc' */
		  if (do_desc)
		    {
		      strcat (name, "_d");
		    }
		}
	      else
		{
		  char att_name_trunc[DB_MAX_IDENTIFIER_LENGTH];

		  (void) intl_identifier_lower (*ptr, att_name_trunc);

		  if (do_desc)
		    {
		      /* make sure last character is not truncated */
		      assert (att_name_prefix_size > 2);
		      if (intl_identifier_fix (att_name_trunc,
					       att_name_prefix_size - 2)
			  != NO_ERROR)
			{
			  assert (false);
			  ERROR0 (error,
				  ER_SM_INVALID_DEF_CONSTRAINT_NAME_PARAMS);
			  free_and_init (name);
			  goto exit;
			}
		      strcat (att_name_trunc, "_d");
		    }
		  else
		    {
		      if (intl_identifier_fix (att_name_trunc,
					       att_name_prefix_size)
			  != NO_ERROR)
			{
			  assert (false);
			  ERROR0 (error,
				  ER_SM_INVALID_DEF_CONSTRAINT_NAME_PARAMS);
			  free_and_init (name);
			  goto exit;
			}
		    }

		  strcat (name, att_name_trunc);
		}
	      k++;
	    }

	  if (att_name_prefix_size != DB_MAX_IDENTIFIER_LENGTH
	      || class_name_prefix_size != DB_MAX_IDENTIFIER_LENGTH)
	    {
	      /* append MD5 */
	      strcat (name, "_");
	      strcat (name, md5_str);

	      assert (strlen (name) <= DB_MAX_IDENTIFIER_LENGTH);
	    }
	}
      else
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, name_length + 1);
	}
    }

exit:
  return name;

#undef MAX_ATTR_IN_AUTO_GEN_NAME
}

/*
 * sm_produce_constraint_name() - Generate a normalized constraint name.
 *    If a constraint name is given <given_name> != NULL, then this name is
 *    downcased and returned. In this case, the constraint type and attribute
 *    names are not needed.
 *    If a given name is not provided <given_name> == NULL, then a
 *    normalized name is generated using the constraint type and attribute
 *    names.
 *    In either case, the returned name is generated its own memory area
 *    and should be deallocated with by calling free()
 *    when it is no longer needed.
 *    This function differs from sm_produce_constraint_name_mop() in that
 *    the class name is supplied as a parameters and therefore, does not
 *    need to be derived.
 *   return: constraint name
 *   class_name(in): Class Name
 *   constraint_type(in): Constraint Type
 *   att_names(in): Attribute Names
 *   asc_desc(in): asc/desc info list
 *   given_name(in): Optional constraint name.
 */

char *
sm_produce_constraint_name (const char *class_name,
			    DB_CONSTRAINT_TYPE constraint_type,
			    const char **att_names,
			    const int *asc_desc, const char *given_name)
{
  char *name = NULL;
  size_t name_size;

  if (given_name == NULL)
    {
      name = sm_default_constraint_name (class_name, constraint_type,
					 att_names, asc_desc);
    }
  else
    {
      name_size = strlen (given_name);
      name = (char *) malloc (name_size + 1);
      if (name != NULL)
	{
	  intl_identifier_lower (given_name, name);
	}
      else
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, name_size + 1);
	}
    }

  return name;
}

/*
 * sm_produce_constraint_name_mop() - This function serves the same
 *    functionality as sm_produce_constraint_name() except that it accepts
 *    a class MOP instead of a class name.
 *   return: constraint name
 *   classop(in): Class Object
 *   constraint_type(in): Constraint Type
 *   att_names(in): Attribute Names
 *   given_name(in): Optional constraint name.
 */

char *
sm_produce_constraint_name_mop (MOP classop,
				DB_CONSTRAINT_TYPE constraint_type,
				const char **att_names,
				const int *asc_desc, const char *given_name)
{
  return sm_produce_constraint_name (sm_class_name (classop), constraint_type,
				     att_names, asc_desc, given_name);
}

/*
 * sm_produce_constraint_name_tmpl() - This function serves the same
 *    functionality as sm_produce_constraint_name() except that it accepts
 *    a class template instead of a class name.
 *   return: constraint name
 *   tmpl(in): Class Template
 *   constraint_type(in): Constraint Type
 *   att_names(in): Attribute Names
 *   given_name(in): Optional constraint name.
 */
char *
sm_produce_constraint_name_tmpl (SM_TEMPLATE * tmpl,
				 DB_CONSTRAINT_TYPE constraint_type,
				 const char **att_names,
				 const int *asc_desc, const char *given_name)
{
  return sm_produce_constraint_name (template_classname (tmpl),
				     constraint_type, att_names, asc_desc,
				     given_name);
}

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * sm_check_index_exist() - Check index is duplicated.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class (or instance) pointer
 *   constraint_type: constraint type
 *   constraint_name(in): Constraint name.
 */
static int
sm_check_index_exist (MOP classop,
		      DB_CONSTRAINT_TYPE constraint_type,
		      const char *constraint_name)
{
  int error = NO_ERROR;
  SM_CLASS *class_;

  if (!DB_IS_CONSTRAINT_INDEX_FAMILY (constraint_type))
    {
      return NO_ERROR;
    }

  error = au_fetch_class (classop, &class_, S_LOCK, AU_ALTER);
  if (error != NO_ERROR)
    {
      return error;
    }

  return classobj_check_index_exist (class_->constraints,
				     class_->header.name, constraint_name);
}
#endif

/*
 * sm_add_constraint() - Add a constraint to the class.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class (or instance) pointer
 *   constraint_type(in): Type of constraint to add (UNIQUE, NOT NULL or INDEX)
 *   constraint_name(in): What to call the new constraint
 *   att_names(in): Names of attributes to be constrained
 *   asc_desc(in): asc/desc info list
 *
 *  Note: When adding NOT NULL constraint, this function doesn't check the
 *	  existing values of the attribute. To make sure NOT NULL constraint
 *	  checks the existing values, use API function 'db_add_constraint'.
 */
int
sm_add_constraint (MOP classop, DB_CONSTRAINT_TYPE constraint_type,
		   const char *constraint_name, const char **att_names,
		   const int *asc_desc)
{
  int error = NO_ERROR;
  SM_TEMPLATE *def;
  MOP newmop = NULL;

  if (att_names == NULL)
    {
      ERROR0 (error, ER_OBJ_INVALID_ARGUMENTS);
      return error;
    }

  switch (constraint_type)
    {
    case DB_CONSTRAINT_INDEX:
    case DB_CONSTRAINT_UNIQUE:
    case DB_CONSTRAINT_PRIMARY_KEY:
      def = smt_edit_class_mop_with_lock (classop, U_LOCK);
      if (def == NULL)
	{
	  error = er_errid ();
	}
      else
	{
	  error = smt_add_constraint (def, constraint_type, constraint_name,
				      att_names, asc_desc);
	  if (error == NO_ERROR)
	    {
	      error = smt_finish_class (def, &newmop);
	    }

	  if (error == NO_ERROR)
	    {
	      error = sm_update_statistics (newmop, true /* update_stats */ ,
					    STATS_WITH_SAMPLING);
	    }
	  else
	    {
	      smt_quit (def);
	    }
	}
      break;

    case DB_CONSTRAINT_NOT_NULL:
      def = smt_edit_class_mop_with_lock (classop, X_LOCK);
      if (def == NULL)
	{
	  error = er_errid ();
	}
      else
	{
	  error = smt_add_constraint (def, constraint_type, constraint_name,
				      att_names, asc_desc);
	  if (error == NO_ERROR)
	    {
	      error = smt_finish_class (def, NULL);
	    }

	  if (error == NO_ERROR)
	    {
	      /* don't have to update stats for NOT NULL
	       */
	    }
	  else
	    {
	      smt_quit (def);
	    }
	}
      break;

    default:
      break;
    }

  return error;
}

/*
 * sm_drop_constraint() - Drops a constraint from a class.
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classop(in): class (or instance) pointer
 *   constraint_type(in): Type of constraint to drop (UNIQUE, PK, NOT NULL or
 *                        INDEX). Foreign keys are not dropped by this
 *                        function. See dbt_drop_constraint instead.
 *   constraint_name(in): The name of the constraint to drop
 *   att_names(in): Names of attributes the constraint is defined on
 */
int
sm_drop_constraint (MOP classop,
		    DB_CONSTRAINT_TYPE constraint_type,
		    const char *constraint_name, const char **att_names)
{
  int error = NO_ERROR;
  SM_TEMPLATE *def = NULL;

  switch (constraint_type)
    {
    case DB_CONSTRAINT_INDEX:
    case DB_CONSTRAINT_UNIQUE:
    case DB_CONSTRAINT_PRIMARY_KEY:
      def = smt_edit_class_mop_with_lock (classop, X_LOCK);
      if (def == NULL)
	{
	  error = er_errid ();
	}
      else
	{
	  error = smt_drop_constraint (def, att_names, constraint_name,
				       SM_MAP_CONSTRAINT_TO_ATTFLAG
				       (constraint_type));

	  if (error == NO_ERROR)
	    {
	      error = smt_finish_class (def, NULL);
	    }

	  if (error != NO_ERROR)
	    {
	      smt_quit (def);
	    }
	}
      break;

    case DB_CONSTRAINT_NOT_NULL:
      def = smt_edit_class_mop_with_lock (classop, X_LOCK);
      if (def == NULL)
	{
	  error = er_errid ();
	}
      else
	{
	  error = smt_drop_constraint (def, att_names, constraint_name,
				       SM_ATTFLAG_NON_NULL);
	  if (error == NO_ERROR)
	    {
	      error = smt_finish_class (def, NULL);
	    }

	  if (error != NO_ERROR)
	    {
	      smt_quit (def);
	    }
	}
      break;

    default:
      break;
    }

  return error;
}

/*
 * sm_free_constraint_info() - Frees a SM_CONSTRAINT_INFO list
 *   save_info(in/out): The list to be freed
 * NOTE: the pointer to the list is set to NULL after the list is freed.
 */
void
sm_free_constraint_info (SM_CONSTRAINT_INFO ** save_info)
{
  SM_CONSTRAINT_INFO *info = NULL;

  if (save_info == NULL || *save_info == NULL)
    {
      return;
    }

  info = *save_info;
  while (info != NULL)
    {
      SM_CONSTRAINT_INFO *next = info->next;
      char **crt_name_p = NULL;

      for (crt_name_p = info->att_names; *crt_name_p != NULL; ++crt_name_p)
	{
	  free_and_init (*crt_name_p);
	}
      free_and_init (info->att_names);

      if (info->ref_attrs != NULL)
	{
	  for (crt_name_p = info->ref_attrs; *crt_name_p != NULL;
	       ++crt_name_p)
	    {
	      free_and_init (*crt_name_p);
	    }
	  free_and_init (info->ref_attrs);
	}

      free_and_init (info->name);
      free_and_init (info->asc_desc);
      free_and_init (info->ref_cls_name);

      free_and_init (info);
      info = next;
    }

  *save_info = NULL;
  return;
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * sm_touch_class () - makes sure that the XASL query cache is emptied
 *                     by performing a null operation on a class
 *   return: NO_ERROR on success, non-zero for ERROR
 *   classmop (in): The class to be "touched"
 */

int
sm_touch_class (MOP classmop)
{
  DB_CTMPL *ctmpl = NULL;
  int error = NO_ERROR;

  ctmpl = dbt_edit_class (classmop);
  if (ctmpl == NULL)
    {
      error = er_errid ();
      goto exit;
    }

  if (dbt_finish_class (ctmpl) == NULL)
    {
      dbt_abort_class (ctmpl);
      error = er_errid ();
      goto exit;
    }

exit:
  return error;
}
#endif

/*
 * sm_save_constraint_info() - Saves the information necessary to recreate a
 *			       constraint
 *   return: NO_ERROR on success, non-zero for ERROR
 *   save_info(in/out): The information saved
 *   c(in): The constraint to be saved
 */
int
sm_save_constraint_info (SM_CONSTRAINT_INFO ** save_info,
			 const SM_CLASS_CONSTRAINT * const c)
{
  int error_code = NO_ERROR;
  SM_CONSTRAINT_INFO *new_constraint = NULL;
  int num_atts = 0;
  int i = 0;
  SM_ATTRIBUTE **crt_att_p = NULL;

  new_constraint =
    (SM_CONSTRAINT_INFO *) calloc (1, sizeof (SM_CONSTRAINT_INFO));
  if (new_constraint == NULL)
    {
      error_code = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1,
	      sizeof (SM_CONSTRAINT_INFO));
      goto error_exit;
    }

  new_constraint->constraint_type = db_constraint_type (c);
  new_constraint->name = strdup (c->name);
  if (new_constraint->name == NULL)
    {
      error_code = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1,
	      strlen (c->name) + 1);
      goto error_exit;
    }

  assert (c->attributes != NULL);
  for (crt_att_p = c->attributes, num_atts = 0; *crt_att_p != NULL;
       ++crt_att_p)
    {
      ++num_atts;
    }
  assert (num_atts > 0 && num_atts == c->num_atts);

  new_constraint->att_names = (char **) calloc (num_atts + 1,
						sizeof (char *));
  if (new_constraint->att_names == NULL)
    {
      error_code = ER_OUT_OF_VIRTUAL_MEMORY;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1,
	      (num_atts + 1) * sizeof (char *));
      goto error_exit;
    }

  for (crt_att_p = c->attributes, i = 0; *crt_att_p != NULL; ++crt_att_p, ++i)
    {
      const char *const attr_name = (*crt_att_p)->name;

      new_constraint->att_names[i] = strdup (attr_name);
      if (new_constraint->att_names[i] == NULL)
	{
	  error_code = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1,
		  strlen (attr_name) + 1);
	  goto error_exit;
	}
    }

  if (c->asc_desc != NULL)
    {
      int i = 0;

      new_constraint->asc_desc = (int *) calloc (num_atts, sizeof (int));
      if (new_constraint->asc_desc == NULL)
	{
	  error_code = ER_OUT_OF_VIRTUAL_MEMORY;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error_code, 1,
		  num_atts * sizeof (int));
	  goto error_exit;
	}
      for (i = 0; i < num_atts; ++i)
	{
	  new_constraint->asc_desc[i] = c->asc_desc[i];
	}
    }

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

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * sm_has_non_null_attribute () - check if whether there is at least
 *                                one non null constraint in a given
 *                                attribute pointer array
 *   return: 1 if it exists, otherwise 0
 *   attrs(in): null terminated array of SM_ATTRIBUTE *
 */
int
sm_has_non_null_attribute (SM_ATTRIBUTE ** attrs)
{
  int i;

  assert (attrs != NULL);

  for (i = 0; attrs[i] != NULL; i++)
    {
      if (attrs[i]->flags & SM_ATTFLAG_NON_NULL)
	{
	  return 1;
	}
    }

  return 0;
}
#endif
