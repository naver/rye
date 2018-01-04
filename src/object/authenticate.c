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
 * authenticate.c - Authorization manager
 */

#ident "$Id$"

/*
 * Note:
 * Need to remove calls to the db_ layer since there are some
 * nasty dependency problems during restart and when the server
 * crashes since we need to touch objects before the database is
 * officially open.
 */

#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#include "porting.h"
#include "misc_string.h"
#include "memory_alloc.h"
#include "dbtype.h"
#include "dbdef.h"
#include "error_manager.h"
#include "boot_cl.h"
#include "work_space.h"
#include "object_primitive.h"
#include "class_object.h"
#include "schema_manager.h"
#include "authenticate.h"
#include "set_object.h"
#include "object_accessor.h"
#include "crypt_opfunc.h"
#include "message_catalog.h"
#include "string_opfunc.h"
#include "locator_cl.h"
#include "db.h"
#include "transform.h"
#include "environment_variable.h"
#include "execute_schema.h"
#include "object_print.h"
#include "execute_statement.h"
#include "optimizer.h"
#include "network_interface_cl.h"
#include "dbval.h"		/* this must be the last header file included */

#if defined(SA_MODE)
extern bool catcls_Enable;
#endif /* SA_MODE */

/*
 * Message id in the set MSGCAT_SET_AUTHORIZATION
 * in the message catalog MSGCAT_CATALOG_RYE (file rye.msg).
 */
#define MSGCAT_AUTH_INVALID_CACHE       1
#define MSGCAT_AUTH_CLASS_NAME          2
#define MSGCAT_AUTH_FROM_USER           3
#define MSGCAT_AUTH_USER_TITLE          4
#define MSGCAT_AUTH_UNDEFINED_USER      5
#define MSGCAT_AUTH_USER_NAME           6
#define MSGCAT_AUTH_USER_ID             7
#define MSGCAT_AUTH_USER_MEMBERS        8
#define MSGCAT_AUTH_USER_GROUPS         9
#define MSGCAT_AUTH_USER_NAME2          10
#define MSGCAT_AUTH_CURRENT_USER        11
#define MSGCAT_AUTH_ROOT_TITLE          12
#define MSGCAT_AUTH_ROOT_USERS          13
#define MSGCAT_AUTH_GRANT_DUMP_ERROR    14
#define MSGCAT_AUTH_AUTH_TITLE          15
#define MSGCAT_AUTH_USER_DIRECT_GROUPS  16

/*
 * Authorization Class Names
 */
const char *AU_ROOT_CLASS_NAME = CT_ROOT_NAME;
const char *AU_USER_CLASS_NAME = CT_USER_NAME;
const char *AU_GRANT_CLASS_NAME = "db_grant";
const char *AU_AUTH_NAME = CT_AUTH_NAME;
const char *AU_PUBLIC_USER_NAME = "PUBLIC";
const char *AU_DBA_USER_NAME = "DBA";


/*
 * Grant set structure
 *
 * Note :
 *    Grant information is stored packed in a sequence.  These
 *    macros define the length of the "elements" of the sequence and the
 *    offsets to particular fields in each element.  Previously, grants
 *    were stored in their own object but that lead to serious performance
 *    problems as we tried to load each grant object from the server.
 *    This way, grants are stored in the set directly with the authorization
 *    object so only one fetch is required.
 *
 */

#define ENCODE_PREFIX_DEFAULT           (char)0	/* unused */
#define ENCODE_PREFIX_SHA2_512          (char)1
#define IS_ENCODED_SHA2_512(string)     (string[0] == ENCODE_PREFIX_SHA2_512)
#define IS_ENCODED_ANY(string) \
        (IS_ENCODED_SHA2_512(string))


/* Macro to determine if a dbvalue is a character strign type. */
#define IS_STRING(n)    (db_value_type(n) == DB_TYPE_VARCHAR)

/* Macro to determine if a name is system catalog class */
#define IS_CATALOG_CLASS(name) \
        (strcmp(name, CT_TABLE_NAME) == 0 \
         || strcmp(name, CT_COLUMN_NAME) == 0 \
         || strcmp(name, CT_DOMAIN_NAME) == 0 \
         || strcmp(name, CT_QUERYSPEC_NAME) == 0 \
         || strcmp(name, CT_INDEX_NAME) == 0 \
         || strcmp(name, CT_INDEXKEY_NAME) == 0 \
         || strcmp(name, CT_AUTH_NAME) == 0 \
         || strcmp(name, CT_DATATYPE_NAME) == 0 \
         || strcmp(name, CT_USER_NAME) == 0 \
         || strcmp(name, CT_COLLATION_NAME) == 0 \
         || strcmp(name, CT_INDEX_STATS_NAME) == 0 \
         || strcmp(name, CT_LOG_ANALYZER_NAME) == 0 \
         || strcmp(name, CT_LOG_APPLIER_NAME) == 0 \
         || strcmp(name, CT_ROOT_NAME) == 0 \
         || strcmp(name, CT_SHARD_GID_SKEY_INFO_NAME) \
         || strcmp(name, CT_SHARD_GID_REMOVED_INFO_NAME))

/*
 * AU_GRANT
 *
 * This is an internal structure used to calculate the recursive
 * effects of a revoke operation.
 */
typedef struct au_grant AU_GRANT;
struct au_grant
{
  struct au_grant *next;

  MOP auth_object;
  MOP user;
  MOP grantor;

  DB_SET *grants;
  int grant_index;

  int grant_option;
  int legal;
};

/*
 * AU_CLASS_CACHE
 *
 * This structure is attached to classes and provides a cache of
 * the authorization bits.  Once authorization is calculated by examining
 * the group/grant hierarchy, the combined vector of bits is stored
 * in the cache for faster access.
 * In releases prior to 2.0, this was a single vector of bits for the
 * active user.  With the introduction of views, it became necessary
 * to relatively quickly perform a "setuid" operation to switch
 * authorization contexts within methods accessed through a view.
 * Because of this, the cache has been extended to be a variable length
 * array of entries, indexed by a user identifier.
 */
typedef struct au_class_cache AU_CLASS_CACHE;
struct au_class_cache
{
  struct au_class_cache *next;

  SM_CLASS *class_;
  unsigned int data[1];
};

/*
 * AU_USER_CACHE
 *
 * This is used to maintain a list of the users that have been
 * registered into the authorization caches.  Each time a "setuid" is
 * performed, the requested user is added to the caches if it is
 * not already present.
 */
typedef struct au_user_cache AU_USER_CACHE;
struct au_user_cache
{
  struct au_user_cache *next;

  DB_OBJECT *user;
  int index;
};

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * CLASS_GRANT
 *
 * Maintains information about a desired grant request.
 */
typedef struct class_grant CLASS_GRANT;
struct class_grant
{
  struct class_grant *next;

  struct class_user *user;
  int cache;
};

/*
 * CLASS_USER
 *
 * Maintains information about a desired grant subject user.
 */
typedef struct class_user CLASS_USER;
struct class_user
{
  struct class_user *next;

  MOP obj;

  CLASS_GRANT *grants;
  int available_auth;
};

/*
 * CLASS_AUTH
 *
 * Maintains information about the grants on a particular class.
 */
typedef struct class_auth CLASS_AUTH;
struct class_auth
{

  MOP class_mop;
  MOP owner;
  CLASS_USER *users;
};
#endif

/*
 * Au_root
 *
 * Global MOP of the authorization root object.
 * This is cached here after the database is restarted so we don't
 * have to keep looking for it.
 */
MOP Au_root = NULL;

/*
 * Au_disable
 *
 * Flag to disable authorization checking.  Only for well behaved
 * internal system functions.  Should not set this directly,
 * use the AU_DISABLE, AU_ENABLE macros instead.
 */
int Au_disable = 1;

/*
 * Au_ignore_passwords
 *
 * When this flag is set, the authorization system will ignore passwords
 * when logging in users.  This is intended for use only be system
 * defined utility programs that want to run as the DBA user but don't
 * want to require a password entry each time.  These would be protected
 * through OS file protected by the DBA rather than by the database
 * authorization mechanism.
 * This initializes to zero and is changed only by calling the function
 * au_enable_system_login().
 */
static int Au_ignore_passwords = 0;

/*
 * Au_dba_user, Au_public_user
 *
 * These are the two system defined user objects.
 * All users are automatically a member of the PUBLIC user and hence
 * grants to PUBLIC are visible to everyone.  The DBA is automatically
 * a member of all groups/users and hence will have all permissions.
 */
MOP Au_public_user = NULL;
MOP Au_dba_user = NULL;

/*
 * Au_user
 *
 * This points to the MOP of the user object of the currently
 * logged in user.  Can be overridden in special cases to change
 * system authorizations.
 */
MOP Au_user = NULL;

/*
 * Au_user_name, Au_user_password(SHA2_512)
 *
 * Saves the registered user name and password.
 * Login normally occurs before the database is restarted so all we
 * do is register the user name.  The user name will be validated
 * against the database at the time of restart.
 * Once the database has been started, we get the user information
 * directly from the user object and no longer use these variables.
 *
 * NOTE: Need to be storing the password in an encrypted string.
 */
static char Au_user_name[DB_MAX_USER_LENGTH + 4] = {
  '\0'
};
static char Au_user_password[AU_MAX_PASSWORD_BUF + 4] = {
  '\0'
};				/* SHA2_512 */

/*
 * Au_password_class
 *
 * This is a hack until we get a proper "system" authorization
 * level.  We need to detect attempts to update or delete
 * system classes by the DBA when there is no approved
 * direct update mechanism.  This is the case for all the
 * authorization classes.  The only way they can be updated is
 * through the authorization functions.
 * To avoid searching for these classes all the time, cache
 * them here.
 */
static DB_OBJECT *Au_authorizations_class;
static DB_OBJECT *Au_authorization_class;
static DB_OBJECT *Au_user_class;

/*
 * Au_user_cache
 *
 * The list of cached users.
 */
static AU_USER_CACHE *Au_user_cache = NULL;

/*
 * Au_class_caches
 *
 * A list of all allocated class caches.  These are maintained on a list
 * so that we can get to all of them easily when they need to be
 * altered.
 */
static AU_CLASS_CACHE *Au_class_caches = NULL;

/*
 * Au_cache_depth
 *
 * These maintain information about the structure of the class caches.
 * Au_cache_depth has largest current index.
 * Au_cache_max has the total size of the allocated arrays.
 * Au_cache_increment has the growth count when the array needs to be
 * extended.
 * The caches are usually allocated larger than actually necessary so
 * we can avoid reallocating all of them when a new user is added.
 * Probably not that big a deal.
 */
static int Au_cache_depth = 0;
static int Au_cache_max = 0;
static int Au_cache_increment = 4;

/*
 * Au_cache_index
 *
 * This is the current index into the class authorization caches.
 * It will be maintained in parallel with the current user.
 * Each user is assigned a particular index, when the user changes,
 * Au_cache_index is changed as well.
 */
static int Au_cache_index = -1;

#if defined (ENABLE_UNUSED_FUNCTION)
static const char *auth_type_name[] = {
  "select", "insert", "update", "delete", "alter", "index", "execute"
};
#endif


#if defined (ENABLE_UNUSED_FUNCTION)
/* 'get_attribute_number' is a statically linked method used only for QA
   scenario */
void get_attribute_number (DB_OBJECT * target, DB_VALUE * result,
			   DB_VALUE * attr_name);

static int au_get_set (MOP obj, const char *attname, DB_SET ** set);
static int au_get_object (MOP obj, const char *attname, MOP * mop_ptr);
static int au_set_get_obj (DB_SET * set, int index, MOP * obj);
#endif
static AU_CLASS_CACHE *au_make_class_cache (int depth);
static void au_free_class_cache (AU_CLASS_CACHE * cache);
static AU_CLASS_CACHE *au_install_class_cache (SM_CLASS * sm_class);

static int au_extend_class_caches (int *index);
static int au_find_user_cache_index (DB_OBJECT * user, int *index,
				     int check_it);
static void free_user_cache (AU_USER_CACHE * u);
static void reset_cache_for_user_and_class (SM_CLASS * sm_class);

static void remove_user_cache_references (MOP user);
static void init_caches (void);
static void flush_caches (void);

static MOP au_make_user (const char *name);
static int au_delete_auth_by_user (char *user_name);

static MOP au_get_auth (MOP user, MOP class_mop);
static int au_insert_update_auth (MOP grantor, MOP user, MOP class_mop,
				  DB_AUTH auth_type, bool set_flag);
static int apply_auth_grants (MOP auth, unsigned int *bits);

static int check_user_name (const char *name);
static int encrypt_password_sha2_512 (const char *pass, char *dest);
#if defined(ENABLE_UNUSED_FUNCTION)
static int io_relseek (const char *pass, int has_prefix, char *dest);
#endif
static bool match_password (const char *user, const char *database);
static int update_cache (MOP classop, SM_CLASS * sm_class,
			 AU_CLASS_CACHE * cache);
static int appropriate_error (unsigned int bits, unsigned int requested);
static int check_grant_option (MOP classop, SM_CLASS * sm_class,
			       DB_AUTH type);

static int is_protected_class (MOP classmop, SM_CLASS * sm_class,
			       DB_AUTH auth);
static int check_authorization (MOP classobj, SM_CLASS * sm_class,
				DB_AUTH type);
static int fetch_class (MOP op, MOP * return_mop, SM_CLASS ** return_class,
			LOCK lock);

static int fetch_instance (MOP op, MOBJ * obj_ptr, LOCK lock);

#if defined(ENABLE_UNUSED_FUNCTION)
static CLASS_GRANT *make_class_grant (CLASS_USER * user, int cache);
static CLASS_USER *make_class_user (MOP user_obj);
static CLASS_USER *find_or_add_user (CLASS_AUTH * auth, MOP user_obj);
#endif

/*
 * DB_ EXTENSION FUNCTIONS
 */

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * au_get_set
 *   return: error code
 *   obj(in):
 *   attname(in):
 *   set(in):
 */
static int
au_get_set (MOP obj, const char *attname, DB_SET ** set)
{
  int error = NO_ERROR;
  DB_VALUE value;

  *set = NULL;
  error = obj_get (obj, attname, &value);
  if (error == NO_ERROR)
    {
      if (!TP_IS_SET_TYPE (DB_VALUE_TYPE (&value)))
	{
	  error = ER_OBJ_DOMAIN_CONFLICT;
	}
      else
	{
	  if (DB_IS_NULL (&value))
	    {
	      *set = NULL;
	    }
	  else
	    {
	      *set = db_get_set (&value);
	    }

	  /*
	   * since we almost ALWAYS end up iterating through the sets fetching
	   * objects, do a vector fetch immediately to avoid
	   * multiple server calls.
	   * Should have a sub db_ function for doing this.
	   */
	  if (*set != NULL)
	    {
	      db_fetch_set (*set, 0);
	      set_filter (*set);
	      /*
	       * shoudl be detecting the filtered elements and marking the
	       * object dirty if possible
	       */
	    }
	}
    }
  return (error);
}

/*
 * au_get_object
 *   return: error code
 *   obj(in):
 *   attname(in):
 *   mop_ptr(in):
 */
static int
au_get_object (MOP obj, const char *attname, MOP * mop_ptr)
{
  int error = NO_ERROR;
  DB_VALUE value;

  *mop_ptr = NULL;
  error = obj_get (obj, attname, &value);
  if (error == NO_ERROR)
    {
      if (DB_VALUE_TYPE (&value) != DB_TYPE_OBJECT)
	{
	  error = ER_OBJ_DOMAIN_CONFLICT;
	}
      else
	{
	  if (DB_IS_NULL (&value))
	    {
	      *mop_ptr = NULL;
	    }
	  else
	    {
	      *mop_ptr = db_get_object (&value);
	    }
	}
    }
  return (error);
}

/*
 * au_set_get_obj -
 *   return: error code
 *   set(in):
 *   index(in):
 *   obj(out):
 */
static int
au_set_get_obj (DB_SET * set, int index, MOP * obj)
{
  int error = NO_ERROR;
  DB_VALUE value;

  *obj = NULL;

  error = set_get_element (set, index, &value);
  if (error == NO_ERROR)
    {
      if (DB_VALUE_TYPE (&value) != DB_TYPE_OBJECT)
	{
	  error = ER_OBJ_DOMAIN_CONFLICT;
	}
      else
	{
	  if (DB_IS_NULL (&value))
	    {
	      *obj = NULL;
	    }
	  else
	    {
	      *obj = db_get_object (&value);
	    }
	}
    }

  return error;
}
#endif

/*
 * AUTHORIZATION CACHES
 */

/*
 * au_make_class_cache - Allocates and initializes a new class cache
 *    return: new cache structure
 *    depth(in): number of elements to include in the cache
 */
static AU_CLASS_CACHE *
au_make_class_cache (int depth)
{
  AU_CLASS_CACHE *new_class_cache = NULL;
  int i;
  size_t size;

  if (depth <= 0)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_GENERIC_ERROR, 1, "");
    }
  else
    {
      size = sizeof (AU_CLASS_CACHE) + ((depth - 1) * sizeof (unsigned int));
      new_class_cache = (AU_CLASS_CACHE *) malloc (size);
      if (new_class_cache == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY,
		  1, size);
	  return NULL;
	}

      new_class_cache->next = NULL;
      new_class_cache->class_ = NULL;
      for (i = 0; i < depth; i++)
	{
	  new_class_cache->data[i] = AU_CACHE_INVALID;
	}
    }

  return new_class_cache;
}

/*
 * au_free_class_cache - Frees a class cache
 *    return: none
 *    cache(in): cache to free
 */
static void
au_free_class_cache (AU_CLASS_CACHE * cache)
{
  if (cache != NULL)
    {
      free_and_init (cache);
    }
}

/*
 * au_install_class_cache - This allocates a new class cache and attaches
 *                          it to a class.
 *   return: new class cache
 *   class(in): class structure to get the new cache
 *
 * Note: Once a cache/class association has been made, we also put the
 *       cache on the global cache list so we can maintain it consistently.
 */
static AU_CLASS_CACHE *
au_install_class_cache (SM_CLASS * sm_class)
{
  AU_CLASS_CACHE *new_class_cache;

  new_class_cache = au_make_class_cache (Au_cache_max);
  if (new_class_cache != NULL)
    {
      new_class_cache->next = Au_class_caches;
      Au_class_caches = new_class_cache;
      new_class_cache->class_ = sm_class;
      sm_class->auth_cache = new_class_cache;
    }

  return new_class_cache;
}

/*
 * au_free_authorization_cache -  This removes a class cache from the global
 *                                cache list, detaches it from the class
 *                                and frees it.
 *   return: none
 *   cache(in): class cache
 */
void
au_free_authorization_cache (void *cache)
{
  AU_CLASS_CACHE *c, *prev;

  if (cache != NULL)
    {
      for (c = Au_class_caches, prev = NULL;
	   c != NULL && c != cache; c = c->next)
	{
	  prev = c;
	}
      if (c != NULL)
	{
	  if (prev == NULL)
	    {
	      Au_class_caches = c->next;
	    }
	  else
	    {
	      prev->next = c->next;
	    }
	}
      au_free_class_cache ((AU_CLASS_CACHE *) cache);
    }
}

/*
 * au_extend_class_caches - This extends the all existing class caches so they
 *                          can contain an additional element.
 *   return: error code
 *   index(out): next available index
 *
 * Note: If we have already preallocated some extra elements it will use one
 *       and avoid reallocating all the caches. If we have no extra elements,
 *       we grow all the caches by a certain amount.
 */
static int
au_extend_class_caches (int *index)
{
  int error = NO_ERROR;
  AU_CLASS_CACHE *c, *new_list, *new_entry, *next;
  int new_max, i;

  if (Au_cache_depth < Au_cache_max)
    {
      *index = Au_cache_depth;
      Au_cache_depth++;
    }
  else
    {
      new_list = NULL;
      new_max = Au_cache_max + Au_cache_increment;

      for (c = Au_class_caches; c != NULL && !error; c = c->next)
	{
	  new_entry = au_make_class_cache (new_max);
	  if (new_entry == NULL)
	    {
	      error = er_errid ();
	      assert (error != NO_ERROR);
	      break;
	    }

	  for (i = 0; i < Au_cache_depth; i++)
	    {
	      new_entry->data[i] = c->data[i];
	    }
	  new_entry->class_ = c->class_;
	  new_entry->next = new_list;
	  new_list = new_entry;
	}

      if (error == NO_ERROR)
	{
	  for (c = Au_class_caches, next = NULL; c != NULL; c = next)
	    {
	      next = c->next;
	      c->class_->auth_cache = NULL;
	      au_free_class_cache (c);
	    }

	  for (c = new_list; c != NULL; c = c->next)
	    {
	      c->class_->auth_cache = c;
	    }

	  Au_class_caches = new_list;
	  Au_cache_max = new_max;
	  *index = Au_cache_depth;
	  Au_cache_depth++;
	}
      else
	{
	  /* free */
	  for (c = new_list, next = NULL; c != NULL; c = next)
	    {
	      next = c->next;
	      c->class_->auth_cache = NULL;
	      au_free_class_cache (c);
	    }
	}

    }

  return error;
}

/*
 * au_find_user_cache_index - This determines the cache index for the given
 *                            user.
 *   return: error code
 *   user(in): user object
 *   index(out): returned user index
 *   check_it(in):
 *
 * Note: If the user has never been added to the authorization cache,
 *       we reserve a new index for the user.  Reserving the user index may
 *       result in growing all the existing class caches.
 *       This is the primary work function for AU_SET_USER() and it should
 *       be fast.
 */
static int
au_find_user_cache_index (DB_OBJECT * user, int *index, int check_it)
{
  int error = NO_ERROR;
  AU_USER_CACHE *u, *new_user_cache;
  DB_OBJECT *class_mop;

  for (u = Au_user_cache; u != NULL && u->user != user; u = u->next)
    {
      ;
    }

  if (u != NULL)
    {
      *index = u->index;
    }
  else
    {
      /*
       * User wasn't in the cache, add it and extend the existing class
       * caches.  First do a little sanity check just to make sure this
       * is a user object.
       */
      if (check_it)
	{
	  class_mop = sm_get_class (user);
	  if (class_mop == NULL)
	    {
	      return er_errid ();
	    }
	  else if (class_mop != Au_user_class)
	    {
	      error = ER_AU_CORRUPTED;	/* need a better error */
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	      return er_errid ();
	    }
	}

      new_user_cache = (AU_USER_CACHE *) malloc (sizeof (AU_USER_CACHE));
      if (new_user_cache != NULL)
	{
	  if ((error = au_extend_class_caches (index)))
	    {
	      free_and_init (new_user_cache);
	    }
	  else
	    {
	      new_user_cache->next = Au_user_cache;
	      Au_user_cache = new_user_cache;
	      new_user_cache->user = user;
	      new_user_cache->index = *index;
	    }
	}
    }

  return error;
}

/*
 * free_user_cache - Frees a user cache. Make sure to clear the MOP pointer.
 *   returns: none
 *   u(in): user cache
 */
static void
free_user_cache (AU_USER_CACHE * u)
{
  if (u != NULL)
    {
      u->user = NULL;		/* clear GC roots */
      free_and_init (u);
    }
}


/*
 * reset_cache_for_user_and_class - This is called whenever a grant or revoke
 *                                  operation is performed. It resets the
 *                                  caches only for a particular
 *                                  user/class pair
 *   return : none
 *   class(in): class structure
 *
 * Note: This was originally written so that only the authorization
 *       cache for the given user was cleared.  This does not work however
 *       if we're changing authorization for a group and there are members
 *       of that group already cached.
 *       We could be smart and try to invalidate the caches of all
 *       members of this user but instead just go ahead and invalidate
 *       everyone's cache for this class.  This isn't optimal but doesn't really
 *       matter that much.  grant/revoke don't happen very often.
 */
static void
reset_cache_for_user_and_class (SM_CLASS * sm_class)
{
  AU_USER_CACHE *u;
  AU_CLASS_CACHE *c;

  for (c = Au_class_caches; c != NULL && c->class_ != sm_class; c = c->next)
    {
      ;
    }

  if (c != NULL)
    {
      /*
       * invalide every user's cache for this class_, could be more
       * selective and do only the given user and its members
       */
      for (u = Au_user_cache; u != NULL; u = u->next)
	{
	  c->data[u->index] = AU_CACHE_INVALID;
	}
    }
}

/*
 * au_reset_authorization_caches - This is called by ws_clear_all_hints()
 *                                 and ws_abort_mops() on transaction
 *                                 boundaries.
 *   return: none
 *
 * Note: We reset all the authorization caches at this point.
 *       This sets all of the authorization entries in a cache to be invalid.
 *       Normally this is done when the authorization for this
 *       class changes in some way.  The next time the cache is used, it
 *       will force the recomputation of the authorization bits.
 *       We should try to be smarter and flush the caches only when we know
 *       that the authorization catalogs have changed in some way.
 */

void
au_reset_authorization_caches (void)
{
  AU_CLASS_CACHE *c;
  int i;

  for (c = Au_class_caches; c != NULL; c = c->next)
    {
      for (i = 0; i < Au_cache_depth; i++)
	{
	  c->data[i] = AU_CACHE_INVALID;
	}
    }
}

/*
 * remove_user_cache_reference - This is called when a user object is deleted.
 *   return: none
 *   user(in): user object
 *
 * Note: If there is an authorization cache entry for this user, we NULL
 *       the user pointer so it will no longer be used.  We could to in
 *       and restructure all the caches to remove the deleted user but user
 *       deletion isn't that common.  Just leave an unused entry in the
 *       cache array.
 */
static void
remove_user_cache_references (MOP user)
{
  AU_USER_CACHE *u;

  for (u = Au_user_cache; u != NULL; u = u->next)
    {
      if (u->user == user)
	{
	  u->user = NULL;
	}
    }
}

/*
 * init_caches - Called during au_init().  Initialize all of the cache
 *               related global variables.
 *   return: none
 */
static void
init_caches (void)
{
  Au_user_cache = NULL;
  Au_class_caches = NULL;
  Au_cache_depth = 0;
  Au_cache_max = 0;
  Au_cache_increment = 4;
  Au_cache_index = -1;
}

/*
 * flush_caches - Called during au_final(). Free the authorization cache
 *                structures and initialize the global variables
 *                to their default state.
 *   return : none
 */
static void
flush_caches (void)
{
  AU_USER_CACHE *u, *nextu;
  AU_CLASS_CACHE *c, *nextc;

  for (c = Au_class_caches, nextc = NULL; c != NULL; c = nextc)
    {
      nextc = c->next;
      c->class_->auth_cache = NULL;
      au_free_class_cache (c);
    }
  for (u = Au_user_cache, nextu = NULL; u != NULL; u = nextu)
    {
      nextu = u->next;
      free_user_cache (u);
    }

  /* clear the associated globals */
  init_caches ();
}


/*
 * COMPARISON
 */

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * toupper_string -  This is used to add a user or compare two user names.
 *   return: convert a string to upper case
 *   name1: user name
 *   name2: user name
 *
 * Note: User names are stored in upper case.
 *       This is split into a separate function in case we need to make
 *       modifications to this for Internationalization.
 */
char *
toupper_string (const char *name1, char *name2)
{
  char *buffer, *ptr;

  buffer = (char *) malloc (strlen (name1) * 2 + 1);
  if (buffer == NULL)
    {
      return NULL;
    }

  intl_mbs_upper (name1, buffer);
  ptr = buffer;
  while (*ptr != '\0')
    {
      *name2++ = *ptr++;
    }
  *name2 = '\0';

  free (buffer);
  return name2;
}
#endif

/*
 * USER/GROUP ACCESS
 */

/*
 * au_find_user - Find a user object by name.
 *   return: user object
 *   user_name(in): name
 *
 * Note: The AU_ROOT_CLASS_NAME class used to have a users attribute
 *       which was a set
 *       containing the object-id for all users.
 *       The users attribute has been eliminated for performance reasons.
 *       A query is now used to find the user.  Since the user name is not
 *       case insensitive, it is set to upper case in the query.  This forces
 *       user names to be set to upper case when users are added.
 */
MOP
au_find_user (const char *user_name)
{
  MOP user = NULL;
  int save;
  int error = NO_ERROR;
  MOP user_class;
  char *upper_case_name = NULL;
  size_t upper_case_name_size;
  DB_IDXKEY user_name_key;

  DB_IDXKEY_MAKE_NULL (&user_name_key);

  if (user_name == NULL)
    {
      return NULL;
    }

  /* disable checking of internal authorization object access */
  AU_DISABLE (save);

  user = NULL;

  upper_case_name_size = strlen (user_name);
  upper_case_name = (char *) malloc (upper_case_name_size + 1);
  if (upper_case_name == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OUT_OF_VIRTUAL_MEMORY, 1,
	      upper_case_name_size);
      return NULL;
    }
  intl_identifier_upper (user_name, upper_case_name);

  /*
   * first try to find the user id by index. This is faster than
   * a query, and will not get blocked out as a server request
   * if the query processing resources are all used up at the moment.
   * This is primarily of importance during logging in.
   */
  user_class = sm_find_class (AU_USER_CLASS_NAME);
  if (user_class)
    {
      db_make_string (&(user_name_key.vals[0]), upper_case_name);
      user_name_key.size = 1;

      user = obj_find_unique (user_class, "name", &user_name_key, S_LOCK);
    }
  error = er_errid ();

  if (error != NO_ERROR)
    {
      if (error == ER_OBJ_OBJECT_NOT_FOUND)
	{
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_AU_INVALID_USER, 1,
		  user_name);
	}
      goto exit;
    }

  assert (user != NULL);

exit:
  AU_ENABLE (save);

  if (upper_case_name)
    {
      free_and_init (upper_case_name);
    }

  return (user);
}

/*
 * au_make_user -  Create a new user object. Convert the name to upper case
 *                 so that au_find_user can use a query.
 *   return: new user object
 *   name(in): user name
 */
static MOP
au_make_user (const char *name)
{
  MOP uclass, user;
  DB_VALUE value;
  char *lname;
  int error;

  user = NULL;
  uclass = sm_find_class (AU_USER_CLASS_NAME);
  if (uclass == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_AU_MISSING_CLASS, 1,
	      AU_USER_CLASS_NAME);
    }
  else
    {
      int name_size;

      user = obj_create (uclass);
      name_size = strlen (name);
      lname = (char *) malloc (name_size + 1);
      if (lname)
	{
	  intl_identifier_upper (name, lname);
	  db_make_string (&value, lname);
	  error = obj_set (user, "name", &value);
	  free_and_init (lname);
	  if (error != NO_ERROR)
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		      ER_AU_ACCESS_ERROR, 2, AU_USER_CLASS_NAME, "name");
	      obj_delete (user);
	      user = NULL;
	    }
	}
      else
	{
	  goto memory_error;
	}
    }
  return (user);

memory_error:
  if (user != NULL)
    {
      obj_delete (user);
    }
  return NULL;
}

static int
au_delete_auth_by_user (char *user_name)
{
  int save;
  int error = NO_ERROR;
  int ret;
  DB_SESSION *session = NULL;
  DB_QUERY_RESULT *result;
  DB_VALUE value;
  char query_buf[1024];

  AU_DISABLE (save);

  sprintf (query_buf, "delete from [%s] where [grantee_name] = ?;",
	   CT_AUTH_NAME);
  session = db_open_buffer (query_buf);

  if (session == NULL)
    {
      goto error_exit;
    }

  db_make_string (&value, user_name);
  db_push_values (session, 1, &value);

  error = db_compile_statement (session);
  if (error != NO_ERROR)
    {
      db_close_session (session);
      goto error_exit;
    }

#if 1				/* TODO - delete me someday */
  assert (session->groupid == NULL_GROUPID);	/* is DML derived from DDL */
#endif
  ret = db_execute_statement (session, &result);
  if (ret < 0)
    {
      error = er_errid ();
      db_close_session (session);
      goto error_exit;
    }

  db_query_end (result);
  db_close_session (session);
  pr_clear_value (&value);

error_exit:
  AU_ENABLE (save);
  return error;
}

static MOP
au_get_auth (MOP user, MOP class_mop)
{
  MOP au_class, au_obj;
  MOP ret_obj = NULL;
  DB_OBJLIST *list, *mop;
  DB_VALUE user_value, value;

  char *grantee_name;
  const char *table_name;
  char *tmp;

  DB_MAKE_NULL (&user_value);
  DB_MAKE_NULL (&value);

  pr_clear_value (&user_value);
  obj_get (user, "name", &user_value);
  grantee_name = db_get_string (&user_value);

  table_name = sm_class_name (class_mop);

  if (grantee_name == NULL || table_name == NULL)
    {
#if 1				/* TODO - trace */
      assert (false);
#endif
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_AU_NO_AUTHORIZATION, 0);
      pr_clear_value (&user_value);
      return ret_obj;
    }

  au_class = sm_find_class (CT_AUTH_NAME);
  if (au_class == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_AU_NO_AUTHORIZATION, 0);
      pr_clear_value (&user_value);
      return ret_obj;
    }

  list = sm_fetch_all_objects (au_class, S_LOCK);
  if (list == NULL)
    {
#if 0				/* is createdb */
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_AU_NO_AUTHORIZATION, 0);
#endif
      pr_clear_value (&user_value);
      return ret_obj;
    }

  for (mop = list; mop != NULL; mop = mop->next)
    {
      au_obj = mop->op;

      pr_clear_value (&value);
      if (obj_get (au_obj, "grantee_name", &value) != NO_ERROR)
	{
	  continue;
	}

      tmp = db_get_string (&value);
      if (tmp && strcmp (tmp, grantee_name) != 0)
	{
	  continue;
	}

      pr_clear_value (&value);
      if (obj_get (au_obj, "table_name", &value) != NO_ERROR)
	{
	  continue;
	}

      tmp = db_get_string (&value);
      if (tmp && strcmp (tmp, table_name) != 0)
	{
	  continue;
	}

      ret_obj = au_obj;
      break;
    }

  ml_ext_free (list);
  pr_clear_value (&value);
  pr_clear_value (&user_value);
  return ret_obj;
}

static int
au_insert_update_auth (UNUSED_ARG MOP grantor, MOP user, MOP class_mop,
		       DB_AUTH auth_type, bool is_set)
{
  MOP au_class;
  MOP au_obj;
  int index;
  int error = NO_ERROR;
//  char *grantee_name;
  const char *table_name;
  DB_VALUE user_value, value;
  DB_AUTH type;
  int i;
  const char *type_set[] = {
    "SELECT_PRIV", "INSERT_PRIV", "UPDATE_PRIV", "DELETE_PRIV", "ALTER_PRIV"
  };

  DB_MAKE_NULL (&user_value);
  DB_MAKE_NULL (&value);

  au_obj = au_get_auth (user, class_mop);

  obj_get (user, "name", &user_value);
//  grantee_name = db_get_string (&user_value);

  table_name = sm_class_name (class_mop);

  if (is_set == true && au_obj == NULL)
    {
      /* create new auth obj */
      au_class = sm_find_class (CT_AUTH_NAME);

      if (au_class == NULL)
	{
	  pr_clear_value (&user_value);
	  error = ER_AU_MISSING_CLASS;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, CT_AUTH_NAME);
	  return error;
	}

      au_obj = db_create_internal (au_class);
      if (au_obj == NULL)
	{
	  pr_clear_value (&user_value);
	  return er_errid ();
	}

      for (i = 0; i < 5; i++)
	{
	  db_make_int (&value, 0);
	  obj_set (au_obj, type_set[i], &value);
	}
    }
  else if (is_set == false && au_obj == NULL)
    {
      /* error */
      pr_clear_value (&user_value);
      error = ER_AU_MISSING_CLASS;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1, CT_AUTH_NAME);
      return error;
    }

  /* set auth obj */
  error = obj_lock (au_obj, 1);
  if (error != NO_ERROR)
    {
      pr_clear_value (&user_value);
      return error;
    }

  obj_set (au_obj, "grantee_name", &user_value);

  db_make_string (&value, table_name);
  obj_set (au_obj, "table_name", &value);

  for (index = DB_AUTH_ALTER; index; index >>= 1)
    {
      if (auth_type & index)
	{
	  for (type = DB_AUTH_SELECT, i = 0; type != (auth_type & index);
	       type = (DB_AUTH) (type << 1), i++)
	    {
	      ;
	    }
	  if (is_set)
	    {
	      db_make_int (&value, 1);
	      obj_set (au_obj, type_set[i], &value);
	    }
	  else
	    {
	      db_make_int (&value, 0);
	      obj_set (au_obj, type_set[i], &value);
	    }
	}
    }

  pr_clear_value (&user_value);

  return error;
}

/*
 * check_user_name
 *   return: error code
 *   name(in): proposed user name
 *
 * Note: This is made void for ansi compatibility. It previously insured
 *       that identifiers which were accepted could be parsed in the
 *       language interface.
 *
 *       ANSI allows any character in an identifier. It also allows reserved
 *       words. In order to parse identifiers with non-alpha characters
 *       or that are reserved words, an escape syntax is definned with double
 *       quotes, "FROM", for example.
 */
static int
check_user_name (UNUSED_ARG const char *name)
{
  return NO_ERROR;
}


/*
 * au_is_dba_group_member -  Determines if a given user is the DBA/a member
 *                           of the DBA group, or not
 *   return: true or false
 *   user(in): user object
 */
bool
au_is_dba_group_member (MOP user)
{
  bool is_member = false;

  if (user == NULL)
    {
#if 1				/* TODO - trace */
      assert (false);
#endif
      return false;		/* avoid gratuitous er_set later */
    }

  if (user == Au_dba_user)
    {
      return true;
    }

  return is_member;
}

/*
 * au_add_user -  Add a user object if one does not already exist.
 *   return: new or existing user object
 *   name(in): user name
 *   exists(out): flag set if user already existed
 *
 * Note: If one already exists, return it and set the flag.
 *       The AU_ROOT_CLASS_NAME class used to have a user attribute
 *       which was a set
 *       containing the object-id for all users.  The users attribute has been
 *       eliminated for performance reasons.
 *
 */
MOP
au_add_user (const char *name, int *exists)
{
  MOP user;
  int save;

  user = NULL;
  if (Au_dba_user != NULL && !au_is_dba_group_member (Au_user))
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_AU_DBA_ONLY, 1,
	      "add_user");
    }
  else if (!check_user_name (name))
    {
      AU_DISABLE (save);
      user = NULL;
      if (exists != NULL)
	{
	  *exists = 0;
	}
      if (name == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_AU_INVALID_USER_NAME,
		  1, "");
	}
      else
	{
	  user = au_find_user (name);
	  if (user != NULL)
	    {
	      if (exists != NULL)
		{
		  *exists = 1;
		}
	    }
	  else
	    {
	      if (er_errid () != ER_AU_INVALID_USER)
		{
		  AU_ENABLE (save);
		  return NULL;
		}
	      user = au_make_user (name);
	    }
	}
      AU_ENABLE (save);
    }
  return (user);
}

/*
 * Password Encoding
 */

/*
 * Password encoding is a bit kludgey to support older databases where the
 * password was stored in an unencoded format.  Since we don't want
 * to invalidate existing databases unless absolutely necessary, we
 * need a way to recognize if the password in the database is encoded or not.
 *
 * The kludge is to store encoded passwords with a special prefix character
 * that could not normally be typed as part of a password.  This will
 * be the binary char \001 or Control-A.  The prefix could be used
 * in the future to identify other encoding schemes in case we find
 * a better way to store passwords.
 *
 * If the password string has this prefix character, we can assume that it
 * has been encoded, otherwise it is assumed to be an older unencoded password.
 *
 */

/*
 * encrypt_password_sha2_512 -  hashing a password string using SHA2 512
 *   return: error code
 *   pass(in): string to encrypt
 *   dest(out): destination buffer
 */
static int
encrypt_password_sha2_512 (const char *pass, char *dest)
{
  int error_status = NO_ERROR;
  char *result_strp = NULL;
  int result_len = 0;

  if (pass == NULL)
    {
      strcpy (dest, "");
    }
  else
    {
      error_status =
	sha_two (NULL, pass, strlen (pass), 512, &result_strp, &result_len);
      if (error_status == NO_ERROR)
	{
	  assert (result_strp != NULL);

	  memcpy (dest + 1, result_strp, result_len);
	  dest[1 + result_len] = '\0';	/* null terminate for match_password() */

	  free_and_init (result_strp);
	}

      dest[0] = ENCODE_PREFIX_SHA2_512;
    }

  assert (error_status == NO_ERROR);

  return error_status;
}

/*
 * au_user_name_dup -  Returns the duplicated string of the name of the current
 *                     user. The string must be freed after use.
 *   return: user name (strdup)
 */
char *
au_user_name_dup (void)
{
  return strdup (Au_user_name);
}

/*
 * match_password -  This compares two passwords to see if they match.
 *   return: non-zero if the passwords match
 *   user(in): user supplied password
 *   database(in): stored database password
 *
 * Note: Either the user or database password can be encrypted or unencrypted.
 *       The database password will only be unencrypted if this is a very
 *       old database.  The user password will be unencrypted if we're logging
 *       in to an active session.
 */
static bool
match_password (const char *user, const char *database)
{
  char buf1[AU_MAX_PASSWORD_BUF + 4];
  char buf2[AU_MAX_PASSWORD_BUF + 4];

  if (user == NULL || database == NULL)
    {
      return false;
    }

  assert (IS_ENCODED_SHA2_512 (database));

  /* get both passwords into an encrypted format */
  /* if database's password was encrypted with SHA2,
   * then, user's password should be encrypted with SHA2,
   */
  if (IS_ENCODED_SHA2_512 (database))
    {
      /* DB: SHA2 */
      STRNCPY (buf2, database, AU_MAX_PASSWORD_BUF + 4);
      if (IS_ENCODED_ANY (user))
	{
	  /* USER:SHA2 */
	  STRNCPY (buf1, Au_user_password, AU_MAX_PASSWORD_BUF + 4);
	}
      else
	{
#if 1				/* TODO - */
	  return false;
#else
	  /* USER:PLAINTEXT -> SHA2 */
	  if (encrypt_password_sha2_512 (user, buf1) != NO_ERROR)
	    {
	      return false;
	    }
#endif
	}
    }
  else
    {
#if 1				/* TODO - */
      return false;
#else
      /* DB:PLAINTEXT -> SHA2 */
      if (encrypt_password_sha2_512 (database, buf2) != NO_ERROR)
	{
	  return false;
	}
      if (IS_ENCODED_ANY (user))
	{
	  /* USER : SHA2 */
	  strcpy (buf1, Au_user_password);
	}
      else
	{
	  /* USER : PLAINTEXT -> SHA2 */
	  if (encrypt_password_sha2_512 (user, buf1) != NO_ERROR)
	    {
	      return false;
	    }
	}
#endif
    }

  return strcmp (buf1, buf2) == 0;
}

/*
 * au_set_password -  Set the password string for a user.
 *                    This should be using encrypted strings.
 *   return:error code
 *   user(in):  user object
 *   password(in): new password
 *   encode(in): flag to enable encryption of the string in the database
 */
int
au_set_password (MOP user, const char *password, bool encode)
{
  int error = NO_ERROR;
  DB_VALUE value;
  int save, len;
  char pbuf[AU_MAX_PASSWORD_BUF + 4];

  AU_DISABLE (save);
  if (Au_user != user && !au_is_dba_group_member (Au_user))
    {
      error = ER_AU_UPDATE_FAILURE;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
    }
  else
    {
      /* convert empty password strings to NULL passwords */
      if (password != NULL)
	{
	  len = strlen (password);
	  if (len == 0)
	    password = NULL;
	  /*
	   * check for large passwords, only do this
	   * if the encode flag is on !
	   */
	  else if (len > AU_MAX_PASSWORD_CHARS && encode)
	    {
	      error = ER_AU_PASSWORD_OVERFLOW;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	    }
	}
      if (error == NO_ERROR)
	{
	  if (encode && password != NULL)
	    {
	      error = encrypt_password_sha2_512 (password, pbuf);
	      if (error == NO_ERROR)
		{
		  db_make_string (&value, pbuf);
		  error = obj_set (user, "password", &value);
		}
	    }
	  else
	    {
	      if (password == NULL)
		{
		  db_make_null (&value);
		}
	      else
		{
		  /* password was already encrypted */
		  strcpy (pbuf, password);
		  db_make_string (&value, pbuf);
		}
	      error = obj_set (user, "password", &value);
	    }
	}
    }
  AU_ENABLE (save);
  return (error);
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * au_drop_member - Remove a member from a group.
 *   return: error code
 *   group(in): group with member to drop
 *   member(in): member to drop
 *
 * Note:
 *
 *    The db_user class used to have a groups and a members attribute.  The
 *    members attribute was eliminated as a performance improvement, but the
 *    direct_groups attribute has been added.  Both groups and direct_groups
 *    are sets.  The direct_groups attribute indicates which groups the user/
 *    group is an immediate member of.  The groups attribute indicates which
 *    groups the user/group is a member of (immediate or otherwise).  The
 *    groups attribute is a flattened set.  When a user/group is dropped from
 *    a group, the group is removed from both the direct_groups and groups
 *    attributes for the user/group.  Then that change is propagated to other
 *    users/groups.
 *    For example,  if U1 is directly in G1 and G1 is directly in G2 and G1
 *    is dropped from G2, G2 is removed from G1's direct_groups and groups
 *    attributes and G2 is also removed from U1's groups attribute.
 */
int
au_drop_member (MOP group, MOP member)
{
  int syserr = NO_ERROR, error = NO_ERROR;
  DB_VALUE groupvalue, member_name_val;
  DB_SET *groups, *member_groups, *member_direct_groups;
  int save;
  char *member_name;

  AU_DISABLE (save);
  db_make_object (&groupvalue, group);

  if ((syserr = au_get_set (member, "groups", &member_groups)) == NO_ERROR)
    {
      if (!set_ismember (member_groups, &groupvalue))
	{
	  error = ER_AU_MEMBER_NOT_FOUND;
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 0);
	}
      else if ((error = au_get_set (group, "groups", &groups)) == NO_ERROR)
	{
	  if (set_ismember (groups, &groupvalue))
	    {
	      error = ER_AU_MEMBER_NOT_FOUND;
	      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 0);
	    }
	  else
	    {
	      error = obj_lock (member, 1);
	      if (error == NO_ERROR)
		{
		  error = au_get_set (member, "direct_groups",
				      &member_direct_groups);
		}

	      if (error == NO_ERROR)
		{
		  if ((error = db_get (member, "name", &member_name_val))
		      == NO_ERROR)
		    {
		      if (DB_IS_NULL (&member_name_val))
			{
			  member_name = NULL;
			}
		      else
			{
			  member_name =
			    (char *) db_get_string (&member_name_val);
			}
		      if ((error =
			   db_set_drop (member_direct_groups,
					&groupvalue)) == NO_ERROR)
			{
			  error = au_compute_groups (member, member_name);
			}
		      db_value_clear (&member_name_val);
		    }
		  set_free (member_direct_groups);
		}
	    }
	  set_free (groups);
	}
      set_free (member_groups);
    }
  AU_ENABLE (save);
  return (error);
}
#endif

int
au_rename_table (const char *old_name, const char *new_name)
{
  int save;
  int error = NO_ERROR;
  int ret;
  DB_SESSION *session = NULL;
  DB_QUERY_RESULT *result;
  DB_VALUE value[2];
  char query_buf[1024];

  AU_DISABLE (save);

  sprintf (query_buf,
	   "update [%s] set [table_name] = ? where [table_name] = ?;",
	   CT_AUTH_NAME);
  session = db_open_buffer (query_buf);

  if (session == NULL)
    {
      goto error;
    }

  db_make_string (&value[0], new_name);
  db_make_string (&value[1], old_name);
  db_push_values (session, 2, &value[0]);

  if (db_compile_statement (session) != NO_ERROR)
    {
      error = er_errid ();
      db_close_session (session);
      goto error;
    }

#if 1				/* TODO - delete me someday */
  assert (session->groupid == NULL_GROUPID);	/* is DML derived from DDL */
#endif
  ret = db_execute_statement (session, &result);
  if (ret < 0)
    {
      error = er_errid ();
      db_close_session (session);
      goto error;
    }

  db_query_end (result);
  db_close_session (session);
  pr_clear_value (&value[0]);
  pr_clear_value (&value[1]);

error:
  AU_ENABLE (save);
  return error;

}

int
au_drop_table (const char *table_name)
{
  int save;
  int error = NO_ERROR;
  int ret;
  DB_SESSION *session = NULL;
  DB_QUERY_RESULT *result;
  DB_VALUE value;
  char query_buf[1024];

  AU_DISABLE (save);

  sprintf (query_buf, "delete from [%s] where [table_name] = ?;",
	   CT_AUTH_NAME);
  session = db_open_buffer (query_buf);

  if (session == NULL)
    {
      goto error;
    }

  db_make_string (&value, table_name);
  db_push_values (session, 1, &value);

  if (db_compile_statement (session) != NO_ERROR)
    {
      error = er_errid ();
      db_close_session (session);
      goto error;
    }

#if 1				/* TODO - delete me someday */
  assert (session->groupid == NULL_GROUPID);	/* is DML derived from DDL */
#endif
  ret = db_execute_statement (session, &result);
  if (ret < 0)
    {
      error = er_errid ();
      db_close_session (session);
      goto error;
    }

  db_query_end (result);
  db_close_session (session);
  pr_clear_value (&value);

error:
  AU_ENABLE (save);
  return error;
}

/*
 * au_drop_user - Drop a user from the database.
 *   return: error code
 *   user(in): user object
 *
 * Note:
 *
 *    This should only be called with DBA privilidges.
 *    The db_user class used to have a groups and a members attribute.  The
 *    members attribute was eliminated as a performance improvement, but the
 *    direct_groups attribute has been added.  Both groups and direct_groups
 *    are sets.  The direct_groups attribute indicates which groups the user/
 *    group is an immediate member of.  The groups attribute indicates which
 *    groups the user/group is a member of (immediate or otherwise).  The
 *    groups attribute is a flattened set.  When a user/group is dropped,
 *    the user/group is removed from both the direct_groups and groups
 *    attributes for all users.  For example,  if U1 is directly in G1 and G1
 *    is directly in G2 and G1 is dropped, G1 & G2 are removed from U1's
 *    groups attribute and G1 is also removed from U1's direct_groups
 *    attribute.
 */
int
au_drop_user (MOP user)
{
  int save;
  DB_SESSION *session = NULL;
  DB_VALUE val[2], value;
  int error = NO_ERROR;
  DB_QUERY_RESULT *result;
  int i;
  DB_VALUE name;
  char *user_name;
  const char *class_name[] = {
    /*
     * drop user command can be called only by DBA group,
     * so we can use query for _db_class directly
     */
    "db_table", NULL
  };
  char query_buf[1024];

  AU_DISABLE (save);

  if (Au_dba_user != NULL && !au_is_dba_group_member (Au_user))
    {
      error = ER_AU_DBA_ONLY;
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 1, "drop_user");
      goto error;
    }

  /* check if user is dba/public or current user */
  if (user == Au_dba_user || user == Au_public_user || user == Au_user)
    {
      db_make_null (&name);
      error = obj_get (user, "name", &name);
      if (error != NO_ERROR)
	{
	  goto error;
	}

      error = ER_AU_CANT_DROP_USER;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 1,
	      db_get_string (&name));
      goto error;
    }

  /* check if user owns class/vclass/serial */
  for (i = 0; class_name[i] != NULL; i++)
    {
      sprintf (query_buf,
	       "select count(*) from [%s] where [owner] = ?;", class_name[i]);
      session = db_open_buffer (query_buf);
      if (session == NULL)
	{
	  goto error;
	}

      db_make_object (&val[0], user);
      db_push_values (session, 1, &val[0]);
      if (db_compile_statement (session) != NO_ERROR)
	{
	  error = er_errid ();
	  db_close_session (session);
	  goto error;
	}

      error = db_execute_statement (session, &result);
      if (error < 0)
	{
	  db_close_session (session);
	  goto error;
	}

      error = db_query_first_tuple (result);
      if (error < 0)
	{
	  db_query_end (result);
	  db_close_session (session);
	  goto error;
	}

      db_make_bigint (&value, 0);
      error = db_query_get_tuple_value (result, 0, &value);
      if (error != NO_ERROR)
	{
	  db_query_end (result);
	  db_close_session (session);
	  goto error;
	}

      if (DB_GET_BIGINT (&value) > 0)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
		  ER_AU_USER_HAS_DATABASE_OBJECTS, 0);
	  db_query_end (result);
	  db_close_session (session);
	  error = ER_AU_USER_HAS_DATABASE_OBJECTS;
	  goto error;
	}

      db_query_end (result);
      db_close_session (session);
      pr_clear_value (&val[0]);
    }

  /*
   * could go through classes created by this user and change ownership
   * to the DBA ? - do this as the classes are referenced instead
   */

  db_make_null (&name);
  error = obj_get (user, "name", &name);
  user_name = db_get_string (&name);

  error = au_delete_auth_by_user (user_name);

  error = obj_delete (user);
  if (error == NO_ERROR)
    {
      remove_user_cache_references (user);
    }

error:
  AU_ENABLE (save);
  return error;
}

/*
 * AUTHORIZATION CACHING
 */
static int
apply_auth_grants (MOP auth, unsigned int *bits)
{
  int error = NO_ERROR;
  DB_VALUE value;

  obj_get (auth, "select_priv", &value);
  *bits |= DB_GET_INTEGER (&value);

  obj_get (auth, "insert_priv", &value);
  *bits |= DB_GET_INTEGER (&value) << 1;

  obj_get (auth, "update_priv", &value);
  *bits |= DB_GET_INTEGER (&value) << 2;

  obj_get (auth, "delete_priv", &value);
  *bits |= DB_GET_INTEGER (&value) << 3;

  obj_get (auth, "alter_priv", &value);
  *bits |= DB_GET_INTEGER (&value) << 4;

  return error;
}

/*
 * update_cache -  This is the main function for parsing all of
 *                 the authorization information for a particular class and
 *                 caching it in the class structure.
 *                 This will be called once after each successful
 *                 read/write lock. It needs to be fast.
 *   return: error code
 *   classop(in):  class MOP to authorize
 *   sm_class(in): direct pointer to class structure
 *   cache(in):
 */
static int
update_cache (MOP classop, SM_CLASS * sm_class, AU_CLASS_CACHE * cache)
{
  int error = NO_ERROR;
  int save;
  MOP auth;
  unsigned int *bits;

  /*
   * must disable here because we may be updating the cache of the system
   * objects and we need to read them in order to update etc.
   */
  AU_DISABLE (save);

  bits = &cache->data[Au_cache_index];

  /* initialize the cache bits */
  *bits = AU_NO_AUTHORIZATION;

  if (sm_issystem (sm_class))
    {
      /* might want to allow grant on system classes */
      *bits = AU_FULL_AUTHORIZATION;
    }
  else if (sm_class->owner == NULL)
    {
      /* This shouldn't happen - assign it to the DBA */
      error = ER_AU_CLASS_WITH_NO_OWNER;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
    }
  else if (au_is_dba_group_member (Au_user))
    {
      *bits = AU_FULL_AUTHORIZATION;
    }
  else if (Au_user == sm_class->owner)
    {
      /* might want to allow grant/revoke on self */
      *bits = AU_FULL_AUTHORIZATION;
    }
  else
    {
      auth = au_get_auth (Au_user, classop);
      if (auth == NULL)
	{
	  error = ER_AU_ACCESS_ERROR;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 2, "db_auth",
		  AU_USER_CLASS_NAME);
	}
      else
	{
	  error = apply_auth_grants (auth, bits);
	}
    }

  AU_ENABLE (save);

  return (error);
}

/*
 * appropriate_error -  This selects an appropriate error code to correspond
 *                      with an authorization failure of a some type
 *   return: error code
 *   bits(in): authorization type
 *   requested(in):
 * TODO : LP64
 */
static int
appropriate_error (unsigned int bits, unsigned int requested)
{
  int error;
  unsigned int mask, atype;
  int i;

  /*
   * Since we don't currently have a way of indicating multiple
   * authorization failures, select the first one in the
   * bit vector that causes problems.
   * Order the switch statement so that its roughly in dependency order
   * to result in better error message.  The main thing is that
   * SELECT should be first.
   */

  error = NO_ERROR;
  mask = 1;
  for (i = 0; i < AU_GRANT_SHIFT && !error; i++)
    {
      if (requested & mask)
	{
	  /* we asked for this one */
	  if ((bits & mask) == 0)
	    {
	      /* but it wasn't available */
	      switch (mask)
		{
		case AU_SELECT:
		  error = ER_AU_SELECT_FAILURE;
		  break;
		case AU_ALTER:
		  error = ER_AU_ALTER_FAILURE;
		  break;
		case AU_UPDATE:
		  error = ER_AU_UPDATE_FAILURE;
		  break;
		case AU_INSERT:
		  error = ER_AU_INSERT_FAILURE;
		  break;
		case AU_DELETE:
		  error = ER_AU_DELETE_FAILURE;
		  break;
		default:
		  error = ER_AU_AUTHORIZATION_FAILURE;
		  break;
		}
	    }
	}
      mask = mask << 1;
    }

  if (!error)
    {
      /* we seemed to have all the basic authorizations, check grant options */
      mask = 1 << AU_GRANT_SHIFT;
      for (i = 0; i < AU_GRANT_SHIFT && !error; i++)
	{
	  if (requested & mask)
	    {
	      /* we asked for this one */
	      if ((bits & mask) == 0)
		{
		  /*
		   * But it wasn't available, convert this back down to the
		   * corresponding basic type and select an appropriate error.
		   *
		   * NOTE: We need to add type specific errors here !
		   *
		   */
		  atype = mask >> AU_GRANT_SHIFT;
		  switch (atype)
		    {
		    case AU_SELECT:
		      error = ER_AU_NO_GRANT_OPTION;
		      break;
		    case AU_ALTER:
		      error = ER_AU_NO_GRANT_OPTION;
		      break;
		    case AU_UPDATE:
		      error = ER_AU_NO_GRANT_OPTION;
		      break;
		    case AU_INSERT:
		      error = ER_AU_NO_GRANT_OPTION;
		      break;
		    case AU_DELETE:
		      error = ER_AU_NO_GRANT_OPTION;
		      break;
		    default:
		      error = ER_AU_NO_GRANT_OPTION;
		      break;
		    }
		}
	    }
	  mask = mask << 1;
	}
    }

  return (error);
}

/*
 * check_grant_option - Checks to see if a class has the grant option for
 *                      a particular authorization type.
 *                      Called by au_grant and au_revoke
 *   return: error code
 *   classop(in):  MOP of class being examined
 *   class(in): direct pointer to class structure
 *   type(in): type of authorization being checked
 *
 * TODO: LP64
 */
static int
check_grant_option (MOP classop, SM_CLASS * sm_class, DB_AUTH type)
{
  int error = NO_ERROR;
  AU_CLASS_CACHE *cache;
  unsigned int cache_bits;
  unsigned int mask;

  /*
   * this potentially can be called during initialization when we don't
   * actually have any users yet.  If so, assume its ok
   */
  if (Au_cache_index < 0)
    {
      return NO_ERROR;
    }

  cache = (AU_CLASS_CACHE *) sm_class->auth_cache;
  if (sm_class->auth_cache == NULL)
    {
      cache = au_install_class_cache (sm_class);
      if (cache == NULL)
	{
	  return er_errid ();
	}
    }
  cache_bits = cache->data[Au_cache_index];

  if (cache_bits == AU_CACHE_INVALID)
    {
      if (update_cache (classop, sm_class, cache))
	{
	  return er_errid ();
	}
      cache_bits = cache->data[Au_cache_index];
    }

  /* build the complete bit mask */
  mask = type | (type << AU_GRANT_SHIFT);
  if ((cache_bits & mask) != mask)
    {
      error = appropriate_error (cache_bits, mask);
      if (error)
	{
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 0);
	}
    }

  return error;
}


/*
 * GRANT OPERATION
 */

/*
 * au_grant - This is the primary interface function for granting authorization
 *            on classes.
 *   return: error code
 *   user(in): user receiving the grant
 *   class_mop(in): class being authorized
 *   type(in): type of authorization
 *   grant_option(in): non-zero if grant option is also being given
 */
int
au_grant (MOP user, MOP class_mop, DB_AUTH type, UNUSED_ARG bool grant_option)
{
  int error = NO_ERROR;
  int save = 0;
  SM_CLASS *classobj;

  AU_DISABLE (save);
  if (user == Au_user)
    {
      /*
       * Treat grant to self condition as a success only. Although this
       * statement is a no-op, it is not an indication of no-success.
       * The "privileges" are indeed already granted to self.
       * Note: Revoke from self is an error, because this cannot be done.
       */
    }
  else if ((error = au_fetch_class_force (class_mop, &classobj,
					  S_LOCK)) == NO_ERROR)
    {
      if (classobj->owner == user)
	{
	  error = ER_AU_CANT_GRANT_OWNER;
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 0);
	}
      else if ((error = check_grant_option (class_mop, classobj,
					    type)) == NO_ERROR)
	{
	  if (type)
	    {
	      error =
		au_insert_update_auth (Au_user, user, class_mop, type, true);
	    }
	  reset_cache_for_user_and_class (classobj);

	  /*
	   * Make sure any cached parse trees are rebuild.  This probably
	   * isn't necessary for GRANT, only REVOKE.
	   */
	  sm_bump_local_schema_version ();
	}
    }

#if 0
fail_end:
#endif
  AU_ENABLE (save);
  return (error);
}


/*
 * REVOKE OPERATION
 */

/*
 * au_revoke - This is the primary interface function for
 *             revoking authorization
 *   return: error code
 *   user(in): user being revoked
 *   class_mop(in): class being revoked
 *   type(in): type of authorization being revoked
 *
 * Note: The authorization of the given type on the given class is removed
 *       from the authorization info stored with the given user.  If this
 *       user has the grant option for this type and has granted authorization
 *       to other users, the revoke will be recursively propagated to all
 *       affected users.
 *
 * TODO : LP64
 */
int
au_revoke (MOP user, MOP class_mop, DB_AUTH type)
{
  int error;
  MOP auth;
  unsigned int bits = 0;
  DB_SET *grants = NULL;
  int save = 0;
  SM_CLASS *classobj;

  AU_DISABLE (save);
  if (user == Au_user)
    {
      error = ER_AU_CANT_REVOKE_SELF;
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 0);
      goto fail_end;
    }

  error = au_fetch_class_force (class_mop, &classobj, S_LOCK);
  if (error != NO_ERROR)
    {
      goto fail_end;
    }

  if (classobj->owner == user)
    {
      error = ER_AU_CANT_REVOKE_OWNER;
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 0);
      goto fail_end;
    }

  error = check_grant_option (class_mop, classobj, type);
  if (error != NO_ERROR)
    {
      goto fail_end;
    }

#if defined(SA_MODE)
  if (catcls_Enable == true)
#endif /* SA_MODE */
    {
      auth = au_get_auth (user, class_mop);
      if (auth == NULL)
	{
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_AU_ACCESS_ERROR, 2,
		  "db_auth", AU_USER_CLASS_NAME);
	  /* do not return error; go ahead */
	}
      else
	{
	  error = apply_auth_grants (auth, &bits);
	  if (error != NO_ERROR)
	    {
	      goto fail_end;
	    }
	}

      if ((bits & (int) type) == 0)
	{
	  error = ER_AU_GRANT_NOT_FOUND;
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 0);
	}
      else
	{
	  error =
	    au_insert_update_auth (Au_user, user, class_mop, type, false);

	  reset_cache_for_user_and_class (classobj);
	  sm_bump_local_schema_version ();
	}
    }

fail_end:
  if (grants != NULL)
    {
      set_free (grants);
    }
  AU_ENABLE (save);
  return (error);
}

/*
 * MISC UTILITIES
 */

/*
 * au_change_owner - This changes the owning user of a class.
 *                   This should be called only by the DBA.
 *   return: error code
 *   classmop(in): class whose owner is to change
 *   owner(in): new owner
 */
int
au_change_owner (MOP classmop, MOP owner)
{
  int error = NO_ERROR;
  SM_CLASS *class_;
  int save;

  AU_DISABLE (save);

  /* Remove group concept in db_user. */
  /* So, check Au_user itself is dba, not a group member. */
  if (!au_is_dba_group_member (Au_user))
    {
      error = ER_AU_DBA_ONLY;
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 1, "change_owner");
    }
  else
    {
      error = au_fetch_class_force (classmop, &class_, X_LOCK);
      if (error == NO_ERROR)
	{
	  /* Change class owner */
	  class_->owner = owner;
	  error = locator_flush_class (classmop);
	}
    }
#if 0
exit_on_error:
#endif
  AU_ENABLE (save);
  return (error);
}

/*
 * au_change_owner_helper - interface to au_change_owner
 *   return: none
 *   obj(in): class whose owner is to change
 *   returnval(out): return value
 *   class(in):
 *   owner(in): new owner
 */
int
au_change_owner_helper (UNUSED_ARG MOP obj, DB_VALUE * returnval,
			DB_VALUE * class_, DB_VALUE * owner)
{
  MOP user, classmop;
  int error = NO_ERROR;
  char *class_name = NULL, *owner_name = NULL;
  SM_CLASS *clsobj;

  db_make_null (returnval);

  if (DB_IS_NULL (class_) || !IS_STRING (class_)
      || (class_name = db_get_string (class_)) == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_AU_INVALID_CLASS, 1, "");
      return ER_AU_INVALID_CLASS;
    }
  if (DB_IS_NULL (owner) || !IS_STRING (owner)
      || (owner_name = db_get_string (owner)) == NULL)
    {
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_AU_INVALID_USER, 1, "");
      return ER_AU_INVALID_USER;
    }

  classmop = sm_find_class (class_name);
  if (classmop == NULL)
    {
      return er_errid ();
    }

  error = au_fetch_class_force (classmop, &clsobj, X_LOCK);
  if (error != NO_ERROR)
    {
      return error;
    }

  /* To change the owner of a system class is not allowed. */
  if (sm_issystem (clsobj))
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE,
	      ER_AU_CANT_ALTER_OWNER_OF_SYSTEM_CLASS, 0);
      return ER_AU_CANT_ALTER_OWNER_OF_SYSTEM_CLASS;
    }

  user = au_find_user (owner_name);
  if (user == NULL)
    {
      return er_errid ();
    }

  error = au_change_owner (classmop, user);
  return error;
}


/*
 * au_get_class_owner - This access the user object that is the owner of
 *                      the class.
 *   return: user object (owner of class_)
 *   classmop(in): class object
 */
MOP
au_get_class_owner (MOP classmop)
{
  MOP owner = NULL;
  SM_CLASS *class_;

  /* should we disable authorization here ? */
  /*
   * should we allow the class owner to be known if the active user doesn't
   * have select authorization ?
   */

  if (au_fetch_class_force (classmop, &class_, S_LOCK) == NO_ERROR)
    {
      owner = class_->owner;
      if (owner == NULL)
	{
	  /* shouln't we try to update the class if the owner wasn't set ? */
	  owner = Au_dba_user;
	}
    }

  return (owner);
}

/*
 * au_check_user - This is used to check for a currently valid user for some
 *                 operations that are not strictly based on
 *                 any particular class.
 *    return: error code
 */
int
au_check_user (void)
{
  int error = NO_ERROR;

  if (Au_user == NULL)
    {
      error = ER_AU_INVALID_USER;
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 1, "");
    }

  return (error);
}

/*
 * au_user_name - Returns the name of the current user, the string must be
 *                freed with ws_free_string (db_string_free).
 *   return: user name (NULL if error)
 *
 * Note: Note that this is what should always be used to get the active user.
 *       Au_user_name is only used as a temporary storage area during login.
 *       Once the database is open, the active user is determined by Au_user
 *       and Au_user_name doesn't necessarily track this.
 */
char *
au_user_name (void)
{
  DB_VALUE value;
  char *name = NULL;

  if (Au_user == NULL)
    {
      /*
       * Database hasn't been started yet, return the registered name
       * if any.
       */
      if (strlen (Au_user_name) == 0)
	{
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, ER_AU_NO_USER_LOGGED_IN,
		  0);
	}
      else
	{
	  name = ws_copy_string (Au_user_name);
	  /*
	   * When this function is called before the workspace memory
	   * manager was not initialized in the case of client
	   * initialization(db_restart()), ws_copy_string() will return
	   * NULL.
	   */
	}
    }
  else
    {
      if (obj_get (Au_user, "name", &value) == NO_ERROR)
	{
	  if (!IS_STRING (&value))
	    {
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_AU_CORRUPTED, 0);
	      pr_clear_value (&value);
	    }
	  else if (DB_IS_NULL (&value))
	    {
	      name = NULL;
	    }
	  else
	    {
	      name = db_get_string (&value);
	      name = ws_copy_string (name);
	      pr_clear_value (&value);
	    }
	}
    }

  return name;
}

/*
 * CLASS ACCESSING
 */

/*
 * is_protected_class - This is a hack to detect attempts to modify one
 *                      of the protected system classes.
 *   return: non-zero if class is protected
 *   classmop(in): class MOP
 *   class(in): class structure
 *   auth(in): authorization type
 *
 * Note: This is necessary because normally when DBA is logged in,
 *       authorization is effectively disabled.
 *       This should be handled by another "system" level of authorization.
 */
static int
is_protected_class (MOP classmop, SM_CLASS * sm_class, DB_AUTH auth)
{
  int illegal = 0;

  if (classmop == Au_authorizations_class
      || IS_CATALOG_CLASS (sm_class->header.name))
    {
      illegal = auth & (AU_ALTER | AU_DELETE | AU_INSERT | AU_UPDATE);
    }
  else if (sm_issystem (sm_class))
    {
      /* if the class is a system class_, can't alter */
      illegal = auth & (AU_ALTER);
    }
  return illegal;
}

/*
 * check_authorization - This is the core routine for checking authorization
 *                       on a class.
 *   return: error code
 *   classobj(in): class MOP
 *   class(in): class structure
 *   type(in): authorization type
 *
 * TODO : LP64
 */
static int
check_authorization (MOP classobj, SM_CLASS * sm_class, DB_AUTH type)
{
  int error = NO_ERROR;
  AU_CLASS_CACHE *cache;
  unsigned int bits;

  /*
   * Callers generally check Au_disable already to avoid the function call.
   * Check it again to be safe, at this point, it isn't going to add anything.
   */
  if (Au_disable)
    {
      return NO_ERROR;
    }

  /* try to catch attempts by even the DBA to update a protected class */
  if (sm_issystem (sm_class) && is_protected_class (classobj, sm_class, type))
    {
      error = appropriate_error (0, type);
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 0);
    }
  else
    {
      cache = (AU_CLASS_CACHE *) sm_class->auth_cache;
      if (cache == NULL)
	{
	  cache = au_install_class_cache (sm_class);
	  if (cache == NULL)
	    {
	      return er_errid ();
	    }
	}
      bits = cache->data[Au_cache_index];

      if ((bits & type) != type)
	{
	  if (bits == AU_CACHE_INVALID)
	    {
	      /* update the cache and try again */
	      error = update_cache (classobj, sm_class, cache);
	      if (error == NO_ERROR)
		{
		  bits = cache->data[Au_cache_index];
		  if ((bits & type) != type)
		    {
		      error = appropriate_error (bits, type);
		      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 0);
		    }
		}
	    }
	  else
	    {
	      error = appropriate_error (bits, type);
	      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 0);
	    }
	}
    }

  return error;
}

/*
 * fetch_class - Work function for au_fetch_class.
 *   return: error code
 *   op(in): class or instance MOP
 *   return_mop(out): returned class MOP
 *   return_class(out): returned class structure
 *   fetchmode(in): desired fetch/locking mode
 */
static int
fetch_class (MOP op, MOP * return_mop, SM_CLASS ** return_class, LOCK lock)
{
  int error = NO_ERROR;
  MOP classmop = NULL;
  SM_CLASS *class_ = NULL;

  *return_mop = NULL;
  *return_class = NULL;

  if (op == NULL)
    {
      return ER_FAILED;
    }

  classmop = NULL;
  class_ = NULL;

  if (locator_is_class (op))
    {
      classmop = op;
    }
  else if (op->class_mop != NULL)
    {
      classmop = op->class_mop;
    }

#if 1				/* TODO - trace */
  assert (classmop != NULL);
#endif

  if (classmop != NULL)
    {
      class_ = (SM_CLASS *) locator_fetch_class (classmop, lock);
    }

  if (lock > S_LOCK && class_ != NULL)
    {
      ws_dirty (classmop);
    }

  /* I've seen cases where locator_fetch has an error but doesn't return NULL ?? */
  /* this is debug only, take out in production */
  /*
   * if (class_ != NULL && Db_error != NO_ERROR)
   * au_log(Db_error, "Inconsistent error handling ?");
   */

  if (class_ == NULL)
    {
      /* does it make sense to check WS_ISMARK_DELETED here ? */
      error = er_errid ();
      /* !!! do we need to mask the error here ? */

      /*
       * if the object was deleted, set the workspace bit so we can avoid this
       * in the future
       */
      if (error == ER_HEAP_UNKNOWN_OBJECT)
	{
	  if (classmop != NULL)
	    {
	      WS_SET_DELETED (classmop);
	    }
	  else
	    {
	      WS_SET_DELETED (op);
	    }
	}
      else if (error == NO_ERROR)
	{
	  /* return NO_ERROR only if class_ is not null. */
	  error = ER_FAILED;
	}
    }
  else
    {
      assert (class_->header.name == NULL
	      || strlen (class_->header.name) >= 0);

      *return_mop = classmop;
      *return_class = class_;
    }

  return error;
}

/*
 * au_fetch_class - This is the primary function for accessing a class
 *   return: error code
 *   op(in): class or instance
 *   class_ptr(out): returned pointer to class structure
 *   lock(in): type of fetch/lock to obtain
 *   type(in): authorization type to check
 *
 */
int
au_fetch_class (MOP op, SM_CLASS ** class_ptr, LOCK lock, DB_AUTH type)
{
  int error = NO_ERROR;
  SM_CLASS *class_;
  MOP classmop;

  if (class_ptr != NULL)
    {
      *class_ptr = NULL;
    }

  if (op == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  if (Au_user == NULL && !Au_disable)
    {
      error = ER_AU_INVALID_USER;
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 1, "");
      return error;
    }

  /* go through the usual fetch process */
  error = fetch_class (op, &classmop, &class_, lock);
  if (error != NO_ERROR)
    {
      return error;
    }

  if (Au_disable || !(error = check_authorization (classmop, class_, type)))
    {
      if (class_ptr != NULL)
	{
	  *class_ptr = class_;
	}
    }

  return error;
}

/*
 * au_fetch_class_force - This is like au_fetch_class except that it will
 *                        not check for authorization.
 *   return: error code
 *   op(in): class or instance MOP
 *   class(out): returned pointer to class structure
 *   fetchmode(in): desired operation
 *
 * Note: Used in a few special cases where authorization checking is
 *       to be disabled.
 */
int
au_fetch_class_force (MOP op, SM_CLASS ** class_, LOCK lock)
{
  MOP classmop;

  return (fetch_class (op, &classmop, class_, lock));
}

/*
 * INSTANCE ACCESSING
 */

/*
 * fetch_instance - Work function for au_fetch_instance.
 *   return: error code
 *   op(in): instance MOP
 *   obj_ptr(out): returned pointer to object
 *   fetchmode(in): desired operation
 */
static int
fetch_instance (MOP op, MOBJ * obj_ptr, LOCK lock)
{
  int error = NO_ERROR;
  MOBJ obj = NULL;
  int pin;
  int save;

  if (obj_ptr != NULL)
    {
      *obj_ptr = NULL;
    }

  if (op == NULL)
    {
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, ER_OBJ_INVALID_ARGUMENTS, 0);
      return ER_OBJ_INVALID_ARGUMENTS;
    }

  /* DO NOT PUT ANY RETURNS FROM HERE UNTIL THE AU_ENABLE */
  AU_DISABLE (save);

  pin = ws_pin (op, 1);
  switch (lock)
    {
    case S_LOCK:
      obj = locator_fetch_instance (op);
      break;
    case X_LOCK:
      obj = locator_update_instance (op);
      break;
    case NA_LOCK:
    case NULL_LOCK:
    case U_LOCK:
      assert (false);
      obj = NULL;
      break;
    }

  (void) ws_pin (op, pin);

  if (obj == NULL)
    {
      /* does it make sense to check WS_ISMARK_DELETED here ? */
      error = er_errid ();

      /*
       * if the object was deleted, set the workspace bit so we can avoid this
       * in the future
       */
      if (error == ER_HEAP_UNKNOWN_OBJECT)
	{
	  WS_SET_DELETED (op);
	}
      else if (error == NO_ERROR)
	{
	  /* return NO_ERROR only if obj is not null. */
	  error = ER_FAILED;
	}
    }
  else if (obj_ptr != NULL)
    {
      *obj_ptr = obj;
    }

  AU_ENABLE (save);

  return error;
}

/*
 * au_fetch_instance - This is the primary interface function for accessing
 *                     an instance.
 *   return: error code
 *   op(in): instance MOP
 *   obj_ptr(in):returned pointer to instance memory
 *   mode(in): access type
 *   type(in): authorization type
 *
 * Note: Fetch the object from the database if necessary, update the class
 *       authorization cache if necessary and check authorization for the
 *       desired operation.
 *
 * Note: If op is a VMOP au_fetch_instance will return set obj_ptr as a
 *       pointer to the BASE INSTANCE memory which is not the instance
 *       associated with op. Therefore, the object returned is not necessarily
 *       the contents of the supplied MOP.
 */
/*
 * TODO We need to carefully examine all callers of au_fetch_instance and make
 *      sure they know that the object returned is not necessarily the
 *      contents of the supplied MOP.
 */
int
au_fetch_instance (MOP op, MOBJ * obj_ptr, LOCK lock, DB_AUTH type)
{
  int error = NO_ERROR;
  SM_CLASS *class_;
  MOP classmop;

  if (Au_user == NULL && !Au_disable)
    {
      error = ER_AU_INVALID_USER;
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 1, "");
      return error;
    }

  error = fetch_class (op, &classmop, &class_, S_LOCK);
  if (error != NO_ERROR)
    {
      /*
       * the class was deleted, make sure the instance MOP also gets
       * the deleted bit set so the upper levels can depend on this
       * behavior
       */
      if (error == ER_HEAP_UNKNOWN_OBJECT)
	{
	  WS_SET_DELETED (op);
	}
      return error;
    }

  if (Au_disable || !(error = check_authorization (classmop, class_, type)))
    {
      error = fetch_instance (op, obj_ptr, lock);
    }

  return error;
}

/*
 * au_fetch_instance_force - Like au_fetch_instance but doesn't check
 *                           for authorization.  Used in special circumstances
 *                           when authorization is disabled.
 *   return: error code
 *   op(in): instance MOP
 *   obj_ptr(out): returned instance memory pointer
 *   fetchmode(in): access type
 */
int
au_fetch_instance_force (MOP op, MOBJ * obj_ptr, LOCK lock)
{
  return (fetch_instance (op, obj_ptr, lock));
}


/*
 * LOGIN/LOGOUT
 */

/*
 * au_disable_passwords -
 *    return: none
 */
void
au_disable_passwords (void)
{
  Au_ignore_passwords = 1;
}

void
au_enable_passwords (void)
{
  Au_ignore_passwords = 0;
}

/*
 * au_set_user -
 *   return: error code
 *   newuser(in):
 */
int
au_set_user (MOP newuser)
{
  int error = NO_ERROR;
  int index;

  if (newuser != NULL && newuser != Au_user)
    {
      if (!(error = au_find_user_cache_index (newuser, &index, 1)))
	{

	  Au_user = newuser;
	  Au_cache_index = index;

	  /*
	   * it is important that we don't call sm_bump_local_schema_version() here
	   * because this function is called during the compilation of vclasses
	   */

	  /*
	   * Entry-level SQL specifies that the schema name is the same as
	   * the current user authorization name.  In any case, this is
	   * the place to set the current schema since the user just changed.
	   */
	  error = sc_set_current_schema (Au_user);
	}
    }
  return (error);
}

/*
 * au_perform_login - This changes the current user using the supplied
 *                    name & password.
 *   return: error code
 *   name(in): user name
 *   password(in): user password
 *
 * Note: It is called both by au_login() and au_start().
 *       Once the name/password have been validated, it calls au_set_user()
 *       to set the user object and calculate the authorization cache index.
 *       Assumes authorization has been disabled.
 */
int
au_perform_login (const char *dbuser, const char *dbpassword,
		  bool ignore_dba_privilege)
{
  int error = NO_ERROR;
  MOP user;
  DB_VALUE value;
  const char *pass;

  if (dbuser == NULL || strlen (dbuser) == 0)
    {
      error = ER_AU_NO_USER_LOGGED_IN;
      er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 0);
    }
  else
    {
      user = au_find_user (dbuser);
      if (user == NULL)
	{
	  error = ER_AU_INVALID_USER;
	  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error, 1, dbuser);
	}
      else
	{
	  if (obj_get (user, "password", &value) != NO_ERROR)
	    {
	      error = ER_AU_CORRUPTED;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	    }
	  else
	    {
	      /*
	       * hack, allow password checking to be turned off in certain
	       * cases, like the utility programs that will always run in DBA
	       * mode but don't want to have to enter a password, also
	       * if a sucessful DBA login ocurred, allow users to be
	       * changed without entering passwords
	       */

	      if (!Au_ignore_passwords
		  && (Au_user == NULL
		      || !au_is_dba_group_member (Au_user)
		      || ignore_dba_privilege))
		{
		  pass = NULL;
		  if (IS_STRING (&value))
		    {
		      if (DB_IS_NULL (&value))
			{
			  pass = NULL;
			}
		      else
			{
			  pass = db_get_string (&value);
			}
		    }

		  if (pass != NULL && strlen (pass))
		    {
		      /* the password is present and must match */
		      if ((dbpassword == NULL) || (strlen (dbpassword) == 0)
			  || !match_password (dbpassword,
					      db_get_string (&value)))
			{
			  error = ER_AU_INVALID_PASSWORD;
			  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error,
				  0);
			}
		    }
		  else
		    {
		      /*
		       * the password in the user object is effectively NULL,
		       * only accept the login if the user supplied an empty
		       * password.
		       * Formerly any password string was accepted
		       * if the stored password was NULL which is
		       * not quite right.
		       */
		      if (dbpassword != NULL && strlen (dbpassword))
			{
			  error = ER_AU_INVALID_PASSWORD;
			  er_set (ER_WARNING_SEVERITY, ARG_FILE_LINE, error,
				  0);
			}
		    }
		  if (pass != NULL)
		    {
		      ws_free_string (db_get_string (&value));
		    }
		}

	      if (error == NO_ERROR)
		{
		  error = AU_SET_USER (user);

		  /* necessary to invalidate vclass cache */
		  sm_bump_local_schema_version ();
		}
	    }
	}
    }
  return (error);
}

/*
 * au_login - Registers a user name and password for a database.
 *   return: error code
 *   name(in): user name
 *   password(in): password
 *   ignore_dba_privilege(in) : whether ignore DBA's privilege or not in login
 *
 * Note: If a database has already been restarted, the user will be validated
 *       immediately, otherwise the name and password are simply saved
 *       in global variables and the validation will ocurr the next time
 *       bo_restart is called.
 */
int
au_login (const char *name, const char *password,
	  UNUSED_ARG bool ignore_dba_privilege)
{
  int error = NO_ERROR;

  /*
   * because the database can be left open after authorization failure,
   * checking Au_root for NULL isn't a reliable way to see of the database
   * is in an "un-restarted" state.  Instead, look at BOOT_IS_CLIENT_RESTARTED
   * which is defined to return non-zero if a valid transaction is in
   * progress.
   */
  if (Au_root == NULL || !BOOT_IS_CLIENT_RESTARTED ())
    {
      /*
       * Save the name & password for later.  Allow the name to be NULL
       * and leave it unmodified.
       */
      if (name != NULL)
	{
	  if (strlen (name) >= DB_MAX_USER_LENGTH)
	    {
	      error = ER_USER_NAME_TOO_LONG;
	      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	      return error;
	    }
	  strcpy (Au_user_name, name);
	}

      if (password == NULL || strlen (password) == 0)
	{
	  strcpy (Au_user_password, "");
	}
      else
	{
	  /* store the password encrypted(SHA2) so we don't
	   * have buffers lying around with the obvious passwords in it.
	   */
	  error = encrypt_password_sha2_512 (password, Au_user_password);
	}
    }
  else
    {
      assert (false);

      error = ER_GENERIC_ERROR;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error,
	      1, "not support change user");

#if 0
      /* Change users within an active database. */
      AU_DISABLE (save);
      error = au_perform_login (name, password, ignore_dba_privilege);
      AU_ENABLE (save);
#endif
    }

  return error;
}

/*
 * au_start - This is called during the bo_resteart initialization sequence
 *            after the database has been successfully opened
 *   return: error code
 *
 * Note: Here we initialize the authorization system by finding the system
 *       objects and validating the registered user.
 */
int
au_start (void)
{
  int error = NO_ERROR;
  MOPLIST mops;
  MOP class_mop;
#if 0				/* unused */
  int save_lock_wait_in_msecs;
#endif

  /*
   * NEED TO MAKE SURE THIS IS 1 IF THE SERVER CRASHED BECAUSE WE'RE
   * GOING TO CALL db_ FUNCTIONS
   */
  db_Connect_status = DB_CONNECTION_STATUS_CONNECTED;

  /*
   * It is important not to enable authorization until after the
   * login is finished, otherwise the system will stop when it tries
   * to validate the user when accessing the authorization objects.
   */
  Au_disable = 1;

  /* locate the various system classes */
  class_mop = sm_find_class (AU_ROOT_CLASS_NAME);
  if (class_mop == NULL)
    {
      error = ER_AU_NO_AUTHORIZATION;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      return (error);
    }
  Au_authorizations_class = class_mop;

  class_mop = sm_find_class (AU_AUTH_NAME);
  if (class_mop == NULL)
    {
      error = ER_AU_NO_AUTHORIZATION;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      return (error);
    }

  Au_authorization_class = class_mop;

  class_mop = sm_find_class (AU_USER_CLASS_NAME);
  if (class_mop == NULL)
    {
      error = ER_AU_NO_AUTHORIZATION;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
      return (error);
    }
  Au_user_class = class_mop;

#if 0				/* unused */
  tran_get_tran_settings (&save_lock_wait_in_msecs, &save_tran_isolation);
  (void) tran_reset_isolation (TRAN_COMMIT_CLASS_UNCOMMIT_INSTANCE);
#endif
  mops = sm_fetch_all_objects (Au_authorizations_class, S_LOCK);
#if 0				/* unused */
  (void) tran_reset_isolation (save_tran_isolation);
#endif

  if (mops == NULL)
    {
      error = ER_AU_NO_AUTHORIZATION;
      er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
    }
  else
    {
      /* this shouldn't happen, not sure what to do */
      if (mops->next != NULL)
	{
	  error = ER_AU_MULTIPLE_ROOTS;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	}

      Au_root = mops->op;
      db_objlist_free (mops);

      if (((Au_public_user = au_find_user (AU_PUBLIC_USER_NAME)) == NULL) ||
	  ((Au_dba_user = au_find_user (AU_DBA_USER_NAME)) == NULL))
	{
	  error = ER_AU_INCOMPLETE_AUTH;
	  er_set (ER_ERROR_SEVERITY, ARG_FILE_LINE, error, 0);
	}
      else
	{
	  /*
	   * If you try to start the authorization system and
	   * there is no user logged in, you will automatically be logged in
	   * as "PUBLIC".
	   */
	  if (strlen (Au_user_name) == 0)
	    {
	      strcpy (Au_user_name, "PUBLIC");
	    }

	  error = au_perform_login (Au_user_name, Au_user_password, false);
	}
    }

  /* make sure this is off */
  Au_disable = 0;

  return (error);
}


/*
 * MIGRATION SUPPORT
 *
 * These functions provide a way to dump the authorization catalog
 * as a sequence of SQL/X statements.  When the statements are evaluated
 * by the interpreter, it will reconstruct the authorization catalog.
 */

/*
 * au_get_user_name - Shorthand function for getting name from user object.
 *                    Must remember to free the string
 *   return: user name string
 *   obj(in): user object
 */
char *
au_get_user_name (MOP obj)
{
  DB_VALUE value;
  int error;
  char *name;

  DB_MAKE_NULL (&value);
  name = NULL;

  error = obj_get (obj, "name", &value);
  if (error == NO_ERROR)
    {
      if (IS_STRING (&value) && !DB_IS_NULL (&value)
	  && db_get_string (&value) != NULL)
	{
	  name = db_get_string (&value);
	}
    }
  return (name);
}

/*
 * GRANT EXPORT
 *
 * This is in support of the authorization migration utilities.  We build
 * hierarchy of grant information and then generate a sequence of
 * SQL/X statemenets to recreate the grants.  Note that the grants have
 * to be done in order of dependencies.
 */

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * make_class_grant - Create a temporary class grant structure.
 *   return: new class grant
 *   user(in): subject user
 *   cache(in): authorization bits to grant
 */
static CLASS_GRANT *
make_class_grant (CLASS_USER * user, int cache)
{
  CLASS_GRANT *grant;

  if ((grant = (CLASS_GRANT *) malloc (sizeof (CLASS_GRANT))) != NULL)
    {
      grant->next = NULL;
      grant->user = user;
      grant->cache = cache;
    }
  return (grant);
}

/*
 * make_class_user - Create a temporary class user structure.
 *   return: new class user structure
 *   user_obj(in): pointer to actual database object for this user
 */
static CLASS_USER *
make_class_user (MOP user_obj)
{
  CLASS_USER *u;

  if ((u = (CLASS_USER *) malloc (sizeof (CLASS_USER))) != NULL)
    {
      u->next = NULL;
      u->obj = user_obj;
      u->grants = NULL;

      /*
       * This authorization of this user class structure would normally
       * be filled in by examining authorizations granted by other users.
       * The DBA user is special in that it should have full authorization
       * without being granted it by any users.  Therefore we need to set
       * the authorization explicitly before any code checks it.
       */
      if (user_obj == Au_dba_user)
	{
	  u->available_auth = AU_FULL_AUTHORIZATION;
	}
      else
	{
	  u->available_auth = 0;
	}
    }
  return (u);
}

/*
 * find_or_add_user - Adds an entry in the user list of a class authorization
 *                    structure for the user object.
 *   return: class user structures
 *   auth(in):class authorization state
 *   user_obj(in):database user object to add
 *
 * Note: If there is already an entry in the list, it returns the found entry
 */
static CLASS_USER *
find_or_add_user (CLASS_AUTH * auth, MOP user_obj)
{
  CLASS_USER *u, *last;

  for (u = auth->users, last = NULL;
       u != NULL && u->obj != user_obj; u = u->next)
    {
      last = u;
    }

  if (u == NULL)
    {
      u = make_class_user (user_obj);
      if (last == NULL)
	{
	  auth->users = u;
	}
      else
	{
	  last->next = u;
	}
    }
  return (u);
}

/*
 * add_class_grant - Makes an entry in the class authorization state
 *                   for a desired grant.
 *   return: error code
 *   auth(in): class authorization state
 *   source(in): source user object
 *   user(in): subject user object
 *   cache(in): authorization cache bits
 */
static int
add_class_grant (CLASS_AUTH * auth, MOP source, MOP user, int cache)
{
  CLASS_USER *u, *gu;
  CLASS_GRANT *g;

  u = find_or_add_user (auth, source);

  for (g = u->grants; g != NULL && g->user->obj != user; g = g->next);
  if (g == NULL)
    {
      if (source != user)
	{
	  gu = find_or_add_user (auth, user);
	  if ((g = make_class_grant (gu, cache)) == NULL)
	    return er_errid ();
	  g->next = u->grants;
	  u->grants = g;
	}
    }
  else
    {
      /*
       * this shouldn't happen, multiple grants from source should already have
       * been combined
       */
      g->cache |= cache;
    }
  return NO_ERROR;
}
#endif

/*
 * DEBUGGING FUNCTIONS
 */

#if defined(ENABLE_UNUSED_FUNCTION)
/*
 * au_print_class_auth() - Dumps authorization information for
 *                         a particular class.
 *   return: none
 *   class(in): class object
 *
 * Note: Used by the test program, should pass in a file pointer here !
 *       The AU_ROOT_CLASS_NAME class used to have a user attribute
 *       which was a set
 *       containing the object-id for all users.  The users attribute has been
 *       eliminated for performance reasons.  A query on the db_user class is
 *       new used to find all users.
 */
void
au_print_class_auth (MOP class_mop)
{
  MOP user;
  DB_SET *grants;
  int j, title, gsize;
  DB_VALUE element, value;
  char *query;
  DB_QUERY_RESULT *query_result;
  DB_QUERY_ERROR query_error;
  int error;
  DB_VALUE user_val;
  const char *qp1 = "select [%s] from [%s];";

  query = (char *) malloc (strlen (qp1) + strlen (AU_USER_CLASS_NAME) * 2);

  if (query)
    {
      sprintf (query, qp1, AU_USER_CLASS_NAME, AU_USER_CLASS_NAME);

      error = db_execute (query, &query_result, &query_error);
      /* error is row count if not negative. */
      if (error > 0)
	{
	  while (db_query_next_tuple (query_result) == DB_CURSOR_SUCCESS)
	    {
	      if (db_query_get_tuple_value (query_result, 0, &user_val) ==
		  NO_ERROR)
		{
		  user = db_get_object (&user_val);
		  title = 0;
		  obj_get (user, "authorization", &value);
		  get_grants (db_get_object (&value), &grants, 1);
		  gsize = set_size (grants);
		  for (j = 0; j < gsize; j += GRANT_ENTRY_LENGTH)
		    {
		      set_get_element (grants, GRANT_ENTRY_CLASS (j),
				       &element);
		      if (db_get_object (&element) == class_mop)
			{
			  if (!title)
			    {
			      obj_get (user, "name", &value);
			      if (db_get_string (&value) != NULL)
				{
				  fprintf (stdout,
					   msgcat_message
					   (MSGCAT_CATALOG_RYE,
					    MSGCAT_SET_AUTHORIZATION,
					    MSGCAT_AUTH_USER_NAME2),
					   db_get_string (&value));
				}
			      pr_clear_value (&value);
			      title = 1;
			    }
			  au_print_grant_entry (grants, j, stdout);
			}
		    }
		  set_free (grants);
		}
	    }
	}
      if (error >= 0)
	{
	  db_query_end (query_result);
	}
      free_and_init (query);
    }
}
#endif /* ENABLE_UNUSED_FUNCTION */

/*
 * AUTHORIZATION CLASSES
 */

/*
 * au_install() - This is used to initialize the authorization system in a
 *                freshly created database.
 *                It should only be called within the createdb tool.
 *   return: error code
 */
int
au_install (void)
{
  MOP root = NULL, user = NULL, new_auth = NULL;
  SM_TEMPLATE *def;
  int exists, save, index;

  AU_DISABLE (save);

  /*
   * create the system authorization objects, add attributes later since they
   * have domain dependencies
   */
  root = db_create_class (AU_ROOT_CLASS_NAME);
  user = db_create_class (AU_USER_CLASS_NAME);
  new_auth = db_create_class (AU_AUTH_NAME);
  if (root == NULL || user == NULL || new_auth == NULL)
    {
      goto exit_on_error;
    }

  /*
   * Authorization root, might not need this if we restrict the generation of
   * user and  group objects but could be useful in other ways - nice to
   * have the methods here for adding/dropping user
   */
  def = smt_edit_class_mop_with_lock (root, X_LOCK);
  if (def == NULL)
    {
      goto exit_on_error;
    }
  smt_add_attribute (def, "charset", "integer", NULL);
  smt_add_attribute (def, "lang", "string", NULL);
  smt_finish_class (def, NULL);

  /* User/group objects */
  def = smt_edit_class_mop_with_lock (user, X_LOCK);
  if (def == NULL)
    {
      goto exit_on_error;
    }
  smt_add_attribute (def, "name", "string", (DB_DOMAIN *) 0);
  smt_add_attribute (def, "id", "integer", (DB_DOMAIN *) 0);
  smt_add_attribute (def, "password", "string", (DB_DOMAIN *) 0);
  smt_finish_class (def, NULL);

  /* Add Index */
  {
    const char *names[] = {
      "name", NULL
    };
    int ret_code;

    ret_code = db_add_constraint (user, DB_CONSTRAINT_INDEX, NULL, names);

    if (ret_code)
      {
	goto exit_on_error;
      }
  }

  /*
   * Authorization object, the grant set could go directly in the user object
   * but it might be better to keep it separate in order to use the special
   * read-once lock for the authorization object only.
   */

  def = smt_edit_class_mop_with_lock (new_auth, X_LOCK);
  if (def == NULL)
    {
      goto exit_on_error;
    }
  smt_add_attribute (def, "grantee_name", "varchar(255)", NULL);
  smt_add_attribute (def, "table_name", "varchar(255)", NULL);
  smt_add_attribute (def, "select_priv", "integer", NULL);
  smt_add_attribute (def, "insert_priv", "integer", NULL);
  smt_add_attribute (def, "update_priv", "integer", NULL);
  smt_add_attribute (def, "delete_priv", "integer", NULL);
  smt_add_attribute (def, "alter_priv", "integer", NULL);
  smt_finish_class (def, NULL);

  /* Add Index */
  {
    const char *names[3] = {
      "grantee_name", "table_name", NULL
    };
    int ret_code;

    ret_code = db_add_constraint (new_auth, DB_CONSTRAINT_INDEX, NULL, names);

    if (ret_code)
      {
	goto exit_on_error;
      }
  }

  /* Create the single authorization root object */
  Au_root = obj_create (root);
  if (Au_root == NULL)
    {
      goto exit_on_error;
    }

  /* create the DBA user and assign ownership of the system classes */
  Au_dba_user = au_add_user ("DBA", &exists);
  if (Au_dba_user == NULL)
    {
      goto exit_on_error;
    }

  /* establish the DBA as the current user */
  if (au_find_user_cache_index (Au_dba_user, &index, 0) != NO_ERROR)
    {
      goto exit_on_error;
    }
  Au_user = Au_dba_user;
  Au_cache_index = index;

  AU_SET_USER (Au_dba_user);

  au_change_owner (root, Au_user);
  au_change_owner (user, Au_user);
  au_change_owner (new_auth, Au_user);

  /* create the PUBLIC user */
  Au_public_user = au_add_user ("PUBLIC", &exists);
  if (Au_public_user == NULL)
    {
      goto exit_on_error;
    }

  /*
   * grant browser access to the authorization objects
   * note that the password class cannot be read by anyone except the DBA
   */
  au_grant (Au_public_user, root, AU_SELECT, false);
  au_grant (Au_public_user, user, AU_SELECT, false);
  au_grant (Au_public_user, new_auth, AU_SELECT, false);

  AU_ENABLE (save);
  return NO_ERROR;

exit_on_error:
  if (Au_public_user != NULL)
    {
      au_drop_user (Au_public_user);
      Au_public_user = NULL;
    }
  if (Au_dba_user != NULL)
    {
      au_drop_user (Au_dba_user);
      Au_dba_user = NULL;
    }
  if (Au_root != NULL)
    {
      obj_delete (Au_root);
      Au_root = NULL;
    }
  if (new_auth != NULL)
    {
      db_drop_class (new_auth);
    }
  if (user != NULL)
    {
      db_drop_class (user);
    }
  if (root != NULL)
    {
      db_drop_class (root);
    }

  AU_ENABLE (save);
  return (er_errid () == NO_ERROR ? ER_FAILED : er_errid ());
}


/*
 * RESTART/SHUTDOWN
 */

/*
 * au_init() - This is called by bo_restart when the database
 *             is being restarted.
 *             It must only be called once.
 *   return: none
 */
void
au_init (void)
{
  Au_root = NULL;
  Au_authorizations_class = NULL;
  Au_authorization_class = NULL;
  Au_user_class = NULL;

  Au_user = NULL;
  Au_public_user = NULL;
  Au_dba_user = NULL;
  Au_disable = 1;

  init_caches ();
}

/*
 * au_final() - Called during the bo_shutdown sequence.
 *   return: none
 */
void
au_final (void)
{
  Au_root = NULL;
  Au_authorizations_class = NULL;
  Au_authorization_class = NULL;
  Au_user_class = NULL;

  Au_user = NULL;
  Au_public_user = NULL;
  Au_dba_user = NULL;
  Au_disable = 1;

  /*
   * could remove the static links here but it isn't necessary and
   * we may need them again the next time we restart
   */

  flush_caches ();
}

#if defined (ENABLE_UNUSED_FUNCTION)
/*
 * au_get_class_privilege() -
 *   return: error code
 *   mop(in):
 *   auth(in):
 */
int
au_get_class_privilege (DB_OBJECT * mop, unsigned int *auth)
{
  SM_CLASS *class_;
  AU_CLASS_CACHE *cache;
  unsigned int bits = 0;
  int error = NO_ERROR;

  if (!Au_disable)
    {
      if (mop == NULL)
	{
	  return ER_FAILED;
	}

      class_ = (SM_CLASS *) mop->object;

      cache = (AU_CLASS_CACHE *) class_->auth_cache;
      if (cache == NULL)
	{
	  cache = au_install_class_cache (class_);
	  if (cache == NULL)
	    {
	      return er_errid ();
	    }
	}

      bits = cache->data[Au_cache_index];
      if (bits == AU_CACHE_INVALID)
	{
	  error = update_cache (mop, class_, cache);
	  if (error == NO_ERROR)
	    {
	      bits = cache->data[Au_cache_index];
	    }
	}
      *auth = bits;
    }

  return error;
}

/*
 * get_attribute_number - attribute number of the given attribute/class
 *   return:
 *   arg1(in):
 *   arg2(in):
 */
void
get_attribute_number (DB_OBJECT * target, DB_VALUE * result,
		      DB_VALUE * attr_name)
{
  int attrid;
  DB_DOMAIN *dom;

  db_make_null (result);

  if (DB_VALUE_TYPE (attr_name) != DB_TYPE_VARCHAR)
    {
      return;
    }

  /* we will only look for regular attributes and not class attributes.
   * this is a limitation of this method.
   */
  if (sm_att_info (target, db_get_string (attr_name), &attrid, &dom) < 0)
    {
      return;
    }

  db_make_int (result, attrid);
}
#endif

/*
 * au_disable - set Au_disable true
 *   return: original Au_disable value
 */
int
au_disable (void)
{
  int save = Au_disable;
  Au_disable = 1;
  return save;
}

/*
 * au_enable - restore Au_disable
 *   return:
 *   save(in): original Au_disable value
 */
void
au_enable (int save)
{
  Au_disable = save;
}

/*
 * au_public_user
 *   return: Au_public_user
 */
MOP
au_get_public_user (void)
{
  return Au_public_user;
}

/*
 * au_public_user
 *   return: Au_public_user
 */
MOP
au_get_dba_user (void)
{
  return Au_dba_user;
}

const char *
au_get_public_user_name (void)
{
  return AU_PUBLIC_USER_NAME;
}

const char *
au_get_user_class_name (void)
{
  return AU_USER_CLASS_NAME;
}
